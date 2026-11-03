//! The job runner: one action at a time per sub-app, with progress, cancellation and history.
//!
//! A run is asynchronous and long — a preview over a few thousand channels takes seconds, an
//! apply with programme checks takes minutes — so the HTTP layer starts it and returns, and
//! the operator watches it through the progress stream or on the run page.
//!
//! Three rules come from the plugin's own behaviour and are enforced here rather than left to
//! each app:
//!
//!   * one run per app at a time, checked and reserved under ONE lock, because two
//!     auto-matches writing the same channels would race;
//!   * a cancelled run actually tells the action to stop, and is recorded as cancelled rather
//!     than as a success;
//!   * a panic in an action frees the app and records the failure, so one bad run cannot lock
//!     the daemon out of ever running that app again.

use crate::config::{CrosstalkConfig, SharedConfig};
use crate::jobs::{JobContext, ProgressEvent, ProgressSink};
use crate::store::{RunStatus, Store};
use crate::subapp::{ActionKind, Registry};
use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// What a caller learns when it starts a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedRun {
    pub run_id: i64,
    pub app: String,
    pub action: String,
    pub dry_run: bool,
}

/// A snapshot of what is running, for the status page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveRun {
    pub run_id: i64,
    pub app: String,
    pub action: String,
    pub dry_run: bool,
    pub current: u64,
    pub total: u64,
    pub phase: String,
    /// Whether a cancellation has been requested. Exposed so the UI can say "stopping".
    pub cancelling: bool,
}

impl ActiveRun {
    pub fn percent(&self) -> Option<u8> {
        if self.total == 0 {
            return None;
        }
        Some(((self.current.min(self.total) * 100) / self.total) as u8)
    }
}

/// Publishes progress to every subscriber and keeps the latest snapshot per run.
struct BroadcastSink {
    sender: broadcast::Sender<ProgressEvent>,
    active: Arc<Mutex<HashMap<i64, ActiveRun>>>,
    run_id: i64,
}

impl ProgressSink for BroadcastSink {
    fn publish(&self, event: &ProgressEvent) {
        if let Some(entry) = self
            .active
            .lock()
            .expect("active lock")
            .get_mut(&self.run_id)
        {
            entry.current = event.current;
            entry.total = event.total;
            entry.phase = event.phase.clone();
        }
        // A send with no receivers is not an error: nobody is watching.
        let _ = self.sender.send(event.clone());
    }
}

/// Clears a run's bookkeeping when it ends, however it ends.
///
/// A drop guard rather than statements after the `.await`: if the action panics, the code
/// after the await never runs, and without this the app would stay "busy" for the life of the
/// daemon.
struct RunSlot {
    run_id: i64,
    app: String,
    store: Store,
    active: Arc<Mutex<HashMap<i64, ActiveRun>>>,
    busy: Arc<Mutex<HashMap<String, i64>>>,
    finish: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>,
    finished: bool,
}

impl RunSlot {
    /// Record the outcome, then release the slot.
    fn finish(
        mut self,
        status: RunStatus,
        summary: Option<&str>,
        counts: &serde_json::Value,
        report: Option<&str>,
    ) {
        if let Err(error) = self
            .store
            .finish_run(self.run_id, status, summary, counts, report)
        {
            tracing::error!(
                run_id = self.run_id,
                "could not record the run's outcome: {error:#}"
            );
        }
        self.finished = true;
        self.release();
    }

    fn release(&mut self) {
        // Only clear the app's slot if it is still THIS run: a cancelled run that a newer run
        // replaced must not evict the newer one's reservation.
        let mut busy = self.busy.lock().expect("busy lock");
        if busy.get(&self.app).is_some_and(|id| *id == self.run_id) {
            busy.remove(&self.app);
        }
        drop(busy);
        self.active
            .lock()
            .expect("active lock")
            .remove(&self.run_id);
        self.finish
            .lock()
            .expect("finish lock")
            .remove(&self.run_id);
    }
}

impl Drop for RunSlot {
    fn drop(&mut self) {
        if !self.finished {
            // Reached when the action panicked: record it as a failure so the history is
            // truthful, then free the app.
            if let Err(error) = self.store.finish_run(
                self.run_id,
                RunStatus::Failed,
                Some("the run panicked; see the daemon log"),
                &serde_json::json!({}),
                None,
            ) {
                tracing::error!(
                    run_id = self.run_id,
                    "could not record a panicked run: {error:#}"
                );
            }
            self.release();
        }
    }
}

/// Whether this failure is the store's one-live-run-per-app index refusing a second
/// `running` row for the app (see [`crate::store::Store::open_shared`]).
fn is_running_conflict(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<rusqlite::Error>(),
        Some(rusqlite::Error::SqliteFailure(failure, _))
            if failure.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

/// Runs sub-app actions.
#[derive(Clone)]
pub struct JobRunner {
    registry: Arc<Registry>,
    config: SharedConfig,
    store: Store,
    sender: broadcast::Sender<ProgressEvent>,
    active: Arc<Mutex<HashMap<i64, ActiveRun>>>,
    /// The run in progress per app.
    busy: Arc<Mutex<HashMap<String, i64>>>,
    /// Cancel flag per run.
    finish: Arc<Mutex<HashMap<i64, Arc<AtomicBool>>>>,
    shutdown: Arc<AtomicBool>,
}

impl std::fmt::Debug for JobRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobRunner")
            .field("running", &self.running())
            .field("shutting_down", &self.is_shutting_down())
            .finish_non_exhaustive()
    }
}

impl JobRunner {
    /// The runner shares the daemon's live config: every run snapshots it at start, so a
    /// save mid-run changes the next run, never the one already in flight.
    pub fn new(registry: Registry, config: SharedConfig, store: Store) -> Self {
        let (sender, _) = broadcast::channel(1024);
        Self {
            registry: Arc::new(registry),
            config,
            store,
            sender,
            active: Arc::new(Mutex::new(HashMap::new())),
            busy: Arc::new(Mutex::new(HashMap::new())),
            finish: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A point-in-time copy of the live config, for callers that need a value now.
    pub fn config_snapshot(&self) -> CrosstalkConfig {
        self.config.snapshot()
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Subscribe to progress events.
    ///
    /// A slow subscriber can lag; `broadcast` drops the oldest events and reports `Lagged`,
    /// which the SSE layer treats as "resynchronise from the run store".
    pub fn subscribe(&self) -> broadcast::Receiver<ProgressEvent> {
        self.sender.subscribe()
    }

    /// What is running right now, oldest run first.
    pub fn running(&self) -> Vec<ActiveRun> {
        let mut runs: Vec<ActiveRun> = self
            .active
            .lock()
            .expect("active lock")
            .values()
            .cloned()
            .collect();
        runs.sort_by_key(|run| run.run_id);
        runs
    }

    /// Whether this app already has a run in progress.
    pub fn is_busy(&self, app: &str) -> bool {
        self.busy.lock().expect("busy lock").contains_key(app)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Ask a run to stop.
    ///
    /// Sets the flag the action polls. The action keeps its slot until it returns, so the
    /// one-run-per-app rule still holds meanwhile; the run is recorded as cancelled.
    pub fn cancel(&self, run_id: i64) -> bool {
        let flags = self.finish.lock().expect("finish lock");
        match flags.get(&run_id) {
            Some(flag) => {
                flag.store(true, Ordering::SeqCst);
                drop(flags);
                if let Some(entry) = self.active.lock().expect("active lock").get_mut(&run_id) {
                    entry.cancelling = true;
                }
                true
            }
            None => false,
        }
    }

    /// Stop accepting new runs and ask every running one to stop.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let flags: Vec<Arc<AtomicBool>> = self
            .finish
            .lock()
            .expect("finish lock")
            .values()
            .cloned()
            .collect();
        for flag in flags {
            flag.store(true, Ordering::SeqCst);
        }
    }

    /// Reserve the app's slot and create the run, or explain why not.
    ///
    /// The check and the reservation happen under one lock: checking first and inserting later
    /// lets two simultaneous requests both through.
    fn reserve(
        &self,
        app_id: &str,
        action_id: &str,
        dry_run: bool,
    ) -> Result<(i64, Arc<AtomicBool>)> {
        if self.is_shutting_down() {
            bail!("the daemon is shutting down; not starting new runs");
        }

        let mut busy = self.busy.lock().expect("busy lock");
        if let Some(existing) = busy.get(app_id) {
            bail!(
                "{app_id} already has run {existing} in progress; wait for it to finish or \
                 cancel it"
            );
        }

        // The busy map above only covers this process. `start_run` also fails when ANOTHER
        // process (the daemon, or a second CLI) already holds a `running` row for the app
        // — the store's partial unique index — and that failure must read as "busy", not
        // as SQLite internals.
        let run_id = match self.store.start_run(app_id, action_id, dry_run) {
            Ok(run_id) => run_id,
            Err(error) if is_running_conflict(&error) => bail!(
                "{app_id} already has a run in progress in another process; wait for it to \
                 finish or cancel it (a stale row from a crash clears when the daemon next \
                 starts)"
            ),
            Err(error) => return Err(error),
        };
        busy.insert(app_id.to_string(), run_id);
        drop(busy);

        let flag = Arc::new(AtomicBool::new(false));
        self.finish
            .lock()
            .expect("finish lock")
            .insert(run_id, Arc::clone(&flag));
        self.active.lock().expect("active lock").insert(
            run_id,
            ActiveRun {
                run_id,
                app: app_id.to_string(),
                action: action_id.to_string(),
                dry_run,
                current: 0,
                total: 0,
                phase: "starting".to_string(),
                cancelling: false,
            },
        );
        Ok((run_id, flag))
    }

    /// Start a run and return immediately.
    ///
    /// The task owns everything it needs, so a dropped HTTP connection does not cancel the
    /// work: an apply that has begun should finish, which is what an operator expects after
    /// closing the tab.
    pub fn start(&self, app_id: &str, action_id: &str, dry_run: bool) -> Result<StartedRun> {
        let (app, _action) = self
            .registry
            .find_action(app_id, action_id)
            .with_context(|| format!("no action {action_id:?} in sub-app {app_id:?}"))?;

        let (run_id, cancel_flag) = self.reserve(app_id, action_id, dry_run)?;

        let runner = self.clone();
        let app = Arc::clone(app);
        let started_app = app_id.to_string();
        let started_action = action_id.to_string();

        tokio::spawn(async move {
            let slot = RunSlot {
                run_id,
                app: started_app.clone(),
                store: runner.store.clone(),
                active: Arc::clone(&runner.active),
                busy: Arc::clone(&runner.busy),
                finish: Arc::clone(&runner.finish),
                finished: false,
            };

            let sink = Arc::new(BroadcastSink {
                sender: runner.sender.clone(),
                active: Arc::clone(&runner.active),
                run_id,
            });
            // Snapshot at run start: a settings save while this run is in flight changes
            // the next run, never this one's context.
            let snapshot = Arc::new(runner.config.snapshot());
            let context = JobContext::new(snapshot, sink.clone()).for_run(
                run_id,
                runner.store.clone(),
                Arc::clone(&cancel_flag),
            );
            // Open the Dispatcharr session once per run, here rather than inside each sub-app, so
            // every action shares one client, one concurrency cap and one token.
            let needs_dispatcharr = app
                .actions()
                .iter()
                .find(|entry| entry.id == started_action)
                .map(|entry| entry.needs_dispatcharr)
                .unwrap_or(true);
            let context = if needs_dispatcharr {
                match context.connect() {
                    Ok(context) => context,
                    Err(error) => {
                        slot.finish(
                            RunStatus::Failed,
                            Some(&format!("{error:#}")),
                            &serde_json::json!({}),
                            None,
                        );
                        runner.prune_history();
                        let mut event = ProgressEvent::new(&started_action, 0, 0, "failed");
                        event.done = true;
                        event.summary = Some(format!("{error:#}"));
                        sink.publish(&event);
                        return;
                    }
                }
            } else {
                context
            };

            let outcome = app.run(&started_action, &context, dry_run).await;
            let cancelled = cancel_flag.load(Ordering::SeqCst);

            let (status, summary, counts, report) = match (&outcome, cancelled) {
                // Cancelled only counts when the action actually stopped: if it returned Ok the
                // work is done, and recording it as cancelled would hide a write that happened.
                (Err(_), true) => (
                    RunStatus::Cancelled,
                    Some("cancelled".to_string()),
                    serde_json::json!({}),
                    None,
                ),
                (Ok(outcome), _) => (
                    RunStatus::Succeeded,
                    Some(outcome.summary.clone()),
                    serde_json::to_value(&outcome.counts).unwrap_or_default(),
                    outcome
                        .report_path
                        .as_ref()
                        .map(|path| path.display().to_string()),
                ),
                (Err(error), false) => (
                    RunStatus::Failed,
                    Some(format!("{error:#}")),
                    serde_json::json!({}),
                    None,
                ),
            };

            slot.finish(status, summary.as_deref(), &counts, report.as_deref());
            runner.prune_history();

            let mut event = ProgressEvent::new(&started_action, 0, 0, status.as_str());
            event.done = true;
            event.summary = summary;
            sink.publish(&event);
        });

        Ok(StartedRun {
            run_id,
            app: app_id.to_string(),
            action: action_id.to_string(),
            dry_run,
        })
    }

    /// Run an action to completion, for the one-shot CLI and tests.
    ///
    /// Takes the same reservation as [`JobRunner::start`], so a one-shot run and a scheduled
    /// one cannot write the same channels at once.
    pub async fn run_to_completion(
        &self,
        app_id: &str,
        action_id: &str,
        dry_run: bool,
    ) -> Result<crate::subapp::JobOutcome> {
        let (app, _action) = self
            .registry
            .find_action(app_id, action_id)
            .with_context(|| format!("no action {action_id:?} in sub-app {app_id:?}"))?;

        let (run_id, cancel_flag) = self.reserve(app_id, action_id, dry_run)?;
        let slot = RunSlot {
            run_id,
            app: app_id.to_string(),
            store: self.store.clone(),
            active: Arc::clone(&self.active),
            busy: Arc::clone(&self.busy),
            finish: Arc::clone(&self.finish),
            finished: false,
        };

        let sink = Arc::new(BroadcastSink {
            sender: self.sender.clone(),
            active: Arc::clone(&self.active),
            run_id,
        });
        // Same snapshot rule as `start`: the run keeps the config it started with.
        let snapshot = Arc::new(self.config.snapshot());
        let context = JobContext::new(snapshot, sink).for_run(
            run_id,
            self.store.clone(),
            Arc::clone(&cancel_flag),
        );
        let needs_dispatcharr = app
            .actions()
            .iter()
            .find(|entry| entry.id == action_id)
            .map(|entry| entry.needs_dispatcharr)
            .unwrap_or(true);
        let context = if needs_dispatcharr {
            match context.connect() {
                Ok(context) => context,
                Err(error) => {
                    slot.finish(
                        RunStatus::Failed,
                        Some(&format!("{error:#}")),
                        &serde_json::json!({}),
                        None,
                    );
                    self.prune_history();
                    return Err(error);
                }
            }
        } else {
            context
        };

        let outcome = app.run(action_id, &context, dry_run).await;
        let cancelled = cancel_flag.load(Ordering::SeqCst);

        match &outcome {
            // Same rule as `start`: only an action that actually stopped is cancelled.
            Err(_) if cancelled => slot.finish(
                RunStatus::Cancelled,
                Some("cancelled"),
                &serde_json::json!({}),
                None,
            ),
            Ok(outcome) => slot.finish(
                RunStatus::Succeeded,
                Some(&outcome.summary),
                &serde_json::to_value(&outcome.counts).unwrap_or_default(),
                outcome
                    .report_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .as_deref(),
            ),
            Err(error) => slot.finish(
                RunStatus::Failed,
                Some(&format!("{error:#}")),
                &serde_json::json!({}),
                None,
            ),
        }
        self.prune_history();
        outcome
    }

    /// Delete run history older than the configured retention.
    ///
    /// Runs after every finished run. A failure here must never fail the run itself, so it
    /// is logged and dropped. The just-finished run is the newest of its app and `prune`
    /// skips running rows, so it can never delete the run that just ended.
    fn prune_history(&self) {
        let days = self.config.snapshot().jobs.run_history_days;
        if days == 0 {
            return;
        }
        match self.store.prune(days, 10) {
            Ok(0) => {}
            Ok(removed) => {
                tracing::info!(removed, days, "pruned old run history");
            }
            Err(error) => {
                tracing::warn!("could not prune old run history: {error:#}");
            }
        }
    }

    /// The dry-run flag a schedule should use for this action.
    ///
    /// A schedule naming a preview is a dry run; naming an apply is not. Deriving it from the
    /// action's own kind means a preview schedule can never write data just because the
    /// scheduler assumed otherwise.
    pub fn scheduled_dry_run(&self, app_id: &str, action_id: &str) -> bool {
        match self.registry.find_action(app_id, action_id) {
            Some((_app, action)) => !action.kind.touches_channels(),
            None => true,
        }
    }

    /// The kind of an action, for callers that need to report on it.
    pub fn action_kind(&self, app_id: &str, action_id: &str) -> Option<ActionKind> {
        self.registry
            .find_action(app_id, action_id)
            .map(|(_, action)| action.kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::VecSink;
    use crate::subapp::{ActionSpec, JobOutcome, SettingsSchema, SettingsValues, SubApp};
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    /// An app whose runs take `delay`, optionally panic, and honour cancellation.
    struct TestApp {
        runs: Arc<AtomicUsize>,
        delay: Duration,
        fail: bool,
        panic: bool,
    }

    impl TestApp {
        fn new(delay: Duration) -> Self {
            Self {
                runs: Arc::new(AtomicUsize::new(0)),
                delay,
                fail: false,
                panic: false,
            }
        }

        fn failing() -> Self {
            Self {
                fail: true,
                ..Self::new(Duration::from_millis(1))
            }
        }

        fn panicking() -> Self {
            Self {
                panic: true,
                ..Self::new(Duration::from_millis(1))
            }
        }
    }

    impl SubApp for TestApp {
        fn id(&self) -> &'static str {
            "test"
        }

        fn name(&self) -> &'static str {
            "Test"
        }

        fn description(&self) -> &'static str {
            "test app"
        }

        fn actions(&self) -> Vec<ActionSpec> {
            // Offline: these tests exercise the runner's bookkeeping, not the client.
            vec![
                ActionSpec::new("run", "Run", "runs", ActionKind::Read).offline(),
                ActionSpec::new("apply", "Apply", "writes", ActionKind::Write).offline(),
                ActionSpec::new("online", "Online", "needs a session", ActionKind::Read),
            ]
        }

        fn schema(&self) -> &SettingsSchema {
            static SCHEMA: std::sync::OnceLock<SettingsSchema> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(SettingsSchema::default)
        }

        fn load_settings(&self, _path: &Path) -> Result<SettingsValues> {
            Ok(SettingsValues::new())
        }

        fn save_settings(&self, _path: &Path, _values: &SettingsValues) -> Result<()> {
            Ok(())
        }

        fn run<'a>(
            &'a self,
            _action: &'a str,
            context: &'a JobContext,
            _dry_run: bool,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>>
        {
            Box::pin(async move {
                self.runs.fetch_add(1, Ordering::SeqCst);
                if self.panic {
                    panic!("deliberate panic");
                }
                context.progress(&ProgressEvent::new("run", 1, 2, "working"));
                // Poll for cancellation while "working", as a real action would.
                let waited = self.delay.as_millis() as u64;
                let mut elapsed = 0;
                while elapsed < waited {
                    if context.is_cancelled() {
                        anyhow::bail!("cancelled by the operator");
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    elapsed += 5;
                }
                if self.fail {
                    anyhow::bail!("deliberate failure");
                }
                Ok(JobOutcome::new("finished").with_count("rows", 2))
            })
        }
    }

    fn runner(app: TestApp, store: Store) -> JobRunner {
        JobRunner::new(
            Registry::new(vec![Arc::new(app)]),
            SharedConfig::new(CrosstalkConfig::default()),
            store,
        )
    }

    async fn wait_until_idle(runner: &JobRunner) {
        for _ in 0..200 {
            if runner.running().is_empty() && !runner.is_busy("test") {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the runner never went idle");
    }

    #[tokio::test]
    async fn a_run_is_recorded_and_reported_finished() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(10)), store.clone());
        let mut events = runner.subscribe();

        let started = runner.start("test", "run", true).expect("started");
        assert!(runner.is_busy("test"));

        let mut done = false;
        while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_secs(5), events.recv()).await
        {
            if event.done {
                done = true;
                break;
            }
        }
        assert!(done, "the run should publish a completion event");

        wait_until_idle(&runner).await;
        let record = store.run(started.run_id).expect("run").expect("present");
        assert_eq!(record.status, RunStatus::Succeeded);
        assert_eq!(record.summary.as_deref(), Some("finished"));
        assert_eq!(record.counts["rows"], 2);
    }

    #[tokio::test]
    async fn a_second_run_for_the_same_app_is_refused() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(100)), store);
        let _first = runner.start("test", "run", true).expect("started");
        let error = runner.start("test", "run", true).expect_err("refused");
        assert!(format!("{error}").contains("already has run"));
    }

    #[tokio::test]
    async fn concurrent_starts_only_let_one_through() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(100)), store);
        let mut accepted = 0;
        for _ in 0..4 {
            if runner.start("test", "run", true).is_ok() {
                accepted += 1;
            }
        }
        assert_eq!(accepted, 1, "only one run may hold the app");
    }

    #[tokio::test]
    async fn cancelling_reaches_the_action_and_is_recorded() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(300)), store.clone());
        let started = runner.start("test", "apply", false).expect("started");

        assert!(runner.cancel(started.run_id));
        // The slot is still held until the action notices, so a new run is still refused.
        assert!(runner.start("test", "apply", false).is_err());

        wait_until_idle(&runner).await;
        let record = store.run(started.run_id).expect("run").expect("present");
        assert_eq!(record.status, RunStatus::Cancelled);
    }

    #[tokio::test]
    async fn cancelling_an_unknown_run_reports_false() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(1)), store);
        assert!(!runner.cancel(999));
    }

    #[tokio::test]
    async fn a_panicking_action_frees_the_app_and_records_a_failure() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::panicking(), store.clone());
        let started = runner.start("test", "run", true).expect("started");

        wait_until_idle(&runner).await;
        let record = store.run(started.run_id).expect("run").expect("present");
        assert_eq!(record.status, RunStatus::Failed);
        assert!(record
            .summary
            .as_deref()
            .unwrap_or_default()
            .contains("panicked"));

        // And the app can be used again.
        assert!(runner.start("test", "run", true).is_ok());
    }

    #[tokio::test]
    async fn a_failure_is_recorded_as_failed() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::failing(), store.clone());
        let outcome = runner.run_to_completion("test", "run", true).await;
        assert!(outcome.is_err());

        let runs = store.recent_runs(1).expect("runs");
        assert_eq!(runs[0].status, RunStatus::Failed);
        assert!(runs[0]
            .summary
            .as_deref()
            .unwrap_or_default()
            .contains("deliberate failure"));
        assert!(!runner.is_busy("test"));
    }

    #[tokio::test]
    async fn a_second_process_holding_the_app_is_refused_as_busy() {
        // Two runners on one store stand in for the daemon and the one-shot CLI: the
        // second sees a busy app, not SQLite internals.
        let store = Store::in_memory().expect("store");
        let daemon = runner(TestApp::new(Duration::from_millis(200)), store.clone());
        let cli = runner(TestApp::new(Duration::from_millis(1)), store.clone());
        let _run = daemon.start("test", "run", true).expect("daemon started");
        let error = cli
            .run_to_completion("test", "run", true)
            .await
            .expect_err("refused");
        assert!(
            format!("{error:#}").contains("already has a run in progress in another process"),
            "{error:#}"
        );
        wait_until_idle(&daemon).await;
    }

    #[tokio::test]
    async fn run_to_completion_reserves_the_slot() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(60)), store.clone());
        let started = runner.start("test", "run", true).expect("started");
        let error = runner
            .run_to_completion("test", "run", true)
            .await
            .expect_err("refused while another run holds the app");
        assert!(format!("{error:#}").contains("already has run"));
        wait_until_idle(&runner).await;
        assert!(!runner.is_busy("test"));
        let _ = started;
    }

    #[tokio::test]
    async fn run_to_completion_returns_the_outcome_and_records_it() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(1)), store.clone());
        let outcome = runner
            .run_to_completion("test", "run", false)
            .await
            .expect("ok");
        assert_eq!(outcome.summary, "finished");
        assert_eq!(store.run_count().expect("count"), 1);
    }

    #[tokio::test]
    async fn shutdown_refuses_new_runs_and_stops_running_ones() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(200)), store.clone());
        let started = runner.start("test", "run", true).expect("started");

        runner.shutdown();
        assert!(runner.is_shutting_down());
        wait_until_idle(&runner).await;

        let record = store.run(started.run_id).expect("run").expect("present");
        assert_eq!(record.status, RunStatus::Cancelled);
        let error = runner.start("test", "run", true).expect_err("refused");
        assert!(format!("{error}").contains("shutting down"));
    }

    #[tokio::test]
    async fn an_action_that_needs_a_session_fails_cleanly_without_credentials() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(1)), store.clone());
        // The default config has no credentials, so the runner cannot open a session.
        let started = runner.start("test", "online", true).expect("started");
        wait_until_idle(&runner).await;

        let record = store.run(started.run_id).expect("run").expect("present");
        assert_eq!(record.status, RunStatus::Failed);
        assert!(record
            .summary
            .as_deref()
            .unwrap_or_default()
            .contains("credentials"));
        assert!(!runner.is_busy("test"), "the app is free again");
    }

    #[tokio::test]
    async fn an_unknown_app_or_action_is_refused() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(1)), store);
        assert!(runner.start("nope", "run", true).is_err());
        assert!(runner.start("test", "nope", true).is_err());
    }

    #[tokio::test]
    async fn scheduled_runs_are_dry_only_for_actions_that_do_not_write() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(1)), store);
        assert!(
            runner.scheduled_dry_run("test", "run"),
            "a read action is a dry run"
        );
        assert!(
            !runner.scheduled_dry_run("test", "apply"),
            "an apply is not"
        );
        assert!(runner.scheduled_dry_run("test", "unknown"));
        assert_eq!(runner.action_kind("test", "apply"), Some(ActionKind::Write));
    }

    #[tokio::test]
    async fn the_context_gives_an_action_its_run_id() {
        // The runner ties the context to the run so rows can be recorded as they are produced.
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(1)), store.clone());
        let run_id = runner
            .store()
            .start_run("test", "run", true)
            .expect("start");
        let context = JobContext::new(
            Arc::new(CrosstalkConfig::default()),
            Arc::new(VecSink::new()),
        )
        .for_run(run_id, store.clone(), Arc::new(AtomicBool::new(false)));
        assert_eq!(context.run_id(), Some(run_id));
        context
            .record_rows(&[crate::store::RunRow {
                ordinal: 0,
                channel_id: 1,
                channel_name: "ESPN".to_string(),
                ..Default::default()
            }])
            .expect("rows");
        assert_eq!(store.rows(run_id).expect("rows").len(), 1);
    }

    #[test]
    fn active_run_reports_progress() {
        let mut active = ActiveRun {
            run_id: 1,
            app: "a".to_string(),
            action: "b".to_string(),
            dry_run: false,
            current: 0,
            total: 0,
            phase: String::new(),
            cancelling: false,
        };
        assert_eq!(active.percent(), None);
        active.total = 10;
        active.current = 4;
        assert_eq!(active.percent(), Some(40));
        active.current = 20;
        assert_eq!(active.percent(), Some(100));
    }

    fn seed_finished_run(store: &Store, app: &str, days_ago: i64) -> i64 {
        let run_id = store.start_run(app, "run", true).expect("start");
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("seeded"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        // `started_at` has one-second resolution; ids break the tie, so backdating every
        // seed to the same old timestamp still leaves their order deterministic.
        let started = (chrono::Utc::now() - chrono::Duration::days(days_ago))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        store
            .set_started_at_for_test(run_id, &started)
            .expect("age");
        run_id
    }

    #[tokio::test]
    async fn a_finished_run_prunes_history_beyond_retention_and_minimum() {
        let store = Store::in_memory().expect("store");
        // Twelve old runs plus the one the test is about to run: 13 total, so pruning to
        // the 10-newest leaves 10 and the just-finished run must be among them.
        for _ in 0..12 {
            seed_finished_run(&store, "test", 30);
        }
        let runner = runner(TestApp::new(Duration::from_millis(1)), store.clone());
        assert_eq!(runner.config_snapshot().jobs.run_history_days, 15);
        let outcome = runner
            .run_to_completion("test", "run", true)
            .await
            .expect("ok");
        assert_eq!(outcome.summary, "finished");
        let runs = store.recent_runs_for("test", 100).expect("runs");
        assert_eq!(runs.len(), 10, "keeps the 10 newest");
        assert!(
            runs.iter()
                .any(|run| run.summary.as_deref() == Some("finished")),
            "the run that just finished is never pruned"
        );
    }

    #[tokio::test]
    async fn zero_retention_disables_pruning() {
        let store = Store::in_memory().expect("store");
        for _ in 0..3 {
            seed_finished_run(&store, "test", 30);
        }
        let config = CrosstalkConfig {
            jobs: crate::config::JobsConfig {
                run_history_days: 0,
                ..crate::config::JobsConfig::default()
            },
            ..CrosstalkConfig::default()
        };
        let runner = JobRunner::new(
            Registry::new(vec![Arc::new(TestApp::new(Duration::from_millis(1)))]),
            SharedConfig::new(config),
            store.clone(),
        );
        runner
            .run_to_completion("test", "run", true)
            .await
            .expect("ok");
        assert_eq!(store.run_count().expect("count"), 4);
    }

    /// An app that records the config value its run started with.
    struct SnapshotApp {
        seen: Arc<std::sync::Mutex<Vec<u16>>>,
        delay: Duration,
    }

    impl SubApp for SnapshotApp {
        fn id(&self) -> &'static str {
            "test"
        }

        fn name(&self) -> &'static str {
            "Test"
        }

        fn description(&self) -> &'static str {
            "test app"
        }

        fn actions(&self) -> Vec<ActionSpec> {
            vec![ActionSpec::new("run", "Run", "runs", ActionKind::Read).offline()]
        }

        fn schema(&self) -> &SettingsSchema {
            static SCHEMA: std::sync::OnceLock<SettingsSchema> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(SettingsSchema::default)
        }

        fn load_settings(&self, _path: &Path) -> Result<SettingsValues> {
            Ok(SettingsValues::new())
        }

        fn save_settings(&self, _path: &Path, _values: &SettingsValues) -> Result<()> {
            Ok(())
        }

        fn run<'a>(
            &'a self,
            _action: &'a str,
            context: &'a JobContext,
            _dry_run: bool,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>>
        {
            Box::pin(async move {
                // Record first, then work: a save that lands while this run is in flight
                // must not change what this run already observed.
                self.seen
                    .lock()
                    .expect("seen lock")
                    .push(context.config().server.port);
                tokio::time::sleep(self.delay).await;
                // Re-read at the end: the snapshot is a clone, so it still says the old value.
                self.seen
                    .lock()
                    .expect("seen lock")
                    .push(context.config().server.port);
                Ok(JobOutcome::new("finished"))
            })
        }
    }

    fn snapshot_runner(seen: Arc<std::sync::Mutex<Vec<u16>>>, delay: Duration) -> JobRunner {
        let mut config = CrosstalkConfig::default();
        config.server.port = 1111;
        JobRunner::new(
            Registry::new(vec![Arc::new(SnapshotApp { seen, delay })]),
            SharedConfig::new(config),
            Store::in_memory().expect("store"),
        )
    }

    #[test]
    fn shared_config_replace_swaps_the_snapshot() {
        let shared = SharedConfig::new(CrosstalkConfig::default());
        assert!(shared.snapshot().matching.ignore_quality_tags);
        let mut next = CrosstalkConfig::default();
        next.matching.ignore_quality_tags = false;
        next.server.port = 9999;
        shared.replace(next);
        let snapshot = shared.snapshot();
        assert!(!snapshot.matching.ignore_quality_tags);
        assert_eq!(snapshot.server.port, 9999);
        // The earlier snapshot is a clone, unaffected by the replace.
        assert_eq!(shared.read().server.port, 9999);
    }

    #[tokio::test]
    async fn a_replace_between_runs_reaches_the_next_run() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let runner = snapshot_runner(Arc::clone(&seen), Duration::from_millis(1));
        runner
            .run_to_completion("test", "run", true)
            .await
            .expect("first run");
        let mut next = runner.config_snapshot();
        next.server.port = 2222;
        runner.config.replace(next);
        runner
            .run_to_completion("test", "run", true)
            .await
            .expect("second run");
        // Each run records its port twice (start and end); the first run saw 1111, the
        // second sees 2222.
        assert_eq!(
            *seen.lock().expect("seen lock"),
            vec![1111, 1111, 2222, 2222]
        );
    }

    #[tokio::test]
    async fn a_replace_mid_run_does_not_mutate_the_running_job() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let runner = snapshot_runner(Arc::clone(&seen), Duration::from_millis(100));
        runner.start("test", "run", true).expect("started");
        // Give the run a moment to record its starting port, then save new settings.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut next = runner.config_snapshot();
        next.server.port = 2222;
        runner.config.replace(next);
        wait_until_idle(&runner).await;
        // Both observations come from the run's own snapshot: the mid-run replace changed
        // the shared value, not the running job's context.
        assert_eq!(*seen.lock().expect("seen lock"), vec![1111, 1111]);
    }
}
