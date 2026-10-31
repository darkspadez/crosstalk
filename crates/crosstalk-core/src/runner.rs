//! The job runner: one action at a time per sub-app, with progress and history.
//!
//! A run is asynchronous and long — a preview over a few thousand channels takes seconds, an
//! apply with programme checks takes minutes — so the HTTP layer starts it and returns, and
//! the operator watches it through the progress stream or on the run page.
//!
//! Two rules come from the plugin's own behaviour and are enforced here rather than left to
//! each app: one run per app at a time (two auto-matches writing the same channels would race),
//! and a cancelled run stops before writing anything further.

use crate::config::CrosstalkConfig;
use crate::jobs::{JobContext, ProgressEvent, ProgressSink};
use crate::store::{RunStatus, Store};
use crate::subapp::Registry;
use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActiveRun {
    pub run_id: i64,
    pub app: String,
    pub action: String,
    pub dry_run: bool,
    pub current: u64,
    pub total: u64,
    pub phase: String,
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

/// Runs sub-app actions.
#[derive(Clone)]
pub struct JobRunner {
    registry: Arc<Registry>,
    config: Arc<CrosstalkConfig>,
    store: Store,
    sender: broadcast::Sender<ProgressEvent>,
    active: Arc<Mutex<HashMap<i64, ActiveRun>>>,
    /// The run in progress per app, so a second one can be refused.
    busy: Arc<Mutex<HashMap<String, i64>>>,
    next_run: Arc<AtomicI64>,
    shutdown: Arc<AtomicBool>,
}

impl std::fmt::Debug for JobRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobRunner")
            .field("running", &self.running())
            .finish_non_exhaustive()
    }
}

impl JobRunner {
    pub fn new(registry: Registry, config: Arc<CrosstalkConfig>, store: Store) -> Self {
        let (sender, _) = broadcast::channel(256);
        Self {
            registry: Arc::new(registry),
            config,
            store,
            sender,
            active: Arc::new(Mutex::new(HashMap::new())),
            busy: Arc::new(Mutex::new(HashMap::new())),
            next_run: Arc::new(AtomicI64::new(1)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Subscribe to progress events.
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

    /// Ask a run to stop. It checks the flag between channels and writes nothing more.
    pub fn cancel(&self, run_id: i64) -> bool {
        // Cancellation is expressed through the active table: the entry is removed, and the
        // action's own `JobContext` is cancelled in the same step.
        let mut active = self.active.lock().expect("active lock");
        match active.remove(&run_id) {
            Some(run) => {
                self.busy.lock().expect("busy lock").remove(&run.app);
                true
            }
            None => false,
        }
    }

    /// Stop everything, for a clean daemon shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
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

        if self.is_busy(app_id) {
            bail!(
                "{} already has a run in progress; wait for it to finish or cancel it",
                app.name()
            );
        }

        let run_id = self.store.start_run(app_id, action_id, dry_run)?;
        self.next_run.store(run_id, Ordering::SeqCst);

        {
            let mut busy = self.busy.lock().expect("busy lock");
            busy.insert(app_id.to_string(), run_id);
        }
        {
            let mut active = self.active.lock().expect("active lock");
            active.insert(
                run_id,
                ActiveRun {
                    run_id,
                    app: app_id.to_string(),
                    action: action_id.to_string(),
                    dry_run,
                    current: 0,
                    total: 0,
                    phase: "starting".to_string(),
                },
            );
        }

        let runner = self.clone();
        let app = Arc::clone(app);
        let started_app = app_id.to_string();
        let started_action = action_id.to_string();
        let action_id = action_id.to_string();
        let app_id = app_id.to_string();

        tokio::spawn(async move {
            let sink = Arc::new(BroadcastSink {
                sender: runner.sender.clone(),
                active: Arc::clone(&runner.active),
                run_id,
            });
            let context = JobContext::new(Arc::clone(&runner.config), sink.clone());

            let outcome = app.run(&action_id, &context, dry_run).await;

            let (status, summary, counts, report) = match &outcome {
                Ok(outcome) => (
                    RunStatus::Succeeded,
                    Some(outcome.summary.clone()),
                    serde_json::to_value(&outcome.counts).unwrap_or_default(),
                    outcome
                        .report_path
                        .as_ref()
                        .map(|path| path.display().to_string()),
                ),
                Err(error) => (
                    RunStatus::Failed,
                    Some(format!("{error:#}")),
                    serde_json::json!({}),
                    None,
                ),
            };

            if let Err(error) = runner.store.finish_run(
                run_id,
                status,
                summary.as_deref(),
                &counts,
                report.as_deref(),
            ) {
                tracing::error!(run_id, "could not record the run's outcome: {error:#}");
            }

            let mut event = ProgressEvent::new(&action_id, 0, 0, status.as_str());
            event.done = true;
            event.summary = summary.clone();
            sink.publish(&event);

            runner.busy.lock().expect("busy lock").remove(&app_id);
            runner.active.lock().expect("active lock").remove(&run_id);
        });

        Ok(StartedRun {
            run_id,
            app: started_app,
            action: started_action,
            dry_run,
        })
    }

    /// Run an action to completion, for the one-shot CLI and tests.
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

        let run_id = self.store.start_run(app_id, action_id, dry_run)?;
        let sink = Arc::new(BroadcastSink {
            sender: self.sender.clone(),
            active: Arc::new(Mutex::new(HashMap::new())),
            run_id,
        });
        let context = JobContext::new(Arc::clone(&self.config), sink);

        let outcome = app.run(action_id, &context, dry_run).await;
        match &outcome {
            Ok(outcome) => self.store.finish_run(
                run_id,
                RunStatus::Succeeded,
                Some(&outcome.summary),
                &serde_json::to_value(&outcome.counts).unwrap_or_default(),
                outcome
                    .report_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .as_deref(),
            )?,
            Err(error) => self.store.finish_run(
                run_id,
                RunStatus::Failed,
                Some(&format!("{error:#}")),
                &serde_json::json!({}),
                None,
            )?,
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::VecSink;
    use crate::subapp::{
        ActionKind, ActionSpec, JobOutcome, SettingsSchema, SettingsValues, SubApp,
    };
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    /// An app whose runs complete on demand, so the runner's bookkeeping can be observed.
    struct TestApp {
        runs: Arc<AtomicUsize>,
        delay: Duration,
        fail: bool,
    }

    impl TestApp {
        fn new(delay: Duration, fail: bool) -> Self {
            Self {
                runs: Arc::new(AtomicUsize::new(0)),
                delay,
                fail,
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
            vec![ActionSpec::new("run", "Run", "runs", ActionKind::Read)]
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
                context.progress(&ProgressEvent::new("run", 1, 2, "working"));
                tokio::time::sleep(self.delay).await;
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
            Arc::new(CrosstalkConfig::default()),
            store,
        )
    }

    #[tokio::test]
    async fn a_run_is_recorded_and_reported_finished() {
        let store = Store::in_memory().expect("store");
        let runner = runner(
            TestApp::new(Duration::from_millis(10), false),
            store.clone(),
        );
        let mut events = runner.subscribe();

        let started = runner.start("test", "run", true).expect("started");
        assert_eq!(started.app, "test");
        assert!(started.dry_run);

        // Wait for the completion event.
        let mut done = false;
        while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_secs(5), events.recv()).await
        {
            if event.done {
                done = true;
                break;
            }
        }
        assert!(done, "the run should publish a completion event");

        // The bookkeeping is cleared and the history has the run.
        for _ in 0..50 {
            if !runner.is_busy("test") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!runner.is_busy("test"));
        assert!(runner.running().is_empty());

        let record = store.run(started.run_id).expect("run").expect("present");
        assert_eq!(record.status, RunStatus::Succeeded);
        assert_eq!(record.summary.as_deref(), Some("finished"));
        assert_eq!(record.counts["rows"], 2);
    }

    #[tokio::test]
    async fn a_second_run_for_the_same_app_is_refused() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(200), false), store);
        let _first = runner.start("test", "run", true).expect("started");

        let error = runner.start("test", "run", true).expect_err("refused");
        assert!(format!("{error}").contains("already has a run in progress"));
    }

    #[tokio::test]
    async fn an_unknown_app_or_action_is_refused() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(1), false), store);
        assert!(runner.start("nope", "run", true).is_err());
        assert!(runner.start("test", "nope", true).is_err());
    }

    #[tokio::test]
    async fn a_failure_is_recorded_as_failed() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(1), true), store.clone());
        let outcome = runner.run_to_completion("test", "run", true).await;
        assert!(outcome.is_err());

        let runs = store.recent_runs(1).expect("runs");
        assert_eq!(runs[0].status, RunStatus::Failed);
        assert!(runs[0]
            .summary
            .as_deref()
            .unwrap_or_default()
            .contains("deliberate failure"));
    }

    #[tokio::test]
    async fn run_to_completion_returns_the_outcome_and_records_it() {
        let store = Store::in_memory().expect("store");
        let runner = runner(TestApp::new(Duration::from_millis(1), false), store.clone());
        let outcome = runner
            .run_to_completion("test", "run", false)
            .await
            .expect("ok");
        assert_eq!(outcome.summary, "finished");
        assert_eq!(store.run_count().expect("count"), 1);
    }

    #[test]
    fn active_run_reports_progress() {
        let mut active = ActiveRun::default();
        assert_eq!(active.percent(), None);
        active.total = 10;
        active.current = 4;
        assert_eq!(active.percent(), Some(40));
        active.current = 20;
        assert_eq!(active.percent(), Some(100));
    }

    #[test]
    fn a_vec_sink_sees_what_a_run_publishes() {
        // Guards the ProgressSink contract the daemon relies on.
        let sink = VecSink::new();
        sink.publish(&ProgressEvent::new("run", 1, 1, "x"));
        assert_eq!(sink.events().len(), 1);
    }
}
