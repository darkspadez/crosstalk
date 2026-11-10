//! Job plumbing: the context an action runs with, and its progress channel.
//!
//! A sub-app never touches the configuration file, the database or the network directly.
//! It receives a [`JobContext`] holding everything the host owns, so the same action code
//! runs from the daemon, from `crosstalk run`, and from a test.
//!
//! # Stopping: the rules every app follows
//!
//! There are three different reasons a long action stops early, and the plugins conflated them.
//! One rule each, enforced by the host so no sub-app can invent its own:
//!
//!   * **Paused** — a schedule window ended ([`JobContext::past_deadline`]). The action checks
//!     between work units, saves its place ([`JobContext::save_checkpoint`]) and returns
//!     [`JobOutcome::paused`](crate::subapp::JobOutcome::paused). The run is recorded `paused`,
//!     and the next firing of the same action loads the checkpoint
//!     ([`JobContext::load_checkpoint`]) and continues. Nothing is lost and nothing is claimed as
//!     finished.
//!   * **Cancelled** — the operator (or a shutdown) asked it to stop. The run is recorded
//!     `cancelled` whether or not writes had landed, and it keeps its counts, its report and the
//!     rows already recorded, so the history says exactly how far it got. It is never recorded
//!     `succeeded`: a cancelled run is not evidence the whole job ran, and a later action that
//!     selects from "the last successful run" must not pick it up. The checkpoint is LEFT in
//!     place, as IPTV Checker left its resume file: an operator who cancels at 03:00 because the
//!     host is needed wants the rest of the backlog to continue tomorrow, not to start over.
//!     [`RunOptions::fresh`](crate::runner::RunOptions) is how they ask to start over.
//!   * **Failed** — an error the run did not recover from. Unchanged from before.
//!
//! An action that throws away its partial result (bails during planning, before any write) is
//! the cancelled case for the record's purposes: the host records it cancelled with an empty
//! count, because nothing landed.
//!
//! Manual runs (`crosstalk run`, the web UI) have no deadline, so they never pause — but they
//! still resume from a checkpoint, and `--fresh` / the "start fresh" checkbox discards one.
//!
//! A workflow ([`crate::workflow`]) changes nothing about these rules: every step is an ordinary
//! run of its app, so it pauses, cancels and resumes by the same rules. What a workflow adds is
//! that a step which PAUSED pauses the workflow too, so the next firing continues at that step
//! instead of re-running the ones before it.

use crate::config::CrosstalkConfig;
use crate::runner::ActiveRun;
use crate::store::{AiDecision, RunRow, Store};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Where the live runs are kept, shared with the runner.
///
/// Handed to a context so a read-only action can report the progress of a run that is blocking
/// it: `status` used to be refused while another run of its app was live, which is exactly when
/// an operator asks what is going on.
type LiveRuns = Arc<Mutex<HashMap<i64, ActiveRun>>>;

/// One progress update, as the UI and the run record show it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProgressEvent {
    /// Which action is running, for a client that joined late.
    pub action: String,
    /// Work completed so far.
    pub current: u64,
    /// Work expected. Zero means "not known yet".
    pub total: u64,
    /// A short human-readable phase, for example "validating candidates".
    pub phase: String,
    pub done: bool,
    /// Filled in when `done` is set.
    pub summary: Option<String>,
}

impl ProgressEvent {
    pub fn new(
        action: impl Into<String>,
        current: u64,
        total: u64,
        phase: impl Into<String>,
    ) -> Self {
        Self {
            action: action.into(),
            current,
            total,
            phase: phase.into(),
            done: false,
            summary: None,
        }
    }

    /// Completion percentage, or `None` while the total is unknown.
    pub fn percent(&self) -> Option<u8> {
        if self.total == 0 {
            return None;
        }
        Some(((self.current.min(self.total) * 100) / self.total) as u8)
    }
}

/// Where progress updates go.
///
/// The daemon's implementation fans out to the SSE stream and the run store; the CLI's
/// prints to stderr; tests use [`NullSink`].
pub trait ProgressSink: Send + Sync {
    fn publish(&self, event: &ProgressEvent);
}

/// Discards updates. Used by tests and by `--quiet` runs.
#[derive(Debug, Default)]
pub struct NullSink;

impl ProgressSink for NullSink {
    fn publish(&self, _event: &ProgressEvent) {}
}

/// Collects updates in memory, for tests and for the one-shot CLI path.
#[derive(Debug, Default)]
pub struct VecSink {
    events: std::sync::Mutex<Vec<ProgressEvent>>,
}

impl VecSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<ProgressEvent> {
        self.events.lock().expect("sink lock").clone()
    }

    pub fn last(&self) -> Option<ProgressEvent> {
        self.events.lock().expect("sink lock").last().cloned()
    }
}

impl ProgressSink for VecSink {
    fn publish(&self, event: &ProgressEvent) {
        self.events.lock().expect("sink lock").push(event.clone());
    }
}

/// Everything an action is given.
#[derive(Clone)]
pub struct JobContext {
    config: Arc<CrosstalkConfig>,
    progress: Arc<dyn ProgressSink>,
    /// The Dispatcharr session, shared by every action so a run costs one login rather than
    /// one per call. `None` until a caller builds it, which keeps a dry check of the
    /// settings file from requiring a reachable server.
    dispatcharr: Option<crate::dispatcharr::DispatcharrClient>,
    /// Cancellation flag, set when the UI cancels a run or the daemon shuts down. Shared with
    /// the runner, so cancelling actually reaches the action instead of only forgetting it.
    cancelled: Arc<AtomicBool>,
    /// The run this context belongs to, and where to record its rows.
    run_id: Option<i64>,
    store: Option<Store>,
    /// Which action this is, so its checkpoint can be found again by the next run of the same
    /// app and action.
    app: Option<String>,
    action: Option<String>,
    /// When the run must stop and checkpoint. `None` for a manual run, which never pauses.
    deadline: Option<DateTime<Utc>>,
    /// The runs in flight, when the runner built this context.
    live: Option<LiveRuns>,
}

impl std::fmt::Debug for JobContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobContext")
            .field("config_dir", &self.config.config_dir)
            .finish_non_exhaustive()
    }
}

impl JobContext {
    pub fn new(config: Arc<CrosstalkConfig>, progress: Arc<dyn ProgressSink>) -> Self {
        Self {
            config,
            progress,
            dispatcharr: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            run_id: None,
            store: None,
            app: None,
            action: None,
            deadline: None,
            live: None,
        }
    }

    /// Tie this context to a run: gives the action the shared cancel flag, a place to record
    /// rows as they are produced, so a crash mid-run still leaves an audit trail, and the
    /// `app`/`action` pair that names its checkpoint.
    pub fn for_run(
        mut self,
        app: impl Into<String>,
        action: impl Into<String>,
        run_id: i64,
        store: Store,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        self.app = Some(app.into());
        self.action = Some(action.into());
        self.run_id = Some(run_id);
        self.store = Some(store);
        self.cancelled = cancelled;
        self
    }

    /// The app and action this context runs as, when it belongs to a recorded run.
    pub fn app(&self) -> Option<&str> {
        self.app.as_deref()
    }

    pub fn action(&self) -> Option<&str> {
        self.action.as_deref()
    }

    /// The instant this run must stop at, when a schedule window bounded it.
    pub fn deadline(&self) -> Option<DateTime<Utc>> {
        self.deadline
    }

    /// Bound this run by a schedule window's end. The runner sets it from the schedule entry;
    /// a manual run leaves it unset and never pauses.
    pub fn with_deadline(mut self, deadline: Option<DateTime<Utc>>) -> Self {
        self.deadline = deadline;
        self
    }

    /// How long is left of the window, or `None` when the run is unbounded.
    pub fn remaining(&self) -> Option<chrono::Duration> {
        self.deadline.map(|deadline| deadline - Utc::now())
    }

    /// Whether the window has closed.
    ///
    /// This is the check an action makes between work units: cheap (two clock reads at most),
    /// and answering `false` for a run with no deadline, so the same code serves a scheduled
    /// and a manual run.
    pub fn past_deadline(&self) -> bool {
        self.deadline.is_some_and(|deadline| Utc::now() >= deadline)
    }

    /// Attach the live-run table, so a read-only action can report what is in progress.
    pub fn with_live_runs(mut self, live: LiveRuns) -> Self {
        self.live = Some(live);
        self
    }

    /// The runs in flight for one app, oldest first. Empty outside the runner.
    pub fn active_runs_for(&self, app: &str) -> Vec<ActiveRun> {
        let Some(live) = &self.live else {
            return Vec::new();
        };
        let mut runs: Vec<ActiveRun> = live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .filter(|run| run.app == app)
            .cloned()
            .collect();
        runs.sort_by_key(|run| run.run_id);
        runs
    }

    /// The cancel flag, for a helper that waits on it — the per-key limiter's acquire, so a
    /// cancelled probe does not sit out a spacing delay.
    pub fn cancel_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    /// This run's saved progress, or `None` when it has none (or this is not a recorded run).
    pub fn load_checkpoint(&self) -> Option<serde_json::Value> {
        let (Some(store), Some(app), Some(action)) = (&self.store, &self.app, &self.action) else {
            return None;
        };
        match store.load_checkpoint(app, action) {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!("could not read the {app}.{action} checkpoint: {error:#}");
                None
            }
        }
    }

    /// Save progress, so the next run of this app and action continues instead of starting over.
    ///
    /// A no-op outside a recorded run, like [`JobContext::record_rows`]: the CLI's test contexts
    /// and a settings check have nowhere to keep one.
    pub fn save_checkpoint(&self, state: &serde_json::Value) -> Result<()> {
        match (&self.store, &self.app, &self.action) {
            (Some(store), Some(app), Some(action)) => store.save_checkpoint(app, action, state),
            _ => Ok(()),
        }
    }

    /// Discard saved progress: the job is finished, or the state is no longer usable.
    pub fn clear_checkpoint(&self) -> Result<()> {
        match (&self.store, &self.app, &self.action) {
            (Some(store), Some(app), Some(action)) => {
                store.clear_checkpoint(app, action).map(|_| ())
            }
            _ => Ok(()),
        }
    }

    /// The run id, when this context belongs to a recorded run.
    pub fn run_id(&self) -> Option<i64> {
        self.run_id
    }

    /// The run store, for an action that reports on history (the status action).
    pub fn store(&self) -> Option<&Store> {
        self.store.as_ref()
    }

    /// Append rows to the run record. A no-op outside a recorded run.
    pub fn record_rows(&self, rows: &[RunRow]) -> anyhow::Result<()> {
        match (&self.store, self.run_id) {
            (Some(store), Some(run_id)) => store.append_rows(run_id, rows),
            _ => Ok(()),
        }
    }

    /// Record one AI request and its answer. A no-op outside a recorded run.
    pub fn record_ai_decision(&self, decision: &AiDecision) -> anyhow::Result<()> {
        match (&self.store, self.run_id) {
            (Some(store), Some(run_id)) => store.record_ai_decision(run_id, decision),
            _ => Ok(()),
        }
    }

    /// Attach a Dispatcharr session.
    pub fn with_dispatcharr(mut self, client: crate::dispatcharr::DispatcharrClient) -> Self {
        self.dispatcharr = Some(client);
        self
    }

    /// Build the session from the configuration, if there are credentials to build it with.
    pub fn connect(mut self) -> anyhow::Result<Self> {
        if !self.config.dispatcharr.has_credentials() {
            anyhow::bail!(
                "no Dispatcharr credentials: set dispatcharr.api_key or username/password"
            );
        }
        let client = crate::dispatcharr::DispatcharrClient::new(&self.config.dispatcharr)?;
        self.dispatcharr = Some(client);
        Ok(self)
    }

    /// The session, or an error explaining what is missing.
    pub fn dispatcharr(&self) -> anyhow::Result<&crate::dispatcharr::DispatcharrClient> {
        self.dispatcharr.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "this run has no Dispatcharr session; build the context with `connect()`"
            )
        })
    }

    /// Whether a session is attached.
    pub fn has_dispatcharr(&self) -> bool {
        self.dispatcharr.is_some()
    }

    /// A context that discards progress: the one-shot CLI path, and tests.
    pub fn headless(config: CrosstalkConfig) -> Self {
        Self::new(Arc::new(config), Arc::new(NullSink))
    }

    /// Alias kept so library tests read naturally.
    pub fn for_tests(config: CrosstalkConfig) -> Self {
        Self::headless(config)
    }

    pub fn config(&self) -> &CrosstalkConfig {
        &self.config
    }

    pub fn config_arc(&self) -> Arc<CrosstalkConfig> {
        Arc::clone(&self.config)
    }

    pub fn exports_dir(&self) -> PathBuf {
        self.config.exports_dir()
    }

    pub fn data_dir(&self) -> PathBuf {
        self.config.data_dir()
    }

    pub fn progress(&self, event: &ProgressEvent) {
        self.progress.publish(event);
    }

    /// A cheap paced reporter: publishes at most once per interval, plus the final event.
    ///
    /// The plugin compared `time.time()` against the last flush every 2 seconds for the
    /// same reason: an update per channel would swamp the websocket without telling the
    /// operator anything new.
    pub fn paced(&self, action: &str, total: u64, interval: Duration) -> PacedProgress {
        PacedProgress {
            sink: Arc::clone(&self.progress),
            action: action.to_string(),
            total,
            interval,
            // `checked_sub` rather than plain subtraction: a very large interval would
            // otherwise panic on the first report.
            last: std::time::Instant::now()
                .checked_sub(interval)
                .unwrap_or_else(std::time::Instant::now),
            current: 0,
        }
    }

    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Rate-limited progress reporter.
pub struct PacedProgress {
    sink: Arc<dyn ProgressSink>,
    action: String,
    total: u64,
    interval: Duration,
    last: std::time::Instant,
    current: u64,
}

impl PacedProgress {
    pub fn set(&mut self, current: u64, phase: &str) {
        self.current = current;
        if self.last.elapsed() >= self.interval {
            self.last = std::time::Instant::now();
            self.sink.publish(&ProgressEvent::new(
                &self.action,
                current,
                self.total,
                phase,
            ));
        }
    }

    pub fn finish(&mut self, summary: impl Into<String>) {
        let mut event = ProgressEvent::new(&self.action, self.current, self.total, "done");
        event.done = true;
        event.summary = Some(summary.into());
        self.sink.publish(&event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_handles_an_unknown_total() {
        assert_eq!(ProgressEvent::new("a", 0, 0, "x").percent(), None);
        assert_eq!(ProgressEvent::new("a", 5, 10, "x").percent(), Some(50));
        // Never over 100.
        assert_eq!(ProgressEvent::new("a", 20, 10, "x").percent(), Some(100));
    }

    #[test]
    fn vec_sink_collects_events() {
        let sink = Arc::new(VecSink::new());
        sink.publish(&ProgressEvent::new("a", 1, 2, "x"));
        assert_eq!(sink.events().len(), 1);
        assert_eq!(sink.last().expect("event").current, 1);
    }

    #[test]
    fn paced_reporter_limits_updates_but_always_reports_the_end() {
        let sink = Arc::new(VecSink::new());
        let context = JobContext::new(Arc::new(CrosstalkConfig::default()), sink.clone());
        let mut paced = context.paced("guide.match", 100, Duration::from_secs(60));
        for index in 0..10 {
            paced.set(index, "working");
        }
        // The first update goes out immediately, so a client that just joined sees the run
        // start; the rest of the loop is inside the interval and is suppressed.
        assert_eq!(sink.events().len(), 1);

        paced.finish("done");
        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert!(events[1].done);
        assert_eq!(events[1].summary.as_deref(), Some("done"));
    }

    #[test]
    fn cancellation_is_visible_to_the_action() {
        let context = JobContext::for_tests(CrosstalkConfig::default());
        assert!(!context.is_cancelled());
        context.cancel();
        assert!(context.is_cancelled());
    }

    #[test]
    fn a_run_without_a_deadline_never_pauses() {
        // A manual run: the same action code serves it, so `past_deadline` must answer false
        // rather than the caller having to know which kind of run it is in.
        let context = JobContext::for_tests(CrosstalkConfig::default());
        assert_eq!(context.deadline(), None);
        assert_eq!(context.remaining(), None);
        assert!(!context.past_deadline());
    }

    #[test]
    fn a_deadline_in_the_past_has_closed_and_one_ahead_has_not() {
        let context = JobContext::for_tests(CrosstalkConfig::default())
            .with_deadline(Some(chrono::Utc::now() + chrono::Duration::hours(1)));
        assert!(!context.past_deadline());
        assert!(context.remaining().expect("a remaining span").num_seconds() > 3_000);

        let closed = JobContext::for_tests(CrosstalkConfig::default())
            .with_deadline(Some(chrono::Utc::now() - chrono::Duration::seconds(1)));
        assert!(closed.past_deadline());
    }

    #[test]
    fn a_checkpoint_needs_a_recorded_run_to_live_in() {
        // Outside a run there is nowhere to keep one: reading is empty and saving is a no-op,
        // like `record_rows`, so a settings check cannot need a database.
        let context = JobContext::for_tests(CrosstalkConfig::default());
        assert!(context.load_checkpoint().is_none());
        context
            .save_checkpoint(&serde_json::json!({"done": 1}))
            .expect("a no-op, not an error");
        context.clear_checkpoint().expect("a no-op, not an error");
    }

    #[test]
    fn a_checkpoint_round_trips_through_the_context() {
        let store = crate::store::Store::in_memory().expect("store");
        let run_id = store.start_run("streams", "probe", false).expect("start");
        let context = JobContext::for_tests(CrosstalkConfig::default()).for_run(
            "streams",
            "probe",
            run_id,
            store.clone(),
            Arc::new(AtomicBool::new(false)),
        );
        assert_eq!(context.app(), Some("streams"));
        assert_eq!(context.action(), Some("probe"));
        assert!(context.load_checkpoint().is_none());

        context
            .save_checkpoint(&serde_json::json!({"remaining": [1, 2]}))
            .expect("save");
        assert_eq!(
            context.load_checkpoint(),
            Some(serde_json::json!({"remaining": [1, 2]}))
        );

        context.clear_checkpoint().expect("clear");
        assert!(context.load_checkpoint().is_none());
    }

    #[test]
    fn the_cancel_flag_is_the_runs_own() {
        // The limiter waits on it, so a cancelled probe does not sit out a spacing delay.
        let context = JobContext::for_tests(CrosstalkConfig::default());
        let flag = context.cancel_flag();
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
        context.cancel();
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn live_runs_are_visible_to_a_status_like_action() {
        // `status` is refused by the one-run-per-app guard, so it is the action that most needs
        // to see the run that is blocking it: the host hands the runner's own table to the
        // context for exactly this.
        let live: LiveRuns = Arc::new(Mutex::new(HashMap::new()));
        live.lock().expect("lock").insert(
            7,
            ActiveRun {
                run_id: 7,
                app: "guide".to_string(),
                action: "match_apply".to_string(),
                dry_run: false,
                current: 40,
                total: 100,
                phase: "validating candidates".to_string(),
                cancelling: false,
            },
        );
        let context =
            JobContext::for_tests(CrosstalkConfig::default()).with_live_runs(Arc::clone(&live));
        let runs = context.active_runs_for("guide");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].percent(), Some(40));
        assert_eq!(runs[0].phase, "validating candidates");
        assert!(context.active_runs_for("streams").is_empty());
        assert!(JobContext::for_tests(CrosstalkConfig::default())
            .active_runs_for("guide")
            .is_empty());
    }

    #[test]
    fn directories_come_from_the_config_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = CrosstalkConfig::load(dir.path()).expect("load");
        let context = JobContext::for_tests(config);
        assert_eq!(context.exports_dir(), dir.path().join("exports"));
    }
}
