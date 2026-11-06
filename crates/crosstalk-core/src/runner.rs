//! The job runner: one action at a time per sub-app, with progress, cancellation and history.
//!
//! A run is asynchronous and long — a preview over a few thousand channels takes seconds, an
//! apply with programme checks takes minutes — so the HTTP layer starts it and returns, and
//! the operator watches it through the progress stream or on the run page.
//!
//! The rules come from the plugin's own behaviour and are enforced here rather than left to
//! each app:
//!
//!   * one GUARDED run per app at a time, checked and reserved under ONE lock, because two
//!     auto-matches writing the same channels would race. A read-only status-like action is
//!     declared unguarded (`ActionSpec::unguarded`) and takes no slot: it is still recorded, so
//!     its answer lands on a run page, but it can run while another run of its app is live — that
//!     is exactly when an operator wants to see the blocking run's progress;
//!   * a cancelled run actually tells the action to stop, and is recorded as cancelled rather
//!     than as a success — keeping the counts, the report and the rows it produced;
//!   * a run that stops at its schedule window's end is recorded paused, distinct from both, so
//!     the next firing resumes it instead of an operator thinking work was lost or finished
//!     (see [`crate::jobs`] for the whole rule);
//!   * a panic in an action frees the app and records the failure, so one bad run cannot lock
//!     the daemon out of ever running that app again;
//!   * a workflow ([`crate::workflow`]) is one more run: it holds the reserved `workflow` slot
//!     (so two workflows never interleave), it is cancellable by its run id like any action, and
//!     its steps are ordinary runs of their own apps that point back at it.

use crate::config::{CrosstalkConfig, SharedConfig, WorkflowStepScope};
use crate::jobs::{JobContext, ProgressEvent, ProgressSink};
use crate::store::{RunStatus, Store};
use crate::subapp::{ActionKind, JobOutcome, OutcomeStatus, Registry};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
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

/// What a caller may change about how a run starts.
///
/// The defaults are a manual run: no deadline (so the action never pauses) and a resume from
/// whatever checkpoint the action left last time. A schedule adds the window's deadline; an
/// operator who wants the backlog from the beginning adds `fresh`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunOptions {
    /// The instant the run must stop and checkpoint: a schedule window's end.
    pub deadline: Option<DateTime<Utc>>,
    /// Ignore and discard saved progress.
    pub fresh: bool,
    /// The workflow run this run is a step of, when a workflow started it.
    ///
    /// Recorded on the run row (`parent_run_id`) so the workflow's run page can list its steps.
    /// Nothing else reads it: a step is an ordinary run in every other respect.
    pub parent_run: Option<i64>,
    /// A workflow step's scope override, replacing the host `[scope]` for this run.
    ///
    /// Set by the workflow executor from the step; every other caller leaves it unset.
    pub scope_override: Option<WorkflowStepScope>,
}

impl RunOptions {
    /// Bound the run by a window's end.
    pub fn with_deadline(mut self, deadline: Option<DateTime<Utc>>) -> Self {
        self.deadline = deadline;
        self
    }

    /// Start the backlog from the beginning, discarding any saved progress.
    pub fn fresh(mut self) -> Self {
        self.fresh = true;
        self
    }

    /// Record this run as a step of the given workflow run.
    pub fn with_parent(mut self, parent_run_id: i64) -> Self {
        self.parent_run = Some(parent_run_id);
        self
    }

    /// Replace the host `[scope]` for this run with a workflow step's scope override.
    pub fn with_scope_override(mut self, scope: Option<WorkflowStepScope>) -> Self {
        self.scope_override = scope;
        self
    }
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

/// How a finished action maps onto a recorded run: the one place every app's stopping rules are
/// applied (the rules themselves are documented in [`crate::jobs`]).
///
/// The counts, the summary and the report survive a cancel, so nothing that landed is hidden:
/// phase 1 recorded a cancelled run with the empty counts an `Err` carries, which made a
/// half-written apply read as "nothing happened".
fn recorded_outcome(
    outcome: &Result<JobOutcome>,
    cancelled: bool,
) -> (RunStatus, Option<String>, serde_json::Value, Option<String>) {
    match outcome {
        Ok(outcome) => {
            let status = match outcome.status {
                OutcomeStatus::Paused => RunStatus::Paused,
                // The action decided it was cancelled itself, which is what a job that stops on
                // request after a write returns.
                OutcomeStatus::Cancelled => RunStatus::Cancelled,
                // A run that says it failed is recorded failed, counts and all: the action's own
                // answer is the truth about why it stopped, exactly as it is for a pause.
                OutcomeStatus::Failed => RunStatus::Failed,
                // A cancel that arrived too late for the action to act on still means the
                // operator asked it to stop: record that, and keep everything it produced.
                OutcomeStatus::Succeeded if cancelled => RunStatus::Cancelled,
                OutcomeStatus::Succeeded => RunStatus::Succeeded,
            };
            (
                status,
                Some(outcome.summary.clone()),
                serde_json::to_value(&outcome.counts).unwrap_or_default(),
                outcome
                    .report_path
                    .as_ref()
                    .map(|path| path.display().to_string()),
            )
        }
        // A cancel is not a failure: the action stopped where the operator asked it to, and its
        // own message says where. Nothing landed, so there is nothing to count.
        Err(error) if cancelled => (
            RunStatus::Cancelled,
            Some(format!("{error:#}")),
            serde_json::json!({}),
            None,
        ),
        Err(error) => (
            RunStatus::Failed,
            Some(format!("{error:#}")),
            serde_json::json!({}),
            None,
        ),
    }
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
    /// lets two simultaneous requests both through. An unguarded action (a read-only status
    /// query) takes no slot at all, and is recorded as an unguarded run so the store's partial
    /// unique index — which covers guarded rows only — does not refuse it.
    fn reserve(
        &self,
        app_id: &str,
        action_id: &str,
        unguarded: bool,
        dry_run: bool,
        options: &RunOptions,
    ) -> Result<(i64, Arc<AtomicBool>)> {
        if self.is_shutting_down() {
            bail!("the daemon is shutting down; not starting new runs");
        }

        if unguarded {
            // No slot, and no busy entry: the whole point is that this may run beside another
            // run of the same app. The workflow link is kept: a `status` step is still a step.
            let run_id =
                self.store
                    .insert_run(app_id, action_id, dry_run, true, options.parent_run)?;
            if options.fresh {
                self.store.clear_checkpoint(app_id, action_id)?;
            }
            return Ok((run_id, self.track(run_id, app_id, action_id, dry_run)));
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
        let started = self
            .store
            .insert_run(app_id, action_id, dry_run, false, options.parent_run);
        let run_id = match started {
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

        if options.fresh {
            // Before the action reads anything: a fresh start must not find last night's place.
            self.store.clear_checkpoint(app_id, action_id)?;
        }
        Ok((run_id, self.track(run_id, app_id, action_id, dry_run)))
    }

    /// Register a started run's cancel flag and its active-run entry.
    fn track(&self, run_id: i64, app_id: &str, action_id: &str, dry_run: bool) -> Arc<AtomicBool> {
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
        flag
    }

    /// Start a run with the default options: no deadline, resume from a checkpoint.
    pub fn start(&self, app_id: &str, action_id: &str, dry_run: bool) -> Result<StartedRun> {
        self.start_with(app_id, action_id, dry_run, RunOptions::default())
    }

    /// Start a run and return immediately.
    ///
    /// The task owns everything it needs, so a dropped HTTP connection does not cancel the
    /// work: an apply that has begun should finish, which is what an operator expects after
    /// closing the tab.
    pub fn start_with(
        &self,
        app_id: &str,
        action_id: &str,
        dry_run: bool,
        options: RunOptions,
    ) -> Result<StartedRun> {
        let (app, spec) = self
            .registry
            .find_action(app_id, action_id)
            .with_context(|| format!("no action {action_id:?} in sub-app {app_id:?}"))?;

        let (run_id, cancel_flag) =
            self.reserve(app_id, spec.id, spec.unguarded, dry_run, &options)?;

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
            let context = runner.context_for(
                &started_app,
                &started_action,
                run_id,
                Arc::clone(&cancel_flag),
                sink.clone(),
                &options,
            );
            // Open the Dispatcharr session once per run, here rather than inside each sub-app, so
            // every action shares one client, one concurrency cap and one token.
            let context = if spec.needs_dispatcharr {
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
            let (status, summary, counts, report) = recorded_outcome(&outcome, cancelled);

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

    /// Run an action to completion with the default options.
    pub async fn run_to_completion(
        &self,
        app_id: &str,
        action_id: &str,
        dry_run: bool,
    ) -> Result<JobOutcome> {
        self.run_to_completion_with(app_id, action_id, dry_run, RunOptions::default())
            .await
    }

    /// Run an action to completion, for the one-shot CLI and tests.
    ///
    /// Takes the same reservation as [`JobRunner::start`], so a one-shot run and a scheduled
    /// one cannot write the same channels at once — unless the action is unguarded, which takes
    /// no slot for the same reason it does not in the daemon.
    pub async fn run_to_completion_with(
        &self,
        app_id: &str,
        action_id: &str,
        dry_run: bool,
        options: RunOptions,
    ) -> Result<JobOutcome> {
        let (app, spec) = self
            .registry
            .find_action(app_id, action_id)
            .with_context(|| format!("no action {action_id:?} in sub-app {app_id:?}"))?;

        let (run_id, cancel_flag) =
            self.reserve(app_id, spec.id, spec.unguarded, dry_run, &options)?;
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
        let context = self.context_for(
            app_id,
            action_id,
            run_id,
            Arc::clone(&cancel_flag),
            sink,
            &options,
        );
        let context = if spec.needs_dispatcharr {
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
        // The record is written from a copy, so the caller still gets the action's own answer.
        let (status, summary, counts, report) = recorded_outcome(&outcome, cancelled);
        slot.finish(status, summary.as_deref(), &counts, report.as_deref());
        self.prune_history();
        outcome
    }

    /// The checks every undo passes before it reserves the app's slot.
    ///
    /// A refused target ([`crate::undo::check_target`]) records no run at all, and the undo
    /// always links to the run it reverses: `parent_run_id` is the undone run, whatever the
    /// caller passed, because that link is what the run pages follow in both directions.
    fn check_undo(
        &self,
        app_id: &str,
        target_run_id: i64,
        options: RunOptions,
    ) -> Result<RunOptions> {
        let target = crate::undo::check_target(&self.store, &self.registry, target_run_id)?;
        if target.app != app_id {
            bail!(
                "run #{target_run_id} belongs to {:?}, not {app_id:?}: undo it through its \
                 own app",
                target.app
            );
        }
        Ok(options.with_parent(target_run_id))
    }

    /// Start an undo of one run, and return immediately.
    ///
    /// The host-level path for [`crate::undo::ACTION_UNDO`]: a reserved action every app gets
    /// without declaring it, so it never appears in schedules, workflows or hook targets.
    /// The run is recorded under the target run's app with `parent_run_id` set to the undone
    /// run, takes the app's run guard like any write, and is cancellable.
    pub fn start_undo(
        &self,
        app_id: &str,
        target_run_id: i64,
        dry_run: bool,
    ) -> Result<StartedRun> {
        self.start_undo_with(app_id, target_run_id, dry_run, RunOptions::default())
    }

    /// Start an undo run with explicit [`RunOptions`].
    ///
    /// The undo always links to the undone run (`parent_run_id`), replacing any parent in
    /// `options`.
    pub fn start_undo_with(
        &self,
        app_id: &str,
        target_run_id: i64,
        dry_run: bool,
        options: RunOptions,
    ) -> Result<StartedRun> {
        let options = self.check_undo(app_id, target_run_id, options)?;

        let (run_id, cancel_flag) =
            self.reserve(app_id, crate::undo::ACTION_UNDO, false, dry_run, &options)?;

        let runner = self.clone();
        let started_app = app_id.to_string();
        let registry = Arc::clone(&self.registry);
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
            let context = runner.context_for(
                &started_app,
                crate::undo::ACTION_UNDO,
                run_id,
                Arc::clone(&cancel_flag),
                sink.clone(),
                &options,
            );
            // An undo always writes channel data (or previews doing so), so it always
            // needs Dispatcharr.
            let context = match context.connect() {
                Ok(context) => context,
                Err(error) => {
                    slot.finish(
                        RunStatus::Failed,
                        Some(&format!("{error:#}")),
                        &serde_json::json!({}),
                        None,
                    );
                    runner.prune_history();
                    let mut event = ProgressEvent::new(crate::undo::ACTION_UNDO, 0, 0, "failed");
                    event.done = true;
                    event.summary = Some(format!("{error:#}"));
                    sink.publish(&event);
                    return;
                }
            };

            let outcome = crate::undo::run_undo(&registry, target_run_id, &context, dry_run).await;
            let cancelled = cancel_flag.load(Ordering::SeqCst);
            let (status, summary, counts, report) = recorded_outcome(&outcome, cancelled);

            slot.finish(status, summary.as_deref(), &counts, report.as_deref());
            runner.prune_history();

            let mut event = ProgressEvent::new(crate::undo::ACTION_UNDO, 0, 0, status.as_str());
            event.done = true;
            event.summary = summary;
            sink.publish(&event);
        });

        Ok(StartedRun {
            run_id,
            app: app_id.to_string(),
            action: crate::undo::ACTION_UNDO.to_string(),
            dry_run,
        })
    }

    /// Run an undo to completion, for the one-shot CLI and tests.
    ///
    /// Takes the same reservation as [`JobRunner::start_undo`], so a one-shot undo and a
    /// scheduled run cannot write the same channels at once.
    pub async fn run_undo_to_completion_with(
        &self,
        app_id: &str,
        target_run_id: i64,
        dry_run: bool,
        options: RunOptions,
    ) -> Result<JobOutcome> {
        let options = self.check_undo(app_id, target_run_id, options)?;

        let (run_id, cancel_flag) =
            self.reserve(app_id, crate::undo::ACTION_UNDO, false, dry_run, &options)?;
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
        let context = self.context_for(
            app_id,
            crate::undo::ACTION_UNDO,
            run_id,
            Arc::clone(&cancel_flag),
            sink,
            &options,
        );
        let context = match context.connect() {
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
        };

        let outcome = crate::undo::run_undo(&self.registry, target_run_id, &context, dry_run).await;
        let cancelled = cancel_flag.load(Ordering::SeqCst);
        let (status, summary, counts, report) = recorded_outcome(&outcome, cancelled);
        slot.finish(status, summary.as_deref(), &counts, report.as_deref());
        self.prune_history();
        outcome
    }

    /// The context one run gets.
    ///
    /// Snapshot at run start: a settings save while this run is in flight changes the next run,
    /// never this one. The same builder for the spawned and the blocking path, so a scheduled
    /// run and a one-shot run see the same deadline, checkpoint and live-run table.
    fn context_for(
        &self,
        app_id: &str,
        action_id: &str,
        run_id: i64,
        cancel_flag: Arc<AtomicBool>,
        sink: Arc<dyn ProgressSink>,
        options: &RunOptions,
    ) -> JobContext {
        let snapshot = Arc::new(self.config.snapshot());
        JobContext::new(snapshot, sink)
            .for_run(app_id, action_id, run_id, self.store.clone(), cancel_flag)
            .with_deadline(options.deadline)
            .with_live_runs(Arc::clone(&self.active))
            .with_scope_override(options.scope_override.clone())
            .with_registry(self.registry.as_ref().clone())
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

    /// Whether a run started by this runner is still in flight.
    ///
    /// The workflow executor watches its steps with this: a step is an ordinary spawned run, so
    /// "did it finish yet" is the live table, not a join handle the runner never exposes.
    /// A run from ANOTHER process is not listed here — but a workflow never starts one, so the
    /// answer is exact for the caller that matters.
    pub fn is_run_active(&self, run_id: i64) -> bool {
        self.active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(&run_id)
    }

    /// Wait for a run to finish, asking it to stop whenever `cancel` is set.
    ///
    /// Used by the workflow executor: the workflow's own cancel flag reaches its running step
    /// this way, so cancelling a workflow cancels the step it is inside rather than waiting for
    /// it to finish first. Polling rather than a channel because the run may also be cancelled
    /// from the UI under its own run id, and that path sets the same flag the runner already
    /// holds.
    pub async fn wait_for_run(&self, run_id: i64, cancel: &AtomicBool) {
        while self.is_run_active(run_id) {
            if cancel.load(Ordering::SeqCst) {
                self.cancel(run_id);
            }
            tokio::time::sleep(RUN_POLL).await;
        }
    }

    /// Take the host's `workflow` slot and register the new run's cancel flag, live entry and
    /// progress sink.
    ///
    /// The workflow executor ([`crate::workflow`]) owns the step loop; this is the bookkeeping
    /// that makes a workflow a run like any other: one at a time (they would contend for the
    /// same apps anyway), cancellable by its run id from the UI or the API, and visible in the
    /// live table while it works.
    pub fn reserve_workflow(
        &self,
        name: &str,
        dry_run: bool,
        options: &RunOptions,
    ) -> Result<WorkflowSlot> {
        let (run_id, cancel) =
            self.reserve(crate::workflow::APP_ID, name, false, dry_run, options)?;
        let sink = Arc::new(BroadcastSink {
            sender: self.sender.clone(),
            active: Arc::clone(&self.active),
            run_id,
        });
        Ok(WorkflowSlot {
            run_id,
            name: name.to_string(),
            cancel,
            sink,
            slot: Some(RunSlot {
                run_id,
                app: crate::workflow::APP_ID.to_string(),
                store: self.store.clone(),
                active: Arc::clone(&self.active),
                busy: Arc::clone(&self.busy),
                finish: Arc::clone(&self.finish),
                finished: false,
            }),
            runner: self.clone(),
        })
    }
}

/// How often [`JobRunner::wait_for_run`] re-reads the live run table.
///
/// Short enough that cancelling a workflow stops its step promptly, long enough that a four-hour
/// probe costs a few hundred thousand lock acquisitions rather than millions.
const RUN_POLL: std::time::Duration = std::time::Duration::from_millis(25);

/// A started workflow's host-side bookkeeping (see [`JobRunner::reserve_workflow`]).
///
/// Dropping one without [`WorkflowSlot::finish`] records the run as failed, exactly as an
/// action's slot does: a workflow whose task was torn down must not leave a `running` row
/// behind claiming it is still working.
pub struct WorkflowSlot {
    run_id: i64,
    /// The workflow's name, for the progress events and the drop guard's failure message.
    name: String,
    cancel: Arc<AtomicBool>,
    sink: Arc<BroadcastSink>,
    /// `None` once the outcome is recorded, so the drop guard cannot record a second one.
    slot: Option<RunSlot>,
    runner: JobRunner,
}

impl WorkflowSlot {
    /// The workflow run's id, for the step runs' parent link and the redirect after a start.
    pub fn run_id(&self) -> i64 {
        self.run_id
    }

    /// The workflow's own cancel flag, shared with the runner's table.
    pub fn cancel_flag(&self) -> &AtomicBool {
        self.cancel.as_ref()
    }

    /// Whether an operator (or a shutdown) asked this workflow to stop.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }

    /// Report progress: which step is running, out of how many.
    pub fn progress(&self, event: &ProgressEvent) {
        self.sink.publish(event);
    }

    /// Record the outcome and free the `workflow` slot, then prune history like any other run.
    pub fn finish(mut self, outcome: &Result<JobOutcome>) {
        let cancelled = self.is_cancelled();
        let (status, summary, counts, report) = recorded_outcome(outcome, cancelled);
        if let Some(slot) = self.slot.take() {
            slot.finish(status, summary.as_deref(), &counts, report.as_deref());
        }
        self.runner.prune_history();

        let mut event = ProgressEvent::new(&self.name, 0, 0, status.as_str());
        event.done = true;
        event.summary = summary;
        self.sink.publish(&event);
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
        .for_run(
            "test",
            "run",
            run_id,
            store.clone(),
            Arc::new(AtomicBool::new(false)),
        );
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

    /// A runner over the resumable test app, on a real clock: pausing depends on real elapsed
    /// time, so the deadline tests below use short work units rather than a paused clock.
    fn resumable_runner(store: Store) -> JobRunner {
        JobRunner::new(
            Registry::new(vec![Arc::new(crate::test_app::ResumableApp::new())]),
            SharedConfig::new(CrosstalkConfig::default()),
            store,
        )
    }

    #[tokio::test]
    async fn a_windowed_run_pauses_at_its_deadline_and_the_next_run_resumes() {
        use crate::store::RunRow;
        use crate::test_app::{RESUMABLE_PER_UNIT, RESUMABLE_UNITS};

        let store = Store::in_memory().expect("store");
        let runner = resumable_runner(store.clone());

        // A deadline a few units in: the run pauses with a partial backlog.
        let deadline = Utc::now() + chrono::Duration::milliseconds(45);
        let first = runner
            .run_to_completion_with(
                "resumable",
                "run",
                false,
                RunOptions::default().with_deadline(Some(deadline)),
            )
            .await
            .expect("a paused run is not an error: it did what its window allowed");

        assert!(first.is_paused(), "{}", first.summary);
        let done = first.counts["done"];
        assert!(
            done > 0 && done < RESUMABLE_UNITS,
            "paused between units, got {done} of {RESUMABLE_UNITS}"
        );

        let runs = store.recent_runs_for("resumable", 1).expect("runs");
        assert_eq!(runs[0].status, RunStatus::Paused);
        assert_eq!(
            runs[0].counts["done"], done,
            "the partial count is recorded"
        );
        assert_eq!(
            store.rows(runs[0].id).expect("rows").len() as i64,
            done,
            "the rows that landed are recorded"
        );
        assert!(
            store
                .rows(runs[0].id)
                .expect("rows")
                .iter()
                .any(|row: &RunRow| row.status == "done"),
            "each finished unit left its row"
        );
        assert_eq!(
            store.load_checkpoint("resumable", "run").expect("read"),
            Some(serde_json::json!({ "done": done })),
            "the checkpoint is what the next window resumes from"
        );

        // The next firing resumes: the total is the whole backlog, not another partial.
        let second = runner
            .run_to_completion_with(
                "resumable",
                "run",
                false,
                RunOptions::default()
                    .with_deadline(Some(Utc::now() + chrono::Duration::seconds(30))),
            )
            .await
            .expect("resumed");
        assert!(!second.is_paused(), "{}", second.summary);
        assert_eq!(second.counts.get("resumed"), Some(&1), "{}", second.summary);
        assert_eq!(second.counts["done"], RESUMABLE_UNITS);
        let runs = store.recent_runs_for("resumable", 1).expect("runs");
        assert_eq!(runs[0].status, RunStatus::Succeeded);
        assert_eq!(
            store.load_checkpoint("resumable", "run").expect("read"),
            None,
            "a finished backlog clears its checkpoint"
        );
        let _ = RESUMABLE_PER_UNIT;
    }

    #[tokio::test]
    async fn a_manual_run_has_no_deadline_and_never_pauses() {
        let store = Store::in_memory().expect("store");
        let runner = resumable_runner(store);
        let outcome = runner
            .run_to_completion("resumable", "run", false)
            .await
            .expect("finishes");
        assert!(!outcome.is_paused());
        assert_eq!(outcome.counts["done"], crate::test_app::RESUMABLE_UNITS);
    }

    #[tokio::test]
    async fn a_fresh_run_discards_a_saved_checkpoint() {
        let store = Store::in_memory().expect("store");
        let runner = resumable_runner(store.clone());
        let paused = runner
            .run_to_completion_with(
                "resumable",
                "run",
                false,
                RunOptions::default().with_deadline(Some(Utc::now())),
            )
            .await
            .expect("pauses immediately");
        assert!(paused.is_paused());
        assert_eq!(paused.counts["done"], 0, "the deadline had already passed");

        // Leave progress behind, then ask for a fresh start.
        store
            .save_checkpoint("resumable", "run", &serde_json::json!({"done": 7}))
            .expect("seed");
        let fresh = runner
            .run_to_completion_with("resumable", "run", false, RunOptions::default().fresh())
            .await
            .expect("finishes");
        assert_eq!(
            fresh.counts["done"],
            crate::test_app::RESUMABLE_UNITS,
            "a fresh run works the whole backlog instead of resuming at 7: {}",
            fresh.summary
        );
        assert!(
            !fresh.counts.contains_key("resumed"),
            "fresh means no resume: {}",
            fresh.summary
        );
    }

    #[tokio::test]
    async fn cancelling_mid_apply_is_recorded_cancelled_with_what_landed() {
        use crate::test_app::RESUMABLE_PER_UNIT;

        let store = Store::in_memory().expect("store");
        let runner = resumable_runner(store.clone());
        let started = runner.start("resumable", "run", false).expect("started");
        // Long enough for a few units, short enough that the backlog is not finished.
        tokio::time::sleep(RESUMABLE_PER_UNIT * 3).await;
        assert!(runner.cancel(started.run_id));
        wait_until_app_idle(&runner, "resumable").await;

        let run = store.run(started.run_id).expect("run").expect("present");
        assert_eq!(
            run.status,
            RunStatus::Cancelled,
            "a cancelled run is never recorded as a success: {:?}",
            run.summary
        );
        let done = store.rows(run.id).expect("rows").len() as i64;
        assert!(done > 0, "the units that landed left their rows");
        assert!(
            run.summary
                .as_deref()
                .unwrap_or_default()
                .contains("cancelled"),
            "the summary says where it stopped: {:?}",
            run.summary
        );
        // Cancelling keeps the place, so the next window continues rather than starting over.
        assert!(
            store
                .load_checkpoint("resumable", "run")
                .expect("read")
                .is_some(),
            "a cancel keeps the checkpoint"
        );
    }

    #[test]
    fn a_finished_action_during_a_cancel_is_recorded_cancelled_with_its_counts() {
        // The cancel arrived too late for the action to act on: the operator still asked it to
        // stop, so the run says cancelled — and the counts and summary it produced survive, so
        // the history does not hide what landed (the phase-1 behaviour did).
        let outcome: Result<JobOutcome> = Ok(JobOutcome::new("Sorted 5 channel(s)")
            .with_count("written", 5)
            .with_count("failed", 0));
        let (status, summary, counts, _) = recorded_outcome(&outcome, true);
        assert_eq!(status, RunStatus::Cancelled);
        assert_eq!(summary.as_deref(), Some("Sorted 5 channel(s)"));
        assert_eq!(counts["written"], 5);

        // The same outcome without a cancel is a success.
        assert_eq!(recorded_outcome(&outcome, false).0, RunStatus::Succeeded);
    }

    #[test]
    fn a_paused_outcome_is_recorded_paused_and_an_explicit_cancel_as_cancelled() {
        let paused: Result<JobOutcome> =
            Ok(JobOutcome::paused("paused after 120 of 900").with_count("done", 120));
        let (status, summary, counts, _) = recorded_outcome(&paused, false);
        assert_eq!(status, RunStatus::Paused);
        assert_eq!(summary.as_deref(), Some("paused after 120 of 900"));
        assert_eq!(counts["done"], 120);
        // A paused run is not "cancelled" just because a flag was also set: the action's own
        // answer is the truth about why it stopped.
        assert_eq!(recorded_outcome(&paused, true).0, RunStatus::Paused);

        let cancelled: Result<JobOutcome> =
            Ok(JobOutcome::new("stopped").with_status(OutcomeStatus::Cancelled));
        assert_eq!(recorded_outcome(&cancelled, false).0, RunStatus::Cancelled);
    }

    #[test]
    fn an_error_during_a_cancel_is_cancelled_not_failed_and_names_where_it_stopped() {
        let failed: Result<JobOutcome> =
            Err(anyhow::anyhow!("cancelled by the operator at unit 4"));
        let (status, summary, _counts, _) = recorded_outcome(&failed, true);
        assert_eq!(status, RunStatus::Cancelled);
        assert!(summary.unwrap_or_default().contains("unit 4"));

        // Without a cancel the same error is a failure, unchanged.
        assert_eq!(recorded_outcome(&failed, false).0, RunStatus::Failed);
    }

    #[tokio::test]
    async fn an_unguarded_action_runs_beside_a_live_run_and_takes_no_slot() {
        use crate::test_app::RESUMABLE_PER_UNIT;

        let store = Store::in_memory().expect("store");
        let runner = resumable_runner(store.clone());
        let live = runner.start("resumable", "run", false).expect("started");
        assert!(runner.is_busy("resumable"));

        // The status-like action is the one that must not be refused: it is what tells an
        // operator what the blocking run is doing.
        let status = runner
            .run_to_completion("resumable", "status", true)
            .await
            .expect("an unguarded action runs while another does");
        assert_eq!(status.summary, "ok");
        // It is recorded, so its answer has a run page of its own…
        let records = store.recent_runs_for("resumable", 10).expect("runs");
        assert!(
            records.iter().any(|run| run.action == "status"),
            "the unguarded run is recorded"
        );
        // …and it holds no slot, so it is not reported as "already running".
        let running = store.running_runs("resumable").expect("running");
        assert_eq!(running.len(), 1);
        assert_eq!(
            running[0].id, live.run_id,
            "only the guarded run holds the app"
        );

        wait_until_app_idle(&runner, "resumable").await;
        let _ = RESUMABLE_PER_UNIT;
    }

    /// Wait until this app has nothing in flight.
    async fn wait_until_app_idle(runner: &JobRunner, app: &str) {
        for _ in 0..400 {
            if !runner.is_busy(app) && runner.running().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the runner never went idle for {app}");
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
