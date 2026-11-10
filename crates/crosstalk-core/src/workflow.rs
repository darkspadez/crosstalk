//! Workflows: named step sequences the host runs in order.
//!
//! A workflow is what the plugins did by calling into each other. Stream-Mapparr's scheduled
//! sequence (`probe → sort_apply`, or IPTV Checker's scan followed by its own post-actions) and
//! its `on_m3u_refresh` hand-off were both "run this action when that one finished"; the port
//! makes the sequence a configuration entry the operator owns:
//!
//! A workflow step is usually a bare `"<app>.<action>"` string. A step that must read a
//! different set of channels than the host `[scope]` carries a table instead:
//!
//! ```toml
//! [workflows.full_sync]
//! steps = [
//!   "lineup.sync_apply",
//!   { action = "streams.match_apply", scope = "lineup" },
//!   { action = "guide.match_apply", scope = { groups = "DIRECTV: *" }, continue_on_failure = true },
//! ]
//! ```
//!
//! TOML forbids a newline inside an inline table, so a step too long for one line is written
//! as an array of tables instead (`[[workflows.full_sync.steps]]`, one `action = ...` table
//! per step; every step is then a table).
//!
//! `scope = "lineup"` is a NAMED scope: the channels one app offers under that name,
//! resolved through the registry at run time (a table there would pin the step to today's
//! group names; a name follows the app). `scope = { ... }` is an inline scope table that
//! REPLACES the host `[scope]` for that step. A named scope yielding channel ids REPLACES
//! the host scope too: the step reads exactly those channels. `continue_on_failure` lets
//! the workflow carry on past a failed match/guide/logo step (a sync step failing still
//! stops); the workflow itself is recorded failed when any step failed, with each step's
//! outcome in the summary.
//!
//! A step's scope comes from the configuration only: no hook, schedule or web request can
//! hand a run a scope override. A named scope REPLACES the host `[scope]`, so it may reach
//! channels `[scope]` excludes; that is the operator's choice, made by writing the step.
//!
//! # What running one means
//!
//! * Every step is an ordinary run: it takes its own app's one-run-per-app slot, gets its own
//!   row in the run store, its own run page and its own CSV. `probe` writes exactly what a
//!   scheduled `probe` writes, which is the point — there is no second execution path to keep
//!   in step with the first.
//! * The workflow itself is one more run record, under the reserved app id `workflow` and the
//!   workflow's name as its action, and each step row points at it (`runs.parent_run_id`). So
//!   "what did last night do" is one page: the workflow, and the steps it ran.
//! * Steps run sequentially and stop at the first that fails or is cancelled — unless the
//!   failed step carries `continue_on_failure`, which records the failure and moves on to
//!   the next step. A step that PAUSED at its schedule window's end pauses the whole
//!   workflow (pausing and cancelling never continue, whatever the flag says): the
//!   checkpoint names the step, and the next firing resumes THAT step — with the step's
//!   own checkpoint picking up its backlog — and continues from there. A workflow that
//!   carried on past failures is recorded failed, with each step's outcome in the summary.
//! * A workflow holds no app BETWEEN its steps, so a hook or a schedule may start a run of the
//!   next step's app in the gap. The step then waits for that run to finish (at most 30
//!   minutes, never past the window, and not once the workflow is cancelled) rather than
//!   failing a sequence whose probe already ran.
//! * The window's deadline reaches every step, so a workflow inside `{ cron = "0 23 * * *",
//!   window = "4h" }` never runs past 03:00 by accident.
//! * Cancelling the workflow cancels the step it is inside (see
//!   [`JobRunner::wait_for_run`](crate::runner::JobRunner::wait_for_run)); the workflow is then
//!   recorded cancelled, keeping what its finished steps did.
//!
//! # What it does not do
//!
//! No branching, no per-step settings beyond scope and the failure policy, no nesting:
//! `[workflows.<name>] steps` may only name `<app>.<action>` actions. A step naming a workflow
//! is a configuration error ([`crate::config::CrosstalkConfig::validate`]), because a cycle
//! would be a hang with no progress and no way out but a restart.

use crate::config::{CrosstalkConfig, ScopeConfig, WorkflowConfig, WorkflowStepScope};
use crate::jobs::ProgressEvent;
use crate::runner::{JobRunner, RunOptions, StartedRun};
use crate::store::{RunStatus, Store};
use crate::subapp::{JobOutcome, OutcomeBuilder, Registry};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde_json::json;

/// The reserved app id a workflow run is recorded under.
///
/// `workflow` is reserved: no sub-app may register it, which is what makes `workflow.<name>` an
/// unambiguous target and schedule key (see [`crate::target`]).
pub const APP_ID: &str = "workflow";

/// One step: `<app>.<action>`, never another workflow, plus what the step may touch and
/// whether the workflow continues past its failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub app: String,
    pub action: String,
    /// Where the step reads its channels from instead of the host `[scope]`.
    pub scope: Option<WorkflowStepScope>,
    /// Carry on past this step when it fails. A cancelled or paused step still stops the
    /// workflow; the workflow itself is recorded failed when any step failed.
    pub continue_on_failure: bool,
}

impl Step {
    /// Parse a bare `"<app>.<action>"` step. A `workflow.<name>` step is refused here rather
    /// than at run time.
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        if text.starts_with("workflow.") {
            bail!(
                "{text:?} is a workflow, and a workflow step must be an <app>.<action>: \
                 workflows do not nest"
            );
        }
        let (app, action) = text
            .split_once('.')
            .with_context(|| format!("workflow step {text:?} must be <app>.<action>"))?;
        if app.is_empty() || action.is_empty() {
            bail!("workflow step {text:?} must be <app>.<action>");
        }
        Ok(Self {
            app: app.to_string(),
            action: action.to_string(),
            scope: None,
            continue_on_failure: false,
        })
    }

    /// Build a step from one `steps` entry, naming the bad step by its index.
    ///
    /// `Workflow::from_entry` wraps this with the workflow's name, so a ten-step workflow's
    /// typo reads `workflow "nightly": step 2 ...` rather than a bare action error.
    pub fn from_entry(index: usize, entry: &crate::config::WorkflowStepEntry) -> Result<Self> {
        let mut step = Self::parse(entry.action())
            .with_context(|| format!("step {} ({})", index + 1, entry.display()))?;
        if let Some(scope) = entry.scope() {
            if let WorkflowStepScope::Table(scope) = scope {
                validate_step_scope(index, entry, scope)?;
            }
            if let WorkflowStepScope::Named(name) = scope {
                if name.trim().is_empty() {
                    bail!(
                        "step {} ({}): the scope name is empty",
                        index + 1,
                        entry.display()
                    );
                }
            }
            step.scope = Some(scope.clone());
        }
        step.continue_on_failure = entry.continue_on_failure();
        Ok(step)
    }

    /// The step as the operator wrote it.
    pub fn label(&self) -> String {
        format!("{}.{}", self.app, self.action)
    }

    /// The step as the dashboard, the run page and the CLI show it: the action plus its
    /// scope override and failure policy, when it carries any.
    pub fn describe(&self) -> String {
        let mut text = self.label();
        if let Some(scope) = &self.scope {
            text.push_str(&format!(" [{}]", scope.describe()));
        }
        if self.continue_on_failure {
            text.push_str(" [continue on failure]");
        }
        text
    }
}

/// Refuse a step table whose inline scope contradicts itself, naming the bad step.
fn validate_step_scope(
    index: usize,
    entry: &crate::config::WorkflowStepEntry,
    scope: &ScopeConfig,
) -> Result<()> {
    if !scope.groups.trim().is_empty() && !scope.ignore_groups.trim().is_empty() {
        bail!(
            "step {} ({}): scope.groups and scope.ignore_groups cannot both be set: use one \
             or the other",
            index + 1,
            entry.display()
        );
    }
    Ok(())
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label())
    }
}

/// One workflow, resolved from the configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workflow {
    pub name: String,
    pub steps: Vec<Step>,
    /// Discard each step's saved progress when a new pass starts (see [`WorkflowConfig`]).
    pub fresh: bool,
}

impl Workflow {
    /// Resolve `name` from `config`, reporting a missing entry with the names that do exist.
    pub fn from_config(config: &CrosstalkConfig, name: &str) -> Result<Self> {
        let entry = config.workflows.get(name).with_context(|| {
            let known: Vec<&str> = config.workflows.keys().map(String::as_str).collect();
            if known.is_empty() {
                format!("no workflow {name:?}: [workflows] is empty")
            } else {
                format!("no workflow {name:?}; configured: {}", known.join(", "))
            }
        })?;
        Self::from_entry(name, entry)
    }

    /// Resolve one `[workflows.<name>]` entry into steps.
    pub fn from_entry(name: &str, entry: &WorkflowConfig) -> Result<Self> {
        if entry.steps.is_empty() {
            bail!("workflow {name:?} has no steps");
        }
        let steps = entry
            .steps
            .iter()
            .enumerate()
            .map(|(index, step)| {
                Step::from_entry(index, step).with_context(|| format!("workflow {name:?}"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            name: name.to_string(),
            steps,
            fresh: entry.fresh,
        })
    }

    /// The steps as written, for a template that lists them.
    pub fn labels(&self) -> Vec<String> {
        self.steps.iter().map(Step::label).collect()
    }
}

/// One workflow as the dashboard lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowInfo {
    pub name: String,
    /// The steps as written. Empty when the entry does not parse, which `valid` reports.
    pub steps: Vec<String>,
    /// Each step with its scope override and failure policy, for the steps table.
    pub details: Vec<StepInfo>,
    pub fresh: bool,
    /// Why the entry cannot run, if it cannot. The dashboard shows it rather than offering a
    /// Run button that would fail.
    pub error: Option<String>,
}

/// One workflow step as the dashboard and the run page list it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepInfo {
    /// The `<app>.<action>` the step runs.
    pub action: String,
    /// How the step's scope override reads (`scope "lineup"`, `scope groups ...`), if any.
    pub scope: Option<String>,
    /// Whether the workflow continues past this step's failure.
    pub continue_on_failure: bool,
}

impl StepInfo {
    fn of(step: &Step) -> Self {
        Self {
            action: step.label(),
            scope: step.scope.as_ref().map(|scope| scope.describe()),
            continue_on_failure: step.continue_on_failure,
        }
    }

    /// How the step reads in a steps table: the action plus its override and policy.
    pub fn describe(&self) -> String {
        let mut text = self.action.clone();
        if let Some(scope) = &self.scope {
            text.push_str(&format!(" [{scope}]"));
        }
        if self.continue_on_failure {
            text.push_str(" [continue on failure]");
        }
        text
    }
}

/// Every configured workflow, in name order, for the UI and `crosstalk validate`.
pub fn describe(config: &CrosstalkConfig) -> Vec<WorkflowInfo> {
    config
        .workflows
        .iter()
        .map(|(name, entry)| match Workflow::from_entry(name, entry) {
            Ok(workflow) => WorkflowInfo {
                name: name.clone(),
                steps: workflow.labels(),
                details: workflow.steps.iter().map(StepInfo::of).collect(),
                fresh: workflow.fresh,
                error: None,
            },
            Err(error) => WorkflowInfo {
                name: name.clone(),
                steps: entry.steps.iter().map(|step| step.display()).collect(),
                details: Vec::new(),
                fresh: entry.fresh,
                error: Some(format!("{error:#}")),
            },
        })
        .collect()
}

/// Resolve a workflow from the live configuration AND the registry.
///
/// Every step must name an action this build registers, checked BEFORE the run record is
/// created: a typo is a configuration error the operator can fix, not a run that failed. (The
/// step loop re-checks anyway, because that is where the action is looked up.)
pub fn resolve(runner: &JobRunner, name: &str) -> Result<Workflow> {
    let workflow = Workflow::from_config(&runner.config_snapshot(), name)?;
    for step in &workflow.steps {
        if runner
            .registry()
            .find_action(&step.app, &step.action)
            .is_none()
        {
            bail!("workflow {name:?}: step {step:?} names no action this build registers");
        }
        // A named scope no app provides would only fail its step at run time; refusing it
        // here keeps a typo (or a scope whose app is not built in) a configuration error.
        if let Some(WorkflowStepScope::Named(scope)) = &step.scope {
            if !runner.registry().provides_named_scope(scope) {
                bail!(
                    "workflow {name:?}: step {} names the scope {:?}, which no app in this \
                     build provides",
                    step.label(),
                    scope.trim()
                );
            }
        }
    }
    Ok(workflow)
}

/// Whether a scheduled or triggered run of this workflow would change nothing.
///
/// The same rule a single action gets: a workflow of read-only steps is a dry run, one with a
/// writing step is not. An unknown workflow answers `true` (a dry run), which is the harmless
/// reading of a typo; validation reports the typo itself.
pub fn scheduled_dry_run(registry: &Registry, config: &CrosstalkConfig, name: &str) -> bool {
    let Some(entry) = config.workflows.get(name) else {
        return true;
    };
    match Workflow::from_entry(name, entry) {
        Ok(workflow) => workflow.steps.iter().all(|step| {
            registry
                .find_action(&step.app, &step.action)
                .is_none_or(|(_app, action)| !action.kind.touches_channels())
        }),
        Err(_) => true,
    }
}

/// Start a workflow run and return immediately.
///
/// The reservation happens here, synchronously, so a caller (the web UI, a hook, the scheduler)
/// learns about a busy workflow slot or an unknown name as a normal error instead of a failure
/// that only shows up on a run page later.
pub fn start(
    runner: &JobRunner,
    name: &str,
    dry_run: bool,
    options: RunOptions,
) -> Result<StartedRun> {
    // The configuration snapshot decides the steps, exactly as it decides a single action's
    // behaviour: a settings save mid-run changes the next run, never this one.
    let workflow = resolve(runner, name)?;
    let slot = runner.reserve_workflow(name, dry_run, &options)?;
    let started = StartedRun {
        run_id: slot.run_id(),
        app: APP_ID.to_string(),
        action: name.to_string(),
        dry_run,
    };

    let runner = runner.clone();
    tokio::spawn(async move {
        let outcome = execute(&runner, &workflow, dry_run, &options, &slot).await;
        slot.finish(&outcome);
    });
    Ok(started)
}

/// Run a workflow to completion, for the one-shot CLI and tests.
///
/// Takes the same `workflow` slot as [`start`], so a one-shot run and a scheduled one cannot run
/// the same steps at once.
pub async fn run_to_completion(
    runner: &JobRunner,
    name: &str,
    dry_run: bool,
    options: RunOptions,
) -> Result<JobOutcome> {
    let workflow = resolve(runner, name)?;
    let slot = runner.reserve_workflow(name, dry_run, &options)?;
    let outcome = execute(runner, &workflow, dry_run, &options, &slot).await;
    // The record is written from a borrow, so the caller still gets the workflow's own answer.
    slot.finish(&outcome);
    outcome
}

/// The step loop: run each step, stopping at the first that fails or is cancelled, pausing when
/// one pauses.
async fn execute(
    runner: &JobRunner,
    workflow: &Workflow,
    dry_run: bool,
    options: &RunOptions,
    slot: &crate::runner::WorkflowSlot,
) -> Result<JobOutcome> {
    let store = runner.store();
    let name = workflow.name.as_str();
    let total = workflow.steps.len() as u64;

    // Where a previous pass stopped. A paused workflow saved the step it was in, so tonight
    // continues there instead of re-running hours of probing to reach it.
    let resumed_from = resume_index(store, workflow, options)?;
    let fresh_pass = options.fresh || (workflow.fresh && resumed_from == 0);
    if resumed_from > 0 {
        tracing::info!(
            workflow = name,
            step = resumed_from,
            "resuming the workflow at its saved step"
        );
    }

    let mut done = 0i64;
    let mut failed = 0i64;
    // Every step's outcome, for the summary of a workflow that carried on past failures:
    // `lab.ok succeeded`, `lab.fail FAILED (reason)`. The run page lists the same steps
    // with their statuses; the summary carries them so the dashboard's last-run line does.
    let mut outcomes: Vec<String> = Vec::new();
    // The failed steps this pass carried past, so a pass that then pauses, is cancelled or
    // is refused still says so (the resumed pass starts a fresh tally).
    let mut failures: Vec<String> = Vec::new();
    for (index, step) in workflow.steps.iter().enumerate().skip(resumed_from) {
        // The operator's cancel and the window's end are the two ways a workflow stops between
        // steps; both keep the place so the next pass continues rather than starts over.
        if slot.is_cancelled() {
            save_step(store, name, index)?;
            return Ok(OutcomeBuilder::new(format!(
                "stopped before step {} of {total}{}",
                index + 1,
                carried_past(&failures)
            ))
            .cancelled()
            .count("steps_run", done)
            .count("steps_failed", failed)
            .count("steps_planned", total as i64)
            .build());
        }
        if options
            .deadline
            .is_some_and(|deadline| Utc::now() >= deadline)
        {
            save_step(store, name, index)?;
            return Ok(OutcomeBuilder::new(format!(
                "paused before step {} of {total} ({}); the next window resumes there{}",
                index + 1,
                step.describe(),
                carried_past(&failures)
            ))
            .paused()
            .count("steps_run", done)
            .count("steps_failed", failed)
            .count("steps_planned", total as i64)
            .build());
        }

        let (step_action, step_dry) = resolve_step(runner.registry(), step, dry_run)?;
        let step_options = RunOptions {
            deadline: options.deadline,
            fresh: fresh_pass,
            parent_run: Some(slot.run_id()),
            // The step's channel universe travels with the step: the action resolves it
            // through its context (see `JobContext::scoped_channels`).
            scope_override: step.scope.clone(),
            // A workflow step never carries a free-text input: inputs are an operator's
            // one-shot question to a single action, not a scheduled step's parameter.
            input: None,
        };
        slot.progress(&ProgressEvent::new(
            name,
            index as u64,
            total,
            format!("running {}", step.describe()),
        ));

        let started = match start_step(
            runner,
            slot,
            options,
            step,
            &step_action,
            step_dry,
            step_options,
        )
        .await
        {
            Ok(started) => started,
            Err(error) => {
                // A refused start (the app is busy, or the step names nothing runnable) is this
                // run's failure: the workflow's contract is a sequence, and a sequence with a
                // hole in it is not one. Nothing is checkpointed, so the next pass starts over.
                // Refusing is not a step outcome, so even `continue_on_failure` does not carry
                // past it: no run exists to say what the step did.
                let _ = store.clear_checkpoint(APP_ID, name);
                return Ok(OutcomeBuilder::new(format!(
                    "stopped at step {} of {total} ({}): {error:#}{}",
                    index + 1,
                    step.describe(),
                    carried_past(&failures)
                ))
                .failed()
                .count("steps_run", done)
                .count("steps_failed", failed)
                .count("steps_planned", total as i64)
                .build());
            }
        };

        runner
            .wait_for_run(started.run_id, slot.cancel_flag())
            .await;

        let record = store
            .run(started.run_id)?
            .with_context(|| format!("step run {} disappeared from the store", started.run_id))?;
        match record.status {
            RunStatus::Succeeded => {
                done += 1;
                outcomes.push(format!("{} succeeded", step.describe()));
                tracing::info!(
                    workflow = name,
                    step = %step,
                    run_id = started.run_id,
                    "workflow step finished"
                );
            }
            RunStatus::Paused => {
                // The step's own window ended mid-backlog. Pausing the workflow here is what
                // makes last night's progress continue tonight instead of being redone.
                // Pausing never continues, whatever the step's failure policy says: the
                // backlog is not a failure, and skipping it would lose work.
                save_step(store, name, index)?;
                return Ok(OutcomeBuilder::new(format!(
                    "paused at step {} of {total} ({}): {}; the next window resumes there{}",
                    index + 1,
                    step.describe(),
                    record.summary.as_deref().unwrap_or("the step paused"),
                    carried_past(&failures)
                ))
                .paused()
                .count("steps_run", done)
                .count("steps_failed", failed)
                .count("steps_planned", total as i64)
                .build());
            }
            RunStatus::Cancelled => {
                // Cancelling never continues either: the operator asked this workflow to stop,
                // and running more steps after that would do the opposite.
                save_step(store, name, index)?;
                return Ok(OutcomeBuilder::new(format!(
                    "stopped at step {} of {total} ({}): {}{}",
                    index + 1,
                    step.describe(),
                    record
                        .summary
                        .as_deref()
                        .unwrap_or("the step was cancelled"),
                    carried_past(&failures)
                ))
                .cancelled()
                .count("steps_run", done)
                .count("steps_failed", failed)
                .count("steps_planned", total as i64)
                .build());
            }
            RunStatus::Failed | RunStatus::Running => {
                // `Running` cannot happen once the run left the live table, but recording it as
                // a failure is the honest reading rather than claiming the sequence completed.
                let reason = record
                    .summary
                    .as_deref()
                    .unwrap_or("the step failed")
                    .to_string();
                if step.continue_on_failure {
                    // A failed step the operator flagged: record it and move on. The workflow
                    // itself still ends failed (see below), so no failure is hidden.
                    failed += 1;
                    outcomes.push(format!("{} FAILED ({reason})", step.describe()));
                    failures.push(step.describe());
                    tracing::warn!(
                        workflow = name,
                        step = %step,
                        run_id = started.run_id,
                        reason = reason.as_str(),
                        "workflow step failed; continuing past it"
                    );
                    continue;
                }
                let _ = store.clear_checkpoint(APP_ID, name);
                return Ok(OutcomeBuilder::new(format!(
                    "stopped at step {} of {total} ({}): {reason}",
                    index + 1,
                    step.describe()
                ))
                .failed()
                .count("steps_run", done)
                .count("steps_failed", failed)
                .count("steps_planned", total as i64)
                .build());
            }
        }
    }

    // The whole sequence ran: there is nothing left to resume.
    store.clear_checkpoint(APP_ID, name)?;
    if failed == 0 {
        return Ok(OutcomeBuilder::new(format!(
            "{total} step(s) run: {}",
            workflow
                .labels()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(" → ")
        ))
        .count("steps_run", done)
        .count("steps_failed", failed)
        .count("steps_planned", total as i64)
        .count("resumed_from_step", resumed_from as i64)
        .build());
    }
    // The workflow carried on past failures: it ends failed, with each step's outcome in
    // the summary (the same outcomes the run page lists per step run).
    Ok(OutcomeBuilder::new(format!(
        "{total} step(s): {}; {done} succeeded, {failed} failed",
        outcomes.join(", ")
    ))
    .failed()
    .count("steps_run", done)
    .count("steps_failed", failed)
    .count("steps_planned", total as i64)
    .count("resumed_from_step", resumed_from as i64)
    .build())
}

/// The failed steps a pass carried past, as a suffix for a summary that stops early.
fn carried_past(failures: &[String]) -> String {
    if failures.is_empty() {
        String::new()
    } else {
        format!(
            "; carried past {} failed step(s) earlier: {}",
            failures.len(),
            failures.join(", ")
        )
    }
}

/// How long a step waits for its app to free before the workflow gives up on it.
///
/// The same bound a hook's queued follow-up has: a webhook-triggered `streams.match_apply` that
/// starts in the gap between two steps holds the app for minutes, not hours.
const STEP_BUSY_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// How often a step waiting for a busy app re-tries its start.
const STEP_BUSY_POLL: std::time::Duration = std::time::Duration::from_millis(250);

/// Start one step, waiting while its app is busy with someone else's run.
///
/// A workflow holds no app between its steps, so a hook or a schedule can take the app in the
/// gap. That run is legitimate work, and failing the whole sequence because of it would throw
/// away a finished probe step; so a refused start whose app is busy (in this process or, via
/// its `running` row, in another) waits for it, bounded by [`STEP_BUSY_MAX_WAIT`], the window's
/// deadline and the workflow's own cancel. Any other refusal is returned at once.
async fn start_step(
    runner: &JobRunner,
    slot: &crate::runner::WorkflowSlot,
    options: &RunOptions,
    step: &Step,
    step_action: &str,
    step_dry: bool,
    step_options: RunOptions,
) -> Result<StartedRun> {
    let give_up = tokio::time::Instant::now() + STEP_BUSY_MAX_WAIT;
    let out_of_time = || {
        tokio::time::Instant::now() >= give_up
            || options
                .deadline
                .is_some_and(|deadline| Utc::now() >= deadline)
    };
    let mut logged = false;
    loop {
        let error = match runner.start_with(&step.app, step_action, step_dry, step_options.clone())
        {
            Ok(started) => return Ok(started),
            Err(error) => error,
        };
        let busy = runner.is_busy(&step.app)
            || runner
                .store()
                .running_runs(&step.app)
                .is_ok_and(|runs| !runs.is_empty());
        if !busy || out_of_time() || slot.is_cancelled() || runner.is_shutting_down() {
            return Err(error);
        }
        if !logged {
            tracing::info!(
                step = %step,
                "the step's app is busy with another run; waiting for it to free"
            );
            logged = true;
        }
        // Never sleep past the window: a step that could only start after it must not.
        let mut pause = STEP_BUSY_POLL;
        if let Some(left) = options
            .deadline
            .and_then(|deadline| (deadline - Utc::now()).to_std().ok())
        {
            pause = pause.min(left);
        }
        tokio::time::sleep(pause).await;
        if out_of_time() || slot.is_cancelled() {
            return Err(error);
        }
    }
}

/// The step index this pass starts at, from the checkpoint a paused or cancelled pass left.
///
/// A checkpoint pointing past the end of a configuration that has since lost steps starts over:
/// the old place names a step that no longer exists, so there is nothing to resume and starting
/// at the end would report success without running anything.
fn resume_index(store: &Store, workflow: &Workflow, options: &RunOptions) -> Result<usize> {
    if options.fresh {
        store.clear_checkpoint(APP_ID, &workflow.name)?;
        return Ok(0);
    }
    let index = store
        .load_checkpoint(APP_ID, &workflow.name)?
        .and_then(|state| state.get("step").and_then(|step| step.as_u64()))
        .unwrap_or(0) as usize;
    if index >= workflow.steps.len() {
        if index > 0 {
            tracing::warn!(
                workflow = workflow.name.as_str(),
                saved_step = index,
                steps = workflow.steps.len(),
                "the saved step is past the end of this workflow's steps; starting over"
            );
            store.clear_checkpoint(APP_ID, &workflow.name)?;
        }
        return Ok(0);
    }
    Ok(index)
}

/// Save the step a paused or cancelled pass stopped at.
fn save_step(store: &Store, name: &str, index: usize) -> Result<()> {
    store.save_checkpoint(APP_ID, name, &json!({ "step": index }))
}

/// Which action a step runs, and whether it is a dry run.
///
/// In an ordinary (real) pass the rule is the scheduler's: an action's own kind decides, so a
/// `_preview` step stays a preview and an `_apply` step really writes. In a dry pass every step
/// is dry, and a writing step is redirected to its `<verb>_preview` when the app has one — the
/// same guarantee `crosstalk run <app> <apply> --dry-run` makes, because an `_apply` action is
/// not required to honour the flag.
fn resolve_step(registry: &Registry, step: &Step, dry_run: bool) -> Result<(String, bool)> {
    let Some((_app, action)) = registry.find_action(&step.app, &step.action) else {
        bail!("no action {:?} in sub-app {:?}", step.action, step.app);
    };
    if !dry_run {
        return Ok((step.action.clone(), !action.kind.touches_channels()));
    }
    if !action.kind.touches_channels() {
        return Ok((step.action.clone(), true));
    }
    if let Some(verb) = step.action.strip_suffix("_apply") {
        let preview = format!("{verb}_preview");
        if registry.find_action(&step.app, &preview).is_some() {
            return Ok((preview, true));
        }
    }
    Ok((step.action.clone(), true))
}

#[cfg(test)]
mod tests;
