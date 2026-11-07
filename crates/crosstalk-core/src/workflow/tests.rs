//! Workflow tests: the step loop, its stopping rules, and the record it leaves.
//!
//! They drive the real [`JobRunner`] and the real store, like the runner's own tests: what they
//! prove is the host's behaviour (order, stop-on-failure, pause/resume, cancel, the parent link),
//! not a mock of it.

use super::*;
use crate::config::{WorkflowConfig, WorkflowStepDetail, WorkflowStepEntry, WorkflowStepScope};
use crate::dispatcharr::{Channel, ChannelGroup, World};
use crate::store::RunRecord;
use crate::subapp::{
    ActionKind, ActionSpec, NamedScope, NamedScopeResolution, SettingsSchema, SettingsValues,
    SubApp,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A small app whose actions succeed, fail or write, so a sequence can be watched step by step.
struct LabApp;

impl SubApp for LabApp {
    fn id(&self) -> &'static str {
        "lab"
    }

    fn name(&self) -> &'static str {
        "Lab"
    }

    fn description(&self) -> &'static str {
        "test steps"
    }

    fn actions(&self) -> Vec<ActionSpec> {
        vec![
            ActionSpec::new("ok", "Ok", "succeeds", ActionKind::Read).offline(),
            ActionSpec::new("fail", "Fail", "fails", ActionKind::Read).offline(),
            // The `<verb>_preview` / `<verb>_apply` pair every real app declares, so the dry-run
            // redirect is exercised the way it is actually written.
            ActionSpec::new("touch_preview", "Touch preview", "reads", ActionKind::Read).offline(),
            ActionSpec::new("touch_apply", "Touch", "writes", ActionKind::Write).offline(),
        ]
    }

    fn schema(&self) -> &SettingsSchema {
        static SCHEMA: std::sync::OnceLock<SettingsSchema> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(SettingsSchema::default)
    }

    fn load_settings(&self, _path: &std::path::Path) -> Result<SettingsValues> {
        Ok(SettingsValues::new())
    }

    fn save_settings(&self, _path: &std::path::Path, _values: &SettingsValues) -> Result<()> {
        Ok(())
    }

    fn run<'a>(
        &'a self,
        action: &'a str,
        _context: &'a crate::jobs::JobContext,
        _dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>> {
        Box::pin(async move {
            match action {
                "fail" => anyhow::bail!("deliberate step failure"),
                other => Ok(JobOutcome::new(format!("{other} done")).with_count("rows", 1)),
            }
        })
    }
}

fn registry() -> Registry {
    Registry::new(vec![
        Arc::new(LabApp),
        Arc::new(crate::test_app::ResumableApp::new()),
    ])
}

fn config_with(steps: &[&str]) -> CrosstalkConfig {
    config_with_entries(
        steps
            .iter()
            .map(|step| crate::config::WorkflowStepEntry::from(*step))
            .collect(),
    )
}

fn config_with_entries(steps: Vec<WorkflowStepEntry>) -> CrosstalkConfig {
    let mut config = CrosstalkConfig::default();
    config.workflows.insert(
        "nightly".to_string(),
        WorkflowConfig {
            steps,
            fresh: false,
        },
    );
    config
}

/// A table step entry, so table steps read naturally in tests.
fn table_step(
    action: &str,
    scope: Option<WorkflowStepScope>,
    continue_on_failure: bool,
) -> WorkflowStepEntry {
    WorkflowStepEntry::Detailed(WorkflowStepDetail {
        action: action.to_string(),
        scope,
        continue_on_failure,
    })
}

fn table_scope(groups: &str) -> WorkflowStepScope {
    WorkflowStepScope::Table(crate::config::ScopeConfig {
        groups: groups.to_string(),
        ..crate::config::ScopeConfig::default()
    })
}

/// A fake app standing in for `lineup` (step 13+): it offers a named scope and records the
/// channels each of its steps resolved, so the tests prove a step's override reaches the
/// action through the executor, the run options and the context.
struct ScopeApp {
    seen: Arc<Mutex<Vec<Vec<i64>>>>,
    /// What the named scope resolves to.
    scope_ids: Vec<i64>,
}

impl ScopeApp {
    fn new(seen: Arc<Mutex<Vec<Vec<i64>>>>, scope_ids: Vec<i64>) -> Self {
        Self { seen, scope_ids }
    }

    fn world() -> World {
        let channel = |id: i64, name: &str, group: Option<i64>| Channel {
            id,
            name: name.to_string(),
            channel_group_id: group,
            ..Channel::default()
        };
        World {
            channels: vec![channel(1, "ESPN", Some(10)), channel(2, "CNN", Some(20))],
            groups: vec![
                ChannelGroup {
                    id: 10,
                    name: "Sports".to_string(),
                },
                ChannelGroup {
                    id: 20,
                    name: "News".to_string(),
                },
            ],
            ..World::default()
        }
    }
}

impl SubApp for ScopeApp {
    fn id(&self) -> &'static str {
        "scope"
    }

    fn name(&self) -> &'static str {
        "Scope"
    }

    fn description(&self) -> &'static str {
        "test scope overrides"
    }

    fn actions(&self) -> Vec<ActionSpec> {
        vec![
            ActionSpec::new("record", "Record", "records its channels", ActionKind::Read).offline(),
        ]
    }

    fn schema(&self) -> &SettingsSchema {
        static SCHEMA: std::sync::OnceLock<SettingsSchema> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(SettingsSchema::default)
    }

    fn load_settings(&self, _path: &std::path::Path) -> Result<SettingsValues> {
        Ok(SettingsValues::new())
    }

    fn save_settings(&self, _path: &std::path::Path, _values: &SettingsValues) -> Result<()> {
        Ok(())
    }

    fn named_scopes(&self) -> Vec<NamedScope> {
        vec![
            NamedScope::new("fakescope", "the fake app's channels"),
            NamedScope::new("brokenscope", "a scope whose resolution fails"),
        ]
    }

    fn resolve_named_scope(
        &self,
        name: &str,
        _config: &crate::config::CrosstalkConfig,
        _world: &World,
        _store: &crate::store::Store,
    ) -> Option<Result<NamedScopeResolution>> {
        match name.trim() {
            "fakescope" => Some(Ok(NamedScopeResolution::Channels(self.scope_ids.clone()))),
            "brokenscope" => Some(Err(anyhow::anyhow!("the scope's store read failed"))),
            _ => None,
        }
    }

    fn run<'a>(
        &'a self,
        action: &'a str,
        context: &'a crate::jobs::JobContext,
        _dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(action, "record");
            // A hand-built world: the point is what the CONTEXT resolves, not Dispatcharr.
            let scoped = context.scoped_channels(&Self::world())?;
            let mut ids: Vec<i64> = scoped.channels.iter().map(|channel| channel.id).collect();
            ids.sort_unstable();
            self.seen.lock().expect("seen lock").push(ids.clone());
            Ok(
                JobOutcome::new(format!("recorded {} channel(s)", ids.len()))
                    .with_count("channels", ids.len() as i64),
            )
        })
    }
}

fn runner(config: CrosstalkConfig) -> JobRunner {
    JobRunner::new(
        registry(),
        crate::config::SharedConfig::new(config),
        Store::in_memory().expect("store"),
    )
}

async fn wait_until_workflow_idle(runner: &JobRunner) {
    for _ in 0..600 {
        if !runner.is_busy(APP_ID) && runner.running().is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the workflow never went idle");
}

/// The workflow run record, newest first.
fn workflow_run(runner: &JobRunner) -> RunRecord {
    runner
        .store()
        .recent_runs_for(APP_ID, 1)
        .expect("runs")
        .into_iter()
        .next()
        .expect("a workflow run")
}

#[tokio::test]
async fn steps_run_in_order_and_each_gets_its_own_run() {
    let runner = runner(config_with(&["lab.ok", "resumable.status"]));
    let outcome = run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect("runs");

    assert!(!outcome.is_failed(), "{}", outcome.summary);
    assert!(!outcome.is_paused());
    assert_eq!(outcome.counts["steps_run"], 2);
    assert_eq!(outcome.counts["steps_planned"], 2);
    assert!(outcome.summary.contains("lab.ok"), "{}", outcome.summary);

    let record = workflow_run(&runner);
    assert_eq!(record.app, APP_ID);
    assert_eq!(record.action, "nightly");
    assert_eq!(record.status, RunStatus::Succeeded);
    assert_eq!(record.parent_run_id, None, "a workflow is nobody's step");

    // Each step is a run of its own app, linked to the workflow's run.
    let steps = runner.store().child_runs(record.id).expect("steps");
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].app, "lab");
    assert_eq!(steps[0].action, "ok");
    assert_eq!(steps[1].app, "resumable");
    assert_eq!(steps[1].action, "status");
    for step in &steps {
        assert_eq!(step.parent_run_id, Some(record.id));
        assert_eq!(step.status, RunStatus::Succeeded);
    }
    // A read step is a dry run, exactly as a scheduled one would be.
    assert!(steps.iter().all(|step| step.dry_run));
}

#[tokio::test]
async fn a_failed_step_stops_the_sequence_and_is_recorded_failed() {
    let runner = runner(config_with(&["lab.ok", "lab.fail", "lab.touch_apply"]));
    let outcome = run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect("the workflow answers rather than erroring");

    assert!(outcome.is_failed(), "{}", outcome.summary);
    assert!(outcome.summary.contains("lab.fail"), "{}", outcome.summary);
    // What the steps that DID run did is kept, which is the whole reason `OutcomeStatus::Failed`
    // exists: an `Err` would record an empty count.
    assert_eq!(outcome.counts["steps_run"], 1);

    let record = workflow_run(&runner);
    assert_eq!(record.status, RunStatus::Failed);
    assert_eq!(record.counts["steps_run"], 1);
    let steps = runner.store().child_runs(record.id).expect("steps");
    assert_eq!(steps.len(), 2, "the third step never started: {steps:?}");
    assert_eq!(steps[1].status, RunStatus::Failed);
    // A failure does not leave a place to resume: the next pass starts over.
    assert_eq!(
        runner
            .store()
            .load_checkpoint(APP_ID, "nightly")
            .expect("read"),
        None
    );
}

#[tokio::test]
async fn a_step_app_that_stays_busy_fails_the_workflow_with_the_reason() {
    let runner = runner(config_with(&["resumable.run"]));
    // Something else holds the step's app (for ~100 ms) for longer than the window lasts.
    let manual = runner
        .start("resumable", "run", false)
        .expect("a manual run holds the app");

    let deadline = Utc::now() + chrono::Duration::milliseconds(40);
    let outcome = run_to_completion(
        &runner,
        "nightly",
        false,
        RunOptions::default().with_deadline(Some(deadline)),
    )
    .await
    .expect("the workflow answers");
    assert!(outcome.is_failed(), "{}", outcome.summary);
    assert!(
        outcome.summary.contains("already has run"),
        "the summary says why: {}",
        outcome.summary
    );
    let record = workflow_run(&runner);
    let steps = runner.store().child_runs(record.id).expect("steps");
    assert!(steps.is_empty(), "no step run was created: {steps:?}");

    runner.cancel(manual.run_id);
    wait_until_workflow_idle(&runner).await;
}

#[tokio::test]
async fn a_step_waits_for_a_run_that_took_its_app_between_steps() {
    // A hook or a schedule can start a run of the step's app in the gap between two steps.
    // That is legitimate work: the step waits for it instead of failing the sequence.
    let runner = runner(config_with(&["lab.ok", "resumable.run"]));
    let manual = runner
        .start("resumable", "run", false)
        .expect("a manual run holds the app");
    let canceller = {
        let runner = runner.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            runner.cancel(manual.run_id);
        })
    };

    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        run_to_completion(&runner, "nightly", false, RunOptions::default()),
    )
    .await
    .expect("the step starts once the app frees")
    .expect("the workflow answers");
    canceller.await.expect("canceller");
    assert!(!outcome.is_failed(), "{}", outcome.summary);
    assert_eq!(outcome.counts["steps_run"], 2, "{}", outcome.summary);
    wait_until_workflow_idle(&runner).await;
}

#[tokio::test]
async fn a_paused_step_pauses_the_workflow_and_the_next_run_resumes_that_step() {
    let runner = runner(config_with(&[
        "lab.ok",
        "resumable.run",
        "lab.touch_preview",
    ]));

    // A window that closes during the SECOND step: the first finishes, the second pauses
    // mid-backlog, and the third must not start.
    let deadline = Utc::now() + chrono::Duration::milliseconds(120);
    let first = run_to_completion(
        &runner,
        "nightly",
        false,
        RunOptions::default().with_deadline(Some(deadline)),
    )
    .await
    .expect("a paused workflow is not an error");

    assert!(first.is_paused(), "{}", first.summary);
    assert!(first.summary.contains("resumable.run"), "{}", first.summary);
    let record = workflow_run(&runner);
    assert_eq!(record.status, RunStatus::Paused);
    assert_eq!(
        runner
            .store()
            .load_checkpoint(APP_ID, "nightly")
            .expect("read"),
        Some(serde_json::json!({ "step": 1 })),
        "the checkpoint names the paused step"
    );
    let first_steps = runner.store().child_runs(record.id).expect("steps");
    assert_eq!(first_steps.len(), 2, "{first_steps:?}");
    assert_eq!(first_steps[1].status, RunStatus::Paused);

    // The next firing continues AT that step — the first is not re-run — and finishes the rest.
    let second = run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect("resumes");
    assert!(!second.is_paused(), "{}", second.summary);
    assert_eq!(second.counts["resumed_from_step"], 1);
    assert_eq!(second.counts["steps_run"], 2, "steps 2 and 3, not step 1");
    assert!(second.summary.contains("lab.touch_preview"));

    let resumed = workflow_run(&runner);
    assert_eq!(resumed.status, RunStatus::Succeeded);
    let resumed_steps = runner.store().child_runs(resumed.id).expect("steps");
    assert_eq!(resumed_steps.len(), 2);
    assert_eq!(
        resumed_steps[0].action, "run",
        "the paused step was resumed"
    );
    // The step's own checkpoint resumed the backlog the earlier window had left.
    assert_eq!(resumed_steps[0].counts["resumed"], 1);
    assert_eq!(resumed_steps[1].action, "touch_preview");
    assert_eq!(
        runner
            .store()
            .load_checkpoint(APP_ID, "nightly")
            .expect("read"),
        None,
        "a finished sequence has nothing left to resume"
    );
}

#[tokio::test]
async fn cancelling_the_workflow_cancels_the_step_it_is_inside() {
    let runner = runner(config_with(&["resumable.run", "lab.ok"]));
    let started = start(&runner, "nightly", false, RunOptions::default()).expect("started");
    assert!(runner.is_busy(APP_ID), "the workflow holds its slot");

    // Long enough for the first unit or two, short enough that the backlog is not finished.
    tokio::time::sleep(crate::test_app::RESUMABLE_PER_UNIT * 3).await;
    assert!(runner.cancel(started.run_id), "the workflow is cancellable");
    wait_until_workflow_idle(&runner).await;

    let record = workflow_run(&runner);
    assert_eq!(record.status, RunStatus::Cancelled);
    let steps = runner.store().child_runs(record.id).expect("steps");
    assert_eq!(steps.len(), 1, "the second step never started: {steps:?}");
    assert_eq!(
        steps[0].status,
        RunStatus::Cancelled,
        "the running step was cancelled with the workflow"
    );
    // The place is kept, so the next pass continues at the step that was interrupted.
    assert_eq!(
        runner
            .store()
            .load_checkpoint(APP_ID, "nightly")
            .expect("read"),
        Some(serde_json::json!({ "step": 0 }))
    );
}

#[tokio::test]
async fn only_one_workflow_runs_at_a_time() {
    let runner = runner(config_with(&["resumable.run"]));
    let first = start(&runner, "nightly", false, RunOptions::default()).expect("started");
    let error = start(&runner, "nightly", false, RunOptions::default()).expect_err("refused");
    assert!(
        format!("{error:#}").contains("already has run"),
        "{error:#}"
    );
    runner.cancel(first.run_id);
    wait_until_workflow_idle(&runner).await;
}

#[tokio::test]
async fn an_unknown_workflow_or_step_is_refused_before_anything_runs() {
    let runner = runner(config_with(&["lab.nope"]));
    let error = run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect_err("no such step");
    assert!(format!("{error:#}").contains("no action"), "{error:#}");

    let error = run_to_completion(&runner, "nope", false, RunOptions::default())
        .await
        .expect_err("no such workflow");
    assert!(format!("{error:#}").contains("no workflow"), "{error:#}");

    // Neither attempt left a run behind: nothing started.
    assert_eq!(runner.store().run_count().expect("count"), 0);
}

#[tokio::test]
async fn a_dry_pass_runs_the_preview_of_a_writing_step() {
    let runner = runner(config_with(&["lab.touch_apply"]));
    let outcome = run_to_completion(&runner, "nightly", true, RunOptions::default())
        .await
        .expect("runs");
    assert!(!outcome.is_failed(), "{}", outcome.summary);

    let record = workflow_run(&runner);
    assert!(record.dry_run);
    let steps = runner.store().child_runs(record.id).expect("steps");
    assert_eq!(steps.len(), 1);
    assert_eq!(
        steps[0].action, "touch_preview",
        "a writing step is redirected to its preview in a dry pass"
    );
    assert!(steps[0].dry_run);
}

#[tokio::test]
async fn a_real_pass_writes_and_keeps_a_read_step_dry() {
    let runner = runner(config_with(&["lab.ok", "lab.touch_apply"]));
    run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect("runs");

    let record = workflow_run(&runner);
    assert!(!record.dry_run);
    let steps = runner.store().child_runs(record.id).expect("steps");
    assert!(steps[0].dry_run, "a read step is still a dry run");
    assert!(!steps[1].dry_run, "a writing step really writes");
    assert_eq!(steps[1].action, "touch_apply");
}

#[tokio::test]
async fn a_fresh_workflow_discards_every_steps_own_place() {
    // `fresh = true` on the entry: a new pass starts each step from the beginning, which is what
    // an operator wants after widening the scope.
    let mut config = config_with(&["resumable.run"]);
    config.workflows.get_mut("nightly").expect("entry").fresh = true;
    let runner = runner(config);
    runner
        .store()
        .save_checkpoint("resumable", "run", &serde_json::json!({"done": 4}))
        .expect("seed a step checkpoint");

    let outcome = run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect("runs");
    assert_eq!(outcome.counts["steps_run"], 1);
    let steps = runner
        .store()
        .child_runs(workflow_run(&runner).id)
        .expect("steps");
    assert!(
        steps[0].counts.get("resumed").is_none(),
        "the step started over: {:?}",
        steps[0].counts
    );
}

#[test]
fn a_step_cannot_be_a_workflow() {
    let error = Step::parse("workflow.other").expect_err("refused");
    assert!(format!("{error:#}").contains("do not nest"), "{error:#}");
}

#[test]
fn every_configured_workflow_is_described_with_its_steps() {
    let mut config = config_with(&["streams.probe", "streams.sort_apply"]);
    config.workflows.insert(
        "broken".to_string(),
        WorkflowConfig {
            steps: vec!["not a step".into()],
            fresh: false,
        },
    );
    let described = describe(&config);
    assert_eq!(described.len(), 2);
    let nightly = described
        .iter()
        .find(|info| info.name == "nightly")
        .expect("listed");
    assert_eq!(
        nightly.steps,
        vec![
            "streams.probe".to_string(),
            "streams.sort_apply".to_string()
        ]
    );
    assert!(nightly.error.is_none());
    let broken = described
        .iter()
        .find(|info| info.name == "broken")
        .expect("listed");
    assert!(broken.error.is_some(), "an unparsable step is reported");
}

#[test]
fn a_table_step_parses_with_its_scope_and_failure_policy() {
    let workflow = Workflow::from_entry(
        "full_sync",
        &WorkflowConfig {
            steps: vec![
                "lineup.sync_apply".into(),
                table_step(
                    "streams.match_apply",
                    Some(WorkflowStepScope::Named("lineup".to_string())),
                    false,
                ),
                table_step("guide.match_apply", Some(table_scope("DIRECTV: *")), true),
            ],
            fresh: false,
        },
    )
    .expect("parses");
    assert_eq!(workflow.steps.len(), 3);
    assert_eq!(workflow.steps[0].scope, None);
    assert!(!workflow.steps[0].continue_on_failure);
    assert_eq!(
        workflow.steps[1].scope,
        Some(WorkflowStepScope::Named("lineup".to_string()))
    );
    assert!(workflow.steps[2].continue_on_failure);
    assert_eq!(
        workflow.steps[2].describe(),
        "guide.match_apply [scope groups \"DIRECTV: *\"] [continue on failure]"
    );
}

#[test]
fn a_bad_table_step_names_its_index() {
    for (entry, fragment) in [
        (
            table_step("workflow.other", None, false),
            "step 1 (workflow.other)",
        ),
        (
            table_step("probe", None, false),
            "step 1 (probe): workflow step \"probe\" must be <app>.<action>",
        ),
        (
            table_step(
                "guide.match_apply",
                Some(WorkflowStepScope::Table(crate::config::ScopeConfig {
                    groups: "A".to_string(),
                    ignore_groups: "B".to_string(),
                    ..crate::config::ScopeConfig::default()
                })),
                false,
            ),
            "scope.groups and scope.ignore_groups cannot both be set",
        ),
        (
            table_step(
                "guide.match_apply",
                Some(WorkflowStepScope::Named("  ".to_string())),
                false,
            ),
            "the scope name is empty",
        ),
    ] {
        let error = Workflow::from_entry(
            "nightly",
            &WorkflowConfig {
                steps: vec![entry],
                fresh: false,
            },
        )
        .expect_err("refused");
        assert!(
            format!("{error:#}").contains(fragment),
            "the error names the bad step: {error:#}"
        );
    }
}

#[tokio::test]
async fn a_table_scope_override_replaces_the_host_scope_for_that_step() {
    // The host scope would select News (channel 2); the step's table says Sports.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let registry = Registry::new(vec![Arc::new(ScopeApp::new(seen.clone(), vec![1]))]);
    let mut config = config_with_entries(vec![table_step(
        "scope.record",
        Some(table_scope("Sports")),
        false,
    )]);
    config.scope.groups = "News".to_string();
    let runner = JobRunner::new(
        registry,
        crate::config::SharedConfig::new(config),
        Store::in_memory().expect("store"),
    );
    let outcome = run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect("runs");
    assert!(!outcome.is_failed(), "{}", outcome.summary);
    // Only the Sports channel reached the action: the table replaced `[scope]`.
    assert_eq!(*seen.lock().expect("seen lock"), vec![vec![1]]);
}

#[tokio::test]
async fn a_named_scope_from_an_app_replaces_the_host_scope() {
    // The host scope says News and the step names the fake app's scope, which resolves to
    // the Sports channel: the named set wins, it is not intersected with the host scope.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let registry = Registry::new(vec![Arc::new(ScopeApp::new(seen.clone(), vec![1]))]);
    let mut config = config_with_entries(vec![table_step(
        "scope.record",
        Some(WorkflowStepScope::Named("fakescope".to_string())),
        false,
    )]);
    config.scope.groups = "News".to_string();
    let runner = JobRunner::new(
        registry,
        crate::config::SharedConfig::new(config),
        Store::in_memory().expect("store"),
    );
    let outcome = run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect("runs");
    assert!(!outcome.is_failed(), "{}", outcome.summary);
    assert_eq!(*seen.lock().expect("seen lock"), vec![vec![1]]);
}

/// A fake app offering a HINTED named scope (`ChannelsWithHints`): the channel set plus
/// what the app knows about it, so the test proves hints reach the step's action through
/// the same executor, run options and context the channel set travels.
struct HintApp {
    seen_ids: Arc<Mutex<Vec<Vec<i64>>>>,
    seen_hints: Arc<Mutex<Vec<Option<String>>>>,
}

impl SubApp for HintApp {
    fn id(&self) -> &'static str {
        "hint"
    }

    fn name(&self) -> &'static str {
        "Hint"
    }

    fn description(&self) -> &'static str {
        "test scope hints"
    }

    fn actions(&self) -> Vec<ActionSpec> {
        vec![ActionSpec::new("record", "Record", "records its scope", ActionKind::Read).offline()]
    }

    fn schema(&self) -> &SettingsSchema {
        static SCHEMA: std::sync::OnceLock<SettingsSchema> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(SettingsSchema::default)
    }

    fn load_settings(&self, _path: &std::path::Path) -> Result<SettingsValues> {
        Ok(SettingsValues::new())
    }

    fn save_settings(&self, _path: &std::path::Path, _values: &SettingsValues) -> Result<()> {
        Ok(())
    }

    fn named_scopes(&self) -> Vec<NamedScope> {
        vec![NamedScope::new(
            "hinted",
            "the fake app's channels, with the country they share",
        )]
    }

    fn resolve_named_scope(
        &self,
        name: &str,
        _config: &crate::config::CrosstalkConfig,
        _world: &World,
        _store: &crate::store::Store,
    ) -> Option<Result<NamedScopeResolution>> {
        match name.trim() {
            "hinted" => Some(Ok(NamedScopeResolution::ChannelsWithHints {
                ids: vec![1],
                hints: crate::subapp::NamedScopeHints::with_country("UK"),
            })),
            _ => None,
        }
    }

    fn run<'a>(
        &'a self,
        action: &'a str,
        context: &'a crate::jobs::JobContext,
        _dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(action, "record");
            // The same hand-built world every scope test reads: the point is what the
            // CONTEXT resolves, not Dispatcharr.
            let world = ScopeApp::world();
            let scoped = context.scoped_channels(&world)?;
            let mut ids: Vec<i64> = scoped.channels.iter().map(|channel| channel.id).collect();
            ids.sort_unstable();
            self.seen_ids.lock().expect("seen lock").push(ids.clone());
            let hints = context.scope_hints(&world)?;
            self.seen_hints
                .lock()
                .expect("seen lock")
                .push(hints.country.clone());
            Ok(JobOutcome::new(format!(
                "recorded {} channel(s) under {:?}",
                ids.len(),
                hints.country,
            )))
        })
    }
}

#[tokio::test]
async fn a_hinted_named_scope_carries_hints_to_the_step() {
    // The step names the hinted scope: the bound still replaces the host scope (only the
    // Sports channel reaches the action), and the hint rides along — the country the
    // providing app reported for those channels.
    let seen_ids = Arc::new(Mutex::new(Vec::new()));
    let seen_hints = Arc::new(Mutex::new(Vec::new()));
    let registry = Registry::new(vec![Arc::new(HintApp {
        seen_ids: seen_ids.clone(),
        seen_hints: seen_hints.clone(),
    })]);
    let mut config = config_with_entries(vec![table_step(
        "hint.record",
        Some(WorkflowStepScope::Named("hinted".to_string())),
        false,
    )]);
    config.scope.groups = "News".to_string();
    let runner = JobRunner::new(
        registry,
        crate::config::SharedConfig::new(config),
        Store::in_memory().expect("store"),
    );
    let outcome = run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect("runs");
    assert!(!outcome.is_failed(), "{}", outcome.summary);
    assert_eq!(*seen_ids.lock().expect("seen lock"), vec![vec![1]]);
    assert_eq!(
        *seen_hints.lock().expect("seen lock"),
        vec![Some("UK".to_string())]
    );
}

#[tokio::test]
async fn an_unknown_scope_name_is_refused_before_the_workflow_starts() {
    // Like a step naming no registered action, a scope no app provides is a configuration
    // error: the start is refused and no run record is created, whatever the step's
    // failure policy says.
    for continue_on_failure in [false, true] {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let registry = Registry::new(vec![
            Arc::new(ScopeApp::new(seen.clone(), vec![1])),
            Arc::new(LabApp),
        ]);
        let runner = JobRunner::new(
            registry,
            crate::config::SharedConfig::new(config_with_entries(vec![
                table_step(
                    "scope.record",
                    Some(WorkflowStepScope::Named("nope".to_string())),
                    continue_on_failure,
                ),
                "lab.ok".into(),
            ])),
            Store::in_memory().expect("store"),
        );
        let error = run_to_completion(&runner, "nightly", false, RunOptions::default())
            .await
            .expect_err("refused up front");
        assert!(format!("{error:#}").contains("\"nope\""), "{error:#}");
        assert!(
            runner
                .store()
                .recent_runs_for(APP_ID, 1)
                .expect("runs")
                .is_empty(),
            "no workflow run was recorded"
        );
        assert!(seen.lock().expect("seen lock").is_empty());
    }
}

#[tokio::test]
async fn a_scope_that_fails_to_resolve_fails_the_step_at_run_time() {
    // A served name whose resolution fails (the app's store read, say) fails the step
    // like any failed step: stopping outright, or carrying on when the step allows it.
    for (continue_on_failure, planned) in [(false, 1_usize), (true, 2_usize)] {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let registry = Registry::new(vec![
            Arc::new(ScopeApp::new(seen.clone(), vec![1])),
            Arc::new(LabApp),
        ]);
        let runner = JobRunner::new(
            registry,
            crate::config::SharedConfig::new(config_with_entries(vec![
                table_step(
                    "scope.record",
                    Some(WorkflowStepScope::Named("brokenscope".to_string())),
                    continue_on_failure,
                ),
                "lab.ok".into(),
            ])),
            Store::in_memory().expect("store"),
        );
        let outcome = run_to_completion(&runner, "nightly", false, RunOptions::default())
            .await
            .expect("the workflow answers rather than erroring");
        assert!(outcome.is_failed(), "{}", outcome.summary);
        assert!(
            outcome.summary.contains("brokenscope"),
            "the summary names the scope: {}",
            outcome.summary
        );
        let record = workflow_run(&runner);
        assert_eq!(record.status, RunStatus::Failed);
        let steps = runner.store().child_runs(record.id).expect("steps");
        // The failing scope stopped its own step (a recorded failure) and, without
        // `continue_on_failure`, the step after it never started.
        assert_eq!(steps.len(), planned, "{steps:?}");
        assert_eq!(steps[0].status, RunStatus::Failed);
    }
}

#[tokio::test]
async fn a_failed_step_with_continue_runs_the_rest_and_ends_failed() {
    let runner = runner(config_with_entries(vec![
        "lab.ok".into(),
        table_step("lab.fail", None, true),
        "lab.ok".into(),
    ]));
    let outcome = run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect("the workflow answers rather than erroring");

    assert!(outcome.is_failed(), "{}", outcome.summary);
    // What the steps that DID run did is kept, and each step's outcome is in the summary.
    assert_eq!(outcome.counts["steps_run"], 2);
    assert_eq!(outcome.counts["steps_failed"], 1);
    assert_eq!(outcome.counts["steps_planned"], 3);
    assert!(outcome.summary.contains("lab.fail"), "{}", outcome.summary);
    assert!(outcome.summary.contains("FAILED"), "{}", outcome.summary);
    assert!(
        outcome.summary.contains("lab.ok succeeded"),
        "{}",
        outcome.summary
    );

    let record = workflow_run(&runner);
    assert_eq!(record.status, RunStatus::Failed);
    assert_eq!(record.counts["steps_failed"], 1);
    let steps = runner.store().child_runs(record.id).expect("steps");
    assert_eq!(steps.len(), 3, "the third step ran: {steps:?}");
    assert_eq!(steps[0].status, RunStatus::Succeeded);
    assert_eq!(steps[1].status, RunStatus::Failed);
    assert_eq!(steps[2].status, RunStatus::Succeeded);
    // A finished sequence — even a failed one — has nothing left to resume.
    assert_eq!(
        runner
            .store()
            .load_checkpoint(APP_ID, "nightly")
            .expect("read"),
        None
    );
}

#[tokio::test]
async fn cancel_still_stops_a_workflow_despite_continue() {
    let runner = runner(config_with_entries(vec![table_step(
        "resumable.run",
        None,
        true,
    )]));
    let started = start(&runner, "nightly", false, RunOptions::default()).expect("started");
    assert!(runner.cancel(started.run_id), "the workflow is cancellable");
    wait_until_workflow_idle(&runner).await;

    let record = workflow_run(&runner);
    assert_eq!(record.status, RunStatus::Cancelled);
    // The place is kept, so the next pass continues at the step that was interrupted.
    assert_eq!(
        runner
            .store()
            .load_checkpoint(APP_ID, "nightly")
            .expect("read"),
        Some(serde_json::json!({ "step": 0 }))
    );
}

#[tokio::test]
async fn pause_still_pauses_a_workflow_despite_continue() {
    let runner = runner(config_with_entries(vec![
        table_step("resumable.run", None, true),
        "lab.ok".into(),
    ]));

    // A window that closes during the FIRST step: it pauses mid-backlog with its failure
    // policy set, and the second step must not start. The first step's backlog takes
    // ~100 ms (`RESUMABLE_UNITS` x `RESUMABLE_PER_UNIT`), so the window must close well
    // inside it: a longer one lets the step finish and pauses before step 2 instead.
    let deadline = Utc::now() + chrono::Duration::milliseconds(40);
    let first = run_to_completion(
        &runner,
        "nightly",
        false,
        RunOptions::default().with_deadline(Some(deadline)),
    )
    .await
    .expect("a paused workflow is not an error");

    assert!(first.is_paused(), "{}", first.summary);
    assert!(first.summary.contains("resumable.run"), "{}", first.summary);
    let record = workflow_run(&runner);
    assert_eq!(record.status, RunStatus::Paused);
    assert_eq!(
        runner
            .store()
            .load_checkpoint(APP_ID, "nightly")
            .expect("read"),
        Some(serde_json::json!({ "step": 0 })),
        "the checkpoint names the paused step"
    );
    let first_steps = runner.store().child_runs(record.id).expect("steps");
    assert_eq!(
        first_steps.len(),
        1,
        "step 2 never started: {first_steps:?}"
    );
    assert_eq!(first_steps[0].status, RunStatus::Paused);

    // The next firing continues AT that step and finishes the rest.
    let second = run_to_completion(&runner, "nightly", false, RunOptions::default())
        .await
        .expect("resumes");
    assert!(!second.is_paused(), "{}", second.summary);
    assert_eq!(second.counts["resumed_from_step"], 0);
    assert_eq!(second.counts["steps_run"], 2, "{}", second.summary);
}
