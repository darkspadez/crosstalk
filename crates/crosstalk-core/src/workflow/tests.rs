//! Workflow tests: the step loop, its stopping rules, and the record it leaves.
//!
//! They drive the real [`JobRunner`] and the real store, like the runner's own tests: what they
//! prove is the host's behaviour (order, stop-on-failure, pause/resume, cancel, the parent link),
//! not a mock of it.

use super::*;
use crate::config::WorkflowConfig;
use crate::store::RunRecord;
use crate::subapp::{ActionKind, ActionSpec, SettingsSchema, SettingsValues, SubApp};
use std::sync::Arc;
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
    let mut config = CrosstalkConfig::default();
    config.workflows.insert(
        "nightly".to_string(),
        WorkflowConfig {
            steps: steps.iter().map(|step| (*step).to_string()).collect(),
            fresh: false,
        },
    );
    config
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
            steps: vec!["not a step".to_string()],
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
