//! The answer a `status` action gives: what the last run did, and what is running right now.
//!
//! Both sub-apps had their own copy of this, and both had the phase-1 defect the review found:
//! `status` is itself a run, so the one-run-per-app guard refused it while another run of the
//! app was live — which is exactly when an operator asks what is going on. The action is now
//! declared unguarded (`ActionSpec::unguarded`), and this is the one implementation of what it
//! says, so the two apps cannot drift.
//!
//! Two sources feed it, deliberately:
//!
//!   * the run STORE holds a `running` row for a live run, including one started by another
//!     process (the one-shot CLI), which is the only cross-process truth there is;
//!   * the runner's in-memory table holds this process's runs' progress — phase, counts, percent
//!     — which is what makes the answer useful rather than just "something is running".
//!
//! A paused last run reads as paused and says it will resume: the operator's question after a
//! windowed job is "did it finish, or does it continue tonight?".

use crate::jobs::JobContext;
use crate::store::RunStatus;
use crate::subapp::{JobOutcome, SubApp};

/// Describe the app's last run and anything in flight, as the `status` action's outcome.
pub fn describe(app: &dyn SubApp, context: &JobContext) -> JobOutcome {
    let Some(store) = context.store() else {
        return JobOutcome::new("No run history is available in this context.");
    };
    let app_id = app.id();
    let last = last_run(context, app_id);
    let mut outcome = match &last {
        Some(run) => {
            let mut summary = format!(
                "Last run: {} {} ({}) started {}",
                run.action,
                run.status,
                if run.dry_run { "preview" } else { "apply" },
                run.started_at
            );
            if let Some(detail) = &run.summary {
                summary.push_str(&format!(" — {detail}"));
            }
            let mut outcome = JobOutcome::new(summary);
            if run.status.is_paused() {
                // The one thing a paused run needs to say beyond its name.
                outcome = JobOutcome::new(format!(
                    "{}; the next scheduled run of {} resumes from its checkpoint, and \
                     `--fresh` starts over",
                    outcome.summary, run.action
                ));
            }
            outcome.with_count(
                "rows",
                store.rows(run.id).map(|rows| rows.len()).unwrap_or(0) as i64,
            )
        }
        None => JobOutcome::new("No run has been recorded yet."),
    };

    let live = live_runs(context, app_id);
    outcome = match live.as_slice() {
        [] => JobOutcome::new(format!("{} Nothing is running now.", outcome.summary)),
        [run] => JobOutcome::new(format!(
            "{} In progress: {} (run {}){}.",
            outcome.summary,
            run.action,
            run.run_id,
            progress_suffix(run)
        )),
        runs => JobOutcome::new(format!(
            "{} In progress: {}.",
            outcome.summary,
            runs.iter()
                .map(|run| format!(
                    "{} (run {}){}",
                    run.action,
                    run.run_id,
                    progress_suffix(run)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    };
    outcome = outcome.with_count("running", live.len() as i64);
    if last.is_some_and(|run| run.status == RunStatus::Paused) {
        outcome = outcome.with_count("paused", 1);
    }
    outcome
}

/// How far along a live run is, or nothing when the store knows the run but this process does
/// not (a run started by another process, or one that has not published progress yet).
fn progress_suffix(run: &crate::runner::ActiveRun) -> String {
    match run.percent() {
        Some(percent) if !run.phase.is_empty() => {
            format!(": {} ({percent}%)", run.phase)
        }
        Some(percent) => format!(": {percent}%"),
        None if !run.phase.is_empty() && run.phase != "starting" => format!(": {}", run.phase),
        None => String::new(),
    }
}

/// The newest FINISHED run that is not this one.
///
/// A `status` run is always the newest of its app, so it has to exclude itself to answer the
/// question it was asked; and a run still `running` is reported under "In progress" instead of
/// twice, so "last run" means a completed one.
fn last_run(context: &JobContext, app_id: &str) -> Option<crate::store::RunRecord> {
    let store = context.store()?;
    store
        .recent_runs_for(app_id, 20)
        .unwrap_or_default()
        .into_iter()
        .find(|run| Some(run.id) != context.run_id() && !run.status.is_running())
}

/// What is in flight for this app, from this process when it knows it and from the store
/// otherwise.
fn live_runs(context: &JobContext, app_id: &str) -> Vec<crate::runner::ActiveRun> {
    let mut live = context.active_runs_for(app_id);
    let Some(store) = context.store() else {
        return live;
    };
    for record in store.running_runs(app_id).unwrap_or_default() {
        if live.iter().any(|run| run.run_id == record.id) {
            continue;
        }
        // A live row from another process: no progress to show, but the fact it is running is
        // what the operator needs.
        live.push(crate::runner::ActiveRun {
            run_id: record.id,
            app: record.app.clone(),
            action: record.action.clone(),
            dry_run: record.dry_run,
            current: 0,
            total: 0,
            phase: String::new(),
            cancelling: false,
        });
    }
    live.sort_by_key(|run| run.run_id);
    live
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CrosstalkConfig;
    use crate::jobs::JobContext;
    use crate::runner::ActiveRun;
    use crate::store::Store;
    use crate::subapp::{ActionKind, ActionSpec, SettingsSchema, SettingsValues, SubApp};
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    struct App;

    impl SubApp for App {
        fn id(&self) -> &'static str {
            "guide"
        }
        fn name(&self) -> &'static str {
            "Guide"
        }
        fn description(&self) -> &'static str {
            "test"
        }
        fn actions(&self) -> Vec<ActionSpec> {
            vec![
                ActionSpec::new("status", "Status", "reads", ActionKind::Read)
                    .offline()
                    .unguarded(),
            ]
        }
        fn schema(&self) -> &SettingsSchema {
            static SCHEMA: std::sync::OnceLock<SettingsSchema> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(SettingsSchema::default)
        }
        fn load_settings(&self, _path: &Path) -> anyhow::Result<SettingsValues> {
            Ok(SettingsValues::new())
        }
        fn save_settings(&self, _path: &Path, _values: &SettingsValues) -> anyhow::Result<()> {
            Ok(())
        }
        fn run<'a>(
            &'a self,
            _action: &'a str,
            _context: &'a JobContext,
            _dry_run: bool,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<JobOutcome>> + Send + 'a>,
        > {
            Box::pin(async { Ok(JobOutcome::new("ok")) })
        }
    }

    fn context(store: &Store, action: &str) -> JobContext {
        let run_id = store
            .start_unguarded_run("guide", action, true)
            .expect("status run");
        JobContext::for_tests(CrosstalkConfig::default()).for_run(
            "guide",
            action,
            run_id,
            store.clone(),
            Arc::new(AtomicBool::new(false)),
        )
    }

    #[test]
    fn it_reports_the_run_before_itself_and_what_is_running() {
        let store = Store::in_memory().expect("store");
        let previous = store
            .start_run("guide", "match_apply", false)
            .expect("start");
        store
            .finish_run(
                previous,
                RunStatus::Succeeded,
                Some("assigned 40 guide(s)"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");

        // A live guarded run, known to the store and to this process's progress table.
        let running = store.start_run("guide", "heal_apply", false).expect("live");
        let live: Arc<Mutex<HashMap<i64, ActiveRun>>> = Arc::new(Mutex::new(HashMap::new()));
        live.lock().expect("lock").insert(
            running,
            ActiveRun {
                run_id: running,
                app: "guide".to_string(),
                action: "heal_apply".to_string(),
                dry_run: false,
                current: 40,
                total: 100,
                phase: "validating candidates".to_string(),
                cancelling: false,
            },
        );

        let outcome = describe(
            &App,
            &context(&store, "status").with_live_runs(Arc::clone(&live)),
        );
        assert!(
            outcome
                .summary
                .starts_with("Last run: match_apply succeeded (apply)"),
            "{}",
            outcome.summary
        );
        assert!(outcome.summary.contains("assigned 40 guide(s)"));
        assert_eq!(
            outcome.counts["running"], 1,
            "the store's live row and this process's progress table are the same run"
        );
        assert!(
            outcome.summary.contains("validating candidates (40%)"),
            "the progress of the blocking run is the point: {}",
            outcome.summary
        );
    }

    #[test]
    fn a_paused_last_run_says_it_will_resume() {
        let store = Store::in_memory().expect("store");
        let previous = store.start_run("guide", "scan", true).expect("start");
        store
            .finish_run(
                previous,
                RunStatus::Paused,
                Some("paused after 400 of 900 channel(s)"),
                &serde_json::json!({"checked": 400}),
                None,
            )
            .expect("finish");

        let outcome = describe(&App, &context(&store, "status"));
        assert!(outcome.summary.contains("paused"), "{}", outcome.summary);
        assert!(
            outcome.summary.contains("resumes from its checkpoint"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts["paused"], 1);
    }

    #[test]
    fn a_run_from_another_process_is_reported_without_progress() {
        // The CLI's status on the daemon's live run: the store knows it, this process's
        // progress table does not.
        let store = Store::in_memory().expect("store");
        store
            .start_run("guide", "watchdog_apply", false)
            .expect("live");
        let outcome = describe(&App, &context(&store, "status"));
        assert!(
            outcome.summary.contains("In progress: watchdog_apply"),
            "{}",
            outcome.summary
        );
        assert!(!outcome.summary.contains('%'), "{}", outcome.summary);
    }

    #[test]
    fn no_history_and_nothing_running_says_so() {
        let store = Store::in_memory().expect("store");
        let outcome = describe(&App, &context(&store, "status"));
        assert!(
            outcome.summary.starts_with("No run has been recorded yet."),
            "{}",
            outcome.summary
        );
        assert!(outcome.summary.contains("Nothing is running now."));
    }

    #[test]
    fn without_a_store_there_is_nothing_to_report() {
        let outcome = describe(&App, &JobContext::for_tests(CrosstalkConfig::default()));
        assert!(outcome.summary.contains("No run history"));
    }
}
