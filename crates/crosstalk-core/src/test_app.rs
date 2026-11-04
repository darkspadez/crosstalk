//! A minimal sub-app, for the tests in this crate.
//!
//! The core cannot reference a real sub-app (they depend on it, not the other way round), so
//! the tests that need one — the registry's, the report builder's, the runner's — share this
//! stub instead of each growing their own. Compiled only for tests.

use crate::jobs::JobContext;
use crate::subapp::{ActionKind, ActionSpec, JobOutcome, SettingsSchema, SettingsValues, SubApp};
use anyhow::Result;
use std::path::Path;

/// Two actions, no settings, and a run that does nothing.
#[derive(Debug, Default)]
pub struct StubApp;

impl StubApp {
    pub fn new() -> Self {
        Self
    }
}

impl SubApp for StubApp {
    fn id(&self) -> &'static str {
        "stub"
    }

    fn name(&self) -> &'static str {
        "Stub"
    }

    fn description(&self) -> &'static str {
        "test app"
    }

    fn actions(&self) -> Vec<ActionSpec> {
        vec![
            ActionSpec::new("preview", "Preview", "reads", ActionKind::Read),
            ActionSpec::new("apply", "Apply", "writes", ActionKind::Write),
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
        _context: &'a JobContext,
        _dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>> {
        Box::pin(async { Ok(JobOutcome::new("ok")) })
    }
}

/// A long job, shrunk: `units` work units, one per `per_unit`, each checkpointed.
///
/// It is the shape `streams.probe` has without a network: it reads its place out of the
/// checkpoint, does one unit at a time, records a row per unit, stops when
/// [`JobContext::past_deadline`] says the window closed — saving its place first — and stops when
/// [`JobContext::is_cancelled`] says the operator did. The tests in `runner` drive it through
/// the real runner, so what they prove is the host's pause/resume/cancel/fresh behaviour rather
/// than this app's.
///
/// [`JobContext::past_deadline`]: crate::jobs::JobContext::past_deadline
/// [`JobContext::is_cancelled`]: crate::jobs::JobContext::is_cancelled
#[derive(Debug, Default)]
pub struct ResumableApp;

/// The unit count [`ResumableApp`] works through.
pub const RESUMABLE_UNITS: i64 = 10;

/// How long one unit of [`ResumableApp`]'s work takes.
pub const RESUMABLE_PER_UNIT: std::time::Duration = std::time::Duration::from_millis(10);

impl ResumableApp {
    pub fn new() -> Self {
        Self
    }
}

impl SubApp for ResumableApp {
    fn id(&self) -> &'static str {
        "resumable"
    }

    fn name(&self) -> &'static str {
        "Resumable"
    }

    fn description(&self) -> &'static str {
        "a long job that pauses at its window and resumes from its checkpoint"
    }

    fn actions(&self) -> Vec<ActionSpec> {
        vec![
            ActionSpec::new("run", "Run", "works through units", ActionKind::Read).offline(),
            // The one action allowed to run beside another of its app.
            ActionSpec::new("status", "Status", "reads the store", ActionKind::Read)
                .offline()
                .unguarded(),
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
        action: &'a str,
        context: &'a JobContext,
        _dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>> {
        Box::pin(async move {
            if action == "status" {
                // Fast and read-only, like the real `status`: the point of it is to run beside
                // a live run of the same app.
                return Ok(JobOutcome::new("ok"));
            }
            // Where the last window got to. A resumed run continues; a fresh start finds none.
            let done = context
                .load_checkpoint()
                .and_then(|state| state.get("done").and_then(serde_json::Value::as_i64))
                .unwrap_or(0);
            let resumed = done > 0;
            let mut done = done;

            while done < RESUMABLE_UNITS {
                if context.is_cancelled() {
                    // Cancelled: the checkpoint stays, as IPTV Checker left its resume file, so
                    // the next window continues. The runner records the run cancelled.
                    anyhow::bail!("cancelled by the operator at unit {done}");
                }
                if context.past_deadline() {
                    // The window closed: save the place, say so, and let the next firing finish.
                    context.save_checkpoint(&serde_json::json!({"done": done}))?;
                    return Ok(JobOutcome::paused(format!(
                        "paused at the window's end after {done} of {RESUMABLE_UNITS} unit(s); \
                         the next run resumes here"
                    ))
                    .with_count("done", done)
                    .with_count("remaining", RESUMABLE_UNITS - done));
                }
                tokio::time::sleep(RESUMABLE_PER_UNIT).await;
                done += 1;
                context.record_rows(&[crate::store::RunRow {
                    ordinal: done,
                    channel_id: done,
                    channel_name: format!("unit {done}"),
                    status: "done".to_string(),
                    ..Default::default()
                }])?;
                context.save_checkpoint(&serde_json::json!({"done": done}))?;
            }

            // The whole backlog is done, so the next run starts from the beginning.
            context.clear_checkpoint()?;
            let mut outcome = JobOutcome::new(format!(
                "finished {RESUMABLE_UNITS} unit(s){}",
                if resumed {
                    ", resuming from the checkpoint"
                } else {
                    ""
                }
            ))
            .with_count("done", done);
            if resumed {
                outcome = outcome.with_count("resumed", 1);
            }
            Ok(outcome)
        })
    }
}
