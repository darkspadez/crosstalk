//! A minimal sub-app, for the tests in this crate.
//!
//! The core cannot reference a real sub-app (they depend on it, not the other way round), so
//! the tests that need one — the registry's, the report builder's — share this stub instead of
//! each growing their own. Compiled only for tests.

use crate::jobs::JobContext;
use crate::subapp::{ActionKind, ActionSpec, JobOutcome, SettingsSchema, SettingsValues, SubApp};
use anyhow::Result;
use std::path::Path;
use std::sync::OnceLock;

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
        static SCHEMA: OnceLock<SettingsSchema> = OnceLock::new();
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
