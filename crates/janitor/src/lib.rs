//! `janitor`: crosstalk's EPG matcher sub-app.
//!
//! The matching pipeline lives in `crosstalk-matching`; this crate owns the operator-facing
//! half: the actions, the settings, the scope filters, the reports, and the AI gate.
//!
//! Only the matching actions are implemented. The Python plugin's cleanup actions (remove
//! by regex, strip from hidden channels, add a bad-EPG suffix, clear exports) and its
//! freshness watchdog are deliberately out of scope for this version, as is the anonymous
//! usage counter.

pub mod settings;

use anyhow::{bail, Result};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{
    ActionKind, ActionSpec, JobOutcome, SettingsSchema, SettingsValues, SubApp,
};
use std::path::Path;
use std::sync::OnceLock;

pub const APP_ID: &str = "janitor";
pub const APP_NAME: &str = "EPG Janitor";

/// The action ids, as used in schedules and URLs.
pub const ACTION_VALIDATE: &str = "validate";
pub const ACTION_AUTOMATCH_PREVIEW: &str = "automatch_preview";
pub const ACTION_AUTOMATCH_APPLY: &str = "automatch";
pub const ACTION_HEAL_PREVIEW: &str = "heal_preview";
pub const ACTION_HEAL_APPLY: &str = "heal";
pub const ACTION_STATUS: &str = "status";

/// The sub-app itself.
#[derive(Debug, Default)]
pub struct JanitorApp;

impl JanitorApp {
    pub fn new() -> Self {
        Self
    }

    /// The settings file this app reads, given the host config directory.
    pub fn settings_path(config_dir: &Path) -> std::path::PathBuf {
        config_dir.join("janitor.toml")
    }
}

impl SubApp for JanitorApp {
    fn id(&self) -> &'static str {
        APP_ID
    }

    fn name(&self) -> &'static str {
        APP_NAME
    }

    fn description(&self) -> &'static str {
        "Match Dispatcharr channels to EPG sources: assign guides where they are missing \
         and repair assignments that carry no programme data."
    }

    fn actions(&self) -> Vec<ActionSpec> {
        vec![
            ActionSpec::new(
                ACTION_VALIDATE,
                "Validate Settings",
                "Check the settings and the connection to Dispatcharr.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_STATUS,
                "Status / Last Results",
                "Show what the last run did, and whether a run is in progress.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_AUTOMATCH_PREVIEW,
                "Preview Auto-Match (Dry Run)",
                "Score every in-scope channel against the eligible EPG entries and write a \
                 CSV. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_AUTOMATCH_APPLY,
                "Apply Auto-Match EPG Assignments",
                "Assign the matched EPG entry to every channel that clears the confidence \
                 threshold.",
                ActionKind::Destructive,
            )
            .with_confirm(
                "This assigns EPG data to matched channels and OVERWRITES an existing \
                 assignment when a new match scores above the threshold. Continue?",
            ),
            ActionSpec::new(
                ACTION_HEAL_PREVIEW,
                "Scan & Heal (Dry Run)",
                "Find assignments with no programme data and look for working replacements, \
                 without changing anything.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_HEAL_APPLY,
                "Scan & Heal (Apply Changes)",
                "Replace assignments that carry no programme data with a working one.",
                ActionKind::Destructive,
            )
            .with_confirm(
                "This replaces broken EPG assignments with working ones. An assignment that \
                 cannot be proven empty is never touched. Continue?",
            ),
        ]
    }

    fn schema(&self) -> &SettingsSchema {
        static SCHEMA: OnceLock<SettingsSchema> = OnceLock::new();
        SCHEMA.get_or_init(settings::schema)
    }

    fn load_settings(&self, path: &Path) -> Result<SettingsValues> {
        let settings = settings::load(path)?;
        Ok(settings::to_values(&settings))
    }

    fn save_settings(&self, path: &Path, values: &SettingsValues) -> Result<()> {
        let schema = settings::schema();
        let errors = schema.validate(values);
        if !errors.is_empty() {
            bail!("invalid settings: {}", errors.join("; "));
        }

        // Start from what is on disk, so anything this form does not render survives.
        let mut settings = settings::load(path)?;
        settings::apply_values(&mut settings, values)?;

        let errors = settings::validate(&settings);
        if !errors.is_empty() {
            bail!("invalid settings: {}", errors.join("; "));
        }
        settings::save(path, &settings)
    }

    fn run<'a>(
        &'a self,
        action: &'a str,
        context: &'a JobContext,
        dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>> {
        Box::pin(async move { self.dispatch(action, context, dry_run).await })
    }
}

impl JanitorApp {
    async fn dispatch(
        &self,
        action: &str,
        context: &JobContext,
        _dry_run: bool,
    ) -> Result<JobOutcome> {
        match action {
            ACTION_VALIDATE => self.validate(context),
            ACTION_STATUS => Ok(JobOutcome::new("No run has been recorded yet.")),
            ACTION_AUTOMATCH_PREVIEW
            | ACTION_AUTOMATCH_APPLY
            | ACTION_HEAL_PREVIEW
            | ACTION_HEAL_APPLY => bail!(
                "{APP_NAME} action {action:?} needs the Dispatcharr client, which is not \
                 wired into this build yet"
            ),
            other => bail!("{APP_NAME} has no action {other:?}"),
        }
    }

    fn validate(&self, context: &JobContext) -> Result<JobOutcome> {
        let path = Self::settings_path(&context.config().config_dir);
        let settings = settings::load(&path)?;
        let config_report = context.config().validate();
        let settings_errors = settings::validate(&settings);

        if !config_report.is_ok() || !settings_errors.is_empty() {
            let mut problems = config_report.errors.clone();
            problems.extend(settings_errors);
            bail!("{}", problems.join("; "));
        }

        let mut outcome = JobOutcome::new(format!(
            "Settings valid. {} channel database(s) enabled, AI verification {}.",
            context.config().matching.databases().len(),
            if settings.ai.enabled { "on" } else { "off" }
        ));
        outcome = outcome.with_count("warnings", config_report.warnings.len() as i64);
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::config::CrosstalkConfig;
    use crosstalk_core::subapp::FieldValue;

    fn app() -> JanitorApp {
        JanitorApp::new()
    }

    #[test]
    fn app_declares_the_matching_actions_only() {
        let ids: Vec<&str> = app().actions().iter().map(|action| action.id).collect();
        assert_eq!(
            ids,
            vec![
                ACTION_VALIDATE,
                ACTION_STATUS,
                ACTION_AUTOMATCH_PREVIEW,
                ACTION_AUTOMATCH_APPLY,
                ACTION_HEAL_PREVIEW,
                ACTION_HEAL_APPLY,
            ]
        );
        // The cleanup actions and the watchdog are out of scope for this version.
        assert!(!ids.contains(&"remove_epg_by_regex"));
        assert!(!ids.contains(&"watchdog_run_check_now"));
    }

    #[test]
    fn apply_actions_are_destructive_and_confirm() {
        let actions = app().actions();
        let apply = actions
            .iter()
            .find(|action| action.id == ACTION_AUTOMATCH_APPLY)
            .expect("apply action");
        assert_eq!(apply.kind, ActionKind::Destructive);
        assert!(apply.confirm.is_some());
        assert!(apply.kind.requires_confirmation());

        let preview = actions
            .iter()
            .find(|action| action.id == ACTION_AUTOMATCH_PREVIEW)
            .expect("preview action");
        assert_eq!(preview.kind, ActionKind::Read);
        assert!(!preview.kind.touches_channels());
    }

    #[test]
    fn settings_round_trip_through_the_app() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = JanitorApp::settings_path(dir.path());

        let mut values = app().load_settings(&path).expect("load defaults");
        values.insert("check_hours", FieldValue::Number(6.0));
        values.insert("ai.enabled", FieldValue::Boolean(true));
        values.insert("ai.model", FieldValue::Text("jev-1.14.0".to_string()));
        app().save_settings(&path, &values).expect("save");

        let reloaded = app().load_settings(&path).expect("reload");
        assert_eq!(reloaded.number("check_hours"), Some(6.0));
        assert_eq!(reloaded.bool("ai.enabled"), Some(true));
        assert_eq!(reloaded.text("ai.model"), Some("jev-1.14.0"));
    }

    #[test]
    fn save_rejects_an_unknown_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = JanitorApp::settings_path(dir.path());
        let mut values = SettingsValues::new();
        values.insert("not_a_setting", FieldValue::Text("x".into()));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("unknown setting"));
    }

    #[test]
    fn save_rejects_a_conflicting_combination() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = JanitorApp::settings_path(dir.path());
        let mut values = SettingsValues::new();
        values.insert("selected_groups", FieldValue::Text("A".into()));
        values.insert("ignore_groups", FieldValue::Text("B".into()));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("cannot both be set"));
    }

    #[tokio::test]
    async fn validate_reports_configuration_problems() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = CrosstalkConfig::load(dir.path()).expect("load");
        let context = JobContext::for_tests(config);

        // Defaults have no Dispatcharr credentials.
        let error = app()
            .dispatch(ACTION_VALIDATE, &context, true)
            .await
            .expect_err("errors");
        assert!(format!("{error}").contains("credentials"));
    }

    #[tokio::test]
    async fn validate_passes_with_credentials() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(dir.path()).expect("load");
        config.dispatcharr.api_key = "test-key".to_string();
        let context = JobContext::for_tests(config);

        let outcome = app()
            .dispatch(ACTION_VALIDATE, &context, true)
            .await
            .expect("ok");
        assert!(outcome.summary.contains("Settings valid"));
        assert_eq!(outcome.counts.get("warnings"), Some(&0));
    }

    #[tokio::test]
    async fn unimplemented_actions_say_what_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let context = JobContext::for_tests(CrosstalkConfig::load(dir.path()).expect("load"));
        let error = app()
            .dispatch(ACTION_AUTOMATCH_PREVIEW, &context, true)
            .await
            .expect_err("not built yet");
        assert!(format!("{error}").contains("Dispatcharr client"));
    }

    #[tokio::test]
    async fn unknown_actions_are_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let context = JobContext::for_tests(CrosstalkConfig::load(dir.path()).expect("load"));
        let error = app()
            .dispatch("no_such_action", &context, true)
            .await
            .expect_err("unknown");
        assert!(format!("{error}").contains("no action"));
    }
}
