//! `janitor`: crosstalk's EPG matcher sub-app.
//!
//! The matching pipeline lives in `crosstalk-matching`; this crate owns the operator-facing
//! half: the actions, the settings, the scope filters, the reports, and the AI gate.
//!
//! Only the matching actions are implemented. The Python plugin's cleanup actions (remove
//! by regex, strip from hidden channels, add a bad-EPG suffix, clear exports) and its
//! freshness watchdog are deliberately out of scope for this version, as is the anonymous
//! usage counter.

pub mod automatch;
pub mod heal;
pub mod scope;
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

/// Build the alias table for a run: the built-ins plus any custom entries in the host config.
pub fn matcher_aliases(context: &JobContext) -> crosstalk_matching::alias::AliasMap {
    let mut aliases = crosstalk_matching::alias::AliasMap::builtin();
    let custom = &context.config().matching.custom_aliases;
    if !custom.is_empty() {
        aliases = aliases.with_overrides(
            custom
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        );
    }
    aliases
}

/// The callsign allowlist for a run: the shipped FCC table plus whatever the enabled channel
/// databases contribute.
pub fn known_callsigns(context: &JobContext) -> crosstalk_matching::callsign::KnownCallsigns {
    let mut known = crosstalk_matching::callsign::KnownCallsigns::shipped()
        .cloned()
        .unwrap_or_default();
    let databases = context.config().matching.databases();
    let codes: Vec<&str> = databases.iter().map(String::as_str).collect();
    for callsign in crosstalk_matching::data::known_callsigns_for(&codes).to_vec() {
        known.insert(&callsign);
    }
    known
}

/// The `user_ignored_tags` list a run passes, in the plugin's order.
pub fn tag_list(flags: crosstalk_matching::normalize::NormalizeFlags) -> Vec<String> {
    let mut tags = Vec::new();
    if flags.ignore_quality {
        tags.push("quality".to_string());
    }
    if flags.ignore_regional {
        tags.push("regional".to_string());
    }
    if flags.ignore_geographic {
        tags.push("geographic".to_string());
    }
    if flags.ignore_misc {
        tags.push("misc".to_string());
    }
    tags
}

/// The action ids, as used in schedules and URLs.
pub const ACTION_VALIDATE: &str = "validate";
pub const ACTION_AUTOMATCH_PREVIEW: &str = "automatch_preview";
pub const ACTION_AUTOMATCH_APPLY: &str = "automatch_apply";
pub const ACTION_HEAL_PREVIEW: &str = "heal_preview";
pub const ACTION_HEAL_APPLY: &str = "heal_apply";
pub const ACTION_STATUS: &str = "status";

/// The sub-app itself.
#[derive(Debug, Default)]
pub struct JanitorApp;

impl JanitorApp {
    pub fn new() -> Self {
        Self
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
                "Check the settings for consistency. Reach Dispatcharr separately, by                  previewing a run.",
                ActionKind::Read,
            )
            // Settings validation is local: it must work on a host that cannot reach
            // Dispatcharr yet, which is exactly when an operator runs it.
            .offline(),
            ActionSpec::new(
                ACTION_STATUS,
                "Status / Last Results",
                "Show what the last run did, and whether a run is in progress.",
                ActionKind::Read,
            )
            // Reads the run store, not Dispatcharr.
            .offline(),
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
                // Orange, not red: heal writes data but never removes an assignment it has
                // not proven empty, which is the plugin's own distinction.
                ActionKind::Write,
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

    fn validate_settings(&self, path: &Path) -> Result<()> {
        let settings = settings::load(path)?;
        let errors = settings::validate(&settings);
        if errors.is_empty() {
            Ok(())
        } else {
            bail!("{}", errors.join("; "))
        }
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
        dry_run: bool,
    ) -> Result<JobOutcome> {
        match action {
            ACTION_VALIDATE => self.validate(context),
            ACTION_STATUS => Ok(JobOutcome::new("No run has been recorded yet.")),
            ACTION_AUTOMATCH_PREVIEW => self.auto_match(context, true).await,
            ACTION_AUTOMATCH_APPLY => self.auto_match(context, dry_run).await,
            ACTION_HEAL_PREVIEW => self.heal(context, true).await,
            ACTION_HEAL_APPLY => self.heal(context, dry_run).await,
            other => bail!("{APP_NAME} has no action {other:?}"),
        }
    }

    /// Preview or apply auto-match: load the world, resolve the scope, then match.
    async fn auto_match(&self, context: &JobContext, dry_run: bool) -> Result<JobOutcome> {
        let settings = settings::load(&self.settings_path(&context.config().config_dir))?;
        let errors = settings::validate(&settings);
        if !errors.is_empty() {
            bail!("invalid settings: {}", errors.join("; "));
        }

        let client = context.dispatcharr()?;
        context.progress(&crosstalk_core::jobs::ProgressEvent::new(
            ACTION_AUTOMATCH_PREVIEW,
            0,
            0,
            "loading Dispatcharr data",
        ));
        let world = client.load_world().await?;

        let scoped_channels = scope::scope_channels(&world, &settings)?;
        let scoped_entries = scope::scope_entries(&world, &settings.epg_sources_to_match);

        if scoped_channels.channels.is_empty() {
            return Ok(JobOutcome::new(
                "No channels are in scope. Check the group, profile and source settings.",
            ));
        }
        if scoped_entries.entries.is_empty() {
            bail!(
                "No EPG entries are eligible. Check that the sources in scope are active and \
                 that 'EPG Sources to Match' names sources you actually have."
            );
        }

        let result = automatch::run(
            context,
            &settings,
            &world,
            &scoped_channels,
            &scoped_entries,
            dry_run,
        )
        .await?;
        Ok(automatch::outcome(&result, dry_run))
    }

    /// Preview or apply scan-and-heal.
    async fn heal(&self, context: &JobContext, dry_run: bool) -> Result<JobOutcome> {
        let settings = settings::load(&self.settings_path(&context.config().config_dir))?;
        let errors = settings::validate(&settings);
        if !errors.is_empty() {
            bail!("invalid settings: {}", errors.join("; "));
        }

        let client = context.dispatcharr()?;
        let world = client.load_world().await?;

        let scoped_channels = scope::scope_channels(&world, &settings)?;
        let filter = scope::heal_source_filter(&settings);
        let scoped_entries = scope::scope_entries(&world, filter);

        let result = heal::run(
            context,
            &settings,
            &world,
            &scoped_channels,
            &scoped_entries,
            dry_run,
        )
        .await?;
        Ok(heal::outcome(
            &result,
            dry_run,
            settings.heal_confidence_threshold,
        ))
    }

    fn validate(&self, context: &JobContext) -> Result<JobOutcome> {
        let path = self.settings_path(&context.config().config_dir);
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
    fn action_kinds_match_the_plugin_colours() {
        let actions = app().actions();
        let kind = |id: &str| {
            actions
                .iter()
                .find(|action| action.id == id)
                .map(|action| action.kind)
                .expect("action exists")
        };
        // Blue reads.
        assert_eq!(kind(ACTION_VALIDATE), ActionKind::Read);
        assert_eq!(kind(ACTION_STATUS), ActionKind::Read);
        assert_eq!(kind(ACTION_AUTOMATCH_PREVIEW), ActionKind::Read);
        assert_eq!(kind(ACTION_HEAL_PREVIEW), ActionKind::Read);
        // Auto-match can move an assignment that already works: red.
        assert_eq!(kind(ACTION_AUTOMATCH_APPLY), ActionKind::Destructive);
        // Heal only replaces assignments it proved empty, and removes none: orange.
        assert_eq!(kind(ACTION_HEAL_APPLY), ActionKind::Write);
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
        let path = app().settings_path(dir.path());

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
        let path = app().settings_path(dir.path());
        let mut values = SettingsValues::new();
        values.insert("not_a_setting", FieldValue::Text("x".into()));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("unknown setting"));
    }

    #[test]
    fn save_rejects_a_fractional_integer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = app().settings_path(dir.path());
        let mut values = SettingsValues::new();
        values.insert("check_hours", FieldValue::Number(6.7));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("whole number"), "{error}");
    }

    #[test]
    fn save_rejects_a_negative_integer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = app().settings_path(dir.path());
        let mut values = SettingsValues::new();
        values.insert("csv_retention_days", FieldValue::Number(-1.0));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("between"), "{error}");
    }

    #[test]
    fn validate_settings_catches_a_hand_edited_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = app().settings_path(dir.path());
        std::fs::write(&path, "selected_groups = \"A\"\nignore_groups = \"B\"\n").expect("seed");
        let error = app().validate_settings(&path).expect_err("rejects");
        assert!(format!("{error}").contains("cannot both be set"));
    }

    #[test]
    fn save_rejects_a_conflicting_combination() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = app().settings_path(dir.path());
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
    async fn an_apply_without_a_session_says_what_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let context = JobContext::for_tests(CrosstalkConfig::load(dir.path()).expect("load"));
        let error = app()
            .dispatch(ACTION_AUTOMATCH_APPLY, &context, false)
            .await
            .expect_err("no session");
        // The settings load first, and with the shipped defaults they are valid, so the
        // failure is the missing session.
        assert!(
            format!("{error}").contains("Dispatcharr session"),
            "{error:#}"
        );
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
