//! `guide`: crosstalk's EPG assignment sub-app.
//!
//! The matching engine lives in `crosstalk-matching`, and the parts every sub-app reuses — the
//! channel scope, the matching walk, the AI gate, the report writer — live in `crosstalk-core`.
//! This crate owns the operator-facing half: the actions, the settings, the EPG source filter,
//! the EPG reason wording, and the write itself.
//!
//! Only the matching actions are implemented. The Python plugin's cleanup actions (remove
//! by regex, strip from hidden channels, add a bad-EPG suffix) and its freshness watchdog are
//! deliberately out of scope for this version, as is the anonymous usage counter. `clear
//! exports` is not an action here but a host feature: one button and one command delete every
//! sub-app's own reports, so each app does not carry its own copy.

pub mod heal;
pub mod matching;
pub mod scope;
pub mod settings;

use anyhow::{bail, Result};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{
    ActionKind, ActionSpec, JobOutcome, SettingsSchema, SettingsValues, SubApp,
};
use std::path::Path;
use std::sync::OnceLock;

pub const APP_ID: &str = "guide";
pub const APP_NAME: &str = "Guide";

/// The action ids, as used in schedules and URLs.
pub const ACTION_VALIDATE: &str = "validate";
pub const ACTION_MATCH_PREVIEW: &str = "match_preview";
pub const ACTION_MATCH_APPLY: &str = "match_apply";
pub const ACTION_HEAL_PREVIEW: &str = "heal_preview";
pub const ACTION_HEAL_APPLY: &str = "heal_apply";
pub const ACTION_STATUS: &str = "status";

/// The sub-app itself.
#[derive(Debug, Default)]
pub struct GuideApp;

impl GuideApp {
    pub fn new() -> Self {
        Self
    }
}

impl SubApp for GuideApp {
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
                ACTION_MATCH_PREVIEW,
                "Preview Auto-Match (Dry Run)",
                "Score every in-scope channel against the eligible EPG entries and write a \
                 CSV. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_MATCH_APPLY,
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

impl GuideApp {
    async fn dispatch(
        &self,
        action: &str,
        context: &JobContext,
        dry_run: bool,
    ) -> Result<JobOutcome> {
        match action {
            ACTION_VALIDATE => self.validate(context),
            ACTION_STATUS => Ok(self.status(context)),
            ACTION_MATCH_PREVIEW => self.auto_match(context, true).await,
            ACTION_MATCH_APPLY => self.auto_match(context, dry_run).await,
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
        // The AI policy lived in this file until it moved to the host: an inverted band or
        // an enabled gate with no provider is still a configuration mistake, and a run must
        // refuse it the way it did before the move. Only the shared scope/AI errors block a
        // run; unrelated host problems (a missing token, Dispatcharr credentials) are the
        // caller's concern and are reported by `validate`.
        let host_errors: Vec<String> = context
            .config()
            .validate()
            .errors
            .into_iter()
            .filter(|error| error.contains("scope.") || error.contains("ai.verify."))
            .collect();
        if !host_errors.is_empty() {
            bail!("invalid settings: {}", host_errors.join("; "));
        }

        let client = context.dispatcharr()?;
        context.progress(&crosstalk_core::jobs::ProgressEvent::new(
            ACTION_MATCH_PREVIEW,
            0,
            0,
            "loading Dispatcharr data",
        ));
        let world = client.load_world().await?;

        let scoped_channels =
            crosstalk_core::scope::scope_channels(&world, &context.config().scope)?;
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

        // Built once per run: one session, one concurrency cap, one audit trail. The policy
        // is the host's `[ai.verify]`, shared with every other sub-app that matches names.
        let ai = crosstalk_core::ai_gate::service(context.config());

        let result = matching::run(
            self,
            context,
            &settings,
            ai.as_ref(),
            &world,
            &scoped_channels,
            &scoped_entries,
            dry_run,
        )
        .await?;
        Ok(matching::outcome(&result, dry_run))
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

        let scoped_channels =
            crosstalk_core::scope::scope_channels(&world, &context.config().scope)?;
        let filter = scope::heal_source_filter(&settings);
        let scoped_entries = scope::scope_entries(&world, filter);

        let result = heal::run(
            self,
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

    /// What the last runs did, from the store rather than from a live session.
    fn status(&self, context: &JobContext) -> JobOutcome {
        let Some(store) = context.store() else {
            return JobOutcome::new("No run history is available in this context.");
        };
        let runs = store.recent_runs_for(APP_ID, 1).unwrap_or_default();
        let Some(run) = runs.into_iter().next() else {
            return JobOutcome::new("No run has been recorded yet.");
        };

        let mut outcome = JobOutcome::new(format!(
            "Last run: {} {} ({}) started {}",
            run.action,
            run.status,
            if run.dry_run { "preview" } else { "apply" },
            run.started_at
        ));
        if let Some(summary) = run.summary {
            outcome = JobOutcome::new(format!("{} — {summary}", outcome.summary));
        }
        for (key, value) in store
            .rows(run.id)
            .map(|rows| rows.len())
            .map(|count| ("rows", count as i64))
            .into_iter()
        {
            outcome = outcome.with_count(key, value);
        }
        outcome
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
            if context.config().ai.verify.enabled {
                "on"
            } else {
                "off"
            }
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

    fn app() -> GuideApp {
        GuideApp::new()
    }

    #[test]
    fn app_declares_the_matching_actions_only() {
        let ids: Vec<&str> = app().actions().iter().map(|action| action.id).collect();
        assert_eq!(
            ids,
            vec![
                ACTION_VALIDATE,
                ACTION_STATUS,
                ACTION_MATCH_PREVIEW,
                ACTION_MATCH_APPLY,
                ACTION_HEAL_PREVIEW,
                ACTION_HEAL_APPLY,
            ]
        );
        // The cleanup actions and the watchdog are out of scope for this version.
        assert!(!ids.contains(&"remove_epg_by_regex"));
        assert!(!ids.contains(&"watchdog_run_check_now"));
    }

    #[test]
    fn the_app_names_its_own_exports() {
        // The prefix and the preamble title come from the sub-app, so the host's "clear
        // exports" and the retention rule agree with what the runs actually wrote.
        assert_eq!(app().export_prefix(), "guide_");
        assert_eq!(
            app().report_title("Auto-Match Preview Report"),
            format!(
                "# crosstalk Guide v{} - Auto-Match Preview Report",
                crosstalk_core::HOST_VERSION
            )
        );
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
        assert_eq!(kind(ACTION_MATCH_PREVIEW), ActionKind::Read);
        assert_eq!(kind(ACTION_HEAL_PREVIEW), ActionKind::Read);
        // Auto-match can move an assignment that already works: red.
        assert_eq!(kind(ACTION_MATCH_APPLY), ActionKind::Destructive);
        // Heal only replaces assignments it proved empty, and removes none: orange.
        assert_eq!(kind(ACTION_HEAL_APPLY), ActionKind::Write);
    }

    #[test]
    fn apply_actions_are_destructive_and_confirm() {
        let actions = app().actions();
        let apply = actions
            .iter()
            .find(|action| action.id == ACTION_MATCH_APPLY)
            .expect("apply action");
        assert_eq!(apply.kind, ActionKind::Destructive);
        assert!(apply.confirm.is_some());
        assert!(apply.kind.requires_confirmation());

        let preview = actions
            .iter()
            .find(|action| action.id == ACTION_MATCH_PREVIEW)
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
        values.insert(
            "epg_sources_to_match",
            FieldValue::Text("Schedules Direct".to_string()),
        );
        app().save_settings(&path, &values).expect("save");

        let reloaded = app().load_settings(&path).expect("reload");
        assert_eq!(reloaded.number("check_hours"), Some(6.0));
        assert_eq!(
            reloaded.text("epg_sources_to_match"),
            Some("Schedules Direct")
        );
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
        values.insert("check_hours", FieldValue::Number(-1.0));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("between"), "{error}");
    }

    #[test]
    fn validate_settings_catches_a_hand_edited_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = app().settings_path(dir.path());
        std::fs::write(&path, "match_confidence_threshold = 0\n").expect("seed");
        let error = app().validate_settings(&path).expect_err("rejects");
        assert!(format!("{error}").contains("at least 1"));
    }

    #[test]
    fn save_rejects_a_settings_file_that_still_holds_a_moved_key() {
        // The clean break: `selected_groups` now lives under [scope] in crosstalk.toml, and
        // the app says so rather than quietly ignoring a value the operator can see.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = app().settings_path(dir.path());
        std::fs::write(&path, "selected_groups = \"A\"\n").expect("seed");
        let error = app().load_settings(&path).expect_err("rejects the old key");
        assert!(format!("{error:#}").contains("crosstalk.toml"), "{error:#}");
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
    async fn an_apply_with_an_inverted_ai_band_is_refused_before_dispatch() {
        // The AI policy moved to the host file, but a run must still refuse it the way it
        // did when the band lived in guide.toml.
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(dir.path()).expect("load");
        config.ai.verify.verify_band_low = 95;
        config.ai.verify.verify_band_high = 80;
        let context = JobContext::for_tests(config);
        let error = app()
            .dispatch(ACTION_MATCH_PREVIEW, &context, true)
            .await
            .expect_err("inverted band");
        assert!(
            format!("{error}").contains("ai.verify.verify_band_low"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn an_apply_without_a_session_says_what_is_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let context = JobContext::for_tests(CrosstalkConfig::load(dir.path()).expect("load"));
        let error = app()
            .dispatch(ACTION_MATCH_APPLY, &context, false)
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
