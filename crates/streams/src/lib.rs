//! `streams`: crosstalk's stream sub-app.
//!
//! The streams app owns which streams a channel has, the order they play in, and whether they
//! work. It is the port of two Dispatcharr plugins: Stream-Mapparr (matching, sorting, mirror
//! fills, visibility) and IPTV Checker (probing, triage, restore). Phase 2 of the consolidation
//! plan replaces their 44 actions with eight `<verb>_preview` / `<verb>_apply` pairs plus three
//! read-only tools, and this crate grows a module per verb:
//!
//!   * [`settings`] — `streams.toml`, whose field ids and defaults are the plugins' own;
//!   * [`selection`] — the stream half of the scope: M3U accounts in priority order, stream
//!     groups, and the regex rules that decide the name matching sees;
//!   * [`quality`] — the one quality model: Stream-Mapparr's sort key and the throughput tiers
//!     it reads, ported statement for statement;
//!   * [`sort`] — `sort_preview` / `sort_apply`, which is what uses that model today.
//!
//! Matching, mirror, visibility, probe, triage and restore arrive in later steps, so
//! [`StreamsApp::actions`] registers `validate`, `status` and the `sort` pair: the layout is
//! there, the halves that write are not (all of them).
//!
//! What the two plugins did that a host does once, this app does not do at all: the dry-run
//! switch is the preview half of a pair (or `--dry-run`), the schedules, windows and
//! post-scan triggers are `[schedules]` / `[workflows]` / `[hooks]` in `crosstalk.toml`, the
//! CSV retention is `[exports]`, the operation lock and progress files are the run store, and
//! `view_*` / `export_results` are the run page and its CSV.

pub mod quality;
pub mod selection;
pub mod settings;
pub mod sort;
#[cfg(test)]
mod test_support;

use anyhow::{bail, Result};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{
    ActionKind, ActionSpec, JobOutcome, SettingsSchema, SettingsValues, SubApp,
};
use std::path::Path;
use std::sync::OnceLock;

pub const APP_ID: &str = "streams";
pub const APP_NAME: &str = "Streams";

/// The action ids, as used in schedules and URLs.
pub const ACTION_VALIDATE: &str = "validate";
pub const ACTION_STATUS: &str = "status";
pub const ACTION_SORT_PREVIEW: &str = "sort_preview";
pub const ACTION_SORT_APPLY: &str = "sort_apply";

/// The sub-app itself.
#[derive(Debug, Default)]
pub struct StreamsApp;

impl StreamsApp {
    pub fn new() -> Self {
        Self
    }
}

impl SubApp for StreamsApp {
    fn id(&self) -> &'static str {
        APP_ID
    }

    fn name(&self) -> &'static str {
        APP_NAME
    }

    fn description(&self) -> &'static str {
        "Own which streams a channel has, the order they play in, and whether they work: \
         match, sort, mirror, probe and triage streams."
    }

    /// The actions, in the order the UI shows them.
    ///
    /// The layout the later steps extend: `validate` and `status` first, then one preview/apply
    /// pair per verb — `match`, `sort`, `mirror`, `probe` (one action, it writes stream stats),
    /// `triage`, `restore`, `visibility` — then the read-only tools (`check_countries`,
    /// `scan_placeholders`, `test_rules`). A pair is declared together, so a preview can never
    /// ship without its apply. `sort` is the first pair in place.
    fn actions(&self) -> Vec<ActionSpec> {
        vec![
            ActionSpec::new(
                ACTION_VALIDATE,
                "Validate Settings",
                "Check the settings for consistency. Reach Dispatcharr separately, by \
                 previewing a run.",
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
                ACTION_SORT_PREVIEW,
                "Sort Alternate Streams (Dry Run)",
                "Order every in-scope channel's streams by the quality model (and by feed zone) \
                 and write a CSV. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_SORT_APPLY,
                "Sort Alternate Streams (Apply)",
                "Rewrite the stream order of every channel whose alternates would reorder, \
                 first stream first.",
                // Orange: it rewrites playback order, which is what decides failover. It never \
                // removes a channel or a guide.
                ActionKind::Write,
            )
            .with_confirm(
                "This REPLACES the stream order of every channel whose alternates would reorder. \
                 Zone routing keeps only the channel's own feed zone and can drop the other \
                 zone's streams from its list. It removes no channel. Continue?",
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

impl StreamsApp {
    async fn dispatch(
        &self,
        action: &str,
        context: &JobContext,
        dry_run: bool,
    ) -> Result<JobOutcome> {
        // Every pair is declared preview-first, and an apply handed a dry run honours it.
        match action {
            ACTION_VALIDATE => self.validate(context),
            ACTION_STATUS => Ok(self.status(context)),
            ACTION_SORT_PREVIEW => self.sort(context, true).await,
            ACTION_SORT_APPLY => self.sort(context, dry_run).await,
            other => bail!("{APP_NAME} has no action {other:?}"),
        }
    }

    /// Sort the in-scope channels' streams: preview (a read) or apply.
    async fn sort(&self, context: &JobContext, dry_run: bool) -> Result<JobOutcome> {
        let settings = self.checked_settings(context)?;
        sort::run(self, context, &settings, dry_run).await
    }

    /// This app's settings, refusing the ones a run would trip over.
    ///
    /// Only the shared errors a run would actually hit are fatal here: the channel scope and
    /// the AI policy. Unrelated host problems (a missing token, Dispatcharr credentials) are
    /// the caller's concern and are reported by `validate`.
    fn checked_settings(&self, context: &JobContext) -> Result<settings::StreamsSettings> {
        let settings = settings::load(&self.settings_path(&context.config().config_dir))?;
        let errors = settings::validate(&settings);
        if !errors.is_empty() {
            bail!("invalid settings: {}", errors.join("; "));
        }
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
        Ok(settings)
    }

    /// What the last runs did, from the store rather than from a live session.
    fn status(&self, context: &JobContext) -> JobOutcome {
        let Some(store) = context.store() else {
            return JobOutcome::new("No run history is available in this context.");
        };
        // This status run is recorded too, and is always the newest: report the one before it.
        let runs = store.recent_runs_for(APP_ID, 2).unwrap_or_default();
        let Some(run) = runs
            .into_iter()
            .find(|run| Some(run.id) != context.run_id())
        else {
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
        if let Ok(rows) = store.rows(run.id) {
            outcome = outcome.with_count("rows", rows.len() as i64);
        }
        outcome
    }

    fn validate(&self, context: &JobContext) -> Result<JobOutcome> {
        let path = self.settings_path(&context.config().config_dir);
        let settings = self.checked_settings(context)?;
        let warnings = settings::warnings(&settings);
        let config_report = context.config().validate();

        if !config_report.is_ok() {
            bail!("{}", config_report.errors.join("; "));
        }

        // What the settings resolve to, so an operator can see the numbers a run will use
        // without reading the file: the preset is a name, the run needs a threshold.
        let rules = selection::RuleSet::parse(&settings.stream_name_regex_rules);
        let mut outcome = JobOutcome::new(format!(
            "Settings valid. match_sensitivity {:?} is a threshold of {}, {} regex rule(s) \
             active, AI verification {}.",
            settings.match_sensitivity,
            settings.match_threshold(),
            rules.rules.len(),
            if context.config().ai.verify.enabled {
                "on"
            } else {
                "off"
            }
        ));
        outcome = outcome.with_count(
            "warnings",
            (config_report.warnings.len() + warnings.len()) as i64,
        );
        outcome = outcome.with_count("regex_rules", rules.rules.len() as i64);
        outcome = outcome.with_count("regex_rules_skipped", rules.rejected().count() as i64);
        // A rejected rule and a fallback preset are the plugin's degrade, not a refusal: they
        // are reported here rather than blocking the run.
        for warning in &warnings {
            tracing::warn!("{}: {warning}", path.display());
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::subapp::FieldValue;
    use crosstalk_core::{config::CrosstalkConfig, store::Store};
    use test_support::Fixture;

    fn app() -> StreamsApp {
        StreamsApp::new()
    }

    #[test]
    fn app_declares_its_actions_in_order() {
        let ids: Vec<&str> = app().actions().iter().map(|action| action.id).collect();
        assert_eq!(
            ids,
            vec![
                ACTION_VALIDATE,
                ACTION_STATUS,
                ACTION_SORT_PREVIEW,
                ACTION_SORT_APPLY
            ]
        );
        // The plugin's own ids are never copied, and its housekeeping is a host feature.
        for retired in [
            "validate_settings",
            "update_schedule",
            "view_last_results",
            "clear_csv_exports",
            "clear_operation_lock",
            "check_scheduler_status",
        ] {
            assert!(!ids.contains(&retired), "{retired} is not an action");
        }
    }

    #[test]
    fn every_reading_action_confirms_nothing() {
        for action in app().actions() {
            if action.kind != ActionKind::Read {
                continue;
            }
            assert!(!action.kind.touches_channels(), "{}", action.id);
            assert!(action.confirm.is_none(), "{}", action.id);
        }
    }

    #[test]
    fn the_offline_actions_are_the_two_that_need_no_dispatcharr() {
        // `validate` has to run before the credentials are right, and `status` reads the run
        // store. `sort_preview` reads Dispatcharr, so it is a read that still needs it.
        let offline: Vec<&str> = app()
            .actions()
            .into_iter()
            .filter(|action| !action.needs_dispatcharr)
            .map(|action| action.id)
            .collect();
        assert_eq!(offline, vec![ACTION_VALIDATE, ACTION_STATUS]);
    }

    #[test]
    fn sort_apply_writes_and_is_confirmed() {
        let action = app()
            .actions()
            .into_iter()
            .find(|action| action.id == ACTION_SORT_APPLY)
            .expect("the apply half of the pair");
        assert_eq!(action.kind, ActionKind::Write);
        assert!(action.needs_dispatcharr);
        let confirm = action.confirm.expect("a write asks first");
        assert!(confirm.contains("stream order"), "{confirm}");
        assert!(confirm.contains("feed zone"), "{confirm}");
    }

    #[test]
    fn the_app_declares_its_own_export_prefix() {
        assert_eq!(app().export_prefix(), "streams_");
        assert_eq!(
            app().report_title("Match Preview Report"),
            format!(
                "# crosstalk Streams v{} - Match Preview Report",
                crosstalk_core::HOST_VERSION
            )
        );
    }

    #[test]
    fn settings_round_trip_through_the_app() {
        let fixture = Fixture::new();
        let path = app().settings_path(fixture.dir.path());

        let mut values = app().load_settings(&path).expect("load defaults");
        values.insert("probe_freshness_hours", FieldValue::Number(6.0));
        values.insert(
            "selected_m3us",
            FieldValue::Text("Main Provider, *".to_string()),
        );
        app().save_settings(&path, &values).expect("save");

        let reloaded = app().load_settings(&path).expect("reload");
        assert_eq!(reloaded.number("probe_freshness_hours"), Some(6.0));
        assert_eq!(reloaded.text("selected_m3us"), Some("Main Provider, *"));
    }

    #[test]
    fn save_rejects_an_unknown_field_a_fraction_and_an_out_of_range_number() {
        let fixture = Fixture::new();
        let path = app().settings_path(fixture.dir.path());

        let mut values = SettingsValues::new();
        values.insert("not_a_setting", FieldValue::Text("x".into()));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("unknown setting"));

        let mut values = SettingsValues::new();
        values.insert("timeout", FieldValue::Number(6.7));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("whole number"), "{error}");

        let mut values = SettingsValues::new();
        values.insert("probe_per_account_limit", FieldValue::Number(0.0));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("between"), "{error}");
    }

    #[test]
    fn save_rejects_a_settings_file_that_still_holds_a_retired_key() {
        let fixture = Fixture::new();
        let path = app().settings_path(fixture.dir.path());
        std::fs::write(&path, "profile_name = \"All\"\n").expect("seed");
        let error = app().load_settings(&path).expect_err("rejects the old key");
        assert!(format!("{error:#}").contains("crosstalk.toml"), "{error:#}");
    }

    #[tokio::test]
    async fn validate_reports_configuration_problems_and_then_passes() {
        let fixture = Fixture::new();
        let context = fixture.context();
        // Defaults have no Dispatcharr credentials.
        let error = app()
            .dispatch(ACTION_VALIDATE, &context, true)
            .await
            .expect_err("errors");
        assert!(format!("{error}").contains("credentials"), "{error:#}");

        let fixture = Fixture::with_credentials();
        let context = fixture.context();
        let outcome = app()
            .dispatch(ACTION_VALIDATE, &context, true)
            .await
            .expect("ok");
        assert!(
            outcome.summary.contains("Settings valid"),
            "{}",
            outcome.summary
        );
        assert!(
            outcome.summary.contains("threshold of 80"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("warnings"), Some(&0));
        assert_eq!(outcome.counts.get("regex_rules"), Some(&0));
    }

    #[tokio::test]
    async fn validate_counts_the_regex_rules_it_could_use() {
        let fixture = Fixture::with_credentials();
        std::fs::write(
            fixture.settings_path(),
            "stream_name_regex_rules = '[[\"^ESPN HD$\", \"ESPN\"], [\"(unclosed\", \"x\"]]'\n",
        )
        .expect("seed");
        let outcome = app()
            .dispatch(ACTION_VALIDATE, &fixture.context(), true)
            .await
            .expect("valid: a rejected rule degrades, it does not refuse");
        assert_eq!(outcome.counts.get("regex_rules"), Some(&1));
        assert_eq!(outcome.counts.get("regex_rules_skipped"), Some(&1));
        assert_eq!(outcome.counts.get("warnings"), Some(&1));
    }

    #[tokio::test]
    async fn validate_refuses_a_hand_edited_file_that_is_out_of_bounds() {
        let fixture = Fixture::with_credentials();
        std::fs::write(fixture.settings_path(), "visible_channel_limit = 0\n").expect("seed");
        let error = app()
            .dispatch(ACTION_VALIDATE, &fixture.context(), true)
            .await
            .expect_err("refuses");
        assert!(
            format!("{error}").contains("visible_channel_limit"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn status_reports_the_run_before_itself() {
        // The status run is recorded like any other, so it is always the newest row.
        let fixture = Fixture::new();
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run(APP_ID, ACTION_VALIDATE, true)
            .expect("start");
        store
            .finish_run(
                run_id,
                crosstalk_core::store::RunStatus::Succeeded,
                Some("recorded by the test"),
                &serde_json::json!({"channels": 3}),
                None,
            )
            .expect("finish");
        let context = fixture.context().for_run(
            store
                .start_run(APP_ID, ACTION_STATUS, true)
                .expect("status run"),
            store.clone(),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );

        let outcome = app()
            .dispatch(ACTION_STATUS, &context, true)
            .await
            .expect("status");
        assert!(
            outcome.summary.starts_with("Last run: validate succeeded"),
            "{}",
            outcome.summary
        );
        assert!(outcome.summary.contains("recorded by the test"));
    }

    #[tokio::test]
    async fn status_says_so_when_there_is_no_history() {
        let fixture = Fixture::new();
        let outcome = app()
            .dispatch(ACTION_STATUS, &fixture.context(), true)
            .await
            .expect("status");
        assert!(
            outcome.summary.contains("No run history"),
            "{}",
            outcome.summary
        );
    }

    #[tokio::test]
    async fn unknown_actions_are_rejected() {
        let context = JobContext::for_tests(CrosstalkConfig::default());
        let error = app()
            .dispatch("no_such_action", &context, true)
            .await
            .expect_err("unknown");
        assert!(format!("{error}").contains("no action"));
    }
}
