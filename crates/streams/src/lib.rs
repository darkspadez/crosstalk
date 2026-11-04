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
//!   * [`matching`] — `match_preview` / `match_apply`: Stream-Mapparr's collector (grouping,
//!     the OTA callsign path, the fuzzy winner as a gate, alias force-includes, the
//!     token/substring family, same-country and zone handling) and the write list it plans;
//!   * [`quality`] — the one quality model: Stream-Mapparr's sort key and the throughput tiers
//!     it reads, ported statement for statement;
//!   * [`sort`] — `sort_preview` / `sort_apply`, which is what uses that model;
//!   * [`epg`] — EPG-aware placeholder matching, the optional path that matches a generic
//!     provider slot (`PPV EVENT 04`) by the programme airing on it;
//!   * [`mirror`] — `mirror_preview` / `mirror_apply`: Stream-Mapparr's Fill Mirror Backups,
//!     which gives each channel the same provider stream id from every active host in its M3U
//!     account's server group;
//!   * [`visibility`] — `visibility_preview` / `visibility_apply`: Stream-Mapparr's Manage
//!     Channel Visibility, which leaves one enabled channel per name group in a profile;
//!   * [`tools`] — `check_countries`, `scan_placeholders` and `test_rules`, the three
//!     read-only tools (with [`placeholder_scan`], the numbered-family grouping behind the
//!     second).
//!
//! Probe, triage and restore arrive in later steps, so [`StreamsApp::actions`] registers
//! `validate`, `status`, the `match`, `sort`, `mirror` and `visibility` pairs and the three
//! tools: the layout is there, the rest is not.
//!
//! What the two plugins did that a host does once, this app does not do at all: the dry-run
//! switch is the preview half of a pair (or `--dry-run`), the schedules, windows and
//! post-scan triggers are `[schedules]` / `[workflows]` / `[hooks]` in `crosstalk.toml`, the
//! CSV retention is `[exports]`, the operation lock and progress files are the run store, and
//! `view_*` / `export_results` are the run page and its CSV.

pub mod epg;
pub mod matching;
pub mod mirror;
pub mod placeholder_scan;
pub mod quality;
pub mod selection;
pub mod settings;
pub mod sort;
#[cfg(test)]
mod test_support;
pub mod tools;
pub mod visibility;

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
pub const ACTION_MATCH_PREVIEW: &str = "match_preview";
pub const ACTION_MATCH_APPLY: &str = "match_apply";
pub const ACTION_SORT_PREVIEW: &str = "sort_preview";
pub const ACTION_SORT_APPLY: &str = "sort_apply";
pub const ACTION_MIRROR_PREVIEW: &str = "mirror_preview";
pub const ACTION_MIRROR_APPLY: &str = "mirror_apply";
pub const ACTION_VISIBILITY_PREVIEW: &str = "visibility_preview";
pub const ACTION_VISIBILITY_APPLY: &str = "visibility_apply";
pub const ACTION_CHECK_COUNTRIES: &str = "check_countries";
pub const ACTION_SCAN_PLACEHOLDERS: &str = "scan_placeholders";
pub const ACTION_TEST_RULES: &str = "test_rules";

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
    /// ship without its apply. `match` and `sort` are in place.
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
                ACTION_MATCH_PREVIEW,
                "Match & Assign Streams (Dry Run)",
                "Match every in-scope channel against the selected streams and write a CSV of \
                 what each channel would carry. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_MATCH_APPLY,
                "Match & Assign Streams (Apply)",
                "Give every in-scope channel the streams it matches: replace its list, or \
                 append to it, keeping custom streams last. With Match US OTA Only on, only US \
                 over-the-air channels, by callsign.",
                // Red: with Overwrite Existing Streams on this REMOVES a channel's current
                // assignments. It removes no channel or guide, which is why it is not a
                // delete, but it is destructive to a working lineup.
                ActionKind::Destructive,
            )
            .with_confirm(
                "This will match and assign streams to channels in the scope. With Overwrite \
                 Existing Streams ON it REPLACES each matched channel's whole stream list with \
                 the matched streams (with Keep Custom Streams Last off, hand-made streams go \
                 too); with it OFF it only adds streams a channel does not already have. A \
                 channel that matches nothing is left exactly as it is, and no channel or guide \
                 is removed. Continue?",
            ),
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
            ActionSpec::new(
                ACTION_MIRROR_PREVIEW,
                "Fill Mirror Backups (Dry Run)",
                "Give each in-scope channel the same provider stream id from every active host \
                 in its M3U account's server group, and write a CSV of the additions. Changes \
                 nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_MIRROR_APPLY,
                "Fill Mirror Backups (Apply)",
                "Insert each channel's backup stream from the other hosts in its server group, \
                 after its last provider stream and before any trailing custom stream.",
                // Orange: it rewrites stream lists, but only ever ADDS to them.
                ActionKind::Write,
            )
            .with_confirm(
                "This ADDS stream(s) to every in-scope channel that is missing a backup from a \
                 host in its M3U account's server group, inserting them after the channel's last \
                 provider stream and before any trailing custom stream. It never removes a \
                 stream, never reorders the ones already there, and each changed channel's \
                 previous order is recorded in this run's rows. Continue?",
            ),
            ActionSpec::new(
                ACTION_VISIBILITY_PREVIEW,
                "Manage Channel Visibility (Dry Run)",
                "Work out which channel of each name group stays enabled in the profile \
                 (the first with a stream) and write a CSV of every membership that would \
                 change. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_VISIBILITY_APPLY,
                "Manage Channel Visibility (Apply)",
                "Leave exactly one channel per name group enabled in the profile — the first \
                 with a stream — and disable every other in-scope channel.",
                // Red: a disabled channel disappears from the profile's lineup. Nothing is \
                // deleted, but a whole lineup can go dark if the scope is wrong.
                ActionKind::Destructive,
            )
            .with_confirm(
                "This DISABLES every in-scope channel in the profile except the first channel \
                 of each name group that has at least one stream. A group where no channel has \
                 a stream goes dark entirely, and so does a channel attached to another one. \
                 Only the channels whose membership actually changes are written, and no \
                 channel or stream is deleted. Continue?",
            ),
            ActionSpec::new(
                ACTION_CHECK_COUNTRIES,
                "Check Stream Countries",
                "Compare the country prefix on every stream's group name with the two-letter \
                 suffix on its EPG identifier and write a CSV of where they disagree. Opens no \
                 provider connection and changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_SCAN_PLACEHOLDERS,
                "Scan Placeholder Names",
                "Find numbered stream-name families (MAX 100, PPV EVENT 04) that none of the \
                 placeholder patterns covers, and suggest a regex for each. Changes no setting.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_TEST_RULES,
                "Test Regex Rules",
                "Show what the stream name regex rules would do to every stream name, with a \
                 sample of the changes and the per-rule validation report. Changes nothing.",
                ActionKind::Read,
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
            ACTION_MATCH_PREVIEW => self.match_streams(context, true).await,
            ACTION_MATCH_APPLY => self.match_streams(context, dry_run).await,
            ACTION_SORT_PREVIEW => self.sort(context, true).await,
            ACTION_SORT_APPLY => self.sort(context, dry_run).await,
            ACTION_MIRROR_PREVIEW => self.mirror(context, true).await,
            ACTION_MIRROR_APPLY => self.mirror(context, dry_run).await,
            ACTION_VISIBILITY_PREVIEW => self.visibility(context, true).await,
            ACTION_VISIBILITY_APPLY => self.visibility(context, dry_run).await,
            ACTION_CHECK_COUNTRIES => {
                let settings = self.checked_settings(context)?;
                tools::run_check_countries(self, context, &settings).await
            }
            ACTION_SCAN_PLACEHOLDERS => {
                let settings = self.checked_settings(context)?;
                tools::run_scan_placeholders(self, context, &settings).await
            }
            ACTION_TEST_RULES => {
                let settings = self.checked_settings(context)?;
                tools::run_test_rules(self, context, &settings).await
            }
            other => bail!("{APP_NAME} has no action {other:?}"),
        }
    }

    /// Match the in-scope channels: preview (a read) or apply.
    async fn match_streams(&self, context: &JobContext, dry_run: bool) -> Result<JobOutcome> {
        let settings = self.checked_settings(context)?;
        matching::run(self, context, &settings, dry_run).await
    }

    /// Sort the in-scope channels' streams: preview (a read) or apply.
    async fn sort(&self, context: &JobContext, dry_run: bool) -> Result<JobOutcome> {
        let settings = self.checked_settings(context)?;
        sort::run(self, context, &settings, dry_run).await
    }

    /// Fill each in-scope channel's mirror backups: preview (a read) or apply.
    async fn mirror(&self, context: &JobContext, dry_run: bool) -> Result<JobOutcome> {
        let settings = self.checked_settings(context)?;
        mirror::run(self, context, &settings, dry_run).await
    }

    /// Manage a profile's channel visibility: preview (a read) or apply.
    async fn visibility(&self, context: &JobContext, dry_run: bool) -> Result<JobOutcome> {
        let settings = self.checked_settings(context)?;
        visibility::run(self, context, &settings, dry_run).await
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
                ACTION_MATCH_PREVIEW,
                ACTION_MATCH_APPLY,
                ACTION_SORT_PREVIEW,
                ACTION_SORT_APPLY,
                ACTION_MIRROR_PREVIEW,
                ACTION_MIRROR_APPLY,
                ACTION_VISIBILITY_PREVIEW,
                ACTION_VISIBILITY_APPLY,
                ACTION_CHECK_COUNTRIES,
                ACTION_SCAN_PLACEHOLDERS,
                ACTION_TEST_RULES
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
    fn mirror_apply_adds_and_visibility_apply_disables() {
        let find = |id: &str| {
            app()
                .actions()
                .into_iter()
                .find(|action| action.id == id)
                .unwrap_or_else(|| panic!("{id} is an action"))
        };
        // A mirror fill only ever ADDS: orange, and it says so.
        let mirror = find(ACTION_MIRROR_APPLY);
        assert_eq!(mirror.kind, ActionKind::Write);
        let confirm = mirror.confirm.expect("an apply asks first");
        assert!(confirm.contains("never removes"), "{confirm}");
        assert!(confirm.contains("previous order"), "{confirm}");
        assert_eq!(find(ACTION_MIRROR_PREVIEW).kind, ActionKind::Read);

        // Visibility DISABLES channels: red, and it names the profile and the rule.
        let visibility = find(ACTION_VISIBILITY_APPLY);
        assert_eq!(visibility.kind, ActionKind::Destructive);
        let confirm = visibility.confirm.expect("a destructive action asks first");
        assert!(confirm.contains("DISABLES"), "{confirm}");
        assert!(confirm.contains("name group"), "{confirm}");
        assert_eq!(
            find(ACTION_VISIBILITY_PREVIEW).kind,
            ActionKind::Read,
            "a preview is a read"
        );
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
