//! `guide`: crosstalk's EPG assignment sub-app.
//!
//! The matching engine lives in `crosstalk-matching`, and the parts every sub-app reuses — the
//! channel scope, the matching walk, the AI gate, the report writer — live in `crosstalk-core`.
//! This crate owns the operator-facing half: the actions, the settings, the EPG source filter,
//! the EPG reason wording, and the write itself.
//!
//! Two families of actions, and both are ports: the matching ones (`match_*`, `heal_*`) assign
//! a guide, and the maintenance ones manage a guide that is missing or about to run dry —
//! the cleanup presets (`scan`, then `tag_missing_*`, `clear_missing_*`, `clear_hidden_*`,
//! `clear_by_regex_*`, `clear_groups_*`) remove or mark a guide that carries no programme data,
//! and the watchdog (`watchdog_preview`, `watchdog_apply`) refreshes the EPG sources whose guide
//! is about to expire. The cleanup half is a preset over `crosstalk-core`'s channel operations:
//! it selects, it plans, and the two writes it can issue are a rename and a guide clear. The
//! plugin's anonymous usage counter is still out of scope, and `clear exports` is not an action
//! here but a host feature: one button and one command delete every sub-app's own reports, so
//! each app does not carry its own copy.

pub mod cleanup;
pub mod heal;
pub mod matching;
pub mod scan;
pub mod scope;
pub mod settings;
#[cfg(test)]
mod test_support;
pub mod watchdog;

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
pub const ACTION_STATUS: &str = "status";
pub const ACTION_MATCH_PREVIEW: &str = "match_preview";
pub const ACTION_MATCH_APPLY: &str = "match_apply";
pub const ACTION_HEAL_PREVIEW: &str = "heal_preview";
pub const ACTION_HEAL_APPLY: &str = "heal_apply";
/// The read-only scan whose rows the cleanup actions select from.
pub const ACTION_SCAN: &str = "scan";
pub const ACTION_TAG_MISSING_PREVIEW: &str = "tag_missing_preview";
pub const ACTION_TAG_MISSING_APPLY: &str = "tag_missing_apply";
pub const ACTION_CLEAR_MISSING_PREVIEW: &str = "clear_missing_preview";
pub const ACTION_CLEAR_MISSING_APPLY: &str = "clear_missing_apply";
pub const ACTION_CLEAR_HIDDEN_PREVIEW: &str = "clear_hidden_preview";
pub const ACTION_CLEAR_HIDDEN_APPLY: &str = "clear_hidden_apply";
pub const ACTION_CLEAR_BY_REGEX_PREVIEW: &str = "clear_by_regex_preview";
pub const ACTION_CLEAR_BY_REGEX_APPLY: &str = "clear_by_regex_apply";
pub const ACTION_CLEAR_GROUPS_PREVIEW: &str = "clear_groups_preview";
pub const ACTION_CLEAR_GROUPS_APPLY: &str = "clear_groups_apply";
/// The freshness watchdog: audit every active source, refresh the stale ones.
pub const ACTION_WATCHDOG_PREVIEW: &str = "watchdog_preview";
pub const ACTION_WATCHDOG_APPLY: &str = "watchdog_apply";

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
            ActionSpec::new(
                ACTION_SCAN,
                "Scan for Missing Program Data",
                "Find in-scope channels whose assigned guide carries no programme data in the \
                 check window. Read-only: the result set is stored for the cleanup actions and \
                 written to a CSV.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_TAG_MISSING_PREVIEW,
                "Tag Missing EPG (Dry Run)",
                "Append the Bad EPG Suffix to the channels the last scan found, optionally \
                 clearing their guide. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_TAG_MISSING_APPLY,
                "Tag Missing EPG (Apply)",
                "Append the Bad EPG Suffix to the channels the last scan found, and clear their \
                 guide when 'Also Remove EPG When Adding Suffix' is on. A channel that already \
                 carries the suffix is left entirely alone.",
                ActionKind::Destructive,
            )
            .with_confirm(
                "This renames the channels the last scan found with missing programme data. If \
                 Also Remove EPG When Adding Suffix is on, it REMOVES their EPG assignments as \
                 well. Continue?",
            ),
            ActionSpec::new(
                ACTION_CLEAR_MISSING_PREVIEW,
                "Clear Missing EPG Assignments (Dry Run)",
                "Show which of the last scan's channels would lose their guide. Changes \
                 nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_CLEAR_MISSING_APPLY,
                "Clear Missing EPG Assignments (Apply)",
                "Remove the EPG assignment from the channels the last scan found with missing \
                 programme data.",
                ActionKind::Destructive,
            )
            .with_confirm(
                "This permanently removes EPG assignments from the channels the last scan found \
                 with missing programme data. It changes no channel name. Continue?",
            ),
            ActionSpec::new(
                ACTION_CLEAR_HIDDEN_PREVIEW,
                "Clear EPG from Hidden Channels (Dry Run)",
                "Show which channels disabled in the [scope] profiles would lose their guide. \
                 Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_CLEAR_HIDDEN_APPLY,
                "Clear EPG from Hidden Channels (Apply)",
                "Remove the EPG assignment from every channel disabled in the [scope] profiles. \
                 Unlike the plugin this never deletes programme data, so a visible channel \
                 sharing the entry keeps its guide.",
                ActionKind::Destructive,
            )
            .with_confirm(
                "This removes EPG assignments from every channel hidden (disabled) in the \
                 [scope] profiles. Programme data is never deleted. Continue?",
            ),
            ActionSpec::new(
                ACTION_CLEAR_BY_REGEX_PREVIEW,
                "Clear EPG by REGEX (Dry Run)",
                "Show which channels would lose their guide because its NAME matches the \
                 configured REGEX. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_CLEAR_BY_REGEX_APPLY,
                "Clear EPG by REGEX (Apply)",
                "Remove the EPG assignment where the assigned guide's name matches the \
                 configured REGEX, within [scope].",
                ActionKind::Destructive,
            )
            .with_confirm(
                "This permanently removes EPG assignments from channels whose guide name \
                 matches the configured REGEX. Continue?",
            ),
            ActionSpec::new(
                ACTION_CLEAR_GROUPS_PREVIEW,
                "Clear ALL EPG in Groups (Dry Run)",
                "Show every channel in the [scope] groups that would lose its guide. Changes \
                 nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_CLEAR_GROUPS_APPLY,
                "Clear ALL EPG in Groups (Apply)",
                "Remove the EPG assignment from EVERY channel in the [scope] groups. Refuses \
                 to run with no groups set, unlike the plugin.",
                ActionKind::Destructive,
            )
            .with_confirm(
                "This permanently removes EPG assignments from EVERY channel in the [scope] \
                 groups. This cannot be undone. Continue?",
            ),
            ActionSpec::new(
                ACTION_WATCHDOG_PREVIEW,
                "EPG Freshness Watchdog (Dry Run)",
                "Audit every active EPG source that has channels mapped to it and report which \
                 ones are stale — errored, or with a guide ending inside the threshold. Queues \
                 nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_WATCHDOG_APPLY,
                "EPG Freshness Watchdog (Apply)",
                "Refresh every active EPG source that is errored or whose guide ends inside the \
                 threshold, then re-read it and record whether the refresh helped. Never \
                 renames or unassigns a channel.",
                // Orange: it writes guide data, and removes no channel assignment. The plugin
                // painted this button green because it touched no channel, but it does queue
                // refreshes on Dispatcharr, which is a write.
                ActionKind::Write,
            )
            .with_confirm(
                "This queues a refresh on Dispatcharr for every stale EPG source — an XMLTV \
                 fetch and parse on its Celery worker — and waits for each one. It changes no \
                 channel name and removes no EPG assignment. Continue?",
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
            ACTION_SCAN => self.scan(context).await,
            ACTION_TAG_MISSING_PREVIEW => {
                self.cleanup(context, cleanup::Cleanup::TagMissing, true)
                    .await
            }
            ACTION_TAG_MISSING_APPLY => {
                self.cleanup(context, cleanup::Cleanup::TagMissing, dry_run)
                    .await
            }
            ACTION_CLEAR_MISSING_PREVIEW => {
                self.cleanup(context, cleanup::Cleanup::ClearMissing, true)
                    .await
            }
            ACTION_CLEAR_MISSING_APPLY => {
                self.cleanup(context, cleanup::Cleanup::ClearMissing, dry_run)
                    .await
            }
            ACTION_CLEAR_HIDDEN_PREVIEW => {
                self.cleanup(context, cleanup::Cleanup::ClearHidden, true)
                    .await
            }
            ACTION_CLEAR_HIDDEN_APPLY => {
                self.cleanup(context, cleanup::Cleanup::ClearHidden, dry_run)
                    .await
            }
            ACTION_CLEAR_BY_REGEX_PREVIEW => {
                self.cleanup(context, cleanup::Cleanup::ClearByRegex, true)
                    .await
            }
            ACTION_CLEAR_BY_REGEX_APPLY => {
                self.cleanup(context, cleanup::Cleanup::ClearByRegex, dry_run)
                    .await
            }
            ACTION_CLEAR_GROUPS_PREVIEW => {
                self.cleanup(context, cleanup::Cleanup::ClearGroups, true)
                    .await
            }
            ACTION_CLEAR_GROUPS_APPLY => {
                self.cleanup(context, cleanup::Cleanup::ClearGroups, dry_run)
                    .await
            }
            ACTION_WATCHDOG_PREVIEW => self.watchdog(context, true).await,
            ACTION_WATCHDOG_APPLY => self.watchdog(context, dry_run).await,
            other => bail!("{APP_NAME} has no action {other:?}"),
        }
    }

    /// The scan: read-only, but it records the result set the cleanup actions select from.
    async fn scan(&self, context: &JobContext) -> Result<JobOutcome> {
        let settings = self.checked_settings(context)?;

        let client = context.dispatcharr()?;
        let world = client.load_world().await?;
        let scoped_channels =
            crosstalk_core::scope::scope_channels(&world, &context.config().scope)?;

        let result = scan::run(self, context, &settings, &world, &scoped_channels).await?;
        Ok(scan::outcome(&result))
    }

    /// One cleanup preset: preview (always a read) or apply.
    async fn cleanup(
        &self,
        context: &JobContext,
        action: cleanup::Cleanup,
        dry_run: bool,
    ) -> Result<JobOutcome> {
        let settings = self.checked_settings(context)?;
        cleanup::run(self, context, &settings, action, dry_run).await
    }

    /// The freshness watchdog: preview (audit only) or apply (audit and refresh).
    async fn watchdog(&self, context: &JobContext, dry_run: bool) -> Result<JobOutcome> {
        let settings = self.checked_settings(context)?;
        watchdog::run(self, context, &settings, dry_run).await
    }

    /// This app's settings plus the shared ones a run depends on.
    ///
    /// The AI policy lived in this file until it moved to the host: an inverted band or an
    /// enabled gate with no provider is still a configuration mistake, and a run must refuse it
    /// the way it did before the move. Only the shared scope/AI errors block a run; unrelated
    /// host problems (a missing token, Dispatcharr credentials) are the caller's concern and
    /// are reported by `validate`.
    fn checked_settings(&self, context: &JobContext) -> Result<settings::GuideSettings> {
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

    /// Preview or apply auto-match: load the world, resolve the scope, then match.
    async fn auto_match(&self, context: &JobContext, dry_run: bool) -> Result<JobOutcome> {
        let settings = self.checked_settings(context)?;

        let client = context.dispatcharr()?;
        context.progress(&crosstalk_core::jobs::ProgressEvent::new(
            "guide.match",
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
        let settings = self.checked_settings(context)?;

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
    fn app_declares_every_action_in_order() {
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
                ACTION_SCAN,
                ACTION_TAG_MISSING_PREVIEW,
                ACTION_TAG_MISSING_APPLY,
                ACTION_CLEAR_MISSING_PREVIEW,
                ACTION_CLEAR_MISSING_APPLY,
                ACTION_CLEAR_HIDDEN_PREVIEW,
                ACTION_CLEAR_HIDDEN_APPLY,
                ACTION_CLEAR_BY_REGEX_PREVIEW,
                ACTION_CLEAR_BY_REGEX_APPLY,
                ACTION_CLEAR_GROUPS_PREVIEW,
                ACTION_CLEAR_GROUPS_APPLY,
                ACTION_WATCHDOG_PREVIEW,
                ACTION_WATCHDOG_APPLY,
            ]
        );
        // The plugin's own ids are never copied, and its CSV housekeeping is a host feature
        // rather than an action.
        assert!(!ids.contains(&"watchdog_run_check_now"));
        assert!(!ids.contains(&"clear_csv_exports"));
    }

    #[test]
    fn every_writing_action_is_a_preview_apply_pair() {
        // The naming rule for every sub-app: an action that writes is `<verb>_preview` /
        // `<verb>_apply`, so a schedule key and an API caller can guess the pair.
        let actions = app().actions();
        let ids: Vec<&str> = actions.iter().map(|action| action.id).collect();
        for id in ids.iter().copied().filter(|id| id.ends_with("_apply")) {
            let verb = id.trim_end_matches("_apply");
            assert!(
                ids.contains(&format!("{verb}_preview").as_str()),
                "{id} has no {verb}_preview"
            );
        }
        for id in ids.iter().copied().filter(|id| id.ends_with("_preview")) {
            let verb = id.trim_end_matches("_preview");
            assert!(
                ids.contains(&format!("{verb}_apply").as_str()),
                "{id} has no {verb}_apply"
            );
            let kind = actions
                .iter()
                .find(|action| action.id == id)
                .expect("action")
                .kind;
            assert_eq!(kind, ActionKind::Read, "a preview is a read: {id}");
        }
    }

    #[test]
    fn the_cleanup_actions_confirm_and_are_red() {
        // Every cleanup apply can remove a guide assignment, which is the plugin's red, and every
        // red confirms.
        let actions = app().actions();
        for id in [
            ACTION_TAG_MISSING_APPLY,
            ACTION_CLEAR_MISSING_APPLY,
            ACTION_CLEAR_HIDDEN_APPLY,
            ACTION_CLEAR_BY_REGEX_APPLY,
            ACTION_CLEAR_GROUPS_APPLY,
        ] {
            let action = actions
                .iter()
                .find(|action| action.id == id)
                .expect("action exists");
            assert_eq!(action.kind, ActionKind::Destructive, "{id}");
            assert!(action.kind.requires_confirmation(), "{id}");
            assert!(action.confirm.is_some(), "{id} confirms");
            assert!(action.needs_dispatcharr, "{id} talks to Dispatcharr");
        }
        // The scan is a read and needs no confirmation.
        let scan = actions
            .iter()
            .find(|action| action.id == ACTION_SCAN)
            .expect("scan");
        assert_eq!(scan.kind, ActionKind::Read);
        assert!(scan.confirm.is_none());
        assert!(!scan.kind.touches_channels());
    }

    #[test]
    fn tag_missing_is_red_because_it_can_clear_a_guide() {
        // `add_bad_epg_suffix` is a rename alone, but with `remove_epg_with_suffix` on it also
        // unassigns, so the pairing — not the current setting — decides the colour.
        let actions = app().actions();
        let action = actions
            .iter()
            .find(|action| action.id == ACTION_TAG_MISSING_APPLY)
            .expect("action");
        assert_eq!(action.kind, ActionKind::Destructive);
        assert!(
            action
                .confirm
                .unwrap_or_default()
                .contains("REMOVES their EPG assignments"),
            "the plugin's own warning is carried over"
        );
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
        assert_eq!(kind(ACTION_SCAN), ActionKind::Read);
        // Auto-match can move an assignment that already works: red.
        assert_eq!(kind(ACTION_MATCH_APPLY), ActionKind::Destructive);
        // Heal only replaces assignments it proved empty, and removes none: orange.
        assert_eq!(kind(ACTION_HEAL_APPLY), ActionKind::Write);
        // The cleanup applies all remove an assignment: red, as in the plugin.
        assert_eq!(kind(ACTION_TAG_MISSING_APPLY), ActionKind::Destructive);
        assert_eq!(kind(ACTION_CLEAR_MISSING_APPLY), ActionKind::Destructive);
        assert_eq!(kind(ACTION_CLEAR_HIDDEN_APPLY), ActionKind::Destructive);
        assert_eq!(kind(ACTION_CLEAR_BY_REGEX_APPLY), ActionKind::Destructive);
        assert_eq!(kind(ACTION_CLEAR_GROUPS_APPLY), ActionKind::Destructive);
        // The watchdog queues refreshes on Dispatcharr but touches no channel: orange, where
        // the plugin painted its button green for the same reason.
        assert_eq!(kind(ACTION_WATCHDOG_PREVIEW), ActionKind::Read);
        assert_eq!(kind(ACTION_WATCHDOG_APPLY), ActionKind::Write);
    }

    #[test]
    fn the_watchdog_apply_confirms_and_touches_no_channel() {
        let actions = app().actions();
        let apply = actions
            .iter()
            .find(|action| action.id == ACTION_WATCHDOG_APPLY)
            .expect("action");
        // Orange and red always confirm, so the operator sees that refreshes are queued.
        assert_eq!(apply.kind, ActionKind::Write);
        assert!(apply.kind.requires_confirmation());
        assert!(apply.confirm.is_some());
        assert!(apply.needs_dispatcharr);
        // Orange is "writes data, removes no assignment": the watchdog queues refreshes and
        // never touches a channel.
        assert!(apply.kind.touches_channels());
        assert!(apply
            .confirm
            .unwrap_or_default()
            .contains("changes no channel name"));

        let preview = actions
            .iter()
            .find(|action| action.id == ACTION_WATCHDOG_PREVIEW)
            .expect("action");
        assert_eq!(preview.kind, ActionKind::Read);
        assert!(preview.confirm.is_none());
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
    fn the_shipped_example_configs_load_and_validate() {
        // The release ships both files for operators to copy; a key renamed in code but not
        // there would make the copied file refuse to load (`deny_unknown_fields`).
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
        let settings = settings::load(&root.join("guide.example.toml")).expect("guide example");
        assert!(settings::validate(&settings).is_empty());

        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::copy(
            root.join("crosstalk.example.toml"),
            dir.path().join("crosstalk.toml"),
        )
        .expect("copy");
        let config = CrosstalkConfig::load_file(dir.path()).expect("host example");
        assert!(
            config
                .validate()
                .errors
                .iter()
                .all(|error| !error.contains("scope.")
                    && !error.contains("ai.verify.")
                    && !error.contains("exports.")),
            "{:?}",
            config.validate().errors
        );
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
    async fn status_reports_the_run_before_itself() {
        // The status run is recorded like any other, so it is always the newest row.
        let world = test_support::world();
        let fixture = test_support::Fixture::start(&world, &[]).await;
        let store = crosstalk_core::store::Store::in_memory().expect("store");
        fixture.record_scan(&store, &[]);
        let context = fixture.context_for_run(APP_ID, ACTION_STATUS, &store);

        let outcome = app()
            .dispatch(ACTION_STATUS, &context, true)
            .await
            .expect("status");
        assert!(
            outcome.summary.starts_with("Last run: scan succeeded"),
            "{}",
            outcome.summary
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
