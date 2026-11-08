//! `lineup`: crosstalk's channel-list sub-app.
//!
//! The lineup app owns the channel list itself: names, numbers, groups, logos, and
//! creating channels. It is the port of three Dispatcharr plugins — Lineuparr (sync,
//! stream/EPG match, logos, tidy), Channel-Mapparr (names, categorize, logos, create)
//! and Channel Name Cleaner (clean, test_name) — and this crate grows a module per verb
//! the way `guide` and `streams` do:
//!
//!   * [`settings`] — `lineup.toml`, whose field ids and defaults are the plugins' own;
//!   * [`lineup_file`] — the 29 embedded lineup files, and Lineuparr's loader pipeline
//!     (country from the `CC_` name prefix, the category, and the filename;
//!     `category_detail` folding; group names; numbers);
//!   * `clean` (step 14), `names` (step 15), `categorize` + `logos` (step 16),
//!     `create` (step 17), `sync` + `tidy` (step 18) — one module per verb, each landing
//!     with its own step and its own oracle report.
//!
//! Steps 13-15 register `validate`, `status`, the `clean_*` pair + `test_name` (14)
//! and the `names_*` pair (15); step 16 the `categorize_*` and `logos_*` pairs;
//! step 17 the `create_*` pair: the settings, the loader, the registry wiring and the
//! named `lineup` scope. The action ids below name the pairs the later steps register,
//! so a schedule key and an API caller can guess the pair from the start: every writing
//! action is a `<verb>_preview` / `<verb>_apply` pair, and a preview is always a read.
//!
//! What the three plugins did that a host does once, this app does not do at all: the
//! dry-run switch is the preview half of a pair (or `--dry-run`), the schedules, windows
//! and post-scan triggers are `[schedules]` / `[workflows]` / `[hooks]` in
//! `crosstalk.toml`, the CSV retention is `[exports]`, the usage counters are not ported,
//! and `clear_csv_exports` is the host's `exports clear`.

pub mod bench;
pub mod categorize;
pub mod clean;
pub mod create;
pub mod lineup_file;
pub mod logos;
pub mod names;
pub mod settings;
pub mod sync;
#[cfg(test)]
mod test_support;
pub mod tidy;

use anyhow::{bail, Result};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{
    ActionKind, ActionSpec, JobOutcome, NamedScope, NamedScopeResolution, SettingsSchema,
    SettingsValues, SubApp,
};
use std::path::Path;
use std::sync::OnceLock;

pub const APP_ID: &str = "lineup";
pub const APP_NAME: &str = "Lineup";

/// The action ids, as used in schedules and URLs.
pub const ACTION_VALIDATE: &str = "validate";
pub const ACTION_STATUS: &str = "status";
// The pairs the later steps register (steps 14-18). Declared here so the plan's action
// table has one home from the skeleton on; `actions()` grows a pair per step.
pub const ACTION_CLEAN_PREVIEW: &str = "clean_preview";
pub const ACTION_CLEAN_APPLY: &str = "clean_apply";
pub const ACTION_TEST_NAME: &str = "test_name";
pub const ACTION_NAMES_PREVIEW: &str = "names_preview";
pub const ACTION_NAMES_APPLY: &str = "names_apply";
pub const ACTION_CATEGORIZE_PREVIEW: &str = "categorize_preview";
pub const ACTION_CATEGORIZE_APPLY: &str = "categorize_apply";
pub const ACTION_LOGOS_PREVIEW: &str = "logos_preview";
pub const ACTION_LOGOS_APPLY: &str = "logos_apply";
pub const ACTION_CREATE_PREVIEW: &str = "create_preview";
pub const ACTION_CREATE_APPLY: &str = "create_apply";
pub const ACTION_SYNC_PREVIEW: &str = "sync_preview";
pub const ACTION_SYNC_APPLY: &str = "sync_apply";
pub const ACTION_TIDY_PREVIEW: &str = "tidy_preview";
pub const ACTION_TIDY_APPLY: &str = "tidy_apply";

/// The named scope this app offers workflow steps (`scope = "lineup"`).
pub const NAMED_SCOPE_LINEUP: &str = "lineup";

/// The ownership key prefix sync/create records under: `lineup:<file>:...`, where `<file>`
/// is the full embedded filename (`US_Combined_lineup.json`, never the short name, so one
/// lineup has one prefix whichever spelling the setting uses). What follows the second
/// `:` is sync's business (step 18). The named scope resolves the ids owned under
/// [`owner_key_prefix`] for the configured lineup file.
pub const OWNER_KEY_PREFIX: &str = "lineup:";

/// The ownership key prefix of one lineup file: `lineup:<full filename>:`.
pub fn owner_key_prefix(lineup_file: &str) -> Result<String> {
    let short = lineup_file::normalize_lineup_name(lineup_file)
        .ok_or_else(|| anyhow::anyhow!("unknown lineup file {lineup_file:?}"))?;
    Ok(format!(
        "{OWNER_KEY_PREFIX}{}:",
        lineup_file::full_filename(short)
    ))
}

/// The sub-app itself.
#[derive(Debug, Default)]
pub struct LineupApp;

impl LineupApp {
    pub fn new() -> Self {
        Self
    }
}

impl SubApp for LineupApp {
    fn id(&self) -> &'static str {
        APP_ID
    }

    fn name(&self) -> &'static str {
        APP_NAME
    }

    fn description(&self) -> &'static str {
        "Own the channel list itself: names, numbers, groups, logos, and creating channels."
    }

    /// The actions, in the order the UI shows them.
    ///
    /// Steps 13-18 register `validate`, `status`, the `clean_*` pair + `test_name`,
    /// the `names_*`, `categorize_*`, `logos_*`, `create_*`, `sync_*` and `tidy_*`
    /// pairs — preview first, so a preview can never ship without its apply.
    fn actions(&self) -> Vec<ActionSpec> {
        vec![
            ActionSpec::new(
                ACTION_VALIDATE,
                "Validate Settings",
                "Check the settings and the configured lineup file for consistency. Reach \
                 Dispatcharr separately, by previewing a run.",
                ActionKind::Read,
            )
            // Settings validation is local: it must work on a host that cannot reach
            // Dispatcharr yet, which is exactly when an operator runs it.
            .offline(),
            ActionSpec::new(
                ACTION_STATUS,
                "Status / Last Results",
                "Show what the last run did, and what is running now — including its progress \
                 when this daemon started it.",
                ActionKind::Read,
            )
            // Reads the run store, not Dispatcharr.
            .offline()
            // Unguarded: refusing it while another run is live is backwards, because that
            // run is what the operator is asking about.
            .unguarded(),
            ActionSpec::new(
                ACTION_CLEAN_PREVIEW,
                "Preview Cleanup (Dry Run)",
                "Compute before → after for every in-scope channel and write a CSV. Changes \
                 nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_CLEAN_APPLY,
                "Apply Cleanup",
                "Rename the channels the preview would change (collisions handled per the \
                 On Collision setting). Undoable through the host undo.",
                ActionKind::Write,
            )
            .with_confirm(
                "This will rename channels in Dispatcharr. The host undo can reverse it. \
                 Continue?",
            ),
            ActionSpec::new(
                ACTION_TEST_NAME,
                "Test a Name",
                "Run the clean rules against a single name from the action's input field, \
                 and show which rules fired, what protection hit, and what identity every \
                 app will match it as. Changes nothing.",
                ActionKind::Read,
            )
            // Local: the cleaning is pure and the identity resolves in-process.
            .offline()
            .with_input(
                "input",
                "Test a Name",
                "Type a channel name, then Run: shows the cleaned name, the fired rules, \
                 and the unified identity (canonical, callsign, zone, country).",
            ),
            ActionSpec::new(
                ACTION_NAMES_PREVIEW,
                "Preview Names (Dry Run)",
                "Compute the canonical name for every in-scope channel (OTA template, \
                 premium canonical + tags, unknown suffix) and write a CSV. Changes \
                 nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_NAMES_APPLY,
                "Apply Names",
                "Rename the channels the preview would change (collisions handled per the \
                 On Collision setting). Undoable through the host undo.",
                ActionKind::Write,
            )
            .with_confirm(
                "This will rename channels in Dispatcharr. The host undo can reverse it. \
                 Continue?",
            ),
            ActionSpec::new(
                ACTION_CATEGORIZE_PREVIEW,
                "Preview Categorize (Dry Run)",
                "Compute the category group each in-scope channel moves to (creating \
                 missing groups) and write a CSV. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_CATEGORIZE_APPLY,
                "Apply Categorize",
                "Create the missing category groups and move the channels the preview \
                 would move. Undoable through the host undo.",
                ActionKind::Write,
            )
            .with_confirm(
                "This will create channel groups and move channels in Dispatcharr. The \
                 host undo can reverse the moves. Continue?",
            ),
            ActionSpec::new(
                ACTION_LOGOS_PREVIEW,
                "Preview Logos (Dry Run)",
                "Compute the logo each in-scope channel takes (EPG icon, Logo Manager, \
                 tv-logos, default) and write a CSV. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_LOGOS_APPLY,
                "Apply Logos",
                "Create the missing logos and assign the logos the preview would assign. \
                 Undoable through the host undo.",
                ActionKind::Write,
            )
            .with_confirm(
                "This will create logos and assign them to channels in Dispatcharr. The \
                 host undo can reverse the assignments. Continue?",
            ),
            ActionSpec::new(
                ACTION_CREATE_PREVIEW,
                "Preview Create (Dry Run)",
                "Compute the channels (and groups) the streams or stations mode would \
                 create and write a CSV. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_CREATE_APPLY,
                "Apply Create",
                "Create the channels (and groups) the preview would create: one channel \
                 per identity family (streams mode) or per OTA station (stations mode). \
                 Undoable through the host undo (created channels hide).",
                ActionKind::Write,
            )
            .with_confirm(
                "This will create channels and groups in Dispatcharr. The host undo can \
                 hide the created channels. Continue?",
            ),
            ActionSpec::new(
                ACTION_SYNC_PREVIEW,
                "Preview Sync (Dry Run)",
                "Compute the groups, channels, renumbers and moves the configured lineup \
                 file implies and write a CSV. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_SYNC_APPLY,
                "Apply Sync",
                "Create the missing lineup groups and channels, then renumber and move \
                 the owned channels that drifted. Owned channels only; renames never. \
                 Undoable through the host undo.",
                ActionKind::Write,
            )
            .with_confirm(
                "This will create channels and groups and move channels in Dispatcharr. \
                 The host undo can reverse the edits and hide the created channels. \
                 Continue?",
            ),
            ActionSpec::new(
                ACTION_TIDY_PREVIEW,
                "Preview Tidy (Dry Run)",
                "Compute which owned lineup channels would hide (no streams) or unhide \
                 (streams again after tidy hid them) and write a CSV. Changes nothing.",
                ActionKind::Read,
            ),
            ActionSpec::new(
                ACTION_TIDY_APPLY,
                "Apply Tidy",
                "Hide the owned lineup channels left with no streams, and unhide the \
                 ones that regained streams after tidy hid them. Owned channels only. \
                 Undoable through the host undo.",
                ActionKind::Destructive,
            )
            .with_confirm(
                "This will hide channels in Dispatcharr. The host undo can unhide them. \
                 Continue?",
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

    /// The named scope workflow steps resolve through: `scope = "lineup"`.
    ///
    /// The channels lineup owns for its configured lineup file — the ids the store
    /// records under the `lineup:<file>:` ownership prefix. Empty until a sync or a
    /// create records ownership (nothing owns anything yet, so the set starts empty);
    /// a step scoped to it then reads no channels rather than every channel: the bound
    /// REPLACES the host `[scope]`, it never falls back to it.
    ///
    /// Only the CONFIGURED file's channels: after the operator switches `lineup_file`,
    /// the old lineup's channels stay owned (undo, hide-when-empty and re-sync of that
    /// lineup still know them) but leave the scope, so a `full_sync` over the new lineup
    /// never re-matches or tidies the old one's channels. Switching back brings them back.
    fn named_scopes(&self) -> Vec<NamedScope> {
        vec![NamedScope::new(
            NAMED_SCOPE_LINEUP,
            "The channels lineup owns for its configured lineup file (empty until a sync \
             or create records ownership)",
        )]
    }

    /// Resolve `scope = "lineup"` to the owned channel ids, with the lineup file's
    /// country as the hint.
    ///
    /// `None` means "not mine" (any other name). `Some` is every channel id the store
    /// records this app owning under the configured file's [`owner_key_prefix`], plus
    /// the file's country for steps that restrict by it (streams' lineup mode). Ids
    /// with no channel in the loaded world are dropped by the scope resolution itself,
    /// so a channel deleted upstream is gone, not an error. A settings file that does
    /// not load (or names an unknown lineup) fails the step rather than scoping it to
    /// nothing or to everything.
    fn resolve_named_scope(
        &self,
        name: &str,
        config: &crosstalk_core::config::CrosstalkConfig,
        _world: &crosstalk_core::dispatcharr::World,
        store: &crosstalk_core::store::Store,
    ) -> Option<Result<NamedScopeResolution>> {
        if name.trim() != NAMED_SCOPE_LINEUP {
            return None;
        }
        Some(self.owned_lineup_channels(&config.config_dir, store))
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

impl LineupApp {
    /// The channel ids owned under the configured lineup file's ownership prefix,
    /// with the file's country as the scope hint.
    ///
    /// The hint is the filename's two-letter country prefix
    /// ([`lineup_file::filename_country`]); a file without one carries no country
    /// hint, and a streams step over it then matches like an ordinary run (only an
    /// explicit `restrict_matching_to_country` restricts). Every vendored file has
    /// one. Per-entry
    /// countries (`CC_` prefixes, category countries) are NOT folded in here: the
    /// hint is the default, and a channel whose identity names a different known
    /// country keeps its own (decided in streams' lineup mode, step 19).
    fn owned_lineup_channels(
        &self,
        config_dir: &Path,
        store: &crosstalk_core::store::Store,
    ) -> Result<NamedScopeResolution> {
        let settings = settings::load(&self.settings_path(config_dir))?;
        let prefix = owner_key_prefix(&settings.lineup_file)?;
        let mut ids: Vec<i64> = store
            .owned_keys(APP_ID)?
            .into_iter()
            .filter(|(kind, _, owner_key)| kind == "channel" && owner_key.starts_with(&prefix))
            .map(|(_, object_id, _)| object_id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        // The hint rides the same spelling the prefix pins (short or full filename,
        // `normalize_lineup_name` settles it), so the country can never disagree with
        // the channel set: both name the configured file.
        let filename = lineup_file::normalize_lineup_name(&settings.lineup_file)
            .map(lineup_file::full_filename)
            .unwrap_or_else(|| settings.lineup_file.clone());
        let mut hints = crosstalk_core::subapp::NamedScopeHints::none();
        hints.country = lineup_file::filename_country(&filename);
        Ok(NamedScopeResolution::ChannelsWithHints { ids, hints })
    }

    async fn dispatch(
        &self,
        action: &str,
        context: &JobContext,
        dry_run: bool,
    ) -> Result<JobOutcome> {
        // Every pair is declared preview-first, and an apply handed a dry run honours it:
        // `clean_apply` with a dry run previews instead of writing.
        match action {
            ACTION_VALIDATE => self.validate(context),
            ACTION_STATUS => Ok(self.status(context)),
            ACTION_CLEAN_PREVIEW => {
                let settings = self.checked_settings(context)?;
                clean::run(self, context, &settings, true).await
            }
            ACTION_CLEAN_APPLY => {
                let settings = self.checked_settings(context)?;
                clean::run(self, context, &settings, dry_run).await
            }
            ACTION_TEST_NAME => {
                let settings = self.checked_settings(context)?;
                clean::run_test_name(context, &settings)
            }
            ACTION_NAMES_PREVIEW => {
                let settings = self.checked_settings(context)?;
                names::run(self, context, &settings, true).await
            }
            ACTION_NAMES_APPLY => {
                let settings = self.checked_settings(context)?;
                names::run(self, context, &settings, dry_run).await
            }
            ACTION_CATEGORIZE_PREVIEW => {
                let settings = self.checked_settings(context)?;
                categorize::run(self, context, &settings, true).await
            }
            ACTION_CATEGORIZE_APPLY => {
                let settings = self.checked_settings(context)?;
                categorize::run(self, context, &settings, dry_run).await
            }
            ACTION_LOGOS_PREVIEW => {
                let settings = self.checked_settings(context)?;
                logos::run(self, context, &settings, true).await
            }
            ACTION_LOGOS_APPLY => {
                let settings = self.checked_settings(context)?;
                logos::run(self, context, &settings, dry_run).await
            }
            ACTION_CREATE_PREVIEW => {
                let settings = self.checked_settings(context)?;
                create::run(self, context, &settings, true).await
            }
            ACTION_CREATE_APPLY => {
                let settings = self.checked_settings(context)?;
                create::run(self, context, &settings, dry_run).await
            }
            ACTION_SYNC_PREVIEW => {
                let settings = self.checked_settings(context)?;
                sync::run(self, context, &settings, true).await
            }
            ACTION_SYNC_APPLY => {
                let settings = self.checked_settings(context)?;
                sync::run(self, context, &settings, dry_run).await
            }
            ACTION_TIDY_PREVIEW => {
                let settings = self.checked_settings(context)?;
                tidy::run(self, context, &settings, true).await
            }
            ACTION_TIDY_APPLY => {
                let settings = self.checked_settings(context)?;
                tidy::run(self, context, &settings, dry_run).await
            }
            other => bail!("{APP_NAME} has no action {other:?} yet"),
        }
    }

    /// This app's settings, refusing the ones a run would trip over.
    ///
    /// Only the shared errors a run would actually hit are fatal here: the channel scope
    /// and the AI policy. Unrelated host problems (a missing token, Dispatcharr
    /// credentials) are the caller's concern and are reported by `validate`.
    fn checked_settings(&self, context: &JobContext) -> Result<settings::LineupSettings> {
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
    ///
    /// Shared with the guide and streams apps: one wording for one question, and one
    /// place that knows the `status` run must exclude itself and that a live run's
    /// progress is the interesting part.
    fn status(&self, context: &JobContext) -> JobOutcome {
        crosstalk_core::status::describe(self, context)
    }

    /// Validate the settings and the configured lineup file.
    ///
    /// The lineup file must exist (it ships embedded, so a typo is the only way this
    /// fails); whether the named channel profiles exist is only checked at run time,
    /// when Dispatcharr can be reached.
    fn validate(&self, context: &JobContext) -> Result<JobOutcome> {
        let path = self.settings_path(&context.config().config_dir);
        let settings = self.checked_settings(context)?;
        let warnings = settings::warnings(&settings);
        let config_report = context.config().validate();

        if !config_report.is_ok() {
            bail!("{}", config_report.errors.join("; "));
        }

        let lineup = lineup_file::load_lineup(
            &settings.lineup_file,
            &settings.category_detail,
            &settings.group_prefix,
        )?;

        // What the settings resolve to, so an operator can see the numbers a run will
        // use without reading the file.
        let mut outcome = JobOutcome::new(format!(
            "Settings valid. Lineup {}: {} channel(s) in {} categorie(s) (detail {}, prefix \
             {:?}); channel profiles are checked at run time.",
            lineup.file_name,
            lineup.entries.len(),
            lineup.categories.len(),
            lineup.detail,
            lineup.prefix,
        ));
        outcome = outcome.with_count("channels", lineup.entries.len() as i64);
        outcome = outcome.with_count("categories", lineup.categories.len() as i64);
        outcome = outcome.with_count(
            "warnings",
            (config_report.warnings.len() + warnings.len()) as i64,
        );
        for warning in &warnings {
            tracing::warn!("{}: {warning}", path.display());
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::config::CrosstalkConfig;
    use crosstalk_core::dispatcharr::World;
    use crosstalk_core::store::Store;
    use crosstalk_core::subapp::FieldValue;
    use test_support::Fixture;

    fn app() -> LineupApp {
        LineupApp::new()
    }

    #[test]
    fn app_declares_validate_status_clean_names_categorize_logos_create_sync_tidy_and_test_name() {
        // Step 13 registered `validate` and `status`; step 14 the `clean_*` pair and
        // `test_name`; step 15 the `names_*` pair; step 16 the `categorize_*` and
        // `logos_*` pairs; step 17 the `create_*` pair; step 18 the `sync_*` and
        // `tidy_*` pairs. All are registered; the plan's action table named them from
        // the start so schedules and URLs can be guessed.
        let ids: Vec<&str> = app().actions().iter().map(|action| action.id).collect();
        assert_eq!(
            ids,
            vec![
                ACTION_VALIDATE,
                ACTION_STATUS,
                ACTION_CLEAN_PREVIEW,
                ACTION_CLEAN_APPLY,
                ACTION_TEST_NAME,
                ACTION_NAMES_PREVIEW,
                ACTION_NAMES_APPLY,
                ACTION_CATEGORIZE_PREVIEW,
                ACTION_CATEGORIZE_APPLY,
                ACTION_LOGOS_PREVIEW,
                ACTION_LOGOS_APPLY,
                ACTION_CREATE_PREVIEW,
                ACTION_CREATE_APPLY,
                ACTION_SYNC_PREVIEW,
                ACTION_SYNC_APPLY,
                ACTION_TIDY_PREVIEW,
                ACTION_TIDY_APPLY,
            ]
        );
        // The clean pair and test_name read like their plugin halves: preview and test
        // are reads, the apply writes and confirms.
        let by_id = |id: &str| {
            app()
                .actions()
                .into_iter()
                .find(|action| action.id == id)
                .unwrap_or_else(|| panic!("{id} is registered"))
        };
        assert_eq!(by_id(ACTION_CLEAN_PREVIEW).kind, ActionKind::Read);
        assert!(by_id(ACTION_CLEAN_PREVIEW).confirm.is_none());
        assert_eq!(by_id(ACTION_CLEAN_APPLY).kind, ActionKind::Write);
        assert!(by_id(ACTION_CLEAN_APPLY).confirm.is_some());
        assert_eq!(by_id(ACTION_TEST_NAME).kind, ActionKind::Read);
        assert!(by_id(ACTION_TEST_NAME).input.is_some());
        // The names pair reads like its plugin halves: the preview is a read, the
        // apply writes and confirms.
        assert_eq!(by_id(ACTION_NAMES_PREVIEW).kind, ActionKind::Read);
        assert!(by_id(ACTION_NAMES_PREVIEW).confirm.is_none());
        assert_eq!(by_id(ACTION_NAMES_APPLY).kind, ActionKind::Write);
        assert!(by_id(ACTION_NAMES_APPLY).confirm.is_some());
        // The categorize, logos and create pairs read the same way.
        assert_eq!(by_id(ACTION_CATEGORIZE_PREVIEW).kind, ActionKind::Read);
        assert!(by_id(ACTION_CATEGORIZE_PREVIEW).confirm.is_none());
        assert_eq!(by_id(ACTION_CATEGORIZE_APPLY).kind, ActionKind::Write);
        assert!(by_id(ACTION_CATEGORIZE_APPLY).confirm.is_some());
        assert_eq!(by_id(ACTION_LOGOS_PREVIEW).kind, ActionKind::Read);
        assert!(by_id(ACTION_LOGOS_PREVIEW).confirm.is_none());
        assert_eq!(by_id(ACTION_LOGOS_APPLY).kind, ActionKind::Write);
        assert!(by_id(ACTION_LOGOS_APPLY).confirm.is_some());
        assert_eq!(by_id(ACTION_CREATE_PREVIEW).kind, ActionKind::Read);
        assert!(by_id(ACTION_CREATE_PREVIEW).confirm.is_none());
        assert_eq!(by_id(ACTION_CREATE_APPLY).kind, ActionKind::Write);
        assert!(by_id(ACTION_CREATE_APPLY).confirm.is_some());
        // The sync pair reads like every other write pair; the tidy apply hides
        // channels, so it is Destructive.
        assert_eq!(by_id(ACTION_SYNC_PREVIEW).kind, ActionKind::Read);
        assert!(by_id(ACTION_SYNC_PREVIEW).confirm.is_none());
        assert_eq!(by_id(ACTION_SYNC_APPLY).kind, ActionKind::Write);
        assert!(by_id(ACTION_SYNC_APPLY).confirm.is_some());
        assert_eq!(by_id(ACTION_TIDY_PREVIEW).kind, ActionKind::Read);
        assert!(by_id(ACTION_TIDY_PREVIEW).confirm.is_none());
        assert_eq!(by_id(ACTION_TIDY_APPLY).kind, ActionKind::Destructive);
        assert!(by_id(ACTION_TIDY_APPLY).confirm.is_some());
        // Every future writing action is a preview/apply pair, and a preview is a read —
        // the naming rule every sub-app follows, pinned before the pairs exist.
        for (preview, apply) in [
            (ACTION_CLEAN_PREVIEW, ACTION_CLEAN_APPLY),
            (ACTION_NAMES_PREVIEW, ACTION_NAMES_APPLY),
            (ACTION_CATEGORIZE_PREVIEW, ACTION_CATEGORIZE_APPLY),
            (ACTION_LOGOS_PREVIEW, ACTION_LOGOS_APPLY),
            (ACTION_CREATE_PREVIEW, ACTION_CREATE_APPLY),
            (ACTION_SYNC_PREVIEW, ACTION_SYNC_APPLY),
            (ACTION_TIDY_PREVIEW, ACTION_TIDY_APPLY),
        ] {
            assert!(preview.ends_with("_preview"), "{preview}");
            assert!(apply.ends_with("_apply"), "{apply}");
            assert_eq!(
                preview.trim_end_matches("_preview"),
                apply.trim_end_matches("_apply")
            );
        }
    }

    #[test]
    fn status_is_the_one_unguarded_action() {
        // It reads the run store and nothing else, so it may run while another run of
        // this app is live — which is exactly when an operator asks it.
        let actions = app().actions();
        let status = actions
            .iter()
            .find(|action| action.id == ACTION_STATUS)
            .expect("status");
        assert_eq!(status.kind, ActionKind::Read);
        assert!(!status.needs_dispatcharr);
        assert!(status.unguarded);
        assert_eq!(
            actions.iter().filter(|action| action.unguarded).count(),
            1,
            "only status bypasses the one-run-per-app guard"
        );
    }

    #[test]
    fn validate_is_offline_and_reads_nothing_remote() {
        let validate = app()
            .actions()
            .into_iter()
            .find(|action| action.id == ACTION_VALIDATE)
            .expect("validate");
        assert_eq!(validate.kind, ActionKind::Read);
        assert!(!validate.needs_dispatcharr);
        assert!(!validate.unguarded);
        assert!(validate.confirm.is_none());
    }

    #[test]
    fn the_app_declares_its_own_export_prefix() {
        assert_eq!(app().export_prefix(), "lineup_");
        assert_eq!(
            app().report_title("Sync Preview Report"),
            format!(
                "# crosstalk Lineup v{} - Sync Preview Report",
                crosstalk_core::HOST_VERSION
            )
        );
    }

    #[test]
    fn the_named_scope_is_lineup_and_starts_empty() {
        // The scope `lineup` resolves to the channels lineup owns for its configured
        // lineup file. Nothing owns anything yet, so the set starts empty — and an
        // empty set is documented, not an error (the bound replaces the host scope, so
        // a step scoped to it touches nothing rather than everything).
        assert_eq!(
            app().named_scopes(),
            vec![NamedScope::new(
                NAMED_SCOPE_LINEUP,
                "The channels lineup owns for its configured lineup file (empty until a \
                 sync or create records ownership)"
            )]
        );
        let fixture = Fixture::new();
        let store = Store::in_memory().expect("store");
        let resolved = app()
            .resolve_named_scope(
                NAMED_SCOPE_LINEUP,
                &fixture.config,
                &World::default(),
                &store,
            )
            .expect("served")
            .expect("resolves");
        // Empty but hinted: the fixture's default lineup file (`US_DirecTV-Premier`)
        // names a country even before anything is owned.
        assert_eq!(
            resolved,
            NamedScopeResolution::ChannelsWithHints {
                ids: Vec::new(),
                hints: crosstalk_core::subapp::NamedScopeHints::with_country("US"),
            }
        );
        assert!(
            app()
                .resolve_named_scope("nope", &fixture.config, &World::default(), &store)
                .is_none(),
            "any other name is not mine"
        );
    }

    #[test]
    fn the_named_scope_resolves_the_configured_files_owned_channels() {
        // Ownership recorded under `lineup:<configured file>:...` resolves; another
        // lineup's channels, groups and other apps' rows do not.
        let fixture = Fixture::new();
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run(APP_ID, ACTION_SYNC_APPLY, false)
            .expect("run");
        for (kind, id, app_id, key) in [
            (
                "channel",
                7,
                APP_ID,
                "lineup:US_DirecTV-Premier_lineup.json:News:CNN",
            ),
            (
                "channel",
                8,
                APP_ID,
                "lineup:US_DirecTV-Premier_lineup.json:News:FOX News",
            ),
            (
                "channel",
                9,
                APP_ID,
                "lineup:UK_Freeview_lineup.json:News:BBC News",
            ),
            (
                "group",
                70,
                APP_ID,
                "lineup:US_DirecTV-Premier_lineup.json:News",
            ),
            (
                "channel",
                11,
                "streams",
                "lineup:US_DirecTV-Premier_lineup.json:x",
            ),
        ] {
            store
                .record_ownership(kind, id, app_id, key, run_id)
                .expect("own");
        }
        let resolve = |fixture: &Fixture| {
            app()
                .resolve_named_scope(
                    NAMED_SCOPE_LINEUP,
                    &fixture.config,
                    &World::default(),
                    &store,
                )
                .expect("served")
        };
        // The default lineup file.
        assert_eq!(
            resolve(&fixture).expect("resolves"),
            NamedScopeResolution::ChannelsWithHints {
                ids: vec![7, 8],
                hints: crosstalk_core::subapp::NamedScopeHints::with_country("US"),
            }
        );
        // Switching lineup_file (either spelling) switches the scope; the old lineup's
        // channels stay owned but leave the scope.
        for spelling in ["UK_Freeview", "UK_Freeview_lineup.json"] {
            std::fs::write(
                fixture.settings_path(),
                format!("lineup_file = \"{spelling}\"\n"),
            )
            .expect("seed");
            assert_eq!(
                resolve(&fixture).expect("resolves"),
                NamedScopeResolution::ChannelsWithHints {
                    ids: vec![9],
                    hints: crosstalk_core::subapp::NamedScopeHints::with_country("UK"),
                },
                "{spelling}"
            );
        }
        // An unknown lineup fails the step instead of scoping it to nothing or all.
        std::fs::write(fixture.settings_path(), "lineup_file = \"Nope\"\n").expect("seed");
        let error = resolve(&fixture).expect_err("unknown lineup");
        assert!(
            format!("{error:#}").contains("unknown lineup file"),
            "{error:#}"
        );
    }

    #[test]
    fn settings_round_trip_through_the_app() {
        let fixture = Fixture::new();
        let path = app().settings_path(fixture.dir.path());

        let mut values = app().load_settings(&path).expect("load defaults");
        values.insert(
            "lineup_file",
            FieldValue::Text("UK_Freeview_lineup.json".to_string()),
        );
        values.insert("category_detail", FieldValue::Text("simple".to_string()));
        values.insert("max_preview_rows", FieldValue::Number(50.0));
        app().save_settings(&path, &values).expect("save");

        let reloaded = app().load_settings(&path).expect("reload");
        assert_eq!(
            reloaded.text("lineup_file"),
            Some("UK_Freeview_lineup.json")
        );
        assert_eq!(reloaded.text("category_detail"), Some("simple"));
        assert_eq!(reloaded.number("max_preview_rows"), Some(50.0));
    }

    #[test]
    fn save_rejects_an_unknown_field_and_an_out_of_range_number() {
        let fixture = Fixture::new();
        let path = app().settings_path(fixture.dir.path());

        let mut values = SettingsValues::new();
        values.insert("not_a_setting", FieldValue::Text("x".into()));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("unknown setting"));

        let mut values = SettingsValues::new();
        values.insert("max_preview_rows", FieldValue::Number(0.0));
        let error = app().save_settings(&path, &values).expect_err("rejects");
        assert!(format!("{error}").contains("between"), "{error}");
    }

    #[test]
    fn save_rejects_a_settings_file_that_still_holds_a_retired_key() {
        let fixture = Fixture::new();
        let path = app().settings_path(fixture.dir.path());
        std::fs::write(&path, "match_sensitivity = \"strict\"\n").expect("seed");
        let error = app().load_settings(&path).expect_err("rejects the old key");
        assert!(format!("{error:#}").contains("[matching]"), "{error:#}");
    }

    #[test]
    fn the_shipped_example_configs_load_and_validate() {
        // The release ships a lineup.example.toml for operators to copy; a key renamed in
        // code but not there would make the copied file refuse to load
        // (`deny_unknown_fields`).
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
        let settings = settings::load(&root.join("lineup.example.toml")).expect("lineup example");
        assert!(settings::validate(&settings).is_empty());
        // And the host example still loads alongside it.
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::copy(
            root.join("crosstalk.example.toml"),
            dir.path().join("crosstalk.toml"),
        )
        .expect("copy");
        let config = CrosstalkConfig::load(dir.path()).expect("host example");
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
            outcome.summary.contains("US_DirecTV-Premier_lineup.json"),
            "{}",
            outcome.summary
        );
        assert!(
            outcome.summary.contains("checked at run time"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("warnings"), Some(&0));
        assert!(
            outcome
                .counts
                .get("channels")
                .is_some_and(|count| *count > 0),
            "{:?}",
            outcome.counts
        );
    }

    #[tokio::test]
    async fn validate_refuses_an_unknown_lineup_file() {
        let fixture = Fixture::with_credentials();
        std::fs::write(
            fixture.settings_path(),
            "lineup_file = \"Nope_lineup.json\"\n",
        )
        .expect("seed");
        let error = app()
            .dispatch(ACTION_VALIDATE, &fixture.context(), true)
            .await
            .expect_err("refuses");
        assert!(
            format!("{error}").contains("unknown lineup file"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn validate_refuses_a_hand_edited_file_that_is_out_of_bounds() {
        let fixture = Fixture::with_credentials();
        std::fs::write(fixture.settings_path(), "category_detail = \"ultra\"\n").expect("seed");
        let error = app()
            .dispatch(ACTION_VALIDATE, &fixture.context(), true)
            .await
            .expect_err("refuses");
        assert!(format!("{error}").contains("category_detail"), "{error:#}");
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
            APP_ID,
            ACTION_STATUS,
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
