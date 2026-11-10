//! The five cleanup presets: one selection, one operation, and the safety rails around them.
//!
//! Every one of them is a preset over [`crosstalk_core::channel_ops`], which is where the
//! selection, the pure plan, the batched write and the CSV already live. What this module adds
//! is the part only the guide app knows:
//!
//!   * which selection and operations each plugin action was (`add_bad_epg_suffix` (2474),
//!     `remove_epg_assignments` (2427), `remove_epg_from_hidden` (2562),
//!     `remove_epg_by_regex` (2915), `remove_all_epg_from_groups` (2968));
//!   * the settings each one refuses to run without, with the plugin's own reasoning;
//!   * where a scan-derived selection comes from: the run store, not a results file, and
//!     named in the summary, the log and the CSV preamble because the confirmation dialog is
//!     static text.
//!
//! Divergences from the plugin, each deliberate and listed in `README.md`:
//!
//!   * `clear_hidden` only UNASSIGNS the guide. The plugin also deleted the entry's
//!     `ProgramData` rows, which blanks every visible channel sharing that entry;
//!   * `clear_groups` refuses to run with `[scope] groups` empty. The plugin applies the same
//!     group filter but treats an empty one as "every channel", which turns one keypress into
//!     "unassign every guide in the install";
//!   * a scan-derived selection carries the guide the scan saw, so a channel whose guide
//!     changed since is skipped rather than written. This applies to `tag_missing` too: a
//!     channel somebody gave a working guide after the scan is not tagged as bad.

use crate::scan::{self, LastScan};
use crate::settings::GuideSettings;
use anyhow::{bail, Result};
use crosstalk_core::channel_ops::{
    self, Field, Op, Plan, PlanOptions, RenameKind, ReportSpec, Selection,
};
use crosstalk_core::config::ScopeConfig;
use crosstalk_core::csv_report;
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};

/// The five cleanup presets, one per plugin action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cleanup {
    /// `add_bad_epg_suffix`: tag the last scan's channels, optionally clearing their guide.
    TagMissing,
    /// `remove_epg_assignments`: clear the guide of every channel the last scan found.
    ClearMissing,
    /// `remove_epg_from_hidden`: clear the guide of channels disabled in a profile.
    ClearHidden,
    /// `remove_epg_by_regex`: clear the guide where the guide's NAME matches a pattern.
    ClearByRegex,
    /// `remove_all_epg_from_groups`: clear the guide of every channel in `[scope] groups`.
    ClearGroups,
}

impl Cleanup {
    /// The verb the action ids are built from (`<verb>_preview` / `<verb>_apply`).
    pub fn verb(self) -> &'static str {
        match self {
            Self::TagMissing => "tag_missing",
            Self::ClearMissing => "clear_missing",
            Self::ClearHidden => "clear_hidden",
            Self::ClearByRegex => "clear_by_regex",
            Self::ClearGroups => "clear_groups",
        }
    }

    /// The name a summary uses for this action.
    pub fn label(self) -> &'static str {
        match self {
            Self::TagMissing => "Tag missing EPG",
            Self::ClearMissing => "Clear missing EPG assignments",
            Self::ClearHidden => "Clear EPG from hidden channels",
            Self::ClearByRegex => "Clear EPG by REGEX",
            Self::ClearGroups => "Clear all EPG in groups",
        }
    }

    /// The report's title, without the preview/applied suffix.
    fn report_title(self) -> &'static str {
        match self {
            Self::TagMissing => "Tag Missing EPG",
            Self::ClearMissing => "Clear Missing EPG Assignments",
            Self::ClearHidden => "Clear EPG from Hidden Channels",
            Self::ClearByRegex => "Clear EPG by REGEX",
            Self::ClearGroups => "Clear ALL EPG in Groups",
        }
    }

    /// Whether the selection is the last scan's result set.
    pub fn from_scan(self) -> bool {
        matches!(self, Self::TagMissing | Self::ClearMissing)
    }

    /// The schedule key this preset runs as, for progress events and the scan announcement.
    fn action_id(self, dry_run: bool) -> String {
        format!(
            "guide.{}_{}",
            self.verb(),
            if dry_run { "preview" } else { "apply" }
        )
    }
}

/// Run one cleanup preset.
///
/// Preview writes the CSV and nothing else; an apply re-plans against the world this run
/// loaded (so a scan from any age is safe) and then writes. [`Plan`] is never cached between
/// the two: an apply always re-reads every selected channel.
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &GuideSettings,
    action: Cleanup,
    dry_run: bool,
) -> Result<JobOutcome> {
    let client = context.dispatcharr()?;
    let action_id = action.action_id(dry_run);
    let scope = context.config().scope.clone();

    // The plugin errored when its results file was missing; this is the same refusal, and it
    // happens before any request so a mistyped action costs nothing.
    let scan = if action.from_scan() {
        Some(scan::last_scan(context)?)
    } else {
        None
    };
    if let Some(scan) = &scan {
        // The confirmation text is static, so this is where an apply says which scan it is
        // about to work from: the run page, the log and the summary all carry it.
        scan::announce(context, &action_id, scan);
    }

    let world = client.load_world().await?;
    let (selection, ops, options) = build(action, settings, &scope, scan.as_ref())?;
    let plan = channel_ops::plan_with_options(&world, &selection, &ops, &options)?;

    // An apply re-plans from the world this run loaded, so a channel that changed since the
    // scan is either skipped (its guide moved) or planned from its current name (a rename).
    let applied = if dry_run {
        None
    } else {
        Some(channel_ops::apply(client, &plan, context, &action_id).await)
    };

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = format!(
        "{}_{}",
        action.verb(),
        if dry_run { "preview" } else { "applied" }
    );
    let title = format!(
        "{} {} Report",
        action.report_title(),
        if dry_run { "Preview" } else { "Applied" }
    );
    let mut spec = ReportSpec::new(app, &schema, &values, &stem, &title);
    if let Some(scan) = &scan {
        spec = spec.note("Result set", scan.describe()).note(
            "Result set age",
            "no limit: every channel is re-read before writing",
        );
    }
    let report = channel_ops::write_ops_report(context, &spec, &plan, applied.as_ref())?;

    context.record_rows(&channel_ops::run_rows(&plan, applied.as_ref()))?;

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let mut built =
        outcome(action, &plan, applied.as_ref(), dry_run, scan.as_ref()).report(Some(report));
    // The cancel rule (see `crosstalk_core::jobs`): a cancelled run keeps the rows and the
    // report it produced and is recorded cancelled, never as a success. Phase 1 kept a
    // partially-written run a success, which made a half-applied cleanup look finished.
    if let Some(note) = channel_ops::cancelled_note(context, &plan, applied.as_ref()) {
        built = built.cancelled().note(note);
    }
    Ok(built.build())
}

/// The selection, the operations and the plan options for one preset.
///
/// Split out so the refusals are a value the tests can call without a server: each one is a
/// setting an operator left blank, and the message says which field and where it lives.
fn build(
    action: Cleanup,
    settings: &GuideSettings,
    scope: &ScopeConfig,
    scan: Option<&LastScan>,
) -> Result<(Selection, Vec<Op>, PlanOptions)> {
    match action {
        Cleanup::TagMissing => {
            let suffix = settings.bad_epg_suffix.clone();
            if suffix.is_empty() {
                // The plugin's own refusal (`add_bad_epg_suffix`, 2480-2482): an empty suffix
                // would append nothing, and the action would claim to have tagged channels.
                bail!(
                    "bad_epg_suffix is empty in guide.toml, so there is nothing to append: the \
                     plugin refuses a blank suffix too"
                );
            }
            let mut ops = vec![Op::Rename {
                kind: RenameKind::Suffix(suffix),
            }];
            if settings.remove_epg_with_suffix {
                ops.push(Op::ClearEpg);
            }
            let selection = Selection::from_scan_rows(&scan_rows(scan)?, scope);
            Ok((
                selection,
                ops,
                // The plugin skips an already-tagged channel ENTIRELY, guide included.
                PlanOptions {
                    suffix_present_skips_rest: true,
                },
            ))
        }
        Cleanup::ClearMissing => Ok((
            Selection::from_scan_rows(&scan_rows(scan)?, scope),
            vec![Op::ClearEpg],
            PlanOptions::default(),
        )),
        Cleanup::ClearHidden => {
            // The plugin requires `channel_profile_name` (2574-2578); here the same value is
            // `[scope] profiles`, which every app shares.
            if scope.profiles.trim().is_empty() {
                bail!(
                    "no channel profile is configured, and clear_hidden needs the profiles whose \
                     disabled channels it should clear: set [scope] profiles in crosstalk.toml"
                );
            }
            Ok((
                Selection::hidden_in(&scope.profiles, scope),
                vec![Op::ClearEpg],
                PlanOptions::default(),
            ))
        }
        Cleanup::ClearByRegex => {
            let pattern = settings.epg_regex_to_remove.trim().to_string();
            if pattern.is_empty() {
                // The plugin's refusal (2919-2921). A blank pattern is worse here than there:
                // it would match every guide and clear all of them.
                bail!(
                    "epg_regex_to_remove is empty in guide.toml, so there is no pattern to match \
                     against a guide's name: set it, or use clear_groups instead"
                );
            }
            Ok((
                Selection {
                    epg_name_regex: Some(pattern),
                    // The plugin's queryset filters `epg_data__isnull=False` first.
                    has_epg: true,
                    ..Selection::in_scope(scope)
                },
                vec![Op::ClearEpg],
                PlanOptions::default(),
            ))
        }
        Cleanup::ClearGroups => {
            if scope.groups.trim().is_empty() {
                // DELIBERATE divergence: the plugin treats an empty group filter as "every
                // channel", so one press would unassign every guide in the install.
                bail!(
                    "no channel groups are configured, and clear_groups refuses to treat that as \
                     'every channel' (the plugin would): set [scope] groups in crosstalk.toml, or \
                     use match_preview to review what an unscoped run would touch"
                );
            }
            Ok((
                Selection {
                    // Nothing assigned means nothing to clear, so it is not a no-op worth
                    // reporting as a skip for a channel the plugin never queried.
                    has_epg: true,
                    ..Selection::in_scope(scope)
                },
                vec![Op::ClearEpg],
                PlanOptions::default(),
            ))
        }
    }
}

/// The rows of the scan a scan-derived preset selects from.
fn scan_rows(scan: Option<&LastScan>) -> Result<Vec<crosstalk_core::store::RunRow>> {
    match scan {
        Some(scan) => Ok(scan.rows.clone()),
        // `run` always resolves the scan for these presets; reaching here is a bug.
        None => bail!("this action selects from a scan but no scan was resolved"),
    }
}

/// The outcome the runner records.
fn outcome(
    action: Cleanup,
    plan: &Plan,
    applied: Option<&channel_ops::ApplyReport>,
    dry_run: bool,
    scan: Option<&LastScan>,
) -> OutcomeBuilder {
    let renames = plan
        .changes
        .iter()
        .filter(|change| change.field == Field::Name)
        .count();
    let clears = plan
        .changes
        .iter()
        .filter(|change| change.field == Field::Epg)
        .count();

    let mut builder = OutcomeBuilder::new(format!(
        "{} {}: {} channel(s) selected, {} change(s) planned",
        action.label(),
        if dry_run { "preview" } else { "applied" },
        plan.selected.len(),
        plan.changes.len()
    ))
    .count("selected", plan.selected.len() as i64)
    .count("changes", plan.changes.len() as i64)
    .count("skipped", plan.skipped.len() as i64);
    match action {
        Cleanup::TagMissing => {
            builder = builder
                .count("tagged", renames as i64)
                .count("guides_cleared", clears as i64);
        }
        _ => {
            builder = builder.count("guides_cleared", clears as i64);
        }
    }
    if let Some(applied) = applied {
        builder = builder
            .count("written", applied.ok as i64)
            .count("failed", applied.failed as i64);
        if applied.failed > 0 {
            builder = builder.note(format!("{} row(s) failed", applied.failed));
        }
    }
    if let Some(scan) = scan {
        builder = builder.note(scan.describe());
    }
    builder
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::{ScanRow, STATUS_MISSING};
    use crate::test_support::{self, Fixture, ESPN_HD};
    use crate::GuideApp;
    use crate::ACTION_TAG_MISSING_APPLY;
    use crosstalk_core::dispatcharr::World;
    use crosstalk_core::store::{RunRow, Store};
    use std::collections::HashMap;

    /// The run rows a scan of `world` would have stored: every channel whose guide carries no
    /// programmes.
    fn scan_rows_of(world: &World) -> Vec<RunRow> {
        let mut rows = Vec::new();
        for (ordinal, channel) in world.channels.iter().enumerate() {
            let Some(epg) = channel.epg_data_id else {
                continue;
            };
            let entry = world
                .entries
                .iter()
                .find(|entry| entry.id == epg)
                .expect("entry");
            // ESPN HD is the fixture's working guide; a scan never reports it.
            if entry.id == ESPN_HD {
                continue;
            }
            rows.push(
                ScanRow {
                    channel_id: channel.id,
                    channel_name: channel.name.clone(),
                    channel_number: channel.display_number(),
                    channel_group: world.group_name(channel.channel_group_id),
                    epg_data_id: epg,
                    epg_tvg_id: entry.tvg_id.clone(),
                    epg_name: entry.name.clone(),
                    epg_source: "Schedules Direct".to_string(),
                }
                .to_run_row(ordinal as i64),
            );
        }
        rows
    }

    /// A store holding the finished scan of `world`, and a context recording into a new run.
    fn recorded_scan(fixture: &Fixture, world: &World, action: &str) -> (Store, JobContext) {
        let store = Store::in_memory().expect("store");
        fixture.record_scan(&store, &scan_rows_of(world));
        let context = fixture.context_for_run(crate::APP_ID, action, &store);
        (store, context)
    }

    /// The `(channel_id, field, value)` triples of every channel edit the server received.
    async fn channel_edits(fixture: &Fixture) -> HashMap<i64, HashMap<String, serde_json::Value>> {
        let mut out: HashMap<i64, HashMap<String, serde_json::Value>> = HashMap::new();
        for body in fixture
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await
        {
            for row in body.as_array().into_iter().flatten() {
                let id = row["id"].as_i64().expect("id");
                let fields = out.entry(id).or_default();
                for (field, value) in row.as_object().expect("row") {
                    if field != "id" {
                        fields.insert(field.clone(), value.clone());
                    }
                }
            }
        }
        out
    }

    /// The channel ids and assigned entry ids of every `batch-set-epg` association.
    async fn cleared(fixture: &Fixture) -> Vec<(i64, Option<i64>)> {
        let mut out = Vec::new();
        for body in fixture
            .bodies("POST", "/api/channels/channels/batch-set-epg/")
            .await
        {
            for row in body["associations"].as_array().into_iter().flatten() {
                out.push((
                    row["channel_id"].as_i64().expect("id"),
                    row["epg_data_id"].as_i64(),
                ));
            }
        }
        out
    }

    // ---- tag_missing -------------------------------------------------------

    #[tokio::test]
    async fn tag_missing_previews_the_scan_and_names_the_scan() {
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let (_store, context) = recorded_scan(&fixture, &world, "tag_missing_preview");
        let settings = GuideSettings::default();

        let outcome = run(
            &GuideApp::new(),
            &context,
            &settings,
            Cleanup::TagMissing,
            true,
        )
        .await
        .expect("preview");

        // Both scan channels are selected; CNN [BadEPG] already carries the suffix and is
        // skipped, so one rename is planned.
        assert_eq!(outcome.counts.get("selected"), Some(&2));
        assert_eq!(outcome.counts.get("tagged"), Some(&1));
        assert_eq!(outcome.counts.get("skipped"), Some(&1));
        assert_eq!(outcome.counts.get("guides_cleared"), Some(&0));
        assert!(
            outcome
                .summary
                .starts_with("Tag missing EPG preview: 2 channel(s) selected"),
            "{}",
            outcome.summary
        );
        // The confirmation text is static, so the scan is named in the summary instead.
        assert!(
            outcome.summary.contains("scan run #1 started"),
            "{}",
            outcome.summary
        );
        assert!(
            outcome.summary.contains("2 channel(s)"),
            "{}",
            outcome.summary
        );

        // A preview writes no channel data at all.
        assert!(channel_edits(&fixture).await.is_empty());
        assert!(cleared(&fixture).await.is_empty());

        // The CSV says which scan it read, and that the age does not matter.
        let text = std::fs::read_to_string(outcome.report_path.expect("report")).expect("read");
        assert!(text.contains("Result set: scan run #1 started"), "{text}");
        assert!(
            text.contains("Changes written: none, this was a preview"),
            "{text}"
        );
        assert!(text.contains("Télé-Québec [BadEPG]"), "{text}");
        assert!(text.contains("already ends with"), "{text}");
    }

    #[tokio::test]
    async fn tag_missing_applies_the_suffix_and_keeps_the_guide_of_a_tagged_channel() {
        // `remove_epg_with_suffix` on: the untagged channel gets both changes, the tagged one
        // gets neither (the plugin's coupling, exercised end to end here).
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let (store, context) = recorded_scan(&fixture, &world, "tag_missing_apply");
        let run_id = context.run_id().expect("run id");
        let settings = GuideSettings {
            remove_epg_with_suffix: true,
            ..GuideSettings::default()
        };

        run(
            &GuideApp::new(),
            &context,
            &settings,
            Cleanup::TagMissing,
            false,
        )
        .await
        .expect("apply");

        let edits = channel_edits(&fixture).await;
        assert_eq!(edits.len(), 1, "{edits:#?}");
        let renamed = edits.get(&4).expect("Télé-Québec was renamed");
        assert_eq!(renamed["name"], serde_json::json!("Télé-Québec [BadEPG]"));
        // CNN keeps its guide: it was skipped entirely, EPG clear included.
        assert_eq!(cleared(&fixture).await, vec![(4, None)]);

        // The rows are the record a restore would read.
        let rows = store.rows(run_id).expect("rows");
        let name_row = rows
            .iter()
            .find(|row| row.channel_id == 4 && row.field == "name")
            .expect("rename row");
        assert_eq!(name_row.status, "ok");
        assert_eq!(name_row.before_value.as_deref(), Some("Télé-Québec"));
        assert_eq!(
            name_row.after_value.as_deref(),
            Some("Télé-Québec [BadEPG]")
        );
        assert!(rows
            .iter()
            .any(|row| row.channel_id == 2 && row.status == "skipped"));
    }

    #[tokio::test]
    async fn tag_missing_refuses_a_blank_suffix() {
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let (_store, context) = recorded_scan(&fixture, &world, "tag_missing_preview");
        let settings = GuideSettings {
            bad_epg_suffix: String::new(),
            ..GuideSettings::default()
        };

        let error = run(
            &GuideApp::new(),
            &context,
            &settings,
            Cleanup::TagMissing,
            false,
        )
        .await
        .expect_err("refuses");
        assert!(
            format!("{error:#}").contains("bad_epg_suffix is empty"),
            "{error:#}"
        );
        assert!(channel_edits(&fixture).await.is_empty());
    }

    #[tokio::test]
    async fn a_scan_derived_action_without_a_scan_says_to_scan_first() {
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let store = Store::in_memory().expect("store");
        let context = fixture.context_for_run(crate::APP_ID, "clear_missing_apply", &store);

        for action in [Cleanup::TagMissing, Cleanup::ClearMissing] {
            let error = run(
                &GuideApp::new(),
                &context,
                &GuideSettings::default(),
                action,
                false,
            )
            .await
            .expect_err("no scan");
            assert!(
                format!("{error:#}").contains("run `scan` first"),
                "{error:#}"
            );
        }
    }

    // ---- clear_missing -----------------------------------------------------

    #[tokio::test]
    async fn clear_missing_clears_exactly_the_scans_channels() {
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let (_store, context) = recorded_scan(&fixture, &world, "clear_missing_apply");

        let outcome = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            Cleanup::ClearMissing,
            false,
        )
        .await
        .expect("apply");

        assert_eq!(outcome.counts.get("guides_cleared"), Some(&2));
        assert_eq!(outcome.counts.get("written"), Some(&2));
        assert_eq!(cleared(&fixture).await, vec![(2, None), (4, None)]);
        // It clears guides only; no name changes.
        assert!(channel_edits(&fixture).await.is_empty());
    }

    #[tokio::test]
    async fn a_channel_whose_guide_changed_since_the_scan_is_left_alone() {
        // Channel 2 was on the empty CNN guide when the scan ran; it is on ESPN HD now, so the
        // stale result set must not throw that assignment away.
        let scanned = test_support::world();
        let mut world = scanned.clone();
        world.channels[1].epg_data_id = Some(ESPN_HD);
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let store = Store::in_memory().expect("store");
        fixture.record_scan(&store, &scan_rows_of(&scanned));
        let context = fixture.context_for_run(crate::APP_ID, "clear_missing_apply", &store);

        let outcome = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            Cleanup::ClearMissing,
            false,
        )
        .await
        .expect("apply");

        assert_eq!(outcome.counts.get("guides_cleared"), Some(&1));
        assert_eq!(cleared(&fixture).await, vec![(4, None)]);
        assert!(
            outcome.summary.contains("1 channel(s) selected"),
            "{}",
            outcome.summary
        );
    }

    #[tokio::test]
    async fn a_scan_that_found_nothing_selects_nothing() {
        // A clean scan stores no rows. An empty id list must mean "none of them", never "no
        // id filter": the latter would clear or tag every channel in scope.
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;

        for action in [Cleanup::TagMissing, Cleanup::ClearMissing] {
            let store = Store::in_memory().expect("store");
            fixture.record_scan(&store, &[]);
            let context = fixture.context_for_run(crate::APP_ID, action.verb(), &store);
            let outcome = run(
                &GuideApp::new(),
                &context,
                &GuideSettings {
                    remove_epg_with_suffix: true,
                    ..GuideSettings::default()
                },
                action,
                false,
            )
            .await
            .expect("apply");
            assert_eq!(outcome.counts.get("selected"), Some(&0), "{action:?}");
            assert_eq!(outcome.counts.get("changes"), Some(&0), "{action:?}");
        }
        assert!(cleared(&fixture).await.is_empty());
        assert!(channel_edits(&fixture).await.is_empty());
    }

    // ---- clear_hidden ------------------------------------------------------

    #[tokio::test]
    async fn clear_hidden_clears_the_guides_of_channels_disabled_in_the_profile() {
        let world = test_support::world();
        let mut fixture = Fixture::start(&world, &[ESPN_HD]).await;
        fixture.config.scope.profiles = "Favorites".to_string();
        let context = fixture.context_for_run(
            crate::APP_ID,
            "clear_hidden_apply",
            &Store::in_memory().expect("store"),
        );

        let outcome = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            Cleanup::ClearHidden,
            false,
        )
        .await
        .expect("apply");

        // Disabled in Favorites are 2, 3 and 5; only 2 has a guide.
        assert_eq!(outcome.counts.get("guides_cleared"), Some(&1));
        assert_eq!(cleared(&fixture).await, vec![(2, None)]);
        // Unassign only: the entry itself is never deleted (the deliberate divergence).
        assert!(fixture
            .bodies("DELETE", "/api/epg/epgdata/")
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn clear_hidden_refuses_without_a_profile() {
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let context = fixture.context();
        let error = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            Cleanup::ClearHidden,
            false,
        )
        .await
        .expect_err("refuses");
        assert!(
            format!("{error:#}").contains("[scope] profiles"),
            "{error:#}"
        );
        assert!(cleared(&fixture).await.is_empty());
    }

    // ---- clear_by_regex ----------------------------------------------------

    #[tokio::test]
    async fn clear_by_regex_matches_the_guides_name_and_not_the_channels() {
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let context = fixture.context_for_run(
            crate::APP_ID,
            "clear_by_regex_apply",
            &Store::in_memory().expect("store"),
        );
        let settings = GuideSettings {
            epg_regex_to_remove: "CNN".to_string(),
            ..GuideSettings::default()
        };

        run(
            &GuideApp::new(),
            &context,
            &settings,
            Cleanup::ClearByRegex,
            false,
        )
        .await
        .expect("apply");

        // Channel 2's NAME also matches, but it is the guide's name that is matched: the
        // pattern `BadEPG` would match nothing at all.
        assert_eq!(cleared(&fixture).await, vec![(2, None)]);

        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let settings = GuideSettings {
            epg_regex_to_remove: "BadEPG".to_string(),
            ..GuideSettings::default()
        };
        let outcome = run(
            &GuideApp::new(),
            &context,
            &settings,
            Cleanup::ClearByRegex,
            false,
        )
        .await
        .expect("apply");
        assert_eq!(outcome.counts.get("guides_cleared"), Some(&0));
        assert!(cleared(&fixture).await.is_empty());
    }

    #[tokio::test]
    async fn clear_by_regex_refuses_a_blank_pattern_and_a_python_only_one() {
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let context = fixture.context();

        let error = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            Cleanup::ClearByRegex,
            false,
        )
        .await
        .expect_err("refuses a blank pattern");
        assert!(
            format!("{error:#}").contains("epg_regex_to_remove is empty"),
            "{error:#}"
        );

        let settings = GuideSettings {
            epg_regex_to_remove: "(CNN)\\1".to_string(),
            ..GuideSettings::default()
        };
        let error = run(
            &GuideApp::new(),
            &context,
            &settings,
            Cleanup::ClearByRegex,
            false,
        )
        .await
        .expect_err("refuses a backreference");
        assert!(
            format!("{error:#}").contains("invalid EPG name pattern"),
            "{error:#}"
        );
    }

    // ---- clear_groups ------------------------------------------------------

    #[tokio::test]
    async fn clear_groups_clears_every_guide_in_the_scoped_groups() {
        let world = test_support::world();
        let mut fixture = Fixture::start(&world, &[ESPN_HD]).await;
        fixture.config.scope.groups = "News".to_string();
        let store = Store::in_memory().expect("store");
        let context = fixture.context_for_run(crate::APP_ID, "clear_groups_apply", &store);
        let run_id = context.run_id().expect("run id");

        let outcome = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            Cleanup::ClearGroups,
            false,
        )
        .await
        .expect("apply");

        // The News group holds CNN and Télé-Québec; Sports' ESPN is untouched.
        assert_eq!(cleared(&fixture).await, vec![(2, None), (4, None)]);
        assert_eq!(outcome.counts.get("guides_cleared"), Some(&2));
        let rows = store.rows(run_id).expect("rows");
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|row| row.field == "epg_data_id" && row.status == "ok"));
        assert_eq!(rows[0].before_value.as_deref(), Some("32"));
        assert_eq!(rows[0].after_value.as_deref(), Some("none"));
    }

    #[tokio::test]
    async fn clear_groups_refuses_an_unscoped_run() {
        // The plugin would treat this as "every channel"; crosstalk will not.
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let context = fixture.context();
        let error = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            Cleanup::ClearGroups,
            false,
        )
        .await
        .expect_err("refuses");
        assert!(format!("{error:#}").contains("[scope] groups"), "{error:#}");
        assert!(cleared(&fixture).await.is_empty());
    }

    #[tokio::test]
    async fn a_cleanup_with_nothing_to_do_still_writes_its_report() {
        let world = test_support::world();
        let mut fixture = Fixture::start(&world, &[ESPN_HD]).await;
        fixture.config.scope.groups = "Sports".to_string();
        let context = fixture.context();
        let settings = GuideSettings {
            epg_regex_to_remove: "Nomatch".to_string(),
            ..GuideSettings::default()
        };

        let outcome = run(
            &GuideApp::new(),
            &context,
            &settings,
            Cleanup::ClearByRegex,
            true,
        )
        .await
        .expect("preview");

        assert_eq!(outcome.counts.get("changes"), Some(&0));
        assert!(cleared(&fixture).await.is_empty());
        let text = std::fs::read_to_string(outcome.report_path.expect("report")).expect("read");
        assert!(text.contains("Changes planned: 0"), "{text}");
    }

    #[tokio::test]
    async fn a_cancelled_apply_writes_nothing_and_fails_its_rows() {
        let world = test_support::world();
        let mut fixture = Fixture::start(&world, &[ESPN_HD]).await;
        fixture.config.scope.groups = "News".to_string();
        let store = Store::in_memory().expect("store");
        let context = fixture.context_for_run(crate::APP_ID, "clear_groups_apply", &store);
        let run_id = context.run_id().expect("run id");
        context.cancel();

        // A cancelled run is not an error any more: it keeps the rows and the report it made
        // and says it was cancelled, so the history shows both that nothing landed and why.
        let outcome = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            Cleanup::ClearGroups,
            false,
        )
        .await
        .expect("a cancelled run still reports what it did");
        assert!(outcome.is_cancelled(), "{}", outcome.summary);
        assert!(
            outcome.summary.contains("cancelled by the operator"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("written"), Some(&0));
        assert!(
            outcome.report_path.is_some(),
            "the report is what says which rows were left undone"
        );
        assert!(cleared(&fixture).await.is_empty());
        let rows = store.rows(run_id).expect("rows");
        assert!(rows.iter().all(|row| row.status == "failed"));
        assert!(rows[0].reason.contains("cancelled"), "{}", rows[0].reason);
    }

    #[tokio::test]
    async fn the_app_runs_a_cleanup_from_the_settings_file() {
        // The whole path an operator takes: guide.toml on disk, then `crosstalk run guide
        // tag_missing_apply`. The suffix the action appends is the configured one, not the
        // default.
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let store = Store::in_memory().expect("store");
        fixture.record_scan(&store, &scan_rows_of(&world));
        crate::settings::save(
            &fixture.settings_path(),
            &GuideSettings {
                bad_epg_suffix: " (No Guide)".to_string(),
                ..GuideSettings::default()
            },
        )
        .expect("write guide.toml");
        let context = fixture.context_for_run(crate::APP_ID, "tag_missing_apply", &store);

        let outcome = GuideApp::new()
            .run(ACTION_TAG_MISSING_APPLY, &context, false)
            .await
            .expect("apply");

        assert!(outcome.summary.contains("Tag missing EPG applied"));
        let edits = channel_edits(&fixture).await;
        assert_eq!(
            edits.get(&4).expect("renamed")["name"],
            serde_json::json!("Télé-Québec (No Guide)")
        );
    }

    #[tokio::test]
    async fn a_stored_scan_row_is_not_a_channel_operation() {
        // A scan row records the guide the channel was on; it writes no field of its own, which
        // is what keeps a later `restore` from replaying it.
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let store = Store::in_memory().expect("store");
        fixture.record_scan(&store, &scan_rows_of(&world));
        let context = fixture.context_for_run(crate::APP_ID, "clear_missing_preview", &store);

        let scan = crate::scan::last_scan(&context).expect("scan");
        assert_eq!(scan.found(), 2);
        assert_eq!(scan.rows[0].status, STATUS_MISSING);
        assert!(scan
            .rows
            .iter()
            .all(|row| row.before_value.is_none() && row.field.is_empty()));
        assert!(scan.rows.iter().all(|row| row.epg_data_id.is_some()));
    }
}
