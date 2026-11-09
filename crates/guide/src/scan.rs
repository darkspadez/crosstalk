//! `scan`: which in-scope channels have a guide but no programme data in the check window?
//!
//! Port of `Plugin._scan_missing_epg_worker`. The plugin wrote the answer to a JSON results
//! file and the five cleanup actions read it back; crosstalk writes it to the run record, so
//! the result set is queryable, keeps its CSV, and cannot be overwritten by an unrelated run.
//! [`last_scan`] is the reader the cleanup actions use.
//!
//! The programme check is the one match and heal already use: `has_programs` over
//! `ProgramWindow::ahead(check_hours)`, bounded by the client's concurrency cap, memoised per
//! entry and window, cancelled between batches and reported per batch. A channel whose assigned
//! entry carries no source counts as empty, which is what the plugin's own query would answer.
//! A channel whose assigned entry the world does NOT know is `unchecked`, never missing: a
//! short or empty entry listing would otherwise put every assigned channel in the result set a
//! cleanup clears. An empty entry listing while in-scope channels carry guides refuses the scan.
//!
//! There is deliberately NO age limit on the result set (see [`last_scan`]): every cleanup
//! action re-plans from the current Dispatcharr state, so an old scan can only be out of date,
//! never unsafe.

use crate::settings::GuideSettings;
use anyhow::{bail, Result};
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{EpgEntry, HasPrograms, ProgramWindow, World};
use crosstalk_core::jobs::{JobContext, ProgressEvent};
use crosstalk_core::scope::ScopedChannels;
use crosstalk_core::store::RunRow;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use std::collections::HashMap;
use std::path::PathBuf;

/// The action id the result set is stored under.
pub const ACTION: &str = crate::ACTION_SCAN;

/// The columns the scan report carries, the same shape as the plugin's own export so an
/// operator's spreadsheet keeps working.
pub const COLUMNS: [&str; 8] = [
    "channel_id",
    "channel_name",
    "channel_number",
    "channel_group",
    "epg_channel_id",
    "epg_channel_name",
    "epg_source",
    "scanned_at",
];

/// The status a stored result row carries, so a later reader can tell a result row from a
/// matching or channel-operation row of the same run.
pub const STATUS_MISSING: &str = "MISSING";

/// Why the channel is in the result set, in the run row's `reason`.
const REASON: &str = "the assigned guide carries no programme data in the checked window";

/// Progress is published under this action name.
const PROGRESS_ACTION: &str = "guide.scan";

/// Channels per concurrent batch. The batch bounds how many checks are in flight on top of the
/// client's own cap, and a cancel is honoured between batches rather than after the whole scope.
const BATCH_CHANNELS: usize = 256;

/// One channel with a guide that carries no programme data.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanRow {
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_number: Option<f64>,
    pub channel_group: String,
    /// The guide entry the channel is assigned to.
    pub epg_data_id: i64,
    /// Its `tvg_id`, which is what the plugin's export calls `epg_channel_id`.
    pub epg_tvg_id: String,
    pub epg_name: String,
    pub epg_source: String,
}

impl ScanRow {
    /// The store's row shape. `channel_id` and `epg_data_id` are what
    /// [`crosstalk_core::channel_ops::Selection::from_scan_rows`] reads: the channel the scan
    /// found, and the guide it was on when the scan saw it.
    pub fn to_run_row(&self, ordinal: i64) -> RunRow {
        RunRow {
            ordinal,
            channel_id: self.channel_id,
            channel_name: self.channel_name.clone(),
            channel_group: self.channel_group.clone(),
            channel_number: self.channel_number,
            current_epg_name: Some(self.epg_name.clone()),
            epg_data_id: Some(self.epg_data_id),
            epg_name: Some(self.epg_name.clone()),
            epg_source: Some(self.epg_source.clone()),
            method: "scan".to_string(),
            status: STATUS_MISSING.to_string(),
            reason: REASON.to_string(),
            ..RunRow::default()
        }
    }
}

/// What a scan run produced.
#[derive(Debug, Clone, Default)]
pub struct ScanOutcome {
    pub rows: Vec<ScanRow>,
    pub report: Option<PathBuf>,
    /// How many in-scope channels carry a guide at all: the plugin's `total_channels`.
    pub checked: usize,
    /// Channels with a guide whose programme lookup FAILED. They are in `checked` but not
    /// in `rows`: a failed lookup is "unknown", never "missing", so a later cleanup can
    /// neither clear nor tag them. The plugin cannot fail this way (its check is one SQL
    /// query); failing the whole scan over one REST blip would be faithful but useless, so
    /// the scan completes with the gap counted and noted instead.
    pub unchecked: usize,
    /// How far ahead programmes were looked for.
    pub check_hours: u32,
    /// Whether a cancel stopped the scan. The rows above are the channels it managed to judge:
    /// they are written and reported, because a partial audit that says it is partial is worth
    /// more than nothing at all — and the result set is selected by "the last SUCCEEDED scan",
    /// so a cancelled one can never be used by a cleanup action.
    pub cancelled: bool,
}

/// What a later action needs from the last scan.
#[derive(Debug, Clone, PartialEq)]
pub struct LastScan {
    pub run_id: i64,
    pub started_at: String,
    pub rows: Vec<RunRow>,
}

impl LastScan {
    /// How many channels the scan found, which is the number the confirm-and-apply questions
    /// are really about.
    pub fn found(&self) -> usize {
        self.rows.len()
    }

    /// The scan in words, for a summary, a report preamble or a log line.
    pub fn describe(&self) -> String {
        format!(
            "scan run #{} started {} finding {} channel(s)",
            self.run_id,
            self.started_at,
            self.found()
        )
    }
}

/// The last finished scan, or the reason there is none.
///
/// The run store is the only source: unlike the plugin's results file there is no way to read
/// a result set from somewhere else, so an unrecorded scan (one killed before it finished) is
/// simply not available. The error says what to do about it.
pub fn last_scan(context: &JobContext) -> Result<LastScan> {
    let store = context.store().ok_or_else(|| {
        anyhow::anyhow!(
            "this run has no run store, so it cannot read the last scan: run the action from the \
             daemon or with `crosstalk run`, which records its runs"
        )
    })?;
    let Some(run) = store.last_succeeded_run(crate::APP_ID, ACTION)? else {
        bail!(
            "no finished `scan` run is recorded, so there is no result set to work from: run \
             `scan` first (its rows are what this action selects)"
        );
    };
    let rows = store.rows(run.id)?;
    Ok(LastScan {
        run_id: run.id,
        started_at: run.started_at,
        rows,
    })
}

/// Run the scan.
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &GuideSettings,
    world: &World,
    scoped_channels: &ScopedChannels,
) -> Result<ScanOutcome> {
    let client = context.dispatcharr()?;
    let check_hours = settings.check_hours;
    let window = ProgramWindow::ahead(check_hours);

    refuse_without_entries(world, scoped_channels)?;

    let entry_by_id: HashMap<i64, &EpgEntry> = world
        .entries
        .iter()
        .map(|entry| (entry.id, entry))
        .collect();

    // Which channels need a request at all. A channel with no guide, a source-less guide or a
    // guide the world does not know needs none: the first is not the plugin's set, the second
    // has nothing to ask, and the third cannot be judged from this listing.
    enum Check {
        Unassigned,
        Empty,
        Unknown,
        Ask(String, i64),
    }
    let checks: Vec<Check> = scoped_channels
        .channels
        .iter()
        .map(|channel| match channel.current_epg_id() {
            None => Check::Unassigned,
            Some(id) => match entry_by_id.get(&id) {
                Some(entry) => match entry.epg_source {
                    Some(source) => Check::Ask(entry.tvg_id.clone(), source),
                    None => Check::Empty,
                },
                None => Check::Unknown,
            },
        })
        .collect();

    let checked = checks
        .iter()
        .filter(|check| !matches!(check, Check::Unassigned))
        .count();
    context.progress(&ProgressEvent::new(
        PROGRESS_ACTION,
        0,
        checked as u64,
        "checking programme data",
    ));

    let concurrency = context.config().dispatcharr.concurrency.max(1);
    let mut answered: Vec<Option<HasPrograms>> = checks.iter().map(|_| None).collect();
    let mut failed: Vec<bool> = checks.iter().map(|_| false).collect();
    let mut cancelled = false;
    for batch_start in (0..checks.len()).step_by(BATCH_CHANNELS) {
        if context.is_cancelled() {
            // Stop asking, keep the answers: the rows below are the channels this run could
            // judge, and `unchecked` says how many it could not.
            cancelled = true;
            break;
        }
        let batch_end = (batch_start + BATCH_CHANNELS).min(checks.len());
        {
            use futures::{stream, StreamExt};
            let resolved: Vec<(usize, Result<HasPrograms>)> =
                stream::iter((batch_start..batch_end).filter_map(|ordinal| {
                    let (tvg_id, source) = match &checks[ordinal] {
                        Check::Ask(tvg_id, source) => (tvg_id.clone(), *source),
                        _ => return None,
                    };
                    let cancel = context.clone();
                    let window = window.clone();
                    Some(async move {
                        // Checked inside the future so a cancel lands within one in-flight
                        // round instead of after the whole batch. A cancel is an abort, not
                        // a result, so the batch cancel check below turns it into one.
                        if cancel.is_cancelled() {
                            return (ordinal, Err(anyhow::anyhow!("cancelled by the operator")));
                        }
                        let answer = client.has_programs(&tvg_id, source, &window).await;
                        (ordinal, answer)
                    })
                }))
                .buffer_unordered(concurrency)
                .collect()
                .await;
            for (ordinal, answer) in resolved {
                match answer {
                    Ok(answer) => answered[ordinal] = Some(answer),
                    // A failed lookup is "unknown", never "missing": one broken entry
                    // must neither fail the scan nor land the channel in the result set a
                    // cleanup later clears. The row loop below counts and skips it.
                    Err(_) => failed[ordinal] = true,
                }
            }
        }
        context.progress(&ProgressEvent::new(
            PROGRESS_ACTION,
            batch_end as u64,
            checked as u64,
            "checking programme data",
        ));
        if context.is_cancelled() {
            cancelled = true;
            break;
        }
    }

    let source_name = |id: Option<i64>| -> String {
        id.and_then(|id| world.sources.iter().find(|source| source.id == id))
            .map(|source| source.name.clone())
            .unwrap_or_else(|| "No Source".to_string())
    };

    let mut rows = Vec::new();
    let mut unchecked = 0;
    for (ordinal, channel) in scoped_channels.channels.iter().enumerate() {
        // A failed lookup skips the channel rather than reporting it: its guide may be
        // fine, and the cleanup actions must never see a channel the scan could not judge.
        if failed[ordinal] {
            unchecked += 1;
            continue;
        }
        let has = match &checks[ordinal] {
            Check::Unassigned => continue,
            // Not in the entry listing: a short read must never look like a missing guide.
            Check::Unknown => {
                unchecked += 1;
                continue;
            }
            Check::Empty => HasPrograms::No,
            Check::Ask(..) => match answered[ordinal] {
                Some(answer) => answer,
                // A cancelled run never asked this one: unknown, like a failed lookup.
                None => {
                    unchecked += 1;
                    continue;
                }
            },
        };
        if has == HasPrograms::Yes {
            continue;
        }
        let epg_data_id = channel.current_epg_id().unwrap_or_default();
        let entry = entry_by_id.get(&epg_data_id).copied();
        rows.push(ScanRow {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_number: channel.display_number(),
            channel_group: world.group_name(channel.channel_group_id),
            epg_data_id,
            epg_tvg_id: entry.map(|entry| entry.tvg_id.clone()).unwrap_or_default(),
            epg_name: entry
                .map(|entry| entry.name.clone())
                .unwrap_or_else(|| "Unknown".to_string()),
            epg_source: source_name(entry.and_then(|entry| entry.epg_source)),
        });
    }

    let run_rows: Vec<RunRow> = rows
        .iter()
        .enumerate()
        .map(|(ordinal, row)| row.to_run_row(ordinal as i64))
        .collect();
    context.record_rows(&run_rows)?;

    let report = write_report(app, context, settings, checked, unchecked, &rows)?;

    if let Some(path) = report.as_deref() {
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string());
        csv_report::prune_exports(
            &context.exports_dir(),
            &app.export_prefix(),
            context.config().exports.retention_days,
            name.as_deref(),
        );
    }

    Ok(ScanOutcome {
        rows,
        report,
        checked,
        unchecked,
        check_hours,
        cancelled,
    })
}

/// Refuse to judge guides from an empty entry listing while in-scope channels carry guides.
///
/// An empty `/epg/epgdata/` answer next to assigned channels is a broken read (a proxy, a
/// restarting server), not "every guide is gone"; acting on it would clear or reassign every
/// assignment in scope. Shared by scan and heal.
pub fn refuse_without_entries(world: &World, scoped_channels: &ScopedChannels) -> Result<()> {
    if !world.entries.is_empty() {
        return Ok(());
    }
    let assigned = scoped_channels
        .channels
        .iter()
        .filter(|channel| channel.current_epg_id().is_some())
        .count();
    if assigned > 0 {
        bail!(
            "Dispatcharr listed no EPG entries, yet {assigned} in-scope channel(s) carry a guide \
             assignment: refusing to judge guides from an empty listing. Check that the EPG \
             sources are loaded and that /api/epg/epgdata/ answers, then run again."
        );
    }
    Ok(())
}

fn write_report(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &GuideSettings,
    checked: usize,
    unchecked: usize,
    rows: &[ScanRow],
) -> Result<Option<PathBuf>> {
    let exports_dir = context.exports_dir();
    let mut report =
        csv_report::Report::new(&exports_dir, app, "scan", "Missing Program Data Report")
            .channels_processed(checked)
            .result("Channels with a guide assigned", checked)
            .result("Channels with missing program data", rows.len())
            .result(
                "Programme window",
                format!("next {}h", settings.check_hours),
            )
            .result(
                "Result set",
                "recorded as this run's rows; the cleanup actions select from it",
            );
    if unchecked > 0 {
        report = report.result(
            "Channels not checked (programme lookup failed, left out of the result set)",
            unchecked,
        );
    }

    let scanned_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let path = report.write(
        context.config(),
        &crate::settings::schema(),
        &crate::settings::to_values(settings),
        &COLUMNS,
        |writer| {
            // Names come from Dispatcharr: defuse spreadsheet formulas.
            for row in rows {
                writer.write_record([
                    row.channel_id.to_string(),
                    csv_report::safe_cell(&row.channel_name),
                    row.channel_number
                        .map(|number| format!("{number}"))
                        .unwrap_or_default(),
                    csv_report::safe_cell(&row.channel_group),
                    csv_report::safe_cell(&row.epg_tvg_id),
                    csv_report::safe_cell(&row.epg_name),
                    csv_report::safe_cell(&row.epg_source),
                    scanned_at.clone(),
                ])?;
            }
            Ok(())
        },
    )?;
    Ok(Some(path))
}

/// The outcome the runner records, in the plugin's own wording.
pub fn outcome(result: &ScanOutcome) -> JobOutcome {
    let missing = result.rows.len();
    let hours = result.check_hours;
    let summary = if result.checked == 0 {
        "EPG scan completed: no in-scope channel carries an EPG assignment. Assign guides \
         first, or check the group and profile settings."
            .to_string()
    } else if missing == 0 {
        format!(
            "EPG scan completed — next {hours}h: all {} channel(s) with a guide carry programme \
             data.",
            result.checked
        )
    } else {
        format!(
            "EPG scan completed — next {hours}h: {missing} of {} channel(s) with a guide carry no \
             programme data.",
            result.checked
        )
    };

    let mut outcome = OutcomeBuilder::new(summary)
        .count("channels_with_epg", result.checked as i64)
        .count("missing_data", missing as i64)
        .count("unchecked", result.unchecked as i64)
        .report(result.report.clone());
    if result.cancelled {
        // The cancel rule (see `crosstalk_core::jobs`): the channels judged so far are in the
        // report and the rows, the rest are `unchecked`, and the run is never recorded as a
        // successful scan — which is what stops a cleanup action selecting from it.
        outcome = outcome.cancelled().note(format!(
            "cancelled by the operator: {} channel(s) were not checked, so this result set is \
             partial and no cleanup action will use it",
            result.unchecked
        ));
    }
    if result.unchecked > 0 {
        outcome = outcome.note(format!(
            "{} channel(s) could not be checked (programme lookup failed) and are left out \
             of the result set, so the cleanup actions will not touch them",
            result.unchecked
        ));
    }
    outcome.build()
}

/// The scan a run is about to work from, as a progress line.
///
/// The confirmation dialog is static text ([`crosstalk_core::subapp::ActionSpec::confirm`]),
/// so this is where the scan's age and size are surfaced: at the start of the run, in the
/// summary and in the CSV preamble. `/runs/<id>` therefore shows which scan an apply used.
pub fn announce(context: &JobContext, action: &str, scan: &LastScan) {
    context.progress(&ProgressEvent::new(
        action,
        0,
        scan.found() as u64,
        format!("planning from {}", scan.describe()),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{self, Fixture, CNN_US, ESPN_HD};
    use crate::GuideApp;
    use crosstalk_core::channel_ops::Selection;
    use crosstalk_core::store::{RunStatus, Store};

    /// The fixture world, with only the ESPN guide carrying programmes.
    async fn fixture() -> (Fixture, World) {
        let world = test_support::world();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        (fixture, world)
    }

    async fn scan(fixture: &Fixture, context: &JobContext, world: &World) -> Result<ScanOutcome> {
        let scoped = fixture.scoped(world);
        run(
            &GuideApp::new(),
            context,
            &GuideSettings::default(),
            world,
            &scoped,
        )
        .await
    }

    #[tokio::test]
    async fn a_scan_finds_the_channels_whose_guide_carries_no_programmes() {
        let (fixture, world) = fixture().await;
        let context = fixture.context();

        let result = scan(&fixture, &context, &world).await.expect("scan");
        let ids: Vec<i64> = result.rows.iter().map(|row| row.channel_id).collect();
        assert_eq!(ids, test_support::missing_ids());
        // Three channels carry a guide at all: the plugin's `total_channels`.
        assert_eq!(result.checked, 3);
        assert_eq!(result.check_hours, 12);

        let cnn = &result.rows[0];
        assert_eq!(cnn.epg_name, "CNN [US]");
        // The plugin's export calls the entry's `tvg_id` `epg_channel_id`.
        assert_eq!(cnn.epg_tvg_id, "cnn.us");
        assert_eq!(cnn.epg_source, "Schedules Direct");
        assert_eq!(cnn.channel_group, "News");
        assert_eq!(cnn.channel_number, Some(2.0));
    }

    #[tokio::test]
    async fn the_scan_writes_the_plugin_export_columns() {
        let (fixture, world) = fixture().await;
        let context = fixture.context();
        let result = scan(&fixture, &context, &world).await.expect("scan");

        let path = result.report.expect("report written");
        let text = std::fs::read_to_string(&path).expect("read report");
        assert!(
            text.starts_with(&format!(
                "# crosstalk Guide v{} - Missing Program Data Report",
                crosstalk_core::HOST_VERSION
            )),
            "{text}"
        );
        assert!(text.contains(
            "channel_id,channel_name,channel_number,channel_group,epg_channel_id,\
             epg_channel_name,epg_source,scanned_at"
        ));
        assert!(text.contains("cnn.us"), "{text}");
        assert!(text.contains("naca.mx"), "{text}");
        // The run says what the result set is for, because a later action reads it.
        assert!(
            text.contains("Result set: recorded as this run's rows"),
            "{text}"
        );
        assert!(
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("guide_scan_")),
            "{}",
            path.display()
        );
    }

    #[tokio::test]
    async fn a_scan_stores_the_result_set_a_cleanup_reads() {
        let (fixture, world) = fixture().await;
        let store = Store::in_memory().expect("store");
        let context = fixture.context_for_run(crate::APP_ID, ACTION, &store);
        let run_id = context.run_id().expect("run id");

        scan(&fixture, &context, &world).await.expect("scan");
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("ok"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");

        let scan = last_scan(&context).expect("the finished scan");
        assert_eq!(scan.run_id, run_id);
        assert_eq!(scan.found(), 2);
        assert!(
            scan.describe().contains("2 channel(s)"),
            "{}",
            scan.describe()
        );

        // Exactly what `Selection::from_scan_rows` needs: the channels, and the guide each one
        // was on when the scan saw it.
        let selection = Selection::from_scan_rows(&scan.rows, &fixture.config.scope);
        assert_eq!(selection.channel_ids, Some(test_support::missing_ids()));
        assert_eq!(selection.expected_epg.get(&2), Some(&Some(CNN_US)));
    }

    #[tokio::test]
    async fn without_a_recorded_scan_the_reader_says_to_run_one() {
        let (fixture, _world) = fixture().await;
        let store = Store::in_memory().expect("store");
        let context = fixture.context_for_run(crate::APP_ID, "clear_missing_preview", &store);
        let error = last_scan(&context).expect_err("no scan");
        assert!(
            format!("{error:#}").contains("run `scan` first"),
            "{error:#}"
        );

        // A run with no store at all cannot look, and says so rather than pretending.
        let error = last_scan(&fixture.context()).expect_err("no store");
        assert!(format!("{error:#}").contains("no run store"), "{error:#}");
    }

    #[tokio::test]
    async fn the_outcome_uses_the_plugin_wording() {
        let (fixture, world) = fixture().await;
        let context = fixture.context();
        let result = scan(&fixture, &context, &world).await.expect("scan");
        let scanned = outcome(&result);
        assert_eq!(
            scanned.summary,
            "EPG scan completed — next 12h: 2 of 3 channel(s) with a guide carry no programme data."
        );
        assert_eq!(scanned.counts.get("missing_data"), Some(&2));
        assert_eq!(scanned.counts.get("channels_with_epg"), Some(&3));

        let clean = ScanOutcome {
            rows: Vec::new(),
            report: None,
            checked: 3,
            unchecked: 0,
            check_hours: 12,
            cancelled: false,
        };
        assert!(outcome(&clean).summary.contains("all 3 channel(s)"));
        let empty = ScanOutcome::default();
        assert!(outcome(&empty).summary.contains("no in-scope channel"));
    }

    #[tokio::test]
    async fn a_failed_programme_lookup_skips_the_channel_instead_of_reporting_it() {
        // A lookup that fails is "unknown", not "missing": reporting it would hand the
        // cleanup actions a channel whose guide may be fine, and `clear_missing_apply`
        // would remove a working assignment on REST-blip evidence.
        let world = test_support::world();
        let fixture = Fixture::start_with_program_failures(&world, &[ESPN_HD], &["cnn.us"]).await;
        let context = fixture.context();

        let result = scan(&fixture, &context, &world).await.expect("scan");
        let ids: Vec<i64> = result.rows.iter().map(|row| row.channel_id).collect();
        assert_eq!(ids, vec![4]);
        // Still counted as carrying a guide (the plugin's `total_channels`), but kept out
        // of the result set a cleanup selects from.
        assert_eq!(result.checked, 3);
        assert_eq!(result.unchecked, 1);

        let outcome = super::outcome(&result);
        assert_eq!(outcome.counts.get("unchecked"), Some(&1));
        assert!(
            outcome.summary.contains("will not touch them"),
            "{}",
            outcome.summary
        );
        let text = std::fs::read_to_string(result.report.expect("report")).expect("read");
        assert!(text.contains("left out of the result set"), "{text}");
    }

    #[tokio::test]
    async fn a_guide_entry_without_a_source_counts_as_empty() {
        // Nothing to ask about: the plugin's own query would find no programmes either.
        let mut world = test_support::world();
        world.entries[1].epg_source = None;
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let context = fixture.context();

        let result = scan(&fixture, &context, &world).await.expect("scan");
        let cnn = result
            .rows
            .iter()
            .find(|row| row.channel_id == 2)
            .expect("the CNN channel is still missing");
        assert_eq!(cnn.epg_source, "No Source");
        // No request was made for it: the world already answered.
        assert!(
            fixture
                .queries("GET", "/api/epg/programs/search/")
                .await
                .iter()
                .all(|query| !query.contains("cnn.us")),
            "the source-less entry is never asked about"
        );
    }

    #[tokio::test]
    async fn a_guide_missing_from_the_entry_listing_is_unchecked_not_missing() {
        // B1: a short entry read must never put a channel in the result set a cleanup clears.
        let mut world = test_support::world();
        world.entries.retain(|entry| entry.id != CNN_US);
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let context = fixture.context();

        let result = scan(&fixture, &context, &world).await.expect("scan");
        assert!(result.rows.iter().all(|row| row.channel_id != 2));
        assert_eq!(result.unchecked, 1);
    }

    #[tokio::test]
    async fn an_empty_entry_listing_with_assigned_channels_refuses_the_scan() {
        // B1: `200 {}` (or `[]`) from /epg/epgdata/ next to assigned channels is a broken
        // read; the scan refuses instead of reporting every guide missing.
        let mut world = test_support::world();
        world.entries.clear();
        let fixture = Fixture::start(&world, &[ESPN_HD]).await;
        let context = fixture.context();

        let error = scan(&fixture, &context, &world).await.expect_err("empty");
        assert!(format!("{error:#}").contains("no EPG entries"), "{error:#}");
    }

    #[tokio::test]
    async fn a_cancelled_scan_reports_the_cancel_and_records_what_it_judged() {
        // Not an error: the rows it managed to judge are recorded, everything it did not reach
        // is counted as unchecked, and the outcome says cancelled so no cleanup action can
        // select from a partial result set.
        let (fixture, world) = fixture().await;
        let store = Store::in_memory().expect("store");
        let context = fixture.context_for_run(crate::APP_ID, crate::ACTION_SCAN, &store);
        context.cancel();
        let result = scan(&fixture, &context, &world)
            .await
            .expect("a cancelled scan still reports");
        assert!(result.cancelled);
        assert!(
            result.rows.is_empty(),
            "nothing was asked, so nothing is judged"
        );
        assert_eq!(
            result.unchecked, result.checked,
            "every channel is unchecked"
        );

        let outcome = outcome(&result);
        assert!(outcome.is_cancelled());
        assert!(
            outcome.summary.contains("cancelled by the operator"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts["unchecked"], result.checked as i64);
    }
}
