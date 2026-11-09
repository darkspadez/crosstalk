//! Scan & heal: find assignments that carry no programme data, and replace them with one that does.
//!
//! Port of `Plugin._scan_and_heal_worker`. The rule that makes this safe to run unattended is
//! that heal only touches an assignment it can PROVE is empty: a channel whose current guide is
//! working is never moved, and a channel for which no replacement is found is left exactly as
//! it was rather than cleared.
//!
//! Step 8 (goal reset): heal searches the channel's identity FAMILY (the unified engine's
//! `rank`, at the ONE `[matching]` cut) instead of the plugin's full ranked list. The
//! family's first member that carries programme data (excluding the entry already
//! assigned) is the replacement; below-cut candidates never enter the family, so the run
//! path no longer proposes `SkippedLowConfidence` replacements (D02/T10: one cut). Janitor's
//! standalone structural score (D06) is retired along with it: callsign agreement decides
//! at family level (C06/D04).
//!
//! Heal never consults the AI gate — it is heuristic-only by design (owner Q4), so every
//! replacement above carries the engine's own score and method, with no model in the loop.
//!
//! The order mirrors the plugin: find the broken channels, search replacements (excluding the
//! one already assigned), take the first family member that has programmes, then apply.

use crate::engine::RunEngine;
use crate::matching::to_scored;
use crate::scope::ScopedEntries;
use crate::settings::GuideSettings;
use anyhow::{Context, Result};
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{EpgEntry, HasPrograms, ProgramWindow, World};
use crosstalk_core::jobs::{JobContext, ProgressEvent};
use crosstalk_core::match_run::{self, ConfidenceBand};
use crosstalk_core::scope::ScopedChannels;
use crosstalk_core::store::RunRow;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Channels per concurrent batch in step 2b. Bounds in-flight validation and honours
/// a cancel between batches rather than only after the whole broken set.
const BATCH_CHANNELS: usize = 256;

/// The columns the heal report carries.
pub const COLUMNS: [&str; 12] = [
    "channel_id",
    "channel_name",
    "channel_number",
    "channel_group",
    "original_epg_name",
    "original_epg_source",
    "new_epg_name",
    "new_epg_source",
    "match_confidence",
    "match_method",
    "status",
    "reason",
];

/// How one channel came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealStatus {
    /// The search found nothing.
    NoReplacementFound,
    /// A replacement was found; the run was a preview.
    ReplacementPreview,
    /// A replacement was applied.
    Healed,
    /// A replacement was found but below the threshold.
    SkippedLowConfidence,
}

impl HealStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            HealStatus::NoReplacementFound => "NO_REPLACEMENT_FOUND",
            HealStatus::ReplacementPreview => "REPLACEMENT_PREVIEW",
            HealStatus::Healed => "HEALED",
            HealStatus::SkippedLowConfidence => "SKIPPED_LOW_CONFIDENCE",
        }
    }
}

/// One channel's heal result.
#[derive(Debug, Clone)]
pub struct HealRow {
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_number: Option<f64>,
    pub channel_group: String,
    pub original_epg_id: i64,
    pub original_epg_name: String,
    pub original_epg_source: String,
    pub new_epg_id: Option<i64>,
    pub new_epg_name: Option<String>,
    pub new_epg_source: Option<String>,
    pub confidence: u8,
    pub method: String,
    pub status: HealStatus,
    pub reason: String,
}

impl HealRow {
    pub fn to_run_row(&self, ordinal: i64) -> RunRow {
        RunRow {
            ordinal,
            channel_id: self.channel_id,
            channel_name: self.channel_name.clone(),
            channel_group: self.channel_group.clone(),
            channel_number: self.channel_number,
            current_epg_name: Some(self.original_epg_name.clone()),
            epg_data_id: self.new_epg_id,
            epg_name: self.new_epg_name.clone(),
            epg_source: self.new_epg_source.clone(),
            score: i64::from(self.confidence),
            method: self.method.clone(),
            status: self.status.as_str().to_string(),
            reason: self.reason.clone(),
            ai_decision: None,
            ..RunRow::default()
        }
    }
}

/// What a heal run produced.
#[derive(Debug, Clone, Default)]
pub struct HealOutcome {
    pub rows: Vec<HealRow>,
    pub report: Option<PathBuf>,
    pub scanned: usize,
    pub broken: usize,
    pub replacements: usize,
    pub applied: usize,
    /// Assigned channels heal could not judge: the programme check failed, or the assigned
    /// entry is missing from the entry listing. They are left exactly as they are.
    pub unchecked: usize,
}

/// Run scan & heal.
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &GuideSettings,
    world: &World,
    scoped_channels: &ScopedChannels,
    scoped_entries: &ScopedEntries,
    dry_run: bool,
) -> Result<HealOutcome> {
    let client = context.dispatcharr()?;
    let window = ProgramWindow::ahead(settings.check_hours);
    // The ONE cut (D02/T10): `[matching]` sensitivity via the run engine below.
    // Heal's own threshold is retired (step 8).
    let allow_without_programs = settings.allow_epg_without_programs;

    crate::scan::refuse_without_entries(world, scoped_channels)?;

    let entry_by_id: HashMap<i64, &EpgEntry> = world
        .entries
        .iter()
        .map(|entry| (entry.id, entry))
        .collect();
    let source_name = |id: Option<i64>| -> String {
        id.and_then(|id| world.sources.iter().find(|source| source.id == id))
            .map(|source| source.name.clone())
            .unwrap_or_else(|| "Unknown".to_string())
    };

    // Step 1: which in-scope channels are assigned but carry no programmes?
    // The checks fan out (P1), bounded by the Dispatcharr concurrency; assignments that
    // need no request resolve without one. Broken channels collect in channel order.
    context.progress(&ProgressEvent::new(
        "guide.heal",
        0,
        scoped_channels.channels.len() as u64,
        "checking existing assignments",
    ));

    enum Detection {
        Unassigned,
        /// The assigned entry is not in the listing: a short read, not proof of emptiness.
        Unknown,
        Decided(HasPrograms),
        Check(String, i64),
    }
    let mut plans: Vec<Detection> = Vec::with_capacity(scoped_channels.channels.len());
    for channel in &scoped_channels.channels {
        let Some(current) = channel.current_epg_id() else {
            plans.push(Detection::Unassigned);
            continue;
        };
        match entry_by_id.get(&current) {
            Some(entry) => match entry.epg_source {
                Some(source) if !allow_without_programs => {
                    plans.push(Detection::Check(entry.tvg_id.clone(), source))
                }
                // Without a source there is nothing to ask about; treat it as empty, which is
                // what the plugin's own query would find.
                Some(_) => plans.push(Detection::Decided(HasPrograms::Yes)),
                None => plans.push(Detection::Decided(HasPrograms::No)),
            },
            None => plans.push(Detection::Unknown),
        }
    }

    // Settle what one request per source and per 50 entries can (E-B1); the checks below
    // then answer those from the memo and search only the rest.
    if !allow_without_programs {
        let asked: Vec<&EpgEntry> = scoped_channels
            .channels
            .iter()
            .filter_map(|channel| entry_by_id.get(&channel.current_epg_id()?).copied())
            .collect();
        crate::programs::seed(context, client, world, asked, &window, "guide.heal").await;
    }

    let validation_bound = context.config().dispatcharr.concurrency.max(1);
    let window_ref = &window;
    // `None` inside: the check failed, which is "unknown", never "no".
    let mut detected: Vec<Option<Option<HasPrograms>>> = (0..plans.len()).map(|_| None).collect();
    // In batches, so a cancel lands between them and a big scope is not silent.
    for batch_start in (0..plans.len()).step_by(BATCH_CHANNELS) {
        if context.is_cancelled() {
            anyhow::bail!("cancelled by the operator");
        }
        let batch_end = (batch_start + BATCH_CHANNELS).min(plans.len());
        use futures::{stream, StreamExt};
        let resolved: Vec<(usize, Option<HasPrograms>)> =
            stream::iter((batch_start..batch_end).filter_map(|ordinal| {
                // Owned up front: the future must not borrow the plan, and the
                // closure argument is an owned index so no borrowed-iterator
                // lifetime leaks into the future's type.
                let (tvg_id, source) = match &plans[ordinal] {
                    Detection::Check(tvg_id, source) => (tvg_id.clone(), *source),
                    _ => return None,
                };
                let cancel = context.clone();
                Some(async move {
                    // Checked inside the future so a cancel lands within one
                    // in-flight round instead of after the whole phase. `None` is
                    // the "no result" value; the cancel check below aborts.
                    if cancel.is_cancelled() {
                        return (ordinal, None);
                    }
                    // A failed check is unknown: treating it as "no" would replace a
                    // working guide on REST-blip evidence.
                    let has = client.has_programs(&tvg_id, source, window_ref).await.ok();
                    (ordinal, has)
                })
            }))
            .buffer_unordered(validation_bound)
            .collect()
            .await;
        for (ordinal, has) in resolved {
            detected[ordinal] = Some(has);
        }
        context.progress(&ProgressEvent::new(
            "guide.heal",
            batch_end as u64,
            plans.len() as u64,
            "checking existing assignments",
        ));
    }
    if context.is_cancelled() {
        anyhow::bail!("cancelled by the operator");
    }

    let mut broken = Vec::new();
    let mut unchecked = 0;
    for (ordinal, (channel, plan)) in scoped_channels
        .channels
        .iter()
        .zip(plans.iter())
        .enumerate()
    {
        let has = match plan {
            Detection::Unassigned => continue,
            Detection::Unknown => {
                unchecked += 1;
                continue;
            }
            Detection::Decided(has) => *has,
            Detection::Check(..) => {
                let checked = detected[ordinal].expect("every checked channel was resolved");
                let Some(has) = checked else {
                    unchecked += 1;
                    continue;
                };
                has
            }
        };
        if has == HasPrograms::No {
            broken.push(channel.clone());
        }
    }

    let scanned = scoped_channels.channels.len();

    // Step 2: search a replacement for each broken channel.
    // The unified stack: engine + identity cache at the ONE host cut. `threshold` is
    // `[matching]` sensitivity, never the retired `heal_confidence_threshold`.
    let run_engine = RunEngine::for_config(&context.config().matching, context.store().cloned());
    let threshold = run_engine.threshold();
    let names: Vec<String> = scoped_entries
        .entries
        .iter()
        .map(|entry| entry.name.clone())
        .collect();
    let index = Arc::new(run_engine.build_index_blocking(&names).await?);

    // Step 2a: score every broken channel's identity family off the async runtime (P3),
    // through the shared family stage: no progress bar, because only the broken channels
    // are scored. `families_all[i]` is `broken[i]`'s best family, in rank order, at the cut.
    let broken_names: Vec<String> = broken.iter().map(|channel| channel.name.clone()).collect();
    let families_all = match_run::score_families(
        &broken_names,
        Arc::clone(run_engine.engine()),
        Arc::clone(&index),
        None,
    )
    .await?;
    // The query identities resolve through the shared cache, so the store holds the
    // identity any other app resolves the same name to. In the background while the
    // walks below run (E-B2), awaited before the run returns.
    let warming = run_engine.start_warming_families(&broken_names, &families_all, &names, &[]);

    // Exclude the entry already assigned from each family: proposing the same broken
    // guide is not a fix. Positions below are into these searched lists, not the raw
    // families.
    let searched: Vec<Vec<crosstalk_matching::pipeline::Scored>> = broken
        .iter()
        .zip(families_all.iter())
        .map(|(channel, family)| {
            let original_id = channel.current_epg_id().unwrap_or(0);
            let kept: Vec<crosstalk_matching::engine::EngineRanked> = family
                .iter()
                .filter(|member| {
                    scoped_entries
                        .entries
                        .get(member.index)
                        .is_some_and(|entry| entry.id != original_id)
                })
                .cloned()
                .collect();
            to_scored(&kept)
        })
        .collect();

    // Step 2b: walk each broken channel's searched family serially, concurrently across
    // channels (P1). Picks resolve by channel order below. Work runs in batches so
    // a cancel is honoured between them and progress is published per batch.
    // The shared walk (same as auto-match): the first family member that carries
    // programme data wins. Below-cut candidates never entered the family (D02/T10: one
    // cut), so the run path no longer proposes sub-threshold SkippedLowConfidence
    // replacements the way the plugin's full-list walk did.
    let rules = crosstalk_core::match_run::WalkRules {
        threshold,
        validate: !allow_without_programs,
    };
    let probe = crate::matching::EpgProbe {
        client,
        window: ProgramWindow::ahead(settings.check_hours),
    };
    if rules.validate {
        // Every member a walk could ask about, settled up front where it can be (E-B1).
        let mut seen = std::collections::HashSet::new();
        let asked: Vec<&EpgEntry> = searched
            .iter()
            .flatten()
            .filter(|candidate| candidate.score >= threshold && seen.insert(candidate.index))
            .filter_map(|candidate| scoped_entries.entries.get(candidate.index))
            .collect();
        crate::programs::seed(context, client, world, asked, &probe.window, "guide.heal").await;
    }
    let probe_ref = &probe;
    let searched_ref = &searched;
    let entries_ref = &scoped_entries.entries;
    let mut picks: Vec<Option<usize>> = (0..broken.len()).map(|_| None).collect();
    // The candidate whose programme check failed and stopped a channel's walk.
    let mut unknown_picks: Vec<Option<String>> = (0..broken.len()).map(|_| None).collect();
    for batch_start in (0..broken.len()).step_by(BATCH_CHANNELS) {
        if context.is_cancelled() {
            anyhow::bail!("cancelled by the operator");
        }
        let batch_end = (batch_start + BATCH_CHANNELS).min(broken.len());
        {
            use futures::{stream, StreamExt};
            let resolved: Vec<(usize, match_run::WalkResult)> =
                stream::iter((batch_start..batch_end).map(|ordinal| {
                    let cancel = context.clone();
                    async move {
                        let walked = match_run::walk(
                            &cancel,
                            entries_ref,
                            &searched_ref[ordinal],
                            None,
                            rules,
                            probe_ref,
                        )
                        .await;
                        (ordinal, walked)
                    }
                }))
                .buffer_unordered(validation_bound)
                .collect()
                .await;
            for (ordinal, walked) in resolved {
                picks[ordinal] = walked.chosen;
                unknown_picks[ordinal] = walked.unknown;
            }
        }
        context.progress(&ProgressEvent::new(
            "guide.heal",
            batch_end as u64,
            broken.len() as u64,
            "searching replacements",
        ));
        if context.is_cancelled() {
            anyhow::bail!("cancelled by the operator");
        }
    }

    let mut rows = Vec::with_capacity(broken.len());
    for (position, channel) in broken.iter().enumerate() {
        let original_id = channel.current_epg_id().unwrap_or(0);
        let original_entry = entry_by_id.get(&original_id);
        let mut row = HealRow {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_number: channel.display_number(),
            channel_group: world.group_name(channel.channel_group_id),
            original_epg_id: original_id,
            original_epg_name: original_entry
                .map(|entry| entry.name.clone())
                .unwrap_or_else(|| "Unknown".to_string()),
            original_epg_source: source_name(original_entry.and_then(|entry| entry.epg_source)),
            new_epg_id: None,
            new_epg_name: None,
            new_epg_source: None,
            confidence: 0,
            method: "NO_REPLACEMENT_FOUND".to_string(),
            status: HealStatus::NoReplacementFound,
            reason: String::new(),
        };

        // The concurrent walk above already chose the first searched family member
        // carrying programme data; this only maps it back.
        let candidates = &searched[position];
        let mut chosen = None;
        if let Some(rank_position) = picks[position] {
            if let Some(candidate) = candidates.get(rank_position) {
                if let Some(entry) = scoped_entries.entries.get(candidate.index) {
                    chosen = Some((candidate, entry));
                }
            }
        }

        if let Some((candidate, entry)) = chosen {
            row.new_epg_id = Some(entry.id);
            row.new_epg_name = Some(entry.name.clone());
            row.new_epg_source = Some(source_name(entry.epg_source));
            row.confidence = candidate.score;
            row.method = candidate.method_label();
            // A preview records every replacement it found. Every family member clears
            // the ONE cut by construction (D02/T10), so an apply heals whatever the walk
            // found; SkippedLowConfidence survives only as a status word (the plugin's
            // vocabulary) for rows constructed elsewhere, never from this run path.
            row.status = if dry_run {
                HealStatus::ReplacementPreview
            } else if candidate.score >= threshold {
                HealStatus::Healed
            } else {
                // Unreachable through the family cut, kept so the reason text still
                // names the cut that would have rejected it.
                HealStatus::SkippedLowConfidence
            };
        }
        row.reason = reason_for(&row, threshold, allow_without_programs);
        if let Some(unknown) = unknown_picks[position].as_ref() {
            row.reason = format!(
                "The programme check for {unknown} failed; left as it is instead of settling \
                 for a lower-ranked replacement"
            );
        }
        rows.push(row);

        if position % 50 == 0 {
            context.progress(&ProgressEvent::new(
                "guide.heal",
                position as u64,
                broken.len() as u64,
                "searching replacements",
            ));
        }
        if context.is_cancelled() {
            anyhow::bail!("cancelled by the operator");
        }
    }

    let run_rows: Vec<RunRow> = rows
        .iter()
        .enumerate()
        .map(|(ordinal, row)| row.to_run_row(ordinal as i64))
        .collect();
    context.record_rows(&run_rows)?;

    let replacements = rows.iter().filter(|row| row.new_epg_id.is_some()).count();

    // Step 3: write the report BEFORE applying, so it reflects the plan. On an apply
    // failure the CSV still exists on disk recording what was attempted and confirmed,
    // and the error below names it (mirroring matching, which also reports before it
    // writes). The run record itself cannot link it: the runner drops the report path on
    // a failed run.
    let report = write_report(app, context, settings, scanned, &rows, dry_run)?;

    // Step 4: write the replacements that clear the threshold.
    let mut applied = 0;
    if !dry_run {
        let associations: Vec<(i64, i64)> = rows
            .iter()
            .filter(|row| row.status == HealStatus::Healed)
            .filter_map(|row| row.new_epg_id.map(|epg| (row.channel_id, epg)))
            .collect();
        let plan = report
            .as_deref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "(no report)".to_string());
        applied = crate::matching::write_assignments(
            client,
            context,
            world,
            &associations,
            run_rows.len() as i64,
            "guide.heal",
        )
        .await
        .with_context(|| {
            format!("applying the EPG replacements; the plan was recorded in {plan}")
        })?;
    }

    // Tidy up only after a successful run: deleting older reports before an apply that then
    // fails would throw away the history while producing nothing to replace it.
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

    warming.finish().await;
    Ok(HealOutcome {
        rows,
        report,
        scanned,
        broken: broken.len(),
        replacements,
        applied,
        unchecked,
    })
}

/// The reason text, in the plugin's wording.
fn reason_for(row: &HealRow, threshold: u8, allow_without_programs: bool) -> String {
    let mut reason = match row.status {
        HealStatus::NoReplacementFound => return "No working replacement EPG found".to_string(),
        HealStatus::SkippedLowConfidence => format!(
            "Replacement found but confidence {}% below threshold ({threshold}%)",
            row.confidence
        ),
        _ => {
            let confidence = row.confidence;
            match ConfidenceBand::of(confidence) {
                ConfidenceBand::Excellent => {
                    format!("Excellent replacement match ({confidence}%)")
                }
                ConfidenceBand::High => format!("High confidence replacement ({confidence}%)"),
                ConfidenceBand::Good => format!("Good replacement match ({confidence}%)"),
                ConfidenceBand::Moderate => {
                    format!("Moderate confidence replacement ({confidence}%)")
                }
                ConfidenceBand::Low => format!("Low confidence replacement ({confidence}%)"),
            }
        }
    };
    if !row.method.is_empty() && row.method != "NO_REPLACEMENT_FOUND" {
        reason.push_str(&format!(" - {}", row.method));
    }
    if allow_without_programs {
        reason.push_str(" [program data check skipped]");
    }
    reason
}

fn write_report(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &GuideSettings,
    scanned: usize,
    rows: &[HealRow],
    dry_run: bool,
) -> Result<Option<PathBuf>> {
    let replacements = rows.iter().filter(|row| row.new_epg_id.is_some()).count();
    let applied = rows
        .iter()
        .filter(|row| row.status == HealStatus::Healed)
        .count();

    let exports_dir = context.exports_dir();
    let cut = crate::engine::cut_setting(&context.config().matching);
    let report = csv_report::Report::new(
        &exports_dir,
        app,
        if dry_run {
            "heal_preview"
        } else {
            "heal_applied"
        },
        if dry_run {
            "Scan and Heal Preview Report"
        } else {
            "Scan and Heal Applied Report"
        },
    )
    .channels_processed(scanned)
    .result("Broken assignments examined", rows.len())
    .result("Replacements found", replacements)
    .result(
        "Replacements written",
        if dry_run {
            "none, this was a preview".to_string()
        } else {
            applied.to_string()
        },
    )
    .setting(cut.0, cut.1);

    let path = report.write(
        context.config(),
        &crate::settings::schema(),
        &crate::settings::to_values(settings),
        &COLUMNS,
        |writer| {
            for row in rows {
                // Names and reasons come from Dispatcharr: defuse spreadsheet formulas.
                writer.write_record([
                    row.channel_id.to_string(),
                    csv_report::safe_cell(&row.channel_name),
                    row.channel_number
                        .map(|number| format!("{number}"))
                        .unwrap_or_default(),
                    csv_report::safe_cell(&row.channel_group),
                    csv_report::safe_cell(&row.original_epg_name),
                    csv_report::safe_cell(&row.original_epg_source),
                    csv_report::safe_cell(
                        &row.new_epg_name
                            .clone()
                            .unwrap_or_else(|| "N/A".to_string()),
                    ),
                    csv_report::safe_cell(
                        &row.new_epg_source
                            .clone()
                            .unwrap_or_else(|| "N/A".to_string()),
                    ),
                    row.confidence.to_string(),
                    csv_report::safe_cell(&row.method),
                    row.status.as_str().to_string(),
                    csv_report::safe_cell(&row.reason),
                ])?;
            }
            Ok(())
        },
    )?;
    Ok(Some(path))
}

/// The outcome the runner records.
pub fn outcome(result: &HealOutcome, dry_run: bool, threshold: u8) -> JobOutcome {
    let high_confidence = result
        .rows
        .iter()
        .filter(|row| row.status == HealStatus::Healed)
        .count();
    let low_confidence = result
        .rows
        .iter()
        .filter(|row| row.status == HealStatus::SkippedLowConfidence)
        .count();

    let mut outcome = OutcomeBuilder::new(format!(
        "Scan & Heal {}: scanned {}, {} broken, {} replacement(s) found",
        if dry_run { "preview" } else { "applied" },
        result.scanned,
        result.broken,
        result.replacements
    ))
    .count("scanned", result.scanned as i64)
    .count("broken", result.broken as i64)
    .count("replacements", result.replacements as i64)
    .count("low_confidence", low_confidence as i64)
    .count("healed", high_confidence as i64)
    .count("applied", result.applied as i64);
    if low_confidence > 0 {
        outcome = outcome.note(format!("{low_confidence} below threshold"));
    }
    if result.unchecked > 0 {
        outcome = outcome
            .count("unchecked", result.unchecked as i64)
            .note(format!(
                "{} assigned channel(s) could not be judged (programme check failed, or the \
                 guide is missing from the entry listing) and were left as they are",
                result.unchecked
            ));
    }
    if dry_run {
        let would_apply = result
            .rows
            .iter()
            .filter(|row| row.new_epg_id.is_some() && row.confidence >= threshold)
            .count();
        outcome = outcome.note(format!("{would_apply} would be applied"));
    } else {
        outcome = outcome.note(format!(
            "{high_confidence} healed, {} assignments written",
            result.applied
        ));
    }
    outcome.report(result.report.clone()).build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GuideApp;
    use crosstalk_core::dispatcharr::{Channel, ChannelGroup, DispatcharrClient, EpgSource};
    use crosstalk_core::jobs::VecSink;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// One channel on a broken guide, one working guide to replace it with.
    async fn fixture(
        old_has_programs: bool,
        new_has_programs: bool,
    ) -> (MockServer, World, JobContext) {
        fixture_with_names(
            "ESPN HD",
            "ESPN (broken)",
            "ESPN",
            old_has_programs,
            new_has_programs,
            true,
        )
        .await
    }

    /// [`fixture`] with caller-chosen names, so a test can arrange a replacement that
    /// scores below the threshold (a structural-only signal, not a name match).
    /// `apply_ok` decides whether the batch-set-epg endpoint answers success: a failing
    /// endpoint proves the report is written before the apply, so the plan survives the
    /// failure on disk.
    async fn fixture_with_names(
        channel_name: &str,
        old_name: &str,
        new_name: &str,
        old_has_programs: bool,
        new_has_programs: bool,
        apply_ok: bool,
    ) -> (MockServer, World, JobContext) {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/channels/channels/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": 1, "name": channel_name, "channel_number": 101, "channel_group_id": 10, "epg_data_id": 7, "streams": []}
            ])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/channels/groups/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([{"id": 10, "name": "Sports"}])),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/channels/profiles/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/epg/sources/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": 4, "name": "Schedules Direct", "is_active": true, "priority": 5}
            ])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/epg/epgdata/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": 7, "name": old_name, "tvg_id": "old.us", "epg_source": 4},
                {"id": 8, "name": new_name, "tvg_id": "new.us", "epg_source": 4}
            ])))
            .mount(&server)
            .await;

        let answer = |has: bool| {
            if has {
                serde_json::json!({"count": 1, "results": [{"id": 1}]})
            } else {
                serde_json::json!({"count": 0, "results": []})
            }
        };
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .and(query_param("tvg_id", "old.us"))
            .respond_with(ResponseTemplate::new(200).set_body_json(answer(old_has_programs)))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .and(query_param("tvg_id", "new.us"))
            .respond_with(ResponseTemplate::new(200).set_body_json(answer(new_has_programs)))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/channels/channels/batch-set-epg/"))
            .respond_with(if apply_ok {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "success": true,
                    "channels_updated": 1,
                    "programs_refreshed": 1
                }))
            } else {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "success": false,
                    "channels_updated": 0
                }))
            })
            .mount(&server)
            .await;

        let world = World {
            channels: vec![Channel {
                id: 1,
                name: channel_name.to_string(),
                channel_number: Some(serde_json::json!(101)),
                channel_group_id: Some(10),
                epg_data_id: Some(7),
                ..Channel::default()
            }],
            groups: vec![ChannelGroup {
                id: 10,
                name: "Sports".to_string(),
            }],
            sources: vec![EpgSource {
                id: 4,
                name: "Schedules Direct".to_string(),
                is_active: true,
                priority: 5,
                ..EpgSource::default()
            }],
            entries: vec![
                EpgEntry {
                    id: 7,
                    name: old_name.to_string(),
                    tvg_id: "old.us".to_string(),
                    epg_source: Some(4),
                    icon_url: None,
                },
                EpgEntry {
                    id: 8,
                    name: new_name.to_string(),
                    tvg_id: "new.us".to_string(),
                    epg_source: Some(4),
                    icon_url: None,
                },
            ],
            ..World::default()
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = crosstalk_core::CrosstalkConfig::load(dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        config.jobs.exports_dir = dir.path().join("exports").display().to_string();
        let client = DispatcharrClient::new(&config.dispatcharr).expect("client");
        let context = JobContext::new(
            std::sync::Arc::new(config),
            std::sync::Arc::new(VecSink::new()),
        )
        .with_dispatcharr(client);
        (server, world, context)
    }

    fn scoped(world: &World) -> (ScopedChannels, ScopedEntries) {
        let scope = crosstalk_core::config::ScopeConfig::default();
        (
            crosstalk_core::scope::scope_channels(world, &scope).expect("channels"),
            crate::scope::scope_entries(world, ""),
        )
    }

    #[tokio::test]
    async fn a_broken_assignment_gets_a_working_replacement() {
        let (_server, world, context) = fixture(false, true).await;
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect("preview");
        assert_eq!(result.broken, 1);
        let row = &result.rows[0];
        assert_eq!(row.status, HealStatus::ReplacementPreview);
        assert_eq!(row.new_epg_id, Some(8), "the new guide, not the broken one");
        assert_eq!(row.original_epg_name, "ESPN (broken)");
        assert_eq!(result.applied, 0);
        assert!(row.reason.contains("replacement match"));
    }

    #[tokio::test]
    async fn an_apply_writes_the_replacement() {
        let (_server, world, context) = fixture(false, true).await;
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);
        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            &world,
            &channels,
            &entries,
            false,
        )
        .await
        .expect("apply");
        assert_eq!(result.rows[0].status, HealStatus::Healed);
        assert_eq!(result.applied, 1);
    }

    #[tokio::test]
    async fn a_failed_apply_still_leaves_the_report_on_disk() {
        // The report is written before the apply, so a batch failure still leaves the plan
        // on disk and names it in the error (the runner drops the report path on failure).
        let (_server, world, context) =
            fixture_with_names("ESPN HD", "ESPN (broken)", "ESPN", false, true, false).await;
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);
        let error = run(
            &GuideApp::new(),
            &context,
            &settings,
            &world,
            &channels,
            &entries,
            false,
        )
        .await
        .expect_err("the apply fails");
        let message = format!("{error:#}");
        assert!(message.contains("recorded in"), "{message}");

        let reports: Vec<_> = std::fs::read_dir(context.exports_dir())
            .expect("exports dir")
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains("heal_applied"))
            })
            .collect();
        assert_eq!(reports.len(), 1, "the plan survives the failure");
        let text = std::fs::read_to_string(&reports[0]).expect("read report");
        assert!(text.contains("ESPN HD"), "{text}");
        assert!(
            message.contains(&reports[0].display().to_string()),
            "the error names the report: {message}"
        );
    }

    #[tokio::test]
    async fn hostile_names_are_defused_in_the_report() {
        // The broken guide and its replacement come from Dispatcharr: names starting
        // with `=`, `+` or `@` must reach the CSV quoted, never as live formulas.
        let (_server, world, context) =
            fixture_with_names("+ESPN HD", "=ESPN (broken)", "@ESPN", false, true, true).await;
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);
        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect("preview");
        assert_eq!(result.broken, 1);
        let row = &result.rows[0];
        assert_eq!(row.original_epg_name, "=ESPN (broken)");

        let report = result.report.as_ref().expect("report written");
        let text = std::fs::read_to_string(report).expect("read report");
        assert!(text.contains("'+ESPN HD"), "channel cell is inert: {text}");
        assert!(
            text.contains("'=ESPN (broken)"),
            "original epg cell is inert: {text}"
        );
        if let Some(new) = row.new_epg_name.as_deref() {
            assert!(
                text.contains(&crosstalk_core::csv_report::safe_cell(new)),
                "replacement cell is inert: {text}"
            );
            assert!(
                !text.lines().any(|line| line.contains(&format!(",{new}"))),
                "no raw formula cell survives: {text}"
            );
        }
    }

    #[tokio::test]
    async fn a_working_assignment_is_never_touched() {
        let (_server, world, context) = fixture(true, true).await;
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);
        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            &world,
            &channels,
            &entries,
            false,
        )
        .await
        .expect("preview");
        assert_eq!(result.broken, 0);
        assert!(result.rows.is_empty());
        assert_eq!(result.applied, 0);
    }

    #[tokio::test]
    async fn a_channel_with_no_working_replacement_is_left_alone() {
        let (_server, world, context) = fixture(false, false).await;
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);
        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            &world,
            &channels,
            &entries,
            false,
        )
        .await
        .expect("apply");
        assert_eq!(result.rows[0].status, HealStatus::NoReplacementFound);
        assert_eq!(result.rows[0].new_epg_id, None);
        assert_eq!(
            result.applied, 0,
            "an unprovable replacement is not written"
        );
        assert_eq!(result.rows[0].reason, "No working replacement EPG found");
    }

    #[tokio::test]
    async fn a_below_cut_replacement_never_enters_the_family() {
        // Step 8 (D02/T10 one cut + the unified selection rule + D06 retired): "KOMO
        // News" against "KOMO 4 News Seattle" scored a structural-only 50 under Janitor
        // and was recorded-but-skipped as SkippedLowConfidence. The unified engine scores
        // the pair below the cut, so it never enters the identity family and heal reports
        // NoReplacementFound instead of a recorded-but-unapplied replacement.
        let (_server, world, context) = fixture_with_names(
            "KOMO News",
            "KOMO (broken)",
            "KOMO 4 News Seattle",
            false,
            true,
            true,
        )
        .await;
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);
        // The ONE cut (D02/T10 `janitor-95`): `[matching] sensitivity` ships at strict.
        assert_eq!(context.config().matching.threshold(), 95);

        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            &world,
            &channels,
            &entries,
            false,
        )
        .await
        .expect("apply");
        assert_eq!(result.broken, 1);
        let row = &result.rows[0];
        assert_eq!(row.status, HealStatus::NoReplacementFound);
        assert_eq!(row.new_epg_id, None, "nothing below the cut is recorded");
        assert_eq!(result.applied, 0, "a below-cut replacement is not written");
        assert_eq!(result.replacements, 0, "it never counts as found");
        assert_eq!(row.reason, "No working replacement EPG found");

        // A preview finds nothing either: the family cut, not the dry run, decides.
        let preview = run(
            &GuideApp::new(),
            &context,
            &settings,
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect("preview");
        assert_eq!(preview.rows[0].status, HealStatus::NoReplacementFound);
        assert_eq!(preview.rows[0].new_epg_id, None);
        assert_eq!(preview.applied, 0);
        let recorded = outcome(&preview, true, context.config().matching.threshold());
        assert_eq!(recorded.counts.get("replacements"), Some(&0));
        assert_eq!(recorded.counts.get("applied"), Some(&0));
    }

    fn row(status: HealStatus, confidence: u8) -> HealRow {
        HealRow {
            channel_id: 1,
            channel_name: "ESPN".to_string(),
            channel_number: Some(101.0),
            channel_group: "Sports".to_string(),
            original_epg_id: 1,
            original_epg_name: "Old".to_string(),
            original_epg_source: "SD".to_string(),
            new_epg_id: if status == HealStatus::NoReplacementFound {
                None
            } else {
                Some(2)
            },
            new_epg_name: Some("New".to_string()),
            new_epg_source: Some("XMLTV".to_string()),
            confidence,
            method: "Callsign".to_string(),
            status,
            reason: String::new(),
        }
    }

    #[test]
    fn the_status_wording_matches_the_plugin() {
        assert_eq!(
            HealStatus::NoReplacementFound.as_str(),
            "NO_REPLACEMENT_FOUND"
        );
        assert_eq!(
            HealStatus::ReplacementPreview.as_str(),
            "REPLACEMENT_PREVIEW"
        );
        assert_eq!(HealStatus::Healed.as_str(), "HEALED");
        assert_eq!(
            HealStatus::SkippedLowConfidence.as_str(),
            "SKIPPED_LOW_CONFIDENCE"
        );
    }

    #[test]
    fn the_reason_reads_like_the_plugin() {
        let mut healed = row(HealStatus::Healed, 100);
        assert_eq!(
            reason_for(&healed, 95, false),
            "Excellent replacement match (100%) - Callsign"
        );
        healed.confidence = 88;
        assert_eq!(
            reason_for(&healed, 95, false),
            "High confidence replacement (88%) - Callsign"
        );

        let missing = row(HealStatus::NoReplacementFound, 0);
        assert_eq!(
            reason_for(&missing, 95, false),
            "No working replacement EPG found"
        );

        let low = row(HealStatus::SkippedLowConfidence, 70);
        assert_eq!(
            reason_for(&low, 95, false),
            "Replacement found but confidence 70% below threshold (95%) - Callsign"
        );

        let skipped = row(HealStatus::Healed, 100);
        assert!(reason_for(&skipped, 95, true).contains("[program data check skipped]"));
    }

    #[test]
    fn the_outcome_counts_the_statuses() {
        let result = HealOutcome {
            rows: vec![
                row(HealStatus::Healed, 100),
                row(HealStatus::NoReplacementFound, 0),
            ],
            report: None,
            scanned: 10,
            broken: 2,
            replacements: 1,
            applied: 1,
            unchecked: 0,
        };
        let recorded = outcome(&result, false, 95);
        assert!(recorded.summary.contains("scanned 10"));
        assert!(recorded.summary.contains("2 broken"));
        assert_eq!(recorded.counts.get("healed"), Some(&1));
        assert_eq!(recorded.counts.get("applied"), Some(&1));
    }

    #[test]
    fn a_preview_says_how_many_would_be_applied() {
        let result = HealOutcome {
            rows: vec![row(HealStatus::ReplacementPreview, 99)],
            report: None,
            scanned: 1,
            broken: 1,
            replacements: 1,
            applied: 0,
            unchecked: 0,
        };
        let recorded = outcome(&result, true, 95);
        assert!(recorded.summary.contains("1 would be applied"));
        assert_eq!(recorded.counts.get("applied"), Some(&0));
    }

    #[tokio::test]
    async fn an_assigned_guide_missing_from_the_entry_listing_is_never_healed() {
        // B1: a short entry read must not read as "this guide is gone". The assigned entry is
        // absent from the world, so the channel is unchecked and left alone.
        let (_server, mut world, context) = fixture(false, true).await;
        world.entries.retain(|entry| entry.id != 7);
        let (channels, entries) = scoped(&world);
        let result = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect("preview");
        assert_eq!(result.broken, 0);
        assert!(result.rows.is_empty());
        assert_eq!(result.unchecked, 1);
    }

    #[tokio::test]
    async fn an_empty_entry_listing_with_assigned_channels_refuses_heal() {
        let (_server, mut world, context) = fixture(false, true).await;
        world.entries.clear();
        let (channels, entries) = scoped(&world);
        let error = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect_err("refused");
        assert!(format!("{error:#}").contains("no EPG entries"), "{error:#}");
    }

    #[tokio::test]
    async fn a_failed_programme_check_leaves_the_assignment_alone() {
        // S1: a transient failure on the CURRENT guide is unknown, not "no programmes".
        let (server, world, context) = fixture(false, true).await;
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .and(query_param("tvg_id", "old.us"))
            .respond_with(ResponseTemplate::new(500))
            .with_priority(1)
            .mount(&server)
            .await;
        let (channels, entries) = scoped(&world);
        let result = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            &world,
            &channels,
            &entries,
            false,
        )
        .await
        .expect("apply");
        assert_eq!(result.broken, 0);
        assert_eq!(result.applied, 0);
        assert_eq!(result.unchecked, 1);
        let recorded = outcome(&result, false, 95);
        assert_eq!(recorded.counts.get("unchecked"), Some(&1));
    }

    #[tokio::test]
    async fn a_failed_replacement_check_stops_the_walk() {
        // S1: a failed check on the best replacement must not settle for the next one.
        let (server, world, context) = fixture(false, true).await;
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .and(query_param("tvg_id", "new.us"))
            .respond_with(ResponseTemplate::new(500))
            .with_priority(1)
            .mount(&server)
            .await;
        let (channels, entries) = scoped(&world);
        let result = run(
            &GuideApp::new(),
            &context,
            &GuideSettings::default(),
            &world,
            &channels,
            &entries,
            false,
        )
        .await
        .expect("apply");
        assert_eq!(result.broken, 1);
        assert_eq!(result.rows[0].new_epg_id, None);
        assert_eq!(result.applied, 0);
        assert!(
            result.rows[0].reason.contains("programme check"),
            "{}",
            result.rows[0].reason
        );
    }

    #[tokio::test]
    async fn a_cancel_before_validation_aborts_promptly() {
        // The cancel flag is set before the detection/replacement network phases:
        // the in-future checks plus the batch guards must abort the run.
        let (_server, world, context) = fixture(false, true).await;
        context.cancel();
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);
        let error = run(
            &GuideApp::new(),
            &context,
            &settings,
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect_err("a cancelled run aborts");
        assert!(format!("{error:#}").contains("cancelled"));
    }
}
