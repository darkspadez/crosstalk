//! Scan & heal: find assignments that carry no programme data, and replace them with one that does.
//!
//! Port of `Plugin._scan_and_heal_worker`. The rule that makes this safe to run unattended is
//! that heal only touches an assignment it can PROVE is empty: a channel whose current guide is
//! working is never moved, and a channel for which no replacement is found is left exactly as
//! it was rather than cleared.
//!
//! Heal never consults the AI gate — it is heuristic-only by design (owner Q4), so every
//! replacement above carries the matcher's own score and method, with no model in the loop.
//!
//! The order mirrors the plugin: find the broken channels, search replacements (excluding the
//! one already assigned), take the first candidate that clears the threshold AND has
//! programmes, then apply only those.

use crate::scope::{ScopedChannels, ScopedEntries};
use crate::settings::JanitorSettings;
use anyhow::{Context, Result};
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{EpgEntry, HasPrograms, ProgramWindow, World};
use crosstalk_core::jobs::{JobContext, ProgressEvent};
use crosstalk_core::store::RunRow;
use crosstalk_core::subapp::JobOutcome;
use crosstalk_matching::pipeline::{CandidateIndex, MatchOptions, Matcher, Scored};
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
}

/// Run scan & heal.
pub async fn run(
    context: &JobContext,
    settings: &JanitorSettings,
    world: &World,
    scoped_channels: &ScopedChannels,
    scoped_entries: &ScopedEntries,
    dry_run: bool,
) -> Result<HealOutcome> {
    let client = context.dispatcharr()?;
    let window = ProgramWindow::ahead(settings.check_hours);
    let threshold = settings.heal_confidence_threshold;
    let allow_without_programs = settings.allow_epg_without_programs;

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
        "janitor.heal",
        0,
        scoped_channels.channels.len() as u64,
        "checking existing assignments",
    ));

    enum Detection {
        Unassigned,
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
            None => plans.push(Detection::Decided(HasPrograms::No)),
        }
    }

    let validation_bound = context.config().dispatcharr.concurrency.max(1);
    let window_ref = &window;
    let mut detected: Vec<Option<HasPrograms>> = (0..plans.len()).map(|_| None).collect();
    {
        use futures::{stream, StreamExt};
        let resolved: Vec<(usize, HasPrograms)> =
            stream::iter((0..plans.len()).filter_map(|ordinal| {
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
                    // in-flight round instead of after the whole phase. `No` is
                    // the "no result" value; the cancel check below aborts.
                    if cancel.is_cancelled() {
                        return (ordinal, HasPrograms::No);
                    }
                    let has = client
                        .has_programs(&tvg_id, source, window_ref)
                        .await
                        .unwrap_or(HasPrograms::No);
                    (ordinal, has)
                })
            }))
            .buffer_unordered(validation_bound)
            .collect()
            .await;
        for (ordinal, has) in resolved {
            detected[ordinal] = Some(has);
        }
    }
    if context.is_cancelled() {
        anyhow::bail!("cancelled by the operator");
    }

    let mut broken = Vec::new();
    for (ordinal, (channel, plan)) in scoped_channels
        .channels
        .iter()
        .zip(plans.iter())
        .enumerate()
    {
        let has = match plan {
            Detection::Unassigned => continue,
            Detection::Decided(has) => *has,
            Detection::Check(..) => detected[ordinal].expect("every checked channel was resolved"),
        };
        if has == HasPrograms::No {
            broken.push(channel.clone());
        }
    }

    let scanned = scoped_channels.channels.len();

    // Step 2: search a replacement for each broken channel.
    let matcher = Arc::new(Matcher::new(
        context.config().matching.normalize_flags(),
        crate::matcher_aliases(context),
        Arc::new(crate::known_callsigns(context)),
        MatchOptions::default(),
    ));
    let names: Vec<String> = scoped_entries
        .entries
        .iter()
        .map(|entry| entry.name.clone())
        .collect();
    let index = Arc::new(CandidateIndex::build(
        &names,
        matcher.normalizer(),
        matcher.known(),
    ));
    let tags = crate::tag_list(context.config().matching.normalize_flags());

    // Step 2a: score every broken channel off the async runtime (P3), the same
    // spawn_blocking + rayon shape automatch uses, so a big heal never stalls a Tokio
    // worker. `par_iter` preserves order on collect: `scored_all[i]` is broken[i].
    let scored_all: Vec<Vec<Scored>> = {
        let matcher = Arc::clone(&matcher);
        let index = Arc::clone(&index);
        let broken_names: Vec<String> = broken.iter().map(|channel| channel.name.clone()).collect();
        tokio::task::spawn_blocking(move || {
            use rayon::prelude::*;
            let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
            broken_names
                .par_iter()
                .map(|name| matcher.score_candidates(name, &index, &tag_refs, None))
                .collect()
        })
        .await
        .context("the scoring stage panicked")?
    };

    // Step 2b: walk each broken channel's ranked list serially, concurrently across
    // channels (P1). Picks resolve by channel order below. Work runs in batches so
    // a cancel is honoured between them and progress is published per batch.
    // Shared borrows for the `async move` futures: capturing the vecs themselves would
    // move them on the first poll.
    let broken_ref = &broken;
    let scored_ref = &scored_all;
    let entries_ref = &scoped_entries.entries;
    let mut picks: Vec<Option<usize>> = (0..broken.len()).map(|_| None).collect();
    for batch_start in (0..broken_ref.len()).step_by(BATCH_CHANNELS) {
        if context.is_cancelled() {
            anyhow::bail!("cancelled by the operator");
        }
        let batch_end = (batch_start + BATCH_CHANNELS).min(broken_ref.len());
        {
            use futures::{stream, StreamExt};
            let resolved: Vec<(usize, Option<usize>)> =
                stream::iter((batch_start..batch_end).map(|ordinal| {
                    let cancel = context.clone();
                    async move {
                        let channel = &broken_ref[ordinal];
                        let original_id = channel.current_epg_id().unwrap_or(0);
                        let candidates = &scored_ref[ordinal];
                        // Exclude the entry already assigned: proposing the same broken guide is
                        // not a fix. Walk every ranked candidate and remember the first one that
                        // carries programme data, regardless of score (mirroring the plugin's
                        // `_find_best_epg_match`): the threshold decides Healed vs
                        // SkippedLowConfidence below, never whether the replacement is recorded.
                        let mut picked = None;
                        for (rank_position, candidate) in candidates.iter().enumerate() {
                            if cancel.is_cancelled() {
                                break;
                            }
                            let Some(entry) = entries_ref.get(candidate.index) else {
                                continue;
                            };
                            if entry.id == original_id {
                                continue;
                            }
                            if allow_without_programs {
                                picked = Some(rank_position);
                                break;
                            }
                            let Some(source) = entry.epg_source else {
                                continue;
                            };
                            if cancel.is_cancelled() {
                                break;
                            }
                            if client
                                .has_programs(&entry.tvg_id, source, window_ref)
                                .await
                                .unwrap_or(HasPrograms::No)
                                == HasPrograms::Yes
                            {
                                picked = Some(rank_position);
                                break;
                            }
                        }
                        (ordinal, picked)
                    }
                }))
                .buffer_unordered(validation_bound)
                .collect()
                .await;
            for (ordinal, picked) in resolved {
                picks[ordinal] = picked;
            }
        }
        context.progress(&ProgressEvent::new(
            "janitor.heal",
            batch_end as u64,
            broken_ref.len() as u64,
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

        // The concurrent walk above already chose the first ranked candidate carrying
        // programme data (same order as the old inline walk); this only maps it back.
        let candidates = &scored_all[position];
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
            // A preview records every replacement it found, whatever the score, exactly
            // as the plugin does: the threshold only decides what an APPLY writes.
            // Only an apply splits Healed (>= threshold) from SkippedLowConfidence.
            row.status = if dry_run {
                HealStatus::ReplacementPreview
            } else if candidate.score >= threshold {
                HealStatus::Healed
            } else {
                // Recorded with its real score, but never applied: only Healed rows are
                // written in step 3 below.
                HealStatus::SkippedLowConfidence
            };
        }
        row.reason = reason_for(&row, threshold, allow_without_programs);
        rows.push(row);

        if position % 50 == 0 {
            context.progress(&ProgressEvent::new(
                "janitor.heal",
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
    // and the error below names it (mirroring automatch, which also reports before it
    // writes). The run record itself cannot link it: the runner drops the report path on
    // a failed run.
    let report = write_report(context, settings, scanned, &rows, dry_run)?;

    // Step 4: write the replacements that clear the threshold.
    let mut applied = 0;
    if !dry_run {
        let associations: Vec<(i64, i64)> = rows
            .iter()
            .filter(|row| row.status == HealStatus::Healed)
            .filter_map(|row| row.new_epg_id.map(|epg| (row.channel_id, epg)))
            .collect();
        if !associations.is_empty() {
            let plan = report
                .as_deref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "(no report)".to_string());
            applied = client.batch_set_epg(&associations).await.with_context(|| {
                format!("applying the EPG replacements; the plan was recorded in {plan}")
            })?;
        }
    }

    // Tidy up only after a successful run: deleting older reports before an apply that then
    // fails would throw away the history while producing nothing to replace it.
    if let Some(path) = report.as_deref() {
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string());
        csv_report::prune_exports(
            &context.exports_dir(),
            settings.csv_retention_days,
            name.as_deref(),
        );
    }

    Ok(HealOutcome {
        rows,
        report,
        scanned,
        broken: broken.len(),
        replacements,
        applied,
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
            if confidence >= 95 {
                format!("Excellent replacement match ({confidence}%)")
            } else if confidence >= 85 {
                format!("High confidence replacement ({confidence}%)")
            } else if confidence >= 75 {
                format!("Good replacement match ({confidence}%)")
            } else if confidence >= 60 {
                format!("Moderate confidence replacement ({confidence}%)")
            } else {
                format!("Low confidence replacement ({confidence}%)")
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
    context: &JobContext,
    settings: &JanitorSettings,
    scanned: usize,
    rows: &[HealRow],
    dry_run: bool,
) -> Result<Option<PathBuf>> {
    let path = csv_report::report_path(
        &context.exports_dir(),
        if dry_run {
            "heal_preview"
        } else {
            "heal_applied"
        },
    );

    let replacements = rows.iter().filter(|row| row.new_epg_id.is_some()).count();
    let applied = rows
        .iter()
        .filter(|row| row.status == HealStatus::Healed)
        .count();

    let preamble = csv_report::preamble(
        context.config(),
        &crate::settings::schema(),
        &crate::settings::to_values(settings),
        if dry_run {
            "Scan and Heal Preview Report"
        } else {
            "Scan and Heal Applied Report"
        },
        scanned,
        &[
            ("Broken assignments examined", rows.len().to_string()),
            ("Replacements found", replacements.to_string()),
            (
                "Replacements written",
                if dry_run {
                    "none, this was a preview".to_string()
                } else {
                    applied.to_string()
                },
            ),
        ],
    );

    csv_report::write_report(&path, &preamble, &COLUMNS, |writer| {
        for row in rows {
            writer.write_record([
                row.channel_id.to_string(),
                row.channel_name.clone(),
                row.channel_number
                    .map(|number| format!("{number}"))
                    .unwrap_or_default(),
                row.channel_group.clone(),
                row.original_epg_name.clone(),
                row.original_epg_source.clone(),
                row.new_epg_name
                    .clone()
                    .unwrap_or_else(|| "N/A".to_string()),
                row.new_epg_source
                    .clone()
                    .unwrap_or_else(|| "N/A".to_string()),
                row.confidence.to_string(),
                row.method.clone(),
                row.status.as_str().to_string(),
                row.reason.clone(),
            ])?;
        }
        Ok(())
    })?;
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

    let mut summary = format!(
        "Scan & Heal {}: scanned {}, {} broken, {} replacement(s) found",
        if dry_run { "preview" } else { "applied" },
        result.scanned,
        result.broken,
        result.replacements
    );
    if low_confidence > 0 {
        summary.push_str(&format!(", {low_confidence} below threshold"));
    }
    if dry_run {
        let would_apply = result
            .rows
            .iter()
            .filter(|row| row.new_epg_id.is_some() && row.confidence >= threshold)
            .count();
        summary.push_str(&format!(", {would_apply} would be applied"));
    } else {
        summary.push_str(&format!(
            ", {high_confidence} healed, {} assignments written",
            result.applied
        ));
    }

    let mut outcome = JobOutcome::new(summary)
        .with_count("scanned", result.scanned as i64)
        .with_count("broken", result.broken as i64)
        .with_count("replacements", result.replacements as i64)
        .with_count("low_confidence", low_confidence as i64)
        .with_count("healed", high_confidence as i64)
        .with_count("applied", result.applied as i64);
    if let Some(report) = result.report.clone() {
        outcome = outcome.with_report(report);
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
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
                {"id": 1, "name": channel_name, "channel_number": 101, "channel_group_id": 10, "epg_data_id": 7}
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
            }],
            entries: vec![
                EpgEntry {
                    id: 7,
                    name: old_name.to_string(),
                    tvg_id: "old.us".to_string(),
                    epg_source: Some(4),
                },
                EpgEntry {
                    id: 8,
                    name: new_name.to_string(),
                    tvg_id: "new.us".to_string(),
                    epg_source: Some(4),
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
        let settings = JanitorSettings::default();
        (
            crate::scope::scope_channels(world, &settings).expect("channels"),
            crate::scope::scope_entries(world, ""),
        )
    }

    #[tokio::test]
    async fn a_broken_assignment_gets_a_working_replacement() {
        let (_server, world, context) = fixture(false, true).await;
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(&context, &settings, &world, &channels, &entries, true)
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
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);
        let result = run(&context, &settings, &world, &channels, &entries, false)
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
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);
        let error = run(&context, &settings, &world, &channels, &entries, false)
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
    async fn a_working_assignment_is_never_touched() {
        let (_server, world, context) = fixture(true, true).await;
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);
        let result = run(&context, &settings, &world, &channels, &entries, false)
            .await
            .expect("preview");
        assert_eq!(result.broken, 0);
        assert!(result.rows.is_empty());
        assert_eq!(result.applied, 0);
    }

    #[tokio::test]
    async fn a_channel_with_no_working_replacement_is_left_alone() {
        let (_server, world, context) = fixture(false, false).await;
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);
        let result = run(&context, &settings, &world, &channels, &entries, false)
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
    async fn a_sub_threshold_replacement_is_recorded_but_never_applied() {
        // "KOMO News" against "KOMO 4 News Seattle" scores a structural-only 50: a
        // working replacement that clears nothing near the default 95 threshold.
        let (_server, world, context) = fixture_with_names(
            "KOMO News",
            "KOMO (broken)",
            "KOMO 4 News Seattle",
            false,
            true,
            true,
        )
        .await;
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);
        assert!(settings.heal_confidence_threshold > 50);

        let result = run(&context, &settings, &world, &channels, &entries, false)
            .await
            .expect("apply");
        assert_eq!(result.broken, 1);
        let row = &result.rows[0];
        assert_eq!(row.status, HealStatus::SkippedLowConfidence);
        assert_eq!(row.new_epg_id, Some(8), "the replacement is recorded");
        assert_eq!(row.new_epg_name.as_deref(), Some("KOMO 4 News Seattle"));
        assert!(
            row.confidence < settings.heal_confidence_threshold && row.confidence > 0,
            "the real sub-threshold score is kept, got {}",
            row.confidence
        );
        assert_eq!(
            result.applied, 0,
            "a low-confidence replacement is not written"
        );
        assert_eq!(result.replacements, 1, "it still counts as found");
        assert!(row.reason.contains("below threshold"), "{}", row.reason);

        // A preview records the replacement too, whatever the score: only an apply
        // splits Healed from SkippedLowConfidence.
        let preview = run(&context, &settings, &world, &channels, &entries, true)
            .await
            .expect("preview");
        assert_eq!(preview.rows[0].status, HealStatus::ReplacementPreview);
        assert_eq!(preview.rows[0].new_epg_id, Some(8));
        assert_eq!(preview.applied, 0);
        let recorded = outcome(&preview, true, settings.heal_confidence_threshold);
        assert_eq!(recorded.counts.get("replacements"), Some(&1));
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
        };
        let recorded = outcome(&result, true, 95);
        assert!(recorded.summary.contains("1 would be applied"));
        assert_eq!(recorded.counts.get("applied"), Some(&0));
    }

    #[tokio::test]
    async fn a_cancel_before_validation_aborts_promptly() {
        // The cancel flag is set before the detection/replacement network phases:
        // the in-future checks plus the batch guards must abort the run.
        let (_server, world, context) = fixture(false, true).await;
        context.cancel();
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);
        let error = run(&context, &settings, &world, &channels, &entries, true)
            .await
            .expect_err("a cancelled run aborts");
        assert!(format!("{error:#}").contains("cancelled"));
    }
}
