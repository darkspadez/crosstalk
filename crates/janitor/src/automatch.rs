//! Auto-match: score every in-scope channel, validate the winner, and (when applying) write it.
//!
//! Port of `Plugin._auto_match_channels` plus the scoring half of `_find_best_epg_match`. The
//! pipeline is split in two stages because they have different bottlenecks:
//!
//!   1. **Scoring** is pure CPU over strings: every channel against every candidate. It runs on
//!      a rayon pool, so the work scales with cores.
//!   2. **Validation** is IO: does this candidate actually carry programmes? It runs only for
//!      the head of each channel's ranked list, through the client's own concurrency cap, and
//!      the answers are memoised, so a thousand channels considering the same ESPN entry cost
//!      one request.
//!
//! Both stages stay faithful to the plugin's order: the highest-scoring candidate that both
//! clears the threshold and has programmes is the one that wins.

use crate::ai_gate;
use crate::scope::{ScopedChannels, ScopedEntries};
use crate::settings::JanitorSettings;
use anyhow::{Context, Result};
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{EpgEntry, HasPrograms, ProgramWindow, World};
use crosstalk_core::jobs::{JobContext, ProgressEvent};
use crosstalk_core::store::RunRow;
use crosstalk_core::subapp::JobOutcome;
use crosstalk_matching::pipeline::{CandidateIndex, MatchOptions, Matcher, Scored};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The columns the auto-match report carries.
pub const COLUMNS: [&str; 15] = [
    "channel_id",
    "channel_name",
    "channel_number",
    "channel_group",
    "match_method",
    "confidence_score",
    "extracted_callsign",
    "epg_source_name",
    "epg_data_id",
    "epg_channel_name",
    "current_epg_id",
    "current_epg_name",
    "has_program_data",
    "reason",
    "ai_decision",
];

/// One channel's result, before and after validation.
#[derive(Debug, Clone, Default)]
pub struct MatchRow {
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_number: Option<f64>,
    pub channel_group: String,
    pub method: String,
    pub score: u8,
    pub callsign: Option<String>,
    pub epg_source_name: Option<String>,
    pub epg_data_id: Option<i64>,
    pub epg_channel_name: Option<String>,
    pub current_epg_id: Option<i64>,
    pub current_epg_name: Option<String>,
    /// Whether the match cleared the threshold AND had programme data.
    pub has_program_data: bool,
    pub reason: String,
    pub ai_decision: Option<String>,
    /// Set when the channel was skipped for a reason worth reporting.
    pub skipped: Option<String>,
}

impl MatchRow {
    /// Convert to the store's row shape.
    pub fn to_run_row(&self, ordinal: i64) -> RunRow {
        RunRow {
            ordinal,
            channel_id: self.channel_id,
            channel_name: self.channel_name.clone(),
            channel_group: self.channel_group.clone(),
            channel_number: self.channel_number,
            current_epg_name: self.current_epg_name.clone(),
            epg_data_id: self.epg_data_id,
            epg_name: self.epg_channel_name.clone(),
            epg_source: self.epg_source_name.clone(),
            score: i64::from(self.score),
            method: self.method.clone(),
            status: if self.has_program_data {
                "MATCHED".to_string()
            } else {
                "NO_MATCH".to_string()
            },
            reason: self.reason.clone(),
            ai_decision: self.ai_decision.clone(),
        }
    }
}

/// What an auto-match run produced.
#[derive(Debug, Clone, Default)]
pub struct AutoMatchOutcome {
    pub rows: Vec<MatchRow>,
    pub report: Option<PathBuf>,
    /// How many assignments the API confirmed, after an apply.
    pub applied: usize,
    pub candidates: usize,
    pub channels: usize,
}

/// Run auto-match.
///
/// `dry_run` writes the report and changes nothing; otherwise the matches are written in one
/// batch, which is also what makes Dispatcharr queue the programme parse for each changed
/// entry.
pub async fn run(
    context: &JobContext,
    settings: &JanitorSettings,
    ai: Option<&crosstalk_core::ai::AiService>,
    world: &World,
    scoped_channels: &ScopedChannels,
    scoped_entries: &ScopedEntries,
    dry_run: bool,
) -> Result<AutoMatchOutcome> {
    let client = context.dispatcharr()?;
    let config = context.config();

    // The candidate side: names, normalised once, shared with every thread.
    let names: Vec<String> = scoped_entries
        .entries
        .iter()
        .map(|entry| entry.name.clone())
        .collect();
    let matcher = Matcher::new(
        config.matching.normalize_flags(),
        crate::matcher_aliases(context),
        Arc::new(crate::known_callsigns(context)),
        MatchOptions::default(),
    );
    let index = Arc::new(CandidateIndex::build(
        &names,
        matcher.normalizer(),
        matcher.known(),
    ));
    let matcher = Arc::new(matcher);

    let total = scoped_channels.channels.len() as u64;
    context.progress(&ProgressEvent::new(
        "janitor.automatch",
        0,
        total,
        "scoring channels",
    ));

    // Stage 1: scoring, in parallel.
    let scored: Vec<(usize, Vec<Scored>)> = {
        let matcher = Arc::clone(&matcher);
        let index = Arc::clone(&index);
        let channels = scoped_channels.channels.clone();
        let tags = crate::tag_list(config.matching.normalize_flags());

        tokio::task::spawn_blocking(move || {
            use rayon::prelude::*;
            let counter = AtomicUsize::new(0);
            let results: Vec<(usize, Vec<Scored>)> = channels
                .par_iter()
                .enumerate()
                .map(|(position, channel)| {
                    let ranked =
                        matcher.score_candidates(&channel.name, &index, &tags_refs(&tags), None);
                    counter.fetch_add(1, Ordering::Relaxed);
                    (position, ranked)
                })
                .collect();
            results
        })
        .await
        .context("the scoring stage panicked")?
    };

    context.progress(&ProgressEvent::new(
        "janitor.automatch",
        total,
        total,
        "validating candidates",
    ));

    // Stage 2: validation, in score order, memoised by the client.
    let window = ProgramWindow::ahead(settings.check_hours);
    let threshold = settings.automatch_confidence_threshold;
    let allow_without_programs = settings.allow_epg_without_programs;
    let source_names: HashMap<i64, String> = scoped_entries
        .sources
        .iter()
        .map(|(id, source)| (*id, source.name.clone()))
        .collect();

    let mut rows = Vec::with_capacity(scoped_channels.channels.len());
    for (position, ranked) in scored {
        let channel = &scoped_channels.channels[position];
        let mut row = MatchRow {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_number: channel.display_number(),
            channel_group: String::new(),
            current_epg_id: channel.current_epg_id(),
            current_epg_name: None,
            callsign: crosstalk_matching::callsign::extract_callsign(
                &channel.name,
                matcher.known(),
            ),
            ..MatchRow::default()
        };

        // An EPG override pins the guide somewhere the column cannot show, so writing the
        // column would change nothing the operator can see. Report it instead.
        if channel.has_epg_override() {
            row.reason = "Channel has an EPG override in Dispatcharr; skipped so the override \
                          is not silently bypassed"
                .to_string();
            row.skipped = Some("override".to_string());
            rows.push(row);
            continue;
        }

        let mut chosen: Option<(&Scored, &EpgEntry, HasPrograms)> = None;
        let mut best_rejected: Option<u8> = None;

        // A high-confidence heuristic match is the fast path: validate it and move on. Only a
        // channel the heuristics are unsure about reaches the model.
        // A heuristic result that already clears the threshold is a confident match and is
        // never re-checked by the model.
        let already_confident = ranked.first().is_some_and(|entry| entry.score >= threshold);
        let gate = if already_confident {
            None
        } else {
            ai_gate::shortlist(&ranked, &settings.ai)
        };

        let mut ai_outcome = None;
        if let Some((reason, offered)) = gate {
            let outcome = ai_gate::ask(
                ai,
                channel,
                &scoped_entries.entries,
                &ranked,
                &offered,
                reason,
                &settings.ai,
            )
            .await;
            // Recorded whether or not it answered: a failed request is part of the story.
            row.ai_decision = Some(outcome.decision.clone());
            context
                .record_ai_decision(&outcome.to_store(rows.len() as i64, &settings.ai.provider))?;
            if outcome.answered {
                let decision_index = outcome.accepted;
                ai_outcome = Some((outcome, decision_index));
            }
        }

        // An accepted AI candidate replaces the heuristic search order: the model's pick is
        // the one to validate, and if it has no programmes the channel is left alone.
        if let Some((_outcome, Some(position))) = ai_outcome.clone() {
            if let Some(candidate) = ranked.get(position) {
                if let Some(entry) = scoped_entries.entries.get(candidate.index) {
                    let has = if allow_without_programs {
                        HasPrograms::Yes
                    } else {
                        match entry.epg_source {
                            Some(source) => client
                                .has_programs(&entry.tvg_id, source, &window)
                                .await
                                .unwrap_or(HasPrograms::No),
                            None => HasPrograms::No,
                        }
                    };
                    if has == HasPrograms::Yes {
                        chosen = Some((candidate, entry, has));
                    } else {
                        // The model said yes and the guide is empty: say that, rather than
                        // reusing the "below threshold" wording the score would suggest.
                        best_rejected = Some(candidate.score);
                        row.reason = format!(
                            "The AI accepted {} (score {}%) but it carries no programme data \
                             in the checked window",
                            entry.name, candidate.score
                        );
                    }
                }
            }
            if let Some(outcome) = ai_outcome.as_ref() {
                context.record_ai_decision(
                    &outcome.0.to_store(rows.len() as i64, &settings.ai.provider),
                )?;
            }
        } else {
            for candidate in &ranked {
                let Some(entry) = scoped_entries.entries.get(candidate.index) else {
                    continue;
                };
                if candidate.score >= threshold {
                    if allow_without_programs {
                        chosen = Some((candidate, entry, HasPrograms::Yes));
                        break;
                    }
                    let Some(source) = entry.epg_source else {
                        continue;
                    };
                    let has = client
                        .has_programs(&entry.tvg_id, source, &window)
                        .await
                        .unwrap_or(HasPrograms::No);
                    match has {
                        HasPrograms::Yes => {
                            chosen = Some((candidate, entry, has));
                            break;
                        }
                        HasPrograms::No => {
                            best_rejected = Some(candidate.score);
                        }
                    }
                }
            }
            if let Some(outcome) = ai_outcome.as_ref() {
                context.record_ai_decision(
                    &outcome.0.to_store(rows.len() as i64, &settings.ai.provider),
                )?;
            }
        }

        match chosen {
            Some((candidate, entry, _)) => {
                row.score = candidate.score;
                row.method = candidate.method_label();
                if ai_outcome.as_ref().is_some_and(|(outcome, accepted)| {
                    accepted.is_some() && outcome.accepted.is_some()
                }) {
                    row.method = format!("{} + AI", row.method);
                }
                row.epg_data_id = Some(entry.id);
                row.epg_channel_name = Some(entry.name.clone());
                row.epg_source_name = entry
                    .epg_source
                    .and_then(|id| source_names.get(&id).cloned());
                row.has_program_data = true;
            }
            None => {
                row.score = ranked.first().map(|entry| entry.score).unwrap_or(0);
                row.method = ranked
                    .first()
                    .map(Scored::method_label)
                    .unwrap_or_else(|| "None".to_string());
            }
        }

        row.reason = reason_for(
            &row,
            threshold,
            best_rejected,
            allow_without_programs,
            ranked.first().map(|entry| entry.score),
        );
        rows.push(row);

        if rows.len() % 200 == 0 {
            context.progress(&ProgressEvent::new(
                "janitor.automatch",
                rows.len() as u64,
                total,
                "validating candidates",
            ));
        }
        if context.is_cancelled() {
            anyhow::bail!("cancelled by the operator");
        }
    }

    attach_group_names(&mut rows, world);
    attach_current_epg_names(&mut rows, world);

    // Persist the rows as they are, so a crash still leaves an audit trail.
    let run_rows: Vec<RunRow> = rows
        .iter()
        .enumerate()
        .map(|(ordinal, row)| row.to_run_row(ordinal as i64))
        .collect();
    context.record_rows(&run_rows)?;

    let matched: Vec<&MatchRow> = rows
        .iter()
        .filter(|row| row.epg_data_id.is_some())
        .collect();

    let report = write_report(context, settings, scoped_channels, &rows, &matched, dry_run)?;

    let mut applied = 0;
    if !dry_run {
        let associations: Vec<(i64, i64)> = matched
            .iter()
            .filter_map(|row| row.epg_data_id.map(|epg| (row.channel_id, epg)))
            .collect();
        if !associations.is_empty() {
            applied = client
                .batch_set_epg(&associations)
                .await
                .context("applying the EPG assignments")?;
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

    Ok(AutoMatchOutcome {
        rows,
        report,
        applied,
        candidates: scoped_entries.entries.len(),
        channels: scoped_channels.channels.len(),
    })
}

fn tags_refs(tags: &[String]) -> Vec<&str> {
    tags.iter().map(String::as_str).collect()
}

/// The reason text, in the plugin's wording.
fn reason_for(
    row: &MatchRow,
    threshold: u8,
    best_rejected: Option<u8>,
    allow_without_programs: bool,
    best_score: Option<u8>,
) -> String {
    if row.skipped.is_some() {
        // A skipped channel already carries the explanation; do not overwrite it with a
        // generic "no match".
        return row.reason.clone();
    }
    if row.epg_data_id.is_some() {
        let score = row.score;
        let mut reason = if score >= 95 {
            format!("Excellent match ({score}%)")
        } else if score >= 85 {
            format!("High confidence match ({score}%)")
        } else if score >= 75 {
            format!("Good match ({score}%)")
        } else if score >= 60 {
            format!("Moderate confidence match ({score}%)")
        } else {
            format!("Low confidence match ({score}%)")
        };
        if !row.method.is_empty() && row.method != "None" {
            reason.push_str(&format!(" - {}", row.method));
        }
        if allow_without_programs {
            reason.push_str(" [program data check skipped]");
        }
        return reason;
    }

    // Nothing was applied. Say why, because "no match" and "matched but the guide is empty"
    // call for very different operator action.
    match (best_score, best_rejected) {
        (Some(score), _) if score < threshold => {
            format!("Match found but confidence score {score}% below threshold ({threshold}%)")
        }
        (Some(score), Some(rejected)) if rejected >= threshold => {
            format!("Best match ({score}%) carries no programme data in the checked window")
        }
        (Some(score), None) => {
            format!("Match found but confidence score {score}% below threshold ({threshold}%)")
        }
        _ => "No matching EPG found".to_string(),
    }
}

fn write_report(
    context: &JobContext,
    settings: &JanitorSettings,
    scoped_channels: &ScopedChannels,
    rows: &[MatchRow],
    matched: &[&MatchRow],
    dry_run: bool,
) -> Result<Option<PathBuf>> {
    let exports_dir = context.exports_dir();
    let stem = if dry_run {
        "automatch_preview"
    } else {
        "automatch_applied"
    };
    let path = csv_report::report_path(&exports_dir, stem);

    let preamble = csv_report::preamble(
        context.config(),
        &crate::settings::schema(),
        &crate::settings::to_values(settings),
        if dry_run {
            "Auto-Match Preview Report"
        } else {
            "Auto-Match Applied Report"
        },
        scoped_channels.channels.len(),
        &[
            ("Channels with a match found", matched.len().to_string()),
            ("Matches carrying program data", matched.len().to_string()),
            (
                "Callsigns extracted",
                rows.iter()
                    .filter(|row| row.callsign.is_some())
                    .count()
                    .to_string(),
            ),
            (
                "Assignments this run applies",
                if dry_run {
                    "none, this was a preview".to_string()
                } else {
                    matched.len().to_string()
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
                row.method.clone(),
                row.score.to_string(),
                row.callsign.clone().unwrap_or_else(|| "N/A".to_string()),
                row.epg_source_name.clone().unwrap_or_default(),
                row.epg_data_id.map(|id| id.to_string()).unwrap_or_default(),
                row.epg_channel_name.clone().unwrap_or_default(),
                row.current_epg_id
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                row.current_epg_name.clone().unwrap_or_default(),
                if row.has_program_data { "Yes" } else { "No" }.to_string(),
                row.reason.clone(),
                row.ai_decision.clone().unwrap_or_default(),
            ])?;
        }
        Ok(())
    })?;
    Ok(Some(path))
}

/// The outcome the runner records.
pub fn outcome(result: &AutoMatchOutcome, dry_run: bool) -> JobOutcome {
    let matched = result
        .rows
        .iter()
        .filter(|row| row.epg_data_id.is_some())
        .count();
    let skipped = result
        .rows
        .iter()
        .filter(|row| row.skipped.is_some())
        .count();

    let mut summary = format!(
        "Auto-match {}: {}/{} matched",
        if dry_run { "preview" } else { "applied" },
        matched,
        result.channels
    );
    if skipped > 0 {
        summary.push_str(&format!(", {skipped} skipped"));
    }
    if !dry_run {
        summary.push_str(&format!(", {} assignments written", result.applied));
    }

    let mut outcome = JobOutcome::new(summary)
        .with_count("channels", result.channels as i64)
        .with_count("candidates", result.candidates as i64)
        .with_count("matched", matched as i64)
        .with_count("skipped", skipped as i64)
        .with_count("applied", result.applied as i64);
    if let Some(report) = result.report.clone() {
        outcome = outcome.with_report(report);
    }
    outcome
}

/// Fill in the group name for each row, using the world the run loaded.
pub fn attach_group_names(rows: &mut [MatchRow], world: &World) {
    let groups: HashMap<i64, String> = world
        .groups
        .iter()
        .map(|group| (group.id, group.name.clone()))
        .collect();
    let by_channel: HashMap<i64, Option<i64>> = world
        .channels
        .iter()
        .map(|channel| (channel.id, channel.channel_group_id))
        .collect();

    for row in rows {
        row.channel_group = by_channel
            .get(&row.channel_id)
            .and_then(|group| *group)
            .and_then(|id| groups.get(&id).cloned())
            .unwrap_or_else(|| "No Group".to_string());
    }
}

/// Fill in each row's current EPG name, from the world the run loaded.
pub fn attach_current_epg_names(rows: &mut [MatchRow], world: &World) {
    let entries: HashMap<i64, String> = world
        .entries
        .iter()
        .map(|entry| (entry.id, entry.name.clone()))
        .collect();
    let by_channel: HashMap<i64, Option<i64>> = world
        .channels
        .iter()
        .map(|channel| (channel.id, channel.current_epg_id()))
        .collect();

    for row in rows {
        row.current_epg_name = by_channel
            .get(&row.channel_id)
            .and_then(|entry| *entry)
            .and_then(|id| entries.get(&id).cloned());
    }
}

/// The channel ids that would be written by an apply.
pub fn associations(rows: &[MatchRow]) -> Vec<(i64, i64)> {
    let mut seen: HashSet<i64> = HashSet::new();
    rows.iter()
        .filter(|row| row.epg_data_id.is_some())
        .filter(|row| seen.insert(row.channel_id))
        .filter_map(|row| row.epg_data_id.map(|epg| (row.channel_id, epg)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::dispatcharr::{Channel, ChannelGroup, DispatcharrClient, EpgSource};
    use crosstalk_core::jobs::VecSink;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// A world with two channels and two guide entries, served over HTTP so the run goes
    /// through the real client path.
    async fn fixture(programs_for: &str) -> (MockServer, World, JobContext) {
        fixture_named(programs_for, "ESPN").await
    }

    /// Same fixture, with the guide entry named so a test can land in the verify band: `ESPN`
    /// scores 100 against `ESPN HD`, while `ESPNX` scores 80.
    async fn fixture_named(
        programs_for: &str,
        entry_name: &str,
    ) -> (MockServer, World, JobContext) {
        fixture_full(programs_for, entry_name, "ESPN HD").await
    }

    /// The same, with the channel name chosen too.
    async fn fixture_full(
        programs_for: &str,
        entry_name: &str,
        channel_name: &str,
    ) -> (MockServer, World, JobContext) {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/channels/channels/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": 1, "name": channel_name, "channel_number": 101, "channel_group_id": 10},
                {"id": 2, "name": "Nothing Like It", "channel_number": 102, "channel_group_id": 10}
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
                {"id": 7, "name": entry_name, "tvg_id": "espn.us", "epg_source": 4}
            ])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .and(query_param("tvg_id", programs_for))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": 1,
                "results": [{"id": 42}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": 0,
                "results": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/channels/channels/batch-set-epg/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true,
                "channels_updated": 1,
                "programs_refreshed": 1
            })))
            .mount(&server)
            .await;

        let world = World {
            channels: vec![
                Channel {
                    id: 1,
                    name: channel_name.to_string(),
                    channel_number: Some(serde_json::json!(101)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 2,
                    name: "Nothing Like It".to_string(),
                    channel_number: Some(serde_json::json!(102)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
            ],
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
            entries: vec![EpgEntry {
                id: 7,
                name: entry_name.to_string(),
                tvg_id: "espn.us".to_string(),
                epg_source: Some(4),
            }],
            ..World::default()
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = crosstalk_core::CrosstalkConfig::load(dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        let exports = dir.path().join("exports");
        config.jobs.exports_dir = exports.display().to_string();
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
    async fn a_preview_matches_and_writes_a_report_without_writing_epg() {
        // The fixture's batch-set-epg mock would answer, but a preview must never call it:
        // wiremock would report an unmatched request only for a path with no mock, so the
        // guard is the `applied` count.
        let (_server, world, context) = fixture("espn.us").await;
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(&context, &settings, None, &world, &channels, &entries, true)
            .await
            .expect("preview");

        assert_eq!(result.channels, 2);
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ESPN HD")
            .expect("row");
        assert_eq!(espn.epg_data_id, Some(7));
        assert!(espn.score >= 85, "score {}", espn.score);
        assert!(espn.has_program_data);
        assert!(espn.reason.contains("match"));
        assert_eq!(espn.channel_group, "Sports");

        let nothing = result
            .rows
            .iter()
            .find(|row| row.channel_name == "Nothing Like It")
            .expect("row");
        assert_eq!(nothing.epg_data_id, None);
        assert_eq!(nothing.reason, "No matching EPG found");

        assert_eq!(result.applied, 0);
        let report = result.report.expect("report written");
        let text = std::fs::read_to_string(&report).expect("read report");
        assert!(text.contains("# EPG Janitor"));
        assert!(text.contains("channel_name,channel_number"));
        assert!(text.contains("ESPN HD"));
    }

    #[tokio::test]
    async fn an_apply_writes_the_assignment() {
        let (_server, world, context) = fixture("espn.us").await;
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &context, &settings, None, &world, &channels, &entries, false,
        )
        .await
        .expect("apply");
        assert_eq!(result.applied, 1);
    }

    #[tokio::test]
    async fn a_candidate_without_programmes_is_rejected() {
        // The only entry has no programmes, so nothing may be applied even though it scores.
        let (_server, world, context) = fixture("some-other-tvg").await;
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(&context, &settings, None, &world, &channels, &entries, true)
            .await
            .expect("preview");
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ESPN HD")
            .expect("row");
        assert_eq!(espn.epg_data_id, None);
        assert!(espn.reason.contains("no programme data"), "{}", espn.reason);
    }

    #[tokio::test]
    async fn allow_without_programs_accepts_an_empty_guide() {
        let (_server, world, context) = fixture("some-other-tvg").await;
        let settings = JanitorSettings {
            allow_epg_without_programs: true,
            ..JanitorSettings::default()
        };
        let (channels, entries) = scoped(&world);
        let result = run(&context, &settings, None, &world, &channels, &entries, true)
            .await
            .expect("preview");
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ESPN HD")
            .expect("row");
        assert_eq!(espn.epg_data_id, Some(7));
        assert!(espn.reason.contains("[program data check skipped]"));
    }

    /// A decider that answers every request with a fixed probability, and counts calls.
    struct FixedDecider {
        probability: f64,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crosstalk_core::ai::Decider for FixedDecider {
        fn provider(&self) -> &str {
            "typesafe"
        }

        fn calibrated(&self) -> bool {
            true
        }

        async fn decide(
            &self,
            request: &crosstalk_core::ai::DecisionRequest,
        ) -> Result<crosstalk_core::ai::DecisionResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut answers = std::collections::BTreeMap::new();
            // Pick the first candidate offered, and answer yes to it.
            if let Some((id, _)) = request
                .questions
                .iter()
                .find(|(id, _)| id.starts_with("same_") && id.as_str() != "same_network")
            {
                answers.insert(
                    "best".to_string(),
                    crosstalk_core::ai::Answer::Choice {
                        choice: id.replacen("same_", "candidate_", 1),
                        confidence: 0.9,
                        probabilities: Default::default(),
                    },
                );
                answers.insert(
                    id.clone(),
                    crosstalk_core::ai::Answer::Noul {
                        noul: self.probability,
                    },
                );
            }
            Ok(crosstalk_core::ai::DecisionResponse {
                model: "stub".to_string(),
                answers,
                input_tokens: 10,
                output_tokens: 2,
                latency_ms: 1,
                calibrated: true,
            })
        }
    }

    fn ai_settings(
        probability: f64,
        calls: Arc<AtomicUsize>,
    ) -> (crosstalk_core::ai::AiService, JanitorSettings) {
        let service = crosstalk_core::ai::AiService::with_decider(
            "typesafe",
            Arc::new(FixedDecider { probability, calls }),
        );
        let settings = JanitorSettings {
            ai: crate::settings::AiSettings {
                enabled: true,
                provider: "typesafe".to_string(),
                model: "stub".to_string(),
                ..crate::settings::AiSettings::default()
            },
            ..JanitorSettings::default()
        };
        (service, settings)
    }

    #[tokio::test]
    async fn a_confident_match_never_calls_the_model() {
        let (_server, world, context) = fixture("espn.us").await;
        let calls = Arc::new(AtomicUsize::new(0));
        let (service, settings) = ai_settings(0.99, Arc::clone(&calls));
        let (channels, entries) = scoped(&world);

        let result = run(
            &context,
            &settings,
            Some(&service),
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect("preview");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "the heuristics were sure");
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ESPN HD")
            .expect("row");
        assert_eq!(espn.epg_data_id, Some(7));
        assert!(espn.ai_decision.is_none());
    }

    #[tokio::test]
    async fn a_borderline_match_is_verified_by_the_model() {
        // `ESPNX` scores 80 against `ESPN HD`: inside the verify band and below the threshold,
        // which is exactly the case the model exists for.
        // A shared but LOW-confidence callsign (mid-name, no parentheses) plus a shared state
        // scores 80: inside the band, below the 95 threshold, and exactly what the model is
        // for. A parenthesised callsign would anchor at 95 and never reach the model.
        let (_server, world, context) = fixture_full(
            "espn.us",
            "XYZ WABC - IL Shelbyville",
            "ABC WABC - IL Springfield",
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let (service, settings) = ai_settings(0.95, Arc::clone(&calls));
        let (channels, entries) = scoped(&world);

        let result = run(
            &context,
            &settings,
            Some(&service),
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect("preview");
        assert!(calls.load(Ordering::SeqCst) >= 1, "the model was asked");
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ABC WABC - IL Springfield")
            .expect("row");
        assert_eq!(espn.epg_data_id, Some(7), "the model's pick was accepted");
        assert!(espn.method.contains("+ AI"), "{}", espn.method);
        assert!(espn
            .ai_decision
            .as_deref()
            .unwrap_or_default()
            .contains("accept"));
    }

    #[tokio::test]
    async fn a_model_that_says_no_leaves_the_channel_unmatched() {
        let (_server, world, context) = fixture_full(
            "espn.us",
            "XYZ WABC - IL Shelbyville",
            "ABC WABC - IL Springfield",
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let (service, settings) = ai_settings(0.05, Arc::clone(&calls));
        let (channels, entries) = scoped(&world);

        let result = run(
            &context,
            &settings,
            Some(&service),
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect("preview");
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ABC WABC - IL Springfield")
            .expect("row");
        assert_eq!(espn.epg_data_id, None);
        assert!(
            espn.ai_decision.is_some(),
            "the decision is recorded even when it is no"
        );
    }

    #[tokio::test]
    async fn an_epg_override_is_skipped_rather_than_bypassed() {
        let (_server, mut world, context) = fixture("espn.us").await;
        world.channels[0].epg_data_id = Some(99);
        world.channels[0].effective_epg_data_id = Some(7);
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(&context, &settings, None, &world, &channels, &entries, true)
            .await
            .expect("preview");
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ESPN HD")
            .expect("row");
        assert_eq!(espn.epg_data_id, None);
        assert_eq!(espn.skipped.as_deref(), Some("override"));
        assert!(espn.reason.contains("override"));
        assert_eq!(associations(&result.rows).len(), 0);
    }

    fn row(name: &str, score: u8, epg: Option<i64>) -> MatchRow {
        MatchRow {
            channel_id: 1,
            channel_name: name.to_string(),
            score,
            epg_data_id: epg,
            method: "Callsign".to_string(),
            has_program_data: epg.is_some(),
            ..MatchRow::default()
        }
    }

    #[test]
    fn the_reason_reads_like_the_plugin() {
        let matched = row("ESPN", 100, Some(1));
        assert_eq!(
            reason_for(&matched, 95, None, false, Some(100)),
            "Excellent match (100%) - Callsign"
        );

        let low = row("ESPN", 70, None);
        assert_eq!(
            reason_for(&low, 95, None, false, Some(70)),
            "Match found but confidence score 70% below threshold (95%)"
        );

        let empty_guide = row("ESPN", 99, None);
        assert_eq!(
            reason_for(&empty_guide, 95, Some(99), false, Some(99)),
            "Best match (99%) carries no programme data in the checked window"
        );

        let nothing = row("ESPN", 0, None);
        assert_eq!(
            reason_for(&nothing, 95, None, false, None),
            "No matching EPG found"
        );
    }

    #[test]
    fn a_skipped_channel_keeps_its_own_reason() {
        let mut skipped = row("ESPN", 0, None);
        skipped.skipped = Some("override".to_string());
        skipped.reason = "Channel has an EPG override".to_string();
        assert!(reason_for(&skipped, 95, None, false, Some(90)).contains("override"));
    }

    #[test]
    fn associations_are_unique_per_channel() {
        let rows = vec![
            row("A", 100, Some(1)),
            row("A", 100, Some(2)),
            row("B", 100, Some(3)),
            row("C", 90, None),
        ];
        // One channel can only end up on one guide; the first row for a channel wins.
        let mut pairs = associations(&rows);
        pairs.sort();
        assert_eq!(pairs, vec![(1, 1)]);
    }

    #[test]
    fn group_names_fall_back_to_no_group() {
        let world = World {
            channels: vec![
                Channel {
                    id: 1,
                    name: "ESPN".to_string(),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 2,
                    name: "Local".to_string(),
                    channel_group_id: None,
                    ..Channel::default()
                },
            ],
            groups: vec![crosstalk_core::dispatcharr::ChannelGroup {
                id: 10,
                name: "Sports".to_string(),
            }],
            ..World::default()
        };
        let mut rows = vec![
            MatchRow {
                channel_id: 1,
                ..MatchRow::default()
            },
            MatchRow {
                channel_id: 2,
                ..MatchRow::default()
            },
        ];
        attach_group_names(&mut rows, &world);
        assert_eq!(rows[0].channel_group, "Sports");
        assert_eq!(rows[1].channel_group, "No Group");
    }

    #[test]
    fn current_epg_names_come_from_the_entry_list() {
        let world = World {
            channels: vec![Channel {
                id: 1,
                epg_data_id: Some(7),
                ..Channel::default()
            }],
            entries: vec![EpgEntry {
                id: 7,
                name: "Old Guide".to_string(),
                ..EpgEntry::default()
            }],
            ..World::default()
        };
        let mut rows = vec![MatchRow {
            channel_id: 1,
            ..MatchRow::default()
        }];
        attach_current_epg_names(&mut rows, &world);
        assert_eq!(rows[0].current_epg_name.as_deref(), Some("Old Guide"));
    }

    #[test]
    fn the_outcome_reports_the_counts() {
        let outcome = outcome(
            &AutoMatchOutcome {
                rows: vec![row("A", 100, Some(1)), row("B", 0, None)],
                report: Some(PathBuf::from("/exports/a.csv")),
                applied: 1,
                candidates: 50,
                channels: 2,
            },
            false,
        );
        assert!(outcome.summary.contains("1/2 matched"));
        assert_eq!(outcome.counts.get("applied"), Some(&1));
        assert_eq!(outcome.counts.get("candidates"), Some(&50));
        assert_eq!(outcome.report_path, Some(PathBuf::from("/exports/a.csv")));
    }
}
