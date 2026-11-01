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
//!
//! AI contract (owner Q4): the model may PROMOTE a candidate inside the verify band or above
//! the rescue floor, and never re-checks or vetoes a heuristic result at or above
//! `automatch_confidence_threshold` — a confident match never reaches the model at all.
//! An AI-accepted pick replaces the heuristic order for same-name guide entries only: every
//! same-name copy is tried in ranked order, and when none carries programme data the
//! channel is left unmatched with a reason saying so. There is deliberately no fallback
//! to other sub-threshold heuristic candidates — the model only runs when no heuristic
//! candidate cleared the threshold, so falling back would contradict the threshold itself.
//! Heal never consults the model.

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

/// The concurrent bound for the AI fan-out: the configured provider's own cap.
///
/// The single source of truth is `[ai.providers.<name>].max_concurrency` in the host
/// config, which both backends also enforce on their own HTTP semaphores. An unknown
/// provider name falls back to the default cap (8); validation enforces a minimum of 1.
pub(crate) fn ai_fanout_bound(config: &crosstalk_core::CrosstalkConfig, provider: &str) -> usize {
    config
        .ai
        .providers
        .get(provider)
        .map(|provider| provider.max_concurrency.max(1))
        .unwrap_or(8)
}

/// Channels per concurrent batch in stage 2. The batch bounds how many validations are
/// in flight at once on top of the semaphore caps, and a cancel is honoured between
/// batches rather than only after the whole scope.
const BATCH_CHANNELS: usize = 256;

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

    // Stage 1: scoring, in parallel. Progress comes from the atomic counter every 500
    // channels (P7): at 20k channels the stage runs ~10 s, which is long enough to go
    // silent otherwise. `progress` is `&self` over a `Send + Sync` sink, so rayon
    // workers can publish directly; rayon preserves order on collect, so `scored` stays
    // in channel order.
    let scored: Vec<(usize, Vec<Scored>)> = {
        let matcher = Arc::clone(&matcher);
        let index = Arc::clone(&index);
        let channels = scoped_channels.channels.clone();
        let tags = crate::tag_list(config.matching.normalize_flags());
        let progress = context.clone();
        let total_channels = channels.len();

        tokio::task::spawn_blocking(move || {
            use rayon::prelude::*;
            let counter = AtomicUsize::new(0);
            // Published `current` must be monotonic: rayon workers finish out of order,
            // so a later-finishing worker with a smaller `done` must not move the
            // bar backwards. Only the largest value so far is published.
            let max_sent = AtomicUsize::new(0);
            let results: Vec<(usize, Vec<Scored>)> = channels
                .par_iter()
                .enumerate()
                .map(|(position, channel)| {
                    let ranked =
                        matcher.score_candidates(&channel.name, &index, &tags_refs(&tags), None);
                    let done = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if done % 500 == 0 || done == total_channels {
                        let prev = max_sent.fetch_max(done, Ordering::Relaxed);
                        if done > prev {
                            progress.progress(&ProgressEvent::new(
                                "janitor.automatch",
                                done as u64,
                                total_channels as u64,
                                "scoring channels",
                            ));
                        }
                    }
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

    // Stage 2 prep (serial, CPU): which channels need the model. The gate decision is
    // pure scoring, so it stays serial; the requests that follow fan out.
    let mut gates: Vec<Option<(ai_gate::GateReason, Vec<usize>)>> =
        Vec::with_capacity(scored.len());
    let mut skipped: Vec<bool> = Vec::with_capacity(scored.len());
    for (position, ranked) in &scored {
        let channel = &scoped_channels.channels[*position];
        if channel.has_epg_override() {
            skipped.push(true);
            gates.push(None);
            continue;
        }
        skipped.push(false);
        // A high-confidence heuristic match is the fast path: validate it and move on.
        // Only a channel the heuristics are unsure about reaches the model.
        // A heuristic result that already clears the threshold is a confident match and
        // is never re-checked by the model.
        let already_confident = ranked.first().is_some_and(|entry| entry.score >= threshold);
        gates.push(if already_confident {
            None
        } else {
            ai_gate::shortlist(ranked, &scoped_entries.entries, &settings.ai)
        });
    }

    // The AI gate fans out (P2), bounded by the provider's own cap; the programme
    // validation walk fans out per channel (P1), bounded by the Dispatcharr concurrency
    // (the client's semaphore caps it again from inside). Each channel's walk stays
    // serial, which preserves the ranked-order winner semantics exactly, and both phases
    // resolve in channel order. Work runs in batches so a cancel is honoured between
    // them rather than only after the whole scope.
    let ai_bound = ai_fanout_bound(context.config(), &settings.ai.provider);
    let validation_bound = config.dispatcharr.concurrency.max(1);
    // Copyable borrows for the `async move` futures below: `&window` inline would capture
    // `window` itself by value and move it on the first batch.
    let window_ref = &window;
    let mut ai_outcomes: Vec<Option<ai_gate::GateOutcome>> =
        (0..scored.len()).map(|_| None).collect();
    let mut walks: Vec<Option<WalkResult>> = (0..scored.len()).map(|_| None).collect();

    for batch_start in (0..scored.len()).step_by(BATCH_CHANNELS) {
        if context.is_cancelled() {
            anyhow::bail!("cancelled by the operator");
        }
        let batch_end = (batch_start + BATCH_CHANNELS).min(scored.len());

        // Phase B: ask the model for every gated channel in the batch, concurrently.
        // Results are stored by channel order and applied below in that order.
        {
            use futures::{stream, StreamExt};
            let asked: Vec<(usize, ai_gate::GateOutcome)> =
                stream::iter((batch_start..batch_end).filter_map(|ordinal| {
                    if skipped[ordinal] {
                        return None;
                    }
                    let (position, ranked) = &scored[ordinal];
                    let (reason, offered) = gates[ordinal].clone()?;
                    let channel = &scoped_channels.channels[*position];
                    let cancel = context.clone();
                    let model = settings.ai.model.clone();
                    let known = matcher.known();
                    Some(async move {
                        // Checked inside the future so a cancel lands within one
                        // in-flight round instead of after the whole batch. The
                        // unanswered outcome is the "no result" value; the batch
                        // cancel check below turns it into an abort.
                        if cancel.is_cancelled() {
                            return (
                                ordinal,
                                ai_gate::GateOutcome {
                                    accepted: None,
                                    reason,
                                    picked: None,
                                    probability: None,
                                    confidence: None,
                                    decision: "cancelled by the operator".to_string(),
                                    model,
                                    calibrated: false,
                                    latency_ms: 0,
                                    input_tokens: 0,
                                    output_tokens: 0,
                                    request: serde_json::json!({}),
                                    response: serde_json::json!({}),
                                    answered: false,
                                },
                            );
                        }
                        let outcome = ai_gate::ask(
                            ai,
                            channel,
                            &scoped_entries.entries,
                            ranked,
                            &offered,
                            reason,
                            known,
                            &settings.ai,
                        )
                        .await;
                        (ordinal, outcome)
                    })
                }))
                .buffer_unordered(ai_bound)
                .collect()
                .await;
            for (ordinal, outcome) in asked {
                ai_outcomes[ordinal] = Some(outcome);
            }
        }

        // Phase C: validate every channel's walk concurrently; each walk is serial.
        {
            use futures::{stream, StreamExt};
            let resolved: Vec<(usize, WalkResult)> =
                stream::iter((batch_start..batch_end).filter_map(|ordinal| {
                    if skipped[ordinal] {
                        return None;
                    }
                    let (_, ranked) = &scored[ordinal];
                    let accepted = ai_outcomes[ordinal]
                        .as_ref()
                        .filter(|outcome| outcome.answered)
                        .and_then(|outcome| outcome.accepted);
                    let cancel = context.clone();
                    Some(async move {
                        let walked = walk_for_channel(
                            client,
                            &cancel,
                            window_ref,
                            ranked,
                            &scoped_entries.entries,
                            accepted,
                            threshold,
                            allow_without_programs,
                        )
                        .await;
                        (ordinal, walked)
                    })
                }))
                .buffer_unordered(validation_bound)
                .collect()
                .await;
            for (ordinal, walked) in resolved {
                walks[ordinal] = Some(walked);
            }
        }

        // Progress after each batch so the network phases are not silent on big
        // scopes, and an abort as soon as a cancel landed inside the batch.
        context.progress(&ProgressEvent::new(
            "janitor.automatch",
            batch_end as u64,
            total,
            "validating candidates",
        ));
        if context.is_cancelled() {
            anyhow::bail!("cancelled by the operator");
        }
    }

    let mut rows = Vec::with_capacity(scoped_channels.channels.len());
    for (ordinal, (position, ranked)) in scored.iter().enumerate() {
        let channel = &scoped_channels.channels[*position];
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
        if skipped[ordinal] {
            row.reason = "Channel has an EPG override in Dispatcharr; skipped so the override \
                          is not silently bypassed"
                .to_string();
            row.skipped = Some("override".to_string());
            rows.push(row);
            continue;
        }

        let answered_outcome = ai_outcomes[ordinal]
            .as_ref()
            .filter(|outcome| outcome.answered);
        if let Some(outcome) = ai_outcomes[ordinal].as_ref() {
            // Recorded exactly once per channel, whether or not it answered: a failed
            // request is part of the story, and the ask-time record already covers both.
            row.ai_decision = Some(outcome.decision.clone());
            context
                .record_ai_decision(&outcome.to_store(rows.len() as i64, &settings.ai.provider))?;
        }

        // The concurrent walk resolved the winner in ranked order; this only maps it
        // back onto the row. See `walk_for_channel` for the search order.
        let walked = walks[ordinal]
            .as_ref()
            .expect("every non-skipped channel was validated");
        let mut chosen: Option<(&Scored, &EpgEntry)> = None;
        let mut best_rejected = walked.best_rejected;
        if let Some(rank_position) = walked.chosen {
            if let Some(candidate) = ranked.get(rank_position) {
                if let Some(entry) = scoped_entries.entries.get(candidate.index) {
                    chosen = Some((candidate, entry));
                }
            }
        }
        if let Some((picked_name, score)) = walked.ai_empty.as_ref() {
            // The model said yes and every same-name guide is empty: say that, rather
            // than reusing the "below threshold" wording the score would suggest.
            best_rejected = Some(*score);
            row.reason = format!(
                "The AI accepted {picked_name} (score {}%) but it carries no programme data \
                 in the checked window",
                score,
            );
        }
        match chosen {
            Some((candidate, entry)) => {
                row.score = candidate.score;
                row.method = candidate.method_label();
                if answered_outcome.is_some_and(|outcome| outcome.accepted.is_some()) {
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

        // A skipped channel and an AI-accepted-but-empty channel already carry their
        // explanation; generating the generic wording over them would lose which case
        // this was.
        if row.reason.is_empty() {
            row.reason = reason_for(
                &row,
                threshold,
                best_rejected,
                allow_without_programs,
                ranked.first().map(|entry| entry.score),
            );
        }
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

/// What one channel's validation walk found, in owned form so the concurrent phase can
/// hand it to the ordered row assembly.
struct WalkResult {
    /// Position in the channel's ranked list that won, if any.
    chosen: Option<usize>,
    /// Score of the last candidate rejected for lack of programmes, if any.
    best_rejected: Option<u8>,
    /// The model's pick and its score, when it accepted a name whose every same-name
    /// guide entry is empty.
    ai_empty: Option<(String, u8)>,
}

/// One channel's validation walk: the first ranked candidate that clears the threshold
/// AND carries programme data wins. An accepted AI pick replaces the heuristic order:
/// every same-name entry is validated in ranked order and the first live duplicate
/// wins, even when the exact picked entry is empty. When every same-name copy is empty
/// the channel is left unmatched with an `ai_empty` reason saying exactly that — there
/// is no fallback to other heuristic candidates, which by construction all scored below
/// the threshold (the model only runs when nothing cleared it), so falling back would
/// contradict the threshold itself.
///
/// Serial per channel and dependency-free across channels, so the caller runs one of
/// these per channel concurrently and resolves the winners in channel order.
#[allow(clippy::too_many_arguments)]
async fn walk_for_channel(
    client: &crosstalk_core::dispatcharr::DispatcharrClient,
    context: &JobContext,
    window: &ProgramWindow,
    ranked: &[Scored],
    entries: &[EpgEntry],
    accepted: Option<usize>,
    threshold: u8,
    allow_without_programs: bool,
) -> WalkResult {
    // Every `has_programs` call below is preceded by a cancel check so a cancel
    // lands within one in-flight round. The empty result is the "no result"
    // value: the outer batch check turns it into an abort.
    fn empty() -> WalkResult {
        WalkResult {
            chosen: None,
            best_rejected: None,
            ai_empty: None,
        }
    }
    if context.is_cancelled() {
        return empty();
    }
    if let Some(position) = accepted {
        let picked_name = ranked
            .get(position)
            .and_then(|candidate| entries.get(candidate.index).map(|entry| entry.name.clone()));
        if let Some(picked_name) = picked_name {
            for (rank_position, candidate) in ranked.iter().enumerate() {
                let Some(entry) = entries.get(candidate.index) else {
                    continue;
                };
                if entry.name != picked_name {
                    continue;
                }
                let has = if allow_without_programs {
                    HasPrograms::Yes
                } else {
                    match entry.epg_source {
                        Some(source) => {
                            if context.is_cancelled() {
                                return empty();
                            }
                            client
                                .has_programs(&entry.tvg_id, source, window)
                                .await
                                .unwrap_or(HasPrograms::No)
                        }
                        None => HasPrograms::No,
                    }
                };
                if has == HasPrograms::Yes {
                    return WalkResult {
                        chosen: Some(rank_position),
                        best_rejected: None,
                        ai_empty: None,
                    };
                }
            }
            let score = ranked.get(position).map(|candidate| candidate.score);
            return WalkResult {
                chosen: None,
                best_rejected: score,
                ai_empty: score.map(|score| (picked_name, score)),
            };
        }
        return WalkResult {
            chosen: None,
            best_rejected: None,
            ai_empty: None,
        };
    }

    let mut best_rejected = None;
    for (rank_position, candidate) in ranked.iter().enumerate() {
        if context.is_cancelled() {
            return empty();
        }
        let Some(entry) = entries.get(candidate.index) else {
            continue;
        };
        if candidate.score >= threshold {
            if allow_without_programs {
                return WalkResult {
                    chosen: Some(rank_position),
                    best_rejected,
                    ai_empty: None,
                };
            }
            let Some(source) = entry.epg_source else {
                continue;
            };
            if context.is_cancelled() {
                return empty();
            }
            let has = client
                .has_programs(&entry.tvg_id, source, window)
                .await
                .unwrap_or(HasPrograms::No);
            match has {
                HasPrograms::Yes => {
                    return WalkResult {
                        chosen: Some(rank_position),
                        best_rejected,
                        ai_empty: None,
                    };
                }
                HasPrograms::No => {
                    best_rejected = Some(candidate.score);
                }
            }
        }
    }
    WalkResult {
        chosen: None,
        best_rejected,
        ai_empty: None,
    }
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
        // Names and reasons come from Dispatcharr: defuse spreadsheet formulas.
        let safe = |text: &str| csv_report::safe_cell(text);
        let safe_opt = |text: &Option<String>| {
            text.as_deref()
                .map(csv_report::safe_cell)
                .unwrap_or_default()
        };
        for row in rows {
            writer.write_record([
                row.channel_id.to_string(),
                safe(&row.channel_name),
                row.channel_number
                    .map(|number| format!("{number}"))
                    .unwrap_or_default(),
                safe(&row.channel_group),
                safe(&row.method),
                row.score.to_string(),
                row.callsign
                    .as_deref()
                    .map(csv_report::safe_cell)
                    .unwrap_or_else(|| "N/A".to_string()),
                safe_opt(&row.epg_source_name),
                row.epg_data_id.map(|id| id.to_string()).unwrap_or_default(),
                safe_opt(&row.epg_channel_name),
                row.current_epg_id
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                safe_opt(&row.current_epg_name),
                if row.has_program_data { "Yes" } else { "No" }.to_string(),
                safe(&row.reason),
                safe_opt(&row.ai_decision),
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
    async fn scoring_reports_progress_from_the_rayon_stage() {
        // P7: the scoring stage publishes as its atomic counter advances, so the UI is
        // not silent on big scopes. Even a two-channel run emits the completion event.
        let (_server, world, base_context) = fixture("espn.us").await;
        let sink = Arc::new(VecSink::new());
        let context = JobContext::new(base_context.config_arc(), sink.clone()).with_dispatcharr(
            base_context
                .dispatcharr()
                .expect("fixture wires a client")
                .clone(),
        );
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);

        run(&context, &settings, None, &world, &channels, &entries, true)
            .await
            .expect("preview");

        let scoring: Vec<_> = sink
            .events()
            .into_iter()
            .filter(|event| {
                event.action == "janitor.automatch" && event.phase == "scoring channels"
            })
            .collect();
        assert!(!scoring.is_empty(), "the scoring stage reported progress");
        let last = scoring.last().expect("at least one scoring event");
        assert_eq!((last.current, last.total), (2, 2));
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

    #[test]
    fn the_ai_fanout_bound_comes_from_the_provider_config() {
        let mut config = crosstalk_core::CrosstalkConfig::default();
        assert_eq!(
            ai_fanout_bound(&config, "typesafe"),
            8,
            "an unconfigured provider falls back to the default cap"
        );

        config.ai.providers.insert(
            "typesafe".to_string(),
            crosstalk_core::config::AiProviderConfig {
                kind: "typesafe".to_string(),
                max_concurrency: 3,
                ..crosstalk_core::config::AiProviderConfig::default()
            },
        );
        assert_eq!(
            ai_fanout_bound(&config, "typesafe"),
            3,
            "the provider's own max_concurrency bounds the fan-out"
        );

        config.ai.providers.insert(
            "zero".to_string(),
            crosstalk_core::config::AiProviderConfig {
                kind: "typesafe".to_string(),
                max_concurrency: 0,
                ..crosstalk_core::config::AiProviderConfig::default()
            },
        );
        assert_eq!(
            ai_fanout_bound(&config, "zero"),
            1,
            "a zero cap still allows one request rather than deadlocking"
        );
    }

    /// A decider whose endpoint is down, so the audit-on-failure path is exercised.
    struct FailingDecider;

    #[async_trait::async_trait]
    impl crosstalk_core::ai::Decider for FailingDecider {
        fn provider(&self) -> &str {
            "typesafe"
        }

        fn calibrated(&self) -> bool {
            true
        }

        async fn decide(
            &self,
            _request: &crosstalk_core::ai::DecisionRequest,
        ) -> Result<crosstalk_core::ai::DecisionResponse> {
            anyhow::bail!("the endpoint is down")
        }
    }

    #[tokio::test]
    async fn each_gated_channel_records_exactly_one_ai_decision() {
        // The Springfield channel lands in the verify band and is answered; the other
        // channel never reaches the model. One gated channel must mean one row — the
        // ask-time record covers answered and failed alike, with no second write in
        // the resolve loop.
        let (_server, world, base_context) = fixture_full(
            "espn.us",
            "XYZ WABC - IL Shelbyville",
            "ABC WABC - IL Springfield",
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let (service, settings) = ai_settings(0.95, Arc::clone(&calls));
        let (channels, entries) = scoped(&world);

        let store = crosstalk_core::store::Store::in_memory().expect("store");
        let run_id = store
            .start_run("janitor", "automatch_preview", true)
            .expect("run");
        let context = JobContext::new(base_context.config_arc(), Arc::new(VecSink::new()))
            .with_dispatcharr(
                base_context
                    .dispatcharr()
                    .expect("fixture wires a client")
                    .clone(),
            )
            .for_run(
                run_id,
                store.clone(),
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
            );

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
        assert_eq!(
            store.ai_decision_count(run_id).expect("count"),
            1,
            "exactly one ai_decisions row for the one gated channel"
        );
        assert!(
            store.ai_decision(run_id, 0).expect("load").is_some(),
            "the gated first row carries the decision"
        );
        assert!(
            store.ai_decision(run_id, 1).expect("load").is_none(),
            "the ungated channel records nothing"
        );
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ABC WABC - IL Springfield")
            .expect("row");
        assert!(espn
            .ai_decision
            .as_deref()
            .unwrap_or_default()
            .contains("accept"));
    }

    #[tokio::test]
    async fn a_failed_ai_request_is_still_audited_exactly_once() {
        let (_server, world, base_context) = fixture_full(
            "espn.us",
            "XYZ WABC - IL Shelbyville",
            "ABC WABC - IL Springfield",
        )
        .await;
        let service =
            crosstalk_core::ai::AiService::with_decider("typesafe", Arc::new(FailingDecider));
        let settings = JanitorSettings {
            ai: crate::settings::AiSettings {
                enabled: true,
                provider: "typesafe".to_string(),
                model: "stub".to_string(),
                ..crate::settings::AiSettings::default()
            },
            ..JanitorSettings::default()
        };
        let (channels, entries) = scoped(&world);

        let store = crosstalk_core::store::Store::in_memory().expect("store");
        let run_id = store
            .start_run("janitor", "automatch_preview", true)
            .expect("run");
        let context = JobContext::new(base_context.config_arc(), Arc::new(VecSink::new()))
            .with_dispatcharr(
                base_context
                    .dispatcharr()
                    .expect("fixture wires a client")
                    .clone(),
            )
            .for_run(
                run_id,
                store.clone(),
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
            );

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
        .expect("a failed model never fails the run");
        assert_eq!(
            store.ai_decision_count(run_id).expect("count"),
            1,
            "the failed request is audited, once"
        );
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ABC WABC - IL Springfield")
            .expect("row");
        assert!(
            espn.ai_decision
                .as_deref()
                .unwrap_or_default()
                .contains("request failed"),
            "the row says the request failed: {:?}",
            espn.ai_decision
        );
        assert_eq!(
            espn.epg_data_id, None,
            "an unanswered channel stays unmatched"
        );
    }

    #[tokio::test]
    async fn an_ai_accept_falls_through_to_a_live_same_name_duplicate() {
        // Two guide entries share one name; the picked (first-ranked) entry is empty but
        // the duplicate carries programmes. The channel must match the duplicate, not be
        // left alone because the exact picked entry is empty.
        let server = MockServer::start().await;
        let channel_name = "ABC WABC - IL Springfield";
        let entry_name = "XYZ WABC - IL Shelbyville";
        Mock::given(method("GET"))
            .and(path("/api/channels/channels/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": 1, "name": channel_name, "channel_number": 101, "channel_group_id": 10}
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
                {"id": 7, "name": entry_name, "tvg_id": "espn-empty.us", "epg_source": 4},
                {"id": 8, "name": entry_name, "tvg_id": "espn-live.us", "epg_source": 4}
            ])))
            .mount(&server)
            .await;
        // Only the duplicate carries programmes; the picked entry is empty.
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .and(query_param("tvg_id", "espn-live.us"))
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

        let world = World {
            channels: vec![Channel {
                id: 1,
                name: channel_name.to_string(),
                channel_number: Some(serde_json::json!(101)),
                channel_group_id: Some(10),
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
                    name: entry_name.to_string(),
                    tvg_id: "espn-empty.us".to_string(),
                    epg_source: Some(4),
                },
                EpgEntry {
                    id: 8,
                    name: entry_name.to_string(),
                    tvg_id: "espn-live.us".to_string(),
                    epg_source: Some(4),
                },
            ],
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
        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == channel_name)
            .expect("row");
        assert_eq!(
            row.epg_data_id,
            Some(8),
            "the live duplicate wins over the empty picked entry"
        );
        assert!(row.method.contains("+ AI"), "{}", row.method);
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

    #[tokio::test]
    async fn a_cancel_before_validation_aborts_promptly() {
        // The cancel flag is set before the validation batches: the batch guard
        // (and the in-future checks) must abort instead of validating.
        let (_server, world, context) = fixture("espn.us").await;
        context.cancel();
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);
        let error = run(&context, &settings, None, &world, &channels, &entries, true)
            .await
            .expect_err("a cancelled run aborts");
        assert!(format!("{error:#}").contains("cancelled"));
    }
    #[tokio::test]
    async fn hostile_names_are_defused_in_the_report() {
        // Dispatcharr-controlled names starting with `=` would execute as spreadsheet
        // formulas when the CSV is opened. The writer prefixes such cells with a quote.
        // `=ESPN` still normalises to ESPN, so the match itself is unaffected.
        let (_server, world, context) = fixture_full("espn.us", "=ESPN", "=ESPN HD").await;
        let settings = JanitorSettings::default();
        let (channels, entries) = scoped(&world);
        let result = run(&context, &settings, None, &world, &channels, &entries, true)
            .await
            .expect("preview");

        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == "=ESPN HD")
            .expect("row");
        assert_eq!(row.epg_channel_name.as_deref(), Some("=ESPN"));

        let report = result.report.expect("report written");
        let text = std::fs::read_to_string(&report).expect("read report");
        assert!(text.contains("'=ESPN HD"), "channel cell is inert: {text}");
        assert!(text.contains("'=ESPN"), "epg cell is inert: {text}");
        assert!(
            !text.lines().any(|line| line.contains(",=ESPN")),
            "no raw formula cell survives: {text}"
        );
    }
}
