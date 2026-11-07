//! Auto-match: score every in-scope channel, validate the winner, and (when applying) write it.
//!
//! Step 8 (goal reset): one ranker, one selection rule. Queries (channels) and candidates
//! (EPG entries) resolve through the unified engine + identity cache (`engine::RunEngine`:
//! store-backed, version-keyed); ranking is the engine's `rank` — the best identity FAMILY
//! per channel, in rank order, at the ONE `[matching]` cut — and selection is the family's
//! first member that carries programme data. Janitor's `_find_best_epg_match` scoring half
//! (standalone structural callsign/state/city/network score, D06) is retired: callsign
//! agreement now decides at family level through the engine's anchor (C06/D04).
//!
//! The pipeline is split in two stages because they have different bottlenecks:
//!
//!   1. **Scoring** is pure CPU over strings: every channel's family against one shared
//!      engine index. It runs on a rayon pool, so the work scales with cores.
//!   2. **Validation** is IO: does this family member actually carry programmes? It runs in
//!      family order, through the client's own concurrency cap, and the answers are
//!      memoised, so a thousand channels considering the same ESPN entry cost one request.
//!
//! Both stages keep the plugin's order within the family: the first family member that both
//! clears the cut AND has programmes is the one that wins.
//!
//! Only the EPG half lives here. The family scoring stage, the validation walk and the AI
//! gate are in `crosstalk_core` (`match_run`, `ai_gate`), because streams and lineup run the
//! same loop against their own candidates. So is the report writer; what stays local is the
//! EPG row, the EPG reason wording, and the write itself.
//!
//! AI contract (owner Q4): the model may PROMOTE a candidate inside the verify band or above
//! the rescue floor, and never re-checks or vetoes a heuristic result at or above the
//! `[matching]` cut — a confident match never reaches the model at all. Because every
//! family member clears the cut, a channel with an EMPTY family is the borderline case:
//! it is re-ranked by the same engine and index at the gate's floor
//! (`RunEngine::shortlist_engine`), and those sub-cut candidates are what the gate offers.
//! An AI-accepted pick replaces the heuristic order for same-name guide entries only: every
//! same-name copy is tried in ranked order, and when none carries programme data the
//! channel is left unmatched with a reason saying so. There is deliberately no fallback
//! to other sub-threshold heuristic candidates — the model only runs when no heuristic
//! candidate cleared the threshold, so falling back would contradict the threshold itself.
//! Heal never consults the model.

use crate::engine::RunEngine;
use crate::scope::ScopedEntries;
use crate::settings::GuideSettings;
use anyhow::{Context, Result};
use crosstalk_core::ai_gate;
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{DispatcharrClient, EpgEntry, HasPrograms, ProgramWindow, World};
use crosstalk_core::jobs::{JobContext, ProgressEvent};
use crosstalk_core::match_run::{self, CandidateProbe, ConfidenceBand, ScoreProgress, WalkRules};
use crosstalk_core::scope::ScopedChannels;
use crosstalk_core::store::RunRow;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use crosstalk_matching::engine::EngineRanked;
use crosstalk_matching::pipeline::Scored;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

/// The walk, the gate and the report all speak the profile `Scored` shape, while the
/// unified engine ranks families of [`EngineRanked`]. This is the one adapter between
/// them: the score and the order carry over untouched, and the engine method maps onto
/// the profile's coarse components (`Alias`/`Exact`/`Substring`/`Fuzzy`/`Callsign`).
/// Janitor's structural components (`Callsign + State`, D06 retired) never appear: the
/// anchor decides callsign agreement at family level (C06/D04) instead of scoring it.
pub fn to_scored(family: &[EngineRanked]) -> Vec<Scored> {
    use crosstalk_matching::engine::EngineMethod;
    family
        .iter()
        .map(|member| {
            let components = match member.method {
                EngineMethod::Alias => vec!["Alias"],
                EngineMethod::Exact => vec!["Exact"],
                EngineMethod::Substring => vec!["Substring"],
                EngineMethod::Fuzzy(_) => vec!["Fuzzy"],
                EngineMethod::Callsign => vec!["Callsign"],
            };
            Scored {
                index: member.index,
                score: member.score,
                components,
            }
        })
        .collect()
}

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
            ..RunRow::default()
        }
    }
}

/// Channels per concurrent batch in stage 2. The batch bounds how many validations are
/// in flight at once on top of the semaphore caps, and a cancel is honoured between
/// batches rather than only after the whole scope.
const BATCH_CHANNELS: usize = 256;

/// What an auto-match run produced.
#[derive(Debug, Clone, Default)]
pub struct MatchOutcome {
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
/// entry. `app` is the run's author, which names the report it writes.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &GuideSettings,
    ai: Option<&crosstalk_core::ai::AiService>,
    world: &World,
    scoped_channels: &ScopedChannels,
    scoped_entries: &ScopedEntries,
    dry_run: bool,
) -> Result<MatchOutcome> {
    let client = context.dispatcharr()?;
    let config = context.config();

    // The unified stack for this run: engine + identity cache (store-backed when the run
    // has a store), deciding at the ONE `[matching]` cut. Threshold, families and the
    // cache are all this object's; `settings` only carries the programme-data policy.
    let run_engine = RunEngine::for_config(&config.matching, context.store().cloned());
    let engine = Arc::clone(run_engine.engine());

    // The candidate side: names, normalised once into one shared engine index.
    let names: Vec<String> = scoped_entries
        .entries
        .iter()
        .map(|entry| entry.name.clone())
        .collect();
    let index = Arc::new(run_engine.build_index(&names));

    let total = scoped_channels.channels.len() as u64;
    context.progress(&ProgressEvent::new(
        "guide.match",
        0,
        total,
        "scoring channels",
    ));

    // Stage 1: family scoring, in parallel. Progress comes from the shared stage, which
    // publishes every 500 channels (P7): at 20k channels the stage runs ~10 s, which is
    // long enough to go silent otherwise. Rayon preserves order on collect, so the
    // results stay in channel order.
    let channel_names: Vec<String> = scoped_channels
        .channels
        .iter()
        .map(|channel| channel.name.clone())
        .collect();
    let families: Vec<Vec<EngineRanked>> = match_run::score_families(
        &channel_names,
        Arc::clone(&engine),
        Arc::clone(&index),
        Some(ScoreProgress {
            context,
            action: "guide.match",
            phase: "scoring channels",
        }),
    )
    .await?;
    // The walk, the gate and the report speak `Scored`; the adapter is the only place
    // the two shapes meet (scores and order carry over untouched).
    let scored: Vec<Vec<Scored>> = families.iter().map(|family| to_scored(family)).collect();

    context.progress(&ProgressEvent::new(
        "guide.match",
        total,
        total,
        "validating candidates",
    ));

    // Stage 2: validation, in family order, memoised by the client.
    let ai_settings = &config.ai.verify;
    // The ONE cut (D02/T10 `janitor-95`): `[matching]` sensitivity, never a guide file
    // value — the retired `match_confidence_threshold` cannot even parse any more.
    let threshold = run_engine.threshold();
    // The walk only asks Dispatcharr when the operator has not waived the programme check.
    let rules = WalkRules {
        threshold,
        validate: !settings.allow_epg_without_programs,
    };
    let probe = EpgProbe {
        client,
        window: ProgramWindow::ahead(settings.check_hours),
    };
    let allow_without_programs = settings.allow_epg_without_programs;
    let source_names: HashMap<i64, String> = scoped_entries
        .sources
        .iter()
        .map(|(id, source)| (*id, source.name.clone()))
        .collect();

    // Stage 1b: the AI shortlist. A channel with no family at the cut is exactly the
    // borderline case the `[ai.verify]` chain exists for, but the decision cut has
    // discarded its sub-cut candidates. Re-rank those channels with the SAME engine
    // and index at the gate's floor (verify band / rescue floor): one ranker, only the
    // cut is a parameter. Every candidate there scores below the decision cut, so the
    // heuristic walk still accepts nothing from it — only a model accept can.
    let mut scored = scored;
    if let Some(shortlist_engine) = run_engine.shortlist_engine(ai_settings) {
        let borderline: Vec<usize> = (0..scored.len())
            .filter(|&ordinal| {
                !scoped_channels.channels[ordinal].has_epg_override()
                    && match_run::needs_gate(
                        scored[ordinal].first().map(|entry| entry.score),
                        threshold,
                    )
            })
            .collect();
        if !borderline.is_empty() {
            let borderline_names: Vec<String> = borderline
                .iter()
                .map(|&ordinal| channel_names[ordinal].clone())
                .collect();
            let shortlists = match_run::score_shortlists(
                &borderline_names,
                shortlist_engine,
                Arc::clone(&index),
            )
            .await?;
            for (ordinal, shortlist) in borderline.into_iter().zip(shortlists) {
                scored[ordinal] = to_scored(&shortlist);
            }
        }
    }

    // Stage 2 prep (serial, CPU): which channels need the model. The gate decision is
    // pure scoring, so it stays serial; the requests that follow fan out.
    let mut gates: Vec<Option<(ai_gate::GateReason, Vec<usize>)>> =
        Vec::with_capacity(scored.len());
    let mut skipped: Vec<bool> = Vec::with_capacity(scored.len());
    for (ordinal, ranked) in scored.iter().enumerate() {
        let channel = &scoped_channels.channels[ordinal];
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
        gates.push(
            if match_run::needs_gate(ranked.first().map(|entry| entry.score), threshold) {
                ai_gate::shortlist(ranked, &scoped_entries.entries, ai_settings)
            } else {
                None
            },
        );
    }

    // Every non-skipped query and its family persist through the identity cache in one
    // batch, so the store holds the identity any other app resolves the same name to.
    run_engine
        .warm_families(&channel_names, &families, &names, &skipped)
        .await;

    // The AI gate fans out (P2), bounded by the provider's own cap; the programme
    // validation walk fans out per channel (P1), bounded by the Dispatcharr concurrency
    // (the client's semaphore caps it again from inside). Each channel's walk stays
    // serial, which preserves the ranked-order winner semantics exactly, and both phases
    // resolve in channel order. Work runs in batches so a cancel is honoured between
    // them rather than only after the whole scope.
    let ai_bound = ai_gate::fanout_bound_for(context.config(), &ai_settings.effective_chain());
    let validation_bound = config.dispatcharr.concurrency.max(1);
    // Copyable borrows for the `async move` futures below: `&probe` inline would capture
    // `probe` itself by value and move it on the first batch.
    let probe_ref = &probe;
    let mut ai_outcomes: Vec<Option<ai_gate::GateOutcome>> =
        (0..scored.len()).map(|_| None).collect();
    let mut walks: Vec<Option<match_run::WalkResult>> = (0..scored.len()).map(|_| None).collect();

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
                    let ranked = &scored[ordinal];
                    let (reason, offered) = gates[ordinal].clone()?;
                    let channel = &scoped_channels.channels[ordinal];
                    let cancel = context.clone();
                    let model = ai_settings.model.clone();
                    let known = engine.known();
                    Some(async move {
                        // Checked inside the future so a cancel lands within one
                        // in-flight round instead of after the whole batch. The
                        // unanswered outcome is the "no result" value; the batch
                        // cancel check below turns it into an abort.
                        if cancel.is_cancelled() {
                            return (ordinal, ai_gate::GateOutcome::cancelled(reason, model));
                        }
                        let outcome = ai_gate::ask(
                            ai,
                            channel,
                            &scoped_entries.entries,
                            ranked,
                            &offered,
                            reason,
                            known,
                            ai_settings,
                            || cancel.is_cancelled(),
                            // The task prefix changes a chat model's prompt and the audited
                            // request, so only a configured chain gets it: shorthand installs
                            // ask exactly what they asked before the chain existed.
                            (!ai_settings.chain.is_empty()).then_some("epg"),
                        )
                        .await;
                        (ordinal, outcome)
                    })
                }))
                .buffer_unordered(ai_bound)
                .collect()
                .await;
            for (ordinal, outcome) in asked {
                // Recorded here, at ask time, exactly once per channel whether or not it
                // answered: a failed request is part of the story too. Recording in the row
                // loop below would lose every earlier batch's paid calls when a cancel
                // lands mid-run, so the audit row is written while the answer is in hand.
                // The row index is the channel ordinal: the row loop pushes exactly one row
                // per ordinal in order, so `ordinal` is the row this outcome will label.
                context
                    .record_ai_decision(&outcome.to_store(ordinal as i64, &ai_settings.provider))?;
                ai_outcomes[ordinal] = Some(outcome);
            }
        }

        // Phase C: validate every channel's walk concurrently; each walk is serial.
        {
            use futures::{stream, StreamExt};
            let resolved: Vec<(usize, match_run::WalkResult)> =
                stream::iter((batch_start..batch_end).filter_map(|ordinal| {
                    if skipped[ordinal] {
                        return None;
                    }
                    let ranked = &scored[ordinal];
                    let accepted = ai_outcomes[ordinal]
                        .as_ref()
                        .filter(|outcome| outcome.answered)
                        .and_then(|outcome| outcome.accepted);
                    let cancel = context.clone();
                    Some(async move {
                        let walked = match_run::walk(
                            &cancel,
                            &scoped_entries.entries,
                            ranked,
                            accepted,
                            rules,
                            probe_ref,
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
            "guide.match",
            batch_end as u64,
            total,
            "validating candidates",
        ));
        if context.is_cancelled() {
            anyhow::bail!("cancelled by the operator");
        }
    }

    let mut rows = Vec::with_capacity(scoped_channels.channels.len());
    for (ordinal, ranked) in scored.iter().enumerate() {
        let channel = &scoped_channels.channels[ordinal];
        let mut row = MatchRow {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_number: channel.display_number(),
            channel_group: String::new(),
            current_epg_id: channel.current_epg_id(),
            current_epg_name: None,
            // The shared-core ladder over the merged allowlist (C01/C02), through the
            // run's engine rather than the retired profile matcher.
            callsign: engine.callsign_of(&channel.name),
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
            // The audit row was already written at ask time (see above), so this only
            // carries the one-line summary onto the report row.
            row.ai_decision = Some(outcome.decision.clone());
        }

        // The concurrent walk resolved the winner in ranked order; this only maps it
        // back onto the row. See `match_run::walk` for the search order.
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
        // An escalated chain says who decided after whom, for example
        // "AI verified (llm/kimi-k3 after typesafe/jev-latest undecided)": that trail
        // belongs on the row, not just in the AI column, because the reason is what the
        // operator reads first. Single-link rows have no trail and read as before.
        if let Some(note) = ai_outcomes[ordinal]
            .as_ref()
            .and_then(|outcome| outcome.chain_note())
        {
            if !row.reason.is_empty() {
                row.reason.push(' ');
            }
            row.reason.push_str(&note);
        }
        rows.push(row);

        if rows.len() % 200 == 0 {
            context.progress(&ProgressEvent::new(
                "guide.match",
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

    let report = write_report(
        app,
        context,
        settings,
        scoped_channels,
        &rows,
        &matched,
        dry_run,
    )?;

    let mut applied = 0;
    if !dry_run {
        let associations: Vec<(i64, Option<i64>)> = matched
            .iter()
            .filter_map(|row| row.epg_data_id.map(|epg| (row.channel_id, Some(epg))))
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
            &app.export_prefix(),
            context.config().exports.retention_days,
            name.as_deref(),
        );
    }

    Ok(MatchOutcome {
        rows,
        report,
        applied,
        candidates: scoped_entries.entries.len(),
        channels: scoped_channels.channels.len(),
    })
}

/// Whether a scoped EPG entry carries programme data in the run's window.
///
/// The one EPG-specific half of the shared walk. A failed check is "no", never an error: one
/// broken entry must not fail a run that can still match the rest. Shared with heal, which
/// walks the same family shape.
pub(crate) struct EpgProbe<'a> {
    pub(crate) client: &'a DispatcharrClient,
    pub(crate) window: ProgramWindow,
}

#[async_trait::async_trait]
impl CandidateProbe<EpgEntry> for EpgProbe<'_> {
    async fn carries_data(&self, entry: &EpgEntry) -> bool {
        // Without a source there is nothing to ask about; the plugin's own query would find
        // nothing either.
        let Some(source) = entry.epg_source else {
            return false;
        };
        self.client
            .has_programs(&entry.tvg_id, source, &self.window)
            .await
            .unwrap_or(HasPrograms::No)
            == HasPrograms::Yes
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
        let mut reason = match ConfidenceBand::of(score) {
            ConfidenceBand::Excellent => format!("Excellent match ({score}%)"),
            ConfidenceBand::High => format!("High confidence match ({score}%)"),
            ConfidenceBand::Good => format!("Good match ({score}%)"),
            ConfidenceBand::Moderate => format!("Moderate confidence match ({score}%)"),
            ConfidenceBand::Low => format!("Low confidence match ({score}%)"),
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
    app: &dyn SubApp,
    context: &JobContext,
    settings: &GuideSettings,
    scoped_channels: &ScopedChannels,
    rows: &[MatchRow],
    matched: &[&MatchRow],
    dry_run: bool,
) -> Result<Option<PathBuf>> {
    let exports_dir = context.exports_dir();
    let cut = crate::engine::cut_setting(&context.config().matching);
    let report = csv_report::Report::new(
        &exports_dir,
        app,
        if dry_run {
            "match_preview"
        } else {
            "match_applied"
        },
        if dry_run {
            "Auto-Match Preview Report"
        } else {
            "Auto-Match Applied Report"
        },
    )
    .channels_processed(scoped_channels.channels.len())
    .result("Channels with a match found", matched.len())
    .result("Matches carrying program data", matched.len())
    .result(
        "Callsigns extracted",
        rows.iter().filter(|row| row.callsign.is_some()).count(),
    )
    .result(
        "Assignments this run applies",
        if dry_run {
            "none, this was a preview".to_string()
        } else {
            matched.len().to_string()
        },
    )
    .setting(cut.0, cut.1);

    let path = report.write(
        context.config(),
        &crate::settings::schema(),
        &crate::settings::to_values(settings),
        &COLUMNS,
        |writer| {
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
        },
    )?;
    Ok(Some(path))
}

/// The outcome the runner records.
pub fn outcome(result: &MatchOutcome, dry_run: bool) -> JobOutcome {
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

    let mut outcome = OutcomeBuilder::new(format!(
        "Auto-match {}: {}/{} matched",
        if dry_run { "preview" } else { "applied" },
        matched,
        result.channels
    ))
    .count("channels", result.channels as i64)
    .count("candidates", result.candidates as i64)
    .count("matched", matched as i64)
    .count("skipped", skipped as i64)
    .count("applied", result.applied as i64);
    if skipped > 0 {
        outcome = outcome.note(format!("{skipped} skipped"));
    }
    if !dry_run {
        outcome = outcome.note(format!("{} assignments written", result.applied));
    }
    outcome.report(result.report.clone()).build()
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
    use crate::GuideApp;
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

    /// Same fixture, with the guide entry named by the caller. The default pair
    /// (`ESPN HD` against `ESPN`) resolves through the unified engine's alias table at
    /// 100 — a confident family, so the model is never asked.
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
                ..EpgSource::default()
            }],
            entries: vec![EpgEntry {
                id: 7,
                name: entry_name.to_string(),
                tvg_id: "espn.us".to_string(),
                epg_source: Some(4),
                icon_url: None,
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
        let scope = crosstalk_core::config::ScopeConfig::default();
        (
            crosstalk_core::scope::scope_channels(world, &scope).expect("channels"),
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
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        run(
            &GuideApp::new(),
            &context,
            &settings,
            None,
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect("preview");

        let scoring: Vec<_> = sink
            .events()
            .into_iter()
            .filter(|event| event.action == "guide.match" && event.phase == "scoring channels")
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
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            None,
            &world,
            &channels,
            &entries,
            true,
        )
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
        // The preamble names the app that wrote it, from the app itself.
        assert!(
            text.starts_with(&format!(
                "# crosstalk Guide v{} - Auto-Match Preview Report",
                crosstalk_core::HOST_VERSION
            )),
            "{text}"
        );
        assert!(text.contains("channel_name,channel_number"));
        assert!(text.contains("ESPN HD"));
    }

    #[tokio::test]
    async fn an_apply_writes_the_assignment() {
        let (_server, world, context) = fixture("espn.us").await;
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            None,
            &world,
            &channels,
            &entries,
            false,
        )
        .await
        .expect("apply");
        assert_eq!(result.applied, 1);
    }

    #[tokio::test]
    async fn a_candidate_without_programmes_is_rejected() {
        // The only entry has no programmes, so nothing may be applied even though it scores.
        let (_server, world, context) = fixture("some-other-tvg").await;
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            None,
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
            .find(|row| row.channel_name == "ESPN HD")
            .expect("row");
        assert_eq!(espn.epg_data_id, None);
        assert!(espn.reason.contains("no programme data"), "{}", espn.reason);
    }

    #[tokio::test]
    async fn allow_without_programs_accepts_an_empty_guide() {
        let (_server, world, context) = fixture("some-other-tvg").await;
        let settings = GuideSettings {
            allow_epg_without_programs: true,
            ..GuideSettings::default()
        };
        let (channels, entries) = scoped(&world);
        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            None,
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

    fn ai_settings(probability: f64, calls: Arc<AtomicUsize>) -> crosstalk_core::ai::AiService {
        crosstalk_core::ai::AiService::with_decider(
            "typesafe",
            Arc::new(FixedDecider { probability, calls }),
        )
    }

    /// The AI policy as the host file holds it: `[ai.verify]`.
    fn ai_policy() -> crosstalk_core::config::AiVerifyConfig {
        crosstalk_core::config::AiVerifyConfig {
            enabled: true,
            provider: "typesafe".to_string(),
            model: "stub".to_string(),
            ..crosstalk_core::config::AiVerifyConfig::default()
        }
    }

    /// The fixture's context with the gate switched on. The policy lives in the host config
    /// now, so a test that wants the model asked has to configure the host, not `guide.toml`.
    fn gate_on(base: &JobContext) -> JobContext {
        let mut config = (*base.config_arc()).clone();
        config.ai.verify = ai_policy();
        JobContext::new(std::sync::Arc::new(config), Arc::new(VecSink::new()))
            .with_dispatcharr(base.dispatcharr().expect("fixture wires a client").clone())
    }

    #[tokio::test]
    async fn a_confident_match_never_calls_the_model() {
        let (_server, world, context) = fixture("espn.us").await;
        let calls = Arc::new(AtomicUsize::new(0));
        let service = ai_settings(0.99, Arc::clone(&calls));
        let context = gate_on(&context);
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
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
        // The cut lives in `[matching]`, so the report names it explicitly.
        let text =
            std::fs::read_to_string(result.report.as_ref().expect("report")).expect("read report");
        assert!(
            text.contains(
                "#   Matching Sensitivity ([matching] in crosstalk.toml): strict, cut 95"
            ),
            "{text}"
        );
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ESPN HD")
            .expect("row");
        assert_eq!(espn.epg_data_id, Some(7));
        assert!(espn.ai_decision.is_none());
    }

    /// The verify-band pair at the default cut: the unified engine scores
    /// `Sky Cinema Premiere` against `Sky Cinema Premiere HD 2` at 90 (substring) — inside
    /// `[ai.verify]`'s 80-94 band and below the strict 95 cut, so the family is empty and
    /// only the model can promote the entry.
    const BAND_CHANNEL: &str = "Sky Cinema Premiere";
    const BAND_ENTRY: &str = "Sky Cinema Premiere HD 2";

    async fn band_fixture() -> (MockServer, World, JobContext) {
        fixture_full("espn.us", BAND_ENTRY, BAND_CHANNEL).await
    }

    #[tokio::test]
    async fn a_verify_band_match_reaches_the_model_at_the_default_cut() {
        // Step 8 + owner Q4: the decision family is empty at the strict cut, so the run
        // re-ranks the channel with the same engine at the gate's floor and asks the
        // model about the 90. An accept promotes it; the heuristics alone would not.
        let (_server, world, context) = band_fixture().await;
        assert_eq!(context.config().matching.threshold(), 95, "the default cut");
        let calls = Arc::new(AtomicUsize::new(0));
        let service = ai_settings(0.95, Arc::clone(&calls));
        let context = gate_on(&context);
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
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
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the model was asked once");
        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == BAND_CHANNEL)
            .expect("row");
        assert_eq!(row.epg_data_id, Some(7), "the model's pick was accepted");
        assert_eq!(row.score, 90, "the row keeps the heuristic score");
        assert!(row.method.contains("+ AI"), "{}", row.method);
        let decision = row.ai_decision.as_deref().unwrap_or_default();
        assert!(decision.contains("verify"), "{decision}");
        assert!(decision.contains("accept"), "{decision}");
    }

    #[tokio::test]
    async fn without_the_gate_a_verify_band_match_stays_unmatched() {
        // The control for the test above: the same channel with the gate off is left
        // unmatched, so the accept is what changed the outcome.
        let (_server, world, context) = band_fixture().await;
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);
        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            None,
            &world,
            &channels,
            &entries,
            true,
        )
        .await
        .expect("preview");
        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == BAND_CHANNEL)
            .expect("row");
        assert_eq!(row.epg_data_id, None);
        assert!(row.ai_decision.is_none());
    }

    #[tokio::test]
    async fn a_model_that_says_no_leaves_the_channel_unmatched() {
        let (_server, world, context) = band_fixture().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let service = ai_settings(0.05, Arc::clone(&calls));
        let context = gate_on(&context);
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
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
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the model was asked");
        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == BAND_CHANNEL)
            .expect("row");
        assert_eq!(row.epg_data_id, None);
        assert!(
            row.ai_decision.is_some(),
            "the decision is recorded even when it is no"
        );
        assert!(!row.method.contains("+ AI"), "{}", row.method);
    }

    #[tokio::test]
    async fn a_rescue_candidate_reaches_the_model_below_the_band() {
        // `Science Channel` against `Science Channel Live` scores 75: under the verify
        // band, above the 60 rescue floor. Rescue offers it and an accept promotes it.
        let (_server, world, context) =
            fixture_full("espn.us", "Science Channel Live", "Science Channel").await;
        let calls = Arc::new(AtomicUsize::new(0));
        let service = ai_settings(0.95, Arc::clone(&calls));
        let context = gate_on(&context);
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
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
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the model was asked");
        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == "Science Channel")
            .expect("row");
        assert_eq!(row.epg_data_id, Some(7));
        assert_eq!(row.score, 75);
        let decision = row.ai_decision.as_deref().unwrap_or_default();
        assert!(decision.contains("rescue"), "{decision}");
    }

    #[tokio::test]
    async fn a_pair_the_engine_cannot_score_never_reaches_the_model() {
        // The WABC pair scored a structural 80 under Janitor (D06, retired); the unified
        // engine gives it no score at any cut, so there is nothing to offer and the
        // model is never asked.
        let (_server, world, context) = fixture_full(
            "espn.us",
            "XYZ WABC - IL Shelbyville",
            "ABC WABC - IL Springfield",
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let service = ai_settings(0.95, Arc::clone(&calls));
        let context = gate_on(&context);
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
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
        assert_eq!(calls.load(Ordering::SeqCst), 0, "the model was not asked");
        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ABC WABC - IL Springfield")
            .expect("row");
        assert_eq!(row.epg_data_id, None);
        assert!(row.ai_decision.is_none());
        assert_eq!(row.reason, "No matching EPG found");
    }

    #[tokio::test]
    async fn a_confident_family_is_never_vetoed_even_by_a_rejecting_model() {
        // Owner Q4 + D02/T10: the model may promote inside the band, never veto a family
        // at or above the cut. `ESPN HD` resolves to `ESPN` at 100, so a model that
        // would reject is never even asked and the confident match stands.
        let (_server, world, context) = fixture("espn.us").await;
        let calls = Arc::new(AtomicUsize::new(0));
        let service = ai_settings(0.05, Arc::clone(&calls));
        let context = gate_on(&context);
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
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
        assert_eq!(calls.load(Ordering::SeqCst), 0, "the model was not asked");
        let espn = result
            .rows
            .iter()
            .find(|row| row.channel_name == "ESPN HD")
            .expect("row");
        assert_eq!(espn.epg_data_id, Some(7));
        assert!(espn.ai_decision.is_none());
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
        // The band channel is answered; the other channel never reaches the model. One
        // gated channel must mean one row — the ask-time record covers answered and
        // failed alike, with no second write in the resolve loop.
        let (_server, world, base_context) = band_fixture().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let service = ai_settings(0.95, Arc::clone(&calls));
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let store = crosstalk_core::store::Store::in_memory().expect("store");
        let run_id = store
            .start_run("guide", "match_preview", true)
            .expect("run");
        let mut config = (*base_context.config_arc()).clone();
        config.ai.verify = ai_policy();
        let context = JobContext::new(std::sync::Arc::new(config), Arc::new(VecSink::new()))
            .with_dispatcharr(
                base_context
                    .dispatcharr()
                    .expect("fixture wires a client")
                    .clone(),
            )
            .for_run(
                crate::APP_ID,
                "match_preview",
                run_id,
                store.clone(),
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
            );

        let result = run(
            &GuideApp::new(),
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
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the model was asked");
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
        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == BAND_CHANNEL)
            .expect("row");
        assert!(row
            .ai_decision
            .as_deref()
            .unwrap_or_default()
            .contains("accept"));
    }

    #[tokio::test]
    async fn a_failed_ai_request_is_still_audited_exactly_once() {
        let (_server, world, base_context) = band_fixture().await;
        let service =
            crosstalk_core::ai::AiService::with_decider("typesafe", Arc::new(FailingDecider));
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let store = crosstalk_core::store::Store::in_memory().expect("store");
        let run_id = store
            .start_run("guide", "match_preview", true)
            .expect("run");
        let mut config = (*base_context.config_arc()).clone();
        config.ai.verify = ai_policy();
        let context = JobContext::new(std::sync::Arc::new(config), Arc::new(VecSink::new()))
            .with_dispatcharr(
                base_context
                    .dispatcharr()
                    .expect("fixture wires a client")
                    .clone(),
            )
            .for_run(
                crate::APP_ID,
                "match_preview",
                run_id,
                store.clone(),
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
            );

        let result = run(
            &GuideApp::new(),
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
        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == BAND_CHANNEL)
            .expect("row");
        assert!(
            row.ai_decision
                .as_deref()
                .unwrap_or_default()
                .contains("request failed"),
            "the row says the request failed: {:?}",
            row.ai_decision
        );
        assert_eq!(
            row.epg_data_id, None,
            "an unanswered channel stays unmatched"
        );
    }

    #[tokio::test]
    async fn an_undecided_first_link_escalates_and_the_row_says_who_decided() {
        // Jev answers 0.5 (between the thresholds, so undecided) and the LLM answers 0.95:
        // the channel matches on the second link, and the row reason names the trail.
        let (_server, world, base_context) = band_fixture().await;
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let service = crosstalk_core::ai::AiService::with_deciders(vec![
            (
                "typesafe".to_string(),
                Arc::new(FixedDecider {
                    probability: 0.5,
                    calls: Arc::clone(&first_calls),
                }) as Arc<dyn crosstalk_core::ai::Decider>,
            ),
            (
                "llm".to_string(),
                Arc::new(FixedDecider {
                    probability: 0.95,
                    calls: Arc::clone(&second_calls),
                }) as Arc<dyn crosstalk_core::ai::Decider>,
            ),
        ]);
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let store = crosstalk_core::store::Store::in_memory().expect("store");
        let run_id = store
            .start_run("guide", "match_preview", true)
            .expect("run");
        let mut config = (*base_context.config_arc()).clone();
        config.ai.verify = ai_policy();
        config.ai.verify.chain = vec![
            crosstalk_core::config::AiChainEntry::new("typesafe", "stub"),
            crosstalk_core::config::AiChainEntry::new("llm", "stub-llm"),
        ];
        let context = JobContext::new(std::sync::Arc::new(config), Arc::new(VecSink::new()))
            .with_dispatcharr(
                base_context
                    .dispatcharr()
                    .expect("fixture wires a client")
                    .clone(),
            )
            .for_run(
                crate::APP_ID,
                "match_preview",
                run_id,
                store.clone(),
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
            );

        let result = run(
            &GuideApp::new(),
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
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1, "Jev was undecided");
        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == BAND_CHANNEL)
            .expect("row");
        assert_eq!(
            row.epg_data_id,
            Some(7),
            "the second link's pick was accepted"
        );
        assert!(
            row.reason
                .contains("AI verified (llm/stub-llm after typesafe/stub undecided)"),
            "the row reason names the trail: {:?}",
            row.reason
        );
        let stored = (0..32)
            .filter_map(|ordinal| store.ai_decision(run_id, ordinal).expect("load"))
            .next()
            .expect("the escalated channel is audited");
        assert_eq!(
            stored.provider, "llm",
            "the audit names the link that decided"
        );
    }

    /// Two guide entries share one name; the first-ranked is empty and the duplicate
    /// carries programmes. Returns the model's call count and the channel's row.
    async fn same_name_duplicate_case(channel_name: &str, entry_name: &str) -> (usize, MatchRow) {
        let server = MockServer::start().await;
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
                ..EpgSource::default()
            }],
            entries: vec![
                EpgEntry {
                    id: 7,
                    name: entry_name.to_string(),
                    tvg_id: "espn-empty.us".to_string(),
                    epg_source: Some(4),
                    icon_url: None,
                },
                EpgEntry {
                    id: 8,
                    name: entry_name.to_string(),
                    tvg_id: "espn-live.us".to_string(),
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
        let exports = dir.path().join("exports");
        config.jobs.exports_dir = exports.display().to_string();
        let client = DispatcharrClient::new(&config.dispatcharr).expect("client");
        let context = JobContext::new(
            std::sync::Arc::new(config),
            std::sync::Arc::new(VecSink::new()),
        )
        .with_dispatcharr(client);

        let calls = Arc::new(AtomicUsize::new(0));
        let service = ai_settings(0.95, Arc::clone(&calls));
        let context = gate_on(&context);
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
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
        let row = result
            .rows
            .iter()
            .find(|row| row.channel_name == channel_name)
            .expect("row")
            .clone();
        (calls.load(Ordering::SeqCst), row)
    }

    #[tokio::test]
    async fn a_confident_family_falls_through_to_a_live_same_name_duplicate() {
        // Step 8 (the unified selection rule): the family's first member is empty, so the
        // walk takes the next one with programmes — no model in the loop.
        let (calls, row) = same_name_duplicate_case("ESPN HD", "ESPN").await;
        assert_eq!(calls, 0, "the model was not asked");
        assert_eq!(
            row.epg_data_id,
            Some(8),
            "the live duplicate wins over the empty first family member"
        );
        assert!(!row.method.contains("+ AI"), "{}", row.method);
    }

    #[tokio::test]
    async fn an_ai_accept_falls_through_to_a_live_same_name_duplicate() {
        // The verify-band variant: the model accepts the name, the picked entry is empty
        // and its same-name duplicate is live, so the duplicate wins.
        let (calls, row) = same_name_duplicate_case(BAND_CHANNEL, BAND_ENTRY).await;
        assert_eq!(calls, 1, "the model was asked");
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
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);

        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            None,
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
            &MatchOutcome {
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
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);
        let error = run(
            &GuideApp::new(),
            &context,
            &settings,
            None,
            &world,
            &channels,
            &entries,
            true,
        )
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
        let settings = GuideSettings::default();
        let (channels, entries) = scoped(&world);
        let result = run(
            &GuideApp::new(),
            &context,
            &settings,
            None,
            &world,
            &channels,
            &entries,
            true,
        )
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
