//! The AI gate: when heuristics are unsure, ask a model.
//!
//! Two situations reach the model, and only two:
//!
//!   * **verify** — the best heuristic score lands inside the configured band. The score is
//!     high enough to be plausible and low enough to be wrong, which is exactly where a
//!     second opinion is worth a request.
//!   * **rescue** — no candidate reached the band, but some candidate scored above the rescue
//!     floor. The heuristics found nothing; the model may still see the match.
//!
//! A heuristic result at or above the match confidence threshold NEVER reaches the model: the
//! model may promote inside the band, but it never re-checks or vetoes a heuristic win.
//! Spending latency and tokens to re-check results the heuristics are already sure about would
//! make the tool slower without making it more accurate.
//!
//! One request carries the whole shortlist. TypeSafe answers every question against one state
//! in parallel, so asking about five candidates costs about as much as asking about one.
//!
//! The gate is generic over what is being matched ([`Query`]) and what it is matched against
//! ([`Candidate`]), because "is this the same channel?" is the same question whatever the
//! candidate is: an EPG entry for guide, a stream for streams, a logo set for lineup.

use crate::ai::{AiService, Answer, ChainAttempt, DecisionRequest, Question};
use crate::config::{AiChainEntry, AiVerifyConfig, CrosstalkConfig};
use crate::dispatcharr::Channel;
use crate::match_run::Candidate;
use crate::store::AiDecision;
use crosstalk_matching::callsign::KnownCallsigns;
use crosstalk_matching::callsign::{extract_callsign, extract_location, network_keyword};
use crosstalk_matching::pipeline::Scored;
use std::collections::BTreeMap;

/// The thing being matched: a channel now, a lineup entry later.
///
/// What the model is told about it comes from the shared payload built in [`build_request`],
/// so every app describes its query side the same way.
pub trait Query {
    fn name(&self) -> &str;
    /// The number the operator sees, printed as the reports print it.
    ///
    /// `None` when the query has no number: a lineup entry or a stream is still a query.
    fn number(&self) -> Option<f64>;
}

impl Query for Channel {
    fn name(&self) -> &str {
        &self.name
    }

    fn number(&self) -> Option<f64> {
        self.display_number()
    }
}

/// Why a channel reached the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateReason {
    /// The best heuristic score is inside the uncertain band.
    Verify,
    /// Nothing reached the band, but something reached the rescue floor.
    Rescue,
}

impl GateReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            GateReason::Verify => "verify",
            GateReason::Rescue => "rescue",
        }
    }
}

/// What the model decided.
#[derive(Debug, Clone, PartialEq)]
pub struct GateOutcome {
    /// Index into the candidate slice that was offered.
    pub accepted: Option<usize>,
    /// The reason the channel was sent at all.
    pub reason: GateReason,
    /// The model's pick, whether or not it was accepted.
    pub picked: Option<String>,
    /// Probability that the picked candidate is the same channel.
    pub probability: Option<f64>,
    /// Confidence in the pick, when the backend reports one.
    pub confidence: Option<f64>,
    /// A one-line summary for the CSV, for example
    /// `verify: espn 0.93 (accept)`.
    ///
    /// When the chain escalated, the trail is appended inside the parentheses, for example
    /// `verify: espn 0.93 (accept, llm/kimi-k3 after typesafe/jev-latest undecided)`.
    pub decision: String,
    /// The model that answered.
    pub model: String,
    pub calibrated: bool,
    pub latency_ms: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    /// The request and response, for the audit trail.
    pub request: serde_json::Value,
    pub response: serde_json::Value,
    /// Every chain link asked, in order. Empty for rows that never reached a provider.
    /// Earlier answered attempts were undecided by construction — a decided link stops
    /// the chain — and failed ones carry their error.
    pub attempts: Vec<ChainAttempt>,
    /// Whether an answer was actually obtained. A failed request leaves the channel undecided
    /// rather than failing the run.
    pub answered: bool,
    /// Whether cancellation stopped the chain between links.
    pub cancelled: bool,
}

impl GateOutcome {
    /// Convert to the store's decision row.
    ///
    /// The recorded provider is the link that decided — the last one that answered — not
    /// the shorthand pair from the settings, which may name a link that was never asked.
    /// `fallback_provider` covers rows that never reached a provider (skipped, cancelled
    /// before the first link) and keeps old callers compiling.
    pub fn to_store(&self, row_ordinal: i64, fallback_provider: &str) -> AiDecision {
        let decided = self
            .attempts
            .iter()
            .rposition(|attempt| attempt.response.is_some());
        let provider = decided
            .and_then(|position| self.attempts.get(position))
            .or(self.attempts.last())
            .map(|attempt| attempt.provider.clone())
            .unwrap_or_else(|| fallback_provider.to_string());
        AiDecision {
            row_ordinal,
            provider,
            model: self.model.clone(),
            latency_ms: self.latency_ms,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            request: self.request.clone(),
            response: self.response.clone(),
            attempts: self
                .attempts
                .iter()
                .enumerate()
                .map(|(position, attempt)| {
                    // A link whose answer stood stops the chain, so every earlier answered
                    // link was undecided; only the last answered one decided.
                    let result = if attempt.response.is_none() {
                        "failed"
                    } else if Some(position) == decided {
                        "decided"
                    } else {
                        "undecided"
                    };
                    serde_json::json!({
                        "provider": attempt.provider,
                        "model": attempt.model,
                        "result": result,
                        "error": attempt.error,
                    })
                })
                .collect(),
        }
    }

    /// One-line escalation summary for the run-row reason, for example
    /// `AI verified (llm/kimi-k3 after typesafe/jev-latest undecided)`.
    ///
    /// `None` when no escalation happened: single-link rows read exactly as before.
    pub fn chain_note(&self) -> Option<String> {
        if self.attempts.len() <= 1 || self.cancelled {
            return None;
        }
        let trail = chain_trail(&self.attempts);
        let state = if self.accepted.is_some() {
            "verified"
        } else if self.attempts.iter().any(|attempt| !attempt.failed()) {
            "undecided"
        } else {
            // Every link failed: the decision line already lists the errors, and the audit
            // row carries them link by link.
            "failed"
        };
        Some(format!("AI {state} ({trail})"))
    }

    /// The "no result" value: the run was cancelled before this channel was asked.
    pub fn cancelled(reason: GateReason, model: String) -> Self {
        Self {
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
            attempts: Vec::new(),
            answered: false,
            cancelled: true,
        }
    }
}

/// Which candidates a channel should offer the model, if any.
///
/// Returns the slice of `scored` worth sending, and why. Empty when the heuristics are
/// confident enough that a request would be wasted.
///
/// The shortlist holds one position per candidate NAME: listings routinely carry the same name
/// under several entries (regional duplicates for guide, mirrored streams for streams), and
/// offering the model the same name twice spends tokens without adding information. The first
/// position in score order wins each name, so the kept entry is the highest-scored one.
pub fn shortlist<C: Candidate>(
    scored: &[Scored],
    candidates: &[C],
    settings: &AiVerifyConfig,
) -> Option<(GateReason, Vec<usize>)> {
    if !settings.enabled || scored.is_empty() {
        return None;
    }

    let best = scored[0].score;
    let in_band = best >= settings.verify_band_low && best <= settings.verify_band_high;
    if in_band {
        return Some((
            GateReason::Verify,
            take_unique(scored, candidates, settings.shortlist),
        ));
    }

    if settings.rescue && best < settings.verify_band_low {
        let eligible: Vec<usize> = scored
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.score >= settings.rescue_floor)
            .map(|(position, _)| position)
            .collect();
        let offered = dedupe_names(scored, candidates, &eligible, settings.shortlist);
        if !offered.is_empty() {
            return Some((GateReason::Rescue, offered));
        }
    }

    None
}

/// Up to `limit` positions from the head of `scored`, in order, keeping the first
/// position seen for each candidate name.
fn take_unique<C: Candidate>(scored: &[Scored], candidates: &[C], limit: usize) -> Vec<usize> {
    let all: Vec<usize> = (0..scored.len().min(limit.max(1))).collect();
    dedupe_names(scored, candidates, &all, limit)
}

fn dedupe_names<C: Candidate>(
    scored: &[Scored],
    candidates: &[C],
    candidates_to_offer: &[usize],
    limit: usize,
) -> Vec<usize> {
    let mut seen = std::collections::HashSet::new();
    candidates_to_offer
        .iter()
        .copied()
        .filter(|position| {
            scored
                .get(*position)
                .and_then(|candidate| candidates.get(candidate.index))
                .is_some_and(|entry| seen.insert(entry.name().to_string()))
        })
        .take(limit.max(1))
        .collect()
}

/// Build the request: the query, the shortlist, and the questions.
///
/// The callsign is extracted with the run's allowlist, the same one the matcher scores with —
/// never an empty default, which would hide denylisted-but-real callsigns (like `KING`) from
/// the model while the matcher itself sees them.
///
/// `task` names the naming domain for a chat-shaped backend's prompt (`"epg"`, `"streams"`,
/// `"names"`); it travels on the request so the audit row shows what was asked, and a
/// decision-shaped backend ignores it.
pub fn build_request<Q: Query, C: Candidate>(
    query: &Q,
    candidates: &[C],
    scored: &[Scored],
    offered: &[usize],
    known: &KnownCallsigns,
    settings: &AiVerifyConfig,
    task: Option<&str>,
) -> DecisionRequest {
    let callsign = extract_callsign(query.name(), known);
    let location = extract_location(query.name());
    let network = network_keyword(query.name());

    let mut state_query = serde_json::json!({
        "name": query.name(),
        "number": query.number(),
    });
    if let Some(callsign) = callsign {
        state_query["callsign"] = serde_json::json!(callsign);
    }
    if let Some(state) = location.state {
        state_query["state"] = serde_json::json!(state);
    }
    if let Some(city) = location.city {
        state_query["city"] = serde_json::json!(city);
    }
    if let Some(network) = network {
        state_query["network"] = serde_json::json!(network);
    }

    let offered_candidates: Vec<serde_json::Value> = offered
        .iter()
        .filter_map(|position| {
            let candidate = scored.get(*position)?;
            let entry = candidates.get(candidate.index)?;
            let mut payload = serde_json::json!({
                "id": format!("candidate_{}", candidate.index),
                "name": entry.name(),
                "score": candidate.score,
                "method": candidate.method_label(),
            });
            for (key, value) in entry.request_fields() {
                payload[key] = value;
            }
            Some(payload)
        })
        .collect();

    let state = serde_json::json!({
        "channel": state_query,
        "candidates": offered_candidates,
    });

    let mut request =
        DecisionRequest::new(settings.provider.clone(), settings.model.clone(), state);
    if let Some(task) = task.map(str::trim).filter(|task| !task.is_empty()) {
        request = request.with_task(task);
    }

    let mut options = BTreeMap::new();
    options.insert(
        "none".to_string(),
        "None of the candidates is the same channel".to_string(),
    );

    for position in offered {
        let Some(candidate) = scored.get(*position) else {
            continue;
        };
        let Some(entry) = candidates.get(candidate.index) else {
            continue;
        };
        let id = format!("candidate_{}", candidate.index);
        request = request.ask(
            format!("same_{}", candidate.index),
            Question::noul_with(
                format!(
                    "Does candidates[id={id}] refer to the same television channel as channel?"
                ),
                "The same channel, ignoring only quality and language tags",
                "A different channel: a numbered sibling, a time-shifted feed, a regional \
                 variant, or a different network",
            ),
        );
        options.insert(
            id,
            format!("{} (heuristic score {})", entry.name(), candidate.score),
        );
    }

    request = request.ask(
        "best",
        Question::choice(
            "Which candidate is the same channel as channel? Choose none if none is.",
            options,
        ),
    );
    request.ask(
        "same_network",
        Question::noul_with(
            "Do channel and the best candidate belong to the same network or brand?",
            "Same network or brand",
            "Different networks or brands",
        ),
    )
}

/// Combine the answers into a decision.
pub fn combine<C: Candidate>(
    outcome: &mut GateOutcome,
    request: &DecisionRequest,
    response: &crate::ai::DecisionResponse,
    scored: &[Scored],
    candidates: &[C],
    offered: &[usize],
    settings: &AiVerifyConfig,
) {
    outcome.model = response.model.clone();
    outcome.calibrated = response.calibrated;
    outcome.latency_ms = response.latency_ms;
    outcome.input_tokens = response.input_tokens;
    outcome.output_tokens = response.output_tokens;
    outcome.answered = true;

    let picked = response
        .choice("best")
        .map(|(choice, confidence)| (choice.to_string(), confidence));
    let (picked_id, choice_confidence) = match picked {
        Some(picked) => picked,
        None => {
            outcome.decision = format!(
                "{}: no answer ({})",
                outcome.reason.as_str(),
                response.model
            );
            return;
        }
    };

    outcome.picked = Some(picked_id.clone());
    outcome.confidence = Some(choice_confidence);

    // The pick has to be one of the candidates that was offered, and its own yes-answer has to
    // agree. A model that names something it was not shown is not evidence of anything.
    let position = offered.iter().copied().find(|position| {
        scored
            .get(*position)
            .map(|candidate| format!("candidate_{}", candidate.index) == picked_id)
            .unwrap_or(false)
    });

    let Some(position) = position else {
        outcome.decision = format!(
            "{}: picked {:?} (not offered, so no action, confidence {:.2})",
            outcome.reason.as_str(),
            picked_id,
            choice_confidence
        );
        return;
    };

    let candidate = &scored[position];
    let Some(entry) = candidates.get(candidate.index) else {
        return;
    };
    let probability = response.noul(&format!("same_{}", candidate.index));
    outcome.probability = probability;

    // A missing yes-probability for the picked candidate is not evidence: the old code
    // defaulted it to 1.0, which auto-accepted a choice the model never backed with a
    // number. Undecided instead, and say which answer was missing.
    let Some(probability) = probability else {
        outcome.decision = format!(
            "{}: {} (undecided, missing same_{} probability, confidence {:.2})",
            outcome.reason.as_str(),
            entry.name(),
            candidate.index,
            choice_confidence
        );
        return;
    };

    // A missing choice confidence arrives as 0.0 (see the OpenAI-compatible backend),
    // which fails the `min_confidence` floor below instead of auto-passing it.
    let verdict = crate::ai::Verdict {
        probability,
        confidence: Some(choice_confidence),
    };

    let accepted = if verdict.reject(settings.reject_threshold) {
        false
    } else {
        verdict.accept(settings.accept_threshold, settings.min_confidence)
    };

    if accepted {
        outcome.accepted = Some(position);
        outcome.decision = format!(
            "{}: {} {:.2} (accept{})",
            outcome.reason.as_str(),
            entry.name(),
            probability,
            if response.calibrated {
                ""
            } else {
                ", uncalibrated"
            }
        );
    } else {
        outcome.decision = format!(
            "{}: {} {:.2} (undecided, confidence {:.2})",
            outcome.reason.as_str(),
            entry.name(),
            probability,
            choice_confidence
        );
    }

    let _ = request;
}

/// Ask the model about one channel, escalating down the `[ai.verify]` chain.
///
/// The first link is asked; when its outcome is *undecided* — the yes-probability lands
/// between the reject and accept thresholds, the answer is missing, or the choice
/// confidence is below `min_confidence` — or its call *failed*, the same request goes to
/// the next link, with only the provider and model rewritten. A confident accept or reject
/// is final, and later links are never asked.
///
/// `cancelled` is checked between links, never during one; `task` names the naming domain
/// for a chat-shaped backend's prompt. A failed chain is reported as `answered: false` and
/// never fails the run: an install whose AI endpoint is down should still match the
/// channels the heuristics are sure about.
#[allow(clippy::too_many_arguments)]
pub async fn ask<Q: Query, C: Candidate>(
    service: Option<&AiService>,
    query: &Q,
    candidates: &[C],
    scored: &[Scored],
    offered: &[usize],
    reason: GateReason,
    known: &KnownCallsigns,
    settings: &AiVerifyConfig,
    cancelled: impl Fn() -> bool,
    task: Option<&str>,
) -> GateOutcome {
    let chain = settings.effective_chain();
    let fallback = chain.first().cloned().unwrap_or(AiChainEntry {
        provider: settings.provider.clone(),
        model: settings.model.clone(),
    });
    let mut outcome = GateOutcome {
        accepted: None,
        reason,
        picked: None,
        probability: None,
        confidence: None,
        decision: format!("{}: skipped (no AI provider available)", reason.as_str()),
        model: fallback.model.clone(),
        calibrated: false,
        latency_ms: 0,
        input_tokens: 0,
        output_tokens: 0,
        request: serde_json::json!({}),
        response: serde_json::json!({}),
        attempts: Vec::new(),
        answered: false,
        cancelled: false,
    };

    let Some(service) = service else {
        return outcome;
    };
    let mut request = build_request(query, candidates, scored, offered, known, settings, task);
    // Address the audited request at the link asked first: with a chain set, the shorthand
    // pair is an untouched default that may name a provider the chain never asks.
    request.provider = fallback.provider.clone();
    request.model = fallback.model.clone();
    outcome.request = serde_json::to_value(&request).unwrap_or_default();

    let chain_outcome = service
        .decide_chain(
            &chain,
            &request,
            |response| {
                link_result(response, &request, scored, candidates, offered, settings)
                    != "undecided"
            },
            &cancelled,
        )
        .await;
    outcome.attempts = chain_outcome.attempts;

    if chain_outcome.cancelled {
        outcome.cancelled = true;
        outcome.model = outcome
            .attempts
            .last()
            .map(|attempt| attempt.model.clone())
            .unwrap_or(fallback.model);
        outcome.decision = format!(
            "{}: cancelled by the operator after {} attempt(s)",
            reason.as_str(),
            outcome.attempts.len(),
        );
        return outcome;
    }

    let Some(deciding) = outcome
        .attempts
        .iter()
        .rev()
        .find(|attempt| attempt.response.is_some())
        .and_then(|attempt| attempt.response.clone())
    else {
        // A single link fails exactly as the gate always did, so a shorthand install's
        // decision line and audit response read as before the chain existed.
        if let [only] = outcome.attempts.as_slice() {
            let error = only.error.as_deref().unwrap_or("unknown error");
            outcome.decision = format!("{}: request failed ({error})", reason.as_str());
            outcome.response = serde_json::json!({"error": error});
            return outcome;
        }
        let failures: Vec<String> = outcome
            .attempts
            .iter()
            .map(|attempt| {
                format!(
                    "{}/{}: {}",
                    attempt.provider,
                    attempt.model,
                    attempt.error.as_deref().unwrap_or("unknown error")
                )
            })
            .collect();
        outcome.decision = if failures.is_empty() {
            format!("{}: no chain links configured", reason.as_str())
        } else {
            format!(
                "{}: request failed ({})",
                reason.as_str(),
                failures.join("; ")
            )
        };
        outcome.response = serde_json::json!({"errors": failures});
        return outcome;
    };

    outcome.response = serde_json::to_value(&deciding).unwrap_or_default();
    combine(
        &mut outcome,
        &request,
        &deciding,
        scored,
        candidates,
        offered,
        settings,
    );
    if outcome.attempts.len() > 1 {
        outcome.decision = with_chain_suffix(&outcome.decision, &outcome.attempts);
    }
    outcome
}

/// One link's answer, as the escalation rule reads it: a confident accept or reject
/// decides, and everything else — between the thresholds, missing, low confidence, or a
/// pick outside the shortlist — hands the same question to the next link.
fn link_result<C: Candidate>(
    response: &crate::ai::DecisionResponse,
    request: &DecisionRequest,
    scored: &[Scored],
    candidates: &[C],
    offered: &[usize],
    settings: &AiVerifyConfig,
) -> &'static str {
    let mut probe = GateOutcome {
        accepted: None,
        reason: GateReason::Verify,
        picked: None,
        probability: None,
        confidence: None,
        decision: String::new(),
        model: String::new(),
        calibrated: false,
        latency_ms: 0,
        input_tokens: 0,
        output_tokens: 0,
        request: serde_json::json!({}),
        response: serde_json::json!({}),
        attempts: Vec::new(),
        answered: false,
        cancelled: false,
    };
    combine(
        &mut probe, request, response, scored, candidates, offered, settings,
    );
    // A reject is final only when the pick it rejects clears the confidence floor too: a
    // low-confidence "no" is as undecided as a low-confidence "yes".
    if probe.accepted.is_some() {
        "accept"
    } else if probe
        .probability
        .is_some_and(|probability| probability <= settings.reject_threshold)
        && probe
            .confidence
            .is_some_and(|confidence| confidence >= settings.min_confidence)
    {
        "reject"
    } else {
        "undecided"
    }
}

/// Append the escalation trail inside a decision line's parentheses, for example
/// `verify: espn 0.93 (accept)` becomes
/// `verify: espn 0.93 (accept, llm/kimi-k3 after typesafe/jev-latest undecided)`.
///
/// Only called for escalated outcomes: a single link keeps its line untouched, so installs
/// without a chain read exactly as before. Every answered attempt before the last was
/// undecided by construction — a decided link stops the chain — and failed ones say so.
fn with_chain_suffix(decision: &str, attempts: &[ChainAttempt]) -> String {
    let trail = chain_trail(attempts);
    match decision.strip_suffix(')') {
        Some(inner) => format!("{inner}, {trail})"),
        None => format!("{decision} ({trail})"),
    }
}

/// Who decided after whom, for example `llm/kimi-k3 after typesafe/jev-latest undecided`,
/// or every link marked failed when none answered.
fn chain_trail(attempts: &[ChainAttempt]) -> String {
    let decided = attempts
        .iter()
        .rposition(|attempt| attempt.response.is_some());
    match decided {
        Some(position) => {
            let decider = &attempts[position];
            let priors: Vec<String> = attempts[..position]
                .iter()
                .map(|attempt| {
                    let state = if attempt.failed() {
                        "failed"
                    } else {
                        "undecided"
                    };
                    format!("{}/{} {state}", attempt.provider, attempt.model)
                })
                .collect();
            if priors.is_empty() {
                format!("{}/{}", decider.provider, decider.model)
            } else {
                format!(
                    "{}/{} after {}",
                    decider.provider,
                    decider.model,
                    priors.join(", ")
                )
            }
        }
        None => attempts
            .iter()
            .map(|attempt| format!("{}/{} failed", attempt.provider, attempt.model))
            .collect::<Vec<_>>()
            .join(", "),
    }
}

/// The steps that make a gate outcome usable by a caller.
pub fn is_answered(outcome: &GateOutcome) -> bool {
    outcome.answered
}

/// The companion answer for the report: whether the model saw the same network.
pub fn network_agreement(response: &crate::ai::DecisionResponse) -> Option<String> {
    match response.answers.get("same_network") {
        Some(Answer::Noul { noul }) => Some(format!("network {noul:.2}")),
        _ => None,
    }
}

/// The AI service a run uses, if the policy is on and any of its chain links is configured.
///
/// An enabled gate with no configured link is a warning rather than an error: the run
/// proceeds heuristic-only, which is what the plugin does when its endpoint is unavailable.
/// A link whose provider is missing degrades the same way at ask time — a failed attempt
/// that hands the question on — so one bad link never fails the run.
pub fn service(config: &CrosstalkConfig) -> Option<AiService> {
    let policy = &config.ai.verify;
    if !policy.enabled {
        return None;
    }
    let service = AiService::from_config(config);
    let chain = policy.effective_chain();
    let missing: Vec<String> = chain
        .iter()
        .filter(|entry| !service.has(&entry.provider))
        .map(|entry| format!("{}/{}", entry.provider, entry.model))
        .collect();
    if missing.len() == chain.len() {
        tracing::warn!(
            "ai.verify.enabled is on but none of its providers are configured ({}); \
             matching without them",
            missing.join(", ")
        );
        return None;
    }
    if !missing.is_empty() {
        tracing::warn!(
            "ai.verify chain links {} are not configured; they will fail and hand on",
            missing.join(", ")
        );
    }
    Some(service)
}

/// The concurrent bound for the AI fan-out: the configured provider's own cap.
///
/// The single source of truth is `[ai.providers.<name>].max_concurrency` in the host config,
/// which both backends also enforce on their own HTTP semaphores. An unknown provider name
/// falls back to the default cap (8); validation enforces a minimum of 1.
pub fn fanout_bound(config: &CrosstalkConfig, provider: &str) -> usize {
    config
        .ai
        .providers
        .get(provider)
        .map(|provider| provider.max_concurrency.max(1))
        .unwrap_or(8)
}

/// The concurrent bound for the AI fan-out over a whole chain: the highest cap among the
/// chain's links, so the roomiest link still gets its share and a single-link chain reads
/// exactly the provider's own cap.
pub fn fanout_bound_for(config: &CrosstalkConfig, chain: &[AiChainEntry]) -> usize {
    chain
        .iter()
        .map(|entry| fanout_bound(config, &entry.provider))
        .max()
        .unwrap_or(8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcharr::EpgEntry;
    use crosstalk_matching::callsign::KnownCallsigns;
    use crosstalk_matching::pipeline::MatchMethod;

    fn scored(score: u8, index: usize) -> Scored {
        Scored {
            index,
            score,
            components: vec!["Fuzzy"],
        }
    }

    fn settings() -> AiVerifyConfig {
        AiVerifyConfig {
            enabled: true,
            provider: "typesafe".to_string(),
            model: "jev-latest".to_string(),
            chain: Vec::new(),
            verify_band_low: 80,
            verify_band_high: 94,
            rescue: true,
            rescue_floor: 60,
            shortlist: 3,
            accept_threshold: 0.8,
            reject_threshold: 0.3,
            min_confidence: 0.5,
        }
    }

    fn channel() -> Channel {
        Channel {
            id: 1,
            name: "ABC 7 (KGO) San Francisco".to_string(),
            ..Channel::default()
        }
    }

    fn entry(id: i64, name: &str) -> EpgEntry {
        EpgEntry {
            id,
            name: name.to_string(),
            tvg_id: format!("tvg{id}"),
            epg_source: Some(4),
        }
    }

    fn named_entries(names: &[&str]) -> Vec<EpgEntry> {
        names
            .iter()
            .enumerate()
            .map(|(position, name)| entry(position as i64, name))
            .collect()
    }

    #[test]
    fn a_confident_match_never_reaches_the_model() {
        let entries = named_entries(&["A", "B"]);
        let ranked = vec![scored(100, 0), scored(70, 1)];
        assert_eq!(shortlist(&ranked, &entries, &settings()), None);

        let ranked = vec![scored(95, 0)];
        assert_eq!(shortlist(&ranked, &entries, &settings()), None);
    }

    #[test]
    fn a_score_inside_the_band_asks_for_verification() {
        let entries = named_entries(&["A", "B", "C", "D"]);
        let ranked = vec![scored(90, 0), scored(85, 1), scored(70, 2), scored(65, 3)];
        let (reason, offered) = shortlist(&ranked, &entries, &settings()).expect("asks");
        assert_eq!(reason, GateReason::Verify);
        // Bounded by the shortlist size.
        assert_eq!(offered, vec![0, 1, 2]);
    }

    #[test]
    fn a_low_score_with_nothing_in_the_band_asks_for_rescue() {
        let entries = named_entries(&["A", "B", "C"]);
        let ranked = vec![scored(70, 0), scored(65, 1), scored(40, 2)];
        let (reason, offered) = shortlist(&ranked, &entries, &settings()).expect("asks");
        assert_eq!(reason, GateReason::Rescue);
        assert_eq!(
            offered,
            vec![0, 1],
            "only candidates above the rescue floor"
        );
    }

    #[test]
    fn nothing_above_the_rescue_floor_is_not_sent() {
        let entries = named_entries(&["A", "B"]);
        let ranked = vec![scored(40, 0), scored(20, 1)];
        assert_eq!(shortlist(&ranked, &entries, &settings()), None);
    }

    #[test]
    fn rescue_can_be_turned_off() {
        let entries = named_entries(&["A"]);
        let ranked = vec![scored(70, 0)];
        let settings = AiVerifyConfig {
            rescue: false,
            ..settings()
        };
        assert_eq!(shortlist(&ranked, &entries, &settings), None);
    }

    #[test]
    fn disabled_ai_sends_nothing() {
        let entries = named_entries(&["A"]);
        let ranked = vec![scored(90, 0)];
        let settings = AiVerifyConfig {
            enabled: false,
            ..settings()
        };
        assert_eq!(shortlist(&ranked, &entries, &settings), None);
    }

    #[test]
    fn duplicate_names_are_offered_once_with_the_best_score_first() {
        // Two guide entries share a name; the shortlist must not spend two slots on it.
        let entries = vec![
            entry(10, "ESPN"),
            entry(11, "ESPN"),
            entry(12, "ESPN 2"),
            entry(13, "ESPN 2"),
        ];
        let ranked = vec![scored(90, 0), scored(89, 1), scored(88, 2), scored(87, 3)];
        let (reason, offered) = shortlist(&ranked, &entries, &settings()).expect("asks");
        assert_eq!(reason, GateReason::Verify);
        assert_eq!(offered, vec![0, 2], "one position per name, best first");
    }

    #[test]
    fn rescue_dedupes_names_too() {
        let entries = vec![entry(10, "ESPN"), entry(11, "ESPN"), entry(12, "Other")];
        let ranked = vec![scored(70, 0), scored(69, 1), scored(65, 2)];
        let (reason, offered) = shortlist(&ranked, &entries, &settings()).expect("asks");
        assert_eq!(reason, GateReason::Rescue);
        assert_eq!(offered, vec![0, 2]);
    }

    #[test]
    fn the_request_carries_the_channel_and_the_shortlist() {
        let entries = vec![entry(10, "ABC San Francisco"), entry(11, "ABC 7")];
        let ranked = vec![scored(90, 0), scored(88, 1)];
        let request = build_request(
            &channel(),
            &entries,
            &ranked,
            &[0, 1],
            &KnownCallsigns::default(),
            &settings(),
            None,
        );

        assert_eq!(request.provider, "typesafe");
        assert_eq!(request.model, "jev-latest");
        assert_eq!(
            request.state["channel"]["name"],
            "ABC 7 (KGO) San Francisco"
        );
        assert_eq!(request.state["channel"]["callsign"], "KGO");
        assert_eq!(request.state["candidates"][0]["name"], "ABC San Francisco");
        assert_eq!(
            request.state["candidates"][0]["tvg_id"], "tvg10",
            "the model needs the tvg_id to tell same-name entries apart"
        );
        assert_eq!(request.state["candidates"][0]["epg_source"], 4);
        // A noul and a choice per candidate, plus the companion question.
        assert!(request.questions.contains_key("same_0"));
        assert!(request.questions.contains_key("same_1"));
        assert!(request.questions.contains_key("best"));
        assert!(request.questions.contains_key("same_network"));
    }

    fn response(
        picked: &str,
        probability: Option<f64>,
        confidence: f64,
    ) -> crate::ai::DecisionResponse {
        let mut answers = BTreeMap::new();
        answers.insert(
            "best".to_string(),
            Answer::Choice {
                choice: picked.to_string(),
                confidence,
                probabilities: BTreeMap::new(),
            },
        );
        if let Some(probability) = probability {
            answers.insert("same_0".to_string(), Answer::Noul { noul: probability });
        }
        crate::ai::DecisionResponse {
            model: "jev-1.14.0".to_string(),
            answers,
            input_tokens: 400,
            output_tokens: 10,
            latency_ms: 120,
            calibrated: true,
        }
    }

    fn outcome() -> GateOutcome {
        GateOutcome {
            accepted: None,
            reason: GateReason::Verify,
            picked: None,
            probability: None,
            confidence: None,
            decision: String::new(),
            model: String::new(),
            calibrated: false,
            latency_ms: 0,
            input_tokens: 0,
            output_tokens: 0,
            request: serde_json::json!({}),
            response: serde_json::json!({}),
            attempts: Vec::new(),
            answered: false,
            cancelled: false,
        }
    }

    #[test]
    fn a_confident_agreeing_answer_is_accepted() {
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        let request = build_request(
            &channel(),
            &entries,
            &ranked,
            &[0],
            &KnownCallsigns::default(),
            &settings(),
            None,
        );
        let response = response("candidate_0", Some(0.93), 0.9);

        let mut result = outcome();
        combine(
            &mut result,
            &request,
            &response,
            &ranked,
            &entries,
            &[0],
            &settings(),
        );
        assert_eq!(result.accepted, Some(0));
        assert!(result.decision.contains("accept"), "{}", result.decision);
        assert!(result.decision.contains("ABC San Francisco"));
        assert_eq!(result.model, "jev-1.14.0");
        assert!(result.calibrated);
    }

    #[test]
    fn a_rejected_probability_is_not_accepted() {
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        let request = build_request(
            &channel(),
            &entries,
            &ranked,
            &[0],
            &KnownCallsigns::default(),
            &settings(),
            None,
        );
        let response = response("candidate_0", Some(0.2), 0.9);

        let mut result = outcome();
        combine(
            &mut result,
            &request,
            &response,
            &ranked,
            &entries,
            &[0],
            &settings(),
        );
        assert_eq!(result.accepted, None);
    }

    #[test]
    fn an_undecided_answer_leaves_the_channel_unmatched() {
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        let request = build_request(
            &channel(),
            &entries,
            &ranked,
            &[0],
            &KnownCallsigns::default(),
            &settings(),
            None,
        );
        // Probability says yes, but the choice confidence does not clear the floor.
        let response = response("candidate_0", Some(0.9), 0.2);

        let mut result = outcome();
        combine(
            &mut result,
            &request,
            &response,
            &ranked,
            &entries,
            &[0],
            &settings(),
        );
        assert_eq!(result.accepted, None);
        assert!(result.decision.contains("undecided"), "{}", result.decision);
    }

    #[test]
    fn a_choice_without_its_yes_probability_is_undecided_not_accepted() {
        // An OpenAI-shaped reply with only {"best": ...} and no confidences/noul:
        // the pick names a candidate but no number backs it, so it must not accept.
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        let request = build_request(
            &channel(),
            &entries,
            &ranked,
            &[0],
            &KnownCallsigns::default(),
            &settings(),
            None,
        );
        let response = response("candidate_0", None, 0.0);

        let mut result = outcome();
        combine(
            &mut result,
            &request,
            &response,
            &ranked,
            &entries,
            &[0],
            &settings(),
        );
        assert_eq!(result.accepted, None);
        assert!(result.decision.contains("undecided"), "{}", result.decision);
        assert!(result.decision.contains("missing"), "{}", result.decision);
        assert!(
            result.decision.contains("same_0"),
            "says which answer was missing: {}",
            result.decision
        );
    }

    #[test]
    fn a_missing_confidence_fails_the_floor_even_with_a_high_probability() {
        // The number says yes but no confidence was reported (0.0): that fails the
        // `min_confidence` floor rather than passing it.
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        let request = build_request(
            &channel(),
            &entries,
            &ranked,
            &[0],
            &KnownCallsigns::default(),
            &settings(),
            None,
        );
        let response = response("candidate_0", Some(0.93), 0.0);

        let mut result = outcome();
        combine(
            &mut result,
            &request,
            &response,
            &ranked,
            &entries,
            &[0],
            &settings(),
        );
        assert_eq!(result.accepted, None);
        assert!(result.decision.contains("undecided"), "{}", result.decision);
    }

    #[test]
    fn an_explicit_confidence_with_a_probability_still_accepts() {
        // The OpenAI shape with both numbers reported behaves as before.
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        let request = build_request(
            &channel(),
            &entries,
            &ranked,
            &[0],
            &KnownCallsigns::default(),
            &settings(),
            None,
        );
        let mut response = response("candidate_0", Some(0.93), 0.9);
        response.calibrated = false;

        let mut result = outcome();
        combine(
            &mut result,
            &request,
            &response,
            &ranked,
            &entries,
            &[0],
            &settings(),
        );
        assert_eq!(result.accepted, Some(0));
        assert!(result.decision.contains("accept"), "{}", result.decision);
    }

    #[test]
    fn a_pick_outside_the_shortlist_is_ignored() {
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        let request = build_request(
            &channel(),
            &entries,
            &ranked,
            &[0],
            &KnownCallsigns::default(),
            &settings(),
            None,
        );
        let response = response("candidate_99", Some(0.99), 0.99);

        let mut result = outcome();
        combine(
            &mut result,
            &request,
            &response,
            &ranked,
            &entries,
            &[0],
            &settings(),
        );
        assert_eq!(result.accepted, None);
        assert!(
            result.decision.contains("not offered"),
            "{}",
            result.decision
        );
    }

    #[test]
    fn picking_none_accepts_nothing() {
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        let request = build_request(
            &channel(),
            &entries,
            &ranked,
            &[0],
            &KnownCallsigns::default(),
            &settings(),
            None,
        );
        let response = response("none", Some(0.9), 0.9);

        let mut result = outcome();
        combine(
            &mut result,
            &request,
            &response,
            &ranked,
            &entries,
            &[0],
            &settings(),
        );
        assert_eq!(result.accepted, None);
    }

    #[test]
    fn the_store_row_carries_the_request_and_response() {
        let mut result = outcome();
        result.model = "jev-latest".to_string();
        result.request = serde_json::json!({"state": "x"});
        result.response = serde_json::json!({"answers": {}});
        let stored = result.to_store(3, "typesafe");
        assert_eq!(stored.row_ordinal, 3);
        assert_eq!(stored.provider, "typesafe");
        assert_eq!(stored.model, "jev-latest");
        assert_eq!(stored.request["state"], "x");
    }

    #[tokio::test]
    async fn without_a_service_the_channel_is_marked_skipped() {
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        let result = ask(
            None,
            &channel(),
            &entries,
            &ranked,
            &[0],
            GateReason::Verify,
            &KnownCallsigns::default(),
            &settings(),
            || false,
            Some("epg"),
        )
        .await;
        assert!(!is_answered(&result));
        assert!(result.decision.contains("no AI provider"));
        assert_eq!(result.accepted, None);
    }

    #[test]
    fn the_request_uses_the_run_allowlist_for_the_callsign() {
        // `KING` is denylisted as an ordinary English word, so an empty allowlist hides
        // it — exactly the bug this guards: the request must see what the matcher sees.
        let entries = vec![entry(10, "KING 5 News")];
        let ranked = vec![scored(90, 0)];
        let king_channel = Channel {
            id: 1,
            name: "Evening News (KING)".to_string(),
            ..Channel::default()
        };

        let empty = KnownCallsigns::default();
        let denied = build_request(
            &king_channel,
            &entries,
            &ranked,
            &[0],
            &empty,
            &settings(),
            None,
        );
        assert_eq!(
            denied.state["channel"].get("callsign"),
            None,
            "an empty allowlist hides the denylisted callsign"
        );

        let mut allowlist = KnownCallsigns::default();
        allowlist.insert("KING");
        let request = build_request(
            &king_channel,
            &entries,
            &ranked,
            &[0],
            &allowlist,
            &settings(),
            None,
        );
        assert_eq!(
            request.state["channel"]["callsign"], "KING",
            "a denylisted-but-real callsign vouched for by the allowlist is told to the model"
        );
        assert_eq!(
            request.state["channel"]["callsign"],
            crosstalk_matching::callsign::extract_callsign(&king_channel.name, &allowlist)
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
            "the request callsign matches the matcher's own extraction"
        );
    }

    #[test]
    fn the_method_is_not_lost() {
        // Guards the report: the AI decision must not replace the heuristic explanation.
        let candidate = Scored {
            index: 0,
            score: 90,
            components: vec!["Alias"],
        };
        assert_eq!(candidate.method_label(), "Alias");
        assert_eq!(MatchMethod::Alias.component(), "Alias");
    }

    #[test]
    fn the_ai_fanout_bound_comes_from_the_provider_config() {
        let mut config = CrosstalkConfig::default();
        assert_eq!(
            fanout_bound(&config, "typesafe"),
            8,
            "an unconfigured provider falls back to the default cap"
        );

        config.ai.providers.insert(
            "typesafe".to_string(),
            crate::config::AiProviderConfig {
                kind: "typesafe".to_string(),
                max_concurrency: 3,
                ..crate::config::AiProviderConfig::default()
            },
        );
        assert_eq!(
            fanout_bound(&config, "typesafe"),
            3,
            "the provider's own max_concurrency bounds the fan-out"
        );

        config.ai.providers.insert(
            "zero".to_string(),
            crate::config::AiProviderConfig {
                kind: "typesafe".to_string(),
                max_concurrency: 0,
                ..crate::config::AiProviderConfig::default()
            },
        );
        assert_eq!(
            fanout_bound(&config, "zero"),
            1,
            "a zero cap still allows one request rather than deadlocking"
        );
    }

    /// A decider scripted for gate-shaped requests: picks the first offered candidate with
    /// a fixed yes-probability and confidence — or fails — and counts its calls.
    struct GateDecider {
        probability: f64,
        confidence: f64,
        model: String,
        fail: bool,
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        trip_cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    }

    #[async_trait::async_trait]
    impl crate::ai::Decider for GateDecider {
        fn provider(&self) -> &str {
            "gate"
        }

        fn calibrated(&self) -> bool {
            true
        }

        async fn decide(
            &self,
            request: &crate::ai::DecisionRequest,
        ) -> anyhow::Result<crate::ai::DecisionResponse> {
            use std::sync::atomic::Ordering;
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(flag) = &self.trip_cancel {
                flag.store(true, Ordering::SeqCst);
            }
            if self.fail {
                anyhow::bail!("the endpoint is down");
            }
            let mut answers = BTreeMap::new();
            if let Some((id, _)) = request
                .questions
                .iter()
                .find(|(id, _)| id.starts_with("same_") && id.as_str() != "same_network")
            {
                answers.insert(
                    "best".to_string(),
                    Answer::Choice {
                        choice: id.replacen("same_", "candidate_", 1),
                        confidence: self.confidence,
                        probabilities: BTreeMap::new(),
                    },
                );
                answers.insert(
                    id.clone(),
                    Answer::Noul {
                        noul: self.probability,
                    },
                );
            }
            Ok(crate::ai::DecisionResponse {
                model: self.model.clone(),
                answers,
                input_tokens: 10,
                output_tokens: 2,
                latency_ms: 1,
                calibrated: true,
            })
        }
    }

    fn gate_service(
        first: (f64, f64, bool),
        second: (f64, f64, bool),
        first_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        second_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        trip_cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ) -> AiService {
        AiService::with_deciders(vec![
            (
                "typesafe".to_string(),
                std::sync::Arc::new(GateDecider {
                    probability: first.0,
                    confidence: first.1,
                    model: "jev-1".to_string(),
                    fail: first.2,
                    calls: first_calls,
                    trip_cancel,
                }),
            ),
            (
                "llm".to_string(),
                std::sync::Arc::new(GateDecider {
                    probability: second.0,
                    confidence: second.1,
                    model: "kimi-k3-1".to_string(),
                    fail: second.2,
                    calls: second_calls,
                    trip_cancel: None,
                }),
            ),
        ])
    }

    fn chain_settings() -> AiVerifyConfig {
        AiVerifyConfig {
            enabled: true,
            chain: vec![
                crate::config::AiChainEntry::new("typesafe", "jev-latest"),
                crate::config::AiChainEntry::new("llm", "kimi-k3"),
            ],
            ..settings()
        }
    }

    async fn ask_chain(
        service: &AiService,
        settings: &AiVerifyConfig,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> GateOutcome {
        use std::sync::atomic::Ordering;
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        ask(
            Some(service),
            &channel(),
            &entries,
            &ranked,
            &[0],
            GateReason::Verify,
            &KnownCallsigns::default(),
            settings,
            || cancelled.load(Ordering::SeqCst),
            Some("epg"),
        )
        .await
    }

    fn no_cancel() -> std::sync::atomic::AtomicBool {
        std::sync::atomic::AtomicBool::new(false)
    }

    #[tokio::test]
    async fn an_undecided_first_link_escalates_to_the_second() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let first_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        // Jev says 0.5: between the thresholds, so undecided. The LLM says 0.93.
        let service = gate_service(
            (0.5, 0.9, false),
            (0.93, 0.9, false),
            std::sync::Arc::clone(&first_calls),
            std::sync::Arc::clone(&second_calls),
            None,
        );
        let cancel = no_cancel();
        let outcome = ask_chain(&service, &chain_settings(), &cancel).await;

        assert_eq!(outcome.accepted, Some(0));
        assert!(outcome.answered);
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
        assert_eq!(outcome.attempts.len(), 2);
        assert!(
            outcome
                .decision
                .contains("accept, llm/kimi-k3 after typesafe/jev-latest undecided"),
            "{}",
            outcome.decision
        );
        assert_eq!(
            outcome.chain_note().as_deref(),
            Some("AI verified (llm/kimi-k3 after typesafe/jev-latest undecided)")
        );
        // The audit row names the link that decided and the one that could not.
        let stored = outcome.to_store(0, "typesafe");
        assert_eq!(stored.provider, "llm");
        assert_eq!(stored.model, "kimi-k3-1", "the answering version is kept");
        assert_eq!(
            stored.attempts,
            serde_json::json!([
                {"provider": "typesafe", "model": "jev-latest", "result": "undecided", "error": None::<String>},
                {"provider": "llm", "model": "kimi-k3", "result": "decided", "error": None::<String>},
            ])
        );
    }

    #[tokio::test]
    async fn a_confident_first_link_never_asks_the_second() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let first_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = gate_service(
            (0.93, 0.9, false),
            (0.95, 0.9, false),
            std::sync::Arc::clone(&first_calls),
            std::sync::Arc::clone(&second_calls),
            None,
        );
        let cancel = no_cancel();
        let outcome = ask_chain(&service, &chain_settings(), &cancel).await;

        assert_eq!(outcome.accepted, Some(0));
        assert_eq!(second_calls.load(Ordering::SeqCst), 0, "never asked");
        assert!(!outcome.decision.contains("after"), "{}", outcome.decision);
        assert_eq!(outcome.chain_note(), None, "no escalation, no trail");
        let stored = outcome.to_store(0, "typesafe");
        assert_eq!(stored.provider, "typesafe");
    }

    #[tokio::test]
    async fn a_failed_first_link_hands_the_question_on() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = gate_service(
            (0.0, 0.0, true),
            (0.93, 0.9, false),
            Default::default(),
            std::sync::Arc::clone(&second_calls),
            None,
        );
        let cancel = no_cancel();
        let outcome = ask_chain(&service, &chain_settings(), &cancel).await;

        assert_eq!(outcome.accepted, Some(0));
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
        assert!(
            outcome
                .decision
                .contains("accept, llm/kimi-k3 after typesafe/jev-latest failed"),
            "{}",
            outcome.decision
        );
        assert_eq!(
            outcome.chain_note().as_deref(),
            Some("AI verified (llm/kimi-k3 after typesafe/jev-latest failed)")
        );
    }

    #[tokio::test]
    async fn all_links_undecided_leaves_the_channel_unmatched_with_every_attempt_recorded() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let first_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = gate_service(
            (0.5, 0.9, false),
            (0.6, 0.9, false),
            std::sync::Arc::clone(&first_calls),
            std::sync::Arc::clone(&second_calls),
            None,
        );
        let cancel = no_cancel();
        let outcome = ask_chain(&service, &chain_settings(), &cancel).await;

        assert_eq!(outcome.accepted, None);
        assert!(outcome.answered, "the last answer stands, even undecided");
        assert_eq!(outcome.attempts.len(), 2);
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
        assert!(
            outcome.decision.contains("undecided"),
            "{}",
            outcome.decision
        );
        assert_eq!(
            outcome.chain_note().as_deref(),
            Some("AI undecided (llm/kimi-k3 after typesafe/jev-latest undecided)")
        );
        let stored = outcome.to_store(0, "typesafe");
        assert_eq!(stored.provider, "llm", "the last link answered last");
        assert_eq!(stored.attempts[0]["result"], "undecided");
        assert_eq!(stored.attempts[1]["result"], "decided");
    }

    #[tokio::test]
    async fn all_links_failed_is_unanswered_not_a_run_failure() {
        let service = gate_service(
            (0.0, 0.0, true),
            (0.0, 0.0, true),
            Default::default(),
            Default::default(),
            None,
        );
        let cancel = no_cancel();
        let outcome = ask_chain(&service, &chain_settings(), &cancel).await;

        assert!(!outcome.answered);
        assert_eq!(outcome.accepted, None);
        assert_eq!(outcome.attempts.len(), 2);
        assert!(
            outcome.decision.contains("typesafe/jev-latest")
                && outcome.decision.contains("llm/kimi-k3"),
            "{}",
            outcome.decision
        );
        assert!(
            outcome.decision.contains("request failed"),
            "{}",
            outcome.decision
        );
    }

    #[tokio::test]
    async fn cancellation_between_links_stops_the_chain() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let cancel = std::sync::Arc::new(AtomicBool::new(false));
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        // The first link leaves the question undecided and trips the cancel as its answer
        // lands: the second link is never asked.
        let service = gate_service(
            (0.5, 0.9, false),
            (0.93, 0.9, false),
            Default::default(),
            std::sync::Arc::clone(&second_calls),
            Some(std::sync::Arc::clone(&cancel)),
        );
        let outcome = ask_chain(&service, &chain_settings(), &cancel).await;

        assert!(outcome.cancelled);
        assert!(!outcome.answered);
        assert_eq!(outcome.attempts.len(), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 0);
        assert!(
            outcome.decision.contains("cancelled by the operator"),
            "{}",
            outcome.decision
        );
        assert_eq!(
            outcome.chain_note(),
            None,
            "a cancelled row is about the cancel"
        );
    }

    #[tokio::test]
    async fn the_task_travels_on_the_request_for_the_audit_trail() {
        let service = gate_service(
            (0.93, 0.9, false),
            (0.93, 0.9, false),
            Default::default(),
            Default::default(),
            None,
        );
        let cancel = no_cancel();
        let outcome = ask_chain(&service, &chain_settings(), &cancel).await;
        assert_eq!(outcome.request["task"], "epg");

        // And a request built without a task carries no task key at all.
        let entries = vec![entry(10, "ABC San Francisco")];
        let ranked = vec![scored(90, 0)];
        let request = build_request(
            &channel(),
            &entries,
            &ranked,
            &[0],
            &KnownCallsigns::default(),
            &settings(),
            None,
        );
        let value = serde_json::to_value(&request).expect("serialises");
        assert!(value.get("task").is_none(), "{value}");
    }

    #[test]
    fn the_chain_fanout_is_the_roomiest_links_cap() {
        let mut config = CrosstalkConfig::default();
        config.ai.providers.insert(
            "typesafe".to_string(),
            crate::config::AiProviderConfig {
                kind: "typesafe".to_string(),
                max_concurrency: 3,
                ..crate::config::AiProviderConfig::default()
            },
        );
        config.ai.providers.insert(
            "llm".to_string(),
            crate::config::AiProviderConfig {
                kind: "openai_compat".to_string(),
                base_url: "http://gpu-box:8000/v1".to_string(),
                max_concurrency: 2,
                ..crate::config::AiProviderConfig::default()
            },
        );
        assert_eq!(
            fanout_bound_for(&config, &chain_settings().effective_chain()),
            3
        );
        assert_eq!(
            fanout_bound_for(&config, &[]),
            8,
            "no links, the default cap"
        );
    }

    #[tokio::test]
    async fn a_single_failed_link_reads_exactly_as_before_the_chain() {
        // The shorthand pair is a one-link chain: its failure line and audit response keep
        // the pre-chain shape, so a shorthand install's CSV and audit rows are unchanged.
        let service = gate_service(
            (0.0, 0.0, true),
            (0.0, 0.0, true),
            Default::default(),
            Default::default(),
            None,
        );
        let cancel = no_cancel();
        let outcome = ask_chain(&service, &settings(), &cancel).await;

        assert!(!outcome.answered);
        assert_eq!(
            outcome.decision,
            "verify: request failed (the endpoint is down)"
        );
        assert_eq!(
            outcome.response,
            serde_json::json!({"error": "the endpoint is down"})
        );
        assert_eq!(outcome.chain_note(), None);
    }

    #[tokio::test]
    async fn a_low_confidence_reject_escalates_and_a_confident_one_is_final() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // 0.1 is below the reject threshold, but a 0.2 choice confidence is under the floor:
        // as undecided as a low-confidence yes, so the LLM is asked.
        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = gate_service(
            (0.1, 0.2, false),
            (0.93, 0.9, false),
            Default::default(),
            std::sync::Arc::clone(&second_calls),
            None,
        );
        let cancel = no_cancel();
        let outcome = ask_chain(&service, &chain_settings(), &cancel).await;
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
        assert_eq!(outcome.accepted, Some(0));

        let second_calls = std::sync::Arc::new(AtomicUsize::new(0));
        let service = gate_service(
            (0.1, 0.9, false),
            (0.93, 0.9, false),
            Default::default(),
            std::sync::Arc::clone(&second_calls),
            None,
        );
        let outcome = ask_chain(&service, &chain_settings(), &cancel).await;
        assert_eq!(
            second_calls.load(Ordering::SeqCst),
            0,
            "a confident reject is final"
        );
        assert_eq!(outcome.accepted, None);
    }

    #[tokio::test]
    async fn the_audited_request_names_the_first_chain_link() {
        // With a chain set the shorthand pair is an untouched default; the audit row must
        // name the link actually asked first, not the default.
        let service = gate_service(
            (0.93, 0.9, false),
            (0.93, 0.9, false),
            Default::default(),
            Default::default(),
            None,
        );
        let mut settings = chain_settings();
        settings.chain.reverse();
        let cancel = no_cancel();
        let outcome = ask_chain(&service, &settings, &cancel).await;
        assert_eq!(outcome.request["provider"], "llm");
        assert_eq!(outcome.request["model"], "kimi-k3");
    }
}
