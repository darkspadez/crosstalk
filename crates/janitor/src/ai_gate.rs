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
//! A heuristic result at or above the automatch confidence threshold NEVER reaches the
//! model: the model may promote inside the band, but it never re-checks or vetoes a
//! heuristic win. Spending latency and tokens to re-check results the heuristics are
//! already sure about would make the tool slower without making it more accurate.
//!
//! One request carries the whole shortlist. TypeSafe answers every question against one state
//! in parallel, so asking about five candidates costs about as much as asking about one.

use crosstalk_core::ai::{AiService, Answer, DecisionRequest, Question};
use crosstalk_core::dispatcharr::{Channel, EpgEntry};
use crosstalk_core::store::AiDecision;
use crosstalk_matching::callsign::{extract_location, network_keyword, KnownCallsigns};
use crosstalk_matching::pipeline::Scored;
use std::collections::BTreeMap;

use crate::settings::AiSettings;

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
    /// Whether an answer was actually obtained. A failed request leaves the channel undecided
    /// rather than failing the run.
    pub answered: bool,
}

impl GateOutcome {
    /// Convert to the store's decision row.
    pub fn to_store(&self, row_ordinal: i64, provider: &str) -> AiDecision {
        AiDecision {
            row_ordinal,
            provider: provider.to_string(),
            model: self.model.clone(),
            latency_ms: self.latency_ms,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            request: self.request.clone(),
            response: self.response.clone(),
        }
    }
}

/// Which candidates a channel should offer the model, if any.
///
/// Returns the slice of `scored` worth sending, and why. Empty when the heuristics are
/// confident enough that a request would be wasted.
///
/// The shortlist holds one position per candidate NAME: guide listings routinely carry the
/// same name under several entries (regional duplicates), and offering the model the same
/// name twice spends tokens without adding information. The first position in score order
/// wins each name, so the kept entry is the highest-scored one.
pub fn shortlist(
    scored: &[Scored],
    entries: &[EpgEntry],
    settings: &AiSettings,
) -> Option<(GateReason, Vec<usize>)> {
    if !settings.enabled || scored.is_empty() {
        return None;
    }

    let best = scored[0].score;
    let in_band = best >= settings.verify_band_low && best <= settings.verify_band_high;
    if in_band {
        return Some((
            GateReason::Verify,
            take_unique(scored, entries, settings.shortlist),
        ));
    }

    if settings.rescue && best < settings.verify_band_low {
        let eligible: Vec<usize> = scored
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.score >= settings.rescue_floor)
            .map(|(position, _)| position)
            .collect();
        let offered = dedupe_names(scored, entries, &eligible, settings.shortlist);
        if !offered.is_empty() {
            return Some((GateReason::Rescue, offered));
        }
    }

    None
}

/// Up to `limit` positions from the head of `scored`, in order, keeping the first
/// position seen for each candidate name.
fn take_unique(scored: &[Scored], entries: &[EpgEntry], limit: usize) -> Vec<usize> {
    let all: Vec<usize> = (0..scored.len().min(limit.max(1))).collect();
    dedupe_names(scored, entries, &all, limit)
}

fn dedupe_names(
    scored: &[Scored],
    entries: &[EpgEntry],
    candidates: &[usize],
    limit: usize,
) -> Vec<usize> {
    let mut seen = std::collections::HashSet::new();
    candidates
        .iter()
        .copied()
        .filter(|position| {
            scored
                .get(*position)
                .and_then(|candidate| entries.get(candidate.index))
                .is_some_and(|entry| seen.insert(entry.name.clone()))
        })
        .take(limit.max(1))
        .collect()
}

/// Build the request: the channel, the shortlist, and the questions.
///
/// The callsign is extracted with the run's allowlist, the same one the matcher scores
/// with — never an empty default, which would hide denylisted-but-real callsigns (like
/// `KING`) from the model while the matcher itself sees them.
pub fn build_request(
    channel: &Channel,
    entries: &[EpgEntry],
    scored: &[Scored],
    offered: &[usize],
    known: &KnownCallsigns,
    settings: &AiSettings,
) -> DecisionRequest {
    let callsign = crosstalk_matching::callsign::extract_callsign(&channel.name, known);
    let location = extract_location(&channel.name);
    let network = network_keyword(&channel.name);

    let mut state_channel = serde_json::json!({
        "name": channel.name,
        "number": channel.display_number(),
    });
    if let Some(callsign) = callsign {
        state_channel["callsign"] = serde_json::json!(callsign);
    }
    if let Some(state) = location.state {
        state_channel["state"] = serde_json::json!(state);
    }
    if let Some(city) = location.city {
        state_channel["city"] = serde_json::json!(city);
    }
    if let Some(network) = network {
        state_channel["network"] = serde_json::json!(network);
    }

    let candidates: Vec<serde_json::Value> = offered
        .iter()
        .filter_map(|position| {
            let candidate = scored.get(*position)?;
            let entry = entries.get(candidate.index)?;
            let mut payload = serde_json::json!({
                "id": format!("candidate_{}", candidate.index),
                "name": entry.name,
                "score": candidate.score,
                "method": candidate.method_label(),
                "tvg_id": entry.tvg_id,
            });
            // The source id is what the model needs to tell two same-name entries apart;
            // the source NAME is not plumbed this far, so the id is what is offered.
            if let Some(source) = entry.epg_source {
                payload["epg_source"] = serde_json::json!(source);
            }
            Some(payload)
        })
        .collect();

    let state = serde_json::json!({
        "channel": state_channel,
        "candidates": candidates,
    });

    let mut request =
        DecisionRequest::new(settings.provider.clone(), settings.model.clone(), state);

    let mut options = BTreeMap::new();
    options.insert(
        "none".to_string(),
        "None of the candidates is the same channel".to_string(),
    );

    for position in offered {
        let Some(candidate) = scored.get(*position) else {
            continue;
        };
        let Some(entry) = entries.get(candidate.index) else {
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
            format!("{} (heuristic score {})", entry.name, candidate.score),
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
pub fn combine(
    outcome: &mut GateOutcome,
    request: &DecisionRequest,
    response: &crosstalk_core::ai::DecisionResponse,
    scored: &[Scored],
    entries: &[EpgEntry],
    offered: &[usize],
    settings: &AiSettings,
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
    let Some(entry) = entries.get(candidate.index) else {
        return;
    };
    let probability = response.noul(&format!("same_{}", candidate.index));
    outcome.probability = probability;

    let verdict = match probability {
        Some(probability) => crosstalk_core::ai::Verdict {
            probability,
            confidence: Some(choice_confidence),
        },
        // A backend that answered the choice but not the noul still gives a usable signal.
        None => crosstalk_core::ai::Verdict {
            probability: 1.0,
            confidence: Some(choice_confidence),
        },
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
            entry.name,
            probability.unwrap_or(1.0),
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
            entry.name,
            probability.unwrap_or(0.0),
            choice_confidence
        );
    }

    let _ = request;
}

/// Ask the model about one channel.
///
/// A failed request is reported as `answered: false` and never fails the run: an install whose
/// AI endpoint is down should still match the channels the heuristics are sure about.
#[allow(clippy::too_many_arguments)]
pub async fn ask(
    service: Option<&AiService>,
    channel: &Channel,
    entries: &[EpgEntry],
    scored: &[Scored],
    offered: &[usize],
    reason: GateReason,
    known: &KnownCallsigns,
    settings: &AiSettings,
) -> GateOutcome {
    let mut outcome = GateOutcome {
        accepted: None,
        reason,
        picked: None,
        probability: None,
        confidence: None,
        decision: format!("{}: skipped (no AI provider available)", reason.as_str()),
        model: settings.model.clone(),
        calibrated: false,
        latency_ms: 0,
        input_tokens: 0,
        output_tokens: 0,
        request: serde_json::json!({}),
        response: serde_json::json!({}),
        answered: false,
    };

    let Some(service) = service else {
        return outcome;
    };
    let request = build_request(channel, entries, scored, offered, known, settings);
    outcome.request = serde_json::to_value(&request).unwrap_or_default();

    match service.decide(&request).await {
        Ok(response) => {
            outcome.response = serde_json::to_value(&response).unwrap_or_default();
            combine(
                &mut outcome,
                &request,
                &response,
                scored,
                entries,
                offered,
                settings,
            );
        }
        Err(error) => {
            outcome.decision = format!("{}: request failed ({error:#})", reason.as_str());
            outcome.response = serde_json::json!({"error": format!("{error:#}")});
        }
    }
    outcome
}

/// The steps that make a gate outcome usable by a caller.
pub fn is_answered(outcome: &GateOutcome) -> bool {
    outcome.answered
}

/// The companion answer for the report: whether the model saw the same network.
pub fn network_agreement(response: &crosstalk_core::ai::DecisionResponse) -> Option<String> {
    match response.answers.get("same_network") {
        Some(Answer::Noul { noul }) => Some(format!("network {noul:.2}")),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_matching::pipeline::MatchMethod;

    fn scored(score: u8, index: usize) -> Scored {
        Scored {
            index,
            score,
            components: vec!["Fuzzy"],
        }
    }

    fn settings() -> AiSettings {
        AiSettings {
            enabled: true,
            provider: "typesafe".to_string(),
            model: "jev-latest".to_string(),
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
        let settings = AiSettings {
            rescue: false,
            ..settings()
        };
        assert_eq!(shortlist(&ranked, &entries, &settings), None);
    }

    #[test]
    fn disabled_ai_sends_nothing() {
        let entries = named_entries(&["A"]);
        let ranked = vec![scored(90, 0)];
        let settings = AiSettings {
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
    ) -> crosstalk_core::ai::DecisionResponse {
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
        crosstalk_core::ai::DecisionResponse {
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
            answered: false,
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
        let denied = build_request(&king_channel, &entries, &ranked, &[0], &empty, &settings());
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
}
