//! JSONL harness for `scripts/parity_sweep.py`.
//!
//! Reads one JSON request per line on stdin, writes one JSON response per line on
//! stdout. The sweep drives six request kinds:
//!
//! * `normalize` (or a request with no `kind`): `{name, ignore_quality,
//!   ignore_regional, ignore_geographic, ignore_misc, tags}` ->
//!   `{"normalized": ...}`. Flags default to true (the plugin's shipped defaults);
//!   `tags` defaults to `[]`.
//! * `callsign`: `{name}` -> `{"callsign": ..., "high_confidence": ...}`.
//! * `location`: `{name}` -> `{"state": ..., "city": ...}`.
//! * `network`: `{name}` -> `{"network": ...}`.
//! * `match`: `{query, candidates, tags}` -> `{"matches": [[name, score, type]]}`,
//!   every match including below-threshold ones (`min_score = 0`).
//! * `score`: `{query, candidates, tags}` -> `{"scored": [[name, score, label]]}`,
//!   the structural+fuzzy scoring half of `Plugin._find_best_epg_match`
//!   (programme-data check excluded, as in the sweep's transcription).
//!
//! The matcher mirrors the sweep's Python setup: threshold 85, the built-in alias
//! table, and the shipped FCC callsign allowlist (the sweep never loads channel
//! databases, so no database-derived callsigns are merged).

use crosstalk_matching::callsign::{
    extract_callsign_with_confidence, extract_location, network_keyword, KnownCallsigns,
};
use crosstalk_matching::normalize::{NormalizeFlags, Normalizer};
use crosstalk_matching::pipeline::{CandidateIndex, Matcher};
use std::io::{BufRead, Write};

fn bool_field(value: &serde_json::Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true)
}

fn tags_of(value: &serde_json::Value) -> Vec<String> {
    value
        .get("tags")
        .and_then(|tags| tags.as_array())
        .map(|tags| {
            tags.iter()
                .filter_map(|tag| tag.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn main() {
    let matcher = Matcher::with_shipped_defaults();
    let shipped: KnownCallsigns = KnownCallsigns::shipped().cloned().unwrap_or_default();

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => continue,
        };
        if line.trim().is_empty() {
            continue;
        }
        let request: serde_json::Value = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(err) => {
                let _ = writeln!(out, "{{\"error\":\"{err}\"}}");
                continue;
            }
        };
        let response = handle(&request, &matcher, &shipped);
        let _ = writeln!(
            out,
            "{}",
            serde_json::to_string(&response).unwrap_or_default()
        );
    }
    let _ = out.flush();
}

fn handle(
    request: &serde_json::Value,
    matcher: &Matcher,
    shipped: &KnownCallsigns,
) -> serde_json::Value {
    let kind = request.get("kind").and_then(|kind| kind.as_str());
    match kind {
        None | Some("normalize") => {
            let flags = NormalizeFlags {
                ignore_quality: bool_field(request, "ignore_quality"),
                ignore_regional: bool_field(request, "ignore_regional"),
                ignore_geographic: bool_field(request, "ignore_geographic"),
                ignore_misc: bool_field(request, "ignore_misc"),
            };
            let tags = tags_of(request);
            let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
            let name = request
                .get("name")
                .and_then(|name| name.as_str())
                .unwrap_or_default();
            let normalized = Normalizer::new(flags).normalize_name(name, &tag_refs);
            serde_json::json!({"normalized": normalized})
        }
        Some("callsign") => {
            let name = request
                .get("name")
                .and_then(|name| name.as_str())
                .unwrap_or_default();
            let (callsign, high) = extract_callsign_with_confidence(name, shipped);
            serde_json::json!({"callsign": callsign, "high_confidence": high})
        }
        Some("location") => {
            let name = request
                .get("name")
                .and_then(|name| name.as_str())
                .unwrap_or_default();
            let location = extract_location(name);
            serde_json::json!({"state": location.state, "city": location.city})
        }
        Some("network") => {
            let name = request
                .get("name")
                .and_then(|name| name.as_str())
                .unwrap_or_default();
            serde_json::json!({"network": network_keyword(name)})
        }
        Some("match") => {
            let query = request
                .get("query")
                .and_then(|query| query.as_str())
                .unwrap_or_default();
            let candidates: Vec<String> = request
                .get("candidates")
                .and_then(|candidates| candidates.as_array())
                .map(|candidates| {
                    candidates
                        .iter()
                        .filter_map(|candidate| candidate.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let tags = tags_of(request);
            let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
            let index = CandidateIndex::build(&candidates, matcher.normalizer(), matcher.known());
            let matches: Vec<serde_json::Value> = matcher
                .match_all_streams_all(query, &index, &tag_refs)
                .iter()
                .map(|ranked| {
                    serde_json::json!([
                        index.name(ranked.index),
                        ranked.score,
                        ranked.method.label()
                    ])
                })
                .collect();
            serde_json::json!({"matches": matches})
        }
        Some("score") => {
            let query = request
                .get("query")
                .and_then(|query| query.as_str())
                .unwrap_or_default();
            let candidates: Vec<String> = request
                .get("candidates")
                .and_then(|candidates| candidates.as_array())
                .map(|candidates| {
                    candidates
                        .iter()
                        .filter_map(|candidate| candidate.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let tags = tags_of(request);
            let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
            let index = CandidateIndex::build(&candidates, matcher.normalizer(), matcher.known());
            let scored: Vec<serde_json::Value> = matcher
                .score_candidates(query, &index, &tag_refs, None)
                .iter()
                .map(|entry| {
                    serde_json::json!([index.name(entry.index), entry.score, entry.method_label()])
                })
                .collect();
            serde_json::json!({"scored": scored})
        }
        Some(other) => serde_json::json!({"error": format!("unknown kind {other}")}),
    }
}
