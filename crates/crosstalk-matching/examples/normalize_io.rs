//! Dev tool: normalise a batch of names, for the Python parity sweep.
//!
//! Reads one JSON object per line from stdin and writes one result object per line.
//! `kind` selects which primitive to exercise:
//!
//!   * `normalize` (default): `{"name", "ignore_quality", "ignore_regional",
//!     "ignore_geographic", "ignore_misc", "tags"}` -> `{"normalized"}`
//!   * `callsign`: `{"name"}` -> `{"callsign", "high_confidence"}`
//!   * `location`: `{"name"}` -> `{"state", "city"}`
//!   * `network`: `{"name"}` -> `{"network"}`
//!   * `match`: `{"query", "candidates", "tags"}` -> `{"matches": [[name, score, method]]}`
//!   * `score`: `{"query", "candidates", "tags"}` -> `{"scored": [[name, score, components]]}`
//!     (the structural-plus-fuzzy half of the EPG scoring, best first)
//!
//! `scripts/parity_sweep.py` drives it and diffs the results against the Python plugin.
//!
//! Not part of the daemon. `cargo run --example normalize_io -p crosstalk-matching`.

use crosstalk_matching::alias::AliasMap;
use crosstalk_matching::callsign::{
    extract_callsign_with_confidence, extract_location, network_keyword, KnownCallsigns,
};
use crosstalk_matching::normalize::{NormalizeFlags, Normalizer};
use crosstalk_matching::pipeline::{CandidateIndex, MatchOptions, Matcher};
use std::io::{self, BufRead, Write};

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let request: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                writeln!(stdout, "{}", serde_json::json!({"error": e.to_string()})).ok();
                continue;
            }
        };
        let name = request.get("name").and_then(|v| v.as_str()).unwrap_or("");
        match request
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("normalize")
        {
            "callsign" => {
                // The golden baseline was produced against the shipped FCC allowlist
                // with no channel databases loaded, so that is what this uses too.
                let known = KnownCallsigns::shipped().cloned().unwrap_or_default();
                let (callsign, high_confidence) = extract_callsign_with_confidence(name, &known);
                writeln!(
                    stdout,
                    "{}",
                    serde_json::json!({"callsign": callsign, "high_confidence": high_confidence})
                )
                .ok();
                continue;
            }
            "match" => {
                let query = request
                    .get("query")
                    .and_then(|v| v.as_str())
                    .or_else(|| request.get("name").and_then(|v| v.as_str()))
                    .unwrap_or("");
                let candidates: Vec<String> = request
                    .get("candidates")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|c| c.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                let tags: Vec<String> = request
                    .get("tags")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|c| c.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();

                let matcher = Matcher::new(
                    NormalizeFlags::default(),
                    AliasMap::builtin(),
                    std::sync::Arc::new(KnownCallsigns::shipped().cloned().unwrap_or_default()),
                    MatchOptions::default(),
                );
                let index = CandidateIndex::build(
                    &candidates,
                    matcher.normalizer(),
                    &tag_refs,
                    matcher.known(),
                );
                // Every match, including below-threshold ones, so ordering and scores are
                // compared too, not just which candidates survived.
                let matches: Vec<serde_json::Value> = matcher
                    .match_all_streams_all(query, &index, &tag_refs)
                    .into_iter()
                    .map(|ranked| {
                        serde_json::json!([
                            index.name(ranked.index),
                            ranked.score,
                            ranked.method.label()
                        ])
                    })
                    .collect();
                writeln!(stdout, "{}", serde_json::json!({"matches": matches})).ok();
                continue;
            }
            "score" => {
                let query = request
                    .get("query")
                    .and_then(|v| v.as_str())
                    .or_else(|| request.get("name").and_then(|v| v.as_str()))
                    .unwrap_or("");
                let candidates: Vec<String> = request
                    .get("candidates")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|c| c.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                let tags: Vec<String> = request
                    .get("tags")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|c| c.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();

                let matcher = Matcher::new(
                    NormalizeFlags::default(),
                    AliasMap::builtin(),
                    std::sync::Arc::new(KnownCallsigns::shipped().cloned().unwrap_or_default()),
                    MatchOptions::default(),
                );
                let index = CandidateIndex::build(
                    &candidates,
                    matcher.normalizer(),
                    &tag_refs,
                    matcher.known(),
                );
                let scored: Vec<serde_json::Value> = matcher
                    .score_candidates(query, &index, &tag_refs, None)
                    .into_iter()
                    .map(|entry| {
                        serde_json::json!([
                            index.name(entry.index),
                            entry.score,
                            entry.method_label()
                        ])
                    })
                    .collect();
                writeln!(stdout, "{}", serde_json::json!({"scored": scored})).ok();
                continue;
            }
            "network" => {
                writeln!(
                    stdout,
                    "{}",
                    serde_json::json!({"network": network_keyword(name)})
                )
                .ok();
                continue;
            }
            "location" => {
                let location = extract_location(name);
                writeln!(
                    stdout,
                    "{}",
                    serde_json::json!({"state": location.state, "city": location.city})
                )
                .ok();
                continue;
            }
            _ => {}
        }

        let flag = |key: &str| request.get(key).and_then(|v| v.as_bool()).unwrap_or(true);
        let flags = NormalizeFlags {
            ignore_quality: flag("ignore_quality"),
            ignore_regional: flag("ignore_regional"),
            ignore_geographic: flag("ignore_geographic"),
            ignore_misc: flag("ignore_misc"),
        };
        let tags: Vec<String> = request
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();

        let normalized = Normalizer::new(flags).normalize_name(name, &tag_refs);
        writeln!(stdout, "{}", serde_json::json!({"normalized": normalized})).ok();
    }
    stdout.flush().ok();
}
