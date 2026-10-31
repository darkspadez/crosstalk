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
//!
//! `scripts/parity_sweep.py` drives it and diffs the results against the Python plugin.
//!
//! Not part of the daemon. `cargo run --example normalize_io -p crosstalk-matching`.

use crosstalk_matching::callsign::{
    extract_callsign_with_confidence, extract_location, network_keyword, KnownCallsigns,
};
use crosstalk_matching::normalize::{NormalizeFlags, Normalizer};
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
