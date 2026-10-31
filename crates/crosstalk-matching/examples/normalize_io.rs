//! Dev tool: normalise a batch of names, for the Python parity sweep.
//!
//! Reads one JSON object per line from stdin:
//! `{"name": "...", "ignore_quality": true, "ignore_regional": true,
//!   "ignore_geographic": true, "ignore_misc": true, "tags": ["..."]}`
//! and writes `{"normalized": "..."}` per line to stdout. `scripts/parity_sweep.py`
//! drives it and diffs the result against the Python plugin.
//!
//! Not part of the daemon. `cargo run --example normalize_io -p crosstalk-matching`.

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
        let name = request.get("name").and_then(|v| v.as_str()).unwrap_or("");

        let normalized = Normalizer::new(flags).normalize_name(name, &tag_refs);
        writeln!(stdout, "{}", serde_json::json!({"normalized": normalized})).ok();
    }
    stdout.flush().ok();
}
