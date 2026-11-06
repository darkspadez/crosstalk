//! JSONL harness for `scripts/oracle_report_guide.py` (phase 3 step 8).
//!
//! Reads one JSON request per line on stdin, writes one JSON response per line on
//! stdout. The request carries the guide end-to-end fixture — the in-scope channels and
//! the eligible EPG entries — and the response carries the unified engine's answer per
//! channel, which the report compares against Janitor's own `_find_best_epg_match`.
//!
//! Request: `{"threshold": 95, "channels": [{"id": 1, "name": "..."}],
//! "entries": [{"id": 7, "name": "..."}]}` (`threshold` optional, default 95).
//!
//! A channel may carry `"probe": "<entry name>"` (the oracle's winner): the row then
//! also says whether the engine's zone guard (Z01: WEST vs not-WEST on the routed feed
//! zone) separates the channel from that entry — `"probe_zone_split": true|false` — so
//! the report attributes a zone-routing difference from the engine itself, not from a
//! word list.
//!
//! Response: `{"rows": [{"channel_id": 1, "winner_id": 7|null, "score": 100,
//! "method": "alias"|...}]}`. Every entry is assumed to carry programme data (the same
//! residual the Janitor oracle runs under: `allow_without_programs=True`), so selection
//! is the best identity family's first member in rank order — the programme-data walk
//! would accept it without asking Dispatcharr.

use crosstalk_matching::engine::{EngineConfig, UnifiedEngine};
use std::io::{BufRead, Write};

fn main() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    // One engine per threshold seen; requests repeat the same cut thousands of times and
    // the merged tables + reverse index are expensive to rebuild.
    let mut engines: std::collections::HashMap<u8, UnifiedEngine> =
        std::collections::HashMap::new();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let request: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
        let response = handle(&request, &mut engines);
        writeln!(stdout, "{response}").expect("stdout line");
    }
}

fn handle(
    request: &serde_json::Value,
    engines: &mut std::collections::HashMap<u8, UnifiedEngine>,
) -> serde_json::Value {
    let threshold = request
        .get("threshold")
        .and_then(serde_json::Value::as_u64)
        .map(|cut| cut.min(100) as u8)
        .unwrap_or(EngineConfig::default().threshold);
    let engine = engines
        .entry(threshold)
        .or_insert_with(|| {
            UnifiedEngine::new(&EngineConfig {
                threshold,
                ..EngineConfig::default()
            })
        })
        .clone();

    let channels: Vec<(i64, String, Option<String>)> = request
        .get("channels")
        .and_then(|channels| channels.as_array())
        .map(|channels| {
            channels
                .iter()
                .map(|channel| {
                    (
                        channel.get("id").and_then(|id| id.as_i64()).unwrap_or(0),
                        channel
                            .get("name")
                            .and_then(|name| name.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        channel
                            .get("probe")
                            .and_then(|probe| probe.as_str())
                            .map(str::to_string),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let entries: Vec<(i64, String)> = request
        .get("entries")
        .and_then(|entries| entries.as_array())
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    (
                        entry.get("id").and_then(|id| id.as_i64()).unwrap_or(0),
                        entry
                            .get("name")
                            .and_then(|name| name.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let entry_names: Vec<String> = entries.iter().map(|(_, name)| name.clone()).collect();
    let index = crosstalk_matching::engine::EngineIndex::build(
        &entry_names,
        &engine,
        &crosstalk_matching::identity::ResolveContext::default(),
    );

    let mut rows = Vec::with_capacity(channels.len());
    let is_west = |name: &str| {
        crosstalk_matching::engine::engine_zone(name) == crosstalk_matching::country::FeedZone::West
    };
    for (channel_id, channel_name, probe) in &channels {
        // The best identity family at the cut, in rank order; the first member wins
        // (every entry carries programme data in this harness).
        let family = engine.rank_index(channel_name, &index);
        let (winner_id, score, method) = family.first().map_or((None, 0, None), |member| {
            (
                entries.get(member.index).map(|(id, _)| *id),
                member.score,
                Some(member.method.label()),
            )
        });
        let mut row = serde_json::json!({
            "channel_id": channel_id,
            "winner_id": winner_id,
            "score": score,
            "method": method,
        });
        if let Some(probe) = probe {
            row["probe_zone_split"] = serde_json::json!(is_west(channel_name) != is_west(probe));
        }
        rows.push(row);
    }
    serde_json::json!({"rows": rows})
}
