//! JSONL harness for `scripts/corpus/engine_parity.py` (phase 3 step 6).
//!
//! Reads one JSON request per line on stdin, writes one JSON response per line on
//! stdout. The engine is the decided unified engine at its default threshold (95);
//! a request may set `"threshold"` (or `"sensitivity"`) to score at another cut.
//!
//! Request kinds:
//!
//! * `normalize` (or no `kind`): `{name}` -> `{"normalized": ...}` (the decided key).
//! * `callsign`: `{name}` -> `{"callsign": ..., "normalized": ...}` (shared-core
//!   ladder over the merged allowlist; `normalized` is the `-suffix`-stripped base).
//! * `alias`: `{name}` -> `{"canonical": ...|null}` (reverse-index resolve, A01).
//! * `same`: `{query, candidate}` -> `{"same": bool, "score": n|0, "method": ...|null}`.
//! * `rank`: `{query, candidates}` -> `{"family": [[index, score, method]]}` (the best
//!   identity family only).
//! * `identity`: `{name}` -> the [`Identity`] as JSON (canonical, callsign, network,
//!   country, category, region, zone, timeshift, tags, key).
//! * `zone`: `{name}` -> `{"zone": "EAST"|"WEST"|"DEFAULT"}` (noguard routing zone).
//! * `pipe`: `{name}` -> `{"collapsed": ...}` (the decided N15 pipe collapse).
//! * `version`: `{}` -> `{"data_version": ...}` (cache namespace).

use crosstalk_matching::callsign::normalize_callsign;
use crosstalk_matching::engine::{
    data_version, is_time_shift, sensitivity_preset, EngineConfig, UnifiedEngine,
};
use crosstalk_matching::identity::{resolve_with, IdentityCache, ResolveContext};
use std::collections::HashMap;
use std::io::{BufRead, Write};

fn str_field<'a>(value: &'a serde_json::Value, field: &str) -> &'a str {
    value
        .get(field)
        .and_then(|entry| entry.as_str())
        .unwrap_or_default()
}

fn candidates_of(value: &serde_json::Value) -> Vec<String> {
    value
        .get("candidates")
        .and_then(|candidates| candidates.as_array())
        .map(|candidates| {
            candidates
                .iter()
                .filter_map(|name| name.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn engine_for(
    request: &serde_json::Value,
    engines: &mut HashMap<String, UnifiedEngine>,
) -> UnifiedEngine {
    let threshold = request
        .get("threshold")
        .and_then(serde_json::Value::as_u64)
        .map(|cut| cut.min(100) as u8)
        .or_else(|| {
            request
                .get("sensitivity")
                .and_then(|sensitivity| sensitivity.as_str())
                .map(sensitivity_preset)
        })
        .unwrap_or(EngineConfig::default().threshold);
    let custom_aliases: HashMap<String, Vec<String>> = request
        .get("custom_aliases")
        .and_then(|aliases| aliases.as_object())
        .map(|aliases| {
            aliases
                .iter()
                .map(|(key, value)| {
                    let variants = value
                        .as_array()
                        .map(|list| {
                            list.iter()
                                .filter_map(|entry| entry.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default();
                    (key.clone(), variants)
                })
                .collect()
        })
        .unwrap_or_default();
    let cache_key = format!(
        "{threshold}:{}",
        serde_json::to_string(&custom_aliases).unwrap_or_default()
    );
    engines
        .entry(cache_key)
        .or_insert_with(|| {
            UnifiedEngine::new(&EngineConfig {
                threshold,
                custom_aliases,
                ..EngineConfig::default()
            })
        })
        .clone()
}

fn main() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    // Engines are expensive to build (merged tables + reverse index) and
    // requests repeat the same configuration thousands of times, so cache by
    // (threshold, custom aliases). The engine itself is Send + Sync with no
    // interior mutability; sequential reuse here is purely a speed-up.
    let mut engines: HashMap<String, UnifiedEngine> = HashMap::new();
    for line in stdin.lock().lines() {
        let line = line.expect("stdin line");
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
    engines: &mut HashMap<String, UnifiedEngine>,
) -> serde_json::Value {
    let kind = request
        .get("kind")
        .and_then(|kind| kind.as_str())
        .unwrap_or("normalize");
    match kind {
        "version" => serde_json::json!({"data_version": data_version()}),
        "normalize" => {
            let engine = engine_for(request, engines);
            serde_json::json!({"normalized": engine.normalize_key(str_field(request, "name"))})
        }
        "callsign" => {
            let engine = engine_for(request, engines);
            let name = str_field(request, "name");
            let (raw, high) = engine.callsign_confidence(name);
            serde_json::json!({
                "callsign": raw,
                "normalized": raw.as_deref().map(normalize_callsign),
                "high_confidence": high,
            })
        }
        "alias" => {
            let engine = engine_for(request, engines);
            serde_json::json!({"canonical": engine.alias_canonical(str_field(request, "name"))})
        }
        "same" => {
            let engine = engine_for(request, engines);
            let query = str_field(request, "query");
            let candidate = str_field(request, "candidate");
            match engine.score(query, candidate) {
                Some((score, method)) => serde_json::json!({
                    "same": true, "score": score, "method": method.label(),
                }),
                None => serde_json::json!({"same": false, "score": 0, "method": None::<String>}),
            }
        }
        "rank" => {
            let engine = engine_for(request, engines);
            let query = str_field(request, "query");
            let candidates = candidates_of(request);
            let family = engine.rank(query, &candidates);
            serde_json::json!({
                "family": family
                    .iter()
                    .map(|entry| serde_json::json!([entry.index, entry.score, entry.method.label()]))
                    .collect::<Vec<_>>(),
            })
        }
        "identity" => {
            let engine = engine_for(request, engines);
            let ctx = ResolveContext::default();
            let identity = resolve_with(&engine, str_field(request, "name"), &ctx);
            serde_json::from_str(&IdentityCache::identity_json(&identity))
                .unwrap_or(serde_json::Value::Null)
        }
        "zone" => {
            let engine = engine_for(request, engines);
            let _ = &engine;
            let name = str_field(request, "name");
            serde_json::json!({"zone": crosstalk_matching::country::extract_zone_noguard(name).label()})
        }
        "timeshift" => {
            serde_json::json!({"timeshift": is_time_shift(str_field(request, "name"))})
        }
        "pipe" => serde_json::json!({
            "collapsed": crosstalk_matching::tags::collapse_pipe_segments(str_field(request, "name")),
        }),
        _ => serde_json::json!({"error": format!("unknown kind {kind}")}),
    }
}
