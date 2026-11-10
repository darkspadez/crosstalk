//! Plain-timing performance harness for `crosstalk-matching` (PERFORMANCE_AUDIT.md §5).
//!
//! Criterion is not vendored offline, so this is a `cargo run --release --example`
//! main that prints the §2 table rows instead of a criterion bench suite:
//!
//! ```sh
//! cargo run --release -p crosstalk-matching --example perf_bench
//! ```
//!
//! Corpus: the real US/UK/CA channel names from `data/channels/` (same source the
//! audit's throwaway harness used), falling back to a synthetic list when the files
//! are absent. The hard-path shape mirrors the audit: queries with HD/quality
//! variants against 5,000 candidates, scored through `score_candidates` on rayon,
//! exactly as the guide app's `match_preview` scoring stage calls it.

use crosstalk_matching::alias::AliasMap;
use crosstalk_matching::callsign::KnownCallsigns;
use crosstalk_matching::normalize::{NormalizeFlags, Normalizer};
use crosstalk_matching::pipeline::{CandidateIndex, Matcher};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn load_corpus() -> Vec<String> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let base = std::path::Path::new(manifest).join("../../data/channels");
    let mut names = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut add = |name: String| {
        if !name.is_empty() && seen.insert(name.clone()) {
            names.push(name);
        }
    };
    let mut loaded = false;
    for code in ["US", "UK", "CA"] {
        let path = base.join(format!("{code}_channels.json"));
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else {
            continue;
        };
        loaded = true;
        let empty = Vec::new();
        let channels = json
            .get("channels")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty);
        for (i, channel) in channels.iter().enumerate() {
            if let Some(name) = channel.get("channel_name").and_then(|v| v.as_str()) {
                add(name.to_string());
            }
            // Same callsign supplement the parity fixture uses: first 400 US entries.
            if code == "US" && i < 400 {
                if let Some(callsign) = channel.get("callsign").and_then(|v| v.as_str()) {
                    add(callsign.to_string());
                }
            }
        }
    }
    if !loaded {
        // Synthetic fallback: enough variety to exercise every pipeline stage.
        let seeds = [
            "ESPN",
            "CNN",
            "BBC One",
            "HBO",
            "Sky Sports News",
            "NBC",
            "ABC",
            "Discovery",
            "National Geographic",
            "Fox News",
            "ITV2",
            "C-SPAN",
            "Cartoon Network",
            "TLC",
            "AMC",
            "FX",
            "Comedy Central",
            "MTV",
        ];
        for i in 0..300 {
            for seed in seeds {
                add(format!("{seed} {i}"));
            }
        }
    }
    names
}

fn bench(label: &str, iters: u64, ops_per_iter: u64, f: impl Fn()) -> Duration {
    // Warmup, then the timed run.
    f();
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let elapsed = start.elapsed();
    println!(
        "    {label:<42} {:>10.3} ms total ({:.0} ops/s)",
        elapsed.as_secs_f64() * 1000.0,
        (iters * ops_per_iter) as f64 / elapsed.as_secs_f64()
    );
    elapsed
}

fn main() {
    let names = load_corpus();
    println!("perf_bench: {} unique names", names.len());

    let normalizer = Normalizer::new(NormalizeFlags::default());

    // §2.1 primitives.
    let sample: Vec<String> = names.iter().take(2000).cloned().collect();
    bench("normalize_name x2000", 5, 2000, || {
        for name in &sample {
            std::hint::black_box(normalizer.normalize_name(name, &[]));
        }
    });
    bench("normalize_name + 4 tags x2000", 5, 2000, || {
        for name in &sample {
            std::hint::black_box(
                normalizer.normalize_name(name, &["quality", "regional", "geographic", "misc"]),
            );
        }
    });

    // CandidateIndex build (§2.1: ~29k names/s at 20k).
    let base_names: Vec<String> = names.iter().take(20000).cloned().collect();
    let mut build_names = base_names;
    while build_names.len() < 20000 {
        // Small corpora (synthetic fallback) are cycled up to the target size.
        if names.is_empty() {
            break;
        }
        let mut grown = build_names.clone();
        grown.extend(names.iter().cloned());
        if grown.len() == build_names.len() {
            break;
        }
        build_names = grown;
    }
    let build_names: Vec<String> = build_names.into_iter().take(20000).collect();
    let known = Arc::new(KnownCallsigns::shipped().cloned().unwrap_or_default());
    let start = Instant::now();
    let index = CandidateIndex::build(&build_names, &normalizer, &known);
    let elapsed = start.elapsed();
    println!(
        "    {:<42} {:>10.3} ms total ({:.0} names/s)",
        format!("CandidateIndex::build x{}", build_names.len()),
        elapsed.as_secs_f64() * 1000.0,
        build_names.len() as f64 / elapsed.as_secs_f64()
    );

    // Hard-path pipeline (§2.2): 200 HD-variant queries x 5000 candidates, rayon.
    let candidates: Vec<String> = names.iter().take(5000).cloned().collect();
    let candidates = if candidates.len() < 5000 {
        let mut grown = candidates;
        while grown.len() < 5000 {
            let extra: Vec<String> = grown
                .iter()
                .take(5000 - grown.len())
                .map(|n| format!("{n} HD"))
                .collect();
            if extra.is_empty() {
                break;
            }
            grown.extend(extra);
        }
        grown
    } else {
        candidates
    };
    let queries: Vec<String> = names.iter().take(200).map(|n| format!("{n} HD")).collect();
    let cand_index = CandidateIndex::build(&candidates, &normalizer, &known);
    let matcher = Matcher::new(
        NormalizeFlags::default(),
        AliasMap::builtin(),
        Arc::clone(&known),
        Default::default(),
    );
    let pairs = queries.len() as u64 * candidates.len() as u64;
    // One warmup iteration before timing the hard path, so the measured run does
    // not include first-touch allocation and thread-pool spin-up.
    {
        use rayon::prelude::*;
        let warm: Vec<usize> = queries
            .par_iter()
            .map(|q| matcher.score_candidates(q, &cand_index, &[], None).len())
            .collect();
        std::hint::black_box(warm);
    }
    // Serial (non-rayon) baseline for the rayon speedup report. The
    // PERFORMANCE_AUDIT acceptance is >=6x; the number is reported, not asserted.
    let serial_start = Instant::now();
    let serial_scored: Vec<usize> = queries
        .iter()
        .map(|q| matcher.score_candidates(q, &cand_index, &[], None).len())
        .collect();
    std::hint::black_box(&serial_scored);
    let serial_elapsed = serial_start.elapsed();
    println!(
        "    {:<42} {:>10.3} ms total ({:.2} M pairs/s over {} pairs)",
        format!(
            "score_candidates hard path {}q x {}c serial",
            queries.len(),
            candidates.len()
        ),
        serial_elapsed.as_secs_f64() * 1000.0,
        pairs as f64 / serial_elapsed.as_secs_f64() / 1e6,
        pairs
    );
    let start = Instant::now();
    {
        use rayon::prelude::*;
        let scored: Vec<usize> = queries
            .par_iter()
            .map(|q| matcher.score_candidates(q, &cand_index, &[], None).len())
            .collect();
        std::hint::black_box(scored);
    }
    let elapsed = start.elapsed();
    println!(
        "    {:<42} {:>10.3} ms total ({:.2} M pairs/s over {} pairs)",
        format!(
            "score_candidates hard path {}q x {}c rayon",
            queries.len(),
            candidates.len()
        ),
        elapsed.as_secs_f64() * 1000.0,
        pairs as f64 / elapsed.as_secs_f64() / 1e6,
        pairs
    );
    let speedup = serial_elapsed.as_secs_f64() / elapsed.as_secs_f64();
    println!(
        "    rayon speedup vs serial: {speedup:.2}x (PERFORMANCE_AUDIT target >=6x, reported only)"
    );
    let _ = index;
}
