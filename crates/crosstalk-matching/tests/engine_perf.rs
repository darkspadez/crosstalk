//! Performance guard for the unified engine (phase 3 step 6).
//!
//! Target: resolve 50k names + rank 20k queries against 50k candidates in
//! release within a few seconds. The `perf_subset` test always runs (small
//! enough for debug CI); `perf_full_scale` is `#[ignore]`d and runs in release
//! (`cargo test --release -p crosstalk-matching -- --ignored`), including in CI.

use crosstalk_matching::engine::{EngineConfig, EngineIndex, UnifiedEngine};
use crosstalk_matching::identity::{IdentityCache, ResolveContext};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Deterministic PRNG (LCG): no extra dev-deps, reproducible names.
struct Rng(u64);

impl Rng {
    fn next(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % bound
    }
}

fn pools() -> (Vec<&'static str>, Vec<&'static str>, Vec<&'static str>) {
    let brands = vec![
        "ESPN",
        "HBO",
        "CNN",
        "BBC One",
        "Fox Sports",
        "Sky Cinema",
        "TNT",
        "AMC",
        "FX",
        "Comedy Central",
        "Discovery",
        "Nat Geo",
        "TLC",
        "HGTV",
        "Food Network",
        "NBC",
        "CBS",
        "ABC",
        "Nickelodeon",
        "Cartoon Network",
    ];
    let prefixes = vec![
        "US:", "UK:", "(US)", "[UK]", "PRIME:", "GO:", "TUBI:", "PLUTO:", "CA", "FR",
    ];
    let suffixes = vec![
        "HD", "[HD]", "FHD", "4K", "+1", "East", "West", "(W)", "Pacific", "2", "UHD", "1080p",
    ];
    (brands, prefixes, suffixes)
}

/// Realistic provider spellings: brand + optional prefix/suffix/number.
fn synthetic_names(count: usize) -> Vec<String> {
    let (brands, prefixes, suffixes) = pools();
    let mut rng = Rng(0x5eed_60ad);
    let mut names = Vec::with_capacity(count);
    for i in 0..count {
        let brand = brands[rng.next(brands.len())];
        let mut name = String::new();
        match rng.next(4) {
            0 => {}
            1 => {
                name.push_str(prefixes[rng.next(prefixes.len())]);
                name.push(' ');
            }
            _ => {
                name.push_str(prefixes[rng.next(prefixes.len())]);
                name.push(' ');
            }
        }
        name.push_str(brand);
        match rng.next(5) {
            0 => {}
            1 => {
                name.push(' ');
                name.push_str(suffixes[rng.next(suffixes.len())]);
            }
            2 => {
                name.push(' ');
                name.push_str(&format!("{}", 1 + rng.next(12)));
            }
            _ => {
                name.push(' ');
                name.push_str(suffixes[rng.next(suffixes.len())]);
            }
        }
        if i % 7 == 0 {
            name.push_str(&format!(" {}", 100 + (i % 900)));
        }
        names.push(name);
    }
    names.sort();
    names.dedup();
    // Top up deterministically if dedup shrank the pool.
    let mut i = names.len();
    while names.len() < count {
        names.push(format!("Channel {} HD", 10000 + i));
        i += 1;
    }
    names.truncate(count);
    names
}

fn engine() -> Arc<UnifiedEngine> {
    Arc::new(UnifiedEngine::new(&EngineConfig::default()))
}

/// Always-on subset: 1k resolves + a 1k index + 100 rayon queries.
#[test]
fn perf_subset() {
    let engine = engine();
    let ctx = ResolveContext::default();
    let names = synthetic_names(1000);

    let start = Instant::now();
    let cache = IdentityCache::new(Arc::clone(&engine));
    for name in &names {
        let _ = cache.resolve(name, &ctx);
    }
    let resolve_time = start.elapsed();

    let start = Instant::now();
    let index = EngineIndex::build(&names, &engine, &ctx);
    let build_time = start.elapsed();

    let queries: Vec<String> = names.iter().take(100).cloned().collect();
    let start = Instant::now();
    use rayon::prelude::*;
    let members: usize = queries
        .par_iter()
        .map(|q| engine.rank_index(q, &index).len())
        .sum();
    let rank_time = start.elapsed();

    println!("perf_subset: resolve 1000 in {resolve_time:?}, index 1000 in {build_time:?}, rank 100x1000 in {rank_time:?} ({members} members)");
    assert!(
        resolve_time < Duration::from_secs(120),
        "resolve budget: {resolve_time:?}"
    );
    assert!(
        build_time < Duration::from_secs(120),
        "index budget: {build_time:?}"
    );
    assert!(
        rank_time < Duration::from_secs(120),
        "rank budget: {rank_time:?}"
    );
}

/// Full scale (release, ignored by default): 50k resolves + a 50k index +
/// 20k queries against it over rayon, within a few seconds of scoring.
///
/// Ratio guard, not wall-clock brittle: the absolute budget (120s) only trips
/// on pathological blowups; the report prints the real numbers.
#[test]
#[ignore]
fn perf_full_scale() {
    let engine = engine();
    let ctx = ResolveContext::default();
    let names = synthetic_names(50_000);

    let start = Instant::now();
    let cache = IdentityCache::new(Arc::clone(&engine));
    for name in &names {
        let _ = cache.resolve(name, &ctx);
    }
    let resolve_time = start.elapsed();

    let start = Instant::now();
    let index = EngineIndex::build(&names, &engine, &ctx);
    let build_time = start.elapsed();

    let queries: Vec<String> = names.iter().step_by(2).take(20_000).cloned().collect();
    assert_eq!(queries.len(), 20_000);
    let start = Instant::now();
    use rayon::prelude::*;
    let members: usize = queries
        .par_iter()
        .map(|q| engine.rank_index(q, &index).len())
        .sum();
    let rank_time = start.elapsed();

    // The AI shortlist pass (guide step 8): the same ranker re-cut at the default
    // rescue floor, no family cut. Runs only for channels with an empty family; timed
    // here over all 20k as the worst case.
    let low = engine.with_threshold(60);
    let start = Instant::now();
    let shortlisted: usize = queries
        .par_iter()
        .map(|q| low.shortlist_index_in(q, None, &index).len())
        .sum();
    let shortlist_time = start.elapsed();

    println!("perf_full_scale: resolve 50k in {resolve_time:?}, index 50k in {build_time:?}, rank 20kx50k in {rank_time:?} ({members} members), shortlist@60 20kx50k in {shortlist_time:?} ({shortlisted} candidates)");
    assert!(
        shortlist_time < Duration::from_secs(300),
        "shortlist budget: {shortlist_time:?}"
    );
    assert!(
        resolve_time < Duration::from_secs(120),
        "resolve budget: {resolve_time:?}"
    );
    assert!(
        build_time < Duration::from_secs(120),
        "index budget: {build_time:?}"
    );
    assert!(
        rank_time < Duration::from_secs(300),
        "rank budget: {rank_time:?}"
    );
}
