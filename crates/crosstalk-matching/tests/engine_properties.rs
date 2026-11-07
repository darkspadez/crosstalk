//! Property and consistency tests for the unified engine (phase 3 step 6).
//!
//! These pin the cross-app contract: one name yields one identity in every app,
//! because every app resolves through the same [`UnifiedEngine`] + [`IdentityCache`].

use crosstalk_matching::engine::{EngineConfig, UnifiedEngine};
use crosstalk_matching::identity::{resolve_with, IdentityCache, ResolveContext};
use std::collections::HashSet;
use std::sync::Arc;

fn engine() -> Arc<UnifiedEngine> {
    Arc::new(UnifiedEngine::new(&EngineConfig::default()))
}

fn ctx() -> ResolveContext {
    ResolveContext::default()
}

/// Same input resolves to the same identity regardless of call order.
#[test]
fn identity_is_independent_of_call_order() {
    let engine = engine();
    let cache = IdentityCache::new(engine);
    let names = [
        "US: ESPN HD",
        "HBO East",
        "Sky Sports F1 UHD",
        "(KING)",
        "Sportsnet (P)",
        "JusticeCentral.TV",
        "BBC One",
        "Comedy Central +1",
    ];
    let forward: Vec<_> = names
        .iter()
        .map(|name| cache.resolve(name, &ctx()))
        .collect();
    let mut backward: Vec<_> = names
        .iter()
        .rev()
        .map(|name| cache.resolve(name, &ctx()))
        .collect();
    backward.reverse();
    assert_eq!(forward, backward);
}

/// Same input resolves to the same identity across threads.
#[test]
fn identity_is_independent_of_thread() {
    let cache = Arc::new(IdentityCache::new(engine()));
    let names: Vec<String> = (0..200).map(|i| format!("Channel {} HD", i % 50)).collect();
    let expected: Vec<_> = names
        .iter()
        .map(|name| cache.resolve(name, &ctx()))
        .collect();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let cache = Arc::clone(&cache);
            let names = names.clone();
            std::thread::spawn(move || {
                names
                    .iter()
                    .map(|name| cache.resolve(name, &ctx()))
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    for handle in handles {
        assert_eq!(handle.join().expect("thread"), expected);
    }
}

/// A name and its provider variants resolve to the same identity.
#[test]
fn provider_variants_share_identity() {
    let engine = engine();
    let families = [
        vec![
            "ESPN",
            "US: ESPN HD",
            "ESPN [HD]",
            "PRIME: ESPN",
            "(US) ESPN",
            "ESPN FHD",
        ],
        vec!["BBC One", "UK: BBC One", "BBC 1", "BBC One HD"],
        vec!["HBO", "HBO HD", "US: HBO", "HBO [FHD]"],
        vec!["Sky Sports F1", "Sky Sports F1 HD", "Sky Sports F1 UHD"],
    ];
    for family in families {
        let base = resolve_with(&engine, family[0], &ctx());
        for variant in &family[1..] {
            let identity = resolve_with(&engine, variant, &ctx());
            assert!(
                identity.same_family(&base),
                "{variant:?} diverged from {:?}: {identity:?} vs {base:?}",
                family[0]
            );
        }
    }
}

/// Near-miss pairs never share a family, at resolve level and at rank level.
#[test]
fn near_misses_never_share_a_family() {
    let engine = engine();
    let pairs = [
        ("BBC One", "BBC Two"),
        ("ESPN", "ESPN 2"),
        ("HBO", "HBO +1"),
        ("ITV2", "ITV2 +1"),
        ("HBO East", "HBO West"),
        ("HBO", "HBO West"),
        ("Sportsnet", "Sportsnet Pacific"),
        ("Sky Sports 1", "Sky Sports 2"),
        ("Fox Sports 1", "Fox Sports 2"),
        ("ABC", "ABC News"),
        ("Sky Cinema Disney", "Sky Cinema Decades"),
    ];
    for (left, right) in pairs {
        let a = resolve_with(&engine, left, &ctx());
        let b = resolve_with(&engine, right, &ctx());
        assert!(!a.same_family(&b), "{left:?} vs {right:?} share a family");
        // And at rank level: the query's family never contains the near-miss.
        let candidates = vec![left.to_string(), right.to_string()];
        let family = engine.rank(left, &candidates);
        assert!(
            family.iter().all(|entry| candidates[entry.index] != right),
            "{left:?} ranked {right:?} into its family"
        );
    }
}

/// Rank is deterministic: repeated calls and reversed candidate order agree on
/// the winner's identity (member order may follow score ties, identity may not).
#[test]
fn rank_winner_identity_is_order_independent() {
    let engine = engine();
    let candidates = vec![
        "ESPN 2".to_string(),
        "Weather Channel".to_string(),
        "US: ESPN HD".to_string(),
        "ESPN".to_string(),
    ];
    let first = engine.rank("ESPN", &candidates);
    let mut reversed = candidates.clone();
    reversed.reverse();
    let second = engine.rank("ESPN", &reversed);
    assert!(!first.is_empty() && !second.is_empty());
    let top_a = resolve_with(&engine, &candidates[first[0].index], &ctx());
    let top_b = resolve_with(&engine, &reversed[second[0].index], &ctx());
    assert!(top_a.same_family(&top_b), "{top_a:?} vs {top_b:?}");
    // Score sets agree as multisets.
    let mut scores_a: Vec<u8> = first.iter().map(|entry| entry.score).collect();
    let mut scores_b: Vec<u8> = second.iter().map(|entry| entry.score).collect();
    scores_a.sort_unstable();
    scores_b.sort_unstable();
    assert_eq!(scores_a, scores_b);
}

/// Data version changes when the engine version does (cache invalidation shape).
#[test]
fn data_version_is_namespaced() {
    let version = crosstalk_matching::engine::data_version();
    // Follow-up fix ranks network-led callsign names onto the station (C07): `eng:5`.
    assert!(version.contains("eng:5"), "{version}");
    // Two calls agree (memoized), and distinct inputs key distinctly.
    assert_eq!(version, crosstalk_matching::engine::data_version());
    let a = IdentityCache::input_key("ESPN", &ctx());
    let b = IdentityCache::input_key("ESPN ", &ctx());
    assert_eq!(a, b, "input keys trim");
    let mut ctx_country = ctx();
    ctx_country.country_hint = Some("US".to_string());
    assert_ne!(a, IdentityCache::input_key("ESPN", &ctx_country));
}

/// Distinct canonicals stay distinct: a spot check over merged-DB neighbors.
#[test]
fn merged_neighbors_stay_distinct() {
    let engine = engine();
    let names = [
        "CNN", "HLN", "BBC One", "BBC Two", "ESPN", "ESPN 2", "HBO", "Cinemax",
    ];
    let mut keys = HashSet::new();
    for name in names {
        let identity = resolve_with(&engine, name, &ctx());
        keys.insert(identity.key.clone());
    }
    // Every one of these is its own channel; keys must all differ.
    assert_eq!(keys.len(), names.len(), "{keys:?}");
}

/// Decoration never moves the family key: a quality tag after or before the name,
/// a bracketed tag, or pipe junk (decided N15) leaves key, zone, timeshift and
/// callsign alone. Pins the step-14 review: `National Geographic USA FHD` and
/// `HD National Geographic USA` key alike (N14 stays `core`; Janitor's bare
/// `USA?\s` strip keyed them apart), and a pipe never strands a country word.
///
/// Names led by a provider/country prefix are not in the list: the core strips
/// prefixes before quality tags (decided N12=`first`), so a leading `HD` hides
/// them (`HD USA Today` keeps `usa`).
#[test]
fn decoration_keeps_the_family_key() {
    let engine = engine();
    let key = |name: &str| resolve_with(&engine, name, &ctx()).family_key();
    let names = [
        "National Geographic USA",
        "National Geographic",
        "5 USA",
        "5",
        "Fear Factor USA",
        "ScreenPix US",
        "USA Network",
        "CNN",
        "ESPN2",
        "BBC One",
        "BBC One West",
        "HBO East",
        "Nickelodeon East",
        "Antenna TV Pacific",
        "Starz Encore West",
        "Sportsnet (P)",
        "Comedy Central +1",
        "ITV1 +1",
        "KABC (ABC)",
        "NBC 4 (WNBC)",
        "Sky Sports Premier League",
        "Sky Sports | Premier League",
        "Sky Sports F1",
        "Discovery Channel",
        "This Is Us TV",
        "AccuWeather",
        "A&E",
        "E!",
        "Canal+",
        "TSN 2",
    ];
    for name in names {
        let bare = key(name);
        for decorated in [
            format!("{name} FHD"),
            format!("{name} [4K]"),
            format!("HD {name}"),
            format!("{name} | HD |"),
        ] {
            assert_eq!(key(&decorated), bare, "{decorated:?} vs {name:?}");
        }
    }
    // A pipe beside a country code keeps a delimiter the core strips (N15).
    for name in ["US | CNN", "RK: US | CNN", "UK | CNN | HD", "CNN | US"] {
        assert_eq!(key(name), key("CNN"), "{name:?}");
    }
}
