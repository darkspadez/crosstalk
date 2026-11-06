//! Shared channel identity: one module resolving channels, streams and EPG entries.
//!
//! Phase 3 step 6 (goal reset). Every app matches on identities: `resolve` computes an
//! [`Identity`] once per name per run (memoized in memory, persisted in the store's
//! `identity_cache` table keyed by input + [`UnifiedEngine::identity_version`]), and
//! [`rank_with`] returns the best identity FAMILY — all candidates resolving to the
//! same identity as the top one — which is the answer shape every app uses (plan: one
//! ranker + one selection rule). Guide assigns the family's first entry with programme data; streams
//! attaches the whole family; lineup writes the family's canonical name.
//!
//! # Identity equality
//!
//! Two identities are the same channel ([`Identity::same_family`]) when their
//! [`FamilyKey`] agrees AND their countries are compatible:
//! - `key`: the decided normalize key (lowercased; nospace-insensitive only for alias
//!   resolution, never for identity);
//! - `zone`: the routed feed zone ([`FeedZone`], `(P)` = `Default` per Z07);
//! - `timeshift`: the `+N` marker flag;
//! - `callsign`: the normalized shared-core callsign (`None` == `None`; a one-sided
//!   callsign still distinguishes, because `ABC (WABC)` and `ABC` are different
//!   broadcast entities);
//! - `country`, when KNOWN ON BOTH SIDES: `UK: CNN` and `US: CNN` share a key but are
//!   different feeds, so they never share a family (Stream-Mapparr bug-158's
//!   country-qualified grouping, Lineuparr's strict cross-border rule, G03). An
//!   unknown country is compatible with any (`CNN` joins either; LU/SM keep untagged
//!   streams, fail-open), the genuinely shared US<->MX feeds stay compatible
//!   ([`lineuparr_cross_border_ok`]), and a callsign-bearing identity ignores country
//!   (Stream-Mapparr bug-161: OTA identity is the FCC callsign; `CA:`/`IN:` state
//!   prefixes read as foreign countries).
//!
//! Country is a compatibility rule rather than a [`FamilyKey`] field because unknown
//! is a wildcard. A family therefore fixes ONE country: the query's (name marker or
//! [`ResolveContext::country_hint`]), else the top candidate's, else the first known
//! country among the top's key-mates in rank order — so every member of a family is
//! pairwise [`same_family`](Identity::same_family).
//!
//! Deliberately NOT part of equality: `network`, `category`, `region` label and `tags`
//! (quality etc.) — those are attributes a feed carries, not identity. In particular
//! `Howard Stern 100` vs `Howard Stern 101`, `BBC One` vs `BBC Two`, `ESPN` vs
//! `ESPN 2`, `X` vs `X +1` and `X` vs `X West` never share a family: their keys
//! differ (number/designator/zone tokens survive normalisation by decision).

use crate::country::{lineuparr_cross_border_ok, lineuparr_stream_country, FeedZone};
use crate::engine::{is_time_shift, EngineMethod, UnifiedEngine};
use crate::tags::{split_tags, TagKind};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The hashable part of identity equality: key + zone + timeshift + callsign.
///
/// The ONE definition both [`Identity::same_family`] and the engine's candidate index
/// (`EngineIndex`) group on; country compatibility is applied on top (module docs).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FamilyKey {
    pub key: String,
    pub zone: FeedZone,
    pub timeshift: bool,
    pub callsign: Option<String>,
}

/// Whether two countries may belong to one family (module docs, "Identity equality").
///
/// `exempt` is true for callsign-bearing identities (bug-161); `key` is the family's
/// normalize key, read by the US<->MX shared-feed list.
pub fn countries_compatible(a: Option<&str>, b: Option<&str>, exempt: bool, key: &str) -> bool {
    match (a, b) {
        _ if exempt => true,
        (Some(a), Some(b)) => lineuparr_cross_border_ok(a, b, key),
        _ => true,
    }
}

/// A channel/stream/EPG entry's shared identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// Canonical display name: the alias-resolved canonical when the name is a known
    /// alias, else the verbatim name trimmed.
    pub canonical_name: String,
    /// Extracted callsign (shared-core ladder, merged allowlist), if any.
    pub callsign: Option<String>,
    /// Bare network keyword (`ABC`, `ESPN`), if the name carries one.
    pub network: Option<String>,
    /// Country: the context hint, else the name's Lineuparr stream marker, if any.
    pub country: Option<String>,
    /// Category, when the caller supplies one (lineup files, M3U groups).
    pub category: Option<String>,
    /// Display region label from [`extract_tags`] (`East`, `Pacific`, ...), if any.
    pub region: Option<String>,
    /// Routed feed zone (Z01/Z02/Z07).
    pub zone: FeedZone,
    /// `+N` time-shift marker present.
    pub timeshift: bool,
    /// Quality and other display tags (`[HD]`, `(W)` re-attachments, ...).
    pub tags: Vec<String>,
    /// The normalize key this identity compares on (decided normalizer, lowercased).
    pub key: String,
}

impl Identity {
    /// The hashable equality fields (everything but the country rule).
    pub fn family_key(&self) -> FamilyKey {
        FamilyKey {
            key: self.key.clone(),
            zone: self.zone,
            timeshift: self.timeshift,
            callsign: self.callsign.clone(),
        }
    }

    /// Identity equality: [`FamilyKey`] equality plus country compatibility.
    ///
    /// See the module docs for the rule and for why network/category/region/tags are
    /// excluded.
    pub fn same_family(&self, other: &Identity) -> bool {
        self.key == other.key
            && self.zone == other.zone
            && self.timeshift == other.timeshift
            && self.callsign == other.callsign
            && countries_compatible(
                self.country.as_deref(),
                other.country.as_deref(),
                self.callsign.is_some(),
                &self.key,
            )
    }
}

/// Context for [`resolve_with`]: what the caller knows about ONE name beyond its text.
///
/// Engine-wide settings (enabled countries, custom aliases, threshold, normalisation
/// toggles) live in [`crate::engine::EngineConfig`]; the cache version covers them
/// ([`UnifiedEngine::identity_version`]).
#[derive(Debug, Clone, Default)]
pub struct ResolveContext {
    /// Optional country for this name (lineup country, M3U group country); wins over
    /// the name's own marker.
    pub country_hint: Option<String>,
    /// Optional category for this name (lineup category, M3U group).
    pub category: Option<String>,
}

/// Resolve `name` to its [`Identity`] through the engine.
///
/// Surrounding whitespace never changes an identity (the cache keys on the trimmed
/// name), so resolution reads the trimmed name throughout.
pub fn resolve_with(engine: &UnifiedEngine, name: &str, ctx: &ResolveContext) -> Identity {
    let name = name.trim();
    let key = engine.normalize_key(name).to_lowercase();
    let callsign = engine
        .callsign_of(name)
        .as_deref()
        .map(crate::callsign::normalize_callsign);
    let network = crate::callsign::network_keyword(name);
    let detected = lineuparr_stream_country(name).map(str::to_string);
    let country = ctx.country_hint.clone().or(detected);
    // Step 8 (migration list): the ONE tag extractor is the shared vocabulary
    // (`tags::split_tags`), not SM's `country::extract_tags` (D09) beside it. The
    // trailing run classifies by kind: the first region word is the display region,
    // every other tag is carried verbatim in brackets (shifts keep their `+N` shape).
    // This differs from SM on bare `Central`/`Atlantic` (real names: `Comedy Central`,
    // `Sky Atlantic` — bracketed only here), on `Middle`/`Far`/`Wild`-led place names
    // (`Middle East` is a place, not a feed region), on `+N` shifts (tags, not core)
    // and on bracket-only quality — none of which the family key, the zone or the
    // country rule reads, so the corpus gate is unaffected.
    let (_core, split) = split_tags(name);
    let regional = split
        .iter()
        .find(|tag| tag.kind == TagKind::Regional)
        .map(|tag| tag.canonical.clone());
    let mut tags = Vec::with_capacity(split.len());
    for tag in &split {
        match tag.kind {
            TagKind::Regional => {}
            TagKind::Timeshift => tags.push(tag.canonical.clone()),
            _ => tags.push(format!("[{}]", tag.canonical)),
        }
    }
    let canonical_name = engine
        .alias_canonical(name)
        .unwrap_or_else(|| name.to_string());
    Identity {
        canonical_name,
        callsign,
        network,
        country,
        category: ctx.category.clone(),
        region: regional,
        zone: crate::country::extract_zone_noguard(name),
        timeshift: is_time_shift(name),
        tags,
        key,
    }
}

/// One family member: the candidate's index, identity, score and method.
#[derive(Debug, Clone)]
pub struct FamilyMember {
    pub index: usize,
    pub identity: Identity,
    pub score: u8,
    pub method: EngineMethod,
}

/// Rank candidates against a query and return the best identity FAMILY.
///
/// The family itself comes from the engine's ONE ranker ([`UnifiedEngine::rank_in`]:
/// score, country rule, cut, family cut); this only attaches each member's identity.
/// `ctx` describes the QUERY (its country hint fixes the family's country); candidates
/// resolve from their own names. Empty when nothing reaches the cut.
pub fn rank_with(
    engine: &UnifiedEngine,
    query: &str,
    candidates: &[String],
    ctx: &ResolveContext,
) -> Vec<FamilyMember> {
    let candidate_ctx = ResolveContext::default();
    engine
        .rank_in(query, ctx.country_hint.as_deref(), candidates)
        .into_iter()
        .map(|entry| FamilyMember {
            identity: resolve_with(engine, &candidates[entry.index], &candidate_ctx),
            index: entry.index,
            score: entry.score,
            method: entry.method,
        })
        .collect()
}

/// Memoized identity resolver: one [`Identity`] per distinct input per run.
///
/// `Send + Sync`; the memo is one mutex-held map, locked only for a lookup or an
/// insert (never across a resolve), so rayon threads share it freely. Two threads
/// racing on the same new input both compute it and insert equal values: resolution
/// is a pure function of (input, context, version). Backed by the store's
/// `identity_cache` table across runs (see `crosstalk_core::identity_cache`); the
/// version ([`UnifiedEngine::identity_version`]) covers the data, the rules AND the
/// engine settings identity depends on, so changing any of them misses every row.
#[derive(Debug, Clone)]
pub struct IdentityCache {
    engine: Arc<UnifiedEngine>,
    memo: Arc<Mutex<HashMap<String, Identity>>>,
    version: String,
}

impl IdentityCache {
    pub fn new(engine: Arc<UnifiedEngine>) -> Self {
        let version = engine.identity_version();
        Self {
            engine,
            memo: Arc::new(Mutex::new(HashMap::new())),
            version,
        }
    }

    pub fn engine(&self) -> &Arc<UnifiedEngine> {
        &self.engine
    }

    /// Cache key for an input: the trimmed name (case kept: callsigns, canonical
    /// names and tags are case-sensitive) plus the context, as one JSON array so no
    /// separator can collide. The version namespaces it separately.
    pub fn input_key(name: &str, ctx: &ResolveContext) -> String {
        serde_json::json!([name.trim(), ctx.country_hint, ctx.category]).to_string()
    }

    pub fn data_version(&self) -> &str {
        &self.version
    }

    /// A memoized identity for `key`, if this run already resolved it.
    pub fn memoized(&self, key: &str) -> Option<Identity> {
        self.memo.lock().expect("identity memo").get(key).cloned()
    }

    /// Insert a known identity into the memo (warming it from a store row).
    pub fn memoize(&self, key: String, identity: Identity) {
        self.memo
            .lock()
            .expect("identity memo")
            .insert(key, identity);
    }

    /// Resolve, memoizing per run. Same input yields the same identity regardless of
    /// call order or thread: resolution is a pure function of (input, version).
    pub fn resolve(&self, name: &str, ctx: &ResolveContext) -> Identity {
        let key = Self::input_key(name, ctx);
        if let Some(hit) = self.memoized(&key) {
            return hit;
        }
        let identity = resolve_with(&self.engine, name, ctx);
        self.memoize(key, identity.clone());
        identity
    }

    /// Rank through the cache: the family from the engine's one ranker, identities
    /// from the memo (see [`rank_with`] for `ctx`).
    pub fn rank(
        &self,
        query: &str,
        candidates: &[String],
        ctx: &ResolveContext,
    ) -> Vec<FamilyMember> {
        let candidate_ctx = ResolveContext::default();
        self.engine
            .rank_in(query, ctx.country_hint.as_deref(), candidates)
            .into_iter()
            .map(|entry| FamilyMember {
                identity: self.resolve(&candidates[entry.index], &candidate_ctx),
                index: entry.index,
                score: entry.score,
                method: entry.method,
            })
            .collect()
    }

    /// Serialize an identity for the store row (`identity_json`).
    pub fn identity_json(identity: &Identity) -> String {
        serde_json::json!({
            "canonical_name": identity.canonical_name,
            "callsign": identity.callsign,
            "network": identity.network,
            "country": identity.country,
            "category": identity.category,
            "region": identity.region,
            "zone": match identity.zone {
                FeedZone::East => "EAST",
                FeedZone::West => "WEST",
                FeedZone::Default => "DEFAULT",
            },
            "timeshift": identity.timeshift,
            "tags": identity.tags,
            "key": identity.key,
        })
        .to_string()
    }

    /// Parse a store row back. `None` on any shape drift (the caller re-resolves).
    pub fn identity_from_json(json: &str) -> Option<Identity> {
        let value: serde_json::Value = serde_json::from_str(json).ok()?;
        let zone = match value.get("zone")?.as_str()? {
            "EAST" => FeedZone::East,
            "WEST" => FeedZone::West,
            _ => FeedZone::Default,
        };
        Some(Identity {
            canonical_name: value.get("canonical_name")?.as_str()?.to_string(),
            callsign: value
                .get("callsign")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            network: value
                .get("network")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            country: value
                .get("country")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            category: value
                .get("category")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            region: value
                .get("region")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            zone,
            timeshift: value.get("timeshift")?.as_bool()?,
            tags: value
                .get("tags")?
                .as_array()?
                .iter()
                .filter_map(|t| t.as_str().map(str::to_string))
                .collect(),
            key: value.get("key")?.as_str()?.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{EngineConfig, UnifiedEngine};

    fn engine() -> Arc<UnifiedEngine> {
        Arc::new(UnifiedEngine::new(&EngineConfig::default()))
    }

    fn ctx() -> ResolveContext {
        ResolveContext::default()
    }

    #[test]
    fn provider_variants_resolve_to_the_same_identity() {
        let engine = engine();
        let base = resolve_with(&engine, "ESPN", &ctx());
        for variant in ["US: ESPN HD", "ESPN [HD]", "PRIME: ESPN", "(US) ESPN"] {
            let identity = resolve_with(&engine, variant, &ctx());
            assert!(
                identity.same_family(&base),
                "{variant:?} -> {identity:?} vs {base:?}"
            );
        }
    }

    #[test]
    fn near_misses_never_share_a_family() {
        let engine = engine();
        let candidates = [
            "BBC One", "BBC Two", "ESPN", "ESPN 2", "HBO", "HBO +1", "HBO East", "HBO West",
        ];
        for (left, right) in [
            ("BBC One", "BBC Two"),
            ("ESPN", "ESPN 2"),
            ("HBO", "HBO +1"),
            ("HBO East", "HBO West"),
            ("BBC One", "ESPN"),
        ] {
            let a = resolve_with(&engine, left, &ctx());
            let b = resolve_with(&engine, right, &ctx());
            assert!(!a.same_family(&b), "{left:?} vs {right:?}");
        }
        let _ = candidates;
    }

    #[test]
    fn rank_returns_the_winners_family_only() {
        let engine = engine();
        let candidates = vec![
            "ESPN".to_string(),
            "US: ESPN HD".to_string(),
            "ESPN 2".to_string(),
            "Weather Channel".to_string(),
        ];
        let family = rank_with(&engine, "ESPN HD", &candidates, &ctx());
        assert!(family.len() >= 2, "{family:?}");
        assert!(family.iter().all(|m| m.index <= 1), "{family:?}");
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| name.to_string()).collect()
    }

    fn family_names(family: &[FamilyMember], candidates: &[String]) -> Vec<String> {
        family
            .iter()
            .map(|member| candidates[member.index].clone())
            .collect()
    }

    #[test]
    fn a_country_known_on_both_sides_is_part_of_identity() {
        let engine = engine();
        let uk = resolve_with(&engine, "UK: CNN", &ctx());
        let us = resolve_with(&engine, "US: CNN HD", &ctx());
        let bare = resolve_with(&engine, "CNN", &ctx());
        assert_eq!(uk.family_key(), us.family_key(), "same key, zone, shift");
        assert!(
            !uk.same_family(&us),
            "UK and US feeds are different channels"
        );
        assert!(
            bare.same_family(&uk) && bare.same_family(&us),
            "unknown is a wildcard"
        );
        // A country hint counts as known.
        let hinted = resolve_with(
            &engine,
            "CNN",
            &ResolveContext {
                country_hint: Some("UK".to_string()),
                ..ResolveContext::default()
            },
        );
        assert!(hinted.same_family(&uk) && !hinted.same_family(&us));
        // Genuinely shared US<->MX feeds stay one channel (G03).
        let us_tm = resolve_with(&engine, "US: Telemundo", &ctx());
        let mx_tm = resolve_with(&engine, "MX: Telemundo", &ctx());
        assert!(us_tm.same_family(&mx_tm));
        let mx_espn = resolve_with(&engine, "MX: ESPN", &ctx());
        assert!(!resolve_with(&engine, "US: ESPN", &ctx()).same_family(&mx_espn));
    }

    #[test]
    fn a_callsign_identity_ignores_country() {
        // bug-161: `CA:`/`IN:` are US state prefixes on OTA groups; the callsign is
        // the identity.
        let engine = engine();
        let a = resolve_with(&engine, "CA: ABC (KABC)", &ctx());
        let b = resolve_with(&engine, "US: ABC (KABC)", &ctx());
        assert_eq!(a.callsign.as_deref(), Some("KABC"));
        assert_ne!(a.country, b.country);
        assert!(a.same_family(&b));
    }

    #[test]
    fn rank_keeps_one_country_per_family() {
        let engine = engine();
        // A US query never takes the UK feed, even with no enabled countries.
        let candidates = names(&["UK: CNN", "US: CNN HD", "CNN"]);
        let family = rank_with(&engine, "US: CNN", &candidates, &ctx());
        assert_eq!(family_names(&family, &candidates), ["US: CNN HD", "CNN"]);
        // So does a query whose country is only a hint (lineup / M3U group).
        let us = ResolveContext {
            country_hint: Some("US".to_string()),
            ..ResolveContext::default()
        };
        let family = rank_with(&engine, "CNN", &candidates, &us);
        assert_eq!(family_names(&family, &candidates), ["US: CNN HD", "CNN"]);
        // An unknown-country query still gets ONE country: never both feeds.
        for order in [
            names(&["UK: CNN", "US: CNN", "CNN HD"]),
            names(&["CNN HD", "US: CNN", "UK: CNN"]),
        ] {
            let family = rank_with(&engine, "CNN", &order, &ctx());
            assert!(family.len() >= 2, "{family:?}");
            for a in &family {
                for b in &family {
                    assert!(a.identity.same_family(&b.identity), "{a:?} vs {b:?}");
                }
            }
        }
        // A foreign-only pool yields nothing for a country-marked query.
        let family = rank_with(&engine, "US: CNN", &names(&["UK: CNN"]), &ctx());
        assert!(family.is_empty(), "{family:?}");
    }

    #[test]
    fn enabled_countries_filter_every_candidate_path() {
        let engine = UnifiedEngine::new(&EngineConfig {
            enabled_countries: vec!["US".to_string()],
            ..EngineConfig::default()
        });
        // Exact-map path: a same-key foreign feed must not re-enter via the family.
        let candidates = names(&["UK: CNN", "CNN HD"]);
        let family = rank_with(&engine, "CNN", &candidates, &ctx());
        assert_eq!(family_names(&family, &candidates), ["CNN HD"]);
        // Fuzzy-block path (token-sort, no exact-map hit): a foreign candidate is
        // dropped by G03 too.
        let candidates = names(&["UK: France 2"]);
        let open = UnifiedEngine::decided();
        let hit = rank_with(&open, "2 france", &candidates, &ctx());
        assert!(matches!(
            hit[..],
            [FamilyMember {
                method: EngineMethod::Fuzzy(_),
                ..
            }]
        ));
        assert!(rank_with(&engine, "2 france", &candidates, &ctx()).is_empty());
    }

    #[test]
    fn index_family_keys_match_resolve() {
        let engine = engine();
        let candidates = names(&[
            "US: ESPN HD",
            "HBO East",
            "HBO West",
            "Sky Sports F1 UHD",
            "(KING)",
            "ABC 7 (KGO)",
            "Sportsnet (P)",
            "Comedy Central +1",
            "  BBC One  ",
            "\u{2605} CNN \u{2605}",
        ]);
        let index = crate::engine::EngineIndex::build(&candidates, &engine, &ctx());
        for (i, name) in candidates.iter().enumerate() {
            let identity = resolve_with(&engine, name, &ctx());
            assert_eq!(index.family_key(i), &identity.family_key(), "{name:?}");
            assert_eq!(index.identity_country(i), identity.country.as_deref());
        }
    }

    #[test]
    fn cache_keys_keep_case_and_versions_cover_identity_settings() {
        assert_ne!(
            IdentityCache::input_key("espn", &ctx()),
            IdentityCache::input_key("ESPN", &ctx())
        );
        assert_eq!(
            IdentityCache::input_key(" ESPN ", &ctx()),
            IdentityCache::input_key("ESPN", &ctx())
        );
        let base = UnifiedEngine::decided().identity_version();
        let mut custom_aliases = HashMap::new();
        custom_aliases.insert("CNN".to_string(), vec!["Cable News".to_string()]);
        let aliased = UnifiedEngine::new(&EngineConfig {
            custom_aliases,
            ..EngineConfig::default()
        });
        assert_ne!(
            base,
            aliased.identity_version(),
            "custom aliases change identities"
        );
        assert_eq!(
            aliased.alias_canonical("Cable News").as_deref(),
            Some("CNN"),
            "and the alias resolves"
        );
        let mut flags = EngineConfig::default().flags;
        flags.ignore_quality = !flags.ignore_quality;
        let toggled = UnifiedEngine::new(&EngineConfig {
            flags,
            ..EngineConfig::default()
        });
        assert_ne!(base, toggled.identity_version(), "toggles change keys");
        // Threshold and enabled countries never change an identity.
        let scoring = UnifiedEngine::new(&EngineConfig {
            threshold: 80,
            enabled_countries: vec!["US".to_string()],
            ..EngineConfig::default()
        });
        assert_eq!(base, scoring.identity_version());
    }

    #[test]
    fn cache_identity_json_round_trips() {
        let cache = IdentityCache::new(engine());
        let identity = cache.resolve("US: ESPN HD", &ctx());
        let json = IdentityCache::identity_json(&identity);
        let back = IdentityCache::identity_from_json(&json).expect("round trip");
        assert_eq!(identity, back);
        assert!(IdentityCache::identity_from_json("garbage").is_none());
    }
}
