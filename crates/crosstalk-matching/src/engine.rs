//! The unified matching engine: ONE code path implementing every decided row.
//!
//! Phase 3 step 6. The decided configuration is defined executably by
//! `scripts/corpus/ablate.py` (a base engine — Lineuparr's matcher over the shared
//! core — with each row's chosen variant swapped in) and recorded in
//! `data/corpus/decisions.json` + `docs/matching-decisions.md`. This module is the Rust
//! reproduction of THAT configured Python engine, and `scripts/corpus/engine_parity.py`
//! checks the two agree pair by pair (target 0 mismatches).
//!
//! # How each decided row maps to code
//!
//! | row | chosen | here |
//! |-----|--------|------|
//! | A01 `identity` | both names through CM's reverse index over the merged table | [`UnifiedEngine::alias_canonical`] + [`UnifiedEngine::same`] step 1 |
//! | A02 `cm-index` | CM's reverse (verbatim + nospace) index | [`UnifiedEngine::reverse_index`] (first-wins, sorted-canonical order) |
//! | A03 `merged-union` | the one alias table | [`merged_alias_entries`] over `data/merged/aliases.json` |
//! | A04 `none` | no alias-declared callsign rescue | absent by construction |
//! | C01 `core-known` | the shared core's ladder | [`CallsignLadder::SharedCoreNoRescue`] |
//! | C02 `merged` | merged station allowlist | [`merged_known_callsigns`] over `data/merged/stations.json` |
//! | C03 `sm` | parenthesized denylist rescue (SM) | the shared-core ladder's allowlist rescue |
//! | C04 `none` | no OTA-number rescue at the loose position | rescue disabled (never constructed) |
//! | C05 `shared` | one `normalize_callsign` | [`normalize_callsign`] (shared) |
//! | C06 `lu-anchor` | callsign decides at app level via the anchor | [`UnifiedEngine::same`] step 2 (95 floor / hard reject, high-confidence only) |
//! | C07 `callsign-first` | a network-led station callsign outranks the national entry | [`UnifiedEngine::same`] step 2a (different known callsign excluded) + [`UnifiedEngine::rank_index_in`] (same callsign first, national fallback) |
//! | D02/T10 `janitor-95` | the decision threshold | [`DEFAULT_THRESHOLD`] = 95, [`sensitivity_preset`] |
//! | D03 `none` | no channel-number boost | absent by construction (pairs carry no number) |
//! | D04 `jan-lu-anchor` | high-confidence anchor in `match_all_streams` | step 2, both sides high-confidence |
//! | D05 `sm` | OTA broadcast path | station-format names resolve via the merged allowlist (C02) |
//! | D06 `janitor` | EPG-side scoring (sole implementation) | [`Matcher::score_candidates`] stays the scorer; step 8 migrates guide onto families |
//! | D07 `sm-collector` | stream-side selection = best identity family | [`UnifiedEngine::rank_index_in`] returns the winner's identity family ([`crate::identity::rank_with`] attaches identities) |
//! | D08 `cm` | name-write rules incl unknown tagging | step 7 (template engine); not in this module |
//! | D09 `sm-both` | `extract_zone` beside `extract_tags` | [`extract_zone_noguard`] + [`extract_tags`] both available |
//! | D10 `cm` | channel-naming target selection | step 15 (lineup names); not in this module |
//! | G01 `lu` | country vocabulary | [`Lineuparr`] detector |
//! | G02 `lu` | detection entry points | [`lineuparr_stream_country`], [`lineuparr_name_country_prefix`], [`crate::country::lineuparr_category_country`] (lineup files), group scan |
//! | G03 `lu-strict` | strict cross-border + US<->MX shared feeds | [`lineuparr_cross_border_ok`] |
//! | G04 `lu-counter` | filter accounting | [`CountryFilterCount`] |
//! | N01 `core-strip` | core quality table (HDR/HEVC strip) | `core_pipeline: true` |
//! | N02 `core` | core bare-zone handling | core `normalize_core` as-is |
//! | N03 `core` | core misc leftovers | core `normalize_core` as-is |
//! | N04 `core` | core provider-prefix table | `core_pipeline: true` (N12=`first` order) |
//! | N05 `janitor-split` | `&` -> ` and ` | `split_ampersand: true` in [`normalize_core`] |
//! | N06 `janitor-letters` | dots split between ASCII letters only | `dots_letters_only: true` |
//! | N07 `janitor-floor` | suffix floor (two tokens must remain) | `suffix_floor: true` |
//! | N08 `sm-rejoin` | designator repair (`F 1` -> `F1` when glued in raw) | `rejoin_designators: true` |
//! | N09 `strip` | decorative-Unicode strip | core decorator strip as-is |
//! | N10 `keep` | bare-region-strip flag off | `_STRIP_BARE_REGION` never set |
//! | N11 `keep` | no Cinemax strip | `remove_cinemax: false` |
//! | N12 `first` | prefix strip before hyphen normalisation | core order as-is |
//! | N13 `strip` | invisible-format (Cf) strip | core Cf strip as-is |
//! | N14 `core` | core geographic table | `core_pipeline: true` |
//! | N15 `pipe-segments` | pipe junk collapses before key/zone/country; a country code keeps a delimiter | [`crate::tags::collapse_pipe_segments`] in [`UnifiedEngine::normalize_key`], [`extract_zone_noguard`], [`lineuparr_stream_country`] |
//! | R01 `fallback` | pure-Python-equivalent arithmetic | [`similarity`] fallback port (no rapidfuzz) |
//! | T01 `core` | core majority-overlap guard | [`has_token_overlap_core`] |
//! | T02 `always` | basic-mode token floor always on | every stage calls the guard (never skipped) |
//! | T03 `cm` | stage structure | alias -> exact -> substring -> token-sort |
//! | T04 `guarded` | exact 0.97 tier is guarded | exact tier also passes the overlap guard |
//! | T05 `scaled` | length-scaled threshold | [`length_scaled_threshold`] at every stage |
//! | T06 `jan-lu` | trailing-number guard | both-trailing-numbers-differ rejects |
//! | T07 `janitor-designator` | JAN disagreement shape over SM designator tokens | [`digit_guard_keep`] |
//! | T08 `cm` | `find_best_match` shape | in-loop guards (see [`Matcher::find_best_match`]) |
//! | T09 `lu` | `match_all_streams` shape | alias -> exact -> substring -> token-sort + anchor + filters |
//! | T11 `sm-flagged` | precompute cache flag memory | the candidate index is built with the run's flags |
//! | T12 `lu` | tie-break on equal score | score desc, then original-token overlap (stable) |
//! | Z01 `sm-zone` | feed-zone routing | `zone_routing_keep` over [`extract_zone_noguard`] |
//! | Z02 `none` | no single-time-zone guard | [`extract_zone_noguard`] (no country escape) |
//! | Z03 `none` | no zone expansion of channel DBs | DBs read verbatim |
//! | Z04 `none` | no East/West/Pacific post-match filter | routing (Z01) is the only zone rule |
//! | Z05 `lu-skip` | M3U group-header skip | [`is_group_header`] |
//! | Z06 `none` | no upgrade-quality segregation | absent by construction |
//! | Z07 `sm-strict` | `(P)` is NOT Pacific (orchestrator, Appendix C) | [`extract_zone_noguard`] never reads `(P)` |
//!
//! No per-plugin branches: the only knobs are the operator-facing
//! [`EngineConfig`] settings (`[matching]` threshold/sensitivity, enabled countries,
//! custom aliases, and the normalisation toggles that already exist — kept with their
//! semantics and mapped onto the engine).

use crate::alias::AliasMap;
use crate::callsign::{normalize_callsign, CallsignLadder, KnownCallsigns};
use crate::country::{
    lineuparr_cross_border_ok, lineuparr_stream_country, CountryFilterCount, FeedZone, Lineuparr,
    Relationship,
};
use crate::identity::{countries_compatible, FamilyKey};
use crate::normalize::{NormalizeFlags, Normalizer};
use crate::pyre::pythonize;
use crate::similarity::{calculate_similarity_chars, length_scaled_threshold};
use crate::tokens::{has_token_overlap_core, process_string_for_matching, trailing_number};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

pub use crate::country::{extract_zone_noguard, is_group_header};

/// Version of this engine's rule encoding. Bumped whenever a decided row's
/// implementation changes; part of the identity cache's `data_version`, so a rule
/// change invalidates cached identities.
///
/// Step 8 moved identity tag extraction onto the shared vocabulary (`tags::split_tags`
/// instead of SM's `country::extract_tags` beside it): `2`.
///
/// Step 9 reads feed zones after stripping trailing writer tags with the same
/// vocabulary (decided Z08: `HBO West (Backup)` is West): `3`.
///
/// Step 14 collapses `|`-delimited junk segments before keying and before the
/// zone/country reads with the same vocabulary (decided N15: `Cinemax East |
/// HD |` keys as `cinemax`, zone East): `4`.
///
/// Follow-up fix reads a network-led callsign name onto its station (decided C07
/// `callsign-first`: `ABC (WABC)` ranks the `WABC` family first): `5`.
pub const ENGINE_VERSION: &str = "5";

/// How many configurations [`UnifiedEngine::cached`] keeps built.
pub const ENGINE_CACHE_CAPACITY: usize = 8;

/// The decided default decision threshold (D02/T10 `janitor-95`).
///
/// Scores are integer percentages; a candidate reaches the family only at or above
/// this. The cut is on THIS engine's score (not Janitor's confidence scale with
/// structural bonuses), set by the cut sweep in `decide.py`.
pub const DEFAULT_THRESHOLD: u8 = 95;

/// Sensitivity presets on the decided scale (Channel-Mapparr's 70/80/90/95 ladder,
/// default 95). `sensitivity_preset` resolves a name to its cut.
pub const SENSITIVITY_PRESETS: [(&str, u8); 4] =
    [("low", 70), ("medium", 80), ("high", 90), ("strict", 95)];

/// Resolve a sensitivity preset name to its decision cut.
///
/// Accepts `low`/`medium`/`high`/`strict` (case-insensitive) and a bare number
/// (`"80"`). Unknown names yield [`DEFAULT_THRESHOLD`].
pub fn sensitivity_preset(name: &str) -> u8 {
    let trimmed = name.trim().to_lowercase();
    for (preset, cut) in SENSITIVITY_PRESETS {
        if trimmed == preset {
            return cut;
        }
    }
    if let Ok(cut) = trimmed.parse::<u8>() {
        if cut <= 100 {
            return cut;
        }
    }
    DEFAULT_THRESHOLD
}

/// Operator-facing settings for the unified engine (`[matching]`).
///
/// The normalisation toggles keep the semantics they already have (`ignore_*`
/// strip that tag class); the engine maps them onto its one pipeline. Structural
/// flags (core body, designator repair, amp split, letter dots, suffix floor) are
/// forced to the decided variants, never operator knobs.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Decision cut (default [`DEFAULT_THRESHOLD`]).
    pub threshold: u8,
    /// Country codes the run matches in (`["US"]`); empty means every country.
    pub enabled_countries: Vec<String>,
    /// Custom aliases, each key REPLACING the built-in list for that name.
    pub custom_aliases: HashMap<String, Vec<String>>,
    /// The operator's normalisation toggles (default: everything ignored).
    pub flags: NormalizeFlags,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            threshold: DEFAULT_THRESHOLD,
            enabled_countries: Vec::new(),
            custom_aliases: HashMap::new(),
            flags: NormalizeFlags::default(),
        }
    }
}

/// One merged alias entry: canonical name plus its variants.
#[derive(Debug, Clone)]
pub struct MergedAliasEntry {
    pub canonical: String,
    pub variants: Vec<String>,
}

/// Parse the merged alias table (`data/merged/aliases.json`, A03 `merged-union`).
///
/// Format: `{"aliases": {canonical: [{alias, sources}]}, "country_aliases": ...}`.
/// Entries return in FILE order (CM's `_rebuild_reverse_alias_index` keeps the
/// first-seen mapping on collision, and file order is what decides it).
pub fn merged_alias_entries() -> Vec<MergedAliasEntry> {
    static ENTRIES: OnceLock<Vec<MergedAliasEntry>> = OnceLock::new();
    ENTRIES
        .get_or_init(|| {
            // `aliases.json` is written sorted by canonical, and serde_json's Map
            // is a BTreeMap without preserve_order, so iteration here IS file
            // order — which is what CM's first-wins reverse index reads.
            let raw = crate::data::merged::MERGED_ALIASES_JSON;
            let value: serde_json::Value = serde_json::from_str(raw).unwrap_or_default();
            let mut out = Vec::new();
            if let Some(aliases) = value.get("aliases").and_then(|v| v.as_object()) {
                for (canonical, entries) in aliases {
                    let mut variants = Vec::new();
                    if let Some(list) = entries.as_array() {
                        for entry in list {
                            if let Some(alias) = entry.get("alias").and_then(|a| a.as_str()) {
                                variants.push(alias.to_string());
                            }
                        }
                    }
                    out.push(MergedAliasEntry {
                        canonical: canonical.clone(),
                        variants,
                    });
                }
            }
            out
        })
        .clone()
}

/// The merged station allowlist (C02 `merged`): every callsign in
/// `data/merged/stations.json`, plus each callsign's `-suffix` base.
pub fn merged_known_callsigns() -> KnownCallsigns {
    static KNOWN: OnceLock<KnownCallsigns> = OnceLock::new();
    KNOWN
        .get_or_init(|| {
            let mut known = KnownCallsigns::new();
            let raw = crate::data::merged::MERGED_STATIONS_JSON;
            let value: serde_json::Value = serde_json::from_str(raw).unwrap_or_default();
            if let Some(stations) = value.get("stations").and_then(|v| v.as_object()) {
                for callsign in stations.keys() {
                    let upper = callsign.to_uppercase();
                    known.insert(&upper);
                    if let Some(base) = upper.split('-').next() {
                        known.insert(base);
                    }
                }
            } else if let Some(stations) = value.get("stations").and_then(|v| v.as_array()) {
                for station in stations {
                    let callsign = station
                        .get("callsign")
                        .and_then(|c| c.as_str())
                        .or_else(|| station.as_str())
                        .unwrap_or("");
                    if !callsign.is_empty() {
                        let upper = callsign.to_uppercase();
                        known.insert(&upper);
                        if let Some(base) = upper.split('-').next() {
                            known.insert(base);
                        }
                    }
                }
            }
            if let Some(shipped) = KnownCallsigns::shipped() {
                for callsign in shipped.to_vec() {
                    known.insert(&callsign);
                }
            }
            known
        })
        .clone()
}

/// The `data_version` that invalidates the identity cache.
///
/// Derived from the merged-data provenance sha, the decisions sha and the engine
/// version, so any data or rule change invalidates every cached identity.
pub fn data_version() -> String {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            let provenance = crate::data::merged::MERGED_PROVENANCE_JSON;
            let decisions = include_str!("../../../data/corpus/decisions.json");
            format!(
                "prov:{}-dec:{}-eng:{}",
                fnv_hex(provenance),
                fnv_hex(decisions),
                ENGINE_VERSION
            )
        })
        .clone()
}

/// FNV-1a 64-bit, hex: a stable, dependency-free digest for versioning (not
/// security). Same input always yields the same digest.
fn fnv_hex(text: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

/// `+1`/`+2` time-shift markers in the ORIGINAL name (Janitor :99 / LU :46, identical).
fn plus_shift_re() -> &'static fancy_regex::Regex {
    static RE: OnceLock<fancy_regex::Regex> = OnceLock::new();
    RE.get_or_init(|| fancy_regex::Regex::new(&pythonize(r"\+\s{0,2}\d{1,2}\b")).expect("static"))
}

/// Whether the ORIGINAL name carries a `+N` time-shift marker.
pub fn is_time_shift(name: &str) -> bool {
    plus_shift_re().is_match(name).unwrap_or(false)
}

/// Collapse the separators ignored for the no-space comparison: whitespace, `&`, `-`.
fn letters_only_re() -> &'static fancy_regex::Regex {
    static RE: OnceLock<fancy_regex::Regex> = OnceLock::new();
    RE.get_or_init(|| fancy_regex::Regex::new(&pythonize(r"[\s&\-]+")).expect("static"))
}

fn nospace(text: &str) -> String {
    letters_only_re().replace_all(text, "").into_owned()
}

/// JAN disagreement shape over SM designator tokens (decided T07).
///
/// Numbered-vs-numbered sharing no designator refuses (`Sky Sports F1` vs
/// `Sky Sports UHD 1`); numbered-vs-unnumbered always passes (`ABC 7 (KGO)` vs
/// `ABC (KGO)` still matches). Shift equality is part of the same guard: a `+1`
/// channel only matches `+1` candidates.
pub fn digit_guard_keep(query: &str, candidate: &str, norm_q: &str, norm_c: &str) -> bool {
    let shift_ok = plus_shift_re().is_match(query).unwrap_or(false)
        == plus_shift_re().is_match(candidate).unwrap_or(false);
    if !shift_ok {
        return false;
    }
    let q_des: HashSet<String> = norm_q
        .split_whitespace()
        .filter(|t| crate::tokens::is_designator_token(t))
        .map(str::to_string)
        .collect();
    let c_des: HashSet<String> = norm_c
        .split_whitespace()
        .filter(|t| crate::tokens::is_designator_token(t))
        .map(str::to_string)
        .collect();
    !(!q_des.is_empty() && !c_des.is_empty() && q_des.is_disjoint(&c_des))
}

/// How a candidate scored on the unified engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineMethod {
    Alias,
    Exact,
    Substring,
    Fuzzy(u8),
    Callsign,
}

impl EngineMethod {
    pub fn label(&self) -> String {
        match self {
            EngineMethod::Alias => "alias".to_string(),
            EngineMethod::Exact => "exact".to_string(),
            EngineMethod::Substring => "substring".to_string(),
            EngineMethod::Fuzzy(score) => format!("fuzzy ({score})"),
            EngineMethod::Callsign => "callsign".to_string(),
        }
    }
}

/// One scored candidate: its index in the queried list, score and method.
#[derive(Debug, Clone)]
pub struct EngineRanked {
    pub index: usize,
    pub score: u8,
    pub method: EngineMethod,
}

/// The unified engine: one normalizer, ladder, alias set, guard path, zone rule.
///
/// `Send + Sync` with no interior mutability in hot paths: the alias reverse index,
/// allowlist and flags are built once; [`UnifiedEngine::same`] and
/// [`UnifiedEngine::rank`] take `&self` and share freely across rayon threads.
#[derive(Debug, Clone)]
pub struct UnifiedEngine {
    normalizer: Normalizer,
    aliases: AliasMap,
    /// CM-style reverse index (A02): normalized key -> canonical, first-wins.
    /// Canonicals iterate in sorted order, so the mapping is deterministic and
    /// matches CM's `_rebuild_reverse_alias_index` exactly (the decided A01
    /// `identity` behavior). A key two canonicals share resolves to the
    /// alphabetically-first one — the false-positive side the decisions doc
    /// already discounts (A01 is capped at weak for this circularity).
    reverse_index: HashMap<String, Option<String>>,
    known: Arc<KnownCallsigns>,
    /// Digest of the settings identities depend on (see [`Self::identity_version`]).
    config_fingerprint: String,
    threshold: u8,
    enabled_countries: Vec<String>,
    detector: Lineuparr,
}

impl UnifiedEngine {
    /// Build the engine from the operator's [`EngineConfig`].
    ///
    /// The operator's `ignore_*` toggles carry through untouched; every structural
    /// flag is forced to its decided variant (see the module table).
    pub fn new(config: &EngineConfig) -> Self {
        let flags = NormalizeFlags {
            core_pipeline: true,
            rejoin_designators: true,
            remove_cinemax: false,
            split_ampersand: true,
            dots_letters_only: true,
            suffix_floor: true,
            ..config.flags
        };
        let normalizer = Normalizer::new(flags);
        let known = Arc::new(merged_known_callsigns());

        let mut base: HashMap<String, Vec<String>> = HashMap::new();
        // File order: CM's reverse index keeps the first-seen mapping on
        // collision, so iteration order decides ties.
        let mut order: Vec<String> = Vec::new();
        for entry in merged_alias_entries() {
            // RAW variant list (no canonical append): the verbatim-alias half
            // reads `alias_map.get(query)` exactly as shipped.
            order.push(entry.canonical.clone());
            base.insert(entry.canonical, entry.variants);
        }
        // Decided disputed aliases (decisions.json `aliases`, weak): the alias
        // string joins its winning canonical's variant list (in place, so file
        // order still decides ties; a new canonical appends at the end).
        for (alias, canonical) in decided_alias_overrides() {
            match base.get_mut(&canonical) {
                Some(variants) => {
                    if !variants.iter().any(|v| v == &alias) {
                        variants.push(alias);
                    }
                }
                None => {
                    order.push(canonical.clone());
                    base.insert(canonical, vec![alias]);
                }
            }
        }
        // Sorted, so a new custom canonical's place in the first-wins order never
        // depends on HashMap iteration order (deterministic across runs).
        let mut custom: Vec<(&String, &Vec<String>)> = config.custom_aliases.iter().collect();
        custom.sort();
        for (key, value) in custom {
            if base.contains_key(key) {
                base.insert(key.clone(), value.clone());
            } else {
                order.push(key.clone());
                base.insert(key.clone(), value.clone());
            }
        }
        let aliases = AliasMap::from_json(&serde_json::to_string(&base).unwrap_or_default())
            .unwrap_or_else(AliasMap::builtin);

        let mut reverse_index: HashMap<String, Option<String>> = HashMap::new();
        // First-wins in sorted-canonical order, exactly like CM's
        // `_rebuild_reverse_alias_index`: later claimants of an already-mapped
        // key are ignored (e.g. `SBS Food` also claims `Food Network`).
        let mut insert_key = |key: String, canonical: &str| {
            reverse_index
                .entry(key)
                .or_insert_with(|| Some(canonical.to_string()));
        };
        let canonicals: Vec<String> = order;
        for canonical in &canonicals {
            let variants = &base[canonical];
            let mut ordered = vec![canonical.clone()];
            for variant in variants {
                if variant != canonical {
                    ordered.push(variant.clone());
                }
            }
            for variant in ordered {
                // Decided N15, like `normalize_key`: alias variants collapse pipe
                // junk before the reverse index keys them.
                let collapsed = crate::tags::collapse_pipe_segments(variant.as_str());
                let normalized = normalizer.normalize_name(&collapsed, &[]);
                if normalized.is_empty() {
                    continue;
                }
                let lowered = normalized.to_lowercase();
                insert_key(lowered.clone(), canonical);
                let stripped = nospace(&lowered);
                if stripped != lowered {
                    insert_key(stripped, canonical);
                }
            }
        }

        // Everything in the config an IDENTITY depends on: the normalisation flags
        // (the key) and the custom aliases (the canonical). Threshold and enabled
        // countries only affect scoring/ranking, which is never cached.
        let mut fingerprint = format!("{flags:?}");
        for (key, value) in {
            let mut custom: Vec<_> = config.custom_aliases.iter().collect();
            custom.sort();
            custom
        } {
            fingerprint.push('\0');
            fingerprint.push_str(&serde_json::json!([key, value]).to_string());
        }

        Self {
            normalizer,
            aliases,
            reverse_index,
            known,
            config_fingerprint: fnv_hex(&fingerprint),
            threshold: config.threshold,
            enabled_countries: config.enabled_countries.clone(),
            detector: Lineuparr,
        }
    }

    /// The decided engine with shipped defaults (threshold 95, every country).
    pub fn decided() -> Self {
        Self::new(&EngineConfig::default())
    }

    /// Cache key for an [`EngineConfig`]: everything an identity or a verdict
    /// depends on (threshold, enabled countries, normalisation flags, aliases).
    pub fn cache_key(config: &EngineConfig) -> String {
        let mut aliases: Vec<(&String, &Vec<String>)> = config.custom_aliases.iter().collect();
        aliases.sort();
        format!(
            "thr:{}|countries:{}|flags:{:?}|aliases:{}",
            config.threshold,
            config.enabled_countries.join(","),
            config.flags,
            serde_json::json!(aliases)
        )
    }

    /// The engine for `config`, shared across callers holding the same key.
    ///
    /// Building the reverse index costs ~0.25 s in debug (every alias variant is
    /// normalized once); parity harnesses run thousands of cases over a handful of
    /// distinct configurations, so per-case construction dominated their runtime.
    /// The engine is immutable after construction and its data is compiled in, so
    /// sharing is a pure speed-up; a changed `[matching]` (a hot reload) is a new key
    /// and gets a fresh engine. The cache holds the [`ENGINE_CACHE_CAPACITY`] most
    /// recently used configurations, so a long-lived daemon whose operator keeps
    /// editing aliases or the threshold does not accumulate one engine per edit.
    pub fn cached(config: &EngineConfig) -> Arc<Self> {
        type Entries = Vec<(String, Arc<UnifiedEngine>)>;
        static CACHE: OnceLock<Mutex<Entries>> = OnceLock::new();
        let cache = CACHE.get_or_init(|| Mutex::new(Vec::new()));
        // A panic elsewhere while the lock was held leaves a list that is still valid.
        let lock = || {
            cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        };
        let key = Self::cache_key(config);
        {
            let mut entries = lock();
            if let Some(at) = entries.iter().position(|(k, _)| *k == key) {
                // Most recently used last: eviction takes the front.
                let entry = entries.remove(at);
                let hit = Arc::clone(&entry.1);
                entries.push(entry);
                return hit;
            }
        }
        // Built outside the lock: other configurations are not held up behind it (two
        // racing misses on one key both build; the second insert simply wins).
        let engine = Arc::new(Self::new(config));
        let mut entries = lock();
        entries.retain(|(k, _)| *k != key);
        entries.push((key, Arc::clone(&engine)));
        if entries.len() > ENGINE_CACHE_CAPACITY {
            let excess = entries.len() - ENGINE_CACHE_CAPACITY;
            entries.drain(..excess);
        }
        engine
    }

    pub fn threshold(&self) -> u8 {
        self.threshold
    }

    /// The identity cache namespace for THIS engine: [`data_version`] (merged data,
    /// decisions, engine version) plus a digest of the operator settings identities
    /// depend on (normalisation toggles, custom aliases). A settings change therefore
    /// misses every cached identity instead of serving stale ones.
    pub fn identity_version(&self) -> String {
        format!("{}-cfg:{}", data_version(), self.config_fingerprint)
    }

    pub fn normalizer(&self) -> &Normalizer {
        &self.normalizer
    }

    pub fn alias_map(&self) -> &AliasMap {
        &self.aliases
    }

    pub fn known(&self) -> &KnownCallsigns {
        &self.known
    }

    /// The normalize key: the decided normalizer with no user tags.
    ///
    /// Decided N15: pipe-delimited junk segments collapse first (`Cinemax East |
    /// HD |` keys as `cinemax`), so the key carries no junk-segment residue.
    pub fn normalize_key(&self, name: &str) -> String {
        let collapsed = crate::tags::collapse_pipe_segments(name);
        self.normalizer.normalize_name(&collapsed, &[])
    }

    /// The extracted callsign (shared-core ladder over the merged allowlist).
    pub fn callsign_of(&self, name: &str) -> Option<String> {
        crate::callsign::extract_callsign_for(name, &self.known, CallsignLadder::SharedCoreNoRescue)
    }

    /// The extracted callsign plus its positional confidence (priorities 1-4 high).
    pub fn callsign_confidence(&self, name: &str) -> (Option<String>, bool) {
        self.callsign_confident(name)
    }

    fn callsign_confident(&self, name: &str) -> (Option<String>, bool) {
        crate::callsign::extract_callsign_with_confidence_for(
            name,
            &self.known,
            CallsignLadder::SharedCoreNoRescue,
        )
    }

    /// The query's callsign-first base (decided C07 `callsign-first`): the
    /// normalized callsign when the query's identity has a high-confidence
    /// callsign that exists in the merged station table, else `None` (the whole
    /// rule is off for that query — including callsign-led names whose callsign
    /// no station row knows, and every name without a callsign).
    fn callsign_first_query(&self, prep: &QueryPrep) -> Option<String> {
        if !prep.high {
            return None;
        }
        let base = prep.base.clone()?;
        if self.known.contains(&base) {
            Some(base)
        } else {
            None
        }
    }

    /// Resolve `name` through the reverse index (A01 `identity`): the canonical both
    /// the query and the candidate must share. Colliding keys yield `None`.
    pub fn alias_canonical(&self, name: &str) -> Option<String> {
        let normalized = self.normalize_key(name);
        if normalized.is_empty() {
            return None;
        }
        let lowered = normalized.to_lowercase();
        if let Some(hit) = self.reverse_index.get(&lowered) {
            return hit.clone();
        }
        self.reverse_index
            .get(&nospace(&lowered))
            .cloned()
            .flatten()
    }

    fn country_of(&self, name: &str) -> Option<&'static str> {
        use crate::country::CountryDetector;
        self.detector.country_from_name(name)
    }

    /// Whether an indexed candidate survives the decided country rule (G03 `lu-strict`).
    ///
    /// Strict only once a country is configured: with no enabled countries everything
    /// survives (coherence: LU's filter needs a lineup country; pair verdicts never
    /// read the vocabulary). Otherwise a candidate with an unknown country survives
    /// (fail-open); a known country must equal the query's, belong to the enabled set,
    /// or be a US<->MX shared feed. The shared-feed list is read on the QUERY's
    /// normalized key (`query_norm`), as Lineuparr folds `normalized_query`.
    pub fn country_keep_index(
        &self,
        query_country: Option<&str>,
        query_norm: &str,
        index: &EngineIndex,
        i: usize,
        count: &mut CountryFilterCount,
    ) -> bool {
        if self.enabled_countries.is_empty() {
            return true;
        }
        let stream_country = index.countries[i];
        let relationship = match (query_country, stream_country) {
            (_, None) => Relationship::Unknown,
            (None, Some(stream)) if self.enabled_countries.iter().any(|c| c == stream) => {
                Relationship::Same
            }
            (Some(channel), Some(stream)) if channel == stream => Relationship::Same,
            (None, Some(_)) | (Some(_), Some(_)) => {
                let anchor =
                    query_country.or_else(|| self.enabled_countries.first().map(String::as_str));
                match (anchor, stream_country) {
                    (Some(channel), Some(stream)) => {
                        if lineuparr_cross_border_ok(channel, stream, query_norm) {
                            Relationship::Same
                        } else {
                            Relationship::Foreign
                        }
                    }
                    _ => Relationship::Unknown,
                }
            }
        };
        count.record(relationship);
        relationship != Relationship::Foreign
    }

    fn score_pair(&self, query: &str, candidate: &str) -> Option<(u8, EngineMethod)> {
        if is_group_header(candidate) || is_group_header(query) {
            return None;
        }
        let norm_q = self.normalize_key(query);
        if norm_q.is_empty() {
            // Only the anchor can still decide (alias needs normalized keys).
            let (query_cs, query_high) = self.callsign_confident(query);
            let (cand_cs, cand_high) = self.callsign_confident(candidate);
            return match (&query_cs, &cand_cs) {
                (Some(q), Some(c))
                    if query_high
                        && cand_high
                        && normalize_callsign(q) == normalize_callsign(c) =>
                {
                    Some((95, EngineMethod::Callsign))
                }
                _ => None,
            };
        }
        let view = CandidateView::build(candidate, self);
        if view.norm_key.is_empty() {
            let (query_cs, query_high) = self.callsign_confident(query);
            return match (&query_cs, &view.callsign) {
                (Some(q), Some(c))
                    if query_high
                        && view.high
                        && normalize_callsign(q) == normalize_callsign(c) =>
                {
                    Some((95, EngineMethod::Callsign))
                }
                _ => None,
            };
        }
        let prep = QueryPrep::build(query, self)?;
        self.score_core(&prep, &view)
    }

    /// ONE scoring path for pairs and indexed ranks: winner-pick + post-filters.
    ///
    /// Best-hit accumulation mirrors `match_all_streams` precedence (highest score
    /// wins; ties keep the earliest stage: alias > fuzzy stages, anchor only
    /// replacing a LOWER score). The single winner is then post-filtered once —
    /// anchor: none; alias: zone; stages: digits (T06/T07) + zone — exactly as
    /// the decided Python engine post-filters `ranked[0]`. `score_pair` and
    /// `rank_index` both funnel through here, so there is one code path.
    fn score_core(&self, prep: &QueryPrep, view: &CandidateView) -> Option<(u8, EngineMethod)> {
        // Anchor picture (C06/D04 `lu-anchor`): agreement needs high confidence
        // on BOTH sides; disagreement hard-rejects (even an alias hit).
        let anchor_agrees = match (prep.base.as_deref(), view.base.as_deref()) {
            (Some(q), Some(c)) if prep.high && view.high => Some(q == normalize_callsign(c)),
            _ => None,
        };

        // Callsign-first (decided C07 `callsign-first`, orchestrator): the query
        // carries a high-confidence callsign the merged station table knows (a
        // network-led station name like `ABC (WABC)`). A candidate carrying a
        // DIFFERENT known callsign is a different station and never joins — the
        // anchor's high/high hard-reject, extended to any known candidate
        // callsign (the anchor only sees high-confidence candidates; a
        // low-confidence `KOMO` in `KOMO News` is still that station). A
        // candidate with NO callsign (the bare national `ABC`) is NOT rejected
        // here: it still matches at pair level, so a pool holding no station
        // keeps today's national family (the ranker prefers a same-callsign
        // candidate whenever one survives the cut — see `ranked_survivors`).
        if let Some(query_base) = self.callsign_first_query(prep) {
            if let Some(candidate_base) = view.base.as_deref() {
                if candidate_base != query_base && self.known.contains(candidate_base) {
                    return None;
                }
            }
        }

        // Best-hit accumulation: highest score wins; ties keep the earliest
        // stage (alias > fuzzy stages; the anchor only replaces a LOWER score).
        #[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
        enum Stage {
            Anchor,
            Fuzzy,
            Alias,
        }
        let mut best: Option<(u8, EngineMethod, Stage)> = None;
        let mut consider = |score: u8, method: EngineMethod, stage: Stage| {
            let replace = match &best {
                None => true,
                Some((best_score, _, best_stage)) => {
                    score > *best_score || (score == *best_score && stage > *best_stage)
                }
            };
            if replace {
                best = Some((score, method, stage));
            }
        };

        // Step 0a: LU-verbatim alias — the query's OWN variants (verbatim key,
        // A01 keeps LU's lookup beside the identity resolve). Exact (spaced or
        // nospace) is 100; otherwise the best spaced-variant similarity with
        // the scaled threshold and the majority guard on processed forms.
        // Candidates under the 2-char floor skip this half (LU reads them
        // through `_get_cached_norm`); the identity half below only needs a
        // non-empty norm. A04 rescue is off (`none`).
        //
        // Variant norms are precomputed per query (QueryPrep): LU reads
        // `alias_map.get(query)` verbatim, and the canonical is not a variant
        // of itself there (`Sky Sports F1` must not fuzzy-hit `Sky Sports 1`
        // off its own name at 92).
        if view.scored {
            if !prep.variant_spaced.is_empty() {
                if prep.variant_lookup.contains(&view.norm_lower)
                    || prep.variant_lookup.contains(&view.nospace)
                {
                    consider(100, EngineMethod::Alias, Stage::Alias);
                } else {
                    let threshold_ratio = f64::from(self.threshold) / 100.0;
                    let candidate_chars: Vec<char> = view.norm_lower.chars().collect();
                    let mut best_ratio = 0.0;
                    let mut best_len = 0;
                    let mut best_processed = String::new();
                    for (alias, processed) in prep
                        .variant_spaced
                        .iter()
                        .zip(prep.variant_processed.iter())
                    {
                        let alias_chars: Vec<char> = alias.chars().collect();
                        let ratio = calculate_similarity_chars(
                            &alias_chars,
                            &candidate_chars,
                            threshold_ratio,
                        );
                        if ratio > best_ratio {
                            best_ratio = ratio;
                            best_len = alias_chars.len().min(candidate_chars.len());
                            best_processed = processed.clone();
                        }
                    }
                    let score = (best_ratio * 100.0) as u8;
                    if score >= length_scaled_threshold(self.threshold, best_len) {
                        let guarded = score < 100
                            && !has_token_overlap_core(&best_processed, &view.processed, 4, true);
                        if !guarded {
                            consider(score, EngineMethod::Alias, Stage::Alias);
                        }
                    }
                }
            }
            // Step 0b: identity alias (A01 `identity`). The query through the
            // reverse index; the candidate's precomputed canonical must match.
            if let Some(query_id) = prep.canonical.as_deref() {
                if view.alias_canonical.as_deref() == Some(query_id) {
                    consider(100, EngineMethod::Alias, Stage::Alias);
                }
            }
        }
        if !view.scored && !view.norm_key.is_empty() {
            // Identity half without the floor (LU's `_get_cached_norm` floor
            // applies to the verbatim half only): `US: W` vs `W` resolve.
            if let Some(query_id) = prep.canonical.as_deref() {
                if view.alias_canonical.as_deref() == Some(query_id) {
                    consider(100, EngineMethod::Alias, Stage::Alias);
                }
            }
        }
        // Anchor: raises a lower score to 95, keeps 100s, hard-rejects on
        // disagreement (even an alias hit).
        match anchor_agrees {
            Some(true) => consider(95, EngineMethod::Callsign, Stage::Anchor),
            Some(false) => {
                return None;
            }
            None => {}
        }

        // Stages 1-3 over the normalized pair (T03 `cm`, T09 `lu`).
        if view.scored {
            let query_nospace = nospace(&prep.lower);
            if let Some(hit) =
                self.fuzzy_stages(&prep.lower, &view.norm_lower, &query_nospace, &view.nospace)
            {
                consider(hit.0, hit.1, Stage::Fuzzy);
            }
        }

        let (score, method, stage) = best?;
        // Post-filters on the single winner (precomputed zone/shift/digits:
        // no regexes per pair). Alias skips digits; the anchor skips all.
        let zone_keep = prep.west == view.west;
        match stage {
            Stage::Anchor => Some((score, method)),
            Stage::Alias => {
                if zone_keep {
                    Some((score, method))
                } else {
                    None
                }
            }
            Stage::Fuzzy => {
                if let (Some(q_num), Some(c_num)) = (prep.trailing, view.trailing) {
                    if q_num != c_num {
                        return None;
                    }
                }
                if prep.shift != view.shift {
                    return None;
                }
                if !prep.designators.is_empty()
                    && !view.designators.is_empty()
                    && prep.designators.is_disjoint(&view.designators)
                {
                    return None;
                }
                if !zone_keep {
                    return None;
                }
                Some((score, method))
            }
        }
    }
    /// Stages 1-3 over one normalized pair (T03 `cm` in T09 `lu` shape).
    ///
    /// Exact (spaced/nospace, then the guarded 0.97 tier T04), substring (length
    /// floor, scaled threshold, always-on majority guard T02/T05/T01) and
    /// token-sort fuzzy with the same guards. Guards (T06/T07/ZONE) are NOT
    /// applied here — the caller post-filters the winner once, as Engine.same
    /// post-filters ranked[0].
    fn fuzzy_stages(
        &self,
        query_lower: &str,
        candidate_lower: &str,
        query_nospace: &str,
        candidate_nospace: &str,
    ) -> Option<(u8, EngineMethod)> {
        let query_chars: Vec<char> = query_lower.chars().collect();
        let candidate_chars: Vec<char> = candidate_lower.chars().collect();
        let threshold_ratio = f64::from(self.threshold) / 100.0;

        // Stage 1: exact after normalisation, then the guarded 0.97 tier (T04).
        if query_nospace == candidate_nospace {
            return Some((100, EngineMethod::Exact));
        }
        let exact_ratio = calculate_similarity_chars(&query_chars, &candidate_chars, 0.97);
        if exact_ratio >= 0.97 {
            let score = (exact_ratio * 100.0) as u8;
            let shorter = query_chars.len().min(candidate_chars.len());
            if score >= length_scaled_threshold(self.threshold, shorter)
                && has_token_overlap_core(query_lower, candidate_lower, 4, true)
            {
                return Some((score, EngineMethod::Exact));
            }
        }

        // Stage 2: substring with the length-ratio floor, scaled threshold and the
        // always-on majority guard (T02).
        if query_lower.contains(candidate_lower) || candidate_lower.contains(query_lower) {
            let shorter = query_chars.len().min(candidate_chars.len());
            let longer = query_chars.len().max(candidate_chars.len());
            if longer > 0 && shorter as f64 / longer as f64 >= 0.75 {
                let ratio =
                    calculate_similarity_chars(&query_chars, &candidate_chars, threshold_ratio);
                let score = (ratio * 100.0) as u8;
                if score >= length_scaled_threshold(self.threshold, shorter)
                    && has_token_overlap_core(query_lower, candidate_lower, 4, true)
                {
                    return Some((score, EngineMethod::Substring));
                }
            }
        }

        // Stage 3: token-sort fuzzy with the same scaled + majority guards.
        // `process_string_for_matching` lowercases internally, so the lowered
        // pair scores exactly as the cased one.
        let processed_q = process_string_for_matching(query_lower);
        let processed_c = process_string_for_matching(candidate_lower);
        let pq: Vec<char> = processed_q.chars().collect();
        let pc: Vec<char> = processed_c.chars().collect();
        let ratio = calculate_similarity_chars(&pq, &pc, threshold_ratio);
        let score = (ratio * 100.0) as u8;
        let shorter = pq.len().min(pc.len());
        if score >= length_scaled_threshold(self.threshold, shorter)
            && has_token_overlap_core(&processed_q, &processed_c, 4, true)
        {
            return Some((score, EngineMethod::Fuzzy(score)));
        }
        None
    }

    /// The decided `same` verdict: `true` when the pair scores at or above the cut.
    pub fn same(&self, query: &str, candidate: &str) -> bool {
        self.score_pair(query, candidate)
            .is_some_and(|(score, _)| score >= self.threshold)
    }

    /// Score a pair, returning score + method when it reaches the cut.
    pub fn score(&self, query: &str, candidate: &str) -> Option<(u8, EngineMethod)> {
        let (score, method) = self.score_pair(query, candidate)?;
        if score >= self.threshold {
            Some((score, method))
        } else {
            None
        }
    }

    /// Rank candidates best-first (score desc, then original-token overlap, stable).
    ///
    /// Applies the group-header skip (Z05), the country rule (G03) and the cut.
    /// Built on [`EngineIndex`]: the index is built once for the candidate set
    /// and every query reuses it, so ranking 20k queries against 50k candidates
    /// scores only token-blocked shortlists instead of re-normalizing the world.
    pub fn rank(&self, query: &str, candidates: &[String]) -> Vec<EngineRanked> {
        self.rank_in(query, None, candidates)
    }

    /// [`rank`](Self::rank) with the query's country hint (lineup country, M3U
    /// group country), which wins over the query name's own marker.
    pub fn rank_in(
        &self,
        query: &str,
        country_hint: Option<&str>,
        candidates: &[String],
    ) -> Vec<EngineRanked> {
        let index = EngineIndex::build(
            candidates,
            self,
            &crate::identity::ResolveContext::default(),
        );
        self.rank_index_in(query, country_hint, &index)
    }

    /// Rank against a prebuilt [`EngineIndex`]: the hot path for runs.
    ///
    /// `Send + Sync`-safe over `&self`/`&index`: every query is independent, so a
    /// run scores queries across rayon threads with no interior mutability.
    pub fn rank_index(&self, query: &str, index: &EngineIndex) -> Vec<EngineRanked> {
        self.rank_index_in(query, None, index)
    }

    /// The ONE ranker: score, country rule (G03), cut, then the family cut.
    ///
    /// The family is every candidate whose [`FamilyKey`] equals the top's and whose
    /// country is compatible with the family's ONE country (the query's — hint, else
    /// its name marker — else the top's, else the first known among the top's
    /// key-mates in rank order; see [`crate::identity`]). Callsign families ignore
    /// country (bug-161).
    pub fn rank_index_in(
        &self,
        query: &str,
        country_hint: Option<&str>,
        index: &EngineIndex,
    ) -> Vec<EngineRanked> {
        let ranked = self.ranked_survivors(query, country_hint, index);
        // The identity-side country (hint, else the name's stream marker): the same
        // notion `Identity::country` carries, so the family cut and `same_family`
        // agree.
        let query_identity_country = country_hint.or_else(|| lineuparr_stream_country(query));
        self.family_cut(ranked, query_identity_country, index)
    }

    /// The same ranker WITHOUT the family cut: every candidate at or above this
    /// engine's cut whose country is compatible with the query's, best first.
    ///
    /// The head is the family's top ([`rank_index_in`](Self::rank_index_in)`[0]`), so
    /// this never disagrees with the family on who leads; it only keeps the runners-up
    /// of OTHER identities. Apps use it, on an engine built with
    /// [`with_threshold`](Self::with_threshold) at the AI rescue floor, to shortlist the
    /// candidates a borderline channel offers the `[ai.verify]` chain: one ranker,
    /// one code path, only the cut is a parameter.
    pub fn shortlist_index_in(
        &self,
        query: &str,
        country_hint: Option<&str>,
        index: &EngineIndex,
    ) -> Vec<EngineRanked> {
        let mut ranked = self.ranked_survivors(query, country_hint, index);
        let query_identity_country = country_hint.or_else(|| lineuparr_stream_country(query));
        ranked.retain(|(r, _, _)| family_compatible(query_identity_country, r.index, index));
        ranked.into_iter().map(|(r, _, _)| r).collect()
    }

    /// The engine with a different decision cut and everything else shared: same
    /// normalizer, aliases, allowlist and identity version (the cut never affects an
    /// identity), so an [`EngineIndex`] built by either serves both.
    pub fn with_threshold(&self, threshold: u8) -> Self {
        Self {
            threshold,
            ..self.clone()
        }
    }

    /// Score, country rule (G03) and cut, ranked best first with the original-token
    /// overlap tie-break: the ranker's list before the family cut.
    ///
    /// Each entry also carries its callsign-first flag (decided C07): a surviving
    /// same-callsign candidate sorts before every higher-scoring non-callsign one.
    fn ranked_survivors(
        &self,
        query: &str,
        country_hint: Option<&str>,
        index: &EngineIndex,
    ) -> Vec<(EngineRanked, usize, bool)> {
        if is_group_header(query) {
            return Vec::new();
        }
        let norm_q = self.normalize_key(query);
        // Query callsign once; candidate callsigns live in the index.
        let (query_cs, query_high) = self.callsign_confident(query);
        let query_base = query_cs.as_deref().map(normalize_callsign);
        // Anchor-only path for names that normalize away.
        if norm_q.is_empty() {
            return self.anchor_ranked(
                query,
                country_hint,
                query_base.as_deref(),
                query_high,
                index,
            );
        }
        let Some(prep) = QueryPrep::build(query, self) else {
            return Vec::new();
        };
        // Decided C07 `callsign-first`: a same-callsign candidate outranks any
        // non-callsign match, so the stripped-key Exact 100 on the bare national
        // entry no longer beats the station's Callsign 95. Candidates with a
        // different KNOWN callsign never reach the sort (`score_core` rejects
        // them); callsign-less candidates stay, ordered after every surviving
        // same-callsign candidate — the fallback when the pool holds no station.
        let first_base = self.callsign_first_query(&prep);
        let query_nospace = nospace(&prep.lower);
        let query_country = country_hint.or_else(|| self.country_of(query));
        let mut count = CountryFilterCount::default();
        let query_tokens = ascii_tokens(&query.to_lowercase());
        let qn = prep.lower.as_str();

        // O(1) probes: exact maps, verbatim-alias variants, identity canonical,
        // callsign map. Each yields candidate indexes; scored once below.
        let mut probe: Vec<usize> = Vec::new();
        if let Some(indexes) = index.spaced_map.get(&prep.lower) {
            probe.extend_from_slice(indexes);
        }
        if let Some(indexes) = index.nospace_map.get(&query_nospace) {
            probe.extend_from_slice(indexes);
        }
        // Verbatim-alias exact: the query's own variant norms hit the maps.
        for variant in &prep.variant_spaced {
            if let Some(indexes) = index.spaced_map.get(variant) {
                probe.extend_from_slice(indexes);
            }
            let stripped = nospace(variant);
            if stripped != *variant {
                if let Some(indexes) = index.nospace_map.get(&stripped) {
                    probe.extend_from_slice(indexes);
                }
            }
        }
        // Identity alias: every candidate resolving to the query's canonical.
        if let Some(query_id) = prep.canonical.as_deref() {
            if let Some(indexes) = index.alias_canon_map.get(query_id) {
                probe.extend_from_slice(indexes);
            }
        }
        // Callsign anchor candidates (scored as 95 on agreement).
        if let Some(base) = prep.base.as_deref() {
            if prep.high {
                if let Some(indexes) = index.callsign_map.get(base) {
                    probe.extend_from_slice(indexes);
                }
            }
        }
        // Fuzzy shortlist: the SOUND token block (see EngineIndex docs).
        probe.extend(index.fuzzy_block(&prep.lower));
        probe.sort_unstable();
        probe.dedup();
        // The country rule (G03) over EVERY shortlisted candidate — probe hits and
        // fuzzy-block hits alike, as Lineuparr filters all of `all_matches` — then
        // score each survivor once, at the cut.
        // The query's own identity key, for the tie-break below.
        let query_key = norm_q.to_lowercase();
        let mut scored: Vec<(EngineRanked, usize, bool)> = Vec::new();
        for i in probe {
            if !self.country_keep_index(query_country, qn, index, i, &mut count) {
                continue;
            }
            if let Some((score, method)) = self.score_view(&prep, i, index) {
                if score < self.threshold {
                    continue;
                }
                let overlap = token_overlap_count(&query_tokens, &index.names[i]);
                let first = first_base
                    .as_deref()
                    .is_some_and(|qb| index.views[i].base.as_deref() == Some(qb));
                scored.push((
                    EngineRanked {
                        index: i,
                        score,
                        method,
                    },
                    tie_rank(index.identity_keys[i].key == query_key, overlap),
                    first,
                ));
            }
        }
        scored.sort_by(|a, b| {
            b.2.cmp(&a.2)
                .then_with(|| b.0.score.cmp(&a.0.score))
                .then_with(|| b.1.cmp(&a.1))
        });
        scored
    }

    /// The family cut over ranked, country-ruled, cut survivors (best first).
    ///
    /// Candidates whose country is incompatible with the query's never win; the top
    /// fixes the [`FamilyKey`]; the family's ONE country is the query's, else the
    /// top's, else the first known one among the top's key-mates in rank order; the
    /// family is every ranked candidate with the top's key and a compatible country.
    /// Every candidate sharing the top's key is already in `ranked` (same key exacts
    /// at 100 through the hash maps, on the same G03 verdict), so nothing is
    /// re-scored here.
    fn family_cut(
        &self,
        mut ranked: Vec<(EngineRanked, usize, bool)>,
        query_country: Option<&str>,
        index: &EngineIndex,
    ) -> Vec<EngineRanked> {
        let compatible = |country: Option<&str>, i: usize| family_compatible(country, i, index);
        ranked.retain(|(r, _, _)| compatible(query_country, r.index));
        let Some((top, _, _)) = ranked.first() else {
            return Vec::new();
        };
        let top_key = &index.identity_keys[top.index];
        let family_country = query_country.or_else(|| {
            ranked
                .iter()
                .filter(|(r, _, _)| &index.identity_keys[r.index] == top_key)
                .find_map(|(r, _, _)| index.identity_country(r.index))
        });
        ranked
            .into_iter()
            .filter(|(r, _, _)| {
                &index.identity_keys[r.index] == top_key && compatible(family_country, r.index)
            })
            .map(|(r, _, _)| r)
            .collect()
    }

    /// Anchor-only ranked list for queries that normalize away (`(KING)`).
    fn anchor_ranked(
        &self,
        query: &str,
        country_hint: Option<&str>,
        query_base: Option<&str>,
        query_high: bool,
        index: &EngineIndex,
    ) -> Vec<(EngineRanked, usize, bool)> {
        let Some(base) = query_base else {
            return Vec::new();
        };
        if !query_high {
            return Vec::new();
        }
        let Some(indexes) = index.callsign_map.get(base) else {
            return Vec::new();
        };
        let query_country = country_hint.or_else(|| self.country_of(query));
        let mut count = CountryFilterCount::default();
        let query_tokens = ascii_tokens(&query.to_lowercase());
        // Anchor-only queries carry one callsign by construction (the map lookup),
        // so callsign-first ordering is vacuous here (`true` throughout).
        let mut ranked: Vec<(EngineRanked, usize, bool)> = Vec::new();
        for &i in indexes {
            if !index.callsign_high[i] || 95 < self.threshold {
                continue;
            }
            if !self.country_keep_index(query_country, "", index, i, &mut count) {
                continue;
            }
            let overlap = token_overlap_count(&query_tokens, &index.names[i]);
            ranked.push((
                EngineRanked {
                    index: i,
                    score: 95,
                    method: EngineMethod::Callsign,
                },
                overlap,
                true,
            ));
        }
        ranked.sort_by(|a, b| b.0.score.cmp(&a.0.score).then_with(|| b.1.cmp(&a.1)));
        ranked
    }

    /// Score one indexed candidate through the ONE scoring path.
    ///
    /// Thin adapter over [`score_core`](Self::score_core): the query side is
    /// computed once per rank call, the candidate side reads the index.
    fn score_view(
        &self,
        prep: &QueryPrep,
        i: usize,
        index: &EngineIndex,
    ) -> Option<(u8, EngineMethod)> {
        self.score_core(prep, &index.views[i])
    }
}

/// Whether indexed candidate `i` may share a family whose ONE country is `country`
/// (callsign identities ignore country, bug-161; see [`countries_compatible`]).
fn family_compatible(country: Option<&str>, i: usize, index: &EngineIndex) -> bool {
    let key = &index.identity_keys[i];
    countries_compatible(
        country,
        index.identity_country(i),
        key.callsign.is_some(),
        &key.key,
    )
}

/// One candidate's precomputed scoring state (built once per candidate set).
#[derive(Debug, Clone)]
struct CandidateView {
    /// Full normalized key (empty when the name normalizes away).
    norm_key: String,
    /// Lowercased normalized key.
    norm_lower: String,
    /// Whether the fuzzy/alias-verbatim stages may score it (2+ chars).
    scored: bool,
    /// No-space comparison form.
    nospace: String,
    /// Token-sorted comparison form.
    processed: String,
    /// Extracted callsign (full) + positional confidence + normalized base.
    callsign: Option<String>,
    high: bool,
    base: Option<String>,
    /// Reverse-index canonical (identity half; no length floor, only empty).
    alias_canonical: Option<String>,
    /// `+N` time-shift marker on the ORIGINAL name.
    shift: bool,
    /// Designator tokens of the normalized key (T07).
    designators: HashSet<String>,
    /// Trailing number of the normalized key (T06).
    trailing: Option<u32>,
    /// Routed feed zone (identity) and whether it is WEST (zone equality without
    /// regexes per pair).
    zone: FeedZone,
    west: bool,
}

impl CandidateView {
    fn build(name: &str, engine: &UnifiedEngine) -> Self {
        let norm_key = engine.normalize_key(name);
        let norm_lower = norm_key.to_lowercase();
        let scored = !norm_key.is_empty() && norm_key.chars().count() >= 2;
        let (callsign, high) = engine.callsign_confident(name);
        let base = callsign.as_deref().map(normalize_callsign);
        let zone = extract_zone_noguard(name);
        Self {
            nospace: nospace(&norm_lower),
            processed: process_string_for_matching(&norm_lower),
            alias_canonical: engine.alias_canonical(name),
            designators: norm_lower
                .split_whitespace()
                .filter(|t| crate::tokens::is_designator_token(t))
                .map(str::to_string)
                .collect(),
            trailing: trailing_number(&norm_lower),
            shift: is_time_shift(name),
            zone,
            west: zone == FeedZone::West,
            norm_key,
            norm_lower,
            scored,
            callsign,
            high,
            base,
        }
    }
}

/// Per-query scoring state, computed once and shared across every candidate.
///
/// The hot loop (`rank_index` over an index) must not re-normalize the query's
/// variants, re-extract its callsign or re-run its zone regexes per candidate:
/// everything query-side lives here.
struct QueryPrep {
    lower: String,
    trailing: Option<u32>,
    designators: HashSet<String>,
    shift: bool,
    west: bool,
    base: Option<String>,
    high: bool,
    canonical: Option<String>,
    variant_lookup: HashSet<String>,
    variant_spaced: Vec<String>,
    variant_processed: Vec<String>,
}

impl QueryPrep {
    fn build(query: &str, engine: &UnifiedEngine) -> Option<Self> {
        let norm = engine.normalize_key(query);
        if norm.is_empty() {
            return None;
        }
        let lower = norm.to_lowercase();
        let (callsign, high) = engine.callsign_confident(query);
        let mut variant_lookup = HashSet::new();
        let mut variant_spaced = Vec::new();
        let mut variant_processed = Vec::new();
        for variant in engine.aliases.aliases_for_exact(query) {
            let normalized = engine.normalize_key(variant);
            if normalized.is_empty() {
                continue;
            }
            let lowered = normalized.to_lowercase();
            variant_lookup.insert(lowered.clone());
            variant_processed.push(process_string_for_matching(&lowered));
            variant_spaced.push(lowered.clone());
            let stripped = nospace(&lowered);
            if stripped != lowered {
                variant_lookup.insert(stripped);
            }
        }
        Some(Self {
            designators: lower
                .split_whitespace()
                .filter(|t| crate::tokens::is_designator_token(t))
                .map(str::to_string)
                .collect(),
            trailing: trailing_number(&lower),
            shift: is_time_shift(query),
            west: extract_zone_noguard(query) == FeedZone::West,
            base: callsign.as_deref().map(normalize_callsign),
            high,
            canonical: engine.alias_canonical(query),
            variant_lookup,
            variant_spaced,
            variant_processed,
            lower,
        })
    }
}

/// The candidate side of a run, normalized once and shared read-only.
///
/// `EngineIndex::build` normalizes a whole candidate list once; every query then
/// reuses it through [`UnifiedEngine::rank_index`]. The struct is `Send + Sync`
/// with no interior mutability, so rayon threads share one index across queries.
///
/// # The SOUND token block
///
/// The fuzzy shortlist ([`fuzzy_block`](Self::fuzzy_block)) never drops a hit:
/// every scored stage carries the always-on majority guard (T01+T02), so a hit
/// shares a meaningful token with the query (raw space for the 0.97/substring
/// stages, processed space for token-sort and alias-fuzzy) unless the guard passes
/// vacuously — which needs an empty meaningful-token set on one side. The block
/// is therefore: candidates sharing a raw- OR processed-meaningful token, plus
/// every candidate with an empty set in either space, plus (when the QUERY's set
/// is empty in either space) the whole list. Exact/nospace hits, verbatim-alias
/// exact hits, identity hits and the anchor never touch the block: they resolve
/// through hash maps in `rank_index`.
///
/// # One index
///
/// This is THE candidate index of the unified engine. `pipeline::CandidateIndex` is
/// the per-profile index behind `MatchProfile` (guide/streams until steps 8-9); it
/// shares no state with this one and is retired together with the profiles, so
/// there is never a second index to keep in sync with the decided rules.
#[derive(Debug)]
pub struct EngineIndex {
    /// Candidate names in order (`EngineRanked.index` positions).
    pub names: Vec<String>,
    views: Vec<CandidateView>,
    spaced_map: HashMap<String, Vec<usize>>,
    nospace_map: HashMap<String, Vec<usize>>,
    raw_token_block: HashMap<String, Vec<usize>>,
    proc_token_block: HashMap<String, Vec<usize>>,
    raw_empty: Vec<usize>,
    proc_empty: Vec<usize>,
    callsign_map: HashMap<String, Vec<usize>>,
    callsign_high: Vec<bool>,
    alias_canon_map: HashMap<String, Vec<usize>>,
    /// Each candidate's [`FamilyKey`] — the same fields `resolve_with` computes,
    /// read off the view (one normalisation per candidate, not two).
    identity_keys: Vec<FamilyKey>,
    /// Detected stream country per candidate (G03 and identity country).
    countries: Vec<Option<&'static str>>,
    /// The build context's country hint: when set it is every candidate's
    /// identity country (as `resolve_with` would give it).
    country_hint: Option<String>,
}

impl EngineIndex {
    /// Normalize a whole candidate list once.
    ///
    /// Identities resolve with `ctx` (country hints/categories the run carries);
    /// one index per candidate set per run.
    pub fn build(
        names: &[String],
        engine: &UnifiedEngine,
        ctx: &crate::identity::ResolveContext,
    ) -> Self {
        let mut index = Self {
            names: names.to_vec(),
            views: Vec::with_capacity(names.len()),
            spaced_map: HashMap::new(),
            nospace_map: HashMap::new(),
            raw_token_block: HashMap::new(),
            proc_token_block: HashMap::new(),
            raw_empty: Vec::new(),
            proc_empty: Vec::new(),
            callsign_map: HashMap::new(),
            callsign_high: Vec::with_capacity(names.len()),
            alias_canon_map: HashMap::new(),
            identity_keys: Vec::with_capacity(names.len()),
            countries: Vec::with_capacity(names.len()),
            country_hint: ctx.country_hint.clone(),
        };
        for (i, name) in names.iter().enumerate() {
            if is_group_header(name) {
                // Headers never score; keep an unscored view so family cuts
                // still see a (never-winning) identity.
                index.views.push(CandidateView {
                    norm_key: String::new(),
                    norm_lower: String::new(),
                    scored: false,
                    nospace: String::new(),
                    processed: String::new(),
                    callsign: None,
                    high: false,
                    base: None,
                    alias_canonical: None,
                    shift: false,
                    designators: HashSet::new(),
                    trailing: None,
                    zone: FeedZone::Default,
                    west: false,
                });
            } else {
                index.views.push(CandidateView::build(name, engine));
            }
            let view = &index.views[i];
            if view.scored {
                index
                    .spaced_map
                    .entry(view.norm_lower.clone())
                    .or_default()
                    .push(i);
                index
                    .nospace_map
                    .entry(view.nospace.clone())
                    .or_default()
                    .push(i);
                let raw = crate::tokens::meaningful_tokens_owned(&view.norm_lower);
                if raw.is_empty() {
                    index.raw_empty.push(i);
                } else {
                    for token in raw {
                        index.raw_token_block.entry(token).or_default().push(i);
                    }
                }
                let proc = crate::tokens::meaningful_tokens_owned(&view.processed);
                if proc.is_empty() {
                    index.proc_empty.push(i);
                } else {
                    for token in proc {
                        index.proc_token_block.entry(token).or_default().push(i);
                    }
                }
            }
            if let Some(base) = view.base.as_deref() {
                index
                    .callsign_map
                    .entry(base.to_string())
                    .or_default()
                    .push(i);
            }
            index.callsign_high.push(view.high);
            if let Some(canonical) = view.alias_canonical.as_deref() {
                if !view.norm_key.is_empty() {
                    index
                        .alias_canon_map
                        .entry(canonical.to_string())
                        .or_default()
                        .push(i);
                }
            }
            // Headers carry an empty key: they never score, so never join a family.
            index.identity_keys.push(FamilyKey {
                key: view.norm_lower.clone(),
                zone: view.zone,
                timeshift: view.shift,
                callsign: view.base.clone(),
            });
            index.countries.push(lineuparr_stream_country(name));
        }
        index
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Candidate `i`'s [`FamilyKey`] (equal to `resolve_with(..).family_key()`).
    pub fn family_key(&self, i: usize) -> &FamilyKey {
        &self.identity_keys[i]
    }

    /// Candidate `i`'s identity country: the build hint, else its stream marker.
    pub fn identity_country(&self, i: usize) -> Option<&str> {
        self.country_hint.as_deref().or(self.countries[i])
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// The SOUND fuzzy shortlist for a lowercased query key (see struct docs).
    pub fn fuzzy_block(&self, query_lower: &str) -> Vec<usize> {
        let raw = crate::tokens::meaningful_tokens_owned(query_lower);
        let processed = process_string_for_matching(query_lower);
        let proc = crate::tokens::meaningful_tokens_owned(&processed);
        if raw.is_empty() || proc.is_empty() {
            return (0..self.names.len()).collect();
        }
        let mut seen = vec![false; self.names.len()];
        let mut out = Vec::new();
        let mut push = |i: usize| {
            if !seen[i] {
                seen[i] = true;
                out.push(i);
            }
        };
        for token in raw.iter().chain(proc.iter()) {
            if let Some(indexes) = self.raw_token_block.get(token) {
                for &i in indexes {
                    push(i);
                }
            }
            if let Some(indexes) = self.proc_token_block.get(token) {
                for &i in indexes {
                    push(i);
                }
            }
        }
        for &i in self.raw_empty.iter().chain(self.proc_empty.iter()) {
            push(i);
        }
        out
    }
}

/// Decided disputed aliases (`decisions.json` `aliases`, weak): alias -> canonical.
fn decided_alias_overrides() -> Vec<(String, String)> {
    static OVERRIDES: OnceLock<Vec<(String, String)>> = OnceLock::new();
    OVERRIDES
        .get_or_init(|| {
            let raw: &str = include_str!("../../../data/corpus/decisions.json");
            let value: serde_json::Value = serde_json::from_str(raw).unwrap_or_default();
            let mut out = Vec::new();
            if let Some(aliases) = value.get("aliases").and_then(|v| v.as_object()) {
                for (alias, decision) in aliases {
                    let status = decision
                        .get("status")
                        .and_then(|s| s.as_str())
                        .unwrap_or("");
                    let canonical = decision
                        .get("canonical")
                        .and_then(|c| c.as_str())
                        .unwrap_or("");
                    if status == "decided" && !canonical.is_empty() {
                        out.push((alias.clone(), canonical.to_string()));
                    }
                }
            }
            out.sort();
            out
        })
        .clone()
}

/// Runs of ASCII letters and digits (Python `re.findall(r'[a-z0-9]+', ...)`).
fn ascii_tokens(name_lower: &str) -> HashSet<String> {
    let mut tokens = HashSet::new();
    let mut current = String::new();
    for c in name_lower.chars() {
        if c.is_ascii_alphanumeric() {
            current.push(c);
        } else if !current.is_empty() {
            tokens.insert(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.insert(current);
    }
    tokens
}

/// The tie-break among equal scores (step 9 review): a candidate whose identity key IS
/// the query's comes first, then T12's original-token overlap.
///
/// Without the first half the overlap alone picks the top, and the top fixes the
/// family: `US: beIN Sports Spanish in English` scores 100 against its own
/// `beIN Sports Spanish in English` (exact) AND against
/// `US: beIN Sports English in Spanish` (token-sort fuzzy, the same words reordered),
/// and the shared `US` token made the OTHER channel the top — so streams attached the
/// wrong channel's whole family and guide assigned its entry. An identity-equal
/// candidate never loses a tie to a different identity. Pair verdicts (`same`/`score`,
/// the corpus gate) are untouched: only the order among equal scores changes.
fn tie_rank(identity_equal: bool, overlap: usize) -> usize {
    const IDENTITY: usize = 1 << 20;
    usize::from(identity_equal) * IDENTITY + overlap.min(IDENTITY - 1)
}

fn token_overlap_count(query_tokens: &HashSet<String>, candidate: &str) -> usize {
    let candidate_lower = candidate.to_lowercase();
    ascii_tokens(&candidate_lower)
        .iter()
        .filter(|token| query_tokens.contains(*token))
        .count()
}

/// Re-exported for the corpus gate: the zone a name routes on.
pub fn engine_zone(name: &str) -> FeedZone {
    crate::country::extract_zone_noguard(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> UnifiedEngine {
        UnifiedEngine::decided()
    }

    #[test]
    fn decided_normalizer_combines_core_with_the_three_janitor_statements() {
        let engine = engine();
        // Core body: HDR/HEVC strip, Cf strip, unconditional decorator strip.
        assert_eq!(engine.normalize_key("ESPN 4K HDR"), "ESPN");
        assert_eq!(engine.normalize_key("★ CNN ★"), "CNN");
        // N05: & -> and.
        assert_eq!(engine.normalize_key("U&Yesterday"), "U and Yesterday");
        // N06: letters-only dots.
        assert_eq!(engine.normalize_key("JusticeCentral.TV"), "Justice Central");
        // N07: suffix floor keeps `Justice Network` whole.
        assert_eq!(engine.normalize_key("Justice Network"), "Justice Network");
        assert_eq!(engine.normalize_key("True Crime Network"), "True Crime");
        // N08: designator repair.
        assert_eq!(engine.normalize_key("Sky Sports F1 UHD"), "Sky Sports F1");
        // N11: no Cinemax strip.
        assert_eq!(engine.normalize_key("Cinemax"), "Cinemax");
    }

    #[test]
    fn cached_engines_follow_the_config_and_stay_bounded() {
        let config = EngineConfig::default();
        let first = UnifiedEngine::cached(&config);
        assert!(Arc::ptr_eq(&first, &UnifiedEngine::cached(&config)));
        // A hot-reloaded `[matching]` is a new key: a fresh engine with the new values.
        let stricter = EngineConfig {
            threshold: 70,
            ..EngineConfig::default()
        };
        let other = UnifiedEngine::cached(&stricter);
        assert!(!Arc::ptr_eq(&first, &other));
        assert_eq!(other.threshold(), 70);
        let mut aliased = EngineConfig::default();
        aliased
            .custom_aliases
            .insert("Zorblax".to_string(), vec!["ZBX".to_string()]);
        assert_ne!(
            UnifiedEngine::cache_key(&aliased),
            UnifiedEngine::cache_key(&config)
        );
        // Bounded: after CAPACITY other configs the first is rebuilt, not kept forever.
        for cut in 0..ENGINE_CACHE_CAPACITY as u8 {
            UnifiedEngine::cached(&EngineConfig {
                threshold: 10 + cut,
                ..EngineConfig::default()
            });
        }
        assert!(!Arc::ptr_eq(&first, &UnifiedEngine::cached(&config)));
    }

    #[test]
    fn bare_p_is_not_pacific() {
        let eng = engine();
        // Decided Z07=sm-strict: (P) routes as DEFAULT and normalises away as misc
        // noise, so `Sportsnet (P)` == `Sportsnet` and != `Sportsnet Pacific`
        // (Lineuparr's (P)=Pacific expectations are the known Appendix C conflict).
        assert_eq!(engine_zone("Sportsnet (P)"), FeedZone::Default);
        assert_eq!(engine_zone("Sportsnet Pacific"), FeedZone::West);
        assert!(eng.same("Sportsnet Pacific", "Sportsnet Pacific HD"));
        assert!(eng.same("Sportsnet (P)", "Sportsnet"));
        assert!(!eng.same("Sportsnet (P)", "Sportsnet Pacific"));
        assert!(!eng.same("Sportsnet", "Sportsnet Pacific"));
    }

    #[test]
    fn zone_routing_separates_feeds() {
        let eng = engine();
        assert!(eng.same("HBO East", "HBO East HD"));
        assert!(!eng.same("HBO East", "HBO West"));
        assert!(!eng.same("HBO", "HBO West"));
    }

    #[test]
    fn designator_guard_keeps_issue_50_apart() {
        let eng = engine();
        assert!(!eng.same("Sky Sports F1 UHD", "Sky Sports UHD 1"));
        assert!(eng.same("Sky Sports F1 UHD", "Sky Sports F1 HD"));
    }

    #[test]
    fn hot_paths_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<UnifiedEngine>();
        assert_send_sync::<EngineIndex>();
        assert_send_sync::<crate::identity::IdentityCache>();
        // No interior mutability in hot paths: sharing across rayon threads
        // needs no locks beyond build time.
        let engine = std::sync::Arc::new(UnifiedEngine::decided());
        let index = EngineIndex::build(
            &["ESPN".to_string()],
            &engine,
            &crate::identity::ResolveContext::default(),
        );
        let index = std::sync::Arc::new(index);
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let (engine, index) = (engine.clone(), index.clone());
                std::thread::spawn(move || engine.rank_index("ESPN HD", &index).len())
            })
            .collect();
        for handle in handles {
            assert!(handle.join().is_ok());
        }
    }

    #[test]
    fn default_threshold_is_95_with_the_shipped_presets() {
        assert_eq!(DEFAULT_THRESHOLD, 95);
        assert_eq!(sensitivity_preset("strict"), 95);
        assert_eq!(sensitivity_preset("medium"), 80);
        assert_eq!(sensitivity_preset("80"), 80);
        assert_eq!(sensitivity_preset("nonsense"), 95);
    }

    #[test]
    fn the_shortlist_is_the_same_ranker_without_the_family_cut() {
        // One ranker: at the decision cut the shortlist leads with the family's top;
        // re-cut lower, it keeps the sub-cut candidates the AI gate offers.
        let engine = UnifiedEngine::decided();
        let candidates: Vec<String> = ["Sky Cinema Premiere HD 2", "Sky Cinema Premiere"]
            .iter()
            .map(|name| name.to_string())
            .collect();
        let index = EngineIndex::build(
            &candidates,
            &engine,
            &crate::identity::ResolveContext::default(),
        );
        let family = engine.rank_index("Sky Cinema Premiere", &index);
        let shortlist = engine.shortlist_index_in("Sky Cinema Premiere", None, &index);
        assert_eq!(
            family.first().map(|r| (r.index, r.score)),
            shortlist.first().map(|r| (r.index, r.score))
        );
        assert!(shortlist.iter().all(|r| r.score >= 95));

        let low = engine.with_threshold(60);
        assert_eq!(low.identity_version(), engine.identity_version());
        let shortlist = low.shortlist_index_in("Sky Cinema Premiere", None, &index);
        assert_eq!(shortlist.len(), 2, "{shortlist:?}");
        assert_eq!(shortlist[0].index, 1);
        assert_eq!((shortlist[1].index, shortlist[1].score), (0, 90));
        // The family at the low cut still holds one identity only.
        assert_eq!(low.rank_index("Sky Cinema Premiere", &index).len(), 1);
    }

    #[test]
    fn callsign_first_prefers_the_station_over_the_national_entry() {
        // Decided C07 `callsign-first` (orchestrator): a network-led callsign name
        // whose callsign the merged station table knows ranks the station family
        // first, even though the bare national entry scores a stripped-key Exact
        // 100 above the station's Callsign 95.
        let engine = UnifiedEngine::decided();
        for (query, station) in [
            ("ABC (WABC)", "WABC"),
            ("US: ABC (WABC) HD", "WABC"),
            ("NBC (KSDK)", "KSDK"),
            ("CBS (WFOR) [HD]", "WFOR"),
        ] {
            let candidates: Vec<String> = [
                "ABC",
                "NBC",
                "CBS",
                "WABC",
                "WABC-TV",
                "ABC 7 New York (WABC)",
                "KSDK",
                "NBC 5 St. Louis KSDK",
                "WFOR-TV",
                "CBS 4 Miami (WFOR)",
            ]
            .iter()
            .map(|name| name.to_string())
            .collect();
            let index = EngineIndex::build(
                &candidates,
                &engine,
                &crate::identity::ResolveContext::default(),
            );
            let family = engine.rank_index(query, &index);
            assert!(!family.is_empty(), "{query} ranks nothing");
            let top = &candidates[family[0].index];
            let (callsign, _) = engine.callsign_confidence(top);
            assert_eq!(
                callsign.as_deref().map(normalize_callsign).as_deref(),
                Some(station),
                "{query} tops {top}"
            );
            // The pair verdict on the national entry still holds (fallback): the
            // preference is a rank ordering, not a rejection.
            let national = station_national(station);
            assert!(
                engine.same(query, national),
                "{query} vs {national} still matches at pair level"
            );
        }
    }

    #[test]
    fn callsign_first_falls_back_to_the_national_family_without_a_station() {
        // The pool decides: with no same-callsign candidate in it, the bare
        // national entry wins — today's behaviour for pools lacking the station.
        let engine = UnifiedEngine::decided();
        let candidates: Vec<String> = ["ABC", "NBC", "CBS", "ESPN"]
            .iter()
            .map(|name| name.to_string())
            .collect();
        let index = EngineIndex::build(
            &candidates,
            &engine,
            &crate::identity::ResolveContext::default(),
        );
        for (query, national) in [
            ("ABC (WABC)", "ABC"),
            ("US: ABC (WABC) HD", "ABC"),
            ("NBC (KSDK)", "NBC"),
            ("CBS (WFOR) [HD]", "CBS"),
        ] {
            let family = engine.rank_index(query, &index);
            assert_eq!(family.len(), 1, "{query}: {family:?}");
            assert_eq!(&candidates[family[0].index], national);
        }
    }

    #[test]
    fn a_different_known_callsign_never_joins() {
        // Decided C07 extends the anchor (C06/D04): a candidate carrying a
        // DIFFERENT known callsign is excluded even when the anchor's high/high
        // shape does not see it (e.g. a low-confidence mention of that station).
        let engine = UnifiedEngine::decided();
        assert!(!engine.same("ABC (WABC)", "NBC (KSDK)"));
        assert!(!engine.same("ABC (WABC)", "NBC 5 St. Louis KSDK"));
        assert!(!engine.same("ABC (WABC)", "KOMO News"));
        let candidates: Vec<String> = ["KSDK", "NBC 5 St. Louis KSDK", "NBC", "ABC"]
            .iter()
            .map(|name| name.to_string())
            .collect();
        let index = EngineIndex::build(
            &candidates,
            &engine,
            &crate::identity::ResolveContext::default(),
        );
        let family = engine.rank_index("ABC (WABC)", &index);
        let names: Vec<&str> = family
            .iter()
            .map(|r| candidates[r.index].as_str())
            .collect();
        assert!(
            names.iter().all(|name| !name.contains("KSDK")),
            "no KSDK member: {names:?}"
        );
    }

    #[test]
    fn callsign_first_leaves_callsign_less_queries_alone() {
        // Property: without a high-confidence station callsign on the query the rule
        // is off — no exclusion, no reordering, whatever the pool carries.
        let engine = UnifiedEngine::decided();
        let candidates: Vec<String> = ["ABC", "WABC", "KSDK", "NBC", "ESPN"]
            .iter()
            .map(|name| name.to_string())
            .collect();
        let index = EngineIndex::build(
            &candidates,
            &engine,
            &crate::identity::ResolveContext::default(),
        );
        // A bare national query still takes its own entry first, stations and all.
        let family = engine.rank_index("ABC", &index);
        assert_eq!(&candidates[family[0].index], "ABC");
        // A known station still joins a callsign-less query when it scores: the
        // query's own entry outranks it, nothing is excluded.
        assert!(engine.same("ABC", "US: ABC HD"));
        let family = engine.rank_index("ABC", &index);
        let names: Vec<&str> = family
            .iter()
            .map(|r| candidates[r.index].as_str())
            .collect();
        assert_eq!(names[0], "ABC", "{names:?}");
        // A callsign the station table does not know disables the rule too.
        assert!(engine.same("ABC (WZZZ)", "ABC"));
        let family = engine.rank_index("ABC (WZZZ)", &index);
        assert_eq!(&candidates[family[0].index], "ABC");
    }

    /// The bare national entry for a station callsign (test helper only).
    fn station_national(station: &str) -> &'static str {
        match station {
            "WABC" => "ABC",
            "KSDK" => "NBC",
            "WFOR" => "CBS",
            _ => panic!("unknown station {station}"),
        }
    }

    #[test]
    fn an_identity_equal_candidate_wins_a_tie_against_a_reordered_name() {
        // Step 9 review: the same words reordered are a different channel. Both score
        // 100 (exact vs token-sort), and the query's `US` token used to make the OTHER
        // channel the top — and with it the family.
        let engine = UnifiedEngine::decided();
        let candidates: Vec<String> = [
            "US: CL beIN Sports English SD in Spanish FHD",
            "CL beIN Sports English SD in Spanish",
            "CL beIN Sports Spanish HD in English (Backup)",
            "CL beIN Sports Spanish HD in English",
        ]
        .iter()
        .map(|name| name.to_string())
        .collect();
        let family = engine.rank_in(
            "US: CL beIN Sports Spanish HD in English",
            None,
            &candidates,
        );
        let names: Vec<&str> = family
            .iter()
            .map(|r| candidates[r.index].as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "CL beIN Sports Spanish HD in English (Backup)",
                "CL beIN Sports Spanish HD in English"
            ]
        );
    }
}
