//! `names_*`: Channel-Mapparr's naming on the unified engine.
//!
//! Port of `reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/plugin.py`
//! `load_and_process_channels` (~1869: OTA path 1979-2005, premium 2008-2037, statuses
//! Renamed/Skipped/`Already in correct format`), `rename_unknown_channels` (2303),
//! `_extract_stream_network` (~1745), `_format_ota_name` (~1772),
//! `rename_unknown_channels` (~2303), `fuzzy_matcher.build_final_channel_name` (~1506,
//! tag re-attachment), `market_index.py` (fallback).
//!
//! # Target-name algorithm per path (with CM citations)
//!
//! Every in-scope channel resolves through the unified engine first (the same
//! [`RunEngine`](crosstalk_core::engine_run::RunEngine) + identity cache guide and
//! streams run on): one [`Identity`](crosstalk_matching::identity::Identity) per
//! channel, with `canonical_name`, `callsign`, `network`, `country`, `zone`.
//!
//! - **OTA** (`plugin.py:1979-2005`): the identity carries a callsign present in the
//!   merged station table (`data/merged/stations.json`: CM's FCC `networks.json` +
//!   supplemental + SM's 1.9k table, newest deduplicated union). The target is
//!   [`render_ota`](crosstalk_core::naming::render_ota) over
//!   [`ota_fields`](crosstalk_core::naming::ota_fields) built from the station row
//!   (network affiliation, city, state, callsign) with the network override the
//!   channel name itself states ([`extract_stream_network`], CM's
//!   `_extract_stream_network`: the leading geo/provider token is stripped, the
//!   first `STREAM_NETWORKS` word wins). A template placeholder with no value — or
//!   an unknown placeholder — refuses (`None`, exactly like `_format_ota_name`
//!   returning `None`): the row is Skipped (`Missing required fields for OTA
//!   format`). Opt-in `ota_market_fallback` resolves a market reference
//!   (`crosstalk_matching::market`, CM's `market_index.py`) against the same
//!   station table when no callsign resolved. A callsign with no station row is
//!   Skipped (`Callsign X not in channel databases`) and never tries premium
//!   (`plugin.py:2008`: a found callsign blocks premium matching).
//! - **Premium** (`plugin.py:2008-2037`): the channel's best identity family against
//!   the merged channel DB (one DB per enabled `[matching]` country, newest union).
//!   Ranking is the engine's one ranker at the one `[matching]` cut; the base name
//!   is the family leader's DB entry (CM's `fuzzy_match` winner, the DB
//!   `channel_name` minus its ignored tag groups, so clean never strips what names
//!   writes). CM's tag re-attachment (`build_final_channel_name`: regional
//!   word, extra paren tags, bracket quality tags) is rebuilt from the shared tag
//!   vocabulary ([`crosstalk_matching::tags`]): ignored tags are the vocabulary's
//!   quality + status kinds (CM's default ignored list, `[4K]/[FHD]/[HD]/[SD]/`
//!   `[Unknown]/[Unk]/[Slow]/[Dead]` expanded to paren forms). Regional, paren and
//!   bracket extraction mirrors `extract_tags` (paren-first regional, last bare
//!   region word, callsign-shaped parens skipped, first-group-is-prefix skipped);
//!   the join is `base [Region] [(Extra)...] [[Quality]...]`.
//! - **Unmatched**: no family at the cut. With `tag_unmatched_channels` the
//!   `unknown_suffix` is appended through
//!   [`apply_suffix`](crosstalk_core::naming::apply_suffix) — never doubled (the
//!   documented fix over CM's `current_name + suffix`, which re-tagged
//!   already-tagged names). With it off, or an empty suffix, the row is Skipped
//!   (`No match found in channels.json`). An already-suffixed name is Skipped
//!   (`Already in correct format` / already tagged), never renamed.
//! - **AI gate** (optional, off by default): a premium channel with no family at the
//!   cut is borderline. It is re-ranked by the same engine and index at the gate's
//!   floor (`RunEngine::shortlist_engine` + `shortlist_index_in`, the ranker without
//!   the family cut) and offered to the `[ai.verify]` chain (task `"names"`).
//!   Only a model accept renames, to the accepted candidate's re-attached name.
//!
//! # Planning, CSV, undo
//!
//! `names_preview` (Read) / `names_apply` (Write, confirm) plan from LIVE state (no
//! saved JSON): one [`Op::Set`](crosstalk_core::channel_ops::Op::Set) `Name` per
//! channel with the shared collision resolver on
//! (`PlanOptions.collision = settings.collision_strategy`), no-op drops. The CSV
//! carries CM's rename columns (`channel_id`, `channel_number`, `channel_group`,
//! `current_name`, `new_name`, `status`, `matcher`, `match_method`, `reason`) plus
//! the identity fields (`callsign`, `network`, `country`, `zone`, `canonical`).
//! Apply writes through [`channel_ops::apply`](crosstalk_core::channel_ops::apply)
//! (bulk edit), recorded as run rows so the host
//! [`undo`](crosstalk_core::undo) reverses it.

use anyhow::Result;
use crosstalk_core::ai_gate;
use crosstalk_core::channel_ops::{self, Field, Op, PlanOptions, Selection, SetValue};
use crosstalk_core::csv_report::{self, Report};
use crosstalk_core::dispatcharr::World;
use crosstalk_core::engine_run::RunEngine;
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use crosstalk_matching::engine::{EngineMethod, EngineRanked, UnifiedEngine};
use crosstalk_matching::identity::{Identity, ResolveContext};
use crosstalk_matching::pipeline::Scored;
use crosstalk_matching::tags::{classify_token, TagKind};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The networks a stream name may explicitly claim (CM's `_STREAM_NETWORKS`,
/// `plugin.py`).
pub const STREAM_NETWORKS: &[&str] = &[
    "ABC",
    "CBS",
    "NBC",
    "FOX",
    "CW",
    "PBS",
    "ION",
    "MYTV",
    "MYNETWORKTV",
    "TELEMUNDO",
    "UNIVISION",
    "UNIMAS",
];

/// The regional words CM re-attaches (`extract_tags`).
const REGIONAL_WORDS: &[&str] = &["East", "West", "Pacific", "Central", "Mountain", "Atlantic"];

/// The CSV columns: CM's rename columns first, then the identity fields.
pub const COLUMNS: [&str; 14] = [
    "id",
    "channel_number",
    "group",
    "status",
    "old_name",
    "new_name",
    "matcher",
    "method",
    "reason",
    "callsign",
    "network",
    "country",
    "zone",
    "canonical",
];

/// One in-scope channel, as names sees it.
#[derive(Debug, Clone)]
pub struct ScopedChannel {
    pub id: i64,
    pub name: String,
    pub number: Option<f64>,
    pub group: String,
}

/// One channel's naming decision, before collision resolution.
#[derive(Debug, Clone)]
pub struct NameRow {
    pub id: i64,
    pub group: String,
    pub number: Option<f64>,
    pub old: String,
    pub new: String,
    /// CM's vocabulary: `Renamed` or `Skipped`.
    pub status: &'static str,
    /// CM's `matcher`: `Broadcast (OTA)`, `Premium/Cable`, `unknown`, `none`.
    pub matcher: String,
    /// CM's `match_method` wording.
    pub method: String,
    pub reason: String,
    pub identity: Identity,
}

/// One channel's final outcome, post-collision: the oracle row.
#[derive(Debug, Clone)]
pub struct NameOutput {
    pub id: i64,
    pub number: Option<f64>,
    pub group: String,
    pub old: String,
    pub new: String,
    pub status: String,
    pub matcher: String,
    pub method: String,
    pub reason: String,
    pub callsign: Option<String>,
    pub network: Option<String>,
    pub country: Option<String>,
    pub zone: String,
    pub canonical: String,
}

/// The network a stream name explicitly claims (CM's `_extract_stream_network`).
///
/// The leading geo/provider token (`US:`, `CITY:`, `[US] |`) is stripped unless it
/// IS a network word; the first network word wins. Only recognized broadcast
/// networks count, so a callsign-led name returns `None`.
pub fn extract_stream_network(channel_name: &str) -> Option<String> {
    let trimmed = channel_name.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut rest = trimmed;
    // Optional leading geo/provider prefix, up to 12 letters, like CM.
    static PREFIX: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let prefix = PREFIX.get_or_init(|| {
        regex::Regex::new(r#"^\s*[\[(]?([A-Za-z]{2,12})[\])]?\s*[:|]\s*(.*)$"#)
            .expect("static regex")
    });
    if let Some(caps) = prefix.captures(trimmed) {
        let head = caps
            .get(1)
            .map(|m| m.as_str().to_uppercase())
            .unwrap_or_default();
        if !STREAM_NETWORKS.contains(&head.as_str()) {
            rest = caps.get(2).map(|m| m.as_str()).unwrap_or(trimmed);
        }
    }
    static TOKEN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let token = TOKEN.get_or_init(|| regex::Regex::new(r"([A-Za-z]{2,12})").expect("static regex"));
    let word = token
        .captures(rest)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_uppercase())?;
    if STREAM_NETWORKS.contains(&word.as_str()) {
        Some(word)
    } else {
        None
    }
}

/// Whether an inner paren tag is ignored (CM's ignored-tags check).
///
/// Ignored = the vocabulary's quality + status kinds (CM's default ignored list:
/// `[4K]/[FHD]/[HD]/[SD]/[Unknown]/[Unk]/[Slow]/[Dead]`, each in both bracket
/// forms). `tag` is the INNER text (no brackets).
fn is_ignored_tag(inner: &str) -> bool {
    match classify_token(inner.trim()) {
        Some(tag) => matches!(tag.kind, TagKind::Quality | TagKind::Status),
        None => false,
    }
}

fn is_callsign_shaped(inner: &str) -> bool {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(r"(?i)^[KW][A-Z]{3}(?:-(?:TV|CD|LP|DT|LD))?$").expect("static regex")
    });
    re.is_match(inner.trim())
}

/// Split CM's re-attachable tags off a query name (`extract_tags`, `plugin.py:975`).
///
/// Returns `(regional, extra_tags, quality_tags)` with original bracket shapes kept:
/// regional is the bare word (title-cased), extras keep `(…)`, qualities keep `[…]`.
/// Ignored (quality/status vocabulary) tags never re-attach. Only the trailing
/// structure CM reads is reproduced: regional prefers the paren form, else the last
/// bare region word; paren groups are all scanned (first skipped when a prefix);
/// bracket groups are all scanned.
pub fn extract_reattach_tags(name: &str) -> (Option<String>, Vec<String>, Vec<String>) {
    // Regional: paren form first, else the last bare region word (CM's two patterns).
    static PAREN_REGION: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let paren_region = PAREN_REGION.get_or_init(|| {
        regex::Regex::new(r"(?i)\((East|West|Pacific|Central|Mountain|Atlantic)\)")
            .expect("static regex")
    });
    let mut regional: Option<String> = paren_region
        .captures(name)
        .and_then(|caps| caps.get(1))
        .map(|m| {
            let mut chars = m.as_str().chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().to_string() + &chars.as_str().to_lowercase(),
            }
        });
    if regional.is_none() {
        // CM's look-ahead finds the LAST bare region word (`re.search` with a
        // trailing `(?!.*\b(region)\b)`); Rust `regex` has no look-around, so
        // collect every word hit and keep the last.
        static WORD: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        let word = WORD.get_or_init(|| {
            regex::Regex::new(r"(?i)\b(East|West|Pacific|Central|Mountain|Atlantic)\b")
                .expect("static regex")
        });
        let last = word.captures_iter(name).last().and_then(|caps| caps.get(1));
        regional = last.map(|m| {
            let mut chars = m.as_str().chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().to_string() + &chars.as_str().to_lowercase(),
            }
        });
    }

    // Paren extras: every `(…)` group except a leading prefix, ignored tags,
    // regional words and callsign-shaped groups (CM's loop verbatim).
    static PARENS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let parens = PARENS.get_or_init(|| regex::Regex::new(r"\(([^\)]+)\)").expect("static regex"));
    let groups: Vec<(usize, String)> = parens
        .captures_iter(name)
        .filter_map(|caps| {
            caps.get(0).map(|full| {
                (
                    full.start(),
                    caps.get(1)
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_default(),
                )
            })
        })
        .collect();
    let first_is_prefix = !groups.is_empty() && name.trim_start().starts_with('(');
    let mut extra_tags = Vec::new();
    for (idx, tag) in groups {
        if idx == 0 && first_is_prefix {
            continue;
        }
        if is_ignored_tag(&tag) {
            continue;
        }
        if REGIONAL_WORDS
            .iter()
            .any(|word| word.eq_ignore_ascii_case(tag.trim()))
        {
            continue;
        }
        if is_callsign_shaped(&tag) {
            continue;
        }
        extra_tags.push(format!("({})", tag));
    }

    // Bracket qualities: every `[…]` group that is not ignored.
    static BRACKETS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let brackets =
        BRACKETS.get_or_init(|| regex::Regex::new(r"\[([^\]]+)\]").expect("static regex"));
    let mut quality_tags = Vec::new();
    for caps in brackets.captures_iter(name) {
        let tag = caps
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        if is_ignored_tag(&tag) {
            continue;
        }
        quality_tags.push(format!("[{}]", tag));
    }

    (regional, extra_tags, quality_tags)
}

/// Re-attach CM's tags onto a premium base (`build_final_channel_name`).
///
/// A tag the base already carries is never re-attached: the point of the
/// re-attachment is preserving provider decorations the DB canonical lacks, and
/// re-adding what is there only doubles it (`KSPT-DT3 (HEROICN)` must stay
/// itself, not become `KSPT-DT3 (HEROICN) (HEROICN)`). CM re-attaches
/// unconditionally; the guard here is the same never-double rule as
/// [`apply_suffix`](crosstalk_core::naming::apply_suffix), one tag at a time.
///
/// The regional word is a feed zone, so a base that already states one (the
/// engine's own zone reading: `Cinemax Action (Pacific)` is West) gains no second:
/// the family already matched zones, and `Cinemax Action (Pacific) West` would be
/// two spellings of one feed.
///
/// The DB base itself loses its ignored (quality/status) tag groups: `BX Culture
/// (HD)` names as `BX Culture`. CM keeps the DB name verbatim, but a base that
/// carries what `clean` strips makes the two actions fight (clean drops `(HD)`,
/// names puts it back, forever); ignored means ignored on both sides.
pub fn build_final_channel_name(
    base: &str,
    regional: Option<&str>,
    extra_tags: &[String],
    quality_tags: &[String],
) -> String {
    let stripped = strip_ignored_tags(base);
    let base = stripped.as_str();
    let mut parts = vec![base.to_string()];
    if let Some(region) = regional.map(str::trim).filter(|r| !r.is_empty()) {
        // Whole-word, case-insensitive: `Central` inside `Comedy Central` counts,
        // `East` inside `Yeast` does not.
        let matches = regex::Regex::new(&format!(r"(?i)\b{}\b", regex::escape(region)))
            .map(|re| re.is_match(base))
            .unwrap_or(false);
        let base_states_zone = crosstalk_matching::engine::engine_zone(base)
            != crosstalk_matching::country::FeedZone::Default;
        if !matches && !base_states_zone {
            parts.push(region.to_string());
        }
    }
    let mut seen: HashSet<String> = HashSet::new();
    for tag in extra_tags.iter().chain(quality_tags.iter()) {
        let folded = tag.to_lowercase();
        if seen.insert(folded.clone()) && !base.to_lowercase().contains(&folded) {
            parts.push(tag.clone());
        }
    }
    parts.join(" ")
}

/// A name without its ignored `(…)`/`[…]` tag groups (quality/status vocabulary),
/// whitespace collapsed. A name that is nothing but ignored tags is kept whole.
fn strip_ignored_tags(name: &str) -> String {
    static GROUP: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let group = GROUP
        .get_or_init(|| regex::Regex::new(r"\(([^()]+)\)|\[([^\[\]]+)\]").expect("static regex"));
    let stripped = group.replace_all(name, |caps: &regex::Captures<'_>| {
        let inner = caps
            .get(1)
            .or_else(|| caps.get(2))
            .map_or("", |m| m.as_str());
        if is_ignored_tag(inner) {
            String::new()
        } else {
            caps[0].to_string()
        }
    });
    let collapsed = stripped.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        name.trim().to_string()
    } else {
        collapsed
    }
}

/// The merged channel-DB names for the enabled `[matching]` countries, in DB order.
///
/// Codes come from [`MatchingConfig::databases`](crosstalk_core::config::MatchingConfig::databases)
/// (explicit list, else the US DB): the same pool the unified engine ranks under.
pub fn premium_db_names(databases: &[String]) -> Vec<String> {
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for code in databases {
        let Some(raw) = crosstalk_matching::data::merged::merged_database_json(code) else {
            continue;
        };
        let parsed: serde_json::Value = serde_json::from_str(raw).unwrap_or_default();
        let channels = parsed
            .get("channels")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for channel in channels {
            let name = channel
                .get("channel_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if name.is_empty() || !seen.insert(name.clone()) {
                continue;
            }
            names.push(name);
        }
    }
    names
}

/// Look a callsign up in the merged station table (full form, then the
/// `-TV`/`-CD`-stripped base, like CM's `channel_lookup` which indexes every
/// station under BOTH spellings).
///
/// The merged table is keyed by full callsign (`WABC-TV`), so the base form
/// (`WABC`) resolves through a cached base -> full map built once per process.
pub fn station_for_callsign(
    callsign: &str,
) -> Option<crosstalk_matching::data::merged::MergedStation> {
    let stations = crosstalk_matching::data::merged::merged_stations();
    let upper = callsign.trim().to_uppercase();
    if upper.is_empty() {
        return None;
    }
    if let Some(station) = stations.stations.get(&upper) {
        return Some(station.clone());
    }
    // The extracted callsign may carry the suffix (`WABC-TV`) while the table
    // holds the other spelling: try the normalized base both ways.
    let base = crosstalk_matching::callsign::normalize_callsign(&upper);
    if let Some(station) = stations.stations.get(&base) {
        return Some(station.clone());
    }
    if let Some(full) = base_lookup()
        .get(&upper)
        .or_else(|| base_lookup().get(&base))
    {
        if let Some(station) = stations.stations.get(full) {
            return Some(station.clone());
        }
    }
    None
}

/// Base callsign -> full table key (first wins), mirroring CM's dual indexing.
///
/// Sorted keys, so the winner never depends on `HashMap` iteration order (which
/// is random per process): two full spellings sharing a base (`KAZT-CD` vs
/// `KAZT-TV`) always resolve the same way, which the oracle `--check` gate pins
/// byte-identically.
fn base_lookup() -> &'static HashMap<String, String> {
    static LOOKUP: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
    LOOKUP.get_or_init(|| {
        let mut keys: Vec<&String> = crosstalk_matching::data::merged::merged_stations()
            .stations
            .keys()
            .collect();
        keys.sort();
        let mut map = HashMap::new();
        for key in keys {
            let base = crosstalk_matching::callsign::normalize_callsign(key);
            map.entry(base).or_insert_with(|| key.clone());
        }
        map
    })
}

/// Whether a callsign is a real station in the merged table.
pub fn is_station_callsign(callsign: &str) -> bool {
    station_for_callsign(callsign).is_some()
}

fn zone_label(identity: &Identity) -> &'static str {
    match identity.zone {
        crosstalk_matching::country::FeedZone::East => "EAST",
        crosstalk_matching::country::FeedZone::West => "WEST",
        crosstalk_matching::country::FeedZone::Default => "DEFAULT",
    }
}

fn method_label(method: &EngineMethod) -> String {
    match method {
        EngineMethod::Alias => "Alias".to_string(),
        EngineMethod::Exact => "Exact".to_string(),
        EngineMethod::Substring => "Substring".to_string(),
        EngineMethod::Fuzzy(score) => format!("Fuzzy Match - fuzzy ({score}) (score: {score})"),
        EngineMethod::Callsign => "Callsign".to_string(),
    }
}

/// The heuristic naming decision for one channel (no AI yet).
///
/// Returns the row plus whether it is borderline: the premium path ran and found
/// no family at the cut (no callsign, no market station). Only borderline rows may
/// reach the AI gate; OTA, blocked-callsign, confident and already-correct premium
/// rows never do.
#[allow(clippy::too_many_arguments)]
pub fn decide_heuristic(
    channel: &ScopedChannel,
    identity: &Identity,
    engine: &UnifiedEngine,
    index: &crosstalk_matching::engine::EngineIndex,
    db_names: &[String],
    settings: &crate::settings::LineupSettings,
) -> (NameRow, bool) {
    let base_row =
        |status: &'static str, new: String, matcher: &str, method: String, reason: String| {
            NameRow {
                id: channel.id,
                group: channel.group.clone(),
                number: channel.number,
                old: channel.name.clone(),
                new,
                status,
                matcher: matcher.to_string(),
                method,
                reason,
                identity: identity.clone(),
            }
        };

    // OTA: the identity's callsign resolves to a merged station.
    if let Some(callsign) = identity.callsign.clone() {
        if is_station_callsign(&callsign) {
            if let Some(station) = station_for_callsign(&callsign) {
                let network_override = extract_stream_network(&channel.name);
                let fields = crosstalk_core::naming::ota_fields(
                    station.network_affiliation.as_deref().unwrap_or(""),
                    network_override.as_deref(),
                    station.community_served_city.as_deref().unwrap_or(""),
                    station.community_served_state.as_deref().unwrap_or(""),
                    &callsign,
                );
                match crosstalk_core::naming::render_ota(&settings.ota_format, &fields) {
                    Some(target) if target != channel.name => {
                        return (
                            base_row(
                                "Renamed",
                                target.clone(),
                                "Broadcast (OTA)",
                                "OTA - Callsign Match".to_string(),
                                format!("OTA callsign {callsign} resolves to a station"),
                            ),
                            false,
                        );
                    }
                    Some(_) => {
                        return (
                            base_row(
                                "Skipped",
                                channel.name.clone(),
                                "none",
                                "No Match".to_string(),
                                "Already in correct format".to_string(),
                            ),
                            false,
                        );
                    }
                    None => {
                        return (
                            base_row(
                                "Skipped",
                                channel.name.clone(),
                                "none",
                                "No Match".to_string(),
                                "Missing required fields for OTA format".to_string(),
                            ),
                            false,
                        );
                    }
                }
            }
        }
    }
    // A printed callsign that resolved no station: the market fallback (below) may
    // still name it, but it never reaches premium (CM's `match_broadcast_channel`
    // returns `(callsign, None)` after the fallback misses, and a found callsign
    // blocks premium: `plugin.py:2008`). Without this, `ABC (WZZZ)` would lose its
    // callsign and land on the national `ABC` entry.
    let blocked_callsign = identity.callsign.clone();

    // OTA market fallback (opt-in): a market reference resolves to one station.
    if settings.ota_market_fallback {
        if let Some(network) = extract_stream_network(&channel.name) {
            if let Some(station) =
                crosstalk_matching::market::market_station_for(&channel.name, &network)
            {
                let callsign = station.callsign.clone();
                let fields = crosstalk_core::naming::ota_fields(
                    station.network_affiliation.as_deref().unwrap_or(""),
                    Some(&network),
                    station.community_served_city.as_deref().unwrap_or(""),
                    station.community_served_state.as_deref().unwrap_or(""),
                    &callsign,
                );
                match crosstalk_core::naming::render_ota(&settings.ota_format, &fields) {
                    Some(target) if target != channel.name => {
                        return (
                            base_row(
                                "Renamed",
                                target,
                                "Broadcast (OTA)",
                                "OTA - Callsign Match".to_string(),
                                format!("OTA market {network} resolves to {callsign}"),
                            ),
                            false,
                        );
                    }
                    Some(_) => {
                        return (
                            base_row(
                                "Skipped",
                                channel.name.clone(),
                                "none",
                                "No Match".to_string(),
                                "Already in correct format".to_string(),
                            ),
                            false,
                        );
                    }
                    None => {
                        return (
                            base_row(
                                "Skipped",
                                channel.name.clone(),
                                "none",
                                "No Match".to_string(),
                                "Missing required fields for OTA format".to_string(),
                            ),
                            false,
                        );
                    }
                }
            }
        }
    }

    // The blocked callsign goes through the unknown handling, like CM's
    // `rename_unknown_channels` tagging every Skipped row.
    if let Some(callsign) = blocked_callsign {
        let (new, status, matcher, method, _) = unmatched_target(&channel.name, settings);
        let reason = if status == "Renamed" {
            format!("Callsign {callsign} not in channel databases; tagged with the unknown suffix")
        } else {
            format!("Callsign {callsign} not in channel databases")
        };
        return (base_row(status, new, &matcher, method, reason), false);
    }

    // Premium: the best identity family against the merged DB at the cut.
    let family = engine.rank_index(&channel.name, index);
    if let Some(top) = family.first() {
        let base = db_names.get(top.index).cloned().unwrap_or_default();
        let (regional, extra, quality) = extract_reattach_tags(&channel.name);
        let target = build_final_channel_name(&base, regional.as_deref(), &extra, &quality);
        if target == channel.name {
            return (
                base_row(
                    "Skipped",
                    channel.name.clone(),
                    "none",
                    "No Match".to_string(),
                    "Already in correct format".to_string(),
                ),
                false,
            );
        }
        return (
            base_row(
                "Renamed",
                target,
                "Premium/Cable",
                method_label(&top.method),
                format!("premium match {}% {}", top.score, method_label(&top.method)),
            ),
            false,
        );
    }

    // No family at the cut: borderline (AI) or unmatched.
    let (new, status, matcher, method, reason) = unmatched_target(&channel.name, settings);
    (base_row(status, new, &matcher, method, reason), true)
}

/// The unmatched target for a name with no family (no AI): tag or skip.
fn unmatched_target(
    name: &str,
    settings: &crate::settings::LineupSettings,
) -> (String, &'static str, String, String, String) {
    if settings.tag_unmatched_channels && !settings.unknown_suffix.trim().is_empty() {
        let (tagged, already) =
            crosstalk_core::naming::apply_suffix(name, &settings.unknown_suffix);
        if already || tagged == name {
            return (
                name.to_string(),
                "Skipped",
                "none".to_string(),
                "No Match".to_string(),
                "Already in correct format".to_string(),
            );
        }
        return (
            tagged,
            "Renamed",
            "unknown".to_string(),
            "Unknown Suffix".to_string(),
            "No match found in channels.json; tagged with the unknown suffix".to_string(),
        );
    }
    (
        name.to_string(),
        "Skipped",
        "none".to_string(),
        "No Match".to_string(),
        "No match found in channels.json".to_string(),
    )
}

/// A premium DB entry as an AI candidate.
#[derive(Debug, Clone)]
struct DbCandidate {
    id: i64,
    name: String,
}

impl crosstalk_core::match_run::Candidate for DbCandidate {
    fn id(&self) -> i64 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A channel as the AI query.
#[derive(Debug, Clone)]
struct NameQuery {
    name: String,
    number: Option<f64>,
}

impl ai_gate::Query for NameQuery {
    fn name(&self) -> &str {
        &self.name
    }

    fn number(&self) -> Option<f64> {
        self.number
    }
}

fn to_scored(family: &[EngineRanked]) -> Vec<Scored> {
    family
        .iter()
        .map(|member| {
            let components = match member.method {
                EngineMethod::Alias => vec!["Alias"],
                EngineMethod::Exact => vec!["Exact"],
                EngineMethod::Substring => vec!["Substring"],
                EngineMethod::Fuzzy(_) => vec!["Fuzzy"],
                EngineMethod::Callsign => vec!["Callsign"],
            };
            Scored {
                index: member.index,
                score: member.score,
                components,
            }
        })
        .collect()
}

/// Plan the renames: the change rows as per-channel `Set Name` through the shared
/// collision resolver (CM has none; Name Cleaner's strategies, global).
///
/// Pure: nothing is written. [`run`] applies the same plan through
/// [`channel_ops::apply`].
pub fn plan_names(
    world: &World,
    selection: &Selection,
    rows: &[NameRow],
    settings: &crate::settings::LineupSettings,
) -> Result<channel_ops::Plan> {
    let strategy = crate::clean::collision_strategy(settings)?;
    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for row in rows {
        if row.status == "Renamed" {
            values.insert(row.id, SetValue::Name(row.new.clone()));
        }
    }
    let ops = vec![Op::Set {
        field: Field::Name,
        values,
    }];
    let options = PlanOptions {
        collision: Some(strategy),
        collision_sep: crate::clean::CleanRules::from_settings(settings).join_separator,
        ..PlanOptions::default()
    };
    channel_ops::plan_with_options(world, selection, &ops, &options)
}

/// Final per-channel outcomes from the naming rows plus the plan.
///
/// A collision-`skip` loser keeps its old name (status `Skipped`, reason names the
/// collision); a `suffix`/`numeric` loser lands on the resolver's name.
pub fn final_outputs(rows: &[NameRow], plan: &channel_ops::Plan) -> Vec<NameOutput> {
    let skipped: HashMap<i64, &str> = plan
        .skipped
        .iter()
        .map(|skip| {
            let status = if skip.reason.contains("collision") {
                "collision"
            } else {
                "nochange"
            };
            (skip.channel_id, status)
        })
        .collect();
    let final_name: HashMap<i64, &str> = plan
        .changes
        .iter()
        .filter_map(|change| match (&change.field, &change.after) {
            (Field::Name, channel_ops::Value::Name(name)) => {
                Some((change.channel_id, name.as_str()))
            }
            _ => None,
        })
        .collect();
    rows.iter()
        .map(|row| {
            let (status, new, reason) = match row.status {
                "Renamed" => match skipped.get(&row.id).copied() {
                    Some("collision") => (
                        "Skipped".to_string(),
                        row.old.clone(),
                        plan.skipped
                            .iter()
                            .find(|skip| skip.channel_id == row.id)
                            .map(|skip| skip.reason.clone())
                            .unwrap_or_else(|| "collision".to_string()),
                    ),
                    _ => (
                        "Renamed".to_string(),
                        final_name
                            .get(&row.id)
                            .copied()
                            .unwrap_or(row.new.as_str())
                            .to_string(),
                        row.reason.clone(),
                    ),
                },
                _ => ("Skipped".to_string(), row.old.clone(), row.reason.clone()),
            };
            NameOutput {
                id: row.id,
                number: row.number,
                group: row.group.clone(),
                old: row.old.clone(),
                new,
                status,
                matcher: row.matcher.clone(),
                method: row.method.clone(),
                reason,
                callsign: row.identity.callsign.clone(),
                network: row.identity.network.clone(),
                country: row.identity.country.clone(),
                zone: zone_label(&row.identity).to_string(),
                canonical: row.identity.canonical_name.clone(),
            }
        })
        .collect()
}

/// Run `names_preview` / `names_apply`.
///
/// Preview plans and writes the CSV; nothing is written to Dispatcharr. Apply
/// re-plans from live state and writes through [`channel_ops::apply`], recorded as
/// run rows for the host undo.
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    let service = ai_gate::service(context.config());
    run_with_service(app, context, settings, dry_run, service.as_ref()).await
}

/// [`run`] with an injectable AI service (tests script the decider).
#[allow(clippy::too_many_lines)]
pub async fn run_with_service(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
    ai: Option<&crosstalk_core::ai::AiService>,
) -> Result<JobOutcome> {
    let action_id = if dry_run {
        crate::ACTION_NAMES_PREVIEW
    } else {
        crate::ACTION_NAMES_APPLY
    };
    let client = context.dispatcharr()?;
    let world: World = client.load_world().await?;
    let config = context.config();

    let base = context.base_selection(&world)?;
    let scoped = context.scoped_channels(&world)?;
    let channels: Vec<ScopedChannel> = scoped
        .channels
        .iter()
        .map(|channel| ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            number: channel.display_number(),
            group: world.group_name(channel.channel_group_id),
        })
        .collect();

    // The unified stack: the same engine + identity cache guide and streams run on.
    let run_engine = RunEngine::for_config(&config.matching, context.store().cloned());
    let engine = Arc::clone(run_engine.engine());
    let db_names = premium_db_names(&config.matching.databases());
    let index = Arc::new(run_engine.build_index(&db_names));

    // Heuristic decisions + families for the identity warm-up.
    let ctx = ResolveContext::default();
    let mut rows: Vec<NameRow> = Vec::with_capacity(channels.len());
    let mut families: Vec<Vec<EngineRanked>> = Vec::with_capacity(channels.len());
    let mut borderline: Vec<usize> = Vec::new();
    for (ordinal, channel) in channels.iter().enumerate() {
        let identity = run_engine.resolve(&channel.name, &ctx);
        let family = engine.rank_index(&channel.name, &index);
        families.push(family.clone());
        let (row, no_family) =
            decide_heuristic(channel, &identity, &engine, &index, &db_names, settings);
        // Borderline = premium path with no family and the AI gate armed. The flag
        // comes from the decision itself, not the row's wording: a tagged unmatched
        // row (`[Unk]`, the default) is borderline, an already-correct premium or
        // OTA row never is.
        if no_family && settings.ai_gate_enabled && config.ai.verify.enabled {
            borderline.push(ordinal);
        }
        rows.push(row);
    }

    // The AI gate: re-rank borderline channels at the gate's floor and ask the chain.
    // Only a model accept renames (task "names"); anything else keeps the unmatched row.
    if !borderline.is_empty() {
        if let (Some(shortlist_engine), Some(service)) =
            (run_engine.shortlist_engine(&config.ai.verify), ai)
        {
            let candidates: Vec<DbCandidate> = db_names
                .iter()
                .enumerate()
                .map(|(i, name)| DbCandidate {
                    id: i as i64,
                    name: name.clone(),
                })
                .collect();
            for ordinal in borderline {
                if context.is_cancelled() {
                    anyhow::bail!("cancelled by the operator");
                }
                let channel = &channels[ordinal];
                let shortlist = shortlist_engine.shortlist_index_in(&channel.name, None, &index);
                if shortlist.is_empty() {
                    continue;
                }
                let scored = to_scored(&shortlist);
                let Some((reason, offered)) =
                    ai_gate::shortlist(&scored, &candidates, &config.ai.verify)
                else {
                    continue;
                };
                let query = NameQuery {
                    name: channel.name.clone(),
                    number: channel.number,
                };
                let outcome = ai_gate::ask(
                    Some(service),
                    &query,
                    &candidates,
                    &scored,
                    &offered,
                    reason,
                    engine.known(),
                    &config.ai.verify,
                    || context.is_cancelled(),
                    Some("names"),
                )
                .await;
                context.record_ai_decision(
                    &outcome.to_store(ordinal as i64, &config.ai.verify.provider),
                )?;
                if !outcome.answered {
                    continue;
                }
                if let Some(position) = outcome.accepted {
                    if let Some(pick) = scored
                        .get(position)
                        .and_then(|s| candidates.get(s.index).map(|c| (c.name.clone(), s.score)))
                    {
                        // The accepted candidate still clears the gate's country rule:
                        // a foreign accept is not a rename.
                        let accept_ok = engine.country_keep_index(
                            None,
                            &engine.normalize_key(&channel.name),
                            &index,
                            scored[position].index,
                            &mut crosstalk_matching::country::CountryFilterCount::default(),
                        );
                        if !accept_ok {
                            continue;
                        }
                        let (regional, extra, quality) = extract_reattach_tags(&channel.name);
                        let target = build_final_channel_name(
                            &pick.0,
                            regional.as_deref(),
                            &extra,
                            &quality,
                        );
                        // An accept onto the name the channel already has leaves it
                        // alone: it was matched, so it is not tagged unknown either.
                        let (status, new) = if target == channel.name {
                            ("Skipped", channel.name.clone())
                        } else {
                            ("Renamed", target)
                        };
                        rows[ordinal] = NameRow {
                            id: channel.id,
                            group: channel.group.clone(),
                            number: channel.number,
                            old: channel.name.clone(),
                            new,
                            status,
                            matcher: "Premium/Cable".to_string(),
                            method: format!("AI {}", outcome.decision),
                            reason: format!("AI gate accepted {} ({}%)", pick.0, pick.1),
                            identity: rows[ordinal].identity.clone(),
                        };
                    }
                }
            }
        }
    }

    // Persist every query + family identity for the other apps.
    let skip = vec![false; channels.len()];
    let queries: Vec<String> = channels.iter().map(|c| c.name.clone()).collect();
    run_engine
        .warm_families(&queries, &families, &db_names, &skip)
        .await;

    let plan = plan_names(&world, &base, &rows, settings)?;

    let applied = if dry_run {
        None
    } else {
        Some(channel_ops::apply(client, &plan, context, action_id).await)
    };

    let outputs = final_outputs(&rows, &plan);
    let mut renamed = 0usize;
    let mut skipped = 0usize;
    let mut ota = 0usize;
    let mut premium = 0usize;
    let mut unknown = 0usize;
    for output in &outputs {
        match output.status.as_str() {
            "Renamed" => {
                renamed += 1;
                match output.matcher.as_str() {
                    "Broadcast (OTA)" => ota += 1,
                    "Premium/Cable" => premium += 1,
                    _ => unknown += 1,
                }
            }
            _ => skipped += 1,
        }
    }

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = if dry_run {
        "names_preview"
    } else {
        "names_applied"
    };
    let title = if dry_run {
        "Names Preview Report"
    } else {
        "Names Applied Report"
    };
    let cut = crosstalk_core::engine_run::cut_setting(&config.matching);
    let report = Report::new(&context.exports_dir(), app, stem, title)
        .channels_processed(plan.selected.len())
        .result("Selection", &scoped.filter_description)
        .result("Changes planned", plan.changes.len())
        .result(
            "Changes written",
            match &applied {
                Some(report) => report.ok.to_string(),
                None => "none, this was a preview".to_string(),
            },
        )
        .result("Skipped", plan.skipped.len() + skipped)
        .setting(cut.0, cut.1)
        .write(context.config(), &schema, &values, &COLUMNS, |writer| {
            for output in &outputs {
                writer.write_record([
                    output.id.to_string(),
                    output.number.map(|n| format!("{n}")).unwrap_or_default(),
                    csv_report::safe_cell(&output.group),
                    output.status.clone(),
                    csv_report::safe_cell(&output.old),
                    csv_report::safe_cell(&output.new),
                    csv_report::safe_cell(&output.matcher),
                    csv_report::safe_cell(&output.method),
                    csv_report::safe_cell(&output.reason),
                    output.callsign.clone().unwrap_or_default(),
                    output.network.clone().unwrap_or_default(),
                    output.country.clone().unwrap_or_default(),
                    output.zone.clone(),
                    csv_report::safe_cell(&output.canonical),
                ])?;
            }
            Ok(())
        })?;

    context.record_rows(&channel_ops::run_rows(&plan, applied.as_ref()))?;

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let mut summary = if dry_run {
        format!(
            "Names preview: {renamed} rename(s) ({ota} OTA, {premium} premium, {unknown} unknown), {skipped} skipped"
        )
    } else {
        let (ok, failed) = applied
            .as_ref()
            .map(|report| (report.ok, report.failed))
            .unwrap_or((0, 0));
        format!(
            "Names apply: renamed {ok} channel(s) ({failed} failed, {ota} OTA, {premium} premium, {unknown} unknown), {skipped} skipped"
        )
    };
    if renamed > 0 {
        summary.push_str(&format!(" — selection: {}", scoped.filter_description));
    }
    let mut built = OutcomeBuilder::new(summary)
        .count("channels", plan.selected.len() as i64)
        .count("renamed", renamed as i64)
        .count("ota", ota as i64)
        .count("premium", premium as i64)
        .count("unknown", unknown as i64)
        .count("skipped", skipped as i64)
        .count(
            "written",
            applied.as_ref().map(|report| report.ok as i64).unwrap_or(0),
        )
        .count(
            "failed",
            applied
                .as_ref()
                .map(|report| report.failed as i64)
                .unwrap_or(0),
        )
        .report(Some(report));
    if let Some(note) = channel_ops::cancelled_note(context, &plan, applied.as_ref()) {
        built = built.cancelled().note(note);
    }
    Ok(built.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::config::CrosstalkConfig;
    use crosstalk_core::dispatcharr::{Channel, ChannelGroup};
    use crosstalk_core::jobs::VecSink;
    use crosstalk_core::store::{RunStatus, Store};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_settings() -> crate::settings::LineupSettings {
        crate::settings::LineupSettings::default()
    }

    fn test_engine() -> (
        Arc<UnifiedEngine>,
        Arc<crosstalk_matching::engine::EngineIndex>,
        Vec<String>,
    ) {
        let engine = Arc::new(UnifiedEngine::decided());
        let config = CrosstalkConfig::default();
        let db_names = premium_db_names(&config.matching.databases());
        assert!(!db_names.is_empty(), "merged US DB is embedded");
        let index = Arc::new(crosstalk_matching::engine::EngineIndex::build(
            &db_names,
            &engine,
            &ResolveContext::default(),
        ));
        (engine, index, db_names)
    }

    fn scoped(id: i64, name: &str) -> ScopedChannel {
        ScopedChannel {
            id,
            name: name.to_string(),
            number: Some(id as f64),
            group: "Test".to_string(),
        }
    }

    fn resolve(engine: &UnifiedEngine, name: &str) -> Identity {
        crosstalk_matching::identity::resolve_with(engine, name, &ResolveContext::default())
    }

    #[test]
    fn stream_network_extraction_reads_the_leading_claim() {
        // CM's `_extract_stream_network`: the geo/provider prefix is stripped, the
        // first STREAM_NETWORKS word wins, callsign-led names return None.
        assert_eq!(
            extract_stream_network("US: CBS 7 Lingfield"),
            Some("CBS".to_string())
        );
        assert_eq!(
            extract_stream_network("CITY: PBS KETC ST. LOUIS"),
            Some("PBS".to_string())
        );
        assert_eq!(
            extract_stream_network("CBS: CBS News"),
            Some("CBS".to_string())
        );
        assert_eq!(extract_stream_network("WABC-TV"), None);
        assert_eq!(extract_stream_network("ESPN HD"), None);
        assert_eq!(extract_stream_network(""), None);
    }

    #[test]
    fn ota_fields_case_like_cm_and_suffixes_strip() {
        // CM's `_format_ota_name` case rules: network upper, city title, state upper,
        // callsign through normalize_callsign.
        let fields =
            crosstalk_core::naming::ota_fields("cbs & fox", None, "birmingham", "al", "WIAT-TV");
        assert_eq!(fields.network, "CBS");
        assert_eq!(fields.city, "Birmingham");
        assert_eq!(fields.state, "AL");
        assert_eq!(fields.callsign, "WIAT");
        // The stream's own network wins over the FCC affiliation.
        let over = crosstalk_core::naming::ota_fields("CBS", Some("fox"), "miami", "fl", "WFOR-TV");
        assert_eq!(
            (over.network.as_str(), over.callsign.as_str()),
            ("FOX", "WFOR")
        );
        // Subchannel affiliation strings parse to the first real network.
        let sub = crosstalk_core::naming::ota_fields(
            "CBS Ch 3.1, CW/MTN Ch 3.2",
            None,
            "miami",
            "fl",
            "WFOR",
        );
        assert_eq!(sub.network, "CBS");
    }

    #[test]
    fn ota_template_refuses_a_missing_placeholder_like_cm() {
        // Every `{word}` placeholder must be known and non-empty, or render_ota is
        // None — exactly like `_format_ota_name` returning None.
        let fields = crosstalk_core::naming::ota_fields("ABC", None, "NEW YORK", "NY", "WABC");
        assert_eq!(
            crosstalk_core::naming::render_ota("{NETWORK} - {STATE} {CITY} ({CALLSIGN})", &fields),
            Some("ABC - NY New York (WABC)".to_string())
        );
        let mut missing = fields.clone();
        missing.city.clear();
        assert_eq!(
            crosstalk_core::naming::render_ota("{NETWORK} - {STATE} {CITY} ({CALLSIGN})", &missing),
            None
        );
        assert_eq!(
            crosstalk_core::naming::render_ota("{name} ({CALLSIGN})", &fields),
            None,
            "unknown placeholders refuse too"
        );
    }

    #[test]
    fn premium_tags_reattach_like_build_final_channel_name() {
        // CM's `extract_tags` + `build_final_channel_name`: regional without parens,
        // extras keep parens, qualities keep brackets, ignored tags never re-attach.
        let (regional, extra, quality) = extract_reattach_tags("ESPN (East) [HD]");
        assert_eq!(regional.as_deref(), Some("East"));
        assert!(is_ignored_tag("HD"), "quality is ignored");
        assert!(
            quality.is_empty(),
            "ignored quality never re-attaches: {quality:?}"
        );
        assert_eq!(
            build_final_channel_name("ESPN", regional.as_deref(), &extra, &quality),
            "ESPN East"
        );
        let (regional, extra, quality) = extract_reattach_tags("CNN (Moldova) [VIP]");
        assert_eq!(regional, None);
        assert_eq!(extra, vec!["(Moldova)".to_string()]);
        assert_eq!(quality, vec!["[VIP]".to_string()], "junk re-attaches");
        assert_eq!(
            build_final_channel_name("CNN", regional.as_deref(), &extra, &quality),
            "CNN (Moldova) [VIP]"
        );
        // Callsign-shaped parens never re-attach.
        let (_, extra, _) = extract_reattach_tags("ABC (WABC) [HD]");
        assert!(extra.is_empty(), "{extra:?}");
    }

    #[test]
    fn reattachment_never_doubles_what_the_base_holds() {
        // The guard in `build_final_channel_name`: a tag already in the base is
        // not re-added, so a canonical name carrying its own tags stays itself.
        assert_eq!(
            build_final_channel_name("KSPT-DT3 (HEROICN)", None, &["(HEROICN)".to_string()], &[]),
            "KSPT-DT3 (HEROICN)"
        );
        assert_eq!(
            build_final_channel_name("truTV (Pacific)", Some("Pacific"), &[], &[]),
            "truTV (Pacific)"
        );
        // The DB base's own ignored tags drop (what clean strips, names never adds).
        assert_eq!(
            build_final_channel_name("BX Culture (HD)", None, &[], &[]),
            "BX Culture"
        );
        // A base stating its feed zone gains no second spelling of it...
        assert_eq!(
            build_final_channel_name("Cinemax Action (Pacific)", Some("West"), &[], &[]),
            "Cinemax Action (Pacific)"
        );
        // ...but a region-looking word that is not a zone still lets one attach.
        assert_eq!(
            build_final_channel_name("Comedy Central", Some("West"), &[], &[]),
            "Comedy Central West"
        );
        // ...while a decoration the base lacks still re-attaches.
        assert_eq!(
            build_final_channel_name(
                "CNN",
                None,
                &["(Moldova)".to_string()],
                &["[VIP]".to_string()]
            ),
            "CNN (Moldova) [VIP]"
        );
    }

    #[test]
    fn unknown_suffix_never_double_appends() {
        // The documented fix over CM's `current_name + suffix`: an already-tagged
        // name is Skipped, never `ESPN [Unk] [Unk]`.
        let settings = test_settings();
        let (tagged, already) =
            crosstalk_core::naming::apply_suffix("ESPN [Unk]", &settings.unknown_suffix);
        assert!((tagged.as_str(), already) == ("ESPN [Unk]", true));
        let (new, status, _, _, _) = unmatched_target("ESPN [Unk]", &settings);
        assert_eq!((new.as_str(), status), ("ESPN [Unk]", "Skipped"));
        let (new, status, _, _, _) = unmatched_target("Zzqz No Such Channel 999", &settings);
        assert_eq!(status, "Renamed");
        assert!(new.ends_with("[Unk]"), "{new:?}");
        // With tagging off, unmatched channels are left alone.
        let mut off = test_settings();
        off.tag_unmatched_channels = false;
        let (new, status, _, _, _) = unmatched_target("Zzqz No Such Channel 999", &off);
        assert_eq!(
            (new.as_str(), status),
            ("Zzqz No Such Channel 999", "Skipped")
        );
    }

    #[test]
    fn heuristic_ota_path_names_a_station_and_blocks_premium() {
        // A callsign present in merged stations takes the OTA path (CM 1979-2005):
        // the template renders, and premium is never tried.
        let (engine, index, db_names) = test_engine();
        let channel = scoped(1, "ABC (WABC)");
        let identity = resolve(&engine, &channel.name);
        assert_eq!(identity.callsign.as_deref(), Some("WABC"));
        let (row, _) = decide_heuristic(
            &channel,
            &identity,
            &engine,
            &index,
            &db_names,
            &test_settings(),
        );
        assert_eq!(row.matcher, "Broadcast (OTA)");
        assert_eq!(row.new, "ABC - NY New York (WABC)");
        assert_eq!(row.status, "Renamed");
        // The same station via a different decoration lands on the same name.
        let channel = scoped(2, "WABC-TV");
        let identity = resolve(&engine, &channel.name);
        let (row2, _) = decide_heuristic(
            &channel,
            &identity,
            &engine,
            &index,
            &db_names,
            &test_settings(),
        );
        assert_eq!(row2.new, row.new);
    }

    #[test]
    fn premium_path_uses_the_db_canonical_plus_tags() {
        // A premium name with provider decoration resolves to the DB canonical with
        // its own tags re-attached — never the decoration as the base.
        // (`ESPN HD` is itself a DB entry, so the decorated form that renames is
        // one the DB does not carry verbatim, e.g. `ESPN FHD`.)
        let (engine, index, db_names) = test_engine();
        assert!(
            db_names.iter().any(|n| n == "ESPN"),
            "ESPN is a DB canonical"
        );
        assert!(
            !db_names.iter().any(|n| n == "ESPN FHD"),
            "FHD form is decoration"
        );
        let channel = scoped(1, "ESPN FHD");
        let identity = resolve(&engine, &channel.name);
        assert!(identity.callsign.is_none());
        let (row, _) = decide_heuristic(
            &channel,
            &identity,
            &engine,
            &index,
            &db_names,
            &test_settings(),
        );
        assert_eq!(row.matcher, "Premium/Cable");
        // Ignored quality (`FHD`) never re-attaches; the base is the DB canonical.
        assert_eq!(row.new, "ESPN");
        // A kept tag (junk) re-attaches onto the canonical.
        let (regional, extra, quality) = extract_reattach_tags("ESPN (Moldova) [VIP]");
        let rebuilt = build_final_channel_name("ESPN", regional.as_deref(), &extra, &quality);
        assert_eq!(rebuilt, "ESPN (Moldova) [VIP]");
    }

    #[test]
    fn renames_keep_the_identity_family() {
        // The invariant: re-resolving the new name lands in the same family.
        //
        // Documented OTA exception: the `{NETWORK} - {STATE} {CITY} ({CALLSIGN})`
        // template changes the normalize KEY (`13 WREX` keys as `13 wrex`, the
        // template as `nbc il rockford`), so `same_family` — which compares keys —
        // does not hold. The callsign IS kept (`WREX` -> `WREX`), and callsign
        // identity ignores country (bug-161), so the broadcast entity is the same
        // station under a different display shape. Premium renames always hold the
        // invariant: the new name re-attaches tags onto the family's DB canonical.
        let (engine, index, db_names) = test_engine();
        let mut checked = 0;
        let mut premium_checked = 0;
        let mut ota_documented = Vec::new();
        let corpus: Vec<String> = {
            let mut names: Vec<String> = db_names.iter().take(60).cloned().collect();
            names.push("ABC (WABC)".to_string());
            names.push("NBC (KSDK)".to_string());
            names.push("ESPN FHD".to_string());
            names.push("Zzqz No Such Channel 999".to_string());
            names
        };
        for name in &corpus {
            let channel = scoped(1, name);
            let identity = resolve(&engine, name);
            let (row, _) = decide_heuristic(
                &channel,
                &identity,
                &engine,
                &index,
                &db_names,
                &test_settings(),
            );
            if row.status != "Renamed" {
                continue;
            }
            checked += 1;
            let after = resolve(&engine, &row.new);
            if row.matcher == "Broadcast (OTA)" {
                assert_eq!(
                    after.callsign, identity.callsign,
                    "OTA keeps the callsign: {name:?} -> {:?}",
                    row.new
                );
                ota_documented.push(format!("{name:?} -> {:?}", row.new));
                continue;
            }
            premium_checked += 1;
            assert!(
                after.same_family(&identity),
                "{name:?} -> {:?}: {identity:?} vs {after:?}",
                row.new
            );
        }
        assert!(checked > 5, "swept {checked} renames");
        assert!(premium_checked > 0, "premium renames swept");
        assert!(!ota_documented.is_empty(), "OTA template case documented");
    }

    #[test]
    fn collisions_resolve_globally_for_names() {
        // Two channels renaming onto one canonical collide globally (step 7 review):
        // `suffix` keeps both, `numeric` numbers, `skip` leaves both alone.
        let world = World {
            channels: vec![
                Channel {
                    id: 1,
                    name: "ESPN HD".to_string(),
                    channel_number: Some(serde_json::json!(1.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 2,
                    name: "ESPN [HD]".to_string(),
                    channel_number: Some(serde_json::json!(2.0)),
                    channel_group_id: Some(20),
                    ..Channel::default()
                },
            ],
            groups: vec![
                ChannelGroup {
                    id: 10,
                    name: "A".to_string(),
                },
                ChannelGroup {
                    id: 20,
                    name: "B".to_string(),
                },
            ],
            ..World::default()
        };
        let rows = vec![
            NameRow {
                id: 1,
                group: "A".to_string(),
                number: Some(1.0),
                old: "ESPN HD".to_string(),
                new: "ESPN".to_string(),
                status: "Renamed",
                matcher: "Premium/Cable".to_string(),
                method: "Exact".to_string(),
                reason: "test".to_string(),
                identity: resolve(&UnifiedEngine::decided(), "ESPN HD"),
            },
            NameRow {
                id: 2,
                group: "B".to_string(),
                number: Some(2.0),
                old: "ESPN [HD]".to_string(),
                new: "ESPN".to_string(),
                status: "Renamed",
                matcher: "Premium/Cable".to_string(),
                method: "Exact".to_string(),
                reason: "test".to_string(),
                identity: resolve(&UnifiedEngine::decided(), "ESPN [HD]"),
            },
        ];
        for strategy in ["suffix", "numeric", "skip"] {
            let mut tuned = test_settings();
            tuned.collision_strategy = strategy.to_string();
            let plan = plan_names(&world, &Selection::default(), &rows, &tuned).expect("plans");
            let outputs = final_outputs(&rows, &plan);
            if strategy == "skip" {
                assert!(
                    outputs.iter().all(|o| o.status == "Skipped"),
                    "{strategy}: {outputs:?}"
                );
            } else {
                let renamed = outputs.iter().filter(|o| o.status == "Renamed").count();
                assert_eq!(renamed, 2, "{strategy}: {outputs:?}");
                // No two channels share a final name.
                let mut seen = HashSet::new();
                for output in &outputs {
                    if output.status == "Renamed" {
                        assert!(seen.insert(output.new.to_lowercase()), "{outputs:?}");
                    }
                }
            }
        }
    }

    /// A decider scripted for gate-shaped requests: picks the first offered candidate.
    struct GateDecider {
        probability: f64,
        confidence: f64,
    }

    #[async_trait::async_trait]
    impl crosstalk_core::ai::Decider for GateDecider {
        fn provider(&self) -> &str {
            "typesafe"
        }

        fn calibrated(&self) -> bool {
            true
        }

        async fn decide(
            &self,
            request: &crosstalk_core::ai::DecisionRequest,
        ) -> anyhow::Result<crosstalk_core::ai::DecisionResponse> {
            use std::collections::BTreeMap;
            let mut answers = BTreeMap::new();
            if let Some((id, _)) = request
                .questions
                .iter()
                .find(|(id, _)| id.starts_with("same_") && id.as_str() != "same_network")
            {
                answers.insert(
                    "best".to_string(),
                    crosstalk_core::ai::Answer::Choice {
                        choice: id.replacen("same_", "candidate_", 1),
                        confidence: self.confidence,
                        probabilities: BTreeMap::new(),
                    },
                );
                answers.insert(
                    id.clone(),
                    crosstalk_core::ai::Answer::Noul {
                        noul: self.probability,
                    },
                );
            }
            Ok(crosstalk_core::ai::DecisionResponse {
                model: "stub".to_string(),
                answers,
                input_tokens: 10,
                output_tokens: 2,
                latency_ms: 1,
                calibrated: true,
            })
        }
    }

    fn scored(score: u8, index: usize) -> Scored {
        Scored {
            index,
            score,
            components: vec!["Fuzzy"],
        }
    }

    fn verify_settings() -> crosstalk_core::config::AiVerifyConfig {
        crosstalk_core::config::AiVerifyConfig {
            enabled: true,
            ..crosstalk_core::config::AiVerifyConfig::default()
        }
    }

    #[test]
    fn ai_shortlist_offers_only_the_verify_band_and_rescue() {
        // At the defaults (band 80-94, rescue floor 60): 90 verifies, 70 rescues,
        // 40 is not sent. Duplicates offer once.
        let candidates = vec![
            DbCandidate {
                id: 0,
                name: "ESPN".to_string(),
            },
            DbCandidate {
                id: 1,
                name: "ESPN 2".to_string(),
            },
        ];
        let settings = verify_settings();
        let (reason, offered) =
            ai_gate::shortlist(&[scored(90, 0), scored(85, 1)], &candidates, &settings)
                .expect("verifies");
        assert_eq!(reason, ai_gate::GateReason::Verify);
        assert_eq!(offered, vec![0, 1]);
        let (reason, offered) =
            ai_gate::shortlist(&[scored(70, 0)], &candidates, &settings).expect("rescues");
        assert_eq!(reason, ai_gate::GateReason::Rescue);
        assert_eq!(offered, vec![0]);
        assert!(ai_gate::shortlist(&[scored(40, 0)], &candidates, &settings).is_none());
        // A confident 95 never reaches the model.
        assert!(ai_gate::shortlist(&[scored(95, 0)], &candidates, &settings).is_none());
    }

    #[tokio::test]
    async fn ai_accept_renames_and_reject_keeps_the_unmatched_row() {
        // The gate at the defaults: an agreeing model promotes (accept), a
        // disagreeing one leaves the unmatched tagging in place.
        let candidates = vec![DbCandidate {
            id: 0,
            name: "ESPN".to_string(),
        }];
        let ranked = vec![scored(85, 0)];
        let settings = verify_settings();
        let (_, offered) = ai_gate::shortlist(&ranked, &candidates, &settings).expect("offered");
        let query = NameQuery {
            name: "Espn ch".to_string(),
            number: None,
        };
        let known = crosstalk_matching::engine::merged_known_callsigns();

        let accept = crosstalk_core::ai::AiService::with_decider(
            "typesafe",
            Arc::new(GateDecider {
                probability: 0.93,
                confidence: 0.9,
            }),
        );
        let outcome = ai_gate::ask(
            Some(&accept),
            &query,
            &candidates,
            &ranked,
            &offered,
            ai_gate::GateReason::Verify,
            &known,
            &settings,
            || false,
            Some("names"),
        )
        .await;
        assert_eq!(outcome.accepted, Some(0));
        assert!(outcome.decision.contains("accept"), "{}", outcome.decision);

        let reject = crosstalk_core::ai::AiService::with_decider(
            "typesafe",
            Arc::new(GateDecider {
                probability: 0.2,
                confidence: 0.9,
            }),
        );
        let outcome = ai_gate::ask(
            Some(&reject),
            &query,
            &candidates,
            &ranked,
            &offered,
            ai_gate::GateReason::Verify,
            &known,
            &settings,
            || false,
            Some("names"),
        )
        .await;
        assert_eq!(outcome.accepted, None);
    }

    async fn serve(world: &World) -> (MockServer, CrosstalkConfig, tempfile::TempDir) {
        let server = MockServer::start().await;
        let list = |items: serde_json::Value| ResponseTemplate::new(200).set_body_json(items);
        for (route, items) in [
            (
                "/api/channels/channels/",
                serde_json::to_value(&world.channels).expect("channels"),
            ),
            (
                "/api/channels/groups/",
                serde_json::to_value(&world.groups).expect("groups"),
            ),
            (
                "/api/channels/profiles/",
                serde_json::to_value(&world.profiles).expect("profiles"),
            ),
            (
                "/api/epg/sources/",
                serde_json::to_value(&world.sources).expect("sources"),
            ),
            (
                "/api/epg/epgdata/",
                serde_json::to_value(&world.entries).expect("entries"),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(list(items))
                .mount(&server)
                .await;
        }
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"message": "ok", "channels": []})),
            )
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        (server, config, dir)
    }

    fn context_for(
        config: &CrosstalkConfig,
        store: &Store,
        app: &str,
        action: &str,
        dry_run: bool,
    ) -> (JobContext, i64) {
        let run_id = store.start_run(app, action, dry_run).expect("start run");
        let context = JobContext::new(Arc::new(config.clone()), Arc::new(VecSink::new()))
            .with_dispatcharr(
                crosstalk_core::dispatcharr::DispatcharrClient::new(&config.dispatcharr)
                    .expect("client"),
            )
            .for_run(
                app,
                action,
                run_id,
                store.clone(),
                Arc::new(AtomicBool::new(false)),
            );
        (context, run_id)
    }

    fn names_world() -> World {
        World {
            channels: vec![
                Channel {
                    id: 1,
                    name: "ABC (WABC)".to_string(),
                    channel_number: Some(serde_json::json!(1.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 2,
                    // `ESPN HD` is itself a merged-DB entry (already correct); the
                    // decorated form that renames is one the DB lacks verbatim.
                    name: "ESPN FHD".to_string(),
                    channel_number: Some(serde_json::json!(2.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 3,
                    name: "Zzqz No Such Channel 999".to_string(),
                    channel_number: Some(serde_json::json!(3.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
            ],
            groups: vec![ChannelGroup {
                id: 10,
                name: "Test".to_string(),
            }],
            ..World::default()
        }
    }

    #[tokio::test]
    async fn preview_writes_nothing_but_the_cm_csv() {
        let world = names_world();
        let (server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, _) = context_for(&config, &store, "lineup", "names_preview", true);
        let outcome = run(&app, &context, &test_settings(), true)
            .await
            .expect("previews");
        assert!(
            outcome.summary.contains("Names preview"),
            "{}",
            outcome.summary
        );
        // No write: the bulk endpoint never saw a request.
        let writes: Vec<_> = server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| {
                request.method.as_str() == "PATCH"
                    && request.url.path() == "/api/channels/channels/edit/bulk/"
            })
            .collect();
        assert!(writes.is_empty(), "a preview writes nothing");
        // The CSV carries CM's rename columns plus the identity fields.
        let path = outcome.report_path.expect("a report");
        let text = std::fs::read_to_string(&path).expect("readable");
        let mut lines = text.lines().filter(|line| !line.starts_with('#'));
        let header = lines.next().expect("header");
        assert_eq!(
            header,
            "id,channel_number,group,status,old_name,new_name,matcher,method,reason,\
             callsign,network,country,zone,canonical"
        );
        let rows: Vec<&str> = lines.collect();
        assert_eq!(rows.len(), 3, "{rows:?}");
        let ota = rows
            .iter()
            .find(|line| line.starts_with("1,"))
            .expect("ch 1");
        assert!(ota.contains("Renamed"), "{ota:?}");
        assert!(ota.contains("Broadcast (OTA)"), "{ota:?}");
        assert!(ota.contains("WABC"), "{ota:?}");
        let unknown = rows
            .iter()
            .find(|line| line.starts_with("3,"))
            .expect("ch 3");
        assert!(unknown.contains("[Unk]"), "{unknown:?}");
    }

    #[tokio::test]
    async fn apply_bulks_the_renames_and_records_name_rows() {
        let world = names_world();
        let (server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, run_id) = context_for(&config, &store, "lineup", "names_apply", false);
        let outcome = run(&app, &context, &test_settings(), false)
            .await
            .expect("applies");
        assert!(
            outcome.summary.contains("Names apply"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("written"), Some(&3));
        let bodies: Vec<serde_json::Value> = server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| {
                request.method.as_str() == "PATCH"
                    && request.url.path() == "/api/channels/channels/edit/bulk/"
            })
            .map(|request| serde_json::from_slice(&request.body).expect("JSON body"))
            .collect();
        assert_eq!(bodies.len(), 1, "{bodies:?}");
        let rows = store.rows(run_id).expect("rows");
        let names: Vec<_> = rows
            .iter()
            .filter(|row| row.field == "name" && row.status == "ok")
            .collect();
        assert_eq!(names.len(), 3, "{rows:?}");
    }

    #[tokio::test]
    async fn undo_round_trips_a_names_apply_through_host_undo() {
        let world = names_world();
        let (_server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, run_id) = context_for(&config, &store, "lineup", "names_apply", false);
        run(&app, &context, &test_settings(), false)
            .await
            .expect("applies");
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("applied"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        // The applied world: every row's `after` is the current name.
        let plan = plan_names(
            &world,
            &Selection::default(),
            &{
                let (engine, index, db_names) = test_engine();
                names_world()
                    .channels
                    .iter()
                    .map(|channel| {
                        let identity = resolve(&engine, &channel.name);
                        let ch = scoped(channel.id, &channel.name);
                        let (row, _) = decide_heuristic(
                            &ch,
                            &identity,
                            &engine,
                            &index,
                            &db_names,
                            &test_settings(),
                        );
                        row
                    })
                    .collect::<Vec<_>>()
            },
            &test_settings(),
        )
        .expect("plans");
        let mut applied = world.clone();
        let outputs = final_outputs(
            &names_world()
                .channels
                .iter()
                .map(|channel| {
                    let (engine, index, db_names) = test_engine();
                    let identity = resolve(&engine, &channel.name);
                    let ch = scoped(channel.id, &channel.name);
                    let (row, _) = decide_heuristic(
                        &ch,
                        &identity,
                        &engine,
                        &index,
                        &db_names,
                        &test_settings(),
                    );
                    row
                })
                .collect::<Vec<_>>(),
            &plan,
        );
        for channel in &mut applied.channels {
            if let Some(output) = outputs.iter().find(|o| o.id == channel.id) {
                channel.name = output.new.clone();
            }
        }
        let registry =
            crosstalk_core::subapp::Registry::new(vec![Arc::new(crate::LineupApp::new())]);
        let undo =
            crosstalk_core::undo::plan_undo(&store, &applied, run_id, &registry).expect("plans");
        assert!(undo.skipped.is_empty(), "{:?}", undo.skipped);
        assert_eq!(undo.changes.len(), 3, "{undo:?}");
    }

    #[test]
    fn market_fallback_never_turns_a_blocked_callsign_premium() {
        // CM's `match_broadcast_channel`: a printed callsign with no station tries
        // the market, and when that misses returns `(callsign, None)` — premium is
        // never tried. Renaming `ABC (WZZZ)` to the national `ABC` entry would drop
        // the station for the network.
        let (engine, index, db_names) = test_engine();
        let channel = scoped(1, "ABC (WZZZ)");
        let identity = resolve(&engine, &channel.name);
        assert_eq!(identity.callsign.as_deref(), Some("WZZZ"));
        for fallback in [false, true] {
            let mut settings = test_settings();
            settings.ota_market_fallback = fallback;
            let (row, borderline) =
                decide_heuristic(&channel, &identity, &engine, &index, &db_names, &settings);
            assert_ne!(row.matcher, "Premium/Cable", "fallback={fallback}: {row:?}");
            assert_eq!(row.new, "ABC (WZZZ) [Unk]", "fallback={fallback}: {row:?}");
            assert!(!borderline, "a blocked callsign never reaches the AI gate");
        }
    }

    #[test]
    fn ota_renames_keep_the_guide_and_streams_families() {
        // Cross-app consistency: after `names_apply` an OTA channel carries the
        // template name, and guide (EPG family, `match_run::score_families` ->
        // `rank_index`) and streams (stream family, `rank_index_in` with no country
        // hint for a callsign channel) must find the station under the new name —
        // the callsign identity path, not the display key.
        let (engine, index, db_names) = test_engine();
        let epg: Vec<String> = [
            "WABC-TV",
            "WABC",
            "ABC 7 New York (WABC)",
            "WREX",
            "WREX-DT",
            "KSDK",
            "NBC 5 St. Louis KSDK",
            "WFOR-TV",
            "ABC",
            "NBC",
            "CBS",
            "ESPN",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let streams: Vec<String> = [
            "US: ABC 7 (WABC) HD",
            "USA | WABC-TV FHD",
            "US: NBC 13 Rockford (WREX)",
            "WREX-DT [HD]",
            "US: NBC (KSDK) HD",
            "CBS 4 Miami (WFOR)",
            "US: ABC HD",
            "US: NBC",
            "ESPN HD",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let ctx = ResolveContext::default();
        let epg_index = crosstalk_matching::engine::EngineIndex::build(&epg, &engine, &ctx);
        let stream_index = crosstalk_matching::engine::EngineIndex::build(&streams, &engine, &ctx);
        let members = |name: &str, idx: &crosstalk_matching::engine::EngineIndex| {
            let mut found: Vec<usize> = engine
                .rank_index_in(name, None, idx)
                .iter()
                .map(|member| member.index)
                .collect();
            found.sort_unstable();
            found
        };
        // Decided C07 `callsign-first` (orchestrator, follow-up engine fix): a
        // network-led station name (`ABC (WABC)`) ranks the station family both
        // BEFORE and AFTER the rename — the same-callsign candidate outranks the
        // bare national entry's stripped-key Exact 100, in every app. Callsign-led
        // names keep the station as before. Pinned so any engine change to that
        // precedence flips this test on purpose.
        let station_only = |members: &[usize], names: &[String], callsign: &str| {
            !members.is_empty()
                && members.iter().all(|&i| {
                    engine.callsign_of(&names[i]).is_some_and(|c| {
                        crosstalk_matching::callsign::normalize_callsign(&c) == callsign
                    })
                })
        };
        let mut swept = 0;
        for (name, _callsign_led) in [
            ("WABC-TV", true),
            ("13 WREX", true),
            ("ABC (WABC)", false),
            ("US: ABC (WABC) HD", false),
            ("NBC (KSDK)", false),
            ("CBS (WFOR) [HD]", false),
        ] {
            let identity = resolve(&engine, name);
            let callsign = identity.callsign.clone().expect("a callsign channel");
            let (row, _) = decide_heuristic(
                &scoped(1, name),
                &identity,
                &engine,
                &index,
                &db_names,
                &test_settings(),
            );
            assert_eq!(row.matcher, "Broadcast (OTA)", "{name:?}: {row:?}");
            let after = resolve(&engine, &row.new);
            assert_eq!(
                after.callsign, identity.callsign,
                "{name:?} -> {:?}",
                row.new
            );
            for (side, idx, pool) in [
                ("guide", &epg_index, &epg),
                ("streams", &stream_index, &streams),
            ] {
                let before = members(name, idx);
                let renamed = members(&row.new, idx);
                assert!(
                    station_only(&renamed, pool, &callsign),
                    "{side}: {:?} must rank onto {callsign}: {:?}",
                    row.new,
                    renamed.iter().map(|&i| &pool[i]).collect::<Vec<_>>()
                );
                // Decided C07: the station family wins before the rename too —
                // callsign-led or network-led, the pool decides, not the shape.
                // The top member may differ (`WABC-TV` Exact before,
                // `ABC 7 New York (WABC)` by Callsign after).
                assert!(
                    station_only(&before, pool, &callsign),
                    "{side}: {name:?} ranked {:?} before the rename",
                    before.iter().map(|&i| &pool[i]).collect::<Vec<_>>()
                );
            }
            swept += 1;
        }
        assert_eq!(swept, 6);
        // Fallback: with no same-callsign candidate in the pool the bare national
        // entry still wins (today's behaviour for pools lacking the station).
        let national_epg: Vec<String> = ["ABC", "NBC", "CBS"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let national_pool =
            crosstalk_matching::engine::EngineIndex::build(&national_epg, &engine, &ctx);
        for (name, national) in [
            ("ABC (WABC)", "ABC"),
            ("US: ABC (WABC) HD", "ABC"),
            ("NBC (KSDK)", "NBC"),
            ("CBS (WFOR) [HD]", "CBS"),
        ] {
            let family = engine.rank_index_in(name, None, &national_pool);
            assert_eq!(family.len(), 1, "{name}: {family:?}");
            assert_eq!(&national_epg[family[0].index], national);
        }
        // A different known station never joins a station query's family.
        let rival_pool: Vec<String> = ["KSDK", "NBC", "ABC"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let rival_index =
            crosstalk_matching::engine::EngineIndex::build(&rival_pool, &engine, &ctx);
        let family = engine.rank_index_in("ABC (WABC)", None, &rival_index);
        let names: Vec<&str> = family
            .iter()
            .map(|member| rival_pool[member.index].as_str())
            .collect();
        assert!(
            names.iter().all(|name| !name.contains("KSDK")),
            "no KSDK member: {names:?}"
        );
    }

    #[test]
    fn names_and_clean_do_not_fight() {
        // The two writers over one name: clean strips junk, names re-attaches CM's
        // tags. Whatever order they run in, one round lands on a name BOTH leave
        // alone (no churn when a schedule alternates them), and names is idempotent.
        // Names never re-adds what clean removed: on a cleaned input the tag is gone,
        // so there is nothing to re-attach (`US: K33MI-D3 (ANTENNA) (Backup)` keeps
        // `(Backup)` under names alone, like CM, and loses it for good under clean).
        //
        // The order is NOT irrelevant, and that is documented, not a fight:
        // `ESPN [HD]` names to `ESPN HD` (that query's family leader, a DB entry
        // whose bare `HD` is part of the name), while clean first gives `ESPN`,
        // itself a DB entry names leaves alone. Both are stable endpoints in the
        // same family.
        let (engine, index, db_names) = test_engine();
        let settings = test_settings();
        let rules = crate::clean::CleanRules::from_settings(&settings);
        let custom = crate::clean::parse_custom_rules(&settings.custom_rules);
        let clean = |name: &str| crate::clean::clean_name(name, &rules, &custom).0;
        let name_of = |name: &str| {
            let identity = resolve(&engine, name);
            decide_heuristic(
                &scoped(1, name),
                &identity,
                &engine,
                &index,
                &db_names,
                &settings,
            )
            .0
            .new
        };
        let mut corpus: Vec<String> = [
            "ESPN FHD",
            "ESPN [HD]",
            "US: ESPN HD",
            "CNN (Backup)",
            "HBO West",
            "HBO (East) [HD]",
            "Cinemax Action West",
            "ABC (WABC)",
            "US: NBC (KSDK) HD",
            "KIEM-DT (NBC)",
            "Comedy Central (Extra)",
            "truTV (Pacific)",
            "CNN (Moldova) [VIP]",
            "ESPN | HD |",
            "Zzqz No Such Channel 999",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        for name in db_names.iter().step_by(397).take(40) {
            corpus.push(name.clone());
            corpus.push(format!("{name} [HD]"));
            corpus.push(format!("US: {name} (Backup)"));
        }
        for name in &corpus {
            let named = name_of(name);
            assert_eq!(name_of(&named), named, "names is idempotent for {name:?}");
            for (order, end) in [
                ("clean after names", clean(&named)),
                ("names after clean", name_of(&clean(name))),
            ] {
                assert_eq!(
                    name_of(&end),
                    end,
                    "{order}: names moves {name:?} -> {end:?}"
                );
                assert_eq!(clean(&end), end, "{order}: clean moves {name:?} -> {end:?}");
            }
        }
        assert_eq!(name_of("ESPN [HD]"), "ESPN HD");
        assert_eq!(name_of(&clean("ESPN [HD]")), "ESPN");
        // The fight this pins: the DB entry `BX Culture (HD)` once named `BX Culture`
        // back to `BX Culture (HD)`, which clean then stripped again.
        assert_eq!(name_of("BX Culture"), "BX Culture");
    }

    /// A gate decider that accepts the first offered candidate and logs each ask.
    struct LoggingDecider {
        asked: std::sync::Mutex<Vec<(Option<String>, String)>>,
    }

    #[async_trait::async_trait]
    impl crosstalk_core::ai::Decider for LoggingDecider {
        fn provider(&self) -> &str {
            "typesafe"
        }

        fn calibrated(&self) -> bool {
            true
        }

        async fn decide(
            &self,
            request: &crosstalk_core::ai::DecisionRequest,
        ) -> anyhow::Result<crosstalk_core::ai::DecisionResponse> {
            self.asked
                .lock()
                .expect("lock")
                .push((request.task.clone(), request.state.to_string()));
            GateDecider {
                probability: 0.95,
                confidence: 0.95,
            }
            .decide(request)
            .await
        }
    }

    #[tokio::test]
    async fn ai_gate_runs_on_tagged_unmatched_rows_and_only_there() {
        // At the defaults unmatched rows are TAGGED (`[Unk]`), and those are exactly
        // the borderline rows the gate exists for. An already-correct premium name
        // and a blocked callsign never reach the model; the accept renames (task
        // "names"), and every ask is audited.
        let world = World {
            channels: vec![
                Channel {
                    id: 1,
                    name: "ESPN Two Extra".to_string(),
                    channel_number: Some(serde_json::json!(1.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 2,
                    name: "ESPN".to_string(),
                    channel_number: Some(serde_json::json!(2.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 3,
                    name: "ABC (WZZZ)".to_string(),
                    channel_number: Some(serde_json::json!(3.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
            ],
            groups: vec![ChannelGroup {
                id: 10,
                name: "Test".to_string(),
            }],
            ..World::default()
        };
        let (_server, mut config, _dir) = serve(&world).await;
        config.ai.verify.enabled = true;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, run_id) = context_for(&config, &store, "lineup", "names_preview", true);
        let mut settings = test_settings();
        settings.ai_gate_enabled = true;
        assert!(
            settings.tag_unmatched_channels,
            "the default tags unmatched rows"
        );
        let decider = Arc::new(LoggingDecider {
            asked: std::sync::Mutex::new(Vec::new()),
        });
        let service = crosstalk_core::ai::AiService::with_decider("typesafe", decider.clone());
        let outcome = run_with_service(&app, &context, &settings, true, Some(&service))
            .await
            .expect("previews");
        let asked = decider.asked.lock().expect("lock").clone();
        assert_eq!(
            asked.len(),
            1,
            "only the borderline row is asked: {asked:?}"
        );
        assert_eq!(asked[0].0.as_deref(), Some("names"));
        assert!(asked[0].1.contains("ESPN Two Extra"), "{asked:?}");
        assert_eq!(store.ai_decision_count(run_id).expect("count"), 1);
        let text =
            std::fs::read_to_string(outcome.report_path.expect("a report")).expect("readable");
        let row = text
            .lines()
            .find(|line| line.starts_with("1,"))
            .expect("ch 1");
        assert!(row.contains(",ESPN Extra,"), "the accept renames: {row:?}");
        assert!(
            !row.contains("[Unk]"),
            "an accepted row is not tagged: {row:?}"
        );
    }
}
