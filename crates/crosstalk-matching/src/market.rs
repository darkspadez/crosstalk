//! Market fallback for OTA naming: resolve a market reference to a station.
//!
//! Port of `reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/market_index.py`
//! (CM's `ota_market_fallback`). The FCC station table is keyed on callsign, so a
//! provider name that prints only a market — `US: ABC 9 HD [SYRACUSE]` — matches
//! nothing. This module reads that market reference (the channel number after the
//! network the name itself claims) and resolves it against the merged station table
//! (`data/merged/stations.json`), in the same two stages CM uses:
//!
//! 1. the market city is the community the station is licensed to (narrowed by state
//!    and channel number when the name gives them);
//! 2. the market city is served from a neighbouring community (the city supplies only
//!    the state; a channel number is required).
//!
//! Both stages return a station only when exactly one candidate survives. Shared (not
//! lineup-only): every app names OTA stations the same way.

use crate::data::merged::{merged_stations, MergedStation};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

/// A market reference parsed from a channel name: the provider's market city, an
/// optional trailing state, and the channel number after the stated network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketReference {
    pub number: Option<String>,
    pub city: String,
    pub state: Option<String>,
}

/// The market index: active stations by licensed city, plus every city -> states.
#[derive(Debug, Clone, Default)]
pub struct MarketIndex {
    pub by_city: HashMap<String, Vec<MergedStation>>,
    pub city_states: HashMap<String, HashSet<String>>,
}

/// Words a provider appends to a market that are not part of the city name.
static NOISE_TOKENS: &[&str] = &["HD", "SD", "FHD", "UHD", "4K", "RAW", "NET", "TV", "DT"];

/// Words marking a secondary service (`FOX 9 PLUS` is WFTC, not KMSP): refused.
static SECONDARY_SERVICE_TOKENS: &[&str] = &["PLUS", "XTRA", "EXTRA", "MAS", "MORE"];

/// Words never a place name alone (national feeds, not markets).
static NON_PLACE_TOKENS: &[&str] = &[
    "NEWS",
    "LIVE",
    "NOW",
    "SPORT",
    "SPORTS",
    "CHANNEL",
    "NETWORK",
    "EAST",
    "WEST",
    "INTERNATIONAL",
    "BUSINESS",
    "WEATHER",
    "SOCCER",
    "DEPORTES",
    "MOVIES",
    "KIDS",
    "MUSIC",
    "KIDZ",
];

/// Market names with no licensed community of their own (CM's `MARKET_ALIASES`).
static MARKET_ALIASES: &[(&str, &str, &str)] = &[
    ("HAMPTON ROADS", "HAMPTON", "VA"),
    ("CHARLES", "LAKE CHARLES", "LA"),
    ("FORT MYERS", "NAPLES", "FL"),
];

fn known_states() -> &'static HashSet<String> {
    static STATES: OnceLock<HashSet<String>> = OnceLock::new();
    STATES.get_or_init(|| {
        merged_stations()
            .stations
            .values()
            .map(|s| {
                s.community_served_state
                    .as_deref()
                    .unwrap_or("")
                    .to_uppercase()
            })
            .filter(|s| !s.is_empty())
            .collect()
    })
}

fn strip_noise(text: &str) -> String {
    text.split_whitespace()
        .filter(|t| !NOISE_TOKENS.contains(&t.to_uppercase().as_str()))
        .collect::<Vec<_>>()
        .join(" ")
}

fn place_characters_ok(city: &str) -> bool {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = RE.get_or_init(|| regex::Regex::new(r"^[A-Z0-9 .'\-/]+$").expect("static regex"));
    re.is_match(city)
}

/// Parse the market reference a name states (`market_index.parse_market_reference`).
///
/// `network` is the network the name itself claims (CM's `_extract_stream_network`).
/// Returns `None` when the name carries a secondary-service token, too little city,
/// illegal characters, or nothing but non-place words.
pub fn parse_market_reference(stream_name: &str, network: &str) -> Option<MarketReference> {
    if stream_name.trim().is_empty() || network.trim().is_empty() {
        return None;
    }
    let stripped = stream_name.trim();
    // Leading geo/provider prefix (`US:`, `CITY:`, `[US] |`), up to 12 letters.
    static PREFIX: OnceLock<regex::Regex> = OnceLock::new();
    let prefix = PREFIX.get_or_init(|| {
        regex::Regex::new(r#"^\s*[\[(]?[A-Za-z]{2,12}[\])]?\s*[:|]\s*"#).expect("static regex")
    });
    let text = prefix.replace(stripped, "").to_string();

    static SPLIT_WORDS: OnceLock<regex::Regex> = OnceLock::new();
    let splitter =
        SPLIT_WORDS.get_or_init(|| regex::Regex::new(r"[\s\[\]()]+").expect("static regex"));
    if splitter
        .split(&text)
        .any(|t| SECONDARY_SERVICE_TOKENS.contains(&t.to_uppercase().as_str()))
    {
        return None;
    }

    // Channel number: the token after the network.
    let mut number: Option<String> = None;
    let escaped = regex::escape(network);
    let num_re =
        regex::Regex::new(&format!(r"(?i)^{escaped}\s+(\d{{1,3}})(?:/\d{{1,3}})?\b")).ok()?;
    if let Some(caps) = num_re.captures(&text) {
        number = caps.get(1).map(|m| m.as_str().to_string());
    }

    // Bracketed market city.
    static BRACKET: OnceLock<regex::Regex> = OnceLock::new();
    let bracket =
        BRACKET.get_or_init(|| regex::Regex::new(r"[\[(]([^\])]+)[\])]").expect("static regex"));
    let mut city: String;
    if let Some(caps) = bracket.captures(&text) {
        let mut inner = caps
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        if inner.trim().len() == 2 && known_states().contains(&inner.trim().to_uppercase()) {
            let before = &text[..caps.get(0).map(|m| m.start()).unwrap_or(0)];
            inner = format!("{before} {inner}");
        }
        city = inner;
    } else {
        city = text.clone();
    }
    let strip_re = regex::Regex::new(&format!(
        r"(?i)^{escaped}\s*(?:\d{{1,3}}(?:/\d{{1,3}})?)?\s*"
    ))
    .ok()?;
    city = strip_re.replace(&city, "").to_string();
    city = strip_noise(&city).to_uppercase();
    city = city
        .trim()
        .trim_matches(|c| c == ' ' || c == '.' || c == ',' || c == '-')
        .to_string();

    // Trailing state code, split linearly (never a backtracking regex on provider input).
    let mut state: Option<String> = None;
    let tokens: Vec<&str> = city.split([' ', ',']).filter(|t| !t.is_empty()).collect();
    if tokens.len() > 1 && known_states().contains(tokens[tokens.len() - 1]) {
        state = Some(tokens[tokens.len() - 1].to_string());
        city = tokens[..tokens.len() - 1].join(" ");
        city = city
            .trim()
            .trim_matches(|c| c == ' ' || c == '.' || c == ',' || c == '-')
            .to_string();
    }

    if city.len() < 3 || !place_characters_ok(&city) {
        return None;
    }
    if city
        .split_whitespace()
        .all(|t| NON_PLACE_TOKENS.contains(&t))
    {
        return None;
    }
    Some(MarketReference {
        number,
        city,
        state,
    })
}

/// Build the market index from the merged station table (CM's `build_market_index`).
pub fn build_market_index() -> MarketIndex {
    let mut by_city: HashMap<String, Vec<MergedStation>> = HashMap::new();
    let mut city_states: HashMap<String, HashSet<String>> = HashMap::new();
    for station in merged_stations().stations.values() {
        let city = station
            .community_served_city
            .as_deref()
            .unwrap_or("")
            .to_uppercase();
        let city = city.trim().to_string();
        let state = station
            .community_served_state
            .as_deref()
            .unwrap_or("")
            .to_uppercase();
        let state = state.trim().to_string();
        if city.is_empty() {
            continue;
        }
        city_states
            .entry(city.clone())
            .or_default()
            .insert(state.clone());
        if station.active_ind.as_deref().unwrap_or("").to_uppercase() == "Y" {
            by_city.entry(city).or_default().push(station.clone());
        }
    }
    MarketIndex {
        by_city,
        city_states,
    }
}

fn cached_index() -> &'static MarketIndex {
    static INDEX: OnceLock<MarketIndex> = OnceLock::new();
    INDEX.get_or_init(build_market_index)
}

fn serves_network(station: &MergedStation, network: &str) -> bool {
    station
        .network_affiliation
        .as_deref()
        .unwrap_or("")
        .to_uppercase()
        .contains(&network.to_uppercase())
}

fn virtual_channel(station: &MergedStation) -> String {
    station
        .tv_virtual_channel
        .clone()
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// Resolve a market reference to the single station it names, or `None` (CM's
/// `resolve_station` / `_resolve_market_station`).
pub fn resolve_market_station(
    reference: &MarketReference,
    network: &str,
    index: &MarketIndex,
) -> Option<MergedStation> {
    let mut city = reference.city.clone();
    let mut state = reference.state.clone();
    if let Some((_, alias_city, alias_state)) =
        MARKET_ALIASES.iter().find(|(alias, _, _)| *alias == city)
    {
        city = alias_city.to_string();
        if state.is_none() {
            state = Some(alias_state.to_string());
        }
    }

    let in_city: Vec<MergedStation> = index
        .by_city
        .get(&city)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|s| serves_network(s, network))
        .collect();
    let mut candidates = in_city.clone();
    if let Some(state) = &state {
        let narrowed: Vec<MergedStation> = candidates
            .iter()
            .filter(|s| {
                s.community_served_state
                    .as_deref()
                    .unwrap_or("")
                    .to_uppercase()
                    == *state
            })
            .cloned()
            .collect();
        if !narrowed.is_empty() {
            candidates = narrowed;
        }
    }
    if let Some(number) = &reference.number {
        let narrowed: Vec<MergedStation> = candidates
            .iter()
            .filter(|s| virtual_channel(s) == *number)
            .cloned()
            .collect();
        if !narrowed.is_empty() {
            candidates = narrowed;
        }
    }
    if candidates.len() == 1 {
        return Some(candidates.into_iter().next().expect("one"));
    }

    // Stage 2: served from a neighbouring community; needs a channel number, and only
    // when the market city carries no station of the network at all.
    if !in_city.is_empty() || reference.number.is_none() {
        return None;
    }
    let states: HashSet<String> = match &state {
        Some(state) => [state.clone()].into_iter().collect(),
        None => index.city_states.get(&city).cloned().unwrap_or_default(),
    };
    if states.is_empty() {
        return None;
    }
    let number = reference.number.clone().unwrap_or_default();
    let wider: Vec<MergedStation> = index
        .by_city
        .values()
        .flatten()
        .filter(|s| {
            serves_network(s, network)
                && virtual_channel(s) == number
                && states.contains(
                    &s.community_served_state
                        .as_deref()
                        .unwrap_or("")
                        .to_uppercase(),
                )
        })
        .cloned()
        .collect();
    if wider.len() == 1 {
        return Some(wider.into_iter().next().expect("one"));
    }
    None
}

/// Parse + resolve in one call over the cached merged index.
pub fn market_station_for(stream_name: &str, network: &str) -> Option<MergedStation> {
    let reference = parse_market_reference(stream_name, network)?;
    resolve_market_station(&reference, network, cached_index())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_market_reference_parses_and_resolves_or_refuses() {
        // Parsing is pure: a secondary-service token refuses, a national feed refuses.
        assert!(parse_market_reference("FOX 9 PLUS HD", "FOX").is_none());
        assert!(parse_market_reference("ABC NEWS LIVE", "ABC").is_none());
        // A market-shaped name parses (resolution needs the merged table to agree).
        let reference = parse_market_reference("US: ABC 9 HD [SYRACUSE]", "ABC").expect("parses");
        assert_eq!(reference.city, "SYRACUSE");
        assert_eq!(reference.number.as_deref(), Some("9"));
        // Resolution never invents: unknown markets resolve to nothing.
        assert!(market_station_for("US: ABC 9 HD [NOWHEREVILLE]", "ABC").is_none());
    }

    #[test]
    fn bracketed_state_reads_as_state_not_city() {
        // The first bracket wins (CM): `[DENVER]` is the city, `(CO)` is ignored.
        let reference = parse_market_reference("CBS 4 HD [DENVER] (CO)", "CBS").expect("parses");
        assert_eq!(reference.city, "DENVER");
        // A trailing state word reads as the state.
        let reference = parse_market_reference("CBS 4 HD DENVER CO", "CBS").expect("parses");
        assert_eq!(reference.state.as_deref(), Some("CO"));
        assert!(reference.city.contains("DENVER"));
    }
}
