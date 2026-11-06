//! Country detection, for restricting matches to streams from the channel's own country.
//!
//! A statement-for-statement port of Stream-Mapparr's `country.py`
//! (`reference/Stream-Mapparr/Stream-Mapparr/country.py`), which its "restrict matching
//! to same country" filter, its same-country-first ordering and its `check_countries`
//! tool all read.
//!
//! The module is deliberately FAIL-OPEN. A token it does not recognise yields `None`,
//! which [`classify`] reports as [`Relationship::Unknown`] and every caller keeps. The
//! plugin measured a fail-closed version against 2,439 real stream names: it dropped 924
//! matches and left 121 channels with no streams at all, because `GO:` and `RK:` are FAST
//! distribution-platform tags on US channels, not countries. Never make this fail closed.
//!
//! `matching_core.py`'s provider-prefix table encodes the same idea for matching and can
//! drift from this module; change them together.
//!
//! # The seam for Lineuparr (phase 3)
//!
//! Country detection is the one matcher input whose vocabulary differs most between
//! plugins: Lineuparr's `country_from_name`/`country_from_group` layer grew separately.
//! So callers take a [`CountryDetector`] rather than this module's free functions, and
//! phase 3 adds Lineuparr's detector as another implementation instead of editing this
//! one. [`StreamMapparr`] is the port below; [`classify`] and the `parse_*`/`*_overrides`
//! helpers are shared by every detector.
//!
//! Decided since (G01/G02 = `lu`): the unified engine reads ONE vocabulary,
//! [`LINEUPARR_KNOWN_CODES`], through the `lineuparr_*` entry points — stream names
//! ([`lineuparr_stream_country`]), lineup-file `CC_` names ([`lineuparr_name_country_prefix`])
//! and lineup categories ([`lineuparr_category_country`]). The SM functions above stay for
//! streams' operator prefix overrides (`stream_prefix_countries`) and the oracle harnesses.
//!
//! The zone helpers ([`extract_zone`]) live here too because the one-timezone country
//! guard is a country question: `BBC One West` is one of the BBC's English regions, not a
//! time-shifted feed, because the UK has a single time zone.

use crate::pyre::{py_splitlines, py_trim, pythonize, widen_case_insensitive_i};
use fancy_regex::Regex;
use std::collections::BTreeMap;
use std::sync::OnceLock;

/// ISO-2 codes this module is willing to name.
///
/// Seeded from Lineuparr's battle-grown set and widened by real reports (`ARG:`,
/// `DO`/`DOM`, `GB`); every code here has to be one the filter's callers understand.
pub const KNOWN_COUNTRY_CODES: [&str; 46] = [
    "AL", "AR", "AT", "AU", "AZ", "BE", "BG", "BR", "CA", "CH", "CO", "CR", "CY", "CZ", "DE", "DK",
    "DO", "ES", "FI", "FR", "GR", "HR", "HU", "IE", "IL", "IN", "IR", "IT", "JP", "KR", "MK", "MX",
    "NL", "NO", "NZ", "PH", "PL", "PT", "RO", "RS", "RU", "SE", "TH", "TR", "UK", "US",
];

/// Three-letter codes mapped onto the ISO-2 codes the shipped databases are keyed by.
const ISO3_TO_ISO2: [(&str, &str); 28] = [
    ("ARG", "AR"),
    ("AUS", "AU"),
    ("AUT", "AT"),
    ("BEL", "BE"),
    ("BRA", "BR"),
    ("CAN", "CA"),
    ("CHE", "CH"),
    ("DEU", "DE"),
    ("DNK", "DK"),
    ("DOM", "DO"),
    ("ESP", "ES"),
    ("FIN", "FI"),
    ("FRA", "FR"),
    ("GBR", "UK"),
    ("GER", "DE"),
    ("IND", "IN"),
    ("IRL", "IE"),
    ("ITA", "IT"),
    ("JPN", "JP"),
    ("KOR", "KR"),
    ("MEX", "MX"),
    ("NLD", "NL"),
    ("NOR", "NO"),
    ("NZL", "NZ"),
    ("POL", "PL"),
    ("PRT", "PT"),
    ("SWE", "SE"),
    ("USA", "US"),
];

/// Two-letter forms that mean a country already in use, folded onto the database code.
///
/// Providers disagree about which code a country gets (`GB`/`UK`, `DR`/`DO`), and the
/// target is the DATABASE code, so `GB` folds onto `UK` rather than introducing a second
/// code the country filter and the channel database would then disagree about.
const ISO2_SYNONYMS: [(&str, &str); 2] = [("DR", "DO"), ("GB", "UK")];

/// Tokens that occupy the country slot but are not countries.
///
/// Consulted BEFORE the whitelist, so a future collision (`CH` = Switzerland vs
/// "Channel") resolves the safe way. Under the fail-open rule this is belt-and-braces
/// rather than load-bearing.
const NON_COUNTRY_PREFIXES: [&str; 51] = [
    // FAST / distribution-platform tags.
    "AF", "AFR", "AMP", "FREEVEE", "GLANCE", "GO", "HUB", "LA", "MEO", "MT", "MXC", "OSN", "PLEX",
    "PLUTO", "RK", "STC", "STIRR", "TUBI", "XUMO",
    // Quality / variant tags seen in this slot ("UK FHD:", "UK HD:", "CA LBW:").
    "4K", "8K", "FHD", "HD", "HDR", "HEVC", "HQ", "LBW", "LQ", "RAW", "SD", "UHD",
    // Bouquet / category.
    "ADULT", "EROTIC", "EVENT", "GOLD", "LIVE", "MLB", "NBA", "NFL", "PPV", "PRIME", "TV", "VIP",
    "XXX", // OTA affiliate-network prefixes seen in the real corpus.
    "ABC", "CBS", "CW", "FOX", "ION", "NBC", "PBS",
];

/// Long-form country words, minimum 4 characters, scanned anywhere in a name.
///
/// Deliberately excludes the short forms `USA`/`UK`/`CA`: that exclusion is what stops
/// `USA Network` and `CNN USA` self-classifying.
const GROUP_NAME_ALIASES: [(&str, &str); 28] = [
    ("ARGENTINA", "AR"),
    ("AUSTRALIA", "AU"),
    ("BRAZIL", "BR"),
    ("BRASIL", "BR"),
    ("BRITAIN", "UK"),
    ("CANADA", "CA"),
    ("DEUTSCHLAND", "DE"),
    ("ENGLAND", "UK"),
    ("ESPANA", "ES"),
    ("ESPAÑA", "ES"),
    ("FRANCE", "FR"),
    ("GERMANY", "DE"),
    ("GREAT BRITAIN", "UK"),
    ("HOLLAND", "NL"),
    ("INDIA", "IN"),
    ("IRELAND", "IE"),
    ("MEXICO", "MX"),
    ("MÉXICO", "MX"),
    ("NETHERLANDS", "NL"),
    ("NORGE", "NO"),
    ("NORWAY", "NO"),
    ("PORTUGAL", "PT"),
    ("SCOTLAND", "UK"),
    ("SPAIN", "ES"),
    ("UNITED KINGDOM", "UK"),
    ("UNITED STATES", "US"),
    ("UNITED STATES OF AMERICA", "US"),
    ("WALES", "UK"),
];

/// Three-letter aliases scanned in GROUP names only. Two-letter codes are never
/// whole-word scanned anywhere: `IN`/`CA`/`IT`/`NO` collide with English words.
const GROUP_SHORT_ALIASES: [(&str, &str); 5] = [
    ("AUS", "AU"),
    ("CAN", "CA"),
    ("GBR", "UK"),
    ("MEX", "MX"),
    ("USA", "US"),
];

/// Codes safe to recognize WITHOUT a delimiter (`US CNN`, `CA TSN 1 HD`).
///
/// Curated: `NO`/`IT`/`IN`/`AT`/`BE`/`TO`/`IS`/`DO` are English words at the head of real
/// titles.
const BARE_CODES: &str = "US|UK|CA|AU|FR|DE|MX|MEX|FRA|GER";

/// Codes in a single time zone, where a bare East/West marker is a region, not a feed.
const SINGLE_TIMEZONE_COUNTRIES: [&str; 21] = [
    "AT", "BE", "CH", "CZ", "DE", "DK", "ES", "FI", "FR", "GR", "IE", "IN", "IT", "NL", "NO", "PL",
    "PT", "SE", "TR", "UK", "ZA",
];

/// The filter's verdict for one channel/stream country pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Relationship {
    Same,
    Foreign,
    /// Either side is unresolved. Callers KEEP unknown streams as lower-priority
    /// alternates, which is the fail-open rule.
    Unknown,
}

impl Relationship {
    /// The plugin's constant name (`SAME`/`FOREIGN`/`UNKNOWN`), for reports.
    pub fn label(self) -> &'static str {
        match self {
            Relationship::Same => "SAME",
            Relationship::Foreign => "FOREIGN",
            Relationship::Unknown => "UNKNOWN",
        }
    }
}

/// A country vocabulary. Phase 3 adds Lineuparr's as a second implementation.
pub trait CountryDetector {
    /// Country for a provider CATEGORY label (`channel_group__name`).
    fn country_from_group(&self, text: &str) -> Option<&'static str>;
    /// Country for a CHANNEL or STREAM name.
    fn country_from_name(&self, text: &str) -> Option<&'static str>;
    /// Country declared by the operator's `PREFIX=COUNTRY` map, if the text matches.
    fn country_from_prefix_overrides(
        &self,
        text: &str,
        overrides: &BTreeMap<String, &'static str>,
    ) -> Option<&'static str>;
}

/// Stream-Mapparr's detector, ported from its `country.py`.
#[derive(Debug, Clone, Copy, Default)]
pub struct StreamMapparr;

/// The Stream-Mapparr detector value.
pub const STREAM_MAPPARR: StreamMapparr = StreamMapparr;

impl CountryDetector for StreamMapparr {
    fn country_from_group(&self, text: &str) -> Option<&'static str> {
        country_from_group(text)
    }

    fn country_from_name(&self, text: &str) -> Option<&'static str> {
        country_from_name(text)
    }

    fn country_from_prefix_overrides(
        &self,
        text: &str,
        overrides: &BTreeMap<String, &'static str>,
    ) -> Option<&'static str> {
        country_from_prefix_overrides(text, overrides)
    }
}

/// `SAME` / `FOREIGN` / `UNKNOWN` for one channel/stream country pair.
pub fn classify(channel_code: Option<&str>, stream_code: Option<&str>) -> Relationship {
    match (channel_code, stream_code) {
        (None, _) | (_, None) => Relationship::Unknown,
        (Some(channel), Some(stream)) if channel == stream => Relationship::Same,
        _ => Relationship::Foreign,
    }
}

/// Country for a provider CATEGORY label (`channel_group__name`).
///
/// Group labels are a controlled-ish vocabulary, so they get the wider scan: anchored
/// marker, then long-form words, then three-letter aliases (`USA Networks`).
pub fn country_from_group(text: &str) -> Option<&'static str> {
    if text.is_empty() {
        return None;
    }
    if let Some(code) = from_prefix(text) {
        return Some(code);
    }
    scan_words(text, group_aliases())
}

/// Country for a CHANNEL or STREAM name.
///
/// Names carry brands, so only an anchored marker or a LONG-FORM country word counts:
/// `CNN Brasil` -> BR, while `CNN USA` and `USA Network` stay unresolved.
pub fn country_from_name(text: &str) -> Option<&'static str> {
    if text.is_empty() {
        return None;
    }
    if let Some(code) = from_prefix(text) {
        return Some(code);
    }
    scan_words(text, name_aliases())
}

/// Parse an operator's `PREFIX=CC` lines into an override map.
///
/// Accepts one per line or comma separated, ignores blanks and anything after a `#`, and
/// drops an entry whose code this module does not know: an invented code would compare
/// unequal to every real one and quietly mark every stream carrying that prefix as
/// foreign, which is a removal rather than a no-op.
pub fn parse_prefix_country_overrides(raw: &str) -> BTreeMap<String, &'static str> {
    let mut out = BTreeMap::new();
    if raw.is_empty() {
        return out;
    }
    for chunk in py_splitlines(&raw.replace(',', "\n")) {
        let line = py_trim(chunk.split('#').next().unwrap_or(""));
        let Some((prefix, code)) = line.split_once('=') else {
            continue;
        };
        let prefix = py_trim(prefix).to_uppercase();
        let mut code = py_trim(code).to_uppercase();
        if prefix.is_empty() || code.is_empty() {
            continue;
        }
        if let Some((_, synonym)) = ISO2_SYNONYMS.iter().find(|(key, _)| *key == code) {
            code = (*synonym).to_string();
        }
        let Some(known) = known_code(&code) else {
            continue;
        };
        out.insert(prefix, known);
    }
    out
}

/// Country for `text` from an operator-declared prefix map.
///
/// Matches only the ANCHORED prefix forms the rest of the module accepts, so a declared
/// prefix of `NOW` matches `NOW: Sky Sports` and `(NOW) Sky Sports` but never the word
/// `now` inside a title.
pub fn country_from_prefix_overrides(
    text: &str,
    overrides: &BTreeMap<String, &'static str>,
) -> Option<&'static str> {
    if text.is_empty() || overrides.is_empty() {
        return None;
    }
    let stripped = py_trim(text);
    let mut candidates: Vec<String> = Vec::new();
    if is_short_alphanumeric(stripped) {
        candidates.push(stripped.to_string());
    }
    for pattern in [wrapped_re(), token_delim_re()] {
        if let Ok(Some(caps)) = pattern.captures(text) {
            if let Some(group) = caps.get(1) {
                candidates.push(group.as_str().to_string());
            }
        }
    }
    for candidate in candidates {
        let key = py_trim(&candidate).to_uppercase();
        if let Some(code) = overrides.get(&key) {
            return Some(*code);
        }
    }
    None
}

/// Python's `re.fullmatch(r'[A-Za-z0-9]{1,12}', stripped)`.
fn is_short_alphanumeric(text: &str) -> bool {
    let len = text.chars().count();
    (1..=12).contains(&len) && text.chars().all(|c| c.is_ascii_alphanumeric())
}

/// The canonical spelling of a known code, or `None`.
fn known_code(code: &str) -> Option<&'static str> {
    KNOWN_COUNTRY_CODES
        .iter()
        .find(|known| **known == code)
        .copied()
}

/// Map a raw prefix token to a whitelisted ISO-2 code, else `None`.
fn normalize_token(token: &str) -> Option<&'static str> {
    if token.is_empty() {
        return None;
    }
    let upper = token.to_uppercase();
    if NON_COUNTRY_PREFIXES.contains(&upper.as_str()) {
        return None;
    }
    if let Some((_, synonym)) = ISO2_SYNONYMS.iter().find(|(key, _)| *key == upper) {
        return known_code(synonym);
    }
    if let Some(code) = known_code(&upper) {
        return Some(code);
    }
    ISO3_TO_ISO2
        .iter()
        .find(|(key, _)| *key == upper)
        .and_then(|(_, mapped)| known_code(mapped))
}

/// An anchored country marker at the START of `text`, or `None`.
///
/// Branch order matters: whole-string first, then the most specific marker shapes, then
/// the bare/space-separated forms that need a curated token set.
fn from_prefix(text: &str) -> Option<&'static str> {
    let stripped = py_trim(text);

    // 0. The value IS a country token ("US", "USA", "UK").
    if is_alpha_code(stripped) {
        return normalize_token(stripped);
    }

    // 1. Matched-delimiter wrap: (US), [UK], |CA|.
    if let Ok(Some(caps)) = wrapped_re().captures(text) {
        if let Some(group) = caps.get(1) {
            return normalize_token(group.as_str());
        }
    }

    // 2. Token + optional space + delimiter: "US:", "US|", "US :", "MEX-".
    if let Ok(Some(caps)) = token_delim_re().captures(text) {
        if let Some(group) = caps.get(1) {
            return normalize_token(group.as_str());
        }
    }

    // 3. Multi-token prefix: "CA LBW:" -> CA. Restricted to the curated set so
    //    "IN PROGRESS:" and "NO LIMIT TV:" cannot be read as countries.
    if let Ok(Some(caps)) = multi_token_re().captures(text) {
        if let Some(group) = caps.get(1) {
            return normalize_token(group.as_str());
        }
    }

    // 4. Country glued to a quality tag: "UKSD:", "USFHD ESPN".
    if let Ok(Some(caps)) = glued_quality_re().captures(text) {
        if let Some(group) = caps.get(1) {
            return normalize_token(group.as_str());
        }
    }

    // 5. Bare curated code + whitespace: "US CNN", "CA TSN 1 HD".
    if let Ok(Some(caps)) = bare_code_re().captures(text) {
        if let Some(group) = caps.get(1) {
            return normalize_token(group.as_str());
        }
    }

    // 6. Bare "USA " with the brand guard that protects the channel USA Network.
    if let Ok(true) = usa_bare_re().is_match(text) {
        return Some("US");
    }

    None
}

/// Python's `re.fullmatch(r'[A-Za-z]{2,3}', stripped)`.
fn is_alpha_code(text: &str) -> bool {
    let len = text.chars().count();
    (2..=3).contains(&len) && text.chars().all(|c| c.is_ascii_alphabetic())
}

/// A precompiled whole-word scan table, longest alias first.
type ScanTable = Vec<(Regex, &'static str)>;

fn compile_scan_table(table: &[(&'static str, &'static str)]) -> ScanTable {
    let mut entries: Vec<(usize, Regex, &'static str)> = table
        .iter()
        .map(|(alias, code)| {
            let pattern = format!(
                "{}{}{}",
                crate::pyre::PY_BOUNDARY,
                crate::pyre::python_escape(alias),
                crate::pyre::PY_BOUNDARY
            );
            let regex =
                Regex::new(&pattern).unwrap_or_else(|e| panic!("bad country alias {alias:?}: {e}"));
            (alias.chars().count(), regex, *code)
        })
        .collect();
    // Longest alias first, measured in characters like Python's `len()`; the sort is
    // stable, so equal lengths keep table order. The plugin sorts its dict the same way,
    // and the only equal-length aliases that can both match one text (`BRAZIL`/`BRASIL`,
    // `MEXICO`/`MÉXICO`, the UK nations) resolve to the same code anyway.
    entries.sort_by_key(|(length, _, _)| std::cmp::Reverse(*length));
    entries
        .into_iter()
        .map(|(_, regex, code)| (regex, code))
        .collect()
}

fn name_aliases() -> &'static ScanTable {
    static TABLE: OnceLock<ScanTable> = OnceLock::new();
    TABLE.get_or_init(|| compile_scan_table(&GROUP_NAME_ALIASES))
}

fn group_aliases() -> &'static ScanTable {
    static TABLE: OnceLock<ScanTable> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut combined: Vec<(&'static str, &'static str)> = GROUP_NAME_ALIASES.to_vec();
        for entry in GROUP_SHORT_ALIASES {
            if !combined.iter().any(|(alias, _)| *alias == entry.0) {
                combined.push(entry);
            }
        }
        compile_scan_table(&combined)
    })
}

/// Longest-alias-first whole-word scan of `text` against a table.
fn scan_words(text: &str, table: &ScanTable) -> Option<&'static str> {
    let upper = text.to_uppercase();
    let mut normalized = String::with_capacity(upper.len());
    for c in upper.chars() {
        if matches!(c, '[' | ']' | '(' | ')' | '_' | '-' | '|' | '┃' | '│') {
            normalized.push(' ');
        } else {
            normalized.push(c);
        }
    }
    let mut collapsed = String::with_capacity(normalized.len());
    for token in crate::pyre::py_split_whitespace(&normalized) {
        if !collapsed.is_empty() {
            collapsed.push(' ');
        }
        collapsed.push_str(token);
    }
    for (regex, code) in table {
        if regex.is_match(&collapsed).unwrap_or(false) {
            return Some(code);
        }
    }
    None
}

fn ci(pattern: &str) -> String {
    format!("(?i){}", widen_case_insensitive_i(&pythonize(pattern)))
}

fn wrapped_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&pythonize(r"^\s*[\(\[\|┃│]\s*([A-Za-z]{2,3})\s*[\)\]\|┃│]"))
            .expect("static regex")
    })
}

fn token_delim_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&pythonize(r"^\s*([A-Za-z]{2,3})\s*[-:|┃│▎]")).expect("static regex")
    })
}

fn multi_token_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&ci(&format!(
            r"^\s*({BARE_CODES})\s+[A-Za-z0-9]{{1,6}}\s*[-:|┃│▎]"
        )))
        .expect("static regex")
    })
}

fn glued_quality_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&ci(r"^\s*(US|UK)(?:SD|HD|FHD|UHD|FD|HEVC|4K|8K)\b")).expect("static regex")
    })
}

fn bare_code_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(&ci(&format!(r"^\s*({BARE_CODES})\s+"))).expect("static regex"))
}

fn usa_bare_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(&ci(r"^\s*USA\s+(?!NETWORK\b)")).expect("static regex"))
}

// ---------------------------------------------------------------------------
// Feed zones (Stream-Mapparr's `extract_zone`).
// ---------------------------------------------------------------------------

/// Canonical feed zone for zone-aware stream routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeedZone {
    East,
    West,
    /// Unmarked; conventionally the East/primary feed in US lineups.
    Default,
}

impl FeedZone {
    /// The plugin's spelling: `EAST`, `WEST`, `DEFAULT`.
    pub fn label(self) -> &'static str {
        match self {
            FeedZone::East => "EAST",
            FeedZone::West => "WEST",
            FeedZone::Default => "DEFAULT",
        }
    }
}

/// A bare marker must also STAND ALONE: parenthesized, the last meaningful word, or
/// followed only by a quality/format tag.
///
/// A marker followed by another WORD is part of a place name, not a feed zone. Measured on
/// a live installation: of 191 assignments an opposite-zone exclusion would have removed,
/// 24 were place names — `West Palm Beach`, `WEST HASTINGS, NE`, `WEST HARTFORD, CT` — and
/// two channels would have been left with no streams at all.
fn zone_standalone() -> String {
    let trailing_tags =
        r"(?:HD|SD|FHD|UHD|4K|8K|HEVC|H\.?265|H\.?264|RAW|FPS|\d+P?|\([A-Z]{1,4}\))";
    // Decorations providers append after a real marker: superscripts, symbols, and any
    // non-letter run. `(?:\W|[^\x00-\x7F])*` is the plugin's tail; the union of "not a
    // Python word character" and "not ASCII" is exactly "not an ASCII word character".
    let tail = r"[^A-Za-z0-9_]*";
    format!(
        r"(?:{tail}$|\s+{tags}(?:\s+{tags})*{tail}$)",
        tail = tail,
        tags = trailing_tags
    )
}

fn zone_west_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&ci(&format!(
            r"\(\s*W(?:EST)?\s*\)|\bWEST\b{}",
            zone_standalone()
        )))
        .expect("static regex")
    })
}

fn zone_east_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&ci(&format!(
            r"\(\s*E(?:AST)?\s*\)|\bEAST\b{}",
            zone_standalone()
        )))
        .expect("static regex")
    })
}

fn zone_pacific_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&ci(&format!(
            r"\(\s*PACIFIC\s*\)|\(\s*PT\s*\)|\bPACIFIC\b{}",
            zone_standalone()
        )))
        .expect("static regex")
    })
}

/// True when the name carries a country marker for a single-time-zone country, so East and
/// West on it are regions rather than feeds.
///
/// Never panics and never fails closed: country detection is optional here and a failure
/// falls through to the ordinary zone rules.
pub fn country_has_one_timezone(name: &str) -> bool {
    country_from_name(name).is_some_and(|code| SINGLE_TIMEZONE_COUNTRIES.contains(&code))
}

/// Whether `name` can possibly carry a feed-zone marker, checked without any regex.
///
/// A pure speed-up with the same answer (see [`extract_zone`]): it may say yes when the
/// patterns would say no, but it must never say no when a pattern would match. Every
/// parenthesized marker (`(W)`, `(EAST)`, `(PT)`, `(Pacific)`) needs a `(` byte, so a name
/// without one can only match the bare words `WEST`/`EAST`/`PACIFIC` — and a bare `PT` is
/// never a marker. A non-ASCII name takes the precise path: Python's `(?i)` folds exotic
/// case variants (the long `ſ` matches `s`) that a byte scan cannot see, so only a pure-ASCII
/// name without those fragments is allowed to skip the patterns.
fn maybe_zoned(name: &str) -> bool {
    if name.bytes().any(|byte| byte == b'(') {
        return true;
    }
    if !name.is_ascii() {
        return true;
    }
    ascii_contains_folded(name.as_bytes(), b"west")
        || ascii_contains_folded(name.as_bytes(), b"east")
        || ascii_contains_folded(name.as_bytes(), b"pacific")
}

/// ASCII case-insensitive substring search, without allocating a lowered copy.
fn ascii_contains_folded(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.len() >= needle.len()
        && haystack.windows(needle.len()).any(|window| {
            window
                .iter()
                .zip(needle)
                .all(|(hay, want)| hay.to_ascii_lowercase() == *want)
        })
}

/// Canonical feed zone for zone-aware routing: `WEST`, `EAST`, or `DEFAULT`.
///
/// Recognizes parenthesized `(W)`/`(E)`/`(WEST)`/`(EAST)` and the bare words `WEST`/
/// `EAST`. A Pacific marker (bare `PACIFIC` or `(Pacific)`/`(PT)`) folds into `WEST`: a US
/// premium channel's West feed IS its Pacific-time feed. Bare single letters `W`/`E`
/// outside parentheses are NOT zones — too many false positives (`W`, `E! Entertainment`).
///
/// In a single-time-zone country the answer is always `DEFAULT`: `BBC One West` is an
/// English region, not a time-shifted feed.
///
/// The three marker patterns are tried BEFORE the single-time-zone guard, which is a pure
/// speed-up with the same answer: with no marker at all the guard's verdict cannot change the
/// result, and the guard is the expensive half (it scans the country tables). Before either,
/// [`maybe_zoned`] rejects the overwhelmingly common unmarked name with a byte scan: a sort
/// calls this once per channel and once per stream — 20k channels and 200k stream references
/// is 220k calls — and almost every name in a lineup carries no marker at all.
pub fn extract_zone(name: &str) -> FeedZone {
    if !maybe_zoned(name) {
        return FeedZone::Default;
    }
    // The Stream-Mapparr transcription (the SM parity sweep's oracle): the raw name,
    // NOT the Z08 tag strip — that is the engine's [`extract_zone_noguard`] alone.
    let west = zone_west_re().is_match(name).unwrap_or(false)
        || zone_pacific_re().is_match(name).unwrap_or(false);
    let east = !west && zone_east_re().is_match(name).unwrap_or(false);
    if !west && !east {
        return FeedZone::Default;
    }
    if country_has_one_timezone(name) {
        return FeedZone::Default;
    }
    if west {
        FeedZone::West
    } else {
        FeedZone::East
    }
}

/// Canonical feed zone WITHOUT the single-time-zone guard (decided Z02=none).
///
/// Same markers as [`extract_zone`] — parenthesized `(W)`/`(E)`/`(WEST)`/`(EAST)`,
/// standalone bare `WEST`/`EAST`, Pacific (`PACIFIC`/`(Pacific)`/`(PT)`) folding into
/// `WEST` — read after collapsing `|`-delimited junk segments (decided N15:
/// `Cinemax East | HD |` is East) and stripping trailing writer tags (decided Z08:
/// `HBO West (Backup)` is West), but no `_country_has_one_timezone` escape: the
/// corpus labels a single-time-zone region (`UK: BBC One West`) as its own channel,
/// the semantic the guard was written against. A bare `(P)` is NOT Pacific (decided
/// Z07=sm-strict: SM's `_ZONE_PACIFIC_RE` matches bare `PACIFIC` and `(Pacific)`/
/// `(PT)` only). The [`zone_pacific_re`] pattern already encodes that stricter
/// reading, so both entry points agree on `(P)`.
pub fn extract_zone_noguard(name: &str) -> FeedZone {
    let collapsed = crate::tags::collapse_pipe_segments(name);
    let name = collapsed.as_ref();
    if !maybe_zoned(name) {
        return FeedZone::Default;
    }
    // Decided Z08: the zone word may carry trailing writer tags (`HBO West (Backup)`
    // is still West), so the patterns read the name with those stripped. The strip only
    // peels quality/junk/status tags and `+N` shifts (which stay shifts: the timeshift
    // flag is read off the original name), never regional words, so the zone itself is
    // always still in the core when one is reported.
    let core = crate::tags::strip_zone_ignorable(name);
    let west = zone_west_re().is_match(core).unwrap_or(false)
        || zone_pacific_re().is_match(core).unwrap_or(false);
    let east = !west && zone_east_re().is_match(core).unwrap_or(false);
    if west {
        FeedZone::West
    } else if east {
        FeedZone::East
    } else {
        FeedZone::Default
    }
}

/// Zone-aware routing keep rule (decided Z01=sm-zone, Z02=none, D09=sm-both).
///
/// A `WEST` name keeps only `WEST` candidates; any other name keeps only non-`WEST`
/// ones. Both sides are read with [`extract_zone_noguard`], so there is exactly one
/// zone rule: feed separation is routing, never a second post-match filter.
/// `(P)` names route as `DEFAULT` on both sides (decided Z07).
pub fn zone_routing_keep(query: &str, candidate: &str) -> bool {
    (extract_zone_noguard(query) == FeedZone::West)
        == (extract_zone_noguard(candidate) == FeedZone::West)
}

// ---------------------------------------------------------------------------
// Lineuparr's country detector (decided G01=lu, G02=lu, G03=lu-strict, G04=lu-counter).
//
// Port of the vocabulary in `reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/
// fuzzy_matcher.py:82-160`: EXCLUDES `AR` (tags Arabic-language channels, not
// Argentina) and INCLUDES `QA`/`VN`/`PK`/`SI`/`ET` plus the feed-confirmed batches;
// `(PLUTO <country>)` full-name map; strict cross-border rule with the US<->MX
// shared-feed exceptions. Entry points mirror Lineuparr's: stream-name detection,
// `CC_` lineup-name prefix split, category detection.
// ---------------------------------------------------------------------------

/// ISO-2 codes Lineuparr's lineup filenames use (one unified vocabulary, G01=lu).
///
/// `AR` is deliberately absent: in these feeds it tags Arabic, not Argentina.
pub const LINEUPARR_KNOWN_CODES: [&str; 49] = [
    "US", "UK", "CA", "AU", "DE", "FR", "IT", "ES", "NL", "BR", "MX", "IN", "IE", "SE", "NO", "DK",
    "PT", "PL", "AT", "CH", "BE", "FI", "TR", "GR", "IR", "AL", "BG", "RO", "RU", "AZ", "HR", "TH",
    "RS", "MK", "IL", "CO", "CR", "CY", "JP", "KR", "CZ", "HU", "NZ", "PH", "VN", "PK", "SI", "ET",
    "QA",
];

/// ISO-3 or colloquial codes Lineuparr folds onto ISO-2.
const LINEUPARR_ISO3_TO_ISO2: [(&str, &str); 11] = [
    ("USA", "US"),
    ("MEX", "MX"),
    ("IRE", "IE"),
    ("GER", "DE"),
    ("FRA", "FR"),
    ("ITA", "IT"),
    ("ESP", "ES"),
    ("NLD", "NL"),
    ("BRA", "BR"),
    ("IND", "IN"),
    ("ETH", "ET"),
];

/// `(PLUTO <COUNTRY>)` full-name variants Lineuparr recognises in the M3U.
const LINEUPARR_PLUTO_MAP: [(&str, &str); 16] = [
    ("USA", "US"),
    ("US", "US"),
    ("UK", "UK"),
    ("BRAZIL", "BR"),
    ("SWEDEN", "SE"),
    ("NORWAY", "NO"),
    ("DENMARK", "DK"),
    ("GERMANY", "DE"),
    ("SPAIN", "ES"),
    ("FRANCE", "FR"),
    ("ITALY", "IT"),
    ("CANADA", "CA"),
    ("MEXICO", "MX"),
    ("INDIA", "IN"),
    ("IRELAND", "IE"),
    ("AUSTRALIA", "AU"),
];

/// Genuinely shared cross-border feeds, keyed by unordered country pair (G03=lu-strict).
///
/// Comparison keys are accent-folded lowercase alphanumerics (`NBC Universo` ->
/// `nbcuniverso`). US<->MX only: the US Spanish-language networks whose M3U feeds are
/// frequently tagged MEX/(MX) even though they are the US feed.
const LINEUPARR_CROSS_BORDER_SHARED: [(&str, &str); 13] = [
    ("US", "univision"),
    ("US", "unimas"),
    ("US", "telemundo"),
    ("US", "galavision"),
    ("US", "universo"),
    ("US", "nbcuniverso"),
    ("US", "tudn"),
    ("US", "telexitos"),
    ("US", "bandamax"),
    ("US", "tlnovelas"),
    ("US", "univisiontlnovelas"),
    ("US", "telemundodeportes"),
    ("US", "universonbc"),
];

fn lineuparr_known(code: &str) -> Option<&'static str> {
    LINEUPARR_KNOWN_CODES
        .iter()
        .find(|known| **known == code)
        .copied()
}

fn lineuparr_normalize_token(token: &str) -> Option<&'static str> {
    if token.is_empty() {
        return None;
    }
    let upper = token.to_uppercase();
    if NON_COUNTRY_PREFIXES.contains(&upper.as_str()) {
        return None;
    }
    if let Some(code) = lineuparr_known(&upper) {
        return Some(code);
    }
    LINEUPARR_ISO3_TO_ISO2
        .iter()
        .find(|(key, _)| *key == upper)
        .and_then(|(_, mapped)| lineuparr_known(mapped))
}

fn lineuparr_pluto_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // re.IGNORECASE in LU (`(Pluto UK) POP` is tagged UK).
        Regex::new(&ci(r"^\s*\(\s*PLUTO\s+([A-Za-z]+)\s*\)")).expect("static regex")
    })
}

fn lineuparr_delim_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&pythonize(r"^\s*([A-Za-z]{2,3})\s*[-:|\u{2503}\u{2502}]"))
            .expect("static regex")
    })
}

fn lineuparr_bare_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&ci(r"^\s*(US|UK|CA|AU|FR|DE|MX|MEX|FRA|GER)\s+")).expect("static regex")
    })
}

fn lineuparr_glued_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&ci(r"^\s*(US|UK)(?:SD|HD|FHD|UHD|FD|HEVC|4K|8K)\b")).expect("static regex")
    })
}

/// Lineuparr's `detect_stream_country`: ISO-2 code from a stream name's leading marker.
///
/// Recognises `(US) ...`, `(USA) ...`, `US: ...`, `MEX: ...`, `(PLUTO UK) ...`
/// (case-insensitive). `None` when no recognised marker is present, so callers keep
/// unlabeled streams (fail-open).
///
/// Decided N15: the name collapses first (`UK | Sky Sports | HD` reads as
/// `UK: Sky Sports`, the delimited marker the reader already takes; `USA | HD |`
/// reads as the bare code `USA`, which no marker form takes, so the USA Network
/// name stays country-less like the cleaned `USA`).
pub fn lineuparr_stream_country(name: &str) -> Option<&'static str> {
    if name.is_empty() {
        return None;
    }
    let collapsed = crate::tags::collapse_pipe_segments(name);
    let name = collapsed.as_ref();
    if let Ok(Some(caps)) = lineuparr_pluto_re().captures(name) {
        if let Some(group) = caps.get(1) {
            let upper = group.as_str().to_uppercase();
            if let Some((_, code)) = LINEUPARR_PLUTO_MAP.iter().find(|(key, _)| *key == upper) {
                return lineuparr_known(code);
            }
            return None;
        }
    }
    if let Ok(Some(caps)) = wrapped_re().captures(name) {
        if let Some(group) = caps.get(1) {
            return lineuparr_normalize_token(group.as_str());
        }
    }
    if let Ok(Some(caps)) = lineuparr_delim_re().captures(name) {
        if let Some(group) = caps.get(1) {
            return lineuparr_normalize_token(group.as_str());
        }
    }
    if let Ok(Some(caps)) = lineuparr_bare_re().captures(name) {
        if let Some(group) = caps.get(1) {
            return lineuparr_normalize_token(group.as_str());
        }
    }
    if let Ok(Some(caps)) = lineuparr_glued_re().captures(name) {
        if let Some(group) = caps.get(1) {
            return lineuparr_normalize_token(group.as_str());
        }
    }
    if usa_bare_re().is_match(name).unwrap_or(false) {
        return Some("US");
    }
    None
}

/// Lineuparr's `detect_name_country_prefix`: split a `CC_` prefix off a lineup name.
///
/// Returns `(country, stripped)` or `(None, name)` unchanged when there is no
/// recognised prefix. Only the curated codes are accepted, so `MTV_Live` is left alone.
pub fn lineuparr_name_country_prefix(channel_name: &str) -> (Option<&'static str>, &str) {
    if channel_name.is_empty() {
        return (None, channel_name);
    }
    let bytes = channel_name.as_bytes();
    if bytes.len() > 3 {
        if let Some(underscore) = channel_name.find('_') {
            if underscore == 2 || underscore == 3 {
                let head = &channel_name[..underscore];
                let rest = &channel_name[underscore + 1..];
                if !rest.is_empty() && !rest.starts_with(char::is_whitespace) {
                    let upper = head.to_uppercase();
                    if let Some(code) = lineuparr_normalize_token(&upper) {
                        if head.chars().all(|c| c.is_ascii_alphabetic()) {
                            return (Some(code), rest);
                        }
                    }
                }
            }
        }
    }
    (None, channel_name)
}

/// Lineuparr's `detect_category_country`: country from a lineup category name.
///
/// Port of `detect_category_country` in
/// `reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/fuzzy_matcher.py`.
/// Single-country lineups carry one country code in the filename and use plain theme
/// categories (`News`, `Sports`); mixed-country lineups encode each channel's country in
/// the category name (`AU| AUSTRALIA VIP`, `UK: Sports`, `NZ Movies`). Returns a
/// whitelisted ISO-2 code, or `None` for ordinary theme categories so the caller falls
/// back to the lineup-level code.
///
/// Two branches with deliberately different breadth, as in the plugin:
///   - delimiter branch (`XX:`/`XX-`/`XX|`): accepts ANY code in the full
///     [`LINEUPARR_KNOWN_CODES`] set;
///   - bare-space branch (`XX Movies`): restricted to the curated set below, so a plain
///     word cannot be eaten by a space-separated token.
///
/// The token vocabulary is Lineuparr's (decided G01=lu): `AR` is absent (it tags
/// Arabic-language channels, not Argentina), and `QA`/`VN`/`PK`/`SI`/`ET` are present.
pub fn lineuparr_category_country(category_name: &str) -> Option<&'static str> {
    if category_name.is_empty() {
        return None;
    }
    static DELIM_RE: std::sync::OnceLock<fancy_regex::Regex> = std::sync::OnceLock::new();
    static BARE_RE: std::sync::OnceLock<fancy_regex::Regex> = std::sync::OnceLock::new();
    // `re.match(r'^\s*([A-Za-z]{2,3})\s*[-:|┃│]', ...)`.
    let delim = DELIM_RE.get_or_init(|| {
        fancy_regex::Regex::new(&crate::pyre::pythonize(
            r"^\s*([A-Za-z]{2,3})\s*[-:|\u{2503}\u{2502}]",
        ))
        .expect("static regex")
    });
    if let Ok(Some(caps)) = delim.captures(category_name) {
        if let Some(group) = caps.get(1) {
            return lineuparr_normalize_token(group.as_str());
        }
    }
    // `re.match(r'^\s*(US|UK|CA|AU|NZ|FR|DE|MX|MEX|FRA|GER)\s+', ..., re.IGNORECASE)`.
    let bare = BARE_RE.get_or_init(|| {
        fancy_regex::Regex::new(&ci(r"^\s*(US|UK|CA|AU|NZ|FR|DE|MX|MEX|FRA|GER)\s+"))
            .expect("static regex")
    });
    if let Ok(Some(caps)) = bare.captures(category_name) {
        if let Some(group) = caps.get(1) {
            return lineuparr_normalize_token(group.as_str());
        }
    }
    None
}

/// Accent-fold, lowercase, strip to alphanumerics (Lineuparr's `_fold_key`).
pub fn lineuparr_fold_key(name: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    name.nfd()
        .filter(|c| {
            unicode_general_category::get_general_category(*c)
                != unicode_general_category::GeneralCategory::NonspacingMark
        })
        .collect::<String>()
        .to_lowercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect()
}

/// Whether a cross-border channel/stream country pair is allowed (G03=lu-strict).
///
/// Strict by default: a lineup accepts only streams tagged with its own country (plus
/// untagged streams, handled by callers via [`Relationship::Unknown`]). The ONLY
/// exceptions are the genuinely shared US<->MX feeds in
/// [`LINEUPARR_CROSS_BORDER_SHARED`], matched on the folded channel key.
pub fn lineuparr_cross_border_ok(
    channel_country: &str,
    stream_country: &str,
    channel_name: &str,
) -> bool {
    if channel_country == stream_country {
        return true;
    }
    let pair_is_us_mx = (channel_country == "US" && stream_country == "MX")
        || (channel_country == "MX" && stream_country == "US");
    if !pair_is_us_mx {
        return false;
    }
    let key = lineuparr_fold_key(channel_name);
    LINEUPARR_CROSS_BORDER_SHARED
        .iter()
        .any(|(code, shared)| *code == "US" && *shared == key)
}

/// Filter accounting for the country rule (G04=lu-counter): reporting only, never
/// changes a verdict by itself.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CountryFilterCount {
    pub kept: u64,
    pub dropped_foreign: u64,
    pub kept_unknown: u64,
}

impl CountryFilterCount {
    pub fn record(&mut self, relationship: Relationship) {
        match relationship {
            Relationship::Same => self.kept += 1,
            Relationship::Unknown => {
                self.kept += 1;
                self.kept_unknown += 1;
            }
            Relationship::Foreign => self.dropped_foreign += 1,
        }
    }
}

/// Lineuparr's country detector (decided G01/G02): one [`CountryDetector`] impl.
#[derive(Debug, Clone, Copy, Default)]
pub struct Lineuparr;

/// The decided country detector value.
pub const LINEUPARR: Lineuparr = Lineuparr;

impl CountryDetector for Lineuparr {
    fn country_from_group(&self, text: &str) -> Option<&'static str> {
        if text.is_empty() {
            return None;
        }
        if let Some(code) = lineuparr_stream_country(text) {
            return Some(code);
        }
        scan_words(text, group_aliases())
    }

    fn country_from_name(&self, text: &str) -> Option<&'static str> {
        if text.is_empty() {
            return None;
        }
        if let Some(code) = lineuparr_stream_country(text) {
            return Some(code);
        }
        scan_words(text, name_aliases())
    }

    fn country_from_prefix_overrides(
        &self,
        text: &str,
        overrides: &BTreeMap<String, &'static str>,
    ) -> Option<&'static str> {
        country_from_prefix_overrides(text, overrides)
    }
}

/// M3U group-header skip (decided Z05=lu-skip).
///
/// True for playlist group/section separators like `##### EUROSPORT #####` or
/// `## 24/7 COMEDY ##`: a run of 2+ `#`/`=`/`*` is decorative (no real channel name
/// contains `##`/`==`/`**`); pipes need 3+ since a single `|` is a common in-name
/// separator. Headers are not real names and are excluded from matching.
pub fn is_group_header(name: &str) -> bool {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(&pythonize(r"[#=*]{2,}|\|{3,}")).expect("static regex"));
    re.is_match(name).unwrap_or(false)
}

/// Tags preserved for display re-attachment (decided D09=sm-both).
///
/// SM's `extract_tags`: regional (parenthesized first, else the last bare word among
/// East/West/Pacific/Central/Mountain/Atlantic), extra parenthesized tags (skipping a
/// leading prefix tag, ignored tags, regional indicators and callsigns) and bracketed
/// quality tags. Runs beside [`extract_zone_noguard`]: routing reads the zone, display
/// re-attaches these.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NameTags {
    pub regional: Option<String>,
    pub extra_tags: Vec<String>,
    pub quality_tags: Vec<String>,
}

/// Extract display tags from a name (SM's `extract_tags`, D09 `sm-both`).
pub fn extract_tags(name: &str, user_ignored_tags: &[&str]) -> NameTags {
    static REGIONAL_PAREN: OnceLock<Regex> = OnceLock::new();
    static REGIONAL_WORD: OnceLock<Regex> = OnceLock::new();
    static PAREN: OnceLock<Regex> = OnceLock::new();
    static BRACKET: OnceLock<Regex> = OnceLock::new();
    static CALLSIGN_TAG: OnceLock<Regex> = OnceLock::new();
    let regional_paren = REGIONAL_PAREN.get_or_init(|| {
        Regex::new(&ci(r"\((East|West|Pacific|Central|Mountain|Atlantic)\)")).expect("static regex")
    });
    let regional_word = REGIONAL_WORD.get_or_init(|| {
        Regex::new(&ci(
            r"\b(East|West|Pacific|Central|Mountain|Atlantic)\b(?!.*\b(East|West|Pacific|Central|Mountain|Atlantic)\b)",
        ))
        .expect("static regex")
    });
    let paren =
        PAREN.get_or_init(|| Regex::new(&pythonize(r"\(([^\)]+)\)")).expect("static regex"));
    let bracket =
        BRACKET.get_or_init(|| Regex::new(&pythonize(r"\[([^\]]+)\]")).expect("static regex"));
    let callsign_tag = CALLSIGN_TAG.get_or_init(|| {
        Regex::new(&pythonize(r"^[KW][A-Z]{3}(?:-(?:TV|CD|LP|DT|LD))?$")).expect("static regex")
    });

    let mut tags = NameTags::default();
    if let Ok(Some(caps)) = regional_paren.captures(name) {
        if let Some(group) = caps.get(1) {
            let mut regional = group.as_str().to_string();
            if let Some(first) = regional.get_mut(..1) {
                first.make_ascii_uppercase();
            }
            tags.regional = Some(regional);
        }
    } else if let Ok(Some(caps)) = regional_word.captures(name) {
        if let Some(group) = caps.get(1) {
            let mut regional = group.as_str().to_string();
            if let Some(first) = regional.get_mut(..1) {
                first.make_ascii_uppercase();
            }
            tags.regional = Some(regional);
        }
    }

    let paren_tags: Vec<String> = paren
        .captures_iter(name)
        .flatten()
        .filter_map(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        .collect();
    let first_is_prefix = crate::pyre::py_trim(name).starts_with('(');
    for (idx, tag) in paren_tags.iter().enumerate() {
        if idx == 0 && first_is_prefix {
            continue;
        }
        if user_ignored_tags.contains(&format!("({tag})").as_str())
            || user_ignored_tags.contains(&format!("[{tag}]").as_str())
        {
            continue;
        }
        let upper = tag.to_uppercase();
        if matches!(
            upper.as_str(),
            "EAST" | "WEST" | "PACIFIC" | "CENTRAL" | "MOUNTAIN" | "ATLANTIC"
        ) {
            continue;
        }
        if callsign_tag.is_match(&upper).unwrap_or(false) {
            continue;
        }
        tags.extra_tags.push(format!("({tag})"));
    }
    for caps in bracket.captures_iter(name).flatten() {
        let Some(group) = caps.get(1) else {
            continue;
        };
        let tag = group.as_str().to_string();
        if !user_ignored_tags.contains(&format!("[{tag}]").as_str())
            && !user_ignored_tags.contains(&format!("({tag})").as_str())
        {
            tags.quality_tags.push(format!("[{tag}]"));
        }
    }
    tags
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lineuparr_category_country_matches_the_plugin() {
        // Mixed-country lineups encode the country in the category name; theme
        // categories fall back to the lineup-level code (None here).
        for (category, expected) in [
            ("AU| AUSTRALIA VIP", Some("AU")),
            ("UK: Sports", Some("UK")),
            ("NZ Movies", Some("NZ")),
            ("US News", Some("US")),
            ("MEX Bein Sports", Some("MX")),
            ("News", None),
            ("Sports", None),
            // Theme words a delimiter must not eat: not whitelisted codes.
            ("Sci-Fi", None),
            ("On-Demand", None),
            ("German", None),
            ("IT Crowd", None),
            ("", None),
        ] {
            assert_eq!(
                lineuparr_category_country(category),
                expected,
                "{category:?}"
            );
        }
        // `AR` is deliberately absent: it tags Arabic, not Argentina.
        assert_eq!(lineuparr_category_country("AR: beIN DRAMA"), None);
        // Lineuparr's newer batch resolves too.
        assert_eq!(lineuparr_category_country("QA: beIN Sports"), Some("QA"));
        assert_eq!(lineuparr_category_country("VN Movies"), None);
    }

    #[test]
    fn lineuparr_pluto_marker_is_case_insensitive() {
        assert_eq!(lineuparr_stream_country("(PLUTO UK) POP"), Some("UK"));
        assert_eq!(lineuparr_stream_country("(Pluto UK) POP"), Some("UK"));
        assert_eq!(lineuparr_stream_country("(pluto germany) mtv"), Some("DE"));
    }

    #[test]
    fn prefix_forms_resolve() {
        for (text, expected) in [
            ("US", Some("US")),
            ("USA", Some("US")),
            ("GB", Some("UK")),
            ("DR", Some("DO")),
            ("GBR", Some("UK")),
            ("DEU: Das Erste", Some("DE")),
            ("(US) TNT", Some("US")),
            ("[UK] BBC One", Some("UK")),
            ("|CA| TSN", Some("CA")),
            ("US: ESPN", Some("US")),
            ("MEX- Canal", Some("MX")),
            ("CA LBW: TSN", Some("CA")),
            ("UKSD: Sky Sports", Some("UK")),
            ("US CNN", Some("US")),
            ("USFHD SOMETHING", Some("US")),
            ("USA  ABC", Some("US")),
        ] {
            assert_eq!(from_prefix(text), expected, "{text:?}");
        }
    }

    #[test]
    fn non_country_prefixes_and_brands_do_not_resolve() {
        // Brands and platform/quality tags in the country slot are not countries.
        for text in [
            "USA Network",
            "GO: ESPN",
            "RK: beIN Sports Xtra",
            "IN PROGRESS: Something",
            "NO LIMIT TV",
            "ADULT: Something",
            "TV: Something",
            "FOX: EL PASO TX KFOX",
            "PPV: Event",
        ] {
            assert_eq!(from_prefix(text), None, "{text:?}");
        }
        // `US Open` and `US Armenian TV` ARE anchored US markers in this module (the
        // brand guard belongs to the normaliser, not the detector).
        assert_eq!(from_prefix("US Open"), Some("US"));
        assert_eq!(country_from_name("USA Network"), None);
        assert_eq!(country_from_name("CNN USA"), None);
    }

    #[test]
    fn long_form_words_scan_names_and_groups() {
        assert_eq!(country_from_name("CNN Brasil"), Some("BR"));
        assert_eq!(country_from_name("CNN USA"), None);
        assert_eq!(country_from_name("USA Network"), None);
        assert_eq!(country_from_name("Great Britain TV"), Some("UK"));
        assert_eq!(country_from_name("GBR Entertainment"), None);
        assert_eq!(country_from_group("USA Networks"), Some("US"));
        assert_eq!(country_from_group("GBR Entertainment"), Some("UK"));
        assert_eq!(country_from_group("Norway"), Some("NO"));
    }

    #[test]
    fn classify_is_fail_open() {
        assert_eq!(classify(Some("US"), Some("US")), Relationship::Same);
        assert_eq!(classify(Some("US"), Some("UK")), Relationship::Foreign);
        assert_eq!(classify(None, Some("UK")), Relationship::Unknown);
        assert_eq!(classify(Some("US"), None), Relationship::Unknown);
        assert_eq!(Relationship::Unknown.label(), "UNKNOWN");
    }

    #[test]
    fn prefix_overrides_parse_and_apply() {
        let overrides =
            parse_prefix_country_overrides("NOW=GB  # Sky in the UK\nPPT=ZZ, MEO=PT\n\n=US\nBAD\n");
        assert_eq!(overrides.get("NOW"), Some(&"UK"));
        // An invented code is dropped rather than marking every stream foreign.
        assert_eq!(overrides.get("PPT"), None);
        assert_eq!(overrides.get("MEO"), Some(&"PT"));
        assert_eq!(
            country_from_prefix_overrides("NOW: Sky Sports", &overrides),
            Some("UK")
        );
        assert_eq!(
            country_from_prefix_overrides("(NOW) Sky Sports", &overrides),
            Some("UK")
        );
        // A bare word is not an anchored prefix.
        assert_eq!(
            country_from_prefix_overrides("now: Sky", &overrides),
            Some("UK")
        );
        assert_eq!(
            country_from_prefix_overrides("Right now on Sky", &overrides),
            None
        );
    }

    #[test]
    fn zones_fold_and_respect_single_timezone_countries() {
        assert_eq!(extract_zone("HBO East"), FeedZone::East);
        assert_eq!(extract_zone("HBO West"), FeedZone::West);
        assert_eq!(extract_zone("HBO (W)"), FeedZone::West);
        assert_eq!(extract_zone("HBO (E)"), FeedZone::East);
        assert_eq!(extract_zone("HBO Pacific"), FeedZone::West);
        assert_eq!(extract_zone("HBO (Pacific)"), FeedZone::West);
        assert_eq!(extract_zone("HBO (PT)"), FeedZone::West);
        assert_eq!(extract_zone("HBO"), FeedZone::Default);
        // Place names are not feed zones.
        assert_eq!(extract_zone("West Palm Beach"), FeedZone::Default);
        assert_eq!(extract_zone("WEST HASTINGS, NE"), FeedZone::Default);
        // Markers followed only by quality tags still count.
        assert_eq!(extract_zone("ESPN West HD"), FeedZone::West);
        assert_eq!(extract_zone("HBO East 4K"), FeedZone::East);
        // A name with no country marker still gets zone detection; a marker for a
        // single-time-zone country turns East/West into a region, not a feed.
        assert_eq!(extract_zone("BBC One East"), FeedZone::East);
        assert_eq!(extract_zone("BBC One West"), FeedZone::West);
        assert_eq!(extract_zone("UK: BBC One West"), FeedZone::Default);
        assert_eq!(extract_zone("UK: Sky Sports West"), FeedZone::Default);
        assert_eq!(extract_zone("US: HBO West"), FeedZone::West);
    }

    #[test]
    fn the_zone_prefilter_never_says_no_to_a_marker() {
        // `maybe_zoned` may over-approximate, never under-: a name it rejects must answer
        // `DEFAULT` on the precise path too. The corpus mixes plain names, every marker
        // spelling, place names, and non-ASCII text (where exotic case folding means only
        // the precise path may decide).
        let names = [
            "ESPN",
            "Channel 12",
            "CNN HD",
            "HBO",
            "HBO East",
            "HBO West",
            "HBO (W)",
            "HBO (E)",
            "HBO (WEST)",
            "HBO (east)",
            "HBO Pacific",
            "HBO (Pacific)",
            "HBO (PT)",
            "ESPN West HD",
            "HBO East 4K",
            "West Palm Beach",
            "WEST HASTINGS, NE",
            "WEST HARTFORD, CT",
            "Key West",
            "Western Movies",
            "Easter Parade",
            "Pacifica",
            "W",
            "E! Entertainment",
            "UK: Sky Sports West",
            "UK: BBC One West",
            "US: HBO West",
            "BBC One West",
            "Café (W)",
            "ESPN WE\u{17f}T",
            "HBO WE\u{130}ST",
            "München West",
            "São Paulo East",
            "TNT (W) \u{1c} HD",
        ];
        for name in names {
            if !maybe_zoned(name) {
                assert_eq!(
                    extract_zone(name),
                    FeedZone::Default,
                    "prefilter rejected {name:?} but the precise path zones it"
                );
            }
        }
        // And the markers the prefilter accepts still resolve on the precise path.
        assert_eq!(extract_zone("HBO West"), FeedZone::West);
        assert_eq!(extract_zone("HBO (W)"), FeedZone::West);
        assert_eq!(extract_zone("HBO East"), FeedZone::East);
        assert_eq!(extract_zone("HBO Pacific"), FeedZone::West);
        assert_eq!(extract_zone("Café (W)"), FeedZone::West);
    }

    #[test]
    fn zone_words_before_trailing_tags_still_route() {
        // Decided Z08 (step 9 review): a zone word followed by a trailing writer tag
        // is still a zone word — the zone is read after stripping the shared tag
        // vocabulary (quality, junk, status; `+1` stays a shift, not a zone).
        for (name, zone) in [
            ("HBO West (Backup)", FeedZone::West),
            ("HBO West [4K]", FeedZone::West),
            ("ITV1 (Central West) HD", FeedZone::West),
            ("HBO (West) [HD]", FeedZone::West),
            ("ESPN West +1", FeedZone::West),
            ("ESPN East (Backup)", FeedZone::East),
            ("HBO Pacific (Backup)", FeedZone::West),
        ] {
            assert_eq!(extract_zone_noguard(name), zone, "{name:?}");
        }
        // The guarded `extract_zone` stays SM's transcription: SM reads these as
        // Default (its tails cover only its own quality list), and the SM parity
        // sweep checks that function against the plugin.
        assert_eq!(extract_zone("HBO West (Backup)"), FeedZone::Default);
        assert_eq!(extract_zone("Antenna TV Pacific [HD]"), FeedZone::Default);
        // The shift survives the strip: the zone read peels `+1`, the timeshift flag
        // still reads the original name.
        assert!(crate::engine::is_time_shift("ESPN West +1"));
        assert!(!crate::engine::is_time_shift("ESPN West"));
        // Stripping never invents a zone: untagged and unknown-tagged names stay put,
        // and words merely containing a zone word are prose, not feeds.
        for name in [
            "HBO",
            "HBO (Backup)",
            "HBO [4K]",
            "CNN (Moldova)",
            "West Ham TV",
            "Westworld",
            "Western Movies",
            "Easter Parade",
        ] {
            assert_eq!(extract_zone_noguard(name), FeedZone::Default, "{name:?}");
        }
        // A whole-tag name and a leading-space tag keep their text (never stripped to
        // empty); superscript marks strip like their plain spellings.
        assert_eq!(crate::tags::strip_zone_ignorable("[HD]"), "[HD]");
        assert_eq!(crate::tags::strip_zone_ignorable("HD (Backup)"), "HD");
        assert_eq!(crate::tags::strip_zone_ignorable(" HD"), " HD");
        assert_eq!(crate::tags::strip_zone_ignorable("HBO West ᴴᴰ"), "HBO West");
        assert_eq!(
            crate::tags::strip_zone_ignorable("HBO West (Backup) (Moldova)"),
            "HBO West (Backup) (Moldova)"
        );
        assert_eq!(
            crate::tags::strip_zone_ignorable("Sky Sports (Backup) West"),
            "Sky Sports (Backup) West"
        );
    }

    #[test]
    fn pipe_segments_collapse_before_zone_and_country() {
        // Decided N15 (step-14 review): `|` residue never reaches the key or the
        // zone/country reads — junk segments drop, the rest join with a space.
        // Keys keep the zone word and the meaningful pipe part.
        let engine = crate::engine::UnifiedEngine::decided();
        // N02=core strips the bare zone word for scoring, so the key is the
        // bare channel (the step-14 spec: key `cinemax`, zone East).
        assert_eq!(engine.normalize_key("Cinemax East | HD |"), "Cinemax");
        assert_eq!(
            engine.normalize_key("Sky Sports | Premier League"),
            "Sky Sports Premier League"
        );
        assert_eq!(engine.normalize_key("UK | Sky Sports | HD"), "Sky Sports");
        // Zones read through the pipes.
        assert_eq!(extract_zone_noguard("Cinemax East | HD |"), FeedZone::East);
        assert_eq!(
            extract_zone_noguard("UK | Sky Sports West | HD"),
            FeedZone::West
        );
        assert_eq!(
            extract_zone_noguard("West Ham TV | HD |"),
            FeedZone::Default
        );
        // A leading country-code segment stays a country marker: the collapsed
        // space form reads exactly like the `:`/`|` marker forms (no regress).
        assert_eq!(lineuparr_stream_country("UK | Sky Sports"), Some("UK"));
        assert_eq!(lineuparr_stream_country("UK: Sky Sports"), Some("UK"));
        assert_eq!(lineuparr_stream_country("UK Sky Sports"), Some("UK"));
        assert_eq!(lineuparr_stream_country("UK | Sky Sports | HD"), Some("UK"));
        assert_eq!(lineuparr_stream_country("HD | UK | Sky One"), Some("UK"));
        // A bare code is not a marker (the decided rule): `USA | HD |` (USA
        // Network) collapses to `USA`, which no marker form takes.
        assert_eq!(lineuparr_stream_country("USA | HD |"), None);
        assert_eq!(lineuparr_stream_country("USA"), None);
    }

    #[test]
    fn detect_via_the_trait() {
        let detector: &dyn CountryDetector = &STREAM_MAPPARR;
        assert_eq!(detector.country_from_name("US: ESPN"), Some("US"));
        assert_eq!(detector.country_from_group("Canada"), Some("CA"));
        assert_eq!(extract_zone("HBO West"), FeedZone::West);
    }
}
