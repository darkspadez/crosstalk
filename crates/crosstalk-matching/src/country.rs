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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn detect_via_the_trait() {
        let detector: &dyn CountryDetector = &STREAM_MAPPARR;
        assert_eq!(detector.country_from_name("US: ESPN"), Some("US"));
        assert_eq!(detector.country_from_group("Canada"), Some("CA"));
        assert_eq!(extract_zone("HBO West"), FeedZone::West);
    }
}
