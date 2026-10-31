//! US callsign extraction, the known-callsign allowlist, and location clues.
//!
//! Port of `FuzzyMatcher._compute_callsign_with_confidence` (the plugin's five-priority
//! ladder, which is not the four-priority one in `matching_core.py`),
//! `normalize_callsign`, `_load_station_callsigns`, `_get_known_callsigns`, and
//! `Plugin._extract_location` plus the network keyword from `_find_best_epg_match`.
//!
//! Why a ladder and not a regex: a US callsign is `K` or `W` plus two to four letters,
//! which is shape-identical to ordinary English words (`WITH`, `KING`, `WAVE`). Position
//! is what separates a station from a word — parentheses are a strong signal, an
//! end-of-name position is weaker, a bare mid-name token is barely a signal at all — and
//! the denylist rejects the frequent words outright.
//!
//! Two allowlists rescue real stations whose callsign is also a word: the shipped FCC
//! facility table, and the callsigns the loaded channel databases use.

use crate::pyre::{pythonize, widen_case_insensitive_i};
use fancy_regex::Regex;
use std::collections::HashSet;
use std::sync::OnceLock;

/// Words that match the callsign shape but are never US broadcast callsigns.
///
/// Copied verbatim from `matching_core._CALLSIGN_DENYLIST`. It is a denylist of
/// *frequent* words rather than a proof: `_is_callsign_allowed` pairs it with the
/// allowlist so a real station named `KING` is still recognised in parentheses.
pub const CALLSIGN_DENYLIST: [&str; 70] = [
    "EAST", "KEEN", "KEEP", "KEPT", "KEY", "KEYS", "KICK", "KID", "KIDS", "KILL", "KIND", "KING",
    "KINGS", "KISS", "KITE", "KNEE", "KNEW", "KNOW", "KNOWN", "WAR", "WARM", "WARS", "WASH",
    "WATCH", "WAVE", "WAVES", "WAY", "WAYS", "WCW", "WEB", "WEEK", "WELL", "WENT", "WERE", "WEST",
    "WHAT", "WHEN", "WHERE", "WHICH", "WHILE", "WHITE", "WHO", "WHY", "WIDE", "WIFE", "WILD",
    "WILL", "WIND", "WINE", "WING", "WINGS", "WINS", "WIRE", "WISE", "WISH", "WITH", "WOLF",
    "WOMAN", "WOMEN", "WOOD", "WORD", "WORDS", "WORK", "WORKS", "WORLD", "WORM", "WORN", "WRAP",
    "WWE", "WWF",
];

/// US state and district codes, used by the last-resort location probe.
const US_STATE_CODES: [&str; 51] = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
];

/// Geographic clue extracted from a name: a two-letter state and/or a city.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Location {
    pub state: Option<String>,
    pub city: Option<String>,
}

impl Location {
    pub fn is_empty(&self) -> bool {
        self.state.is_none() && self.city.is_none()
    }
}

/// Allows extraction to tell a real station from a callsign-shaped English word.
///
/// Built from two sources, merged, exactly as the plugin does: the shipped FCC facility
/// table (every licensed US television callsign) and the leading callsign of any name in
/// a loaded channel database that uses station format. The database source is kept
/// because it carries callsigns the FCC table does not.
#[derive(Debug, Clone, Default)]
pub struct KnownCallsigns {
    callsigns: HashSet<String>,
}

impl KnownCallsigns {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse the shipped `us_station_callsigns.json` payload.
    ///
    /// Both key spellings the file has used are read (`callsigns`, `carried_over`).
    /// Entries are uppercased and trimmed; anything unreadable yields an empty set, which
    /// costs matches rather than producing wrong ones.
    pub fn from_station_json(json: &str) -> Self {
        let mut callsigns = HashSet::new();
        let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
            return Self { callsigns };
        };
        for key in ["callsigns", "carried_over"] {
            if let Some(entries) = value.get(key).and_then(|v| v.as_array()) {
                for entry in entries {
                    if let Some(text) = entry.as_str() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            callsigns.insert(trimmed.to_uppercase());
                        }
                    }
                }
            }
        }
        Self { callsigns }
    }

    /// Add the callsigns implied by one channel-database entry.
    ///
    /// A broadcast entry carries a `callsign`; a premium entry is station format only
    /// when its name starts with a callsign followed by `(` or `-` (`KGTV (ABC)`,
    /// `WPLG-DT`). Requiring that shape is what keeps `WORLD Fishing Network` out.
    pub fn add_database_entry(&mut self, callsign: Option<&str>, name: Option<&str>) {
        if let Some(callsign) = callsign {
            let upper = callsign.trim().to_uppercase();
            if let Some(base) = leading_callsign(&upper) {
                self.callsigns.insert(base);
            }
        }
        if let Some(name) = name {
            let upper = name.trim().to_uppercase();
            if station_format(&upper) {
                if let Some(base) = leading_callsign(&upper) {
                    self.callsigns.insert(base);
                }
            }
        }
    }

    pub fn insert(&mut self, callsign: &str) {
        self.callsigns.insert(callsign.trim().to_uppercase());
    }

    pub fn contains(&self, callsign: &str) -> bool {
        self.callsigns.contains(callsign)
    }

    pub fn is_empty(&self) -> bool {
        self.callsigns.is_empty()
    }

    pub fn len(&self) -> usize {
        self.callsigns.len()
    }

    /// The shipped FCC allowlist, parsed once. `None` when the file is missing from the
    /// build, which only ever costs matches; callers fall back to the database-derived
    /// callsigns alone.
    pub fn shipped() -> Option<&'static KnownCallsigns> {
        static SHIPPED: OnceLock<Option<KnownCallsigns>> = OnceLock::new();
        SHIPPED
            .get_or_init(|| {
                let json = crate::data::US_STATION_CALLSIGNS_JSON?;
                Some(KnownCallsigns::from_station_json(json))
            })
            .as_ref()
    }
}

/// The leading `K`/`W` callsign of an uppercased station-format string.
fn leading_callsign(upper: &str) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(&format!(
            "(?i){}",
            widen_case_insensitive_i(&pythonize(r"^([KW][A-Z]{2,4})"))
        ))
        .expect("static regex")
    });
    re.captures(upper)
        .ok()
        .flatten()
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_uppercase())
}

/// Whether an uppercased name uses station format: a callsign followed by `(` or `-`.
fn station_format(upper: &str) -> bool {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(&format!(
            "(?i){}",
            widen_case_insensitive_i(&pythonize(
                r"^([KW][A-Z]{2,4})(?:-(?:TV|CD|LP|DT|LD)\d?)?\s*[(\-]"
            ))
        ))
        .expect("static regex")
    });
    re.is_match(upper).unwrap_or(false)
}

/// Compiled ladder patterns. Every one is `re.IGNORECASE` in the plugin except the two
/// provider-prefix strips, which are noted individually.
struct Ladder {
    /// `^D\d+-` — no IGNORECASE in the plugin.
    strip_d_prefix: Regex,
    /// `^USA?\s*[^a-zA-Z0-9]*\s*` — IGNORECASE.
    strip_us_prefix: Regex,
    /// Priority 1: four-letter callsign in parentheses, optional `-TV`-style suffix.
    paren_four: Regex,
    /// Priority 1b: grandfathered three-letter callsign in parentheses.
    paren_three: Regex,
    /// Priority 2: callsign with a broadcast suffix in parentheses.
    paren_suffix: Regex,
    /// Priority 3: leading callsign immediately followed by a parenthesised tag.
    leading_network: Regex,
    /// Priority 4: callsign at end of name, optional dot-suffix.
    end_of_name: Regex,
    /// Priority 5: any callsign-shaped word.
    any_word: Regex,
}

fn ladder() -> &'static Ladder {
    static LADDER: OnceLock<Ladder> = OnceLock::new();
    LADDER.get_or_init(|| {
        let ci = |pattern: &str| {
            Regex::new(&format!(
                "(?i){}",
                widen_case_insensitive_i(&pythonize(pattern))
            ))
            .unwrap_or_else(|e| panic!("bad callsign regex {pattern:?}: {e}"))
        };
        Ladder {
            strip_d_prefix: Regex::new(r"^D\d+-").expect("static regex"),
            // `^USA?\s*[^a-zA-Z0-9]*\s*`. The negated class matters: under Python's
            // `(?i)` it also excludes `ı`/`İ`, so without the widening `US ıWABC` would
            // lose its prefix and find a callsign where the plugin finds none.
            strip_us_prefix: Regex::new(&format!(
                "(?i){}",
                widen_case_insensitive_i(&pythonize(r"^USA?\s*[^a-zA-Z0-9]*\s*"))
            ))
            .expect("static regex"),
            paren_four: ci(r"\(([KW][A-Z]{3})(?:-[A-Z\s]+)?\)"),
            paren_three: ci(r"\(([KW][A-Z]{2})\)"),
            paren_suffix: ci(r"\(([KW][A-Z]{2,4}-(?:TV|CD|LP|DT|LD))\)"),
            leading_network: ci(
                r"^\s*(([KW][A-Z]{2,4})(?:-(?:TV|CD|LP|DT|LD)\d?)?)\s*\([A-Za-z0-9]+\)",
            ),
            end_of_name: ci(r"\b([KW][A-Z]{2,4}(?:-(?:TV|CD|LP|DT|LD))?)\s*(?:\.[a-z]+)?\s*$"),
            any_word: ci(r"\b([KW][A-Z]{2,4}(?:-(?:TV|CD|LP|DT|LD))?)\b"),
        }
    })
}

fn denied(callsign: &str) -> bool {
    CALLSIGN_DENYLIST.contains(&callsign)
}

/// Is this candidate acceptable as a callsign?
///
/// A denylisted word passes only when the allowlist vouches for it, which is how the real
/// stations `KING`, `WAVE`, `WOOD` and `WHO` survive.
fn allowed(callsign: &str, known: &KnownCallsigns) -> bool {
    !denied(callsign) || known.contains(callsign)
}

fn group1(caps: &fancy_regex::Captures<'_>, index: usize) -> Option<String> {
    caps.get(index).map(|m| m.as_str().to_uppercase())
}

/// Extract a US callsign and say how confident the position makes it.
///
/// High confidence means priorities 1-4: parentheses, a parenthesised suffix, a leading
/// `CALLSIGN (NETWORK)`, or end-of-name. Priority 5 — any callsign-shaped word — is low
/// confidence, because a loose mid-name token is as likely to be `WITH` as `WABC`.
///
/// The leading-`CALLSIGN (NETWORK)` form is promoted only when the allowlist confirms the
/// callsign: no denylist can bound the set of English words shaped like a callsign, so
/// `KILN (ABC)` and `WHIP (FOX)` must not be promoted, while a real station whose
/// callsign is a word (`WAVE (NBC)`) is rescued by the allowlist.
pub fn extract_callsign_with_confidence(
    channel_name: &str,
    known: &KnownCallsigns,
) -> (Option<String>, bool) {
    let l = ladder();
    let name = l.strip_d_prefix.replace(channel_name, "").into_owned();
    let name = l.strip_us_prefix.replace(&name, "").into_owned();

    // Priority 1: four-letter callsign in parentheses. Parentheses are an explicit
    // callsign signal, so a denylisted real station is rescued here.
    if let Ok(Some(caps)) = l.paren_four.captures(&name) {
        if let Some(callsign) = group1(&caps, 1) {
            if allowed(&callsign, known) {
                return (Some(callsign), true);
            }
        }
    }

    // Priority 1b: grandfathered three-letter callsigns (WWL, WJZ, KYW, WRC).
    if let Ok(Some(caps)) = l.paren_three.captures(&name) {
        if let Some(callsign) = group1(&caps, 1) {
            if allowed(&callsign, known) {
                return (Some(callsign), true);
            }
        }
    }

    // Priority 2: callsign with a broadcast suffix in parentheses. The full
    // `CALL-SUFFIX` is kept so `normalize_callsign` can strip it deterministically.
    if let Ok(Some(caps)) = l.paren_suffix.captures(&name) {
        if let Some(callsign) = group1(&caps, 1) {
            return (Some(callsign), true);
        }
    }

    // Priority 3: leading `CALLSIGN (NETWORK)`, gated on the allowlist.
    if let Ok(Some(caps)) = l.leading_network.captures(&name) {
        let base = caps.get(2).map(|m| m.as_str().to_uppercase());
        if base.as_deref().is_some_and(|b| known.contains(b)) {
            if let Some(full) = group1(&caps, 1) {
                return (Some(full), true);
            }
        }
    }

    // Priority 4: end of name. A denylisted word is NOT rescued here: end position alone
    // is too weak a signal for a word that is also a real callsign.
    if let Ok(Some(caps)) = l.end_of_name.captures(&name) {
        if let Some(callsign) = group1(&caps, 1) {
            if !denied(&callsign) {
                return (Some(callsign), true);
            }
        }
    }

    // Priority 5: any callsign-shaped word. Low confidence by construction.
    if let Ok(Some(caps)) = l.any_word.captures(&name) {
        if let Some(callsign) = group1(&caps, 1) {
            if !denied(&callsign) {
                return (Some(callsign), false);
            }
        }
    }

    (None, false)
}

/// The callsign alone, discarding the confidence flag.
pub fn extract_callsign(channel_name: &str, known: &KnownCallsigns) -> Option<String> {
    extract_callsign_with_confidence(channel_name, known).0
}

/// Remove the broadcast suffix (`-TV`, `-CD`, `-LP`, `-DT`, `-LD`) from a callsign.
///
/// Case-sensitive, as in the plugin; callers pass an already-uppercased callsign.
pub fn normalize_callsign(callsign: &str) -> String {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"-(?:TV|CD|LP|DT|LD)$").expect("static regex"));
    re.replace(callsign, "").into_owned()
}

/// The broad network label a name carries, if any: one of `ABC`, `NBC`, `CBS`, `FOX`,
/// `PBS`, `CW`, `ION`, `MNT`, `IND`, word-bounded and case-insensitive.
///
/// Only ever a tie-breaker on top of a stronger signal: a bare network keyword matches
/// too many channels to identify one (in the plugin's own measurements, 859 of 2,950
/// channels produced a network-only match that never validated).
pub fn network_keyword(channel_name: &str) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(&format!(
            "(?i){}",
            pythonize(r"\b(ABC|NBC|CBS|FOX|PBS|CW|[iIıİ]ON|MNT|[iIıİ]ND)\b")
        ))
        .expect("static regex")
    });
    let caps = re.captures(channel_name).ok().flatten()?;
    caps.get(1).map(|m| m.as_str().to_uppercase())
}

/// Extract a geographic clue: a two-letter state code and, when the name makes it
/// unambiguous, a city.
///
/// Three patterns, tried in order, exactly as the plugin does:
///   * `ABC - IL Harrisburg (WSIL)` -> state `IL`, city `Harrisburg`;
///   * `NBC (WKBW) NY Buffalo` -> state `NY`, city `Buffalo`;
///   * a bare two-letter state code from the US list, with no city.
///
/// All three are case-sensitive in the plugin, so `CBS - il Harrisburg` yields nothing.
pub fn extract_location(channel_name: &str) -> Location {
    let p1 = || -> &'static Regex {
        static RE: OnceLock<Regex> = OnceLock::new();
        RE.get_or_init(|| {
            Regex::new(&pythonize(r"-\s*([A-Z]{2})\s+([A-Za-z\s]+?)(?:\s*\(|$)"))
                .expect("static regex")
        })
    };
    let p2 = || -> &'static Regex {
        static RE: OnceLock<Regex> = OnceLock::new();
        RE.get_or_init(|| {
            Regex::new(&pythonize(
                r"(?:\([A-Z]{4}\)\s*)?([A-Z]{2})\s+([A-Za-z\s]+)",
            ))
            .expect("static regex")
        })
    };
    let p3 = || -> &'static Regex {
        static RE: OnceLock<Regex> = OnceLock::new();
        RE.get_or_init(|| Regex::new(&pythonize(r"\b([A-Z]{2})\b")).expect("static regex"))
    };

    if let Ok(Some(caps)) = p1().captures(channel_name) {
        let state = caps.get(1).map(|m| m.as_str().to_string());
        let city = caps
            .get(2)
            .map(|m| crate::pyre::py_trim(m.as_str()).to_string());
        return Location { state, city };
    }

    if let Ok(Some(caps)) = p2().captures(channel_name) {
        let state = caps.get(1).map(|m| m.as_str().to_string());
        let city = caps.get(2).map(|m| {
            let raw = m.as_str();
            // Trailing parenthesised content is not part of the city.
            let cut = raw.find('(').unwrap_or(raw.len());
            crate::pyre::py_trim(&raw[..cut]).to_string()
        });
        if let Some(city) = city {
            if city.chars().count() > 2 {
                return Location {
                    state,
                    city: Some(city),
                };
            }
        }
    }

    if let Ok(Some(caps)) = p3().captures(channel_name) {
        if let Some(state) = caps.get(1).map(|m| m.as_str().to_string()) {
            if US_STATE_CODES.contains(&state.as_str()) {
                return Location {
                    state: Some(state),
                    city: None,
                };
            }
        }
    }

    Location::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known() -> KnownCallsigns {
        let mut k = KnownCallsigns::new();
        for callsign in ["KING", "WAVE", "WOOD", "WHO", "KGTV", "WPLG"] {
            k.insert(callsign);
        }
        k
    }

    #[test]
    fn paren_four_letter_callsign_is_high_confidence() {
        assert_eq!(
            extract_callsign_with_confidence("ABC (WABC)", &known()),
            (Some("WABC".to_string()), true)
        );
        // A denylisted word spelled as a real station is rescued by the allowlist.
        assert_eq!(
            extract_callsign_with_confidence("CBS (KING)", &known()),
            (Some("KING".to_string()), true)
        );
    }

    #[test]
    fn paren_three_letter_callsign() {
        assert_eq!(
            extract_callsign_with_confidence("NBC (WWL)", &known()),
            (Some("WWL".to_string()), true)
        );
    }

    #[test]
    fn paren_suffix_and_suffixed_paren_callsigns() {
        // `(WABC-TV)` is captured by the four-letter priority, which swallows the
        // suffix, so the base callsign comes back. A suffix with no hyphen
        // (`(WABCDT)`) matches no priority at all.
        assert_eq!(
            extract_callsign_with_confidence("FOX (WABC-TV)", &known()),
            (Some("WABC".to_string()), true)
        );
        assert_eq!(
            extract_callsign_with_confidence("FOX (WABCDT)", &known()),
            (None, false)
        );
    }

    #[test]
    fn leading_callsign_then_network_needs_the_allowlist() {
        // `KGTV` is in the allowlist; `KILN` is a word shaped like a callsign.
        assert_eq!(
            extract_callsign_with_confidence("KGTV (ABC)", &known()),
            (Some("KGTV".to_string()), true)
        );
        // A callsign-shaped word is not promoted, but priority 5 still reports it at
        // low confidence, exactly as the plugin does.
        assert_eq!(
            extract_callsign_with_confidence("KILN (ABC)", &known()),
            (Some("KILN".to_string()), false)
        );
        assert_eq!(
            extract_callsign_with_confidence("WHIP (FOX)", &known()),
            (Some("WHIP".to_string()), false)
        );
    }

    #[test]
    fn end_of_name_is_high_confidence_but_not_rescued() {
        assert_eq!(
            extract_callsign_with_confidence("ABC WABC-DT", &known()),
            (Some("WABC-DT".to_string()), true)
        );
        // A denylisted word at the end is not rescued.
        assert_eq!(
            extract_callsign_with_confidence("WOLF KING", &known()),
            (None, false)
        );
    }

    #[test]
    fn loose_word_is_low_confidence() {
        assert_eq!(
            extract_callsign_with_confidence("Bizarre Foods with Andrew Zimmern", &known()),
            (None, false)
        );
        assert_eq!(
            extract_callsign_with_confidence("KOMO News", &known()),
            (Some("KOMO".to_string()), false)
        );
    }

    #[test]
    fn provider_prefixes_are_stripped_before_the_ladder() {
        assert_eq!(
            extract_callsign_with_confidence("D1-WABC-TV", &known()),
            (Some("WABC-TV".to_string()), true)
        );
        assert_eq!(
            extract_callsign_with_confidence("US: WABC", &known()),
            (Some("WABC".to_string()), true)
        );
    }

    #[test]
    fn normalize_callsign_strips_suffixes() {
        assert_eq!(normalize_callsign("WABC-TV"), "WABC");
        assert_eq!(normalize_callsign("WABC-DT"), "WABC");
        assert_eq!(normalize_callsign("WABC"), "WABC");
        assert_eq!(normalize_callsign("WABC-LD"), "WABC");
    }

    #[test]
    fn location_patterns() {
        let loc = extract_location("ABC - IL Harrisburg (WSIL)");
        assert_eq!(loc.state.as_deref(), Some("IL"));
        assert_eq!(loc.city.as_deref(), Some("Harrisburg"));

        let loc = extract_location("NBC (WKBW) NY Buffalo");
        assert_eq!(loc.state.as_deref(), Some("NY"));
        assert_eq!(loc.city.as_deref(), Some("Buffalo"));

        let loc = extract_location("CBS - OH Cleveland");
        assert_eq!(loc.state.as_deref(), Some("OH"));
        assert_eq!(loc.city.as_deref(), Some("Cleveland"));

        // A bare state code, no city.
        let loc = extract_location("ESPN NY");
        assert_eq!(loc.state.as_deref(), Some("NY"));
        assert!(loc.city.is_none());

        assert!(extract_location("HBO").is_empty());
    }

    #[test]
    fn network_keyword_is_uppercased() {
        assert_eq!(network_keyword("abc 7 news").as_deref(), Some("ABC"));
        assert_eq!(network_keyword("HBO"), None);
        // Word-bounded: `INDIANA` is not the `IND` keyword.
        assert_eq!(network_keyword("INDIANA POLITICS"), None);
        assert_eq!(network_keyword("IND TV").as_deref(), Some("IND"));
    }

    #[test]
    fn station_json_parses_both_key_spellings() {
        let known = KnownCallsigns::from_station_json(
            r#"{"callsigns": ["KAAH", " kaal "], "carried_over": ["WABC"]}"#,
        );
        assert!(known.contains("KAAH"));
        assert!(known.contains("KAAL"));
        assert!(known.contains("WABC"));
        assert_eq!(known.len(), 3);
        // Garbage degrades to an empty set rather than failing.
        assert!(KnownCallsigns::from_station_json("not json").is_empty());
    }

    #[test]
    fn database_entries_contribute_station_format_callsigns() {
        let mut known = KnownCallsigns::new();
        known.add_database_entry(Some("KAAL-TV"), None);
        known.add_database_entry(None, Some("KGTV (ABC)"));
        known.add_database_entry(None, Some("WPLG-DT"));
        // Not station format, so not a callsign.
        known.add_database_entry(None, Some("WORLD Fishing Network"));
        assert!(known.contains("KAAL"));
        assert!(known.contains("KGTV"));
        assert!(known.contains("WPLG"));
        assert!(!known.contains("WORLD"));
    }

    #[test]
    fn shipped_allowlist_loads() {
        let shipped = KnownCallsigns::shipped().expect("data/us_station_callsigns.json");
        assert!(shipped.len() > 3000, "got {}", shipped.len());
        assert!(shipped.contains("WABC"));
        assert!(
            shipped.contains("KING"),
            "a real station whose callsign is a word"
        );
    }
}
