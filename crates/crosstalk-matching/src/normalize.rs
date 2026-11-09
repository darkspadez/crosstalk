//! Channel-name normalisation.
//!
//! A statement-for-statement port of `FuzzyMatcher.normalize_name` in
//! `reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/fuzzy_matcher.py`, and of the
//! vendored `matching_core.py` body that Stream-Mapparr runs instead
//! (`NormalizeFlags::core_pipeline`). They are one function with two documented branches,
//! not two normalisers: the flags decide which plugin's body runs, and everything that is
//! the same text is shared.
//!
//! The default branch is the *plugin's* version, not `matching_core.py`'s, and the two
//! differ in ways that change output: the core strips Unicode format characters (category
//! Cf) up front and replaces decorative symbols (`So`/`No`/`Lm`/`Sk`, such as `★`) with
//! spaces at the end, while the plugin does neither. It also applies its own quality-tag
//! table, its own regional and geographic tables, and a two-token floor before stripping a
//! trailing `Network`/`Channel`/`TV`. The golden baseline in
//! `tests/fixtures/matcher_golden_baseline.json` was generated from the plugin's branch.
//!
//! Details that are load-bearing and easy to "improve" into bugs:
//!
//!   * Quality, resolution and geographic substitutions replace with an EMPTY string,
//!     while quality tags in brackets, regional and misc substitutions replace with a
//!     SPACE. Deleting instead of spacing glues a tag to its neighbours (`CNN [HD] USA`
//!     -> `CNNUSA`). The core branch has the same rule with its own tables.
//!   * Whitespace is collapsed only at the very end. Collapsing earlier breaks the
//!     anchored `\s+Network$` rules.
//!   * In the plugin branch, hyphens become spaces before the provider-prefix table runs,
//!     which is why the table's own `-` alternatives are effectively dead for the hyphen
//!     form. The core branch runs the table FIRST, which is why `FR - Canal+ FHD` loses
//!     its prefix there and not in the plugin.

use crate::pyre::{
    py_split_whitespace, py_trim, py_trim_start, python_escape, pythonize,
    widen_case_insensitive_i, PY_BOUNDARY, PY_SPACE,
};
use fancy_regex::Regex as FancyRegex;
use regex::Regex;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use unicode_general_category::{get_general_category, GeneralCategory};
use unicode_names2::name as unicode_name;
use unicode_normalization::UnicodeNormalization;

/// One of the two regex engines. Patterns with lookaround must go through
/// `fancy_regex`; everything else uses `regex` for its speed.
enum Pat {
    Std(Regex),
    Fancy(FancyRegex),
}

impl Pat {
    fn std(pattern: &str) -> Self {
        Pat::Std(Regex::new(pattern).unwrap_or_else(|e| panic!("bad regex {pattern:?}: {e}")))
    }

    fn fancy(pattern: &str) -> Self {
        Pat::Fancy(
            FancyRegex::new(pattern).unwrap_or_else(|e| panic!("bad regex {pattern:?}: {e}")),
        )
    }

    /// A case-insensitive Python pattern, rewritten for the Rust engine.
    ///
    /// Python's `(?i)` matches the Turkish dotless `ı` (U+0131) wherever an `i` or `I`
    /// can match, and its capital counterpart `İ` (U+0130). The Rust engine's simple
    /// case folding does not, and NFKD leaves both characters alone, so they would reach
    /// the pattern unchanged. The letter classes that contain an `i` are therefore
    /// widened explicitly. Only two literal `I`s remain in the tables (`IT` and `IN` in
    /// the country-prefix alternation), and those are written out in the pattern itself.
    fn py_ci(pattern: &str) -> Self {
        let widened = widen_case_insensitive_i(&pythonize(pattern));
        Pat::fancy(&format!("(?i){widened}"))
    }

    /// A case-sensitive Python pattern, rewritten for the Rust engine.
    fn py(pattern: &str) -> Self {
        Pat::fancy(&pythonize(pattern))
    }

    /// Replace every match with `rep`.
    ///
    /// A regex-engine failure (only reachable through the fancy engine's backtrack
    /// limit) leaves the input unchanged: this runs inside a matching job, and one
    /// unparseable name must not abort a whole run. `try_replacen` is used rather than
    /// `replace_all` because the latter unwraps that error and panics.
    fn sub(&self, haystack: &str, rep: &str) -> String {
        match self {
            Pat::Std(re) => re.replace_all(haystack, rep).into_owned(),
            Pat::Fancy(re) => re
                .try_replacen(haystack, 0, rep)
                .map(|cow| cow.into_owned())
                .unwrap_or_else(|_| haystack.to_string()),
        }
    }

    /// Replace every match with the result of `f`, applied to the matched text.
    fn sub_fn<F>(&self, haystack: &str, f: &F) -> String
    where
        F: Fn(&str) -> String,
    {
        match self {
            Pat::Std(re) => re
                .replace_all(haystack, |caps: &regex::Captures| f(&caps[0]))
                .into_owned(),
            Pat::Fancy(re) => re
                .try_replacen(haystack, 0, |caps: &fancy_regex::Captures| {
                    f(caps.get(0).map(|m| m.as_str()).unwrap_or_default())
                })
                .map(|cow| cow.into_owned())
                .unwrap_or_else(|_| haystack.to_string()),
        }
    }
}

/// Quality and status tags providers append to names. Every entry consumes the
/// whitespace around it, which is why the substitution must be a space.
const QUALITY_PATTERNS: [&str; 6] = [
    r"\s*\[(4K|8K|UHD|FHD|HD|SD|FD|Unknown|Unk|Slow|Dead|Backup)\]\s*",
    r"\s*\((4K|8K|UHD|FHD|HD|SD|FD|Unknown|Unk|Slow|Dead|Backup)\)\s*",
    r"^\s*(4K|8K|UHD|FHD|HD|SD|FD|Unknown|Unk|Slow|Dead)\b\s*",
    r"\s*\b(4K|8K|UHD|FHD|HD|SD|FD|Unknown|Unk|Slow|Dead)$",
    r"\s+\b(4K|8K|UHD|FHD|HD|SD|FD|Unknown|Unk|Slow|Dead)\b\s+",
    r"\b(?:4K|8K|UHD|FHD|HD|SD|FD|Unknown|Unk|Slow|Dead):\s",
];

/// Numeric resolution markers (`720p`, `1080i`, `3840P`). The 3-4 digit bounds exclude
/// two-digit noise and five-digit numbers, and the `p`/`i` must be glued to the digits so
/// a spaced roman numeral (`Volume 100 I`) survives.
const RESOLUTION_PATTERNS: [&str; 1] = [r"\b\d{3,4}[pi]\b"];

/// Time-zone region words that never distinguish separate feeds.
const REGIONAL_PATTERNS: [&str; 8] = [
    r"\s[Pp][Aa][Cc][Ii][Ff][Ii][Cc]",
    r"\s[Cc][Ee][Nn][Tt][Rr][Aa][Ll]",
    r"\s[Mm][Oo][Uu][Nn][Tt][Aa][Ii][Nn]",
    r"\s[Aa][Tt][Ll][Aa][Nn][Tt][Ii][Cc]",
    r"\s*\([Pp][Aa][Cc][Ii][Ff][Ii][Cc]\)\s*",
    r"\s*\([Cc][Ee][Nn][Tt][Rr][Aa][Ll]\)\s*",
    r"\s*\([Mm][Oo][Uu][Nn][Tt][Aa][Ii][Nn]\)\s*",
    r"\s*\([Aa][Tt][Ll][Aa][Nn][Tt][Ii][Cc]\)\s*",
];

/// East/West, stripped only when regional tags are ignored. With them preserved the
/// regional filter can still see which feed a name describes.
const REGIONAL_EAST_WEST_PATTERNS: [&str; 4] = [
    r"\s[Ee][Aa][Ss][Tt]\b",
    r"\s[Ww][Ee][Ss][Tt]\b",
    r"\s*\([Ee][Aa][Ss][Tt]\)\s*",
    r"\s*\([Ww][Ee][Ss][Tt]\)\s*",
];

/// Country-code prefixes and tags. The `(?!network|open)` guards protect the brands
/// `USA Network` and `US Open`, which are names rather than country tags.
const GEOGRAPHIC_PATTERNS: [&str; 6] = [
    r"\b[A-Z]{2,3}[:┃│]\s*",
    r"\b[A-Z]{2,3}\s*-\s*",
    r"(?:\|[A-Z]{2,3}\||┃[A-Z]{2,3}┃|│[A-Z]{2,3}│)\s*",
    r"\[[A-Z]{2,3}\]\s*",
    r"\bUSA?:\s(?!network\b|open\b)",
    r"\bUSA?\s(?!network\b|open\b)",
];

/// IPTV provider prefixes. `US Open` is a tennis brand, and the trailing `MEX`/`FR`/`MX`
/// set is trimmed to what the channel databases actually use.
const PROVIDER_PREFIX_PATTERNS: [&str; 6] = [
    r"^(?:US|USA|UK|CA|AU|FR|DE|ES|[iIıİ]T|NL|BR|MX|[iIıİ]N)\s*[:\-\|┃│]\s*",
    r"^\s*\((?:US|USA|UK|CA|AU|FR|DE|ES|[iIıİ]T|NL|BR|MX|[iIıİ]N)\)\s*",
    r"\s*[\|┃│]\s*(?:US|USA|UK|CA|AU|FR|DE|ES|[iIıİ]T|NL|BR|MX|[iIıİ]N)\s*$",
    r"^(?:US|UK)(?:SD|HD|FHD|UHD|FD|HEVC|4K|8K)\b\s*[:\-\|┃│]?\s*",
    r"^(?:US(?!\s+open\b)|FR|MX|MEX)\s+",
    r"^(?:RK|GO|TUBI|PLUTO|XUMO|PLEX|STIRR|FREEVEE|GLANCE)\s*[:\-\|┃│]\s*",
];

/// Miscellaneous bracketed noise. The last entry is the broad catch-all, skipped when
/// regional tags are preserved so `(East)`/`(West)` survive for the region filter.
const MISC_PATTERNS: [&str; 3] = [r"\s*\([A-Z]\)\s*", r"\s*\(CX\)\s*", r"\s*\([^)]*\)\s*"];

/// The broad catch-all in [`MISC_PATTERNS`].
const MISC_CATCH_ALL: &str = r"\s*\([^)]*\)\s*";

// ---------------------------------------------------------------------------
// `matching_core.py` (the vendored shared core) pattern tables.
//
// Stream-Mapparr subclasses the shared core and does NOT override
// `normalize_name`, so its normalisation is this table's, not the EPG Janitor
// plugin's (which overrode the method outright). The two disagree in several
// load-bearing places — provider prefixes are stripped before hyphens become
// spaces, `[HD]` is stripped even when quality tags are kept, trailing
// `Network`/`Channel`/`TV` go unconditionally, and decorative Unicode
// categories are removed — so they are kept as separate tables rather than
// merged into one mostly-shared list.
// ---------------------------------------------------------------------------

/// The shared core's quality table: `HDR`/`HEVC` are quality tags there, and there is
/// no trailing-colon form (that one belongs to EPG Janitor).
const CORE_QUALITY_PATTERNS: [&str; 5] = [
    r"\s*\[(4K|8K|UHD|FHD|HD|HDR|HEVC|SD|FD|Unknown|Unk|Slow|Dead|Backup)\]\s*",
    r"\s*\((4K|8K|UHD|FHD|HD|HDR|HEVC|SD|FD|Unknown|Unk|Slow|Dead|Backup)\)\s*",
    r"^\s*(4K|8K|UHD|FHD|HD|HDR|HEVC|SD|FD|Unknown|Unk|Slow|Dead)\b\s*",
    r"\s*\b(4K|8K|UHD|FHD|HD|HDR|HEVC|SD|FD|Unknown|Unk|Slow|Dead)$",
    r"\s+\b(4K|8K|UHD|FHD|HD|HDR|HEVC|SD|FD|Unknown|Unk|Slow|Dead)\b\s+",
];

/// The shared core strips only the parenthesised region words; bare `Pacific`/
/// `Central`/`Mountain`/`Atlantic` are brands (`Comedy Central`, `The Atlantic`).
const CORE_REGIONAL_PATTERNS: [&str; 4] = [
    r"\s*\([Pp][Aa][Cc][Ii][Ff][Ii][Cc]\)\s*",
    r"\s*\([Cc][Ee][Nn][Tt][Rr][Aa][Ll]\)\s*",
    r"\s*\([Mm][Oo][Uu][Nn][Tt][Aa][Ii][Nn]\)\s*",
    r"\s*\([Aa][Tt][Ll][Aa][Nn][Tt][Ii][Cc]\)\s*",
];

/// The shared core's country-prefix table, in its own order: delimited prefixes, bare
/// country words, the `USA ` brand guard, a country glued to a quality tag, a balanced
/// delimiter wrap, a trailing bar tag, content categories, then FAST platform tags.
const CORE_PROVIDER_PREFIX_PATTERNS: [&str; 8] = [
    r"^(?:US|USA|UK|CA|AU|FR|DE|ES|[iI\u{131}\u{130}]T|NL|BR|MX|[iI\u{131}\u{130}]N)\s*[:\-\|┃│]\s*",
    r"^(?:US|UK|CA|AU|FR|DE|MX|MEX|FRA|GER)\s+",
    r"^USA\s+(?!NETWORK\b)",
    r"^(?:US|UK)(?:SD|HD|FHD|UHD|FD|HEVC|4K|8K)\b\s*[:\-\|┃│]?\s*",
    r"^\s*(?:\(\s*(?:US|USA|UK|CA|AU|FR|DE|ES|IT|NL|BR|MX|IN)\s*\)|\[\s*(?:US|USA|UK|CA|AU|FR|DE|ES|IT|NL|BR|MX|IN)\s*\]|\|\s*(?:US|USA|UK|CA|AU|FR|DE|ES|IT|NL|BR|MX|IN)\s*\||┃\s*(?:US|USA|UK|CA|AU|FR|DE|ES|IT|NL|BR|MX|IN)\s*┃|│\s*(?:US|USA|UK|CA|AU|FR|DE|ES|IT|NL|BR|MX|IN)\s*│)\s*",
    r"\s*[\|┃│]\s*(?:US|USA|UK|CA|AU|FR|DE|ES|IT|NL|BR|MX|IN)\s*$",
    r"^(?:ADULT|EROTIC|PRIME|GOLD)\s*[:\-\|┃│]\s*",
    r"^(?:RK|GO|TUBI|PLUTO|XUMO|PLEX|STIRR|FREEVEE|GLANCE)\s*[:\-\|┃│]\s*",
];

/// The shared core's country tag table: no `USA:`/`USA ` brand guards (those live in its
/// provider-prefix table) and the same four marker shapes as EPG Janitor's.
const CORE_GEOGRAPHIC_PATTERNS: [&str; 4] = [
    r"\b[A-Z]{2,3}[:┃│]\s*",
    r"\b[A-Z]{2,3}\s*-\s*",
    r"(?:\|[A-Z]{2,3}\||┃[A-Z]{2,3}┃|│[A-Z]{2,3}│)\s*",
    r"\[[A-Z]{2,3}\]\s*",
];

/// The shared core's only miscellaneous pattern. It runs only when BOTH `ignore_misc` and
/// `ignore_regional` are set, so a preserved `(East)` stays visible to the region filter.
const CORE_MISC_PATTERNS: [&str; 1] = [r"\s*\([^)]*\)\s*"];

/// The bracketed country tag, which doubles as a quality tag (`[HD]`) when quality tags
/// are preserved.
const GEO_BRACKET: &str = r"\[[A-Z]{2,3}\]\s*";

/// Spelled-out numbers fold to digits so `BBC Three` and `BBC 3` share tokens.
const NUM_WORDS_RE: &str = r"\b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b";

/// A leading box-bar bouquet tag (`┃CANAL+┃ NPO 1`). Box bars never occur in real names.
const LEADING_BAR_TAG_RE: &str = r"^\s*[┃│]\s*[^┃│]*[┃│]\s*";

/// Callsigns in parentheses, stripped when regional tags are ignored.
const CALLSIGN_PAREN_RE: &str = r"\([KW][A-Z]{3}(?:-(?:TV|CD|LP|DT|LD))?\)";

/// The same, with the region abbreviations excluded when they are being preserved.
const CALLSIGN_PAREN_RE_KEEP_REGION: &str =
    r"\([KW](?!EST\)|ACIFIC\)|ENTRAL\)|OUNTAIN\)|TLANTIC\))[A-Z]{3}(?:-(?:TV|CD|LP|DT|LD))?\)";

/// Any remaining short parenthesised group, stripped only with regional tags ignored.
const PAREN_ALNUM_RE: &str = r"\([A-Z0-9]+\)";

/// Zero-width and invisible code points that only add noise.
const ZERO_WIDTH: [char; 2] = ['\u{FE0F}', '\u{200D}'];

/// Emoji that visually stand in for a letter when embedded inside a word.
const EMOJI_LETTERS: [(char, char); 1] = [('\u{26BD}', 'o')];

/// Pictographic ornaments removed wherever they appear.
const EMOJI_ORNAMENTS: [char; 3] = ['\u{266C}', '\u{263E}', '\u{26BD}'];

/// The flags that decide how much is stripped. Defaults are Janitor's shipped defaults.
///
/// The first four are the plugin's four toggles. The rest exist because EPG Janitor and
/// Stream-Mapparr normalise through different code: Janitor overrides `normalize_name`
/// outright, Stream-Mapparr runs the vendored shared core and only repairs designators
/// on top of it. Each flag below names the plugin that needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalizeFlags {
    pub ignore_quality: bool,
    pub ignore_regional: bool,
    pub ignore_geographic: bool,
    pub ignore_misc: bool,
    /// Stream-Mapparr needs this: run `matching_core.py`'s `normalize_name` body instead
    /// of EPG Janitor's plugin override. The two differ in provider-prefix ordering,
    /// quality tables, East/West and region handling, trailing-suffix stripping and the
    /// decorative-Unicode strip, none of which is a toggle in either plugin.
    pub core_pipeline: bool,
    /// Stream-Mapparr needs this: rejoin a designator the core split apart, so `F1` and
    /// `TF1` stay whole instead of becoming `F 1` (its issue #50 fix). Only a split the
    /// raw name itself wrote glued is undone; `Sky Sports 1` stays two tokens.
    pub rejoin_designators: bool,
    /// Stream-Mapparr needs this: strip a leading `Cinemax` (the shared core's opt-in
    /// `remove_cinemax`), which its match passes for a channel whose name mentions `max`
    /// so `Cinemax` and `MoreMax` can meet. EPG Janitor never sets it.
    pub remove_cinemax: bool,
    /// Unified engine (N05=janitor-split): fold `&` to ` and ` inside the core
    /// pipeline. The shared core never does this; Janitor does. The decided
    /// engine is the core body plus this statement.
    pub split_ampersand: bool,
    /// Unified engine (N06=janitor-letters): split dots only between ASCII
    /// letters (`JusticeCentral.TV`), not between word characters (`97.2`
    /// keeps its dot). The shared core splits on `\\w`; Janitor narrows it.
    pub dots_letters_only: bool,
    /// Unified engine (N07=janitor-floor): strip a trailing
    /// `Network`/`Channel`/`TV` only when at least two tokens would remain
    /// (`Justice Network` keeps its suffix). The shared core strips
    /// unconditionally.
    pub suffix_floor: bool,
}

impl Default for NormalizeFlags {
    fn default() -> Self {
        Self {
            ignore_quality: true,
            ignore_regional: true,
            ignore_geographic: true,
            ignore_misc: true,
            core_pipeline: false,
            rejoin_designators: false,
            remove_cinemax: false,
            split_ampersand: false,
            dots_letters_only: false,
            suffix_floor: false,
        }
    }
}

impl NormalizeFlags {
    /// The `user_ignored_tags` list a run passes, in the plugin's order.
    pub fn ignored_tags(&self) -> Vec<String> {
        let mut tags = Vec::new();
        if self.ignore_quality {
            tags.push("quality".to_string());
        }
        if self.ignore_regional {
            tags.push("regional".to_string());
        }
        if self.ignore_geographic {
            tags.push("geographic".to_string());
        }
        if self.ignore_misc {
            tags.push("misc".to_string());
        }
        tags
    }
}

struct Tables {
    quality: Vec<Pat>,
    resolution: Vec<Pat>,
    regional: Vec<Pat>,
    regional_east_west: Vec<Pat>,
    geographic: Vec<Pat>,
    provider_prefixes: Vec<Pat>,
    misc: Vec<Pat>,
    num_words: Pat,
    leading_bar_tag: Pat,
    callsign_paren: Pat,
    callsign_paren_keep_region: Pat,
    paren_alnum: Pat,
    the_prefix: Pat,
    trailing_network: Pat,
    trailing_channel: Pat,
    trailing_tv: Pat,
    letter_digit: Pat,
    digit_letter: Pat,
    hyphen: Pat,
    dot_between_letters: Pat,
    camel_lower_upper_lower: Pat,
    camel_acronym: Pat,
    leading_paren: Pat,
    ampersand: Pat,
    whitespace: Pat,
}

fn tables() -> &'static Tables {
    static TABLES: OnceLock<Tables> = OnceLock::new();
    TABLES.get_or_init(|| Tables {
        quality: QUALITY_PATTERNS.iter().map(|p| Pat::py_ci(p)).collect(),
        resolution: RESOLUTION_PATTERNS.iter().map(|p| Pat::py_ci(p)).collect(),
        regional: REGIONAL_PATTERNS.iter().map(|p| Pat::py_ci(p)).collect(),
        regional_east_west: REGIONAL_EAST_WEST_PATTERNS
            .iter()
            .map(|p| Pat::py_ci(p))
            .collect(),
        geographic: GEOGRAPHIC_PATTERNS.iter().map(|p| Pat::py_ci(p)).collect(),
        provider_prefixes: PROVIDER_PREFIX_PATTERNS
            .iter()
            .map(|p| Pat::py_ci(p))
            .collect(),
        misc: MISC_PATTERNS.iter().map(|p| Pat::py_ci(p)).collect(),
        num_words: Pat::py_ci(NUM_WORDS_RE),
        leading_bar_tag: Pat::py(LEADING_BAR_TAG_RE),
        callsign_paren: Pat::py_ci(CALLSIGN_PAREN_RE),
        callsign_paren_keep_region: Pat::py_ci(CALLSIGN_PAREN_RE_KEEP_REGION),
        // Case-SENSITIVE in the plugin: `re.sub(r'\([A-Z0-9]+\)', '', name)` has no
        // re.IGNORECASE, so `(Spurs)` and `(abc)` survive while `(W)` does not.
        paren_alnum: Pat::py(PAREN_ALNUM_RE),
        the_prefix: Pat::py_ci(r"^The\s+"),
        trailing_network: Pat::py_ci(r"\s+Network\s*$"),
        trailing_channel: Pat::py_ci(r"\s+Channel\s*$"),
        trailing_tv: Pat::py_ci(r"\s+TV\s*$"),
        // Case-sensitive ASCII pairs, exactly as written in the plugin.
        letter_digit: Pat::std(r"([a-zA-Z])(\d)"),
        digit_letter: Pat::std(r"(\d)([a-zA-Z])"),
        hyphen: Pat::std("-"),
        dot_between_letters: Pat::fancy(r"(?<=[A-Za-z])\.(?=[A-Za-z])"),
        camel_lower_upper_lower: Pat::std(r"([a-z])([A-Z][a-z])"),
        camel_acronym: Pat::py(r"([a-z]{4,})([A-Z]{2,})\b"),
        leading_paren: Pat::py(r"^\s*\([^)]+\)\s*"),
        ampersand: Pat::py(r"\s*&\s*"),
        whitespace: Pat::py(r"\s+"),
    })
}

/// `matching_core.py`'s pattern set, compiled once. Only the tables that differ from
/// EPG Janitor's live here; the shared ones (resolution markers, number words, the
/// callsign and parenthesis strips, the ASCII spacers and the camel-case splits) are
/// the same text and are reused from [`tables`].
struct CoreTables {
    quality: Vec<Pat>,
    regional: Vec<Pat>,
    geographic: Vec<Pat>,
    provider_prefixes: Vec<Pat>,
    misc: Vec<Pat>,
    region_paren_east_west: Pat,
    region_bare_east_west: Pat,
    dot_between_word_chars: Pat,
    the_prefix: Pat,
    cinemax: Pat,
}

/// Python's word character, `[^\W]`: every alphanumeric category plus `_`.
const PY_WORD_CLASS: &str = r"[\p{L}\p{Nd}\p{Nl}\p{No}_]";

fn core_tables() -> &'static CoreTables {
    static TABLES: OnceLock<CoreTables> = OnceLock::new();
    TABLES.get_or_init(|| CoreTables {
        quality: CORE_QUALITY_PATTERNS
            .iter()
            .map(|p| Pat::py_ci(p))
            .collect(),
        regional: CORE_REGIONAL_PATTERNS
            .iter()
            .map(|p| Pat::py_ci(p))
            .collect(),
        geographic: CORE_GEOGRAPHIC_PATTERNS
            .iter()
            .map(|p| Pat::py_ci(p))
            .collect(),
        provider_prefixes: CORE_PROVIDER_PREFIX_PATTERNS
            .iter()
            .map(|p| Pat::py_ci(p))
            .collect(),
        misc: CORE_MISC_PATTERNS.iter().map(|p| Pat::py_ci(p)).collect(),
        // `\((eastern|east|e|west|w))` and the bare words, both when regional tags go.
        region_paren_east_west: Pat::py_ci(r"\(\s*(?:eastern|east|e|west|w)\s*\)"),
        region_bare_east_west: Pat::py_ci(r"\b(?:eastern|east|west)\b"),
        // `(?<=\w)\.(?=\w)`; `\w` is Python's, not the Rust engine's.
        dot_between_word_chars: Pat::fancy(&format!("(?<={PY_WORD_CLASS})\\.(?={PY_WORD_CLASS})")),
        // `^\s*The\s+`: the leading `\s*` matters because the substitutions above leave
        // the name starting with a space once a prefix is removed.
        the_prefix: Pat::py_ci(r"^\s*The\s+"),
        cinemax: Pat::py_ci(r"\bCinemax\b\s*"),
    })
}

/// The split of a glued designator the core made: `"F 1"`, `"TF 1"`.
fn rejoin_designator_re() -> &'static FancyRegex {
    static RE: OnceLock<FancyRegex> = OnceLock::new();
    RE.get_or_init(|| {
        FancyRegex::new(r"(?<![A-Za-z0-9])([A-Za-z]{1,2}) ([0-9]{1,2})(?![A-Za-z0-9])")
            .expect("static regex")
    })
}

/// Designators written glued in the RAW name, lowercased. Only these may be rejoined.
fn source_designator_re() -> &'static FancyRegex {
    static RE: OnceLock<FancyRegex> = OnceLock::new();
    RE.get_or_init(|| {
        FancyRegex::new(r"(?<![A-Za-z0-9])[A-Za-z]{1,2}[0-9]{1,2}").expect("static regex")
    })
}

///
/// Detection is by Unicode character *name*, so it covers superscripts, modifier-letter
/// superscript capitals and Latin small capitals wherever they live, including outside
/// the obvious blocks.
pub fn is_decorative_char(c: char) -> bool {
    if c.is_ascii() {
        return false;
    }
    if c == '\u{25C9}' {
        // FISHEYE, whose Unicode name carries no decoration keyword.
        return true;
    }
    let Some(nm) = unicode_name(c).map(|n| n.to_string()) else {
        // An unnamed code point (control character, lone surrogate) is not decoration.
        return false;
    };
    nm.contains("SUPERSCRIPT")
        || nm.contains("SUBSCRIPT")
        || nm.contains("SMALL CAPITAL")
        || nm.contains("MODIFIER LETTER")
}

/// Drop whitespace tokens that are pure stylised decoration, then NFKD-canonicalise.
///
/// A token is decoration when it has at least one decorative character, no ASCII
/// alphanumeric, and every character is decorative or ASCII punctuation. Real ASCII
/// words (`Gold`, `VIP`) and non-Latin letters (Arabic, Cyrillic, CJK) are always kept.
pub fn strip_stylized_tokens(name: &str) -> String {
    if name.is_ascii() {
        return name.to_string();
    }
    let mut kept: Vec<&str> = Vec::new();
    for token in py_split_whitespace(name) {
        let has_decorative = token.chars().any(is_decorative_char);
        let has_ascii_alnum = token.chars().any(|c| c.is_ascii_alphanumeric());
        let only_decorative_or_punct = token
            .chars()
            .all(|c| is_decorative_char(c) || (c.is_ascii() && !c.is_ascii_alphanumeric()));
        if has_decorative && only_decorative_or_punct && !has_ascii_alnum {
            continue;
        }
        kept.push(token);
    }
    kept.join(" ").nfkd().collect()
}

/// Map emoji-as-letters to their letter and strip emoji decoration.
///
/// The substitution fires only when the glyph is flanked by ASCII letters, so `SP⚽RTS`
/// becomes `SPoRTS` while a standalone `⚽` is treated as decoration and dropped.
pub fn normalize_emoji(name: &str) -> String {
    if name.is_ascii() {
        return name.to_string();
    }
    let stripped: String = name.chars().filter(|c| !ZERO_WIDTH.contains(c)).collect();

    let chars: Vec<char> = stripped.chars().collect();
    let mut with_letters = String::with_capacity(stripped.len());
    for (i, &c) in chars.iter().enumerate() {
        let mapped = EMOJI_LETTERS
            .iter()
            .find(|(glyph, _)| *glyph == c)
            .and_then(|(_, l)| {
                let prev_ascii = i > 0 && chars[i - 1].is_ascii_alphabetic();
                let next_ascii = chars.get(i + 1).is_some_and(|n| n.is_ascii_alphabetic());
                (prev_ascii && next_ascii).then_some(*l)
            });
        with_letters.push(mapped.unwrap_or(c));
    }

    with_letters
        .chars()
        .filter(|c| !EMOJI_ORNAMENTS.contains(c))
        .collect()
}

/// Normalises channel and guide names for matching.
///
/// Cheap to clone and safe to share: the compiled pattern tables are process-wide, so
/// only the flags live in the value.
#[derive(Debug, Clone, Copy, Default)]
pub struct Normalizer {
    flags: NormalizeFlags,
}

impl Normalizer {
    pub fn new(flags: NormalizeFlags) -> Self {
        Self { flags }
    }

    pub fn flags(&self) -> NormalizeFlags {
        self.flags
    }

    /// The same flags with `matching_core`'s `remove_cinemax` cleared.
    ///
    /// Stream-Mapparr's collector sets that opt-in from the CHANNEL name (`'max' in
    /// `channel_name`), but the channel side itself is never stripped: `plugin.py`
    /// cleans the channel name without it and `fuzzy_match`/`find_best_match` normalise
    /// the query without it. Only the candidate side can carry it, and even there the
    /// precomputed index is built without it (the plugin's `precompute_normalizations`
    /// takes no such flag); it reaches only `find_best_match`'s uncached-candidate
    /// fallback. Clearing it here reproduces that asymmetry, exactly as in the plugin.
    pub fn without_cinemax(&self) -> Self {
        self.with_remove_cinemax(false)
    }

    /// The same flags with `matching_core`'s `remove_cinemax` set as requested.
    ///
    /// Used for the one path that honours it: a candidate the index did not precompute
    /// (a name normalising to under two characters), which the plugin normalises
    /// on the fly with the call's flag.
    pub fn with_remove_cinemax(&self, remove_cinemax: bool) -> Self {
        Self {
            flags: NormalizeFlags {
                remove_cinemax,
                ..self.flags
            },
        }
    }

    /// Remove tags, prefixes and noise so two names can be compared.
    ///
    /// `user_ignored_tags` are extra literals to strip. A tag containing brackets or
    /// parentheses is treated as a literal substring; a bare word is stripped on word
    /// boundaries; anything else as a literal substring.
    pub fn normalize_name(&self, name: &str, user_ignored_tags: &[&str]) -> String {
        let normalized = if self.flags.core_pipeline {
            self.normalize_core(name, user_ignored_tags)
        } else {
            self.normalize_plugin(name, user_ignored_tags)
        };
        if self.flags.rejoin_designators && !normalized.is_empty() {
            rejoin_designators(&normalized, name)
        } else {
            normalized
        }
    }

    /// `matching_core.py`'s `normalize_name`, statement for statement.
    ///
    /// This is the body Stream-Mapparr runs (it subclasses the shared core and does not
    /// override normalisation). It differs from EPG Janitor's override in more than one
    /// place, all of them reproduced here rather than tidied into agreement: provider
    /// prefixes before hyphen normalisation, the `HDR`/`HEVC` quality table with no
    /// colon form, a Unicode-word dot split, `(e)`/`(w)`/`eastern`/`east`/`west`
    /// stripping, `[HD]` stripped as a country tag even when quality tags are kept, the
    /// misc catch-all gated on both flags, unconditional trailing
    /// `Network`/`Channel`/`TV` stripping, and the decorative-Unicode strip at the end.
    fn normalize_core(&self, name: &str, user_ignored_tags: &[&str]) -> String {
        let t = core_tables();
        let shared = tables();
        let f = self.flags;

        // Zero-width/invisible format characters (category Cf) are padding, so they are
        // removed rather than spaced.
        let mut s: String = name
            .chars()
            .filter(|c| get_general_category(*c) != GeneralCategory::Format)
            .collect();

        s = shared.leading_bar_tag.sub(&s, "");
        s = normalize_emoji(&s);
        s = strip_stylized_tokens(&s);

        // Provider prefixes run BEFORE hyphens become spaces, so `FR - Canal+ FHD` loses
        // its `FR - ` while the separator is still a hyphen.
        for pattern in &t.provider_prefixes {
            s = pattern.sub(&s, "");
        }

        if f.ignore_quality {
            // Chained markers (`4K HDR`) expose another tag on the next pass.
            loop {
                let previous = s.clone();
                for pattern in &shared.resolution {
                    s = pattern.sub(&s, "");
                }
                for pattern in &t.quality {
                    s = pattern.sub(&s, " ");
                }
                if previous == s {
                    break;
                }
            }
        }

        s = shared.letter_digit.sub(&s, "$1 $2");
        s = shared.digit_letter.sub(&s, "$1 $2");
        s = shared.hyphen.sub(&s, " ");
        if f.dots_letters_only {
            s = shared.dot_between_letters.sub(&s, " ");
        } else {
            s = t.dot_between_word_chars.sub(&s, " ");
        }
        s = shared.num_words.sub_fn(&s, &number_word_as_digit);
        s = shared.camel_lower_upper_lower.sub(&s, "$1 $2");
        s = shared.camel_acronym.sub(&s, "$1 $2");
        if f.split_ampersand {
            s = shared.ampersand.sub(&s, " and ");
        }

        if f.ignore_regional {
            s = t.region_paren_east_west.sub(&s, " ");
            s = t.region_bare_east_west.sub(&s, " ");
        }

        while py_trim_start(&s).starts_with('(') {
            let next = shared.leading_paren.sub(&s, "");
            if next == s {
                break;
            }
            s = next;
        }

        // The shared core's other opt-in, `remove_country_prefix`, is never set by any
        // shipping plugin, so it is not a flag here.
        if f.remove_cinemax {
            s = t.cinemax.sub(&s, "");
        }

        if f.ignore_regional {
            for pattern in &t.regional {
                s = pattern.sub(&s, " ");
            }
        }
        if f.ignore_geographic {
            for pattern in &t.geographic {
                s = pattern.sub(&s, " ");
            }
        }
        // The catch-all would also strip `(East)`/`(West)`, which the region filter still
        // needs when regional tags are preserved.
        if f.ignore_misc && f.ignore_regional {
            for pattern in &t.misc {
                s = pattern.sub(&s, " ");
            }
        }

        for tag in user_ignored_tags {
            s = self.apply_ignored_tag(&s, tag);
        }

        if f.ignore_regional {
            s = shared.callsign_paren.sub(&s, "");
        } else {
            s = shared.callsign_paren_keep_region.sub(&s, "");
        }

        if f.ignore_regional {
            s = shared.paren_alnum.sub(&s, "");
        }

        // N07: the decided engine keeps Janitor's two-token floor; the raw core
        // strips unconditionally.
        s = t.the_prefix.sub(&s, "");
        if f.suffix_floor {
            for pattern in [
                &shared.trailing_network,
                &shared.trailing_channel,
                &shared.trailing_tv,
            ] {
                let stripped = pattern.sub(&s, "");
                let stripped = py_trim(&stripped);
                if !stripped.is_empty() && py_split_whitespace(stripped).count() >= 2 {
                    s = stripped.to_string();
                }
            }
        } else {
            s = shared.trailing_network.sub(&s, "");
            s = shared.trailing_channel.sub(&s, "");
            s = shared.trailing_tv.sub(&s, "");
        }

        // Decorative Unicode badges (`◉`, `²`, superscript `ᴿᴬᵂ`) become spaces.
        s = s
            .chars()
            .map(|c| if is_decorator_category(c) { ' ' } else { c })
            .collect();

        py_trim(&shared.whitespace.sub(&s, " ")).to_string()
    }

    /// EPG Janitor's own `normalize_name`, the plugin override (not the shared core's).
    fn normalize_plugin(&self, name: &str, user_ignored_tags: &[&str]) -> String {
        let t = tables();
        let f = self.flags;
        let mut s = name.to_string();

        s = t.leading_bar_tag.sub(&s, "");
        s = normalize_emoji(&s);
        s = strip_stylized_tokens(&s);

        if f.ignore_quality {
            // Resolution markers go first: the digit/letter spacer below would otherwise
            // split `3840P` into `3840 P` and break the word-boundary anchors.
            for pattern in &t.resolution {
                s = pattern.sub(&s, "");
            }
            for pattern in &t.quality {
                s = pattern.sub(&s, " ");
            }
        }

        // Spacing around numbers, so `MTV2` and `MTV 2` tokenise alike.
        s = t.letter_digit.sub(&s, "$1 $2");
        s = t.digit_letter.sub(&s, "$1 $2");

        s = t.hyphen.sub(&s, " ");

        s = t.num_words.sub_fn(&s, &number_word_as_digit);

        s = t.dot_between_letters.sub(&s, " ");

        // CamelCase splits. The four-character floor on the acronym rule protects short
        // brands like `MeTV`, whose existing matches depend on the un-split form.
        s = t.camel_lower_upper_lower.sub(&s, "$1 $2");
        s = t.camel_acronym.sub(&s, "$1 $2");

        while py_trim_start(&s).starts_with('(') {
            let next = t.leading_paren.sub(&s, "");
            if next == s {
                break;
            }
            s = next;
        }

        for pattern in &t.provider_prefixes {
            s = pattern.sub(&s, "");
        }

        s = t.ampersand.sub(&s, " and ");

        if f.ignore_regional {
            for pattern in &t.regional {
                s = pattern.sub(&s, " ");
            }
            for pattern in &t.regional_east_west {
                s = pattern.sub(&s, " ");
            }
        }

        if f.ignore_geographic {
            for (i, pattern) in t.geographic.iter().enumerate() {
                // A bracketed quality tag ([HD]) must survive when quality tags are kept.
                if !f.ignore_quality && GEOGRAPHIC_PATTERNS[i] == GEO_BRACKET {
                    continue;
                }
                s = pattern.sub(&s, "");
            }
        }

        if f.ignore_misc {
            for (i, pattern) in t.misc.iter().enumerate() {
                // The catch-all would also strip (East)/(West), which the regional
                // filter needs when regional tags are preserved.
                if MISC_PATTERNS[i] == MISC_CATCH_ALL && !f.ignore_regional {
                    continue;
                }
                s = pattern.sub(&s, " ");
            }
        }

        for tag in user_ignored_tags {
            s = self.apply_ignored_tag(&s, tag);
        }

        if f.ignore_regional {
            s = t.callsign_paren.sub(&s, "");
        } else {
            s = t.callsign_paren_keep_region.sub(&s, "");
        }

        if f.ignore_regional {
            s = t.paren_alnum.sub(&s, "");
        }

        // `^The ` first, then trailing brand suffixes only when at least two tokens
        // would remain: stripping unconditionally turns `Justice Network` into
        // `Justice`, which then matches `Justice Central`.
        s = t.the_prefix.sub(&s, "");
        for pattern in [&t.trailing_network, &t.trailing_channel, &t.trailing_tv] {
            let stripped = pattern.sub(&s, "");
            let stripped = py_trim(&stripped);
            if !stripped.is_empty() && py_split_whitespace(stripped).count() >= 2 {
                s = stripped.to_string();
            }
        }

        // The removals above can glue a digit to a token (`ABC 7 (KGO) SAN` ->
        // `ABC 7SAN`), which breaks the dominant US OTA name shape, so re-space.
        s = t.letter_digit.sub(&s, "$1 $2");
        s = t.digit_letter.sub(&s, "$1 $2");

        py_trim(&t.whitespace.sub(&s, " ")).to_string()
    }

    /// Strip one user-configured tag.
    ///
    /// Three paths, exactly as the plugin takes them: a tag with brackets or parentheses
    /// is a literal substring; a bare word (Python `^\w+$`) is stripped on Python word
    /// boundaries; anything else is a literal substring. An empty tag falls through to
    /// the literal path, where Python's `\s*` pattern deletes every whitespace character
    /// rather than doing nothing.
    fn apply_ignored_tag(&self, name: &str, tag: &str) -> String {
        let Some(re) = tag_regex(tag) else {
            return name.to_string();
        };
        re.try_replacen(name, 0, "")
            .map(|cow| cow.into_owned())
            .unwrap_or_else(|_| name.to_string())
    }
}

/// Rejoin digit stems the shared core split apart, keeping `F1`/`TF1` whole.
///
/// Stream-Mapparr's issue #50: the core splits a letter from a following digit
/// unconditionally, so `Sky Sports F1` became `Sky Sports F 1` and the manufactured bare
/// `1` then read as a numbered sibling of `Sky Sports UHD 1`. Rejoining restores the
/// designator; the numeric-sibling guard in the matcher then refuses the pair. Only a
/// split the RAW name itself wrote glued is undone (`High Street TV 1` stays apart),
/// which is what keeps this a fix for a handful of names rather than a rewrite of every
/// numbered one.
fn rejoin_designators(normalized: &str, original: &str) -> String {
    let glued: std::collections::HashSet<String> = source_designator_re()
        .find_iter(original)
        .filter_map(Result::ok)
        .map(|m| m.as_str().to_lowercase())
        .collect();
    if glued.is_empty() {
        return normalized.to_string();
    }

    let mut out = String::with_capacity(normalized.len());
    let mut last = 0;
    for captures in rejoin_designator_re().captures_iter(normalized) {
        let Ok(captures) = captures else { continue };
        let Some(whole) = captures.get(0) else {
            continue;
        };
        let (Some(stem), Some(digits)) = (captures.get(1), captures.get(2)) else {
            continue;
        };
        out.push_str(&normalized[last..whole.start()]);
        let joined = format!("{}{}", stem.as_str(), digits.as_str());
        if glued.contains(&joined.to_lowercase()) {
            out.push_str(&joined);
        } else {
            out.push_str(whole.as_str());
        }
        last = whole.end();
    }
    out.push_str(&normalized[last..]);
    out
}

/// How many distinct tags [`tag_regex`] keeps compiled. Tags come from settings (a handful
/// per run), so this is never reached in practice; it bounds the cache if a caller ever feeds
/// it names instead of tags. Past it the cache starts over.
const TAG_REGEX_CACHE_MAX: usize = 4096;

type TagMap = HashMap<String, Option<std::sync::Arc<FancyRegex>>>;

fn tag_cache() -> &'static Mutex<TagMap> {
    static CACHE: OnceLock<Mutex<TagMap>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The cache, even after a panic elsewhere poisoned it: it only ever holds complete
/// entries, so a poisoned guard is still a consistent map.
fn lock_tag_cache() -> std::sync::MutexGuard<'static, TagMap> {
    tag_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Compile (and cache) the pattern for one user-configured tag.
///
/// The cache is not an optimisation detail: `fancy_regex` compilation costs tens of
/// milliseconds, and a run normalises tens of thousands of names against the same
/// handful of tags, so compiling per call would dominate the run. It holds at most
/// [`TAG_REGEX_CACHE_MAX`] tags.
fn tag_regex(tag: &str) -> Option<std::sync::Arc<FancyRegex>> {
    if let Some(cached) = lock_tag_cache().get(tag) {
        return cached.clone();
    }

    let bracketed =
        tag.contains('[') || tag.contains(']') || tag.contains('(') || tag.contains(')');
    // Python tests `re.match(r'^\w+$', tag)`, which does not match an empty tag, so an
    // empty tag takes the literal path and deletes the name's whitespace.
    let bare_word =
        !tag.is_empty() && !bracketed && tag.chars().all(|c| is_py_word_char(c) || c == '_');

    let literal = ci_literal(tag);
    let pattern = if bare_word {
        format!("(?i){PY_BOUNDARY}{literal}{PY_BOUNDARY}")
    } else {
        format!("(?i){literal}{PY_SPACE}*")
    };
    let compiled = FancyRegex::new(&pattern).ok().map(std::sync::Arc::new);

    remember(
        &mut lock_tag_cache(),
        tag,
        compiled.clone(),
        TAG_REGEX_CACHE_MAX,
    );
    compiled
}

/// Insert into a cache holding at most `max` entries, starting over when it is full.
fn remember<V>(cache: &mut HashMap<String, V>, key: &str, value: V, max: usize) {
    if cache.len() >= max && !cache.contains_key(key) {
        cache.clear();
    }
    cache.insert(key.to_string(), value);
}

/// Escape a literal for a case-insensitive pattern, keeping Python's `(?i)` meaning for
/// the letters `i`/`I`, which also match the Turkish dotless `ı` and dotted `İ`.
fn ci_literal(literal: &str) -> String {
    let mut out = String::with_capacity(literal.len() + 4);
    for c in literal.chars() {
        match c {
            'i' | 'I' | '\u{131}' | '\u{130}' => out.push_str("[iI\u{131}\u{130}]"),
            other => out.push_str(&python_escape(&other.to_string())),
        }
    }
    out
}
fn is_py_word_char(c: char) -> bool {
    matches!(
        get_general_category(c),
        GeneralCategory::UppercaseLetter
            | GeneralCategory::LowercaseLetter
            | GeneralCategory::TitlecaseLetter
            | GeneralCategory::ModifierLetter
            | GeneralCategory::OtherLetter
            | GeneralCategory::DecimalNumber
            | GeneralCategory::LetterNumber
            | GeneralCategory::OtherNumber
    )
}

/// Spelled-out number to digit, matching the plugin's `NUM_WORDS` table. A word outside
/// the twelve is returned unchanged, which cannot happen because the pattern only
/// matches those twelve.
fn number_word_as_digit(word: &str) -> String {
    let lowered = word.to_ascii_lowercase();
    let digit = match lowered.as_str() {
        "one" => "1",
        "two" => "2",
        "three" => "3",
        "four" => "4",
        "five" => "5",
        "six" => "6",
        "seven" => "7",
        "eight" => "8",
        "nine" => "9",
        "ten" => "10",
        "eleven" => "11",
        "twelve" => "12",
        other => other,
    };
    digit.to_string()
}

/// The general categories Janitor does *not* strip, exported for tests that pin the
/// difference from `matching_core.py` (which turns these into spaces).
pub fn is_decorator_category(c: char) -> bool {
    matches!(
        get_general_category(c),
        GeneralCategory::OtherSymbol
            | GeneralCategory::OtherNumber
            | GeneralCategory::ModifierLetter
            | GeneralCategory::ModifierSymbol
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_on(name: &str) -> String {
        Normalizer::default().normalize_name(name, &[])
    }

    #[test]
    fn provider_and_country_prefixes() {
        assert_eq!(all_on("US: USA Network HD"), "USA Network");
        assert_eq!(all_on("US| ESPN"), "ESPN");
        assert_eq!(all_on("[US] CNN"), "CNN");
        assert_eq!(all_on("UK: BBC One"), "BBC 1");
        assert_eq!(all_on("┃US┃ ESPN"), "ESPN");
    }

    #[test]
    fn country_tag_never_eats_a_brand() {
        // `USA Network` and `US Open` are names, not country tags.
        assert_eq!(all_on("US: USA Network HD"), "USA Network");
        assert_eq!(all_on("US Open"), "US Open");
        assert_eq!(all_on("US Open Tennis"), "US Open Tennis");
        // A genuine country tag still goes.
        assert_eq!(all_on("US Armenian TV"), "Armenian TV");
        // `GER TV` must not collapse to `TV`.
        assert!(all_on("GER TV").to_lowercase().contains("ger"));
    }

    #[test]
    fn quality_and_resolution_tags() {
        assert_eq!(all_on("HBO HD"), "HBO");
        assert_eq!(all_on("ESPN [FHD]"), "ESPN");
        assert_eq!(all_on("Discovery Channel 4K"), "Discovery Channel");
        assert_eq!(all_on("ESPN 1080p"), "ESPN");
        // A bare number is not a resolution marker.
        assert_eq!(all_on("Channel 4"), "Channel 4");
        // A spaced roman numeral is not a resolution marker either.
        assert_eq!(all_on("Volume 100 I"), "Volume 100 I");
    }

    #[test]
    fn brackets_do_not_glue_neighbours() {
        // The substitutions consume their surrounding whitespace, so they must insert a
        // space: deleting would produce `CNNUSA`.
        assert_eq!(all_on("CNN [HD] USA"), "CNN USA");
        assert_eq!(
            all_on("Big Ten Network (Southern California) Alternate"),
            "Big 10 Network Alternate"
        );
        assert_eq!(all_on("Penthouse (TEN) On Demand"), "Penthouse On Demand");
    }

    #[test]
    fn regional_tags_follow_the_flag() {
        assert_eq!(all_on("HBO East"), "HBO");
        assert_eq!(all_on("ESPN Pacific"), "ESPN");
        assert_eq!(all_on("Fox Sports West"), "Fox Sports");
        let keep = Normalizer::new(NormalizeFlags {
            ignore_regional: false,
            ..NormalizeFlags::default()
        });
        assert_eq!(keep.normalize_name("HBO East", &[]), "HBO East");
        assert_eq!(keep.normalize_name("HBO West", &[]), "HBO West");
        // `(W)` is removed by the single-letter misc rule even here, exactly as in the
        // plugin: the misc table runs whether or not regional tags are preserved.
        assert_eq!(keep.normalize_name("HBO (W)", &[]), "HBO");
    }

    #[test]
    fn number_words_and_camel_case() {
        assert_eq!(all_on("BBC Three"), "BBC 3");
        assert_eq!(
            all_on("Three Angels Broadcasting Network"),
            "3 Angels Broadcasting"
        );
        assert_eq!(all_on("JusticeCentral.TV"), "Justice TV");
        assert_eq!(all_on("DangerTV"), "Danger TV");
        assert_eq!(all_on("NewsNation"), "News Nation");
        // Short brands keep their un-split form.
        assert_eq!(all_on("getTV"), "getTV");
    }

    #[test]
    fn trailing_suffix_needs_two_tokens_left() {
        assert_eq!(all_on("True Crime Network"), "True Crime");
        // `Justice Network` would lose its only qualifying token, so `Network` stays.
        assert_eq!(all_on("Justice Network"), "Justice Network");
        assert_eq!(all_on("Justice Central.TV"), "Justice TV");
    }

    #[test]
    fn stylised_unicode_and_emoji() {
        // Plain ASCII `RAW` is a word, so it survives while `UHD` is stripped.
        assert_eq!(all_on("TNT UHD RAW"), "TNT RAW");
        // A superscript `RAW` is a pure decoration token and is dropped whole.
        let raw = "\u{1D3F}\u{1D2C}\u{1D42}";
        assert_eq!(all_on(&format!("TNT UHD {raw}")), "TNT");
        // A soccer ball between ASCII letters stands in for `o`.
        let ball = '\u{26BD}';
        assert_eq!(all_on(&format!("beIN SP{ball}RTS")), "beIN SPoRTS");
    }

    #[test]
    fn decorative_symbols_are_kept() {
        // Janitor's normaliser has no decorator-category strip (the shared core does),
        // so a star survives. This is pinned because it is easy to "fix" by accident.
        assert_eq!(all_on("★ CNN ★"), "★ CNN ★");
        assert!(is_decorator_category('★'));
    }

    #[test]
    fn non_latin_names_are_preserved() {
        assert_eq!(all_on("Россия 1"), "Россия 1");
        assert_eq!(all_on("beИN SPORTS"), "beИN SPORTS");
    }

    #[test]
    fn user_ignored_tags() {
        let n = Normalizer::default();
        // A bare word is stripped on word boundaries.
        assert_eq!(
            n.normalize_name("Sky Sports Racing", &["Racing"]),
            "Sky Sports"
        );
        // A bracketed tag is a literal substring.
        assert_eq!(n.normalize_name("CNN [FOO] USA", &["[FOO]"]), "CNN USA");
        // A tag whose text only exists BEFORE the digit/letter spacer runs cannot match
        // afterwards (`F1` becomes `F 1`), so it is stripped only in its spaced form.
        assert_eq!(
            n.normalize_name("Sky Sports F1 Racing", &["F1"]),
            "Sky Sports F 1 Racing"
        );
        assert_eq!(
            n.normalize_name("Sky Sports F1 Racing", &["F 1"]),
            "Sky Sports Racing"
        );
    }

    /// Helper-level cases transcribed from `tests/test_normalization_port.py`, which
    /// pins the three input-cleaning fixes (stylised-Unicode strip, emoji-as-letter,
    /// numeric resolution markers).
    #[test]
    fn input_cleaning_helpers() {
        let ball = '\u{26BD}';
        assert_eq!(normalize_emoji(&format!("SP{ball}RTS")), "SPoRTS");
        // An edge ball is decoration, not the letter `o`.
        assert_eq!(normalize_emoji(&format!("BE{ball}")), "BE");
        assert_eq!(normalize_emoji("\u{FE0F}HULU"), "HULU");
        assert_eq!(normalize_emoji("Fox Sports"), "Fox Sports");

        let raw = "\u{1D3F}\u{1D2C}\u{1D42}";
        assert_eq!(
            strip_stylized_tokens(&format!("WEATHERNATION {raw}")),
            "WEATHERNATION"
        );
        // An ASCII tier word is not decoration and stays.
        assert_eq!(strip_stylized_tokens(&format!("Gold {raw}")), "Gold");
        assert_eq!(strip_stylized_tokens("Fox Sports 1"), "Fox Sports 1");

        assert!(is_decorative_char('\u{1D3F}'));
        assert!(!is_decorative_char('G'));
        assert!(!is_decorative_char('4'));
    }

    #[test]
    fn ampersand_becomes_and() {
        assert_eq!(all_on("U&Yesterday"), "U and Yesterday");
    }

    #[test]
    fn the_tag_cache_is_bounded_and_survives_a_poisoned_lock() {
        // Past the cap the cache starts over instead of growing.
        let mut cache: HashMap<String, u32> = HashMap::new();
        for n in 0..10 {
            remember(&mut cache, &format!("tag{n}"), n, 3);
            assert!(cache.len() <= 3, "{cache:?}");
        }
        assert_eq!(cache.get("tag9"), Some(&9));
        remember(&mut cache, "tag9", 90, 3);
        assert_eq!(
            cache.get("tag9"),
            Some(&90),
            "a known key is replaced, not evicted"
        );

        // A panic while the lock is held poisons it; the next lookup still answers.
        let _ = std::thread::spawn(|| {
            let _guard = tag_cache().lock();
            panic!("poison the tag cache");
        })
        .join();
        assert!(tag_regex("HD").is_some());
        assert!(
            tag_regex("HD").is_some(),
            "and again, from the poisoned cache"
        );
    }
}

#[cfg(test)]
mod core_branch_tests {
    use super::*;

    /// Stream-Mapparr's normaliser: the shared core's body plus the designator repair.
    fn stream_mapparr() -> Normalizer {
        Normalizer::new(NormalizeFlags {
            core_pipeline: true,
            rejoin_designators: true,
            ..NormalizeFlags::default()
        })
    }

    fn all_on(name: &str) -> String {
        stream_mapparr().normalize_name(name, &[])
    }

    /// Every expected value here is `matching_core.py`'s output for the same input
    /// (plus the plugin's designator repair). The sweep checks this at scale; these pin
    /// the load-bearing differences so a plain `cargo test` catches a regression.
    #[test]
    fn core_normalisation_differs_from_the_plugin_where_it_should() {
        // Provider prefixes run BEFORE hyphens become spaces, so the hyphen is still a
        // separator when `FR - ` is removed.
        assert_eq!(all_on("FR - Canal+ FHD"), "Canal+");
        // Trailing Network/Channel/TV go unconditionally (no two-token floor).
        assert_eq!(all_on("Justice Network"), "Justice");
        assert_eq!(all_on("True Crime Network"), "True Crime");
        assert_eq!(all_on("West TV"), "");
        assert_eq!(all_on("Slow TV"), "");
        // `USA Network` loses `Network` here, and `US Open` loses the country word.
        assert_eq!(all_on("US: USA Network HD"), "USA");
        assert_eq!(all_on("US Open"), "Open");
        // Ampersands are left alone (the plugin folds them to "and").
        assert_eq!(all_on("U&Yesterday"), "U&Yesterday");
        assert_eq!(all_on("A&E"), "A&E");
        // Decorative Unicode categories become spaces; zero-width format characters go.
        assert_eq!(all_on("★ CNN ★"), "CNN");
        assert_eq!(all_on("◉ CNN"), "CNN");
        assert_eq!(all_on("Channel \u{b2}"), "Channel");
        assert_eq!(all_on("Ч\u{200b}\u{200b} CNN"), "Ч CNN");
        // HDR/HEVC are quality tags in the core, and chained markers strip in passes.
        assert_eq!(all_on("ESPN 4K HDR"), "ESPN");
        assert_eq!(all_on("TNT UHD RAW"), "TNT RAW");
        // A dot between word characters splits, and camel case still splits.
        assert_eq!(all_on("JusticeCentral.TV"), "Justice Central");
        assert_eq!(all_on("NewsNation"), "News Nation");
        assert_eq!(all_on("getTV"), "getTV");
        assert_eq!(all_on("BBC Three"), "BBC 3");
    }

    #[test]
    fn core_quality_geographic_gating() {
        // The core strips `[HD]` as a country tag even when quality tags are kept; the
        // plugin skips that pattern in the same situation.
        let keep_quality = Normalizer::new(NormalizeFlags {
            core_pipeline: true,
            ignore_quality: false,
            ..NormalizeFlags::default()
        });
        assert_eq!(keep_quality.normalize_name("CNN [HD] USA", &[]), "CNN USA");
        assert_eq!(keep_quality.normalize_name("HBO HD", &[]), "HBO HD");
        let plugin = Normalizer::new(NormalizeFlags {
            ignore_quality: false,
            ..NormalizeFlags::default()
        });
        assert_eq!(plugin.normalize_name("CNN [HD] USA", &[]), "CNN [HD] USA");
    }

    #[test]
    fn core_region_and_misc_gating() {
        let keep_region = Normalizer::new(NormalizeFlags {
            core_pipeline: true,
            ignore_regional: false,
            ..NormalizeFlags::default()
        });
        // `(W)` and bare East/West survive when regional tags are kept...
        assert_eq!(keep_region.normalize_name("HBO (W)", &[]), "HBO (W)");
        assert_eq!(
            keep_region.normalize_name("BBC One West", &[]),
            "BBC 1 West"
        );
        // ...and so does the misc catch-all, because the core gates it on BOTH flags.
        let keep_misc = Normalizer::new(NormalizeFlags {
            core_pipeline: true,
            ignore_misc: false,
            ..NormalizeFlags::default()
        });
        assert_eq!(
            keep_misc.normalize_name("Big Ten Network (Southern California) Alternate", &[]),
            "Big 10 Network (Southern California) Alternate"
        );
        assert_eq!(
            keep_misc.normalize_name("Penthouse (TEN) On Demand", &[]),
            "Penthouse On Demand"
        );
    }

    #[test]
    fn designator_repair_only_undoes_a_split_the_raw_name_had() {
        assert_eq!(all_on("Sky Sports F1 UHD"), "Sky Sports F1");
        // The manufactured bare `1` is what used to make the sibling look matchable.
        assert_eq!(all_on("Sky Sports UHD 1"), "Sky Sports 1");
        // Nothing the source wrote apart is joined.
        assert_eq!(all_on("High Street TV 1"), "High Street TV 1");
        assert_eq!(all_on("That's 60s"), "That's 60 s");
        assert_eq!(all_on("at 49ers"), "at 49 ers");
        // Without the flag the core's split stands.
        let no_repair = Normalizer::new(NormalizeFlags {
            core_pipeline: true,
            ..NormalizeFlags::default()
        });
        assert_eq!(
            no_repair.normalize_name("Sky Sports F1 UHD", &[]),
            "Sky Sports F 1"
        );
        // The repair is driven by the RAW name: `F1` there is what licenses joining the
        // normalized `F 1`. Writing `F 1` apart in the source changes nothing by itself.
        assert_eq!(all_on("Sky Sports F 1"), "Sky Sports F 1");
    }

    #[test]
    fn cinemax_is_an_opt_in() {
        assert_eq!(all_on("Cinemax Action"), "Cinemax Action");
        let cinemax = Normalizer::new(NormalizeFlags {
            core_pipeline: true,
            rejoin_designators: true,
            remove_cinemax: true,
            ..NormalizeFlags::default()
        });
        assert_eq!(cinemax.normalize_name("Cinemax Action", &[]), "Action");
        assert_eq!(cinemax.normalize_name("Cinemax", &[]), "");
    }
}
