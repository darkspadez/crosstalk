//! Channel-name normalisation.
//!
//! A statement-for-statement port of `FuzzyMatcher.normalize_name` in
//! `reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/fuzzy_matcher.py`.
//!
//! It is the *plugin's* version, not `matching_core.py`'s, and the two differ in ways
//! that change output: the core strips Unicode format characters (category Cf) up front
//! and replaces decorative symbols (`So`/`No`/`Lm`/`Sk`, such as `★`) with spaces at the
//! end, while the plugin does neither. It also applies its own quality-tag table, its
//! own regional and geographic tables, and a two-token floor before stripping a trailing
//! `Network`/`Channel`/`TV`. Port the plugin's; the golden baseline in
//! `tests/fixtures/matcher_golden_baseline.json` was generated from it.
//!
//! Three details are load-bearing and easy to "improve" into bugs:
//!
//!   * Quality, resolution and geographic substitutions replace with an EMPTY string,
//!     while quality tags in brackets, regional and misc substitutions replace with a
//!     SPACE. Deleting instead of spacing glues a tag to its neighbours (`CNN [HD] USA`
//!     -> `CNNUSA`).
//!   * Whitespace is collapsed only at the very end. Collapsing earlier breaks the
//!     anchored `\s+Network$` rules.
//!   * Hyphens become spaces before the provider-prefix table runs, which is why the
//!     table's own `-` alternatives are effectively dead for the hyphen form.

use fancy_regex::Regex as FancyRegex;
use regex::Regex;
use std::sync::OnceLock;
use unicode_general_category::{get_general_category, GeneralCategory};
use unicode_names2::name as unicode_name;
use unicode_normalization::UnicodeNormalization;

/// Python's `\b`, rebuilt for the Rust engine.
///
/// A word character for Python's `\w` is `[\p{L}\p{Nd}\p{Nl}\p{No}_]`. The Rust
/// engine's `\w` is a *different* set — it adds combining marks, connector punctuation,
/// join controls and Alphabetic symbols, and omits `No`. That matters here because NFKD
/// turns every accented letter into a letter plus a combining mark, so the two engines
/// disagree about where a word boundary sits in `Télé-Québec`.
const PY_BOUNDARY: &str = concat!(
    r"(?:(?<=[\p{L}\p{Nd}\p{Nl}\p{No}_])(?![\p{L}\p{Nd}\p{Nl}\p{No}_])",
    r"|(?<![\p{L}\p{Nd}\p{Nl}\p{No}_])(?=[\p{L}\p{Nd}\p{Nl}\p{No}_]))",
);

/// Python's `\s`, which also matches the four C0 separators `\x1c`-`\x1f` that Rust's
/// Unicode `White_Space` property excludes.
const PY_SPACE: &str = r"[\s\x{1c}-\x{1f}]";

/// Rewrite a Python pattern so the Rust engine gives it Python's meaning:
/// `\b` becomes [`PY_BOUNDARY`] and `\s` becomes [`PY_SPACE`].
///
/// Every rewritten pattern must be compiled with the fancy engine, because the boundary
/// form uses lookarounds.
fn pythonize(pattern: &str) -> String {
    pattern.replace(r"\b", PY_BOUNDARY).replace(r"\s", PY_SPACE)
}

/// Python's `str.isspace()` includes the C0 separators that `char::is_whitespace` does
/// not, and `str.split()` / `str.strip()` use exactly that definition.
fn py_is_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python's `str.split()` with no arguments.
fn py_split_whitespace(s: &str) -> impl Iterator<Item = &str> {
    s.split(py_is_space).filter(|token| !token.is_empty())
}

/// Python's `str.strip()`.
fn py_trim(s: &str) -> &str {
    s.trim_matches(py_is_space)
}

/// Python's `str.lstrip()`.
fn py_trim_start(s: &str) -> &str {
    s.trim_start_matches(py_is_space)
}

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
        let widened = pythonize(pattern)
            .replace("[A-Z]", "[A-Z\u{131}\u{130}]")
            .replace("[Ii]", "[Ii\u{131}\u{130}]")
            .replace("[pi]", "[pi\u{131}\u{130}]");
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalizeFlags {
    pub ignore_quality: bool,
    pub ignore_regional: bool,
    pub ignore_geographic: bool,
    pub ignore_misc: bool,
}

impl Default for NormalizeFlags {
    fn default() -> Self {
        Self {
            ignore_quality: true,
            ignore_regional: true,
            ignore_geographic: true,
            ignore_misc: true,
        }
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

/// True for a stylised letterform or ornament that carries no meaning in a channel name.
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

    /// Remove tags, prefixes and noise so two names can be compared.
    ///
    /// `user_ignored_tags` are extra literals to strip. A tag containing brackets or
    /// parentheses is treated as a literal substring; a bare word is stripped on word
    /// boundaries; anything else as a literal substring.
    pub fn normalize_name(&self, name: &str, user_ignored_tags: &[&str]) -> String {
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
        let escaped = python_escape(tag);
        let bracketed =
            tag.contains('[') || tag.contains(']') || tag.contains('(') || tag.contains(')');
        // Python tests `re.match(r'^\w+$', tag)`, which does not match an empty tag, so
        // an empty tag takes the literal path and deletes the name's whitespace.
        let bare_word =
            !tag.is_empty() && !bracketed && tag.chars().all(|c| is_py_word_char(c) || c == '_');

        let pattern = if bare_word {
            format!("(?i){PY_BOUNDARY}{escaped}{PY_BOUNDARY}")
        } else {
            format!("(?i){escaped}{PY_SPACE}*")
        };
        // Already written against the Python-compatible constants, so it must not go
        // through `pythonize` a second time.
        match FancyRegex::new(&pattern) {
            Ok(re) => re
                .try_replacen(name, 0, "")
                .map(|cow| cow.into_owned())
                .unwrap_or_else(|_| name.to_string()),
            Err(_) => name.to_string(),
        }
    }
}

/// Python's `re.escape`, which escapes whitespace as well as the regex metacharacters.
fn python_escape(literal: &str) -> String {
    let mut out = String::with_capacity(literal.len());
    for c in literal.chars() {
        if c.is_ascii_punctuation() || c.is_whitespace() {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Python's `\w` character class, used by the `^\w+$` bare-word test.
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
}
