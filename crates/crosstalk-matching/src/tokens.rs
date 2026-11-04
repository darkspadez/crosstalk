//! Token-level helpers used by the guards.
//!
//! Port of `matching_core.py`'s `process_string_for_matching`, `_has_token_overlap`,
//! `_trailing_number` and `_is_distinctive`.
//!
//! These are the guards that keep a high Levenshtein score from becoming a wrong
//! match: numbered siblings, divergent brands, and a channel that is a strict subset of
//! a more specific one. They are reproduced statement for statement, because a
//! "tidier" version changes real match outcomes.

use regex::Regex;
use std::collections::HashSet;
use std::sync::OnceLock;
use unicode_general_category::{get_general_category, GeneralCategory as G};
use unicode_normalization::UnicodeNormalization;

/// Words that carry no discriminating power on their own.
const COMMON_WORDS: [&str; 13] = [
    "the",
    "and",
    "of",
    "in",
    "on",
    "at",
    "to",
    "for",
    "a",
    "an",
    "network",
    "channel",
    "television",
];

/// Numeric and ordinal tokens that identify sibling channels.
const NUMERIC_TOKENS: [&str; 29] = [
    "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven",
    "twelve", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "first", "second",
    "third", "fourth", "fifth",
];

fn letter_digit_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // Python: r'([^\W\d_])(\d)' — one alphanumeric character that is not a digit and
    // not an underscore, followed by a digit. That class is `L* ∪ Nl ∪ No`, not `\p{L}`,
    // so `〇5` and `ᛮ3` split in Python and must split here too.
    RE.get_or_init(|| Regex::new(r"([\p{L}\p{Nl}\p{No}])(\d)").expect("static regex"))
}

fn trailing_number_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // Python: r'(?:^|\s)(\d{1,4})\s*$'. `\d` there is any Unicode decimal digit, so
    // Arabic-Indic and friends must count; the captured run is NFKC-folded to ASCII
    // before parsing, which is exactly what Python's `int()` accepts.
    RE.get_or_init(|| Regex::new(r"(?:^|\s)(\d{1,4})\s*$").expect("static regex"))
}

/// Python's `str.isalnum()`: a letter in any script, or a digit in Nd/Nl/No.
///
/// Rust's `char::is_alphanumeric` uses the Alphabetic *derived* property, which also
/// accepts some spacing marks that Python excludes, so the categories are checked
/// directly instead.
pub fn is_alnum(c: char) -> bool {
    matches!(
        get_general_category(c),
        G::UppercaseLetter
            | G::LowercaseLetter
            | G::TitlecaseLetter
            | G::ModifierLetter
            | G::OtherLetter
            | G::DecimalNumber
            | G::LetterNumber
            | G::OtherNumber
    )
}

/// Whether a token is numeric, for the single-character-token rule in majority mode.
///
/// Python uses `str.isdigit()`, which is the decimal digits (Nd) plus the `No`
/// characters whose Unicode Numeric_Type is Digit (superscript two etc.). This accepts
/// Nd only. That is deliberately narrower than Rust's `is_numeric` (Nd/Nl/No), which
/// would keep `½`, `Ⅻ` and `〇` as meaningful where Python drops them; the residual
/// difference is limited to single-character superscript tokens, which no real channel
/// name contains.
pub fn is_digit(token: &str) -> bool {
    !token.is_empty() && token.chars().all(is_digit_char)
}

/// Non-decimal characters Python's `str.isdigit()` also accepts.
///
/// `str.isdigit()` is the decimal digits (Nd) plus every character whose Unicode
/// Numeric_Type is Digit — superscripts, circled numbers, Ethiopic and Kharoshthi digits.
/// Those are category `No`, so a decimal-only test would reject `፩` and `²` where Python
/// accepts them. Generated from Python's own table; regenerate if a new script's digits
/// are added.
const DIGIT_NOT_DECIMAL_RANGES: [(u32, u32); 20] = [
    (0xB2, 0xB3),
    (0xB9, 0xB9),
    (0x1369, 0x1371),
    (0x19DA, 0x19DA),
    (0x2070, 0x2070),
    (0x2074, 0x2079),
    (0x2080, 0x2089),
    (0x2460, 0x2468),
    (0x2474, 0x247C),
    (0x2488, 0x2490),
    (0x24EA, 0x24EA),
    (0x24F5, 0x24FD),
    (0x24FF, 0x24FF),
    (0x2776, 0x277E),
    (0x2780, 0x2788),
    (0x278A, 0x2792),
    (0x10A40, 0x10A43),
    (0x10E60, 0x10E68),
    (0x11052, 0x1105A),
    (0x1F100, 0x1F10A),
];

/// Python's `str.isdigit()` for one character: decimal digits plus the Digit numeric type.
///
/// Public because the streams placeholder scan splits names on digit runs the same way the
/// matcher reads a channel number, and the two must agree about `²` and `٣`.
pub fn is_digit_char(c: char) -> bool {
    if get_general_category(c) == G::DecimalNumber {
        return true;
    }
    let cp = c as u32;
    DIGIT_NOT_DECIMAL_RANGES
        .iter()
        .any(|(start, end)| cp >= *start && cp <= *end)
}

/// Start code point of every run of ten decimal digits (Nd) in Unicode 17.
///
/// `\d` in both Python's regex engine and Rust's matches Nd, so a channel number may be
/// written in any script ("hbo ٣"). Python's `int()` converts those; Rust has no
/// equivalent, so the block starts are listed here and the value is the offset into the
/// run. Regenerate with `scripts/` tooling if a new script's digits are ever added.
const DECIMAL_BLOCK_STARTS: [u32; 77] = [
    0x0030, 0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6, 0x0B66, 0x0BE6, 0x0C66, 0x0CE6,
    0x0D66, 0x0DE6, 0x0E50, 0x0ED0, 0x0F20, 0x1040, 0x1090, 0x17E0, 0x1810, 0x1946, 0x19D0, 0x1A80,
    0x1A90, 0x1B50, 0x1BB0, 0x1C40, 0x1C50, 0xA620, 0xA8D0, 0xA900, 0xA9D0, 0xA9F0, 0xAA50, 0xABF0,
    0xFF10, 0x104A0, 0x10D30, 0x10D40, 0x11066, 0x110F0, 0x11136, 0x111D0, 0x112F0, 0x11450,
    0x114D0, 0x11650, 0x116C0, 0x116D0, 0x116DA, 0x11730, 0x118E0, 0x11950, 0x11BF0, 0x11C50,
    0x11D50, 0x11DA0, 0x11DE0, 0x11F50, 0x16130, 0x16A60, 0x16AC0, 0x16B50, 0x16D70, 0x1CCF0,
    0x1D7CE, 0x1D7D8, 0x1D7E2, 0x1D7EC, 0x1D7F6, 0x1E140, 0x1E2F0, 0x1E4F0, 0x1E5F1, 0x1E950,
    0x1FBF0,
];

/// The numeric value of a decimal digit in any script, matching what Python's `int()`
/// accepts.
pub fn decimal_digit_value(c: char) -> Option<u32> {
    if let Some(v) = c.to_digit(10) {
        return Some(v);
    }
    let cp = c as u32;
    let idx = DECIMAL_BLOCK_STARTS.partition_point(|&start| start <= cp);
    if idx == 0 {
        return None;
    }
    let offset = cp - DECIMAL_BLOCK_STARTS[idx - 1];
    if offset < 10 && get_general_category(c) == G::DecimalNumber {
        Some(offset)
    } else {
        None
    }
}

/// Length in Unicode scalar values, which is what Python's `len()` counts.
pub fn char_len(s: &str) -> usize {
    s.chars().count()
}

/// True when a token is a channel DESIGNATOR: the part of a name that tells numbered
/// siblings apart.
///
/// Digits alone (`1`, `12`) or a short stem with digits (`f1`, `e4`, `m6`, `tf1`). A word
/// such as `sports` or `hd` is not one, and neither is a bare letter. This is
/// Stream-Mapparr's rule (`is_designator_token` in its `fuzzy_matcher.py`): its numeric
/// guard compares designators rather than digits, so `Sky Sports F1` and
/// `Sky Sports UHD 1` are seen to disagree once the designator is rejoined whole.
pub fn is_designator_token(token: &str) -> bool {
    fn matches(token: &str) -> bool {
        let bytes = token.as_bytes();
        if bytes.is_empty() {
            return false;
        }
        let (stem, digits) = if bytes.iter().all(u8::is_ascii_digit) {
            ("", token)
        } else {
            let split = bytes
                .iter()
                .position(|b| b.is_ascii_digit())
                .unwrap_or(bytes.len());
            (&token[..split], &token[split..])
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        if !stem.is_empty() && (stem.len() > 3 || !stem.bytes().all(|b| b.is_ascii_lowercase())) {
            return false;
        }
        digits.len() <= 2
    }
    matches(&crate::pyre::py_trim(token).to_lowercase())
}

/// The designator tokens of a name, lowercased: Stream-Mapparr's
/// `{t.lower() for t in name.split() if is_designator_token(t)}`.
pub fn designator_tokens(name: &str) -> HashSet<String> {
    crate::pyre::py_split_whitespace(name)
        .filter(|token| is_designator_token(token))
        .map(str::to_lowercase)
        .collect()
}

/// Token-sort form used by the third fuzzy stage.
///
/// NFKD-fold, drop combining marks, lowercase, split a letter-glued digit, keep
/// alphanumerics of any script plus `+` (so `Disney+` stays distinct from `Disney`),
/// then sort the tokens. Sorting is what makes this a token-sort comparison: word order
/// stops mattering, so `News ABC` and `ABC News` collapse to the same string.
pub fn process_string_for_matching(s: &str) -> String {
    let folded: String = s
        .nfkd()
        .filter(|c| {
            !matches!(
                unicode_general_category::get_general_category(*c),
                unicode_general_category::GeneralCategory::NonspacingMark
            )
        })
        .collect();
    let lowered = folded.to_lowercase();
    let split = letter_digit_re().replace_all(&lowered, "$1 $2");

    let mut cleaned = String::with_capacity(split.len());
    for c in split.chars() {
        if is_alnum(c) || c == '+' {
            cleaned.push(c);
        } else {
            cleaned.push(' ');
        }
    }

    let mut tokens: Vec<&str> = cleaned
        .split_whitespace()
        .filter(|t| !t.is_empty())
        .collect();
    tokens.sort_unstable();
    tokens.join(" ")
}

/// True when a token is long enough to identify a channel on its own in the subset
/// guard. Five characters, exactly as in Janitor's `_has_token_overlap`; shorter extras
/// such as `live`, `now` or `two` never turn a legitimate extension into a rejection.
pub fn is_distinctive(token: &str) -> bool {
    char_len(token) >= 5
}

fn meaningful_tokens(s: &str) -> HashSet<&str> {
    s.split_whitespace()
        .filter(|t| !COMMON_WORDS.contains(t))
        .filter(|t| char_len(t) >= 2 || is_digit(t))
        .collect()
}

/// Whether two strings share enough distinctive tokens to be the same channel.
///
/// `require_majority == false` (basic mode): at least one shared token of
/// `min_token_len` or more. If either side has no such token, the guard passes
/// vacuously — there is nothing to disprove.
///
/// `require_majority == true`: more than half of the smaller token set must be shared,
/// and then three further guards run:
///   * **subset** — one side is a strict subset and the larger side has a distinctively
///     long (five characters or more) token the smaller lacks, so the candidate is a
///     *more specific* channel (`In Country Television` vs `Country Music Television`);
///   * **divergent** — both sides have unique tokens and at least one is four
///     characters or longer, so they describe different brands
///     (`Sky Cinema Disney` vs `Sky Cinema Decades`);
///   * **numeric/ordinal** — both sides have their own number word or digit
///     (`BBC One` vs `BBC Two`).
///
/// Note that Janitor's guard is *not* the one in `matching_core.py`: this plugin
/// overrides `_has_token_overlap`, and its subset rule is a plain five-character length
/// test rather than the core's `_is_distinctive`. The two disagree on real names —
/// `nickelodeon` vs `nickelodeon teen` is accepted here and rejected by the core — so
/// this ports the plugin's version.
pub fn has_token_overlap(a: &str, b: &str, min_token_len: usize, require_majority: bool) -> bool {
    if require_majority {
        let tokens_a = meaningful_tokens(a);
        let tokens_b = meaningful_tokens(b);
        if tokens_a.is_empty() || tokens_b.is_empty() {
            return true;
        }

        let shared: HashSet<&str> = tokens_a.intersection(&tokens_b).copied().collect();
        if shared.is_empty() {
            return false;
        }
        let smaller = tokens_a.len().min(tokens_b.len());
        if shared.len() * 2 <= smaller {
            return false;
        }

        let unique_a: HashSet<&str> = tokens_a.difference(&tokens_b).copied().collect();
        let unique_b: HashSet<&str> = tokens_b.difference(&tokens_a).copied().collect();

        // Subset guard.
        if unique_a.is_empty() {
            if unique_b.iter().any(|t| is_distinctive(t)) {
                return false;
            }
        } else if unique_b.is_empty() && unique_a.iter().any(|t| is_distinctive(t)) {
            return false;
        }

        // Divergent guard.
        if !unique_a.is_empty()
            && !unique_b.is_empty()
            && unique_a.union(&unique_b).any(|t| char_len(t) >= 4)
        {
            return false;
        }

        // Numeric/ordinal divergent guard.
        let has_numeric = |set: &HashSet<&str>| set.iter().any(|t| NUMERIC_TOKENS.contains(t));
        if has_numeric(&unique_a) && has_numeric(&unique_b) {
            return false;
        }

        return true;
    }

    let tokens_a: HashSet<&str> = a
        .split_whitespace()
        .filter(|t| !COMMON_WORDS.contains(t))
        .filter(|t| char_len(t) >= min_token_len)
        .collect();
    let tokens_b: HashSet<&str> = b
        .split_whitespace()
        .filter(|t| !COMMON_WORDS.contains(t))
        .filter(|t| char_len(t) >= min_token_len)
        .collect();
    if tokens_a.is_empty() || tokens_b.is_empty() {
        return true;
    }
    tokens_a.intersection(&tokens_b).next().is_some()
}

/// The integer value of a space-separated, purely-numeric trailing token.
///
/// `HBO 2` -> `Some(2)`, `DIRECTV 4K Live 1` -> `Some(1)`, `ESPN` -> `None`, and
/// `ESPN2` -> `None` because the digit is glued to the letters. Used to reject
/// `Foo 1` against `Foo 2`.
pub fn trailing_number(name: &str) -> Option<u32> {
    let captured = trailing_number_re()
        .captures(name)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())?;
    let mut value: u32 = 0;
    for c in captured.chars() {
        value = value * 10 + decimal_digit_value(c)?;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_string_sorts_and_strips() {
        assert_eq!(process_string_for_matching("ABC News"), "abc news");
        assert_eq!(process_string_for_matching("News ABC"), "abc news");
        // Leading parenthetical noise becomes a token, as in the golden baseline.
        assert_eq!(process_string_for_matching("(D1) CBS"), "1 cbs d");
        // `+` survives so Disney+ stays distinct from Disney.
        assert_eq!(process_string_for_matching("Disney+"), "disney+");
        assert_eq!(process_string_for_matching("Discovery+"), "discovery+");
        // Accents fold away.
        assert_eq!(
            process_string_for_matching("CNN en Español"),
            "cnn en espanol"
        );
    }

    #[test]
    fn process_string_keeps_non_latin_scripts() {
        assert_eq!(process_string_for_matching("Россия 1"), "1 россия");
        assert_eq!(process_string_for_matching("日本 NHK"), "nhk 日本");
    }

    #[test]
    fn basic_mode_requires_one_shared_long_token() {
        assert!(has_token_overlap("america racing", "america bbc", 4, false));
        assert!(!has_token_overlap("the one show", "the big bang", 4, false));
    }

    #[test]
    fn majority_mode_requires_majority_overlap() {
        assert!(has_token_overlap(
            "america racing",
            "america racing hd",
            4,
            true
        ));
        assert!(!has_token_overlap("america racing", "america bbc", 4, true));
        assert!(!has_token_overlap(
            "america racing sports",
            "america racing news",
            4,
            true
        ));
    }

    #[test]
    fn majority_mode_runs_numeric_and_ordinal_guards() {
        assert!(!has_token_overlap("bbc one", "bbc two", 4, true));
        assert!(!has_token_overlap("espn 1", "espn 2", 4, true));
    }

    #[test]
    fn majority_mode_rejects_divergent_brands() {
        assert!(!has_token_overlap(
            "sky cinema disney",
            "sky cinema decades",
            4,
            true
        ));
    }

    #[test]
    fn majority_mode_rejects_specific_subset() {
        assert!(!has_token_overlap(
            "in country television",
            "country music television",
            4,
            true
        ));
    }

    #[test]
    fn majority_mode_accepts_four_character_specialisation() {
        // Janitor's subset guard is a five-character test, so a four-character
        // specialisation is accepted where the shared core would reject it.
        assert!(has_token_overlap(
            "nickelodeon",
            "nickelodeon teen",
            4,
            true
        ));
        assert!(has_token_overlap("discovery", "discovery life", 4, true));
        assert!(has_token_overlap("abc", "abc kids", 4, true));
        assert!(has_token_overlap("espn", "espn 360", 4, true));
        // Five characters hits the subset guard and is rejected.
        assert!(!has_token_overlap("tv5", "tv5 monde", 4, true));
    }

    #[test]
    fn majority_mode_allows_legit_extension() {
        // The only extra token is `live`, which is non-distinctive.
        assert!(has_token_overlap("abc news", "abc news live", 4, true));
        assert!(has_token_overlap("abc news", "abc live news", 4, true));
    }

    #[test]
    fn trailing_number_cases() {
        assert_eq!(trailing_number("HBO 2"), Some(2));
        assert_eq!(trailing_number("Sky Sports News 13"), Some(13));
        assert_eq!(trailing_number("DIRECTV 4K Live 1"), Some(1));
        assert_eq!(trailing_number("ESPN"), None);
        assert_eq!(trailing_number("ESPN2"), None);
        assert_eq!(trailing_number("1"), Some(1));
        // A Unicode decimal digit is a `\d` in Python and parses through `int()`.
        assert_eq!(trailing_number("hbo ٣"), Some(3));
    }

    #[test]
    fn distinctive_rules() {
        assert!(is_distinctive("child"));
        assert!(is_distinctive("music"));
        assert!(!is_distinctive("live"));
        assert!(!is_distinctive("teen"));
        assert!(!is_distinctive("360"));
    }

    #[test]
    fn designator_token_rules() {
        for token in ["1", "12", "f1", "e4", "m6", "tf1", "F1", " f 1 ", "49"] {
            // `" f 1 "` trims to `f 1`, which is not one token: spaces are not designators.
            if token.contains(' ') {
                assert!(!is_designator_token(token), "{token:?}");
                continue;
            }
            assert!(is_designator_token(token), "{token:?}");
        }
        for token in ["sports", "hd", "f", "123", "tf123", "f1x", ""] {
            assert!(!is_designator_token(token), "{token:?}");
        }
        assert_eq!(
            designator_tokens("SKY SPORTS F 1 UHD")
                .into_iter()
                .collect::<Vec<_>>(),
            vec!["1".to_string()]
        );
        assert_eq!(
            designator_tokens("sky sports f1 uhd")
                .into_iter()
                .collect::<Vec<_>>(),
            vec!["f1".to_string()]
        );
    }

    #[test]
    fn numeric_token_rules() {
        assert!(is_digit("360"));
        // Nd of any script.
        assert!(is_digit("٣"));
        // Numeric_Type=Digit `No` characters, which Python's `isdigit()` accepts.
        assert!(is_digit("፩"));
        assert!(is_digit("²"));
        assert!(is_digit("①"));
        assert!(is_digit("𐩀"));
        // Not digits to Python: Numeric_Type=Numeric/None.
        assert!(!is_digit("½"));
        assert!(!is_digit("Ⅻ"));
        assert!(!is_digit(""));
        assert!(!is_digit("12a"));
    }
}
