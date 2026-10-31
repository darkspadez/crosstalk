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
use unicode_normalization::UnicodeNormalization;

/// Stream-label variants that are not distinctive enough to distinguish channels.
/// `ABC News` should still match `ABC News Live`.
const NON_DISTINCTIVE_TOKENS: [&str; 3] = ["live", "now", "new"];

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
    // Python: r'([^\W\d_])(\d)' — a letter of any script followed by a digit.
    RE.get_or_init(|| Regex::new(r"(\p{L})(\d)").expect("static regex"))
}

fn trailing_number_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // Python: r'(?:^|\s)(\d{1,4})\s*$'. ASCII digits only: every real channel number is
    // ASCII, and Python's Unicode-digit acceptance there is an accident of `\d`.
    RE.get_or_init(|| Regex::new(r"(?:^|\s)([0-9]{1,4})\s*$").expect("static regex"))
}

/// Python's `str.isalnum()`: a letter in any script, or a digit in Nd/Nl/No.
///
/// Rust's `char::is_alphanumeric` uses the Alphabetic *derived* property, which also
/// accepts some spacing marks that Python excludes, so the categories are checked
/// directly instead.
pub fn is_alnum(c: char) -> bool {
    use unicode_general_category::{get_general_category, GeneralCategory as G};
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

/// Python's `str.isdigit()` for the purposes of these guards: an ASCII digit or any
/// other decimal digit. `t.isdigit()` is used on normalized tokens, which are ASCII.
pub fn is_digit(token: &str) -> bool {
    !token.is_empty() && token.chars().all(|c| c.is_ascii_digit() || c.is_numeric())
}

/// Length in Unicode scalar values, which is what Python's `len()` counts.
pub fn char_len(s: &str) -> usize {
    s.chars().count()
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

/// True when a token is specific enough to matter in the subset and divergent guards.
///
/// Four characters is the floor, or a two-digit number (`360` distinguishes
/// `Canal+Sport` from `Canal+Sport 360`). `live`/`now`/`new` are excluded because
/// providers add and drop them freely.
pub fn is_distinctive(token: &str) -> bool {
    if NON_DISTINCTIVE_TOKENS.contains(&token) {
        return false;
    }
    char_len(token) >= 4 || (is_digit(token) && char_len(token) >= 2)
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
///   * **subset** — one side is a strict subset and the larger side has a distinctive
///     token the smaller lacks, so the candidate is a *more specific* channel
///     (`Nickelodeon` vs `Nickelodeon Teen`);
///   * **divergent** — both sides have unique tokens and at least one is four
///     characters or longer, so they describe different brands
///     (`Sky Cinema Disney` vs `Sky Cinema Decades`);
///   * **numeric/ordinal** — both sides have their own number word or digit
///     (`BBC One` vs `BBC Two`).
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
    !tokens_a.intersection(&tokens_b).next().is_none()
}

/// The integer value of a space-separated, purely-numeric trailing token.
///
/// `HBO 2` -> `Some(2)`, `DIRECTV 4K Live 1` -> `Some(1)`, `ESPN` -> `None`, and
/// `ESPN2` -> `None` because the digit is glued to the letters. Used to reject
/// `Foo 1` against `Foo 2`.
pub fn trailing_number(name: &str) -> Option<u32> {
    trailing_number_re()
        .captures(name)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse().ok())
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
    }

    #[test]
    fn distinctive_rules() {
        assert!(is_distinctive("teen"));
        assert!(is_distinctive("360"));
        assert!(!is_distinctive("live"));
        assert!(!is_distinctive("7"));
        assert!(!is_distinctive("hd"));
    }
}
