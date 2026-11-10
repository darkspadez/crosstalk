//! Language-word parentheticals the engine keeps as words (decided N16).
//!
//! Every plugin's normalizer drops parenthesized and bracketed segments, which is
//! right for packaging (`(Backup)`, `[HD]`, `(CX)`) and wrong for a language: `Al
//! Jazeera (Arabic)` keyed as bare `Al Jazeera`, which every shipped alias table
//! maps to Al Jazeera English. Before keying, the engine turns a parenthetical or
//! bracketed segment whose whole content is a language word from [`LANGUAGE_WORDS`]
//! into that plain word (`Al Jazeera (Arabic)` -> `Al Jazeera Arabic`), so the
//! normalizer never sees it as a parenthetical. Two-letter codes (`(PT)`, `(EN)`)
//! are not language words: they are as often countries or zones.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::OnceLock;
use unicode_general_category::{get_general_category, GeneralCategory};
use unicode_normalization::UnicodeNormalization;

/// The language words a parenthetical may hold to stay in the name: English names
/// plus common native spellings. Compared case-insensitively and accent-tolerantly
/// ([`fold_language_word`]), so `(FRANÇAIS)` and `[francais]` both count.
///
/// Mirrored by `LANGUAGE_WORDS` in `scripts/corpus/ablate.py`; `engine_parity.py`'s
/// `language` kind checks the two agree on every word.
pub const LANGUAGE_WORDS: [&str; 38] = [
    "Arabic",
    "English",
    "French",
    "Français",
    "Spanish",
    "Español",
    "Portuguese",
    "Português",
    "German",
    "Deutsch",
    "Italian",
    "Italiano",
    "Hindi",
    "Urdu",
    "Turkish",
    "Türkçe",
    "Russian",
    "Chinese",
    "Mandarin",
    "Cantonese",
    "Korean",
    "Japanese",
    "Persian",
    "Farsi",
    "Hebrew",
    "Dutch",
    "Polish",
    "Greek",
    "Tamil",
    "Telugu",
    "Punjabi",
    "Bengali",
    "Tagalog",
    "Vietnamese",
    "Thai",
    "Swahili",
    "Somali",
    "Amharic",
];

/// NFKD, drop nonspacing marks, lowercase: the token-sort stage's accent folding
/// (`tokens::process_string_for_matching`), so `Türkçe` folds to `turkce`.
fn fold_language_word(word: &str) -> String {
    let folded: String = word
        .nfkd()
        .filter(|c| get_general_category(*c) != GeneralCategory::NonspacingMark)
        .collect();
    folded.to_lowercase()
}

fn folded_words() -> &'static HashSet<String> {
    static WORDS: OnceLock<HashSet<String>> = OnceLock::new();
    WORDS.get_or_init(|| {
        LANGUAGE_WORDS
            .iter()
            .map(|w| fold_language_word(w))
            .collect()
    })
}

/// Whether `word` (already trimmed) is one of [`LANGUAGE_WORDS`].
pub fn is_language_word(word: &str) -> bool {
    !word.is_empty() && folded_words().contains(&fold_language_word(word))
}

/// Unwrap `(Language)` / `[Language]` segments into plain words (decided N16).
///
/// A segment counts when its brackets match (`(`…`)` or `[`…`]`), it holds no other
/// bracket, and its trimmed content is a whole language word. The word replaces the
/// segment as written, separated from its neighbours by one space where they are not
/// already whitespace: `Al Jazeera (Arabic)` -> `Al Jazeera Arabic`, `CPAC(French) HD`
/// -> `CPAC French HD`. Every other parenthetical is left for the normalizer exactly
/// as before (`RTP (PT)`, `HBO (Backup)`, `TV (Arabic HD)`). Without a `(` or `[`
/// the name borrows unchanged.
///
/// Byte parity with `scripts/corpus/ablate.py::unwrap_language_brackets` is pinned
/// by `engine_parity.py` (`language` kind) and `ablate.py --self-test`.
pub fn unwrap_language_brackets(name: &str) -> Cow<'_, str> {
    if !name.contains(['(', '[']) {
        return Cow::Borrowed(name);
    }
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(name.len());
    let mut changed = false;
    let mut i = 0;
    while i < chars.len() {
        let close = match chars[i] {
            '(' => ')',
            '[' => ']',
            c => {
                out.push(c);
                i += 1;
                continue;
            }
        };
        let end = chars[i + 1..]
            .iter()
            .position(|c| matches!(c, '(' | ')' | '[' | ']'))
            .map(|offset| i + 1 + offset)
            .filter(|&j| chars[j] == close);
        let word = end.map(|j| chars[i + 1..j].iter().collect::<String>());
        match (end, word) {
            (Some(j), Some(word)) if is_language_word(word.trim()) => {
                if out.chars().next_back().is_some_and(|c| !c.is_whitespace()) {
                    out.push(' ');
                }
                out.push_str(word.trim());
                if chars.get(j + 1).is_some_and(|c| !c.is_whitespace()) {
                    out.push(' ');
                }
                changed = true;
                i = j + 1;
            }
            _ => {
                out.push(chars[i]);
                i += 1;
            }
        }
    }
    if changed {
        Cow::Owned(out)
    } else {
        Cow::Borrowed(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_parentheticals_become_words() {
        assert_eq!(
            unwrap_language_brackets("Al Jazeera (Arabic)"),
            "Al Jazeera Arabic"
        );
        assert_eq!(
            unwrap_language_brackets("Al Jazeera [English]"),
            "Al Jazeera English"
        );
        assert_eq!(
            unwrap_language_brackets("Cable Public Affairs HD Channel (French)"),
            "Cable Public Affairs HD Channel French"
        );
        assert_eq!(unwrap_language_brackets("CPAC(French)HD"), "CPAC French HD");
        assert_eq!(unwrap_language_brackets("TV5 ( Français )"), "TV5 Français");
        assert_eq!(unwrap_language_brackets("TRT (TÜRKÇE)"), "TRT TÜRKÇE");
        assert_eq!(unwrap_language_brackets("CNN (espanol)"), "CNN espanol");
    }

    #[test]
    fn other_parentheticals_are_untouched() {
        for name in [
            "RTP (PT)",
            "BBC (EN)",
            "HBO (Backup)",
            "TV (Arabic HD)",
            "Al Jazeera",
            "Al Jazeera Arabic",
            "X (Arabic]",
            "Arabic)",
        ] {
            assert!(
                matches!(unwrap_language_brackets(name), Cow::Borrowed(_)),
                "{name}"
            );
        }
        // Only the innermost matched pair unwraps.
        assert_eq!(unwrap_language_brackets("X ((Arabic))"), "X ( Arabic )");
    }
}
