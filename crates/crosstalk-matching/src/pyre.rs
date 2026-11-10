//! Emulation of the few Python `re` semantics that the Rust engine does not share.
//!
//! The matcher is a port of a Python implementation, and four differences between the
//! two engines change output on real data:
//!
//!   1. `\w` (and therefore `\b`) is a *different* character set. Python's is
//!      `[alnum_]`, the Rust engine's adds combining marks, connector punctuation, join
//!      controls and Alphabetic symbols and omits `No`. This bites on any accented name,
//!      because NFKD splits `é` into `e` plus a combining mark.
//!   2. `\s` in Python also matches the four C0 separators `\x1c`-`\x1f`, which Rust's
//!      `White_Space` property excludes. So do `str.split()`, `str.strip()` and friends.
//!   3. `(?i)` in Python matches the Turkish dotless `ı` (U+0131) wherever `i` or `I` can
//!      match, and `İ` (U+0130). Rust's simple case folding matches neither.
//!   4. `re.escape` also escapes whitespace, which matters for user-supplied tags.
//!
//! Everything here exists so the ported patterns behave as they do in Python. Patterns
//! that are rewritten by [`pythonize`] must be compiled with `fancy_regex`, because the
//! boundary form uses lookarounds.

/// Python's `\b`, rebuilt for the Rust engine.
pub const PY_BOUNDARY: &str = concat!(
    r"(?:(?<=[\p{L}\p{Nd}\p{Nl}\p{No}_])(?![\p{L}\p{Nd}\p{Nl}\p{No}_])",
    r"|(?<![\p{L}\p{Nd}\p{Nl}\p{No}_])(?=[\p{L}\p{Nd}\p{Nl}\p{No}_]))",
);

/// Python's `\s`, which also matches `\x1c`-`\x1f`.
pub const PY_SPACE: &str = r"[\s\x{1c}-\x{1f}]";

/// Rewrite a Python pattern so the Rust engine gives it Python's meaning.
pub fn pythonize(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() * 2);
    let mut chars = pattern.chars().peekable();
    let mut in_class = false;

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                // Inside a class `\b` is a backspace, not a word boundary: leave it.
                Some('b') if !in_class => out.push_str(PY_BOUNDARY),
                Some('s') => {
                    if in_class {
                        out.push_str(r"\s\x{1c}-\x{1f}");
                    } else {
                        out.push_str(PY_SPACE);
                    }
                }
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
            continue;
        }
        match c {
            '[' if !in_class => {
                in_class = true;
                out.push(c);
            }
            ']' if in_class => {
                in_class = false;
                out.push(c);
            }
            other => out.push(other),
        }
    }
    out
}

/// Widen the letter classes that contain an `i` so Python's `(?i)` dotless-`ı` behaviour
/// is reproduced. Paired with a `(?i)` prefix.
pub fn widen_case_insensitive_i(pattern: &str) -> String {
    const EXTRAS: &str = "\u{131}\u{130}";

    let mut out = String::with_capacity(pattern.len() + 16);
    let mut chars = pattern.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '[' {
            out.push(c);
            continue;
        }

        // Buffer the whole class so the decision is made once, on its full text.
        let mut class = String::from("[");
        let mut closed = false;
        for inner in chars.by_ref() {
            class.push(inner);
            if inner == ']' {
                closed = true;
                break;
            }
        }
        if !closed {
            out.push_str(&class);
            break;
        }

        let already = class.contains('\u{131}') || class.contains('\u{130}') || class.contains('ı');
        if already {
            out.push_str(&class);
            continue;
        }

        let mut widened = String::with_capacity(class.len() + 16);
        let mut previous_was_range = false;
        let inner = class.chars();
        for ch in inner {
            widened.push(ch);
            match ch {
                'i' | 'I' => {
                    widened.push_str(EXTRAS);
                    previous_was_range = false;
                }
                // `A-Z` / `a-z` under Python's (?i) admits the Turkish pair as well.
                'Z' | 'z' if previous_was_range => {
                    widened.push_str(EXTRAS);
                    previous_was_range = false;
                }
                '-' => previous_was_range = true,
                _ => previous_was_range = false,
            }
        }
        out.push_str(&widened);
    }
    out
}

/// Python's `str.isspace()`, which includes the C0 separators.
pub fn py_is_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python's `str.split()` with no arguments.
pub fn py_split_whitespace(s: &str) -> impl Iterator<Item = &str> {
    s.split(py_is_space).filter(|token| !token.is_empty())
}

/// Python's `str.strip()`.
pub fn py_trim(s: &str) -> &str {
    s.trim_matches(py_is_space)
}

/// Python's `str.lstrip()`.
pub fn py_trim_start(s: &str) -> &str {
    s.trim_start_matches(py_is_space)
}

/// Python's `str.splitlines()`: splits on `\n`, `\r`, `\r\n`, `\v`, `\f`, `\x1c`-`\x1e`,
/// `\x85`, U+2028 and U+2029.
///
/// The extra terminators matter for the M3U and CSV readers, which use `splitlines()`:
/// a lone `\r` or a U+2028 inside a name splits a line in Python and not in Rust, which
/// changes the parsed stream set.
pub fn py_splitlines(text: &str) -> Vec<&str> {
    fn is_break(c: char) -> bool {
        matches!(
            c,
            '\n' | '\r'
                | '\u{b}'
                | '\u{c}'
                | '\u{1c}'
                | '\u{1d}'
                | '\u{1e}'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        )
    }

    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((index, c)) = chars.next() {
        if !is_break(c) {
            continue;
        }
        lines.push(&text[start..index]);
        // `\r\n` is a single break.
        if c == '\r' {
            if let Some((next_index, '\n')) = chars.peek().copied() {
                chars.next();
                start = next_index + 1;
                continue;
            }
        }
        start = index + c.len_utf8();
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

/// Python's `re.escape`.
///
/// Python escapes the regex metacharacters plus ASCII whitespace, and nothing else. That
/// distinction is load-bearing here: escaping a character the Rust engine gives a meaning
/// changes the pattern, and `<` is the case that bites — `\<` is a word-boundary
/// assertion in `fancy_regex`, so an escaped `<HD>` tag would never match.
pub fn python_escape(literal: &str) -> String {
    const SPECIAL: &str = r"()[]{}?*+-|^$\.&~#";
    let mut out = String::with_capacity(literal.len());
    for c in literal.chars() {
        if SPECIAL.contains(c) || (c.is_ascii() && c.is_ascii_whitespace()) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Python's `str.casefold()`, which alias lookup uses for its keys.
///
/// Casefold is Unicode *full case folding* (the C+F mappings), not `to_lowercase`:
/// `ß` folds to `ss`, the Greek final sigma to plain sigma, Cherokee lowercase letters
/// to the uppercase letters, and the micro sign to Greek mu. Rust's standard library
/// has no casefold, so the 183 code points whose fold differs from their full lowercase
/// mapping are tabulated; every other code point folds exactly as it lowercases.
///
/// The table is the set of code points where, for a single character, Python's
/// `chr(cp).casefold()` differs from `chr(cp).lower()` and the latter is one character;
/// `scripts/` may regenerate it by walking `range(0x110000)` and printing the sorted
/// `(lower, casefold)` pairs as Rust escapes. The fold of the lowered text is then
/// exact for every code point (checked exhaustively when the table was built).
pub fn py_casefold(text: &str) -> String {
    let lowered = text.to_lowercase();
    let mut out = String::with_capacity(lowered.len());
    for c in lowered.chars() {
        match casefold_exception(c) {
            Some(replacement) => out.push_str(replacement),
            None => out.push(c),
        }
    }
    out
}

/// The full-casefold replacement for a code point already lowercased, if it differs.
fn casefold_exception(c: char) -> Option<&'static str> {
    // Sorted by code point, so the lookup is a binary search.
    const TABLE: [(char, &str); 183] = [
        ('\u{b5}', "\u{3bc}"),
        ('\u{df}', "\u{73}\u{73}"),
        ('\u{149}', "\u{2bc}\u{6e}"),
        ('\u{17f}', "\u{73}"),
        ('\u{1f0}', "\u{6a}\u{30c}"),
        ('\u{345}', "\u{3b9}"),
        ('\u{390}', "\u{3b9}\u{308}\u{301}"),
        ('\u{3b0}', "\u{3c5}\u{308}\u{301}"),
        ('\u{3c2}', "\u{3c3}"),
        ('\u{3d0}', "\u{3b2}"),
        ('\u{3d1}', "\u{3b8}"),
        ('\u{3d5}', "\u{3c6}"),
        ('\u{3d6}', "\u{3c0}"),
        ('\u{3f0}', "\u{3ba}"),
        ('\u{3f1}', "\u{3c1}"),
        ('\u{3f5}', "\u{3b5}"),
        ('\u{587}', "\u{565}\u{582}"),
        ('\u{13f8}', "\u{13f0}"),
        ('\u{13f9}', "\u{13f1}"),
        ('\u{13fa}', "\u{13f2}"),
        ('\u{13fb}', "\u{13f3}"),
        ('\u{13fc}', "\u{13f4}"),
        ('\u{13fd}', "\u{13f5}"),
        ('\u{1c80}', "\u{432}"),
        ('\u{1c81}', "\u{434}"),
        ('\u{1c82}', "\u{43e}"),
        ('\u{1c83}', "\u{441}"),
        ('\u{1c84}', "\u{442}"),
        ('\u{1c85}', "\u{442}"),
        ('\u{1c86}', "\u{44a}"),
        ('\u{1c87}', "\u{463}"),
        ('\u{1c88}', "\u{a64b}"),
        ('\u{1e96}', "\u{68}\u{331}"),
        ('\u{1e97}', "\u{74}\u{308}"),
        ('\u{1e98}', "\u{77}\u{30a}"),
        ('\u{1e99}', "\u{79}\u{30a}"),
        ('\u{1e9a}', "\u{61}\u{2be}"),
        ('\u{1e9b}', "\u{1e61}"),
        ('\u{1f50}', "\u{3c5}\u{313}"),
        ('\u{1f52}', "\u{3c5}\u{313}\u{300}"),
        ('\u{1f54}', "\u{3c5}\u{313}\u{301}"),
        ('\u{1f56}', "\u{3c5}\u{313}\u{342}"),
        ('\u{1f80}', "\u{1f00}\u{3b9}"),
        ('\u{1f81}', "\u{1f01}\u{3b9}"),
        ('\u{1f82}', "\u{1f02}\u{3b9}"),
        ('\u{1f83}', "\u{1f03}\u{3b9}"),
        ('\u{1f84}', "\u{1f04}\u{3b9}"),
        ('\u{1f85}', "\u{1f05}\u{3b9}"),
        ('\u{1f86}', "\u{1f06}\u{3b9}"),
        ('\u{1f87}', "\u{1f07}\u{3b9}"),
        ('\u{1f90}', "\u{1f20}\u{3b9}"),
        ('\u{1f91}', "\u{1f21}\u{3b9}"),
        ('\u{1f92}', "\u{1f22}\u{3b9}"),
        ('\u{1f93}', "\u{1f23}\u{3b9}"),
        ('\u{1f94}', "\u{1f24}\u{3b9}"),
        ('\u{1f95}', "\u{1f25}\u{3b9}"),
        ('\u{1f96}', "\u{1f26}\u{3b9}"),
        ('\u{1f97}', "\u{1f27}\u{3b9}"),
        ('\u{1fa0}', "\u{1f60}\u{3b9}"),
        ('\u{1fa1}', "\u{1f61}\u{3b9}"),
        ('\u{1fa2}', "\u{1f62}\u{3b9}"),
        ('\u{1fa3}', "\u{1f63}\u{3b9}"),
        ('\u{1fa4}', "\u{1f64}\u{3b9}"),
        ('\u{1fa5}', "\u{1f65}\u{3b9}"),
        ('\u{1fa6}', "\u{1f66}\u{3b9}"),
        ('\u{1fa7}', "\u{1f67}\u{3b9}"),
        ('\u{1fb2}', "\u{1f70}\u{3b9}"),
        ('\u{1fb3}', "\u{3b1}\u{3b9}"),
        ('\u{1fb4}', "\u{3ac}\u{3b9}"),
        ('\u{1fb6}', "\u{3b1}\u{342}"),
        ('\u{1fb7}', "\u{3b1}\u{342}\u{3b9}"),
        ('\u{1fbe}', "\u{3b9}"),
        ('\u{1fc2}', "\u{1f74}\u{3b9}"),
        ('\u{1fc3}', "\u{3b7}\u{3b9}"),
        ('\u{1fc4}', "\u{3ae}\u{3b9}"),
        ('\u{1fc6}', "\u{3b7}\u{342}"),
        ('\u{1fc7}', "\u{3b7}\u{342}\u{3b9}"),
        ('\u{1fd2}', "\u{3b9}\u{308}\u{300}"),
        ('\u{1fd3}', "\u{3b9}\u{308}\u{301}"),
        ('\u{1fd6}', "\u{3b9}\u{342}"),
        ('\u{1fd7}', "\u{3b9}\u{308}\u{342}"),
        ('\u{1fe2}', "\u{3c5}\u{308}\u{300}"),
        ('\u{1fe3}', "\u{3c5}\u{308}\u{301}"),
        ('\u{1fe4}', "\u{3c1}\u{313}"),
        ('\u{1fe6}', "\u{3c5}\u{342}"),
        ('\u{1fe7}', "\u{3c5}\u{308}\u{342}"),
        ('\u{1ff2}', "\u{1f7c}\u{3b9}"),
        ('\u{1ff3}', "\u{3c9}\u{3b9}"),
        ('\u{1ff4}', "\u{3ce}\u{3b9}"),
        ('\u{1ff6}', "\u{3c9}\u{342}"),
        ('\u{1ff7}', "\u{3c9}\u{342}\u{3b9}"),
        ('\u{ab70}', "\u{13a0}"),
        ('\u{ab71}', "\u{13a1}"),
        ('\u{ab72}', "\u{13a2}"),
        ('\u{ab73}', "\u{13a3}"),
        ('\u{ab74}', "\u{13a4}"),
        ('\u{ab75}', "\u{13a5}"),
        ('\u{ab76}', "\u{13a6}"),
        ('\u{ab77}', "\u{13a7}"),
        ('\u{ab78}', "\u{13a8}"),
        ('\u{ab79}', "\u{13a9}"),
        ('\u{ab7a}', "\u{13aa}"),
        ('\u{ab7b}', "\u{13ab}"),
        ('\u{ab7c}', "\u{13ac}"),
        ('\u{ab7d}', "\u{13ad}"),
        ('\u{ab7e}', "\u{13ae}"),
        ('\u{ab7f}', "\u{13af}"),
        ('\u{ab80}', "\u{13b0}"),
        ('\u{ab81}', "\u{13b1}"),
        ('\u{ab82}', "\u{13b2}"),
        ('\u{ab83}', "\u{13b3}"),
        ('\u{ab84}', "\u{13b4}"),
        ('\u{ab85}', "\u{13b5}"),
        ('\u{ab86}', "\u{13b6}"),
        ('\u{ab87}', "\u{13b7}"),
        ('\u{ab88}', "\u{13b8}"),
        ('\u{ab89}', "\u{13b9}"),
        ('\u{ab8a}', "\u{13ba}"),
        ('\u{ab8b}', "\u{13bb}"),
        ('\u{ab8c}', "\u{13bc}"),
        ('\u{ab8d}', "\u{13bd}"),
        ('\u{ab8e}', "\u{13be}"),
        ('\u{ab8f}', "\u{13bf}"),
        ('\u{ab90}', "\u{13c0}"),
        ('\u{ab91}', "\u{13c1}"),
        ('\u{ab92}', "\u{13c2}"),
        ('\u{ab93}', "\u{13c3}"),
        ('\u{ab94}', "\u{13c4}"),
        ('\u{ab95}', "\u{13c5}"),
        ('\u{ab96}', "\u{13c6}"),
        ('\u{ab97}', "\u{13c7}"),
        ('\u{ab98}', "\u{13c8}"),
        ('\u{ab99}', "\u{13c9}"),
        ('\u{ab9a}', "\u{13ca}"),
        ('\u{ab9b}', "\u{13cb}"),
        ('\u{ab9c}', "\u{13cc}"),
        ('\u{ab9d}', "\u{13cd}"),
        ('\u{ab9e}', "\u{13ce}"),
        ('\u{ab9f}', "\u{13cf}"),
        ('\u{aba0}', "\u{13d0}"),
        ('\u{aba1}', "\u{13d1}"),
        ('\u{aba2}', "\u{13d2}"),
        ('\u{aba3}', "\u{13d3}"),
        ('\u{aba4}', "\u{13d4}"),
        ('\u{aba5}', "\u{13d5}"),
        ('\u{aba6}', "\u{13d6}"),
        ('\u{aba7}', "\u{13d7}"),
        ('\u{aba8}', "\u{13d8}"),
        ('\u{aba9}', "\u{13d9}"),
        ('\u{abaa}', "\u{13da}"),
        ('\u{abab}', "\u{13db}"),
        ('\u{abac}', "\u{13dc}"),
        ('\u{abad}', "\u{13dd}"),
        ('\u{abae}', "\u{13de}"),
        ('\u{abaf}', "\u{13df}"),
        ('\u{abb0}', "\u{13e0}"),
        ('\u{abb1}', "\u{13e1}"),
        ('\u{abb2}', "\u{13e2}"),
        ('\u{abb3}', "\u{13e3}"),
        ('\u{abb4}', "\u{13e4}"),
        ('\u{abb5}', "\u{13e5}"),
        ('\u{abb6}', "\u{13e6}"),
        ('\u{abb7}', "\u{13e7}"),
        ('\u{abb8}', "\u{13e8}"),
        ('\u{abb9}', "\u{13e9}"),
        ('\u{abba}', "\u{13ea}"),
        ('\u{abbb}', "\u{13eb}"),
        ('\u{abbc}', "\u{13ec}"),
        ('\u{abbd}', "\u{13ed}"),
        ('\u{abbe}', "\u{13ee}"),
        ('\u{abbf}', "\u{13ef}"),
        ('\u{fb00}', "\u{66}\u{66}"),
        ('\u{fb01}', "\u{66}\u{69}"),
        ('\u{fb02}', "\u{66}\u{6c}"),
        ('\u{fb03}', "\u{66}\u{66}\u{69}"),
        ('\u{fb04}', "\u{66}\u{66}\u{6c}"),
        ('\u{fb05}', "\u{73}\u{74}"),
        ('\u{fb06}', "\u{73}\u{74}"),
        ('\u{fb13}', "\u{574}\u{576}"),
        ('\u{fb14}', "\u{574}\u{565}"),
        ('\u{fb15}', "\u{574}\u{56b}"),
        ('\u{fb16}', "\u{57e}\u{576}"),
        ('\u{fb17}', "\u{574}\u{56d}"),
    ];
    TABLE
        .binary_search_by_key(&c, |(key, _)| *key)
        .ok()
        .map(|index| TABLE[index].1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_splits_on_python_word_characters() {
        let re = fancy_regex::Regex::new(&pythonize(r"\bHD\b")).expect("compiles");
        assert!(re.is_match("CNN HD USA").expect("matches"));
        assert!(!re.is_match("CHDUSA").expect("matches"));
        // A combining mark is NOT a Python word character, so Python places a boundary
        // between the mark and `HD`; the Rust engine counts marks as word characters and
        // would not.
        let combining = "e\u{0301}HD ";
        assert!(re.is_match(combining).expect("matches"));
        let plain = regex::Regex::new(r"\bHD\b").expect("compiles");
        assert!(
            !plain.is_match(combining),
            "the engines really do differ here"
        );
    }

    #[test]
    fn space_includes_c0_separators() {
        let re = fancy_regex::Regex::new(&pythonize(r"^\s+$")).expect("compiles");
        assert!(re.is_match("\u{1c}").expect("matches"));
        assert!(re.is_match("\u{1f}").expect("matches"));
        assert!(!re.is_match("x").expect("matches"));
    }

    #[test]
    fn whitespace_helpers_match_python() {
        assert_eq!(
            py_split_whitespace("  a\u{1c}b  c ").collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert_eq!(py_trim("\u{1c} a \u{1f}"), "a");
        assert_eq!(py_trim_start("\u{1c}  a "), "a ");
    }

    #[test]
    fn escape_covers_whitespace_and_punctuation() {
        assert_eq!(python_escape("a b"), r"a\ b");
        assert_eq!(python_escape("[HD]"), r"\[HD\]");
        assert_eq!(python_escape("HD"), "HD");
        assert_eq!(python_escape(""), "");
        // `<` and `>` are NOT escaped: `\<` would be a word boundary in the engine.
        assert_eq!(python_escape("<HD>"), "<HD>");
        assert_eq!(python_escape("a>b"), "a>b");
    }

    #[test]
    fn casefold_matches_python_on_the_tricky_code_points() {
        // Each expected string is Python's `str.casefold()` output.
        for (input, expected) in [
            ("STRASSE", "strasse"),
            ("Straße", "strasse"),
            ("ẞ", "ss"),
            ("ΣΊΣΥΦΟΣ", "σίσυφοσ"),
            ("ΟΔΟΣ", "οδοσ"),
            ("µ", "μ"),
            ("ſ", "s"),
            ("ﬄ", "ffl"),
            ("ᏸ", "Ᏸ"),
            ("İ", "i̇"),
            ("ŉ", "ʼn"),
            ("CNN En Español", "cnn en español"),
            ("Galavisión", "galavisión"),
            ("UniMás", "unimás"),
        ] {
            assert_eq!(py_casefold(input), expected, "input {input:?}");
        }
    }

    #[test]
    fn dotless_i_widening() {
        let re =
            fancy_regex::Regex::new(&format!("(?i){}", widen_case_insensitive_i(r"[A-Z]{2,3}:")))
                .expect("compiles");
        assert!(re.is_match("D\u{0131}:").expect("matches"));
        assert!(re.is_match("US:").expect("matches"));
    }
}
