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
        while let Some(inner) = chars.next() {
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
        let mut inner = class.chars().peekable();
        while let Some(ch) = inner.next() {
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
    fn dotless_i_widening() {
        let re =
            fancy_regex::Regex::new(&format!("(?i){}", widen_case_insensitive_i(r"[A-Z]{2,3}:")))
                .expect("compiles");
        assert!(re.is_match("D\u{0131}:").expect("matches"));
        assert!(re.is_match("US:").expect("matches"));
    }
}
