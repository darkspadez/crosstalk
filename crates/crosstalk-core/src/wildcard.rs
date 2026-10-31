//! Wildcard matching for the list settings: channel groups and EPG source names.
//!
//! Port of `wildcard_match.expand_patterns` from the plugin. The rule that matters is that a
//! token is a GLOB only when it contains `*` or `?`; everything else is a literal, and literals
//! are matched case-insensitively for source names (where providers disagree about case) but
//! case-sensitively for group names (where `Sports` and `SPORTS` are different groups).
//!
//! The ordering rule is deliberate too: a name matched by several tokens is listed under the
//! LOWEST matching token index, so the report reads in the order the operator wrote.

/// Resolve operator tokens against the available names.
///
/// Returns the matched names (ordered by the lowest matching token index, then by the order
/// the names were supplied) and the tokens that matched nothing.
pub fn expand_patterns(
    tokens: &[String],
    available_names: &[String],
    ci_plain: bool,
) -> (Vec<String>, Vec<String>) {
    let mut matched: Vec<(usize, usize, String)> = Vec::new(); // (token index, name index, name)
    let mut matched_token = vec![false; tokens.len()];

    for (token_index, token) in tokens.iter().enumerate() {
        let is_glob = token.contains('*') || token.contains('?');
        let token_lower = token.to_lowercase();

        for (name_index, name) in available_names.iter().enumerate() {
            let hit = if is_glob {
                glob_match(&token_lower, &name.to_lowercase())
            } else if ci_plain {
                name.to_lowercase() == token_lower
            } else {
                name == token
            };

            if !hit {
                continue;
            }
            matched_token[token_index] = true;

            match matched
                .iter_mut()
                .find(|(_, name_index_matched, _)| *name_index_matched == name_index)
            {
                Some(entry) => {
                    if token_index < entry.0 {
                        entry.0 = token_index;
                    }
                }
                None => matched.push((token_index, name_index, name.clone())),
            }
        }
    }

    matched.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let names = matched.into_iter().map(|(_, _, name)| name).collect();
    let unmatched = tokens
        .iter()
        .enumerate()
        .filter(|(index, _)| !matched_token[*index])
        .map(|(_, token)| token.clone())
        .collect();
    (names, unmatched)
}

/// `*` and `?` matching, without the escape rules a full glob library brings.
///
/// Both sides are lowercased by the caller, so this works on characters rather than bytes: a
/// name with an accent must not be split mid-character.
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();

    // Iterative backtracking: linear in practice, and no recursion depth to worry about.
    let (mut p, mut t) = (0usize, 0usize);
    let (mut star, mut star_match) = (None, 0usize);

    while t < text.len() {
        if p < pattern.len() && (pattern[p] == '?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == '*' {
            star = Some(p);
            star_match = t;
            p += 1;
        } else if let Some(star_index) = star {
            p = star_index + 1;
            star_match += 1;
            t = star_match;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }
    p == pattern.len()
}

/// Split a comma/newline separated setting into tokens.
pub fn parse_tokens(value: &str) -> Vec<String> {
    value
        .split([',', '\n'])
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn a_literal_matches_case_sensitively_for_groups() {
        let (matched, unmatched) =
            expand_patterns(&owned(&["Sports"]), &owned(&["Sports", "news"]), false);
        assert_eq!(matched, owned(&["Sports"]));
        assert!(unmatched.is_empty());

        let (matched, unmatched) = expand_patterns(&owned(&["sports"]), &owned(&["Sports"]), false);
        assert!(matched.is_empty());
        assert_eq!(unmatched, owned(&["sports"]));
    }

    #[test]
    fn a_literal_matches_case_insensitively_for_sources() {
        let (matched, _) = expand_patterns(
            &owned(&["schedules direct"]),
            &owned(&["Schedules Direct"]),
            true,
        );
        assert_eq!(matched, owned(&["Schedules Direct"]));
    }

    #[test]
    fn globs_are_case_insensitive_and_support_star_and_question() {
        let names = owned(&["US: All", "us: sports", "UK: All"]);
        let (matched, unmatched) = expand_patterns(&owned(&["US*"]), &names, false);
        assert_eq!(matched, owned(&["US: All", "us: sports"]));
        assert!(unmatched.is_empty());

        let (matched, _) = expand_patterns(&owned(&["U?: All"]), &names, false);
        assert_eq!(matched, owned(&["US: All", "UK: All"]));
    }

    #[test]
    fn results_follow_token_order_then_name_order() {
        let names = owned(&["B", "A", "C"]);
        let (matched, _) = expand_patterns(&owned(&["C", "A"]), &names, false);
        // `C` is listed first because its token came first.
        assert_eq!(matched, owned(&["C", "A"]));
    }

    #[test]
    fn a_name_matched_by_several_tokens_is_listed_once() {
        let (matched, _) =
            expand_patterns(&owned(&["A*", "*A"]), &owned(&["AB", "AC", "DA"]), false);
        assert_eq!(matched, owned(&["AB", "AC", "DA"]));
    }

    #[test]
    fn unmatched_tokens_are_reported_in_input_order() {
        let (_, unmatched) = expand_patterns(&owned(&["zz", "Sports"]), &owned(&["Sports"]), false);
        assert_eq!(unmatched, owned(&["zz"]));
    }

    #[test]
    fn an_empty_token_list_matches_nothing() {
        let (matched, unmatched) = expand_patterns(&[], &owned(&["A"]), false);
        assert!(matched.is_empty());
        assert!(unmatched.is_empty());
    }

    #[test]
    fn glob_matching_handles_the_edges() {
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("a*b", "ab"));
        assert!(glob_match("a*b", "axxxb"));
        assert!(!glob_match("a*b", "axxx"));
        assert!(glob_match("?bc", "abc"));
        assert!(!glob_match("?bc", "bc"));
        assert!(glob_match("a*c", "abc"));
        assert!(!glob_match("a*c", "abcd"));
        // Multi-byte characters are matched whole.
        assert!(glob_match("é*", "école"));
        assert!(!glob_match("e*", "école"));
    }

    #[test]
    fn tokens_split_on_commas_and_newlines() {
        assert_eq!(
            parse_tokens(" DTV: All , DISH: All\nPPV* "),
            owned(&["DTV: All", "DISH: All", "PPV*"])
        );
        assert!(parse_tokens("  ,  \n ").is_empty());
    }
}
