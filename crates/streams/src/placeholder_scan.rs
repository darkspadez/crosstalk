//! The numbered-family scan behind `scan_placeholders`: Stream-Mapparr's `placeholder_scan.py`.
//!
//! GitHub issue #43: `epg_placeholder_name_patterns` only helps with the naming schemes the
//! operator already thought of, and nothing in the interface separates "this installation has
//! no placeholder families" from "the patterns you wrote match none of them". The reporter
//! found two whole uncovered families — one of them their largest — only by pulling every
//! stream name through the API by hand.
//!
//! This is a straight port of the module, which is pure stdlib Python with no Django and no
//! plugin import (its own tests import it directly, and so does
//! `scripts/tools_parity.py`): group every stream name by replacing its digit runs with a
//! slot, then describe the families that recur.
//!
//! Three findings from the module are kept because they were measured on live data:
//!
//!   * **A digit immediately followed by K is a resolution tag.** Replacing `4K`/`8K` turned
//!     293 unrelated names into one false family.
//!   * **A family needs [`MIN_DISTINCT_NUMBERS`] different numbers in its slot**, not that many
//!     streams: five rows of `HBO 1` are five sources for one name, not five slots.
//!   * **Coverage is decided with `fullmatch`**, the same way the matcher decides whether a
//!     name is a placeholder, so an unanchored pattern does not count as covering anything.
//!
//! A suggested pattern uses `\d+` for a slot and backslash escapes for everything else, and is
//! written ASCII-only (`\u00e9`) — the readout is opened in an editor that may use another
//! codepage. Both Python's `re` and Rust's `regex` accept `\uXXXX`/`\UXXXXXXXX`, so a
//! suggestion pastes into the setting and compiles in the engine that consumes it. The plugin's
//! `SUGGESTION_MAX_LEN` warning is kept for the same reason: a longer suggestion would be
//! skipped in silence by the pattern setting.

use crosstalk_matching::tokens::is_digit_char;

/// A family needs at least this many DIFFERENT numbers in its slot to be reported.
pub const MIN_DISTINCT_NUMBERS: usize = 3;

/// How many uncovered families the readout describes in full.
pub const DETAIL_LIMIT: usize = 25;

/// The placeholder-pattern setting refuses a pattern longer than this, so a suggestion over the
/// limit would be skipped in silence if pasted. `PluginConfig.REGEX_PATTERN_MAX_LEN`.
pub const SUGGESTION_MAX_LEN: usize = 500;

/// The slot marker in a template. A name containing a literal one is stored with it
/// backslash-escaped, so the two can never be confused.
pub const SLOT: char = '#';

/// A family at or above this share of members carrying an EPG identifier is more likely to be
/// an ordinary numbered channel lineup than a provider event slot.
pub const MOSTLY_EPG_SHARE: f64 = 0.5;

/// Characters that need escaping to appear literally in the suggested regex.
///
/// Deliberately NOT a general escape: Python 3.7's `re.escape` also escapes the space and the
/// hash, which would turn a readable, pasteable suggestion such as
/// `^Triller TV \| Event \d+$` into an unreadable one.
const REGEX_SPECIALS: [char; 15] = [
    '(', ')', '[', ']', '{', '}', '?', '*', '+', '-', '|', '^', '$', '\\', '.',
];

/// One stream name as the scan reads it: the name itself and whether it carries an EPG id.
#[derive(Debug, Clone, Copy)]
pub struct ScanStream<'a> {
    pub name: &'a str,
    pub has_epg_id: bool,
}

/// What a scan did, so a partial walk can never pass for a finished one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanStats {
    pub scanned: usize,
    pub skipped_long: usize,
}

/// One numbered family, best candidate first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Family {
    pub template: String,
    pub count: usize,
    pub distinct_numbers: usize,
    pub example: String,
    pub with_epg_id: usize,
    pub covered: usize,
    pub uncovered: usize,
    pub suggested: String,
}

impl Family {
    /// True when at least [`MOSTLY_EPG_SHARE`] of the family's streams carry an EPG identifier:
    /// a pattern over such a family REPLACES a working name with whatever is airing.
    pub fn mostly_carries_epg(&self) -> bool {
        if self.count == 0 {
            return false;
        }
        self.with_epg_id as f64 >= MOSTLY_EPG_SHARE * self.count as f64
    }

    /// Whether the suggestion would be refused by the setting for length.
    pub fn suggestion_too_long(&self) -> bool {
        self.suggested.chars().count() > SUGGESTION_MAX_LEN
    }
}

/// Rewrite every non-ASCII character as a backslash-u escape.
///
/// The readout is plain ASCII on purpose, the same rule as the CSV preamble: it is opened in a
/// text editor that may be using another codepage, where a non-ASCII byte becomes mojibake.
/// Provider stream names really do carry such characters (the plugin measured a circled bullet
/// and superscript letters). The escape is not merely readable, it is also correct inside a
/// suggested pattern: both Python's `re` and Rust's `regex` accept `\uXXXX` and `\UXXXXXXXX`.
pub fn ascii_safe(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        let code = ch as u32;
        if code < 128 {
            out.push(ch);
        } else if code <= 0xFFFF {
            out.push_str(&format!("\\u{code:04x}"));
        } else {
            // Eight hex digits, not four: the plugin's own bug report was that the shorter
            // form read only the first four and the pasted pattern matched nothing.
            out.push_str(&format!("\\U{code:08x}"));
        }
    }
    out
}

/// Escape `text` so it matches itself inside a regular expression.
fn escape_literal(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if REGEX_SPECIALS.contains(&ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Split `name` into its digit-stripped template and the numbers removed.
///
/// The numbers come back in the order they appeared, so a caller can count distinct slot
/// values; the vector is empty when the name holds no slot at all.
///
/// A digit run is left alone — and so contributes no slot — when it starts at a word boundary
/// and is immediately followed by the letter K: that is a resolution tag (`4K`, `8k`), not a
/// numbered slot. The boundary is required: in `X264K` the run is glued to a letter on the
/// left, so it IS a slot and the template is `X#K`.
pub fn template_of(name: &str) -> (String, Vec<String>) {
    let mut parts = String::with_capacity(name.len());
    let mut numbers: Vec<String> = Vec::new();
    if name.is_empty() {
        return (parts, numbers);
    }
    let chars: Vec<char> = name.chars().collect();
    let mut index = 0usize;
    while index < chars.len() {
        let ch = chars[index];
        if !is_digit_char(ch) {
            if ch == SLOT || ch == '\\' {
                // Both are escaped, and the backslash MUST be: a backslash followed by a digit
                // produces the same two characters as an escaped hash, so two different names
                // would group into one family and neither would match the suggested pattern.
                parts.push('\\');
            }
            parts.push(ch);
            index += 1;
            continue;
        }
        let start = index;
        while index < chars.len() && is_digit_char(chars[index]) {
            index += 1;
        }
        let run: String = chars[start..index].iter().collect();
        let at_boundary = start == 0 || !chars[start - 1].is_alphanumeric();
        let followed_by_k = index < chars.len() && (chars[index] == 'k' || chars[index] == 'K');
        if at_boundary && followed_by_k {
            parts.push_str(&run);
            // The K is upper-cased in the TEMPLATE only, never in the name: patterns are
            // compiled case-insensitively, so 4k and 4K are one family.
            parts.push('K');
            index += 1;
            continue;
        }
        parts.push(SLOT);
        numbers.push(run);
    }
    (parts, numbers)
}

/// Turn a template into an anchored regex the operator can paste.
///
/// Every slot becomes `\d+`, an escaped literal hash becomes a literal hash, and everything
/// else is escaped so it matches itself. Anchored at both ends because placeholder eligibility
/// is decided with `fullmatch` over the whole name.
pub fn suggested_pattern(template: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut literal = String::new();
    let chars: Vec<char> = template.chars().collect();
    let mut index = 0usize;
    while index < chars.len() {
        let ch = chars[index];
        if ch == '\\'
            && index + 1 < chars.len()
            && (chars[index + 1] == SLOT || chars[index + 1] == '\\')
        {
            literal.push(chars[index + 1]);
            index += 2;
            continue;
        }
        if ch == SLOT {
            out.push(escape_literal(&literal));
            literal.clear();
            out.push(r"\d+".to_string());
            index += 1;
            continue;
        }
        literal.push(ch);
        index += 1;
    }
    out.push(escape_literal(&literal));
    ascii_safe(&format!("^{}$", out.concat()))
}

/// Group `streams` by template and describe every numbered family found.
///
/// `patterns` are the compiled placeholder patterns already configured; coverage is decided
/// with a whole-name match, the same way the matcher decides whether a name is a placeholder.
/// A family is reported whether or not it is covered — the caller decides what to show.
///
/// The plugin's runtime containment exists because it runs inside a gevent worker where a loop
/// that never yields freezes every request: it yields every 500 names, skips a name over
/// `REGEX_NAME_MAX_LEN`, and stops at a wall-clock budget. Only the name cap has a port
/// (`max_name_len`), because Rust's `regex` is linear in the input and this walk is a single
/// pass over the names, so there is nothing to bound and a partial result cannot happen.
pub fn scan_families<'a>(
    streams: impl IntoIterator<Item = ScanStream<'a>>,
    patterns: &[regex::Regex],
    max_name_len: Option<usize>,
) -> (Vec<Family>, ScanStats) {
    // Collected so the walk can read a family's members twice (the example and the counts)
    // without assuming the caller's iterator is replayable.
    let streams: Vec<ScanStream<'a>> = streams.into_iter().collect();
    scan_families_borrowed(&streams, patterns, max_name_len)
}

fn scan_families_borrowed(
    streams: &[ScanStream<'_>],
    patterns: &[regex::Regex],
    max_name_len: Option<usize>,
) -> (Vec<Family>, ScanStats) {
    struct Group {
        template: String,
        count: usize,
        slots: Vec<std::collections::HashSet<String>>,
        example: String,
        with_epg_id: usize,
        covered: usize,
    }

    let mut grouped: std::collections::HashMap<String, Group> = std::collections::HashMap::new();
    let mut stats = ScanStats::default();

    for stream in streams {
        let name = stream.name;
        if name.is_empty() {
            continue;
        }
        if max_name_len.is_some_and(|cap| name.chars().count() > cap) {
            stats.skipped_long += 1;
            continue;
        }
        stats.scanned += 1;
        let (template, numbers) = template_of(name);
        if numbers.is_empty() {
            continue;
        }
        let group = grouped.entry(template.clone()).or_insert_with(|| Group {
            template,
            count: 0,
            slots: vec![std::collections::HashSet::new(); numbers.len()],
            example: name.to_string(),
            with_epg_id: 0,
            covered: 0,
        });
        group.count += 1;
        // Distinct numbers are counted WITHIN a slot, not across slots: counting combinations
        // let a two-slot template with two values in each slot show four "different numbers"
        // while neither slot held three.
        for (position, value) in numbers.iter().enumerate() {
            if position < group.slots.len() {
                group.slots[position].insert(value.clone());
            }
        }
        if name < group.example.as_str() {
            group.example = name.to_string();
        }
        if stream.has_epg_id {
            group.with_epg_id += 1;
        }
        if patterns.iter().any(|pattern| pattern.is_match(name)) {
            group.covered += 1;
        }
    }

    let mut families: Vec<Family> = Vec::new();
    for group in grouped.into_values() {
        let distinct = group
            .slots
            .iter()
            .map(std::collections::HashSet::len)
            .max()
            .unwrap_or(0);
        if distinct < MIN_DISTINCT_NUMBERS {
            continue;
        }
        families.push(Family {
            count: group.count,
            distinct_numbers: distinct,
            example: group.example,
            with_epg_id: group.with_epg_id,
            covered: group.covered,
            uncovered: group.count - group.covered,
            suggested: suggested_pattern(&group.template),
            template: group.template,
        });
    }
    // Largest first. The EPG identifier count is NOT a ranking key: measured on 25,068 live
    // names it ranked 16 families above the other 115 while the largest family was reduced to
    // one line. The signal is kept as an annotation on each family instead.
    families.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.template.cmp(&right.template))
    });
    (families, stats)
}

/// The plugin's `render_report` headline, as the parts its action's toast is built from.
///
/// The full readout is the CSV crosstalk writes; these are the lines an operator reads in the
/// run summary, in the plugin's own order and wording.
pub fn headline(
    families: &[Family],
    total_streams: usize,
    pattern_count: usize,
    feature_enabled: bool,
    stats: &ScanStats,
) -> Vec<String> {
    let uncovered: Vec<&Family> = families
        .iter()
        .filter(|family| family.uncovered > 0)
        .collect();
    let mut parts: Vec<String> = Vec::new();
    if families.is_empty() {
        parts.push(format!(
            "No numbered stream-name families were found across {total_streams} streams, so \
             there is nothing to cover."
        ));
    } else if uncovered.is_empty() {
        parts.push(format!(
            "{} numbered families found, and your {} pattern(s) cover them all.",
            families.len(),
            pattern_count
        ));
    } else {
        // The headline leads with the LARGEST uncovered family, not with the one carrying the
        // most EPG identifiers (see `scan_families`' ordering note).
        let top = uncovered[0];
        let word = if uncovered.len() == 1 {
            "family"
        } else {
            "families"
        };
        parts.push(format!(
            "{} numbered {word} that no pattern of yours covers.",
            uncovered.len()
        ));
        parts.push(format!(
            "Largest: {} ({} streams, {} carrying an EPG id).",
            ascii_safe(&top.template),
            top.count,
            top.with_epg_id
        ));
        parts.push(format!("Pattern to paste: {}", top.suggested));
        if top.mostly_carries_epg() {
            parts.push(
                "CAUTION: most of its streams carry an EPG id, so a pattern here replaces a \
                 working name."
                    .to_string(),
            );
        }
    }
    if !feature_enabled {
        parts.push("EPG-Based Placeholder Matching is currently off.".to_string());
    }
    if stats.skipped_long > 0 {
        parts.push(format!(
            "{} name(s) were too long to scan and were skipped.",
            stats.skipped_long
        ));
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;
    use regex::RegexBuilder;

    fn pat(patterns: &[&str]) -> Vec<regex::Regex> {
        patterns
            .iter()
            .map(|pattern| {
                RegexBuilder::new(&format!("^(?:{pattern})$"))
                    .case_insensitive(true)
                    .build()
                    .expect("pattern")
            })
            .collect()
    }

    fn streams(names: &[(&'static str, bool)]) -> Vec<ScanStream<'static>> {
        names
            .iter()
            .map(|(name, has_epg_id)| ScanStream {
                name,
                has_epg_id: *has_epg_id,
            })
            .collect()
    }

    fn scan(names: &[(&'static str, bool)], patterns: &[&str]) -> Vec<Family> {
        scan_families(streams(names), &pat(patterns), None).0
    }

    #[test]
    fn a_digit_run_becomes_one_slot() {
        assert_eq!(
            template_of("MAX 100"),
            ("MAX #".to_string(), vec!["100".into()])
        );
        assert_eq!(
            template_of("Triller TV | Event 07"),
            ("Triller TV | Event #".to_string(), vec!["07".into()])
        );
        assert_eq!(
            template_of("LIVE EVENT 04 - 11am"),
            (
                "LIVE EVENT # - #am".to_string(),
                vec!["04".into(), "11".into()]
            )
        );
    }

    #[test]
    fn a_resolution_tag_is_not_a_slot() {
        assert_eq!(
            template_of("US: CINEMANIA 4K"),
            ("US: CINEMANIA 4K".to_string(), Vec::new())
        );
        assert_eq!(
            template_of("PPV 12 | 4K"),
            ("PPV # | 4K".to_string(), vec!["12".into()])
        );
        // Upper-cased in the template so 4k and 4K are one family; the name is never changed.
        assert_eq!(
            template_of("Movie 8k"),
            ("Movie 8K".to_string(), Vec::new())
        );
        // The boundary is required: glued to a letter on the left, the run is a slot.
        assert_eq!(
            template_of("X264K"),
            ("X#K".to_string(), vec!["264".into()])
        );
        assert_eq!(template_of("CNN"), ("CNN".to_string(), Vec::new()));
    }

    #[test]
    fn a_literal_hash_and_backslash_are_escaped_in_the_template() {
        let (template, numbers) = template_of("Event #7");
        assert_eq!(numbers, vec!["7".to_string()]);
        // The literal hash is escaped, so the slot marker that follows cannot be confused
        // with it: `Event \##` is "escaped hash" then "slot".
        assert_eq!(template, "Event \\##".to_string());
        // The suggested pattern matches the real names, and a name without the hash is not a
        // family member.
        let pattern = suggested_pattern(&template);
        let compiled = RegexBuilder::new(&pattern)
            .case_insensitive(true)
            .build()
            .expect("compiles");
        assert!(compiled.is_match("Event #7"));
        assert!(compiled.is_match("Event #12"));
        assert!(!compiled.is_match("Event 7"));
    }

    #[test]
    fn the_suggested_pattern_is_anchored_escaped_and_compiles_in_rust() {
        assert_eq!(
            suggested_pattern("Triller TV | Event #"),
            r"^Triller TV \| Event \d+$"
        );
        assert_eq!(suggested_pattern("PPV # | 4K"), r"^PPV \d+ \| 4K$");
        for template in [
            "MAX #",
            "Event \\#7",
            "Caf\u{e9} #",
            " \u{1F600} # ",
            "(x) # [y] #",
        ] {
            let pattern = suggested_pattern(template);
            regex::Regex::new(&pattern)
                .unwrap_or_else(|error| panic!("{template:?} suggested {pattern:?}: {error}"));
        }
    }

    #[test]
    fn ascii_safe_escapes_bmp_and_astral_characters() {
        assert_eq!(ascii_safe("Caf\u{e9}"), "Caf\\u00e9");
        assert_eq!(ascii_safe("\u{1F600}"), "\\U0001f600");
        assert_eq!(ascii_safe("plain"), "plain");
    }

    #[test]
    fn a_recurring_numbered_family_is_a_candidate() {
        let families = scan(
            &[
                ("MAX 100", false),
                ("MAX 101", false),
                ("MAX 102", false),
                ("MAX 103", false),
            ],
            &[],
        );
        assert_eq!(families.len(), 1);
        let family = &families[0];
        assert_eq!(family.template, "MAX #");
        assert_eq!(family.count, 4);
        assert_eq!(family.distinct_numbers, 4);
        assert_eq!(family.example, "MAX 100");
        assert_eq!(family.suggested, r"^MAX \d+$");
        assert_eq!(family.uncovered, 4);
    }

    #[test]
    fn the_threshold_is_three_distinct_numbers_not_three_streams() {
        assert_eq!(MIN_DISTINCT_NUMBERS, 3);
        assert!(scan(&[("MAX 1", false), ("MAX 2", false)], &[]).is_empty());
        assert_eq!(
            scan(&[("MAX 1", false), ("MAX 2", false), ("MAX 3", false)], &[]).len(),
            1
        );
        // Nine rows of one number are nine sources for one name, not nine slots.
        let names: Vec<(&'static str, bool)> = vec![("HBO 1", false); 9];
        assert!(scan(&names, &[]).is_empty());
    }

    #[test]
    fn coverage_uses_whole_name_matches() {
        let names = &[("MAX 100", false), ("MAX 101", false), ("MAX 102", false)];
        // Anchored: covered.
        let families = scan(names, &[r"MAX \d+"]);
        assert_eq!(families[0].covered, 3);
        assert_eq!(families[0].uncovered, 0);
        // Unanchored: the matcher's own fullmatch would not, and neither does coverage.
        let families = scan(names, &["MAX"]);
        assert_eq!(families[0].covered, 0);
    }

    #[test]
    fn families_are_ordered_by_size_then_template() {
        let families = scan(
            &[
                ("BIG 1", false),
                ("BIG 2", false),
                ("BIG 3", false),
                ("BIG 4", false),
                ("AAA 1", false),
                ("AAA 2", false),
                ("AAA 3", false),
            ],
            &[],
        );
        assert_eq!(
            families
                .iter()
                .map(|f| f.template.as_str())
                .collect::<Vec<_>>(),
            vec!["BIG #", "AAA #"]
        );
    }

    #[test]
    fn a_mostly_epg_family_is_flagged() {
        let families = scan(&[("PPV 1", true), ("PPV 2", true), ("PPV 3", false)], &[]);
        assert!(families[0].mostly_carries_epg());
        let families = scan(&[("PPV 1", true), ("PPV 2", false), ("PPV 3", false)], &[]);
        assert!(!families[0].mostly_carries_epg());
    }

    #[test]
    fn the_headline_follows_the_plugins_words() {
        let families = scan(
            &[
                ("PPV EVENT 1", false),
                ("PPV EVENT 2", false),
                ("PPV EVENT 3", false),
            ],
            &[],
        );
        let stats = ScanStats::default();
        let parts = headline(&families, 3, 0, false, &stats);
        assert!(
            parts[0].starts_with("1 numbered family that no pattern of yours covers"),
            "{parts:?}"
        );
        assert!(
            parts[1].starts_with("Largest: PPV EVENT # (3 streams, 0 carrying"),
            "{parts:?}"
        );
        assert!(
            parts[2].starts_with("Pattern to paste: ^PPV EVENT \\d+$"),
            "{parts:?}"
        );
        assert!(parts.last().unwrap().contains("currently off"), "{parts:?}");

        // Covered: no names, and no suggestion.
        let families = scan(
            &[
                ("PPV EVENT 1", false),
                ("PPV EVENT 2", false),
                ("PPV EVENT 3", false),
            ],
            &[r"^PPV EVENT \d+$"],
        );
        let parts = headline(&families, 3, 1, true, &stats);
        assert!(parts[0].contains("cover them all"), "{parts:?}");
        assert_eq!(parts.len(), 1);

        let parts = headline(&[], 0, 0, true, &stats);
        assert!(
            parts[0].starts_with("No numbered stream-name families"),
            "{parts:?}"
        );
    }

    #[test]
    fn a_name_over_the_cap_is_skipped_and_counted() {
        let long: &'static str = Box::leak("MAX 100 ".repeat(100).into_boxed_str());
        let (families, stats) = scan_families(
            streams(&[
                (long, false),
                ("MAX 1", false),
                ("MAX 2", false),
                ("MAX 3", false),
            ]),
            &[],
            Some(50),
        );
        assert_eq!(stats.skipped_long, 1);
        assert_eq!(stats.scanned, 3);
        assert_eq!(families.len(), 1);
    }
}
