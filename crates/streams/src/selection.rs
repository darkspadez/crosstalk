//! Stream selection: which streams a match may draw from, at what priority, and under what
//! name.
//!
//! Port of Stream-Mapparr's `load_process_channels_action` (plugin.py ~7520-7710), which is
//! where the stream half of the scope is resolved, plus the choke point
//! `_apply_regex_rules_to_streams` (plugin.py ~222) that stamps the name matching sees.
//!
//! Three filters, in the plugin's order, each resolved into a hash lookup built ONCE for the
//! whole run — this has to hold up at ~50k streams and ~100 accounts, so nothing here walks one
//! collection for every element of another:
//!
//!   1. **M3U accounts** (`selected_m3us`). The list is ORDERED: a stream's position in its
//!      account's list is its priority, 0 first, and that order decides failover. It is a
//!      filter as well: without a `*` entry only the accounts it names are used. Names match
//!      EXACTLY, case-sensitively, like the plugin's own `m3u_name_to_id` lookup: providers
//!      disagree about case and Dispatcharr enforces name uniqueness case-sensitively, so a
//!      case-insensitive match could admit an account the plugin would never name. The one
//!      exception is a lone `*`, the plugin's "every other source" entry. Eligibility
//!      (named, and switched on) follows the shared [`crosstalk_core::scope::sources`] shape
//!      that guide uses for EPG sources; the priority order is the plugin's own layer and
//!      lives here.
//!   2. **Stream groups** (`selected_stream_groups`, `stream_group_filter_mode`). The plugin
//!      looks a group name up in an exact, case-sensitive `{name: id}` map and has no
//!      wildcards, and neither does this port: a token with `*` in it matches no group, so
//!      it lands in the unknown-names warning instead of silently selecting a family the
//!      plugin would have fallen back past. The mode is resolved SEPARATELY from the
//!      channel-group mode, so excluding a channel group does not invert this list.
//!   3. **Regex rules** (`stream_name_regex_rules`). Applied to the name MATCHING sees, never
//!      to the name in Dispatcharr: quality sorting, zone routing, country restriction and the
//!      duplicate check all keep reading the original name.
//!
//! What comes out is a [`StreamPool`]: every stream by id, the eligible ids, the priority of
//! each, the cleaned match name of each, and an index from cleaned name to stream ids so the
//! matching collector never walks the pool.

use crate::settings::{StreamsSettings, FILTER_MODES};
use anyhow::Result;
use crosstalk_core::dispatcharr::{M3uAccount, Stream, World, WorldKind};
use crosstalk_core::scope::sources;
use crosstalk_core::wildcard::parse_tokens;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// The token that stands for "every source this list does not name".
///
/// Stream-Mapparr's `Plugin.M3U_WILDCARD`. It is handled before glob expansion, because it
/// means the opposite of "an account whose name is `*`".
pub const WILDCARD: &str = "*";

/// The priority of a stream whose account the list did not name and no wildcard covers.
/// Stream-Mapparr's `Plugin.M3U_UNLISTED_PRIORITY`: such a stream is kept and sorts last.
pub const UNLISTED_PRIORITY: usize = 999;

/// Stream-Mapparr's `REGEX_RULES_MAX`: how many rules the setting may carry.
pub const REGEX_RULES_MAX: usize = 50;
/// Stream-Mapparr's `REGEX_PATTERN_MAX_LEN`: per pattern and per replacement.
pub const REGEX_PATTERN_MAX_LEN: usize = 500;
/// The compiled-size cap on an operator's pattern (1 MiB, a tenth of `regex`'s default). A
/// 500-character pattern can still ask for a large program (`\w{100}` repeated); this keeps
/// one setting from holding megabytes per rule. Not in the plugin.
pub const REGEX_SIZE_LIMIT: usize = 1 << 20;
/// Stream-Mapparr's `REGEX_NAME_MAX_LEN`: longer names skip pre-processing.
pub const REGEX_NAME_MAX_LEN: usize = 500;
/// Stream-Mapparr's `REGEX_GROWTH_FACTOR`: per-name output cap, times the original length.
pub const REGEX_GROWTH_FACTOR: usize = 4;
/// Stream-Mapparr's `REGEX_GROWTH_FLOOR`: the floor under that cap.
pub const REGEX_GROWTH_FLOOR: usize = 1000;
/// Stream-Mapparr's `REGEX_PASS_BUDGET_S`: cumulative wall clock for one application pass.
pub const REGEX_PASS_BUDGET: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------------------
// Regex rules
// ---------------------------------------------------------------------------------------

/// One usable rule: the pattern it was compiled from and its replacement, already in the
/// replacement syntax Rust's `regex` expands.
#[derive(Debug, Clone)]
pub struct Rule {
    pub pattern: String,
    pub regex: Regex,
    pub replacement: String,
}

/// What one entry of the JSON list was worth.
///
/// The statuses are the plugin's own (`_resolve_stream_regex_rules`'s report dict), so a run's
/// log reads the same way: `ok`, `invalid_json_shape`, `too_long`, `empty_pattern`,
/// `compile_error`. `bad_escape` is crosstalk's: Python only rejects a bad replacement escape
/// when it applies the rule to a name, which would count an error per stream; refusing it once
/// at parse time says the same thing in one line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleReport {
    pub index: usize,
    /// The pattern, truncated to 80 characters for the log, as the plugin does.
    pub pattern: String,
    pub status: &'static str,
    pub detail: String,
}

impl RuleReport {
    pub fn is_ok(&self) -> bool {
        self.status == "ok"
    }
}

/// The parsed setting: the rules that passed every gate, and what happened to the rest.
#[derive(Debug, Clone, Default)]
pub struct RuleSet {
    pub rules: Vec<Rule>,
    pub report: Vec<RuleReport>,
}

impl RuleSet {
    /// Parse a `[find, replace]` JSON list, degrading rather than failing, exactly as the
    /// plugin does: an unusable entry is skipped and reported, and a setting that is not JSON
    /// at all disables the feature with one report line.
    pub fn parse(raw: &str) -> Self {
        let raw = raw.trim();
        if raw.is_empty() {
            return Self::default();
        }

        let entries = match serde_json::from_str::<serde_json::Value>(raw) {
            Ok(serde_json::Value::Array(entries)) => entries,
            Ok(_) => {
                return Self {
                    rules: Vec::new(),
                    report: vec![RuleReport {
                        index: 0,
                        pattern: truncate(raw),
                        status: "invalid_json_shape",
                        detail: "top level must be a JSON array".to_string(),
                    }],
                }
            }
            Err(error) => {
                return Self {
                    rules: Vec::new(),
                    report: vec![RuleReport {
                        index: 0,
                        pattern: truncate(raw),
                        status: "invalid_json_shape",
                        detail: error.to_string(),
                    }],
                }
            }
        };

        let mut rules: Vec<Rule> = Vec::new();
        let mut report: Vec<RuleReport> = Vec::new();
        for (index, entry) in entries.into_iter().enumerate() {
            let pair = match &entry {
                serde_json::Value::Array(items)
                    if items.len() == 2 && items.iter().all(|item| item.is_string()) =>
                {
                    let pattern = items[0].as_str().unwrap_or_default().to_string();
                    let replacement = items[1].as_str().unwrap_or_default().to_string();
                    Some((pattern, replacement))
                }
                _ => None,
            };
            let Some((pattern, replacement)) = pair else {
                report.push(RuleReport {
                    index,
                    pattern: truncate(&entry.to_string()),
                    status: "invalid_json_shape",
                    detail: "each rule must be [\"pattern\", \"replacement\"]".to_string(),
                });
                continue;
            };

            match build_rule(&pattern, &replacement, rules.len()) {
                Ok(rule) => {
                    report.push(RuleReport {
                        index,
                        pattern: truncate(&pattern),
                        status: "ok",
                        detail: String::new(),
                    });
                    rules.push(rule);
                }
                Err((status, detail)) => report.push(RuleReport {
                    index,
                    pattern: truncate(&pattern),
                    status,
                    detail,
                }),
            }
        }

        Self { rules, report }
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// The entries that did not become a rule.
    pub fn rejected(&self) -> impl Iterator<Item = &RuleReport> {
        self.report.iter().filter(|entry| !entry.is_ok())
    }

    /// One line naming every rejected entry, or `None` when the setting is clean.
    pub fn rejection_summary(&self) -> Option<String> {
        let rejected: Vec<String> = self
            .rejected()
            .map(|entry| {
                format!(
                    "rule {} ({}): {}",
                    entry.index + 1,
                    entry.status,
                    entry.detail
                )
            })
            .collect();
        if rejected.is_empty() {
            None
        } else {
            Some(format!(
                "stream_name_regex_rules: {} of {} rule(s) skipped; {}",
                rejected.len(),
                self.report.len(),
                rejected.join("; ")
            ))
        }
    }

    /// Apply the rules to ONE name, in order, with no containment bookkeeping.
    ///
    /// The EPG paths clean a single string at a time — a programme title, a channel name — and
    /// the plugin runs exactly this: `for compiled, replacement: cleaned = compiled.sub(...)`.
    /// The per-name growth revert is not applied here because a title is bounded by the guide
    /// row, not by an operator's rule set; the stream-name pass keeps it (see [`Self::apply`]).
    pub fn apply_to_str(&self, name: &str) -> String {
        let mut result = name.to_string();
        for rule in &self.rules {
            result = rule
                .regex
                .replace_all(&result, rule.replacement.as_str())
                .into_owned();
        }
        result
    }

    /// Apply every rule to every name, stamping the name matching should see.
    ///
    /// Faithful to `_apply_regex_rules_to_streams`, containment included: a name over
    /// [`REGEX_NAME_MAX_LEN`] is skipped, one whose output grows past
    /// `max(4x, 1000)` characters reverts to what the offending rule received and stops there,
    /// and the whole pass gives up after [`REGEX_PASS_BUDGET`] (reporting that it did, rather
    /// than quietly half-cleaning the pool).
    pub fn apply(&self, names: &[(i64, &str)]) -> (HashMap<i64, String>, RegexRun) {
        let mut cleaned: HashMap<i64, String> = HashMap::with_capacity(names.len());
        let mut counters = RegexRun::default();
        if self.rules.is_empty() {
            for (id, name) in names {
                cleaned.insert(*id, (*name).to_string());
            }
            return (cleaned, counters);
        }

        let deadline = Instant::now() + REGEX_PASS_BUDGET;
        for (id, name) in names {
            let mut result = (*name).to_string();
            if counters.budget_tripped {
                cleaned.insert(*id, result);
                continue;
            }
            if Instant::now() >= deadline {
                counters.budget_tripped = true;
                cleaned.insert(*id, result);
                continue;
            }
            let length = name.chars().count();
            if length > REGEX_NAME_MAX_LEN {
                counters.skipped_long += 1;
                cleaned.insert(*id, result);
                continue;
            }
            let limit = (REGEX_GROWTH_FACTOR * length).max(REGEX_GROWTH_FLOOR);
            for rule in &self.rules {
                let before = result.clone();
                result = rule
                    .regex
                    .replace_all(&result, rule.replacement.as_str())
                    .into_owned();
                if result.chars().count() > limit {
                    counters.growth_reverted += 1;
                    result = before;
                    break;
                }
            }
            if result != *name {
                counters.changed += 1;
                if result.trim().is_empty() {
                    counters.emptied += 1;
                }
            }
            cleaned.insert(*id, result);
        }
        (cleaned, counters)
    }
}

/// Gate one `[pattern, replacement]` pair, in the plugin's order of checks, and compile it.
///
/// `existing` is how many rules have already been kept, because the cap counts usable rules.
fn build_rule(
    pattern: &str,
    replacement: &str,
    existing: usize,
) -> std::result::Result<Rule, (&'static str, String)> {
    if existing + 1 > REGEX_RULES_MAX {
        return Err(("too_long", format!("over the {REGEX_RULES_MAX}-rule cap")));
    }
    if pattern.chars().count() > REGEX_PATTERN_MAX_LEN
        || replacement.chars().count() > REGEX_PATTERN_MAX_LEN
    {
        return Err((
            "too_long",
            format!("pattern/replacement over {REGEX_PATTERN_MAX_LEN} characters"),
        ));
    }
    if pattern.is_empty() {
        return Err((
            "empty_pattern",
            "an empty find pattern matches everywhere".to_string(),
        ));
    }
    let regex =
        compile_pattern(pattern).map_err(|error| ("compile_error", format!("{error:#}")))?;
    // Group references are checked NOW, against this pattern's own groups: Python only
    // rejects `\99` or `\g<name>` when it applies the rule to a name (one error per
    // stream), while refusing the rule once says the same thing in one line and leaves every
    // name exactly as the plugin's per-name skip would. Rust's `regex` has no ReDoS backtracking,
    // so the plugin's `unsafe_pattern` gate (nested quantifiers Python cannot run safely) is
    // deliberately absent: such a pattern runs in linear time here and does what the operator
    // wrote, instead of being skipped.
    let replacement = translate_replacement(replacement, &regex)
        .map_err(|error| ("bad_escape", format!("{error:#}")))?;
    Ok(Rule {
        pattern: pattern.to_string(),
        regex,
        replacement,
    })
}

/// What one application pass did, in the plugin's counters (`_apply_regex_rules_to_streams`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RegexRun {
    pub changed: usize,
    pub skipped_long: usize,
    pub growth_reverted: usize,
    pub emptied: usize,
    /// The wall-clock budget ran out: the rest of the pool kept its original name.
    pub budget_tripped: bool,
}

impl RegexRun {
    /// Whether anything happened that an operator should be told about.
    pub fn has_warnings(&self) -> bool {
        self.skipped_long > 0 || self.growth_reverted > 0 || self.emptied > 0 || self.budget_tripped
    }

    /// The plugin's one aggregate warning line, adapted to crosstalk.
    pub fn summary(&self) -> Option<String> {
        if !self.has_warnings() {
            return None;
        }
        let mut parts = vec![format!("{} name(s) pre-processed", self.changed)];
        if self.skipped_long > 0 {
            parts.push(format!(
                "{} name(s) over {REGEX_NAME_MAX_LEN} characters skipped",
                self.skipped_long
            ));
        }
        if self.growth_reverted > 0 {
            parts.push(format!("{} growth-capped", self.growth_reverted));
        }
        if self.emptied > 0 {
            parts.push(format!(
                "{} emptied and therefore unmatchable",
                self.emptied
            ));
        }
        if self.budget_tripped {
            parts.push("time budget exceeded, the remaining names were left alone".to_string());
        }
        Some(format!("stream name regex rules: {}", parts.join("; ")))
    }
}

/// Compile one rule, refusing what Rust's `regex` cannot express the way Python means it.
///
/// The rejection approach is `channel_ops::compile_pattern`'s: compile, and turn the engine's
/// own complaint into an explanation of the difference, because "invalid regex" alone leaves an
/// operator hunting for a typo when the real problem is that Python's `re` has features Rust's
/// engine deliberately does not.
fn compile_pattern(pattern: &str) -> Result<Regex> {
    regex::RegexBuilder::new(pattern)
        .size_limit(REGEX_SIZE_LIMIT)
        .build()
        .map_err(|error| {
            if let regex::Error::CompiledTooBig(limit) = error {
                return anyhow::anyhow!(
                    "stream name pattern {pattern:?} compiles past the {limit}-byte limit; \
                     simplify it (fewer or smaller repetitions)"
                );
            }
            anyhow::anyhow!(
                "invalid stream name pattern {pattern:?}: {error}. Rust's regex engine is not \
             Python's `re`: backreferences (\\1), lookaround ((?=…), (?<=…)) and conditionals \
             are not supported, and a pattern that needs one has to be rewritten"
            )
        })
}

/// Translate a Python `re.sub` replacement into the syntax Rust's `regex` expands.
///
/// The plugin's help text promises Python replacement syntax, so `\1` and `\g<name>` are what
/// operators write and what has to work. The two syntaxes are close but not the same: Rust
/// expands `$1` and `${name}`, and treats `$` as the only special character, so a literal `$`
/// has to be doubled. Every other escape follows CPython's `_parse_template` exactly, verified
/// against the interpreter (including the surprising ones):
///
///   * `\a \b \f \n \r \t \v \\` are control characters; `\g<0>` is the whole match.
///   * `\1`..`\99` are group references (at most two digits), refused here when the pattern
///     has no such group — Python raises `invalid group reference` per name, which is the same
///     outcome as refusing the rule once.
///   * A backslash followed by `0` starts an OCTAL escape of up to three digits (`\0`, `\07`,
///     `\123`), never group 0; three octal digits win even when they start with 1-7, while two
///     digits are a group reference (`\70` is group 70, not `8`). Above `\377` is refused.
///   * A backslash before anything else that is not a letter or digit (punctuation, a space,
///     `$`) is literal: Python keeps BOTH characters, so `\$` stays a backslash and a dollar.
///   * An escape Python rejects (`\d`, an unclosed `\g<`, a trailing backslash) is refused
///     here rather than copied through, which is what would otherwise produce a silent `\d`
///     in a stream name.
///
/// The one translator for every Python-replacement setting: lineup's `clean` custom rules
/// use it too, so the same replacement text means the same thing in both apps.
pub fn translate_replacement(replacement: &str, regex: &Regex) -> Result<String> {
    let groups = regex.captures_len() - 1;
    let named: Vec<&str> = regex.capture_names().flatten().collect();
    let check_numbered = |number: usize, raw: &str| -> Result<()> {
        if number == 0 || number > groups {
            anyhow::bail!("invalid group reference {raw}: the pattern has {groups} group(s)");
        }
        Ok(())
    };

    let mut out = String::with_capacity(replacement.len());
    let mut chars = replacement.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            // `$` is Rust replacement syntax; everything else is literal.
            if ch == '$' {
                out.push_str("$$");
            } else {
                out.push(ch);
            }
            continue;
        }
        let Some(next) = chars.peek().copied() else {
            anyhow::bail!(
                "bad escape (end of pattern): the replacement ends with a lone backslash"
            );
        };
        // Three octal digits win over a group reference, even starting with 1-7 (`\123` is
        // `S`, while two digits are always a group: `\70` is group 70, never `8`).
        let mut octal = String::new();
        let mut probe = chars.clone();
        for _ in 0..3 {
            match probe.next() {
                Some(digit) if matches!(digit, '0'..='7') => octal.push(digit),
                _ => break,
            }
        }
        if octal.len() == 3 {
            let value = u32::from_str_radix(&octal, 8).expect("three octal digits parse");
            if value > 0o377 {
                anyhow::bail!(
                    "octal escape value \\{octal} outside of range 0-0o377 in the replacement"
                );
            }
            for _ in 0..3 {
                chars.next();
            }
            out.push(char::from_u32(value).expect("octal under 0o400 is a char"));
            continue;
        }
        match next {
            '\\' => {
                chars.next();
                out.push('\\');
            }
            'a' => {
                chars.next();
                out.push('\u{7}');
            }
            'b' => {
                chars.next();
                out.push('\u{8}');
            }
            'f' => {
                chars.next();
                out.push('\u{c}');
            }
            'n' => {
                chars.next();
                out.push('\n');
            }
            'r' => {
                chars.next();
                out.push('\r');
            }
            't' => {
                chars.next();
                out.push('\t');
            }
            'v' => {
                chars.next();
                out.push('\u{b}');
            }
            'g' => {
                chars.next();
                match chars.next() {
                    Some('<') => {}
                    Some(other) => anyhow::bail!(
                        "bad group reference \\g{other}: expected \\g<name> or \\g<1>"
                    ),
                    None => anyhow::bail!(
                        "bad group reference \\g at the end: expected \\g<name> or \\g<1>"
                    ),
                }
                let mut group = String::new();
                let mut closed = false;
                for ch in chars.by_ref() {
                    if ch == '>' {
                        closed = true;
                        break;
                    }
                    group.push(ch);
                }
                if !closed {
                    anyhow::bail!("missing >, unterminated name in \\g<{group} in the replacement");
                }
                if group.is_empty() {
                    anyhow::bail!("bad group reference \\g<>: the group name is empty");
                }
                if group.chars().all(|ch| ch.is_ascii_digit()) {
                    // `\g<0>` is the whole match and always valid; `\0` never reaches here
                    // (a leading zero takes the octal path above).
                    let number: usize = group.parse().unwrap_or(usize::MAX);
                    if number != 0 {
                        check_numbered(number, &group)?;
                    }
                } else if !named.contains(&group.as_str()) {
                    anyhow::bail!(
                        "invalid group reference \\g<{group}>: the pattern has no such group name"
                    );
                }
                out.push_str(&format!("${{{group}}}"));
            }
            '0'..='9' => {
                // At most two digits: `\12` is group 12, never group 1 plus a literal `2`,
                // and a leading `0` is octal here only when three digits follow (handled
                // above), so `\01` is the byte 0x01.
                let mut raw = String::new();
                for _ in 0..2 {
                    match chars.peek().copied() {
                        Some(digit) if digit.is_ascii_digit() => {
                            raw.push(digit);
                            chars.next();
                        }
                        _ => break,
                    }
                }
                if raw.starts_with('0') {
                    let value = u32::from_str_radix(&raw, 8).expect("octal digits parse");
                    out.push(char::from_u32(value).expect("octal under 0o400 is a char"));
                } else {
                    let number: usize = raw.parse().expect("digits parse");
                    check_numbered(number, &raw)?;
                    out.push_str(&format!("${{{raw}}}"));
                }
            }
            other if other.is_ascii_alphabetic() => {
                anyhow::bail!(
                    "bad escape \\{other} in the replacement: use \\1 or \\g<name> for a group, \
                     \\\\ for a backslash, or remove it"
                );
            }
            // Punctuation, whitespace, `$`, non-ASCII: Python keeps the backslash AND the
            // character, so the port copies both (doubling a `$` for Rust's expansion).
            other => {
                chars.next();
                out.push('\\');
                if other == '$' {
                    out.push_str("$$");
                } else {
                    out.push(other);
                }
            }
        }
    }
    Ok(out)
}

/// Stream-Mapparr's `_parse_tags`: comma-separated, with quotes keeping a COMMA inside a tag
/// (`"Breakfast, Lunch"`).
///
/// Quotes do not keep spaces: the plugin strips every tag it splits off, quoted or not, which
/// its own help text ("use quotes to preserve spaces") claims otherwise. The port follows the
/// code, since an installation's saved `ignore_tags` string was written against the code.
pub fn parse_ignore_tags(text: &str) -> Vec<String> {
    if text.trim().is_empty() {
        return Vec::new();
    }
    let mut tags: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    for ch in text.chars() {
        if (ch == '"' || ch == '\'') && (quote.is_none() || quote == Some(ch)) {
            quote = match quote {
                None => Some(ch),
                Some(_) => None,
            };
            continue;
        }
        if ch == ',' && quote.is_none() {
            let tag = current.trim();
            if !tag.is_empty() {
                tags.push(tag.to_string());
            }
            current.clear();
            continue;
        }
        current.push(ch);
    }
    // The last entry is trimmed too, except when a quote is still open: that is the plugin's
    // own asymmetry, kept because an operator's existing settings were written against it.
    let tag = if quote.is_some() {
        current.trim_end().to_string()
    } else {
        current.trim().to_string()
    };
    if !tag.is_empty() {
        tags.push(tag);
    }
    tags
}

fn truncate(text: &str) -> String {
    text.chars().take(80).collect()
}

// ---------------------------------------------------------------------------------------
// M3U accounts
// ---------------------------------------------------------------------------------------

/// The account half of the selection: the shared eligibility filter plus the plugin's ordered
/// priority list.
#[derive(Debug, Clone, Default)]
pub struct AccountSelection {
    /// Which accounts the list names, which are switched on, and which names matched nothing.
    pub sources: sources::Selection,
    /// Account id -> priority, 0 first, for the accounts the list named.
    pub priorities: HashMap<i64, usize>,
    /// The position a `*` entry stands at, when the list has one.
    pub other_priority: Option<usize>,
    /// Entries that took a position, in the order they were written, `*` included.
    pub order: Vec<String>,
}

impl AccountSelection {
    /// Whether a stream from this account may be used.
    ///
    /// A stream with no account at all (a hand-made row Dispatcharr did not attribute) is only
    /// eligible when the operator's list narrowed nothing — the plugin keeps every stream in
    /// that case, and dropping a custom stream silently would be worse.
    pub fn eligible(&self, account: Option<i64>) -> bool {
        match account {
            Some(id) => self.sources.is_eligible(id),
            // No account at all: admitted when the list names no particular accounts, which is
            // the empty list and the wildcard, exactly when the plugin's `other_priority`
            // stamps a priority on every stream.
            None => self.sources.named.is_none() || self.other_priority.is_some(),
        }
    }

    /// The priority of a stream from this account: its position in the list, the `*` position,
    /// or [`UNLISTED_PRIORITY`].
    pub fn priority(&self, account: Option<i64>) -> usize {
        account
            .and_then(|id| self.priorities.get(&id).copied())
            .or(self.other_priority)
            .unwrap_or(UNLISTED_PRIORITY)
    }

    /// The list in words, for the report preamble.
    pub fn describe(&self) -> String {
        if self.order.is_empty() {
            return "all active M3U accounts".to_string();
        }
        let named: Vec<String> = self
            .order
            .iter()
            .map(|token| {
                if token == WILDCARD {
                    "* (every other account)".to_string()
                } else {
                    format!("'{token}'")
                }
            })
            .collect();
        format!("M3U accounts {} in priority order", named.join(", "))
    }
}

/// Resolve `selected_m3us` against the accounts an install has.
///
/// A faithful port of `Plugin._resolve_m3u_priorities`: each token is looked up EXACTLY,
/// case-sensitively, in the install's account names. The only exception is a lone `*`, the
/// plugin's own "every other source" entry, which takes a position like any name. Anything
/// else — different case, a `*` inside a name, an account added later — matches nothing and
/// lands in the unknown-names warning, exactly as the plugin reports it missing.
///
/// Eligibility (named, and switched on) follows the shared [`sources::Selection`] shape: a
/// switched-off account is named but never eligible, and a list that names nothing usable
/// falls back to every active account with a warning, rather than failing the run. The REST
/// streams endpoint already excludes inactive accounts' streams server-side, so the switched-off
/// exclusion below is defense-in-depth over REST data; it still counts in the pool stats for
/// worlds built any other way.
pub fn select_accounts(accounts: &[M3uAccount], filter: &str) -> AccountSelection {
    // Name -> id once, so a token costs one lookup rather than a scan per account.
    let by_name: HashMap<&str, i64> = accounts
        .iter()
        .map(|account| (account.name.as_str(), account.id))
        .collect();

    let mut priorities: HashMap<i64, usize> = HashMap::new();
    let mut other_priority: Option<usize> = None;
    let mut order: Vec<String> = Vec::new();
    let mut unmatched: Vec<String> = Vec::new();
    let mut position = 0usize;
    let tokens = parse_tokens(filter);

    for token in &tokens {
        if token == WILDCARD {
            if other_priority.is_none() {
                other_priority = Some(position);
                order.push(token.clone());
                position += 1;
            }
            continue;
        }
        // The plugin's rule: `if token not in m3u_name_to_id: missing`. No case folding,
        // no globbing — a repeated entry counts once, at its first position.
        match by_name.get(token.as_str()).copied() {
            Some(id) => {
                if priorities.contains_key(&id) {
                    continue;
                }
                priorities.insert(id, position);
                order.push(token.clone());
                position += 1;
            }
            None => {
                if !unmatched.contains(token) {
                    unmatched.push(token.clone());
                }
            }
        }
    }

    let enabled: HashSet<i64> = accounts
        .iter()
        .filter(|account| account.is_active)
        .map(|account| account.id)
        .collect();
    // A caller that counts what the filter excluded can still tell "the operator named it and
    // it is off" from "the operator did not name it": `named` keeps switched-off accounts
    // the filter named, and `is_eligible` enforces the switched-on half.
    let mut named: HashSet<i64> = priorities.keys().copied().collect();
    if other_priority.is_some() {
        named.extend(accounts.iter().map(|account| account.id));
    }
    // Nothing usable named, but something written: the plugin warns and uses every stream,
    // so a typo selects everything rather than nothing.
    let fell_back = !tokens.is_empty() && order.is_empty();
    let sources = if order.is_empty() {
        if !unmatched.is_empty() {
            tracing::warn!(
                "selected_m3us names no M3U account this install has ({}); every active account \
                 is used instead",
                unmatched.join(", ")
            );
        }
        sources::Selection {
            named: None,
            enabled,
            unmatched,
            fell_back,
        }
    } else {
        sources::Selection {
            named: Some(named),
            enabled,
            unmatched,
            fell_back: false,
        }
    };

    AccountSelection {
        sources,
        priorities,
        other_priority,
        order,
    }
}

// ---------------------------------------------------------------------------------------
// Stream groups
// ---------------------------------------------------------------------------------------

/// Whether a group list names the groups to use or the groups to skip.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum GroupMode {
    #[default]
    Include,
    Exclude,
}

impl GroupMode {
    /// Resolve the setting. Anything unrecognized is `include`, which is the plugin's rule: the
    /// pre-existing behaviour, so a value a newer build wrote keeps a list of WANTED groups
    /// meaning wanted instead of inverting it into a list of skipped ones.
    pub fn from_setting(value: &str) -> Self {
        let wanted = value.trim().to_lowercase();
        if wanted == FILTER_MODES[1] {
            Self::Exclude
        } else {
            // Empty, `include`, and anything this build does not understand. Include is the
            // pre-existing behaviour, so a list of WANTED groups keeps meaning wanted.
            Self::Include
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Include => FILTER_MODES[0],
            Self::Exclude => FILTER_MODES[1],
        }
    }
}

/// The stream-group half of the selection.
#[derive(Debug, Clone, Default)]
pub struct StreamGroupSelection {
    pub mode: GroupMode,
    /// The ids of the groups the list named.
    pub ids: HashSet<i64>,
    /// The group names the list resolved to, in the operator's order.
    pub names: Vec<String>,
    /// Tokens that matched no stream group.
    pub unmatched: Vec<String>,
    /// The list named nothing usable, so every stream group was used instead.
    pub fell_back: bool,
}

impl StreamGroupSelection {
    /// Whether a stream in this group may be used.
    ///
    /// A stream with no group survives `exclude` and is dropped by `include`, which is what each
    /// mode means: it is not one of the named groups either way.
    pub fn admits(&self, group: Option<i64>) -> bool {
        if self.ids.is_empty() {
            return true;
        }
        let named = group.is_some_and(|id| self.ids.contains(&id));
        match self.mode {
            GroupMode::Include => named,
            GroupMode::Exclude => !named,
        }
    }

    /// The list in words, for the report preamble.
    pub fn describe(&self) -> String {
        if self.ids.is_empty() {
            return "all stream groups".to_string();
        }
        let names: Vec<String> = self.names.iter().map(|name| format!("'{name}'")).collect();
        match self.mode {
            GroupMode::Include => format!("in stream groups {}", names.join(", ")),
            GroupMode::Exclude => format!("ignoring stream groups {}", names.join(", ")),
        }
    }
}

/// Resolve the stream-group list.
///
/// The plugin's exact lookup: each token is compared case-sensitively against EVERY channel
/// group Dispatcharr knows (`group_name_to_id`), not only the groups streams are in — a named
/// group that currently holds no streams is still a valid name and filters everything out,
/// rather than falling back to all groups. A token that names no group is unknown, and a list
/// that names nothing usable falls back to every group with a warning, as the plugin does.
/// There are no wildcards: a token carrying `*` matches no group and lands in the
/// unknown-names warning instead of selecting a family the plugin would have fallen back past
/// (in `exclude` mode a glob `*` would otherwise drop EVERY stream where the plugin uses all).
pub fn select_stream_groups(world: &World, filter: &str, mode: &str) -> StreamGroupSelection {
    let mode = GroupMode::from_setting(mode);
    let tokens = parse_tokens(filter);
    if tokens.is_empty() {
        return StreamGroupSelection {
            mode,
            ..StreamGroupSelection::default()
        };
    }

    // Name -> id once, so a token costs one lookup rather than a scan per group.
    let by_name: HashMap<&str, i64> = world
        .groups
        .iter()
        .map(|group| (group.name.as_str(), group.id))
        .collect();
    let mut matched: Vec<String> = Vec::new();
    let mut unmatched: Vec<String> = Vec::new();
    for token in tokens {
        if by_name.contains_key(token.as_str()) {
            if !matched.contains(&token) {
                matched.push(token);
            }
        } else if !unmatched.contains(&token) {
            unmatched.push(token);
        }
    }
    if matched.is_empty() {
        return StreamGroupSelection {
            mode,
            ids: HashSet::new(),
            names: Vec::new(),
            unmatched,
            fell_back: true,
        };
    }
    let ids: HashSet<i64> = matched
        .iter()
        .filter_map(|name| by_name.get(name.as_str()).copied())
        .collect();

    StreamGroupSelection {
        mode,
        ids,
        names: matched,
        unmatched,
        fell_back: false,
    }
}

// ---------------------------------------------------------------------------------------
// The pool
// ---------------------------------------------------------------------------------------

/// What the selection did, for the report preamble, the log and the counts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Streams Dispatcharr reported.
    pub streams: usize,
    /// Streams a match may draw from.
    pub eligible: usize,
    /// Streams dropped because their M3U account is switched off.
    pub dropped_inactive_account: usize,
    /// Streams dropped because the account list did not name their account.
    pub dropped_account: usize,
    /// Streams dropped by the stream-group filter.
    pub dropped_stream_group: usize,
    /// What the regex rules did.
    pub regex: RegexRun,
}

/// Every stream a run may draw from, indexed once.
///
/// Built with hash maps on purpose: a match run reads the pool several times per channel, and a
/// linear scan of 50k streams per channel is the shape that made the plugin too slow.
#[derive(Debug, Clone, Default)]
pub struct StreamPool<'a> {
    /// Every stream Dispatcharr reported, by id, borrowed from the world.
    ///
    /// Borrowed rather than cloned: fifty thousand streams with their `stream_stats` blobs is
    /// tens of megabytes, and the world outlives the pool for the whole run. The whole set, not
    /// only the eligible part, because `keep_custom_streams`, restore and mirror all need to
    /// read a stream a channel already carries even when this run may not draw from it.
    pub streams: HashMap<i64, &'a Stream>,
    /// Eligible stream ids, in the order Dispatcharr listed them (which is stable, so a run
    /// without rules produces the same order twice).
    pub eligible: Vec<i64>,
    /// The same set, for a membership test per candidate.
    pub eligible_ids: HashSet<i64>,
    /// M3U priority per eligible stream: 0 is the operator's first account.
    pub priority: HashMap<i64, usize>,
    /// The name MATCHING sees per eligible stream, after the regex rules.
    pub match_name: HashMap<i64, String>,
    /// Eligible stream ids by cleaned name, so the collector can find a name family without
    /// walking the pool.
    pub by_match_name: HashMap<String, Vec<i64>>,
    /// M3U account id -> name, for report labels.
    pub account_names: HashMap<i64, String>,
    /// The selection in words, for the report preamble.
    pub description: String,
    pub stats: PoolStats,
    /// What an operator should be told: unknown names, regex containment, a fallback.
    pub warnings: Vec<String>,
}

impl<'a> StreamPool<'a> {
    pub fn len(&self) -> usize {
        self.eligible.len()
    }

    pub fn is_empty(&self) -> bool {
        self.eligible.is_empty()
    }

    /// One stream, eligible or not.
    pub fn get(&self, id: i64) -> Option<&'a Stream> {
        self.streams.get(&id).copied()
    }

    pub fn is_eligible(&self, id: i64) -> bool {
        self.eligible_ids.contains(&id)
    }

    /// A stream as the quality key sees it: its stats, custom flag and this run's M3U
    /// priority (an id the pool does not know is unlisted, with no stats). The one view
    /// `match` ranks a channel's streams on and `lineup.create` orders a new channel's by.
    pub fn quality_view(&self, id: i64) -> crate::quality::StreamView<'a> {
        match self.get(id) {
            Some(stream) => crate::quality::StreamView {
                id,
                name: &stream.name,
                stats: Some(&stream.stream_stats),
                m3u_priority: self.priority.get(&id).copied().unwrap_or(UNLISTED_PRIORITY),
                is_custom: stream.is_custom,
            },
            None => crate::quality::StreamView {
                id,
                name: "",
                stats: None,
                m3u_priority: UNLISTED_PRIORITY,
                is_custom: false,
            },
        }
    }

    /// The name MATCHING sees for a stream, or `None` when it is not eligible this run.
    pub fn match_name_of(&self, id: i64) -> Option<&str> {
        self.match_name.get(&id).map(String::as_str)
    }

    /// A stream's M3U account name, for a CSV label.
    ///
    /// The plugin's `_m3u_name_map` fallback is kept: an id the account list did not carry
    /// prints as `m3u-<id>` rather than as a wrong name, and a hand-made stream with no account
    /// at all prints as `custom`.
    pub fn account_name(&self, stream: &Stream) -> String {
        match stream.m3u_account {
            Some(id) => self
                .account_names
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("m3u-{id}")),
            None => "custom".to_string(),
        }
    }
}

/// Resolve the stream half of the scope: accounts, stream groups, then the regex rules.
///
/// Fails only on a world that was loaded without the slices this needs; every filter problem is
/// the plugin's degrade — an unknown name is warned about and the run proceeds.
pub fn build<'a>(world: &'a World, settings: &StreamsSettings) -> Result<StreamPool<'a>> {
    world.require(WorldKind::Streams)?;
    world.require(WorldKind::M3uAccounts)?;

    let accounts = select_accounts(&world.m3u_accounts, &settings.selected_m3us);
    // The group filter resolves names through the group list, so a world that never loaded it
    // would fall back to "every stream group" and read as if the filter were off.
    if !parse_tokens(&settings.selected_stream_groups).is_empty() {
        world.require(WorldKind::Groups)?;
    }
    let groups = select_stream_groups(
        world,
        &settings.selected_stream_groups,
        &settings.stream_group_filter_mode,
    );

    let rules = RuleSet::parse(&settings.stream_name_regex_rules);

    let mut pool = StreamPool {
        streams: HashMap::with_capacity(world.streams.len()),
        account_names: world
            .m3u_accounts
            .iter()
            .map(|account| (account.id, account.name.clone()))
            .collect(),
        description: format!("{}, {}", accounts.describe(), groups.describe()),
        stats: PoolStats {
            streams: world.streams.len(),
            ..PoolStats::default()
        },
        ..StreamPool::default()
    };

    let mut warnings: Vec<String> = Vec::new();
    if accounts.sources.fell_back {
        warnings.push(format!(
            "selected_m3us named no account this install has ({}); every active account is \
             used instead",
            if accounts.sources.unmatched.is_empty() {
                "empty list".to_string()
            } else {
                accounts.sources.unmatched.join(", ")
            }
        ));
    } else if !accounts.sources.unmatched.is_empty() {
        warnings.push(format!(
            "selected_m3us names no M3U account called {}",
            accounts
                .sources
                .unmatched
                .iter()
                .map(|name| format!("'{name}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if groups.fell_back {
        warnings.push(format!(
            "selected_stream_groups named no stream group this install has ({}); every stream \
             group is used instead",
            groups.unmatched.join(", ")
        ));
    } else if !groups.unmatched.is_empty() {
        warnings.push(format!(
            "selected_stream_groups names no stream group called {}",
            groups
                .unmatched
                .iter()
                .map(|name| format!("'{name}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    // One pass over the streams: every filter is a lookup, so this is O(streams), not
    // O(streams × accounts).
    let mut eligible_names: Vec<(i64, &str)> = Vec::with_capacity(world.streams.len());
    for stream in &world.streams {
        if !groups.admits(stream.channel_group) {
            pool.stats.dropped_stream_group += 1;
            continue;
        }
        if !accounts.eligible(stream.m3u_account) {
            // Which of the two reasons it was: an account the list did not name, or one that
            // exists and is switched off. A different message, so `validate` can explain a run
            // that found nothing.
            if stream
                .m3u_account
                .is_some_and(|id| accounts.sources.enabled.contains(&id))
            {
                pool.stats.dropped_account += 1;
            } else {
                pool.stats.dropped_inactive_account += 1;
            }
            continue;
        }
        pool.priority
            .insert(stream.id, accounts.priority(stream.m3u_account));
        pool.eligible.push(stream.id);
        eligible_names.push((stream.id, stream.name.as_str()));
    }
    pool.stats.eligible = pool.eligible.len();

    let (cleaned, regex) = rules.apply(&eligible_names);
    pool.stats.regex = regex;
    if let Some(summary) = pool.stats.regex.summary() {
        warnings.push(summary);
    }
    pool.eligible_ids = pool.eligible.iter().copied().collect();
    for (id, name) in cleaned {
        pool.by_match_name.entry(name.clone()).or_default().push(id);
        pool.match_name.insert(id, name);
    }
    // Keyed insertion order is not deterministic (`HashMap`), so the name index is sorted: a
    // report or a test that reads it gets the same order twice.
    for ids in pool.by_match_name.values_mut() {
        ids.sort_unstable();
    }
    pool.streams = world
        .streams
        .iter()
        .map(|stream| (stream.id, stream))
        .collect();
    pool.warnings = warnings;
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;

    fn tag(value: &str) -> Vec<String> {
        parse_ignore_tags(value)
    }

    #[test]
    fn the_wildcard_keeps_every_account_and_takes_a_position() {
        // "Free Provider, *": the named account first, every other account after it. The
        // plugin's own example from the setting's help text.
        let accounts = test_support::accounts();
        let selection = select_accounts(&accounts, "Main Provider, *");
        let main = test_support::PROVIDER_A;
        let backup = test_support::PROVIDER_B;
        assert_eq!(selection.priority(Some(main)), 0);
        assert_eq!(
            selection.priority(Some(backup)),
            1,
            "* stands at the second position"
        );
        assert_eq!(selection.order, vec!["Main Provider", "*"]);
        assert!(selection.eligible(Some(main)) && selection.eligible(Some(backup)));
    }

    #[test]
    fn without_a_wildcard_only_the_named_accounts_are_eligible() {
        let accounts = test_support::accounts();
        let selection = select_accounts(&accounts, "Backup Provider");
        assert!(selection.eligible(Some(test_support::PROVIDER_B)));
        assert!(!selection.eligible(Some(test_support::PROVIDER_A)));
        assert_eq!(selection.priority(Some(test_support::PROVIDER_B)), 0);
        // An account the list did not name has no position at all.
        assert_eq!(
            selection.priority(Some(test_support::PROVIDER_A)),
            UNLISTED_PRIORITY
        );
        assert!(selection.sources.unmatched.is_empty());
    }

    #[test]
    fn a_disabled_account_is_named_but_never_eligible() {
        let accounts = test_support::accounts();
        let selection = select_accounts(&accounts, "Retired Provider, *");
        assert!(!selection.eligible(Some(test_support::PROVIDER_OFF)));
        // It still took a priority position: the setting says where it would rank.
        assert_eq!(selection.priority(Some(test_support::PROVIDER_OFF)), 0);
        assert!(selection.eligible(Some(test_support::PROVIDER_A)));
    }

    #[test]
    fn an_unknown_account_is_warned_about_and_falls_back_to_every_account() {
        let accounts = test_support::accounts();
        let selection = select_accounts(&accounts, "Nowhere TV");
        assert_eq!(selection.sources.unmatched, vec!["Nowhere TV".to_string()]);
        assert!(selection.sources.fell_back);
        // Every enabled account is eligible, exactly as the plugin proceeds with everything.
        assert!(selection.eligible(Some(test_support::PROVIDER_A)));
        assert!(selection.eligible(Some(test_support::PROVIDER_B)));
        assert!(!selection.eligible(Some(test_support::PROVIDER_OFF)));
        assert_eq!(
            selection.priority(Some(test_support::PROVIDER_A)),
            UNLISTED_PRIORITY
        );
    }

    #[test]
    fn account_names_match_exactly_like_the_plugin() {
        // The plugin looks each token up in an exact `{name: id}` map: different case is a
        // different name, and a glob is just a name no account carries. Both land in the
        // unknown-names warning, and a list that names nothing usable falls back to every
        // account — exactly as the plugin warns and proceeds with everything.
        let accounts = test_support::accounts();
        let selection = select_accounts(&accounts, "main provider");
        assert_eq!(
            selection.sources.unmatched,
            vec!["main provider".to_string()]
        );
        assert!(selection.sources.fell_back);
        assert!(selection.eligible(Some(test_support::PROVIDER_A)));
        assert_eq!(
            selection.priority(Some(test_support::PROVIDER_A)),
            UNLISTED_PRIORITY,
            "a fallback names nothing, so nothing has a position"
        );

        let selection = select_accounts(&accounts, "Main*");
        assert_eq!(selection.sources.unmatched, vec!["Main*".to_string()]);
        assert!(selection.sources.fell_back);
    }

    #[test]
    fn a_lone_star_is_the_only_wildcard_and_takes_a_position() {
        // `*` is the plugin's own "every other source" entry, not a glob: an account
        // literally named `*` would be unreachable through the setting, which is the same
        // trade-off the plugin makes.
        let accounts = test_support::accounts();
        let selection = select_accounts(&accounts, "Main Provider, *");
        assert!(selection.sources.unmatched.is_empty());
        assert!(!selection.sources.fell_back);
        assert_eq!(selection.order, vec!["Main Provider", "*"]);
    }

    #[test]
    fn an_empty_account_list_uses_every_active_account() {
        let accounts = test_support::accounts();
        for filter in ["", "   "] {
            let selection = select_accounts(&accounts, filter);
            assert!(selection.eligible(Some(test_support::PROVIDER_A)));
            assert!(selection.eligible(Some(test_support::PROVIDER_B)));
            assert!(!selection.eligible(Some(test_support::PROVIDER_OFF)));
            assert_eq!(
                selection.priority(Some(test_support::PROVIDER_A)),
                UNLISTED_PRIORITY
            );
            assert_eq!(selection.describe(), "all active M3U accounts");
        }
    }

    #[test]
    fn a_stream_with_no_account_is_only_eligible_when_the_list_narrowed_nothing() {
        let accounts = test_support::accounts();
        assert!(select_accounts(&accounts, "").eligible(None));
        assert!(!select_accounts(&accounts, "Main Provider").eligible(None));
        assert!(select_accounts(&accounts, "*").eligible(None));
    }

    #[test]
    fn a_duplicate_entry_counts_once_at_its_first_position() {
        // The plugin's rule: `if account_id in priority_map: continue`.
        let accounts = test_support::accounts();
        let selection =
            select_accounts(&accounts, "Backup Provider, Main Provider, Backup Provider");
        assert_eq!(selection.priority(Some(test_support::PROVIDER_B)), 0);
        assert_eq!(selection.priority(Some(test_support::PROVIDER_A)), 1);
        assert_eq!(selection.order, vec!["Backup Provider", "Main Provider"]);
    }

    #[test]
    fn a_named_group_with_no_streams_is_still_a_valid_name() {
        // The plugin resolves against every channel group, not only the ones streams are in:
        // PPV exists but carries no streams, so it resolves — and then filters everything
        // out, exactly as the plugin's group filter would leave an empty pool.
        let world = test_support::world();
        let groups = select_stream_groups(&world, "PPV", "include");
        assert!(
            !groups.fell_back,
            "PPV exists, so the list named something usable"
        );
        assert_eq!(groups.ids, HashSet::from([test_support::PPV]));
        assert!(!groups.admits(Some(test_support::TVE)));
        assert!(!groups.admits(Some(test_support::CABLE)));
    }

    #[test]
    fn the_stream_group_include_mode_keeps_only_the_named_groups() {
        let world = test_support::world();
        let groups = select_stream_groups(&world, "TVE", "include");
        assert!(groups.admits(Some(test_support::TVE)));
        assert!(!groups.admits(Some(test_support::CABLE)));
        assert!(
            !groups.admits(None),
            "a stream with no group is in no named group"
        );
        assert_eq!(groups.describe(), "in stream groups 'TVE'");
    }

    #[test]
    fn the_stream_group_exclude_mode_drops_the_named_groups_and_keeps_the_rest() {
        let world = test_support::world();
        let groups = select_stream_groups(&world, "Cable", "exclude");
        assert!(!groups.admits(Some(test_support::CABLE)));
        assert!(groups.admits(Some(test_support::TVE)));
        assert!(
            groups.admits(None),
            "exclude mode keeps a stream with no group"
        );
        assert_eq!(groups.describe(), "ignoring stream groups 'Cable'");
    }

    #[test]
    fn stream_group_names_are_case_sensitive_with_no_wildcards() {
        // The plugin's `{name: id}` lookup is exact: `tve` is unknown (and falls back to
        // every group), and a glob is just a name no group carries. In `exclude` mode a `*`
        // must NOT drop every stream: the plugin has no wildcard here, so `*` is unknown and
        // the run uses all groups.
        let world = test_support::world();
        let groups = select_stream_groups(&world, "tve", "include");
        assert!(groups.ids.is_empty());
        assert!(groups.fell_back);
        assert_eq!(groups.unmatched, vec!["tve".to_string()]);

        for filter in ["TV*", "*"] {
            let groups = select_stream_groups(&world, filter, "include");
            assert!(groups.fell_back, "{filter} names no group");
            assert!(groups.admits(Some(test_support::TVE)));
        }
        let groups = select_stream_groups(&world, "*", "exclude");
        assert!(groups.fell_back);
        assert!(groups.admits(Some(test_support::TVE)));
        assert!(groups.admits(Some(test_support::CABLE)));
    }

    #[test]
    fn an_empty_stream_group_list_uses_every_group() {
        let world = test_support::world();
        let groups = select_stream_groups(&world, "", "exclude");
        assert!(groups.admits(Some(test_support::TVE)));
        assert!(groups.admits(None));
        assert_eq!(groups.describe(), "all stream groups");
        assert_eq!(
            groups.mode,
            GroupMode::Exclude,
            "the mode is still resolved"
        );
    }

    #[test]
    fn an_unusable_stream_group_mode_is_read_as_include() {
        let world = test_support::world();
        let groups = select_stream_groups(&world, "TVE", "skip");
        assert_eq!(groups.mode, GroupMode::Include);
        assert!(groups.admits(Some(test_support::TVE)));
        assert!(!groups.admits(Some(test_support::CABLE)));
    }

    #[test]
    fn the_pool_keeps_every_stream_and_indexes_the_eligible_ones() {
        let world = test_support::world();
        let pool = build(&world, &StreamsSettings::default()).expect("pool");
        // Every stream, so restore and mirror can read one this run may not draw from.
        assert_eq!(pool.streams.len(), world.streams.len());
        // The stream on the disabled account is not eligible.
        assert!(!pool.is_eligible(test_support::CNN_OFF));
        assert_eq!(pool.stats.dropped_inactive_account, 1);
        assert_eq!(pool.stats.eligible, world.streams.len() - 1);
        // The custom stream survives an empty list.
        assert!(pool.is_eligible(test_support::CUSTOM_STREAM));
    }

    #[test]
    fn priority_order_follows_the_account_list() {
        let world = test_support::world();
        let settings = StreamsSettings {
            selected_m3us: "Backup Provider, Main Provider".to_string(),
            ..StreamsSettings::default()
        };
        let pool = build(&world, &settings).expect("pool");
        assert_eq!(pool.priority[&test_support::ESPN_HD], 1);
        assert_eq!(pool.priority[&test_support::ESPN], 0);
    }

    #[test]
    fn a_named_account_list_drops_the_accounts_it_does_not_name() {
        let world = test_support::world();
        let settings = StreamsSettings {
            selected_m3us: "Main Provider".to_string(),
            ..StreamsSettings::default()
        };
        let pool = build(&world, &settings).expect("pool");
        assert!(pool.is_eligible(test_support::ESPN_HD));
        assert!(pool.is_eligible(test_support::ESPN_4K));
        // The backup account's copy and the custom stream are both unnamed.
        assert!(!pool.is_eligible(test_support::ESPN));
        assert!(!pool.is_eligible(test_support::CUSTOM_STREAM));
        assert_eq!(pool.stats.dropped_account, 2);
        // The retired account is counted apart, because "switched off" is a different fix from
        // "not in the list".
        assert_eq!(pool.stats.dropped_inactive_account, 1);
    }

    #[test]
    fn the_stream_group_filter_drops_whole_groups() {
        let world = test_support::world();
        let settings = StreamsSettings {
            selected_stream_groups: "Cable".to_string(),
            stream_group_filter_mode: "exclude".to_string(),
            ..StreamsSettings::default()
        };
        let pool = build(&world, &settings).expect("pool");
        assert!(
            pool.is_eligible(test_support::ESPN_HD),
            "TVE survives exclude"
        );
        assert!(!pool.is_eligible(test_support::ESPN), "Cable is dropped");
        // Two streams are in Cable, and the group filter runs before the account filter: the
        // switched-off account's `CNN` is counted here, not as an inactive account.
        assert_eq!(pool.stats.dropped_stream_group, 2);
        assert_eq!(pool.stats.dropped_inactive_account, 0);
    }

    #[test]
    fn the_pool_builds_a_name_index_from_the_matching_names() {
        let world = test_support::world();
        let settings = StreamsSettings {
            stream_name_regex_rules: r#"[["\\s*\\[4K\\]\\s*", ""], ["\\s+HD$", ""]]"#.to_string(),
            ..StreamsSettings::default()
        };
        let pool = build(&world, &settings).expect("pool");
        assert_eq!(pool.match_name_of(test_support::ESPN_4K), Some("ESPN"));
        assert_eq!(pool.match_name_of(test_support::ESPN_HD), Some("ESPN"));
        // All three copies clean to ESPN: two accounts' copies and the 4K one.
        let family = pool.by_match_name.get("ESPN").expect("a family");
        assert_eq!(family.len(), 3, "{family:?}");
        assert!(family.contains(&test_support::ESPN_HD));
        assert!(family.contains(&test_support::ESPN));
        assert!(family.contains(&test_support::ESPN_4K));
        // The raw name is still what the pool knows Dispatcharr carries.
        assert_eq!(
            pool.get(test_support::ESPN_4K).expect("stream").name,
            "ESPN [4K]"
        );
    }

    #[test]
    fn the_name_index_is_sorted_so_two_runs_agree() {
        let world = test_support::world();
        let pool = build(&world, &StreamsSettings::default()).expect("pool");
        for ids in pool.by_match_name.values() {
            let mut sorted = ids.clone();
            sorted.sort_unstable();
            assert_eq!(*ids, sorted);
        }
    }

    #[test]
    fn an_empty_pool_is_not_an_error_but_says_why() {
        let world = test_support::world();
        let settings = StreamsSettings {
            selected_stream_groups: "Nope".to_string(),
            ..StreamsSettings::default()
        };
        // A group list that names nothing falls back to every group, as the plugin does.
        let pool = build(&world, &settings).expect("pool");
        assert!(!pool.is_empty());
        assert!(
            pool.warnings.iter().any(|w| w.contains("Nope")),
            "{:?}",
            pool.warnings
        );
    }

    #[test]
    fn a_world_without_streams_is_refused_rather_than_read_as_empty() {
        let mut world = test_support::world();
        world.loaded = crosstalk_core::dispatcharr::WorldKinds::only(WorldKind::Channels);
        let error = build(&world, &StreamsSettings::default()).expect_err("refused");
        assert!(format!("{error}").contains("Streams"), "{error}");
    }

    #[test]
    fn the_pool_holds_the_plan_scale() {
        // ~50k streams and ~100 accounts, the plan's stated scale. What this pins is the SHAPE,
        // not a number: every filter is a lookup, so the run stays linear. A walk of the pool
        // per account or per token would take minutes here rather than milliseconds, and a
        // debug build is the slowest it ever gets.
        let mut accounts = Vec::with_capacity(100);
        for id in 0..100 {
            accounts.push(M3uAccount {
                id,
                name: format!("Provider {id}"),
                is_active: true,
                ..M3uAccount::default()
            });
        }
        let streams: Vec<Stream> = (0..50_000)
            .map(|id| Stream {
                id,
                name: format!("Channel {} HD", id % 20_000),
                m3u_account: Some(id % 100),
                channel_group: Some(id % 3),
                ..Stream::default()
            })
            .collect();
        let world = World {
            m3u_accounts: accounts,
            groups: (0..3)
                .map(|id| crosstalk_core::dispatcharr::ChannelGroup {
                    id,
                    name: format!("Group {id}"),
                })
                .collect(),
            streams,
            ..World::default()
        };

        let settings = StreamsSettings {
            selected_m3us: "Provider 0, Provider 1, *".to_string(),
            selected_stream_groups: "Group 0, Group 1".to_string(),
            stream_name_regex_rules: r#"[["\\s+HD$", ""]]"#.to_string(),
            ..StreamsSettings::default()
        };
        let started = Instant::now();
        let pool = build(&world, &settings).expect("pool");
        let elapsed = started.elapsed();

        assert_eq!(pool.stats.streams, 50_000);
        // Group 2 is excluded, so two thirds of the pool survives.
        assert!(pool.stats.eligible > 30_000, "{:?}", pool.stats);
        assert_eq!(pool.eligible_ids.len(), pool.stats.eligible);
        assert_eq!(pool.match_name.len(), pool.stats.eligible);
        // The 20k-name index collapses the pool into families.
        assert!(pool.by_match_name.len() <= 20_000);
        // Generous by orders of magnitude: the point is to fail on a quadratic regression, not
        // to time the machine.
        assert!(elapsed.as_secs() < 20, "building the pool took {elapsed:?}");
    }

    #[test]
    fn the_pool_describes_what_it_selected() {
        let world = test_support::world();
        let settings = StreamsSettings {
            selected_m3us: "Main Provider, *".to_string(),
            selected_stream_groups: "TVE".to_string(),
            ..StreamsSettings::default()
        };
        let pool = build(&world, &settings).expect("pool");
        assert_eq!(
            pool.description,
            "M3U accounts 'Main Provider', * (every other account) in priority order, in stream \
             groups 'TVE'"
        );
    }

    #[test]
    fn an_unmatched_account_name_is_a_warning_not_a_failure() {
        let world = test_support::world();
        let settings = StreamsSettings {
            selected_m3us: "Main Provider, Ghost TV".to_string(),
            ..StreamsSettings::default()
        };
        let pool = build(&world, &settings).expect("pool");
        assert!(
            pool.warnings.iter().any(|w| w.contains("Ghost TV")),
            "{:?}",
            pool.warnings
        );
        assert!(pool.is_eligible(test_support::ESPN_HD));
    }

    // --- regex rules -------------------------------------------------------------------

    #[test]
    fn rules_are_applied_in_order() {
        let rules = RuleSet::parse(r#"[["\\[HD\\]", ""], ["\\s+", " "]]"#);
        assert_eq!(rules.rules.len(), 2);
        let names = [(1i64, "ESPN  [HD]  Extra".to_string())];
        let pairs: Vec<(i64, &str)> = names
            .iter()
            .map(|(id, name)| (*id, name.as_str()))
            .collect();
        let (cleaned, counters) = rules.apply(&pairs);
        assert_eq!(
            cleaned[&1], "ESPN Extra",
            "the second rule collapses the gap"
        );
        assert_eq!(counters.changed, 1);
    }

    #[test]
    fn python_group_references_in_a_replacement_work() {
        // `\1` and `\g<name>` are what the plugin's help text promises.
        let rules = RuleSet::parse(r#"[["^(\\w+) HD$", "\\1"]]"#);
        assert!(rules.rejected().count() == 0, "{:?}", rules.report);
        let pairs = vec![(1i64, "ESPN HD")];
        let (cleaned, _) = rules.apply(&pairs);
        assert_eq!(cleaned[&1], "ESPN");

        let rules = RuleSet::parse(r#"[["^(?<base>\\w+) HD$", "\\g<base>"]]"#);
        assert!(rules.rejected().count() == 0, "{:?}", rules.report);
        let (cleaned, _) = rules.apply(&pairs);
        assert_eq!(cleaned[&1], "ESPN");

        // A literal $ is not a group reference.
        let rules = RuleSet::parse(r#"[["^(\\w+) HD$", "\\1 5$"]]"#);
        let (cleaned, _) = rules.apply(&pairs);
        assert_eq!(cleaned[&1], "ESPN 5$");
    }

    #[test]
    fn a_python_only_pattern_is_refused_with_an_explanation() {
        // Lookahead: Python has it, Rust's regex does not. The plugin would run the rule; the
        // port has to say so rather than match something else.
        let rules = RuleSet::parse(r#"[["ESPN(?= HD)", "X"]]"#);
        assert!(rules.rules.is_empty());
        let rejected: Vec<_> = rules.rejected().collect();
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].status, "compile_error");
        assert!(
            rejected[0].detail.contains("lookaround"),
            "{}",
            rejected[0].detail
        );
        assert!(rules.rejection_summary().unwrap().contains("rule 1"));
    }

    #[test]
    fn a_bad_replacement_escape_is_refused_once() {
        // Python raises `bad escape \d` on every name it applies the rule to; refusing the rule
        // says it once.
        let rules = RuleSet::parse(r#"[["HD", "\\d"]]"#);
        assert!(rules.rules.is_empty());
        let rejected: Vec<_> = rules.rejected().collect();
        assert_eq!(rejected[0].status, "bad_escape");
        assert!(rejected[0].detail.contains("\\d"), "{}", rejected[0].detail);
    }

    #[test]
    fn the_rule_gates_are_the_plugins() {
        // Not a JSON list at all: the feature is disabled with one report line.
        let rules = RuleSet::parse("not json");
        assert!(rules.rules.is_empty());
        assert_eq!(rules.report.len(), 1);
        assert_eq!(rules.report[0].status, "invalid_json_shape");

        // A shape that is not [pattern, replacement].
        let rules = RuleSet::parse(r#"[["only-one"], ["a", 1]]"#);
        assert_eq!(rules.rules.len(), 0);
        assert!(rules
            .rejected()
            .all(|entry| entry.status == "invalid_json_shape"));

        // An empty pattern matches everywhere: refused, as in the plugin.
        let rules = RuleSet::parse(r#"[["", "x"]]"#);
        assert_eq!(rules.rejected().next().unwrap().status, "empty_pattern");

        // An empty replacement is fine: that is how a tag is stripped.
        let rules = RuleSet::parse(r#"[["x", ""]]"#);
        assert_eq!(rules.rules.len(), 1);

        // Over the length cap.
        let long = "a".repeat(REGEX_PATTERN_MAX_LEN + 1);
        let rules = RuleSet::parse(&format!("[[\"{long}\", \"\"]]"));
        assert_eq!(rules.rejected().next().unwrap().status, "too_long");

        // Over the rule cap: the first REGEX_RULES_MAX survive, the rest are reported.
        let entries: Vec<String> = (0..REGEX_RULES_MAX + 2)
            .map(|index| format!("[\"p{index}\", \"\"]"))
            .collect();
        let rules = RuleSet::parse(&format!("[{}]", entries.join(",")));
        assert_eq!(rules.rules.len(), REGEX_RULES_MAX);
        assert_eq!(rules.rejected().count(), 2);
    }

    #[test]
    fn a_long_name_is_skipped_by_the_containment() {
        let rules = RuleSet::parse(r#"[["X", "Y"]]"#);
        let long = "X".repeat(REGEX_NAME_MAX_LEN + 1);
        let pairs = vec![(1i64, long.as_str())];
        let (cleaned, counters) = rules.apply(&pairs);
        assert_eq!(cleaned[&1], long, "left exactly as it was");
        assert_eq!(counters.skipped_long, 1);
        assert!(counters.summary().unwrap().contains("characters skipped"));
    }

    #[test]
    fn a_rule_that_blows_a_name_up_is_reverted_and_stops() {
        // `(a)` -> `$1$1` doubles the name. Under the cap that is a normal rewrite...
        let rules = RuleSet::parse(r#"[["(a)", "\\1\\1"]]"#);
        let name = "a".repeat(300);
        let pairs = vec![(1i64, name.as_str())];
        let (cleaned, counters) = rules.apply(&pairs);
        assert_eq!(cleaned[&1].chars().count(), 600);
        assert_eq!(counters.growth_reverted, 0);

        // ...and past max(4x, 1000) it reverts to what the rule received and stops there,
        // which is the plugin's `growth_reverted` behaviour.
        let rules = RuleSet::parse(&format!(
            r#"[["a", "{}"], ["a", "zzz"]]"#,
            "b".repeat(REGEX_PATTERN_MAX_LEN)
        ));
        let pairs = vec![(1i64, name.as_str())];
        let (cleaned, counters) = rules.apply(&pairs);
        assert_eq!(counters.growth_reverted, 1, "{:?}", counters);
        assert_eq!(cleaned[&1], name, "the name is left as it arrived");
        assert_eq!(counters.changed, 0, "nothing was written for this name");
    }

    #[test]
    fn an_emptied_name_is_counted_because_nothing_can_match_it() {
        let rules = RuleSet::parse(r#"[["^.*$", ""]]"#);
        let pairs = vec![(1i64, "ESPN")];
        let (cleaned, counters) = rules.apply(&pairs);
        assert_eq!(cleaned[&1], "");
        assert_eq!(counters.emptied, 1);
        assert!(counters.summary().unwrap().contains("unmatchable"));
    }

    #[test]
    fn no_rules_stamp_the_raw_name() {
        let rules = RuleSet::parse("");
        let pairs = vec![(7i64, "ESPN HD")];
        let (cleaned, counters) = rules.apply(&pairs);
        assert_eq!(cleaned[&7], "ESPN HD");
        assert_eq!(counters, RegexRun::default());
        assert!(counters.summary().is_none());
    }

    #[test]
    fn ignore_tags_split_on_commas_outside_quotes() {
        // The plugin's own placeholder, from the setting's help text. Every tag is trimmed,
        // quoted or not, which is what `_parse_tags` does.
        assert_eq!(
            tag(r#"4K, [4K], " East", "[Dead]""#),
            vec![
                "4K".to_string(),
                "[4K]".to_string(),
                "East".to_string(),
                "[Dead]".to_string()
            ]
        );
        // Quoting keeps a COMMA inside one tag, which is what it is for.
        assert_eq!(
            tag(r#""Breakfast, Lunch", 4K"#),
            vec!["Breakfast, Lunch".to_string(), "4K".to_string()]
        );
        assert_eq!(tag("  "), Vec::<String>::new());
        assert_eq!(tag(",,"), Vec::<String>::new());
        // Single quotes work too.
        assert_eq!(
            tag("' East', 4K"),
            vec!["East".to_string(), "4K".to_string()]
        );
        // An unterminated quote keeps what it opened.
        assert_eq!(
            tag("\" East"),
            vec![" East".to_string()],
            "a still-open quote is not trimmed"
        );
    }

    #[test]
    fn the_replacement_translation_covers_the_escapes() {
        // One group: `\1` is valid, `\2` is refused the way Python raises `invalid group
        // reference` — at parse, once, rather than per stream.
        let one = Regex::new(r"(a)").expect("test pattern");
        let many = Regex::new(r"(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)").expect("test pattern");
        let named = Regex::new(r"(?<base>\\w+) HD").expect("test pattern");
        let any = Regex::new(r"x").expect("test pattern");
        let translate = |replacement: &str| translate_replacement(replacement, &one);
        assert_eq!(translate("plain").unwrap(), "plain");
        assert_eq!(translate("\\1").unwrap(), "${1}");
        assert!(translate("\\2").is_err());
        assert_eq!(
            translate_replacement("\\12", &many).unwrap(),
            "${12}",
            "two digits are one group reference"
        );
        assert!(translate_replacement("\\12", &one).is_err());
        assert_eq!(translate_replacement("\\g<0>", &any).unwrap(), "${0}");
        assert_eq!(translate_replacement("\\g<00>", &any).unwrap(), "${00}");
        assert_eq!(
            translate_replacement("\\g<base>", &named).unwrap(),
            "${base}"
        );
        assert!(translate_replacement("\\g<missing>", &named).is_err());
        assert_eq!(translate("a$b").unwrap(), "a$$b");
        assert_eq!(translate("a\\tb").unwrap(), "a\tb");
        assert_eq!(translate("a\\ab").unwrap(), "a\u{7}b");
        assert_eq!(translate("a\\bb").unwrap(), "a\u{8}b");
        assert_eq!(translate("a\\fb").unwrap(), "a\u{c}b");
        assert_eq!(translate("a\\vb").unwrap(), "a\u{b}b");
        assert_eq!(translate("a\\\\b").unwrap(), "a\\b");
        // A backslash before punctuation is literal in Python — backslash AND character —
        // so the port copies both (doubling a `$` for Rust's expansion).
        assert_eq!(translate("a\\$b").unwrap(), "a\\$$b");
        assert_eq!(translate("a\\.b").unwrap(), "a\\.b");
        assert!(translate("a\\").is_err());
        assert!(translate("\\g1").is_err());
        assert!(translate("\\g").is_err());
        assert!(translate("\\g<base").is_err());
        assert!(translate("\\g<>").is_err());
        // `\\0` is NUL, never group 0; three octal digits win over a group reference.
        assert_eq!(translate("\\0").unwrap(), "\0");
        assert_eq!(translate("\\01").unwrap(), "\u{1}");
        assert_eq!(translate_replacement("\\123", &many).unwrap(), "S");
        assert!(translate_replacement("\\70", &one)
            .unwrap_err()
            .to_string()
            .contains("invalid group reference"));
        assert!(translate_replacement("\\777", &any).is_err());
    }

    #[test]
    fn translated_replacements_apply_like_python() {
        // End to end through `RuleSet`: the translated replacement must produce what
        // `re.sub` produces, not just translate plausibly.
        let rules = RuleSet::parse(r#"[["^(\\w+) HD$", "\\1 5$"]]"#);
        assert!(rules.rejected().count() == 0, "{:?}", rules.report);
        let pairs = vec![(1i64, "ESPN HD")];
        let (cleaned, _) = rules.apply(&pairs);
        assert_eq!(cleaned[&1], "ESPN 5$");

        let rules = RuleSet::parse(r#"[["^(\\w+) HD$", "\\g<1>-\\g<0>"]]"#);
        let (cleaned, _) = rules.apply(&pairs);
        assert_eq!(cleaned[&1], "ESPN-ESPN HD");

        // `\\g<00>` is the whole match and `\\g<01>` is group 1, in both engines.
        let rules = RuleSet::parse(r#"[["^(a)(b)$", "\\g<00>/\\g<01>"]]"#);
        let pairs = vec![(1i64, "ab")];
        let (cleaned, _) = rules.apply(&pairs);
        assert_eq!(cleaned[&1], "ab/a");

        let rules = RuleSet::parse(r#"[["HD", "A\\$B"]]"#);
        let pairs = vec![(1i64, "ESPN HD")];
        let (cleaned, _) = rules.apply(&pairs);
        assert_eq!(cleaned[&1], "ESPN A\\$B");

        // An invalid group reference is refused once at parse, and the name is untouched —
        // the same outcome as the plugin's per-name skip, without an error per stream.
        let rules = RuleSet::parse(r#"[["(a)", "\\2"]]"#);
        assert_eq!(rules.rules.len(), 0);
        assert_eq!(rules.rejected().next().unwrap().status, "bad_escape");
    }
}
