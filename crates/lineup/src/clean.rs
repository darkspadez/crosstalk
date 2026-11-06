//! `clean_*` + `test_name`: Channel Name Cleaner's pipeline on the unified tags.
//!
//! Port of `reference/dispatcharr-channel-name-cleaner/channel_name_cleaner/plugin.py`
//! (NC), rules `plugin.py:148-191`, protect `135-146`, scope `194-231`, collisions
//! `233-268`, preview/apply `326-369`, `test_name` `404-423`.
//!
//! # Rule order (NC's exact order, `plugin.py:148-191`)
//!
//! 1. `unicode`: strip fancy-unicode quality marks (`plugin.py:46 `UNICODE_QUALITY`)
//! 2. `nfkc`: Unicode NFKC normalize
//! 3. `bracket`: strip junk-only bracket groups (`BRACKET_JUNK_INNER`, `:49`)
//! 4. `pipe`: split on `|`, drop empty and quality-only segments, rejoin (`:50-60`)
//! 5. `plus_one`: `Channel 4+1` -> `Channel 4 +1` (`re.sub(r"(?<=[A-Za-z0-9])\+1\b", …)`)
//! 6. `custom`: operator `regex => replacement` lines, in order
//! 7. `casing`: smart title-case
//! 8. `whitespace`: collapse runs and trim
//!
//! Each step fires only when it changes the string (`step()` in `_clean`), and the
//! fired rule names are the CSV's `rules` column. The token lists come from the shared
//! tag vocabulary ([`crosstalk_matching::tags`]), NOT re-declared here:
//! [`tags::is_pipe_junk_segment`](crosstalk_matching::tags::is_pipe_junk_segment) and
//! [`tags::is_bracket_junk_inner`](crosstalk_matching::tags::is_bracket_junk_inner)
//! carry NC's exact sets (quality `FHD\d*|UHD|…|BACKUP` + `\d{3,4}p`, codepoint for
//! codepoint per the vocabulary tests), filtered to `sources` containing `"NC"` so the
//! wider unified table (IC's `Unknown`, LU's bare `1080`) never strips what NC keeps.
//! The unicode marks are the non-ASCII members of `tokens_from(Quality, "NC")` —
//! `plugin.py:46`'s six marks, read from the vocabulary rather than re-typed. The one
//! structural pattern the table cannot enumerate (junk tokens joined by `/,&+` inside
//! one bracket pair) lives in the vocabulary as `is_bracket_junk_inner` (step 14
//! addition), so there is still exactly one definition of "junk".
//!
//! # Protect list (`plugin.py:135-146`)
//!
//! A channel is left untouched when its group is in `protected_groups` (casefolded
//! exact), when [`is_foreign_script`] matches (NC's `FOREIGN_SCRIPT` ranges,
//! `plugin.py:52-55`: Greek, Cyrillic, Hebrew, Arabic, Devanagari, Hiragana/Katakana,
//! CJK, Hangul — codepoint for codepoint), when the name starts with `&`
//! (`protect_leading_symbol`), or when `protect_regex` matches. `protect_regex` and
//! the custom rules compile through
//! [`channel_ops::compile_pattern`](crosstalk_core::channel_ops::compile_pattern),
//! the shared Python-`re` rejection: an invalid pattern is skipped with a warning,
//! exactly like NC's `LOGGER.warning … ignoring invalid custom rule`, except the
//! message names the Rust-`regex` gap (backreferences, lookaround) instead of
//! silently differing.
//!
//! # Scope: the unified scope (`plugin.py:194-231`)
//!
//! NC scoped by `selected_groups` / `ignore_groups` as casefolded EXACT group names —
//! no wildcards. Those two settings now live in the host `[scope]` (plan decision,
//! step 13 review), and clean plans over
//! [`JobContext::base_selection`](crosstalk_core::jobs::JobContext::base_selection):
//! the host `[scope]` (or the workflow step's override, including `scope = "lineup"`),
//! resolved through [`crosstalk_core::scope`] with its `*`/`?` wildcards,
//! case-sensitive names and mutual-exclusion rules. That is the deliberate divergence:
//! `UK | *` now means what the scope says everywhere else in crosstalk, where NC
//! needed the literal group name. `protected_groups` stays NC-exact (casefolded,
//! no wildcards) because it is a protect list, not a scope.
//!
//! # Collisions (`plugin.py:233-268`)
//!
//! Per-channel names go through [`Op::Set`](crosstalk_core::channel_ops::Op::Set)
//! with `PlanOptions.collision = settings.collision_strategy` and
//! `collision_sep = settings.join_separator`, resolved by the ONE shared resolver
//! ([`crosstalk_core::naming::resolve_collisions`], global like NC). NC's `suffix`
//! strategy re-appends the first removed pipe segment (`base + sep + rq[0]`, `:250`);
//! the plan supplies it per channel through
//! [`PlanOptions::collision_removed`](crosstalk_core::channel_ops::PlanOptions::collision_removed)
//! (the pipe step's removed list, uppercased), so the resolver needs no second code
//! path. Two fixes over NC ride along from the shared resolver (step 7 review):
//! slots are counted (NC's set arithmetic freed a name another channel still held)
//! and a `skip` loser's old name stays occupied (cascaded to a fixpoint). Global,
//! not per group (step 7 review: Dispatcharr's outputs list channels flat).
//!
//! # CSV, apply, undo
//!
//! The report carries NC's columns (`id,group,status,old_name,new_name,rules`,
//! `plugin.py:299-306`) plus the shared commented preamble; `rules` is `|`-joined
//! like NC's. An apply plans from live state (never a replayed file) and writes
//! through [`channel_ops::apply`] (bulk edit), recorded as run rows so the host
//! [`undo`](crosstalk_core::undo) reverses it. NC's timestamped backup CSVs and the
//! plugin-dir `backups/` folder are NOT ported: undo is the host's, from the run
//! record (a hand edit since survives undo via the chain rule, and undo-of-undo is a
//! normal run).

use anyhow::{bail, Result};
use crosstalk_core::channel_ops::{self, Field, Op, PlanOptions, Selection, SetValue};
use crosstalk_core::csv_report::{self, Report};
use crosstalk_core::dispatcharr::World;
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use crosstalk_matching::tags;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::OnceLock;

/// The CSV columns: NC's (`plugin.py:299-306`), in NC's order.
pub const COLUMNS: [&str; 6] = ["id", "group", "status", "old_name", "new_name", "rules"];

/// One custom rule: `pattern => replacement`, compiled in file order.
#[derive(Debug, Clone)]
pub struct CustomRule {
    /// The pattern source, for warnings.
    pub source: String,
    pub pattern: regex::Regex,
    pub replacement: String,
}

/// Parse `custom_rules` (one `regex => replacement` per line, `plugin.py:100-109`).
///
/// An invalid pattern is skipped with a warning naming the line — NC's
/// `LOGGER.warning … ignoring invalid custom rule`. The compile goes through the
/// shared [`channel_ops::compile_pattern`], so Python-only syntax is refused with
/// the Rust-`regex` explanation rather than silently matching differently.
pub fn parse_custom_rules(raw: &str) -> Vec<CustomRule> {
    let mut rules = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || !line.contains("=>") {
            continue;
        }
        let (pat, rep) = line.split_once("=>").expect("contains =>");
        let pat = pat.trim();
        let rep = rep.trim();
        let compiled = channel_ops::compile_pattern(pat).and_then(|pattern| {
            let replacement = python_replacement(rep, &pattern)?;
            Ok((pattern, replacement))
        });
        match compiled {
            Ok((pattern, replacement)) => rules.push(CustomRule {
                source: line.to_string(),
                pattern,
                replacement,
            }),
            Err(error) => {
                tracing::warn!("lineup clean: ignoring invalid custom rule {line:?}: {error:#}");
            }
        }
    }
    rules
}

/// Translate a Python `re.sub` replacement (NC's custom rules) into the Rust `regex`
/// replacement that produces the same text.
///
/// Python reads `\1` / `\g<1>` / `\g<name>` as group references, `\\` as a
/// backslash, `\n`-style letters as their control characters, and keeps any other
/// non-letter escape (`\.`) verbatim; `$` is plain text. Rust reads `$1` / `${name}` and
/// nothing else, so a straight copy would turn NC's `\1` into a literal and a `$` in the
/// replacement into a group. What Python refuses (`\q`, a group the pattern lacks, a
/// trailing `\`) is refused here too — NC skipped such a rule on `re.error`, or crashed
/// on an unknown group name. Octal escapes (`\0`, `\123`) are refused as unsupported.
fn python_replacement(rep: &str, pattern: &regex::Regex) -> Result<String> {
    let groups = pattern.captures_len() - 1;
    let names: BTreeSet<&str> = pattern.capture_names().flatten().collect();
    let group = |reference: &str| -> Result<String> {
        match reference.parse::<usize>() {
            Ok(index) if index <= groups => Ok(format!("${{{index}}}")),
            Ok(index) => bail!("the replacement refers to group {index}, the pattern has {groups}"),
            Err(_) if names.contains(reference) => Ok(format!("${{{reference}}}")),
            Err(_) => bail!("the replacement refers to unknown group name {reference:?}"),
        }
    };
    let mut out = String::with_capacity(rep.len());
    let mut chars = rep.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '$' => out.push_str("$$"),
            '\\' => match chars.next() {
                None => bail!("the replacement ends in a lone backslash"),
                Some('\\') => out.push('\\'),
                Some('g') => {
                    if chars.next() != Some('<') {
                        bail!("the replacement has a malformed \\g<...> group reference");
                    }
                    let mut reference = String::new();
                    loop {
                        match chars.next() {
                            Some('>') => break,
                            Some(c) => reference.push(c),
                            None => bail!("the replacement has an unclosed \\g<...>"),
                        }
                    }
                    out.push_str(&group(&reference)?);
                }
                Some(digit @ '1'..='9') => {
                    let mut reference = digit.to_string();
                    if let Some(next) = chars.peek().copied().filter(char::is_ascii_digit) {
                        chars.next();
                        reference.push(next);
                        if chars.peek().is_some_and(char::is_ascii_digit) {
                            bail!("octal escapes in the replacement are not supported");
                        }
                    }
                    out.push_str(&group(&reference)?);
                }
                Some('0') => bail!("octal escapes in the replacement are not supported"),
                Some(letter) if letter.is_ascii_alphabetic() => {
                    let control = match letter {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        'f' => '\u{0c}',
                        'v' => '\u{0b}',
                        'a' => '\u{07}',
                        'b' => '\u{08}',
                        other => bail!("the replacement has a bad escape \\{other}"),
                    };
                    out.push(control);
                }
                Some(other) => {
                    out.push('\\');
                    if other == '$' {
                        out.push_str("$$");
                    } else {
                        out.push(other);
                    }
                }
            },
            other => out.push(other),
        }
    }
    Ok(out)
}

/// The cleaning switches, from [`crate::settings::LineupSettings`].
#[derive(Debug, Clone)]
pub struct CleanRules {
    pub strip_unicode_quality: bool,
    pub unicode_nfkc: bool,
    pub strip_bracketed_junk: bool,
    pub strip_pipe_junk: bool,
    pub normalize_plus_one: bool,
    pub fix_casing: bool,
    pub normalize_whitespace: bool,
    pub join_separator: String,
}

impl CleanRules {
    pub fn from_settings(settings: &crate::settings::LineupSettings) -> Self {
        let mut sep = settings.join_separator.clone();
        if sep.is_empty() {
            // NC's `_rules`: an empty separator reads as a space (`plugin.py:117-119`).
            sep = " ".to_string();
        }
        Self {
            strip_unicode_quality: settings.strip_unicode_quality,
            unicode_nfkc: settings.unicode_nfkc,
            strip_bracketed_junk: settings.strip_bracketed_junk,
            strip_pipe_junk: settings.strip_pipe_junk,
            normalize_plus_one: settings.normalize_plus_one,
            fix_casing: settings.fix_casing,
            normalize_whitespace: settings.normalize_whitespace,
            join_separator: sep,
        }
    }
}

/// The unicode quality marks, from the vocabulary (NC `plugin.py:46`, in vocabulary
/// order — replacement order is irrelevant, the marks are disjoint).
fn unicode_marks() -> &'static [String] {
    static MARKS: OnceLock<Vec<String>> = OnceLock::new();
    MARKS.get_or_init(|| {
        tags::tokens_from(tags::TagKind::Quality, "NC")
            .into_iter()
            .filter(|token| !token.is_ascii())
            .map(str::to_string)
            .collect()
    })
}

/// Strip one bracket pair's junk: `text` with every `(…)` / `[…]` whose inner text is
/// [`tags::is_bracket_junk_inner`] replaced by a space (NC's `_strip_bracketed_junk`,
/// `plugin.py:174-179`).
fn strip_bracketed_junk(text: &str) -> String {
    // NC runs parens first, then square brackets; each pair matches one level only
    // (`[^()]*`, `[^\[\]]*`), so nesting peels inside-out across the two passes.
    let paren = {
        static RE: OnceLock<regex::Regex> = OnceLock::new();
        RE.get_or_init(|| regex::Regex::new(r"\(([^()]*)\)").expect("static regex"))
    };
    let square = {
        static RE: OnceLock<regex::Regex> = OnceLock::new();
        RE.get_or_init(|| regex::Regex::new(r"\[([^\[\]]*)\]").expect("static regex"))
    };
    let after_paren = paren
        .replace_all(text, |caps: &regex::Captures| {
            if tags::is_bracket_junk_inner(&caps[1]) {
                " ".to_string()
            } else {
                caps[0].to_string()
            }
        })
        .to_string();
    square
        .replace_all(&after_paren, |caps: &regex::Captures| {
            if tags::is_bracket_junk_inner(&caps[1]) {
                " ".to_string()
            } else {
                caps[0].to_string()
            }
        })
        .to_string()
}

/// NC's `_smart_title` (`plugin.py:158-166`): per `split(" ")` word, keep words that
/// are already uppercase or carry a digit, else first-upper + rest-lower; empty
/// words are dropped, which collapses runs (the whitespace step would anyway).
fn smart_title(text: &str) -> String {
    let mut out = Vec::new();
    for word in text.split(' ') {
        if word.is_empty() {
            continue;
        }
        // Python `str.isupper()`: at least one cased char, all cased chars uppercase.
        let has_cased = word.chars().any(|c| c.is_alphabetic());
        let all_upper = word
            .chars()
            .filter(|c| c.is_alphabetic())
            .all(|c| c.is_uppercase());
        if (has_cased && all_upper) || word.chars().any(|c| c.is_ascii_digit()) {
            out.push(word.to_string());
        } else {
            let mut chars = word.chars();
            match chars.next() {
                None => {}
                Some(first) => {
                    let mut titled = String::new();
                    titled.extend(first.to_uppercase());
                    titled.push_str(&chars.as_str().to_lowercase());
                    out.push(titled);
                }
            }
        }
    }
    out.join(" ")
}

/// Clean one name through NC's rule order (`plugin.py:148-172`).
///
/// Returns the cleaned name (trimmed), the fired rule tags, and the removed pipe
/// quality segments (uppercased, in order — the `suffix` collision strategy's
/// `rq[0]`). Pure: no I/O, no settings beyond `rules` + `custom`.
pub fn clean_name(
    name: &str,
    rules: &CleanRules,
    custom: &[CustomRule],
) -> (String, BTreeSet<String>, Vec<String>) {
    let mut s = name.to_string();
    let mut fired: BTreeSet<String> = BTreeSet::new();
    let mut removed: Vec<String> = Vec::new();

    if rules.strip_unicode_quality {
        let mut tmp = s.clone();
        for mark in unicode_marks() {
            tmp = tmp.replace(mark.as_str(), " ");
        }
        if tmp != s {
            fired.insert("unicode".to_string());
            s = tmp;
        }
    }
    if rules.unicode_nfkc {
        use unicode_normalization::UnicodeNormalization;
        let folded: String = s.nfkc().collect();
        if folded != s {
            fired.insert("nfkc".to_string());
            s = folded;
        }
    }
    if rules.strip_bracketed_junk {
        let next = strip_bracketed_junk(&s);
        if next != s {
            fired.insert("bracket".to_string());
            s = next;
        }
    }
    if rules.strip_pipe_junk && s.contains('|') {
        let mut kept: Vec<String> = Vec::new();
        for part in s.split('|').map(str::trim) {
            if part.is_empty() {
                continue;
            }
            if tags::is_pipe_junk_segment(part) {
                // NC's `removed.append(part.upper())` (`plugin.py:189`).
                removed.push(part.to_uppercase());
                continue;
            }
            kept.push(part.to_string());
        }
        let joined = kept.join(&rules.join_separator);
        if joined != s {
            fired.insert("pipe".to_string());
            s = joined;
        }
    }
    if rules.normalize_plus_one {
        // NC: `re.sub(r"(?<=[A-Za-z0-9])\+1\b", " +1", s)`. Rust `regex` has no
        // look-behind, so the preceding char is captured and kept — byte-identical
        // output for every input (the char is re-emitted verbatim).
        static RE: OnceLock<regex::Regex> = OnceLock::new();
        let re = RE.get_or_init(|| regex::Regex::new(r"([A-Za-z0-9])\+1\b").expect("static regex"));
        let next = re.replace_all(&s, "${1} +1").to_string();
        if next != s {
            fired.insert("plus_one".to_string());
            s = next;
        }
    }
    for rule in custom {
        // NC wraps each substitution in try/except re.error (`plugin.py:198-201`); a
        // compiled Rust regex cannot fail at match time, so this only records fires.
        let next = rule
            .pattern
            .replace_all(&s, rule.replacement.as_str())
            .to_string();
        if next != s {
            fired.insert("custom".to_string());
            s = next;
        }
    }
    if rules.fix_casing {
        let next = smart_title(&s);
        if next != s {
            fired.insert("casing".to_string());
            s = next;
        }
    }
    if rules.normalize_whitespace {
        static RE: OnceLock<regex::Regex> = OnceLock::new();
        let re = RE.get_or_init(|| regex::Regex::new(r"\s{2,}").expect("static regex"));
        let next = re.replace_all(&s, " ").trim().to_string();
        if next != s {
            fired.insert("whitespace".to_string());
            s = next;
        }
    }
    (s.trim().to_string(), fired, removed)
}

/// Whether a name carries a real non-Latin script (NC's `FOREIGN_SCRIPT`,
/// `plugin.py:52-55`).
///
/// The ranges are NC's, codepoint for codepoint: Greek `Ͱ-Ͽ`, Cyrillic `Ѐ-ӿ`,
/// Hebrew `֐-׿`, Arabic `؀-ۿ`, Devanagari `ऀ-ॿ`, Hiragana/Katakana `぀-ヿ`, CJK
/// `㐀-鿿`, Hangul `가-힯`. Structural ranges, not token lists, so they live here
/// rather than in the vocabulary.
pub fn is_foreign_script(name: &str) -> bool {
    name.chars().any(|c| {
        matches!(c,
            '\u{0370}'..='\u{03FF}'
            | '\u{0400}'..='\u{04FF}'
            | '\u{0590}'..='\u{05FF}'
            | '\u{0600}'..='\u{06FF}'
            | '\u{0900}'..='\u{097F}'
            | '\u{3040}'..='\u{30FF}'
            | '\u{3400}'..='\u{9FFF}'
            | '\u{AC00}'..='\u{D7AF}')
    })
}

/// Why a channel is protected, when it is (NC's `_is_protected`, `plugin.py:135-146`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtectReason {
    Group,
    ForeignScript,
    LeadingSymbol,
    Regex,
}

impl ProtectReason {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Group => "protected group",
            Self::ForeignScript => "non-Latin script",
            Self::LeadingSymbol => "leading &",
            Self::Regex => "protect_regex",
        }
    }
}

/// Split a comma-separated group list into casefolded names (NC's `_csv_set`,
/// `plugin.py:92-95`).
///
/// Rust has no `str.casefold`; `to_lowercase` is the closest (group names are ASCII
/// in practice; the exotic casefold-only mappings like `ß` → `ss` compare by their
/// lowercase form instead — documented, never silently widened to wildcards).
pub fn csv_set(raw: &str) -> BTreeSet<String> {
    raw.split(',')
        .map(|part| part.trim().to_lowercase())
        .filter(|part| !part.is_empty())
        .collect()
}

/// Whether a channel is protected, and why (NC's `_is_protected`).
pub fn is_protected(
    name: &str,
    group: &str,
    settings: &crate::settings::LineupSettings,
    protect_re: Option<&regex::Regex>,
) -> Option<ProtectReason> {
    if csv_set(&settings.protected_groups).contains(&group.trim().to_lowercase()) {
        return Some(ProtectReason::Group);
    }
    if settings.protect_foreign_scripts && is_foreign_script(name) {
        return Some(ProtectReason::ForeignScript);
    }
    if settings.protect_leading_symbol && name.trim_start().starts_with('&') {
        return Some(ProtectReason::LeadingSymbol);
    }
    if let Some(re) = protect_re {
        if re.is_match(name) {
            return Some(ProtectReason::Regex);
        }
    }
    None
}

/// Compile `protect_regex`, warning-and-ignoring an invalid one (NC's
/// `plugin.py:220-224` warning, through the shared pattern compiler).
pub fn compile_protect_regex(raw: &str) -> Option<regex::Regex> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    match channel_ops::compile_pattern(trimmed) {
        Ok(re) => Some(re),
        Err(error) => {
            tracing::warn!("lineup clean: invalid protect_regex ignored: {error:#}");
            None
        }
    }
}

/// Parse `collision_strategy` into the shared strategy (validated at load, so this
/// cannot fail on a validated settings file — it bails naming the setting instead).
pub fn collision_strategy(
    settings: &crate::settings::LineupSettings,
) -> Result<crosstalk_core::naming::CollisionStrategy> {
    match settings.collision_strategy.trim().to_lowercase().as_str() {
        "suffix" => Ok(crosstalk_core::naming::CollisionStrategy::Suffix),
        "numeric" => Ok(crosstalk_core::naming::CollisionStrategy::Numeric),
        "skip" => Ok(crosstalk_core::naming::CollisionStrategy::Skip),
        other => bail!("collision_strategy must be one of suffix, numeric or skip, got {other:?}"),
    }
}

/// One channel's clean outcome, before collision resolution.
#[derive(Debug, Clone)]
pub struct CleanRow {
    pub id: i64,
    pub group: String,
    pub old: String,
    pub new: String,
    pub status: CleanStatus,
    /// Fired rule tags, `|`-joined in the CSV (NC's `rules` column).
    pub rules: BTreeSet<String>,
    /// Removed pipe quality segments, uppercased (the `suffix` strategy's memory).
    pub removed: Vec<String>,
    /// Why a protected channel was left alone.
    pub protect_reason: Option<ProtectReason>,
}

/// The per-row status, in NC's vocabulary (`plugin.py:233-264`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanStatus {
    Protected,
    NoChange,
    Change,
}

impl CleanStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Protected => "protected",
            Self::NoChange => "nochange",
            Self::Change => "change",
        }
    }
}

/// Compute every in-scope channel's clean outcome (NC's `_compute_changes` minus
/// the collision pass, `plugin.py:194-231`).
pub fn compute_rows(
    channels: &[ScopedChannel],
    settings: &crate::settings::LineupSettings,
    rules: &CleanRules,
    custom: &[CustomRule],
    protect_re: Option<&regex::Regex>,
) -> Vec<CleanRow> {
    let mut rows = Vec::with_capacity(channels.len());
    for channel in channels {
        if let Some(reason) = is_protected(&channel.name, &channel.group, settings, protect_re) {
            rows.push(CleanRow {
                id: channel.id,
                group: channel.group.clone(),
                old: channel.name.clone(),
                new: channel.name.clone(),
                status: CleanStatus::Protected,
                rules: BTreeSet::new(),
                removed: Vec::new(),
                protect_reason: Some(reason),
            });
            continue;
        }
        let (new, fired, removed) = clean_name(&channel.name, rules, custom);
        if new.is_empty() || new == channel.name {
            rows.push(CleanRow {
                id: channel.id,
                group: channel.group.clone(),
                old: channel.name.clone(),
                new: channel.name.clone(),
                status: CleanStatus::NoChange,
                rules: fired,
                removed,
                protect_reason: None,
            });
            continue;
        }
        rows.push(CleanRow {
            id: channel.id,
            group: channel.group.clone(),
            old: channel.name.clone(),
            new,
            status: CleanStatus::Change,
            rules: fired,
            removed,
            protect_reason: None,
        });
    }
    rows
}

/// One in-scope channel, as the clean sees it.
#[derive(Debug, Clone)]
pub struct ScopedChannel {
    pub id: i64,
    pub name: String,
    pub group: String,
}

/// One channel's final clean outcome, post-collision: the oracle row.
///
/// `status` is NC's vocabulary (`protected` / `nochange` / `change` / `collision`),
/// `new` the final name (collision-resolved), `rules` the fired rule tags in NC's
/// `|`-joined order, `removed` the uppercased pipe quality the run dropped.
#[derive(Debug, Clone)]
pub struct CleanOutput {
    pub id: i64,
    pub group: String,
    pub old: String,
    pub new: String,
    pub status: String,
    pub rules: Vec<String>,
    pub removed: Vec<String>,
}

/// Plan the cleaning: the change rows as a per-channel `Set Name` through the shared
/// collision resolver (NC's `_resolve_collisions` strategies, global).
///
/// Pure: nothing is written. The same plan `clean::run` applies through
/// [`channel_ops::apply`], and the same one the oracle example dumps.
/// [`CleanRules::from_settings`] supplies the `suffix` separator; `settings` supplies
/// the strategy. An empty change set still plans (NC's preview of a clean world).
pub fn plan_clean(
    world: &World,
    selection: &Selection,
    rows: &[CleanRow],
    settings: &crate::settings::LineupSettings,
) -> Result<channel_ops::Plan> {
    let strategy = collision_strategy(settings)?;
    let sep = CleanRules::from_settings(settings).join_separator;
    let mut values: HashMap<i64, SetValue> = HashMap::new();
    let mut removed: HashMap<i64, Vec<String>> = HashMap::new();
    for row in rows {
        if row.status == CleanStatus::Change {
            values.insert(row.id, SetValue::Name(row.new.clone()));
            if !row.removed.is_empty() {
                removed.insert(row.id, row.removed.clone());
            }
        }
    }
    let ops = vec![Op::Set {
        field: Field::Name,
        values,
    }];
    let options = PlanOptions {
        collision: Some(strategy),
        collision_sep: sep,
        collision_removed: removed,
        ..PlanOptions::default()
    };
    channel_ops::plan_with_options(world, selection, &ops, &options)
}

/// Final per-channel outcomes (NC's row shape) from the cleaning rows plus the plan.
///
/// A plan skip of a change row is NC's `collision` status; the final name is the
/// plan's (the collision strategy may have rewritten it).
pub fn final_outputs(rows: &[CleanRow], plan: &channel_ops::Plan) -> Vec<CleanOutput> {
    let skipped_status: HashMap<i64, &str> = plan
        .skipped
        .iter()
        .map(|skip| {
            let status = if skip.reason.contains("collision") {
                "collision"
            } else {
                "nochange"
            };
            (skip.channel_id, status)
        })
        .collect();
    let final_name: HashMap<i64, &str> = plan
        .changes
        .iter()
        .filter_map(|change| match (&change.field, &change.after) {
            (Field::Name, channel_ops::Value::Name(name)) => {
                Some((change.channel_id, name.as_str()))
            }
            _ => None,
        })
        .collect();
    rows.iter()
        .map(|row| {
            let (status, new) = match row.status {
                CleanStatus::Protected => ("protected", row.old.as_str()),
                CleanStatus::NoChange => ("nochange", row.old.as_str()),
                CleanStatus::Change => (
                    skipped_status.get(&row.id).copied().unwrap_or("change"),
                    final_name.get(&row.id).copied().unwrap_or(row.new.as_str()),
                ),
            };
            CleanOutput {
                id: row.id,
                group: row.group.clone(),
                old: row.old.clone(),
                new: new.to_string(),
                status: status.to_string(),
                rules: row.rules.iter().cloned().collect(),
                removed: row.removed.clone(),
            }
        })
        .collect()
}

/// Run `clean_preview` / `clean_apply`.
///
/// Preview plans and writes the CSV; nothing is written to Dispatcharr. Apply re-plans
/// from live state and writes through [`channel_ops::apply`], recording run rows for
/// the host undo. Both record rows (preview: planned; apply: with outcomes), like the
/// guide cleanup presets.
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    let action_id = if dry_run {
        crate::ACTION_CLEAN_PREVIEW
    } else {
        crate::ACTION_CLEAN_APPLY
    };
    let client = context.dispatcharr()?;
    let world: World = client.load_world().await?;

    let rules = CleanRules::from_settings(settings);
    let custom = parse_custom_rules(&settings.custom_rules);
    let protect_re = compile_protect_regex(&settings.protect_regex);

    // The step's channel universe: the host `[scope]` (or the step override, including
    // `scope = "lineup"`), resolved with wildcards — the unified scope. `base` feeds
    // the plan's selection strings plus an optional id bound; `scoped` is the same
    // universe as loaded channels, which the cleaning computes over.
    let base = context.base_selection(&world)?;
    let scoped = context.scoped_channels(&world)?;
    let channels: Vec<ScopedChannel> = scoped
        .channels
        .iter()
        .map(|channel| ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            group: world.group_name(channel.channel_group_id),
        })
        .collect();
    let rows = compute_rows(&channels, settings, &rules, &custom, protect_re.as_ref());
    let plan = plan_clean(&world, &base, &rows, settings)?;

    let applied = if dry_run {
        None
    } else {
        Some(channel_ops::apply(client, &plan, context, action_id).await)
    };

    // The CSV is NC's columns in NC's order; `rules` is `|`-joined like NC's.
    let outputs = final_outputs(&rows, &plan);
    // Rule counts over the cleaning (NC's "By rule" line counts changes, protected and
    // no-change rows carry no rules by construction — protected rows never cleaned).
    let mut rule_counts: BTreeMap<&str, usize> = BTreeMap::new();
    let mut group_counts: BTreeMap<&str, usize> = BTreeMap::new();
    // Status counts in NC's words for the preamble.
    let mut changed = 0usize;
    let mut collisions = 0usize;
    let mut protected = 0usize;
    let mut nochange = 0usize;
    for output in &outputs {
        match output.status.as_str() {
            "protected" => protected += 1,
            "nochange" => nochange += 1,
            "collision" => collisions += 1,
            _ => {
                changed += 1;
                for rule in &output.rules {
                    *rule_counts.entry(rule.as_str()).or_default() += 1;
                }
                *group_counts.entry(output.group.as_str()).or_default() += 1;
            }
        }
    }

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = if dry_run {
        "clean_preview"
    } else {
        "clean_applied"
    };
    let title = if dry_run {
        "Clean Preview Report"
    } else {
        "Clean Applied Report"
    };
    let report = Report::new(&context.exports_dir(), app, stem, title)
        .channels_processed(plan.selected.len())
        .result("Selection", &scoped.filter_description)
        .result("Changes planned", plan.changes.len())
        .result(
            "Changes written",
            match &applied {
                Some(report) => report.ok.to_string(),
                None => "none, this was a preview".to_string(),
            },
        )
        .result("Skipped", plan.skipped.len() + protected + nochange)
        .write(context.config(), &schema, &values, &COLUMNS, |writer| {
            for output in &outputs {
                writer.write_record([
                    output.id.to_string(),
                    csv_report::safe_cell(&output.group),
                    output.status.clone(),
                    csv_report::safe_cell(&output.old),
                    csv_report::safe_cell(&output.new),
                    output.rules.join("|"),
                ])?;
            }
            Ok(())
        })?;

    context.record_rows(&channel_ops::run_rows(&plan, applied.as_ref()))?;
    // The protected/no-change rows never enter the plan, so they never enter run rows;
    // the CSV is their record (NC's `last_preview.csv` listed every row too).

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let mut summary = if dry_run {
        format!("Clean preview: {changed} change(s), {collisions} collision(s), {protected} protected, {nochange} unchanged")
    } else {
        let (ok, failed) = applied
            .as_ref()
            .map(|report| (report.ok, report.failed))
            .unwrap_or((0, 0));
        format!("Clean apply: renamed {ok} channel(s) ({failed} failed), {collisions} collision(s), {protected} protected, {nochange} unchanged")
    };
    if !rule_counts.is_empty() {
        let by_rule = rule_counts
            .into_iter()
            .map(|(rule, count)| format!("{rule}={count}"))
            .collect::<Vec<_>>()
            .join(", ");
        summary.push_str(&format!(" — by rule: {by_rule}"));
    }
    let mut built = OutcomeBuilder::new(summary)
        .count("channels", plan.selected.len() as i64)
        .count("changed", changed as i64)
        .count("collisions", collisions as i64)
        .count("protected", protected as i64)
        .count("unchanged", nochange as i64)
        .count(
            "written",
            applied.as_ref().map(|report| report.ok as i64).unwrap_or(0),
        )
        .count(
            "failed",
            applied
                .as_ref()
                .map(|report| report.failed as i64)
                .unwrap_or(0),
        )
        .report(Some(report));
    if let Some(note) = channel_ops::cancelled_note(context, &plan, applied.as_ref()) {
        built = built.cancelled().note(note);
    }
    Ok(built.build())
}

/// Run `test_name`: try the rules on the action's input name (NC's `_test_name`,
/// `plugin.py:404-423`), plus the unified engine's identity for that input.
///
/// Needs the input name: the action declares
/// [`ActionSpec::with_input`](crosstalk_core::subapp::ActionSpec::with_input), the CLI
/// passes `--input`, the web renders the field, and the runner carries it in
/// [`JobContext::input`](crosstalk_core::jobs::JobContext::input). No Dispatcharr
/// calls: the cleaning is local, and the identity resolves through the in-process
/// unified engine over the merged data. The input is stored in the run record by the
/// runner, so the run page shows what was tried.
pub fn run_test_name(
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
) -> Result<JobOutcome> {
    let Some(input) = context.input() else {
        bail!(
            "test_name needs an input name: run `crosstalk run lineup test_name --input \
             \"<text>\"`, or type a name in the action's input field on the app page"
        );
    };
    let rules = CleanRules::from_settings(settings);
    let custom = parse_custom_rules(&settings.custom_rules);
    let protect_re = compile_protect_regex(&settings.protect_regex);

    // Protection is checked with no group (NC's `_test_name` passes `""`,
    // `plugin.py:414`): a group protect cannot hit, the rest can.
    if let Some(reason) = is_protected(input, "", settings, protect_re.as_ref()) {
        let summary = format!(
            "PROTECTED — {input:?} would be left unchanged ({})",
            reason.label()
        );
        return Ok(OutcomeBuilder::new(summary)
            .count("protected", 1)
            .count("changed", 0)
            .build());
    }
    let (cleaned, fired, removed) = clean_name(input, &rules, &custom);
    let display = if cleaned.is_empty() { input } else { &cleaned };
    let rules_txt = if fired.is_empty() {
        "none".to_string()
    } else {
        fired.iter().cloned().collect::<Vec<_>>().join(", ")
    };
    let mut lines = vec![
        input.to_string(),
        format!("  ->  {display}"),
        format!("Rules fired: {rules_txt}"),
    ];
    if !removed.is_empty() {
        lines.push(format!("Removed quality: {}", removed.join(", ")));
    }
    // The unified engine's identity for the input: what every app will match it as
    // (canonical name, callsign, zone, country). Resolved with the run's `[matching]`
    // engine config, defaulting the country hint to none (the name's own marker wins).
    let engine =
        crosstalk_matching::engine::UnifiedEngine::new(&context.config().matching.engine_config());
    let resolve = |name: &str| {
        crosstalk_matching::identity::resolve_with(
            &engine,
            name,
            &crosstalk_matching::identity::ResolveContext::default(),
        )
    };
    let describe = |identity: &crosstalk_matching::identity::Identity| {
        format!(
            "canonical {:?}, callsign {}, zone {:?}, timeshift {}, country {}",
            identity.canonical_name,
            identity.callsign.as_deref().unwrap_or("(none)"),
            identity.zone,
            if identity.timeshift { "yes" } else { "no" },
            identity.country.as_deref().unwrap_or("(unknown)"),
        )
    };
    let identity = resolve(input);
    lines.push(format!("Identity: {}", describe(&identity)));
    // The cleaned name is what guide and streams will match after an apply: show it
    // too, and say which identity fields the rename moves (see `identity_drift`).
    if display != input {
        let after = resolve(display);
        lines.push(format!("Identity after clean: {}", describe(&after)));
        let drift = identity_drift(&identity, &after);
        if !drift.is_empty() {
            lines.push(format!(
                "Note: the rename changes the {} every app matches on",
                drift.join(", ")
            ));
        }
    }

    Ok(OutcomeBuilder::new(lines.join("\n"))
        .count("changed", i64::from(display != input))
        .count("protected", 0)
        .build())
}

/// The identity fields a rename moves: `zone`, `timeshift`, `callsign`, `country`,
/// in that order, for each that differs. The family key is left out on purpose: it
/// moves on every real clean (`ESPN | HD |` keys as `espn | |`), which is the point.
///
/// The default rules never move timeshift or callsign (pinned on the corpus by
/// `clean_keeps_identity_on_the_corpus`). Zone and country move only where the
/// engine misread the uncleaned name: pipe residue (`Cinemax East | HD |` reads as
/// the default zone until the `| HD |` goes) and names that are nothing but a
/// country code. The opt-in `fix_casing` moves the key in a way that matters
/// (`AccuWeather` -> `Accuweather` loses the camel-case split, so the family key goes
/// from `accu weather` to `accuweather`), Name Cleaner's documented risk with the
/// setting off by default.
pub fn identity_drift(
    before: &crosstalk_matching::identity::Identity,
    after: &crosstalk_matching::identity::Identity,
) -> Vec<&'static str> {
    let mut drift = Vec::new();
    if before.zone != after.zone {
        drift.push("zone");
    }
    if before.timeshift != after.timeshift {
        drift.push("timeshift");
    }
    if before.callsign != after.callsign {
        drift.push("callsign");
    }
    if before.country != after.country {
        drift.push("country");
    }
    drift
}

/// The base [`Selection`] every clean test builds on: the host scope strings, no filter.
pub fn test_selection(scope: &crosstalk_core::config::ScopeConfig) -> Selection {
    Selection::in_scope(scope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::config::ScopeConfig;

    fn settings() -> crate::settings::LineupSettings {
        crate::settings::LineupSettings::default()
    }

    fn rules() -> CleanRules {
        CleanRules::from_settings(&settings())
    }

    #[test]
    fn rule_order_is_unicode_nfkc_bracket_pipe_plus_one_custom_casing_whitespace() {
        // One name exercising the whole pipeline proves the order, not just each rule:
        // the superscript strips, the bracket strips, the pipe rejoins with ` - `, the
        // custom rule rewrites, the casing titles, the whitespace collapses.
        let mut tuned = settings();
        tuned.join_separator = " - ".to_string();
        tuned.fix_casing = true;
        tuned.custom_rules = "sky => SKY".to_string();
        let rules = CleanRules::from_settings(&tuned);
        let custom = parse_custom_rules(&tuned.custom_rules);
        // ᴴᴰ strips (unicode), [VIP] strips (bracket), "| HD |" drops the segment and
        // rejoins with " - " (pipe), "sky" rewrites (custom), casing titles the rest
        // ("event" -> "Event"; only already-uppercase words like "SKY" are kept).
        let (cleaned, fired, removed) =
            clean_name("sky Sports ᴴᴰ [VIP] | HD | event 03", &rules, &custom);
        assert_eq!(cleaned, "SKY Sports - Event 03");
        assert!(fired.contains("unicode"), "{fired:?}");
        assert!(fired.contains("bracket"), "{fired:?}");
        assert!(fired.contains("pipe"), "{fired:?}");
        assert!(fired.contains("custom"), "{fired:?}");
        assert_eq!(removed, vec!["HD".to_string()]);
    }

    #[test]
    fn each_rule_fires_alone() {
        let base = settings();
        let rules = CleanRules::from_settings(&base);
        let custom = parse_custom_rules("");
        // Unicode marks.
        let (next, fired, _) = clean_name("007 ᴴᴰ", &rules, &custom);
        assert_eq!((next.as_str(), fired.contains("unicode")), ("007", true));
        // NFKC is off by default; on it folds full-width.
        let (next, _, _) = clean_name("ＥＳＰＮ", &rules, &custom);
        assert_eq!(next, "ＥＳＰＮ");
        let mut nfkc = base.clone();
        nfkc.unicode_nfkc = true;
        let (next, fired, _) = clean_name("ＥＳＰＮ", &CleanRules::from_settings(&nfkc), &custom);
        assert_eq!((next.as_str(), fired.contains("nfkc")), ("ESPN", true));
        // Bracket junk strips, prose survives (NC's Moldova case).
        let (next, fired, _) = clean_name("ESPN (1080p)", &rules, &custom);
        assert_eq!((next.as_str(), fired.contains("bracket")), ("ESPN", true));
        let (next, fired, _) = clean_name("CNN (Moldova)", &rules, &custom);
        assert_eq!(next, "CNN (Moldova)");
        assert!(!fired.contains("bracket"), "{fired:?}");
        // Pipe: quality-only segments drop with removed memory; prose joins.
        let (next, fired, removed) = clean_name("Rte One +1 | HD |", &rules, &custom);
        assert_eq!(next, "Rte One +1");
        assert_eq!(removed, vec!["HD".to_string()]);
        assert!(fired.contains("pipe"), "{fired:?}");
        // +1 is off by default.
        let (next, _, _) = clean_name("Channel 4+1", &rules, &custom);
        assert_eq!(next, "Channel 4+1");
        let mut plus = base.clone();
        plus.normalize_plus_one = true;
        let (next, fired, _) =
            clean_name("Channel 4+1", &CleanRules::from_settings(&plus), &custom);
        assert_eq!(
            (next.as_str(), fired.contains("plus_one")),
            ("Channel 4 +1", true)
        );
        // `1+1 International` is a channel name, not a shift (the look-behind needs
        // an alnum before the `+`, and `1+1`'s `+1` is followed by end/`\b`? `1+1`:
        // the `+1` at index 1 IS preceded by `1` — NC would rewrite `1+1` to `1 +1`.
        // That is NC's documented risk (the setting is off by default); pin it.
        let (next, _, _) = clean_name(
            "1+1 International",
            &CleanRules::from_settings(&plus),
            &custom,
        );
        assert_eq!(next, "1 +1 International");
        // Casing is off by default; on it keeps acronyms and digits.
        let (next, _, _) = clean_name("bbc news", &rules, &custom);
        assert_eq!(next, "bbc news");
        let mut casing = base.clone();
        casing.fix_casing = true;
        // Smart title keeps already-uppercase words (`BBC` stays) but cannot know an
        // acronym typed lowercase (`bbc` -> `Bbc`, NC's `_smart_title` verbatim).
        let (next, _, _) = clean_name("BBC news 24", &CleanRules::from_settings(&casing), &custom);
        assert_eq!(next, "BBC News 24");
        let (next, _, _) = clean_name("bbc news 24", &CleanRules::from_settings(&casing), &custom);
        assert_eq!(next, "Bbc News 24");
        // Whitespace collapses and trims.
        let (next, fired, _) = clean_name("  ESPN   HD  ", &rules, &custom);
        assert_eq!(
            (next.as_str(), fired.contains("whitespace")),
            ("ESPN HD", true)
        );
    }

    #[test]
    fn custom_rules_run_in_order_and_invalid_ones_are_skipped() {
        let rules = rules();
        let custom = parse_custom_rules("sky => SKY\n[invalid => x\nrte => RTE");
        assert_eq!(custom.len(), 2, "the invalid line is skipped");
        let (next, fired, _) = clean_name("sky rte", &rules, &custom);
        assert_eq!((next.as_str(), fired.contains("custom")), ("SKY RTE", true));
    }

    #[test]
    fn custom_replacements_read_like_python_re_sub() {
        // NC's replacements are Python `re.sub` templates: `\1` / `\g<name>` are groups
        // and `$` is text. Copied verbatim into Rust, `\1` would be a literal and
        // `$1E` an empty group.
        let rules = rules();
        let custom = parse_custom_rules(
            "^(\\w+) One\\b => \\1 1\nRte => R$1E\n(?P<net>Sky) Sports => \\g<net> \\g<1>",
        );
        assert_eq!(custom.len(), 3);
        let (next, _, _) = clean_name("BBC One HD", &rules, &custom);
        assert_eq!(next, "BBC 1 HD");
        let (next, _, _) = clean_name("Rte Two", &rules, &custom);
        assert_eq!(next, "R$1E Two");
        let (next, _, _) = clean_name("Sky Sports", &rules, &custom);
        assert_eq!(next, "Sky Sky");
        let (next, _, _) = clean_name("Path", &rules, &parse_custom_rules("a => \\.\\\\"));
        assert_eq!(next, "P\\.\\th");
        // What Python refuses is refused (the rule is skipped with a warning).
        for bad in [
            "a => \\q",
            "a => \\1",
            "(a) => \\g<x>",
            "a => b\\",
            "(a) => \\0",
        ] {
            assert!(parse_custom_rules(bad).is_empty(), "{bad:?}");
        }
    }

    #[test]
    fn protections_hit_before_any_rule() {
        let base = settings();
        // Protected groups are casefolded exact, no wildcards.
        assert_eq!(
            is_protected("ESPN | HD |", "todays live events", &base, None),
            Some(ProtectReason::Group)
        );
        assert_eq!(
            is_protected("ESPN | HD |", "Todays Live*", &base, None),
            None
        );
        // Foreign scripts (NC's ranges): Cyrillic is left alone.
        assert_eq!(
            is_protected("РТР-Планета | HD |", "News", &base, None),
            Some(ProtectReason::ForeignScript)
        );
        // Leading `&`.
        assert_eq!(
            is_protected("&TV | HD |", "News", &base, None),
            Some(ProtectReason::LeadingSymbol)
        );
        // Custom protect regex.
        let re = compile_protect_regex("^(PPV|Event)\\b").expect("compiles");
        assert_eq!(
            is_protected("PPV Golf | HD |", "News", &base, Some(&re)),
            Some(ProtectReason::Regex)
        );
        assert_eq!(is_protected("ESPN | HD |", "News", &base, Some(&re)), None);
        // An invalid protect regex warns and ignores (NC's warning).
        assert!(compile_protect_regex("[invalid").is_none());
    }

    #[test]
    fn cleaning_is_idempotent_for_default_rules() {
        // clean(clean(x)) == clean(x): every step is a fixpoint on its own output.
        let rules = rules();
        let custom = parse_custom_rules(&settings().custom_rules);
        let corpus = [
            "ESPN",
            "Sky Atlantic | FHD |",
            "Rte One +1 | HD |",
            "007 ᴴᴰ",
            "ESPN (1080p) [VIP]",
            "CNN (Moldova)",
            "&TV",
            "РТР-Планета",
            "  spaced   out  ",
            "BBC News 24",
            "ITV2 +1 | 4K |",
            "Sky Cinema Disney [FHD] | MULTI |",
        ];
        for name in corpus {
            let (once, _, _) = clean_name(name, &rules, &custom);
            let (twice, fired_twice, _) = clean_name(&once, &rules, &custom);
            assert_eq!(once, twice, "{name:?} is not a fixpoint");
            assert!(fired_twice.is_empty(), "{name:?} refires {fired_twice:?}");
        }
    }

    /// Every merged lineup channel name, deduplicated, in a stable order.
    fn lineup_names() -> Vec<String> {
        let mut names = BTreeSet::new();
        for file in crosstalk_matching::data::merged::merged_lineup_names() {
            let json = crosstalk_matching::data::merged::merged_lineup_json(file).expect("file");
            let value: serde_json::Value = serde_json::from_str(json).expect("lineup json");
            for entries in value["categories"]
                .as_object()
                .expect("categories")
                .values()
            {
                for entry in entries.as_array().into_iter().flatten() {
                    if let Some(name) = entry["name"].as_str() {
                        names.insert(name.to_string());
                    }
                }
            }
        }
        names.into_iter().collect()
    }

    #[test]
    fn clean_keeps_identity_on_the_corpus() {
        // Cross-app invariant: a rename by `clean_apply` must not move what guide and
        // streams match the channel as. Junk the rules strip (pipe quality, junk
        // brackets, unicode marks) is decoration, so a decorated name cleans to a name
        // with the SAME zone, timeshift, callsign and country as its undecorated form
        // — country prefixes (`UK |` -> `UK`), `+1` and regions (`East`, `(West)`)
        // included. Compared against the undecorated form rather than the raw input
        // because the engine misreads pipe residue (`Cinemax East | HD |` keys as
        // `cinemax | |`, default zone): there cleaning moves identity TO the right
        // reading, which is the point (`identity_drift`).
        use crosstalk_matching::identity::{resolve_with, Identity, ResolveContext};
        let engine = crosstalk_matching::engine::UnifiedEngine::new(
            &crosstalk_core::config::MatchingConfig::default().engine_config(),
        );
        let resolve = |name: &str| resolve_with(&engine, name, &ResolveContext::default());
        let fields = |identity: &Identity| {
            (
                identity.zone,
                identity.timeshift,
                identity.callsign.clone(),
                identity.country.clone(),
            )
        };
        let mut plus = settings();
        plus.normalize_plus_one = true;
        let rule_sets = [rules(), CleanRules::from_settings(&plus)];
        let mut checked = 0usize;
        let mut failures = Vec::new();
        // A stride keeps the debug-build run short; every lineup file is still sampled.
        for name in lineup_names().iter().step_by(6) {
            let (bare, _, _) = clean_name(name, &rule_sets[0], &[]);
            if bare != *name {
                // The lineup name itself carries junk: its own row in the sweep below.
                continue;
            }
            for (decorated, plain) in [
                (format!("{name} | HD |"), name.clone()),
                (format!("{name} [FHD]"), name.clone()),
                (format!("{name} (1080p / HEVC)"), name.clone()),
                (format!("{name} ᴴᴰ"), name.clone()),
                (format!("UK | {name} | HD"), format!("UK | {name}")),
                (format!("{name} +1 | FHD |"), format!("{name} +1")),
                (format!("{name} East | HD |"), format!("{name} East")),
                (format!("{name} (West) [HD]"), format!("{name} (West)")),
            ] {
                let want = fields(&resolve(&plain));
                let mut seen: Vec<String> = Vec::new();
                for rules in &rule_sets {
                    let (cleaned, _, _) = clean_name(&decorated, rules, &[]);
                    checked += 1;
                    if seen.contains(&cleaned) {
                        continue;
                    }
                    let have = fields(&resolve(&cleaned));
                    if want != have {
                        failures.push(format!(
                            "{decorated:?} -> {cleaned:?}: {want:?} != {have:?}"
                        ));
                    }
                    seen.push(cleaned);
                }
            }
        }
        assert!(checked > 4_000, "the sweep covers the lineups ({checked})");
        assert!(
            failures.is_empty(),
            "{} of {checked} cleans moved identity:\n{}",
            failures.len(),
            failures
                .iter()
                .take(20)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    #[test]
    fn clean_never_moves_timeshift_callsign_zone_or_country_of_a_clean_name() {
        // The raw-input half of the invariant, on names with no pipe residue: what the
        // rules strip never carries identity. The two documented exceptions are pinned
        // here so they stay deliberate: a name that is nothing but a country code loses
        // its country reading once the junk beside it goes (`USA | HD |` read `USA |`
        // as a country marker; the lineup's `USA` is USA Network), and pipe residue in
        // front (`HD | UK | Sky One`) hid a country the cleaned name shows.
        use crosstalk_matching::identity::{resolve_with, ResolveContext};
        let engine = crosstalk_matching::engine::UnifiedEngine::new(
            &crosstalk_core::config::MatchingConfig::default().engine_config(),
        );
        let resolve = |name: &str| resolve_with(&engine, name, &ResolveContext::default());
        let rules = rules();
        for name in [
            "HBO (East) (HD)",
            "HBO (West) [HD]",
            "HBO 2 East [FHD]",
            "E4 +1 (HD)",
            "ITV1 +1 [1080p]",
            "Fox (P) [HD]",
            "AMC (Pacific) (720p / HEVC)",
            "BBC One (UK) [HD]",
            "[UK] Sky One (HD)",
            "UK: BBC One ᴴᴰ",
            "WABC (HD)",
            "NBC 4 (WNBC) [FHD]",
            "KABC-DT [HD]",
            "Sky Sports Main Event ᵁᴴᴰ",
        ] {
            let (cleaned, fired, _) = clean_name(name, &rules, &[]);
            assert_ne!(cleaned, name, "{name:?} is a real clean");
            let drift = identity_drift(&resolve(name), &resolve(&cleaned));
            assert!(
                drift.is_empty(),
                "{name:?} -> {cleaned:?} ({fired:?}) moved {drift:?}"
            );
        }
        for (name, moved) in [
            ("USA | HD |", "country"),
            ("FR [HD]", "country"),
            ("HD | UK | Sky One", "country"),
            ("Cinemax East | HD |", "zone"),
        ] {
            let (cleaned, _, _) = clean_name(name, &rules, &[]);
            let drift = identity_drift(&resolve(name), &resolve(&cleaned));
            assert_eq!(drift, vec![moved], "{name:?} -> {cleaned:?}");
        }
    }

    #[test]
    fn scope_comes_from_base_selection_not_settings() {
        // NC's `selected_groups` / `ignore_groups` are gone from lineup.toml (moved to
        // host `[scope]`): the clean reads `base_selection`, so a step override
        // (including `scope = "lineup"`) bounds it like every other write.
        let scope = ScopeConfig {
            groups: "News".to_string(),
            ..ScopeConfig::default()
        };
        let selection = test_selection(&scope);
        assert_eq!(selection.groups, "News");
        assert!(selection.channel_ids.is_none());
    }
}

/// Plan-level collision, preview/apply and undo tests: the channel-ops half of clean.
///
/// The pure unit tests above pin NC's rules; these pin the host half — the plan the
/// cleaning becomes, the CSV it writes, the bulk write it issues, and the undo that
/// reverses it. The mock server speaks the five listings `load_world` reads plus the
/// one `edit/bulk` write clean issues.
#[cfg(test)]
mod apply_tests {
    use super::*;
    use crosstalk_core::config::CrosstalkConfig;
    use crosstalk_core::dispatcharr::{Channel, ChannelGroup, DispatcharrClient, World};
    use crosstalk_core::jobs::VecSink;
    use crosstalk_core::store::{RunStatus, Store};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const SPORTS: i64 = 10;
    const NEWS: i64 = 20;

    /// Five channels: two cleaning to one base (the collision pair), one Moldova
    /// no-change, one `&`-protected, one bracket-strip change.
    fn world() -> World {
        let channel = |id: i64, name: &str, group: Option<i64>| Channel {
            id,
            name: name.to_string(),
            channel_number: Some(serde_json::json!(id as f64)),
            channel_group_id: group,
            ..Channel::default()
        };
        World {
            channels: vec![
                channel(1, "Sky Atlantic | FHD |", Some(SPORTS)),
                channel(2, "Sky Atlantic | HD |", Some(NEWS)),
                channel(3, "CNN (Moldova)", Some(NEWS)),
                channel(4, "&TV | HD |", Some(NEWS)),
                channel(5, "ESPN (1080p)", Some(SPORTS)),
            ],
            groups: vec![
                ChannelGroup {
                    id: SPORTS,
                    name: "Sports".to_string(),
                },
                ChannelGroup {
                    id: NEWS,
                    name: "News".to_string(),
                },
            ],
            ..World::default()
        }
    }

    async fn serve(world: &World) -> (MockServer, CrosstalkConfig, tempfile::TempDir) {
        let server = MockServer::start().await;
        let list = |items: serde_json::Value| ResponseTemplate::new(200).set_body_json(items);
        for (route, items) in [
            (
                "/api/channels/channels/",
                serde_json::to_value(&world.channels).expect("channels"),
            ),
            (
                "/api/channels/groups/",
                serde_json::to_value(&world.groups).expect("groups"),
            ),
            (
                "/api/channels/profiles/",
                serde_json::to_value(&world.profiles).expect("profiles"),
            ),
            (
                "/api/epg/sources/",
                serde_json::to_value(&world.sources).expect("sources"),
            ),
            (
                "/api/epg/epgdata/",
                serde_json::to_value(&world.entries).expect("entries"),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(list(items))
                .mount(&server)
                .await;
        }
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"message": "ok", "channels": []})),
            )
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        (server, config, dir)
    }

    fn context_for(
        config: &CrosstalkConfig,
        store: &Store,
        app: &str,
        action: &str,
        dry_run: bool,
    ) -> (JobContext, i64) {
        let run_id = store.start_run(app, action, dry_run).expect("start run");
        let context = JobContext::new(Arc::new(config.clone()), Arc::new(VecSink::new()))
            .with_dispatcharr(DispatcharrClient::new(&config.dispatcharr).expect("client"))
            .for_run(
                app,
                action,
                run_id,
                store.clone(),
                Arc::new(AtomicBool::new(false)),
            );
        (context, run_id)
    }

    /// Plan one world's cleaning with one collision strategy, returning the final name
    /// per changed channel id (or the skip reason when it loses). Uses the same
    /// [`plan_clean`] + [`final_outputs`] the action runs, so these tests pin the real
    /// path rather than a copy of it.
    fn plan_names(world: &World, strategy: &str) -> (HashMap<i64, String>, Vec<String>) {
        plan_names_with(
            world,
            &crate::settings::LineupSettings {
                collision_strategy: strategy.to_string(),
                ..crate::settings::LineupSettings::default()
            },
        )
    }

    fn plan_names_with(
        world: &World,
        tuned: &crate::settings::LineupSettings,
    ) -> (HashMap<i64, String>, Vec<String>) {
        let rules = CleanRules::from_settings(tuned);
        let custom = parse_custom_rules(&tuned.custom_rules);
        let protect_re = compile_protect_regex(&tuned.protect_regex);
        let channels: Vec<ScopedChannel> = world
            .channels
            .iter()
            .map(|channel| ScopedChannel {
                id: channel.id,
                name: channel.name.clone(),
                group: world.group_name(channel.channel_group_id),
            })
            .collect();
        let rows = compute_rows(&channels, tuned, &rules, &custom, protect_re.as_ref());
        let base = Selection::default();
        let plan = plan_clean(world, &base, &rows, tuned).expect("plans");
        let mut names = HashMap::new();
        let mut skips = Vec::new();
        for output in final_outputs(&rows, &plan) {
            match output.status.as_str() {
                "change" => {
                    names.insert(output.id, output.new);
                }
                "collision" => skips.push(format!(
                    "{}: collision, the rename to {:?} is skipped",
                    output.id, output.new
                )),
                _ => {}
            }
        }
        (names, skips)
    }

    #[test]
    fn collisions_suffix_numeric_skip() {
        // `suffix` (NC's default) re-appends each channel's removed segment: with
        // `FHD` vs `HD` removed, both survive as qualified names.
        let (names, skips) = plan_names(&world(), "suffix");
        assert_eq!(names.get(&1).map(String::as_str), Some("Sky Atlantic FHD"));
        assert_eq!(names.get(&2).map(String::as_str), Some("Sky Atlantic HD"));
        assert_eq!(names.get(&5).map(String::as_str), Some("ESPN"));
        assert!(skips.is_empty(), "{skips:?}");
        // `numeric` counts past every taken `base (N)`, in channel-id order.
        let (names, _) = plan_names(&world(), "numeric");
        assert_eq!(names.get(&1).map(String::as_str), Some("Sky Atlantic (2)"));
        assert_eq!(names.get(&2).map(String::as_str), Some("Sky Atlantic (3)"));
        // `skip` leaves both claimants alone and reports the collision.
        let (names, skips) = plan_names(&world(), "skip");
        assert!(!names.contains_key(&1), "{names:?}");
        assert!(!names.contains_key(&2), "{names:?}");
        assert_eq!(skips.len(), 2, "{skips:?}");
        assert!(
            skips.iter().all(|skip| skip.contains("collision")),
            "{skips:?}"
        );
    }

    fn channel(id: i64, name: &str, group: i64) -> Channel {
        Channel {
            id,
            name: name.to_string(),
            channel_number: Some(serde_json::json!(id as f64)),
            channel_group_id: Some(group),
            ..Channel::default()
        }
    }

    #[test]
    fn a_kept_name_stays_taken_when_a_case_twin_is_renamed_away() {
        // Documented divergence from NC (step 7 review fix, kept out of the oracle
        // corpus): NC's `taken = existing_cf - vacated` is set arithmetic, so renaming
        // `espn` away frees `espn` although `ESPN` still holds it, and channel 3 then
        // cleans straight onto `ESPN` — a duplicate. The shared resolver counts
        // slots: 3 is a collision and takes its removed tag.
        let mut world = world();
        world.channels = vec![
            channel(1, "ESPN", SPORTS),
            channel(2, "espn", SPORTS),
            channel(3, "ESPN | HD |", NEWS),
        ];
        let tuned = crate::settings::LineupSettings {
            custom_rules: "^espn$ => ESPN Classic".to_string(),
            ..crate::settings::LineupSettings::default()
        };
        let (names, skips) = plan_names_with(&world, &tuned);
        assert!(!names.contains_key(&1), "{names:?}");
        assert_eq!(names.get(&2).map(String::as_str), Some("ESPN Classic"));
        assert_eq!(names.get(&3).map(String::as_str), Some("ESPN HD"));
        assert!(skips.is_empty(), "{skips:?}");
    }

    #[test]
    fn a_removed_tag_suffix_that_is_taken_falls_back_to_a_number() {
        // A realistic collision with the removed-tag memory: the base AND the
        // base + removed tag are both held by kept channels, so `suffix` falls back to
        // NC's ` (N)` counter (plugin.py:255-259) rather than duplicating either.
        let mut world = world();
        world.channels = vec![
            channel(1, "Sky Atlantic", SPORTS),
            channel(2, "sky atlantic hd", SPORTS),
            channel(3, "Sky Atlantic | HD |", NEWS),
        ];
        let (names, _) = plan_names(&world, "suffix");
        assert_eq!(names.get(&3).map(String::as_str), Some("Sky Atlantic (2)"));
    }

    #[test]
    fn clean_scope_is_the_unified_scope_not_nc_casefold() {
        // Documented divergence from NC (kept out of the oracle corpus, which spells
        // scope groups in exact case): NC's `selected_groups` matched casefolded exact
        // names; the host `[scope]` clean plans over takes `*`/`?` wildcards and
        // compares case-sensitively, like every other crosstalk scope.
        let world = world();
        let ids = |groups: &str| -> Result<Vec<i64>> {
            let scope = crosstalk_core::config::ScopeConfig {
                groups: groups.to_string(),
                ..crosstalk_core::config::ScopeConfig::default()
            };
            Ok(crosstalk_core::scope::scope_channels(&world, &scope)?
                .channels
                .iter()
                .map(|channel| channel.id)
                .collect())
        };
        assert_eq!(
            ids("Sport*").expect("scopes"),
            vec![1, 5],
            "NC would match no group named `Sport*`"
        );
        // NC would casefold `sports` onto `Sports`; the unified scope refuses a group
        // it cannot find rather than cleaning nothing (or everything) silently.
        assert!(ids("sports").is_err());
    }

    #[test]
    fn no_op_dropping_keeps_protected_and_unchanged_out_of_the_plan() {
        let (names, _) = plan_names(&world(), "suffix");
        // Protected (`&TV`) and no-change (`CNN (Moldova)`) never enter the values map.
        assert!(!names.contains_key(&3), "{names:?}");
        assert!(!names.contains_key(&4), "{names:?}");
    }

    #[tokio::test]
    async fn preview_writes_nothing_but_writes_the_nc_csv() {
        let world = world();
        let (server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, _) = context_for(&config, &store, "lineup", "clean_preview", true);
        let outcome = run(
            &app,
            &context,
            &crate::settings::LineupSettings::default(),
            true,
        )
        .await
        .expect("previews");
        assert!(
            outcome.summary.contains("Clean preview"),
            "{}",
            outcome.summary
        );
        // No write: the bulk endpoint never saw a request.
        let writes: Vec<_> = server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| {
                request.method.as_str() == "PATCH"
                    && request.url.path() == "/api/channels/channels/edit/bulk/"
            })
            .collect();
        assert!(writes.is_empty(), "a preview writes nothing");
        // The CSV is NC's columns in NC's order, with the fired rules.
        let path = outcome.report_path.expect("a report");
        let text = std::fs::read_to_string(&path).expect("readable");
        let data: Vec<&str> = text.lines().filter(|line| !line.starts_with('#')).collect();
        assert_eq!(data[0], "id,group,status,old_name,new_name,rules");
        assert_eq!(data.len(), 6, "{data:?}");
        let sky: Vec<&str> = data[1..]
            .iter()
            .find(|line| line.starts_with("1,"))
            .expect("ch 1")
            .split(',')
            .collect();
        assert_eq!(sky[2], "change");
        assert!(sky[5].contains("pipe"), "{sky:?}");
        let moldova: Vec<&str> = data[1..]
            .iter()
            .find(|line| line.starts_with("3,"))
            .expect("ch 3")
            .split(',')
            .collect();
        assert_eq!(moldova[2], "nochange");
        let protected: Vec<&str> = data[1..]
            .iter()
            .find(|line| line.starts_with("4,"))
            .expect("ch 4")
            .split(',')
            .collect();
        assert_eq!(protected[2], "protected");
    }

    #[tokio::test]
    async fn apply_bulks_the_renames_and_records_name_rows() {
        let world = world();
        let (server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, run_id) = context_for(&config, &store, "lineup", "clean_apply", false);
        let outcome = run(
            &app,
            &context,
            &crate::settings::LineupSettings::default(),
            false,
        )
        .await
        .expect("applies");
        assert!(
            outcome.summary.contains("Clean apply"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("written"), Some(&3));
        // One bulk write carrying the three renames.
        let bodies: Vec<serde_json::Value> = server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| {
                request.method.as_str() == "PATCH"
                    && request.url.path() == "/api/channels/channels/edit/bulk/"
            })
            .map(|request| serde_json::from_slice(&request.body).expect("JSON body"))
            .collect();
        assert_eq!(bodies.len(), 1, "{bodies:?}");
        // The run rows are name writes with before/after, which is what host undo reads.
        let rows = store.rows(run_id).expect("rows");
        let names: Vec<_> = rows
            .iter()
            .filter(|row| row.field == "name" && row.status == "ok")
            .collect();
        assert_eq!(names.len(), 3, "{rows:?}");
        for row in &names {
            assert!(
                row.before_value.is_some() && row.after_value.is_some(),
                "{row:?}"
            );
        }
    }

    #[tokio::test]
    async fn undo_round_trips_a_clean_apply_through_host_undo() {
        // Apply, then plan the host undo over the applied world: every row reverses to
        // the name the run found, including the suffix-resolved collision pair.
        let world = world();
        let (_server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, run_id) = context_for(&config, &store, "lineup", "clean_apply", false);
        run(
            &app,
            &context,
            &crate::settings::LineupSettings::default(),
            false,
        )
        .await
        .expect("applies");
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("applied"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let mut applied = world.clone();
        for channel in &mut applied.channels {
            match channel.id {
                1 => channel.name = "Sky Atlantic FHD".to_string(),
                2 => channel.name = "Sky Atlantic HD".to_string(),
                5 => channel.name = "ESPN".to_string(),
                _ => {}
            }
        }
        let registry =
            crosstalk_core::subapp::Registry::new(vec![Arc::new(crate::LineupApp::new())]);
        let plan =
            crosstalk_core::undo::plan_undo(&store, &applied, run_id, &registry).expect("plans");
        assert!(plan.skipped.is_empty(), "{:?}", plan.skipped);
        assert_eq!(plan.changes.len(), 3, "{plan:?}");
        let mut reversed: HashMap<i64, String> = HashMap::new();
        for change in &plan.changes {
            if let crosstalk_core::channel_ops::SetValue::Name(name) = &change.reverse {
                reversed.insert(change.channel_id, name.clone());
            }
        }
        assert_eq!(
            reversed.get(&1).map(String::as_str),
            Some("Sky Atlantic | FHD |")
        );
        assert_eq!(
            reversed.get(&2).map(String::as_str),
            Some("Sky Atlantic | HD |")
        );
        assert_eq!(reversed.get(&5).map(String::as_str), Some("ESPN (1080p)"));
    }

    #[test]
    fn test_name_needs_an_input_and_reports_rules_protection_and_identity() {
        let config = CrosstalkConfig::default();
        // No input: refuse, naming the CLI flag and the web field.
        let context = JobContext::for_tests(config.clone());
        let error = run_test_name(&context, &crate::settings::LineupSettings::default())
            .expect_err("needs an input");
        assert!(format!("{error:#}").contains("--input"), "{error:#}");
        // One name in: the cleaned name, the fired rules and the removed quality.
        let context = JobContext::for_tests(config.clone())
            .with_input(Some("Sky Atlantic | FHD |".to_string()));
        let outcome =
            run_test_name(&context, &crate::settings::LineupSettings::default()).expect("runs");
        assert!(
            outcome.summary.contains("Sky Atlantic"),
            "{}",
            outcome.summary
        );
        assert!(
            outcome.summary.contains("Rules fired: pipe"),
            "{}",
            outcome.summary
        );
        assert!(
            outcome.summary.contains("Removed quality: FHD"),
            "{}",
            outcome.summary
        );
        // The unified engine's identity for the input: what every app will match it as.
        assert!(outcome.summary.contains("Identity:"), "{}", outcome.summary);
        assert!(outcome.summary.contains("canonical"), "{}", outcome.summary);
        assert!(outcome.summary.contains("callsign"), "{}", outcome.summary);
        assert!(outcome.summary.contains("zone"), "{}", outcome.summary);
        assert!(outcome.summary.contains("country"), "{}", outcome.summary);
        // A protected name reports the protection instead of cleaning.
        let context = JobContext::for_tests(config).with_input(Some("&TV | HD |".to_string()));
        let outcome =
            run_test_name(&context, &crate::settings::LineupSettings::default()).expect("runs");
        assert!(
            outcome.summary.starts_with("PROTECTED"),
            "{}",
            outcome.summary
        );
        assert!(outcome.summary.contains("leading &"), "{}", outcome.summary);
    }

    #[test]
    fn test_name_declares_its_input_on_the_action() {
        // The host renders this on the web button and accepts `--input` for it on the CLI.
        let test_name = crate::LineupApp::new()
            .actions()
            .into_iter()
            .find(|action| action.id == crate::ACTION_TEST_NAME)
            .expect("test_name is registered");
        let input = test_name.input.expect("declares an input");
        assert_eq!(input.name, "input");
        assert!(!input.label.is_empty() && !input.help.is_empty());
    }

    #[test]
    fn action_input_round_trips_through_job_context_and_the_run_record() {
        // The runner carries `--input` into the context and stores it on the run row, so
        // the run page shows what name was tried.
        let config = CrosstalkConfig::default();
        let context = JobContext::for_tests(config).with_input(Some("  Sky  ".to_string()));
        assert_eq!(context.input(), Some("Sky"));
        let blank =
            JobContext::for_tests(CrosstalkConfig::default()).with_input(Some("   ".to_string()));
        assert_eq!(blank.input(), None);
        let store = Store::in_memory().expect("store");
        let run_id = store.start_run("lineup", "test_name", true).expect("start");
        store.set_run_input(run_id, "Sky Atlantic").expect("store");
        let record = store.run(run_id).expect("run").expect("present");
        assert_eq!(record.action_input.as_deref(), Some("Sky Atlantic"));
    }
}
