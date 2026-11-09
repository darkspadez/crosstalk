//! The read-only tools: `check_countries`, `scan_placeholders` and `test_rules`.
//!
//! Stream-Mapparr ships three actions that open no provider connection and change nothing, and
//! all three exist because their answer is otherwise only obtainable by hand:
//!
//!   * **`check_countries`** — `check_stream_countries_action` (plugin.py:2608): compares the
//!     country prefix on a stream's GROUP name (which the country restriction trusts) with the
//!     two-letter suffix on its EPG identifier (which whoever built the guide wrote). Two
//!     database columns, no provider connection, and it never filters: a channel carried in one
//!     country and made in another is ordinary.
//!   * **`scan_placeholders`** — `scan_placeholder_names_action` (2703) over `placeholder_scan`
//!     (ported in [`crate::placeholder_scan`]): the numbered stream-name families the
//!     configured placeholder patterns do not cover, each with a regex to paste.
//!   * **`test_rules`** — `test_regex_rules_action` (6575): what `stream_name_regex_rules`
//!     would do to EVERY stream name (unscoped), plus the per-rule validation report. The
//!     flagship case is invisible junk — two identical-looking names — so samples escape
//!     non-printing characters.
//!
//! Each writes one CSV and returns a summary; none writes to Dispatcharr. The plugin wrote a
//! `.txt` readout to its own directory and put the headline in a toast; crosstalk writes the
//! CSV into `[exports]` and the headline into the run summary, which is where every other app's
//! report already goes.

use crate::placeholder_scan::{self, ScanStats, ScanStream};
use crate::selection::{RuleReport, RuleSet, REGEX_NAME_MAX_LEN};
use crate::settings::StreamsSettings;
use anyhow::{bail, Result};
use crosstalk_core::channel_ops::ReportSpec;
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{World, WorldKind, WorldKinds};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{OutcomeBuilder, SubApp};
use regex::Regex;
use std::collections::HashMap;

/// Stream-Mapparr's `REGEX_TEST_SAMPLES`: how many changed names the summary quotes.
pub const REGEX_TEST_SAMPLES: usize = 20;

// `PluginConfig.REGEX_YIELD_EVERY` is a gevent concern and has no port here; the scan's
// containment that DOES matter over REST is the name cap, which is kept.

// =======================================================================================
// check_countries
// =======================================================================================

/// `_GROUP_COUNTRY_RE`: a group label like "UK| NEWS" or "US: SPORT". Either separator,
/// because this provider uses both.
const GROUP_COUNTRY_PATTERN: &str = r"^\s*([A-Za-z]{2})\s*[|:]";

/// `_TVG_SUFFIX_RE`: the two-letter suffix on a guide identifier (`skynews.uk`).
const TVG_SUFFIX_PATTERN: &str = r"\.([A-Za-z]{2})$";

/// `_country_label_disagreement` (2608's classmethod): the stream's two independent country
/// signals, either side `None` when that signal is absent.
///
/// Telling "they disagree" from "only one exists" is the whole point: treating an unknown as a
/// mismatch is how a check starts flagging thousands of ordinary streams.
pub fn country_label_disagreement(
    group_name: &str,
    tvg_id: &str,
) -> (Option<String>, Option<String>) {
    static GROUP: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    static TVG: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let group = GROUP.get_or_init(|| Regex::new(GROUP_COUNTRY_PATTERN).expect("static regex"));
    let tvg = TVG.get_or_init(|| Regex::new(TVG_SUFFIX_PATTERN).expect("static regex"));
    let group_code = group
        .captures(group_name)
        .and_then(|captures| captures.get(1))
        .map(|code| code.as_str().to_uppercase());
    let tvg_code = tvg
        .captures(tvg_id.trim())
        .and_then(|captures| captures.get(1))
        .map(|code| code.as_str().to_uppercase());
    (group_code, tvg_code)
}

/// One stream's verdict, as the CSV row and the counters read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountryVerdict {
    /// Both signals present and equal.
    Agree,
    /// Both present and different. Reported, never acted on.
    Disagree,
    /// The group name carries no country marker.
    NoGroupCountry,
    /// The guide identifier carries no two-letter suffix (a bare number, a longer tail).
    NoEpgCountry,
}

impl CountryVerdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Agree => "agree",
            Self::Disagree => "disagree",
            Self::NoGroupCountry => "no_group_country",
            Self::NoEpgCountry => "no_epg_country",
        }
    }
}

/// What the country check found.
#[derive(Debug, Clone, Default)]
pub struct CountryCheck {
    pub streams: usize,
    /// Streams carrying both signals: the denominator of the agreement rate.
    pub both: usize,
    pub agree: usize,
    pub disagree: usize,
    pub no_group_country: usize,
    pub no_epg_country: usize,
    /// `(name, group, group_code, epg_code)`, sorted for the readout.
    pub rows: Vec<CountryRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CountryRow {
    pub stream_id: i64,
    pub stream_name: String,
    pub group: String,
    pub group_code: Option<String>,
    pub epg_code: Option<String>,
    pub verdict: CountryVerdict,
}

impl CountryCheck {
    pub fn rate(&self) -> f64 {
        if self.both == 0 {
            0.0
        } else {
            100.0 * self.agree as f64 / self.both as f64
        }
    }

    /// The plugin's message, wording included: a rate computed from zero comparisons would
    /// read as a clean bill of health when in fact nothing was measured.
    pub fn message(&self) -> String {
        if self.both == 0 {
            return "No stream carries both a group country and an EPG identifier suffix, so \
                    there was nothing to compare."
                .to_string();
        }
        let rate = self.rate();
        format!(
            "{} of {} streams agree ({rate:.1}%). {} disagree. A disagreement usually means \
             the channel is carried in one country and made in another.",
            self.agree, self.both, self.disagree,
        )
    }
}

/// Compare every stream's two country signals.
pub fn check_countries(world: &World) -> CountryCheck {
    let group_names: HashMap<i64, &str> = world
        .groups
        .iter()
        .map(|group| (group.id, group.name.as_str()))
        .collect();
    let mut check = CountryCheck {
        streams: world.streams.len(),
        ..CountryCheck::default()
    };
    for stream in &world.streams {
        let group = stream
            .channel_group
            .and_then(|id| group_names.get(&id).copied())
            .unwrap_or("");
        let (group_code, epg_code) = country_label_disagreement(group, &stream.tvg_id);
        let verdict = match (&group_code, &epg_code) {
            (Some(left), Some(right)) => {
                check.both += 1;
                if left == right {
                    check.agree += 1;
                    CountryVerdict::Agree
                } else {
                    check.disagree += 1;
                    CountryVerdict::Disagree
                }
            }
            (None, _) => {
                check.no_group_country += 1;
                CountryVerdict::NoGroupCountry
            }
            (_, None) => {
                check.no_epg_country += 1;
                CountryVerdict::NoEpgCountry
            }
        };
        check.rows.push(CountryRow {
            stream_id: stream.id,
            stream_name: stream.name.clone(),
            group: group.to_string(),
            group_code,
            epg_code,
            verdict,
        });
    }
    // The plugin's readout sorts `(name, group, group_code, tvg_code)` tuples; Dispatcharr does
    // not promise a stream order, so sorting makes two runs over one library comparable.
    check.rows.sort_by(|left, right| {
        left.stream_name
            .cmp(&right.stream_name)
            .then_with(|| left.group.cmp(&right.group))
            .then_with(|| left.group_code.cmp(&right.group_code))
            .then_with(|| left.epg_code.cmp(&right.epg_code))
    });
    check
}

/// `check_countries`: the action.
pub async fn run_check_countries(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
) -> Result<crosstalk_core::subapp::JobOutcome> {
    let client = context.dispatcharr()?;
    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.check_countries",
        0,
        0,
        "loading streams",
    ));
    let world = client
        .load_world_with(WorldKinds::only(WorldKind::Streams) | WorldKind::Groups)
        .await?;
    let check = check_countries(&world);

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let spec = ReportSpec::new(
        app,
        &schema,
        &values,
        "check_countries",
        "Stream Country Label Check",
    )
    .note("Streams loaded", world.streams.len().to_string())
    .note("Streams carrying both signals", check.both.to_string())
    .note("They agree", check.agree.to_string())
    .note("They disagree", check.disagree.to_string())
    .note("Only a group country", check.no_group_country.to_string())
    .note("Only a guide suffix", check.no_epg_country.to_string())
    .note("Agreement rate", format!("{:.1}%", check.rate()))
    .note(
        "Note",
        "A disagreement is not automatically a fault: a channel carried in one country and \
         made in another is ordinary. This reports, it never filters.",
    );

    let exports_dir = context.exports_dir();
    // `Report::write` applies `report_title` itself, so the plain title goes in here.
    let report = csv_report::Report::new(&exports_dir, spec.app, spec.stem, spec.title)
        .channels_processed(0)
        .result("Streams loaded", check.streams)
        .result("Streams carrying both signals", check.both)
        .result("They agree", check.agree)
        .result("They disagree", check.disagree)
        .result("Agreement rate", format!("{:.1}%", check.rate()));
    let report = spec
        .notes
        .iter()
        .fold(report, |report, (label, value)| report.result(label, value));
    let path = report.write(
        context.config(),
        spec.schema,
        spec.values,
        &COUNTRY_COLUMNS,
        |writer| {
            for row in &check.rows {
                writer.write_record([
                    row.verdict.as_str().to_string(),
                    csv_report::safe_cell(&row.stream_name),
                    csv_report::safe_cell(&row.group),
                    row.group_code.clone().unwrap_or_default(),
                    row.epg_code.clone().unwrap_or_default(),
                    row.stream_id.to_string(),
                ])?;
            }
            Ok(())
        },
    )?;
    report.prune(context.config().exports.retention_days, &path);

    Ok(OutcomeBuilder::new(check.message())
        .count("streams", check.streams as i64)
        .count("comparable", check.both as i64)
        .count("agree", check.agree as i64)
        .count("disagree", check.disagree as i64)
        .count("no_group_country", check.no_group_country as i64)
        .count("no_epg_country", check.no_epg_country as i64)
        .report(Some(path))
        .build())
}

const COUNTRY_COLUMNS: [&str; 6] = [
    "status",
    "stream_name",
    "group",
    "group_country",
    "epg_country",
    "stream_id",
];

// =======================================================================================
// scan_placeholders
// =======================================================================================

/// What the placeholder scan found.
#[derive(Debug, Clone, Default)]
pub struct PlaceholderScan {
    pub streams: usize,
    /// Pattern entries that compiled (coverage is decided by these alone).
    pub patterns: usize,
    /// Pattern entries the resolver refused (over the cap, too long, uncompilable).
    pub rejected_patterns: usize,
    pub feature_enabled: bool,
    pub families: Vec<placeholder_scan::Family>,
    pub stats: ScanStats,
}

impl PlaceholderScan {
    pub fn uncovered(&self) -> impl Iterator<Item = &placeholder_scan::Family> {
        self.families.iter().filter(|family| family.uncovered > 0)
    }

    /// The plugin's own headline, line for line.
    pub fn headline(&self) -> Vec<String> {
        placeholder_scan::headline(
            &self.families,
            self.streams,
            self.patterns,
            self.feature_enabled,
            &self.stats,
        )
    }
}

/// Scan every stream name for numbered families.
///
/// The patterns are resolved WITHOUT the feature toggle gating them — a pattern list is worth
/// checking for coverage whether or not the feature is switched on — and the readout says
/// plainly when it is off, since a list that is never consulted covers nothing in practice.
pub fn scan_placeholders(world: &World, settings: &StreamsSettings) -> PlaceholderScan {
    let (patterns, report) =
        crate::epg::resolve_placeholder_patterns(&settings.epg_placeholder_name_patterns);
    let streams: Vec<ScanStream<'_>> = world
        .streams
        .iter()
        .map(|stream| ScanStream {
            name: stream.name.as_str(),
            has_epg_id: !stream.tvg_id.trim().is_empty(),
        })
        .collect();
    let (families, stats) =
        placeholder_scan::scan_families(streams, &patterns, Some(REGEX_NAME_MAX_LEN));
    PlaceholderScan {
        streams: world.streams.len(),
        patterns: patterns.len(),
        rejected_patterns: report.iter().filter(|entry| !entry.is_ok()).count(),
        // The plugin's readout uses the FLAG alone here (not `enabled and bool(patterns)`):
        // a list that is never consulted covers nothing in practice, and it says so.
        feature_enabled: settings.epg_placeholder_matching_enabled,
        families,
        stats,
    }
}

const PLACEHOLDER_COLUMNS: [&str; 11] = [
    "template",
    "streams",
    "distinct_numbers",
    "example",
    "carrying_epg_id",
    "covered",
    "uncovered",
    "suggested_pattern",
    "suggested_too_long",
    "caution_mostly_epg",
    "to_description_limit",
];

/// `scan_placeholders`: the action.
pub async fn run_scan_placeholders(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
) -> Result<crosstalk_core::subapp::JobOutcome> {
    let client = context.dispatcharr()?;
    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.scan_placeholders",
        0,
        0,
        "loading streams",
    ));
    let world = client
        .load_world_with(WorldKinds::only(WorldKind::Streams))
        .await?;
    let scan = scan_placeholders(&world, settings);

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let spec = ReportSpec::new(
        app,
        &schema,
        &values,
        "scan_placeholders",
        "Placeholder Name Family Scan",
    )
    .note("Streams scanned", scan.streams.to_string())
    .note("Numbered families found", scan.families.len().to_string())
    .note("Placeholder patterns configured", scan.patterns.to_string())
    .note("Patterns rejected", scan.rejected_patterns.to_string())
    .note(
        "Families no pattern covers",
        scan.uncovered().count().to_string(),
    )
    .note(
        "Name-length cap",
        format!(
            "{REGEX_NAME_MAX_LEN} characters; {} name(s) skipped",
            scan.stats.skipped_long
        ),
    );
    let exports_dir = context.exports_dir();
    // `Report::write` applies `report_title` itself, so the plain title goes in here.
    let report = csv_report::Report::new(&exports_dir, spec.app, spec.stem, spec.title)
        .channels_processed(0)
        .result("Streams scanned", scan.streams)
        .result("Numbered families found", scan.families.len())
        .result("Families no pattern covers", scan.uncovered().count());
    let report = spec
        .notes
        .iter()
        .fold(report, |report, (label, value)| report.result(label, value));
    // The CSV holds every family (covered ones too, with `uncovered = 0`), in the plugin's
    // order: the excluded-here part of its readout is the prose, not the data.
    let path = report.write(
        context.config(),
        spec.schema,
        spec.values,
        &PLACEHOLDER_COLUMNS,
        |writer| {
            for (index, family) in scan.families.iter().enumerate() {
                writer.write_record(placeholder_row(index, family))?;
            }
            Ok(())
        },
    )?;
    report.prune(context.config().exports.retention_days, &path);

    let headline = scan.headline();
    // Newline-joined like the plugin's own message (`_fit_toast` joins its parts with
    // newlines): the harness compares the two while the plugin's fits its toast budget.
    Ok(OutcomeBuilder::new(headline.join("\n"))
        .count("streams", scan.streams as i64)
        .count("families", scan.families.len() as i64)
        .count("uncovered", scan.uncovered().count() as i64)
        .count("patterns", scan.patterns as i64)
        .report(Some(path))
        .build())
}

/// One family's CSV row. The three text cells are provider stream names (or built from
/// them), so they are ASCII-escaped like the readout and formula-defused like every other cell.
fn placeholder_row(index: usize, family: &placeholder_scan::Family) -> [String; 11] {
    [
        csv_report::safe_cell(&placeholder_scan::ascii_safe(&family.template)),
        family.count.to_string(),
        family.distinct_numbers.to_string(),
        csv_report::safe_cell(&placeholder_scan::ascii_safe(&family.example)),
        family.with_epg_id.to_string(),
        family.covered.to_string(),
        family.uncovered.to_string(),
        csv_report::safe_cell(&placeholder_scan::ascii_safe(&family.suggested)),
        family.suggestion_too_long().to_string(),
        family.mostly_carries_epg().to_string(),
        (index < placeholder_scan::DETAIL_LIMIT).to_string(),
    ]
}

// =======================================================================================
// test_rules
// =======================================================================================

/// One changed name, as the report lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedName {
    pub stream_id: i64,
    pub before: String,
    pub after: String,
}

/// What a rule test found.
#[derive(Debug, Clone, Default)]
pub struct RuleTest {
    pub streams: usize,
    pub rules: usize,
    pub report: Vec<RuleReport>,
    /// Every changed name, sorted by `(before, stream_id)`: Dispatcharr promises no stream
    /// order, so the CSV is sorted to make two runs over one library comparable.
    pub changed: Vec<ChangedName>,
    /// The stream ids in the order the world listed them — the order the plugin's
    /// `_get_all_streams` returned them in, which is the order its samples take the
    /// first twenty from. Kept separately because the CSV sorts and the samples must not.
    pub input_order: Vec<i64>,
    pub counters: crate::selection::RegexRun,
}

impl RuleTest {
    pub fn rejected(&self) -> impl Iterator<Item = &RuleReport> {
        self.report.iter().filter(|entry| !entry.is_ok())
    }

    /// `None` when every rule resolved; otherwise the plugin's "no valid rules" list.
    pub fn rejection_lines(&self) -> Vec<String> {
        self.report
            .iter()
            .filter(|entry| !entry.is_ok())
            .map(|entry| {
                let mut line = format!("rule {}: {}", entry.index + 1, entry.status);
                if !entry.detail.is_empty() {
                    line.push_str(&format!(" — {}", entry.detail));
                }
                line
            })
            .collect()
    }

    /// The plugin's samples: `before -> after`, non-printing characters escaped, `after`
    /// truncated to 80 characters the way the plugin truncates both halves.
    ///
    /// The first twenty in the world's listing order, not in the CSV's sorted order: the
    /// plugin slices the stream list as `_get_all_streams` returned it, so the message
    /// matches only when the same twenty are quoted.
    pub fn samples(&self) -> Vec<String> {
        let changed: HashMap<i64, &ChangedName> = self
            .changed
            .iter()
            .map(|name| (name.stream_id, name))
            .collect();
        self.input_order
            .iter()
            .filter_map(|id| changed.get(id))
            .take(REGEX_TEST_SAMPLES)
            .map(|name| {
                let before = escape_invisibles(&truncate(&name.before, 80));
                let after = truncate(&name.after, 80);
                if after.trim().is_empty() {
                    format!("{before} -> (empty — stream becomes unmatchable)")
                } else {
                    format!("{before} -> {}", escape_invisibles(&after))
                }
            })
            .collect()
    }

    /// The plugin's summary, part for part (plugin.py:6575-6660), joined the way the
    /// plugin joins it (`_fit_toast` is `"\\n"`-joined lines, clipped to its budget).
    ///
    /// One deliberate difference: no toast budget here — the run summary carries the whole
    /// message and the CSV the whole readout, so a long sample list is kept rather than
    /// clipped. The harness compares against the plugin's message only while it fits in
    /// the budget unclipped, where the two must be identical.
    pub fn message(&self) -> String {
        let mut parts: Vec<String> = vec![match self.rejected().count() {
            0 => format!("{} rule(s) ok", self.rules),
            rejected => format!("{} rule(s) ok, {rejected} rejected", self.rules),
        }];
        parts.extend(self.rejection_lines());
        parts.push(format!(
            "{} of {} stream names would change (tested against ALL streams — ignores \
             M3U/group scoping).",
            comma(self.changed.len()),
            comma(self.streams)
        ));
        if self.counters.emptied > 0 {
            parts.push(format!(
                "⚠ {} name(s) become empty (unmatchable).",
                self.counters.emptied
            ));
        }
        if self.counters.skipped_long > 0
            || self.counters.growth_reverted > 0
            || self.counters.budget_tripped
        {
            parts.push(format!(
                "⚠ containment: {} skipped (too long), {} growth-capped{}",
                self.counters.skipped_long,
                self.counters.growth_reverted,
                if self.counters.budget_tripped {
                    ", time budget exceeded"
                } else {
                    ""
                }
            ));
        }
        parts.push(
            "Note: normalization and ignore tags apply AFTER these rules — remaining \
             quality tags etc. are cleaned downstream."
                .to_string(),
        );
        let samples = self.samples();
        if !samples.is_empty() {
            parts.push("Samples (non-printing chars escaped):".to_string());
            parts.extend(samples.iter().cloned());
        }
        parts.join("\n")
    }
}

/// The plugin's `f"{n:,}"`: thousands separators on the change counts.
///
/// Only the "X of Y stream names" line carries them — the rule counts, the emptied count
/// and the containment counts are plain, exactly as in the plugin.
fn comma(count: usize) -> String {
    let digits: Vec<char> = count.to_string().chars().rev().collect();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.iter().enumerate() {
        if index > 0 && index % 3 == 0 {
            out.push(',');
        }
        out.push(*digit);
    }
    out.chars().rev().collect()
}

fn truncate(text: &str, limit: usize) -> String {
    text.chars().take(limit).collect()
}

/// `_escape_invisibles`: render non-printing characters visibly for the samples.
///
/// The flagship case for this action is invisible junk, so two identical-looking names must not
/// print identically. The plugin escapes every character whose Unicode general category starts
/// with `C` (control, format, surrogate, private-use and unassigned — `unicodedata.category`
/// — so the zero-width family and every other `Cf` is covered, not just the ones a list
/// remembered) plus the Box Drawing / Block Elements ranges (`U+2500-U+259F`, which are
/// symbols, not `C*`, and are escaped anyway). A `char` can never be a surrogate, but the
/// category test covers it for free, exactly as the plugin's does.
pub fn escape_invisibles(text: &str) -> String {
    use unicode_general_category::{get_general_category, GeneralCategory as G};
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        let code = ch as u32;
        let invisible = matches!(
            get_general_category(ch),
            G::Control | G::Format | G::Surrogate | G::PrivateUse | G::Unassigned
        ) || (0x2500..=0x259F).contains(&code);
        if invisible {
            if code <= 0xFFFF {
                out.push_str(&format!("\\u{code:04x}"));
            } else {
                out.push_str(&format!("\\U{code:08x}"));
            }
        } else {
            out.push(ch);
        }
    }
    out
}

/// Test the configured rules against EVERY stream name.
///
/// Unscoped on purpose, as the plugin's action is: the question is what the rules do to names,
/// not what a scoped match would select.
pub fn test_rules(world: &World, settings: &StreamsSettings) -> RuleTest {
    let rules = RuleSet::parse(&settings.stream_name_regex_rules);
    let pairs: Vec<(i64, &str)> = world
        .streams
        .iter()
        .map(|stream| (stream.id, stream.name.as_str()))
        .collect();
    let (cleaned, counters) = rules.apply(&pairs);
    let names: HashMap<i64, &str> = pairs.into_iter().collect();
    let mut changed: Vec<ChangedName> = cleaned
        .into_iter()
        .filter_map(|(id, after)| {
            let before = names.get(&id).copied().unwrap_or_default();
            (after != before).then(|| ChangedName {
                stream_id: id,
                before: before.to_string(),
                after,
            })
        })
        .collect();
    let input_order: Vec<i64> = world.streams.iter().map(|stream| stream.id).collect();
    // Dispatcharr's stream order is not promised; sort so two runs are comparable. The
    // samples deliberately do NOT use this order (see `RuleTest::samples`).
    changed.sort_by(|left, right| {
        left.before
            .cmp(&right.before)
            .then_with(|| left.stream_id.cmp(&right.stream_id))
    });
    RuleTest {
        streams: world.streams.len(),
        rules: rules.rules.len(),
        report: rules.report,
        changed,
        input_order,
        counters,
    }
}

const RULE_COLUMNS: [&str; 7] = [
    "kind",
    "rule_index",
    "rule_status",
    "rule_detail",
    "pattern",
    "stream_name",
    "match_name",
];

/// `test_rules`: the action.
pub async fn run_test_rules(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
) -> Result<crosstalk_core::subapp::JobOutcome> {
    let rules = RuleSet::parse(&settings.stream_name_regex_rules);
    // The plugin's own errors, before touching Dispatcharr: a run with nothing to test against
    // is a configuration question, not an empty result.
    if rules.rules.is_empty() {
        if rules.report.is_empty() {
            bail!("No rules configured — set Stream Name Regex Rules first.");
        }
        let mut lines = vec!["No valid rules.".to_string()];
        lines.extend(
            rules
                .report
                .iter()
                .map(|entry| {
                    let mut line = format!("rule {}: {}", entry.index + 1, entry.status);
                    if !entry.detail.is_empty() {
                        line.push_str(&format!(" — {}", entry.detail));
                    }
                    line
                })
                .collect::<Vec<_>>(),
        );
        bail!("{}", lines.join("; "));
    }

    let client = context.dispatcharr()?;
    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.test_rules",
        0,
        0,
        "loading streams",
    ));
    let world = client
        .load_world_with(WorldKinds::only(WorldKind::Streams))
        .await?;
    if world.streams.is_empty() {
        bail!("No streams loaded from Dispatcharr — nothing to test against.");
    }
    let test = test_rules(&world, settings);
    let message = test.message();

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let spec = ReportSpec::new(app, &schema, &values, "test_rules", "Regex Rule Test")
        .note("Rules ok", test.rules.to_string())
        .note("Rules rejected", test.rejected().count().to_string())
        .note("Names that would change", test.changed.len().to_string())
        .note(
            "Names emptied (unmatchable)",
            test.counters.emptied.to_string(),
        )
        .note(
            "Names skipped (too long)",
            test.counters.skipped_long.to_string(),
        )
        .note(
            "Names growth-capped",
            test.counters.growth_reverted.to_string(),
        )
        .note(
            "Note",
            "Tested against ALL streams: this ignores M3U and stream-group scoping, and \
             normalization and ignore tags apply after these rules.",
        );
    let exports_dir = context.exports_dir();
    // `Report::write` applies `report_title` itself, so the plain title goes in here.
    let report = csv_report::Report::new(&exports_dir, spec.app, spec.stem, spec.title)
        .channels_processed(0)
        .result("Rules ok", test.rules)
        .result("Rules rejected", test.rejected().count())
        .result("Names that would change", test.changed.len());
    let report = spec
        .notes
        .iter()
        .fold(report, |report, (label, value)| report.result(label, value));
    let path = report.write(
        context.config(),
        spec.schema,
        spec.values,
        &RULE_COLUMNS,
        |writer| {
            for entry in &test.report {
                writer.write_record([
                    "rule".to_string(),
                    (entry.index + 1).to_string(),
                    entry.status.to_string(),
                    csv_report::safe_cell(&entry.detail),
                    csv_report::safe_cell(&entry.pattern),
                    String::new(),
                    String::new(),
                ])?;
            }
            for name in &test.changed {
                writer.write_record([
                    "stream".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    csv_report::safe_cell(&name.before),
                    csv_report::safe_cell(&name.after),
                ])?;
            }
            Ok(())
        },
    )?;
    report.prune(context.config().exports.retention_days, &path);

    Ok(OutcomeBuilder::new(message)
        .count("rules", test.rules as i64)
        .count("rules_rejected", test.rejected().count() as i64)
        .count("streams", test.streams as i64)
        .count("changed", test.changed.len() as i64)
        .count("emptied", test.counters.emptied as i64)
        .count("skipped_long", test.counters.skipped_long as i64)
        .count("growth_reverted", test.counters.growth_reverted as i64)
        .report(Some(path))
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::dispatcharr::{ChannelGroup, Stream, World};

    fn world() -> World {
        World {
            groups: vec![
                ChannelGroup {
                    id: 100,
                    name: "UK| NEWS".to_string(),
                },
                ChannelGroup {
                    id: 101,
                    name: "NEWS".to_string(),
                },
            ],
            streams: vec![
                Stream {
                    id: 1,
                    name: "Sky News".to_string(),
                    channel_group: Some(100),
                    tvg_id: "skynews.uk".to_string(),
                    ..Stream::default()
                },
                Stream {
                    id: 2,
                    name: "Al Jazeera".to_string(),
                    channel_group: Some(100),
                    tvg_id: "aljazeera.qa".to_string(),
                    ..Stream::default()
                },
                Stream {
                    id: 3,
                    name: "Unlabelled".to_string(),
                    channel_group: Some(101),
                    tvg_id: "skynews.uk".to_string(),
                    ..Stream::default()
                },
                Stream {
                    id: 4,
                    name: "Bare number".to_string(),
                    channel_group: Some(100),
                    tvg_id: "12345".to_string(),
                    ..Stream::default()
                },
            ],
            ..World::default()
        }
    }

    #[test]
    fn both_country_signals_are_read_and_absent_is_not_a_disagreement() {
        assert_eq!(
            country_label_disagreement("UK| NEWS", "skynews.uk"),
            (Some("UK".to_string()), Some("UK".to_string()))
        );
        // Both separators, and case is normalised on both sides.
        assert_eq!(
            country_label_disagreement("us: SPORT", "ESPN.US").0,
            Some("US".to_string())
        );
        assert_eq!(country_label_disagreement("NEWS", "skynews.uk").0, None);
        assert_eq!(country_label_disagreement("UK| NEWS", "").1, None);
        // A bare number is not a country, and exactly two letters are anchored to the end.
        assert_eq!(country_label_disagreement("UK| NEWS", "12345").1, None);
        assert_eq!(
            country_label_disagreement("UK| NEWS", "channel.info").1,
            None
        );
        // A multi-part domain uses the final label.
        assert_eq!(
            country_label_disagreement("UK| NEWS", "bbc.co.uk").1,
            Some("UK".to_string())
        );
    }

    #[test]
    fn the_countries_check_counts_agreements_and_disagreements() {
        let check = check_countries(&world());
        assert_eq!(check.streams, 4);
        assert_eq!(check.both, 2);
        assert_eq!(check.agree, 1);
        assert_eq!(check.disagree, 1);
        assert_eq!(check.no_group_country, 1);
        assert_eq!(check.no_epg_country, 1);
        // The disagreement is the Al Jazeera row: carried in the UK group, made in QA.
        let row = check
            .rows
            .iter()
            .find(|row| row.stream_name == "Al Jazeera")
            .expect("row");
        assert_eq!(row.verdict, CountryVerdict::Disagree);
        assert_eq!(row.group_code.as_deref(), Some("UK"));
        assert_eq!(row.epg_code.as_deref(), Some("QA"));
        assert!(check.message().contains("1 of 2 streams agree"));
        assert!(check.message().contains("1 disagree"));
    }

    #[test]
    fn nothing_comparable_says_so_rather_than_claiming_success() {
        let check = check_countries(&World::default());
        assert_eq!(check.both, 0);
        assert!(check.message().contains("nothing to compare"));
        assert!(!check.message().contains("100"));
        assert_eq!(check.rate(), 0.0);
    }

    #[test]
    fn the_scan_reports_families_with_a_pasteable_pattern() {
        let world = World {
            streams: (1..=4)
                .map(|number| Stream {
                    id: number,
                    name: format!("PPV EVENT 0{number}"),
                    ..Stream::default()
                })
                .collect(),
            ..World::default()
        };
        let settings = StreamsSettings {
            epg_placeholder_name_patterns: "^MAX \\\\d+$".to_string(),
            ..StreamsSettings::default()
        };
        let scan = scan_placeholders(&world, &settings);
        assert_eq!(scan.streams, 4);
        assert_eq!(scan.patterns, 1);
        // The feature is off by default, so the headline says the list is never consulted.
        assert!(!scan.feature_enabled);
        assert_eq!(scan.families.len(), 1);
        assert_eq!(scan.families[0].suggested, r"^PPV EVENT \d+$");
        let headline = scan.headline().join(" ");
        assert!(headline.contains("1 numbered family"), "{headline}");
        assert!(headline.contains("currently off"), "{headline}");
    }

    #[test]
    fn the_rule_test_reports_changes_and_rejections() {
        let world = World {
            streams: vec![
                Stream {
                    id: 1,
                    name: "ESPN ▎ HD".to_string(),
                    ..Stream::default()
                },
                Stream {
                    id: 2,
                    name: "CNN".to_string(),
                    ..Stream::default()
                },
            ],
            ..World::default()
        };
        let settings = StreamsSettings {
            stream_name_regex_rules: r#"[["\\s*▎\\s*", " "], ["(unclosed", "x"]]"#.to_string(),
            ..StreamsSettings::default()
        };
        let test = test_rules(&world, &settings);
        assert_eq!(test.rules, 1);
        assert_eq!(test.rejected().count(), 1);
        assert_eq!(test.changed.len(), 1);
        assert_eq!(test.changed[0].before, "ESPN ▎ HD");
        assert_eq!(test.changed[0].after, "ESPN HD");
        let samples = test.samples();
        assert_eq!(samples.len(), 1);
        assert!(samples[0].contains("ESPN HD"), "{samples:?}");
        assert!(test.rejection_lines()[0].contains("compile_error"));
    }

    #[test]
    fn invisible_characters_are_escaped_for_the_samples() {
        // The flagship case: names that look identical but are not.
        assert_eq!(escape_invisibles("ESPN\u{200b}HD"), "ESPN\\u200bHD");
        assert_eq!(escape_invisibles("A\u{7}B"), "A\\u0007B");
        assert_eq!(escape_invisibles("▎"), "\\u258e");
        assert_eq!(escape_invisibles("plain"), "plain");
        // Every `C*` general category, not just the remembered codepoints: the plugin
        // escapes on `unicodedata.category(ch)[0] == "C"`, so a Syriac abbreviation
        // mark (Cf) escapes exactly like a zero-width space — while a Mongolian
        // variation selector (Mn: `unicodedata.category('\u180b') == 'Mn'`) does not,
        // because the plugin's test is the category's first letter, not a codepoint list.
        assert_eq!(escape_invisibles("ESPN\u{70f}HD"), "ESPN\\u070fHD");
        assert_eq!(escape_invisibles("X\u{180b}Y"), "X\u{180b}Y");
        // An unassigned codepoint is `Cn`, and so escapes too.
        assert_eq!(escape_invisibles("X\u{2065}Y"), "X\\u2065Y");
    }

    #[test]
    fn thousands_separators_land_only_on_the_change_counts() {
        assert_eq!(comma(0), "0");
        assert_eq!(comma(999), "999");
        assert_eq!(comma(1_000), "1,000");
        assert_eq!(comma(1_234_567), "1,234,567");
    }

    #[test]
    fn the_rule_summary_counts_carry_separators_like_the_plugin() {
        // 1,500 streams so the `f"{n:,}"` branch is exercised, not just asserted on.
        let world = World {
            streams: (0..1_500)
                .map(|index| Stream {
                    id: index,
                    name: format!("ESPN {index}"),
                    ..Stream::default()
                })
                .collect(),
            ..World::default()
        };
        let settings = StreamsSettings {
            stream_name_regex_rules: r#"[["^ESPN ", "ESPN-"]]"#.to_string(),
            ..StreamsSettings::default()
        };
        let test = test_rules(&world, &settings);
        assert_eq!(test.changed.len(), 1_500);
        let message = test.message();
        assert!(
            message.contains("1,500 of 1,500 stream names would change"),
            "{message}"
        );
        // ... while the rule count itself stays plain: the plugin formats only that line.
        assert!(message.starts_with("1 rule(s) ok\n"), "{message}");
    }

    use crate::test_support::ServerFixture;

    fn tool_world() -> World {
        World {
            groups: vec![
                ChannelGroup {
                    id: 100,
                    name: "UK| NEWS".to_string(),
                },
                ChannelGroup {
                    id: 101,
                    name: "US: SPORT".to_string(),
                },
            ],
            streams: vec![
                Stream {
                    id: 1,
                    name: "Sky News".to_string(),
                    m3u_account: Some(1),
                    channel_group: Some(100),
                    tvg_id: "skynews.uk".to_string(),
                    ..Stream::default()
                },
                Stream {
                    id: 2,
                    name: "Al Jazeera".to_string(),
                    m3u_account: Some(1),
                    channel_group: Some(101),
                    tvg_id: "aljazeera.qa".to_string(),
                    ..Stream::default()
                },
                Stream {
                    id: 3,
                    name: "PPV EVENT 04".to_string(),
                    m3u_account: Some(1),
                    channel_group: Some(101),
                    tvg_id: "ppv04.us".to_string(),
                    ..Stream::default()
                },
                Stream {
                    id: 4,
                    name: "PPV EVENT 05".to_string(),
                    m3u_account: Some(1),
                    channel_group: Some(101),
                    tvg_id: "ppv05.us".to_string(),
                    ..Stream::default()
                },
                Stream {
                    id: 5,
                    name: "PPV EVENT 06".to_string(),
                    m3u_account: Some(1),
                    channel_group: Some(101),
                    tvg_id: "ppv06.us".to_string(),
                    ..Stream::default()
                },
            ],
            ..World::default()
        }
    }

    #[tokio::test]
    async fn the_country_check_writes_a_csv_and_changes_nothing() {
        let world = tool_world();
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run("check_countries", &store, true);
        let outcome = run_check_countries(
            &crate::StreamsApp::new(),
            &context,
            &StreamsSettings::default(),
        )
        .await
        .expect("run");
        assert!(
            outcome.summary.contains("4 of 5 streams agree"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("disagree"), Some(&1));
        let path = outcome.report_path.expect("report");
        let report = std::fs::read_to_string(&path).expect("read");
        // The title is built once: the app's `report_title` is applied by the writer, not by
        // the caller as well.
        assert_eq!(
            report.lines().next().unwrap_or_default(),
            crate::StreamsApp::new().report_title("Stream Country Label Check")
        );
        assert!(report.contains("Al Jazeera"), "{report}");
        assert!(report.contains("not automatically a fault"), "{report}");
        // Read-only: nothing was written to Dispatcharr.
        assert!(fixture
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await
            .is_empty());
    }

    #[test]
    fn a_placeholder_row_defuses_formulas_in_its_text_cells() {
        let family = placeholder_scan::Family {
            template: "=HYPERLINK(\"http://x\") #".to_string(),
            count: 2,
            distinct_numbers: 2,
            example: "+cmd|' /C calc'!A0 1".to_string(),
            with_epg_id: 0,
            covered: 0,
            uncovered: 2,
            suggested: "@SUM(1) \\d+ é".to_string(),
        };
        let row = placeholder_row(0, &family);
        assert!(row[0].starts_with("'="), "{}", row[0]);
        assert!(row[3].starts_with("'+"), "{}", row[3]);
        assert!(row[7].starts_with("'@"), "{}", row[7]);
        assert!(row[7].ends_with("\\u00e9"), "ASCII-escaped too: {}", row[7]);
        // A pattern that does not look like a formula is pasteable as written.
        let plain = placeholder_scan::Family {
            template: "PPV EVENT #".to_string(),
            example: "PPV EVENT 1".to_string(),
            suggested: "^PPV EVENT \\d+$".to_string(),
            ..family
        };
        let row = placeholder_row(0, &plain);
        assert_eq!(row[0], "PPV EVENT #");
        assert_eq!(row[7], "^PPV EVENT \\d+$");
    }

    #[tokio::test]
    async fn the_placeholder_scan_writes_every_family_with_its_suggestion() {
        let world = tool_world();
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run("scan_placeholders", &store, true);
        // A pattern list that does not cover the PPV family, so the suggestion is the point.
        let settings = StreamsSettings {
            epg_placeholder_matching_enabled: true,
            epg_placeholder_name_patterns: "^MAX \\d+$".to_string(),
            ..StreamsSettings::default()
        };
        let outcome = run_scan_placeholders(&crate::StreamsApp::new(), &context, &settings)
            .await
            .expect("run");
        assert_eq!(outcome.counts.get("families"), Some(&1));
        assert_eq!(outcome.counts.get("uncovered"), Some(&1));
        assert!(
            outcome
                .summary
                .contains("Pattern to paste: ^PPV EVENT \\d+$"),
            "{}",
            outcome.summary
        );
        let report = std::fs::read_to_string(outcome.report_path.expect("report")).expect("read");
        assert!(report.contains("PPV EVENT #"), "{report}");
        assert!(report.contains(r"^PPV EVENT \d+$"), "{report}");
        // Read-only: nothing was written to Dispatcharr.
        assert!(fixture
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await
            .is_empty());
        assert!(fixture
            .bodies("PUT", "/api/channels/channels/edit/bulk/")
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn the_rule_test_writes_the_rule_report_and_the_changed_names() {
        let world = tool_world();
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run("test_rules", &store, true);
        let settings = StreamsSettings {
            stream_name_regex_rules: r#"[["^PPV EVENT (\\d+)$", "PPV \\1"], ["(unclosed", "x"]]"#
                .to_string(),
            ..StreamsSettings::default()
        };
        let outcome = run_test_rules(&crate::StreamsApp::new(), &context, &settings)
            .await
            .expect("run");
        assert_eq!(outcome.counts.get("rules"), Some(&1));
        assert_eq!(outcome.counts.get("rules_rejected"), Some(&1));
        assert_eq!(outcome.counts.get("changed"), Some(&3));
        assert!(
            outcome.summary.contains("1 rule(s) ok, 1 rejected"),
            "{}",
            outcome.summary
        );
        let report = std::fs::read_to_string(outcome.report_path.expect("report")).expect("read");
        assert!(report.contains("compile_error"), "{report}");
        assert!(report.contains("PPV 04"), "{report}");
        // Read-only: nothing was written to Dispatcharr.
        assert!(fixture
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn the_rule_test_refuses_before_reading_dispatcharr_when_nothing_is_usable() {
        let fixture = crate::test_support::Fixture::with_credentials();
        let context = fixture.context();
        let settings = StreamsSettings {
            stream_name_regex_rules: r#"[["(unclosed", "x"]]"#.to_string(),
            ..StreamsSettings::default()
        };
        let error = run_test_rules(&crate::StreamsApp::new(), &context, &settings)
            .await
            .expect_err("no valid rules");
        assert!(format!("{error}").contains("No valid rules"), "{error}");

        let error = run_test_rules(
            &crate::StreamsApp::new(),
            &context,
            &StreamsSettings::default(),
        )
        .await
        .expect_err("no rules at all");
        assert!(
            format!("{error}").contains("No rules configured"),
            "{error}"
        );
    }

    #[test]
    fn an_empty_name_change_is_reported_as_unmatchable() {
        let world = World {
            streams: vec![Stream {
                id: 1,
                name: "UK BBC 1".to_string(),
                ..Stream::default()
            }],
            ..World::default()
        };
        let settings = StreamsSettings {
            stream_name_regex_rules: r#"[["^UK.*", ""]]"#.to_string(),
            ..StreamsSettings::default()
        };
        let test = test_rules(&world, &settings);
        assert_eq!(test.counters.emptied, 1);
        assert!(test.samples()[0].contains("(empty"), "{:?}", test.samples());
    }
}
