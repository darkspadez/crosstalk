//! EPG-aware placeholder matching: matching a generic provider slot by what is airing on it.
//!
//! Port of Stream-Mapparr's optional EPG path (`epg_placeholder_matching_enabled`, off by
//! default). Some providers name PPV and event slots generically — `PPV EVENT 04`, `MAX 101` —
//! and the real event title only ever lives in programme data. With the feature on, such a name
//! is resolved to the currently airing programme title (cleaned by the configured rules) and
//! MATCHED under that title; neither `Channel.name` nor `Stream.name` is ever written.
//!
//! Three of the plugin's functions live here, path for path:
//!
//!   * [`EpgSettings`] — `_resolve_epg_matching_settings` (plugin.py:3808) and
//!     `_resolve_epg_placeholder_patterns` (3607): the placeholder patterns (case-insensitive,
//!     whole-name matches), the title cleanup rules, the skip titles, the channel schedule
//!     cleanup rules and the watched stream names.
//!   * [`EpgTitles`] — `_resolve_current_epg_title_for_epg_data_id` (3723) plus its two
//!     callers (3775, 3781): the per-entry title cache, filled from ONE batched read of
//!     `/epg/current-programs/`, with the "Next Event: X at TIME on DATE" gate
//!     (`_epg_next_event_date_is_today`, 3690).
//!   * The two consumers: the stream side (`_apply_epg_resolution_to_streams`, 3848, a
//!     `match_name` override) and the channel side (`_match_streams_to_channel`'s prefix,
//!     5849-5871, plus the schedule cleanup and `_is_epg_placeholder_name`).
//!
//! What is deliberately different, and why:
//!
//!   * **One request per fifty programmes, not one per lookup.** The plugin queries
//!     `ProgramData` per EPG record and memoizes per pass (~8600 queries on a
//!     1440-channel instance before the cache was added). Crosstalk resolves the set of
//!     EPG record ids a run actually needs, reads them in one chunked
//!     `POST /epg/current-programs/` (50 ids per request, the endpoint's own limit) and
//!     serves every lookup from a hash map. The answer is the same; the programme-read
//!     count is O(entries needed / 50) instead of O(lookups).
//!
//!   * **The `tvg_id` -> record join still lists every EPG entry.** A stream carries only
//!     the guide identifier string, and the record id it points at comes from listing
//!     `GET /epg/epgdata/` (~55 requests at 55k entries, `ceil(n / 1000)`). That listing
//!     is not narrowed the way the programme read is, because no verified endpoint
//!     narrows it: `EPGData` has no `tvg_id` filter (checked against Dispatcharr's
//!     `EPGDataViewSet`, which takes none), and the programme search is not an
//!     equivalent — it answers across every source sharing one `tvg_id` where the plugin
//!     takes the lowest record id, it never emits the `parsing` marker, and it skips the
//!     dummy-source and byte-index fallbacks `current-programs` applies. Listing the
//!     entries is the only read with the same answer, so it is kept and documented here
//!     rather than optimised into a divergence.
//!   * **`is_parsing()` markers are honoured.** The REST endpoint answers
//!     `{"epg_data_id": N, "parsing": true}` while a guide index is being rebuilt; that is
//!     treated as "no current programme" (the plugin cannot see the marker at all, because it
//!     queries the table directly).
//!   * **No unsafe-pattern gate.** The plugin refuses a nested-quantifier pattern
//!     (`_pattern_is_unsafe`) because Python's `re` can backtrack exponentially inside a worker
//!     that never yields. Rust's `regex` has no backtracking, so the pattern does what the
//!     operator wrote. The rule cap, the length cap and case-insensitivity are kept.
//!   * **The local day comes from Dispatcharr's own timezone.** The plugin reads General
//!     Settings -> Time Zone in-process; over REST the same value is one `GET
//!     /core/settings/` (the `system_settings` row's `value.time_zone`, read once per
//!     run) via [`DispatcharrClient::system_timezone`](crosstalk_core::dispatcharr::DispatcharrClient::system_timezone).
//!     The embedded "Mon D" clause is compared against today in that zone
//!     ([`today_in_zone`](crosstalk_core::dispatcharr::today_in_zone)), so an appliance and
//!     a Dispatcharr that disagree about the timezone still agree about the day. When the
//!     settings read fails, the run falls back to the host's date (`chrono::Local`) and
//!     says so in the report — the plugin's UTC fallback, one step closer to home.

use crate::selection::{RuleSet, REGEX_NAME_MAX_LEN, REGEX_PATTERN_MAX_LEN, REGEX_RULES_MAX};
use crate::settings::StreamsSettings;
use chrono::{Datelike, NaiveDate};
use crosstalk_core::dispatcharr::{Channel, CurrentProgram, EpgEntry, Stream};
use regex::{Regex, RegexBuilder};
use std::collections::{HashMap, HashSet};

/// How many placeholder patterns the setting may carry: `PluginConfig.REGEX_RULES_MAX`, the
/// same cap the plugin applies here.
pub const PLACEHOLDER_PATTERNS_MAX: usize = REGEX_RULES_MAX;

// ---------------------------------------------------------------------------------------
// The settings
// ---------------------------------------------------------------------------------------

/// One entry of the placeholder pattern list, and what it was worth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternReport {
    /// The pattern, truncated to 80 characters for the log.
    pub pattern: String,
    /// `ok`, `too_long`, `over_cap` or `compile_error`.
    pub status: &'static str,
    pub detail: String,
}

impl PatternReport {
    pub fn is_ok(&self) -> bool {
        self.status == "ok"
    }
}

/// `_resolve_epg_matching_settings` (3808): the EPG block resolved once per run.
#[derive(Debug, Clone, Default)]
pub struct EpgSettings {
    /// The feature is ON: the flag is set AND at least one pattern compiled. The plugin's
    /// `enabled and bool(patterns)`.
    pub enabled: bool,
    /// Compiled placeholder patterns: case-insensitive, whole-name matches (`fullmatch`).
    pub patterns: Vec<Regex>,
    /// What each entry of the setting was worth, for `validate` and the report.
    pub pattern_report: Vec<PatternReport>,
    /// `epg_title_cleanup_rules`, applied to a raw programme title.
    pub cleanup_rules: RuleSet,
    /// `epg_channel_schedule_cleanup_rules`, applied to a CHANNEL name before matching.
    pub channel_cleanup_rules: RuleSet,
    /// `epg_skip_titles`, lowercased: a cleaned title that is one of these is no signal.
    pub skip_titles: HashSet<String>,
    /// `epg_watch_source_streams`, lowercased: streams whose current programme may add them.
    pub watch_names: HashSet<String>,
}

impl EpgSettings {
    /// Resolve the block from `streams.toml`.
    pub fn from_settings(settings: &StreamsSettings) -> Self {
        let (patterns, pattern_report) =
            resolve_placeholder_patterns(&settings.epg_placeholder_name_patterns);
        Self {
            enabled: settings.epg_placeholder_matching_enabled && !patterns.is_empty(),
            patterns,
            pattern_report,
            cleanup_rules: RuleSet::parse(&settings.epg_title_cleanup_rules),
            channel_cleanup_rules: RuleSet::parse(&settings.epg_channel_schedule_cleanup_rules),
            skip_titles: comma_set(&settings.epg_skip_titles),
            watch_names: comma_set(&settings.epg_watch_source_streams),
        }
    }

    /// `_is_epg_placeholder_name` (3651): the WHOLE name matches one of the patterns.
    ///
    /// `fullmatch`, not `search`: the setting decides whether the entire name is a generic
    /// slot, so an unanchored pattern must not match a mere substring of a real name. The
    /// patterns are compiled anchored, so this is a plain `is_match`.
    pub fn is_placeholder(&self, name: &str) -> bool {
        if name.is_empty() || !self.enabled {
            return false;
        }
        self.patterns.iter().any(|pattern| pattern.is_match(name))
    }

    /// The channel name matching should see: the schedule cleanup rules, then — for a
    /// placeholder name — the currently airing title.
    ///
    /// `_match_streams_to_channel`'s prefix (5849-5871). The schedule cleanup runs on EVERY
    /// channel name while the feature is on (a name like "WWE Monday Night Raw | Monday @ 5"
    /// dilutes the score against a resolved title and its stray digit trips the
    /// channel-has-numbers guard), and the substitution only on a placeholder.
    pub fn channel_name(&self, name: &str, titles: &EpgTitles, epg_data_id: Option<i64>) -> String {
        if !self.enabled {
            return name.to_string();
        }
        let cleaned = self.channel_cleanup_rules.apply_to_str(name);
        if self.is_placeholder(&cleaned) {
            if let Some(title) = titles.title_for_epg(epg_data_id) {
                return title.to_string();
            }
        }
        cleaned
    }
}

/// `_resolve_epg_placeholder_patterns` (3607): parse and compile the pattern setting.
///
/// One pattern per line or comma-separated; blank entries ignored. Degrade-don't-fail: an
/// over-cap, over-long or uncompilable entry is reported and skipped, never raised.
pub fn resolve_placeholder_patterns(raw: &str) -> (Vec<Regex>, Vec<PatternReport>) {
    let mut patterns: Vec<Regex> = Vec::new();
    let mut report: Vec<PatternReport> = Vec::new();
    let raw = raw.trim();
    if raw.is_empty() {
        return (patterns, report);
    }
    for entry in raw.replace(',', "\n").split('\n') {
        let pattern = entry.trim();
        if pattern.is_empty() {
            continue;
        }
        let truncated = pattern.chars().take(80).collect();
        if patterns.len() + 1 > PLACEHOLDER_PATTERNS_MAX {
            report.push(PatternReport {
                pattern: truncated,
                status: "over_cap",
                detail: format!("over the {PLACEHOLDER_PATTERNS_MAX}-pattern cap, skipped"),
            });
            continue;
        }
        if pattern.chars().count() > REGEX_PATTERN_MAX_LEN {
            report.push(PatternReport {
                pattern: truncated,
                status: "too_long",
                detail: format!("over {REGEX_PATTERN_MAX_LEN} characters, skipped"),
            });
            continue;
        }
        // `re.compile(pattern, re.IGNORECASE)` plus `fullmatch` at use: anchor here so the
        // whole-name decision cannot drift from what the compiled pattern is.
        match RegexBuilder::new(&format!("^(?:{pattern})$"))
            .case_insensitive(true)
            .build()
        {
            Ok(compiled) => {
                report.push(PatternReport {
                    pattern: truncated,
                    status: "ok",
                    detail: String::new(),
                });
                patterns.push(compiled);
            }
            Err(error) => report.push(PatternReport {
                pattern: truncated,
                status: "compile_error",
                detail: format!("{error}"),
            }),
        }
    }
    (patterns, report)
}

/// A comma-separated setting as a set of trimmed, lowercased entries.
fn comma_set(raw: &str) -> HashSet<String> {
    raw.split(',')
        .map(|entry| entry.trim().to_lowercase())
        .filter(|entry| !entry.is_empty())
        .collect()
}

/// The host's local calendar day, as `(month, day)`.
///
/// Only the fallback: a run that could read Dispatcharr's timezone compares the "Next
/// Event" clause against [`today_in_zone`](crosstalk_core::dispatcharr::today_in_zone)
/// instead, so both ends of the comparison live in the same zone. When that read fails the
/// run degrades to this rather than refusing the match over one auxiliary request.
pub fn local_today() -> (u32, u32) {
    let today = chrono::Local::now().date_naive();
    (today.month(), today.day())
}

// ---------------------------------------------------------------------------------------
// The resolved titles
// ---------------------------------------------------------------------------------------

/// What one run's EPG lookups produced: the title per EPG record, and the `tvg_id` join.
///
/// Built once, before the (blocking) matching pass: the async fetches happen up front and the
/// collector only reads maps. An entry with nothing airing is present as `None`, exactly like
/// the plugin's cache, which is how a repeated lookup of the same idle entry stays free.
#[derive(Debug, Clone, Default)]
pub struct EpgTitles {
    settings: EpgSettings,
    /// `EPGData.tvg_id` -> EPG record id: the join a stream name needs, since a Stream carries
    /// only the guide id string.
    entry_by_tvg: HashMap<String, i64>,
    /// EPG record id -> the cleaned, currently airing title, if any.
    titles: HashMap<i64, Option<String>>,
    /// The calendar day the "Next Event: … on DATE" gate compares against.
    local_today: (u32, u32),
}

impl EpgTitles {
    /// Prepare the maps from the world's EPG entries; titles arrive with [`Self::set_programmes`].
    pub fn prepare(settings: EpgSettings, entries: &[EpgEntry], local_today: (u32, u32)) -> Self {
        let mut by_tvg: HashMap<String, i64> = HashMap::with_capacity(entries.len());
        for entry in entries {
            let tvg_id = entry.tvg_id.trim();
            if tvg_id.is_empty() {
                continue;
            }
            // `EPGData.objects.filter(tvg_id=...).values_list('id', flat=True).first()`: the
            // first row the database returns, which for an id column means the lowest.
            by_tvg
                .entry(tvg_id.to_string())
                .and_modify(|existing| {
                    if entry.id < *existing {
                        *existing = entry.id;
                    }
                })
                .or_insert(entry.id);
        }
        Self {
            settings,
            entry_by_tvg: by_tvg,
            titles: HashMap::new(),
            local_today,
        }
    }

    /// Resolve the titles from a `current_programs` answer.
    ///
    /// `CurrentProgram::is_parsing()` is respected: a guide whose index is being rebuilt says
    /// "not known yet", which is not a title to match against. Over REST an entry answers
    /// at most one row, so the earliest-row rule below only matters to a caller handing
    /// over more than one — and there it mirrors the plugin's
    /// `.order_by('start_time').first()`: the earliest-starting row decides, even when it is
    /// empty or a parsing marker, and a later row for the same entry is not consulted.
    pub fn set_programmes(&mut self, programmes: &[CurrentProgram]) {
        // Postgres sorts NULL start times last under ascending order, so a row with no time
        // never outranks a timed one the way a bare `Option` sort (Nones first) would let it.
        let mut ordered: Vec<&CurrentProgram> = programmes.iter().collect();
        ordered.sort_by_key(|programme| (programme.start_time.is_none(), programme.start_time));
        let mut seen: HashSet<i64> = HashSet::new();
        for programme in ordered {
            if programme.epg_data_id == 0 || !seen.insert(programme.epg_data_id) {
                continue;
            }
            let title = if programme.has_title() {
                self.resolve(&programme.title)
            } else {
                None
            };
            self.titles.insert(programme.epg_data_id, title);
        }
    }

    /// The titles a run needs: one call, before the matching pass.
    ///
    /// Only the entries a placeholder name or a watched stream can reach: a channel whose
    /// (schedule-cleaned) name is a placeholder, and a stream whose raw name is a placeholder
    /// or on the watch list. Everything else is left unfetched — at 50k streams that is the
    /// difference between a handful of ids and the whole guide.
    pub fn needed_ids(&self, channels: &[Channel], streams: &[Stream]) -> Vec<i64> {
        let mut needed: HashSet<i64> = HashSet::new();
        if !self.settings.enabled {
            return Vec::new();
        }
        for channel in channels {
            let cleaned = self
                .settings
                .channel_cleanup_rules
                .apply_to_str(&channel.name);
            if !self.settings.is_placeholder(&cleaned) {
                continue;
            }
            if let Some(id) = channel.current_epg_id() {
                needed.insert(id);
            }
        }
        for stream in streams {
            // A stream with no guide id can never resolve an EPG record, so it costs nothing
            // to skip it before the pattern match.
            if stream.tvg_id.trim().is_empty() {
                continue;
            }
            let raw = stream.name.as_str();
            if raw.chars().count() > REGEX_NAME_MAX_LEN {
                continue;
            }
            let watched = self
                .settings
                .watch_names
                .contains(&raw.trim().to_lowercase());
            if !watched && !self.settings.is_placeholder(raw) {
                continue;
            }
            if let Some(id) = self.entry_by_tvg.get(stream.tvg_id.trim()) {
                needed.insert(*id);
            }
        }
        let mut ids: Vec<i64> = needed.into_iter().collect();
        ids.sort_unstable();
        ids
    }

    pub fn settings(&self) -> &EpgSettings {
        &self.settings
    }

    pub fn is_enabled(&self) -> bool {
        self.settings.enabled
    }

    /// The cleaned current title on an EPG record, if one is airing.
    pub fn title_for_epg(&self, epg_data_id: Option<i64>) -> Option<&str> {
        let id = epg_data_id?;
        self.titles.get(&id)?.as_deref()
    }

    /// The same, joined through a stream's `tvg_id`.
    pub fn title_for_tvg(&self, tvg_id: &str) -> Option<&str> {
        let tvg_id = tvg_id.trim();
        if tvg_id.is_empty() {
            return None;
        }
        self.title_for_epg(self.entry_by_tvg.get(tvg_id).copied())
    }

    /// The same, for a stream row.
    pub fn title_for_stream(&self, stream: &Stream) -> Option<&str> {
        self.title_for_tvg(&stream.tvg_id)
    }

    /// The channel name matching should see (see [`EpgSettings::channel_name`]).
    pub fn channel_name(&self, channel: &Channel) -> String {
        self.settings
            .channel_name(&channel.name, self, channel.current_epg_id())
    }

    /// `_resolve_current_epg_title_for_epg_data_id`'s gates (3723), applied to one raw title.
    ///
    /// In order: the "Next Event: X at TIME on DATE" clause must be today's (a waiting
    /// placeholder can stay the current row for days), then the cleanup rules, then the
    /// trimmed result must not be empty and must not be a configured skip title.
    fn resolve(&self, raw: &str) -> Option<String> {
        if raw.is_empty() || !next_event_date_is_today(raw, self.local_today) {
            return None;
        }
        let cleaned = self.settings.cleanup_rules.apply_to_str(raw);
        let cleaned = cleaned.trim();
        if cleaned.is_empty() || self.settings.skip_titles.contains(&cleaned.to_lowercase()) {
            return None;
        }
        Some(cleaned.to_string())
    }
}

/// What the stream-side pass did, for the counters the plugin reports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EpgRun {
    /// Placeholder-named streams the pass considered.
    pub checked: usize,
    /// Of those, how many resolved to a current title (and had their match name replaced).
    pub resolved: usize,
    /// Stream names over [`REGEX_NAME_MAX_LEN`], skipped.
    pub skipped_long: usize,
    /// Streams force-included by the watched-stream rule, summed over groups.
    pub watch_hits: usize,
}

impl EpgRun {
    pub fn summary(&self) -> String {
        format!(
            "{}/{} placeholder stream(s) resolved to a current EPG title",
            self.resolved, self.checked
        )
    }
}

/// `_epg_next_event_date_is_today` (3690): whether a "Next Event" clause is dated today.
///
/// True when the title carries no `at HH:MMAM/PM on Mon D` clause (a live, unwrapped title is
/// exactly what a real event looks like once the provider flips it), or when the embedded date
/// is today's. False only when a date IS embedded, parses, and is not today. A clause that
/// matches the shape but not a recognised month name is treated permissively, as the plugin
/// does (and, like the plugin, it is the caller's log that notices).
pub fn next_event_date_is_today(title: &str, today: (u32, u32)) -> bool {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let regex = RE.get_or_init(|| {
        Regex::new(r"(?i)\bat\s+\d{1,2}:\d{2}\s*[AP]M\s+on\s+([A-Za-z]{3,9}\s+\d{1,2})\s*$")
            .expect("static regex")
    });
    let Some(captures) = regex.captures(title) else {
        return true;
    };
    let Some(captured) = captures.get(1) else {
        return true;
    };
    parse_month_day(captured.as_str().trim())
        .map(|date| date == today)
        .unwrap_or(true)
}

/// Parse a captured `Mon D` / `Month D` clause, returning `(month, day)`.
///
/// Both recognised formats are tried in order (`%b %d %Y`, `%B %d %Y`), and chrono is lenient
/// about which of the two spellings `%b`/`%B` accept, so `Aug 5`, `Aug 05`, `August 5` and
/// `August 05` all land on the same day — the plugin's own review finding.
///
/// The year is a fixed leap year rather than the current one: the comparison is a
/// `(month, day)` pair, and a year that is not a leap year would reject a `Feb 29` clause that
/// the plugin's `strptime` (given the current year) accepts in a leap year.
fn parse_month_day(captured: &str) -> Option<(u32, u32)> {
    const LEAP_YEAR: i32 = 2024;
    let text = format!("{captured} {LEAP_YEAR}");
    for format in ["%b %d %Y", "%B %d %Y"] {
        if let Ok(date) = NaiveDate::parse_from_str(&text, format) {
            return Some((date.month(), date.day()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_patterns(raw: &str) -> EpgSettings {
        EpgSettings::from_settings(&StreamsSettings {
            epg_placeholder_matching_enabled: true,
            epg_placeholder_name_patterns: raw.to_string(),
            ..StreamsSettings::default()
        })
    }

    fn entry(id: i64, tvg_id: &str) -> EpgEntry {
        EpgEntry {
            id,
            name: String::new(),
            tvg_id: tvg_id.to_string(),
            epg_source: Some(1),
        }
    }

    fn programme(epg_data_id: i64, title: &str) -> CurrentProgram {
        CurrentProgram {
            epg_data_id,
            title: title.to_string(),
            ..CurrentProgram::default()
        }
    }

    #[test]
    fn only_a_whole_name_matches_a_placeholder_pattern() {
        let settings = with_patterns("PPV");
        // `fullmatch`, not `search`: the shipped defaults are anchored, and an unanchored
        // operator pattern must not match a substring of a real name either.
        assert!(settings.is_placeholder("PPV"));
        assert!(!settings.is_placeholder("PPV EVENT 04"));
        let settings = with_patterns(r"^PPV EVENT \d+$");
        assert!(settings.is_placeholder("PPV EVENT 04"));
        assert!(!settings.is_placeholder("XPPV EVENT 04X"));
        assert!(!settings.is_placeholder(""));
    }

    #[test]
    fn the_shipped_defaults_are_the_plugins() {
        let settings = EpgSettings::from_settings(&StreamsSettings::default());
        // Off by default, so nothing is a placeholder even though the list parses.
        assert!(!settings.enabled);
        assert!(!settings.is_placeholder("PPV EVENT 04"));
        assert_eq!(settings.patterns.len(), 6);
        assert_eq!(settings.skip_titles.len(), 3);
        assert!(settings.skip_titles.contains("no event today"));
    }

    #[test]
    fn a_bad_pattern_is_reported_and_skipped_not_raised() {
        let (patterns, report) = resolve_placeholder_patterns("^HBO ([\n^HBO \\d+$");
        assert_eq!(patterns.len(), 1);
        assert_eq!(report.len(), 2);
        assert_eq!(report[0].status, "compile_error");
        assert!(!report[0].detail.is_empty(), "{:?}", report[0]);
        assert!(report[1].is_ok());
    }

    #[test]
    fn the_pattern_list_is_capped_and_long_patterns_skipped() {
        let many: Vec<String> = (0..PLACEHOLDER_PATTERNS_MAX + 3)
            .map(|index| format!("^X{index}$"))
            .collect();
        let (patterns, report) = resolve_placeholder_patterns(&many.join("\n"));
        assert_eq!(patterns.len(), PLACEHOLDER_PATTERNS_MAX);
        assert_eq!(
            report
                .iter()
                .filter(|entry| entry.status == "over_cap")
                .count(),
            3
        );

        let long = format!("^{}$", "a".repeat(REGEX_PATTERN_MAX_LEN + 1));
        let (patterns, report) = resolve_placeholder_patterns(&long);
        assert!(patterns.is_empty());
        assert_eq!(report[0].status, "too_long");
    }

    #[test]
    fn a_cleaned_title_is_trimmed_and_skip_titles_ignored() {
        let settings = with_patterns(r"^PPV EVENT \d+$");
        let titles = {
            let mut titles = EpgTitles::prepare(settings, &[entry(7, "slot.us")], (8, 5));
            titles.set_programmes(&[
                programme(7, "Next Event: WWE MONDAY NIGHT RAW at 05:00PM on Aug 5"),
                programme(8, "No Event Today"),
                programme(9, "   "),
            ]);
            titles
        };
        assert_eq!(titles.title_for_epg(Some(7)), Some("WWE MONDAY NIGHT RAW"));
        // A skip title, whatever the gate before it said.
        assert_eq!(titles.title_for_epg(Some(8)), None);
        assert_eq!(titles.title_for_epg(Some(9)), None);
        assert_eq!(titles.title_for_epg(None), None);
        assert_eq!(titles.title_for_epg(Some(404)), None);
    }

    #[test]
    fn a_next_event_clause_dated_another_day_is_not_today() {
        // The bug the gate exists for: a waiting placeholder stays the current row for days.
        assert!(next_event_date_is_today("WWE MONDAY NIGHT RAW", (8, 5)));
        assert!(next_event_date_is_today(
            "Next Event: Some Show at 05:00pm on Aug 05",
            (8, 5)
        ));
        assert!(next_event_date_is_today(
            "Next Event: Some Show at 05:00PM on August 5",
            (8, 5)
        ));
        assert!(!next_event_date_is_today(
            "Next Event: AEW Grand Slam at 05:00PM on Aug 6",
            (8, 5)
        ));
        // An unrecognised month name is permissive, exactly as the plugin logs and keeps it.
        assert!(next_event_date_is_today(
            "Next Event: Some Show at 05:00PM on Zzz 5",
            (8, 5)
        ));
        // A leap day parses (the comparison is a month/day pair, not a real date this year).
        assert!(next_event_date_is_today(
            "Next Event: Some Show at 05:00PM on Feb 29",
            (2, 29)
        ));
        // The clause must be at the end: a date in the middle is not a waiting prefix.
        assert!(next_event_date_is_today(
            "Live at 05:00PM on Aug 6 highlights",
            (8, 5)
        ));
    }

    #[test]
    fn the_earliest_row_decides_even_when_it_is_empty() {
        // The plugin's `.order_by('start_time').first()`: one row per entry, and the
        // earliest-starting row is the answer even when it carries no title. A later
        // overlapping row for the same entry is not consulted.
        let settings = with_patterns(r"^PPV EVENT \d+$");
        let mut titles = EpgTitles::prepare(settings, &[entry(7, "slot.us")], (8, 5));
        let early = CurrentProgram {
            epg_data_id: 7,
            title: String::new(),
            start_time: Some(chrono::Utc::now() - chrono::Duration::hours(2)),
            ..CurrentProgram::default()
        };
        let late = CurrentProgram {
            epg_data_id: 7,
            title: "WWE SUMMERSLAM".to_string(),
            start_time: Some(chrono::Utc::now() - chrono::Duration::hours(1)),
            ..CurrentProgram::default()
        };
        titles.set_programmes(&[late, early]);
        assert_eq!(titles.title_for_epg(Some(7)), None);
    }

    #[test]
    fn a_parsing_marker_is_not_a_title() {
        let settings = with_patterns(r"^PPV EVENT \d+$");
        let mut titles = EpgTitles::prepare(settings, &[entry(7, "slot.us")], (8, 5));
        titles.set_programmes(&[CurrentProgram {
            epg_data_id: 7,
            parsing: true,
            ..CurrentProgram::default()
        }]);
        assert_eq!(titles.title_for_epg(Some(7)), None);
    }

    #[test]
    fn a_channel_is_matched_under_its_current_title_and_the_schedule_is_stripped() {
        let settings = with_patterns(r"^PPV EVENT \d+$");
        let mut titles = EpgTitles::prepare(settings, &[entry(7, "slot.us")], (8, 5));
        titles.set_programmes(&[programme(7, "WWE SUMMERSLAM")]);
        let placeholder = Channel {
            id: 1,
            name: "PPV EVENT 04".to_string(),
            epg_data_id: Some(7),
            ..Channel::default()
        };
        assert_eq!(titles.channel_name(&placeholder), "WWE SUMMERSLAM");

        // A real name is cleaned by the schedule rules but never substituted.
        let scheduled = Channel {
            id: 2,
            name: "WWE Monday Night Raw | Monday @ 5".to_string(),
            epg_data_id: Some(7),
            ..Channel::default()
        };
        assert_eq!(titles.channel_name(&scheduled), "WWE Monday Night Raw");

        // A placeholder with nothing airing keeps its own name.
        let idle = Channel {
            id: 3,
            name: "PPV EVENT 04".to_string(),
            ..Channel::default()
        };
        assert_eq!(titles.channel_name(&idle), "PPV EVENT 04");
    }

    #[test]
    fn only_the_entries_a_run_can_reach_are_asked_for() {
        let settings = with_patterns(r"^PPV EVENT \d+$");
        let titles = EpgTitles::prepare(
            settings,
            &[entry(7, "a.us"), entry(8, "b.us"), entry(9, "c.us")],
            (8, 5),
        );
        let channels = vec![
            Channel {
                id: 1,
                name: "PPV EVENT 04".to_string(),
                epg_data_id: Some(8),
                ..Channel::default()
            },
            // Not a placeholder: never asked about.
            Channel {
                id: 2,
                name: "ESPN".to_string(),
                epg_data_id: Some(9),
                ..Channel::default()
            },
        ];
        let streams = vec![
            Stream {
                id: 10,
                name: "PPV EVENT 04".to_string(),
                tvg_id: "a.us".to_string(),
                ..Stream::default()
            },
            // A watched stream is asked about even though its name is not a placeholder.
            Stream {
                id: 11,
                name: "ESPN".to_string(),
                tvg_id: "c.us".to_string(),
                ..Stream::default()
            },
        ];
        let mut settings = titles.settings().clone();
        settings.watch_names.insert("espn".to_string());
        let mut titles = titles;
        titles.settings = settings;
        assert_eq!(titles.needed_ids(&channels, &streams), vec![7, 8, 9]);
    }

    #[test]
    fn the_tvg_join_takes_the_lowest_entry_id_and_ignores_blank_ids() {
        let settings = with_patterns(r"^PPV EVENT \d+$");
        let titles = EpgTitles::prepare(
            settings,
            &[entry(9, "dup.us"), entry(7, "dup.us"), entry(8, "  ")],
            (8, 5),
        );
        assert_eq!(titles.title_for_tvg("dup.us"), None); // nothing fetched yet
        let mut titles = titles;
        titles.set_programmes(&[programme(7, "EVENT A"), programme(9, "EVENT B")]);
        assert_eq!(titles.title_for_tvg("dup.us"), Some("EVENT A"));
        assert_eq!(titles.title_for_tvg("  "), None);
        assert_eq!(titles.title_for_tvg(""), None);
    }
}
