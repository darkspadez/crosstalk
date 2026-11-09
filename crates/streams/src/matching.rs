//! Matching: which streams a channel should carry, and in what order.
//!
//! Step 9 (goal reset): streams runs the unified matching layer — one ranker (the
//! unified engine's best identity family), one selection rule (the whole family, all
//! members ranked, above the one `[matching]` cut), one cache. Stream-Mapparr's Match &
//! Assign (`add_streams_to_channels_action`), its Preview (`preview_changes_action`) and
//! its US-OTA-only mode (`match_us_ota_only_action`) are the oracles, not the target:
//! where the answer now differs, `docs/oracle-reports/streams.md` attributes every
//! difference.
//!
//! # The shape of the answer
//!
//! A run is a pipeline:
//!
//!   1. **Scope the channels** — `[scope]` in `crosstalk.toml` (`scope::scope_channels`).
//!   2. **Group them by identity family** — [`build_groups`]: channels resolving to the
//!      same identity family key, qualified (`key@@US`) with the country the leader's
//!      family is ranked under — the filter's channel country when it is on, the name's
//!      own marker when it is off — unless the identity carries a callsign. The same
//!      grouping function serves [`crate::visibility`].
//!   3. **Collect once per group** — the group's answer is the best identity family of
//!      streams: the leader's family from the unified engine at the `[matching]` cut,
//!      all members in rank order, plus the EPG watched-stream force-include (when the
//!      optional EPG path is on). With the EPG path on, a placeholder channel or stream
//!      is matched under the programme airing on it — see [`crate::epg`] for that half.
//!      Then the `_finalize_streams` tail (quality sort, same-country first, dedupe).
//!   4. **Assign to the group's channels** — `visible_channel_limit` / zone siblings
//!      (`_channels_to_update_for_group`), per-channel zone routing and country partition,
//!      then `_write_channel_streams`'s overwrite-or-append list.
//!
//! The engine and its index are built once per run ([`RunData`]) and every group ranks
//! against the shared index on rayon, off the async runtime (like
//! [`crosstalk_core::match_run`]); the answer is assembled in group order, so parallelism
//! cannot reorder a result.
//!
//! # Removed Stream-Mapparr rules (each traced to its decisions row)
//!
//!   * The OTA callsign regex path with corroboration (C06/D04: the engine's callsign
//!     anchor decides callsign agreement at family level; station-format names resolve
//!     through the merged allowlist, C02/D05).
//!   * Alias force-includes (A01/A02: the merged reverse index is inside the engine; an
//!     alias hit is a family member or nothing).
//!   * The fuzzy gate plus the token/substring family with its number guards (T03/T05/T06:
//!     stage structure, length scaling and the trailing-number guard live in the engine).
//!   * SM's normalized-name groups (D07: grouping is the identity family key).
//!   * The per-app cut `match_sensitivity` (D02/T10: retired in favour of `[matching]`
//!     sensitivity).
//!
//! # Deliberate differences from the plugin
//!
//!   * **Only real changes are written.** The plugin deletes and recreates every matched
//!     channel's list; crosstalk plans `Op::Set { field: Streams }` only for a channel whose
//!     list actually differs, so a second run over an unchanged library writes nothing.
//!     The end state is identical.
//!   * **Preview's threshold analysis is not ported.** `preview_changes_action` also calls
//!     `_get_matches_at_thresholds`, which re-runs the pipeline at 5-point steps and collects
//!     by exact match against the *winning stream name* rather than the channel name (the
//!     plugin itself notes this path disagrees with the real matcher). Those rows were advice,
//!     never a write, and the CSV's own `will_update=No` marked them so.
//!   * **`filter_dead_streams` is off by default, as in the plugin.** When on, a stream is
//!     dead on exactly the evidence Stream-Mapparr's `_stream_stats_says_dead` reads — an
//!     empty `stream_stats` (what IPTV Checker writes for a dead stream), a zero width or
//!     height, or a `0x0` resolution — or a `dead` probe verdict in the store, which is the
//!     same verdict collected on purpose. A stream with no evidence either way is kept (fail
//!     open, as the plugin's "no metadata" branch does), and when every candidate is dead
//!     the whole list is kept rather than matching nothing.
//!   * **Borderline groups go through the `[ai.verify]` chain** (task "streams", same
//!     pattern as guide's step-8 review: `UnifiedEngine::with_threshold`,
//!     `UnifiedEngine::shortlist_index_in`, `match_run::score_shortlists`,
//!     `RunEngine::shortlist_engine`). A group with no family at the cut is re-ranked at
//!     the gate's floor; only a model accept can attach a sub-cut stream.
//!
//! # Lineup mode (step 19)
//!
//! A workflow step with `scope = "lineup"` reads lineup's owned channels through the
//! id bound (step 12: the bound already replaces the host scope, so the channel set
//! needs no work here). What lineup mode adds is the country restriction, from the
//! hint the scope carries — the lineup file's country — with no dependency from
//! streams back to lineup: lineup sets the hint on its own
//! [`crosstalk_core::subapp::NamedScopeResolution`], core carries it on
//! [`crosstalk_core::jobs::ResolvedScope`], and this module reads it through
//! [`crosstalk_core::jobs::JobContext::scope_hints`].
//!
//! With the hint and `lineup_mode_country_restriction` on (the default), the run
//! behaves as if `restrict_matching_to_country` was on, with the lineup's country as
//! the default channel country ([`MatchOptions::lineup_country`], enforced in
//! [`effective_channel_country`]). The hint is the default, not the law: a channel
//! whose identity names a different known country keeps its own (a foreign entry in a
//! domestic lineup still matches its own country's streams), and a channel whose
//! identity carries a callsign is never country-qualified (bug-161), hint or not.
//! With the setting off, a lineup step matches exactly like an ordinary run.

use crate::epg::{EpgRun, EpgSettings, EpgTitles};
use crate::quality::{self, QualityKey, QualityModel, ThroughputSource};
use crate::selection::{self, StreamPool};
use crate::settings::StreamsSettings;
use anyhow::{Context, Result};
use crosstalk_core::ai_gate;
use crosstalk_core::channel_ops::{self, ApplyReport, Field, Op, Plan, ReportSpec, SetValue};
use crosstalk_core::config::{AiVerifyConfig, MatchingConfig, ScopeConfig};
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{Channel, Stream, World, WorldKind, WorldKinds};
use crosstalk_core::engine_run::RunEngine;
use crosstalk_core::jobs::JobContext;
use crosstalk_core::scope::scope_channels;
use crosstalk_core::store::Store;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use crosstalk_matching::callsign::{
    extract_callsign_with_confidence_for, normalize_callsign, CallsignLadder, CALLSIGN_DENYLIST,
};
use crosstalk_matching::country::{
    self, classify, country_from_group, country_from_name, extract_zone_noguard,
    parse_prefix_country_overrides, FeedZone, Relationship,
};
use crosstalk_matching::data::{is_us_station, stream_mapparr_stations};
use crosstalk_matching::engine::{EngineIndex as UnifiedIndex, EngineRanked, UnifiedEngine};
use crosstalk_matching::identity::{resolve_with, FamilyKey, ResolveContext};
use crosstalk_matching::normalize::Normalizer;
use crosstalk_matching::pipeline::Scored;
// Oracle tooling only (parity tests, the indexed-vs-full-scan check): the Stream-Mapparr
// profile matcher and its entry points. Production scores with the unified engine.
#[cfg(test)]
use crosstalk_matching::pipeline::{
    BestMatch, CandidateIndex, MatchOptions as MatcherOptions, Matcher,
};
#[cfg(test)]
use crosstalk_matching::profile::MatchProfile;
#[cfg(test)]
use crosstalk_matching::pyre::py_split_whitespace;
#[cfg(test)]
use crosstalk_matching::similarity::calculate_similarity_chars;
#[cfg(test)]
use crosstalk_matching::tokens::{is_digit, process_string_for_matching};
use rayon::prelude::*;
use regex::Regex;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

// =======================================================================================
// The channel databases: `_load_channels_data` and friends
// =======================================================================================

/// One entry of an enabled `*_channels.json`: a channel name and the country it belongs to.
///
/// Stream-Mapparr loads these into a flat list (`_load_channels_data`, 5466) with a
/// `_country_code` stamped from the file, and then scans the list three ways per channel.
/// Crosstalk indexes them once by name (the list is ~10k entries and a run asks 20k times).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseEntry {
    pub name: String,
    /// `_country_code`: the file's country code.
    pub code: String,
    /// A `callsign` the entry itself carries. None of the shipped databases carry one (see
    /// `_channel_ota_callsign`), but the field is honoured when it is there.
    pub callsign: Option<String>,
}

/// The channel databases a run resolves countries and OTA-ness from.
#[derive(Debug, Clone, Default)]
pub struct ChannelDatabase {
    entries: Vec<DatabaseEntry>,
    /// Exact name -> first entry with that name (`_get_channel_info_from_json`'s first tier).
    by_exact: HashMap<String, usize>,
    /// Lowercased name -> every entry with that name, in file order
    /// (`_get_all_channel_info_matches`, and the second tier of the exact lookup).
    by_lower: HashMap<String, Vec<usize>>,
}

impl ChannelDatabase {
    /// An empty database: no entry, which means no country and no OTA channel.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Build from entries, indexing them the way both lookups read them.
    pub fn from_entries(entries: impl IntoIterator<Item = DatabaseEntry>) -> Self {
        let entries: Vec<DatabaseEntry> = entries.into_iter().collect();
        let mut by_exact: HashMap<String, usize> = HashMap::new();
        let mut by_lower: HashMap<String, Vec<usize>> = HashMap::new();
        for (index, entry) in entries.iter().enumerate() {
            by_exact.entry(entry.name.clone()).or_insert(index);
            by_lower
                .entry(entry.name.to_lowercase())
                .or_default()
                .push(index);
        }
        Self {
            entries,
            by_exact,
            by_lower,
        }
    }

    /// The MERGED databases for `codes`, in code order, concatenated the way
    /// `_load_channels_data` concatenates the plugin's enabled files.
    ///
    /// Phase 3 review: one merged data set for every app. The plugin read its own
    /// per-country files; the country filter now reads the same union tables the
    /// unified engine, lineup's names and categorize read
    /// (`crosstalk_matching::data::merged::merged_channels`), so a name only another
    /// plugin's table listed resolves to its country here too, and `DO`/`PE`/`PL`/`PT`
    /// are selectable like every other merged country. The merged entries carry no
    /// `callsign` field (none of the plugins' files did either), so OTA-ness still
    /// comes from the station table.
    pub fn shipped(codes: &[&str]) -> Self {
        let mut entries: Vec<DatabaseEntry> = Vec::new();
        for code in codes {
            let Some(db) = crosstalk_matching::data::merged::merged_channels(code) else {
                continue;
            };
            for channel in &db.channels {
                entries.push(DatabaseEntry {
                    name: channel.channel_name.clone(),
                    code: code.to_uppercase(),
                    callsign: None,
                });
            }
        }
        Self::from_entries(entries)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `_get_channel_info_from_json` (6743): an exact-case match first, then a
    /// case-insensitive one, both first-wins.
    fn info(&self, channel_name: &str) -> Option<&DatabaseEntry> {
        if let Some(index) = self.by_exact.get(channel_name) {
            return self.entries.get(*index);
        }
        self.by_lower
            .get(&channel_name.to_lowercase())
            .and_then(|indices| indices.first())
            .and_then(|index| self.entries.get(*index))
    }

    /// `_get_all_channel_info_matches` (6755): every case-insensitive match, file order.
    fn matches(&self, channel_name: &str) -> &[usize] {
        self.by_lower
            .get(&channel_name.to_lowercase())
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

// =======================================================================================
// Country: `_channel_country_code`, `_stream_country_code`, `classify`
// =======================================================================================

/// `_channel_country_code` (4515) for one channel, ambiguity guard included.
///
/// With `matches` (the country filter's need-matches path) the database signal is trusted
/// only when every matching entry agrees on one code; otherwise the group/name signal wins.
/// Without it (the grouping path when the filter is off never asks) the single entry wins
/// outright. `None` means the channel's country is unknown, and the filter fails open.
fn channel_country_code(
    channel: &Channel,
    database: &ChannelDatabase,
    need_matches: bool,
    group_name: &str,
) -> Option<String> {
    let group_name_code = country_from_group(group_name)
        .or_else(|| country_from_name(&channel.name))
        .map(str::to_string);
    if need_matches {
        let matches = database.matches(&channel.name);
        let mut codes: HashSet<String> = HashSet::new();
        for index in matches {
            if let Some(entry) = database.entries.get(*index) {
                if !entry.code.trim().is_empty() {
                    codes.insert(entry.code.trim().to_uppercase());
                }
            }
        }
        if codes.len() == 1 {
            return codes.into_iter().next();
        }
        return group_name_code;
    }
    if let Some(entry) = database.info(&channel.name) {
        if !entry.code.trim().is_empty() {
            return Some(entry.code.trim().to_uppercase());
        }
    }
    group_name_code
}

/// `_stream_stats_says_dead` (plugin.py:5191): true only on positive evidence that a stream
/// is dead.
///
/// `stats` is the Dispatcharr `Stream.stream_stats` JSON value. Evidence of death:
/// an empty dict (IPTV Checker writes exactly `{}` for a stream it found dead), a zero
/// width or height (from width/height when both are usable integers, otherwise from a `WxH`
/// resolution string). Everything else, including null, non-dict values, audio-only stats
/// and unparseable resolutions, is NOT dead (fail open).
///
/// Python's `int()` is reproduced exactly: booleans are unusable, floats truncate toward
/// zero, and strings must parse as a whole integer (so `"3.5"` and `"0x10"` are not
/// numbers, matching `int()` raising `ValueError`).
pub fn stream_stats_says_dead(stats: &serde_json::Value) -> bool {
    let Some(object) = stats.as_object() else {
        return false;
    };
    if object.is_empty() {
        return true;
    }
    fn as_int(value: Option<&serde_json::Value>) -> Option<i64> {
        let value = value?;
        if value.is_boolean() {
            return None;
        }
        match value {
            serde_json::Value::Number(number) => {
                if let Some(int) = number.as_i64() {
                    Some(int)
                } else if let Some(uint) = number.as_u64() {
                    uint.try_into().ok()
                } else {
                    number.as_f64().and_then(|float| {
                        if float.is_finite() {
                            Some(float.trunc() as i64)
                        } else {
                            None
                        }
                    })
                }
            }
            serde_json::Value::String(text) => py_int_str(text),
            _ => None,
        }
    }
    let width = as_int(object.get("width"));
    let height = as_int(object.get("height"));
    if let (Some(width), Some(height)) = (width, height) {
        return width == 0 || height == 0;
    }
    if let Some(resolution) = object.get("resolution").and_then(|value| value.as_str()) {
        let lowered = resolution.to_lowercase();
        let parts: Vec<&str> = lowered.split('x').collect();
        if parts.len() == 2 {
            if let (Some(width), Some(height)) = (py_int_str(parts[0]), py_int_str(parts[1])) {
                return width == 0 || height == 0;
            }
        }
    }
    false
}

/// Python's `int()` over a string: surrounding whitespace is ignored, the rest must be a
/// whole integer. Underscore separators (`int("1_0") == 10`) are not reproduced: stream
/// stats never hold them, and accepting them would need a second parse path.
fn py_int_str(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    // `i64::from_str` accepts a leading `+`, as `int()` does, and rejects floats, hex
    // and anything with leftover characters, as `int()` does.
    trimmed.parse::<i64>().ok()
}

/// `_stream_country_code` (4582): the group label, then the RAW name, then the operator's
/// prefix overrides (group label first, as the plugin consults it).
///
/// Reads the raw `Stream.name`, never the regex-rule match name: a rule that strips a
/// country prefix for matching must not blind country detection (issue #36).
fn stream_country_code(
    stream: &Stream,
    group_name: &str,
    overrides: &BTreeMap<String, &'static str>,
) -> Option<String> {
    country_from_group(group_name)
        .or_else(|| country_from_name(&stream.name))
        .or_else(|| country::country_from_prefix_overrides(&stream.name, overrides))
        .or_else(|| country::country_from_prefix_overrides(group_name, overrides))
        .map(str::to_string)
}

/// Stream-Mapparr's `country_stats` counters, one run's worth.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CountryStats {
    /// Groups where the filter engaged (a country was known).
    pub engaged: usize,
    /// Groups where it was skipped because the channel's country was unknown.
    pub skipped_unknown_channel: usize,
    pub foreign_dropped: usize,
    pub unknown_kept: usize,
}

impl CountryStats {
    fn merge(&mut self, other: CountryStats) {
        self.engaged += other.engaged;
        self.skipped_unknown_channel += other.skipped_unknown_channel;
        self.foreign_dropped += other.foreign_dropped;
        self.unknown_kept += other.unknown_kept;
    }
}

// =======================================================================================
// Name data and the index
// =======================================================================================

/// A channel or stream name in the forms the collector compares.
///
/// Two normalisers are in play and they are deliberately not the same one: the fuzzy stages
/// read the candidate cache (`precompute_normalizations`, which the plugin fills WITH the
/// run's ignore tags and WITHOUT `remove_cinemax`), while the family collection re-cleans each
/// name with `_clean_channel_name`, which honours `remove_cinemax` for a channel whose name
/// contains `max`. [`StreamNames`] keeps the first; the second is computed on demand for those
/// channels only ([`StreamNames::cinemax_variant`]).
#[cfg(test)]
struct StreamNames {
    /// The name matching sees (`_mname`), per eligible position.
    match_names: Vec<String>,
    /// Whitespace tokens of the normalised lowercased name (`stream_lower.split()`).
    tokens: Vec<Vec<String>>,
    /// Whether the name contains the word `Cinemax` at all: `remove_cinemax` only ever strips
    /// that word, so every other name cleans identically with the flag on and the cached
    /// normalisation serves both.
    has_cinemax: Vec<bool>,
}

#[cfg(test)]
impl StreamNames {
    /// Built over the run's candidate names, in the order and length of the candidate index.
    ///
    /// The names handed over are the ones MATCHING sees — after the regex rules and, with the
    /// EPG feature on, after the placeholder substitution — so every predicate below reads the
    /// same string the index was built from. Reading the pool's own names here instead would
    /// split the two views apart exactly on the streams EPG resolution rewrote.
    fn new(match_names: &[String], index: &CandidateIndex) -> Self {
        let match_names: Vec<String> = match_names.to_vec();
        let tokens = (0..match_names.len())
            .map(|position| {
                index
                    .normalized_lower(position)
                    .map(|lower| {
                        py_split_whitespace(lower)
                            .map(str::to_string)
                            .collect::<Vec<String>>()
                    })
                    .unwrap_or_default()
            })
            .collect();
        let has_cinemax = match_names
            .iter()
            .map(|name| name.to_lowercase().contains("cinemax"))
            .collect();
        Self {
            match_names,
            tokens,
            has_cinemax,
        }
    }
}

/// `re.sub(r'[\s&\-]+', '', text)`: the plugin's no-space form.
#[cfg(test)]
fn strip_nospace(text: &str) -> String {
    text.chars()
        .filter(|c| !(c.is_whitespace() || *c == '&' || *c == '-'))
        .collect()
}

/// `_same_but_for_word_breaks` (637): identical once spaces are removed.
#[cfg(test)]
fn same_but_for_word_breaks(left: &str, right: &str) -> bool {
    if left.is_empty() || right.is_empty() {
        return false;
    }
    left.replace(' ', "") == right.replace(' ', "")
}

/// A two-character gram, packed for a hash key.
#[cfg(test)]
fn pack_gram(left: char, right: char) -> u64 {
    ((left as u64) << 32) | (right as u64)
}

/// Every adjacent pair of characters in `text`, as packed grams.
#[cfg(test)]
fn grams(text: &str) -> Vec<u64> {
    let chars: Vec<char> = text.chars().collect();
    chars
        .windows(2)
        .map(|pair| pack_gram(pair[0], pair[1]))
        .collect()
}

/// The postings of one (gram, length-bucket) pair.
#[cfg(test)]
type Postings = Vec<u32>;

/// Postings keyed by a gram and a coarse length bucket, so a lookup only walks names whose
/// length could possibly qualify.
#[cfg(test)]
type GramIndex = HashMap<(u64, u16), Postings>;

/// The length bucket of a name: `len >> 2`, so names within four characters share a bucket.
#[cfg(test)]
fn bucket(len: usize) -> u16 {
    (len.min(u32::MAX as usize) >> 2) as u16
}

/// `_match_streams_to_channel`'s per-group start point: the streams it may have to consider.
///
/// # Why this is a sound superset
///
/// Every way the plugin can collect or gate on a stream is one of four comparisons between the
/// cleaned channel name `C` and a cleaned stream name `S`, and each has a block here whose
/// argument is that no qualifying stream can fall outside it:
///
///   * **Exact, and `_same_but_for_word_breaks`.** Both are equality once whitespace, `&` and
///     `-` are removed, which is [`Self::by_nospace`]'s key. (The exact branch is a subset:
///     two equal names always have equal no-space forms.)
///   * **Substring with a 0.75 length ratio** (`C ⊂ S` or `S ⊂ C`). If `C` is a substring of
///     `S`, every adjacent-character pair of `C` is one of `S` — grams are compared on the
///     same normalised text, so the pair is contiguous in `S` too. That is
///     [`Self::by_gram`], and its length buckets carry the 0.75 length window. `S ⊂ C` is
///     covered from the other end: `S`'s grams are `C`'s, and `S` is long enough to have one
///     unless both names are short, which is the exact case (below).
///   * **Token overlap**, which the plugin always puts through `calculate_similarity` as well
///     (directly, or through `_same_but_for_word_breaks` for the word-break form). So a
///     collected stream always scores at least the threshold, and the next point applies.
///   * **Any fuzzy score at or above the threshold**, in any of `fuzzy_match`'s three stages.
///     The ratio is `1 - distance / max(len)`, so a stream can only qualify when
///     `min(len) / max(len) >= threshold / 100` — the length window — and, by the q-gram
///     lemma with `q = 2`, two such strings share at least `(n - 1) - 2·distance` grams for
///     `n = max(len)`. At the lowest threshold the presets allow (70) and `n >= 3` that is at
///     least one, and at `n <= 2` only the exact case can reach it (a single edit costs half
///     the string). Gram comparisons on the *normalised* text cover stages 1 and 2; stage 3
///     scores the token-sorted form, so [`Self::by_gram_processed`] indexes that form as well.
///   * **The OTA callsign path**, which is a word-boundary search for one callsign: every
///     stream name carrying it as a word is in [`Self::by_word_upper`] (the case-sensitive
///     US-OTA mode uses [`Self::by_callsign_upper`], which only holds upper-case station
///     forms).
///
/// [`Self::by_token`] is not needed for the argument — the gram blocks already cover every
/// scoring path — but it is cheap, it is the index the plan names, and it keeps a reordered
/// token family (where grams of the sorted form differ from the raw name's) inside one block.
///
/// The blocks are a UNION: being generous only costs time. Every predicate is then the
/// plugin's own, applied per candidate.
#[derive(Debug, Default)]
#[cfg(test)]
pub struct NameIndex {
    /// Normalised-lower name, whitespace/`&`/`-` removed -> positions.
    by_nospace: HashMap<String, Postings>,
    /// Gram of the normalised-lower name -> positions.
    by_gram: GramIndex,
    /// Gram of the token-sorted form -> positions.
    by_gram_processed: GramIndex,
    /// Whitespace token of the normalised-lower name -> positions.
    by_token: HashMap<String, Postings>,
    /// Upper-cased word (`\w+` run) of the match name -> positions. The grouped OTA path
    /// searches case-insensitively, so the key is folded.
    by_word_upper: HashMap<String, Postings>,
    /// Base callsign of every upper-case callsign-shaped occurrence -> positions. The US-OTA
    /// mode's pattern is case-SENSITIVE (`\bWKRG(?:-[A-Z]{2}\d?)?\b`), so folding would admit
    /// names it refuses.
    by_callsign_upper: HashMap<String, Postings>,
    /// `len >> 2` of the normalised name / of the token-sorted form, per position.
    norm_bucket: Vec<u16>,
    processed_bucket: Vec<u16>,
    /// Lengths, for the exact window tests.
    norm_len: Vec<usize>,
    processed_len: Vec<usize>,
}

#[cfg(test)]
impl NameIndex {
    /// Build every block over the pool's eligible streams, reading the normalisations the
    /// candidate index already holds so each name is normalised once per run.
    fn build(
        index: &CandidateIndex,
        names: &StreamNames,
        normalizer: &Normalizer,
        tags: &[&str],
    ) -> Self {
        // The candidate index's own length: `pool.eligible` may be longer when a probe
        // verdict excluded streams from this run.
        let total = index.len();
        let mut built = Self {
            norm_bucket: Vec::with_capacity(total),
            processed_bucket: Vec::with_capacity(total),
            norm_len: Vec::with_capacity(total),
            processed_len: Vec::with_capacity(total),
            ..Self::default()
        };
        for position in 0..total {
            // The candidate index drops names that normalise to fewer than two characters
            // (every fuzzy stage skips those). The alias lookup does NOT skip them — an
            // exact alias hit may still be "Three" -> "3" — so the no-space block is built
            // from the normalisation itself, which the index has only for longer names.
            let lower = match index.normalized_lower(position) {
                Some(lower) => lower.to_string(),
                None => normalizer
                    .without_cinemax()
                    .normalize_name(&names.match_names[position], tags)
                    .to_lowercase(),
            };
            let nospace = strip_nospace(&lower);
            let norm_len = lower.chars().count();
            let processed = index
                .processed_name(position)
                .unwrap_or_default()
                .to_string();
            let processed_len = processed.chars().count();
            built.norm_len.push(norm_len);
            built.processed_len.push(processed_len);
            built.norm_bucket.push(bucket(norm_len));
            built.processed_bucket.push(bucket(processed_len));
            let position = position as u32;
            if !nospace.is_empty() {
                built.by_nospace.entry(nospace).or_default().push(position);
            }
            if norm_len >= 2 {
                let key = bucket(norm_len);
                for gram in grams(&lower) {
                    built.by_gram.entry((gram, key)).or_default().push(position);
                }
            }
            if processed_len >= 2 {
                let key = bucket(processed_len);
                for gram in grams(&processed) {
                    built
                        .by_gram_processed
                        .entry((gram, key))
                        .or_default()
                        .push(position);
                }
            }
            for token in &names.tokens[position as usize] {
                built
                    .by_token
                    .entry(token.clone())
                    .or_default()
                    .push(position);
            }
            for word in word_runs(&names.match_names[position as usize]) {
                built
                    .by_word_upper
                    .entry(word.to_uppercase())
                    .or_default()
                    .push(position);
            }
            for callsign in upper_callsigns(&names.match_names[position as usize]) {
                built
                    .by_callsign_upper
                    .entry(callsign)
                    .or_default()
                    .push(position);
            }
        }
        for postings in built.by_nospace.values_mut() {
            postings.sort_unstable();
        }
        built
    }

    /// The candidate set for one channel name, in position order.
    ///
    /// `threshold` sizes the length windows; `processed` is the channel's token-sorted form.
    fn candidates(&self, lower: &str, processed: &str, threshold: u8) -> Vec<u32> {
        let mut out: Vec<u32> = Vec::new();
        let nospace = strip_nospace(lower);
        if let Some(postings) = self.by_nospace.get(&nospace) {
            out.extend_from_slice(postings);
        }
        let t = f64::from(threshold) / 100.0;
        // The fuzzy stages' length window (ratio >= threshold), widened by the substring
        // branch's 0.75 floor.
        let norm_len = lower.chars().count();
        let (low, high) = length_window(norm_len, t.min(0.75));
        collect_blocks(&self.by_gram, &grams(lower), low, high, &mut out);
        let processed_len = processed.chars().count();
        let (low, high) = length_window(processed_len, t);
        collect_blocks(
            &self.by_gram_processed,
            &grams(processed),
            low,
            high,
            &mut out,
        );
        for token in py_split_whitespace(lower) {
            if let Some(postings) = self.by_token.get(token) {
                out.extend_from_slice(postings);
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// The positions whose word index holds `word` (the grouped OTA callsign path).
    fn word_candidates(&self, word: &str) -> Vec<u32> {
        self.by_word_upper
            .get(&word.to_uppercase())
            .cloned()
            .unwrap_or_default()
    }

    /// The positions holding at least one of `tokens`.
    ///
    /// Used to INTERSECT the candidate union: a numbered channel's family hit always carries
    /// one of its digit tokens (both the containment branch and the token branch enforce that
    /// guard), so restricting to those positions drops nothing the plugin would collect while
    /// it turns a shared brand word ("Sports 12" among 40,000 "Sports …" streams) into a
    /// handful of candidates.
    fn any_token(&self, tokens: &[&str]) -> HashSet<u32> {
        let mut out = HashSet::new();
        for token in tokens {
            if let Some(postings) = self.by_token.get(*token) {
                out.extend(postings.iter().copied());
            }
        }
        out
    }
}

/// The inclusive length window a qualifying pair's other side falls in.
///
/// `min/max >= t` rewrites to `other >= t · my_len` and `other <= my_len / t`.
#[cfg(test)]
fn length_window(my_len: usize, t: f64) -> (usize, usize) {
    if my_len == 0 {
        return (0, usize::MAX);
    }
    let low = (my_len as f64 * t).ceil() as usize;
    let high = ((my_len as f64) / t).floor() as usize;
    (low.max(1), high)
}

/// Union the postings of `grams` whose length bucket could lie in `[low, high]`.
#[cfg(test)]
fn collect_blocks(index: &GramIndex, grams: &[u64], low: usize, high: usize, out: &mut Vec<u32>) {
    if grams.is_empty() {
        return;
    }
    let first_bucket = bucket(low.max(1));
    let last_bucket = bucket(high);
    for gram in grams {
        for slot in first_bucket..=last_bucket {
            if let Some(postings) = index.get(&(*gram, slot)) {
                out.extend_from_slice(postings);
            }
        }
    }
}

/// Every `\w+` run of a name (Python's `\w` includes `_` and Unicode letters/digits).
#[cfg(test)]
fn word_runs(name: &str) -> Vec<&str> {
    let mut runs = Vec::new();
    let mut start: Option<usize> = None;
    for (offset, character) in name.char_indices() {
        let is_word = character.is_alphanumeric() || character == '_';
        match (is_word, start) {
            (true, None) => start = Some(offset),
            (false, Some(from)) => {
                runs.push(&name[from..offset]);
                start = None;
            }
            _ => {}
        }
    }
    if let Some(from) = start {
        runs.push(&name[from..]);
    }
    runs
}

/// Every upper-case callsign-shaped occurrence, as its base callsign.
///
/// `match_us_ota_only_action` searches `\bBASE(?:-[A-Z]{2}\d?)?\b` case-sensitively, so this
/// captures exactly that shape and folds the suffix away with `normalize_callsign`, which is
/// what the action compares against its station table.
fn upper_callsigns(name: &str) -> Vec<String> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"\b([KW][A-Z]{3,4}(?:-[A-Z]{2}\d?)?)\b").expect("static regex")
    });
    let mut keys: Vec<String> = Vec::new();
    for hit in re.find_iter(name) {
        let found = hit.as_str().to_uppercase();
        // The pattern a channel's callsign compiles to is `\bBASE(?:-SUFFIX)?\b`, so an
        // occurrence `BASE-SUFFIX` is reachable from base `BASE` (and the full form, for a
        // channel whose own callsign carries the suffix). Both are keyed, plus the
        // `-TV`/`-DT`-stripped form the plugin's own normalisation produces.
        if let Some((lead, _)) = found.split_once('-') {
            keys.push(lead.to_string());
        }
        keys.push(found.clone());
        let normalized = normalize_callsign(&found).to_uppercase();
        if normalized != found {
            keys.push(normalized);
        }
    }
    keys.sort_unstable();
    keys.dedup();
    keys
}

// =======================================================================================
// The collector
// =======================================================================================

/// What one channel group's collection produced.
#[derive(Debug, Clone, Default)]
struct Collected {
    /// The finalised stream ids, in the plugin's order.
    ids: Vec<i64>,
    /// The plugin's reason string, for the report.
    reason: String,
    country: CountryStats,
    /// Streams `_collect_epg_watch_streams` force-included for this group.
    watch_hits: usize,
    /// The leader's match name and country hint: the async `[ai.verify]` gate re-ranks
    /// empty groups at the floor from exactly this query (step 9).
    gate_query: String,
    gate_hint: Option<String>,
}

/// One group's AI-gate input: the leader's match name, its country hint, whether the
/// `[matching]` cut left it empty, and the channels the accept would attach to.
///
/// Recorded by [`plan`] (which already ranks every group) so the async gate in `run`
/// re-ranks only the empty groups at the `[ai.verify]` floor — the same pattern as
/// guide's step-8 review.
#[derive(Debug, Clone, Default)]
pub struct GateGroup {
    /// Position into [`MatchOutcome::gate_candidates`]' channels: the group's leader.
    pub leader_channel_id: i64,
    /// The match name the cut ranked (`matching_channel_name` of the leader).
    pub query: String,
    /// The country hint the cut ranked with (`None` when the filter is off or exempt).
    pub country_hint: Option<String>,
    /// The cut left this group with no family: the gate may promote a sub-cut family.
    pub empty: bool,
    /// The group's channel ids, in priority order: an accept attaches to all of them.
    pub member_ids: Vec<i64>,
}

/// One scoped channel's group membership.
///
/// Shared with [`crate::visibility`]: one grouping for match and visibility (step 9 keys
/// it by identity family, so both apps group the same channels together). `pub` so the
/// cross-app consistency gate reads the same groups; constructed only by [`build_groups`].
#[derive(Debug, Clone)]
pub struct Group {
    /// The grouping key, kept for diagnostics (the report names groups by their channels).
    #[allow(dead_code)]
    pub(crate) key: String,
    /// Positions in the scoped channel list, in channel order.
    ///
    /// `pub` so the cross-app consistency gate reads the same groups match and
    /// visibility plan against.
    pub members: Vec<usize>,
}

/// The result of one group: its collection and the assignments for its channels.
struct GroupResult {
    collected: Collected,
    assignments: Vec<Assignment>,
}

/// One channel's planned stream list.
#[derive(Debug, Clone)]
pub struct Assignment {
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_number: Option<f64>,
    pub channel_group: String,
    /// The channel's list before the run.
    pub before: Vec<i64>,
    /// The list the run would write, in playback order.
    pub after: Vec<i64>,
    /// Strings the group leader matched, before this channel's own zone/country handling.
    pub matched: usize,
    /// `_streams_to_assign`'s `keep_existing`: nothing may be written for this channel.
    pub keep_existing: bool,
    /// Whether `visible_channel_limit` / the zone rule selected this channel at all. A group's
    /// other channels are reported (Preview lists them) but never written.
    pub updated: bool,
    /// The zone routing gave this channel, when it gave one.
    pub zone: Option<FeedZone>,
    pub reason: String,
}

impl Assignment {
    /// Whether the write would change the channel's list.
    pub fn changed(&self) -> bool {
        self.before != self.after
    }

    /// Whether this channel is written at all.
    ///
    /// Three reasons to leave it alone, all the plugin's: the group's `visible_channel_limit`
    /// did not select it, zone routing found no stream for its zone while it already holds
    /// streams (`keep_existing`), or the group matched nothing at all — bug-063's rule that
    /// "overwrite" only replaces when there is something to replace WITH, so a zero-match run
    /// can never clear a working channel.
    pub fn will_write(&self) -> bool {
        self.updated && !self.keep_existing && self.matched > 0 && self.changed()
    }
}

/// The grouping key of one channel: the identity family key, qualified with the country
/// the ranker fixes for the channel's family.
///
/// Step 9 (D07): grouping is the [`FamilyKey`] (normalize key + zone + timeshift +
/// callsign), not SM's normalized name. Identity equality also holds country when it is
/// known (G03: `UK: CNN` and `US: CNN` are different feeds), and the collector ranks the
/// group LEADER, so every member must be a channel the leader's family is right for: the
/// key carries the family's country, `@@<code>`, exactly as [`Collector::collect`] fixes
/// it — the country filter's code when the filter is on ([`query_country`]: SM's
/// `_channel_country_code`, database and group name included, bug-158), else the name's
/// own marker (the engine's rule with no hint). Filter on or off, two channels whose
/// families would differ by country never share a group, so a leader's family is never
/// written to another country's channel, and visibility never enables one country's
/// channel to disable the other's (bug-158 review I1). A callsign-bearing identity is
/// never country-qualified (bug-161: families ignore country for callsigns). A channel
/// whose country is unknown is not qualified (fail open).
///
/// `pub(crate)` because [`crate::visibility`] groups through the same key: match and
/// visibility group the same channels together. `lineup_country` is the `scope =
/// "lineup"` hint (the lineup file's country); `None` is an ordinary run.
pub(crate) fn group_identity_key(
    channel: &Channel,
    engine: &UnifiedEngine,
    restrict_country: bool,
    database: &ChannelDatabase,
    group_name: &str,
    lineup_country: Option<&str>,
) -> String {
    let identity = resolve_with(engine, &channel.name, &ResolveContext::default());
    let FamilyKey {
        key,
        zone,
        timeshift,
        callsign,
    } = identity.family_key();
    let mut base = format!(
        "{key}|{zone:?}|{timeshift}|{}",
        callsign.as_deref().unwrap_or("")
    );
    if callsign.is_none() {
        let hint = effective_channel_country(
            engine,
            channel,
            restrict_country,
            database,
            group_name,
            lineup_country,
        );
        if let Some(country) = hint.as_deref().or(identity.country.as_deref()) {
            base.push_str(&format!("@@{country}"));
        }
    }
    base
}

/// The country hint a channel's family is ranked under: the country filter's code
/// (`_channel_country_code`, ambiguity guard included) when the filter is on, `None`
/// when it is off (the engine then reads the name's own marker). One helper for the
/// grouping key and the collector, so the two can never disagree on a channel's country.
/// Callsign exemption (bug-161) is the caller's: both callers check the identity first.
///
/// Step 19 (lineup mode): `lineup_country` is the `scope = "lineup"` hint, the lineup
/// file's country. With it, the hint is the DEFAULT channel country: a channel whose
/// own identity names a different known country keeps its own (a foreign entry in a
/// domestic lineup still matches its own country's streams), and every other channel
/// is ranked under the lineup's. `None` is an ordinary run: the database/group/name
/// code, or nothing when the filter is off.
fn effective_channel_country(
    engine: &UnifiedEngine,
    channel: &Channel,
    restrict_country: bool,
    database: &ChannelDatabase,
    group_name: &str,
    lineup_country: Option<&str>,
) -> Option<String> {
    if !restrict_country {
        return None;
    }
    let Some(hint) = lineup_country
        .map(str::trim)
        .filter(|hint| !hint.is_empty())
    else {
        return query_country(channel, restrict_country, database, group_name);
    };
    let identity = resolve_with(engine, &channel.name, &ResolveContext::default());
    if let Some(own) = identity.country.as_deref() {
        if !own.eq_ignore_ascii_case(hint) {
            return Some(own.to_uppercase());
        }
    }
    Some(hint.to_uppercase())
}

fn query_country(
    channel: &Channel,
    restrict_country: bool,
    database: &ChannelDatabase,
    group_name: &str,
) -> Option<String> {
    if !restrict_country {
        return None;
    }
    channel_country_code(channel, database, true, group_name)
}

/// Build the channel groups: one group per identity family key.
///
/// Step 9 replaces SM's `_build_channel_groups` (6820): the key is
/// [`group_identity_key`], so a channel name gets the same group in match and in
/// visibility, and the same identity it gets in guide. `normalizer`/`tags` stay in the
/// signature (unused) so the oracle harnesses keep compiling against one grouping entry
/// point; the database and group names feed the country qualifier.
///
/// `pub` (not just `pub(crate)`) so [`crate::visibility`] and the cross-app
/// consistency gate group through this same function: one grouping for match and
/// visibility. `lineup_country` is the `scope = "lineup"` hint
/// ([`MatchOptions::lineup_country`]); `None` is an ordinary run.
#[allow(clippy::too_many_arguments)]
pub fn build_groups(
    channels: &[Channel],
    database: &ChannelDatabase,
    _normalizer: &Normalizer,
    _tags: &[&str],
    restrict_country: bool,
    group_names: &HashMap<i64, String>,
    engine: &UnifiedEngine,
    lineup_country: Option<&str>,
) -> Vec<Group> {
    let mut groups: Vec<Group> = Vec::new();
    let mut positions: HashMap<String, usize> = HashMap::new();
    // One key per distinct (name, channel group): channels share names (four per group
    // in the bench world, more in real lineups), resolution is the grouping's whole
    // cost, and the country qualifier reads the channel group's name.
    let mut keys: HashMap<(&str, Option<i64>), String> = HashMap::new();
    for (position, channel) in channels.iter().enumerate() {
        let key = keys
            .entry((channel.name.as_str(), channel.channel_group_id))
            .or_insert_with(|| {
                let group_name = channel
                    .channel_group_id
                    .and_then(|id| group_names.get(&id))
                    .map(String::as_str)
                    .unwrap_or("No Group");
                group_identity_key(
                    channel,
                    engine,
                    restrict_country,
                    database,
                    group_name,
                    lineup_country,
                )
            })
            .clone();
        match positions.get(&key) {
            Some(existing) => groups[*existing].members.push(position),
            None => {
                positions.insert(key.clone(), groups.len());
                groups.push(Group {
                    key,
                    members: vec![position],
                });
            }
        }
    }
    groups
}

/// `_channel_ota_callsign` (5620): the callsign that gates OTA matching for a channel.
///
/// The database entry's own `callsign` wins when it has one; otherwise `_resolve_ota_callsign`
/// reads one out of the Dispatcharr channel name (the bug-063 fallback, which is the ONLY path
/// that resolves a callsign in production). Every site that asks "is this channel OTA" goes
/// through here, so the country filter's exemption and the callsign matcher cannot disagree.
#[cfg(test)]
fn ota_callsign(
    channel: &Channel,
    database: &ChannelDatabase,
    normalizer: &Normalizer,
    tags: &[&str],
) -> Option<String> {
    if let Some(entry) = database.info(&channel.name) {
        if let Some(callsign) = entry.callsign.as_ref().map(|value| value.trim()) {
            if !callsign.is_empty() {
                return Some(callsign.to_string());
            }
        }
    }
    resolve_ota_callsign(&channel.name, normalizer, tags)
}

/// `_resolve_ota_callsign` (5585): a paren callsign, unless it is a 3-letter word the station
/// table does not list.
#[cfg(test)]
fn resolve_ota_callsign(
    channel_name: &str,
    normalizer: &Normalizer,
    tags: &[&str],
) -> Option<String> {
    let known = crosstalk_matching::data::stream_mapparr_known_callsigns();
    let stripped = if tags.is_empty() {
        channel_name.to_string()
    } else {
        remove_ignore_tags(channel_name, normalizer, tags)
    };
    let (callsign, high_confidence) =
        extract_callsign_with_confidence_for(&stripped, &known, CallsignLadder::SharedCore);
    if let Some(callsign) = callsign {
        if is_us_station(&callsign) {
            if needs_corroboration(&callsign) && !high_confidence {
                return None;
            }
            return Some(callsign);
        }
    }
    // The paren fallback is the only path that fires against the shipped data: the paren
    // callsign of `ABC - AL Montgomery (WNCF)`.
    let callsign = extract_paren_callsign(&stripped)?;
    if fcc_rejects_callsign(&callsign) {
        return None;
    }
    Some(callsign)
}

/// `Plugin._remove_ignore_tags` (5560): a plain word is stripped on word boundaries, anything
/// else as a literal followed by `\s*`.
#[cfg(test)]
fn remove_ignore_tags(name: &str, normalizer: &Normalizer, tags: &[&str]) -> String {
    let mut out = name.to_string();
    for tag in tags {
        let escaped = regex::escape(tag);
        let pattern = if is_plain_word(tag) {
            format!(r"\b{escaped}\b")
        } else {
            format!(r"{escaped}\s*")
        };
        if let Ok(regex) = regex::RegexBuilder::new(&pattern)
            .case_insensitive(true)
            .build()
        {
            out = regex.replace_all(&out, "").into_owned();
        }
    }
    let _ = normalizer;
    out
}

/// Python's `^\w+$` for a tag.
#[cfg(test)]
fn is_plain_word(tag: &str) -> bool {
    !tag.is_empty() && tag.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// `_extract_paren_callsign` (5536): `(WKRG)` / `(WKRG-TV)`, minus the brand words.
#[cfg(test)]
fn extract_paren_callsign(name: &str) -> Option<String> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"(?i)\(([KW][A-Z]{2,3})(?:-(?:TV|CD|LP|DT|LD))?\)").expect("static regex")
    });
    let caps = re.captures(name)?;
    let callsign = caps.get(1)?.as_str().to_uppercase();
    if matches!(callsign.as_str(), "WEST" | "EAST" | "KIDS" | "WILD") {
        return None;
    }
    Some(callsign)
}

/// `_fcc_rejects_callsign` (5572): a 3-letter callsign the station table does not list.
#[cfg(test)]
fn fcc_rejects_callsign(callsign: &str) -> bool {
    callsign.chars().count() == 3 && !is_us_station(callsign)
}

/// `_callsign_needs_corroboration` (5647): the callsign is also an English word.
fn needs_corroboration(callsign: &str) -> bool {
    if callsign.is_empty() {
        return false;
    }
    let base = normalize_callsign(callsign);
    let upper = callsign.to_uppercase();
    CALLSIGN_DENYLIST.contains(&upper.as_str())
        || CALLSIGN_DENYLIST.contains(&base.to_uppercase().as_str())
}

/// `_callsign_corroborated` (5659): the stream name proves it means the station.
fn callsign_corroborated(stream_name: &str, callsign: &str) -> bool {
    if stream_name.is_empty() {
        return false;
    }
    let upper = stream_name.to_uppercase();
    let escaped = regex::escape(&callsign.to_uppercase());
    for pattern in [
        format!(r"\({escaped}\)"),
        format!(r"\b{escaped}-(?:TV|DT|CD|LP|LD)\b"),
    ] {
        if let Ok(regex) = Regex::new(&pattern) {
            if regex.is_match(&upper) {
                return true;
            }
        }
    }
    let stations = stream_mapparr_stations();
    let station = stations
        .get(&callsign.to_uppercase())
        .or_else(|| stations.get(&normalize_callsign(callsign).to_uppercase()));
    let Some(station) = station else {
        return false;
    };
    let network = station.network_affiliation.trim().to_uppercase();
    if !network.is_empty() {
        if let Ok(regex) = Regex::new(&format!(r"\b{}\b", regex::escape(&network))) {
            if regex.is_match(&upper) {
                return true;
            }
        }
    }
    let city = station.community_served_city.trim().to_uppercase();
    if !city.is_empty() && upper.contains(&city) {
        return true;
    }
    false
}

/// `re.search(r'\b' + re.escape(callsign) + r'\b', name, re.IGNORECASE)`, the grouped path's
/// predicate.
#[cfg(test)]
fn word_boundary_contains_ci(name: &str, callsign: &str) -> bool {
    let pattern = format!(r"(?i)\b{}\b", regex::escape(callsign));
    match Regex::new(&pattern) {
        Ok(regex) => regex.is_match(name),
        Err(_) => false,
    }
}

// =======================================================================================
// Everything a run resolves once
// =======================================================================================

/// The streams one group's collection may look at, plus the facts the collector needs about
/// each of them.
///
/// Built once per run: the candidate index (one normalisation per stream), the name blocks,
/// the quality keys, the zones and the countries. A group then only reads them.
struct RunData<'a> {
    pool: &'a StreamPool<'a>,
    database: ChannelDatabase,
    /// The unified engine (one ranker) and its candidate index over the eligible streams'
    /// match names: the production collection path ranks leaders against this.
    engine: Arc<UnifiedEngine>,
    engine_index: UnifiedIndex,
    /// The eligible streams' match names (EPG-resolved, regex rules applied), in pool
    /// order: what the OTA prefilter scans and what the gate reports through.
    /// Production reads only these, never the oracle names below.
    match_names: Vec<String>,
    /// The original positions each deduped engine-index name stands for, in
    /// first-occurrence order. Pools carry whole families under one name (ten streams
    /// per family in the bench world); ranking the name once and expanding to its
    /// positions scores a fraction of the candidates for the same family (same key,
    /// same name-derived country, pool order among equals).
    unique_members: Vec<Vec<usize>>,
    /// The SM oracle index over the eligible streams, built WITH the run's ignore tags so
    /// its normalisations are the ones `fuzzy_match` scores (`precompute_normalizations`).
    /// Oracle tooling only (parity tests, the indexed-vs-full-scan check): production
    /// collection ranks against `engine_index`. Absent from the binary.
    #[cfg(test)]
    index: CandidateIndex,
    #[cfg(test)]
    names: StreamNames,
    #[cfg(test)]
    name_index: NameIndex,
    /// The upper-case callsign prefilter for the US-OTA-only mode (the mode's narrow
    /// candidate rule; one map, built once per run).
    callsign_index: HashMap<String, Vec<usize>>,
    /// Eligible stream ids, in pool order: `ids[position]`.
    ids: Vec<i64>,
    /// The reverse of `ids`, so a stream id resolves to its position in one lookup.
    position_of_id: HashMap<i64, usize>,
    /// The quality sort key of each position's stream.
    keys: Vec<QualityKey>,
    /// EPG-aware placeholder matching, when the feature is on: the resolved current titles and
    /// the name overrides they allow. `None` when it is off, which is the default.
    epg: Option<&'a EpgTitles>,
    /// Positions whose RAW name is on the watched-stream list
    /// (`_collect_epg_watch_streams`), in pool order. Built once per run, because the plugin's
    /// scan of every stream per group is the shape this port exists to avoid.
    watch_positions: Vec<usize>,
    /// What the stream-side EPG pass did, for the report.
    epg_run: EpgRun,
    /// The identity zone of `stream['name']`, from the RAW name, as `_order_streams_for_zone`
    /// reads it (step 9: `extract_zone_noguard`, the one zone rule Z01/Z02/Z07).
    zones: HashMap<i64, FeedZone>,
    /// The stream's country, per position. Empty when the country filter is off.
    stream_countries: Vec<Option<String>>,
    /// Positions per stream country code, built once when the filter is on, so a group
    /// reads its same-country set from a lookup instead of scanning the pool.
    by_country: HashMap<String, Vec<usize>>,
    /// Positions carrying any country code, for the foreign/unknown counters.
    known_total: usize,
    group_names: HashMap<i64, String>,
    tags: Vec<String>,
    normalizer: Normalizer,
    /// The oracle cut the SM collector scores at (the host `[matching]` cut): read only
    /// by the oracle collection path. Absent from the binary.
    #[cfg(test)]
    threshold: u8,
}

impl<'a> RunData<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        world: &'a World,
        pool: &'a StreamPool<'a>,
        options: &MatchOptions,
        model: &QualityModel,
        throughput: &dyn ThroughputSource,
        dead: &HashSet<i64>,
        restrict_country: bool,
        epg: Option<&'a EpgTitles>,
    ) -> Self {
        let tags: Vec<String> = options.ignore_tags.clone();

        // `_filter_working_streams`: a dead stream is not a candidate, on exactly the
        // plugin's evidence — `stream_stats` saying dead (`_stream_stats_says_dead`, which is
        // what IPTV Checker writes) or a `dead` probe verdict in the store. Only when the
        // dead filter is on, as in the plugin. When every stream is dead the plugin keeps
        // the whole list rather than matching nothing, and so does this.
        let dead_stats: HashSet<i64> = if options.filter_dead_streams {
            pool.eligible
                .iter()
                .filter(|id| {
                    pool.get(**id)
                        .is_some_and(|stream| stream_stats_says_dead(&stream.stream_stats))
                })
                .copied()
                .collect()
        } else {
            HashSet::new()
        };
        let mut ids: Vec<i64> = Vec::with_capacity(pool.eligible.len());
        for id in &pool.eligible {
            if options.filter_dead_streams && (dead.contains(id) || dead_stats.contains(id)) {
                continue;
            }
            ids.push(*id);
        }
        if ids.is_empty() && !pool.eligible.is_empty() {
            ids = pool.eligible.clone();
        }

        let names: Vec<String> = ids
            .iter()
            .map(|id| pool.match_name_of(*id).unwrap_or_default().to_string())
            .collect();

        // `_apply_epg_resolution_to_streams` (3848): a placeholder-named stream's match name
        // becomes the currently airing programme title. It runs AFTER the regex rules and
        // overrides their result for placeholder streams only; the RAW name stays everywhere
        // else (ordering, quality, dedupe, the report).
        let mut names = names;
        let mut epg_run = EpgRun::default();
        if let Some(titles) = epg.filter(|titles| titles.is_enabled()) {
            for (position, id) in ids.iter().enumerate() {
                let Some(stream) = pool.get(*id) else {
                    continue;
                };
                let raw = stream.name.as_str();
                if raw.chars().count() > selection::REGEX_NAME_MAX_LEN {
                    epg_run.skipped_long += 1;
                    continue;
                }
                if !titles.settings().is_placeholder(raw) {
                    continue;
                }
                epg_run.checked += 1;
                if let Some(title) = titles.title_for_stream(stream) {
                    names[position] = title.to_string();
                    epg_run.resolved += 1;
                }
            }
        }
        // Oracle tooling only: the SM profile index, its names and its blocks (parity
        // tests, the indexed-vs-full-scan check). Production ranks `match_names`
        // against the unified index. Absent from the binary.
        #[cfg(test)]
        let (index, stream_names, name_index) = {
            let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
            let index = CandidateIndex::build_with_tags(&names, &options.matcher, &tag_refs);
            let stream_names = StreamNames::new(&names, &index);
            let name_index = NameIndex::build(
                &index,
                &stream_names,
                &options.matcher.normalizer().without_cinemax(),
                &tag_refs,
            );
            (index, stream_names, name_index)
        };

        let keys: Vec<QualityKey> = ids
            .iter()
            .map(|id| quality::key_of(&pool.quality_view(*id), model, throughput))
            .collect();

        let mut zones: HashMap<i64, FeedZone> = HashMap::with_capacity(world.streams.len());
        for stream in &world.streams {
            zones.insert(stream.id, extract_zone_noguard(&stream.name));
        }

        let group_names: HashMap<i64, String> = world
            .groups
            .iter()
            .map(|group| (group.id, group.name.clone()))
            .collect();

        let mut stream_countries: Vec<Option<String>> = Vec::new();
        if restrict_country {
            stream_countries = ids
                .iter()
                .map(|id| match pool.get(*id) {
                    Some(stream) => {
                        let group_name = stream
                            .channel_group
                            .and_then(|group| group_names.get(&group))
                            .map(String::as_str)
                            .unwrap_or("");
                        stream_country_code(stream, group_name, &options.prefix_countries)
                    }
                    None => None,
                })
                .collect();
        }

        let position_of_id: HashMap<i64, usize> = ids
            .iter()
            .enumerate()
            .map(|(position, id)| (*id, position))
            .collect();

        // The watched-stream list, resolved once: a stream whose RAW name is on it can be
        // force-included on another channel's group when its current programme contains that
        // channel's name (`_collect_epg_watch_streams`).
        let mut watch_positions: Vec<usize> = Vec::new();
        if let Some(titles) = epg.filter(|titles| titles.is_enabled()) {
            let watch_names = &titles.settings().watch_names;
            if !watch_names.is_empty() {
                for (position, id) in ids.iter().enumerate() {
                    let Some(stream) = pool.get(*id) else {
                        continue;
                    };
                    if watch_names.contains(&stream.name.trim().to_lowercase()) {
                        watch_positions.push(position);
                    }
                }
            }
        }

        let mut by_country: HashMap<String, Vec<usize>> = HashMap::new();
        let mut known_total = 0;
        if restrict_country {
            for (position, code) in stream_countries.iter().enumerate() {
                if let Some(code) = code {
                    by_country.entry(code.clone()).or_default().push(position);
                    known_total += 1;
                }
            }
        }

        // The unified candidate index over the run's match names (EPG-resolved above):
        // one normalisation per stream, shared by every group's family rank.
        // Dedup the match names for the engine index, keeping first-occurrence order:
        // pools carry whole families under one name, and every position behind one
        // name shares its key, its name-derived country and its zone. Expansion back to
        // positions is pool-ordered, which is the engine's own tie order (stable sort).
        let mut unique_names: Vec<String> = Vec::new();
        let mut unique_members: Vec<Vec<usize>> = Vec::new();
        let mut unique_of: HashMap<&str, usize> = HashMap::new();
        for (position, name) in names.iter().enumerate() {
            if let Some(&unique) = unique_of.get(name.as_str()) {
                unique_members[unique].push(position);
            } else {
                unique_of.insert(name.as_str(), unique_names.len());
                unique_names.push(name.clone());
                unique_members.push(vec![position]);
            }
        }
        let engine_index =
            UnifiedIndex::build(&unique_names, &options.engine, &ResolveContext::default());
        // The US-OTA-only mode's upper-case callsign prefilter (the mode's narrow
        // candidate rule, not the matcher): every upper-case callsign-shaped occurrence
        // keyed by its base, so a channel's callsign resolves to positions in one lookup.
        let mut callsign_index: HashMap<String, Vec<usize>> = HashMap::new();
        for (position, name) in names.iter().enumerate() {
            for callsign in upper_callsigns(name) {
                callsign_index.entry(callsign).or_default().push(position);
            }
        }

        Self {
            pool,
            database: options.database.clone(),
            engine: Arc::clone(&options.engine),
            engine_index,
            #[cfg(test)]
            index,
            match_names: names,
            #[cfg(test)]
            names: stream_names,
            #[cfg(test)]
            name_index,
            unique_members,
            callsign_index,
            ids,
            position_of_id,
            keys,
            epg,
            watch_positions,
            epg_run,
            zones,
            stream_countries,
            by_country,
            known_total,
            group_names,
            tags,
            // One normalizer: the unified engine's decided flags (step 9), not the SM
            // profile's. `clean_channel` (the watch path) and `zone_routed` read this.
            normalizer: *options.engine.normalizer(),
            #[cfg(test)]
            threshold: options.engine.threshold(),
        }
    }

    fn tag_refs(&self) -> Vec<&str> {
        self.tags.iter().map(String::as_str).collect()
    }

    /// The channel name matching should see: the plugin's local `channel_name`, which the EPG
    /// path rewrites (schedule cleanup, then the current title for a placeholder).
    fn matching_channel_name(&self, channel: &Channel) -> String {
        match self.epg {
            Some(titles) if titles.is_enabled() => titles.channel_name(channel),
            _ => channel.name.clone(),
        }
    }

    /// The group label of a channel, as `channel_group__name`.
    fn channel_group_name(&self, channel: &Channel) -> String {
        channel
            .channel_group_id
            .and_then(|id| self.group_names.get(&id))
            .cloned()
            .unwrap_or_else(|| "No Group".to_string())
    }

    /// `_clean_channel_name(channel_name, ignore_tags, ...)` for the channel side.
    ///
    /// The channel name is never normalised with `remove_cinemax` — the plugin sets that flag
    /// for the candidate side only (see `Normalizer::without_cinemax`).
    fn clean_channel(&self, name: &str) -> String {
        let tags = self.tag_refs();
        self.normalizer.normalize_name(name, &tags).to_lowercase()
    }

    /// `_clean_channel_name(name, ..., remove_cinemax=True)`, for the family comparison on a
    /// channel whose name contains `max` (the plugin cleans the CHANNEL side with the same
    /// flag it cleans the candidate side with).
    #[cfg(test)]
    fn clean_channel_cinemax(&self, name: &str) -> String {
        if !name.to_lowercase().contains("cinemax") {
            return self.clean_channel(name);
        }
        let tags = self.tag_refs();
        self.normalizer
            .with_remove_cinemax(true)
            .normalize_name(name, &tags)
            .to_lowercase()
    }

    /// The same, for a stream and a channel whose name contains `max`:
    /// `_clean_channel_name(_mname(stream), ..., remove_cinemax=channel_has_max)`.
    #[cfg(test)]
    fn clean_stream(&self, position: usize, remove_cinemax: bool) -> (String, Vec<char>) {
        if !remove_cinemax {
            let lower = self
                .index
                .normalized_lower(position)
                .unwrap_or_default()
                .to_string();
            let chars = self
                .index
                .normalized_chars(position)
                .map(<[char]>::to_vec)
                .unwrap_or_default();
            return (lower, chars);
        }
        if !self.names.has_cinemax[position] {
            // `remove_cinemax` strips the word `Cinemax` and nothing else, so a name without
            // it cleans identically: reuse the cached normalisation instead of paying for a
            // second full pass per candidate.
            let lower = self
                .index
                .normalized_lower(position)
                .unwrap_or_default()
                .to_string();
            let chars = self
                .index
                .normalized_chars(position)
                .map(<[char]>::to_vec)
                .unwrap_or_default();
            return (lower, chars);
        }
        let tags = self.tag_refs();
        let cleaned = self
            .normalizer
            .with_remove_cinemax(true)
            .normalize_name(&self.names.match_names[position], &tags)
            .to_lowercase();
        let chars = cleaned.chars().collect();
        (cleaned, chars)
    }

    /// `_filter_working_streams`'s country predicate for one position.
    fn stream_country(&self, position: usize) -> Option<&str> {
        self.stream_countries
            .get(position)
            .and_then(|code| code.as_deref())
    }
}

// =======================================================================================
// The collector: `_match_streams_to_channel`
// =======================================================================================

/// `_finalize_streams`'s dedupe key: `(name, m3u_account)` and, with
/// `allow_same_name_streams`, the URL.
type DedupKey = (String, Option<i64>, Option<String>);

/// One channel group's collection, run once for the group.
struct Collector<'a> {
    data: &'a RunData<'a>,
    options: &'a MatchOptions,
}

impl<'a> Collector<'a> {
    /// The group's answer: the leader's best identity family of streams at the one
    /// `[matching]` cut (step 9, D07: the whole family, all members in rank order),
    /// plus the EPG watched-stream force-includes when that path is on.
    ///
    /// Removed Stream-Mapparr rules, each traced in the module docs: the OTA callsign
    /// regex path (C06/D04: the engine anchor decides at family level), alias
    /// force-includes (A01/A02: the merged reverse index is inside the engine), and the
    /// fuzzy gate with the token/substring family (T03/T05/T06). EPG placeholder titles
    /// still supply the match name (`matching_channel_name`); dedupe, same-country
    /// first, zone routing and the quality sort below are unchanged.
    fn collect(&self, leader: &Channel) -> Collected {
        let data = self.data;
        let group_name = data.channel_group_name(leader);
        // The match name: the EPG path rewrites it (schedule cleanup, then the current
        // title for a placeholder), and the family rank reads it. The country filter
        // reads the channel's real name, as the plugin's `_get_channel_info_and_matches`
        // does.
        let channel_name = data.matching_channel_name(leader);
        // bug-158/bug-161: the enforced country, the same-country set and the counters —
        // one helper shared with the `[ai.verify]` accept path.
        let (channel_country, same_country, country) =
            group_country(data, self.options, leader, &group_name);
        let excluded = |position: usize| -> bool {
            match channel_country.as_deref() {
                Some(code) => {
                    matches!(
                        classify(Some(code), data.stream_country(position)),
                        Relationship::Foreign
                    )
                }
                None => false,
            }
        };

        // One ranker: the leader's best identity family at the `[matching]` cut, all
        // members in rank order (D07). The engine already applies its country rule (G03)
        // on the same hint; `excluded` keeps the run's foreign/unknown counters exact.
        // The rank runs over the DEDUPED names and expands to positions in pool order:
        // same set and order as ranking every position (same key, same name-derived
        // country, stable tie order), at a fraction of the candidates.
        let family = data.engine.rank_index_in(
            &channel_name,
            channel_country.as_deref(),
            &data.engine_index,
        );
        let top = family
            .first()
            .map(|member| (member.score, member.method.clone()));
        let mut matched: Vec<usize> = Vec::new();
        for member in &family {
            let Some(members) = data.unique_members.get(member.index) else {
                continue;
            };
            for position in members {
                if !excluded(*position) {
                    matched.push(*position);
                }
            }
        }
        // `_collect_epg_watch_streams`: the independent force-include list — a watched
        // stream keeps matching its own channel by name AND can surface here. Token
        // CONTAINMENT is the gate (kept: it is EPG data, not an SM matching rule).
        let watch = self.watch_positions(&channel_name, &excluded);
        let watch_hits = watch.len();
        let mut seen: HashSet<usize> = matched.iter().copied().collect();
        for position in watch {
            if seen.insert(position) {
                matched.push(position);
            }
        }

        if matched.is_empty() {
            return Collected {
                ids: Vec::new(),
                reason: "No identity family match".to_string(),
                country,
                watch_hits: 0,
                gate_query: channel_name,
                gate_hint: channel_country,
            };
        }
        let reason = match top {
            Some((score, method)) => {
                format!("Identity family match ({}, score: {score})", method.label())
            }
            None => "Identity family match (watch-only)".to_string(),
        };
        Collected {
            ids: self.finalize(&matched, same_country.as_ref()),
            reason,
            gate_query: channel_name,
            gate_hint: channel_country,
            country,
            watch_hits,
        }
    }

    /// The Stream-Mapparr collector (`_match_streams_to_channel`, 5808), path for path,
    /// over the indexed candidate set. Oracle tooling only: the parity tests pin the port
    /// against the plugin, and `tests::the_indexed_collector_agrees_with_a_full_scan`
    /// proves the index never changes its answer. Production collects with [`Self::collect`].
    #[cfg(test)]
    fn collect_with(&self, leader: &Channel, all: bool) -> Collected {
        let data = self.data;
        let group_name = data.channel_group_name(leader);
        // The plugin's local `channel_name`: the EPG path rewrites it (schedule cleanup, then
        // the current title for a placeholder) and every MATCHING predicate below reads it.
        // The OTA callsign and the country filter read the channel's real name, as they do
        // in the plugin (`_channel_ota_callsign` / `_get_channel_info_and_matches` take the
        // channel row, not this string).
        let channel_name = data.matching_channel_name(leader);
        let channel_has_max = channel_name.to_lowercase().contains("max");
        let callsign = ota_callsign(leader, &data.database, &data.normalizer, &data.tag_refs());

        // bug-158/bug-161: the country filter removes proven-foreign streams, is skipped for
        // OTA channels, and never reorders (the order feeds `fuzzy_match`).
        let mut country = CountryStats::default();
        let channel_country = if self.options.restrict_matching_to_country && callsign.is_none() {
            let code = channel_country_code(leader, &data.database, true, &group_name);
            if code.is_some() {
                country.engaged += 1;
            } else {
                country.skipped_unknown_channel += 1;
            }
            code
        } else {
            None
        };
        let excluded = |position: usize| -> bool {
            match channel_country.as_deref() {
                Some(code) => {
                    matches!(
                        classify(Some(code), data.stream_country(position)),
                        Relationship::Foreign
                    )
                }
                None => false,
            }
        };
        // The same-country set and the counters come from the run-once country table
        // (`by_country`), not from a scan of the pool: `classify` is exact string
        // equality, so the positions filed under the channel's code ARE the Same set, the
        // other filed positions are the Foreign ones, and the rest are Unknown.
        let same_country: Option<HashSet<i64>> = match channel_country.as_deref() {
            Some(code) => {
                let same: HashSet<i64> = data
                    .by_country
                    .get(code)
                    .map(|positions| {
                        positions
                            .iter()
                            .map(|position| data.ids[*position])
                            .collect()
                    })
                    .unwrap_or_default();
                country.foreign_dropped += data.known_total.saturating_sub(same.len());
                country.unknown_kept += data.ids.len().saturating_sub(data.known_total);
                if same.is_empty() {
                    None
                } else {
                    Some(same)
                }
            }
            None => None,
        };

        let cleaned_channel = data.clean_channel(&channel_name);
        // The family loop compares against the `remove_cinemax` cleaning of the channel name
        // (the plugin passes `channel_has_max` there too), which for every other name is the
        // same string.
        let family_channel = if channel_has_max {
            data.clean_channel_cinemax(&channel_name)
        } else {
            cleaned_channel.clone()
        };
        let channel_chars: Vec<char> = family_channel.chars().collect();
        let family_tokens: Vec<&str> = {
            let mut tokens: Vec<&str> = py_split_whitespace(&family_channel).collect();
            tokens.sort_unstable();
            tokens.dedup();
            tokens
        };
        let family_digits: Vec<&str> = family_tokens
            .iter()
            .copied()
            .filter(|token| is_digit(token))
            .collect();

        // --- The OTA callsign path, which runs FIRST and short-circuits everything else.
        if let Some(callsign) = callsign.as_deref() {
            let positions: Vec<u32> = if !all && callsign.chars().all(|c| c.is_alphanumeric()) {
                data.name_index.word_candidates(callsign)
            } else {
                // A callsign carrying a suffix (`(WKRG-DT)`) is not a word run; the plugin's
                // regex still searches for it, so this group scans (rare, and one group).
                (0..data.ids.len() as u32).collect()
            };
            let needs = needs_corroboration(callsign);
            let mut matched: Vec<usize> = Vec::new();
            for position in positions {
                let position = position as usize;
                if excluded(position) {
                    continue;
                }
                let name = &data.names.match_names[position];
                if !word_boundary_contains_ci(name, callsign) {
                    continue;
                }
                if needs && !callsign_corroborated(name, callsign) {
                    continue;
                }
                matched.push(position);
            }
            if !matched.is_empty() {
                return Collected {
                    ids: self.finalize(&matched, same_country.as_ref()),
                    reason: "Callsign match".to_string(),
                    country,
                    watch_hits: 0,
                    gate_query: channel_name.clone(),
                    gate_hint: channel_country.clone(),
                };
            }
        }

        // --- The fuzzy path. Two candidate sets, each narrowed ONLY by a condition the
        // plugin's own code makes necessary:
        //
        //   * the family can only collect a candidate that is a sub- or super-string of the
        //     channel's name (both string branches require it) carrying one of the channel's
        //     digit tokens (the number guard), so for a numbered channel `by_token` is a
        //     sound — and tiny — candidate set;
        //   * `fuzzy_match` skips every candidate that shares no designator token with the
        //     query, in all three of its stages, so the gate is soundly restricted the same
        //     way.
        //
        // When neither narrowing applies (a name with no digits at all, like "ESPN"), the
        // indexed union is used, which is sound by construction (see [`NameIndex`]). The
        // union is built LAZILY: at 20k channels it is the difference between a handful of
        // candidates per group and thousands.
        let channel_designators = crosstalk_matching::tokens::designator_tokens(&cleaned_channel);
        let union = || -> Vec<u32> {
            let mut candidates = data.name_index.candidates(
                &cleaned_channel,
                &process_string_for_matching(&cleaned_channel),
                data.threshold,
            );
            if channel_has_max {
                // The family side compares against the cinemax-cleaned name, so its blocks are
                // candidates for a family hit too.
                let mut extra = data.name_index.candidates(
                    &family_channel,
                    &process_string_for_matching(&family_channel),
                    data.threshold,
                );
                candidates.append(&mut extra);
                candidates.sort_unstable();
                candidates.dedup();
            }
            candidates
        };
        let token_set = |tokens: &[&str]| -> Vec<u32> {
            let mut positions: Vec<u32> = data.name_index.any_token(tokens).into_iter().collect();
            positions.sort_unstable();
            positions
        };
        let all_positions = || -> Vec<u32> { (0..data.ids.len() as u32).collect() };
        let designator_set: Vec<&str> = channel_designators.iter().map(String::as_str).collect();
        // Which set needs the (potentially wide) indexed union: the gate when the channel has
        // no designator to narrow by, the family when it has no digit token to narrow by (or
        // when `remove_cinemax` moves the name it compares against).
        let gate_needs_union = !all && designator_set.is_empty();
        let family_needs_union = !all && (channel_has_max || family_digits.is_empty());
        let mut union_positions: Vec<u32> = Vec::new();
        if gate_needs_union || family_needs_union {
            union_positions = union();
        }
        let gate_candidates: Vec<u32> = if all {
            all_positions()
        } else if gate_needs_union {
            union_positions.clone()
        } else {
            token_set(&designator_set)
        };
        let family_candidates: Vec<u32> = if all {
            all_positions()
        } else if family_needs_union {
            union_positions
        } else {
            token_set(&family_digits)
        };
        let gate_positions: Vec<usize> = gate_candidates
            .iter()
            .map(|position| *position as usize)
            .filter(|position| !excluded(*position))
            .collect();
        let family_positions: Vec<usize> = family_candidates
            .iter()
            .map(|position| *position as usize)
            .filter(|position| !excluded(*position))
            .collect();
        // The gate is a yes/no question ("did ANY stream reach the threshold"), so a hit in
        // the smaller family set answers it without scanning the whole union. Only when that
        // set has no hit does the sound union decide — and that is what keeps a class of
        // groups cheap at 20k channels, where the union is thousands of streams wide.
        let mut winner: Option<BestMatch> = self.options.matcher.fuzzy_match_in(
            &channel_name,
            &data.index,
            &data.tag_refs(),
            channel_has_max,
            &family_positions,
        );
        if winner.is_none() && gate_positions.len() > family_positions.len() {
            winner = self.options.matcher.fuzzy_match_in(
                &channel_name,
                &data.index,
                &data.tag_refs(),
                channel_has_max,
                &gate_positions,
            );
        }

        let aliases = self.alias_positions(&channel_name, &excluded);
        // `_collect_epg_watch_streams`: the independent force-include list, exactly like the
        // alias list — a watched stream keeps matching its own channel by name AND can
        // surface here.
        let watch = self.watch_positions(&channel_name, &excluded);
        let watch_hits = watch.len();

        if winner.is_none() && aliases.is_empty() && watch.is_empty() {
            return Collected {
                ids: Vec::new(),
                reason: "No fuzzy match".to_string(),
                country,
                watch_hits: 0,
                gate_query: channel_name,
                gate_hint: channel_country,
            };
        }

        let alias_set: HashSet<usize> = aliases.iter().chain(watch.iter()).copied().collect();
        // `list(alias_streams) + list(epg_watch_streams)`: the force-includes first, in the
        // plugin's order. A stream on both lists appears twice here and `_finalize_streams`'
        // dedupe keeps the first, exactly as the plugin's does.
        let had_aliases = !aliases.is_empty();
        let mut matched: Vec<usize> = aliases;
        matched.extend(watch.iter().copied());
        for position in &family_positions {
            if alias_set.contains(position) {
                continue;
            }
            let (stream_lower, stream_chars) = data.clean_stream(*position, channel_has_max);
            let stream_tokens: Vec<&str> = py_split_whitespace(&stream_lower).collect();
            if family_hit(
                &stream_lower,
                &stream_chars,
                &stream_tokens,
                &family_channel,
                &channel_chars,
                &family_tokens,
                &family_digits,
                data.threshold,
            ) {
                matched.push(*position);
            }
        }

        if matched.is_empty() {
            return Collected {
                ids: Vec::new(),
                reason: "No fuzzy match".to_string(),
                country,
                watch_hits,
                gate_query: channel_name,
                gate_hint: channel_country,
            };
        }
        // `_match_streams_to_channel`'s reason: an alias-only group says so, anything the
        // fuzzy stage found names the stage and its score. A group force-included ONLY by the
        // watched-stream rule keeps the plugin's own wording for that case, which is the
        // fuzzy report with no winner.
        let reason = match &winner {
            Some(winner) => format!(
                "Fuzzy match ({}, score: {})",
                winner.method.label(),
                winner.score
            ),
            None if had_aliases => "Alias match".to_string(),
            None => "Fuzzy match (None, score: 0)".to_string(),
        };
        Collected {
            ids: self.finalize(&matched, same_country.as_ref()),
            reason,
            country,
            watch_hits,
            gate_query: channel_name,
            gate_hint: channel_country,
        }
    }
}

impl<'a> Collector<'a> {
    /// `_collect_alias_streams` (4021): streams whose normalised name exactly equals one of
    /// the channel's alias variants.
    ///
    /// `alias_lookup` returns candidate NAMES; the collector then keeps every stream carrying
    /// one of them, in stream order. Looking the variants up in the no-space block reaches the
    /// same streams (an equal normalised form implies an equal no-space form) and the exact
    /// predicate below rejects anything the block over-collected.
    #[cfg(test)]
    fn alias_positions(&self, channel_name: &str, excluded: &dyn Fn(usize) -> bool) -> Vec<usize> {
        let matcher = &self.options.matcher;
        let variants = matcher.alias_variants(channel_name);
        if variants.is_empty() {
            return Vec::new();
        }
        let normalizer = matcher.normalizer().without_cinemax();
        let tags = self.data.tag_refs();
        let mut lows: HashSet<String> = HashSet::new();
        let mut nospaces: HashSet<String> = HashSet::new();
        for variant in variants {
            let normalized = normalizer.normalize_name(variant, &tags);
            if normalized.is_empty() {
                continue;
            }
            let lower = normalized.to_lowercase();
            nospaces.insert(strip_nospace(&lower));
            lows.insert(lower);
        }
        if lows.is_empty() {
            return Vec::new();
        }
        let mut positions: Vec<usize> = Vec::new();
        for nospace in &nospaces {
            if let Some(postings) = self.data.name_index.by_nospace.get(nospace) {
                positions.extend(postings.iter().map(|position| *position as usize));
            }
        }
        positions.sort_unstable();
        positions.dedup();
        // The alias collection runs on the COUNTRY-FILTERED working list in the plugin
        // (`_collect_alias_streams(channel_name, working_streams, ...)`), so a proven-foreign
        // stream is not force-included even when it is an alias hit.
        positions.retain(|position| {
            if excluded(*position) {
                return false;
            }
            // `alias_lookup`'s `_forms` fallback: the candidate cache drops names that
            // normalise to fewer than two characters, but an alias hit is exact equality,
            // so a name that cleans to "3" can still equal its alias (#68). Recompute
            // the normalisation for those, exactly as the index builder does.
            let (lower, nospace) = match self.data.index.normalized_lower(*position) {
                Some(lower) => (lower.to_string(), strip_nospace(lower)),
                None => {
                    let normalized =
                        normalizer.normalize_name(&self.data.names.match_names[*position], &tags);
                    if normalized.is_empty() {
                        return false;
                    }
                    let lower = normalized.to_lowercase();
                    let nospace = strip_nospace(&lower);
                    (lower, nospace)
                }
            };
            lows.contains(&lower) || nospaces.contains(&nospace)
        });
        positions
    }

    /// `_collect_epg_watch_streams` (4045): force-include a watched stream when its CURRENT
    /// programme title contains every cleaned token of the target channel name.
    ///
    /// The "ESPN airs a WWE SummerSlam pre-show" case: the real event never gets its own
    /// placeholder stream and only ever appears as time-boxed programming on a real,
    /// permanently-named channel. Deliberately NOT done through the `match_name` override:
    /// that would stop the watched stream matching its OWN channel.
    ///
    /// Token CONTAINMENT is the gate, not whole-string similarity: a real title is the target
    /// name plus promotional wrapping ("WWE SummerSlam Special"), which dilutes a ratio below
    /// any reasonable threshold even for a correct hit (the plugin measured 63% against a 70%
    /// threshold). A channel name that cleans to fewer than two tokens is too generic to gate
    /// on containment ("Max", "Live", "News" would swallow a watched stream's whole day), so
    /// it force-includes nothing — and the check happens BEFORE any title is resolved.
    fn watch_positions(&self, channel_name: &str, excluded: &dyn Fn(usize) -> bool) -> Vec<usize> {
        let Some(titles) = self.data.epg.filter(|titles| titles.is_enabled()) else {
            return Vec::new();
        };
        if titles.settings().watch_names.is_empty() || self.data.watch_positions.is_empty() {
            return Vec::new();
        }
        let cleaned_channel = self.data.clean_channel(channel_name);
        let channel_tokens: HashSet<&str> = cleaned_channel.split_whitespace().collect();
        if channel_tokens.len() < 2 {
            return Vec::new();
        }
        let mut hits: Vec<usize> = Vec::new();
        for position in &self.data.watch_positions {
            if excluded(*position) {
                continue;
            }
            let Some(stream) = self.data.pool.get(self.data.ids[*position]) else {
                continue;
            };
            let Some(title) = titles.title_for_stream(stream) else {
                continue;
            };
            let cleaned_title = self.data.clean_channel(title);
            let title_tokens: HashSet<&str> = cleaned_title.split_whitespace().collect();
            if channel_tokens.is_subset(&title_tokens) {
                hits.push(*position);
            }
        }
        hits
    }

    /// `_finalize_streams` (5441): quality sort, same-country first, then dedupe.
    fn finalize(&self, matched: &[usize], same_country: Option<&HashSet<i64>>) -> Vec<i64> {
        let data = self.data;
        let mut positions: Vec<usize> = matched.to_vec();
        // Python's `sorted(streams, key=...)` is stable, so ties keep the collection order.
        positions.sort_by_key(|position| data.keys[*position]);
        let mut ids: Vec<i64> = positions
            .iter()
            .map(|position| data.ids[*position])
            .collect();
        if let Some(same) = same_country {
            let (first, rest): (Vec<i64>, Vec<i64>) =
                ids.into_iter().partition(|id| same.contains(id));
            ids = first;
            ids.extend(rest);
        }
        // `_deduplicate_streams` (5391): keyed on the RAW name and the account, with the URL
        // added when the operator allows same-named streams from one source. A stream with no
        // name is never kept; the first of a key wins.
        let mut seen: HashSet<DedupKey> = HashSet::new();
        let mut deduplicated: Vec<i64> = Vec::with_capacity(ids.len());
        for id in ids {
            let Some(stream) = data.pool.get(id) else {
                continue;
            };
            if stream.name.is_empty() {
                continue;
            }
            let url = if self.options.allow_same_name_streams {
                stream.url.clone().filter(|url| !url.is_empty())
            } else {
                None
            };
            let key = (stream.name.clone(), stream.m3u_account, url);
            if seen.insert(key) {
                deduplicated.push(id);
            }
        }
        deduplicated
    }
}

/// The family collection's per-stream predicate: the loop's body in
/// `_match_streams_to_channel` (6037-6160), branch for branch.
///
/// `stream_lower`/`channel_lower` are the cleaned names lowercased, `chars` are the same
/// strings as scalar values (the similarity ratio's input). `stream_tokens`/`channel_tokens`
/// are the whitespace tokens of the two names, pre-split by the caller: this runs once per
/// candidate per group, so it allocates nothing.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn family_hit(
    stream_lower: &str,
    stream_chars: &[char],
    stream_tokens: &[&str],
    channel_lower: &str,
    channel_chars: &[char],
    channel_tokens: &[&str],
    channel_digits: &[&str],
    threshold: u8,
) -> bool {
    let stream_len = stream_lower.chars().count();
    let channel_len = channel_lower.chars().count();
    if stream_len < 2 || channel_len < 2 {
        return false;
    }
    // Exact.
    if stream_lower == channel_lower {
        return true;
    }
    let word_breaks = same_but_for_word_breaks(stream_lower, channel_lower);

    // Substring, in either direction.
    if stream_lower.contains(channel_lower) || channel_lower.contains(stream_lower) {
        // Issue #62: "nebula one event 1" is contained in "nebula one event 12", so the
        // containment branch applies the same number rule as the token path.
        if !channel_digits.is_empty()
            && !stream_tokens
                .iter()
                .filter(|token| is_digit(token))
                .any(|token| channel_digits.contains(token))
        {
            return false;
        }
        // The 75% length floor: "story" is a substring of "history" but not the same channel.
        let length_ratio = stream_len.min(channel_len) as f64 / stream_len.max(channel_len) as f64;
        if length_ratio >= 0.75 {
            let similarity = calculate_similarity_chars(
                stream_chars,
                channel_chars,
                f64::from(threshold) / 100.0,
            );
            if percentage(similarity) >= threshold || word_breaks {
                return true;
            }
        }
        return false;
    }

    // A channel carrying digits keeps its single-digit tokens, so "Premier Sports 1" requires
    // the "1"; and a channel with digits can only match a stream carrying one of the same
    // numbers.
    let channel_has_numbers = channel_lower.chars().any(|c| c.is_numeric());
    if !channel_digits.is_empty() {
        let stream_digits: Vec<&str> = stream_tokens
            .iter()
            .copied()
            .filter(|token| is_digit(token))
            .collect();
        if stream_digits.is_empty() {
            return false;
        }
        if !stream_digits
            .iter()
            .any(|token| channel_digits.contains(token))
        {
            return false;
        }
    }
    // The token sets the overlap is measured on: every token for a numbered channel, the
    // multi-character ones otherwise (`len(t) >= 1` and `len(t) > 1` in the plugin).
    let keep = |token: &str| {
        if channel_has_numbers {
            !token.is_empty()
        } else {
            token.chars().count() > 1
        }
    };
    // The plugin works on SETS of tokens, so a repeated word counts once.
    let mut stream_tokens: Vec<&str> = stream_tokens
        .iter()
        .copied()
        .filter(|token| keep(token))
        .collect();
    stream_tokens.sort_unstable();
    stream_tokens.dedup();
    let mut channel_tokens: Vec<&str> = channel_tokens
        .iter()
        .copied()
        .filter(|token| keep(token))
        .collect();
    channel_tokens.sort_unstable();
    channel_tokens.dedup();
    if stream_tokens.is_empty() || channel_tokens.is_empty() {
        return false;
    }
    let common = channel_tokens
        .iter()
        .filter(|token| stream_tokens.contains(token))
        .count();
    let overlap_ratio = common as f64 / stream_tokens.len().min(channel_tokens.len()) as f64;
    let all_channel_tokens_present = channel_tokens
        .iter()
        .all(|token| stream_tokens.contains(token))
        || word_breaks;
    // HYBRID strictness: at 90 and above every channel token must be present, below that a
    // sufficient overlap is enough. Both keep "Premier Sports 1" off "Premier Sports 2".
    let should_check_similarity = if threshold >= 90 {
        all_channel_tokens_present
    } else {
        let min_tokens_needed = stream_tokens.len().min(channel_tokens.len());
        let sufficient = common >= min_tokens_needed || overlap_ratio >= 0.75;
        sufficient || all_channel_tokens_present
    };
    if !should_check_similarity {
        return false;
    }
    let similarity =
        calculate_similarity_chars(stream_chars, channel_chars, f64::from(threshold) / 100.0);
    percentage(similarity) >= threshold || word_breaks
}

/// Python's `int(ratio * 100)`: truncating, so a 0.7 that lands at 69.999… stays below 70 —
/// which is what the plugin's own float comparison does.
#[cfg(test)]
fn percentage(ratio: f64) -> u8 {
    (ratio * 100.0) as u8
}

// =======================================================================================
// Options and the plan
// =======================================================================================

/// Everything a match run resolves from `streams.toml` and `crosstalk.toml`.
pub struct MatchOptions {
    /// The unified engine the run scores with, at the one `[matching]` cut (step 9,
    /// D02/T10 `janitor-95`). Production (`plan`, `collect`, `build_groups`) reads only
    /// this; `match_sensitivity` is retired.
    pub engine: Arc<UnifiedEngine>,
    /// The Stream-Mapparr profile matcher: oracle tooling for the parity tests and the
    /// indexed-vs-full-scan check. Production never consults it: every production
    /// predicate (grouping, collection, the cut) is the unified engine. Built only
    /// under `cfg(test)`, so the binary contains one matcher.
    #[cfg(test)]
    pub matcher: Matcher,
    /// The quality model the finalise sort uses (sort's own, shared).
    pub model: QualityModel,
    pub overwrite_streams: bool,
    pub keep_custom_streams: bool,
    pub allow_same_name_streams: bool,
    pub restrict_matching_to_country: bool,
    /// The `scope = "lineup"` hint: the lineup file's country (step 19).
    ///
    /// `None` is an ordinary run. `Some` means lineup mode is on: `run` sets it from
    /// the scope hints when `lineup_mode_country_restriction` allows, and turns
    /// `restrict_matching_to_country` on alongside it, so the grouping key, the
    /// collector and the zone routing all read the lineup default through
    /// `effective_channel_country`. Never read from another crate: lineup sets the
    /// hint on its own named-scope resolution, core carries it, streams reads it.
    pub lineup_country: Option<String>,
    pub visible_channel_limit: usize,
    /// `ota_only`: run Stream-Mapparr's **Match US OTA Only** instead of Match & Assign.
    ///
    /// The plugin ships that as a separate action, bound to a second button. It is not a
    /// scope option in disguise: it matches per CHANNEL (no name grouping, no
    /// `visible_channel_limit`), only channels whose callsign is a real US station, only
    /// upper-case callsign occurrences in stream names, and it skips the country filter and
    /// zone routing entirely. Making it a setting on `match_apply` was the plan's choice over
    /// a third action pair, because it is the same write with a narrower candidate rule.
    pub ota_only: bool,
    pub ignore_tags: Vec<String>,
    pub prefix_countries: BTreeMap<String, &'static str>,
    /// The channel databases a run resolves countries and OTA-ness from.
    pub database: ChannelDatabase,
    /// Skip streams IPTV Checker found dead. Stream-Mapparr's `filter_dead_streams`.
    pub filter_dead_streams: bool,
    /// Streams a probe verdict found dead: not candidates when the dead filter is on.
    pub dead_streams: HashSet<i64>,
    /// The EPG placeholder titles a run resolved, when `epg_placeholder_matching_enabled` is
    /// on. `None` — the default — is the plugin's default too: names match as they are written.
    pub epg: Option<Arc<EpgTitles>>,
}

impl MatchOptions {
    /// Turn lineup mode on from a step's scope hints (step 19).
    ///
    /// A `scope = "lineup"` step already reads lineup's owned channels through the id
    /// bound; what the hint adds is the country restriction, on by default. The hint is
    /// the lineup file's country and the default, not the law: a channel whose identity
    /// names a different known country keeps its own (see `effective_channel_country`).
    /// Off (`lineup_mode_country_restriction = false`), or no country hint, matches like
    /// an ordinary run. Match and visibility both call this, so a lineup step groups the
    /// same channels together in each.
    pub fn apply_scope_hints(
        &mut self,
        settings: &StreamsSettings,
        hints: &crosstalk_core::subapp::NamedScopeHints,
    ) {
        if let Some(country) = hints.country.clone() {
            if settings.lineup_mode_country_restriction {
                self.restrict_matching_to_country = true;
                self.lineup_country = Some(country);
            }
        }
    }

    /// The options `streams.toml` and the host's `[matching]` describe.
    pub fn new(settings: &StreamsSettings, matching: &MatchingConfig) -> Self {
        Self::with_database(settings, matching, None)
    }

    /// As [`Self::new`], with the channel database supplied instead of resolved from
    /// `[matching]`.
    ///
    /// The parity harness and the tests hand a fixture database over; a run resolves the
    /// enabled `*_channels.json` files once (parsing sixteen of them is start-up work, not
    /// per-case work).
    pub fn with_database(
        settings: &StreamsSettings,
        matching: &MatchingConfig,
        database: Option<ChannelDatabase>,
    ) -> Self {
        // Shared per configuration (see `UnifiedEngine::cached`): parity harnesses
        // run thousands of cases over a handful of configs, and per-case
        // construction dominated their runtime.
        let engine = UnifiedEngine::cached(&matching.engine_config());
        // Oracle tooling only (parity tests, the indexed-vs-full-scan check): the SM
        // profile matcher at the host `[matching]` cut. Production scores with
        // `engine`, above — and without this field in the binary.
        #[cfg(test)]
        let matcher = {
            let config = matching.matcher_config();
            let profile = MatchProfile::StreamMapparr;
            Matcher::new(
                profile.normalize_flags(config.flags),
                config.aliases_for(profile),
                Arc::new(config.known_callsigns_for(profile)),
                MatcherOptions {
                    match_threshold: matching.threshold(),
                    ..profile.options()
                },
            )
        };
        let database = database.unwrap_or_else(|| {
            let codes: Vec<String> = matching.databases();
            let codes: Vec<&str> = codes.iter().map(String::as_str).collect();
            ChannelDatabase::shipped(&codes)
        });
        Self {
            engine,
            #[cfg(test)]
            matcher,
            model: QualityModel::from_settings(settings),
            overwrite_streams: settings.overwrite_streams,
            keep_custom_streams: settings.keep_custom_streams,
            allow_same_name_streams: settings.allow_same_name_streams,
            restrict_matching_to_country: settings.restrict_matching_to_country,
            lineup_country: None,
            visible_channel_limit: settings.visible_channel_limit.max(1) as usize,
            ota_only: settings.ota_only,
            filter_dead_streams: settings.filter_dead_streams,
            ignore_tags: settings.ignore_tag_list(),
            prefix_countries: parse_prefix_country_overrides(&settings.stream_prefix_countries),
            database,
            dead_streams: HashSet::new(),
            epg: None,
        }
    }

    /// Carry the EPG placeholder titles this run resolved (see [`crate::epg`]).
    pub fn with_epg(mut self, titles: Arc<EpgTitles>) -> Self {
        self.epg = Some(titles);
        self
    }
}

/// The OTA-only mode's skip counters, as the plugin reports them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OtaSkipped {
    pub no_callsign: usize,
    pub not_in_database: usize,
    pub no_streams: usize,
}

/// What a match run decided.
#[derive(Debug, Clone, Default)]
pub struct MatchOutcome {
    /// In-scope channels.
    pub channels: usize,
    /// Name groups the collector ran once for (one per channel in OTA-only mode, which does
    /// not group).
    pub groups: usize,
    /// One entry per in-scope channel, in the plugin's order: the groups in channel order,
    /// then each group's channels in priority order.
    pub assignments: Vec<Assignment>,
    pub country: CountryStats,
    /// The scope in words, for the report preamble.
    pub description: String,
    /// What the stream selection did.
    pub pool: selection::PoolStats,
    /// Eligible streams a probe verdict kept out of the candidate pool.
    pub dead_filtered: usize,
    /// OTA-only mode's skips.
    pub ota: Option<OtaSkipped>,
    /// What EPG-aware placeholder matching did, when it is on.
    pub epg: Option<EpgRun>,
    /// One entry per group, in group order: the async `[ai.verify]` gate's inputs.
    /// Empty for OTA-only mode (no grouping, no gate).
    pub gate_groups: Vec<GateGroup>,
    /// The candidate match names the engine index ranked, in position order, with their
    /// stream ids: shortlist positions resolve through these.
    pub gate_candidates: Vec<(i64, String)>,
}

impl MatchOutcome {
    /// The channels a write would touch, in order.
    pub fn writes(&self) -> impl Iterator<Item = &Assignment> {
        self.assignments
            .iter()
            .filter(|assignment| assignment.will_write())
    }

    pub fn write_count(&self) -> usize {
        self.writes().count()
    }

    /// The channels the group's `visible_channel_limit` / zone rule selected AND the group
    /// matched streams for: what the plugin counts as "channels updated".
    pub fn updated_count(&self) -> usize {
        self.assignments
            .iter()
            .filter(|assignment| assignment.updated && assignment.matched > 0)
            .count()
    }

    /// Streams the writes would add or replace, over every changed list.
    pub fn streams_planned(&self) -> usize {
        self.writes().map(|assignment| assignment.after.len()).sum()
    }
}

/// Plan a match run: scope, group, collect, assign. Nothing is written.
pub fn plan(
    world: &World,
    scope: &ScopeConfig,
    settings: &StreamsSettings,
    options: &MatchOptions,
    throughput: &dyn ThroughputSource,
) -> Result<MatchOutcome> {
    world.require(WorldKind::Channels)?;
    world.require(WorldKind::Streams)?;
    let pool = selection::build(world, settings)?;
    let scoped = scope_channels(world, scope)?;
    // The quality model's clock was taken when the options were built, which is the start of
    // this run: every freshness check in the finalise sort uses that one instant.
    let model = options.model.clone();
    let mut outcome = MatchOutcome {
        channels: scoped.channels.len(),
        description: scoped.filter_description.clone(),
        pool: pool.stats.clone(),
        ..MatchOutcome::default()
    };
    if options.epg.is_some() {
        outcome.epg = Some(EpgRun::default());
    }
    outcome.dead_filtered = if options.filter_dead_streams {
        pool.eligible
            .iter()
            .filter(|id| {
                options.dead_streams.contains(id)
                    || pool
                        .get(**id)
                        .is_some_and(|stream| stream_stats_says_dead(&stream.stream_stats))
            })
            .count()
    } else {
        0
    };

    if options.ota_only {
        return Ok(ota_plan(
            world,
            &pool,
            &scoped.channels,
            options,
            &model,
            throughput,
            outcome,
        ));
    }

    let data = RunData::new(
        world,
        &pool,
        options,
        &model,
        throughput,
        &options.dead_streams,
        options.restrict_matching_to_country,
        options.epg.as_deref(),
    );
    let tag_refs = data.tag_refs();
    let groups = build_groups(
        &scoped.channels,
        &data.database,
        &data.normalizer,
        &tag_refs,
        options.restrict_matching_to_country,
        &data.group_names,
        &options.engine,
        options.lineup_country.as_deref(),
    );
    // `_zone_routed_map` runs once over every scoped channel, because a channel qualifies
    // through a SIBLING's zone.
    let zones = crate::sort::zone_routed(&scoped.channels, &data.normalizer, &data.tags);
    outcome.groups = groups.len();

    let collector = Collector {
        data: &data,
        options,
    };
    let results: Vec<GroupResult> = groups
        .par_iter()
        .map(|group| collect_group(&collector, group, &scoped.channels, &zones))
        .collect();
    for (group, result) in groups.iter().zip(results) {
        outcome.country.merge(result.collected.country);
        if let Some(epg) = outcome.epg.as_mut() {
            epg.watch_hits += result.collected.watch_hits;
        }
        let sorted = sort_channels_by_priority(group, &scoped.channels);
        outcome.gate_groups.push(GateGroup {
            leader_channel_id: scoped.channels[sorted[0]].id,
            query: result.collected.gate_query.clone(),
            country_hint: result.collected.gate_hint.clone(),
            empty: result.collected.ids.is_empty(),
            member_ids: sorted
                .iter()
                .map(|position| scoped.channels[*position].id)
                .collect(),
        });
        outcome.assignments.extend(result.assignments);
    }
    outcome.gate_candidates = data
        .ids
        .iter()
        .zip(data.match_names.iter())
        .map(|(id, name)| (*id, name.clone()))
        .collect();
    // The stream-side pass ran once, in `RunData::new`, before the groups.
    if let Some(epg) = outcome.epg.as_mut() {
        epg.checked = data.epg_run.checked;
        epg.resolved = data.epg_run.resolved;
        epg.skipped_long = data.epg_run.skipped_long;
    }
    Ok(outcome)
}

/// One group's work: collect once for the leader, then assign to the channels it covers.
fn collect_group(
    collector: &Collector<'_>,
    group: &Group,
    channels: &[Channel],
    zones: &HashMap<i64, FeedZone>,
) -> GroupResult {
    let sorted = sort_channels_by_priority(group, channels);
    let leader = &channels[sorted[0]];
    let collected = collector.collect(leader);
    assign_group(collector, group, &sorted, channels, zones, &collected)
}

/// Assign one group's collected streams to its channels: `visible_channel_limit` / zone
/// siblings select the channels, then per-channel zone routing, the country partition and
/// the overwrite-or-append list.
///
/// Split from [`collect_group`] so the `[ai.verify]` gate reuses the same assignment for
/// a model-promoted family: only the collection differs (floor-ranked, model-accepted).
fn assign_group(
    collector: &Collector<'_>,
    _group: &Group,
    sorted: &[usize],
    channels: &[Channel],
    zones: &HashMap<i64, FeedZone>,
    collected: &Collected,
) -> GroupResult {
    let to_update = channels_to_update(
        sorted,
        channels,
        collector.options.visible_channel_limit,
        zones,
    );
    let mut assignments: Vec<Assignment> = Vec::with_capacity(sorted.len());
    for channel_position in sorted.iter() {
        let channel = &channels[*channel_position];
        // `to_update` holds channel POSITIONS (values of `sorted`), not indices into it.
        let updated = to_update.contains(channel_position);
        let mut assignment = Assignment {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_number: channel.display_number(),
            channel_group: collector.data.channel_group_name(channel),
            before: channel.streams.clone(),
            after: channel.streams.clone(),
            matched: collected.ids.len(),
            keep_existing: !updated,
            updated,
            zone: zones.get(&channel.id).copied(),
            reason: collected.reason.clone(),
        };
        if updated && !collected.ids.is_empty() {
            let same_country = same_country_ids_for(collector, channel, &collected.ids);
            let (streams, keep_existing) = streams_to_assign(
                collector.data,
                &collected.ids,
                channel,
                assignment.zone,
                same_country.as_ref(),
            );
            assignment.keep_existing = keep_existing;
            assignment.after = if keep_existing {
                channel.streams.clone()
            } else {
                write_list(collector.options, collector.data, channel, &streams)
            };
        }
        assignments.push(assignment);
    }
    GroupResult {
        collected: collected.clone(),
        assignments,
    }
}

/// The shortlist adapter: the gate speaks the profile `Scored` shape, while the unified
/// engine ranks families of [`EngineRanked`]. The score and the order carry over untouched,
/// and the engine method maps onto the coarse components (the same adapter as guide's
/// `to_scored`: the walk, the gate and the report share one shape).
fn to_scored(shortlist: &[EngineRanked]) -> Vec<Scored> {
    use crosstalk_matching::engine::EngineMethod;
    shortlist
        .iter()
        .map(|member| {
            let components = match member.method {
                EngineMethod::Alias => vec!["Alias"],
                EngineMethod::Exact => vec!["Exact"],
                EngineMethod::Substring => vec!["Substring"],
                EngineMethod::Fuzzy(_) => vec!["Fuzzy"],
                EngineMethod::Callsign => vec!["Callsign"],
            };
            Scored {
                index: member.index,
                score: member.score,
                components,
            }
        })
        .collect()
}

/// The `[ai.verify]` gate for borderline groups (step 9, task "streams"): a group with
/// no family at the `[matching]` cut is re-ranked by the same engine and index at the
/// gate's floor ([`RunEngine::shortlist_engine`]: [`UnifiedEngine::with_threshold`] +
/// [`UnifiedEngine::shortlist_index_in`], guide's step-8 pattern, threaded with the
/// group's country hint so the gate applies the same country rule as the cut), and the
/// verify-band / rescue candidates go to the chain with task `"streams"`. Only a model
/// accept attaches streams — the accepted candidate's identity family at the floor —
/// through the same finalize + assign path as the cut ([`assign_group`]).
///
/// A no-op unless the gate can fire and at least one group came back empty: with the
/// default (off) policy this costs nothing, and [`plan`] alone never consults a model.
#[allow(clippy::too_many_arguments)]
async fn apply_ai_gate(
    outcome: &mut MatchOutcome,
    world: &World,
    scoped: &[Channel],
    settings: &StreamsSettings,
    options: &Arc<MatchOptions>,
    throughput: &Arc<dyn ThroughputSource>,
    context: &JobContext,
    ai: Option<&crosstalk_core::ai::AiService>,
) -> Result<()> {
    let ai_settings: &AiVerifyConfig = &context.config().ai.verify;
    // The run's own engine (built once from `[matching]` in `MatchOptions`), re-cut at
    // the gate's floor: the same ranker, the same data, no second load.
    let run_engine = RunEngine::with_engine(Arc::clone(&options.engine), None);
    let Some(short_engine) = run_engine.shortlist_engine(ai_settings) else {
        return Ok(());
    };
    let empty: Vec<usize> = outcome
        .gate_groups
        .iter()
        .enumerate()
        .filter(|(_, group)| group.empty)
        .map(|(ordinal, _)| ordinal)
        .collect();
    if empty.is_empty() {
        return Ok(());
    }
    // Position-aligned candidates for the gate: every engine-index position's stream, so
    // `Scored.index` addresses the candidate slice (the same contract guide's walk uses).
    // A dangling id skips the gate, never the run.
    let by_id: HashMap<i64, &Stream> = world
        .streams
        .iter()
        .map(|stream| (stream.id, stream))
        .collect();
    let mut candidates: Vec<Stream> = Vec::with_capacity(outcome.gate_candidates.len());
    for (id, _) in &outcome.gate_candidates {
        let Some(stream) = by_id.get(id).copied() else {
            return Ok(());
        };
        candidates.push(stream.clone());
    }
    let candidate_names: Vec<String> = outcome
        .gate_candidates
        .iter()
        .map(|(_, name)| name.clone())
        .collect();
    let index = Arc::new(UnifiedIndex::build(
        &candidate_names,
        &options.engine,
        &ResolveContext::default(),
    ));
    // The same ranker at the floor, with each group's own country hint: rayon off the
    // async runtime, group order preserved. (`match_run::score_shortlists` is guide's
    // hintless convenience wrapper; streams groups carry hints, so the hint threads
    // through here to keep the gate's country rule identical to the cut's.)
    let queries: Vec<(String, Option<String>)> = empty
        .iter()
        .map(|&ordinal| {
            let group = &outcome.gate_groups[ordinal];
            (group.query.clone(), group.country_hint.clone())
        })
        .collect();
    let shortlists: Vec<Vec<EngineRanked>> = tokio::task::spawn_blocking({
        let short_engine = Arc::clone(&short_engine);
        let index = Arc::clone(&index);
        move || {
            queries
                .par_iter()
                .map(|(query, hint)| {
                    short_engine.shortlist_index_in(query, hint.as_deref(), &index)
                })
                .collect()
        }
    })
    .await
    .context("the AI shortlist stage panicked")?;
    // The gate + ask, one group at a time: borderline groups are rare, and a cancel
    // lands between groups rather than after the whole scope.
    let known = run_engine.engine().known().clone();
    // The audit row is the REPORT row the answer labels: the leader's assignment (the
    // CSV writes one row per assignment, in `assignments` order), as guide records the
    // channel ordinal of its one-row-per-channel report.
    let report_row: HashMap<i64, usize> = outcome
        .assignments
        .iter()
        .enumerate()
        .map(|(row, assignment)| (assignment.channel_id, row))
        .collect();
    let mut accepts: Vec<(usize, usize)> = Vec::new();
    for (position, &ordinal) in empty.iter().enumerate() {
        if context.is_cancelled() {
            anyhow::bail!("cancelled by the operator");
        }
        let group = &outcome.gate_groups[ordinal];
        let Some(&row) = report_row.get(&group.leader_channel_id) else {
            continue;
        };
        let scored = to_scored(&shortlists[position]);
        let Some((reason, offered)) = ai_gate::shortlist(&scored, &candidates, ai_settings) else {
            continue;
        };
        let Some(leader) = scoped
            .iter()
            .find(|channel| channel.id == group.leader_channel_id)
        else {
            continue;
        };
        let gate_outcome = ai_gate::ask(
            ai,
            leader,
            &candidates,
            &scored,
            &offered,
            reason,
            &known,
            ai_settings,
            || context.is_cancelled(),
            // The task prefix changes a chat model's prompt and the audited request, so
            // only a configured chain gets it (guide's rule): shorthand installs ask
            // exactly what they asked before the chain existed.
            (!ai_settings.chain.is_empty()).then_some("streams"),
        )
        .await;
        context.record_ai_decision(&gate_outcome.to_store(row as i64, &ai_settings.provider))?;
        if gate_outcome.answered {
            if let Some(accepted) = gate_outcome.accepted {
                accepts.push((ordinal, accepted));
            }
        }
    }
    if accepts.is_empty() {
        return Ok(());
    }
    // The accepted candidate's identity family at the floor: the whole family promotes,
    // never a lone stream (D07). Family keys read off the run's own index.
    let mut families: Vec<(usize, Vec<i64>, String)> = Vec::with_capacity(accepts.len());
    for &(ordinal, accepted) in &accepts {
        let position = empty.iter().position(|&o| o == ordinal);
        let Some(shortlist) = position.and_then(|position| shortlists.get(position)) else {
            continue;
        };
        let Some(accepted_member) = shortlist.get(accepted) else {
            continue;
        };
        let key = index.family_key(accepted_member.index).clone();
        let mut family: Vec<i64> = Vec::new();
        for member in shortlist {
            if *index.family_key(member.index) == key {
                if let Some((id, _)) = outcome.gate_candidates.get(member.index) {
                    family.push(*id);
                }
            }
        }
        families.push((
            ordinal,
            family,
            format!(
                "AI-accepted {} (score: {})",
                accepted_member.method.label(),
                accepted_member.score
            ),
        ));
    }
    if families.is_empty() {
        return Ok(());
    }
    // Apply every accept through the same finalize + assign path as the cut, inside one
    // blocking section: the run's own data, zones and groups, rebuilt deterministically
    // from the same inputs `plan` used.
    let leaders: Vec<(usize, i64)> = families
        .iter()
        .map(|(ordinal, _, _)| (*ordinal, outcome.gate_groups[*ordinal].leader_channel_id))
        .collect();
    let outcome_assignments = tokio::task::spawn_blocking({
        let options = Arc::clone(options);
        let throughput: Arc<dyn ThroughputSource> = Arc::clone(throughput);
        let world = world.clone();
        let settings = settings.clone();
        let scoped = scoped.to_vec();
        move || -> Result<Vec<(usize, GroupResult)>> {
            let pool = selection::build(&world, &settings)?;
            let data = RunData::new(
                &world,
                &pool,
                &options,
                &options.model,
                throughput.as_ref(),
                &options.dead_streams,
                options.restrict_matching_to_country,
                options.epg.as_deref(),
            );
            let tag_refs = data.tag_refs();
            let groups = build_groups(
                &scoped,
                &data.database,
                &data.normalizer,
                &tag_refs,
                options.restrict_matching_to_country,
                &data.group_names,
                &options.engine,
                options.lineup_country.as_deref(),
            );
            let zones = crate::sort::zone_routed(&scoped, &data.normalizer, &data.tags);
            let by_leader: HashMap<i64, &Group> = groups
                .iter()
                .map(|group| {
                    (
                        scoped[sort_channels_by_priority(group, &scoped)[0]].id,
                        group,
                    )
                })
                .collect();
            let collector = Collector {
                data: &data,
                options: &options,
            };
            let mut applied: Vec<(usize, GroupResult)> = Vec::new();
            for ((ordinal, leader_id), (_, family_ids, reason)) in
                leaders.iter().zip(families.iter())
            {
                let Some(group) = by_leader.get(leader_id).copied() else {
                    continue;
                };
                let sorted = sort_channels_by_priority(group, &scoped);
                let leader = &scoped[sorted[0]];
                let group_name = data.channel_group_name(leader);
                // The group's country stats were counted when `plan` collected it; only
                // the enforced country and the same-country set are read here.
                let (channel_country, same_country, _) =
                    group_country(&data, &options, leader, &group_name);
                // The cut path's exclusion, exactly: a stream the filter proves foreign
                // is never attached, whoever accepted its family.
                let positions: Vec<usize> = family_ids
                    .iter()
                    .filter_map(|id| data.position_of_id.get(id).copied())
                    .filter(|position| {
                        !channel_country.as_deref().is_some_and(|code| {
                            matches!(
                                classify(Some(code), data.stream_country(*position)),
                                Relationship::Foreign
                            )
                        })
                    })
                    .collect();
                let collected = Collected {
                    ids: collector.finalize(&positions, same_country.as_ref()),
                    reason: reason.clone(),
                    country: CountryStats::default(),
                    watch_hits: 0,
                    gate_query: String::new(),
                    gate_hint: None,
                };
                let result = assign_group(&collector, group, &sorted, &scoped, &zones, &collected);
                applied.push((*ordinal, result));
            }
            Ok(applied)
        }
    })
    .await??;
    // Splice the accepts back into the outcome, in place: the report and the write read
    // `assignments`, so a promoted family flows through the same write list as the cut.
    for (ordinal, result) in outcome_assignments {
        let fresh: HashMap<i64, Assignment> = result
            .assignments
            .into_iter()
            .map(|assignment| (assignment.channel_id, assignment))
            .collect();
        for assignment in outcome.assignments.iter_mut() {
            if let Some(replacement) = fresh.get(&assignment.channel_id) {
                *assignment = replacement.clone();
            }
        }
        if let Some(group) = outcome.gate_groups.get_mut(ordinal) {
            group.empty = false;
        }
    }
    Ok(())
}

/// The group-level country rule for `leader`: the filter's enforced country (with the
/// bug-161 callsign exemption) plus the run-once same-country set and counters.
///
/// Shared by [`Collector::collect`] and the `[ai.verify]` accept path, so a promoted
/// family partitions and counts exactly like a cut family.
fn group_country(
    data: &RunData<'_>,
    options: &MatchOptions,
    leader: &Channel,
    group_name: &str,
) -> (Option<String>, Option<HashSet<i64>>, CountryStats) {
    // bug-161: a callsign-bearing identity is never country-qualified, and the filter
    // is skipped for it. The identity callsign (engine anchor, C06/D04) replaces the SM
    // callsign regex path.
    let identity = resolve_with(&data.engine, &leader.name, &ResolveContext::default());
    let mut country = CountryStats::default();
    if !(options.restrict_matching_to_country && identity.callsign.is_none()) {
        return (None, None, country);
    }
    // The same helper the grouping key reads, so a group's key and its leader's hint
    // name one country — lineup mode included (see `effective_channel_country`): the
    // key qualifies with the lineup default and the collector enforces it.
    let code = effective_channel_country(
        &data.engine,
        leader,
        options.restrict_matching_to_country,
        &data.database,
        group_name,
        options.lineup_country.as_deref(),
    );
    if code.is_some() {
        country.engaged += 1;
    } else {
        country.skipped_unknown_channel += 1;
    }
    let same_country: Option<HashSet<i64>> = match code.as_deref() {
        Some(code) => {
            let same: HashSet<i64> = data
                .by_country
                .get(code)
                .map(|positions| {
                    positions
                        .iter()
                        .map(|position| data.ids[*position])
                        .collect()
                })
                .unwrap_or_default();
            country.foreign_dropped += data.known_total.saturating_sub(same.len());
            country.unknown_kept += data.ids.len().saturating_sub(data.known_total);
            if same.is_empty() {
                None
            } else {
                Some(same)
            }
        }
        None => None,
    };
    (code, same_country, country)
}

/// `Plugin._sort_channels_by_priority` (8217): quality tag first, then channel number.
///
/// Returns positions into `channels`, stable, so ties keep world order (as Python's `sorted`
/// does). [`crate::visibility`] reads the same priority order, which is what makes its
/// "first eligible channel of the group" agree with the group leader match uses.
pub(crate) fn sort_channels_by_priority(group: &Group, channels: &[Channel]) -> Vec<usize> {
    let mut sorted = group.members.clone();
    // `sorted(channels, key=(quality_index, channel_number))`: stable, so two channels equal
    // on both keys keep world order (Python's `sorted` and `slice::sort_by` agree).
    sorted.sort_by(|left, right| {
        let key = |position: &usize| {
            let channel = &channels[*position];
            (
                channel_quality_index(&channel.name),
                channel.display_number().unwrap_or(999_999.0),
            )
        };
        let (left_quality, left_number) = key(left);
        let (right_quality, right_number) = key(right);
        left_quality.cmp(&right_quality).then_with(|| {
            left_number
                .partial_cmp(&right_number)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    sorted
}

/// `PluginConfig.CHANNEL_QUALITY_TAG_ORDER`, and `_extract_channel_quality_tag`'s reading of
/// it: the first tag present as a plain substring wins, and the empty tag means "none of the
/// others is there".
const CHANNEL_QUALITY_TAGS: [&str; 7] = [
    "[4K]",
    "[UHD]",
    "[FHD]",
    "[HD]",
    "[SD]",
    "[Unknown]",
    "[Slow]",
];

fn channel_quality_index(name: &str) -> usize {
    for (index, tag) in CHANNEL_QUALITY_TAGS.iter().enumerate() {
        if name.contains(tag) {
            return index;
        }
    }
    CHANNEL_QUALITY_TAGS.len()
}

/// `_channels_to_update_for_group` (4266): the top `visible_channel_limit`, plus the top
/// channel of any zone-routed sibling whose feed zone is not represented yet — zone siblings
/// are distinct feeds, not the duplicates the limit is meant to cap.
///
/// Returns positions into `sorted` (which is what the caller iterates), not channel ids.
fn channels_to_update(
    sorted: &[usize],
    channels: &[Channel],
    limit: usize,
    zones: &HashMap<i64, FeedZone>,
) -> Vec<usize> {
    let mut chosen: Vec<usize> = sorted.iter().take(limit).copied().collect();
    if zones.is_empty() {
        return chosen;
    }
    let mut covered: HashSet<FeedZone> = chosen
        .iter()
        .filter_map(|position| zones.get(&channels[*position].id).copied())
        .collect();
    for position in sorted.iter().skip(limit) {
        let Some(zone) = zones.get(&channels[*position].id).copied() else {
            continue;
        };
        if covered.insert(zone) {
            chosen.push(*position);
        }
    }
    chosen
}

/// `_own_zone` / `_order_streams_for_zone` (4282) with `keep_all_if_empty` selectable.
///
/// Drops the other feed zone outright rather than ranking it last, keeps quality order inside
/// a zone, and — when the country filter engaged — makes the country tier the OUTER key so a
/// proven same-country West feed still beats an unproven generic one.
fn order_streams_for_zone(
    ids: &[i64],
    channel_zone: FeedZone,
    same_country: Option<&HashSet<i64>>,
    keep_all_if_empty: bool,
    zones: &HashMap<i64, FeedZone>,
) -> (Vec<i64>, bool) {
    if ids.is_empty() {
        return (Vec::new(), false);
    }
    let stream_zone = |id: i64| zones.get(&id).copied().unwrap_or(FeedZone::Default);
    let own: Vec<i64> = ids
        .iter()
        .copied()
        .filter(|id| {
            if channel_zone == FeedZone::West {
                stream_zone(*id) == FeedZone::West
            } else {
                stream_zone(*id) != FeedZone::West
            }
        })
        .collect();
    let mut kept_other_zone = false;
    let kept: Vec<i64> = if own.is_empty() {
        if !keep_all_if_empty {
            return (Vec::new(), false);
        }
        // A caller that keeps the other zone says so out loud (the plugin logs a warning per
        // channel; the run counts them instead).
        kept_other_zone = true;
        ids.to_vec()
    } else {
        own
    };
    let same = same_country.cloned().unwrap_or_default();
    let mut ranked: Vec<i64> = kept;
    ranked.sort_by_key(|id| {
        (
            u8::from(!same.contains(id)),
            crate::sort::zone_affinity_rank(channel_zone, stream_zone(*id)),
        )
    });
    (ranked, kept_other_zone)
}

/// `_streams_for_channel` (4339): the country partition, and zone routing when it applies.
fn streams_for_channel(
    ids: &[i64],
    channel_zone: Option<FeedZone>,
    same_country: Option<&HashSet<i64>>,
    keep_all_if_empty: bool,
    zones: &HashMap<i64, FeedZone>,
) -> Vec<i64> {
    if let Some(zone) = channel_zone {
        return order_streams_for_zone(ids, zone, same_country, keep_all_if_empty, zones).0;
    }
    match same_country {
        Some(same) if !same.is_empty() => {
            let (first, rest): (Vec<i64>, Vec<i64>) = ids.iter().partition(|id| same.contains(id));
            let mut ordered = first;
            ordered.extend(rest);
            ordered
        }
        _ => ids.to_vec(),
    }
}

/// `_streams_to_assign` (4364): what one channel gets, and whether it is left alone.
///
/// Issue 55: when zone filtering leaves a channel no stream of its own zone, the other-zone
/// streams are assigned ONLY to a channel holding no streams at all — a channel that already
/// holds streams is untouched, in both overwrite modes.
fn streams_to_assign(
    data: &RunData<'_>,
    matched: &[i64],
    channel: &Channel,
    zone: Option<FeedZone>,
    same_country: Option<&HashSet<i64>>,
) -> (Vec<i64>, bool) {
    let streams = streams_for_channel(matched, zone, same_country, false, &data.zones);
    if !streams.is_empty() || matched.is_empty() {
        return (streams, false);
    }
    if !channel.streams.is_empty() {
        return (Vec::new(), true);
    }
    (
        streams_for_channel(matched, zone, same_country, true, &data.zones),
        false,
    )
}

/// `_write_channel_streams` (9764): the list the channel ends with.
///
/// Overwrite replaces the list with the matches, re-appending the channel's existing CUSTOM
/// streams in their old order (issue 70); append keeps the existing list and adds only the
/// streams the channel does not already hold.
fn write_list(
    options: &MatchOptions,
    data: &RunData<'_>,
    channel: &Channel,
    streams: &[i64],
) -> Vec<i64> {
    if options.overwrite_streams {
        let custom_tail: Vec<i64> = channel
            .streams
            .iter()
            .copied()
            .filter(|id| data.pool.get(*id).is_some_and(|stream| stream.is_custom))
            .collect();
        if custom_tail.is_empty() || !options.keep_custom_streams {
            return streams.to_vec();
        }
        let tail: HashSet<i64> = custom_tail.iter().copied().collect();
        let mut ordered: Vec<i64> = streams
            .iter()
            .copied()
            .filter(|id| !tail.contains(id))
            .collect();
        ordered.extend(custom_tail);
        return ordered;
    }
    let existing: HashSet<i64> = channel.streams.iter().copied().collect();
    let mut ordered = channel.streams.clone();
    ordered.extend(streams.iter().copied().filter(|id| !existing.contains(id)));
    ordered
}

/// `_same_country_ids_for` (4397): the matched streams proven to share THIS channel's country.
fn same_country_ids_for(
    collector: &Collector<'_>,
    channel: &Channel,
    matched: &[i64],
) -> Option<HashSet<i64>> {
    if !collector.options.restrict_matching_to_country {
        return None;
    }
    let data = collector.data;
    // An OTA channel is exempt from the filter (bug-161), so nothing here may promote a
    // stream on a country code that is not being enforced. The identity callsign (engine
    // anchor, C06/D04) replaces the SM callsign regex path.
    if resolve_with(&data.engine, &channel.name, &ResolveContext::default())
        .callsign
        .is_some()
    {
        return None;
    }
    let group_name = data.channel_group_name(channel);
    let code = effective_channel_country(
        &data.engine,
        channel,
        collector.options.restrict_matching_to_country,
        &data.database,
        &group_name,
        collector.options.lineup_country.as_deref(),
    )?;
    let mut same: HashSet<i64> = HashSet::new();
    for id in matched {
        let Some(position) = data.position_of_id.get(id).copied() else {
            continue;
        };
        if matches!(
            classify(Some(&code), data.stream_country(position)),
            Relationship::Same
        ) {
            same.insert(*id);
        }
    }
    if same.is_empty() {
        None
    } else {
        Some(same)
    }
}

// =======================================================================================
// Match US OTA Only: `match_us_ota_only_action` (9156)
// =======================================================================================

/// The US-OTA-only mode: per channel, no grouping, upper-case callsigns only.
///
/// It is the same write as Match & Assign with a narrower candidate rule — a channel must
/// resolve to a callsign the FCC station table lists, and only an UPPER-CASE occurrence of
/// that callsign (optionally suffixed `-DT2`) in a stream name counts. No country filter, no
/// zone routing, no `visible_channel_limit`: the plugin matches every scoped channel.
fn ota_plan(
    world: &World,
    pool: &StreamPool<'_>,
    channels: &[Channel],
    options: &MatchOptions,
    model: &QualityModel,
    throughput: &dyn ThroughputSource,
    mut outcome: MatchOutcome,
) -> MatchOutcome {
    let data = RunData::new(
        world,
        pool,
        options,
        model,
        throughput,
        &options.dead_streams,
        false,
        options.epg.as_deref(),
    );
    if options.epg.is_some() {
        outcome.epg = Some(EpgRun {
            checked: data.epg_run.checked,
            resolved: data.epg_run.resolved,
            skipped_long: data.epg_run.skipped_long,
            watch_hits: 0,
        });
    }
    let known = crosstalk_matching::data::stream_mapparr_known_callsigns();
    let mut skipped = OtaSkipped::default();
    let mut assignments: Vec<Assignment> = Vec::with_capacity(channels.len());
    outcome.groups = channels.len();
    for channel in channels {
        let mut assignment = Assignment {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_number: channel.display_number(),
            channel_group: data.channel_group_name(channel),
            before: channel.streams.clone(),
            after: channel.streams.clone(),
            matched: 0,
            keep_existing: false,
            updated: false,
            zone: None,
            reason: String::new(),
        };
        // `extract_callsign` on the channel name, then the US database's base callsigns.
        let callsign =
            extract_callsign_with_confidence_for(&channel.name, &known, CallsignLadder::SharedCore)
                .0;
        let Some(callsign) = callsign else {
            skipped.no_callsign += 1;
            assignment.reason = "no US callsign in the channel name".to_string();
            assignments.push(assignment);
            continue;
        };
        let base = normalize_callsign(&callsign).to_uppercase();
        if !is_us_station(&base) {
            skipped.not_in_database += 1;
            assignment.reason = format!("callsign {base} is not a US station the table lists");
            assignments.push(assignment);
            continue;
        }
        // `\bBASE(?:-[A-Z]{2}\d?)?\b`, case-SENSITIVE, over `_mname(stream)`.
        let pattern = match Regex::new(&format!(r"\b{}(?:-[A-Z]{{2}}\d?)?\b", regex::escape(&base)))
        {
            Ok(pattern) => pattern,
            Err(_) => {
                skipped.no_streams += 1;
                assignment.reason = format!("callsign {base} is not a usable pattern");
                assignments.push(assignment);
                continue;
            }
        };
        let needs = needs_corroboration(&base);
        let mut matched: Vec<usize> = Vec::new();
        // The run-once upper-case callsign prefilter (the mode's narrow candidate rule,
        // not the matcher): case-sensitive, like the plugin's own pattern.
        let empty: Vec<usize> = Vec::new();
        for position in data.callsign_index.get(&base).unwrap_or(&empty) {
            let position = *position;
            let name = &data.match_names[position];
            if !pattern.is_match(name) {
                continue;
            }
            if needs && !callsign_corroborated(name, &base) {
                continue;
            }
            matched.push(position);
        }
        if matched.is_empty() {
            skipped.no_streams += 1;
            assignment.reason = format!("no stream carries the upper-case callsign {base}");
            assignments.push(assignment);
            continue;
        }
        // `_sort_streams_by_quality` then `_deduplicate_streams`: no country partition and no
        // zone routing in this action.
        let collector = Collector {
            data: &data,
            options,
        };
        // `_sort_streams_by_quality` then `_deduplicate_streams`: no country partition and no
        // zone routing in this action.
        let matched_ids = collector.finalize(&matched, None);
        assignment.after = write_list(options, &data, channel, &matched_ids);
        assignment.matched = matched_ids.len();
        assignment.updated = true;
        assignment.reason = format!("US OTA callsign: {base}");
        assignments.push(assignment);
    }
    outcome.assignments = assignments;
    outcome.ota = Some(skipped);
    outcome
}

// =======================================================================================
// The action
// =======================================================================================

/// The report's title, without the preview/applied suffix.
const REPORT_TITLE: &str = "Match & Assign Streams";

/// The preview CSV's columns: Stream-Mapparr's own six, then what crosstalk decided.
///
/// `will_update`, `threshold`, `channel_id`, `channel_name`, `matched_streams` and
/// `stream_names` are the plugin's preview columns (8232's DictWriter), so an operator's
/// existing reading habits and any downstream parser keep working. `before`/`after` are the
/// channel's stream lists as crosstalk plans them, `status` says whether the row was written,
/// and `reason` carries the plugin's match reason.
const MATCH_COLUMNS: [&str; 11] = [
    "will_update",
    "threshold",
    "channel_id",
    "channel_name",
    "matched_streams",
    "stream_names",
    "group",
    "before",
    "after",
    "status",
    "reason",
];

/// The action: preview (read, writes the CSV) or apply (writes stream lists).
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    let client = context.dispatcharr()?;
    let action_id = format!(
        "streams.match_{}",
        if dry_run { "preview" } else { "apply" }
    );

    // Every slice a match reads: channels (their own lists), groups (labels, stream groups,
    // zone bases), profiles (the scope's profile filter), streams (names, stats, accounts,
    // custom flag) and the M3U accounts (priority order). EPG entries are loaded only when
    // placeholder matching is on: they are the `tvg_id` -> EPG record join it needs.
    let epg_settings = EpgSettings::from_settings(settings);
    let mut kinds = WorldKinds::only(WorldKind::Channels)
        | WorldKind::Groups
        | WorldKind::Profiles
        | WorldKind::Streams
        | WorldKind::M3uAccounts;
    if epg_settings.enabled {
        kinds = kinds | WorldKind::EpgEntries;
    }
    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.match",
        0,
        0,
        "loading Dispatcharr data",
    ));
    let world = Arc::new(client.load_world_with(kinds).await?);

    // The step's channel universe: a table override replaces `[scope]`; a named scope
    // resolving to ids replaces it with exactly those channels. `tables` feeds the pure
    // planners below.
    let resolved = context.resolve_scope(&world)?;

    let mut options = MatchOptions::new(settings, &context.config().matching);
    options.apply_scope_hints(settings, &resolved.scope_hints);
    // EPG-aware placeholder matching: resolve the titles BEFORE the blocking pass, in ONE
    // batched read for the entries a placeholder or watched name can actually reach. The
    // plugin asks the programme table per lookup (memoized per pass); this asks once.
    let mut epg_error: Option<String> = None;
    let mut epg_timezone_error: Option<String> = None;
    if epg_settings.enabled {
        // The entry pool a placeholder can reach: the step's channels, not the host's.
        let scoped = context.scoped_channels(&world)?;
        // The "Next Event" gate compares an embedded date against today in Dispatcharr's
        // own timezone (what the plugin reads in-process), fetched once per run. A settings
        // read that fails degrades to the host's date rather than refusing the match.
        let today = match client.system_timezone().await {
            Ok(zone) => {
                tracing::info!("streams.match: Dispatcharr timezone is {zone}");
                crosstalk_core::dispatcharr::today_in_zone(&zone)
            }
            Err(error) => {
                tracing::warn!(
                    "streams.match: could not read Dispatcharr's timezone, so the EPG \
                     date gate uses the host's date for this run: {error:#}"
                );
                epg_timezone_error = Some(format!("{error:#}"));
                crate::epg::local_today()
            }
        };
        let mut titles = EpgTitles::prepare(epg_settings.clone(), &world.entries, today);
        let needed = titles.needed_ids(&scoped.channels, &world.streams);
        tracing::info!(
            "streams.match: EPG placeholder matching compiled {} of {} pattern(s); asking about \
             {} EPG entr{}",
            epg_settings.patterns.len(),
            epg_settings.pattern_report.len(),
            needed.len(),
            if needed.len() == 1 { "y" } else { "ies" }
        );
        if !needed.is_empty() {
            context.progress(&crosstalk_core::jobs::ProgressEvent::new(
                "streams.match",
                0,
                0,
                "reading what is airing on the placeholder slots",
            ));
            // Degrade-don't-fail, like every other EPG read in the plugin: a guide that
            // cannot be read leaves the names as they are written, and the report says so.
            // Refusing the run instead would block a whole match over one auxiliary read.
            match client.current_programs(&needed).await {
                Ok(programmes) => titles.set_programmes(&programmes),
                Err(error) => {
                    tracing::warn!(
                        "streams.match: could not read what is airing, so EPG placeholder \
                         matching is skipped for this run: {error:#}"
                    );
                    epg_error = Some(format!("{error:#}"));
                }
            }
        }
        options = options.with_epg(Arc::new(titles));
    }
    // The store's `stream_health` verdicts join `stream_stats` as dead evidence, but only
    // when the dead filter is on, as in the plugin. A context with no store matches as the
    // plugin does without IPTV Checker metadata: nothing is store-dead.
    if settings.filter_dead_streams {
        if let Some(store) = context.store() {
            options.dead_streams = dead_streams(store)?;
        }
    }
    let options = Arc::new(options);
    let throughput: Arc<dyn ThroughputSource> =
        Arc::from(quality::throughput_source(context.store())?);

    let settings_for_plan = settings.clone();
    let scope_for_plan = resolved.tables.clone();
    // The blocking pass plans against the step's universe: a filtered world for an id
    // bound (with an empty scope table), the loaded world otherwise.
    let world_for_plan: Arc<World> = match &resolved.limit {
        Some(limit) => Arc::new(world.retaining_channels(limit)),
        None => Arc::clone(&world),
    };
    let options_for_plan = options.clone();
    let throughput_for_plan = Arc::clone(&throughput);
    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.match",
        0,
        0,
        "matching channel groups",
    ));
    let plan_started = std::time::Instant::now();
    // Rayon on a blocking thread, like `match_run::score_names`: a match over 20k groups is
    // CPU-bound and must never sit on the async runtime's worker.
    let outcome = tokio::task::spawn_blocking(move || {
        plan(
            &world_for_plan,
            &scope_for_plan,
            &settings_for_plan,
            &options_for_plan,
            throughput_for_plan.as_ref(),
        )
    })
    .await??;
    let plan_elapsed = plan_started.elapsed();
    tracing::info!(
        "streams.match: {} channel(s) in {} group(s) planned in {:?}",
        outcome.channels,
        outcome.groups,
        plan_elapsed
    );
    let mut outcome = outcome;

    // The `[ai.verify]` gate for borderline groups (step 9, task "streams"): a no-op
    // unless the gate can fire and the cut left a group empty. Only a model accept
    // attaches streams, so this runs before the nothing-matched check below. The gate
    // reads the step's channels, like the plan did.
    let scoped_for_gate = context.scoped_channels(&world)?;
    let ai = ai_gate::service(context.config());
    apply_ai_gate(
        &mut outcome,
        &world,
        &scoped_for_gate.channels,
        settings,
        &options,
        &throughput,
        context,
        ai.as_ref(),
    )
    .await?;

    // One batch identity write for every query and family member, so guide, streams and
    // lineup resolve the same name to the same identity (cross-app consistency): one
    // read pass plus ONE write transaction, resolved on rayon off the runtime.
    {
        let run_engine =
            RunEngine::for_config(&context.config().matching, context.store().cloned());
        let queries: Vec<String> = outcome
            .assignments
            .iter()
            .map(|assignment| assignment.channel_name.clone())
            .collect();
        let position_of_candidate: HashMap<i64, usize> = outcome
            .gate_candidates
            .iter()
            .enumerate()
            .map(|(position, (id, _))| (*id, position))
            .collect();
        let families: Vec<Vec<EngineRanked>> = outcome
            .assignments
            .iter()
            .map(|assignment| {
                assignment
                    .after
                    .iter()
                    .filter_map(|id| position_of_candidate.get(id).copied())
                    .map(|index| EngineRanked {
                        index,
                        score: run_engine.threshold(),
                        method: crosstalk_matching::engine::EngineMethod::Exact,
                    })
                    .collect()
            })
            .collect();
        let candidates: Vec<String> = outcome
            .gate_candidates
            .iter()
            .map(|(_, name)| name.clone())
            .collect();
        run_engine
            .warm_families(&queries, &families, &candidates, &[])
            .await;
    }

    if outcome.channels == 0 {
        return Ok(JobOutcome::new(
            "No channels are in scope. Check the group and profile settings in crosstalk.toml.",
        ));
    }
    if outcome.groups > 0
        && outcome.channels > 0
        && outcome.assignments.iter().all(|row| row.matched == 0)
    {
        // The plugin's own wording for a run that matched nothing at all.
        return Ok(JobOutcome::new(
            "No channel matched any stream. Check the [matching] cut and the stream \
             selection in streams.toml.",
        ));
    }

    // Only real changes are written: a channel already holding the planned list is not a row.
    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for assignment in outcome.writes() {
        values.insert(
            assignment.channel_id,
            SetValue::Streams(assignment.after.clone()),
        );
    }
    let op_plan = if values.is_empty() {
        Plan {
            description: format!(
                "channels in scope{}",
                if outcome.description.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", outcome.description)
                }
            ),
            ..Plan::default()
        }
    } else {
        let operations = vec![Op::Set {
            field: Field::Streams,
            values,
        }];
        // The write plan starts from the run's channel universe, so a step override
        // bounds the write like `[scope]` bounds an ordinary run.
        channel_ops::plan(&world, &context.base_selection(&world)?, &operations)?
    };

    let applied = if dry_run || op_plan.changes.is_empty() {
        None
    } else {
        Some(channel_ops::apply(client, &op_plan, context, &action_id).await)
    };

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = format!("match_{}", if dry_run { "preview" } else { "applied" });
    let title = format!(
        "{REPORT_TITLE} {} Report",
        if dry_run { "Preview" } else { "Applied" }
    );
    let mut spec = ReportSpec::new(app, &schema, &values, &stem, &title)
        .note(
            "Match mode",
            if options.ota_only {
                "US OTA only (callsign matching, no grouping)".to_string()
            } else if let Some(country) = options.lineup_country.as_deref() {
                format!(
                    "Match & Assign over the lineup scope, restricted to the lineup \
                     country {country} (a channel whose identity names a different known \
                     country keeps its own)"
                )
            } else {
                "Match & Assign (identity family groups)".to_string()
            },
        )
        .note(
            "Matching Sensitivity ([matching] in crosstalk.toml)",
            format!(
                "{}, cut {} out of 100, higher is stricter",
                context.config().matching.sensitivity.trim(),
                context.config().matching.threshold()
            ),
        )
        .note(
            "Write mode",
            format!(
                "{}, {}",
                if settings.overwrite_streams {
                    "replace each channel's stream list"
                } else {
                    "append streams the channel does not have"
                },
                if settings.keep_custom_streams {
                    "custom streams kept last"
                } else {
                    "custom streams not kept"
                }
            ),
        )
        .note(
            "Streams",
            format!(
                "{} eligible of {} ({} dead, {} account, {} stream group, {} regex rule(s) \
                 rejected)",
                outcome.pool.eligible,
                outcome.pool.streams,
                outcome.dead_filtered,
                outcome.pool.dropped_account + outcome.pool.dropped_inactive_account,
                outcome.pool.dropped_stream_group,
                selection::RuleSet::parse(&settings.stream_name_regex_rules)
                    .rejected()
                    .count(),
            ),
        );
    if let Some(epg) = outcome.epg.as_ref() {
        let epg_state = match &epg_error {
            Some(error) => format!("current programmes could not be read ({error})"),
            None => epg.summary(),
        };
        spec = spec.note(
            "EPG placeholder matching",
            format!(
                "{} pattern(s); {} — {} force-included by the watched-stream rule, {} name(s) \
                 over {} characters skipped",
                EpgSettings::from_settings(settings).patterns.len(),
                epg_state,
                epg.watch_hits,
                epg.skipped_long,
                selection::REGEX_NAME_MAX_LEN,
            ),
        );
        // The date gate ran on the host's date, not Dispatcharr's, so a boundary-day
        // title may have resolved differently than in the plugin. The log carries both.
        if let Some(error) = &epg_timezone_error {
            spec = spec.note(
                "Dispatcharr timezone",
                format!(
                    "the system timezone could not be read ({error}), so the EPG date \
                     gate used the host's date for this run"
                ),
            );
        }
    }
    if settings.restrict_matching_to_country || options.lineup_country.is_some() {
        spec = spec.note(
            "Country filter",
            format!(
                "engaged on {} group(s), {} foreign stream(s) dropped, skipped on {} \
                     (no country detected){}",
                outcome.country.engaged,
                outcome.country.foreign_dropped,
                outcome.country.skipped_unknown_channel,
                options
                    .lineup_country
                    .as_deref()
                    .map_or(String::new(), |country| {
                        format!("; lineup mode restricted to the lineup country {country}")
                    })
            ),
        );
    }
    let report = write_match_report(
        context,
        &spec,
        &outcome,
        &op_plan,
        applied.as_ref(),
        &context.config().matching,
        &world,
    )?;

    context.record_rows(&channel_ops::run_rows(&op_plan, applied.as_ref()))?;

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let changed = outcome.write_count();
    // The plugin's own completion wording (plugin.py:9080-9100), including its "nothing new
    // to add" case, which reads as a failure otherwise: with Overwrite off, a second run over
    // an unchanged library adds nothing.
    let matched_channels = outcome.updated_count();
    let mut builder = OutcomeBuilder::new(if dry_run {
        format!(
            "Dry run complete. Would update {changed} channel(s) with {} stream(s).",
            outcome.streams_planned()
        )
    } else if changed == 0 && matched_channels > 0 {
        format!(
            "Matched {matched_channels} channels. Nothing new to add: every matched stream was \
             already assigned. Turn on Overwrite Existing Streams to replace a channel's list \
             instead of adding to it."
        )
    } else {
        format!(
            "Matched and assigned {} streams across {changed} channels.",
            outcome.streams_planned()
        )
    })
    .count("selected", outcome.channels as i64)
    .count("groups", outcome.groups as i64)
    .count("matched", outcome.updated_count() as i64)
    .count("changes", op_plan.changes.len() as i64)
    .count("streams_planned", outcome.streams_planned() as i64)
    .count("dead_streams", outcome.dead_filtered as i64)
    .count("foreign_dropped", outcome.country.foreign_dropped as i64)
    .count(
        "country_skipped",
        outcome.country.skipped_unknown_channel as i64,
    );
    if let Some(applied) = applied.as_ref() {
        builder = builder
            .count("written", applied.ok as i64)
            .count("failed", applied.failed as i64);
        if applied.failed > 0 {
            builder = builder.note(format!("{} row(s) failed", applied.failed));
        }
    }
    if outcome.country.skipped_unknown_channel > 0 {
        builder = builder.note(format!(
            "country filter skipped on {} group(s), no country detected",
            outcome.country.skipped_unknown_channel
        ));
    }
    if let Some(ota) = outcome.ota {
        builder = builder
            .count("ota_no_callsign", ota.no_callsign as i64)
            .count("ota_not_in_database", ota.not_in_database as i64)
            .count("ota_no_streams", ota.no_streams as i64);
    }
    // The cancel rule (see crosstalk_core::jobs): the streams that landed stay recorded and the
    // run says it was cancelled rather than claiming a complete match.
    if let Some(note) = channel_ops::cancelled_note(context, &op_plan, applied.as_ref()) {
        builder = builder.cancelled().note(note);
    }
    Ok(builder.report(Some(report)).build())
}

/// The streams a probe verdict says are dead: `streams.probe`'s class, read only when the
/// dead filter is on.
///
/// This is the ADDITIONAL signal, next to `stream_stats` (see [`stream_stats_says_dead`],
/// which is what IPTV Checker itself writes): the probe records the same verdict in the
/// store as the class `dead`. Every other class (blank, frozen, silent, skipped) means the
/// stream plays, so it stays a candidate — triage decides what to do about bad content,
/// not matching.
fn dead_streams(store: &Store) -> Result<HashSet<i64>> {
    let health = store.stream_health_map()?;
    Ok(health
        .into_iter()
        .filter(|(_, row)| row.class == "dead")
        .map(|(id, _)| id)
        .collect())
}

/// The CSV: one row per in-scope channel, in the plugin's preview shape plus the write.
fn write_match_report(
    context: &JobContext,
    spec: &ReportSpec<'_>,
    outcome: &MatchOutcome,
    plan: &Plan,
    applied: Option<&ApplyReport>,
    matching: &MatchingConfig,
    world: &World,
) -> Result<PathBuf> {
    // The one decision cut (D02/T10): `[matching]` sensitivity, not a per-app threshold.
    let threshold = matching.threshold();
    let status_of = |channel_id: i64, assignment: &Assignment| -> (String, String) {
        let change = applied
            .and_then(|report| {
                report
                    .changes
                    .iter()
                    .find(|change| change.channel_id == channel_id)
            })
            .or_else(|| {
                plan.changes
                    .iter()
                    .find(|change| change.channel_id == channel_id)
            });
        match change {
            Some(change) => (change.status.as_str().to_string(), change.reason.clone()),
            None if assignment.keep_existing => ("kept".to_string(), assignment.reason.clone()),
            None if assignment.will_write() => ("planned".to_string(), assignment.reason.clone()),
            None => ("unchanged".to_string(), assignment.reason.clone()),
        }
    };
    let display = |ids: &[i64]| -> String {
        if ids.is_empty() {
            return "none".to_string();
        }
        ids.iter()
            .map(
                |id| match world.streams.iter().find(|stream| stream.id == *id) {
                    Some(stream) => match stream_label(world, stream) {
                        Some(label) => format!("{} [{label}]", stream.name),
                        None => stream.name.clone(),
                    },
                    None => id.to_string(),
                },
            )
            .collect::<Vec<_>>()
            .join("; ")
    };
    let exports_dir: PathBuf = context.exports_dir();
    let report = csv_report::Report::new(&exports_dir, spec.app, spec.stem, spec.title)
        .channels_processed(outcome.channels.max(plan.selected.len()))
        .result("Selection", &plan.description)
        .result("Changes planned", plan.changes.len())
        .result(
            "Changes written",
            match applied {
                Some(report) => report.ok.to_string(),
                None => "none, this was a preview".to_string(),
            },
        )
        .result("Groups", outcome.groups)
        .result("Channels matched", outcome.updated_count())
        .result("Streams planned", outcome.streams_planned());
    let report = spec
        .notes
        .iter()
        .fold(report, |report, (label, value)| report.result(label, value));
    report.write(
        context.config(),
        spec.schema,
        spec.values,
        &MATCH_COLUMNS,
        |writer| {
            for assignment in &outcome.assignments {
                let (status, reason) = status_of(assignment.channel_id, assignment);
                writer.write_record([
                    if assignment.will_write() { "Yes" } else { "No" }.to_string(),
                    threshold.to_string(),
                    assignment.channel_id.to_string(),
                    csv_report::safe_cell(&assignment.channel_name),
                    assignment.matched.to_string(),
                    csv_report::safe_cell(&display(&assignment.after)),
                    csv_report::safe_cell(&assignment.channel_group),
                    csv_report::safe_cell(&display(&assignment.before)),
                    csv_report::safe_cell(&display(&assignment.after)),
                    status,
                    csv_report::safe_cell(&reason),
                ])?;
            }
            for skip in &plan.skipped {
                writer.write_record([
                    "No".to_string(),
                    threshold.to_string(),
                    skip.channel_id.to_string(),
                    csv_report::safe_cell(&skip.channel_name),
                    "0".to_string(),
                    String::new(),
                    csv_report::safe_cell(&skip.channel_group),
                    String::new(),
                    String::new(),
                    "skipped".to_string(),
                    csv_report::safe_cell(&skip.reason),
                ])?;
            }
            Ok(())
        },
    )
}

/// A stream's M3U account name, for the CSV label (`_labeled_stream_names`'s `[source]`).
///
/// `None` for a stream with no account: the plugin leaves it as its bare name
/// (`name_map.get(None)` is absent), and so does the report.
fn stream_label(world: &World, stream: &Stream) -> Option<String> {
    let id = stream.m3u_account?;
    Some(
        world
            .m3u_accounts
            .iter()
            .find(|account| account.id == id)
            .map(|account| account.name.clone())
            .unwrap_or_else(|| format!("m3u-{id}")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::ServerFixture;
    use crosstalk_core::dispatcharr::{ChannelGroup, M3uAccount};
    use serde_json::json;

    const TVE: i64 = 100;
    const CABLE: i64 = 101;

    fn groups() -> Vec<ChannelGroup> {
        vec![
            ChannelGroup {
                id: TVE,
                name: "TVE".to_string(),
            },
            ChannelGroup {
                id: CABLE,
                name: "Cable".to_string(),
            },
        ]
    }

    fn accounts() -> Vec<M3uAccount> {
        vec![
            M3uAccount {
                id: 1,
                name: "Main".to_string(),
                is_active: true,
                ..M3uAccount::default()
            },
            M3uAccount {
                id: 2,
                name: "Backup".to_string(),
                is_active: true,
                ..M3uAccount::default()
            },
            M3uAccount {
                id: 9,
                name: "Custom".to_string(),
                is_active: true,
                locked: true,
                ..M3uAccount::default()
            },
        ]
    }

    fn stream(id: i64, name: &str, account: Option<i64>, group: Option<i64>) -> Stream {
        Stream {
            id,
            name: name.to_string(),
            m3u_account: account,
            channel_group: group,
            stream_stats: json!({"width": 1920, "height": 1080, "source_fps": 60}),
            ..Stream::default()
        }
    }

    fn channel(id: i64, name: &str, streams: Vec<i64>, number: i64) -> Channel {
        Channel {
            id,
            name: name.to_string(),
            channel_number: Some(json!(number)),
            channel_group_id: Some(TVE),
            streams,
            ..Channel::default()
        }
    }

    fn match_options(settings: &StreamsSettings) -> MatchOptions {
        MatchOptions::with_database(
            settings,
            &MatchingConfig::default(),
            Some(ChannelDatabase::empty()),
        )
    }

    /// Step 9: the cut is the host `[matching]` sensitivity, so a test that varies the
    /// cut builds its own [`MatchingConfig`]. The retired SM preset names map onto the
    /// host presets (`relaxed`/`normal`/`strict`/`exact` = 70/80/90/95).
    fn matching_at(preset: &str) -> MatchingConfig {
        MatchingConfig {
            sensitivity: match preset {
                "relaxed" => "low".to_string(),
                "normal" => "medium".to_string(),
                "strict" => "high".to_string(),
                "exact" => "strict".to_string(),
                other => other.to_string(),
            },
            ..MatchingConfig::default()
        }
    }

    fn plan_match_with(
        world: &World,
        settings: &StreamsSettings,
        matching: &MatchingConfig,
    ) -> MatchOutcome {
        let options =
            MatchOptions::with_database(settings, matching, Some(ChannelDatabase::empty()));
        let throughput = quality::MemoryThroughputSource::new();
        plan(
            world,
            &ScopeConfig::default(),
            settings,
            &options,
            &throughput,
        )
        .expect("plan")
    }

    fn plan_match(world: &World, settings: &StreamsSettings) -> MatchOutcome {
        plan_match_with(world, settings, &MatchingConfig::default())
    }

    fn assignment(outcome: &MatchOutcome, channel_id: i64) -> &Assignment {
        outcome
            .assignments
            .iter()
            .find(|assignment| assignment.channel_id == channel_id)
            .unwrap_or_else(|| panic!("no assignment for channel {channel_id}"))
    }

    #[test]
    fn a_name_family_collects_every_variant_of_the_name() {
        let world = World {
            channels: vec![channel(1, "ESPN", vec![], 1)],
            streams: vec![
                stream(10, "ESPN HD", Some(1), Some(TVE)),
                stream(11, "ESPN [4K]", Some(2), Some(TVE)),
                stream(12, "ESPN", Some(2), Some(CABLE)),
                stream(13, "CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let outcome = plan_match(&world, &StreamsSettings::default());
        let row = assignment(&outcome, 1);
        assert_eq!(row.matched, 3, "{:?}", row.after);
        // Both feeds on the first account carry identical stats, so the sort ties and the
        // collection order stands: the quality model's own order is `sort`'s test.
        assert_eq!(row.after, vec![10, 11, 12]);
        // Step 9: the whole identity family at the `[matching]` cut (D07).
        assert!(
            row.reason.starts_with("Identity family match"),
            "{}",
            row.reason
        );
    }

    #[test]
    fn a_numbered_family_only_collects_its_own_number() {
        // Issue #62: at every sensitivity preset, "Event 1" must not collect "Event 12".
        let mut world = World {
            channels: vec![
                channel(1, "NebulaOne Event 1", vec![], 1),
                channel(2, "NebulaOne Event 12", vec![], 2),
            ],
            streams: (1..=20)
                .map(|number| {
                    stream(
                        100 + number,
                        &format!("NebulaOne Event {number} HD (WEB)"),
                        Some(1),
                        Some(TVE),
                    )
                })
                .collect(),
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        for sensitivity in ["relaxed", "normal", "strict", "exact"] {
            let settings = StreamsSettings {
                ignore_tags: "HD, (WEB)".to_string(),
                ..StreamsSettings::default()
            };
            let outcome = plan_match_with(&world, &settings, &matching_at(sensitivity));
            assert_eq!(
                assignment(&outcome, 1).after,
                vec![101],
                "{sensitivity}: one"
            );
            assert_eq!(
                assignment(&outcome, 2).after,
                vec![112],
                "{sensitivity}: twelve"
            );
        }
        let _ = &mut world;
    }

    #[test]
    fn an_alias_table_hit_joins_the_family() {
        // Step 9: alias force-includes are retired (A01/A02) — the merged reverse index
        // is inside the engine, so an alias hit is a family member scoring at the cut or
        // nothing at all. `MS Now` IS an MSNBC identity in the merged table, while CNN
        // is not, so the family is exactly the alias member.
        let world = World {
            channels: vec![channel(1, "MSNBC", vec![], 1)],
            streams: vec![
                stream(10, "MS Now", Some(1), Some(TVE)),
                stream(11, "CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let outcome = plan_match_with(&world, &StreamsSettings::default(), &matching_at("exact"));
        let row = assignment(&outcome, 1);
        assert_eq!(row.after, vec![10], "{:?}", row.reason);
        assert_eq!(row.reason, "Identity family match (alias, score: 100)");
    }

    #[test]
    fn the_identity_callsign_anchor_matches_a_paren_callsign_and_skips_uncorroborated_words() {
        // Step 9: the SM callsign regex path with corroboration is retired (C06/D04) —
        // the engine's callsign anchor decides agreement at family level and needs high
        // confidence on BOTH sides. A paren callsign still resolves; a denylisted word
        // the station table corroborated by city no longer does (oracle: D04 strictness).
        // The digit guard still keeps a mid-name word from collecting anything.
        let world = World {
            channels: vec![
                channel(1, "ABC - AL Montgomery (WNCF)", vec![], 1),
                // "(KING)" is a high-confidence extraction; a denylisted callsign is only
                // accepted from one (bug-098), which is what lets KING-TV match at all.
                channel(2, "Seattle (KING)", vec![], 2),
                channel(3, "24/7 KING OF THE HILL", vec![], 3),
            ],
            streams: vec![
                stream(10, "WNCF-DT", Some(1), Some(TVE)),
                stream(11, "ABC 32 Montgomery", Some(1), Some(TVE)),
                stream(12, "KING OF THE HILL", Some(1), Some(TVE)),
                stream(13, "KING 5 Seattle NBC", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let outcome = plan_match(&world, &StreamsSettings::default());
        let ota = assignment(&outcome, 1);
        assert_eq!(ota.after, vec![10], "{:?} {}", ota.after, ota.reason);
        assert_eq!(ota.reason, "Identity family match (callsign, score: 95)");
        // A denylisted word needs the anchor's high confidence on both sides
        // (C06/D04): the city's corroboration no longer promotes it, so the group is
        // empty — while the plugin's regex path collected stream 13.
        let king = assignment(&outcome, 2);
        assert!(king.after.is_empty(), "{:?} {}", king.after, king.reason);
        assert_eq!(king.reason, "No identity family match");
        // "24/7 KING OF THE HILL" resolves no callsign (mid-name, low confidence), and the
        // digit guard then keeps it from collecting anything.
        let hill = assignment(&outcome, 3);
        assert!(hill.after.is_empty(), "{:?} {}", hill.after, hill.reason);
    }

    #[test]
    fn the_visible_channel_limit_takes_the_quality_tagged_channel_and_zone_siblings_join() {
        let mut world = World {
            channels: vec![
                channel(1, "ESPN", vec![], 5),
                channel(2, "ESPN [HD]", vec![], 2),
                channel(3, "ESPN [4K]", vec![], 9),
            ],
            streams: vec![
                stream(10, "ESPN HD", Some(1), Some(TVE)),
                stream(11, "ESPN 4K", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let outcome = plan_match(&world, &StreamsSettings::default());
        // Visible Channel Limit 1: only the best-tagged, then lowest-numbered channel.
        assert_eq!(outcome.updated_count(), 1);
        assert!(assignment(&outcome, 3).will_write());
        assert!(!assignment(&outcome, 1).will_write());
        assert!(!assignment(&outcome, 2).will_write());

        // A limit of 2 takes the 4K channel and the HD one, in tag order.
        let settings = StreamsSettings {
            visible_channel_limit: 2,
            ..StreamsSettings::default()
        };
        let outcome = plan_match(&world, &settings);
        assert_eq!(outcome.updated_count(), 2);
        assert!(assignment(&outcome, 2).will_write());
        assert!(!assignment(&outcome, 1).will_write());

        // Zone siblings are distinct feeds, not duplicates: a West channel joins the update
        // list even at a limit of 1.
        world.channels.push(channel(4, "ESPN (W)", vec![], 7));
        world
            .streams
            .push(stream(12, "ESPN West", Some(1), Some(TVE)));
        let outcome = plan_match(&world, &StreamsSettings::default());
        assert_eq!(outcome.updated_count(), 2, "{:?}", outcome.assignments);
        assert_eq!(assignment(&outcome, 4).after, vec![12]);
    }

    #[test]
    fn overwrite_replace_with_the_matches_and_keep_the_custom_tail_last() {
        let world = World {
            channels: vec![channel(1, "ESPN", vec![13, 20, 21], 1)],
            streams: vec![
                stream(10, "ESPN HD", Some(1), Some(TVE)),
                stream(11, "ESPN 4K", Some(1), Some(TVE)),
                stream(13, "Could Not Dispatch", Some(9), None),
                Stream {
                    id: 13,
                    is_custom: true,
                    ..stream(13, "Could Not Dispatch", Some(9), None)
                },
                Stream {
                    id: 20,
                    is_custom: true,
                    ..stream(20, "Local Insert", Some(9), None)
                },
                stream(21, "Something Else", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let outcome = plan_match(&world, &StreamsSettings::default());
        let row = assignment(&outcome, 1);
        // The new list, then the channel's custom streams in their old order.
        assert_eq!(row.after, vec![10, 11, 13, 20], "{:?}", row.after);

        // With Keep Custom Streams Last off, the overwrite removes them.
        let settings = StreamsSettings {
            keep_custom_streams: false,
            ..StreamsSettings::default()
        };
        let outcome = plan_match(&world, &settings);
        assert_eq!(assignment(&outcome, 1).after, vec![10, 11]);
    }

    #[test]
    fn append_only_adds_streams_the_channel_does_not_already_have() {
        let world = World {
            channels: vec![channel(1, "ESPN", vec![12, 40], 1)],
            streams: vec![
                stream(10, "ESPN HD", Some(1), Some(TVE)),
                stream(11, "ESPN 4K", Some(1), Some(TVE)),
                stream(12, "ESPN", Some(2), Some(TVE)),
                Stream {
                    id: 40,
                    is_custom: true,
                    ..stream(40, "Could Not Dispatch", Some(9), None)
                },
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let settings = StreamsSettings {
            overwrite_streams: false,
            ..StreamsSettings::default()
        };
        let outcome = plan_match(&world, &settings);
        let row = assignment(&outcome, 1);
        // The existing list first, then what the channel was missing, in match order.
        assert_eq!(row.after, vec![12, 40, 10, 11], "{:?}", row.after);
    }

    #[test]
    fn a_group_that_matches_nothing_never_clears_a_channel() {
        // bug-063: "overwrite" only replaces when there is something to replace WITH.
        let world = World {
            channels: vec![channel(1, "Totally Unmatched", vec![10, 11], 1)],
            streams: vec![
                stream(10, "ESPN", Some(1), Some(TVE)),
                stream(11, "CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let outcome = plan_match(&world, &StreamsSettings::default());
        let row = assignment(&outcome, 1);
        assert_eq!(row.matched, 0);
        assert_eq!(row.after, row.before);
        assert!(!row.will_write());
        assert!(outcome.writes().next().is_none());
    }

    #[test]
    fn the_country_filter_drops_foreign_streams_and_sorts_same_country_first() {
        let world = World {
            channels: vec![channel(1, "CNN", vec![], 1)],
            streams: vec![
                stream(10, "UK: CNN", Some(1), Some(TVE)),
                stream(11, "CNN", Some(1), Some(TVE)),
                stream(12, "US: CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let mut options = match_options(&StreamsSettings {
            restrict_matching_to_country: true,
            ..StreamsSettings::default()
        });
        // The fixture database says CNN is American.
        options.database = ChannelDatabase::from_entries([DatabaseEntry {
            name: "CNN".to_string(),
            code: "US".to_string(),
            callsign: None,
        }]);
        let throughput = quality::MemoryThroughputSource::new();
        let settings = StreamsSettings {
            restrict_matching_to_country: true,
            ..StreamsSettings::default()
        };
        let outcome = plan(
            &world,
            &ScopeConfig::default(),
            &settings,
            &options,
            &throughput,
        )
        .expect("plan");
        let row = assignment(&outcome, 1);
        // The UK feed is dropped outright; the same-country feed leads the unknown one.
        assert_eq!(row.after, vec![12, 11], "{:?}", row.after);
        assert_eq!(outcome.country.engaged, 1);
        assert_eq!(outcome.country.foreign_dropped, 1);
    }

    #[test]
    fn lineup_mode_restricts_to_the_lineup_country_without_the_explicit_filter() {
        // Step 19: `run` turns the filter on from the scope hint and sets
        // `lineup_country`; `plan` then behaves like the explicit filter, with the
        // lineup file's country as the default. The channel names no country, so the
        // US hint is what enforces.
        let world = World {
            channels: vec![channel(1, "CNN", vec![], 1)],
            streams: vec![
                stream(10, "UK: CNN", Some(1), Some(TVE)),
                stream(11, "CNN", Some(1), Some(TVE)),
                stream(12, "US: CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let mut options = match_options(&StreamsSettings::default());
        options.restrict_matching_to_country = true;
        options.lineup_country = Some("US".to_string());
        let throughput = quality::MemoryThroughputSource::new();
        let outcome = plan(
            &world,
            &ScopeConfig::default(),
            &StreamsSettings::default(),
            &options,
            &throughput,
        )
        .expect("plan");
        let row = assignment(&outcome, 1);
        // The UK feed is dropped outright; the same-country feed leads the unknown one.
        assert_eq!(row.after, vec![12, 11], "{:?}", row.after);
        assert_eq!(outcome.country.engaged, 1);
        assert_eq!(outcome.country.foreign_dropped, 1);
    }

    #[test]
    fn scope_hints_turn_lineup_mode_on_only_when_the_setting_allows() {
        let hinted = crosstalk_core::subapp::NamedScopeHints::with_country("UK");
        let settings = StreamsSettings::default();
        let mut options = match_options(&settings);
        options.apply_scope_hints(&settings, &hinted);
        assert!(options.restrict_matching_to_country);
        assert_eq!(options.lineup_country.as_deref(), Some("UK"));

        // Off: a lineup step matches like an ordinary run.
        let off = StreamsSettings {
            lineup_mode_country_restriction: false,
            ..StreamsSettings::default()
        };
        let mut options = match_options(&off);
        options.apply_scope_hints(&off, &hinted);
        assert!(!options.restrict_matching_to_country);
        assert_eq!(options.lineup_country, None);

        // No hint (an ordinary run, or a lineup file without a country prefix).
        let mut options = match_options(&settings);
        options.apply_scope_hints(&settings, &crosstalk_core::subapp::NamedScopeHints::none());
        assert!(!options.restrict_matching_to_country);
        assert_eq!(options.lineup_country, None);
    }

    #[test]
    fn lineup_mode_keeps_a_channel_whose_identity_names_another_country() {
        // Step 19: the hint is the DEFAULT, not the law. The channel's own identity
        // is British, so the US lineup hint does not re-nationalise it: the US feed
        // is the foreign one here.
        let world = World {
            channels: vec![channel(1, "UK: CNN", vec![], 1)],
            streams: vec![
                stream(10, "UK: CNN", Some(1), Some(TVE)),
                stream(11, "CNN", Some(1), Some(TVE)),
                stream(12, "US: CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let mut options = match_options(&StreamsSettings::default());
        options.restrict_matching_to_country = true;
        options.lineup_country = Some("US".to_string());
        let throughput = quality::MemoryThroughputSource::new();
        let outcome = plan(
            &world,
            &ScopeConfig::default(),
            &StreamsSettings::default(),
            &options,
            &throughput,
        )
        .expect("plan");
        let row = assignment(&outcome, 1);
        // Enforced UK (the channel's own), not the US hint: the US feed drops.
        assert_eq!(row.after, vec![10, 11], "{:?}", row.after);
        assert_eq!(outcome.country.engaged, 1);
        assert_eq!(outcome.country.foreign_dropped, 1);
    }

    #[test]
    fn two_countries_never_share_a_group_whether_or_not_the_filter_is_on() {
        // Filter OFF: `UK: CNN` and `US: CNN` share a normalize key, but their families
        // differ by country (G03), so the UK leader's family must not land on the US
        // channel.
        let world = World {
            channels: vec![
                channel(1, "UK: CNN", vec![], 1),
                channel(2, "US: CNN", vec![], 2),
            ],
            streams: vec![
                stream(10, "UK: CNN", Some(1), Some(TVE)),
                stream(11, "US: CNN", Some(1), Some(TVE)),
                stream(12, "CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let outcome = plan_match(&world, &StreamsSettings::default());
        assert_eq!(outcome.groups, 2);
        assert!(!assignment(&outcome, 1).after.contains(&11));
        assert!(!assignment(&outcome, 2).after.contains(&10));
        assert!(assignment(&outcome, 2).after.contains(&11));

        // Filter ON: the country comes from the channel GROUP's name as well (SM's
        // `_channel_country_code`, bug-158), so two bare `CNN`s in a US and a UK group
        // are two groups — and visibility, which groups through the same function,
        // cannot enable one and disable the other.
        let world = World {
            channels: vec![
                Channel {
                    channel_group_id: Some(200),
                    ..channel(1, "CNN", vec![], 1)
                },
                Channel {
                    channel_group_id: Some(201),
                    ..channel(2, "CNN", vec![], 2)
                },
            ],
            streams: vec![
                stream(10, "UK: CNN", Some(1), Some(TVE)),
                stream(11, "US: CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: vec![
                ChannelGroup {
                    id: 200,
                    name: "US: News".to_string(),
                },
                ChannelGroup {
                    id: 201,
                    name: "UK: News".to_string(),
                },
                ChannelGroup {
                    id: TVE,
                    name: "TVE".to_string(),
                },
            ],
            ..World::default()
        };
        let settings = StreamsSettings {
            restrict_matching_to_country: true,
            ..StreamsSettings::default()
        };
        let outcome = plan_match(&world, &settings);
        assert_eq!(outcome.groups, 2);
        assert_eq!(assignment(&outcome, 1).after, vec![11]);
        assert_eq!(assignment(&outcome, 2).after, vec![10]);
        let off = plan_match(&world, &StreamsSettings::default());
        assert_eq!(off.groups, 1, "no country is known with the filter off");
    }

    #[test]
    fn a_dead_stream_is_not_a_candidate_unless_every_stream_is_dead() {
        let world = World {
            channels: vec![
                channel(1, "ESPN", vec![], 1),
                channel(2, "CNN", vec![10], 2),
            ],
            streams: vec![
                stream(10, "ESPN", Some(1), Some(TVE)),
                stream(11, "ESPN HD", Some(1), Some(TVE)),
                stream(12, "CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let settings = StreamsSettings {
            filter_dead_streams: true,
            ..StreamsSettings::default()
        };
        let mut options = match_options(&settings);
        options.dead_streams = [10].into_iter().collect();
        let throughput = quality::MemoryThroughputSource::new();
        let outcome = plan(
            &world,
            &ScopeConfig::default(),
            &settings,
            &options,
            &throughput,
        )
        .expect("plan");
        assert_eq!(assignment(&outcome, 1).after, vec![11]);
        assert_eq!(outcome.dead_filtered, 1);

        // Every stream is dead: `_filter_working_streams` keeps the whole list rather than
        // leaving nothing to match, so the run matches as it would with the filter off.
        let mut options = match_options(&settings);
        options.dead_streams = [10, 11, 12].into_iter().collect();
        let outcome = plan(
            &world,
            &ScopeConfig::default(),
            &settings,
            &options,
            &throughput,
        )
        .expect("plan");
        assert_eq!(assignment(&outcome, 2).after, vec![12]);

        // The filter is off by default, as in the plugin: a dead verdict alone keeps
        // every stream a candidate.
        let off = match_options(&StreamsSettings::default());
        let outcome = plan(
            &world,
            &ScopeConfig::default(),
            &StreamsSettings::default(),
            &off,
            &throughput,
        )
        .expect("plan");
        assert_eq!(outcome.dead_filtered, 0);
    }

    #[test]
    fn stream_stats_says_dead_matches_the_plugin_rule() {
        // IPTV Checker writes exactly `{}` for a stream it found dead.
        assert!(stream_stats_says_dead(&json!({})));
        // A zero width or height, as integers.
        assert!(stream_stats_says_dead(&json!({"width": 0, "height": 0})));
        assert!(stream_stats_says_dead(&json!({"width": 0, "height": 1080})));
        assert!(stream_stats_says_dead(&json!({"width": 1920, "height": 0})));
        assert!(!stream_stats_says_dead(
            &json!({"width": 1920, "height": 1080})
        ));
        // Numeric strings parse, as `int()` does; anything else is not a number.
        assert!(stream_stats_says_dead(
            &json!({"width": "0", "height": "0"})
        ));
        assert!(!stream_stats_says_dead(
            &json!({"width": "1920", "height": "1080"})
        ));
        assert!(!stream_stats_says_dead(
            &json!({"width": "3.5", "height": 0})
        ));
        // Booleans are unusable, so a boolean width falls through to the resolution.
        assert!(!stream_stats_says_dead(
            &json!({"width": true, "height": 0})
        ));
        // The `WxH` resolution string, case-insensitively, exactly two parts.
        assert!(stream_stats_says_dead(&json!({"resolution": "0x0"})));
        assert!(stream_stats_says_dead(&json!({"resolution": "0X0"})));
        assert!(!stream_stats_says_dead(&json!({"resolution": "1920x1080"})));
        assert!(!stream_stats_says_dead(
            &json!({"resolution": "1920x1080x720"})
        ));
        assert!(!stream_stats_says_dead(&json!({"resolution": "unknown"})));
        // Width/height win when both are usable; a zero resolution does not revive them.
        assert!(!stream_stats_says_dead(
            &json!({"width": 1920, "height": 1080, "resolution": "0x0"})
        ));
        // Everything else is fail-open: null, non-dicts, audio-only stats.
        assert!(!stream_stats_says_dead(&json!(null)));
        assert!(!stream_stats_says_dead(&json!([])));
        assert!(!stream_stats_says_dead(&json!("0x0")));
        assert!(!stream_stats_says_dead(&json!({"audio_codec": "aac"})));
    }

    #[test]
    fn a_stats_dead_stream_is_filtered_only_when_the_filter_is_on() {
        let world = World {
            channels: vec![channel(1, "ESPN", vec![], 1)],
            streams: vec![
                Stream {
                    stream_stats: json!({}),
                    ..stream(10, "ESPN", Some(1), Some(TVE))
                },
                stream(11, "ESPN HD", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        // Off by default: the empty-stats stream stays a candidate (order is the
        // quality model's own, so compare as a set).
        let outcome = plan_match(&world, &StreamsSettings::default());
        let mut after = assignment(&outcome, 1).after.clone();
        after.sort_unstable();
        assert_eq!(after, vec![10, 11]);
        // On: the no-evidence stream is skipped, like the plugin's filter.
        let settings = StreamsSettings {
            filter_dead_streams: true,
            ..StreamsSettings::default()
        };
        let outcome = plan_match(&world, &settings);
        assert_eq!(assignment(&outcome, 1).after, vec![11]);
        assert_eq!(outcome.dead_filtered, 1);
    }

    #[test]
    fn ota_only_matches_upper_case_callsigns_and_skips_channels_without_a_station() {
        let world = World {
            channels: vec![
                channel(1, "ABC - AL Montgomery (WNCF)", vec![], 1),
                channel(2, "Not A Station", vec![20], 2),
            ],
            streams: vec![
                stream(10, "WNCF", Some(1), Some(TVE)),
                stream(11, "WNCF-DT2", Some(1), Some(TVE)),
                // The US-OTA action's pattern is case-sensitive, so this is not a match.
                stream(12, "wncf", Some(1), Some(TVE)),
                stream(20, "Something", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let settings = StreamsSettings {
            ota_only: true,
            ..StreamsSettings::default()
        };
        let outcome = plan_match(&world, &settings);
        let row = assignment(&outcome, 1);
        assert_eq!(row.after.len(), 2, "{:?} {}", row.after, row.reason);
        assert_eq!(row.reason, "US OTA callsign: WNCF");
        assert!(!row.after.contains(&12));
        // A channel with no US callsign is skipped and left alone.
        let skipped = assignment(&outcome, 2);
        assert!(!skipped.will_write());
        assert_eq!(outcome.ota.expect("counters").no_callsign, 1);
    }

    /// The index is a superset prefilter, so the indexed collector must return exactly what a
    /// full scan returns.
    ///
    /// This is the guard for the argument on [`NameIndex`]: it drives the SAME collector over
    /// generated adversarial names — word breaks, numbered families, designators, cinemax,
    /// country prefixes, callsigns, short names — with the index on and off, at every preset.
    #[test]
    fn the_indexed_collector_agrees_with_a_full_scan() {
        let names = [
            "espn",
            "espn hd",
            "espn 2",
            "espn2",
            "espn news",
            "euro news",
            "euronews",
            "fox sports 1",
            "fox sports 2",
            "nebula one event 1",
            "nebula one event 12",
            "aa bb cc f1",
            "aa bb cc dd",
            "king 5 seattle",
            "king of the hill",
            "cnn",
            "us: cnn",
            "uk: cnn",
            "cinemax",
            "cinemax hd",
            "hbo max",
            "max",
            "max hd",
            "discovery+",
            "discovery plus",
            "bbc one",
            "wbbc one",
            "premier sports 1",
            "premier sports 2",
            // Very short names: the fuzzy stages skip them, but an exact alias hit or a
            // no-space equality must still find them.
            "three",
            "3",
            "go",
            "go 1",
            "go 12",
            "at",
            "a",
            "of",
            "f1",
            "tf1",
            "sky sports f1 uhd",
            "sky sports uhd 1",
            "mtv",
            "mtv2",
            "global",
            "hd 1",
            "24/7 south park",
            "south park",
            // Punctuation and non-ASCII: the narrowing compares normalised text, so the
            // index must be built from the same normalisation the predicates read.
            "caf\u{e9} m\u{fc}ller",
            "cafe muller",
            "na\u{ef}ve",
            "naive hd",
            "c\u{f4}te",
            // Token-sort forms: word order must not matter to the third fuzzy stage's
            // block, which indexes the sorted form.
            "news abc",
            "abc news",
            "hd news abc",
            "abc - al montgomery (wncf)",
            "wncf",
            "wncf-dt",
            "wncf-dt2",
            "wgn",
            // A callsign inside a longer token: not a word hit, so the grouped OTA path
            // scans, and the family must still narrow soundly.
            "wgn9",
            "wgn america",
            "ms now",
            "msnbc",
            "antena 3",
            "antena3",
            "antena 3 internacional",
            "starz encore",
            "starz encore (w)",
            "key west",
            "tnt",
            "tnt hd",
        ];
        let prefixes = ["", "US: ", "UK: ", "HD ", "24/7 ", "[CA] "];
        // A deterministic pseudo-random walk, so a failure is reproducible.
        let mut state = 0x5eed_1234_u64;
        let mut next = |bound: usize| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((state >> 33) as usize) % bound
        };
        let mut compared = 0;
        for round in 0..60 {
            let mut streams = Vec::new();
            for index in 0..12 {
                let name = format!(
                    "{}{}",
                    prefixes[next(prefixes.len())],
                    names[next(names.len())]
                );
                streams.push(stream(
                    index as i64 + 1,
                    &name,
                    Some(if next(4) == 0 { 2 } else { 1 }),
                    Some(TVE),
                ));
            }
            let mut channels = Vec::new();
            for index in 0..4 {
                let mut channel_name = names[next(names.len())].to_string();
                if next(3) == 0 {
                    channel_name = format!("{}{}", prefixes[next(prefixes.len())], channel_name);
                }
                channels.push(channel(
                    index as i64 + 1,
                    &channel_name,
                    Vec::new(),
                    index as i64 + 1,
                ));
            }
            let world = World {
                channels,
                streams,
                m3u_accounts: accounts(),
                groups: groups(),
                ..World::default()
            };
            for sensitivity in ["relaxed", "normal", "strict", "exact"] {
                for restrict in [false, true] {
                    let settings = StreamsSettings {
                        restrict_matching_to_country: restrict,
                        ignore_tags: if round % 3 == 0 {
                            "HD".to_string()
                        } else {
                            String::new()
                        },
                        ..StreamsSettings::default()
                    };
                    let matching = matching_at(sensitivity);
                    let mut options = MatchOptions::with_database(
                        &settings,
                        &matching,
                        Some(ChannelDatabase::empty()),
                    );
                    options.database = ChannelDatabase::from_entries([
                        DatabaseEntry {
                            name: "CNN".to_string(),
                            code: "US".to_string(),
                            callsign: None,
                        },
                        DatabaseEntry {
                            name: "BBC One".to_string(),
                            code: "UK".to_string(),
                            callsign: None,
                        },
                    ]);
                    let indexed = collected(&world, &settings, &options, false);
                    let scanned = collected(&world, &settings, &options, true);
                    for (left, right) in indexed.iter().zip(&scanned) {
                        compared += 1;
                        assert_eq!(
                            left,
                            right,
                            "round {round}, {sensitivity}, restrict={restrict}: the index \
                             dropped or added a stream\n  indexed={left:?}\n  scanned={right:?}\n  \
                             channels={:?}\n  streams={:?}",
                            world.channels.iter().map(|c| &c.name).collect::<Vec<_>>(),
                            world.streams.iter().map(|s| &s.name).collect::<Vec<_>>(),
                        );
                    }
                }
            }
        }
        assert!(compared > 1_000, "compared {compared} group(s)");
    }

    /// An alias whose text shares no gram with the channel (`Three` → `3`) is still
    /// force-included: the alias block is a no-space lookup, not a gram lookup, and the
    /// exact-equality fallback covers names the candidate cache drops (#68).
    #[test]
    fn a_short_alias_hit_is_force_included() {
        let world = World {
            channels: vec![channel(1, "Three", vec![], 1)],
            streams: vec![
                stream(10, "3", Some(1), Some(TVE)),
                stream(11, "CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let mut matching = MatchingConfig::default();
        matching
            .custom_aliases
            .insert("Three".to_string(), vec!["3".to_string()]);
        let settings = StreamsSettings::default();
        let options =
            MatchOptions::with_database(&settings, &matching, Some(ChannelDatabase::empty()));
        // The indexed collector and the full scan agree, and both collect the alias hit
        // even though `3` shares no gram with `three` and normalises to one character.
        let indexed = collected(&world, &settings, &options, false);
        let scanned = collected(&world, &settings, &options, true);
        assert_eq!(indexed, scanned);
        assert_eq!(indexed, vec![vec![10]], "{indexed:?}");
    }

    /// Collect with the index on (`all = false`) or off (`all = true`), once per group.
    fn collected(
        world: &World,
        settings: &StreamsSettings,
        options: &MatchOptions,
        all: bool,
    ) -> Vec<Vec<i64>> {
        let pool = selection::build(world, settings).expect("pool");
        let throughput = quality::MemoryThroughputSource::new();
        let data = RunData::new(
            world,
            &pool,
            options,
            &options.model,
            &throughput,
            &options.dead_streams,
            options.restrict_matching_to_country,
            options.epg.as_deref(),
        );
        let tags = data.tag_refs();
        let scoped = scope_channels(world, &ScopeConfig::default()).expect("scope");
        let groups = build_groups(
            &scoped.channels,
            &data.database,
            &data.normalizer,
            &tags,
            options.restrict_matching_to_country,
            &data.group_names,
            &options.engine,
            options.lineup_country.as_deref(),
        );
        let collector = Collector {
            data: &data,
            options,
        };
        groups
            .iter()
            .map(|group| {
                let sorted = sort_channels_by_priority(group, &scoped.channels);
                collector.collect_with(&scoped.channels[sorted[0]], all).ids
            })
            .collect()
    }

    #[tokio::test]
    async fn preview_writes_the_report_and_changes_nothing() {
        let world = World {
            channels: vec![
                channel(1, "ESPN", vec![12], 1),
                channel(2, "ESPN2", vec![], 2),
            ],
            streams: vec![
                stream(10, "ESPN HD", Some(1), Some(TVE)),
                stream(11, "ESPN 4K", Some(1), Some(TVE)),
                stream(12, "ESPN", Some(2), Some(TVE)),
                stream(13, "ESPN2", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run(crate::ACTION_MATCH_PREVIEW, &store, true);
        let settings = StreamsSettings::default();

        let outcome = run(&crate::StreamsApp::new(), &context, &settings, true)
            .await
            .expect("preview");
        assert!(
            outcome.summary.contains("Would update"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("groups"), Some(&2));
        // Channel 1's list reorders (account priority) and channel 2 gains its match.
        assert_eq!(outcome.counts.get("changes"), Some(&2));
        assert!(outcome.report_path.is_some());
        assert!(
            fixture
                .bodies("PATCH", "/api/channels/channels/edit/bulk/")
                .await
                .is_empty(),
            "a preview writes nothing"
        );

        let report = std::fs::read_to_string(outcome.report_path.unwrap()).expect("report");
        // The plugin's preview columns, then crosstalk's before/after.
        for column in [
            "will_update",
            "threshold",
            "channel_id",
            "channel_name",
            "matched_streams",
            "stream_names",
            "before",
            "after",
        ] {
            assert!(report.contains(column), "{column} missing: {report}");
        }
        assert!(report.contains("ESPN 4K [Main]"), "{report}");
    }

    #[tokio::test]
    async fn a_cancelled_apply_is_recorded_cancelled_and_keeps_what_it_did() {
        let world = World {
            channels: vec![
                channel(1, "ESPN", vec![12], 1),
                channel(2, "CNN", vec![20], 2),
            ],
            streams: vec![
                stream(10, "ESPN HD", Some(1), Some(TVE)),
                stream(11, "ESPN 4K", Some(1), Some(TVE)),
                stream(12, "ESPN", Some(2), Some(TVE)),
                stream(20, "CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run(crate::ACTION_MATCH_APPLY, &store, false);
        let run_id = context.run_id().expect("run");
        context.cancel();

        let outcome = run(
            &crate::StreamsApp::new(),
            &context,
            &StreamsSettings::default(),
            false,
        )
        .await
        .expect("a cancelled run still reports what it did");
        assert!(outcome.is_cancelled(), "{}", outcome.summary);
        assert!(
            outcome.summary.contains("cancelled by the operator"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("written"), Some(&0));
        assert!(fixture
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await
            .is_empty());
        let rows = store.rows(run_id).expect("rows");
        assert!(!rows.is_empty());
        assert!(
            rows.iter()
                .all(|row| row.status == "failed" && row.reason.contains("cancelled")),
            "{rows:?}"
        );
    }

    #[tokio::test]
    async fn apply_writes_only_the_channels_whose_list_changed() {
        let world = World {
            channels: vec![
                channel(1, "ESPN", vec![12], 1),
                channel(2, "CNN", vec![20], 2),
            ],
            streams: vec![
                stream(10, "ESPN HD", Some(1), Some(TVE)),
                stream(11, "ESPN 4K", Some(1), Some(TVE)),
                stream(12, "ESPN", Some(2), Some(TVE)),
                stream(20, "CNN", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run(crate::ACTION_MATCH_APPLY, &store, false);
        let settings = StreamsSettings::default();

        let outcome = run(&crate::StreamsApp::new(), &context, &settings, false)
            .await
            .expect("apply");
        assert!(
            outcome.summary.contains("Matched and assigned"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("written"), Some(&1));

        let bodies = fixture
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await;
        assert_eq!(bodies.len(), 1, "one batched write");
        let rows = bodies[0].as_array().expect("rows");
        assert_eq!(rows.len(), 1, "only the changed channel: {rows:?}");
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[0]["streams"], json!([10, 11, 12]));

        // CNN already holds exactly its match, so it is not a row at all.
        let run_id = context.run_id().expect("run");
        let rows = store.rows(run_id).expect("rows");
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].field, "streams");
        assert_eq!(rows[0].status, "ok");
        assert_eq!(rows[0].before_value.as_deref(), Some("12"));
        assert_eq!(rows[0].after_value.as_deref(), Some("10,11,12"));
    }

    #[tokio::test]
    async fn apply_says_so_when_there_is_nothing_to_write() {
        let world = World {
            channels: vec![channel(1, "CNN", vec![20], 1)],
            streams: vec![stream(20, "CNN", Some(1), Some(TVE))],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run(crate::ACTION_MATCH_APPLY, &store, false);
        let outcome = run(
            &crate::StreamsApp::new(),
            &context,
            &StreamsSettings::default(),
            false,
        )
        .await
        .expect("apply");
        // The plugin's own wording for a run that matched but had nothing to add.
        assert!(
            outcome.summary.contains("Nothing new to add"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("changes"), Some(&0));
    }

    #[tokio::test]
    async fn a_match_preview_needs_a_reachable_dispatcharr() {
        let fixture = crate::test_support::Fixture::with_credentials();
        let context = fixture.context();
        let error = run(
            &crate::StreamsApp::new(),
            &context,
            &StreamsSettings::default(),
            true,
        )
        .await
        .expect_err("no Dispatcharr");
        assert!(format!("{error:#}").contains("Dispatcharr"), "{error:#}");
    }

    // -----------------------------------------------------------------------------------
    // EPG-aware placeholder matching
    // -----------------------------------------------------------------------------------

    fn epg_settings(overrides: &str) -> StreamsSettings {
        StreamsSettings {
            epg_placeholder_matching_enabled: true,
            epg_placeholder_name_patterns: overrides.to_string(),
            ..StreamsSettings::default()
        }
    }

    fn programme(epg_data_id: i64, title: &str) -> crosstalk_core::dispatcharr::CurrentProgram {
        crosstalk_core::dispatcharr::CurrentProgram {
            epg_data_id,
            title: title.to_string(),
            ..crosstalk_core::dispatcharr::CurrentProgram::default()
        }
    }

    fn epg_world() -> World {
        World {
            channels: vec![
                Channel {
                    id: 1,
                    name: "PPV EVENT 04".to_string(),
                    channel_number: Some(json!(1)),
                    channel_group_id: Some(TVE),
                    epg_data_id: Some(7),
                    streams: Vec::new(),
                    ..Channel::default()
                },
                Channel {
                    id: 2,
                    name: "WWE SummerSlam".to_string(),
                    channel_number: Some(json!(2)),
                    channel_group_id: Some(TVE),
                    streams: Vec::new(),
                    ..Channel::default()
                },
            ],
            streams: vec![
                stream(10, "WWE SummerSlam", Some(1), Some(TVE)),
                stream(11, "PPV EVENT 04", Some(1), Some(TVE)),
                stream(12, "ESPN", Some(1), Some(TVE)),
                stream(13, "PPV EVENT 09", Some(1), Some(TVE)),
            ],
            entries: vec![
                crosstalk_core::dispatcharr::EpgEntry {
                    id: 7,
                    name: "PPV 04".to_string(),
                    tvg_id: "ppv04.us".to_string(),
                    epg_source: Some(1),
                    icon_url: None,
                },
                crosstalk_core::dispatcharr::EpgEntry {
                    id: 8,
                    name: "ESPN".to_string(),
                    tvg_id: "espn.us".to_string(),
                    epg_source: Some(1),
                    icon_url: None,
                },
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        }
    }

    /// Plan with the EPG titles a run would have resolved, so the collector sees exactly what
    /// the action hands it.
    fn plan_with_epg(
        world: &World,
        settings: &StreamsSettings,
        programmes: &[crosstalk_core::dispatcharr::CurrentProgram],
    ) -> MatchOutcome {
        let mut options = match_options(settings);
        if settings.epg_placeholder_matching_enabled {
            let epg = crate::epg::EpgSettings::from_settings(settings);
            let mut titles = EpgTitles::prepare(epg, &world.entries, (8, 5));
            titles.set_programmes(programmes);
            options = options.with_epg(Arc::new(titles));
        }
        let throughput = quality::MemoryThroughputSource::new();
        plan(
            world,
            &ScopeConfig::default(),
            settings,
            &options,
            &throughput,
        )
        .expect("plan")
    }

    #[test]
    fn a_placeholder_channel_is_matched_under_its_current_title() {
        let world = epg_world();
        let settings = epg_settings(r"^PPV EVENT \d+$");
        let outcome = plan_with_epg(&world, &settings, &[programme(7, "WWE SummerSlam")]);
        let row = assignment(&outcome, 1);
        // The channel's own name would have matched stream 11 ("PPV EVENT 04"); under the
        // resolved title it matches the event stream instead.
        assert_eq!(row.after, vec![10], "{row:?}");
        assert_eq!(row.matched, 1);
        // The stream side resolves the same title, so the two placeholder names still collect
        // each other's streams: stream 11's match name became "WWE SummerSlam" too.
        assert!(outcome.epg.is_some());
    }

    #[test]
    fn a_placeholder_stream_is_matched_under_its_current_title() {
        // The concrete case the feature exists for: the CHANNEL carries the real name and the
        // provider's stream is the generic slot.
        let world = World {
            streams: vec![
                stream(11, "PPV EVENT 04", Some(1), Some(TVE)),
                stream(13, "PPV EVENT 09", Some(1), Some(TVE)),
            ],
            entries: vec![crosstalk_core::dispatcharr::EpgEntry {
                id: 7,
                name: "PPV 04".to_string(),
                tvg_id: "ppv04.us".to_string(),
                epg_source: Some(1),
                icon_url: None,
            }],
            ..World {
                channels: vec![Channel {
                    id: 2,
                    name: "WWE SummerSlam".to_string(),
                    channel_number: Some(json!(2)),
                    channel_group_id: Some(TVE),
                    streams: Vec::new(),
                    ..Channel::default()
                }],
                streams: Vec::new(),
                m3u_accounts: accounts(),
                groups: groups(),
                ..World::default()
            }
        };
        // Give the placeholder stream its guide id, which the fixture helper does not set.
        let mut world = world;
        world.streams[0].tvg_id = "ppv04.us".to_string();
        let settings = epg_settings(r"^PPV EVENT \d+$");
        let outcome = plan_with_epg(&world, &settings, &[programme(7, "WWE SummerSlam")]);
        let row = assignment(&outcome, 2);
        assert_eq!(row.after, vec![11], "{row:?}");
        // `PPV EVENT 09` has no guide id, so it keeps its own name and matches nothing.
        assert_eq!(row.reason, "Identity family match (exact, score: 100)");
    }

    #[test]
    fn a_watched_stream_is_force_included_when_its_programme_contains_the_channel_name() {
        let mut world = epg_world();
        world.streams[2].tvg_id = "espn.us".to_string();
        let settings = StreamsSettings {
            epg_watch_source_streams: "ESPN".to_string(),
            ..epg_settings(r"^PPV EVENT \d+$")
        };
        let outcome = plan_with_epg(
            &world,
            &settings,
            &[
                programme(7, "WWE SummerSlam"),
                programme(8, "WWE SummerSlam Special"),
            ],
        );
        let row = assignment(&outcome, 2);
        // The watched ESPN stream joins the family hit on the event's own name.
        assert!(row.after.contains(&12), "{row:?}");
        assert!(row.after.contains(&10), "{row:?}");
        // Two groups force-included it: the event channel's own, and the placeholder
        // channel's, whose resolved title is the same event. The plugin counts per call, so
        // this is the same tally it would report over the same pass.
        assert_eq!(outcome.epg.map(|epg| epg.watch_hits), Some(2));
    }

    #[test]
    fn a_watched_stream_needs_every_channel_token_and_two_of_them() {
        let mut world = epg_world();
        world.streams[2].tvg_id = "espn.us".to_string();
        let settings = StreamsSettings {
            epg_watch_source_streams: "ESPN".to_string(),
            ..epg_settings(r"^PPV EVENT \d+$")
        };
        // "SummerSlam 2026 Kickoff" drops the "WWE" token: the real but weaker guide entry the
        // plugin's test says must not match.
        let outcome = plan_with_epg(
            &world,
            &settings,
            &[
                programme(7, "WWE SummerSlam"),
                programme(8, "SummerSlam 2026 Kickoff"),
            ],
        );
        assert!(!assignment(&outcome, 2).after.contains(&12));
        assert_eq!(outcome.epg.map(|epg| epg.watch_hits), Some(0));

        // A single-token channel name is too generic to gate on containment at all
        // ("Max", "Live", "News" would swallow a watched stream's whole day).
        let mut world = world.clone();
        world.channels[1].name = "Max".to_string();
        let outcome = plan_with_epg(
            &world,
            &settings,
            &[
                programme(7, "WWE SummerSlam"),
                programme(8, "WWE SummerSlam Special"),
            ],
        );
        assert!(!assignment(&outcome, 2).after.contains(&12));
    }

    #[test]
    fn the_feature_off_matches_the_names_as_written() {
        let world = epg_world();
        let outcome = plan_match(&world, &StreamsSettings::default());
        assert!(outcome.epg.is_none());
        // Off is the plugin's default: "PPV EVENT 04" matches its own slot stream, not .
        assert_eq!(assignment(&outcome, 1).after, vec![11]);
    }

    #[test]
    fn a_skip_title_and_an_old_next_event_clause_resolve_to_nothing() {
        let world = epg_world();
        let settings = epg_settings(r"^PPV EVENT \d+$");
        let outcome = plan_with_epg(
            &world,
            &settings,
            &[
                programme(7, "No Event Today"),
                programme(8, "Next Event: WWE SummerSlam at 05:00PM on Aug 9"),
            ],
        );
        // Nothing resolved: the channel keeps its own placeholder name.
        assert_eq!(assignment(&outcome, 1).after, vec![11]);
        assert_eq!(outcome.epg.map(|epg| epg.resolved), Some(0));
    }

    #[test]
    fn a_channel_carrying_an_inline_schedule_is_cleaned_before_matching() {
        // Without the schedule strip, the trailing "| Monday @ 5" dilutes the score against the
        // resolved title and its stray digit trips the channel-has-numbers guard.
        let world = World {
            channels: vec![Channel {
                id: 5,
                name: "WWE SummerSlam | Monday @ 5".to_string(),
                channel_number: Some(json!(5)),
                channel_group_id: Some(TVE),
                streams: Vec::new(),
                ..Channel::default()
            }],
            streams: vec![stream(30, "WWE SummerSlam", Some(1), Some(TVE))],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        };
        let settings = epg_settings(r"^PPV EVENT \d+$");
        let outcome = plan_with_epg(&world, &settings, &[]);
        assert_eq!(assignment(&outcome, 5).after, vec![30]);
    }

    #[tokio::test]
    async fn nothing_is_asked_of_the_guide_when_no_name_can_use_it() {
        // The feature is on but every name is a real one: no entry is needed, so the run must
        // not spend a request on the guide at 50k streams.
        let mut world = epg_world();
        world.channels[0].name = "ESPN2".to_string();
        world.channels[0].epg_data_id = Some(7);
        world.channels[0].streams = vec![12];
        world.streams = vec![crosstalk_core::dispatcharr::Stream {
            id: 12,
            name: "ESPN2".to_string(),
            m3u_account: Some(1),
            channel_group: Some(TVE),
            ..Stream::default()
        }];
        let fixture = ServerFixture::start_with_programmes(&world, Vec::new()).await;
        let store = fixture.store();
        let context = fixture.context_for_run("match_preview", &store, true);
        let settings = epg_settings(r"^PPV EVENT \d+$");
        run(&crate::StreamsApp::new(), &context, &settings, true)
            .await
            .expect("preview");
        let bodies = fixture.bodies("POST", "/api/epg/current-programs/").await;
        assert!(bodies.is_empty(), "{bodies:?}");
    }

    #[tokio::test]
    async fn an_unreadable_guide_degrades_to_matching_the_names_as_written() {
        // The plugin treats every EPG lookup failure as "no title" and proceeds; a run that
        // refused the whole match over one auxiliary read would be worse than the plugin.
        let mut world = epg_world();
        world.channels[0].streams = vec![11];
        let fixture = ServerFixture::start_with_epg_error(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run("match_preview", &store, true);
        let settings = epg_settings(r"^PPV EVENT \d+$");
        let outcome = run(&crate::StreamsApp::new(), &context, &settings, true)
            .await
            .expect("preview still runs");
        // The event channel still matches by its own name; the placeholder's group matched
        // only its own slot stream, so it is unchanged and the run is not refused.
        assert_eq!(outcome.counts.get("changes"), Some(&1));
        assert!(
            outcome.summary.contains("Dry run complete"),
            "{}",
            outcome.summary
        );
        let report = std::fs::read_to_string(outcome.report_path.expect("report")).expect("read");

        assert!(
            report.contains("current programmes could not be read"),
            "the report says the guide was unreadable"
        );
    }

    #[tokio::test]
    async fn an_unreadable_timezone_degrades_to_the_host_date_and_says_so() {
        // The settings read is auxiliary, like the guide read: a 500 there must not refuse
        // the match, but the report must admit the date gate ran on the host's date.
        let mut world = epg_world();
        world.channels[0].streams = vec![11];
        let fixture = ServerFixture::start_with_timezone_error(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run("match_preview", &store, true);
        let settings = epg_settings(r"^PPV EVENT \d+$");
        let outcome = run(&crate::StreamsApp::new(), &context, &settings, true)
            .await
            .expect("preview still runs");
        assert_eq!(outcome.counts.get("changes"), Some(&1));
        let report = std::fs::read_to_string(outcome.report_path.expect("report")).expect("read");
        assert!(
            report.contains("host's date"),
            "the report admits the fallback: {report}"
        );
    }

    #[tokio::test]
    async fn the_timezone_is_read_once_per_run_and_only_when_the_epg_path_is_on() {
        // One `GET /core/settings/` per run, and only when placeholder matching is on:
        // with the feature off the date gate never runs, so the run must not spend the
        // request at all.
        let mut world = epg_world();
        world.channels[0].streams = vec![11];
        let fixture = ServerFixture::start_with_programmes(&world, Vec::new()).await;
        let store = fixture.store();
        let context = fixture.context_for_run("match_preview", &store, true);
        run(
            &crate::StreamsApp::new(),
            &context,
            &StreamsSettings::default(),
            true,
        )
        .await
        .expect("preview");
        assert_eq!(
            fixture.request_count("GET", "/api/core/settings/").await,
            0,
            "the date gate is off, so no timezone is read"
        );

        let fixture = ServerFixture::start_with_programmes(&world, Vec::new()).await;
        let store = fixture.store();
        let context = fixture.context_for_run("match_preview", &store, true);
        run(
            &crate::StreamsApp::new(),
            &context,
            &epg_settings(r"^PPV EVENT \d+$"),
            true,
        )
        .await
        .expect("preview");
        assert_eq!(
            fixture.request_count("GET", "/api/core/settings/").await,
            1,
            "the zone is read once per run, not once per lookup"
        );
    }

    #[tokio::test]
    async fn a_run_reads_the_placeholder_titles_in_one_batched_request() {
        let mut world = epg_world();
        world.channels[0].streams = vec![11];
        let fixture =
            ServerFixture::start_with_programmes(&world, vec![programme(7, "WWE SummerSlam")])
                .await;
        let store = fixture.store();
        let context = fixture.context_for_run("match_preview", &store, true);
        let settings = epg_settings(r"^PPV EVENT \d+$");
        let outcome = run(&crate::StreamsApp::new(), &context, &settings, true)
            .await
            .expect("preview");
        assert!(
            outcome.summary.contains("Would update"),
            "{}",
            outcome.summary
        );
        // One request for the entries the run can reach, and only the ones it can reach: the
        // channel's epg_data_id and the placeholder stream's tvg_id both point at entry 7.
        let bodies = fixture.bodies("POST", "/api/epg/current-programs/").await;
        assert_eq!(bodies.len(), 1, "{bodies:?}");
        assert_eq!(bodies[0], json!({"epg_data_ids": [7]}));
    }

    /// A synthetic world of `channels` channels and 2.5×`channels` streams over 100 accounts
    /// (two provider feeds each, a custom fallback on every fourth, and a quarter as much
    /// noise).
    ///
    /// The names are the shape a real lineup has: a brand word, a channel number and a quality
    /// tag, with two or three feeds per channel plus noise. That is the bench the plan asks
    /// for (20k channels / 50k streams / 100 accounts); the small version is the always-on
    /// guard.
    fn synthetic_world(channels: usize) -> World {
        const BRANDS: [&str; 40] = [
            "Sports",
            "News",
            "Movies",
            "Kids",
            "Music",
            "Docs",
            "Comedy",
            "Drama",
            "Hits",
            "Plus",
            "Gold",
            "Extra",
            "Max",
            "One",
            "Two",
            "World",
            "Local",
            "City",
            "Travel",
            "Food",
            "Home",
            "Auto",
            "Speed",
            "Racing",
            "Golf",
            "Tennis",
            "Fight",
            "Boxing",
            "Wrestling",
            "Anime",
            "Latino",
            "Rock",
            "Pop",
            "Jazz",
            "Classic",
            "Retro",
            "Indie",
            "Family",
            "Faith",
            "Shop",
        ];
        let accounts: Vec<M3uAccount> = (1..=100)
            .map(|id| M3uAccount {
                id,
                name: format!("Provider {id}"),
                is_active: true,
                ..M3uAccount::default()
            })
            .collect();
        let mut world = World {
            m3u_accounts: accounts,
            groups: groups(),
            ..World::default()
        };
        let mut stream_id = 1_000_000;
        for index in 0..channels {
            let brand = BRANDS[index % BRANDS.len()];
            let number = index / BRANDS.len() + 1;
            let name = format!("{brand} {number}");
            let mut ids = Vec::new();
            for feed in 0..2 {
                ids.push(stream_id);
                world.streams.push(Stream {
                    id: stream_id,
                    name: if feed == 0 {
                        format!("{name} HD")
                    } else {
                        format!("{name} 4K")
                    },
                    m3u_account: Some((index % 100 + 1) as i64),
                    channel_group: Some(TVE),
                    stream_stats: json!({"width": 1920, "height": 1080, "source_fps": 60}),
                    ..Stream::default()
                });
                stream_id += 1;
            }
            // Every fourth channel carries a third, custom fallback.
            if index % 4 == 0 {
                ids.push(stream_id);
                world.streams.push(Stream {
                    id: stream_id,
                    name: format!("Fallback {}", index),
                    m3u_account: Some(9),
                    is_custom: true,
                    stream_stats: json!({}),
                    ..Stream::default()
                });
                stream_id += 1;
            }
            world.channels.push(Channel {
                id: index as i64 + 1,
                name,
                channel_number: Some(json!(index as i64 + 1)),
                channel_group_id: Some(TVE),
                streams: ids,
                ..Channel::default()
            });
        }
        // Noise: streams that match nothing, which the index must still reject cheaply.
        for index in 0..channels / 4 {
            world.streams.push(Stream {
                id: stream_id,
                name: format!("Unrelated Feed {}", index),
                m3u_account: Some((index % 100 + 1) as i64),
                channel_group: Some(CABLE),
                ..Stream::default()
            });
            stream_id += 1;
        }
        world
    }

    /// The always-on scale guard: a few hundred channels, so an accidental `O(channels ×
    /// streams)` loop (a full pool scan per group) shows up as a timeout rather than as a slow
    /// production run.
    #[test]
    fn matching_a_thousand_channels_of_two_streams_plans_quickly() {
        let world = synthetic_world(1_000);
        let started = std::time::Instant::now();
        let result = plan_match(&world, &StreamsSettings::default());
        let elapsed = started.elapsed();
        assert_eq!(result.channels, 1_000);
        assert_eq!(
            result.write_count(),
            0,
            "every channel already holds its list"
        );
        // Generous: this runs in a DEBUG build.
        let bound = std::time::Duration::from_secs(60);
        assert!(
            elapsed < bound,
            "planning {} channels took {elapsed:?}, over the {bound:?} bound",
            world.channels.len()
        );
        eprintln!(
            "match bench (guard): 1000 channels, {} streams, {:?}",
            world.streams.len(),
            elapsed
        );
    }

    /// The plan's own scale target: 20k channels, 50k streams, 100 accounts.
    ///
    /// Ignored because a DEBUG build takes seconds: what makes it fast is that every group
    /// starts from the name index instead of the stream list, and every per-stream decision
    /// (normalisation, quality key, zone) is made once per stream per run. Run it with:
    ///
    /// ```text
    /// cargo test --release -p crosstalk-streams --lib -- --ignored matching_twenty_thousand
    /// ```
    #[test]
    #[ignore = "20k channels / 50k streams: seconds in a debug build, well under one in release"]
    fn matching_twenty_thousand_channels_and_fifty_thousand_streams_stays_fast() {
        let world = synthetic_world(20_000);
        assert_eq!(world.channels.len(), 20_000);
        assert_eq!(world.streams.len(), 50_000);
        let started = std::time::Instant::now();
        let result = plan_match(&world, &StreamsSettings::default());
        let elapsed = started.elapsed();
        assert_eq!(result.channels, 20_000);
        let bound = std::time::Duration::from_secs(120);
        assert!(
            elapsed < bound,
            "planning {} channels took {elapsed:?}, over the {bound:?} bound",
            world.channels.len()
        );
        eprintln!(
            "match bench: 20k channels, {} streams, {} write(s), {:?}",
            world.streams.len(),
            result.write_count(),
            elapsed
        );
    }

    // ---------------------------------------------------------------------------------
    // The `[ai.verify]` gate (step 9, task "streams")
    // ---------------------------------------------------------------------------------

    /// A decider that picks the first offered candidate at a fixed yes-probability.
    struct FixedDecider {
        probability: f64,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        tasks: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }

    #[async_trait::async_trait]
    impl crosstalk_core::ai::Decider for FixedDecider {
        fn provider(&self) -> &str {
            "typesafe"
        }

        fn calibrated(&self) -> bool {
            true
        }

        async fn decide(
            &self,
            request: &crosstalk_core::ai::DecisionRequest,
        ) -> Result<crosstalk_core::ai::DecisionResponse> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.tasks.lock().expect("tasks").push(
                serde_json::to_value(request)
                    .ok()
                    .and_then(|value| value.get("task").and_then(|t| t.as_str().map(String::from))),
            );
            let mut answers = std::collections::BTreeMap::new();
            if let Some((id, _)) = request
                .questions
                .iter()
                .find(|(id, _)| id.starts_with("same_") && id.as_str() != "same_network")
            {
                answers.insert(
                    "best".to_string(),
                    crosstalk_core::ai::Answer::Choice {
                        choice: id.replacen("same_", "candidate_", 1),
                        confidence: 0.9,
                        probabilities: Default::default(),
                    },
                );
                answers.insert(
                    id.clone(),
                    crosstalk_core::ai::Answer::Noul {
                        noul: self.probability,
                    },
                );
            }
            Ok(crosstalk_core::ai::DecisionResponse {
                model: "stub".to_string(),
                answers,
                input_tokens: 10,
                output_tokens: 2,
                latency_ms: 1,
                calibrated: true,
            })
        }
    }

    struct Gate {
        outcome: MatchOutcome,
        store: Store,
        run_id: i64,
        calls: usize,
        tasks: Vec<Option<String>>,
        result: Result<()>,
    }

    /// One plan + gate over `world`: the `[ai.verify]` policy on, a stub model answering
    /// `probability`, the run recorded in an in-memory store.
    async fn gate_run(
        world: World,
        settings: StreamsSettings,
        probability: f64,
        chain: bool,
        cancelled: bool,
    ) -> Gate {
        let mut config = crosstalk_core::config::CrosstalkConfig::default();
        config.ai.verify = AiVerifyConfig {
            enabled: true,
            provider: "typesafe".to_string(),
            model: "stub".to_string(),
            ..AiVerifyConfig::default()
        };
        if chain {
            config.ai.verify.chain = vec![crosstalk_core::config::AiChainEntry {
                provider: "typesafe".to_string(),
                model: "stub".to_string(),
            }];
        }
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run("streams", "match_preview", true)
            .expect("run");
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(cancelled));
        let context = JobContext::new(
            Arc::new(config),
            Arc::new(crosstalk_core::jobs::VecSink::new()),
        )
        .for_run("streams", "match_preview", run_id, store.clone(), flag);
        let options = Arc::new(MatchOptions::with_database(
            &settings,
            &MatchingConfig::default(),
            Some(ChannelDatabase::empty()),
        ));
        let throughput: Arc<dyn ThroughputSource> =
            Arc::new(quality::MemoryThroughputSource::new());
        let mut outcome = plan(
            &world,
            &ScopeConfig::default(),
            &settings,
            &options,
            throughput.as_ref(),
        )
        .expect("plan");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let tasks = Arc::new(std::sync::Mutex::new(Vec::new()));
        let service = crosstalk_core::ai::AiService::with_decider(
            "typesafe",
            Arc::new(FixedDecider {
                probability,
                calls: Arc::clone(&calls),
                tasks: Arc::clone(&tasks),
            }),
        );
        let scoped = scope_channels(&world, &ScopeConfig::default()).expect("scope");
        let result = apply_ai_gate(
            &mut outcome,
            &world,
            &scoped.channels,
            &settings,
            &options,
            &throughput,
            &context,
            Some(&service),
        )
        .await;
        let tasks = tasks.lock().expect("tasks").clone();
        Gate {
            outcome,
            store,
            run_id,
            calls: calls.load(std::sync::atomic::Ordering::SeqCst),
            tasks,
            result,
        }
    }

    /// A confident channel first, then a borderline one: `Sky Cinema Premiere` scores 90
    /// (substring) against its only stream, inside the verify band and under the cut 95.
    fn borderline_world() -> World {
        World {
            channels: vec![
                channel(1, "ESPN", vec![], 1),
                channel(2, "Sky Cinema Premiere", vec![], 2),
            ],
            streams: vec![
                stream(10, "ESPN", Some(1), Some(TVE)),
                stream(11, "Sky Cinema Premiere HD 2", Some(1), Some(TVE)),
            ],
            m3u_accounts: accounts(),
            groups: groups(),
            ..World::default()
        }
    }

    #[tokio::test]
    async fn the_cut_alone_never_attaches_a_borderline_stream() {
        let outcome = plan_match(&borderline_world(), &StreamsSettings::default());
        assert!(assignment(&outcome, 2).after.is_empty());
        assert!(assignment(&outcome, 1).after.contains(&10));
    }

    #[tokio::test]
    async fn a_model_accept_attaches_the_borderline_family_and_is_audited_on_its_row() {
        let gate = gate_run(
            borderline_world(),
            StreamsSettings::default(),
            0.95,
            false,
            false,
        )
        .await;
        gate.result.expect("gate");
        assert_eq!(gate.calls, 1, "only the borderline group reaches the model");
        let row = assignment(&gate.outcome, 2);
        assert_eq!(row.after, vec![11]);
        assert!(row.reason.starts_with("AI-accepted"), "{}", row.reason);
        assert!(row.will_write());
        // The confident group is untouched.
        assert_eq!(assignment(&gate.outcome, 1).after, vec![10]);
        // One audit row, on the REPORT row of the borderline channel (row 1), not on
        // the group ordinal's or the confident channel's.
        assert_eq!(gate.store.ai_decision_count(gate.run_id).expect("count"), 1);
        let row_of = gate
            .outcome
            .assignments
            .iter()
            .position(|assignment| assignment.channel_id == 2)
            .expect("row") as i64;
        assert!(gate
            .store
            .ai_decision(gate.run_id, row_of)
            .expect("load")
            .is_some());
        // No chain configured: no task prefix (guide's rule).
        assert_eq!(gate.tasks, vec![None]);
    }

    #[tokio::test]
    async fn a_configured_chain_asks_with_the_streams_task() {
        let gate = gate_run(
            borderline_world(),
            StreamsSettings::default(),
            0.95,
            true,
            false,
        )
        .await;
        gate.result.expect("gate");
        assert_eq!(gate.tasks, vec![Some("streams".to_string())]);
    }

    #[tokio::test]
    async fn a_model_that_does_not_accept_attaches_nothing_but_is_audited() {
        let gate = gate_run(
            borderline_world(),
            StreamsSettings::default(),
            0.10,
            false,
            false,
        )
        .await;
        gate.result.expect("gate");
        assert_eq!(gate.calls, 1);
        assert!(assignment(&gate.outcome, 2).after.is_empty());
        assert!(!assignment(&gate.outcome, 2).will_write());
        assert_eq!(gate.store.ai_decision_count(gate.run_id).expect("count"), 1);
    }

    #[tokio::test]
    async fn a_cancelled_run_asks_nothing() {
        let gate = gate_run(
            borderline_world(),
            StreamsSettings::default(),
            0.95,
            false,
            true,
        )
        .await;
        assert!(gate.result.is_err(), "the gate stops on the cancel flag");
        assert_eq!(gate.calls, 0);
        assert!(assignment(&gate.outcome, 2).after.is_empty());
    }

    #[tokio::test]
    async fn an_accepted_family_never_carries_a_stream_the_country_filter_proves_foreign() {
        // The stream's GROUP says UK (no marker in its name, so the engine's own country
        // rule keeps it); the channel's group says US. The cut path drops it, and so must
        // the accept path.
        let world = World {
            channels: vec![Channel {
                channel_group_id: Some(200),
                ..channel(2, "Sky Cinema Premiere", vec![], 2)
            }],
            streams: vec![
                stream(11, "Sky Cinema Premiere HD 2", Some(1), Some(201)),
                stream(12, "Sky Cinema Premiere HD 2", Some(2), Some(200)),
            ],
            m3u_accounts: accounts(),
            groups: vec![
                ChannelGroup {
                    id: 200,
                    name: "US: Movies".to_string(),
                },
                ChannelGroup {
                    id: 201,
                    name: "UK: Movies".to_string(),
                },
            ],
            ..World::default()
        };
        let settings = StreamsSettings {
            restrict_matching_to_country: true,
            ..StreamsSettings::default()
        };
        let gate = gate_run(world, settings, 0.95, false, false).await;
        gate.result.expect("gate");
        assert_eq!(gate.calls, 1);
        assert_eq!(assignment(&gate.outcome, 2).after, vec![12]);
    }

    // ---------------------------------------------------------------------------------
    // What an AI request may carry (C-S4)
    // ---------------------------------------------------------------------------------

    /// What must never reach a model or its audit trail: the parts of a provider URL that
    /// are credentials, the account's name, the provider's key, and any URL at all.
    const AI_MARKERS: [&str; 6] = [
        "USERMARK",
        "PASSMARK",
        "TOKENMARK",
        "SECRET-ACCT",
        "KEYMARK",
        "://",
    ];

    const SECRET_STREAM_URL: &str =
        "http://USERMARK:PASSMARK@provider.test/live/USERMARK/PASSMARK/1.ts?token=TOKENMARK";

    /// Serialises every request before passing it on, as a provider would receive it.
    struct RecordingDecider {
        inner: FixedDecider,
        seen: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl crosstalk_core::ai::Decider for RecordingDecider {
        fn provider(&self) -> &str {
            crosstalk_core::ai::Decider::provider(&self.inner)
        }

        fn calibrated(&self) -> bool {
            crosstalk_core::ai::Decider::calibrated(&self.inner)
        }

        async fn decide(
            &self,
            request: &crosstalk_core::ai::DecisionRequest,
        ) -> Result<crosstalk_core::ai::DecisionResponse> {
            self.seen
                .lock()
                .expect("seen")
                .push(serde_json::to_string(request).expect("a request serialises"));
            crosstalk_core::ai::Decider::decide(&self.inner, request).await
        }
    }

    #[tokio::test]
    async fn an_ai_request_from_the_stream_match_carries_no_secret() {
        // The borderline world, with every stream on a credentialed URL and every account
        // named and pointed at one: none of it may reach the model or the audit trail.
        let mut world = borderline_world();
        for stream in &mut world.streams {
            stream.url = Some(SECRET_STREAM_URL.to_string());
        }
        for account in &mut world.m3u_accounts {
            account.name = format!("SECRET-ACCT {}", account.id);
            account.server_url = Some(SECRET_STREAM_URL.to_string());
        }
        let settings = StreamsSettings::default();
        let mut config = crosstalk_core::config::CrosstalkConfig::default();
        config.ai.verify = AiVerifyConfig {
            enabled: true,
            provider: "typesafe".to_string(),
            model: "stub".to_string(),
            ..AiVerifyConfig::default()
        };
        config.ai.providers.insert(
            "typesafe".to_string(),
            crosstalk_core::config::AiProviderConfig {
                api_key: "KEYMARK".to_string(),
                ..crosstalk_core::config::AiProviderConfig::default()
            },
        );
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run("streams", "match_preview", true)
            .expect("run");
        let context = JobContext::new(
            Arc::new(config),
            Arc::new(crosstalk_core::jobs::VecSink::new()),
        )
        .for_run(
            "streams",
            "match_preview",
            run_id,
            store.clone(),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        let options = Arc::new(match_options(&settings));
        let throughput: Arc<dyn ThroughputSource> =
            Arc::new(quality::MemoryThroughputSource::new());
        let mut outcome = plan(
            &world,
            &ScopeConfig::default(),
            &settings,
            &options,
            throughput.as_ref(),
        )
        .expect("plan");
        let recorder = Arc::new(RecordingDecider {
            inner: FixedDecider {
                probability: 0.95,
                calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
            },
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let service = crosstalk_core::ai::AiService::with_decider("typesafe", recorder.clone());
        let scoped = scope_channels(&world, &ScopeConfig::default()).expect("scope");
        apply_ai_gate(
            &mut outcome,
            &world,
            &scoped.channels,
            &settings,
            &options,
            &throughput,
            &context,
            Some(&service),
        )
        .await
        .expect("gate");

        let mut sent = recorder.seen.lock().expect("seen").clone();
        assert!(!sent.is_empty(), "the borderline channel reached the model");
        // And what was stored about it.
        let stored: Vec<String> = (0..64)
            .filter_map(|ordinal| store.ai_decision(run_id, ordinal).expect("load"))
            .flat_map(|decision| {
                [decision.request, decision.response, decision.attempts]
                    .map(|value| value.to_string())
            })
            .collect();
        assert!(!stored.is_empty(), "the ask was audited");
        sent.extend(stored);
        for text in &sent {
            for marker in AI_MARKERS {
                assert!(!text.contains(marker), "{marker} in {text}");
            }
        }
    }
}
