//! The matching pipeline: alias, exact, substring, token-sort fuzzy, plus structural
//! scoring and the guards that stop near-identical names from matching.
//!
//! Port of `FuzzyMatcher.match_all_streams`, `alias_match`, `_channel_number_boost`,
//! `precompute_normalizations` and `Plugin._find_best_epg_match`'s scoring half.
//!
//! # Shape of the port
//!
//! Python re-normalises the candidate list on every query and memoises inside the matcher
//! instance. Here the candidate side is normalised once into a [`CandidateIndex`], which
//! is then shared read-only across threads; the query side is computed per call. That is
//! the same work, minus the repeated normalisation, and it is what makes the per-channel
//! loop safe to run in parallel.
//!
//! `_find_best_epg_match` interleaves scoring with a database check (does this EPG entry
//! have programme data?). That check is IO, so it is not here: this module returns the
//! ranked candidates and the caller walks them, which is the same order the plugin walks.

use crate::alias::AliasMap;
use crate::callsign::{
    extract_callsign_with_confidence, extract_location, network_keyword, normalize_callsign,
    KnownCallsigns, Location,
};
use crate::normalize::{NormalizeFlags, Normalizer};
use crate::pyre::{py_split_whitespace, pythonize};
use crate::similarity::{calculate_similarity_cutoff, length_scaled_threshold};
use crate::tokens::{has_token_overlap, is_digit, process_string_for_matching, trailing_number};
use std::sync::{Arc, OnceLock};

/// Tunables that are identical to the plugin's constants. Kept in one struct so a future
/// opt-in behaviour change is additive rather than a rewrite.
#[derive(Debug, Clone, Copy)]
pub struct MatchOptions {
    /// `FUZZY_MATCH_THRESHOLD` in the plugin, 85. Scores below this are dropped.
    pub match_threshold: u8,
}

impl Default for MatchOptions {
    fn default() -> Self {
        Self {
            match_threshold: 85,
        }
    }
}

/// How a candidate scored. The plugin records these as strings in its CSV.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchMethod {
    Alias,
    Exact,
    Substring,
    /// Token-sort Levenshtein, carrying its percentage.
    Fuzzy(u8),
    /// A callsign anchor floored the score.
    Callsign,
}

impl MatchMethod {
    /// The label the plugin writes into `match_method`.
    pub fn label(&self) -> String {
        match self {
            MatchMethod::Alias => "alias".to_string(),
            MatchMethod::Exact => "exact".to_string(),
            MatchMethod::Substring => "substring".to_string(),
            MatchMethod::Fuzzy(score) => format!("fuzzy ({score})"),
            MatchMethod::Callsign => "callsign".to_string(),
        }
    }

    /// The coarse label the plugin puts in a report: aliases are distinguished, every
    /// other stage is `Fuzzy`.
    pub fn component(&self) -> &'static str {
        match self {
            MatchMethod::Alias => "Alias",
            _ => "Fuzzy",
        }
    }
}

/// The accumulator `match_all_streams` uses, mirroring Python's plain `dict`.
///
/// Order matters: the plugin's final sort is stable, so ties come out in insertion order,
/// which is "aliases first (already sorted), then candidate order, then callsign rescues".
/// A `HashMap` here would shuffle tied results between runs and could change which entry a
/// caller picks first.
#[derive(Default)]
struct MatchSet {
    /// Candidates in insertion order; `None` marks a removed entry, which keeps the
    /// positions of the survivors exactly as Python's dict does after a `pop`.
    slots: Vec<Option<(usize, u8, MatchMethod)>>,
    position: std::collections::HashMap<usize, usize>,
}

impl MatchSet {
    fn contains(&self, candidate: usize) -> bool {
        self.position.contains_key(&candidate)
    }

    /// Insert, or raise the score of an existing entry in place (keeping its position).
    fn insert_if_better(&mut self, candidate: usize, score: u8, method: MatchMethod) {
        match self
            .position
            .get(&candidate)
            .and_then(|pos| self.slots[*pos].as_mut())
        {
            Some(entry) => {
                if score > entry.1 {
                    entry.1 = score;
                    entry.2 = method;
                }
            }
            None => {
                self.position.insert(candidate, self.slots.len());
                self.slots.push(Some((candidate, score, method)));
            }
        }
    }

    /// Replace the entry outright, as Python's `all_matches[x] = (...)` does.
    fn set(&mut self, candidate: usize, score: u8, method: MatchMethod) {
        match self
            .position
            .get(&candidate)
            .and_then(|pos| self.slots[*pos].as_mut())
        {
            Some(entry) => {
                entry.1 = score;
                entry.2 = method;
            }
            None => {
                self.position.insert(candidate, self.slots.len());
                self.slots.push(Some((candidate, score, method)));
            }
        }
    }

    fn get_score(&self, candidate: usize) -> Option<u8> {
        self.position
            .get(&candidate)
            .and_then(|pos| self.slots[*pos].as_ref())
            .map(|entry| entry.1)
    }

    fn remove(&mut self, candidate: usize) {
        if let Some(pos) = self.position.remove(&candidate) {
            self.slots[pos] = None;
        }
    }

    fn iter(&self) -> impl Iterator<Item = &(usize, u8, MatchMethod)> {
        self.slots.iter().flatten()
    }

    fn retain(&mut self, mut keep: impl FnMut(usize) -> bool) {
        for slot in self.slots.iter_mut() {
            if let Some(entry) = slot {
                if !keep(entry.0) {
                    self.position.remove(&entry.0);
                    *slot = None;
                }
            }
        }
    }
}

/// One scored candidate from [`Matcher::match_all_streams`], ordered best first.
#[derive(Debug, Clone)]
pub struct Ranked {
    /// Position in the [`CandidateIndex`], so the caller can look up its own record.
    pub index: usize,
    pub score: u8,
    pub method: MatchMethod,
}

/// One candidate after structural and fuzzy scoring, ordered best first.
#[derive(Debug, Clone)]
pub struct Scored {
    pub index: usize,
    pub score: u8,
    /// The contributing signals, in the plugin's wording, joined by ` + ` when reported.
    pub components: Vec<&'static str>,
}

impl Scored {
    /// `"Callsign + State"`, exactly as the plugin's CSV shows it.
    pub fn method_label(&self) -> String {
        if self.components.is_empty() {
            "None".to_string()
        } else {
            self.components.join(" + ")
        }
    }
}

/// The candidate side of the match, normalised once and shared read-only.
#[derive(Debug)]
pub struct CandidateIndex {
    names: Vec<String>,
    normalized_lower: Vec<Option<String>>,
    normalized_nospace: Vec<Option<String>>,
    processed: Vec<Option<String>>,
    callsign: Vec<Option<String>>,
    callsign_high_confidence: Vec<bool>,
    location: Vec<Location>,
    /// Whether the ORIGINAL name carries a `+N` time-shift marker. Precomputed because
    /// the guard runs once per candidate per query, which is the hot loop.
    is_shift: Vec<bool>,
    /// Digit tokens of the normalized name, precomputed for the digit-token guard.
    digit_tokens: Vec<Vec<String>>,
    /// Index of the first candidate carrying this name. The plugin keys its results by
    /// NAME, so a duplicate name can only ever produce one entry; this is that key.
    first_of_name: Vec<usize>,
}

/// Collapse the separators the plugin ignores for its no-space comparison: any
/// whitespace, `&` and `-`.
fn letters_only_re() -> &'static fancy_regex::Regex {
    static RE: OnceLock<fancy_regex::Regex> = OnceLock::new();
    RE.get_or_init(|| fancy_regex::Regex::new(&pythonize(r"[\s&\-]+")).expect("static regex"))
}

fn nospace(text: &str) -> String {
    letters_only_re().replace_all(text, "").into_owned()
}

/// `+1`/`+2` time-shift markers in the ORIGINAL name. `\d{1,2}` excludes brand `+`
/// (`Discovery+`, `Disney+`).
fn plus_shift_re() -> &'static fancy_regex::Regex {
    static RE: OnceLock<fancy_regex::Regex> = OnceLock::new();
    RE.get_or_init(|| {
        fancy_regex::Regex::new(&pythonize(r"\+\s{0,2}\d{1,2}\b")).expect("static regex")
    })
}

fn is_time_shift(name: &str) -> bool {
    plus_shift_re().is_match(name).unwrap_or(false)
}

fn lower(text: &str) -> String {
    // Python's `str.lower()` and Rust's are both full case mappings; the case that
    // matters here (Turkish `I`) is handled by the same mapping in both.
    text.to_lowercase()
}

impl CandidateIndex {
    /// Normalise a whole candidate list once.
    ///
    /// Names that normalise to empty or to a single character are stored as `None`,
    /// because the plugin skips them at match time (`if not candidate_lower`).
    ///
    /// No ignored tags are applied here, and that is not an oversight: the plugin warms
    /// its cache with `precompute_normalizations(names)`, which passes no tags, while the
    /// QUERY is normalised with the caller's tag set. Reproducing that asymmetry matters,
    /// because a tag such as the literal word `quality` is stripped from the query but not
    /// from the candidates.
    pub fn build(names: &[String], normalizer: &Normalizer, known: &KnownCallsigns) -> Self {
        let mut index = Self {
            names: names.to_vec(),
            normalized_lower: Vec::with_capacity(names.len()),
            normalized_nospace: Vec::with_capacity(names.len()),
            processed: Vec::with_capacity(names.len()),
            callsign: Vec::with_capacity(names.len()),
            callsign_high_confidence: Vec::with_capacity(names.len()),
            location: Vec::with_capacity(names.len()),
            is_shift: Vec::with_capacity(names.len()),
            digit_tokens: Vec::with_capacity(names.len()),
            first_of_name: Vec::with_capacity(names.len()),
        };
        let mut seen_names: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        for name in names {
            let normalized = normalizer.normalize_name(name, &[]);
            let chars = normalized.chars().count();
            if chars >= 2 {
                let lowercased = lower(&normalized);
                index.normalized_lower.push(Some(lowercased.clone()));
                index.normalized_nospace.push(Some(nospace(&lowercased)));
                index
                    .processed
                    .push(Some(process_string_for_matching(&normalized)));
            } else {
                index.normalized_lower.push(None);
                index.normalized_nospace.push(None);
                index.processed.push(None);
            }

            let (callsign, high) = extract_callsign_with_confidence(name, known);
            index.callsign.push(callsign);
            index.callsign_high_confidence.push(high);
            index.location.push(extract_location(name));
            index.is_shift.push(is_time_shift(name));
            index.digit_tokens.push(
                index
                    .normalized_lower
                    .last()
                    .and_then(|entry| entry.as_deref())
                    .map(|lowered| {
                        py_split_whitespace(lowered)
                            .filter(|token| is_digit(token))
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default(),
            );
            let next_index = index.first_of_name.len();
            index
                .first_of_name
                .push(*seen_names.entry(name.clone()).or_insert(next_index));
        }

        index
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn name(&self, index: usize) -> &str {
        &self.names[index]
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn callsign(&self, index: usize) -> Option<&str> {
        self.callsign[index].as_deref()
    }

    pub fn location(&self, index: usize) -> &Location {
        &self.location[index]
    }

    fn normalized(&self, index: usize) -> Option<(&str, &str)> {
        match (
            self.normalized_lower[index].as_deref(),
            self.normalized_nospace[index].as_deref(),
        ) {
            (Some(lower), Some(no_space)) => Some((lower, no_space)),
            _ => None,
        }
    }

    fn processed(&self, index: usize) -> Option<&str> {
        self.processed[index].as_deref()
    }

    /// The candidate that represents this name in the results, which is the first one
    /// carrying it.
    fn representative(&self, index: usize) -> usize {
        self.first_of_name[index]
    }
}

/// A configured matcher: flags, aliases, allowlist and threshold.
///
/// `Matcher` is immutable after construction and `Send + Sync`, so one instance can serve
/// every thread in a parallel run. The candidate index is passed in per query rather than
/// stored, because a run can search different candidate sets (auto-match uses the scoped
/// EPG list, heal uses the fallback sources).
#[derive(Debug, Clone)]
pub struct Matcher {
    normalizer: Normalizer,
    alias_map: AliasMap,
    known: Arc<KnownCallsigns>,
    options: MatchOptions,
}

impl Matcher {
    pub fn new(
        flags: NormalizeFlags,
        alias_map: AliasMap,
        known: Arc<KnownCallsigns>,
        options: MatchOptions,
    ) -> Self {
        Self {
            normalizer: Normalizer::new(flags),
            alias_map,
            known,
            options,
        }
    }

    /// A matcher with the plugin's shipped defaults: every tag class ignored, the built-in
    /// alias table, the FCC allowlist and a threshold of 85.
    pub fn with_shipped_defaults() -> Self {
        Self::new(
            NormalizeFlags::default(),
            AliasMap::builtin(),
            Arc::new(KnownCallsigns::shipped().cloned().unwrap_or_default()),
            MatchOptions::default(),
        )
    }

    pub fn normalizer(&self) -> &Normalizer {
        &self.normalizer
    }

    pub fn alias_map(&self) -> &AliasMap {
        &self.alias_map
    }

    pub fn known(&self) -> &KnownCallsigns {
        &self.known
    }

    pub fn options(&self) -> MatchOptions {
        self.options
    }

    /// Install a different allowlist, for example one that also carries the callsigns the
    /// loaded channel databases use.
    pub fn with_known(mut self, known: KnownCallsigns) -> Self {
        self.known = Arc::new(known);
        self
    }

    pub fn normalize(&self, name: &str, user_ignored_tags: &[&str]) -> String {
        self.normalizer.normalize_name(name, user_ignored_tags)
    }

    /// Stage 0: alias-table matching.
    ///
    /// Every alias of the query name is normalised once, then each candidate is compared:
    /// an exact (spaced or space-stripped) hit scores 100, otherwise the best Levenshtein
    /// ratio against a spaced alias is scored with the length-scaled threshold and the
    /// majority token guard. Aliases are the community-curated escapes for names that
    /// scoring cannot join on its own (`MS Now` for `MSNBC`).
    pub fn alias_match(
        &self,
        query: &str,
        index: &CandidateIndex,
        user_ignored_tags: &[&str],
    ) -> Vec<Ranked> {
        let aliases = self.alias_map.aliases_for(query);
        if aliases.is_empty() {
            return Vec::new();
        }

        let mut lookup = std::collections::HashMap::new();
        let mut spaced: Vec<String> = Vec::new();
        for alias in aliases {
            let normalized = self.normalizer.normalize_name(alias, user_ignored_tags);
            if normalized.is_empty() {
                continue;
            }
            let lowered = lower(&normalized);
            lookup.insert(lowered.clone(), ());
            spaced.push(lowered.clone());
            let no_space = nospace(&lowered);
            if no_space != lowered {
                lookup.insert(no_space, ());
            }
        }
        if lookup.is_empty() {
            return Vec::new();
        }

        let threshold_ratio = f64::from(self.options.match_threshold) / 100.0;
        let mut matches = Vec::new();

        for candidate in 0..index.len() {
            let Some((candidate_lower, candidate_nospace)) = index.normalized(candidate) else {
                continue;
            };

            if lookup.contains_key(candidate_lower) || lookup.contains_key(candidate_nospace) {
                matches.push(Ranked {
                    index: candidate,
                    score: 100,
                    method: MatchMethod::Alias,
                });
                continue;
            }

            let mut best_ratio = 0.0;
            let mut best_len = 0;
            let mut best_alias = String::new();
            for alias in &spaced {
                let ratio = calculate_similarity_cutoff(alias, candidate_lower, threshold_ratio);
                if ratio > best_ratio {
                    best_ratio = ratio;
                    best_len = alias.chars().count().min(candidate_lower.chars().count());
                    best_alias = alias.clone();
                }
            }

            let score = (best_ratio * 100.0) as u8;
            let effective_threshold =
                length_scaled_threshold(self.options.match_threshold, best_len);

            if score >= effective_threshold
                && score < 100
                && !has_token_overlap(&best_alias, candidate_lower, 4, true)
            {
                continue;
            }
            if score >= effective_threshold {
                matches.push(Ranked {
                    index: candidate,
                    score,
                    method: MatchMethod::Alias,
                });
            }
        }

        matches.sort_by_key(|entry| std::cmp::Reverse(entry.score));
        matches
    }

    /// Every match, including ones below the threshold.
    ///
    /// Exists so the parity sweep can compare the whole ranking against the Python
    /// implementation rather than only the survivors.
    pub fn match_all_streams_all(
        &self,
        query: &str,
        index: &CandidateIndex,
        user_ignored_tags: &[&str],
    ) -> Vec<Ranked> {
        self.match_all_inner(query, index, user_ignored_tags, 0)
    }

    /// The full pipeline: alias, exact, substring, token-sort fuzzy, callsign anchor and
    /// the regional filter. Returns every candidate at or above the threshold, best first.
    ///
    /// The channel-number boost is not applied: the plugin's EPG path passes
    /// `channel_number=None`, so the boost never fires for these runs. Use
    /// [`Matcher::channel_number_boost`] directly if a caller ever needs it.
    pub fn match_all_streams(
        &self,
        query: &str,
        index: &CandidateIndex,
        user_ignored_tags: &[&str],
    ) -> Vec<Ranked> {
        self.match_all_inner(
            query,
            index,
            user_ignored_tags,
            self.options.match_threshold,
        )
    }

    fn match_all_inner(
        &self,
        query: &str,
        index: &CandidateIndex,
        user_ignored_tags: &[&str],
        min_score: u8,
    ) -> Vec<Ranked> {
        if index.is_empty() {
            return Vec::new();
        }

        let (query_callsign, query_high_confidence) =
            extract_callsign_with_confidence(query, &self.known);
        let query_callsign_normalized = query_callsign.as_deref().map(normalize_callsign);
        let mut callsign_anchored: std::collections::HashSet<usize> =
            std::collections::HashSet::new();

        let mut all_matches = MatchSet::default();

        for ranked in self.alias_match(query, index, user_ignored_tags) {
            all_matches.insert_if_better(
                index.representative(ranked.index),
                ranked.score,
                ranked.method,
            );
        }

        let normalized_query = self.normalizer.normalize_name(query, user_ignored_tags);
        if !normalized_query.is_empty() {
            let query_lower = lower(&normalized_query);
            let query_nospace = nospace(&query_lower);
            let query_trailing = trailing_number(&query_lower);
            let query_digits: Vec<&str> = py_split_whitespace(&query_lower)
                .filter(|token| is_digit(token))
                .collect();
            let query_is_shift = is_time_shift(query);
            let processed_query = process_string_for_matching(&normalized_query);

            for candidate in 0..index.len() {
                let representative = index.representative(candidate);
                if all_matches.contains(representative) {
                    continue;
                }
                let Some((candidate_lower, candidate_nospace)) = index.normalized(candidate) else {
                    continue;
                };

                // Sibling-number guard: only skips when BOTH sides carry a differing
                // trailing number (`HBO 1` vs `HBO 2`).
                if let Some(query_number) = query_trailing {
                    if let Some(candidate_number) = trailing_number(candidate_lower) {
                        if candidate_number != query_number {
                            continue;
                        }
                    }
                }

                // Digit-token guard: reject only on genuine number DISAGREEMENT. A
                // numbered channel against an unnumbered candidate is allowed, because US
                // OTA names carry the broadcast number while the guide entry does not
                // (`ABC 7 (KGO) San Francisco` vs `ABC San Francisco`).
                if !query_digits.is_empty() {
                    let candidate_digits = &index.digit_tokens[candidate];
                    if !candidate_digits.is_empty()
                        && !candidate_digits
                            .iter()
                            .any(|token| query_digits.contains(&token.as_str()))
                    {
                        continue;
                    }
                }

                // Time-shift guard: a `+1` channel only matches `+1` candidates.
                if query_is_shift != index.is_shift[candidate] {
                    continue;
                }

                let mut score: u8 = 0;
                let mut method: Option<MatchMethod> = None;

                // Stage 1: exact after normalisation.
                if query_nospace == candidate_nospace {
                    score = 100;
                    method = Some(MatchMethod::Exact);
                } else {
                    let ratio = calculate_similarity_cutoff(&query_lower, candidate_lower, 0.97);
                    if ratio >= 0.97 {
                        score = (ratio * 100.0) as u8;
                        method = Some(MatchMethod::Exact);
                    }
                }

                // Stage 2: substring, with a length-ratio floor and the majority guard.
                if method.is_none()
                    && (candidate_lower.contains(&query_lower)
                        || query_lower.contains(candidate_lower))
                {
                    let shorter = query_lower
                        .chars()
                        .count()
                        .min(candidate_lower.chars().count());
                    let longer = query_lower
                        .chars()
                        .count()
                        .max(candidate_lower.chars().count());
                    let length_ratio = shorter as f64 / longer as f64;
                    if length_ratio >= 0.75 {
                        let threshold_ratio = f64::from(self.options.match_threshold) / 100.0;
                        let ratio = calculate_similarity_cutoff(
                            &query_lower,
                            candidate_lower,
                            threshold_ratio,
                        );
                        let sub_score = (ratio * 100.0) as u8;
                        let sub_threshold =
                            length_scaled_threshold(self.options.match_threshold, shorter);
                        if sub_score >= sub_threshold
                            && has_token_overlap(&query_lower, candidate_lower, 4, true)
                        {
                            score = sub_score;
                            method = Some(MatchMethod::Substring);
                        }
                    }
                }

                // Stage 3: token-sort Levenshtein.
                if method.is_none() {
                    if let Some(processed_candidate) = index.processed(candidate) {
                        let threshold_ratio = f64::from(self.options.match_threshold) / 100.0;
                        let ratio = calculate_similarity_cutoff(
                            &processed_query,
                            processed_candidate,
                            threshold_ratio,
                        );
                        let fuzzy_score = (ratio * 100.0) as u8;
                        let shorter = processed_query
                            .chars()
                            .count()
                            .min(processed_candidate.chars().count());
                        let fuzzy_threshold =
                            length_scaled_threshold(self.options.match_threshold, shorter);
                        if fuzzy_score >= fuzzy_threshold
                            && has_token_overlap(&processed_query, processed_candidate, 4, true)
                        {
                            score = fuzzy_score;
                            method = Some(MatchMethod::Fuzzy(fuzzy_score));
                        }
                    }
                }

                if let Some(method) = method {
                    if score > 0 {
                        all_matches.insert_if_better(representative, score, method);
                    }
                }
            }
        }

        // Callsign anchor: a shared high-confidence callsign floors the score at 95 and
        // rescues an otherwise-unmatched candidate; a disagreeing one hard-rejects a
        // false positive. Both directions require high confidence on BOTH sides, because a
        // loose mid-name token shaped like a callsign (`WITH`) is not evidence either way.
        if let Some(query_normalized_callsign) = query_callsign_normalized.as_deref() {
            if query_high_confidence {
                for candidate in 0..index.len() {
                    let candidate_callsign = index.callsign(candidate);
                    let candidate_high = index.callsign_high_confidence[candidate];
                    let Some(candidate_callsign) = candidate_callsign else {
                        continue;
                    };
                    if !candidate_high {
                        continue;
                    }
                    let representative = index.representative(candidate);
                    if normalize_callsign(candidate_callsign) == query_normalized_callsign {
                        let replace = match all_matches.get_score(representative) {
                            None => true,
                            Some(score) => score < 95,
                        };
                        if replace {
                            all_matches.set(representative, 95, MatchMethod::Callsign);
                        }
                        callsign_anchored.insert(representative);
                    } else {
                        all_matches.remove(representative);
                    }
                }
            }
        }

        // Regional filter: East must not take a West-only feed, Pacific is the same feed
        // as West, and a regionless query rejects Pacific/West candidates (they default to
        // East). Markers come from the ORIGINAL names, because normalisation strips them.
        let original_lower = lower(query);
        let has = |needle: &str| original_lower.contains(needle);
        let query_has_east = has("east") || contains_abbrev(&original_lower, "e");
        let query_has_west =
            (has("west") && !has("western")) || contains_abbrev(&original_lower, "w");
        let query_has_pacific = has("pacific") || contains_abbrev(&original_lower, "p");

        if query_has_east || query_has_west || query_has_pacific {
            all_matches.retain(|candidate| {
                if callsign_anchored.contains(&candidate) {
                    return true;
                }
                let name = lower(index.name(candidate));
                let candidate_east = name.contains("east");
                let candidate_west = name.contains("west") && !name.contains("western");
                let candidate_pacific = name.contains("pacific");
                let candidate_region = candidate_east || candidate_west || candidate_pacific;

                if query_has_east {
                    !(candidate_west && !candidate_east) && !(candidate_pacific && !candidate_east)
                } else if query_has_west {
                    !(candidate_east && !candidate_west && !candidate_pacific) && candidate_region
                } else {
                    // Pacific query accepts Pacific or West.
                    !(candidate_east && !candidate_pacific && !candidate_west) && candidate_region
                }
            });
        } else if !user_ignored_tags.contains(&"regional") {
            // Regionless query prefers regionless candidates; the filter is dropped when it
            // would eliminate everything, which is why the survivors are counted first.
            let keeps = |candidate: usize| {
                if callsign_anchored.contains(&candidate) {
                    return true;
                }
                let lowered = lower(index.name(candidate));
                !(lowered.contains("pacific")
                    || (lowered.contains("west") && !lowered.contains("western")))
            };
            if all_matches
                .iter()
                .any(|(candidate, _, _)| keeps(*candidate))
            {
                all_matches.retain(keeps);
            }
        }

        // Deterministic tie-break: on equal score, prefer the candidate sharing more
        // ORIGINAL-name tokens with the query.
        let mut results: Vec<(Ranked, usize)> = all_matches
            .iter()
            .filter(|(_, score, _)| *score >= min_score)
            .map(|(candidate, score, method)| {
                (
                    Ranked {
                        index: *candidate,
                        score: *score,
                        method: method.clone(),
                    },
                    ascii_token_overlap(&original_lower, index.name(*candidate)),
                )
            })
            .collect();

        // Stable: equal scores keep insertion order, then the original-token overlap
        // breaks the tie, exactly as the plugin's `sort(key=(score, overlap), reverse)`.
        results.sort_by(|a, b| b.0.score.cmp(&a.0.score).then_with(|| b.1.cmp(&a.1)));
        results.into_iter().map(|(ranked, _)| ranked).collect()
    }

    /// `_channel_number_boost`: five points when a three-or-more-digit channel number
    /// appears in the candidate with clear delimiters.
    pub fn channel_number_boost(&self, candidate: &str, expected: Option<u32>) -> u8 {
        let Some(expected) = expected else {
            return 0;
        };
        let text = expected.to_string();
        if text.len() < 3 {
            return 0;
        }
        let pattern = format!(r"(?:^|[\s\[\(]){}(?:$|[\s\]\)])", regex::escape(&text));
        static CACHE: OnceLock<std::sync::Mutex<std::collections::HashMap<String, regex::Regex>>> =
            OnceLock::new();
        let cache = CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        let mut guard = cache.lock().expect("boost cache");
        let re = guard
            .entry(text.clone())
            .or_insert_with(|| regex::Regex::new(&pattern).expect("static regex"));
        if re.is_match(candidate) {
            5
        } else {
            0
        }
    }

    /// Structural and fuzzy scoring against an index, best first. This is the scoring half
    /// of `_find_best_epg_match`; the caller then walks the list to find the first
    /// candidate that actually has programme data.
    ///
    /// The two scores are computed independently and the better one wins, with a strict
    /// `>` on ties so a structural explanation (`Callsign + State`) is preferred over
    /// `Fuzzy` at the same number. Gating fuzzy on structural being zero was tried in the
    /// plugin and reverted: it capped obvious name-exact matches at 60 whenever a weak
    /// structural signal fired.
    pub fn score_candidates(
        &self,
        channel_name: &str,
        index: &CandidateIndex,
        user_ignored_tags: &[&str],
        exclude: Option<usize>,
    ) -> Vec<Scored> {
        let (channel_callsign, _) = extract_callsign_with_confidence(channel_name, &self.known);
        let channel_location = extract_location(channel_name);
        let channel_network = network_keyword(channel_name);

        let fuzzy: std::collections::HashMap<usize, (u8, MatchMethod)> = self
            .match_all_streams(channel_name, index, user_ignored_tags)
            .into_iter()
            .map(|ranked| (ranked.index, (ranked.score, ranked.method)))
            .collect();

        let mut scored: Vec<(u8, Scored)> = Vec::new();
        for candidate in 0..index.len() {
            if exclude == Some(candidate) {
                continue;
            }

            let mut structural: u8 = 0;
            let mut structural_components: Vec<&'static str> = Vec::new();

            if let Some(channel_callsign) = channel_callsign.as_deref() {
                if index.callsign(candidate).is_some_and(|candidate_callsign| {
                    candidate_callsign.eq_ignore_ascii_case(channel_callsign)
                }) {
                    structural += 50;
                    structural_components.push("Callsign");
                }
            }

            if let Some(state) = channel_location.state.as_deref() {
                let candidate_location = index.location(candidate);
                if candidate_location.state.as_deref() == Some(state) {
                    structural += 30;
                    structural_components.push("State");
                    if let (Some(channel_city), Some(candidate_city)) = (
                        channel_location.city.as_deref(),
                        candidate_location.city.as_deref(),
                    ) {
                        let a = channel_city.to_uppercase();
                        let b = candidate_city.to_uppercase();
                        if a.contains(&b) || b.contains(&a) {
                            structural += 20;
                            structural_components.push("City");
                        }
                    }
                }
            }

            // A bare network keyword is too weak to identify a channel on its own, so it
            // only adds on top of a stronger structural signal.
            if let Some(network) = channel_network.as_deref() {
                if structural > 0 && index.name(candidate).to_uppercase().contains(network) {
                    structural += 10;
                    structural_components.push("Network");
                }
            }

            let (fuzzy_score, fuzzy_components) = match fuzzy.get(&candidate) {
                Some((score, method)) => (*score, vec![method.component()]),
                None => (0, Vec::new()),
            };

            let (score, components) = if fuzzy_score > structural {
                (fuzzy_score, fuzzy_components)
            } else {
                (structural, structural_components)
            };

            if score > 0 {
                // The raw score is kept for sorting: structural signals can total 110,
                // and the plugin sorts before capping, so a 110 structural candidate
                // outranks a 100 fuzzy one. Capping first would silently reorder them.
                scored.push((
                    score,
                    Scored {
                        index: candidate,
                        score,
                        components,
                    },
                ));
            }
        }

        // Stable, so equal scores keep the candidate order, as Python's `list.sort` does.
        scored.sort_by_key(|(raw, _)| std::cmp::Reverse(*raw));
        scored
            .into_iter()
            .map(|(_, mut entry)| {
                entry.score = entry.score.min(100);
                entry
            })
            .collect()
    }
}

/// Runs of ASCII letters and digits, which is what Python's `re.findall(r'[a-z0-9]+', ...)`
/// produces on an already-lowercased name.
fn ascii_tokens(name_lower: &str) -> std::collections::HashSet<String> {
    let mut tokens = std::collections::HashSet::new();
    let mut current = String::new();
    for c in name_lower.chars() {
        if c.is_ascii_alphanumeric() {
            current.push(c);
        } else if !current.is_empty() {
            tokens.insert(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.insert(current);
    }
    tokens
}

/// How many distinct ASCII tokens the candidate shares with the query. Python's
/// deterministic tie-break compares exactly these sets.
fn ascii_token_overlap(query_lower: &str, candidate: &str) -> usize {
    let candidate_lower = lower(candidate);
    let query_tokens = ascii_tokens(query_lower);
    ascii_tokens(&candidate_lower)
        .iter()
        .filter(|token| query_tokens.contains(*token))
        .count()
}

/// `(e)` / `(w)` / `(p)` region abbreviations in an already-lowercased name.
fn contains_abbrev(name: &str, letter: &str) -> bool {
    static CACHE: OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, fancy_regex::Regex>>,
    > = OnceLock::new();
    let cache = CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut guard = cache.lock().expect("abbrev cache");
    let re = guard.entry(letter.to_string()).or_insert_with(|| {
        fancy_regex::Regex::new(&pythonize(&format!(r"\(\s*{letter}\s*\)"))).expect("static regex")
    });
    re.is_match(name).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matcher() -> Matcher {
        Matcher::with_shipped_defaults()
    }

    fn index_for(matcher: &Matcher, names: &[&str]) -> CandidateIndex {
        let owned: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        CandidateIndex::build(&owned, matcher.normalizer(), matcher.known())
    }

    fn best(matcher: &Matcher, query: &str, names: &[&str]) -> Option<(usize, u8, String)> {
        let index = index_for(matcher, names);
        matcher
            .match_all_streams(query, &index, &[])
            .into_iter()
            .next()
            .map(|ranked| (ranked.index, ranked.score, ranked.method.label()))
    }

    #[test]
    fn exact_after_normalisation_scores_100() {
        let m = matcher();
        assert_eq!(
            best(&m, "ESPN HD", &["ESPN", "ESPN2"]),
            Some((0, 100, "exact".into()))
        );
        // Separators are ignored in the no-space comparison.
        assert_eq!(
            best(&m, "C-SPAN 2", &["CSPAN2"]).map(|(_, score, _)| score),
            Some(100)
        );
    }

    #[test]
    fn numbered_siblings_do_not_match() {
        let m = matcher();
        // Both sides numbered and different: rejected outright.
        assert!(best(&m, "Sky Sports News 13", &["Sky Sports News 14"]).is_none());
        // A numbered channel against an unnumbered candidate is ALLOWED by the digit
        // guard; whether it then clears the threshold is a scoring question. This pair
        // does, which is the case the guard exists to protect.
        assert_eq!(
            best(&m, "ABC 7 (KGO) San Francisco HD", &["ABC San Francisco"]),
            Some((0, 89, "fuzzy (89)".into()))
        );
        // Dallas does not, in the plugin either: the extra `5` costs enough.
        assert!(best(&m, "NBC 5 (KXAS) Dallas", &["NBC Dallas"]).is_none());
    }

    #[test]
    fn time_shift_channels_do_not_cross_match() {
        let m = matcher();
        assert!(best(&m, "ITV2 +1", &["ITV2"]).is_none());
        assert!(best(&m, "ITV2", &["ITV2 +1"]).is_none());
        assert!(best(&m, "ITV2 +1", &["ITV2 +1"]).is_some());
        // A brand `+` is not a time shift.
        assert!(best(&m, "Discovery+", &["Discovery+"]).is_some());
    }

    #[test]
    fn divergent_brands_do_not_match() {
        let m = matcher();
        assert!(best(&m, "Sky Cinema Disney", &["Sky Cinema Decades"]).is_none());
    }

    #[test]
    fn callsign_anchor_floors_and_rejects() {
        let m = matcher();
        // A shared high-confidence callsign floors an otherwise weak score at 95.
        let ranked = best(&m, "ABC (WABC)", &["Something Else (WABC)"]).expect("anchored");
        assert_eq!(ranked.1, 95);
        assert_eq!(ranked.2, "callsign");
        // A disagreeing high-confidence callsign hard-rejects even a strong name match.
        assert!(best(&m, "ABC (WABC)", &["ABC (WXYZ)"]).is_none());
    }

    #[test]
    fn regional_filter_keeps_east_and_west_apart() {
        let m = matcher();
        let index = index_for(&m, &["HBO West", "HBO East"]);
        let ranked = m.match_all_streams("HBO East", &index, &[]);
        assert_eq!(index.name(ranked[0].index), "HBO East");

        let index = index_for(&m, ["HBO West"].as_slice());
        let ranked = m.match_all_streams("HBO West", &index, &[]);
        assert_eq!(ranked.len(), 1);
        assert_eq!(index.name(ranked[0].index), "HBO West");
    }

    #[test]
    fn alias_table_is_used_first() {
        let m = matcher();
        for (query, candidate) in [
            ("MSNBC", "MS Now"),
            ("MS Now", "MSNBC"),
            ("NHL Network", "NHL Network HD"),
            ("Weather Channel", "TWC"),
        ] {
            let index = index_for(&m, &[candidate]);
            let ranked = m.match_all_streams(query, &index, &[]);
            assert_eq!(
                (ranked[0].score, ranked[0].method.clone()),
                (100, MatchMethod::Alias),
                "{query:?} -> {candidate:?}"
            );
        }
        // An alias entry that is not in the table gains nothing.
        let index = index_for(&m, &["FNC"]);
        assert!(m
            .match_all_streams("Fox News Channel", &index, &[])
            .is_empty());
    }

    #[test]
    fn structural_callsign_signal_scores_without_a_fuzzy_match() {
        let m = matcher();
        // `KOMO` here is a LOW-confidence callsign (mid-name token), so no anchor floors
        // the score; the fuzzy pipeline cannot reach the threshold either. Only the
        // structural callsign signal is left, which is exactly what it is for.
        let index = index_for(&m, &["KOMO 4 News Seattle"]);
        assert!(m.match_all_streams("KOMO News", &index, &[]).is_empty());
        let scored = m.score_candidates("KOMO News", &index, &[], None);
        assert_eq!(scored.len(), 1);
        assert_eq!(scored[0].components, vec!["Callsign"]);
        assert_eq!(scored[0].score, 50);
        assert_eq!(scored[0].method_label(), "Callsign");
    }

    #[test]
    fn structural_state_and_city_signals_add_up() {
        let m = matcher();
        // State 30 + city 20 = 50. The callsigns differ (`WSIL` vs `WMAQ`), so no
        // callsign points; with a matching callsign the plugin's ceiling is 100.
        let index = index_for(&m, &["ABC - IL Harrisburg (WSIL)"]);
        let scored = m.score_candidates("NBC - IL Harrisburg (WMAQ)", &index, &[], None);
        assert_eq!(scored.len(), 1);
        assert_eq!(scored[0].score, 50);
        assert_eq!(scored[0].components, vec!["State", "City"]);
    }

    #[test]
    fn raw_structural_score_outranks_a_fuzzy_hundred() {
        // Callsign 50 + State 30 + City 20 + Network 10 = 110, which the plugin sorts on
        // BEFORE capping. The first candidate's fuzzy 100 must therefore come second.
        let m = matcher();
        let index = index_for(&m, &["ABC - NY New York", "ABC - NY New York (WABC)"]);
        let scored = m.score_candidates("ABC - NY New York (WABC)", &index, &[], None);
        assert_eq!(scored[0].index, 1);
        assert_eq!(scored[0].score, 100, "reported score is capped");
        assert_eq!(scored[1].index, 0);
        assert_eq!(scored[1].score, 100);
    }

    #[test]
    fn an_empty_city_does_not_score() {
        // `CBS - NY  (WCBS)` yields an empty city, which Python's truthiness test drops:
        // State alone (30), not State + City (50).
        let m = matcher();
        let index = index_for(&m, &["CBS - NY  (WCBS)"]);
        let scored = m.score_candidates("ABC - NY  (WABC)", &index, &[], None);
        assert_eq!(scored.len(), 1);
        assert_eq!(scored[0].components, vec!["State"]);
        assert_eq!(scored[0].score, 30);
    }

    #[test]
    fn tied_scores_keep_insertion_order_then_prefer_shared_tokens() {
        let m = matcher();
        // Two candidates that normalise identically tie on score; the tie-break prefers
        // the one sharing more ASCII tokens with the original query.
        let index = index_for(&m, &["ESPN Alt", "ESPN"]);
        let ranked = m.match_all_streams_all("ESPN", &index, &[]);
        assert_eq!(index.name(ranked[0].index), "ESPN");
        // Reversing the input order must not change that.
        let index = index_for(&m, &["ESPN", "ESPN Alt"]);
        let ranked = m.match_all_streams_all("ESPN", &index, &[]);
        assert_eq!(index.name(ranked[0].index), "ESPN");
    }

    #[test]
    fn excluded_candidate_is_skipped() {
        let m = matcher();
        let index = index_for(&m, &["ESPN"]);
        assert!(m.score_candidates("ESPN", &index, &[], Some(0)).is_empty());
    }

    #[test]
    fn channel_number_boost_needs_three_digits() {
        let m = matcher();
        assert_eq!(m.channel_number_boost("News 101 HD", Some(101)), 5);
        assert_eq!(m.channel_number_boost("ESPN2", Some(2)), 0);
        assert_eq!(m.channel_number_boost("News 101", None), 0);
    }

    #[test]
    fn low_scores_are_dropped() {
        let m = matcher();
        assert!(best(&m, "HBO", &["WeatherNation"]).is_none());
    }
}
