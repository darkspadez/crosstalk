//! JSONL harness for `scripts/parity_sweep.py` and `scripts/parity_sweep_sm.py`.
//!
//! Reads one JSON request per line on stdin, writes one JSON response per line on
//! stdout. A request may carry `"profile": "epg-janitor" | "stream-mapparr"`; it
//! defaults to `epg-janitor`, which is what the original sweep sends.
//!
//! Request kinds:
//!
//! * `normalize` (or a request with no `kind`): `{name, ignore_quality,
//!   ignore_regional, ignore_geographic, ignore_misc, tags}` ->
//!   `{"normalized": ...}`. Flags default to true (the plugins' shipped defaults);
//!   `tags` defaults to `[]`. The profile's structural flags are applied on top.
//! * `callsign`: `{name}` -> `{"callsign": ..., "high_confidence": ...}`, with the
//!   profile's callsign ladder and allowlist.
//! * `location`: `{name}` -> `{"state": ..., "city": ...}`.
//! * `network`: `{name}` -> `{"network": ...}`.
//! * `match`: EPG Janitor: `{query, candidates, tags}` ->
//!   `{"matches": [[name, score, type]]}`, every match including below-threshold ones
//!   (`min_score = 0`). Stream-Mapparr: the same request -> `{"match": [name, score,
//!   type] | null}`, its `fuzzy_match` best single match.
//! * `score`: EPG Janitor: `{query, candidates, tags}` -> `{"scored": [[name, score,
//!   label]]}`, the structural+fuzzy scoring half of `Plugin._find_best_epg_match`
//!   (programme-data check excluded, as in the sweep's transcription).
//!   Stream-Mapparr: `{"best_match": [name, score] | null}`, its `find_best_match`.
//! * `alias`: Stream-Mapparr only: `{query, candidates, tags, alias_country?}` ->
//!   `{"hits": [name, ...]}`, its exact-normalised `alias_lookup`.
//! * `zone`: Stream-Mapparr only: `{name}` -> `{"zone": "EAST"|"WEST"|"DEFAULT"}`.
//! * `country`: Stream-Mapparr only: `{name, overrides_raw?}` -> `{"from_name": ...,
//!   "from_group": ..., "override": ...}`.
//! * `classify`: `{channel, stream}` -> `{"relationship": "SAME"|"FOREIGN"|"UNKNOWN"}`.
//!
//! The EPG Janitor matcher mirrors `scripts/parity_sweep.py`'s Python setup: threshold
//! 85, the built-in alias table, and the shipped FCC callsign allowlist (that sweep never
//! loads channel databases, so no database-derived callsigns are merged). The
//! Stream-Mapparr matcher mirrors its plugin's start-up: all sixteen databases (through
//! the allowlist they contribute) plus `networks.json`, and its own alias table.

use crosstalk_matching::alias::AliasMap;
use crosstalk_matching::callsign::{
    extract_callsign_with_confidence_for, extract_location, network_keyword, KnownCallsigns,
};
use crosstalk_matching::country;
use crosstalk_matching::normalize::{NormalizeFlags, Normalizer};
use crosstalk_matching::pipeline::{CandidateIndex, Matcher};
use crosstalk_matching::profile::MatchProfile;
use std::io::{BufRead, Write};
use std::sync::{Arc, OnceLock};

fn bool_field(value: &serde_json::Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true)
}

fn bool_field_default_false(value: &serde_json::Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn str_field<'a>(value: &'a serde_json::Value, field: &str) -> &'a str {
    value
        .get(field)
        .and_then(|entry| entry.as_str())
        .unwrap_or_default()
}

fn tags_of(value: &serde_json::Value) -> Vec<String> {
    value
        .get("tags")
        .and_then(|tags| tags.as_array())
        .map(|tags| {
            tags.iter()
                .filter_map(|tag| tag.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn candidates_of(value: &serde_json::Value) -> Vec<String> {
    value
        .get("candidates")
        .and_then(|candidates| candidates.as_array())
        .map(|candidates| {
            candidates
                .iter()
                .filter_map(|candidate| candidate.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn profile_of(request: &serde_json::Value) -> MatchProfile {
    request
        .get("profile")
        .and_then(|profile| profile.as_str())
        .and_then(MatchProfile::from_name)
        .unwrap_or(MatchProfile::EpgJanitor)
}

/// EPG Janitor's shipped FCC allowlist, parsed once.
fn janitor_known() -> Arc<KnownCallsigns> {
    static KNOWN: OnceLock<Arc<KnownCallsigns>> = OnceLock::new();
    KNOWN
        .get_or_init(|| Arc::new(KnownCallsigns::shipped().cloned().unwrap_or_default()))
        .clone()
}

/// Stream-Mapparr's callsign allowlist, parsed once: it walks sixteen databases and a
/// 448 KB station table, which is far too much work per request.
fn stream_mapparr_known() -> Arc<KnownCallsigns> {
    static KNOWN: OnceLock<Arc<KnownCallsigns>> = OnceLock::new();
    KNOWN
        .get_or_init(|| Arc::new(crosstalk_matching::data::stream_mapparr_known_callsigns()))
        .clone()
}

/// Everything that changes which normaliser/allowlist/alias table a matcher holds.
///
/// `remove_cinemax` is deliberately NOT a key: the plugin never strips the query and
/// precomputes candidates without the flag, so the matcher holds it cleared and the
/// call sites pass the request's flag straight to `fuzzy_match`/`find_best_match`.
#[derive(Clone, PartialEq, Eq)]
struct MatcherKey {
    profile: MatchProfile,
    ignore_quality: bool,
    ignore_regional: bool,
    ignore_geographic: bool,
    ignore_misc: bool,
    alias_country: Option<String>,
}

impl MatcherKey {
    fn of(request: &serde_json::Value, profile: MatchProfile) -> Self {
        Self {
            profile,
            ignore_quality: bool_field(request, "ignore_quality"),
            ignore_regional: bool_field(request, "ignore_regional"),
            ignore_geographic: bool_field(request, "ignore_geographic"),
            ignore_misc: bool_field(request, "ignore_misc"),
            alias_country: request
                .get("alias_country")
                .and_then(|value| value.as_str())
                .map(str::to_string),
        }
    }
}

/// A one-entry cache for the matcher and the candidate index.
///
/// The sweeps send hundreds of consecutive cases against the same pool, and normalising a
/// 60-name pool for every one of them dominated the run. Rebuilding only when the pool or
/// the flags change is the same work the plugins do once per run.
#[derive(Default)]
struct Harness {
    cached: Option<Cached>,
}

struct Cached {
    key: MatcherKey,
    candidates: Vec<String>,
    matcher: Matcher,
    /// Built only for the kinds that need one (match/score/alias).
    index: Option<CandidateIndex>,
}

impl Harness {
    fn matcher(&mut self, request: &serde_json::Value, profile: MatchProfile) -> &Matcher {
        let key = MatcherKey::of(request, profile);
        if self.cached.as_ref().is_some_and(|cached| cached.key != key) {
            self.cached = None;
        }
        if self.cached.is_none() {
            let matcher = matcher_for(request, profile);
            self.cached = Some(Cached {
                key,
                candidates: Vec::new(),
                matcher,
                index: None,
            });
        }
        &self.cached.as_ref().expect("just built").matcher
    }

    fn matcher_and_index(
        &mut self,
        request: &serde_json::Value,
        profile: MatchProfile,
        candidates: &[String],
    ) -> (&Matcher, &CandidateIndex) {
        let key = MatcherKey::of(request, profile);
        let stale = self
            .cached
            .as_ref()
            .is_some_and(|cached| cached.key != key || cached.candidates != candidates);
        if stale {
            self.cached = None;
        }
        if self.cached.is_none() {
            let matcher = matcher_for(request, profile);
            let index = CandidateIndex::build(candidates, &matcher);
            self.cached = Some(Cached {
                key,
                candidates: candidates.to_vec(),
                matcher,
                index: Some(index),
            });
        }
        let cached = self.cached.as_ref().expect("just built");
        (&cached.matcher, cached.index.as_ref().expect("built above"))
    }
}

fn matcher_for(request: &serde_json::Value, profile: MatchProfile) -> Matcher {
    // The request's `remove_cinemax` never reaches the matcher: the plugin strips
    // neither the query nor a precomputed candidate with it, so the matcher holds it
    // cleared and the match/score handlers pass the flag to the call itself.
    let flags = NormalizeFlags {
        ignore_quality: bool_field(request, "ignore_quality"),
        ignore_regional: bool_field(request, "ignore_regional"),
        ignore_geographic: bool_field(request, "ignore_geographic"),
        ignore_misc: bool_field(request, "ignore_misc"),
        ..NormalizeFlags::default()
    };
    let flags = profile.normalize_flags(flags);
    let known = match profile {
        MatchProfile::EpgJanitor => janitor_known(),
        MatchProfile::StreamMapparr => stream_mapparr_known(),
    };
    let aliases = match profile {
        MatchProfile::EpgJanitor => AliasMap::builtin(),
        MatchProfile::StreamMapparr => {
            AliasMap::stream_mapparr(request.get("alias_country").and_then(|c| c.as_str()))
        }
    };
    Matcher::new(flags, aliases, known, profile.options())
}

fn main() {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());

    let mut harness = Harness::default();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => continue,
        };
        if line.trim().is_empty() {
            continue;
        }
        let request: serde_json::Value = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(err) => {
                let response = serde_json::json!({"error": err.to_string()});
                let _ = writeln!(
                    out,
                    "{}",
                    serde_json::to_string(&response).unwrap_or_default()
                );
                continue;
            }
        };
        let response = handle(&request, &mut harness);
        let _ = writeln!(
            out,
            "{}",
            serde_json::to_string(&response).unwrap_or_default()
        );
    }
    let _ = out.flush();
}

fn handle(request: &serde_json::Value, harness: &mut Harness) -> serde_json::Value {
    if let Some(name) = request.get("profile").and_then(|value| value.as_str()) {
        if MatchProfile::from_name(name).is_none() {
            return serde_json::json!({"error": format!("unknown profile {name}")});
        }
    }
    let profile = profile_of(request);
    let kind = request.get("kind").and_then(|kind| kind.as_str());
    match kind {
        None | Some("normalize") => {
            let flags = NormalizeFlags {
                ignore_quality: bool_field(request, "ignore_quality"),
                ignore_regional: bool_field(request, "ignore_regional"),
                ignore_geographic: bool_field(request, "ignore_geographic"),
                ignore_misc: bool_field(request, "ignore_misc"),
                remove_cinemax: bool_field_default_false(request, "remove_cinemax"),
                ..NormalizeFlags::default()
            };
            let tags = tags_of(request);
            let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
            let normalized = Normalizer::new(profile.normalize_flags(flags))
                .normalize_name(str_field(request, "name"), &tag_refs);
            serde_json::json!({"normalized": normalized})
        }
        Some("callsign") => {
            let matcher = harness.matcher(request, profile);
            let (callsign, high) = extract_callsign_with_confidence_for(
                str_field(request, "name"),
                matcher.known(),
                matcher.options().callsign_ladder,
            );
            serde_json::json!({"callsign": callsign, "high_confidence": high})
        }
        Some("location") => {
            let location = extract_location(str_field(request, "name"));
            serde_json::json!({"state": location.state, "city": location.city})
        }
        Some("network") => {
            serde_json::json!({"network": network_keyword(str_field(request, "name"))})
        }
        Some("match") => {
            let candidates = candidates_of(request);
            let tags = tags_of(request);
            let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
            let (matcher, index) = harness.matcher_and_index(request, profile, &candidates);
            match profile {
                MatchProfile::EpgJanitor => {
                    let matches: Vec<serde_json::Value> = matcher
                        .match_all_streams_all(str_field(request, "query"), index, &tag_refs)
                        .iter()
                        .map(|ranked| {
                            serde_json::json!([
                                index.name(ranked.index),
                                ranked.score,
                                ranked.method.label()
                            ])
                        })
                        .collect();
                    serde_json::json!({"matches": matches})
                }
                MatchProfile::StreamMapparr => {
                    let remove_cinemax = bool_field_default_false(request, "remove_cinemax");
                    let best = matcher.fuzzy_match(
                        str_field(request, "query"),
                        index,
                        &tag_refs,
                        remove_cinemax,
                    );
                    serde_json::json!({
                        "match": best.map(|entry| serde_json::json!([
                            index.name(entry.index),
                            entry.score,
                            entry.method.label()
                        ]))
                    })
                }
            }
        }
        Some("score") => {
            let candidates = candidates_of(request);
            let tags = tags_of(request);
            let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
            let (matcher, index) = harness.matcher_and_index(request, profile, &candidates);
            match profile {
                MatchProfile::EpgJanitor => {
                    let scored: Vec<serde_json::Value> = matcher
                        .score_candidates(str_field(request, "query"), index, &tag_refs, None)
                        .iter()
                        .map(|entry| {
                            serde_json::json!([
                                index.name(entry.index),
                                entry.score,
                                entry.method_label()
                            ])
                        })
                        .collect();
                    serde_json::json!({"scored": scored})
                }
                MatchProfile::StreamMapparr => {
                    let remove_cinemax = bool_field_default_false(request, "remove_cinemax");
                    let best = matcher.find_best_match(
                        str_field(request, "query"),
                        index,
                        &tag_refs,
                        remove_cinemax,
                    );
                    serde_json::json!({
                        "best_match": best.map(|(candidate, score)| serde_json::json!([
                            index.name(candidate),
                            score
                        ]))
                    })
                }
            }
        }
        Some("alias") => {
            if profile != MatchProfile::StreamMapparr {
                return serde_json::json!({"error": "alias is a Stream-Mapparr kind"});
            }
            let candidates = candidates_of(request);
            let tags = tags_of(request);
            let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
            let (matcher, index) = harness.matcher_and_index(request, profile, &candidates);
            let hits: Vec<&str> = matcher
                .alias_lookup(str_field(request, "query"), index, &tag_refs)
                .into_iter()
                .map(|candidate| index.name(candidate))
                .collect();
            serde_json::json!({"hits": hits})
        }
        Some("zone") => {
            serde_json::json!({"zone": country::extract_zone(str_field(request, "name")).label()})
        }
        Some("country") => {
            let name = str_field(request, "name");
            let overrides =
                country::parse_prefix_country_overrides(str_field(request, "overrides_raw"));
            serde_json::json!({
                "from_name": country::country_from_name(name),
                "from_group": country::country_from_group(name),
                "override": country::country_from_prefix_overrides(name, &overrides),
            })
        }
        Some("classify") => {
            let channel = request.get("channel").and_then(|v| v.as_str());
            let stream = request.get("stream").and_then(|v| v.as_str());
            serde_json::json!({
                "relationship": country::classify(channel, stream).label()
            })
        }
        Some(other) => serde_json::json!({"error": format!("unknown kind {other}")}),
    }
}
