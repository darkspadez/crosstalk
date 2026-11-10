//! Named matcher profiles: one engine, one flag set per plugin.
//!
//! The plugins do not each get their own matcher. They share this crate and differ only in
//! the flags they select here, so a fix to normalisation, aliases, callsigns, tokens or
//! scoring reaches every app at once.
//!
//! # Stream-Mapparr's matcher versus EPG Janitor's
//!
//! Both plugins vendor the same `matching_core.py` (verified byte-identical), then layer
//! their own code on top. Stream-Mapparr barely overrides it; EPG Janitor overrides the
//! normaliser and the callsign ladder outright. This table is the function-by-function
//! result of diffing `reference/Stream-Mapparr/Stream-Mapparr/fuzzy_matcher.py` against
//! `reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/fuzzy_matcher.py`; the *"here"*
//! column says where the difference lives in this crate.
//!
//! | function / data | Stream-Mapparr | EPG Janitor | here |
//! |---|---|---|---|
//! | `matching_core.py` | byte-identical | byte-identical | one port (`similarity`, `tokens`, core half of `normalize`) |
//! | `normalize_name` | the core's body, plus a designator repair (`F1` stays `F1`) | its own override: different provider-prefix order, quality table, region rules, unconditional trailing-suffix strip, no decorative-Unicode strip | `NormalizeFlags::core_pipeline` + `NormalizeFlags::rejoin_designators` |
//! | `remove_cinemax` | the core's opt-in, set per channel whose name contains `max` | absent | `NormalizeFlags::remove_cinemax` |
//! | `remove_country_prefix`, `strip_bare_region` | core opt-ins, never set by either plugin | — | not ported (dead) |
//! | `process_string_for_matching` | core's (keeps `+`) | core's (keeps `+`) | one port (`tokens`) |
//! | `calculate_similarity`, `_length_scaled_threshold` | core's | core's | one port (`similarity`) |
//! | `_has_token_overlap` | core's (5-char distinctive) — and its `fuzzy_match` does not call it at all | its own override (5-char subset rule) — used by every stage | Janitor's is ported; SM never calls it |
//! | callsign ladder | core's four priorities; a denylisted word is rescued by an OTA number right after it (`KING 5`) | its own five-priority override, adding `CALLSIGN (NETWORK)` and no OTA rescue | `MatchOptions::callsign_ladder` |
//! | callsign allowlist | every `channel_lookup` key: database callsigns + `networks.json` stations and their bases | shipped FCC facility table + station-format names from the databases | `data::stream_mapparr_known_callsigns` vs `MatcherConfig::known_callsigns` |
//! | channel databases | 16 files, `zones` field expanded into East/West variants, plus `networks.json` (1,915 FCC stations) | 12 files, no zones, no station table | `data::stream_mapparr_*` (only the 5 differing/extra files are duplicated) |
//! | alias table | 198 entries + `COUNTRY_ALIASES` overrides for `DO`/`PE` | 204 entries, no country overrides | `AliasMap::stream_mapparr` |
//! | alias lookup | `alias_lookup`: exact, verbatim key then case-folded fallback (`_fold_alias_key`) | `alias_match`: trimmed verbatim key, then fuzzy similarity per alias | `MatchOptions::fold_alias_keys` + `Matcher::alias_lookup` vs `Matcher::alias_match` |
//! | numeric-sibling guard | `is_designator_token` (digits, or 1-3 letters + 1-2 digits: `f1`, `tf1`) | `str.isdigit()` tokens | `tokens::is_designator_token` |
//! | `find_best_match` | designator guard, `min_ratio` early exit | no guard, no cut-off | `Matcher::find_best_match` |
//! | `fuzzy_match` | three stages, **no** majority-overlap guard and **no** length-scaled threshold | three stages interleaved, majority-overlap guard and length-scaled threshold at every stage | `Matcher::fuzzy_match` is SM's; Janitor's is not ported (its pipeline uses `match_all_streams`) |
//! | `match_all_streams` | absent (the collector lives in its `plugin.py`) | present, with regional filter and callsign anchor | `Matcher::match_all_streams` is Janitor's |
//! | `_channel_number_boost` | absent (its number handling lives in the `plugin.py` collector) | present, 5 points for a 3+ digit number | `Matcher::channel_number_boost` is Janitor's |
//! | `extract_zone`, `_country_has_one_timezone` | present (East/West/Pacific routing, single-time-zone guard) | absent | `country::extract_zone` |
//! | `country.py` (detector, overrides, `classify`) | present | absent | `country` module, behind [`crate::country::CountryDetector`] |
//! | `extract_tags` | regional includes Pacific/Central/Mountain/Atlantic | East/West only | not ported for either (no matcher path uses it) |
//! | precompute caches | remember the flag set they were filled with | do not | not ported (the candidate index is built with the run's flags) |
//! | `_get_known_callsigns`, `_load_station_callsigns` | absent | present (lazy, from the FCC file) | Janitor's, folded into `KnownCallsigns::shipped` |
//!
//! Everything else in the two files is the same code path with the same constants; a
//! difference that is not listed here would be a porting bug, and the two parity sweeps
//! (`scripts/parity_sweep.py`, `scripts/parity_sweep_sm.py`) are what check that.

use crate::alias::AliasMap;
use crate::callsign::KnownCallsigns;
use crate::data::{known_callsigns_for, stream_mapparr_known_callsigns};
use crate::normalize::NormalizeFlags;
use crate::pipeline::{MatchOptions, Matcher};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The plugin a matcher is configured for.
///
/// A profile is a flag set on the single engine, never a copy of it. Each profile keeps its
/// own parity sweep, because parity is what the flags are tuned against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchProfile {
    /// EPG Janitor plugin's matcher flags, parity-tested against
    /// `reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/fuzzy_matcher.py`.
    EpgJanitor,
    /// Stream-Mapparr's matcher, parity-tested against
    /// `reference/Stream-Mapparr/Stream-Mapparr/fuzzy_matcher.py` and its `country.py`.
    StreamMapparr,
}

impl MatchProfile {
    /// Parse a profile from its command-line / configuration spelling.
    ///
    /// Accepts `epg-janitor`, `epgjanitor`, `janitor`, `stream-mapparr`,
    /// `streammapparr` and `sm`, case-insensitively.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.trim().to_lowercase().replace('_', "-").as_str() {
            "epg-janitor" | "epgjanitor" | "janitor" => Some(MatchProfile::EpgJanitor),
            "stream-mapparr" | "streammapparr" | "sm" => Some(MatchProfile::StreamMapparr),
            _ => None,
        }
    }

    /// The plugin's name, as reported in a run.
    pub fn name(self) -> &'static str {
        match self {
            MatchProfile::EpgJanitor => "epg-janitor",
            MatchProfile::StreamMapparr => "stream-mapparr",
        }
    }

    /// The options this profile runs with.
    pub fn options(self) -> MatchOptions {
        match self {
            // The plugin's own constants. Where Janitor differs from `matching_core.py` in a
            // hard-coded guard (its 5-character subset guard against the core's 4) the port
            // keeps the Janitor spelling, so nothing here overrides it.
            MatchProfile::EpgJanitor => MatchOptions::default(),
            MatchProfile::StreamMapparr => MatchOptions {
                match_threshold: 85,
                callsign_ladder: crate::callsign::CallsignLadder::SharedCore,
                fold_alias_keys: true,
            },
        }
    }

    /// The normalisation flags for this profile, given the host's settings.
    ///
    /// The host's four toggles carry through untouched; the profile adds the structural
    /// switches its plugin's code implies.
    pub fn normalize_flags(self, host: NormalizeFlags) -> NormalizeFlags {
        match self {
            MatchProfile::EpgJanitor => host,
            MatchProfile::StreamMapparr => NormalizeFlags {
                core_pipeline: true,
                rejoin_designators: true,
                ..host
            },
        }
    }

    /// The built-in alias table for this profile.
    ///
    /// `country` only means something for Stream-Mapparr, whose `COUNTRY_ALIASES` table
    /// extends the base aliases for the selected channel database.
    pub fn builtin_aliases(self, country: Option<&str>) -> AliasMap {
        match self {
            MatchProfile::EpgJanitor => AliasMap::builtin(),
            MatchProfile::StreamMapparr => AliasMap::stream_mapparr(country),
        }
    }

    /// The callsign allowlist this profile's matcher holds.
    pub fn known_callsigns(self, configured_databases: &[String]) -> KnownCallsigns {
        match self {
            MatchProfile::EpgJanitor => {
                let mut known = KnownCallsigns::shipped().cloned().unwrap_or_default();
                let codes: Vec<&str> = configured_databases.iter().map(String::as_str).collect();
                for callsign in known_callsigns_for(&codes).to_vec() {
                    known.insert(&callsign);
                }
                known
            }
            // Stream-Mapparr loads all sixteen databases and the station table at start-up
            // and never narrows them, so the host's database list does not apply.
            MatchProfile::StreamMapparr => stream_mapparr_known_callsigns(),
        }
    }
}

impl Matcher {
    /// A matcher with this profile's shipped data and flags, for the parity sweeps and for
    /// tools that need one region's vocabulary without a host configuration.
    pub fn for_profile(profile: MatchProfile) -> Self {
        Self::new(
            profile.normalize_flags(NormalizeFlags::default()),
            profile.builtin_aliases(None),
            Arc::new(profile.known_callsigns(&[])),
            profile.options(),
        )
    }
}

/// The host's name-matching configuration, in this crate's own terms.
///
/// `crosstalk-core` owns the file (`[matching]` in `crosstalk.toml`) and cannot be a
/// dependency of this crate: the dependency runs the other way. So the host hands its values
/// over in this shape, and `MatchingConfig::matcher` there is the adapter.
#[derive(Debug, Clone, Default)]
pub struct MatcherConfig {
    /// Which tag classes to strip before comparing.
    pub flags: NormalizeFlags,
    /// Country database codes whose callsigns join the shipped FCC allowlist.
    pub databases: Vec<String>,
    /// Custom aliases, each key REPLACING the built-in list for that name.
    pub custom_aliases: BTreeMap<String, Vec<String>>,
    /// Stream-Mapparr's country-scoped alias table (`DO`/`PE`); ignored by the other
    /// profiles.
    pub alias_country: Option<String>,
}

impl MatcherConfig {
    /// The alias table for a run: the profile's built-ins plus any custom entries.
    pub fn aliases_for(&self, profile: MatchProfile) -> AliasMap {
        let builtin = profile.builtin_aliases(self.alias_country.as_deref());
        if self.custom_aliases.is_empty() {
            return builtin;
        }
        builtin.with_overrides(
            self.custom_aliases
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        )
    }

    /// The alias table for a run in the EPG Janitor table's terms.
    pub fn aliases(&self) -> AliasMap {
        self.aliases_for(MatchProfile::EpgJanitor)
    }

    /// The callsign allowlist for a run.
    pub fn known_callsigns_for(&self, profile: MatchProfile) -> KnownCallsigns {
        profile.known_callsigns(&self.databases)
    }

    /// The callsign allowlist for a run in the EPG Janitor profile's terms.
    pub fn known_callsigns(&self) -> KnownCallsigns {
        self.known_callsigns_for(MatchProfile::EpgJanitor)
    }
}

impl Matcher {
    /// Build the matcher a profile describes: its flags, aliases, allowlist and options.
    ///
    /// This is how every sub-app builds a matcher, so the flags a run matches with are the
    /// ones the host file states and the profile's parity sweep guards.
    pub fn from_config(config: &MatcherConfig, profile: MatchProfile) -> Self {
        Self::new(
            profile.normalize_flags(config.flags),
            config.aliases_for(profile),
            Arc::new(config.known_callsigns_for(profile)),
            profile.options(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> MatcherConfig {
        MatcherConfig {
            flags: NormalizeFlags::default(),
            databases: vec!["US".to_string()],
            custom_aliases: BTreeMap::new(),
            alias_country: None,
        }
    }

    #[test]
    fn from_config_uses_the_profiles_options_and_the_host_values() {
        let matcher = Matcher::from_config(&config(), MatchProfile::EpgJanitor);
        assert_eq!(matcher.options().match_threshold, 85);
        assert_eq!(matcher.normalizer().flags(), NormalizeFlags::default());
        // The US database contributes callsigns to the shipped allowlist.
        assert!(matcher.known().contains("WABC"));
    }

    #[test]
    fn a_custom_alias_replaces_the_builtin_entry() {
        let mut config = config();
        config
            .custom_aliases
            .insert("MSNBC".to_string(), vec!["MS Now".to_string()]);
        let matcher = Matcher::from_config(&config, MatchProfile::EpgJanitor);
        assert_eq!(
            matcher.alias_map().aliases_for("MSNBC"),
            vec!["MS Now".to_string()]
        );
    }

    #[test]
    fn the_tag_list_follows_the_flags() {
        let mut config = config();
        config.flags = NormalizeFlags {
            ignore_regional: false,
            ..NormalizeFlags::default()
        };
        let matcher = Matcher::from_config(&config, MatchProfile::EpgJanitor);
        assert_eq!(matcher.tags(), vec!["quality", "geographic", "misc"]);
    }

    #[test]
    fn the_stream_mapparr_profile_selects_its_own_flags_and_data() {
        let config = config();
        let matcher = Matcher::from_config(&config, MatchProfile::StreamMapparr);
        let flags = matcher.normalizer().flags();
        assert!(flags.core_pipeline && flags.rejoin_designators);
        assert_eq!(
            matcher.options().callsign_ladder,
            crate::callsign::CallsignLadder::SharedCore
        );
        assert!(matcher.options().fold_alias_keys);
        // Stream-Mapparr's own table, and its station table's callsigns.
        assert!(!matcher.alias_map().aliases_for("Antena 3").is_empty());
        assert!(matcher.known().contains("KFCT"));
    }

    #[test]
    fn profiles_parse_from_their_reported_names() {
        for (text, expected) in [
            ("epg-janitor", MatchProfile::EpgJanitor),
            ("EPGJanitor", MatchProfile::EpgJanitor),
            ("stream-mapparr", MatchProfile::StreamMapparr),
            ("StreamMapparr", MatchProfile::StreamMapparr),
            ("sm", MatchProfile::StreamMapparr),
        ] {
            assert_eq!(MatchProfile::from_name(text), Some(expected), "{text}");
            assert_eq!(expected.name(), expected.name());
        }
        assert_eq!(MatchProfile::from_name("lineuparr"), None);
    }
}
