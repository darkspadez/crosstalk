//! Named matcher profiles: one engine, one flag set per plugin.
//!
//! The plugins do not each get their own matcher. They share this crate and differ only in
//! the flags they select here, so a fix to normalisation, aliases, callsigns, tokens or
//! scoring reaches every app at once.

use crate::alias::AliasMap;
use crate::callsign::KnownCallsigns;
use crate::data::known_callsigns_for;
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
}

impl MatchProfile {
    /// The options this profile runs with.
    fn options(self) -> MatchOptions {
        match self {
            // The plugin's own constants. Where Janitor differs from `matching_core.py` in a
            // hard-coded guard (its 5-character subset guard against the core's 4) the port
            // keeps the Janitor spelling, so nothing here overrides it.
            MatchProfile::EpgJanitor => MatchOptions::default(),
        }
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
}

impl MatcherConfig {
    /// The alias table for a run: the built-ins plus any custom entries.
    pub fn aliases(&self) -> AliasMap {
        let builtin = AliasMap::builtin();
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

    /// The callsign allowlist for a run: the shipped FCC table plus whatever the enabled
    /// channel databases contribute.
    pub fn known_callsigns(&self) -> KnownCallsigns {
        let mut known = KnownCallsigns::shipped().cloned().unwrap_or_default();
        let codes: Vec<&str> = self.databases.iter().map(String::as_str).collect();
        for callsign in known_callsigns_for(&codes).to_vec() {
            known.insert(&callsign);
        }
        known
    }
}

impl Matcher {
    /// Build the matcher a profile describes: its flags, aliases, allowlist and options.
    ///
    /// This is how every sub-app builds a matcher, so the flags a run matches with are the
    /// ones the host file states and the profile's parity sweep guards.
    pub fn from_config(config: &MatcherConfig, profile: MatchProfile) -> Self {
        Self::new(
            config.flags,
            config.aliases(),
            Arc::new(config.known_callsigns()),
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
            ignore_quality: true,
            ignore_regional: false,
            ignore_geographic: true,
            ignore_misc: false,
        };
        let matcher = Matcher::from_config(&config, MatchProfile::EpgJanitor);
        assert_eq!(matcher.tags(), vec!["quality", "geographic"]);
    }
}
