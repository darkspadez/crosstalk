//! Compile-time data shipped with the matcher.
//!
//! The channel databases, the FCC callsign allowlist and the built-in alias table are
//! constants of the matcher rather than operator data, so they are embedded in the
//! binary instead of being read from a directory at run time. That removes a class of
//! "plugin installed without its data files" failures and makes the daemon a single
//! self-contained artifact.
//!
//! Country databases are parsed lazily, one file at a time, so a run that only needs the
//! United States does not pay for the other eleven.

/// The FCC-derived allowlist of every licensed US television callsign.
pub const US_STATION_CALLSIGNS_JSON: Option<&str> =
    Some(include_str!("../../../data/us_station_callsigns.json"));

/// Country code -> raw JSON of `<CC>_channels.json`.
pub const CHANNEL_DATABASES: [(&str, &str); 12] = [
    (
        "AU",
        include_str!("../../../data/channels/AU_channels.json"),
    ),
    (
        "BR",
        include_str!("../../../data/channels/BR_channels.json"),
    ),
    (
        "CA",
        include_str!("../../../data/channels/CA_channels.json"),
    ),
    (
        "DE",
        include_str!("../../../data/channels/DE_channels.json"),
    ),
    (
        "ES",
        include_str!("../../../data/channels/ES_channels.json"),
    ),
    (
        "FR",
        include_str!("../../../data/channels/FR_channels.json"),
    ),
    (
        "IN",
        include_str!("../../../data/channels/IN_channels.json"),
    ),
    (
        "MX",
        include_str!("../../../data/channels/MX_channels.json"),
    ),
    (
        "NL",
        include_str!("../../../data/channels/NL_channels.json"),
    ),
    (
        "NO",
        include_str!("../../../data/channels/NO_channels.json"),
    ),
    (
        "UK",
        include_str!("../../../data/channels/UK_channels.json"),
    ),
    (
        "US",
        include_str!("../../../data/channels/US_channels.json"),
    ),
];

/// The built-in alias table, converted from the plugin's `aliases.py`.
pub const ALIASES_JSON: &str = include_str!("../../../data/aliases.json");

/// Codes of every shipped channel database, sorted.
pub fn available_database_codes() -> Vec<&'static str> {
    let mut codes: Vec<&'static str> = CHANNEL_DATABASES.iter().map(|(code, _)| *code).collect();
    codes.sort_unstable();
    codes
}

/// Raw JSON for one country database.
pub fn channel_database_json(code: &str) -> Option<&'static str> {
    CHANNEL_DATABASES
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(code))
        .map(|(_, json)| *json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_databases_are_present_and_parse() {
        for (code, json) in CHANNEL_DATABASES {
            let parsed: serde_json::Value =
                serde_json::from_str(json).unwrap_or_else(|e| panic!("{code}: {e}"));
            let channels = parsed
                .get("channels")
                .and_then(|v| v.as_array())
                .unwrap_or_else(|| panic!("{code} has a channels array"));
            assert!(!channels.is_empty(), "{code} is not empty");
        }
    }

    #[test]
    fn lookup_is_case_insensitive() {
        assert!(channel_database_json("us").is_some());
        assert!(channel_database_json("US").is_some());
        assert!(channel_database_json("ZZ").is_none());
    }

    #[test]
    fn aliases_parse_as_an_object_of_string_arrays() {
        let parsed: serde_json::Value = serde_json::from_str(ALIASES_JSON).expect("aliases parse");
        let map = parsed.as_object().expect("object");
        assert!(map.len() > 200, "got {}", map.len());
        for (key, value) in map {
            let list = value
                .as_array()
                .unwrap_or_else(|| panic!("{key} is an array"));
            assert!(!list.is_empty(), "{key} is not empty");
        }
    }
}

/// Callsigns implied by the channel databases for `codes`.
///
/// Mirrors `FuzzyMatcher.reload_databases` + `_get_known_callsigns`: entries whose `type`
/// mentions `broadcast` feed the broadcast loop, everything else the premium loop. Only
/// the codes named are read, so a single-region run does not pay for the other databases.
pub fn known_callsigns_for(codes: &[&str]) -> crate::callsign::KnownCallsigns {
    let mut known = crate::callsign::KnownCallsigns::new();
    for code in codes {
        let Some(json) = channel_database_json(code) else {
            continue;
        };
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json) else {
            continue;
        };
        let Some(channels) = parsed.get("channels").and_then(|value| value.as_array()) else {
            continue;
        };
        for channel in channels {
            let kind = channel
                .get("type")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_lowercase();
            let name = channel.get("channel_name").and_then(|value| value.as_str());
            if kind.contains("broadcast") {
                let callsign = channel.get("callsign").and_then(|value| value.as_str());
                known.add_broadcast_entry(callsign, name);
            } else if let Some(name) = name {
                known.add_premium_entry(name);
            }
        }
    }
    known
}

#[cfg(test)]
mod database_callsign_tests {
    use super::*;

    #[test]
    fn known_callsigns_pick_up_station_format_names() {
        let known = known_callsigns_for(&["US"]);
        assert!(!known.is_empty());
        // The FCC file already has these; the point is that the loader runs and merges.
        assert!(known.contains("WABC") || known.contains("KGTV"));
    }

    #[test]
    fn unknown_country_contributes_nothing() {
        assert!(known_callsigns_for(&["ZZ"]).is_empty());
    }
}
