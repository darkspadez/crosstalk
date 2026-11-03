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
//!
//! Two data sets ship. EPG Janitor's is `data/channels/` plus its FCC callsign allowlist;
//! Stream-Mapparr's is sixteen databases plus its station table (`networks.json`), and
//! only the files that differ from EPG Janitor's copies are duplicated under
//! `data/stream-mapparr/`: the four countries only it ships (DO, PE, PL, PT) and the US
//! database, whose entries carry a `zones` field. A shared file is read once.

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

/// The built-in alias table, converted from EPG Janitor's `aliases.py`.
pub const ALIASES_JSON: &str = include_str!("../../../data/aliases.json");

// ---------------------------------------------------------------------------
// Stream-Mapparr's data set.
//
// Stream-Mapparr ships sixteen channel databases and an FCC station table of its own
// (`networks.json`), and its alias table is a different, larger one. Only the files that
// differ from EPG Janitor's copies are duplicated here: the rest reuse `data/channels/`,
// so a fix to a shared country database lands once.
// ---------------------------------------------------------------------------

/// Country code -> raw JSON for the Stream-Mapparr-only databases: four countries EPG
/// Janitor does not ship, plus the US database whose entries carry a `zones` field.
pub const STREAM_MAPPARR_CHANNEL_DATABASES: [(&str, &str); 5] = [
    (
        "DO",
        include_str!("../../../data/stream-mapparr/DO_channels.json"),
    ),
    (
        "PE",
        include_str!("../../../data/stream-mapparr/PE_channels.json"),
    ),
    (
        "PL",
        include_str!("../../../data/stream-mapparr/PL_channels.json"),
    ),
    (
        "PT",
        include_str!("../../../data/stream-mapparr/PT_channels.json"),
    ),
    (
        "US",
        include_str!("../../../data/stream-mapparr/US_channels.json"),
    ),
];

/// Stream-Mapparr's alias table (`CHANNEL_ALIASES` in its `aliases.py`).
pub const STREAM_MAPPARR_ALIASES_JSON: &str =
    include_str!("../../../data/stream-mapparr/aliases.json");

/// Stream-Mapparr's country-scoped alias overrides (`COUNTRY_ALIASES`), keyed by
/// database code. Only `DO` and `PE` carry entries.
pub const STREAM_MAPPARR_COUNTRY_ALIASES_JSON: &str =
    include_str!("../../../data/stream-mapparr/country_aliases.json");

/// Stream-Mapparr's FCC station table: callsign, affiliation and community served.
///
/// The per-country databases carry no `broadcast` entries, so this file is the OTA /
/// callsign source: 1,915 stations, each indexed by full and base callsign.
pub const STREAM_MAPPARR_NETWORKS_JSON: &str =
    include_str!("../../../data/stream-mapparr/networks.json");

/// Every country database code Stream-Mapparr loads, sorted.
pub fn stream_mapparr_codes() -> Vec<&'static str> {
    let mut codes: Vec<&'static str> = available_database_codes();
    for (code, _) in STREAM_MAPPARR_CHANNEL_DATABASES {
        if !codes.contains(&code) {
            codes.push(code);
        }
    }
    codes.sort_unstable();
    codes
}

/// Raw JSON for one country database in the Stream-Mapparr data set.
///
/// Its own copy wins where one exists; a country only EPG Janitor ships still resolves.
pub fn stream_mapparr_database_json(code: &str) -> Option<&'static str> {
    STREAM_MAPPARR_CHANNEL_DATABASES
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(code))
        .map(|(_, json)| *json)
        .or_else(|| channel_database_json(code))
}

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

/// The callsign allowlist Stream-Mapparr's matcher holds.
///
/// `FuzzyMatcher._load_channel_databases` builds `channel_lookup` from two sources and
/// then passes every KEY of it to `set_known_callsigns`:
///
///   * a `broadcast` channel's callsign and its `-TV`/`-CD`/...-stripped base, from the
///     per-country databases (which ship none in this version, but the loop is kept);
///   * every `networks.json` station's callsign and base, first entry winning.
///
/// Unlike EPG Janitor's allowlist there is no shipped FCC file and no station-format
/// scan of premium names: the station table supplies those callsigns instead.
pub fn stream_mapparr_known_callsigns() -> crate::callsign::KnownCallsigns {
    static CACHED: std::sync::OnceLock<crate::callsign::KnownCallsigns> =
        std::sync::OnceLock::new();
    // Parsing sixteen databases and a 448 KB station table is start-up work, not per-call
    // work: a run may build more than one matcher.
    CACHED
        .get_or_init(build_stream_mapparr_known_callsigns)
        .clone()
}

fn build_stream_mapparr_known_callsigns() -> crate::callsign::KnownCallsigns {
    let mut known = crate::callsign::KnownCallsigns::new();

    for code in stream_mapparr_codes() {
        let Some(json) = stream_mapparr_database_json(code) else {
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
            if !kind.contains("broadcast") {
                continue;
            }
            let callsign = channel
                .get("callsign")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .trim();
            if callsign.is_empty() {
                continue;
            }
            known.insert(&callsign.to_uppercase());
            let base = crate::callsign::normalize_callsign(callsign);
            if base != callsign {
                known.insert(&base.to_uppercase());
            }
        }
    }

    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(STREAM_MAPPARR_NETWORKS_JSON) else {
        return known;
    };
    for station in parsed.as_array().into_iter().flatten() {
        let callsign = station
            .get("callsign")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .trim()
            .to_uppercase();
        if callsign.is_empty() {
            continue;
        }
        known.insert(&callsign);
        let base = crate::callsign::normalize_callsign(&callsign);
        if base != callsign {
            known.insert(&base);
        }
    }

    known
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

#[cfg(test)]
mod stream_mapparr_tests {
    use super::*;

    #[test]
    fn the_stream_mapparr_set_covers_sixteen_countries() {
        let codes = stream_mapparr_codes();
        assert_eq!(codes.len(), 16, "{codes:?}");
        for code in ["DO", "PE", "PL", "PT", "US", "UK", "NO"] {
            assert!(codes.contains(&code), "{code} missing");
        }
    }

    #[test]
    fn differing_databases_resolve_to_the_stream_mapparr_copy() {
        // The US file differs only by its `zones` field, but it is still its own file.
        let us = stream_mapparr_database_json("US").expect("US");
        assert!(us.contains("\"zones\""));
        assert_eq!(stream_mapparr_database_json("us"), Some(us));
        // A shared country database falls back to the one EPG Janitor ships.
        assert_eq!(
            stream_mapparr_database_json("UK"),
            channel_database_json("UK")
        );
        assert_eq!(stream_mapparr_database_json("ZZ"), None);
    }

    #[test]
    fn the_station_table_contributes_callsigns_and_bases() {
        let known = stream_mapparr_known_callsigns();
        // `networks.json` holds FCC station entries; the first one is KFCT.
        assert!(known.contains("KFCT"));
        assert!(known.len() > 1_500, "got {}", known.len());
        // Shared with the shipped FCC table, which Stream-Mapparr does not use.
        assert!(known.contains("WABC"));
    }

    #[test]
    fn aliases_parse_as_objects_of_string_arrays() {
        let parsed: serde_json::Value =
            serde_json::from_str(STREAM_MAPPARR_ALIASES_JSON).expect("aliases parse");
        let map = parsed.as_object().expect("object");
        assert_eq!(map.len(), 198, "Stream-Mapparr ships 198 entries");
        for (key, value) in map {
            let list = value.as_array().unwrap_or_else(|| panic!("{key}"));
            assert!(!list.is_empty(), "{key} is not empty");
        }

        let parsed: serde_json::Value =
            serde_json::from_str(STREAM_MAPPARR_COUNTRY_ALIASES_JSON).expect("country aliases");
        let map = parsed.as_object().expect("object");
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("DO"));
        assert!(map.contains_key("PE"));
    }
}
