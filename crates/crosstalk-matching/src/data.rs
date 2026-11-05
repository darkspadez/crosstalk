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

/// One entry of Stream-Mapparr's FCC station table (`networks.json`).
///
/// Only the three fields its OTA paths read are kept: the callsign itself, the network
/// affiliation and the community served (both of which corroborate a common-word callsign
/// such as `KING` or `WHO`), and the state, which no caller reads today but which the same
/// table carries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Station {
    /// The full callsign as the table spells it (`WEWS-TV`, `KFCT`).
    pub callsign: String,
    pub network_affiliation: String,
    pub community_served_city: String,
    pub community_served_state: String,
}

/// The station table, keyed by BOTH the full callsign and its `-TV`/`-CD`/`-DT…`-stripped
/// base, first entry winning.
///
/// This is `FuzzyMatcher._load_broadcast_stations` (fuzzy_matcher.py:326): every station is
/// indexed under its full callsign and, when they differ, under its base, with `setdefault`
/// semantics so a subchannel entry later in the file cannot clobber the main affiliate. The
/// matcher's `channel_lookup` is this table plus the channel databases' `broadcast` entries
/// (which the shipped databases do not carry), so this IS the OTA lookup in production.
pub fn stream_mapparr_stations() -> &'static std::collections::HashMap<String, Station> {
    static CACHED: std::sync::OnceLock<std::collections::HashMap<String, Station>> =
        std::sync::OnceLock::new();
    CACHED.get_or_init(|| {
        let mut stations = std::collections::HashMap::new();
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(STREAM_MAPPARR_NETWORKS_JSON)
        else {
            return stations;
        };
        for station in parsed.as_array().into_iter().flatten() {
            let text = |key: &str| {
                station
                    .get(key)
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .trim()
                    .to_string()
            };
            let callsign = text("callsign").to_uppercase();
            if callsign.is_empty() {
                continue;
            }
            let entry = Station {
                callsign: callsign.clone(),
                network_affiliation: text("network_affiliation"),
                community_served_city: text("community_served_city"),
                community_served_state: text("community_served_state").to_uppercase(),
            };
            stations
                .entry(callsign.clone())
                .or_insert_with(|| entry.clone());
            let base = crate::callsign::normalize_callsign(&callsign);
            if base != callsign {
                stations.entry(base).or_insert(entry);
            }
        }
        stations
    })
}

/// Whether the station table lists this callsign (full or base form).
///
/// `_build_us_callsign_database`'s membership test: the OTA-only action skips any channel
/// whose base callsign is not a real station, so a callsign-shaped word in a channel name
/// never collects streams on its own.
pub fn is_us_station(callsign: &str) -> bool {
    let wanted = callsign.trim().to_uppercase();
    if wanted.is_empty() {
        return false;
    }
    let stations = stream_mapparr_stations();
    stations.contains_key(&wanted)
        || stations.contains_key(&crate::callsign::normalize_callsign(&wanted))
}

#[cfg(test)]
mod station_tests {
    use super::*;

    #[test]
    fn every_station_is_indexed_by_full_and_base_callsign() {
        let stations = stream_mapparr_stations();
        assert!(stations.len() > 1_500, "got {}", stations.len());
        let full = stations.get("KFCT").expect("KFCT is in the table");
        assert!(!full.network_affiliation.is_empty() || !full.community_served_city.is_empty());
        assert!(is_us_station("kfct"));
        // A callsign-shaped word that is not a station is not in the table.
        assert!(!is_us_station("ZZZZ"));
        // The base form resolves a suffixed callsign, as the plugin's setdefault does.
        for callsign in ["WEWS-TV", "WABC-TV"] {
            let base = crate::callsign::normalize_callsign(callsign);
            if stations.contains_key(callsign) {
                assert!(stations.contains_key(&base), "{callsign} -> {base}");
            }
        }
    }
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

// ---------------------------------------------------------------------------
// Merged phase-3 data set (`data/merged/`, built by
// `scripts/data/build_merged.py` from all four plugins' shipped tables).
//
// This is the one deduplicated union the unified engine of phase 3 step 6 will
// match against: one channel database per country, one station table keyed by
// callsign, one alias table, and Lineuparr's lineup files. It is NOT wired
// into matching yet (step 6 does that); the existing per-plugin profiles
// above keep loading their current files, so every parity gate stays at 0.
//
// The build script reproduces `data/merged/` byte for byte
// (`build_merged.py --check`, enforced in CI); the tests below pin the merge
// counts against the embedded `PROVENANCE.json`.
// ---------------------------------------------------------------------------

/// The merged data set: one union channel database per country.
pub mod merged {
    use std::collections::{HashMap, HashSet};
    use std::sync::OnceLock;

    /// Country code -> merged union `<CC>.json` (compact JSON, see
    /// `scripts/data/build_merged.py`).
    pub const MERGED_CHANNEL_DATABASES: [(&str, &str); 16] = [
        ("AU", include_str!("../../../data/merged/channels/AU.json")),
        ("BR", include_str!("../../../data/merged/channels/BR.json")),
        ("CA", include_str!("../../../data/merged/channels/CA.json")),
        ("DE", include_str!("../../../data/merged/channels/DE.json")),
        ("DO", include_str!("../../../data/merged/channels/DO.json")),
        ("ES", include_str!("../../../data/merged/channels/ES.json")),
        ("FR", include_str!("../../../data/merged/channels/FR.json")),
        ("IN", include_str!("../../../data/merged/channels/IN.json")),
        ("MX", include_str!("../../../data/merged/channels/MX.json")),
        ("NL", include_str!("../../../data/merged/channels/NL.json")),
        ("NO", include_str!("../../../data/merged/channels/NO.json")),
        ("PE", include_str!("../../../data/merged/channels/PE.json")),
        ("PL", include_str!("../../../data/merged/channels/PL.json")),
        ("PT", include_str!("../../../data/merged/channels/PT.json")),
        ("UK", include_str!("../../../data/merged/channels/UK.json")),
        ("US", include_str!("../../../data/merged/channels/US.json")),
    ];

    /// The merged station table (`data/merged/stations.json`).
    pub const MERGED_STATIONS_JSON: &str = include_str!("../../../data/merged/stations.json");

    /// The merged alias table (`data/merged/aliases.json`).
    pub const MERGED_ALIASES_JSON: &str = include_str!("../../../data/merged/aliases.json");

    /// The machine-readable merge log (`data/merged/PROVENANCE.json`).
    pub const MERGED_PROVENANCE_JSON: &str = include_str!("../../../data/merged/PROVENANCE.json");

    /// Lineup name -> verbatim Lineuparr lineup file.
    pub const MERGED_LINEUPS: [(&str, &str); 29] = [
        (
            "AU_Foxtel",
            include_str!("../../../data/merged/lineups/AU_Foxtel_lineup.json"),
        ),
        (
            "CA_Telus-Optik",
            include_str!("../../../data/merged/lineups/CA_Telus-Optik_lineup.json"),
        ),
        (
            "DO_Altice",
            include_str!("../../../data/merged/lineups/DO_Altice_lineup.json"),
        ),
        (
            "DO_Claro",
            include_str!("../../../data/merged/lineups/DO_Claro_lineup.json"),
        ),
        (
            "DO_Combined",
            include_str!("../../../data/merged/lineups/DO_Combined_lineup.json"),
        ),
        (
            "ES_Movistar",
            include_str!("../../../data/merged/lineups/ES_Movistar_lineup.json"),
        ),
        (
            "FR_CanalPlus",
            include_str!("../../../data/merged/lineups/FR_CanalPlus_lineup.json"),
        ),
        (
            "FR_CanalPlus_TNT",
            include_str!("../../../data/merged/lineups/FR_CanalPlus_TNT_lineup.json"),
        ),
        (
            "NL_KPN",
            include_str!("../../../data/merged/lineups/NL_KPN_lineup.json"),
        ),
        (
            "NL_ODIDO",
            include_str!("../../../data/merged/lineups/NL_ODIDO_lineup.json"),
        ),
        (
            "PE_Claro",
            include_str!("../../../data/merged/lineups/PE_Claro_lineup.json"),
        ),
        (
            "QA_beIN-MENA",
            include_str!("../../../data/merged/lineups/QA_beIN-MENA_lineup.json"),
        ),
        (
            "TZ_Azam-TV",
            include_str!("../../../data/merged/lineups/TZ_Azam-TV_lineup.json"),
        ),
        (
            "TZ_Combined",
            include_str!("../../../data/merged/lineups/TZ_Combined_lineup.json"),
        ),
        (
            "TZ_DStv-Premium",
            include_str!("../../../data/merged/lineups/TZ_DStv-Premium_lineup.json"),
        ),
        (
            "UK_Combined",
            include_str!("../../../data/merged/lineups/UK_Combined_lineup.json"),
        ),
        (
            "UK_Freeview",
            include_str!("../../../data/merged/lineups/UK_Freeview_lineup.json"),
        ),
        (
            "UK_SkyTV",
            include_str!("../../../data/merged/lineups/UK_SkyTV_lineup.json"),
        ),
        (
            "UK_SkyTV_ENG_full",
            include_str!("../../../data/merged/lineups/UK_SkyTV_ENG_full_lineup.json"),
        ),
        (
            "UK_SkyTV_ENG_simple",
            include_str!("../../../data/merged/lineups/UK_SkyTV_ENG_simple_lineup.json"),
        ),
        (
            "US_Combined",
            include_str!("../../../data/merged/lineups/US_Combined_lineup.json"),
        ),
        (
            "US_DirecTV-Premier",
            include_str!("../../../data/merged/lineups/US_DirecTV-Premier_lineup.json"),
        ),
        (
            "US_DISH-Top250",
            include_str!("../../../data/merged/lineups/US_DISH-Top250_lineup.json"),
        ),
        (
            "US_Optimum",
            include_str!("../../../data/merged/lineups/US_Optimum_lineup.json"),
        ),
        (
            "US_Spectrum-Tampa-Bay-All",
            include_str!("../../../data/merged/lineups/US_Spectrum-Tampa-Bay-All_lineup.json"),
        ),
        (
            "US_Spectrum-Tampa-Bay-No-Spanish",
            include_str!(
                "../../../data/merged/lineups/US_Spectrum-Tampa-Bay-No-Spanish_lineup.json"
            ),
        ),
        (
            "US_Spectrum-Tampa-Bay-Spanish-Only",
            include_str!(
                "../../../data/merged/lineups/US_Spectrum-Tampa-Bay-Spanish-Only_lineup.json"
            ),
        ),
        (
            "US_Verizon-FIOS",
            include_str!("../../../data/merged/lineups/US_Verizon-FIOS_lineup.json"),
        ),
        (
            "US_Verizon-FIOS-All-11743",
            include_str!("../../../data/merged/lineups/US_Verizon-FIOS-All-11743_lineup.json"),
        ),
    ];

    /// One entry of a merged per-country channel database.
    ///
    /// The dedup key is the verbatim `channel_name` (see the build script);
    /// `sources` names the plugins whose files carried the entry.
    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct MergedChannelEntry {
        pub channel_name: String,
        pub category: Option<String>,
        #[serde(rename = "type")]
        pub kind: Option<String>,
        pub zones: Option<Vec<String>>,
        #[serde(default)]
        pub sources: Vec<String>,
    }

    /// One merged per-country channel database.
    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct MergedChannelDatabase {
        pub country_code: String,
        pub country_name: Option<String>,
        pub version: Option<String>,
        pub channels: Vec<MergedChannelEntry>,
    }

    /// One entry of the merged station table, keyed by full callsign.
    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct MergedStation {
        pub callsign: String,
        pub network_affiliation: Option<String>,
        pub community_served_city: Option<String>,
        pub community_served_state: Option<String>,
        pub tv_virtual_channel: Option<String>,
        pub facility_id: Option<String>,
        pub active_ind: Option<String>,
        pub station_class: Option<String>,
        #[serde(default)]
        pub sources: Vec<String>,
    }

    /// The merged station table plus Janitor's callsign allowlist.
    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct MergedStationsFile {
        pub stations: HashMap<String, MergedStation>,
        pub callsign_allowlist: Vec<String>,
    }

    /// One alias value and the plugins that list it under its canonical.
    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct MergedAliasEntry {
        pub alias: String,
        #[serde(default)]
        pub sources: Vec<String>,
    }

    /// The merged alias table: canonical -> aliases, plus per-country tables.
    ///
    /// The union of every plugin's claims: an alias listed under two or more
    /// canonicals is kept under each (both accept it, as in every plugin) and
    /// recorded in `data/merged/alias_conflicts.json` for the corpus step to
    /// decide reverse (alias -> canonical) lookups.
    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct MergedAliasesFile {
        pub aliases: HashMap<String, Vec<MergedAliasEntry>>,
        pub country_aliases: HashMap<String, HashMap<String, Vec<MergedAliasEntry>>>,
    }

    /// Codes of every merged country database, sorted.
    pub fn merged_codes() -> Vec<&'static str> {
        let mut codes: Vec<&'static str> = MERGED_CHANNEL_DATABASES
            .iter()
            .map(|(code, _)| *code)
            .collect();
        codes.sort_unstable();
        codes
    }

    /// Raw JSON for one merged country database.
    pub fn merged_database_json(code: &str) -> Option<&'static str> {
        MERGED_CHANNEL_DATABASES
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(code))
            .map(|(_, json)| *json)
    }

    /// Names of every merged lineup file (without the `_lineup.json` suffix).
    pub fn merged_lineup_names() -> Vec<&'static str> {
        MERGED_LINEUPS.iter().map(|(name, _)| *name).collect()
    }

    /// Raw JSON for one merged lineup file.
    pub fn merged_lineup_json(name: &str) -> Option<&'static str> {
        MERGED_LINEUPS
            .iter()
            .find(|(candidate, _)| *candidate == name)
            .map(|(_, json)| *json)
    }

    /// The parsed merged station table, cached per process.
    pub fn merged_stations() -> &'static MergedStationsFile {
        static CACHED: OnceLock<MergedStationsFile> = OnceLock::new();
        CACHED.get_or_init(|| {
            serde_json::from_str(MERGED_STATIONS_JSON).expect("merged stations parse")
        })
    }

    /// The parsed merged alias table, cached per process.
    pub fn merged_aliases() -> &'static MergedAliasesFile {
        static CACHED: OnceLock<MergedAliasesFile> = OnceLock::new();
        CACHED.get_or_init(|| {
            serde_json::from_str(MERGED_ALIASES_JSON).expect("merged aliases parse")
        })
    }

    /// The parsed merge provenance log, cached per process.
    pub fn merged_provenance() -> &'static serde_json::Value {
        static CACHED: OnceLock<serde_json::Value> = OnceLock::new();
        CACHED.get_or_init(|| {
            serde_json::from_str(MERGED_PROVENANCE_JSON).expect("provenance parses")
        })
    }

    /// Full-plus-base station keys, as the plugins' `channel_lookup` holds them:
    /// every station under its full callsign and, when different, under its
    /// `normalize_callsign` base form.
    fn merged_station_keys() -> &'static HashSet<String> {
        static CACHED: OnceLock<HashSet<String>> = OnceLock::new();
        CACHED.get_or_init(|| {
            let mut keys = HashSet::new();
            for key in merged_stations().stations.keys() {
                keys.insert(key.clone());
                keys.insert(crate::callsign::normalize_callsign(key));
            }
            keys
        })
    }

    /// Whether this callsign is a real station in the merged table.
    ///
    /// Membership is what Janitor's allowlist vouches for: an exact key hit,
    /// or a hit under the core `normalize_callsign` base form (the merge
    /// report verifies every allowlisted callsign resolves this way, e.g.
    /// allowlisted `KAAH` via merged `KAAH-TV`).
    pub fn is_merged_station(callsign: &str) -> bool {
        let wanted = callsign.trim().to_uppercase();
        if wanted.is_empty() {
            return false;
        }
        merged_station_keys().contains(&wanted)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn provenance_count(path: &str) -> usize {
            merged_provenance()
                .pointer(path)
                .unwrap_or_else(|| panic!("{path} in PROVENANCE.json"))
                .as_u64()
                .unwrap_or_else(|| panic!("{path} is a number")) as usize
        }

        #[test]
        fn every_merged_database_parses_and_matches_provenance() {
            assert_eq!(merged_codes().len(), 16, "{:?}", merged_codes());
            for code in merged_codes() {
                let json = merged_database_json(code).expect("database resolves");
                let db: MergedChannelDatabase =
                    serde_json::from_str(json).unwrap_or_else(|e| panic!("{code}: {e}"));
                assert_eq!(db.country_code, code);
                assert!(!db.channels.is_empty(), "{code} is not empty");
                let expected = provenance_count(&format!("/channels/{code}/rows_out"));
                assert_eq!(db.channels.len(), expected, "{code} count");
                // Lowercase lookup resolves, unknown codes do not.
                assert!(merged_database_json(&code.to_lowercase()).is_some());
            }
            assert_eq!(merged_database_json("ZZ"), None);
            // The US database carries the zones field the merge preserves.
            let us: MergedChannelDatabase =
                serde_json::from_str(merged_database_json("US").expect("US")).expect("US parses");
            assert!(
                us.channels.iter().any(|c| c.zones.is_some()),
                "SM zones survive the merge"
            );
            for entry in &us.channels {
                assert!(
                    !entry.sources.is_empty(),
                    "{} has sources",
                    entry.channel_name
                );
            }
            // Case-only variants are distinct entries in the sources and stay so.
            let afro: Vec<_> = us
                .channels
                .iter()
                .filter(|c| c.channel_name.eq_ignore_ascii_case("afro"))
                .map(|c| (c.channel_name.as_str(), c.kind.as_deref()))
                .collect();
            assert_eq!(
                afro,
                [("AFRO", Some("Regional")), ("Afro", Some("National"))],
                "case variants kept apart"
            );
        }

        #[test]
        fn stations_parse_and_match_provenance() {
            let stations = merged_stations();
            assert_eq!(
                stations.stations.len(),
                provenance_count("/stations/rows_out")
            );
            // CM's table + supplemental only: SM's file is CM's predecessor and
            // its extra rows are cancelled licences, which stay dropped.
            assert_eq!(
                stations.stations.len(),
                provenance_count("/stations/rows_in/CM")
                    + provenance_count("/stations/rows_in/CM-supplemental")
            );
            assert!(!is_merged_station("DWHTV"), "cancelled licence dropped");
            assert_eq!(
                stations.callsign_allowlist.len(),
                provenance_count("/stations/allowlist_in")
            );
            // Every allowlisted callsign resolves, exactly or by base form.
            for callsign in &stations.callsign_allowlist {
                assert!(is_merged_station(callsign), "{callsign} is a station");
                assert!(
                    is_merged_station(&callsign.to_lowercase()),
                    "{callsign} matches case-insensitively"
                );
            }
            assert!(!is_merged_station("ZZZZ"));
            assert!(!is_merged_station(""));
        }

        #[test]
        fn aliases_parse_and_match_provenance_with_conflicts_kept() {
            let aliases = merged_aliases();
            assert_eq!(
                aliases.aliases.len(),
                provenance_count("/aliases/canonicals_out")
            );
            let rows: usize = aliases.aliases.values().map(Vec::len).sum();
            assert_eq!(rows, provenance_count("/aliases/aliases_out"));
            for (country, table) in &aliases.country_aliases {
                let expected =
                    provenance_count(&format!("/aliases/countries/{country}/canonicals_out"));
                assert_eq!(table.len(), expected, "{country} canonicals");
            }
            // A multi-canonical alias is kept under EVERY claimant (union) and
            // listed in alias_conflicts.json for step 5 to decide.
            let detail: serde_json::Value =
                serde_json::from_str(include_str!("../../../data/merged/alias_conflicts.json"))
                    .expect("alias_conflicts parses");
            let conflicts = detail["conflicts"].as_array().expect("conflicts array");
            assert_eq!(conflicts.len(), provenance_count("/aliases/conflict_count"));
            assert!(!conflicts.is_empty(), "the merge records conflicts");
            for conflict in conflicts {
                let alias = conflict["alias"].as_str().expect("alias is a string");
                let claimants = conflict["claimed_by"].as_object().expect("claimants");
                assert!(claimants.len() > 1, "{alias} has several claimants");
                for canonical in claimants.keys() {
                    assert!(
                        aliases.aliases[canonical]
                            .iter()
                            .any(|entry| entry.alias == alias),
                        "{alias} kept under {canonical}"
                    );
                }
            }
            // Behaviour every plugin agrees on survives: FS 1 -> FS1 and Fox Sports 1.
            for canonical in ["FS1", "Fox Sports 1"] {
                assert!(aliases.aliases[canonical].iter().any(|e| e.alias == "FS 1"));
            }
        }

        #[test]
        fn lineups_parse_and_match_provenance() {
            assert_eq!(merged_lineup_names().len(), 29);
            let mut total = 0usize;
            for name in merged_lineup_names() {
                let json = merged_lineup_json(name).expect("lineup resolves");
                let parsed: serde_json::Value =
                    serde_json::from_str(json).unwrap_or_else(|e| panic!("{name}: {e}"));
                let categories = parsed
                    .get("categories")
                    .and_then(|v| v.as_object())
                    .expect("categories");
                assert!(!categories.is_empty(), "{name} has categories");
                total += categories
                    .values()
                    .filter_map(|v| v.as_array())
                    .map(Vec::len)
                    .sum::<usize>();
            }
            assert_eq!(total, provenance_count("/lineups/channels"));
            assert_eq!(merged_lineup_json("XX"), None);
        }
    }
}
