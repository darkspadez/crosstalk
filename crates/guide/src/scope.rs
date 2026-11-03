//! Scope: which channels and which EPG entries a run may touch.
//!
//! Port of `Plugin._validate_and_filter_groups` and `_get_filtered_epg_data`. The rules are
//! faithful to the plugin, including the ones that surprise people:
//!
//!   * `Channel Groups` and `Ignore Groups` are mutually exclusive, and using both raises;
//!   * an EPG source list is a FILTER, not a priority order — priority comes from each
//!     source's own Dispatcharr priority, higher winning;
//!   * a disabled source is skipped, and leaving the filter empty makes every active source
//!     eligible, foreign-country guides included;
//!   * channel database selection is a separate decision that never widens the candidate set
//!     at run time; it only decides which reference names the matcher knows.

use crate::settings::GuideSettings;
use anyhow::{bail, Result};
use crosstalk_core::dispatcharr::{
    Channel, ChannelGroup, ChannelProfile, EpgEntry, EpgSource, World,
};
use crosstalk_core::wildcard::{expand_patterns, parse_tokens};
use std::collections::{HashMap, HashSet};

/// The channels a run will process, with the filter that produced them described in words.
#[derive(Debug, Clone, Default)]
pub struct ScopedChannels {
    pub channels: Vec<Channel>,
    /// ` in groups 'A', 'B'` or ` ignoring groups 'PPV'`, for the report and the log.
    pub filter_description: String,
    /// The profile names that were found, for the log.
    pub profiles_used: Vec<String>,
}

/// Resolve the channel scope.
///
/// Fails with the plugin's own wording when both group settings are set, when a named profile
/// does not exist, or when no group matches: those are configuration mistakes an operator can
/// fix, not empty results to be explained later.
pub fn scope_channels(world: &World, settings: &GuideSettings) -> Result<ScopedChannels> {
    let selected = settings.selected_groups.trim();
    let ignored = settings.ignore_groups.trim();
    if !selected.is_empty() && !ignored.is_empty() {
        bail!(
            "Cannot use both 'Channel Groups' and 'Ignore Groups' at the same time. Please use \
             only one."
        );
    }

    let mut channels = world.channels.clone();
    let mut filter_description = String::new();
    let mut profiles_used = Vec::new();

    // Profiles first: they are the coarsest filter and the plugin applied them in this order.
    let want_profiles = parse_tokens(&settings.channel_profile_name);
    if !want_profiles.is_empty() {
        let visible = profile_scope(world, &want_profiles)?;
        profiles_used = visible.found.clone();
        channels.retain(|channel| visible.ids.contains(&channel.id));
        filter_description.push_str(&visible.description);
    }

    let group_names: Vec<String> = world
        .groups
        .iter()
        .map(|group| group.name.clone())
        .collect();
    let group_by_name: HashMap<&str, &ChannelGroup> = world
        .groups
        .iter()
        .map(|group| (group.name.as_str(), group))
        .collect();

    if !selected.is_empty() {
        let tokens = parse_tokens(selected);
        // Group names are matched case-sensitively, like the plugin: `Sports` and `SPORTS` are
        // different groups in Dispatcharr.
        let (matched, _unmatched) = expand_patterns(&tokens, &group_names, false);
        if matched.is_empty() {
            bail!(
                "None of the specified groups were found: {}",
                tokens.join(", ")
            );
        }
        let ids: HashSet<i64> = matched
            .iter()
            .filter_map(|name| group_by_name.get(name.as_str()).map(|group| group.id))
            .collect();
        channels.retain(|channel| channel.channel_group_id.is_some_and(|id| ids.contains(&id)));
        filter_description.push_str(&phrase(&matched, "in", "groups", false));
    } else if !ignored.is_empty() {
        let tokens = parse_tokens(ignored);
        let (matched, _unmatched) = expand_patterns(&tokens, &group_names, false);
        let ids: HashSet<i64> = matched
            .iter()
            .filter_map(|name| group_by_name.get(name.as_str()).map(|group| group.id))
            .collect();
        if !ids.is_empty() {
            channels
                .retain(|channel| !channel.channel_group_id.is_some_and(|id| ids.contains(&id)));
        }
        let resolved = if matched.is_empty() { tokens } else { matched };
        filter_description.push_str(&phrase(&resolved, "ignoring", "groups", true));
    }

    Ok(ScopedChannels {
        channels,
        filter_description,
        profiles_used,
    })
}

struct ProfileScope {
    ids: HashSet<i64>,
    found: Vec<String>,
    description: String,
}

fn profile_scope(world: &World, wanted: &[String]) -> Result<ProfileScope> {
    let by_name: HashMap<String, &ChannelProfile> = world
        .profiles
        .iter()
        .map(|profile| (profile.name.trim().to_uppercase(), profile))
        .collect();

    let mut ids = HashSet::new();
    let mut found = Vec::new();
    let mut missing = Vec::new();

    for name in wanted {
        match by_name.get(&name.trim().to_uppercase()) {
            Some(profile) => {
                // Only a profile that actually contributes channels counts as "found": a
                // profile with nothing visible in it is the case the error below reports.
                ids.extend(profile.channels.iter().copied());
                if !profile.channels.is_empty() {
                    found.push(profile.name.clone());
                }
            }
            None => missing.push(name.clone()),
        }
    }

    if ids.is_empty() {
        // Two different mistakes, and the plugin distinguishes them: no such profile at all,
        // versus a profile that exists but has nothing enabled in it.
        if !missing.is_empty() && found.is_empty() {
            let available: Vec<&str> = world
                .profiles
                .iter()
                .map(|profile| profile.name.as_str())
                .collect();
            bail!(
                "None of the specified Channel Profiles were found: {}. Available profiles: {}",
                wanted.join(", "),
                if available.is_empty() {
                    "(none)".to_string()
                } else {
                    available.join(", ")
                }
            );
        }
        bail!(
            "The specified Channel Profile(s) have no visible channels: {}",
            wanted.join(", ")
        );
    }

    if !missing.is_empty() {
        tracing::warn!(
            "profile(s) not found: {}; available: {}",
            missing.join(", "),
            world
                .profiles
                .iter()
                .map(|profile| profile.name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let description = if found.len() == 1 {
        format!(" in profile '{}'", found[0])
    } else {
        phrase(&found, "in", "profiles", false)
    };

    Ok(ProfileScope {
        ids,
        found,
        description,
    })
}

/// ` in groups 'A', 'B'`, quoting a single item or listing several.
fn phrase(items: &[String], lead: &str, noun: &str, parens: bool) -> String {
    if items.is_empty() {
        return String::new();
    }
    let quoted: Vec<String> = items.iter().map(|item| format!("'{item}'")).collect();
    let plural = if items.len() == 1 {
        noun.to_string()
    } else {
        format!("{noun}s")
    };
    if parens {
        format!(" ({lead} {} {plural})", quoted.join(", "))
    } else {
        format!(" {lead} {} {plural}", quoted.join(", "))
    }
}

/// The EPG candidates a run may match against, ordered by source priority.
#[derive(Debug, Clone, Default)]
pub struct ScopedEntries {
    pub entries: Vec<EpgEntry>,
    /// Source id -> source, for reporting the source name of a match.
    pub sources: HashMap<i64, EpgSource>,
    /// `Name (priority)` list, high to low, for the log.
    pub priority_order: String,
    /// How many entries were dropped because their source is disabled.
    pub excluded_inactive: usize,
}

/// Resolve the EPG scope.
///
/// `epg_sources_to_match` selects WHICH sources are eligible; it is never a priority list.
/// Candidates are then ordered by each source's Dispatcharr priority, higher first, and ties
/// keep their original order.
pub fn scope_entries(world: &World, filter: &str) -> ScopedEntries {
    let sources: HashMap<i64, EpgSource> = world
        .sources
        .iter()
        .map(|source| (source.id, source.clone()))
        .collect();
    let active: HashSet<i64> = world
        .sources
        .iter()
        .filter(|source| source.is_active)
        .map(|source| source.id)
        .collect();

    let mut candidates: Vec<EpgEntry> = world.entries.clone();

    let tokens = parse_tokens(filter);
    if !tokens.is_empty() {
        let names: Vec<String> = world
            .sources
            .iter()
            .map(|source| source.name.clone())
            .collect();
        // Source names are matched case-insensitively: providers disagree about case, and an
        // operator typing `schedules direct` means the source called `Schedules Direct`.
        let (matched, unmatched) = expand_patterns(&tokens, &names, true);
        if !unmatched.is_empty() {
            tracing::warn!(
                "invalid EPG source name(s): {}; available: {}",
                unmatched.join(", "),
                names.join(", ")
            );
        }
        let eligible: HashSet<i64> = world
            .sources
            .iter()
            .filter(|source| matched.contains(&source.name))
            .map(|source| source.id)
            .collect();
        if eligible.is_empty() {
            tracing::warn!("no valid EPG sources in {filter:?}; using every active source");
        } else {
            candidates.retain(|entry| {
                entry
                    .epg_source
                    .is_some_and(|source| eligible.contains(&source))
            });
        }
    }

    let before = candidates.len();
    candidates.retain(|entry| {
        entry
            .epg_source
            .is_some_and(|source| active.contains(&source))
    });
    let excluded_inactive = before - candidates.len();

    // Stable sort by priority, higher first: ties keep the order the API returned them in.
    candidates.sort_by_key(|entry| {
        std::cmp::Reverse(
            entry
                .epg_source
                .and_then(|source| sources.get(&source))
                .map(|source| source.priority)
                .unwrap_or(0),
        )
    });

    let mut present: Vec<&EpgSource> = {
        let ids: HashSet<i64> = candidates
            .iter()
            .filter_map(|entry| entry.epg_source)
            .collect();
        ids.iter().filter_map(|id| sources.get(id)).collect()
    };
    present.sort_by_key(|source| std::cmp::Reverse(source.priority));
    let priority_order = present
        .iter()
        .map(|source| format!("{} ({})", source.name, source.priority))
        .collect::<Vec<_>>()
        .join(", ");

    ScopedEntries {
        entries: candidates,
        sources,
        priority_order,
        excluded_inactive,
    }
}

/// The fallback source filter heal should search, which falls back to the auto-match filter.
///
/// Mirrors the plugin: an empty heal filter means "the same sources auto-match uses".
pub fn heal_source_filter(settings: &GuideSettings) -> &str {
    let heal = settings.heal_fallback_sources.trim();
    if heal.is_empty() {
        settings.epg_sources_to_match.trim()
    } else {
        heal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel(id: i64, name: &str, group: Option<i64>) -> Channel {
        Channel {
            id,
            name: name.to_string(),
            channel_group_id: group,
            ..Channel::default()
        }
    }

    fn world() -> World {
        World {
            channels: vec![
                channel(1, "ESPN", Some(10)),
                channel(2, "CNN", Some(10)),
                channel(3, "PPV 1", Some(20)),
                channel(4, "Local", None),
            ],
            groups: vec![
                ChannelGroup {
                    id: 10,
                    name: "Sports".to_string(),
                },
                ChannelGroup {
                    id: 20,
                    name: "PPV Live".to_string(),
                },
            ],
            profiles: vec![
                ChannelProfile {
                    id: 1,
                    name: "All".to_string(),
                    channels: vec![1, 2, 3, 4],
                },
                ChannelProfile {
                    id: 2,
                    name: "Favorites".to_string(),
                    channels: vec![1],
                },
                ChannelProfile {
                    id: 3,
                    name: "Empty".to_string(),
                    channels: vec![],
                },
            ],
            sources: vec![
                EpgSource {
                    id: 100,
                    name: "Schedules Direct".to_string(),
                    is_active: true,
                    priority: 10,
                },
                EpgSource {
                    id: 101,
                    name: "XMLTV".to_string(),
                    is_active: true,
                    priority: 5,
                },
                EpgSource {
                    id: 102,
                    name: "Old Source".to_string(),
                    is_active: false,
                    priority: 20,
                },
            ],
            entries: vec![
                EpgEntry {
                    id: 1,
                    name: "ESPN".to_string(),
                    tvg_id: "espn.us".to_string(),
                    epg_source: Some(101),
                },
                EpgEntry {
                    id: 2,
                    name: "ESPN HD".to_string(),
                    tvg_id: "espn.hd".to_string(),
                    epg_source: Some(100),
                },
                EpgEntry {
                    id: 3,
                    name: "Gone".to_string(),
                    tvg_id: "gone".to_string(),
                    epg_source: Some(102),
                },
            ],
        }
    }

    fn defaults() -> GuideSettings {
        GuideSettings::default()
    }

    #[test]
    fn no_filters_means_every_channel() {
        let scoped = scope_channels(&world(), &defaults()).expect("ok");
        assert_eq!(scoped.channels.len(), 4);
        assert_eq!(scoped.filter_description, "");
    }

    #[test]
    fn both_group_settings_is_an_error() {
        let settings = GuideSettings {
            selected_groups: "Sports".to_string(),
            ignore_groups: "PPV*".to_string(),
            ..defaults()
        };
        let error = scope_channels(&world(), &settings).expect_err("rejects");
        assert!(format!("{error}").contains("Cannot use both"));
    }

    #[test]
    fn selected_groups_filter_and_describe() {
        let settings = GuideSettings {
            selected_groups: "Sports".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 2);
        assert_eq!(scoped.filter_description, " in 'Sports' groups");
    }

    #[test]
    fn selected_groups_accept_wildcards() {
        let settings = GuideSettings {
            selected_groups: "Spo*".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 2);
    }

    #[test]
    fn an_unknown_selected_group_is_an_error() {
        let settings = GuideSettings {
            selected_groups: "Nope".to_string(),
            ..defaults()
        };
        let error = scope_channels(&world(), &settings).expect_err("rejects");
        assert!(format!("{error}").contains("None of the specified groups"));
    }

    #[test]
    fn ignored_groups_exclude_and_describe() {
        let settings = GuideSettings {
            ignore_groups: "PPV*".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 3);
        assert!(scoped.filter_description.contains("ignoring"));
        assert!(scoped.filter_description.contains("'PPV Live'"));
    }

    #[test]
    fn an_unknown_ignored_group_only_warns() {
        let settings = GuideSettings {
            ignore_groups: "Nope".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 4);
    }

    #[test]
    fn profiles_are_matched_case_insensitively() {
        let settings = GuideSettings {
            channel_profile_name: "favorites".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 1);
        assert_eq!(scoped.channels[0].name, "ESPN");
        assert_eq!(scoped.filter_description, " in profile 'Favorites'");
    }

    #[test]
    fn several_profiles_union_their_channels() {
        let settings = GuideSettings {
            channel_profile_name: "Favorites, Empty".to_string(),
            ..defaults()
        };
        // `Empty` contributes nothing but does not fail, because `Favorites` found channels.
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 1);
        assert_eq!(scoped.profiles_used, vec!["Favorites".to_string()]);
    }

    #[test]
    fn an_unknown_profile_is_an_error_naming_the_available_ones() {
        let settings = GuideSettings {
            channel_profile_name: "Nope".to_string(),
            ..defaults()
        };
        let error = scope_channels(&world(), &settings).expect_err("rejects");
        let message = format!("{error}");
        assert!(message.contains("None of the specified Channel Profiles"));
        assert!(message.contains("All"), "{message}");
    }

    #[test]
    fn an_empty_profile_is_an_error() {
        let settings = GuideSettings {
            channel_profile_name: "Empty".to_string(),
            ..defaults()
        };
        let error = scope_channels(&world(), &settings).expect_err("rejects");
        assert!(format!("{error}").contains("no visible channels"));
    }

    #[test]
    fn profile_and_group_filters_combine() {
        let settings = GuideSettings {
            channel_profile_name: "All".to_string(),
            selected_groups: "PPV*".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 1);
        assert_eq!(scoped.channels[0].name, "PPV 1");
        assert!(scoped.filter_description.contains("in profile 'All'"));
    }

    #[test]
    fn entries_are_ordered_by_source_priority() {
        let scoped = scope_entries(&world(), "");
        // The disabled source is dropped, and the higher-priority source comes first.
        assert_eq!(scoped.excluded_inactive, 1);
        assert_eq!(scoped.entries.len(), 2);
        assert_eq!(scoped.entries[0].epg_source, Some(100), "priority 10 first");
        assert_eq!(scoped.entries[1].epg_source, Some(101));
        assert_eq!(scoped.priority_order, "Schedules Direct (10), XMLTV (5)");
    }

    #[test]
    fn an_empty_source_filter_keeps_every_active_source() {
        let scoped = scope_entries(&world(), "");
        assert_eq!(scoped.entries.len(), 2);

        let scoped = scope_entries(&world(), "   ");
        assert_eq!(scoped.entries.len(), 2);
    }

    #[test]
    fn the_source_filter_selects_and_is_case_insensitive() {
        let scoped = scope_entries(&world(), "xmltv");
        assert_eq!(scoped.entries.len(), 1);
        assert_eq!(scoped.entries[0].id, 1);

        let scoped = scope_entries(&world(), "*Direct");
        assert_eq!(scoped.entries.len(), 1);
        assert_eq!(scoped.entries[0].id, 2);
    }

    #[test]
    fn an_unknown_source_filter_falls_back_to_every_active_source() {
        let scoped = scope_entries(&world(), "Nope");
        assert_eq!(
            scoped.entries.len(),
            2,
            "the plugin proceeds with everything"
        );
    }

    #[test]
    fn a_disabled_source_is_skipped_even_when_named() {
        let scoped = scope_entries(&world(), "Old Source");
        assert!(scoped.entries.is_empty());
        assert_eq!(scoped.excluded_inactive, 1);
    }

    #[test]
    fn equal_priorities_keep_their_original_order() {
        let mut world = world();
        world.sources[0].priority = 5; // tie with XMLTV
        let scoped = scope_entries(&world, "");
        // Entries came in as XMLTV (101) then Schedules Direct (100); a stable sort keeps that.
        assert_eq!(scoped.entries[0].epg_source, Some(101));
        assert_eq!(scoped.entries[1].epg_source, Some(100));
    }

    #[test]
    fn heal_falls_back_to_the_match_filter() {
        let settings = GuideSettings {
            epg_sources_to_match: "Schedules Direct".to_string(),
            heal_fallback_sources: String::new(),
            ..defaults()
        };
        assert_eq!(heal_source_filter(&settings), "Schedules Direct");

        let settings = GuideSettings {
            epg_sources_to_match: "Schedules Direct".to_string(),
            heal_fallback_sources: "XMLTV".to_string(),
            ..defaults()
        };
        assert_eq!(heal_source_filter(&settings), "XMLTV");
    }
}
