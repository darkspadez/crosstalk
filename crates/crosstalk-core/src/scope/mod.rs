//! Scope: which channels a run may touch.
//!
//! Port of `Plugin._validate_and_filter_groups`. This is the channel half of the scope, and
//! it lives in the host because every sub-app asks the same question: profiles, groups and
//! ignored groups select channels, never candidates. The rules are faithful to the plugin,
//! including the one that surprises people: `Channel Groups` and `Ignore Groups` are
//! mutually exclusive, and using both raises.
//!
//! Candidate selection is a different question and a different shape: an app filters its own
//! kind of source (EPG sources for guide, M3U accounts for streams), so that half is in
//! [`sources`].

pub mod sources;

use crate::config::ScopeConfig;
use crate::dispatcharr::{Channel, ChannelGroup, ChannelProfile, World};
use crate::wildcard::{expand_patterns, parse_tokens};
use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};

/// The channels a run will process, with the filter that produced them described in words.
#[derive(Debug, Clone, Default)]
pub struct ScopedChannels {
    pub channels: Vec<Channel>,
    /// ` in groups 'A', 'B'` or ` ignoring groups 'PPV'`, for the report and the log.
    pub filter_description: String,
    /// The profile names that were found, for the log.
    pub profiles_used: Vec<String>,
    /// `ignore_groups` tokens that matched no group. Such a token excludes nothing, so a
    /// write refuses to run on it (see `channel_ops::Plan::scope_warnings`).
    pub unmatched_ignore: Vec<String>,
}

/// Resolve the channel scope.
///
/// Fails with the plugin's own wording when both group settings are set, when a named profile
/// does not exist, or when no group matches: those are configuration mistakes an operator can
/// fix, not empty results to be explained later.
pub fn scope_channels(world: &World, scope: &ScopeConfig) -> Result<ScopedChannels> {
    let selected = scope.groups.trim();
    let ignored = scope.ignore_groups.trim();
    if !selected.is_empty() && !ignored.is_empty() {
        bail!(
            "Cannot use both 'Channel Groups' and 'Ignore Groups' at the same time. Please use \
             only one."
        );
    }

    let mut channels = world.channels.clone();
    let mut filter_description = String::new();
    let mut profiles_used = Vec::new();
    let mut unmatched_ignore = Vec::new();

    // Profiles first: they are the coarsest filter and the plugin applied them in this order.
    let want_profiles = parse_tokens(&scope.profiles);
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
        let (matched, unmatched) = expand_patterns(&tokens, &group_names, false);
        unmatched_ignore = unmatched;
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
        unmatched_ignore,
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
            ..World::default()
        }
    }

    fn defaults() -> ScopeConfig {
        ScopeConfig::default()
    }

    #[test]
    fn no_filters_means_every_channel() {
        let scoped = scope_channels(&world(), &defaults()).expect("ok");
        assert_eq!(scoped.channels.len(), 4);
        assert_eq!(scoped.filter_description, "");
    }

    #[test]
    fn both_group_settings_is_an_error() {
        let settings = ScopeConfig {
            groups: "Sports".to_string(),
            ignore_groups: "PPV*".to_string(),
            ..defaults()
        };
        let error = scope_channels(&world(), &settings).expect_err("rejects");
        assert!(format!("{error}").contains("Cannot use both"));
    }

    #[test]
    fn selected_groups_filter_and_describe() {
        let settings = ScopeConfig {
            groups: "Sports".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 2);
        assert_eq!(scoped.filter_description, " in 'Sports' groups");
    }

    #[test]
    fn selected_groups_accept_wildcards() {
        let settings = ScopeConfig {
            groups: "Spo*".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 2);
    }

    #[test]
    fn an_unknown_selected_group_is_an_error() {
        let settings = ScopeConfig {
            groups: "Nope".to_string(),
            ..defaults()
        };
        let error = scope_channels(&world(), &settings).expect_err("rejects");
        assert!(format!("{error}").contains("None of the specified groups"));
    }

    #[test]
    fn ignored_groups_exclude_and_describe() {
        let settings = ScopeConfig {
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
        let settings = ScopeConfig {
            ignore_groups: "Nope".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 4);
        // S6: the token is kept, so a write can refuse to run on a scope that excludes nothing.
        assert_eq!(scoped.unmatched_ignore, vec!["Nope".to_string()]);
    }

    #[test]
    fn profiles_are_matched_case_insensitively() {
        let settings = ScopeConfig {
            profiles: "favorites".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 1);
        assert_eq!(scoped.channels[0].name, "ESPN");
        assert_eq!(scoped.filter_description, " in profile 'Favorites'");
    }

    #[test]
    fn several_profiles_union_their_channels() {
        let settings = ScopeConfig {
            profiles: "Favorites, Empty".to_string(),
            ..defaults()
        };
        // `Empty` contributes nothing but does not fail, because `Favorites` found channels.
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 1);
        assert_eq!(scoped.profiles_used, vec!["Favorites".to_string()]);
    }

    #[test]
    fn an_unknown_profile_is_an_error_naming_the_available_ones() {
        let settings = ScopeConfig {
            profiles: "Nope".to_string(),
            ..defaults()
        };
        let error = scope_channels(&world(), &settings).expect_err("rejects");
        let message = format!("{error}");
        assert!(message.contains("None of the specified Channel Profiles"));
        assert!(message.contains("All"), "{message}");
    }

    #[test]
    fn an_empty_profile_is_an_error() {
        let settings = ScopeConfig {
            profiles: "Empty".to_string(),
            ..defaults()
        };
        let error = scope_channels(&world(), &settings).expect_err("rejects");
        assert!(format!("{error}").contains("no visible channels"));
    }

    #[test]
    fn profile_and_group_filters_combine() {
        let settings = ScopeConfig {
            profiles: "All".to_string(),
            groups: "PPV*".to_string(),
            ..defaults()
        };
        let scoped = scope_channels(&world(), &settings).expect("ok");
        assert_eq!(scoped.channels.len(), 1);
        assert_eq!(scoped.channels[0].name, "PPV 1");
        assert!(scoped.filter_description.contains("in profile 'All'"));
    }
}
