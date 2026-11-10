//! Scope: which EPG entries a run may match against.
//!
//! Port of `Plugin._get_filtered_epg_data`. The channel half of the scope — profiles, groups
//! and ignored groups — is the same question for every sub-app and lives in
//! `crosstalk_core::scope`; this module is the EPG-specific half, which only guide has.
//!
//! The rules are faithful to the plugin, including the ones that surprise people:
//!
//!   * an EPG source list is a FILTER, not a priority order — priority comes from each
//!     source's own Dispatcharr priority, higher winning;
//!   * a disabled source is skipped, and leaving the filter empty makes every active source
//!     eligible, foreign-country guides included;
//!   * channel database selection is a separate decision that never widens the candidate set
//!     at run time; it only decides which reference names the matcher knows.

use crate::settings::GuideSettings;
use crosstalk_core::dispatcharr::{EpgEntry, EpgSource, World};
use crosstalk_core::scope::sources;
use std::collections::{HashMap, HashSet};

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
/// `epg_sources_to_match` selects WHICH sources are eligible (see
/// [`crosstalk_core::scope::sources`]); it is never a priority list. Candidates are then
/// ordered by each source's Dispatcharr priority, higher first, and ties keep their original
/// order.
pub fn scope_entries(world: &World, filter: &str) -> ScopedEntries {
    let sources_by_id: HashMap<i64, EpgSource> = world
        .sources
        .iter()
        .map(|source| (source.id, source.clone()))
        .collect();

    let selection = sources::select(&world.sources, filter);
    let mut candidates: Vec<EpgEntry> = world.entries.clone();
    if let Some(named) = &selection.named {
        candidates.retain(|entry| {
            entry
                .epg_source
                .is_some_and(|source| named.contains(&source))
        });
    }

    let before = candidates.len();
    candidates.retain(|entry| {
        entry
            .epg_source
            .is_some_and(|source| selection.enabled.contains(&source))
    });
    let excluded_inactive = before - candidates.len();

    // Stable sort by priority, higher first: ties keep the order the API returned them in.
    candidates.sort_by_key(|entry| {
        std::cmp::Reverse(
            entry
                .epg_source
                .and_then(|source| sources_by_id.get(&source))
                .map(|source| source.priority)
                .unwrap_or(0),
        )
    });

    let mut present: Vec<&EpgSource> = {
        let ids: HashSet<i64> = candidates
            .iter()
            .filter_map(|entry| entry.epg_source)
            .collect();
        ids.iter().filter_map(|id| sources_by_id.get(id)).collect()
    };
    present.sort_by_key(|source| std::cmp::Reverse(source.priority));
    let priority_order = present
        .iter()
        .map(|source| format!("{} ({})", source.name, source.priority))
        .collect::<Vec<_>>()
        .join(", ");

    ScopedEntries {
        entries: candidates,
        sources: sources_by_id,
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
    use crosstalk_core::dispatcharr::World;

    fn world() -> World {
        World {
            sources: vec![
                EpgSource {
                    id: 100,
                    name: "Schedules Direct".to_string(),
                    is_active: true,
                    priority: 10,
                    ..EpgSource::default()
                },
                EpgSource {
                    id: 101,
                    name: "XMLTV".to_string(),
                    is_active: true,
                    priority: 5,
                    ..EpgSource::default()
                },
                EpgSource {
                    id: 102,
                    name: "Old Source".to_string(),
                    is_active: false,
                    priority: 20,
                    ..EpgSource::default()
                },
            ],
            entries: vec![
                EpgEntry {
                    id: 1,
                    name: "ESPN".to_string(),
                    tvg_id: "espn.us".to_string(),
                    epg_source: Some(101),
                    icon_url: None,
                },
                EpgEntry {
                    id: 2,
                    name: "ESPN HD".to_string(),
                    tvg_id: "espn.hd".to_string(),
                    epg_source: Some(100),
                    icon_url: None,
                },
                EpgEntry {
                    id: 3,
                    name: "Gone".to_string(),
                    tvg_id: "gone".to_string(),
                    epg_source: Some(102),
                    icon_url: None,
                },
            ],
            ..World::default()
        }
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
            ..GuideSettings::default()
        };
        assert_eq!(heal_source_filter(&settings), "Schedules Direct");

        let settings = GuideSettings {
            epg_sources_to_match: "Schedules Direct".to_string(),
            heal_fallback_sources: "XMLTV".to_string(),
            ..GuideSettings::default()
        };
        assert_eq!(heal_source_filter(&settings), "XMLTV");
    }
}
