//! Source selection: which of an app's sources a name filter makes eligible.
//!
//! Port of `Plugin._get_filtered_epg_data`'s source half. Guide filters EPG sources, streams
//! filters M3U accounts, and the question is the same one both times: does the operator's
//! comma-separated list name this source, and is the source switched on? The rules that
//! surprise people are the plugin's:
//!
//!   * the list is a FILTER, not a priority order — priority comes from each source's own
//!     Dispatcharr priority, higher winning;
//!   * names are matched case-insensitively, because providers disagree about case;
//!   * a disabled source is skipped even when it is named;
//!   * a list that names nothing at all falls back to every enabled source, with a warning,
//!     rather than failing the run.
//!
//! Filtering the CANDIDATES by source, and ordering them, stays with the app: only it knows
//! what a candidate is and how the two relate.

use crate::dispatcharr::{EpgSource, M3uAccount};
use crate::wildcard::{expand_patterns, parse_tokens};
use std::collections::HashSet;

/// One selectable source: an EPG source for guide, an M3U account for streams.
pub trait SourceKind {
    fn id(&self) -> i64;
    fn name(&self) -> &str;
    /// Whether the source is switched on. A disabled source is never eligible.
    fn enabled(&self) -> bool;
}

impl SourceKind for EpgSource {
    fn id(&self) -> i64 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn enabled(&self) -> bool {
        self.is_active
    }
}

/// An M3U account is the streams app's kind of source, and it answers the same three
/// questions: whether the filter names it, what it is called, and whether it is switched on.
///
/// The streams app adds its own layer on top — an ORDERED priority list, and the plugin's `*`
/// entry meaning every other source — because an account's position decides failover order,
/// which no EPG source has. Eligibility itself is this shared filter's.
impl SourceKind for M3uAccount {
    fn id(&self) -> i64 {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn enabled(&self) -> bool {
        self.is_active
    }
}

/// What a name filter selected.
#[derive(Debug, Clone, Default)]
pub struct Selection {
    /// The sources the filter named, or `None` when it narrowed nothing: an empty filter, or
    /// one that names no source at all, leaves every source selected.
    pub named: Option<HashSet<i64>>,
    /// The sources that are switched on.
    pub enabled: HashSet<i64>,
    /// Filter tokens that matched no source name, for the caller's warning.
    pub unmatched: Vec<String>,
    /// Whether the filter named nothing usable and every source was selected instead.
    pub fell_back: bool,
}

impl Selection {
    /// Whether a candidate from this source may be used.
    pub fn is_eligible(&self, source: i64) -> bool {
        self.named
            .as_ref()
            .map(|named| named.contains(&source))
            .unwrap_or(true)
            && self.enabled.contains(&source)
    }
}

/// Resolve a source filter against the sources an install has.
///
/// `named` keeps disabled sources the filter named, so a caller that counts what the filter
/// excluded can still tell "the operator named it and it is off" from "the operator did not
/// name it". Use [`Selection::is_eligible`] for the actual decision.
pub fn select<S: SourceKind>(sources: &[S], filter: &str) -> Selection {
    let enabled: HashSet<i64> = sources
        .iter()
        .filter(|source| source.enabled())
        .map(SourceKind::id)
        .collect();

    let tokens = parse_tokens(filter);
    if tokens.is_empty() {
        return Selection {
            named: None,
            enabled,
            ..Selection::default()
        };
    }

    let names: Vec<String> = sources
        .iter()
        .map(|source| source.name().to_string())
        .collect();
    let (matched, unmatched) = expand_patterns(&tokens, &names, true);
    if !unmatched.is_empty() {
        tracing::warn!(
            "invalid source name(s): {}; available: {}",
            unmatched.join(", "),
            names.join(", ")
        );
    }

    let named: HashSet<i64> = sources
        .iter()
        .filter(|source| matched.iter().any(|name| name == source.name()))
        .map(SourceKind::id)
        .collect();
    if named.is_empty() {
        tracing::warn!("no valid sources in {filter:?}; using every active source");
        return Selection {
            named: None,
            enabled,
            unmatched,
            fell_back: true,
        };
    }

    Selection {
        named: Some(named),
        enabled,
        unmatched,
        fell_back: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(id: i64, name: &str, enabled: bool) -> EpgSource {
        EpgSource {
            id,
            name: name.to_string(),
            is_active: enabled,
            priority: 10 - id,
            ..EpgSource::default()
        }
    }

    fn sources() -> Vec<EpgSource> {
        vec![
            source(0, "Schedules Direct", true),
            source(1, "XMLTV", true),
            source(2, "Old Source", false),
        ]
    }

    #[test]
    fn an_empty_filter_selects_every_source() {
        for filter in ["", "   "] {
            let selection = select(&sources(), filter);
            assert_eq!(selection.named, None, "{filter:?}");
            assert_eq!(selection.enabled.len(), 2);
            assert!(selection.unmatched.is_empty());
            assert!(!selection.fell_back);
            assert!(selection.is_eligible(0) && selection.is_eligible(1));
        }
    }

    #[test]
    fn the_filter_selects_and_is_case_insensitive() {
        let selection = select(&sources(), "xmltv");
        assert_eq!(selection.named, Some(HashSet::from([1])));
        assert!(selection.is_eligible(1));
        assert!(!selection.is_eligible(0));

        let selection = select(&sources(), "*Direct");
        assert_eq!(selection.named, Some(HashSet::from([0])));
        assert!(selection.is_eligible(0));
    }

    #[test]
    fn an_unknown_filter_falls_back_to_every_source() {
        let selection = select(&sources(), "Nope");
        assert!(selection.fell_back);
        assert_eq!(selection.unmatched, vec!["Nope".to_string()]);
        assert_eq!(selection.named, None, "the plugin proceeds with everything");
        assert!(
            selection.is_eligible(0),
            "the plugin proceeds with everything"
        );
    }

    #[test]
    fn an_m3u_account_is_a_source_too() {
        // The streams app filters M3U accounts with this same function; only what a source IS
        // differs, so the rules (named, switched on, `*` wildcards, case-insensitive names)
        // are the ones guide already proved.
        let accounts = vec![
            M3uAccount {
                id: 1,
                name: "Main Provider".to_string(),
                is_active: true,
                ..M3uAccount::default()
            },
            M3uAccount {
                id: 2,
                name: "Retired Provider".to_string(),
                is_active: false,
                ..M3uAccount::default()
            },
        ];

        let selection = select(&accounts, "");
        assert_eq!(selection.named, None);
        assert!(selection.is_eligible(1));
        assert!(
            !selection.is_eligible(2),
            "a switched-off account is skipped"
        );

        let selection = select(&accounts, "main provider");
        assert_eq!(selection.named, Some(HashSet::from([1])));
        assert!(selection.is_eligible(1));
        assert!(!selection.is_eligible(2));

        let selection = select(&accounts, "Retired Provider");
        assert_eq!(selection.named, Some(HashSet::from([2])));
        assert!(!selection.is_eligible(2), "named and off is still skipped");
    }

    #[test]
    fn a_disabled_source_is_named_but_never_eligible() {
        let selection = select(&sources(), "Old Source");
        assert_eq!(selection.named, Some(HashSet::from([2])));
        assert!(
            !selection.is_eligible(2),
            "a named source that is off is skipped"
        );
        assert!(
            !selection.is_eligible(0),
            "nothing it did not name is eligible"
        );
    }
}
