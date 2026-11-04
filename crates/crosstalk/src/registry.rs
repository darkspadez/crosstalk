//! The sub-apps this binary links in.
//!
//! A new sub-app is a crate plus one line here. The registry lives in the binary rather
//! than in `crosstalk-core`, because a sub-app depends on the core and the core cannot
//! depend back on it.

use crosstalk_core::subapp::Registry;
use std::sync::Arc;

/// Every sub-app compiled into this binary.
pub fn builtin() -> Registry {
    Registry::new(vec![
        Arc::new(crosstalk_guide::GuideApp::new()),
        Arc::new(crosstalk_streams::StreamsApp::new()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guide_is_linked_in() {
        let registry = builtin();
        let app = registry.get("guide").expect("guide is linked");
        assert_eq!(app.name(), "Guide");
        assert!(!app.actions().is_empty());
        assert!(!app.schema().fields().is_empty());
    }

    #[test]
    fn streams_is_linked_in() {
        // The dashboard, the settings form, `crosstalk validate` and `crosstalk run streams
        // status` all come off the registry, so being here is the whole wiring.
        let registry = builtin();
        let app = registry.get("streams").expect("streams is linked");
        assert_eq!(app.name(), "Streams");
        assert!(app.description().contains("stream"));
        let ids: Vec<&str> = app.actions().iter().map(|action| action.id).collect();
        assert_eq!(
            ids,
            vec![
                "validate",
                "status",
                "match_preview",
                "match_apply",
                "sort_preview",
                "sort_apply",
                "mirror_preview",
                "mirror_apply",
                "visibility_preview",
                "visibility_apply",
                "check_countries",
                "scan_placeholders",
                "test_rules"
            ]
        );
        // Every section of the plan's settings layout is in the form.
        let sections: Vec<&str> = app.schema().sections().map(|s| s.label).collect();
        assert_eq!(
            sections,
            vec![
                "Stream Selection",
                "Match",
                "EPG-Aware Placeholder Matching",
                "Sort",
                "Probe",
                "Triage",
            ]
        );
        assert_eq!(
            app.settings_path(std::path::Path::new("/config")),
            std::path::Path::new("/config/streams.toml")
        );
    }

    #[test]
    fn export_prefixes_must_not_nest() {
        // One prefix that starts another makes single-prefix matching ambiguous: every
        // `guide_extra_*` file also matches `guide_`, so retention or a `--app guide` clear
        // could delete the wrong app's reports. Ownership falls back to the longest match
        // (see `csv_report::owning_prefix`), but the rule here keeps the ambiguous case from
        // ever being registered. Add the `guide_extra` app and this fails first, on purpose.
        let prefixes: Vec<String> = builtin().iter().map(|app| app.export_prefix()).collect();
        for left in &prefixes {
            for right in &prefixes {
                if left != right {
                    assert!(
                        !right.starts_with(left.as_str()),
                        "export prefix {left:?} names every {right:?} file too: ids must not nest"
                    );
                }
            }
        }
    }

    #[test]
    fn every_action_id_is_unique_within_its_app() {
        for app in builtin().iter() {
            let mut seen = std::collections::HashSet::new();
            for action in app.actions() {
                assert!(
                    seen.insert(action.id),
                    "{} declares {} twice",
                    app.id(),
                    action.id
                );
            }
        }
    }
}
