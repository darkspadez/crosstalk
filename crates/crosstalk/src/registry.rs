//! The sub-apps this binary links in.
//!
//! A new sub-app is a crate plus one line here. The registry lives in the binary rather
//! than in `crosstalk-core`, because a sub-app depends on the core and the core cannot
//! depend back on it.

use crosstalk_core::subapp::Registry;
use std::sync::Arc;

/// Every sub-app compiled into this binary.
pub fn builtin() -> Registry {
    Registry::new(vec![Arc::new(crosstalk_guide::GuideApp::new())])
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
