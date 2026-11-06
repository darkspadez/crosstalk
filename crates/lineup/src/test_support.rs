//! Fixtures shared by the lineup crate's tests: a config directory and a run context.
//!
//! The skeleton has no Dispatcharr surface yet (only `validate` and `status`, both
//! offline), so the fixture is small on purpose: a temp config dir the settings load
//! from, and a context `validate` can refuse (no credentials) or pass (with them).

use crosstalk_core::config::CrosstalkConfig;
use crosstalk_core::jobs::JobContext;
use std::path::PathBuf;

/// A configuration directory a run can read its `lineup.toml` from.
pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub config: CrosstalkConfig,
}

impl Fixture {
    pub fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = CrosstalkConfig::load(dir.path()).expect("config");
        Self { dir, config }
    }

    /// With Dispatcharr credentials, so `validate` has nothing to complain about but settings.
    pub fn with_credentials() -> Self {
        let mut fixture = Self::new();
        fixture.config.dispatcharr.api_key = "test-key".to_string();
        fixture
    }

    /// The `lineup.toml` this fixture writes.
    pub fn settings_path(&self) -> PathBuf {
        self.dir.path().join("lineup.toml")
    }

    pub fn context(&self) -> JobContext {
        JobContext::for_tests(self.config.clone())
    }
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}
