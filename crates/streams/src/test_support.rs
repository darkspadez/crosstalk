//! Fixtures shared by the streams crate's tests.
//!
//! The selection tests all ask the same question of the same world — which streams may a match
//! draw from, and at what priority — so the world lives here once and each test says only what it
//! varies. It is small on purpose: the shapes that matter are an INACTIVE account, a stream in
//! no group, a hand-made custom stream, and two accounts carrying the same stream name.

use crosstalk_core::config::CrosstalkConfig;
use crosstalk_core::dispatcharr::{ChannelGroup, M3uAccount, Stream, World};
use crosstalk_core::jobs::JobContext;
use std::path::PathBuf;

/// An active account, first in every priority list by default.
pub const PROVIDER_A: i64 = 1;
/// A second active account.
pub const PROVIDER_B: i64 = 2;
/// An account that exists and is switched off: never eligible, whatever the list says.
pub const PROVIDER_OFF: i64 = 3;
/// The locked account hand-made streams belong to.
pub const CUSTOM_ACCOUNT: i64 = 9;

/// Stream group ids.
pub const TVE: i64 = 100;
pub const CABLE: i64 = 101;
/// A group no stream is in, so it is not a stream group.
pub const PPV: i64 = 102;

/// `ESPN HD`, on the first account, in TVE.
pub const ESPN_HD: i64 = 10;
/// `ESPN`, the same name from the second account, in Cable.
pub const ESPN: i64 = 11;
/// `ESPN [4K]`, which the regex rules clean to `ESPN`.
pub const ESPN_4K: i64 = 12;
/// `CNN`, on the switched-off account: never eligible.
pub const CNN_OFF: i64 = 13;
/// A hand-made stream in no group.
pub const CUSTOM_STREAM: i64 = 15;

/// The M3U accounts [`world`] carries.
pub fn accounts() -> Vec<M3uAccount> {
    vec![
        M3uAccount {
            id: PROVIDER_A,
            name: "Main Provider".to_string(),
            is_active: true,
            account_type: "XC".to_string(),
            max_streams: Some(2),
            ..M3uAccount::default()
        },
        M3uAccount {
            id: PROVIDER_B,
            name: "Backup Provider".to_string(),
            is_active: true,
            account_type: "STD".to_string(),
            max_streams: Some(1),
            ..M3uAccount::default()
        },
        M3uAccount {
            id: PROVIDER_OFF,
            name: "Retired Provider".to_string(),
            is_active: false,
            ..M3uAccount::default()
        },
        M3uAccount {
            id: CUSTOM_ACCOUNT,
            name: "Custom".to_string(),
            is_active: true,
            locked: true,
            ..M3uAccount::default()
        },
    ]
}

fn stream(id: i64, name: &str, account: Option<i64>, group: Option<i64>) -> Stream {
    Stream {
        id,
        name: name.to_string(),
        m3u_account: account,
        channel_group: group,
        ..Stream::default()
    }
}

/// A world with four accounts, three groups and five streams.
pub fn world() -> World {
    World {
        m3u_accounts: accounts(),
        groups: vec![
            ChannelGroup {
                id: TVE,
                name: "TVE".to_string(),
            },
            ChannelGroup {
                id: CABLE,
                name: "Cable".to_string(),
            },
            ChannelGroup {
                id: PPV,
                name: "PPV".to_string(),
            },
        ],
        streams: vec![
            stream(ESPN_HD, "ESPN HD", Some(PROVIDER_A), Some(TVE)),
            stream(ESPN, "ESPN", Some(PROVIDER_B), Some(CABLE)),
            stream(ESPN_4K, "ESPN [4K]", Some(PROVIDER_A), Some(TVE)),
            stream(CNN_OFF, "CNN", Some(PROVIDER_OFF), Some(CABLE)),
            stream(CUSTOM_STREAM, "Custom Feed", Some(CUSTOM_ACCOUNT), None),
        ],
        ..World::default()
    }
}

/// A configuration directory a run can read its `streams.toml` from.
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

    /// The `streams.toml` this fixture writes.
    pub fn settings_path(&self) -> PathBuf {
        self.dir.path().join("streams.toml")
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
