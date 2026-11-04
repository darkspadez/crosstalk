//! Fixtures shared by the streams crate's tests.
//!
//! The selection tests all ask the same question of the same world — which streams may a match
//! draw from, and at what priority — so the world lives here once and each test says only what it
//! varies. It is small on purpose: the shapes that matter are an INACTIVE account, a stream in
//! no group, a hand-made custom stream, and two accounts carrying the same stream name.
//!
//! The sort fixtures are the same world with the stream facts a quality key reads: a 4K feed, an
//! HD feed, an SD feed, a stream with no stats at all, a slate that claims 1080p while carrying
//! almost no video, and a hand-made fallback stream.

use crosstalk_core::config::CrosstalkConfig;
use crosstalk_core::dispatcharr::{
    Channel, ChannelGroup, CurrentProgram, DispatcharrClient, M3uAccount, Stream, World,
};
use crosstalk_core::jobs::{JobContext, VecSink};
use crosstalk_core::store::Store;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

/// The sort fixture's channel ids.
pub const CHANNEL_SD_FIRST: i64 = 1;
/// A channel whose list is already in the sorted order.
pub const CHANNEL_SORTED: i64 = 2;
/// A single-stream channel: never a sort candidate.
pub const CHANNEL_ONE_STREAM: i64 = 3;

/// `ESPN SD`: 720x576 at 25 fps, the worst picture of the ESPN feeds.
pub const SD_FEED: i64 = 20;
/// `ESPN HD`: 1920x1080 at 60 fps on the LOWER-priority account.
pub const HD_FEED: i64 = 21;
/// `ESPN 4K`: 3840x2160 at 50 fps on the FIRST account.
pub const UHD_FEED: i64 = 22;
/// A feed with no `stream_stats` at all.
pub const NO_STATS_FEED: i64 = 23;
/// A slate: claims 1080p, carries 190 kbps.
pub const SLATE_FEED: i64 = 24;
/// A hand-made fallback stream that must stay last.
pub const FALLBACK_FEED: i64 = 25;
/// `ESPN West`: zone routing drops it from an unmarked channel's list.
pub const WEST_FEED: i64 = 26;

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

/// A world to sort: three channels and six feeds with the facts the quality key reads.
///
/// The channels are deliberately unmarked (`ESPN`, not `ESPN East`), which is what most of a
/// real lineup looks like and is what makes zone routing drop West streams.
pub fn sort_world() -> World {
    let stats = |width: i64, height: i64, fps: f64, bitrate: Option<i64>| {
        let mut payload = serde_json::json!({
            "width": width,
            "height": height,
            "source_fps": fps,
            "video_codec": "h264",
        });
        if let Some(bitrate) = bitrate {
            payload["video_bitrate"] = serde_json::json!(bitrate);
        }
        payload
    };
    World {
        channels: vec![
            Channel {
                id: CHANNEL_SD_FIRST,
                name: "ESPN".to_string(),
                channel_number: Some(serde_json::json!(1)),
                channel_group_id: Some(TVE),
                // Worst first, and the hand-made fallback in the middle of the list.
                streams: vec![
                    SD_FEED,
                    WEST_FEED,
                    FALLBACK_FEED,
                    NO_STATS_FEED,
                    SLATE_FEED,
                    HD_FEED,
                    UHD_FEED,
                ],
                ..Channel::default()
            },
            Channel {
                id: CHANNEL_SORTED,
                name: "ESPN2".to_string(),
                channel_number: Some(serde_json::json!(2)),
                channel_group_id: Some(TVE),
                // Already in the order the model produces.
                streams: vec![UHD_FEED, HD_FEED, SD_FEED],
                ..Channel::default()
            },
            Channel {
                id: CHANNEL_ONE_STREAM,
                name: "Local".to_string(),
                channel_number: Some(serde_json::json!(3)),
                channel_group_id: Some(CABLE),
                streams: vec![SD_FEED],
                ..Channel::default()
            },
        ],
        streams: vec![
            Stream {
                id: SD_FEED,
                name: "ESPN SD".to_string(),
                m3u_account: Some(PROVIDER_A),
                stream_stats: stats(720, 576, 25.0, Some(1200)),
                ..Stream::default()
            },
            Stream {
                id: HD_FEED,
                name: "ESPN HD".to_string(),
                m3u_account: Some(PROVIDER_B),
                stream_stats: stats(1920, 1080, 60.0, Some(2656)),
                ..Stream::default()
            },
            Stream {
                id: UHD_FEED,
                name: "ESPN 4K".to_string(),
                m3u_account: Some(PROVIDER_A),
                stream_stats: stats(3840, 2160, 50.0, Some(9000)),
                ..Stream::default()
            },
            Stream {
                id: NO_STATS_FEED,
                name: "ESPN Backup".to_string(),
                m3u_account: Some(PROVIDER_B),
                stream_stats: serde_json::json!({}),
                ..Stream::default()
            },
            Stream {
                id: SLATE_FEED,
                name: "ESPN Slate".to_string(),
                m3u_account: Some(PROVIDER_A),
                stream_stats: stats(1920, 1080, 50.0, Some(190)),
                ..Stream::default()
            },
            Stream {
                id: WEST_FEED,
                name: "ESPN West".to_string(),
                m3u_account: Some(PROVIDER_A),
                stream_stats: stats(1280, 720, 60.0, Some(4000)),
                ..Stream::default()
            },
            Stream {
                id: FALLBACK_FEED,
                name: "Could Not Dispatch".to_string(),
                m3u_account: Some(CUSTOM_ACCOUNT),
                is_custom: true,
                stream_stats: serde_json::json!({}),
                ..Stream::default()
            },
        ],
        ..world()
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

/// A fixture serving `world` over HTTP, with the channel-stream write endpoint answered.
///
/// The sort reads four listings and writes one, so this is the same mock set the guide crate
/// uses, minus the EPG ones a sort never asks for.
pub struct ServerFixture {
    pub server: MockServer,
    /// Only kept alive: it owns the config and export directories the fixture points at, so
    /// dropping it at the end of a test removes them.
    pub _dir: tempfile::TempDir,
    pub config: CrosstalkConfig,
}

impl ServerFixture {
    pub async fn start(world: &World) -> Self {
        Self::start_with_programmes(world, Vec::new()).await
    }

    /// The same, with what `/epg/current-programs/` answers: the EPG placeholder path's one
    /// batched read.
    pub async fn start_with_programmes(world: &World, programmes: Vec<CurrentProgram>) -> Self {
        Self::start_inner(world, programmes, 200, 200).await
    }

    /// The same, with the EPG read failing: the run must degrade, not abort.
    pub async fn start_with_epg_error(world: &World) -> Self {
        Self::start_inner(world, Vec::new(), 500, 200).await
    }

    /// The same, with the system-timezone read failing: the date gate must fall back to
    /// the host's date, say so in the report, and still run.
    pub async fn start_with_timezone_error(world: &World) -> Self {
        Self::start_inner(world, Vec::new(), 200, 500).await
    }

    async fn start_inner(
        world: &World,
        programmes: Vec<CurrentProgram>,
        epg_status: u16,
        timezone_status: u16,
    ) -> Self {
        let server = MockServer::start().await;
        let list = |items: serde_json::Value| ResponseTemplate::new(200).set_body_json(items);

        Mock::given(method("GET"))
            .and(path("/api/channels/channels/"))
            .respond_with(list(
                serde_json::to_value(&world.channels).expect("channels"),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/channels/groups/"))
            .respond_with(list(serde_json::to_value(&world.groups).expect("groups")))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/channels/profiles/"))
            .respond_with(list(
                serde_json::to_value(&world.profiles).expect("profiles"),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/channels/streams/"))
            .respond_with(list(serde_json::to_value(&world.streams).expect("streams")))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/m3u/accounts/"))
            .respond_with(list(
                serde_json::to_value(&world.m3u_accounts).expect("accounts"),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/epg/epgdata/"))
            .respond_with(list(serde_json::to_value(&world.entries).expect("entries")))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/epg/current-programs/"))
            .respond_with(if epg_status == 200 {
                list(serde_json::to_value(&programmes).expect("programmes"))
            } else {
                ResponseTemplate::new(epg_status).set_body_string("no guide for you")
            })
            .mount(&server)
            .await;
        // Dispatcharr's configured timezone, read once per EPG run for the "Next Event"
        // date gate (see `DispatcharrClient::system_timezone`).
        Mock::given(method("GET"))
            .and(path("/api/core/settings/"))
            .respond_with(if timezone_status == 200 {
                list(serde_json::json!([
                    {"id": 1, "key": "stream_settings", "name": "Stream", "value": {}},
                    {"id": 2, "key": "system_settings", "name": "System",
                     "value": {"time_zone": "UTC"}},
                ]))
            } else {
                ResponseTemplate::new(timezone_status).set_body_string("no settings for you")
            })
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"message": "ok", "channels": []})),
            )
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        config.jobs.exports_dir = dir.path().join("exports").display().to_string();
        config.jobs.data_dir = dir.path().join("data").display().to_string();

        Self {
            server,
            _dir: dir,
            config,
        }
    }

    pub fn client(&self) -> DispatcharrClient {
        DispatcharrClient::new(&self.config.dispatcharr).expect("client")
    }

    /// A fresh in-memory run store.
    pub fn store(&self) -> Store {
        Store::in_memory().expect("store")
    }

    /// A context tied to a recorded run in `store`, so rows land there.
    pub fn context_for_run(&self, action: &str, store: &Store, dry_run: bool) -> JobContext {
        let run_id = store
            .start_run(crate::APP_ID, action, dry_run)
            .expect("start run");
        JobContext::new(Arc::new(self.config.clone()), Arc::new(VecSink::new()))
            .with_dispatcharr(self.client())
            .for_run(run_id, store.clone(), Arc::new(AtomicBool::new(false)))
    }

    /// How many requests were received for one path and method.
    ///
    /// Separate from [`Self::bodies`]: a GET carries no JSON body to parse, so counting
    /// is the only read that works for them (the timezone and entry listings).
    pub async fn request_count(&self, method_name: &str, path: &str) -> usize {
        self.server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.method.as_str() == method_name)
            .filter(|request| request.url.path() == path)
            .count()
    }

    /// The JSON bodies of every request received for one path and method.
    pub async fn bodies(&self, method_name: &str, path: &str) -> Vec<serde_json::Value> {
        self.server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.method.as_str() == method_name)
            .filter(|request| request.url.path() == path)
            .map(|request| serde_json::from_slice(&request.body).expect("JSON body"))
            .collect()
    }
}
