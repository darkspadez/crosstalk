//! Fixtures shared by the guide crate's action tests.
//!
//! The scan and the five cleanup actions all need the same world over HTTP: five channels, two
//! with a guide that carries no programme data, plus the two write endpoints. Building that in
//! every test would be a lot of mocks to keep in step, so it lives here once and each test says
//! only what it varies.

use crosstalk_core::config::CrosstalkConfig;
use crosstalk_core::dispatcharr::{
    Channel, ChannelGroup, ChannelProfile, DispatcharrClient, EpgEntry, EpgSource, World,
};
use crosstalk_core::jobs::{JobContext, VecSink};
use crosstalk_core::scope::{scope_channels, ScopedChannels};
use crosstalk_core::store::Store;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The one EPG source every guide entry belongs to.
pub const SOURCE: i64 = 4;
/// `ESPN HD`, the guide that DOES carry programmes.
pub const ESPN_HD: i64 = 31;
/// `CNN [US]`, an empty guide, on channel 2 (`CNN [BadEPG]`).
pub const CNN_US: i64 = 32;
/// `Náutica Ñ`, an empty guide, on channel 4 (`Télé-Québec`).
pub const NACA: i64 = 33;

/// Group ids.
pub const SPORTS: i64 = 10;
pub const NEWS: i64 = 20;
pub const PPV: i64 = 30;

/// Profile ids.
pub const ALL: i64 = 1;
pub const FAVORITES: i64 = 2;

/// A world with:
///
/// * `ESPN` (1, Sports, guide 31, has programmes),
/// * `CNN [BadEPG]` (2, News, guide 32, empty) — already carries the bad-EPG suffix,
/// * `PPV 1` (3, PPV, no guide),
/// * `Télé-Québec` (4, News, guide 33, empty),
/// * `Local` (5, no group, no guide),
/// * profiles `All` (everything) and `Favorites` (1 and 4 enabled).
pub fn world() -> World {
    let channel = |id: i64, name: &str, group: Option<i64>, epg: Option<i64>| Channel {
        id,
        name: name.to_string(),
        channel_number: Some(serde_json::json!(id)),
        channel_group_id: group,
        epg_data_id: epg,
        ..Channel::default()
    };
    World {
        channels: vec![
            channel(1, "ESPN", Some(SPORTS), Some(ESPN_HD)),
            channel(2, "CNN [BadEPG]", Some(NEWS), Some(CNN_US)),
            channel(3, "PPV 1", Some(PPV), None),
            channel(4, "Télé-Québec", Some(NEWS), Some(NACA)),
            channel(5, "Local", None, None),
        ],
        groups: vec![
            ChannelGroup {
                id: SPORTS,
                name: "Sports".to_string(),
            },
            ChannelGroup {
                id: NEWS,
                name: "News".to_string(),
            },
            ChannelGroup {
                id: PPV,
                name: "PPV Live".to_string(),
            },
        ],
        profiles: vec![
            ChannelProfile {
                id: ALL,
                name: "All".to_string(),
                channels: vec![1, 2, 3, 4, 5],
            },
            ChannelProfile {
                id: FAVORITES,
                name: "Favorites".to_string(),
                channels: vec![1, 4],
            },
        ],
        sources: vec![EpgSource {
            id: SOURCE,
            name: "Schedules Direct".to_string(),
            is_active: true,
            priority: 5,
            ..EpgSource::default()
        }],
        entries: vec![
            EpgEntry {
                id: ESPN_HD,
                name: "ESPN HD".to_string(),
                tvg_id: "espn.us".to_string(),
                epg_source: Some(SOURCE),
                icon_url: None,
            },
            EpgEntry {
                id: CNN_US,
                name: "CNN [US]".to_string(),
                tvg_id: "cnn.us".to_string(),
                epg_source: Some(SOURCE),
                icon_url: None,
            },
            EpgEntry {
                id: NACA,
                name: "Náutica Ñ".to_string(),
                tvg_id: "naca.mx".to_string(),
                epg_source: Some(SOURCE),
                icon_url: None,
            },
        ],
        ..World::default()
    }
}

/// The channels [`world`] gives a guide that carries no programmes: the scan's answer, and
/// what the cleanup actions select.
pub fn missing_ids() -> Vec<i64> {
    vec![2, 4]
}

/// A mock Dispatcharr serving `world`, plus the config and directories a run needs.
pub struct Fixture {
    pub server: MockServer,
    pub dir: tempfile::TempDir,
    pub config: CrosstalkConfig,
}

impl Fixture {
    /// Serve `world` and answer the programme check for every entry: only the entry ids listed
    /// in `with_programmes` have data in the window.
    pub async fn start(world: &World, with_programmes: &[i64]) -> Self {
        Self::start_impl(world, with_programmes, &[]).await
    }

    /// Like [`Fixture::start`], but the programme check FAILS (HTTP 500) for the entries
    /// whose `tvg_id` is listed: the scan must skip those channels, not report them missing.
    pub async fn start_with_program_failures(
        world: &World,
        with_programmes: &[i64],
        failing_tvg_ids: &[&str],
    ) -> Self {
        Self::start_impl(world, with_programmes, failing_tvg_ids).await
    }

    async fn start_impl(world: &World, with_programmes: &[i64], failing_tvg_ids: &[&str]) -> Self {
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
            .and(path("/api/epg/sources/"))
            .respond_with(list(serde_json::to_value(&world.sources).expect("sources")))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/epg/epgdata/"))
            .respond_with(list(serde_json::to_value(&world.entries).expect("entries")))
            .mount(&server)
            .await;

        // A failing lookup answers 500, mounted before the catch-all so it fires first.
        for tvg_id in failing_tvg_ids {
            Mock::given(method("GET"))
                .and(path("/api/epg/programs/search/"))
                .and(query_param("tvg_id", *tvg_id))
                .respond_with(ResponseTemplate::new(500))
                .mount(&server)
                .await;
        }
        // One programme answer per entry that carries data; the catch-all below answers "none"
        // for everything else.
        for entry in &world.entries {
            if !with_programmes.contains(&entry.id) {
                continue;
            }
            Mock::given(method("GET"))
                .and(path("/api/epg/programs/search/"))
                .and(query_param("tvg_id", entry.tvg_id.as_str()))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"count": 1, "results": [{"id": 42}]})),
                )
                .mount(&server)
                .await;
        }
        // The per-source horizon probe of the programme pre-seed (`fields=end_time`, no
        // tvg_id): the one source carries something in the window when any entry does.
        if !with_programmes.is_empty() {
            Mock::given(method("GET"))
                .and(path("/api/epg/programs/search/"))
                .and(query_param("fields", "end_time"))
                .respond_with(ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"count": 1, "results": [{"end_time": "2099-01-01T00:00:00Z"}]}),
                ))
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"count": 0, "results": []})),
            )
            .mount(&server)
            .await;

        // The write endpoints, answered generically so a test asserts on the request.
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"message": "ok", "channels": []})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/channels/channels/batch-set-epg/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"success": true, "channels_updated": 1})),
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
            dir,
            config,
        }
    }

    /// The `guide.toml` this fixture writes, when a test needs a setting the daemon would
    /// otherwise default.
    pub fn settings_path(&self) -> std::path::PathBuf {
        self.dir.path().join("guide.toml")
    }

    pub fn client(&self) -> DispatcharrClient {
        DispatcharrClient::new(&self.config.dispatcharr).expect("client")
    }

    /// A context with a session but no run: enough for a read, not for reading history.
    pub fn context(&self) -> JobContext {
        JobContext::new(Arc::new(self.config.clone()), Arc::new(VecSink::new()))
            .with_dispatcharr(self.client())
    }

    /// A context tied to a recorded run, so rows land in `store`.
    pub fn context_for_run(&self, app: &str, action: &str, store: &Store) -> JobContext {
        let run_id = store.start_run(app, action, true).expect("start run");
        JobContext::new(Arc::new(self.config.clone()), Arc::new(VecSink::new()))
            .with_dispatcharr(self.client())
            .for_run(
                app,
                action,
                run_id,
                store.clone(),
                Arc::new(AtomicBool::new(false)),
            )
    }

    /// The in-scope channels of `world`, as the actions resolve them.
    pub fn scoped(&self, world: &World) -> ScopedChannels {
        scope_channels(world, &self.config.scope).expect("scope")
    }

    /// The JSON bodies of every request received for one path and method.
    pub async fn bodies(&self, method_name: &str, path: &str) -> Vec<serde_json::Value> {
        self.requests(method_name, path)
            .await
            .into_iter()
            .map(|request| serde_json::from_slice(&request.body).expect("JSON body"))
            .collect()
    }

    /// The query strings of every request received for one path and method.
    pub async fn queries(&self, method_name: &str, path: &str) -> Vec<String> {
        self.requests(method_name, path)
            .await
            .into_iter()
            .map(|request| request.url.query().unwrap_or_default().to_string())
            .collect()
    }

    async fn requests(&self, method_name: &str, path: &str) -> Vec<wiremock::Request> {
        self.server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.method.as_str() == method_name)
            .filter(|request| request.url.path() == path)
            .collect()
    }

    /// Record a finished scan run whose rows are `rows`, so a cleanup action has a result set.
    pub fn record_scan(&self, store: &Store, rows: &[crosstalk_core::store::RunRow]) -> i64 {
        let run_id = store.start_run("guide", "scan", true).expect("start");
        store.append_rows(run_id, rows).expect("rows");
        store
            .finish_run(
                run_id,
                crosstalk_core::store::RunStatus::Succeeded,
                Some("recorded by the test"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        run_id
    }
}
