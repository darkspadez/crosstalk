//! The `full_sync` workflow end to end (step 19).
//!
//! A wiremock Dispatcharr serves one stateful world, and the real registry (guide +
//! streams + lineup) runs the `full_sync` workflow exactly as
//! `config/crosstalk.example.toml` ships it, through the real runner:
//!
//! ```text
//! lineup.sync_apply
//!   -> streams.match_apply (scope "lineup", lineup mode)
//!   -> guide.match_apply (scope "lineup")
//!   -> lineup.logos_apply (scope "lineup")
//!   -> lineup.tidy_apply
//! ```
//!
//! The seed world holds a foreign channel (`ESPN`, never owned, empty — an unscoped
//! match or tidy would touch it), and four crafted streams: a plain `BBC One HD` (unknown country, kept), a second
//! `BBC One HD` from another account in the `US` stream group (proven foreign, dropped
//! by lineup mode), a `US: BBC Two` (foreign, never attached) and an `ESPN` feed for
//! the foreign channel. `without_lineup_mode_the_foreign_streams_attach` is the
//! control: with `lineup_mode_country_restriction = false` both US streams attach, so
//! the country assertions below measure lineup mode and nothing else.
//! The assertions pin the whole chain:
//!
//! * sync creates the lineup's owned channels (names + numbers from the file);
//! * streams.match attaches only to owned channels (the foreign channel is untouched)
//!   and drops the proven-foreign stream (lineup mode restricts to the lineup's UK);
//! * guide.match assigns only owned channels;
//! * logos resolves the assigned entry's icon onto the owned channel;
//! * tidy hides the owned channel left with no streams;
//! * a rerun changes no Dispatcharr state and writes nothing at all (guide skips an
//!   assignment the channel already has, like every channel-ops write);
//! * undo of each step reverses it (created channels end hidden).
//!
//! Cross-app consistency rides the shared identity cache: after the workflow the
//! store holds one-version identity rows for the synced channel name (`BBC One`), its
//! stream's name (`BBC One HD`) and its guide entry's name (`BBC1`), all resolving to
//! the same family key.

use crosstalk_core::config::{CrosstalkConfig, MatchingConfig, SharedConfig, WorkflowStepScope};
use crosstalk_core::dispatcharr::{
    Channel, ChannelGroup, ChannelProfile, EpgEntry, EpgSource, Logo, M3uAccount, Stream,
};
use crosstalk_core::identity_cache::CachedIdentities;
use crosstalk_core::runner::{JobRunner, RunOptions};
use crosstalk_core::store::{RunStatus, Store};
use crosstalk_core::subapp::Registry;
use crosstalk_guide::settings::GuideSettings;
use crosstalk_guide::GuideApp;
use crosstalk_lineup::settings::LineupSettings;
use crosstalk_lineup::{lineup_file, LineupApp};
use crosstalk_matching::engine::UnifiedEngine;
use crosstalk_matching::identity::{IdentityCache, ResolveContext};
use crosstalk_streams::settings::StreamsSettings;
use crosstalk_streams::StreamsApp;
use std::sync::{Arc, Mutex};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const LINEUP: &str = "UK_Freeview";
/// The first two entries of `UK_Freeview` (Entertainment): the channels the test
/// asserts on by name. Read from the file at runtime, so a lineup edit that renames
/// them fails loudly here instead of asserting on strangers.
const FIRST_NAME: &str = "BBC One";
const SECOND_NAME: &str = "BBC Two";
/// The guide entry's own spelling, distinct from the channel's, so the cross-app check
/// below compares three different inputs rather than one name with itself.
const ENTRY_NAME: &str = "BBC1";

const CABLE: i64 = 100;
const US_GROUP: i64 = 101;

const FOREIGN_CHANNEL: i64 = 9001;
const STREAM_UK: i64 = 11;
const STREAM_US_SAME_NAME: i64 = 12;
const STREAM_US_PREFIXED: i64 = 13;
const STREAM_FOREIGN: i64 = 14;
const M3U: i64 = 1;
const M3U_US: i64 = 2;
const SOURCE: i64 = 4;
const ENTRY_BBC: i64 = 7;
const ENTRY_ESPN: i64 = 8;

/// Write counters, so the rerun can prove it wrote nothing new.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct WriteCounts {
    post_channels: usize,
    post_groups: usize,
    post_logos: usize,
    patch_bulk: usize,
    post_batch_epg: usize,
}

struct MockState {
    channels: Vec<Channel>,
    groups: Vec<ChannelGroup>,
    profiles: Vec<ChannelProfile>,
    sources: Vec<EpgSource>,
    entries: Vec<EpgEntry>,
    streams: Vec<Stream>,
    accounts: Vec<M3uAccount>,
    logos: Vec<Logo>,
    next_id: i64,
    writes: WriteCounts,
}

impl MockState {
    fn channel(&self, id: i64) -> Option<Channel> {
        self.channels.iter().find(|c| c.id == id).cloned()
    }

    /// Every stream id held by one of `owned_ids`' channels.
    fn owned_stream_ids(&self, owned_ids: &[i64]) -> Vec<i64> {
        self.channels
            .iter()
            .filter(|c| owned_ids.contains(&c.id))
            .flat_map(|c| c.streams.clone())
            .collect()
    }
}

fn stream(id: i64, name: &str, group: Option<i64>) -> Stream {
    Stream {
        id,
        name: name.to_string(),
        m3u_account: Some(M3U),
        channel_group: group,
        ..Stream::default()
    }
}

fn seed_state() -> MockState {
    let lineup = lineup_file::load_lineup(LINEUP, "normal", "").expect("the test lineup loads");
    assert_eq!(lineup.entries[0].name, FIRST_NAME, "{}", lineup.file_name);
    assert_eq!(lineup.entries[1].name, SECOND_NAME, "{}", lineup.file_name);
    MockState {
        channels: vec![Channel {
            id: FOREIGN_CHANNEL,
            name: "ESPN".to_string(),
            channel_number: Some(serde_json::json!(9999)),
            // Empty, with a same-named `ESPN` stream and guide entry in the world: an
            // unscoped match would fill both and an unscoped tidy would hide it, so
            // every assertion that it stays this way measures the lineup scope.
            streams: Vec::new(),
            ..Channel::default()
        }],
        groups: vec![
            ChannelGroup {
                id: CABLE,
                name: "Cable".to_string(),
            },
            ChannelGroup {
                id: US_GROUP,
                name: "US".to_string(),
            },
        ],
        profiles: Vec::new(),
        sources: vec![EpgSource {
            id: SOURCE,
            name: "Schedules Direct".to_string(),
            is_active: true,
            priority: 5,
            ..EpgSource::default()
        }],
        entries: vec![
            EpgEntry {
                id: ENTRY_BBC,
                name: ENTRY_NAME.to_string(),
                tvg_id: "bbc.one".to_string(),
                epg_source: Some(SOURCE),
                icon_url: Some("http://icons.example/bbcone.png".to_string()),
            },
            EpgEntry {
                id: ENTRY_ESPN,
                name: "ESPN".to_string(),
                tvg_id: "espn.us".to_string(),
                epg_source: Some(SOURCE),
                icon_url: None,
            },
        ],
        streams: vec![
            stream(STREAM_UK, "BBC One HD", Some(CABLE)),
            // A second account, so the same-name rule (one account's duplicates fold)
            // cannot be what drops it: only the country can.
            Stream {
                m3u_account: Some(M3U_US),
                ..stream(STREAM_US_SAME_NAME, "BBC One HD", Some(US_GROUP))
            },
            stream(
                STREAM_US_PREFIXED,
                &format!("US: {SECOND_NAME}"),
                Some(US_GROUP),
            ),
            stream(STREAM_FOREIGN, "ESPN", Some(CABLE)),
        ],
        accounts: vec![
            M3uAccount {
                id: M3U,
                name: "Main".to_string(),
                is_active: true,
                account_type: "XC".to_string(),
                max_streams: Some(2),
                ..M3uAccount::default()
            },
            M3uAccount {
                id: M3U_US,
                name: "US Provider".to_string(),
                is_active: true,
                account_type: "XC".to_string(),
                max_streams: Some(2),
                ..M3uAccount::default()
            },
        ],
        logos: Vec::new(),
        next_id: 10000,
        writes: WriteCounts::default(),
    }
}

/// Apply one bulk-edit row to the mocked channel, for the fields the five steps and
/// their undos write. An unknown id edits nothing (the assertions, not the mock, say
/// what must exist).
fn apply_edit(state: &mut MockState, edit: &serde_json::Value) -> Option<Channel> {
    let id = edit.get("id")?.as_i64()?;
    let channel = state.channels.iter_mut().find(|c| c.id == id)?;
    if let Some(name) = edit.get("name").and_then(|v| v.as_str()) {
        channel.name = name.to_string();
    }
    if let Some(number) = edit.get("channel_number") {
        channel.channel_number = if number.is_null() {
            None
        } else {
            Some(number.clone())
        };
    }
    if let Some(group) = edit.get("channel_group_id") {
        channel.channel_group_id = if group.is_null() {
            None
        } else {
            group.as_i64()
        };
    }
    if let Some(list) = edit.get("streams").and_then(|v| v.as_array()) {
        channel.streams = list.iter().filter_map(|v| v.as_i64()).collect();
    }
    if let Some(epg) = edit.get("epg_data_id") {
        channel.epg_data_id = if epg.is_null() { None } else { epg.as_i64() };
        channel.effective_epg_data_id = channel.epg_data_id;
    }
    if let Some(logo) = edit.get("logo_id") {
        channel.logo_id = if logo.is_null() { None } else { logo.as_i64() };
    }
    if let Some(hidden) = edit.get("hidden_from_output").and_then(|v| v.as_bool()) {
        channel.hidden_from_output = hidden;
    }
    Some(channel.clone())
}

async fn serve(state: Arc<Mutex<MockState>>) -> MockServer {
    let server = MockServer::start().await;

    // Listings read the live state, so the rerun plans against the first run's writes.
    for route in [
        "/api/channels/channels/",
        "/api/channels/groups/",
        "/api/channels/profiles/",
        "/api/channels/streams/",
        "/api/m3u/accounts/",
        "/api/epg/sources/",
        "/api/epg/epgdata/",
        "/api/channels/logos/",
    ] {
        let state = Arc::clone(&state);
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(move |_: &wiremock::Request| {
                let state = state.lock().expect("mock state");
                let items = match route {
                    "/api/channels/channels/" => serde_json::to_value(&state.channels),
                    "/api/channels/groups/" => serde_json::to_value(&state.groups),
                    "/api/channels/profiles/" => serde_json::to_value(&state.profiles),
                    "/api/channels/streams/" => serde_json::to_value(&state.streams),
                    "/api/m3u/accounts/" => serde_json::to_value(&state.accounts),
                    "/api/epg/sources/" => serde_json::to_value(&state.sources),
                    "/api/epg/epgdata/" => serde_json::to_value(&state.entries),
                    "/api/channels/logos/" => serde_json::to_value(&state.logos),
                    _ => unreachable!("mocked route"),
                }
                .expect("serialise the mocked listing");
                ResponseTemplate::new(200).set_body_json(items)
            })
            .mount(&server)
            .await;
    }

    // Programme validation: only the BBC entry carries programmes in the window, so a
    // guide family without programme data stays unassigned (data validation, not
    // selection).
    Mock::given(method("GET"))
        .and(path("/api/epg/programs/search/"))
        .respond_with(|request: &wiremock::Request| {
            let tvg = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "tvg_id")
                .map(|(_, value)| value.to_string());
            let (count, results) = if tvg.as_deref() == Some("bbc.one") {
                (1, serde_json::json!([{"id": 42}]))
            } else {
                (0, serde_json::json!([]))
            };
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"count": count, "results": results}))
        })
        .mount(&server)
        .await;

    // Dispatcharr's configured timezone (some runs read it once; static here).
    Mock::given(method("GET"))
        .and(path("/api/core/settings/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "key": "stream_settings", "name": "Stream", "value": {}},
            {"id": 2, "key": "system_settings", "name": "System",
             "value": {"time_zone": "UTC"}},
        ])))
        .mount(&server)
        .await;

    // EPG placeholder resolution is off in this test; answer empty either way.
    Mock::given(method("POST"))
        .and(path("/api/epg/current-programs/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;

    // Group get-or-create: an existing name answers 400 (like DRF's UniqueValidator)
    // and the client recovers it through the listing above.
    let groups = Arc::clone(&state);
    Mock::given(method("POST"))
        .and(path("/api/channels/groups/"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let name = body
                .get("name")
                .and_then(|name| name.as_str())
                .unwrap_or_default()
                .to_string();
            let mut state = groups.lock().expect("mock state");
            if state.groups.iter().any(|g| g.name == name) {
                return ResponseTemplate::new(400).set_body_json(serde_json::json!({
                    "name": ["channel group with this name already exists"],
                }));
            }
            let id = state.next_id;
            state.next_id += 1;
            state.groups.push(ChannelGroup { id, name });
            state.writes.post_groups += 1;
            let created = state.groups.last().expect("just pushed").clone();
            ResponseTemplate::new(201).set_body_json(created)
        })
        .mount(&server)
        .await;

    // Channel creates land in the state with fresh ids, like upstream.
    let channels = Arc::clone(&state);
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let mut state = channels.lock().expect("mock state");
            let id = state.next_id;
            state.next_id += 1;
            let mut channel = Channel {
                id,
                name: body
                    .get("name")
                    .and_then(|name| name.as_str())
                    .unwrap_or_default()
                    .to_string(),
                channel_number: body.get("channel_number").cloned(),
                channel_group_id: body.get("channel_group_id").and_then(|v| v.as_i64()),
                streams: body
                    .get("streams")
                    .and_then(|value| value.as_array())
                    .map(|list| list.iter().filter_map(|v| v.as_i64()).collect())
                    .unwrap_or_default(),
                epg_data_id: body.get("epg_data_id").and_then(|v| v.as_i64()),
                logo_id: body.get("logo_id").and_then(|v| v.as_i64()),
                ..Channel::default()
            };
            channel.effective_epg_data_id = channel.epg_data_id;
            state.channels.push(channel.clone());
            state.writes.post_channels += 1;
            ResponseTemplate::new(201).set_body_json(channel)
        })
        .mount(&server)
        .await;

    // Logo creates: the URL is unique upstream; a re-sent create answers 400 and the
    // client recovers it through the listing above.
    let logos = Arc::clone(&state);
    Mock::given(method("POST"))
        .and(path("/api/channels/logos/"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let name = body
                .get("name")
                .and_then(|name| name.as_str())
                .unwrap_or_default()
                .to_string();
            let url = body
                .get("url")
                .and_then(|url| url.as_str())
                .unwrap_or_default()
                .to_string();
            let mut state = logos.lock().expect("mock state");
            if state.logos.iter().any(|logo| logo.url == url) {
                return ResponseTemplate::new(400).set_body_json(serde_json::json!({
                    "url": ["A logo with this URL already exists"],
                }));
            }
            let id = state.next_id;
            state.next_id += 1;
            state.logos.push(Logo {
                id,
                name,
                url,
                ..Logo::default()
            });
            state.writes.post_logos += 1;
            let created = state.logos.last().expect("just pushed").clone();
            ResponseTemplate::new(201).set_body_json(created)
        })
        .mount(&server)
        .await;

    // Bulk channel edits (streams, epg, logo, hidden flag, numbers): applied to the
    // state, echoing the updated channels like upstream.
    let bulk = Arc::clone(&state);
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/edit/bulk/"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let mut state = bulk.lock().expect("mock state");
            let mut updated = Vec::new();
            if let Some(edits) = body.as_array() {
                for edit in edits {
                    if let Some(channel) = apply_edit(&mut state, edit) {
                        updated.push(channel);
                    }
                }
            }
            state.writes.patch_bulk += 1;
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"message": "ok", "channels": updated}))
        })
        .mount(&server)
        .await;

    // Guide assignments: applied to the state, echoed like upstream.
    let epg = Arc::clone(&state);
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/batch-set-epg/"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap_or_default();
            let mut state = epg.lock().expect("mock state");
            let mut count = 0i64;
            if let Some(links) = body.get("associations").and_then(|v| v.as_array()) {
                for link in links {
                    let Some(id) = link.get("channel_id").and_then(|v| v.as_i64()) else {
                        continue;
                    };
                    let epg_id = link.get("epg_data_id").and_then(|v| v.as_i64());
                    if let Some(channel) = state.channels.iter_mut().find(|c| c.id == id) {
                        channel.epg_data_id = epg_id;
                        channel.effective_epg_data_id = epg_id;
                        count += 1;
                    }
                }
            }
            state.writes.post_batch_epg += 1;
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true,
                "channels_updated": count,
                "programs_refreshed": 1,
            }))
        })
        .mount(&server)
        .await;

    server
}

/// The `full_sync` workflow exactly as `config/crosstalk.example.toml` ships it
/// (commented out there): the `# [workflows.full_sync]` block up to its first blank
/// comment line, uncommented. The test then runs what an operator would copy.
fn example_full_sync_block() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../config/crosstalk.example.toml");
    let text = std::fs::read_to_string(&path).expect("the host example config");
    let lines: Vec<&str> = text
        .lines()
        .skip_while(|line| line.trim() != "# [workflows.full_sync]")
        .take_while(|line| line.trim() != "#")
        .collect();
    assert!(!lines.is_empty(), "the example ships a full_sync workflow");
    lines
        .iter()
        .map(|line| line.strip_prefix("# ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
}

fn full_sync_config(dir: &std::path::Path) -> CrosstalkConfig {
    std::fs::write(dir.join("crosstalk.toml"), example_full_sync_block())
        .expect("seed crosstalk.toml");
    let mut config = CrosstalkConfig::load(dir).expect("the example workflow loads");
    config.jobs.exports_dir = dir.join("exports").display().to_string();
    config.jobs.data_dir = dir.join("data").display().to_string();
    // The shipped shape: sync stops the sequence, the three middle steps carry on, and
    // every step between sync and tidy reads the lineup's owned channels.
    let scope = || Some(WorkflowStepScope::Named("lineup".to_string()));
    let shipped: Vec<(String, Option<WorkflowStepScope>, bool)> = config.workflows["full_sync"]
        .steps
        .iter()
        .map(|step| {
            (
                step.action().to_string(),
                step.scope().cloned(),
                step.continue_on_failure(),
            )
        })
        .collect();
    assert_eq!(
        shipped,
        vec![
            ("lineup.sync_apply".to_string(), None, false),
            ("streams.match_apply".to_string(), scope(), true),
            ("guide.match_apply".to_string(), scope(), true),
            ("lineup.logos_apply".to_string(), scope(), true),
            ("lineup.tidy_apply".to_string(), None, false),
        ]
    );
    crosstalk_core::workflow::Workflow::from_config(&config, "full_sync")
        .expect("the example workflow resolves");
    config
}

/// The run id of the workflow's step `app.action` (runs store the two apart).
fn step_run_id(store: &Store, workflow_id: i64, action: &str) -> i64 {
    let (app, verb) = action.split_once('.').expect("an app.action target");
    store
        .child_runs(workflow_id)
        .expect("steps")
        .iter()
        .find(|run| run.app == app && run.action == verb)
        .unwrap_or_else(|| panic!("a {action} step ran"))
        .id
}

/// The identity the shared cache holds for `name`: every app resolves through it, so
/// the sync output, the streams match and the guide match must agree exactly.
fn cached_family_key(store: &Store, engine: &std::sync::Arc<UnifiedEngine>, name: &str) -> String {
    let cached = CachedIdentities::new(std::sync::Arc::clone(engine), Arc::new(store.clone()));
    let version = cached.data_version().to_string();
    let key = IdentityCache::input_key(name, &ResolveContext::default());
    let json = store
        .identity_cache_get(&key, &version)
        .expect("the cache reads")
        .unwrap_or_else(|| panic!("the shared cache holds an identity for {name:?}"));
    IdentityCache::identity_from_json(&json)
        .unwrap_or_else(|| panic!("the cached identity for {name:?} parses"))
        .family_key()
        .key
}

/// One wired harness: the stateful mock, the store and a runner over the real registry
/// with the shipped `full_sync` workflow. `streams` is the streams app's settings file.
struct Harness {
    _dir: tempfile::TempDir,
    _server: MockServer,
    state: Arc<Mutex<MockState>>,
    store: Store,
    runner: JobRunner,
}

async fn harness(streams: StreamsSettings) -> Harness {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = Arc::new(Mutex::new(seed_state()));
    let server = serve(Arc::clone(&state)).await;

    let mut config = full_sync_config(dir.path());
    config.dispatcharr.base_url = server.uri();
    config.dispatcharr.api_key = "test-key".to_string();

    // Lineup points at the test file with EPG-only logos (no tv-logos network reads);
    // guide runs on its defaults (strict cut).
    let lineup_settings = LineupSettings {
        lineup_file: LINEUP.to_string(),
        logo_sources: "epg".to_string(),
        ..LineupSettings::default()
    };
    std::fs::write(
        dir.path().join("lineup.toml"),
        toml::to_string_pretty(&lineup_settings).expect("lineup settings"),
    )
    .expect("seed lineup.toml");
    std::fs::write(
        dir.path().join("streams.toml"),
        toml::to_string_pretty(&streams).expect("streams settings"),
    )
    .expect("seed streams.toml");
    std::fs::write(
        dir.path().join("guide.toml"),
        toml::to_string_pretty(&GuideSettings::default()).expect("guide settings"),
    )
    .expect("seed guide.toml");

    let store = Store::in_memory().expect("store");
    let registry = Registry::new(vec![
        Arc::new(GuideApp::new()),
        Arc::new(StreamsApp::new()),
        Arc::new(LineupApp::new()),
    ]);
    let runner = JobRunner::new(registry, SharedConfig::new(config), store.clone());
    Harness {
        _dir: dir,
        _server: server,
        state,
        store,
        runner,
    }
}

impl Harness {
    async fn run_full_sync(&self) {
        let outcome = crosstalk_core::workflow::run_to_completion(
            &self.runner,
            "full_sync",
            false,
            RunOptions::default(),
        )
        .await
        .expect("the workflow runs");
        assert!(!outcome.is_failed(), "{}", outcome.summary);
    }

    fn channel_named(&self, name: &str) -> Channel {
        self.state
            .lock()
            .expect("mock state")
            .channels
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("a channel named {name:?}"))
            .clone()
    }
}

#[tokio::test]
async fn without_lineup_mode_the_foreign_streams_attach() {
    // The control for the country assertions below: the same world and workflow with
    // `lineup_mode_country_restriction = false` attaches the US-prefixed stream to
    // BBC Two, so it is lineup mode — not the matcher alone — that keeps it off.
    let harness = harness(StreamsSettings {
        lineup_mode_country_restriction: false,
        ..StreamsSettings::default()
    })
    .await;
    harness.run_full_sync().await;
    let bbc_two = harness.channel_named(SECOND_NAME);
    assert_eq!(bbc_two.streams, vec![STREAM_US_PREFIXED], "{bbc_two:?}");
    let bbc = harness.channel_named(FIRST_NAME);
    assert!(bbc.streams.contains(&STREAM_US_SAME_NAME), "{bbc:?}");
}

#[tokio::test]
async fn full_sync_workflow_end_to_end() {
    // Streams on its defaults: strict cut, lineup mode on.
    let Harness {
        _dir,
        _server,
        state,
        store,
        runner,
    } = harness(StreamsSettings::default()).await;
    let outcome = crosstalk_core::workflow::run_to_completion(
        &runner,
        "full_sync",
        false,
        RunOptions::default(),
    )
    .await
    .expect("the workflow runs");
    assert!(!outcome.is_failed(), "{}", outcome.summary);

    let workflow_id = store
        .recent_runs_for("workflow", 1)
        .expect("runs")
        .into_iter()
        .next()
        .expect("a workflow run")
        .id;
    let steps = store.child_runs(workflow_id).expect("steps");
    assert_eq!(steps.len(), 5, "{steps:?}");
    for step in &steps {
        assert_eq!(step.status, RunStatus::Succeeded, "{step:?}");
    }

    // The lineup's channels now exist, named and numbered from the file.
    let snapshot = state.lock().expect("mock state").channels.clone();
    let bbc = snapshot
        .iter()
        .find(|c| c.name == FIRST_NAME)
        .expect("sync created the first channel")
        .clone();
    let bbc_two = snapshot
        .iter()
        .find(|c| c.name == SECOND_NAME)
        .expect("sync created the second channel")
        .clone();
    assert_eq!(bbc.channel_number, Some(serde_json::json!(1.0)));
    assert_eq!(bbc_two.channel_number, Some(serde_json::json!(2.0)));

    // Streams matched only owned channels, and only the unknown-country feed:
    // the US-marked same-name stream is proven foreign against the UK lineup hint,
    // and the prefixed US stream never attaches anywhere.
    assert_eq!(bbc.streams, vec![STREAM_UK], "{bbc:?}");
    assert!(bbc_two.streams.is_empty(), "{bbc_two:?}");
    let foreign = snapshot
        .iter()
        .find(|c| c.id == FOREIGN_CHANNEL)
        .expect("the foreign channel still exists")
        .clone();
    assert!(foreign.streams.is_empty(), "{foreign:?}");
    assert_eq!(foreign.epg_data_id, None, "{foreign:?}");
    assert_eq!(foreign.logo_id, None, "{foreign:?}");
    assert!(!foreign.hidden_from_output, "{foreign:?}");
    let owned_ids: Vec<i64> = {
        let owned: Vec<(String, i64, String)> = store.owned_keys("lineup").expect("ownership");
        let ids: Vec<i64> = owned
            .into_iter()
            .filter(|(kind, _, _)| kind == "channel")
            .map(|(_, id, _)| id)
            .collect();
        assert!(ids.contains(&bbc.id), "the first channel is owned");
        assert!(ids.contains(&bbc_two.id), "the second channel is owned");
        assert!(
            !ids.contains(&FOREIGN_CHANNEL),
            "the foreign channel is not owned"
        );
        ids
    };
    let held = state
        .lock()
        .expect("mock state")
        .owned_stream_ids(&owned_ids);
    assert!(held.contains(&STREAM_UK), "{held:?}");
    assert!(
        !held.contains(&STREAM_US_SAME_NAME),
        "lineup mode drops it: {held:?}"
    );
    assert!(
        !held.contains(&STREAM_US_PREFIXED),
        "foreign, never attached: {held:?}"
    );

    // Guide matched the owned channel (programme data), never the foreign one.
    assert_eq!(bbc.epg_data_id, Some(ENTRY_BBC), "{bbc:?}");
    let foreign = state
        .lock()
        .expect("mock state")
        .channel(FOREIGN_CHANNEL)
        .expect("foreign");
    assert_eq!(foreign.epg_data_id, None, "{foreign:?}");

    // Logos resolved the assigned entry's icon onto the owned channel.
    let bbc = state
        .lock()
        .expect("mock state")
        .channel(bbc.id)
        .expect("first");
    let logo_id = bbc.logo_id.expect("logos assigned the EPG icon");
    let logos = state.lock().expect("mock state").logos.clone();
    assert!(
        logos
            .iter()
            .any(|logo| logo.id == logo_id && logo.url == "http://icons.example/bbcone.png"),
        "{logos:?}"
    );

    // Tidy hid the owned channel left with no streams — and nothing else.
    let hidden: Vec<String> = state
        .lock()
        .expect("mock state")
        .channels
        .iter()
        .filter(|c| c.hidden_from_output)
        .map(|c| c.name.clone())
        .collect();
    assert!(
        hidden.iter().any(|name| name == SECOND_NAME),
        "tidy hid the stream-less owned channel: {hidden:?}"
    );
    assert!(
        !hidden.iter().any(|name| name == FIRST_NAME),
        "a matched channel stays visible: {hidden:?}"
    );

    // Cross-app consistency: one shared cache, one family for the synced name, its
    // stream's HD-suffixed name and its guide entry.
    let engine = UnifiedEngine::cached(&MatchingConfig::default().engine_config());
    let (channel_key, stream_key, entry_key) = (
        cached_family_key(&store, &engine, FIRST_NAME),
        cached_family_key(&store, &engine, "BBC One HD"),
        cached_family_key(&store, &engine, ENTRY_NAME),
    );
    assert_eq!(channel_key, stream_key, "streams agrees with sync");
    assert_eq!(channel_key, entry_key, "guide agrees with sync");

    // A rerun changes no Dispatcharr state and writes nothing: every step plans
    // against the first run's writes and skips what is already in place.
    let before_writes = state.lock().expect("mock state").writes.clone();
    let before_channels =
        serde_json::to_value(&state.lock().expect("mock state").channels).expect("snapshot");
    let rerun = crosstalk_core::workflow::run_to_completion(
        &runner,
        "full_sync",
        false,
        RunOptions::default(),
    )
    .await
    .expect("the rerun runs");
    assert!(!rerun.is_failed(), "{}", rerun.summary);
    {
        let after = state.lock().expect("mock state");
        assert_eq!(after.writes, before_writes, "the rerun wrote nothing");
        assert_eq!(
            serde_json::to_value(&after.channels).expect("snapshot"),
            before_channels,
            "no state changed"
        );
    }

    // Undo of each step reverses it, last step first.
    let undo = |app: &'static str, action: &str| {
        let target = step_run_id(&store, workflow_id, action);
        runner.run_undo_to_completion_with(app, target, false, RunOptions::default())
    };
    let undone = |outcome: anyhow::Result<crosstalk_core::subapp::JobOutcome>, what: &str| {
        let outcome = outcome.unwrap_or_else(|error| panic!("undo {what} runs: {error:#}"));
        assert!(!outcome.is_failed(), "undo {what}: {}", outcome.summary);
    };
    let read = |id: i64| {
        state
            .lock()
            .expect("mock state")
            .channel(id)
            .unwrap_or_else(|| panic!("channel {id} still listed"))
    };

    undone(undo("lineup", "lineup.tidy_apply").await, "tidy");
    assert!(!read(bbc_two.id).hidden_from_output, "tidy undo unhides");

    undone(undo("lineup", "lineup.logos_apply").await, "logos");
    assert_eq!(read(bbc.id).logo_id, None, "logos undo clears the logo");

    undone(undo("guide", "guide.match_apply").await, "guide match");
    assert_eq!(
        read(bbc.id).epg_data_id,
        None,
        "guide undo clears the entry"
    );

    undone(
        undo("streams", "streams.match_apply").await,
        "streams match",
    );
    assert!(
        read(bbc.id).streams.is_empty(),
        "streams undo clears the list"
    );

    undone(undo("lineup", "lineup.sync_apply").await, "sync");
    assert!(
        read(bbc.id).hidden_from_output,
        "sync undo hides the created channel"
    );
    assert!(
        read(bbc_two.id).hidden_from_output,
        "sync undo hides every created channel"
    );
    let foreign = read(FOREIGN_CHANNEL);
    assert!(
        !foreign.hidden_from_output,
        "the foreign channel is never hidden"
    );
    assert!(foreign.streams.is_empty(), "the foreign list is untouched");
}
