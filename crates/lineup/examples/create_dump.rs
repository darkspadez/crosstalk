//! Dump the create pipeline's decisions for the `oracle_report_lineup_create.py` oracle.
//!
//! Reads one JSON document from stdin:
//!
//! ```json
//! {
//!   "mode": "streams",
//!   "channels": [{"id": 1, "name": "CNN", "group": "Test", "number": 1, "streams": []}],
//!   "groups": [{"id": 10, "name": "Test"}],
//!   "streams": [{"id": 101, "name": "CNN", "group": "Sports", "m3u_account": "Main"}],
//!   "accounts": [{"id": 1, "name": "Main"}],
//!   "settings": {"create_mode": "streams", "m3u_group_filter": "", ...},
//!   "streams_settings": {"selected_m3us": ""}
//! }
//! ```
//!
//! `settings` is a partial `lineup.toml` create section and `streams_settings` a
//! partial `streams.toml` selection section: unknown fields are refused, missing
//! ones default (serde `default` + `deny_unknown_fields`, like the files).
//! Prints one JSON object with the mode's decision rows — the same decisions
//! ([`decide_streams`](crosstalk_lineup::create::decide_streams) /
//! [`decide_stations`](crosstalk_lineup::create::decide_stations)) the action
//! plans from, plus the planned request (final names, numbers, groups) — to
//! stdout. The oracle compares these against Channel-Mapparr's real
//! `import_m3u_streams` / `create_channels_from_streams` over the same world.

use crosstalk_core::config::CrosstalkConfig;
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, M3uAccount, Stream, World};
use crosstalk_lineup::create as create_mod;
use std::collections::{HashMap, HashSet};
use std::io::Read;

#[derive(Debug, serde::Deserialize)]
struct InputChannel {
    id: i64,
    name: String,
    #[serde(default)]
    group: String,
    #[serde(default)]
    number: Option<f64>,
    #[serde(default)]
    streams: Vec<i64>,
}

#[derive(Debug, serde::Deserialize)]
struct InputGroup {
    id: i64,
    name: String,
}

#[derive(Debug, serde::Deserialize)]
struct InputStream {
    id: i64,
    name: String,
    #[serde(default)]
    group: String,
    #[serde(default)]
    m3u_account: String,
}

#[derive(Debug, serde::Deserialize)]
struct InputAccount {
    id: i64,
    name: String,
}

#[derive(Debug, serde::Deserialize)]
struct Input {
    #[serde(default = "default_mode")]
    mode: String,
    #[serde(default)]
    channels: Vec<InputChannel>,
    #[serde(default)]
    groups: Vec<InputGroup>,
    #[serde(default)]
    streams: Vec<InputStream>,
    #[serde(default)]
    accounts: Vec<InputAccount>,
    #[serde(default)]
    settings: serde_json::Value,
    #[serde(default)]
    streams_settings: serde_json::Value,
}

fn default_mode() -> String {
    "streams".to_string()
}

fn main() -> anyhow::Result<()> {
    let mut text = String::new();
    std::io::stdin().read_to_string(&mut text)?;
    let input: Input = serde_json::from_str(&text)?;

    let settings: crosstalk_lineup::settings::LineupSettings = if input.settings.is_null()
        || input.settings.as_object().is_some_and(|map| map.is_empty())
    {
        crosstalk_lineup::settings::LineupSettings::default()
    } else {
        serde_json::from_value(input.settings)?
    };
    let streams_settings: crosstalk_streams::settings::StreamsSettings =
        if input.streams_settings.is_null()
            || input
                .streams_settings
                .as_object()
                .is_some_and(|map| map.is_empty())
        {
            crosstalk_streams::settings::StreamsSettings::default()
        } else {
            serde_json::from_value(input.streams_settings)?
        };

    // Groups first (streams and channels resolve names through them, like the run).
    let mut group_ids: HashMap<String, i64> = HashMap::new();
    let mut groups: Vec<ChannelGroup> = Vec::new();
    for group in &input.groups {
        group_ids.insert(group.name.clone(), group.id);
        groups.push(ChannelGroup {
            id: group.id,
            name: group.name.clone(),
        });
    }
    let mut group_id_of = |name: &str| -> Option<i64> {
        if name.is_empty() {
            return None;
        }
        if let Some(&id) = group_ids.get(name) {
            return Some(id);
        }
        let id = 1000 + group_ids.len() as i64;
        group_ids.insert(name.to_string(), id);
        groups.push(ChannelGroup {
            id,
            name: name.to_string(),
        });
        Some(id)
    };
    let mut account_ids: HashMap<String, i64> = HashMap::new();
    let mut accounts: Vec<M3uAccount> = Vec::new();
    for account in &input.accounts {
        account_ids.insert(account.name.clone(), account.id);
        accounts.push(M3uAccount {
            id: account.id,
            name: account.name.clone(),
            is_active: true,
            ..M3uAccount::default()
        });
    }
    let world_channels: Vec<Channel> = input
        .channels
        .iter()
        .map(|channel| Channel {
            id: channel.id,
            name: channel.name.clone(),
            channel_number: channel.number.map(|n| serde_json::json!(n)),
            channel_group_id: group_id_of(&channel.group),
            streams: channel.streams.clone(),
            ..Channel::default()
        })
        .collect();
    let world_streams: Vec<Stream> = input
        .streams
        .iter()
        .map(|stream| Stream {
            id: stream.id,
            name: stream.name.clone(),
            m3u_account: account_ids.get(&stream.m3u_account).copied(),
            channel_group: group_id_of(&stream.group),
            ..Stream::default()
        })
        .collect();
    let world = World {
        channels: world_channels,
        groups,
        streams: world_streams,
        m3u_accounts: accounts,
        ..World::default()
    };

    // The same stack the action runs on: host `[matching]` defaults, no store.
    let config = CrosstalkConfig::default();
    let run_engine = crosstalk_core::engine_run::RunEngine::for_config(&config.matching, None);
    let engine = std::sync::Arc::clone(run_engine.engine());
    let db_names = crosstalk_lineup::names::premium_db_names(&config.matching.databases());
    let categories = crosstalk_lineup::categorize::category_map(&config.matching.databases());
    let index = std::sync::Arc::new(run_engine.build_index(&db_names));

    if input.mode == "stations" {
        let wanted: HashSet<&str> = settings
            .seed_source_groups
            .split(',')
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .collect();
        let linked: HashSet<i64> = world
            .channels
            .iter()
            .flat_map(|channel| channel.streams.iter().copied())
            .collect();
        let account_names: HashMap<i64, String> = world
            .m3u_accounts
            .iter()
            .map(|account| (account.id, account.name.clone()))
            .collect();
        let mut candidates: Vec<create_mod::StationCandidate> = Vec::new();
        for stream in &world.streams {
            let group = world.group_name(stream.channel_group);
            if !wanted.contains(group.as_str()) {
                continue;
            }
            if linked.contains(&stream.id) || stream.name.trim().is_empty() {
                continue;
            }
            candidates.push(create_mod::StationCandidate {
                id: stream.id,
                name: stream.name.clone(),
                group,
                account: stream
                    .m3u_account
                    .and_then(|id| account_names.get(&id).cloned())
                    .unwrap_or_else(|| "custom".to_string()),
            });
        }
        let existing: HashSet<String> = world
            .channels
            .iter()
            .map(|channel| channel.name.trim().to_lowercase())
            .collect();
        let mut rows = create_mod::decide_stations(
            &candidates,
            &engine,
            &settings,
            &existing,
            &HashMap::new(),
        );
        let request = create_mod::build_stations_request(
            &world,
            &mut rows,
            &settings,
            settings.seed_target_group.trim(),
        )?;
        let dumped: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "action": row.action,
                    "proposed": row.proposed,
                    "callsign": row.callsign,
                    "source_names": row.source_names,
                    "stream_ids": row.stream_ids,
                    "accounts": row.accounts,
                    "number": row.number,
                    "reason": row.reason,
                    "key": row.key,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "mode": "stations",
                "rows": dumped,
                "request": serde_json::json!({
                    "groups": request.groups.iter().map(|g| &g.name).collect::<Vec<_>>(),
                    "channels": request.channels.iter().map(|c| serde_json::json!({
                        "name": c.name, "number": c.number, "key": c.key,
                    })).collect::<Vec<_>>(),
                }),
            }))?
        );
        return Ok(());
    }

    // Streams mode (the default): the real pool, the real filter, the real decisions.
    let pool = crosstalk_streams::selection::build(&world, &streams_settings)?;
    let candidates = create_mod::streams_candidates(&world, &pool, &settings.m3u_group_filter);
    let stack = create_mod::EngineStack {
        engine: &engine,
        index: &index,
        db_names: &db_names,
        categories: &categories,
    };
    let world_names = create_mod::world_identities(&run_engine, &world);
    let mut rows = create_mod::decide_streams(
        &candidates,
        &stack,
        &settings,
        &HashMap::new(),
        &world_names,
    );
    // The streams app's quality order over the same pool (no store: every
    // stream's throughput is unknown, like a store-less run).
    let model = crosstalk_streams::quality::QualityModel::from_settings(&streams_settings);
    let throughput = crosstalk_streams::quality::MemoryThroughputSource::new();
    let order = |ids: &[i64]| create_mod::quality_order(&pool, &model, &throughput, ids);
    let request = create_mod::build_streams_request(&world, &mut rows, &order)?;
    let dumped: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "stream_id": row.stream_id,
                "stream_name": row.stream_name,
                "m3u_source": row.m3u_source,
                "priority": row.priority,
                "matcher": row.matcher,
                "method": row.method,
                "category": row.category,
                "target_group": row.target_group,
                "will_import": row.will_import,
                "is_head": row.is_head,
                "family": row.family,
                "family_size": row.family_size,
                "channel_name": row.channel_name,
                "number": row.number,
                "note": row.note,
                "callsign": row.identity.callsign,
                "canonical": row.identity.canonical_name,
            })
        })
        .collect();
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "mode": "streams",
            "rows": dumped,
            "request": serde_json::json!({
                "groups": request.groups.iter().map(|g| &g.name).collect::<Vec<_>>(),
                "channels": request.channels.iter().map(|c| serde_json::json!({
                    "name": c.name, "number": c.number,
                    "streams": c.streams, "key": c.key,
                })).collect::<Vec<_>>(),
            }),
        }))?
    );
    Ok(())
}
