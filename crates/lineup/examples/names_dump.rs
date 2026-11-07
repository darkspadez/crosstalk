//! Dump the names pipeline's decisions for the `oracle_report_lineup_names.py` oracle.
//!
//! Reads one JSON document from stdin:
//!
//! ```json
//! {
//!   "channels": [{"id": 1, "name": "ABC (WABC)", "group": "Test", "number": 1}],
//!   "scope": {"groups": "", "ignore_groups": ""},
//!   "settings": {"ota_format": "...", "tag_unmatched_channels": true, ...}
//! }
//! ```
//!
//! `settings` is a partial `lineup.toml` names section: unknown fields are refused,
//! missing ones default (serde `default` + `deny_unknown_fields`, like the file).
//! Prints one JSON array of per-channel rows — the same decision
//! ([`NameRow`](crosstalk_lineup::names::NameRow) fields plus the engineered target)
//! the action plans from — to stdout. The oracle compares these against
//! Channel-Mapparr's real `load_and_process_channels` over the same corpus.

use crosstalk_core::config::ScopeConfig;
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, World};
use crosstalk_core::engine_run::RunEngine;
use crosstalk_lineup::names::NameTarget;
use crosstalk_matching::identity::ResolveContext;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

#[derive(Debug, serde::Deserialize)]
struct InputChannel {
    id: i64,
    name: String,
    #[serde(default)]
    group: String,
    #[serde(default)]
    number: Option<f64>,
}

#[derive(Debug, serde::Deserialize)]
struct Input {
    channels: Vec<InputChannel>,
    #[serde(default)]
    scope: ScopeInput,
    #[serde(default)]
    settings: serde_json::Value,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ScopeInput {
    #[serde(default)]
    groups: String,
    #[serde(default)]
    ignore_groups: String,
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

    let mut group_ids: HashMap<String, i64> = HashMap::new();
    let mut groups: Vec<ChannelGroup> = Vec::new();
    for channel in &input.channels {
        if !channel.group.is_empty() && !group_ids.contains_key(&channel.group) {
            let id = 1000 + group_ids.len() as i64;
            group_ids.insert(channel.group.clone(), id);
            groups.push(ChannelGroup {
                id,
                name: channel.group.clone(),
            });
        }
    }
    let world = World {
        channels: input
            .channels
            .iter()
            .map(|channel| Channel {
                id: channel.id,
                name: channel.name.clone(),
                channel_number: channel
                    .number
                    .map(|n| serde_json::json!(n))
                    .or(Some(serde_json::json!(channel.id as f64))),
                channel_group_id: group_ids.get(&channel.group).copied(),
                ..Channel::default()
            })
            .collect(),
        groups,
        ..World::default()
    };

    let scope = ScopeConfig {
        groups: input.scope.groups,
        ignore_groups: input.scope.ignore_groups,
        ..ScopeConfig::default()
    };
    let scoped = crosstalk_core::scope::scope_channels(&world, &scope)?;

    // The same stack the action runs on: host `[matching]` defaults, no store.
    // (`CrosstalkConfig::default` carries no config dir; the oracle never reads files.)
    let config = crosstalk_core::config::CrosstalkConfig::default();
    let run_engine = RunEngine::for_config(&config.matching, None);
    let engine = Arc::clone(run_engine.engine());
    let db_names = crosstalk_lineup::names::premium_db_names(&config.matching.databases());
    let index = run_engine.build_index(&db_names);
    // The same ranker with no cut: what the unified engine scores below the host
    // cut, so the oracle can verify a `cut` attribution instead of assuming it.
    let floor_engine = engine.with_threshold(0);

    let mut dumped = Vec::new();
    for channel in &scoped.channels {
        let scoped_channel = crosstalk_lineup::names::ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            number: channel.display_number(),
            group: world.group_name(channel.channel_group_id),
        };
        let identity = run_engine.resolve(&channel.name, &ResolveContext::default());
        let (row, _) = crosstalk_lineup::names::decide_heuristic(
            &scoped_channel,
            &identity,
            &engine,
            &index,
            &db_names,
            &settings,
        );
        // The decision kind and target behind the row (the one naming decision,
        // which `create` reads too): the create oracle checks create == names.
        let (target_kind, target_name) = match crosstalk_lineup::names::decide_target(
            &channel.name,
            &identity,
            &engine,
            &index,
            &db_names,
            &settings,
        ) {
            NameTarget::Ota {
                name: Some(name), ..
            } => ("ota", Some(name)),
            NameTarget::Ota { name: None, .. } => ("ota-missing-fields", None),
            NameTarget::BlockedCallsign(_) => ("blocked-callsign", None),
            NameTarget::Premium { name, .. } => ("premium", Some(name)),
            NameTarget::Unmatched => ("unmatched", None),
        };
        // The family leader (before tag re-attachment) for attribution: the DB
        // name the unified engine ranked first at the cut, if any.
        let family = engine.rank_index(&channel.name, &index);
        let (leader, leader_score) = match family.first() {
            Some(top) => (
                serde_json::json!(db_names.get(top.index)),
                serde_json::json!(top.score),
            ),
            None => (serde_json::json!(null), serde_json::json!(null)),
        };
        let floor: Vec<serde_json::Value> = floor_engine
            .shortlist_index_in(&channel.name, None, &index)
            .iter()
            .take(5)
            .map(|member| serde_json::json!([db_names.get(member.index), member.score]))
            .collect();
        dumped.push(serde_json::json!({
            "id": row.id,
            "group": row.group,
            "status": row.status,
            "old": row.old,
            "new": row.new,
            "matcher": row.matcher,
            "method": row.method,
            "reason": row.reason,
            "callsign": row.identity.callsign,
            "network": row.identity.network,
            "country": row.identity.country,
            "canonical": row.identity.canonical_name,
            "leader": leader,
            "leader_score": leader_score,
            "floor": floor,
            "target_kind": target_kind,
            "target_name": target_name,
        }));
    }
    println!("{}", serde_json::to_string(&dumped)?);
    Ok(())
}
