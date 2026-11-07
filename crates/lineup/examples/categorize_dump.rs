//! Dump the categorize pipeline's decisions for the `oracle_report_lineup_categorize.py` oracle.
//!
//! Reads one JSON document from stdin:
//!
//! ```json
//! {
//!   "channels": [{"id": 1, "name": "CNN", "group": "Test", "number": 1}],
//!   "scope": {"groups": "", "ignore_groups": ""},
//!   "settings": {"category_groups": ""}
//! }
//! ```
//!
//! `settings` is a partial `lineup.toml` categorize section: unknown fields are refused,
//! missing ones default (serde `default` + `deny_unknown_fields`, like the file).
//! Prints one JSON array of per-channel rows — the same decision
//! ([`CategoryRow`](crosstalk_lineup::categorize::CategoryRow) fields) the action plans
//! from, plus the ranker's no-cut `floor` shortlist (DB name, score, category) that
//! verifies a `cut` attribution, and the no-cut score of each channel's optional
//! `probe` DB name over the whole shortlist — to stdout. The oracle compares these against
//! Channel-Mapparr's real `category_groups_dry_run` decision path over the same corpus.

use crosstalk_core::config::ScopeConfig;
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, World};
use crosstalk_core::engine_run::RunEngine;
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
    /// A DB name to score against this channel with no cut (the oracle's CM
    /// winner): `probe_score` answers whether the ranker scores it at all, and how.
    #[serde(default)]
    probe: Option<String>,
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

    let probes: HashMap<i64, String> = input
        .channels
        .iter()
        .filter_map(|channel| channel.probe.clone().map(|probe| (channel.id, probe)))
        .collect();
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
    // CM's category scope on top of the host scope, like the action.
    let channels = crosstalk_lineup::categorize::restrict_to_category_scope(
        &world,
        &scoped.channels,
        &settings.category_groups,
    )?;

    // The same stack the action runs on: host `[matching]` defaults, no store.
    let config = crosstalk_core::config::CrosstalkConfig::default();
    let run_engine = RunEngine::for_config(&config.matching, None);
    let engine = Arc::clone(run_engine.engine());
    let db_names = crosstalk_lineup::names::premium_db_names(&config.matching.databases());
    let categories = crosstalk_lineup::categorize::category_map(&config.matching.databases());
    let index = run_engine.build_index(&db_names);
    // The same ranker with no cut: what the unified engine scores below the host
    // cut, so the oracle can verify a `cut` attribution instead of assuming it.
    let floor_engine = engine.with_threshold(0);

    let mut dumped = Vec::new();
    for channel in &channels {
        let scoped_channel = crosstalk_lineup::categorize::ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            number: channel.display_number(),
            group: world.group_name(channel.channel_group_id),
        };
        let identity = run_engine.resolve(&channel.name, &ResolveContext::default());
        let row = crosstalk_lineup::categorize::decide_category(
            &scoped_channel,
            &identity,
            &engine,
            &index,
            &db_names,
            &categories,
        );
        let shortlist = floor_engine.shortlist_index_in(&channel.name, None, &index);
        // The probe's no-cut score over the WHOLE shortlist (never a truncated
        // view): `null` means the ranker never scores it, even with no cut.
        let probe_score = probes.get(&channel.id).and_then(|probe| {
            shortlist
                .iter()
                .find(|member| db_names.get(member.index) == Some(probe))
                .map(|member| member.score)
        });
        let floor: Vec<serde_json::Value> = shortlist
            .iter()
            .take(5)
            .map(|member| {
                let name = db_names.get(member.index).cloned().unwrap_or_default();
                let category = categories
                    .get(&name.to_lowercase())
                    .map(|(_, category)| category.clone())
                    .unwrap_or_default();
                serde_json::json!([name, member.score, category])
            })
            .collect();
        dumped.push(serde_json::json!({
            "id": row.id,
            "group": row.group,
            "old": row.old,
            "category": row.category,
            "match_name": row.match_name,
            "match_type": row.match_type,
            "score": row.score,
            "reason": row.reason,
            "callsign": row.identity.callsign,
            "network": row.identity.network,
            "country": row.identity.country,
            "canonical": row.identity.canonical_name,
            "floor": floor,
            "probe_score": probe_score,
        }));
    }
    println!("{}", serde_json::to_string(&dumped)?);
    Ok(())
}
