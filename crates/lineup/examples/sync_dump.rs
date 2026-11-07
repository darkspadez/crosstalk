//! Dump the sync pipeline's decisions for `scripts/oracle_report_lineup_sync.py`.
//!
//! Reads one JSON document from stdin: either a single case
//!
//! ```json
//! {
//!   "settings": {"lineup_file": "US_Combined_lineup.json", "category_detail": "normal", ...},
//!   "channels": [{"id": 1, "name": "CNN", "number": 206, "group": "DIRECTV: News"}],
//!   "groups": [{"id": 10, "name": "DIRECTV: News"}],
//!   "profiles": [{"id": 3, "name": "Living Room"}],
//!   "owned": {"lineup:US_Combined_lineup.json:CNN": 1},
//!   "owned_groups": [10]
//! }
//! ```
//!
//! or a batch: `{"cases": [{"id": "case-1", ...same fields...}, ...]}` (one
//! process for the whole oracle matrix, so the 464-case sweep costs one Rust
//! startup, not 464). `settings` is a partial `lineup.toml` sync section:
//! unknown fields are refused, missing ones default (serde `default` +
//! `deny_unknown_fields`, like the file). `owned` maps channel owner keys to
//! live channel ids; `owned_groups` lists the live group ids this sync owns.
//!
//! Prints one JSON object (or `{"cases": [...]}` for a batch) with the loaded
//! lineup's groups (name, owner key), one decision row per entry (the same
//! [`decide_sync`](crosstalk_lineup::sync::decide_sync) +
//! [`plan_sync_creates`](crosstalk_lineup::sync::plan_sync_creates) the action
//! plans from),
//! the legacy migration decision and the resolved profile ids — the oracle
//! compares these against Lineuparr's real `_sync_groups` + `_sync_channels`
//! over the same world.

use crosstalk_lineup::sync as sync_mod;
use std::collections::{HashMap, HashSet};
use std::io::Read;

#[derive(Debug, serde::Deserialize)]
struct InputChannel {
    id: i64,
    name: String,
    #[serde(default)]
    number: Option<f64>,
    #[serde(default)]
    group: String,
}

#[derive(Debug, serde::Deserialize)]
struct InputGroup {
    id: i64,
    name: String,
}

#[derive(Debug, serde::Deserialize)]
struct InputProfile {
    id: i64,
    name: String,
}

#[derive(Debug, serde::Deserialize)]
struct Case {
    #[serde(default)]
    id: String,
    #[serde(default)]
    settings: serde_json::Value,
    #[serde(default)]
    channels: Vec<InputChannel>,
    #[serde(default)]
    groups: Vec<InputGroup>,
    #[serde(default)]
    profiles: Vec<InputProfile>,
    #[serde(default)]
    owned: HashMap<String, i64>,
    #[serde(default)]
    owned_groups: Vec<i64>,
}

#[derive(Debug, serde::Deserialize)]
struct Input {
    #[serde(default)]
    cases: Vec<Case>,
    // A single case carries the case fields at the top level.
    #[serde(default)]
    id: String,
    #[serde(default)]
    settings: serde_json::Value,
    #[serde(default)]
    channels: Vec<InputChannel>,
    #[serde(default)]
    groups: Vec<InputGroup>,
    #[serde(default)]
    profiles: Vec<InputProfile>,
    #[serde(default)]
    owned: HashMap<String, i64>,
    #[serde(default)]
    owned_groups: Vec<i64>,
}

fn main() -> anyhow::Result<()> {
    let mut text = String::new();
    std::io::stdin().read_to_string(&mut text)?;
    let input: Input = serde_json::from_str(&text)?;
    let batched = !input.cases.is_empty();
    let cases = if input.cases.is_empty() {
        vec![Case {
            id: input.id,
            settings: input.settings,
            channels: input.channels,
            groups: input.groups,
            profiles: input.profiles,
            owned: input.owned,
            owned_groups: input.owned_groups,
        }]
    } else {
        input.cases
    };
    let mut out = Vec::with_capacity(cases.len());
    for case in &cases {
        out.push(run_case(case)?);
    }
    if batched {
        println!("{}", serde_json::json!({"cases": out}));
    } else {
        println!(
            "{}",
            out.into_iter().next().unwrap_or(serde_json::json!({}))
        );
    }
    Ok(())
}

fn group_id_of(
    name: &str,
    groups: &mut Vec<crosstalk_core::dispatcharr::ChannelGroup>,
    group_ids: &mut HashMap<String, i64>,
) -> Option<i64> {
    if name.is_empty() {
        return None;
    }
    if let Some(&id) = group_ids.get(name) {
        return Some(id);
    }
    let id = 1_000_000 + group_ids.len() as i64;
    group_ids.insert(name.to_string(), id);
    groups.push(crosstalk_core::dispatcharr::ChannelGroup {
        id,
        name: name.to_string(),
    });
    Some(id)
}

fn run_case(case: &Case) -> anyhow::Result<serde_json::Value> {
    let settings: crosstalk_lineup::settings::LineupSettings =
        if case.settings.is_null() || case.settings.as_object().is_some_and(|map| map.is_empty()) {
            crosstalk_lineup::settings::LineupSettings::default()
        } else {
            serde_json::from_value(case.settings.clone())?
        };
    // Groups first (channels resolve names through them, like the run).
    let mut group_ids: HashMap<String, i64> = HashMap::new();
    let mut groups: Vec<crosstalk_core::dispatcharr::ChannelGroup> = Vec::new();
    for group in &case.groups {
        group_ids.insert(group.name.clone(), group.id);
        groups.push(crosstalk_core::dispatcharr::ChannelGroup {
            id: group.id,
            name: group.name.clone(),
        });
    }
    let mut world_channels: Vec<crosstalk_core::dispatcharr::Channel> = Vec::new();
    for channel in &case.channels {
        world_channels.push(crosstalk_core::dispatcharr::Channel {
            id: channel.id,
            name: channel.name.clone(),
            channel_number: channel.number.map(|number| serde_json::json!(number)),
            channel_group_id: group_id_of(&channel.group, &mut groups, &mut group_ids),
            ..crosstalk_core::dispatcharr::Channel::default()
        });
    }
    let world_profiles: Vec<crosstalk_core::dispatcharr::ChannelProfile> = case
        .profiles
        .iter()
        .map(|profile| crosstalk_core::dispatcharr::ChannelProfile {
            id: profile.id,
            name: profile.name.clone(),
            channels: Vec::new(),
        })
        .collect();
    let world = crosstalk_core::dispatcharr::World {
        channels: world_channels,
        groups,
        profiles: world_profiles,
        ..crosstalk_core::dispatcharr::World::default()
    };

    let lineup = crosstalk_lineup::lineup_file::load_lineup(
        &settings.lineup_file,
        &settings.category_detail,
        &settings.group_prefix,
    )?;
    let mut decisions = sync_mod::decide_sync(&world, &lineup, &settings, &case.owned)?;
    // The action's create planning too (owned-key dedupe, the global number
    // check): a refused create is a `Skipped` row here exactly as in the run.
    let (profile_ids, unknown_profiles) = sync_mod::resolve_profile_ids(&world, &settings);
    sync_mod::plan_sync_creates(&world, &case.owned, &mut decisions, profile_ids.clone())?;
    let owned_groups: HashSet<i64> = case.owned_groups.iter().copied().collect();
    let migration = if lineup
        .categories
        .iter()
        .any(|category| category == crosstalk_lineup::lineup_file::SINGLE_GROUP_CATEGORY)
    {
        sync_mod::decide_migration(&world, &lineup.prefix, &owned_groups)
    } else {
        let (old, new) = sync_mod::migration_names(&lineup.prefix);
        crosstalk_lineup::sync::Migration {
            old,
            new,
            id: None,
            action: crosstalk_lineup::sync::MigrationAction::None,
            reason: "category_detail is not \"none\": nothing to migrate".to_string(),
        }
    };

    let rows: Vec<serde_json::Value> = decisions
        .rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "name": row.name,
                "number": row.number,
                "category": row.category,
                "group": row.group,
                "country": row.country,
                "channel_id": row.channel_id,
                "current_group": row.current_group,
                "current_number": row.current_number,
                "action": format!("{:?}", row.action),
                "reason": row.reason,
                "key": sync_mod::channel_owner_key(&decisions.file, &row.name),
            })
        })
        .collect();
    Ok(serde_json::json!({
        "id": case.id,
        "file": decisions.file,
        "prefix": lineup.prefix,
        "detail": lineup.detail,
        "groups": decisions.groups.iter().map(|group| serde_json::json!({
            "name": group.name,
            "key": group.key,
        })).collect::<Vec<_>>(),
        "rows": rows,
        "migration": {
            "old": migration.old,
            "new": migration.new,
            "id": migration.id,
            "action": format!("{:?}", migration.action),
            "reason": migration.reason,
        },
        "profiles": {
            "resolved": profile_ids.unwrap_or_default(),
            "unknown": unknown_profiles,
        },
    }))
}
