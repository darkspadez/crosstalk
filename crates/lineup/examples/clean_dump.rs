//! Dump the clean pipeline's final outcomes for the `clean_parity.py` oracle.
//!
//! Reads one JSON document from stdin:
//!
//! ```json
//! {
//!   "channels": [{"id": 1, "name": "Sky Atlantic | FHD |", "group": "Sports"}],
//!   "scope": {"groups": "", "ignore_groups": ""},
//!   "settings": {"strip_pipe_junk": true, ...}
//! }
//! ```
//!
//! `settings` is a partial `lineup.toml` clean section: unknown fields are refused,
//! missing ones default (serde `default` + `deny_unknown_fields`, like the file).
//! Prints one JSON array of `{id, group, status, old, new, rules, removed}` — the
//! same [`CleanOutput`](crosstalk_lineup::clean::CleanOutput) the action writes to
//! its CSV — to stdout. The oracle compares these against Name Cleaner's real
//! `_compute_changes` over the same corpus.

use crosstalk_core::config::ScopeConfig;
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, World};
use crosstalk_lineup::clean;
use std::collections::HashMap;
use std::io::Read;

#[derive(Debug, serde::Deserialize)]
struct InputChannel {
    id: i64,
    name: String,
    #[serde(default)]
    group: String,
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

    // Groups are numbered in first-seen order; the ids never leave this process.
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
                channel_number: Some(serde_json::json!(channel.id as f64)),
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
    let selection = crosstalk_core::channel_ops::Selection::in_scope(&scope);
    // The oracle never uses wildcard spellings for scope: NC matches group names
    // exact-casefold, and the harnessed comparison would otherwise measure the
    // documented unified-scope divergence instead of the pipeline.
    let scoped = crosstalk_core::scope::scope_channels(&world, &scope)?;
    let channels: Vec<clean::ScopedChannel> = scoped
        .channels
        .iter()
        .map(|channel| clean::ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            group: world.group_name(channel.channel_group_id),
        })
        .collect();

    let rules = clean::CleanRules::from_settings(&settings);
    let custom = clean::parse_custom_rules(&settings.custom_rules);
    let protect_re = clean::compile_protect_regex(&settings.protect_regex);
    let rows = clean::compute_rows(&channels, &settings, &rules, &custom, protect_re.as_ref());
    let plan = clean::plan_clean(&world, &selection, &rows, &settings)?;
    let outputs = clean::final_outputs(&rows, &plan);

    let dumped: Vec<serde_json::Value> = outputs
        .iter()
        .map(|output| {
            serde_json::json!({
                "id": output.id,
                "group": output.group,
                "status": output.status,
                "old": output.old,
                "new": output.new,
                "rules": output.rules,
                "removed": output.removed,
            })
        })
        .collect();
    println!("{}", serde_json::to_string(&dumped)?);
    Ok(())
}
