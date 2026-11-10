//! Dump the logos pipeline's decisions for the `oracle_report_lineup_logos.py` oracle.
//!
//! Reads one JSON document from stdin:
//!
//! ```json
//! {
//!   "channels": [{"id": 1, "name": "CNN", "group": "Test", "number": 1,
//!                 "logo_id": null, "epg_id": 40}],
//!   "groups": [{"id": 10, "name": "Test"}],
//!   "logos": [{"id": 71, "name": "ESPN", "url": "https://example.com/espn.png"}],
//!   "entries": [{"id": 40, "name": "CNN entry",
//!                "icon_url": "https://example.com/epg-cnn.png"}],
//!   "scope": {"groups": "", "ignore_groups": ""},
//!   "settings": {"logo_sources": "epg, logo_manager, tv-logos",
//!                "replace_existing": false, "default_logo": "",
//!                "tv_logos_countries": ""},
//!   "tree": {"tree": [{"path": "countries/united-states/cnn-us.png",
//!                      "type": "blob"}]}
//! }
//! ```
//!
//! `settings` is a partial `lineup.toml` logos section: unknown fields are refused,
//! missing ones default. `tree` is the recorded tv-logos recursive-tree payload (or
//! null when the tv-logos tier is off — the dump then resolves exactly like the
//! action with no index). Prints one JSON array of per-channel rows — the same
//! decision ([`LogoRow`](crosstalk_lineup::logos::LogoRow) fields) the action plans
//! from, plus the per-tier evidence (EPG icon used, manager hit, default hit, the
//! searched country dirs and every scored tv-logos file) that verifies the
//! attribution — to stdout. The oracle compares these against Channel-Mapparr's
//! `_assign_tv_logos` and Lineuparr's `apply_logo_match` tiers over the same
//! recorded tree.

use crosstalk_core::config::ScopeConfig;
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, EpgEntry, Logo, World};
use crosstalk_core::engine_run::RunEngine;
use crosstalk_core::logos::TvLogosIndex;
use crosstalk_matching::identity::ResolveContext;
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
    logo_id: Option<i64>,
    #[serde(default)]
    epg_id: Option<i64>,
}

#[derive(Debug, serde::Deserialize)]
struct InputGroup {
    id: i64,
    name: String,
}

#[derive(Debug, serde::Deserialize)]
struct InputLogo {
    id: i64,
    name: String,
    url: String,
}

#[derive(Debug, serde::Deserialize)]
struct InputEntry {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    icon_url: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct Input {
    channels: Vec<InputChannel>,
    #[serde(default)]
    groups: Vec<InputGroup>,
    #[serde(default)]
    logos: Vec<InputLogo>,
    #[serde(default)]
    entries: Vec<InputEntry>,
    #[serde(default)]
    scope: ScopeInput,
    #[serde(default)]
    settings: serde_json::Value,
    #[serde(default)]
    tree: Option<serde_json::Value>,
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
    let order = crosstalk_core::logos::LogoSource::parse_order(&settings.logo_sources)?;

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
                channel_group_id: input
                    .groups
                    .iter()
                    .find(|group| group.name == channel.group)
                    .map(|group| group.id),
                logo_id: channel.logo_id,
                epg_data_id: channel.epg_id,
                ..Channel::default()
            })
            .collect(),
        groups: input
            .groups
            .iter()
            .map(|group| ChannelGroup {
                id: group.id,
                name: group.name.clone(),
            })
            .collect(),
        logos: input
            .logos
            .iter()
            .map(|logo| Logo {
                id: logo.id,
                name: logo.name.clone(),
                url: logo.url.clone(),
                ..Logo::default()
            })
            .collect(),
        entries: input
            .entries
            .iter()
            .map(|entry| EpgEntry {
                id: entry.id,
                name: entry.name.clone(),
                icon_url: entry.icon_url.clone(),
                ..EpgEntry::default()
            })
            .collect(),
        ..World::default()
    };

    let scope = ScopeConfig {
        groups: input.scope.groups,
        ignore_groups: input.scope.ignore_groups,
        ..ScopeConfig::default()
    };
    let scoped = crosstalk_core::scope::scope_channels(&world, &scope)?;
    let lookups = crosstalk_lineup::logos::LogoWorld::of(&world);
    let index = input.tree.as_ref().map(TvLogosIndex::from_tree);
    let sources = lookups.sources(order, index.as_ref(), &settings);

    // The same stack the action runs on: host `[matching]` defaults, no store.
    let config = crosstalk_core::config::CrosstalkConfig::default();
    let run_engine = RunEngine::for_config(&config.matching, None);
    let ctx = ResolveContext::default();

    let mut dumped = Vec::new();
    for channel in &scoped.channels {
        let scoped_channel = crosstalk_lineup::logos::ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            number: channel.display_number(),
            group: world.group_name(channel.channel_group_id),
            logo_id: channel.logo_id,
            epg_id: channel.current_epg_id(),
        };
        let identity = run_engine.resolve(&channel.name, &ctx);
        let codes = crosstalk_lineup::logos::tv_countries(
            &settings,
            &identity,
            &config.matching.databases(),
        );
        let row = crosstalk_lineup::logos::decide_logo(
            &scoped_channel,
            &identity,
            &lookups,
            &sources,
            &codes,
            &settings,
        );
        // The tier evidence the attribution checks read.
        let epg_icon = channel
            .current_epg_id()
            .and_then(|id| lookups.epg_icons.get(&id).copied());
        let manager_hit = lookups
            .by_name
            .get(&channel.name.to_lowercase())
            .map(|(_, url)| url.clone());
        let default_hit = {
            let name = settings.default_logo.trim();
            if name.is_empty() {
                None
            } else {
                lookups
                    .by_name
                    .get(&name.to_lowercase())
                    .map(|(_, url)| url.clone())
            }
        };
        // Every directory the tv-logos tier may read: each code's country directory
        // and, for a station-shaped name, its station directory too.
        let searched: Vec<serde_json::Value> = codes
            .iter()
            .filter_map(|code| crosstalk_core::logos::country_dir_for(code).map(|dir| (code, dir)))
            .flat_map(|(code, dir)| {
                std::iter::once(serde_json::json!({"code": code, "dir": dir})).chain(
                    TvLogosIndex::station_dir_for(dir)
                        .map(|station| serde_json::json!({"code": code, "dir": station})),
                )
            })
            .collect();
        let (tv_hit, tv_scored) = match index.as_ref() {
            Some(index) => {
                let (hit, scored) = crosstalk_core::logos::search_tv_logos(
                    crosstalk_lineup::logos::tv_match_name(&channel.name),
                    index,
                    &codes,
                    crosstalk_core::logos::LOGO_MATCH_THRESHOLD,
                );
                let hit = hit.map(|hit| {
                    serde_json::json!({
                        "url": hit.url, "file": hit.file,
                        "dir": hit.dir, "score": hit.score,
                    })
                });
                let scored = scored
                    .into_iter()
                    .map(|file| {
                        serde_json::json!({
                            "dir": file.dir, "file": file.file,
                            "score": file.score, "norm_file": file.norm_file,
                        })
                    })
                    .collect::<Vec<_>>();
                (hit, scored)
            }
            None => (None, Vec::new()),
        };
        dumped.push(serde_json::json!({
            "id": row.id,
            "group": row.group,
            "old": row.old,
            "url": row.url,
            "source": row.source.map(|source| source.label().to_string()),
            "reason": row.reason,
            "callsign": row.identity.callsign,
            "country": row.identity.country,
            "canonical": row.identity.canonical_name,
            "epg_icon": epg_icon,
            "manager_hit": manager_hit,
            "default_hit": default_hit,
            "searched": searched,
            "zone_free": crosstalk_matching::country::strip_zone_marker(
                crosstalk_lineup::logos::tv_match_name(&channel.name)),
            "norm_channel": crosstalk_core::logos::normalize_channel_name(
                crosstalk_lineup::logos::tv_match_name(&channel.name)),
            "tv_hit": tv_hit,
            "tv": tv_scored,
        }));
    }
    println!("{}", serde_json::to_string(&dumped)?);
    Ok(())
}
