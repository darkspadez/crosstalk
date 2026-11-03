//! Sort a synthetic world from stdin and print the orders, for `scripts/sort_parity.py`.
//!
//! One JSON object per line, one line back per case. The case carries a whole tiny world — the
//! channels (with their current ordered stream lists), the streams (with the `stream_stats`
//! IPTV Checker writes), the M3U accounts, the throughput cache and the sort settings — because
//! zone routing is a property of the channel GROUP, not of one channel.
//!
//! The harness drives the plugin's real `sort_streams_action` inner loop over the same case, so
//! this binary is the only Rust in the comparison: it maps the case onto [`World`] and
//! `streams.toml`'s settings and prints what [`crosstalk_streams::sort::orders`] decided.
//!
//! ```text
//! echo '{"channels":[{"id":1,"name":"ESPN","streams":[1,2]}],"streams":[...]}' \
//!   | cargo run -p crosstalk-streams --example sort_io
//! ```

use crosstalk_core::config::MatchingConfig;
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, M3uAccount, Stream, World};
use crosstalk_streams::quality::{MemoryThroughputSource, ThroughputEntry};
use crosstalk_streams::settings::StreamsSettings;
use crosstalk_streams::sort::{self, SortOptions};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;

/// A channel of the fixture world.
#[derive(Debug, Clone, Deserialize)]
struct ChannelInput {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    group: Option<i64>,
    /// The channel's current list, in playback order.
    #[serde(default)]
    streams: Vec<i64>,
}

/// A stream of the fixture world.
#[derive(Debug, Clone, Deserialize)]
struct StreamInput {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    account: Option<i64>,
    #[serde(default)]
    custom: bool,
    #[serde(default)]
    stats: Value,
}

#[derive(Debug, Clone, Deserialize)]
struct AccountInput {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default = "default_true")]
    active: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct GroupInput {
    id: i64,
    #[serde(default)]
    name: String,
}

fn default_true() -> bool {
    true
}

/// The sort settings of one case, in `streams.toml`'s own names.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct SettingsInput {
    prioritize_quality: bool,
    audio_channels_priority: String,
    audio_codec_priority: String,
    bitrate_sort_step_kbps: Option<u32>,
    demote_content_starved: Option<bool>,
    content_bitrate_floor_kbps: Option<u32>,
    enable_throughput_sorting: Option<bool>,
    bitrate_safety_margin: Option<f64>,
    probe_cache_ttl_minutes: Option<u32>,
    keep_custom_streams: Option<bool>,
    selected_m3us: String,
    ignore_tags: String,
    // `[matching]`: what zone routing normalises with.
    ignore_quality: Option<bool>,
    ignore_regional: Option<bool>,
    ignore_geographic: Option<bool>,
    ignore_misc: Option<bool>,
}

/// One case: a world and the settings to sort it with.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct Case {
    settings: SettingsInput,
    channels: Vec<ChannelInput>,
    streams: Vec<StreamInput>,
    accounts: Vec<AccountInput>,
    groups: Vec<GroupInput>,
    /// `{"<stream id>": {plugin cache entry}}`, the same shape SM's JSON cache has.
    throughput: HashMap<String, Value>,
}

impl Case {
    fn world(&self) -> World {
        World {
            channels: self
                .channels
                .iter()
                .map(|channel| Channel {
                    id: channel.id,
                    name: channel.name.clone(),
                    channel_group_id: channel.group,
                    streams: channel.streams.clone(),
                    ..Channel::default()
                })
                .collect(),
            groups: self
                .groups
                .iter()
                .map(|group| ChannelGroup {
                    id: group.id,
                    name: group.name.clone(),
                })
                .collect(),
            m3u_accounts: self
                .accounts
                .iter()
                .map(|account| M3uAccount {
                    id: account.id,
                    name: account.name.clone(),
                    is_active: account.active,
                    ..M3uAccount::default()
                })
                .collect(),
            streams: self
                .streams
                .iter()
                .map(|stream| Stream {
                    id: stream.id,
                    name: stream.name.clone(),
                    m3u_account: stream.account,
                    is_custom: stream.custom,
                    stream_stats: stream.stats.clone(),
                    ..Stream::default()
                })
                .collect(),
            ..World::default()
        }
    }

    fn settings(&self) -> StreamsSettings {
        let mut settings = StreamsSettings::default();
        let input = &self.settings;
        settings.prioritize_quality = input.prioritize_quality;
        settings.audio_channels_priority = input.audio_channels_priority.clone();
        settings.audio_codec_priority = input.audio_codec_priority.clone();
        if let Some(step) = input.bitrate_sort_step_kbps {
            settings.bitrate_sort_step_kbps = step;
        }
        if let Some(demote) = input.demote_content_starved {
            settings.demote_content_starved = demote;
        }
        if let Some(floor) = input.content_bitrate_floor_kbps {
            settings.content_bitrate_floor_kbps = floor;
        }
        if let Some(enabled) = input.enable_throughput_sorting {
            settings.enable_throughput_sorting = enabled;
        }
        if let Some(margin) = input.bitrate_safety_margin {
            settings.bitrate_safety_margin = margin;
        }
        if let Some(ttl) = input.probe_cache_ttl_minutes {
            settings.probe_cache_ttl_minutes = ttl;
        }
        if let Some(keep) = input.keep_custom_streams {
            settings.keep_custom_streams = keep;
        }
        settings.selected_m3us = input.selected_m3us.clone();
        settings.ignore_tags = input.ignore_tags.clone();
        settings
    }

    fn matching(&self) -> MatchingConfig {
        let defaults = MatchingConfig::default();
        MatchingConfig {
            ignore_quality_tags: self
                .settings
                .ignore_quality
                .unwrap_or(defaults.ignore_quality_tags),
            ignore_regional_tags: self
                .settings
                .ignore_regional
                .unwrap_or(defaults.ignore_regional_tags),
            ignore_geographic_tags: self
                .settings
                .ignore_geographic
                .unwrap_or(defaults.ignore_geographic_tags),
            ignore_misc_tags: self
                .settings
                .ignore_misc
                .unwrap_or(defaults.ignore_misc_tags),
            ..defaults
        }
    }
}

fn main() -> anyhow::Result<()> {
    // The harness compares the plugin's answer with ours for every case it sends; a panic here
    // would be a Rust bug, so let it fail loudly.
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut line = String::new();
    while std::io::BufRead::read_line(&mut reader, &mut line)? > 0 {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let case: Case = serde_json::from_str(trimmed)?;
        let answer = run_case(&case)?;
        println!("{}", serde_json::to_string(&answer)?);
        line.clear();
    }
    Ok(())
}

fn run_case(case: &Case) -> anyhow::Result<Value> {
    let world = case.world();
    let settings = case.settings();
    let matching = case.matching();
    let options = SortOptions::new(&settings, &world, &matching);

    let mut throughput = MemoryThroughputSource::new();
    for (key, entry) in &case.throughput {
        let id: i64 = key.trim().parse()?;
        throughput.insert(id, ThroughputEntry::from_cache_json(entry));
    }

    let result = sort::orders(
        &world,
        &crosstalk_core::config::ScopeConfig::default(),
        &options,
        &throughput,
    )?;

    let mut orders = serde_json::Map::new();
    let mut changed = Vec::new();
    for order in &result.orders {
        orders.insert(
            order.channel_id.to_string(),
            json!({
                "before": order.before,
                "after": order.after,
                "changed": order.changed(),
                "dropped_by_zone": order.dropped_by_zone,
            }),
        );
        if order.changed() {
            changed.push(order.channel_id);
        }
    }
    Ok(json!({
        "orders": orders,
        "changed": changed,
        "candidates": result.candidates,
        "missing_streams": result.missing_streams,
        "kept_other_zone": result.kept_other_zone,
    }))
}
