//! Match a synthetic world from stdin and print the per-channel write lists, for
//! `scripts/match_parity.py`.
//!
//! One JSON object per line, one line back per case. The case carries a whole tiny world — the
//! channels (with their current ordered stream lists), the streams (with the `stream_stats`
//! IPTV Checker writes), the M3U accounts, the channel groups, the channel-database entries the
//! country filter reads, the dead-stream verdicts and the match settings — because grouping,
//! zone routing and `visible_channel_limit` are properties of a channel SET, not of one channel.
//!
//! The harness drives Stream-Mapparr's real `_build_channel_groups`, `_sort_channels_by_priority`,
//! `_match_streams_to_channel`, `_channels_to_update_for_group`, `_streams_to_assign`,
//! `_same_country_ids_for` and `_write_channel_streams` over the same case, so this binary is
//! the only Rust in the comparison.
//!
//! ```text
//! echo '{"channels":[{"id":1,"name":"ESPN","streams":[1]}],"streams":[...]}' \
//!   | cargo run -p crosstalk-streams --example match_io
//! ```

use crosstalk_core::config::{MatchingConfig, ScopeConfig};
use crosstalk_core::dispatcharr::{
    Channel, ChannelGroup, CurrentProgram, EpgEntry, M3uAccount, Stream, World,
};
use crosstalk_streams::epg::{EpgSettings, EpgTitles};
use crosstalk_streams::matching::{self, ChannelDatabase, DatabaseEntry, MatchOptions};
use crosstalk_streams::quality::{MemoryThroughputSource, ThroughputEntry};
use crosstalk_streams::settings::StreamsSettings;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
struct ChannelInput {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    group: Option<i64>,
    #[serde(default)]
    number: Option<f64>,
    /// The channel's current list, in playback order.
    #[serde(default)]
    streams: Vec<i64>,
    /// The EPG record the channel's own column points at (`Channel.epg_data_id`).
    #[serde(default)]
    epg_data_id: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct StreamInput {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    account: Option<i64>,
    #[serde(default)]
    group: Option<i64>,
    #[serde(default)]
    custom: bool,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    stats: Value,
    /// The guide identifier the placeholder lookup joins on (`Stream.tvg_id`).
    #[serde(default)]
    tvg_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct AccountInput {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default = "default_true")]
    active: bool,
}

impl AccountInput {
    fn to_account(&self) -> M3uAccount {
        M3uAccount {
            id: self.id,
            name: self.name.clone(),
            is_active: self.active,
            ..M3uAccount::default()
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
struct GroupInput {
    id: i64,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct DatabaseInput {
    name: String,
    code: String,
    #[serde(default)]
    callsign: Option<String>,
}

/// The match settings of one case, in `streams.toml`'s own names.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct SettingsInput {
    match_sensitivity: String,
    overwrite_streams: bool,
    keep_custom_streams: bool,
    allow_same_name_streams: bool,
    restrict_matching_to_country: bool,
    stream_prefix_countries: String,
    visible_channel_limit: u32,
    ota_only: bool,
    filter_dead_streams: bool,
    ignore_tags: String,
    selected_m3us: String,
    selected_stream_groups: String,
    stream_group_filter_mode: String,
    stream_name_regex_rules: String,
    // The quality model the finalise sort reads.
    prioritize_quality: bool,
    audio_channels_priority: String,
    audio_codec_priority: String,
    bitrate_sort_step_kbps: u32,
    demote_content_starved: bool,
    content_bitrate_floor_kbps: u32,
    enable_throughput_sorting: bool,
    bitrate_safety_margin: f64,
    probe_cache_ttl_minutes: u32,
    // The EPG-aware placeholder block.
    epg_placeholder_matching_enabled: bool,
    epg_placeholder_name_patterns: String,
    epg_title_cleanup_rules: String,
    epg_skip_titles: String,
    epg_channel_schedule_cleanup_rules: String,
    epg_watch_source_streams: String,
    // `[matching]`.
    ignore_quality: bool,
    ignore_regional: bool,
    ignore_geographic: bool,
    ignore_misc: bool,
}

impl Default for SettingsInput {
    fn default() -> Self {
        Self {
            match_sensitivity: "normal".to_string(),
            overwrite_streams: true,
            keep_custom_streams: true,
            allow_same_name_streams: false,
            restrict_matching_to_country: false,
            stream_prefix_countries: String::new(),
            visible_channel_limit: 1,
            ota_only: false,
            filter_dead_streams: false,
            ignore_tags: String::new(),
            selected_m3us: String::new(),
            selected_stream_groups: String::new(),
            stream_group_filter_mode: "include".to_string(),
            stream_name_regex_rules: String::new(),
            prioritize_quality: false,
            audio_channels_priority: String::new(),
            audio_codec_priority: String::new(),
            bitrate_sort_step_kbps: 500,
            demote_content_starved: true,
            content_bitrate_floor_kbps: 300,
            enable_throughput_sorting: true,
            bitrate_safety_margin: 1.10,
            probe_cache_ttl_minutes: 30,
            epg_placeholder_matching_enabled: false,
            epg_placeholder_name_patterns:
                crosstalk_streams::settings::EPG_PLACEHOLDER_NAME_PATTERNS.to_string(),
            epg_title_cleanup_rules: crosstalk_streams::settings::EPG_TITLE_CLEANUP_RULES
                .to_string(),
            epg_skip_titles: crosstalk_streams::settings::EPG_SKIP_TITLES.to_string(),
            epg_channel_schedule_cleanup_rules:
                crosstalk_streams::settings::EPG_CHANNEL_SCHEDULE_CLEANUP_RULES.to_string(),
            epg_watch_source_streams: String::new(),
            ignore_quality: true,
            ignore_regional: true,
            ignore_geographic: true,
            ignore_misc: true,
        }
    }
}

/// One case: a world and the settings to match it with.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct Case {
    settings: SettingsInput,
    channels: Vec<ChannelInput>,
    streams: Vec<StreamInput>,
    accounts: Vec<AccountInput>,
    groups: Vec<GroupInput>,
    /// The channel databases the country/OTA lookups read, as `(channel_name, country code)`.
    channels_data: Vec<DatabaseInput>,
    /// `{"<stream id>": {plugin cache entry}}`, the same shape SM's throughput cache has.
    throughput: HashMap<String, Value>,
    /// Streams a probe verdict found dead (`filter_dead_streams`).
    dead: Vec<i64>,
    /// The world's EPG entries: the `tvg_id` -> record-id join the placeholder path needs.
    #[serde(default)]
    epg_entries: Vec<EpgEntryInput>,
    /// What is airing right now, per EPG record id: `{"7": "WWE SummerSlam"}`. The RAW titles,
    /// so the plugin's own cleanup/skip/date gates decide the rest on both sides.
    #[serde(default)]
    current_programs: HashMap<String, String>,
    /// The local calendar day the "Next Event: … on DATE" gate compares against, `M-D`.
    #[serde(default = "default_today")]
    epg_today: String,
}

fn default_today() -> String {
    "8-5".to_string()
}

#[derive(Debug, Clone, Deserialize)]
struct EpgEntryInput {
    id: i64,
    #[serde(default)]
    tvg_id: String,
}

impl Case {
    /// The local day the EPG date gate compares against.
    fn today(&self) -> (u32, u32) {
        let mut parts = self.epg_today.split('-');
        let month = parts.next().and_then(|value| value.trim().parse().ok());
        let day = parts.next().and_then(|value| value.trim().parse().ok());
        (month.unwrap_or(1), day.unwrap_or(1))
    }

    fn world(&self) -> World {
        World {
            channels: self
                .channels
                .iter()
                .map(|channel| Channel {
                    id: channel.id,
                    name: channel.name.clone(),
                    channel_group_id: channel.group,
                    channel_number: channel
                        .number
                        .map(|number| json!(number))
                        .or(Some(Value::Null)),
                    streams: channel.streams.clone(),
                    epg_data_id: channel.epg_data_id,
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
            m3u_accounts: self.accounts.iter().map(AccountInput::to_account).collect(),
            entries: self
                .epg_entries
                .iter()
                .map(|entry| EpgEntry {
                    id: entry.id,
                    name: String::new(),
                    tvg_id: entry.tvg_id.clone(),
                    epg_source: Some(1),
                })
                .collect(),
            streams: self
                .streams
                .iter()
                .map(|stream| Stream {
                    id: stream.id,
                    name: stream.name.clone(),
                    m3u_account: stream.account,
                    channel_group: stream.group,
                    is_custom: stream.custom,
                    url: stream.url.clone(),
                    stream_stats: stream.stats.clone(),
                    tvg_id: stream.tvg_id.clone(),
                    ..Stream::default()
                })
                .collect(),
            ..World::default()
        }
    }

    fn settings(&self) -> StreamsSettings {
        let input = &self.settings;
        StreamsSettings {
            match_sensitivity: input.match_sensitivity.clone(),
            overwrite_streams: input.overwrite_streams,
            keep_custom_streams: input.keep_custom_streams,
            allow_same_name_streams: input.allow_same_name_streams,
            restrict_matching_to_country: input.restrict_matching_to_country,
            stream_prefix_countries: input.stream_prefix_countries.clone(),
            visible_channel_limit: input.visible_channel_limit,
            ota_only: input.ota_only,
            filter_dead_streams: input.filter_dead_streams,
            ignore_tags: input.ignore_tags.clone(),
            selected_m3us: input.selected_m3us.clone(),
            selected_stream_groups: input.selected_stream_groups.clone(),
            stream_group_filter_mode: input.stream_group_filter_mode.clone(),
            stream_name_regex_rules: input.stream_name_regex_rules.clone(),
            epg_placeholder_matching_enabled: input.epg_placeholder_matching_enabled,
            epg_placeholder_name_patterns: input.epg_placeholder_name_patterns.clone(),
            epg_title_cleanup_rules: input.epg_title_cleanup_rules.clone(),
            epg_skip_titles: input.epg_skip_titles.clone(),
            epg_channel_schedule_cleanup_rules: input.epg_channel_schedule_cleanup_rules.clone(),
            epg_watch_source_streams: input.epg_watch_source_streams.clone(),
            prioritize_quality: input.prioritize_quality,
            audio_channels_priority: input.audio_channels_priority.clone(),
            audio_codec_priority: input.audio_codec_priority.clone(),
            bitrate_sort_step_kbps: input.bitrate_sort_step_kbps,
            demote_content_starved: input.demote_content_starved,
            content_bitrate_floor_kbps: input.content_bitrate_floor_kbps,
            enable_throughput_sorting: input.enable_throughput_sorting,
            bitrate_safety_margin: input.bitrate_safety_margin,
            probe_cache_ttl_minutes: input.probe_cache_ttl_minutes,
            ..StreamsSettings::default()
        }
    }

    fn matching(&self) -> MatchingConfig {
        let defaults = MatchingConfig::default();
        MatchingConfig {
            ignore_quality_tags: self.settings.ignore_quality,
            ignore_regional_tags: self.settings.ignore_regional,
            ignore_geographic_tags: self.settings.ignore_geographic,
            ignore_misc_tags: self.settings.ignore_misc,
            ..defaults
        }
    }

    fn database(&self) -> ChannelDatabase {
        ChannelDatabase::from_entries(self.channels_data.iter().map(|entry| DatabaseEntry {
            name: entry.name.clone(),
            code: entry.code.clone(),
            callsign: entry.callsign.clone(),
        }))
    }
}

fn main() -> anyhow::Result<()> {
    // A panic here would be a Rust bug, so let it fail loudly: the harness reports it.
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
    let mut options = MatchOptions::with_database(&settings, &matching, Some(case.database()));
    options.dead_streams = case.dead.iter().copied().collect();
    if settings.epg_placeholder_matching_enabled {
        let mut titles = EpgTitles::prepare(
            EpgSettings::from_settings(&settings),
            &world.entries,
            case.today(),
        );
        let mut programmes: Vec<CurrentProgram> = case
            .current_programs
            .iter()
            .map(|(id, title)| CurrentProgram {
                epg_data_id: id.trim().parse().unwrap_or_default(),
                title: title.clone(),
                ..CurrentProgram::default()
            })
            .collect();
        programmes.sort_by_key(|programme| programme.epg_data_id);
        titles.set_programmes(&programmes);
        options = options.with_epg(std::sync::Arc::new(titles));
    }

    let mut throughput = MemoryThroughputSource::new();
    for (key, entry) in &case.throughput {
        let id: i64 = key.trim().parse()?;
        throughput.insert(id, ThroughputEntry::from_cache_json(entry));
    }

    let outcome = matching::plan(
        &world,
        &ScopeConfig::default(),
        &settings,
        &options,
        &throughput,
    )?;

    let mut rows = Vec::new();
    for assignment in &outcome.assignments {
        rows.push(json!({
            "channel_id": assignment.channel_id,
            "before": assignment.before,
            "after": assignment.after,
            "matched": assignment.matched,
            "keep_existing": assignment.keep_existing,
            "updated": assignment.updated,
            "will_write": assignment.will_write(),
            "reason": assignment.reason,
            "zone": assignment.zone.map(|zone| zone.label()),
        }));
    }
    Ok(json!({
        "assignments": rows,
        "epg": outcome.epg.map(|epg| json!({
            "checked": epg.checked,
            "resolved": epg.resolved,
            "skipped_long": epg.skipped_long,
            "watch_hits": epg.watch_hits,
        })),
        "channels": outcome.channels,
        "groups": outcome.groups,
        "country": {
            "engaged": outcome.country.engaged,
            "skipped_unknown_channel": outcome.country.skipped_unknown_channel,
            "foreign_dropped": outcome.country.foreign_dropped,
            "unknown_kept": outcome.country.unknown_kept,
        },
        "dead_filtered": outcome.dead_filtered,
        "ota": outcome.ota.map(|ota| json!({
            "no_callsign": ota.no_callsign,
            "not_in_database": ota.not_in_database,
            "no_streams": ota.no_streams,
        })),
    }))
}
