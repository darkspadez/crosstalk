//! `streams` settings: the values, and the form schema the UI renders.
//!
//! The field ids, labels and help text follow the two plugins this app ports — Stream-Mapparr
//! (`plugin.py`'s `Plugin.fields`, which is the single source of truth there; its
//! `plugin.json` ships an empty `fields` array on purpose) and IPTV Checker
//! (`iptv_checker/plugin.json`) — because operators already know those names.
//!
//! Only what the streams app decides for itself lives here. Everything shared with the other
//! sub-apps stays in `crosstalk.toml` and is NOT duplicated:
//!
//!   * `[scope]` — the channel half of the scope. Stream-Mapparr's `profile_name` and
//!     `selected_groups` are `[scope].profiles` and `[scope].groups`, and its
//!     `group_filter_mode = "exclude"` is `[scope].ignore_groups` (the host models the two
//!     modes as two lists instead of a list plus a switch). IPTV Checker's `channel_groups` /
//!     `channel_groups_mode` are the same two settings.
//!   * `[matching]` — the tag handling. Stream-Mapparr's `tag_handling` presets *are* the four
//!     `ignore_*_tags` booleans, and its `custom_aliases` is `[matching].custom_aliases`; its
//!     `channel_database` is `[matching].enabled_databases`.
//!   * `[exports]` — the CSV retention both plugins grew their own copy of.
//!   * scheduling — a schedule in `crosstalk.toml` replaces both plugins' `scheduled_*`,
//!     window and post-run toggles, and the host's webhook replaces Stream-Mapparr's
//!     `on_m3u_refresh` / `on_iptv_checker_scan`.
//!
//! Stream-Mapparr's `rate_limiting` (four preset delays between database writes) is not ported
//! as a preset: crosstalk writes stream lists over REST with bounded concurrency, so the
//! equivalent knob is `write_concurrency` here.

use anyhow::{bail, Context, Result};
use crosstalk_core::subapp::{
    whole_number, FieldKind, FieldSpec, FieldValue, SettingsSchema, SettingsValues,
};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Stream-Mapparr's `SENSITIVITY_MAP`: the preset name the operator picks, and the fuzzy score
/// it stands for (plugin.py, `PluginConfig.SENSITIVITY_MAP`).
pub const SENSITIVITY: [(&str, u8); 4] = [
    ("relaxed", 70),
    ("normal", 80),
    ("strict", 90),
    ("exact", 95),
];

/// The preset a missing or unrecognized `match_sensitivity` resolves to.
///
/// `normal` rather than Stream-Mapparr's own fallback: the plugin's
/// `_get_fuzzy_match_threshold` falls back to its `fuzzy_match_threshold` default of 85 when
/// the preset is unusable, which is not one of the four presets. A form that renders `normal`
/// and a run that scores at 85 would be two different answers, so an unusable preset is
/// rejected by [`validate`] and a run refuses it; this is only what a scan of the map yields
/// when someone asks without checking.
pub const DEFAULT_SENSITIVITY: &str = "normal";

/// The two group-filter modes, as Stream-Mapparr spells them (`GROUP_FILTER_MODES`).
pub const FILTER_MODES: [&str; 2] = ["include", "exclude"];
pub const DEFAULT_FILTER_MODE: &str = "include";

/// Every whole-number setting, with the bounds the form and a hand-edited file are both held
/// to: `(id, min, max)`.
///
/// The plugin floors nothing (`visible_channel_limit` of 0 simply enables no channel, and a
/// negative timeout is a crash waiting to happen), so these are crosstalk's: a bound tight
/// enough to catch a typo, loose enough that every sane installation is inside it. The test
/// `every_bound_names_a_real_setting` keeps this table and the schema in step.
const NUMBER_BOUNDS: &[(&str, u32, u32)] = &[
    // Match
    ("visible_channel_limit", 1, 1_000),
    // Sort
    ("bitrate_sort_step_kbps", 0, 100_000),
    ("content_bitrate_floor_kbps", 0, 100_000),
    // Crosstalk-only: the plugin's `rate_limiting` presets are replaced by a real number.
    ("write_concurrency", 1, 64),
    // Probe
    ("probe_freshness_hours", 0, 8_760),
    ("timeout", 1, 300),
    ("probe_timeout", 1, 300),
    ("dead_connection_retries", 0, 10),
    ("probe_per_account_limit", 1, 32),
    ("stream_check_delay", 0, 600),
    ("probe_rate_per_minute", 1, 600),
    ("probe_duration_seconds", 1, 120),
    ("probe_cache_ttl_minutes", 0, 10_080),
    ("black_screen_sample_seconds", 1, 60),
    ("black_screen_min_black_seconds", 0, 60),
    ("black_screen_ffmpeg_timeout", 1, 300),
    ("frozen_video_min_seconds", 1, 60),
];

/// Silent-audio threshold bounds. Negative by definition: dBFS silence is below zero.
const SILENT_AUDIO_MAX_DB_BOUNDS: (i32, i32) = (-100, -1);
/// `bitrate_safety_margin` bounds: under 1.0 every stream is "insufficient".
const BITRATE_MARGIN_BOUNDS: (f64, f64) = (1.0, 10.0);

/// The bounds for one whole-number setting.
fn bounds(id: &str) -> (u32, u32) {
    NUMBER_BOUNDS
        .iter()
        .find(|(field, _, _)| *field == id)
        .map(|(_, min, max)| (*min, *max))
        .unwrap_or_else(|| panic!("{id} declares no bounds"))
}

/// A signed whole number in range, for the one probe threshold that is negative.
fn whole_i32(field: &str, value: f64, min: i32, max: i32) -> Result<i32> {
    if !value.is_finite() || value.fract() != 0.0 {
        bail!("{field} must be a whole number, got {value}");
    }
    if value < f64::from(min) || value > f64::from(max) {
        bail!("{field} must be between {min} and {max}, got {value}");
    }
    Ok(value as i32)
}

/// Everything `streams.toml` holds.
///
/// The `Default` impl is written out rather than derived: `#[serde(default)]` fills missing
/// fields from it, so a derived one would silently default every threshold to 0 — a
/// valid-looking configuration that rejects or demotes everything.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct StreamsSettings {
    // --- Stream selection ---------------------------------------------------------------
    /// M3U account names, in priority order. Empty means every active account.
    /// Stream-Mapparr's `selected_m3us`.
    pub selected_m3us: String,
    /// Stream group names to draw candidates from. Empty means every stream group.
    /// Stream-Mapparr's `selected_stream_groups`.
    pub selected_stream_groups: String,
    /// `include` or `exclude`, resolved separately from the channel-group mode.
    /// Stream-Mapparr's `stream_group_filter_mode`.
    pub stream_group_filter_mode: String,
    /// A JSON list of `[find, replace]` regex pairs applied to stream names before matching.
    /// Stream-Mapparr's `stream_name_regex_rules`.
    pub stream_name_regex_rules: String,
    /// Extra tags to strip while matching, with quote support for tags that carry spaces.
    /// Stream-Mapparr's `ignore_tags`.
    pub ignore_tags: String,

    // --- Match --------------------------------------------------------------------------
    /// `relaxed` / `normal` / `strict` / `exact`, which is 70 / 80 / 90 / 95.
    /// Stream-Mapparr's `match_sensitivity`.
    pub match_sensitivity: String,
    /// Replace a channel's stream list instead of appending to it.
    /// Stream-Mapparr's `overwrite_streams`.
    pub overwrite_streams: bool,
    /// Keep a channel's hand-made streams, after the provider ones. `keep_custom_streams`.
    pub keep_custom_streams: bool,
    /// Keep distinct same-named streams from ONE account. `allow_same_name_streams`.
    pub allow_same_name_streams: bool,
    /// Only match streams detected as the channel's country. `restrict_matching_to_country`.
    pub restrict_matching_to_country: bool,
    /// `PREFIX=COUNTRY` pairs telling the country filter what a provider prefix means.
    /// Stream-Mapparr's `stream_prefix_countries`.
    pub stream_prefix_countries: String,
    /// How many channels of one name group receive streams. `visible_channel_limit`.
    pub visible_channel_limit: u32,
    /// Resolve placeholder names through the currently airing programme.
    /// Stream-Mapparr's `epg_placeholder_matching_enabled`.
    pub epg_placeholder_matching_enabled: bool,
    /// One regex per line; only a name matching one of these is treated as a placeholder.
    /// Stream-Mapparr's `epg_placeholder_name_patterns`.
    pub epg_placeholder_name_patterns: String,
    /// `[find, replace]` pairs applied to the raw programme title. `epg_title_cleanup_rules`.
    pub epg_title_cleanup_rules: String,
    /// Programme titles that carry no event signal, comma-separated. `epg_skip_titles`.
    pub epg_skip_titles: String,
    /// `[find, replace]` pairs applied to the CHANNEL name. `epg_channel_schedule_cleanup_rules`.
    pub epg_channel_schedule_cleanup_rules: String,
    /// Permanently-named streams whose current programme may add them as an alternate.
    /// Stream-Mapparr's `epg_watch_source_streams`.
    pub epg_watch_source_streams: String,

    // --- Sort ---------------------------------------------------------------------------
    /// Quality before source priority, rather than the reverse. `prioritize_quality`.
    pub prioritize_quality: bool,
    /// Audio layouts, most preferred first. Stream-Mapparr's `audio_channels_priority`.
    pub audio_channels_priority: String,
    /// Audio codecs, most preferred first. Stream-Mapparr's `audio_codec_priority`.
    pub audio_codec_priority: String,
    /// Round measured bitrates down to this step before comparing. `bitrate_sort_step_kbps`.
    pub bitrate_sort_step_kbps: u32,
    /// Rank a stream last when it claims 720p+ but carries almost no video.
    /// Stream-Mapparr's `demote_content_starved`.
    pub demote_content_starved: bool,
    /// The content bitrate below which such a stream counts as a placeholder.
    /// Stream-Mapparr's `content_bitrate_floor_kbps`.
    pub content_bitrate_floor_kbps: u32,
    /// Sort by measured throughput tier before resolution/FPS. `enable_throughput_sorting`.
    pub enable_throughput_sorting: bool,
    /// How many stream-list writes are in flight at once.
    ///
    /// Crosstalk-only: Stream-Mapparr's `rate_limiting` preset paced one ORM write at a time,
    /// which is what made it too slow at this scale. Stream-list writes go over REST in bulk
    /// chunks, so the knob that matters is how many requests may overlap.
    pub write_concurrency: u32,

    // --- Probe --------------------------------------------------------------------------
    /// Check a channel's backup streams as well as its first one. IPTV Checker's
    /// `check_alternative_streams`.
    pub check_alternative_streams: bool,
    /// Only check channels enabled in at least one profile. IPTV Checker's
    /// `only_visible_channels`.
    pub only_visible_channels: bool,
    /// Skip a stream probed within this many hours; 0 probes everything every run.
    ///
    /// Crosstalk-only: the plugin kept a JSON progress file per window and probed whatever it
    /// had not reached, so there was no notion of freshness. Hours, not minutes, because the
    /// window that matters is "since the last nightly run".
    pub probe_freshness_hours: u32,
    /// Connection timeout. IPTV Checker's `timeout`.
    pub timeout: u32,
    /// How long to wait for data after connecting. IPTV Checker's `probe_timeout`.
    pub probe_timeout: u32,
    /// Retries when a stream looks dead. IPTV Checker's `dead_connection_retries`.
    pub dead_connection_retries: u32,
    /// Concurrent connections per M3U account, never above the account's `max_streams`.
    ///
    /// Crosstalk-only, and it replaces IPTV Checker's global `parallel_workers` (2): a
    /// per-account cap is what a provider's connection limit is actually keyed on, and total
    /// parallelism then grows with the number of accounts instead of being fixed.
    pub probe_per_account_limit: u32,
    /// Cooldown after each probe. IPTV Checker's `stream_check_delay`.
    pub stream_check_delay: u32,
    /// Cap on probes started per minute in one run. Stream-Mapparr's `probe_rate_per_minute`.
    pub probe_rate_per_minute: u32,
    /// Length of one throughput probe. Stream-Mapparr's `probe_duration_seconds`.
    pub probe_duration_seconds: u32,
    /// How long a measured throughput stays usable for sorting.
    /// Stream-Mapparr's `probe_cache_ttl_minutes`.
    pub probe_cache_ttl_minutes: u32,
    /// The multiplier over a stream's nominal bitrate that counts as healthy.
    /// Stream-Mapparr's `bitrate_safety_margin`.
    pub bitrate_safety_margin: f64,
    /// IPTV Checker's `black_screen_detection`.
    pub black_screen_detection: bool,
    /// IPTV Checker's `black_screen_sample_seconds`.
    pub black_screen_sample_seconds: u32,
    /// IPTV Checker's `black_screen_min_black_seconds`.
    pub black_screen_min_black_seconds: u32,
    /// IPTV Checker's `black_screen_ffmpeg_timeout`.
    pub black_screen_ffmpeg_timeout: u32,
    /// IPTV Checker's `placeholder_file_detection`.
    pub placeholder_file_detection: bool,
    /// IPTV Checker's `frozen_video_detection`.
    pub frozen_video_detection: bool,
    /// IPTV Checker's `frozen_video_min_seconds`.
    pub frozen_video_min_seconds: u32,
    /// IPTV Checker's `silent_audio_detection`.
    pub silent_audio_detection: bool,
    /// IPTV Checker's `silent_audio_max_db`: mean volume at or below this counts as silence.
    pub silent_audio_max_db: i32,
    /// Hosts ffprobe cannot validate (Streamlink). IPTV Checker's `streamlink_hosts`.
    pub streamlink_hosts: String,
    /// IPTV Checker's `ffprobe_path`.
    pub ffprobe_path: String,
    /// IPTV Checker's `ffmpeg_path`.
    pub ffmpeg_path: String,

    // --- Triage -------------------------------------------------------------------------
    /// Rename format for a channel every one of whose streams failed. IPTV Checker's
    /// `dead_rename_format`.
    pub dead_rename_format: String,
    /// Group dead channels are moved to. IPTV Checker's `move_to_group_name`.
    pub move_to_group_name: String,
    /// IPTV Checker's `black_screen_rename_format`.
    pub black_screen_rename_format: String,
    /// IPTV Checker's `move_black_screen_group`.
    pub move_black_screen_group: String,
    /// IPTV Checker's `low_framerate_rename_format`.
    pub low_framerate_rename_format: String,
    /// IPTV Checker's `move_low_framerate_group`.
    pub move_low_framerate_group: String,
    /// Formats to append as a suffix, comma-separated. IPTV Checker's `video_format_suffixes`.
    pub video_format_suffixes: String,
}

impl Default for StreamsSettings {
    fn default() -> Self {
        Self {
            // Stream-Mapparr's shipped defaults.
            selected_m3us: String::new(),
            selected_stream_groups: String::new(),
            stream_group_filter_mode: DEFAULT_FILTER_MODE.to_string(),
            stream_name_regex_rules: String::new(),
            ignore_tags: String::new(),

            match_sensitivity: DEFAULT_SENSITIVITY.to_string(),
            overwrite_streams: true,
            keep_custom_streams: true,
            allow_same_name_streams: false,
            restrict_matching_to_country: false,
            stream_prefix_countries: String::new(),
            visible_channel_limit: 1,
            epg_placeholder_matching_enabled: false,
            epg_placeholder_name_patterns: EPG_PLACEHOLDER_NAME_PATTERNS.to_string(),
            epg_title_cleanup_rules: EPG_TITLE_CLEANUP_RULES.to_string(),
            epg_skip_titles: EPG_SKIP_TITLES.to_string(),
            epg_channel_schedule_cleanup_rules: EPG_CHANNEL_SCHEDULE_CLEANUP_RULES.to_string(),
            epg_watch_source_streams: String::new(),

            prioritize_quality: false,
            audio_channels_priority: String::new(),
            audio_codec_priority: String::new(),
            bitrate_sort_step_kbps: 500,
            demote_content_starved: true,
            content_bitrate_floor_kbps: 300,
            enable_throughput_sorting: true,
            write_concurrency: 8,

            check_alternative_streams: true,
            only_visible_channels: false,
            probe_freshness_hours: 24,
            timeout: 10,
            probe_timeout: 20,
            dead_connection_retries: 3,
            probe_per_account_limit: 1,
            stream_check_delay: 2,
            probe_rate_per_minute: 6,
            probe_duration_seconds: 8,
            probe_cache_ttl_minutes: 30,
            bitrate_safety_margin: 1.10,
            black_screen_detection: false,
            black_screen_sample_seconds: 6,
            black_screen_min_black_seconds: 3,
            black_screen_ffmpeg_timeout: 20,
            placeholder_file_detection: false,
            frozen_video_detection: false,
            frozen_video_min_seconds: 4,
            silent_audio_detection: false,
            silent_audio_max_db: -70,
            streamlink_hosts: "youtube.com, youtu.be, twitch.tv, kick.com".to_string(),
            ffprobe_path: "/usr/local/bin/ffprobe".to_string(),
            ffmpeg_path: "/usr/local/bin/ffmpeg".to_string(),

            dead_rename_format: "{name} [DEAD]".to_string(),
            move_to_group_name: "Graveyard".to_string(),
            black_screen_rename_format: "{name} [Blank]".to_string(),
            move_black_screen_group: "Black Screens".to_string(),
            low_framerate_rename_format: "{name} [Slow]".to_string(),
            move_low_framerate_group: "Slow".to_string(),
            video_format_suffixes: "UHD, FHD, HD, SD, Unknown".to_string(),
        }
    }
}

/// Stream-Mapparr's `DEFAULT_EPG_PLACEHOLDER_NAME_PATTERNS`, line for line.
pub const EPG_PLACEHOLDER_NAME_PATTERNS: &str = "^PPV EVENT \\d+$\n\
^LIVE EVENT \\d+$\n\
^PPV \\d+ \\|?$\n\
^HBO \\d+$\n\
^MAX \\d+$\n\
^Triller TV \\| Event \\d+$";

/// Stream-Mapparr's `DEFAULT_EPG_TITLE_CLEANUP_RULES` (a `[find, replace]` JSON list).
pub const EPG_TITLE_CLEANUP_RULES: &str =
    r#"[["^Next Event:\\s*", ""], ["\\s+at \\d{1,2}:\\d{2}\\s*[AP]M on \\w+ \\d{1,2}$", ""]]"#;

/// Stream-Mapparr's `DEFAULT_EPG_SKIP_TITLES`.
pub const EPG_SKIP_TITLES: &str = "Signing Off,No Event Today,No Game Today";

/// Stream-Mapparr's `DEFAULT_EPG_CHANNEL_SCHEDULE_CLEANUP_RULES`.
pub const EPG_CHANNEL_SCHEDULE_CLEANUP_RULES: &str =
    r#"[["(?i)\\s*\\|\\s*\\w+\\s*@\\s*\\d{1,2}(?::\\d{2})?\\s*[AP]?M?\\s*$", ""]]"#;

impl StreamsSettings {
    /// The fuzzy score `match_sensitivity` stands for.
    pub fn match_threshold(&self) -> u8 {
        threshold_for(&self.match_sensitivity)
    }

    /// The parsed `ignore_tags` list, quotes removed.
    pub fn ignore_tag_list(&self) -> Vec<String> {
        crate::selection::parse_ignore_tags(&self.ignore_tags)
    }

    /// The parsed `streamlink_hosts` list, lowercased.
    pub fn streamlink_host_list(&self) -> Vec<String> {
        crosstalk_core::wildcard::parse_tokens(&self.streamlink_hosts)
            .into_iter()
            .map(|host| host.to_lowercase())
            .collect()
    }
}

/// The threshold one preset name stands for, or `normal`'s when the name is not a preset.
pub fn threshold_for(sensitivity: &str) -> u8 {
    let wanted = sensitivity.trim().to_lowercase();
    SENSITIVITY
        .iter()
        .find(|(name, _)| *name == wanted)
        .map(|(_, threshold)| *threshold)
        .unwrap_or_else(|| {
            SENSITIVITY
                .iter()
                .find(|(name, _)| *name == DEFAULT_SENSITIVITY)
                .map(|(_, threshold)| *threshold)
                .expect("the default preset exists")
        })
}

/// Settings that are no longer read from this file, and where each went.
///
/// `deny_unknown_fields` already rejects them; this exists so the rejection says where the
/// value went instead of only that the key is unknown. A clean break is deliberate: silently
/// accepting the old keys would leave two files (or a dead key) claiming one setting.
const MOVED: [(&str, &str); 46] = [
    ("profile_name", "[scope] profiles in crosstalk.toml"),
    ("selected_groups", "[scope] groups in crosstalk.toml"),
    (
        "group_filter_mode",
        "[scope] ignore_groups in crosstalk.toml (Stream-Mapparr's exclude mode is the host's \
         Ignore Groups)",
    ),
    ("channel_groups", "[scope] groups in crosstalk.toml"),
    (
        "channel_groups_mode",
        "[scope] ignore_groups in crosstalk.toml",
    ),
    (
        "tag_handling",
        "[matching] ignore_quality_tags / ignore_regional_tags / ignore_geographic_tags / \
         ignore_misc_tags in crosstalk.toml",
    ),
    (
        "custom_aliases",
        "[matching] custom_aliases in crosstalk.toml",
    ),
    (
        "channel_database",
        "[matching] enabled_databases in crosstalk.toml",
    ),
    (
        "csv_export_retention_days",
        "[exports] retention_days in crosstalk.toml",
    ),
    (
        "csv_retention_days",
        "[exports] retention_days in crosstalk.toml",
    ),
    (
        "dry_run_mode",
        "the preview half of each action pair, or --dry-run",
    ),
    ("rate_limiting", "write_concurrency in this file"),
    ("parallel_workers", "probe_per_account_limit in this file"),
    (
        "enable_parallel_checking",
        "probe_per_account_limit in this file",
    ),
    (
        "scheduled_times",
        "[schedules] in crosstalk.toml, e.g. \"streams.match_apply\" = \"0 6 * * *\"",
    ),
    (
        "schedule_window_enabled",
        "[schedules] windows in crosstalk.toml",
    ),
    (
        "auto_match_on_m3u_refresh",
        "the [hooks] m3u_refresh list in crosstalk.toml",
    ),
    (
        "run_after_iptv_checker_scan",
        "a workflow in crosstalk.toml (streams.probe then streams.sort_apply)",
    ),
    (
        "wait_for_iptv_checker",
        "a workflow in crosstalk.toml: steps run in order, so nothing has to wait",
    ),
    (
        "filter_dead_streams",
        "streams.probe's verdicts, which match reads",
    ),
    (
        "notify_enabled",
        "not ported: crosstalk has no notification service",
    ),
    (
        "notify_report_on",
        "not ported: crosstalk has no notification service",
    ),
    (
        "notify_report_format",
        "not ported: crosstalk has no notification service",
    ),
    (
        "iptv_checker_max_wait_hours",
        "a workflow in crosstalk.toml: steps run in order, so nothing has to wait",
    ),
    (
        "scheduled_match_streams",
        "[schedules] in crosstalk.toml, e.g. \"streams.match_apply\" = \"0 6 * * *\"",
    ),
    (
        "scheduled_sort_streams",
        "[schedules] in crosstalk.toml, e.g. \"streams.sort_apply\" = \"0 6 * * *\"",
    ),
    (
        "scheduled_fill_mirror_backups",
        "[schedules] in crosstalk.toml, e.g. \"streams.mirror_apply\" = \"0 6 * * *\"",
    ),
    (
        "enable_scheduled_csv_export",
        "[exports] in crosstalk.toml; every report is written, retention is shared",
    ),
    ("schedule_end_mode", "[schedules] windows in crosstalk.toml"),
    (
        "schedule_duration_hours",
        "[schedules] windows in crosstalk.toml",
    ),
    ("schedule_end_time", "[schedules] windows in crosstalk.toml"),
    (
        "scheduler_email_report",
        "not ported: crosstalk has no notification service",
    ),
    (
        "scheduler_trigger_stream_mapparr",
        "a workflow in crosstalk.toml (streams.probe then streams.sort_apply)",
    ),
    (
        "scheduler_restore_channels",
        "a workflow in crosstalk.toml, or streams.restore_apply on its own schedule",
    ),
    (
        "scheduler_rename_dead_channels",
        "streams.triage_apply, on a [schedules] entry",
    ),
    (
        "scheduler_rename_black_screen_channels",
        "streams.triage_apply, on a [schedules] entry",
    ),
    (
        "scheduler_rename_low_framerate_channels",
        "streams.triage_apply, on a [schedules] entry",
    ),
    (
        "scheduler_add_video_format_suffix",
        "streams.triage_apply, on a [schedules] entry",
    ),
    (
        "scheduler_move_dead_channels",
        "streams.triage_apply, on a [schedules] entry",
    ),
    (
        "scheduler_move_black_screen_channels",
        "streams.triage_apply, on a [schedules] entry",
    ),
    (
        "scheduler_move_low_framerate_channels",
        "streams.triage_apply, on a [schedules] entry",
    ),
    (
        "scheduler_delete_dead_channels",
        "not ported: deletes are out of scope",
    ),
    (
        "auto_delete_confirmation",
        "not ported: deletes are out of scope and never automatic",
    ),
    (
        "ffprobe_flags",
        "fixed probe flags in this build: show_streams, show_packets, an error-level log",
    ),
    (
        "ffprobe_analysis_duration",
        "fixed probe flags in this build: 8 s of analysis before the 30-packet floor",
    ),
    (
        "share_usage_counts",
        "not ported: no telemetry leaves the host",
    ),
];

/// A hint naming the new home for every retired setting the file still mentions.
fn moved_settings_hint(text: &str) -> Option<String> {
    let mut moved: Vec<String> = Vec::new();
    for (old, new) in MOVED {
        let names_it = text.lines().map(str::trim).any(|line| {
            !line.starts_with('#')
                && line.starts_with(old)
                && line[old.len()..].trim_start().starts_with('=')
        });
        if names_it {
            moved.push(format!("{old} -> {new}"));
        }
    }
    if moved.is_empty() {
        None
    } else {
        Some(format!(
            "settings this file no longer holds: {}",
            moved.join("; ")
        ))
    }
}

/// Section ids, so a field never points at a section that does not exist.
pub const SECTION_SELECTION: &str = "_section_stream_selection";
pub const SECTION_MATCH: &str = "_section_match";
pub const SECTION_EPG: &str = "_section_epg_placeholder";
pub const SECTION_SORT: &str = "_section_sort";
pub const SECTION_PROBE: &str = "_section_probe";
pub const SECTION_TRIAGE: &str = "_section_triage";

/// The settings form.
pub fn schema() -> SettingsSchema {
    SettingsSchema::new(vec![
        FieldSpec::section(
            SECTION_SELECTION,
            "Stream Selection",
            "Which streams a run may draw from. The channel side of the scope — profiles, \
             groups and ignored groups — is shared with every other sub-app and lives under \
             [scope] in crosstalk.toml.",
        ),
        FieldSpec::new(
            "selected_m3us",
            SECTION_SELECTION,
            "M3U Sources (comma-separated, prioritized)",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("IPTV Provider 1, Local M3U, Sports")
        .with_help(
            "M3U account names to draw streams from, in priority order, or empty for every \
             active account. Names match exactly and case-sensitively, as in Stream-Mapparr: \
             a different case names a different account, and only a lone * is a wildcard, \
             standing for every other account. Order matters: streams from an earlier account \
             rank above later ones. Without a * entry only the accounts listed are used, so \
             \"Free Provider, *\" puts Free Provider first and every other account after it, \
             including accounts added later. A disabled account is never used.",
        ),
        FieldSpec::new(
            "selected_stream_groups",
            SECTION_SELECTION,
            "Stream Groups (comma-separated)",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("TVE, Cable, Satellite")
        .with_help(
            "Stream groups to draw candidate streams from, or empty for every stream group. \
             Names match exactly and case-sensitively, as in Stream-Mapparr — there are no \
             wildcards here, because Dispatcharr treats Sports and SPORTS as different \
             groups and a glob would silently select a family Stream-Mapparr falls back past.",
        ),
        FieldSpec::new(
            "stream_group_filter_mode",
            SECTION_SELECTION,
            "Stream Groups Mode",
            FieldKind::Text,
            FieldValue::Text(DEFAULT_FILTER_MODE.to_string()),
        )
        .with_placeholder("include or exclude")
        .with_help(
            "include uses only the stream groups listed above; exclude uses every stream \
             group except them. Resolved separately from the channel-group filter in [scope], \
             so excluding a channel group does not also invert this list. Empty list = every \
             group either way.",
        ),
        FieldSpec::new(
            "stream_name_regex_rules",
            SECTION_SELECTION,
            "Stream Name Regex Rules (JSON)",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder(r#"[["\\s*▎\\s*", " "], ["\\bVIP\\b", ""]]"#)
        .with_help(
            "A JSON list of [find, replace] regex pairs, applied in order to stream names \
             before matching. Matching only: a stream's name in Dispatcharr is never changed. \
             Rust's regex engine is not Python's re: backreferences and lookaround in a \
             pattern are refused with an explanation. In a replacement, \\1 and \\g<name> \
             work as they do in Python. Use the test_rules action to preview the effect.",
        ),
        FieldSpec::new(
            "ignore_tags",
            SECTION_SELECTION,
            "Ignore Tags (comma-separated)",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder(r#"4K, [4K], " East", "[Dead]""#)
        .with_help(
            "Extra tags to strip while matching, on top of the [matching] tag handling in \
             crosstalk.toml. A tag is trimmed either way, as in the plugin; quote one to keep \
             a comma inside it, e.g. \"Breakfast, Lunch\".",
        ),
        FieldSpec::section(
            SECTION_MATCH,
            "Match",
            "How a channel's stream list is decided. With Overwrite Existing Streams on, Match \
             & Assign REPLACES a channel's whole stream list, so preview first and read the \
             CSV.",
        ),
        FieldSpec::new(
            "match_sensitivity",
            SECTION_MATCH,
            "Match Sensitivity",
            FieldKind::Text,
            FieldValue::Text(DEFAULT_SENSITIVITY.to_string()),
        )
        .with_placeholder("relaxed, normal, strict or exact")
        .with_help(
            "How closely a stream name must match a channel name: relaxed (70) accepts more \
             matches with more false positives, normal (80) is balanced, strict (90) wants \
             high confidence, exact (95) is near-exact.",
        ),
        FieldSpec::new(
            "overwrite_streams",
            SECTION_MATCH,
            "Overwrite Existing Streams",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "When on, all existing streams are removed and replaced with the matched ones, so \
             a channel can end a run with fewer streams than it started with. When off, \
             matches are added and existing streams are preserved.",
        ),
        FieldSpec::new(
            "keep_custom_streams",
            SECTION_MATCH,
            "Keep Custom Streams Last",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "When on, custom streams already on a channel (hand-made, not from an M3U \
             account) survive an overwrite and stay after the provider streams. Pair it with \
             a fallback player. When off, an overwrite removes them like any other stream.",
        ),
        FieldSpec::new(
            "allow_same_name_streams",
            SECTION_MATCH,
            "Keep Same-Named Streams From One Source",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "When off (the default), streams sharing a name within ONE M3U account are \
             treated as duplicates and only the first is assigned. Turn it on when a provider \
             publishes several genuinely different feeds under one identical name. \
             Same-named streams from DIFFERENT accounts are always kept, and true duplicates \
             (same name, account and URL) are always collapsed.",
        ),
        FieldSpec::new(
            "restrict_matching_to_country",
            SECTION_MATCH,
            "Restrict Matching To Same Country",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "When on, a channel only matches streams detected as the same country. Detection \
             checks the channel database entry first, then the group name, then the channel \
             name. A stream with no recognizable country marker is not dropped, it sorts \
             behind same-country matches. Best with one [matching] database selected: with \
             several, a name that disagrees between them is treated as ambiguous and the \
             filter is skipped for that channel rather than guessing.",
        ),
        FieldSpec::new(
            "stream_prefix_countries",
            SECTION_MATCH,
            "Stream Prefix Countries (comma-separated)",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("NOW=UK, GO=US")
        .with_help(
            "Tell the country filter what a provider prefix means, as PREFIX=COUNTRY pairs, \
             for a prefix that names a platform rather than a country: NOW is Sky's service \
             in the UK and in Italy, so no default can be right for everyone. A prefix is \
             only consulted when nothing else identified the stream, and only at the start of \
             a name (NOW: or (NOW)), never the word inside a title.",
        ),
        FieldSpec::new(
            "visible_channel_limit",
            SECTION_MATCH,
            "Visible Channel Limit",
            FieldKind::Number,
            FieldValue::Number(1.0),
        )
        .with_help(
            "How many channels of one name group receive streams; the rest are left alone. At \
             least 1, because 0 would give no channel a stream. Channels are prioritized by \
             quality tags, then by channel number.",
        ),
        FieldSpec::section(
            SECTION_EPG,
            "EPG-Aware Placeholder Matching",
            "Some providers give PPV and event slots generic placeholder names (PPV EVENT 04) \
             on both the channel and the stream, while the real event title only ever appears \
             in programme data. When enabled, a channel or stream whose name matches one of \
             the patterns below is matched using its CURRENTLY AIRING programme title \
             instead. Matching only: no channel or stream name is ever modified.",
        ),
        FieldSpec::new(
            "epg_placeholder_matching_enabled",
            SECTION_EPG,
            "Enable EPG-Based Placeholder Matching",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help("Master switch for EPG-aware placeholder matching. Off by default."),
        FieldSpec::new(
            "epg_placeholder_name_patterns",
            SECTION_EPG,
            "Placeholder Name Patterns (one regex per line)",
            FieldKind::Text,
            FieldValue::Text(EPG_PLACEHOLDER_NAME_PATTERNS.to_string()),
        )
        .with_placeholder("^PPV EVENT \\d+$")
        .with_help(
            "A channel or stream name is only ever treated as a generic placeholder — and so \
             eligible for EPG substitution — if it matches one of these regexes. Anything \
             else (Dodgers, AEW Redemption) is matched exactly as it is. One regex per line.",
        ),
        FieldSpec::new(
            "epg_title_cleanup_rules",
            SECTION_EPG,
            "EPG Title Cleanup Rules (JSON)",
            FieldKind::Text,
            FieldValue::Text(EPG_TITLE_CLEANUP_RULES.to_string()),
        )
        .with_help(
            "The same [find, replace] JSON format as Stream Name Regex Rules, applied to the \
             raw programme title before it is used for matching — for example to strip a \
             \"Next Event: X at 6:00AM on Jul 26\" wrapper down to just X.",
        ),
        FieldSpec::new(
            "epg_skip_titles",
            SECTION_EPG,
            "Skip Titles (comma-separated)",
            FieldKind::Text,
            FieldValue::Text(EPG_SKIP_TITLES.to_string()),
        )
        .with_placeholder("Signing Off")
        .with_help(
            "When the cleaned current title matches one of these (case-insensitively) the \
             slot is idle and carries no event signal, so the channel and stream stay on \
             their literal placeholder name for this pass.",
        ),
        FieldSpec::new(
            "epg_channel_schedule_cleanup_rules",
            SECTION_EPG,
            "Channel Schedule Suffix Cleanup Rules (JSON)",
            FieldKind::Text,
            FieldValue::Text(EPG_CHANNEL_SCHEDULE_CLEANUP_RULES.to_string()),
        )
        .with_help(
            "The same [find, replace] JSON format, applied to the CHANNEL name whenever EPG \
             placeholder matching is on, before it is compared with a resolved title. The \
             default strips a trailing \"| Weekday @ Time\" annotation from a name like \
             \"WWE Monday Night Raw | Monday @ 5\" so the schedule text does not dilute the \
             score. Adjust or clear it if your naming convention differs.",
        ),
        FieldSpec::new(
            "epg_watch_source_streams",
            SECTION_EPG,
            "EPG Event Watch, Source Streams (comma-separated exact names)",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("ESPN, ESPN 2, ESPN News")
        .with_help(
            "Some events only ever appear as time-boxed programming on a real, permanently \
             named channel such as ESPN. List those channels' exact stream names here to make \
             them an EXTRA alternate for any channel whose name is contained in that stream's \
             current programme title. A watched stream keeps matching its own channel exactly \
             as before. Empty disables it; requires placeholder matching above.",
        ),
        FieldSpec::section(
            SECTION_SORT,
            "Sort",
            "How a channel's streams are ordered. The first entry is the one that plays, so \
             these settings decide failover order.",
        ),
        FieldSpec::new(
            "prioritize_quality",
            SECTION_SORT,
            "Prioritize Quality Before Source",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "When on, streams are sorted by quality first and then by M3U account priority. \
             When off, account priority comes first.",
        ),
        FieldSpec::new(
            "audio_channels_priority",
            SECTION_SORT,
            "Audio Channels Priority",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("7.1, 5.1, stereo, mono")
        .with_help(
            "Audio layouts, most preferred first. Case-insensitive substring match. Anything \
             unlisted, or with no audio information, sorts last. Blank disables it.",
        ),
        FieldSpec::new(
            "audio_codec_priority",
            SECTION_SORT,
            "Audio Codec Priority",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("eac3, ac3, aac, mp2")
        .with_help(
            "Audio codecs, most preferred first. Case-insensitive substring match. Anything \
             unlisted, or with no audio information, sorts last. Blank disables it, and \
             channel layout is ranked before codec.",
        ),
        FieldSpec::new(
            "bitrate_sort_step_kbps",
            SECTION_SORT,
            "Bitrate Sort Step (kbps)",
            FieldKind::Number,
            FieldValue::Number(500.0),
        )
        .with_help(
            "When two streams tie on resolution, frame rate and audio, the higher measured \
             video bitrate goes first. Bitrates are rounded down to this step before they are \
             compared, so streams within one step keep their order and run-to-run variation \
             does not reshuffle them. A stream with no measured bitrate sorts after one that \
             has one. 0 turns the tie-breaker off.",
        ),
        FieldSpec::new(
            "demote_content_starved",
            SECTION_SORT,
            "Demote Placeholder Streams",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "Rank a stream last when it claims 720p or higher but carries less video than the \
             floor below. Providers serve looping slates that report a high resolution while \
             carrying almost no picture, and the throughput probe cannot catch them because a \
             still image arrives faster than realtime. Streams are only moved down the order, \
             never removed, and a stream with no measured bitrate is never affected.",
        ),
        FieldSpec::new(
            "content_bitrate_floor_kbps",
            SECTION_SORT,
            "Placeholder Bitrate Floor (kbps)",
            FieldKind::Number,
            FieldValue::Number(300.0),
        )
        .with_help(
            "A stream claiming 720p or higher while carrying less than this much video counts \
             as a placeholder and ranks last. 300 is deliberately generous so an efficient \
             encode is not demoted, and standard-definition streams are never checked. Raise \
             it only if genuine placeholders are getting through.",
        ),
        FieldSpec::new(
            "enable_throughput_sorting",
            SECTION_SORT,
            "Enable Throughput-Based Sorting",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "When on, streams are sorted by a measured-throughput tier (healthy, marginal, \
             unknown, insufficient) before resolution and frame rate. Measurements come from \
             the probe action, and sorting falls back to resolution and frame rate for a \
             stream with no fresh measurement.",
        ),
        FieldSpec::new(
            "write_concurrency",
            SECTION_SORT,
            "Write Concurrency",
            FieldKind::Number,
            FieldValue::Number(8.0),
        )
        .with_help(
            "How many stream-list writes may be in flight at once. This replaces Stream-\
             Mapparr's Rate Limiting presets, which paced one database write at a time and \
             could not finish a large run. Raise it only if Dispatcharr keeps up.",
        ),
        FieldSpec::section(
            SECTION_PROBE,
            "Probe",
            "What a probe run checks and how hard it may push the providers. A probe opens a \
             real connection to the provider and consumes one of your connection slots, which \
             is why the rate is capped per account and results are reused while fresh. \
             ffprobe and ffmpeg must be in the image; the probe runs where crosstalk runs.",
        ),
        FieldSpec::new(
            "check_alternative_streams",
            SECTION_PROBE,
            "Check Alternative Streams",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "Probe a channel's backup streams as well as its first one. When off, only the \
             primary stream is checked, which is much faster but cannot show that a channel \
             still plays on a backup.",
        ),
        FieldSpec::new(
            "only_visible_channels",
            SECTION_PROBE,
            "Only Check Visible Channels",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "Only check channels enabled in at least one profile. Channels disabled in every \
             profile are skipped, which is how hidden and archived channels stay out of a run.",
        ),
        FieldSpec::new(
            "probe_freshness_hours",
            SECTION_PROBE,
            "Skip Streams Probed Within (Hours)",
            FieldKind::Number,
            FieldValue::Number(24.0),
        )
        .with_help(
            "A stream probed more recently than this is skipped, so a nightly run only covers \
             what has gone stale and a large backlog clears over several nights. 0 probes \
             every stream every run.",
        ),
        FieldSpec::new(
            "timeout",
            SECTION_PROBE,
            "Connection Timeout (seconds)",
            FieldKind::Number,
            FieldValue::Number(10.0),
        )
        .with_help("Network connection timeout, for streams that refuse connections."),
        FieldSpec::new(
            "probe_timeout",
            SECTION_PROBE,
            "Probe Timeout (seconds)",
            FieldKind::Number,
            FieldValue::Number(20.0),
        )
        .with_help(
            "How long to wait for a stream to start delivering data after connecting. Raise \
             it for streams with a slow startup.",
        ),
        FieldSpec::new(
            "dead_connection_retries",
            SECTION_PROBE,
            "Dead Connection Retries",
            FieldKind::Number,
            FieldValue::Number(3.0),
        )
        .with_help(
            "How many times to retry a stream that looks dead before calling it dead. 0 means \
             one attempt. A retry that succeeds makes the stream alive.",
        ),
        FieldSpec::new(
            "probe_per_account_limit",
            SECTION_PROBE,
            "Connections Per M3U Account",
            FieldKind::Number,
            FieldValue::Number(1.0),
        )
        .with_help(
            "Concurrent connections to ONE M3U account, never above that account's own stream \
             limit. 1 is the safe default, because a provider counts connections per account \
             whatever this host does. Total parallelism grows with the number of accounts, so \
             a hundred accounts are checked a hundred lanes wide.",
        ),
        FieldSpec::new(
            "stream_check_delay",
            SECTION_PROBE,
            "Per-Stream Cooldown (seconds)",
            FieldKind::Number,
            FieldValue::Number(2.0),
        )
        .with_help(
            "How long a lane waits after finishing one probe before taking the next, which \
             keeps a provider from seeing back-to-back connection slots. Retry passes wait \
             three times as long.",
        ),
        FieldSpec::new(
            "probe_rate_per_minute",
            SECTION_PROBE,
            "Probe Rate (probes / minute)",
            FieldKind::Number,
            FieldValue::Number(6.0),
        )
        .with_help(
            "Cap on probes started per minute across the whole run, on top of the per-account \
             limit.",
        ),
        FieldSpec::new(
            "probe_duration_seconds",
            SECTION_PROBE,
            "Probe Duration (seconds)",
            FieldKind::Number,
            FieldValue::Number(8.0),
        )
        .with_help(
            "Length of one throughput probe: long enough to clear TCP slow-start, short \
             enough not to stall ranking.",
        ),
        FieldSpec::new(
            "probe_cache_ttl_minutes",
            SECTION_PROBE,
            "Probe Cache TTL (minutes)",
            FieldKind::Number,
            FieldValue::Number(30.0),
        )
        .with_help(
            "How long a measured throughput stays usable for sorting before it is re-\
             measured. Separate from the freshness window above, which decides whether a \
             stream is probed at all.",
        ),
        FieldSpec::new(
            "bitrate_safety_margin",
            SECTION_PROBE,
            "Bitrate Safety Margin",
            FieldKind::Number,
            FieldValue::Number(1.1),
        )
        .with_placeholder("1.10")
        .with_help(
            "Multiplier applied to a stream's nominal bitrate for its claimed resolution. A \
             stream measuring below nominal × this counts as insufficient and ranks last. At \
             least 1.0: below it every stream would be insufficient.",
        ),
        FieldSpec::new(
            "black_screen_detection",
            SECTION_PROBE,
            "Detect Blank-Screen Streams",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "When on, every stream that passes ffprobe is decoded for a few seconds with \
             ffmpeg's blackdetect filter and a pure all-black stream is recorded as blank. \
             Adds seconds per alive stream. Fail-open: if ffmpeg is missing or errors, the \
             stream stays alive. Very dark but not black no-signal slates are not detected.",
        ),
        FieldSpec::new(
            "black_screen_sample_seconds",
            SECTION_PROBE,
            "Blank-Screen Sample (seconds)",
            FieldKind::Number,
            FieldValue::Number(6.0),
        )
        .with_help("How many seconds of video to decode when testing for a blank screen."),
        FieldSpec::new(
            "black_screen_min_black_seconds",
            SECTION_PROBE,
            "Continuous Blank Required (seconds)",
            FieldKind::Number,
            FieldValue::Number(3.0),
        )
        .with_help(
            "Minimum continuous run of black video, within the sample, before a stream counts \
             as blank. Keep it a few seconds below the sample so connection and keyframe \
             latency does not hide a blank stream.",
        ),
        FieldSpec::new(
            "black_screen_ffmpeg_timeout",
            SECTION_PROBE,
            "Blank-Screen ffmpeg Timeout (seconds)",
            FieldKind::Number,
            FieldValue::Number(20.0),
        )
        .with_help(
            "Hard wall-clock cap on the ffmpeg decode, connection included. If it is \
             exceeded the stream is left alive.",
        ),
        FieldSpec::new(
            "placeholder_file_detection",
            SECTION_PROBE,
            "Detect Placeholder-File Streams",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "A continuous live stream reports no fixed duration; one that does is serving a \
             finite file, which providers use as a dead-channel placeholder. Adds no probe \
             time, because the duration is already read. Leave it off if any channel here is \
             fed by a finite file you want to keep.",
        ),
        FieldSpec::new(
            "frozen_video_detection",
            SECTION_PROBE,
            "Detect Frozen-Video Streams",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "Checks the decoded sample for a continuous run of identical frames. A still \
             frame passes ffprobe and is not black, so blank-screen detection cannot see it. \
             Shares the same single decode pass. Fail-open, and a blank screen takes \
             precedence when both fire.",
        ),
        FieldSpec::new(
            "frozen_video_min_seconds",
            SECTION_PROBE,
            "Frozen-Video Minimum (seconds)",
            FieldKind::Number,
            FieldValue::Number(4.0),
        )
        .with_help(
            "How many continuous seconds the picture must stay identical before a stream \
             counts as frozen. Automatically reduced to fit inside the sample length, since a \
             value at or above it could never be reached.",
        ),
        FieldSpec::new(
            "silent_audio_detection",
            SECTION_PROBE,
            "Detect Silent-Audio Streams",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "Measures the mean volume of the decoded sample. A stream with no audio track at \
             all is not flagged. Fail-open: no measurement is not evidence of silence.",
        ),
        FieldSpec::new(
            "silent_audio_max_db",
            SECTION_PROBE,
            "Silence Threshold (dBFS)",
            FieldKind::Number,
            FieldValue::Number(-70.0),
        )
        .with_help(
            "Mean volume at or below this level counts as silence. Must be negative, and \
             between -100 and -1. Digitally silent audio reports about -91 dB and the \
             quietest real channel measured -44 dB, so -70 sits midway with margin on both \
             sides; raise it toward -50 to catch very quiet channels.",
        ),
        FieldSpec::new(
            "streamlink_hosts",
            SECTION_PROBE,
            "Streamlink-Only Hosts (skip ffprobe)",
            FieldKind::Text,
            FieldValue::Text("youtube.com, youtu.be, twitch.tv, kick.com".to_string()),
        )
        .with_placeholder("youtube.com, twitch.tv")
        .with_help(
            "Host suffixes ffprobe cannot validate, served via Streamlink. A stream on one of \
             these is recorded as SKIPPED rather than dead, so triage leaves it alone. Use \
             full domains, not bare words.",
        ),
        FieldSpec::new(
            "ffprobe_path",
            SECTION_PROBE,
            "FFprobe Path",
            FieldKind::Text,
            FieldValue::Text("/usr/local/bin/ffprobe".to_string()),
        )
        .with_placeholder("/usr/local/bin/ffprobe")
        .with_help(
            "Full path to the ffprobe executable. The analysis flags are fixed by the probe \
             (show_streams, show_packets, an error-level log) rather than configurable, \
             because a flag set that breaks the parser would fail every stream at once.",
        ),
        FieldSpec::new(
            "ffmpeg_path",
            SECTION_PROBE,
            "FFmpeg Path",
            FieldKind::Text,
            FieldValue::Text("/usr/local/bin/ffmpeg".to_string()),
        )
        .with_placeholder("/usr/local/bin/ffmpeg")
        .with_help(
            "Full path to the ffmpeg executable, used for the one content-detection decode \
             pass. Unused when every content check is off.",
        ),
        FieldSpec::section(
            SECTION_TRIAGE,
            "Triage",
            "What a triage run does with what the last probe found. Tags are applied by \
             renaming the channel, and every change is recorded so restore can undo it.",
        ),
        FieldSpec::new(
            "dead_rename_format",
            SECTION_TRIAGE,
            "Dead Channel Rename Format",
            FieldKind::Text,
            FieldValue::Text("{name} [DEAD]".to_string()),
        )
        .with_placeholder("[DEAD] {name}")
        .with_help(
            "Used when EVERY stream of a channel failed. {name} is the original channel name. \
             A channel that still plays on a backup is never renamed.",
        ),
        FieldSpec::new(
            "move_to_group_name",
            SECTION_TRIAGE,
            "Move Dead Channels to Group",
            FieldKind::Text,
            FieldValue::Text("Graveyard".to_string()),
        )
        .with_help("The group dead channels are moved into. Empty moves nothing."),
        FieldSpec::new(
            "black_screen_rename_format",
            SECTION_TRIAGE,
            "Blank-Screen Channel Rename Format",
            FieldKind::Text,
            FieldValue::Text("{name} [Blank]".to_string()),
        )
        .with_placeholder("{name} [Blank]")
        .with_help(
            "Used for channels serving a blank picture. Blank-screen channels are excluded \
             from the dead rename and move, so they are never double-tagged.",
        ),
        FieldSpec::new(
            "move_black_screen_group",
            SECTION_TRIAGE,
            "Move Blank-Screen Channels to Group",
            FieldKind::Text,
            FieldValue::Text("Black Screens".to_string()),
        )
        .with_help("The group blank-screen channels are moved into. Empty moves nothing."),
        FieldSpec::new(
            "low_framerate_rename_format",
            SECTION_TRIAGE,
            "Low Framerate Rename Format — Below 24 fps",
            FieldKind::Text,
            FieldValue::Text("{name} [Slow]".to_string()),
        )
        .with_placeholder("[SLOW] {name}")
        .with_help("Used for channels whose streams deliver below 24 fps."),
        FieldSpec::new(
            "move_low_framerate_group",
            SECTION_TRIAGE,
            "Move Low Framerate Channels to Group",
            FieldKind::Text,
            FieldValue::Text("Slow".to_string()),
        )
        .with_help("The group low-framerate channels are moved into. Empty moves nothing."),
        FieldSpec::new(
            "video_format_suffixes",
            SECTION_TRIAGE,
            "Add Video Format Suffixes — [UHD], [FHD], [HD], [SD], [Unknown]",
            FieldKind::Text,
            FieldValue::Text("UHD, FHD, HD, SD, Unknown".to_string()),
        )
        .with_help(
            "Formats to append as a suffix to channels that play, comma-separated. Restore \
             does not strip these, because it only undoes the tags it wrote on channels it \
             marked as failing.",
        ),
    ])
}

/// `streams.toml` -> settings values.
pub fn to_values(settings: &StreamsSettings) -> SettingsValues {
    let mut values = SettingsValues::new();
    for (id, text) in [
        ("selected_m3us", &settings.selected_m3us),
        ("selected_stream_groups", &settings.selected_stream_groups),
        (
            "stream_group_filter_mode",
            &settings.stream_group_filter_mode,
        ),
        ("stream_name_regex_rules", &settings.stream_name_regex_rules),
        ("ignore_tags", &settings.ignore_tags),
        ("match_sensitivity", &settings.match_sensitivity),
        ("stream_prefix_countries", &settings.stream_prefix_countries),
        (
            "epg_placeholder_name_patterns",
            &settings.epg_placeholder_name_patterns,
        ),
        ("epg_title_cleanup_rules", &settings.epg_title_cleanup_rules),
        ("epg_skip_titles", &settings.epg_skip_titles),
        (
            "epg_channel_schedule_cleanup_rules",
            &settings.epg_channel_schedule_cleanup_rules,
        ),
        (
            "epg_watch_source_streams",
            &settings.epg_watch_source_streams,
        ),
        ("audio_channels_priority", &settings.audio_channels_priority),
        ("audio_codec_priority", &settings.audio_codec_priority),
        ("streamlink_hosts", &settings.streamlink_hosts),
        ("ffprobe_path", &settings.ffprobe_path),
        ("ffmpeg_path", &settings.ffmpeg_path),
        ("dead_rename_format", &settings.dead_rename_format),
        ("move_to_group_name", &settings.move_to_group_name),
        (
            "black_screen_rename_format",
            &settings.black_screen_rename_format,
        ),
        ("move_black_screen_group", &settings.move_black_screen_group),
        (
            "low_framerate_rename_format",
            &settings.low_framerate_rename_format,
        ),
        (
            "move_low_framerate_group",
            &settings.move_low_framerate_group,
        ),
        ("video_format_suffixes", &settings.video_format_suffixes),
    ] {
        values.insert(id, FieldValue::Text(text.clone()));
    }

    for (id, flag) in [
        ("overwrite_streams", settings.overwrite_streams),
        ("keep_custom_streams", settings.keep_custom_streams),
        ("allow_same_name_streams", settings.allow_same_name_streams),
        (
            "restrict_matching_to_country",
            settings.restrict_matching_to_country,
        ),
        (
            "epg_placeholder_matching_enabled",
            settings.epg_placeholder_matching_enabled,
        ),
        ("prioritize_quality", settings.prioritize_quality),
        ("demote_content_starved", settings.demote_content_starved),
        (
            "enable_throughput_sorting",
            settings.enable_throughput_sorting,
        ),
        (
            "check_alternative_streams",
            settings.check_alternative_streams,
        ),
        ("only_visible_channels", settings.only_visible_channels),
        ("black_screen_detection", settings.black_screen_detection),
        (
            "placeholder_file_detection",
            settings.placeholder_file_detection,
        ),
        ("frozen_video_detection", settings.frozen_video_detection),
        ("silent_audio_detection", settings.silent_audio_detection),
    ] {
        values.insert(id, FieldValue::Boolean(flag));
    }

    for (id, number) in [
        (
            "visible_channel_limit",
            f64::from(settings.visible_channel_limit),
        ),
        (
            "bitrate_sort_step_kbps",
            f64::from(settings.bitrate_sort_step_kbps),
        ),
        (
            "content_bitrate_floor_kbps",
            f64::from(settings.content_bitrate_floor_kbps),
        ),
        ("write_concurrency", f64::from(settings.write_concurrency)),
        (
            "probe_freshness_hours",
            f64::from(settings.probe_freshness_hours),
        ),
        ("timeout", f64::from(settings.timeout)),
        ("probe_timeout", f64::from(settings.probe_timeout)),
        (
            "dead_connection_retries",
            f64::from(settings.dead_connection_retries),
        ),
        (
            "probe_per_account_limit",
            f64::from(settings.probe_per_account_limit),
        ),
        ("stream_check_delay", f64::from(settings.stream_check_delay)),
        (
            "probe_rate_per_minute",
            f64::from(settings.probe_rate_per_minute),
        ),
        (
            "probe_duration_seconds",
            f64::from(settings.probe_duration_seconds),
        ),
        (
            "probe_cache_ttl_minutes",
            f64::from(settings.probe_cache_ttl_minutes),
        ),
        ("bitrate_safety_margin", settings.bitrate_safety_margin),
        (
            "black_screen_sample_seconds",
            f64::from(settings.black_screen_sample_seconds),
        ),
        (
            "black_screen_min_black_seconds",
            f64::from(settings.black_screen_min_black_seconds),
        ),
        (
            "black_screen_ffmpeg_timeout",
            f64::from(settings.black_screen_ffmpeg_timeout),
        ),
        (
            "frozen_video_min_seconds",
            f64::from(settings.frozen_video_min_seconds),
        ),
        (
            "silent_audio_max_db",
            f64::from(settings.silent_audio_max_db),
        ),
    ] {
        values.insert(id, FieldValue::Number(number));
    }
    values
}

/// Assign one whole-number setting, held to the bounds [`NUMBER_BOUNDS`] declares.
fn set_number(settings: &mut StreamsSettings, id: &str, value: f64) -> Result<()> {
    let (min, max) = bounds(id);
    let number = whole_number(id, value, min, max)?;
    match id {
        "visible_channel_limit" => settings.visible_channel_limit = number,
        "bitrate_sort_step_kbps" => settings.bitrate_sort_step_kbps = number,
        "content_bitrate_floor_kbps" => settings.content_bitrate_floor_kbps = number,
        "write_concurrency" => settings.write_concurrency = number,
        "probe_freshness_hours" => settings.probe_freshness_hours = number,
        "timeout" => settings.timeout = number,
        "probe_timeout" => settings.probe_timeout = number,
        "dead_connection_retries" => settings.dead_connection_retries = number,
        "probe_per_account_limit" => settings.probe_per_account_limit = number,
        "stream_check_delay" => settings.stream_check_delay = number,
        "probe_rate_per_minute" => settings.probe_rate_per_minute = number,
        "probe_duration_seconds" => settings.probe_duration_seconds = number,
        "probe_cache_ttl_minutes" => settings.probe_cache_ttl_minutes = number,
        "black_screen_sample_seconds" => settings.black_screen_sample_seconds = number,
        "black_screen_min_black_seconds" => settings.black_screen_min_black_seconds = number,
        "black_screen_ffmpeg_timeout" => settings.black_screen_ffmpeg_timeout = number,
        "frozen_video_min_seconds" => settings.frozen_video_min_seconds = number,
        other => bail!("unknown whole-number setting {other:?}"),
    }
    Ok(())
}

/// Settings values -> `streams.toml`, keeping anything the form does not know about.
pub fn apply_values(settings: &mut StreamsSettings, values: &SettingsValues) -> Result<()> {
    // Texts, one field at a time: a table would be shorter and would hide which struct field a
    // form id writes to, which is the one thing this function has to get right.
    if let Some(text) = values.text("selected_m3us") {
        settings.selected_m3us = text.to_string();
    }
    if let Some(text) = values.text("selected_stream_groups") {
        settings.selected_stream_groups = text.to_string();
    }
    if let Some(text) = values.text("stream_group_filter_mode") {
        settings.stream_group_filter_mode = text.to_string();
    }
    if let Some(text) = values.text("stream_name_regex_rules") {
        settings.stream_name_regex_rules = text.to_string();
    }
    if let Some(text) = values.text("ignore_tags") {
        settings.ignore_tags = text.to_string();
    }
    if let Some(text) = values.text("match_sensitivity") {
        settings.match_sensitivity = text.to_string();
    }
    if let Some(text) = values.text("stream_prefix_countries") {
        settings.stream_prefix_countries = text.to_string();
    }
    if let Some(text) = values.text("epg_placeholder_name_patterns") {
        settings.epg_placeholder_name_patterns = text.to_string();
    }
    if let Some(text) = values.text("epg_title_cleanup_rules") {
        settings.epg_title_cleanup_rules = text.to_string();
    }
    if let Some(text) = values.text("epg_skip_titles") {
        settings.epg_skip_titles = text.to_string();
    }
    if let Some(text) = values.text("epg_channel_schedule_cleanup_rules") {
        settings.epg_channel_schedule_cleanup_rules = text.to_string();
    }
    if let Some(text) = values.text("epg_watch_source_streams") {
        settings.epg_watch_source_streams = text.to_string();
    }
    if let Some(text) = values.text("audio_channels_priority") {
        settings.audio_channels_priority = text.to_string();
    }
    if let Some(text) = values.text("audio_codec_priority") {
        settings.audio_codec_priority = text.to_string();
    }
    if let Some(text) = values.text("streamlink_hosts") {
        settings.streamlink_hosts = text.to_string();
    }
    if let Some(text) = values.text("ffprobe_path") {
        settings.ffprobe_path = text.to_string();
    }
    if let Some(text) = values.text("ffmpeg_path") {
        settings.ffmpeg_path = text.to_string();
    }
    if let Some(text) = values.text("dead_rename_format") {
        settings.dead_rename_format = text.to_string();
    }
    if let Some(text) = values.text("move_to_group_name") {
        settings.move_to_group_name = text.to_string();
    }
    if let Some(text) = values.text("black_screen_rename_format") {
        settings.black_screen_rename_format = text.to_string();
    }
    if let Some(text) = values.text("move_black_screen_group") {
        settings.move_black_screen_group = text.to_string();
    }
    if let Some(text) = values.text("low_framerate_rename_format") {
        settings.low_framerate_rename_format = text.to_string();
    }
    if let Some(text) = values.text("move_low_framerate_group") {
        settings.move_low_framerate_group = text.to_string();
    }
    if let Some(text) = values.text("video_format_suffixes") {
        settings.video_format_suffixes = text.to_string();
    }

    // Booleans.
    if let Some(value) = values.bool("overwrite_streams") {
        settings.overwrite_streams = value;
    }
    if let Some(value) = values.bool("keep_custom_streams") {
        settings.keep_custom_streams = value;
    }
    if let Some(value) = values.bool("allow_same_name_streams") {
        settings.allow_same_name_streams = value;
    }
    if let Some(value) = values.bool("restrict_matching_to_country") {
        settings.restrict_matching_to_country = value;
    }
    if let Some(value) = values.bool("epg_placeholder_matching_enabled") {
        settings.epg_placeholder_matching_enabled = value;
    }
    if let Some(value) = values.bool("prioritize_quality") {
        settings.prioritize_quality = value;
    }
    if let Some(value) = values.bool("demote_content_starved") {
        settings.demote_content_starved = value;
    }
    if let Some(value) = values.bool("enable_throughput_sorting") {
        settings.enable_throughput_sorting = value;
    }
    if let Some(value) = values.bool("check_alternative_streams") {
        settings.check_alternative_streams = value;
    }
    if let Some(value) = values.bool("only_visible_channels") {
        settings.only_visible_channels = value;
    }
    if let Some(value) = values.bool("black_screen_detection") {
        settings.black_screen_detection = value;
    }
    if let Some(value) = values.bool("placeholder_file_detection") {
        settings.placeholder_file_detection = value;
    }
    if let Some(value) = values.bool("frozen_video_detection") {
        settings.frozen_video_detection = value;
    }
    if let Some(value) = values.bool("silent_audio_detection") {
        settings.silent_audio_detection = value;
    }

    // Whole numbers: the bounds table is the only place a range is written down.
    for (id, _, _) in NUMBER_BOUNDS {
        if let Some(value) = values.number(id) {
            set_number(settings, id, value)?;
        }
    }
    if let Some(value) = values.number("silent_audio_max_db") {
        let (min, max) = SILENT_AUDIO_MAX_DB_BOUNDS;
        settings.silent_audio_max_db = whole_i32("silent_audio_max_db", value, min, max)?;
    }
    if let Some(value) = values.number("bitrate_safety_margin") {
        let (min, max) = BITRATE_MARGIN_BOUNDS;
        if !(min..=max).contains(&value) {
            bail!("bitrate_safety_margin must be between {min} and {max}, got {value}");
        }
        settings.bitrate_safety_margin = value;
    }
    Ok(())
}

/// Load `streams.toml`, falling back to defaults when missing.
pub fn load(path: &Path) -> Result<StreamsSettings> {
    if !path.exists() {
        return Ok(StreamsSettings::default());
    }
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    match toml::from_str::<StreamsSettings>(&text) {
        Ok(settings) => Ok(settings),
        Err(error) => match moved_settings_hint(&text) {
            Some(hint) => Err(anyhow::anyhow!(
                "parsing {}: {error}; {hint}",
                path.display()
            )),
            None => Err(anyhow::Error::new(error))
                .with_context(|| format!("parsing {}", path.display())),
        },
    }
}

/// Write `streams.toml` through the comment-preserving writer.
pub fn save(path: &Path, settings: &StreamsSettings) -> Result<()> {
    crosstalk_core::config::save_with_comments(path, settings)
}

/// Problems an operator must fix before a run, as human-readable strings.
///
/// Only what makes a run WRONG: an unusable preset, an out-of-bounds number, a rename format
/// that would erase a name. A regex rule that cannot be compiled is not here — the plugin
/// degrades instead of failing (it skips the rule and runs), so it is a warning, see
/// [`warnings`].
pub fn validate(settings: &StreamsSettings) -> Vec<String> {
    let mut errors = Vec::new();

    if !SENSITIVITY
        .iter()
        .any(|(name, _)| *name == settings.match_sensitivity.trim().to_lowercase())
    {
        errors.push(format!(
            "match_sensitivity must be one of {}, got {:?}",
            SENSITIVITY
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>()
                .join(", "),
            settings.match_sensitivity
        ));
    }
    if !FILTER_MODES
        .iter()
        .any(|mode| *mode == settings.stream_group_filter_mode.trim().to_lowercase())
    {
        errors.push(format!(
            "stream_group_filter_mode must be one of {}, got {:?}",
            FILTER_MODES.join(", "),
            settings.stream_group_filter_mode
        ));
    }

    // One pass over the numbers, read back through the same mapping the form uses: a bound
    // enforced in `apply_values` alone would let a hand-edited file through.
    let values = to_values(settings);
    for (id, min, max) in NUMBER_BOUNDS {
        if let Some(number) = values.number(id) {
            if number.fract() != 0.0 || !(*min..=*max).contains(&(number as u32)) {
                errors.push(format!(
                    "{id} must be a whole number between {min} and {max}"
                ));
            }
        }
    }
    let (db_min, db_max) = SILENT_AUDIO_MAX_DB_BOUNDS;
    if settings.silent_audio_max_db < db_min || settings.silent_audio_max_db > db_max {
        errors.push(format!(
            "silent_audio_max_db must be between {db_min} and {db_max}, got {}",
            settings.silent_audio_max_db
        ));
    }
    let (margin_min, margin_max) = BITRATE_MARGIN_BOUNDS;
    if !(margin_min..=margin_max).contains(&settings.bitrate_safety_margin) {
        errors.push(format!(
            "bitrate_safety_margin must be between {margin_min} and {margin_max}, got {}",
            settings.bitrate_safety_margin
        ));
    }

    for (id, format) in [
        ("dead_rename_format", &settings.dead_rename_format),
        (
            "black_screen_rename_format",
            &settings.black_screen_rename_format,
        ),
        (
            "low_framerate_rename_format",
            &settings.low_framerate_rename_format,
        ),
    ] {
        if format.trim().is_empty() {
            errors.push(format!(
                "{id} must not be empty: it would erase the channel name"
            ));
        }
    }

    errors
}

/// Things a run should say out loud but still runs through, as human-readable strings.
///
/// A bad regex rule is the plugin's own degrade: it is skipped, counted, and the run
/// continues, so refusing the run over one would be worse than the plugin.
pub fn warnings(settings: &StreamsSettings) -> Vec<String> {
    let mut warnings = Vec::new();
    let parsed = crate::selection::RuleSet::parse(&settings.stream_name_regex_rules);
    if let Some(message) = parsed.rejection_summary() {
        warnings.push(message);
    }
    if !SENSITIVITY
        .iter()
        .any(|(name, _)| *name == settings.match_sensitivity.trim().to_lowercase())
    {
        warnings.push(format!(
            "match_sensitivity {:?} is not a preset; a run will use {} ({})",
            settings.match_sensitivity,
            DEFAULT_SENSITIVITY,
            threshold_for(DEFAULT_SENSITIVITY)
        ));
    }
    if !FILTER_MODES
        .iter()
        .any(|mode| *mode == settings.stream_group_filter_mode.trim().to_lowercase())
    {
        warnings.push(format!(
            "stream_group_filter_mode {:?} is not a mode; a run will read it as {}",
            settings.stream_group_filter_mode, DEFAULT_FILTER_MODE
        ));
    }
    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn schema_ids_are_unique_and_every_field_has_a_section() {
        let schema = schema();
        let mut ids = HashSet::new();
        for field in schema.fields() {
            assert!(ids.insert(field.id), "duplicate field id {}", field.id);
        }
        let sections: HashSet<&str> = schema.sections().map(|section| section.id).collect();
        for field in schema.editable() {
            let section = field.section.expect("editable fields have a section");
            assert!(
                sections.contains(section),
                "{} points at unknown section {section}",
                field.id
            );
        }
    }

    #[test]
    fn schema_defaults_agree_with_the_struct() {
        // A form showing one default and a save writing another is the classic settings bug.
        let defaults = StreamsSettings::default();
        let values = to_values(&defaults);
        for field in schema().editable() {
            assert_eq!(
                values.get(field.id),
                Some(&field.default),
                "{} disagrees between the struct and the schema",
                field.id
            );
        }
    }

    #[test]
    fn the_form_lists_every_key_the_file_holds() {
        // `every_editable_field_has_a_mapping` runs schema -> values. This runs the other way:
        // a struct field with no form field would be invisible in the UI and impossible to
        // edit, and a settings file would carry a key nobody can change.
        let rendered = toml::to_string_pretty(&StreamsSettings::default()).expect("serialise");
        let parsed: toml::Value = toml::from_str(&rendered).expect("parse");
        let keys = parsed.as_table().expect("a table").len();
        assert_eq!(keys, schema().editable().count());
    }

    #[test]
    fn defaults_round_trip_through_values() {
        let settings = StreamsSettings::default();
        let values = to_values(&settings);
        let mut rebuilt = StreamsSettings::default();
        apply_values(&mut rebuilt, &values).expect("apply");
        assert_eq!(rebuilt, settings);
    }

    #[test]
    fn every_editable_field_has_a_mapping() {
        // A field with no accessor in `to_values`/`apply_values` would render an empty control
        // and silently drop the operator's edit.
        let settings = StreamsSettings {
            selected_m3us: "Main, *".to_string(),
            selected_stream_groups: "TVE".to_string(),
            stream_group_filter_mode: "exclude".to_string(),
            stream_name_regex_rules: r#"[["\\s+", " "]]"#.to_string(),
            ignore_tags: "4K, \" East\"".to_string(),
            match_sensitivity: "strict".to_string(),
            overwrite_streams: false,
            keep_custom_streams: false,
            allow_same_name_streams: true,
            restrict_matching_to_country: true,
            stream_prefix_countries: "NOW=UK".to_string(),
            visible_channel_limit: 3,
            epg_placeholder_matching_enabled: true,
            epg_placeholder_name_patterns: "^PPV \\d+$".to_string(),
            epg_title_cleanup_rules: r#"[["^Next ", ""]]"#.to_string(),
            epg_skip_titles: "Signing Off".to_string(),
            epg_channel_schedule_cleanup_rules: r#"[[" \\|.*", ""]]"#.to_string(),
            epg_watch_source_streams: "ESPN".to_string(),
            prioritize_quality: true,
            audio_channels_priority: "7.1, 5.1".to_string(),
            audio_codec_priority: "eac3".to_string(),
            bitrate_sort_step_kbps: 0,
            demote_content_starved: false,
            content_bitrate_floor_kbps: 400,
            enable_throughput_sorting: false,
            write_concurrency: 4,
            check_alternative_streams: false,
            only_visible_channels: true,
            probe_freshness_hours: 0,
            timeout: 5,
            probe_timeout: 15,
            dead_connection_retries: 1,
            probe_per_account_limit: 2,
            stream_check_delay: 1,
            probe_rate_per_minute: 30,
            probe_duration_seconds: 4,
            probe_cache_ttl_minutes: 60,
            bitrate_safety_margin: 1.25,
            black_screen_detection: true,
            black_screen_sample_seconds: 8,
            black_screen_min_black_seconds: 4,
            black_screen_ffmpeg_timeout: 30,
            placeholder_file_detection: true,
            frozen_video_detection: true,
            frozen_video_min_seconds: 3,
            silent_audio_detection: true,
            silent_audio_max_db: -60,
            streamlink_hosts: "twitch.tv".to_string(),
            ffprobe_path: "/usr/bin/ffprobe".to_string(),
            ffmpeg_path: "/usr/bin/ffmpeg".to_string(),
            dead_rename_format: "[DEAD] {name}".to_string(),
            move_to_group_name: "Dead".to_string(),
            black_screen_rename_format: "[Blank] {name}".to_string(),
            move_black_screen_group: "Blank".to_string(),
            low_framerate_rename_format: "[Slow] {name}".to_string(),
            move_low_framerate_group: "Slow".to_string(),
            video_format_suffixes: "HD, SD".to_string(),
        };
        let values = to_values(&settings);
        for field in schema().editable() {
            assert!(
                values.get(field.id).is_some(),
                "{} has no value mapping",
                field.id
            );
        }

        let mut rebuilt = StreamsSettings::default();
        apply_values(&mut rebuilt, &values).expect("apply");
        assert_eq!(rebuilt, settings);
    }

    #[test]
    fn every_bound_names_a_real_setting() {
        // The bounds table is read back through `to_values` by `validate`, so an id that no
        // setting carries would silently check nothing.
        let values = to_values(&StreamsSettings::default());
        for (id, min, max) in NUMBER_BOUNDS {
            assert!(values.number(id).is_some(), "{id} is not a setting");
            assert!(min < max, "{id} has an empty range");
        }
        assert_eq!(NUMBER_BOUNDS.len(), 17);
    }

    #[test]
    fn the_sensitivity_presets_are_the_plugins() {
        // plugin.py `PluginConfig.SENSITIVITY_MAP`.
        assert_eq!(
            SENSITIVITY,
            [
                ("relaxed", 70),
                ("normal", 80),
                ("strict", 90),
                ("exact", 95)
            ]
        );
        for (name, threshold) in SENSITIVITY {
            assert_eq!(threshold_for(name), threshold);
            assert_eq!(threshold_for(&name.to_uppercase()), threshold);
        }
        assert_eq!(threshold_for("nonsense"), 80);
        assert_eq!(threshold_for(" Normal "), 80);
    }

    #[test]
    fn the_shipped_defaults_are_the_plugins() {
        let settings = StreamsSettings::default();
        // Stream-Mapparr `PluginConfig`.
        assert_eq!(settings.selected_m3us, "");
        assert_eq!(settings.selected_stream_groups, "");
        assert_eq!(settings.stream_group_filter_mode, "include");
        assert_eq!(settings.match_sensitivity, "normal");
        assert!(settings.overwrite_streams);
        assert!(settings.keep_custom_streams);
        assert!(!settings.allow_same_name_streams);
        assert!(!settings.restrict_matching_to_country);
        assert_eq!(settings.stream_prefix_countries, "");
        assert_eq!(settings.visible_channel_limit, 1);
        assert!(!settings.epg_placeholder_matching_enabled);
        assert_eq!(
            settings.epg_skip_titles,
            "Signing Off,No Event Today,No Game Today"
        );
        assert!(!settings.prioritize_quality);
        assert_eq!(settings.audio_channels_priority, "");
        assert_eq!(settings.audio_codec_priority, "");
        assert_eq!(settings.bitrate_sort_step_kbps, 500);
        assert!(settings.demote_content_starved);
        assert_eq!(settings.content_bitrate_floor_kbps, 300);
        assert!(settings.enable_throughput_sorting);
        assert_eq!(settings.probe_duration_seconds, 8);
        assert_eq!(settings.probe_cache_ttl_minutes, 30);
        assert_eq!(settings.probe_rate_per_minute, 6);
        assert_eq!(settings.bitrate_safety_margin, 1.10);
        // IPTV Checker `plugin.json`.
        assert_eq!(settings.timeout, 10);
        assert_eq!(settings.probe_timeout, 20);
        assert_eq!(settings.dead_connection_retries, 3);
        assert_eq!(settings.stream_check_delay, 2);
        assert!(settings.check_alternative_streams);
        assert!(!settings.only_visible_channels);
        assert!(!settings.black_screen_detection);
        assert_eq!(settings.black_screen_sample_seconds, 6);
        assert_eq!(settings.black_screen_min_black_seconds, 3);
        assert_eq!(settings.black_screen_ffmpeg_timeout, 20);
        assert!(!settings.placeholder_file_detection);
        assert!(!settings.frozen_video_detection);
        assert_eq!(settings.frozen_video_min_seconds, 4);
        assert!(!settings.silent_audio_detection);
        assert_eq!(settings.silent_audio_max_db, -70);
        assert_eq!(
            settings.streamlink_hosts,
            "youtube.com, youtu.be, twitch.tv, kick.com"
        );
        assert_eq!(settings.ffprobe_path, "/usr/local/bin/ffprobe");
        assert_eq!(settings.ffmpeg_path, "/usr/local/bin/ffmpeg");
        assert_eq!(settings.dead_rename_format, "{name} [DEAD]");
        assert_eq!(settings.move_to_group_name, "Graveyard");
        assert_eq!(settings.black_screen_rename_format, "{name} [Blank]");
        assert_eq!(settings.move_black_screen_group, "Black Screens");
        assert_eq!(settings.low_framerate_rename_format, "{name} [Slow]");
        assert_eq!(settings.move_low_framerate_group, "Slow");
        assert_eq!(settings.video_format_suffixes, "UHD, FHD, HD, SD, Unknown");
        // Crosstalk's own: the plan's "one connection per account", the write concurrency
        // that replaces `rate_limiting`, and the freshness window.
        assert_eq!(settings.probe_per_account_limit, 1);
        assert_eq!(settings.write_concurrency, 8);
        assert_eq!(settings.probe_freshness_hours, 24);
    }

    #[test]
    fn the_epg_placeholder_defaults_are_the_plugins() {
        // plugin.py `PluginConfig.DEFAULT_EPG_PLACEHOLDER_NAME_PATTERNS` and friends. These are
        // regexes inside a JSON string inside a TOML value: three layers of escaping that a
        // "tidy-up" would break, so they are pinned exactly.
        assert_eq!(
            EPG_PLACEHOLDER_NAME_PATTERNS,
            "^PPV EVENT \\d+$\n^LIVE EVENT \\d+$\n^PPV \\d+ \\|?$\n^HBO \\d+$\n^MAX \\d+$\n\
             ^Triller TV \\| Event \\d+$"
        );
        assert_eq!(EPG_PLACEHOLDER_NAME_PATTERNS.lines().count(), 6);
        assert_eq!(
            EPG_TITLE_CLEANUP_RULES,
            r#"[["^Next Event:\\s*", ""], ["\\s+at \\d{1,2}:\\d{2}\\s*[AP]M on \\w+ \\d{1,2}$", ""]]"#
        );
        assert_eq!(
            EPG_CHANNEL_SCHEDULE_CLEANUP_RULES,
            r#"[["(?i)\\s*\\|\\s*\\w+\\s*@\\s*\\d{1,2}(?::\\d{2})?\\s*[AP]?M?\\s*$", ""]]"#
        );
        // Both are valid JSON whose strings are valid Rust regexes: the pair is what makes the
        // feature work at all.
        let rules = crate::selection::RuleSet::parse(EPG_TITLE_CLEANUP_RULES);
        assert_eq!(rules.rules.len(), 2, "{:?}", rules.report);
        assert!(rules.rejected().count() == 0);
        let rules = crate::selection::RuleSet::parse(EPG_CHANNEL_SCHEDULE_CLEANUP_RULES);
        assert_eq!(rules.rules.len(), 1, "{:?}", rules.report);
        // The placeholder patterns are one regex per line, not JSON.
        for line in EPG_PLACEHOLDER_NAME_PATTERNS.lines() {
            regex::Regex::new(line).unwrap_or_else(|error| panic!("{line}: {error}"));
        }
    }

    #[test]
    fn validate_catches_an_unusable_preset_and_mode() {
        let mut settings = StreamsSettings::default();
        assert!(validate(&settings).is_empty());

        settings.match_sensitivity = "lenient".to_string();
        settings.stream_group_filter_mode = "skip".to_string();
        let errors = validate(&settings);
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(errors[0].contains("match_sensitivity"));
        assert!(errors[1].contains("stream_group_filter_mode"));
        // A run still proceeds on the documented fallbacks, and says so.
        let warnings = warnings(&settings);
        assert!(
            warnings.iter().any(|w| w.contains("normal (80)")),
            "{warnings:?}"
        );
        assert!(
            warnings.iter().any(|w| w.contains("include")),
            "{warnings:?}"
        );
    }

    #[test]
    fn validate_catches_out_of_bounds_numbers_from_a_hand_edited_file() {
        let settings = StreamsSettings {
            visible_channel_limit: 0,
            probe_per_account_limit: 100,
            silent_audio_max_db: 0,
            bitrate_safety_margin: 0.5,
            ..StreamsSettings::default()
        };
        let errors = validate(&settings);
        assert!(
            errors.iter().any(|e| e.contains("visible_channel_limit")),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|e| e.contains("probe_per_account_limit")),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|e| e.contains("silent_audio_max_db")),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|e| e.contains("bitrate_safety_margin")),
            "{errors:?}"
        );
    }

    #[test]
    fn validate_refuses_a_rename_format_that_would_erase_a_name() {
        let settings = StreamsSettings {
            dead_rename_format: "   ".to_string(),
            ..StreamsSettings::default()
        };
        let errors = validate(&settings);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("dead_rename_format"));
    }

    #[test]
    fn apply_rejects_out_of_range_numbers_and_names_the_field() {
        let mut settings = StreamsSettings::default();
        let mut values = SettingsValues::new();
        values.insert("timeout", FieldValue::Number(0.0));
        let error = apply_values(&mut settings, &values).expect_err("rejects");
        assert!(format!("{error}").contains("timeout"), "{error}");
        assert!(format!("{error}").contains("between 1 and 300"), "{error}");

        // Out of range without being fractional, and fractional within range: both refused.
        let mut values = SettingsValues::new();
        values.insert("probe_freshness_hours", FieldValue::Number(6.5));
        assert!(apply_values(&mut settings, &values).is_err());
        let mut values = SettingsValues::new();
        values.insert("visible_channel_limit", FieldValue::Number(0.0));
        assert!(apply_values(&mut settings, &values).is_err());
        // 0 is a valid choice where 0 turns something off.
        let mut values = SettingsValues::new();
        values.insert("bitrate_sort_step_kbps", FieldValue::Number(0.0));
        apply_values(&mut settings, &values).expect("0 turns the tie-breaker off");
        assert_eq!(settings.bitrate_sort_step_kbps, 0);
    }

    #[test]
    fn bad_regex_rules_are_a_warning_not_a_refusal() {
        // The plugin degrades: a rule it cannot compile is skipped and the run continues.
        let settings = StreamsSettings {
            stream_name_regex_rules: r#"[["(unclosed", ""]]"#.to_string(),
            ..StreamsSettings::default()
        };
        assert!(validate(&settings).is_empty());
        let warnings = warnings(&settings);
        assert!(warnings.iter().any(|w| w.contains("regex")), "{warnings:?}");
    }

    #[test]
    fn a_missing_file_means_defaults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let loaded = load(&dir.path().join("absent.toml")).expect("load");
        assert_eq!(loaded, StreamsSettings::default());
    }

    #[test]
    fn save_and_load_preserve_comments_and_the_regex_defaults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("streams.toml");
        std::fs::write(&path, "# chosen for the test\nprobe_freshness_hours = 12\n").expect("seed");

        let settings = StreamsSettings {
            probe_freshness_hours: 48,
            ..StreamsSettings::default()
        };
        save(&path, &settings).expect("save");
        let text = std::fs::read_to_string(&path).expect("read");
        assert!(text.contains("# chosen for the test"), "{text}");

        // The EPG defaults survive a save/load round trip through TOML escaping.
        let loaded = load(&path).expect("load");
        assert_eq!(loaded, settings);
        assert_eq!(loaded.epg_title_cleanup_rules, EPG_TITLE_CLEANUP_RULES);
    }

    #[test]
    fn unknown_keys_are_rejected_loudly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("streams.toml");
        std::fs::write(&path, "visible_channel_limitt = 3\n").expect("seed");
        let error = load(&path).expect_err("rejects the typo");
        assert!(
            format!("{error:#}").contains("visible_channel_limitt"),
            "the error names the key: {error:#}"
        );
    }

    #[test]
    fn a_retired_setting_is_rejected_with_a_pointer_to_its_new_home() {
        // The clean break is intended. The failure has to say where the value went, because
        // `deny_unknown_fields` alone only says the key is unknown.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("streams.toml");
        std::fs::write(
            &path,
            "profile_name = \"All\"\ngroup_filter_mode = \"exclude\"\ntag_handling = \"keep_all\"\n\
             dry_run_mode = true\nrate_limiting = \"low\"\nscheduled_times = \"0600\"\n",
        )
        .expect("seed");
        let error = format!("{:#}", load(&path).expect_err("rejects the old keys"));
        assert!(error.contains("[scope] profiles"), "{error}");
        assert!(error.contains("ignore_groups"), "{error}");
        assert!(error.contains("[matching]"), "{error}");
        assert!(error.contains("--dry-run"), "{error}");
        assert!(error.contains("write_concurrency"), "{error}");
        assert!(error.contains("[schedules]"), "{error}");
    }

    #[test]
    fn every_retired_plugin_key_has_a_pointer() {
        // Both plugins' field lists, minus what this file still holds and what was never a
        // file key (section headers, action buttons, event hooks). A key missing here is
        // still rejected by `deny_unknown_fields`, but without saying where it went — which
        // is the whole point of the MOVED table.
        let retired = [
            // Stream-Mapparr settings that live elsewhere now.
            "profile_name",
            "selected_groups",
            "group_filter_mode",
            "tag_handling",
            "custom_aliases",
            "channel_database",
            "csv_export_retention_days",
            "dry_run_mode",
            "rate_limiting",
            "auto_match_on_m3u_refresh",
            "run_after_iptv_checker_scan",
            "wait_for_iptv_checker",
            "iptv_checker_max_wait_hours",
            "filter_dead_streams",
            "notify_enabled",
            "notify_report_on",
            "notify_report_format",
            "scheduled_times",
            "scheduled_match_streams",
            "scheduled_sort_streams",
            "scheduled_fill_mirror_backups",
            "enable_scheduled_csv_export",
            // IPTV Checker settings that live elsewhere now.
            "channel_groups",
            "channel_groups_mode",
            "enable_parallel_checking",
            "parallel_workers",
            "scheduled_times",
            "schedule_window_enabled",
            "schedule_end_mode",
            "schedule_duration_hours",
            "schedule_end_time",
            "scheduler_email_report",
            "scheduler_trigger_stream_mapparr",
            "csv_retention_days",
            "scheduler_restore_channels",
            "scheduler_rename_dead_channels",
            "scheduler_rename_black_screen_channels",
            "scheduler_rename_low_framerate_channels",
            "scheduler_add_video_format_suffix",
            "scheduler_move_dead_channels",
            "scheduler_move_black_screen_channels",
            "scheduler_move_low_framerate_channels",
            "scheduler_delete_dead_channels",
            "auto_delete_confirmation",
            "ffprobe_flags",
            "ffprobe_analysis_duration",
            "share_usage_counts",
        ];
        let moved: std::collections::HashSet<&str> = MOVED.iter().map(|(old, _)| *old).collect();
        // `scheduled_times` is one key both plugins grew; it needs one entry, not two.
        let mut missing: Vec<&&str> = retired
            .iter()
            .filter(|key| !moved.contains(**key))
            .collect();
        missing.sort();
        missing.dedup();
        assert!(
            missing.is_empty(),
            "retired keys with no pointer: {missing:?}"
        );
    }

    #[test]
    fn the_shipped_example_config_loads_and_validates() {
        // The release ships the file for operators to copy; a key renamed in code but not there
        // would make the copied file refuse to load.
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
        let settings = load(&root.join("streams.example.toml")).expect("streams example");
        assert!(validate(&settings).is_empty(), "{:?}", validate(&settings));
        assert!(warnings(&settings).is_empty(), "{:?}", warnings(&settings));
        // And it really is the defaults, all commented, so a copied file behaves like a fresh
        // install.
        assert_eq!(settings, StreamsSettings::default());
    }

    #[test]
    fn the_ignore_tags_list_is_parsed_with_quotes() {
        let settings = StreamsSettings {
            ignore_tags: "\" East\", 4K, \"Breakfast, Lunch\"".to_string(),
            ..StreamsSettings::default()
        };
        // Every tag is trimmed, quoted or not: `_parse_tags` strips each one it splits off.
        assert_eq!(
            settings.ignore_tag_list(),
            vec![
                "East".to_string(),
                "4K".to_string(),
                "Breakfast, Lunch".to_string()
            ]
        );
        assert!(StreamsSettings::default().ignore_tag_list().is_empty());
    }

    #[test]
    fn the_streamlink_hosts_list_is_lowercased() {
        let settings = StreamsSettings {
            streamlink_hosts: "YouTube.com, TWITCH.tv".to_string(),
            ..StreamsSettings::default()
        };
        assert_eq!(
            settings.streamlink_host_list(),
            vec!["youtube.com".to_string(), "twitch.tv".to_string()]
        );
    }
}
