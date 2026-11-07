//! `lineup` settings: the values, and the form schema the UI renders.
//!
//! The field ids, labels and help text follow the three plugins this app ports —
//! Lineuparr (`Lineuparr/plugin.py`'s `Plugin.fields`, the single source of truth there),
//! Channel-Mapparr (`Channel-Maparr/plugin.json` plus `plugin.py`) and Channel Name Cleaner
//! (`channel_name_cleaner/plugin.json`) — because operators already know those names.
//!
//! Only what the lineup app decides for itself lives here. Everything shared with the
//! other sub-apps stays in `crosstalk.toml` and is NOT duplicated:
//!
//!   * `[scope]` — the channel half of the scope. Channel-Mapparr's `selected_groups` and
//!     Name Cleaner's `selected_groups` are `[scope].groups`, and their `ignore_groups` are
//!     `[scope].ignore_groups` (the host models the two lists directly instead of a list
//!     plus a mode switch).
//!   * `[matching]` — the match cut and the tag handling. Lineuparr's and Channel-Mapparr's
//!     `match_sensitivity` presets *are* the host sensitivity, their `channel_databases`
//!     are `[matching].enabled_databases`, Channel-Mapparr's `ignored_tags` are the host
//!     `ignore_*_tags`, and Lineuparr's `custom_aliases` is `[matching].custom_aliases`.
//!   * `[exports]` — the CSV retention every plugin grew its own copy of.
//!   * scheduling — a schedule in `crosstalk.toml` replaces the plugins' schedulers and
//!     windows, and the host webhook replaces Lineuparr's `on_m3u_refresh`.
//!   * write pacing — Lineuparr's `rate_limiting` (pauses between database writes) is
//!     `[dispatcharr] concurrency` in `crosstalk.toml`, the host's one cap on concurrent
//!     requests; this app has no write-concurrency knob of its own.
//!
//! Nor does it duplicate another app's settings. Lineuparr's matcher is not ported
//! (plan decision): its stream half is `streams.match` in lineup mode and its EPG half is
//! `guide.match`, so the settings those halves read live in those apps' files —
//! `epg_sources` is guide's `epg_sources_to_match`, `refresh_epg_after_match` is a
//! `guide.watchdog_apply` step, `preserve_existing_streams` is streams'
//! `overwrite_streams` (inverted) and Channel-Mapparr's `m3u_sources` is streams'
//! `selected_m3us`. What a run touches is the scope, not a setting:
//! `single_channel_name` is a workflow step's scope, and Lineuparr's replace-mode delete
//! is the `lineup.tidy_apply` step a workflow lists after `streams.match_apply`. Each
//! retired key fails at load naming its new home (see [`MOVED`]).

use anyhow::{Context, Result};
use crosstalk_core::subapp::{
    whole_number, FieldKind, FieldSpec, FieldValue, SettingsSchema, SettingsValues,
};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Every whole-number setting, with the bounds the form and a hand-edited file are both
/// held to: `(id, min, max)`.
///
/// The plugins floor nothing (`max_preview_rows` of 0 simply shows nothing), so these are
/// crosstalk's: a bound tight enough to catch a typo, loose enough that every sane
/// installation is inside it. The test `every_bound_names_a_real_setting` keeps this
/// table and the schema in step.
const NUMBER_BOUNDS: &[(&str, u32, u32)] = &[("max_preview_rows", 1, 100_000)];

/// The bounds for one whole-number setting.
fn bounds(id: &str) -> (u32, u32) {
    NUMBER_BOUNDS
        .iter()
        .find(|(field, _, _)| *field == id)
        .map(|(_, min, max)| (*min, *max))
        .unwrap_or_else(|| panic!("{id} declares no bounds"))
}

/// Everything `lineup.toml` holds.
///
/// The `Default` impl is written out rather than derived: `#[serde(default)]` fills
/// missing fields from it, so a derived one would silently default every threshold to 0 —
/// a valid-looking configuration that refuses or demotes everything.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct LineupSettings {
    // --- Clean (Channel Name Cleaner) ---------------------------------------------------
    /// Split on `|`, drop empty and quality-only segments. NC's `strip_pipe_junk`.
    pub strip_pipe_junk: bool,
    /// Strip bracketed quality/junk tags. NC's `strip_bracketed_junk`.
    pub strip_bracketed_junk: bool,
    /// Strip fancy-unicode quality marks. NC's `strip_unicode_quality`.
    pub strip_unicode_quality: bool,
    /// Fold styled/full-width characters to plain ASCII. NC's `unicode_nfkc`.
    pub unicode_nfkc: bool,
    /// How remaining pipe segments are rejoined. NC's `join_separator`.
    pub join_separator: String,
    /// Collapse multiple spaces and trim. NC's `normalize_whitespace`.
    pub normalize_whitespace: bool,
    /// `'Channel 4+1'` -> `'Channel 4 +1'`. NC's `normalize_plus_one`.
    pub normalize_plus_one: bool,
    /// Title-case names. NC's `fix_casing`.
    pub fix_casing: bool,
    /// What to do when a cleaned name collides: `suffix`, `numeric` or `skip`.
    /// NC's `collision_strategy`.
    pub collision_strategy: String,
    /// Comma-separated group names to skip entirely. NC's `protected_groups`.
    pub protected_groups: String,
    /// Skip names with non-Latin text. NC's `protect_foreign_scripts`.
    pub protect_foreign_scripts: bool,
    /// Skip names starting with `&`. NC's `protect_leading_symbol`.
    pub protect_leading_symbol: bool,
    /// Any channel name matching this regex is left untouched. NC's `protect_regex`.
    pub protect_regex: String,
    /// Extra find/replace, one `regex => replacement` per line. NC's `custom_rules`.
    pub custom_rules: String,
    /// Rows listed on screen; the full set is always written to the CSV.
    /// NC's `max_preview_rows`.
    pub max_preview_rows: u32,

    // --- Names (Channel-Mapparr rename + unknown tagging, AI gate) ----------------------
    /// OTA template, e.g. `{NETWORK} - {STATE} {CITY} ({CALLSIGN})`. CM's `ota_format`.
    pub ota_format: String,
    /// Fall back to the market when a channel has no callsign. CM's `ota_market_fallback`.
    pub ota_market_fallback: bool,
    /// Suffix appended to a channel no database recognises. CM's `unknown_suffix`.
    pub unknown_suffix: String,
    /// Tag channels no database recognises with [`unknown_suffix`](Self::unknown_suffix).
    /// CM's `rename_unknown_channels` (tag unmatched = the option).
    pub tag_unmatched_channels: bool,
    /// Ask the `[ai.verify]` chain about borderline premium renames. Off by default:
    /// with it off a borderline rename is left alone rather than guessed.
    pub ai_gate_enabled: bool,

    // --- Categorize (Channel-Mapparr organize) -------------------------------------------
    /// Category group names the organize step manages. CM's `category_groups`.
    pub category_groups: String,

    // --- Logos (CM apply/replace tv-logos + LU logo tiers) -------------------------------
    /// Logo assigned to a channel with no better match, by name. CM's `default_logo`.
    pub default_logo: String,
    /// Where a logo may come from, in priority order. Lineuparr's tiers
    /// (EPG icon, Logo Manager name, tv-logos) plus Channel-Mapparr's network
    /// fallback (`network`) as one source list, resolved by `core::logos`.
    pub logo_sources: String,
    /// Also replace a logo a channel already has when the resolver finds a match.
    /// Channel-Mapparr's `replace_tv_logos` (as opposed to `apply_tv_logos`, which
    /// leaves channels with a logo alone).
    pub replace_existing: bool,
    /// Country codes whose tv-logos directories the per-channel logo step searches.
    /// Empty means the lineup file's own country.
    pub tv_logos_countries: String,

    // --- Create (CM import M3U streams / create channels from streams) --------------------
    /// What `create_*` makes: `streams` (one channel per matched stream, CM's
    /// `import_m3u_streams`) or `stations` (one channel per OTA station from
    /// unattached streams, CM's `create_channels_from_streams`).
    pub create_mode: String,
    /// Comma-separated M3U group titles to import from, applied BEFORE matching and
    /// within the stream pool streams' `selected_m3us` selects. CM's `m3u_group_filter`.
    pub m3u_group_filter: String,
    /// Comma-separated category names, applied AFTER matching. CM's `m3u_category_filter`.
    pub m3u_category_filter: String,
    /// Put every imported channel in this one group instead of by category.
    /// CM's `m3u_custom_group_name`.
    pub m3u_custom_group_name: String,
    /// Stream groups to scan for unattached streams (stations mode). CM's
    /// `seed_source_groups`.
    pub seed_source_groups: String,
    /// The existing channel group the new stations are created in. CM's
    /// `seed_target_group`.
    pub seed_target_group: String,
    /// The first channel number to assign. Empty starts above the highest channel
    /// number on the system. CM's `seed_start_number`.
    pub seed_start_number: String,
    /// Comma-separated network names to skip when creating stations. CM's
    /// `seed_exclude_networks`.
    pub seed_exclude_networks: String,

    // --- Sync (Lineuparr sync + tidy) -----------------------------------------------------
    /// The provider lineup to mirror. LU's `lineup_file`.
    pub lineup_file: String,
    /// Group name prefix: blank = auto from the lineup package name, `none` = no prefix.
    /// LU's `group_prefix`.
    pub group_prefix: String,
    /// How lineup categories fold: `none`, `refined`, `simple` or `normal`.
    /// LU's `category_detail`.
    pub category_detail: String,
    /// How a new channel gets its number: `lineup`, `auto_next`, `auto_highest` or
    /// `specific`. LU's `channel_numbering`.
    pub channel_numbering: String,
    /// The first number for `specific` numbering; every other mode ignores it.
    /// LU's `starting_channel_number`.
    pub starting_channel_number: String,
    /// Channel profile(s) to enable the synced channels in. LU's `channel_profiles`.
    pub channel_profiles: String,
}

impl Default for LineupSettings {
    fn default() -> Self {
        Self {
            // Name Cleaner's shipped defaults.
            strip_pipe_junk: true,
            strip_bracketed_junk: true,
            strip_unicode_quality: true,
            unicode_nfkc: false,
            join_separator: " ".to_string(),
            normalize_whitespace: true,
            normalize_plus_one: false,
            fix_casing: false,
            collision_strategy: "suffix".to_string(),
            protected_groups: "Todays Live Events, Info | Service Status".to_string(),
            protect_foreign_scripts: true,
            protect_leading_symbol: true,
            protect_regex: String::new(),
            custom_rules: String::new(),
            max_preview_rows: 400,

            // Channel-Mapparr's shipped defaults.
            ota_format: "{NETWORK} - {STATE} {CITY} ({CALLSIGN})".to_string(),
            ota_market_fallback: false,
            unknown_suffix: " [Unk]".to_string(),
            tag_unmatched_channels: true,
            // The AI gate is crosstalk-only and defaults OFF (plan decision): a
            // borderline premium rename is left alone unless the operator opts in.
            ai_gate_enabled: false,

            category_groups: String::new(),

            default_logo: String::new(),
            // Lineuparr's tiers, in Lineuparr's order: the EPG icon first, then the
            // Logo Manager name, then tv-logos.
            logo_sources: "epg, logo_manager, tv-logos, network".to_string(),
            replace_existing: false,
            tv_logos_countries: String::new(),

            // Channel-Mapparr's shipped defaults.
            create_mode: "streams".to_string(),
            m3u_group_filter: String::new(),
            m3u_category_filter: String::new(),
            m3u_custom_group_name: String::new(),
            seed_source_groups: String::new(),
            seed_target_group: String::new(),
            seed_start_number: String::new(),
            seed_exclude_networks: String::new(),

            // Lineuparr's shipped defaults.
            lineup_file: "US_DirecTV-Premier_lineup.json".to_string(),
            group_prefix: String::new(),
            category_detail: "normal".to_string(),
            channel_numbering: "lineup".to_string(),
            starting_channel_number: String::new(),
            channel_profiles: String::new(),
        }
    }
}

/// The collision strategies Name Cleaner offers.
pub const COLLISION_STRATEGIES: [&str; 3] = ["suffix", "numeric", "skip"];
/// What `create_*` may make.
pub const CREATE_MODES: [&str; 2] = ["streams", "stations"];

/// Settings that are no longer read from this file, and where each went.
///
/// `deny_unknown_fields` already rejects them; this exists so the rejection says where
/// the value went instead of only that the key is unknown. A clean break is deliberate:
/// silently accepting the old keys would leave two files (or a dead key) claiming one
/// setting.
const MOVED: &[(&str, &str)] = &[
    (
        "channel_databases",
        "[matching] enabled_databases in crosstalk.toml",
    ),
    (
        "match_sensitivity",
        "[matching] sensitivity in crosstalk.toml",
    ),
    (
        "ignored_tags",
        "[matching] ignore_quality_tags / ignore_regional_tags / ignore_geographic_tags / \
         ignore_misc_tags in crosstalk.toml",
    ),
    (
        "custom_aliases",
        "[matching] custom_aliases in crosstalk.toml",
    ),
    ("selected_groups", "[scope] groups in crosstalk.toml"),
    ("ignore_groups", "[scope] ignore_groups in crosstalk.toml"),
    (
        "rate_limiting",
        "[dispatcharr] concurrency in crosstalk.toml (bounded REST concurrency replaces the \
         pause presets)",
    ),
    // Lineuparr's matcher is not ported: its EPG half is guide.match, its stream half
    // streams.match in lineup mode, so their settings live in those apps' files.
    (
        "epg_sources",
        "epg_sources_to_match in guide.toml (guide.match replaces Lineuparr's EPG match)",
    ),
    (
        "refresh_epg_after_match",
        "a guide.watchdog_apply step after guide.match_apply in the workflow",
    ),
    (
        "preserve_existing_streams",
        "overwrite_streams in streams.toml (inverted: preserve = overwrite_streams false); \
         the replace-mode delete is the lineup.tidy_apply workflow step",
    ),
    (
        "m3u_sources",
        "selected_m3us in streams.toml (the one M3U account selection)",
    ),
    (
        "single_channel_name",
        "a workflow step scope, e.g. {action = \"streams.match_preview\", scope = \
         {groups = \"...\"}}",
    ),
    (
        "test_name",
        "the test_name action's own input (a name to try is not a saved setting)",
    ),
    (
        "csv_retention_days",
        "[exports] retention_days in crosstalk.toml",
    ),
    (
        "csv_export_retention_days",
        "[exports] retention_days in crosstalk.toml",
    ),
    (
        "dry_run_mode",
        "the preview half of each action pair, or --dry-run",
    ),
    (
        "enable_schedule",
        "[schedules] in crosstalk.toml, e.g. \"lineup.sync_apply\" = \"0 6 * * *\"",
    ),
    ("schedule_hours", "[schedules] in crosstalk.toml"),
    ("scheduled_times", "[schedules] in crosstalk.toml"),
    (
        "schedule_window_enabled",
        "[schedules] windows in crosstalk.toml",
    ),
    (
        "auto_match_on_m3u_refresh",
        "the [hooks] m3u_refresh list in crosstalk.toml",
    ),
    (
        "on_m3u_refresh",
        "the [hooks] m3u_refresh list in crosstalk.toml",
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
        "email_report_now",
        "not ported: crosstalk has no notification service",
    ),
    (
        "share_usage_counts",
        "not ported: no telemetry leaves the host",
    ),
];

/// Lineuparr legacy keys that predate the current names, and what each means now.
///
/// Unlike [`MOVED`], these name a key in THIS file, because the old value silently meant
/// something the new code would otherwise misread.
const RETIRED: &[(&str, &str)] = &[
    (
        "ignore_channel_numbers",
        "channel_numbering = \"auto_next\" in this file",
    ),
    ("plugin_status", "the status action"),
    ("clear_csv_exports", "the host \"exports clear\" command"),
    (
        "prioritize_quality",
        "the unified quality model: streams always order by it, no switch",
    ),
    (
        "quality_aware_stream_matching",
        "streams.match (lineup mode, step 19): quality twins are one identity family",
    ),
];

/// A hint naming the new home for every retired or moved setting the file still mentions.
fn moved_settings_hint(text: &str) -> Option<String> {
    let mut moved: Vec<String> = Vec::new();
    for (old, new) in MOVED.iter().chain(RETIRED.iter()) {
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
pub const SECTION_CLEAN: &str = "_section_clean";
pub const SECTION_NAMES: &str = "_section_names";
pub const SECTION_CATEGORIZE: &str = "_section_categorize";
pub const SECTION_LOGOS: &str = "_section_logos";
pub const SECTION_CREATE: &str = "_section_create";
pub const SECTION_SYNC: &str = "_section_sync";

/// The settings form.
#[allow(clippy::too_many_lines)]
pub fn schema() -> SettingsSchema {
    SettingsSchema::new(vec![
        FieldSpec::section(
            SECTION_CLEAN,
            "Clean",
            "Channel Name Cleaner's fixed rule order: pipe and bracket junk first, then the \
             riskier normalizations, each gated by its own switch. Only the first three are on \
             by default, as in the plugin.",
        ),
        FieldSpec::new(
            "strip_pipe_junk",
            SECTION_CLEAN,
            "Strip pipe-delimited junk & quality tags",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "Split on '|', drop empty and quality-only segments. 'Rte One +1 | HD |' -> \
             'Rte One +1'.",
        ),
        FieldSpec::new(
            "strip_bracketed_junk",
            SECTION_CLEAN,
            "Strip bracketed quality/junk tags",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "Remove (1080p), (720p), [VIP], [Multi] etc. — only when the brackets contain \
             nothing but junk tokens, so '(Moldova)' survives.",
        ),
        FieldSpec::new(
            "strip_unicode_quality",
            SECTION_CLEAN,
            "Strip fancy-unicode quality marks",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help("Remove superscript marks like 'ᴴᴰ'. '007 ᴴᴰ' -> '007'."),
        FieldSpec::new(
            "unicode_nfkc",
            SECTION_CLEAN,
            "Unicode NFKC normalize",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "Fold stylised/full-width characters to plain ASCII. OFF by default, as in the \
             plugin.",
        ),
        FieldSpec::new(
            "join_separator",
            SECTION_CLEAN,
            "Join Separator",
            FieldKind::Text,
            FieldValue::Text(" ".to_string()),
        )
        .with_placeholder("a space, or ' - '")
        .with_help(
            "How remaining pipe segments are rejoined. 'Sky Sports+ | Event 03' -> 'Sky \
             Sports+ Event 03' (space) or '... - Event 03'.",
        ),
        FieldSpec::new(
            "normalize_whitespace",
            SECTION_CLEAN,
            "Collapse extra whitespace",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help("Collapse multiple spaces and trim."),
        FieldSpec::new(
            "normalize_plus_one",
            SECTION_CLEAN,
            "Normalize '+1' spacing (risky)",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "'Channel 4+1' -> 'Channel 4 +1'. OFF — it can mangle channels named like \
             '1+1 International'.",
        ),
        FieldSpec::new(
            "fix_casing",
            SECTION_CLEAN,
            "Title-case names (risky)",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help("OFF — it fights broadcaster acronyms (BBC, ITV, RTE)."),
        FieldSpec::new(
            "collision_strategy",
            SECTION_CLEAN,
            "On Collision",
            FieldKind::Text,
            FieldValue::Text("suffix".to_string()),
        )
        .with_placeholder("suffix, numeric or skip")
        .with_help(
            "When a cleaned name would duplicate another channel: 'suffix' keeps a clean \
             quality suffix (Sky Cinema Disney FHD), 'numeric' appends a number (Sky Cinema \
             Disney (2)), 'skip' leaves the original name.",
        ),
        FieldSpec::new(
            "protected_groups",
            SECTION_CLEAN,
            "Protected Groups",
            FieldKind::Text,
            FieldValue::Text("Todays Live Events, Info | Service Status".to_string()),
        )
        .with_placeholder("Group A, Group B")
        .with_help("Comma-separated group names to skip entirely."),
        FieldSpec::new(
            "protect_foreign_scripts",
            SECTION_CLEAN,
            "Protect non-Latin names",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help("Skip names with Cyrillic/Greek/Arabic/Hebrew/Devanagari/CJK/Hangul text."),
        FieldSpec::new(
            "protect_leading_symbol",
            SECTION_CLEAN,
            "Protect names starting with '&'",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help("Skip channels like '&TV', '&flix'."),
        FieldSpec::new(
            "protect_regex",
            SECTION_CLEAN,
            "Custom Protect Regex",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. ^(PPV|Event)\\b")
        .with_help("Any channel name matching this regex is left untouched."),
        FieldSpec::new(
            "custom_rules",
            SECTION_CLEAN,
            "Custom Rules (regex)",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("pattern => replacement (one per line)")
        .with_help("Python regex, one 'regex => replacement' per line. e.g. '\\bRte\\b => RTE'."),
        FieldSpec::new(
            "max_preview_rows",
            SECTION_CLEAN,
            "Max Preview Rows Shown",
            FieldKind::Number,
            FieldValue::Number(400.0),
        )
        .with_help("Rows listed on screen. The full set is always written to the CSV."),
        FieldSpec::section(
            SECTION_NAMES,
            "Names",
            "Channel-Mapparr's naming: the OTA template for broadcast stations, the unknown \
             suffix for channels no database recognises, and the AI gate for borderline \
             premium renames (off by default).",
        ),
        FieldSpec::new(
            "ota_format",
            SECTION_NAMES,
            "OTA Name Format",
            FieldKind::Text,
            FieldValue::Text("{NETWORK} - {STATE} {CITY} ({CALLSIGN})".to_string()),
        )
        .with_help(
            "Template for US over-the-air channel names, from the callsign ladder and the \
             station table. The callsign suffix is stripped.",
        ),
        FieldSpec::new(
            "ota_market_fallback",
            SECTION_NAMES,
            "Match by Market When No Callsign",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help("Fall back to the market when a channel has no callsign."),
        FieldSpec::new(
            "unknown_suffix",
            SECTION_NAMES,
            "Unknown Channel Suffix",
            FieldKind::Text,
            FieldValue::Text(" [Unk]".to_string()),
        )
        .with_placeholder(" [Unk]")
        .with_help(
            "Suffix appended to a channel no database recognises. The leading space is part \
             of the value: without it the name reads 'ESPN[Unk]'.",
        ),
        FieldSpec::new(
            "tag_unmatched_channels",
            SECTION_NAMES,
            "Tag Unmatched Channels",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "Tag channels no database recognises with the suffix above. With it off, \
             unmatched channels are left alone.",
        ),
        FieldSpec::new(
            "ai_gate_enabled",
            SECTION_NAMES,
            "AI Gate for Borderline Renames",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "Ask the [ai.verify] chain about borderline premium renames. Off by default: a \
             borderline rename is left alone rather than guessed.",
        ),
        FieldSpec::section(
            SECTION_CATEGORIZE,
            "Categorize",
            "Channel-Mapparr's organize step: the category group names it manages.",
        ),
        FieldSpec::new(
            "category_groups",
            SECTION_CATEGORIZE,
            "Category Organization Groups",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. News, Sports, Movies")
        .with_help("Category group names the organize step manages. Empty means none."),
        FieldSpec::section(
            SECTION_LOGOS,
            "Logos",
            "The unified logo resolver: Lineuparr's tier order (EPG icon, Logo Manager name, \
             tv-logos) as one source list, Channel-Mapparr's default logo, and whether a \
             channel that already has a logo is left alone.",
        ),
        FieldSpec::new(
            "default_logo",
            SECTION_LOGOS,
            "Default Logo",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_help("Logo assigned to a channel with no better match, by name."),
        FieldSpec::new(
            "logo_sources",
            SECTION_LOGOS,
            "Logo Sources (comma-separated, in order)",
            FieldKind::Text,
            FieldValue::Text("epg, logo_manager, tv-logos, network".to_string()),
        )
        .with_help(
            "Where a logo may come from, in priority order: the EPG icon, the Logo Manager \
             name, tv-logos, then the station's network logo (channels with no logo only).",
        ),
        FieldSpec::new(
            "replace_existing",
            SECTION_LOGOS,
            "Replace Existing Logos",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "Also replace a logo a channel already has when the resolver finds a match. \
             With it off, channels with a logo are left alone.",
        ),
        FieldSpec::new(
            "tv_logos_countries",
            SECTION_LOGOS,
            "tv-logos Countries (comma-separated)",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_help(
            "Country codes whose tv-logos directories the per-channel logo step searches. \
             Empty means the lineup file's own country.",
        ),
        FieldSpec::section(
            SECTION_CREATE,
            "Create",
            "Channel-Mapparr's two creation modes. 'streams' makes one channel per matched \
             stream (and links it); 'stations' makes one channel per OTA station from \
             unattached streams in the source groups, and attaches no streams.",
        ),
        FieldSpec::new(
            "create_mode",
            SECTION_CREATE,
            "Create Mode",
            FieldKind::Text,
            FieldValue::Text("streams".to_string()),
        )
        .with_placeholder("streams or stations")
        .with_help(
            "'streams': one channel per matched stream. 'stations': one channel per OTA \
             station from unattached streams.",
        ),
        FieldSpec::new(
            "m3u_group_filter",
            SECTION_CREATE,
            "M3U Group Filter",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("USA Premium, Sports, Movies")
        .with_help(
            "Comma-separated M3U group titles to import from, applied BEFORE matching. \
             Leave empty to read every group. Which M3U accounts are read is streams' \
             selected_m3us.",
        ),
        FieldSpec::new(
            "m3u_category_filter",
            SECTION_CREATE,
            "Category Filter",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("Broadcast, Sports, News")
        .with_help(
            "Comma-separated category names from the channel database, applied AFTER \
             matching. Leave empty to keep every category.",
        ),
        FieldSpec::new(
            "m3u_custom_group_name",
            SECTION_CREATE,
            "Custom Import Group Name",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("My Custom Group")
        .with_help(
            "Put every imported channel in this one group instead of sorting them into \
             groups by category.",
        ),
        FieldSpec::new(
            "seed_source_groups",
            SECTION_CREATE,
            "Stream Groups to Seed From",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("US| ABC, US| CBS")
        .with_help(
            "Stations mode: comma-separated stream groups to scan. Only streams in these \
             groups that are not attached to any channel are considered. Leave empty to do \
             nothing.",
        ),
        FieldSpec::new(
            "seed_target_group",
            SECTION_CREATE,
            "Channel Group to Create In",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("US: ABC")
        .with_help("Stations mode: the existing channel group the new channels are created in."),
        FieldSpec::new(
            "seed_start_number",
            SECTION_CREATE,
            "First Channel Number to Use",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("5110")
        .with_help(
            "Stations mode: the first channel number to assign. Numbers already in use are \
             skipped. Leave empty to start above the highest channel number on the system.",
        ),
        FieldSpec::new(
            "seed_exclude_networks",
            SECTION_CREATE,
            "Networks to Skip When Creating Channels",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("Telemundo, Univision")
        .with_help(
            "Stations mode: comma-separated network names. A station carrying one of them \
             is reported and skipped rather than created.",
        ),
        FieldSpec::section(
            SECTION_SYNC,
            "Sync",
            "Lineuparr's sync: the provider lineup to mirror, how its categories fold into \
             groups, and how each new channel gets its number. The numbers come from the \
             lineup file itself by default, which is why two lineups can disagree about what \
             channel 206 is.",
        ),
        FieldSpec::new(
            "lineup_file",
            SECTION_SYNC,
            "Lineup File",
            FieldKind::Text,
            FieldValue::Text("US_DirecTV-Premier_lineup.json".to_string()),
        )
        .with_help(
            "The provider channel lineup to mirror. Channels, groups, and numbering are \
             based on this file; its two-letter filename prefix is also what the country \
             filter compares a stream against.",
        ),
        FieldSpec::new(
            "group_prefix",
            SECTION_SYNC,
            "Channel Group Prefix",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("blank = auto  |  none = no prefix  |  e.g. US ")
        .with_help(
            "Blank = auto from the lineup package name. 'none' = no prefix, category names \
             only. Add a trailing separator to control the format (e.g. 'US ' gives 'US \
             News', not 'US: News').",
        ),
        FieldSpec::new(
            "category_detail",
            SECTION_SYNC,
            "Category Detail",
            FieldKind::Text,
            FieldValue::Text("normal".to_string()),
        )
        .with_placeholder("none, refined, simple or normal")
        .with_help(
            "How lineup categories fold into groups. 'refined' merges into 6 broad groups \
             (News, Sports, Movies, Family, Foreign, Entertainment), 'simple' into 7 \
             (News & Info, Sports & Outdoors, etc.), 'none' merges everything into the \
             prefix alone, 'normal' keeps all original categories.",
        ),
        FieldSpec::new(
            "channel_numbering",
            SECTION_SYNC,
            "Channel Numbering",
            FieldKind::Text,
            FieldValue::Text("lineup".to_string()),
        )
        .with_placeholder("lineup, auto_next, auto_highest or specific")
        .with_help(
            "How each new channel gets its number. 'lineup' takes the number from the \
             lineup file (and sets no number when the file has none). The two auto modes \
             fill free slots, from 1 upwards or above the highest existing channel. \
             'specific' counts up from Starting Channel Number below.",
        ),
        FieldSpec::new(
            "starting_channel_number",
            SECTION_SYNC,
            "Starting Channel Number (Specific mode only)",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. 1000")
        .with_help(
            "The first number to use when Channel Numbering is 'specific'. Channels are \
             numbered up from this value, skipping any number already taken. Every other \
             mode ignores this setting.",
        ),
        FieldSpec::new(
            "channel_profiles",
            SECTION_SYNC,
            "Channel Profiles",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("blank = none  |  Living Room, Kids")
        .with_help(
            "Channel profile(s) to enable the synced channels in. Leave blank to leave \
             profiles alone. Separate several with commas; names are not case-sensitive. \
             Whether a named profile exists is only checked at run time, when Dispatcharr \
             can be reached.",
        ),
    ])
}

/// `lineup.toml` -> settings values.
pub fn to_values(settings: &LineupSettings) -> SettingsValues {
    let mut values = SettingsValues::new();
    // One entry per schema text field: a field the form renders but the values drop is
    // a save that silently forgets (pinned by the round-trip test).
    for (id, value) in [
        ("join_separator", &settings.join_separator),
        ("collision_strategy", &settings.collision_strategy),
        ("protected_groups", &settings.protected_groups),
        ("protect_regex", &settings.protect_regex),
        ("custom_rules", &settings.custom_rules),
        ("ota_format", &settings.ota_format),
        ("unknown_suffix", &settings.unknown_suffix),
        ("category_groups", &settings.category_groups),
        ("default_logo", &settings.default_logo),
        ("logo_sources", &settings.logo_sources),
        ("tv_logos_countries", &settings.tv_logos_countries),
        ("create_mode", &settings.create_mode),
        ("m3u_group_filter", &settings.m3u_group_filter),
        ("m3u_category_filter", &settings.m3u_category_filter),
        ("m3u_custom_group_name", &settings.m3u_custom_group_name),
        ("seed_source_groups", &settings.seed_source_groups),
        ("seed_target_group", &settings.seed_target_group),
        ("seed_start_number", &settings.seed_start_number),
        ("seed_exclude_networks", &settings.seed_exclude_networks),
        ("lineup_file", &settings.lineup_file),
        ("group_prefix", &settings.group_prefix),
        ("category_detail", &settings.category_detail),
        ("channel_numbering", &settings.channel_numbering),
        ("starting_channel_number", &settings.starting_channel_number),
        ("channel_profiles", &settings.channel_profiles),
    ] {
        values.insert(id, FieldValue::Text(value.clone()));
    }
    for (id, value) in [
        ("strip_pipe_junk", settings.strip_pipe_junk),
        ("strip_bracketed_junk", settings.strip_bracketed_junk),
        ("strip_unicode_quality", settings.strip_unicode_quality),
        ("unicode_nfkc", settings.unicode_nfkc),
        ("normalize_whitespace", settings.normalize_whitespace),
        ("normalize_plus_one", settings.normalize_plus_one),
        ("fix_casing", settings.fix_casing),
        ("protect_foreign_scripts", settings.protect_foreign_scripts),
        ("protect_leading_symbol", settings.protect_leading_symbol),
        ("ota_market_fallback", settings.ota_market_fallback),
        ("tag_unmatched_channels", settings.tag_unmatched_channels),
        ("ai_gate_enabled", settings.ai_gate_enabled),
        ("replace_existing", settings.replace_existing),
    ] {
        values.insert(id, FieldValue::Boolean(value));
    }
    values.insert(
        "max_preview_rows",
        FieldValue::Number(f64::from(settings.max_preview_rows)),
    );
    values
}

/// Settings values -> `lineup.toml`, keeping anything the form does not know about.
pub fn apply_values(settings: &mut LineupSettings, values: &SettingsValues) -> Result<()> {
    if let Some(text) = values.text("join_separator") {
        settings.join_separator = text.to_string();
    }
    if let Some(text) = values.text("collision_strategy") {
        settings.collision_strategy = text.to_string();
    }
    if let Some(text) = values.text("protected_groups") {
        settings.protected_groups = text.to_string();
    }
    if let Some(text) = values.text("protect_regex") {
        settings.protect_regex = text.to_string();
    }
    if let Some(text) = values.text("custom_rules") {
        settings.custom_rules = text.to_string();
    }
    if let Some(text) = values.text("ota_format") {
        settings.ota_format = text.to_string();
    }
    if let Some(text) = values.text("unknown_suffix") {
        settings.unknown_suffix = text.to_string();
    }
    if let Some(text) = values.text("category_groups") {
        settings.category_groups = text.to_string();
    }
    if let Some(text) = values.text("default_logo") {
        settings.default_logo = text.to_string();
    }
    if let Some(text) = values.text("logo_sources") {
        settings.logo_sources = text.to_string();
    }
    if let Some(text) = values.text("tv_logos_countries") {
        settings.tv_logos_countries = text.to_string();
    }
    if let Some(text) = values.text("create_mode") {
        settings.create_mode = text.to_string();
    }
    if let Some(text) = values.text("m3u_group_filter") {
        settings.m3u_group_filter = text.to_string();
    }
    if let Some(text) = values.text("m3u_category_filter") {
        settings.m3u_category_filter = text.to_string();
    }
    if let Some(text) = values.text("m3u_custom_group_name") {
        settings.m3u_custom_group_name = text.to_string();
    }
    if let Some(text) = values.text("seed_source_groups") {
        settings.seed_source_groups = text.to_string();
    }
    if let Some(text) = values.text("seed_target_group") {
        settings.seed_target_group = text.to_string();
    }
    if let Some(text) = values.text("seed_start_number") {
        settings.seed_start_number = text.to_string();
    }
    if let Some(text) = values.text("seed_exclude_networks") {
        settings.seed_exclude_networks = text.to_string();
    }
    if let Some(text) = values.text("lineup_file") {
        settings.lineup_file = text.to_string();
    }
    if let Some(text) = values.text("group_prefix") {
        settings.group_prefix = text.to_string();
    }
    if let Some(text) = values.text("category_detail") {
        settings.category_detail = text.to_string();
    }
    if let Some(text) = values.text("channel_numbering") {
        settings.channel_numbering = text.to_string();
    }
    if let Some(text) = values.text("starting_channel_number") {
        settings.starting_channel_number = text.to_string();
    }
    if let Some(text) = values.text("channel_profiles") {
        settings.channel_profiles = text.to_string();
    }
    for (id, target) in [
        ("strip_pipe_junk", &mut settings.strip_pipe_junk),
        ("strip_bracketed_junk", &mut settings.strip_bracketed_junk),
        ("strip_unicode_quality", &mut settings.strip_unicode_quality),
        ("unicode_nfkc", &mut settings.unicode_nfkc),
        ("normalize_whitespace", &mut settings.normalize_whitespace),
        ("normalize_plus_one", &mut settings.normalize_plus_one),
        ("fix_casing", &mut settings.fix_casing),
        (
            "protect_foreign_scripts",
            &mut settings.protect_foreign_scripts,
        ),
        (
            "protect_leading_symbol",
            &mut settings.protect_leading_symbol,
        ),
        ("ota_market_fallback", &mut settings.ota_market_fallback),
        (
            "tag_unmatched_channels",
            &mut settings.tag_unmatched_channels,
        ),
        ("ai_gate_enabled", &mut settings.ai_gate_enabled),
        ("replace_existing", &mut settings.replace_existing),
    ] {
        if let Some(value) = values.bool(id) {
            *target = value;
        }
    }
    if let Some(number) = values.number("max_preview_rows") {
        let (min, max) = bounds("max_preview_rows");
        settings.max_preview_rows = whole_number("max_preview_rows", number, min, max)?;
    }
    Ok(())
}

/// Load `lineup.toml`, falling back to defaults when missing.
pub fn load(path: &Path) -> Result<LineupSettings> {
    if !path.exists() {
        return Ok(LineupSettings::default());
    }
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    match toml::from_str::<LineupSettings>(&text) {
        Ok(settings) => Ok(settings),
        Err(error) => {
            // Name the setting's new home rather than only that the key is unknown: the
            // rejection itself is intended, but "unknown key" alone would not say where the
            // value went.
            match moved_settings_hint(&text) {
                Some(hint) => Err(anyhow::anyhow!(
                    "parsing {}: {error}; {hint}",
                    path.display()
                )),
                None => Err(anyhow::Error::new(error))
                    .with_context(|| format!("parsing {}", path.display())),
            }
        }
    }
}

/// Write `lineup.toml` through the comment-preserving writer.
pub fn save(path: &Path, settings: &LineupSettings) -> Result<()> {
    crosstalk_core::config::save_with_comments(path, settings)
}

/// A positive whole channel number, or an error naming the setting.
fn positive_number(field: &str, raw: &str) -> Result<u32> {
    let trimmed = raw.trim();
    match trimmed.parse::<u32>() {
        Ok(number) if number > 0 => Ok(number),
        _ => anyhow::bail!("{field} must be a positive whole number, got {raw:?}"),
    }
}

/// Problems an operator needs to fix, as human-readable strings.
pub fn validate(settings: &LineupSettings) -> Vec<String> {
    let mut errors = Vec::new();
    if !crate::lineup_file::CATEGORY_DETAILS.contains(&settings.category_detail.as_str()) {
        errors.push(format!(
            "category_detail must be one of {}, got {:?}",
            crate::lineup_file::CATEGORY_DETAILS.join(", "),
            settings.category_detail
        ));
    }
    if !crate::lineup_file::NUMBERING_MODES.contains(&settings.channel_numbering.as_str()) {
        errors.push(format!(
            "channel_numbering must be one of {}, got {:?}",
            crate::lineup_file::NUMBERING_MODES.join(", "),
            settings.channel_numbering
        ));
    }
    if !COLLISION_STRATEGIES.contains(&settings.collision_strategy.as_str()) {
        errors.push(format!(
            "collision_strategy must be one of {}, got {:?}",
            COLLISION_STRATEGIES.join(", "),
            settings.collision_strategy
        ));
    }
    if !CREATE_MODES.contains(&settings.create_mode.as_str()) {
        errors.push(format!(
            "create_mode must be one of {}, got {:?}",
            CREATE_MODES.join(", "),
            settings.create_mode
        ));
    }
    if settings.channel_numbering == "specific" {
        if settings.starting_channel_number.trim().is_empty() {
            errors.push(
                "starting_channel_number must be set when channel_numbering is \"specific\""
                    .to_string(),
            );
        } else if let Err(error) =
            positive_number("starting_channel_number", &settings.starting_channel_number)
        {
            errors.push(format!("{error:#}"));
        }
    } else if !settings.starting_channel_number.trim().is_empty() {
        // A typo here is silent in every other mode (LU ignores it), so say so now
        // rather than at the moment the operator switches modes.
        if let Err(error) =
            positive_number("starting_channel_number", &settings.starting_channel_number)
        {
            errors.push(format!("{error:#}"));
        }
    }
    if !settings.seed_start_number.trim().is_empty() {
        if let Err(error) = positive_number("seed_start_number", &settings.seed_start_number) {
            errors.push(format!("{error:#}"));
        }
    }
    let (min, max) = bounds("max_preview_rows");
    if settings.max_preview_rows < min || settings.max_preview_rows > max {
        errors.push(format!(
            "max_preview_rows must be between {min} and {max}, got {}",
            settings.max_preview_rows
        ));
    }
    if let Err(error) = crosstalk_core::logos::LogoSource::parse_order(&settings.logo_sources) {
        errors.push(format!("logo_sources: {error:#}"));
    }
    errors
}

/// Non-fatal observations: valid configurations with a consequence worth naming.
pub fn warnings(settings: &LineupSettings) -> Vec<String> {
    let mut warnings = Vec::new();
    if settings.create_mode == "stations" && settings.seed_source_groups.trim().is_empty() {
        warnings.push(
            "create_mode is \"stations\" with no seed_source_groups: stations mode does nothing"
                .to_string(),
        );
    }
    if settings.channel_numbering != "specific"
        && !settings.starting_channel_number.trim().is_empty()
    {
        warnings.push(
            "starting_channel_number is set but channel_numbering is not \"specific\": it is \
             ignored"
                .to_string(),
        );
    }
    // A country code with no tv-logos directory silently disables the tv-logos
    // tier for that code: say so now rather than at the moment the run finds
    // nothing (a typo like "UKK" would otherwise read as "no match").
    for code in crate::logos::parse_country_codes(&settings.tv_logos_countries) {
        if crosstalk_core::logos::country_dir_for(&code).is_none() {
            warnings.push(format!(
                "tv_logos_countries names {code:?}, which has no tv-logos directory: the \
                 tv-logos tier skips it"
            ));
        }
    }
    warnings
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_ids_are_unique_and_every_field_has_a_section() {
        let schema = schema();
        let mut ids = std::collections::HashSet::new();
        for field in schema.fields() {
            assert!(ids.insert(field.id), "duplicate field id {}", field.id);
        }
        let sections: std::collections::HashSet<&str> =
            schema.sections().map(|section| section.id).collect();
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
    fn every_bound_names_a_real_setting() {
        // A bound for a setting the schema never renders is a typo that no form and no
        // hand-edited file can ever trip over.
        let schema = schema();
        for (id, min, max) in NUMBER_BOUNDS {
            assert!(schema.field(id).is_some(), "{id} is not a schema field");
            assert!(min <= max, "{id} bounds are inverted");
        }
        for field in schema.editable() {
            if field.kind == FieldKind::Number {
                assert!(
                    NUMBER_BOUNDS.iter().any(|(id, _, _)| *id == field.id),
                    "{} is a number with no bounds row",
                    field.id
                );
            }
        }
    }

    #[test]
    fn defaults_validate_clean() {
        assert!(validate(&LineupSettings::default()).is_empty());
    }

    #[test]
    fn defaults_match_the_plugins() {
        // The ports keep the plugins' shipped defaults, except where the plan decides
        // otherwise (the AI gate is crosstalk-only and off; the logo tier order is
        // Lineuparr's).
        let settings = LineupSettings::default();
        assert!(settings.strip_pipe_junk);
        assert!(settings.strip_bracketed_junk);
        assert!(settings.strip_unicode_quality);
        assert!(!settings.unicode_nfkc);
        assert_eq!(settings.join_separator, " ");
        assert!(settings.normalize_whitespace);
        assert!(!settings.normalize_plus_one);
        assert!(!settings.fix_casing);
        assert_eq!(settings.collision_strategy, "suffix");
        assert_eq!(
            settings.protected_groups,
            "Todays Live Events, Info | Service Status"
        );
        assert!(settings.protect_foreign_scripts);
        assert!(settings.protect_leading_symbol);
        assert_eq!(settings.max_preview_rows, 400);
        assert_eq!(
            settings.ota_format,
            "{NETWORK} - {STATE} {CITY} ({CALLSIGN})"
        );
        assert!(!settings.ota_market_fallback);
        assert_eq!(settings.unknown_suffix, " [Unk]");
        assert!(settings.tag_unmatched_channels);
        assert!(!settings.ai_gate_enabled);
        assert_eq!(
            settings.logo_sources,
            "epg, logo_manager, tv-logos, network"
        );
        assert!(!settings.replace_existing);
        assert_eq!(settings.create_mode, "streams");
        assert_eq!(settings.lineup_file, "US_DirecTV-Premier_lineup.json");
        assert_eq!(settings.category_detail, "normal");
        assert_eq!(settings.channel_numbering, "lineup");
    }

    #[test]
    fn validate_catches_an_unknown_enum() {
        for (field, value) in [
            ("category_detail", "ultra"),
            ("channel_numbering", "round_robin"),
            ("collision_strategy", "explode"),
            ("create_mode", "satellites"),
        ] {
            let settings = match field {
                "category_detail" => LineupSettings {
                    category_detail: value.to_string(),
                    ..LineupSettings::default()
                },
                "channel_numbering" => LineupSettings {
                    channel_numbering: value.to_string(),
                    ..LineupSettings::default()
                },
                "collision_strategy" => LineupSettings {
                    collision_strategy: value.to_string(),
                    ..LineupSettings::default()
                },
                _ => LineupSettings {
                    create_mode: value.to_string(),
                    ..LineupSettings::default()
                },
            };
            let errors = validate(&settings);
            assert_eq!(errors.len(), 1, "{field}={value}: {errors:?}");
            assert!(errors[0].contains(field), "{field}={value}: {errors:?}");
        }
    }

    #[test]
    fn specific_numbering_needs_a_positive_start() {
        let settings = LineupSettings {
            channel_numbering: "specific".to_string(),
            ..LineupSettings::default()
        };
        let errors = validate(&settings);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("starting_channel_number"), "{errors:?}");

        for raw in ["0", "-3", "abc", "10.5", ""] {
            let settings = LineupSettings {
                channel_numbering: "specific".to_string(),
                starting_channel_number: raw.to_string(),
                ..LineupSettings::default()
            };
            let errors = validate(&settings);
            assert!(
                errors
                    .iter()
                    .any(|error| error.contains("starting_channel_number")),
                "{raw:?}: {errors:?}"
            );
        }
        let settings = LineupSettings {
            channel_numbering: "specific".to_string(),
            starting_channel_number: "1000".to_string(),
            ..LineupSettings::default()
        };
        assert!(validate(&settings).is_empty());
    }

    #[test]
    fn out_of_range_numbers_fail() {
        for rows in [0, 100_001] {
            let settings = LineupSettings {
                max_preview_rows: rows,
                ..LineupSettings::default()
            };
            let errors = validate(&settings);
            assert_eq!(errors.len(), 1, "{rows}: {errors:?}");
            assert!(errors[0].contains("max_preview_rows"), "{errors:?}");
        }
    }

    #[test]
    fn warnings_name_consequences() {
        let settings = LineupSettings::default();
        assert!(warnings(&settings).is_empty());

        let stations = LineupSettings {
            create_mode: "stations".to_string(),
            ..LineupSettings::default()
        };
        assert!(
            warnings(&stations)
                .iter()
                .any(|w| w.contains("seed_source_groups")),
            "{:?}",
            warnings(&stations)
        );
    }

    #[test]
    fn round_trip_preserves_every_field() {
        let settings = LineupSettings {
            strip_pipe_junk: false,
            collision_strategy: "numeric".to_string(),
            max_preview_rows: 50,
            ota_format: "{CALLSIGN}".to_string(),
            tag_unmatched_channels: false,
            ai_gate_enabled: true,
            category_groups: "News".to_string(),
            logo_sources: "tv-logos".to_string(),
            replace_existing: true,
            create_mode: "stations".to_string(),
            seed_start_number: "5110".to_string(),
            lineup_file: "UK_Freeview_lineup.json".to_string(),
            group_prefix: "none".to_string(),
            category_detail: "simple".to_string(),
            channel_numbering: "specific".to_string(),
            starting_channel_number: "1000".to_string(),
            channel_profiles: "Living Room".to_string(),
            ..LineupSettings::default()
        };

        let values = to_values(&settings);
        // Every schema field round-trips: a field the form renders but the values drop is
        // a save that silently forgets.
        let schema = schema();
        for field in schema.editable() {
            assert!(
                values.get(field.id).is_some(),
                "{} is rendered but never saved",
                field.id
            );
        }

        let mut restored = LineupSettings::default();
        apply_values(&mut restored, &values).expect("applies");
        assert_eq!(restored, settings);
    }

    #[test]
    fn moved_keys_fail_at_load_naming_the_host_file() {
        // The clean break: a value that moved to crosstalk.toml fails loudly instead of
        // running with a setting the run ignores.
        for (old, hint) in [
            ("selected_groups", "crosstalk.toml"),
            ("channel_databases", "[matching]"),
            ("match_sensitivity", "[matching]"),
            ("ignored_tags", "[matching]"),
            ("rate_limiting", "[dispatcharr] concurrency"),
            // Another app's concern: one place per setting.
            ("epg_sources", "guide.toml"),
            ("refresh_epg_after_match", "guide.watchdog_apply"),
            (
                "preserve_existing_streams",
                "overwrite_streams in streams.toml",
            ),
            ("m3u_sources", "selected_m3us in streams.toml"),
            ("single_channel_name", "workflow step scope"),
            ("test_name", "test_name action"),
            ("enable_schedule", "[schedules]"),
            ("notify_enabled", "not ported"),
            ("share_usage_counts", "not ported"),
            ("ignore_channel_numbers", "channel_numbering"),
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("lineup.toml");
            std::fs::write(&path, format!("{old} = \"x\"\n")).expect("seed");
            let error = load(&path).expect_err("rejects the old key");
            assert!(format!("{error:#}").contains(hint), "{old}: {error:#}");
        }
    }
}
