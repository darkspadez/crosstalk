//! `guide` settings: the values, and the form schema the UI and the CSV preamble use.
//!
//! The field ids, labels and help text follow the Python plugin's `plugin.json`, because
//! operators already know those names and the CSV preamble prints them.
//!
//! Only what the guide app decides for itself lives here. The scope filter, the CSV
//! retention and the AI policy are shared with every other sub-app and live in
//! `crosstalk.toml` (`[scope]`, `[exports]`, `[ai.verify]`), which is also where the
//! provider endpoints they refer to already were. `epg_sources_to_match` stays here
//! because only this app has EPG sources to filter.

use anyhow::{Context, Result};
use crosstalk_core::subapp::{
    whole_number, FieldKind, FieldSpec, FieldValue, SettingsSchema, SettingsValues,
};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// The `bad_epg_suffix` default, derived from the shared tag vocabulary
/// (`tags.json` `status`/`BadEPG`, the token guide appends) rather than a literal:
/// step 8 (migration list) moves the literal onto the vocabulary row.
pub fn bad_epg_default() -> String {
    let token =
        crosstalk_matching::tags::tokens_from(crosstalk_matching::tags::TagKind::Status, "guide")
            .into_iter()
            .next()
            .unwrap_or("BadEPG");
    format!(" [{token}]")
}

/// Everything `guide.toml` holds.
///
/// The `Default` impl is written out rather than derived: `#[serde(default)]` fills
/// missing fields from it, so a derived one would silently default every threshold to 0,
/// which is a valid-looking configuration that rejects nothing.
///
/// Step 8 retired `match_confidence_threshold` and `heal_confidence_threshold` (goal
/// reset: ONE threshold in `[matching] sensitivity`): they are gone from the struct, so
/// an old file fails at load with a pointer to the host setting (see [`load`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct GuideSettings {
    pub epg_sources_to_match: String,
    /// How far ahead programme data must exist for a match to count.
    pub check_hours: u32,
    pub allow_epg_without_programs: bool,
    pub heal_fallback_sources: String,
    /// Appended to the name of a channel the scan found without programme data.
    /// The leading space is part of the value, exactly as in the plugin.
    pub bad_epg_suffix: String,
    /// Also unassign the guide when the suffix is added (`add_bad_epg_suffix` with
    /// `remove_epg_with_suffix`).
    pub remove_epg_with_suffix: bool,
    /// The pattern `clear_by_regex` matches against the assigned guide's NAME.
    pub epg_regex_to_remove: String,
    /// A guide ending inside this many hours counts as stale for the watchdog. The plugin's
    /// `watchdog_horizon_threshold_hours`.
    pub watchdog_horizon_threshold_hours: u32,
    /// Comma-separated EPG source ids the watchdog must never touch. The plugin's
    /// `watchdog_exclude_source_ids`.
    pub watchdog_exclude_source_ids: String,
    /// Log every recovery. A failure is always logged. The plugin's
    /// `watchdog_log_on_recovery`.
    pub watchdog_log_on_recovery: bool,
    /// How long the watchdog waits for a queued refresh to move a source. Crosstalk-only: the
    /// plugin's refresh ran inline in the Celery task it was already in, so it had nothing to
    /// wait for. `0` requests the refresh and does not wait.
    pub watchdog_refresh_timeout_minutes: u32,
}

impl Default for GuideSettings {
    fn default() -> Self {
        Self {
            epg_sources_to_match: String::new(),
            // The plugin's shipped defaults.
            check_hours: 12,
            allow_epg_without_programs: false,
            heal_fallback_sources: String::new(),
            // The plugin's shipped default, leading space included — derived from the
            // shared vocabulary's `status`/`BadEPG` row (step 8, migration list).
            bad_epg_suffix: bad_epg_default(),
            remove_epg_with_suffix: false,
            epg_regex_to_remove: String::new(),
            // The plugin's shipped defaults, minus the two the schedule replaces
            // (`watchdog_enabled` and `watchdog_check_interval_hours`).
            watchdog_horizon_threshold_hours: 12,
            watchdog_exclude_source_ids: String::new(),
            watchdog_log_on_recovery: true,
            watchdog_refresh_timeout_minutes: 10,
        }
    }
}

/// Settings that moved to `crosstalk.toml`, as `(what a guide.toml line looks like, where it
/// lives now)`.
///
/// `deny_unknown_fields` already rejects them; this exists so the rejection says where the
/// value went instead of only that the key is unknown. The move is a clean break on purpose:
/// silently accepting the old keys would leave two files claiming the same setting.
const MOVED: [(&str, &str); 5] = [
    ("channel_profile_name", "[scope] profiles in crosstalk.toml"),
    ("selected_groups", "[scope] groups in crosstalk.toml"),
    ("ignore_groups", "[scope] ignore_groups in crosstalk.toml"),
    (
        "csv_retention_days",
        "[exports] retention_days in crosstalk.toml",
    ),
    (
        "ai",
        "[ai.verify] in crosstalk.toml (the provider endpoints stay under [ai.providers])",
    ),
];

/// A hint naming `crosstalk.toml` for every moved setting the file still mentions.
fn moved_settings_hint(text: &str) -> Option<String> {
    let mut moved: Vec<String> = Vec::new();
    for (old, new) in MOVED {
        // One entry per setting, whatever the file says it more than once.
        let names_it = text.lines().map(str::trim).any(|line| {
            !line.starts_with('#')
                && if old == "ai" {
                    // The old policy was a table of its own; a dotted `ai.` key means the
                    // same thing.
                    line.starts_with("[ai]") || line.starts_with("[ai.") || line.starts_with("ai.")
                } else {
                    line.starts_with(old) && line[old.len()..].trim_start().starts_with('=')
                }
        });
        if names_it {
            moved.push(format!("{old} -> {new}"));
        }
    }
    if moved.is_empty() {
        None
    } else {
        Some(format!(
            "settings that moved to the host file: {}",
            moved.join("; ")
        ))
    }
}

/// Settings retired in step 8 (goal reset: one threshold in `[matching] sensitivity`),
/// as `(what a guide.toml line looks like, where the cut lives now)`.
///
/// Unlike [`MOVED`], there is no per-setting host equivalent to point at: the cut is one
/// shared value. The load rejection names it, so an old file fails loudly instead of
/// running at a threshold the run ignores.
const RETIRED: [(&str, &str); 2] = [
    (
        "match_confidence_threshold",
        "[matching] sensitivity in crosstalk.toml",
    ),
    (
        "heal_confidence_threshold",
        "[matching] sensitivity in crosstalk.toml",
    ),
];

/// A hint naming the host setting for every retired threshold the file still mentions.
fn retired_settings_hint(text: &str) -> Option<String> {
    let mut retired: Vec<String> = Vec::new();
    for (old, new) in RETIRED {
        let names_it = text.lines().map(str::trim).any(|line| {
            !line.starts_with('#')
                && line.starts_with(old)
                && line[old.len()..].trim_start().starts_with('=')
        });
        if names_it {
            retired.push(format!("{old} -> {new}"));
        }
    }
    if retired.is_empty() {
        None
    } else {
        Some(format!(
            "retired settings (the one decision cut is the host's; a preset \
             strict/high/medium/low or the old number, e.g. sensitivity = \"95\"): {}",
            retired.join("; ")
        ))
    }
}

/// Section ids, so a field never points at a section that does not exist.
pub const SECTION_SCOPE: &str = "_section_scope";
pub const SECTION_MATCH: &str = "_section_match";
pub const SECTION_HEAL: &str = "_section_heal";
pub const SECTION_CLEANUP: &str = "_section_cleanup";
pub const SECTION_WATCHDOG: &str = "_section_watchdog";

/// The settings form.
pub fn schema() -> SettingsSchema {
    SettingsSchema::new(vec![
        FieldSpec::section(
            SECTION_SCOPE,
            "Scope",
            "Which EPG sources this app may draw candidates from. The channel side of the \
             scope — profiles, groups and ignored groups — is shared with every other \
             sub-app and lives under [scope] in crosstalk.toml.",
        ),
        FieldSpec::new(
            "epg_sources_to_match",
            SECTION_SCOPE,
            "EPG Sources to Match",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("leave empty for all sources")
        .with_help(
            "Comma-separated source names, a filter rather than a priority list. Supports * \
             and ? wildcards. Priority comes from each source's Dispatcharr priority; \
             disabled sources are skipped.",
        ),
        FieldSpec::new(
            "check_hours",
            SECTION_SCOPE,
            "Hours to Check Ahead",
            FieldKind::Number,
            FieldValue::Number(12.0),
        )
        .with_help("Window used to validate that a matched EPG actually has programme data."),
        FieldSpec::section(
            SECTION_MATCH,
            "Auto-Match",
            "Apply Auto-Match is not additive: it OVERWRITES an existing assignment whenever \
             a new match scores above the cut, so a channel already on the right guide \
             can be moved. Preview first and read the CSV. The cut is the ONE threshold \
             every app shares: [matching] sensitivity in crosstalk.toml (guide's own \
             match_confidence_threshold is retired). The AI policy that verifies a \
             borderline score lives under [ai.verify] in crosstalk.toml.",
        ),
        FieldSpec::new(
            "allow_epg_without_programs",
            SECTION_MATCH,
            "Allow EPG Without Program Data",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "When on, auto-match accepts entries with no current schedule. Usually off, but \
             turn it on for the first auto-match against a freshly added EPG source: \
             Dispatcharr only imports programmes for channels already mapped, so a new \
             source starts empty and every match would be rejected.",
        ),
        FieldSpec::section(
            SECTION_HEAL,
            "Scan & Heal",
            "Find channels whose existing assignment carries no programme data and replace it \
             with one that does. Heal only touches assignments it can prove are empty, and \
             it never consults the model. Replacements must clear the same ONE cut as \
             auto-match: [matching] sensitivity in crosstalk.toml (guide's own \
             heal_confidence_threshold is retired).",
        ),
        FieldSpec::new(
            "heal_fallback_sources",
            SECTION_HEAL,
            "Heal Fallback EPG Sources",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. Schedules Direct, xmltv.net")
        .with_help(
            "Sources heal may pick replacements from. Empty means the channel's current \
             source only.",
        ),
        FieldSpec::section(
            SECTION_CLEANUP,
            "Cleanup & Maintenance",
            "The cleanup actions work from the last scan's result set, so run `scan` first. \
             They re-read every channel's current name and guide before writing, so an old \
             scan cannot tag a channel twice or clear a guide that has since been reassigned. \
             Every one refuses to run with the setting it needs left blank.",
        ),
        FieldSpec::new(
            "epg_regex_to_remove",
            SECTION_CLEANUP,
            "EPG Name REGEX to Remove",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. ^XYZ_\\d+$")
        .with_help(
            "A regex, matched case-sensitively and unanchored against the assigned guide's \
             NAME, not the channel's. Channels matching it get their EPG removed by 'Clear EPG \
             by REGEX'. Python's `re` and Rust's `regex` are not identical: backreferences and \
             lookaround are refused with an explanation rather than matching something else.",
        ),
        FieldSpec::new(
            "bad_epg_suffix",
            SECTION_CLEANUP,
            "Bad EPG Suffix",
            FieldKind::Text,
            FieldValue::Text(" [BadEPG]".to_string()),
        )
        .with_help(
            "Appended to the name of every channel the last scan found without programme \
             data. The leading space is part of the suffix: without it the name reads \
             'ESPN[BadEPG]'. A channel that already ends with the suffix is skipped entirely, \
             so its guide survives even when 'Also Remove EPG When Adding Suffix' is on.",
        ),
        FieldSpec::new(
            "remove_epg_with_suffix",
            SECTION_CLEANUP,
            "Also Remove EPG When Adding Suffix",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help(
            "When on, 'Add Bad EPG Suffix to Channels' also strips each tagged channel's EPG \
             assignment in the same pass.",
        ),
        FieldSpec::section(
            SECTION_WATCHDOG,
            "EPG Freshness Watchdog",
            "Audits every active EPG source that has channels mapped to it and refreshes the ones \
             that are errored or whose guide is about to run dry, then records whether that \
             helped. It never touches a channel. There is no on/off switch and no interval here: \
             arm it with a schedule instead, e.g. \"guide.watchdog_apply\" = \"0 */6 * * *\" in \
             crosstalk.toml, and remember a schedule naming watchdog_preview stays a preview.",
        ),
        FieldSpec::new(
            "watchdog_horizon_threshold_hours",
            SECTION_WATCHDOG,
            "Refresh When Guide Ends Within (Hours)",
            FieldKind::Number,
            FieldValue::Number(12.0),
        )
        .with_help(
            "If a source's newest programme ends within this many hours, refresh it now. At \
             least 1: 0 would make every guide stale. An outcome is judged by RELATIVE \
             improvement, so a feed that legitimately ends sooner than this is not reported \
             broken for ever.",
        ),
        FieldSpec::new(
            "watchdog_refresh_timeout_minutes",
            SECTION_WATCHDOG,
            "Refresh Timeout (Minutes)",
            FieldKind::Number,
            FieldValue::Number(10.0),
        )
        .with_help(
            "How long to wait for a queued refresh to move a source before giving up on the \
             wait. A large XMLTV fetch and parse takes minutes. 0 requests the refresh and does \
             not wait (the row then says the refresh did not settle).",
        ),
        FieldSpec::new(
            "watchdog_exclude_source_ids",
            SECTION_WATCHDOG,
            "Excluded EPG Source IDs",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. 39, 21")
        .with_help("Comma-separated EPGSource IDs the watchdog must never touch."),
        FieldSpec::new(
            "watchdog_log_on_recovery",
            SECTION_WATCHDOG,
            "Log Every Recovery",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "Write a log line when the watchdog fixes a source. A failure is always logged, and \
             every source gets a row in the report either way. The plugin called this 'Log a \
             System Event On Self-Heal'; crosstalk has no Dispatcharr System Events.",
        ),
    ])
}

/// `guide.toml` -> settings values.
pub fn to_values(settings: &GuideSettings) -> SettingsValues {
    let mut values = SettingsValues::new();
    values.insert(
        "epg_sources_to_match",
        FieldValue::Text(settings.epg_sources_to_match.clone()),
    );
    values.insert(
        "check_hours",
        FieldValue::Number(f64::from(settings.check_hours)),
    );
    values.insert(
        "allow_epg_without_programs",
        FieldValue::Boolean(settings.allow_epg_without_programs),
    );
    values.insert(
        "heal_fallback_sources",
        FieldValue::Text(settings.heal_fallback_sources.clone()),
    );
    values.insert(
        "bad_epg_suffix",
        FieldValue::Text(settings.bad_epg_suffix.clone()),
    );
    values.insert(
        "remove_epg_with_suffix",
        FieldValue::Boolean(settings.remove_epg_with_suffix),
    );
    values.insert(
        "epg_regex_to_remove",
        FieldValue::Text(settings.epg_regex_to_remove.clone()),
    );
    values.insert(
        "watchdog_horizon_threshold_hours",
        FieldValue::Number(f64::from(settings.watchdog_horizon_threshold_hours)),
    );
    values.insert(
        "watchdog_exclude_source_ids",
        FieldValue::Text(settings.watchdog_exclude_source_ids.clone()),
    );
    values.insert(
        "watchdog_log_on_recovery",
        FieldValue::Boolean(settings.watchdog_log_on_recovery),
    );
    values.insert(
        "watchdog_refresh_timeout_minutes",
        FieldValue::Number(f64::from(settings.watchdog_refresh_timeout_minutes)),
    );
    values
}

/// Settings values -> `guide.toml`, keeping anything the form does not know about.
pub fn apply_values(settings: &mut GuideSettings, values: &SettingsValues) -> Result<()> {
    if let Some(text) = values.text("epg_sources_to_match") {
        settings.epg_sources_to_match = text.to_string();
    }
    if let Some(text) = values.text("heal_fallback_sources") {
        settings.heal_fallback_sources = text.to_string();
    }
    if let Some(text) = values.text("bad_epg_suffix") {
        settings.bad_epg_suffix = text.to_string();
    }
    if let Some(text) = values.text("epg_regex_to_remove") {
        settings.epg_regex_to_remove = text.to_string();
    }
    if let Some(text) = values.text("watchdog_exclude_source_ids") {
        settings.watchdog_exclude_source_ids = text.to_string();
    }
    if let Some(value) = values.bool("remove_epg_with_suffix") {
        settings.remove_epg_with_suffix = value;
    }
    if let Some(value) = values.bool("watchdog_log_on_recovery") {
        settings.watchdog_log_on_recovery = value;
    }
    if let Some(number) = values.number("check_hours") {
        settings.check_hours = whole_number("check_hours", number, 0, 8_760)?;
    }
    if let Some(number) = values.number("watchdog_horizon_threshold_hours") {
        // Floor 1: the plugin's `_INT_FLOORS`, because 0 would make every guide stale.
        settings.watchdog_horizon_threshold_hours =
            whole_number("watchdog_horizon_threshold_hours", number, 1, 8_760)?;
    }
    if let Some(number) = values.number("watchdog_refresh_timeout_minutes") {
        settings.watchdog_refresh_timeout_minutes =
            whole_number("watchdog_refresh_timeout_minutes", number, 0, 1_440)?;
    }
    if let Some(value) = values.bool("allow_epg_without_programs") {
        settings.allow_epg_without_programs = value;
    }
    Ok(())
}

/// Load `guide.toml`, falling back to defaults when missing.
pub fn load(path: &Path) -> Result<GuideSettings> {
    if !path.exists() {
        return Ok(GuideSettings::default());
    }
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    match toml::from_str::<GuideSettings>(&text) {
        Ok(settings) => Ok(settings),
        Err(error) => {
            // Name the file's new home for a value that moved to crosstalk.toml, or the
            // host cut for a retired threshold: the rejection itself is intended, but
            // "unknown key" alone would not say where the setting went.
            let mut hints: Vec<String> = Vec::new();
            hints.extend(moved_settings_hint(&text));
            hints.extend(retired_settings_hint(&text));
            match (!hints.is_empty()).then(|| hints.join("; ")) {
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

/// Write `guide.toml` through the comment-preserving writer.
pub fn save(path: &Path, settings: &GuideSettings) -> Result<()> {
    crosstalk_core::config::save_with_comments(path, settings)
}

/// Problems an operator needs to fix, as human-readable strings.
///
/// Step 8 retired both confidence thresholds (the cut is `[matching] sensitivity` in
/// `crosstalk.toml`), so a parsed file is valid by construction: the retired keys never
/// survive [`load`].
pub fn validate(_settings: &GuideSettings) -> Vec<String> {
    Vec::new()
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

    /// The fields that moved to `crosstalk.toml`, and where they went.
    const MOVED_IDS: [&str; 3] = ["channel_profile_name", "selected_groups", "ignore_groups"];

    #[test]
    fn the_moved_fields_are_no_longer_here() {
        // They are rendered and saved by the host form now; a leftover copy would give an
        // operator two places to edit one setting.
        let schema = schema();
        for id in MOVED_IDS.iter().copied().chain([
            "csv_retention_days",
            "ai.enabled",
            "ai.provider",
            "ai.model",
            // Step 8 (goal reset, D02/T10): one host cut, not two guide thresholds.
            "match_confidence_threshold",
            "heal_confidence_threshold",
        ]) {
            assert!(schema.field(id).is_none(), "{id} is still in guide.toml");
        }
    }

    #[test]
    fn defaults_round_trip_through_values() {
        let settings = GuideSettings::default();
        let values = to_values(&settings);
        let mut rebuilt = GuideSettings::default();
        apply_values(&mut rebuilt, &values).expect("apply");
        assert_eq!(rebuilt, settings);
    }

    #[test]
    fn the_cleanup_defaults_are_the_plugins() {
        // plugin.json: bad_epg_suffix default " [BadEPG]" (leading space), the removal flag
        // off, no REGEX. The leading space is a value, not formatting. Step 8
        // (migration list): the default is derived from the vocabulary's
        // `status`/`BadEPG` row, so it stays the plugin's value via the shared table.
        let settings = GuideSettings::default();
        assert_eq!(settings.bad_epg_suffix, bad_epg_default());
        assert_eq!(settings.bad_epg_suffix, " [BadEPG]");
        assert!(!settings.remove_epg_with_suffix);
        assert!(settings.epg_regex_to_remove.is_empty());
        // Schema defaults agree with the struct's, or the form would show one value and save
        // another.
        let schema = schema();
        assert_eq!(
            schema.field("bad_epg_suffix").expect("field").default,
            FieldValue::Text(" [BadEPG]".to_string())
        );
        assert_eq!(
            schema
                .field("remove_epg_with_suffix")
                .expect("field")
                .default,
            FieldValue::Boolean(false)
        );
        assert_eq!(
            schema.field("epg_regex_to_remove").expect("field").default,
            FieldValue::Text(String::new())
        );
    }

    #[test]
    fn the_watchdog_defaults_are_the_plugins_minus_the_schedule() {
        // plugin.json: watchdog_horizon_threshold_hours 12, exclude list empty, log on recovery
        // on. `watchdog_enabled` and `watchdog_check_interval_hours` are gone: a schedule is the
        // interval, and a schedule naming the preview half stays a preview.
        let settings = GuideSettings::default();
        assert_eq!(settings.watchdog_horizon_threshold_hours, 12);
        assert_eq!(settings.watchdog_exclude_source_ids, "");
        assert!(settings.watchdog_log_on_recovery);
        // Crosstalk-only: the plugin's refresh ran inline, so it waited for nothing.
        assert_eq!(settings.watchdog_refresh_timeout_minutes, 10);

        // Schema defaults agree with the struct's, or the form would show one value and save
        // another.
        let schema = schema();
        for (id, default) in [
            ("watchdog_horizon_threshold_hours", FieldValue::Number(12.0)),
            ("watchdog_refresh_timeout_minutes", FieldValue::Number(10.0)),
            (
                "watchdog_exclude_source_ids",
                FieldValue::Text(String::new()),
            ),
            ("watchdog_log_on_recovery", FieldValue::Boolean(true)),
        ] {
            assert_eq!(schema.field(id).expect("field").default, default, "{id}");
        }
        assert!(schema.field("watchdog_enabled").is_none());
        assert!(schema.field("watchdog_check_interval_hours").is_none());
    }

    #[test]
    fn the_watchdog_threshold_floors_at_one() {
        // The plugin's `_INT_FLOORS`: 0 would make every guide stale. The form refuses it, and
        // a hand-edited file is floored by the run rather than honoured.
        let mut settings = GuideSettings::default();
        let mut values = SettingsValues::new();
        values.insert("watchdog_horizon_threshold_hours", FieldValue::Number(0.0));
        assert!(apply_values(&mut settings, &values).is_err());

        // 0 minutes is allowed: request the refresh and do not wait for it.
        let mut values = SettingsValues::new();
        values.insert("watchdog_refresh_timeout_minutes", FieldValue::Number(0.0));
        apply_values(&mut settings, &values).expect("0 minutes is a valid choice");
        assert_eq!(settings.watchdog_refresh_timeout_minutes, 0);
    }

    #[test]
    fn the_bad_epg_suffix_keeps_its_leading_space_through_a_save() {
        // The suffix is appended verbatim, so a save that trimmed it would rename channels to
        // `ESPN[BadEPG]`.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("guide.toml");
        let settings = GuideSettings::default();
        save(&path, &settings).expect("save");
        let loaded = load(&path).expect("load");
        assert_eq!(loaded.bad_epg_suffix, " [BadEPG]");
    }

    #[test]
    fn every_editable_field_has_a_mapping() {
        // A field with no accessor in `to_values`/`apply_values` would render an empty form
        // control and silently drop the operator's edit.
        let settings = GuideSettings {
            epg_sources_to_match: "C".to_string(),
            check_hours: 5,
            allow_epg_without_programs: true,
            heal_fallback_sources: "D".to_string(),
            bad_epg_suffix: " (no guide)".to_string(),
            remove_epg_with_suffix: true,
            epg_regex_to_remove: "^XYZ_".to_string(),
            watchdog_horizon_threshold_hours: 24,
            watchdog_exclude_source_ids: "39, 21".to_string(),
            watchdog_log_on_recovery: false,
            watchdog_refresh_timeout_minutes: 5,
        };
        let values = to_values(&settings);
        for field in schema().editable() {
            assert!(
                values.get(field.id).is_some(),
                "{} has no value mapping",
                field.id
            );
        }

        let mut rebuilt = GuideSettings::default();
        apply_values(&mut rebuilt, &values).expect("apply");
        assert_eq!(rebuilt, settings);
    }

    #[test]
    fn apply_rejects_out_of_range_numbers() {
        let mut settings = GuideSettings::default();
        let mut values = SettingsValues::new();
        values.insert("check_hours", FieldValue::Number(6.5));
        assert!(apply_values(&mut settings, &values).is_err());
    }

    #[test]
    fn the_retired_thresholds_fail_at_load_with_a_pointer_to_the_host_cut() {
        // Step 8 (goal reset, D02/T10): one threshold in `[matching] sensitivity`. The
        // old keys are unknown fields now, and the error says where the cut lives.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("guide.toml");
        for retired in ["match_confidence_threshold", "heal_confidence_threshold"] {
            std::fs::write(&path, format!("{retired} = 80\n")).expect("seed");
            let error = format!("{:#}", load(&path).expect_err("rejects the retired key"));
            assert!(error.contains(retired), "{error}");
            assert!(error.contains("[matching] sensitivity"), "{error}");
            assert!(error.contains("crosstalk.toml"), "{error}");
        }
        // And they are gone from the form, the values and validation alike.
        let schema = schema();
        assert!(schema.field("match_confidence_threshold").is_none());
        assert!(schema.field("heal_confidence_threshold").is_none());
        assert!(validate(&GuideSettings::default()).is_empty());
    }

    #[test]
    fn save_and_load_preserve_comments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("guide.toml");
        std::fs::write(
            &path,
            "# Chosen to match the Python plugin's defaults\ncheck_hours = 99\n",
        )
        .expect("seed");

        let settings = GuideSettings {
            check_hours: 6,
            ..GuideSettings::default()
        };
        save(&path, &settings).expect("save");
        let text = std::fs::read_to_string(&path).expect("read");
        assert!(text.contains("# Chosen to match"), "{text}");

        let loaded = load(&path).expect("load");
        assert_eq!(loaded.check_hours, 6);
    }

    #[test]
    fn unknown_keys_are_rejected_loudly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("guide.toml");
        std::fs::write(&path, "check_hourss = 99\n").expect("seed");
        let error = load(&path).expect_err("rejects the typo");
        assert!(
            format!("{error:#}").contains("check_hourss"),
            "the error names the key: {error:#}"
        );
    }

    #[test]
    fn a_moved_setting_is_rejected_with_a_pointer_to_the_host_file() {
        // The clean break is intended: two files claiming one setting would be worse than a
        // loud failure. The failure has to say where the value went.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("guide.toml");
        std::fs::write(
            &path,
            "selected_groups = \"Sports\"\ncsv_retention_days = 7\n",
        )
        .expect("seed");
        let error = format!("{:#}", load(&path).expect_err("rejects the old keys"));
        assert!(error.contains("selected_groups"), "{error}");
        assert!(error.contains("[scope] groups"), "{error}");
        assert!(error.contains("crosstalk.toml"), "{error}");
        assert!(error.contains("[exports] retention_days"), "{error}");

        std::fs::write(&path, "[ai]\nenabled = true\n").expect("seed");
        let error = format!("{:#}", load(&path).expect_err("rejects the old table"));
        assert!(error.contains("[ai.verify]"), "{error}");
        assert!(error.contains("crosstalk.toml"), "{error}");
    }

    #[test]
    fn a_missing_file_means_defaults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let loaded = load(&dir.path().join("absent.toml")).expect("load");
        assert_eq!(loaded, GuideSettings::default());
    }
}
