//! `janitor` settings: the values, and the form schema the UI and the CSV preamble use.
//!
//! The field ids, labels and help text follow the Python plugin's `plugin.json`, because
//! operators already know those names and the CSV preamble prints them. The AI block is new
//! and lives here rather than in the host file, because which model verifies a match is a
//! matching decision, not a connection detail.

use anyhow::{Context, Result};
use crosstalk_core::subapp::{FieldKind, FieldSpec, FieldValue, SettingsSchema, SettingsValues};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// How the AI verifier behaves.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct AiSettings {
    /// Ask the model about borderline or unmatched channels.
    pub enabled: bool,
    /// Which entry under `[ai.providers]` in `crosstalk.toml` to use.
    pub provider: String,
    /// Model name, as the provider names it: `jev-latest`, `clef`, and so on.
    pub model: String,
    /// Score band, inclusive, in which a heuristic result is treated as uncertain.
    pub verify_band_low: u8,
    pub verify_band_high: u8,
    /// Ask the model about candidates when no heuristic result cleared the band.
    pub rescue: bool,
    /// Lowest heuristic score worth showing the model during a rescue.
    pub rescue_floor: u8,
    /// How many candidates to put in one request.
    pub shortlist: usize,
    /// Accept a candidate when the model's yes-probability reaches this.
    pub accept_threshold: f64,
    /// Drop a candidate outright at or below this.
    pub reject_threshold: f64,
    /// Minimum choice confidence needed to act on the model's pick.
    pub min_confidence: f64,
}

impl Default for AiSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "typesafe".to_string(),
            model: "jev-latest".to_string(),
            verify_band_low: 80,
            verify_band_high: 94,
            rescue: true,
            rescue_floor: 60,
            shortlist: 5,
            accept_threshold: 0.8,
            reject_threshold: 0.3,
            min_confidence: 0.5,
        }
    }
}

/// Everything `janitor.toml` holds.
///
/// The `Default` impl is written out rather than derived: `#[serde(default)]` fills
/// missing fields from it, so a derived one would silently default every threshold to 0,
/// which is a valid-looking configuration that rejects nothing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct JanitorSettings {
    /// Profile names to scope to, comma-separated. The first one is used by actions that
    /// need exactly one.
    pub channel_profile_name: String,
    pub selected_groups: String,
    pub ignore_groups: String,
    pub epg_sources_to_match: String,
    /// How far ahead programme data must exist for a match to count.
    pub check_hours: u32,
    pub automatch_confidence_threshold: u8,
    pub allow_epg_without_programs: bool,
    pub heal_fallback_sources: String,
    pub heal_confidence_threshold: u8,
    /// Delete this plugin's CSV exports older than this many days. 0 keeps everything.
    pub csv_retention_days: u32,
    pub ai: AiSettings,
}

impl Default for JanitorSettings {
    fn default() -> Self {
        Self {
            channel_profile_name: String::new(),
            selected_groups: String::new(),
            ignore_groups: String::new(),
            epg_sources_to_match: String::new(),
            // The plugin's shipped defaults.
            check_hours: 12,
            automatch_confidence_threshold: 95,
            allow_epg_without_programs: false,
            heal_fallback_sources: String::new(),
            heal_confidence_threshold: 95,
            csv_retention_days: 0,
            ai: AiSettings::default(),
        }
    }
}

/// Section ids, so a field never points at a section that does not exist.
pub const SECTION_SCOPE: &str = "_section_scope";
pub const SECTION_AUTOMATCH: &str = "_section_automatch";
pub const SECTION_HEAL: &str = "_section_heal";
pub const SECTION_AI: &str = "_section_ai";
pub const SECTION_HOUSEKEEPING: &str = "_section_housekeeping";

/// The settings form.
pub fn schema() -> SettingsSchema {
    SettingsSchema::new(vec![
        FieldSpec::section(
            SECTION_SCOPE,
            "Scope",
            "Limit which channels and EPG sources this app touches. Leaving EPG Sources to \
             Match empty does not mean no filter: it makes every active source eligible, \
             foreign-country guides included, and the matcher has no country awareness, so \
             scope this to your own region.",
        ),
        FieldSpec::new(
            "channel_profile_name",
            SECTION_SCOPE,
            "Channel Profile Names",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. All, Favorites")
        .with_help(
            "Comma-separated profile names. Ignored when empty. Actions that need exactly \
             one profile use the first name given.",
        ),
        FieldSpec::new(
            "selected_groups",
            SECTION_SCOPE,
            "Channel Groups",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. DTV: All, DISH: All")
        .with_help(
            "Only process channels in these groups. Empty includes every group. Supports * \
             and ? wildcards, case-insensitively. Cannot be combined with Ignore Groups.",
        ),
        FieldSpec::new(
            "ignore_groups",
            SECTION_SCOPE,
            "Ignore Groups",
            FieldKind::Text,
            FieldValue::Text(String::new()),
        )
        .with_placeholder("e.g. PPV Live Events, 24/7 Streams")
        .with_help(
            "Exclude channels in these groups, after the Channel Groups filter. Supports * \
             and ? wildcards, case-insensitively.",
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
            SECTION_AUTOMATCH,
            "Auto-Match",
            "Apply Auto-Match is not additive: it OVERWRITES an existing assignment whenever \
             a new match scores above the threshold, so a channel already on the right guide \
             can be moved. Preview first and read the CSV.",
        ),
        FieldSpec::new(
            "automatch_confidence_threshold",
            SECTION_AUTOMATCH,
            "Auto-Match Confidence Threshold",
            FieldKind::Number,
            FieldValue::Number(95.0),
        )
        .with_help(
            "0-100. Matches below this score are rejected. 95 is strict; lower values accept \
             more matches at higher false-positive risk.",
        ),
        FieldSpec::new(
            "allow_epg_without_programs",
            SECTION_AUTOMATCH,
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
             with one that does. Heal only touches assignments it can prove are empty.",
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
        FieldSpec::new(
            "heal_confidence_threshold",
            SECTION_HEAL,
            "Heal Confidence Threshold",
            FieldKind::Number,
            FieldValue::Number(95.0),
        )
        .with_help("Minimum score for a replacement EPG to be applied during heal."),
        FieldSpec::section(
            SECTION_AI,
            "AI Verification",
            "The heuristics decide first and the model only promotes: it is asked about \
             results in the uncertain band below, or about candidates for a channel the \
             heuristics could not match at all. A heuristic result at or above the \
             Auto-Match Confidence Threshold is never re-checked or vetoed by the model. \
             An AI-accepted pick is still validated for programme data: when every \
             same-name copy is empty the channel is left unmatched with a reason saying \
             so, and there is no fallback to other candidates. Heal never consults the \
             model. The HOST holds the endpoint and key: see [ai.providers] in \
             crosstalk.toml.",
        ),
        FieldSpec::new(
            "ai.enabled",
            SECTION_AI,
            "Enable AI Verification",
            FieldKind::Boolean,
            FieldValue::Boolean(false),
        )
        .with_help("When off, matching is purely heuristic and no request is ever made."),
        FieldSpec::new(
            "ai.provider",
            SECTION_AI,
            "AI Provider",
            FieldKind::Text,
            FieldValue::Text("typesafe".to_string()),
        )
        .with_placeholder("typesafe")
        .with_help("Name of an [ai.providers.<name>] entry in crosstalk.toml."),
        FieldSpec::new(
            "ai.model",
            SECTION_AI,
            "AI Model",
            FieldKind::Text,
            FieldValue::Text("jev-latest".to_string()),
        )
        .with_placeholder("jev-latest")
        .with_help("Model name as the provider spells it."),
        FieldSpec::new(
            "ai.verify_band_low",
            SECTION_AI,
            "Verify Band Low",
            FieldKind::Number,
            FieldValue::Number(80.0),
        )
        .with_help(
            "A heuristic score in this band, both ends inclusive, is uncertain enough to ask \
             the model about.",
        ),
        FieldSpec::new(
            "ai.verify_band_high",
            SECTION_AI,
            "Verify Band High",
            FieldKind::Number,
            FieldValue::Number(94.0),
        )
        .with_help(
            "Scores above this are not offered to the model. Scores at or above the \
             Auto-Match Confidence Threshold are never offered either, whatever the band \
             says: the model promotes inside the band but never re-checks or vetoes a \
             heuristic win.",
        ),
        FieldSpec::new(
            "ai.rescue",
            SECTION_AI,
            "Rescue Unmatched Channels",
            FieldKind::Boolean,
            FieldValue::Boolean(true),
        )
        .with_help(
            "Offer the model a shortlist when no heuristic result reached the band, but some \
             candidate scored at or above the rescue floor.",
        ),
        FieldSpec::new(
            "ai.rescue_floor",
            SECTION_AI,
            "Rescue Floor",
            FieldKind::Number,
            FieldValue::Number(60.0),
        )
        .with_help("Lowest heuristic score worth showing the model during a rescue."),
        FieldSpec::new(
            "ai.shortlist",
            SECTION_AI,
            "Shortlist Size",
            FieldKind::Number,
            FieldValue::Number(5.0),
        )
        .with_help("How many candidates go into one request. More candidates cost tokens."),
        FieldSpec::new(
            "ai.accept_threshold",
            SECTION_AI,
            "AI Accept Threshold",
            FieldKind::Number,
            FieldValue::Number(0.8),
        )
        .with_help(
            "0-1. A candidate is accepted when the model's yes-probability reaches this and \
             its choice agrees. An accepted pick is still validated for programme data: \
             when every same-name copy is empty the channel is left unmatched and the \
             reason says so.",
        ),
        FieldSpec::new(
            "ai.reject_threshold",
            SECTION_AI,
            "AI Reject Threshold",
            FieldKind::Number,
            FieldValue::Number(0.3),
        )
        .with_help("0-1. At or below this the candidate is dropped outright."),
        FieldSpec::new(
            "ai.min_confidence",
            SECTION_AI,
            "AI Minimum Confidence",
            FieldKind::Number,
            FieldValue::Number(0.5),
        )
        .with_help(
            "0-1. Below this the model's pick is treated as undecided and the channel is left \
             unmatched.",
        ),
        FieldSpec::section(
            SECTION_HOUSEKEEPING,
            "Housekeeping",
            "Age-based tidying of the CSV files this app writes. Off unless a number of days \
             is set, and the file a run has just written is always kept.",
        ),
        FieldSpec::new(
            "csv_retention_days",
            SECTION_HOUSEKEEPING,
            "Delete CSV Exports Older Than (Days)",
            FieldKind::Number,
            FieldValue::Number(0.0),
        )
        .with_help(
            "0 keeps every file, which is the default. Only files named epg_janitor_*.csv are \
             touched, because the exports directory is shared with other apps.",
        ),
    ])
}

/// `janitor.toml` -> settings values.
pub fn to_values(settings: &JanitorSettings) -> SettingsValues {
    let mut values = SettingsValues::new();
    values.insert(
        "channel_profile_name",
        FieldValue::Text(settings.channel_profile_name.clone()),
    );
    values.insert(
        "selected_groups",
        FieldValue::Text(settings.selected_groups.clone()),
    );
    values.insert(
        "ignore_groups",
        FieldValue::Text(settings.ignore_groups.clone()),
    );
    values.insert(
        "epg_sources_to_match",
        FieldValue::Text(settings.epg_sources_to_match.clone()),
    );
    values.insert(
        "check_hours",
        FieldValue::Number(f64::from(settings.check_hours)),
    );
    values.insert(
        "automatch_confidence_threshold",
        FieldValue::Number(f64::from(settings.automatch_confidence_threshold)),
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
        "heal_confidence_threshold",
        FieldValue::Number(f64::from(settings.heal_confidence_threshold)),
    );
    values.insert(
        "csv_retention_days",
        FieldValue::Number(f64::from(settings.csv_retention_days)),
    );
    values.insert("ai.enabled", FieldValue::Boolean(settings.ai.enabled));
    values.insert(
        "ai.provider",
        FieldValue::Text(settings.ai.provider.clone()),
    );
    values.insert("ai.model", FieldValue::Text(settings.ai.model.clone()));
    values.insert(
        "ai.verify_band_low",
        FieldValue::Number(f64::from(settings.ai.verify_band_low)),
    );
    values.insert(
        "ai.verify_band_high",
        FieldValue::Number(f64::from(settings.ai.verify_band_high)),
    );
    values.insert("ai.rescue", FieldValue::Boolean(settings.ai.rescue));
    values.insert(
        "ai.rescue_floor",
        FieldValue::Number(f64::from(settings.ai.rescue_floor)),
    );
    values.insert(
        "ai.shortlist",
        FieldValue::Number(settings.ai.shortlist as f64),
    );
    values.insert(
        "ai.accept_threshold",
        FieldValue::Number(settings.ai.accept_threshold),
    );
    values.insert(
        "ai.reject_threshold",
        FieldValue::Number(settings.ai.reject_threshold),
    );
    values.insert(
        "ai.min_confidence",
        FieldValue::Number(settings.ai.min_confidence),
    );
    values
}

/// Settings values -> `janitor.toml`, keeping anything the form does not know about.
pub fn apply_values(settings: &mut JanitorSettings, values: &SettingsValues) -> Result<()> {
    if let Some(text) = values.text("channel_profile_name") {
        settings.channel_profile_name = text.to_string();
    }
    if let Some(text) = values.text("selected_groups") {
        settings.selected_groups = text.to_string();
    }
    if let Some(text) = values.text("ignore_groups") {
        settings.ignore_groups = text.to_string();
    }
    if let Some(text) = values.text("epg_sources_to_match") {
        settings.epg_sources_to_match = text.to_string();
    }
    if let Some(text) = values.text("heal_fallback_sources") {
        settings.heal_fallback_sources = text.to_string();
    }
    if let Some(number) = values.number("check_hours") {
        settings.check_hours = whole_number("check_hours", number, 0, 8_760)?;
    }
    if let Some(number) = values.number("automatch_confidence_threshold") {
        settings.automatch_confidence_threshold = clamp_u8(number)?;
    }
    if let Some(number) = values.number("heal_confidence_threshold") {
        settings.heal_confidence_threshold = clamp_u8(number)?;
    }
    if let Some(number) = values.number("csv_retention_days") {
        settings.csv_retention_days = whole_number("csv_retention_days", number, 0, 3_650)?;
    }
    if let Some(value) = values.bool("allow_epg_without_programs") {
        settings.allow_epg_without_programs = value;
    }
    if let Some(value) = values.bool("ai.enabled") {
        settings.ai.enabled = value;
    }
    if let Some(value) = values.bool("ai.rescue") {
        settings.ai.rescue = value;
    }
    if let Some(text) = values.text("ai.provider") {
        settings.ai.provider = text.to_string();
    }
    if let Some(text) = values.text("ai.model") {
        settings.ai.model = text.to_string();
    }
    if let Some(number) = values.number("ai.verify_band_low") {
        settings.ai.verify_band_low = clamp_u8(number)?;
    }
    if let Some(number) = values.number("ai.verify_band_high") {
        settings.ai.verify_band_high = clamp_u8(number)?;
    }
    if let Some(number) = values.number("ai.rescue_floor") {
        settings.ai.rescue_floor = clamp_u8(number)?;
    }
    if let Some(number) = values.number("ai.shortlist") {
        settings.ai.shortlist = whole_number("ai.shortlist", number, 1, 255)? as usize;
    }
    if let Some(number) = values.number("ai.accept_threshold") {
        settings.ai.accept_threshold = clamp_unit(number)?;
    }
    if let Some(number) = values.number("ai.reject_threshold") {
        settings.ai.reject_threshold = clamp_unit(number)?;
    }
    if let Some(number) = values.number("ai.min_confidence") {
        settings.ai.min_confidence = clamp_unit(number)?;
    }
    Ok(())
}

/// A number that must already be a whole number in range.
///
/// Truncating silently is how a typed `6.7` becomes `6` and a typed `-1` becomes `0`; both
/// look like a saved setting and neither is what was asked for.
fn whole_number(field: &str, value: f64, min: u32, max: u32) -> Result<u32> {
    if !value.is_finite() || value.fract() != 0.0 {
        anyhow::bail!("{field} must be a whole number, got {value}");
    }
    if value < f64::from(min) || value > f64::from(max) {
        anyhow::bail!("{field} must be between {min} and {max}, got {value}");
    }
    Ok(value as u32)
}

fn clamp_u8(value: f64) -> Result<u8> {
    if !(0.0..=100.0).contains(&value) {
        anyhow::bail!("value {value} is outside 0-100");
    }
    Ok(value.round() as u8)
}

fn clamp_unit(value: f64) -> Result<f64> {
    if !(0.0..=1.0).contains(&value) {
        anyhow::bail!("value {value} is outside 0-1");
    }
    Ok(value)
}

/// Load `janitor.toml`, falling back to defaults when missing.
pub fn load(path: &Path) -> Result<JanitorSettings> {
    if !path.exists() {
        return Ok(JanitorSettings::default());
    }
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Write `janitor.toml` through the comment-preserving writer.
pub fn save(path: &Path, settings: &JanitorSettings) -> Result<()> {
    crosstalk_core::config::save_with_comments(path, settings)
}

/// Problems an operator needs to fix, as human-readable strings.
pub fn validate(settings: &JanitorSettings) -> Vec<String> {
    let mut errors = Vec::new();
    if !settings.selected_groups.trim().is_empty() && !settings.ignore_groups.trim().is_empty() {
        errors.push(
            "selected_groups and ignore_groups cannot both be set: use one or the other"
                .to_string(),
        );
    }
    if settings.ai.verify_band_low > settings.ai.verify_band_high {
        errors.push(format!(
            "ai.verify_band_low ({}) is above ai.verify_band_high ({})",
            settings.ai.verify_band_low, settings.ai.verify_band_high
        ));
    }
    if settings.ai.reject_threshold >= settings.ai.accept_threshold {
        errors.push(format!(
            "ai.reject_threshold ({}) must be below ai.accept_threshold ({})",
            settings.ai.reject_threshold, settings.ai.accept_threshold
        ));
    }
    if settings.automatch_confidence_threshold == 0 {
        errors.push("automatch_confidence_threshold must be at least 1".to_string());
    }
    if settings.ai.rescue_floor > settings.ai.verify_band_low {
        errors.push(format!(
            "ai.rescue_floor ({}) is above ai.verify_band_low ({}), so a rescued channel \
             would be one the band already covers",
            settings.ai.rescue_floor, settings.ai.verify_band_low
        ));
    }
    if settings.ai.shortlist == 0 {
        errors.push("ai.shortlist must be at least 1".to_string());
    }
    if settings.heal_confidence_threshold == 0 {
        errors.push("heal_confidence_threshold must be at least 1".to_string());
    }
    if settings.ai.enabled && settings.ai.provider.trim().is_empty() {
        errors.push("ai.enabled is on but ai.provider is empty".to_string());
    }
    if settings.ai.enabled && settings.ai.model.trim().is_empty() {
        errors.push("ai.enabled is on but ai.model is empty".to_string());
    }
    errors
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
    fn defaults_round_trip_through_values() {
        let settings = JanitorSettings::default();
        let values = to_values(&settings);
        let mut rebuilt = JanitorSettings::default();
        apply_values(&mut rebuilt, &values).expect("apply");
        assert_eq!(rebuilt, settings);
    }

    #[test]
    fn every_editable_field_has_a_mapping() {
        // A field with no accessor in `to_values`/`apply_values` would render an empty form
        // control and silently drop the operator's edit.
        let settings = JanitorSettings {
            channel_profile_name: "All".to_string(),
            selected_groups: "A".to_string(),
            ignore_groups: "B".to_string(),
            epg_sources_to_match: "C".to_string(),
            check_hours: 5,
            automatch_confidence_threshold: 70,
            allow_epg_without_programs: true,
            heal_fallback_sources: "D".to_string(),
            heal_confidence_threshold: 75,
            csv_retention_days: 3,
            ai: AiSettings {
                enabled: true,
                provider: "p".to_string(),
                model: "m".to_string(),
                verify_band_low: 50,
                verify_band_high: 60,
                rescue: false,
                rescue_floor: 40,
                shortlist: 3,
                accept_threshold: 0.9,
                reject_threshold: 0.1,
                min_confidence: 0.6,
            },
        };
        let values = to_values(&settings);
        for field in schema().editable() {
            assert!(
                values.get(field.id).is_some(),
                "{} has no value mapping",
                field.id
            );
        }

        let mut rebuilt = JanitorSettings::default();
        apply_values(&mut rebuilt, &values).expect("apply");
        assert_eq!(rebuilt, settings);
    }

    #[test]
    fn apply_rejects_out_of_range_numbers() {
        let mut settings = JanitorSettings::default();
        let mut values = SettingsValues::new();
        values.insert("automatch_confidence_threshold", FieldValue::Number(120.0));
        assert!(apply_values(&mut settings, &values).is_err());

        let mut values = SettingsValues::new();
        values.insert("ai.accept_threshold", FieldValue::Number(1.5));
        assert!(apply_values(&mut settings, &values).is_err());
    }

    #[test]
    fn validate_catches_conflicting_and_inverted_settings() {
        let mut settings = JanitorSettings::default();
        assert!(validate(&settings).is_empty());

        settings.selected_groups = "A".to_string();
        settings.ignore_groups = "B".to_string();
        settings.ai.verify_band_low = 95;
        settings.ai.verify_band_high = 80;
        settings.ai.reject_threshold = 0.9;
        settings.ai.accept_threshold = 0.5;
        settings.automatch_confidence_threshold = 0;
        let errors = validate(&settings);
        assert_eq!(errors.len(), 4, "{errors:?}");
    }

    #[test]
    fn save_and_load_preserve_comments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("janitor.toml");
        std::fs::write(
            &path,
            "# Chosen to match the Python plugin's defaults\ncheck_hours = 99\n",
        )
        .expect("seed");

        let settings = JanitorSettings {
            check_hours: 6,
            ..JanitorSettings::default()
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
        let path = dir.path().join("janitor.toml");
        std::fs::write(&path, "check_hourss = 99\n").expect("seed");
        let error = load(&path).expect_err("rejects the typo");
        assert!(
            format!("{error:#}").contains("check_hourss"),
            "the error names the key: {error:#}"
        );

        std::fs::write(&path, "[ai]\nenabeld = true\n").expect("seed");
        let error = load(&path).expect_err("rejects the typo");
        assert!(
            format!("{error:#}").contains("enabeld"),
            "the error names the key: {error:#}"
        );
    }

    #[test]
    fn a_missing_file_means_defaults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let loaded = load(&dir.path().join("absent.toml")).expect("load");
        assert_eq!(loaded, JanitorSettings::default());
    }

    #[test]
    fn ai_defaults_match_the_plan() {
        let ai = AiSettings::default();
        assert!(!ai.enabled);
        assert_eq!(ai.verify_band_low, 80);
        assert_eq!(ai.verify_band_high, 94);
        assert!(ai.rescue);
        assert_eq!(ai.rescue_floor, 60);
        assert_eq!(ai.shortlist, 5);
        assert_eq!(ai.accept_threshold, 0.8);
        assert_eq!(ai.reject_threshold, 0.3);
        assert_eq!(ai.min_confidence, 0.5);
    }
}
