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

/// Everything `guide.toml` holds.
///
/// The `Default` impl is written out rather than derived: `#[serde(default)]` fills
/// missing fields from it, so a derived one would silently default every threshold to 0,
/// which is a valid-looking configuration that rejects nothing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct GuideSettings {
    pub epg_sources_to_match: String,
    /// How far ahead programme data must exist for a match to count.
    pub check_hours: u32,
    pub match_confidence_threshold: u8,
    pub allow_epg_without_programs: bool,
    pub heal_fallback_sources: String,
    pub heal_confidence_threshold: u8,
}

impl Default for GuideSettings {
    fn default() -> Self {
        Self {
            epg_sources_to_match: String::new(),
            // The plugin's shipped defaults.
            check_hours: 12,
            match_confidence_threshold: 95,
            allow_epg_without_programs: false,
            heal_fallback_sources: String::new(),
            heal_confidence_threshold: 95,
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

/// Section ids, so a field never points at a section that does not exist.
pub const SECTION_SCOPE: &str = "_section_scope";
pub const SECTION_MATCH: &str = "_section_match";
pub const SECTION_HEAL: &str = "_section_heal";

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
             a new match scores above the threshold, so a channel already on the right guide \
             can be moved. Preview first and read the CSV. The AI policy that verifies a \
             borderline score lives under [ai.verify] in crosstalk.toml.",
        ),
        FieldSpec::new(
            "match_confidence_threshold",
            SECTION_MATCH,
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
             it never consults the model.",
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
        "match_confidence_threshold",
        FieldValue::Number(f64::from(settings.match_confidence_threshold)),
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
    if let Some(number) = values.number("check_hours") {
        settings.check_hours = whole_number("check_hours", number, 0, 8_760)?;
    }
    if let Some(number) = values.number("match_confidence_threshold") {
        settings.match_confidence_threshold = crosstalk_core::subapp::clamp_u8(number)?;
    }
    if let Some(number) = values.number("heal_confidence_threshold") {
        settings.heal_confidence_threshold = crosstalk_core::subapp::clamp_u8(number)?;
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
            // Name the file's new home for a value that moved to crosstalk.toml: the
            // rejection itself is intended, but "unknown key" alone would not say where the
            // setting went.
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

/// Write `guide.toml` through the comment-preserving writer.
pub fn save(path: &Path, settings: &GuideSettings) -> Result<()> {
    crosstalk_core::config::save_with_comments(path, settings)
}

/// Problems an operator needs to fix, as human-readable strings.
pub fn validate(settings: &GuideSettings) -> Vec<String> {
    let mut errors = Vec::new();
    if settings.match_confidence_threshold == 0 {
        errors.push("match_confidence_threshold must be at least 1".to_string());
    }
    if settings.heal_confidence_threshold == 0 {
        errors.push("heal_confidence_threshold must be at least 1".to_string());
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
    fn every_editable_field_has_a_mapping() {
        // A field with no accessor in `to_values`/`apply_values` would render an empty form
        // control and silently drop the operator's edit.
        let settings = GuideSettings {
            epg_sources_to_match: "C".to_string(),
            check_hours: 5,
            match_confidence_threshold: 70,
            allow_epg_without_programs: true,
            heal_fallback_sources: "D".to_string(),
            heal_confidence_threshold: 75,
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
        values.insert("match_confidence_threshold", FieldValue::Number(120.0));
        assert!(apply_values(&mut settings, &values).is_err());

        let mut values = SettingsValues::new();
        values.insert("check_hours", FieldValue::Number(6.5));
        assert!(apply_values(&mut settings, &values).is_err());
    }

    #[test]
    fn validate_catches_a_zero_threshold() {
        let mut settings = GuideSettings::default();
        assert!(validate(&settings).is_empty());

        settings.match_confidence_threshold = 0;
        settings.heal_confidence_threshold = 0;
        assert_eq!(validate(&settings).len(), 2);
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
