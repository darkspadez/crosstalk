//! Sub-apps: the contract every crosstalk tool implements.
//!
//! A sub-app is a crate compiled into the daemon. It declares its actions, the settings
//! form it wants the web UI to render, and how to run each action. The host supplies the
//! things a sub-app must not own itself: the Dispatcharr connection, the AI providers, the
//! thread pool, the run store and the progress channel.
//!
//! Why a trait and a static registry rather than dynamic loading: Rust has no stable ABI,
//! so a `.so` plugin would need a C shim and an exact toolchain match. Compiling every
//! sub-app in keeps one type-checked binary, and adding one is a crate plus a line in
//! [`Registry::builtin`].
//!
//! The action kinds are the plugin's button colours, preserved because they tell an
//! operator the consequence before they press anything: blue reads, green writes no channel
//! data, orange writes channel data but removes no assignment, red can remove an assignment
//! and always confirms.

use crate::config::CrosstalkConfig;
use crate::jobs::JobContext;
use anyhow::Result;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// What pressing an action can do to the installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    /// Reads only. The plugin's blue.
    Read,
    /// Writes files the plugin owns, never channel data. The plugin's green.
    LocalWrite,
    /// Writes channel data but removes no assignment. The plugin's orange.
    Write,
    /// Can remove a guide assignment. The plugin's red, and always confirms.
    Destructive,
}

impl ActionKind {
    /// CSS class the UI uses, so a colour never has to be remembered at a call site.
    pub fn css_class(&self) -> &'static str {
        match self {
            ActionKind::Read => "action-read",
            ActionKind::LocalWrite => "action-local",
            ActionKind::Write => "action-write",
            ActionKind::Destructive => "action-destructive",
        }
    }

    /// Whether the UI must ask before running it.
    pub fn requires_confirmation(&self) -> bool {
        matches!(self, ActionKind::Destructive | ActionKind::Write)
    }

    /// Whether the action may change channel data at all.
    pub fn touches_channels(&self) -> bool {
        matches!(self, ActionKind::Write | ActionKind::Destructive)
    }
}

/// One runnable action.
#[derive(Debug, Clone)]
pub struct ActionSpec {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub kind: ActionKind,
    /// Extra sentence shown in the confirmation dialog.
    pub confirm: Option<&'static str>,
}

impl ActionSpec {
    pub fn new(
        id: &'static str,
        label: &'static str,
        description: &'static str,
        kind: ActionKind,
    ) -> Self {
        Self {
            id,
            label,
            description,
            kind,
            confirm: None,
        }
    }

    pub fn with_confirm(mut self, message: &'static str) -> Self {
        self.confirm = Some(message);
        self
    }
}

/// The value type of one setting, which decides the form control.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldKind {
    Text,
    Number,
    Boolean,
    /// A JSON object typed into a textarea.
    JsonObject,
    /// A non-editable explanatory block.
    Info,
}

/// One setting, as the UI form and the CSV preamble need it.
#[derive(Debug, Clone)]
pub struct FieldSpec {
    pub id: &'static str,
    /// Which form section it belongs to; `None` for section headers.
    pub section: Option<&'static str>,
    pub label: &'static str,
    pub help_text: &'static str,
    pub kind: FieldKind,
    pub default: FieldValue,
    pub placeholder: Option<&'static str>,
}

impl FieldSpec {
    /// A section header block.
    pub fn section(id: &'static str, label: &'static str, help_text: &'static str) -> Self {
        Self {
            id,
            section: None,
            label,
            help_text,
            kind: FieldKind::Info,
            default: FieldValue::Text(String::new()),
            placeholder: None,
        }
    }

    pub fn new(
        id: &'static str,
        section: &'static str,
        label: &'static str,
        kind: FieldKind,
        default: FieldValue,
    ) -> Self {
        Self {
            id,
            section: Some(section),
            label,
            help_text: "",
            kind,
            default,
            placeholder: None,
        }
    }

    pub fn with_help(mut self, help_text: &'static str) -> Self {
        self.help_text = help_text;
        self
    }

    pub fn with_placeholder(mut self, placeholder: &'static str) -> Self {
        self.placeholder = Some(placeholder);
        self
    }

    /// Whether this field is a section header.
    pub fn is_section(&self) -> bool {
        self.section.is_none() && self.kind == FieldKind::Info
    }
}

/// A setting's value, in the shape the form and the CSV report use.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Text(String),
    Number(f64),
    Boolean(bool),
    Json(BTreeMap<String, Vec<String>>),
}

impl FieldValue {
    /// How the value reads to a person in the CSV preamble.
    pub fn render(&self) -> String {
        match self {
            FieldValue::Text(text) => text.clone(),
            FieldValue::Number(number) => {
                if number.fract() == 0.0 {
                    format!("{number:.0}")
                } else {
                    number.to_string()
                }
            }
            FieldValue::Boolean(value) => {
                if *value {
                    "Yes".to_string()
                } else {
                    "No".to_string()
                }
            }
            FieldValue::Json(map) => {
                if map.is_empty() {
                    "(not set)".to_string()
                } else {
                    format!("{} custom alias key(s)", map.len())
                }
            }
        }
    }
}

/// The settings form a sub-app declares.
#[derive(Debug, Clone, Default)]
pub struct SettingsSchema {
    fields: Vec<FieldSpec>,
}

impl SettingsSchema {
    pub fn new(fields: Vec<FieldSpec>) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &[FieldSpec] {
        &self.fields
    }

    pub fn field(&self, id: &str) -> Option<&FieldSpec> {
        self.fields.iter().find(|field| field.id == id)
    }

    /// Fields that can be edited, in declaration order.
    pub fn editable(&self) -> impl Iterator<Item = &FieldSpec> {
        self.fields.iter().filter(|field| !field.is_section())
    }

    /// Section headers, in declaration order.
    pub fn sections(&self) -> impl Iterator<Item = &FieldSpec> {
        self.fields.iter().filter(|field| field.is_section())
    }

    /// Check a set of submitted values against the schema.
    ///
    /// Unknown ids are rejected rather than ignored: a typo in a form field would otherwise
    /// look like a saved setting.
    pub fn validate(&self, values: &SettingsValues) -> Vec<String> {
        let mut errors = Vec::new();
        for (id, value) in values.iter() {
            let Some(field) = self.field(id) else {
                errors.push(format!("unknown setting {id:?}"));
                continue;
            };
            let matches_kind = matches!(
                (&field.kind, value),
                (FieldKind::Text, FieldValue::Text(_))
                    | (FieldKind::Number, FieldValue::Number(_))
                    | (FieldKind::Boolean, FieldValue::Boolean(_))
                    | (FieldKind::JsonObject, FieldValue::Json(_))
            );
            if !matches_kind {
                errors.push(format!(
                    "setting {id:?} expects {:?}, got {:?}",
                    field.kind, value
                ));
            }
        }
        errors
    }

    /// Fill in defaults for any setting the caller did not supply.
    pub fn with_defaults(&self, values: &SettingsValues) -> SettingsValues {
        let mut filled = values.clone();
        for field in self.editable() {
            filled
                .0
                .entry(field.id.to_string())
                .or_insert_with(|| field.default.clone());
        }
        filled
    }
}

/// An ordered set of setting values.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SettingsValues(BTreeMap<String, FieldValue>);

impl SettingsValues {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, id: impl Into<String>, value: FieldValue) -> &mut Self {
        self.0.insert(id.into(), value);
        self
    }

    pub fn get(&self, id: &str) -> Option<&FieldValue> {
        self.0.get(id)
    }

    pub fn text(&self, id: &str) -> Option<&str> {
        match self.0.get(id) {
            Some(FieldValue::Text(text)) => Some(text),
            _ => None,
        }
    }

    pub fn number(&self, id: &str) -> Option<f64> {
        match self.0.get(id) {
            Some(FieldValue::Number(number)) => Some(*number),
            _ => None,
        }
    }

    pub fn number_or(&self, id: &str, fallback: f64) -> f64 {
        self.number(id).unwrap_or(fallback)
    }

    pub fn bool(&self, id: &str) -> Option<bool> {
        match self.0.get(id) {
            Some(FieldValue::Boolean(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn bool_or(&self, id: &str, fallback: bool) -> bool {
        self.bool(id).unwrap_or(fallback)
    }

    pub fn json(&self, id: &str) -> Option<&BTreeMap<String, Vec<String>>> {
        match self.0.get(id) {
            Some(FieldValue::Json(map)) => Some(map),
            _ => None,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &FieldValue)> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// How one action finished, for the run record and the toast.
#[derive(Debug, Clone)]
pub struct JobOutcome {
    pub summary: String,
    /// Machine-readable counts, shown in the UI and stored with the run.
    pub counts: BTreeMap<String, i64>,
    /// CSV written by the action, if any.
    pub report_path: Option<PathBuf>,
}

impl JobOutcome {
    pub fn new(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            counts: BTreeMap::new(),
            report_path: None,
        }
    }

    pub fn with_count(mut self, key: impl Into<String>, value: i64) -> Self {
        self.counts.insert(key.into(), value);
        self
    }

    pub fn with_report(mut self, path: impl Into<PathBuf>) -> Self {
        self.report_path = Some(path.into());
        self
    }
}

/// A crosstalk sub-app.
pub trait SubApp: Send + Sync {
    /// Stable id, used in URLs, schedule keys and the CLI.
    fn id(&self) -> &'static str;

    /// Display name.
    fn name(&self) -> &'static str;

    /// One-line description for the dashboard.
    fn description(&self) -> &'static str;

    /// Every action this app offers.
    fn actions(&self) -> Vec<ActionSpec>;

    /// The settings form and the CSV preamble both come from this.
    fn schema(&self) -> &SettingsSchema;

    /// Load this app's settings file. A missing file yields the schema defaults.
    fn load_settings(&self, path: &Path) -> Result<SettingsValues>;

    /// Where this app's settings file lives, given the host config directory.
    fn settings_path(&self, config_dir: &Path) -> PathBuf {
        config_dir.join(format!("{}.toml", self.id()))
    }

    /// Write settings back, preserving comments.
    fn save_settings(&self, path: &Path, values: &SettingsValues) -> Result<()>;

    /// Run one action. `dry_run` is set for a preview.
    fn run<'a>(
        &'a self,
        action: &'a str,
        context: &'a JobContext,
        dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>>;
}

/// The set of sub-apps this binary was built with.
#[derive(Clone, Default)]
pub struct Registry {
    apps: Vec<Arc<dyn SubApp>>,
}

impl Registry {
    pub fn new(apps: Vec<Arc<dyn SubApp>>) -> Self {
        Self { apps }
    }

    pub fn get(&self, id: &str) -> Option<&Arc<dyn SubApp>> {
        self.apps.iter().find(|app| app.id() == id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn SubApp>> {
        self.apps.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.apps.is_empty()
    }

    /// Locate an action by `<app>/<action>` or `<app>.<action>`.
    pub fn find_action(&self, app: &str, action: &str) -> Option<(&Arc<dyn SubApp>, ActionSpec)> {
        let app = self.get(app)?;
        let spec = app.actions().into_iter().find(|entry| entry.id == action)?;
        Some((app, spec))
    }

    /// Resolve the schedule keys against the registered apps, reporting any that name an
    /// action which does not exist. A typo in a schedule would otherwise just never run.
    pub fn unknown_schedules(&self, config: &CrosstalkConfig) -> Vec<String> {
        let mut unknown = Vec::new();
        for key in config.schedules.keys() {
            let Some((app, action)) = key.split_once('.') else {
                unknown.push(key.clone());
                continue;
            };
            if self.find_action(app, action).is_none() {
                unknown.push(key.clone());
            }
        }
        unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_kinds_carry_the_plugin_colours() {
        assert_eq!(ActionKind::Read.css_class(), "action-read");
        assert!(!ActionKind::Read.requires_confirmation());
        assert!(!ActionKind::Read.touches_channels());

        assert!(!ActionKind::LocalWrite.touches_channels());
        assert!(!ActionKind::LocalWrite.requires_confirmation());

        assert!(ActionKind::Write.touches_channels());
        assert!(ActionKind::Write.requires_confirmation());

        assert!(ActionKind::Destructive.touches_channels());
        assert!(ActionKind::Destructive.requires_confirmation());
    }

    #[test]
    fn schema_validates_unknown_and_mistyped_values() {
        let schema = SettingsSchema::new(vec![
            FieldSpec::section("_section_a", "Section", "help"),
            FieldSpec::new(
                "threshold",
                "_section_a",
                "Threshold",
                FieldKind::Number,
                FieldValue::Number(95.0),
            ),
            FieldSpec::new(
                "enabled",
                "_section_a",
                "Enabled",
                FieldKind::Boolean,
                FieldValue::Boolean(true),
            ),
        ]);

        let mut values = SettingsValues::new();
        values.insert("threshold", FieldValue::Number(90.0));
        assert!(schema.validate(&values).is_empty());

        values.insert("nope", FieldValue::Text("x".into()));
        let errors = schema.validate(&values);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("unknown setting"));

        let mut values = SettingsValues::new();
        values.insert("threshold", FieldValue::Text("90".into()));
        let errors = schema.validate(&values);
        assert!(errors[0].contains("expects Number"), "{errors:?}");
    }

    #[test]
    fn schema_fills_defaults_without_overwriting() {
        let schema = SettingsSchema::new(vec![FieldSpec::new(
            "threshold",
            "_section_a",
            "Threshold",
            FieldKind::Number,
            FieldValue::Number(95.0),
        )]);
        let mut values = SettingsValues::new();
        values.insert("threshold", FieldValue::Number(80.0));

        let filled = schema.with_defaults(&values);
        assert_eq!(filled.number("threshold"), Some(80.0));

        let filled = schema.with_defaults(&SettingsValues::new());
        assert_eq!(filled.number("threshold"), Some(95.0));
    }

    #[test]
    fn field_values_render_for_the_csv_preamble() {
        assert_eq!(FieldValue::Boolean(true).render(), "Yes");
        assert_eq!(FieldValue::Boolean(false).render(), "No");
        assert_eq!(FieldValue::Number(95.0).render(), "95");
        assert_eq!(FieldValue::Number(1.5).render(), "1.5");
        assert_eq!(FieldValue::Text("x".into()).render(), "x");
        assert_eq!(FieldValue::Json(BTreeMap::new()).render(), "(not set)");
    }

    #[test]
    fn settings_accessors_fall_back() {
        let mut values = SettingsValues::new();
        values.insert("hours", FieldValue::Number(12.0));
        values.insert("flag", FieldValue::Boolean(true));
        values.insert("name", FieldValue::Text("All".into()));

        assert_eq!(values.number_or("hours", 1.0), 12.0);
        assert_eq!(values.number_or("missing", 1.0), 1.0);
        assert!(values.bool_or("flag", false));
        assert!(!values.bool_or("missing", false));
        assert_eq!(values.text("name"), Some("All"));
    }

    /// A minimal sub-app, so the registry can be tested without depending on janitor (which
    /// depends on this crate, so it cannot be referenced here).
    struct StubApp;

    impl SubApp for StubApp {
        fn id(&self) -> &'static str {
            "stub"
        }

        fn name(&self) -> &'static str {
            "Stub"
        }

        fn description(&self) -> &'static str {
            "test app"
        }

        fn actions(&self) -> Vec<ActionSpec> {
            vec![
                ActionSpec::new("preview", "Preview", "reads", ActionKind::Read),
                ActionSpec::new("apply", "Apply", "writes", ActionKind::Write),
            ]
        }

        fn schema(&self) -> &SettingsSchema {
            static SCHEMA: std::sync::OnceLock<SettingsSchema> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(SettingsSchema::default)
        }

        fn load_settings(&self, _path: &Path) -> Result<SettingsValues> {
            Ok(SettingsValues::new())
        }

        fn save_settings(&self, _path: &Path, _values: &SettingsValues) -> Result<()> {
            Ok(())
        }

        fn run<'a>(
            &'a self,
            _action: &'a str,
            _context: &'a JobContext,
            _dry_run: bool,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>>
        {
            Box::pin(async { Ok(JobOutcome::new("ok")) })
        }
    }

    fn stub_registry() -> Registry {
        Registry::new(vec![Arc::new(StubApp)])
    }

    #[test]
    fn registry_finds_apps_and_actions() {
        let registry = stub_registry();
        let app = registry.get("stub").expect("stub is registered");
        assert_eq!(app.id(), "stub");
        assert_eq!(app.actions().len(), 2);

        let (_, action) = registry
            .find_action("stub", "apply")
            .expect("action resolves");
        assert_eq!(action.kind, ActionKind::Write);
        assert!(registry.find_action("stub", "nope").is_none());
        assert!(registry.find_action("nope", "nope").is_none());
    }

    #[test]
    fn registry_reports_unknown_schedules() {
        let registry = stub_registry();
        let mut config = CrosstalkConfig::default();
        for key in ["stub.apply", "stub.nope", "typo"] {
            config
                .schedules
                .insert(key.to_string(), "0 4 * * *".to_string());
        }
        let unknown = registry.unknown_schedules(&config);
        assert_eq!(unknown, vec!["stub.nope".to_string(), "typo".to_string()]);
    }
}
