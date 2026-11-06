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

use crate::config::{CrosstalkConfig, ScopeConfig, WorkflowStepScope};
use crate::dispatcharr::World;
use crate::jobs::JobContext;
use crate::store::Store;
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

/// One free-text input an action takes, rendered as a text field on its action button
/// (web) and passed as `--input` (CLI).
///
/// A single string input is the whole mechanism on purpose: the one action that needs one
/// today (`lineup.test_name`, Name Cleaner's "Test a Name") takes a single name to try.
/// A second input would be a second field, not a map: one input keeps the CLI flag, the
/// web form, the run record and the tests to one shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionInput {
    /// The form field name and the CLI flag value carrier (one input, so always `input`).
    pub name: &'static str,
    /// The field label the web form shows.
    pub label: &'static str,
    /// One line of help shown under the field.
    pub help: &'static str,
}

impl ActionInput {
    pub const fn new(name: &'static str, label: &'static str, help: &'static str) -> Self {
        Self { name, label, help }
    }
}

/// The longest free-text input a run accepts, in characters. A channel name is far
/// shorter; the cap keeps a pasted blob out of the run record, the summary and the logs.
pub const MAX_ACTION_INPUT_CHARS: usize = 256;

/// Refuse an input the action cannot take: one for an action that declares none, one
/// longer than [`MAX_ACTION_INPUT_CHARS`], or one carrying control characters (a newline
/// or escape would forge lines in the summary, the run page and the logs).
///
/// The runner calls this before reserving a run, so every caller — CLI, web, API — is
/// held to it; the CLI and the web form only add friendlier messages in front.
pub fn check_action_input(spec: &ActionSpec, input: Option<&str>) -> Result<()> {
    let Some(input) = input else {
        return Ok(());
    };
    if spec.input.is_none() {
        anyhow::bail!("action {:?} takes no input", spec.id);
    }
    let chars = input.chars().count();
    if chars > MAX_ACTION_INPUT_CHARS {
        anyhow::bail!("the input is {chars} characters; the limit is {MAX_ACTION_INPUT_CHARS}");
    }
    if input.chars().any(char::is_control) {
        anyhow::bail!("the input must be one line of text, without control characters");
    }
    Ok(())
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
    /// Whether running it needs a reachable Dispatcharr.
    pub needs_dispatcharr: bool,
    /// Whether the action may run while another run of its app is live.
    ///
    /// Only for an action that reads the run store and touches nothing else — `status` is the
    /// one case today. Refusing it while a run is live is exactly backwards: the run that holds
    /// the slot is the one the operator wants to see, and its progress is the answer. An
    /// unguarded run is still recorded (so its answer lands on a run page), but it takes no slot
    /// and is not counted as "already running" by the cross-process guard.
    pub unguarded: bool,
    /// The action's single free-text input, when it takes one. Rendered as a text field on
    /// the web action button and passed as `--input` on the CLI; carried by
    /// [`crate::jobs::JobContext::input`] and stored in the run record.
    pub input: Option<ActionInput>,
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
            needs_dispatcharr: true,
            unguarded: false,
            input: None,
        }
    }

    /// Declare the action's single free-text input (see [`ActionInput`]).
    ///
    /// Only a `Read` action takes one today: an input never changes what a write plans
    /// beyond the one name it tries, and a write with a free-text input would be a
    /// confirmation dialog away from a typo renaming channels. This is a hard panic in
    /// every build, like [`ActionSpec::unguarded`], so the rule cannot drift.
    pub fn with_input(
        mut self,
        name: &'static str,
        label: &'static str,
        help: &'static str,
    ) -> Self {
        assert_eq!(
            self.kind,
            ActionKind::Read,
            "only a read-only action takes a free-text input: {}",
            self.id
        );
        self.input = Some(ActionInput::new(name, label, help));
        self
    }

    /// Mark an action that works entirely offline, so it does not demand credentials.
    pub fn offline(mut self) -> Self {
        self.needs_dispatcharr = false;
        self
    }

    /// Mark a read-only action that may run beside another run of its app (see
    /// [`ActionSpec::unguarded`]).
    ///
    /// Only a `Read` action qualifies: it writes nothing another run could race. This is a
    /// hard panic in every build, not a `debug_assert`: a write action marked unguarded
    /// would silently race another run of its app, and the registry re-checks it too.
    pub fn unguarded(mut self) -> Self {
        assert_eq!(
            self.kind,
            ActionKind::Read,
            "only a read-only action may bypass the one-run-per-app guard: {}",
            self.id
        );
        self.unguarded = true;
        self
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

    /// Drop a value, so an absent form control can mean "unchanged" rather than "cleared".
    pub fn remove(&mut self, id: &str) -> Option<FieldValue> {
        self.0.remove(id)
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

/// One channel set an app offers workflow steps by name: `scope = "lineup"` in a step.
///
/// The name is resolved through the [`Registry`] at run time, when the world and the store
/// are both at hand. The `lineup` app (phase 3, step 13+) will register its configured lineup
/// under the name `lineup`; until then any app — including a test's fake — may provide names.
/// A named scope REPLACES the host `[scope]` for that step: it is the step's channel
/// universe, not a filter on top of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedScope {
    /// The name a step writes (`scope = "lineup"`).
    pub name: String,
    /// One line for the dashboard and the validation error that lists what exists.
    pub description: String,
}

impl NamedScope {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
        }
    }
}

/// What a named scope resolves to, for the step that named it.
///
/// Either shape REPLACES the host `[scope]`: the step reads exactly these channels. An
/// explicit id set is the ownership answer ("the channels lineup X made"); an inline
/// [`ScopeConfig`] is the group/profile answer ("`DIRECTV: *`"). The id set wins when an
/// app can name its channels exactly, because group names can be renamed by hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamedScopeResolution {
    /// Exactly these channel ids, in no particular order. Ids with no channel in the
    /// loaded world are dropped: a channel deleted upstream is gone, not an error.
    Channels(Vec<i64>),
    /// A scope table to resolve like an inline one.
    Scope(ScopeConfig),
}

/// A number that must already be a whole number in range.
///
/// Truncating silently is how a typed `6.7` becomes `6` and a typed `-1` becomes `0`; both
/// look like a saved setting and neither is what was asked for. Shared because every
/// settings form, the host's included, needs the same rule.
pub fn whole_number(field: &str, value: f64, min: u32, max: u32) -> Result<u32> {
    if !value.is_finite() || value.fract() != 0.0 {
        anyhow::bail!("{field} must be a whole number, got {value}");
    }
    if value < f64::from(min) || value > f64::from(max) {
        anyhow::bail!("{field} must be between {min} and {max}, got {value}");
    }
    Ok(value as u32)
}

/// A number that must sit in 0-100, rounded rather than truncated.
pub fn clamp_u8(value: f64) -> Result<u8> {
    if !(0.0..=100.0).contains(&value) {
        anyhow::bail!("value {value} is outside 0-100");
    }
    Ok(value.round() as u8)
}

/// A number that must sit in 0-1: the AI probability thresholds.
pub fn clamp_unit(value: f64) -> Result<f64> {
    if !(0.0..=1.0).contains(&value) {
        anyhow::bail!("value {value} is outside 0-1");
    }
    Ok(value)
}

/// How one action finished, beyond the summary and the counts.
///
/// The host turns this into the recorded [`crate::store::RunStatus`], so a sub-app never decides
/// what its own stopping means in the history — the rules are the host's and documented in
/// [`crate::jobs`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OutcomeStatus {
    /// The action did the whole job it was asked for.
    #[default]
    Succeeded,
    /// The action stopped at its window's deadline with progress saved to resume.
    Paused,
    /// The action stopped because the run was cancelled.
    Cancelled,
    /// The action ran, did some of its work and stopped on an error it did not recover from.
    ///
    /// Distinct from returning `Err`: an `Err` means the run produced no usable outcome at all
    /// (see [`crate::runner`]), so the record keeps no counts. A composite run (a workflow) that
    /// stops at a failed step has counts — what its earlier steps did — and a summary naming the
    /// step, and this is how it says both.
    Failed,
}

/// How one action finished, for the run record and the toast.
#[derive(Debug, Clone)]
pub struct JobOutcome {
    pub summary: String,
    /// Machine-readable counts, shown in the UI and stored with the run.
    pub counts: BTreeMap<String, i64>,
    /// CSV written by the action, if any.
    pub report_path: Option<PathBuf>,
    /// Whether this was a whole job, a pause at the window's deadline, or a cancel.
    pub status: OutcomeStatus,
}

impl JobOutcome {
    pub fn new(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            counts: BTreeMap::new(),
            report_path: None,
            status: OutcomeStatus::Succeeded,
        }
    }

    /// A run that stopped at its window's deadline with its progress saved.
    ///
    /// The action must have checkpointed before returning this, or the next window starts from
    /// the beginning with a `paused` row claiming otherwise.
    pub fn paused(summary: impl Into<String>) -> Self {
        Self {
            status: OutcomeStatus::Paused,
            ..Self::new(summary)
        }
    }

    /// Mark this outcome cancelled, keeping its counts, report and summary.
    pub fn with_cancelled(mut self) -> Self {
        self.status = OutcomeStatus::Cancelled;
        self
    }

    /// A run that did some of its work and stopped on an unrecovered error.
    ///
    /// The counts and the summary are kept: a workflow that failed at its third step still says
    /// what the first two did.
    pub fn failed(summary: impl Into<String>) -> Self {
        Self {
            status: OutcomeStatus::Failed,
            ..Self::new(summary)
        }
    }

    pub fn with_status(mut self, status: OutcomeStatus) -> Self {
        self.status = status;
        self
    }

    pub fn is_paused(&self) -> bool {
        self.status == OutcomeStatus::Paused
    }

    pub fn is_cancelled(&self) -> bool {
        self.status == OutcomeStatus::Cancelled
    }

    pub fn is_failed(&self) -> bool {
        self.status == OutcomeStatus::Failed
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

/// Assemble a run's outcome: the one-line summary, the counts and the CSV it wrote.
///
/// Every action ends by saying what it did, and the shape is the same each time: a sentence,
/// a `, {clause}` per thing worth mentioning, the machine-readable counts, and the report. The
/// clauses are the app's own wording; the collection is this.
#[derive(Debug, Clone, Default)]
pub struct OutcomeBuilder {
    summary: String,
    counts: BTreeMap<String, i64>,
    report: Option<PathBuf>,
    status: OutcomeStatus,
}

impl OutcomeBuilder {
    pub fn new(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            counts: BTreeMap::new(),
            report: None,
            status: OutcomeStatus::Succeeded,
        }
    }

    /// This run stopped at its window's deadline and will resume.
    pub fn paused(mut self) -> Self {
        self.status = OutcomeStatus::Paused;
        self
    }

    /// This run was cancelled; keep everything it did.
    pub fn cancelled(mut self) -> Self {
        self.status = OutcomeStatus::Cancelled;
        self
    }

    /// This run stopped on an error, keeping the counts of what it did do.
    pub fn failed(mut self) -> Self {
        self.status = OutcomeStatus::Failed;
        self
    }

    pub fn status(mut self, status: OutcomeStatus) -> Self {
        self.status = status;
        self
    }

    /// Append `, {note}` to the summary: a clause such as `3 below threshold`.
    pub fn note(mut self, note: impl std::fmt::Display) -> Self {
        self.summary.push_str(&format!(", {note}"));
        self
    }

    /// A machine-readable count, shown in the UI and stored with the run.
    pub fn count(mut self, key: impl Into<String>, value: i64) -> Self {
        self.counts.insert(key.into(), value);
        self
    }

    /// The CSV the action wrote, if it wrote one.
    pub fn report(mut self, report: Option<PathBuf>) -> Self {
        self.report = report;
        self
    }

    pub fn build(self) -> JobOutcome {
        JobOutcome {
            summary: self.summary,
            counts: self.counts,
            report_path: self.report,
            status: self.status,
        }
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

    /// The file-name prefix this app's CSV exports carry in the shared exports directory.
    ///
    /// `/data/exports` is shared with the plugins and with the other sub-apps, so the prefix is
    /// how an app recognizes its own history: retention only prunes these files, and the host's
    /// "clear exports" only deletes them. Derived from the id so the two can never drift apart.
    ///
    /// Ids must not nest: no id may start with another id plus `_` (so no `guide_extra` while
    /// `guide` exists), because `guide_extra_….csv` would otherwise also match `guide_` and a
    /// single-prefix decision could delete the wrong app's files. The registry test pins this; a
    /// per-app decision against the full registry uses `owning_prefix` for the same reason.
    fn export_prefix(&self) -> String {
        format!("{}_", self.id())
    }

    /// The first line of a report: `# crosstalk <name> v<version> - <report>`.
    ///
    /// A comment, like every other preamble line, so a spreadsheet skips it and reads the
    /// columns from the first line that does not start with a `#`. The app names itself here
    /// for the same reason the prefix comes from the id: a caller cannot forget to update it.
    fn report_title(&self, report: &str) -> String {
        format!(
            "# crosstalk {} v{} - {report}",
            self.name(),
            crate::HOST_VERSION
        )
    }

    /// Write settings back, preserving comments.
    fn save_settings(&self, path: &Path, values: &SettingsValues) -> Result<()>;

    /// Check the settings file on disk, without writing anything.
    ///
    /// The default loads the file and runs the schema over its values, which catches a
    /// hand-edited file whose types no longer match. An app with cross-field rules should
    /// override it and keep the same behaviour.
    fn validate_settings(&self, path: &Path) -> Result<()> {
        let values = self.load_settings(path)?;
        let errors = self.schema().validate(&values);
        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("{}", errors.join("; "))
        }
    }

    /// Run one action. `dry_run` is set for a preview.
    fn run<'a>(
        &'a self,
        action: &'a str,
        context: &'a JobContext,
        dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>>;

    /// The named scopes this app offers workflow steps (`scope = "<name>"`).
    ///
    /// Empty by default: most apps select channels through the host `[scope]` and offer
    /// nothing by name. The `lineup` app registers its configured lineup here, so a
    /// `full_sync` step can read "the channels lineup owns" without pinning group names.
    fn named_scopes(&self) -> Vec<NamedScope> {
        Vec::new()
    }

    /// Resolve one of [`Self::named_scopes`] to channels, for the step that named it.
    ///
    /// `None` means "not mine"; the registry asks every app in turn. `Some(Err)` is a
    /// real failure (the store could not be read), which fails the step rather than
    /// silently scoping it to nothing. `config` is the run's host configuration, so an
    /// app can read its own settings file (`config.config_dir`) to say which of its
    /// objects the name means.
    fn resolve_named_scope(
        &self,
        name: &str,
        config: &CrosstalkConfig,
        world: &World,
        store: &Store,
    ) -> Option<Result<NamedScopeResolution>> {
        let _ = (name, config, world, store);
        None
    }
}

/// The set of sub-apps this binary was built with.
#[derive(Clone, Default)]
pub struct Registry {
    apps: Vec<Arc<dyn SubApp>>,
}

impl Registry {
    pub fn new(apps: Vec<Arc<dyn SubApp>>) -> Self {
        // A duplicate id would make `get` return the first quietly, and the second app
        // would be unreachable from the UI, the CLI and the scheduler.
        let mut seen = std::collections::HashSet::new();
        let mut scopes = std::collections::HashSet::new();
        for app in &apps {
            // `workflow` is the host's own app id: it is what `workflow.<name>` means in a
            // schedule key, a hook target and a run row, so a sub-app registered under it
            // would make every one of those ambiguous.
            assert!(
                app.id() != crate::target::WORKFLOW_APP,
                "the app id {:?} is reserved for workflows",
                app.id()
            );
            assert!(
                seen.insert(app.id()),
                "two sub-apps share the id {:?}",
                app.id()
            );
            // Two apps offering one named scope would make `scope = "<name>"` ambiguous:
            // the registry resolves first-match, so the second app's channels would never
            // be read. Like a duplicate app id, this is a build-time bug, not a run-time
            // choice.
            for scope in app.named_scopes() {
                assert!(
                    scopes.insert(scope.name.clone()),
                    "two sub-apps offer the named scope {:?}",
                    scope.name
                );
            }
            // A hard check, not a `debug_assert`: the builder already panics, but an app
            // could set the flag by struct literal in the same crate, and an unguarded write
            // action would race another run of its app with no guard to stop it.
            for action in app.actions() {
                assert!(
                    !action.unguarded || action.kind == ActionKind::Read,
                    "action {}.{} is unguarded but not read-only: only a read-only action \
                     may bypass the one-run-per-app guard",
                    app.id(),
                    action.id
                );
            }
        }
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

    /// Locate one app's action by app id and action id.
    pub fn find_action(&self, app: &str, action: &str) -> Option<(&Arc<dyn SubApp>, ActionSpec)> {
        let app = self.get(app)?;
        let spec = app.actions().into_iter().find(|entry| entry.id == action)?;
        Some((app, spec))
    }

    /// Resolve the schedule keys against the registered apps and the configured workflows,
    /// reporting any that name something which does not exist. A typo in a schedule would
    /// otherwise just never run.
    ///
    /// A key is a [`crate::target::Target`]: `<app>.<action>`, or `workflow.<name>` for a step
    /// sequence, which is how a workflow gets scheduled.
    pub fn unknown_schedules(&self, config: &CrosstalkConfig) -> Vec<String> {
        let mut unknown = Vec::new();
        for key in config.schedules.keys() {
            match crate::target::Target::parse(key) {
                Ok(target) if target.exists(self, config) => {}
                _ => unknown.push(key.clone()),
            }
        }
        unknown
    }

    /// Workflow steps that name no registered action, as `(workflow, step)` pairs.
    ///
    /// A step naming a workflow is caught earlier, by the configuration's own shape rules (a
    /// workflow never nests), so what is left here is a typo or a step for an app this build
    /// does not link in.
    pub fn unknown_workflow_steps(&self, config: &CrosstalkConfig) -> Vec<(String, String)> {
        let mut unknown = Vec::new();
        for (name, workflow) in &config.workflows {
            for step in &workflow.steps {
                let action = step.action();
                if crate::workflow::Step::parse(action)
                    .map(|step| self.find_action(&step.app, &step.action).is_none())
                    .unwrap_or(true)
                {
                    unknown.push((name.clone(), step.display()));
                }
            }
        }
        unknown
    }

    /// Workflow step scope names no app provides, as `(workflow, scope)` pairs.
    ///
    /// The configuration's own validation cannot check these (it has no registry: a name the
    /// build does not serve yet may belong to an app that registers later, like `lineup`),
    /// so this registry-aware check is the load-time half. The run-time half is the step
    /// executor, which fails the step when the name still resolves to nothing.
    pub fn unknown_workflow_scopes(&self, config: &CrosstalkConfig) -> Vec<(String, String)> {
        let mut unknown = Vec::new();
        for (name, workflow) in &config.workflows {
            for step in &workflow.steps {
                if let Some(WorkflowStepScope::Named(scope_name)) = step.scope() {
                    if !self.provides_named_scope(scope_name) {
                        unknown.push((name.clone(), scope_name.clone()));
                    }
                }
            }
        }
        unknown
    }

    /// Whether any registered app offers this named scope.
    pub fn provides_named_scope(&self, name: &str) -> bool {
        let wanted = name.trim();
        self.apps
            .iter()
            .any(|app| app.named_scopes().iter().any(|scope| scope.name == wanted))
    }

    /// Every named scope the registered apps offer, as `(app, scope)` pairs, in order.
    pub fn known_named_scopes(&self) -> Vec<(String, NamedScope)> {
        let mut known = Vec::new();
        for app in &self.apps {
            for scope in app.named_scopes() {
                known.push((app.id().to_string(), scope));
            }
        }
        known
    }

    /// Resolve a step's named scope through the apps that offer one.
    ///
    /// The first app claiming the name wins; two apps offering the same name is a
    /// registration the registry test would catch like a duplicate app id, so the lookup
    /// stays first-match. An unclaimed name is an error listing what is claimed, which is
    /// what the failed step reports.
    pub fn resolve_named_scope(
        &self,
        name: &str,
        config: &CrosstalkConfig,
        world: &World,
        store: &Store,
    ) -> Result<NamedScopeResolution> {
        let wanted = name.trim();
        for app in &self.apps {
            if let Some(resolved) = app.resolve_named_scope(wanted, config, world, store) {
                return resolved;
            }
        }
        let mut known: Vec<String> = self
            .known_named_scopes()
            .iter()
            .map(|(_, scope)| scope.name.clone())
            .collect();
        known.sort();
        known.dedup();
        if known.is_empty() {
            anyhow::bail!(
                "workflow scope {wanted:?} names no scope this build provides: no sub-app \
                 registers a named scope"
            );
        }
        anyhow::bail!(
            "workflow scope {wanted:?} names no scope this build provides; known: {}",
            known.join(", ")
        )
    }

    /// Hook targets that name no registered action and no configured workflow, as
    /// `(event, target)` pairs.
    pub fn unknown_hook_targets(&self, config: &CrosstalkConfig) -> Vec<(String, String)> {
        let mut unknown = Vec::new();
        for (event, targets) in &config.hooks.events {
            for target in targets {
                match crate::target::Target::parse(target) {
                    Ok(parsed) if parsed.exists(self, config) => {}
                    _ => unknown.push((event.clone(), target.clone())),
                }
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
    fn action_input_is_refused_unless_declared_short_and_one_line() {
        let plain = ActionSpec::new("plain", "Plain", "help", ActionKind::Read);
        let takes = ActionSpec::new("takes", "Takes", "help", ActionKind::Read)
            .with_input("input", "Name", "help");
        assert!(check_action_input(&plain, None).is_ok());
        assert!(check_action_input(&takes, None).is_ok());
        assert!(check_action_input(&takes, Some("Sky Atlantic | HD |")).is_ok());
        assert!(check_action_input(&plain, Some("Sky")).is_err());
        let at_cap = "é".repeat(MAX_ACTION_INPUT_CHARS);
        assert!(check_action_input(&takes, Some(&at_cap)).is_ok());
        let over = "x".repeat(MAX_ACTION_INPUT_CHARS + 1);
        assert!(check_action_input(&takes, Some(&over)).is_err());
        for forged in ["Sky\nRules fired: none", "Sky\r", "Sky\u{1b}[31m"] {
            assert!(
                check_action_input(&takes, Some(forged)).is_err(),
                "{forged:?}"
            );
        }
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
    fn whole_number_refuses_fractions_and_out_of_range_values() {
        // Truncating silently would turn a typed 6.7 into 6 and look like a saved setting.
        assert_eq!(whole_number("hours", 6.0, 0, 8_760).expect("ok"), 6);
        assert!(whole_number("hours", 6.7, 0, 8_760).is_err());
        assert!(whole_number("hours", -1.0, 0, 8_760).is_err());
        assert!(whole_number("hours", 8_761.0, 0, 8_760).is_err());
        assert!(whole_number("hours", f64::NAN, 0, 8_760).is_err());
        // The error names the field, because a form has several.
        assert!(format!("{}", whole_number("hours", 6.7, 0, 8_760).unwrap_err()).contains("hours"));
    }

    #[test]
    fn the_clamps_hold_their_ranges() {
        assert_eq!(clamp_u8(94.4).expect("ok"), 94);
        assert_eq!(clamp_u8(100.0).expect("ok"), 100);
        assert!(clamp_u8(101.0).is_err());
        assert!(clamp_u8(-1.0).is_err());

        assert_eq!(clamp_unit(0.8).expect("ok"), 0.8);
        assert!(clamp_unit(1.5).is_err());
        assert!(clamp_unit(-0.1).is_err());
    }

    #[test]
    fn a_value_can_be_dropped_from_a_form_snapshot() {
        let mut values = SettingsValues::new();
        values.insert("hours", FieldValue::Number(12.0));
        assert!(values.remove("hours").is_some());
        assert!(values.get("hours").is_none());
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

    /// The shared minimal sub-app: the core cannot depend on a real one, which depends on it.
    use crate::test_app::StubApp;

    fn stub_registry() -> Registry {
        Registry::new(vec![Arc::new(StubApp::new())])
    }

    #[test]
    fn an_apps_exports_are_named_after_its_id() {
        let app = StubApp::new();
        assert_eq!(app.export_prefix(), "stub_");
        assert_eq!(
            app.report_title("Preview Report"),
            format!("# crosstalk Stub v{} - Preview Report", crate::HOST_VERSION)
        );
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
    fn registry_reports_unknown_workflow_steps_and_hook_targets() {
        // Validation has the registry, the configuration alone does not: a step or a hook target
        // naming an action this build does not register is the typo only this method can see.
        let registry = stub_registry();
        let mut config = CrosstalkConfig::default();
        config.workflows.insert(
            "nightly".to_string(),
            crate::config::WorkflowConfig {
                steps: vec!["stub.apply".into(), "stub.nope".into()],
                fresh: false,
            },
        );
        config.workflows.insert(
            "nested".to_string(),
            crate::config::WorkflowConfig {
                steps: vec!["workflow.other".into()],
                fresh: false,
            },
        );
        config.hooks.events.insert(
            "m3u_refresh".to_string(),
            vec!["stub.preview".to_string(), "stub.nope".to_string()],
        );
        config.hooks.events.insert(
            "epg_refresh".to_string(),
            vec![
                "workflow.nightly".to_string(),
                "workflow.missing".to_string(),
            ],
        );
        config.schedules.insert(
            "workflow.nightly".to_string(),
            crate::config::ScheduleEntry::cron("0 4 * * *"),
        );
        config.schedules.insert(
            "workflow.missing".to_string(),
            crate::config::ScheduleEntry::cron("0 4 * * *"),
        );

        assert_eq!(
            registry.unknown_workflow_steps(&config),
            vec![
                ("nested".to_string(), "workflow.other".to_string()),
                ("nightly".to_string(), "stub.nope".to_string()),
            ]
        );
        assert_eq!(
            registry.unknown_hook_targets(&config),
            vec![
                ("epg_refresh".to_string(), "workflow.missing".to_string()),
                ("m3u_refresh".to_string(), "stub.nope".to_string()),
            ]
        );
        // A configured workflow is a valid schedule key; a missing one is not.
        assert_eq!(
            registry.unknown_schedules(&config),
            vec!["workflow.missing".to_string()]
        );
    }

    #[test]
    fn registry_reports_unknown_schedules() {
        let registry = stub_registry();
        let mut config = CrosstalkConfig::default();
        for key in ["stub.apply", "stub.nope", "typo"] {
            config.schedules.insert(
                key.to_string(),
                crate::config::ScheduleEntry::cron("0 4 * * *"),
            );
        }
        let unknown = registry.unknown_schedules(&config);
        assert_eq!(unknown, vec!["stub.nope".to_string(), "typo".to_string()]);
    }

    /// A fake app offering one named scope, standing in for `lineup` until step 13.
    struct ScopedApp;

    impl SubApp for ScopedApp {
        fn id(&self) -> &'static str {
            "scoped"
        }

        fn name(&self) -> &'static str {
            "Scoped"
        }

        fn description(&self) -> &'static str {
            "test named scopes"
        }

        fn actions(&self) -> Vec<ActionSpec> {
            vec![ActionSpec::new("act", "Act", "acts", ActionKind::Read).offline()]
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

        fn named_scopes(&self) -> Vec<NamedScope> {
            vec![NamedScope::new("fakescope", "the fake app's channels")]
        }

        fn resolve_named_scope(
            &self,
            name: &str,
            _config: &CrosstalkConfig,
            _world: &World,
            _store: &Store,
        ) -> Option<Result<NamedScopeResolution>> {
            if name.trim() == "fakescope" {
                Some(Ok(NamedScopeResolution::Channels(vec![7])))
            } else {
                None
            }
        }

        fn run<'a>(
            &'a self,
            _action: &'a str,
            _context: &'a JobContext,
            _dry_run: bool,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>>
        {
            Box::pin(async move { Ok(JobOutcome::new("done")) })
        }
    }

    fn scoped_registry() -> Registry {
        Registry::new(vec![Arc::new(ScopedApp)])
    }

    fn scoped_config() -> CrosstalkConfig {
        let mut config = CrosstalkConfig::default();
        config.workflows.insert(
            "nightly".to_string(),
            crate::config::WorkflowConfig {
                steps: vec![crate::config::WorkflowStepEntry::Detailed(
                    crate::config::WorkflowStepDetail {
                        action: "scoped.act".to_string(),
                        scope: Some(crate::config::WorkflowStepScope::Named(
                            "fakescope".to_string(),
                        )),
                        continue_on_failure: false,
                    },
                )],
                fresh: false,
            },
        );
        config
    }

    #[test]
    fn registry_knows_the_scopes_its_apps_offer() {
        // No app in the stub registry offers a named scope: any name is unknown there.
        assert!(!stub_registry().provides_named_scope("fakescope"));
        assert!(stub_registry().known_named_scopes().is_empty());

        let registry = scoped_registry();
        assert!(registry.provides_named_scope("fakescope"));
        assert!(!registry.provides_named_scope("nope"));
        assert_eq!(
            registry.known_named_scopes(),
            vec![(
                "scoped".to_string(),
                NamedScope::new("fakescope", "the fake app's channels")
            )]
        );
        // The configuration's own validation cannot check names (it has no registry), so
        // this registry-aware check is the load-time half: a served name is not unknown.
        assert!(registry
            .unknown_workflow_scopes(&scoped_config())
            .is_empty());
        assert_eq!(
            stub_registry().unknown_workflow_scopes(&scoped_config()),
            vec![("nightly".to_string(), "fakescope".to_string())]
        );
    }

    #[test]
    fn resolving_an_unknown_scope_names_what_exists() {
        let world = World::default();
        let store = Store::in_memory().expect("store");
        let error = scoped_registry()
            .resolve_named_scope("nope", &CrosstalkConfig::default(), &world, &store)
            .expect_err("unknown");
        assert!(format!("{error:#}").contains("\"nope\""), "{error:#}");
        assert!(
            format!("{error:#}").contains("fakescope"),
            "the error lists what exists: {error:#}"
        );

        let resolved = scoped_registry()
            .resolve_named_scope("fakescope", &CrosstalkConfig::default(), &world, &store)
            .expect("served");
        assert_eq!(resolved, NamedScopeResolution::Channels(vec![7]));
    }
}
