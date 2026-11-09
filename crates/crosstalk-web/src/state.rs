//! State the templates render, built from the registry, the runner and the store.
//!
//! Keeping the conversion here rather than in the route handlers means a template never has to
//! know about `SettingsSchema`, `RunRow` or `ActionKind`, and the same view models are what the
//! tests assert on.

use crosstalk_core::config::{CrosstalkConfig, SharedConfig};
use crosstalk_core::runner::JobRunner;
use crosstalk_core::scheduler::{ScheduleInfo, Scheduler};
use crosstalk_core::store::{RunRecord, RunRow, Store};
use crosstalk_core::subapp::{
    ActionKind, FieldKind, FieldValue, Registry, SettingsSchema, SettingsValues,
};
use std::sync::Arc;

/// Everything a request needs.
#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    /// The daemon's live config. Handlers snapshot it per request, so a settings save
    /// takes effect without a restart; runs snapshot it at start and keep their own copy.
    pub config: SharedConfig,
    pub runner: JobRunner,
    pub store: Store,
    /// The armed scheduler, shared with the serve path. Present in the daemon so a host
    /// settings save can re-arm schedules; absent in tests and one-shot contexts.
    pub scheduler: Option<Arc<tokio::sync::Mutex<Scheduler>>>,
    /// Random secret generated at startup, the CSRF token of a daemon with no auth token
    /// (signed-in browsers carry their session's own instead).
    csrf_token: String,
    /// Per-client auth failure counting: wrong tokens on POST /login and wrong Bearer tokens on
    /// every route.
    login_limiter: Arc<crate::auth::LoginLimiter>,
    /// Signed-in browsers.
    sessions: Arc<crate::session::Sessions>,
    /// Requests in flight, bounded by [`crate::limits::MAX_IN_FLIGHT`].
    in_flight: Arc<tokio::sync::Semaphore>,
    /// The Dispatcharr Connect webhook's debouncer and trigger.
    hooks: Arc<crosstalk_core::hooks::Hooks>,
    /// Per-IP delivery counting for the webhook route, on the same sliding-window counter the
    /// login page uses.
    hook_limiter: Arc<crate::auth::LoginLimiter>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field(
                "apps",
                &self.registry.iter().map(|app| app.id()).collect::<Vec<_>>(),
            )
            .field("auth", &self.token().is_some())
            .finish_non_exhaustive()
    }
}

impl AppState {
    pub fn new(registry: Registry, config: SharedConfig, runner: JobRunner, store: Store) -> Self {
        let hooks = crosstalk_core::hooks::Hooks::new(runner.clone());
        Self {
            registry: Arc::new(registry),
            config,
            runner,
            store,
            scheduler: None,
            csrf_token: crate::csrf::generate_token(),
            login_limiter: Arc::new(crate::auth::LoginLimiter::default()),
            sessions: Arc::new(crate::session::Sessions::default()),
            in_flight: Arc::new(tokio::sync::Semaphore::new(crate::limits::MAX_IN_FLIGHT)),
            hooks,
            // The bound is read from the live configuration per delivery, so this limiter only
            // owns the count: its own maximum is deliberately wide open.
            hook_limiter: Arc::new(crate::auth::LoginLimiter::new(
                u32::MAX,
                std::time::Duration::from_secs(60),
            )),
        }
    }

    /// Attach the armed scheduler so host settings saves can re-arm schedules.
    pub fn with_scheduler(mut self, scheduler: Arc<tokio::sync::Mutex<Scheduler>>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    /// A point-in-time copy of the live config, for rendering and validating.
    pub fn snapshot(&self) -> CrosstalkConfig {
        self.config.snapshot()
    }

    /// The token a request must present, if any, read from the CURRENT config: a token
    /// rotation saved in the UI takes effect on the next request, without a restart.
    pub fn token(&self) -> Option<String> {
        self.config.read().server.resolved_token()
    }

    /// The daemon-wide CSRF secret: what POSTs carry back on a daemon with no auth token.
    pub fn csrf_token(&self) -> &str {
        &self.csrf_token
    }

    /// The auth failure limiter the gate and POST /login check.
    pub fn login_limiter(&self) -> &crate::auth::LoginLimiter {
        &self.login_limiter
    }

    /// The signed-in browsers.
    pub fn sessions(&self) -> &crate::session::Sessions {
        &self.sessions
    }

    /// The in-flight request bound.
    pub fn in_flight(&self) -> &Arc<tokio::sync::Semaphore> {
        &self.in_flight
    }

    /// The listener's security settings from the CURRENT config, read once per request.
    pub fn server_view(&self) -> ServerView {
        let config = self.config.read();
        let server = &config.server;
        ServerView {
            token: server.resolved_token(),
            trusted: server.trusted_cidrs(),
            public_origin: server.public_origin(),
            secure_cookies: server.secure_cookies,
            hsts: server.hsts,
            bind: server.bind.clone(),
        }
    }

    /// The webhook route's debouncer and trigger.
    pub fn hooks(&self) -> &Arc<crosstalk_core::hooks::Hooks> {
        &self.hooks
    }

    /// The per-address delivery limiter the webhook route checks.
    pub fn hook_limiter(&self) -> &crate::auth::LoginLimiter {
        &self.hook_limiter
    }
}

/// The `[server]` settings a request is judged by, read once per request (the outermost layer
/// computes it and the inner ones reuse it from the request's extensions).
#[derive(Debug, Clone)]
pub struct ServerView {
    pub token: Option<String>,
    pub trusted: Vec<crosstalk_core::net::Cidr>,
    pub public_origin: Option<String>,
    pub secure_cookies: bool,
    pub hsts: bool,
    pub bind: String,
}

impl ServerView {
    /// The view the outer layer attached to this request, or a fresh one.
    pub fn of_request(state: &AppState, request: &axum::extract::Request) -> Arc<ServerView> {
        request
            .extensions()
            .get::<Arc<ServerView>>()
            .cloned()
            .unwrap_or_else(|| Arc::new(state.server_view()))
    }
}

/// The CSRF token the page being rendered must embed: the signed-in session's own, or the
/// daemon-wide one when there is no session (a daemon with no token, or a Bearer script).
#[derive(Debug, Clone)]
pub struct CsrfToken(pub String);

#[axum::async_trait]
impl axum::extract::FromRequestParts<AppState> for CsrfToken {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Ok(CsrfToken(
            match parts.extensions.get::<crate::auth::Authenticated>() {
                Some(crate::auth::Authenticated::Session(session)) => session.csrf.clone(),
                _ => state.csrf_token().to_string(),
            },
        ))
    }
}

/// One app on the dashboard.
#[derive(Debug, Clone)]
pub struct UiApp {
    pub id: String,
    pub name: String,
    pub description: String,
    /// A one-line summary of the last run.
    pub last_run: Option<String>,
}

/// One action, with the colour and confirmation the UI needs.
#[derive(Debug, Clone)]
pub struct UiAction {
    pub id: String,
    pub label: String,
    pub description: String,
    /// CSS class carrying the plugin's button colour.
    pub css: String,
    pub confirm: Option<String>,
    /// Whether this action can change channel data, which decides the dry-run flag.
    pub kind_touches_channels: bool,
    /// Whether it takes no one-run-per-app slot: a read-only status query stays clickable while
    /// another run is live, which is exactly when an operator wants it.
    pub unguarded: bool,
    /// Whether it writes, and so is worth offering "start fresh" for.
    pub writing: bool,
    pub disabled: bool,
    /// The action's single free-text input, when it declares one: label and help for the
    /// text field the action button renders.
    pub input_label: Option<String>,
    pub input_help: Option<String>,
}

/// One workflow, as the dashboard lists it.
#[derive(Debug, Clone)]
pub struct UiWorkflow {
    pub name: String,
    /// The steps, ` → `-joined.
    pub steps: String,
    /// Each step with its scope override and failure policy, for the steps table.
    pub step_details: Vec<UiStep>,
    /// How many steps the entry has, for the Run button's confirmation text.
    pub step_count: usize,
    /// Whether a new pass discards each step's saved progress.
    pub fresh: bool,
    /// Why the entry cannot run, when it cannot. The dashboard shows this instead of a button.
    pub error: Option<String>,
    /// A one-line summary of the last run of this workflow.
    pub last_run: Option<String>,
    /// Whether this workflow's slot is taken right now.
    pub busy: bool,
    /// Whether a run of it would change nothing, which decides whether the form confirms.
    pub dry_run: bool,
}

/// One workflow step, as the dashboard's steps table and a run page's planned steps list it.
#[derive(Debug, Clone)]
pub struct UiStep {
    /// The `<app>.<action>` the step runs.
    pub action: String,
    /// How the step's scope override reads (`scope "lineup"`, `scope groups ...`), if any.
    pub scope: Option<String>,
    /// Whether the workflow continues past this step's failure.
    pub continue_on_failure: bool,
}

/// The `[hooks]` table, as the dashboard shows it (read-only).
#[derive(Debug, Clone, Default)]
pub struct UiHooks {
    /// Event name to targets, in name order.
    pub events: Vec<(String, Vec<String>)>,
    /// The debounce window in words.
    pub debounce: String,
    /// The route Dispatcharr posts to.
    pub route: String,
}

/// Build the dashboard's workflow list.
///
/// Read from the live config, the registry and the store only: a workflow is configuration, so
/// there is nothing to create here — the "web display" for workflows is this list plus the Run
/// button and the last result.
pub fn workflows(runner: &JobRunner, config: &CrosstalkConfig, store: &Store) -> Vec<UiWorkflow> {
    let busy = runner.is_busy(crosstalk_core::workflow::APP_ID);
    crosstalk_core::workflow::describe(config)
        .into_iter()
        .map(|info| {
            // The last run of THIS workflow, not of any workflow.
            let last_run = store
                .recent_runs_for(crosstalk_core::workflow::APP_ID, 50)
                .ok()
                .and_then(|runs| runs.into_iter().find(|run| run.action == info.name))
                .map(|run| describe_last_run(&run));
            UiWorkflow {
                name: info.name.clone(),
                steps: info.steps.join(" → "),
                step_details: info
                    .details
                    .iter()
                    .map(|detail| UiStep {
                        action: detail.action.clone(),
                        scope: detail.scope.clone(),
                        continue_on_failure: detail.continue_on_failure,
                    })
                    .collect(),
                step_count: info.steps.len(),
                fresh: info.fresh,
                error: info.error,
                last_run,
                busy,
                // The same rule the Run handler applies, from the same place: a workflow of
                // read-only steps needs no confirmation because it writes nothing.
                dry_run: crosstalk_core::workflow::scheduled_dry_run(
                    runner.registry(),
                    config,
                    &info.name,
                ),
            }
        })
        .collect()
}

/// One line about a finished run, worded like the app list's.
fn describe_last_run(run: &RunRecord) -> String {
    let paused = if run.status.is_paused() {
        " — resumes at its next window"
    } else {
        ""
    };
    let summary = run
        .summary
        .as_deref()
        .map(|summary| format!(": {summary}"))
        .unwrap_or_default();
    format!(
        "{} {} ({}){paused}{summary}",
        run.action,
        run.status.as_str(),
        run.started_at
    )
}

/// Build the dashboard's hooks display.
pub fn hooks(config: &CrosstalkConfig) -> UiHooks {
    let debounce = config.hooks.debounce_secs;
    UiHooks {
        events: crosstalk_core::hooks::describe(&config.hooks),
        debounce: if debounce <= 0.0 {
            "runs on the first delivery".to_string()
        } else if debounce.fract() == 0.0 {
            format!("{debounce:.0} s")
        } else {
            format!("{debounce} s")
        },
        route: crosstalk_core::hooks::ROUTE.to_string(),
    }
}

/// The step runs of a workflow run, in order. Empty for an ordinary run.
pub fn steps_of(store: &Store, run: &RunRecord) -> Vec<RunRecord> {
    if run.app != crosstalk_core::workflow::APP_ID {
        return Vec::new();
    }
    store.child_runs(run.id).unwrap_or_default()
}

/// A workflow run's PLANNED steps: each step with its scope override and failure policy.
///
/// The executed steps (see [`steps_of`]) carry each step's outcome; these carry the plan —
/// which step reads which channels and which failures the workflow carries past — so the
/// run page shows both. Empty for an ordinary run, or when the configuration no longer
/// names the workflow.
pub fn planned_steps(config: &CrosstalkConfig, run: &RunRecord) -> Vec<UiStep> {
    if run.app != crosstalk_core::workflow::APP_ID {
        return Vec::new();
    }
    config
        .workflows
        .get(&run.action)
        .and_then(|entry| crosstalk_core::workflow::Workflow::from_entry(&run.action, entry).ok())
        .map(|workflow| {
            workflow
                .steps
                .iter()
                .map(|step| UiStep {
                    action: step.label(),
                    scope: step.scope.as_ref().map(|scope| scope.describe()),
                    continue_on_failure: step.continue_on_failure,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The workflow run a step belongs to, when it belongs to one.
pub fn parent_of(store: &Store, run: &RunRecord) -> Option<RunRecord> {
    let parent = run.parent_run_id?;
    store.run(parent).ok().flatten()
}

/// Whether this run is itself an undo (preview or apply).
pub fn is_undo_run(run: &RunRecord) -> bool {
    run.action == crosstalk_core::undo::ACTION_UNDO
}

/// Whether the run page offers "Undo this run".
///
/// Exactly the host's own refusal check ([`crosstalk_core::undo::check_target`]): a
/// finished, non-preview run of a known app with `ok` channel rows. Previews, live and
/// paused runs, workflow runs (their steps are undone one by one), probes and other runs
/// with no channel write never qualify. Failed and cancelled runs do, row by row, for the
/// rows that landed. This is the eligibility check, not the chain rule: a run whose rows
/// have all been superseded still offers the button, and its preview reports every row
/// left alone. Undo runs qualify too, so undoing an undo (the redo) is one more click.
pub fn undo_eligible(registry: &Registry, store: &Store, run: &RunRecord) -> bool {
    crosstalk_core::undo::check_target(store, registry, run.id).is_ok()
}

/// The undo runs that reversed this run, newest first.
///
/// An undone run lists them so an operator can follow the chain in either direction: the
/// undo run's page links back through its parent, this lists the other way.
pub fn undo_runs(store: &Store, run: &RunRecord) -> Vec<RunRecord> {
    store
        .child_runs(run.id)
        .unwrap_or_default()
        .into_iter()
        .filter(is_undo_run)
        .rev()
        .collect()
}

/// One setting field, ready to render.
#[derive(Debug, Clone)]
pub struct UiField {
    pub id: String,
    pub label: String,
    pub help_text: String,
    /// `text`, `number`, `boolean`, `json`, or `section`.
    pub kind: String,
    pub value: String,
    pub checked: bool,
    pub is_section: bool,
}

/// One app's settings form.
#[derive(Debug, Clone)]
pub struct UiSettings {
    pub id: String,
    pub name: String,
    pub fields: Vec<UiField>,
    pub error: Option<String>,
}

/// How a run row reads in the table.
#[derive(Debug, Clone)]
pub struct UiRow {
    /// What this row is about, as the page labels it: a channel, a stream or an EPG source.
    pub subject: String,
    pub channel_id: i64,
    pub channel_name: String,
    /// The channel number as printed, empty when it has none.
    pub channel_number: String,
    pub channel_group: String,
    pub score: i64,
    pub method: String,
    /// The guide the run chose (a matching row), or the guide a channel operation saw.
    pub epg_name: String,
    pub epg_source: String,
    /// The channel field a channel operation wrote (`name`, `epg_data_id`, …). Empty for a
    /// matching row, which writes no field of its own.
    pub field: String,
    /// That field's value before and after, in the shape a person reads (a guide's name rather
    /// than its id, `No EPG` rather than `none`).
    pub before: String,
    pub after: String,
    pub status: String,
    /// Lowercased status, for the CSS class.
    pub status_class: String,
    pub reason: String,
    pub ai_decision: Option<String>,
}

/// Build the dashboard's app list.
pub fn apps(registry: &Registry, store: &Store) -> Vec<UiApp> {
    registry
        .iter()
        .map(|app| {
            let last_run = store
                .recent_runs_for(app.id(), 1)
                .ok()
                .and_then(|runs| runs.into_iter().next())
                .map(|run| {
                    // A paused run says what it will do next: "paused" alone reads like a
                    // problem, when it means the backlog continues at the next window.
                    let paused = if run.status.is_paused() {
                        " — resumes at its next window"
                    } else {
                        ""
                    };
                    format!(
                        "{} {} ({}){paused}",
                        run.action,
                        run.status.as_str(),
                        run.started_at
                    )
                });
            UiApp {
                id: app.id().to_string(),
                name: app.name().to_string(),
                description: app.description().to_string(),
                last_run,
            }
        })
        .collect()
}

/// Build one app's action list, marking them disabled while a run is in progress.
pub fn actions(registry: &Registry, runner: &JobRunner, app_id: &str) -> Vec<UiAction> {
    let busy = runner.is_busy(app_id);
    match registry.get(app_id) {
        Some(app) => app
            .actions()
            .into_iter()
            .map(|action| UiAction {
                id: action.id.to_string(),
                label: action.label.to_string(),
                description: action.description.to_string(),
                css: action.kind.css_class().to_string(),
                confirm: action.confirm.map(str::to_string),
                kind_touches_channels: action.kind.touches_channels(),
                unguarded: action.unguarded,
                writing: action.kind.touches_channels(),
                // An unguarded action is never disabled by another run: the guard does not
                // apply to it.
                disabled: busy && !action.unguarded,
                input_label: action.input.map(|input| input.label.to_string()),
                input_help: action.input.map(|input| input.help.to_string()),
            })
            .collect(),
        None => Vec::new(),
    }
}

/// The settings form for one schema, with the values as they stand.
pub fn fields(schema: &SettingsSchema, values: &SettingsValues) -> Vec<UiField> {
    schema
        .fields()
        .iter()
        .map(|field| {
            if field.is_section() {
                return UiField {
                    id: field.id.to_string(),
                    label: field.label.to_string(),
                    help_text: field.help_text.to_string(),
                    kind: "section".to_string(),
                    value: String::new(),
                    checked: false,
                    is_section: true,
                };
            }

            let value = values
                .get(field.id)
                .cloned()
                .unwrap_or_else(|| field.default.clone());
            let (kind, rendered, checked) = match (&field.kind, &value) {
                (FieldKind::Boolean, FieldValue::Boolean(flag)) => {
                    ("boolean".to_string(), String::new(), *flag)
                }
                (FieldKind::Number, FieldValue::Number(number)) => {
                    ("number".to_string(), format!("{number}"), false)
                }
                (FieldKind::JsonObject, FieldValue::Json(map)) => (
                    "json".to_string(),
                    serde_json::to_string_pretty(map).unwrap_or_else(|_| "{}".to_string()),
                    false,
                ),
                (_, value) => ("text".to_string(), value.render(), false),
            };

            UiField {
                id: field.id.to_string(),
                label: field.label.to_string(),
                help_text: field.help_text.to_string(),
                kind,
                value: rendered,
                checked,
                is_section: false,
            }
        })
        .collect()
}

/// Build a run row for the table.
///
/// The row columns are generic because the run rows are: a matching row carries a score and a
/// chosen guide, a channel-operation row carries a field and its before/after values, and the
/// same table has to read both. `before`/`after` are rendered for a person — a guide's NAME
/// instead of its id, `No EPG` instead of the stored `none` — because the machine values are
/// only meaningful to a restore.
pub fn run_row(row: &RunRow) -> UiRow {
    UiRow {
        subject: row.subject.label().to_string(),
        channel_id: row.channel_id,
        channel_name: row.channel_name.clone(),
        channel_number: row
            .channel_number
            .map(|number| format!("{number}"))
            .unwrap_or_default(),
        channel_group: row.channel_group.clone(),
        score: row.score,
        method: row.method.clone(),
        epg_name: row.epg_name.clone().unwrap_or_default(),
        epg_source: row.epg_source.clone().unwrap_or_default(),
        field: row.field.clone(),
        before: display_value(row, row.before_value.as_deref(), true),
        after: display_value(row, row.after_value.as_deref(), false),
        status: row.status.clone(),
        status_class: row.status.to_lowercase(),
        reason: row.reason.clone(),
        ai_decision: row.ai_decision.clone(),
    }
}

/// One side of a channel operation, as a reader needs it.
///
/// `before` side of an EPG clear has the guide's name in `current_epg_name`, which is far more
/// use than the id the store keeps for a restore. Everything else passes through as written.
fn display_value(row: &RunRow, value: Option<&str>, before: bool) -> String {
    let Some(value) = value else {
        return String::new();
    };
    if row.field == "epg_data_id" {
        if value == "none" {
            return "No EPG".to_string();
        }
        if before {
            if let Some(name) = row.current_epg_name.clone() {
                return name;
            }
        }
    }
    value.to_string()
}

pub fn rows(store: &Store, run_id: i64) -> Vec<UiRow> {
    store
        .rows(run_id)
        .map(|rows| rows.iter().map(run_row).collect())
        .unwrap_or_default()
}

/// The schedules, from the config, as the dashboard lists them.
pub fn schedules(config: &CrosstalkConfig) -> Vec<ScheduleInfo> {
    crosstalk_core::scheduler::describe(config)
}

/// The file-name prefixes every registered app's CSV exports carry.
///
/// Taken from the apps themselves rather than from a list kept here, so the host's "clear
/// exports" and an app's own retention rule cannot disagree about what is ours.
pub fn export_prefixes(registry: &Registry) -> Vec<String> {
    registry.iter().map(|app| app.export_prefix()).collect()
}

/// The apps' own CSV exports currently in the exports directory.
pub fn exports_on_disk(config: &CrosstalkConfig, registry: &Registry) -> usize {
    crosstalk_core::csv_report::app_exports(&config.exports_dir(), &export_prefixes(registry)).len()
}

/// Whether the report a run recorded is still on disk.
///
/// Retention and the host's "clear exports" both delete reports, so a run page must ask
/// before offering a download: the run record keeps naming a file that may be gone, and the
/// page should say so instead of linking to a 410.
pub fn report_available(run: &RunRecord) -> bool {
    run.report_path
        .as_deref()
        .is_some_and(|path| std::path::Path::new(path).is_file())
}

/// The counts of a run, as `(key, value)` pairs, for the status panel.
pub fn counts(run: &RunRecord) -> Vec<(String, i64)> {
    match run.counts.as_object() {
        Some(map) => map
            .iter()
            .filter_map(|(key, value)| value.as_i64().map(|value| (key.clone(), value)))
            .filter(|(_, value)| *value != 0)
            .collect(),
        None => Vec::new(),
    }
}

/// Turn submitted form fields into settings values, using the schema to know the types.
///
/// A checkbox that is absent means false, which is why the schema decides the type rather than
/// the form: an unticked box sends nothing at all.
pub fn values_from_form(
    schema: &SettingsSchema,
    form: &std::collections::HashMap<String, String>,
) -> Result<SettingsValues, Vec<String>> {
    let mut values = SettingsValues::new();
    let mut errors = Vec::new();

    for field in schema.editable() {
        let raw = form.get(field.id);
        match field.kind {
            FieldKind::Boolean => {
                let checked =
                    raw.is_some_and(|value| value == "true" || value == "on" || value == "1");
                values.insert(field.id, FieldValue::Boolean(checked));
            }
            FieldKind::Number => {
                // A missing control means "unchanged": fall back to the field's default rather
                // than failing the whole form.
                let text = match raw {
                    Some(text) => text.trim().to_string(),
                    None => match &field.default {
                        FieldValue::Number(number) => format!("{number}"),
                        _ => String::new(),
                    },
                };
                match text.parse::<f64>() {
                    Ok(number) => {
                        values.insert(field.id, FieldValue::Number(number));
                    }
                    Err(_) => {
                        errors.push(format!("{} must be a number, got {text:?}", field.label))
                    }
                }
            }
            FieldKind::JsonObject => {
                let text = raw.map(String::as_str).unwrap_or("").trim();
                if text.is_empty() {
                    values.insert(
                        field.id,
                        FieldValue::Json(std::collections::BTreeMap::new()),
                    );
                    continue;
                }
                match serde_json::from_str::<std::collections::BTreeMap<String, Vec<String>>>(text)
                {
                    Ok(map) => {
                        values.insert(field.id, FieldValue::Json(map));
                    }
                    Err(error) => errors.push(format!(
                        "{} must be a JSON object of name -> [aliases]: {error}",
                        field.label
                    )),
                }
            }
            FieldKind::Text | FieldKind::Info => {
                values.insert(field.id, FieldValue::Text(raw.cloned().unwrap_or_default()));
            }
        }
    }

    if errors.is_empty() {
        Ok(values)
    } else {
        Err(errors)
    }
}

/// Whether an action, as registered, is a dry run by kind.
pub fn is_read(kind: ActionKind) -> bool {
    !kind.touches_channels()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::subapp::{FieldSpec, SettingsSchema};

    fn schema() -> SettingsSchema {
        SettingsSchema::new(vec![
            FieldSpec::section("_s", "Section", "help"),
            FieldSpec::new(
                "name",
                "_s",
                "Name",
                FieldKind::Text,
                FieldValue::Text("default".to_string()),
            ),
            FieldSpec::new(
                "hours",
                "_s",
                "Hours",
                FieldKind::Number,
                FieldValue::Number(12.0),
            ),
            FieldSpec::new(
                "flag",
                "_s",
                "Flag",
                FieldKind::Boolean,
                FieldValue::Boolean(true),
            ),
            FieldSpec::new(
                "aliases",
                "_s",
                "Aliases",
                FieldKind::JsonObject,
                FieldValue::Json(Default::default()),
            ),
        ])
    }

    #[test]
    fn fields_render_every_kind() {
        let rendered = fields(&schema(), &SettingsValues::new());
        assert!(rendered[0].is_section);
        assert_eq!(rendered[1].kind, "text");
        assert_eq!(rendered[1].value, "default");
        assert_eq!(rendered[2].kind, "number");
        assert_eq!(rendered[2].value, "12");
        assert_eq!(rendered[3].kind, "boolean");
        assert!(rendered[3].checked);
        assert_eq!(rendered[4].kind, "json");
        assert_eq!(rendered[4].value, "{}");
    }

    #[test]
    fn a_form_round_trips_through_values() {
        let mut form = std::collections::HashMap::new();
        form.insert("name".to_string(), "Custom".to_string());
        form.insert("hours".to_string(), "6".to_string());
        form.insert("flag".to_string(), "true".to_string());
        form.insert(
            "aliases".to_string(),
            r#"{"ESPN": ["ESPN US"]}"#.to_string(),
        );

        let values = values_from_form(&schema(), &form).expect("parses");
        assert_eq!(values.text("name"), Some("Custom"));
        assert_eq!(values.number("hours"), Some(6.0));
        assert!(values.bool("flag").unwrap_or(false));
        assert_eq!(
            values
                .json("aliases")
                .and_then(|map| map.get("ESPN"))
                .cloned(),
            Some(vec!["ESPN US".to_string()])
        );
    }

    #[test]
    fn an_unticked_checkbox_is_false_not_missing() {
        // A browser sends nothing for an unticked box, so the schema has to supply `false`.
        let form = std::collections::HashMap::new();
        let values = values_from_form(&schema(), &form).expect("parses");
        assert_eq!(values.bool("flag"), Some(false));
    }

    #[test]
    fn a_bad_number_is_reported_against_its_label() {
        let mut form = std::collections::HashMap::new();
        form.insert("hours".to_string(), "six".to_string());
        let errors = values_from_form(&schema(), &form).expect_err("rejects");
        assert!(errors[0].contains("Hours"), "{errors:?}");
    }

    #[test]
    fn bad_alias_json_is_reported() {
        let mut form = std::collections::HashMap::new();
        form.insert("aliases".to_string(), "[not an object]".to_string());
        let errors = values_from_form(&schema(), &form).expect_err("rejects");
        assert!(errors[0].contains("Aliases"), "{errors:?}");
    }

    #[test]
    fn an_empty_alias_box_means_none() {
        let mut form = std::collections::HashMap::new();
        form.insert("aliases".to_string(), "   ".to_string());
        let values = values_from_form(&schema(), &form).expect("parses");
        assert!(values.json("aliases").is_some_and(|map| map.is_empty()));
    }

    #[test]
    fn run_rows_become_table_rows() {
        let row = RunRow {
            channel_name: "ESPN".to_string(),
            channel_group: "Sports".to_string(),
            score: 95,
            method: "Callsign".to_string(),
            epg_name: Some("ESPN HD".to_string()),
            epg_source: Some("SD".to_string()),
            status: "MATCHED".to_string(),
            reason: "Excellent match".to_string(),
            ai_decision: Some("verify: accept".to_string()),
            ..RunRow::default()
        };
        let ui = run_row(&row);
        assert_eq!(ui.status_class, "matched");
        assert_eq!(ui.epg_name, "ESPN HD");
        // A row says what it is about; a channel row is the default.
        assert_eq!(ui.subject, "Channel");

        let empty = run_row(&RunRow::default());
        assert_eq!(empty.epg_name, "");
    }

    #[test]
    fn a_row_is_labelled_by_what_it_is_about() {
        let watchdog = RunRow {
            subject: crosstalk_core::store::Subject::EpgSource,
            channel_id: 4,
            channel_name: "Schedules Direct".to_string(),
            field: "epg_source".to_string(),
            ..RunRow::default()
        };
        assert_eq!(run_row(&watchdog).subject, "EPG source");

        let probe = RunRow {
            subject: crosstalk_core::store::Subject::Stream,
            channel_id: 51_000,
            channel_name: "ESPN HD (alt)".to_string(),
            ..RunRow::default()
        };
        assert_eq!(run_row(&probe).subject, "Stream");
    }

    #[test]
    fn counts_skip_zeroes() {
        let run = RunRecord {
            id: 1,
            app: "guide".to_string(),
            action: "match".to_string(),
            dry_run: true,
            started_at: String::new(),
            finished_at: None,
            status: crosstalk_core::store::RunStatus::Succeeded,
            summary: None,
            counts: serde_json::json!({"matched": 3, "skipped": 0}),
            report_path: None,
            parent_run_id: None,
            action_input: None,
        };
        assert_eq!(counts(&run), vec![("matched".to_string(), 3)]);
    }
}
