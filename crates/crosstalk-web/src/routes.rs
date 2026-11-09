//! The HTTP routes: pages, the JSON API, and the actions that start runs.

use crate::auth;
use crate::client::ClientInfo;
use crate::limits;
use crate::state::{
    self, AppState, CsrfToken, UiApp, UiField, UiHooks, UiRow, UiSettings, UiWorkflow,
};
use askama::Template;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Form, Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use crosstalk_core::store::{RunRecord, RunStatus};
use crosstalk_core::subapp::Registry;
use std::collections::HashMap;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

/// The longest free-text action input the server accepts, in characters. The form says the
/// same; this is what a hand-made POST meets.
pub const MAX_ACTION_INPUT: usize = 256;

/// Build the router.
///
/// The layers, outermost first: security headers (and the `[server]` settings read once for the
/// request), tracing, the 30 s timeout, the in-flight bound, the auth gate, the Host/Origin
/// check, CSRF, then each route with its own body bound. Auth runs before anything reads a body
/// or answers about CSRF, so an unauthenticated request learns nothing and costs nothing.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route(
            "/login",
            get(login_page)
                .post(login)
                .layer(DefaultBodyLimit::max(limits::LOGIN_BODY)),
        )
        .route("/logout", post(logout))
        .route("/", get(dashboard))
        .route("/apps/:app", get(app_page))
        .route("/apps/:app/actions/:action", post(start_action))
        .route("/runs", get(runs_page))
        .route("/runs/:id", get(run_page))
        .route("/runs/:id/status", get(run_status))
        .route("/runs/:id/rows", get(run_rows))
        .route("/runs/:id/csv", get(run_csv))
        .route("/runs/:id/cancel", post(cancel_run))
        .route("/runs/:id/undo", post(start_undo))
        .route("/exports/clear", post(clear_exports))
        .route("/settings", get(settings_page))
        .route(
            "/settings/host",
            post(save_host_settings).layer(DefaultBodyLimit::max(limits::SETTINGS_BODY)),
        )
        .route(
            "/settings/:app",
            post(save_app_settings).layer(DefaultBodyLimit::max(limits::SETTINGS_BODY)),
        )
        // Dispatcharr Connect posts here. Bearer-only (`auth::gate`), CSRF-exempt because of
        // that (`csrf::check`), and it answers 202 before any work starts.
        .route(
            crosstalk_core::hooks::ROUTE,
            post(dispatcharr_hook).layer(DefaultBodyLimit::max(limits::HOOK_BODY)),
        )
        // A workflow's Run button on the dashboard. It writes, so it confirms like an apply.
        .route("/workflows/:name/run", post(start_workflow))
        .route("/api/v1/apps", get(api_apps))
        .route("/api/v1/runs", get(api_runs))
        .route("/api/v1/runs/:id", get(api_run))
        .route("/api/v1/runs/:id/rows", get(api_run_rows))
        // Embedded rather than read from disk: the runtime image has no source tree, and an
        // install behind no internet still needs its stylesheet and htmx.
        .route("/static/app.css", get(stylesheet))
        .route("/static/htmx.min.js", get(htmx))
        .route("/static/confirm.js", get(confirm_script))
        // Every other route's body bound; the routes above that need another set their own.
        .layer(DefaultBodyLimit::max(limits::DEFAULT_BODY))
        // Innermost check: a forged cross-site POST fails closed even with no token set.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::csrf::check,
        ))
        // Outside CSRF, so a DNS-rebinding Host is rejected before anything trusts it.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::host::check,
        ))
        // Outside both: the sender is known (and counted) before anything else looks at the
        // request, so a wrong token is a 401/429 and never a CSRF 403.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::gate,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            limits::concurrency,
        ))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            limits::REQUEST_TIMEOUT,
        ))
        .layer(TraceLayer::new_for_http())
        // Outside everything, so even a rejection from an inner layer (a 403 from the
        // Host or CSRF checks, a 401, a 429, a redirect) carries the browser security headers.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::security::headers,
        ))
        .with_state(state)
}

// --- templates ---------------------------------------------------------------------

/// Renders an askama template as an HTML response.
///
/// askama 0.16 dropped its axum integration (the `askama_axum` crate, which pulled in the
/// unmaintained `basic-toml`), so every page type gets this blanket shim instead. A render
/// failure is a bug in a template, not in the request: it is logged and answered with 500.
trait RenderPage {
    fn into_response(self) -> Response;
}

impl<T: Template> RenderPage for T {
    fn into_response(self) -> Response {
        match self.render() {
            Ok(body) => axum::response::Html(body).into_response(),
            Err(error) => {
                tracing::error!("rendering a page failed: {error}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "the page could not be rendered\n",
                )
                    .into_response()
            }
        }
    }
}

#[derive(Template)]
#[template(path = "login.html")]
struct LoginTemplate {
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardTemplate {
    version: &'static str,
    csrf_token: String,
    apps: Vec<UiApp>,
    schedules: Vec<crosstalk_core::scheduler::ScheduleInfo>,
    workflows: Vec<UiWorkflow>,
    hooks: UiHooks,
    runs: Vec<RunRecord>,
    warnings: Vec<String>,
    /// What the last "clear exports" did, on the page it redirected back to.
    notice: Option<String>,
    /// The apps' own report files currently on disk, so the button says what it would delete.
    export_count: usize,
}

#[derive(Template)]
#[template(path = "app.html")]
struct AppTemplate {
    version: &'static str,
    csrf_token: String,
    app: UiApp,
    actions: Vec<crate::state::UiAction>,
    running: Option<crosstalk_core::runner::ActiveRun>,
    runs: Vec<RunRecord>,
}

#[derive(Template)]
#[template(path = "runs.html")]
struct RunsTemplate {
    version: &'static str,
    csrf_token: String,
    runs: Vec<RunRecord>,
}

#[derive(Template)]
#[template(path = "run.html")]
struct RunTemplate {
    version: &'static str,
    csrf_token: String,
    run: RunRecord,
    // The embedded panels need the same values as their standalone routes.
    active: Option<crosstalk_core::runner::ActiveRun>,
    counts: Vec<(String, i64)>,
    rows: Vec<UiRow>,
    /// Whether the CSV the run recorded is still on disk.
    report_available: bool,
    /// The steps of a workflow run, in the order they ran. Empty for an ordinary run.
    steps: Vec<RunRecord>,
    /// The workflow's planned steps, with each step's scope override and failure policy.
    /// Empty for an ordinary run.
    planned: Vec<crate::state::UiStep>,
    /// Whether this run is a workflow's, whose results live on its steps' pages.
    is_workflow: bool,
    /// The workflow run this run is a step of, when it is one.
    parent: Option<RunRecord>,
    /// Whether the page offers "Undo this run": a finished, non-preview run with `ok`
    /// channel rows left to reverse.
    undo_eligible: bool,
    /// The undo runs that reversed this run, newest first. An undone run lists them so an
    /// operator can follow the chain in either direction.
    undo_runs: Vec<RunRecord>,
    /// Whether this run is an undo PREVIEW whose target can still be undone, so the page
    /// offers "Apply undo".
    apply_undo_offered: bool,
}

#[derive(Template)]
#[template(path = "_run_status.html")]
struct RunStatusTemplate {
    csrf_token: String,
    run: RunRecord,
    active: Option<crosstalk_core::runner::ActiveRun>,
    counts: Vec<(String, i64)>,
    report_available: bool,
}

#[derive(Template)]
#[template(path = "_rows.html")]
struct RowsTemplate {
    rows: Vec<UiRow>,
}

#[derive(Template)]
#[template(path = "settings.html")]
struct SettingsTemplate {
    version: &'static str,
    csrf_token: String,
    saved: bool,
    host_error: Option<String>,
    host_notice: Option<String>,
    host_fields: Vec<UiField>,
    apps: Vec<UiSettings>,
}

// --- handlers ----------------------------------------------------------------------

async fn health() -> &'static str {
    "ok\n"
}

async fn stylesheet() -> Response {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../static/app.css"),
    )
        .into_response()
}

async fn htmx() -> Response {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../static/htmx.min.js"),
    )
        .into_response()
}

/// The delegated `data-confirm` submit listener (see `static/confirm.js`).
async fn confirm_script() -> Response {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../static/confirm.js"),
    )
        .into_response()
}

async fn login_page() -> Response {
    LoginTemplate { error: None }.into_response()
}

#[derive(serde::Deserialize)]
struct LoginForm {
    token: String,
}

async fn login(
    State(state): State<AppState>,
    client: ClientInfo,
    Form(form): Form<LoginForm>,
) -> Response {
    let Some(expected) = state.token() else {
        // No token configured: there is nothing to sign in to.
        return Redirect::to("/").into_response();
    };

    // Per-client failure counting, checked and counted in one step: ten wrong tokens in five
    // minutes locks the client out with a 429, so the single all-powerful token cannot be
    // guessed at line rate. Wrong Bearer tokens on any other route count toward the same limit.
    if let Err(retry) = state.login_limiter().try_attempt(client.key) {
        return auth::too_many(retry);
    }

    if !auth::tokens_match(&expected, form.token.trim()) {
        return (
            StatusCode::UNAUTHORIZED,
            LoginTemplate {
                error: Some("That token does not match the configured one.".to_string()),
            }
            .into_response(),
        )
            .into_response();
    }
    state.login_limiter().record_success(client.key);

    // The browser gets a session id, never the token itself.
    let session = state.sessions().create(&expected);
    let secure = state.server_view().secure_cookies || client.https;
    let mut response = Redirect::to("/").into_response();
    if let Ok(value) =
        header::HeaderValue::from_str(&crate::session::set_cookie(&session.id, secure))
    {
        response.headers_mut().append(header::SET_COOKIE, value);
    }
    // A cookie an older version set to the raw token is cleared on the way in.
    let [_, legacy] = crate::session::clear_cookies(secure);
    if let Ok(value) = header::HeaderValue::from_str(&legacy) {
        response.headers_mut().append(header::SET_COOKIE, value);
    }
    response
}

async fn logout(State(state): State<AppState>, client: ClientInfo, headers: HeaderMap) -> Response {
    if let Some(id) = crate::session::session_id(&headers) {
        state.sessions().remove(&id);
    }
    let secure = state.server_view().secure_cookies || client.https;
    let mut response = Redirect::to("/login").into_response();
    for cookie in crate::session::clear_cookies(secure) {
        if let Ok(value) = header::HeaderValue::from_str(&cookie) {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
    }
    response
}

async fn dashboard(
    State(state): State<AppState>,
    CsrfToken(csrf_token): CsrfToken,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let runs = state.store.recent_runs(20).unwrap_or_default();
    let snapshot = state.snapshot();
    let warnings = snapshot.validate().warnings;
    DashboardTemplate {
        version: crosstalk_core::HOST_VERSION,
        csrf_token,
        apps: state::apps(&state.registry, &state.store),
        schedules: state::schedules(&snapshot),
        workflows: state::workflows(&state.runner, &snapshot, &state.store),
        hooks: state::hooks(&snapshot),
        runs,
        warnings,
        notice: clear_notice(&query),
        export_count: state::exports_on_disk(&snapshot, &state.registry),
    }
    .into_response()
}

/// The summary a "clear exports" redirects back to the dashboard with.
///
/// A count per outcome, because "deleted 12" on its own would hide the answers an operator
/// needs: how much was kept because a run may still hold it, and whether anything refused to go.
fn clear_notice(query: &HashMap<String, String>) -> Option<String> {
    let count = |key: &str| {
        query
            .get(key)
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0)
    };
    if !query.contains_key("cleared") {
        return None;
    }
    let (deleted, kept, failed) = (count("cleared"), count("kept"), count("failed"));
    let plural = |n: usize| if n == 1 { "" } else { "s" };
    let mut notice = format!("Deleted {deleted} report file{}.", plural(deleted));
    if kept > 0 {
        notice.push_str(&format!(
            " Kept {kept} written in the last {} minutes, in case a run is still using them.",
            crosstalk_core::csv_report::CLEAR_GRACE.as_secs() / 60
        ));
    }
    if failed > 0 {
        notice.push_str(&format!(
            " {failed} could not be deleted; the log says why."
        ));
    }
    Some(notice)
}

/// Delete the registered apps' CSV exports. One host feature for what every plugin had its own
/// `clear_csv_exports` action for.
async fn clear_exports(
    State(state): State<AppState>,
    Form(form): Form<ClearExportsForm>,
) -> Response {
    // The same server-side confirmation every channel-changing action needs: the browser's
    // dialog is convenience, not protection.
    if form.confirm.as_deref() != Some("true") {
        return (
            StatusCode::BAD_REQUEST,
            "clearing deletes files: resubmit the form with confirm=true\n",
        )
            .into_response();
    }

    // A run writes its report and then applies it, so a clear during a run could delete the
    // file that run is about to link. The daemon knows its own runs exactly, so it refuses
    // rather than guessing from timestamps; a one-shot CLI clear cannot know and keeps the
    // most recent reports instead (see `csv_report::CLEAR_GRACE`).
    if let Some(active) = state.runner.running().into_iter().next() {
        return (
            StatusCode::CONFLICT,
            format!(
                "a run is in progress ({} / {}): clear the exports after it finishes\n",
                active.app, active.action
            ),
        )
            .into_response();
    }

    let snapshot = state.snapshot();
    let summary = crosstalk_core::csv_report::clear_exports(
        &snapshot.exports_dir(),
        &state::export_prefixes(&state.registry),
    );
    tracing::info!(
        deleted = summary.deleted,
        kept = summary.kept_recent,
        failed = summary.failed,
        "cleared the CSV exports"
    );
    Redirect::to(&format!(
        "/?cleared={}&kept={}&failed={}",
        summary.deleted, summary.kept_recent, summary.failed
    ))
    .into_response()
}

async fn app_page(
    State(state): State<AppState>,
    CsrfToken(csrf_token): CsrfToken,
    Path(app_id): Path<String>,
) -> Response {
    let Some(app) = state.registry.get(&app_id) else {
        return (StatusCode::NOT_FOUND, "no such app\n").into_response();
    };

    AppTemplate {
        version: crosstalk_core::HOST_VERSION,
        csrf_token,
        app: UiApp {
            id: app.id().to_string(),
            name: app.name().to_string(),
            description: app.description().to_string(),
            last_run: None,
        },
        actions: state::actions(&state.registry, &state.runner, &app_id),
        running: state
            .runner
            .running()
            .into_iter()
            .find(|run| run.app == app_id),
        runs: state.store.recent_runs_for(&app_id, 15).unwrap_or_default(),
    }
    .into_response()
}

#[derive(serde::Deserialize)]
struct ClearExportsForm {
    /// Server-side confirmation for a deletion, mirroring the CLI's `--yes`.
    #[serde(default)]
    confirm: Option<String>,
}

#[derive(serde::Deserialize)]
struct StartForm {
    #[serde(default)]
    dry_run: Option<String>,
    /// Server-side confirmation for channel-changing actions, mirroring the CLI's `--yes`.
    /// The UI's confirm dialog submits it; a forged POST without it is rejected.
    #[serde(default)]
    confirm: Option<String>,
    /// "Start fresh": ignore and discard the progress a paused run saved. Unchecked is the
    /// default because resuming is the point of a windowed job; the checkbox is how an
    /// operator asks for the backlog from the beginning.
    #[serde(default)]
    fresh: Option<String>,
    /// The action's single free-text input, when it declares one. Ignored for actions
    /// without an input (the form only renders the field then).
    #[serde(default)]
    input: Option<String>,
}

async fn start_action(
    State(state): State<AppState>,
    Path((app_id, action_id)): Path<(String, String)>,
    Form(form): Form<StartForm>,
) -> Response {
    let Some((_app, action)) = state.registry.find_action(&app_id, &action_id) else {
        return (StatusCode::NOT_FOUND, "no such action\n").into_response();
    };

    // Write/Destructive actions need a server-side confirmation, not just the browser's
    // confirm() dialog: without it, any page the operator visits could trigger an apply.
    if action.kind.requires_confirmation() && form.confirm.as_deref() != Some("true") {
        return (
            StatusCode::BAD_REQUEST,
            "this action changes data: resubmit the form with confirm=true\n",
        )
            .into_response();
    }

    // The form says whether the button was a preview; the action's kind is the authority, so a
    // preview action stays a preview even if the form is tampered with.
    let dry_run = !action.kind.touches_channels()
        || form.dry_run.as_deref().is_some_and(|value| value == "true");

    let options = crosstalk_core::runner::RunOptions::default();
    let options = if form.fresh.as_deref().is_some_and(|value| value == "true") {
        options.fresh()
    } else {
        options
    };
    // The form's field is short; a hand-made POST is held to the same bound before the text is
    // recorded anywhere.
    if form
        .input
        .as_deref()
        .is_some_and(|text| text.chars().count() > MAX_ACTION_INPUT)
    {
        return (
            StatusCode::BAD_REQUEST,
            format!("the input is longer than {MAX_ACTION_INPUT} characters\n"),
        )
            .into_response();
    }
    // An input for an action that declares none is a forged field: drop it rather than
    // record text no action reads. An empty input is no input (the context trims it).
    let options = match (action.input, form.input.clone()) {
        (Some(_), Some(text)) if !text.trim().is_empty() => options.with_input(Some(text)),
        _ => options,
    };
    match state
        .runner
        .start_with(&app_id, &action_id, dry_run, options)
    {
        Ok(started) => Redirect::to(&format!("/runs/{}", started.run_id)).into_response(),
        Err(error) => (
            StatusCode::CONFLICT,
            format!("could not start the run: {error:#}\n"),
        )
            .into_response(),
    }
}

/// `POST /workflows/:name/run`: the dashboard's Run button.
///
/// A workflow writes (a workflow of read-only steps is possible, but not the default), so it
/// confirms exactly like an apply action does: the browser dialog is convenience, and the
/// hidden `confirm` field is what the server checks.
async fn start_workflow(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Form(form): Form<StartForm>,
) -> Response {
    let snapshot = state.snapshot();
    if !snapshot.workflows.contains_key(&name) {
        return (StatusCode::NOT_FOUND, "no such workflow\n").into_response();
    }
    let dry_run = crosstalk_core::workflow::scheduled_dry_run(&state.registry, &snapshot, &name);
    if !dry_run && form.confirm.as_deref() != Some("true") {
        return (
            StatusCode::BAD_REQUEST,
            "this workflow changes data: resubmit the form with confirm=true\n",
        )
            .into_response();
    }

    let options = crosstalk_core::runner::RunOptions::default();
    let options = if form.fresh.as_deref().is_some_and(|value| value == "true") {
        options.fresh()
    } else {
        options
    };
    // Shape AND registry: a workflow whose step names no action is a configuration error to
    // report, not a run that fails a second later.
    if let Err(error) = crosstalk_core::workflow::resolve(&state.runner, &name) {
        return (
            StatusCode::BAD_REQUEST,
            format!("workflow {name} cannot run: {error:#}\n"),
        )
            .into_response();
    }
    match crosstalk_core::workflow::start(&state.runner, &name, dry_run, options) {
        Ok(started) => Redirect::to(&format!("/runs/{}", started.run_id)).into_response(),
        Err(error) => (
            StatusCode::CONFLICT,
            format!("could not start the workflow: {error:#}\n"),
        )
            .into_response(),
    }
}

/// `POST /hooks/dispatcharr`: one Dispatcharr Connect delivery.
///
/// Answers 202 as soon as the delivery is understood, because Dispatcharr's webhook handler waits
/// ten seconds for an answer and a match or a probe takes far longer: the work is the debounced
/// task's, not this request's (see [`crosstalk_core::hooks`]).
///
/// Every response after authentication is 202, including an unknown event: Dispatcharr logs a
/// failed delivery for anything else, and "I do not act on this event" is not a delivery failure.
async fn dispatcharr_hook(
    State(state): State<AppState>,
    client: ClientInfo,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let snapshot = state.snapshot();

    // A delivery starts real work, so the route counts them per client. The bound comes from
    // the live configuration, so raising it needs no restart; checking and counting are one
    // step, so a burst of concurrent deliveries cannot all slip under it.
    let limit = snapshot.hooks.rate_per_minute;
    if limit > 0 {
        if let Err(retry) = state.hook_limiter().try_attempt_within(client.key, limit) {
            let mut response = (
                StatusCode::TOO_MANY_REQUESTS,
                format!("too many hook deliveries: retry in {retry}s\n"),
            )
                .into_response();
            if let Ok(value) = header::HeaderValue::from_str(&retry.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
            return response;
        }
    }

    let fields = crosstalk_core::hooks::read_fields(&body);
    let configured: std::collections::BTreeSet<String> =
        snapshot.hooks.events.keys().cloned().collect();
    let resolved = crosstalk_core::hooks::read_event(
        query.get("event").map(String::as_str),
        &fields,
        &configured,
    );
    let Some(event) = resolved else {
        // The count and a few names, not every key: a body is the sender's to fill, and the log
        // is not the place for all of it.
        tracing::info!(
            bytes = body.len(),
            field_count = fields.len(),
            fields = ?log_keys(&fields),
            "hook delivery names no event this configuration knows; ignored"
        );
        return (
            StatusCode::ACCEPTED,
            "accepted: no event named, nothing to do\n",
        )
            .into_response();
    };

    // The payload's own fields, logged (never discarded silently): it is what tells an operator
    // which account refreshed, and it is all Dispatcharr sends about the burst.
    tracing::info!(
        event = event.name.as_str(),
        from = event.source.as_str(),
        account = fields
            .get("account_name")
            .map(|value| truncate(value, LOG_FIELD_BYTES))
            .unwrap_or("-"),
        source = fields
            .get("source_name")
            .map(|value| truncate(value, LOG_FIELD_BYTES))
            .unwrap_or("-"),
        field_count = fields.len(),
        "hook delivery received"
    );
    let delivered = state.hooks().deliver(&event.name);
    let body = match &delivered {
        crosstalk_core::hooks::Delivered::Unknown => {
            format!(
                "accepted: {} is not a configured event\n",
                echo(&event.name)
            )
        }
        crosstalk_core::hooks::Delivered::Unconfigured => {
            format!(
                "accepted: {} has no targets configured\n",
                echo(&event.name)
            )
        }
        crosstalk_core::hooks::Delivered::Coalesced { event } => {
            format!("accepted: joined the settling {} burst\n", echo(event))
        }
        crosstalk_core::hooks::Delivered::Scheduled { event, targets } => {
            format!(
                "accepted: {} runs {targets} target(s) once the burst settles\n",
                echo(event)
            )
        }
    };
    (StatusCode::ACCEPTED, body).into_response()
}

/// How many of a delivery's field names the log line lists.
const LOG_KEYS: usize = 10;
/// The longest field name or value the log line carries, in bytes.
const LOG_FIELD_BYTES: usize = 64;

/// The first [`LOG_KEYS`] field names of a delivery, each cut to [`LOG_FIELD_BYTES`].
fn log_keys(fields: &std::collections::BTreeMap<String, String>) -> Vec<&str> {
    fields
        .keys()
        .take(LOG_KEYS)
        .map(|key| truncate(key, LOG_FIELD_BYTES))
        .collect()
}

/// `text` cut to at most `max` bytes, on a character boundary.
fn truncate(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// An event name as the 202 answer may echo it: the characters an event name is made of, and
/// `?` for anything else, at most 64 of them. Already free of control characters
/// (`hooks::read_event` refuses those), but the answer is the sender's to read, so it says only
/// what it needs to.
fn echo(name: &str) -> String {
    name.chars()
        .take(LOG_FIELD_BYTES)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':') {
                c
            } else {
                '?'
            }
        })
        .collect()
}

async fn runs_page(State(state): State<AppState>, CsrfToken(csrf_token): CsrfToken) -> Response {
    RunsTemplate {
        version: crosstalk_core::HOST_VERSION,
        csrf_token,
        runs: state.store.recent_runs(100).unwrap_or_default(),
    }
    .into_response()
}

async fn load_run(state: &AppState, run_id: i64) -> Option<RunRecord> {
    state.store.run(run_id).ok().flatten()
}

async fn run_page(
    State(state): State<AppState>,
    CsrfToken(csrf_token): CsrfToken,
    Path(id): Path<i64>,
) -> Response {
    match load_run(&state, id).await {
        Some(run) => RunTemplate {
            version: crosstalk_core::HOST_VERSION,
            csrf_token,
            counts: state::counts(&run),
            active: state
                .runner
                .running()
                .into_iter()
                .find(|entry| entry.run_id == id),
            rows: state::rows(&state.store, id),
            report_available: state::report_available(&run),
            steps: state::steps_of(&state.store, &run),
            planned: state::planned_steps(&state.snapshot(), &run),
            is_workflow: run.app == crosstalk_core::workflow::APP_ID,
            parent: state::parent_of(&state.store, &run),
            undo_eligible: state::undo_eligible(&state.registry, &state.store, &run),
            undo_runs: state::undo_runs(&state.store, &run),
            apply_undo_offered: state::is_undo_run(&run)
                && run.dry_run
                && run.parent_run_id.is_some_and(|target| {
                    crosstalk_core::undo::check_target(&state.store, &state.registry, target)
                        .is_ok()
                }),
            run,
        }
        .into_response(),
        None => (StatusCode::NOT_FOUND, "no such run\n").into_response(),
    }
}

async fn run_status(
    State(state): State<AppState>,
    CsrfToken(csrf_token): CsrfToken,
    Path(id): Path<i64>,
) -> Response {
    let Some(run) = load_run(&state, id).await else {
        return (StatusCode::NOT_FOUND, "no such run\n").into_response();
    };
    let active = state
        .runner
        .running()
        .into_iter()
        .find(|entry| entry.run_id == id);
    RunStatusTemplate {
        csrf_token,
        counts: state::counts(&run),
        report_available: state::report_available(&run),
        run,
        active,
    }
    .into_response()
}

async fn run_rows(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    RowsTemplate {
        rows: state::rows(&state.store, id),
    }
    .into_response()
}

async fn run_csv(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let Some(run) = load_run(&state, id).await else {
        return (StatusCode::NOT_FOUND, "no such run\n").into_response();
    };
    let Some(path) = run.report_path else {
        return (StatusCode::NOT_FOUND, "this run wrote no report\n").into_response();
    };
    // Confine the download to the exports directory: the path comes from the database,
    // and a future feature writing user-influenced paths must not turn this into a file
    // read. Both sides are canonicalized so a `..` or a symlink cannot escape. A path
    // that resolves to nothing is gone; one that resolves outside the exports
    // directory does not exist as far as this endpoint is concerned.
    let confined = match std::fs::canonicalize(&path) {
        Ok(resolved) => resolved,
        Err(error) => {
            return (
                StatusCode::GONE,
                format!("the report is no longer on disk: {error}\n"),
            )
                .into_response()
        }
    };
    let exports = match std::fs::canonicalize(state.snapshot().exports_dir()) {
        Ok(directory) => directory,
        Err(_) => return (StatusCode::NOT_FOUND, "no such report\n").into_response(),
    };
    if !confined.starts_with(&exports) {
        return (StatusCode::NOT_FOUND, "no such report\n").into_response();
    }
    match std::fs::read(&confined) {
        Ok(bytes) => {
            let name = std::path::Path::new(&path)
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_else(|| "report.csv".to_string());
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("text/csv"),
            );
            headers.insert(
                header::CONTENT_DISPOSITION,
                header::HeaderValue::from_str(&format!("attachment; filename=\"{name}\""))
                    .unwrap_or_else(|_| header::HeaderValue::from_static("attachment")),
            );
            (headers, bytes).into_response()
        }
        Err(error) => (
            StatusCode::GONE,
            format!("the report is no longer on disk: {error}\n"),
        )
            .into_response(),
    }
}

async fn cancel_run(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    state.runner.cancel(id);
    Redirect::to(&format!("/runs/{id}")).into_response()
}

#[derive(serde::Deserialize)]
struct UndoForm {
    /// Server-side confirmation for the APPLY half, mirroring the CLI's `--yes`: without it
    /// the request starts a preview instead. The browser's confirm dialog is convenience,
    /// not protection.
    #[serde(default)]
    confirm: Option<String>,
}

/// `POST /runs/:id/undo`: start undoing one run.
///
/// Without `confirm=true` this starts a PREVIEW (a dry run that changes nothing) and the
/// undo button's page says so; the preview run's page offers "Apply undo", which posts
/// back here with `confirm=true`. The undo is recorded under the target run's app with
/// `parent_run_id` set to the undone run, so undoing an undo restores the applied state.
async fn start_undo(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Form(form): Form<UndoForm>,
) -> Response {
    let Some(target) = load_run(&state, id).await else {
        return (StatusCode::NOT_FOUND, "no such run\n").into_response();
    };
    let dry_run = form.confirm.as_deref() != Some("true");
    // Preview and apply alike: a refused target starts no run, and the answer says why.
    if let Err(error) = crosstalk_core::undo::check_target(&state.store, &state.registry, target.id)
    {
        return (
            StatusCode::BAD_REQUEST,
            format!("this run cannot be undone: {error:#}\n"),
        )
            .into_response();
    }
    match state.runner.start_undo_with(
        &target.app,
        id,
        dry_run,
        crosstalk_core::runner::RunOptions::default(),
    ) {
        Ok(started) => Redirect::to(&format!("/runs/{}", started.run_id)).into_response(),
        Err(error) => (
            StatusCode::CONFLICT,
            format!("could not start the undo: {error:#}\n"),
        )
            .into_response(),
    }
}

async fn settings_page(
    State(state): State<AppState>,
    CsrfToken(csrf_token): CsrfToken,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let saved = query.get("saved").is_some_and(|value| value == "1");
    let env_overrides = query.get("env_overrides").is_some_and(|value| value == "1");
    let snapshot = state.snapshot();
    SettingsTemplate {
        version: crosstalk_core::HOST_VERSION,
        csrf_token,
        saved,
        host_error: None,
        host_notice: env_overrides.then(|| {
            "Some values come from environment variables and override this form: the page \
             shows the effective values, and saving here will not change them until the \
             environment override is removed."
                .to_string()
        }),
        host_fields: state::fields(
            &crosstalk_core::config::host_schema(),
            &crosstalk_core::config::host_values(&snapshot),
        ),
        apps: app_forms(&state, None),
    }
    .into_response()
}

fn app_forms(state: &AppState, error: Option<String>) -> Vec<UiSettings> {
    let config_dir = state.config.config_dir();
    state
        .registry
        .iter()
        .map(|app| {
            let path = app.settings_path(&config_dir);
            let values = app.load_settings(&path).unwrap_or_default();
            UiSettings {
                id: app.id().to_string(),
                name: app.name().to_string(),
                fields: state::fields(app.schema(), &values),
                error: error.clone(),
            }
        })
        .collect()
}

/// Render the settings page with a host-level error, keeping the just-submitted values.
fn settings_with_host_error(state: &AppState, csrf_token: String, error: String) -> Response {
    let snapshot = state.snapshot();
    SettingsTemplate {
        version: crosstalk_core::HOST_VERSION,
        csrf_token,
        saved: false,
        host_error: Some(error),
        host_notice: None,
        host_fields: state::fields(
            &crosstalk_core::config::host_schema(),
            &crosstalk_core::config::host_values(&snapshot),
        ),
        apps: app_forms(state, None),
    }
    .into_response()
}

async fn save_host_settings(
    State(state): State<AppState>,
    CsrfToken(csrf_token): CsrfToken,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let config_dir = state.config.config_dir();
    // Saved into the FILE-only configuration, never the effective one: writing the effective
    // config would copy an environment-supplied secret into the file.
    let mut config = match crosstalk_core::CrosstalkConfig::load_file(&config_dir) {
        Ok(config) => config,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not read the configuration: {error:#}\n"),
            )
                .into_response()
        }
    };

    // What was already wrong before this save. The form cannot fix a missing Dispatcharr
    // key, so those problems must not block saving; a problem the save INTRODUCES must.
    let already_wrong = config.validate().errors;

    // The schema decides the types, as it does for an app's own form. A control the page
    // did not submit is left alone rather than cleared, so a partial POST cannot reset
    // values it never mentioned; an unticked checkbox is the exception, because browsers
    // send nothing for it and "absent" there means false.
    let schema = crosstalk_core::config::host_schema();
    let mut values = match state::values_from_form(&schema, &form) {
        Ok(values) => values,
        Err(errors) => {
            return (StatusCode::BAD_REQUEST, format!("{}\n", errors.join("; "))).into_response()
        }
    };
    for field in schema.editable() {
        if field.kind != crosstalk_core::subapp::FieldKind::Boolean && !form.contains_key(field.id)
        {
            values.remove(field.id);
        }
    }
    if let Err(error) = crosstalk_core::config::apply_host_values(&mut config, &values) {
        return (StatusCode::BAD_REQUEST, format!("{error:#}\n")).into_response();
    }
    // Cross-field rules (the AI band, the scope conflict between groups and ignore_groups)
    // were caught when these fields lived on an app's form, and they still are: a save that
    // introduces one is refused before it reaches the file.
    let introduced: Vec<String> = config
        .validate()
        .errors
        .into_iter()
        .filter(|error| !already_wrong.contains(error))
        .collect();
    if !introduced.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            format!("this change is inconsistent: {}\n", introduced.join("; ")),
        )
            .into_response();
    }

    let path = config_dir.join("crosstalk.toml");
    if let Err(error) = config.save(&path) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not save the configuration: {error:#}\n"),
        )
            .into_response();
    }

    // Hot reload: swap the daemon's live config so the NEXT run uses the saved values, and
    // re-arm the schedules from them. A run already in flight keeps the snapshot it started
    // with. When the reload or the re-arm fails, the file IS saved but the daemon still runs
    // the old values, so say so on the page instead of claiming success.
    let fresh = match crosstalk_core::CrosstalkConfig::load(&config_dir) {
        Ok(fresh) => fresh,
        Err(error) => {
            tracing::error!("host settings saved but the config would not reload: {error:#}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                settings_with_host_error(
                    &state,
                    csrf_token.clone(),
                    format!(
                        "Saved to {path}, but the daemon could not reload it and still runs the \
                         old values: {error:#}",
                        path = path.display()
                    ),
                ),
            )
                .into_response();
        }
    };
    // Refuse a swap that would leave the daemon unauthenticated: a non-loopback listener
    // with no token is a configuration the daemon refuses to start with, so it must not be
    // swapped in at runtime either. The file IS saved at this point, so say so: a restart
    // would refuse to start until a token is restored.
    // The same listener rules the daemon starts under: a too-short token on a reachable bind,
    // trusted proxies without a token, a bad CIDR or public URL. The file is saved; the daemon
    // keeps running the old values.
    let security = fresh.server.security_errors();
    if !security.is_empty() {
        tracing::warn!(
            problems = security.join("; ").as_str(),
            "host settings saved but NOT applied: the reloaded [server] settings are unsafe"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            settings_with_host_error(
                &state,
                csrf_token.clone(),
                format!(
                    "Saved to {path}, but NOT applied: {}. A restart with this file would \
                     refuse to start.",
                    security.join("; "),
                    path = path.display(),
                ),
            ),
        )
            .into_response();
    }
    if crate::auth::requires_token(&fresh.server.bind, fresh.server.resolved_token().as_deref()) {
        tracing::warn!(
            bind = fresh.server.bind.as_str(),
            "host settings saved but NOT applied: the reloaded config has a non-loopback bind with no token"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            settings_with_host_error(
                &state,
                csrf_token.clone(),
                format!(
                    "Saved to {path}, but NOT applied: the reloaded configuration binds \
                     {bind} with no token, which would leave the daemon unauthenticated. A \
                     restart with this file would refuse to start. Restore a token \
                     (server.token or server.token_env) and save again.",
                    path = path.display(),
                    bind = fresh.server.bind,
                ),
            ),
        )
            .into_response();
    }

    // The file holds exactly what the form submitted, so any difference between it and the
    // effective config is an environment override the form cannot change. Name it on the
    // page rather than showing values the operator cannot edit into effect.
    let overridden = env_override_fields(&config, &fresh);
    if !overridden.is_empty() {
        tracing::warn!(
            fields = overridden.join(", ").as_str(),
            "host settings saved, but environment variables override the saved values"
        );
    }

    let previous = state.snapshot();
    let previous_schedules = previous.schedules;
    // A workflow's steps are what decide a `workflow.<name>` schedule's dry-run flag, and a
    // schedule naming a workflow that did not exist was not armed at all: both make the
    // workflows part of "did anything that decides the armed jobs change".
    let previous_workflows = previous.workflows;
    state.config.replace(fresh);
    let snapshot = state.snapshot();
    // The re-arm only matters when what the armed jobs DO changed: re-adding identical jobs
    // would churn every schedule's next firing for no reason.
    if previous_schedules != snapshot.schedules || previous_workflows != snapshot.workflows {
        if let Some(scheduler) = &state.scheduler {
            if let Err(error) = scheduler.lock().await.rearm(&state.runner, &snapshot).await {
                tracing::error!(
                    "host settings applied but the schedules would not re-arm: {error:#}"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    settings_with_host_error(
                        &state,
                        csrf_token.clone(),
                        format!("Settings applied, but the schedules would not re-arm: {error:#}"),
                    ),
                )
                    .into_response();
            }
        }
    }
    tracing::info!(
        ignore_quality = snapshot.matching.ignore_quality_tags,
        databases = ?snapshot.matching.databases(),
        schedules = snapshot.schedules.len(),
        "host settings saved and applied without a restart"
    );

    if overridden.is_empty() {
        Redirect::to("/settings?saved=1").into_response()
    } else {
        Redirect::to("/settings?saved=1&env_overrides=1").into_response()
    }
}

/// The host-form fields where the effective config differs from the file.
///
/// After a save the file holds exactly what the form submitted, so a difference here means
/// an environment variable is overriding the UI for that field: the page shows the effective
/// value, and saving the form cannot change it until the override is removed. Compared over
/// the form's own values so a field added to the schema is covered without a second list to
/// keep in step.
fn env_override_fields(
    file: &crosstalk_core::CrosstalkConfig,
    effective: &crosstalk_core::CrosstalkConfig,
) -> Vec<&'static str> {
    let from_file = crosstalk_core::config::host_values(file);
    let from_env = crosstalk_core::config::host_values(effective);
    crosstalk_core::config::host_schema()
        .editable()
        .filter(|field| from_file.get(field.id) != from_env.get(field.id))
        .map(|field| field.id)
        .collect()
}

async fn save_app_settings(
    State(state): State<AppState>,
    CsrfToken(csrf_token): CsrfToken,
    Path(app_id): Path<String>,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let Some(app) = state.registry.get(&app_id) else {
        return (StatusCode::NOT_FOUND, "no such app\n").into_response();
    };

    let values = match state::values_from_form(app.schema(), &form) {
        Ok(values) => values,
        Err(errors) => {
            let snapshot = state.snapshot();
            return SettingsTemplate {
                version: crosstalk_core::HOST_VERSION,
                csrf_token: csrf_token.clone(),
                saved: false,
                host_error: None,
                host_notice: None,
                host_fields: state::fields(
                    &crosstalk_core::config::host_schema(),
                    &crosstalk_core::config::host_values(&snapshot),
                ),
                apps: app_forms(&state, Some(errors.join("; "))),
            }
            .into_response();
        }
    };

    let path = app.settings_path(&state.config.config_dir());
    if let Err(error) = app.save_settings(&path, &values) {
        let snapshot = state.snapshot();
        return SettingsTemplate {
            version: crosstalk_core::HOST_VERSION,
            csrf_token: csrf_token.clone(),
            saved: false,
            host_error: None,
            host_notice: None,
            host_fields: state::fields(
                &crosstalk_core::config::host_schema(),
                &crosstalk_core::config::host_values(&snapshot),
            ),
            apps: app_forms(&state, Some(format!("{error:#}"))),
        }
        .into_response();
    }

    // Record the change, so a run's behaviour can be explained later.
    let as_json = serde_json::to_value(
        values
            .iter()
            .map(|(key, value)| (key.clone(), value.render()))
            .collect::<HashMap<_, _>>(),
    )
    .unwrap_or_default();
    if let Err(error) = state.store.record_settings_change(&app_id, &as_json) {
        tracing::warn!("could not record the settings change: {error:#}");
    }

    Redirect::to("/settings?saved=1").into_response()
}

// --- JSON API ----------------------------------------------------------------------

async fn api_apps(State(state): State<AppState>) -> Json<serde_json::Value> {
    let apps: Vec<serde_json::Value> = state
        .registry
        .iter()
        .map(|app| {
            serde_json::json!({
                "id": app.id(),
                "name": app.name(),
                "description": app.description(),
                "actions": app.actions().iter().map(|action| serde_json::json!({
                    "id": action.id,
                    "label": action.label,
                    "kind": format!("{:?}", action.kind).to_lowercase(),
                    "needs_dispatcharr": action.needs_dispatcharr,
                })).collect::<Vec<_>>(),
                "settings": app.schema().editable().map(|field| field.id).collect::<Vec<_>>(),
            })
        })
        .collect();
    Json(serde_json::json!({ "apps": apps }))
}

async fn api_runs(State(state): State<AppState>) -> Json<serde_json::Value> {
    let runs = state.store.recent_runs(100).unwrap_or_default();
    Json(serde_json::json!({ "runs": runs }))
}

async fn api_run(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    match load_run(&state, id).await {
        Some(run) => {
            let active = state
                .runner
                .running()
                .into_iter()
                .find(|entry| entry.run_id == id);
            Json(serde_json::json!({
                "run": run,
                "active": active.map(|entry| serde_json::json!({
                    "current": entry.current,
                    "total": entry.total,
                    "phase": entry.phase,
                    "cancelling": entry.cancelling,
                })),
            }))
            .into_response()
        }
        None => (StatusCode::NOT_FOUND, "no such run\n").into_response(),
    }
}

async fn api_run_rows(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "rows": state.store.rows(id).unwrap_or_default() }))
}

/// Whether a run status is terminal, for callers polling the API.
pub fn is_terminal(status: RunStatus) -> bool {
    !matches!(status, RunStatus::Running)
}

/// The registry, for the daemon's start-up logging.
pub fn describe_registry(registry: &Registry) -> String {
    registry
        .iter()
        .map(|app| format!("{} ({})", app.name(), app.id()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::{echo, env_override_fields, log_keys, truncate};
    use crosstalk_core::config::CrosstalkConfig;

    #[test]
    fn the_hook_log_names_a_few_short_keys() {
        let fields: std::collections::BTreeMap<String, String> = (0..25)
            .map(|n| (format!("{n:02}{}", "k".repeat(100)), String::new()))
            .collect();
        let keys = log_keys(&fields);
        assert_eq!(keys.len(), 10);
        assert!(keys.iter().all(|key| key.len() == 64), "{keys:?}");
        // A multi-byte character straddling the cut is dropped whole, not split.
        let text = format!("{}é", "a".repeat(63));
        assert_eq!(truncate(&text, 64), "a".repeat(63));
        assert_eq!(truncate("short", 64), "short");
    }

    #[test]
    fn an_echoed_event_name_is_tame() {
        assert_eq!(echo("m3u_refresh"), "m3u_refresh");
        assert_eq!(echo("a b<c>"), "a?b?c?");
        assert_eq!(echo(&"e".repeat(200)).len(), 64);
    }

    #[test]
    fn identical_configs_have_no_overrides() {
        let file = CrosstalkConfig::default();
        let effective = CrosstalkConfig::default();
        assert!(env_override_fields(&file, &effective).is_empty());
    }

    #[test]
    fn every_host_field_the_form_edits_is_compared() {
        // The shared settings moved into the same form: a difference an environment variable
        // makes there has to be named too, or the page would show values that cannot be saved.
        let file = CrosstalkConfig::default();
        let mut effective = CrosstalkConfig {
            matching: crosstalk_core::config::MatchingConfig {
                ignore_quality_tags: !file.matching.ignore_quality_tags,
                ignore_regional_tags: !file.matching.ignore_regional_tags,
                ignore_geographic_tags: !file.matching.ignore_geographic_tags,
                ignore_misc_tags: !file.matching.ignore_misc_tags,
                enabled_databases: vec!["UK".to_string()],
                ..crosstalk_core::config::MatchingConfig::default()
            },
            scope: crosstalk_core::config::ScopeConfig {
                profiles: "All".to_string(),
                groups: String::new(),
                ignore_groups: "PPV".to_string(),
            },
            exports: crosstalk_core::config::ExportsConfig { retention_days: 7 },
            ..CrosstalkConfig::default()
        };
        effective
            .matching
            .custom_aliases
            .insert("ESPN".to_string(), vec!["ESPN US".to_string()]);
        effective.ai.verify.enabled = !file.ai.verify.enabled;
        effective.ai.verify.model = "another-model".to_string();

        let fields = env_override_fields(&file, &effective);
        for expected in [
            "matching.ignore_quality_tags",
            "matching.ignore_regional_tags",
            "matching.ignore_geographic_tags",
            "matching.ignore_misc_tags",
            "matching.enabled_databases",
            "matching.custom_aliases",
            "scope.profiles",
            "scope.ignore_groups",
            "exports.retention_days",
            "ai.verify.enabled",
            "ai.verify.model",
        ] {
            assert!(fields.contains(&expected), "{expected} missing: {fields:?}");
        }
        assert!(
            !fields.contains(&"scope.groups"),
            "an unchanged field is not named: {fields:?}"
        );
    }

    #[test]
    fn only_the_differing_fields_are_named() {
        let file = CrosstalkConfig::default();
        let effective = CrosstalkConfig {
            matching: crosstalk_core::config::MatchingConfig {
                ignore_quality_tags: !file.matching.ignore_quality_tags,
                ..crosstalk_core::config::MatchingConfig::default()
            },
            ..CrosstalkConfig::default()
        };
        assert_eq!(
            env_override_fields(&file, &effective),
            vec!["matching.ignore_quality_tags"]
        );
    }
}
