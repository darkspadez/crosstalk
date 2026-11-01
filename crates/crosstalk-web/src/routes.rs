//! The HTTP routes: pages, the JSON API, and the actions that start runs.

use crate::auth::{self, COOKIE};
use crate::state::{self, AppState, UiApp, UiField, UiRow, UiSettings};
use askama::Template;
use axum::extract::{Form, Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use crosstalk_core::store::{RunRecord, RunStatus};
use crosstalk_core::subapp::Registry;
use std::collections::HashMap;
use tower_http::trace::TraceLayer;

/// Build the router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/login", get(login_page).post(login))
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
        .route("/settings", get(settings_page))
        .route("/settings/host", post(save_host_settings))
        .route("/settings/:app", post(save_app_settings))
        .route("/api/v1/apps", get(api_apps))
        .route("/api/v1/runs", get(api_runs))
        .route("/api/v1/runs/:id", get(api_run))
        .route("/api/v1/runs/:id/rows", get(api_run_rows))
        // Embedded rather than read from disk: the runtime image has no source tree, and an
        // install behind no internet still needs its stylesheet and htmx.
        .route("/static/app.css", get(stylesheet))
        .route("/static/htmx.min.js", get(htmx))
        .route("/static/confirm.js", get(confirm_script))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require,
        ))
        // Outside auth, so a forged cross-site POST fails closed even with no token set.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::csrf::check,
        ))
        // Outside CSRF, so a DNS-rebinding Host is rejected before anything trusts it.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::host::check,
        ))
        .layer(TraceLayer::new_for_http())
        // Outside everything, so even a rejection from an inner layer (a 403 from the
        // Host or CSRF checks, a 401, a redirect) carries the browser security headers.
        .layer(axum::middleware::from_fn(crate::security::headers))
        .with_state(state)
}

// --- templates ---------------------------------------------------------------------

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
    runs: Vec<RunRecord>,
    warnings: Vec<String>,
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
}

#[derive(Template)]
#[template(path = "_run_status.html")]
struct RunStatusTemplate {
    csrf_token: String,
    run: RunRecord,
    active: Option<crosstalk_core::runner::ActiveRun>,
    counts: Vec<(String, i64)>,
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

async fn login_page() -> LoginTemplate {
    LoginTemplate { error: None }
}

#[derive(serde::Deserialize)]
struct LoginForm {
    token: String,
}

async fn login(
    State(state): State<AppState>,
    connect: Option<axum::extract::ConnectInfo<std::net::SocketAddr>>,
    Form(form): Form<LoginForm>,
) -> Response {
    let Some(expected) = state.token() else {
        // No token configured: there is nothing to sign in to.
        return Redirect::to("/").into_response();
    };

    // Per-IP failure counting: ten wrong tokens in five minutes locks the address out
    // with a 429, so the single all-powerful token cannot be guessed at line rate.
    // Without connection info (tests, some proxies) every attempt shares one bucket,
    // which only makes the limit stricter.
    let ip = connect
        .map(|info| info.0.ip())
        .unwrap_or_else(|| std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    if !state.login_limiter().allowed(ip) {
        let mut response = (
            StatusCode::TOO_MANY_REQUESTS,
            "too many failed sign-in attempts: wait a few minutes and try again\n",
        )
            .into_response();
        if let Ok(value) = header::HeaderValue::from_str(
            &state
                .login_limiter()
                .retry_after_secs(ip)
                .max(1)
                .to_string(),
        ) {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        return response;
    }

    if !crate::auth::tokens_match(&expected, form.token.trim()) {
        state.login_limiter().record_failure(ip);
        return LoginTemplate {
            error: Some("That token does not match the configured one.".to_string()),
        }
        .into_response();
    }
    state.login_limiter().record_success(ip);

    let cookie = format!(
        "{COOKIE}={}; Path=/; HttpOnly; SameSite=Strict",
        form.token.trim()
    );
    let mut response = Redirect::to("/").into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        header::HeaderValue::from_str(&cookie)
            .unwrap_or_else(|_| header::HeaderValue::from_static("crosstalk_token=; Path=/")),
    );
    response
}

async fn logout() -> Response {
    let mut response = Redirect::to("/login").into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        header::HeaderValue::from_static("crosstalk_token=; Path=/; Max-Age=0"),
    );
    response
}

async fn dashboard(State(state): State<AppState>) -> Response {
    let runs = state.store.recent_runs(20).unwrap_or_default();
    let snapshot = state.snapshot();
    let warnings = snapshot.validate().warnings;
    DashboardTemplate {
        version: crosstalk_core::HOST_VERSION,
        csrf_token: state.csrf_token().to_string(),
        apps: state::apps(&state.registry, &state.store),
        schedules: state::schedules(&snapshot),
        runs,
        warnings,
    }
    .into_response()
}

async fn app_page(State(state): State<AppState>, Path(app_id): Path<String>) -> Response {
    let Some(app) = state.registry.get(&app_id) else {
        return (StatusCode::NOT_FOUND, "no such app\n").into_response();
    };

    AppTemplate {
        version: crosstalk_core::HOST_VERSION,
        csrf_token: state.csrf_token().to_string(),
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
struct StartForm {
    #[serde(default)]
    dry_run: Option<String>,
    /// Server-side confirmation for channel-changing actions, mirroring the CLI's `--yes`.
    /// The UI's confirm dialog submits it; a forged POST without it is rejected.
    #[serde(default)]
    confirm: Option<String>,
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

    match state.runner.start(&app_id, &action_id, dry_run) {
        Ok(started) => Redirect::to(&format!("/runs/{}", started.run_id)).into_response(),
        Err(error) => (
            StatusCode::CONFLICT,
            format!("could not start the run: {error:#}\n"),
        )
            .into_response(),
    }
}

async fn runs_page(State(state): State<AppState>) -> Response {
    RunsTemplate {
        version: crosstalk_core::HOST_VERSION,
        csrf_token: state.csrf_token().to_string(),
        runs: state.store.recent_runs(100).unwrap_or_default(),
    }
    .into_response()
}

async fn load_run(state: &AppState, run_id: i64) -> Option<RunRecord> {
    state.store.run(run_id).ok().flatten()
}

async fn run_page(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    match load_run(&state, id).await {
        Some(run) => RunTemplate {
            version: crosstalk_core::HOST_VERSION,
            csrf_token: state.csrf_token().to_string(),
            counts: state::counts(&run),
            active: state
                .runner
                .running()
                .into_iter()
                .find(|entry| entry.run_id == id),
            rows: state::rows(&state.store, id),
            run,
        }
        .into_response(),
        None => (StatusCode::NOT_FOUND, "no such run\n").into_response(),
    }
}

async fn run_status(State(state): State<AppState>, Path(id): Path<i64>) -> Response {
    let Some(run) = load_run(&state, id).await else {
        return (StatusCode::NOT_FOUND, "no such run\n").into_response();
    };
    let active = state
        .runner
        .running()
        .into_iter()
        .find(|entry| entry.run_id == id);
    RunStatusTemplate {
        csrf_token: state.csrf_token().to_string(),
        counts: state::counts(&run),
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

async fn settings_page(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let saved = query.get("saved").is_some_and(|value| value == "1");
    let env_overrides = query.get("env_overrides").is_some_and(|value| value == "1");
    let snapshot = state.snapshot();
    SettingsTemplate {
        version: crosstalk_core::HOST_VERSION,
        csrf_token: state.csrf_token().to_string(),
        saved,
        host_error: None,
        host_notice: env_overrides.then(|| {
            "Some values come from environment variables and override this form: the page \
             shows the effective values, and saving here will not change them until the \
             environment override is removed."
                .to_string()
        }),
        host_fields: state::fields(
            &crosstalk_core::config::matching_schema(),
            &host_values(&snapshot),
        ),
        apps: app_forms(&state, None),
    }
    .into_response()
}

/// The host's own settings as values, for the form.
fn host_values(config: &crosstalk_core::CrosstalkConfig) -> crosstalk_core::SettingsValues {
    use crosstalk_core::subapp::{FieldValue, SettingsValues};
    let mut values = SettingsValues::new();
    values.insert(
        "matching.ignore_quality_tags",
        FieldValue::Boolean(config.matching.ignore_quality_tags),
    );
    values.insert(
        "matching.ignore_regional_tags",
        FieldValue::Boolean(config.matching.ignore_regional_tags),
    );
    values.insert(
        "matching.ignore_geographic_tags",
        FieldValue::Boolean(config.matching.ignore_geographic_tags),
    );
    values.insert(
        "matching.ignore_misc_tags",
        FieldValue::Boolean(config.matching.ignore_misc_tags),
    );
    values.insert(
        "matching.enabled_databases",
        FieldValue::Text(config.matching.databases().join(", ")),
    );
    values.insert(
        "matching.custom_aliases",
        FieldValue::Json(config.matching.custom_aliases.clone()),
    );
    values
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
fn settings_with_host_error(state: &AppState, error: String) -> Response {
    let snapshot = state.snapshot();
    SettingsTemplate {
        version: crosstalk_core::HOST_VERSION,
        csrf_token: state.csrf_token().to_string(),
        saved: false,
        host_error: Some(error),
        host_notice: None,
        host_fields: state::fields(
            &crosstalk_core::config::matching_schema(),
            &host_values(&snapshot),
        ),
        apps: app_forms(state, None),
    }
    .into_response()
}

async fn save_host_settings(
    State(state): State<AppState>,
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

    let flag = |key: &str| form.get(key).is_some_and(|value| value == "true");
    config.matching.ignore_quality_tags = flag("matching.ignore_quality_tags");
    config.matching.ignore_regional_tags = flag("matching.ignore_regional_tags");
    config.matching.ignore_geographic_tags = flag("matching.ignore_geographic_tags");
    config.matching.ignore_misc_tags = flag("matching.ignore_misc_tags");
    config.matching.enabled_databases = form
        .get("matching.enabled_databases")
        .map(|value| {
            value
                .split([',', '\n'])
                .map(str::trim)
                .filter(|code| !code.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    if let Some(text) = form.get("matching.custom_aliases") {
        let text = text.trim();
        if !text.is_empty() {
            match serde_json::from_str::<std::collections::BTreeMap<String, Vec<String>>>(text) {
                Ok(map) => config.matching.custom_aliases = map,
                Err(error) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        format!(
                            "custom aliases must be a JSON object of name -> [aliases]: {error}\n"
                        ),
                    )
                        .into_response()
                }
            }
        } else {
            config.matching.custom_aliases.clear();
        }
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
    if crate::auth::requires_token(&fresh.server.bind, fresh.server.resolved_token().as_deref()) {
        tracing::warn!(
            bind = fresh.server.bind.as_str(),
            "host settings saved but NOT applied: the reloaded config has a non-loopback bind with no token"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            settings_with_host_error(
                &state,
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
    let overridden = env_override_fields(&config.matching, &fresh.matching);
    if !overridden.is_empty() {
        tracing::warn!(
            fields = overridden.join(", ").as_str(),
            "host settings saved, but environment variables override the saved values"
        );
    }

    let previous_schedules = state.snapshot().schedules;
    state.config.replace(fresh);
    let snapshot = state.snapshot();
    // The re-arm only matters when the expressions changed: re-adding identical jobs would
    // churn every schedule's next firing for no reason.
    if previous_schedules != snapshot.schedules {
        if let Some(scheduler) = &state.scheduler {
            if let Err(error) = scheduler.lock().await.rearm(&state.runner, &snapshot).await {
                tracing::error!(
                    "host settings applied but the schedules would not re-arm: {error:#}"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    settings_with_host_error(
                        &state,
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

/// The host-matching fields where the effective config differs from the file.
///
/// After a save the file holds exactly what the form submitted, so a difference here means
/// an environment variable is overriding the UI for that field: the page shows the effective
/// value, and saving the form cannot change it until the override is removed.
fn env_override_fields(
    file: &crosstalk_core::config::MatchingConfig,
    effective: &crosstalk_core::config::MatchingConfig,
) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if file.ignore_quality_tags != effective.ignore_quality_tags {
        fields.push("matching.ignore_quality_tags");
    }
    if file.ignore_regional_tags != effective.ignore_regional_tags {
        fields.push("matching.ignore_regional_tags");
    }
    if file.ignore_geographic_tags != effective.ignore_geographic_tags {
        fields.push("matching.ignore_geographic_tags");
    }
    if file.ignore_misc_tags != effective.ignore_misc_tags {
        fields.push("matching.ignore_misc_tags");
    }
    if file.databases() != effective.databases() {
        fields.push("matching.enabled_databases");
    }
    if file.custom_aliases != effective.custom_aliases {
        fields.push("matching.custom_aliases");
    }
    fields
}

async fn save_app_settings(
    State(state): State<AppState>,
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
                csrf_token: state.csrf_token().to_string(),
                saved: false,
                host_error: None,
                host_notice: None,
                host_fields: state::fields(
                    &crosstalk_core::config::matching_schema(),
                    &host_values(&snapshot),
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
            csrf_token: state.csrf_token().to_string(),
            saved: false,
            host_error: None,
            host_notice: None,
            host_fields: state::fields(
                &crosstalk_core::config::matching_schema(),
                &host_values(&snapshot),
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
    use super::env_override_fields;
    use crosstalk_core::config::MatchingConfig;

    #[test]
    fn identical_matching_configs_have_no_overrides() {
        let file = MatchingConfig::default();
        let effective = MatchingConfig::default();
        assert!(env_override_fields(&file, &effective).is_empty());
    }

    #[test]
    fn every_matching_field_the_form_edits_is_compared() {
        let file = MatchingConfig::default();
        let mut effective = MatchingConfig {
            ignore_quality_tags: !file.ignore_quality_tags,
            ignore_regional_tags: !file.ignore_regional_tags,
            ignore_geographic_tags: !file.ignore_geographic_tags,
            ignore_misc_tags: !file.ignore_misc_tags,
            enabled_databases: vec!["UK".to_string()],
            ..MatchingConfig::default()
        };
        effective
            .custom_aliases
            .insert("ESPN".to_string(), vec!["ESPN US".to_string()]);

        let fields = env_override_fields(&file, &effective);
        assert!(
            fields.contains(&"matching.ignore_quality_tags"),
            "{fields:?}"
        );
        assert!(
            fields.contains(&"matching.ignore_regional_tags"),
            "{fields:?}"
        );
        assert!(
            fields.contains(&"matching.ignore_geographic_tags"),
            "{fields:?}"
        );
        assert!(fields.contains(&"matching.ignore_misc_tags"), "{fields:?}");
        assert!(fields.contains(&"matching.enabled_databases"), "{fields:?}");
        assert!(fields.contains(&"matching.custom_aliases"), "{fields:?}");
    }

    #[test]
    fn only_the_differing_fields_are_named() {
        let file = MatchingConfig::default();
        let effective = MatchingConfig {
            ignore_quality_tags: !file.ignore_quality_tags,
            ..MatchingConfig::default()
        };
        assert_eq!(
            env_override_fields(&file, &effective),
            vec!["matching.ignore_quality_tags"]
        );
    }
}
