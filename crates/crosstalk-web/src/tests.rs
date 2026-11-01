//! Route-level tests: the pages render, the actions start runs, and auth is enforced.

use crate::auth;
use crate::routes::router;
use crate::state::AppState;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use crosstalk_core::config::CrosstalkConfig;
use crosstalk_core::runner::JobRunner;
use crosstalk_core::store::Store;
use crosstalk_core::subapp::{
    ActionKind, ActionSpec, JobOutcome, Registry, SettingsSchema, SettingsValues, SubApp,
};
use std::path::Path;
use std::sync::Arc;
use tower::ServiceExt;

struct EchoApp;

impl SubApp for EchoApp {
    fn id(&self) -> &'static str {
        "echo"
    }

    fn name(&self) -> &'static str {
        "Echo"
    }

    fn description(&self) -> &'static str {
        "writes a row"
    }

    fn actions(&self) -> Vec<ActionSpec> {
        vec![
            ActionSpec::new("peek", "Peek", "reads", ActionKind::Read),
            ActionSpec::new("poke", "Poke", "writes", ActionKind::Write)
                .with_confirm("This writes data. Continue?"),
        ]
    }

    fn schema(&self) -> &SettingsSchema {
        static SCHEMA: std::sync::OnceLock<SettingsSchema> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(SettingsSchema::default)
    }

    fn load_settings(&self, _path: &Path) -> anyhow::Result<SettingsValues> {
        Ok(SettingsValues::new())
    }

    fn save_settings(&self, _path: &Path, _values: &SettingsValues) -> anyhow::Result<()> {
        Ok(())
    }

    fn run<'a>(
        &'a self,
        _action: &'a str,
        context: &'a crosstalk_core::jobs::JobContext,
        _dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<JobOutcome>> + Send + 'a>>
    {
        Box::pin(async move {
            context.record_rows(&[crosstalk_core::store::RunRow {
                ordinal: 0,
                channel_id: 1,
                channel_name: "ESPN".to_string(),
                channel_group: "Sports".to_string(),
                score: 96,
                method: "Callsign".to_string(),
                status: "MATCHED".to_string(),
                reason: "Excellent match (96%)".to_string(),
                ..Default::default()
            }])?;
            Ok(JobOutcome::new("one channel matched").with_count("matched", 1))
        })
    }
}

fn state(token: Option<&str>) -> (AppState, Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = CrosstalkConfig::load(dir.path()).expect("config");
    config.server.token = token.unwrap_or_default().to_string();
    config.server.token_env = String::new();
    config.jobs.exports_dir = dir.path().join("exports").display().to_string();

    let store = Store::in_memory().expect("store");
    let registry = Registry::new(vec![Arc::new(EchoApp)]);
    // ONE shared config for the runner and the UI, like the daemon: a save the UI applies
    // is the value the runner's next run snapshots.
    let shared = crosstalk_core::config::SharedConfig::new(config);
    let runner = JobRunner::new(
        Registry::new(vec![Arc::new(EchoApp)]),
        shared.clone(),
        store.clone(),
    );
    let state = AppState::new(registry, shared, runner, store.clone());
    (state, store, dir)
}

async fn get(state: &AppState, uri: &str) -> (StatusCode, String) {
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("body");
    (status, String::from_utf8_lossy(&body).to_string())
}

async fn get_with_cookie(state: &AppState, uri: &str, token: &str) -> (StatusCode, String) {
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("cookie", format!("{}={token}", auth::COOKIE))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("body");
    (status, String::from_utf8_lossy(&body).to_string())
}

async fn post_form(state: &AppState, uri: &str, body: &str) -> StatusCode {
    post_form_with_csrf(state, uri, body, true).await
}

/// POST a form with extra headers (Host, Origin, Authorization), always with the CSRF
/// header unless the caller opts out via `with_csrf = false`.
async fn post_raw(
    state: &AppState,
    uri: &str,
    body: &str,
    headers: &[(&str, &str)],
    with_csrf: bool,
) -> (StatusCode, axum::http::HeaderMap) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/x-www-form-urlencoded");
    if with_csrf {
        builder = builder.header(crate::csrf::CSRF_HEADER, state.csrf_token());
    }
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = router(state.clone())
        .oneshot(builder.body(Body::from(body.to_string())).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    (status, headers)
}

/// POST a form, with or without this daemon's CSRF token. Scripts send the token via the
/// `x-csrf-token` header; browsers send the hidden form field instead, which the
/// middleware reads from the body the same way.
async fn post_form_with_csrf(
    state: &AppState,
    uri: &str,
    body: &str,
    with_csrf: bool,
) -> StatusCode {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/x-www-form-urlencoded");
    if with_csrf {
        builder = builder.header(crate::csrf::CSRF_HEADER, state.csrf_token());
    }
    let response = router(state.clone())
        .oneshot(builder.body(Body::from(body.to_string())).expect("request"))
        .await
        .expect("response");
    response.status()
}

#[tokio::test]
async fn the_dashboard_lists_the_apps_and_their_actions() {
    let (state, _store, _dir) = state(None);
    let (status, body) = get(&state, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Dashboard"), "{body}");
    assert!(body.contains("Echo"), "{body}");
    assert!(body.contains("No schedules"), "an empty config says so");
}

#[tokio::test]
async fn an_app_page_shows_every_action_with_its_colour() {
    let (state, _store, _dir) = state(None);
    let (status, body) = get(&state, "/apps/echo").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("tag action-read"), "{body}");
    assert!(body.contains("tag action-write"), "{body}");
    // A writing action confirms before running: via a data-confirm attribute read by the
    // self-hosted confirm.js, never an inline onsubmit handler (the CSP forbids those).
    assert!(body.contains("data-confirm="), "{body}");
    assert!(!body.contains("onsubmit"), "{body}");
    assert!(
        body.contains("This writes data. Continue?"),
        "the confirm message survives: {body}"
    );
}

#[tokio::test]
async fn an_unknown_app_is_a_404() {
    let (state, _store, _dir) = state(None);
    let (status, _body) = get(&state, "/apps/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn starting_an_action_redirects_to_its_run_page() {
    let (state, store, _dir) = state(None);
    let status = post_form(&state, "/apps/echo/actions/peek", "dry_run=true").await;
    assert_eq!(status, StatusCode::SEE_OTHER);

    // The run is recorded and finishes.
    for _ in 0..100 {
        if store.run_count().expect("count") > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let runs = store.recent_runs(1).expect("runs");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].action, "peek");
    assert!(runs[0].dry_run, "a read action is always a dry run");
}

#[tokio::test]
async fn a_form_cannot_turn_a_preview_action_into_an_apply() {
    let (state, store, _dir) = state(None);
    // The form claims it is an apply; the action's kind wins.
    let status = post_form(&state, "/apps/echo/actions/peek", "dry_run=false").await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    for _ in 0..100 {
        if store.run_count().expect("count") > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let runs = store.recent_runs(1).expect("runs");
    assert!(runs[0].dry_run);
}

#[tokio::test]
async fn the_run_page_shows_status_rows_and_the_csv_link() {
    let (state, store, _dir) = state(None);
    let run_id = store.start_run("echo", "peek", true).expect("run");
    store
        .record_ai_decision(
            run_id,
            &crosstalk_core::store::AiDecision {
                row_ordinal: 0,
                provider: "typesafe".to_string(),
                model: "jev".to_string(),
                ..Default::default()
            },
        )
        .expect("decision");
    store
        .append_rows(
            run_id,
            &[crosstalk_core::store::RunRow {
                ordinal: 0,
                channel_id: 1,
                channel_name: "ESPN".to_string(),
                status: "MATCHED".to_string(),
                reason: "Excellent match (96%)".to_string(),
                ai_decision: Some("verify: accept".to_string()),
                ..Default::default()
            }],
        )
        .expect("rows");
    store
        .finish_run(
            run_id,
            crosstalk_core::store::RunStatus::Succeeded,
            Some("one channel matched"),
            &serde_json::json!({"matched": 1}),
            Some("/tmp/does-not-exist.csv"),
        )
        .expect("finish");

    let (status, body) = get(&state, &format!("/runs/{run_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Run #"), "{body}");

    let (status, rows) = get(&state, &format!("/runs/{run_id}/rows")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(rows.contains("ESPN"), "{rows}");
    assert!(rows.contains("verify: accept"), "{rows}");

    let (status, panel) = get(&state, &format!("/runs/{run_id}/status")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(panel.contains("succeeded"), "{panel}");
    assert!(panel.contains("one channel matched"), "{panel}");
    assert!(panel.contains("Download CSV"), "{panel}");

    // The report is gone, so the download says so rather than serving an empty file.
    let (status, _body) = get(&state, &format!("/runs/{run_id}/csv")).await;
    assert_eq!(status, StatusCode::GONE);
}

#[tokio::test]
async fn the_json_api_answers_with_the_same_data() {
    let (state, store, _dir) = state(None);
    store.start_run("echo", "peek", true).expect("run");

    let (status, body) = get(&state, "/api/v1/apps").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"echo\""), "{body}");

    let (status, body) = get(&state, "/api/v1/runs").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"peek\""), "{body}");

    let (status, _body) = get(&state, "/api/v1/runs/9999").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_configured_token_is_required() {
    let (state, _store, _dir) = state(Some("s3cret"));

    // Without a token: an API caller gets 401.
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/api/v1/apps")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // A browser is sent to the login page instead.
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/")
                .header("accept", "text/html")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        response
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok()),
        Some("/login")
    );

    // The login page itself is public.
    let (status, body) = get(&state, "/login").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Access token"), "{body}");

    // With the right cookie, the page renders.
    let (status, _body) = get_with_cookie(&state, "/", "s3cret").await;
    assert_eq!(status, StatusCode::OK);

    // With the wrong one it does not.
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/api/v1/apps")
                .header("authorization", "Bearer wrong")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_health_check_needs_no_token() {
    let (state, _store, _dir) = state(Some("s3cret"));
    let (status, body) = get(&state, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("ok"));
}

#[tokio::test]
async fn the_settings_page_renders_both_host_and_app_forms() {
    let (state, _store, dir) = state(None);
    std::fs::write(dir.path().join("echo.toml"), "").expect("seed");
    let (status, body) = get(&state, "/settings").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Ignore Quality Tags"), "{body}");
    assert!(body.contains("Save host settings"), "{body}");
}

#[tokio::test]
async fn saving_host_settings_refuses_to_write_environment_secrets() {
    // The route loads the FILE-only config, so this is the guard being exercised: an effective
    // config would refuse to save at all.
    let (state, _store, dir) = state(None);
    std::fs::write(
        dir.path().join("crosstalk.toml"),
        "[dispatcharr]\napi_key = \"file-key\"\n",
    )
    .expect("seed");

    let body = "matching.ignore_quality_tags=true&matching.enabled_databases=US,UK&matching.custom_aliases=";
    let status = post_form(&state, "/settings/host", body).await;
    assert_eq!(status, StatusCode::SEE_OTHER);

    let text = std::fs::read_to_string(dir.path().join("crosstalk.toml")).expect("read");
    assert!(text.contains("file-key"), "{text}");
    assert!(
        text.contains("UK"),
        "the new database list is written: {text}"
    );
}

#[tokio::test]
async fn saving_bad_alias_json_is_a_400() {
    let (state, _store, dir) = state(None);
    std::fs::write(dir.path().join("crosstalk.toml"), "").expect("seed");
    let status = post_form(&state, "/settings/host", "matching.custom_aliases=not-json").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn cancelling_from_the_ui_is_accepted() {
    let (state, _store, _dir) = state(None);
    let status = post_form(&state, "/runs/9999/cancel", "").await;
    // Cancelling a run that is not running is harmless; the page it returns to is the run page.
    assert_eq!(status, StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn a_post_without_the_csrf_token_is_rejected() {
    let (state, _store, _dir) = state(None);
    // No header, no form field: the forged cross-site POST shape.
    assert_eq!(
        post_form_with_csrf(&state, "/apps/echo/actions/peek", "dry_run=true", false).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        post_form_with_csrf(&state, "/settings/host", "", false).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        post_form_with_csrf(&state, "/runs/9999/cancel", "", false).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        post_form_with_csrf(&state, "/logout", "", false).await,
        StatusCode::FORBIDDEN
    );
    // A wrong token is the same as none.
    let status = router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apps/echo/actions/peek")
                .header("content-type", "application/x-www-form-urlencoded")
                .header(crate::csrf::CSRF_HEADER, "wrong")
                .body(Body::from("dry_run=true".to_string()))
                .expect("request"),
        )
        .await
        .expect("response")
        .status();
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn the_form_field_carries_the_same_token_as_the_header() {
    // Browsers send the hidden field, not the header: prove that path works too.
    let (state, store, _dir) = state(None);
    let body = format!("dry_run=true&csrf_token={}", state.csrf_token());
    let status = post_form_with_csrf(&state, "/apps/echo/actions/peek", &body, false).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    for _ in 0..100 {
        if store.run_count().expect("count") > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(store.run_count().expect("count"), 1);
}

#[tokio::test]
async fn a_channel_changing_action_needs_a_server_side_confirmation() {
    let (state, store, _dir) = state(None);
    // `poke` is a Write action: CSRF alone is not enough without confirm=true.
    let status = post_form(&state, "/apps/echo/actions/poke", "dry_run=false").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(store.run_count().expect("count"), 0, "nothing started");

    // A read action never needs it.
    let status = post_form(&state, "/apps/echo/actions/peek", "dry_run=true").await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    // Let the preview run finish so its busy lock is released before the apply below.
    for _ in 0..200 {
        if !state.runner.is_busy("echo") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(!state.runner.is_busy("echo"));

    // With the confirmation the run starts.
    let status = post_form(&state, "/apps/echo/actions/poke", "confirm=true").await;
    assert_eq!(status, StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn every_post_form_embeds_the_csrf_token() {
    let (state, _store, _dir) = state(None);
    let (status, body) = get(&state, "/apps/echo").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#"name="csrf_token""#), "{body}");
    assert!(body.contains(r#"name="confirm""#), "{body}");

    let (status, body) = get(&state, "/settings").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#"name="csrf_token""#), "{body}");

    let (status, body) = get(&state, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#"name="csrf_token""#), "logout form: {body}");
}

#[tokio::test]
async fn get_responses_do_not_set_a_csrf_cookie() {
    // The token lives server-side; the hidden field and the header are the channels, so
    // there is no cookie to persist. Validation never read one back.
    let (state, _store, _dir) = state(None);
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let cookies: Vec<String> = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|value| value.to_str().ok().map(str::to_string))
        .collect();
    assert!(
        cookies
            .iter()
            .all(|cookie| !cookie.starts_with("crosstalk_csrf=")),
        "{cookies:?}"
    );
}

#[tokio::test]
async fn a_rebinding_host_is_rejected_without_a_token() {
    // No token configured: only loopback Host values may reach the daemon. A
    // DNS-rebinding page's hostname resolves to 127.0.0.1 but arrives as a foreign
    // Host, which is exactly what this rejects.
    let (state, _store, _dir) = state(None);
    let (status, _) = post_raw(
        &state,
        "/apps/echo/actions/peek",
        "dry_run=true",
        &[("host", "evil.example")],
        true,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // A rebinding read is rejected too, not just POSTs: the page wants the csrf_token.
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/")
                .header("host", "evil.example")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn loopback_hosts_reach_a_tokenless_daemon() {
    let (state, _store, _dir) = state(None);
    for host in [
        "localhost",
        "localhost:8730",
        "127.0.0.1",
        "127.0.0.2:8730",
        "[::1]",
    ] {
        let (status, _) = post_raw(
            &state,
            "/apps/echo/actions/peek",
            "dry_run=true",
            &[("host", host)],
            true,
        )
        .await;
        assert_eq!(status, StatusCode::SEE_OTHER, "host {host}");
        // Let each preview run finish so its busy lock is released before the next POST.
        for _ in 0..200 {
            if !state.runner.is_busy("echo") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}

#[tokio::test]
async fn a_configured_token_skips_the_host_check_but_not_auth() {
    // Behind a reverse proxy the Host is foreign by design; the bearer token is the
    // protection there, so the Host check steps aside and auth decides.
    let (state, _store, _dir) = state(Some("s3cret"));
    let (status, _) = post_raw(
        &state,
        "/apps/echo/actions/peek",
        "dry_run=true",
        &[
            ("host", "proxy.internal"),
            ("authorization", "Bearer s3cret"),
        ],
        false,
    )
    .await;
    // Bearer is CSRF-exempt, so no csrf token is needed; auth passes on the token.
    assert_eq!(status, StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn an_origin_that_disagrees_with_host_is_a_forgery() {
    let (state, _store, _dir) = state(None);
    let (status, _) = post_raw(
        &state,
        "/apps/echo/actions/peek",
        "dry_run=true",
        &[("host", "localhost"), ("origin", "http://evil.example")],
        true,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // A matching Origin is the browser's own page and passes.
    let (status, _) = post_raw(
        &state,
        "/apps/echo/actions/peek",
        "dry_run=true",
        &[("host", "localhost"), ("origin", "http://localhost")],
        true,
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn a_script_bearer_is_csrf_exempt_and_a_wrong_one_is_not() {
    let (state, _store, _dir) = state(Some("s3cret"));
    // Valid Bearer, no CSRF token at all: scripts do not have the synchroniser token.
    let (status, _) = post_raw(
        &state,
        "/apps/echo/actions/peek",
        "dry_run=true",
        &[("authorization", "Bearer s3cret")],
        false,
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    for _ in 0..200 {
        if !state.runner.is_busy("echo") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // A wrong Bearer buys nothing: without a CSRF token the POST still fails closed.
    let (status, _) = post_raw(
        &state,
        "/apps/echo/actions/peek",
        "dry_run=true",
        &[("authorization", "Bearer wrong")],
        false,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn the_login_endpoint_is_exempt_from_csrf() {
    // Login carries the auth token itself; with no token configured it redirects home.
    let (state, _store, _dir) = state(None);
    let status = post_form_with_csrf(&state, "/login", "token=anything", false).await;
    assert_ne!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn bad_logins_without_connect_info_share_the_fallback_bucket() {
    // `oneshot` requests carry no `ConnectInfo<SocketAddr>`, exactly like the daemon
    // served without `into_make_service_with_connect_info`: every attempt falls back to
    // the one unspecified-address bucket, so ten bad tokens lock the eleventh out.
    // That the lockout fires proves the fallback still counts instead of letting
    // connect-info-less requests through unlimited.
    let (state, _store, _dir) = state(Some("secret"));
    for attempt in 0..crate::auth::MAX_LOGIN_FAILURES {
        let status = post_form_with_csrf(&state, "/login", "token=wrong", false).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "bad attempt {attempt} re-renders login"
        );
    }
    let status = post_form_with_csrf(&state, "/login", "token=wrong", false).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn saving_host_settings_applies_without_a_restart() {
    let (state, _store, dir) = state(None);
    std::fs::write(dir.path().join("crosstalk.toml"), "").expect("seed");
    assert!(state.snapshot().matching.ignore_quality_tags);

    // Untick the quality-tags box (an absent checkbox means false) and save.
    let status = post_form(&state, "/settings/host", "matching.enabled_databases=US").await;
    assert_eq!(status, StatusCode::SEE_OTHER);

    // The file holds the new value and the live config matches it: no restart needed.
    let text = std::fs::read_to_string(dir.path().join("crosstalk.toml")).expect("read");
    assert!(text.contains("ignore_quality_tags = false"), "{text}");
    assert!(!state.snapshot().matching.ignore_quality_tags);
    assert_eq!(
        state.snapshot().matching.databases(),
        vec!["US".to_string()]
    );

    // The settings page renders the saved value from the live config: the quality-tags
    // checkbox is now unticked (no `checked` on its input).
    let (status, body) = get(&state, "/settings").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Ignore Quality Tags"), "{body}");
    let input = body
        .lines()
        .find(|line| line.contains("name=\"matching.ignore_quality_tags\""))
        .unwrap_or("")
        .to_string();
    assert!(!input.is_empty(), "the host form renders the field: {body}");
    assert!(
        !input.contains("checked"),
        "the box reflects the save: {input}"
    );
}

/// POST a form with extra headers and return the status and body, for asserting on pages
/// rendered with an error rather than a redirect.
async fn post_page(
    state: &AppState,
    uri: &str,
    body: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, String) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/x-www-form-urlencoded")
        .header(crate::csrf::CSRF_HEADER, state.csrf_token());
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = router(state.clone())
        .oneshot(builder.body(Body::from(body.to_string())).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let text = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("body");
    (status, String::from_utf8_lossy(&text).to_string())
}

#[tokio::test]
async fn saving_host_settings_refuses_a_swap_that_would_drop_auth() {
    // The live config binds a non-loopback address with a token; the file on disk binds the
    // same address with no token (the secret lived in the environment and is gone). Saving
    // must still write the file, but must not swap the unauthenticated config in.
    let (state, _store, dir) = state(Some("s3cret"));
    std::fs::write(
        dir.path().join("crosstalk.toml"),
        "[server]\nbind = \"0.0.0.0\"\n",
    )
    .expect("seed");

    let (status, body) = post_page(
        &state,
        "/settings/host",
        "matching.enabled_databases=US",
        &[("cookie", "crosstalk_token=s3cret")],
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.contains("NOT applied"), "{body}");
    assert!(body.contains("unauthenticated"), "{body}");
    assert!(body.contains("refuse to start"), "{body}");

    // The file save itself happened: the submitted databases are on disk.
    let text = std::fs::read_to_string(dir.path().join("crosstalk.toml")).expect("read");
    assert!(text.contains("US"), "{text}");

    // But the live config is untouched: the old snapshot, token included, is retained.
    assert_eq!(
        state.snapshot().server.resolved_token().as_deref(),
        Some("s3cret")
    );
    assert!(
        state.snapshot().matching.ignore_quality_tags,
        "the unticked box must not have been applied"
    );
}

/// GET a path and return the status, headers and body, for asserting on headers.
async fn get_full(state: &AppState, uri: &str) -> (StatusCode, axum::http::HeaderMap, String) {
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("body");
    (status, headers, String::from_utf8_lossy(&body).to_string())
}

fn security_headers(headers: &axum::http::HeaderMap) {
    let csp = headers
        .get("content-security-policy")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(
        csp.contains("default-src 'self'"),
        "CSP carries a strict default: {csp}"
    );
    assert!(
        !csp.contains("unsafe-inline"),
        "no inline scripts survive: {csp}"
    );
    assert!(
        csp.contains("frame-ancestors 'none'"),
        "clickjacking is denied in the policy: {csp}"
    );
    assert_eq!(
        headers
            .get("x-frame-options")
            .and_then(|value| value.to_str().ok()),
        Some("DENY")
    );
    assert_eq!(
        headers
            .get("x-content-type-options")
            .and_then(|value| value.to_str().ok()),
        Some("nosniff")
    );
    assert_eq!(
        headers
            .get("referrer-policy")
            .and_then(|value| value.to_str().ok()),
        Some("same-origin")
    );
}

#[tokio::test]
async fn every_response_carries_the_browser_security_headers() {
    let (state, _store, _dir) = state(None);

    // A normal page.
    let (status, headers, _body) = get_full(&state, "/").await;
    assert_eq!(status, StatusCode::OK);
    security_headers(&headers);

    // The standalone login page, which does not extend the base template.
    let (status, headers, _body) = get_full(&state, "/login").await;
    assert_eq!(status, StatusCode::OK);
    security_headers(&headers);

    // A rejection from an inner layer: the headers layer sits outside CSRF, so even a
    // forged POST without the token is answered with the headers attached.
    let (status, headers) = post_raw(
        &state,
        "/apps/echo/actions/peek",
        "dry_run=true",
        &[],
        false,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    security_headers(&headers);
}

#[tokio::test]
async fn the_confirm_dialog_is_a_self_hosted_script_not_an_inline_handler() {
    let (state, _store, _dir) = state(None);

    // The script is served from the daemon itself, so the CSP's script-src 'self' allows it.
    let (status, headers, body) = get_full(&state, "/static/confirm.js").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/javascript; charset=utf-8")
    );
    assert!(body.contains("data-confirm"), "{body}");
    security_headers(&headers);

    // Every page loads it.
    let (_status, _headers, body) = get_full(&state, "/").await;
    assert!(
        body.contains(r#"<script src="/static/confirm.js""#),
        "the base template loads the confirm script: {body}"
    );
}

#[tokio::test]
async fn ten_wrong_tokens_lock_the_address_out_of_login() {
    let (state, _store, _dir) = state(Some("s3cret"));

    for attempt in 1..=10 {
        let status = post_form_with_csrf(&state, "/login", "token=wrong", false).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "attempt {attempt} shows the error page"
        );
    }

    // The eleventh attempt in five minutes is a 429, with a Retry-After.
    let (status, headers) = post_raw(&state, "/login", "token=wrong", &[], false).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(
        headers.contains_key("retry-after"),
        "the lockout names its wait: {headers:?}"
    );

    // Even the right token waits out the lockout.
    let (status, _) = post_raw(&state, "/login", "token=s3cret", &[], false).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn a_correct_sign_in_clears_the_failure_count() {
    let (state, _store, _dir) = state(Some("s3cret"));

    // Nine failures, then the right token: the sign-in succeeds and the count resets.
    for _ in 0..9 {
        let status = post_form_with_csrf(&state, "/login", "token=wrong", false).await;
        assert_eq!(status, StatusCode::OK);
    }
    let status = post_form_with_csrf(&state, "/login", "token=s3cret", false).await;
    assert_eq!(status, StatusCode::SEE_OTHER);

    // Nine more failures are tolerated again: without the reset the first of these
    // would already be the eleventh and locked out.
    for attempt in 1..=10 {
        let status = post_form_with_csrf(&state, "/login", "token=wrong", false).await;
        assert_eq!(status, StatusCode::OK, "attempt {attempt} after the reset");
    }
    let (status, _) = post_raw(&state, "/login", "token=wrong", &[], false).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn a_csv_outside_the_exports_directory_is_not_served() {
    let (state, store, dir) = state(None);
    let exports = dir.path().join("exports");
    std::fs::create_dir_all(&exports).expect("exports");

    // A report inside the exports directory downloads as before.
    let inside = exports.join("epg_janitor_test.csv");
    std::fs::write(&inside, "channel_name\nESPN\n").expect("write");
    let run_id = store.start_run("echo", "peek", true).expect("run");
    store
        .finish_run(
            run_id,
            crosstalk_core::store::RunStatus::Succeeded,
            None,
            &serde_json::json!({}),
            Some(inside.to_str().expect("path")),
        )
        .expect("finish");
    let (status, headers, body) = get_full(&state, &format!("/runs/{run_id}/csv")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("ESPN"), "{body}");
    assert!(headers.contains_key("content-disposition"), "{headers:?}");

    // A report path pointing elsewhere — even at a file that exists — is a 404, not a
    // file read. The row is hand-planted, standing in for any future feature that
    // lets a path into the database from somewhere less trusted.
    let outside = dir.path().join("secret.csv");
    std::fs::write(&outside, "classified\n").expect("write");
    let run_id = store.start_run("echo", "peek", true).expect("run");
    store
        .finish_run(
            run_id,
            crosstalk_core::store::RunStatus::Succeeded,
            None,
            &serde_json::json!({}),
            Some(outside.to_str().expect("path")),
        )
        .expect("finish");
    let (status, _headers, body) = get_full(&state, &format!("/runs/{run_id}/csv")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(!body.contains("classified"), "nothing leaks: {body}");

    // A `..` escape through a symlinked-looking path resolves the same way.
    let sneaky = exports.join("..").join("secret.csv");
    let run_id = store.start_run("echo", "peek", true).expect("run");
    store
        .finish_run(
            run_id,
            crosstalk_core::store::RunStatus::Succeeded,
            None,
            &serde_json::json!({}),
            Some(sneaky.to_str().expect("path")),
        )
        .expect("finish");
    let (status, _headers, body) = get_full(&state, &format!("/runs/{run_id}/csv")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(!body.contains("classified"), "nothing leaks: {body}");
}
