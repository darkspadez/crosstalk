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
    let runner = JobRunner::new(
        Registry::new(vec![Arc::new(EchoApp)]),
        Arc::new(config.clone()),
        store.clone(),
    );
    let state = AppState::new(registry, Arc::new(config), runner, store.clone());
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
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
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
    // A writing action confirms before running.
    assert!(body.contains("confirm("), "{body}");
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
