use super::*;
use crate::config::DispatcharrConfig;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// The cross-language vectors `bridge/tests/test_plugin.py` checks too.
const VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../bridge/tests/vectors.json"
));

const SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const RUN_PATH: &str = "/api/plugins/plugins/crosstalk_bridge/run/";

fn vectors() -> Value {
    serde_json::from_str(VECTORS).expect("vectors.json parses")
}

fn text<'a>(case: &'a Value, key: &str) -> &'a str {
    case[key].as_str().expect("a string field")
}

fn config(base_url: &str, secret: &str) -> CrosstalkConfig {
    CrosstalkConfig {
        dispatcharr: DispatcharrConfig {
            base_url: base_url.to_string(),
            api_key: "test-key".to_string(),
            timeout_secs: 5,
            concurrency: 4,
            ..DispatcharrConfig::default()
        },
        bridge: BridgeConfig {
            enabled: true,
            secret: secret.to_string(),
            secret_env: String::new(),
            ..BridgeConfig::default()
        },
        ..CrosstalkConfig::default()
    }
}

fn client(config: &CrosstalkConfig) -> DispatcharrClient {
    DispatcharrClient::new(&config.dispatcharr).expect("client")
}

fn bridge(config: &CrosstalkConfig) -> BridgeClient {
    BridgeClient::new(client(config), &config.bridge).expect("bridge")
}

fn answer(result: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "success": true, "result": result }))
}

fn refused(op: &Value, reason: &str) -> ResponseTemplate {
    answer(json!({
        "ok": false, "op": op, "applied": 0, "skipped": [],
        "errors": [{"id": null, "reason": reason}], "version": "0.1.0",
    }))
}

/// A fake plugin: checks the request the way `bridge/plugin.py` does (MAC under `secret`,
/// action = signed op, timestamp, nonce shape) and answers every id. Channel or stream 404
/// is reported as not found.
fn fake_plugin(
    secret: &'static str,
    version: &'static str,
) -> impl Fn(&Request) -> ResponseTemplate {
    move |request: &Request| {
        let body: Value = serde_json::from_slice(&request.body).expect("a JSON body");
        let envelope = &body["params"]["envelope"];
        let mac = body["params"]["mac"].as_str().unwrap_or_default();
        let op = envelope["op"].clone();
        if sign(secret, envelope).ok().as_deref() != Some(mac) {
            return refused(
                &op,
                "bad_mac: the signature does not match the configured secret",
            );
        }
        if body["action"] != op {
            return refused(&op, "bad_request: the signed op does not match the action");
        }
        let age = chrono::Utc::now().timestamp() - envelope["ts"].as_i64().unwrap_or(0);
        let nonce = envelope["nonce"].as_str().unwrap_or_default();
        if age.abs() > REPLAY_WINDOW_SECS || nonce.len() != 32 || envelope["v"] != json!(1) {
            return refused(&op, "stale");
        }
        let payload = &envelope["payload"];
        let ids: Vec<i64> = match op.as_str().unwrap_or_default() {
            "ping" => {
                return answer(json!({
                    "ok": true, "op": "ping", "applied": 0, "skipped": [], "errors": [],
                    "version": version, "protocol": 1, "capabilities": OPS,
                    "key": "current", "configured": "env", "grace": "none",
                }))
            }
            "clear_channel_groups" => payload["channel_ids"]
                .as_array()
                .map(|ids| ids.iter().filter_map(Value::as_i64).collect())
                .unwrap_or_default(),
            "set_stream_lists" => payload["lists"]
                .as_array()
                .map(|lists| {
                    lists
                        .iter()
                        .filter_map(|list| list["channel_id"].as_i64())
                        .collect()
                })
                .unwrap_or_default(),
            "set_stream_stats" => payload["stats"]
                .as_array()
                .map(|rows| {
                    rows.iter()
                        .filter_map(|row| row["stream_id"].as_i64())
                        .collect()
                })
                .unwrap_or_default(),
            _ => return refused(&op, "unknown_op"),
        };
        let errors: Vec<Value> = ids
            .iter()
            .filter(|id| **id == 404)
            .map(|id| json!({"id": id, "reason": "not_found: no such row"}))
            .collect();
        answer(json!({
            "ok": true, "op": op, "applied": ids.len() - errors.len(), "skipped": [],
            "errors": errors, "version": version,
        }))
    }
}

const REPLAY_WINDOW_SECS: i64 = 300;

// ── the shared vectors ──────────────────────────────────────────────────────────────────────

#[test]
fn canonical_json_matches_the_shared_vectors() {
    let vectors = vectors();
    let cases = vectors["canonical"].as_array().expect("canonical cases");
    assert!(cases.len() > 10);
    for case in cases {
        let value: Value = serde_json::from_str(text(case, "json")).expect("vector json parses");
        assert_eq!(
            canonical_json(&value).expect("canonical"),
            text(case, "canonical"),
            "{}",
            text(case, "name")
        );
    }
}

#[test]
fn values_without_a_canonical_form_are_refused_like_the_plugin_refuses_them() {
    for case in vectors()["rejects"].as_array().expect("rejects") {
        // A lone surrogate does not even parse here; Python parses it and then refuses it.
        let refused = match serde_json::from_str::<Value>(text(case, "json")) {
            Ok(value) => canonical_json(&value).is_err(),
            Err(_) => true,
        };
        assert!(refused, "{}", text(case, "name"));
    }
}

#[test]
fn signatures_match_the_shared_vectors() {
    for case in vectors()["signatures"].as_array().expect("signatures") {
        let envelope: Value = serde_json::from_str(text(case, "envelope")).expect("envelope");
        assert_eq!(
            canonical_json(&envelope).expect("canonical"),
            text(case, "canonical"),
            "{}",
            text(case, "name")
        );
        assert_eq!(
            sign(text(case, "secret"), &envelope).expect("signed"),
            text(case, "mac"),
            "{}",
            text(case, "name")
        );

        // And the client builds exactly that envelope from the same op, payload, time, nonce.
        let config = config("http://localhost:9", text(case, "secret"));
        let params = bridge(&config)
            .signed_params_at(
                text(&envelope, "op"),
                envelope["payload"].clone(),
                envelope["ts"].as_i64().expect("ts"),
                text(&envelope, "nonce"),
            )
            .expect("params");
        assert_eq!(params["envelope"], envelope, "{}", text(case, "name"));
        assert_eq!(params["mac"], case["mac"], "{}", text(case, "name"));
    }
}

#[test]
fn floats_follow_ecmascript_layout() {
    for (float, expected) in [
        (1.0, "1"),
        (-1.5, "-1.5"),
        (1e21, "1e+21"),
        (1e20, "100000000000000000000"),
        (1e-6, "0.000001"),
        (1e-7, "1e-7"),
        (123.456, "123.456"),
        (-0.0, "0"),
        (5e-324, "5e-324"),
    ] {
        assert_eq!(float_text(float).expect("finite"), expected, "{float}");
    }
    assert!(float_text(f64::NAN).is_err());
    assert!(float_text(f64::INFINITY).is_err());
}

#[test]
fn a_fresh_request_has_a_new_nonce_and_the_current_time() {
    let config = config("http://localhost:9", SECRET);
    let bridge = bridge(&config);
    let one = bridge.signed_params("ping", json!({})).expect("params");
    let two = bridge.signed_params("ping", json!({})).expect("params");
    let nonce = text(&one["envelope"], "nonce");
    assert_eq!(nonce.len(), 32);
    assert!(nonce
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    assert_ne!(one["envelope"]["nonce"], two["envelope"]["nonce"]);
    let age = chrono::Utc::now().timestamp() - one["envelope"]["ts"].as_i64().expect("ts");
    assert!(age.abs() <= 5);
    assert_eq!(one["envelope"]["v"], json!(1));
}

#[test]
fn a_generated_secret_is_64_hex_characters_and_never_repeats() {
    let one = generate_secret().expect("secret");
    assert_eq!(one.len(), 64);
    assert!(one.chars().all(|c| c.is_ascii_hexdigit()));
    assert_ne!(one, generate_secret().expect("secret"));
}

#[test]
fn the_client_never_prints_its_secret() {
    let config = config("http://localhost:9", SECRET);
    let printed = format!("{:?} {:?}", bridge(&config), config.bridge);
    assert!(!printed.contains(SECRET), "{printed}");
    assert!(printed.contains("<redacted>"));
}

// ── small pieces ────────────────────────────────────────────────────────────────────────────

#[test]
fn versions_and_keys_parse() {
    assert_eq!(parse_version("0.1.0"), Some((0, 1, 0)));
    assert_eq!(parse_version("v1.2.3-rc.1"), Some((1, 2, 3)));
    assert_eq!(parse_version("2"), Some((2, 0, 0)));
    assert_eq!(parse_version("1.x"), None);
    assert_eq!(parse_version("1.2.3.4"), None);
    assert!(parse_version("0.0.9") < parse_version("0.1.0"));
    assert!(parse_version("0.10.0") > parse_version("0.9.9"));

    assert_eq!(
        alternate_plugin_key("crosstalk_bridge").as_deref(),
        Some("crosstalk-bridge")
    );
    assert_eq!(
        alternate_plugin_key("crosstalk-bridge").as_deref(),
        Some("crosstalk_bridge")
    );
    assert_eq!(alternate_plugin_key("bridge"), None);
    assert!(valid_plugin_key("crosstalk_bridge"));
    assert!(!valid_plugin_key("../secrets"));
}

#[test]
fn chunks_respect_every_cap() {
    let items: Vec<Value> = (0..4_500).map(Value::from).collect();
    let weights = vec![0; items.len()];
    let ranges = chunk_ranges(&items, &weights, MAX_CHANNELS, usize::MAX);
    assert_eq!(ranges, vec![0..2_000, 2_000..4_000, 4_000..4_500]);

    // Weight: 30 lists of 1,000 streams at 20,000 per request is 20 + 10.
    let lists: Vec<Value> = (0..30).map(Value::from).collect();
    let weights = vec![1_000; 30];
    let ranges = chunk_ranges(&lists, &weights, MAX_CHANNELS, MAX_STREAM_REFS);
    assert_eq!(ranges, vec![0..20, 20..30]);

    // Bytes: rows of ~100 KB go ten or fewer to a request.
    let big: Vec<Value> = (0..25).map(|_| Value::from("x".repeat(100_000))).collect();
    let weights = vec![0; big.len()];
    for range in chunk_ranges(&big, &weights, MAX_STREAMS, usize::MAX) {
        assert!(range.len() <= 10 && !range.is_empty());
    }
    assert!(chunk_ranges(&[], &[], MAX_CHANNELS, usize::MAX).is_empty());
}

#[test]
fn row_results_attribute_errors_by_id_and_distrust_a_short_answer() {
    let ids: Vec<Value> = [1, 2, 3].into_iter().map(Value::from).collect();
    let response = BridgeResponse {
        ok: true,
        applied: 1,
        skipped: vec![json!(3)],
        errors: vec![BridgeRowError {
            id: json!(2),
            reason: "not_found: no such channel".to_string(),
        }],
        ..BridgeResponse::default()
    };
    let results = row_results(&ids, Ok(response.clone()));
    assert!(results[0].is_ok());
    let error = format!("{:#}", results[1].as_ref().expect_err("failed"));
    assert!(error.contains("not_found"), "{error}");
    assert!(results[2].is_ok());

    let short = BridgeResponse {
        applied: 0,
        ..response
    };
    for result in row_results(&ids, Ok(short)) {
        assert!(format!("{:#}", result.expect_err("unknown")).contains("unknown"));
    }

    for result in row_results(&ids, Err(anyhow!("connection refused"))) {
        assert!(format!("{:#}", result.expect_err("failed")).contains("connection refused"));
    }
}

#[test]
fn a_ping_answer_is_judged_on_protocol_version_and_capabilities() {
    let good = BridgeResponse {
        ok: true,
        version: "0.1.0".to_string(),
        protocol: Some(1),
        capabilities: OPS.iter().map(|op| op.to_string()).collect(),
        ..BridgeResponse::default()
    };
    assert!(check_ping(&good, "0.1.0").is_ok());
    let old = BridgeResponse {
        version: "0.0.9".to_string(),
        ..good.clone()
    };
    assert!(format!("{:#}", check_ping(&old, "0.1.0").expect_err("old")).contains("older"));
    let other = BridgeResponse {
        protocol: Some(2),
        ..good.clone()
    };
    assert!(check_ping(&other, "0.1.0").is_err());
    let partial = BridgeResponse {
        capabilities: vec!["ping".to_string()],
        ..good
    };
    let error = format!("{:#}", check_ping(&partial, "0.1.0").expect_err("partial"));
    assert!(error.contains("set_stream_lists"), "{error}");
}

// ── against a fake Dispatcharr ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_bridge_is_off_by_default_and_a_disabled_one_sends_nothing() {
    assert!(!BridgeConfig::default().enabled);
    assert!(!CrosstalkConfig::default().bridge.enabled);
    let server = MockServer::start().await;
    let mut config = config(&server.uri(), SECRET);
    config.bridge.enabled = false;
    assert_eq!(
        check(&config, &client(&config)).await,
        BridgeStatus::Disabled
    );
    assert!(for_config(&config, &client(&config)).is_none());
    assert!(server
        .received_requests()
        .await
        .unwrap_or_default()
        .is_empty());
}

#[tokio::test]
async fn ping_checks_the_mac_and_the_version() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(RUN_PATH))
        .respond_with(fake_plugin(SECRET, "0.1.0"))
        .mount(&server)
        .await;
    let config = config(&server.uri(), SECRET);
    match check(&config, &client(&config)).await {
        BridgeStatus::Ready {
            version,
            plugin_key,
            note,
        } => {
            assert_eq!(version, "0.1.0");
            assert_eq!(plugin_key, "crosstalk_bridge");
            assert_eq!(note.as_deref(), Some("plugin secret configured: env"));
        }
        other => panic!("not ready: {other:?}"),
    }

    // A secret the plugin does not have is refused, and the reason says so.
    let wrong = config_with_secret(&server.uri(), &"f".repeat(64));
    match check(&wrong, &client(&wrong)).await {
        BridgeStatus::Unavailable { reason } => assert!(reason.contains("bad_mac"), "{reason}"),
        other => panic!("a wrong secret was accepted: {other:?}"),
    }
}

fn config_with_secret(base_url: &str, secret: &str) -> CrosstalkConfig {
    config(base_url, secret)
}

#[tokio::test]
async fn an_old_plugin_is_not_used() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(RUN_PATH))
        .respond_with(fake_plugin(SECRET, "0.0.9"))
        .mount(&server)
        .await;
    let config = config(&server.uri(), SECRET);
    match check(&config, &client(&config)).await {
        BridgeStatus::Unavailable { reason } => assert!(reason.contains("older"), "{reason}"),
        other => panic!("an old plugin was accepted: {other:?}"),
    }
}

#[tokio::test]
async fn a_missing_plugin_is_tried_under_its_other_spelling() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(RUN_PATH))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "success": false, "error": "Plugin not found"
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/plugins/plugins/crosstalk-bridge/run/"))
        .respond_with(fake_plugin(SECRET, "0.1.0"))
        .mount(&server)
        .await;
    let config = config(&server.uri(), SECRET);
    match check(&config, &client(&config)).await {
        BridgeStatus::Ready {
            plugin_key, note, ..
        } => {
            assert_eq!(plugin_key, "crosstalk-bridge");
            let note = note.expect("a note");
            assert!(
                note.contains("bridge.plugin_key = \"crosstalk-bridge\""),
                "{note}"
            );
        }
        other => panic!("not ready: {other:?}"),
    }

    // Neither spelling installed: unavailable, with Dispatcharr's answer.
    let empty = MockServer::start().await;
    let config = config_with_secret(&empty.uri(), SECRET);
    assert!(matches!(
        check(&config, &client(&config)).await,
        BridgeStatus::Unavailable { .. }
    ));
}

#[tokio::test]
async fn a_disabled_plugin_reports_dispatcharrs_reason() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(RUN_PATH))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "success": false, "error": "Plugin is disabled"
        })))
        .mount(&server)
        .await;
    let config = config(&server.uri(), SECRET);
    let results = bridge(&config).clear_channel_groups(&[1, 2]).await;
    assert_eq!(results.len(), 2);
    for (_, result) in results {
        let error = format!("{:#}", result.expect_err("refused"));
        assert!(error.contains("Plugin is disabled"), "{error}");
    }
}

#[tokio::test]
async fn writes_are_chunked_and_answered_per_row() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(RUN_PATH))
        .respond_with(fake_plugin(SECRET, "0.1.0"))
        .mount(&server)
        .await;
    let config = config(&server.uri(), SECRET);
    let bridge = bridge(&config);

    let ids: Vec<i64> = (10_001..=14_500).collect();
    let results = bridge.clear_channel_groups(&ids).await;
    assert_eq!(results.len(), 4_500);
    assert!(results.iter().all(|(_, result)| result.is_ok()));
    assert_eq!(
        results.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        ids,
        "answers come back in input order"
    );
    let sizes: Vec<usize> = server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .map(|request| {
            let body: Value = serde_json::from_slice(&request.body).expect("json");
            body["params"]["envelope"]["payload"]["channel_ids"]
                .as_array()
                .map_or(0, Vec::len)
        })
        .collect();
    assert_eq!(sizes, vec![2_000, 2_000, 500]);

    // One unknown channel fails alone; a list over the per-channel cap never leaves.
    let lists = vec![
        (7, vec![30, 20, 10]),
        (404, vec![1]),
        (8, (0..1_001).collect()),
        (9, vec![]),
    ];
    let results = bridge.set_stream_lists(&lists).await;
    let outcome: Vec<(i64, bool)> = results
        .iter()
        .map(|(id, result)| (*id, result.is_ok()))
        .collect();
    assert_eq!(
        outcome,
        vec![(7, true), (404, false), (8, false), (9, true)]
    );

    let stats = vec![
        (30, json!({"width": 1920, "source_fps": 29.97})),
        (31, Value::Null),
        (32, json!([1, 2])),
    ];
    let results = bridge.set_stream_stats(&stats).await;
    let outcome: Vec<(i64, bool)> = results
        .iter()
        .map(|(id, result)| (*id, result.is_ok()))
        .collect();
    assert_eq!(outcome, vec![(30, true), (31, true), (32, false)]);
}

#[tokio::test]
async fn only_a_verified_bridge_is_handed_out() {
    // The one test that touches the process-wide verdict: its secret is its own, so no other
    // test's configuration can match it.
    const OWN_SECRET: &str = "a1b2c3d4e5f6a7b8c9d0a1b2c3d4e5f6a7b8c9d0a1b2c3d4e5f6a7b8c9d0a1b2";
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(RUN_PATH))
        .respond_with(fake_plugin(OWN_SECRET, "0.1.0"))
        .mount(&server)
        .await;
    let config = config(&server.uri(), OWN_SECRET);
    let client = client(&config);
    assert!(
        for_config(&config, &client).is_none(),
        "nothing verified yet"
    );
    assert!(matches!(
        check_and_activate(&config, &client).await,
        BridgeStatus::Ready { .. }
    ));
    let handed = for_config(&config, &client).expect("verified");
    assert_eq!(handed.plugin_key(), "crosstalk_bridge");

    // A secret changed after the check is not trusted until the next check.
    let rotated = config_with_secret(&server.uri(), &"b".repeat(64));
    assert!(for_config(&rotated, &client).is_none());
    // Nor is a disabled bridge, whatever was verified.
    let mut disabled = config.clone();
    disabled.bridge.enabled = false;
    assert!(for_config(&disabled, &client).is_none());
}
