use super::*;
use wiremock::matchers::{body_json, body_partial_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(base_url: &str) -> DispatcharrConfig {
    DispatcharrConfig {
        base_url: base_url.to_string(),
        api_key: "test-key".to_string(),
        timeout_secs: 5,
        concurrency: 4,
        ..DispatcharrConfig::default()
    }
}

fn password_config(base_url: &str) -> DispatcharrConfig {
    DispatcharrConfig {
        base_url: base_url.to_string(),
        api_key: String::new(),
        api_key_env: String::new(),
        username: "admin".to_string(),
        password: "hunter2".to_string(),
        password_env: String::new(),
        timeout_secs: 5,
        concurrency: 4,
    }
}

#[test]
fn a_channel_carries_attached_channel_id_only_when_the_build_sends_it() {
    // Dispatcharr's own `Channel` has no `attached_channel` field and its serializer never
    // sends the key, so a stock listing parses to `None` — the visibility action then treats
    // every channel as unattached, which is what Stream-Mapparr's `_meta.get_field` guard
    // amounts to there. A build that does carry the column gets the plugin's rule instead.
    let stock: Channel = serde_json::from_value(serde_json::json!({
        "id": 7, "name": "TNT", "streams": [1, 2]
    }))
    .expect("a stock channel");
    assert_eq!(stock.attached_channel_id, None);

    let patched: Channel = serde_json::from_value(serde_json::json!({
        "id": 8, "name": "TNT", "attached_channel_id": 7
    }))
    .expect("a channel with the column");
    assert_eq!(patched.attached_channel_id, Some(7));
}

#[tokio::test]
async fn channels_ask_for_hidden_ones_too() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/channels/"))
        .and(query_param("visibility_filter", "all"))
        .and(header("Authorization", "ApiKey test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "name": "ESPN HD", "channel_number": 101, "channel_group_id": 3, "epg_data_id": 9, "streams": []},
            {"id": 2, "name": "CNN", "channel_number": "102", "epg_data_id": null, "streams": [4]}
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let channels = client.channels().await.expect("channels");
    assert_eq!(channels.len(), 2);
    assert_eq!(channels[0].name, "ESPN HD");
    assert_eq!(channels[0].display_number(), Some(101.0));
    assert_eq!(channels[1].display_number(), Some(102.0));
    assert_eq!(channels[1].current_epg_id(), None);
    assert_eq!(client.channel(1).expect("cached").name, "ESPN HD");
}

#[test]
fn the_raw_epg_column_is_what_a_run_reasons_about() {
    let channel = Channel {
        id: 1,
        epg_data_id: Some(5),
        effective_epg_data_id: Some(7),
        ..Channel::default()
    };
    assert_eq!(channel.current_epg_id(), Some(5));
    assert!(channel.has_epg_override());

    let plain = Channel {
        id: 2,
        epg_data_id: Some(5),
        effective_epg_data_id: Some(5),
        ..Channel::default()
    };
    assert!(!plain.has_epg_override());

    let override_only = Channel {
        id: 3,
        epg_data_id: None,
        effective_epg_data_id: Some(7),
        ..Channel::default()
    };
    assert!(override_only.has_epg_override());
}

#[tokio::test]
async fn bad_credentials_say_which_setting_to_check() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(401).set_body_string("{\"detail\":\"no\"}"))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let error = client.channels().await.expect_err("401");
    let message = format!("{error:#}");
    assert!(message.contains("dispatcharr.api_key"), "{message}");
    assert!(message.contains("administrator"), "{message}");
}

#[tokio::test]
async fn logging_in_trades_the_password_for_a_token() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/accounts/token/"))
        .and(body_json(serde_json::json!({
            "username": "admin",
            "password": "hunter2"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access": "access-token",
            "refresh": "refresh-token"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/channels/groups/"))
        .and(header("Authorization", "Bearer access-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!([{"id": 1, "name": "Sports"}])),
        )
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&password_config(&server.uri())).expect("client");
    client.authenticate().await.expect("logs in");
    let groups = client.groups().await.expect("groups");
    assert_eq!(groups.len(), 1);
}

#[tokio::test]
async fn a_lapsed_token_is_refreshed_once() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/accounts/token/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access": "stale",
            "refresh": "refresh-token"
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/accounts/token/refresh/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"access": "fresh"})),
        )
        .mount(&server)
        .await;
    // The first call with `stale` is rejected; the retry with `fresh` succeeds.
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/"))
        .and(header("Authorization", "Bearer stale"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/"))
        .and(header("Authorization", "Bearer fresh"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{"id": 1, "name": "SD"}])),
        )
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&password_config(&server.uri())).expect("client");
    let sources = client.epg_sources().await.expect("refreshes");
    assert_eq!(sources.len(), 1);
}

#[tokio::test]
async fn a_bad_password_fails_on_login_not_later() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/accounts/token/"))
        .respond_with(ResponseTemplate::new(401).set_body_string("{\"detail\":\"no\"}"))
        .mount(&server)
        .await;
    let client = DispatcharrClient::new(&password_config(&server.uri())).expect("client");
    let error = client.authenticate().await.expect_err("401");
    assert!(format!("{error}").contains("login failed"));
}

#[tokio::test]
async fn no_credentials_at_all_says_so() {
    let config = DispatcharrConfig {
        api_key: String::new(),
        api_key_env: String::new(),
        ..DispatcharrConfig::default()
    };
    let client = DispatcharrClient::new(&config).expect("client");
    let error = client.authenticate().await.expect_err("no credentials");
    assert!(format!("{error}").contains("no Dispatcharr credentials"));
}

#[tokio::test]
async fn epg_entries_accept_a_bare_array_with_a_null_tvg_id() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/epgdata/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "name": "ESPN", "tvg_id": "espn.us", "epg_source": 4},
            {"id": 2, "name": "Nameless", "tvg_id": null, "epg_source": 4}
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let entries = client.epg_entries().await.expect("entries");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[1].tvg_id, "");
}

#[tokio::test]
async fn epg_entries_also_walk_a_page_envelope() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/epgdata/"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "next": "http://x/api/epg/epgdata/?page=2",
            "results": [{"id": 1, "name": "ESPN", "tvg_id": "espn.us", "epg_source": 4}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/epg/epgdata/"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "next": null,
            "results": [{"id": 2, "name": "BBC One", "tvg_id": "bbc1.uk", "epg_source": 5}]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let entries = client.epg_entries().await.expect("entries");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[1].name, "BBC One");
}

#[tokio::test]
async fn profile_channels_come_from_the_profile_list() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/profiles/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "name": "All", "channels": [10, 11, 12]},
            {"id": 2, "name": "Favorites", "channels": [11]}
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    assert_eq!(
        client.profile_channels(1).await.expect("ids"),
        vec![10, 11, 12]
    );
    assert_eq!(client.profile_channels(2).await.expect("ids"), vec![11]);
    assert!(client.profile_channels(99).await.expect("ids").is_empty());
}

#[tokio::test]
async fn programme_checks_are_memoised_per_window() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/programs/search/"))
        .and(query_param("tvg_id", "espn.us"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 1,
            "results": [{"id": 42}]
        })))
        .expect(2)
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let window = ProgramWindow::ahead(12);
    assert_eq!(
        client
            .has_programs("espn.us", 4, &window)
            .await
            .expect("yes"),
        HasPrograms::Yes
    );
    // Same window: served from the cache.
    assert_eq!(
        client
            .has_programs("espn.us", 4, &window)
            .await
            .expect("yes"),
        HasPrograms::Yes
    );
    // A different window must not inherit the old answer, so this one goes out.
    let other = ProgramWindow {
        start: "2026-01-01T00:00:00Z".to_string(),
        end: "2026-01-02T00:00:00Z".to_string(),
    };
    assert_eq!(
        client
            .has_programs("espn.us", 4, &other)
            .await
            .expect("yes"),
        HasPrograms::Yes
    );
}

#[tokio::test]
async fn programme_checks_report_no_for_an_empty_result() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/programs/search/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 0,
            "results": []
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    assert_eq!(
        client
            .has_programs("empty.us", 4, &ProgramWindow::ahead(12))
            .await
            .expect("no"),
        HasPrograms::No
    );
}

#[tokio::test]
async fn an_entry_without_a_tvg_id_costs_no_request() {
    let server = MockServer::start().await;
    // No mock mounted: any request would fail with a 404.
    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    assert_eq!(
        client
            .has_programs("", 4, &ProgramWindow::ahead(12))
            .await
            .expect("no"),
        HasPrograms::No
    );
}

#[tokio::test]
async fn a_transient_server_error_is_retried() {
    let server = MockServer::start().await;
    // The first response is a 503; wiremock serves mocks in registration order when they
    // are equally specific, so the second one answers the retry.
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{"id": 1, "name": "SD"}])),
        )
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let sources = client.epg_sources().await.expect("retried");
    assert_eq!(sources.len(), 1);
}

#[tokio::test]
async fn user_level_comes_from_the_me_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/accounts/users/me/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"id": 1, "user_level": 10})),
        )
        .mount(&server)
        .await;
    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    assert_eq!(client.user_level().await.expect("level"), 10);
}

#[tokio::test]
async fn batch_set_epg_posts_associations_and_counts_updates() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/batch-set-epg/"))
        .and(body_json(serde_json::json!({
            "associations": [
                {"channel_id": 1, "epg_data_id": 10},
                {"channel_id": 2, "epg_data_id": 11}
            ]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "channels_updated": 1,
            "programs_refreshed": 2
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    // One channel was already on that entry upstream, so fewer updates than associations
    // is normal and must not read as a failure.
    assert_eq!(
        client
            .batch_set_epg(&[(1, Some(10)), (2, Some(11))])
            .await
            .expect("ok"),
        1
    );
}

#[tokio::test]
async fn batch_set_epg_surfaces_a_reported_failure() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": false,
            "channels_updated": 0
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let error = client
        .batch_set_epg(&[(1, Some(10))])
        .await
        .expect_err("failure");
    let message = format!("{error:#}");
    assert!(message.contains("reported failure"), "{message}");
    assert!(message.contains("chunk 1/1"), "{message}");
}

#[tokio::test]
async fn batch_set_epg_chunks_large_writes_and_sums_counts() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/batch-set-epg/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "channels_updated": 7
        })))
        .expect(3)
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let associations: Vec<(i64, Option<i64>)> =
        (1..=1200).map(|id| (id, Some(id + 10_000))).collect();
    assert_eq!(client.batch_set_epg(&associations).await.expect("ok"), 21);

    let requests = server.received_requests().await.expect("requests");
    assert_eq!(requests.len(), 3);
    let mut sizes = requests
        .iter()
        .map(|request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).expect("json body");
            body["associations"].as_array().expect("array").len()
        })
        .collect::<Vec<_>>();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![200, 500, 500]);
}

#[tokio::test]
async fn batch_set_epg_retries_a_transient_server_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/batch-set-epg/"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/batch-set-epg/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "channels_updated": 2
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    assert_eq!(
        client
            .batch_set_epg(&[(1, Some(10)), (2, Some(11))])
            .await
            .expect("ok"),
        2
    );
    let requests = server.received_requests().await.expect("requests");
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn batch_set_epg_refreshes_a_lapsed_jwt_and_retries() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/accounts/token/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access": "stale",
            "refresh": "refresh-token"
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/accounts/token/refresh/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"access": "fresh"})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/batch-set-epg/"))
        .and(header("Authorization", "Bearer stale"))
        .respond_with(ResponseTemplate::new(401))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/batch-set-epg/"))
        .and(header("Authorization", "Bearer fresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "channels_updated": 1
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&password_config(&server.uri())).expect("client");
    assert_eq!(client.batch_set_epg(&[(1, Some(10))]).await.expect("ok"), 1);
}

#[tokio::test]
async fn batch_set_epg_failure_names_the_chunk_and_confirmed_progress() {
    let server = MockServer::start().await;
    // The first chunk succeeds; every later attempt fails, so the error must say
    // which chunk failed and how many associations were already confirmed.
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/batch-set-epg/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "channels_updated": 500
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/batch-set-epg/"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let associations: Vec<(i64, Option<i64>)> = (1..=600).map(|id| (id, Some(id + 1))).collect();
    let error = client
        .batch_set_epg(&associations)
        .await
        .expect_err("fails");
    let message = format!("{error:#}");
    assert!(message.contains("chunk 2/2"), "{message}");
    assert!(
        message.contains("500 associations were confirmed"),
        "{message}"
    );
}

#[tokio::test]
async fn an_empty_association_list_writes_nothing() {
    let server = MockServer::start().await;
    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    assert_eq!(client.batch_set_epg(&[]).await.expect("ok"), 0);
}

#[tokio::test]
async fn load_world_fetches_everything() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/channels/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "name": "ESPN", "channel_group_id": 2, "streams": []}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/channels/groups/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!([{"id": 2, "name": "Sports"}])),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/channels/profiles/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "name": "All", "channels": [1]}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 4, "name": "Schedules Direct", "is_active": true, "priority": 5}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/epg/epgdata/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 7, "name": "ESPN", "tvg_id": "espn.us", "epg_source": 4}
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let world = client.load_world().await.expect("world");
    assert_eq!(world.channels.len(), 1);
    assert_eq!(world.group_name(Some(2)), "Sports");
    assert_eq!(world.group_name(None), "No Group");
    assert_eq!(world.sources[0].priority, 5);
    assert_eq!(world.entries[0].tvg_id, "espn.us");
}

#[tokio::test]
async fn list_endpoints_accept_a_results_envelope() {
    // Some builds wrap the listings in a page envelope, the way epg_entries already
    // tolerates: channels, groups, profiles and sources must read the same shape.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/channels/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 1,
            "next": null,
            "results": [{"id": 1, "name": "ESPN HD", "streams": []}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/channels/groups/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "results": [{"id": 10, "name": "Sports"}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/channels/profiles/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "results": [{"id": 3, "name": "Favs", "channels": [1]}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "results": [{"id": 4, "name": "SD"}]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let channels = client.channels().await.expect("channels");
    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].name, "ESPN HD");
    let groups = client.groups().await.expect("groups");
    assert_eq!(groups[0].name, "Sports");
    assert_eq!(client.profile_channels(3).await.expect("ids"), vec![1]);
    let sources = client.epg_sources().await.expect("sources");
    assert_eq!(sources[0].name, "SD");
}

#[tokio::test]
async fn list_endpoints_follow_a_non_null_next_page() {
    // An envelope with a non-null `next` means more pages: both must be returned,
    // not just the first.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/channels/"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "next": "http://x/api/channels/channels/?page=2",
            "results": [{"id": 1, "name": "ESPN HD", "streams": []}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/channels/channels/"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "next": null,
            "results": [{"id": 2, "name": "CNN", "streams": []}]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let channels = client.channels().await.expect("channels");
    assert_eq!(channels.len(), 2);
    assert_eq!(channels[0].name, "ESPN HD");
    assert_eq!(channels[1].name, "CNN");
}

#[tokio::test]
async fn a_list_object_without_results_is_an_error_not_an_empty_list() {
    // `results` is required: silently reading an empty list would drop channels.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/channels/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 0,
            "next": null
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let error = client.channels().await.expect_err("no results");
    let message = format!("{error:#}");
    assert!(message.contains("listing channels"), "{message}");
}

#[tokio::test]
async fn an_epg_entry_object_without_results_is_an_error_not_zero_entries() {
    // B1: `200 {}` used to parse as an empty page, and zero entries read as "every assigned
    // guide is gone" to scan and heal.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/epgdata/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let error = client.epg_entries().await.expect_err("no results");
    let message = format!("{error:#}");
    assert!(message.contains("listing EPG entries"), "{message}");
}

#[tokio::test]
async fn a_listed_channel_without_streams_fails_the_listing() {
    // S8/D-W2: stream-list writes replace the whole list, so a row that left `streams` out
    // must not read as "no streams" (tidy would hide it, a sort would write over it).
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/channels/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "name": "ESPN", "streams": [10]},
            {"id": 2, "name": "CNN"}
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let error = client.channels().await.expect_err("no streams key");
    assert!(
        format!("{error:#}").contains("listing channels"),
        "{error:#}"
    );
}

#[test]
fn a_profile_without_its_channel_list_is_an_error() {
    // B4: a missing `channels` key would read as "enables nothing", which clear_hidden
    // turns into "every channel is disabled".
    let parsed = serde_json::from_value::<ChannelProfile>(serde_json::json!({
        "id": 2, "name": "Favorites"
    }));
    assert!(parsed.is_err());
}

#[tokio::test]
async fn a_redirected_write_is_an_error_and_never_resent_as_a_get() {
    // D-W1: following a 301 on a PATCH re-sends it as a GET whose 200 would read as a
    // confirmed write. Redirects are not followed; the 3xx is the error.
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/epg/sources/4/"))
        .respond_with(
            ResponseTemplate::new(301)
                .insert_header("Location", format!("{}/elsewhere/", server.uri()).as_str()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/elsewhere/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 4})))
        .expect(0)
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let error = client
        .update_epg_source(4, &EpgSourceEdit::default())
        .await
        .expect_err("a redirect is not a write");
    let message = format!("{error:#}");
    assert!(message.contains("base_url redirects"), "{message}");
    assert!(message.contains("/elsewhere/"), "{message}");

    // Reads are refused the same way.
    Mock::given(method("GET"))
        .and(path("/api/channels/groups/"))
        .respond_with(ResponseTemplate::new(302).insert_header("Location", "/elsewhere/"))
        .mount(&server)
        .await;
    let error = client.groups().await.expect_err("a redirected read");
    assert!(format!("{error:#}").contains("base_url redirects"));
}

#[test]
fn api_root_appends_api_once() {
    let config = DispatcharrConfig {
        base_url: "http://host:9191".to_string(),
        ..DispatcharrConfig::default()
    };
    let client = DispatcharrClient::new(&config).expect("client");
    assert_eq!(client.base(), "http://host:9191/api");
}

#[test]
fn window_covers_the_requested_span() {
    let window = ProgramWindow::ahead(12);
    let start: chrono::DateTime<chrono::Utc> = window.start.parse().expect("iso");
    let end: chrono::DateTime<chrono::Utc> = window.end.parse().expect("iso");
    assert_eq!((end - start).num_hours(), 12);
}

// ── reads the later phases need ────────────────────────────────────────────────

#[tokio::test]
async fn streams_carry_their_probe_stats_account_and_group() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/streams/"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": 11,
                "name": "ESPN HD (backup)",
                "tvg_id": null,
                "stream_id": 4512,
                "is_custom": false,
                "url": "http://provider/live/user/pass/4512.ts",
                "m3u_account": 3,
                "channel_group": 7,
                "logo_url": "http://logo/espn.png",
                "stream_stats": {"resolution": "1920x1080", "fps": 59.94},
                "is_stale": false
            },
            {
                "id": 12,
                "name": "Hand made",
                "is_custom": true,
                "stream_id": null
            }
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let streams = client.streams().await.expect("streams");
    assert_eq!(streams.len(), 2);
    assert_eq!(streams[0].name, "ESPN HD (backup)");
    // Nullable upstream; a null must not fail the listing.
    assert_eq!(streams[0].tvg_id, "");
    assert_eq!(streams[0].m3u_account, Some(3));
    assert_eq!(streams[0].channel_group, Some(7));
    assert_eq!(streams[0].stream_stats["fps"], 59.94);
    // The provider id (mirror fills), the URL (probe) and the custom flag (keep custom).
    assert_eq!(streams[0].stream_id, Some(4512));
    assert_eq!(
        streams[0].url.as_deref(),
        Some("http://provider/live/user/pass/4512.ts")
    );
    assert!(!streams[0].is_custom);
    // A build (or a standard user's key) that omits the URL leaves it absent, not an error.
    assert_eq!(streams[1].stream_id, None);
    assert!(streams[1].is_custom);
    assert!(streams[1].url.is_none());
}

#[tokio::test]
async fn a_channel_stream_list_keeps_the_playback_order() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/channels/7/streams/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 30, "name": "first"},
            {"id": 20, "name": "second"}
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let streams = client.channel_streams(7).await.expect("streams");
    let ids: Vec<i64> = streams.iter().map(|stream| stream.id).collect();
    assert_eq!(ids, vec![30, 20]);
}

#[tokio::test]
async fn m3u_accounts_parse_the_provider_metadata() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/m3u/accounts/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 3, "name": "Provider A", "is_active": true, "account_type": "STD", "max_streams": 5,
             "server_url": "http://provider", "server_group": 9},
            {"id": 4, "name": "Custom", "account_type": "XC", "locked": true, "server_url": null,
             "server_group": null}
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let accounts = client.m3u_accounts().await.expect("accounts");
    assert_eq!(accounts.len(), 2);
    assert_eq!(accounts[0].max_streams, Some(5));
    assert!(accounts[1].locked);
    // Mirror backups group accounts by the server group, and match the same stream across
    // them by the provider stream id.
    assert_eq!(accounts[0].server_url.as_deref(), Some("http://provider"));
    assert_eq!(accounts[0].server_group, Some(9));
    assert!(accounts[1].server_url.is_none());
    assert_eq!(accounts[1].server_group, None);
}

#[tokio::test]
async fn logos_report_their_usage() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/logos/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "name": "ESPN", "url": "http://logo/espn.png", "channel_count": 3, "is_used": true}
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let logos = client.logos().await.expect("logos");
    assert_eq!(logos[0].channel_count, 3);
    assert!(logos[0].is_used);
}

#[tokio::test]
async fn creating_a_logo_posts_its_name_and_url() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/channels/logos/"))
        .and(body_json(serde_json::json!({
            "name": "ESPN",
            "url": "http://logo/espn.png"
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": 9,
            "name": "ESPN",
            "url": "http://logo/espn.png"
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let logo = client
        .create_logo("ESPN", "http://logo/espn.png")
        .await
        .expect("logo");
    assert_eq!(logo.id, 9);
}

#[tokio::test]
async fn a_create_answered_400_for_an_existing_name_or_url_returns_the_landed_object() {
    // DRF's UniqueValidator answers 400 before the serializer's get_or_create runs: the
    // shape a retry after a lost answer meets. The client looks the exact name/URL up.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/channels/groups/"))
        .respond_with(ResponseTemplate::new(400).set_body_json(
            serde_json::json!({"name": ["channel group with this name already exists."]}),
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/channels/groups/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 4, "name": "news"},
            {"id": 5, "name": "News"},
        ])))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/channels/logos/"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(serde_json::json!({"url": ["logo with this url already exists."]})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/channels/logos/"))
        .and(query_param("name", "CNN"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "results": [
                {"id": 8, "name": "CNN HD", "url": "http://logo/cnn-hd.png"},
                {"id": 9, "name": "CNN", "url": "http://logo/cnn.png"},
            ],
            "next": null,
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    // Exact, case-sensitive: "news" is a different group upstream.
    assert_eq!(client.create_group("News").await.expect("group").id, 5);
    assert!(
        client.create_group("Sports").await.is_err(),
        "nothing landed"
    );
    assert_eq!(
        client
            .create_logo("CNN", "http://logo/cnn.png")
            .await
            .expect("logo")
            .id,
        9
    );
}

#[tokio::test]
async fn a_group_is_created_and_renamed() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/channels/groups/"))
        .and(body_json(serde_json::json!({"name": "Sports"})))
        .respond_with(
            ResponseTemplate::new(201)
                .set_body_json(serde_json::json!({"id": 12, "name": "Sports"})),
        )
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/groups/12/"))
        .and(body_json(serde_json::json!({"name": "Sport"})))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"id": 12, "name": "Sport"})),
        )
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    assert_eq!(client.create_group("Sports").await.expect("group").id, 12);
    assert_eq!(
        client
            .rename_group(12, "Sport")
            .await
            .expect("renamed")
            .name,
        "Sport"
    );
}

// ── channel writes ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn edits_post_a_bare_array_and_send_null_where_a_field_is_cleared() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/edit/bulk/"))
        .and(body_json(serde_json::json!([
            {"id": 1, "name": "ESPN"},
            {"id": 2, "channel_group_id": 5},
            // `Set::Null`: an explicit null moves the channel to "No Group", which an absent
            // field would not.
            {"id": 3, "channel_group_id": null}
        ])))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": "Successfully updated 3 channels",
            "channels": [{"id": 1}, {"id": 2}, {"id": 3}]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let edits = vec![
        ChannelEdit::rename(1, "ESPN"),
        ChannelEdit::move_to_group(2, Some(5)),
        ChannelEdit::move_to_group(3, None),
    ];
    assert_eq!(client.edit_channels(&edits).await.expect("edited"), 3);
}

#[test]
fn an_untouched_edit_field_is_left_out_of_the_payload() {
    assert_eq!(
        serde_json::to_value(ChannelEdit::new(3)).expect("json"),
        serde_json::json!({"id": 3})
    );
    assert_eq!(
        serde_json::to_value(ChannelEdit::visible(3, false)).expect("json"),
        serde_json::json!({"id": 3, "hidden_from_output": true})
    );
}

#[tokio::test]
async fn edits_are_chunked_and_their_counts_summed() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/edit/bulk/"))
        .respond_with(|request: &wiremock::Request| {
            let edits: Vec<serde_json::Value> =
                serde_json::from_slice(&request.body).expect("json");
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "message": "ok",
                "channels": edits
            }))
        })
        .expect(3)
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let edits: Vec<ChannelEdit> = (1..=1200).map(|id| ChannelEdit::rename(id, "x")).collect();
    assert_eq!(client.edit_channels(&edits).await.expect("edited"), 1200);

    let requests = server.received_requests().await.expect("requests");
    let mut sizes = requests
        .iter()
        .map(|request| {
            let edits: Vec<serde_json::Value> =
                serde_json::from_slice(&request.body).expect("json body");
            edits.len()
        })
        .collect::<Vec<_>>();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![200, 500, 500]);
}

#[tokio::test]
async fn a_failing_edit_chunk_names_itself_and_the_confirmed_progress() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/edit/bulk/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": "ok",
            "channels": [{"id": 1}]
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/edit/bulk/"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "errors": [{"channel_id": 9, "error": "Channel not found"}]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let edits: Vec<ChannelEdit> = (0..600).map(|id| ChannelEdit::rename(id, "x")).collect();
    let error = client.edit_channels(&edits).await.expect_err("fails");
    let message = format!("{error:#}");
    assert!(message.contains("chunk 2/2"), "{message}");
    assert!(message.contains("500 channels were confirmed"), "{message}");
    assert!(message.contains("Channel not found"), "{message}");
}

#[tokio::test]
async fn a_regex_rename_posts_the_pattern_and_counts_the_names_it_changed() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/edit/bulk-regex/"))
        .and(body_json(serde_json::json!({
            "channel_ids": [1, 2],
            "find": "\\sHD$",
            "replace": " HD",
            "flags": "i"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "updated_count": 2
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let rename = ChannelRegexRename::new(vec![1, 2], "\\sHD$", " HD").ignoring_case();
    assert_eq!(
        client
            .rename_channels_regex(&rename)
            .await
            .expect("renamed"),
        2
    );
}

#[tokio::test]
async fn a_regex_rename_that_cannot_match_anything_costs_no_request() {
    // No mock mounted: a request would answer 404 and fail the test with the wrong error.
    let server = MockServer::start().await;
    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");

    let error = client
        .rename_channels_regex(&ChannelRegexRename::new(vec![], "HD", ""))
        .await
        .expect_err("no ids");
    assert!(format!("{error}").contains("at least one channel id"));

    let error = client
        .rename_channels_regex(&ChannelRegexRename::new(vec![1], "  ", ""))
        .await
        .expect_err("no pattern");
    assert!(format!("{error}").contains("needs a `find` pattern"));
}

#[tokio::test]
async fn profile_membership_updates_post_the_flags_and_count_both_writes() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/profiles/2/channels/bulk-update/"))
        .and(body_json(serde_json::json!({
            "channels": [
                {"channel_id": 10, "enabled": true},
                {"channel_id": 11, "enabled": false}
            ]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "success",
            "updated": 1,
            "created": 1,
            "invalid_channels": []
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    assert_eq!(
        client
            .set_profile_membership(2, &[(10, true), (11, false)])
            .await
            .expect("updated"),
        2
    );
}

#[tokio::test]
async fn profile_membership_writes_are_chunked() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/profiles/2/channels/bulk-update/"))
        .respond_with(|request: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).expect("json body");
            let count = body["channels"].as_array().expect("array").len();
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "success",
                "updated": count,
                "created": 0,
                "invalid_channels": []
            }))
        })
        .expect(3)
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let channels: Vec<(i64, bool)> = (1..=1200).map(|id| (id, true)).collect();
    assert_eq!(
        client
            .set_profile_membership(2, &channels)
            .await
            .expect("ok"),
        1200
    );
}

#[tokio::test]
async fn a_membership_update_that_names_unknown_channels_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/profiles/2/channels/bulk-update/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "success",
            "updated": 0,
            "created": 0,
            "invalid_channels": [99]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let error = client
        .set_profile_membership(2, &[(99, true)])
        .await
        .expect_err("invalid");
    assert!(format!("{error:#}").contains("rejected channel ids [99]"));
}

#[tokio::test]
async fn clearing_a_guide_sends_a_null_epg_data_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/batch-set-epg/"))
        .and(body_json(serde_json::json!({
            "associations": [
                {"channel_id": 1, "epg_data_id": null},
                {"channel_id": 2, "epg_data_id": 11}
            ]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "channels_updated": 2
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    assert_eq!(
        client
            .batch_set_epg(&[(1, None), (2, Some(11))])
            .await
            .expect("ok"),
        2
    );
}

#[tokio::test]
async fn setting_a_channels_streams_patches_that_one_channel() {
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/7/"))
        .and(body_json(serde_json::json!({"streams": [30, 20, 10]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 7,
            "name": "ESPN",
            "streams": [30, 20, 10]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let channel = client
        .set_channel_streams(7, &[30, 20, 10])
        .await
        .expect("written");
    assert_eq!(channel.streams, vec![30, 20, 10]);
}

#[tokio::test]
async fn bulk_stream_lists_go_out_as_edit_bulk_rows() {
    // `edit/bulk` accepts a `streams` array per row and replaces each channel's list with
    // the array's order, so two channels cost one request rather than one PATCH each.
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/edit/bulk/"))
        .and(body_json(serde_json::json!([
            {"id": 7, "streams": [30, 20]},
            {"id": 8, "streams": []}
        ])))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": "ok",
            "channels": [{"id": 7}, {"id": 8}]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let results = client
        .bulk_set_channel_streams(&[(7, vec![30, 20]), (8, vec![])])
        .await;
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|(_, result)| result.is_ok()));
    // One request for both channels: nothing fell back to the per-channel route.
    assert_eq!(
        server.received_requests().await.unwrap_or_default().len(),
        1
    );
}

#[tokio::test]
async fn a_rejected_bulk_chunk_falls_back_to_one_patch_per_channel() {
    // One bad row rejects its whole `edit/bulk` chunk with a 400 before anything is
    // written. The chunk is then retried row by row, so the good channel still gets its
    // list and only the bad one fails — with that row's own error, not the chunk's.
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/edit/bulk/"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "errors": [{"channel_id": 8, "errors": {"streams": ["Invalid pk"]}}],
            "updated_count": 0
        })))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/7/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 7})))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/8/"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "streams": ["Invalid pk \"999999\" - object does not exist."]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let results = client
        .bulk_set_channel_streams(&[(7, vec![30, 20]), (8, vec![999_999])])
        .await;
    assert_eq!(results.len(), 2);
    assert!(results[0].1.is_ok(), "{:?}", results[0].1);
    let error = format!("{:?}", results[1].1);
    assert!(error.contains('8'), "{error}");
    // One rejected bulk request plus one PATCH per channel in the chunk.
    assert_eq!(
        server.received_requests().await.unwrap_or_default().len(),
        3
    );
}

#[tokio::test]
async fn bulk_stream_lists_are_chunked_at_five_hundred() {
    // `edit/bulk` validates the chunk whole, so the client keeps chunks at the same 500 the
    // name/group edits use; 501 channels cost two bulk requests and no per-channel PATCH.
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/channels/edit/bulk/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "message": "ok",
            "channels": []
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let lists: Vec<(i64, Vec<i64>)> = (1..=501).map(|id| (id, vec![id + 10_000])).collect();
    let results = client.bulk_set_channel_streams(&lists).await;
    assert_eq!(results.len(), 501);
    assert!(results.iter().all(|(_, result)| result.is_ok()));
    assert_eq!(
        server.received_requests().await.unwrap_or_default().len(),
        2
    );
}

#[tokio::test]
async fn stream_stats_are_patched_on_that_one_stream() {
    // `/channels/streams/{id}/` is the only route that writes one stream's stats, and the
    // column is writable there even for a provider stream. The timestamp goes with it: nothing
    // upstream stamps it for an API write, and Dispatcharr's own stats delta filters on it.
    let server = MockServer::start().await;
    // Partial: the timestamp is the caller's clock, so only its presence can be asserted.
    Mock::given(method("PATCH"))
        .and(path("/api/channels/streams/11/"))
        .and(body_partial_json(serde_json::json!({
            "stream_stats": {"height": 1080, "fps": 59.94}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 11,
            "name": "ESPN HD",
            "stream_stats": {"height": 1080, "fps": 59.94}
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let stats = serde_json::json!({"height": 1080, "fps": 59.94});
    let written = client
        .update_stream_stats(11, &stats)
        .await
        .expect("written");
    assert_eq!(written.id, 11);
    assert_eq!(written.stream_stats["height"], 1080);

    // The write also stamps `stream_stats_updated_at`, which is what Dispatcharr's stats delta
    // filters on; without it a probe's result never reaches the channel table.
    let requests = server.received_requests().await.expect("requests");
    let body: serde_json::Value =
        serde_json::from_slice(&requests[0].body).expect("the request body");
    let stamped = body["stream_stats_updated_at"]
        .as_str()
        .expect("a timestamp");
    assert!(stamped.ends_with('Z'), "{stamped}");
}

#[tokio::test]
async fn stream_stats_are_cleared_with_an_empty_object() {
    // A dead stream's stats are cleared to `{}` (IPTV Checker's own rule), which is a real
    // write rather than "no change".
    let server = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path("/api/channels/streams/11/"))
        .and(wiremock::matchers::body_string_contains(
            "\"stream_stats\":{}",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 11})))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    client
        .update_stream_stats(11, &serde_json::json!({}))
        .await
        .expect("cleared");
}

#[tokio::test]
async fn current_programs_are_asked_for_by_epg_entry_and_chunked_at_fifty() {
    // `POST /epg/current-programs/` takes epg_data_ids and answers one flat array. The view
    // truncates at 50 ids, so 60 entries must be two requests or the last 10 are silently
    // unanswered.
    let server = MockServer::start().await;
    let first: Vec<i64> = (1..=50).collect();
    Mock::given(method("POST"))
        .and(path("/api/epg/current-programs/"))
        .and(body_json(serde_json::json!({"epg_data_ids": first})))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 900, "epg_data_id": 1, "title": "SportsCenter",
             "start_time": "2026-11-01T11:00:00Z", "end_time": "2026-11-01T12:00:00Z"},
            {"epg_data_id": 2, "parsing": true}
        ])))
        .mount(&server)
        .await;
    let second: Vec<i64> = (51..=60).collect();
    Mock::given(method("POST"))
        .and(path("/api/epg/current-programs/"))
        .and(body_json(serde_json::json!({"epg_data_ids": second})))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"epg_data_id": 51, "title": null, "tvg_id": null}
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let ids: Vec<i64> = (1..=60).collect();
    let programs = client.current_programs(&ids).await.expect("programs");
    assert_eq!(programs.len(), 3);
    assert_eq!(programs[0].epg_data_id, 1);
    assert_eq!(programs[0].title, "SportsCenter");
    assert!(programs[0].has_title());
    assert!(programs[0].start_time.is_some());
    // The "index rebuilding" marker is not a programme, and must not read as "nothing airing".
    assert_eq!(programs[1].epg_data_id, 2);
    assert!(programs[1].is_parsing());
    assert!(!programs[1].has_title());
    // A null title is absent, not a parse error.
    assert_eq!(programs[2].title, "");
    assert_eq!(programs[2].tvg_id, "");
}

#[tokio::test]
async fn an_empty_current_program_request_costs_nothing() {
    let server = MockServer::start().await;
    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let programs = client.current_programs(&[]).await.expect("nothing");
    assert!(programs.is_empty());
    assert_eq!(
        server.received_requests().await.unwrap_or_default().len(),
        0
    );
}

#[tokio::test]
async fn creating_a_channel_posts_the_typed_payload() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/"))
        .and(body_json(serde_json::json!({
            "name": "ESPN",
            "channel_group_id": 5,
            "streams": [11],
            "channel_profile_ids": []
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": 42,
            "name": "ESPN",
            "channel_group_id": 5,
            "streams": [11]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let create = ChannelCreate {
        channel_group_id: Some(5),
        streams: Some(vec![11]),
        channel_profile_ids: Some(Vec::new()),
        ..ChannelCreate::new("ESPN")
    };
    let channel = client.create_channel(&create).await.expect("created");
    assert_eq!(channel.id, 42);
    assert_eq!(channel.streams, vec![11]);
}

#[tokio::test]
async fn bulk_channel_creation_returns_the_queued_task() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/channels/channels/from-stream/bulk/"))
        .and(body_json(serde_json::json!({
            "stream_ids": [1, 2],
            "starting_channel_number": 0
        })))
        .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
            "task_id": "6f1c",
            "message": "Bulk channel creation task started for 2 streams",
            "stream_count": 2,
            "status": "started"
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let request = StreamsToChannels {
        stream_ids: vec![1, 2],
        channel_profile_ids: None,
        starting_channel_number: Some(0),
    };
    let task = client
        .create_channels_from_streams(&request)
        .await
        .expect("queued");
    assert_eq!(task.task_id, "6f1c");
    assert_eq!(task.stream_count, Some(2));
}

// ── EPG sources, horizons and refreshes ───────────────────────────────────────

#[test]
fn a_source_knows_whether_it_is_dummy_broken_or_busy() {
    let dummy = EpgSource {
        source_type: "dummy".to_string(),
        status: "idle".to_string(),
        ..EpgSource::default()
    };
    assert!(dummy.is_dummy());
    assert!(!dummy.is_broken());
    assert!(!dummy.is_refreshing());

    let broken = EpgSource {
        source_type: "xmltv".to_string(),
        status: "error".to_string(),
        ..EpgSource::default()
    };
    assert!(broken.is_broken());

    let busy = EpgSource {
        status: "parsing".to_string(),
        ..EpgSource::default()
    };
    assert!(busy.is_refreshing());
}

#[tokio::test]
async fn a_dummy_source_is_created_at_every_sources_endpoint() {
    let server = MockServer::start().await;
    let row = serde_json::json!({
        "id": 9,
        "name": "Events placeholder",
        "source_type": "dummy",
        "is_active": true,
        "priority": 0,
        "status": "idle",
        "updated_at": "2026-01-01T00:00:00Z",
        "custom_properties": {"title_pattern": "^(?P<team>.+?) vs"},
        "has_channels": true
    });

    Mock::given(method("POST"))
        .and(path("/api/epg/sources/"))
        .and(body_json(serde_json::json!({
            "name": "Events placeholder",
            "source_type": "dummy",
            "custom_properties": {"title_pattern": "^(?P<team>.+?) vs"}
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(row.clone()))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/api/epg/sources/9/"))
        .and(body_json(serde_json::json!({"priority": 5})))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 9,
            "name": "Events placeholder",
            "source_type": "dummy",
            "priority": 5
        })))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/epg/sources/9/"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let created = client
        .create_epg_source(&EpgSourceEdit::dummy(
            "Events placeholder",
            serde_json::json!({"title_pattern": "^(?P<team>.+?) vs"}),
        ))
        .await
        .expect("created");
    assert_eq!(created.id, 9);
    assert!(created.is_dummy());
    assert_eq!(
        created.custom_properties["title_pattern"],
        "^(?P<team>.+?) vs"
    );
    assert!(created.has_channels);
    assert!(created.updated_at.is_some());

    let updated = client
        .update_epg_source(
            9,
            &EpgSourceEdit {
                priority: Some(5),
                ..EpgSourceEdit::default()
            },
        )
        .await
        .expect("updated");
    assert_eq!(updated.priority, 5);

    client.delete_epg_source(9).await.expect("deleted");
}

#[tokio::test]
async fn creating_a_source_without_a_name_or_type_costs_no_request() {
    let server = MockServer::start().await;
    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");

    let error = client
        .create_epg_source(&EpgSourceEdit {
            source_type: Some("dummy".to_string()),
            ..EpgSourceEdit::default()
        })
        .await
        .expect_err("no name");
    assert!(format!("{error}").contains("needs a name"));

    let error = client
        .create_epg_source(&EpgSourceEdit {
            name: Some("Nameless".to_string()),
            ..EpgSourceEdit::default()
        })
        .await
        .expect_err("no type");
    assert!(format!("{error}").contains("needs a source_type"));
}

#[tokio::test]
async fn a_refresh_request_posts_the_source_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/epg/import/"))
        .and(body_json(serde_json::json!({"id": 9})))
        .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
            "success": true,
            "message": "EPG data refresh initiated."
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    client.refresh_epg_source(9).await.expect("requested");
}

#[tokio::test]
async fn a_refresh_that_dispatcharr_refuses_says_why() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/epg/import/"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "success": false,
            "message": "Dummy EPG sources do not require refreshing."
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let error = client.refresh_epg_source(9).await.expect_err("refused");
    let message = format!("{error:#}");
    assert!(
        message.contains("Dummy EPG sources do not require refreshing"),
        "{message}"
    );
}

#[tokio::test]
async fn a_source_state_is_read_with_one_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/9/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 9,
            "name": "Schedules Direct",
            "status": "fetching",
            "updated_at": null,
            "last_message": "",
            "epg_data_count": 120
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let state = client.epg_source_state(9).await.expect("state");
    assert_eq!(state.id, 9);
    assert_eq!(state.status, "fetching");
    assert_eq!(state.updated_at, None);
}

#[tokio::test]
async fn the_cheap_horizon_probe_is_one_filtered_search() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/programs/search/"))
        .and(query_param("epg_source", "4"))
        .and(query_param("end_after", "2026-05-01T00:00:00Z"))
        .and(query_param("page_size", "1"))
        .and(query_param("fields", "end_time"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 87,
            "next": null,
            "previous": null,
            "results": [{"end_time": "2026-05-02T03:00:00Z"}]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let cutoff: chrono::DateTime<chrono::Utc> = "2026-05-01T00:00:00Z".parse().expect("iso");
    assert!(client.epg_ends_at_or_after(4, cutoff).await.expect("probe"));
}

#[tokio::test]
async fn an_empty_probe_reports_no_coverage() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/programs/search/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 0,
            "next": null,
            "previous": null,
            "results": []
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let cutoff: chrono::DateTime<chrono::Utc> = "2026-05-01T00:00:00Z".parse().expect("iso");
    assert!(!client.epg_ends_at_or_after(4, cutoff).await.expect("probe"));
}

#[tokio::test]
async fn the_horizon_bisects_the_bracket_to_the_minute() {
    let server = MockServer::start().await;
    // The search endpoint cannot order by end_time, so the client has to find the last
    // programme by asking "does anything end at or after T?". The stub answers exactly that
    // for a guide that really runs out at the first instant below.
    let horizon: chrono::DateTime<chrono::Utc> = "2026-01-10T06:30:00Z".parse().expect("iso");
    Mock::given(method("GET"))
        .and(path("/api/epg/programs/search/"))
        .and(query_param("epg_source", "4"))
        .and(query_param("page_size", "1"))
        .and(query_param("fields", "end_time"))
        .respond_with(move |request: &wiremock::Request| {
            let cutoff = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "end_after")
                .map(|(_, value)| value.to_string())
                .expect("end_after");
            let cutoff: chrono::DateTime<chrono::Utc> = cutoff.parse().expect("iso cutoff");
            let covered = horizon >= cutoff;
            let results = if covered {
                serde_json::json!([{"end_time": "2026-01-10T06:30:00Z"}])
            } else {
                serde_json::json!([])
            };
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": usize::from(covered),
                "next": null,
                "previous": null,
                "results": results
            }))
        })
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let since: chrono::DateTime<chrono::Utc> = "2026-01-01T00:00:00Z".parse().expect("iso");
    let until: chrono::DateTime<chrono::Utc> = "2026-02-01T00:00:00Z".parse().expect("iso");
    let found = client
        .epg_horizon_between(4, since, until)
        .await
        .expect("horizon");
    let at = match found {
        EpgHorizon::At(at) => at,
        other => panic!("expected a found horizon, got {other:?}"),
    };
    // Rounded down to the minute: never later than the truth, never more than a minute early.
    assert!(at <= horizon, "{at} <= {horizon}");
    assert!(horizon - at < chrono::Duration::minutes(1), "{at}");

    let requests = server.received_requests().await.expect("requests");
    // Two bracket probes plus one per halving of a 31-day span, to the minute.
    assert!(
        requests.len() <= 20,
        "the bisection must stay cheap, took {} requests",
        requests.len()
    );
    assert!(requests.len() > 3);
}

#[tokio::test]
async fn a_bracket_that_is_already_empty_or_overrun_says_so() {
    let server = MockServer::start().await;
    let horizon: chrono::DateTime<chrono::Utc> = "2026-01-10T00:00:00Z".parse().expect("iso");
    Mock::given(method("GET"))
        .and(path("/api/epg/programs/search/"))
        .respond_with(move |request: &wiremock::Request| {
            let cutoff = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "end_after")
                .map(|(_, value)| value.to_string())
                .expect("end_after");
            let cutoff: chrono::DateTime<chrono::Utc> = cutoff.parse().expect("iso cutoff");
            let covered = horizon >= cutoff;
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": usize::from(covered),
                "results": if covered { serde_json::json!([{"end_time": "2026-01-10T00:00:00Z"}]) } else { serde_json::json!([]) }
            }))
        })
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    // Everything has already run out.
    let since: chrono::DateTime<chrono::Utc> = "2026-03-01T00:00:00Z".parse().expect("iso");
    let until: chrono::DateTime<chrono::Utc> = "2026-04-01T00:00:00Z".parse().expect("iso");
    assert_eq!(
        client
            .epg_horizon_between(4, since, until)
            .await
            .expect("empty"),
        EpgHorizon::Empty
    );

    // So much guide that the bracket cannot see the end of it.
    let since: chrono::DateTime<chrono::Utc> = "2026-01-01T00:00:00Z".parse().expect("iso");
    let until: chrono::DateTime<chrono::Utc> = "2026-01-05T00:00:00Z".parse().expect("iso");
    assert_eq!(
        client
            .epg_horizon_between(4, since, until)
            .await
            .expect("beyond"),
        EpgHorizon::Beyond(until)
    );
}

#[tokio::test]
async fn a_refresh_poll_reports_the_state_that_moved() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/9/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 9,
            "status": "fetching",
            "updated_at": null
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/9/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 9,
            "status": "error",
            "updated_at": null,
            "last_message": "HTTP error 404"
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let before = EpgSourceState {
        id: 9,
        status: "fetching".to_string(),
        updated_at: None,
        last_message: None,
    };
    let outcome = client
        .await_epg_refresh(9, &before, Duration::from_secs(5), || false)
        .await
        .expect("polled");
    match outcome {
        EpgRefresh::Changed(state) => {
            // A failed refresh moves the status only; waiting for `updated_at` would hang.
            assert_eq!(state.status, "error");
            assert_eq!(state.last_message.as_deref(), Some("HTTP error 404"));
        }
        other => panic!("expected a change, got {other:?}"),
    }
}

#[tokio::test]
async fn a_refresh_poll_waits_past_mid_flight_for_the_settled_state() {
    // The regression this pins: the first movement is `fetching`, and returning it would hand
    // the watchdog a source with no settled horizon to read — every healthy refresh would
    // classify `still_broken`. The poll must wait for the terminal state instead.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/9/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 9,
            "status": "fetching",
            "updated_at": null
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/9/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 9,
            "status": "success",
            "updated_at": "2026-07-17T12:00:00Z"
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let before = EpgSourceState {
        id: 9,
        status: "success".to_string(),
        updated_at: None,
        last_message: None,
    };
    let outcome = client
        .await_epg_refresh(9, &before, Duration::from_secs(30), || false)
        .await
        .expect("polled");
    match outcome {
        EpgRefresh::Changed(state) => assert_eq!(state.status, "success"),
        other => panic!("expected the settled state, got {other:?}"),
    }
}

#[tokio::test]
async fn a_refresh_poll_gives_up_at_the_timeout() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/epg/sources/9/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 9,
            "status": "parsing",
            "updated_at": null
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let before = EpgSourceState {
        id: 9,
        status: "parsing".to_string(),
        updated_at: None,
        last_message: None,
    };
    let outcome = client
        .await_epg_refresh(9, &before, Duration::from_millis(120), || false)
        .await
        .expect("polled");
    match outcome {
        EpgRefresh::TimedOut(state) => assert_eq!(state.status, "parsing"),
        other => panic!("expected a timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn a_refresh_poll_stops_when_the_operator_cancels() {
    let server = MockServer::start().await;
    // Nothing mounted: the poll must not send a request after a cancel.
    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let before = EpgSourceState::default();
    let error = client
        .await_epg_refresh(9, &before, Duration::from_secs(5), || true)
        .await
        .expect_err("cancelled");
    assert!(format!("{error}").contains("cancelled by the operator"));
}

// ── plugins (the bridge) ───────────────────────────────────────────────────────

#[tokio::test]
async fn the_plugin_list_reports_presence_version_and_actions() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/plugins/plugins/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "plugins": [
                {
                    "key": "crosstalk-bridge",
                    "name": "Crosstalk Bridge",
                    "version": "0.2.0",
                    "enabled": true,
                    "missing": false,
                    "actions": [{"id": "orm_write", "label": "ORM write"}]
                },
                {"key": "ghost", "name": "Ghost", "missing": true}
            ]
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let plugins = client.plugins().await.expect("plugins");
    assert_eq!(plugins.len(), 2);

    let bridge = client
        .plugin("crosstalk-bridge")
        .await
        .expect("listed")
        .expect("installed");
    assert_eq!(bridge.version, "0.2.0");
    assert!(bridge.has_action("orm_write"));
    assert!(!bridge.has_action("something_else"));

    // A config row with no code behind it cannot run anything, so it reads as absent.
    assert!(client.plugin("ghost").await.expect("listed").is_none());
    assert!(client.plugin("nope").await.expect("listed").is_none());
}

#[tokio::test]
async fn running_a_plugin_action_posts_the_action_and_its_params() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/plugins/plugins/crosstalk-bridge/run/"))
        .and(body_json(serde_json::json!({
            "action": "orm_write",
            "params": {"streams": {"7": [30, 20]}}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "result": {"updated": 1}
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let result = client
        .run_plugin_action(
            "crosstalk-bridge",
            "orm_write",
            &serde_json::json!({"streams": {"7": [30, 20]}}),
        )
        .await
        .expect("ran");
    assert_eq!(result["updated"], 1);
}

#[tokio::test]
async fn a_plugin_that_refuses_an_action_reports_its_reason() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/plugins/plugins/crosstalk-bridge/run/"))
        .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
            "success": false,
            "error": "Plugin is disabled"
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let error = client
        .run_plugin_action("crosstalk-bridge", "orm_write", &serde_json::json!({}))
        .await
        .expect_err("refused");
    assert!(format!("{error:#}").contains("Plugin is disabled"));
}

#[tokio::test]
async fn a_plugin_key_that_is_not_one_path_segment_is_refused_before_sending() {
    let server = MockServer::start().await;
    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");

    let error = client
        .run_plugin_action("../secrets", "orm_write", &serde_json::json!({}))
        .await
        .expect_err("bad key");
    assert!(format!("{error}").contains("not a valid plugin key"));

    let error = client
        .run_plugin_action("crosstalk-bridge", "  ", &serde_json::json!({}))
        .await
        .expect_err("no action");
    assert!(format!("{error}").contains("needs an action id"));
}

// ── a world that only loads what it needs ─────────────────────────────────────

#[test]
fn world_kinds_compose_and_default_to_the_guide_slices() {
    let both = WorldKind::Channels | WorldKind::Groups;
    assert!(both.contains(WorldKind::Channels));
    assert!(both.contains(WorldKind::Groups));
    assert!(!both.contains(WorldKind::Streams));

    assert_eq!(
        WorldKinds::only(WorldKind::Streams).with(WorldKind::Logos),
        WorldKinds::only(WorldKind::Logos) | WorldKind::Streams
    );
    assert_eq!(
        WorldKinds::DEFAULT
            .with(WorldKind::Streams)
            .without(WorldKind::Streams),
        WorldKinds::DEFAULT
    );

    // `load_world()` keeps fetching exactly what it always did: the streams and logos of a
    // large install are not part of a guide run.
    assert_eq!(WorldKinds::default(), WorldKinds::DEFAULT);
    assert!(!WorldKinds::DEFAULT.contains(WorldKind::Streams));
    assert!(!WorldKinds::DEFAULT.contains(WorldKind::M3uAccounts));
    assert!(!WorldKinds::DEFAULT.contains(WorldKind::Logos));
    for kind in [
        WorldKind::Channels,
        WorldKind::Groups,
        WorldKind::Profiles,
        WorldKind::EpgSources,
        WorldKind::EpgEntries,
        WorldKind::Streams,
        WorldKind::M3uAccounts,
        WorldKind::Logos,
    ] {
        assert!(WorldKinds::ALL.contains(kind), "{kind:?}");
    }
    assert!(WorldKinds::NONE.is_empty());
}

#[tokio::test]
async fn a_selected_world_fetches_only_what_was_asked_for() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/channels/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!([{"id": 1, "name": "ESPN", "channel_group_id": 2, "streams": []}]),
        ))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let world = client
        .load_world_with(WorldKinds::only(WorldKind::Channels))
        .await
        .expect("world");
    assert_eq!(world.channels.len(), 1);
    assert!(world.has(WorldKind::Channels));
    // Not fetched, so empty and reported as such rather than as "there are no groups".
    assert!(world.groups.is_empty());
    assert!(!world.has(WorldKind::Groups));
    let error = world.require(WorldKind::Groups).expect_err("not loaded");
    assert!(format!("{error}").contains("Groups"), "{error}");

    let requests = server.received_requests().await.expect("requests");
    assert_eq!(requests.len(), 1, "an unasked slice must cost no request");
}

#[tokio::test]
async fn an_everything_world_carries_streams_accounts_and_logos() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels/streams/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!([{"id": 11, "name": "ESPN HD"}])),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/m3u/accounts/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!([{"id": 3, "name": "Provider A"}])),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/channels/logos/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "name": "ESPN", "url": "http://logo/espn.png"}
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    let world = client
        .load_world_with(WorldKind::Streams | WorldKind::M3uAccounts | WorldKind::Logos)
        .await
        .expect("world");
    assert_eq!(world.stream_map().len(), 1);
    assert_eq!(world.m3u_account_map()[&3].name, "Provider A");
    assert_eq!(world.logos.len(), 1);
}

#[test]
fn coerce_timezone_keeps_valid_names_and_falls_back_to_utc() {
    // Stream-Mapparr's `coerce_timezone`, including the link names pytz accepts.
    assert_eq!(coerce_timezone("US/Eastern"), "US/Eastern");
    assert_eq!(coerce_timezone("Europe/Oslo"), "Europe/Oslo");
    assert_eq!(coerce_timezone("  UTC  "), "UTC");
    assert_eq!(coerce_timezone(""), "UTC");
    assert_eq!(coerce_timezone("   "), "UTC");
    assert_eq!(coerce_timezone("Not/AZone"), "UTC");
    assert_eq!(coerce_timezone("US/Central/Bogus"), "UTC");
}

#[test]
fn today_in_zone_matches_utc_for_utc_and_the_host_for_garbage() {
    use chrono::Datelike;
    let now = chrono::Utc::now().date_naive();
    assert_eq!(today_in_zone("UTC"), (now.month(), now.day()));
    let local = chrono::Local::now().date_naive();
    assert_eq!(today_in_zone("Not/AZone"), (local.month(), local.day()));
}

#[tokio::test]
async fn system_timezone_reads_the_system_settings_row() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/core/settings/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "key": "stream_settings", "name": "Stream Settings", "value": {}},
            {"id": 2, "key": "system_settings", "name": "System Settings",
             "value": {"time_zone": "America/Phoenix"}},
        ])))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    assert_eq!(
        client.system_timezone().await.expect("zone"),
        "America/Phoenix"
    );
}

#[tokio::test]
async fn system_timezone_accepts_a_page_envelope_and_repairs_bad_names() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/core/settings/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "results": [
                {"id": 2, "key": "system_settings", "name": "System Settings",
                 "value": {"time_zone": "Not/AZone"}},
            ],
            "next": null,
        })))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    // An unknown name is not an error: the plugin falls back to UTC, so this does too.
    assert_eq!(client.system_timezone().await.expect("zone"), "UTC");
}

#[tokio::test]
async fn system_timezone_is_utc_when_the_row_or_name_is_missing() {
    for body in [
        serde_json::json!([]),
        serde_json::json!([{"id": 1, "key": "stream_settings", "value": {}}]),
        serde_json::json!([{"id": 2, "key": "system_settings", "value": {}}]),
        serde_json::json!([{"id": 2, "key": "system_settings", "value": {"time_zone": null}}]),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/core/settings/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        assert_eq!(client.system_timezone().await.expect("zone"), "UTC");
    }
}

#[tokio::test]
async fn system_timezone_propagates_a_transport_failure_for_the_caller_to_degrade() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/core/settings/"))
        .respond_with(ResponseTemplate::new(500).set_body_string("bad gateway"))
        .mount(&server)
        .await;

    let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
    client.system_timezone().await.expect_err("500");
}
