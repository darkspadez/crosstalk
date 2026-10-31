//! The Dispatcharr REST client.
//!
//! The Python plugin read Dispatcharr's database directly through the Django ORM. A separate
//! process cannot, so this is the interface it has instead. Everything the matcher needs is
//! here: channels, groups, profiles, EPG sources, EPG entries, and the one write (assign EPG
//! to channels).
//!
//! Two shapes of the API are load-bearing and easy to get wrong:
//!
//!   * the channel list is only paginated when a `page` or `page_size` parameter is present,
//!     so asking for it plainly returns everything;
//!   * the programme search has no `epg_id` filter, so the programme-data check is keyed on
//!     `(tvg_id, epg_source)`. It is the expensive call — it is the only thing the plugin did
//!     in-process that costs a round trip here — so it is memoised and gated by a semaphore.

use crate::config::DispatcharrConfig;
use anyhow::{bail, Context, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// One channel, as the matcher needs it.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Channel {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub channel_number: Option<serde_json::Value>,
    #[serde(default)]
    pub channel_group_id: Option<i64>,
    /// The EPG entry currently assigned, if any.
    #[serde(default)]
    pub epg_data_id: Option<i64>,
    /// Present in newer Dispatcharr builds: an override can pin the EPG without changing the
    /// channel's own column. The effective value is what the UI shows.
    #[serde(default)]
    pub effective_epg_data_id: Option<i64>,
}

impl Channel {
    /// The EPG entry this channel is actually on.
    pub fn current_epg_id(&self) -> Option<i64> {
        self.effective_epg_data_id.or(self.epg_data_id)
    }

    /// The channel number as the reports print it.
    pub fn display_number(&self) -> Option<f64> {
        match self.channel_number.as_ref()? {
            serde_json::Value::Number(number) => number.as_f64(),
            serde_json::Value::String(text) => text.parse().ok(),
            _ => None,
        }
    }
}

/// A channel group.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ChannelGroup {
    pub id: i64,
    #[serde(default)]
    pub name: String,
}

/// A channel profile.
///
/// `channels` is the list of channel ids ENABLED in the profile, which Dispatcharr's profile
/// serializer computes server-side (`ChannelProfileSerializer.get_channels`). That makes the
/// profile scope filter a single request rather than one per channel.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ChannelProfile {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub channels: Vec<i64>,
}

/// One EPG source.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct EpgSource {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_true")]
    pub is_active: bool,
    /// Higher wins when two candidates score the same.
    #[serde(default)]
    pub priority: i64,
}

fn default_true() -> bool {
    true
}

/// One EPG entry: a channel inside a guide source.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct EpgEntry {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub tvg_id: String,
    #[serde(default)]
    pub epg_source: Option<i64>,
}

/// A programme row. The check only needs to know whether one exists, so the fields are not
/// read; uid=501(darkspadez) gid=20(staff) groups=20(staff),12(everyone),61(localaccounts),79(_appserverusr),80(admin),81(_appserveradm),701(com.apple.sharepoint.group.1),33(_appstore),98(_lpadmin),100(_lpoperator),204(_developer),250(_analyticsusers),395(com.apple.access_ftp),398(com.apple.access_screensharing),399(com.apple.access_ssh),400(com.apple.access_remote_ae) is named so a renamed column cannot turn a real row into a parse error.
#[derive(Debug, Clone, Deserialize)]
struct ProgramRow {
    #[serde(default, rename = "id")]
    _id: Option<i64>,
}

/// How a programme-data check is answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HasPrograms {
    Yes,
    No,
}

/// The client.
///
/// Cheap to clone: the HTTP connection pool is shared, so a run's threads can each hold a
/// handle without duplicating sockets.
#[derive(Clone)]
pub struct DispatcharrClient {
    http: reqwest::Client,
    base: String,
    /// Memoised programme checks, keyed by `(tvg_id, source id)`.
    program_cache: Arc<dashmap::DashMap<(String, i64), HasPrograms>>,
    /// Caps concurrent programme checks: the endpoint is the one call that scales with the
    /// candidate count, and Dispatcharr is a single Django process.
    permits: Arc<tokio::sync::Semaphore>,
    channel_cache: Arc<dashmap::DashMap<i64, Channel>>,
}

impl std::fmt::Debug for DispatcharrClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatcharrClient")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl DispatcharrClient {
    /// Build a client from the host configuration.
    ///
    /// The API key is preferred and sent as `X-API-Key`, which never expires; otherwise the
    /// client logs in once and keeps the access token.
    pub fn new(config: &DispatcharrConfig) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(key) = config.resolved_api_key() {
            let value = reqwest::header::HeaderValue::from_str(&key)
                .context("the Dispatcharr API key is not a valid header value")?;
            headers.insert("X-API-Key", value);
        }

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs.max(1)))
            .default_headers(headers)
            .user_agent(concat!("crosstalk/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("building the HTTP client")?;

        Ok(Self {
            http,
            base: config.api_root(),
            program_cache: Arc::new(dashmap::DashMap::new()),
            permits: Arc::new(tokio::sync::Semaphore::new(config.concurrency.max(1))),
            channel_cache: Arc::new(dashmap::DashMap::new()),
        })
    }

    /// The API root every request is built from.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// A GET returning JSON, with the status checked and the body included on failure.
    async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        let url = format!("{}{}", self.base, path);
        let response = self
            .http
            .get(&url)
            .query(query)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;

        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            let body = response.text().await.unwrap_or_default();
            bail!(
                "Dispatcharr rejected the credentials for {url} ({status}). Check \
                 dispatcharr.api_key, or the username and password. Response: {}",
                truncate(&body, 400)
            );
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("GET {url} failed with {status}: {}", truncate(&body, 400));
        }
        response
            .json::<T>()
            .await
            .with_context(|| format!("GET {url} returned an unexpected body"))
    }

    /// Every channel.
    ///
    /// The API returns the whole list when no paging parameter is sent, which is what a run
    /// wants: scope filtering happens here, not on the server, because the group and profile
    /// filters accept wildcards.
    pub async fn channels(&self) -> Result<Vec<Channel>> {
        let channels: Vec<Channel> = self
            .get_json("/channels/channels/", &[])
            .await
            .context("listing channels")?;
        for channel in &channels {
            self.channel_cache.insert(channel.id, channel.clone());
        }
        Ok(channels)
    }

    /// A channel by id, from the cache the last listing filled.
    pub fn channel(&self, id: i64) -> Option<Channel> {
        self.channel_cache.get(&id).map(|entry| entry.clone())
    }

    pub async fn groups(&self) -> Result<Vec<ChannelGroup>> {
        self.get_json("/channels/groups/", &[])
            .await
            .context("listing channel groups")
    }

    pub async fn profiles(&self) -> Result<Vec<ChannelProfile>> {
        self.get_json("/channels/profiles/", &[])
            .await
            .context("listing channel profiles")
    }

    /// Channel ids enabled in a profile.
    ///
    /// Reads the profile list, which already carries the enabled channel ids; there is no
    /// separate membership endpoint to walk.
    pub async fn profile_channels(&self, profile_id: i64) -> Result<Vec<i64>> {
        let profiles = self.profiles().await?;
        Ok(profiles
            .into_iter()
            .find(|profile| profile.id == profile_id)
            .map(|profile| profile.channels)
            .unwrap_or_default())
    }

    pub async fn epg_sources(&self) -> Result<Vec<EpgSource>> {
        self.get_json("/epg/sources/", &[])
            .await
            .context("listing EPG sources")
    }

    /// Every EPG entry, walking the paginated endpoint.
    ///
    /// The entries endpoint IS paginated, and Dispatcharr's default page is small, so a
    /// single request would silently match against a fraction of the guide data.
    pub async fn epg_entries(&self) -> Result<Vec<EpgEntry>> {
        #[derive(Deserialize)]
        struct Page {
            #[serde(default)]
            results: Vec<EpgEntry>,
            #[serde(default)]
            next: Option<serde_json::Value>,
        }

        /// Dispatcharr paginates this endpoint, but a build with pagination disabled
        /// answers with a bare array; both shapes have to work, or the run would silently
        /// match against nothing.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Payload {
            Paged(Page),
            Bare(Vec<EpgEntry>),
        }

        let mut entries = Vec::new();
        let mut page = 1u32;
        loop {
            let payload: Payload = self
                .get_json(
                    "/epg/epgdata/",
                    &[
                        ("page", page.to_string()),
                        ("page_size", "1000".to_string()),
                    ],
                )
                .await
                .context("listing EPG entries")?;

            match payload {
                Payload::Bare(list) => {
                    entries.extend(list);
                    break;
                }
                Payload::Paged(page_payload) => {
                    entries.extend(page_payload.results);
                    if page_payload.next.is_none_or(|next| next.is_null()) {
                        break;
                    }
                    page += 1;
                    if page > 10_000 {
                        bail!("the EPG entry list did not terminate after 10000 pages");
                    }
                }
            }
        }
        Ok(entries)
    }

    /// Whether an EPG entry has any programme inside the window.
    ///
    /// Memoised: a run checks the same entry once per channel that considers it, and the
    /// answer cannot change during a run.
    pub async fn has_programs(
        &self,
        tvg_id: &str,
        epg_source: i64,
        window: ProgramWindow,
    ) -> Result<HasPrograms> {
        if tvg_id.is_empty() {
            // Nothing to ask about: an entry with no tvg_id cannot carry programmes keyed by
            // one, and the plugin's own query would filter on an empty string.
            return Ok(HasPrograms::No);
        }

        let key = (tvg_id.to_string(), epg_source);
        if let Some(cached) = self.program_cache.get(&key) {
            return Ok(*cached);
        }

        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("the programme-check semaphore was closed"))?;

        // Another thread may have filled the cache while this one waited for a permit.
        if let Some(cached) = self.program_cache.get(&key) {
            return Ok(*cached);
        }

        let rows: PaginatedPrograms = self
            .get_json(
                "/epg/programs/search/",
                &[
                    ("tvg_id", tvg_id.to_string()),
                    ("epg_source", epg_source.to_string()),
                    ("end_after", window.start.clone()),
                    ("start_before", window.end.clone()),
                    ("page_size", "1".to_string()),
                    ("fields", "id".to_string()),
                ],
            )
            .await
            .with_context(|| format!("checking programme data for tvg_id {tvg_id:?}"))?;

        let answer = if rows.results.is_empty() {
            HasPrograms::No
        } else {
            HasPrograms::Yes
        };
        self.program_cache.insert(key, answer);
        Ok(answer)
    }

    /// Assign EPG entries to channels.
    ///
    /// Dispatcharr queues the programme parse for every changed entry itself, which is the
    /// step the plugin had to trigger by hand through a Django signal.
    pub async fn batch_set_epg(&self, associations: &[(i64, i64)]) -> Result<usize> {
        if associations.is_empty() {
            return Ok(0);
        }

        #[derive(Serialize)]
        struct Association {
            channel_id: i64,
            epg_data_id: i64,
        }
        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            channels_updated: i64,
            #[serde(default)]
            success: bool,
        }

        let body = serde_json::json!({
            "associations": associations
                .iter()
                .map(|(channel_id, epg_data_id)| Association {
                    channel_id: *channel_id,
                    epg_data_id: *epg_data_id,
                })
                .collect::<Vec<_>>(),
        });

        let url = format!("{}/channels/channels/batch-set-epg/", self.base);
        let response = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("POST {url} failed with {status}: {}", truncate(&text, 400));
        }
        let parsed: Response = serde_json::from_str(&text).with_context(|| {
            format!(
                "POST {url} returned an unexpected body: {}",
                truncate(&text, 200)
            )
        })?;
        if !parsed.success {
            bail!("POST {url} reported failure: {}", truncate(&text, 400));
        }
        Ok(parsed.channels_updated.max(0) as usize)
    }

    /// Fetch every source of truth a run needs, once, in parallel.
    pub async fn load_world(&self) -> Result<World> {
        let (channels, groups, profiles, sources, entries) = tokio::try_join!(
            self.channels(),
            self.groups(),
            self.profiles(),
            self.epg_sources(),
            self.epg_entries(),
        )?;
        Ok(World {
            channels,
            groups,
            profiles,
            sources,
            entries,
        })
    }
}

#[derive(Deserialize)]
struct PaginatedPrograms {
    #[serde(default)]
    results: Vec<ProgramRow>,
}

/// The programme-data window: programmes overlapping `[start, end)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgramWindow {
    /// ISO 8601 instant, sent as `end_after`.
    pub start: String,
    /// ISO 8601 instant, sent as `start_before`.
    pub end: String,
}

impl ProgramWindow {
    /// A window from now to now plus `hours`.
    pub fn ahead(hours: u32) -> Self {
        let now = chrono::Utc::now();
        let end = now + chrono::Duration::hours(i64::from(hours));
        Self {
            start: now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            end: end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }
    }
}

/// Everything a run loads from Dispatcharr.
#[derive(Debug, Clone, Default)]
pub struct World {
    pub channels: Vec<Channel>,
    pub groups: Vec<ChannelGroup>,
    pub profiles: Vec<ChannelProfile>,
    pub sources: Vec<EpgSource>,
    pub entries: Vec<EpgEntry>,
}

impl World {
    /// Source id -> source.
    pub fn source_map(&self) -> HashMap<i64, &EpgSource> {
        self.sources
            .iter()
            .map(|source| (source.id, source))
            .collect()
    }

    pub fn group_name(&self, id: Option<i64>) -> String {
        match id.and_then(|id| self.groups.iter().find(|group| group.id == id)) {
            Some(group) => group.name.clone(),
            None => "No Group".to_string(),
        }
    }
}

fn truncate(text: &str, limit: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= limit {
        return trimmed.to_string();
    }
    let cut: String = trimmed.chars().take(limit).collect();
    format!("{cut}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param};
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

    #[tokio::test]
    async fn channels_are_sent_an_api_key_and_parsed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/channels/channels/"))
            .and(header("X-API-Key", "test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": 1, "name": "ESPN HD", "channel_number": 101, "channel_group_id": 3, "epg_data_id": 9},
                {"id": 2, "name": "CNN", "channel_number": "102", "epg_data_id": null}
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

    #[tokio::test]
    async fn effective_epg_id_wins_over_the_column() {
        let channel = Channel {
            id: 1,
            epg_data_id: Some(5),
            effective_epg_data_id: Some(7),
            ..Channel::default()
        };
        assert_eq!(channel.current_epg_id(), Some(7));
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
    }

    #[tokio::test]
    async fn epg_entries_walk_every_page() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/epg/epgdata/"))
            .and(query_param("page", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": 3,
                "next": "http://x/api/epg/epgdata/?page=2",
                "results": [
                    {"id": 1, "name": "ESPN", "tvg_id": "espn.us", "epg_source": 4},
                    {"id": 2, "name": "CNN", "tvg_id": "cnn.us", "epg_source": 4}
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/epg/epgdata/"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": 3,
                "next": null,
                "results": [{"id": 3, "name": "BBC One", "tvg_id": "bbc1.uk", "epg_source": 5}]
            })))
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let entries = client.epg_entries().await.expect("entries");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[2].name, "BBC One");
        assert_eq!(entries[2].epg_source, Some(5));
    }

    #[tokio::test]
    async fn epg_entries_accept_a_bare_array() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/epg/epgdata/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"id": 1, "name": "ESPN", "tvg_id": "espn.us", "epg_source": 4}
            ])))
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let entries = client.epg_entries().await.expect("entries");
        assert_eq!(entries.len(), 1);
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
    async fn programme_checks_are_memoised() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/epg/programs/search/"))
            .and(query_param("tvg_id", "espn.us"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": 1,
                "results": [{"id": 42}]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let window = ProgramWindow::ahead(12);
        assert_eq!(
            client
                .has_programs("espn.us", 4, window.clone())
                .await
                .expect("yes"),
            HasPrograms::Yes
        );
        // The second call is served from the cache: the mock allows exactly one request.
        assert_eq!(
            client
                .has_programs("espn.us", 4, window)
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
                .has_programs("empty.us", 4, ProgramWindow::ahead(12))
                .await
                .expect("no"),
            HasPrograms::No
        );
    }

    #[tokio::test]
    async fn an_entry_without_a_tvg_id_costs_no_request() {
        let server = MockServer::start().await;
        // No mock mounted: any request would fail the test with a 404.
        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        assert_eq!(
            client
                .has_programs("", 4, ProgramWindow::ahead(12))
                .await
                .expect("no"),
            HasPrograms::No
        );
    }

    #[tokio::test]
    async fn batch_set_epg_posts_associations_and_counts_updates() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/channels/channels/batch-set-epg/"))
            .and(wiremock::matchers::body_json(serde_json::json!({
                "associations": [
                    {"channel_id": 1, "epg_data_id": 10},
                    {"channel_id": 2, "epg_data_id": 11}
                ]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true,
                "channels_updated": 2,
                "programs_refreshed": 2
            })))
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let updated = client.batch_set_epg(&[(1, 10), (2, 11)]).await.expect("ok");
        assert_eq!(updated, 2);
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
        let error = client.batch_set_epg(&[(1, 10)]).await.expect_err("failure");
        assert!(format!("{error}").contains("reported failure"));
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
                {"id": 1, "name": "ESPN", "channel_group_id": 2}
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
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([{"id": 1, "name": "All"}])),
            )
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
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "next": null,
                "results": [{"id": 7, "name": "ESPN", "tvg_id": "espn.us", "epg_source": 4}]
            })))
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
}
