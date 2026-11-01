//! The Dispatcharr REST client.
//!
//! The Python plugin read Dispatcharr's database directly through the Django ORM. A separate
//! process cannot, so this is the interface it has instead. Everything the matcher needs is
//! here: channels, groups, profiles, EPG sources, EPG entries, and the one write (assign EPG
//! to channels).
//!
//! Three shapes of the API are load-bearing and easy to get wrong:
//!
//!   * the channel list is only paginated when a `page` or `page_size` parameter is present,
//!     AND it hides channels with `hidden_from_output` unless `visibility_filter=all` is sent.
//!     The plugin read `Channel.objects.all()`, so the filter has to be sent explicitly or a
//!     hidden channel would silently drop out of scope.
//!   * the EPG entry list is NOT paginated upstream; some builds answer with an envelope, so
//!     both shapes are accepted.
//!   * the programme search has no `epg_id` filter, so the programme-data check is keyed on
//!     `(tvg_id, source)`, which `EPGData.unique_together` makes unique. It is the expensive
//!     call — the only thing the plugin did in-process that costs a round trip here — so it is
//!     memoised per window and gated by a semaphore.

use crate::config::DispatcharrConfig;
use anyhow::{bail, Context, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Attempts per request, for the statuses and failures that are worth retrying.
const MAX_ATTEMPTS: u32 = 3;
/// Base backoff; doubled per attempt.
const RETRY_BASE: Duration = Duration::from_millis(250);
/// Associations per `batch-set-epg` request. One request for a whole install would sit
/// under a single timeout; chunks keep each write small enough to retry on its own.
const BATCH_SET_EPG_CHUNK_SIZE: usize = 500;

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
    /// The EPG entry assigned on the channel's own column.
    #[serde(default)]
    pub epg_data_id: Option<i64>,
    /// An override can pin a different EPG without changing the column. Newer builds only.
    #[serde(default)]
    pub effective_epg_data_id: Option<i64>,
}

impl Channel {
    /// The EPG entry this channel is on, by its own column.
    ///
    /// The raw column is what the plugin compares against and what `batch-set-epg` writes, so
    /// it is what a run must reason about. Reporting the override here would make a preview
    /// claim a channel is on one guide while the write changes another.
    pub fn current_epg_id(&self) -> Option<i64> {
        self.epg_data_id
    }

    /// Whether an override is pinning a different EPG. Such a channel is skipped and
    /// reported, because writing its column would change nothing the operator can see.
    pub fn has_epg_override(&self) -> bool {
        match (self.epg_data_id, self.effective_epg_data_id) {
            (Some(raw), Some(effective)) => raw != effective,
            (None, Some(_)) => true,
            _ => false,
        }
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

/// An EPG entry: a channel inside a guide source.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct EpgEntry {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    /// `EPGData.tvg_id` is nullable upstream, and a single null would otherwise fail the
    /// whole listing.
    #[serde(default, deserialize_with = "null_to_empty")]
    pub tvg_id: String,
    #[serde(default)]
    pub epg_source: Option<i64>,
}

fn null_to_empty<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

/// A programme row. The check only needs to know whether one exists, so the fields are not
/// read; `id` is named so a renamed column cannot turn a real row into a parse error.
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

/// Where the credentials come from.
#[derive(Debug, Clone, Default)]
struct Credentials {
    api_key: Option<String>,
    username: String,
    password: Option<String>,
    /// The JWT, when logging in rather than using an API key.
    token: Arc<tokio::sync::RwLock<Option<String>>>,
    refresh: Arc<tokio::sync::RwLock<Option<String>>>,
}

impl Credentials {
    fn uses_api_key(&self) -> bool {
        self.api_key.is_some()
    }

    fn can_log_in(&self) -> bool {
        !self.username.trim().is_empty() && self.password.is_some()
    }
}

/// The client.
///
/// Cheap to clone: the HTTP connection pool and the token are shared, so a run's threads can
/// each hold a handle without duplicating sockets or logging in again.
#[derive(Clone)]
pub struct DispatcharrClient {
    http: reqwest::Client,
    base: String,
    credentials: Arc<Credentials>,
    /// Memoised programme checks, keyed by `(tvg_id, source, window start, window end)`.
    program_cache: Arc<dashmap::DashMap<(String, i64, String, String), HasPrograms>>,
    /// Caps concurrent programme checks: that endpoint is the one call that scales with the
    /// candidate count, and Dispatcharr is a single Django process.
    permits: Arc<tokio::sync::Semaphore>,
    channel_cache: Arc<dashmap::DashMap<i64, Channel>>,
}

impl std::fmt::Debug for DispatcharrClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatcharrClient")
            .field("base", &self.base)
            .field(
                "auth",
                &if self.credentials.uses_api_key() {
                    "api-key"
                } else {
                    "jwt"
                },
            )
            .finish_non_exhaustive()
    }
}

impl DispatcharrClient {
    /// Build a client from the host configuration.
    ///
    /// An API key is used when configured, because it does not expire. Otherwise the client
    /// logs in with the username and password and refreshes the token when it lapses.
    pub fn new(config: &DispatcharrConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            // Per-request timeout. The bulk listings run inside it: a full channel list on a
            // large instance is a few megabytes, not a hang.
            .timeout(Duration::from_secs(config.timeout_secs.max(1)))
            .user_agent(concat!("crosstalk/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("building the HTTP client")?;

        let api_key = config.resolved_api_key();
        if let Some(key) = api_key.as_deref() {
            // Fail here rather than on the first request, with a message about the key.
            reqwest::header::HeaderValue::from_str(key)
                .context("the Dispatcharr API key is not a valid header value")?;
        }

        Ok(Self {
            http,
            base: config.api_root(),
            credentials: Arc::new(Credentials {
                api_key,
                username: config.username.trim().to_string(),
                password: config.resolved_password(),
                token: Arc::new(tokio::sync::RwLock::new(None)),
                refresh: Arc::new(tokio::sync::RwLock::new(None)),
            }),
            program_cache: Arc::new(dashmap::DashMap::new()),
            permits: Arc::new(tokio::sync::Semaphore::new(config.concurrency.max(1))),
            channel_cache: Arc::new(dashmap::DashMap::new()),
        })
    }

    /// The API root every request is built from.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Obtain a token up front when logging in, so a bad password fails immediately rather
    /// than on the ninth candidate.
    pub async fn authenticate(&self) -> Result<()> {
        if self.credentials.uses_api_key() {
            return Ok(());
        }
        if !self.credentials.can_log_in() {
            bail!(
                "no Dispatcharr credentials: set dispatcharr.api_key, or dispatcharr.username \
                 and dispatcharr.password"
            );
        }
        self.log_in().await.map(|_| ())
    }

    /// POST `/accounts/token/` and keep the access and refresh tokens.
    async fn log_in(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Body<'a> {
            username: &'a str,
            password: &'a str,
        }
        #[derive(Deserialize)]
        struct Tokens {
            access: String,
            #[serde(default)]
            refresh: Option<String>,
        }

        let password = self
            .credentials
            .password
            .clone()
            .context("no password configured")?;
        let url = format!("{}/accounts/token/", self.base);
        let response = self
            .http
            .post(&url)
            .json(&Body {
                username: &self.credentials.username,
                password: &password,
            })
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "Dispatcharr login failed for user {:?} ({status}). Check \
                 dispatcharr.username and dispatcharr.password. Response: {}",
                self.credentials.username,
                truncate(&body, 300)
            );
        }
        let tokens: Tokens = serde_json::from_str(&body)
            .with_context(|| format!("POST {url} returned an unexpected body"))?;
        *self.credentials.token.write().await = Some(tokens.access.clone());
        if tokens.refresh.is_some() {
            *self.credentials.refresh.write().await = tokens.refresh;
        }
        Ok(tokens.access)
    }

    /// Trade the refresh token for a new access token.
    async fn refresh_token(&self) -> Result<String> {
        #[derive(Deserialize)]
        struct Tokens {
            access: String,
        }

        let refresh = self
            .credentials
            .refresh
            .read()
            .await
            .clone()
            .context("no refresh token")?;
        let url = format!("{}/accounts/token/refresh/", self.base);
        let response = self
            .http
            .post(&url)
            .json(&serde_json::json!({ "refresh": refresh }))
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            // A refresh token can also lapse, and the fallback is a fresh login.
            return self.log_in().await;
        }
        let tokens: Tokens = serde_json::from_str(&body)
            .with_context(|| format!("POST {url} returned an unexpected body"))?;
        *self.credentials.token.write().await = Some(tokens.access.clone());
        Ok(tokens.access)
    }

    /// The authorization header to send, logging in first if nothing is available yet.
    async fn authorization(&self) -> Result<Option<String>> {
        if let Some(key) = self.credentials.api_key.as_deref() {
            return Ok(Some(format!("ApiKey {key}")));
        }
        if let Some(token) = self.credentials.token.read().await.clone() {
            return Ok(Some(format!("Bearer {token}")));
        }
        Ok(Some(format!("Bearer {}", self.log_in().await?)))
    }

    /// Send a GET, retrying the failures worth retrying and re-authenticating once on 401.
    async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        let url = format!("{}{}", self.base, path);
        let mut refreshed = false;
        let mut attempt = 0;

        loop {
            attempt += 1;
            let authorization = self.authorization().await?;
            let mut request = self.http.get(&url).query(query);
            if let Some(value) = authorization.as_deref() {
                request = request.header(reqwest::header::AUTHORIZATION, value);
                // The API key goes in its own header; `ApiKey`/`Bearer` in Authorization is
                // the documented alternative and is what the server accepts for both.
            }

            let outcome = request.send().await;
            match outcome {
                Ok(response) => {
                    let status = response.status();

                    if status == reqwest::StatusCode::UNAUTHORIZED && !refreshed {
                        refreshed = true;
                        if !self.credentials.uses_api_key() {
                            self.refresh_token().await?;
                        }
                        continue;
                    }

                    let retryable = status.is_server_error()
                        || status == reqwest::StatusCode::TOO_MANY_REQUESTS;
                    if retryable && attempt < MAX_ATTEMPTS {
                        // `Retry-After` when the server states one, otherwise exponential
                        // backoff from the attempt number.
                        let retry_after = response
                            .headers()
                            .get(reqwest::header::RETRY_AFTER)
                            .and_then(|value| value.to_str().ok())
                            .and_then(|value| value.parse::<u64>().ok())
                            .map(Duration::from_secs)
                            .unwrap_or_else(|| RETRY_BASE * 2u32.pow(attempt - 1));
                        tokio::time::sleep(retry_after).await;
                        continue;
                    }

                    if status == reqwest::StatusCode::UNAUTHORIZED
                        || status == reqwest::StatusCode::FORBIDDEN
                    {
                        let body = response.text().await.unwrap_or_default();
                        bail!(
                            "Dispatcharr rejected the credentials for {url} ({status}). Check \
                             dispatcharr.api_key, or the username and password, and that the \
                             account is an administrator. Response: {}",
                            truncate(&body, 300)
                        );
                    }
                    if !status.is_success() {
                        let body = response.text().await.unwrap_or_default();
                        bail!("GET {url} failed with {status}: {}", truncate(&body, 300));
                    }
                    return response
                        .json::<T>()
                        .await
                        .with_context(|| format!("GET {url} returned an unexpected body"));
                }
                Err(error) => {
                    // A connection error is usually a restarting container: worth retrying.
                    if attempt < MAX_ATTEMPTS && (error.is_connect() || error.is_timeout()) {
                        tokio::time::sleep(RETRY_BASE * 2u32.pow(attempt - 1)).await;
                        continue;
                    }
                    return Err(error).with_context(|| format!("GET {url}"));
                }
            }
        }
    }

    /// Every channel, hidden ones included.
    ///
    /// `visibility_filter=all` is not optional: the list endpoint hides channels whose
    /// `hidden_from_output` is set unless it is sent, and the plugin read every channel.
    pub async fn channels(&self) -> Result<Vec<Channel>> {
        let channels: Vec<Channel> = self
            .get_json(
                "/channels/channels/",
                &[("visibility_filter", "all".to_string())],
            )
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

    /// Every EPG entry.
    ///
    /// Upstream this endpoint is not paginated, so a bare array is the normal answer. Some
    /// builds put it behind a page envelope, so both shapes are accepted; a page parameter is
    /// sent because a build that does paginate would otherwise silently return only the first
    /// page.
    pub async fn epg_entries(&self) -> Result<Vec<EpgEntry>> {
        #[derive(Deserialize)]
        struct Page {
            #[serde(default)]
            results: Vec<EpgEntry>,
            #[serde(default)]
            next: Option<serde_json::Value>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Payload {
            Bare(Vec<EpgEntry>),
            Paged(Page),
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
                Payload::Paged(envelope) => {
                    entries.extend(envelope.results);
                    if envelope.next.is_none_or(|next| next.is_null()) {
                        break;
                    }
                    page += 1;
                    if page > 1_000 {
                        bail!("the EPG entry list did not terminate after 1000 pages");
                    }
                }
            }
        }
        Ok(entries)
    }

    /// The signed-in user's level, `10` being administrator.
    ///
    /// Worth asking: with a non-administrator key, the programme search only returns
    /// programmes for guide data already mapped to a channel, so every candidate for a channel
    /// with no guide would come back empty and a preview would look like it found nothing.
    pub async fn user_level(&self) -> Result<i64> {
        #[derive(Deserialize)]
        struct Me {
            #[serde(default)]
            user_level: i64,
        }
        let me: Me = self
            .get_json("/accounts/users/me/", &[])
            .await
            .context("reading the current user")?;
        Ok(me.user_level)
    }

    /// Whether an EPG entry has any programme inside the window.
    ///
    /// Memoised on the entry AND the window: a run asks the same question many times, but a
    /// later run with a different window must not inherit an old answer.
    pub async fn has_programs(
        &self,
        tvg_id: &str,
        epg_source: i64,
        window: &ProgramWindow,
    ) -> Result<HasPrograms> {
        if tvg_id.is_empty() {
            // Nothing to ask about: programmes are keyed by tvg_id, and the plugin's own
            // query would filter on an empty string.
            return Ok(HasPrograms::No);
        }

        let key = (
            tvg_id.to_string(),
            epg_source,
            window.start.clone(),
            window.end.clone(),
        );
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
    /// step the plugin had to trigger by hand through a Django signal. `channels_updated` can
    /// legitimately be smaller than the number of associations: a channel already on that
    /// entry is skipped upstream, and that is not an error.
    ///
    /// Associations are sent in chunks of at most [`BATCH_SET_EPG_CHUNK_SIZE`]. Each chunk
    /// gets the same retry/refresh treatment as the GET path (retry 5xx/429 honouring
    /// `Retry-After` with exponential backoff, one JWT refresh on 401), and the returned
    /// counts are summed. A chunk failure names the chunk and how many associations were
    /// already confirmed, so the caller can report partial progress.
    pub async fn batch_set_epg(&self, associations: &[(i64, i64)]) -> Result<usize> {
        if associations.is_empty() {
            // Upstream rejects an empty list with 400; a run with nothing to write is a
            // no-op, not a failure.
            return Ok(0);
        }

        let total_chunks = associations.len().div_ceil(BATCH_SET_EPG_CHUNK_SIZE);
        let mut confirmed: usize = 0;
        let mut updated: usize = 0;
        for (index, chunk) in associations.chunks(BATCH_SET_EPG_CHUNK_SIZE).enumerate() {
            let chunk_no = index + 1;
            match self.post_epg_chunk(chunk).await {
                Ok(count) => {
                    confirmed += chunk.len();
                    updated += count;
                }
                Err(error) => {
                    anyhow::bail!(
                        "chunk {chunk_no}/{total_chunks} failed after {confirmed} associations were confirmed: {error:#}"
                    );
                }
            }
        }
        Ok(updated)
    }

    /// POST one chunk of associations, with retry/refresh mirroring [`Self::get_json`].
    async fn post_epg_chunk(&self, chunk: &[(i64, i64)]) -> Result<usize> {
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
            "associations": chunk
                .iter()
                .map(|(channel_id, epg_data_id)| Association {
                    channel_id: *channel_id,
                    epg_data_id: *epg_data_id,
                })
                .collect::<Vec<_>>(),
        });

        let url = format!("{}/channels/channels/batch-set-epg/", self.base);
        let mut refreshed = false;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let authorization = self.authorization().await?;
            let mut request = self.http.post(&url).json(&body);
            if let Some(value) = authorization.as_deref() {
                request = request.header(reqwest::header::AUTHORIZATION, value);
            }
            let outcome = request.send().await;
            match outcome {
                Ok(response) => {
                    let status = response.status();

                    if status == reqwest::StatusCode::UNAUTHORIZED && !refreshed {
                        refreshed = true;
                        if !self.credentials.uses_api_key() {
                            self.refresh_token().await?;
                        }
                        continue;
                    }

                    let retryable = status.is_server_error()
                        || status == reqwest::StatusCode::TOO_MANY_REQUESTS;
                    if retryable && attempt < MAX_ATTEMPTS {
                        let retry_after = response
                            .headers()
                            .get(reqwest::header::RETRY_AFTER)
                            .and_then(|value| value.to_str().ok())
                            .and_then(|value| value.parse::<u64>().ok())
                            .map(Duration::from_secs)
                            .unwrap_or_else(|| RETRY_BASE * 2u32.pow(attempt - 1));
                        tokio::time::sleep(retry_after).await;
                        continue;
                    }

                    if status == reqwest::StatusCode::UNAUTHORIZED
                        || status == reqwest::StatusCode::FORBIDDEN
                    {
                        let body = response.text().await.unwrap_or_default();
                        anyhow::bail!(
                            "Dispatcharr rejected the credentials for POST {url} ({status}). Check \
                             dispatcharr.api_key, or the username and password, and that the \
                             account is an administrator. Response: {}",
                            truncate(&body, 300)
                        );
                    }
                    if !status.is_success() {
                        let body = response.text().await.unwrap_or_default();
                        anyhow::bail!("POST {url} failed with {status}: {}", truncate(&body, 300));
                    }
                    let text = response.text().await.unwrap_or_default();
                    let parsed: Response = serde_json::from_str(&text).with_context(|| {
                        format!(
                            "POST {url} returned an unexpected body: {}",
                            truncate(&text, 200)
                        )
                    })?;
                    if !parsed.success {
                        anyhow::bail!("POST {url} reported failure: {}", truncate(&text, 300));
                    }
                    return Ok(parsed.channels_updated.max(0) as usize);
                }
                Err(error) => {
                    if attempt < MAX_ATTEMPTS && (error.is_connect() || error.is_timeout()) {
                        tokio::time::sleep(RETRY_BASE * 2u32.pow(attempt - 1)).await;
                        continue;
                    }
                    return Err(error).with_context(|| format!("POST {url}"));
                }
            }
        }
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
    /// ISO 8601 instant, sent as `end_after` (`end_time >= start`).
    pub start: String,
    /// ISO 8601 instant, sent as `start_before` (`start_time <= end`).
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
    use wiremock::matchers::{body_json, header, method, path, query_param};
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

    #[tokio::test]
    async fn channels_ask_for_hidden_ones_too() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/channels/channels/"))
            .and(query_param("visibility_filter", "all"))
            .and(header("Authorization", "ApiKey test-key"))
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
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([{"id": 1, "name": "SD"}])),
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
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([{"id": 1, "name": "SD"}])),
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
            client.batch_set_epg(&[(1, 10), (2, 11)]).await.expect("ok"),
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
        let error = client.batch_set_epg(&[(1, 10)]).await.expect_err("failure");
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
        let associations: Vec<(i64, i64)> = (1..=1200).map(|id| (id, id + 10_000)).collect();
        assert_eq!(client.batch_set_epg(&associations).await.expect("ok"), 21);

        let requests = server.received_requests().await.expect("requests");
        assert_eq!(requests.len(), 3);
        let mut sizes = requests
            .iter()
            .map(|request| {
                let body: serde_json::Value =
                    serde_json::from_slice(&request.body).expect("json body");
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
            client.batch_set_epg(&[(1, 10), (2, 11)]).await.expect("ok"),
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
        assert_eq!(client.batch_set_epg(&[(1, 10)]).await.expect("ok"), 1);
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
        let associations: Vec<(i64, i64)> = (1..=600).map(|id| (id, id + 1)).collect();
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
