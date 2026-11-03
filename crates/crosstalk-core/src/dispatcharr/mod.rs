//! The Dispatcharr REST client.
//!
//! The Python plugins read Dispatcharr's database directly through the Django ORM. A separate
//! process cannot, so this is the interface it has instead: the reads a run needs (channels,
//! groups, profiles, streams, M3U accounts, logos, EPG sources and entries) and the writes the
//! sub-apps make. Every endpoint here is checked against the DRF view that serves it, and the
//! ones with surprising shapes are called out below.
//!
//! Load-bearing shapes of the API, each of which has cost a wrong assumption:
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
//!   * `edit/bulk` takes a bare JSON array, not an object, and answers `{message, channels}`;
//!     a rejected row comes back as 400 with the per-channel `errors` the serializer raised.
//!   * `edit/bulk-regex` rewrites names server-side, so it is NOT idempotent: it is sent once
//!     and a lost response is an error, because a retry would apply the pattern twice.
//!     `from-stream/bulk` (202 with a task id, a message and a stream count) and channel
//!     create are in the same class (a retry duplicates rows).
//!   * the EPG import endpoint only queues a Celery task and answers 202, so a refresh is
//!     "requested", never "done"; [`DispatcharrClient::await_epg_refresh`] polls the source's
//!     own `status`/`updated_at` until it moves.
//!
//! Reads need a standard user; every write here is behind `IsAdmin` upstream, so a run that
//! writes needs an administrator's key.

mod channels;
mod epg;
mod plugins;
mod types;

pub use types::*;

use crate::config::DispatcharrConfig;
use anyhow::{bail, Context, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
mod tests;

/// Attempts per request, for the statuses and failures that are worth retrying.
const MAX_ATTEMPTS: u32 = 3;
/// Base backoff; doubled per attempt.
const RETRY_BASE: Duration = Duration::from_millis(250);
/// Upper bound on a single retry wait. A server-sent `Retry-After` of minutes or hours
/// would otherwise stall a run for that long; the cap keeps a retry a delay, not a hang.
/// The request is still retried, just sooner than the server asked.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

/// How long to wait before the next attempt: the server's `Retry-After` when it states
/// one, otherwise exponential backoff from the attempt number. Either way capped at
/// [`MAX_RETRY_AFTER`], so a wayward header cannot stall a run.
fn retry_delay(headers: &reqwest::header::HeaderMap, attempt: u32) -> Duration {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| RETRY_BASE * 2u32.pow(attempt.saturating_sub(1)))
        .min(MAX_RETRY_AFTER)
}

/// Whether a write may be re-sent after a status or transport failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retry {
    /// Safe to re-send: the endpoint changes nothing when it is applied twice.
    Idempotent,
    /// Exactly once. For creates and transformations whose second application would duplicate
    /// work (a channel row, a queued task, a regex rewrite) a lost response has to surface as
    /// an error rather than silently repeat.
    Once,
}

impl Retry {
    /// Whether this policy allows another attempt after `attempt` tries.
    fn allows(self, attempt: u32) -> bool {
        self == Self::Idempotent && attempt < MAX_ATTEMPTS
    }
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
                        tokio::time::sleep(retry_delay(response.headers(), attempt)).await;
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

    /// A write, with the same re-auth and retry treatment as [`Self::get_json`].
    ///
    /// Returns the response body unparsed: the endpoints differ (a full object, a
    /// `{"success": …}` receipt, or no body at all for a delete), so each caller decides.
    /// `retry` is the caller's statement about whether applying the write twice is a no-op.
    async fn send_write(
        &self,
        method: &reqwest::Method,
        path: &str,
        body: Option<&serde_json::Value>,
        retry: Retry,
    ) -> Result<String> {
        let url = format!("{}{}", self.base, path);
        let mut refreshed = false;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let authorization = self.authorization().await?;
            let mut request = self.http.request(method.clone(), &url);
            if let Some(payload) = body {
                request = request.json(payload);
            }
            if let Some(value) = authorization.as_deref() {
                request = request.header(reqwest::header::AUTHORIZATION, value);
            }

            match request.send().await {
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
                    if retryable && retry.allows(attempt) {
                        tokio::time::sleep(retry_delay(response.headers(), attempt)).await;
                        continue;
                    }

                    if status == reqwest::StatusCode::UNAUTHORIZED
                        || status == reqwest::StatusCode::FORBIDDEN
                    {
                        let body = response.text().await.unwrap_or_default();
                        bail!(
                            "Dispatcharr rejected the credentials for {method} {url} ({status}). \
                             Check dispatcharr.api_key, or the username and password, and that \
                             the account is an administrator. Response: {}",
                            truncate(&body, 300)
                        );
                    }
                    if !status.is_success() {
                        let body = response.text().await.unwrap_or_default();
                        bail!(
                            "{method} {url} failed with {status}: {}",
                            truncate(&body, 300)
                        );
                    }
                    return response
                        .text()
                        .await
                        .with_context(|| format!("{method} {url}: reading the response body"));
                }
                Err(error) => {
                    if retry.allows(attempt) && (error.is_connect() || error.is_timeout()) {
                        tokio::time::sleep(RETRY_BASE * 2u32.pow(attempt - 1)).await;
                        continue;
                    }
                    return Err(error).with_context(|| format!("{method} {url}"));
                }
            }
        }
    }

    /// [`Self::send_write`] with the response parsed.
    async fn write_json<T: DeserializeOwned>(
        &self,
        method: &reqwest::Method,
        path: &str,
        body: Option<&serde_json::Value>,
        retry: Retry,
    ) -> Result<T> {
        let text = self.send_write(method, path, body, retry).await?;
        serde_json::from_str(&text).with_context(|| {
            format!(
                "{method} {path} returned an unexpected body: {}",
                truncate(&text, 200)
            )
        })
    }

    /// A write whose response body is not read at all (a 204 delete).
    ///
    /// The body is still drained, so the connection goes back to the pool.
    async fn write_ignored(
        &self,
        method: &reqwest::Method,
        path: &str,
        retry: Retry,
    ) -> Result<()> {
        self.send_write(method, path, None, retry).await.map(|_| ())
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

    /// A list endpoint that answers either a bare array or a page envelope.
    ///
    /// Like [`Self::epg_entries`]: most builds answer these listings with a bare array,
    /// but some put them behind a `{"results": [...]}` envelope. When the envelope
    /// carries a non-null `next`, further pages are followed with `page`/`page_size`
    /// parameters (capped, like [`Self::epg_entries`]) instead of silently returning only
    /// the first page. `results` is required: a JSON object without it is an error, not
    /// an empty list.
    async fn get_list<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
        what: &str,
    ) -> Result<Vec<T>> {
        #[derive(Deserialize)]
        struct Envelope<T> {
            results: Vec<T>,
            #[serde(default)]
            next: Option<serde_json::Value>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Payload<T> {
            Bare(Vec<T>),
            Paged(Envelope<T>),
        }

        let mut items = Vec::new();
        let mut page = 1u32;
        loop {
            let mut paged_query: Vec<(&str, String)> = query.to_vec();
            paged_query.push(("page", page.to_string()));
            paged_query.push(("page_size", "1000".to_string()));
            let payload: Payload<T> =
                self.get_json(path, &paged_query).await.with_context(|| {
                    format!(
                        "{}: expected a JSON array or a {{\"results\": [...]}} envelope",
                        what
                    )
                })?;
            match payload {
                Payload::Bare(list) => {
                    items.extend(list);
                    break;
                }
                Payload::Paged(envelope) => {
                    items.extend(envelope.results);
                    if envelope.next.is_none_or(|next| next.is_null()) {
                        break;
                    }
                    page += 1;
                    if page > 1_000 {
                        bail!("{}: the list did not terminate after 1000 pages", what);
                    }
                }
            }
        }
        Ok(items)
    }

    /// Fetch every source of truth a run needs, once, in parallel.
    ///
    /// This is [`Self::load_world_with`] for [`WorldKinds::DEFAULT`], the five slices every
    /// guide run reads. Streams, M3U accounts and logos are left empty (and reported as not
    /// loaded) because fetching them would add two large listings to every guide run.
    pub async fn load_world(&self) -> Result<World> {
        self.load_world_with(WorldKinds::DEFAULT).await
    }

    /// Fetch only the slices a run asked for, once, in parallel.
    ///
    /// A slice that is not asked for costs no request: an `async fn` future does nothing
    /// until it is awaited, so the unrequested ones are dropped unbaked. Which slices were
    /// loaded is recorded on the [`World`], so [`World::require`] can tell a caller that its
    /// group names were never fetched instead of reporting every group as missing.
    pub async fn load_world_with(&self, kinds: WorldKinds) -> Result<World> {
        let (channels, groups, profiles, sources, entries, streams, m3u_accounts, logos) = tokio::join!(
            fetch_if(kinds.contains(WorldKind::Channels), self.channels()),
            fetch_if(kinds.contains(WorldKind::Groups), self.groups()),
            fetch_if(kinds.contains(WorldKind::Profiles), self.profiles()),
            fetch_if(kinds.contains(WorldKind::EpgSources), self.epg_sources()),
            fetch_if(kinds.contains(WorldKind::EpgEntries), self.epg_entries()),
            fetch_if(kinds.contains(WorldKind::Streams), self.streams()),
            fetch_if(kinds.contains(WorldKind::M3uAccounts), self.m3u_accounts()),
            fetch_if(kinds.contains(WorldKind::Logos), self.logos()),
        );
        Ok(World {
            channels: channels?,
            groups: groups?,
            profiles: profiles?,
            sources: sources?,
            entries: entries?,
            streams: streams?,
            m3u_accounts: m3u_accounts?,
            logos: logos?,
            loaded: kinds,
        })
    }
}

/// Await `future` when `wanted`, and otherwise answer an empty list without polling it.
///
/// Not polling is the point: an unfetched world slice must cost no request.
async fn fetch_if<T>(
    wanted: bool,
    future: impl std::future::Future<Output = Result<Vec<T>>>,
) -> Result<Vec<T>> {
    if wanted {
        future.await
    } else {
        Ok(Vec::new())
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
