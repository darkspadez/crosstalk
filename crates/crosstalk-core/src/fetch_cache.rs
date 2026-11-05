//! A TTL file cache for unauthenticated HTTP indexes (`core::fetch_cache`).
//!
//! Phase 3 step 7: the tv-logos index is fetched over the network ONCE per day
//! (one recursive git-tree request) and read from disk between refreshes. The
//! cache lives in the data dir, keys bodies by URL hash, and stores the ETag
//! beside each body:
//!
//! * fresh (younger than [`FetchCache::ttl`]) → served from disk, no network;
//! * stale with an ETag → conditional `If-None-Match`; a `304` refreshes the
//!   timestamp without rewriting the body;
//! * network failure with a stale body on disk → the stale body is served WITH
//!   a warning ([`Fetched::stale`] is true and a `tracing::warn!` names the
//!   URL), because a lineup run during an outage beats no lineup run;
//! * network failure with no body → the error propagates.
//!
//! The cache never authenticates (no tokens, no basic auth — the tv-logos tree
//! is public), always sets `User-Agent` (`crosstalk/<version>`), and refuses to
//! store bodies over [`FetchCache::size_cap`] (the live body is still
//! returned, just not cached, with a warning). Bodies are `Vec<u8>`; the JSON
//! parsing is the caller's business.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// How long a cached tv-logos tree is served without revalidating (24 h).
pub const TV_LOGOS_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Above this a body is served but never stored (32 MiB). The recursive
/// tv-logos tree measured 3.4 MiB / 10,966 entries on 2026-11-05, and GitHub
/// truncates a tree response near 7 MB, so the cap leaves room for growth
/// without ever refusing a legitimate tree.
pub const DEFAULT_SIZE_CAP: usize = 32 * 1024 * 1024;

/// What a fetch returned.
#[derive(Debug, Clone)]
pub struct Fetched {
    pub body: Vec<u8>,
    /// True when the body came from a stale cache entry because the network
    /// failed (a warning was already logged naming the URL).
    pub stale: bool,
    /// True when no request left the process (fresh disk entry or 304 path
    /// that only revalidated — the 304 still sends a request; this is the
    /// fresh-disk case).
    pub from_cache: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Meta {
    url: String,
    etag: Option<String>,
    fetched_at_secs: u64,
    /// The stored body's length: a body/meta pair from two different writes
    /// (a crash between the two renames, a concurrent writer) is ignored.
    #[serde(default)]
    body_len: Option<u64>,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Write a file atomically: a unique temp file in the same directory, then a
/// rename over the target. A reader sees the old file or the new one, never
/// half of one.
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = path.with_file_name(format!(".{file_name}.{}.{seq}.tmp", std::process::id()));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

/// A TTL file cache for one family of URLs.
#[derive(Debug, Clone)]
pub struct FetchCache {
    dir: PathBuf,
    ttl: Duration,
    size_cap: usize,
    user_agent: String,
    client: reqwest::Client,
}

impl FetchCache {
    /// A cache rooted at `dir` (created on first store), with a TTL and cap.
    pub fn new(dir: PathBuf, ttl: Duration, size_cap: usize) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(format!("{}/{}", crate::HOST_NAME, crate::HOST_VERSION))
            .timeout(Duration::from_secs(30))
            .build()
            .context("building the fetch cache HTTP client")?;
        Ok(Self {
            dir,
            ttl,
            size_cap,
            user_agent: format!("{}/{}", crate::HOST_NAME, crate::HOST_VERSION),
            client,
        })
    }

    /// The tv-logos cache: `data_dir/fetch_cache`, 24 h TTL, default cap.
    pub fn tv_logos(data_dir: &Path) -> Result<Self> {
        Self::new(data_dir.join("fetch_cache"), TV_LOGOS_TTL, DEFAULT_SIZE_CAP)
    }

    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    fn slot(&self, url: &str) -> (PathBuf, PathBuf) {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in url.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let stem = format!("{hash:016x}");
        (
            self.dir.join(format!("{stem}.body")),
            self.dir.join(format!("{stem}.meta.json")),
        )
    }

    fn read_entry(&self, url: &str) -> Option<(Meta, Vec<u8>)> {
        let (body_path, meta_path) = self.slot(url);
        let meta_raw = std::fs::read(meta_path).ok()?;
        let meta: Meta = serde_json::from_slice(&meta_raw).ok()?;
        if meta.url != url {
            return None;
        }
        let body = std::fs::read(body_path).ok()?;
        if meta.body_len.is_some_and(|len| len != body.len() as u64) {
            return None;
        }
        Some((meta, body))
    }

    fn age(&self, meta: &Meta) -> Option<Duration> {
        let fetched = SystemTime::UNIX_EPOCH + Duration::from_secs(meta.fetched_at_secs);
        SystemTime::now().duration_since(fetched).ok()
    }

    fn store(&self, url: &str, etag: Option<String>, body: &[u8]) -> Result<()> {
        if body.len() > self.size_cap {
            tracing::warn!(
                url = url,
                bytes = body.len(),
                cap = self.size_cap,
                "fetch cache body over the size cap: served, not stored"
            );
            return Ok(());
        }
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("creating fetch cache dir {}", self.dir.display()))?;
        let (body_path, meta_path) = self.slot(url);
        let meta = Meta {
            url: url.to_string(),
            etag,
            fetched_at_secs: now_secs(),
            body_len: Some(body.len() as u64),
        };
        // Body first, meta last: the meta's `body_len` vouches for the body, so
        // an interrupted pair reads as a miss rather than a wrong body.
        write_atomic(&body_path, body)
            .with_context(|| format!("writing fetch cache body for {url}"))?;
        write_atomic(
            &meta_path,
            &serde_json::to_vec(&meta).expect("meta serializes"),
        )
        .with_context(|| format!("writing fetch cache meta for {url}"))?;
        Ok(())
    }

    /// Fetch `url`, through the cache.
    ///
    /// No auth is ever attached: the client carries no credentials by
    /// construction, and this method takes no token parameter on purpose.
    pub async fn fetch(&self, url: &str) -> Result<Fetched> {
        let cached = self.read_entry(url);
        if let Some((meta, body)) = &cached {
            if self.age(meta).is_some_and(|age| age < self.ttl) {
                return Ok(Fetched {
                    body: body.clone(),
                    stale: false,
                    from_cache: true,
                });
            }
        }
        let mut request = self.client.get(url);
        if let Some((meta, _)) = &cached {
            if let Some(etag) = meta.etag.as_deref() {
                request = request.header(reqwest::header::IF_NONE_MATCH, etag);
            }
        }
        match request.send().await {
            Ok(response) => {
                if response.status() == reqwest::StatusCode::NOT_MODIFIED {
                    // A 304 we did not ask for (no cached entry, so no
                    // If-None-Match: a misbehaving proxy) carries no body to serve.
                    let Some((mut meta, body)) = cached else {
                        anyhow::bail!("fetching {url} returned 304 without a cached copy");
                    };
                    meta.fetched_at_secs = now_secs();
                    let (_, meta_path) = self.slot(url);
                    let _ = write_atomic(
                        &meta_path,
                        &serde_json::to_vec(&meta).expect("meta serializes"),
                    );
                    return Ok(Fetched {
                        body,
                        stale: false,
                        from_cache: false,
                    });
                }
                let status = response.status();
                // A non-success status is a failure like a network error: with a
                // cached body the run continues on stale data (warned), without
                // one the status is the error.
                if !status.is_success() {
                    match cached {
                        Some((_, body)) => {
                            tracing::warn!(
                                url = url,
                                status = status.as_u16(),
                                "index fetch failed; serving the stale cached body"
                            );
                            return Ok(Fetched {
                                body,
                                stale: true,
                                from_cache: true,
                            });
                        }
                        None => anyhow::bail!("fetching {url} failed with HTTP {status}"),
                    }
                }
                let etag = response
                    .headers()
                    .get(reqwest::header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let body = response
                    .bytes()
                    .await
                    .with_context(|| format!("reading fetch cache body for {url}"))?
                    .to_vec();
                self.store(url, etag, &body)?;
                Ok(Fetched {
                    body,
                    stale: false,
                    from_cache: false,
                })
            }
            Err(error) => match cached {
                Some((_, body)) => {
                    tracing::warn!(
                        url = url,
                        error = format!("{error:#}"),
                        "network fetch failed; serving the stale cached body"
                    );
                    Ok(Fetched {
                        body,
                        stale: true,
                        from_cache: true,
                    })
                }
                None => Err(anyhow::anyhow!("fetching {url} failed: {error:#}")),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn cache_in(dir: &Path) -> FetchCache {
        FetchCache::new(dir.to_path_buf(), Duration::from_secs(60), 1024 * 1024).expect("cache")
    }

    #[tokio::test]
    async fn a_fresh_entry_serves_from_disk_without_network() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache_in(dir.path());
        let server = MockServer::start().await;
        let url = format!("{}/tree", server.uri());
        Mock::given(method("GET"))
            .and(path("/tree"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"tree\":[]}"))
            .expect(1)
            .mount(&server)
            .await;
        let first = cache.fetch(&url).await.expect("fetch");
        assert!(!first.from_cache && !first.stale);
        // The mock expects exactly one request: the second fetch must not hit it.
        let second = cache.fetch(&url).await.expect("fetch");
        assert!(second.from_cache && !second.stale);
        assert_eq!(second.body, first.body);
        server.verify().await;
    }

    #[tokio::test]
    async fn a_stale_etag_revalidates_with_if_none_match() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = FetchCache::new(
            dir.path().to_path_buf(),
            Duration::from_secs(0),
            1024 * 1024,
        )
        .expect("cache");
        let server = MockServer::start().await;
        let url = format!("{}/tree", server.uri());
        Mock::given(method("GET"))
            .and(path("/tree"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("v1")
                    .insert_header("etag", "\"abc\""),
            )
            .expect(1)
            .mount(&server)
            .await;
        cache.fetch(&url).await.expect("v1");
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/tree"))
            .and(header("if-none-match", "\"abc\""))
            .respond_with(ResponseTemplate::new(304))
            .expect(1)
            .mount(&server)
            .await;
        let out = cache.fetch(&url).await.expect("revalidate");
        assert_eq!(out.body, b"v1");
        assert!(!out.stale && !out.from_cache);
        server.verify().await;
    }

    #[tokio::test]
    async fn an_offline_stale_entry_serves_stale_with_a_warning() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache_in(dir.path());
        let server = MockServer::start().await;
        let url = format!("{}/tree", server.uri());
        Mock::given(method("GET"))
            .and(path("/tree"))
            .respond_with(ResponseTemplate::new(200).set_body_string("cached"))
            .mount(&server)
            .await;
        cache.fetch(&url).await.expect("prime");
        // No mocks mounted: the tree request 404s, and the stale body serves.
        server.reset().await;
        let stale_cache = FetchCache::new(
            dir.path().to_path_buf(),
            Duration::from_secs(0),
            1024 * 1024,
        )
        .expect("cache");
        let out = stale_cache.fetch(&url).await.expect("stale");
        assert!(out.stale);
        assert_eq!(out.body, b"cached");
    }

    #[tokio::test]
    async fn offline_with_no_cache_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache_in(dir.path());
        let out = cache.fetch("http://127.0.0.1:1/tree").await;
        assert!(out.is_err(), "{out:?}");
    }

    #[tokio::test]
    async fn oversized_bodies_are_served_but_not_stored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache =
            FetchCache::new(dir.path().to_path_buf(), Duration::from_secs(60), 4).expect("cache");
        let server = MockServer::start().await;
        let url = format!("{}/tree", server.uri());
        Mock::given(method("GET"))
            .and(path("/tree"))
            .respond_with(ResponseTemplate::new(200).set_body_string("way-too-big"))
            .expect(2)
            .mount(&server)
            .await;
        let out = cache.fetch(&url).await.expect("fetch");
        assert_eq!(out.body, b"way-too-big");
        // Not stored (over cap), so the second fetch hits the network again.
        cache.fetch(&url).await.expect("fetch");
        server.verify().await;
    }

    #[tokio::test]
    async fn a_torn_body_meta_pair_reads_as_a_miss() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache_in(dir.path());
        let server = MockServer::start().await;
        let url = format!("{}/tree", server.uri());
        Mock::given(method("GET"))
            .and(path("/tree"))
            .respond_with(ResponseTemplate::new(200).set_body_string("v1"))
            .expect(2)
            .mount(&server)
            .await;
        cache.fetch(&url).await.expect("v1");
        // A body from another write (length disagrees with the meta) is not served.
        let (body_path, _) = cache.slot(&url);
        std::fs::write(&body_path, b"torn-longer-body").expect("tear");
        let out = cache.fetch(&url).await.expect("refetch");
        assert_eq!(out.body, b"v1");
        assert!(!out.from_cache);
        // No temp files are left behind.
        let leftovers = std::fs::read_dir(dir.path())
            .expect("dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .count();
        assert_eq!(leftovers, 0);
        server.verify().await;
    }

    #[test]
    fn user_agent_names_the_host() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = cache_in(dir.path());
        assert!(
            cache.user_agent().starts_with("crosstalk/"),
            "{}",
            cache.user_agent()
        );
    }
}
