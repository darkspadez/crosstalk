//! Token authentication for the daemon's HTTP surface.
//!
//! One master token, from `server.token` or the environment variable it names. A script sends it
//! as `Authorization: Bearer <token>` on every request; a browser presents it once to the login
//! page and gets a session (see [`crate::session`]), so the token itself never lives in a
//! cookie.
//!
//! The Dispatcharr webhook can have its own: `[hooks] token`, accepted as a Bearer on
//! `POST /hooks/dispatcharr` only and refused (401) everywhere else, so the credential Connect
//! stores can trigger a configured refresh and nothing more. While one is set, the master token
//! still works on the hook route for this release and logs a once-per-process warning; that
//! fallback goes in 0.2.0. Without one, the route takes the master token as before.
//!
//! [`gate`] is the one place that decides, and it runs before the Host and CSRF checks so that
//! nothing about a request is examined (or its body read) before its sender is known:
//!
//!   * a locked-out client is answered 429 before its credential is even looked at;
//!   * every wrong Bearer token on every route, and every wrong token posted to `/login`,
//!     counts toward the same per-client limit (ten in five minutes);
//!   * a wrong Bearer POST is a 401 (or 429), never the CSRF layer's 403, so the answer says
//!     nothing about whether a guess was close.
//!
//! With no token configured, every request is allowed — but only because the daemon refuses to
//! start that way on anything but a loopback listener with no proxy in front (see
//! [`requires_token`] and `ServerConfig::security_errors`), and a request carrying proxy headers
//! is refused outright.

use crate::client::ClientInfo;
use crate::session::SessionRef;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// Failed sign-ins (and wrong Bearer tokens) allowed per client before it is locked out.
pub const MAX_LOGIN_FAILURES: u32 = 10;
/// The window those failures are counted in.
pub const LOGIN_WINDOW: Duration = Duration::from_secs(5 * 60);
/// Clients a limiter tracks at once. Past it, the least recently seen tenth is dropped: an
/// attacker spraying addresses can only make the limiter forget, never grow without bound.
pub const MAX_TRACKED_CLIENTS: usize = 10_000;
/// Events kept per client; a count past this is "far over any limit" either way.
const MAX_HITS_PER_CLIENT: usize = 4096;

/// How a request was let through, for the CSRF layer and the templates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authenticated {
    /// No token is configured (a loopback-only daemon): the daemon-wide CSRF token applies.
    Open,
    /// A correct `Authorization: Bearer` token (the master token, or the hook token on the hook
    /// route): a script, exempt from CSRF.
    Bearer,
    /// A browser session, whose own CSRF token its forms carry.
    Session(SessionRef),
}

/// A per-client sliding-window counter, used twice: the auth failure limit (wrong tokens on
/// `POST /login` and wrong Bearer tokens anywhere) and the webhook's delivery rate.
///
/// Keyed by [`ClientInfo::key`], so behind a trusted proxy each client has its own count and a
/// spoofed `X-Forwarded-For` from anyone else changes nothing. Expired entries are swept once
/// per window, not on every call, and the map is capped at [`MAX_TRACKED_CLIENTS`].
#[derive(Debug)]
pub struct LoginLimiter {
    inner: std::sync::Mutex<LimiterInner>,
    max_failures: u32,
    window: Duration,
    capacity: usize,
}

#[derive(Debug)]
struct LimiterInner {
    buckets: HashMap<IpAddr, Bucket>,
    next_sweep: Instant,
}

#[derive(Debug, Default)]
struct Bucket {
    hits: VecDeque<Instant>,
}

impl Bucket {
    fn prune(&mut self, now: Instant, window: Duration) {
        while self
            .hits
            .front()
            .is_some_and(|at| now.duration_since(*at) >= window)
        {
            self.hits.pop_front();
        }
    }

    fn last(&self) -> Option<Instant> {
        self.hits.back().copied()
    }
}

impl LoginLimiter {
    pub fn new(max_failures: u32, window: Duration) -> Self {
        Self::with_capacity(max_failures, window, MAX_TRACKED_CLIENTS)
    }

    /// A limiter tracking at most `capacity` clients.
    pub fn with_capacity(max_failures: u32, window: Duration, capacity: usize) -> Self {
        Self {
            inner: std::sync::Mutex::new(LimiterInner {
                buckets: HashMap::new(),
                next_sweep: Instant::now() + window,
            }),
            max_failures,
            window,
            capacity: capacity.max(1),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LimiterInner> {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let now = Instant::now();
        // Amortised: one full sweep per window, however many calls arrive in it.
        if now >= inner.next_sweep {
            let window = self.window;
            inner.buckets.retain(|_, bucket| {
                bucket.prune(now, window);
                !bucket.hits.is_empty()
            });
            inner.next_sweep = now + window;
        }
        inner
    }

    /// The live count for `key`, pruned.
    fn count(&self, inner: &mut LimiterInner, key: IpAddr, now: Instant) -> u32 {
        let count = match inner.buckets.get_mut(&key) {
            Some(bucket) => {
                bucket.prune(now, self.window);
                bucket.hits.len() as u32
            }
            None => return 0,
        };
        if count == 0 {
            inner.buckets.remove(&key);
        }
        count
    }

    fn push(&self, inner: &mut LimiterInner, key: IpAddr, now: Instant) {
        if !inner.buckets.contains_key(&key) && inner.buckets.len() >= self.capacity {
            self.evict(inner, now);
        }
        let bucket = inner.buckets.entry(key).or_default();
        bucket.hits.push_back(now);
        if bucket.hits.len() > MAX_HITS_PER_CLIENT {
            bucket.hits.pop_front();
        }
    }

    /// Make room: drop what has expired, then the least recently seen tenth.
    fn evict(&self, inner: &mut LimiterInner, now: Instant) {
        let window = self.window;
        inner.buckets.retain(|_, bucket| {
            bucket.prune(now, window);
            !bucket.hits.is_empty()
        });
        if inner.buckets.len() < self.capacity {
            return;
        }
        let mut seen: Vec<(Instant, IpAddr)> = inner
            .buckets
            .iter()
            .map(|(key, bucket)| (bucket.last().unwrap_or(now), *key))
            .collect();
        let evicted = (self.capacity / 10).max(1).min(seen.len());
        seen.select_nth_unstable_by_key(evicted - 1, |(last, _)| *last);
        for (_, key) in &seen[..evicted] {
            inner.buckets.remove(key);
        }
    }

    /// Seconds until `key` is back under `max`, for `Retry-After`.
    fn retry_after(&self, inner: &LimiterInner, key: IpAddr, max: u32, now: Instant) -> u64 {
        let Some(bucket) = inner.buckets.get(&key) else {
            return 0;
        };
        let len = bucket.hits.len();
        let max = max as usize;
        // The hit whose expiry brings the count back under `max`.
        let index = if len >= max && max > 0 { len - max } else { 0 };
        bucket
            .hits
            .get(index)
            .map(|at| {
                self.window
                    .checked_sub(now.duration_since(*at))
                    .map(|left| left.as_secs().max(1))
                    .unwrap_or(1)
            })
            .unwrap_or(0)
    }

    /// Whether this client may attempt a sign-in now.
    pub fn allowed(&self, ip: IpAddr) -> bool {
        self.hits(ip) < self.max_failures
    }

    /// How many counted events this client has inside the window.
    pub fn hits(&self, ip: IpAddr) -> u32 {
        let mut inner = self.lock();
        self.count(&mut inner, ip, Instant::now())
    }

    /// `Ok` when the client is under the limit, else the seconds until it is. Counts nothing:
    /// the gate asks this of every request before looking at its credential.
    pub fn check(&self, ip: IpAddr) -> Result<(), u64> {
        let mut inner = self.lock();
        let now = Instant::now();
        if self.count(&mut inner, ip, now) >= self.max_failures {
            return Err(self.retry_after(&inner, ip, self.max_failures, now).max(1));
        }
        Ok(())
    }

    /// Check and count in one step, so concurrent attempts cannot all pass a check that each
    /// would fail once the others were recorded. A correct credential afterwards calls
    /// [`Self::forgive`] (or [`Self::record_success`]).
    pub fn try_attempt(&self, ip: IpAddr) -> Result<(), u64> {
        self.try_attempt_within(ip, self.max_failures)
    }

    /// [`Self::try_attempt`] against a bound from elsewhere: the webhook's live
    /// `hooks.rate_per_minute`.
    pub fn try_attempt_within(&self, ip: IpAddr, max: u32) -> Result<(), u64> {
        let mut inner = self.lock();
        let now = Instant::now();
        if self.count(&mut inner, ip, now) >= max {
            return Err(self.retry_after(&inner, ip, max, now).max(1));
        }
        self.push(&mut inner, ip, now);
        Ok(())
    }

    /// Take back the one event a successful [`Self::try_attempt`] counted.
    pub fn forgive(&self, ip: IpAddr) {
        let mut inner = self.lock();
        let emptied = match inner.buckets.get_mut(&ip) {
            Some(bucket) => {
                bucket.hits.pop_back();
                bucket.hits.is_empty()
            }
            None => false,
        };
        if emptied {
            inner.buckets.remove(&ip);
        }
    }

    /// Count one event for this client: a failed sign-in, or one webhook delivery.
    pub fn record_hit(&self, ip: IpAddr) {
        let mut inner = self.lock();
        self.push(&mut inner, ip, Instant::now());
    }

    /// Seconds until the client is back under the limit, for the `Retry-After` header.
    pub fn retry_after_secs(&self, ip: IpAddr) -> u64 {
        let inner = self.lock();
        self.retry_after(&inner, ip, self.max_failures, Instant::now())
    }

    /// Record a wrong token from this client.
    pub fn record_failure(&self, ip: IpAddr) {
        self.record_hit(ip);
    }

    /// A correct sign-in clears the client's failures.
    pub fn record_success(&self, ip: IpAddr) {
        self.lock().buckets.remove(&ip);
    }

    /// How many clients the limiter is tracking.
    pub fn tracked(&self) -> usize {
        self.lock().buckets.len()
    }
}

impl Default for LoginLimiter {
    fn default() -> Self {
        Self::new(MAX_LOGIN_FAILURES, LOGIN_WINDOW)
    }
}

/// Whether the daemon must require a token, given the bind address.
///
/// A loopback listener with no token is fine: only processes on the host can reach it. Anything
/// else without a token is a misconfiguration the daemon refuses to run with. (A loopback
/// listener behind a proxy is caught by `ServerConfig::security_errors`, which requires a token
/// once `trusted_proxies` is set, and by [`gate`], which refuses proxied requests when tokenless.)
pub fn requires_token(bind: &str, token: Option<&str>) -> bool {
    token.is_none() && !crosstalk_core::net::is_loopback_bind(bind)
}

/// The Bearer token in the `Authorization` header, and nothing else.
///
/// This is the only way the master token travels after sign-in: a browser holds a session id,
/// not the token.
pub fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    let value = value.trim();
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))?
        .trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

/// Compare two secrets in constant time: both are hashed to 32 bytes first, so neither the
/// content nor the length of either side shows in the time taken.
pub fn tokens_match(expected: &str, presented: &str) -> bool {
    digests_match(
        &crate::sha256::digest(expected.as_bytes()),
        &crate::sha256::digest(presented.as_bytes()),
    )
}

/// Constant-time equality of two digests.
pub fn digests_match(a: &[u8; 32], b: &[u8; 32]) -> bool {
    crate::sha256::digests_match(a, b)
}

/// What a presented Bearer token is, for the route it was presented on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Credential {
    /// The master token.
    Master,
    /// The hook token, on the hook route.
    Hook,
    /// Neither: a wrong token, or the hook token anywhere but the hook route.
    Wrong,
}

/// Judge a Bearer token. Both comparisons always run, each in constant time, so the answer's
/// timing says nothing about which token came close.
fn bearer_credential(
    view: &crate::state::ServerView,
    is_hook: bool,
    presented: &str,
) -> Credential {
    let master = view
        .token
        .as_deref()
        .is_some_and(|expected| tokens_match(expected, presented));
    let hook = view
        .hook_token
        .as_deref()
        .is_some_and(|expected| tokens_match(expected, presented));
    if is_hook && hook {
        Credential::Hook
    } else if master {
        Credential::Master
    } else {
        Credential::Wrong
    }
}

/// Whether the master-token-on-the-hook-route warning has been logged by this process.
static MASTER_ON_HOOK_WARNED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Log, once per process, that a delivery used the master token although a hook token is set.
fn warn_master_on_hook() {
    if !MASTER_ON_HOOK_WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::warn!(
            "a webhook delivery used the master token although [hooks] token is set: give \
             Dispatcharr Connect the hook token instead. The master token is accepted on \
             {} until crosstalk 0.2.0, then refused",
            crosstalk_core::hooks::ROUTE
        );
    }
}

/// Whether this process has logged the master-token-on-the-hook-route warning.
pub fn master_on_hook_warned() -> bool {
    MASTER_ON_HOOK_WARNED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Whether a request is reachable without a credential.
///
/// The login page (to GET; posting to it is an attempt and goes through the limiter), the
/// health check and the static assets the login page needs.
pub fn is_public(method: &Method, path: &str) -> bool {
    path == "/health"
        || path.starts_with("/static/")
        || (path == "/login" && (method == Method::GET || method == Method::HEAD))
}

/// The answer to any unauthenticated request: generic, so it says nothing about why.
pub fn unauthorized() -> Response {
    let mut response = (StatusCode::UNAUTHORIZED, "unauthorized\n").into_response();
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        header::HeaderValue::from_static("Bearer"),
    );
    response
}

/// The answer to a locked-out client.
pub fn too_many(retry_after: u64) -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        "too many failed attempts: wait a few minutes and try again\n",
    )
        .into_response();
    if let Ok(value) = header::HeaderValue::from_str(&retry_after.max(1).to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

/// The one auth layer: client identification, the limiter, then the credential.
///
/// See the module docs for the order and why. It inserts the [`ClientInfo`] on every request
/// and an [`Authenticated`] on every request it lets through to a protected route.
pub async fn gate(
    State(state): State<crate::state::AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let view = crate::state::ServerView::of_request(&state, &request);
    let client = ClientInfo::of_request(&request, &view.trusted);
    request.extensions_mut().insert(client);
    let path = request.uri().path().to_string();
    let is_hook = path == crosstalk_core::hooks::ROUTE;

    if view.token.is_none() {
        // Tokenless is only safe while nothing but this host can reach the daemon. A request
        // that came through a proxy came from somewhere else, whatever its socket says.
        if crate::client::has_forwarding_headers(request.headers()) {
            tracing::warn!(
                path = path.as_str(),
                "refused a proxied request to a daemon with no token: set server.token before \
                 putting crosstalk behind a reverse proxy"
            );
            return unauthorized();
        }
        if !is_hook {
            request.extensions_mut().insert(Authenticated::Open);
            return next.run(request).await;
        }
        // A route that starts real work needs a credential to check: the hook token, when one
        // is configured, goes through the limiter below like any Bearer.
        if view.hook_token.is_none() {
            tracing::warn!(
                "refused a webhook delivery: the route needs a configured token (set \
                 hooks.token or server.token and give Dispatcharr the same value as a Bearer \
                 token)"
            );
            return unauthorized();
        }
    }

    if is_public(request.method(), &path) {
        return next.run(request).await;
    }

    let limiter = state.login_limiter();
    if let Err(retry) = limiter.check(client.key) {
        return too_many(retry);
    }

    // A sign-in attempt: the handler counts it atomically with its own comparison.
    if path == "/login" {
        return next.run(request).await;
    }

    if let Some(presented) = bearer_token(request.headers()) {
        if let Err(retry) = limiter.try_attempt(client.key) {
            return too_many(retry);
        }
        match bearer_credential(&view, is_hook, &presented) {
            Credential::Hook => {}
            Credential::Master => {
                if is_hook && view.hook_token.is_some() {
                    warn_master_on_hook();
                }
            }
            Credential::Wrong => {
                tracing::warn!(
                    client = %client.ip,
                    path = path.as_str(),
                    "refused a wrong Bearer token"
                );
                return unauthorized();
            }
        }
        limiter.forgive(client.key);
        request.extensions_mut().insert(Authenticated::Bearer);
        return next.run(request).await;
    }

    // The webhook starts real work, so a browser session cannot trigger it by visiting a URL:
    // only the Bearer header counts there.
    if let (false, Some(expected)) = (is_hook, view.token.as_deref()) {
        if let Some(id) = crate::session::session_id(request.headers()) {
            if let Some(session) = state.sessions().touch(&id, expected) {
                request
                    .extensions_mut()
                    .insert(Authenticated::Session(session));
                return next.run(request).await;
            }
        }
    }

    // A browser gets the login page; anything else a 401, because a redirect would look like a
    // success to a script.
    let wants_html = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("text/html"));
    if wants_html && !is_hook {
        Redirect::to("/login").into_response()
    } else {
        unauthorized()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).expect("name"),
                value.parse().expect("value"),
            );
        }
        map
    }

    #[test]
    fn a_loopback_listener_needs_no_token() {
        assert!(!requires_token("127.0.0.1", None));
        assert!(!requires_token("localhost", None));
        assert!(!requires_token("::1", None));
        assert!(!requires_token("[::1]", None));
    }

    #[test]
    fn a_public_listener_needs_a_token() {
        assert!(requires_token("0.0.0.0", None));
        assert!(requires_token("192.168.1.5", None));
        // A name that merely starts like a loopback address is not one.
        assert!(requires_token("127.example.com", None));
        // And a configured token satisfies it.
        assert!(!requires_token("0.0.0.0", Some("secret")));
    }

    #[test]
    fn only_the_header_carries_the_token() {
        let with_header = headers(&[("authorization", "Bearer abc123")]);
        assert_eq!(bearer_token(&with_header).as_deref(), Some("abc123"));

        // The old token cookie is not a credential any more.
        let with_cookie = headers(&[("cookie", "other=1; crosstalk_token=xyz")]);
        assert!(bearer_token(&with_cookie).is_none());

        assert!(bearer_token(&headers(&[])).is_none());
    }

    #[test]
    fn an_empty_bearer_is_not_a_token() {
        assert!(bearer_token(&headers(&[("authorization", "Bearer ")])).is_none());
    }

    #[test]
    fn comparison_is_length_checked() {
        assert!(tokens_match("secret", "secret"));
        assert!(!tokens_match("secret", "secre"));
        assert!(!tokens_match("secret", "secreT"));
        assert!(!tokens_match("secret", ""));
        assert!(!tokens_match("secret", "secretsecret"));
    }

    #[test]
    fn the_login_page_and_assets_are_public() {
        assert!(is_public(&Method::GET, "/login"));
        assert!(is_public(&Method::HEAD, "/login"));
        assert!(
            !is_public(&Method::POST, "/login"),
            "an attempt goes through the limiter"
        );
        assert!(is_public(&Method::GET, "/static/app.css"));
        assert!(is_public(&Method::GET, "/health"));
        assert!(!is_public(&Method::GET, "/"));
        assert!(!is_public(&Method::GET, "/apps/guide"));
        assert!(!is_public(&Method::POST, crosstalk_core::hooks::ROUTE));
    }
}

#[cfg(test)]
mod limiter_tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn ip() -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))
    }

    #[test]
    fn ten_failures_lock_the_address_out() {
        let limiter = LoginLimiter::default();
        assert!(limiter.allowed(ip()));
        for _ in 0..10 {
            limiter.record_failure(ip());
        }
        assert!(!limiter.allowed(ip()));
        assert!(limiter.retry_after_secs(ip()) > 0);
        assert!(limiter.check(ip()).is_err());
    }

    #[test]
    fn a_success_clears_the_count() {
        let limiter = LoginLimiter::default();
        for _ in 0..9 {
            limiter.record_failure(ip());
        }
        limiter.record_success(ip());
        for _ in 0..9 {
            assert!(limiter.allowed(ip()));
            limiter.record_failure(ip());
        }
        assert!(limiter.allowed(ip()), "the count restarted at the success");
    }

    #[test]
    fn expired_failures_stop_counting() {
        let limiter = LoginLimiter::new(2, Duration::from_millis(50));
        limiter.record_failure(ip());
        limiter.record_failure(ip());
        assert!(!limiter.allowed(ip()));
        std::thread::sleep(Duration::from_millis(80));
        assert!(limiter.allowed(ip()));
    }

    #[test]
    fn the_counter_is_reusable_with_a_bound_from_elsewhere() {
        // The webhook route keeps its own count and reads the bound from the live configuration,
        // so the limiter has to expose the count rather than only a yes/no against its own max.
        let limiter = LoginLimiter::new(u32::MAX, LOGIN_WINDOW);
        assert_eq!(limiter.hits(ip()), 0);
        for expected in 1..=3 {
            limiter.record_hit(ip());
            assert_eq!(limiter.hits(ip()), expected);
        }
        // `record_failure` is the same counter: a sign-in attempt and a hook delivery are both
        // "one more event from this address".
        limiter.record_failure(ip());
        assert_eq!(limiter.hits(ip()), 4);
        assert!(limiter.try_attempt_within(ip(), 5).is_ok());
        assert!(limiter.try_attempt_within(ip(), 5).is_err());
    }

    #[test]
    fn addresses_are_counted_separately() {
        let limiter = LoginLimiter::new(1, LOGIN_WINDOW);
        let other = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        limiter.record_failure(ip());
        assert!(!limiter.allowed(ip()));
        assert!(limiter.allowed(other));
    }

    #[test]
    fn concurrent_attempts_cannot_overshoot_the_limit() {
        // Check-and-count is one step: forty racing attempts admit exactly ten.
        let limiter = std::sync::Arc::new(LoginLimiter::default());
        let admitted = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let threads: Vec<_> = (0..40)
            .map(|_| {
                let limiter = std::sync::Arc::clone(&limiter);
                let admitted = std::sync::Arc::clone(&admitted);
                std::thread::spawn(move || {
                    if limiter.try_attempt(ip()).is_ok() {
                        admitted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("thread");
        }
        assert_eq!(
            admitted.load(std::sync::atomic::Ordering::SeqCst),
            MAX_LOGIN_FAILURES
        );
    }

    #[test]
    fn a_forgiven_attempt_does_not_count() {
        let limiter = LoginLimiter::default();
        for _ in 0..50 {
            assert!(limiter.try_attempt(ip()).is_ok());
            limiter.forgive(ip());
        }
        assert_eq!(limiter.hits(ip()), 0);
        assert_eq!(limiter.tracked(), 0, "an empty bucket is dropped");
    }

    #[test]
    fn the_map_is_capped() {
        let limiter = LoginLimiter::with_capacity(10, LOGIN_WINDOW, 100);
        for n in 0..1000u32 {
            limiter.record_failure(IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + n)));
        }
        assert!(limiter.tracked() <= 100, "{}", limiter.tracked());
        // The most recent client is still counted.
        assert_eq!(
            limiter.hits(IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + 999))),
            1
        );
    }
}
