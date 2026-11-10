//! Token authentication for the daemon's HTTP surface.
//!
//! One token, from `server.token` or the environment variable it names. A browser presents it
//! as a cookie set by the login page; a script sends `Authorization: Bearer <token>`.
//!
//! With no token configured, every request is allowed — but only because the configuration
//! validation warns loudly when the listener is not loopback, and the daemon refuses to start
//! that way (see [`requires_token`]).

use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};

/// The cookie the login page sets.
pub const COOKIE: &str = "crosstalk_token";

/// Failed sign-ins allowed per IP before the address is locked out.
pub const MAX_LOGIN_FAILURES: u32 = 10;
/// The window those failures are counted in.
pub const LOGIN_WINDOW: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// A per-address sliding-window counter, used for two things: brute-force guard on
/// `POST /login` (failed sign-ins) and rate limiting `POST /hooks/dispatcharr` (every delivery).
///
/// Ten wrong tokens in five minutes locks the address out with a 429, because the one
/// token guards everything and never expires. A correct sign-in clears the address's
/// failures, so a typo streak followed by the right token does not lock anyone out.
/// Expired failures are pruned on every check, so idle addresses do not accumulate.
///
/// The webhook route uses the same counting through [`LoginLimiter::hits`], because a delivery
/// starts real work and a Connect integration configured twice (or a loop) must not start a run
/// per request. Its bound comes from the live configuration rather than from the limiter, so
/// raising `hooks.rate_per_minute` takes effect without a restart.
#[derive(Debug)]
pub struct LoginLimiter {
    failures:
        std::sync::Mutex<std::collections::HashMap<std::net::IpAddr, Vec<std::time::Instant>>>,
    max_failures: u32,
    window: std::time::Duration,
}

impl LoginLimiter {
    pub fn new(max_failures: u32, window: std::time::Duration) -> Self {
        Self {
            failures: std::sync::Mutex::new(std::collections::HashMap::new()),
            max_failures,
            window,
        }
    }

    /// Whether this address may attempt a sign-in now.
    pub fn allowed(&self, ip: std::net::IpAddr) -> bool {
        self.hits(ip) < self.max_failures
    }

    /// How many counted events this address has inside the window, expired ones pruned.
    ///
    /// The webhook route compares this against `hooks.rate_per_minute` itself, so the bound comes
    /// from the configuration rather than from the limiter that holds the count.
    pub fn hits(&self, ip: std::net::IpAddr) -> u32 {
        let mut failures = self.failures.lock().expect("login limiter lock");
        let now = std::time::Instant::now();
        let window = self.window;
        failures.retain(|_, instants| {
            instants.retain(|at| now.duration_since(*at) < window);
            !instants.is_empty()
        });
        failures
            .get(&ip)
            .map(|instants| instants.len() as u32)
            .unwrap_or(0)
    }

    /// Count one event for this address: a failed sign-in, or one webhook delivery.
    pub fn record_hit(&self, ip: std::net::IpAddr) {
        self.failures
            .lock()
            .expect("login limiter lock")
            .entry(ip)
            .or_default()
            .push(std::time::Instant::now());
    }

    /// Seconds until the oldest failure expires, for the `Retry-After` header.
    pub fn retry_after_secs(&self, ip: std::net::IpAddr) -> u64 {
        let failures = self.failures.lock().expect("login limiter lock");
        let now = std::time::Instant::now();
        failures
            .get(&ip)
            .and_then(|instants| instants.iter().min())
            .map(|oldest| {
                self.window
                    .checked_sub(now.duration_since(*oldest))
                    .map(|left| left.as_secs().max(1))
                    .unwrap_or(1)
            })
            .unwrap_or(0)
    }

    /// Record a wrong token from this address.
    pub fn record_failure(&self, ip: std::net::IpAddr) {
        self.record_hit(ip);
    }

    /// A correct sign-in clears the address's failures.
    pub fn record_success(&self, ip: std::net::IpAddr) {
        self.failures
            .lock()
            .expect("login limiter lock")
            .remove(&ip);
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
/// else without a token is a misconfiguration the daemon refuses to run with.
pub fn requires_token(bind: &str, token: Option<&str>) -> bool {
    if token.is_some() {
        return false;
    }
    let bind = bind.trim();
    !(bind == "localhost" || bind.starts_with("127.") || bind == "::1" || bind == "[::1]")
}

/// The Bearer token in the `Authorization` header, and nothing else.
///
/// A script (and the webhook) presents the token this way; a browser sends the cookie instead.
/// The distinction matters twice: CSRF exempts a request whose Bearer token checks out (a
/// cross-site form cannot set that header), and the webhook route accepts ONLY this form, so a
/// session cookie alone cannot trigger work.
pub fn bearer_token(request: &Request) -> Option<String> {
    let value = request
        .headers()
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

/// Pull the presented token out of a request: the header first, then the cookie.
pub fn presented_token(request: &Request) -> Option<String> {
    if let Some(token) = bearer_token(request) {
        return Some(token);
    }
    let cookies = request.headers().get(header::COOKIE)?.to_str().ok()?;
    for cookie in cookies.split(';') {
        let cookie = cookie.trim();
        if let Some(value) = cookie.strip_prefix(&format!("{COOKIE}=")) {
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// Compare two tokens without leaking their length through timing.
pub fn tokens_match(expected: &str, presented: &str) -> bool {
    let expected = expected.as_bytes();
    let presented = presented.as_bytes();
    if expected.len() != presented.len() {
        return false;
    }
    let mut difference = 0u8;
    for (a, b) in expected.iter().zip(presented.iter()) {
        difference |= a ^ b;
    }
    difference == 0
}

/// Whether a path is reachable without a token.
///
/// The login page and the stylesheet have to be, or the login page could not be rendered.
fn is_public(path: &str) -> bool {
    path == "/login" || path == "/health" || path.starts_with("/static/")
}

/// Reject an unauthenticated request.
///
/// A browser gets redirected to the login page; anything else gets a 401, because a redirect
/// would look like a success to a script.
pub async fn require(
    axum::extract::State(state): axum::extract::State<crate::state::AppState>,
    request: Request,
    next: Next,
) -> Response {
    // The webhook route has its own rule: a Bearer token, never the session cookie. It is
    // checked before the "no token configured means open" default, because that default would
    // leave a route that starts real work reachable with no credential at all.
    if request.uri().path() == crosstalk_core::hooks::ROUTE {
        return require_webhook_bearer(state, request, next).await;
    }

    let Some(expected) = state.token() else {
        return next.run(request).await;
    };

    if is_public(request.uri().path()) {
        return next.run(request).await;
    }

    match presented_token(&request) {
        Some(presented) if tokens_match(&expected, &presented) => next.run(request).await,
        _ => {
            let wants_html = request
                .headers()
                .get(header::ACCEPT)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.contains("text/html"));
            if wants_html {
                Redirect::to("/login").into_response()
            } else {
                (StatusCode::UNAUTHORIZED, "a token is required\n").into_response()
            }
        }
    }
}

/// Authenticate a webhook delivery: a Bearer token, and nothing else.
///
/// Three refusals, all 401:
///   * no `server.token` configured — the route has no credential to check, and a route that
///     starts real work must not be reachable without one. (The daemon only runs tokenless on a
///     loopback bind, where Dispatcharr on another host could not reach it anyway.)
///   * no `Authorization: Bearer …` header. A session cookie is deliberately NOT accepted: a
///     browser session should not be able to trigger an apply by visiting a URL.
///   * a token that does not match.
async fn require_webhook_bearer(
    state: crate::state::AppState,
    request: Request,
    next: Next,
) -> Response {
    let Some(expected) = state.token() else {
        return (
            StatusCode::UNAUTHORIZED,
            "the webhook route needs a configured server token: set server.token or \
             server.token_env and give Dispatcharr the same value as a Bearer token\n",
        )
            .into_response();
    };
    match bearer_token(&request) {
        Some(presented) if tokens_match(&expected, &presented) => next.run(request).await,
        Some(_) => (StatusCode::UNAUTHORIZED, "invalid token\n").into_response(),
        None => (
            StatusCode::UNAUTHORIZED,
            "the webhook route accepts only an Authorization: Bearer <token> header, not a \
             session cookie\n",
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    fn request(headers: &[(&str, &str)]) -> Request {
        let mut builder = Request::builder().uri("/");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(Body::empty()).expect("request")
    }

    #[test]
    fn a_loopback_listener_needs_no_token() {
        assert!(!requires_token("127.0.0.1", None));
        assert!(!requires_token("localhost", None));
        assert!(!requires_token("::1", None));
    }

    #[test]
    fn a_public_listener_needs_a_token() {
        assert!(requires_token("0.0.0.0", None));
        assert!(requires_token("192.168.1.5", None));
        // And a configured token satisfies it.
        assert!(!requires_token("0.0.0.0", Some("secret")));
    }

    #[test]
    fn the_header_is_preferred_and_both_forms_work() {
        let with_header = request(&[("authorization", "Bearer abc123")]);
        assert_eq!(presented_token(&with_header).as_deref(), Some("abc123"));

        let with_cookie = request(&[("cookie", "other=1; crosstalk_token=xyz")]);
        assert_eq!(presented_token(&with_cookie).as_deref(), Some("xyz"));

        let neither = request(&[]);
        assert!(presented_token(&neither).is_none());
    }

    #[test]
    fn an_empty_bearer_is_not_a_token() {
        let request = request(&[("authorization", "Bearer ")]);
        assert!(presented_token(&request).is_none());
    }

    #[test]
    fn comparison_is_length_checked() {
        assert!(tokens_match("secret", "secret"));
        assert!(!tokens_match("secret", "secre"));
        assert!(!tokens_match("secret", "secreT"));
        assert!(!tokens_match("secret", ""));
    }

    #[test]
    fn the_login_page_and_assets_are_public() {
        assert!(is_public("/login"));
        assert!(is_public("/static/app.css"));
        assert!(is_public("/health"));
        assert!(!is_public("/"));
        assert!(!is_public("/apps/guide"));
    }
}

#[cfg(test)]
mod limiter_tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

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
        let limiter = LoginLimiter::new(2, std::time::Duration::from_millis(50));
        limiter.record_failure(ip());
        limiter.record_failure(ip());
        assert!(!limiter.allowed(ip()));
        std::thread::sleep(std::time::Duration::from_millis(80));
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
    }

    #[test]
    fn addresses_are_counted_separately() {
        let limiter = LoginLimiter::new(1, LOGIN_WINDOW);
        let other = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        limiter.record_failure(ip());
        assert!(!limiter.allowed(ip()));
        assert!(limiter.allowed(other));
    }
}
