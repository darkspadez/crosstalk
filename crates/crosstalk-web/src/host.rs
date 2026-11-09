//! Host and Origin allowlist: the DNS-rebinding defence.
//!
//! `csrf.rs` stops a forged cross-site POST, but a DNS-rebinding page is SAME-origin: the
//! attacker's hostname resolves to 127.0.0.1, so the browser sends the request "home" and a
//! script on that page can READ the response, including the `csrf_token` hidden field. The
//! CSRF token cannot defend against a reader, so the Host header has to.
//!
//! The rule:
//!
//!   * When the server has NO token configured, only loopback Host values are allowed:
//!     `localhost`, `127.0.0.0/8` in any dotted form, `::1` (bracketed or bare), and the
//!     configured bind host when it names a specific address. A tokenless daemon only
//!     listens on loopback anyway (startup refuses anything else), so a Host of
//!     `evil.example` can only arrive via rebinding and is rejected.
//!   * When a token IS configured, the Host check is skipped: bearer auth is the
//!     protection there, and legitimate deployments sit behind reverse proxies whose Host
//!     values are foreign by design. Rejecting those would break every proxied install
//!     while adding nothing an attacker without the token could bypass.
//!   * Always, on anything but GET/HEAD/OPTIONS: when an `Origin` header is present, its
//!     scheme, host and port must be the daemon's own origin. Browsers send `Origin` on POSTs;
//!     a cross-site forgery carries the attacker's origin, so a mismatch is a forgery even with
//!     a valid Host. The daemon's origin is `[server] public_url` when set (a proxy that
//!     rewrites `Host` needs it), else what a trusted proxy's `X-Forwarded-Proto` /
//!     `X-Forwarded-Host` say, else `http://` plus the `Host` header.
//!
//! A missing Host header is allowed through: direct IP clients and the test harness do not
//! always send one, and there is nothing to compare against. The rebinding attacker cannot
//! use this: browsers always send Host.

use axum::extract::{Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Reject requests whose Host is not allowed, and writes whose Origin is not this daemon's.
pub async fn check(
    State(state): State<crate::state::AppState>,
    request: Request,
    next: Next,
) -> Response {
    let view = crate::state::ServerView::of_request(&state, &request);
    let client = request
        .extensions()
        .get::<crate::client::ClientInfo>()
        .copied()
        .unwrap_or_else(|| crate::client::ClientInfo::of_request(&request, &view.trusted));

    if !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) {
        if let Some(origin) = request
            .headers()
            .get(header::ORIGIN)
            .and_then(|value| value.to_str().ok())
        {
            let presented = crosstalk_core::net::url_origin(origin);
            if let Some(expected) = expected_origin(&view, &client, request.headers()) {
                if presented.as_deref() != Some(expected.as_str()) {
                    return (
                        StatusCode::FORBIDDEN,
                        "the request origin does not match this daemon\n",
                    )
                        .into_response();
                }
            }
        }
    }

    // A configured token means bearer auth is the protection (see the module docs): only
    // the tokenless loopback listener needs the Host allowlist. Both are read from the
    // current config, so a token rotation or bind change saved in the UI applies here too.
    if view.token.is_some() {
        return next.run(request).await;
    }

    if let Some(host) = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
    {
        let host = host_part(host);
        if !host.is_empty() && !is_allowed(&host, &view.bind) {
            return (
                StatusCode::FORBIDDEN,
                "the request host is not this daemon: possible DNS rebinding\n",
            )
                .into_response();
        }
    }

    next.run(request).await
}

/// The origin a browser page of this daemon posts from, when it can be known.
///
/// `public_url` wins; then a trusted proxy's word; then the `Host` header over plain http (the
/// daemon itself never terminates TLS). `None` (no Host at all) skips the comparison, as a
/// missing Host did before.
fn expected_origin(
    view: &crate::state::ServerView,
    client: &crate::client::ClientInfo,
    headers: &axum::http::HeaderMap,
) -> Option<String> {
    if let Some(origin) = &view.public_origin {
        return Some(origin.clone());
    }
    let forwarded_proto = client
        .via_trusted_proxy
        .then(|| crate::client::forwarded_proto(headers))
        .flatten();
    let scheme = forwarded_proto.as_deref().unwrap_or("http");
    if client.via_trusted_proxy {
        if let Some(host) = crate::client::forwarded_host(headers) {
            return crosstalk_core::net::origin(scheme, &host);
        }
    }
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())?;
    // A Host that is no origin at all cannot match any Origin: compare against something that
    // never equals a parsed one.
    Some(crosstalk_core::net::origin(scheme, host).unwrap_or_else(|| "invalid".to_string()))
}

/// The host portion of a Host header value: ports stripped, brackets removed, lowercased.
fn host_part(value: &str) -> String {
    let value = value.trim();
    if let Some(rest) = value.strip_prefix('[') {
        // `[::1]` or `[::1]:8730`.
        if let Some(end) = rest.find(']') {
            return rest[..end].to_lowercase();
        }
        return value.to_lowercase();
    }
    // Strip `:port` only when there is exactly one colon: a bare `::1` is an address, not
    // a host with a port.
    if value.matches(':').count() == 1 {
        if let Some((host, _)) = value.rsplit_once(':') {
            return host.to_lowercase();
        }
    }
    value.to_lowercase()
}

/// Whether a Host header value may reach a tokenless daemon.
///
/// Normalises the value itself, so callers that already stripped the port get the same
/// answer as the middleware path.
fn is_allowed(host: &str, bind: &str) -> bool {
    let host = host_part(host);
    let host = host.as_str();
    if host == "localhost" || host == "::1" {
        return true;
    }
    if is_loopback_v4(host) {
        return true;
    }
    // The configured bind host, when it names a specific address: an operator who bound
    // `127.0.0.2` reaches the daemon as `127.0.0.2`, and that Host is legitimate. A
    // wildcard bind (`0.0.0.0`, `::`) matches nothing here on purpose: with no token the
    // daemon refuses to run on a wildcard at all, so there is no legitimate foreign Host
    // to allow.
    let bind = host_part(bind.trim());
    if bind == "0.0.0.0" || bind == "::" || bind.is_empty() {
        return false;
    }
    host == bind
}

/// Whether the host is a dotted `127.0.0.0/8` address in any form (`127.1`, `127.0.0.1`).
fn is_loopback_v4(host: &str) -> bool {
    let parts: Vec<&str> = host.split('.').collect();
    if parts.len() != 4 || parts[0] != "127" {
        return false;
    }
    parts.iter().all(|part| {
        !part.is_empty()
            && part.len() <= 3
            && part.bytes().all(|b| b.is_ascii_digit())
            && part.parse::<u8>().is_ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_hosts_are_allowed() {
        assert!(is_allowed("localhost", "127.0.0.1"));
        assert!(is_allowed("LOCALHOST", "127.0.0.1"));
        assert!(is_allowed("127.0.0.1", "127.0.0.1"));
        assert!(is_allowed("127.0.0.2", "127.0.0.1"));
        assert!(is_allowed("127.1.2.3", "127.0.0.1"));
        assert!(is_allowed("::1", "127.0.0.1"));
    }

    #[test]
    fn rebinding_hosts_are_rejected() {
        assert!(!is_allowed("evil.example", "127.0.0.1"));
        assert!(!is_allowed("attacker.test", "127.0.0.1"));
        assert!(!is_allowed("192.168.1.5", "127.0.0.1"));
        assert!(!is_allowed("128.0.0.1", "127.0.0.1"));
        assert!(!is_allowed("1270.0.0.1", "127.0.0.1"));
    }

    #[test]
    fn the_bound_address_is_allowed_but_a_wildcard_allows_nothing() {
        assert!(is_allowed("127.0.0.2", "127.0.0.2"));
        assert!(!is_allowed("evil.example", "127.0.0.2"));
        assert!(!is_allowed("evil.example", "0.0.0.0"));
        assert!(!is_allowed("anything", "::"));
        // Loopback is still loopback under a wildcard bind.
        assert!(is_allowed("127.0.0.1", "0.0.0.0"));
    }

    #[test]
    fn ports_and_brackets_are_stripped() {
        assert_eq!(host_part("evil.example:8730"), "evil.example");
        assert_eq!(host_part("[::1]:8730"), "::1");
        assert_eq!(host_part("[::1]"), "::1");
        assert_eq!(host_part("::1"), "::1");
        assert_eq!(host_part("LOCALHOST:8730"), "localhost");
    }
}
