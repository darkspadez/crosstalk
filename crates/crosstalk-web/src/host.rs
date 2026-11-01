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
//!   * Always, on POST: when an `Origin` header is present its host must match `Host`.
//!     Browsers send `Origin` on POSTs; a cross-site forgery carries the attacker's origin
//!     while `Host` names the daemon, so a mismatch is a forgery even with a valid Host.
//!
//! A missing Host header is allowed through: direct IP clients and the test harness do not
//! always send one, and there is nothing to compare against. The rebinding attacker cannot
//! use this: browsers always send Host.

use axum::extract::{Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Reject requests whose Host is not allowed, and POSTs whose Origin disagrees with Host.
pub async fn check(
    State(state): State<crate::state::AppState>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() == Method::POST {
        if let Some(origin) = origin_of(&request) {
            let host = request
                .headers()
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                .map(host_part);
            if let Some(host) = host {
                if origin != host && !origin.is_empty() && !host.is_empty() {
                    return (
                        StatusCode::FORBIDDEN,
                        "the request origin does not match the host\n",
                    )
                        .into_response();
                }
            }
        }
    }

    // A configured token means bearer auth is the protection (see the module docs): only
    // the tokenless loopback listener needs the Host allowlist. Both are read from the
    // current snapshot, so a token rotation or bind change saved in the UI applies here too.
    let snapshot = state.snapshot();
    if snapshot.server.resolved_token().is_some() {
        return next.run(request).await;
    }

    if let Some(host) = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
    {
        let host = host_part(host);
        if !host.is_empty() && !is_allowed(&host, &snapshot.server.bind) {
            return (
                StatusCode::FORBIDDEN,
                "the request host is not this daemon: possible DNS rebinding\n",
            )
                .into_response();
        }
    }

    next.run(request).await
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

/// The host portion of an Origin header value (`scheme://host[:port]`), if one is present.
fn origin_of(request: &Request) -> Option<String> {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())?;
    let authority = match origin.split_once("://") {
        Some((_, rest)) => rest,
        None => origin,
    };
    let authority = authority.split('/').next().unwrap_or(authority);
    Some(host_part(authority))
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
