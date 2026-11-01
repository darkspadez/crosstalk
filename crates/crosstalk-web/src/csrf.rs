//! CSRF protection for the daemon's browser forms.
//!
//! The threat is a cross-origin form POST: with no token configured on a loopback listener,
//! any web page the operator visits could otherwise submit a form to the daemon (starting an
//! apply action, rewriting settings) as a CORS "simple request" with no preflight. The fix is
//! a synchroniser token: a random secret generated once at daemon startup, embedded as a hidden
//! field in every server-rendered POST form, and checked on every POST.
//!
//! The token is accepted two ways: the `csrf_token` form field (what the templates send) or
//! the `x-csrf-token` header (what scripts and the test suite send). `/login` is exempt: it is
//! the endpoint that establishes trust, and it carries the auth token itself. Requiring CSRF
//! on login would also deadlock a fresh browser with no cookie yet.
//!
//! A request that presents a valid `Authorization: Bearer <token>` header matching the
//! configured server token is also exempt: a cross-site page cannot forge that header (it
//! would need a CORS preflight the daemon never answers), so such a request is a script, not
//! a browser form. Browsers use the cookie plus `csrf_token`; scripts use Bearer and skip
//! CSRF.
//!
//! No new dependencies: randomness comes from the operating system (`/dev/urandom` on unix,
//! OS-seeded `RandomState` hashing elsewhere), comparison is constant-time.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Cookie name once used to persist a copy of the token. Kept so older clients parsing it
/// do not break; the middleware no longer sets it and validation never trusted it back.
///
/// Validation compares against the server-side secret only: the hidden form field and the
/// header are the channels, and a cookie copy would add nothing (a cross-site page cannot
/// read it, and anything that can read it already has the token).
pub const CSRF_COOKIE: &str = "crosstalk_csrf";
/// Hidden form field every POST template carries.
pub const CSRF_FIELD: &str = "csrf_token";
/// Header scripts (and the test suite) send instead of the form field.
pub const CSRF_HEADER: &str = "x-csrf-token";

/// Generate the daemon's CSRF secret: 32 random bytes, hex-encoded.
pub fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    if !os_random(&mut bytes) {
        fallback_random(&mut bytes);
    }
    hex(&bytes)
}

/// One OS random refill, reporting whether it worked.
fn os_random(bytes: &mut [u8]) -> bool {
    #[cfg(unix)]
    {
        // `getrandom(2)` without libc: `/dev/urandom` never blocks and is the documented
        // userspace interface to the same CSPRNG.
        use std::io::Read;
        std::fs::File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(bytes))
            .is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = bytes;
        false
    }
}

/// Last resort when the OS source is unavailable: SipHash outputs keyed by fresh
/// OS-seeded [`std::collections::hash_map::RandomState`]s. Each `RandomState::new()` draws
/// fresh random keys from the OS, so the outputs are unpredictable to anyone without them.
fn fallback_random(bytes: &mut [u8]) -> bool {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    for chunk in bytes.chunks_mut(8) {
        let state = RandomState::new();
        let mut hasher = state.build_hasher();
        hasher.write_u64(COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        hasher.write_u128(now_nanos());
        hasher.write_usize(&state as *const _ as usize);
        hasher.write_u32(std::process::id());
        let value = hasher.finish();
        chunk.copy_from_slice(&value.to_le_bytes()[..chunk.len()]);
    }
    true
}

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(DIGITS[(byte >> 4) as usize] as char);
        text.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    text
}

/// Whether the request carries this daemon's CSRF token, via the header or the form field.
pub fn is_valid(token: &str, request: &Request) -> bool {
    if let Some(header) = request
        .headers()
        .get(CSRF_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        return crate::auth::tokens_match(token, header.trim());
    }
    false
}

/// Pull the `csrf_token` field out of a urlencoded body.
///
/// The token is hex, so it survives urlencoding unchanged; a byte scan for the field is
/// enough and avoids a form-parsing dependency. Percent-decoding is unnecessary: any
/// encoding that changes the bytes cannot match the hex token anyway.
fn body_has_token(token: &str, body: &[u8]) -> bool {
    const PREFIX: &[u8] = b"csrf_token=";
    let mut rest = body;
    loop {
        let Some(start) = find(rest, PREFIX) else {
            return false;
        };
        // The field name must start the remaining body or follow a `&`: without this,
        // `not_csrf_token=...` would match the forged suffix.
        if start == 0 || rest[start - 1] == b'&' {
            let after = &rest[start + PREFIX.len()..];
            let end = after
                .iter()
                .position(|byte| *byte == b'&')
                .unwrap_or(after.len());
            if let Ok(value) = std::str::from_utf8(&after[..end]) {
                if crate::auth::tokens_match(token, value) {
                    return true;
                }
            }
        }
        rest = &rest[start + PREFIX.len()..];
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// The Bearer token of a script request, when the Authorization header carries one.
///
/// Only the header counts, never the cookie: the login cookie is sent automatically and a
/// cross-site page could ride it, while `Authorization` needs an explicit, preflighted
/// header a forged form cannot set.
fn bearer_token(request: &Request) -> Option<&str> {
    let value = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))?;
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

/// Reject POSTs without the daemon's CSRF token.
///
/// Runs outside the auth check, so a forged cross-site POST fails closed with 403 even when
/// no auth token is configured at all (the tokenless-loopback default).
pub async fn check(
    State(state): State<crate::state::AppState>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() == Method::GET {
        return next.run(request).await;
    }

    if request.method() != Method::POST {
        return next.run(request).await;
    }

    // The token-issuing endpoint: it carries the auth token itself.
    if request.uri().path() == "/login" {
        return next.run(request).await;
    }

    // A script presenting the configured server token cannot be a forged browser form:
    // cross-site pages cannot set the Authorization header. Only when a token is
    // configured, and only on an exact constant-time match.
    if let Some(expected) = state.token() {
        if let Some(presented) = bearer_token(&request) {
            if crate::auth::tokens_match(&expected, presented) {
                return next.run(request).await;
            }
        }
    }

    if is_valid(state.csrf_token(), &request) {
        return next.run(request).await;
    }

    // Fall back to the form field, which means consuming the body and rebuilding it.
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, 1 << 20).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return (StatusCode::PAYLOAD_TOO_LARGE, "the form was too large\n").into_response()
        }
    };
    if !body_has_token(state.csrf_token(), &bytes) {
        return (
            StatusCode::FORBIDDEN,
            "missing or invalid CSRF token: reload the page and resubmit the form\n",
        )
            .into_response();
    }
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    fn post(uri: &str, headers: &[(&str, &str)], body: &[u8]) -> Request {
        let mut builder = Request::builder().method("POST").uri(uri);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(Body::from(body.to_vec())).expect("request")
    }

    #[test]
    fn generated_tokens_are_unique_hex() {
        let first = generate_token();
        let second = generate_token();
        assert_eq!(first.len(), 64);
        assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }

    #[test]
    fn the_header_is_accepted() {
        let request = post(
            "/settings/host",
            &[("x-csrf-token", "abc123")],
            b"matching.ignore_quality_tags=true",
        );
        assert!(is_valid("abc123", &request));
        assert!(!is_valid("other", &request));
    }

    #[test]
    fn the_form_field_is_found() {
        assert!(body_has_token("abc123", b"a=1&csrf_token=abc123&b=2"));
        assert!(body_has_token("abc123", b"csrf_token=abc123"));
        assert!(!body_has_token("abc123", b"csrf_token=wrong"));
        assert!(!body_has_token("abc123", b"a=1"));
        assert!(!body_has_token("abc123", b"csrf_token="));
        // A field-prefix decoy does not match.
        assert!(!body_has_token("abc123", b"not_csrf_token=abc123"));
    }
}
