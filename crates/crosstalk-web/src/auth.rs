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

/// Pull the presented token out of a request: the header first, then the cookie.
pub fn presented_token(request: &Request) -> Option<String> {
    if let Some(value) = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        let value = value.trim();
        if let Some(token) = value
            .strip_prefix("Bearer ")
            .or_else(|| value.strip_prefix("bearer "))
        {
            let token = token.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
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
        assert!(!is_public("/apps/janitor"));
    }
}
