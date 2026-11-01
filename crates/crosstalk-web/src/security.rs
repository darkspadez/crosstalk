//! Browser security headers on every response.
//!
//! The UI is server-rendered HTML with self-hosted htmx and one small self-hosted script
//! (`confirm.js`); there are no inline scripts, inline styles, or third-party assets, so a
//! strict Content-Security-Policy is feasible and costs nothing:
//!
//!   * `default-src 'self'` — everything loads from the daemon itself.
//!   * `script-src 'self'` — htmx and the confirm delegation only. The old
//!     `onsubmit="return confirm(...)"` inline handler could not survive this, which is why
//!     the templates carry `data-confirm` attributes read by `confirm.js` instead.
//!   * `style-src 'self'` — the one stylesheet; no inline `style=` attributes exist.
//!   * `frame-ancestors 'none'` plus `X-Frame-Options: DENY` — the daemon must never render
//!     inside another page (clickjacking a logged-in operator past the confirm dialogs).
//!   * `X-Content-Type-Options: nosniff` — a CSV download must never be sniffed as HTML.
//!   * `Referrer-Policy: same-origin` — the token cookie's pages leak no URL on navigation.
//!
//! The layer sits outside the auth/CSRF/Host layers, so even a rejected request (403, 401,
//! redirect) carries the headers.

use axum::extract::Request;
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;

/// The policy, kept in one place so the tests assert on the same string the daemon sends.
pub const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; script-src 'self'; \
    style-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'self'; \
    form-action 'self'; frame-ancestors 'none'";

/// Attach the security headers to the response, whatever it is.
pub async fn headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        header::HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    headers.insert(
        header::X_FRAME_OPTIONS,
        header::HeaderValue::from_static("DENY"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        header::HeaderValue::from_static("same-origin"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_policy_is_strict_and_self_hosted() {
        assert!(CONTENT_SECURITY_POLICY.contains("default-src 'self'"));
        assert!(CONTENT_SECURITY_POLICY.contains("script-src 'self'"));
        assert!(
            !CONTENT_SECURITY_POLICY.contains("unsafe-inline"),
            "inline handlers must stay dead: {CONTENT_SECURITY_POLICY}"
        );
        assert!(
            !CONTENT_SECURITY_POLICY.contains("unsafe-eval"),
            "{CONTENT_SECURITY_POLICY}"
        );
        assert!(
            CONTENT_SECURITY_POLICY.contains("frame-ancestors 'none'"),
            "{CONTENT_SECURITY_POLICY}"
        );
    }
}
