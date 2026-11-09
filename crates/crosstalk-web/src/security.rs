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
//!   * `Referrer-Policy: same-origin` — the signed-in pages leak no URL on navigation.
//!   * `Cache-Control: no-store` on everything but `/static/*` — a run page or a settings
//!     form must not linger in a shared or back-button cache; the embedded assets are cached
//!     for a day.
//!   * `Strict-Transport-Security` when the request came over https through a trusted proxy,
//!     or `[server] hsts = true` — the daemon never terminates TLS itself, so it only says so
//!     when it knows.
//!   * `Cross-Origin-Opener-Policy: same-origin` and a `Permissions-Policy` that switches off
//!     the device APIs the UI never uses.
//!
//! The layer sits outside every other layer, so even a rejected request (403, 401, 429,
//! redirect) carries the headers. It also reads the `[server]` settings once for the request
//! and hands them inward.

use axum::extract::{Request, State};
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;
use std::sync::Arc;

/// The policy, kept in one place so the tests assert on the same string the daemon sends.
pub const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; script-src 'self'; \
    style-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'self'; \
    form-action 'self'; frame-ancestors 'none'";

/// `Strict-Transport-Security`: a year, this host only (the operator's other subdomains are
/// not the daemon's to decide).
pub const HSTS: &str = "max-age=31536000";
/// The browser features the UI never uses, switched off.
pub const PERMISSIONS_POLICY: &str = "camera=(), microphone=(), geolocation=(), payment=(), usb=()";
/// How long the embedded assets may be cached.
pub const STATIC_CACHE: &str = "public, max-age=86400";

/// Attach the security headers to the response, whatever it is.
pub async fn headers(
    State(state): State<crate::state::AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let view = Arc::new(state.server_view());
    let client = crate::client::ClientInfo::of_request(&request, &view.trusted);
    let hsts = view.hsts || client.https;
    let is_static = request.uri().path().starts_with("/static/");
    request.extensions_mut().insert(Arc::clone(&view));

    let mut response = next.run(request).await;
    let cacheable = is_static && response.status().is_success();
    let headers = response.headers_mut();
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static(if cacheable { STATIC_CACHE } else { "no-store" }),
    );
    if hsts {
        headers.insert(
            header::STRICT_TRANSPORT_SECURITY,
            header::HeaderValue::from_static(HSTS),
        );
    }
    headers.insert(
        header::HeaderName::from_static("cross-origin-opener-policy"),
        header::HeaderValue::from_static("same-origin"),
    );
    headers.insert(
        header::HeaderName::from_static("permissions-policy"),
        header::HeaderValue::from_static(PERMISSIONS_POLICY),
    );
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
