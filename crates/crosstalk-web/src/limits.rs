//! Resource bounds on the HTTP surface: request bodies, time per request, requests in flight.
//!
//! Every body is bounded by its route: a sign-in is one short field, a webhook delivery a small
//! dict, a settings form the largest thing the UI posts. The CSRF layer, which may have to read a
//! form body for its hidden field, reads under the same bound — and only after the auth gate has
//! let the request through, so an unauthenticated POST never has its body buffered at all.

use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// `POST /login`: one token field.
pub const LOGIN_BODY: usize = 4 * 1024;
/// `POST /hooks/dispatcharr`: a refresh's details dict.
pub const HOOK_BODY: usize = 64 * 1024;
/// `POST /settings/*`: the largest form the UI has (aliases, regexes).
pub const SETTINGS_BODY: usize = 256 * 1024;
/// Everything else.
pub const DEFAULT_BODY: usize = 64 * 1024;

/// How long one request may take before it is answered 408.
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Requests served at once; past it the daemon answers 503 rather than queueing.
pub const MAX_IN_FLIGHT: usize = 256;

/// The body bound for a path, the same one the route's `DefaultBodyLimit` sets.
pub fn body_limit(path: &str) -> usize {
    if path == "/login" {
        LOGIN_BODY
    } else if path == crosstalk_core::hooks::ROUTE {
        HOOK_BODY
    } else if path == "/settings" || path.starts_with("/settings/") {
        SETTINGS_BODY
    } else {
        DEFAULT_BODY
    }
}

/// Refuse a request past [`MAX_IN_FLIGHT`] with a 503 instead of letting it queue.
pub async fn concurrency(
    State(state): State<crate::state::AppState>,
    request: Request,
    next: Next,
) -> Response {
    let Ok(_permit) = state.in_flight().clone().try_acquire_owned() else {
        let mut response =
            (StatusCode::SERVICE_UNAVAILABLE, "busy: retry shortly\n").into_response();
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
        return response;
    };
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_route_has_its_own_bound() {
        assert_eq!(body_limit("/login"), 4096);
        assert_eq!(body_limit("/hooks/dispatcharr"), 65536);
        assert_eq!(body_limit("/settings/host"), 262144);
        assert_eq!(body_limit("/settings/guide"), 262144);
        assert_eq!(body_limit("/apps/guide/actions/match_apply"), 65536);
        assert_eq!(body_limit("/settingsx"), 65536);
    }
}
