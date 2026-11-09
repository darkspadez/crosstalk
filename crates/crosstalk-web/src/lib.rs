//! crosstalk's web UI and JSON API.
//!
//! Server-rendered Askama templates with htmx for the live parts, so the whole interface is one
//! binary with no Node toolchain. The pages are ordinary forms and tables; the run page polls
//! its status and rows instead of holding an SSE connection, which keeps the browser from
//! needing a reconnect path for a page an operator glances at.
//!
//! Authentication is a bearer token: the one in `crosstalk.toml` (or its environment variable),
//! sent as an `Authorization: Bearer` header by a script, or presented once to the login page,
//! which answers with a server-side session cookie.

pub mod auth;
pub mod client;
pub mod csrf;
pub mod host;
pub mod limits;
pub mod routes;
pub mod security;
pub mod session;
pub mod sha256;
pub mod state;

pub use routes::router;
pub use state::{AppState, UiApp, UiField};

/// The default port, matching the example configuration.
pub const DEFAULT_PORT: u16 = 8730;

#[cfg(test)]
mod tests;
