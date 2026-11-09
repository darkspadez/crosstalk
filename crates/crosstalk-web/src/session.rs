//! Browser sessions: what the login page hands out instead of the master token.
//!
//! A successful sign-in creates a random 32-byte session id held server-side with its own CSRF
//! token. The browser gets only the id, in `crosstalk_session` (`HttpOnly; SameSite=Strict`,
//! plus `Secure` over https), so the master token never sits in a cookie jar, a session can be
//! revoked by signing out, and it expires on its own: 12 hours idle, 7 days at most. Sessions
//! live in memory, so a restart signs every browser out — the cheap, safe default for a
//! single-operator daemon.
//!
//! Each session remembers a digest of the token it was created under; rotating the token signs
//! every session out on its next request.

use axum::http::{header, HeaderMap};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The session cookie's name.
pub const COOKIE: &str = "crosstalk_session";
/// The cookie earlier versions set to the raw master token. No longer accepted; the login and
/// logout responses expire it.
pub const LEGACY_COOKIE: &str = "crosstalk_token";
/// A session unused this long is gone.
pub const IDLE: Duration = Duration::from_secs(12 * 60 * 60);
/// A session this old is gone however busy it is.
pub const ABSOLUTE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// More live sessions than one operator could plausibly need; the oldest is dropped past it.
pub const MAX_SESSIONS: usize = 1024;

/// One signed-in browser.
#[derive(Debug, Clone)]
struct Session {
    /// The synchroniser token this session's forms carry.
    csrf: String,
    /// The digest of the master token this session was created under.
    token_digest: [u8; 32],
    created: Instant,
    last_seen: Instant,
}

/// A live session, as the auth gate hands it to the inner layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRef {
    pub id: String,
    pub csrf: String,
}

/// The daemon's sessions.
#[derive(Debug)]
pub struct Sessions {
    map: Mutex<HashMap<String, Session>>,
    idle: Duration,
    absolute: Duration,
}

impl Default for Sessions {
    fn default() -> Self {
        Self::new(IDLE, ABSOLUTE)
    }
}

impl Sessions {
    pub fn new(idle: Duration, absolute: Duration) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            idle,
            absolute,
        }
    }

    /// Start a session under the current master token, returning its id and CSRF token.
    pub fn create(&self, token: &str) -> SessionRef {
        let id = crate::csrf::generate_token();
        let csrf = crate::csrf::generate_token();
        let now = Instant::now();
        let mut map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        if map.len() >= MAX_SESSIONS {
            self.prune(&mut map, now);
        }
        if map.len() >= MAX_SESSIONS {
            if let Some(oldest) = map
                .iter()
                .min_by_key(|(_, session)| session.created)
                .map(|(id, _)| id.clone())
            {
                map.remove(&oldest);
            }
        }
        map.insert(
            id.clone(),
            Session {
                csrf: csrf.clone(),
                token_digest: crate::sha256::digest(token.as_bytes()),
                created: now,
                last_seen: now,
            },
        );
        SessionRef { id, csrf }
    }

    /// The live session with this id, refreshing its idle timer. An expired session, or one
    /// created under a token that is no longer the configured one, is removed and is `None`.
    pub fn touch(&self, id: &str, token: &str) -> Option<SessionRef> {
        let now = Instant::now();
        let digest = crate::sha256::digest(token.as_bytes());
        let mut map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        let session = map.get_mut(id)?;
        let live = now.duration_since(session.last_seen) < self.idle
            && now.duration_since(session.created) < self.absolute
            && crate::auth::digests_match(&session.token_digest, &digest);
        if !live {
            map.remove(id);
            return None;
        }
        session.last_seen = now;
        Some(SessionRef {
            id: id.to_string(),
            csrf: session.csrf.clone(),
        })
    }

    /// Sign one session out.
    pub fn remove(&self, id: &str) {
        self.map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(id);
    }

    /// How many sessions are held, for tests.
    pub fn len(&self) -> usize {
        self.map.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Whether no session is held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn prune(&self, map: &mut HashMap<String, Session>, now: Instant) {
        map.retain(|_, session| {
            now.duration_since(session.last_seen) < self.idle
                && now.duration_since(session.created) < self.absolute
        });
    }
}

/// The session id a request's cookie carries, if any.
pub fn session_id(headers: &HeaderMap) -> Option<String> {
    let prefix = format!("{COOKIE}=");
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .map(str::trim)
        .find_map(|cookie| cookie.strip_prefix(&prefix))
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// The `Set-Cookie` value that hands a browser its session.
pub fn set_cookie(id: &str, secure: bool) -> String {
    format!(
        "{COOKIE}={id}; Path=/; Max-Age={}; HttpOnly; SameSite=Strict{}",
        ABSOLUTE.as_secs(),
        if secure { "; Secure" } else { "" }
    )
}

/// The `Set-Cookie` values that clear the session cookie and the legacy token cookie.
pub fn clear_cookies(secure: bool) -> [String; 2] {
    let secure = if secure { "; Secure" } else { "" };
    [
        format!("{COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict{secure}"),
        format!("{LEGACY_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict{secure}"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_is_random_and_carries_its_own_csrf_token() {
        let sessions = Sessions::default();
        let one = sessions.create("token");
        let two = sessions.create("token");
        assert_eq!(one.id.len(), 64);
        assert_ne!(one.id, two.id);
        assert_ne!(one.csrf, two.csrf);
        assert_ne!(one.id, one.csrf);
        assert_eq!(sessions.touch(&one.id, "token"), Some(one.clone()));
        assert!(sessions.touch("not-a-session", "token").is_none());
    }

    #[test]
    fn signing_out_and_rotating_the_token_end_a_session() {
        let sessions = Sessions::default();
        let one = sessions.create("token");
        sessions.remove(&one.id);
        assert!(sessions.touch(&one.id, "token").is_none());

        let two = sessions.create("token");
        assert!(sessions.touch(&two.id, "rotated").is_none());
        assert!(
            sessions.touch(&two.id, "token").is_none(),
            "removed, not just refused"
        );
    }

    #[test]
    fn idle_and_absolute_expiry() {
        let sessions = Sessions::new(Duration::from_millis(40), Duration::from_secs(60));
        let idle = sessions.create("token");
        std::thread::sleep(Duration::from_millis(60));
        assert!(sessions.touch(&idle.id, "token").is_none(), "idle expiry");

        let sessions = Sessions::new(Duration::from_secs(60), Duration::from_millis(40));
        let old = sessions.create("token");
        assert!(sessions.touch(&old.id, "token").is_some());
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            sessions.touch(&old.id, "token").is_none(),
            "absolute expiry"
        );
    }

    #[test]
    fn the_cookie_is_read_and_written_strictly() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            "other=1; crosstalk_session=abc; crosstalk_token=t"
                .parse()
                .expect("value"),
        );
        assert_eq!(session_id(&headers).as_deref(), Some("abc"));
        let cookie = set_cookie("abc", false);
        assert!(cookie.contains("HttpOnly"), "{cookie}");
        assert!(cookie.contains("SameSite=Strict"), "{cookie}");
        assert!(!cookie.contains("Secure"), "{cookie}");
        assert!(set_cookie("abc", true).ends_with("; Secure"));
    }
}
