//! Browser sessions: what the login page hands out instead of the master token.
//!
//! A successful sign-in issues a signed cookie, `crosstalk_session` (`HttpOnly;
//! SameSite=Strict`, plus `Secure` over https). Its value is
//! `base64url(id || issued_at || last_seen_bucket || absolute_expiry || mac)`: a random 16-byte
//! id, three big-endian unix-second fields, and an HMAC-SHA256 over them under a random 32-byte
//! key kept in `<data_dir>/session.key`. Nothing about a session is stored while it is live, so
//! a restart signs nobody out, and the cookie holds nothing a leak could use elsewhere.
//!
//! * It expires on its own: 12 hours idle, 7 days at most. `last_seen_bucket` counts 5-minute
//!   buckets, and an active session's cookie is re-issued at most once per bucket.
//! * The MAC also covers a digest of the master token, so rotating the token signs every
//!   session out on its next request.
//! * Signing out records the id in SQLite (`session_revocations`, kept until the cookie would
//!   have expired), so a copied cookie dies on logout, across restarts too.
//! * `crosstalk sessions revoke-all` replaces the key; a running daemon notices the new file on
//!   the next request and every cookie stops verifying.
//! * Each session's CSRF token is `HMAC(key, "csrf" || id)`: per session, and nothing to store.
//!
//! The key file is created `0600` on first start, and the daemon refuses to start while it is
//! accessible to group or others. A key that later goes missing or loose fails closed: every
//! cookie is refused and sign-in answers 503 until it is fixed.

use axum::http::{header, HeaderMap};
use crosstalk_core::store::Store;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

/// The session cookie's name.
pub const COOKIE: &str = "crosstalk_session";
/// The cookie earlier versions set to the raw master token. No longer accepted; the login and
/// logout responses expire it.
pub const LEGACY_COOKIE: &str = "crosstalk_token";
/// A session unused this long is gone.
pub const IDLE: Duration = Duration::from_secs(12 * 60 * 60);
/// A session this old is gone however busy it is.
pub const ABSOLUTE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// An active session's cookie is re-issued at most this often (the `last_seen` resolution).
pub const REFRESH: Duration = Duration::from_secs(5 * 60);
/// The signing key's file name in the data directory.
pub const KEY_FILE: &str = "session.key";
/// The signing key's length.
pub const KEY_BYTES: usize = 32;

const ID_BYTES: usize = 16;
const PAYLOAD_BYTES: usize = ID_BYTES + 3 * 8;
const COOKIE_BYTES: usize = PAYLOAD_BYTES + 32;
/// What the MAC input starts with, so a cookie MAC can never be mistaken for another HMAC
/// under the same key (the CSRF tokens).
const MAC_LABEL: &[u8] = b"crosstalk-session-v1";

/// A live session, as the auth gate hands it to the inner layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRef {
    /// The session id, hex.
    pub id: String,
    /// The synchroniser token this session's forms carry.
    pub csrf: String,
}

/// A cookie that verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    pub session: SessionRef,
    /// A re-issued cookie value with a fresh `last_seen_bucket`, when the old one is a bucket or
    /// more behind, with its `Max-Age`.
    pub refreshed: Option<(String, u64)>,
    /// When the session ends however busy it is (unix seconds).
    pub expires_at: u64,
}

/// A new session: the cookie value and its `Max-Age`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issued {
    pub session: SessionRef,
    pub cookie: String,
    pub max_age: u64,
}

/// What a cookie says, once its MAC checks out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Claims {
    id: [u8; ID_BYTES],
    issued_at: u64,
    last_seen_bucket: u64,
    expires_at: u64,
}

impl Claims {
    fn payload(&self) -> [u8; PAYLOAD_BYTES] {
        let mut out = [0u8; PAYLOAD_BYTES];
        out[..ID_BYTES].copy_from_slice(&self.id);
        out[ID_BYTES..ID_BYTES + 8].copy_from_slice(&self.issued_at.to_be_bytes());
        out[ID_BYTES + 8..ID_BYTES + 16].copy_from_slice(&self.last_seen_bucket.to_be_bytes());
        out[ID_BYTES + 16..].copy_from_slice(&self.expires_at.to_be_bytes());
        out
    }

    fn parse(payload: &[u8]) -> Self {
        let word = |at: usize| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&payload[at..at + 8]);
            u64::from_be_bytes(bytes)
        };
        let mut id = [0u8; ID_BYTES];
        id.copy_from_slice(&payload[..ID_BYTES]);
        Self {
            id,
            issued_at: word(ID_BYTES),
            last_seen_bucket: word(ID_BYTES + 8),
            expires_at: word(ID_BYTES + 16),
        }
    }
}

/// Where the signing key comes from.
#[derive(Debug)]
struct KeyState {
    key: Option<[u8; KEY_BYTES]>,
    /// The key file, for a daemon; `None` for an in-memory key.
    file: Option<PathBuf>,
    /// What the file looked like when it was read, to notice a `revoke-all`.
    stamp: Option<FileStamp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileStamp {
    modified: Option<std::time::SystemTime>,
    len: u64,
    inode: u64,
}

impl FileStamp {
    fn of(metadata: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        let inode = std::os::unix::fs::MetadataExt::ino(metadata);
        #[cfg(not(unix))]
        let inode = 0;
        Self {
            modified: metadata.modified().ok(),
            len: metadata.len(),
            inode,
        }
    }
}

/// The daemon's sessions: the signing key and the revocation list.
pub struct Sessions {
    key: Mutex<KeyState>,
    store: Store,
}

impl std::fmt::Debug for Sessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.key.lock().unwrap_or_else(|p| p.into_inner());
        f.debug_struct("Sessions")
            .field("key", &state.key.map(|_| "<redacted>"))
            .field("file", &state.file)
            .finish_non_exhaustive()
    }
}

impl Sessions {
    /// Sessions under a random key held in memory: they end with the process. For tests and
    /// one-shot contexts; the daemon uses [`Sessions::open`].
    pub fn ephemeral(store: Store) -> Self {
        let mut key = [0u8; KEY_BYTES];
        crate::csrf::random_bytes(&mut key);
        Self {
            key: Mutex::new(KeyState {
                key: Some(key),
                file: None,
                stamp: None,
            }),
            store,
        }
    }

    /// Sessions under the key in `<data_dir>/session.key`, creating it (`0600`) on first start.
    /// Refuses a key file that group or others can access, or that is not a 32-byte file.
    pub fn open(data_dir: &Path, store: Store) -> anyhow::Result<Self> {
        let path = key_path(data_dir);
        let (key, stamp) = load_or_create_key(&path)?;
        Ok(Self {
            key: Mutex::new(KeyState {
                key: Some(key),
                file: Some(path),
                stamp: Some(stamp),
            }),
            store,
        })
    }

    /// The current key, re-read when the file changed since it was last read. `None` when the
    /// file is gone or unsafe: fail closed.
    fn key(&self) -> Option<[u8; KEY_BYTES]> {
        let mut state = self.key.lock().unwrap_or_else(|p| p.into_inner());
        let Some(path) = state.file.clone() else {
            return state.key;
        };
        let stamp = std::fs::metadata(&path)
            .ok()
            .map(|meta| FileStamp::of(&meta));
        if stamp.is_some() && stamp == state.stamp {
            return state.key;
        }
        match read_key(&path) {
            Ok((key, stamp)) => {
                if state.key.is_some() && state.key != Some(key) {
                    tracing::info!(
                        "{} changed: every browser session was signed out",
                        path.display()
                    );
                }
                state.key = Some(key);
                state.stamp = Some(stamp);
            }
            Err(error) => {
                if state.key.is_some() {
                    tracing::error!("refusing every browser session: {error:#}");
                }
                state.key = None;
                state.stamp = stamp;
            }
        }
        state.key
    }

    /// Start a session under the current master token. `None` when the key is unavailable.
    pub fn create(&self, token: &str) -> Option<Issued> {
        self.create_at(token, unix_now())
    }

    /// [`Self::create`] at a given time (unix seconds).
    pub fn create_at(&self, token: &str, now: u64) -> Option<Issued> {
        let key = self.key()?;
        let mut id = [0u8; ID_BYTES];
        crate::csrf::random_bytes(&mut id);
        let claims = Claims {
            id,
            issued_at: now,
            last_seen_bucket: now / REFRESH.as_secs(),
            expires_at: now + ABSOLUTE.as_secs(),
        };
        Some(Issued {
            session: session_ref(&key, &claims),
            cookie: sign(&key, token, &claims),
            max_age: claims.expires_at - now,
        })
    }

    /// The live session a cookie value carries, if its MAC verifies under the current key and
    /// master token, it has not expired, and it was not signed out.
    pub fn verify(&self, cookie: &str, token: &str) -> Option<Verified> {
        self.verify_at(cookie, token, unix_now())
    }

    /// [`Self::verify`] at a given time (unix seconds).
    pub fn verify_at(&self, cookie: &str, token: &str, now: u64) -> Option<Verified> {
        let key = self.key()?;
        let claims = open(&key, token, cookie)?;
        let refresh = REFRESH.as_secs();
        let live = now < claims.expires_at
            && claims.expires_at <= claims.issued_at.saturating_add(ABSOLUTE.as_secs())
            && now
                < claims
                    .last_seen_bucket
                    .saturating_mul(refresh)
                    .saturating_add(IDLE.as_secs());
        if !live {
            return None;
        }
        let session = session_ref(&key, &claims);
        match self.store.session_revoked(&session.id) {
            Ok(false) => {}
            Ok(true) => return None,
            Err(error) => {
                tracing::error!("could not read the session revocations, refusing: {error:#}");
                return None;
            }
        }
        let bucket = now / refresh;
        // Written as a plain `if` rather than a closure: rustc 1.89 (the declared minimum)
        // hits an internal compiler error on a closure capturing `Claims` here.
        let refreshed = if bucket > claims.last_seen_bucket {
            let mut fresh = claims;
            fresh.last_seen_bucket = bucket;
            Some((sign(&key, token, &fresh), claims.expires_at - now))
        } else {
            None
        };
        Some(Verified {
            session,
            refreshed,
            expires_at: claims.expires_at,
        })
    }

    /// Sign a session out: its id is remembered until its cookie would have expired anyway.
    /// A cookie that does not verify is already dead and needs nothing.
    pub fn revoke(&self, cookie: &str, token: &str) -> anyhow::Result<()> {
        let now = unix_now();
        let Some(verified) = self.verify_at(cookie, token, now) else {
            return Ok(());
        };
        self.store.revoke_session(
            &verified.session.id,
            i64::try_from(verified.expires_at).unwrap_or(i64::MAX),
            i64::try_from(now).unwrap_or(i64::MAX),
        )
    }
}

/// Where a data directory keeps the session key.
pub fn key_path(data_dir: &Path) -> PathBuf {
    data_dir.join(KEY_FILE)
}

/// Replace the session key with a fresh one, signing every browser out. What
/// `crosstalk sessions revoke-all` does; a running daemon picks the new key up on its next
/// request.
pub fn rotate_key(data_dir: &Path) -> anyhow::Result<PathBuf> {
    use anyhow::Context;
    crosstalk_core::perms::create_private_dir_all(data_dir)
        .with_context(|| format!("creating {}", data_dir.display()))?;
    let path = key_path(data_dir);
    let temporary = data_dir.join(format!(".{KEY_FILE}.tmp.{}", std::process::id()));
    let _ = std::fs::remove_file(&temporary);
    write_new_key(&temporary)?;
    std::fs::rename(&temporary, &path).with_context(|| format!("replacing {}", path.display()))?;
    Ok(path)
}

fn load_or_create_key(path: &Path) -> anyhow::Result<([u8; KEY_BYTES], FileStamp)> {
    use anyhow::Context;
    if let Some(parent) = path.parent() {
        crosstalk_core::perms::create_private_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    match write_new_key(path) {
        Ok(()) => {
            tracing::info!("created the session key {}", path.display());
        }
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists) => {}
        Err(error) => return Err(error),
    }
    read_key(path)
}

/// Create `path` (which must not exist) holding a fresh key, `0600`.
fn write_new_key(path: &Path) -> anyhow::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(anyhow::Error::from)?;
    let mut key = [0u8; KEY_BYTES];
    crate::csrf::random_bytes(&mut key);
    file.write_all(&key)?;
    file.sync_all()?;
    Ok(())
}

/// Read a key file, refusing one that is not a regular 32-byte file only its owner can access.
fn read_key(path: &Path) -> anyhow::Result<([u8; KEY_BYTES], FileStamp)> {
    use anyhow::Context;
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("reading the session key {}", path.display()))?;
    if !metadata.is_file() {
        anyhow::bail!("the session key {} is not a regular file", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            anyhow::bail!(
                "the session key {} is accessible to other accounts (mode {mode:o}): anyone who \
                 can read it can forge a signed-in session. Run `chmod 600` on it, or delete it \
                 to sign everyone out and have a new one created",
                path.display()
            );
        }
    }
    let bytes = std::fs::read(path)
        .with_context(|| format!("reading the session key {}", path.display()))?;
    let key: [u8; KEY_BYTES] = bytes.as_slice().try_into().map_err(|_| {
        anyhow::anyhow!(
            "the session key {} is {} bytes, not {KEY_BYTES}: delete it to have a new one \
             created (every browser is signed out)",
            path.display(),
            bytes.len()
        )
    })?;
    Ok((key, FileStamp::of(&metadata)))
}

fn mac(key: &[u8; KEY_BYTES], token: &str, payload: &[u8]) -> [u8; 32] {
    let mut message = Vec::with_capacity(MAC_LABEL.len() + 32 + payload.len());
    message.extend_from_slice(MAC_LABEL);
    message.extend_from_slice(&crate::sha256::digest(token.as_bytes()));
    message.extend_from_slice(payload);
    crate::sha256::hmac_sha256(key, &message)
}

fn sign(key: &[u8; KEY_BYTES], token: &str, claims: &Claims) -> String {
    let payload = claims.payload();
    let mut bytes = Vec::with_capacity(COOKIE_BYTES);
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&mac(key, token, &payload));
    base64url_encode(&bytes)
}

/// The claims of a cookie whose MAC verifies (compared in constant time).
fn open(key: &[u8; KEY_BYTES], token: &str, cookie: &str) -> Option<Claims> {
    let bytes = base64url_decode(cookie)?;
    if bytes.len() != COOKIE_BYTES {
        return None;
    }
    let (payload, presented) = bytes.split_at(PAYLOAD_BYTES);
    let presented: [u8; 32] = presented.try_into().ok()?;
    if !crate::sha256::digests_match(&mac(key, token, payload), &presented) {
        return None;
    }
    Some(Claims::parse(payload))
}

fn session_ref(key: &[u8; KEY_BYTES], claims: &Claims) -> SessionRef {
    let mut message = Vec::with_capacity(4 + ID_BYTES);
    message.extend_from_slice(b"csrf");
    message.extend_from_slice(&claims.id);
    SessionRef {
        id: crate::sha256::hex(&claims.id),
        csrf: crate::sha256::hex(&crate::sha256::hmac_sha256(key, &message)),
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

const BASE64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Unpadded base64url.
fn base64url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        let digits = chunk.len() + 1;
        for index in 0..digits {
            out.push(BASE64URL[((n >> (18 - 6 * index)) & 0x3f) as usize] as char);
        }
    }
    out
}

/// Unpadded base64url, strictly: anything else (padding, other alphabets, a dangling
/// character, non-zero trailing bits) is `None`.
fn base64url_decode(text: &str) -> Option<Vec<u8>> {
    let value = |byte: u8| -> Option<u32> {
        BASE64URL
            .iter()
            .position(|digit| *digit == byte)
            .map(|index| index as u32)
    };
    let bytes = text.as_bytes();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut n = 0u32;
        for (index, byte) in chunk.iter().enumerate() {
            n |= value(*byte)? << (18 - 6 * index);
        }
        let produced = chunk.len() - 1;
        let unused_bits = 24 - 8 * produced;
        if n & ((1 << unused_bits) - 1) != 0 && chunk.len() < 4 {
            return None;
        }
        for index in 0..produced {
            out.push((n >> (16 - 8 * index)) as u8);
        }
    }
    Some(out)
}

/// The session cookie's value in a request, if any.
pub fn cookie_value(headers: &HeaderMap) -> Option<String> {
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
pub fn set_cookie(value: &str, max_age: u64, secure: bool) -> String {
    format!(
        "{COOKIE}={value}; Path=/; Max-Age={max_age}; HttpOnly; SameSite=Strict{}",
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

    const NOW: u64 = 1_800_000_000;

    fn sessions() -> Sessions {
        Sessions::ephemeral(Store::in_memory().expect("store"))
    }

    #[test]
    fn a_session_is_random_signed_and_carries_its_own_csrf_token() {
        let sessions = sessions();
        let one = sessions.create_at("token", NOW).expect("issued");
        let two = sessions.create_at("token", NOW).expect("issued");
        assert_eq!(one.session.id.len(), 2 * ID_BYTES);
        assert_ne!(one.session.id, two.session.id);
        assert_ne!(one.session.csrf, two.session.csrf);
        assert_eq!(one.max_age, ABSOLUTE.as_secs());
        let verified = sessions
            .verify_at(&one.cookie, "token", NOW + 1)
            .expect("live");
        assert_eq!(verified.session, one.session);
        assert_eq!(verified.refreshed, None, "same bucket");
        assert!(sessions.verify_at("not-a-session", "token", NOW).is_none());
    }

    #[test]
    fn a_tampered_cookie_does_not_verify() {
        let sessions = sessions();
        let issued = sessions.create_at("token", NOW).expect("issued");
        let mut bytes = base64url_decode(&issued.cookie).expect("decodes");
        // Push the absolute expiry a year out: the MAC no longer matches.
        bytes[ID_BYTES + 16..PAYLOAD_BYTES].copy_from_slice(&(NOW + 365 * 24 * 3600).to_be_bytes());
        assert!(sessions
            .verify_at(&base64url_encode(&bytes), "token", NOW)
            .is_none());
        // Truncated, extended, or under another key.
        assert!(sessions
            .verify_at(&issued.cookie[..issued.cookie.len() - 2], "token", NOW)
            .is_none());
        assert!(sessions
            .verify_at(&format!("{}AA", issued.cookie), "token", NOW)
            .is_none());
        assert!(super::tests::sessions()
            .verify_at(&issued.cookie, "token", NOW)
            .is_none());
    }

    #[test]
    fn idle_and_absolute_expiry() {
        let sessions = sessions();
        let issued = sessions.create_at("token", NOW).expect("issued");
        let idle = IDLE.as_secs();
        assert!(sessions
            .verify_at(&issued.cookie, "token", NOW + idle - REFRESH.as_secs())
            .is_some());
        assert!(
            sessions
                .verify_at(&issued.cookie, "token", NOW + idle + 1)
                .is_none(),
            "idle expiry"
        );

        // Kept busy, it lives to the absolute limit and no further.
        let mut cookie = issued.cookie.clone();
        let mut now = NOW;
        while now + 3600 < NOW + ABSOLUTE.as_secs() {
            now += 3600;
            let verified = sessions.verify_at(&cookie, "token", now).expect("busy");
            cookie = verified.refreshed.expect("re-issued").0;
        }
        assert!(
            sessions
                .verify_at(&cookie, "token", NOW + ABSOLUTE.as_secs())
                .is_none(),
            "absolute expiry"
        );
    }

    #[test]
    fn an_active_cookie_is_refreshed_at_most_once_per_bucket() {
        let sessions = sessions();
        let start = NOW - NOW % REFRESH.as_secs();
        let issued = sessions.create_at("token", start).expect("issued");
        let same = sessions
            .verify_at(&issued.cookie, "token", start + REFRESH.as_secs() - 1)
            .expect("live");
        assert!(same.refreshed.is_none());
        let next = sessions
            .verify_at(&issued.cookie, "token", start + REFRESH.as_secs())
            .expect("live");
        let (cookie, max_age) = next.refreshed.expect("re-issued");
        assert_eq!(max_age, ABSOLUTE.as_secs() - REFRESH.as_secs());
        let again = sessions
            .verify_at(&cookie, "token", start + REFRESH.as_secs() + 10)
            .expect("live");
        assert_eq!(again.session, issued.session, "the same session");
        assert!(again.refreshed.is_none());
    }

    #[test]
    fn rotating_the_token_or_the_key_ends_every_session() {
        let sessions = sessions();
        let issued = sessions.create_at("token", NOW).expect("issued");
        assert!(sessions.verify_at(&issued.cookie, "rotated", NOW).is_none());

        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::in_memory().expect("store");
        let sessions = Sessions::open(dir.path(), store).expect("open");
        let issued = sessions.create("token").expect("issued");
        assert!(sessions.verify(&issued.cookie, "token").is_some());
        rotate_key(dir.path()).expect("rotate");
        assert!(
            sessions.verify(&issued.cookie, "token").is_none(),
            "a running daemon notices the new key"
        );
        let fresh = sessions.create("token").expect("issued");
        assert!(sessions.verify(&fresh.cookie, "token").is_some());
    }

    #[test]
    fn signing_out_is_remembered() {
        let sessions = sessions();
        let issued = sessions.create("token").expect("issued");
        sessions.revoke(&issued.cookie, "token").expect("revoke");
        assert!(sessions.verify(&issued.cookie, "token").is_none());
        let other = sessions.create("token").expect("issued");
        assert!(sessions.verify(&other.cookie, "token").is_some());
    }

    #[cfg(unix)]
    #[test]
    fn the_key_file_is_created_private_and_a_loose_one_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("data");
        Sessions::open(&data, Store::in_memory().expect("store")).expect("open");
        let path = key_path(&data);
        let mode = std::fs::metadata(&path).expect("key").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(std::fs::read(&path).expect("read").len(), KEY_BYTES);

        for loose in [0o640, 0o604, 0o620] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(loose)).expect("chmod");
            let error =
                Sessions::open(&data, Store::in_memory().expect("store")).expect_err("refused");
            assert!(
                format!("{error:#}").contains("accessible to other accounts"),
                "{error:#}"
            );
        }

        // A running daemon whose key goes loose fails closed.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        let sessions = Sessions::open(&data, Store::in_memory().expect("store")).expect("open");
        let issued = sessions.create("token").expect("issued");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        // Touch it so the change is seen even on a coarse clock.
        let key = std::fs::read(&path).expect("read");
        std::fs::remove_file(&path).expect("remove");
        std::fs::write(&path, key).expect("write");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        assert!(sessions.verify(&issued.cookie, "token").is_none());
        assert!(sessions.create("token").is_none());

        // A key of the wrong length is refused too.
        std::fs::write(&path, b"short").expect("write");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        assert!(Sessions::open(&data, Store::in_memory().expect("store")).is_err());
    }

    #[test]
    fn base64url_round_trips_and_is_strict() {
        for len in 0..10 {
            let bytes: Vec<u8> = (0..len).map(|n| (n * 37 + 11) as u8).collect();
            let text = base64url_encode(&bytes);
            assert!(!text.contains('='));
            assert_eq!(base64url_decode(&text), Some(bytes));
        }
        assert_eq!(base64url_encode(b"\xfb\xff"), "-_8");
        assert!(base64url_decode("ab+/").is_none());
        assert!(base64url_decode("abc=").is_none());
        assert!(base64url_decode("a").is_none());
        assert!(base64url_decode("-_9").is_none(), "non-zero trailing bits");
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
        assert_eq!(cookie_value(&headers).as_deref(), Some("abc"));
        let cookie = set_cookie("abc", 60, false);
        assert!(cookie.contains("HttpOnly"), "{cookie}");
        assert!(cookie.contains("SameSite=Strict"), "{cookie}");
        assert!(cookie.contains("Max-Age=60"), "{cookie}");
        assert!(!cookie.contains("Secure"), "{cookie}");
        assert!(set_cookie("abc", 60, true).ends_with("; Secure"));
    }
}
