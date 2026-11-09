//! The `crosstalk-bridge` client: the four writes Dispatcharr's REST API cannot do.
//!
//! The contract is `docs/bridge.md`; the plugin is `bridge/plugin.py`. In short: every request
//! goes through Dispatcharr's plugin-run API (`POST /api/plugins/plugins/<key>/run/`), so it
//! carries the Dispatcharr credential like any other call, and its `params` are a signed
//! envelope — `{"envelope": {"v", "ts", "nonce", "op", "payload"}, "mac": hex}` with the MAC an
//! HMAC-SHA256 under the `[bridge]` secret over [`canonical_json`] of the envelope. The plugin
//! refuses anything unsigned, older than five minutes, replayed, or not one of [`OPS`].
//!
//! Where it plugs in:
//!
//!   * [`BridgeClient`] implements [`StreamListWriter`] and [`GroupClearer`], the seams
//!     [`crate::channel_ops::apply`] writes stream lists and "No Group" moves through;
//!   * [`BridgeClient::set_stream_stats`] is the probe's stats write when the bridge is up;
//!   * [`BridgeClient::upsert_epg`] is the generated-guide write, for phase 4.
//!
//! Whether to use it is decided once per process: `serve` (and a one-shot `run`) ping the
//! plugin at start-up through [`check_and_activate`], which remembers the plugin key and a
//! digest of the secret that answered. [`for_config`] then hands out a client only while the
//! configuration still names that key and secret, so a settings save that changes either
//! falls back to REST until the next restart re-checks it, rather than signing with a secret
//! nobody verified.

use crate::channel_ops::StreamListWriter;
use crate::config::{BridgeConfig, CrosstalkConfig};
use crate::dispatcharr::DispatcharrClient;
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt::Write as _;
use std::sync::RwLock;

/// The envelope version this client signs.
pub const PROTOCOL: u64 = 1;

/// The oldest plugin version this crosstalk speaks to, and `[bridge] min_version`'s default.
pub const MIN_PLUGIN_VERSION: &str = "0.1.0";

/// The shortest secret the plugin accepts.
pub const MIN_SECRET_CHARS: usize = 32;

/// Every operation the plugin allows; anything else is refused before it runs.
pub const OPS: [&str; 5] = [
    "ping",
    "set_stream_lists",
    "set_stream_stats",
    "clear_channel_groups",
    "upsert_epg",
];

/// The writes crosstalk needs the plugin to have.
const REQUIRED_OPS: [&str; 4] = [
    "set_stream_lists",
    "set_stream_stats",
    "clear_channel_groups",
    "upsert_epg",
];

/// Per-request caps, the same numbers as `LIMITS` in `bridge/plugin.py`. The client chunks to
/// stay inside them, and also keeps each request body under [`MAX_BODY_BYTES`].
pub const MAX_CHANNELS: usize = 2_000;
pub const MAX_LIST_LEN: usize = 1_000;
pub const MAX_STREAM_REFS: usize = 20_000;
pub const MAX_STREAMS: usize = 5_000;
pub const MAX_STATS_BYTES: usize = 16_384;
pub const MAX_EPG_ENTRIES: usize = 2_000;
pub const MAX_PROGRAMS: usize = 50_000;

/// A request body this client will not exceed: Django refuses bodies past
/// `DATA_UPLOAD_MAX_MEMORY_SIZE` (2.5 MB by default), and a reverse proxy may stop sooner.
pub const MAX_BODY_BYTES: usize = 1_000_000;

/// Integers outside ±(2^53 − 1) have no canonical form: not every JSON reader keeps them exact.
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
const MAX_DEPTH: usize = 32;

// ── canonical JSON ──────────────────────────────────────────────────────────────────────────

/// The canonical JSON text of `value` (docs/bridge.md, "Canonical JSON"), byte for byte what
/// `canonical_json` in `bridge/plugin.py` produces:
///
///   * no whitespace; object keys sorted by code point (the order of their UTF-8 bytes);
///   * strings escape `"` and `\`, write `\b \f \n \r \t`, write any other character below
///     U+0020 as `\u00xx` (lowercase), and everything else as itself in UTF-8;
///   * integers in ±(2^53 − 1) as plain decimals; anything outside is an error;
///   * other numbers as ECMAScript's `Number.prototype.toString` writes them (RFC 8785): the
///     shortest digits that read back as the same double, `1e+21` / `1e-7` past the thresholds,
///     `2` for `2.0` and `0` for `-0.0`; NaN and infinities are errors.
///
/// `bridge/tests/vectors.json` pins both implementations.
pub fn canonical_json(value: &Value) -> Result<String> {
    let mut out = String::new();
    encode(value, &mut out, 0)?;
    Ok(out)
}

fn encode(value: &Value, out: &mut String, depth: usize) -> Result<()> {
    if depth > MAX_DEPTH {
        bail!("nested deeper than {MAX_DEPTH} levels");
    }
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => out.push_str(&number_text(number)?),
        Value::String(text) => push_string(text, out),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                encode(item, out, depth + 1)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                push_string(key, out);
                out.push(':');
                encode(&map[key.as_str()], out, depth + 1)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn push_string(text: &str, out: &mut String) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{0c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            control if u32::from(control) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(control));
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

fn number_text(number: &serde_json::Number) -> Result<String> {
    if let Some(integer) = number.as_i64() {
        if integer.unsigned_abs() > MAX_SAFE_INTEGER {
            bail!("integer {integer} is outside ±(2^53 - 1)");
        }
        return Ok(integer.to_string());
    }
    if let Some(integer) = number.as_u64() {
        if integer > MAX_SAFE_INTEGER {
            bail!("integer {integer} is outside ±(2^53 - 1)");
        }
        return Ok(integer.to_string());
    }
    let float = number
        .as_f64()
        .with_context(|| format!("{number} is not a number"))?;
    float_text(float)
}

/// A finite double as ECMAScript's `Number.prototype.toString` writes it.
fn float_text(float: f64) -> Result<String> {
    if !float.is_finite() {
        bail!("NaN and infinities have no JSON form");
    }
    if float == 0.0 {
        return Ok("0".to_string());
    }
    let sign = if float < 0.0 { "-" } else { "" };
    // `{:e}` is the shortest text that reads back as the same double (`1.5e16`); only its
    // layout differs from ECMAScript's, so take its digits and exponent and lay them out again.
    let shortest = format!("{:e}", float.abs());
    let (mantissa, exponent) = shortest
        .split_once('e')
        .with_context(|| format!("unexpected float text {shortest:?}"))?;
    let exponent: i64 = exponent
        .parse()
        .with_context(|| format!("unexpected float text {shortest:?}"))?;
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    // value = 0.<digits> x 10^point
    let mut point = whole.len() as i64 + exponent;
    let stripped = digits.trim_start_matches('0');
    point -= (digits.len() - stripped.len()) as i64;
    let digits = stripped.trim_end_matches('0');
    Ok(format!("{sign}{}", layout(digits, point)))
}

fn layout(digits: &str, point: i64) -> String {
    let count = digits.len() as i64;
    if (count..=21).contains(&point) {
        format!("{digits}{}", "0".repeat((point - count) as usize))
    } else if (1..=21).contains(&point) {
        let (head, tail) = digits.split_at(point as usize);
        format!("{head}.{tail}")
    } else if (-5..=0).contains(&point) {
        format!("0.{}{digits}", "0".repeat(point.unsigned_abs() as usize))
    } else {
        let exponent = point - 1;
        let (head, tail) = digits.split_at(1);
        let mantissa = if tail.is_empty() {
            head.to_string()
        } else {
            format!("{head}.{tail}")
        };
        let sign = if exponent > 0 { "+" } else { "-" };
        format!("{mantissa}e{sign}{}", exponent.unsigned_abs())
    }
}

/// The lowercase hex HMAC-SHA256 of `envelope`'s canonical JSON under `secret`.
pub fn sign(secret: &str, envelope: &Value) -> Result<String> {
    let message = canonical_json(envelope).context("the envelope has no canonical form")?;
    Ok(crate::sha256::hex(&crate::sha256::hmac_sha256(
        secret.as_bytes(),
        message.as_bytes(),
    )))
}

// ── secrets, versions, keys ─────────────────────────────────────────────────────────────────

/// `N` bytes from the operating system's CSPRNG. An error, never a weaker fallback: these are
/// the bridge secret and request nonces.
fn random_bytes<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0u8; N];
    #[cfg(unix)]
    {
        use std::io::Read;
        std::fs::File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut bytes))
            .context("reading /dev/urandom")?;
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        let _ = &mut bytes;
        bail!("the bridge needs /dev/urandom, which this platform does not have")
    }
}

/// A new bridge secret: 32 random bytes, hex (64 characters).
pub fn generate_secret() -> Result<String> {
    Ok(crate::sha256::hex(&random_bytes::<32>()?))
}

/// `major.minor.patch` from `0.1.0`, `v0.1.0` or `0.1.0-rc.1` (the suffix is ignored).
pub fn parse_version(text: &str) -> Option<(u64, u64, u64)> {
    let text = text.trim();
    let text = text.strip_prefix('v').unwrap_or(text);
    let core = text.split(['-', '+']).next()?;
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().map_or(Some(0), |part| part.parse().ok())?;
    let patch = parts.next().map_or(Some(0), |part| part.parse().ok())?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

/// The other spelling of a plugin key: `crosstalk_bridge` for `crosstalk-bridge` and back.
///
/// Dispatcharr's zip import turns `-` into `_` in the folder name, while a folder copied in by
/// hand keeps its `-`; the start-up check tries both before calling the plugin missing.
pub fn alternate_plugin_key(key: &str) -> Option<String> {
    let other = if key.contains('_') {
        key.replace('_', "-")
    } else {
        key.replace('-', "_")
    };
    (other != key).then_some(other)
}

/// Whether `key` is one URL path segment, the shape Dispatcharr's run route takes.
pub fn valid_plugin_key(key: &str) -> bool {
    !key.is_empty()
        && key.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}

// ── the client ──────────────────────────────────────────────────────────────────────────────

/// What the plugin answers, for every operation.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BridgeResponse {
    #[serde(default)]
    pub ok: bool,
    #[serde(default)]
    pub op: Option<String>,
    /// Rows written.
    #[serde(default)]
    pub applied: u64,
    /// Ids already in the requested state: answered, nothing written.
    #[serde(default)]
    pub skipped: Vec<Value>,
    /// Ids left untouched, each with its reason; an `id` of `null` refuses the whole request.
    #[serde(default)]
    pub errors: Vec<BridgeRowError>,
    #[serde(default)]
    pub version: String,
    /// `ping` only.
    #[serde(default)]
    pub protocol: Option<u64>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// `ping` only: which of the plugin's secrets verified the request (`current`/`previous`).
    #[serde(default)]
    pub key: Option<String>,
    /// `ping` only: the rotation grace (`none`, `active`, `expired`, `invalid`).
    #[serde(default)]
    pub grace: Option<String>,
    /// `ping` only: where the plugin's current secret comes from (`env`, `settings`, `none`).
    #[serde(default)]
    pub configured: Option<String>,
}

/// One id the plugin did not write, and why.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BridgeRowError {
    #[serde(default)]
    pub id: Value,
    #[serde(default)]
    pub reason: String,
}

impl BridgeResponse {
    /// The reasons a whole request was refused, or every reason when none is whole-request.
    fn refusal(&self) -> String {
        let reasons: Vec<&str> = self
            .errors
            .iter()
            .filter(|error| error.id.is_null())
            .map(|error| error.reason.as_str())
            .collect();
        if reasons.is_empty() {
            "no reason given".to_string()
        } else {
            reasons.join("; ")
        }
    }
}

/// One guide entry for [`BridgeClient::upsert_epg`]: the `EPGData` row keyed by
/// `(tvg_id, epg_source)` and its programmes inside the request's window.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EpgEntryUpsert {
    pub tvg_id: String,
    pub name: String,
    pub icon_url: Option<String>,
    pub programs: Vec<EpgProgram>,
}

/// One programme, unix seconds. Its `start` must fall inside the request's window.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EpgProgram {
    pub start: i64,
    pub end: i64,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_properties: Option<Value>,
}

/// A signed-request client for the bridge plugin.
#[derive(Clone)]
pub struct BridgeClient {
    client: DispatcharrClient,
    plugin_key: String,
    secret: String,
}

impl std::fmt::Debug for BridgeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeClient")
            .field("plugin_key", &self.plugin_key)
            .field("secret", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl BridgeClient {
    /// A client for the plugin `config` names, signing with its resolved secret.
    pub fn new(client: DispatcharrClient, config: &BridgeConfig) -> Result<Self> {
        let secret = config
            .resolved_secret()
            .context("no bridge secret: run `crosstalk bridge secret`")?;
        if secret.chars().count() < MIN_SECRET_CHARS {
            bail!(
                "the bridge secret is shorter than {MIN_SECRET_CHARS} characters, which the \
                 plugin refuses: run `crosstalk bridge rotate`"
            );
        }
        if !valid_plugin_key(&config.plugin_key) {
            bail!(
                "bridge.plugin_key {:?} is not a plugin key",
                config.plugin_key
            );
        }
        Ok(Self {
            client,
            plugin_key: config.plugin_key.clone(),
            secret,
        })
    }

    /// The same client, addressing the plugin under another key.
    fn with_plugin_key(mut self, plugin_key: &str) -> Self {
        self.plugin_key = plugin_key.to_string();
        self
    }

    /// The key requests go to.
    pub fn plugin_key(&self) -> &str {
        &self.plugin_key
    }

    /// The `params` of one request: the envelope and its MAC.
    pub fn signed_params(&self, op: &str, payload: Value) -> Result<Value> {
        let nonce = crate::sha256::hex(&random_bytes::<16>()?);
        self.signed_params_at(op, payload, chrono::Utc::now().timestamp(), &nonce)
    }

    fn signed_params_at(&self, op: &str, payload: Value, ts: i64, nonce: &str) -> Result<Value> {
        let envelope = json!({
            "v": PROTOCOL,
            "ts": ts,
            "nonce": nonce,
            "op": op,
            "payload": payload,
        });
        let mac = sign(&self.secret, &envelope)?;
        Ok(json!({ "envelope": envelope, "mac": mac }))
    }

    /// Send one signed operation. An error is a request the plugin did not process at all:
    /// transport, Dispatcharr's own refusal (not installed, disabled), or the plugin's
    /// whole-request refusal (bad MAC, stale, too large...), whose reason it carries.
    pub async fn call(&self, op: &str, payload: Value) -> Result<BridgeResponse> {
        let params = self.signed_params(op, payload)?;
        let result = self
            .client
            .run_plugin_action(&self.plugin_key, op, &params)
            .await
            .context("calling the crosstalk-bridge plugin")?;
        let response: BridgeResponse = serde_json::from_value(result).with_context(|| {
            format!(
                "the plugin {:?} did not answer like the crosstalk bridge",
                self.plugin_key
            )
        })?;
        if !response.ok {
            bail!(
                "the crosstalk bridge refused {op}: {}",
                crate::redact::urls_to_hosts(&response.refusal())
            );
        }
        Ok(response)
    }

    /// `ping`: the plugin's version, protocol and capabilities. Writes nothing.
    pub async fn ping(&self) -> Result<BridgeResponse> {
        self.call("ping", json!({})).await
    }

    /// Ping and check the answer against what this crosstalk needs.
    pub async fn check(&self, min_version: &str) -> Result<BridgeResponse> {
        let response = self.ping().await?;
        check_ping(&response, min_version)?;
        Ok(response)
    }

    /// Replace each channel's stream list, in playback order: one answer per channel, in input
    /// order. Chunked to the plugin's caps; a failed chunk fails its own channels only.
    pub async fn set_stream_lists(&self, lists: &[(i64, Vec<i64>)]) -> Vec<(i64, Result<()>)> {
        let mut answers: Vec<Option<Result<()>>> = lists.iter().map(|_| None).collect();
        let mut sendable: Vec<usize> = Vec::new();
        for (index, (_, streams)) in lists.iter().enumerate() {
            if streams.len() > MAX_LIST_LEN {
                answers[index] = Some(Err(anyhow!(
                    "a list of {} streams is longer than the bridge's {MAX_LIST_LEN}",
                    streams.len()
                )));
            } else {
                sendable.push(index);
            }
        }
        let items: Vec<Value> = sendable
            .iter()
            .map(|index| json!({"channel_id": lists[*index].0, "stream_ids": lists[*index].1}))
            .collect();
        let weights: Vec<usize> = sendable.iter().map(|index| lists[*index].1.len()).collect();
        for range in chunk_ranges(&items, &weights, MAX_CHANNELS, MAX_STREAM_REFS) {
            let ids: Vec<Value> = sendable[range.clone()]
                .iter()
                .map(|index| Value::from(lists[*index].0))
                .collect();
            let payload = json!({ "lists": items[range.clone()] });
            let outcome = self.call("set_stream_lists", payload).await;
            for (position, result) in row_results(&ids, outcome).into_iter().enumerate() {
                answers[sendable[range.start + position]] = Some(result);
            }
        }
        lists
            .iter()
            .zip(answers)
            .map(|((channel_id, _), answer)| {
                (
                    *channel_id,
                    answer.unwrap_or_else(|| Err(anyhow!("not sent to the bridge"))),
                )
            })
            .collect()
    }

    /// Set each stream's `stream_stats` (an object, or `null`) and stamp
    /// `stream_stats_updated_at` with now: one answer per stream, in input order.
    pub async fn set_stream_stats(&self, stats: &[(i64, Value)]) -> Vec<(i64, Result<()>)> {
        let now = chrono::Utc::now().timestamp();
        let mut answers: Vec<Option<Result<()>>> = stats.iter().map(|_| None).collect();
        let mut sendable: Vec<usize> = Vec::new();
        for (index, (_, value)) in stats.iter().enumerate() {
            let problem = if value.is_object() || value.is_null() {
                match canonical_json(value) {
                    Ok(text) if text.len() > MAX_STATS_BYTES => Some(format!(
                        "these stats are {} bytes, over the bridge's {MAX_STATS_BYTES}",
                        text.len()
                    )),
                    Ok(_) => None,
                    Err(error) => Some(format!("these stats cannot be signed: {error:#}")),
                }
            } else {
                Some("stream stats must be an object or null".to_string())
            };
            match problem {
                Some(problem) => answers[index] = Some(Err(anyhow!(problem))),
                None => sendable.push(index),
            }
        }
        let items: Vec<Value> = sendable
            .iter()
            .map(|index| {
                json!({
                    "stream_id": stats[*index].0,
                    "stream_stats": stats[*index].1,
                    "updated_at": now,
                })
            })
            .collect();
        let weights = vec![0; items.len()];
        for range in chunk_ranges(&items, &weights, MAX_STREAMS, usize::MAX) {
            let ids: Vec<Value> = sendable[range.clone()]
                .iter()
                .map(|index| Value::from(stats[*index].0))
                .collect();
            let payload = json!({ "stats": items[range.clone()] });
            let outcome = self.call("set_stream_stats", payload).await;
            for (position, result) in row_results(&ids, outcome).into_iter().enumerate() {
                answers[sendable[range.start + position]] = Some(result);
            }
        }
        stats
            .iter()
            .zip(answers)
            .map(|((stream_id, _), answer)| {
                (
                    *stream_id,
                    answer.unwrap_or_else(|| Err(anyhow!("not sent to the bridge"))),
                )
            })
            .collect()
    }

    /// Move each channel to "No Group" (`channel_group` NULL): one answer per channel.
    pub async fn clear_channel_groups(&self, channel_ids: &[i64]) -> Vec<(i64, Result<()>)> {
        let items: Vec<Value> = channel_ids.iter().map(|id| Value::from(*id)).collect();
        let weights = vec![0; items.len()];
        let mut answers = Vec::with_capacity(channel_ids.len());
        for range in chunk_ranges(&items, &weights, MAX_CHANNELS, usize::MAX) {
            let ids = &items[range.clone()];
            let outcome = self
                .call("clear_channel_groups", json!({ "channel_ids": ids }))
                .await;
            for (id, result) in channel_ids[range].iter().zip(row_results(ids, outcome)) {
                answers.push((*id, result));
            }
        }
        answers
    }

    /// Create or update guide entries in one EPG source and replace their programmes that
    /// start inside `window` (unix seconds, `start..end`): one answer per entry, by `tvg_id`.
    ///
    /// The plugin refuses a source its operator has not listed in its `epg_sources` setting.
    pub async fn upsert_epg(
        &self,
        epg_source_id: i64,
        window: (i64, i64),
        entries: &[EpgEntryUpsert],
    ) -> Vec<(String, Result<()>)> {
        let items: Vec<Value> = entries
            .iter()
            .map(|entry| serde_json::to_value(entry).unwrap_or(Value::Null))
            .collect();
        let weights: Vec<usize> = entries.iter().map(|entry| entry.programs.len()).collect();
        let mut answers = Vec::with_capacity(entries.len());
        for range in chunk_ranges(&items, &weights, MAX_EPG_ENTRIES, MAX_PROGRAMS) {
            let ids: Vec<Value> = entries[range.clone()]
                .iter()
                .map(|entry| Value::from(entry.tvg_id.clone()))
                .collect();
            let payload = json!({
                "epg_source_id": epg_source_id,
                "window": {"start": window.0, "end": window.1},
                "entries": items[range.clone()],
            });
            let outcome = self.call("upsert_epg", payload).await;
            for (entry, result) in entries[range].iter().zip(row_results(&ids, outcome)) {
                answers.push((entry.tvg_id.clone(), result));
            }
        }
        answers
    }
}

/// Whether a ping answer is a bridge this crosstalk can use.
fn check_ping(response: &BridgeResponse, min_version: &str) -> Result<()> {
    if response.protocol != Some(PROTOCOL) {
        bail!(
            "the plugin speaks bridge protocol {}, and this crosstalk speaks {PROTOCOL}",
            response
                .protocol
                .map_or_else(|| "(none)".to_string(), |protocol| protocol.to_string())
        );
    }
    let wanted = parse_version(min_version)
        .with_context(|| format!("bridge.min_version {min_version:?} is not a version"))?;
    let have = parse_version(&response.version).with_context(|| {
        format!(
            "the plugin reports version {:?}, which is not a version",
            response.version
        )
    })?;
    if have < wanted {
        bail!(
            "the plugin is version {}, older than bridge.min_version {min_version}: install \
             the crosstalk-bridge zip from this crosstalk's release",
            response.version
        );
    }
    let missing: Vec<&str> = REQUIRED_OPS
        .iter()
        .copied()
        .filter(|op| !response.capabilities.iter().any(|have| have == op))
        .collect();
    if !missing.is_empty() {
        bail!(
            "the plugin does not offer {}: install the crosstalk-bridge zip from this \
             crosstalk's release",
            missing.join(", ")
        );
    }
    Ok(())
}

/// Consecutive ranges of `items` with at most `max_items` items, at most `max_weight` total
/// weight and at most [`MAX_BODY_BYTES`] of serialized items each. An item that alone breaks a
/// cap still gets a range of its own: the plugin then refuses it, attributed to its rows.
fn chunk_ranges(
    items: &[Value],
    weights: &[usize],
    max_items: usize,
    max_weight: usize,
) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut weight = 0usize;
    let mut bytes = 0usize;
    for (index, item) in items.iter().enumerate() {
        let item_bytes = serde_json::to_string(item).map_or(0, |text| text.len()) + 1;
        let item_weight = weights.get(index).copied().unwrap_or(0);
        let full = index - start >= max_items
            || weight.saturating_add(item_weight) > max_weight
            || bytes + item_bytes > MAX_BODY_BYTES;
        if index > start && full {
            ranges.push(start..index);
            start = index;
            weight = 0;
            bytes = 0;
        }
        weight = weight.saturating_add(item_weight);
        bytes += item_bytes;
    }
    if start < items.len() {
        ranges.push(start..items.len());
    }
    ranges
}

/// One result per id from a chunk's answer.
///
/// An id in `errors` failed with its reason; every other id landed or was already so. The
/// plugin answers every id it was sent exactly once (counted in `applied`, listed in `skipped`
/// or in `errors`); an answer that does not add up fails the whole chunk, because there is
/// no telling which rows it covers.
fn row_results(ids: &[Value], outcome: Result<BridgeResponse>) -> Vec<Result<()>> {
    let response = match outcome {
        Ok(response) => response,
        Err(error) => {
            let message = format!("{error:#}");
            return ids.iter().map(|_| Err(anyhow!(message.clone()))).collect();
        }
    };
    let failed: std::collections::HashMap<String, &str> = response
        .errors
        .iter()
        .filter(|error| !error.id.is_null())
        .map(|error| (error.id.to_string(), error.reason.as_str()))
        .collect();
    let answered = response.applied as usize + response.skipped.len() + failed.len();
    if answered != ids.len() {
        let message = format!(
            "the crosstalk bridge answered {answered} result(s) for {} row(s), so these rows' \
             outcome is unknown: re-run to converge",
            ids.len()
        );
        return ids.iter().map(|_| Err(anyhow!(message.clone()))).collect();
    }
    ids.iter()
        .map(|id| match failed.get(&id.to_string()) {
            Some(reason) => Err(anyhow!(
                "the crosstalk bridge did not write it: {}",
                crate::redact::urls_to_hosts(reason)
            )),
            None => Ok(()),
        })
        .collect()
}

#[async_trait::async_trait]
impl StreamListWriter for BridgeClient {
    async fn write_many(&self, lists: &[(i64, Vec<i64>)]) -> Vec<(i64, Result<()>)> {
        self.set_stream_lists(lists).await
    }
}

#[async_trait::async_trait]
impl crate::channel_ops::GroupClearer for BridgeClient {
    async fn clear_many(&self, channel_ids: &[i64]) -> Vec<(i64, Result<()>)> {
        self.clear_channel_groups(channel_ids).await
    }
}

// ── start-up check and the process-wide verdict ─────────────────────────────────────────────

/// What the start-up check found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeStatus {
    /// `[bridge] enabled = false`: REST only, as without a bridge.
    Disabled,
    /// The plugin answered a signed ping at a usable version.
    Ready {
        version: String,
        /// The key that answered: the configured one, or its other spelling.
        plugin_key: String,
        /// Set when the plugin verified crosstalk's secret only as its `previous_secret`.
        note: Option<String>,
    },
    /// Enabled but unusable; bridge-backed paths fall back to REST, or report that they need
    /// the bridge, for this run of the process.
    Unavailable { reason: String },
}

impl BridgeStatus {
    /// One line for the log and `validate`.
    pub fn describe(&self) -> String {
        match self {
            Self::Disabled => "bridge: disabled ([bridge] enabled = false)".to_string(),
            Self::Ready {
                version,
                plugin_key,
                note,
            } => match note {
                Some(note) => {
                    format!("bridge: crosstalk-bridge {version} ready as {plugin_key:?} ({note})")
                }
                None => format!("bridge: crosstalk-bridge {version} ready as {plugin_key:?}"),
            },
            Self::Unavailable { reason } => format!(
                "bridge: NOT used this run, falling back to REST where REST can do the write: \
                 {reason}"
            ),
        }
    }
}

/// Ping the plugin `config` names and judge the answer. No side effects: `validate` uses it.
pub async fn check(config: &CrosstalkConfig, client: &DispatcharrClient) -> BridgeStatus {
    if !config.bridge.enabled {
        return BridgeStatus::Disabled;
    }
    let bridge = match BridgeClient::new(client.clone(), &config.bridge) {
        Ok(bridge) => bridge,
        Err(error) => {
            return BridgeStatus::Unavailable {
                reason: format!("{error:#}"),
            }
        }
    };
    let configured_key = config.bridge.plugin_key.clone();
    let mut answered = bridge.check(&config.bridge.min_version).await;
    let mut answered_key = configured_key.clone();
    // Not installed under the configured key: Dispatcharr answers 404. The other spelling is
    // the same plugin installed by hand (or by the zip import), so try it before giving up.
    if let Err(error) = &answered {
        let missing =
            crate::dispatcharr::http_status(error) == Some(reqwest::StatusCode::NOT_FOUND);
        if let (true, Some(other)) = (missing, alternate_plugin_key(&configured_key)) {
            let retry = bridge
                .clone()
                .with_plugin_key(&other)
                .check(&config.bridge.min_version)
                .await;
            if retry.is_ok() {
                answered = retry;
                answered_key = other;
            }
        }
    }
    match answered {
        Ok(response) => {
            let mut notes = Vec::new();
            if answered_key != configured_key {
                notes.push(format!(
                    "no plugin answers as {configured_key:?}, {answered_key:?} did: set \
                     bridge.plugin_key = {answered_key:?}"
                ));
            }
            if response.key.as_deref() == Some("previous") {
                notes.push(
                    "the plugin accepted this secret as its previous secret: finish the \
                     rotation by making it the plugin's current secret"
                        .to_string(),
                );
            }
            if let Some(source) = response.configured.as_deref() {
                notes.push(format!("plugin secret configured: {source}"));
            }
            BridgeStatus::Ready {
                version: response.version,
                plugin_key: answered_key,
                note: (!notes.is_empty()).then(|| notes.join("; ")),
            }
        }
        Err(error) => BridgeStatus::Unavailable {
            reason: format!("{error:#}"),
        },
    }
}

/// [`check`], then remember the verdict for [`for_config`] and log it.
pub async fn check_and_activate(
    config: &CrosstalkConfig,
    client: &DispatcharrClient,
) -> BridgeStatus {
    let status = check(config, client).await;
    let verified = match &status {
        BridgeStatus::Ready { plugin_key, .. } => {
            config.bridge.resolved_secret().map(|secret| Verified {
                configured_key: config.bridge.plugin_key.clone(),
                answered_key: plugin_key.clone(),
                secret_digest: crate::sha256::digest(secret.as_bytes()),
            })
        }
        _ => None,
    };
    *VERIFIED
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = verified;
    match &status {
        BridgeStatus::Disabled | BridgeStatus::Ready { .. } => {
            tracing::info!("{}", status.describe());
        }
        BridgeStatus::Unavailable { .. } => tracing::warn!("{}", status.describe()),
    }
    status
}

/// The plugin key and secret digest the start-up ping verified.
#[derive(Debug, Clone)]
struct Verified {
    /// `[bridge] plugin_key` when it was checked.
    configured_key: String,
    /// The key that answered, which requests go to.
    answered_key: String,
    secret_digest: [u8; 32],
}

static VERIFIED: RwLock<Option<Verified>> = RwLock::new(None);

/// A bridge client for this configuration, when it is enabled and names the plugin key and
/// secret the start-up check verified; `None` means "use REST".
pub fn for_config(config: &CrosstalkConfig, client: &DispatcharrClient) -> Option<BridgeClient> {
    if !config.bridge.enabled {
        return None;
    }
    let secret = config.bridge.resolved_secret()?;
    let verified = VERIFIED
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()?;
    let same = verified.configured_key == config.bridge.plugin_key
        && crate::sha256::digests_match(
            &verified.secret_digest,
            &crate::sha256::digest(secret.as_bytes()),
        );
    if !same {
        return None;
    }
    BridgeClient::new(client.clone(), &config.bridge)
        .ok()
        .map(|bridge| bridge.with_plugin_key(&verified.answered_key))
}

#[cfg(test)]
mod tests;
