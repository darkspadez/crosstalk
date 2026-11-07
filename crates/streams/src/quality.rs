//! The one quality model: Stream-Mapparr's sort key, and the throughput evidence it sorts on.
//!
//! `sort` and `match` both sort with this key (`match` uses it to finalise a channel's list), so
//! the key lives here once rather than in each action. Everything below is a statement-for-
//! statement port of `Plugin._sort_streams_by_quality` (`reference/Stream-Mapparr/Stream-Mapparr/
//! plugin.py:4739`) and the helpers it calls, cited line by line. There is one deliberate
//! difference, marked [`QualityModel::now`]: time comes from the caller, so a run's order is
//! reproducible and the parity harness can compare a stale cache entry against the same `now`.
//!
//! # The key
//!
//! Python sorts with a tuple, so every component is compared left to right and the sort is
//! STABLE (equal keys keep the input order). Flattened, with the two `prioritize_quality`
//! orders written out (4739-4823):
//!
//! ```text
//! content_starved      (0, 1)   the run's `demote_content_starved`, see [_is_content_starved]
//! throughput_tier      (0..3)   healthy, marginal, unknown, insufficient (throughput on only)
//! tier                 (0..3)   0 HD+30fps, 1 HD or 30fps, 2 SD, 3 no resolution at all
//! m3u_priority         (0..999) position of the stream's account in `selected_m3us`
//!   -- the two above swap when `prioritize_quality` is off (4788-4803)
//! audio_channels_rank  (0..n)   `_audio_rank` against `audio_channels_priority`
//! audio_codec_rank     (0..n)   `_audio_rank` against `audio_codec_priority`
//! -width x -height     negative pixel count: the larger picture first
//! -source_fps          the higher frame rate first
//! bitrate_measured     (0, 1)   0 a bitrate was measured, 1 none was (step > 0 only)
//! -bitrate // step     negative rounded-down bitrate: the higher step first
//! -throughput_mbps     the faster measured stream first (throughput on only)
//! ```
//!
//! A stream with no usable resolution (`width` or `height` 0) always takes tier 3 and ranks
//! `(0, 0)` for pixels and frame rate, so with throughput sorting OFF it sorts behind every
//! stream that does state a resolution. With throughput sorting ON the tier is prefixed to the
//! whole key (4786-4796), so a healthy measurement still puts it ahead of a stream whose
//! measurement is insufficient — and it appends `0.0` rather than its rate, so its rate only
//! ever breaks a tie between two such streams.
//!
//! # Where the media facts come from
//!
//! `stream_stats`, the JSON Dispatcharr stores per stream, which IPTV Checker writes
//! (`iptv_checker/plugin.py:5276` for an alive stream, `:4971` for the all-null metadata a
//! dead one is merged into `{}` with). Keys read, and how a bad value degrades:
//!
//! | key | read by | garbage value |
//! |---|---|---|
//! | `width`, `height` | [`dimensions`] | `int()` semantics, unparseable reads 0 |
//! | `resolution` | [`dimensions`] | only `"1920x1080"` style strings are parsed |
//! | `source_fps` | [`frame_rate`] | SM raises and the whole sort errors; here it reads 0 |
//! | `video_bitrate`, `ffmpeg_output_bitrate` | [`bitrate_kbps`] | skipped, not measured |
//! | `audio_channels` | [`audio_rank`] | stringified, so `6` becomes `"6"` |
//! | `audio_codec` | [`audio_rank`] | stringified, lowercased |
//!
//! `source_fps` in the tier path is the one place the port is deliberately more forgiving:
//! `fps = stats.get('source_fps', 0) or 0` is used as a number in `fps >= 30` (4790) and in
//! `-fps` (4797), so a provider that wrote `"29.97"` as a string makes the plugin's whole sort
//! action fail. A numeric string reads as its number here — the order the plugin would give
//! the same stat written numerically — and only a value nothing can read becomes 0, which is
//! what the plugin's own `_stats_dimensions` does for width and height. The NOMINAL path is
//! not forgiving and needs
//! none: `_estimate_nominal_bitrate` runs `float(fps or 0)` in a `try` that never raises, so a
//! numeric string takes its fps band on both sides and only truthy garbage falls back — the
//! private `estimate_fps` mirrors that coercion exactly. (The parity harness keeps both string
//! shapes out of the corpus all the same, because the tier path gives the plugin no answer to
//! compare against.)
//!
//! The throughput entry has two more crash-only divergences, both verified against the plugin
//! source: `nominal_bitrate_mbps` as a non-empty string crashes the plugin at `nominal <= 0`
//! (4994, outside the `try`), and `throughput_mbps` as a non-empty string crashes it at
//! `float(entry.get('throughput_mbps') or 0.0)` (4811, uncaught). Here the first is parsed when
//! it parses and re-estimated when it does not, and the second reads 0.0 — the plugin's own
//! answer for a missing measurement. A probe never writes either shape; only a hand-edited
//! cache could carry one.
//!
//! # Throughput
//!
//! The plugin reads its own JSON cache (`/data/stream_mapparr_throughput_cache.json`, written by
//! `probe_throughput` at 10437-10445) behind a TTL. crosstalk reads the store's `stream_health`
//! table instead, which is the same evidence in the same place the rest of the app keeps what it
//! knows ([`StoreThroughputSource`]); [`MemoryThroughputSource`] is what tests and the parity
//! harness use. Both are behind [`ThroughputSource`] so a run can sort without a store at all —
//! every entry then reads "unknown", which is the plugin's own answer when its cache is empty.

use chrono::{DateTime, NaiveDateTime, Utc};
use crosstalk_core::config::MatchingConfig;
use crosstalk_core::store::{Store, StreamHealth};
use crosstalk_matching::normalize::{NormalizeFlags, Normalizer};
use regex::Regex;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::OnceLock;

// ---------------------------------------------------------------------------------------
// The plugin's constants (518-565)
// ---------------------------------------------------------------------------------------

/// `DEFAULT_PROBE_CACHE_TTL_MINUTES`: how long a measured throughput stays usable.
pub const DEFAULT_PROBE_CACHE_TTL_MINUTES: i64 = 30;
/// `DEFAULT_BITRATE_SAFETY_MARGIN`: above this multiple of a stream's nominal bitrate it is
/// healthy rather than marginal.
pub const DEFAULT_BITRATE_SAFETY_MARGIN: f64 = 1.10;
/// `PROBE_HEALTHY_MARGIN_MULTIPLIER`: between the safety margin and this it is marginal.
pub const PROBE_HEALTHY_MARGIN_MULTIPLIER: f64 = 1.5;
/// `CONTENT_STARVED_MIN_HEIGHT`: the placeholder floor only applies at 720p and above.
pub const CONTENT_STARVED_MIN_HEIGHT: i64 = 720;
/// `DEFAULT_CONTENT_BITRATE_FLOOR_KBPS`.
pub const DEFAULT_CONTENT_BITRATE_FLOOR_KBPS: f64 = 300.0;
/// `DEFAULT_BITRATE_SORT_STEP_KBPS`.
pub const DEFAULT_BITRATE_SORT_STEP_KBPS: f64 = 500.0;
/// `NOMINAL_BITRATE_FALLBACK_MBPS`: "assume ~720p30" when the stats say nothing.
pub const NOMINAL_BITRATE_FALLBACK_MBPS: f64 = 2.5;
/// `CONTENT_STARVED_LABEL`: what the CSV calls a demoted stream.
pub const CONTENT_STARVED_LABEL: &str = "placeholder";

/// `PluginConfig.NOMINAL_BITRATE_TABLE`, keyed by `(height bucket, fps band >= 35)`.
const NOMINAL_BITRATE_TABLE: [(i64, bool, f64); 10] = [
    (2160, false, 16.0),
    (2160, true, 25.0),
    (1080, false, 4.0),
    (1080, true, 6.0),
    (720, false, 2.5),
    (720, true, 4.0),
    (480, false, 1.5),
    (480, true, 2.0),
    (360, false, 0.8),
    (360, true, 1.2),
];

/// The height buckets, coarse to fine, in the order `_estimate_nominal_bitrate` checks them.
const HEIGHT_BUCKETS: [i64; 5] = [2160, 1080, 720, 480, 360];

// ---------------------------------------------------------------------------------------
// stream_stats
// ---------------------------------------------------------------------------------------

/// The `stream_stats` object, or an empty one for `null`, a list, a number or a missing key.
///
/// The plugin does `stream.get('stats', {}) or {}` and then `isinstance(stats, dict)` before
/// every read, so anything that is not an object behaves as "no facts at all".
fn stats_object(stats: Option<&Value>) -> Option<&Map<String, Value>> {
    stats?.as_object()
}

/// One `stream_stats` value.
fn stat<'a>(stats: Option<&'a Value>, key: &str) -> Option<&'a Value> {
    stats_object(stats)?.get(key)
}

/// `int(value)` with Python's failure mode: an unparseable or absent value reads 0.
///
/// This is `_stats_dimensions`' `int(stats.get('width') or 0)` inside its `try`/`except`
/// (5018-5026): a float truncates toward zero, a numeric string parses, and anything else
/// (a list, `"1920.0"`, an object) becomes 0. Underscores between digits parse too:
/// Python's `int()` allows them (`int("1_000")` is 1000), so a second attempt with the
/// underscores removed keeps a provider that wrote one from reading as no resolution.
fn py_int(value: Option<&Value>) -> i64 {
    match value {
        None | Some(Value::Null) => 0,
        Some(Value::Bool(flag)) => i64::from(*flag),
        Some(Value::Number(number)) => number
            .as_i64()
            .or_else(|| number.as_u64().map(|value| value as i64))
            .or_else(|| number.as_f64().map(|value| value.trunc() as i64))
            .unwrap_or(0),
        Some(Value::String(text)) => {
            let text = text.trim();
            text.parse::<i64>()
                .or_else(|_| text.replace('_', "").parse::<i64>())
                .unwrap_or(0)
        }
        Some(_) => 0,
    }
}

/// Everything one stream's key reads from its `stream_stats`, fetched in a single pass over
/// the object.
///
/// The plugin's helpers each re-read the dict per component (`_stats_dimensions`, the audio
/// ranks, the bitrate); the values are the same either way, but at 200k stream references a
/// run parses the same object once instead of seven times. The per-field coercions are the
/// shared [`py_int`]/[`py_float`] below, so the values are exactly what the one-at-a-time
/// readers return.
#[derive(Debug, Clone, Copy, Default)]
struct Facts<'v> {
    width: Option<&'v Value>,
    height: Option<&'v Value>,
    resolution: Option<&'v str>,
    fps_raw: Option<&'v Value>,
    video_bitrate: Option<&'v Value>,
    ffmpeg_bitrate: Option<&'v Value>,
    audio_channels: Option<&'v Value>,
    audio_codec: Option<&'v Value>,
    /// Whether `stats` was an object at all: anything else reads as "no facts", like the
    /// plugin's `isinstance(stats, dict)` guard.
    present: bool,
}

fn facts(stats: Option<&Value>) -> Facts<'_> {
    let Some(object) = stats_object(stats) else {
        return Facts::default();
    };
    let mut out = Facts {
        present: true,
        ..Facts::default()
    };
    for (key, value) in object {
        match key.as_str() {
            "width" => out.width = Some(value),
            "height" => out.height = Some(value),
            "resolution" => {
                if let Value::String(text) = value {
                    out.resolution = Some(text);
                }
            }
            "source_fps" => out.fps_raw = Some(value),
            "video_bitrate" => out.video_bitrate = Some(value),
            "ffmpeg_output_bitrate" => out.ffmpeg_bitrate = Some(value),
            "audio_channels" => out.audio_channels = Some(value),
            "audio_codec" => out.audio_codec = Some(value),
            _ => {}
        }
    }
    out
}

impl<'v> Facts<'v> {
    /// [`dimensions`] from the already-fetched values.
    fn dimensions(&self) -> (i64, i64) {
        if !self.present {
            return (0, 0);
        }
        let width = py_int(self.width);
        let height = py_int(self.height);
        if width > 0 && height > 0 {
            return (width, height);
        }
        if let Some(text) = self.resolution {
            if let Some(captures) = resolution_re().captures(text) {
                let parse = |group: usize| {
                    captures
                        .get(group)
                        .and_then(|value| value.as_str().parse::<i64>().ok())
                        .unwrap_or(0)
                };
                return (parse(1), parse(2));
            }
        }
        (width.max(0), height.max(0))
    }

    /// [`frame_rate`] from the already-fetched value.
    fn frame_rate(&self) -> f64 {
        match py_float(self.fps_raw) {
            Some(fps) if fps.is_finite() => fps,
            _ => 0.0,
        }
    }

    /// [`bitrate_kbps`] from the already-fetched values.
    fn bitrate(&self) -> Option<f64> {
        if !self.present {
            return None;
        }
        let mut best: Option<f64> = None;
        for value in [self.video_bitrate, self.ffmpeg_bitrate] {
            let Some(value) = py_float(value) else {
                continue;
            };
            if !(value > 0.0 && value < f64::INFINITY) {
                continue;
            }
            best = Some(best.map_or(value, |best| best.max(value)));
        }
        best
    }
}

/// `float(value)` for the numerics this model reads: numbers, numeric strings and booleans.
///
/// `None` is Python's `ValueError`/`TypeError`, which every caller turns into "not measured".
fn py_float(value: Option<&Value>) -> Option<f64> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::Bool(flag)) => Some(if *flag { 1.0 } else { 0.0 }),
        Some(Value::Number(number)) => number.as_f64(),
        Some(Value::String(text)) => text.trim().parse::<f64>().ok(),
        Some(_) => None,
    }
}

/// `str(value)` as far as an audio stat is concerned: `None` is `""`, a number keeps its JSON
/// spelling (`6` is `"6"`, `6.0` is `"6.0"`, like Python's `str`), a boolean is `True`/`False`.
fn py_str(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::Bool(flag)) => {
            if *flag {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        Some(Value::Number(number)) => {
            if let Some(integer) = number.as_i64() {
                integer.to_string()
            } else if let Some(unsigned) = number.as_u64() {
                unsigned.to_string()
            } else {
                let value = number.as_f64().unwrap_or_default();
                // Python's `str(6.0)` is "6.0", never "6".
                if value.is_finite() && value.fract() == 0.0 {
                    format!("{value:.1}")
                } else {
                    format!("{value}")
                }
            }
        }
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
    }
}

fn resolution_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s*(\d+)\s*[xX]\s*(\d+)").expect("static resolution regex"))
}

/// `(width, height)` from a `stream_stats` dict, or `(0, 0)`.
///
/// `Plugin._stats_dimensions` (5001-5030). Dispatcharr's own preview writes only `resolution`
/// (`"1920x1080"`) and never `width`/`height`; IPTV Checker writes all three. Integer
/// width/height win when both are positive, then `resolution` is parsed, and otherwise
/// whatever was read is returned as it stands — a height-only record still feeds placeholder
/// detection.
pub fn dimensions(stats: Option<&Value>) -> (i64, i64) {
    if stats_object(stats).is_none() {
        return (0, 0);
    }
    let width = py_int(stat(stats, "width"));
    let height = py_int(stat(stats, "height"));
    if width > 0 && height > 0 {
        return (width, height);
    }
    if let Some(Value::String(text)) = stat(stats, "resolution") {
        if let Some(captures) = resolution_re().captures(text) {
            let parse = |group: usize| {
                captures
                    .get(group)
                    .and_then(|value| value.as_str().parse::<i64>().ok())
                    .unwrap_or(0)
            };
            return (parse(1), parse(2));
        }
    }
    (width.max(0), height.max(0))
}

/// `source_fps` as a number; 0 when the stats carry none anything can read.
pub fn frame_rate(stats: Option<&Value>) -> f64 {
    match py_float(stat(stats, "source_fps")) {
        Some(fps) if fps.is_finite() => fps,
        _ => 0.0,
    }
}

/// The higher of the two recorded video bitrates in kbps, or `None` when neither was measured.
///
/// `Plugin._stats_bitrate_kbps` (4862-4882): zero, a negative, an infinity, a not-a-number and
/// anything unparseable all mean "not measured", and the HIGHER field wins because the two were
/// measured disagreeing on real streams.
pub fn bitrate_kbps(stats: Option<&Value>) -> Option<f64> {
    let object = stats_object(stats)?;
    let mut best: Option<f64> = None;
    for key in ["video_bitrate", "ffmpeg_output_bitrate"] {
        let Some(value) = py_float(object.get(key)) else {
            continue;
        };
        if !(value > 0.0 && value < f64::INFINITY) {
            continue;
        }
        best = Some(best.map_or(value, |best| best.max(value)));
    }
    best
}

/// `Plugin._is_content_starved` (4909-4945): provably too little video for the resolution it
/// claims.
///
/// Only positive evidence counts. No stats, no height and no measured bitrate all answer
/// `false`, because absence of data is not evidence of a slate; and the floor only applies at
/// [`CONTENT_STARVED_MIN_HEIGHT`] and above.
pub fn is_content_starved(stats: Option<&Value>, floor_kbps: f64) -> bool {
    if stats_object(stats).is_none() {
        return false;
    }
    is_content_starved_facts(dimensions(stats).1, bitrate_kbps(stats), floor_kbps)
}

/// The same verdict from already-read facts, so [`key_of`] does not parse the stats three
/// times over.
fn is_content_starved_facts(height: i64, bitrate: Option<f64>, floor_kbps: f64) -> bool {
    if height < CONTENT_STARVED_MIN_HEIGHT {
        return false;
    }
    // Zero means "not measured", not "carries no video".
    match bitrate {
        Some(best) => best < floor_kbps,
        None => false,
    }
}

/// `Plugin._audio_rank` (4723-4737): the index of the first priority token that is a
/// case-insensitive substring of the value, or `len(list)` when nothing matched.
///
/// An empty list answers 0 for every stream, which is the no-op the setting documents.
pub fn audio_rank(value: Option<&Value>, priority: &[String]) -> usize {
    if priority.is_empty() {
        return 0;
    }
    let value = py_str(value).trim().to_lowercase();
    if !value.is_empty() {
        for (index, token) in priority.iter().enumerate() {
            if !token.is_empty() && value.contains(token.as_str()) {
                return index;
            }
        }
    }
    priority.len()
}

/// `Plugin._bitrate_sort_key` (4884-4900): `(0, -bitrate // step)` for a measured bitrate,
/// `(1, 0)` for an unmeasured one, and `(0, 0)` for everything when the step is 0.
pub fn bitrate_sort_key(stats: Option<&Value>, step_kbps: f64) -> (u8, i64) {
    // `if not step > 0` — a 0, a negative and a not-a-number all switch the tie-breaker off.
    if step_kbps.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
        return (0, 0);
    }
    match bitrate_kbps(stats) {
        None => (1, 0),
        Some(bitrate) => (0, -((bitrate / step_kbps).floor() as i64)),
    }
}

/// The frame rate as `_estimate_nominal_bitrate` reads it: `float(fps or 0)` (5032-5040).
///
/// Python-falsy stats (`null`, `false`, `0`, `""`, `[]`, `{}`) read as 0 and still take a
/// height bucket; anything else goes through [`py_float`], so a numeric string parses and
/// only truthy garbage answers `None` (the caller falls back). `Some` may carry an
/// infinity or a not-a-number, exactly like the plugin's `float()`: the bucket loop only
/// ever compares them, which is total either way.
fn estimate_fps(raw: Option<&Value>) -> Option<f64> {
    let falsy = match raw {
        None | Some(Value::Null) => true,
        Some(Value::Bool(flag)) => !flag,
        Some(Value::Number(number)) => number.as_f64().is_some_and(|value| value == 0.0),
        Some(Value::String(text)) => text.is_empty(),
        Some(Value::Array(items)) => items.is_empty(),
        Some(Value::Object(map)) => map.is_empty(),
    };
    if falsy {
        return Some(0.0);
    }
    py_float(raw)
}

/// `Plugin._estimate_nominal_bitrate` (5032-5049): the Mbps a stream of this shape needs.
///
/// The plugin is handed the whole `(width, height)` pair and sizes its bucket off the height
/// alone, so width is not a parameter here.
pub fn estimate_nominal_bitrate(height: i64, fps: f64) -> f64 {
    let height = height.max(0);
    if height <= 0 {
        return NOMINAL_BITRATE_FALLBACK_MBPS;
    }
    let high = fps >= 35.0;
    for bucket in HEIGHT_BUCKETS {
        if height >= bucket {
            return NOMINAL_BITRATE_TABLE
                .iter()
                .find(|(size, band, _)| *size == bucket && *band == high)
                .map(|(_, _, rate)| *rate)
                .unwrap_or(NOMINAL_BITRATE_FALLBACK_MBPS);
        }
    }
    NOMINAL_BITRATE_FALLBACK_MBPS
}

// ---------------------------------------------------------------------------------------
// Throughput evidence
// ---------------------------------------------------------------------------------------

/// One stream's measured delivery rate, in the shape the sort reads.
///
/// The plugin's cache entry carries `throughput_mbps`, `throughput_measured_at`,
/// `nominal_bitrate_mbps`, `edge_ip` and `probe_duration_s` (10437-10444). Only the first three
/// change an order; `edge_ip` is kept because the CSV export prints it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ThroughputEntry {
    /// `None` means the probe ran and measured nothing (a failure), which is never "fresh".
    pub throughput_mbps: Option<f64>,
    pub measured_at: Option<DateTime<Utc>>,
    /// The nominal bitrate recorded WITH the measurement. When absent or non-positive the
    /// nominal is re-estimated from the stream's own stats.
    pub nominal_bitrate_mbps: Option<f64>,
    pub edge_ip: Option<String>,
}

impl ThroughputEntry {
    /// An entry from the plugin cache's own JSON shape, for fixtures and tests.
    pub fn from_cache_json(value: &Value) -> Self {
        let field = |key: &str| value.get(key);
        Self {
            throughput_mbps: py_float(field("throughput_mbps")),
            measured_at: field("throughput_measured_at")
                .and_then(Value::as_str)
                .and_then(parse_timestamp),
            nominal_bitrate_mbps: py_float(field("nominal_bitrate_mbps")),
            edge_ip: field("edge_ip")
                .and_then(Value::as_str)
                .map(str::to_string)
                .filter(|edge| !edge.is_empty()),
        }
    }

    /// The same evidence as a `stream_health` row.
    pub fn from_health(health: &StreamHealth) -> Self {
        Self {
            throughput_mbps: health.throughput_mbps,
            measured_at: health.throughput_at.as_deref().and_then(parse_timestamp),
            nominal_bitrate_mbps: None,
            edge_ip: health.edge_ip.clone().filter(|edge| !edge.is_empty()),
        }
    }
}

/// Parse the timestamps a cache entry carries.
///
/// `_is_probe_fresh` does `datetime.fromisoformat(ts.replace('Z', '+00:00'))` and assumes UTC
/// for a naive value (5085-5092). The same two accepted shapes: an RFC 3339 timestamp with an
/// offset, or a bare `YYYY-MM-DDTHH:MM:SS[.ffffff]` read as UTC.
fn parse_timestamp(text: &str) -> Option<DateTime<Utc>> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(text) {
        return Some(parsed.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(text, format) {
            return Some(naive.and_utc());
        }
    }
    None
}

/// `Plugin._is_probe_fresh` (5072-5095): a positive measurement inside the TTL.
///
/// A failed probe (`throughput_mbps` is `None`) is never fresh, so a run that errors on every
/// URL cannot lock the streams out of re-probing for a whole TTL window, and an entry with no
/// timestamp is not fresh either.
pub fn is_probe_fresh(entry: &ThroughputEntry, ttl_minutes: i64, now: DateTime<Utc>) -> bool {
    if entry.throughput_mbps.is_none() {
        return false;
    }
    let Some(measured) = entry.measured_at else {
        return false;
    };
    (now - measured).num_seconds() < ttl_minutes * 60
}

/// Where a run reads measured throughput from.
///
/// The plugin reads one JSON file; crosstalk reads the store. Keeping the read behind a trait
/// means a run without a store sorts exactly as the plugin does with an empty cache — every
/// stream "unknown" — rather than refusing to run.
pub trait ThroughputSource: Send + Sync {
    /// The measurement for this stream, or `None` when it has never been probed.
    fn entry(&self, stream_id: i64) -> Option<ThroughputEntry>;
}

/// Measurements held in memory: what tests, the parity harness and a store-less run use.
#[derive(Debug, Clone, Default)]
pub struct MemoryThroughputSource {
    entries: HashMap<i64, ThroughputEntry>,
}

impl MemoryThroughputSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, stream_id: i64, entry: ThroughputEntry) {
        self.entries.insert(stream_id, entry);
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl ThroughputSource for MemoryThroughputSource {
    fn entry(&self, stream_id: i64) -> Option<ThroughputEntry> {
        self.entries.get(&stream_id).cloned()
    }
}

/// Measurements read from the store's `stream_health` table.
///
/// Loaded once per run: at ~50k streams the whole table is a few megabytes and one query, while
/// a lookup per stream inside the sort would be a query per stream.
#[derive(Debug, Clone, Default)]
pub struct StoreThroughputSource {
    entries: HashMap<i64, ThroughputEntry>,
}

impl StoreThroughputSource {
    /// Load every row. A stream with no row simply has no measurement.
    pub fn load(store: &Store) -> anyhow::Result<Self> {
        let entries = store
            .stream_health_map()?
            .values()
            .map(|health| (health.stream_id, ThroughputEntry::from_health(health)))
            .collect();
        Ok(Self { entries })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl ThroughputSource for StoreThroughputSource {
    fn entry(&self, stream_id: i64) -> Option<ThroughputEntry> {
        self.entries.get(&stream_id).cloned()
    }
}

/// The throughput evidence a run reads: the store's `stream_health`, or nothing when the
/// context has no store (every stream "unknown", like the plugin with an empty cache).
pub fn throughput_source(store: Option<&Store>) -> anyhow::Result<Box<dyn ThroughputSource>> {
    Ok(match store {
        Some(store) => Box::new(StoreThroughputSource::load(store)?),
        None => Box::new(MemoryThroughputSource::new()),
    })
}

// ---------------------------------------------------------------------------------------
// The model
// ---------------------------------------------------------------------------------------

/// Everything the key reads besides the stream itself.
///
/// Resolved once per run from `streams.toml` (the plugin re-reads its settings dict on every
/// call and then re-primes the same values onto `self`, 4684-4712). `now` is the one addition:
/// the plugin calls `timezone.now()` per freshness check, which makes an order impossible to
/// reproduce and impossible to test at a boundary.
#[derive(Debug, Clone)]
pub struct QualityModel {
    pub prioritize_quality: bool,
    /// `audio_channels_priority`, parsed and lowercased.
    pub audio_channels_priority: Vec<String>,
    /// `audio_codec_priority`, parsed and lowercased.
    pub audio_codec_priority: Vec<String>,
    pub bitrate_sort_step_kbps: f64,
    pub demote_content_starved: bool,
    pub content_bitrate_floor_kbps: f64,
    pub enable_throughput_sorting: bool,
    pub bitrate_safety_margin: f64,
    pub probe_cache_ttl_minutes: i64,
    /// The run's clock, for [`is_probe_fresh`].
    pub now: DateTime<Utc>,
}

impl Default for QualityModel {
    fn default() -> Self {
        Self {
            prioritize_quality: false,
            audio_channels_priority: Vec::new(),
            audio_codec_priority: Vec::new(),
            bitrate_sort_step_kbps: DEFAULT_BITRATE_SORT_STEP_KBPS,
            demote_content_starved: true,
            content_bitrate_floor_kbps: DEFAULT_CONTENT_BITRATE_FLOOR_KBPS,
            enable_throughput_sorting: true,
            bitrate_safety_margin: DEFAULT_BITRATE_SAFETY_MARGIN,
            probe_cache_ttl_minutes: DEFAULT_PROBE_CACHE_TTL_MINUTES,
            now: Utc::now(),
        }
    }
}

impl QualityModel {
    /// The model `streams.toml` describes.
    pub fn from_settings(settings: &crate::settings::StreamsSettings) -> Self {
        Self {
            prioritize_quality: settings.prioritize_quality,
            audio_channels_priority: lowercased(&settings.audio_channels_priority),
            audio_codec_priority: lowercased(&settings.audio_codec_priority),
            bitrate_sort_step_kbps: f64::from(settings.bitrate_sort_step_kbps),
            demote_content_starved: settings.demote_content_starved,
            // `_resolve_content_starved_floor` (4826-4842) falls back to the default rather
            // than switching the check off: a floor of 0 would look configured and do nothing.
            content_bitrate_floor_kbps: if settings.content_bitrate_floor_kbps == 0 {
                DEFAULT_CONTENT_BITRATE_FLOOR_KBPS
            } else {
                f64::from(settings.content_bitrate_floor_kbps)
            },
            enable_throughput_sorting: settings.enable_throughput_sorting,
            // `_prime_throughput_state` (4690-4706): anything unusable takes the default.
            bitrate_safety_margin: if settings.bitrate_safety_margin.is_finite()
                && settings.bitrate_safety_margin > 0.0
            {
                settings.bitrate_safety_margin
            } else {
                DEFAULT_BITRATE_SAFETY_MARGIN
            },
            // Kept as written, 0 included: `_prime_throughput_state` (4697-4706) only falls
            // back when the value is not a number, so a TTL of 0 makes every measurement stale
            // in the plugin and does here too.
            probe_cache_ttl_minutes: i64::from(settings.probe_cache_ttl_minutes),
            now: Utc::now(),
        }
    }
}

/// `_parse_priority_list` (4714-4720): the comma/quote parser, lowercased.
pub fn priority_list(raw: &str) -> Vec<String> {
    crate::selection::parse_ignore_tags(raw)
        .into_iter()
        .map(|token| token.to_lowercase())
        .collect()
}

fn lowercased(raw: &str) -> Vec<String> {
    priority_list(raw)
}

/// One stream as the key sees it: the facts, not the whole `Stream`.
///
/// Kept separate from [`crosstalk_core::dispatcharr::Stream`] so the model can be swept against
/// the plugin over fixtures that carry nothing else, and so the m3u priority (a property of the
/// RUN's account list, not of the stream) travels with it.
#[derive(Debug, Clone, Copy)]
pub struct StreamView<'a> {
    pub id: i64,
    pub name: &'a str,
    pub stats: Option<&'a Value>,
    /// Position of the stream's M3U account in `selected_m3us`; 999 when unlisted.
    pub m3u_priority: usize,
    pub is_custom: bool,
}

/// `Plugin._classify_stream_throughput` (4963-4996): 0 healthy, 1 marginal, 2 unknown,
/// 3 insufficient.
///
/// A stale entry is "unknown" rather than its old tier, so an outdated probe cannot keep
/// steering the order; the operator refreshes it by probing again.
pub fn classify_throughput(
    stats: Option<&Value>,
    entry: Option<&ThroughputEntry>,
    model: &QualityModel,
) -> u8 {
    let Some(entry) = entry else {
        return 2;
    };
    if !is_probe_fresh(entry, model.probe_cache_ttl_minutes, model.now) {
        return 2;
    }
    let Some(mbps) = entry.throughput_mbps else {
        return 2;
    };
    let read = facts(stats);
    let nominal = match entry.nominal_bitrate_mbps {
        Some(nominal) if nominal > 0.0 => nominal,
        // `_classify_stream_throughput` (4992-4996) re-estimates from the RAW stat here:
        // `_estimate_nominal_bitrate` runs `float(fps or 0)` in a `try`, so a missing or
        // Python-falsy value reads 0 and still takes a height bucket, a numeric string
        // parses (`"50"` takes the 35fps band), and only truthy garbage falls back.
        // [`frame_rate`] would read 0 for the strings too, which is the tier path's answer
        // where the plugin raises — not this path's, which never raises at all.
        _ => match estimate_fps(read.fps_raw) {
            Some(fps) => estimate_nominal_bitrate(read.dimensions().1, fps),
            None => NOMINAL_BITRATE_FALLBACK_MBPS,
        },
    };
    if mbps < nominal * model.bitrate_safety_margin {
        return 3;
    }
    if mbps < nominal * PROBE_HEALTHY_MARGIN_MULTIPLIER {
        return 1;
    }
    0
}

/// A float compared in ascending order, total over every value.
///
/// The key's last components are negated rates (`-fps`, `-mbps`), so ascending comparison is
/// "higher first" without a comparator of its own. `total_cmp` is what gives the type an `Ord`
/// at all; NaN cannot reach it, because [`frame_rate`] and [`py_float`] both refuse it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// A descending rate, with `-0.0` folded into `0.0` so two "no rate" streams tie.
fn descending(value: f64) -> OrdF64 {
    OrdF64(if value == 0.0 { 0.0 } else { -value })
}

/// The flattened sort key.
///
/// One struct rather than the plugin's nested tuple, compared field by field in the plugin's
/// order: `rank_first`/`rank_second` are `(tier, m3u_priority)` or `(m3u_priority, tier)`
/// depending on `prioritize_quality`, which is resolved when the key is built.
///
/// Crate-visible (not public) so [`crate::sort`] can compute each stream's key once per run
/// and reuse it across every channel that carries the stream, instead of re-parsing the same
/// stats per channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct QualityKey {
    content_starved: u8,
    throughput_tier: u8,
    rank_first: usize,
    rank_second: usize,
    audio_channels_rank: usize,
    audio_codec_rank: usize,
    pixels: i64,
    fps: OrdF64,
    bitrate_measured: u8,
    bitrate_steps: i64,
    throughput: OrdF64,
}

/// The key one stream sorts on.
///
/// The facts every component reads (`dimensions`, `frame_rate`, `bitrate_kbps`) are parsed
/// once here and shared, rather than re-read per component the way the plugin's separate
/// helpers do: same values, a third of the map lookups.
pub(crate) fn key_of(
    stream: &StreamView<'_>,
    model: &QualityModel,
    throughput: &dyn ThroughputSource,
) -> QualityKey {
    let stats = stream.stats;
    let entry = if model.enable_throughput_sorting {
        throughput.entry(stream.id)
    } else {
        None
    };
    let throughput_tier = if model.enable_throughput_sorting {
        classify_throughput(stats, entry.as_ref(), model)
    } else {
        0
    };
    let read = facts(stats);
    let (width, height) = read.dimensions();
    let fps = read.frame_rate();
    let bitrate = read.bitrate();
    let content_starved = u8::from(
        model.demote_content_starved
            && is_content_starved_facts(height, bitrate, model.content_bitrate_floor_kbps),
    );
    let audio_channels_rank = audio_rank(read.audio_channels, &model.audio_channels_priority);
    let audio_codec_rank = audio_rank(read.audio_codec, &model.audio_codec_priority);
    // `_bitrate_sort_key` from the already-read bitrate, not a second parse.
    let (bitrate_measured, bitrate_steps) = if model.bitrate_sort_step_kbps > 0.0 {
        match bitrate {
            None => (1, 0),
            Some(rate) => (0, -((rate / model.bitrate_sort_step_kbps).floor() as i64)),
        }
    } else {
        (0, 0)
    }; // `if not step > 0`: 0, negative and NaN switch the tie-breaker off (4884-4900)
    let (tier, pixels, neg_fps, throughput_rate) = if width == 0 || height == 0 {
        // No usable resolution: tier 3, `(0, 0)` for the pixel and frame-rate components, and
        // a constant rate component (4784-4796).
        (3u8, 0i64, descending(0.0), 0.0)
    } else {
        let is_hd = width >= 1280 && height >= 720;
        let good_fps = fps >= 30.0;
        let tier = if is_hd && good_fps {
            0
        } else if is_hd || good_fps {
            1
        } else {
            2
        };
        let rate = if model.enable_throughput_sorting {
            // `entry.get('throughput_mbps') or 0.0`, from the entry whether or not it is fresh:
            // freshness is the TIER's business, and a stale rate only ever breaks a tie
            // between two streams of the same tier (4809-4813).
            entry
                .as_ref()
                .and_then(|entry| entry.throughput_mbps)
                .unwrap_or(0.0)
        } else {
            0.0
        };
        (tier, width.saturating_mul(height), descending(fps), rate)
    };

    let (rank_first, rank_second) = if model.prioritize_quality {
        (usize::from(tier), stream.m3u_priority)
    } else {
        (stream.m3u_priority, usize::from(tier))
    };

    QualityKey {
        content_starved,
        throughput_tier,
        rank_first,
        rank_second,
        audio_channels_rank,
        audio_codec_rank,
        pixels: -pixels,
        fps: neg_fps,
        bitrate_measured,
        bitrate_steps,
        throughput: descending(throughput_rate),
    }
}

impl QualityKey {
    /// The report label for a stream that sorted on this key: the placeholder verdict first,
    /// then the throughput tier, exactly like [`tier_label`]. A changed channel's tiers come
    /// from the keys it already sorted on, so the report never re-reads the stats.
    pub(crate) fn tier_label(&self) -> &'static str {
        if self.content_starved == 1 {
            return CONTENT_STARVED_LABEL;
        }
        match self.throughput_tier {
            0 => "healthy",
            1 => "marginal",
            3 => "insufficient",
            _ => "unknown",
        }
    }
}

/// Sort streams by the quality key, stably.
///
/// The plugin's `sorted(streams, key=scored)` (4822): every key is computed once, and streams
/// whose keys are equal keep the order they arrived in. That stability is what makes a
/// re-sorted channel's list deterministic instead of reshuffling equal alternates per run.
pub fn sort_streams<'a>(
    streams: &[StreamView<'a>],
    model: &QualityModel,
    throughput: &dyn ThroughputSource,
) -> Vec<StreamView<'a>> {
    let mut scored: Vec<(QualityKey, StreamView<'a>)> = streams
        .iter()
        .map(|stream| (key_of(stream, model, throughput), *stream))
        .collect();
    scored.sort_by_key(|scored| scored.0);
    scored.into_iter().map(|(_, stream)| stream).collect()
}

/// What [`sort_streams`] decided about one stream, for a report's diagnostics.
///
/// `_stream_tier_label` (4947-4961): a stream proven content-starved reports as
/// [`CONTENT_STARVED_LABEL`] rather than as a throughput tier, because placement is the reason
/// it moved. The label is only meaningful when the demotion setting is on.
pub fn tier_label(
    stats: Option<&Value>,
    entry: Option<&ThroughputEntry>,
    model: &QualityModel,
) -> &'static str {
    if model.demote_content_starved && is_content_starved(stats, model.content_bitrate_floor_kbps) {
        return CONTENT_STARVED_LABEL;
    }
    match classify_throughput(stats, entry, model) {
        0 => "healthy",
        1 => "marginal",
        3 => "insufficient",
        _ => "unknown",
    }
}

/// The normaliser zone routing uses for a channel name.
///
/// `_clean_channel_name` (4382-4400) normalises with the plugin's ignore flags, which here are
/// the host's `[matching]` toggles plus the unified engine's decided structural flags —
/// the same normalizer the engine itself runs (step 9: one normalizer), so the base name
/// zone routing groups siblings by is the name the matcher compares.
pub fn zone_normalizer(matching: &MatchingConfig) -> Normalizer {
    // The decided flags are the engine's (`UnifiedEngine::new` forces them): the core
    // pipeline (N01), designator rejoin (N08), `&`-split (N05), letter-only dots (N06)
    // and the suffix floor (N07). No profile switch survives beside them.
    let mut flags: NormalizeFlags = matching.normalize_flags();
    flags.core_pipeline = true;
    flags.rejoin_designators = true;
    flags.split_ampersand = true;
    flags.dots_letters_only = true;
    flags.suffix_floor = true;
    Normalizer::new(flags)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use serde_json::json;

    fn at(seconds_from_now: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap() + Duration::seconds(seconds_from_now)
    }

    fn model() -> QualityModel {
        QualityModel {
            now: at(0),
            ..QualityModel::default()
        }
    }

    fn view<'a>(id: i64, name: &'a str, stats: &'a Value, priority: usize) -> StreamView<'a> {
        StreamView {
            id,
            name,
            stats: Some(stats),
            m3u_priority: priority,
            is_custom: false,
        }
    }

    fn ids(streams: &[StreamView<'_>]) -> Vec<i64> {
        streams.iter().map(|stream| stream.id).collect()
    }

    #[test]
    fn dimensions_read_width_height_then_resolution() {
        // `_stats_dimensions` (5001-5030): integers win, then `resolution`, then whatever was
        // read.
        for (stats, expected) in [
            (json!({"width": 1920, "height": 1080}), (1920, 1080)),
            (json!({"resolution": "1920x1080"}), (1920, 1080)),
            (json!({"resolution": "1280X720"}), (1280, 720)),
            (json!({"resolution": "3840x2160p"}), (3840, 2160)),
            // Integer width/height win over `resolution` when BOTH are positive.
            (
                json!({"width": 1280, "height": 720, "resolution": "1920x1080"}),
                (1280, 720),
            ),
            // A resolution string is only consulted when the pair is incomplete.
            (
                json!({"width": 1280, "height": 0, "resolution": "1920x1080"}),
                (1920, 1080),
            ),
            (json!({"width": "1920", "height": "1080"}), (1920, 1080)),
            (json!({"width": "wide", "height": "tall"}), (0, 0)),
            (json!({"resolution": "unknown"}), (0, 0)),
            (json!({"width": 1920}), (1920, 0)),
            (json!({"height": 1080}), (0, 1080)),
            // A negative width is clamped to 0 and the pair stays incomplete.
            (json!({"width": -1920, "height": 1080}), (0, 1080)),
            (json!({}), (0, 0)),
            (json!(null), (0, 0)),
            // Not a dict at all: `isinstance(stats, dict)` is false.
            (json!([1, 2, 3]), (0, 0)),
        ] {
            assert_eq!(dimensions(Some(&stats)), expected, "{stats}");
        }
        assert_eq!(dimensions(None), (0, 0));
    }

    #[test]
    fn bitrate_is_the_higher_of_the_two_measured_fields() {
        // `_stats_bitrate_kbps` (4862-4882).
        assert_eq!(
            bitrate_kbps(Some(&json!({"video_bitrate": 2656}))),
            Some(2656.0)
        );
        assert_eq!(
            bitrate_kbps(Some(&json!({"ffmpeg_output_bitrate": 3010}))),
            Some(3010.0)
        );
        // The two were measured disagreeing on real streams: the higher one wins.
        assert_eq!(
            bitrate_kbps(Some(
                &json!({"video_bitrate": 124, "ffmpeg_output_bitrate": 389})
            )),
            Some(389.0)
        );
        assert_eq!(
            bitrate_kbps(Some(&json!({"video_bitrate": "1499"}))),
            Some(1499.0)
        );
        // Zero, negative, garbage and absent all mean "not measured".
        assert_eq!(
            bitrate_kbps(Some(
                &json!({"video_bitrate": 0, "ffmpeg_output_bitrate": 0})
            )),
            None
        );
        assert_eq!(bitrate_kbps(Some(&json!({"video_bitrate": -5}))), None);
        assert_eq!(bitrate_kbps(Some(&json!({"video_bitrate": "fast"}))), None);
        assert_eq!(bitrate_kbps(Some(&json!({}))), None);
        assert_eq!(bitrate_kbps(Some(&json!({"video_bitrate": null}))), None);
        assert_eq!(bitrate_kbps(None), None);
    }

    #[test]
    fn content_starvation_needs_positive_evidence_at_720p_or_more() {
        // `_is_content_starved` (4909-4945): fail open, floor only at 720p and above.
        assert!(is_content_starved(
            Some(&json!({"height": 1080, "video_bitrate": 190})),
            300.0
        ));
        // The floor itself is not starvation.
        assert!(!is_content_starved(
            Some(&json!({"height": 720, "video_bitrate": 300})),
            300.0
        ));
        // Below 720p the floor does not apply, however low the bitrate.
        assert!(!is_content_starved(
            Some(&json!({"height": 480, "video_bitrate": 100})),
            300.0
        ));
        // No measured bitrate is not evidence of a slate.
        assert!(!is_content_starved(Some(&json!({"height": 1080})), 300.0));
        assert!(!is_content_starved(Some(&json!({})), 300.0));
        assert!(!is_content_starved(None, 300.0));
    }

    #[test]
    fn audio_rank_is_the_first_substring_match_or_last() {
        // `_audio_rank` (4723-4737). The plugin's own placeholder list is layout NAMES, so a
        // channel COUNT matches nothing; a list of counts matches a number that contains it.
        let layouts = priority_list("7.1, 5.1, stereo, mono");
        assert_eq!(audio_rank(Some(&json!("7.1")), &layouts), 0);
        assert_eq!(audio_rank(Some(&json!("5.1")), &layouts), 1);
        assert_eq!(audio_rank(Some(&json!("Stereo")), &layouts), 2);
        assert_eq!(audio_rank(Some(&json!("mono")), &layouts), 3);
        assert_eq!(audio_rank(Some(&json!(6)), &layouts), 4);
        // No match, no value and an empty value all sort last.
        assert_eq!(audio_rank(Some(&json!("atmos")), &layouts), 4);
        assert_eq!(audio_rank(None, &layouts), 4);
        assert_eq!(audio_rank(Some(&json!("")), &layouts), 4);
        // A count list matches the stringified count; Python keeps a float's ".0", so 6.0
        // still contains "6".
        let counts = priority_list("8, 6, 2");
        assert_eq!(audio_rank(Some(&json!(8)), &counts), 0);
        assert_eq!(audio_rank(Some(&json!(6)), &counts), 1);
        assert_eq!(audio_rank(Some(&json!(6.0)), &counts), 1);
        assert_eq!(audio_rank(Some(&json!(2)), &counts), 2);
        // An empty priority list is a no-op for every stream.
        assert_eq!(audio_rank(Some(&json!("atmos")), &[]), 0);
        // Codecs are matched the same way, case-insensitively.
        let codecs = priority_list("eac3, ac3, aac, mp2");
        assert_eq!(audio_rank(Some(&json!("EAC3")), &codecs), 0);
        assert_eq!(audio_rank(Some(&json!("ac3")), &codecs), 1);
        assert_eq!(audio_rank(Some(&json!("truehd")), &codecs), 4);
    }

    #[test]
    fn the_bitrate_tiebreak_rounds_down_and_switches_off_at_zero() {
        // `_bitrate_sort_key` (4884-4900).
        let stats = json!({"video_bitrate": 2656});
        assert_eq!(bitrate_sort_key(Some(&stats), 500.0), (0, -5));
        assert_eq!(bitrate_sort_key(Some(&stats), 100.0), (0, -26));
        // A step of 0 turns the tie-breaker off for every stream, measured or not.
        assert_eq!(bitrate_sort_key(Some(&stats), 0.0), (0, 0));
        assert_eq!(bitrate_sort_key(Some(&json!({})), 0.0), (0, 0));
        // An unmeasured bitrate sorts after a measured one.
        assert_eq!(bitrate_sort_key(Some(&json!({})), 500.0), (1, 0));
        // 190 // 500 is 0, which is still "measured".
        assert_eq!(
            bitrate_sort_key(Some(&json!({"video_bitrate": 190})), 500.0),
            (0, 0)
        );
    }

    #[test]
    fn nominal_bitrate_buckets_floor_down_and_split_on_35fps() {
        // `_estimate_nominal_bitrate` (5032-5049) and `NOMINAL_BITRATE_TABLE` (553-565).
        assert_eq!(estimate_nominal_bitrate(2160, 24.0), 16.0);
        assert_eq!(estimate_nominal_bitrate(2160, 60.0), 25.0);
        assert_eq!(estimate_nominal_bitrate(1440, 24.0), 4.0); // floors into the 1080 bucket
        assert_eq!(estimate_nominal_bitrate(1080, 50.0), 6.0);
        assert_eq!(estimate_nominal_bitrate(720, 30.0), 2.5);
        assert_eq!(estimate_nominal_bitrate(720, 35.0), 4.0);
        assert_eq!(estimate_nominal_bitrate(480, 30.0), 1.5);
        assert_eq!(estimate_nominal_bitrate(360, 60.0), 1.2);
        // Below the smallest bucket and with no height at all: the fallback.
        assert_eq!(
            estimate_nominal_bitrate(240, 30.0),
            NOMINAL_BITRATE_FALLBACK_MBPS
        );
        assert_eq!(
            estimate_nominal_bitrate(0, 30.0),
            NOMINAL_BITRATE_FALLBACK_MBPS
        );
    }

    #[test]
    fn freshness_needs_a_measurement_and_a_usable_timestamp() {
        // `_is_probe_fresh` (5072-5095).
        let now = at(0);
        let fresh = ThroughputEntry {
            throughput_mbps: Some(10.0),
            measured_at: Some(at(-60)),
            ..ThroughputEntry::default()
        };
        assert!(is_probe_fresh(&fresh, 30, now));
        let stale = ThroughputEntry {
            measured_at: Some(at(-3600)),
            ..fresh.clone()
        };
        assert!(!is_probe_fresh(&stale, 30, now));
        // A failed probe is never fresh, however recent.
        let failed = ThroughputEntry {
            throughput_mbps: None,
            measured_at: Some(at(-1)),
            nominal_bitrate_mbps: None,
            edge_ip: None,
        };
        assert!(!is_probe_fresh(&failed, 30, now));
        // No timestamp at all.
        let undated = ThroughputEntry {
            measured_at: None,
            ..fresh
        };
        assert!(!is_probe_fresh(&undated, 30, now));
        // The boundary itself is stale: `(now - measured) < ttl * 60` is strict.
        let boundary = ThroughputEntry {
            throughput_mbps: Some(10.0),
            measured_at: Some(at(-1800)),
            ..ThroughputEntry::default()
        };
        assert!(!is_probe_fresh(&boundary, 30, now));
        let just_fresh = ThroughputEntry {
            measured_at: Some(at(-1799)),
            ..boundary.clone()
        };
        assert!(is_probe_fresh(&just_fresh, 30, now));
    }

    #[test]
    fn not_a_number_and_infinity_never_crash_a_tier() {
        // JSON cannot carry either, but the store holds raw floats: NaN compares false
        // against every bound, so it falls through to healthy — the plugin's own `<` chain
        // does exactly the same — while -inf is below the safety margin and insufficient.
        let model = QualityModel {
            now: at(0),
            ..QualityModel::default()
        };
        let stats = json!({"width": 1920, "height": 1080, "source_fps": 50});
        for (odd, expected) in [(f64::NAN, 0), (f64::INFINITY, 0), (f64::NEG_INFINITY, 3)] {
            let entry = ThroughputEntry {
                throughput_mbps: Some(odd),
                measured_at: Some(at(-60)),
                nominal_bitrate_mbps: Some(6.0),
                edge_ip: None,
            };
            assert_eq!(
                classify_throughput(Some(&stats), Some(&entry), &model),
                expected,
                "throughput {odd}"
            );
        }
    }

    #[test]
    fn the_nominal_estimate_reads_fps_like_the_plugin_does() {
        // `_estimate_nominal_bitrate` runs `float(fps or 0)`: falsy stats read 0 and still
        // take a bucket, a numeric string parses, only truthy garbage falls back.
        assert_eq!(estimate_nominal_bitrate(1080, 0.0), 4.0);
        let fifty = json!(50);
        let fifty_string = json!("50");
        let fast = json!("fast");
        let null = json!(null);
        let empty = json!("");
        assert_eq!(estimate_fps(Some(&fifty)), Some(50.0));
        assert_eq!(estimate_fps(Some(&fifty_string)), Some(50.0));
        assert_eq!(estimate_fps(Some(&fast)), None);
        assert_eq!(estimate_fps(None), Some(0.0));
        assert_eq!(estimate_fps(Some(&null)), Some(0.0));
        assert_eq!(estimate_fps(Some(&empty)), Some(0.0));
        // And the tier uses it: a 1080p stream measured at 7 Mbps is marginal against the
        // 50fps band (nominal 6.0: between 6.6 and 9.0) but healthy against the 0fps one
        // (nominal 4.0).
        let model = QualityModel {
            now: at(0),
            ..QualityModel::default()
        };
        let entry = ThroughputEntry {
            throughput_mbps: Some(7.0),
            measured_at: Some(at(-60)),
            nominal_bitrate_mbps: None,
            edge_ip: None,
        };
        let stats = json!({"width": 1920, "height": 1080, "source_fps": "50"});
        assert_eq!(classify_throughput(Some(&stats), Some(&entry), &model), 1);
        // The tier path reads the numeric string as its number — the order the plugin would
        // give the same stat written numerically — and only an unreadable value as 0, where
        // the plugin raises and has no answer to match.
        assert_eq!(frame_rate(Some(&stats)), 50.0);
        let garbage = json!({"width": 1920, "height": 1080, "source_fps": "fast"});
        assert_eq!(frame_rate(Some(&garbage)), 0.0);
    }

    #[test]
    fn python_int_allows_underscores_between_digits() {
        let wide = json!({"width": "1_920", "height": "1_080"});
        assert_eq!(dimensions(Some(&wide)), (1920, 1080));
    }

    #[test]
    fn cache_timestamps_parse_both_shapes_the_plugin_writes() {
        // `datetime.fromisoformat(ts.replace('Z', '+00:00'))`, naive read as UTC.
        let with_offset = parse_timestamp("2026-11-01T12:00:00+00:00").expect("offset");
        assert_eq!(with_offset, at(0));
        let zulu = parse_timestamp("2026-11-01T12:00:00Z").expect("zulu");
        assert_eq!(zulu, at(0));
        let naive = parse_timestamp("2026-11-01T12:00:00").expect("naive");
        assert_eq!(naive, at(0));
        let fractional = parse_timestamp("2026-11-01T12:00:00.123456").expect("fractional");
        assert_eq!(fractional.timestamp(), at(0).timestamp());
        assert_eq!(parse_timestamp("yesterday"), None);
        assert_eq!(parse_timestamp(""), None);
    }

    #[test]
    fn throughput_tiers_follow_the_safety_margin_and_the_healthy_multiplier() {
        // `_classify_stream_throughput` (4963-4996).
        let model = QualityModel {
            now: at(0),
            bitrate_safety_margin: 1.10,
            ..QualityModel::default()
        };
        let stats = json!({"width": 1920, "height": 1080, "source_fps": 50});
        let entry = |mbps: f64, nominal: Option<f64>, age: i64| ThroughputEntry {
            throughput_mbps: Some(mbps),
            measured_at: Some(at(-age)),
            nominal_bitrate_mbps: nominal,
            edge_ip: None,
        };
        // nominal 6.0 at 1080p50: 3.0 is below 6.6, 7.0 is between 6.6 and 9.0, 10 is above.
        assert_eq!(
            classify_throughput(Some(&stats), Some(&entry(3.0, Some(6.0), 60)), &model),
            3
        );
        assert_eq!(
            classify_throughput(Some(&stats), Some(&entry(7.0, Some(6.0), 60)), &model),
            1
        );
        assert_eq!(
            classify_throughput(Some(&stats), Some(&entry(10.0, Some(6.0), 60)), &model),
            0
        );
        // An entry with no nominal of its own is measured against the stats' own estimate.
        assert_eq!(
            classify_throughput(Some(&stats), Some(&entry(10.0, None, 60)), &model),
            0
        );
        assert_eq!(
            classify_throughput(Some(&stats), Some(&entry(1.0, None, 60)), &model),
            3
        );
        // Stale, failed and missing are all "unknown".
        assert_eq!(
            classify_throughput(Some(&stats), Some(&entry(10.0, Some(6.0), 7200)), &model),
            2
        );
        assert_eq!(
            classify_throughput(Some(&stats), Some(&entry(0.0, Some(6.0), 60)), &model),
            3
        );
        let failed = ThroughputEntry {
            throughput_mbps: None,
            measured_at: Some(at(-60)),
            nominal_bitrate_mbps: Some(6.0),
            edge_ip: None,
        };
        assert_eq!(classify_throughput(Some(&stats), Some(&failed), &model), 2);
        assert_eq!(classify_throughput(Some(&stats), None, &model), 2);
    }

    #[test]
    fn the_two_prioritize_quality_orders_match_the_plugins_own_tests() {
        // The fixtures and both expected orders are Stream-Mapparr's own
        // (`tests/test_sort_prioritize_quality.py:52-100`).
        let stats_sd = json!({"width": 720, "height": 576, "source_fps": 25});
        let stats_uhd = json!({"width": 3840, "height": 2160, "source_fps": 50});
        let stats_hd = json!({"width": 1920, "height": 1080, "source_fps": 50});
        let streams = vec![
            view(1, "UK: SKY NEWS SD (Strong TV)", &stats_sd, 0),
            view(2, "UK: SKY NEWS HEVC 4K (Promax)", &stats_uhd, 2),
            view(3, "UK: SKY NEWS HD (Promax)", &stats_hd, 2),
        ];
        let quality_first = QualityModel {
            prioritize_quality: true,
            enable_throughput_sorting: false,
            ..model()
        };
        assert_eq!(
            ids(&sort_streams(&streams, &quality_first, &NoThroughput)),
            [2, 3, 1]
        );

        let source_first = QualityModel {
            prioritize_quality: false,
            enable_throughput_sorting: false,
            ..model()
        };
        assert_eq!(
            ids(&sort_streams(&streams, &source_first, &NoThroughput)),
            [1, 2, 3]
        );
    }

    #[test]
    fn a_placeholder_is_demoted_only_when_the_rule_is_on() {
        // `tests/test_sort_live_settings.py:20-28`: the slate claims 1080p while carrying
        // 190 kbps; the real stream is 720p.
        let slate = json!({"width": 1920, "height": 1080, "source_fps": 50, "video_bitrate": 190});
        let real = json!({"width": 1280, "height": 720, "source_fps": 50, "video_bitrate": 3000});
        let streams = vec![view(1, "slate", &slate, 0), view(2, "real", &real, 0)];

        let demoted = QualityModel {
            demote_content_starved: true,
            enable_throughput_sorting: false,
            ..model()
        };
        assert_eq!(
            ids(&sort_streams(&streams, &demoted, &NoThroughput)),
            [2, 1]
        );

        let plain = QualityModel {
            demote_content_starved: false,
            enable_throughput_sorting: false,
            ..model()
        };
        // With the rule off the higher resolution wins, whatever it carries.
        assert_eq!(ids(&sort_streams(&streams, &plain, &NoThroughput)), [1, 2]);
        assert_eq!(
            tier_label(Some(&slate), None, &demoted),
            CONTENT_STARVED_LABEL
        );
        assert_eq!(tier_label(Some(&slate), None, &plain), "unknown");
    }

    #[test]
    fn a_stream_with_no_resolution_sorts_behind_every_one_that_has_one() {
        let no_stats = json!({});
        let sd = json!({"width": 720, "height": 576, "source_fps": 25});
        let streams = vec![view(1, "unprobed", &no_stats, 0), view(2, "SD", &sd, 0)];
        let plain = QualityModel {
            enable_throughput_sorting: false,
            ..model()
        };
        assert_eq!(ids(&sort_streams(&streams, &plain, &NoThroughput)), [2, 1]);

        // With throughput sorting on, a HEALTHY measurement outranks the resolution tier
        // entirely (4786: the tier is prefixed to the whole key). Without one, the stream
        // with no resolution is behind every stream that has one.
        let mut throughput = MemoryThroughputSource::new();
        throughput.insert(
            1,
            ThroughputEntry {
                throughput_mbps: Some(50.0),
                measured_at: Some(at(-60)),
                nominal_bitrate_mbps: Some(2.5),
                edge_ip: None,
            },
        );
        assert_eq!(ids(&sort_streams(&streams, &model(), &throughput)), [1, 2]);
        let no_measurements = MemoryThroughputSource::new();
        assert_eq!(
            ids(&sort_streams(&streams, &model(), &no_measurements)),
            [2, 1]
        );
    }

    #[test]
    fn equal_keys_keep_the_order_they_arrived_in() {
        // `sorted(...)` is stable, which is what keeps a re-sort from reshuffling alternates
        // that measure the same.
        let stats = json!({"width": 1920, "height": 1080, "source_fps": 50});
        let streams = vec![
            view(5, "a", &stats, 0),
            view(3, "b", &stats, 0),
            view(9, "c", &stats, 0),
        ];
        let model = QualityModel {
            enable_throughput_sorting: false,
            ..model()
        };
        assert_eq!(
            ids(&sort_streams(&streams, &model, &NoThroughput)),
            [5, 3, 9]
        );
    }

    #[test]
    fn throughput_breaks_a_tie_between_equal_streams() {
        let stats = json!({"width": 1920, "height": 1080, "source_fps": 50});
        let streams = vec![view(1, "slow", &stats, 0), view(2, "fast", &stats, 0)];
        let mut throughput = MemoryThroughputSource::new();
        for (id, mbps) in [(1, 18.0), (2, 20.0)] {
            throughput.insert(
                id,
                ThroughputEntry {
                    throughput_mbps: Some(mbps),
                    measured_at: Some(at(-60)),
                    nominal_bitrate_mbps: Some(6.0),
                    edge_ip: None,
                },
            );
        }
        let sorted = sort_streams(&streams, &model(), &throughput);
        assert_eq!(ids(&sorted), [2, 1]);

        // A STALE measurement keeps its rate as the last tie-breaker even though its tier is
        // "unknown" (`-mbps`, 4809-4813).
        let mut stale = MemoryThroughputSource::new();
        stale.insert(
            1,
            ThroughputEntry {
                throughput_mbps: Some(25.0),
                measured_at: Some(at(-7200)),
                nominal_bitrate_mbps: Some(6.0),
                edge_ip: None,
            },
        );
        stale.insert(
            2,
            ThroughputEntry {
                throughput_mbps: Some(20.0),
                measured_at: Some(at(-7200)),
                nominal_bitrate_mbps: Some(6.0),
                edge_ip: None,
            },
        );
        assert_eq!(ids(&sort_streams(&streams, &model(), &stale)), [1, 2]);
    }

    #[test]
    fn a_healthy_tier_comes_before_resolution() {
        // 4786-4797: the throughput tier is prefixed to the whole key.
        let sd = json!({"width": 720, "height": 576, "source_fps": 25});
        let uhd = json!({"width": 3840, "height": 2160, "source_fps": 50});
        let streams = vec![view(1, "4K", &uhd, 0), view(2, "SD", &sd, 0)];
        let mut throughput = MemoryThroughputSource::new();
        throughput.insert(
            2,
            ThroughputEntry {
                throughput_mbps: Some(20.0),
                measured_at: Some(at(-60)),
                nominal_bitrate_mbps: Some(2.5),
                edge_ip: None,
            },
        );
        throughput.insert(
            1,
            ThroughputEntry {
                throughput_mbps: Some(1.0),
                measured_at: Some(at(-60)),
                nominal_bitrate_mbps: Some(25.0),
                edge_ip: None,
            },
        );
        assert_eq!(ids(&sort_streams(&streams, &model(), &throughput)), [2, 1]);
    }

    #[test]
    fn settings_resolve_defaults_the_way_the_plugin_resolvers_do() {
        // `_resolve_content_starved_floor` (4826-4842): a floor of 0 is the default, not "off".
        let mut settings = crate::settings::StreamsSettings {
            content_bitrate_floor_kbps: 0,
            probe_cache_ttl_minutes: 0,
            bitrate_safety_margin: f64::NAN,
            ..crate::settings::StreamsSettings::default()
        };
        let model = QualityModel::from_settings(&settings);
        assert_eq!(
            model.content_bitrate_floor_kbps,
            DEFAULT_CONTENT_BITRATE_FLOOR_KBPS
        );
        // A TTL of 0 is honoured, not defaulted: the plugin only falls back when the value is
        // not a number, and a 0 makes every measurement stale.
        assert_eq!(model.probe_cache_ttl_minutes, 0);
        assert_eq!(model.bitrate_safety_margin, DEFAULT_BITRATE_SAFETY_MARGIN);

        settings.audio_channels_priority = "7.1, Stereo".to_string();
        settings.bitrate_sort_step_kbps = 1000;
        let model = QualityModel::from_settings(&settings);
        assert_eq!(model.audio_channels_priority, vec!["7.1", "stereo"]);
        assert_eq!(model.bitrate_sort_step_kbps, 1000.0);
    }

    #[test]
    fn a_store_backed_source_reads_the_health_table() {
        let store = Store::in_memory().expect("store");
        store
            .upsert_stream_health(&crosstalk_core::store::StreamHealth {
                stream_id: 7,
                probed_at: Some("2026-11-01T11:59:00+00:00".to_string()),
                class: "alive".to_string(),
                error_type: None,
                throughput_mbps: Some(12.5),
                throughput_at: Some("2026-11-01T11:59:00+00:00".to_string()),
                edge_ip: Some("edge.example".to_string()),
                consecutive_failures: 0,
            })
            .expect("row");
        let source = StoreThroughputSource::load(&store).expect("load");
        let entry = source.entry(7).expect("entry");
        assert_eq!(entry.throughput_mbps, Some(12.5));
        assert_eq!(entry.measured_at, Some(at(-60)));
        assert_eq!(entry.edge_ip.as_deref(), Some("edge.example"));
        assert_eq!(source.entry(8), None);
    }

    /// A source with no measurements at all: the plugin's empty cache.
    struct NoThroughput;

    impl ThroughputSource for NoThroughput {
        fn entry(&self, _stream_id: i64) -> Option<ThroughputEntry> {
            None
        }
    }
}
