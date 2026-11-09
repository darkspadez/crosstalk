//! ffprobe: the command line and the JSON parse, ported from IPTV Checker's `check_stream`.
//!
//! The command is `check_stream` (4930) lines 5000-5057, flag for flag; the parse is lines
//! 5061-5190. Both are deliberately literal, because the output of a port is supposed to be
//! the plugin's output, and IPTV Checker's `stream_stats` is read by Dispatcharr's own player
//! menu as well as by crosstalk's sort.
//!
//! # The command
//!
//! ```text
//! ffprobe -print_format json -user_agent 'VLC/3.0.21 LibVLC/3.0.21'
//!         -timeout <connection timeout × 1e6>        # µs, network I/O
//!         -analyzeduration <probe timeout × 1e6>     # µs, wait for stream data
//!         -probesize 10000000                        # 10 MB, for slow starts
//!         -loglevel error -show_streams -show_packets -show_format
//!         -read_intervals %+8
//!         -protocol_whitelist <PROTOCOL_WHITELIST> -i <url>
//! ```
//!
//! The last line is a deliberate departure from the plugin, which passed the URL as a bare
//! positional argument: `-i` keeps a URL that starts with `-` from being read as an option
//! (`-report` writes a log file into the working directory), and the protocol whitelist keeps
//! a nested input (an HLS playlist naming `file:` segments, a `concat:` list) from opening
//! anything but the network. The URL's own scheme is checked before the tool is run at all
//! ([`crate::probe::classify::is_probeable_url`]). The parity harness compares the command
//! lines with this one difference normalised away.
//!
//! Two of those numbers are settings in the plugin and fixed here:
//!
//!   * `ffprobe_flags` is fixed at the plugin's DEFAULT_FFPROBE_FLAGS,
//!     `-show_streams,-show_packets,-loglevel error` — a flag set that breaks the parser would
//!     fail every stream at once, and `streams.toml` says so where the retired key would be;
//!   * `ffprobe_analysis_duration` is [`ANALYZE_SECONDS`]. The form's wording is "8 s of
//!     analysis before the 30-packet floor"; the plugin's shipped value was 5 s, and 8 s is the
//!     longer window that makes a packet-derived bitrate possible on a live HLS feed (the
//!     packet fallback needs 30 packets). The parity harness passes the same 8 s to the plugin,
//!     so the command lines still compare equal.
//!
//! The process timeout is the plugin's: `probe_timeout + analysis_duration + 5` seconds.
//!
//! # The parse
//!
//! Facts come from the FIRST video stream and the FIRST audio stream, and the fallbacks are
//! order-sensitive in a way that matters on live feeds:
//!
//!   1. `video.bit_rate` — rare on live MPEG-TS/HLS;
//!   2. `format.bit_rate` — the container "bandwidth", usually present on TS;
//!   3. packets — at least [`MIN_PACKETS_FOR_BITRATE_CALC`] (30) video packets, bitrate
//!      `bytes × 8 / (duration_time × 1000)`. HLS is the case: no per-stream bitrate and no
//!      container bitrate, so without this every HLS stream would rank as having none.
//!
//! A container `duration` is also recorded, because a continuous live stream reports none: a
//! finite one means ffprobe is reading a FILE, which is the placeholder-file fingerprint
//! (`_is_placeholder_file`, 4766).
//!
//! Deliberate non-port: a stream entry with no `codec_type` key raises in the plugin
//! (`s['codec_type']`) and the whole probe becomes `Other`/Dead. Here it is
//! [`Parsed::Malformed`] too, so the classification stays the same — but only after the JSON
//! itself parsed, which is what the plugin's `except Exception` conflated.

use crate::probe::tool::ToolSpec;
use serde_json::{Map, Value};
use std::time::Duration;

/// The user agent IPTV Checker sends, from `check_stream` 5014 and `_analyze_stream_content`
/// 4884. Not a setting: a provider that filters on it expects a player's.
pub const USER_AGENT: &str = "VLC/3.0.21 LibVLC/3.0.21";

/// `-probesize 10000000`: 10 MB of buffer, for streams that start slowly.
pub const PROBESIZE: u64 = 10_000_000;

/// `ffprobe_analysis_duration`, fixed (see the module doc). Also the `-read_intervals` window.
pub const ANALYZE_SECONDS: u64 = 8;

/// The plugin's buffer on the process timeout (`probe_timeout + analysis + 5`).
pub const TIMEOUT_BUFFER_SECONDS: u64 = 5;

/// `PluginConfig.MIN_PACKETS_FOR_BITRATE_CALC` (173): below this the packet-derived bitrate is
/// noise rather than a measurement.
pub const MIN_PACKETS_FOR_BITRATE_CALC: usize = 30;

/// `-protocol_whitelist`, for both tools: the network protocols the allow-listed URL schemes
/// need, plus what they open underneath (`tls`/`tcp`/`udp`, `crypto` for AES-128 HLS, `hls`
/// for `hls+http`, `httpproxy` when `http_proxy` is set — without it a proxied install would
/// read every stream as dead, and `data` for an HLS key or playlist given inline as a
/// `data:` URI, which some providers do). This is ffmpeg's own default nested whitelist for
/// an `http` input (`http,https,tls,rtp,tcp,udp,crypto,httpproxy,data`) plus the other
/// allow-listed schemes, so a stream ffprobe reads unrestricted still reads here. No `file`,
/// `pipe`, `concat` or `subfile`: nothing that reaches the local file system.
pub const PROTOCOL_WHITELIST: &str =
    "http,https,tls,tcp,udp,rtp,rtmp,rtmps,rtsp,srt,crypto,hls,httpproxy,data";

/// How long the ffprobe process may run for a connection timeout and probe timeout.
pub fn total_timeout(probe_timeout_seconds: u32) -> Duration {
    Duration::from_secs(u64::from(probe_timeout_seconds) + ANALYZE_SECONDS + TIMEOUT_BUFFER_SECONDS)
}

/// The ffprobe invocation for one URL, argument for argument as `check_stream` builds it.
pub fn command(
    executable: &str,
    url: &str,
    connection_timeout_seconds: u32,
    probe_timeout_seconds: u32,
) -> ToolSpec {
    let mut args: Vec<String> = Vec::with_capacity(16);
    for arg in ["-print_format", "json", "-user_agent", USER_AGENT] {
        args.push(arg.to_string());
    }
    args.push("-timeout".to_string());
    args.push((u64::from(connection_timeout_seconds) * 1_000_000).to_string());
    args.push("-analyzeduration".to_string());
    args.push((u64::from(probe_timeout_seconds) * 1_000_000).to_string());
    args.push("-probesize".to_string());
    args.push(PROBESIZE.to_string());
    // `ffprobe_flags` (fixed): `-loglevel error` first, then the two show flags, exactly as
    // the plugin's loop appends them.
    args.push("-loglevel".to_string());
    args.push("error".to_string());
    args.push("-show_streams".to_string());
    args.push("-show_packets".to_string());
    // Always added by the plugin so the container-level bitrate and duration are readable.
    args.push("-show_format".to_string());
    args.push("-read_intervals".to_string());
    args.push(format!("%+{ANALYZE_SECONDS}"));
    // Not the plugin's: see the module doc.
    args.push("-protocol_whitelist".to_string());
    args.push(PROTOCOL_WHITELIST.to_string());
    args.push("-i".to_string());
    args.push(url.to_string());
    ToolSpec::new(executable, args, total_timeout(probe_timeout_seconds))
}

/// What `-show_format` adds, in the plugin's `ffprobe_data` names.
///
/// Recorded for every stream that reports one, whether or not the placeholder detector is on,
/// because it is the fingerprint an operator can see in the CSV before deciding to act on it
/// (5193-5200).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExtraFacts {
    pub frame_count: Option<usize>,
    /// IC records `frames[0]['pts']`, or the string `N/A` when absent.
    pub first_frame_pts: Option<Value>,
    pub packet_count: Option<usize>,
    /// `calculated_bitrate_kbps`: the packet-derived bitrate, only when it was the one used.
    pub calculated_bitrate_kbps: Option<f64>,
    /// A finite duration means a file, not a broadcast.
    pub container_duration_seconds: Option<f64>,
    pub container_bitrate_kbps: Option<i64>,
    /// Filled by the content pass when silent-audio detection measured a level.
    pub mean_volume_db: Option<f64>,
}

/// The media facts an ALIVE stream reports, in the plugin's `dispatcharr_metadata` names.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MediaFacts {
    pub video_codec: Option<String>,
    /// `"{width}x{height}"`, the plugin's `resolution`.
    pub resolution: String,
    pub width: i64,
    pub height: i64,
    /// `round(parse_framerate(r_frame_rate), 1)`.
    pub source_fps: f64,
    pub pixel_format: Option<String>,
    /// Whole kbps, as the plugin rounds it before writing.
    pub video_bitrate: Option<i64>,
    pub audio_codec: Option<String>,
    pub sample_rate: Option<i64>,
    pub audio_channels: Option<String>,
    pub audio_bitrate: Option<f64>,
    pub stream_type: Option<String>,
    pub extra: ExtraFacts,
}

impl MediaFacts {
    /// `_get_stream_format`: UHD / FHD / HD / SD / Unknown, from the resolution string.
    pub fn format_label(&self) -> &'static str {
        stream_format(&self.resolution)
    }

    /// The plugin's `dispatcharr_metadata` for an ALIVE stream: ALL twelve keys, nulls kept.
    ///
    /// `_update_dispatcharr_metadata` reads the nulls and drops them, and an all-null dict is
    /// how the plugin says "clear"; producing that dict unchanged is what makes the merge rule
    /// comparable, key for key, against the plugin's own return value.
    pub fn dispatcharr_metadata(&self) -> Map<String, Value> {
        let mut metadata = Map::new();
        metadata.insert(
            "video_codec".to_string(),
            optional(self.video_codec.clone()),
        );
        metadata.insert(
            "resolution".to_string(),
            Value::String(self.resolution.clone()),
        );
        metadata.insert("width".to_string(), Value::from(self.width));
        metadata.insert("height".to_string(), Value::from(self.height));
        metadata.insert("source_fps".to_string(), number_value(self.source_fps));
        metadata.insert(
            "pixel_format".to_string(),
            optional(self.pixel_format.clone()),
        );
        metadata.insert("video_bitrate".to_string(), optional(self.video_bitrate));
        metadata.insert(
            "audio_codec".to_string(),
            optional(self.audio_codec.clone()),
        );
        metadata.insert("sample_rate".to_string(), optional(self.sample_rate));
        metadata.insert(
            "audio_channels".to_string(),
            optional(self.audio_channels.clone()),
        );
        metadata.insert(
            "audio_bitrate".to_string(),
            self.audio_bitrate.map(number_value).unwrap_or(Value::Null),
        );
        metadata.insert(
            "stream_type".to_string(),
            optional(self.stream_type.clone()),
        );
        metadata
    }

    /// The `stream_stats` keys an ALIVE probe writes, nulls dropped (`_update_dispatcharr_metadata`
    /// 5431-5448: "Filter out None values for cleaner storage").
    ///
    /// `width`/`height` are kept even at 0, because 0 is not None and the plugin stores it; that
    /// is also exactly the "0x0 resolution" evidence the match's dead-stream filter reads.
    pub fn stream_stats(&self) -> Map<String, Value> {
        let mut stats = Map::new();
        let mut put = |key: &str, value: Value| {
            stats.insert(key.to_string(), value);
        };
        if let Some(codec) = &self.video_codec {
            put("video_codec", Value::String(codec.clone()));
        }
        put("resolution", Value::String(self.resolution.clone()));
        put("width", Value::from(self.width));
        put("height", Value::from(self.height));
        put("source_fps", number_value(self.source_fps));
        if let Some(format) = &self.pixel_format {
            put("pixel_format", Value::String(format.clone()));
        }
        if let Some(bitrate) = self.video_bitrate {
            put("video_bitrate", Value::from(bitrate));
        }
        if let Some(codec) = &self.audio_codec {
            put("audio_codec", Value::String(codec.clone()));
        }
        if let Some(rate) = self.sample_rate {
            put("sample_rate", Value::from(rate));
        }
        if let Some(channels) = &self.audio_channels {
            put("audio_channels", Value::String(channels.clone()));
        }
        if let Some(bitrate) = self.audio_bitrate {
            put("audio_bitrate", number_value(bitrate));
        }
        if let Some(stream_type) = &self.stream_type {
            put("stream_type", Value::String(stream_type.clone()));
        }
        stats
    }
}

/// A value or an explicit null, which is how the plugin's metadata dict carries "unknown".
fn optional<T: Into<Value>>(value: Option<T>) -> Value {
    value.map(Into::into).unwrap_or(Value::Null)
}

/// JSON without an `f64` that serializes to `null` for a non-finite value.
fn number_value(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// What one `-print_format json` run yielded.
#[derive(Debug, Clone, PartialEq)]
pub enum Parsed {
    /// A video stream was found: the facts an Alive stream carries.
    Video(Box<MediaFacts>),
    /// `returncode == 0` but no video stream: an audio-only/radio stream, which the plugin
    /// classifies `No Video Stream` and then treats as Skipped.
    NoVideo,
    /// The JSON did not parse, or a stream entry was unusable. The plugin called this `Other`
    /// with the exception's text.
    Malformed(String),
}

/// Parse one ffprobe JSON document.
pub fn parse(stdout: &str) -> Parsed {
    let document: Value = match serde_json::from_str(stdout) {
        Ok(document) => document,
        Err(error) => return Parsed::Malformed(error.to_string()),
    };
    let streams = match document.get("streams") {
        Some(Value::Array(streams)) => streams.as_slice(),
        Some(Value::Null) | None => &[],
        Some(_) => return Parsed::Malformed("'streams' is not a list".to_string()),
    };

    // `next((s for s in … if s['codec_type'] == 'video'), None)`: every entry is inspected, so
    // an entry without the key raises even when an earlier entry matched. Kept, because it is
    // what the plugin does with a malformed corpus.
    let mut video: Option<&Map<String, Value>> = None;
    let mut audio: Option<&Map<String, Value>> = None;
    for stream in streams {
        let Some(object) = stream.as_object() else {
            return Parsed::Malformed("a stream is not an object".to_string());
        };
        let Some(kind) = object.get("codec_type").and_then(Value::as_str) else {
            return Parsed::Malformed("'codec_type'".to_string());
        };
        match kind {
            "video" if video.is_none() => video = Some(object),
            "audio" if audio.is_none() => audio = Some(object),
            _ => {}
        }
    }
    let Some(video) = video else {
        return Parsed::NoVideo;
    };

    let width = integer_field(video, "width");
    let height = integer_field(video, "height");
    let resolution = format!("{width}x{height}");
    let framerate = round_half_even(
        parse_framerate(
            video
                .get("r_frame_rate")
                .and_then(Value::as_str)
                .unwrap_or("0/1"),
        ),
        1,
    );
    let video_bitrate = video_bitrate(&document, video);
    let source_fps = framerate;

    let mut facts = MediaFacts {
        video_codec: string_field(video, "codec_name"),
        resolution,
        width,
        height,
        source_fps,
        pixel_format: string_field(video, "pix_fmt"),
        video_bitrate,
        stream_type: stream_type(&document),
        ..MediaFacts::default()
    };

    if let Some(audio) = audio {
        facts.audio_codec = string_field(audio, "codec_name");
        facts.sample_rate = audio.get("sample_rate").and_then(|value| match value {
            // `if sample_rate:` then `int(sample_rate)`, a failure reading as None.
            Value::Null => None,
            Value::String(text) if text.is_empty() => None,
            Value::String(text) => text.parse::<i64>().ok().filter(|rate| *rate != 0),
            Value::Number(number) => number.as_i64().filter(|rate| *rate != 0),
            _ => None,
        });
        facts.audio_channels = audio_channels(audio);
        // `if audio_stream.get('bit_rate'):`: a zero is falsy to the plugin, so it writes none.
        facts.audio_bitrate = audio
            .get("bit_rate")
            .and_then(truthy_float)
            .filter(|bps| *bps != 0.0)
            .map(|bps| bps / 1000.0);
    }

    // The optional evidence blocks, from what `-show_frames`/`-show_packets`/`-show_format`
    // returned. Recorded whether or not the detectors are on.
    let mut extra = ExtraFacts::default();
    if let Some(Value::Array(frames)) = document.get("frames") {
        extra.frame_count = Some(frames.len());
        extra.first_frame_pts = Some(
            frames
                .first()
                .and_then(|frame| frame.get("pts").cloned())
                .unwrap_or_else(|| Value::String("N/A".to_string())),
        );
    }
    if let Some(Value::Array(packets)) = document.get("packets") {
        extra.packet_count = Some(packets.len());
        if facts.video_bitrate.is_none() {
            if let Some(calculated) = packet_bitrate(packets, video) {
                facts.video_bitrate = Some(calculated.round_half_even_int());
                extra.calculated_bitrate_kbps = Some(calculated);
            }
        }
    }
    extra.container_duration_seconds = container_duration(&document);
    extra.container_bitrate_kbps = container_bitrate_kbps(&document);
    facts.extra = extra;

    Parsed::Video(Box::new(facts))
}

/// `_is_placeholder_file` (4766): a finite container duration, which a live stream never has.
pub fn is_placeholder_file(facts: &MediaFacts) -> bool {
    facts.extra.container_duration_seconds.is_some()
}

/// `int(video_stream.get('width', 0))`, tolerant of a string and of a missing/null key.
fn integer_field(object: &Map<String, Value>, key: &str) -> i64 {
    match object.get(key) {
        Some(Value::Number(number)) => number.as_i64().unwrap_or(0),
        Some(Value::String(text)) => text.parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn string_field(object: &Map<String, Value>, key: &str) -> Option<String> {
    object.get(key).and_then(Value::as_str).map(str::to_string)
}

/// `float(value)` for a value the plugin tested for truthiness (`if stream.get('bit_rate')`).
fn truthy_float(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

/// The bitrate sources, in the plugin's order: the stream's own, then the container's.
///
/// The packet fallback is applied in [`parse`], where the packet list is in hand; both sources
/// here read through Python truthiness (`if video_stream.get('bit_rate')`), so a zero behaves
/// like a missing key and ends at the packet fallback too.
fn video_bitrate(document: &Value, video: &Map<String, Value>) -> Option<i64> {
    let own = video
        .get("bit_rate")
        .and_then(truthy_float)
        .filter(|value| *value != 0.0);
    if let Some(value) = own {
        return Some((value / 1000.0).round_half_even_int());
    }
    document
        .get("format")
        .and_then(|format| format.get("bit_rate"))
        .and_then(truthy_float)
        .filter(|value| *value != 0.0)
        .map(|value| (value / 1000.0).round_half_even_int())
}

/// `(total_size × 8) / (total_duration × 1000)`, over the video stream's packets.
///
/// The plugin falls back to ALL packets when no packet carries the video stream's index, and
/// refuses a sample below [`MIN_PACKETS_FOR_BITRATE_CALC`] packets or a zero total duration.
pub fn packet_bitrate(packets: &[Value], video: &Map<String, Value>) -> Option<f64> {
    let index = video.get("index").and_then(Value::as_i64);
    let mut selected: Vec<&Value> = Vec::new();
    if let Some(index) = index {
        selected = packets
            .iter()
            .filter(|packet| packet.get("stream_index").and_then(Value::as_i64) == Some(index))
            .collect();
    }
    if selected.is_empty() {
        selected = packets.iter().collect();
    }
    if selected.len() < MIN_PACKETS_FOR_BITRATE_CALC {
        return None;
    }
    let mut total_size: u64 = 0;
    let mut durations: Vec<f64> = Vec::with_capacity(selected.len());
    for packet in selected {
        // `int(p.get('size', 0))` and `float(p.get('duration_time') or 0)`.
        total_size += packet
            .get("size")
            .and_then(|size| match size {
                Value::Number(number) => number
                    .as_i64()
                    .or_else(|| number.as_f64().map(|value| value as i64)),
                Value::String(text) => text.parse::<i64>().ok(),
                _ => None,
            })
            .unwrap_or(0)
            .max(0) as u64;
        durations.push(
            packet
                .get("duration_time")
                .and_then(truthy_float)
                .unwrap_or(0.0),
        );
    }
    let total_duration = python_sum(&durations);
    if total_duration <= 0.0 {
        return None;
    }
    Some((total_size as f64 * 8.0) / (total_duration * 1000.0))
}

/// `sum(float(x) for …)`, with CPython's compensated summation.
///
/// CPython 3.12 replaced the naive left-to-right float accumulation in `sum()` with Neumaier
/// compensation (`bltinmodule.c`, `builtin_sum_impl`). The difference is not academic here: 48
/// packet durations of 0.02 s sum to 0.9600000000000005 naively and to 0.96 compensated, which
/// moves the derived bitrate in its last three digits. IPTV Checker runs on that CPython, so a
/// plain fold would make every packet-derived bitrate differ from the plugin's by an ulp.
pub fn python_sum(values: &[f64]) -> f64 {
    let mut total = 0.0f64;
    let mut compensation = 0.0f64;
    for value in values {
        let next = total + value;
        if total.abs() >= value.abs() {
            compensation += (total - next) + value;
        } else {
            compensation += (value - next) + total;
        }
        total = next;
    }
    if compensation != 0.0 && compensation.is_finite() {
        total += compensation;
    }
    total
}

/// `_parse_container_duration` (4729): seconds, or None for a live stream and for junk.
pub fn container_duration(document: &Value) -> Option<f64> {
    let raw = document.get("format")?.get("duration")?;
    let seconds = match raw {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) if text.is_empty() || text == "N/A" => return None,
        Value::String(text) => text.parse::<f64>().ok()?,
        _ => return None,
    };
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    Some(seconds)
}

/// `_parse_container_bitrate_kbps` (4749): whole kbps, or None.
pub fn container_bitrate_kbps(document: &Value) -> Option<i64> {
    let raw = document.get("format")?.get("bit_rate")?;
    if matches!(raw, Value::String(text) if text.is_empty() || text == "N/A") {
        return None;
    }
    truthy_float(raw).map(|value| (value / 1000.0).round_half_even_int())
}

/// The container's own name, in the plugin's three special cases plus the first alias.
pub fn stream_type(document: &Value) -> Option<String> {
    let format = document.get("format")?;
    // `if probe_data.get('format'):` — an empty object is falsy, so the plugin leaves the
    // stream type unset rather than calling it "unknown".
    match format {
        Value::Object(map) if map.is_empty() => return None,
        Value::Object(_) => {}
        _ => return None,
    }
    let name = format
        .get("format_name")
        .and_then(Value::as_str)
        .unwrap_or("");
    if name.contains("mpegts") {
        Some("mpegts".to_string())
    } else if name.contains("hls") || name.contains("m3u8") {
        Some("hls".to_string())
    } else if name.contains("flv") {
        Some("flv".to_string())
    } else if name.is_empty() {
        Some("unknown".to_string())
    } else {
        Some(name.split(',').next().unwrap_or_default().to_string())
    }
}

/// `audio_stream.get('channel_layout') or audio_stream.get('channels')`, with the plugin's
/// count→layout table.
fn audio_channels(audio: &Map<String, Value>) -> Option<String> {
    let layout = audio
        .get("channel_layout")
        .and_then(Value::as_str)
        .filter(|layout| !layout.is_empty());
    if let Some(layout) = layout {
        return Some(layout.to_string());
    }
    match audio.get("channels") {
        Some(Value::Number(number)) => number.as_i64().map(channel_layout_name),
        Some(Value::String(text)) => Some(text.clone()),
        _ => None,
    }
}

/// `channel_map.get(audio_channels, f'{audio_channels}ch')`.
fn channel_layout_name(count: i64) -> String {
    match count {
        1 => "mono".to_string(),
        2 => "stereo".to_string(),
        6 => "5.1".to_string(),
        8 => "7.1".to_string(),
        other => format!("{other}ch"),
    }
}

/// `parse_framerate` (4656): `NUM/DEN`, or a plain number, else 0.
pub fn parse_framerate(text: &str) -> f64 {
    if let Some((numerator, denominator)) = text.split_once('/') {
        let (Ok(numerator), Ok(denominator)) =
            (numerator.parse::<f64>(), denominator.parse::<f64>())
        else {
            return 0.0;
        };
        if denominator == 0.0 {
            return 0.0;
        }
        numerator / denominator
    } else {
        text.parse::<f64>().unwrap_or(0.0)
    }
}

/// `_get_stream_format` (4644), including its `x`-only parse.
///
/// A pure extraction onto the shared model ([`crosstalk_core::quality::QualityTier`]):
/// the width bands are decided once in core, and this stays byte-identical by delegating
/// to them (the probe parity harness pins the boundary table).
pub fn stream_format(resolution: &str) -> &'static str {
    crosstalk_core::quality::stream_format_of(resolution)
}

/// `round(value, places)` with Python's semantics: half-to-even over the value's exact binary
/// form. Rust's `f64::round` is half-away-from-zero, which would turn 23.95 into 24.0 where
/// Python says 23.9.
pub fn round_half_even(value: f64, places: usize) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let text = format!("{value:.places$}");
    text.parse().unwrap_or(value)
}

/// `int(round(value))`, the plugin's rounding of a bitrate to whole kbps.
pub trait RoundHalfEven {
    fn round_half_even_int(self) -> i64;
}

impl RoundHalfEven for f64 {
    fn round_half_even_int(self) -> i64 {
        if !self.is_finite() {
            return 0;
        }
        format!("{self:.0}").parse().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_command_matches_the_plugins_flag_order() {
        let spec = command("/usr/local/bin/ffprobe", "http://host/live.ts", 10, 20);
        assert_eq!(
            spec.args,
            vec![
                "-print_format",
                "json",
                "-user_agent",
                "VLC/3.0.21 LibVLC/3.0.21",
                "-timeout",
                "10000000",
                "-analyzeduration",
                "20000000",
                "-probesize",
                "10000000",
                "-loglevel",
                "error",
                "-show_streams",
                "-show_packets",
                "-show_format",
                "-read_intervals",
                "%+8",
                "-protocol_whitelist",
                PROTOCOL_WHITELIST,
                "-i",
                "http://host/live.ts",
            ]
        );
        // The URL is always the value of `-i`, never a positional that could parse as a flag.
        let at = spec.args.iter().position(|arg| arg == "-i").expect("-i");
        assert_eq!(spec.args[at + 1], "http://host/live.ts");
        // Nothing local: ffmpeg's default nested whitelist for an http input is a subset,
        // so an unrestricted ffprobe's HLS (inline `data:` keys included) still reads here.
        let allowed: Vec<&str> = PROTOCOL_WHITELIST.split(',').collect();
        for local in ["file", "pipe", "concat", "subfile", "cache", "fd"] {
            assert!(!allowed.contains(&local), "{local}");
        }
        for needed in [
            "http",
            "https",
            "tls",
            "rtp",
            "tcp",
            "udp",
            "crypto",
            "httpproxy",
            "data",
        ] {
            assert!(allowed.contains(&needed), "{needed}");
        }
        // `probe_timeout + analysis + 5`.
        assert_eq!(spec.timeout, Duration::from_secs(20 + 8 + 5));
    }

    #[test]
    fn a_video_stream_reports_the_stats_iptv_checker_writes() {
        let parsed = parse(
            r#"{
              "streams": [
                {"index": 0, "codec_type": "video", "codec_name": "h264", "width": 1920,
                 "height": 1080, "r_frame_rate": "60000/1001", "pix_fmt": "yuv420p",
                 "bit_rate": "5000000"},
                {"index": 1, "codec_type": "audio", "codec_name": "aac", "sample_rate": "48000",
                 "channels": 2, "bit_rate": "128000"}
              ],
              "format": {"format_name": "mpegts", "duration": "N/A"}
            }"#,
        );
        let Parsed::Video(facts) = parsed else {
            panic!("expected video, got {parsed:?}");
        };
        assert_eq!(facts.resolution, "1920x1080");
        assert_eq!(facts.width, 1920);
        assert_eq!(facts.height, 1080);
        // 60000/1001 = 59.94 -> 59.9, one decimal like the plugin.
        assert_eq!(facts.source_fps, 59.9);
        assert_eq!(facts.video_bitrate, Some(5000));
        assert_eq!(facts.audio_channels.as_deref(), Some("stereo"));
        assert_eq!(facts.sample_rate, Some(48000));
        assert_eq!(facts.audio_bitrate, Some(128.0));
        assert_eq!(facts.stream_type.as_deref(), Some("mpegts"));
        assert_eq!(facts.format_label(), "FHD");

        let stats = facts.stream_stats();
        assert_eq!(
            stats.get("resolution"),
            Some(&Value::String("1920x1080".into()))
        );
        assert_eq!(stats.get("video_bitrate"), Some(&Value::from(5000)));
        assert_eq!(stats.get("source_fps"), Some(&serde_json::json!(59.9)));
        assert_eq!(stats.get("audio_bitrate"), Some(&serde_json::json!(128.0)));
    }

    #[test]
    fn a_live_stream_with_no_bitrate_falls_back_to_its_packets() {
        // The HLS shape: no per-stream bit_rate, no container bit_rate, 30+ packets.
        let mut packets = Vec::new();
        for index in 0..40 {
            packets.push(serde_json::json!({
                "stream_index": 0, "size": 10_000, "duration_time": "0.033333",
            }));
            let _ = index;
        }
        let document = serde_json::json!({
            "streams": [
                {"index": 0, "codec_type": "video", "width": 1280, "height": 720,
                 "r_frame_rate": "30/1"},
            ],
            "format": {"format_name": "hls,applehttp"},
            "packets": packets,
        });
        let Parsed::Video(facts) = parse(&document.to_string()) else {
            panic!("expected video");
        };
        // 400 000 bytes * 8 / (1.33332 s * 1000) = 2400.024 kbps, which the plugin rounds to
        // whole kbps before writing it.
        assert_eq!(facts.video_bitrate, Some(2400));
        let calculated = facts.extra.calculated_bitrate_kbps.expect("calculated");
        assert!(
            (calculated - 2400.024).abs() < 0.01,
            "calculated bitrate {calculated}"
        );
        assert_eq!(facts.stream_type.as_deref(), Some("hls"));
    }

    #[test]
    fn the_packet_duration_is_summed_the_way_cpython_sums_floats() {
        // 48 x 0.02: naive accumulation drifts, compensated summation does not.
        let values = vec![0.02f64; 48];
        let naive: f64 = values.iter().sum();
        assert_ne!(naive, 0.96, "the naive fold drifts: {naive:?}");
        assert_eq!(python_sum(&values), 0.96);
        assert_eq!(python_sum(&[0.5, 0.25]), 0.75);
        assert_eq!(python_sum(&[]), 0.0);
    }

    #[test]
    fn fewer_than_thirty_packets_are_not_a_measurement() {
        let packets: Vec<Value> = (0..29)
            .map(
                |_| serde_json::json!({"stream_index": 0, "size": 10_000, "duration_time": "0.03"}),
            )
            .collect();
        assert!(packet_bitrate(&packets, &Map::new()).is_none());
        // Zero total duration is not a measurement either.
        let packets: Vec<Value> = (0..40)
            .map(|_| serde_json::json!({"stream_index": 0, "size": 10_000}))
            .collect();
        assert!(packet_bitrate(&packets, &Map::new()).is_none());
    }

    #[test]
    fn an_audio_only_stream_is_no_video_rather_than_dead() {
        let parsed = parse(
            r#"{"streams":[{"codec_type":"audio","codec_name":"mp2"}],
                "format":{"format_name":"mp3"}}"#,
        );
        assert_eq!(parsed, Parsed::NoVideo);
        assert_eq!(parse(r#"{"streams":[]}"#), Parsed::NoVideo);
        assert_eq!(parse(r#"{"streams":null}"#), Parsed::NoVideo);
    }

    #[test]
    fn unparseable_json_and_a_stream_without_a_codec_type_are_malformed() {
        assert!(matches!(parse("not json"), Parsed::Malformed(_)));
        assert!(matches!(
            parse(r#"{"streams":[{"index":0}]}"#),
            Parsed::Malformed(message) if message.contains("codec_type")
        ));
    }

    #[test]
    fn a_container_duration_is_the_placeholder_fingerprint() {
        let file = serde_json::json!({
            "streams": [{"codec_type": "video", "width": 1920, "height": 1080}],
            "format": {"duration": "600.0", "bit_rate": "192953"},
        });
        let Parsed::Video(facts) = parse(&file.to_string()) else {
            panic!("expected video");
        };
        assert_eq!(facts.extra.container_duration_seconds, Some(600.0));
        assert_eq!(facts.extra.container_bitrate_kbps, Some(193));
        assert!(is_placeholder_file(&facts));

        // A live TS reports no duration at all. So does the string "N/A", which ffprobe
        // prints for a live feed's format.
        for format in [
            serde_json::json!({"format_name": "mpegts"}),
            serde_json::json!({"format_name": "mpegts", "duration": "N/A"}),
            serde_json::json!({"format_name": "mpegts", "duration": "0"}),
        ] {
            let document = serde_json::json!({
                "streams": [{"codec_type": "video", "width": 1280, "height": 720}],
                "format": format,
            });
            let Parsed::Video(facts) = parse(&document.to_string()) else {
                panic!("expected video");
            };
            assert!(!is_placeholder_file(&facts), "{format}");
        }
    }

    #[test]
    fn python_rounding_is_half_to_even() {
        // `round(0.25, 1)` is 0.2 and `round(0.35, 1)` is 0.3 in Python; half-away-from-zero
        // would give 0.3 and 0.4.
        assert_eq!(round_half_even(0.25, 1), 0.2);
        assert_eq!(round_half_even(0.35, 1), 0.3);
        assert_eq!(round_half_even(23.976, 1), 24.0);
        assert_eq!(round_half_even(29.97002997, 1), 30.0);
        // `int(round(x))` for bitrates.
        assert_eq!(2656.5f64.round_half_even_int(), 2656);
        assert_eq!(2657.5f64.round_half_even_int(), 2658);
        assert_eq!(2499.6f64.round_half_even_int(), 2500);
    }

    #[test]
    fn the_stream_format_ladder_is_the_plugins() {
        for (resolution, expected) in [
            ("3840x2160", "UHD"),
            ("3799x2160", "FHD"),
            ("3800x2160", "UHD"),
            ("1920x1080", "FHD"),
            ("1280x720", "HD"),
            ("1199x720", "SD"),
            ("720x576", "SD"),
            ("0x0", "Unknown"),
            ("unknown", "Unknown"),
        ] {
            assert_eq!(stream_format(resolution), expected, "{resolution}");
        }
    }

    #[test]
    fn channel_counts_map_to_layout_names() {
        let document = serde_json::json!({
            "streams": [
                {"codec_type": "video", "width": 1280, "height": 720},
                {"codec_type": "audio", "channels": 6},
            ],
            "format": {},
        });
        let Parsed::Video(facts) = parse(&document.to_string()) else {
            panic!("expected video");
        };
        assert_eq!(facts.audio_channels.as_deref(), Some("5.1"));

        // A layout string wins over the count, as the plugin's `or` reads it.
        let document = serde_json::json!({
            "streams": [
                {"codec_type": "video", "width": 1280, "height": 720},
                {"codec_type": "audio", "channels": 2, "channel_layout": "quad"},
            ],
            "format": {},
        });
        let Parsed::Video(facts) = parse(&document.to_string()) else {
            panic!("expected video");
        };
        assert_eq!(facts.audio_channels.as_deref(), Some("quad"));
    }
}
