//! `probe`: IPTV Checker's `load_groups` + `check_streams` and Stream-Mapparr's throughput
//! probe, as one action that measures every in-scope stream and records what it found.
//!
//! Port of IPTV Checker's `load_groups_action` (plugin.py:2763, the selection),
//! `check_streams_action` (2924) with both processing loops (2965 sequential, 3113 parallel),
//! `check_stream` (4930), `_analyze_stream_content` (4846) and `_update_dispatcharr_metadata`
//! (5386), plus Stream-Mapparr's `probe_throughput_action` (10278).
//!
//! # One action, not a pair (the shape decision)
//!
//! Every other verb in this app is a `<verb>_preview` / `<verb>_apply` pair because its write is
//! to CHANNEL data, where "what would change" is the question an operator asks before pressing
//! anything. A probe writes no channel: it writes two things only it owns —
//! `Stream.stream_stats` in Dispatcharr (the shared media facts players read) and the store's
//! `stream_health` row (what only crosstalk reads). So it is one action of kind
//! [`ActionKind::Write`](crosstalk_core::subapp::ActionKind::Write) — it does write, and it must
//! confirm — and `--dry-run` (the plan's allowance) runs the whole probe, records health, the
//! CSV and the verdicts, and skips the Dispatcharr write entirely. A pair would have had to
//! price every stream twice to preview a number.
//!
//! # Selection
//!
//! The shared `[scope]` decides the channels (IPTV Checker's Channel Profile + Channel Groups +
//! mode, which phase 1 already merged into `[scope]`), then:
//!
//!   * `check_alternative_streams` — every stream of the channel, or only its primary
//!     (`Channel.streams[0]`, the playback order Dispatcharr returns);
//!   * `only_visible_channels` — drop channels disabled in every profile;
//!   * `probe_unattached_streams` — add streams attached to no channel at all. IPTV Checker had
//!     no way to check an orphan stream, and a provider's new stream is exactly that until a
//!     match runs;
//!   * `probe_freshness_hours` — skip a stream whose `stream_health` row says it was probed
//!     inside the window. This is the port of the plugin's resume file's *effect* (a backlog
//!     that clears over several nights) without its bug: the plugin's idea of progress was a
//!     JSON file a settings change could invalidate, and it had no notion of "this stream was
//!     fine an hour ago".
//!
//! The queue is ordered by `(M3U account, stream id)`, so two runs over the same world probe in
//! the same order. Channel order is not used: a stream on three channels is one probe.
//!
//! # Concurrency, exactly as the plan decided
//!
//! One lane per M3U account, never above that account's `max_streams`, `probe_per_account_limit`
//! connections inside a lane and `stream_check_delay` between starts on the same account — all
//! of it [`crosstalk_core::limiter`], keyed `m3u:<id>`. Total parallelism therefore grows with
//! the number of accounts, which is what a per-account connection limit is keyed on. IPTV
//! Checker's global `parallel_workers` (2) is the setting this replaces; neither plugin capped
//! ITS stream checks per minute (IPTV Checker is paced by its workers, its per-stream delay and
//! its 429 backoff), so neither does this probe.
//!
//! `probe_rate_per_minute` throttles the throughput downloads ONLY, which is the one place
//! Stream-Mapparr used it (`probe_throughput_action`: a global rate cap plus a per-account
//! delay around the HTTP downloads). A throughput download also holds its own limiter permit:
//! it is a second connection to the same provider, so it counts against the same cap.
//!
//! # A windowed, resumable job
//!
//! The probe is the long one: at 20k channels / 50k streams one pass outlives a night. So it
//! follows the host's rules (`crosstalk_core::jobs`): between streams it checks the run's
//! deadline and returns [`JobOutcome::paused`] with the remaining queue checkpointed; the next
//! firing of `streams.probe` resumes that queue. Cancellation keeps what landed, records the run
//! cancelled, and leaves a checkpoint for tomorrow.
//!
//! # What it writes
//!
//!   * `stream_health`, one row per probed stream: `probed_at`, the fine class
//!     (`alive`/`dead`/`skipped`/`blank`/`frozen`/`silent`/`placeholder`), the error type,
//!     `consecutive_failures`, and the throughput measurement when one was taken. This replaces
//!     Stream-Mapparr's JSON throughput cache and IPTV Checker's results file;
//!   * `Stream.stream_stats`, read-merge-written per stream exactly as
//!     `_update_dispatcharr_metadata` did it (the base re-read from Dispatcharr just before
//!     the write, never the hours-old world): non-null keys merged on Alive, `{}` on Dead,
//!     untouched on Skipped (a skipped stream was never judged, so writing `0x0` would read as
//!     dead to every other reader — the plugin's own comment, 5361);
//!   * run rows: one per probed stream (`subject: Stream`, with what became of its
//!     `stream_stats` in `method`), and the per-channel verdicts triage
//!     reads ([`VERDICT_FIELD`], [`FORMAT_FIELD`]). The verdicts live in run rows rather than a
//!     table of their own because the run row already carries a `field`, a `before` and an
//!     `after`, and triage's rule is "select run rows by field, never by channel id alone";
//!   * one CSV, with IPTV Checker's columns plus its `ffprobe_*` evidence as fixed columns.
//!
//! # Divergences, each deliberate
//!
//!   * **a missing tool refuses the run** instead of marking every stream dead (see
//!     [`crate::probe::tool`]);
//!   * **retries are per stream**, in the plugin's retryable set and count, spaced
//!     `stream_check_delay × 3` apart — the plugin retried in global passes, which at 50k
//!     streams means re-walking the whole queue;
//!   * **duplicate probes are gone**: the plugin's loaded-channels file repeated a stream once
//!     per channel carrying it, so a stream on four channels cost four probes and four rows;
//!   * **an unchanged `stream_stats` is not rewritten** (the same end state, fewer requests);
//!   * **the ffprobe analysis window is a constant** ([`ffprobe::ANALYZE_SECONDS`]), because the
//!     settings form retired `ffprobe_analysis_duration`;
//!   * **throughput runs when `enable_throughput_sorting` is on**, not as a separate action, and
//!     it costs a second connection per stream. With the setting off no download happens at all;
//!   * **the CSV's column set is fixed** rather than the plugin's dynamic `ffprobe_*` columns,
//!     so two runs' reports can be diffed.

pub mod classify;
pub mod content;
pub mod ffprobe;
pub mod throughput;
pub mod tool;

#[cfg(test)]
mod tests;

use crate::settings::StreamsSettings;
use anyhow::Result;
use chrono::{DateTime, SecondsFormat, Utc};
use classify::Class;
use content::ContentOptions;
use crosstalk_core::channel_ops::ReportSpec;
use crosstalk_core::config::ScopeConfig;
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{
    Channel, DispatcharrClient, Stream, World, WorldKind, WorldKinds,
};
use crosstalk_core::jobs::{JobContext, PacedProgress, ProgressEvent};
use crosstalk_core::limiter::{Limiter, LimiterConfig};
use crosstalk_core::store::{RunRow, RunStatus, Store, StreamHealth, Subject};
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use ffprobe::{ExtraFacts, MediaFacts, Parsed};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use throughput::{ThroughputMeasure, ThroughputMeter};
use tool::{ToolRunner, ToolSpec};

/// The action id.
pub const ACTION: &str = "probe";

/// The report's title, without the dry-run suffix.
const REPORT_TITLE: &str = "Probe Streams";

/// The run-row field a per-STREAM probe result is recorded under (`subject: Stream`).
pub const STREAM_FIELD: &str = "probe";
/// The run-row field a per-CHANNEL verdict is recorded under: `dead`, `blank`,
/// `low_frame_rate`, `alive`, `skipped` or `inconclusive`.
///
/// `triage` reads exactly this field from the last probe run, and `restore` reads what triage
/// wrote. The plugin's own three
/// predicates are preserved in the values: `dead` is every stream Dead and none a black screen
/// (`_is_dead_nonblack`, 3319), `blank` is every stream Dead because it is a black screen
/// (`_is_black_screen`, 3323), `low_frame_rate` is every ALIVE stream below
/// [`classify::LOW_FRAMERATE_THRESHOLD`] (`rename_low_framerate_channels_action`, 3626).
pub const VERDICT_FIELD: &str = "probe_verdict";
/// The run-row field a channel's format tier is recorded under (`UHD`, `FHD`, `HD`, `SD`,
/// `Unknown`): what `add_video_format_suffix_action` (3686) appends.
pub const FORMAT_FIELD: &str = "probe_format";

/// How often progress is published, matching the plugin's two-second flush.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(2);

/// How many run rows go into one store transaction. At 20k channels / 50k streams a probe
/// writes more rows than any other action, and a single transaction for all of them would hold
/// the whole table twice (the vector and the statement).
const ROW_BATCH: usize = 5_000;

/// The CSV's columns: IPTV Checker's base twelve, with the stream's name added, and its
/// `ffprobe_*` evidence appended as fixed columns.
pub const COLUMNS: [&str; 22] = [
    "channel_id",
    "channel_name",
    "stream_id",
    "stream_name",
    "status",
    "format",
    "framerate_num",
    "error_type",
    "error",
    "retry_count",
    "connection_timeout_seconds",
    "probe_timeout_seconds",
    "ffprobe_monitoring_seconds",
    "container_duration_seconds",
    "container_bitrate_kbps",
    "calculated_bitrate_kbps",
    "packet_count",
    "frame_count",
    "mean_volume_db",
    "throughput_mbps",
    "edge_ip",
    "health_class",
];

// =======================================================================================
// Configuration
// =======================================================================================

/// Everything one probe run needs, resolved once from `streams.toml`.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub ffprobe_path: String,
    pub ffmpeg_path: String,
    /// `timeout`: the connection timeout (`-timeout`/`-rw_timeout`, in µs).
    pub connection_timeout: u32,
    /// `probe_timeout`: how long ffprobe waits for stream data (`-analyzeduration`).
    pub probe_timeout: u32,
    /// `dead_connection_retries`.
    pub retries: u32,
    /// `stream_check_delay`, in seconds: the limiter's per-account spacing, and the base of the
    /// retry backoff (`× 3`, as the plugin waits between retry passes).
    pub stream_check_delay: u32,
    pub streamlink_hosts: Vec<String>,
    pub content: ContentOptions,
    /// `placeholder_file_detection`.
    pub placeholder_detection: bool,
    /// `probe_duration_seconds` when throughput is measured, `None` when it is off.
    pub throughput: Option<Duration>,
}

impl ProbeConfig {
    pub fn from_settings(settings: &StreamsSettings) -> Self {
        Self {
            ffprobe_path: settings.ffprobe_path.clone(),
            ffmpeg_path: settings.ffmpeg_path.clone(),
            connection_timeout: settings.timeout,
            probe_timeout: settings.probe_timeout,
            retries: settings.dead_connection_retries,
            stream_check_delay: settings.stream_check_delay,
            streamlink_hosts: settings.streamlink_host_list(),
            content: ContentOptions {
                sample_seconds: settings.black_screen_sample_seconds,
                min_black_seconds: settings.black_screen_min_black_seconds,
                frozen_min_seconds: settings.frozen_video_min_seconds,
                want_black: settings.black_screen_detection,
                want_freeze: settings.frozen_video_detection,
                // An audio detector cannot speak for a stream with no audio track; the probe
                // clears this per stream once ffprobe has reported one.
                want_audio: settings.silent_audio_detection,
                audio_threshold_db: settings.silent_audio_max_db,
                timeout: Duration::from_secs(u64::from(settings.black_screen_ffmpeg_timeout)),
            },
            placeholder_detection: settings.placeholder_file_detection,
            throughput: settings
                .enable_throughput_sorting
                .then(|| Duration::from_secs(u64::from(settings.probe_duration_seconds))),
        }
    }

    /// ffmpeg is only needed when a content detector is armed.
    pub fn needs_ffmpeg(&self) -> bool {
        self.content.any()
    }

    /// How long a retry pass waits: the plugin's `delay × 3`.
    pub fn retry_backoff(&self) -> Duration {
        Duration::from_secs(u64::from(self.stream_check_delay) * 3)
    }

    /// The content options for one stream, with the audio detector dropped when there is no
    /// audio track (the plugin's `want_audio = … and audio_stream is not None`).
    pub fn content_for(&self, has_audio: bool) -> ContentOptions {
        let mut options = self.content;
        options.want_audio = self.content.want_audio && has_audio;
        options
    }
}

/// Where the probes' two outside dependencies come from.
///
/// The action builds the real ones ([`ProbeEngines::process`]); a test or the parity harness
/// supplies [`tool::FakeToolRunner`] and [`throughput::FakeMeter`], which is what makes "probe
/// without network" true rather than aspirational.
#[derive(Clone)]
pub struct ProbeEngines {
    pub tools: Arc<dyn ToolRunner>,
    pub meter: Arc<dyn ThroughputMeter>,
}

impl ProbeEngines {
    /// The real engines: `tokio::process` for the tools, HTTP for the throughput window.
    pub fn process(cancelled: Arc<AtomicBool>, config: &ProbeConfig) -> Self {
        Self {
            tools: Arc::new(tool::ProcessRunner::new(Arc::clone(&cancelled))),
            meter: match config.throughput {
                Some(_) => Arc::new(throughput::HttpMeter::new(cancelled)),
                None => Arc::new(throughput::NoMeter),
            },
        }
    }

    /// Nothing measured and nothing run: for tests that only care about selection.
    pub fn inert() -> Self {
        Self {
            tools: Arc::new(tool::FakeToolRunner::new()),
            meter: Arc::new(throughput::FakeMeter::new()),
        }
    }
}

impl std::fmt::Debug for ProbeEngines {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeEngines").finish_non_exhaustive()
    }
}

/// The rate-limit guard: IPTV Checker's `RateLimitGuard` (303), same constants.
///
/// Five 429s inside a minute trip a cooldown that starts at a minute and doubles up to ten,
/// decaying back to the base after five clean minutes. It exists because a provider that starts
/// answering 429 will keep doing it if the client keeps knocking: pausing the whole run is
/// cheaper than being banned, and the 429s themselves are recorded as Skipped, never Dead.
#[derive(Debug)]
pub struct Throttle {
    state: Mutex<ThrottleState>,
}

#[derive(Debug)]
struct ThrottleState {
    hits: VecDeque<Instant>,
    cooldown_until: Option<Instant>,
    next_cooldown: Duration,
    last_hit: Option<Instant>,
}

/// `RateLimitGuard`'s five constants.
pub const THROTTLE_WINDOW: Duration = Duration::from_secs(60);
pub const THROTTLE_TRIP_THRESHOLD: usize = 5;
pub const THROTTLE_BASE_COOLDOWN: Duration = Duration::from_secs(60);
pub const THROTTLE_MAX_COOLDOWN: Duration = Duration::from_secs(600);
pub const THROTTLE_DECAY: Duration = Duration::from_secs(300);

impl Throttle {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(ThrottleState {
                hits: VecDeque::new(),
                cooldown_until: None,
                next_cooldown: THROTTLE_BASE_COOLDOWN,
                last_hit: None,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ThrottleState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// `record_hit`: count a 429 and trip the cooldown when they come in bursts.
    pub fn record_hit(&self) {
        let now = Instant::now();
        let mut state = self.lock();
        state.hits.push_back(now);
        state.last_hit = Some(now);
        while let Some(oldest) = state.hits.front() {
            if now.saturating_duration_since(*oldest) < THROTTLE_WINDOW {
                break;
            }
            state.hits.pop_front();
        }
        let cooling = state.cooldown_until.is_some_and(|until| now < until);
        if state.hits.len() >= THROTTLE_TRIP_THRESHOLD && !cooling {
            let cooldown = state.next_cooldown;
            state.cooldown_until = Some(now + cooldown);
            state.next_cooldown = (state.next_cooldown * 2).min(THROTTLE_MAX_COOLDOWN);
            state.hits.clear();
        }
    }

    /// How long the current cooldown still has to run.
    pub fn cooldown(&self) -> Duration {
        let now = Instant::now();
        let mut state = self.lock();
        // `_next_cooldown` decays back to the base after a clean stretch.
        if let Some(last) = state.last_hit {
            if now.saturating_duration_since(last) > THROTTLE_DECAY {
                state.next_cooldown = THROTTLE_BASE_COOLDOWN;
            }
        }
        match state.cooldown_until {
            Some(until) if until > now => until - now,
            _ => Duration::ZERO,
        }
    }

    /// `wait_if_throttled`: sleep out the cooldown, giving up when the run is cancelled.
    pub async fn wait(&self, cancelled: &Arc<AtomicBool>) {
        loop {
            let remaining = self.cooldown();
            if remaining.is_zero() || cancelled.load(Ordering::SeqCst) {
                return;
            }
            // Re-read each tick so a fresh trip that EXTENDS the cooldown is honoured rather
            // than every worker waking on the original deadline.
            tokio::time::sleep(remaining.min(Duration::from_secs(1))).await;
        }
    }
}

impl Default for Throttle {
    fn default() -> Self {
        Self::new()
    }
}

// =======================================================================================
// One stream
// =======================================================================================

/// One stream, as the probe sees it.
#[derive(Debug, Clone, Copy)]
pub struct ProbeTarget<'a> {
    pub stream_id: i64,
    pub stream_name: &'a str,
    /// The M3U account, or 0 when the stream names none (Stream-Mapparr's
    /// `m3u_account_id or 0`, 10392).
    pub account: i64,
    pub url: &'a str,
}

/// What one stream's probe concluded.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamVerdict {
    pub stream_id: i64,
    pub stream_name: String,
    pub account: i64,
    pub class: Class,
    /// The plugin's own wording (`N/A`, `Timeout`, `Black Screen`, …).
    pub error_type: String,
    /// `stream_health.class`: finer than [`Class`] for the content deaths.
    pub health_class: &'static str,
    /// `UHD` / `FHD` / `HD` / `SD` / `Unknown`.
    pub format: &'static str,
    /// The probed frame rate, rounded to one decimal as the plugin stores it.
    pub framerate: f64,
    /// The media facts, on Alive. `None` for a dead or skipped stream.
    pub facts: Option<MediaFacts>,
    /// The ffprobe evidence block, which survives a dead verdict as the plugin kept it.
    pub extra: ExtraFacts,
    /// How many retries were spent before this verdict.
    pub retries: u32,
    /// The (URL-masked) failure text.
    pub error: String,
    /// The throughput measurement, when one was taken.
    pub throughput: Option<ThroughputMeasure>,
    /// The `stream_health.class` the store held before this run, when it held one.
    pub previous_class: Option<String>,
}

impl StreamVerdict {
    /// A verdict from its recorded parts: what triage, a test or the parity harness reads back.
    pub fn from_parts(
        stream_id: i64,
        class: Class,
        error_type: &str,
        framerate: f64,
        format: &'static str,
    ) -> Self {
        let target = ProbeTarget {
            stream_id,
            stream_name: "",
            account: 0,
            url: "",
        };
        let mut verdict = Self::new(&target, class, error_type);
        verdict.framerate = framerate;
        verdict.format = format;
        verdict
    }

    fn new(target: &ProbeTarget<'_>, class: Class, error_type: &str) -> Self {
        Self {
            stream_id: target.stream_id,
            stream_name: target.stream_name.to_string(),
            account: target.account,
            class,
            error_type: error_type.to_string(),
            health_class: classify::health_class(class, error_type),
            // The plugin's `default_return`: a stream that is not Alive reports `N/A` and 0 fps,
            // including the content deaths, which keep only their evidence block.
            format: "N/A",
            framerate: 0.0,
            facts: None,
            extra: ExtraFacts::default(),
            retries: 0,
            error: String::new(),
            throughput: None,
            previous_class: None,
        }
    }

    /// Whether this stream was judged, and judged well.
    pub fn alive(&self) -> bool {
        self.class == Class::Alive
    }

    /// Whether this stream was judged bad.
    pub fn dead(&self) -> bool {
        self.class == Class::Dead
    }

    /// `_is_dead_nonblack`: dead because of a probing failure, not a black screen.
    pub fn is_dead_nonblack(&self) -> bool {
        self.class == Class::Dead && self.error_type != classify::BLACK_SCREEN
    }

    /// `_is_black_screen`.
    pub fn is_black_screen(&self) -> bool {
        self.class == Class::Dead && self.error_type == classify::BLACK_SCREEN
    }

    /// The `stream_stats` an ALIVE stream merges, or `{}` when the stats must be cleared.
    pub fn merged_stats(&self, existing: &Value) -> Value {
        match (&self.facts, self.class) {
            (Some(facts), Class::Alive) => {
                let mut merged = existing.as_object().cloned().unwrap_or_default();
                for (key, value) in facts.stream_stats() {
                    merged.insert(key, value);
                }
                Value::Object(merged)
            }
            // Skipped is never written, so this arm is the dead one: clear, as the plugin does.
            _ => Value::Object(Map::new()),
        }
    }

    /// Whether this verdict is worth writing to `Stream.stream_stats` at all.
    pub fn writes_stats(&self) -> bool {
        self.class != Class::Skipped
    }
}

/// Probe one stream: ffprobe, retries, then the content pass.
///
/// The order is IPTV Checker's: skip Streamlink-only hosts before opening anything, wait out any
/// rate-limit cooldown, probe, retry only what a retry can fix, and only then run the content
/// pass on a stream that already looks alive. The throughput measurement is the caller's, so it
/// can hold its own connection permit.
pub async fn probe_stream(
    tools: &dyn ToolRunner,
    config: &ProbeConfig,
    throttle: &Throttle,
    cancelled: &Arc<AtomicBool>,
    target: &ProbeTarget<'_>,
) -> Result<StreamVerdict> {
    // 4947: a Streamlink-only host cannot be validated by ffprobe. Skipped, not Dead, so no
    // destructive action downstream ever treats a YouTube stream as a broken channel.
    if classify::is_streamlink_only(target.url, &config.streamlink_hosts) {
        let mut verdict = StreamVerdict::new(target, Class::Skipped, classify::SKIPPED);
        verdict.error = "Streamlink-only host (ffprobe cannot validate)".to_string();
        return Ok(verdict);
    }

    let mut attempt: u32 = 0;
    let mut verdict = loop {
        throttle.wait(cancelled).await;
        let mut verdict = probe_once(tools, config, target).await?;
        verdict.retries = attempt;
        if verdict.error_type == classify::CANCELLED {
            return Ok(verdict);
        }
        if verdict.class == Class::Dead
            && classify::is_retryable(&verdict.error_type)
            && attempt < config.retries
            && !cancelled.load(Ordering::SeqCst)
        {
            attempt += 1;
            // 3249: the plugin backs off three cooldowns between retry passes so a provider
            // can release the connection slot it is still holding.
            if !config.retry_backoff().is_zero() {
                tokio::time::sleep(config.retry_backoff()).await;
            }
            continue;
        }
        break verdict;
    };

    // 5357: a 429 is the provider asking us to slow down, not evidence the stream is broken.
    if verdict.error_type == classify::RATE_LIMITED {
        throttle.record_hit();
        verdict.class = Class::Skipped;
        verdict.health_class = classify::health_class(verdict.class, &verdict.error_type);
    }
    // 5371: an audio-only stream is a working stream with no video track.
    if verdict.error_type == classify::NO_VIDEO_STREAM {
        verdict.class = Class::Skipped;
        verdict.health_class = classify::health_class(verdict.class, &verdict.error_type);
    }
    Ok(verdict)
}

/// One attempt: the ffprobe pass, then (on success) the placeholder check and the content pass.
async fn probe_once(
    tools: &dyn ToolRunner,
    config: &ProbeConfig,
    target: &ProbeTarget<'_>,
) -> Result<StreamVerdict> {
    let spec = ffprobe::command(
        &config.ffprobe_path,
        target.url,
        config.connection_timeout,
        config.probe_timeout,
    );
    let output = tools.run(&spec).await?;

    // The operator (or a shutdown) killed the invocation: IPTV Checker called this `Cancelled`
    // and excluded it from its counts, because a killed probe says nothing about the stream.
    if output.cancelled {
        let mut verdict = StreamVerdict::new(target, Class::Skipped, classify::CANCELLED);
        verdict.error = "probe cancelled by the operator".to_string();
        return Ok(verdict);
    }
    if output.timed_out {
        // 5341: the plugin's wording, including the number it waited.
        let mut verdict = StreamVerdict::new(target, Class::Dead, "Timeout");
        verdict.error = classify::mask_url_in_error(
            &format!(
                "Connection timeout after {} seconds",
                spec.timeout.as_secs()
            ),
            target.url,
            target.stream_id,
        );
        return Ok(verdict);
    }
    if !output.success() {
        // 5297: the plugin classified the trimmed stderr, with a fallback when it was empty.
        let stderr = output.stderr.trim();
        let error_output = if stderr.is_empty() {
            "Stream not accessible"
        } else {
            stderr
        };
        let error_type = classify::classify_error(error_output, output.status);
        let mut verdict = StreamVerdict::new(target, Class::Dead, error_type);
        verdict.error = classify::mask_url_in_error(error_output, target.url, target.stream_id);
        return Ok(verdict);
    }

    match ffprobe::parse(&output.stdout) {
        Parsed::NoVideo => {
            // 5304: exit 0 with no video stream, which the caller reclassifies as Skipped.
            let mut verdict = StreamVerdict::new(target, Class::Dead, classify::NO_VIDEO_STREAM);
            verdict.error =
                classify::mask_url_in_error("No video stream found", target.url, target.stream_id);
            Ok(verdict)
        }
        // An exception inside the plugin's parse: `Other`, with the message.
        Parsed::Malformed(message) => {
            let mut verdict = StreamVerdict::new(target, Class::Dead, "Other");
            verdict.error = classify::mask_url_in_error(&message, target.url, target.stream_id);
            Ok(verdict)
        }
        Parsed::Video(facts) => finish_alive(tools, config, target, *facts).await,
    }
}

/// The alive path: placeholder evidence, then the one content pass, then the facts.
async fn finish_alive(
    tools: &dyn ToolRunner,
    config: &ProbeConfig,
    target: &ProbeTarget<'_>,
    mut facts: MediaFacts,
) -> Result<StreamVerdict> {
    let mut verdict = StreamVerdict::new(target, Class::Alive, classify::NO_ERROR);
    verdict.format = facts.format_label();
    verdict.framerate = facts.source_fps;

    // 5195: a finite container duration means ffprobe is reading a file, not a broadcast.
    if config.placeholder_detection && ffprobe::is_placeholder_file(&facts) {
        let duration = facts.extra.container_duration_seconds.unwrap_or_default();
        let mut dead = StreamVerdict::new(target, Class::Dead, classify::PLACEHOLDER_FILE);
        dead.error = format!(
            "Stream reports a fixed {duration:.1}s duration; a continuous live stream reports none"
        );
        dead.extra = facts.extra.clone();
        return Ok(dead);
    }

    // 5206: one decode pass, fail-open. A stream with no audio track cannot be silent.
    let options = config.content_for(facts.audio_codec.is_some() || facts.sample_rate.is_some());
    if options.any() {
        let spec = content::command(
            &config.ffmpeg_path,
            target.url,
            config.connection_timeout,
            &options,
        );
        // A tool that cannot START is a host fault, not a stream fault: it aborts the run
        // rather than marking a stream dead (see the module doc).
        let output = tools.run(&spec).await?;
        if !output.cancelled {
            let answers = content::parse(&output.stderr, output.success(), &options);
            // Exactly the plugin's order (5233-5268): a black or frozen verdict returns BEFORE
            // the volume is recorded, so its evidence block carries no `mean_volume_db` even
            // though volumedetect ran in the same pass; a silent verdict carries the level it
            // measured, because that is what the message quotes.
            let content_verdict = content::verdict(&answers, &options);
            let records_volume = options.want_audio
                && !matches!(
                    content_verdict,
                    Some(content::ContentVerdict::Black | content::ContentVerdict::Frozen)
                );
            if let Some(content_verdict) = content_verdict {
                let mut dead =
                    StreamVerdict::new(target, Class::Dead, content_verdict.error_type());
                dead.error = content_verdict.message();
                dead.extra = facts.extra.clone();
                if records_volume {
                    dead.extra.mean_volume_db = answers.audio_db;
                }
                return Ok(dead);
            }
            if records_volume {
                facts.extra.mean_volume_db = answers.audio_db;
            }
        }
    }

    verdict.extra = facts.extra.clone();
    verdict.facts = Some(facts);
    Ok(verdict)
}

/// Measure one alive or dead stream's delivery, holding the caller's connection permit.
pub async fn measure_throughput(
    meter: &dyn ThroughputMeter,
    config: &ProbeConfig,
    url: &str,
    cancelled: &AtomicBool,
) -> Option<ThroughputMeasure> {
    let duration = config.throughput?;
    if cancelled.load(Ordering::SeqCst) {
        return None;
    }
    Some(meter.measure(url, duration).await)
}

// =======================================================================================
// Selection
// =======================================================================================

/// One in-scope channel and the streams this run considers for it, in playback order.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelSelection {
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_group: String,
    pub streams: Vec<i64>,
}

/// One stream to probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueueEntry {
    /// The M3U account, which is the limiter key (0 when the stream names none).
    pub account: i64,
    pub stream_id: i64,
}

impl QueueEntry {
    /// The limiter key for this entry's account.
    pub fn key(&self) -> String {
        account_key(self.account)
    }
}

/// The limiter key for one M3U account.
pub fn account_key(account: i64) -> String {
    format!("m3u:{account}")
}

/// What a run selected, and what it left out.
#[derive(Debug, Clone, Default)]
pub struct Selection {
    /// The in-scope channels, in world order.
    pub channels: Vec<ChannelSelection>,
    /// The streams to probe, ordered `(account, stream id)`.
    pub queue: Vec<QueueEntry>,
    /// Streams skipped because their health row is inside `probe_freshness_hours`.
    pub skipped_fresh: usize,
    /// Streams skipped because Dispatcharr carries no URL for them (the plugin filtered these
    /// out of its queue the same way, `if s.get('url')`).
    pub skipped_no_url: usize,
    /// Stream ids a channel still lists that the stream list no longer holds: the plugin's
    /// results file simply had no URL for them either.
    pub skipped_missing: usize,
    /// Unattached streams the setting added to the selection.
    pub unattached: usize,
}

impl Selection {
    /// Every stream id the run considers, in queue order.
    pub fn stream_ids(&self) -> Vec<i64> {
        self.queue.iter().map(|entry| entry.stream_id).collect()
    }
}

/// Resolve the selection: the scope's channels, their streams, freshness and the orphan rule.
pub fn select(
    world: &World,
    scope: &ScopeConfig,
    settings: &StreamsSettings,
    health: &HashMap<i64, StreamHealth>,
    now: DateTime<Utc>,
) -> Result<Selection> {
    world.require(WorldKind::Channels)?;
    world.require(WorldKind::Streams)?;
    let mut scoped = crosstalk_core::scope::scope_channels(world, scope)?;

    // 2843: `only_visible_channels` keeps the channels enabled in at least one profile.
    if settings.only_visible_channels {
        world.require(WorldKind::Profiles)?;
        let visible: std::collections::HashSet<i64> = world
            .profiles
            .iter()
            .flat_map(|profile| profile.channels.iter().copied())
            .collect();
        scoped
            .channels
            .retain(|channel| visible.contains(&channel.id));
    }

    let by_id: HashMap<i64, &Stream> = world
        .streams
        .iter()
        .map(|stream| (stream.id, stream))
        .collect();
    let mut selection = Selection::default();
    let mut selected: BTreeMap<i64, i64> = BTreeMap::new();

    for channel in &scoped.channels {
        let ids: Vec<i64> = if settings.check_alternative_streams {
            channel.streams.clone()
        } else {
            // The primary stream is the first in the channel's playback order.
            channel.streams.iter().copied().take(1).collect()
        };
        for id in &ids {
            match by_id.get(id) {
                Some(stream) => {
                    selected.insert(*id, stream.m3u_account.unwrap_or(0));
                }
                None => selection.skipped_missing += 1,
            }
        }
        selection.channels.push(ChannelSelection {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_group: world.group_name(channel.channel_group_id),
            streams: ids,
        });
    }

    // Orphan streams: attached to no channel at all, in the WHOLE world — a channel outside the
    // scope must not make its streams look unattached.
    if settings.probe_unattached_streams {
        let attached: std::collections::HashSet<i64> = world
            .channels
            .iter()
            .flat_map(|channel: &Channel| channel.streams.iter().copied())
            .collect();
        for stream in &world.streams {
            if attached.contains(&stream.id) || selected.contains_key(&stream.id) {
                continue;
            }
            selected.insert(stream.id, stream.m3u_account.unwrap_or(0));
            selection.unattached += 1;
        }
    }

    let mut seen_no_url: std::collections::HashSet<i64> = Default::default();
    for (stream_id, account) in selected {
        if stream_has_no_url(by_id.get(&stream_id).copied()) {
            if seen_no_url.insert(stream_id) {
                selection.skipped_no_url += 1;
            }
            continue;
        }
        if is_fresh(health.get(&stream_id), settings.probe_freshness_hours, now) {
            selection.skipped_fresh += 1;
            continue;
        }
        selection.queue.push(QueueEntry { account, stream_id });
    }
    // `selected` was keyed by stream id, so ordering by account is the one explicit sort left;
    // `sort_by_key` is stable, so two runs agree exactly.
    selection
        .queue
        .sort_by_key(|entry| (entry.account, entry.stream_id));
    Ok(selection)
}

/// A stream without a URL cannot be probed; the plugin dropped it from its queue.
fn stream_has_no_url(stream: Option<&Stream>) -> bool {
    match stream {
        Some(stream) => !stream
            .url
            .as_deref()
            .map(str::trim)
            .is_some_and(|url| !url.is_empty()),
        None => true,
    }
}

/// Whether a stream's last media probe is inside the freshness window.
///
/// A row that carries no `probed_at` (a throughput-only measurement) is not fresh: its media
/// facts were never read. `0` hours means everything is due, which is the setting's wording.
pub fn is_fresh(health: Option<&StreamHealth>, freshness_hours: u32, now: DateTime<Utc>) -> bool {
    if freshness_hours == 0 {
        return false;
    }
    let Some(health) = health else {
        return false;
    };
    if health.class.is_empty() {
        return false;
    }
    let Some(probed_at) = health
        .probed_at
        .as_deref()
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|stamp| stamp.with_timezone(&Utc))
    else {
        return false;
    };
    now.signed_duration_since(probed_at) < chrono::Duration::hours(i64::from(freshness_hours))
}

/// The fingerprint of every setting that changes WHAT a probe measures.
///
/// IPTV Checker's `pending_resume.json` carries a `settings_fingerprint` and DISCARDS the resume
/// state when it no longer matches (`_apply_pending_resume_to_loaded_channels`, 1008): a backlog
/// collected under different detectors, timeouts or a different scope is not the backlog this
/// run would have queued, so continuing it would mix two definitions of "probed". Same rule, one
/// string instead of a dict.
pub fn probe_fingerprint(settings: &StreamsSettings, scope: &ScopeConfig) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let parts: [String; 26] = [
        scope.profiles.clone(),
        scope.groups.clone(),
        scope.ignore_groups.clone(),
        settings.check_alternative_streams.to_string(),
        settings.only_visible_channels.to_string(),
        settings.probe_unattached_streams.to_string(),
        settings.probe_freshness_hours.to_string(),
        settings.timeout.to_string(),
        settings.probe_timeout.to_string(),
        settings.dead_connection_retries.to_string(),
        settings.black_screen_detection.to_string(),
        settings.black_screen_sample_seconds.to_string(),
        settings.black_screen_min_black_seconds.to_string(),
        settings.frozen_video_detection.to_string(),
        settings.frozen_video_min_seconds.to_string(),
        settings.silent_audio_detection.to_string(),
        settings.silent_audio_max_db.to_string(),
        settings.placeholder_file_detection.to_string(),
        settings.streamlink_hosts.clone(),
        settings.ffprobe_path.clone(),
        settings.ffmpeg_path.clone(),
        settings.enable_throughput_sorting.to_string(),
        settings.probe_duration_seconds.to_string(),
        settings.probe_per_account_limit.to_string(),
        settings.stream_check_delay.to_string(),
        settings.probe_rate_per_minute.to_string(),
    ];
    parts.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Apply a resumed checkpoint to a freshly resolved queue.
///
/// The checkpoint is AUTHORITATIVE, like the plugin's `remaining_stream_ids`: a resumed run
/// probes exactly what it left, in the order it left it, and nothing else. A stream a match
/// attached overnight waits for the next fresh pass rather than jumping a backlog that is
/// already a night behind. Two ways out, both the plugin's: a settings fingerprint that no
/// longer matches discards the checkpoint, and a checkpoint whose ids are all gone from the
/// selection (channels removed, streams deleted) falls back to the full queue.
pub fn resume(
    queue: Vec<QueueEntry>,
    checkpoint: Option<&Value>,
    fingerprint: &str,
) -> Vec<QueueEntry> {
    let Some(checkpoint) = checkpoint else {
        return queue;
    };
    if checkpoint.get("fingerprint").and_then(Value::as_str) != Some(fingerprint) {
        if checkpoint.get("remaining").is_some() {
            tracing::warn!(
                "the probe checkpoint was collected under different settings; starting a fresh \
                 pass instead of resuming it"
            );
        }
        return queue;
    }
    let Some(remaining) = checkpoint.get("remaining").and_then(Value::as_array) else {
        return queue;
    };
    let mut order: HashMap<i64, usize> = HashMap::new();
    for (index, value) in remaining.iter().enumerate() {
        if let Some(id) = value.as_i64() {
            order.entry(id).or_insert(index);
        }
    }
    if order.is_empty() {
        return queue;
    }
    let mut resumed: Vec<QueueEntry> = queue
        .into_iter()
        .filter(|entry| order.contains_key(&entry.stream_id))
        .collect();
    if resumed.is_empty() {
        return Vec::new();
    }
    resumed.sort_by_key(|entry| order.get(&entry.stream_id).copied().unwrap_or(usize::MAX));
    resumed
}

// =======================================================================================
// Per-channel verdicts
// =======================================================================================

/// What a probe run says about a whole channel, in IPTV Checker's three predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelVerdict {
    /// Every stream Dead and none a black screen: `_is_dead_nonblack` on every row.
    Dead,
    /// Every stream Dead because it is a black screen.
    Blank,
    /// At least one Alive stream and every Alive stream below
    /// [`classify::LOW_FRAMERATE_THRESHOLD`].
    LowFrameRate,
    /// At least one Alive stream, and not all of them slow.
    Alive,
    /// Every stream Skipped: never dead, never alive.
    Skipped,
    /// A mix that matches no predicate — a dead stream beside a skipped one, a black screen
    /// beside a timeout. IPTV Checker acted on no such channel, and neither does triage.
    Inconclusive,
}

impl ChannelVerdict {
    /// Every verdict, in the order the counts are reported.
    pub const ALL: [Self; 6] = [
        Self::Dead,
        Self::Blank,
        Self::LowFrameRate,
        Self::Alive,
        Self::Skipped,
        Self::Inconclusive,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Dead => "dead",
            Self::Blank => "blank",
            Self::LowFrameRate => "low_frame_rate",
            Self::Alive => "alive",
            Self::Skipped => "skipped",
            Self::Inconclusive => "inconclusive",
        }
    }

    /// Parse the stored form, for triage.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "dead" => Some(Self::Dead),
            "blank" => Some(Self::Blank),
            "low_frame_rate" => Some(Self::LowFrameRate),
            "alive" => Some(Self::Alive),
            "skipped" => Some(Self::Skipped),
            "inconclusive" => Some(Self::Inconclusive),
            _ => None,
        }
    }
}

/// The channel verdict for its probed streams, from IPTV Checker's own predicates.
///
/// Order matters and is the plugin's: "every stream failed" is checked before "every alive
/// stream is slow". A channel with one slow alive stream and three dead ones is
/// `low_frame_rate`, not `dead` — the dead lookup needs EVERY row dead-nonblack and the alive
/// row fails it, while the low-framerate lookup only considers the alive rows (the plugin's
/// `_channels_where(..., among=Alive)`). The plugin's rename/move did the same with it.
pub fn verdict_for(streams: &[&StreamVerdict]) -> ChannelVerdict {
    if streams.is_empty() {
        return ChannelVerdict::Inconclusive;
    }
    if streams.iter().all(|verdict| verdict.is_dead_nonblack()) {
        return ChannelVerdict::Dead;
    }
    if streams.iter().all(|verdict| verdict.is_black_screen()) {
        return ChannelVerdict::Blank;
    }
    let alive: Vec<&&StreamVerdict> = streams
        .iter()
        .filter(|verdict| verdict.class == Class::Alive)
        .collect();
    if !alive.is_empty() {
        if alive
            .iter()
            .all(|verdict| classify::is_low_framerate(verdict.framerate))
        {
            return ChannelVerdict::LowFrameRate;
        }
        return ChannelVerdict::Alive;
    }
    if streams
        .iter()
        .all(|verdict| verdict.class == Class::Skipped)
    {
        return ChannelVerdict::Skipped;
    }
    ChannelVerdict::Inconclusive
}

/// The verdict's name, for a caller that has the streams but not the enum.
pub fn channel_verdict_name(streams: &[&StreamVerdict]) -> &'static str {
    verdict_for(streams).as_str()
}

/// The channel's format tier, from its LAST alive stream in playback order.
///
/// `add_video_format_suffix_action` (3732) builds `channel_formats` by overwriting as it walks
/// the results, so the last alive row of a channel wins. The results were in the channel's
/// playback order, so this walks the list backwards — deterministic where the plugin's answer
/// depended on row order.
pub fn format_for(streams: &[&StreamVerdict]) -> Option<&'static str> {
    streams
        .iter()
        .rev()
        .find(|verdict| verdict.class == Class::Alive)
        .map(|verdict| verdict.format)
}

/// A channel's verdict and format, ready to be written as run rows.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelResult {
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_group: String,
    pub verdict: ChannelVerdict,
    pub format: Option<&'static str>,
    /// The alive/dead/skipped counts and the dead streams' error types.
    pub reason: String,
    pub alive: usize,
    pub dead: usize,
    pub skipped: usize,
}

/// A channel's recorded verdict, as `triage` reads it back out of the run rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedVerdict {
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_group: String,
    pub verdict: ChannelVerdict,
    /// The channel's format tier from [`FORMAT_FIELD`], when it had an alive stream.
    pub format: Option<String>,
}

/// Read the per-channel verdicts a probe run recorded, for `triage` and `restore`.
///
/// The one place that knows the row shape: `field == "probe_verdict"` carries the verdict and
/// `field == "probe_format"` the format tier, and a channel the run did not judge is simply
/// absent. Step 11's rule is "select run rows by field, never by channel id alone", so this is
/// the reader a triage run uses instead of re-parsing the CSV or a results file.
pub fn recorded_verdicts(rows: &[RunRow]) -> Vec<RecordedVerdict> {
    let mut verdicts: std::collections::BTreeMap<i64, RecordedVerdict> =
        std::collections::BTreeMap::new();
    for row in rows {
        if row.subject != Subject::Channel {
            continue;
        }
        match row.field.as_str() {
            VERDICT_FIELD => {
                let Some(verdict) = row.after_value.as_deref().and_then(ChannelVerdict::parse)
                else {
                    continue;
                };
                verdicts.insert(
                    row.channel_id,
                    RecordedVerdict {
                        channel_id: row.channel_id,
                        channel_name: row.channel_name.clone(),
                        channel_group: row.channel_group.clone(),
                        verdict,
                        format: None,
                    },
                );
            }
            FORMAT_FIELD => {
                if let Some(entry) = verdicts.get_mut(&row.channel_id) {
                    entry.format = row.after_value.clone();
                }
            }
            _ => {}
        }
    }
    verdicts.into_values().collect()
}

/// How many of the app's recent runs [`last_verdicts`] looks at to explain a missing probe.
const RECENT_RUNS: usize = 200;

/// The newest probe's verdicts, or the reason there is none.
///
/// The run store is the only source: unlike the plugin's `results.json` there is no file to
/// read instead, so a probe that was killed before it finished is simply not a result set. A
/// SUCCEEDED run is required for the same reason the guide's scan requires one — a paused or
/// cancelled probe may hold partial rows, and a partial set would silently skip the channels
/// the run never reached. `triage` and `restore` both plan from this.
///
/// A windowed probe finishes its backlog over several runs: each window checkpoints the
/// remainder and the next run resumes it, so the verdicts of one pass are split across the
/// paused runs and the succeeded run that drained the queue. The verdicts here are the union
/// of that whole sequence — the newest succeeded run plus every finished run back to (but
/// excluding) the previous succeeded one, oldest first, newest winning per channel — which is
/// the plugin's accumulated `results.json`. A `--dry-run` probe counts like any other: it
/// records the same verdict rows and only skips the Dispatcharr write.
pub struct LastProbe {
    pub run_id: i64,
    pub started_at: String,
    /// Every run the verdicts were read from, newest first: the succeeded run, then the
    /// finished runs back to the previous succeeded one. One entry in the common case.
    pub run_ids: Vec<i64>,
    pub verdicts: Vec<RecordedVerdict>,
}

impl LastProbe {
    /// The verdicts in words, for a summary, a report preamble or a log line.
    pub fn describe(&self) -> String {
        let mut counts: std::collections::BTreeMap<&str, usize> = Default::default();
        for verdict in &self.verdicts {
            *counts.entry(verdict.verdict.as_str()).or_default() += 1;
        }
        let tally = counts
            .iter()
            .map(|(verdict, count)| format!("{count} {verdict}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "probe run #{} started {} judging {} channel(s){}{tally}{}",
            self.run_id,
            self.started_at,
            self.verdicts.len(),
            if tally.is_empty() { "" } else { ": " },
            if self.run_ids.len() > 1 {
                format!(" (across {} runs)", self.run_ids.len())
            } else {
                String::new()
            },
        )
    }
}

/// The newest succeeded `probe` run's per-channel verdicts.
pub fn last_verdicts(context: &JobContext) -> Result<LastProbe> {
    let store = context.store().ok_or_else(|| {
        anyhow::anyhow!(
            "this run has no run store, so it cannot read the last probe: run the action from \
             the daemon or with `crosstalk run`, which record their runs"
        )
    })?;
    let Some(run) = store.last_succeeded_run(crate::APP_ID, ACTION)? else {
        // "No probe at all" and "the probe did not finish" are different problems, and an
        // operator looking at the wrong one wastes the evening. The newest run of the action
        // says which it is.
        let newest = store
            .recent_runs_for(crate::APP_ID, RECENT_RUNS)?
            .into_iter()
            .find(|candidate| candidate.action == ACTION);
        if let Some(run) = newest {
            anyhow::bail!(
                "the newest `probe` run (#{} started {}) ended {} and is not a result set: its \
                 rows are partial, so acting on them would skip the channels it never reached. \
                 Run `probe` to completion first",
                run.id,
                run.started_at,
                run.status
            );
        }
        anyhow::bail!(
            "no finished `probe` run is recorded, so there is no result set to act on: run \
             `probe` first (its per-channel verdicts are what this action selects)"
        );
    };
    // The verdicts of the sequence that run completed: the run itself plus every finished
    // run back to (excluding) the previous succeeded one, oldest first so the newest verdict
    // per channel wins. A windowed probe splits one pass across a paused run per night and the
    // succeeded run that drained the queue; reading the newest run alone would silently drop
    // every channel the earlier windows judged. A finished run's rows are measurements even
    // when the run did not succeed — a pause, a cancel and a failure all keep what they
    // recorded — while a still-running run may still be writing and is never read.
    let recent = store.recent_runs_for(crate::APP_ID, RECENT_RUNS)?;
    let anchor = recent
        .iter()
        .position(|candidate| candidate.action == ACTION && candidate.id == run.id)
        .unwrap_or(0);
    let mut sequence = vec![run.id];
    for candidate in recent.iter().skip(anchor + 1) {
        if candidate.action != ACTION {
            continue;
        }
        // A succeeded run is the previous complete result set, which ends the sequence: its
        // channels are either re-judged above (newer wins) or older than this sequence. A
        // running run may still be writing, so it is never read (the one-live-run guard
        // refuses a second live run anyway, which makes this free).
        if candidate.status == RunStatus::Succeeded || candidate.status == RunStatus::Running {
            break;
        }
        sequence.push(candidate.id);
    }
    sequence.reverse();
    // The two fields the reader understands, not the whole run: the per-stream rows of a 50k
    // stream probe are an order of magnitude more than the verdicts triage needs.
    let mut merged: std::collections::BTreeMap<i64, RecordedVerdict> =
        std::collections::BTreeMap::new();
    for run_id in &sequence {
        let rows = store.rows_of_fields(*run_id, &[VERDICT_FIELD, FORMAT_FIELD])?;
        for verdict in recorded_verdicts(&rows) {
            merged.insert(verdict.channel_id, verdict);
        }
    }
    let mut run_ids = sequence;
    run_ids.reverse();
    Ok(LastProbe {
        run_id: run.id,
        started_at: run.started_at,
        run_ids,
        verdicts: merged.into_values().collect(),
    })
}

/// Say which result set a run is about to plan from.
pub fn announce(context: &JobContext, action: &str, probe: &LastProbe) {
    context.progress(&ProgressEvent::new(
        action,
        0,
        probe.verdicts.len() as u64,
        format!("planning from {}", probe.describe()),
    ));
}

/// Roll the per-stream verdicts up per in-scope channel.
pub fn channel_results(
    selection: &Selection,
    verdicts: &HashMap<i64, StreamVerdict>,
) -> Vec<ChannelResult> {
    selection
        .channels
        .iter()
        .filter_map(|channel| {
            let judged: Vec<&StreamVerdict> = channel
                .streams
                .iter()
                .filter_map(|id| verdicts.get(id))
                .collect();
            // A channel whose streams were not judged (a cancelled run, a backlog that did not
            // reach it) gets NO verdict row: `inconclusive` would be a claim the run cannot
            // make, and triage must not read one.
            if judged.is_empty() {
                return None;
            }
            let alive = judged.iter().filter(|v| v.class == Class::Alive).count();
            let dead = judged.iter().filter(|v| v.class == Class::Dead).count();
            let skipped = judged.iter().filter(|v| v.class == Class::Skipped).count();
            let mut by_error: HashMap<&str, usize> = HashMap::new();
            for verdict in judged.iter().filter(|v| v.class == Class::Dead) {
                *by_error.entry(verdict.error_type.as_str()).or_default() += 1;
            }
            let mut errors: Vec<(&str, usize)> = by_error.into_iter().collect();
            errors.sort();
            let reason = format!(
                "{alive} alive, {dead} dead, {skipped} skipped of {} stream(s){}",
                channel.streams.len(),
                if errors.is_empty() {
                    String::new()
                } else {
                    format!(
                        ". Dead: {}",
                        errors
                            .iter()
                            .map(|(error_type, count)| format!("{count}× {error_type}"))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )
                }
            );
            Some(ChannelResult {
                channel_id: channel.channel_id,
                channel_name: channel.channel_name.clone(),
                channel_group: channel.channel_group.clone(),
                verdict: verdict_for(&judged),
                format: format_for(&judged),
                reason,
                alive,
                dead,
                skipped,
            })
        })
        .collect()
}

// =======================================================================================
// Workers
// =======================================================================================

/// Everything one lane needs, resolved once and shared.
struct Session {
    config: ProbeConfig,
    tools: Arc<dyn ToolRunner>,
    meter: Arc<dyn ThroughputMeter>,
    limiter: Limiter,
    /// Stream-Mapparr's global `probe_rate_per_minute`, on the throughput downloads only: the
    /// ffprobe checks above are paced per account, never by this cap.
    throughput_rate: Limiter,
    throttle: Arc<Throttle>,
    cancelled: Arc<AtomicBool>,
    /// Set by any lane that saw the window close, so every lane stops taking work.
    stop: AtomicBool,
    deadline: Option<DateTime<Utc>>,
    streams: HashMap<i64, Stream>,
    done: AtomicUsize,
    progress: Mutex<PacedProgress>,
    /// Where each batch's `stream_health` rows go as soon as the batch finishes, so a run
    /// killed mid-night (a crash, a restart) keeps what it probed: those streams are fresh
    /// afterwards and the next run skips them. `None` in a bare test context.
    store: Option<Store>,
    /// The health the store held when the run started: the previous class and failure count
    /// each new row builds on. A stream is probed once per run, so it never goes stale.
    health: HashMap<i64, StreamHealth>,
    health_written: AtomicUsize,
}

/// What one lane produced.
struct LaneResult {
    verdicts: Vec<StreamVerdict>,
    remaining: Vec<QueueEntry>,
}

impl Session {
    fn should_stop(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
            || self.cancelled.load(Ordering::SeqCst)
            || self.deadline.is_some_and(|deadline| Utc::now() >= deadline)
    }

    fn advance(&self, phase: &str) {
        let done = self.done.fetch_add(1, Ordering::SeqCst) + 1;
        if let Ok(mut progress) = self.progress.lock() {
            progress.set(done as u64, phase);
        }
    }
}

/// Probe one queue entry: one connection for the probe, one for the throughput window.
async fn probe_entry(session: &Arc<Session>, entry: QueueEntry) -> Result<Option<StreamVerdict>> {
    let Some(stream) = session.streams.get(&entry.stream_id) else {
        return Ok(None);
    };
    let Some(url) = stream.url.as_deref().filter(|url| !url.trim().is_empty()) else {
        return Ok(None);
    };
    let target = ProbeTarget {
        stream_id: stream.id,
        stream_name: &stream.name,
        account: entry.account,
        url,
    };
    let key = entry.key();

    // The permit is held for the WHOLE attempt: a provider counts the connection, not the
    // subprocess. A cancelled wait returns no verdict, and the entry stays queued.
    let Ok(permit) = session
        .limiter
        .acquire_or_cancel(&key, &session.cancelled)
        .await
    else {
        return Ok(None);
    };
    let verdict = probe_stream(
        session.tools.as_ref(),
        &session.config,
        &session.throttle,
        &session.cancelled,
        &target,
    )
    .await;
    drop(permit);
    let mut verdict = verdict?;

    // Stream-Mapparr's measurement, when the sort would use it. Skipped streams are not
    // measured: nothing about them is being judged, and a Streamlink-only URL cannot be read
    // as a transport stream anyway. The download holds a limiter permit (a second connection
    // to the same provider) AND passes the global per-minute gate, which throttles the
    // downloads only — never the ffprobe checks above.
    if verdict.class != Class::Skipped && session.config.throughput.is_some() {
        // The rate gate is taken first and dropped: it counts starts, so holding it would not
        // pace anything, while holding a connection permit for a minute-long rate wait would
        // idle a slot the ffprobe checks could use.
        if session
            .throughput_rate
            .acquire_or_cancel(&key, &session.cancelled)
            .await
            .is_err()
        {
            return Ok(Some(verdict));
        }
        if let Ok(permit) = session
            .limiter
            .acquire_or_cancel(&key, &session.cancelled)
            .await
        {
            verdict.throughput = measure_throughput(
                session.meter.as_ref(),
                &session.config,
                url,
                &session.cancelled,
            )
            .await;
            drop(permit);
        }
    }
    Ok(Some(verdict))
}

/// Run one M3U account's queue: `permits` probes at a time, stopping on pause or cancel.
async fn run_lane(session: Arc<Session>, mut queue: VecDeque<QueueEntry>) -> Result<LaneResult> {
    let permits = queue
        .front()
        .map(|entry| session.limiter.permits_for(&entry.key()))
        .unwrap_or(1)
        .max(1);
    let mut verdicts = Vec::new();
    let mut unprobed: Vec<QueueEntry> = Vec::new();
    while !queue.is_empty() {
        if session.should_stop() {
            break;
        }
        let batch: Vec<QueueEntry> = queue.iter().copied().take(permits).collect();
        for _ in 0..batch.len() {
            queue.pop_front();
        }

        let batch_start = verdicts.len();
        let mut set = tokio::task::JoinSet::new();
        for entry in batch {
            let session = Arc::clone(&session);
            set.spawn(async move { (entry, probe_entry(&session, entry).await) });
        }
        while let Some(joined) = set.join_next().await {
            let (entry, result) = joined?;
            match result? {
                Some(verdict) if verdict.error_type == classify::CANCELLED => {
                    // Killed mid-flight: record nothing (no health row, no CSV row) and keep the
                    // stream for the next run. Pushing it back on the queue would spin, because
                    // a cancelled run's should_stop only ends the loop on the next iteration.
                    unprobed.push(entry);
                }
                Some(verdict) => {
                    verdicts.push(verdict);
                    session.advance("probing streams");
                }
                // A stream the world lost between selection and the probe: keep it for the next
                // run rather than dropping it silently.
                None => unprobed.push(entry),
            }
        }
        // The batch's health now, not at the end of the run: hours of probing must survive a
        // run that never reaches its end.
        if let Some(store) = &session.store {
            let written =
                write_health(store, &session.health, &verdicts[batch_start..], Utc::now())?;
            session.health_written.fetch_add(written, Ordering::SeqCst);
        }
    }
    if session.should_stop() {
        // Tell the other lanes too: a closed window is the run's, not one lane's.
        session.stop.store(true, Ordering::SeqCst);
    }
    let mut remaining: Vec<QueueEntry> = unprobed;
    remaining.extend(queue);
    Ok(LaneResult {
        verdicts,
        remaining,
    })
}

// =======================================================================================
// The action
// =======================================================================================

/// The action, with the real ffprobe/ffmpeg/HTTP engines.
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    let config = ProbeConfig::from_settings(settings);
    let engines = ProbeEngines::process(context.cancel_flag(), &config);
    run_with(app, context, settings, dry_run, engines).await
}

/// The action, with the engines supplied: what a test and the parity harness drive.
pub async fn run_with(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
    dry_run: bool,
    engines: ProbeEngines,
) -> Result<JobOutcome> {
    let client = context.dispatcharr()?;
    let scope = context.config().scope.clone();
    let config = ProbeConfig::from_settings(settings);

    context.progress(&ProgressEvent::new(
        "streams.probe",
        0,
        0,
        "loading Dispatcharr data",
    ));
    let kinds = WorldKinds::only(WorldKind::Channels)
        | WorldKind::Groups
        | WorldKind::Profiles
        | WorldKind::Streams
        | WorldKind::M3uAccounts;
    let world = client.load_world_with(kinds).await?;

    // The store's `stream_health` is both the freshness evidence and where this run's verdicts
    // go. A context without a store (a bare test context) probes everything and writes no
    // health; the CSV and the outcome still say what it found.
    let store = context.store().cloned();
    let health = match &store {
        Some(store) => store.stream_health_map()?,
        None => HashMap::new(),
    };
    let now = Utc::now();
    let mut selection = select(&world, &scope, settings, &health, now)?;
    if selection.channels.is_empty() {
        return Ok(JobOutcome::new(
            "No channels are in scope. Check the group and profile settings in crosstalk.toml.",
        ));
    }
    let fingerprint = probe_fingerprint(settings, &scope);
    selection.queue = resume(
        selection.queue,
        context.load_checkpoint().as_ref(),
        &fingerprint,
    );
    let total = selection.queue.len();

    let mut clauses: Vec<String> = Vec::new();
    if selection.skipped_fresh > 0 {
        clauses.push(format!(
            "{} stream(s) probed within {} hour(s) were skipped",
            selection.skipped_fresh, settings.probe_freshness_hours
        ));
    }
    if selection.skipped_no_url > 0 {
        clauses.push(format!(
            "{} stream(s) carry no URL and cannot be probed",
            selection.skipped_no_url
        ));
    }
    if selection.skipped_missing > 0 {
        clauses.push(format!(
            "{} stream id(s) a channel lists are no longer in the stream list",
            selection.skipped_missing
        ));
    }
    if total == 0 {
        // A drained queue means the backlog is finished: the checkpoint is not needed again.
        context.clear_checkpoint()?;
        let mut builder = OutcomeBuilder::new(format!(
            "Nothing to probe: {} channel(s) in scope and no stream is due.",
            selection.channels.len()
        ))
        .count("selected", selection.channels.len() as i64)
        .count("queued", 0)
        .count("probed", 0)
        .count("skipped_fresh", selection.skipped_fresh as i64)
        .count("skipped_no_url", selection.skipped_no_url as i64)
        .count("skipped_missing", selection.skipped_missing as i64);
        for clause in clauses {
            builder = builder.note(clause);
        }
        return Ok(builder.build());
    }

    engines.tools.preflight(
        &config.ffprobe_path,
        config.needs_ffmpeg().then_some(config.ffmpeg_path.as_str()),
    )?;

    let by_id: HashMap<i64, &Stream> = world
        .streams
        .iter()
        .map(|stream| (stream.id, stream))
        .collect();
    let mut streams: HashMap<i64, Stream> = HashMap::with_capacity(total);
    for entry in &selection.queue {
        if let Some(stream) = by_id.get(&entry.stream_id) {
            streams.insert(stream.id, (*stream).clone());
        }
    }

    let session = Arc::new(Session {
        config: config.clone(),
        tools: Arc::clone(&engines.tools),
        meter: Arc::clone(&engines.meter),
        limiter: build_limiter(settings, &world),
        throughput_rate: build_throughput_limiter(settings),
        throttle: Arc::new(Throttle::new()),
        cancelled: context.cancel_flag(),
        stop: AtomicBool::new(false),
        deadline: context.deadline(),
        streams,
        done: AtomicUsize::new(0),
        progress: Mutex::new(context.paced("streams.probe", total as u64, PROGRESS_INTERVAL)),
        store: store.clone(),
        health,
        health_written: AtomicUsize::new(0),
    });

    // One lane per account: a provider counts connections per account, so the parallelism that
    // matters is the number of accounts.
    let mut queues: BTreeMap<i64, VecDeque<QueueEntry>> = BTreeMap::new();
    for entry in &selection.queue {
        queues.entry(entry.account).or_default().push_back(*entry);
    }
    let mut handles = Vec::with_capacity(queues.len());
    for (_, queue) in queues {
        let session = Arc::clone(&session);
        handles.push(tokio::spawn(run_lane(session, queue)));
    }
    let mut verdicts: Vec<StreamVerdict> = Vec::with_capacity(total);
    let mut remaining: Vec<QueueEntry> = Vec::new();
    for handle in handles {
        let lane = handle.await??;
        verdicts.extend(lane.verdicts);
        remaining.extend(lane.remaining);
    }
    verdicts.sort_by_key(|verdict| verdict.stream_id);
    remaining.sort();
    let cancelled = context.is_cancelled();
    let paused = !remaining.is_empty() && !cancelled;

    // The checkpoint: the remaining queue, so tomorrow continues. Written on a pause and on a
    // cancel (the host's rule is that a cancel keeps its place), cleared when the queue drained.
    // Written when the run STOPS (a pause, or a cancel), not per stream: the store's health
    // rows, written after every batch, are the finer-grained record, and they are what makes an
    // interrupted run resume — a stream probed before a crash is fresh afterwards, so the next
    // run skips it. This is the plugin's resume file with the state moved where it belongs.
    if remaining.is_empty() {
        context.clear_checkpoint()?;
    } else {
        context.save_checkpoint(&json!({
            "remaining": remaining.iter().map(|entry| entry.stream_id).collect::<Vec<i64>>(),
            "probed": verdicts.len(),
            "fingerprint": fingerprint,
        }))?;
    }

    // Attach what the store already knew, so a row can show the transition.
    for verdict in &mut verdicts {
        verdict.previous_class = session
            .health
            .get(&verdict.stream_id)
            .map(|row| row.class.clone());
    }

    // `stream_health` went in batch by batch (crosstalk's own record, so it survives a
    // Dispatcharr write that fails, and a run that never gets here).
    let health_written = session.health_written.load(Ordering::SeqCst);

    // `stream_stats`, read-merge-write, skipped entirely on a dry run. The merge base is
    // re-read now, not taken from the world loaded hours ago: Dispatcharr's live proxy writes
    // `stream_stats` while a stream plays, and merging over the old snapshot would revert it.
    let stats = if dry_run || !verdicts.iter().any(StreamVerdict::writes_stats) {
        HashMap::new()
    } else {
        match client.streams().await {
            Ok(fresh) => {
                let current: HashMap<i64, Value> = fresh
                    .into_iter()
                    .map(|stream| (stream.id, stream.stream_stats))
                    .collect();
                write_stats(
                    client,
                    &verdicts,
                    &current,
                    settings.write_concurrency.max(1) as usize,
                )
                .await
            }
            Err(error) => {
                // Without a fresh base a merge could revert what the proxy wrote: write
                // nothing, and say so on every row that would have been written.
                let reason = format!(
                    "stats not written: re-reading the stream list before the write failed: \
                     {error:#}"
                );
                tracing::warn!("{reason}");
                verdicts
                    .iter()
                    .filter(|verdict| verdict.writes_stats())
                    .map(|verdict| (verdict.stream_id, StatsOutcome::Failed(reason.clone())))
                    .collect()
            }
        }
    };
    let count_stats =
        |wanted: fn(&StatsOutcome) -> bool| stats.values().filter(|o| wanted(o)).count();
    let stats_written = count_stats(|outcome| matches!(outcome, StatsOutcome::Written));
    let stats_unchanged = count_stats(|outcome| matches!(outcome, StatsOutcome::Unchanged));
    let stats_failed = count_stats(|outcome| matches!(outcome, StatsOutcome::Failed(_)));

    let by_stream: HashMap<i64, StreamVerdict> = verdicts
        .iter()
        .map(|verdict| (verdict.stream_id, verdict.clone()))
        .collect();
    let channels = channel_results(&selection, &by_stream);

    // Run rows: the per-stream record, then the per-channel verdicts triage reads. Written in
    // batches so a 50k-stream run does not hold the whole table as a `Vec<RunRow>` plus one
    // enormous transaction; ordinals stay unique because they are assigned across the whole set.
    let rows = run_rows(&verdicts, &channels, &world, &by_id, &stats, dry_run);
    for batch in rows.chunks(ROW_BATCH) {
        context.record_rows(batch)?;
    }

    let dead_by_error = error_distribution(&verdicts);
    let alive = verdicts.iter().filter(|verdict| verdict.alive()).count();
    let dead = verdicts.iter().filter(|verdict| verdict.dead()).count();
    let skipped = verdicts
        .iter()
        .filter(|verdict| verdict.class == Class::Skipped)
        .count();
    let retried: u32 = verdicts.iter().map(|verdict| verdict.retries).sum();
    let path = write_report(
        app,
        context,
        settings,
        &config,
        &selection,
        &verdicts,
        &channels,
        total,
        dry_run,
        (stats_written, stats_unchanged, stats_failed),
        &dead_by_error,
        remaining.len(),
        paused,
    )?;

    if let Ok(mut progress) = session.progress.lock() {
        progress.finish(format!("{alive} alive, {dead} dead, {skipped} skipped"));
    }

    let mut builder = OutcomeBuilder::new(if dry_run {
        format!(
            "Dry run complete. Probed {total} stream(s): {alive} alive, {dead} dead, {skipped} \
             skipped. No stream stats were written."
        )
    } else {
        format!(
            "Probed {total} stream(s): {alive} alive, {dead} dead, {skipped} skipped. \
             {stats_written} stream stats written."
        )
    })
    .count("selected", selection.channels.len() as i64)
    .count("queued", total as i64)
    .count("probed", verdicts.len() as i64)
    .count("alive", alive as i64)
    .count("dead", dead as i64)
    .count("skipped", skipped as i64)
    .count("retries", i64::from(retried))
    .count("skipped_fresh", selection.skipped_fresh as i64)
    .count("skipped_no_url", selection.skipped_no_url as i64)
    .count("unattached", selection.unattached as i64)
    .count("health_written", health_written as i64)
    .count("stats_written", stats_written as i64)
    .count("stats_unchanged", stats_unchanged as i64)
    .count("stats_failed", stats_failed as i64)
    .count("remaining", remaining.len() as i64);
    for verdict in ChannelVerdict::ALL {
        let count = channels
            .iter()
            .filter(|channel| channel.verdict == verdict)
            .count();
        builder = builder.count(format!("channels_{}", verdict.as_str()), count as i64);
    }
    for (error_type, count) in &dead_by_error {
        builder = builder.count(format!("dead_{}", slug(error_type)), *count as i64);
    }
    if dry_run {
        builder = builder.note("dry run: no stream stats were written");
    }
    for clause in clauses {
        builder = builder.note(clause);
    }
    if stats_failed > 0 {
        builder = builder.note(format!(
            "{stats_failed} stream stat write(s) failed; the run's health rows and CSV are \
             complete regardless"
        ));
    }
    if !remaining.is_empty() {
        builder = builder.note(format!(
            "{} stream(s) left for the next run in the checkpoint",
            remaining.len()
        ));
    }
    if paused {
        builder = builder.paused();
    }
    if cancelled {
        // The cancel rule: whatever landed stays recorded, and the run says so rather than
        // claiming a whole probe.
        builder = builder
            .cancelled()
            .note("cancelled by the operator; the remaining queue is checkpointed");
    }
    Ok(builder.report(Some(path)).build())
}

/// Write the CSV and return its path.
#[allow(clippy::too_many_arguments)]
fn write_report(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
    config: &ProbeConfig,
    selection: &Selection,
    verdicts: &[StreamVerdict],
    channels: &[ChannelResult],
    total: usize,
    dry_run: bool,
    stats: (usize, usize, usize),
    dead_by_error: &[(String, usize)],
    remaining: usize,
    paused: bool,
) -> Result<std::path::PathBuf> {
    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = if dry_run { "probe_dry_run" } else { "probe" };
    let title = format!(
        "{REPORT_TITLE} {} Report",
        if dry_run { "Dry Run" } else { "Applied" }
    );
    let alive = verdicts.iter().filter(|verdict| verdict.alive()).count();
    let dead = verdicts.iter().filter(|verdict| verdict.dead()).count();
    let skipped = verdicts
        .iter()
        .filter(|verdict| verdict.class == Class::Skipped)
        .count();
    let retried: u32 = verdicts.iter().map(|verdict| verdict.retries).sum();
    let mut spec = ReportSpec::new(app, &schema, &values, stem, &title)
        .note(
            "Selection",
            format!(
                "{} channel(s), {total} stream(s) due, {} fresh, {} without a URL, {} missing, \
                 {} unattached",
                selection.channels.len(),
                selection.skipped_fresh,
                selection.skipped_no_url,
                selection.skipped_missing,
                selection.unattached
            ),
        )
        .note(
            "Results",
            format!("{alive} alive, {dead} dead, {skipped} skipped, {retried} retr(y/ies)"),
        )
        .note(
            "Limits",
            format!(
                "{} connection(s) per M3U account (never above its max_streams), {} s spacing, \
                 {} throughput download(s)/minute, {} retry pass(es)",
                settings.probe_per_account_limit,
                settings.stream_check_delay,
                settings.probe_rate_per_minute,
                settings.dead_connection_retries
            ),
        )
        .note("Content checks", describe_content(&config.content))
        .note(
            "Throughput",
            match config.throughput {
                Some(duration) => format!(
                    "measured: {} s download per stream (throughput sorting is on)",
                    duration.as_secs()
                ),
                None => "not measured: enable_throughput_sorting is off".to_string(),
            },
        )
        .note(
            "Channel verdicts",
            ChannelVerdict::ALL
                .iter()
                .map(|verdict| {
                    format!(
                        "{} {}",
                        channels
                            .iter()
                            .filter(|channel| channel.verdict == *verdict)
                            .count(),
                        verdict.as_str()
                    )
                })
                .collect::<Vec<String>>()
                .join(", "),
        )
        .note(
            "Stream stats",
            format!(
                "{} written, {} unchanged, {} failed",
                stats.0, stats.1, stats.2
            ),
        );
    if !dead_by_error.is_empty() {
        spec = spec.note(
            "Dead by error type",
            dead_by_error
                .iter()
                .map(|(error_type, count)| format!("{error_type}: {count}"))
                .collect::<Vec<String>>()
                .join(", "),
        );
    }
    if remaining == 0 {
        spec = spec.note("Backlog", "complete: every stream due was probed");
    } else {
        spec = spec.note(
            "Backlog",
            format!(
                "{remaining} stream(s) left for the next run, in the {} checkpoint",
                if paused {
                    "window-closed"
                } else {
                    "cancelled run's"
                }
            ),
        );
    }
    if dry_run {
        spec = spec.note(
            "Dry run",
            "stream stats were not written; stream_health, run rows and this CSV were",
        );
    }

    // The channel each stream was selected from, for the CSV's channel columns: the first
    // in-scope channel carrying it, which is what the plugin's per-channel rows showed.
    let mut first_channel: HashMap<i64, &ChannelSelection> = HashMap::new();
    for channel in &selection.channels {
        for stream_id in &channel.streams {
            first_channel.entry(*stream_id).or_insert(channel);
        }
    }

    let exports_dir = context.exports_dir();
    let report = csv_report::Report::new(&exports_dir, spec.app, spec.stem, spec.title)
        .channels_processed(selection.channels.len())
        .result(
            "Streams probed",
            format!("{alive} alive, {dead} dead, {skipped} skipped"),
        )
        .result(
            "Stream stats",
            format!(
                "{} written, {} unchanged, {} failed",
                stats.0, stats.1, stats.2
            ),
        );
    let report = spec
        .notes
        .iter()
        .fold(report, |report, (label, value)| report.result(label, value));
    let path = report.write(
        context.config(),
        spec.schema,
        spec.values,
        &COLUMNS,
        |writer| {
            for verdict in verdicts {
                let channel = first_channel.get(&verdict.stream_id).copied();
                let extra = &verdict.extra;
                writer.write_record([
                    channel.map(|c| c.channel_id).unwrap_or(0).to_string(),
                    csv_report::safe_cell(
                        &channel.map(|c| c.channel_name.clone()).unwrap_or_default(),
                    ),
                    verdict.stream_id.to_string(),
                    csv_report::safe_cell(&verdict.stream_name),
                    verdict.class.label().to_string(),
                    verdict.format.to_string(),
                    format!("{:.1}", verdict.framerate),
                    verdict.error_type.clone(),
                    csv_report::safe_cell(&verdict.error),
                    verdict.retries.to_string(),
                    config.connection_timeout.to_string(),
                    config.probe_timeout.to_string(),
                    ffprobe::ANALYZE_SECONDS.to_string(),
                    optional(extra.container_duration_seconds),
                    optional(extra.container_bitrate_kbps),
                    optional(extra.calculated_bitrate_kbps),
                    optional(extra.packet_count),
                    optional(extra.frame_count),
                    optional(extra.mean_volume_db),
                    optional(verdict.throughput.as_ref().and_then(|measure| measure.mbps)),
                    verdict
                        .throughput
                        .as_ref()
                        .and_then(|measure| measure.edge_ip.clone())
                        .unwrap_or_default(),
                    verdict.health_class.to_string(),
                ])?;
            }
            Ok(())
        },
    )?;
    report.prune(context.config().exports.retention_days, &path);
    Ok(path)
}

fn optional<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

/// The dead streams' error types, most common first.
fn error_distribution(verdicts: &[StreamVerdict]) -> Vec<(String, usize)> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for verdict in verdicts.iter().filter(|verdict| verdict.dead()) {
        *counts.entry(verdict.error_type.as_str()).or_default() += 1;
    }
    let mut rows: Vec<(String, usize)> = counts
        .into_iter()
        .map(|(error_type, count)| (error_type.to_string(), count))
        .collect();
    rows.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    rows
}

/// A counts key from an error type: `404 Not Found` → `404_not_found`.
fn slug(text: &str) -> String {
    let mut slug = String::with_capacity(text.len());
    for character in text.chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character.to_ascii_lowercase());
        } else if !slug.ends_with('_') {
            slug.push('_');
        }
    }
    slug.trim_matches('_').to_string()
}

/// The limiter the plan specifies: per-account permits, the account's own cap and the
/// per-account spacing. No global rate: neither plugin capped its stream checks per minute
/// (IPTV Checker is paced by its workers, its per-stream delay and its 429 backoff), so the
/// ffprobe checks are paced per account only. The global `probe_rate_per_minute` lives on the
/// throughput downloads ([`build_throughput_limiter`]), which is the one place Stream-Mapparr
/// used it.
pub fn build_limiter(settings: &StreamsSettings, world: &World) -> Limiter {
    let mut limiter = Limiter::new(LimiterConfig {
        permits: settings.probe_per_account_limit.max(1) as usize,
        global_permits: 0,
        min_spacing: Duration::from_secs(u64::from(settings.stream_check_delay)),
        rate_per_minute: 0,
    });
    for account in &world.m3u_accounts {
        // `max_streams` is the provider's own connection limit: an override above it would be
        // refused by the provider anyway, and 0 means it publishes none.
        if let Some(max_streams) = account.max_streams.filter(|max| *max > 0) {
            limiter = limiter.with_hard_cap(account_key(account.id), max_streams as usize);
        }
    }
    limiter
}

/// The global gate on the throughput downloads: `probe_rate_per_minute` starts per rolling
/// minute, across every account.
///
/// A limiter with no concurrency limit of its own (the connection permit still comes from
/// [`build_limiter`)): it only counts starts. Keyed per account like the main limiter, but the
/// rate is global, so every key shares one budget.
pub fn build_throughput_limiter(settings: &StreamsSettings) -> Limiter {
    Limiter::new(LimiterConfig {
        permits: usize::MAX / 2,
        global_permits: 0,
        min_spacing: Duration::ZERO,
        rate_per_minute: settings.probe_rate_per_minute,
    })
}

/// One `stream_health` row per probed stream, merged over what the store already held.
fn write_health(
    store: &Store,
    previous: &HashMap<i64, StreamHealth>,
    verdicts: &[StreamVerdict],
    now: DateTime<Utc>,
) -> Result<usize> {
    let stamp = now.to_rfc3339_opts(SecondsFormat::Secs, true);
    let mut rows: Vec<StreamHealth> = Vec::with_capacity(verdicts.len());
    for verdict in verdicts {
        let before = previous
            .get(&verdict.stream_id)
            .cloned()
            .unwrap_or_default();
        let (throughput_mbps, throughput_at, edge_ip) = match &verdict.throughput {
            // A measurement was taken (even a failed one): it replaces the old one, because a
            // failed probe must not stay "fresh" for the whole TTL (`_is_probe_fresh`).
            Some(measure) => (measure.mbps, Some(stamp.clone()), measure.edge_ip.clone()),
            None => (
                before.throughput_mbps,
                before.throughput_at.clone(),
                before.edge_ip.clone(),
            ),
        };
        let consecutive_failures = match verdict.class {
            Class::Alive => 0,
            Class::Dead => before.consecutive_failures.saturating_add(1),
            Class::Skipped => before.consecutive_failures,
        };
        rows.push(StreamHealth {
            stream_id: verdict.stream_id,
            probed_at: Some(stamp.clone()),
            class: verdict.health_class.to_string(),
            error_type: (verdict.class != Class::Alive).then(|| verdict.error_type.clone()),
            throughput_mbps,
            throughput_at,
            edge_ip,
            consecutive_failures,
        });
    }
    store.upsert_stream_health_many(&rows)
}

/// What happened to one stream's `stream_stats`: the probe's one write outside channel ops,
/// so its outcome is recorded on the stream's run row (the `method` column) like any write.
#[derive(Debug, Clone, PartialEq, Eq)]
enum StatsOutcome {
    Written,
    Unchanged,
    Failed(String),
}

/// Write the stream stats that change, with bounded concurrency.
///
/// Streams whose stats would not change are not rewritten: the end state is the same and a
/// nightly run over 50k streams should not be 50k PATCHes. Returns each judged stream's
/// outcome (a skipped stream is absent: its stats are never touched); a failed write is
/// recorded and reported, never fatal — the probe's own record is already in the store.
async fn write_stats(
    client: &DispatcharrClient,
    verdicts: &[StreamVerdict],
    current: &HashMap<i64, Value>,
    concurrency: usize,
) -> HashMap<i64, StatsOutcome> {
    let mut outcomes: HashMap<i64, StatsOutcome> = HashMap::new();
    let mut writes: Vec<(i64, Value)> = Vec::new();
    for verdict in verdicts.iter().filter(|verdict| verdict.writes_stats()) {
        let existing = current
            .get(&verdict.stream_id)
            .cloned()
            .unwrap_or(json!({}));
        let merged = verdict.merged_stats(&existing);
        if merged == existing {
            outcomes.insert(verdict.stream_id, StatsOutcome::Unchanged);
            continue;
        }
        writes.push((verdict.stream_id, merged));
    }

    for chunk in writes.chunks(concurrency.max(1)) {
        let mut set = tokio::task::JoinSet::new();
        for (stream_id, stats) in chunk {
            let client = client.clone();
            let stream_id = *stream_id;
            let stats = stats.clone();
            set.spawn(async move {
                (
                    stream_id,
                    client.update_stream_stats(stream_id, &stats).await,
                )
            });
        }
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((stream_id, Ok(_))) => {
                    outcomes.insert(stream_id, StatsOutcome::Written);
                }
                Ok((stream_id, Err(error))) => {
                    tracing::warn!("could not write stream stats: {error:#}");
                    outcomes.insert(stream_id, StatsOutcome::Failed(format!("{error:#}")));
                }
                Err(error) => {
                    // A panicked task loses its id; the stream reads as never answered below.
                    tracing::warn!("stream stat write task failed: {error}");
                }
            }
        }
    }
    for (stream_id, _) in &writes {
        outcomes
            .entry(*stream_id)
            .or_insert_with(|| StatsOutcome::Failed("the write task failed".to_string()));
    }
    outcomes
}

/// Every run row this run writes, numbered.
///
/// The ordinals matter: the store keys rows on `(run_id, ordinal)` with `INSERT OR REPLACE`, so
/// a column of zero ordinals would leave exactly one row behind.
fn run_rows(
    verdicts: &[StreamVerdict],
    channels: &[ChannelResult],
    world: &World,
    by_id: &HashMap<i64, &Stream>,
    stats: &HashMap<i64, StatsOutcome>,
    dry_run: bool,
) -> Vec<RunRow> {
    let mut rows = stream_rows(verdicts, world, by_id, stats, dry_run);
    rows.extend(channel_rows(channels));
    for (ordinal, row) in rows.iter_mut().enumerate() {
        row.ordinal = ordinal as i64;
    }
    rows
}

/// One run row per probed stream, with the transition the store recorded.
///
/// `method` says what became of the stream's `stream_stats`, the probe's one Dispatcharr
/// write: `stats_written`, `stats_unchanged`, `stats_failed` (the error appended to the
/// reason), `stats_dry_run`, or empty for a skipped stream, whose stats are never touched.
fn stream_rows(
    verdicts: &[StreamVerdict],
    world: &World,
    by_id: &HashMap<i64, &Stream>,
    stats: &HashMap<i64, StatsOutcome>,
    dry_run: bool,
) -> Vec<RunRow> {
    verdicts
        .iter()
        .map(|verdict| {
            let (method, failure) = match stats.get(&verdict.stream_id) {
                Some(StatsOutcome::Written) => ("stats_written", None),
                Some(StatsOutcome::Unchanged) => ("stats_unchanged", None),
                Some(StatsOutcome::Failed(error)) => ("stats_failed", Some(error.as_str())),
                None if dry_run && verdict.writes_stats() => ("stats_dry_run", None),
                None => ("", None),
            };
            let reason = if verdict.error.is_empty() {
                verdict.error_type.clone()
            } else {
                verdict.error.clone()
            };
            let reason = match failure {
                Some(error) if reason.is_empty() => format!("stats write failed: {error}"),
                Some(error) => format!("{reason}; stats write failed: {error}"),
                None => reason,
            };
            RunRow {
                subject: Subject::Stream,
                channel_id: verdict.stream_id,
                channel_name: verdict.stream_name.clone(),
                channel_group: by_id
                    .get(&verdict.stream_id)
                    .map(|stream| world.group_name(stream.channel_group))
                    .unwrap_or_default(),
                status: verdict.class.label().to_string(),
                reason,
                method: method.to_string(),
                field: STREAM_FIELD.to_string(),
                before_value: verdict.previous_class.clone(),
                after_value: Some(verdict.health_class.to_string()),
                ..RunRow::default()
            }
        })
        .collect()
}

/// The per-channel verdict rows triage reads.
fn channel_rows(channels: &[ChannelResult]) -> Vec<RunRow> {
    let mut rows = Vec::with_capacity(channels.len() * 2);
    for channel in channels {
        rows.push(RunRow {
            subject: Subject::Channel,
            channel_id: channel.channel_id,
            channel_name: channel.channel_name.clone(),
            channel_group: channel.channel_group.clone(),
            status: channel.verdict.as_str().to_string(),
            reason: channel.reason.clone(),
            field: VERDICT_FIELD.to_string(),
            before_value: None,
            after_value: Some(channel.verdict.as_str().to_string()),
            ..RunRow::default()
        });
        if let Some(format) = channel.format {
            rows.push(RunRow {
                subject: Subject::Channel,
                channel_id: channel.channel_id,
                channel_name: channel.channel_name.clone(),
                channel_group: channel.channel_group.clone(),
                status: "format".to_string(),
                reason: format!("last alive stream reports {format}"),
                field: FORMAT_FIELD.to_string(),
                before_value: None,
                after_value: Some(format.to_string()),
                ..RunRow::default()
            });
        }
    }
    rows
}

/// The content checks in a sentence, for the report's preamble.
fn describe_content(options: &ContentOptions) -> String {
    if !options.any() {
        return "none armed (no ffmpeg pass)".to_string();
    }
    let mut parts: Vec<String> = Vec::new();
    if options.want_black {
        parts.push(format!(
            "black screen: {} s of {} s must be black",
            options.min_black_seconds, options.sample_seconds
        ));
    }
    if options.want_freeze {
        parts.push(format!(
            "frozen: {} s frozen inside {} s",
            content::effective_freeze_seconds(options.frozen_min_seconds, options.sample_seconds),
            options.sample_seconds
        ));
    }
    if options.want_audio {
        parts.push(format!(
            "silent: mean volume at or below {} dB",
            options.audio_threshold_db
        ));
    }
    parts.join("; ")
}

/// The ffprobe invocation a probe would run, for diagnostics and the parity harness.
pub fn ffprobe_command(config: &ProbeConfig, url: &str) -> ToolSpec {
    ffprobe::command(
        &config.ffprobe_path,
        url,
        config.connection_timeout,
        config.probe_timeout,
    )
}
