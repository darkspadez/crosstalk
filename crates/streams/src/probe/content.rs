//! The one ffmpeg decode pass: black, frozen and silent in a single connection.
//!
//! Port of IPTV Checker's `_analyze_stream_content` (4846) and the four parsers it uses
//! (`_parse_blackdetect_output` 4706, `_parse_freezedetect_output` 4780,
//! `_parse_mean_volume_db` 4800, `_is_silent_audio` 4820) plus
//! `_effective_freeze_seconds` (4831).
//!
//! # Why one pass
//!
//! `blackdetect`, `freezedetect` and `volumedetect` all report through ffmpeg's `info` log, and
//! they compose into one filtergraph, so enabling three detectors costs the provider ONE
//! connection rather than three (the plugin's own comment, 4847-4852). `-loglevel error` would
//! suppress every one of them, which is why this pass logs at `info` even though the ffprobe
//! pass does not.
//!
//! # Precedence, and why it is ordered
//!
//! A black screen is ALSO a still picture, so `freezedetect` fires on it too. Black must win, or
//! enabling freeze detection would relabel every blank screen as "frozen" and break the
//! blank-screen rename/move actions, which match on the error type `Black Screen` (5233-5241).
//! The order is therefore: black, then frozen, then silent.
//!
//! # Fail-open
//!
//! Every verdict is `None` when it could not be measured (ffmpeg missing, timed out, errored,
//! killed, or the log line absent), and `None` never marks a stream dead. A stream whose audio
//! track is missing cannot be silent either: `want_audio` is only set when ffprobe found an
//! audio stream (5215-5217), so a detector never invents a fault it cannot see.
//!
//! # The freeze clamp
//!
//! `freezedetect` reports only once a freeze has lasted `d` seconds, so a `d` at or beyond the
//! sample length can NEVER mature: the setting would look active while being structurally
//! unable to fire. The plugin clamps it to `max(1, min(wanted, sample - 1))` instead of obeying
//! it literally, and so does this.

use crate::probe::tool::ToolSpec;
use std::time::Duration;

/// `-user_agent`, the same one the ffprobe pass sends.
pub use crate::probe::ffprobe::USER_AGENT;

/// `blackdetect=d=…:pic_th=0.98`: the plugin's threshold for "this frame is black".
pub const BLACK_PICTURE_THRESHOLD: f64 = 0.98;

/// `freezedetect=n=-60dB`: the noise floor below which two frames count as identical.
pub const FREEZE_NOISE_DB: i32 = -60;

/// What a content pass was asked to measure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentOptions {
    /// `black_screen_sample_seconds`: how much of the stream to decode.
    pub sample_seconds: u32,
    /// `black_screen_min_black_seconds`.
    pub min_black_seconds: u32,
    /// `frozen_video_min_seconds`, clamped by [`effective_freeze_seconds`].
    pub frozen_min_seconds: u32,
    /// `black_screen_detection`: whether a black verdict may mark a stream dead.
    pub want_black: bool,
    /// `frozen_video_detection`.
    pub want_freeze: bool,
    /// `silent_audio_detection` AND the stream has an audio track.
    pub want_audio: bool,
    /// `silent_audio_max_db`.
    pub audio_threshold_db: i32,
    /// `black_screen_ffmpeg_timeout`: the wall-clock cap, connection included.
    pub timeout: Duration,
}

impl ContentOptions {
    /// No detector is armed: the pass is not run at all.
    pub fn any(&self) -> bool {
        self.want_black || self.want_freeze || self.want_audio
    }

    /// The `-vf` chain: blackdetect first so its verdict is always available for precedence.
    pub fn video_filters(&self) -> String {
        let mut filters = vec![format!(
            "blackdetect=d={}:pic_th={BLACK_PICTURE_THRESHOLD}",
            self.min_black_seconds
        )];
        if self.want_freeze {
            filters.push(format!(
                "freezedetect=n={FREEZE_NOISE_DB}dB:d={}",
                effective_freeze_seconds(self.frozen_min_seconds, self.sample_seconds)
            ));
        }
        filters.join(",")
    }
}

/// `_effective_freeze_seconds`: clamp a freeze window that could never mature inside the sample.
pub fn effective_freeze_seconds(min_seconds: u32, sample_seconds: u32) -> u32 {
    let sample = sample_seconds.max(1);
    min_seconds.clamp(1, sample.saturating_sub(1).max(1))
}

/// The ffmpeg invocation, argument for argument as `_analyze_stream_content` builds it.
///
/// `connection_timeout_seconds` is `streams.toml`'s `timeout` (`-rw_timeout`, in µs), not the
/// probe timeout: this is the connection's own cap, while `ContentOptions::timeout` bounds the
/// whole process.
pub fn command(
    executable: &str,
    url: &str,
    connection_timeout_seconds: u32,
    options: &ContentOptions,
) -> ToolSpec {
    let mut args: Vec<String> = Vec::with_capacity(20);
    for arg in [
        "-hide_banner",
        "-nostats",
        "-loglevel",
        "info",
        "-user_agent",
    ] {
        args.push(arg.to_string());
    }
    args.push(USER_AGENT.to_string());
    // Input options MUST precede `-i` or ffmpeg silently ignores them.
    args.push("-rw_timeout".to_string());
    args.push((u64::from(connection_timeout_seconds) * 1_000_000).to_string());
    args.push("-i".to_string());
    args.push(url.to_string());
    args.push("-t".to_string());
    args.push(options.sample_seconds.to_string());
    if options.want_audio {
        args.push("-af".to_string());
        args.push("volumedetect".to_string());
    } else {
        args.push("-an".to_string());
    }
    args.push("-vf".to_string());
    args.push(options.video_filters());
    args.push("-f".to_string());
    args.push("null".to_string());
    args.push("-".to_string());
    ToolSpec::new(executable, args, options.timeout)
}

/// What one decode pass answered. `None` means "not measured or not decidable".
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ContentAnswers {
    pub black: Option<bool>,
    pub frozen: Option<bool>,
    pub audio_db: Option<f64>,
}

/// The content verdict that marks a stream dead, in precedence order.
#[derive(Debug, Clone, PartialEq)]
pub enum ContentVerdict {
    /// `error_type = 'Black Screen'`.
    Black,
    /// `error_type = 'Frozen Video'`.
    Frozen,
    /// `error_type = 'Silent Audio'`.
    Silent { mean_db: f64, threshold_db: i32 },
}

impl ContentVerdict {
    /// The plugin's `error_type` for this verdict.
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::Black => "Black Screen",
            Self::Frozen => "Frozen Video",
            Self::Silent { .. } => "Silent Audio",
        }
    }

    /// The plugin's error message for this verdict.
    pub fn message(&self) -> String {
        match self {
            Self::Black => "Stream decodes to a black screen".to_string(),
            Self::Frozen => "Stream decodes to a frozen (unchanging) picture".to_string(),
            // `{mean_db}` in Rust prints "-91" where Python's f-string prints "-91.0"; `{:?}`
            // is the shortest round-tripping form, which is what Python's float repr is.
            Self::Silent {
                mean_db,
                threshold_db,
            } => format!(
                "Audio track is silent (mean volume {mean_db:?} dB, threshold {threshold_db} dB)"
            ),
        }
    }
}

/// Parse the decode pass's stderr into the three answers, exactly as the plugin does.
///
/// `clean_exit` is `returncode == 0`; a non-zero exit can still carry evidence (a black frame
/// logged before a read error), and the plugin trusts POSITIVE evidence from any exit while
/// only taking a negative verdict from a clean one.
pub fn parse(stderr: &str, clean_exit: bool, options: &ContentOptions) -> ContentAnswers {
    let mut answers = ContentAnswers::default();
    if !black_segments(stderr).is_empty() {
        answers.black = Some(true);
    } else if clean_exit {
        answers.black = Some(false);
    }
    if options.want_freeze {
        if !freeze_starts(stderr).is_empty() {
            answers.frozen = Some(true);
        } else if clean_exit {
            answers.frozen = Some(false);
        }
    }
    if options.want_audio {
        answers.audio_db = mean_volume_db(stderr);
    }
    answers
}

/// The verdict the answers authorise, honouring the detectors that were armed.
pub fn verdict(answers: &ContentAnswers, options: &ContentOptions) -> Option<ContentVerdict> {
    if options.want_black && answers.black == Some(true) {
        return Some(ContentVerdict::Black);
    }
    if options.want_freeze && answers.frozen == Some(true) {
        return Some(ContentVerdict::Frozen);
    }
    if options.want_audio && is_silent_audio(answers.audio_db, options.audio_threshold_db) {
        return Some(ContentVerdict::Silent {
            mean_db: answers.audio_db.unwrap_or_default(),
            threshold_db: options.audio_threshold_db,
        });
    }
    None
}

/// `black_start:<a> black_end:<b> black_duration:<c>` lines, in log order.
pub fn black_segments(stderr: &str) -> Vec<(f64, f64, f64)> {
    let pattern = regex::Regex::new(
        r"black_start:(?P<start>[\d.]+)\s+black_end:(?P<end>[\d.]+)\s+black_duration:(?P<dur>[\d.]+)",
    )
    .expect("static regex");
    pattern
        .captures_iter(stderr)
        .filter_map(|captures| {
            Some((
                captures.name("start")?.as_str().parse().ok()?,
                captures.name("end")?.as_str().parse().ok()?,
                captures.name("dur")?.as_str().parse().ok()?,
            ))
        })
        .collect()
}

/// `lavfi.freezedetect.freeze_start: <n>` values.
///
/// Keyed on freeze_start and NOT freeze_end: a freeze running to the end of the capture prints
/// a start with no end, which is the common case for a still picture.
pub fn freeze_starts(stderr: &str) -> Vec<f64> {
    let pattern = regex::Regex::new(r"lavfi\.freezedetect\.freeze_start:\s*(?P<start>[\d.]+)")
        .expect("static regex");
    pattern
        .captures_iter(stderr)
        .filter_map(|captures| captures.name("start")?.as_str().parse().ok())
        .collect()
}

/// `mean_volume: <db> dB`, including `-inf` for digital silence.
pub fn mean_volume_db(stderr: &str) -> Option<f64> {
    let pattern =
        regex::Regex::new(r"mean_volume:\s*(-?(?:inf|[\d.]+))\s*dB").expect("static regex");
    let captures = pattern.captures(stderr)?;
    match captures.get(1)?.as_str() {
        "-inf" => Some(f64::NEG_INFINITY),
        "inf" => Some(f64::INFINITY),
        text => text.parse().ok(),
    }
}

/// `_is_silent_audio`: true only on POSITIVE evidence. An unmeasured level is not silence.
pub fn is_silent_audio(mean_db: Option<f64>, threshold_db: i32) -> bool {
    match mean_db {
        Some(level) => level <= f64::from(threshold_db),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(want_black: bool, want_freeze: bool, want_audio: bool) -> ContentOptions {
        ContentOptions {
            sample_seconds: 6,
            min_black_seconds: 3,
            frozen_min_seconds: 4,
            want_black,
            want_freeze,
            want_audio,
            audio_threshold_db: -70,
            timeout: Duration::from_secs(20),
        }
    }

    #[test]
    fn the_command_puts_input_options_before_i_and_logs_at_info() {
        let spec = command(
            "/usr/local/bin/ffmpeg",
            "http://host/live.ts",
            10,
            &options(true, true, true),
        );
        assert_eq!(
            spec.args,
            vec![
                "-hide_banner",
                "-nostats",
                "-loglevel",
                "info",
                "-user_agent",
                "VLC/3.0.21 LibVLC/3.0.21",
                "-rw_timeout",
                "10000000",
                "-i",
                "http://host/live.ts",
                "-t",
                "6",
                "-af",
                "volumedetect",
                "-vf",
                "blackdetect=d=3:pic_th=0.98,freezedetect=n=-60dB:d=4",
                "-f",
                "null",
                "-",
            ]
        );
        assert_eq!(spec.timeout, Duration::from_secs(20));

        // With no audio detector the pass decodes no audio at all.
        let spec = command("/ffmpeg", "http://h/a.ts", 5, &options(true, false, false));
        assert!(spec.args.contains(&"-an".to_string()));
        assert!(!spec.args.contains(&"-af".to_string()));
        assert!(spec
            .args
            .contains(&"blackdetect=d=3:pic_th=0.98".to_string()));
    }

    #[test]
    fn a_freeze_window_that_could_never_mature_is_clamped_inside_the_sample() {
        assert_eq!(effective_freeze_seconds(4, 6), 4);
        // d >= sample would never report: clamped to sample - 1.
        assert_eq!(effective_freeze_seconds(9, 6), 5);
        assert_eq!(effective_freeze_seconds(0, 6), 1);
        // A one-second sample cannot hold a freeze at all; 1 is the floor.
        assert_eq!(effective_freeze_seconds(4, 1), 1);
    }

    #[test]
    fn a_black_screen_wins_over_a_frozen_one() {
        // Real ffmpeg 8.1.2 output for a black lavfi source: both detectors fire.
        let stderr = "[blackdetect @ 0x1] black_start:0 black_end:6 black_duration:6\n\
                      [Parsed_freezedetect_1 @ 0x2] lavfi.freezedetect.freeze_start: 0.0666670\n";
        let armed = options(true, true, false);
        let answers = parse(stderr, true, &armed);
        assert_eq!(answers.black, Some(true));
        assert_eq!(answers.frozen, Some(true));
        assert_eq!(
            verdict(&answers, &armed),
            Some(ContentVerdict::Black),
            "blank must win, or every blank screen would be relabelled frozen"
        );
        // With black detection off, the freeze verdict stands on its own.
        let freeze_only = options(false, true, false);
        assert_eq!(
            verdict(&answers, &freeze_only),
            Some(ContentVerdict::Frozen)
        );
    }

    #[test]
    fn a_clean_pass_without_evidence_is_not_black_and_not_frozen() {
        let options = options(true, true, false);
        let answers = parse("frame= 120 fps=30\n", true, &options);
        assert_eq!(answers.black, Some(false));
        assert_eq!(answers.frozen, Some(false));
        assert_eq!(verdict(&answers, &options), None);
    }

    #[test]
    fn a_failed_pass_yields_no_negative_verdict_but_trusts_positive_evidence() {
        let options = options(true, true, false);
        // Non-zero exit, no log line: nothing measured, nothing claimed.
        let answers = parse("Connection refused\n", false, &options);
        assert_eq!(answers.black, None);
        assert_eq!(answers.frozen, None);
        assert_eq!(verdict(&answers, &options), None);

        // Non-zero exit WITH a black segment: the evidence still counts.
        let answers = parse(
            "[blackdetect @ 0x1] black_start:1.5 black_end:4 black_duration:2.5\n",
            false,
            &options,
        );
        assert_eq!(verdict(&answers, &options), Some(ContentVerdict::Black));
    }

    #[test]
    fn silence_is_only_silence_on_positive_evidence() {
        let options = options(false, false, true);
        let quiet = parse("mean_volume: -91.0 dB\n", true, &options);
        assert_eq!(quiet.audio_db, Some(-91.0));
        assert!(matches!(
            verdict(&quiet, &options),
            Some(ContentVerdict::Silent { mean_db, .. }) if mean_db == -91.0
        ));

        // `-inf` is digital silence: readable rather than "not measured".
        let digital = parse("mean_volume: -inf dB\n", true, &options);
        assert_eq!(digital.audio_db, Some(f64::NEG_INFINITY));
        assert!(verdict(&digital, &options).is_some());

        // A loud stream is not silent, and neither is an unmeasured one.
        let loud = parse("mean_volume: -12.3 dB\n", true, &options);
        assert_eq!(verdict(&loud, &options), None);
        let unmeasured = parse("frame= 1 fps=1\n", true, &options);
        assert_eq!(unmeasured.audio_db, None);
        assert_eq!(verdict(&unmeasured, &options), None);
        // The threshold itself counts as silent (`<=`).
        assert!(is_silent_audio(Some(-70.0), -70));
        assert!(!is_silent_audio(Some(-69.9), -70));
    }

    #[test]
    fn the_audio_threshold_is_a_setting_and_silence_is_not_claimed_when_it_is_off() {
        let mut options = options(false, false, true);
        options.audio_threshold_db = -50;
        let answers = parse("mean_volume: -60.0 dB\n", true, &options);
        assert!(verdict(&answers, &options).is_some(), "-60 <= -50");
        options.want_audio = false;
        assert_eq!(verdict(&answers, &options), None, "detector off");
    }

    #[test]
    fn every_verdict_carries_the_plugins_wording() {
        assert_eq!(ContentVerdict::Black.error_type(), "Black Screen");
        assert_eq!(ContentVerdict::Frozen.error_type(), "Frozen Video");
        assert_eq!(
            ContentVerdict::Silent {
                mean_db: -91.0,
                threshold_db: -70
            }
            .error_type(),
            "Silent Audio"
        );
        assert!(ContentVerdict::Black.message().contains("black screen"));
        assert!(ContentVerdict::Frozen.message().contains("frozen"));
        // Python's float repr, which the plugin's f-string prints: "-91.0", not "-91".
        assert!(ContentVerdict::Silent {
            mean_db: -91.0,
            threshold_db: -70
        }
        .message()
        .contains("-91.0 dB"));
    }
}
