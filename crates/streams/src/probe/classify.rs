//! Classification: which class a stream is in, and why.
//!
//! Port of IPTV Checker's error classification (`check_stream` 5296-5383), its skip rules
//! (`_is_streamlink_only_url` 4694, the 429 path 5357, the audio-only path 5371) and its URL
//! masking (`_mask_url_in_error` 4665).
//!
//! # The three classes
//!
//!   * **Alive** — ffprobe exited 0 and found a video stream (and no armed content detector
//!     found a fault).
//!   * **Dead** — anything else: an error, a timeout, no video stream after retries, or a
//!     content verdict. `error_type` says which.
//!   * **Skipped** — NOT JUDGED: a Streamlink-only host, an HTTP 429, or an audio-only stream.
//!     Skipped exists so that destructive actions downstream (rename, move, delete) leave the
//!     stream alone. A 429 is the provider asking us to stop, not evidence the stream is broken;
//!     an audio-only stream is working and simply carries no video. Treating either as dead is
//!     how IPTV Checker used to graveyard working radio stations.
//!
//! # `stream_health.class` is finer than the class
//!
//! The store's column distinguishes the content deaths (`blank`, `frozen`, `silent`,
//! `placeholder`) because `triage` names its per-class rename format and group from settings,
//! while `error_type` keeps the plugin's own wording so an operator's muscle memory and the
//! existing reports still read the same. [`health_class`] is the one mapping between the two.
//!
//! # The order of the classifier
//!
//! First match wins, in the plugin's order, and the order is load-bearing: `ffprobe` prints
//! `Error opening input: Server returned 404 Not Found` for a 404 and
//! `... 429 Too Many Requests` for a 429, so the numeric tests have to be told apart, and a
//! timeout message that also contains "404" is still a timeout. `Stream Unreachable` is the
//! leftover for `returncode == 1`, which is ffprobe's generic failure.

/// One stream's class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    Alive,
    Dead,
    Skipped,
}

impl Class {
    /// The store's coarse class (`stream_health.class` keeps the finer `health_class`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Alive => "alive",
            Self::Dead => "dead",
            Self::Skipped => "skipped",
        }
    }

    /// The plugin's status wording, which the CSV and the run rows keep.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Alive => "Alive",
            Self::Dead => "Dead",
            Self::Skipped => "Skipped",
        }
    }

    /// Parse the stored form. An unknown value is a stream with no verdict.
    pub fn parse(text: &str) -> Option<Self> {
        match text.to_ascii_lowercase().as_str() {
            "alive" => Some(Self::Alive),
            "dead" | "blank" | "frozen" | "silent" | "placeholder" => Some(Self::Dead),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }
}

/// `N/A`: the error type IPTV Checker stores for a stream that passed.
pub const NO_ERROR: &str = "N/A";
/// The error type of a Streamlink-only host.
pub const SKIPPED: &str = "Skipped";
/// HTTP 429: the provider asked us to back off.
pub const RATE_LIMITED: &str = "Rate Limited";
/// An audio-only (radio) stream: working, but no video to check.
pub const NO_VIDEO_STREAM: &str = "No Video Stream";
/// The run was cancelled while the tool was in flight, so the invocation was killed.
///
/// IPTV Checker fabricated the same row (`{'status': 'Dead', 'error_type': 'Cancelled'}`) and
/// then excluded it from its counts: a cancelled probe is not evidence about the stream. Here it
/// is Skipped as well as dropped, so no health row claims a verdict either.
pub const CANCELLED: &str = "Cancelled";
/// A finite container duration: the provider is serving a file.
pub const PLACEHOLDER_FILE: &str = "Placeholder File";
/// Decodes to black.
pub const BLACK_SCREEN: &str = "Black Screen";
/// Decodes to a still picture.
pub const FROZEN_VIDEO: &str = "Frozen Video";
/// Decodes to silence and the detector was armed.
pub const SILENT_AUDIO: &str = "Silent Audio";

/// IC's `LOW_FRAMERATE_THRESHOLD` (plugin.py:180): below this a stream is "slow" (PAL 25 and
/// film 24 are deliberately safe).
pub const LOW_FRAMERATE_THRESHOLD: f64 = 24.0;

/// The error types a retry can fix (`_process_streams_parallel` 3243).
///
/// Deliberately narrow: a 404 or a 403 will not become a 200, and retrying a 429 is exactly what
/// the provider asked us not to do (the rate-limit guard handles that case).
pub const RETRYABLE: [&str; 5] = [
    "Timeout",
    "Connection Refused",
    "Network Unreachable",
    "Stream Unreachable",
    "Server Error",
];

/// Whether another attempt could change the verdict.
pub fn is_retryable(error_type: &str) -> bool {
    RETRYABLE.contains(&error_type)
}

/// `check_stream` 5296-5338: the error type for a failed ffprobe, from its stderr and exit code.
///
/// `error_output` is the trimmed stderr the plugin classified; the caller passes the plugin's
/// fallback (`Stream not accessible`) when stderr was empty.
pub fn classify_error(error_output: &str, returncode: Option<i32>) -> &'static str {
    let lower = error_output.to_lowercase();
    let has = |needle: &str| error_output.contains(needle);
    if lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("connection timeout")
    {
        "Timeout"
    } else if lower.contains("option not found") || lower.contains("unrecognized option") {
        "FFprobe Option Error"
    } else if has("404") || (lower.contains("not found") && lower.contains("http")) {
        "404 Not Found"
    } else if has("403") || lower.contains("forbidden") {
        "403 Forbidden"
    } else if lower.contains("too many requests")
        || lower.contains("rate limit")
        || rate_limit_token().is_match(error_output)
    {
        "Rate Limited"
    } else if has("500") || lower.contains("internal server error") {
        "Server Error"
    } else if lower.contains("connection refused") {
        "Connection Refused"
    } else if lower.contains("network unreachable") || lower.contains("no route to host") {
        "Network Unreachable"
    } else if lower.contains("invalid data found") || lower.contains("invalid argument") {
        "Invalid Stream"
    } else if lower.contains("protocol not supported") {
        "Unsupported Protocol"
    } else if returncode == Some(1) {
        "Stream Unreachable"
    } else {
        "Other"
    }
}

/// `re.search(r'\b429\b', …)`: a bare 429 token, not the digits inside a bigger number.
fn rate_limit_token() -> &'static regex::Regex {
    static PATTERN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    PATTERN.get_or_init(|| regex::Regex::new(r"\b429\b").expect("static regex"))
}

/// The error type's `stream_health.class`, which is finer than [`Class`].
///
/// `triage` acts per class (`dead_rename_format` for a dead channel, `black_screen_rename_format`
/// for a blank one), so the content deaths have their own class rather than sharing `dead`.
pub fn health_class(class: Class, error_type: &str) -> &'static str {
    match (class, error_type) {
        (Class::Alive, _) => "alive",
        (Class::Skipped, _) => "skipped",
        (Class::Dead, BLACK_SCREEN) => "blank",
        (Class::Dead, FROZEN_VIDEO) => "frozen",
        (Class::Dead, SILENT_AUDIO) => "silent",
        (Class::Dead, PLACEHOLDER_FILE) => "placeholder",
        (Class::Dead, _) => "dead",
    }
}

/// `_is_low_framerate` (3618): a known, sub-threshold framerate.
///
/// Zero is NOT low (a stream with no measured framerate says nothing), which is the plugin's
/// `0 < fps < 24`.
pub fn is_low_framerate(fps: f64) -> bool {
    fps > 0.0 && fps < LOW_FRAMERATE_THRESHOLD
}

/// `_is_streamlink_only_url` (4694): a host suffix the configured list names.
///
/// Matches the host exactly or as a parent domain, so `youtube.com` covers
/// `www.youtube.com` and not `notyoutube.com`.
pub fn is_streamlink_only(url: &str, hosts: &[String]) -> bool {
    let host = match url::host_of(url) {
        Some(host) => host,
        None => return false,
    };
    hosts.iter().any(|suffix| {
        let suffix = suffix.trim().trim_start_matches('.').to_lowercase();
        !suffix.is_empty() && (host == suffix || host.ends_with(&format!(".{suffix}")))
    })
}

/// `_mask_url_in_error` (4665): replace the provider URL with the stream id.
///
/// A probe error is written to a CSV, a run row and a log line, all of which leave the host in a
/// report an operator may paste into an issue. The URL carries the provider credential, so it
/// never leaves the probe: the plugin's own rule, kept.
pub fn mask_url_in_error(message: &str, url: &str, stream_id: i64) -> String {
    if message.is_empty() || url.is_empty() {
        return message.to_string();
    }
    let masked = message.replace(url, &format!("[Stream ID: {stream_id}]"));
    // The URL-encoded spelling appears when the tool quotes the whole request line.
    match url_encode(url) {
        Some(encoded) if masked.contains(&encoded) => {
            masked.replace(&encoded, &format!("[Stream ID: {stream_id}]"))
        }
        _ => masked,
    }
}

/// `urllib.parse.quote(url, safe='')` for the characters a URL actually carries: everything but
/// the RFC 3986 unreserved set is percent-encoded, which is what the plugin's masking compares
/// against.
fn url_encode(url: &str) -> Option<String> {
    let mut encoded = String::with_capacity(url.len());
    let mut changed = false;
    for byte in url.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char)
            }
            other => {
                changed = true;
                encoded.push_str(&format!("%{other:02X}"));
            }
        }
    }
    changed.then_some(encoded)
}

/// The minimum host parsing a probe needs: the authority's host, lowercased.
mod url {
    /// The host of `scheme://[userinfo@]host[:port]/…`, or `None` for a URL without one.
    ///
    /// `urllib.parse.urlparse(url).hostname` is the plugin's source; a hand-rolled parser keeps
    /// this crate off a second URL dependency, and it only has to be right about the host.
    pub fn host_of(url: &str) -> Option<String> {
        let (_, rest) = url.split_once("://")?;
        let authority = rest.split(['/', '?', '#']).next()?;
        let host_port = authority.rsplit('@').next()?;
        // An IPv6 literal keeps its brackets off and its colons in.
        let host = if let Some(rest) = host_port.strip_prefix('[') {
            rest.split(']').next()?
        } else {
            host_port.split(':').next()?
        };
        Some(host.trim_end_matches('.').to_lowercase()).filter(|host| !host.is_empty())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn hosts_are_read_the_way_urlparse_reads_them() {
            assert_eq!(
                host_of("http://example.com/a.ts"),
                Some("example.com".into())
            );
            assert_eq!(
                host_of("https://user:pw@Host.Example.COM:8080/x?y"),
                Some("host.example.com".into())
            );
            assert_eq!(host_of("rtmp://host"), Some("host".into()));
            assert_eq!(host_of("http://[::1]:8080/x"), Some("::1".into()));
            assert_eq!(host_of("not a url"), None);
            assert_eq!(host_of("http:///x"), None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_classifier_matches_the_plugins_order() {
        // Real ffprobe wordings for each case.
        for (stderr, returncode, expected) in [
            ("http://h/x: Connection timed out\n", Some(1), "Timeout"),
            (
                "Unrecognized option 'foo'.\nError splitting the argument list: Option not found\n",
                Some(1),
                "FFprobe Option Error",
            ),
            (
                "http://h/x: Server returned 404 Not Found\n",
                Some(1),
                "404 Not Found",
            ),
            (
                "http://h/x: Server returned 403 Forbidden\n",
                Some(1),
                "403 Forbidden",
            ),
            (
                "http://h/x: Server returned 429 Too Many Requests\n",
                Some(1),
                "Rate Limited",
            ),
            (
                "http://h/x: Server returned 500 Internal Server Error\n",
                Some(1),
                "Server Error",
            ),
            ("Connection refused\n", Some(255), "Connection Refused"),
            ("Network unreachable\n", Some(1), "Network Unreachable"),
            (
                // glibc's own wording. The plugin tests for `'network unreachable' in …`, so
                // "is" between the words means the class falls through to the exit-1 leftover;
                // kept literal, because the point of the port is the plugin's output.
                "Network is unreachable\n",
                Some(1),
                "Stream Unreachable",
            ),
            (
                "http://h/x: Invalid data found when processing input\n",
                Some(1),
                "Invalid Stream",
            ),
            ("Protocol not supported\n", Some(1), "Unsupported Protocol"),
            // The generic exit-1 leftover.
            ("something went wrong\n", Some(1), "Stream Unreachable"),
            ("something went wrong\n", None, "Other"),
            ("", None, "Other"),
        ] {
            assert_eq!(
                classify_error(stderr, returncode),
                expected,
                "stderr = {stderr:?}, returncode = {returncode:?}"
            );
        }
    }

    #[test]
    fn a_timeout_wins_over_a_number_the_message_also_carries() {
        // A provider that times out while reporting a 404-ish upstream message is still a
        // timeout: the first matching test wins, as in the plugin.
        assert_eq!(
            classify_error("upstream 404 timed out after 10s", Some(1)),
            "Timeout"
        );
        // The bare-429 test is word-bounded: 4291 is not a rate limit.
        assert_eq!(
            classify_error("http error 4291", Some(1)),
            "Stream Unreachable"
        );
    }

    #[test]
    fn the_health_class_name_is_finer_than_the_class() {
        assert_eq!(health_class(Class::Alive, NO_ERROR), "alive");
        assert_eq!(health_class(Class::Skipped, SKIPPED), "skipped");
        assert_eq!(health_class(Class::Dead, BLACK_SCREEN), "blank");
        assert_eq!(health_class(Class::Dead, FROZEN_VIDEO), "frozen");
        assert_eq!(health_class(Class::Dead, SILENT_AUDIO), "silent");
        assert_eq!(health_class(Class::Dead, PLACEHOLDER_FILE), "placeholder");
        assert_eq!(health_class(Class::Dead, "Timeout"), "dead");
        // A blank/frozen/silent/placeholder row reads back as Dead, so a sort or a match that
        // only asks "is it dead" keeps the plugin's own answer.
        for stored in ["dead", "blank", "frozen", "silent", "placeholder"] {
            assert_eq!(Class::parse(stored), Some(Class::Dead), "{stored}");
        }
        assert_eq!(Class::parse("alive"), Some(Class::Alive));
        assert_eq!(Class::parse(""), None);
    }

    #[test]
    fn only_a_retryable_error_type_is_retried() {
        for retryable in RETRYABLE {
            assert!(is_retryable(retryable), "{retryable}");
        }
        for permanent in [
            "404 Not Found",
            "403 Forbidden",
            RATE_LIMITED,
            NO_VIDEO_STREAM,
            "Black Screen",
            "Other",
        ] {
            assert!(!is_retryable(permanent), "{permanent}");
        }
    }

    #[test]
    fn a_streamlink_host_is_matched_by_suffix_not_by_substring() {
        let hosts = vec!["youtube.com".to_string(), "twitch.tv".to_string()];
        assert!(is_streamlink_only(
            "https://www.youtube.com/watch?v=1",
            &hosts
        ));
        assert!(is_streamlink_only("https://YouTube.com/x", &hosts));
        assert!(is_streamlink_only("https://twitch.tv/x", &hosts));
        assert!(!is_streamlink_only("https://notyoutube.com/x", &hosts));
        assert!(!is_streamlink_only(
            "https://youtube.com.evil.test/x",
            &hosts
        ));
        assert!(!is_streamlink_only("http://provider.test/live.ts", &hosts));
        assert!(!is_streamlink_only("not a url", &hosts));
        assert!(!is_streamlink_only("http://provider.test/x", &[]));
    }

    #[test]
    fn a_url_never_leaves_the_probe_in_an_error() {
        let masked = mask_url_in_error(
            "http://host/live/u/p/12345.ts: Server returned 404 Not Found",
            "http://host/live/u/p/12345.ts",
            77,
        );
        assert_eq!(masked, "[Stream ID: 77]: Server returned 404 Not Found");

        // The URL-encoded spelling, when a tool quotes the request line.
        let url = "http://host/live/u/p/1.ts?token=a b";
        let quoted = format!("{}: refused", url_encode(url).expect("encodes"));
        assert_eq!(
            mask_url_in_error(&quoted, url, 5),
            "[Stream ID: 5]: refused"
        );

        // Nothing to mask, nothing changed.
        assert_eq!(mask_url_in_error("boom", "", 1), "boom");
        assert_eq!(mask_url_in_error("", "http://h/x", 1), "");
        assert_eq!(mask_url_in_error("boom", "http://other/x", 1), "boom");
    }

    #[test]
    fn a_low_framerate_needs_a_known_sub_threshold_value() {
        assert!(is_low_framerate(23.9));
        assert!(is_low_framerate(1.0));
        assert!(!is_low_framerate(24.0), "24 is the threshold, not below it");
        assert!(!is_low_framerate(25.0));
        assert!(!is_low_framerate(0.0), "no measurement is not slow");
        assert!(!is_low_framerate(-1.0));
    }
}
