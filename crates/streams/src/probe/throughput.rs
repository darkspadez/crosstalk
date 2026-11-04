//! Throughput: measuring how fast a provider actually delivers bytes.
//!
//! Port of Stream-Mapparr's `_probe_stream_throughput` (plugin.py:5096) and the cache entry
//! `probe_throughput_action` (10278) writes, folded into `streams.probe` as the plan's
//! "throughput mode".
//!
//! # What it measures, and why an HTTP download
//!
//! Bitrate says what the provider CLAIMS; throughput says what it DELIVERS. Stream-Mapparr read
//! the URL for `probe_duration_seconds` (8), summed the bytes, and reported Mbit/s against a
//! nominal bitrate estimated from resolution (and frame rate) as `_estimate_nominal_bitrate`
//! (5032). The quality sort then has a "can this stream keep up with its own picture" tier,
//! which is what makes it drop a 4K feed that only delivers 1 Mbit/s behind an HD one that
//! delivers 30 (see [`crate::quality::classify_throughput`]).
//!
//! # The two guards, both kept
//!
//!   * **a short read is a FAILED probe, not 0 Mbit/s.** An HLS playlist is a few kilobytes of
//!     text, and a redirect can end at a 0-byte endpoint: reporting `~0.01 Mbit/s` for one would
//!     rank a perfectly good stream as insufficient. So fewer than 64 KiB in under a second is
//!     `None` (5096's `bytes_read < 64 * 1024 and elapsed < 1.0`);
//!   * **a failed probe is never "fresh".** The measurement is written even when it is `None`,
//!     because `quality::is_probe_fresh` refuses a `None` — a run that errors on every URL must
//!     not lock those streams out of re-probing for a whole TTL window.
//!
//! # Why a trait
//!
//! The action has to run in tests and in the parity harness without a provider. [`ThroughputMeter`]
//! is that seam: [`HttpMeter`] streams a real response, [`FakeMeter`] answers from a table and
//! [`NoMeter`] is what a run with throughput sorting off uses (no request at all, rather than a
//! request whose answer is thrown away).
//!
//! # One user agent, no account lookup
//!
//! Stream-Mapparr looked up the M3U account's own user agent (`_get_m3u_user_agent`, 5152) and
//! fell back to its default. Dispatcharr does not serialise `M3UAccount.user_agent` to the API
//! the client reads, so this sends the default; a per-account user agent is a client addition,
//! not a probe behaviour, and the probe's own ffprobe pass already sends VLC's.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Stream-Mapparr's `DEFAULT_PROBE_USER_AGENT` (523).
pub const DEFAULT_USER_AGENT: &str = "VLC/3.0.20 LibVLC/3.0.20";

/// `MIN_READ_BYTES` / `MIN_READ_SECONDS`: below both, the read was not a sustained transfer.
pub const MIN_READ_BYTES: u64 = 64 * 1024;
pub const MIN_READ_SECONDS: f64 = 1.0;

/// `urlopen(req, timeout=10)`: the open (connect, TLS, first byte) cap.
pub const OPEN_TIMEOUT: Duration = Duration::from_secs(10);

/// What a log line may say about a URL: its scheme and host, and nothing else.
///
/// Provider URLs carry credentials (a userinfo, a token path, a query string), and a log line
/// is one paste into an issue away from public. The host is what an operator needs to spot a
/// per-edge throttle; the rest never leaves the probe (see
/// [`crate::probe::classify::mask_url_in_error`]).
fn log_target(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(parsed) => match parsed.host_str() {
            Some(host) => format!("{}://{host}", parsed.scheme()),
            None => "the stream URL".to_string(),
        },
        Err(_) => "the stream URL".to_string(),
    }
}

///
/// `edge_ip` is the final URL's host after redirects, kept for diagnostics and for spotting
/// per-edge throttling. Stream-Mapparr did not resolve it to an address — one DNS round trip
/// would double the probe's cost — so neither does this.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ThroughputMeasure {
    /// `None` means the probe ran and measured nothing, which is never "fresh".
    pub mbps: Option<f64>,
    pub edge_ip: Option<String>,
}

impl ThroughputMeasure {
    pub fn measured(mbps: f64, edge_ip: Option<String>) -> Self {
        Self {
            mbps: Some(mbps),
            edge_ip,
        }
    }

    pub fn failed(edge_ip: Option<String>) -> Self {
        Self {
            mbps: None,
            edge_ip,
        }
    }
}

/// Anything that can measure one URL's delivery rate.
pub trait ThroughputMeter: Send + Sync {
    fn measure<'a>(
        &'a self,
        url: &'a str,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = ThroughputMeasure> + Send + 'a>>;
}

/// No measurement at all: what a run with throughput sorting off uses.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoMeter;

impl ThroughputMeter for NoMeter {
    fn measure<'a>(
        &'a self,
        _url: &'a str,
        _duration: Duration,
    ) -> Pin<Box<dyn Future<Output = ThroughputMeasure> + Send + 'a>> {
        Box::pin(async { ThroughputMeasure::default() })
    }
}

/// Answers from a table, keyed by URL. A missing entry measures nothing (like a failed open).
#[derive(Debug, Default, Clone)]
pub struct FakeMeter {
    answers: std::sync::Arc<Mutex<HashMap<String, ThroughputMeasure>>>,
    calls: std::sync::Arc<Mutex<Vec<String>>>,
}

impl FakeMeter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn answering(&self, url: &str, measure: ThroughputMeasure) {
        self.answers
            .lock()
            .expect("fake meter lock")
            .insert(url.to_string(), measure);
    }

    /// Every URL it was asked about, in order.
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("fake meter lock").clone()
    }
}

impl ThroughputMeter for FakeMeter {
    fn measure<'a>(
        &'a self,
        url: &'a str,
        _duration: Duration,
    ) -> Pin<Box<dyn Future<Output = ThroughputMeasure> + Send + 'a>> {
        let answer = {
            self.calls
                .lock()
                .expect("fake meter lock")
                .push(url.to_string());
            self.answers
                .lock()
                .expect("fake meter lock")
                .get(url)
                .cloned()
        };
        Box::pin(async move { answer.unwrap_or_default() })
    }
}

/// The real meter: one HTTP GET, read for `duration`, bytes → Mbit/s.
pub struct HttpMeter {
    client: reqwest::Client,
    user_agent: String,
    cancelled: Arc<AtomicBool>,
}

impl std::fmt::Debug for HttpMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpMeter")
            .field("user_agent", &self.user_agent)
            .finish_non_exhaustive()
    }
}

impl HttpMeter {
    /// A meter tied to one run's cancel flag, so a cancelled run stops reading at once.
    ///
    /// The client has no TOTAL timeout on purpose: the read loop enforces the duration, and a
    /// total timeout shorter than the window would abort a healthy stream mid-measurement.
    pub fn new(cancelled: Arc<AtomicBool>) -> Self {
        Self::with_user_agent(cancelled, DEFAULT_USER_AGENT)
    }

    pub fn with_user_agent(cancelled: Arc<AtomicBool>, user_agent: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(OPEN_TIMEOUT)
            .user_agent(user_agent.into())
            .build()
            .unwrap_or_default();
        Self {
            client,
            user_agent: DEFAULT_USER_AGENT.to_string(),
            cancelled,
        }
    }

    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    async fn read(&self, url: &str, duration: Duration) -> ThroughputMeasure {
        let target = log_target(url);
        let response = match self.client.get(url).send().await {
            Ok(response) => response,
            Err(error) => {
                // WARNING, not DEBUG: a run where every probe failed has to be visible in a
                // normal log, which is exactly what Stream-Mapparr's 5110 comment says.
                tracing::warn!("throughput probe could not open {target}: {error}");
                return ThroughputMeasure::default();
            }
        };
        let edge = edge_host(response.url());

        let mut response = response;
        let started = Instant::now();
        let deadline = started + duration;
        let mut bytes: u64 = 0;
        loop {
            if self.cancelled.load(Ordering::SeqCst) {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let remaining = deadline - now;
            match tokio::time::timeout(remaining, response.chunk()).await {
                // The server closed the connection (an HLS playlist, a redirect to nothing).
                Ok(Ok(Some(chunk))) => bytes += chunk.len() as u64,
                Ok(Ok(None)) => break,
                Ok(Err(error)) => {
                    tracing::warn!("throughput probe read interrupted for {target}: {error}");
                    break;
                }
                // The window closed mid-read: whatever arrived counts.
                Err(_) => break,
            }
        }

        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        if bytes < MIN_READ_BYTES && elapsed < MIN_READ_SECONDS {
            tracing::warn!(
                "throughput probe got only {bytes} bytes in {elapsed:.2}s for {target} (likely an \
                 HLS playlist or a redirect, not a sustained read)"
            );
            return ThroughputMeasure::failed(edge);
        }
        ThroughputMeasure::measured(mbps(bytes, elapsed), edge)
    }
}

impl ThroughputMeter for HttpMeter {
    fn measure<'a>(
        &'a self,
        url: &'a str,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = ThroughputMeasure> + Send + 'a>> {
        Box::pin(self.read(url, duration))
    }
}

/// `(bytes × 8) / (elapsed × 1e6)`: Mbit/s with the plugin's own arithmetic and floor.
pub fn mbps(bytes: u64, elapsed_seconds: f64) -> f64 {
    let elapsed = elapsed_seconds.max(0.001);
    (bytes as f64 * 8.0) / (elapsed * 1_000_000.0)
}

/// The final URL's `netloc` (`host[:port]`), which is what Stream-Mapparr recorded.
fn edge_host(url: &reqwest::Url) -> Option<String> {
    let host = url.host_str()?;
    match url.port() {
        Some(port) => Some(format!("{host}:{port}")),
        None => Some(host.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_log_line_names_the_host_and_never_the_credential() {
        assert_eq!(
            log_target("http://user:pass@provider.test:8080/live/1.ts?token=secret"),
            "http://provider.test"
        );
        assert_eq!(
            log_target("https://provider.test/live/1.ts?token=secret"),
            "https://provider.test"
        );
        assert_eq!(log_target("not a url"), "the stream URL");
    }

    #[test]
    fn megabits_per_second_uses_the_plugins_arithmetic() {
        // 1 MB in one second is 8 Mbit/s.
        assert!((mbps(1_000_000, 1.0) - 8.0).abs() < 1e-9);
        // 5 MB in 8 seconds is 5 Mbit/s.
        assert!((mbps(5_000_000, 8.0) - 5.0).abs() < 1e-9);
        // The elapsed floor: a zero-elapsed read does not divide by zero.
        assert!(mbps(1_000, 0.0).is_finite());
    }

    #[tokio::test]
    async fn a_meter_that_is_off_measures_nothing_and_makes_no_request() {
        let meter = NoMeter;
        let measure = meter
            .measure("http://provider.test/x", Duration::from_secs(8))
            .await;
        assert_eq!(measure, ThroughputMeasure::default());
    }

    #[tokio::test]
    async fn the_fake_meter_answers_from_its_table() {
        let meter = FakeMeter::new();
        meter.answering(
            "http://a.test/x",
            ThroughputMeasure::measured(30.0, Some("edge.test".into())),
        );
        let measure = meter
            .measure("http://a.test/x", Duration::from_secs(8))
            .await;
        assert_eq!(measure.mbps, Some(30.0));
        assert_eq!(measure.edge_ip.as_deref(), Some("edge.test"));
        // A URL with no answer measured nothing — the same as a failed open.
        let measure = meter
            .measure("http://b.test/x", Duration::from_secs(8))
            .await;
        assert_eq!(measure, ThroughputMeasure::default());
        assert_eq!(meter.calls().len(), 2);
    }

    #[test]
    fn a_measurement_keeps_its_two_shapes_apart() {
        // A failed probe still records the edge it reached, and is never fresh.
        let failed = ThroughputMeasure::failed(Some("edge.test".into()));
        assert_eq!(failed.mbps, None);
        assert_eq!(failed.edge_ip.as_deref(), Some("edge.test"));
        let entry = crate::quality::ThroughputEntry {
            throughput_mbps: failed.mbps,
            measured_at: Some(chrono::Utc::now()),
            nominal_bitrate_mbps: None,
            edge_ip: failed.edge_ip.clone(),
        };
        assert!(!crate::quality::is_probe_fresh(
            &entry,
            30,
            chrono::Utc::now()
        ));
    }

    #[test]
    fn the_edge_is_the_host_and_port_of_the_final_url() {
        let url = reqwest::Url::parse("http://edge7.provider.test:8080/live/x.ts?token=1")
            .expect("parses");
        assert_eq!(edge_host(&url).as_deref(), Some("edge7.provider.test:8080"));
        let url = reqwest::Url::parse("https://edge.provider.test/x").expect("parses");
        assert_eq!(edge_host(&url).as_deref(), Some("edge.provider.test"));
    }

    /// A real local HTTP server, no provider: 200 KB in one second is ~1.6 Mbit/s, and a
    /// playlist-sized body is a failed probe.
    #[tokio::test]
    async fn the_http_meter_reads_a_real_response_and_refuses_a_tiny_one() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/bulk.ts"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0u8; 200_000]))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/playlist.m3u8"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string("#EXTM3U\n#EXTINF:6,\nseg.ts\n"),
            )
            .mount(&server)
            .await;

        let meter = HttpMeter::new(Arc::new(AtomicBool::new(false)));
        // A short window keeps the test fast; the floor is on bytes AND elapsed, so a small
        // body inside the window is still refused.
        let measure = meter
            .measure(
                &format!("{}/bulk.ts", server.uri()),
                Duration::from_millis(200),
            )
            .await;
        assert!(
            measure.mbps.is_some(),
            "a 200 KB body is a measurement: {measure:?}"
        );
        assert!(measure.mbps.unwrap() > 0.0);
        assert!(measure.edge_ip.is_some());

        let measure = meter
            .measure(
                &format!("{}/playlist.m3u8", server.uri()),
                Duration::from_millis(200),
            )
            .await;
        assert_eq!(measure.mbps, None, "a playlist is not a sustained read");
        assert!(measure.edge_ip.is_some(), "the edge is still recorded");

        // An open failure measures nothing at all.
        let measure = meter
            .measure("http://127.0.0.1:1/nothing", Duration::from_millis(200))
            .await;
        assert_eq!(measure, ThroughputMeasure::default());
    }
}
