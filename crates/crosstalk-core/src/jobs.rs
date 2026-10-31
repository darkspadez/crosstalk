//! Job plumbing: the context an action runs with, and its progress channel.
//!
//! A sub-app never touches the configuration file, the database or the network directly.
//! It receives a [`JobContext`] holding everything the host owns, so the same action code
//! runs from the daemon, from `crosstalk run`, and from a test.

use crate::config::CrosstalkConfig;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// One progress update, as the UI and the run record show it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProgressEvent {
    /// Which action is running, for a client that joined late.
    pub action: String,
    /// Work completed so far.
    pub current: u64,
    /// Work expected. Zero means "not known yet".
    pub total: u64,
    /// A short human-readable phase, for example "validating candidates".
    pub phase: String,
    pub done: bool,
    /// Filled in when `done` is set.
    pub summary: Option<String>,
}

impl ProgressEvent {
    pub fn new(
        action: impl Into<String>,
        current: u64,
        total: u64,
        phase: impl Into<String>,
    ) -> Self {
        Self {
            action: action.into(),
            current,
            total,
            phase: phase.into(),
            done: false,
            summary: None,
        }
    }

    /// Completion percentage, or `None` while the total is unknown.
    pub fn percent(&self) -> Option<u8> {
        if self.total == 0 {
            return None;
        }
        Some(((self.current.min(self.total) * 100) / self.total) as u8)
    }
}

/// Where progress updates go.
///
/// The daemon's implementation fans out to the SSE stream and the run store; the CLI's
/// prints to stderr; tests use [`NullSink`].
pub trait ProgressSink: Send + Sync {
    fn publish(&self, event: &ProgressEvent);
}

/// Discards updates. Used by tests and by `--quiet` runs.
#[derive(Debug, Default)]
pub struct NullSink;

impl ProgressSink for NullSink {
    fn publish(&self, _event: &ProgressEvent) {}
}

/// Collects updates in memory, for tests and for the one-shot CLI path.
#[derive(Debug, Default)]
pub struct VecSink {
    events: std::sync::Mutex<Vec<ProgressEvent>>,
}

impl VecSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<ProgressEvent> {
        self.events.lock().expect("sink lock").clone()
    }

    pub fn last(&self) -> Option<ProgressEvent> {
        self.events.lock().expect("sink lock").last().cloned()
    }
}

impl ProgressSink for VecSink {
    fn publish(&self, event: &ProgressEvent) {
        self.events.lock().expect("sink lock").push(event.clone());
    }
}

/// Everything an action is given.
#[derive(Clone)]
pub struct JobContext {
    config: Arc<CrosstalkConfig>,
    progress: Arc<dyn ProgressSink>,
    /// Cancellation flag, set when the UI cancels a run or the daemon shuts down.
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Debug for JobContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobContext")
            .field("config_dir", &self.config.config_dir)
            .finish_non_exhaustive()
    }
}

impl JobContext {
    pub fn new(config: Arc<CrosstalkConfig>, progress: Arc<dyn ProgressSink>) -> Self {
        Self {
            config,
            progress,
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// A context that discards progress: the one-shot CLI path, and tests.
    pub fn headless(config: CrosstalkConfig) -> Self {
        Self::new(Arc::new(config), Arc::new(NullSink))
    }

    /// Alias kept so library tests read naturally.
    pub fn for_tests(config: CrosstalkConfig) -> Self {
        Self::headless(config)
    }

    pub fn config(&self) -> &CrosstalkConfig {
        &self.config
    }

    pub fn config_arc(&self) -> Arc<CrosstalkConfig> {
        Arc::clone(&self.config)
    }

    pub fn exports_dir(&self) -> PathBuf {
        self.config.exports_dir()
    }

    pub fn data_dir(&self) -> PathBuf {
        self.config.data_dir()
    }

    pub fn progress(&self, event: &ProgressEvent) {
        self.progress.publish(event);
    }

    /// A cheap paced reporter: publishes at most once per interval, plus the final event.
    ///
    /// The plugin compared `time.time()` against the last flush every 2 seconds for the
    /// same reason: an update per channel would swamp the websocket without telling the
    /// operator anything new.
    pub fn paced(&self, action: &str, total: u64, interval: Duration) -> PacedProgress {
        PacedProgress {
            sink: Arc::clone(&self.progress),
            action: action.to_string(),
            total,
            interval,
            // `checked_sub` rather than plain subtraction: a very large interval would
            // otherwise panic on the first report.
            last: std::time::Instant::now()
                .checked_sub(interval)
                .unwrap_or_else(std::time::Instant::now),
            current: 0,
        }
    }

    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Rate-limited progress reporter.
pub struct PacedProgress {
    sink: Arc<dyn ProgressSink>,
    action: String,
    total: u64,
    interval: Duration,
    last: std::time::Instant,
    current: u64,
}

impl PacedProgress {
    pub fn set(&mut self, current: u64, phase: &str) {
        self.current = current;
        if self.last.elapsed() >= self.interval {
            self.last = std::time::Instant::now();
            self.sink.publish(&ProgressEvent::new(
                &self.action,
                current,
                self.total,
                phase,
            ));
        }
    }

    pub fn finish(&mut self, summary: impl Into<String>) {
        let mut event = ProgressEvent::new(&self.action, self.current, self.total, "done");
        event.done = true;
        event.summary = Some(summary.into());
        self.sink.publish(&event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_handles_an_unknown_total() {
        assert_eq!(ProgressEvent::new("a", 0, 0, "x").percent(), None);
        assert_eq!(ProgressEvent::new("a", 5, 10, "x").percent(), Some(50));
        // Never over 100.
        assert_eq!(ProgressEvent::new("a", 20, 10, "x").percent(), Some(100));
    }

    #[test]
    fn vec_sink_collects_events() {
        let sink = Arc::new(VecSink::new());
        sink.publish(&ProgressEvent::new("a", 1, 2, "x"));
        assert_eq!(sink.events().len(), 1);
        assert_eq!(sink.last().expect("event").current, 1);
    }

    #[test]
    fn paced_reporter_limits_updates_but_always_reports_the_end() {
        let sink = Arc::new(VecSink::new());
        let context = JobContext::new(Arc::new(CrosstalkConfig::default()), sink.clone());
        let mut paced = context.paced("janitor.automatch", 100, Duration::from_secs(60));
        for index in 0..10 {
            paced.set(index, "working");
        }
        // The first update goes out immediately, so a client that just joined sees the run
        // start; the rest of the loop is inside the interval and is suppressed.
        assert_eq!(sink.events().len(), 1);

        paced.finish("done");
        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert!(events[1].done);
        assert_eq!(events[1].summary.as_deref(), Some("done"));
    }

    #[test]
    fn cancellation_is_visible_to_the_action() {
        let context = JobContext::for_tests(CrosstalkConfig::default());
        assert!(!context.is_cancelled());
        context.cancel();
        assert!(context.is_cancelled());
    }

    #[test]
    fn directories_come_from_the_config_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = CrosstalkConfig::load(dir.path()).expect("load");
        let context = JobContext::for_tests(config);
        assert_eq!(context.exports_dir(), dir.path().join("exports"));
    }
}
