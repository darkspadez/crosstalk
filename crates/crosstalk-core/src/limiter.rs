//! Per-key concurrency and rate limiting.
//!
//! Both ported plugins rate limiting at once, in three different places, with settings that
//! only ever applied to their own worker pool:
//!
//!   * IPTV Checker caps connections per M3U account (`max_streams_per_provider`, 2 workers by
//!     default) and spaces streams by `stream_delay` seconds;
//!   * Stream-Mapparr spaces probes per account by 1 s and caps them globally at
//!     `probe_rate_per_minute`;
//!   * the plan's rule for `streams.probe` is 1 connection per M3U account by default, never
//!     above that account's own `max_streams`, raised per account in settings.
//!
//! One limiter expresses all three, keyed by whatever the caller identifies the provider
//! account with (`"m3u:7"`), so `probe`, `match` and the stream-list writer can share it
//! without either knowing the other's settings:
//!
//!   * **per-key permits** — how many work units of one key may be in flight at once;
//!   * **per-key overrides and hard caps** — an account raised to 4, but never above its
//!     `max_streams`;
//!   * **a global cap** — total work units across every key;
//!   * **per-key spacing** — a minimum gap between two starts of the same key;
//!   * **a global rate** — starts per minute, counted over a rolling minute.
//!
//! Waiting is cancellation-aware: [`Limiter::acquire_or_cancel`] gives up within one poll
//! interval of the run's cancel flag being set, rather than when the wait would have ended.
//! That matters because a spacing of a few seconds and a per-minute rate both mean a run can
//! spend most of its wall clock waiting.
//!
//! Fairness is "wake everyone, first to re-check wins": a release notifies every waiter on
//! every key and each re-checks its own key under the lock. The counts involved (a handful of
//! provider accounts, a few dozen workers) make that cheaper than a queue, and no waiter can be
//! starved into a deadlock because the check is always against live state.

use anyhow::{bail, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

/// How often a waiting acquire re-reads the cancel flag.
///
/// A wait can be a whole minute long (the global rate), and a cancelled run must not sit in it:
/// this is the worst-case delay between the operator pressing Cancel and the acquire returning.
pub const CANCEL_POLL: Duration = Duration::from_millis(50);

/// The window a [`LimiterConfig::rate_per_minute`] is counted over.
pub const RATE_WINDOW: Duration = Duration::from_secs(60);

/// What a limiter may admit, configured once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimiterConfig {
    /// Work units of one key allowed at once, when the key has no override. A key with 0 is
    /// treated as 1: a limiter that admits nothing would deadlock its first caller.
    pub permits: usize,
    /// Work units allowed across every key at once. 0 means unlimited.
    pub global_permits: usize,
    /// Minimum gap between two granted acquires of the same key. Zero means no spacing.
    pub min_spacing: Duration,
    /// Granted acquires per rolling minute, across every key. 0 means unlimited.
    pub rate_per_minute: u32,
}

impl Default for LimiterConfig {
    fn default() -> Self {
        Self {
            // The plan's default for probing: one connection per M3U account.
            permits: 1,
            global_permits: 0,
            min_spacing: Duration::ZERO,
            rate_per_minute: 0,
        }
    }
}

/// The per-key counters, under one lock.
#[derive(Debug, Default)]
struct State {
    in_use: HashMap<String, usize>,
    total: usize,
    /// The earliest instant the key's next start is allowed (spacing).
    next_start: HashMap<String, Instant>,
    /// Start times inside the rolling minute, oldest first.
    starts: VecDeque<Instant>,
}

/// What one attempt to acquire found.
enum Wait {
    /// A permit was granted; the caller owns it until the [`Permit`] drops.
    Granted,
    /// A permit count is exhausted: only a release can help, so wait for the notification.
    Blocked,
    /// A time-based limit (spacing or rate) ends at this instant.
    At(Instant),
}

impl State {
    fn try_acquire(
        &mut self,
        key: &str,
        limit: usize,
        config: &LimiterConfig,
        now: Instant,
    ) -> Wait {
        // The rolling minute: drop the starts that have aged out before counting.
        while let Some(start) = self.starts.front() {
            if now.saturating_duration_since(*start) < RATE_WINDOW {
                break;
            }
            self.starts.pop_front();
        }
        // Spacing state is only needed while a key is live or its gap has not elapsed: an
        // idle key whose gap passed is forgotten, so the map stays at the live keys plus the
        // recently used instead of every key ever seen.
        let in_use = &self.in_use;
        self.next_start
            .retain(|other, next| *next > now || in_use.contains_key(other));

        if self.in_use.get(key).copied().unwrap_or(0) >= limit {
            return Wait::Blocked;
        }
        if config.global_permits > 0 && self.total >= config.global_permits {
            return Wait::Blocked;
        }

        let mut at: Option<Instant> = None;
        if !config.min_spacing.is_zero() {
            if let Some(next) = self.next_start.get(key) {
                if *next > now {
                    at = Some(*next);
                }
            }
        }
        if config.rate_per_minute > 0 {
            let rate = config.rate_per_minute as usize;
            if self.starts.len() >= rate {
                // The oldest start inside the window decides when one more fits.
                let oldest = self.starts[self.starts.len() - rate];
                at = Some(at.map_or(oldest + RATE_WINDOW, |current| {
                    current.max(oldest + RATE_WINDOW)
                }));
            }
        }
        if let Some(instant) = at {
            return Wait::At(instant);
        }

        *self.in_use.entry(key.to_string()).or_insert(0) += 1;
        self.total += 1;
        if !config.min_spacing.is_zero() {
            self.next_start
                .insert(key.to_string(), now + config.min_spacing);
        }
        if config.rate_per_minute > 0 {
            self.starts.push_back(now);
        }
        Wait::Granted
    }

    fn release(&mut self, key: &str) {
        if let Some(count) = self.in_use.get_mut(key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.in_use.remove(key);
            }
        }
        self.total = self.total.saturating_sub(1);
    }
}

/// The configuration, behind its own lock so a builder call works whether or not the limiter is
/// already shared. In practice every key is configured before the limiter is handed to workers.
#[derive(Debug)]
struct Settings {
    base: LimiterConfig,
    overrides: HashMap<String, usize>,
    caps: HashMap<String, usize>,
}

#[derive(Debug)]
struct Inner {
    settings: Mutex<Settings>,
    state: Mutex<State>,
    released: Notify,
}

impl Inner {
    /// Recover from a poisoned lock: a panic while holding the counters must not take every
    /// later run down with it.
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn settings(&self) -> std::sync::MutexGuard<'_, Settings> {
        self.settings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// How many permits one key gets: its override (or the default), clamped by its hard cap.
    fn permits_for(&self, key: &str) -> usize {
        let settings = self.settings();
        let wanted = settings
            .overrides
            .get(key)
            .copied()
            .unwrap_or(settings.base.permits)
            .max(1);
        match settings.caps.get(key).copied().filter(|cap| *cap > 0) {
            Some(cap) => wanted.min(cap.max(1)),
            None => wanted,
        }
    }

    fn config(&self) -> LimiterConfig {
        self.settings().base
    }
}

/// A keyed limiter. Cloneable and cheap: the counters are shared.
#[derive(Debug, Clone)]
pub struct Limiter {
    inner: Arc<Inner>,
}

impl Limiter {
    pub fn new(config: LimiterConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                settings: Mutex::new(Settings {
                    base: config,
                    overrides: HashMap::new(),
                    caps: HashMap::new(),
                }),
                state: Mutex::new(State::default()),
                released: Notify::new(),
            }),
        }
    }

    /// Give one key its own permit count (the plan's "raise per account in settings").
    ///
    /// Configure before sharing: the limiter is `Clone` over shared counters, so this
    /// mutates every handle, not just this one. In practice every key is configured before
    /// the limiter is handed to workers.
    pub fn with_permits(self, key: impl Into<String>, permits: usize) -> Self {
        self.inner.settings().overrides.insert(key.into(), permits);
        self
    }

    /// Hold one key at or below `cap` permits whatever its override says — an M3U account's
    /// `max_streams`. A cap of 0 means "no cap known", which is what a provider that does not
    /// publish one reads as.
    pub fn with_hard_cap(self, key: impl Into<String>, cap: usize) -> Self {
        self.inner.settings().caps.insert(key.into(), cap);
        self
    }

    /// The config this limiter was built with.
    pub fn config(&self) -> LimiterConfig {
        self.inner.config()
    }

    /// How many permits `key` may hold at once, after its override and its hard cap.
    pub fn permits_for(&self, key: &str) -> usize {
        self.inner.permits_for(key)
    }

    /// How many permits `key` is holding right now.
    pub fn in_use(&self, key: &str) -> usize {
        self.inner.lock().in_use.get(key).copied().unwrap_or(0)
    }

    /// How many permits every key is holding right now.
    pub fn total_in_use(&self) -> usize {
        self.inner.lock().total
    }

    /// Wait for a permit for `key`.
    ///
    /// Infallible on purpose: a caller that cannot be cancelled (a one-shot tool) should not
    /// have to handle an error that can never happen.
    pub async fn acquire(&self, key: &str) -> Permit {
        self.acquire_inner(key, None)
            .await
            .expect("an acquire without a cancel flag cannot fail")
    }

    /// Wait for a permit for `key`, giving up when the run is cancelled.
    ///
    /// The flag is the job's own cancel flag ([`crate::jobs::JobContext::cancel_flag`]), so a
    /// cancelled probe stops inside a spacing wait instead of after it.
    pub async fn acquire_or_cancel(
        &self,
        key: &str,
        cancelled: &Arc<AtomicBool>,
    ) -> Result<Permit> {
        self.acquire_inner(key, Some(cancelled)).await
    }

    async fn acquire_inner(
        &self,
        key: &str,
        cancelled: Option<&Arc<AtomicBool>>,
    ) -> Result<Permit> {
        loop {
            if let Some(flag) = cancelled {
                if flag.load(Ordering::SeqCst) {
                    bail!("cancelled by the operator while waiting for a {key} slot");
                }
            }
            // Re-read every pass: an operator may raise an account's permits while a probe
            // is waiting, and the waiter must see the new budget on its next check.
            let limit = self.inner.permits_for(key);

            // Register interest BEFORE looking at the counters: a release that lands between
            // the check and the await would otherwise be missed and the waiter would sit until
            // its timer expired.
            let released = self.inner.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();

            let config = self.inner.config();
            let wait = {
                let mut state = self.inner.lock();
                state.try_acquire(key, limit, &config, Instant::now())
            };
            match wait {
                Wait::Granted => {
                    return Ok(Permit {
                        inner: Arc::clone(&self.inner),
                        key: key.to_string(),
                    })
                }
                Wait::Blocked => match cancelled {
                    Some(flag) => {
                        tokio::select! {
                            _ = released => {}
                            _ = until_cancelled(flag) => {
                                bail!("cancelled by the operator while waiting for a {key} slot")
                            }
                        }
                    }
                    None => released.await,
                },
                Wait::At(instant) => match cancelled {
                    Some(flag) => {
                        tokio::select! {
                            _ = released => {}
                            _ = tokio::time::sleep_until(instant) => {}
                            _ = until_cancelled(flag) => {
                                bail!("cancelled by the operator while waiting for a {key} slot")
                            }
                        }
                    }
                    None => {
                        tokio::select! {
                            _ = released => {}
                            _ = tokio::time::sleep_until(instant) => {}
                        }
                    }
                },
            }
        }
    }
}

/// Resolves once the flag is set, checked every [`CANCEL_POLL`].
async fn until_cancelled(flag: &Arc<AtomicBool>) {
    loop {
        if flag.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(CANCEL_POLL).await;
    }
}

/// One granted slot. Dropping it releases the permit, so a `?` on the work in between cannot
/// leak a connection.
#[derive(Debug)]
pub struct Permit {
    inner: Arc<Inner>,
    key: String,
}

impl Permit {
    /// The key this permit belongs to.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.inner.lock().release(&self.key);
        self.inner.released.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter(permits: usize) -> Limiter {
        Limiter::new(LimiterConfig {
            permits,
            ..LimiterConfig::default()
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_key_admits_its_permits_then_waits_for_a_release() {
        let limiter = limiter(2);
        let first = limiter.acquire("m3u:1").await;
        let _second = limiter.acquire("m3u:1").await;
        assert_eq!(limiter.in_use("m3u:1"), 2);
        assert_eq!(limiter.total_in_use(), 2);

        let mut third = Box::pin(limiter.acquire("m3u:1"));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut third)
                .await
                .is_err(),
            "the third acquire must wait for a permit"
        );

        drop(first);
        assert_eq!(limiter.in_use("m3u:1"), 1);
        let permit = tokio::time::timeout(Duration::from_secs(1), third)
            .await
            .expect("the release wakes the waiter");
        assert_eq!(permit.key(), "m3u:1");
    }

    #[tokio::test(start_paused = true)]
    async fn keys_do_not_share_their_permits() {
        let limiter = limiter(1);
        let _one = limiter.acquire("m3u:1").await;
        // A second account has its own connection budget: the plan's whole point of keying.
        let _two = tokio::time::timeout(Duration::from_millis(10), limiter.acquire("m3u:2"))
            .await
            .expect("another key is not blocked");
        assert_eq!(limiter.in_use("m3u:1"), 1);
        assert_eq!(limiter.in_use("m3u:2"), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_override_and_a_hard_cap_are_both_honoured() {
        let limiter = limiter(1)
            .with_permits("m3u:7", 4)
            .with_hard_cap("m3u:7", 2);
        assert_eq!(
            limiter.permits_for("m3u:7"),
            2,
            "the cap wins over the override"
        );
        assert_eq!(limiter.permits_for("m3u:1"), 1);

        // An account with no published `max_streams` writes 0, which must not become a cap of
        // zero permits: that would deadlock the account's first stream.
        let unlimited = limiter
            .clone()
            .with_permits("m3u:8", 3)
            .with_hard_cap("m3u:8", 0);
        assert_eq!(unlimited.permits_for("m3u:8"), 3);

        // A permit count of 0 is a setting mistake, not a lockout.
        assert_eq!(
            limiter
                .clone()
                .with_permits("m3u:9", 0)
                .permits_for("m3u:9"),
            1
        );

        let first = limiter.acquire("m3u:7").await;
        let _second = limiter.acquire("m3u:7").await;
        let mut third = Box::pin(limiter.acquire("m3u:7"));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut third)
                .await
                .is_err(),
            "the hard cap of 2 holds even though the override says 4"
        );
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), third)
            .await
            .expect("released");
    }

    #[tokio::test(start_paused = true)]
    async fn a_global_cap_holds_across_keys() {
        let limiter = Limiter::new(LimiterConfig {
            permits: 4,
            global_permits: 2,
            ..LimiterConfig::default()
        });
        let _a = limiter.acquire("m3u:1").await;
        let _b = limiter.acquire("m3u:2").await;
        assert_eq!(limiter.total_in_use(), 2);

        let mut third = Box::pin(limiter.acquire("m3u:3"));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut third)
                .await
                .is_err(),
            "a third key waits for the global cap"
        );
        drop(_a);
        tokio::time::timeout(Duration::from_secs(1), third)
            .await
            .expect("the global slot was released");
    }

    #[tokio::test(start_paused = true)]
    async fn spacing_delays_the_next_start_of_the_same_key_only() {
        let limiter = Limiter::new(LimiterConfig {
            permits: 1,
            min_spacing: Duration::from_secs(2),
            ..LimiterConfig::default()
        });
        drop(limiter.acquire("m3u:1").await);
        // Released, so the permit is free; the spacing is what holds the next start back.
        let mut second = Box::pin(limiter.acquire("m3u:1"));
        assert!(
            tokio::time::timeout(Duration::from_millis(500), &mut second)
                .await
                .is_err(),
            "the second start waits out the spacing"
        );
        // Another key is not spaced against the first.
        tokio::time::timeout(Duration::from_millis(10), limiter.acquire("m3u:2"))
            .await
            .expect("another key starts at once");

        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("spacing elapsed");
    }

    #[tokio::test(start_paused = true)]
    async fn the_rate_caps_starts_per_rolling_minute() {
        let limiter = Limiter::new(LimiterConfig {
            permits: 1,
            rate_per_minute: 2,
            ..LimiterConfig::default()
        });
        drop(limiter.acquire("m3u:1").await);
        drop(limiter.acquire("m3u:2").await);

        let mut third = Box::pin(limiter.acquire("m3u:3"));
        assert!(
            tokio::time::timeout(Duration::from_secs(30), &mut third)
                .await
                .is_err(),
            "two starts in the minute is the whole budget"
        );
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::time::timeout(Duration::from_secs(1), third)
            .await
            .expect("the window rolled over");
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_wait_gives_up_instead_of_waiting_out_the_spacing() {
        let limiter = Limiter::new(LimiterConfig {
            permits: 1,
            min_spacing: Duration::from_secs(60),
            ..LimiterConfig::default()
        });
        drop(limiter.acquire("m3u:1").await);

        let flag = Arc::new(AtomicBool::new(false));
        let waiting = {
            let limiter = limiter.clone();
            let flag = Arc::clone(&flag);
            tokio::spawn(async move { limiter.acquire_or_cancel("m3u:1", &flag).await })
        };
        // Let the waiter reach its spacing wait, then cancel it.
        tokio::task::yield_now().await;
        flag.store(true, Ordering::SeqCst);
        let result = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the waiter returns")
            .expect("no panic");
        let error = result.expect_err("the acquire was cancelled");
        assert!(format!("{error}").contains("cancelled"), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn an_already_cancelled_run_does_not_take_a_permit() {
        let limiter = limiter(1);
        let flag = Arc::new(AtomicBool::new(true));
        assert!(limiter.acquire_or_cancel("m3u:1", &flag).await.is_err());
        assert_eq!(limiter.total_in_use(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_permit_never_leaks_a_slot() {
        // A `?` in the middle of a probe drops the permit: the connection is not leaked, which
        // is the whole reason the permit is an owned value rather than a counter pair.
        let limiter = limiter(1);
        {
            let _permit = limiter.acquire("m3u:1").await;
            assert_eq!(limiter.in_use("m3u:1"), 1);
        }
        assert_eq!(limiter.in_use("m3u:1"), 0);
        assert_eq!(limiter.total_in_use(), 0);
        // And the slot is usable again straight away.
        let _again = tokio::time::timeout(Duration::from_millis(10), limiter.acquire("m3u:1"))
            .await
            .expect("free");
    }
}
