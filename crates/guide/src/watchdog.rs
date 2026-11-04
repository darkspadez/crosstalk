//! `watchdog`: refresh the EPG sources whose guide is about to run dry, or whose last refresh
//! failed.
//!
//! Port of `EPG-Janitor/epg_watchdog.py` plus its Django glue (`plugin.py:78-175` and
//! `watchdog_run_check_now_action` at 3137). Dispatcharr's own EPG refresh has neither a retry
//! nor any freshness awareness, so a source that starts failing — or whose guide simply runs
//! out — stays broken until somebody notices (bug-072: epgshare-UK sat on `status=error` for
//! three days and silently blanked `UK: All`). The watchdog is the answer to that: it audits
//! the sources, refreshes the ones that need it, and records whether that helped.
//!
//! The decisions are the plugin's, verbatim:
//!
//!   * [`parse_exclude_ids`], [`is_candidate`], [`is_stale`], [`classify_outcome`],
//!     [`WatchdogSettings::coerce`] and the loop in [`run_check`] are line-for-line ports, and
//!     every case in the plugin's `tests/test_watchdog.py` is ported beside them;
//!   * outcome is judged by RELATIVE improvement — an error clearing, or the guide horizon
//!     moving forward — never against the arming threshold, so a legitimately short-horizon
//!     feed is not reported "still broken" for ever;
//!   * there is no debounce, because `EPGSource.updated_at` moves only on a *successful*
//!     refresh and therefore cannot mark a failed attempt;
//!   * a refresh that raises is logged and NOT counted as refreshed, and its classification is
//!     skipped.
//!
//! ## What the REST API costs
//!
//! The plugin read `Max(ProgramData.end_time)` per source with one SQL aggregate and had the
//! exact horizon for free. Here the same question is
//! [`epg_ends_at_or_after`](crosstalk_core::dispatcharr::DispatcharrClient::epg_ends_at_or_after),
//! so [`Reach`] says how much of it a run actually paid for:
//!
//!   * **is the guide still covering `now + threshold`?** one request. That is the whole stale
//!     decision for a healthy source, and the common case;
//!   * **what does it cover, then?** a bisection back from the cutoff — two requests for a
//!     source with no guide at all (nothing ends at or after 1970: the plugin's
//!     `Max(end_time) IS NULL`, so it is not refreshed, as in the plugin) and ~28 for one
//!     that is short but not empty (the cutoff probe, the 1970 probe, its re-probe inside
//!     the bracket, and ~25 bisection steps).
//!
//! Only a source this run is about to refresh pays for the bisection, and it has to: the report
//! prints "horizon before → after", and [`classify_outcome`]'s relative test compares the two.
//! The bracket reaches back to 1970 rather than stopping at the client's own seven-day lookback
//! because a guide that died a month ago is exactly the case the watchdog exists for — reading
//! it as "no guide" would leave it broken for ever — and the bisection costs `log2` of the
//! bracket, so the wider one is ~28 requests instead of ~17, on a source that needs a
//! refresh.
//!
//! ## Refreshes are sequential
//!
//! One source at a time, each waited to completion before the next. A refresh is a full XMLTV
//! fetch and parse on Dispatcharr's Celery worker: firing every stale source at once would queue
//! N of them against a small install's worker pool and then poll all N in parallel, which is
//! both a load spike and a timeout that no longer means anything. Sequential is also what the
//! plugin does (it calls `refresh_epg_data` inline).
//!
//! ## Deliberate divergences from the plugin
//!
//!   * **The schedule replaces the two on/off settings.** `watchdog_enabled` and
//!     `watchdog_check_interval_hours` are gone: the host has cron schedules, and the watchdog is
//!     an action, so `"guide.watchdog_apply" = "0 */6 * * *"` is the interval. A schedule naming
//!     the preview half stays a preview.
//!   * **One action, two halves.** `watchdog_preview` reports what would be refreshed and touches
//!     nothing; `watchdog_apply` refreshes. The plugin's button did both, which is why its colour
//!     was green even though it rewrites guide data: here the apply is orange and confirms.
//!   * **A refresh is waited for to a settled status, with a timeout.** The plugin's refresh
//!     ran inline in the Celery task it was already in; here it is queued and polled
//!     ([`await_epg_refresh`](crosstalk_core::dispatcharr::DispatcharrClient::await_epg_refresh))
//!     for `watchdog_refresh_timeout_minutes`. The wait ends when the source leaves
//!     `fetching`/`parsing` — returning on the first movement would hand the classifier a
//!     source mid-flight, whose horizon has no settled value to read — and a wait that lapses
//!     is reported as such rather than pretending the refresh failed.
//!   * **Recoveries are logged to the run log and the CSV**, not to a Dispatcharr System Event.
//!     `watchdog_log_on_recovery` survives, with the same meaning: a recovery is an info line,
//!     a failure is always a warning, and both always get a row.
//!   * **`has mapped channels` is read from the world**, not from `EpgSource::has_channels`.
//!     See [`mapped_source_ids`] for why.
//!   * **A failed re-read is "no improvement"**, where the plugin distinguished a vanished source
//!     (`DoesNotExist` → keep the before state) from a database error. Over REST every read can
//!     fail; an improvement that cannot be read is not an improvement, so the row says
//!     `still_broken` and the reason carries the error.

use crate::settings::GuideSettings;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{DispatcharrClient, EpgEntry, EpgHorizon, EpgSourceState, World};
use crosstalk_core::jobs::{JobContext, ProgressEvent};
use crosstalk_core::store::{RunRow, Subject};
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::time::Duration;

/// The columns the watchdog report carries: one row per candidate source.
pub const COLUMNS: [&str; 9] = [
    "source_id",
    "source_name",
    "status_before",
    "status_after",
    "horizon_before",
    "horizon_after",
    "stale",
    "action",
    "outcome",
];

/// Progress is published under this action name, whichever half is running.
const PROGRESS_ACTION: &str = "guide.watchdog";

// ---------------------------------------------------------------------------------------
// The ported decision logic
// ---------------------------------------------------------------------------------------

/// How far one source's guide reaches, and how much the run paid to find out.
///
/// The plugin's `SourceState.horizon` was an exact `max(end_time)` or `None`. Over REST the
/// exact value is the expensive half of the question, so the run records the precision it
/// actually has — and every rule below only ever compares what it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// No programme on the source at all: the plugin's `Max(end_time) IS NULL`, read as
    /// "nothing ends at or after 1970".
    Empty,
    /// A probe at this instant found a programme ending at or after it, and the run did not
    /// pay for the exact end. The true horizon is no earlier than this.
    AtLeast(DateTime<Utc>),
    /// The latest programme end, to the minute and rounded down (a bisection).
    At(DateTime<Utc>),
    /// Not asked, because the source's refresh status already decided the outcome.
    Unknown,
}

impl Reach {
    /// The value the plugin's `SourceState.horizon` holds: `None` for a guide that is not there
    /// (or was never read), and otherwise the best instant the run knows.
    pub fn horizon(self) -> Option<DateTime<Utc>> {
        match self {
            Self::Empty | Self::Unknown => None,
            Self::AtLeast(instant) | Self::At(instant) => Some(instant),
        }
    }

    /// The instant as a report prints it. `at least` marks a lower bound, so a reader is never
    /// told a guide ends at a time the run did not verify.
    pub fn describe(self) -> String {
        match self {
            Self::Empty => "empty (no programme at all)".to_string(),
            Self::Unknown => "not read".to_string(),
            Self::At(instant) => iso(instant),
            Self::AtLeast(instant) => format!("at least {}", iso(instant)),
        }
    }
}

/// One source's state, as the decisions see it.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceState {
    pub id: i64,
    pub name: String,
    /// Dispatcharr's `EPGSource.status`: `idle`, `fetching`, `parsing`, `success`, `error` or
    /// `disabled`.
    pub status: String,
    pub reach: Reach,
}

impl SourceState {
    pub fn new(id: i64, name: impl Into<String>, status: impl Into<String>, reach: Reach) -> Self {
        Self {
            id,
            name: name.into(),
            status: status.into(),
            reach,
        }
    }

    /// The plugin's `SourceState.horizon`.
    pub fn horizon(&self) -> Option<DateTime<Utc>> {
        self.reach.horizon()
    }
}

/// `watchdog_exclude_source_ids`, comma-separated. Anything that is not a number is ignored, as
/// in the plugin: a typo excludes nothing, rather than everything.
pub fn parse_exclude_ids(raw: &str) -> BTreeSet<i64> {
    raw.split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() || !part.chars().all(|c| c.is_ascii_digit()) {
                return None;
            }
            part.parse::<i64>().ok()
        })
        .collect()
}

/// `is_active`, non-dummy and has-mapped-channel are enforced when the candidates are collected
/// (the plugin's ORM query); the only decision left here is the user's exclude list.
pub fn is_candidate(source: &SourceState, exclude: &BTreeSet<i64>) -> bool {
    !exclude.contains(&source.id)
}

/// Whether a source needs a refresh now.
///
/// A source whose guide carries nothing at all is NOT stale unless its last refresh errored —
/// exactly the plugin's rule, kept because refreshing cannot conjure a guide that the provider
/// does not have, and a source that has never had one would otherwise be refreshed for ever.
pub fn is_stale(source: &SourceState, now: DateTime<Utc>, threshold_hours: u32) -> bool {
    if source.status == "error" {
        return true;
    }
    match source.horizon() {
        Some(horizon) => horizon < now + chrono::Duration::hours(i64::from(threshold_hours)),
        None => false,
    }
}

/// How a refreshed source came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Recovered,
    StillBroken,
}

impl Outcome {
    /// The word the report and the run row carry (the plugin's own).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Recovered => "recovered",
            Self::StillBroken => "still_broken",
        }
    }
}

/// Judge by RELATIVE improvement, never against the arming threshold.
pub fn classify_outcome(before: &SourceState, after: &SourceState) -> Outcome {
    if after.status == "error" {
        return Outcome::StillBroken;
    }
    if before.status == "error" {
        return Outcome::Recovered;
    }
    match (before.horizon(), after.horizon()) {
        // Was empty, now populated.
        (None, Some(_)) => Outcome::Recovered,
        // The horizon moved forward.
        (Some(before_horizon), Some(after_horizon)) if after_horizon > before_horizon => {
            Outcome::Recovered
        }
        // Unchanged, moved backwards, or gone: no improvement.
        _ => Outcome::StillBroken,
    }
}

/// The watchdog settings, coerced: floors applied, the exclude list parsed once.
///
/// The plugin's `coerce_settings` merged `DEFAULTS` over a settings dict and floored two
/// integers. `guide.toml` is typed (`deny_unknown_fields`, no wrong-type values survive the
/// load), so the defaults live on [`GuideSettings`] and only the floors are left to enforce —
/// which still matters, because `watchdog_horizon_threshold_hours = 0` parses fine and would
/// make every guide stale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchdogSettings {
    /// A guide ending inside this many hours counts as stale. Floored at 1.
    pub horizon_threshold_hours: u32,
    /// Comma-separated EPG source ids the watchdog must never touch.
    pub exclude_source_ids: String,
    /// Log a recovery. A failure is always logged.
    pub log_on_recovery: bool,
    /// How long to wait for a queued refresh to move the source. `0` requests it and does not
    /// wait.
    pub refresh_timeout: Duration,
}

impl Default for WatchdogSettings {
    fn default() -> Self {
        Self {
            // The plugin's shipped defaults (plugin.json), minus the two the schedule replaces.
            horizon_threshold_hours: 12,
            exclude_source_ids: String::new(),
            log_on_recovery: true,
            // Crosstalk-only: the plugin's refresh ran inline, so it had nothing to wait for.
            refresh_timeout: Duration::from_secs(10 * 60),
        }
    }
}

impl WatchdogSettings {
    /// This app's settings, coerced. Non-watchdog settings pass through untouched (the form's
    /// own readers handle them).
    pub fn coerce(settings: &GuideSettings) -> Self {
        Self {
            // The plugin's `_INT_FLOORS`: a threshold of 0 would make every guide stale.
            horizon_threshold_hours: settings.watchdog_horizon_threshold_hours.max(1),
            exclude_source_ids: settings.watchdog_exclude_source_ids.clone(),
            log_on_recovery: settings.watchdog_log_on_recovery,
            refresh_timeout: Duration::from_secs(
                60 * u64::from(settings.watchdog_refresh_timeout_minutes),
            ),
        }
    }

    /// The exclude list, parsed.
    pub fn exclude(&self) -> BTreeSet<i64> {
        parse_exclude_ids(&self.exclude_source_ids)
    }
}

// ---------------------------------------------------------------------------------------
// The orchestration
// ---------------------------------------------------------------------------------------

/// Whether the check refreshes, or only says what it would refresh.
///
/// Crosstalk-only: the plugin had one button that refreshed. The split is the naming rule every
/// sub-app follows — an action that writes is `<verb>_preview` / `<verb>_apply` — and it makes
/// the audit readable without queueing a single refresh.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Preview,
    Apply,
}

/// What waiting for one refresh saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refresh {
    /// The source moved: its status or its last successful refresh changed.
    Settled,
    /// The wait lapsed with nothing changed; the Celery task may still be running.
    Timeout,
}

/// What the check did to one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Not stale: nothing was done.
    None,
    /// Preview only: a refresh would have been requested.
    WouldRefresh,
    /// A refresh was requested and waited for.
    Refreshed(Refresh),
    /// A refresh was requested, and the run was cancelled before it settled. The queue entry
    /// stays on Dispatcharr, so the row records it rather than pretending nothing happened —
    /// this is the "record rows for refreshes already queued" rule from the phase-1 review.
    Queued,
    /// The refresh request itself failed. Classification is skipped, as in the plugin.
    RefreshFailed(String),
}

impl Action {
    fn describe(&self) -> String {
        match self {
            Self::None => "no action".to_string(),
            Self::WouldRefresh => "would refresh".to_string(),
            Self::Refreshed(Refresh::Settled) => "refreshed".to_string(),
            Self::Refreshed(Refresh::Timeout) => {
                "refresh requested, did not settle within the timeout".to_string()
            }
            Self::Queued => {
                "refresh queued, then the run was cancelled before it settled".to_string()
            }
            Self::RefreshFailed(error) => format!("refresh failed: {error}"),
        }
    }
}

/// One source, as the report and the run row record it.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub source_id: i64,
    pub name: String,
    pub status_before: String,
    pub status_after: Option<String>,
    pub horizon_before: Reach,
    pub horizon_after: Option<Reach>,
    pub stale: bool,
    pub action: Action,
    pub outcome: Option<Outcome>,
}

impl Row {
    /// The single word the CSV "outcome" column and the run row's status carry.
    pub fn status(&self) -> &'static str {
        match (&self.outcome, &self.action) {
            (Some(outcome), _) => outcome.as_str(),
            (None, Action::RefreshFailed(_)) => "refresh_failed",
            (None, Action::WouldRefresh) => "would_refresh",
            (None, Action::Queued) => "queued",
            (None, _) => "fresh",
        }
    }

    /// The story behind the word, in one line: what it looked like, what changed, what was done.
    pub fn reason(&self) -> String {
        format!(
            "{}; status {} -> {}; horizon {} -> {}; {}",
            if self.stale { "stale" } else { "not stale" },
            self.status_before,
            self.status_after.as_deref().unwrap_or("not re-read"),
            self.horizon_before.describe(),
            self.horizon_after
                .map(Reach::describe)
                .unwrap_or_else(|| "not re-read".to_string()),
            self.action.describe(),
        )
    }
}

/// What one check produced.
///
/// The four counters are the plugin's summary; `failed`, `would_refresh` and `rows` are the
/// additions crosstalk needs to write a report (the plugin only logged the first and had no
/// preview).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Summary {
    /// Candidate sources audited, excluded ones not included.
    pub checked: usize,
    /// Sources whose refresh was requested (and, in apply mode, waited for).
    pub refreshed: Vec<i64>,
    pub recovered: Vec<i64>,
    pub still_broken: Vec<i64>,
    /// Sources whose refresh request failed.
    pub failed: Vec<i64>,
    /// Preview only: the sources that would have been refreshed.
    pub would_refresh: Vec<i64>,
    /// One row per candidate, in the order they were audited.
    pub rows: Vec<Row>,
    /// Whether a cancel stopped the audit. The rows above are still written: a refresh that was
    /// already queued on Dispatcharr happened, and a run page that omitted it would hide it.
    pub cancelled: bool,
}

/// The I/O one check performs, injected so the loop is unit-testable without a server.
#[async_trait::async_trait]
pub trait WatchdogIo: Send + Sync {
    /// The candidate sources — active, non-dummy, mapped to at least one channel, minus the
    /// excludes — each with the freshness of its guide already read against `cutoff`.
    async fn candidates(
        &self,
        cutoff: DateTime<Utc>,
        exclude: &BTreeSet<i64>,
    ) -> Result<Vec<SourceState>>;

    /// Request a refresh and wait for it to settle, or for the timeout.
    async fn refresh(&self, source_id: i64) -> Result<Refresh>;

    /// Re-read a source after its refresh: `None` when it is no longer there.
    async fn reread(&self, source_id: i64) -> Result<Option<SourceState>>;

    /// Record what happened, at the severity the outcome deserves.
    fn log(&self, outcome: Outcome, before: &SourceState, after: &SourceState);

    /// Whether the operator asked the run to stop.
    fn cancelled(&self) -> bool {
        false
    }
}

/// Pure orchestration: audit → refresh the stale ones → re-read → classify → log.
///
/// All I/O is injected so the loop is unit-testable. Stateless: every run re-reads every source.
/// A refresh that raises is logged and NOT counted as refreshed, and its classification is
/// skipped — the plugin's rule, kept because an attempt that failed tells us nothing about the
/// guide.
pub async fn run_check<IO: WatchdogIo + ?Sized>(
    io: &IO,
    settings: &WatchdogSettings,
    now: DateTime<Utc>,
    mode: Mode,
) -> Result<Summary> {
    let exclude = settings.exclude();
    let cutoff = now + chrono::Duration::hours(i64::from(settings.horizon_threshold_hours));
    let mut summary = Summary::default();

    let candidates = io.candidates(cutoff, &exclude).await?;
    // A cancel during THAT read: the audit has nothing to add, and the caller still needs to
    // know the run was cancelled rather than that nothing was mapped.
    if io.cancelled() {
        summary.cancelled = true;
    }
    for before in candidates {
        if summary.cancelled {
            break;
        }
        if io.cancelled() {
            // Not a bail: the rows already decided are the audit this run produced, and the
            // caller records them before saying the run was cancelled.
            summary.cancelled = true;
            break;
        }
        summary.checked += 1;

        if !is_stale(&before, now, settings.horizon_threshold_hours) {
            summary.rows.push(Row {
                source_id: before.id,
                name: before.name.clone(),
                status_before: before.status.clone(),
                status_after: None,
                horizon_before: before.reach,
                horizon_after: None,
                stale: false,
                action: Action::None,
                outcome: None,
            });
            continue;
        }

        if mode == Mode::Preview {
            summary.would_refresh.push(before.id);
            summary.rows.push(Row {
                source_id: before.id,
                name: before.name.clone(),
                status_before: before.status.clone(),
                status_after: None,
                horizon_before: before.reach,
                horizon_after: None,
                stale: true,
                action: Action::WouldRefresh,
                outcome: None,
            });
            continue;
        }

        let refresh = match io.refresh(before.id).await {
            Ok(refresh) => refresh,
            Err(error) => {
                // A cancel is not a refresh failure. The POST above already reached Dispatcharr
                // (the settle wait is what observes the cancel), so the queue entry exists and
                // the row says so; then the run stops.
                if io.cancelled() {
                    summary.cancelled = true;
                    summary.rows.push(Row {
                        source_id: before.id,
                        name: before.name.clone(),
                        status_before: before.status.clone(),
                        status_after: None,
                        horizon_before: before.reach,
                        horizon_after: None,
                        stale: true,
                        action: Action::Queued,
                        outcome: None,
                    });
                    break;
                }
                tracing::error!("guide watchdog refresh({}) failed: {error:#}", before.id);
                summary.failed.push(before.id);
                summary.rows.push(Row {
                    source_id: before.id,
                    name: before.name.clone(),
                    status_before: before.status.clone(),
                    status_after: None,
                    horizon_before: before.reach,
                    horizon_after: None,
                    stale: true,
                    action: Action::RefreshFailed(format!("{error:#}")),
                    outcome: None,
                });
                continue;
            }
        };
        summary.refreshed.push(before.id);

        // The plugin's `reread_source(id) or before`: a source that cannot be re-read is
        // classified against what it looked like before the refresh, which can only read as
        // "no improvement".
        let after = match io.reread(before.id).await {
            Ok(Some(after)) => after,
            Ok(None) => {
                tracing::warn!(
                    "guide watchdog: EPG source {} disappeared during its refresh",
                    before.id
                );
                before.clone()
            }
            Err(error) => {
                tracing::warn!(
                    "guide watchdog: could not re-read EPG source {} after its refresh: {error:#}",
                    before.id
                );
                before.clone()
            }
        };

        let outcome = classify_outcome(&before, &after);
        match outcome {
            Outcome::Recovered => {
                summary.recovered.push(before.id);
                if settings.log_on_recovery {
                    io.log(outcome, &before, &after);
                }
            }
            Outcome::StillBroken => {
                summary.still_broken.push(before.id);
                io.log(outcome, &before, &after);
            }
        }
        summary.rows.push(Row {
            source_id: before.id,
            name: before.name.clone(),
            status_before: before.status.clone(),
            status_after: Some(after.status.clone()),
            horizon_before: before.reach,
            horizon_after: Some(after.reach),
            stale: true,
            action: Action::Refreshed(refresh),
            outcome: Some(outcome),
        });
    }

    Ok(summary)
}

// ---------------------------------------------------------------------------------------
// The glue
// ---------------------------------------------------------------------------------------

/// The sources at least one channel is mapped to.
///
/// The plugin's `_wd_mapped_source_ids` is one ORM query —
/// `Channel.objects.filter(epg_data__isnull=False).values_list("epg_data__epg_source_id")` — and
/// this is the same chain read from the world the run already loaded: channel → its own EPG
/// entry → that entry's source. `EpgSource::has_channels` annotates the same fact server-side,
/// but it is a serializer field whose build-to-build semantics nothing here pins down, while the
/// chain above is exactly the ORM query's (it includes channels hidden from output, because the
/// client lists those, and it drops an entry with no source, as the query's join does). It also
/// costs no request: channels and entries are both in [`WorldKinds::DEFAULT`].
///
/// [`WorldKinds::DEFAULT`]: crosstalk_core::dispatcharr::WorldKinds::DEFAULT
pub fn mapped_source_ids(world: &World) -> BTreeSet<i64> {
    let entries: HashMap<i64, &EpgEntry> = world
        .entries
        .iter()
        .map(|entry| (entry.id, entry))
        .collect();
    world
        .channels
        .iter()
        .filter_map(|channel| channel.current_epg_id())
        .filter_map(|id| entries.get(&id))
        .filter_map(|entry| entry.epg_source)
        .collect()
}

/// How many channels are mapped to `sources` — the channels whose guide this run audited.
pub fn mapped_channels(world: &World, sources: &BTreeSet<i64>) -> usize {
    let entries: HashMap<i64, &EpgEntry> = world
        .entries
        .iter()
        .map(|entry| (entry.id, entry))
        .collect();
    world
        .channels
        .iter()
        .filter_map(|channel| channel.current_epg_id())
        .filter_map(|id| entries.get(&id))
        .filter_map(|entry| entry.epg_source)
        .filter(|source| sources.contains(source))
        .count()
}

/// One run's session against Dispatcharr: everything the injected I/O needs.
struct Session<'a> {
    client: &'a DispatcharrClient,
    context: &'a JobContext,
    world: &'a World,
    settings: &'a WatchdogSettings,
    /// One instant for the whole run: every cutoff and every bracket is relative to it, so the
    /// before and after reads of a source answer the same question.
    now: DateTime<Utc>,
}

impl Session<'_> {
    /// How far one source's guide reaches, read as cheaply as the question allows.
    ///
    /// Two questions, never three: "does programme data reach the cutoff?" — one request, and
    /// the whole answer for a healthy source — and, only when it does not, "how far does it
    /// reach?" over `[1970, cutoff)`. That one answers both halves: nothing ending at or after
    /// 1970 in one request is the plugin's `Max(end_time) IS NULL`, and anything else is
    /// bisected to the minute (~25 bisection steps plus the bracket's own two probes, and only
    /// for a source about to be refreshed).
    async fn read_reach(&self, source_id: i64, cutoff: DateTime<Utc>) -> Result<Reach> {
        if self.client.epg_ends_at_or_after(source_id, cutoff).await? {
            return Ok(Reach::AtLeast(cutoff));
        }
        match self
            .client
            .epg_horizon_between(source_id, DateTime::<Utc>::UNIX_EPOCH, cutoff)
            .await?
        {
            EpgHorizon::At(instant) => Ok(Reach::At(instant)),
            EpgHorizon::Beyond(instant) => Ok(Reach::AtLeast(instant)),
            EpgHorizon::Empty => Ok(Reach::Empty),
        }
    }

    /// The state a refresh moves, as the listing read it: the baseline the wait compares
    /// against.
    fn before_state(&self, source_id: i64) -> Result<EpgSourceState> {
        let source = self
            .world
            .sources
            .iter()
            .find(|source| source.id == source_id)
            .with_context(|| {
                format!("EPG source {source_id} is not in the listing this run read")
            })?;
        Ok(EpgSourceState {
            id: source_id,
            status: source.status.clone(),
            updated_at: source.updated_at,
            last_message: source.last_message.clone(),
        })
    }
}

#[async_trait::async_trait]
impl WatchdogIo for Session<'_> {
    async fn candidates(
        &self,
        cutoff: DateTime<Utc>,
        exclude: &BTreeSet<i64>,
    ) -> Result<Vec<SourceState>> {
        // The plugin's queryset: `EPGSource.objects.filter(is_active=True).exclude(
        // source_type="dummy")` intersected with the mapped-source set, then the user's exclude
        // list. The exclude is applied before any request, so an excluded source costs nothing.
        let mapped = mapped_source_ids(self.world);
        let mut chosen = Vec::new();
        for source in &self.world.sources {
            if !source.is_active || source.is_dummy() || !mapped.contains(&source.id) {
                continue;
            }
            let state = SourceState::new(
                source.id,
                source.name.clone(),
                source.status.clone(),
                Reach::Unknown,
            );
            if is_candidate(&state, exclude) {
                chosen.push(state);
            }
        }

        let total = chosen.len() as u64;
        let mut states = Vec::with_capacity(chosen.len());
        for (index, mut state) in chosen.into_iter().enumerate() {
            if self.context.is_cancelled() {
                // Hand back what has been read so far; `run_check` sees the flag, marks the run
                // cancelled and stops. Bailing here would throw away the audit of every source
                // already checked.
                break;
            }
            self.context.progress(&ProgressEvent::new(
                PROGRESS_ACTION,
                index as u64,
                total,
                "checking EPG freshness",
            ));
            state.reach = self.read_reach(state.id, cutoff).await?;
            states.push(state);
        }
        Ok(states)
    }

    async fn refresh(&self, source_id: i64) -> Result<Refresh> {
        // No skip when the source is already `fetching`/`parsing`: the plugin's inline call
        // had none either, and a second `POST /epg/import/` is harmless — the task takes a
        // Redis lock (`acquire_task_lock('refresh_epg_data', id)`) and a duplicate returns
        // "already running" without doing anything. The settle wait below then follows
        // whichever attempt is actually running to its terminal status.
        let before = self.before_state(source_id)?;
        // A fetch and parse takes minutes, so the wait is visible in the UI rather than looking
        // like a run that hung. One at a time, so there is no percentage to publish.
        self.context.progress(&ProgressEvent::new(
            PROGRESS_ACTION,
            0,
            0,
            format!("waiting for EPG source {source_id} to refresh"),
        ));
        self.client.refresh_epg_source(source_id).await?;
        let context = self.context;
        let refresh = self
            .client
            .await_epg_refresh(source_id, &before, self.settings.refresh_timeout, || {
                context.is_cancelled()
            })
            .await?;
        Ok(match refresh {
            crosstalk_core::dispatcharr::EpgRefresh::Changed(_) => Refresh::Settled,
            crosstalk_core::dispatcharr::EpgRefresh::TimedOut(_) => Refresh::Timeout,
        })
    }

    async fn reread(&self, source_id: i64) -> Result<Option<SourceState>> {
        let state = match self.client.epg_source_state(source_id).await {
            Ok(state) => state,
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        let name = self
            .world
            .sources
            .iter()
            .find(|source| source.id == source_id)
            .map(|source| source.name.clone())
            .unwrap_or_default();
        // An errored source's status already decides the outcome, and one that is still being
        // rewritten has no settled horizon to read: both are recorded unread rather than
        // measured mid-flight.
        let reach = if matches!(state.status.as_str(), "error" | "fetching" | "parsing") {
            Reach::Unknown
        } else {
            let cutoff = self.now
                + chrono::Duration::hours(i64::from(self.settings.horizon_threshold_hours));
            self.read_reach(source_id, cutoff).await?
        };
        Ok(Some(SourceState {
            id: source_id,
            name,
            status: state.status,
            reach,
        }))
    }

    fn log(&self, outcome: Outcome, before: &SourceState, after: &SourceState) {
        // The plugin wrote a Dispatcharr System Event here and mirrored it to the log; crosstalk
        // has no System Events, so the log line and the run row are the record.
        let before_horizon = before.reach.describe();
        let after_horizon = after.reach.describe();
        match outcome {
            Outcome::Recovered => tracing::info!(
                "guide watchdog recovered: {} ({}) horizon {before_horizon} -> {after_horizon}",
                after.name,
                after.status
            ),
            Outcome::StillBroken => tracing::warn!(
                "guide watchdog still broken: {} ({}) horizon {before_horizon} -> {after_horizon}",
                after.name,
                after.status
            ),
        }
    }

    fn cancelled(&self) -> bool {
        self.context.is_cancelled()
    }
}

/// Whether a client error is Dispatcharr's 404: the source went away between the refresh and
/// the re-read. The client keeps no typed error, so this reads its message — the status text,
/// not a bare number, so a source whose id merely contains 404 is not mistaken for gone.
fn is_not_found(error: &anyhow::Error) -> bool {
    format!("{error:#}").contains("404 Not Found")
}

/// An instant as every report in this repo prints it: second precision, UTC.
fn iso(instant: DateTime<Utc>) -> String {
    instant.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

// ---------------------------------------------------------------------------------------
// The action
// ---------------------------------------------------------------------------------------

/// Preview (audit only) or apply (audit and refresh).
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &GuideSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    let client = context.dispatcharr()?;
    let watchdog = WatchdogSettings::coerce(settings);
    let now = Utc::now();
    // A non-administrator key only sees programmes on guide data a channel it can reach is
    // mapped to (the search endpoint scopes rows by channel access), so a source may read
    // short or empty for it — and a refresh POST would answer 403. Warn rather than audit
    // silently: the run below would otherwise report those sources as fresh, or refresh
    // nothing while claiming it checked everything.
    let non_admin = match client.user_level().await {
        Ok(level) => level < 10,
        Err(error) => {
            tracing::debug!("guide watchdog: could not read the API key's user level: {error:#}");
            false
        }
    };
    if non_admin {
        tracing::warn!("guide watchdog: the Dispatcharr API key is not an administrator's; programme visibility follows channel access, so a source may read short or empty. Use an administrator key for the watchdog.");
    }
    let world = client.load_world().await?;

    let session = Session {
        client,
        context,
        world: &world,
        settings: &watchdog,
        now,
    };
    let mode = if dry_run { Mode::Preview } else { Mode::Apply };
    let summary = run_check(&session, &watchdog, now, mode).await?;

    let audited: BTreeSet<i64> = summary.rows.iter().map(|row| row.source_id).collect();
    let report = write_report(app, context, settings, &summary, &world, &audited, dry_run)?;
    context.record_rows(&run_rows(&summary))?;

    if let Some(path) = report.as_deref() {
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string());
        csv_report::prune_exports(
            &context.exports_dir(),
            &app.export_prefix(),
            context.config().exports.retention_days,
            name.as_deref(),
        );
    }

    let mut built = outcome(&summary, dry_run, &watchdog);
    if non_admin {
        built = built.note(
            "the Dispatcharr API key is not an administrator's: programme visibility follows \
             channel access, so a source may read short or empty. Use an administrator key for \
             the watchdog",
        );
    }
    Ok(built.report(report).build())
}

fn write_report(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &GuideSettings,
    summary: &Summary,
    world: &World,
    audited: &BTreeSet<i64>,
    dry_run: bool,
) -> Result<Option<PathBuf>> {
    // A source row is not a channel row, but the preamble line is the same question: how much
    // of the install did this run look at? The channels mapped to the audited sources is the
    // honest count.
    let channels = mapped_channels(world, audited);
    let exports_dir = context.exports_dir();
    let mut report = csv_report::Report::new(
        &exports_dir,
        app,
        if dry_run {
            "watchdog_preview"
        } else {
            "watchdog_applied"
        },
        if dry_run {
            "EPG Freshness Watchdog Preview Report"
        } else {
            "EPG Freshness Watchdog Applied Report"
        },
    )
    .channels_processed(channels)
    .result("Sources checked", summary.checked)
    .result("Channels mapped to those sources", channels)
    .result(
        "Stale sources",
        summary.rows.iter().filter(|row| row.stale).count(),
    );
    if dry_run {
        report = report.result("Would refresh", summary.would_refresh.len());
    } else {
        report = report
            .result("Refreshed", summary.refreshed.len())
            .result("Recovered", summary.recovered.len())
            .result("Still broken", summary.still_broken.len())
            .result("Refresh failed", summary.failed.len());
    }
    let timeout_minutes = WatchdogSettings::coerce(settings).refresh_timeout.as_secs() / 60;
    report = report
        .result(
            "Refresh wait (per source)",
            format!("{timeout_minutes} minutes"),
        )
        .result(
            "Schedule",
            "not a setting: arm guide.watchdog_apply in [schedules], e.g. \"0 */6 * * *\"",
        );

    let path = report.write(
        context.config(),
        &crate::settings::schema(),
        &crate::settings::to_values(settings),
        &COLUMNS,
        |writer| {
            // Source names come from Dispatcharr: defuse spreadsheet formulas.
            for row in &summary.rows {
                writer.write_record([
                    row.source_id.to_string(),
                    csv_report::safe_cell(&row.name),
                    csv_report::safe_cell(&row.status_before),
                    row.status_after
                        .as_deref()
                        .map(csv_report::safe_cell)
                        .unwrap_or_default(),
                    csv_report::safe_cell(&row.horizon_before.describe()),
                    row.horizon_after
                        .map(|reach| csv_report::safe_cell(&reach.describe()))
                        .unwrap_or_default(),
                    if row.stale { "yes" } else { "no" }.to_string(),
                    csv_report::safe_cell(&row.action.describe()),
                    row.status().to_string(),
                ])?;
            }
            Ok(())
        },
    )?;
    Ok(Some(path))
}

/// One run row per candidate source.
///
/// A source is not a channel, so the row says so: `subject` is EPG source and the identity
/// columns carry the source's id and name. `field` names what was observed (`epg_source`),
/// the before/after values are its refresh status, and `reason` carries the horizons and the
/// action, so the run page tells the whole story.
pub fn run_rows(summary: &Summary) -> Vec<RunRow> {
    summary
        .rows
        .iter()
        .enumerate()
        .map(|(ordinal, row)| RunRow {
            ordinal: ordinal as i64,
            subject: Subject::EpgSource,
            channel_id: row.source_id,
            channel_name: row.name.clone(),
            channel_group: String::new(),
            channel_number: None,
            method: "watchdog".to_string(),
            field: "epg_source".to_string(),
            status: row.status().to_string(),
            reason: row.reason(),
            before_value: Some(row.status_before.clone()),
            after_value: row.status_after.clone(),
            epg_source: Some(row.name.clone()),
            ..RunRow::default()
        })
        .collect()
}

/// The outcome the runner records, in the plugin's own wording.
pub fn outcome(summary: &Summary, dry_run: bool, settings: &WatchdogSettings) -> OutcomeBuilder {
    let mut builder = if dry_run {
        OutcomeBuilder::new(format!(
            "EPG freshness watchdog preview: checked {} source(s); {} stale, nothing refreshed",
            summary.checked,
            summary.would_refresh.len()
        ))
        .count("checked", summary.checked as i64)
        .count("stale", summary.would_refresh.len() as i64)
        .count("would_refresh", summary.would_refresh.len() as i64)
    } else {
        OutcomeBuilder::new(format!(
            "EPG freshness watchdog applied: checked {} source(s); refreshed {} (recovered {}, \
             still broken {})",
            summary.checked,
            summary.refreshed.len(),
            summary.recovered.len(),
            summary.still_broken.len()
        ))
        .count("checked", summary.checked as i64)
        .count("refreshed", summary.refreshed.len() as i64)
        .count("recovered", summary.recovered.len() as i64)
        .count("still_broken", summary.still_broken.len() as i64)
        .count("failed", summary.failed.len() as i64)
    };
    if summary.checked == 0 && !summary.cancelled {
        // Not an error, but it is worth saying out loud: nothing is being watched. Either every
        // source is inactive or a dummy one, nothing is mapped to one, or the exclude list
        // covers them all. A cancelled run has its own note instead.
        builder = builder.note(
            "no active, non-dummy EPG source has channels mapped to it (or they are all \
             excluded), so nothing was audited",
        );
    }
    if !summary.failed.is_empty() {
        builder = builder.note(format!(
            "{} refresh request(s) failed and were not classified",
            summary.failed.len()
        ));
    }
    if summary.cancelled {
        // The cancel rule (see `crosstalk_core::jobs`): the audit and the queued refreshes are
        // recorded, and the run says it was cancelled instead of reporting a whole sweep.
        builder = builder.note(format!(
            "cancelled by the operator after auditing {} source(s); refreshes already queued \
             on Dispatcharr are recorded here and keep running there",
            summary.checked
        ));
        builder = builder.cancelled();
    }
    let default = WatchdogSettings::default().horizon_threshold_hours;
    if settings.horizon_threshold_hours != default {
        builder = builder.note(format!(
            "guide ending within {}h counts as stale",
            settings.horizon_threshold_hours
        ));
    }
    builder
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::bail;
    use chrono::{Duration as ChronoDuration, TimeZone};
    use std::sync::Mutex;

    /// The plugin's `NOW`: a fixed instant, so every boundary case is exact.
    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 17, 11, 0, 0).unwrap()
    }

    fn src(id: i64, status: &str, reach: Reach) -> SourceState {
        SourceState::new(id, "epgshare-UK", status, reach)
    }

    /// The plugin's `horizon=...` / `horizon=None` in [`Reach`] terms.
    fn at(hours: i64) -> Reach {
        Reach::At(now() + ChronoDuration::hours(hours))
    }

    // ---- candidate / stale / classify ------------------------------------

    #[test]
    fn parse_exclude_ids_ignores_everything_that_is_not_an_id() {
        assert_eq!(
            parse_exclude_ids("22, 40 ,x, 7"),
            BTreeSet::from([22, 40, 7])
        );
        assert!(parse_exclude_ids("").is_empty());
        assert!(parse_exclude_ids("   ").is_empty());
        assert!(parse_exclude_ids("-3, 4.5, ,").is_empty());
    }

    #[test]
    fn the_candidate_gate_is_the_exclude_list_only() {
        assert!(is_candidate(
            &src(22, "success", Reach::Unknown),
            &BTreeSet::new()
        ));
        assert!(!is_candidate(
            &src(22, "success", Reach::Unknown),
            &BTreeSet::from([22])
        ));
        assert!(is_candidate(
            &src(7, "success", Reach::Unknown),
            &BTreeSet::from([22, 40])
        ));
    }

    #[test]
    fn a_source_whose_refresh_errored_is_stale_whatever_its_horizon() {
        assert!(is_stale(
            &src(22, "error", Reach::At(now() + ChronoDuration::days(2))),
            now(),
            12
        ));
    }

    #[test]
    fn staleness_follows_the_horizon_and_its_strict_boundary() {
        assert!(is_stale(&src(22, "success", at(6)), now(), 12));
        assert!(!is_stale(&src(22, "success", at(48)), now(), 12));
        // Exact boundary: the strict `<` means horizon == now + threshold is NOT stale.
        assert!(!is_stale(&src(22, "success", at(12)), now(), 12));
    }

    #[test]
    fn a_guide_that_is_not_there_is_judged_by_status_alone() {
        // The plugin's `horizon=None`: refreshing cannot conjure a guide, so a source with no
        // programme is only stale when its refresh errored.
        assert!(!is_stale(&src(22, "success", Reach::Empty), now(), 12));
        assert!(is_stale(&src(22, "error", Reach::Empty), now(), 12));
    }

    #[test]
    fn a_lower_bound_horizon_is_compared_as_a_lower_bound() {
        // `AtLeast(cutoff)` is what a probe that found guide at the cutoff proves: not stale,
        // because the real horizon is at or after it.
        let cutoff = now() + ChronoDuration::hours(12);
        assert!(!is_stale(
            &src(22, "success", Reach::AtLeast(cutoff)),
            now(),
            12
        ));
        // A lower bound below the cutoff could still be short, so it reads short — the glue
        // never produces that combination, and the conservative reading is the safe one.
        assert!(is_stale(
            &src(22, "success", Reach::AtLeast(now())),
            now(),
            12
        ));
    }

    #[test]
    fn outcome_is_relative_never_against_the_threshold() {
        let good = src(22, "success", Reach::At(now() + ChronoDuration::days(2)));
        // error -> success is recovered even with a still-short horizon.
        assert_eq!(
            classify_outcome(&src(22, "error", at(1)), &src(22, "success", at(1))),
            Outcome::Recovered
        );
        // Horizon advanced = recovered.
        assert_eq!(
            classify_outcome(&src(22, "success", at(3)), &good),
            Outcome::Recovered
        );
        // Still error = still broken.
        assert_eq!(
            classify_outcome(
                &src(22, "error", Reach::Unknown),
                &src(22, "error", Reach::At(now() + ChronoDuration::days(2)))
            ),
            Outcome::StillBroken
        );
        // The refresh did not help (same short horizon, was success) = still broken.
        assert_eq!(
            classify_outcome(&src(22, "success", at(3)), &src(22, "success", at(3))),
            Outcome::StillBroken
        );
        // Was empty, now populated = recovered.
        assert_eq!(
            classify_outcome(
                &src(22, "success", Reach::Empty),
                &src(22, "success", Reach::At(now() + ChronoDuration::days(1)))
            ),
            Outcome::Recovered
        );
        // success -> error regression = still broken.
        assert_eq!(
            classify_outcome(&good, &src(22, "error", Reach::Unknown)),
            Outcome::StillBroken
        );
    }

    #[test]
    fn a_horizon_that_moved_backwards_is_not_a_recovery() {
        assert_eq!(
            classify_outcome(&src(22, "success", at(6)), &src(22, "success", at(2))),
            Outcome::StillBroken
        );
    }

    // ---- coerce_settings --------------------------------------------------

    #[test]
    fn the_coerced_defaults_are_the_plugins() {
        let coerced = WatchdogSettings::coerce(&GuideSettings::default());
        assert_eq!(coerced.horizon_threshold_hours, 12);
        assert_eq!(coerced.exclude_source_ids, "");
        assert!(coerced.log_on_recovery);
        assert_eq!(coerced.refresh_timeout, Duration::from_secs(600));
        // The two settings the schedule replaced are gone on purpose.
        let schema = crate::settings::schema();
        assert!(schema.field("watchdog_enabled").is_none());
        assert!(schema.field("watchdog_check_interval_hours").is_none());
    }

    #[test]
    fn a_hand_edited_zero_threshold_is_floored_not_honoured() {
        // The plugin's `_INT_FLOORS`: 0 would make every guide stale, so it becomes 1.
        let settings = GuideSettings {
            watchdog_horizon_threshold_hours: 0,
            ..GuideSettings::default()
        };
        assert_eq!(
            WatchdogSettings::coerce(&settings).horizon_threshold_hours,
            1
        );
    }

    #[test]
    fn coercion_leaves_the_rest_of_the_settings_alone() {
        // The plugin's "non-watchdog keys pass through untouched": here the other guide
        // settings simply are not read, so nothing to do — but the exclude list is parsed
        // once and only ids survive.
        let settings = GuideSettings {
            watchdog_exclude_source_ids: "22, 40 ,x".to_string(),
            match_confidence_threshold: 70,
            ..GuideSettings::default()
        };
        let coerced = WatchdogSettings::coerce(&settings);
        assert_eq!(coerced.exclude(), BTreeSet::from([22, 40]));
    }

    // ---- run_check, over injected fakes ----------------------------------
    //
    // The plugin's `_run` helper: a fake world that records the refreshes and the logged
    // events, so the loop is tested without a server.

    #[derive(Default)]
    struct Fake {
        states: Vec<SourceState>,
        /// What a re-read returns, by source id; a missing one means "same as before".
        after: HashMap<i64, SourceState>,
        /// Sources whose refresh raises.
        fails: BTreeSet<i64>,
        refreshed: Mutex<Vec<i64>>,
        logs: Mutex<Vec<(Outcome, i64)>>,
        /// Set before the run, or by the refresh that observes a cancel mid-flight.
        cancelled: std::sync::atomic::AtomicBool,
        /// Sources whose refresh is "queued, then the operator cancelled while it settled":
        /// the trait's `refresh` then returns the cancel error, as `await_epg_refresh` does.
        cancel_in_refresh: BTreeSet<i64>,
    }

    impl Fake {
        fn new(states: Vec<SourceState>) -> Self {
            Self {
                states,
                ..Self::default()
            }
        }

        fn failing(mut self, ids: [i64; 1]) -> Self {
            self.fails.extend(ids);
            self
        }

        fn rereads(mut self, after: SourceState) -> Self {
            self.after.insert(after.id, after);
            self
        }

        fn cancelling_refresh(mut self, id: i64) -> Self {
            self.cancel_in_refresh.insert(id);
            self
        }

        fn refreshed(&self) -> Vec<i64> {
            self.refreshed.lock().unwrap().clone()
        }

        fn logs(&self) -> Vec<(Outcome, i64)> {
            self.logs.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl WatchdogIo for Fake {
        async fn candidates(
            &self,
            _cutoff: DateTime<Utc>,
            exclude: &BTreeSet<i64>,
        ) -> Result<Vec<SourceState>> {
            Ok(self
                .states
                .iter()
                .filter(|state| is_candidate(state, exclude))
                .cloned()
                .collect())
        }

        async fn refresh(&self, source_id: i64) -> Result<Refresh> {
            if self.cancel_in_refresh.contains(&source_id) {
                // The POST reached Dispatcharr before the cancel was observed, which is what
                // the real client does: queue, then wait, then notice.
                self.refreshed.lock().unwrap().push(source_id);
                self.cancelled
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                bail!("cancelled by the operator");
            }
            if self.fails.contains(&source_id) {
                bail!("connect failed");
            }
            self.refreshed.lock().unwrap().push(source_id);
            Ok(Refresh::Settled)
        }

        async fn reread(&self, source_id: i64) -> Result<Option<SourceState>> {
            Ok(Some(self.after.get(&source_id).cloned().unwrap_or_else(
                || {
                    self.states
                        .iter()
                        .find(|state| state.id == source_id)
                        .cloned()
                        .expect("a re-read is of a collected source")
                },
            )))
        }

        fn log(&self, outcome: Outcome, _before: &SourceState, after: &SourceState) {
            self.logs.lock().unwrap().push((outcome, after.id));
        }

        fn cancelled(&self) -> bool {
            self.cancelled.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    async fn run_fake(fake: &Fake, settings: &WatchdogSettings, mode: Mode) -> Result<Summary> {
        run_check(fake, settings, now(), mode).await
    }

    #[tokio::test]
    async fn a_stale_source_is_refreshed_reread_and_recovered() {
        let dry = src(22, "error", at(1));
        let good = src(22, "success", Reach::At(now() + ChronoDuration::days(2)));
        let fake = Fake::new(vec![dry]).rereads(good);

        let summary = run_fake(&fake, &WatchdogSettings::default(), Mode::Apply)
            .await
            .expect("check");

        assert_eq!(fake.refreshed(), vec![22]);
        assert_eq!(summary.refreshed, vec![22]);
        assert_eq!(summary.recovered, vec![22]);
        assert!(summary.still_broken.is_empty());
        assert_eq!(summary.rows.len(), 1);
        assert_eq!(summary.rows[0].status(), "recovered");
        assert_eq!(
            fake.logs(),
            vec![(Outcome::Recovered, 22)],
            "a recovery is logged by default"
        );
    }

    #[tokio::test]
    async fn a_healthy_source_is_counted_and_not_refreshed() {
        let healthy = src(22, "success", Reach::Empty);
        let fake = Fake::new(vec![healthy]);

        let summary = run_fake(&fake, &WatchdogSettings::default(), Mode::Apply)
            .await
            .expect("check");

        assert!(fake.refreshed().is_empty());
        assert_eq!(summary.checked, 1);
        assert!(summary.refreshed.is_empty());
        assert!(summary.recovered.is_empty());
        assert!(summary.still_broken.is_empty());
        assert_eq!(summary.rows[0].status(), "fresh");
        assert!(fake.logs().is_empty());
    }

    #[tokio::test]
    async fn a_refresh_that_raises_is_swallowed_and_not_counted() {
        let broken = src(22, "error", Reach::Unknown);
        let fake = Fake::new(vec![broken]).failing([22]);

        let summary = run_fake(&fake, &WatchdogSettings::default(), Mode::Apply)
            .await
            .expect("a failed refresh is not a failed check");

        assert!(summary.refreshed.is_empty());
        assert!(summary.still_broken.is_empty());
        assert_eq!(summary.failed, vec![22]);
        assert!(fake.logs().is_empty());
        assert_eq!(summary.rows[0].status(), "refresh_failed");
        assert!(summary.rows[0].reason().contains("connect failed"));
    }

    #[tokio::test]
    async fn a_source_that_stays_broken_is_logged() {
        let broken = src(22, "error", Reach::Unknown);
        let fake = Fake::new(vec![broken.clone()]).rereads(broken);

        let summary = run_fake(&fake, &WatchdogSettings::default(), Mode::Apply)
            .await
            .expect("check");

        assert_eq!(summary.refreshed, vec![22]);
        assert_eq!(summary.still_broken, vec![22]);
        assert_eq!(fake.logs(), vec![(Outcome::StillBroken, 22)]);
        assert_eq!(summary.rows[0].status(), "still_broken");
    }

    #[tokio::test]
    async fn an_excluded_source_is_never_touched() {
        let broken = src(22, "error", Reach::Unknown);
        let fake = Fake::new(vec![broken]);
        let settings = WatchdogSettings {
            exclude_source_ids: "22".to_string(),
            ..WatchdogSettings::default()
        };

        let summary = run_fake(&fake, &settings, Mode::Apply)
            .await
            .expect("check");

        assert!(fake.refreshed().is_empty());
        assert_eq!(summary.checked, 0);
        assert!(summary.rows.is_empty());
    }

    #[tokio::test]
    async fn a_recovery_is_not_logged_when_logging_is_off() {
        let dry = src(22, "error", Reach::Unknown);
        let good = src(22, "success", Reach::At(now() + ChronoDuration::days(2)));
        let fake = Fake::new(vec![dry]).rereads(good);
        let settings = WatchdogSettings {
            log_on_recovery: false,
            ..WatchdogSettings::default()
        };

        let summary = run_fake(&fake, &settings, Mode::Apply)
            .await
            .expect("check");

        assert_eq!(summary.recovered, vec![22]);
        assert!(fake.logs().is_empty());
    }

    #[tokio::test]
    async fn an_empty_candidate_list_is_a_no_op() {
        let fake = Fake::default();
        let summary = run_fake(&fake, &WatchdogSettings::default(), Mode::Apply)
            .await
            .expect("check");
        assert_eq!(
            summary,
            Summary {
                checked: 0,
                ..Summary::default()
            }
        );
    }

    #[tokio::test]
    async fn a_preview_audits_and_refreshes_nothing() {
        let dry = src(22, "error", Reach::Unknown);
        let healthy = src(7, "success", Reach::At(now() + ChronoDuration::days(3)));
        let fake = Fake::new(vec![dry, healthy]);

        let summary = run_fake(&fake, &WatchdogSettings::default(), Mode::Preview)
            .await
            .expect("preview");

        assert!(fake.refreshed().is_empty(), "a preview queues no refresh");
        assert_eq!(summary.checked, 2);
        assert_eq!(summary.would_refresh, vec![22]);
        assert!(summary.refreshed.is_empty());
        assert!(summary.recovered.is_empty());
        assert_eq!(summary.rows[0].status(), "would_refresh");
        assert_eq!(summary.rows[1].status(), "fresh");
    }

    #[tokio::test]
    async fn a_cancel_stops_the_check_before_the_next_source() {
        let broken = src(22, "error", Reach::Unknown);
        let fake = Fake {
            cancelled: std::sync::atomic::AtomicBool::new(true),
            ..Fake::new(vec![broken])
        };
        // Not an error any more: the summary says cancelled and the caller records it, so the
        // run page shows a cancelled run instead of a failed one.
        let summary = run_fake(&fake, &WatchdogSettings::default(), Mode::Apply)
            .await
            .expect("a cancelled check still reports");
        assert!(summary.cancelled);
        assert!(fake.refreshed().is_empty());
        assert!(summary.rows.is_empty());
    }

    #[tokio::test]
    async fn a_cancel_during_a_refresh_records_the_source_that_was_already_queued() {
        // The phase-1 review's exact finding: the watchdog bailed on cancel and lost the rows
        // for refreshes it had already queued on Dispatcharr. The queue entry keeps running
        // there, so the audit has to say so.
        let broken = src(22, "error", Reach::Unknown);
        let healthy = src(7, "success", Reach::At(now() + ChronoDuration::days(3)));
        let fake = Fake::new(vec![broken, healthy]).cancelling_refresh(22);

        let summary = run_fake(&fake, &WatchdogSettings::default(), Mode::Apply)
            .await
            .expect("a cancelled check still reports");
        assert_eq!(fake.refreshed(), vec![22], "the refresh was queued");
        assert!(summary.cancelled);
        assert_eq!(summary.checked, 1, "the audit stopped at the cancel");
        assert_eq!(summary.rows.len(), 1);
        assert_eq!(summary.rows[0].source_id, 22);
        assert_eq!(summary.rows[0].status(), "queued");
        assert!(summary.refreshed.is_empty(), "it never settled");
        assert!(
            !summary.rows.iter().any(|row| row.source_id == 7),
            "the next source is not audited"
        );
    }

    // ---- the glue against a mock Dispatcharr ------------------------------

    /// The glue, against a miniature Dispatcharr.
    ///
    /// The plugin's own watchdog tests inject fakes, because its glue is Django. Crosstalk's
    /// glue is HTTP, so these run the real [`Session`] against a mock that behaves like the API:
    /// a source's guide is one "latest programme end", `/epg/programs/search/` filters on
    /// `end_after` exactly as Dispatcharr does (which is what makes the bisection work), and
    /// `/epg/import/` moves the source the way a real refresh does.
    mod glue {
        use super::*;
        use crosstalk_core::config::CrosstalkConfig;
        use crosstalk_core::dispatcharr::{Channel, EpgEntry, World};
        use crosstalk_core::jobs::VecSink;
        use crosstalk_core::store::Store;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Mutex};
        use wiremock::matchers::{method, path, path_regex};
        use wiremock::{Mock, MockServer, Request, ResponseTemplate};

        /// What a refresh does to a simulated source.
        #[derive(Debug, Clone, Copy)]
        enum OnRefresh {
            /// A fetch that lands a guide ending at this instant: `status` becomes `success`
            /// and `updated_at` moves.
            Fetch(Option<DateTime<Utc>>),
            /// Like [`OnRefresh::Fetch`], but through `fetching` first: the first state read
            /// observes the refresh mid-flight and the second sees it settled, the way a real
            /// Celery task moves the source.
            SlowFetch(Option<DateTime<Utc>>),
            /// The request itself is refused with this status.
            Fail(u16),
            /// The Celery task never moves the source: a stuck or slow refresh.
            Nothing,
        }

        /// One source in the mock.
        #[derive(Debug, Clone)]
        struct SimSource {
            id: i64,
            name: String,
            status: String,
            updated_at: Option<String>,
            /// The latest programme end the horizon endpoint reports; `None` for no guide.
            end: Option<DateTime<Utc>>,
            is_active: bool,
            source_type: String,
            on_refresh: OnRefresh,
            /// A slow fetch in progress: settled to `success` with this guide on the second
            /// state read (the first observes it mid-flight).
            pending: Option<Option<DateTime<Utc>>>,
            /// State reads to stay `fetching` for before the pending guide lands.
            settle_polls: usize,
        }

        impl SimSource {
            fn new(id: i64, name: &str, status: &str, end: Option<DateTime<Utc>>) -> Self {
                Self {
                    id,
                    name: name.to_string(),
                    status: status.to_string(),
                    updated_at: Some("2026-01-01T00:00:00Z".to_string()),
                    end,
                    is_active: true,
                    source_type: "xmltv".to_string(),
                    on_refresh: OnRefresh::Fetch(end),
                    pending: None,
                    settle_polls: 0,
                }
            }

            fn dummy(mut self) -> Self {
                self.source_type = "dummy".to_string();
                self
            }

            fn inactive(mut self) -> Self {
                self.is_active = false;
                self
            }

            fn on(mut self, on_refresh: OnRefresh) -> Self {
                self.on_refresh = on_refresh;
                self
            }

            fn listing(&self) -> serde_json::Value {
                serde_json::json!({
                    "id": self.id,
                    "name": self.name,
                    "is_active": self.is_active,
                    "source_type": self.source_type,
                    "status": self.status,
                    "updated_at": self.updated_at,
                })
            }

            fn state(&self) -> serde_json::Value {
                serde_json::json!({
                    "id": self.id,
                    "status": self.status,
                    "updated_at": self.updated_at,
                    "last_message": null,
                })
            }
        }

        /// A mock Dispatcharr serving `world`'s channels and entries plus `sources`.
        struct Harness {
            server: MockServer,
            dir: tempfile::TempDir,
            config: CrosstalkConfig,
        }

        impl Harness {
            async fn start(world: &World, sources: Vec<SimSource>) -> Self {
                Self::start_as(world, sources, 10).await
            }

            /// `user_level` is what `/accounts/users/me/` reports: `10` is an administrator's
            /// key, anything less exercises the non-administrator warning.
            async fn start_as(world: &World, sources: Vec<SimSource>, user_level: i64) -> Self {
                let server = MockServer::start().await;
                let sources = Arc::new(Mutex::new(sources));

                Mock::given(method("GET"))
                    .and(path("/api/accounts/users/me/"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "id": 1,
                        "user_level": user_level,
                    })))
                    .mount(&server)
                    .await;

                let list =
                    |items: serde_json::Value| ResponseTemplate::new(200).set_body_json(items);
                Mock::given(method("GET"))
                    .and(path("/api/channels/channels/"))
                    .respond_with(list(
                        serde_json::to_value(&world.channels).expect("channels"),
                    ))
                    .mount(&server)
                    .await;
                Mock::given(method("GET"))
                    .and(path("/api/channels/groups/"))
                    .respond_with(list(serde_json::json!([])))
                    .mount(&server)
                    .await;
                Mock::given(method("GET"))
                    .and(path("/api/channels/profiles/"))
                    .respond_with(list(serde_json::json!([])))
                    .mount(&server)
                    .await;
                Mock::given(method("GET"))
                    .and(path("/api/epg/epgdata/"))
                    .respond_with(list(serde_json::to_value(&world.entries).expect("entries")))
                    .mount(&server)
                    .await;

                // The listing has to be mounted before the per-id route: same priority, so
                // the first mount wins and `/api/epg/sources/` is not read as a detail path.
                let listing = Arc::clone(&sources);
                Mock::given(method("GET"))
                    .and(path("/api/epg/sources/"))
                    .respond_with(move |_request: &Request| {
                        let rows: Vec<serde_json::Value> = listing
                            .lock()
                            .expect("sources")
                            .iter()
                            .map(SimSource::listing)
                            .collect();
                        ResponseTemplate::new(200).set_body_json(rows)
                    })
                    .mount(&server)
                    .await;

                let detail = Arc::clone(&sources);
                Mock::given(method("GET"))
                    .and(path_regex(r"^/api/epg/sources/\d+/$"))
                    .respond_with(move |request: &Request| {
                        let id = path_id(request.url.path());
                        let mut sources = detail.lock().expect("sources");
                        match sources.iter_mut().find(|source| source.id == id) {
                            Some(source) => {
                                // A slow fetch settles on a later read: the first observes it
                                // mid-flight, exactly what the settle wait is for.
                                if source.settle_polls > 0 {
                                    source.settle_polls -= 1;
                                } else if let Some(end) = source.pending.take() {
                                    source.status = "success".to_string();
                                    source.end = end;
                                    source.updated_at = Some(iso(Utc::now()));
                                }
                                ResponseTemplate::new(200).set_body_json(source.state())
                            }
                            None => ResponseTemplate::new(404),
                        }
                    })
                    .mount(&server)
                    .await;

                // The horizon probe: `end_after` is `end_time >= cutoff`, as upstream.
                let horizon = Arc::clone(&sources);
                Mock::given(method("GET"))
                    .and(path("/api/epg/programs/search/"))
                    .respond_with(move |request: &Request| {
                        let query: HashMap<String, String> =
                            request.url.query_pairs().into_owned().collect();
                        let id: i64 = query
                            .get("epg_source")
                            .and_then(|value| value.parse().ok())
                            .unwrap_or(-1);
                        let cutoff = query
                            .get("end_after")
                            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                            .map(|instant| instant.with_timezone(&Utc));
                        let sources = horizon.lock().expect("sources");
                        let end = sources
                            .iter()
                            .find(|source| source.id == id)
                            .and_then(|source| source.end);
                        match (end, cutoff) {
                            (Some(end), Some(cutoff)) if end >= cutoff => {
                                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                                    "count": 1,
                                    "results": [{"end_time": iso(end)}],
                                }))
                            }
                            _ => ResponseTemplate::new(200)
                                .set_body_json(serde_json::json!({"count": 0, "results": []})),
                        }
                    })
                    .mount(&server)
                    .await;

                let refresh = Arc::clone(&sources);
                Mock::given(method("POST"))
                    .and(path("/api/epg/import/"))
                    .respond_with(move |request: &Request| {
                        let body: serde_json::Value =
                            serde_json::from_slice(&request.body).unwrap_or_default();
                        let id = body["id"].as_i64().unwrap_or(-1);
                        let mut sources = refresh.lock().expect("sources");
                        let Some(source) = sources.iter_mut().find(|source| source.id == id) else {
                            return ResponseTemplate::new(404);
                        };
                        match source.on_refresh {
                            // 400, not 500: a 5xx is retried and would slow the test down for
                            // no extra coverage.
                            OnRefresh::Fail(status) => ResponseTemplate::new(status),
                            OnRefresh::Nothing => ResponseTemplate::new(202)
                                .set_body_json(serde_json::json!({"success": true})),
                            OnRefresh::Fetch(end) => {
                                source.status = "success".to_string();
                                source.end = end;
                                source.updated_at = Some(iso(Utc::now()));
                                ResponseTemplate::new(202)
                                    .set_body_json(serde_json::json!({"success": true}))
                            }
                            OnRefresh::SlowFetch(end) => {
                                source.status = "fetching".to_string();
                                source.pending = Some(end);
                                source.settle_polls = 1;
                                ResponseTemplate::new(202)
                                    .set_body_json(serde_json::json!({"success": true}))
                            }
                        }
                    })
                    .mount(&server)
                    .await;

                let dir = tempfile::tempdir().expect("tempdir");
                let mut config = CrosstalkConfig::load(dir.path()).expect("config");
                config.dispatcharr.base_url = server.uri();
                config.dispatcharr.api_key = "test-key".to_string();
                config.jobs.exports_dir = dir.path().join("exports").display().to_string();
                config.jobs.data_dir = dir.path().join("data").display().to_string();

                Self {
                    server,
                    dir,
                    config,
                }
            }

            fn context_for_run(&self, store: &Store, action: &str, dry_run: bool) -> JobContext {
                let run_id = store
                    .start_run(crate::APP_ID, action, dry_run)
                    .expect("run");
                JobContext::new(Arc::new(self.config.clone()), Arc::new(VecSink::new()))
                    .with_dispatcharr(
                        DispatcharrClient::new(&self.config.dispatcharr).expect("client"),
                    )
                    .for_run(
                        crate::APP_ID,
                        action,
                        run_id,
                        store.clone(),
                        Arc::new(AtomicBool::new(false)),
                    )
            }

            /// How many requests of one method reached one path.
            async fn count(&self, method_name: &str, path: &str) -> usize {
                self.server
                    .received_requests()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .filter(|request| request.method.as_str() == method_name)
                    .filter(|request| request.url.path() == path)
                    .count()
            }

            /// The body of every refresh request, in order.
            async fn refreshes(&self) -> Vec<i64> {
                self.server
                    .received_requests()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .filter(|request| request.method.as_str() == "POST")
                    .filter(|request| request.url.path() == "/api/epg/import/")
                    .map(|request| {
                        serde_json::from_slice::<serde_json::Value>(&request.body)
                            .expect("JSON body")["id"]
                            .as_i64()
                            .expect("id")
                    })
                    .collect()
            }
        }

        fn path_id(path: &str) -> i64 {
            path.trim_end_matches('/')
                .rsplit('/')
                .next()
                .and_then(|id| id.parse().ok())
                .unwrap_or(-1)
        }

        /// One channel per source id 20-25, each on its own guide entry: being mapped is what
        /// makes a source a candidate, so a test can add any source to the listing and know
        /// whether it is mapped without touching this.
        fn world() -> World {
            let channel = |id: i64, name: &str, epg: i64| Channel {
                id,
                name: name.to_string(),
                epg_data_id: Some(epg),
                ..Channel::default()
            };
            World {
                channels: vec![
                    channel(1, "UK: BBC One", 300),
                    channel(2, "US: ESPN", 301),
                    channel(3, "Placeholder", 302),
                    channel(4, "Other", 303),
                    channel(5, "Quiet", 304),
                    channel(6, "Inactive", 305),
                ],
                // Entry id = 280 + source id, so the mapping reads at a glance.
                entries: (20..=25)
                    .map(|source| EpgEntry {
                        id: 280 + source,
                        name: format!("guide {source}"),
                        tvg_id: format!("tvg.{source}"),
                        epg_source: Some(source),
                    })
                    .collect(),
                ..World::default()
            }
        }

        fn long() -> Option<DateTime<Utc>> {
            Some(Utc::now() + ChronoDuration::days(30))
        }

        fn short() -> Option<DateTime<Utc>> {
            Some(Utc::now() + ChronoDuration::hours(3))
        }

        fn app() -> crate::GuideApp {
            crate::GuideApp::new()
        }

        #[tokio::test]
        async fn an_errored_source_is_refreshed_and_recovered_by_its_status() {
            // The bug-072 case: the source sits on `error` with a guide that has run dry.
            let world = world();
            let harness = Harness::start(
                &world,
                vec![
                    SimSource::new(20, "epgshare-UK", "error", short()),
                    SimSource::new(21, "Schedules Direct", "success", long()),
                ],
            )
            .await;
            let store = Store::in_memory().expect("store");
            let context = harness.context_for_run(&store, crate::ACTION_WATCHDOG_APPLY, false);

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("apply");

            assert_eq!(harness.refreshes().await, vec![20]);
            assert_eq!(outcome.counts.get("checked"), Some(&2));
            assert_eq!(outcome.counts.get("refreshed"), Some(&1));
            assert_eq!(outcome.counts.get("recovered"), Some(&1));
            assert_eq!(outcome.counts.get("still_broken"), Some(&0));
            assert!(
                outcome.summary.contains("recovered 1, still broken 0"),
                "{}",
                outcome.summary
            );

            // One row per candidate source, in the order the sources were audited.
            let rows = store.rows(context.run_id().expect("run")).expect("rows");
            assert_eq!(rows.len(), 2);
            // The identity columns carry the SOURCE here, which is what `subject` says: a
            // restore reading this run must not take id 20 for a channel id.
            assert_eq!(rows[0].subject, Subject::EpgSource);
            assert_eq!(rows[0].channel_id, 20);
            assert_eq!(rows[0].channel_name, "epgshare-UK");
            assert_eq!(rows[0].field, "epg_source");
            assert_eq!(rows[0].status, "recovered");
            assert_eq!(rows[0].before_value.as_deref(), Some("error"));
            assert_eq!(rows[0].after_value.as_deref(), Some("success"));
            // The horizon of a short guide is exact: found by bisection, so the report can say
            // when the guide was due to run out.
            assert!(
                rows[0].reason.contains("stale; status error -> success"),
                "{}",
                rows[0].reason
            );
            assert_eq!(rows[1].status, "fresh");
        }

        #[tokio::test]
        async fn a_short_source_is_recovered_when_its_horizon_moves_forward() {
            // No status change at all: `updated_at` moving is the only sign the fetch ran, and
            // the horizon is what decides.
            let world = world();
            let harness = Harness::start(
                &world,
                vec![SimSource::new(20, "epgshare-UK", "success", short())
                    .on(OnRefresh::Fetch(long()))],
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("apply");

            assert_eq!(harness.refreshes().await, vec![20]);
            assert_eq!(outcome.counts.get("recovered"), Some(&1));
            let store = context.store().expect("store");
            let rows = store.rows(context.run_id().expect("run")).expect("rows");
            assert_eq!(rows[0].before_value.as_deref(), Some("success"));
            assert_eq!(rows[0].after_value.as_deref(), Some("success"));
            assert!(
                rows[0].reason.contains("horizon") && rows[0].reason.contains("-> at least"),
                "before: an exact short end; after: a guide that reaches past the cutoff: {}",
                rows[0].reason
            );
        }

        #[tokio::test]
        async fn a_refresh_observed_mid_flight_settles_and_recovers() {
            // The settle wait, end to end: the first poll sees `fetching` — not the answer —
            // and only the settled `success` below classifies. Returning the mid-flight state
            // would read "no improvement" for a refresh that worked.
            let world = world();
            let harness = Harness::start(
                &world,
                vec![SimSource::new(20, "epgshare-UK", "success", short())
                    .on(OnRefresh::SlowFetch(long()))],
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("apply");

            assert_eq!(harness.refreshes().await, vec![20]);
            assert_eq!(outcome.counts.get("recovered"), Some(&1));
            assert_eq!(outcome.counts.get("still_broken"), Some(&0));
        }

        #[tokio::test]
        async fn a_source_that_does_not_improve_is_still_broken() {
            // The refresh succeeds but lands the same short guide: relative improvement says
            // still broken, even though the status is fine and the horizon is not empty.
            let world = world();
            let same = Some(Utc::now() + ChronoDuration::hours(3));
            let harness = Harness::start(
                &world,
                vec![SimSource::new(20, "epgshare-UK", "success", short())
                    .on(OnRefresh::Fetch(same))],
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("apply");

            assert_eq!(outcome.counts.get("recovered"), Some(&0));
            assert_eq!(outcome.counts.get("still_broken"), Some(&1));
            let store = context.store().expect("store");
            let rows = store.rows(context.run_id().expect("run")).expect("rows");
            assert_eq!(rows[0].status, "still_broken");
        }

        #[tokio::test]
        async fn a_fresh_source_costs_one_request_and_is_not_refreshed() {
            let world = world();
            let harness = Harness::start(
                &world,
                vec![SimSource::new(21, "Schedules Direct", "success", long())],
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("apply");

            assert!(harness.refreshes().await.is_empty());
            assert_eq!(outcome.counts.get("checked"), Some(&1));
            // The whole stale decision is one probe: "does programme data reach the cutoff?".
            assert_eq!(harness.count("GET", "/api/epg/programs/search/").await, 1);
        }

        #[tokio::test]
        async fn an_excluded_source_is_not_even_probed() {
            let world = world();
            let harness = Harness::start(
                &world,
                vec![
                    SimSource::new(20, "epgshare-UK", "error", short()),
                    SimSource::new(24, "quiet", "error", short()),
                ],
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );
            let settings = GuideSettings {
                watchdog_exclude_source_ids: "24".to_string(),
                ..GuideSettings::default()
            };

            let outcome = run(&app(), &context, &settings, false)
                .await
                .expect("apply");

            assert_eq!(outcome.counts.get("checked"), Some(&1));
            assert_eq!(harness.refreshes().await, vec![20]);
        }

        #[tokio::test]
        async fn a_dummy_or_unmapped_source_is_skipped() {
            // The plugin's query: active, non-dummy, at least one channel mapped. A dummy
            // source cannot be refreshed at all, and a source nothing watches has no guide
            // worth auditing.
            let world = world();
            let harness = Harness::start(
                &world,
                vec![
                    SimSource::new(20, "epgshare-UK", "error", short()),
                    // Mapped, but a dummy source has no upstream guide to fetch.
                    SimSource::new(22, "Events placeholder", "error", short()).dummy(),
                    // Mapped, but disabled: the plugin's query is `is_active=True`.
                    SimSource::new(25, "inactive", "error", short()).inactive(),
                    // Active and non-dummy, but no channel is mapped to it, so it has no guide
                    // worth auditing.
                    SimSource::new(26, "unmapped", "error", short()),
                ],
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("apply");

            assert_eq!(outcome.counts.get("checked"), Some(&1));
            assert_eq!(harness.refreshes().await, vec![20]);
        }

        #[tokio::test]
        async fn a_refresh_request_that_fails_is_reported_and_not_classified() {
            let world = world();
            let harness = Harness::start(
                &world,
                vec![SimSource::new(20, "epgshare-UK", "error", short()).on(OnRefresh::Fail(400))],
            )
            .await;
            let store = Store::in_memory().expect("store");
            let context = harness.context_for_run(&store, crate::ACTION_WATCHDOG_APPLY, false);

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("a refused refresh is not a failed run");

            assert_eq!(outcome.counts.get("refreshed"), Some(&0));
            assert_eq!(outcome.counts.get("failed"), Some(&1));
            assert_eq!(outcome.counts.get("recovered"), Some(&0));
            assert!(
                outcome.summary.contains("1 refresh request(s) failed"),
                "{}",
                outcome.summary
            );
            let rows = store.rows(context.run_id().expect("run")).expect("rows");
            assert_eq!(rows[0].status, "refresh_failed");
            assert!(
                rows[0].reason.contains("refresh failed"),
                "{}",
                rows[0].reason
            );
        }

        #[tokio::test]
        async fn a_refresh_that_never_settles_is_reported_as_a_timeout() {
            // A queued Celery task that never moves the source: the wait lapses, and the row
            // says so instead of pretending the refresh failed.
            let world = world();
            let harness = Harness::start(
                &world,
                vec![SimSource::new(20, "epgshare-UK", "error", short()).on(OnRefresh::Nothing)],
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );
            let settings = GuideSettings {
                // 0 minutes: request it and do not wait. The same code path as a lapsed wait.
                watchdog_refresh_timeout_minutes: 0,
                ..GuideSettings::default()
            };

            let outcome = run(&app(), &context, &settings, false)
                .await
                .expect("apply");

            assert_eq!(harness.refreshes().await, vec![20]);
            assert_eq!(outcome.counts.get("refreshed"), Some(&1));
            // The status never moved, so the source is still broken.
            assert_eq!(outcome.counts.get("still_broken"), Some(&1));
            let store = context.store().expect("store");
            let rows = store.rows(context.run_id().expect("run")).expect("rows");
            assert!(
                rows[0].reason.contains("did not settle within the timeout"),
                "{}",
                rows[0].reason
            );
        }

        #[tokio::test]
        async fn an_improvement_to_a_still_short_feed_is_a_recovery() {
            // The plugin's central promise: with a 4-day threshold a 20h horizon is still short
            // by the arming rule, and the run must still call the improvement a recovery.
            let world = world();
            let harness =
                Harness::start(
                    &world,
                    vec![SimSource::new(20, "epgshare-UK", "success", short()).on(
                        OnRefresh::Fetch(Some(Utc::now() + ChronoDuration::hours(20))),
                    )],
                )
                .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );
            let settings = GuideSettings {
                watchdog_horizon_threshold_hours: 96,
                ..GuideSettings::default()
            };

            let outcome = run(&app(), &context, &settings, false)
                .await
                .expect("apply");

            assert_eq!(outcome.counts.get("recovered"), Some(&1));
            assert_eq!(outcome.counts.get("still_broken"), Some(&0));
        }

        #[tokio::test]
        async fn a_preview_reports_the_stale_sources_and_refreshes_none() {
            let world = world();
            let harness = Harness::start(
                &world,
                vec![
                    SimSource::new(20, "epgshare-UK", "error", short()),
                    SimSource::new(21, "Schedules Direct", "success", long()),
                ],
            )
            .await;
            let store = Store::in_memory().expect("store");
            let context = harness.context_for_run(&store, crate::ACTION_WATCHDOG_PREVIEW, true);

            let outcome = run(&app(), &context, &GuideSettings::default(), true)
                .await
                .expect("preview");

            assert!(
                harness.refreshes().await.is_empty(),
                "a preview queues nothing"
            );
            assert_eq!(outcome.counts.get("checked"), Some(&2));
            assert_eq!(outcome.counts.get("stale"), Some(&1));
            assert_eq!(outcome.counts.get("would_refresh"), Some(&1));
            assert_eq!(outcome.counts.get("refreshed"), None);

            let rows = store.rows(context.run_id().expect("run")).expect("rows");
            assert_eq!(rows[0].status, "would_refresh");
            assert_eq!(rows[1].status, "fresh");

            let text = std::fs::read_to_string(outcome.report_path.expect("report"))
                .expect("read the report");
            assert!(
                text.starts_with("# crosstalk Guide v"),
                "the report names the app: {text}"
            );
            assert!(text.contains("Would refresh: 1"), "{text}");
            assert!(
                text.contains("arm guide.watchdog_apply in [schedules]"),
                "the schedule replaces the plugin's interval setting: {text}"
            );

            // One data row per candidate source: id, name, status before/after, horizon
            // before/after, stale, action, outcome.
            assert!(
                text.contains(
                    "source_id,source_name,status_before,status_after,horizon_before,\
                     horizon_after,stale,action,outcome"
                ),
                "{text}"
            );
            let row = |prefix: &str| {
                text.lines()
                    .find(|line| line.starts_with(prefix))
                    .unwrap_or_else(|| panic!("no row for {prefix}: {text}"))
                    .to_string()
            };
            let stale = row("20,epgshare-UK,");
            assert!(
                stale.contains(",error,,"),
                "status before, not re-read: {stale}"
            );
            assert!(
                stale.contains(",yes,would refresh,would_refresh"),
                "{stale}"
            );
            let fresh = row("21,Schedules Direct,");
            assert!(fresh.contains(",success,,at least "), "{fresh}");
            assert!(fresh.contains(",no,no action,fresh"), "{fresh}");
        }

        #[tokio::test]
        async fn a_non_administrator_key_warns_in_the_summary() {
            // The programme search scopes rows by channel access, so a lesser key reads some
            // sources short or empty; the run must say so rather than report them as fresh.
            let world = world();
            let harness = Harness::start_as(
                &world,
                vec![SimSource::new(21, "Schedules Direct", "success", long())],
                1,
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_PREVIEW,
                true,
            );

            let outcome = run(&app(), &context, &GuideSettings::default(), true)
                .await
                .expect("preview");

            assert!(
                outcome.summary.contains("not an administrator's"),
                "{}",
                outcome.summary
            );
        }

        #[tokio::test]
        async fn a_cancelled_run_refreshes_nothing() {
            let world = world();
            let harness = Harness::start(
                &world,
                vec![SimSource::new(20, "epgshare-UK", "error", short())],
            )
            .await;
            let store = Store::in_memory().expect("store");
            let context = harness.context_for_run(&store, crate::ACTION_WATCHDOG_APPLY, false);
            context.cancel();

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("a cancelled run still reports what it did");
            assert!(outcome.is_cancelled(), "{}", outcome.summary);
            assert!(
                outcome.summary.contains("cancelled by the operator"),
                "{}",
                outcome.summary
            );
            assert!(harness.refreshes().await.is_empty());
        }

        #[tokio::test]
        async fn the_app_runs_the_watchdog_from_its_own_settings() {
            // The whole path an operator or a schedule takes: guide.toml on disk, then the
            // action by id. The threshold in the file is what decides staleness.
            let world = world();
            let harness = Harness::start(
                &world,
                vec![SimSource::new(
                    21,
                    "Schedules Direct",
                    "success",
                    Some(Utc::now() + ChronoDuration::hours(30)),
                )
                .on(OnRefresh::Fetch(long()))],
            )
            .await;
            let store = Store::in_memory().expect("store");
            crate::settings::save(
                &harness.dir.path().join("guide.toml"),
                &GuideSettings {
                    // 30 hours of guide left is stale under a 48-hour threshold, fresh under
                    // the default 12.
                    watchdog_horizon_threshold_hours: 48,
                    ..GuideSettings::default()
                },
            )
            .expect("write guide.toml");
            let context = harness.context_for_run(&store, crate::ACTION_WATCHDOG_APPLY, false);

            let outcome = app()
                .run(crate::ACTION_WATCHDOG_APPLY, &context, false)
                .await
                .expect("apply");

            assert_eq!(harness.refreshes().await, vec![21]);
            assert_eq!(outcome.counts.get("recovered"), Some(&1));
            assert!(
                outcome
                    .summary
                    .contains("guide ending within 48h counts as stale"),
                "{}",
                outcome.summary
            );
        }

        #[tokio::test]
        async fn a_guide_that_died_weeks_ago_is_still_stale() {
            // The bracket reaches back to 1970 on purpose: a guide that ended a month ago must
            // read as "ends then", not as "no guide", or the source it belongs to would never
            // be refreshed — the exact silent breakage the watchdog exists for.
            let world = world();
            let dead = Some(Utc::now() - ChronoDuration::days(30));
            let harness = Harness::start(
                &world,
                vec![
                    SimSource::new(20, "epgshare-UK", "success", dead).on(OnRefresh::Fetch(long()))
                ],
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("apply");

            assert_eq!(harness.refreshes().await, vec![20]);
            assert_eq!(outcome.counts.get("recovered"), Some(&1));
            let store = context.store().expect("store");
            let rows = store.rows(context.run_id().expect("run")).expect("rows");
            // The dead end is reported as an instant, not as "empty".
            assert!(
                rows[0]
                    .reason
                    .contains("stale; status success -> success; horizon 20"),
                "{}",
                rows[0].reason
            );
        }

        #[tokio::test]
        async fn a_source_with_no_guide_at_all_is_not_stale() {
            // The plugin's `horizon is None` rule: refreshing cannot conjure a guide the
            // provider does not have, so only an errored refresh makes such a source stale.
            let world = world();
            let harness = Harness::start(
                &world,
                vec![SimSource::new(20, "epgshare-UK", "success", None)],
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("apply");

            assert!(harness.refreshes().await.is_empty());
            assert_eq!(outcome.counts.get("checked"), Some(&1));
            // Cutoff probe says no, the epoch probe says no: two requests, then it stops.
            assert_eq!(harness.count("GET", "/api/epg/programs/search/").await, 2);
            let store = context.store().expect("store");
            let rows = store.rows(context.run_id().expect("run")).expect("rows");
            assert!(
                rows[0]
                    .reason
                    .contains("horizon empty (no programme at all)"),
                "{}",
                rows[0].reason
            );
        }

        #[tokio::test]
        async fn a_source_with_no_guide_is_still_refreshed_when_it_errored() {
            let world = world();
            let harness = Harness::start(
                &world,
                vec![SimSource::new(20, "epgshare-UK", "error", None)],
            )
            .await;
            let context = harness.context_for_run(
                &Store::in_memory().expect("store"),
                crate::ACTION_WATCHDOG_APPLY,
                false,
            );

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("apply");

            assert_eq!(harness.refreshes().await, vec![20]);
            assert_eq!(outcome.counts.get("recovered"), Some(&1));
        }

        #[tokio::test]
        async fn a_refresh_that_lands_nothing_is_not_a_recovery() {
            // The status moved (so the wait settled) but the guide went from a short one to
            // none at all: a regression, not an improvement.
            let world = world();
            let harness = Harness::start(
                &world,
                vec![SimSource::new(20, "epgshare-UK", "success", short())
                    .on(OnRefresh::Fetch(None))],
            )
            .await;
            let store = Store::in_memory().expect("store");
            let context = harness.context_for_run(&store, crate::ACTION_WATCHDOG_APPLY, false);

            let outcome = run(&app(), &context, &GuideSettings::default(), false)
                .await
                .expect("apply");

            // The fetch landed nothing at all: the horizon went from an exact short end to
            // empty, which is a regression, not a recovery.
            assert_eq!(outcome.counts.get("recovered"), Some(&0));
            assert_eq!(outcome.counts.get("still_broken"), Some(&1));
            let rows = store.rows(context.run_id().expect("run")).expect("rows");
            assert!(rows[0].reason.contains("empty"), "{}", rows[0].reason);
        }
    }
}
