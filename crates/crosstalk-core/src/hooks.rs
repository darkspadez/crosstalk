//! `/hooks/dispatcharr`: what Dispatcharr's Connect integration can trigger.
//!
//! Dispatcharr fires a Connect event when an M3U account or an EPG source finishes refreshing.
//! Stream-Mapparr and IPTV Checker both hung work off those events (its `on_m3u_refresh` action,
//! its post-scan hand-off), and the host does the same here — one route, one config table, any
//! action or workflow:
//!
//! ```toml
//! [hooks]
//! m3u_refresh = ["streams.match_apply"]
//! epg_refresh = ["workflow.nightly"]
//! ```
//!
//! # What Dispatcharr actually sends
//!
//! Read from Dispatcharr's own source at port time — `apps/connect/handlers/webhook.py`,
//! `apps/connect/utils.py`, `core/utils.py::dispatch_event_system`, and the two callers of
//! `log_system_event` in `apps/m3u/tasks.py` and `apps/epg/tasks.py`:
//!
//! * the integration config is `{ url, headers }`, and the handler posts the payload to `url`
//!   with those headers, so a custom `Authorization` header — and, for a JSON body, a
//!   `Content-Type: application/json` — are the operator's to set;
//! * with no subscription `payload_template`, the payload is the event's details **dict**, so
//!   `requests.post(url, data=dict)` sends `account_name=Foo&elapsed_time=12.3&…` — form-encoded,
//!   and **without an event name** (the plugin side gets `{"event": …, "payload": …}`; the
//!   webhook side does not);
//! * `m3u_refresh` details are `account_name`, `elapsed_time`, `streams_created`,
//!   `streams_updated`, `streams_stale`, `streams_deleted`, `total_processed`;
//! * `epg_refresh` details are `source_name`, `programs`, `channels`, `skipped_programs` (and
//!   `unmapped_channels` from the XMLTV path).
//!
//! So the body can be JSON or form-encoded, and the event name may be absent. [`read_event`]
//! handles all of it, in this order: the `?event=` query parameter, an `event` field, a `type`
//! field, and — when nothing says it — the payload shape, but only for events the configuration
//! actually names.
//!
//! # Debounce
//!
//! A refresh of twenty M3U accounts fires twenty `m3u_refresh` events within a few seconds; that
//! is one logical "the playlists changed", so one match. Every delivery of the same event
//! restarts a timer, and the targets run once the burst has been quiet for
//! `hooks.debounce_secs`. An event that arrives while the target is busy queues **one** follow-up
//! run for when it frees — Stream-Mapparr's dirty-flag behaviour, which is what keeps a late
//! account from being dropped and a busy app from being started twenty times.

use crate::config::HooksConfig;
use crate::runner::JobRunner;
use crate::target::Target;
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The route Dispatcharr posts to.
pub const ROUTE: &str = "/hooks/dispatcharr";

/// How often a queued follow-up re-checks whether its target is free.
const FOLLOW_UP_POLL: Duration = Duration::from_millis(250);

/// How long a queued follow-up waits for its target before giving up.
///
/// Bounded so a target that stays busy (a probe that pauses and resumes for days) cannot leave a
/// watcher behind forever. Giving up is logged: the next event still triggers normally.
const FOLLOW_UP_MAX_WAIT: Duration = Duration::from_secs(30 * 60);

/// Where an event name came from, for the log line an operator reads when a hook misfires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSource {
    /// A `?event=` query parameter.
    Query,
    /// An `event` field in the body.
    Field,
    /// A `type` field in the body (Dispatcharr's websocket-update shape).
    Type,
    /// Guessed from the payload's own keys, because Dispatcharr's default delivery names no
    /// event.
    PayloadShape,
}

impl EventSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventSource::Query => "query",
            EventSource::Field => "event field",
            EventSource::Type => "type field",
            EventSource::PayloadShape => "payload shape",
        }
    }
}

/// The event a delivery names, and how it was found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedEvent {
    pub name: String,
    pub source: EventSource,
}

/// The longest event name a delivery may carry in its query string or body.
///
/// Dispatcharr's own event names are a dozen characters; an operator's custom subscription
/// names are not much longer. Anything past this is not an event but a mistake (or a probe),
/// and the name is echoed back in the 202 body and the log line — so it is refused as unknown
/// rather than passed through unbounded. A configured event with a longer name can still fire
/// from the scheduler or the CLI; only webhook deliveries are capped.
const MAX_EVENT_NAME_LEN: usize = 128;

/// Whether a name from a delivery is usable as an event: present after trimming, and short.
fn valid_name(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.len() > MAX_EVENT_NAME_LEN {
        return None;
    }
    Some(value.to_string())
}

/// The shape rules for Dispatcharr's two refresh events, in the order they are tried.
///
/// Each entry is (event, keys any one of which identifies it). Only the refresh's own counters
/// count — deliberately NOT `account_name`/`source_name`: the `m3u_error`/`epg_error` events
/// carry those too (with only a `message` beside them), and an error delivery must never be
/// mistaken for a successful refresh. Both refresh callers always send the counters, so this
/// loses nothing on real payloads.
const SHAPE_RULES: &[(&str, &[&str])] = &[
    (
        "m3u_refresh",
        &[
            "total_processed",
            "streams_created",
            "streams_updated",
            "elapsed_time",
        ],
    ),
    (
        "epg_refresh",
        &[
            "programs",
            "channels",
            "skipped_programs",
            "unmapped_channels",
        ],
    ),
];

/// Read a delivery body as fields, leniently.
///
/// A JSON object contributes its scalar values as text (`{"account_name": "Foo"}` and
/// `account_name=Foo` become the same map). A form-encoded body is parsed by hand: `+` is a
/// space, `%XX` is a byte, and a malformed escape is kept literally rather than failing the
/// delivery — Dispatcharr sends whatever an account is named, including a `%`.
pub fn read_fields(body: &[u8]) -> BTreeMap<String, String> {
    let text = match std::str::from_utf8(body) {
        Ok(text) => text.trim(),
        Err(_) => return BTreeMap::new(),
    };
    if text.is_empty() {
        return BTreeMap::new();
    }
    if text.starts_with('{') {
        if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(text) {
            return map
                .into_iter()
                .map(|(key, value)| (key, json_scalar(&value)))
                .collect();
        }
    }
    parse_form(text)
}

/// One JSON value as the text a form field would have carried.
fn json_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Parse `a=1&b=2` with percent decoding.
fn parse_form(text: &str) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for pair in text.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((key, value)) => (key, value),
            None => (pair, ""),
        };
        fields.insert(percent_decode(key), percent_decode(value));
    }
    fields
}

/// Decode `+` and `%XX`, keeping anything malformed as written.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = (bytes[index + 1] as char).to_digit(16);
                let low = (bytes[index + 2] as char).to_digit(16);
                match (high, low) {
                    (Some(high), Some(low)) => {
                        out.push((high * 16 + low) as u8);
                        index += 3;
                    }
                    _ => {
                        out.push(bytes[index]);
                        index += 1;
                    }
                }
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

/// Work out which event a delivery is about.
///
/// `configured` is the set of event names the configuration names: payload-shape guessing only
/// answers with one of those, so a delivery cannot be attributed to an event nobody configured
/// (and cannot run the wrong targets when two events share a field name).
pub fn read_event(
    query_event: Option<&str>,
    fields: &BTreeMap<String, String>,
    configured: &BTreeSet<String>,
) -> Option<ResolvedEvent> {
    let explicit = |value: Option<&String>, source: EventSource| {
        value
            .and_then(|value| valid_name(value))
            .map(|name| ResolvedEvent { name, source })
    };
    if let Some(name) = query_event.and_then(valid_name) {
        return Some(ResolvedEvent {
            name,
            source: EventSource::Query,
        });
    }
    if let Some(event) = explicit(fields.get("event"), EventSource::Field) {
        return Some(event);
    }
    if let Some(event) = explicit(fields.get("type"), EventSource::Type) {
        return Some(event);
    }
    for (event, markers) in SHAPE_RULES {
        if configured.contains(*event) && markers.iter().any(|marker| fields.contains_key(*marker))
        {
            return Some(ResolvedEvent {
                name: (*event).to_string(),
                source: EventSource::PayloadShape,
            });
        }
    }
    None
}

/// What a delivery did, for the log line and the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivered {
    /// Nothing names this event: logged and ignored.
    Unknown,
    /// The event is known but the configuration lists no targets for it.
    Unconfigured,
    /// The delivery joined a burst that is already settling: the run happens once, later.
    Coalesced { event: String },
    /// Targets were resolved and will run when the burst settles.
    Scheduled { event: String, targets: usize },
}

/// The debouncer and trigger: one per daemon.
///
/// Cheap to clone through an `Arc`; holds only the bursts in flight. It reads the live config on
/// every delivery, so a saved `[hooks]` change takes effect on the next event without a restart.
pub struct Hooks {
    runner: JobRunner,
    /// One entry per event with a burst settling. The task that owns it clears it before running
    /// its targets, so an event that arrives DURING a run starts a fresh cycle (which is what
    /// gives a busy target its one follow-up).
    pending: Mutex<HashMap<String, Arc<Pending>>>,
    /// Targets with a follow-up already queued, so a burst during a busy run queues one, not N.
    follow_ups: Mutex<HashSet<String>>,
    /// How long a follow-up waits between checks, and how long it waits in total. Configurable so
    /// a test does not sleep for a minute.
    follow_up_poll: Duration,
    follow_up_max_wait: Duration,
}

/// One settling burst.
#[derive(Debug)]
struct Pending {
    /// When the last delivery of this event arrived.
    last: Mutex<tokio::time::Instant>,
}

impl Pending {
    fn new() -> Self {
        Self {
            last: Mutex::new(tokio::time::Instant::now()),
        }
    }

    /// Restart the timer: the burst is not over.
    fn touch(&self) {
        *self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = tokio::time::Instant::now();
    }

    /// When the burst will have been quiet for `debounce`.
    fn fire_at(&self, debounce: Duration) -> tokio::time::Instant {
        *self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            + debounce
    }
}

impl std::fmt::Debug for Hooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hooks")
            .field("settling", &self.settling())
            .finish_non_exhaustive()
    }
}

impl Hooks {
    pub fn new(runner: JobRunner) -> Arc<Self> {
        Self::new_with_pacing(runner, FOLLOW_UP_POLL, FOLLOW_UP_MAX_WAIT)
    }

    /// The service with explicit follow-up pacing: the daemon's `new` with the constants, and the
    /// tests with a poll short enough not to sleep for a minute.
    pub fn new_with_pacing(
        runner: JobRunner,
        follow_up_poll: Duration,
        follow_up_max_wait: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            runner,
            pending: Mutex::new(HashMap::new()),
            follow_ups: Mutex::new(HashSet::new()),
            follow_up_poll,
            follow_up_max_wait,
        })
    }

    /// How many events have a burst settling right now, for the tests and the status line.
    pub fn settling(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// The targets currently waiting for their app to free up.
    pub fn queued_follow_ups(&self) -> Vec<String> {
        let mut queued: Vec<String> = self
            .follow_ups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect();
        queued.sort();
        queued
    }

    /// Deliver one event: resolve its targets and schedule the debounced run.
    ///
    /// Returns immediately, which is what keeps the route's answer at 202: Dispatcharr's handler
    /// has a ten-second timeout and a match can take minutes.
    pub fn deliver(self: &Arc<Self>, event: &str) -> Delivered {
        let config = self.runner.config_snapshot();
        if !config.hooks.events.contains_key(event) {
            tracing::info!(
                event,
                "hook delivery for an event the configuration does not name"
            );
            return Delivered::Unknown;
        }
        let targets = config.hooks.targets(event).to_vec();
        if targets.is_empty() {
            tracing::info!(event, "hook delivery ignored: no targets configured");
            return Delivered::Unconfigured;
        }

        let debounce = config.hooks.debounce();
        let pending = {
            let mut map = self
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match map.get(event) {
                // The burst is already settling: restart its timer and let the one task run.
                Some(existing) => {
                    existing.touch();
                    tracing::debug!(event, "hook delivery joined the burst already settling");
                    return Delivered::Coalesced {
                        event: event.to_string(),
                    };
                }
                None => {
                    let pending = Arc::new(Pending::new());
                    map.insert(event.to_string(), Arc::clone(&pending));
                    pending
                }
            }
        };

        let hooks = Arc::clone(self);
        let event_name = event.to_string();
        tokio::spawn(async move {
            hooks.settle(&event_name, &pending, debounce).await;
        });
        Delivered::Scheduled {
            event: event.to_string(),
            targets: targets.len(),
        }
    }

    /// Wait out the burst, then run the event's targets.
    async fn settle(self: Arc<Self>, event: &str, pending: &Arc<Pending>, debounce: Duration) {
        loop {
            let fire_at = pending.fire_at(debounce);
            tokio::time::sleep_until(fire_at).await;
            // Another delivery may have restarted the timer while this task slept: if it did,
            // the new deadline is later than the one just reached, so wait again.
            if pending.fire_at(debounce) <= fire_at {
                break;
            }
        }
        // Cleared before the targets run: a delivery during the run starts a new cycle, which is
        // how a burst that lands mid-run queues its single follow-up instead of being lost.
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(event);

        let config = self.runner.config_snapshot();
        let targets = config.hooks.targets(event).to_vec();
        tracing::info!(
            event,
            targets = targets.len(),
            "hook burst settled; starting its targets"
        );
        for target in targets {
            self.fire(event, &target).await;
        }
    }

    /// Start one target, or queue the single follow-up a busy target earns.
    ///
    /// A target that names nothing is logged and dropped: a follow-up only makes sense for work
    /// that WILL run once the app frees, and retrying a typo for half an hour helps nobody.
    async fn fire(self: &Arc<Self>, event: &str, target: &str) {
        let parsed = match Target::parse(target) {
            Ok(parsed) => parsed,
            Err(error) => {
                tracing::warn!(event, target, "hook target is malformed: {error:#}");
                return;
            }
        };
        if !parsed.exists(self.runner.registry(), &self.runner.config_snapshot()) {
            tracing::warn!(
                event,
                target,
                "hook target names no action or workflow this build knows; ignoring it"
            );
            return;
        }
        match self.try_start(&parsed) {
            Ok(_) => {}
            Err(error) => {
                // It exists, so the refusal is "busy" (or the daemon is shutting down). Only a
                // busy app earns a follow-up: a shutdown is not going to free up.
                if self.runner.is_shutting_down() {
                    tracing::warn!(event, target, "hook target not started: {error:#}");
                    return;
                }
                tracing::warn!(event, target, "hook target is busy: {error:#}");
                self.queue_follow_up(event, parsed);
            }
        }
    }

    /// Start a target with the host's dry-run rule for a trigger: an action's own kind decides,
    /// a workflow is a real run when any of its steps writes.
    fn try_start(&self, target: &Target) -> Result<crate::runner::StartedRun> {
        let dry_run = target.dry_run(self.runner.registry(), &self.runner.config_snapshot());
        // A hook has no window: it is an event, not a schedule, so nothing bounds the run.
        let started = target.start(&self.runner, dry_run, Default::default())?;
        tracing::info!(
            target = %target,
            run_id = started.run_id,
            dry_run,
            "hook target started"
        );
        Ok(started)
    }

    /// Queue ONE follow-up for a target that could not start, and run it when it frees.
    ///
    /// At most one: a burst of twenty deliveries during a run must not become twenty more runs
    /// after it, which is exactly what Stream-Mapparr's dirty flag was for.
    fn queue_follow_up(self: &Arc<Self>, event: &str, target: Target) {
        let key = target.key();
        let queued = self
            .follow_ups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key.clone());
        if !queued {
            tracing::info!(
                event,
                target = key.as_str(),
                "a follow-up is already queued for this target; this delivery joins it"
            );
            return;
        }
        tracing::info!(
            event,
            target = key.as_str(),
            "queued one follow-up run for when the target frees"
        );

        let hooks = Arc::clone(self);
        let event = event.to_string();
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + hooks.follow_up_max_wait;
            while hooks.is_busy(&target) {
                if tokio::time::Instant::now() >= deadline {
                    tracing::warn!(
                        event,
                        target = key.as_str(),
                        "gave up waiting for the target to free; the follow-up is dropped"
                    );
                    hooks.clear_follow_up(&key);
                    return;
                }
                tokio::time::sleep(hooks.follow_up_poll).await;
            }
            // Cleared before the start: a delivery during the follow-up run may queue the next
            // one, exactly as it may after a scheduled run.
            hooks.clear_follow_up(&key);
            match hooks.try_start(&target) {
                Ok(_) => tracing::info!(
                    event,
                    target = key.as_str(),
                    "the queued follow-up run started"
                ),
                Err(error) => tracing::warn!(
                    event,
                    target = key.as_str(),
                    "the queued follow-up could not start: {error:#}"
                ),
            }
        });
    }

    fn clear_follow_up(&self, key: &str) {
        self.follow_ups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(key);
    }

    fn is_busy(&self, target: &Target) -> bool {
        // The in-process slot AND the cross-process one: a one-shot CLI run holds no entry in
        // this process's busy map, but its `running` row still means the app cannot take more
        // work. Without the store half a follow-up queued during a CLI run would check once,
        // see an idle map and drop itself instead of waiting.
        self.runner.is_busy(target.app())
            || self
                .runner
                .store()
                .running_runs(target.app())
                .is_ok_and(|runs| !runs.is_empty())
    }
}

/// The event name and target list a configuration describes, for the UI's read-only display.
pub fn describe(config: &HooksConfig) -> Vec<(String, Vec<String>)> {
    config
        .events
        .iter()
        .map(|(event, targets)| (event.clone(), targets.clone()))
        .collect()
}

#[cfg(test)]
mod tests;
