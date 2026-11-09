//! EPG endpoints: sources, entries, the programme search, horizons and refreshes.
//!
//! The programme side is where the REST surface is weakest, so the shapes that matter are
//! spelled out here rather than in the method bodies:
//!
//!   * `GET /epg/programs/` (the plain list) has no filterset and no pagination class in the
//!     view, so it answers the whole `ProgramData` table. Never use it.
//!   * `GET /epg/programs/search/` is the filtered one: `epg_source`, `tvg_id` and the four
//!     time bounds are filters, `fields=` trims the row and `page_size` caps at 500. It always
//!     orders by `start_time` — there is no ordering parameter — which is why an exact
//!     horizon costs a bisection (see [`DispatcharrClient::epg_horizon`]).

use super::types::PaginatedPrograms;
use super::*;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use std::time::Duration;

/// How many programmes the placeholder matcher asks about per request. Upstream the view
/// silently truncates `epg_data_ids` to 50, so a longer list would answer only its first 50
/// entries with no error to notice.
const CURRENT_PROGRAMS_CHUNK_SIZE: usize = 50;

/// How long [`DispatcharrClient::await_epg_refresh`] waits between polls. A fetch-and-parse of
/// a large XMLTV file takes minutes, so a tighter interval would only add requests.
const EPG_REFRESH_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// How far back the default horizon bracket reaches: a source whose guide ended longer ago
/// than this reads as [`EpgHorizon::Empty`].
const HORIZON_LOOKBACK_DAYS: i64 = 7;

/// How far ahead the default horizon bracket reaches. Longer guides exist, but a source that
/// still has programmes beyond this has nothing the weekly watchdog needs to know.
const HORIZON_AHEAD_DAYS: i64 = 30;

impl DispatcharrClient {
    pub async fn epg_sources(&self) -> Result<Vec<EpgSource>> {
        self.get_list("/epg/sources/", &[], "listing EPG sources")
            .await
    }

    /// One EPG source, with the fields a refresh moves.
    ///
    /// `GET /epg/sources/{id}/` returns the whole row (including an `epg_data_count`); only
    /// the status fields are read, because that is all a refresh poll needs.
    pub async fn epg_source_state(&self, source_id: i64) -> Result<EpgSourceState> {
        self.get_json(&format!("/epg/sources/{source_id}/"), &[])
            .await
            .with_context(|| format!("reading the state of EPG source {source_id}"))
    }

    /// Create an EPG source, including a custom dummy one.
    ///
    /// `POST /epg/sources/` needs `name` (unique) and `source_type`; both are checked here so
    /// a missing one is a client-side error rather than a 400. A dummy source's programmes are
    /// generated from `custom_properties` instead of fetched, and Dispatcharr refuses to
    /// refresh it, which is why it never appears in a horizon check.
    ///
    /// Not retried: `name` is unique, so a re-sent create answers 400 for a create that
    /// worked.
    pub async fn create_epg_source(&self, source: &EpgSourceEdit) -> Result<EpgSource> {
        let name = source
            .name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .context("an EPG source needs a name")?;
        let source_type = source
            .source_type
            .as_deref()
            .map(str::trim)
            .filter(|kind| !kind.is_empty())
            .context("an EPG source needs a source_type (xmltv, schedules_direct or dummy)")?;

        let body = serde_json::to_value(source).context("serializing the EPG source")?;
        self.write_json(
            &reqwest::Method::POST,
            "/epg/sources/",
            Some(&body),
            Retry::Once,
        )
        .await
        .with_context(|| format!("creating the {source_type} EPG source {name:?}"))
    }

    /// Update an EPG source; an absent field is left alone.
    ///
    /// Toggling `is_active` also moves `status` to `idle` (or `disabled`) upstream, so the
    /// returned source is the one to read back.
    pub async fn update_epg_source(
        &self,
        source_id: i64,
        source: &EpgSourceEdit,
    ) -> Result<EpgSource> {
        let body = serde_json::to_value(source).context("serializing the EPG source")?;
        self.write_json(
            &reqwest::Method::PATCH,
            &format!("/epg/sources/{source_id}/"),
            Some(&body),
            Retry::Idempotent,
        )
        .await
        .with_context(|| format!("updating EPG source {source_id}"))
    }

    /// Delete an EPG source, and with it the guide data it carried.
    ///
    /// Not retried: a retry after a lost response answers 404 and would report a failure for a
    /// delete that worked.
    pub async fn delete_epg_source(&self, source_id: i64) -> Result<()> {
        self.write_ignored(
            &reqwest::Method::DELETE,
            &format!("/epg/sources/{source_id}/"),
            Retry::Once,
        )
        .await
        .with_context(|| format!("deleting EPG source {source_id}"))
    }

    /// Every EPG entry.
    ///
    /// Upstream this endpoint is not paginated, so a bare array is the normal answer. Some
    /// builds put it behind a page envelope, so both shapes are accepted; a page parameter is
    /// sent because a build that does paginate would otherwise silently return only the first
    /// page.
    ///
    /// `results` is required in the envelope: an object without it (`200 {}` from a proxy or
    /// a half-started server) is an error, not zero entries, because "no entries" reads as
    /// "every assigned guide is gone" to the scan and heal actions.
    pub async fn epg_entries(&self) -> Result<Vec<EpgEntry>> {
        self.get_list("/epg/epgdata/", &[], "listing EPG entries")
            .await
    }

    /// Whether an EPG entry has any programme inside the window.
    ///
    /// Memoised on the entry AND the window: a run asks the same question many times, but a
    /// later run with a different window must not inherit an old answer.
    pub async fn has_programs(
        &self,
        tvg_id: &str,
        epg_source: i64,
        window: &ProgramWindow,
    ) -> Result<HasPrograms> {
        if tvg_id.is_empty() {
            // Nothing to ask about: programmes are keyed by tvg_id, and the plugin's own
            // query would filter on an empty string.
            return Ok(HasPrograms::No);
        }

        let key = (
            tvg_id.to_string(),
            epg_source,
            window.start.clone(),
            window.end.clone(),
        );
        if let Some(cached) = self.program_cache.get(&key) {
            return Ok(*cached);
        }

        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("the programme-check semaphore was closed"))?;

        // Another thread may have filled the cache while this one waited for a permit.
        if let Some(cached) = self.program_cache.get(&key) {
            return Ok(*cached);
        }

        let rows: PaginatedPrograms = self
            .get_json(
                "/epg/programs/search/",
                &[
                    ("tvg_id", tvg_id.to_string()),
                    ("epg_source", epg_source.to_string()),
                    ("end_after", window.start.clone()),
                    ("start_before", window.end.clone()),
                    ("page_size", "1".to_string()),
                    ("fields", "id".to_string()),
                ],
            )
            .await
            .with_context(|| format!("checking programme data for tvg_id {tvg_id:?}"))?;

        let answer = if rows.results.is_empty() {
            HasPrograms::No
        } else {
            HasPrograms::Yes
        };
        self.program_cache.insert(key, answer);
        Ok(answer)
    }

    /// Whether any programme on an EPG source ends at or after `cutoff`.
    ///
    /// One request: `GET /epg/programs/search/?epg_source=<id>&end_after=<cutoff>` with
    /// `page_size=1&fields=end_time`. The search endpoint filters `end_time__gte` and answers
    /// a `count` plus the page, so this is the cheapest horizon question the REST API answers:
    /// one indexed count and one row, per source.
    ///
    /// It is the question the watchdog actually asks — "does this source still cover the next
    /// N hours?" — and it never needs the exact end time to decide. Like
    /// [`Self::has_programs`], a non-administrator key only sees programmes on guide data a
    /// channel it can reach is mapped to, so an unmapped source reads as empty for it.
    pub async fn epg_ends_at_or_after(
        &self,
        source_id: i64,
        cutoff: DateTime<Utc>,
    ) -> Result<bool> {
        let rows: PaginatedPrograms = self
            .get_json(
                "/epg/programs/search/",
                &[
                    ("epg_source", source_id.to_string()),
                    ("end_after", rfc3339(cutoff)),
                    ("page_size", "1".to_string()),
                    ("fields", "end_time".to_string()),
                ],
            )
            .await
            .with_context(|| {
                format!("checking whether EPG source {source_id} still covers {cutoff}")
            })?;
        Ok(!rows.results.is_empty())
    }

    /// The programmes airing right now on these EPG entries.
    ///
    /// `POST /epg/current-programs/` — there is no GET route on that view. The body is
    /// `{"epg_data_ids": [...]}` and the answer is a FLAT array of
    /// [`CurrentProgram`]s, each carrying the `epg_data_id` it belongs to; an entry with
    /// nothing airing is simply absent. Up to [`CURRENT_PROGRAMS_CHUNK_SIZE`] ids per request
    /// (the view truncates silently beyond that), so this chunks and concatenates.
    ///
    /// Two things a caller must not misread: an entry whose guide index is still being built
    /// answers `{"epg_data_id": N, "parsing": true}` instead of a programme (see
    /// [`CurrentProgram::is_parsing`]), and an id that does not exist is ignored rather than
    /// rejected. POST is `IsAdmin` upstream, so this needs an administrator's key.
    ///
    /// The placeholder matcher is what reads this: it matches a placeholder channel's or
    /// stream's name against what is airing on its guide entry, which is one request per 50
    /// entries rather than a programme search per entry.
    pub async fn current_programs(&self, epg_data_ids: &[i64]) -> Result<Vec<CurrentProgram>> {
        if epg_data_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut programs: Vec<CurrentProgram> = Vec::with_capacity(epg_data_ids.len());
        for chunk in epg_data_ids.chunks(CURRENT_PROGRAMS_CHUNK_SIZE) {
            let body = serde_json::json!({ "epg_data_ids": chunk });
            let answered: Vec<CurrentProgram> = self
                .write_json(
                    &reqwest::Method::POST,
                    "/epg/current-programs/",
                    Some(&body),
                    Retry::Idempotent,
                )
                .await
                .with_context(|| {
                    format!(
                        "reading what is airing on {} EPG entr{}",
                        chunk.len(),
                        if chunk.len() == 1 { "y" } else { "ies" }
                    )
                })?;
            programs.extend(answered);
        }
        Ok(programs)
    }

    /// The latest programme end time on an EPG source, over the default bracket: the last
    /// seven days against the next thirty, in UTC.
    ///
    /// See [`Self::epg_horizon_between`] for the cost; this is the convenience the watchdog
    /// uses when it wants to report *when* a guide runs out, not just whether it does.
    pub async fn epg_horizon(&self, source_id: i64) -> Result<EpgHorizon> {
        let now = Utc::now();
        self.epg_horizon_between(
            source_id,
            now - chrono::Duration::days(HORIZON_LOOKBACK_DAYS),
            now + chrono::Duration::days(HORIZON_AHEAD_DAYS),
        )
        .await
    }

    /// The latest programme end time on an EPG source, inside `[since, until)`.
    ///
    /// There is no cheap route for this, and the two candidates are both unusable as written:
    ///
    ///   * `GET /epg/programs/` (the plain list) has no filterset class and no pagination class
    ///     in its view, so it dumps the entire `ProgramData` table.
    ///   * `GET /epg/programs/search/` filters by `epg_source` and by `end_time`, accepts
    ///     `fields=` and `page_size=`, but orders by `start_time` unconditionally. The newest
    ///     row on page 1 is therefore the latest *start*, not the latest *end*, and no
    ///     `ordering` parameter exists.
    ///
    /// So the horizon is found by bisecting the existence probe of
    /// [`Self::epg_ends_at_or_after`] over the bracket: about `log2(window / 1 minute)`
    /// requests per source — 16 for the default 37-day bracket — each of them the indexed
    /// count above. That is affordable for a watchdog that runs a few times a day, and is why
    /// the threshold question has its own one-request method: use
    /// [`Self::epg_ends_at_or_after`] when the decision is all that is needed, and this when
    /// the report has to say when the guide runs out.
    ///
    /// The result is rounded down to the minute, so it never claims more guide than there is.
    pub async fn epg_horizon_between(
        &self,
        source_id: i64,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<EpgHorizon> {
        if until <= since {
            bail!("the horizon bracket must end after it starts ({since} .. {until})");
        }
        if !self.epg_ends_at_or_after(source_id, since).await? {
            return Ok(EpgHorizon::Empty);
        }
        if self.epg_ends_at_or_after(source_id, until).await? {
            return Ok(EpgHorizon::Beyond(until));
        }

        // Invariant: something ends at or after `low`, nothing ends at or after `high`.
        let mut low = since;
        let mut high = until;
        while high - low > chrono::Duration::minutes(1) {
            let middle = low + (high - low) / 2;
            if self.epg_ends_at_or_after(source_id, middle).await? {
                low = middle;
            } else {
                high = middle;
            }
        }
        Ok(EpgHorizon::At(low))
    }

    /// Ask Dispatcharr to refresh one EPG source.
    ///
    /// `POST /epg/import/` `{"id": <id>}` answers 202 as soon as the Celery task is queued:
    /// the fetch and parse happen later, so this means "requested", never "done". Follow it
    /// with [`Self::await_epg_refresh`] when the caller needs to know how it went.
    ///
    /// A dummy source is rejected with 400 ("Dummy EPG sources do not require refreshing");
    /// its programmes are generated, so there is nothing to fetch. Not retried: a re-sent
    /// request queues a second refresh of the same source.
    pub async fn refresh_epg_source(&self, source_id: i64) -> Result<()> {
        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            success: bool,
            #[serde(default)]
            message: String,
        }

        let body = serde_json::json!({ "id": source_id });
        let response: Response = self
            .write_json(
                &reqwest::Method::POST,
                "/epg/import/",
                Some(&body),
                Retry::Once,
            )
            .await
            .with_context(|| format!("requesting a refresh of EPG source {source_id}"))?;
        if !response.success {
            bail!(
                "Dispatcharr refused the refresh of EPG source {source_id}: {}",
                response.message.trim()
            );
        }
        Ok(())
    }

    /// Poll a source until its refresh settles, or `timeout` lapses.
    ///
    /// A refresh moves through `fetching` → `parsing` → `success`/`error`, and only the last
    /// step is the answer: returning on the first movement would hand the caller a source
    /// mid-flight, whose horizon has no settled value to read (upstream models this the same
    /// way with `_NON_TERMINAL_REFRESH_STATUSES`). So this waits until the status leaves
    /// `fetching`/`parsing` AND the state differs from `before`, or the timeout lapses.
    ///
    /// A refresh writes two things: `status` and, only on success, `updated_at`. So "changed"
    /// means either field moved since `before` — a failed refresh moves the status alone, and
    /// waiting for `updated_at` would then wait out the whole timeout. A refresh that ran and
    /// failed back to the same status still counts as changed when a mid-flight status was
    /// observed, so a fast error is not reported as "did not settle".
    ///
    /// `cancelled` is checked between polls, so an operator's cancel lands within one interval
    /// rather than after the refresh finishes; it returns the repo's usual "cancelled by the
    /// operator" error. Either outcome carries the state the last poll read, which is what the
    /// watchdog classifies as recovered or still broken.
    pub async fn await_epg_refresh(
        &self,
        source_id: i64,
        before: &EpgSourceState,
        timeout: Duration,
        cancelled: impl Fn() -> bool,
    ) -> Result<EpgRefresh> {
        let started = std::time::Instant::now();
        // Whether a mid-flight status was observed. A refresh that ran and failed back to
        // the status it started from leaves no other trace, so this is what distinguishes
        // "ran and failed" from "never ran" at the timeout.
        let mut saw_mid_flight = false;
        loop {
            if cancelled() {
                bail!("cancelled by the operator");
            }
            let state = self.epg_source_state(source_id).await?;
            let mid_flight = matches!(state.status.as_str(), "fetching" | "parsing");
            saw_mid_flight |= mid_flight;
            if !mid_flight && (state.changed_since(before) || saw_mid_flight) {
                return Ok(EpgRefresh::Changed(state));
            }
            let elapsed = started.elapsed();
            if elapsed >= timeout {
                return Ok(EpgRefresh::TimedOut(state));
            }
            tokio::time::sleep(EPG_REFRESH_POLL_INTERVAL.min(timeout - elapsed)).await;
        }
    }
}

/// An instant as the query string wants it: second precision, UTC.
fn rfc3339(instant: DateTime<Utc>) -> String {
    instant.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
