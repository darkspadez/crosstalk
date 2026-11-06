//! Run history: the SQLite store behind the UI, the API and the status action.
//!
//! Every run is recorded with its rows, its AI decisions and its timings, so "what did it do
//! last night" is a query rather than a scroll through logs. The CSV exports stay, because
//! they are what an operator sends to someone else; this is what the daemon can search.
//!
//! One connection behind a mutex is deliberate. SQLite serialises writers anyway, the writes
//! are small and local, and a pool would only add contention to the one thing a run does not
//! need to be fast at.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// The data version, kept in `PRAGMA user_version`.
///
/// The schema itself is created with `IF NOT EXISTS`, so it needs no version; what does need
/// one is the one-time rename of recorded history below, which must not run twice.
const DATA_VERSION: i64 = 1;

/// How a run ended.
///
/// `Paused` is its own outcome because it means something no other status does: the run stopped
/// cleanly at its window's deadline, saved its progress, and will continue on the next firing.
/// Recording it as cancelled would make an operator think work was lost; recording it as
/// succeeded would claim a job was finished when it was not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Paused,
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::Succeeded => "succeeded",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
            RunStatus::Paused => "paused",
        }
    }

    /// Whether this run is still going, for a template that must not compare strings.
    pub fn is_running(&self) -> bool {
        matches!(self, RunStatus::Running)
    }

    /// Whether the run stopped at its window's deadline and will resume on the next firing.
    /// Public because the templates ask: a paused run reads as "paused, resumes at the next
    /// window" rather than as an unfinished or failed one.
    pub fn is_paused(&self) -> bool {
        matches!(self, RunStatus::Paused)
    }

    fn parse(text: &str) -> Self {
        match text {
            "succeeded" => RunStatus::Succeeded,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            "paused" => RunStatus::Paused,
            _ => RunStatus::Running,
        }
    }
}

/// One recorded run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: i64,
    pub app: String,
    pub action: String,
    pub dry_run: bool,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub status: RunStatus,
    pub summary: Option<String>,
    /// Machine-readable counts, as JSON.
    pub counts: serde_json::Value,
    pub report_path: Option<String>,
    /// The workflow run this run is a step of, when it is one.
    ///
    /// A workflow run is an ordinary row in this table (app `workflow`, action = the workflow's
    /// name), and each step is an ordinary row of its own app; this is the link between them, so
    /// the workflow's run page can list its steps without a second table and without guessing
    /// from timestamps which runs belong together.
    pub parent_run_id: Option<i64>,
}

/// What a run row is about.
///
/// Rows started out channel-shaped, and the later apps needed to record other things: a probe
/// measures provider streams, and the guide's watchdog checks EPG sources. Rather than reuse
/// `channel_id` for an id from another table (which made a row impossible to read without
/// knowing which action wrote it, and could aim a restore at whatever channel carried that
/// number), the row says what its id belongs to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Subject {
    /// A channel, which every channel operation and every guide action writes.
    #[default]
    Channel,
    /// A provider stream, as probe rows record (`channel_id` carries the stream id).
    Stream,
    /// An EPG source, as the watchdog records (`channel_id` carries the source id).
    EpgSource,
}

impl Subject {
    /// The stored form, which is also the wire form.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Channel => "channel",
            Self::Stream => "stream",
            Self::EpgSource => "epg_source",
        }
    }

    /// How the UI names it. `Stream` is a provider stream rather than a channel's list, which
    /// is why it says so.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Channel => "Channel",
            Self::Stream => "Stream",
            Self::EpgSource => "EPG source",
        }
    }

    /// Parse the stored form. An unknown value is read as a channel: a row written by a newer
    /// build is still a row, and refusing it would hide it from the run page.
    fn parse(text: &str) -> Self {
        match text {
            "stream" => Self::Stream,
            "epg_source" => Self::EpgSource,
            _ => Self::Channel,
        }
    }
}

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One row of a run: what it is about (a channel, a stream, an EPG source) and what was
/// decided for it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunRow {
    pub ordinal: i64,
    /// What `channel_id` is an id of. Channel rows keep the pre-subject meaning, which is what
    /// the DEFAULT on the column backfills.
    pub subject: Subject,
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_group: String,
    pub channel_number: Option<f64>,
    /// Current EPG, before the run.
    pub current_epg_name: Option<String>,
    /// The candidate the run picked.
    pub epg_data_id: Option<i64>,
    pub epg_name: Option<String>,
    pub epg_source: Option<String>,
    pub score: i64,
    pub method: String,
    /// The plugin's own status wording: `HEALED`, `REPLACEMENT_PREVIEW`, ...
    pub status: String,
    pub reason: String,
    /// What the model said, when it was asked. Empty when it was not.
    pub ai_decision: Option<String>,
    /// The channel field a channel operation wrote (`name`, `channel_group_id`, `epg_data_id`,
    /// `hidden_from_output`, `profile_membership:<id>`), empty for a matching row.
    ///
    /// `before_value`/`after_value` are the machine values of that field: an id for a group
    /// or an EPG entry, `none` for an unassigned one, `true`/`false` for a flag. Together with
    /// the identity columns they are what a later `restore` reverses, which is why they are
    /// generic rather than EPG-shaped: phase 2 restores names and groups, not only guides.
    pub field: String,
    pub before_value: Option<String>,
    pub after_value: Option<String>,
}

/// What crosstalk knows about one provider stream that Dispatcharr's `stream_stats` does not.
///
/// `stream_stats` is the shared truth for MEDIA facts (codecs, resolution, bitrates) because
/// players and Dispatcharr itself read it. This table holds what only crosstalk uses: when the
/// stream was last probed, how it turned out, and how fast the provider actually delivered it.
/// It replaces Stream-Mapparr's throughput JSON file and IPTV Checker's loaded-channels file
/// with one row per stream, so `probe` writes and `sort` reads the same evidence.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamHealth {
    pub stream_id: i64,
    /// When the last probe finished, RFC 3339. `None` for a row written by a throughput probe
    /// alone, which measures delivery without inspecting the media.
    pub probed_at: Option<String>,
    /// `alive`, `dead`, `skipped`, `blank`, `frozen`, `silent` or `placeholder`; empty when the
    /// row only carries a throughput measurement.
    pub class: String,
    /// The probe's own failure wording (`Timeout`, `403 Forbidden`, ...).
    pub error_type: Option<String>,
    /// Measured delivery in Mbit/s. `None` means the probe measured nothing, which is never
    /// "fresh" (see `crosstalk_streams::quality::is_probe_fresh`).
    pub throughput_mbps: Option<f64>,
    /// When that measurement was taken, RFC 3339. Separate from `probed_at` because a
    /// throughput probe and a media probe are different runs.
    pub throughput_at: Option<String>,
    /// The host the probe finally reached, for diagnostics.
    pub edge_ip: Option<String>,
    /// How many probes in a row have failed. A stream that fails once and recovers does not
    /// climb to "dead" without a run of failures.
    pub consecutive_failures: i64,
}

/// One AI request and its answer, kept for auditing why a row was accepted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiDecision {
    pub row_ordinal: i64,
    pub provider: String,
    pub model: String,
    pub latency_ms: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub request: serde_json::Value,
    pub response: serde_json::Value,
    /// Every chain link asked, in order: `{provider, model, result, ...}`. The last entry
    /// with a response decided; earlier ones were undecided or failed. Empty for rows
    /// written before the chain existed, which asked exactly one link.
    #[serde(default)]
    pub attempts: serde_json::Value,
}

impl Default for AiDecision {
    fn default() -> Self {
        Self {
            row_ordinal: 0,
            provider: String::new(),
            model: String::new(),
            latency_ms: 0,
            input_tokens: 0,
            output_tokens: 0,
            request: serde_json::Value::Null,
            response: serde_json::Value::Null,
            attempts: serde_json::json!([]),
        }
    }
}

/// The store handle. Cloneable and cheap: the connection is shared.
#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").finish_non_exhaustive()
    }
}

impl Store {
    /// Open (or create) the database at `path` and migrate it.
    ///
    /// For the daemon: a run left `running` by a crash or a restart can never finish, so
    /// opening retires those rows. A SECOND process must not use this (see
    /// [`Store::open_shared`]): retiring from the CLI would mark the daemon's live run
    /// failed while it is still writing.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, true)
    }

    /// Open the database from a second process (the one-shot CLI), without retiring.
    ///
    /// Same schema migration as [`Store::open`], but `running` rows are left alone: they
    /// belong to the daemon's live runs, and only the daemon's next [`Store::open`] may
    /// retire them. Callers must refuse to start work an already-`running` row covers (see
    /// [`Store::running_runs`]): the in-process one-run-per-app guard cannot see across
    /// processes, and SQLite serialises writers but does not stop two processes from
    /// running the same app at once. (`WAL` + `busy_timeout` below are what let the two
    /// processes share the file at all.)
    pub fn open_shared(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, false)
    }

    fn open_with(path: impl AsRef<Path>, retire_running: bool) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let connection = Connection::open(path)
            .with_context(|| format!("opening the run store at {}", path.display()))?;
        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        store.migrate(retire_running)?;
        Ok(store)
    }

    /// An in-memory store, for tests.
    pub fn in_memory() -> Result<Self> {
        let store = Self {
            connection: Arc::new(Mutex::new(Connection::open_in_memory()?)),
        };
        store.migrate(true)?;
        Ok(store)
    }

    fn with_connection<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        // Recover from a poisoned lock rather than panicking: a poisoned store would take the
        // whole daemon down on the next write, long after whatever panicked.
        let guard = self
            .connection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f(&guard)
    }

    /// Create the schema. Idempotent, so it runs on every open.
    ///
    /// `retire_running` is true for the daemon's own open and false for a second
    /// process's [`Store::open_shared`]: only the daemon may declare another run dead.
    fn migrate(&self, retire_running: bool) -> Result<()> {
        self.with_connection(|connection| {
            connection.execute_batch(
                r#"
                PRAGMA journal_mode = WAL;
                PRAGMA foreign_keys = ON;
                PRAGMA busy_timeout = 5000;

                CREATE TABLE IF NOT EXISTS runs (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    app          TEXT    NOT NULL,
                    action       TEXT    NOT NULL,
                    dry_run      INTEGER NOT NULL,
                    started_at   TEXT    NOT NULL,
                    finished_at  TEXT,
                    status       TEXT    NOT NULL,
                    summary      TEXT,
                    counts       TEXT    NOT NULL DEFAULT '{}',
                    report_path  TEXT,
                    -- The workflow run a step run belongs to (NULL for a run started on its
                    -- own). No foreign key: a parent can be pruned before its steps and the
                    -- child must survive as history either way.
                    parent_run_id INTEGER
                );
                CREATE INDEX IF NOT EXISTS runs_started ON runs(started_at DESC);
                CREATE INDEX IF NOT EXISTS runs_app ON runs(app, started_at DESC);

                CREATE TABLE IF NOT EXISTS run_rows (
                    run_id           INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    ordinal          INTEGER NOT NULL,
                    channel_id       INTEGER NOT NULL,
                    channel_name     TEXT    NOT NULL,
                    channel_group    TEXT    NOT NULL DEFAULT '',
                    channel_number   REAL,
                    current_epg_name TEXT,
                    epg_data_id      INTEGER,
                    epg_name         TEXT,
                    epg_source       TEXT,
                    score            INTEGER NOT NULL DEFAULT 0,
                    method           TEXT    NOT NULL DEFAULT '',
                    status           TEXT    NOT NULL DEFAULT '',
                    reason           TEXT    NOT NULL DEFAULT '',
                    ai_decision      TEXT,
                    field            TEXT    NOT NULL DEFAULT '',
                    before_value     TEXT,
                    after_value      TEXT,
                    -- What channel_id is an id of: channel (default), stream, epg_source.
                    subject          TEXT    NOT NULL DEFAULT 'channel',
                    PRIMARY KEY (run_id, ordinal)
                );
                CREATE INDEX IF NOT EXISTS run_rows_channel ON run_rows(run_id, channel_id);

                CREATE TABLE IF NOT EXISTS ai_decisions (
                    run_id        INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    row_ordinal   INTEGER NOT NULL,
                    provider      TEXT    NOT NULL,
                    model         TEXT    NOT NULL,
                    latency_ms    INTEGER NOT NULL DEFAULT 0,
                    input_tokens  INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    request       TEXT    NOT NULL DEFAULT '{}',
                    response      TEXT    NOT NULL DEFAULT '{}',
                    attempts      TEXT    NOT NULL DEFAULT '[]',
                    PRIMARY KEY (run_id, row_ordinal)
                );

                CREATE TABLE IF NOT EXISTS settings_history (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    app         TEXT NOT NULL,
                    changed_at  TEXT NOT NULL,
                    --  is reserved in SQLite.
                    settings    TEXT NOT NULL
                );

                -- Where a long job keeps its place between schedule windows. Keyed by the action
                -- rather than by the run: the whole point is that tonight's run is a different
                -- run from last night's and still continues it. `stream_health` and this table
                -- are the two pieces of state a sub-app owns that outlive a run.
                CREATE TABLE IF NOT EXISTS job_checkpoints (
                    app        TEXT NOT NULL,
                    action     TEXT NOT NULL,
                    state      TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (app, action)
                );

                -- What crosstalk knows about a provider stream. Idempotent: the table is
                -- created on every open and never altered, so a store from an earlier build
                -- simply grows it. `stream_id` is Dispatcharr's, not a row of ours.
                CREATE TABLE IF NOT EXISTS stream_health (
                    stream_id            INTEGER PRIMARY KEY,
                    probed_at            TEXT,
                    class                TEXT    NOT NULL DEFAULT '',
                    error_type           TEXT,
                    throughput_mbps      REAL,
                    throughput_at        TEXT,
                    edge_ip              TEXT,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0
                );

                -- Shared channel identities (phase 3 step 6): one row per distinct resolved
                -- input, namespaced by `data_version` (merged-data provenance sha +
                -- decisions sha + engine version + the engine settings identities
                -- depend on, see `UnifiedEngine::identity_version`). A data, rule or
                -- settings change yields a new version, so stale rows are never read
                -- — only pruned.
                CREATE TABLE IF NOT EXISTS identity_cache (
                    input_key     TEXT NOT NULL,
                    data_version  TEXT NOT NULL,
                    identity_json TEXT NOT NULL,
                    cached_at     TEXT NOT NULL,
                    PRIMARY KEY (input_key, data_version)
                );
                CREATE INDEX IF NOT EXISTS identity_cache_version ON identity_cache(data_version);
                "#,
            )?;
            // A run left `running` by a crash or a restart can never finish, and pruning
            // skips running rows, so it would otherwise show as live forever. Only the
            // daemon's own open retires: a second process cannot tell a live run from a
            // stale one, and retiring a live run rewrites history under it.
            if retire_running {
                connection.execute(
                    "UPDATE runs SET status = 'failed', \
                     summary = COALESCE(summary, 'the daemon restarted while this run was in progress'), \
                     finished_at = COALESCE(finished_at, ?1) WHERE status = 'running'",
                    params![chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)],
                )?;
            }
            add_run_row_columns(connection)?;
            add_run_columns(connection)?;
            add_ai_decision_columns(connection)?;
            // Before the index below: the rename turns `janitor` rows into `guide` rows, and
            // must not be the statement that trips over it.
            rename_recorded_history(connection)?;
            // One live GUARDED run per app, across processes: the in-memory busy map stops two
            // runs in THIS process, but the CLI is another process on the same file. A partial
            // index (no extra row when nothing is running), created after the retire above
            // so a restarted daemon never trips on its own stale rows.
            //
            // `unguarded` is what lets a read-only status action run while another run of the
            // app is live: it is still recorded, so its output lands on a run page like every
            // other action's, but it takes no slot. An index built before that column existed
            // has to be replaced — `IF NOT EXISTS` would keep the old definition — and the
            // replacement only happens ONCE: dropping and re-creating it on every open would
            // leave a moment with no guard at all, which is long enough for two processes
            // opening together to insert competing `running` rows. The swap runs in one
            // transaction under the write lock (`busy_timeout`), so a daemon and a CLI opening
            // together serialize: the second sees the new definition and does nothing, and
            // `IF EXISTS` keeps a DROP that lost the race from failing.
            let index = (|| -> Result<()> {
                if index_definition(connection, "runs_one_running_per_app")?
                    .is_some_and(|sql| sql.contains("unguarded"))
                {
                    return Ok(());
                }
                let transaction = connection.unchecked_transaction()?;
                transaction.execute_batch(
                    "DROP INDEX IF EXISTS runs_one_running_per_app; \
                     CREATE UNIQUE INDEX IF NOT EXISTS runs_one_running_per_app ON \
                     runs(app) WHERE status = 'running' AND unguarded = 0",
                )?;
                transaction.commit()?;
                Ok(())
            })();
            match index {
                Ok(()) => {}
                // A database from before the index can hold two stale `running` rows for one
                // app (a crash while the CLI and the daemon both ran it). Only the daemon may
                // retire them, so a shared open goes without the index until then; the CLI
                // still refuses to start over a `running` row (`running_runs`).
                Err(error) if !retire_running => tracing::warn!(
                    "the one-run-per-app index cannot be created until the daemon retires \
                     stale running rows: {error}"
                ),
                Err(error) => return Err(error),
            }
            Ok(())
        })
    }

    /// Start a run and return its id.
    ///
    /// Takes the app's one-live-run slot: the partial unique index refuses a second `running`
    /// row for the same app. A read-only status-like action starts with
    /// [`Store::start_unguarded_run`] instead.
    pub fn start_run(&self, app: &str, action: &str, dry_run: bool) -> Result<i64> {
        self.insert_run(app, action, dry_run, false, None)
    }

    /// Start a run that does not take the app's one-live-run slot.
    ///
    /// For an action that only reads the run store (`status`, the dashboard's own questions):
    /// refusing it while a run is live would mean the operator cannot see the run that is
    /// blocking them, which is exactly when they need to. It is still recorded, so a caller can
    /// follow it to a run page like any other, and two unguarded runs may overlap because
    /// neither writes anything the other reads.
    pub fn start_unguarded_run(&self, app: &str, action: &str, dry_run: bool) -> Result<i64> {
        self.insert_run(app, action, dry_run, true, None)
    }

    /// Insert one run row: the one place the column list and the slot flags live, so a new column
    /// cannot be forgotten by one of the callers.
    ///
    /// `parent_run_id` is the workflow run a step belongs to (see [`crate::workflow`]); the
    /// runner passes it from [`crate::runner::RunOptions::parent_run`], which is how a step run
    /// stays an ordinary run of its own app with an ordinary slot rule.
    pub(crate) fn insert_run(
        &self,
        app: &str,
        action: &str,
        dry_run: bool,
        unguarded: bool,
        parent_run_id: Option<i64>,
    ) -> Result<i64> {
        let started = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO runs (app, action, dry_run, started_at, status, counts, unguarded, \
                 parent_run_id) VALUES (?1, ?2, ?3, ?4, ?5, '{}', ?6, ?7)",
                params![
                    app,
                    action,
                    i64::from(dry_run),
                    started,
                    RunStatus::Running.as_str(),
                    i64::from(unguarded),
                    parent_run_id,
                ],
            )?;
            Ok(connection.last_insert_rowid())
        })
    }

    /// Mark a run finished.
    pub fn finish_run(
        &self,
        run_id: i64,
        status: RunStatus,
        summary: Option<&str>,
        counts: &serde_json::Value,
        report_path: Option<&str>,
    ) -> Result<()> {
        let finished = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let counts = serde_json::to_string(counts)?;
        self.with_connection(|connection| {
            connection.execute(
                "UPDATE runs SET finished_at = ?2, status = ?3, summary = ?4, counts = ?5, \
                 report_path = ?6 WHERE id = ?1",
                params![
                    run_id,
                    finished,
                    status.as_str(),
                    summary,
                    counts,
                    report_path
                ],
            )?;
            Ok(())
        })
    }

    /// Append rows to a run, in one transaction.
    pub fn append_rows(&self, run_id: i64, rows: &[RunRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.with_connection(|connection| {
            let transaction = connection.unchecked_transaction()?;
            {
                let mut statement = transaction.prepare(
                    "INSERT OR REPLACE INTO run_rows (run_id, ordinal, channel_id, channel_name, \
                     channel_group, channel_number, current_epg_name, epg_data_id, epg_name, \
                     epg_source, score, method, status, reason, ai_decision, field, \
                     before_value, after_value, subject) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, \
                     ?16, ?17, ?18, ?19)",
                )?;
                for row in rows {
                    statement.execute(params![
                        run_id,
                        row.ordinal,
                        row.channel_id,
                        row.channel_name,
                        row.channel_group,
                        row.channel_number,
                        row.current_epg_name,
                        row.epg_data_id,
                        row.epg_name,
                        row.epg_source,
                        row.score,
                        row.method,
                        row.status,
                        row.reason,
                        row.ai_decision,
                        row.field,
                        row.before_value,
                        row.after_value,
                        row.subject.as_str(),
                    ])?;
                }
            }
            transaction.commit()?;
            Ok(())
        })
    }

    /// Record one AI request and its answer.
    pub fn record_ai_decision(&self, run_id: i64, decision: &AiDecision) -> Result<()> {
        let request = serde_json::to_string(&decision.request)?;
        let response = serde_json::to_string(&decision.response)?;
        let attempts = serde_json::to_string(&decision.attempts)?;
        self.with_connection(|connection| {
            connection.execute(
                "INSERT OR REPLACE INTO ai_decisions (run_id, row_ordinal, provider, model, \
                 latency_ms, input_tokens, output_tokens, request, response, attempts) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    run_id,
                    decision.row_ordinal,
                    decision.provider,
                    decision.model,
                    decision.latency_ms,
                    decision.input_tokens,
                    decision.output_tokens,
                    request,
                    response,
                    attempts,
                ],
            )?;
            Ok(())
        })
    }

    /// Record a settings change, so a run can be explained later.
    pub fn record_settings_change(&self, app: &str, values: &serde_json::Value) -> Result<()> {
        let changed = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let values = serde_json::to_string(values)?;
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO settings_history (app, changed_at, settings) VALUES (?1, ?2, ?3)",
                params![app, changed, values],
            )?;
            Ok(())
        })
    }

    /// The most recent runs, newest first.
    pub fn recent_runs(&self, limit: usize) -> Result<Vec<RunRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(&format!(
                "SELECT {RUN_COLUMNS} FROM runs ORDER BY started_at DESC, id DESC LIMIT ?1"
            ))?;
            let rows = statement.query_map(params![limit as i64], map_run)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
    }

    /// Runs for one app, newest first.
    pub fn recent_runs_for(&self, app: &str, limit: usize) -> Result<Vec<RunRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(&format!(
                "SELECT {RUN_COLUMNS} FROM runs WHERE app = ?2 \
                 ORDER BY started_at DESC, id DESC LIMIT ?1"
            ))?;
            let rows = statement.query_map(params![limit as i64, app], map_run)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
    }

    /// The runs that were steps of one workflow run, in the order they ran.
    pub fn child_runs(&self, parent_run_id: i64) -> Result<Vec<RunRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(&format!(
                "SELECT {RUN_COLUMNS} FROM runs WHERE parent_run_id = ?1 ORDER BY started_at, id"
            ))?;
            let rows = statement.query_map(params![parent_run_id], map_run)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
    }

    /// The most recent SUCCEEDED run of one action for one app.
    ///
    /// This is how a later action finds the result set an earlier one produced (the guide's
    /// cleanup actions read the last `scan`), which is why it is keyed by app AND action and
    /// requires the run to have finished cleanly: a cancelled or failed run may hold partial
    /// rows, and a partial result set would silently skip channels the run never reached.
    /// There is no age limit — the planning step re-reads the current state of every channel
    /// anyway, so a stale result set is safe, only out of date.
    pub fn last_succeeded_run(&self, app: &str, action: &str) -> Result<Option<RunRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(&format!(
                "SELECT {RUN_COLUMNS} FROM runs WHERE app = ?1 AND action = ?2 AND status = ?3 \
                 ORDER BY started_at DESC, id DESC LIMIT 1"
            ))?;
            statement
                .query_row(params![app, action, RunStatus::Succeeded.as_str()], map_run)
                .optional()
                .map_err(Into::into)
        })
    }

    /// The live (`running`) GUARDED runs of one app, oldest first.
    ///
    /// The cross-process guard: a second process (the one-shot CLI) refuses to start an
    /// app that has one of these, because the in-process busy map cannot see it. A row
    /// here is either the daemon's live run or a stale row from a crash, which only the
    /// daemon's next [`Store::open`] retires — so the caller reports both cases and lets
    /// the operator decide, rather than retiring what might be alive.
    ///
    /// Unguarded rows (a `status` query in flight) are excluded: they hold no slot, so
    /// reporting them as "already running" would refuse work nothing is blocking.
    pub fn running_runs(&self, app: &str) -> Result<Vec<RunRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(&format!(
                "SELECT {RUN_COLUMNS} FROM runs WHERE app = ?1 AND status = ?2 AND unguarded = 0 \
                 ORDER BY started_at, id"
            ))?;
            let rows = statement.query_map(params![app, RunStatus::Running.as_str()], map_run)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
    }

    /// One run by id.
    pub fn run(&self, run_id: i64) -> Result<Option<RunRecord>> {
        self.with_connection(|connection| {
            let mut statement =
                connection.prepare(&format!("SELECT {RUN_COLUMNS} FROM runs WHERE id = ?1"))?;
            statement
                .query_row(params![run_id], map_run)
                .optional()
                .map_err(Into::into)
        })
    }

    /// The rows of a run, in the order the run produced them.
    pub fn rows(&self, run_id: i64) -> Result<Vec<RunRow>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(&format!(
                "SELECT {RUN_ROW_COLUMNS} FROM run_rows WHERE run_id = ?1 ORDER BY ordinal"
            ))?;
            let rows = statement.query_map(params![run_id], map_run_row)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
    }

    /// One run's rows, limited to the FIELDS asked for.
    ///
    /// A probe run at 50k streams writes an order of magnitude more stream rows than the
    /// per-channel verdicts its readers want, so `triage` and `restore` read the two field
    /// names instead of the whole run: same rows, a fraction of the memory.
    pub fn rows_of_fields(&self, run_id: i64, fields: &[&str]) -> Result<Vec<RunRow>> {
        if fields.is_empty() {
            return Ok(Vec::new());
        }
        self.with_connection(|connection| {
            let sql = format!(
                "SELECT {RUN_ROW_COLUMNS} FROM run_rows WHERE run_id = ? AND field IN ({}) \
                 ORDER BY ordinal",
                marks(fields.len()),
            );
            let mut statement = connection.prepare(&sql)?;
            let mut values: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(fields.len() + 1);
            values.push(&run_id);
            values.extend(fields.iter().map(|field| field as &dyn rusqlite::ToSql));
            let rows = statement.query_map(rusqlite::params_from_iter(values), map_run_row)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
    }

    /// The rows of a set of FIELDS from the newest FINISHED runs of some ACTIONS, oldest run
    /// first — what a `restore` reads to work out which recorded changes are still in effect.
    ///
    /// `fields` is the channel fields to read (`name`, `channel_group_id`), and `actions` the
    /// runs that wrote them (`streams.triage_apply`); every other row is filtered out in SQL,
    /// because a triage run at 20k channels carries rows a restore has no use for.
    ///
    /// Three exclusions, each for a different reason:
    ///
    ///   * a DRY RUN's rows are plans, not writes, and reversing one would write a value the
    ///     channel never had;
    ///   * a RUNNING run may still be writing, so it is not history yet;
    ///   * rows whose own `status` is not `ok` never landed — a restore may be recorded from a
    ///     cancelled run's confirmed rows, which is why the RUN's status is not narrowed to
    ///     `succeeded`. `RunStatus::Paused` and `RunStatus::Failed` runs are in for the same
    ///     reason; the row's own status decides.
    ///
    /// `runs` caps the scan at the newest N runs, so a busy install does not read a year of
    /// rows to find one name. `run_history_days` prunes the table underneath; a change older
    /// than the cap is simply not found, and the caller falls back to what the name itself
    /// says.
    pub fn field_history(
        &self,
        app: &str,
        actions: &[&str],
        fields: &[&str],
        runs: usize,
    ) -> Result<Vec<RunRow>> {
        if actions.is_empty() || fields.is_empty() || runs == 0 {
            return Ok(Vec::new());
        }
        self.with_connection(|connection| {
            let sql = format!(
                "SELECT {RUN_ROW_COLUMNS} FROM run_rows WHERE subject = 'channel' \
                 AND field IN ({}) AND run_id IN (SELECT id FROM runs WHERE app = ? \
                 AND dry_run = 0 AND status != 'running' AND action IN ({}) \
                 ORDER BY started_at DESC, id DESC LIMIT ?) ORDER BY run_id, ordinal",
                marks(fields.len()),
                marks(actions.len()),
            );
            let mut statement = connection.prepare(&sql)?;
            // Positional parameters follow the SQL text: the field list, then the app, the
            // action list and the run cap.
            let limit = runs as i64;
            let mut values: Vec<&dyn rusqlite::ToSql> =
                Vec::with_capacity(fields.len() + 3 + actions.len());
            values.extend(fields.iter().map(|field| field as &dyn rusqlite::ToSql));
            values.push(&app);
            values.extend(actions.iter().map(|action| action as &dyn rusqlite::ToSql));
            values.push(&limit);
            let rows = statement.query_map(rusqlite::params_from_iter(values), map_run_row)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
    }

    /// How many AI decisions are recorded for one run.
    pub fn ai_decision_count(&self, run_id: i64) -> Result<u64> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM ai_decisions WHERE run_id = ?1",
                    params![run_id],
                    |row| row.get(0),
                )
                .map_err(Into::into)
        })
    }

    /// The AI decision recorded for one row, if any.
    pub fn ai_decision(&self, run_id: i64, ordinal: i64) -> Result<Option<AiDecision>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT row_ordinal, provider, model, latency_ms, input_tokens, output_tokens, \
                 request, response, attempts FROM ai_decisions WHERE run_id = ?1 AND row_ordinal = ?2",
            )?;
            statement
                .query_row(params![run_id, ordinal], |row| {
                    let request: String = row.get(6)?;
                    let response: String = row.get(7)?;
                    let attempts: String = row.get(8)?;
                    Ok(AiDecision {
                        row_ordinal: row.get(0)?,
                        provider: row.get(1)?,
                        model: row.get(2)?,
                        latency_ms: row.get(3)?,
                        input_tokens: row.get(4)?,
                        output_tokens: row.get(5)?,
                        request: serde_json::from_str(&request).unwrap_or_default(),
                        response: serde_json::from_str(&response).unwrap_or_default(),
                        attempts: serde_json::from_str(&attempts)
                            .unwrap_or(serde_json::json!([])),
                    })
                })
                .optional()
                .map_err(Into::into)
        })
    }

    /// Delete finished runs older than `keep_days`, keeping the most recent `keep_minimum`
    /// per app. Returns how many runs were removed.
    ///
    /// Unguarded runs (a `status` query in flight, then finished) are history of a question,
    /// not of work: they are pruned by age alone and never count toward the minimum, so an
    /// operator who polls `status` cannot evict the real runs the minimum is there to keep.
    pub fn prune(&self, keep_days: u32, keep_minimum: usize) -> Result<usize> {
        if keep_days == 0 {
            return Ok(0);
        }
        let cutoff = (chrono::Utc::now() - chrono::Duration::days(i64::from(keep_days)))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        self.with_connection(|connection| {
            let removed = connection.execute(
                "DELETE FROM runs WHERE status != 'running' AND started_at < ?1 AND ( \
                     unguarded != 0 OR id NOT IN ( \
                     SELECT id FROM ( \
                         SELECT id, ROW_NUMBER() OVER (PARTITION BY app \
                             ORDER BY started_at DESC, id DESC) AS position \
                         FROM runs WHERE status != 'running' AND unguarded = 0 \
                     ) WHERE position <= ?2 \
                 )) ",
                params![cutoff, keep_minimum as i64],
            )?;
            Ok(removed)
        })
    }

    /// How many runs are stored.
    pub fn run_count(&self) -> Result<u64> {
        self.with_connection(|connection| {
            connection
                .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))
                .map_err(Into::into)
        })
    }

    /// Save a job's progress, replacing whatever was there.
    ///
    /// Keyed by app and action rather than by run: the next firing of the same schedule is a
    /// different run and must still find it. The state is the action's own JSON — the store
    /// never looks inside, because only the action knows what "where I got to" means.
    pub fn save_checkpoint(
        &self,
        app: &str,
        action: &str,
        state: &serde_json::Value,
    ) -> Result<()> {
        let updated = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let state = serde_json::to_string(state)?;
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO job_checkpoints (app, action, state, updated_at) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(app, action) DO UPDATE SET state = excluded.state, \
                 updated_at = excluded.updated_at",
                params![app, action, state, updated],
            )?;
            Ok(())
        })
    }

    /// A job's saved progress, or `None` when it has none.
    ///
    /// A state that no longer parses is dropped rather than failing the run: a checkpoint is an
    /// optimisation over starting again, never a reason not to run.
    pub fn load_checkpoint(&self, app: &str, action: &str) -> Result<Option<serde_json::Value>> {
        let state: Option<String> = self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT state FROM job_checkpoints WHERE app = ?1 AND action = ?2",
                    params![app, action],
                    |row| row.get(0),
                )
                .optional()
                .map_err(Into::into)
        })?;
        Ok(match state {
            Some(text) => match serde_json::from_str(&text) {
                Ok(value) => Some(value),
                Err(error) => {
                    tracing::warn!(
                        "discarding the {app}.{action} checkpoint: its state does not parse: \
                         {error}"
                    );
                    None
                }
            },
            None => None,
        })
    }

    /// When a job's progress was last saved, for the run page and the status action.
    pub fn checkpoint_updated_at(&self, app: &str, action: &str) -> Result<Option<String>> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT updated_at FROM job_checkpoints WHERE app = ?1 AND action = ?2",
                    params![app, action],
                    |row| row.get(0),
                )
                .optional()
                .map_err(Into::into)
        })
    }

    /// Discard a job's saved progress. `true` when there was some.
    ///
    /// Called when a resumed job finishes its whole backlog, and when an operator asks for a
    /// fresh start (`crosstalk run --fresh`, the "start fresh" checkbox).
    pub fn clear_checkpoint(&self, app: &str, action: &str) -> Result<bool> {
        self.with_connection(|connection| {
            let removed = connection.execute(
                "DELETE FROM job_checkpoints WHERE app = ?1 AND action = ?2",
                params![app, action],
            )?;
            Ok(removed > 0)
        })
    }

    /// A cached identity for (`input_key`, `data_version`), or `None`.
    ///
    /// Rows are namespaced by data version, so a merged-data or rule change never
    /// reads a stale identity: it simply misses and re-resolves. A row that no
    /// longer parses is dropped rather than failing the run (same rule as
    /// [`Store::load_checkpoint`]).
    pub fn identity_cache_get(
        &self,
        input_key: &str,
        data_version: &str,
    ) -> Result<Option<String>> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT identity_json FROM identity_cache WHERE input_key = ?1 AND data_version = ?2",
                    params![input_key, data_version],
                    |row| row.get(0),
                )
                .optional()
                .map_err(Into::into)
        })
    }

    /// [`identity_cache_get`](Self::identity_cache_get) for many keys under one lock:
    /// the rows that exist, by key. A run warms thousands of identities at once, and
    /// one statement per key through the shared connection would serialize them.
    pub fn identity_cache_get_many(
        &self,
        input_keys: &[String],
        data_version: &str,
    ) -> Result<std::collections::HashMap<String, String>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare_cached(
                "SELECT identity_json FROM identity_cache WHERE input_key = ?1 AND data_version = ?2",
            )?;
            let mut found = std::collections::HashMap::new();
            for key in input_keys {
                let json: Option<String> = statement
                    .query_row(params![key, data_version], |row| row.get(0))
                    .optional()?;
                if let Some(json) = json {
                    found.insert(key.clone(), json);
                }
            }
            Ok(found)
        })
    }

    /// [`identity_cache_put`](Self::identity_cache_put) for many rows in ONE
    /// transaction (one commit, not one fsync per identity).
    pub fn identity_cache_put_many(
        &self,
        rows: &[(String, String)],
        data_version: &str,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let cached_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        self.with_connection(|connection| {
            let transaction = connection.unchecked_transaction()?;
            {
                let mut statement = transaction.prepare_cached(
                    "INSERT INTO identity_cache (input_key, data_version, identity_json, cached_at) \
                     VALUES (?1, ?2, ?3, ?4) ON CONFLICT(input_key, data_version) DO UPDATE SET \
                     identity_json = excluded.identity_json, cached_at = excluded.cached_at",
                )?;
                for (key, json) in rows {
                    statement.execute(params![key, data_version, json, cached_at])?;
                }
            }
            transaction.commit()?;
            Ok(())
        })
    }

    /// Store one resolved identity, replacing any row for the same key + version.
    pub fn identity_cache_put(
        &self,
        input_key: &str,
        data_version: &str,
        identity_json: &str,
    ) -> Result<()> {
        let cached_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO identity_cache (input_key, data_version, identity_json, cached_at) \
                 VALUES (?1, ?2, ?3, ?4) ON CONFLICT(input_key, data_version) DO UPDATE SET \
                 identity_json = excluded.identity_json, cached_at = excluded.cached_at",
                params![input_key, data_version, identity_json, cached_at],
            )?;
            Ok(())
        })
    }

    /// Drop every cached identity from a data version other than `keep_version`.
    ///
    /// Returns the number of rows removed. Called after a resolve run warms the
    /// current version, so one data/rule/settings change does not grow the table
    /// forever. Every app shares the one `[matching]` engine, hence one version;
    /// rows of any other version are stale by definition.
    pub fn identity_cache_prune(&self, keep_version: &str) -> Result<u64> {
        self.with_connection(|connection| {
            let removed = connection.execute(
                "DELETE FROM identity_cache WHERE data_version != ?1",
                params![keep_version],
            )?;
            Ok(removed as u64)
        })
    }

    /// Record one stream's probe outcome, replacing whatever was there.
    ///
    /// Written by `probe` once per stream and read by `sort`, so the row is the stream's whole
    /// health in one place rather than a merge of old and new columns: a probe that measured
    /// nothing must clear the previous measurement, or a stream that has been failing for hours
    /// would keep sorting on the rate it had this morning.
    pub fn upsert_stream_health(&self, health: &StreamHealth) -> Result<()> {
        self.upsert_stream_health_many(std::slice::from_ref(health))
            .map(|_| ())
    }

    /// [`Store::upsert_stream_health`] for many streams in ONE transaction: a probe batch, or
    /// a whole run's worth, costs one commit instead of one per stream.
    pub fn upsert_stream_health_many(&self, rows: &[StreamHealth]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.with_connection(|connection| {
            let transaction = connection.unchecked_transaction()?;
            {
                let mut statement = transaction.prepare_cached(
                    "INSERT INTO stream_health (stream_id, probed_at, class, error_type, \
                     throughput_mbps, throughput_at, edge_ip, consecutive_failures) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                     ON CONFLICT(stream_id) DO UPDATE SET \
                         probed_at = excluded.probed_at, \
                         class = excluded.class, \
                         error_type = excluded.error_type, \
                         throughput_mbps = excluded.throughput_mbps, \
                         throughput_at = excluded.throughput_at, \
                         edge_ip = excluded.edge_ip, \
                         consecutive_failures = excluded.consecutive_failures",
                )?;
                for health in rows {
                    statement.execute(params![
                        health.stream_id,
                        health.probed_at,
                        health.class,
                        health.error_type,
                        health.throughput_mbps,
                        health.throughput_at,
                        health.edge_ip,
                        health.consecutive_failures,
                    ])?;
                }
            }
            transaction.commit()?;
            Ok(rows.len())
        })
    }

    /// One stream's health, or `None` when it has never been probed.
    pub fn stream_health(&self, stream_id: i64) -> Result<Option<StreamHealth>> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT stream_id, probed_at, class, error_type, throughput_mbps, \
                     throughput_at, edge_ip, consecutive_failures FROM stream_health \
                     WHERE stream_id = ?1",
                    params![stream_id],
                    map_stream_health,
                )
                .optional()
                .map_err(Into::into)
        })
    }

    /// Every stream's health, keyed by stream id.
    ///
    /// One query for a whole sort run: at ~50k streams this is a few megabytes, while a lookup
    /// per stream inside the sort would be a query per stream.
    pub fn stream_health_map(&self) -> Result<std::collections::HashMap<i64, StreamHealth>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT stream_id, probed_at, class, error_type, throughput_mbps, \
                 throughput_at, edge_ip, consecutive_failures FROM stream_health",
            )?;
            let rows = statement.query_map([], map_stream_health)?;
            let mut out = std::collections::HashMap::new();
            for row in rows {
                let health = row?;
                out.insert(health.stream_id, health);
            }
            Ok(out)
        })
    }

    /// Backdate a run's `started_at`, for tests that seed old history.
    #[cfg(test)]
    pub(crate) fn set_started_at_for_test(&self, run_id: i64, started_at: &str) -> Result<()> {
        self.with_connection(|connection| {
            connection
                .execute(
                    "UPDATE runs SET started_at = ?1 WHERE id = ?2",
                    params![started_at, run_id],
                )
                .map(|_| ())
                .map_err(Into::into)
        })
    }

    /// The `busy_timeout` in effect on this connection, for tests.
    #[cfg(test)]
    pub(crate) fn busy_timeout_for_test(&self) -> Result<i64> {
        self.with_connection(|connection| {
            connection
                .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
                .map_err(Into::into)
        })
    }
}

/// The SQL of a named index, or `None` when it does not exist.
///
/// Used to tell an index built by an older release from this build's: the definition is the
/// only durable record of which columns an index covers.
fn index_definition(connection: &Connection, name: &str) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
            params![name],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map(Option::flatten)
        .map_err(Into::into)
}

/// Whether this SQLite error is a lost `ADD COLUMN` race: two processes opening together
/// both saw the column missing, and the second's `ALTER TABLE` landed after the first's.
fn is_duplicate_column(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(_, Some(message))
            if message.contains("duplicate column name")
    )
}

/// Add one column when it is missing, tolerating a concurrent migrator that added it first.
fn add_column(connection: &Connection, table: &str, name: &str, definition: &str) -> Result<()> {
    if let Err(error) = connection.execute_batch(&format!(
        "ALTER TABLE {table} ADD COLUMN {name} {definition}"
    )) {
        if is_duplicate_column(&error) {
            return Ok(());
        }
        return Err(anyhow::anyhow!("adding {table}.{name}: {error:#}"));
    }
    Ok(())
}

/// Add the columns the run table gained after the first release.
///
/// `unguarded` marks a run that holds no slot: a read-only status-like action that must be
/// allowed to run while another run of the same app is live. The DEFAULT is the backfill — every
/// row written before this column existed was a guarded run — so the partial index can be built
/// on top of it without a pass over history.
fn add_run_columns(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(runs)")?;
    let existing: std::collections::HashSet<String> = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    for (name, definition) in [
        ("unguarded", "INTEGER NOT NULL DEFAULT 0"),
        // The workflow link. Nullable, so every row written before it existed reads back as a
        // run that was started on its own, which is what it was.
        ("parent_run_id", "INTEGER"),
    ] {
        if !existing.contains(name) {
            // A daemon and a CLI opening together can both see the column missing: the second
            // `ALTER TABLE` then fails with "duplicate column name", which means the work is
            // done rather than that something went wrong.
            add_column(connection, "runs", name, definition)?;
        }
    }
    Ok(())
}

/// Add the columns channel operations and the run-row subject record, when this database
/// predates them.
///
/// `CREATE TABLE IF NOT EXISTS` leaves an existing `run_rows` alone, so a store written by an
/// earlier build would reject the new inserts. The columns are nullable or defaulted, so an
/// old row reads back as "no channel-operation data" and as a channel row, which is what it
/// was — except for the watchdog's rows, which the backfill below re-labels as EPG sources.
fn add_run_row_columns(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(run_rows)")?;
    let existing: std::collections::HashSet<String> = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    for (name, definition) in [
        ("field", "TEXT NOT NULL DEFAULT ''"),
        ("before_value", "TEXT"),
        ("after_value", "TEXT"),
        // The DEFAULT is the backfill for every ordinary row: before this column existed all
        // of them were about a channel. The watchdog's rows are the exception, below.
        ("subject", "TEXT NOT NULL DEFAULT 'channel'"),
    ] {
        if !existing.contains(name) {
            // Same lost-race tolerance as `add_run_columns`: two processes migrating together
            // must not fail the second open.
            add_column(connection, "run_rows", name, definition)?;
        }
    }
    // Watchdog rows predate the column: they carry an EPG SOURCE id in `channel_id` and mark
    // themselves with `field = 'epg_source'` (the field they observed, not one they wrote), so
    // the DEFAULT would mislabel them as channels. Runs once, in the same pass that adds the
    // column — the `subject != 'epg_source'` guard is what makes it safe to re-run anyway.
    if !existing.contains("subject") {
        connection.execute(
            "UPDATE run_rows SET subject = 'epg_source' \
             WHERE field = 'epg_source' AND subject != 'epg_source'",
            [],
        )?;
    }
    Ok(())
}

/// Add the columns the AI audit gained after the first release.
///
/// `CREATE TABLE IF NOT EXISTS` leaves an existing `ai_decisions` alone, so a store written
/// by an earlier build would reject the new inserts. The column is defaulted, so an old row
/// reads back as "one link asked", which is what it was: the chain did not exist yet.
fn add_ai_decision_columns(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(ai_decisions)")?;
    let existing: std::collections::HashSet<String> = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    // Same lost-race tolerance as `add_run_columns`: two processes migrating together
    // must not fail the second open.
    if !existing.contains("attempts") {
        add_column(
            connection,
            "ai_decisions",
            "attempts",
            "TEXT NOT NULL DEFAULT '[]'",
        )?;
    }
    Ok(())
}

/// Rename the pre-rename history once: the sub-app is `guide` and its matching actions are
/// `match_*`.
///
/// The store records what it was told, so a database written by an earlier build would show
/// the old names in the UI and make `recent_runs_for("guide")` miss every old run. The rewrite
/// is one transaction, and the version stamp makes it idempotent: a later open sees
/// `DATA_VERSION` and touches nothing. Nothing else is renamed — the old rows carry no other
/// reference to the app.
fn rename_recorded_history(connection: &Connection) -> Result<()> {
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version >= DATA_VERSION {
        return Ok(());
    }
    let transaction = connection.unchecked_transaction()?;
    transaction.execute("UPDATE runs SET app = 'guide' WHERE app = 'janitor'", [])?;
    transaction.execute(
        "UPDATE runs SET action = 'match_preview' WHERE action = 'automatch_preview'",
        [],
    )?;
    transaction.execute(
        "UPDATE runs SET action = 'match_apply' WHERE action = 'automatch_apply'",
        [],
    )?;
    transaction.execute(
        "UPDATE settings_history SET app = 'guide' WHERE app = 'janitor'",
        [],
    )?;
    // `PRAGMA` takes no bound parameter, and the value is a constant of this build.
    transaction.execute_batch(&format!("PRAGMA user_version = {DATA_VERSION}"))?;
    transaction.commit()?;
    Ok(())
}

fn map_stream_health(row: &rusqlite::Row<'_>) -> rusqlite::Result<StreamHealth> {
    Ok(StreamHealth {
        stream_id: row.get(0)?,
        probed_at: row.get(1)?,
        class: row.get(2)?,
        error_type: row.get(3)?,
        throughput_mbps: row.get(4)?,
        throughput_at: row.get(5)?,
        edge_ip: row.get(6)?,
        consecutive_failures: row.get(7)?,
    })
}

/// The `runs` columns a [`RunRecord`] is built from, in the order [`map_run`] reads them.
///
/// One list rather than five copies: every query that returns a run record has to select the
/// same columns, and a column added to the table but missed in one query would make that query
/// fail at runtime instead of at compile time.
const RUN_COLUMNS: &str = "id, app, action, dry_run, started_at, finished_at, status, summary, \
     counts, report_path, parent_run_id";

/// One `runs` row, from the columns [`RUN_COLUMNS`] selects.
fn map_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRecord> {
    let counts: String = row.get(8)?;
    let status: String = row.get(6)?;
    Ok(RunRecord {
        id: row.get(0)?,
        app: row.get(1)?,
        action: row.get(2)?,
        dry_run: row.get::<_, i64>(3)? != 0,
        started_at: row.get(4)?,
        finished_at: row.get(5)?,
        status: RunStatus::parse(&status),
        summary: row.get(7)?,
        counts: serde_json::from_str(&counts).unwrap_or_default(),
        report_path: row.get(9)?,
        parent_run_id: row.get(10)?,
    })
}

/// The `run_rows` columns a [`RunRow`] is built from, in the order [`map_run_row`] reads them.
const RUN_ROW_COLUMNS: &str = "ordinal, channel_id, channel_name, channel_group, \
     channel_number, current_epg_name, epg_data_id, epg_name, epg_source, score, method, \
     status, reason, ai_decision, field, before_value, after_value, subject";

/// One `run_rows` row, from the columns [`RUN_ROW_COLUMNS`] selects.
fn map_run_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRow> {
    let subject: String = row.get(17)?;
    Ok(RunRow {
        ordinal: row.get(0)?,
        subject: Subject::parse(&subject),
        channel_id: row.get(1)?,
        channel_name: row.get(2)?,
        channel_group: row.get(3)?,
        channel_number: row.get(4)?,
        current_epg_name: row.get(5)?,
        epg_data_id: row.get(6)?,
        epg_name: row.get(7)?,
        epg_source: row.get(8)?,
        score: row.get(9)?,
        method: row.get(10)?,
        status: row.get(11)?,
        reason: row.get(12)?,
        ai_decision: row.get(13)?,
        field: row.get(14)?,
        before_value: row.get(15)?,
        after_value: row.get(16)?,
    })
}

/// `n` comma-separated SQL placeholders, for an `IN (...)` list built at run time.
fn marks(n: usize) -> String {
    std::iter::repeat_n("?", n).collect::<Vec<_>>().join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_health_row_is_upserted_and_replaced_whole() {
        // `probe` writes one of these per stream and `sort` reads them all: the row is the
        // stream's whole health, so a probe that measured nothing clears the old measurement
        // rather than leaving a stale rate for the sort to trust.
        let store = Store::in_memory().expect("store");
        let alive = StreamHealth {
            stream_id: 11,
            probed_at: Some("2026-11-01T12:00:00+00:00".to_string()),
            class: "alive".to_string(),
            error_type: None,
            throughput_mbps: Some(12.5),
            throughput_at: Some("2026-11-01T11:59:00+00:00".to_string()),
            edge_ip: Some("edge.example".to_string()),
            consecutive_failures: 0,
        };
        store.upsert_stream_health(&alive).expect("insert");
        assert_eq!(store.stream_health(11).expect("read"), Some(alive.clone()));

        let dead = StreamHealth {
            stream_id: 11,
            class: "dead".to_string(),
            error_type: Some("Timeout".to_string()),
            throughput_mbps: None,
            consecutive_failures: 3,
            ..alive.clone()
        };
        store.upsert_stream_health(&dead).expect("update");
        assert_eq!(store.stream_health(11).expect("read"), Some(dead));
        assert_eq!(store.stream_health(12).expect("read"), None);

        store
            .upsert_stream_health(&StreamHealth {
                stream_id: 12,
                class: "skipped".to_string(),
                ..StreamHealth::default()
            })
            .expect("second row");
        let map = store.stream_health_map().expect("map");
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&11).map(|row| row.class.as_str()), Some("dead"));
        assert_eq!(map.get(&12).map(|row| row.class.as_str()), Some("skipped"));
    }

    #[test]
    fn a_checkpoint_is_saved_read_and_cleared_by_app_and_action() {
        // The next window is a different run of the same action, so the place it got to is
        // keyed by app+action and outlives the run: this is what replaces IPTV Checker's
        // pending_resume.json.
        let store = Store::in_memory().expect("store");
        assert_eq!(
            store.load_checkpoint("streams", "probe").expect("read"),
            None
        );

        store
            .save_checkpoint(
                "streams",
                "probe",
                &serde_json::json!({"remaining": [4, 5, 6], "done": 2}),
            )
            .expect("save");
        assert_eq!(
            store.load_checkpoint("streams", "probe").expect("read"),
            Some(serde_json::json!({"remaining": [4, 5, 6], "done": 2}))
        );
        assert!(store
            .checkpoint_updated_at("streams", "probe")
            .expect("updated")
            .is_some());
        // Another action of the same app has its own place.
        assert_eq!(
            store
                .load_checkpoint("streams", "sort_apply")
                .expect("read"),
            None
        );

        // A second save replaces the first rather than piling up.
        store
            .save_checkpoint("streams", "probe", &serde_json::json!({"remaining": [6]}))
            .expect("save again");
        assert_eq!(
            store.load_checkpoint("streams", "probe").expect("read"),
            Some(serde_json::json!({"remaining": [6]}))
        );

        assert!(store.clear_checkpoint("streams", "probe").expect("clear"));
        assert_eq!(
            store.load_checkpoint("streams", "probe").expect("read"),
            None
        );
        assert!(
            !store.clear_checkpoint("streams", "probe").expect("clear"),
            "clearing nothing is not an error"
        );
    }

    #[test]
    fn an_unreadable_checkpoint_reads_as_none_instead_of_failing_the_run() {
        // A checkpoint is an optimisation over starting again, never a reason not to run.
        let store = Store::in_memory().expect("store");
        store
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO job_checkpoints (app, action, state, updated_at) \
                     VALUES ('streams', 'probe', 'not json', '2026-11-01T00:00:00+00:00')",
                    [],
                )?;
                Ok(())
            })
            .expect("seed");
        assert_eq!(
            store.load_checkpoint("streams", "probe").expect("read"),
            None
        );
    }

    #[test]
    fn paused_is_its_own_status_and_reads_back() {
        // It must not be confused with running (which holds the slot), cancelled (work dropped)
        // or succeeded (job done): a paused run is one that will continue.
        let store = Store::in_memory().expect("store");
        let run_id = store.start_run("streams", "probe", false).expect("start");
        store
            .finish_run(
                run_id,
                RunStatus::Paused,
                Some("paused at the window's end"),
                &serde_json::json!({"probed": 120}),
                None,
            )
            .expect("finish");
        let run = store.run(run_id).expect("run").expect("present");
        assert_eq!(run.status, RunStatus::Paused);
        assert_eq!(run.status.as_str(), "paused");
        assert!(run.status.is_paused());
        assert!(!run.status.is_running());
        // A paused run does not hold the app's slot: the next firing must be able to start.
        assert!(store.running_runs("streams").expect("running").is_empty());
        assert_eq!(store.run_count().expect("count"), 1);
    }

    #[test]
    fn a_paused_run_is_not_a_successful_run_for_a_later_action() {
        // `last_succeeded_run` is how a later action finds "the result set the last run
        // produced": a paused or cancelled run's rows are partial, so they must not qualify.
        let store = Store::in_memory().expect("store");
        let run_id = store.start_run("guide", "scan", true).expect("start");
        store
            .finish_run(
                run_id,
                RunStatus::Paused,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        assert!(store
            .last_succeeded_run("guide", "scan")
            .expect("query")
            .is_none());
    }

    #[tokio::test]
    async fn an_unguarded_run_takes_no_slot_and_is_not_reported_as_running() {
        // The status action used to be refused while another run of its app was live, which is
        // exactly when an operator asks what is going on. It is still recorded, so its answer
        // lands on a run page.
        let store = Store::in_memory().expect("store");
        let guarded = store
            .start_run("guide", "match_apply", false)
            .expect("guarded");
        let unguarded = store
            .start_unguarded_run("guide", "status", true)
            .expect("an unguarded run may start beside a live one");

        assert_eq!(
            store.running_runs("guide").expect("running").len(),
            1,
            "the cross-process guard sees the guarded run only"
        );
        assert_eq!(store.running_runs("guide").expect("running")[0].id, guarded);
        // And the slot is still held for a second guarded run, as the index requires.
        assert!(store.start_run("guide", "match_apply", false).is_err());

        store
            .finish_run(
                unguarded,
                RunStatus::Succeeded,
                Some("ok"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        assert_eq!(
            store.run(unguarded).expect("run").expect("present").status,
            RunStatus::Succeeded
        );
    }

    #[test]
    fn the_guard_index_is_rebuilt_once_and_then_left_alone() {
        // The index gained a column, so an older database's definition has to be replaced —
        // exactly once. Rebuilding it on every open would leave a moment with no guard, which is
        // long enough for two processes opening together to insert competing `running` rows.
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("crosstalk.db");
        {
            let connection = Connection::open(&path).expect("open");
            connection
                .execute_batch(
                    "CREATE TABLE runs (id INTEGER PRIMARY KEY AUTOINCREMENT, app TEXT NOT NULL, \
                     action TEXT NOT NULL, dry_run INTEGER NOT NULL, started_at TEXT NOT NULL, \
                     finished_at TEXT, status TEXT NOT NULL, summary TEXT, \
                     counts TEXT NOT NULL DEFAULT '{}', report_path TEXT); \
                     CREATE UNIQUE INDEX runs_one_running_per_app ON runs(app) \
                     WHERE status = 'running';",
                )
                .expect("old schema");
        }
        let store = Store::open(&path).expect("migrate");
        let definition = store
            .with_connection(|connection| index_definition(connection, "runs_one_running_per_app"))
            .expect("read the index")
            .expect("the index exists");
        assert!(definition.contains("unguarded"), "{definition}");

        // Reopening keeps the same definition and still accepts work.
        drop(store);
        let store = Store::open(&path).expect("migrate again");
        assert!(store.start_run("guide", "scan", true).is_ok());
        assert!(
            store.start_run("guide", "scan", true).is_err(),
            "the guard still holds"
        );
    }

    #[test]
    fn a_store_from_before_unguarded_and_checkpoints_gains_both_on_open() {
        // The upgrade path: a database written by the release before this one has no
        // `unguarded` column, no `job_checkpoints` table and an index without the column. The
        // migration must add all three without touching the history.
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("crosstalk.db");
        {
            let connection = Connection::open(&path).expect("open");
            connection
                .execute_batch(
                    "CREATE TABLE runs (id INTEGER PRIMARY KEY AUTOINCREMENT, app TEXT NOT NULL, \
                     action TEXT NOT NULL, dry_run INTEGER NOT NULL, started_at TEXT NOT NULL, \
                     finished_at TEXT, status TEXT NOT NULL, summary TEXT, \
                     counts TEXT NOT NULL DEFAULT '{}', report_path TEXT); \
                     CREATE UNIQUE INDEX runs_one_running_per_app ON runs(app) \
                     WHERE status = 'running'; \
                     INSERT INTO runs (app, action, dry_run, started_at, status) \
                     VALUES ('guide', 'scan', 1, '2026-11-01T00:00:00+00:00', 'succeeded'); \
                     PRAGMA user_version = 1",
                )
                .expect("old schema");
        }
        let store = Store::open(&path).expect("migrate");
        // The history survives, and the new column defaults to "guarded".
        assert_eq!(store.run_count().expect("count"), 1);
        assert!(store.running_runs("guide").expect("running").is_empty());
        // A row written before the workflow link reads back as a run started on its own,
        // which is what it was: the added column is NULL and no step list claims it.
        let history = store.recent_runs(10).expect("history");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].parent_run_id, None);
        assert!(
            store
                .child_runs(history[0].id)
                .expect("children")
                .is_empty(),
            "an old run has no steps"
        );
        store
            .save_checkpoint("streams", "probe", &serde_json::json!({"done": 1}))
            .expect("the table exists after the migration");
        // The re-created index has the new definition: an unguarded run may start beside a
        // guarded one, which the old index would have refused.
        let _guarded = store.start_run("streams", "probe", false).expect("guarded");
        store
            .start_unguarded_run("streams", "status", true)
            .expect("the index excludes unguarded rows");
    }

    #[test]
    fn a_store_from_before_stream_health_gains_the_table_on_open() {
        // The table is created with `IF NOT EXISTS` on every open, so an older store grows it
        // without a version bump and without touching the run history.
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("crosstalk.db");
        {
            let connection = Connection::open(&path).expect("open");
            connection
                .execute_batch(
                    "CREATE TABLE runs (id INTEGER PRIMARY KEY AUTOINCREMENT, app TEXT NOT NULL, \
                     action TEXT NOT NULL, dry_run INTEGER NOT NULL, started_at TEXT NOT NULL, \
                     finished_at TEXT, status TEXT NOT NULL, summary TEXT, \
                     counts TEXT NOT NULL DEFAULT '{}', report_path TEXT); \
                     PRAGMA user_version = 1",
                )
                .expect("old schema");
        }
        let store = Store::open(&path).expect("migrate");
        store
            .upsert_stream_health(&StreamHealth {
                stream_id: 1,
                class: "alive".to_string(),
                ..StreamHealth::default()
            })
            .expect("the table exists after the migration");
    }

    fn sample_row(ordinal: i64, name: &str, score: i64) -> RunRow {
        RunRow {
            ordinal,
            subject: Subject::Channel,
            channel_id: ordinal + 100,
            channel_name: name.to_string(),
            channel_group: "Sports".to_string(),
            channel_number: Some(101.0),
            current_epg_name: Some("Old Guide".to_string()),
            epg_data_id: Some(9),
            epg_name: Some("ESPN".to_string()),
            epg_source: Some("Schedules Direct".to_string()),
            score,
            method: "Callsign".to_string(),
            status: "HEALED".to_string(),
            reason: "Excellent replacement match (100%)".to_string(),
            ai_decision: None,
            field: String::new(),
            before_value: None,
            after_value: None,
        }
    }

    #[test]
    fn a_run_round_trips() {
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run("guide", "match_apply", false)
            .expect("start");

        let running = store.run(run_id).expect("run").expect("present");
        assert_eq!(running.status, RunStatus::Running);
        assert!(running.finished_at.is_none());

        store
            .append_rows(
                run_id,
                &[sample_row(0, "ESPN", 100), sample_row(1, "CNN", 88)],
            )
            .expect("rows");
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("2 matched"),
                &serde_json::json!({"matched": 2}),
                Some("/exports/a.csv"),
            )
            .expect("finish");

        let record = store.run(run_id).expect("run").expect("present");
        assert_eq!(record.status, RunStatus::Succeeded);
        assert_eq!(record.summary.as_deref(), Some("2 matched"));
        assert_eq!(record.counts["matched"], 2);
        assert!(record.finished_at.is_some());

        let rows = store.rows(run_id).expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].channel_name, "ESPN");
        assert_eq!(rows[1].score, 88);
    }

    #[test]
    fn a_channel_operation_row_keeps_what_it_changed() {
        // A restore reads these three columns, so they have to survive the round trip even
        // though the matching columns are empty.
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run("guide", "clear_groups_apply", false)
            .expect("start");
        store
            .append_rows(
                run_id,
                &[RunRow {
                    ordinal: 0,
                    channel_id: 7,
                    channel_name: "ESPN".to_string(),
                    field: "epg_data_id".to_string(),
                    before_value: Some("31".to_string()),
                    after_value: Some("none".to_string()),
                    method: "clear_epg".to_string(),
                    status: "ok".to_string(),
                    ..RunRow::default()
                }],
            )
            .expect("rows");

        let rows = store.rows(run_id).expect("rows");
        assert_eq!(rows[0].field, "epg_data_id");
        assert_eq!(rows[0].before_value.as_deref(), Some("31"));
        assert_eq!(rows[0].after_value.as_deref(), Some("none"));
    }

    #[test]
    fn a_subject_round_trips_and_defaults_to_a_channel() {
        let store = Store::in_memory().expect("store");
        let run_id = store.start_run("streams", "probe", false).expect("start");
        store
            .append_rows(
                run_id,
                &[
                    RunRow {
                        ordinal: 0,
                        subject: Subject::Channel,
                        channel_id: 7,
                        channel_name: "ESPN".to_string(),
                        ..RunRow::default()
                    },
                    RunRow {
                        ordinal: 1,
                        subject: Subject::Stream,
                        channel_id: 51_000,
                        channel_name: "ESPN HD (alt)".to_string(),
                        ..RunRow::default()
                    },
                    RunRow {
                        ordinal: 2,
                        subject: Subject::EpgSource,
                        channel_id: 4,
                        channel_name: "Schedules Direct".to_string(),
                        field: "epg_source".to_string(),
                        ..RunRow::default()
                    },
                ],
            )
            .expect("rows");

        let rows = store.rows(run_id).expect("rows");
        assert_eq!(rows[0].subject, Subject::Channel);
        assert_eq!(rows[1].subject, Subject::Stream);
        assert_eq!(rows[2].subject, Subject::EpgSource);
        // The stored form is the stable one; the label is only for the page.
        assert_eq!(rows[2].subject.as_str(), "epg_source");
        assert_eq!(rows[2].subject.label(), "EPG source");
        assert_eq!(Subject::default(), Subject::Channel);
    }

    #[test]
    fn a_store_written_before_the_subject_column_backfills_the_watchdog_rows() {
        // The column arrived with the streams phase. Watchdog rows were written with an EPG
        // SOURCE id in `channel_id` and `field = 'epg_source'`, so the DEFAULT alone would
        // call them channels and a restore would read a source id as a channel id.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("runs.db");
        {
            let connection = Connection::open(&path).expect("open");
            connection
                .execute_batch(
                    "CREATE TABLE runs (id INTEGER PRIMARY KEY AUTOINCREMENT, app TEXT NOT NULL, \
                     action TEXT NOT NULL, dry_run INTEGER NOT NULL, started_at TEXT NOT NULL, \
                     finished_at TEXT, status TEXT NOT NULL, summary TEXT, \
                     counts TEXT NOT NULL DEFAULT '{}', report_path TEXT); \
                     CREATE TABLE run_rows (run_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, \
                     channel_id INTEGER NOT NULL, channel_name TEXT NOT NULL, \
                     channel_group TEXT NOT NULL DEFAULT '', channel_number REAL, \
                     current_epg_name TEXT, epg_data_id INTEGER, epg_name TEXT, epg_source TEXT, \
                     score INTEGER NOT NULL DEFAULT 0, method TEXT NOT NULL DEFAULT '', \
                     status TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', \
                     ai_decision TEXT, field TEXT NOT NULL DEFAULT '', before_value TEXT, \
                     after_value TEXT, PRIMARY KEY (run_id, ordinal)); \
                     INSERT INTO runs (app, action, dry_run, started_at, status) \
                     VALUES ('guide', 'watchdog_apply', 0, '2026-11-01T00:00:00Z', 'succeeded'); \
                     INSERT INTO run_rows (run_id, ordinal, channel_id, channel_name, field) \
                     VALUES (1, 0, 4, 'Schedules Direct', 'epg_source'); \
                     INSERT INTO run_rows (run_id, ordinal, channel_id, channel_name, field) \
                     VALUES (1, 1, 7, 'ESPN', 'name');",
                )
                .expect("old schema");
            connection
                .execute_batch("PRAGMA user_version = 1")
                .expect("stamp version");
        }

        let store = Store::open(&path).expect("reopens");
        let rows = store.rows(1).expect("rows");
        assert_eq!(rows[0].subject, Subject::EpgSource);
        assert_eq!(rows[1].subject, Subject::Channel);

        // And the backfill is idempotent: a second open changes nothing, and a channel row
        // that happens to carry the `epg_source` field label is not rewritten.
        drop(store);
        let store = Store::open(&path).expect("reopens again");
        assert_eq!(store.rows(1).expect("rows")[0].subject, Subject::EpgSource);
    }

    #[test]
    fn a_store_written_before_the_channel_operation_columns_is_upgraded() {
        // The columns arrived with channel operations; an existing database (cedro's) must
        // gain them rather than fail the first insert.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("runs.db");
        {
            let connection = Connection::open(&path).expect("open");
            connection
                .execute_batch(
                    "CREATE TABLE run_rows (run_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, \
                     channel_id INTEGER NOT NULL, channel_name TEXT NOT NULL, \
                     channel_group TEXT NOT NULL DEFAULT '', channel_number REAL, \
                     current_epg_name TEXT, epg_data_id INTEGER, epg_name TEXT, epg_source TEXT, \
                     score INTEGER NOT NULL DEFAULT 0, method TEXT NOT NULL DEFAULT '', \
                     status TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', \
                     ai_decision TEXT, PRIMARY KEY (run_id, ordinal));",
                )
                .expect("old schema");
            // The rename migration already ran on this database (it is at DATA_VERSION),
            // so only the column upgrade may touch it.
            connection
                .execute_batch("PRAGMA user_version = 1")
                .expect("stamp version");
        }

        let store = Store::open(&path).expect("reopens");
        let run_id = store
            .start_run("guide", "tag_missing_apply", false)
            .expect("start");
        store
            .append_rows(
                run_id,
                &[RunRow {
                    ordinal: 0,
                    channel_id: 1,
                    field: "name".to_string(),
                    before_value: Some("ESPN".to_string()),
                    after_value: Some("ESPN [BadEPG]".to_string()),
                    ..RunRow::default()
                }],
            )
            .expect("inserts into the upgraded table");
        assert_eq!(store.rows(run_id).expect("rows").len(), 1);
    }

    #[test]
    fn rows_of_fields_reads_only_the_fields_asked_for() {
        // A probe run is mostly stream rows; its readers want the per-channel verdicts.
        let store = Store::in_memory().expect("store");
        let run_id = store.start_run("streams", "probe", false).expect("start");
        let rows: Vec<RunRow> = ["probe", "probe_verdict", "probe_format"]
            .iter()
            .enumerate()
            .map(|(ordinal, field)| RunRow {
                ordinal: ordinal as i64,
                subject: if *field == "probe" {
                    Subject::Stream
                } else {
                    Subject::Channel
                },
                channel_id: 1,
                field: field.to_string(),
                after_value: Some("alive".to_string()),
                ..RunRow::default()
            })
            .collect();
        store.append_rows(run_id, &rows).expect("rows");

        let verdicts = store
            .rows_of_fields(run_id, &["probe_verdict", "probe_format"])
            .expect("rows");
        assert_eq!(verdicts.len(), 2, "{verdicts:?}");
        assert!(verdicts.iter().all(|row| row.field != "probe"));
        assert!(store.rows_of_fields(run_id, &[]).expect("rows").is_empty());
        // Another run's rows are never in the answer.
        let other = store.start_run("guide", "scan", true).expect("start");
        assert!(store
            .rows_of_fields(other, &["probe_verdict"])
            .expect("rows")
            .is_empty());
    }

    #[test]
    fn field_history_reads_the_channel_rows_of_the_actions_asked_for() {
        // What a restore reads: the channel rows of `name` and `channel_group_id` from the
        // newest finished triage runs, oldest run first, and nothing else.
        let store = Store::in_memory().expect("store");
        let counter = std::cell::Cell::new(0i64);
        let row = |channel_id: i64, field: &str, before: &str, after: &str| {
            let ordinal = counter.get();
            counter.set(ordinal + 1);
            RunRow {
                ordinal,
                subject: Subject::Channel,
                channel_id,
                field: field.to_string(),
                before_value: Some(before.to_string()),
                after_value: Some(after.to_string()),
                status: "ok".to_string(),
                ..RunRow::default()
            }
        };

        // A dry run's rows are plans, not writes; a still-running run is not history yet; a
        // stream row is about a stream, and another action's rows are not triage's.
        let preview = store
            .start_run("streams", "triage_apply", true)
            .expect("start");
        store
            .append_rows(preview, &[row(1, "name", "A", "B")])
            .expect("rows");
        store
            .finish_run(
                preview,
                RunStatus::Succeeded,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");

        let guide = store
            .start_run("guide", "tag_missing_apply", false)
            .expect("start");
        store
            .append_rows(guide, &[row(1, "name", "C", "D")])
            .expect("rows");
        store
            .finish_run(
                guide,
                RunStatus::Succeeded,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");

        let triage = store
            .start_run("streams", "triage_apply", false)
            .expect("start");
        store
            .append_rows(
                triage,
                &[
                    row(1, "name", "X", "X [DEAD]"),
                    row(1, "channel_group_id", "100", "102"),
                    row(2, "epg_data_id", "5", "none"),
                    RunRow {
                        ordinal: 99,
                        subject: Subject::Stream,
                        channel_id: 1,
                        field: "name".to_string(),
                        ..RunRow::default()
                    },
                ],
            )
            .expect("rows");
        store
            .finish_run(
                triage,
                RunStatus::Cancelled,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");

        // A run still writing is not history yet: it is started here, last, so the store's
        // one-live-run-per-app index allows the finished ones above.
        let running = store
            .start_run("streams", "triage_apply", false)
            .expect("start");
        store
            .append_rows(running, &[row(1, "name", "B", "C")])
            .expect("rows");

        let history = store
            .field_history(
                "streams",
                &["triage_apply"],
                &["name", "channel_group_id"],
                50,
            )
            .expect("history");
        // The cancelled run's confirmed rows ARE history (its `ok` rows landed), the dry run's
        // and the guide's are not, and the EPG and stream rows are filtered out.
        assert_eq!(history.len(), 2, "{history:?}");
        assert_eq!(history[0].field, "name");
        assert_eq!(history[0].before_value.as_deref(), Some("X"));
        assert_eq!(history[1].field, "channel_group_id");

        // A cap keeps the scan bounded: one run back finds nothing older.
        let none = store
            .field_history("streams", &["triage_apply"], &["name"], 0)
            .expect("history");
        assert!(none.is_empty());
        assert!(store
            .field_history("streams", &[], &["name"], 10)
            .expect("history")
            .is_empty());
    }

    #[test]
    fn a_step_run_is_linked_to_the_workflow_run_it_belongs_to() {
        // A workflow run is an ordinary row in this table, and every step points at it: that is
        // how the workflow's run page lists what it ran, without a second table and without
        // guessing from timestamps.
        let store = Store::in_memory().expect("store");
        let workflow = store
            .start_run("workflow", "nightly", false)
            .expect("start");
        let probe = store
            .insert_run("streams", "probe", false, false, Some(workflow))
            .expect("probe");
        // Steps run one at a time, so the first has finished before the second starts — which is
        // also what keeps the one-live-run-per-app index out of the way for a workflow that lists
        // the same app twice.
        store
            .finish_run(
                probe,
                RunStatus::Succeeded,
                Some("done"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let sort = store
            .insert_run("streams", "sort_apply", false, false, Some(workflow))
            .expect("sort");
        // An ordinary run, started on its own, has no parent.
        store
            .finish_run(
                sort,
                RunStatus::Succeeded,
                Some("done"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let manual = store.start_run("streams", "probe", false).expect("manual");

        let steps = store.child_runs(workflow).expect("steps");
        assert_eq!(
            steps.iter().map(|run| run.id).collect::<Vec<_>>(),
            vec![probe, sort],
            "in the order they ran"
        );
        assert!(steps.iter().all(|run| run.parent_run_id == Some(workflow)));
        assert_eq!(
            store
                .run(manual)
                .expect("run")
                .expect("present")
                .parent_run_id,
            None
        );
        assert_eq!(
            store
                .run(workflow)
                .expect("run")
                .expect("present")
                .parent_run_id,
            None,
            "a workflow is nobody's step"
        );
        assert!(store.child_runs(manual).expect("children").is_empty());
    }

    #[test]
    fn the_last_succeeded_run_is_found_by_app_and_action() {
        // The guide's cleanup actions read the result set of the last `scan` through this.
        let store = Store::in_memory().expect("store");
        assert!(store
            .last_succeeded_run("guide", "scan")
            .expect("query")
            .is_none());

        let scan = store.start_run("guide", "scan", true).expect("start");
        store
            .finish_run(
                scan,
                RunStatus::Succeeded,
                Some("3 missing"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        // A cancelled scan is not a result set: its rows are partial by definition.
        let cancelled = store.start_run("guide", "scan", true).expect("start");
        store
            .finish_run(
                cancelled,
                RunStatus::Cancelled,
                Some("cancelled"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        // Another action's run is not a scan, however recent.
        let other = store
            .start_run("guide", "match_preview", true)
            .expect("start");
        store
            .finish_run(
                other,
                RunStatus::Succeeded,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");

        let found = store
            .last_succeeded_run("guide", "scan")
            .expect("query")
            .expect("the succeeded scan");
        assert_eq!(found.id, scan);
        assert!(store
            .last_succeeded_run("streams", "scan")
            .expect("query")
            .is_none());
    }

    #[test]
    fn a_shared_open_leaves_running_rows_alone() {
        // The one-shot CLI opens the daemon's file with this: retiring here would mark the
        // daemon's live run failed while it is still writing.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.db");
        let store = Store::open(&path).expect("open");
        let run_id = store.start_run("guide", "scan", true).expect("start");

        let shared = Store::open_shared(&path).expect("shared open");
        let running = shared.running_runs("guide").expect("query");
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].id, run_id);
        assert_eq!(running[0].status, RunStatus::Running);
        assert!(shared.running_runs("streams").expect("query").is_empty());

        // And the daemon's own next open still retires the stale row.
        drop(store);
        drop(shared);
        let store = Store::open(&path).expect("reopen");
        assert!(store.running_runs("guide").expect("query").is_empty());
        assert_eq!(
            store.run(run_id).expect("run").expect("present").status,
            RunStatus::Failed
        );
    }

    #[test]
    fn a_second_live_run_for_the_same_app_is_refused() {
        // The cross-process guard: the busy map cannot see another process, so the store
        // itself refuses a second `running` row per app. Finished rows never conflict, and
        // another app is unaffected.
        let store = Store::in_memory().expect("store");
        let _first = store.start_run("guide", "scan", true).expect("start");
        let error = store
            .start_run("guide", "clear_missing_apply", false)
            .expect_err("refused");
        assert!(
            format!("{error:#}").contains("UNIQUE constraint failed"),
            "{error:#}"
        );
        store
            .start_run("streams", "scan", true)
            .expect("another app may run");
        store
            .finish_run(
                _first,
                RunStatus::Succeeded,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        store
            .start_run("guide", "scan", true)
            .expect("free again after finish");
    }

    #[test]
    fn rows_are_replaced_not_duplicated() {
        let store = Store::in_memory().expect("store");
        let run_id = store.start_run("guide", "heal_apply", true).expect("start");
        store
            .append_rows(run_id, &[sample_row(0, "ESPN", 100)])
            .expect("first");
        store
            .append_rows(run_id, &[sample_row(0, "ESPN", 95)])
            .expect("second");
        let rows = store.rows(run_id).expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].score, 95);
    }

    #[test]
    fn ai_decisions_round_trip() {
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run("guide", "match_preview", true)
            .expect("start");
        let decision = AiDecision {
            row_ordinal: 3,
            provider: "typesafe".to_string(),
            model: "jev-latest".to_string(),
            latency_ms: 120,
            input_tokens: 400,
            output_tokens: 40,
            request: serde_json::json!({"state": "x"}),
            response: serde_json::json!({"answers": {}}),
            attempts: serde_json::json!([
                {"provider": "typesafe", "model": "jev-latest", "result": "decided"},
            ]),
        };
        store.record_ai_decision(run_id, &decision).expect("record");

        let loaded = store
            .ai_decision(run_id, 3)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.model, "jev-latest");
        assert_eq!(loaded.latency_ms, 120);
        assert_eq!(loaded.attempts, decision.attempts);
        assert!(store.ai_decision(run_id, 4).expect("load").is_none());
    }

    #[test]
    fn ai_decision_attempts_default_to_empty_for_old_rows() {
        // A database written before the chain existed has no `attempts` column: opening it
        // backfills the column, and its rows read back as "one link asked", which is what
        // they were.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("store.sqlite");
        {
            let connection = rusqlite::Connection::open(&path).expect("open");
            connection
                .execute_batch(
                    "CREATE TABLE runs (id INTEGER PRIMARY KEY AUTOINCREMENT, app TEXT NOT \
                     NULL, action TEXT NOT NULL, dry_run INTEGER NOT NULL, started_at TEXT NOT \
                     NULL, finished_at TEXT, status TEXT NOT NULL, summary TEXT, counts TEXT \
                     NOT NULL DEFAULT '{}', report_path TEXT, parent_run_id INTEGER); \
                     CREATE TABLE ai_decisions (run_id INTEGER NOT NULL, row_ordinal INTEGER \
                     NOT NULL, provider TEXT NOT NULL, model TEXT NOT NULL, latency_ms INTEGER \
                     NOT NULL DEFAULT 0, input_tokens INTEGER NOT NULL DEFAULT 0, output_tokens \
                     INTEGER NOT NULL DEFAULT 0, request TEXT NOT NULL DEFAULT '{}', response \
                     TEXT NOT NULL DEFAULT '{}', PRIMARY KEY (run_id, row_ordinal));",
                )
                .expect("old schema");
            connection
                .execute(
                    "INSERT INTO runs (app, action, dry_run, started_at, status, counts) \
                     VALUES ('guide', 'match_preview', 1, '2026-01-01T00:00:00Z', 'succeeded', '{}')",
                    [],
                )
                .expect("run");
            connection
                .execute(
                    "INSERT INTO ai_decisions (run_id, row_ordinal, provider, model) VALUES \
                     (1, 0, 'typesafe', 'jev-latest')",
                    [],
                )
                .expect("decision");
        }
        // Opening migrates idempotently: the column appears, and opening again changes
        // nothing.
        let store = Store::open(&path).expect("migrate");
        let reopened = Store::open(&path).expect("migrate again");
        for store in [&store, &reopened] {
            let loaded = store.ai_decision(1, 0).expect("load").expect("present");
            assert_eq!(loaded.provider, "typesafe");
            assert_eq!(
                loaded.attempts,
                serde_json::json!([]),
                "an old row asked exactly one link"
            );
        }
    }

    #[test]
    fn recent_runs_are_newest_first_and_filterable() {
        let store = Store::in_memory().expect("store");
        let first = store
            .start_run("guide", "match_preview", true)
            .expect("start");
        // SQLite's timestamp resolution is a second, so order by id as well; this sleeps to
        // make the timestamps differ where the platform allows it.
        std::thread::sleep(std::time::Duration::from_millis(5));
        let second = store.start_run("other", "something", true).expect("start");

        let all = store.recent_runs(10).expect("runs");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, second);
        assert_eq!(all[1].id, first);

        let guide_only = store.recent_runs_for("guide", 10).expect("runs");
        assert_eq!(guide_only.len(), 1);
        assert_eq!(guide_only[0].id, first);

        assert_eq!(store.run_count().expect("count"), 2);
    }

    #[test]
    fn settings_changes_are_recorded() {
        let store = Store::in_memory().expect("store");
        store
            .record_settings_change("guide", &serde_json::json!({"check_hours": 6}))
            .expect("record");
        let count: i64 = store
            .with_connection(|connection| {
                connection
                    .query_row("SELECT COUNT(*) FROM settings_history", [], |row| {
                        row.get(0)
                    })
                    .map_err(Into::into)
            })
            .expect("count");
        assert_eq!(count, 1);
    }

    #[test]
    fn pruning_keeps_recent_runs_and_a_minimum() {
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run("guide", "match_preview", true)
            .expect("start");
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");

        // Pretend the run is a month old.
        let old = (chrono::Utc::now() - chrono::Duration::days(30))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        store
            .with_connection(|connection| {
                connection
                    .execute("UPDATE runs SET started_at = ?1", params![old])
                    .map_err(Into::into)
            })
            .expect("age");

        // A positive retention removes it, because the minimum is met by nothing else.
        assert_eq!(store.prune(7, 0).expect("prune"), 1);
        assert_eq!(store.run_count().expect("count"), 0);
    }

    #[test]
    fn pruning_off_by_default() {
        let store = Store::in_memory().expect("store");
        store
            .start_run("guide", "match_preview", true)
            .expect("start");
        assert_eq!(store.prune(0, 0).expect("prune"), 0);
        assert_eq!(store.run_count().expect("count"), 1);
    }

    #[test]
    fn a_running_run_is_never_pruned() {
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run("guide", "match_apply", false)
            .expect("start");
        let old = (chrono::Utc::now() - chrono::Duration::days(30))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        store
            .with_connection(|connection| {
                connection
                    .execute(
                        "UPDATE runs SET started_at = ?1 WHERE id = ?2",
                        params![old, run_id],
                    )
                    .map_err(Into::into)
            })
            .expect("age");
        assert_eq!(store.prune(7, 0).expect("prune"), 0);
    }

    #[test]
    fn the_connection_waits_on_a_locked_database() {
        // A second process (or an operator's sqlite3) holding a lock must not fail
        // instantly with SQLITE_BUSY; the 5 s timeout gives it time to clear.
        let store = Store::in_memory().expect("store");
        assert_eq!(store.busy_timeout_for_test().expect("pragma"), 5000);
    }

    #[test]
    fn a_file_backed_store_reopens_with_its_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.db");
        {
            let store = Store::open(&path).expect("open");
            let run_id = store
                .start_run("guide", "match_preview", true)
                .expect("start");
            store
                .finish_run(
                    run_id,
                    RunStatus::Succeeded,
                    Some("ok"),
                    &serde_json::json!({}),
                    None,
                )
                .expect("finish");
        }
        let store = Store::open(&path).expect("reopen");
        assert_eq!(store.run_count().expect("count"), 1);
    }

    #[test]
    fn a_fresh_database_is_stamped_without_touching_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.db");
        let store = Store::open(&path).expect("open");
        let version: i64 = store
            .with_connection(|connection| {
                connection
                    .query_row("PRAGMA user_version", [], |row| row.get(0))
                    .map_err(Into::into)
            })
            .expect("version");
        assert_eq!(version, DATA_VERSION);
        // A fresh database has nothing to rename and stays usable.
        let run_id = store
            .start_run("guide", "match_preview", true)
            .expect("start");
        assert_eq!(store.recent_runs_for("guide", 10).expect("runs").len(), 1);
        assert_eq!(
            store.run(run_id).expect("run").expect("present").app,
            "guide"
        );
    }

    #[test]
    fn an_old_history_is_renamed_and_only_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.db");
        {
            let store = Store::open(&path).expect("open");
            for (action, dry_run) in [("automatch_preview", true), ("automatch_apply", false)] {
                let run_id = store.start_run("janitor", action, dry_run).expect("start");
                store
                    .finish_run(
                        run_id,
                        RunStatus::Succeeded,
                        Some("ok"),
                        &serde_json::json!({}),
                        None,
                    )
                    .expect("finish");
            }
            // A heal run keeps its action: only the app is renamed.
            let heal_id = store
                .start_run("janitor", "heal_preview", true)
                .expect("start");
            store
                .finish_run(
                    heal_id,
                    RunStatus::Succeeded,
                    Some("ok"),
                    &serde_json::json!({}),
                    None,
                )
                .expect("finish");
            // A row already written under the new name must survive untouched.
            let guide_id = store
                .start_run("guide", "match_preview", true)
                .expect("start");
            store
                .finish_run(
                    guide_id,
                    RunStatus::Succeeded,
                    Some("ok"),
                    &serde_json::json!({}),
                    None,
                )
                .expect("finish");
            store
                .record_settings_change("janitor", &serde_json::json!({"check_hours": 6}))
                .expect("record");
            // Rewind the stamp: this is what a database written before the rename looks like.
            store
                .with_connection(|connection| {
                    connection
                        .execute_batch("PRAGMA user_version = 0")
                        .map_err(Into::into)
                })
                .expect("rewind");
        }

        let store = Store::open(&path).expect("reopen");
        let runs = store.recent_runs_for("guide", 10).expect("runs");
        assert_eq!(runs.len(), 4);
        // Newest first: the pre-existing guide row was written last.
        assert_eq!(runs[0].action, "match_preview");
        assert!(runs.iter().all(|run| run.app == "guide"));
        let actions: Vec<&str> = runs.iter().map(|run| run.action.as_str()).collect();
        assert!(actions.contains(&"match_apply"));
        assert!(actions.contains(&"heal_preview"));
        // The heal run kept its action; only the app was renamed.
        let heal = runs
            .iter()
            .find(|run| run.action == "heal_preview")
            .expect("heal");
        assert_eq!(heal.app, "guide");
        assert!(store
            .recent_runs_for("janitor", 10)
            .expect("runs")
            .is_empty());

        let apps: Vec<String> = store
            .with_connection(|connection| {
                let mut statement = connection.prepare("SELECT app FROM settings_history")?;
                let apps = statement
                    .query_map([], |row| row.get(0))?
                    .collect::<rusqlite::Result<Vec<String>>>()?;
                Ok(apps)
            })
            .expect("settings history");
        assert_eq!(apps, vec!["guide".to_string()]);

        // A second open is a no-op: the stamp says the rewrite already ran, so a row written
        // afterwards keeps whatever app it was given.
        store
            .start_run("janitor", "automatch_apply", false)
            .expect("start");
        let store = Store::open(&path).expect("reopen again");
        assert_eq!(store.recent_runs_for("janitor", 10).expect("runs").len(), 1);
        assert_eq!(store.recent_runs_for("guide", 10).expect("runs").len(), 4);
    }

    /// The schema exactly as v0.1.0 (`e74a3ad`) created it: no channel-operation columns, no
    /// one-run-per-app index, `user_version` 0.
    const V0_1_0_SCHEMA: &str = "
        CREATE TABLE runs (id INTEGER PRIMARY KEY AUTOINCREMENT, app TEXT NOT NULL,
            action TEXT NOT NULL, dry_run INTEGER NOT NULL, started_at TEXT NOT NULL,
            finished_at TEXT, status TEXT NOT NULL, summary TEXT,
            counts TEXT NOT NULL DEFAULT '{}', report_path TEXT);
        CREATE INDEX runs_started ON runs(started_at DESC);
        CREATE INDEX runs_app ON runs(app, started_at DESC);
        CREATE TABLE run_rows (run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL, channel_id INTEGER NOT NULL, channel_name TEXT NOT NULL,
            channel_group TEXT NOT NULL DEFAULT '', channel_number REAL, current_epg_name TEXT,
            epg_data_id INTEGER, epg_name TEXT, epg_source TEXT,
            score INTEGER NOT NULL DEFAULT 0, method TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', ai_decision TEXT,
            PRIMARY KEY (run_id, ordinal));
        CREATE INDEX run_rows_channel ON run_rows(run_id, channel_id);
        CREATE TABLE ai_decisions (run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
            row_ordinal INTEGER NOT NULL, provider TEXT NOT NULL, model TEXT NOT NULL,
            latency_ms INTEGER NOT NULL DEFAULT 0, input_tokens INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0, request TEXT NOT NULL DEFAULT '{}',
            response TEXT NOT NULL DEFAULT '{}', PRIMARY KEY (run_id, row_ordinal));
        CREATE TABLE settings_history (id INTEGER PRIMARY KEY AUTOINCREMENT, app TEXT NOT NULL,
            changed_at TEXT NOT NULL, settings TEXT NOT NULL);
    ";

    #[test]
    fn a_v0_1_0_database_with_stale_running_rows_upgrades_from_either_process() {
        // v0.1.0 had no cross-process guard, so a crash with the CLI and the daemon both on
        // janitor can leave two `running` rows for it. The CLI opening first must still work
        // (it cannot create the index yet), and the daemon's open must retire, rename and
        // index in one go.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.db");
        {
            let connection = Connection::open(&path).expect("open");
            connection
                .execute_batch(V0_1_0_SCHEMA)
                .expect("v0.1.0 schema");
            for action in ["automatch_apply", "heal_apply"] {
                connection
                    .execute(
                        "INSERT INTO runs (app, action, dry_run, started_at, status) \
                         VALUES ('janitor', ?1, 0, '2026-10-30T00:00:00Z', 'running')",
                        params![action],
                    )
                    .expect("stale run");
            }
            connection
                .execute(
                    "INSERT INTO run_rows (run_id, ordinal, channel_id, channel_name) \
                     VALUES (1, 0, 7, 'ESPN')",
                    [],
                )
                .expect("old row");
        }

        let shared = Store::open_shared(&path).expect("the CLI opens a v0.1.0 database");
        // Renamed, and still running: only the daemon may retire them.
        assert_eq!(shared.running_runs("guide").expect("running").len(), 2);
        assert_eq!(shared.rows(1).expect("rows")[0].field, "");
        drop(shared);

        let store = Store::open(&path).expect("the daemon opens it");
        assert!(store.running_runs("guide").expect("running").is_empty());
        let runs = store.recent_runs_for("guide", 10).expect("runs");
        assert!(runs.iter().any(|run| run.action == "match_apply"));
        // The index exists now: a second live run for the app is refused.
        store
            .start_run("guide", "scan", true)
            .expect("first live run");
        assert!(store.start_run("guide", "scan", true).is_err());
    }
}
