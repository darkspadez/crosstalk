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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
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
        }
    }

    /// Whether this run is still going, for a template that must not compare strings.
    pub fn is_running(&self) -> bool {
        matches!(self, RunStatus::Running)
    }

    fn parse(text: &str) -> Self {
        match text {
            "succeeded" => RunStatus::Succeeded,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
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
}

/// One row of a run: a channel and what was decided for it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RunRow {
    pub ordinal: i64,
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

/// One AI request and its answer, kept for auditing why a row was accepted.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AiDecision {
    pub row_ordinal: i64,
    pub provider: String,
    pub model: String,
    pub latency_ms: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub request: serde_json::Value,
    pub response: serde_json::Value,
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
                    report_path  TEXT
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
                    PRIMARY KEY (run_id, row_ordinal)
                );

                CREATE TABLE IF NOT EXISTS settings_history (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    app         TEXT NOT NULL,
                    changed_at  TEXT NOT NULL,
                    --  is reserved in SQLite.
                    settings    TEXT NOT NULL
                );
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
            // One live run per app, across processes: the in-memory busy map stops two runs
            // in THIS process, but the CLI is another process on the same file. A partial
            // index (no extra row when nothing is running), created after the retire above
            // so a restarted daemon never trips on its own stale rows.
            connection.execute_batch(
                "CREATE UNIQUE INDEX IF NOT EXISTS runs_one_running_per_app ON runs(app) WHERE \
                 status = 'running'",
            )?;
            add_run_row_columns(connection)?;
            rename_recorded_history(connection)?;
            Ok(())
        })
    }

    /// Start a run and return its id.
    pub fn start_run(&self, app: &str, action: &str, dry_run: bool) -> Result<i64> {
        let started = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        self.with_connection(|connection| {
            connection.execute(
                "INSERT INTO runs (app, action, dry_run, started_at, status, counts) \
                 VALUES (?1, ?2, ?3, ?4, ?5, '{}')",
                params![
                    app,
                    action,
                    i64::from(dry_run),
                    started,
                    RunStatus::Running.as_str()
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
                     before_value, after_value) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, \
                     ?16, ?17, ?18)",
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
        self.with_connection(|connection| {
            connection.execute(
                "INSERT OR REPLACE INTO ai_decisions (run_id, row_ordinal, provider, model, \
                 latency_ms, input_tokens, output_tokens, request, response) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
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
            let mut statement = connection.prepare(
                "SELECT id, app, action, dry_run, started_at, finished_at, status, summary, \
                 counts, report_path FROM runs ORDER BY started_at DESC, id DESC LIMIT ?1",
            )?;
            let rows = statement.query_map(params![limit as i64], map_run)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
    }

    /// Runs for one app, newest first.
    pub fn recent_runs_for(&self, app: &str, limit: usize) -> Result<Vec<RunRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT id, app, action, dry_run, started_at, finished_at, status, summary, \
                 counts, report_path FROM runs WHERE app = ?2 \
                 ORDER BY started_at DESC, id DESC LIMIT ?1",
            )?;
            let rows = statement.query_map(params![limit as i64, app], map_run)?;
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
            let mut statement = connection.prepare(
                "SELECT id, app, action, dry_run, started_at, finished_at, status, summary, \
                 counts, report_path FROM runs WHERE app = ?1 AND action = ?2 AND status = ?3 \
                 ORDER BY started_at DESC, id DESC LIMIT 1",
            )?;
            statement
                .query_row(params![app, action, RunStatus::Succeeded.as_str()], map_run)
                .optional()
                .map_err(Into::into)
        })
    }

    /// The live (`running`) runs of one app, oldest first.
    ///
    /// The cross-process guard: a second process (the one-shot CLI) refuses to start an
    /// app that has one of these, because the in-process busy map cannot see it. A row
    /// here is either the daemon's live run or a stale row from a crash, which only the
    /// daemon's next [`Store::open`] retires — so the caller reports both cases and lets
    /// the operator decide, rather than retiring what might be alive.
    pub fn running_runs(&self, app: &str) -> Result<Vec<RunRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT id, app, action, dry_run, started_at, finished_at, status, summary, \
                 counts, report_path FROM runs WHERE app = ?1 AND status = ?2 \
                 ORDER BY started_at, id",
            )?;
            let rows = statement.query_map(params![app, RunStatus::Running.as_str()], map_run)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
    }

    /// One run by id.
    pub fn run(&self, run_id: i64) -> Result<Option<RunRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT id, app, action, dry_run, started_at, finished_at, status, summary, \
                 counts, report_path FROM runs WHERE id = ?1",
            )?;
            statement
                .query_row(params![run_id], map_run)
                .optional()
                .map_err(Into::into)
        })
    }

    /// The rows of a run, in the order the run produced them.
    pub fn rows(&self, run_id: i64) -> Result<Vec<RunRow>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT ordinal, channel_id, channel_name, channel_group, channel_number, \
                 current_epg_name, epg_data_id, epg_name, epg_source, score, method, status, \
                 reason, ai_decision, field, before_value, after_value FROM run_rows \
                 WHERE run_id = ?1 ORDER BY ordinal",
            )?;
            let rows = statement.query_map(params![run_id], |row| {
                Ok(RunRow {
                    ordinal: row.get(0)?,
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
            })?;
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
                 request, response FROM ai_decisions WHERE run_id = ?1 AND row_ordinal = ?2",
            )?;
            statement
                .query_row(params![run_id, ordinal], |row| {
                    let request: String = row.get(6)?;
                    let response: String = row.get(7)?;
                    Ok(AiDecision {
                        row_ordinal: row.get(0)?,
                        provider: row.get(1)?,
                        model: row.get(2)?,
                        latency_ms: row.get(3)?,
                        input_tokens: row.get(4)?,
                        output_tokens: row.get(5)?,
                        request: serde_json::from_str(&request).unwrap_or_default(),
                        response: serde_json::from_str(&response).unwrap_or_default(),
                    })
                })
                .optional()
                .map_err(Into::into)
        })
    }

    /// Delete finished runs older than `keep_days`, keeping the most recent `keep_minimum`
    /// per app. Returns how many runs were removed.
    pub fn prune(&self, keep_days: u32, keep_minimum: usize) -> Result<usize> {
        if keep_days == 0 {
            return Ok(0);
        }
        let cutoff = (chrono::Utc::now() - chrono::Duration::days(i64::from(keep_days)))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        self.with_connection(|connection| {
            let removed = connection.execute(
                "DELETE FROM runs WHERE status != 'running' AND started_at < ?1 AND id NOT IN ( \
                     SELECT id FROM ( \
                         SELECT id, ROW_NUMBER() OVER (PARTITION BY app \
                             ORDER BY started_at DESC, id DESC) AS position \
                         FROM runs WHERE status != 'running' \
                     ) WHERE position <= ?2 \
                 )",
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

/// Add the columns channel operations record, when this database predates them.
///
/// `CREATE TABLE IF NOT EXISTS` leaves an existing `run_rows` alone, so a store written by an
/// earlier build would reject the new inserts. The columns are nullable or defaulted, so an
/// old row reads back as "no channel-operation data", which is exactly what it is.
fn add_run_row_columns(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(run_rows)")?;
    let existing: std::collections::HashSet<String> = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    for (name, definition) in [
        ("field", "TEXT NOT NULL DEFAULT ''"),
        ("before_value", "TEXT"),
        ("after_value", "TEXT"),
    ] {
        if !existing.contains(name) {
            connection.execute_batch(&format!(
                "ALTER TABLE run_rows ADD COLUMN {name} {definition}"
            ))?;
        }
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_row(ordinal: i64, name: &str, score: i64) -> RunRow {
        RunRow {
            ordinal,
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
        };
        store.record_ai_decision(run_id, &decision).expect("record");

        let loaded = store
            .ai_decision(run_id, 3)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.model, "jev-latest");
        assert_eq!(loaded.latency_ms, 120);
        assert!(store.ai_decision(run_id, 4).expect("load").is_none());
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
}
