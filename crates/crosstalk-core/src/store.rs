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

/// How a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
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
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
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
        store.migrate()?;
        Ok(store)
    }

    /// An in-memory store, for tests.
    pub fn in_memory() -> Result<Self> {
        let store = Self {
            connection: Arc::new(Mutex::new(Connection::open_in_memory()?)),
        };
        store.migrate()?;
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
    fn migrate(&self) -> Result<()> {
        self.with_connection(|connection| {
            connection.execute_batch(
                r#"
                PRAGMA journal_mode = WAL;
                PRAGMA foreign_keys = ON;

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
            // skips running rows, so it would otherwise show as live forever.
            connection.execute(
                "UPDATE runs SET status = 'failed', \
                 summary = COALESCE(summary, 'the daemon restarted while this run was in progress'), \
                 finished_at = COALESCE(finished_at, ?1) WHERE status = 'running'",
                params![chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)],
            )?;
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
                     epg_source, score, method, status, reason, ai_decision) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
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
                 reason, ai_decision FROM run_rows WHERE run_id = ?1 ORDER BY ordinal",
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
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
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
        }
    }

    #[test]
    fn a_run_round_trips() {
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run("janitor", "automatch_apply", false)
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
    fn rows_are_replaced_not_duplicated() {
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run("janitor", "heal_apply", true)
            .expect("start");
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
            .start_run("janitor", "automatch_preview", true)
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
            .start_run("janitor", "automatch_preview", true)
            .expect("start");
        // SQLite's timestamp resolution is a second, so order by id as well; this sleeps to
        // make the timestamps differ where the platform allows it.
        std::thread::sleep(std::time::Duration::from_millis(5));
        let second = store.start_run("other", "something", true).expect("start");

        let all = store.recent_runs(10).expect("runs");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, second);
        assert_eq!(all[1].id, first);

        let janitor_only = store.recent_runs_for("janitor", 10).expect("runs");
        assert_eq!(janitor_only.len(), 1);
        assert_eq!(janitor_only[0].id, first);

        assert_eq!(store.run_count().expect("count"), 2);
    }

    #[test]
    fn settings_changes_are_recorded() {
        let store = Store::in_memory().expect("store");
        store
            .record_settings_change("janitor", &serde_json::json!({"check_hours": 6}))
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
            .start_run("janitor", "automatch_preview", true)
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
            .start_run("janitor", "automatch_preview", true)
            .expect("start");
        assert_eq!(store.prune(0, 0).expect("prune"), 0);
        assert_eq!(store.run_count().expect("count"), 1);
    }

    #[test]
    fn a_running_run_is_never_pruned() {
        let store = Store::in_memory().expect("store");
        let run_id = store
            .start_run("janitor", "automatch_apply", false)
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
    fn a_file_backed_store_reopens_with_its_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("crosstalk.db");
        {
            let store = Store::open(&path).expect("open");
            let run_id = store
                .start_run("janitor", "automatch_preview", true)
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
}
