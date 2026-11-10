//! The scheduler: cron entries from `[schedules]` in `crosstalk.toml`.
//!
//! The plugin armed its watchdog through Celery's periodic-task table, which meant clicking
//! "Validate Settings" after enabling it before anything ran. Here a schedule is just a config
//! entry, and the UI shows each entry's next run so an operator can see it was understood
//! rather than hope.
//!
//! Cron expressions are written the way people write them — five fields, minute first. The
//! scheduler underneath wants seconds first, so the translation happens in one place
//! ([`normalize_cron`]) and is covered by tests.

use crate::config::CrosstalkConfig;
use crate::runner::JobRunner;
use anyhow::{bail, Context, Result};
use croner::Cron;
use tokio_cron_scheduler::{Job, JobScheduler};

/// One configured schedule, as the UI lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleInfo {
    /// `app.action`, the key in the config file.
    pub key: String,
    pub app: String,
    pub action: String,
    /// The expression as written.
    pub cron: String,
    /// The next time it will fire, when the expression parses.
    pub next_run: Option<String>,
}

/// Turn a five-field cron expression into the six-field form the scheduler needs.
///
/// Five fields are `minute hour day month weekday`; the scheduler's parser (croner) wants
/// seconds at the front. Six fields are passed through, so second precision is available.
/// Seven fields are rejected: croner has no year field, so a seven-field expression would be
/// displayed as valid and then never fire.
pub fn normalize_cron(expression: &str) -> Result<String> {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    match fields.len() {
        5 => Ok(format!("0 {}", fields.join(" "))),
        6 => Ok(fields.join(" ")),
        other => bail!("a cron expression needs 5 or 6 fields, got {other}: {expression:?}"),
    }
}

/// Parse an expression the way the scheduler will.
///
/// The scheduler uses croner, and croner's rules differ from the `cron` crate's: 0 and 7 are
/// both Sunday, 1 is Monday, and day-of-month/day-of-week are ORed rather than ANDed. Parsing
/// with the same library is the only way the "next run" the UI shows can be the run that
/// actually happens.
pub fn parse_schedule(expression: &str) -> Result<Cron> {
    let normalized = normalize_cron(expression)?;
    Cron::new(&normalized)
        .with_seconds_required()
        .parse()
        .with_context(|| format!("invalid cron expression {expression:?}"))
}

/// Split `app.action` into its halves.
fn split_key(key: &str) -> Result<(&str, &str)> {
    key.split_once('.')
        .with_context(|| format!("schedule {key:?} must be written <app>.<action>"))
}

/// The schedules the configuration describes, with their next run times.
///
/// Pure: it reads the config and the clock, never the scheduler, so the UI can show what will
/// happen even before the daemon arms anything.
pub fn describe(config: &CrosstalkConfig) -> Vec<ScheduleInfo> {
    let mut described = Vec::new();
    for (key, expression) in &config.schedules {
        let Ok((app, action)) = split_key(key) else {
            described.push(ScheduleInfo {
                key: key.clone(),
                app: String::new(),
                action: String::new(),
                cron: expression.clone(),
                next_run: None,
            });
            continue;
        };
        described.push(ScheduleInfo {
            key: key.clone(),
            app: app.to_string(),
            action: action.to_string(),
            cron: expression.clone(),
            next_run: next_run(expression),
        });
    }
    described
}

/// A schedule's next firing time, as an ISO 8601 instant.
fn next_run(expression: &str) -> Option<String> {
    let schedule = parse_schedule(expression).ok()?;
    schedule
        .find_next_occurrence(&chrono::Utc::now(), false)
        .ok()
        .map(|instant| instant.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

/// The armed scheduler.
///
/// The armed job ids are tracked so [`Scheduler::rearm`] can remove exactly what `start`
/// armed and arm the new config: saving host settings re-arms without a daemon restart.
pub struct Scheduler {
    inner: JobScheduler,
    jobs: std::sync::Mutex<Vec<uuid::Uuid>>,
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler").finish_non_exhaustive()
    }
}

impl Scheduler {
    /// Arm every schedule that names a real action.
    ///
    /// An entry naming an unknown app or action is skipped with a warning rather than aborting
    /// the daemon: a typo in one schedule should not stop the others, and `crosstalk validate`
    /// reports it as an error.
    pub async fn start(runner: JobRunner, config: &CrosstalkConfig) -> Result<Self> {
        let inner = JobScheduler::new()
            .await
            .context("creating the job scheduler")?;
        let jobs = arm_jobs(&inner, &runner, config).await;
        inner.start().await.context("starting the scheduler")?;
        Ok(Self {
            inner,
            jobs: std::sync::Mutex::new(jobs),
        })
    }

    /// How many jobs are currently armed. Exposed for tests: there is no public job listing
    /// on the scheduler itself, so the count is what proves a re-arm replaced rather than
    /// duplicated.
    pub fn jobs_count(&self) -> usize {
        self.jobs.lock().map(|jobs| jobs.len()).unwrap_or(0)
    }

    /// Drop every armed job and arm the new config in its place.
    ///
    /// Called after a settings save swapped the shared config: the next firing of each
    /// schedule uses the new expression, and schedules removed from the file stop firing.
    /// Runs already in flight are untouched; only their future firings change.
    ///
    /// Arming is per-entry: one bad entry never stops the others, and every successfully
    /// armed job is tracked, including partial successes, so a later re-arm can remove
    /// exactly what is armed.
    pub async fn rearm(&mut self, runner: &JobRunner, config: &CrosstalkConfig) -> Result<()> {
        let armed: Vec<uuid::Uuid> = self
            .jobs
            .lock()
            .map(|mut jobs| std::mem::take(&mut *jobs))
            .unwrap_or_default();
        for id in armed {
            if let Err(error) = self.inner.remove(&id).await {
                tracing::warn!("removing a replaced schedule: {error:#}");
            }
        }
        let jobs = arm_jobs(&self.inner, runner, config).await;
        *self
            .jobs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = jobs;
        tracing::info!(
            count = self.jobs_count(),
            "schedules re-armed from the saved configuration"
        );
        Ok(())
    }

    /// Stop every armed job.
    ///
    /// Schedules fire in UTC; that is the scheduler's behaviour and is worth knowing when an
    /// expression is written for local time.
    ///
    /// Takes `&mut self` so a scheduler shared behind a mutex (the daemon's serve path
    /// shares one between the web UI's re-arm and shutdown) can stop without being moved out.
    pub async fn shutdown(&mut self) {
        if let Err(error) = self.inner.shutdown().await {
            tracing::warn!("shutting the scheduler down: {error:#}");
        }
    }
}

/// Arm every schedule in `config` on `inner`, returning the armed job ids.
///
/// Shared by [`Scheduler::start`] and [`Scheduler::rearm`] so both agree on what "armed"
/// means: entries naming an unknown app or action, or carrying a bad expression, are skipped
/// with a warning and reported by `crosstalk validate`. A job that fails to build or to add
/// is skipped the same way rather than aborting the rest: the ids already armed are returned
/// so the caller still tracks every partial success, and a later re-arm can remove them.
async fn arm_jobs(
    inner: &JobScheduler,
    runner: &JobRunner,
    config: &CrosstalkConfig,
) -> Vec<uuid::Uuid> {
    let mut armed = Vec::new();
    for (key, expression) in &config.schedules {
        let Ok((app, action)) = split_key(key) else {
            tracing::warn!("schedule {key:?} is not <app>.<action>; ignoring it");
            continue;
        };
        if runner.registry().find_action(app, action).is_none() {
            tracing::warn!("schedule {key:?} names an unknown action; ignoring it");
            continue;
        }

        let normalized = match normalize_cron(expression) {
            Ok(normalized) => normalized,
            Err(error) => {
                tracing::warn!("schedule {key:?}: {error:#}; ignoring it");
                continue;
            }
        };
        if let Err(error) = parse_schedule(expression) {
            // Skipping rather than failing: a typo in one schedule must not stop the
            // daemon or the other schedules. `crosstalk validate` reports it as an error.
            tracing::warn!("schedule {key:?}: {error:#}; ignoring it");
            continue;
        }

        let job_runner = runner.clone();
        let app = app.to_string();
        let action = action.to_string();
        let dry_run = runner.scheduled_dry_run(&app, &action);
        let job = match Job::new_async(normalized.as_str(), move |_uuid, _lock| {
            let runner = job_runner.clone();
            let app = app.clone();
            let action = action.clone();
            Box::pin(async move {
                // The flag comes from the action's own kind, so a schedule naming a
                // preview stays a preview instead of writing because a schedule said so.
                match runner.start(&app, &action, dry_run) {
                    Ok(started) => tracing::info!(
                        app,
                        action,
                        run_id = started.run_id,
                        "scheduled run started"
                    ),
                    Err(error) => {
                        tracing::warn!(app, action, "scheduled run not started: {error:#}")
                    }
                }
            })
        }) {
            Ok(job) => job,
            Err(error) => {
                tracing::warn!("schedule {key:?}: {error:#}; ignoring it");
                continue;
            }
        };

        match inner.add(job).await {
            Ok(id) => armed.push(id),
            Err(error) => {
                tracing::warn!("schedule {key:?}: could not be armed: {error:#}; ignoring it");
            }
        }
    }
    armed
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn config_with(schedules: &[(&str, &str)]) -> CrosstalkConfig {
        let mut config = CrosstalkConfig::default();
        let mut map = BTreeMap::new();
        for (key, expression) in schedules {
            map.insert((*key).to_string(), (*expression).to_string());
        }
        config.schedules = map;
        config
    }

    #[test]
    fn five_field_expressions_gain_a_seconds_field() {
        assert_eq!(normalize_cron("0 4 * * *").expect("ok"), "0 0 4 * * *");
        assert_eq!(normalize_cron("*/5 * * * *").expect("ok"), "0 */5 * * * *");
    }

    #[test]
    fn six_field_expressions_pass_through() {
        assert_eq!(normalize_cron("30 0 4 * * *").expect("ok"), "30 0 4 * * *");
    }

    #[test]
    fn a_short_or_seven_field_expression_is_rejected() {
        assert!(normalize_cron("* * *").is_err());
        // Seven fields would carry a year, which the scheduler's parser has no field for.
        assert!(normalize_cron("0 0 4 * * * 2027").is_err());
    }

    #[test]
    fn parsing_agrees_with_the_scheduler_about_weekdays() {
        // croner counts 1 as Monday and 0 or 7 as Sunday, which is the opposite of the `cron`
        // crate's numbering. Both must give the same next run for the same expression.
        for expression in ["0 4 * * 1", "0 4 * * 0", "0 4 * * 7", "0 4 * * MON"] {
            let next = next_run(expression).unwrap_or_else(|| panic!("{expression} parses"));
            let parsed: chrono::DateTime<chrono::Utc> = next.parse().expect("iso");
            assert_eq!(parsed.format("%H:%M").to_string(), "04:00");
        }
        // 1 is Monday.
        let monday = next_run("0 4 * * 1").expect("next");
        let parsed: chrono::DateTime<chrono::Utc> = monday.parse().expect("iso");
        assert_eq!(parsed.format("%A").to_string(), "Monday");
        // 0 and 7 are Sunday.
        for expression in ["0 4 * * 0", "0 4 * * 7"] {
            let sunday = next_run(expression).expect("next");
            let parsed: chrono::DateTime<chrono::Utc> = sunday.parse().expect("iso");
            assert_eq!(parsed.format("%A").to_string(), "Sunday", "{expression}");
        }
    }

    #[test]
    fn a_misspelled_weekday_is_rejected_by_the_real_parser() {
        assert!(parse_schedule("0 4 * * MONDAY").is_err());
        assert!(parse_schedule("0 4 * * MON").is_ok());
    }

    #[test]
    fn describe_reports_the_next_run() {
        let config = config_with(&[("guide.match_apply", "0 4 * * *")]);
        let described = describe(&config);
        assert_eq!(described.len(), 1);
        assert_eq!(described[0].app, "guide");
        assert_eq!(described[0].action, "match_apply");
        assert_eq!(described[0].cron, "0 4 * * *");
        let next = described[0].next_run.as_deref().expect("a next run");
        let parsed: chrono::DateTime<chrono::Utc> = next.parse().expect("iso 8601");
        assert_eq!(parsed.format("%H:%M:%S").to_string(), "04:00:00");
    }

    #[test]
    fn describe_marks_a_bad_key_or_expression_without_failing() {
        let config = config_with(&[("typo", "0 4 * * *"), ("guide.heal_apply", "nope")]);
        let described = describe(&config);
        assert_eq!(described.len(), 2);
        assert!(described.iter().all(|entry| entry.next_run.is_none()));
        assert!(described.iter().any(|entry| entry.app.is_empty()));
    }

    #[tokio::test]
    async fn arming_skips_unknown_actions_and_bad_expressions() {
        let config = config_with(&[
            ("guide.nope", "0 4 * * *"),
            ("not-a-key", "0 4 * * *"),
            ("guide.match_apply", "not a cron"),
        ]);
        let store = crate::store::Store::in_memory().expect("store");
        let runner = JobRunner::new(
            crate::subapp::Registry::new(vec![]),
            crate::config::SharedConfig::new(config.clone()),
            store,
        );
        // Nothing here names a real action, so arming succeeds with no jobs.
        let mut scheduler = Scheduler::start(runner, &config).await.expect("arms");
        assert_eq!(scheduler.jobs_count(), 0);
        scheduler.shutdown().await;
    }

    #[tokio::test]
    async fn rearm_replaces_armed_jobs_without_duplicating() {
        use crate::jobs::JobContext;
        use crate::subapp::JobOutcome;
        use crate::subapp::{ActionKind, ActionSpec};

        struct PreviewApp;
        impl crate::subapp::SubApp for PreviewApp {
            fn id(&self) -> &'static str {
                "guide"
            }
            fn name(&self) -> &'static str {
                "Guide"
            }
            fn description(&self) -> &'static str {
                "test"
            }
            fn actions(&self) -> Vec<ActionSpec> {
                vec![ActionSpec::new("match", "Match", "runs", ActionKind::Read).offline()]
            }
            fn schema(&self) -> &crate::subapp::SettingsSchema {
                static SCHEMA: std::sync::OnceLock<crate::subapp::SettingsSchema> =
                    std::sync::OnceLock::new();
                SCHEMA.get_or_init(crate::subapp::SettingsSchema::default)
            }
            fn load_settings(
                &self,
                _path: &std::path::Path,
            ) -> anyhow::Result<crate::subapp::SettingsValues> {
                Ok(crate::subapp::SettingsValues::new())
            }
            fn save_settings(
                &self,
                _path: &std::path::Path,
                _values: &crate::subapp::SettingsValues,
            ) -> anyhow::Result<()> {
                Ok(())
            }
            fn run<'a>(
                &'a self,
                _action: &'a str,
                _context: &'a JobContext,
                _dry_run: bool,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<JobOutcome>> + Send + 'a>,
            > {
                Box::pin(async move { Ok(JobOutcome::new("finished")) })
            }
        }

        let store = crate::store::Store::in_memory().expect("store");
        let runner = JobRunner::new(
            crate::subapp::Registry::new(vec![Arc::new(PreviewApp)]),
            crate::config::SharedConfig::new(CrosstalkConfig::default()),
            store,
        );
        // Start with no schedules: nothing armed.
        let mut scheduler = Scheduler::start(runner.clone(), &CrosstalkConfig::default())
            .await
            .expect("arms");
        assert_eq!(scheduler.jobs_count(), 0);

        // Re-arm with one schedule: it arms.
        let with_one = config_with(&[("guide.match", "0 4 * * *")]);
        scheduler.rearm(&runner, &with_one).await.expect("re-arms");
        assert_eq!(scheduler.jobs_count(), 1);
        assert_eq!(describe(&with_one).len(), 1);

        // Re-arming with the same config replaces rather than duplicates.
        scheduler
            .rearm(&runner, &with_one)
            .await
            .expect("re-arms again");
        assert_eq!(scheduler.jobs_count(), 1);

        // Clearing the schedules disarms everything.
        scheduler
            .rearm(&runner, &CrosstalkConfig::default())
            .await
            .expect("disarms");
        assert_eq!(scheduler.jobs_count(), 0);
        scheduler.shutdown().await;
    }

    #[tokio::test]
    async fn rearm_tracks_partial_successes_past_bad_entries() {
        use crate::jobs::JobContext;
        use crate::subapp::JobOutcome;
        use crate::subapp::{ActionKind, ActionSpec};

        struct TwoActionApp;
        impl crate::subapp::SubApp for TwoActionApp {
            fn id(&self) -> &'static str {
                "guide"
            }
            fn name(&self) -> &'static str {
                "Guide"
            }
            fn description(&self) -> &'static str {
                "test"
            }
            fn actions(&self) -> Vec<ActionSpec> {
                vec![
                    ActionSpec::new("match", "Match", "runs", ActionKind::Read).offline(),
                    ActionSpec::new("heal", "Heal", "runs", ActionKind::Read).offline(),
                ]
            }
            fn schema(&self) -> &crate::subapp::SettingsSchema {
                static SCHEMA: std::sync::OnceLock<crate::subapp::SettingsSchema> =
                    std::sync::OnceLock::new();
                SCHEMA.get_or_init(crate::subapp::SettingsSchema::default)
            }
            fn load_settings(
                &self,
                _path: &std::path::Path,
            ) -> anyhow::Result<crate::subapp::SettingsValues> {
                Ok(crate::subapp::SettingsValues::new())
            }
            fn save_settings(
                &self,
                _path: &std::path::Path,
                _values: &crate::subapp::SettingsValues,
            ) -> anyhow::Result<()> {
                Ok(())
            }
            fn run<'a>(
                &'a self,
                _action: &'a str,
                _context: &'a JobContext,
                _dry_run: bool,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<JobOutcome>> + Send + 'a>,
            > {
                Box::pin(async move { Ok(JobOutcome::new("finished")) })
            }
        }

        let store = crate::store::Store::in_memory().expect("store");
        let runner = JobRunner::new(
            crate::subapp::Registry::new(vec![Arc::new(TwoActionApp)]),
            crate::config::SharedConfig::new(CrosstalkConfig::default()),
            store,
        );
        let mut scheduler = Scheduler::start(runner.clone(), &CrosstalkConfig::default())
            .await
            .expect("arms");

        // BTreeMap order puts the bad entries on both sides of the good one: an unknown
        // action first, a bad expression on a known action last. The good entry must still
        // arm, and its id must be tracked.
        let partial = config_with(&[
            ("guide.aardvark", "0 4 * * *"),
            ("guide.match", "0 4 * * *"),
            ("guide.heal", "not a cron"),
        ]);
        scheduler.rearm(&runner, &partial).await.expect("re-arms");
        assert_eq!(scheduler.jobs_count(), 1);

        // A later re-arm removes exactly what was tracked: nothing is left armed.
        scheduler
            .rearm(&runner, &CrosstalkConfig::default())
            .await
            .expect("disarms");
        assert_eq!(scheduler.jobs_count(), 0);
        scheduler.shutdown().await;
    }
}
