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
use std::str::FromStr;
use std::sync::Arc;
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
/// Five fields are `minute hour day month weekday`; six add seconds at the front. Anything
/// already six or seven fields is passed through, so a writer who wants second precision can
/// have it.
pub fn normalize_cron(expression: &str) -> Result<String> {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    match fields.len() {
        5 => Ok(format!("0 {}", fields.join(" "))),
        6 | 7 => Ok(fields.join(" ")),
        other => bail!("a cron expression needs 5 to 7 fields, got {other}: {expression:?}"),
    }
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
    let normalized = normalize_cron(expression).ok()?;
    let schedule = cron::Schedule::from_str(&normalized).ok()?;
    schedule
        .upcoming(chrono::Utc)
        .next()
        .map(|instant| instant.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

/// The armed scheduler.
pub struct Scheduler {
    inner: JobScheduler,
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

            let job_runner = runner.clone();
            let app = app.to_string();
            let action = action.to_string();
            let job = Job::new_async(normalized.as_str(), move |_uuid, _lock| {
                let runner = job_runner.clone();
                let app = app.clone();
                let action = action.clone();
                Box::pin(async move {
                    // A scheduled run is an apply: the point of a schedule is that it acts.
                    match runner.start(&app, &action, false) {
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
            })
            .with_context(|| format!("parsing the cron expression for {key:?}"))?;

            inner
                .add(job)
                .await
                .with_context(|| format!("arming the schedule {key:?}"))?;
        }

        inner.start().await.context("starting the scheduler")?;
        Ok(Self { inner })
    }

    /// Stop every armed job.
    pub async fn shutdown(mut self) {
        if let Err(error) = self.inner.shutdown().await {
            tracing::warn!("shutting the scheduler down: {error:#}");
        }
    }
}

/// Build a runner and scheduler pair, for the daemon and for tests.
pub async fn build(
    registry: crate::subapp::Registry,
    config: Arc<CrosstalkConfig>,
    store: crate::store::Store,
) -> Result<(JobRunner, Scheduler)> {
    let runner = JobRunner::new(registry, Arc::clone(&config), store);
    let scheduler = Scheduler::start(runner.clone(), &config).await?;
    Ok((runner, scheduler))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

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
    fn six_and_seven_field_expressions_pass_through() {
        assert_eq!(normalize_cron("30 0 4 * * *").expect("ok"), "30 0 4 * * *");
        assert_eq!(
            normalize_cron("0 0 4 * * * 2027").expect("ok"),
            "0 0 4 * * * 2027"
        );
    }

    #[test]
    fn a_short_or_long_expression_is_rejected() {
        assert!(normalize_cron("* * *").is_err());
        assert!(normalize_cron("* * * * * * * *").is_err());
    }

    #[test]
    fn describe_reports_the_next_run() {
        let config = config_with(&[("janitor.automatch_apply", "0 4 * * *")]);
        let described = describe(&config);
        assert_eq!(described.len(), 1);
        assert_eq!(described[0].app, "janitor");
        assert_eq!(described[0].action, "automatch_apply");
        assert_eq!(described[0].cron, "0 4 * * *");
        let next = described[0].next_run.as_deref().expect("a next run");
        let parsed: chrono::DateTime<chrono::Utc> = next.parse().expect("iso 8601");
        assert_eq!(parsed.format("%H:%M:%S").to_string(), "04:00:00");
    }

    #[test]
    fn describe_marks_a_bad_key_or_expression_without_failing() {
        let config = config_with(&[("typo", "0 4 * * *"), ("janitor.heal_apply", "nope")]);
        let described = describe(&config);
        assert_eq!(described.len(), 2);
        assert!(described.iter().all(|entry| entry.next_run.is_none()));
        assert!(described.iter().any(|entry| entry.app.is_empty()));
    }

    #[tokio::test]
    async fn arming_skips_unknown_actions_and_bad_expressions() {
        let config = config_with(&[
            ("janitor.nope", "0 4 * * *"),
            ("not-a-key", "0 4 * * *"),
            ("janitor.automatch_apply", "not a cron"),
        ]);
        let store = crate::store::Store::in_memory().expect("store");
        let runner = JobRunner::new(
            crate::subapp::Registry::new(vec![]),
            Arc::new(config.clone()),
            store,
        );
        // Nothing here names a real action, so arming succeeds with no jobs.
        let scheduler = Scheduler::start(runner, &config).await.expect("arms");
        scheduler.shutdown().await;
    }
}
