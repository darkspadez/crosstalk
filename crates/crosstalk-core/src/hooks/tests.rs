//! Hook tests: the lenient parse, the debounce, and the single follow-up a busy target earns.
//!
//! The payloads are Dispatcharr's real shapes, taken from its callers (`log_system_event` in
//! `apps/m3u/tasks.py` and `apps/epg/tasks.py`): a refresh's details dict, form-encoded when no
//! payload template is set, and carrying no event name at all.

use super::*;
use crate::config::{CrosstalkConfig, WorkflowConfig};
use crate::subapp::{ActionKind, ActionSpec, JobOutcome, SettingsSchema, SettingsValues, SubApp};
use std::sync::Arc;

/// An app with a fast action and a slow one, so "busy" is a real state rather than a mock.
struct SlowApp;

impl SubApp for SlowApp {
    fn id(&self) -> &'static str {
        "poke"
    }

    fn name(&self) -> &'static str {
        "Poke"
    }

    fn description(&self) -> &'static str {
        "test app"
    }

    fn actions(&self) -> Vec<ActionSpec> {
        vec![
            ActionSpec::new("fast", "Fast", "returns at once", ActionKind::Read).offline(),
            ActionSpec::new("slow", "Slow", "takes a moment", ActionKind::Read).offline(),
        ]
    }

    fn schema(&self) -> &SettingsSchema {
        static SCHEMA: std::sync::OnceLock<SettingsSchema> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(SettingsSchema::default)
    }

    fn load_settings(&self, _path: &std::path::Path) -> Result<SettingsValues> {
        Ok(SettingsValues::new())
    }

    fn save_settings(&self, _path: &std::path::Path, _values: &SettingsValues) -> Result<()> {
        Ok(())
    }

    fn run<'a>(
        &'a self,
        action: &'a str,
        _context: &'a crate::jobs::JobContext,
        _dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JobOutcome>> + Send + 'a>> {
        Box::pin(async move {
            if action == "slow" {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Ok(JobOutcome::new("done"))
        })
    }
}

fn config(events: &[(&str, &[&str])], debounce_secs: f64) -> CrosstalkConfig {
    let mut config = CrosstalkConfig::default();
    config.hooks.debounce_secs = debounce_secs;
    config.hooks.events = events
        .iter()
        .map(|(event, targets)| {
            (
                (*event).to_string(),
                targets.iter().map(|target| (*target).to_string()).collect(),
            )
        })
        .collect();
    config
}

fn service(config: CrosstalkConfig) -> (Arc<Hooks>, JobRunner) {
    let runner = JobRunner::new(
        crate::subapp::Registry::new(vec![Arc::new(SlowApp)]),
        crate::config::SharedConfig::new(config),
        crate::store::Store::in_memory().expect("store"),
    );
    let service = Hooks::new_with_pacing(
        runner.clone(),
        Duration::from_millis(20),
        Duration::from_secs(5),
    );
    (service, runner)
}

/// How many runs of `app` the store holds.
fn run_count(runner: &JobRunner, app: &str) -> usize {
    runner
        .store()
        .recent_runs_for(app, 100)
        .expect("runs")
        .len()
}

async fn wait_for_runs(runner: &JobRunner, app: &str, wanted: usize) {
    for _ in 0..200 {
        if run_count(runner, app) >= wanted && !runner.is_busy(app) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "expected {wanted} run(s) of {app}, saw {}",
        run_count(runner, app)
    );
}

#[test]
fn a_json_body_is_read_as_fields() {
    let fields = read_fields(
        br#"{"event": "m3u_refresh", "account_name": "Provider A", "streams_created": 12}"#,
    );
    assert_eq!(fields.get("event").map(String::as_str), Some("m3u_refresh"));
    assert_eq!(
        fields.get("account_name").map(String::as_str),
        Some("Provider A")
    );
    // A number becomes the text a form field would have carried.
    assert_eq!(
        fields.get("streams_created").map(String::as_str),
        Some("12")
    );
}

#[test]
fn dispatcharrs_default_form_body_is_read_too() {
    // What `requests.post(url, data=payload_dict)` sends when no payload template is set.
    let fields = read_fields(
        b"account_name=Provider+A&elapsed_time=12.3&streams_created=5&streams_deleted=0",
    );
    assert_eq!(
        fields.get("account_name").map(String::as_str),
        Some("Provider A")
    );
    assert_eq!(fields.get("elapsed_time").map(String::as_str), Some("12.3"));
}

#[test]
fn an_odd_body_does_not_fail_the_delivery() {
    // A malformed escape, a lone key and an empty body are all "no fields", not an error: a
    // delivery that panics is a delivery Dispatcharr retries forever.
    assert_eq!(
        read_fields(b"account_name=100%")
            .get("account_name")
            .map(String::as_str),
        Some("100%")
    );
    assert!(read_fields(b"justakey").contains_key("justakey"));
    assert!(read_fields(b"").is_empty());
    assert!(read_fields(&[0xff, 0xfe]).is_empty());
}

#[test]
fn percent_escapes_decode() {
    assert_eq!(percent_decode("Provider%20A"), "Provider A");
    assert_eq!(percent_decode("a%2Bb"), "a+b");
    assert_eq!(percent_decode("50%25"), "50%");
    assert_eq!(percent_decode("%zz"), "%zz");
}

#[test]
fn the_event_name_comes_from_the_query_the_body_or_the_shape() {
    let configured: BTreeSet<String> = ["m3u_refresh", "epg_refresh"]
        .into_iter()
        .map(str::to_string)
        .collect();

    let query = read_event(Some("m3u_refresh"), &BTreeMap::new(), &configured).expect("query");
    assert_eq!(query.source, EventSource::Query);

    let mut fields = BTreeMap::new();
    fields.insert("event".to_string(), "epg_refresh".to_string());
    let field = read_event(None, &fields, &configured).expect("event field");
    assert_eq!(field.name, "epg_refresh");
    assert_eq!(field.source, EventSource::Field);

    // Dispatcharr's websocket-shaped payloads carry `type`.
    let mut fields = BTreeMap::new();
    fields.insert("type".to_string(), "m3u_refresh".to_string());
    assert_eq!(
        read_event(None, &fields, &configured).expect("type").source,
        EventSource::Type
    );

    // A default delivery names no event at all: the payload's own keys decide, and only for an
    // event the configuration names. The bodies below are Dispatcharr's real details dicts.
    let mut fields = BTreeMap::new();
    fields.insert("account_name".to_string(), "Provider A".to_string());
    fields.insert("elapsed_time".to_string(), "12.3".to_string());
    fields.insert("total_processed".to_string(), "100".to_string());
    let inferred = read_event(None, &fields, &configured).expect("shape");
    assert_eq!(inferred.name, "m3u_refresh");
    assert_eq!(inferred.source, EventSource::PayloadShape);

    let mut fields = BTreeMap::new();
    fields.insert("source_name".to_string(), "Schedules Direct".to_string());
    fields.insert("programs".to_string(), "1000".to_string());
    assert_eq!(
        read_event(None, &fields, &configured).expect("shape").name,
        "epg_refresh"
    );

    // An error delivery shares the name field but carries no counters: it must not be
    // mistaken for the refresh it reports the failure of.
    let mut fields = BTreeMap::new();
    fields.insert("account_name".to_string(), "Provider A".to_string());
    fields.insert("message".to_string(), "connection refused".to_string());
    assert!(read_event(None, &fields, &configured).is_none());
    let mut fields = BTreeMap::new();
    fields.insert("source_name".to_string(), "Schedules Direct".to_string());
    fields.insert("message".to_string(), "connection refused".to_string());
    assert!(read_event(None, &fields, &configured).is_none());

    // The same body against a configuration that names neither is an unknown event, not a guess.
    assert!(read_event(None, &fields, &BTreeSet::new()).is_none());
    // And nothing at all is nothing.
    assert!(read_event(None, &BTreeMap::new(), &configured).is_none());
}

#[test]
fn an_overlong_event_name_is_unknown_not_echoed() {
    // The name comes back in the 202 body and the log line, so a megabyte of `?event=` must
    // not pass through unbounded: overlong names from a delivery are refused as unknown.
    let configured: BTreeSet<String> = ["m3u_refresh"].into_iter().map(str::to_string).collect();
    let long = "e".repeat(1024);
    assert!(read_event(Some(&long), &BTreeMap::new(), &configured).is_none());
    let mut fields = BTreeMap::new();
    fields.insert("event".to_string(), long.clone());
    assert!(read_event(None, &fields, &configured).is_none());
    fields.insert("event".to_string(), "m3u_refresh".to_string());
    assert!(read_event(None, &fields, &configured).is_some());
}

#[tokio::test]
async fn an_unknown_or_unconfigured_event_starts_nothing() {
    let (hooks, runner) = service(config(&[("m3u_refresh", &["poke.fast"])], 0.0));
    assert_eq!(hooks.deliver("channel_start"), Delivered::Unknown);

    let (hooks, _runner) = service(config(&[("m3u_refresh", &[])], 0.0));
    assert_eq!(hooks.deliver("m3u_refresh"), Delivered::Unconfigured);
    assert_eq!(run_count(&runner, "poke"), 0);
}

#[tokio::test]
async fn a_burst_of_deliveries_becomes_one_run() {
    let (hooks, runner) = service(config(&[("m3u_refresh", &["poke.fast"])], 0.15));
    // Six accounts refreshing together, as Stream-Mapparr's `on_m3u_refresh` saw them.
    for _ in 0..6 {
        hooks.deliver("m3u_refresh");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(hooks.settling(), 1, "one burst, not six overlapping ones");
    wait_for_runs(&runner, "poke", 1).await;
    // Nothing else fires: the burst produced exactly one run.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(run_count(&runner, "poke"), 1);
    assert_eq!(hooks.settling(), 0, "the burst entry is cleared");
}

#[tokio::test]
async fn a_busy_target_gets_exactly_one_follow_up() {
    let (hooks, runner) = service(config(&[("m3u_refresh", &["poke.slow"])], 0.0));
    // Something already holds the app, so the hook cannot start its target.
    let live = runner.start("poke", "slow", false).expect("a live run");
    assert!(runner.is_busy("poke"));

    hooks.deliver("m3u_refresh");
    // Wait for the (zero-length) debounce to fire and find the app busy.
    for _ in 0..200 {
        if !hooks.queued_follow_ups().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        hooks.queued_follow_ups(),
        vec!["poke.slow".to_string()],
        "one follow-up is queued"
    );

    // More deliveries during the same busy stretch do not queue more.
    for _ in 0..5 {
        hooks.deliver("m3u_refresh");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(hooks.queued_follow_ups().len(), 1);

    // The live run ends, and exactly one follow-up runs in its place.
    runner.cancel(live.run_id);
    for _ in 0..300 {
        if !runner.is_busy("poke") && hooks.queued_follow_ups().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    wait_for_runs(&runner, "poke", 2).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        run_count(&runner, "poke"),
        2,
        "the queued follow-up ran once, not six times"
    );
}

#[tokio::test]
async fn a_hook_can_trigger_a_workflow() {
    let mut config = config(&[("epg_refresh", &["workflow.nightly"])], 0.0);
    config.workflows.insert(
        "nightly".to_string(),
        WorkflowConfig {
            steps: vec!["poke.fast".to_string()],
            fresh: false,
        },
    );
    let (hooks, runner) = service(config);
    assert_eq!(
        hooks.deliver("epg_refresh"),
        Delivered::Scheduled {
            event: "epg_refresh".to_string(),
            targets: 1
        }
    );
    wait_for_runs(&runner, crate::workflow::APP_ID, 1).await;
    let record = runner
        .store()
        .recent_runs_for(crate::workflow::APP_ID, 1)
        .expect("runs")
        .into_iter()
        .next()
        .expect("a workflow run");
    assert_eq!(record.action, "nightly");
    assert_eq!(record.status, crate::store::RunStatus::Succeeded);
    assert_eq!(
        runner.store().child_runs(record.id).expect("steps").len(),
        1,
        "the workflow ran its step"
    );
}

#[tokio::test]
async fn a_hook_target_that_names_nothing_is_logged_and_skipped() {
    let (hooks, runner) = service(config(&[("m3u_refresh", &["poke.nope"])], 0.0));
    hooks.deliver("m3u_refresh");
    // Wait for the burst to settle and the target to be tried.
    for _ in 0..200 {
        if hooks.settling() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(run_count(&runner, "poke"), 0);
    assert!(
        hooks.queued_follow_ups().is_empty(),
        "an unknown action is not 'busy', so nothing is queued for later"
    );
}
