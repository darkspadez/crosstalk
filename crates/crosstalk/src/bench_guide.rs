//! `crosstalk bench guide --synthetic`: the guide plans (match, scan, heal) at the plan's
//! scale against an in-process fake Dispatcharr (docs/audit/E-perf.md, E-S9).
//!
//! Guide's plans are mostly I/O: each asks Dispatcharr which EPG entries carry programmes.
//! The fake answers the routes those checks read — the per-entry and per-source programme
//! search and `POST /epg/current-programs/` — after a configurable delay per request, and
//! counts every request by kind, so the report says what a run costs in requests as well
//! as in seconds. The world is built, not loaded; every run is a preview (nothing is
//! written to Dispatcharr) recorded in a file store, as a daemon run is.

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use crosstalk_core::config::{CrosstalkConfig, ScopeConfig};
use crosstalk_core::dispatcharr::{
    Channel, ChannelGroup, DispatcharrClient, EpgEntry, EpgSource, World,
};
use crosstalk_core::jobs::{JobContext, NullSink};
use crosstalk_core::store::{RunStatus, Store};
use crosstalk_guide::settings::GuideSettings;
use crosstalk_streams::bench::{thousands, BenchPhase, BenchReport};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The shape of the synthetic guide world and the fake server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuideBenchOptions {
    /// In-scope channels.
    pub channels: usize,
    /// EPG entries, spread over `sources`.
    pub entries: usize,
    pub sources: usize,
    /// What every fake request waits before it answers, in milliseconds.
    pub delay_ms: u64,
}

impl Default for GuideBenchOptions {
    fn default() -> Self {
        Self {
            channels: 20_000,
            entries: 55_000,
            sources: 20,
            delay_ms: 2,
        }
    }
}

impl GuideBenchOptions {
    pub fn checked(self) -> Result<Self> {
        if self.channels == 0 || self.entries == 0 || self.sources == 0 {
            anyhow::bail!(
                "a synthetic guide world needs at least one channel, one EPG entry and one source"
            );
        }
        Ok(self)
    }

    fn source_of(&self, entry: i64) -> i64 {
        entry % self.sources as i64 + 1
    }

    /// One source in ten carries nothing in the window (its guide ran out).
    fn source_empty(source: i64) -> bool {
        source % 10 == 0
    }

    /// Whether an entry carries programmes in the window: never on an empty source, and
    /// one entry in seven is empty anyway.
    fn has_programmes(&self, entry: i64) -> bool {
        !Self::source_empty(self.source_of(entry)) && entry % 7 != 0
    }

    /// Whether a programme is airing on the entry now: most entries with programmes, so
    /// the rest exercise the per-entry fallback.
    fn airing(&self, entry: i64) -> bool {
        self.has_programmes(entry) && entry % 11 != 0
    }
}

fn tvg_id(entry: i64) -> String {
    format!("e{entry}.tv")
}

fn entry_of(tvg_id: &str) -> Option<i64> {
    tvg_id.strip_prefix('e')?.strip_suffix(".tv")?.parse().ok()
}

/// Request counters, by what the request asked.
#[derive(Default)]
struct Counts {
    searches: AtomicUsize,
    source_probes: AtomicUsize,
    batches: AtomicUsize,
}

impl Counts {
    fn take(&self) -> (usize, usize, usize) {
        (
            self.searches.swap(0, Ordering::Relaxed),
            self.source_probes.swap(0, Ordering::Relaxed),
            self.batches.swap(0, Ordering::Relaxed),
        )
    }
}

struct Fake {
    options: GuideBenchOptions,
    counts: Counts,
}

async fn search(
    State(fake): State<Arc<Fake>>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    tokio::time::sleep(Duration::from_millis(fake.options.delay_ms)).await;
    let found = if let Some(tvg) = query.get("tvg_id") {
        fake.counts.searches.fetch_add(1, Ordering::Relaxed);
        entry_of(tvg).is_some_and(|entry| fake.options.has_programmes(entry))
    } else {
        fake.counts.source_probes.fetch_add(1, Ordering::Relaxed);
        query
            .get("epg_source")
            .and_then(|source| source.parse::<i64>().ok())
            .is_some_and(|source| !GuideBenchOptions::source_empty(source))
    };
    Json(if found {
        serde_json::json!({"count": 1, "results": [{"id": 1, "end_time": "2099-01-01T00:00:00Z"}]})
    } else {
        serde_json::json!({"count": 0, "results": []})
    })
}

async fn current_programs(
    State(fake): State<Arc<Fake>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    tokio::time::sleep(Duration::from_millis(fake.options.delay_ms)).await;
    fake.counts.batches.fetch_add(1, Ordering::Relaxed);
    let now = chrono::Utc::now();
    let programs: Vec<serde_json::Value> = body["epg_data_ids"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_i64)
        .filter_map(|entry| {
            if entry % 97 == 0 {
                // An index being rebuilt: settles nothing, as upstream's marker.
                return Some(serde_json::json!({"epg_data_id": entry, "parsing": true}));
            }
            fake.options.airing(entry).then(|| {
                serde_json::json!({
                    "epg_data_id": entry,
                    "title": "Programme",
                    "start_time": (now - chrono::Duration::minutes(10)).to_rfc3339(),
                    "end_time": (now + chrono::Duration::minutes(20)).to_rfc3339(),
                    "tvg_id": tvg_id(entry),
                })
            })
        })
        .collect();
    Json(serde_json::Value::Array(programs))
}

/// Provider spellings of a guide entry and of a channel, cycled over the database names.
const ENTRY_DECORATIONS: [&str; 5] = [
    "{name}",
    "{name} HD",
    "{name} (East)",
    "US: {name}",
    "{name} [US]",
];
const CHANNEL_DECORATIONS: [&str; 4] = ["{name}", "US: {name} HD", "{name} FHD", "USA | {name}"];

/// The synthetic world: entries named from the US channel database (several spellings
/// per name, spread over the sources), channels named the same way in 50 groups, three in
/// four assigned a guide.
pub fn synthetic_world(options: &GuideBenchOptions) -> World {
    let base: Vec<String> = crosstalk_lineup::names::premium_db_names(&["US".to_string()]);
    let name = |index: usize, decorations: &[&str]| {
        let plain = &base[index % base.len()];
        decorations[(index / base.len()) % decorations.len()].replace("{name}", plain)
    };
    let sources: Vec<EpgSource> = (1..=options.sources as i64)
        .map(|id| EpgSource {
            id,
            name: format!("Source {id}"),
            is_active: true,
            priority: 5,
            source_type: "xmltv".to_string(),
            ..EpgSource::default()
        })
        .collect();
    let entries: Vec<EpgEntry> = (0..options.entries)
        .map(|index| {
            let id = index as i64 + 1;
            EpgEntry {
                id,
                name: name(index, &ENTRY_DECORATIONS),
                tvg_id: tvg_id(id),
                epg_source: Some(options.source_of(id)),
                icon_url: None,
            }
        })
        .collect();
    let groups: Vec<ChannelGroup> = (0..50)
        .map(|index| ChannelGroup {
            id: 1000 + index,
            name: format!("Provider {index:02}"),
        })
        .collect();
    let channels: Vec<Channel> = (0..options.channels)
        .map(|index| {
            let id = index as i64 + 1;
            Channel {
                id,
                name: name(index, &CHANNEL_DECORATIONS),
                channel_number: Some(serde_json::json!(id)),
                channel_group_id: Some(groups[index % groups.len()].id),
                epg_data_id: (index % 4 != 3).then_some((index % options.entries) as i64 + 1),
                ..Channel::default()
            }
        })
        .collect();
    World {
        channels,
        groups,
        sources,
        entries,
        ..World::default()
    }
}

/// A scratch directory under the system temp dir, removed when dropped.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "crosstalk-bench-guide-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path)
            .with_context(|| format!("creating the bench scratch dir {}", path.display()))?;
        Ok(Self(path))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Which plan a phase runs.
#[derive(Debug, Clone, Copy)]
enum Plan {
    Match,
    Scan,
    Heal,
}

impl Plan {
    fn action(self) -> &'static str {
        match self {
            Plan::Match => crosstalk_guide::ACTION_MATCH_PREVIEW,
            Plan::Scan => crosstalk_guide::ACTION_SCAN,
            Plan::Heal => crosstalk_guide::ACTION_HEAL_PREVIEW,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Plan::Match => "match",
            Plan::Scan => "scan",
            Plan::Heal => "heal",
        }
    }
}

/// Time the guide plans over a synthetic world against the fake Dispatcharr.
pub async fn synthetic(
    config: &CrosstalkConfig,
    settings: &GuideSettings,
    options: &GuideBenchOptions,
) -> Result<BenchReport> {
    let options = options.checked()?;
    let started = Instant::now();
    let world = synthetic_world(&options);
    let build = started.elapsed();

    let fake = Arc::new(Fake {
        options,
        counts: Counts::default(),
    });
    let app = Router::new()
        .route("/api/epg/programs/search/", get(search))
        .route("/api/epg/current-programs/", post(current_programs))
        .with_state(Arc::clone(&fake));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("binding the fake Dispatcharr")?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let scratch = Scratch::new()?;
    let store = Store::open(scratch.0.join("bench.db")).context("opening the bench store")?;
    let mut config = config.clone();
    config.dispatcharr.base_url = format!("http://{address}");
    config.dispatcharr.api_key = "bench".to_string();
    config.jobs.exports_dir = scratch.0.join("exports").display().to_string();
    config.jobs.data_dir = scratch.0.join("data").display().to_string();
    // Every channel of the synthetic world: an operator's scope would time a different
    // world than the one described.
    config.scope = ScopeConfig::default();
    let config = Arc::new(config);

    let scoped = crosstalk_core::scope::scope_channels(&world, &config.scope)?;
    let scoped_entries = crosstalk_guide::scope::scope_entries(&world, "");

    let mut phases = Vec::new();
    for plan in [Plan::Match, Plan::Scan, Plan::Heal] {
        fake.counts.take();
        let run_id = store.start_run(crosstalk_guide::APP_ID, plan.action(), true)?;
        // A fresh client per plan: its programme memo starts empty, as a run's does.
        let context = JobContext::new(Arc::clone(&config), Arc::new(NullSink))
            .with_dispatcharr(DispatcharrClient::new(&config.dispatcharr)?)
            .for_run(
                crosstalk_guide::APP_ID,
                plan.action(),
                run_id,
                store.clone(),
                Arc::new(AtomicBool::new(false)),
            );
        let guide = crosstalk_guide::GuideApp::new();
        let started = Instant::now();
        let outcome: Result<String> = match plan {
            Plan::Match => crosstalk_guide::matching::run(
                &guide,
                &context,
                settings,
                None,
                &world,
                &scoped,
                &scoped_entries,
                true,
            )
            .await
            .map(|result| {
                format!(
                    "{} channel(s) against {} entr(y/ies), {} matched",
                    thousands(result.channels),
                    thousands(result.candidates),
                    thousands(
                        result
                            .rows
                            .iter()
                            .filter(|row| row.epg_data_id.is_some())
                            .count()
                    )
                )
            }),
            Plan::Scan => crosstalk_guide::scan::run(&guide, &context, settings, &world, &scoped)
                .await
                .map(|result| {
                    format!(
                        "{} channel(s) with a guide, {} missing programmes, {} unchecked",
                        thousands(result.checked),
                        thousands(result.rows.len()),
                        thousands(result.unchecked)
                    )
                }),
            Plan::Heal => crosstalk_guide::heal::run(
                &guide,
                &context,
                settings,
                &world,
                &scoped,
                &scoped_entries,
                true,
            )
            .await
            .map(|result| {
                format!(
                    "{} channel(s) scanned, {} broken, {} replacement(s) found",
                    thousands(result.scanned),
                    thousands(result.broken),
                    thousands(result.replacements)
                )
            }),
        };
        let elapsed = started.elapsed();
        let (searches, probes, batches) = fake.counts.take();
        let _ = store.finish_run(
            run_id,
            if outcome.is_ok() {
                RunStatus::Succeeded
            } else {
                RunStatus::Failed
            },
            Some("recorded by the bench"),
            &serde_json::json!({}),
            None,
        );
        phases.push(match outcome {
            Ok(detail) => BenchPhase {
                action: plan.label().to_string(),
                setup_time: None,
                plan_time: Some(elapsed),
                write_plan_time: None,
                error: None,
                detail: format!(
                    "{detail}; reads: {} source probe(s), {} airing-now batch(es), {} \
                     per-entry search(es)",
                    thousands(probes),
                    thousands(batches),
                    thousands(searches)
                ),
                writes: 0,
                requests: probes + batches + searches,
            },
            Err(error) => BenchPhase {
                action: plan.label().to_string(),
                setup_time: None,
                plan_time: None,
                write_plan_time: None,
                error: Some(format!("{error:#}")),
                detail: String::new(),
                writes: 0,
                requests: probes + batches + searches,
            },
        });
    }
    server.abort();

    let mut notes = Vec::new();
    if cfg!(debug_assertions) {
        notes.push(
            "DEBUG BUILD: these are debug numbers. Run `cargo run --release -p crosstalk -- \
             bench guide --synthetic` for the figures a decision (or the README) should quote."
                .to_string(),
        );
    }
    notes.push(format!(
        "fake Dispatcharr on {address}: {} ms per request, {} concurrent request(s) \
         ([dispatcharr] concurrency); one source in ten has nothing in the window, one entry \
         in seven carries no programmes, most of the rest are airing now",
        options.delay_ms, config.dispatcharr.concurrency
    ));
    notes.push(
        "plan = the whole preview run (engine, index, scoring, programme checks, report), \
         timed end to end; requests = every read the fake answered (writes: none, previews)"
            .to_string(),
    );
    notes.push(format!(
        "settings: check_hours={}, allow_epg_without_programs={}, [matching] cut {}",
        settings.check_hours,
        settings.allow_epg_without_programs,
        config.matching.threshold()
    ));
    Ok(BenchReport {
        title: format!(
            "synthetic guide world: {} channel(s), {} EPG entr(y/ies) in {} source(s)",
            thousands(options.channels),
            thousands(options.entries),
            thousands(options.sources)
        ),
        setup: vec![("build the world".to_string(), build)],
        phases,
        write_timing: None,
        notes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_small_guide_bench_times_every_plan_and_counts_requests() {
        let dir = tempfile::tempdir().expect("dir");
        let config = CrosstalkConfig::load(dir.path()).expect("config");
        let options = GuideBenchOptions {
            channels: 200,
            entries: 600,
            sources: 10,
            delay_ms: 0,
        };
        let report = synthetic(&config, &GuideSettings::default(), &options)
            .await
            .expect("bench");
        for line in report.lines() {
            eprintln!("{line}");
        }
        let actions: Vec<&str> = report.phases.iter().map(|p| p.action.as_str()).collect();
        assert_eq!(actions, ["match", "scan", "heal"]);
        for phase in &report.phases {
            assert!(phase.error.is_none(), "{}: {:?}", phase.action, phase.error);
            assert!(phase.requests > 0, "{} asked nothing", phase.action);
        }
        // The scan asks about 150 assigned entries: one probe per source and a handful of
        // batches settle most of them, so the searches stay well under one per channel.
        let scan = &report.phases[1];
        assert!(scan.requests < 150, "{}", scan.detail);
        assert!(report.lines().iter().any(|line| line.contains("SLA: ")));
    }
}
