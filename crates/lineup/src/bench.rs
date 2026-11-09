//! `crosstalk bench lineup --synthetic`: time the lineup plans at the plan's scale.
//!
//! The plan (phase 3, Verification) asks for 20k-channel benches of the `names`,
//! `clean`, `categorize` and `logos` plans and a 500-channel lineup `sync`; the audit
//! (docs/audit/E-perf.md, E-S9) adds `create` from 50k streams, `tidy` and the host
//! `undo` plan. This module builds a synthetic world in memory and runs exactly the
//! planning stages each action runs — the same `decide_*` functions over the same
//! `RunEngine`, then the same `channel_ops` plan — with no network and no Dispatcharr.
//! The identity cache is backed by a FILE store in a scratch directory, as a run's is,
//! so the batch warm pays real commits; the `logos` plan searches a synthetic tv-logos
//! tree (1,500 files per country) so its fuzzy tier is timed too.
//!
//! The report reuses the streams bench's shapes ([`BenchPhase`], [`BenchReport`]): one
//! report format for every app, as the plan asks.

use crate::categorize::{self, CategoryRow};
use crate::clean::{self, CleanRules};
use crate::logos::{self, LogoWorld};
use crate::names::{self, NameRow};
use crate::settings::LineupSettings;
use crate::sync::{self, SyncAction};
use anyhow::{bail, Context, Result};
use crosstalk_core::channel_ops::{self, Field, Op, PlanOptions, Selection, SetValue};
use crosstalk_core::config::{MatchingConfig, ScopeConfig};
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, EpgEntry, Logo, World};
use crosstalk_core::engine_run::RunEngine;
use crosstalk_core::logos::{LogoSource, TvLogosIndex};
use crosstalk_core::store::{RunStatus, Store};
use crosstalk_matching::engine::UnifiedEngine;
use crosstalk_matching::identity::ResolveContext;
use crosstalk_streams::bench::{requests_for, thousands, BenchPhase, BenchReport};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The shape of a synthetic lineup world.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntheticOptions {
    /// Channels to build for the names/clean/categorize/logos plans.
    pub channels: usize,
    /// Lineup entries to sync (the first N of the configured lineup file).
    pub lineup_channels: usize,
    /// Streams the `create` plan imports from.
    pub streams: usize,
    /// Synthetic tv-logos files per country directory.
    pub tv_logos_per_country: usize,
}

impl Default for SyntheticOptions {
    fn default() -> Self {
        Self {
            channels: 20_000,
            lineup_channels: 500,
            streams: 50_000,
            tv_logos_per_country: 1_500,
        }
    }
}

impl SyntheticOptions {
    pub fn checked(self) -> Result<Self> {
        if self.channels == 0 || self.lineup_channels == 0 || self.streams == 0 {
            bail!(
                "a synthetic lineup world needs at least one channel, one lineup entry and one \
                 stream"
            );
        }
        Ok(self)
    }

    pub fn describe(&self) -> String {
        format!(
            "synthetic lineup world: {} channel(s), {} lineup entr(y/ies), {} stream(s) to \
             create from, {} tv-logos file(s) per country",
            thousands(self.channels),
            thousands(self.lineup_channels),
            thousands(self.streams),
            thousands(self.tv_logos_per_country)
        )
    }
}

/// The provider spellings a channel list carries, cycled over the merged names so the
/// clean/names plans see the shapes they exist for (prefixes, quality words, brackets,
/// pipe junk, plain names).
const DECORATIONS: [&str; 8] = [
    "{name}",
    "US: {name} HD",
    "{name} [FHD]",
    "{name} | HD |",
    "USA | {name} FHD",
    "{name} (East)",
    "HD {name}",
    "US | {name} | 1080p",
];

/// Build a world of `options.channels` channels named from the merged US channel
/// database (cycled, decorated) plus the station table (OTA names), in 100 groups,
/// with a Logo Manager that knows every tenth name and no guide assignments.
///
/// Deterministic: the same options build the same world.
pub fn synthetic_world(options: &SyntheticOptions) -> World {
    let base_names: Vec<String> = names::premium_db_names(&["US".to_string()]);
    let stations: Vec<String> = {
        let mut keys: Vec<&String> = crosstalk_matching::data::merged::merged_stations()
            .stations
            .keys()
            .collect();
        keys.sort();
        keys.into_iter()
            .take(options.channels / 10)
            .map(|callsign| format!("{callsign} HD"))
            .collect()
    };
    let groups: Vec<ChannelGroup> = (0..100)
        .map(|index| ChannelGroup {
            id: 1000 + index as i64,
            name: format!("Provider {index:02}"),
        })
        .collect();
    let mut channels = Vec::with_capacity(options.channels);
    let mut logos = Vec::new();
    for index in 0..options.channels {
        let name = if index % 10 == 9 && !stations.is_empty() {
            stations[(index / 10) % stations.len()].clone()
        } else {
            let base = &base_names[index % base_names.len()];
            DECORATIONS[index % DECORATIONS.len()].replace("{name}", base)
        };
        let id = index as i64 + 1;
        let logo_id = (index % 10 == 0).then(|| {
            logos.push(Logo {
                id: 5000 + id,
                name: name.clone(),
                url: format!("https://logos.example/{id}.png"),
                ..Logo::default()
            });
            5000 + id
        });
        channels.push(Channel {
            id,
            name,
            channel_number: Some(serde_json::json!(id)),
            channel_group_id: Some(groups[index % groups.len()].id),
            logo_id,
            ..Channel::default()
        });
    }
    World {
        channels,
        groups,
        logos,
        entries: vec![EpgEntry::default()],
        ..World::default()
    }
}

fn phase(action: &str) -> BenchPhase {
    BenchPhase {
        action: action.to_string(),
        setup_time: None,
        plan_time: None,
        write_plan_time: None,
        error: None,
        detail: String::new(),
        writes: 0,
        requests: 0,
    }
}

fn failed(action: &str, error: impl std::fmt::Display) -> BenchPhase {
    BenchPhase {
        error: Some(format!("{error:#}")),
        ..phase(action)
    }
}

fn clean_phase(world: &World, settings: &LineupSettings) -> BenchPhase {
    let started = Instant::now();
    let rules = CleanRules::from_settings(settings);
    let custom = clean::parse_custom_rules(&settings.custom_rules);
    let protect_re = clean::compile_protect_regex(&settings.protect_regex);
    let channels: Vec<clean::ScopedChannel> = world
        .channels
        .iter()
        .map(|channel| clean::ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            group: world.group_name(channel.channel_group_id),
        })
        .collect();
    let rows = clean::compute_rows(&channels, settings, &rules, &custom, protect_re.as_ref());
    let plan_time = started.elapsed();
    let started = Instant::now();
    let plan = match clean::plan_clean(world, &Selection::default(), &rows, settings) {
        Ok(plan) => plan,
        Err(error) => return failed("clean", error),
    };
    let write_plan_time = started.elapsed();
    let changed = rows
        .iter()
        .filter(|row| row.status == clean::CleanStatus::Change)
        .count();
    BenchPhase {
        plan_time: Some(plan_time),
        write_plan_time: Some(write_plan_time),
        detail: format!(
            "{} channel(s), {} cleaned, {} planned rename(s) after collisions",
            thousands(rows.len()),
            thousands(changed),
            thousands(plan.changes.len())
        ),
        writes: plan.changes.len(),
        requests: requests_for(plan.changes.len()),
        ..phase("clean")
    }
}

/// The engine, identity resolver and merged-DB index every matching plan shares; built
/// once and timed as setup, as a run builds them once.
struct Stack {
    engine: Arc<UnifiedEngine>,
    store: Store,
    index: crosstalk_matching::engine::EngineIndex,
    db_names: Vec<String>,
    setup_time: Duration,
}

impl Stack {
    fn build(matching: &MatchingConfig, store: Store) -> Self {
        let started = Instant::now();
        let run_engine = RunEngine::for_config(matching, Some(store.clone()));
        let db_names = names::premium_db_names(&matching.databases());
        let index = run_engine.build_index(&db_names);
        Self {
            engine: Arc::clone(run_engine.engine()),
            store,
            index,
            db_names,
            setup_time: started.elapsed(),
        }
    }

    /// The resolver one action run starts with: an empty memo over the shared store, as
    /// earlier runs left it (the first action pays the cold cache, the next read rows).
    fn run_engine(&self) -> RunEngine {
        RunEngine::with_engine(Arc::clone(&self.engine), Some(self.store.clone()))
    }
}

/// Every name once, no hint: what an action warms before its rayon stage (E-B3).
fn warm_list<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<(String, ResolveContext)> {
    names
        .into_iter()
        .map(|name| (name.to_string(), ResolveContext::default()))
        .collect()
}

/// A scratch directory under the system temp dir, removed when dropped: the bench's file
/// store lives here, so the identity cache pays the commits a run pays (E-S9).
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "crosstalk-bench-lineup-{}-{nanos}",
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

/// The names plan, plus the plan itself so the undo phase can record it as an apply.
fn names_phase(
    world: &World,
    settings: &LineupSettings,
    stack: &Stack,
) -> (BenchPhase, Option<channel_ops::Plan>) {
    let run_engine = stack.run_engine();
    let ctx = ResolveContext::default();
    let started = Instant::now();
    let channels: Vec<names::ScopedChannel> = world
        .channels
        .iter()
        .map(|channel| names::ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            number: channel.display_number(),
            group: world.group_name(channel.channel_group_id),
        })
        .collect();
    // As the action runs it (E-B3): one batch warm, then the rayon stage on memo hits.
    run_engine.warm(&warm_list(
        channels.iter().map(|channel| channel.name.as_str()),
    ));
    let decided: Vec<(NameRow, Vec<crosstalk_matching::engine::EngineRanked>)> = channels
        .par_iter()
        .map(|channel| {
            let identity = run_engine.resolve(&channel.name, &ctx);
            let family = run_engine.engine().rank_index(&channel.name, &stack.index);
            let row = names::decide_heuristic_with_family(
                channel,
                &identity,
                &family,
                &stack.db_names,
                settings,
            )
            .0;
            (row, family)
        })
        .collect();
    // The family identities the run persists for the other apps (awaited before a run
    // returns, so counted here).
    let mut members: Vec<&str> = Vec::new();
    for (_, family) in &decided {
        for member in family {
            if let Some(name) = stack.db_names.get(member.index) {
                members.push(name.as_str());
            }
        }
    }
    run_engine.warm(&warm_list(members));
    let rows: Vec<NameRow> = decided.into_iter().map(|(row, _)| row).collect();
    let plan_time = started.elapsed();
    let started = Instant::now();
    let plan = match names::plan_names(world, &Selection::default(), &rows, settings) {
        Ok(plan) => plan,
        Err(error) => return (failed("names", error), None),
    };
    let write_plan_time = started.elapsed();
    let mut ota = 0usize;
    let mut premium = 0usize;
    let mut renamed = 0usize;
    for row in &rows {
        if row.status == "Renamed" {
            renamed += 1;
            match row.matcher.as_str() {
                "Broadcast (OTA)" => ota += 1,
                "Premium/Cable" => premium += 1,
                _ => {}
            }
        }
    }
    let phase = BenchPhase {
        setup_time: Some(stack.setup_time),
        plan_time: Some(plan_time),
        write_plan_time: Some(write_plan_time),
        detail: format!(
            "{} channel(s) warmed, resolved and ranked on a cold file store, {} rename(s) ({} \
             OTA, {} premium), {} planned after collisions",
            thousands(rows.len()),
            thousands(renamed),
            thousands(ota),
            thousands(premium),
            thousands(plan.changes.len())
        ),
        writes: plan.changes.len(),
        requests: requests_for(plan.changes.len()),
        ..phase("names")
    };
    (phase, Some(plan))
}

fn categorize_phase(
    world: &World,
    settings: &LineupSettings,
    matching: &MatchingConfig,
    stack: &Stack,
) -> BenchPhase {
    let _ = settings;
    let run_engine = stack.run_engine();
    let ctx = ResolveContext::default();
    let started = Instant::now();
    let categories = categorize::category_map(&matching.databases());
    run_engine.warm(&warm_list(
        world.channels.iter().map(|channel| channel.name.as_str()),
    ));
    let rows: Vec<CategoryRow> = world
        .channels
        .par_iter()
        .map(|channel| {
            let scoped = categorize::ScopedChannel {
                id: channel.id,
                name: channel.name.clone(),
                number: channel.display_number(),
                group: world.group_name(channel.channel_group_id),
            };
            let identity = run_engine.resolve(&channel.name, &ctx);
            categorize::decide_category(
                &scoped,
                &identity,
                run_engine.engine(),
                &stack.index,
                &stack.db_names,
                &categories,
            )
        })
        .collect();
    let moves = categorize::planned_moves(&rows, "");
    let plan_time = started.elapsed();
    // A preview plans only moves into groups that exist; the rest go through the create
    // stage first. Here every category group is given an id, so the move plan covers
    // every decided move (what an apply plans after its creates land).
    let mut group_ids: HashMap<String, i64> = world
        .groups
        .iter()
        .map(|group| (group.name.clone(), group.id))
        .collect();
    let mut planned_world = world.clone();
    let mut next_id = world.groups.iter().map(|group| group.id).max().unwrap_or(0) + 1;
    for row in &moves {
        if let Some(category) = row.category.as_deref() {
            if !group_ids.contains_key(category) {
                group_ids.insert(category.to_string(), next_id);
                planned_world.groups.push(ChannelGroup {
                    id: next_id,
                    name: category.to_string(),
                });
                next_id += 1;
            }
        }
    }
    let started = Instant::now();
    let plan =
        match categorize::plan_moves(&planned_world, &Selection::default(), &moves, &group_ids) {
            Ok(plan) => plan,
            Err(error) => return failed("categorize", error),
        };
    let write_plan_time = started.elapsed();
    let categorized = rows.iter().filter(|row| row.category.is_some()).count();
    BenchPhase {
        setup_time: Some(stack.setup_time),
        plan_time: Some(plan_time),
        write_plan_time: Some(write_plan_time),
        detail: format!(
            "{} channel(s), {} with a category, {} move(s) into {} group(s)",
            thousands(rows.len()),
            thousands(categorized),
            thousands(plan.changes.len()),
            thousands(planned_world.groups.len() - world.groups.len())
        ),
        writes: plan.changes.len(),
        requests: requests_for(plan.changes.len()),
        ..phase("categorize")
    }
}

fn logos_phase(
    world: &World,
    settings: &LineupSettings,
    matching: &MatchingConfig,
    stack: &Stack,
    tv_index: &TvLogosIndex,
) -> BenchPhase {
    let order = match LogoSource::parse_order(&settings.logo_sources) {
        Ok(order) => order,
        Err(error) => return failed("logos", error),
    };
    let run_engine = stack.run_engine();
    let ctx = ResolveContext::default();
    let started = Instant::now();
    let lookups = LogoWorld::of(world);
    // The synthetic tv-logos tree: the fuzzy tier scores every channel the earlier
    // tiers leave against each country directory's files (E-B4).
    let sources = lookups.sources(order, Some(tv_index), settings);
    let enabled = matching.databases();
    let channels: Vec<logos::ScopedChannel> = world
        .channels
        .iter()
        .map(|channel| logos::ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            number: channel.display_number(),
            group: world.group_name(channel.channel_group_id),
            logo_id: channel.logo_id,
            epg_id: channel.current_epg_id(),
        })
        .collect();
    run_engine.warm(&warm_list(
        channels.iter().map(|channel| channel.name.as_str()),
    ));
    let rows: Vec<logos::LogoRow> = channels
        .par_iter()
        .map(|channel| {
            let identity = run_engine.resolve(&channel.name, &ctx);
            let codes = logos::tv_countries(settings, &identity, &enabled);
            logos::decide_logo(channel, &identity, &lookups, &sources, &codes, settings)
        })
        .collect();
    let assign: Vec<&logos::LogoRow> = rows.iter().filter(|row| row.url.is_some()).collect();
    let plan_time = started.elapsed();
    let logo_ids: HashMap<String, i64> = lookups
        .by_url
        .iter()
        .map(|(url, (id, _))| ((*url).to_string(), *id))
        .collect();
    let started = Instant::now();
    let plan = match logos::plan_assignments(world, &Selection::default(), &assign, &logo_ids) {
        Ok(plan) => plan,
        Err(error) => return failed("logos", error),
    };
    let write_plan_time = started.elapsed();
    BenchPhase {
        setup_time: Some(stack.setup_time),
        plan_time: Some(plan_time),
        write_plan_time: Some(write_plan_time),
        detail: format!(
            "{} channel(s), {} resolved to a logo URL (tv-logos tree of {} file(s) in {} \
             dir(s)), {} assignment(s) planned",
            thousands(rows.len()),
            thousands(assign.len()),
            thousands(tv_index.files.values().map(Vec::len).sum::<usize>()),
            thousands(tv_index.files.len()),
            thousands(plan.changes.len())
        ),
        writes: plan.changes.len(),
        requests: requests_for(plan.changes.len()),
        ..phase("logos")
    }
}

/// A synthetic tv-logos tree: `per_country` files in each enabled country's directory,
/// named from that country's channel database the way tv-logos names them
/// (`<slug>-<suffix>.png`), padded with numbered fillers when the database is smaller.
pub fn synthetic_tv_index(matching: &MatchingConfig, per_country: usize) -> TvLogosIndex {
    let slug = |name: &str| {
        let mut out = String::with_capacity(name.len());
        for c in name.to_lowercase().chars() {
            if c.is_alphanumeric() {
                out.push(c);
            } else if !out.ends_with('-') {
                out.push('-');
            }
        }
        out.trim_matches('-').to_string()
    };
    let mut codes = matching.databases();
    if !codes.iter().any(|code| code.eq_ignore_ascii_case("US")) {
        codes.push("US".to_string());
    }
    let mut tree: Vec<serde_json::Value> = Vec::new();
    for code in &codes {
        let Some(dir) = crosstalk_core::logos::country_dir_for(code) else {
            continue;
        };
        let suffix = crosstalk_core::logos::country_suffix_for(code);
        let mut seen: HashSet<String> = HashSet::new();
        let mut files: Vec<String> = names::premium_db_names(std::slice::from_ref(code))
            .iter()
            .map(|name| slug(name))
            .filter(|stem| !stem.is_empty() && seen.insert(stem.clone()))
            .take(per_country)
            .collect();
        let mut filler = 0usize;
        while files.len() < per_country {
            filler += 1;
            files.push(format!("filler-channel-{filler}"));
        }
        for stem in files {
            tree.push(serde_json::json!({
                "type": "blob",
                "path": format!("countries/{dir}/{stem}-{suffix}.png"),
            }));
        }
    }
    TvLogosIndex::from_tree(&serde_json::json!({ "tree": tree, "truncated": false }))
}

/// `create` (streams mode) over the streams bench's synthetic world: every eligible stream
/// a candidate, every live channel's identity for the foreign-family check, one decision
/// per candidate, then the create plan with the collision and global number checks.
fn create_phase(
    settings: &LineupSettings,
    matching: &MatchingConfig,
    stack: &Stack,
    options: &SyntheticOptions,
) -> BenchPhase {
    let world =
        crosstalk_streams::bench::synthetic_world(&crosstalk_streams::bench::SyntheticOptions {
            channels: options.channels,
            streams: options.streams,
            accounts: 100,
        });
    let streams_settings = crosstalk_streams::settings::StreamsSettings::default();
    let run_engine = stack.run_engine();
    let started = Instant::now();
    let pool = match crosstalk_streams::selection::build(&world, &streams_settings) {
        Ok(pool) => pool,
        Err(error) => return failed("create", error),
    };
    let candidates = crate::create::streams_candidates(&world, &pool, &settings.m3u_group_filter);
    let live: Vec<(i64, String)> = world
        .channels
        .iter()
        .map(|channel| (channel.id, channel.name.clone()))
        .collect();
    run_engine.warm(&warm_list(live.iter().map(|(_, name)| name.as_str())));
    let world_names = crate::create::identities_of(&run_engine, &live);
    let categories = categorize::category_map(&matching.databases());
    let engine_stack = crate::create::EngineStack {
        engine: run_engine.engine(),
        index: &stack.index,
        db_names: &stack.db_names,
        categories: &categories,
    };
    let mut rows = crate::create::decide_streams(
        &candidates,
        &engine_stack,
        settings,
        &HashMap::new(),
        &world_names,
    );
    let plan_time = started.elapsed();
    let started = Instant::now();
    let model = crosstalk_streams::quality::QualityModel::from_settings(&streams_settings);
    let throughput = crosstalk_streams::quality::MemoryThroughputSource::new();
    let order = |ids: &[i64]| crate::create::quality_order(&pool, &model, &throughput, ids);
    let request = match crate::create::build_streams_request(&world, &mut rows, &order) {
        Ok(request) => request,
        Err(error) => return failed("create", error),
    };
    let plan_options = match crate::clean::collision_strategy(settings) {
        Ok(collision) => PlanOptions {
            collision: Some(collision),
            collision_sep: settings.join_separator.clone(),
            number_conflicts: true,
            ..PlanOptions::default()
        },
        Err(error) => return failed("create", error),
    };
    let plan = match channel_ops::plan_creates(&world, &HashMap::new(), &request, &plan_options) {
        Ok(plan) => plan,
        Err(error) => return failed("create", error),
    };
    let write_plan_time = started.elapsed();
    let posts = plan.groups.len() + plan.logos.len() + plan.channels.len();
    BenchPhase {
        setup_time: Some(stack.setup_time),
        plan_time: Some(plan_time),
        write_plan_time: Some(write_plan_time),
        detail: format!(
            "{} stream(s), {} candidate(s), {} live channel identit(y/ies); {} channel(s) and \
             {} group(s) to create, {} refused",
            thousands(world.streams.len()),
            thousands(candidates.len()),
            thousands(world_names.len()),
            thousands(plan.channels.len()),
            thousands(plan.groups.len()),
            thousands(plan.skipped.len())
        ),
        writes: posts,
        // One POST per created object (creates are never batched upstream).
        requests: posts,
        ..phase("create")
    }
}

/// `tidy` over the synthetic world: every other channel owned by the lineup, every fourth
/// one carrying a stream, so half the owned set hides; the tidy history is read from the
/// store as a run reads it.
fn tidy_phase(world: &World, store: &Store) -> BenchPhase {
    let mut world = world.clone();
    for channel in &mut world.channels {
        if channel.id % 4 == 0 {
            channel.streams = vec![channel.id];
        }
    }
    let owned: HashSet<i64> = world
        .channels
        .iter()
        .filter(|channel| channel.id % 2 == 0)
        .map(|channel| channel.id)
        .collect();
    let started = Instant::now();
    let hid = match crate::tidy::hid_by_tidy_runs(store) {
        Ok(hid) => hid,
        Err(error) => return failed("tidy", error),
    };
    let rows = crate::tidy::decide_tidy(&world, &owned, &hid);
    let plan_time = started.elapsed();
    let started = Instant::now();
    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for row in &rows {
        match row.action {
            crate::tidy::TidyAction::Hide => {
                values.insert(row.id, SetValue::Flag(true));
            }
            crate::tidy::TidyAction::Unhide => {
                values.insert(row.id, SetValue::Flag(false));
            }
            crate::tidy::TidyAction::Keep => {}
        }
    }
    let mut selection = Selection::in_scope(&ScopeConfig::default());
    selection.channel_ids = Some(values.keys().copied().collect());
    let plan = if values.is_empty() {
        channel_ops::Plan::default()
    } else {
        match channel_ops::plan_with_options(
            &world,
            &selection,
            &[Op::Set {
                field: Field::HiddenFromOutput,
                values,
            }],
            &PlanOptions::default(),
        ) {
            Ok(plan) => plan,
            Err(error) => return failed("tidy", error),
        }
    };
    let write_plan_time = started.elapsed();
    BenchPhase {
        plan_time: Some(plan_time),
        write_plan_time: Some(write_plan_time),
        detail: format!(
            "{} owned channel(s) judged, {} hide/unhide write(s) planned",
            thousands(rows.len()),
            thousands(plan.changes.len())
        ),
        writes: plan.changes.len(),
        requests: requests_for(plan.changes.len()),
        ..phase("tidy")
    }
}

/// Record `rows` as a finished `names_apply` run, as the runner would.
fn record_apply(store: &Store, rows: &[crosstalk_core::store::RunRow]) -> Result<i64> {
    let run_id = store.start_run(crate::APP_ID, crate::ACTION_NAMES_APPLY, false)?;
    store.append_rows(run_id, rows)?;
    store.finish_run(
        run_id,
        RunStatus::Succeeded,
        Some("recorded by the bench"),
        &serde_json::json!({}),
        None,
    )?;
    Ok(run_id)
}

/// The host `undo` plan of a names apply: the names plan recorded as a finished apply in
/// the file store, then planned back against the world that apply leaves.
fn undo_phase(world: &World, names_plan: Option<&channel_ops::Plan>, store: &Store) -> BenchPhase {
    let Some(names_plan) = names_plan else {
        return failed(
            "undo",
            "the names plan failed, so there is no apply to undo",
        );
    };
    let mut rows = channel_ops::run_rows(names_plan, None);
    for row in &mut rows {
        if row.status == "planned" {
            row.status = "ok".to_string();
        }
    }
    let run_id = match record_apply(store, &rows) {
        Ok(run_id) => run_id,
        Err(error) => return failed("undo", error),
    };
    // The world the apply leaves: every recorded rename landed.
    let mut applied = world.clone();
    let position: HashMap<i64, usize> = applied
        .channels
        .iter()
        .enumerate()
        .map(|(position, channel)| (channel.id, position))
        .collect();
    for row in &rows {
        if row.field != "name" {
            continue;
        }
        if let (Some(&position), Some(after)) =
            (position.get(&row.channel_id), row.after_value.as_deref())
        {
            applied.channels[position].name = after.to_string();
        }
    }
    let registry = crosstalk_core::subapp::Registry::new(vec![Arc::new(crate::LineupApp::new())]);
    let started = Instant::now();
    let undo = match crosstalk_core::undo::plan_undo(store, &applied, run_id, &registry) {
        Ok(undo) => undo,
        Err(error) => return failed("undo", error),
    };
    let plan_time = started.elapsed();
    BenchPhase {
        plan_time: Some(plan_time),
        write_plan_time: Some(Duration::ZERO),
        detail: format!(
            "names apply of {} row(s) recorded, {} reversal(s) planned, {} skipped",
            thousands(rows.len()),
            thousands(undo.changes.len()),
            thousands(undo.skipped.len())
        ),
        writes: undo.changes.len(),
        requests: requests_for(undo.changes.len()),
        ..phase("undo")
    }
}

/// `sync` twice: a first run over an empty world (every entry is a create, planned
/// through the create stage with the global number check), then a rerun over the world
/// those creates would leave, owned (the no-op every scheduled sync is).
fn sync_phases(settings: &LineupSettings, entries: usize) -> Vec<BenchPhase> {
    let mut lineup = match crate::lineup_file::load_lineup(
        &settings.lineup_file,
        &settings.category_detail,
        &settings.group_prefix,
    ) {
        Ok(lineup) => lineup,
        Err(error) => return vec![failed("sync", error)],
    };
    lineup.entries.truncate(entries);
    let kept: HashSet<String> = lineup
        .entries
        .iter()
        .map(|entry| entry.category.clone())
        .collect();
    lineup.categories.retain(|category| kept.contains(category));

    // First run: nothing exists yet.
    let empty = World::default();
    let owned: HashMap<String, i64> = HashMap::new();
    let started = Instant::now();
    let mut decisions = match sync::decide_sync(&empty, &lineup, settings, &owned) {
        Ok(decisions) => decisions,
        Err(error) => return vec![failed("sync", error)],
    };
    let plan_time = started.elapsed();
    let started = Instant::now();
    let create_plan = match sync::plan_sync_creates(&empty, &owned, &mut decisions, None) {
        Ok(plan) => plan,
        Err(error) => return vec![failed("sync", error)],
    };
    let write_plan_time = started.elapsed();
    let first = BenchPhase {
        plan_time: Some(plan_time),
        write_plan_time: Some(write_plan_time),
        detail: format!(
            "first sync over an empty world: {} entr(y/ies), {} group(s) and {} channel(s) to \
             create, {} refused",
            thousands(decisions.rows.len()),
            thousands(create_plan.groups.len()),
            thousands(create_plan.channels.len()),
            thousands(create_plan.skipped.len())
        ),
        writes: create_plan.groups.len() + create_plan.channels.len(),
        requests: create_plan.groups.len() + create_plan.channels.len(),
        ..phase("sync (first run)")
    };

    // Rerun: the world those creates leave, owned by this lineup.
    let group_ids: HashMap<String, i64> = decisions
        .groups
        .iter()
        .enumerate()
        .map(|(index, group)| (group.name.clone(), 100 + index as i64))
        .collect();
    let mut world = World {
        groups: group_ids
            .iter()
            .map(|(name, id)| ChannelGroup {
                id: *id,
                name: name.clone(),
            })
            .collect(),
        ..World::default()
    };
    let mut owned: HashMap<String, i64> = HashMap::new();
    for (index, row) in decisions
        .rows
        .iter()
        .filter(|row| row.action == SyncAction::Create)
        .enumerate()
    {
        let id = 10_000 + index as i64;
        world.channels.push(Channel {
            id,
            name: row.name.clone(),
            channel_number: row.number.map(|number| serde_json::json!(number)),
            channel_group_id: group_ids.get(&row.group).copied(),
            ..Channel::default()
        });
        owned.insert(sync::channel_owner_key(&lineup.file_name, &row.name), id);
    }
    let started = Instant::now();
    let mut decisions = match sync::decide_sync(&world, &lineup, settings, &owned) {
        Ok(decisions) => decisions,
        Err(error) => return vec![first, failed("sync (rerun)", error)],
    };
    let plan_time = started.elapsed();
    let started = Instant::now();
    let create_plan = match sync::plan_sync_creates(&world, &owned, &mut decisions, None) {
        Ok(plan) => plan,
        Err(error) => return vec![first, failed("sync (rerun)", error)],
    };
    let (numbers, moves, _refused) = sync::edit_values(&decisions, &group_ids);
    let edits = numbers.len() + moves.len();
    let write_plan_time = started.elapsed();
    let kept = decisions
        .rows
        .iter()
        .filter(|row| row.action == SyncAction::Keep)
        .count();
    let rerun = BenchPhase {
        plan_time: Some(plan_time),
        write_plan_time: Some(write_plan_time),
        detail: format!(
            "rerun over the synced world: {} owned channel(s) kept, {} create(s), {} edit(s)",
            thousands(kept),
            thousands(create_plan.channels.len()),
            thousands(edits)
        ),
        writes: create_plan.channels.len() + edits,
        requests: create_plan.channels.len() + requests_for(edits),
        ..phase("sync (rerun)")
    };
    vec![first, rerun]
}

/// Time every lineup plan over a synthetic world. No network, no store, no Dispatcharr.
pub fn synthetic(
    settings: &LineupSettings,
    matching: &MatchingConfig,
    options: &SyntheticOptions,
) -> Result<BenchReport> {
    let options = options.checked()?;
    let started = Instant::now();
    let world = synthetic_world(&options);
    let tv_index = synthetic_tv_index(matching, options.tv_logos_per_country);
    let build = started.elapsed();
    // A file store, as a run's: the identity warm and the history reads pay real I/O.
    let scratch = Scratch::new()?;
    let store = Store::open(scratch.0.join("bench.db")).context("opening the bench store")?;
    let stack = Stack::build(matching, store.clone());

    let (names, names_plan) = names_phase(&world, settings, &stack);
    let mut phases = vec![
        clean_phase(&world, settings),
        names,
        categorize_phase(&world, settings, matching, &stack),
        logos_phase(&world, settings, matching, &stack, &tv_index),
    ];
    phases.extend(sync_phases(settings, options.lineup_channels));
    phases.push(create_phase(settings, matching, &stack, &options));
    phases.push(tidy_phase(&world, &store));
    phases.push(undo_phase(&world, names_plan.as_ref(), &store));

    let mut notes = Vec::new();
    if cfg!(debug_assertions) {
        notes.push(
            "DEBUG BUILD: these are debug numbers, 10-50x slower than a release build on this \
             code. Run `cargo run --release -p crosstalk -- bench lineup --synthetic` for the \
             figures a decision (or the README) should quote."
                .to_string(),
        );
    }
    notes.push(format!(
        "settings: lineup_file={}, category_detail={}, channel_numbering={}, \
         tag_unmatched_channels={}, logo_sources={}, [matching] cut {} over {}",
        settings.lineup_file,
        settings.category_detail,
        settings.channel_numbering,
        settings.tag_unmatched_channels,
        settings.logo_sources,
        matching.threshold(),
        matching.databases().join(",")
    ));
    notes.push(format!(
        "setup = the unified engine, the identity resolver and the merged-DB index, built \
         once per run and shared by names, categorize, logos and create; plan = the action's \
         own decide stage over {} rayon thread(s), as the action runs it (batch identity \
         warm on a file store included); write plan = the shared channel_ops plan \
         (collision resolver, number check)",
        rayon::current_num_threads()
    ));
    notes.push(
        "scope: every channel of the synthetic world; the world is built, not loaded, so no \
         Dispatcharr read is timed here"
            .to_string(),
    );
    Ok(BenchReport {
        title: options.describe(),
        setup: vec![
            ("build the world".to_string(), build),
            ("engine + merged index".to_string(), stack.setup_time),
        ],
        phases,
        write_timing: None,
        notes,
    })
}

/// A report line per phase, for the CLI and the tests.
pub fn print(report: &BenchReport) {
    report.print();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small() -> SyntheticOptions {
        SyntheticOptions {
            channels: 120,
            lineup_channels: 40,
            streams: 300,
            tv_logos_per_country: 50,
        }
    }

    #[test]
    fn a_synthetic_world_has_the_shape_the_options_ask_for() {
        let world = synthetic_world(&small());
        assert_eq!(world.channels.len(), 120);
        assert_eq!(world.groups.len(), 100);
        assert_eq!(world.logos.len(), 12);
        // Station names are in the mix (every tenth channel), decorated.
        assert!(world
            .channels
            .iter()
            .any(|channel| channel.name.ends_with(" HD") && channel.name.len() <= 12));
        // Deterministic.
        let again = synthetic_world(&small());
        assert_eq!(
            world.channels.iter().map(|c| &c.name).collect::<Vec<_>>(),
            again.channels.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_synthetic_bench_times_every_phase() {
        let settings = LineupSettings::default();
        let matching = MatchingConfig::default();
        let report = synthetic(&settings, &matching, &small()).expect("bench");
        for line in report.lines() {
            eprintln!("{line}");
        }
        let actions: Vec<&str> = report.phases.iter().map(|p| p.action.as_str()).collect();
        assert_eq!(
            actions,
            [
                "clean",
                "names",
                "categorize",
                "logos",
                "sync (first run)",
                "sync (rerun)",
                "create",
                "tidy",
                "undo"
            ]
        );
        for phase in &report.phases {
            assert!(phase.error.is_none(), "{}: {:?}", phase.action, phase.error);
            assert!(phase.plan_time.is_some(), "{} timed", phase.action);
            assert!(
                phase.write_plan_time.is_some(),
                "{} write plan timed",
                phase.action
            );
        }
        // Decorated names clean and rename; the first sync creates every entry and the
        // rerun writes nothing.
        assert!(report.phases[0].writes > 0, "{}", report.phases[0].detail);
        assert!(report.phases[1].writes > 0, "{}", report.phases[1].detail);
        assert!(report.phases[4].writes >= 40, "{}", report.phases[4].detail);
        assert_eq!(report.phases[5].writes, 0, "{}", report.phases[5].detail);
        assert!(report
            .lines()
            .iter()
            .any(|line| line.contains("synthetic lineup world")));
        // Tidy hides the owned stream-less half; the undo plans the names apply back.
        assert!(report.phases[7].writes > 0, "{}", report.phases[7].detail);
        assert!(report.phases[8].writes > 0, "{}", report.phases[8].detail);
        // Every bench ends with the SLA verdict, and the JSON carries it.
        assert!(report.lines().iter().any(|line| line.contains("SLA: ")));
        let json = report.to_json();
        assert_eq!(json["phases"].as_array().map(Vec::len), Some(9));
        assert!(json["sla"]["passed"].is_boolean());
    }

    #[test]
    fn the_synthetic_tv_logos_tree_has_the_asked_size() {
        let index = synthetic_tv_index(&MatchingConfig::default(), 50);
        assert!(!index.files.is_empty());
        for files in index.files.values() {
            assert_eq!(files.len(), 50);
        }
    }

    #[test]
    fn a_zero_sized_world_is_refused() {
        assert!(SyntheticOptions {
            channels: 0,
            ..SyntheticOptions::default()
        }
        .checked()
        .is_err());
    }
}
