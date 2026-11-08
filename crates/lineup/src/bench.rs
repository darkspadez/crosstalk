//! `crosstalk bench lineup --synthetic`: time the lineup plans at the plan's scale.
//!
//! The plan (phase 3, Verification) asks for 20k-channel benches of the `names`,
//! `clean`, `categorize` and `logos` plans and a 500-channel lineup `sync`. This module
//! builds a synthetic world in memory and runs exactly the pure planning stages each
//! action runs — the same `decide_*` functions over the same `RunEngine`, then the same
//! `channel_ops` plan — with no network, no store and no Dispatcharr, so the numbers
//! are the plans' own and nothing else.
//!
//! The report reuses the streams bench's shapes ([`BenchPhase`], [`BenchReport`]): one
//! report format for every app, as the plan asks.

use crate::categorize::{self, CategoryRow};
use crate::clean::{self, CleanRules};
use crate::logos::{self, LogoWorld};
use crate::names::{self, NameRow};
use crate::settings::LineupSettings;
use crate::sync::{self, SyncAction};
use anyhow::{bail, Result};
use crosstalk_core::channel_ops::Selection;
use crosstalk_core::config::MatchingConfig;
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, EpgEntry, Logo, World};
use crosstalk_core::engine_run::RunEngine;
use crosstalk_core::logos::LogoSource;
use crosstalk_matching::identity::ResolveContext;
use crosstalk_streams::bench::{requests_for, thousands, BenchPhase, BenchReport};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// The shape of a synthetic lineup world.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntheticOptions {
    /// Channels to build for the names/clean/categorize/logos plans.
    pub channels: usize,
    /// Lineup entries to sync (the first N of the configured lineup file).
    pub lineup_channels: usize,
}

impl Default for SyntheticOptions {
    fn default() -> Self {
        Self {
            channels: 20_000,
            lineup_channels: 500,
        }
    }
}

impl SyntheticOptions {
    pub fn checked(self) -> Result<Self> {
        if self.channels == 0 || self.lineup_channels == 0 {
            bail!("a synthetic lineup world needs at least one channel and one lineup entry");
        }
        Ok(self)
    }

    pub fn describe(&self) -> String {
        format!(
            "synthetic lineup world: {} channel(s), {} lineup entr(y/ies)",
            thousands(self.channels),
            thousands(self.lineup_channels)
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
    run_engine: RunEngine,
    index: crosstalk_matching::engine::EngineIndex,
    db_names: Vec<String>,
    setup_time: Duration,
}

impl Stack {
    fn build(matching: &MatchingConfig) -> Self {
        let started = Instant::now();
        let run_engine = RunEngine::for_config(matching, None);
        let db_names = names::premium_db_names(&matching.databases());
        let index = run_engine.build_index(&db_names);
        Self {
            run_engine,
            index,
            db_names,
            setup_time: started.elapsed(),
        }
    }
}

fn names_phase(world: &World, settings: &LineupSettings, stack: &Stack) -> BenchPhase {
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
    let rows: Vec<NameRow> = channels
        .par_iter()
        .map(|channel| {
            let identity = stack.run_engine.resolve(&channel.name, &ctx);
            let family = stack
                .run_engine
                .engine()
                .rank_index(&channel.name, &stack.index);
            names::decide_heuristic_with_family(
                channel,
                &identity,
                &family,
                &stack.db_names,
                settings,
            )
            .0
        })
        .collect();
    let plan_time = started.elapsed();
    let started = Instant::now();
    let plan = match names::plan_names(world, &Selection::default(), &rows, settings) {
        Ok(plan) => plan,
        Err(error) => return failed("names", error),
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
    BenchPhase {
        setup_time: Some(stack.setup_time),
        plan_time: Some(plan_time),
        write_plan_time: Some(write_plan_time),
        detail: format!(
            "{} channel(s) resolved and ranked, {} rename(s) ({} OTA, {} premium), {} planned \
             after collisions",
            thousands(rows.len()),
            thousands(renamed),
            thousands(ota),
            thousands(premium),
            thousands(plan.changes.len())
        ),
        writes: plan.changes.len(),
        requests: requests_for(plan.changes.len()),
        ..phase("names")
    }
}

fn categorize_phase(
    world: &World,
    settings: &LineupSettings,
    matching: &MatchingConfig,
    stack: &Stack,
) -> BenchPhase {
    let _ = settings;
    let ctx = ResolveContext::default();
    let started = Instant::now();
    let categories = categorize::category_map(&matching.databases());
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
            let identity = stack.run_engine.resolve(&channel.name, &ctx);
            categorize::decide_category(
                &scoped,
                &identity,
                stack.run_engine.engine(),
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
) -> BenchPhase {
    let order = match LogoSource::parse_order(&settings.logo_sources) {
        Ok(order) => order,
        Err(error) => return failed("logos", error),
    };
    let ctx = ResolveContext::default();
    let started = Instant::now();
    let lookups = LogoWorld::of(world);
    // No tv-logos index: a synthetic bench never fetches, so the tv-logos and network
    // tiers answer nothing and the EPG / Logo Manager / default tiers are what is timed.
    let sources = lookups.sources(order, None, settings);
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
    let rows: Vec<logos::LogoRow> = channels
        .par_iter()
        .map(|channel| {
            let identity = stack.run_engine.resolve(&channel.name, &ctx);
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
            "{} channel(s), {} resolved to a logo URL (no tv-logos index: EPG, Logo Manager \
             and default tiers only), {} assignment(s) planned",
            thousands(rows.len()),
            thousands(assign.len()),
            thousands(plan.changes.len())
        ),
        writes: plan.changes.len(),
        requests: requests_for(plan.changes.len()),
        ..phase("logos")
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
    let build = started.elapsed();
    let stack = Stack::build(matching);

    let mut phases = vec![
        clean_phase(&world, settings),
        names_phase(&world, settings, &stack),
        categorize_phase(&world, settings, matching, &stack),
        logos_phase(&world, settings, matching, &stack),
    ];
    phases.extend(sync_phases(settings, options.lineup_channels));

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
         once per run and shared by names, categorize and logos; plan = the action's own \
         decide stage over {} rayon thread(s), as the action runs it; write plan = the shared \
         channel_ops plan (collision resolver, number check)",
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
                "sync (rerun)"
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
    }

    #[test]
    fn a_zero_sized_world_is_refused() {
        assert!(SyntheticOptions {
            channels: 0,
            lineup_channels: 1
        }
        .checked()
        .is_err());
    }
}
