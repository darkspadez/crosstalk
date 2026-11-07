//! `categorize_*`: Channel-Mapparr's organize-by-category on the unified engine.
//!
//! Port of `reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/plugin.py`
//! `category_groups_dry_run_action` (~2887) and `organize_by_category_action`
//! (~3134): the category preview columns, the premium category map, the
//! normalized-exact then fuzzy classification, the get-or-create groups and the
//! bulk move of channels whose group differs.
//!
//! # Target-category algorithm per path (with CM citations)
//!
//! Every in-scope channel resolves through the same [`RunEngine`] + identity
//! cache guide and streams run on, then ranks against the merged premium channel
//! DB (the enabled `[matching]` countries, newest union) at the one host cut:
//!
//! - **Premium** (`plugin.py:3230-3260`): CM maps `channel_name.lower()` to its
//!   DB category, tries the normalized name first (`Premium (Exact)`), then the
//!   fuzzy winner (`Premium (Fuzzy - score: N)`). The unified side ranks with
//!   the one ranker; the identity family's first member (rank order) the map
//!   holds supplies the category. An entry with no category is no category
//!   (CM's map only holds entries that have one), and so is a COUNTRY label
//!   ([`is_country_category`], a fix over CM: the merged DBs mark station and
//!   regional-feed entries with their country, and CM would move those
//!   channels into a group named `United States`). CM's scorer is replaced by the unified ranker — the Exact/Fuzzy
//!   split is kept for the report, with Exact meaning CM's: the channel's
//!   normalize key IS the leader's (`norm_lower in category_map_premium`), at
//!   100, whichever engine stage (alias, exact) scored it.
//! - **OTA**: no category. CM's organize path has deliberately NO callsign
//!   branch (`plugin.py:3165-3177`: the FCC station table carries no category
//!   field, and populating it would move every over-the-air channel into one
//!   group), and its name-only premium path never reaches a station entry from
//!   an OTA-shaped name (`CBS - AL Birmingham (WIAT)` is `No Match` in CM). The
//!   unified ranker DOES reach the station entry (C07 callsign-first), and those
//!   entries carry the country as their category (`WIAT (CBS)` → `United
//!   States`), so a channel whose identity callsign is a merged station — the
//!   same OTA test `names` uses ([`crate::names::is_station_callsign`]) — is
//!   never classified: it stays where it is (strictly safer than CM where CM
//!   would classify a network-led name by its bare network, `ABC (WABC)` →
//!   `Broadcast`; the oracle's `ota` bucket). No `Broadcast` group is ever
//!   created here (that string only exists in CM's stream-import path,
//!   `OTA_IMPORT_CATEGORY`).
//!
//! # Scope
//!
//! The run plans over the host `[scope]` (or the step override, `scope =
//! "lineup"` included), restricted further by `category_groups` when set — CM's
//! category scope (`_resolve_group_scope` with `include_key =
//! "category_groups"`): exact, case-sensitive names, no wildcards, and a token
//! naming no group fails the run. The target side mirrors CM too: a category
//! the operator ignored (`[scope].ignore_groups`, case-insensitive wildcards
//! like CM's `is_ignored_name_tokens`) is never created or moved into, and the
//! run reports it instead.
//!
//! # Planning, CSV, undo
//!
//! `categorize_preview` (Read) / `categorize_apply` (Write, confirm) plan from
//! LIVE state: missing groups go through the create stage ([`NewGroup`], deduped
//! by exact name against the world, owned by lineup under
//! [`category_owner_key`]), moves go through [`Op::Set`] `Group` per channel,
//! only real changes (a channel already in its category group is a skip, like
//! CM's `new_group_name != current_group_name`). The CSV carries CM's preview
//! columns (`plugin.py:1388`) plus the identity fields names carries. Apply
//! creates groups first, then re-plans the moves against the world augmented
//! with the created groups (same ids, same names — the augmentation only fills
//! in what this run just created) and writes through [`channel_ops::apply`].
//! Group creation is recorded with the moves, so the host
//! [`undo`](crosstalk_core::undo) reverses the moves; created groups are left
//! and listed (the plan decision for every create stage).
//!
//! Collision checks are n/a: group names are categories, never renamed, and a
//! group whose exact name exists is reused rather than created.

use anyhow::{bail, Result};
use crosstalk_core::channel_ops::{
    self, ChannelCreator, CreateRequest, Field, NewGroup, Op, PlanOptions, RestChannelCreator,
    Selection, SetValue,
};
use crosstalk_core::csv_report::{self, Report};
use crosstalk_core::dispatcharr::{ChannelGroup, World, WorldKind, WorldKinds};
use crosstalk_core::engine_run::RunEngine;
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use crosstalk_core::wildcard::{expand_patterns, parse_tokens};
use crosstalk_matching::engine::{EngineRanked, UnifiedEngine};
use crosstalk_matching::identity::{Identity, ResolveContext};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The CSV columns: CM's preview columns (`plugin.py:1388`) first, then whether the
/// group exists, the row status and reason, then the identity fields names carries.
pub const COLUMNS: [&str; 15] = [
    "id",
    "channel_name",
    "current_group",
    "new_group",
    "category",
    "match_type",
    "match_value",
    "group_exists",
    "status",
    "reason",
    "callsign",
    "network",
    "country",
    "zone",
    "canonical",
];

/// The ownership key of a category group lineup creates: `lineup:category:<name>`.
pub fn category_owner_key(category: &str) -> String {
    format!("lineup:category:{category}")
}

/// One in-scope channel, as categorize sees it.
#[derive(Debug, Clone)]
pub struct ScopedChannel {
    pub id: i64,
    pub name: String,
    pub number: Option<f64>,
    pub group: String,
}

/// One channel's classification: the DB category (if any) and how it was found.
#[derive(Debug, Clone)]
pub struct CategoryRow {
    pub id: i64,
    pub group: String,
    pub number: Option<f64>,
    pub old: String,
    /// The DB category, or `None` when the family leader carries none (or there
    /// is no family at the cut).
    pub category: Option<String>,
    /// The family leader's DB name, when there is one.
    pub match_name: Option<String>,
    /// The leader's score (0–100).
    pub score: Option<u8>,
    /// CM's vocabulary: `Premium (Exact)` or `Premium (Fuzzy - score: N)`.
    pub match_type: String,
    pub reason: String,
    pub identity: Identity,
}

/// One channel's final outcome: the move (or the skip), post-plan.
#[derive(Debug, Clone)]
pub struct CategoryOutput {
    pub id: i64,
    pub number: Option<f64>,
    pub old: String,
    pub current_group: String,
    pub new_group: String,
    pub category: String,
    pub match_type: String,
    pub match_value: String,
    pub group_exists: String,
    /// `move`, `planned`, `failed` or `skipped`.
    pub status: String,
    pub reason: String,
    pub callsign: Option<String>,
    pub network: Option<String>,
    pub country: Option<String>,
    pub zone: String,
    pub canonical: String,
}

/// The merged premium DB as a category map: lowercase name → (DB name, category).
///
/// CM builds the map per run (`category_map_premium`), last country wins on a
/// repeated name; entries without a category never enter it, and neither do
/// entries whose category is a country label ([`is_country_category`]), so a
/// regional duplicate (`HUNGRY` -> `United States`) never shadows the real entry
/// (`Hungry` -> `Lifestyle`) whatever the DB order. The engine index
/// still ranks over every DB name ([`crate::names::premium_db_names`]) — like
/// CM, whose fuzzy match runs over `premium_channels` whole and only then looks
/// the winner up in the map.
pub fn category_map(databases: &[String]) -> HashMap<String, (String, String)> {
    let mut map: HashMap<String, (String, String)> = HashMap::new();
    for code in databases {
        let Some(raw) = crosstalk_matching::data::merged::merged_database_json(code) else {
            continue;
        };
        let parsed: serde_json::Value = serde_json::from_str(raw).unwrap_or_default();
        let channels = parsed
            .get("channels")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        for channel in channels {
            let name = channel
                .get("channel_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            let category = channel
                .get("category")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            if name.is_empty() || category.is_empty() || is_country_category(&category) {
                continue;
            }
            map.insert(name.to_lowercase(), (name, category));
        }
    }
    map
}

/// Whether a DB category is a country label rather than a category (a fix over CM).
///
/// The merged DBs mark station and regional-feed entries with their COUNTRY as the
/// category (30,654 of the US DB's entries say `United States`; `United Kingdom`,
/// `Canada`, `Brazil`, ... likewise). CM moves such channels into a group named after
/// the country; crosstalk treats every merged DB's `country_name` and `country_code`
/// (case-insensitive) as no category, so categorize never creates or fills a
/// country-named group. Strictly safer: it only ever moves fewer channels.
pub fn is_country_category(category: &str) -> bool {
    static LABELS: std::sync::OnceLock<HashSet<String>> = std::sync::OnceLock::new();
    let labels = LABELS.get_or_init(|| {
        let mut labels = HashSet::new();
        for code in crosstalk_matching::data::merged::merged_codes() {
            labels.insert(code.to_lowercase());
            let Some(raw) = crosstalk_matching::data::merged::merged_database_json(code) else {
                continue;
            };
            let parsed: serde_json::Value = serde_json::from_str(raw).unwrap_or_default();
            for key in ["country_name", "country_code"] {
                if let Some(value) = parsed.get(key).and_then(|v| v.as_str()) {
                    labels.insert(value.trim().to_lowercase());
                }
            }
        }
        labels
    });
    labels.contains(&category.trim().to_lowercase())
}

/// CM's label: `Premium (Exact)` when the channel's normalize key is the leader's
/// (CM's `norm_lower in category_map_premium`) at 100, whichever engine stage scored
/// it (an alias hit on the same key is still CM's exact), else the fuzzy label.
fn match_type_of(engine: &UnifiedEngine, name: &str, leader: &str, score: u8) -> String {
    if score == 100 && engine.normalize_key(name) == engine.normalize_key(leader) {
        "Premium (Exact)".to_string()
    } else {
        format!("Premium (Fuzzy - score: {score})")
    }
}

/// Classify one channel: the best identity family's leader, looked up in the
/// category map (CM's normalized-exact then fuzzy, `plugin.py:3230-3260`).
///
/// A leader with no category is no classification — CM's map never holds such
/// an entry, so the channel stays where it is rather than moving to an empty
/// group name.
pub fn decide_category(
    channel: &ScopedChannel,
    identity: &Identity,
    engine: &UnifiedEngine,
    index: &crosstalk_matching::engine::EngineIndex,
    db_names: &[String],
    categories: &HashMap<String, (String, String)>,
) -> CategoryRow {
    let base = |category: Option<String>,
                match_name: Option<String>,
                score: Option<u8>,
                match_type: String,
                reason: String| {
        CategoryRow {
            id: channel.id,
            group: channel.group.clone(),
            number: channel.number,
            old: channel.name.clone(),
            category,
            match_name,
            score,
            match_type,
            reason,
            identity: identity.clone(),
        }
    };
    // OTA: one definition with `names` (the identity callsign is a merged station).
    if let Some(callsign) = identity
        .callsign
        .as_deref()
        .filter(|callsign| crate::names::is_station_callsign(callsign))
    {
        return base(
            None,
            None,
            None,
            "OTA".to_string(),
            format!(
                "OTA station {callsign}: CM classifies by name only (no callsign branch), \
                 so the channel stays where it is"
            ),
        );
    }
    let family: Vec<EngineRanked> = engine.rank_index(&channel.name, index);
    let Some(top) = family.first() else {
        return base(
            None,
            None,
            None,
            "No Match".to_string(),
            "no premium entry at the host cut, so no category".to_string(),
        );
    };
    let leader = db_names.get(top.index).cloned().unwrap_or_default();
    // The family's category: its first member (rank order) the category map holds.
    // Every member is the same identity, so a regional duplicate the map skips (a
    // country label, `CNN HD` -> `United States`) never hides the real one (`CNN`).
    let categorized = family.iter().find_map(|member| {
        let name = db_names.get(member.index)?;
        categories
            .get(&name.to_lowercase())
            .map(|(_, category)| (member, name.clone(), category.clone()))
    });
    match categorized {
        Some((member, name, category)) => {
            let match_type = match_type_of(engine, &channel.name, &name, member.score);
            let via = if name == leader {
                String::new()
            } else {
                format!(" (family of {leader:?})")
            };
            base(
                Some(category),
                Some(name.clone()),
                Some(member.score),
                match_type.clone(),
                format!("{match_type} {name}{via}"),
            )
        }
        None => base(
            None,
            Some(leader.clone()),
            Some(top.score),
            match_type_of(engine, &channel.name, &leader, top.score),
            format!(
                "no member of {leader:?}'s family carries a category (a country label is \
                 none), so no move"
            ),
        ),
    }
}

/// Restrict the scoped channels to CM's category scope (`category_groups`).
///
/// Empty means every scoped channel (CM's blank include reads all groups plus
/// ungrouped). Non-empty keeps only channels whose group name is EXACTLY one of
/// the tokens — case-sensitive, no wildcards, like CM's `include_wildcards =
/// False` branch — and a token naming no group fails the run, like CM's
/// `GroupScopeError` (`None of the groups named in 'Category Organization
/// Groups' could be found`). Un grouped channels drop out when the list is set
/// (CM's `include_ungrouped = False`).
pub fn restrict_to_category_scope(
    world: &World,
    channels: &[crosstalk_core::dispatcharr::Channel],
    category_groups: &str,
) -> Result<Vec<crosstalk_core::dispatcharr::Channel>> {
    let tokens = parse_tokens(category_groups);
    if tokens.is_empty() {
        return Ok(channels.to_vec());
    }
    let available: HashSet<&str> = world
        .groups
        .iter()
        .map(|group| group.name.as_str())
        .collect();
    let matched: HashSet<&str> = tokens
        .iter()
        .map(String::as_str)
        .filter(|token| available.contains(token))
        .collect();
    if matched.is_empty() {
        bail!(
            "None of the groups named in 'Category Organization Groups' could be found: {}",
            tokens.join(", ")
        );
    }
    Ok(channels
        .iter()
        .filter(|channel| matched.contains(world.group_name(channel.channel_group_id).as_str()))
        .cloned()
        .collect())
}

/// Whether a category is an ignored target: never created, never moved into.
///
/// CM's `is_ignored_name_tokens` on the new group name (case-insensitive,
/// wildcards), checked against `[scope].ignore_groups` — the host home of CM's
/// `ignore_groups`.
pub fn is_ignored_target(category: &str, ignore_groups: &str) -> bool {
    if category.trim().is_empty() {
        return false;
    }
    let (matched, _) = expand_patterns(&parse_tokens(ignore_groups), &[category.to_string()], true);
    !matched.is_empty()
}

/// The set moves of the classified rows: channels whose category differs from
/// their current group and whose target is not ignored.
pub fn planned_moves<'a>(rows: &'a [CategoryRow], ignore_groups: &str) -> Vec<&'a CategoryRow> {
    rows.iter()
        .filter(|row| {
            row.category
                .as_deref()
                .is_some_and(|category| category != row.group)
                && !row
                    .category
                    .as_deref()
                    .is_some_and(|category| is_ignored_target(category, ignore_groups))
        })
        .collect()
}

/// Plan the moves as one per-channel `Set Group` through the shared planner.
///
/// Pure: nothing is written. Callers pass only the moves whose group id is
/// already known (existing groups for a preview; existing plus just-created
/// groups, resolved against a world augmented with the creates, for an apply).
pub fn plan_moves(
    world: &World,
    selection: &Selection,
    moves: &[&CategoryRow],
    group_ids: &HashMap<String, i64>,
) -> Result<channel_ops::Plan> {
    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for row in moves {
        if let Some(category) = row.category.as_deref() {
            if let Some(id) = group_ids.get(category) {
                values.insert(row.id, SetValue::Group(Some(*id)));
            }
        }
    }
    if values.is_empty() {
        return Ok(channel_ops::Plan::default());
    }
    let ops = vec![Op::Set {
        field: Field::Group,
        values,
    }];
    channel_ops::plan_with_options(world, selection, &ops, &PlanOptions::default())
}

/// Final per-channel outcomes from the classified rows plus the plans.
///
/// `group_exists` is CM's wording (`Yes` / `No (will be created)`); a move to a
/// group the create stage refused (or, on preview, a move whose group is still
/// to be created) reports `planned` rather than a plan status.
pub fn final_outputs(
    rows: &[CategoryRow],
    moves: &[&CategoryRow],
    existing: &HashSet<String>,
    plan: &channel_ops::Plan,
    applied: Option<&channel_ops::ApplyReport>,
) -> Vec<CategoryOutput> {
    let moving: HashSet<i64> = moves.iter().map(|row| row.id).collect();
    let planned: HashMap<i64, &channel_ops::Change> = plan
        .changes
        .iter()
        .map(|change| (change.channel_id, change))
        .collect();
    let skipped: HashMap<i64, &str> = plan
        .skipped
        .iter()
        .map(|skip| (skip.channel_id, skip.reason.as_str()))
        .collect();
    rows.iter()
        .map(|row| {
            let (status, new_group, group_exists, reason) = match row.category.as_deref() {
                None => (
                    "skipped".to_string(),
                    row.group.clone(),
                    String::new(),
                    row.reason.clone(),
                ),
                Some(category) if category == row.group => (
                    "skipped".to_string(),
                    row.group.clone(),
                    "Yes".to_string(),
                    "already in its category group".to_string(),
                ),
                Some(category) => {
                    let exists = if existing.contains(category) {
                        "Yes"
                    } else {
                        "No (will be created)"
                    };
                    if !moving.contains(&row.id) {
                        (
                            "skipped".to_string(),
                            category.to_string(),
                            exists.to_string(),
                            format!(
                                "target group {category:?} is in Channel Groups to Ignore, \
                                 so the channel stays where it is"
                            ),
                        )
                    } else if let Some(change) = planned.get(&row.id) {
                        let status = match applied {
                            None => "planned",
                            Some(_) => match change.status {
                                channel_ops::RowStatus::Ok => "move",
                                channel_ops::RowStatus::Failed => "failed",
                                channel_ops::RowStatus::Planned => "planned",
                            },
                        };
                        let reason = if applied.is_some() && !change.reason.is_empty() {
                            change.reason.clone()
                        } else {
                            row.reason.clone()
                        };
                        (
                            status.to_string(),
                            category.to_string(),
                            exists.to_string(),
                            reason,
                        )
                    } else if let Some(reason) = skipped.get(&row.id) {
                        (
                            "skipped".to_string(),
                            category.to_string(),
                            exists.to_string(),
                            (*reason).to_string(),
                        )
                    } else {
                        (
                            "planned".to_string(),
                            category.to_string(),
                            exists.to_string(),
                            row.reason.clone(),
                        )
                    }
                }
            };
            CategoryOutput {
                id: row.id,
                number: row.number,
                old: row.old.clone(),
                current_group: row.group.clone(),
                new_group,
                category: row.category.clone().unwrap_or_default(),
                match_type: row.match_type.clone(),
                match_value: row.match_name.clone().unwrap_or_default(),
                group_exists,
                status,
                reason,
                callsign: row.identity.callsign.clone(),
                network: row.identity.network.clone(),
                country: row.identity.country.clone(),
                zone: crate::names::zone_label(&row.identity).to_string(),
                canonical: row.identity.canonical_name.clone(),
            }
        })
        .collect()
}

/// Run `categorize_preview` / `categorize_apply`.
///
/// Preview plans and writes the CSV; nothing is written to Dispatcharr. Apply
/// creates the missing groups first, then moves every channel whose category
/// group differs, recorded as run rows for the host undo.
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    run_with_creator(app, context, settings, dry_run, None).await
}

/// [`run`] with an injectable group creator (tests script the POSTs).
#[allow(clippy::too_many_lines)]
pub async fn run_with_creator(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
    creator: Option<Arc<dyn ChannelCreator>>,
) -> Result<JobOutcome> {
    let action_id = if dry_run {
        crate::ACTION_CATEGORIZE_PREVIEW
    } else {
        crate::ACTION_CATEGORIZE_APPLY
    };
    let client = context.dispatcharr()?;
    let world: World = client
        .load_world_with(WorldKinds::only(WorldKind::Channels) | WorldKind::Groups)
        .await?;
    let config = context.config();

    let base = context.base_selection(&world)?;
    let scoped = context.scoped_channels(&world)?;
    let in_category_scope =
        restrict_to_category_scope(&world, &scoped.channels, &settings.category_groups)?;

    // The unified stack: the same engine + identity cache guide and streams run on.
    let run_engine = RunEngine::for_config(&config.matching, context.store().cloned());
    let engine = Arc::clone(run_engine.engine());
    let db_names = crate::names::premium_db_names(&config.matching.databases());
    let categories = category_map(&config.matching.databases());
    let index = Arc::new(run_engine.build_index(&db_names));

    let ctx = ResolveContext::default();
    let mut rows: Vec<CategoryRow> = Vec::with_capacity(in_category_scope.len());
    for channel in &in_category_scope {
        let scoped_channel = ScopedChannel {
            id: channel.id,
            name: channel.name.clone(),
            number: channel.display_number(),
            group: world.group_name(channel.channel_group_id),
        };
        let identity = run_engine.resolve(&channel.name, &ctx);
        rows.push(decide_category(
            &scoped_channel,
            &identity,
            &engine,
            &index,
            &db_names,
            &categories,
        ));
    }

    let moves = planned_moves(&rows, &base.ignore_groups);
    let existing: HashSet<String> = world
        .groups
        .iter()
        .map(|group| group.name.clone())
        .collect();
    // Missing categories become create-stage groups, owned by lineup, in first-
    // seen order (stable: the run reads the same world next time).
    let mut missing: Vec<String> = Vec::new();
    let mut seen_missing: HashSet<String> = HashSet::new();
    for row in &moves {
        if let Some(category) = row.category.as_deref() {
            if !existing.contains(category) && seen_missing.insert(category.to_string()) {
                missing.push(category.to_string());
            }
        }
    }

    // Crash-safe creates (intents + ownership, step 10 review): reconcile before
    // planning so an earlier run's landed group is reused, never duplicated.
    let owned = channel_ops::load_owned(context, &world, crate::APP_ID)?;
    let request = CreateRequest {
        app: crate::APP_ID.to_string(),
        groups: missing
            .iter()
            .map(|name| NewGroup {
                name: name.clone(),
                key: category_owner_key(name),
            })
            .collect(),
        logos: Vec::new(),
        channels: Vec::new(),
    };
    let create_plan =
        channel_ops::plan_creates(&world, &owned.channels, &request, &PlanOptions::default())?;

    let creator: Arc<dyn ChannelCreator> = match creator {
        Some(creator) => creator,
        None => Arc::new(RestChannelCreator::new(
            client.clone(),
            config.dispatcharr.concurrency,
        )),
    };
    let create_report = if dry_run {
        None
    } else {
        Some(channel_ops::apply_creates(creator, &create_plan, context, action_id).await)
    };

    // The Set plan runs against the world augmented with the groups this run
    // just created (same names, real ids) — the channels themselves are still
    // the live state the preview read.
    let mut planned_world = world.clone();
    if let Some(report) = create_report.as_ref() {
        for (group, outcome) in create_plan.groups.iter().zip(report.groups.iter()) {
            if let Some(id) = outcome.id {
                if group.existing_id.is_none()
                    && !planned_world.groups.iter().any(|known| known.id == id)
                {
                    planned_world.groups.push(ChannelGroup {
                        id,
                        name: group.name.clone(),
                    });
                }
            }
        }
    }
    let group_ids: HashMap<String, i64> = planned_world
        .groups
        .iter()
        .map(|group| (group.name.clone(), group.id))
        .collect();
    // A preview can only move into groups that already exist; a move into a
    // group the create stage would make stays a decision until the apply.
    let plannable: Vec<&CategoryRow> = if dry_run {
        moves
            .iter()
            .filter(|row| {
                row.category
                    .as_deref()
                    .is_some_and(|category| existing.contains(category))
            })
            .copied()
            .collect()
    } else {
        // A group the create stage refused leaves its channels behind with the
        // reason, instead of planning a move to an id that does not exist.
        let live: HashSet<&str> = group_ids.keys().map(String::as_str).collect();
        moves
            .iter()
            .filter(|row| {
                row.category
                    .as_deref()
                    .is_some_and(|category| live.contains(category))
            })
            .copied()
            .collect()
    };
    let refused: HashSet<i64> = moves
        .iter()
        .filter(|row| !plannable.iter().any(|planned| planned.id == row.id))
        .map(|row| row.id)
        .collect();
    let plan = plan_moves(&planned_world, &base, &plannable, &group_ids)?;
    let applied = if dry_run {
        None
    } else {
        Some(channel_ops::apply(client, &plan, context, action_id).await)
    };

    // A preview cannot move into a group that does not exist yet: those rows stay
    // decisions (`planned`, the create stage lists the group). On an apply, a group
    // the create stage refused leaves its channels behind with the reason.
    let mut outputs = final_outputs(&rows, &plannable, &existing, &plan, applied.as_ref());
    if !dry_run {
        for output in outputs.iter_mut() {
            if refused.contains(&output.id) {
                output.status = "failed".to_string();
                output.reason = format!(
                    "group {:?} could not be created, so the channel stays where it is",
                    output.new_group,
                );
            }
        }
    }

    let mut moved = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;
    for output in &outputs {
        match output.status.as_str() {
            "move" | "planned" => moved += 1,
            "failed" => failed += 1,
            _ => skipped += 1,
        }
    }
    let created = create_report
        .as_ref()
        .map(|report| {
            report
                .groups
                .iter()
                .filter(|outcome| outcome.created && outcome.status == channel_ops::RowStatus::Ok)
                .count()
        })
        .unwrap_or_else(|| {
            create_plan
                .groups
                .iter()
                .filter(|group| group.existing_id.is_none())
                .count()
        });

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = if dry_run {
        "categorize_preview"
    } else {
        "categorize_applied"
    };
    let title = if dry_run {
        "Categorize Preview Report"
    } else {
        "Categorize Applied Report"
    };
    let cut = crosstalk_core::engine_run::cut_setting(&config.matching);
    let report = Report::new(&context.exports_dir(), app, stem, title)
        .channels_processed(in_category_scope.len())
        .result("Selection", &scoped.filter_description)
        .result("Category scope", category_scope_description(settings))
        .result("Moves planned", plan.changes.len())
        .result(
            "Moves written",
            match &applied {
                Some(report) => report.ok.to_string(),
                None => "none, this was a preview".to_string(),
            },
        )
        .result("Skipped", plan.skipped.len() + skipped)
        .setting(cut.0, cut.1)
        .write(context.config(), &schema, &values, &COLUMNS, |writer| {
            for output in &outputs {
                writer.write_record([
                    output.id.to_string(),
                    csv_report::safe_cell(&output.old),
                    csv_report::safe_cell(&output.current_group),
                    csv_report::safe_cell(&output.new_group),
                    csv_report::safe_cell(&output.category),
                    csv_report::safe_cell(&output.match_type),
                    csv_report::safe_cell(&output.match_value),
                    csv_report::safe_cell(&output.group_exists),
                    output.status.clone(),
                    csv_report::safe_cell(&output.reason),
                    output.callsign.clone().unwrap_or_default(),
                    output.network.clone().unwrap_or_default(),
                    output.country.clone().unwrap_or_default(),
                    output.zone.clone(),
                    csv_report::safe_cell(&output.canonical),
                ])?;
            }
            Ok(())
        })?;

    context.record_rows(&channel_ops::create_and_edit_run_rows(
        &create_plan,
        create_report.as_ref(),
        &plan,
        applied.as_ref(),
    ))?;

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let targets: HashSet<&str> = moves
        .iter()
        .filter_map(|row| row.category.as_deref())
        .collect();
    let mut summary = if dry_run {
        format!(
            "Categorize preview: {moved} move(s) into {} categor(ies) ({created} new group(s)), {skipped} skipped",
            targets.len(),
        )
    } else {
        let (ok, failed) = applied
            .as_ref()
            .map(|report| (report.ok, report.failed))
            .unwrap_or((0, 0));
        format!(
            "Categorize apply: moved {ok} channel(s) ({failed} failed, {created} group(s) created), {skipped} skipped"
        )
    };
    if moved > 0 {
        summary.push_str(&format!(" — selection: {}", scoped.filter_description));
    }
    let mut built = OutcomeBuilder::new(summary)
        .count("channels", in_category_scope.len() as i64)
        .count("moves", moved as i64)
        .count("groups_created", created as i64)
        .count("skipped", skipped as i64)
        .count(
            "written",
            applied.as_ref().map(|report| report.ok as i64).unwrap_or(0),
        )
        .count(
            "failed",
            applied
                .as_ref()
                .map(|report| report.failed as i64)
                .unwrap_or(failed as i64),
        )
        .report(Some(report));
    if let Some(note) = channel_ops::cancelled_note(context, &plan, applied.as_ref()) {
        built = built.cancelled().note(note);
    }
    Ok(built.build())
}

/// The category scope in words, for the report preamble.
fn category_scope_description(settings: &crate::settings::LineupSettings) -> String {
    let trimmed = settings.category_groups.trim();
    if trimmed.is_empty() {
        "every category (category_groups is empty)".to_string()
    } else {
        format!("categories of groups: {trimmed}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::config::CrosstalkConfig;
    use crosstalk_core::dispatcharr::{Channel, ChannelGroup};
    use crosstalk_core::jobs::VecSink;
    use crosstalk_core::store::{RunStatus, Store};
    use crosstalk_core::subapp::Registry;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_settings() -> crate::settings::LineupSettings {
        crate::settings::LineupSettings::default()
    }

    /// The engine, its index over the merged DB names, those names and the category map.
    type TestEngine = (
        Arc<UnifiedEngine>,
        Arc<crosstalk_matching::engine::EngineIndex>,
        Vec<String>,
        HashMap<String, (String, String)>,
    );

    fn test_engine() -> TestEngine {
        let engine = Arc::new(UnifiedEngine::decided());
        let config = CrosstalkConfig::default();
        let db_names = crate::names::premium_db_names(&config.matching.databases());
        assert!(!db_names.is_empty(), "merged US DB is embedded");
        let categories = category_map(&config.matching.databases());
        assert!(!categories.is_empty(), "merged US DB carries categories");
        let index = Arc::new(crosstalk_matching::engine::EngineIndex::build(
            &db_names,
            &engine,
            &ResolveContext::default(),
        ));
        (engine, index, db_names, categories)
    }

    fn scoped(id: i64, name: &str, group: &str) -> ScopedChannel {
        ScopedChannel {
            id,
            name: name.to_string(),
            number: Some(id as f64),
            group: group.to_string(),
        }
    }

    #[test]
    fn premium_names_classify_to_their_db_category() {
        // The merged DB's own names resolve to the category the DB states.
        let (engine, index, db_names, categories) = test_engine();
        let (name, category) = categories
            .values()
            .find(|(name, _)| name == "CNN")
            .cloned()
            .expect("CNN is a categorized DB entry");
        let channel = scoped(1, "CNN", "Test");
        let identity = crosstalk_matching::identity::resolve_with(
            &engine,
            &channel.name,
            &ResolveContext::default(),
        );
        let row = decide_category(&channel, &identity, &engine, &index, &db_names, &categories);
        assert_eq!(row.category.as_deref(), Some(category.as_str()));
        assert_eq!(row.match_name.as_deref(), Some(name.as_str()));
        assert_eq!(row.match_type, "Premium (Exact)");
    }

    #[test]
    fn country_labels_are_no_category() {
        // Every merged DB's country name and code is a label, not a category.
        for label in [
            "United States",
            "united kingdom",
            "Brazil",
            "Canada",
            "US",
            "uk",
        ] {
            assert!(is_country_category(label), "{label}");
        }
        for category in ["News", "Sports", "Regional", "Movies", "Broadcast"] {
            assert!(!is_country_category(category), "{category}");
        }
        let (_, _, _, categories) = test_engine();
        assert!(
            categories
                .values()
                .all(|(_, category)| !is_country_category(category)),
            "the map never holds a country label"
        );
        // The regional duplicate never shadows the real entry, whatever the order.
        assert_eq!(
            categories
                .get("hungry")
                .map(|(_, category)| category.as_str()),
            Some("Lifestyle")
        );
        assert_eq!(categories.get("cnn hd"), None);
    }

    #[test]
    fn a_decorated_name_takes_its_familys_category() {
        // `US: CNN HD` may lead with the regional `CNN HD` entry (a country label);
        // the family's first categorized member decides, so it still reads News.
        let (engine, index, db_names, categories) = test_engine();
        for name in ["US: CNN HD", "CNN HD", "CNN"] {
            let channel = scoped(1, name, "Test");
            let identity = crosstalk_matching::identity::resolve_with(
                &engine,
                &channel.name,
                &ResolveContext::default(),
            );
            let row = decide_category(&channel, &identity, &engine, &index, &db_names, &categories);
            assert_eq!(row.category.as_deref(), Some("News"), "{name}: {row:?}");
        }
    }

    #[test]
    fn an_ota_station_name_stays_put() {
        // CM gives OTA no category in organize_by_category (no callsign branch,
        // and its name-only path is `No Match` for this name). The unified ranker
        // reaches the `WIAT (CBS)` station entry (C07), whose DB category is the
        // country (`United States`): an OTA identity is never classified.
        let (engine, index, db_names, categories) = test_engine();
        let channel = scoped(1, "CBS - AL Birmingham (WIAT) HD", "Test");
        let identity = crosstalk_matching::identity::resolve_with(
            &engine,
            &channel.name,
            &ResolveContext::default(),
        );
        assert_eq!(identity.callsign.as_deref(), Some("WIAT"));
        let row = decide_category(&channel, &identity, &engine, &index, &db_names, &categories);
        assert_eq!(row.category, None, "OTA takes no category here");
        assert_eq!(row.match_type, "OTA");
        assert!(row.reason.contains("OTA station WIAT"), "{}", row.reason);
    }

    #[test]
    fn the_category_scope_is_exact_and_fail_closed() {
        let world = World {
            channels: vec![
                Channel {
                    id: 1,
                    name: "CNN".to_string(),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 2,
                    name: "ESPN".to_string(),
                    channel_group_id: Some(11),
                    ..Channel::default()
                },
                Channel {
                    id: 3,
                    name: "HBO".to_string(),
                    channel_group_id: None,
                    ..Channel::default()
                },
            ],
            groups: vec![
                ChannelGroup {
                    id: 10,
                    name: "News".to_string(),
                },
                ChannelGroup {
                    id: 11,
                    name: "Sports".to_string(),
                },
            ],
            ..World::default()
        };
        // Empty: every scoped channel, ungrouped included.
        let all = restrict_to_category_scope(&world, &world.channels, "").expect("all");
        assert_eq!(all.len(), 3);
        // Exact names, case-sensitive, no wildcards: `News` matches, `news` and
        // `*` match nothing — and a list matching nothing fails the run.
        let news = restrict_to_category_scope(&world, &world.channels, "News").expect("news");
        assert_eq!(news.iter().map(|c| c.id).collect::<Vec<_>>(), vec![1]);
        let error = restrict_to_category_scope(&world, &world.channels, "news").expect_err("case");
        assert!(format!("{error:#}").contains("news"), "{error:#}");
        // A token naming no group fails the run, like CM's GroupScopeError.
        let error = restrict_to_category_scope(&world, &world.channels, "Nope").expect_err("fails");
        assert!(format!("{error:#}").contains("Nope"), "{error:#}");
        // A list where only some tokens match filters to the matched groups.
        let some =
            restrict_to_category_scope(&world, &world.channels, "News, Nope").expect("partial");
        assert_eq!(some.iter().map(|c| c.id).collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn ignored_targets_are_never_created_or_entered() {
        assert!(is_ignored_target("News", "news"));
        assert!(is_ignored_target("News", "New*"));
        assert!(!is_ignored_target("News", "Sports"));
        assert!(!is_ignored_target("News", ""));
        // Case-insensitive like CM's is_ignored_name_tokens.
        assert!(is_ignored_target("NEWS", "news"));
    }

    #[test]
    fn owner_keys_name_the_category() {
        assert_eq!(category_owner_key("News"), "lineup:category:News");
    }

    async fn serve(world: &World) -> (MockServer, CrosstalkConfig, tempfile::TempDir) {
        let server = MockServer::start().await;
        let list = |items: serde_json::Value| ResponseTemplate::new(200).set_body_json(items);
        for (route, items) in [
            (
                "/api/channels/channels/",
                serde_json::to_value(&world.channels).expect("channels"),
            ),
            (
                "/api/channels/groups/",
                serde_json::to_value(&world.groups).expect("groups"),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(list(items))
                .mount(&server)
                .await;
        }
        // One mock per created group (ids the undo half of the test asserts).
        for (name, id) in [("News", 9004), ("Sports", 9006)] {
            Mock::given(method("POST"))
                .and(path("/api/channels/groups/"))
                .and(body_string_contains(format!("\"name\":\"{name}\"")))
                .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                    "id": id,
                    "name": name,
                })))
                .mount(&server)
                .await;
        }
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"message": "ok", "channels": []})),
            )
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        (server, config, dir)
    }

    fn context_for(
        config: &CrosstalkConfig,
        store: &Store,
        app: &str,
        action: &str,
        dry_run: bool,
    ) -> (JobContext, i64) {
        let run_id = store.start_run(app, action, dry_run).expect("start run");
        let context = JobContext::new(Arc::new(config.clone()), Arc::new(VecSink::new()))
            .with_dispatcharr(
                crosstalk_core::dispatcharr::DispatcharrClient::new(&config.dispatcharr)
                    .expect("client"),
            )
            .for_run(
                app,
                action,
                run_id,
                store.clone(),
                Arc::new(AtomicBool::new(false)),
            );
        (context, run_id)
    }

    fn categorize_world() -> World {
        World {
            channels: vec![
                Channel {
                    id: 1,
                    name: "CNN".to_string(),
                    channel_number: Some(serde_json::json!(1.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 2,
                    name: "ESPN".to_string(),
                    channel_number: Some(serde_json::json!(2.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
                Channel {
                    id: 3,
                    name: "Zzqz No Such Channel 999".to_string(),
                    channel_number: Some(serde_json::json!(3.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
            ],
            groups: vec![ChannelGroup {
                id: 10,
                name: "Test".to_string(),
            }],
            ..World::default()
        }
    }

    #[tokio::test]
    async fn preview_writes_nothing_but_the_cm_csv() {
        let world = categorize_world();
        // No POST routes mounted: a preview that writes anything fails loudly.
        let server = MockServer::start().await;
        let list = |items: serde_json::Value| ResponseTemplate::new(200).set_body_json(items);
        for (route, items) in [
            (
                "/api/channels/channels/",
                serde_json::to_value(&world.channels).expect("channels"),
            ),
            (
                "/api/channels/groups/",
                serde_json::to_value(&world.groups).expect("groups"),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(list(items))
                .mount(&server)
                .await;
        }
        let _dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(_dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, _) = context_for(&config, &store, "lineup", "categorize_preview", true);
        let outcome = run(&app, &context, &test_settings(), true)
            .await
            .expect("previews");
        assert!(
            outcome.summary.contains("Categorize preview"),
            "{}",
            outcome.summary
        );
        let writes: Vec<_> = server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.method.as_str() != "GET")
            .collect();
        assert!(writes.is_empty(), "a preview writes nothing");
        let path = outcome.report_path.expect("a report");
        let text = std::fs::read_to_string(&path).expect("readable");
        let mut lines = text.lines().filter(|line| !line.starts_with('#'));
        let header = lines.next().expect("header");
        assert_eq!(
            header,
            "id,channel_name,current_group,new_group,category,match_type,match_value,\
             group_exists,status,reason,callsign,network,country,zone,canonical"
        );
        assert_eq!(lines.count(), 3, "every in-scope channel is a row");
    }

    #[tokio::test]
    async fn apply_creates_owned_groups_and_moves() {
        let world = categorize_world();
        let (server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, run_id) = context_for(&config, &store, "lineup", "categorize_apply", false);
        let outcome = run(&app, &context, &test_settings(), false)
            .await
            .expect("applies");
        assert!(
            outcome.summary.contains("Categorize apply"),
            "{}",
            outcome.summary
        );
        // The moves went out as bulk edits, the missing categories as group POSTs.
        let requests = server.received_requests().await.unwrap_or_default();
        let posts = requests
            .iter()
            .filter(|request| {
                request.method.as_str() == "POST" && request.url.path() == "/api/channels/groups/"
            })
            .count();
        assert!(posts > 0, "missing category groups are created");
        // Ownership: the created groups are recorded under lineup:category:<name>.
        let owned = store.owned_keys(crate::APP_ID).expect("owned");
        assert!(
            owned
                .iter()
                .any(|(kind, _, key)| kind == "group" && key.starts_with("lineup:category:")),
            "{owned:?}"
        );
        // Undo reverses the moves (the created groups are left and listed).
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("applied"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let mut moved_world = world.clone();
        for channel in moved_world.channels.iter_mut() {
            if channel.id == 1 {
                channel.channel_group_id = Some(9000 + "News".len() as i64);
            }
        }
        // The created News group is in the world for the undo to see.
        moved_world.groups.push(ChannelGroup {
            id: 9000 + "News".len() as i64,
            name: "News".to_string(),
        });
        let registry = Registry::new(vec![Arc::new(crate::LineupApp::new())]);
        let undo = crosstalk_core::undo::plan_undo(&store, &moved_world, run_id, &registry)
            .expect("undo plans");
        assert!(
            undo.changes.iter().any(|change| change.channel_id == 1
                && change.reverse == channel_ops::SetValue::Group(Some(10))),
            "{undo:?}"
        );
        assert!(
            undo.left.iter().any(|left| left.kind == "group"),
            "created groups are left and listed: {undo:?}"
        );
    }

    #[tokio::test]
    async fn category_groups_restricts_the_source() {
        let world = categorize_world();
        let (server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let mut settings = test_settings();
        settings.category_groups = "Test".to_string();
        let (context, first) = context_for(&config, &store, "lineup", "categorize_preview", true);
        let outcome = run(&app, &context, &settings, true)
            .await
            .expect("previews");
        assert_eq!(outcome.counts.get("channels"), Some(&3));
        // One guarded run per app at a time: finish the first before the second.
        store
            .finish_run(
                first,
                RunStatus::Succeeded,
                Some("previewed"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        // A token naming no group fails the run, like CM's GroupScopeError.
        let mut settings = test_settings();
        settings.category_groups = "Nope".to_string();
        let (context, _) = context_for(&config, &store, "lineup", "categorize_preview", true);
        let error = run(&app, &context, &settings, true)
            .await
            .expect_err("refuses");
        assert!(format!("{error:#}").contains("Nope"), "{error:#}");
        let _ = server;
    }
}
