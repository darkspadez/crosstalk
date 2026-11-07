//! `create_*`: Channel-Mapparr's two creation modes on the unified engine.
//!
//! Port of `reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/plugin.py`
//! `import_m3u_streams` (~3342-4041: `_fetch_streams_from_m3u_sources`,
//! `_match_streams_to_categories`, `_ensure_category_groups_exist`,
//! `_get_next_channel_number`, `_detect_duplicate_channels`,
//! `_import_matched_streams`) and `create_channels_from_streams` (~4508-4768:
//! `_collect_unattached_streams`, `_seed_resolver`, `build_seed_plan`,
//! `allocate_channel_numbers`, `_create_seed_channels`), plus
//! `channel_seeder.py` (`build_seed_plan`, `allocate_channel_numbers`).
//!
//! # Target-name/category algorithm per mode (with CM citations)
//!
//! Both modes resolve through the same [`RunEngine`] + identity cache guide and
//! streams run on, and both name through the SAME helpers [`names`](crate::names)
//! uses — the created channel carries the name a rename would have given it:
//!
//! - **Streams** (`import_m3u_streams`): candidate streams come from streams'
//!   [`StreamPool`](crosstalk_streams::selection::StreamPool)
//!   (`selected_m3us` account priorities and stream-group selection, the one M3U
//!   account selection since the step-13 review moved CM `m3u_sources` there),
//!   narrowed by `m3u_group_filter` BEFORE matching (CM's
//!   `_fetch_streams_from_m3u_sources`: resolved group name, case-insensitive,
//!   `plugin.py:3448-3470`). Each stream's identity is the unified
//!   [`Identity`](crosstalk_matching::identity::Identity); its target name is
//!   [`decide_target`](crate::names::decide_target) on the stream's match name —
//!   the ONE naming decision `names` renames by (OTA template for stations incl.
//!   the opt-in market fallback, premium canonical + tag re-attachment). Only an
//!   OTA or premium target is created: a station-less printed callsign (never
//!   premium, like names), an OTA template missing a field and an unmatched
//!   stream are reported, never created — the unknown-suffix tag names applies
//!   is a rename, never a reason to create (CM imported only matched streams
//!   too). A premium target's category is
//!   [`decide_category`](crate::categorize::decide_category) (country-named
//!   categories are none, like categorize); an OTA target maps to CM's
//!   `OTA_IMPORT_CATEGORY` (`"Broadcast"`, `plugin.py:72,3557`) — categorize
//!   never classifies OTA (no callsign branch there), but the import path is
//!   the one place CM ever writes that category, so create keeps it.
//!   `m3u_category_filter` applies AFTER matching, case-insensitively
//!   (`plugin.py:3641`); the group is `m3u_custom_group_name` or the category.
//!   A family with no category and no custom group is skipped with that reason
//!   (CM fell back to the DB's country label or `Entertainment`; step-16
//!   decision A makes a country label no category, and create never invents
//!   one).
//! - **Stations** (`create_channels_from_streams` + `channel_seeder.py`):
//!   unattached streams (no `ChannelStream` row, `plugin.py:4508`) in
//!   `seed_source_groups` (exact group names, like CM), OTA stations only
//!   ([`decide_ota_target`](crate::names::decide_ota_target), the OTA half of
//!   the one naming decision: callsign station, opt-in market fallback, a
//!   station-less callsign never premium — CM's `_seed_resolver` renders "the
//!   same OTA name format the rename action uses"), grouped by proposed name
//!   (sorted, like `build_seed_plan`), skipping existing names
//!   case-insensitively and `seed_exclude_networks` (station primary network or
//!   a word in the stream name, `plugin.py:4530`), one channel per station in
//!   `seed_target_group`, numbers from `seed_start_number` via
//!   [`next_free_numbers`](crosstalk_core::channel_ops::next_free_numbers)
//!   (global, skipping used — the same walk as `allocate_channel_numbers`),
//!   no streams attached (as CM: a created channel is a target for
//!   `streams.match`, `channel_seeder.py` module docs).
//!
//! # Documented divergences from CM (fixes, strictly safer)
//!
//! - **One channel per identity family** (streams mode). CM makes one channel
//!   per matched STREAM (`_import_matched_streams` loops every stream, suffixing
//!   duplicates `[{M3U}-{id}]`), so one station carried once per M3U account
//!   becomes N channels. The apps complement each other instead: one channel
//!   per [`same_family`](crosstalk_matching::identity::Identity::same_family)
//!   group, carrying the family's streams in the streams app's quality order
//!   ([`quality_order`]: the key `streams.match` ranks on, over the same pool) —
//!   exactly what `streams.match` would then attach. The seeder already groups
//!   this way.
//! - **A stream whose identity already has an owned channel is never imported
//!   again.** CM duplicated on rerun (its name-suffix dedupe only sees names,
//!   and the second run's names differ by suffix). Ownership keys
//!   `lineup:create:<identity key>` plus a same-family check against live
//!   channels make a rerun (and a foreign same-identity channel) a reported
//!   skip, never a duplicate.
//! - **Collision guard on create names**: the shared resolver
//!   (`PlanOptions::collision`, global like every rename) instead of CM's
//!   per-import `[N]` / `[M3U-id]` suffixes.
//! - **Global number-conflict check** (`PlanOptions::number_conflicts`): a
//!   planned number already used by any channel is refused, like every create.
//! - **No AI gate**: unmatched streams are reported, never guessed.
//! - **A missing stations target group is created (owned)** instead of erroring
//!   like CM (`Create it first, then run this action`); an ambiguous (duplicated)
//!   target name still fails, like CM.
//!
//! # Planning, CSV, undo
//!
//! `create_preview` (Read) / `create_apply` (Write, confirm) plan from LIVE
//! state through [`plan_creates`](crosstalk_core::channel_ops::plan_creates)
//! (groups first, `load_owned` reconciled before planning so intents adopt and
//! reruns dedupe) and apply through
//! [`apply_creates`](crosstalk_core::channel_ops::apply_creates) (concurrent
//! POSTs behind the [`ChannelCreator`](crosstalk_core::channel_ops::ChannelCreator)
//! seam). Ownership keys are [`create_owner_key`] for channels and
//! [`create_group_owner_key`] for groups. The host
//! [`undo`](crosstalk_core::undo) hides created channels; created groups are
//! left and listed (the plan decision for every create stage).

use anyhow::{bail, Result};
use crosstalk_core::channel_ops::{
    self, ChannelCreator, CreateRequest, GroupRef, NewChannel, NewGroup, PlanOptions,
    RestChannelCreator,
};
use crosstalk_core::csv_report::{self, Report};
use crosstalk_core::dispatcharr::{World, WorldKind, WorldKinds};
use crosstalk_core::engine_run::RunEngine;
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use crosstalk_core::wildcard::parse_tokens;
use crosstalk_matching::engine::{EngineIndex, UnifiedEngine};
use crosstalk_matching::identity::{Identity, ResolveContext};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// CM's import category for OTA stations (`plugin.py:72`).
pub const OTA_IMPORT_CATEGORY: &str = "Broadcast";

/// The streams-mode CSV columns: CM's import preview columns
/// (`plugin.py:~3980`: Stream ID through Notes) plus the planned channel, its
/// number and the identity family the streams join as one.
pub const COLUMNS_STREAMS: [&str; 13] = [
    "id",
    "stream_name",
    "m3u_source",
    "priority",
    "match_type",
    "match_method",
    "category",
    "target_group",
    "group_exists",
    "will_import",
    "planned_channel",
    "channel_number",
    "notes",
];

/// The stations-mode CSV columns: CM's seed preview columns
/// (`_export_seed_preview`) plus the allocated number and the reason.
pub const COLUMNS_STATIONS: [&str; 7] = [
    "action",
    "proposed_channel",
    "source_streams",
    "stream_count",
    "m3u_accounts",
    "channel_number",
    "reason",
];

/// The ownership key of a created channel: `lineup:create:<identity key>`.
///
/// The identity key is the family's equality fields (normalize key, zone,
/// timeshift, callsign — [`Identity::family_key`] in words), so a rerun finds
/// the object it made whatever the channel was renamed to since.
pub fn create_owner_key(identity: &Identity) -> String {
    format!("lineup:create:{}", family_key_string(identity))
}

/// The family key in words (also the grouping key for one-channel-per-family).
pub fn family_key_string(identity: &Identity) -> String {
    format!(
        "{}|{:?}|{}|{}",
        identity.key,
        identity.zone,
        identity.timeshift,
        identity.callsign.as_deref().unwrap_or("-")
    )
}

/// The ownership key of a group create makes: `lineup:create:group:<name>`.
pub fn create_group_owner_key(name: &str) -> String {
    format!("lineup:create:group:{name}")
}

/// The unified stack one create run resolves and ranks on.
pub struct EngineStack<'a> {
    pub engine: &'a UnifiedEngine,
    pub index: &'a EngineIndex,
    pub db_names: &'a [String],
    pub categories: &'a HashMap<String, (String, String)>,
}

// ---------------------------------------------------------------------------
// Streams mode
// ---------------------------------------------------------------------------

/// One candidate stream, as create sees it.
#[derive(Debug, Clone)]
pub struct CandidateStream {
    pub id: i64,
    pub name: String,
    /// The name matching sees (pool regex rules applied).
    pub match_name: String,
    pub priority: usize,
    pub account: String,
    pub group: String,
}

/// One stream's import decision (one row per stream, like CM's preview).
#[derive(Debug, Clone)]
pub struct StreamRow {
    pub stream_id: i64,
    pub stream_name: String,
    pub m3u_source: String,
    pub priority: usize,
    pub matcher: String,
    pub method: String,
    pub category: String,
    pub target_group: String,
    pub group_exists: bool,
    /// The family head's row carries the create; members join; the rest skip.
    pub will_import: bool,
    pub is_head: bool,
    pub family: String,
    pub family_size: usize,
    pub channel_name: String,
    pub number: Option<f64>,
    pub note: String,
    pub identity: Identity,
}

/// Build streams-mode candidates from the pool: eligible streams in
/// (priority, id) order, narrowed by the pre-match M3U group filter (CM's
/// resolved-group-name, case-insensitive match; a stream with no group never
/// matches a filter).
pub fn streams_candidates(
    world: &World,
    pool: &crosstalk_streams::selection::StreamPool<'_>,
    group_filter: &str,
) -> Vec<CandidateStream> {
    let filter: HashSet<String> = parse_tokens(group_filter)
        .into_iter()
        .map(|token| token.to_lowercase())
        .collect();
    let mut candidates: Vec<CandidateStream> = Vec::new();
    for id in &pool.eligible {
        let Some(stream) = pool.get(*id) else {
            continue;
        };
        if !filter.is_empty() {
            let name = stream.channel_group.and_then(|group_id| {
                world
                    .groups
                    .iter()
                    .find(|group| group.id == group_id)
                    .map(|group| group.name.to_lowercase())
            });
            if name.as_deref().is_none_or(|name| !filter.contains(name)) {
                continue;
            }
        }
        candidates.push(CandidateStream {
            id: stream.id,
            name: stream.name.clone(),
            match_name: pool
                .match_name_of(stream.id)
                .unwrap_or(stream.name.as_str())
                .to_string(),
            priority: pool.priority.get(&stream.id).copied().unwrap_or(999),
            account: pool.account_name(stream),
            group: world.group_name(stream.channel_group),
        });
    }
    candidates.sort_by_key(|candidate| (candidate.priority, candidate.id));
    candidates
}

/// Every live channel's identity, for the foreign same-identity check.
pub fn world_identities(run_engine: &RunEngine, world: &World) -> Vec<(i64, String, Identity)> {
    let ctx = ResolveContext::default();
    world
        .channels
        .iter()
        .map(|channel| {
            (
                channel.id,
                channel.name.clone(),
                run_engine.resolve(&channel.name, &ctx),
            )
        })
        .collect()
}

/// Decide every candidate stream: match, categorize, group by family.
///
/// Pure: nothing is written. `owned` maps this app's owner keys to live channel
/// ids ([`load_owned`](crosstalk_core::channel_ops::load_owned)); `world_names`
/// resolves every live channel name to its identity for the foreign
/// same-identity check. Returns one row per candidate in candidate order; the
/// caller numbers the creatable families and posts them.
#[allow(clippy::too_many_arguments)]
pub fn decide_streams(
    candidates: &[CandidateStream],
    stack: &EngineStack<'_>,
    settings: &crate::settings::LineupSettings,
    owned: &HashMap<String, i64>,
    world_names: &[(i64, String, Identity)],
) -> Vec<StreamRow> {
    let category_filter: HashSet<String> = parse_tokens(&settings.m3u_category_filter)
        .into_iter()
        .map(|token| token.to_lowercase())
        .collect();
    let custom_group = settings.m3u_custom_group_name.trim().to_string();

    // Per-stream match + category first (order kept for the report).
    struct Matched {
        candidate: CandidateStream,
        identity: Identity,
        matcher: String,
        method: String,
        target: Option<String>,
        category: Option<String>,
        no_match_reason: Option<String>,
    }
    let mut matched: Vec<Matched> = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let ctx = ResolveContext::default();
        let identity =
            crosstalk_matching::identity::resolve_with(stack.engine, &candidate.match_name, &ctx);
        // The name and the category come from the SAME decisions names and
        // categorize make (one function each), so a created channel is named
        // exactly as `names_apply` would name it and filed where `categorize`
        // would file it.
        let target = crate::names::decide_target(
            &candidate.match_name,
            &identity,
            stack.engine,
            stack.index,
            stack.db_names,
            settings,
        );
        let (matcher, method, target, category, no_match_reason) = match target {
            // OTA stations take CM's import category (`plugin.py:3557`); categorize
            // never classifies OTA (no callsign branch), this is the one place CM
            // writes it.
            crate::names::NameTarget::Ota {
                name: Some(name), ..
            } => (
                "Broadcast (OTA)".to_string(),
                "OTA - Callsign Match".to_string(),
                Some(name),
                Some(OTA_IMPORT_CATEGORY.to_string()),
                None,
            ),
            crate::names::NameTarget::Ota {
                callsign,
                name: None,
                ..
            } => (
                "Broadcast (OTA)".to_string(),
                "OTA - Callsign Match".to_string(),
                None,
                None,
                Some(format!(
                    "OTA station {callsign}: missing required fields for the OTA format"
                )),
            ),
            // CM's import tried premium after a station-less callsign; names never
            // does (`ABC (WZZZ)` must not become the national `ABC`), so neither
            // does create.
            crate::names::NameTarget::BlockedCallsign(callsign) => (
                "none".to_string(),
                "No Match".to_string(),
                None,
                None,
                Some(format!(
                    "callsign {callsign} is not in the station table, and a printed \
                     callsign never matches premium"
                )),
            ),
            crate::names::NameTarget::Premium {
                name,
                score: _,
                method,
            } => {
                let scoped = crate::categorize::ScopedChannel {
                    id: candidate.id,
                    name: candidate.match_name.clone(),
                    number: None,
                    group: candidate.group.clone(),
                };
                // A country label or a family with no category is none, like
                // categorize (step-16 decision A).
                let category = crate::categorize::decide_category(
                    &scoped,
                    &identity,
                    stack.engine,
                    stack.index,
                    stack.db_names,
                    stack.categories,
                )
                .category;
                (
                    "Premium/Cable".to_string(),
                    crate::names::method_label(&method),
                    Some(name),
                    category,
                    None,
                )
            }
            crate::names::NameTarget::Unmatched => (
                "none".to_string(),
                "No Match".to_string(),
                None,
                None,
                Some("no match in the channel databases at the host cut".to_string()),
            ),
        };
        matched.push(Matched {
            candidate: candidate.clone(),
            identity,
            matcher,
            method,
            target,
            category,
            no_match_reason,
        });
    }

    // Group by identity family (first-seen order = pool-priority order).
    let mut families: Vec<(String, Vec<usize>)> = Vec::new();
    let mut family_pos: HashMap<String, usize> = HashMap::new();
    for (ordinal, row) in matched.iter().enumerate() {
        let key = family_key_string(&row.identity);
        match family_pos.get(&key) {
            Some(&pos) => families[pos].1.push(ordinal),
            None => {
                family_pos.insert(key.clone(), families.len());
                families.push((key, vec![ordinal]));
            }
        }
    }

    // One decision per stream.
    let mut rows: Vec<StreamRow> = Vec::with_capacity(matched.len());
    for (family_key, members) in &families {
        let head = &matched[members[0]];
        let key = create_owner_key(&head.identity);
        // The family's shared facts, from the head (lowest priority, then id):
        // every member is the same identity, so the target name the head's
        // decorations re-attach is the channel's name.
        let mut decision: Option<String> = None; // Some = skip reason
        let mut target_group = String::new();
        let mut category_label = String::new();
        if head.target.is_none() {
            decision = Some(format!(
                "{}, so nothing is created (create never guesses: no unknown suffix, no AI)",
                head.no_match_reason.as_deref().unwrap_or("no match")
            ));
        } else if let Some(id) = owned.get(&key) {
            decision = Some(format!(
                "key {key:?} was already created as channel {id}, so no duplicate is made"
            ));
        } else if let Some((id, name, _)) = world_names
            .iter()
            .find(|(_, _, identity)| identity.same_family(&head.identity))
        {
            decision = Some(format!(
                "a channel with the same identity already exists (channel {id} {name:?}), \
                 so nothing is created"
            ));
        } else {
            let category = head.category.clone().unwrap_or_default();
            if !category_filter.is_empty() && !category_filter.contains(&category.to_lowercase()) {
                decision = Some(format!("Category '{category}' not in filter list"));
            } else if category.is_empty() && custom_group.is_empty() {
                decision = Some(
                    "no category (the family's DB entries carry none, or only a country \
                     label) and no custom import group, so there is no group to create in"
                        .to_string(),
                );
            } else {
                category_label = category.clone();
                target_group = if custom_group.is_empty() {
                    category
                } else {
                    custom_group.clone()
                };
            }
        }
        let will_import = decision.is_none();
        let channel_name = if will_import {
            head.target.clone().unwrap_or_default()
        } else {
            String::new()
        };
        for (member_ordinal, &index) in members.iter().enumerate() {
            let row = &matched[index];
            let (import, note, head_only) = match decision.as_deref() {
                None if member_ordinal == 0 => (
                    true,
                    format!(
                        "family of {} stream(s); attaches the family's streams in the \
                         streams app's quality order",
                        members.len()
                    ),
                    true,
                ),
                None => (
                    true,
                    format!("joins the family channel {channel_name:?}"),
                    false,
                ),
                Some(reason) => (false, reason.to_string(), false),
            };
            rows.push(StreamRow {
                stream_id: row.candidate.id,
                stream_name: row.candidate.name.clone(),
                m3u_source: row.candidate.account.clone(),
                priority: row.candidate.priority,
                matcher: row.matcher.clone(),
                method: row.method.clone(),
                category: category_label.clone(),
                target_group: target_group.clone(),
                group_exists: false, // filled by the caller against the world
                will_import: import,
                is_head: head_only,
                family: family_key.clone(),
                family_size: members.len(),
                channel_name: channel_name.clone(),
                number: None, // filled when the families are numbered
                note,
                identity: row.identity.clone(),
            });
        }
    }
    // Back in candidate order (families were first-seen, members in order, so a
    // stable sort by stream id order restores the report's stream order).
    let mut order: HashMap<i64, usize> = HashMap::new();
    for (ordinal, candidate) in candidates.iter().enumerate() {
        order.insert(candidate.id, ordinal);
    }
    rows.sort_by_key(|row| order.get(&row.stream_id).copied().unwrap_or(usize::MAX));
    rows
}

/// The creatable families of decided rows, in first-seen order: the head rows
/// that will import.
pub fn creatable_families(rows: &[StreamRow]) -> Vec<&StreamRow> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut heads = Vec::new();
    for row in rows {
        if row.is_head && seen.insert(row.family.as_str()) {
            heads.push(row);
        }
    }
    heads
}

/// Build the create request for the creatable families: missing groups plus one
/// channel per family, its streams in the order `order` gives them (the run
/// passes [`quality_order`]: the streams app's quality key, the order
/// `streams.match` would attach them in).
///
/// Numbers come from [`next_free_numbers`](crosstalk_core::channel_ops::next_free_numbers)
/// above the highest channel number in use (CM's max+1, `plugin.py:3749`), so a
/// used number is never handed out; the plan's own conflict pass is still on.
pub fn build_streams_request(
    world: &World,
    rows: &mut [StreamRow],
    order: &dyn Fn(&[i64]) -> Vec<i64>,
) -> Result<CreateRequest> {
    let mut heads: Vec<usize> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for (ordinal, row) in rows.iter().enumerate() {
        if row.is_head && seen.insert(row.family.as_str()) {
            heads.push(ordinal);
        }
    }
    let start = world
        .channels
        .iter()
        .filter_map(|channel| channel.display_number())
        .fold(None::<f64>, |max, number| {
            Some(max.map_or(number, |m: f64| m.max(number)))
        })
        .map_or(1.0, |max| max + 1.0);
    let numbers = next_free(world, start, heads.len());
    // Groups in first-seen order (stable: the same world plans the same groups).
    let mut group_index: HashMap<String, usize> = HashMap::new();
    let mut groups: Vec<NewGroup> = Vec::new();
    for &ordinal in &heads {
        let name = rows[ordinal].target_group.clone();
        if !group_index.contains_key(&name) {
            group_index.insert(name.clone(), groups.len());
            groups.push(NewGroup {
                name: name.clone(),
                key: create_group_owner_key(&name),
            });
        }
    }
    let mut channels: Vec<NewChannel> = Vec::new();
    for (&ordinal, number) in heads.iter().zip(numbers.iter()) {
        let family = rows[ordinal].family.clone();
        // Members arrive in candidate order (M3U priority, then id); the
        // streams app's sort decides the final order.
        let members: Vec<i64> = rows
            .iter()
            .filter(|row| row.family == family)
            .map(|row| row.stream_id)
            .collect();
        let streams = order(&members);
        let group = *group_index.get(&rows[ordinal].target_group).unwrap_or(&0);
        channels.push(NewChannel {
            name: rows[ordinal].channel_name.clone(),
            number: Some(*number),
            group: Some(GroupRef::Planned(group)),
            logo: None,
            streams,
            profile_ids: None,
            tvg_id: None,
            epg_data_id: None,
            key: create_owner_key(&rows[ordinal].identity),
        });
        rows[ordinal].number = Some(*number);
        for row in rows.iter_mut().filter(|row| row.family == family) {
            row.number = Some(*number);
        }
    }
    Ok(CreateRequest {
        app: crate::APP_ID.to_string(),
        groups,
        logos: Vec::new(),
        channels,
    })
}

/// A family's streams in the streams app's order:
/// [`sort_streams`](crosstalk_streams::quality::sort_streams) over the pool's
/// [`quality_view`](crosstalk_streams::selection::StreamPool::quality_view) (the
/// key `streams.match` ranks on: M3U priority first unless `prioritize_quality`,
/// then throughput, resolution, frame rate, ...), stable, so equal keys keep the
/// candidate order.
pub fn quality_order(
    pool: &crosstalk_streams::selection::StreamPool<'_>,
    model: &crosstalk_streams::quality::QualityModel,
    throughput: &dyn crosstalk_streams::quality::ThroughputSource,
    ids: &[i64],
) -> Vec<i64> {
    let views: Vec<_> = ids.iter().map(|id| pool.quality_view(*id)).collect();
    crosstalk_streams::quality::sort_streams(&views, model, throughput)
        .into_iter()
        .map(|view| view.id)
        .collect()
}

/// [`next_free_numbers`](crosstalk_core::channel_ops::next_free_numbers), one
/// place so both modes share the global walk.
fn next_free(world: &World, start: f64, n: usize) -> Vec<f64> {
    channel_ops::next_free_numbers(world, start, n)
}

// ---------------------------------------------------------------------------
// Stations mode
// ---------------------------------------------------------------------------

/// One candidate stream for stations mode.
#[derive(Debug, Clone)]
pub struct StationCandidate {
    pub id: i64,
    pub name: String,
    pub group: String,
    pub account: String,
}

/// One station's seed decision (one row per station, like `build_seed_plan`).
#[derive(Debug, Clone)]
pub struct StationRow {
    /// `create`, `skip, name already used`, `skip, network excluded` or `unresolved`.
    pub action: String,
    pub proposed: String,
    pub callsign: Option<String>,
    pub source_names: Vec<String>,
    pub stream_ids: Vec<i64>,
    pub accounts: Vec<String>,
    pub number: Option<f64>,
    pub reason: String,
    pub key: String,
}

/// The networks named in a station's free-text affiliation field, in order
/// (CM's `station_affiliation.station_networks`, verbatim).
///
/// Uppercase, duplicates removed, subchannel numbers and parenthetical
/// annotations stripped. `&` splits a segment unless the whole segment is one
/// of the known ampersand names (`Heroes & Icons`); a leading callsign
/// (`KALB/NBC`) is dropped when something follows.
pub fn station_networks(affiliation: &str) -> Vec<String> {
    static NOT_APPLICABLE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let not_applicable = NOT_APPLICABLE
        .get_or_init(|| regex::Regex::new(r"^\s*N\s*/\s*A\s*$").expect("static regex"));
    if affiliation.trim().is_empty() || not_applicable.is_match(affiliation) {
        return Vec::new();
    }
    static PARENS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let parens = PARENS.get_or_init(|| regex::Regex::new(r"\([^)]*\)").expect("static regex"));
    static SUBCHANNEL: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let subchannel = SUBCHANNEL
        .get_or_init(|| regex::Regex::new(r"(?i)\bCh\.?\s*\d+(?:\.\d+)?").expect("static regex"));
    static DECIMAL: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let decimal = DECIMAL.get_or_init(|| regex::Regex::new(r"\b\d+\.\d+\b").expect("static regex"));
    let mut text = parens.replace_all(affiliation, " ").to_string();
    text = subchannel.replace_all(&text, " ").to_string();
    text = decimal.replace_all(&text, " ").to_string();
    // Split on comma, semicolon, slash, "and" and ". " first, but never on
    // "&" at this stage (CM's comment, kept: whether "&" separates or
    // belongs is judged per segment below).
    static SPLIT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let split = SPLIT
        .get_or_init(|| regex::Regex::new(r"(?i)[,;/]|\s+\band\b\s+|\.\s+").expect("static regex"));
    static AMPERSAND: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let ampersand = AMPERSAND.get_or_init(|| regex::Regex::new(r"\s*&\s*").expect("static regex"));
    static CALLSIGN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let callsign =
        CALLSIGN.get_or_init(|| regex::Regex::new(r"^[KW][A-Z]{2,3}$").expect("static regex"));
    static WS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let ws = WS.get_or_init(|| regex::Regex::new(r"\s+").expect("static regex"));
    const AMPERSAND_NAMES: [&str; 3] = ["HEROES & ICONS", "H&I", "H & I"];
    const DROP_TOKENS: [&str; 5] = ["CH", "DT", "TV", "AND", "ON"];
    let mut parts: Vec<String> = Vec::new();
    for segment in split.split(&text) {
        let cleaned = ws
            .replace_all(segment.trim().trim_matches('.'), " ")
            .to_uppercase();
        if cleaned.contains('&') && !AMPERSAND_NAMES.contains(&cleaned.as_str()) {
            parts.extend(ampersand.split(segment).map(str::to_string));
        } else {
            parts.push(segment.to_string());
        }
    }
    let mut networks: Vec<String> = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        let token = part.trim().trim_matches('.').to_uppercase();
        if token.is_empty() || DROP_TOKENS.contains(&token.as_str()) {
            continue;
        }
        if index == 0 && parts.len() > 1 && callsign.is_match(&token) {
            continue;
        }
        let token = ws.replace_all(&token, " ").to_string();
        if !networks.contains(&token) {
            networks.push(token);
        }
    }
    networks
}

/// The primary network a station IS: the first network named, or `None`
/// (CM's `station_affiliation.primary_network`).
fn primary_network(affiliation: &str) -> Option<String> {
    station_networks(affiliation).into_iter().next()
}

/// Whether a station is excluded: its primary network or a word in the stream
/// name is in the skip set (`plugin.py:4530-4590`: the record is the better
/// signal, but a low-power record often names no network, so the name is read
/// too).
fn station_excluded(
    station_affiliation: Option<&str>,
    stream_name: &str,
    skip: &HashSet<String>,
) -> bool {
    if skip.is_empty() {
        return false;
    }
    let mut signals: HashSet<String> = HashSet::new();
    if let Some(primary) = station_affiliation.and_then(primary_network) {
        signals.insert(primary);
    }
    static WORDS: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let words = WORDS.get_or_init(|| regex::Regex::new(r"[^A-Za-z]+").expect("static regex"));
    signals.extend(
        words
            .split(stream_name)
            .filter(|w| !w.is_empty())
            .map(|w| w.to_uppercase()),
    );
    !skip.is_disjoint(&signals)
}

/// Decide every candidate stream's station: resolve, exclude, group by proposed
/// name (sorted, like `build_seed_plan`).
///
/// Pure: nothing is written. `existing` holds every live channel name
/// (case-insensitive skip, like the seeder); `owned` the app's live owner keys
/// (rerun dedupe: a created-then-hidden channel keeps its ownership, so undo
/// followed by a rerun creates nothing).
pub fn decide_stations(
    candidates: &[StationCandidate],
    engine: &UnifiedEngine,
    settings: &crate::settings::LineupSettings,
    existing: &HashSet<String>,
    owned: &HashMap<String, i64>,
) -> Vec<StationRow> {
    let skip_networks: HashSet<String> = parse_tokens(&settings.seed_exclude_networks)
        .into_iter()
        .map(|token| token.to_uppercase())
        .collect();
    let ctx = ResolveContext::default();

    // One lookup per stream name (the seeder memoizes the same way).
    let mut by_proposed: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut by_unresolved: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut by_excluded: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    // Resolved facts per candidate: (proposed, callsign, affiliation, identity).
    type Resolved = (String, String, Option<String>, Identity);
    let mut resolved: Vec<Option<Resolved>> = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let identity = crosstalk_matching::identity::resolve_with(engine, &candidate.name, &ctx);
        // The OTA decision names makes (callsign station, then the opt-in
        // market fallback; a station-less callsign never goes premium): CM's
        // `_seed_resolver` renders "the same OTA name format the rename action
        // uses". A template missing a field is unresolved, like a rename skip.
        let hit = match crate::names::decide_ota_target(&candidate.name, &identity, settings) {
            Some(crate::names::NameTarget::Ota {
                callsign,
                station,
                name: Some(name),
                ..
            }) => Some((name, callsign, station.network_affiliation)),
            _ => None,
        };
        match hit {
            Some((proposed, callsign, affiliation)) => {
                let excluded =
                    station_excluded(affiliation.as_deref(), &candidate.name, &skip_networks);
                resolved.push(Some((proposed.clone(), callsign, affiliation, identity)));
                let ordinal = resolved.len() - 1;
                if excluded {
                    by_excluded.entry(proposed).or_default().push(ordinal);
                } else {
                    by_proposed.entry(proposed).or_default().push(ordinal);
                }
            }
            None => {
                resolved.push(None);
                let ordinal = resolved.len() - 1;
                by_unresolved
                    .entry(candidate.name.clone())
                    .or_default()
                    .push(ordinal);
            }
        }
    }

    let mut rows = Vec::new();
    for (proposed, members) in &by_proposed {
        let (callsign, key) = match resolved[members[0]].as_ref() {
            Some((_, callsign, _, identity)) => {
                (Some(callsign.clone()), create_owner_key(identity))
            }
            None => (None, String::new()),
        };
        let mut source_names: Vec<String> = Vec::new();
        let mut stream_ids: Vec<i64> = Vec::new();
        let mut accounts: Vec<String> = Vec::new();
        for &ordinal in members {
            let candidate = &candidates[ordinal];
            if !source_names.contains(&candidate.name) {
                source_names.push(candidate.name.clone());
            }
            stream_ids.push(candidate.id);
            if !accounts.contains(&candidate.account) {
                accounts.push(candidate.account.clone());
            }
        }
        let (action, reason) = if existing.contains(&proposed.trim().to_lowercase()) {
            (
                "skip, name already used".to_string(),
                format!("a channel named {proposed:?} already exists"),
            )
        } else if !key.is_empty() && owned.contains_key(&key) {
            let id = owned.get(&key).copied().unwrap_or(0);
            (
                "skip, name already used".to_string(),
                format!("key {key:?} was already created as channel {id}, so no duplicate is made"),
            )
        } else {
            (
                "create".to_string(),
                format!(
                    "station {} from {} unattached stream(s)",
                    callsign.as_deref().unwrap_or("?"),
                    stream_ids.len()
                ),
            )
        };
        rows.push(StationRow {
            action,
            proposed: proposed.clone(),
            callsign,
            source_names,
            stream_ids,
            accounts,
            number: None,
            reason,
            key,
        });
    }
    for (proposed, members) in &by_excluded {
        let mut source_names: Vec<String> = Vec::new();
        let mut stream_ids: Vec<i64> = Vec::new();
        let mut accounts: Vec<String> = Vec::new();
        for &ordinal in members {
            let candidate = &candidates[ordinal];
            if !source_names.contains(&candidate.name) {
                source_names.push(candidate.name.clone());
            }
            stream_ids.push(candidate.id);
            if !accounts.contains(&candidate.account) {
                accounts.push(candidate.account.clone());
            }
        }
        rows.push(StationRow {
            action: "skip, network excluded".to_string(),
            proposed: proposed.clone(),
            callsign: None,
            source_names,
            stream_ids,
            accounts,
            number: None,
            reason: "the station's network is in Networks to Skip".to_string(),
            key: String::new(),
        });
    }
    for (source, members) in &by_unresolved {
        let mut stream_ids: Vec<i64> = Vec::new();
        let mut accounts: Vec<String> = Vec::new();
        for &ordinal in members {
            let candidate = &candidates[ordinal];
            stream_ids.push(candidate.id);
            if !accounts.contains(&candidate.account) {
                accounts.push(candidate.account.clone());
            }
        }
        rows.push(StationRow {
            action: "unresolved".to_string(),
            proposed: String::new(),
            callsign: None,
            source_names: vec![source.clone()],
            stream_ids,
            accounts,
            number: None,
            reason: "no OTA station resolves from the stream name".to_string(),
            key: String::new(),
        });
    }
    rows
}

/// Build the create request for the stations to create: the target group plus
/// one channel per station, no streams attached (as CM).
pub fn build_stations_request(
    world: &World,
    rows: &mut [StationRow],
    settings: &crate::settings::LineupSettings,
    target_group: &str,
) -> Result<CreateRequest> {
    let create: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| row.action == "create")
        .map(|(ordinal, _)| ordinal)
        .collect();
    let start = if settings.seed_start_number.trim().is_empty() {
        world
            .channels
            .iter()
            .filter_map(|channel| channel.display_number())
            .fold(None::<f64>, |max, number| {
                Some(max.map_or(number, |m: f64| m.max(number)))
            })
            .map_or(1.0, |max| max + 1.0)
    } else {
        settings
            .seed_start_number
            .trim()
            .parse::<f64>()
            .map_err(|_| {
                anyhow::anyhow!(
                    "seed_start_number must be a number, got {:?}",
                    settings.seed_start_number
                )
            })?
    };
    let numbers = next_free(world, start, create.len());
    for (&ordinal, number) in create.iter().zip(numbers.iter()) {
        rows[ordinal].number = Some(*number);
    }
    let channels: Vec<NewChannel> = create
        .iter()
        .map(|&ordinal| {
            let row = &rows[ordinal];
            NewChannel {
                name: row.proposed.clone(),
                number: row.number,
                group: Some(GroupRef::Planned(0)),
                logo: None,
                // No streams attached: a created channel is a target for
                // streams.match (CM, channel_seeder.py).
                streams: Vec::new(),
                profile_ids: None,
                tvg_id: None,
                epg_data_id: None,
                key: row.key.clone(),
            }
        })
        .collect();
    Ok(CreateRequest {
        app: crate::APP_ID.to_string(),
        groups: vec![NewGroup {
            name: target_group.to_string(),
            key: create_group_owner_key(target_group),
        }],
        logos: Vec::new(),
        channels,
    })
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

/// The streams settings create reads: the one M3U account selection
/// (`selected_m3us`) plus the stream-group filter and name rules the
/// [`StreamPool`](crosstalk_streams::selection::StreamPool) is built from.
fn streams_settings(
    config_dir: &std::path::Path,
) -> Result<crosstalk_streams::settings::StreamsSettings> {
    crosstalk_streams::settings::load(&config_dir.join("streams.toml"))
}

/// Run `create_preview` / `create_apply`.
///
/// Preview plans and writes the CSV; nothing is written to Dispatcharr. Apply
/// creates the missing groups first, then one channel per identity family
/// (streams mode) or per station (stations mode), recorded as run rows for the
/// host undo.
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    run_with_creator(app, context, settings, dry_run, None).await
}

/// [`run`] with an injectable channel creator (tests script the POSTs).
#[allow(clippy::too_many_lines)]
pub async fn run_with_creator(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
    creator: Option<Arc<dyn ChannelCreator>>,
) -> Result<JobOutcome> {
    let action_id = if dry_run {
        crate::ACTION_CREATE_PREVIEW
    } else {
        crate::ACTION_CREATE_APPLY
    };
    match settings.create_mode.trim() {
        "streams" => run_streams(app, context, settings, dry_run, action_id, creator).await,
        "stations" => run_stations(app, context, settings, dry_run, action_id, creator).await,
        other => bail!("create_mode must be one of streams, stations, got {other:?}"),
    }
}

fn plan_options(settings: &crate::settings::LineupSettings) -> Result<PlanOptions> {
    Ok(PlanOptions {
        collision: Some(crate::clean::collision_strategy(settings)?),
        collision_sep: settings.join_separator.clone(),
        number_conflicts: true,
        ..PlanOptions::default()
    })
}

fn creator_of(
    context: &JobContext,
    creator: Option<Arc<dyn ChannelCreator>>,
) -> Result<Arc<dyn ChannelCreator>> {
    match creator {
        Some(creator) => Ok(creator),
        None => {
            let client = context.dispatcharr()?;
            Ok(Arc::new(RestChannelCreator::new(
                client.clone(),
                context.config().dispatcharr.concurrency,
            )))
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn run_streams(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
    action_id: &str,
    creator: Option<Arc<dyn ChannelCreator>>,
) -> Result<JobOutcome> {
    let client = context.dispatcharr()?;
    let world: World = client
        .load_world_with(
            WorldKinds::only(WorldKind::Channels)
                | WorldKind::Groups
                | WorldKind::Streams
                | WorldKind::M3uAccounts,
        )
        .await?;
    let config = context.config();
    let base = context.base_selection(&world)?;

    // The stream pool: streams' own selection (accounts, priorities, groups,
    // name rules), the step-13 home of CM's `m3u_sources`.
    let streams_config = streams_settings(&config.config_dir)?;
    let pool = crosstalk_streams::selection::build(&world, &streams_config)?;

    // `m3u_group_filter` BEFORE matching (CM): the stream's resolved group
    // name, case-insensitive; a stream with no group never matches a filter.
    let candidates = streams_candidates(&world, &pool, &settings.m3u_group_filter);

    // The unified stack: the same engine + identity cache guide and streams run on.
    let run_engine = RunEngine::for_config(&config.matching, context.store().cloned());
    let engine = Arc::clone(run_engine.engine());
    let db_names = crate::names::premium_db_names(&config.matching.databases());
    let categories = crate::categorize::category_map(&config.matching.databases());
    let index = Arc::new(run_engine.build_index(&db_names));
    let stack = EngineStack {
        engine: &engine,
        index: &index,
        db_names: &db_names,
        categories: &categories,
    };

    // Crash-safe creates (intents + ownership, step 10 review): reconcile before
    // planning so an earlier run's landed channel is reused, never duplicated.
    let owned = channel_ops::load_owned(context, &world, crate::APP_ID)?;
    let world_names = world_identities(&run_engine, &world);
    let mut rows = decide_streams(&candidates, &stack, settings, &owned.channels, &world_names);

    // CM refuses to write into an ignored group (GroupScopeError): fail the run
    // naming the groups rather than creating half the plan.
    let mut targets: Vec<String> = Vec::new();
    for row in creatable_families(&rows) {
        if !targets.contains(&row.target_group) {
            targets.push(row.target_group.clone());
        }
    }
    let blocked: Vec<String> = targets
        .into_iter()
        .filter(|name| crate::categorize::is_ignored_target(name, &base.ignore_groups))
        .collect();
    if !blocked.is_empty() {
        bail!(
            "Import would create or write into group(s) listed in 'Channel Groups to \
             Ignore': {}. Change the import target or remove them from the ignore list.",
            blocked.join(", ")
        );
    }

    let existing: HashSet<String> = world
        .groups
        .iter()
        .map(|group| group.name.clone())
        .collect();
    for row in rows.iter_mut() {
        row.group_exists = existing.contains(&row.target_group);
    }
    // The family's streams in the order `streams.match` would attach them: the
    // streams app's quality model over the same pool (one sort, not a second rule).
    let model = crosstalk_streams::quality::QualityModel::from_settings(&streams_config);
    let throughput = crosstalk_streams::quality::throughput_source(context.store())?;
    let order = |ids: &[i64]| quality_order(&pool, &model, throughput.as_ref(), ids);
    let request = build_streams_request(&world, &mut rows, &order)?;
    let options = plan_options(settings)?;
    let create_plan = channel_ops::plan_creates(&world, &owned.channels, &request, &options)?;

    let creator = creator_of(context, creator)?;
    let create_report = if dry_run {
        None
    } else {
        Some(channel_ops::apply_creates(creator, &create_plan, context, action_id).await)
    };

    // The plan may have resolved collisions (renamed) or refused numbers: read
    // the final names back by key for the report.
    let final_name: HashMap<String, &str> = create_plan
        .channels
        .iter()
        .map(|channel| (channel.key.clone(), channel.name.as_str()))
        .collect();
    let final_number: HashMap<String, Option<f64>> = create_plan
        .channels
        .iter()
        .map(|channel| (channel.key.clone(), channel.number))
        .collect();
    let families = creatable_families(&rows).len();
    let joined = rows
        .iter()
        .filter(|row| row.will_import && !row.is_head)
        .count();
    let skipped = rows.iter().filter(|row| !row.will_import).count();
    let created = create_report
        .as_ref()
        .map(|report| {
            report
                .channels
                .iter()
                .filter(|outcome| outcome.created && outcome.status == channel_ops::RowStatus::Ok)
                .count()
        })
        .unwrap_or(families);
    let groups_created = create_report
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
        "create_preview"
    } else {
        "create_applied"
    };
    let title = if dry_run {
        "Create Preview Report"
    } else {
        "Create Applied Report"
    };
    let cut = crosstalk_core::engine_run::cut_setting(&config.matching);
    let report = Report::new(&context.exports_dir(), app, stem, title)
        .channels_processed(candidates.len())
        .result("Selection", pool.description.clone())
        .result("M3U group filter", group_filter_description(settings))
        .result("Families to create", families)
        .result(
            "Channels written",
            if dry_run {
                "none, this was a preview".to_string()
            } else {
                created.to_string()
            },
        )
        .result("Skipped streams", skipped)
        .setting(cut.0, cut.1)
        .write(
            context.config(),
            &schema,
            &values,
            &COLUMNS_STREAMS,
            |writer| {
                for row in &rows {
                    let key = create_owner_key(&row.identity);
                    let planned = final_name
                        .get(&key)
                        .copied()
                        .unwrap_or(row.channel_name.as_str());
                    let number = final_number
                        .get(&key)
                        .copied()
                        .flatten()
                        .or(row.number)
                        .map(|n| format!("{n}"))
                        .unwrap_or_default();
                    let will = if row.will_import { "Yes" } else { "No" };
                    writer.write_record([
                        row.stream_id.to_string(),
                        csv_report::safe_cell(&row.stream_name),
                        csv_report::safe_cell(&row.m3u_source),
                        row.priority.to_string(),
                        csv_report::safe_cell(&row.matcher),
                        csv_report::safe_cell(&row.method),
                        csv_report::safe_cell(&row.category),
                        csv_report::safe_cell(&row.target_group),
                        if row.group_exists {
                            "Yes".to_string()
                        } else {
                            "No (will be created)".to_string()
                        },
                        will.to_string(),
                        csv_report::safe_cell(planned),
                        number,
                        csv_report::safe_cell(&row.note),
                    ])?;
                }
                Ok(())
            },
        )?;

    context.record_rows(&channel_ops::create_run_rows(
        &create_plan,
        create_report.as_ref(),
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

    let mut summary = if dry_run {
        format!(
            "Create preview (streams): {families} channel(s) from {} stream(s) \
             ({joined} joining a family channel), {skipped} skipped ({groups_created} new group(s))",
            candidates.len()
        )
    } else {
        let failed = create_report
            .as_ref()
            .map(|report| report.failed)
            .unwrap_or(0);
        format!(
            "Create apply (streams): created {created} channel(s) ({failed} failed, \
             {groups_created} group(s) created), {skipped} stream(s) skipped"
        )
    };
    if families > 0 {
        summary.push_str(&format!(" — selection: {}", pool.description));
    }
    Ok(OutcomeBuilder::new(summary)
        .count("streams", candidates.len() as i64)
        .count("families", families as i64)
        .count("joined", joined as i64)
        .count("created", created as i64)
        .count("groups_created", groups_created as i64)
        .count("skipped", skipped as i64)
        .count(
            "failed",
            create_report
                .as_ref()
                .map(|report| report.failed as i64)
                .unwrap_or(0),
        )
        .report(Some(report))
        .build())
}

fn group_filter_description(settings: &crate::settings::LineupSettings) -> String {
    let trimmed = settings.m3u_group_filter.trim();
    if trimmed.is_empty() {
        "every M3U group".to_string()
    } else {
        format!("M3U groups: {trimmed}")
    }
}

#[allow(clippy::too_many_lines)]
async fn run_stations(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &crate::settings::LineupSettings,
    dry_run: bool,
    action_id: &str,
    creator: Option<Arc<dyn ChannelCreator>>,
) -> Result<JobOutcome> {
    let source_names: Vec<String> = parse_tokens(&settings.seed_source_groups);
    if source_names.is_empty() {
        bail!("Set Stream Groups to Seed From before running this action.");
    }
    let target_name = settings.seed_target_group.trim().to_string();
    if target_name.is_empty() {
        bail!("Set Channel Group to Create In before running this action.");
    }

    let client = context.dispatcharr()?;
    let world: World = client
        .load_world_with(
            WorldKinds::only(WorldKind::Channels)
                | WorldKind::Groups
                | WorldKind::Streams
                | WorldKind::M3uAccounts,
        )
        .await?;
    let config = context.config();
    let base = context.base_selection(&world)?;

    // CM refuses an ignored or ambiguous target before planning.
    if crate::categorize::is_ignored_target(&target_name, &base.ignore_groups) {
        bail!(
            "Import would create or write into group(s) listed in 'Channel Groups to \
             Ignore': {target_name}. Change the import target or remove them from the \
             ignore list."
        );
    }
    if world
        .groups
        .iter()
        .filter(|group| group.name == target_name)
        .count()
        > 1
    {
        bail!(
            "More than one channel group is named {target_name:?}. Rename one of them so \
             the target is unambiguous."
        );
    }

    // Unattached streams in the source groups (exact names, like CM): a stream is
    // unattached when no channel's stream list references it.
    let wanted: HashSet<&str> = source_names.iter().map(String::as_str).collect();
    let linked: HashSet<i64> = world
        .channels
        .iter()
        .flat_map(|channel| channel.streams.iter().copied())
        .collect();
    let account_names: HashMap<i64, String> = world
        .m3u_accounts
        .iter()
        .map(|account| (account.id, account.name.clone()))
        .collect();
    let mut candidates: Vec<StationCandidate> = Vec::new();
    for stream in &world.streams {
        let group = world.group_name(stream.channel_group);
        if !wanted.contains(group.as_str()) {
            continue;
        }
        if linked.contains(&stream.id) || stream.name.trim().is_empty() {
            continue;
        }
        candidates.push(StationCandidate {
            id: stream.id,
            name: stream.name.clone(),
            group,
            account: stream
                .m3u_account
                .and_then(|id| account_names.get(&id).cloned())
                .unwrap_or_else(|| "custom".to_string()),
        });
    }
    candidates.sort_by(|a, b| (&a.name, a.id).cmp(&(&b.name, b.id)));

    let run_engine = RunEngine::for_config(&config.matching, context.store().cloned());
    let engine = Arc::clone(run_engine.engine());
    let owned = channel_ops::load_owned(context, &world, crate::APP_ID)?;
    let existing: HashSet<String> = world
        .channels
        .iter()
        .map(|channel| channel.name.trim().to_lowercase())
        .collect();
    let mut rows = decide_stations(&candidates, &engine, settings, &existing, &owned.channels);
    let request = build_stations_request(&world, &mut rows, settings, &target_name)?;
    let options = plan_options(settings)?;
    let create_plan = channel_ops::plan_creates(&world, &owned.channels, &request, &options)?;

    let creator = creator_of(context, creator)?;
    let create_report = if dry_run {
        None
    } else {
        Some(channel_ops::apply_creates(creator, &create_plan, context, action_id).await)
    };

    let create_count = rows.iter().filter(|row| row.action == "create").count();
    let skip_names = rows
        .iter()
        .filter(|row| row.action == "skip, name already used")
        .count();
    let excluded = rows
        .iter()
        .filter(|row| row.action == "skip, network excluded")
        .count();
    let unresolved = rows.iter().filter(|row| row.action == "unresolved").count();
    let created = create_report
        .as_ref()
        .map(|report| {
            report
                .channels
                .iter()
                .filter(|outcome| outcome.created && outcome.status == channel_ops::RowStatus::Ok)
                .count()
        })
        .unwrap_or(create_count);

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = if dry_run {
        "create_preview"
    } else {
        "create_applied"
    };
    let title = if dry_run {
        "Create Preview Report"
    } else {
        "Create Applied Report"
    };
    let cut = crosstalk_core::engine_run::cut_setting(&config.matching);
    let report = Report::new(&context.exports_dir(), app, stem, title)
        .channels_processed(candidates.len())
        .result("Source groups", source_names.join(", "))
        .result("Target group", target_name.clone())
        .result("Stations to create", create_count)
        .result(
            "Channels written",
            if dry_run {
                "none, this was a preview".to_string()
            } else {
                created.to_string()
            },
        )
        .result("Skipped, name already used", skip_names)
        .result("Skipped, network excluded", excluded)
        .result("Streams left unresolved", unresolved)
        .setting(cut.0, cut.1)
        .write(
            context.config(),
            &schema,
            &values,
            &COLUMNS_STATIONS,
            |writer| {
                for row in &rows {
                    writer.write_record([
                        row.action.clone(),
                        csv_report::safe_cell(&row.proposed),
                        csv_report::safe_cell(&row.source_names.join(" | ")),
                        row.stream_ids.len().to_string(),
                        csv_report::safe_cell(&row.accounts.join(" ")),
                        row.number.map(|n| format!("{n}")).unwrap_or_default(),
                        csv_report::safe_cell(&row.reason),
                    ])?;
                }
                Ok(())
            },
        )?;

    context.record_rows(&channel_ops::create_run_rows(
        &create_plan,
        create_report.as_ref(),
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

    let summary = if dry_run {
        format!(
            "Create preview (stations): would create {create_count} channel(s) in \
             {target_name:?}, skip {skip_names} already named, skip {excluded} by network, \
             leave {unresolved} unresolved"
        )
    } else {
        let failed = create_report
            .as_ref()
            .map(|report| report.failed)
            .unwrap_or(0);
        format!(
            "Create apply (stations): created {created} channel(s) in {target_name:?} \
             ({failed} failed, no streams attached), {skip_names} already named, \
             {excluded} excluded, {unresolved} unresolved"
        )
    };
    Ok(OutcomeBuilder::new(summary)
        .count("streams", candidates.len() as i64)
        .count("stations", create_count as i64)
        .count("created", created as i64)
        .count("skip_names", skip_names as i64)
        .count("excluded", excluded as i64)
        .count("unresolved", unresolved as i64)
        .count(
            "failed",
            create_report
                .as_ref()
                .map(|report| report.failed as i64)
                .unwrap_or(0),
        )
        .report(Some(report))
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::channel_ops::{Field, SetValue};
    use crosstalk_core::config::CrosstalkConfig;
    use crosstalk_core::dispatcharr::{
        Channel, ChannelCreate, ChannelGroup, Logo, M3uAccount, Stream,
    };
    use crosstalk_core::jobs::VecSink;
    use crosstalk_core::store::{CreateIntent, RunStatus, Store};
    use crosstalk_core::subapp::Registry;
    use std::sync::atomic::AtomicBool;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_settings() -> crate::settings::LineupSettings {
        crate::settings::LineupSettings::default()
    }

    /// The unified stack the tests decide on: the decided engine, its index over
    /// the merged DB names, those names and the category map.
    struct Stack {
        engine: UnifiedEngine,
        index: EngineIndex,
        db_names: Vec<String>,
        categories: HashMap<String, (String, String)>,
    }

    fn stack() -> Stack {
        let engine = UnifiedEngine::decided();
        let config = CrosstalkConfig::default();
        let db_names = crate::names::premium_db_names(&config.matching.databases());
        assert!(!db_names.is_empty(), "merged US DB is embedded");
        let categories = crate::categorize::category_map(&config.matching.databases());
        assert!(!categories.is_empty(), "merged US DB carries categories");
        let index = crosstalk_matching::engine::EngineIndex::build(
            &db_names,
            &engine,
            &ResolveContext::default(),
        );
        Stack {
            engine,
            index,
            db_names,
            categories,
        }
    }

    fn engine_stack(stack: &Stack) -> EngineStack<'_> {
        EngineStack {
            engine: &stack.engine,
            index: &stack.index,
            db_names: &stack.db_names,
            categories: &stack.categories,
        }
    }

    fn candidate(id: i64, name: &str) -> CandidateStream {
        CandidateStream {
            id,
            name: name.to_string(),
            match_name: name.to_string(),
            priority: 0,
            account: "Main Provider".to_string(),
            group: "Sports".to_string(),
        }
    }

    #[test]
    fn owner_keys_name_the_family_and_the_group() {
        let engine = UnifiedEngine::decided();
        let a =
            crosstalk_matching::identity::resolve_with(&engine, "CNN", &ResolveContext::default());
        assert!(create_owner_key(&a).starts_with("lineup:create:"));
        assert_eq!(create_owner_key(&a), create_owner_key(&a));
        assert_eq!(create_group_owner_key("News"), "lineup:create:group:News");
    }

    #[test]
    fn streams_mode_groups_one_channel_per_family() {
        // CM makes one channel per matched STREAM (suffixing duplicates); the
        // apps complement each other instead: one channel per identity family
        // carrying the family's streams.
        let stack = stack();
        let candidates = vec![
            candidate(101, "CNN"),
            CandidateStream {
                id: 102,
                priority: 1,
                account: "Backup Provider".to_string(),
                ..candidate(102, "CNN")
            },
            candidate(103, "ESPN"),
        ];
        let rows = decide_streams(
            &candidates,
            &engine_stack(&stack),
            &test_settings(),
            &HashMap::new(),
            &[],
        );
        assert_eq!(rows.len(), 3);
        let heads = creatable_families(&rows);
        assert_eq!(heads.len(), 2, "CNN x2 is one family, ESPN another");
        let cnn = heads
            .iter()
            .find(|row| row.stream_id == 101)
            .expect("CNN head");
        assert!(cnn.is_head && cnn.will_import);
        assert_eq!(cnn.family_size, 2);
        let joiner = rows
            .iter()
            .find(|row| row.stream_id == 102)
            .expect("member");
        assert!(joiner.will_import && !joiner.is_head);
        assert!(joiner.note.contains("joins the family channel"));
        // The channel name is the same target names.rs would give the head.
        let scoped = crate::names::ScopedChannel {
            id: 101,
            name: "CNN".to_string(),
            number: None,
            group: "Sports".to_string(),
        };
        let identity = crosstalk_matching::identity::resolve_with(
            &stack.engine,
            "CNN",
            &ResolveContext::default(),
        );
        let (name_row, _) = crate::names::decide_heuristic(
            &scoped,
            &identity,
            &stack.engine,
            &stack.index,
            &stack.db_names,
            &test_settings(),
        );
        let expected = if name_row.status == "Renamed" {
            name_row.new
        } else {
            name_row.old
        };
        assert_eq!(cnn.channel_name, expected);
        // And the category is the same categorize gives (News), not a guess.
        assert_eq!(cnn.category, "News");
        assert_eq!(cnn.target_group, "News");
    }

    #[test]
    fn streams_mode_skips_owned_and_foreign_identities() {
        let stack = stack();
        let candidates = vec![candidate(101, "CNN"), candidate(103, "ESPN")];
        // Owned: an earlier run's key is never duplicated (CM duplicated).
        let identity = crosstalk_matching::identity::resolve_with(
            &stack.engine,
            "CNN",
            &ResolveContext::default(),
        );
        let owned: HashMap<String, i64> =
            [(create_owner_key(&identity), 5001)].into_iter().collect();
        let rows = decide_streams(
            &candidates,
            &engine_stack(&stack),
            &test_settings(),
            &owned,
            &[],
        );
        let cnn = rows.iter().find(|row| row.stream_id == 101).expect("CNN");
        assert!(!cnn.will_import);
        assert!(
            cnn.note.contains("already created as channel 5001"),
            "{}",
            cnn.note
        );
        // Foreign: a live channel with the same identity is reported, not created.
        let foreign = vec![(7, "CNN".to_string(), identity)];
        let rows = decide_streams(
            &candidates,
            &engine_stack(&stack),
            &test_settings(),
            &HashMap::new(),
            &foreign,
        );
        let cnn = rows.iter().find(|row| row.stream_id == 101).expect("CNN");
        assert!(!cnn.will_import);
        assert!(
            cnn.note.contains("same identity already exists"),
            "{}",
            cnn.note
        );
        // ESPN is untouched by either.
        let espn = rows.iter().find(|row| row.stream_id == 103).expect("ESPN");
        assert!(espn.will_import);
    }

    #[test]
    fn streams_mode_skips_unmatched_and_filters_categories() {
        let stack = stack();
        let candidates = vec![
            candidate(101, "CNN"),
            candidate(102, "Zzqz No Such Channel 999"),
        ];
        // Unmatched streams are reported, never created (no AI gate in create).
        let rows = decide_streams(
            &candidates,
            &engine_stack(&stack),
            &test_settings(),
            &HashMap::new(),
            &[],
        );
        let unknown = rows
            .iter()
            .find(|row| row.stream_id == 102)
            .expect("unknown");
        assert!(!unknown.will_import);
        assert!(unknown.note.contains("no match"), "{}", unknown.note);
        // The category filter applies after matching, case-insensitively (CM).
        let mut filtered = test_settings();
        filtered.m3u_category_filter = "sports".to_string();
        let rows = decide_streams(
            &candidates,
            &engine_stack(&stack),
            &filtered,
            &HashMap::new(),
            &[],
        );
        let cnn = rows.iter().find(|row| row.stream_id == 101).expect("CNN");
        assert!(!cnn.will_import);
        assert!(cnn.note.contains("not in filter list"), "{}", cnn.note);
        // A custom group rescues a category-less family; without it there is
        // no group to create in.
        let mut custom = test_settings();
        custom.m3u_custom_group_name = "Imports".to_string();
        let rows = decide_streams(
            &candidates,
            &engine_stack(&stack),
            &custom,
            &HashMap::new(),
            &[],
        );
        let unknown = rows
            .iter()
            .find(|row| row.stream_id == 102)
            .expect("unknown");
        assert!(
            !unknown.will_import,
            "unmatched stays unmatched with a custom group"
        );
    }

    #[test]
    fn streams_mode_never_creates_what_names_would_only_tag() {
        // `tag_unmatched_channels` is on by default: names renames an unmatched
        // channel to `<name> [Unk]` and tags a station-less callsign the same
        // way. Create reads the same decision but never makes a channel from it.
        let stack = stack();
        let settings = test_settings();
        assert!(settings.tag_unmatched_channels, "the default tags");
        let candidates = vec![
            candidate(101, "Zzqz No Such Channel 999"),
            candidate(102, "ABC (WZZZ)"),
        ];
        let mut custom = settings.clone();
        custom.m3u_custom_group_name = "Imports".to_string();
        for settings in [&settings, &custom] {
            let rows = decide_streams(
                &candidates,
                &engine_stack(&stack),
                settings,
                &HashMap::new(),
                &[],
            );
            assert!(rows.iter().all(|row| !row.will_import), "{rows:?}");
            assert!(creatable_families(&rows).is_empty());
        }
        let rows = decide_streams(
            &candidates,
            &engine_stack(&stack),
            &settings,
            &HashMap::new(),
            &[],
        );
        let blocked = rows
            .iter()
            .find(|row| row.stream_id == 102)
            .expect("blocked");
        assert!(blocked.note.contains("WZZZ"), "{}", blocked.note);
        assert!(
            blocked.note.contains("never matches premium"),
            "{}",
            blocked.note
        );
    }

    #[test]
    fn streams_mode_skips_a_family_with_no_category_unless_a_custom_group_is_set() {
        // Step-16 decision A: a country label is no category. With no category
        // and no custom import group there is no group to create in, so the
        // family is skipped with that reason; a custom group takes it in.
        let stack = stack();
        let no_categories: HashMap<String, (String, String)> = HashMap::new();
        let bare = EngineStack {
            categories: &no_categories,
            ..engine_stack(&stack)
        };
        let candidates = vec![candidate(101, "CNN")];
        let rows = decide_streams(&candidates, &bare, &test_settings(), &HashMap::new(), &[]);
        assert!(!rows[0].will_import);
        assert!(
            rows[0].note.contains("no group to create in"),
            "{}",
            rows[0].note
        );
        let mut custom = test_settings();
        custom.m3u_custom_group_name = "Imports".to_string();
        let rows = decide_streams(&candidates, &bare, &custom, &HashMap::new(), &[]);
        assert!(rows[0].will_import, "{}", rows[0].note);
        assert_eq!(rows[0].target_group, "Imports");
        assert_eq!(rows[0].category, "");
    }

    #[test]
    fn family_streams_follow_the_streams_quality_order() {
        // The order is the streams app's sort over the pool, not a second rule:
        // with `prioritize_quality` the 1080p60 backup (tier 0) outranks the 720p
        // main (tier 1).
        let mut world = create_world();
        world.streams[0].stream_stats = serde_json::json!({"resolution": "1280x720"});
        world.streams[1].stream_stats =
            serde_json::json!({"resolution": "1920x1080", "source_fps": 60.0});
        let settings = crosstalk_streams::settings::StreamsSettings::default();
        let pool = crosstalk_streams::selection::build(&world, &settings).expect("pool");
        let throughput = crosstalk_streams::quality::MemoryThroughputSource::new();
        let mut model = crosstalk_streams::quality::QualityModel::from_settings(&settings);
        model.prioritize_quality = false;
        let by_priority = quality_order(&pool, &model, &throughput, &[101, 102]);
        model.prioritize_quality = true;
        let by_quality = quality_order(&pool, &model, &throughput, &[101, 102]);
        // Whatever the key, it is the key streams.match ranks on.
        let views: Vec<_> = [101, 102].iter().map(|id| pool.quality_view(*id)).collect();
        let expected: Vec<i64> =
            crosstalk_streams::quality::sort_streams(&views, &model, &throughput)
                .into_iter()
                .map(|view| view.id)
                .collect();
        assert_eq!(by_quality, expected);
        assert_eq!(
            by_quality,
            vec![102, 101],
            "1080p first under prioritize_quality"
        );
        assert_eq!(by_priority.len(), 2);
    }

    #[test]
    fn streams_mode_ota_takes_the_broadcast_category() {
        // Categorize never classifies OTA (no callsign branch); the import path
        // is the one place CM writes `Broadcast`, so create keeps it.
        let stack = stack();
        let candidates = vec![candidate(101, "CBS - AL Birmingham (WIAT)")];
        let rows = decide_streams(
            &candidates,
            &engine_stack(&stack),
            &test_settings(),
            &HashMap::new(),
            &[],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].category, OTA_IMPORT_CATEGORY);
        assert_eq!(rows[0].target_group, OTA_IMPORT_CATEGORY);
        assert!(rows[0].will_import, "{}", rows[0].note);
        assert!(
            rows[0].channel_name.contains("WIAT"),
            "{}",
            rows[0].channel_name
        );
    }

    #[test]
    fn stations_mode_groups_by_station_and_skips() {
        let stack = stack();
        let one = |id: i64, name: &str| StationCandidate {
            id,
            name: name.to_string(),
            group: "US| CBS".to_string(),
            account: "Main".to_string(),
        };
        let candidates = vec![
            one(201, "CBS - AL Birmingham (WIAT)"),
            one(202, "CBS - AL Birmingham (WIAT) HD"),
            one(203, "ESPN HD"),
            one(204, "ABC (WZZZ)"),
        ];
        let rows = decide_stations(
            &candidates,
            &stack.engine,
            &test_settings(),
            &HashSet::new(),
            &HashMap::new(),
        );
        // One station, two streams: one create row (CM's seeder groups the same way).
        let create: Vec<&StationRow> = rows.iter().filter(|row| row.action == "create").collect();
        assert_eq!(create.len(), 1, "{rows:?}");
        assert_eq!(create[0].stream_ids.len(), 2);
        assert!(
            create[0].proposed.contains("WIAT"),
            "{}",
            create[0].proposed
        );
        assert!(
            !create[0].key.is_empty(),
            "a create carries its ownership key"
        );
        // The national names resolve to no station (and a blocked callsign never
        // goes premium): unresolved, never created.
        let unresolved: Vec<&StationRow> = rows
            .iter()
            .filter(|row| row.action == "unresolved")
            .collect();
        assert_eq!(unresolved.len(), 2, "{rows:?}");
        // Existing names skip case-insensitively (the seeder's rule).
        let existing: HashSet<String> = [create[0].proposed.trim().to_lowercase()]
            .into_iter()
            .collect();
        let rerun = decide_stations(
            &candidates,
            &stack.engine,
            &test_settings(),
            &existing,
            &HashMap::new(),
        );
        assert!(
            rerun.iter().filter(|row| row.action == "create").count() == 0,
            "{rerun:?}"
        );
        assert!(
            rerun
                .iter()
                .any(|row| row.action == "skip, name already used"),
            "{rerun:?}"
        );
        // Networks to skip exclude by the station record (WIAT is CBS).
        let mut excluded = test_settings();
        excluded.seed_exclude_networks = "CBS".to_string();
        let rows = decide_stations(
            &candidates,
            &stack.engine,
            &excluded,
            &HashSet::new(),
            &HashMap::new(),
        );
        assert!(
            rows.iter()
                .any(|row| row.action == "skip, network excluded"),
            "{rows:?}"
        );
        assert_eq!(
            rows.iter().filter(|row| row.action == "create").count(),
            0,
            "{rows:?}"
        );
    }

    #[test]
    fn stations_numbers_come_from_the_start_and_skip_used() {
        // `allocate_channel_numbers` walks up from the start skipping used ones;
        // `next_free_numbers` is the same walk, global across groups.
        let world = World {
            channels: vec![
                Channel {
                    id: 1,
                    name: "Old".to_string(),
                    channel_number: Some(serde_json::json!(100.0)),
                    channel_group_id: Some(9),
                    ..Channel::default()
                },
                Channel {
                    id: 2,
                    name: "Other Group".to_string(),
                    channel_number: Some(serde_json::json!(101.0)),
                    channel_group_id: Some(10),
                    ..Channel::default()
                },
            ],
            ..World::default()
        };
        let mut settings = test_settings();
        settings.seed_start_number = "100".to_string();
        let mut rows = vec![StationRow {
            action: "create".to_string(),
            proposed: "CBS - AL Birmingham (WIAT)".to_string(),
            callsign: Some("WIAT".to_string()),
            source_names: vec!["WIAT".to_string()],
            stream_ids: vec![201],
            accounts: vec!["Main".to_string()],
            number: None,
            reason: String::new(),
            key: "lineup:create:station".to_string(),
        }];
        let request = build_stations_request(&world, &mut rows, &settings, "OTA").expect("request");
        // 100 and 101 are taken (in different groups: numbers are global), so
        // the walk lands on 102.
        assert_eq!(rows[0].number, Some(102.0));
        assert_eq!(request.channels[0].number, Some(102.0));
        assert!(
            request.channels[0].streams.is_empty(),
            "no streams attached, as CM"
        );
        // Empty start means above the highest in use.
        settings.seed_start_number = String::new();
        let mut rows2 = rows.clone();
        rows2[0].number = None;
        build_stations_request(&world, &mut rows2, &settings, "OTA").expect("request");
        assert_eq!(rows2[0].number, Some(102.0));
    }

    #[test]
    fn affiliation_networks_read_like_the_station_table() {
        // CM's `station_affiliation` cases, verbatim: structure is dropped,
        // ampersands split unless they ARE the name, N/A names nothing.
        assert_eq!(station_networks("CBS"), vec!["CBS".to_string()]);
        assert_eq!(station_networks("KALB/NBC"), vec!["NBC".to_string()]);
        assert_eq!(
            station_networks("This TV & Start"),
            vec!["THIS TV".to_string(), "START".to_string()]
        );
        assert_eq!(
            station_networks("Heroes & Icons"),
            vec!["HEROES & ICONS".to_string()]
        );
        assert_eq!(station_networks("N/A"), Vec::<String>::new());
        assert_eq!(station_networks(""), Vec::<String>::new());
        assert_eq!(
            station_networks("ABC, Telemundo"),
            vec!["ABC".to_string(), "TELEMUNDO".to_string()]
        );
        assert_eq!(primary_network("KALB/NBC"), Some("NBC".to_string()));
        assert_eq!(primary_network(""), None);
        // The record wins, but a bare stream name still excludes when the record
        // is empty (CM's low-power case).
        let skip: HashSet<String> = ["TELEMUNDO".to_string()].into_iter().collect();
        assert!(station_excluded(Some("Telemundo"), "WNJU", &skip));
        assert!(station_excluded(None, "Telemundo 47", &skip));
        assert!(!station_excluded(Some("CBS"), "WIAT", &skip));
    }

    /// An in-memory [`ChannelCreator`]: ids count up from `next`, and every
    /// payload is recorded for the world the rerun plans against.
    struct FakeCreator {
        next: std::sync::Mutex<i64>,
        groups: std::sync::Mutex<Vec<ChannelGroup>>,
        channels: std::sync::Mutex<Vec<Channel>>,
    }

    impl FakeCreator {
        fn new(next: i64) -> Self {
            Self {
                next: std::sync::Mutex::new(next),
                groups: std::sync::Mutex::new(Vec::new()),
                channels: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn id(&self) -> i64 {
            let mut next = self.next.lock().expect("lock");
            let id = *next;
            *next += 1;
            id
        }
    }

    #[async_trait::async_trait]
    impl ChannelCreator for FakeCreator {
        async fn create_group(&self, name: &str) -> Result<ChannelGroup> {
            let group = ChannelGroup {
                id: self.id(),
                name: name.to_string(),
            };
            self.groups.lock().expect("lock").push(group.clone());
            Ok(group)
        }

        async fn create_logo(&self, name: &str, url: &str) -> Result<Logo> {
            Ok(Logo {
                id: self.id(),
                name: name.to_string(),
                url: url.to_string(),
                ..Logo::default()
            })
        }

        async fn create_channel(&self, channel: &ChannelCreate) -> Result<Channel> {
            let created = Channel {
                id: self.id(),
                name: channel.name.clone(),
                channel_number: channel.channel_number.map(|n| serde_json::json!(n)),
                channel_group_id: channel.channel_group_id,
                logo_id: channel.logo_id,
                streams: channel.streams.clone().unwrap_or_default(),
                ..Channel::default()
            };
            self.channels.lock().expect("lock").push(created.clone());
            Ok(created)
        }

        fn concurrency(&self) -> usize {
            8
        }
    }

    fn stream(id: i64, name: &str, group: Option<i64>, account: Option<i64>) -> Stream {
        Stream {
            id,
            name: name.to_string(),
            m3u_account: account,
            channel_group: group,
            ..Stream::default()
        }
    }

    fn channel(id: i64, name: &str, number: f64, group: Option<i64>) -> Channel {
        Channel {
            id,
            name: name.to_string(),
            channel_number: Some(serde_json::json!(number)),
            channel_group_id: group,
            ..Channel::default()
        }
    }

    fn create_world() -> World {
        World {
            channels: Vec::new(),
            groups: vec![ChannelGroup {
                id: 10,
                name: "Sports".to_string(),
            }],
            streams: vec![
                stream(101, "CNN", Some(10), Some(1)),
                stream(102, "CNN", Some(10), Some(2)),
                stream(103, "ESPN", Some(10), Some(1)),
            ],
            m3u_accounts: vec![
                M3uAccount {
                    id: 1,
                    name: "Main Provider".to_string(),
                    is_active: true,
                    ..M3uAccount::default()
                },
                M3uAccount {
                    id: 2,
                    name: "Backup Provider".to_string(),
                    is_active: true,
                    ..M3uAccount::default()
                },
            ],
            ..World::default()
        }
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
            (
                "/api/channels/streams/",
                serde_json::to_value(&world.streams).expect("streams"),
            ),
            (
                "/api/m3u/accounts/",
                serde_json::to_value(&world.m3u_accounts).expect("accounts"),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(list(items))
                .mount(&server)
                .await;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = CrosstalkConfig::load(dir.path()).expect("config");
        config.dispatcharr.base_url = server.uri();
        config.dispatcharr.api_key = "test-key".to_string();
        (server, config, dir)
    }

    fn context_for(
        config: &CrosstalkConfig,
        store: &Store,
        action: &str,
        dry_run: bool,
    ) -> (JobContext, i64) {
        let run_id = store
            .start_run(crate::APP_ID, action, dry_run)
            .expect("start run");
        let context = JobContext::new(Arc::new(config.clone()), Arc::new(VecSink::new()))
            .with_dispatcharr(
                crosstalk_core::dispatcharr::DispatcharrClient::new(&config.dispatcharr)
                    .expect("client"),
            )
            .for_run(
                crate::APP_ID,
                action,
                run_id,
                store.clone(),
                Arc::new(AtomicBool::new(false)),
            );
        (context, run_id)
    }

    #[tokio::test]
    async fn streams_preview_writes_nothing() {
        let world = create_world();
        let (server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, _) = context_for(&config, &store, crate::ACTION_CREATE_PREVIEW, true);
        let outcome = run(&app, &context, &test_settings(), true)
            .await
            .expect("preview");
        assert!(
            outcome.summary.contains("Create preview (streams)"),
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
        assert!(text.contains("stream_name"), "{text}");
        assert!(text.contains("CNN"), "{text}");
    }

    #[tokio::test]
    async fn streams_apply_creates_one_channel_per_family_then_rerun_creates_nothing() {
        let world = create_world();
        let (server, config, _dir) = serve(&world).await;
        let _ = server;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let creator = Arc::new(FakeCreator::new(5000));
        let (context, run_id) = context_for(&config, &store, crate::ACTION_CREATE_APPLY, false);
        let outcome = run_with_creator(
            &app,
            &context,
            &test_settings(),
            false,
            Some(creator.clone()),
        )
        .await
        .expect("applies");
        assert!(
            outcome.summary.contains("Create apply (streams)"),
            "{}",
            outcome.summary
        );
        // One channel per family, the family's streams attached in the streams
        // app's order (no stats, no probes: M3U priority, then candidate order).
        let channels = creator.channels.lock().expect("lock").clone();
        assert_eq!(channels.len(), 2, "{channels:?}");
        let cnn = channels
            .iter()
            .find(|c| c.streams == vec![101, 102])
            .expect("CNN");
        assert_eq!(cnn.name, "CNN");
        assert_eq!(cnn.channel_number, Some(serde_json::json!(1.0)));
        let espn = channels
            .iter()
            .find(|c| c.streams == vec![103])
            .expect("ESPN");
        assert_eq!(espn.channel_number, Some(serde_json::json!(2.0)));
        // Missing groups are created owned under lineup:create:group:<name>;
        // an existing exact name (Sports, id 10) is reused, never re-posted.
        let groups = creator.groups.lock().expect("lock").clone();
        assert_eq!(groups.len(), 1, "only News is new: {groups:?}");
        assert_eq!(groups[0].name, "News");
        assert_eq!(
            espn.channel_group_id,
            Some(10),
            "ESPN lands in the existing Sports"
        );
        assert_eq!(cnn.channel_group_id, Some(groups[0].id));
        let owned = store.owned_keys(crate::APP_ID).expect("owned");
        assert!(
            owned
                .iter()
                .any(|(kind, _, key)| kind == "channel" && key.starts_with("lineup:create:")),
            "{owned:?}"
        );
        assert!(
            owned
                .iter()
                .any(|(kind, _, key)| kind == "group" && key == "lineup:create:group:News"),
            "{owned:?}"
        );
        // Run rows: one created row per object, so undo lists them.
        let rows = store.rows(run_id).expect("rows");
        assert!(
            rows.iter()
                .any(|row| row.field == "created" && row.channel_name == "CNN"),
            "{rows:?}"
        );
        // Rerun against the world the apply landed: owned keys dedupe, so
        // nothing is created again (CM duplicated here).
        let mut landed = world.clone();
        landed.groups.extend(groups);
        landed.channels.extend(channels);
        let (server2, config2, _dir2) = serve(&landed).await;
        let _ = server2;
        // One guarded run per app at a time: finish the first before the rerun.
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("applied"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let (context2, _) = context_for(&config2, &store, crate::ACTION_CREATE_APPLY, false);
        let before = creator.channels.lock().expect("lock").len();
        let outcome2 = run_with_creator(
            &app,
            &context2,
            &test_settings(),
            false,
            Some(creator.clone()),
        )
        .await
        .expect("reruns");
        assert_eq!(
            creator.channels.lock().expect("lock").len(),
            before,
            "a rerun posts no channel"
        );
        assert!(
            outcome2.summary.contains("created 0 channel(s)"),
            "{}",
            outcome2.summary
        );
    }

    #[tokio::test]
    async fn stations_apply_creates_named_numbered_channels_with_no_streams() {
        let mut world = World {
            channels: vec![channel(1, "Old", 100.0, Some(9))],
            groups: vec![
                ChannelGroup {
                    id: 20,
                    name: "US| CBS".to_string(),
                },
                ChannelGroup {
                    id: 9,
                    name: "Other".to_string(),
                },
            ],
            streams: vec![
                stream(201, "CBS - AL Birmingham (WIAT)", Some(20), Some(1)),
                stream(202, "CBS - AL Birmingham (WIAT) HD", Some(20), Some(1)),
                stream(203, "ESPN HD", Some(20), Some(1)),
            ],
            m3u_accounts: vec![M3uAccount {
                id: 1,
                name: "Main Provider".to_string(),
                is_active: true,
                ..M3uAccount::default()
            }],
            ..World::default()
        };
        // Stream 201 is already attached: only 202 seeds the station.
        world.channels.push(Channel {
            id: 2,
            name: "Holder".to_string(),
            channel_number: Some(serde_json::json!(50.0)),
            channel_group_id: Some(9),
            streams: vec![201],
            ..Channel::default()
        });
        let (server, config, _dir) = serve(&world).await;
        let _ = server;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let creator = Arc::new(FakeCreator::new(5000));
        let mut settings = test_settings();
        settings.create_mode = "stations".to_string();
        settings.seed_source_groups = "US| CBS".to_string();
        settings.seed_target_group = "OTA".to_string();
        settings.seed_start_number = "100".to_string();
        let (context, _) = context_for(&config, &store, crate::ACTION_CREATE_APPLY, false);
        let outcome = run_with_creator(&app, &context, &settings, false, Some(creator.clone()))
            .await
            .expect("applies");
        assert!(
            outcome.summary.contains("Create apply (stations)"),
            "{}",
            outcome.summary
        );
        let channels = creator.channels.lock().expect("lock").clone();
        assert_eq!(channels.len(), 1, "{channels:?}");
        assert!(channels[0].name.contains("WIAT"), "{}", channels[0].name);
        assert_eq!(
            channels[0].channel_group_id,
            Some(5000),
            "the created target group"
        );
        assert_eq!(
            channels[0].channel_number,
            Some(serde_json::json!(101.0)),
            "100 taken"
        );
        assert!(channels[0].streams.is_empty(), "no streams attached, as CM");
        // The missing target group was created owned (CM errored instead).
        let groups = creator.groups.lock().expect("lock").clone();
        assert_eq!(groups.len(), 1, "{groups:?}");
        assert_eq!(groups[0].id, 5000);
        assert_eq!(groups[0].name, "OTA");
        let owned = store.owned_keys(crate::APP_ID).expect("owned");
        assert!(
            owned
                .iter()
                .any(|(kind, _, key)| kind == "group" && key == "lineup:create:group:OTA"),
            "{owned:?}"
        );
    }

    #[tokio::test]
    async fn undo_hides_created_channels_and_lists_created_groups() {
        // The host undo hides created channels (never deletes); created groups
        // are left and listed.
        let world = create_world();
        let (server, config, _dir) = serve(&world).await;
        let _ = server;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let creator = Arc::new(FakeCreator::new(5000));
        let (context, run_id) = context_for(&config, &store, crate::ACTION_CREATE_APPLY, false);
        run_with_creator(
            &app,
            &context,
            &test_settings(),
            false,
            Some(creator.clone()),
        )
        .await
        .expect("applies");
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("created"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        // The world the apply landed: created channels visible, groups present.
        let mut landed = world.clone();
        landed
            .groups
            .extend(creator.groups.lock().expect("lock").clone());
        landed
            .channels
            .extend(creator.channels.lock().expect("lock").clone());
        let app_object: Arc<dyn crosstalk_core::subapp::SubApp> = Arc::new(crate::LineupApp::new());
        let registry = Registry::new(vec![app_object]);
        let plan =
            crosstalk_core::undo::plan_undo(&store, &landed, run_id, &registry).expect("plans");
        let hidden: Vec<i64> = plan
            .changes
            .iter()
            .filter(|change| {
                change.field == Field::HiddenFromOutput && change.reverse == SetValue::Flag(true)
            })
            .map(|change| change.channel_id)
            .collect();
        assert_eq!(hidden.len(), 2, "both created channels hide: {plan:?}");
        assert!(
            plan.left.iter().any(|left| left.kind == "group"),
            "created groups are left and listed: {plan:?}"
        );
    }

    #[test]
    fn intent_recovery_adopts_the_landed_channel() {
        // A crash between the POST and the ownership row leaves an intent; the
        // next run's load_owned adopts the landed object instead of duplicating it.
        let store = Store::in_memory().expect("store");
        let config = CrosstalkConfig::default();
        let run_id = store
            .start_run(crate::APP_ID, crate::ACTION_CREATE_APPLY, false)
            .expect("run");
        let context = JobContext::new(Arc::new(config), Arc::new(VecSink::new())).for_run(
            crate::APP_ID,
            crate::ACTION_CREATE_APPLY,
            run_id,
            store.clone(),
            Arc::new(AtomicBool::new(false)),
        );
        // The key is the family's real key: the reconcile adopts under it and
        // the next plan dedupes on it.
        let engine = UnifiedEngine::decided();
        let identity =
            crosstalk_matching::identity::resolve_with(&engine, "CNN", &ResolveContext::default());
        let key = create_owner_key(&identity);
        store
            .record_intents(&[CreateIntent {
                kind: "channel".to_string(),
                app: crate::APP_ID.to_string(),
                owner_key: key.clone(),
                name: "CNN".to_string(),
                group_id: Some(10),
                number: Some(1.0),
                url: None,
                after_id: 0,
                run_id,
            }])
            .expect("intent");
        let world = World {
            channels: vec![channel(50, "CNN", 1.0, Some(10))],
            groups: vec![ChannelGroup {
                id: 10,
                name: "News".to_string(),
            }],
            ..World::default()
        };
        let owned = channel_ops::load_owned(&context, &world, crate::APP_ID).expect("reconcile");
        assert_eq!(owned.channels.get(&key), Some(&50));
        assert_eq!(owned.recovered.len(), 1);
        // And the adopted key dedupes the next plan.
        let stack = stack();
        let rows = decide_streams(
            &[candidate(101, "CNN")],
            &engine_stack(&stack),
            &test_settings(),
            &owned.channels,
            &[],
        );
        assert!(!rows[0].will_import);
    }

    #[test]
    fn create_names_and_numbers_go_through_the_shared_guards() {
        // The collision guard and the global number check ride every create plan.
        let world = World {
            channels: vec![channel(1, "Twin", 5.0, Some(10))],
            groups: vec![ChannelGroup {
                id: 10,
                name: "Test".to_string(),
            }],
            ..World::default()
        };
        let request = || channel_ops::CreateRequest {
            app: crate::APP_ID.to_string(),
            groups: Vec::new(),
            logos: Vec::new(),
            channels: vec![
                NewChannel {
                    name: "Twin".to_string(),
                    number: Some(5.0),
                    group: None,
                    logo: None,
                    streams: Vec::new(),
                    profile_ids: None,
                    tvg_id: None,
                    epg_data_id: None,
                    key: "lineup:create:a".to_string(),
                },
                NewChannel {
                    name: "Twin".to_string(),
                    number: Some(6.0),
                    group: None,
                    logo: None,
                    streams: Vec::new(),
                    profile_ids: None,
                    tvg_id: None,
                    epg_data_id: None,
                    key: "lineup:create:b".to_string(),
                },
            ],
        };
        let numeric = PlanOptions {
            collision: Some(crosstalk_core::naming::CollisionStrategy::Numeric),
            number_conflicts: true,
            ..PlanOptions::default()
        };
        let plan = channel_ops::plan_creates(&world, &HashMap::new(), &request(), &numeric)
            .expect("plans");
        // "Twin" at 5.0 is taken (name and number, globally): the first create
        // is refused on the number, the second is renamed by the guard.
        assert!(
            plan.skipped
                .iter()
                .any(|skip| skip.name == "Twin" && skip.reason.contains("already used")),
            "{plan:?}"
        );
        assert!(
            plan.channels.iter().any(|channel| channel.name != "Twin"),
            "the surviving twin is renamed: {plan:?}"
        );
        let skip = PlanOptions {
            collision: Some(crosstalk_core::naming::CollisionStrategy::Skip),
            number_conflicts: true,
            ..PlanOptions::default()
        };
        let plan =
            channel_ops::plan_creates(&world, &HashMap::new(), &request(), &skip).expect("plans");
        assert!(
            plan.channels.len() + plan.skipped.len() == 2,
            "every requested object is accounted for: {plan:?}"
        );
    }

    #[test]
    fn created_names_re_resolve_to_the_identity_they_were_made_from() {
        // Guide and streams match on identities: a created channel's name must
        // re-resolve to the identity it was created from (premium: same family;
        // OTA template: the callsign is kept, like names pins).
        let stack = stack();
        let candidates = vec![
            candidate(101, "CNN"),
            candidate(102, "CBS - AL Birmingham (WIAT)"),
        ];
        let rows = decide_streams(
            &candidates,
            &engine_stack(&stack),
            &test_settings(),
            &HashMap::new(),
            &[],
        );
        for row in rows.iter().filter(|row| row.is_head && row.will_import) {
            let again = crosstalk_matching::identity::resolve_with(
                &stack.engine,
                &row.channel_name,
                &ResolveContext::default(),
            );
            if row
                .identity
                .callsign
                .as_deref()
                .is_some_and(crate::names::is_station_callsign)
            {
                assert_eq!(
                    again.callsign, row.identity.callsign,
                    "OTA keeps the callsign: {}",
                    row.channel_name
                );
            } else {
                assert!(
                    again.same_family(&row.identity),
                    "{} re-resolves into its family",
                    row.channel_name
                );
            }
        }
    }
}
