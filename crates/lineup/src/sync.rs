//! `sync_*`: Lineuparr's group + channel sync on the owned-channel rule.
//!
//! Port of `reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/plugin.py`
//! `_sync_groups` (~2877: `_migrate_single_group_name` + get-or-create per category)
//! and `_sync_channels` / `_do_sync_channels` (~2919-3060: get-or-create by
//! `(name, group)`, numbering via `_get_channel_number` /
//! `_build_used_channel_numbers` ~1050-1121, renumber existing,
//! `_enable_channels_in_profiles` ~1612). Group names come from the loader
//! ([`make_group_name`](crate::lineup_file::make_group_name)); numbers from the
//! file (`_parse_channel_number`).
//!
//! # Target algorithm per entry (with LU citations)
//!
//! For every lineup entry in LU's order (folded categories, file order within):
//!
//! - **Groups**: every category's group (`_make_group_name`) goes through the
//!   create stage ([`NewGroup`], exact-name reuse against the world, like
//!   `get_or_create`), owned `lineup:group:<file>:<group>`.
//! - **Owned channels** (an entry whose owner key `lineup:<file>:<name>` is live
//!   in the store): the channel is moved to the entry's group when it differs
//!   (what a `category_detail` change looks like: the key carries no category,
//!   so the same channel moves instead of being re-created), and — in
//!   `lineup` numbering mode only — renumbered when the file's number differs.
//!   Renames never happen here: `names_apply` owns names, and a rename would
//!   fight it every run (documented, not a parity gap: LU renamed nothing
//!   either, it matched by name).
//! - **New channels**: a foreign channel with the same name in the target group
//!   is reported and skipped (LU's `get_or_create(name, group)` ADOPTED it —
//!   the documented fix: only channels this sync owns are ever adopted, so a
//!   same-name clash never silently gains streams/a number/a group). Otherwise
//!   the channel is created with the lineup entry's name (the canonical lineup
//!   name, NOT `names_apply`'s template: sync mirrors the provider file, names
//!   canonicalizes what Dispatcharr shows — the two disagree on purpose and the
//!   oracle pins the boundary), the target group, no streams (streams attach in
//!   step 19's `streams.match` lineup mode), and profile membership via
//!   `channel_profile_ids` at create.
//! - **Numbers**: `lineup` takes the file's number (`None` stays numberless,
//!   like LU's empty `defaults`); the two auto modes assign ONLY new channels,
//!   via the shared global [`next_free_numbers`](crosstalk_core::channel_ops::next_free_numbers)
//!   (`auto_next` from 1, `auto_highest` above the highest number in use).
//!   `specific` counts up from `starting_channel_number`, skipping used. Every
//!   assignment is globally checked (step 10 review): a refused number keeps
//!   its old number and gains a skip row naming the holder.
//! - **Duplicates within a lineup** (26 cases across the shipped files: the
//!   same name in two categories): one owned channel per `(file, name)` — the
//!   first category in LU's order wins. LU made one channel per `(name, group)`.
//! - **Profiles**: resolved by name, case-insensitively, like
//!   `_resolve_channel_profiles`; unknown names warn and are skipped. Existing
//!   owned channels never gain membership rows (phase-2 rule): LU enabled every
//!   synced channel, crosstalk only sets `channel_profile_ids` on the create.
//! - **`_migrate_single_group_name`** (~2834): the pre-fix `"<prefix>: All"`
//!   group is renamed to the prefix-only name — owned groups only (LU renamed
//!   any such group; a foreign `"<prefix>: All"` is left alone and reported).
//!
//! # Fixes over LU (strictly safer, documented)
//!
//! Owned channels keep their number on re-sync (LU renumbered every channel on
//! every rerun in the auto modes: the assigner walks every entry and writes
//! whatever it lands on, so inserting one entry shifts every later channel).
//! Auto modes here assign only new channels, so a rerun — even after the world
//! changed — is a no-op. Only owned channels are adopted by `(name, group)`; a
//! foreign clash is reported and skipped. Deletes are `tidy`'s hide, never a
//! sync write.
//!
//! # Planning, CSV, undo
//!
//! `sync_preview` (Read) / `sync_apply` (Write, confirm) plan from LIVE state:
//! groups through [`plan_creates`](crosstalk_core::channel_ops::plan_creates)
//! (intents + [`load_owned`](crosstalk_core::channel_ops::load_owned) reconciled
//! first), renumbers and moves through [`Op::Set`] `Number` / `Group` per
//! channel, the migration as one group rename. Apply order: migrate, groups,
//! channels, edits — recorded as run rows (create rows first, edit rows shifted
//! past them, the migration row first of all) for the host undo: created
//! channels hide on undo, renumbers/moves reverse, created groups are left and
//! listed, the group rename is reported but not reversed (undo only reverses
//! channel writes).

use anyhow::{bail, Result};
use crosstalk_core::channel_ops::{
    self, ChannelCreator, CreateRequest, Field, GroupRef, NewChannel, NewGroup, Op, PlanOptions,
    RestChannelCreator, Selection, SetValue,
};
use crosstalk_core::config::ScopeConfig;
use crosstalk_core::csv_report::{self, Report};
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, World, WorldKind, WorldKinds};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::store::{RunRow, Subject};
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use crosstalk_core::wildcard::parse_tokens;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The sync CSV columns: the entry, where it lands, and what the run did.
pub const COLUMNS_SYNC: [&str; 9] = [
    "id",
    "channel_name",
    "category",
    "target_group",
    "group_exists",
    "channel_number",
    "status",
    "reason",
    "country",
];

/// The ownership key of a group sync creates: `lineup:group:<file>:<group>`,
/// where `<file>` is the full embedded filename (like the named scope's
/// prefix, so switching `lineup_file` spellings never re-keys).
pub fn group_owner_key(file: &str, group: &str) -> String {
    format!("{}group:{file}:{group}", crate::OWNER_KEY_PREFIX)
}

/// The ownership key of a channel sync owns: `lineup:<file>:<name>`.
///
/// The tail after `lineup:<file>:` is the lineup ENTRY name, never the folded
/// category (step 13 review): changing `category_detail` moves the owned
/// channel to its new group instead of re-keying (and re-creating) it.
pub fn channel_owner_key(file: &str, name: &str) -> String {
    match crate::owner_key_prefix(file) {
        Ok(prefix) => format!("{prefix}{name}"),
        Err(_) => format!("{}{file}:{name}", crate::OWNER_KEY_PREFIX),
    }
}

/// The prefix group ownership keys of one file live under:
/// `lineup:group:<file>:`.
pub fn group_owner_prefix(file: &str) -> String {
    format!("{}group:{file}:", crate::OWNER_KEY_PREFIX)
}

/// What one lineup entry becomes: the pure decision `decide_sync` makes and the
/// action (and the oracle dump) reports.
#[derive(Debug, Clone, PartialEq)]
pub enum SyncAction {
    /// A new channel will be created (number already assigned per the mode).
    Create,
    /// The owned channel already matches the entry: nothing to do.
    Keep,
    /// The owned channel keeps its group but takes the file's number
    /// (`lineup` mode only).
    Renumber,
    /// The owned channel keeps its number but moves to the entry's group.
    Move,
    /// Both (a `category_detail` change that also re-numbers in `lineup` mode).
    MoveRenumber,
    /// No write: the reason says why (foreign clash, duplicate, refused).
    Skipped,
}

/// One entry's sync decision.
#[derive(Debug, Clone)]
pub struct SyncRow {
    pub name: String,
    /// The number the channel has after the run: the file's (`lineup`), the
    /// assigned (`auto_*`, `specific`), or the kept one (owned).
    pub number: Option<f64>,
    pub category: String,
    pub group: String,
    pub country: Option<String>,
    /// The live id of the owned channel, when the entry owns one.
    pub channel_id: Option<i64>,
    pub current_group: Option<String>,
    pub current_number: Option<f64>,
    pub action: SyncAction,
    pub reason: String,
}

/// The pure sync plan: groups in file order plus one row per entry.
#[derive(Debug, Clone)]
pub struct SyncDecisions {
    pub file: String,
    pub groups: Vec<NewGroup>,
    pub rows: Vec<SyncRow>,
}

/// Decide every entry of a loaded lineup against a live world.
///
/// Pure: nothing is written. `owned` maps this sync's channel owner keys
/// (`lineup:<file>:<name>`) to live channel ids
/// ([`load_owned`](crosstalk_core::channel_ops::load_owned) reconciles intents
/// and forgets deleted channels before this runs). Returns one row per entry in
/// LU's order (folded categories, file order within each).
pub fn decide_sync(
    world: &World,
    lineup: &crate::lineup_file::Lineup,
    settings: &crate::settings::LineupSettings,
    owned: &HashMap<String, i64>,
) -> Result<SyncDecisions> {
    let mode = settings.channel_numbering.trim();
    if !crate::lineup_file::NUMBERING_MODES.contains(&mode) {
        bail!(
            "channel_numbering must be one of {}, got {mode:?}",
            crate::lineup_file::NUMBERING_MODES.join(", ")
        );
    }
    // One owned channel per (file, name): the first category in LU's order
    // wins; a later same-named entry is a reported skip, never a second channel.
    let mut seen: HashSet<&str> = HashSet::new();
    let mut rows: Vec<SyncRow> = Vec::with_capacity(lineup.entries.len());
    for entry in &lineup.entries {
        let base = SyncRow {
            name: entry.name.clone(),
            number: entry.number.map(|number| number as f64),
            category: entry.category.clone(),
            group: entry.group.clone(),
            country: entry.country.clone(),
            channel_id: None,
            current_group: None,
            current_number: None,
            action: SyncAction::Create,
            reason: String::new(),
        };
        if !seen.insert(entry.name.as_str()) {
            rows.push(SyncRow {
                number: None,
                action: SyncAction::Skipped,
                reason: format!(
                    "the lineup names {:?} twice (here in {:?}); one owned channel per \
                     (file, name): the first category wins, so this entry is skipped",
                    entry.name, entry.category,
                ),
                ..base
            });
            continue;
        }
        let key = channel_owner_key(&lineup.file_name, &entry.name);
        match owned.get(&key) {
            Some(id) => {
                let channel = world.channels.iter().find(|channel| channel.id == *id);
                match channel {
                    None => rows.push(SyncRow {
                        number: None,
                        action: SyncAction::Skipped,
                        reason: format!(
                            "key {key:?} was owned as channel {id} but it is gone from \
                             Dispatcharr; it will be created again next run"
                        ),
                        ..base
                    }),
                    Some(channel) => {
                        let current_group = world.group_name(channel.channel_group_id);
                        let current_number = channel.display_number();
                        // Renumbers happen only here, only in `lineup` mode, only for
                        // owned channels: the auto modes assign new channels only
                        // (LU renumbered every channel on every rerun).
                        let renumber = mode == "lineup"
                            && base.number.is_some()
                            && current_number != base.number;
                        let moves = current_group != entry.group;
                        let (action, reason) = match (renumber, moves) {
                            (false, false) => (
                                SyncAction::Keep,
                                format!(
                                    "channel {id} already matches the lineup entry \
                                     (rename never: names_apply owns names)"
                                ),
                            ),
                            (true, false) => (
                                SyncAction::Renumber,
                                format!(
                                    "the lineup number changed to {} (was {})",
                                    format_number(&base.number),
                                    format_number(&current_number),
                                ),
                            ),
                            (false, true) => (
                                SyncAction::Move,
                                format!(
                                    "moves from {current_group:?} to {:?} (the folded \
                                     category changed; the owner key carries no category)",
                                    entry.group,
                                ),
                            ),
                            (true, true) => (
                                SyncAction::MoveRenumber,
                                format!(
                                    "moves from {current_group:?} to {:?} and takes the \
                                     lineup number {} (was {})",
                                    entry.group,
                                    format_number(&base.number),
                                    format_number(&current_number),
                                ),
                            ),
                        };
                        rows.push(SyncRow {
                            number: if renumber {
                                base.number
                            } else {
                                current_number
                            },
                            channel_id: Some(*id),
                            current_group: Some(current_group),
                            current_number,
                            action,
                            reason,
                            ..base
                        });
                    }
                }
            }
            None => {
                // LU's get_or_create(name, group) adopted whatever it found here;
                // only owned channels are adopted, so a foreign same-named
                // channel in the target group is a reported skip, never a take.
                let clash = world.channels.iter().find(|channel| {
                    channel.name == entry.name
                        && world.group_name(channel.channel_group_id) == entry.group
                });
                match clash {
                    Some(channel) => rows.push(SyncRow {
                        number: None,
                        action: SyncAction::Skipped,
                        reason: format!(
                            "channel {} ({:?}) already has this name in {:?}: it is \
                             foreign, so it is reported and skipped rather than adopted",
                            channel.id, channel.name, entry.group,
                        ),
                        ..base
                    }),
                    None => rows.push(base),
                }
            }
        }
    }
    // Numbers for the creates the modes assign (owned channels never change
    // here): one global walk in row order, so a used number is never handed out.
    if mode != "lineup" {
        let start = match mode {
            "auto_next" => 1.0,
            "auto_highest" => world
                .channels
                .iter()
                .filter_map(|channel| channel.display_number())
                .fold(None::<f64>, |max, number| {
                    Some(max.map_or(number, |m: f64| m.max(number)))
                })
                .map_or(1.0, |max| max + 1.0),
            _ => specific_start(settings)?,
        };
        let need = rows
            .iter()
            .filter(|row| row.action == SyncAction::Create)
            .count();
        let mut free = channel_ops::next_free_numbers(world, start, need).into_iter();
        for row in rows
            .iter_mut()
            .filter(|row| row.action == SyncAction::Create)
        {
            row.number = free.next();
        }
    }
    // Groups in first-seen order (stable: the same world plans the same groups).
    let mut groups: Vec<NewGroup> = Vec::new();
    let mut seen_groups: HashSet<&str> = HashSet::new();
    for row in &rows {
        if seen_groups.insert(row.group.as_str()) {
            groups.push(NewGroup {
                name: row.group.clone(),
                key: group_owner_key(&lineup.file_name, &row.group),
            });
        }
    }
    Ok(SyncDecisions {
        file: lineup.file_name.clone(),
        groups,
        rows,
    })
}

/// The first number for `specific` numbering.
fn specific_start(settings: &crate::settings::LineupSettings) -> Result<f64> {
    let raw = settings.starting_channel_number.trim();
    match raw.parse::<f64>() {
        Ok(number) if number > 0.0 => Ok(number),
        _ => bail!(
            "starting_channel_number must be a positive number when channel_numbering is \
             \"specific\", got {:?}",
            settings.starting_channel_number,
        ),
    }
}

fn format_number(number: &Option<f64>) -> String {
    number.map(|number| format!("{number}")).unwrap_or_default()
}

/// The `channel_profile_ids` a create carries, resolved case-insensitively like
/// `_resolve_channel_profiles` (unknown names warn and are skipped).
///
/// `None` is never answered: Dispatcharr reads an omitted `channel_profile_ids`
/// as "every profile" (`ChannelCreate`), so an unconfigured sync sends `[]`
/// (none, like LU's ORM create with no profiles) rather than omitting it.
pub fn resolve_profile_ids(
    world: &World,
    settings: &crate::settings::LineupSettings,
) -> (Option<Vec<i64>>, Vec<String>) {
    let tokens = parse_tokens(&settings.channel_profiles);
    if tokens.is_empty() {
        return (Some(Vec::new()), Vec::new());
    }
    let mut ids: Vec<i64> = Vec::new();
    let mut unknown: Vec<String> = Vec::new();
    for token in tokens {
        match world
            .profiles
            .iter()
            .find(|profile| profile.name.to_lowercase() == token.to_lowercase())
        {
            Some(profile) if !ids.contains(&profile.id) => ids.push(profile.id),
            Some(_) => {}
            None => unknown.push(token),
        }
    }
    for name in &unknown {
        tracing::warn!("channel profile {name:?} not found: it is skipped");
    }
    (Some(ids), unknown)
}

/// Build the create request for the decisions: missing groups plus one channel
/// per `Create` row, in row order.
pub fn build_create_request(
    decisions: &SyncDecisions,
    profile_ids: Option<Vec<i64>>,
) -> CreateRequest {
    let group_index: HashMap<&str, usize> = decisions
        .groups
        .iter()
        .enumerate()
        .map(|(index, group)| (group.name.as_str(), index))
        .collect();
    let channels: Vec<NewChannel> = decisions
        .rows
        .iter()
        .filter(|row| row.action == SyncAction::Create)
        .map(|row| NewChannel {
            name: row.name.clone(),
            number: row.number,
            group: group_index
                .get(row.group.as_str())
                .map(|index| GroupRef::Planned(*index)),
            logo: None,
            // No streams: step 19's `streams.match` lineup mode attaches them.
            streams: Vec::new(),
            profile_ids: profile_ids.clone(),
            tvg_id: None,
            epg_data_id: None,
            key: channel_owner_key(&decisions.file, &row.name),
        })
        .collect();
    CreateRequest {
        app: crate::APP_ID.to_string(),
        groups: decisions.groups.clone(),
        logos: Vec::new(),
        channels,
    }
}

/// Plan the decided creates through the shared create stage
/// ([`plan_creates`](crosstalk_core::channel_ops::plan_creates): exact-name
/// group reuse, owned-key dedupe, the global number check) and turn every
/// refused create into a skip on its row — the one planning step the action
/// and the oracle dump share.
pub fn plan_sync_creates(
    world: &World,
    owned: &HashMap<String, i64>,
    decisions: &mut SyncDecisions,
    profile_ids: Option<Vec<i64>>,
) -> Result<channel_ops::CreatePlan> {
    let request = build_create_request(decisions, profile_ids);
    let plan = channel_ops::plan_creates(
        world,
        owned,
        &request,
        &PlanOptions {
            number_conflicts: true,
            ..PlanOptions::default()
        },
    )?;
    // A create the number check refused is a skip on its row, not a silent drop.
    let refused: HashMap<&str, &str> = plan
        .skipped
        .iter()
        .map(|skip| (skip.key.as_str(), skip.reason.as_str()))
        .collect();
    for row in decisions.rows.iter_mut() {
        if row.action == SyncAction::Create {
            let key = channel_owner_key(&decisions.file, &row.name);
            if let Some(reason) = refused.get(key.as_str()) {
                row.action = SyncAction::Skipped;
                row.reason = (*reason).to_string();
                row.number = None;
            }
        }
    }
    Ok(plan)
}

/// The per-channel edit values for the decided renumbers and moves.
///
/// Returns the `Number` values, the `Group` values (against `group_ids`), and
/// the rows whose target group has no id (a refused group create): those stay
/// behind with the reason instead of planning a move to nowhere.
#[allow(clippy::type_complexity)]
pub fn edit_values(
    decisions: &SyncDecisions,
    group_ids: &HashMap<String, i64>,
) -> (
    HashMap<i64, SetValue>,
    HashMap<i64, SetValue>,
    Vec<(i64, String)>,
) {
    let mut numbers: HashMap<i64, SetValue> = HashMap::new();
    let mut groups: HashMap<i64, SetValue> = HashMap::new();
    let mut refused: Vec<(i64, String)> = Vec::new();
    for row in &decisions.rows {
        let Some(id) = row.channel_id else {
            continue;
        };
        match row.action {
            SyncAction::Renumber | SyncAction::MoveRenumber => {
                numbers.insert(id, SetValue::Number(row.number));
            }
            SyncAction::Create | SyncAction::Keep | SyncAction::Move | SyncAction::Skipped => {}
        }
        match row.action {
            SyncAction::Move | SyncAction::MoveRenumber => match group_ids.get(&row.group) {
                Some(group_id) => {
                    groups.insert(id, SetValue::Group(Some(*group_id)));
                }
                None => refused.push((
                    id,
                    format!(
                        "group {:?} could not be created, so the channel stays where it is",
                        row.group,
                    ),
                )),
            },
            SyncAction::Create | SyncAction::Keep | SyncAction::Renumber | SyncAction::Skipped => {}
        }
    }
    (numbers, groups, refused)
}

/// The legacy `"<prefix>: All"` / prefix-only rename
/// (`_migrate_single_group_name`, `plugin.py:2834`): the old and new names.
pub fn migration_names(prefix: &str) -> (String, String) {
    // The plugin's rule verbatim: a trailing separator (colon, underscore, dash,
    // slash, SPACE) appends directly, otherwise `": "` joins; no prefix at all
    // is the bare sentinel.
    let old = if prefix.is_empty() {
        crate::lineup_file::SINGLE_GROUP_CATEGORY.to_string()
    } else if matches!(prefix.chars().last(), Some(':' | '_' | '-' | '/' | ' ')) {
        format!("{prefix}{}", crate::lineup_file::SINGLE_GROUP_CATEGORY)
    } else {
        format!("{prefix}: {}", crate::lineup_file::SINGLE_GROUP_CATEGORY)
    };
    let new =
        crate::lineup_file::make_group_name(prefix, crate::lineup_file::SINGLE_GROUP_CATEGORY);
    (old, new)
}

/// What the migration does: rename the pre-fix group, or leave it alone.
#[derive(Debug, Clone, PartialEq)]
pub enum MigrationAction {
    /// No pre-fix group to rename (or the names already agree).
    None,
    /// Rename the owned pre-fix group to the prefix-only name.
    Rename,
    /// Leave it alone: the reason says why (both names exist, or foreign).
    Skipped,
}

/// One migration decision: the names, the action and the reason.
#[derive(Debug, Clone)]
pub struct Migration {
    pub old: String,
    pub new: String,
    /// The live id of the pre-fix group, when there is one.
    pub id: Option<i64>,
    pub action: MigrationAction,
    pub reason: String,
}

/// Decide the legacy group rename: owned groups only.
///
/// LU renamed any `"<prefix>: All"` group it found; crosstalk renames only a
/// group this sync owns (under `lineup:group:<file>:<old>`): a foreign group
/// with the old name is left alone and reported, never renamed.
///
/// `owned_groups` are the live group ids owned under this file's
/// [`group_owner_prefix`]: the caller narrows them, so the decision never sees
/// another lineup's groups.
pub fn decide_migration(world: &World, prefix: &str, owned_groups: &HashSet<i64>) -> Migration {
    let (old, new) = migration_names(prefix);
    let base = Migration {
        old: old.clone(),
        new: new.clone(),
        id: None,
        action: MigrationAction::None,
        reason: format!("no group {old:?} exists: nothing to migrate"),
    };
    if old == new {
        return Migration {
            reason: format!(
                "no prefix: {old:?} already is the prefix-only name, so nothing migrates"
            ),
            ..base
        };
    }
    let Some(group) = world.groups.iter().find(|group| group.name == old) else {
        return base;
    };
    let both = world.groups.iter().any(|known| known.name == new);
    if both {
        return Migration {
            id: Some(group.id),
            action: MigrationAction::Skipped,
            reason: format!(
                "both {old:?} and {new:?} exist: merging them would move channels, so both \
                 are left as-is (merge them by hand)"
            ),
            ..base
        };
    }
    if !owned_groups.contains(&group.id) {
        return Migration {
            id: Some(group.id),
            action: MigrationAction::Skipped,
            reason: format!(
                "group {old:?} (id {}) is foreign: the migration renames owned groups only, \
                 so it is left alone",
                group.id,
            ),
            ..base
        };
    }
    Migration {
        id: Some(group.id),
        action: MigrationAction::Rename,
        reason: format!(
            "renames the owned pre-fix group {old:?} to {new:?} (channels stay assigned: \
             only the name changes)"
        ),
        ..base
    }
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

/// Run `sync_preview` / `sync_apply`.
///
/// Preview plans and writes the CSV; nothing is written to Dispatcharr. Apply
/// migrates the legacy group name, creates the missing groups and channels,
/// then renumbers/moves the owned channels that drifted, recorded as run rows
/// for the host undo.
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
        crate::ACTION_SYNC_PREVIEW
    } else {
        crate::ACTION_SYNC_APPLY
    };
    let client = context.dispatcharr()?;
    let world: World = client
        .load_world_with(
            WorldKinds::only(WorldKind::Channels) | WorldKind::Groups | WorldKind::Profiles,
        )
        .await?;
    let config = context.config();
    let base = context.base_selection(&world)?;

    let lineup = crate::lineup_file::load_lineup(
        &settings.lineup_file,
        &settings.category_detail,
        &settings.group_prefix,
    )?;

    // Crash-safe creates (intents + ownership, step 10 review): reconcile before
    // planning so an earlier run's landed channel is reused, never duplicated.
    let owned = channel_ops::load_owned(context, &world, crate::APP_ID, dry_run).await?;
    let owned_groups: HashSet<i64> = match context.store() {
        Some(store) => store
            .owned_keys(crate::APP_ID)?
            .into_iter()
            .filter(|(kind, _, key)| {
                kind == "group" && key.starts_with(&group_owner_prefix(&lineup.file_name))
            })
            .map(|(_, id, _)| id)
            .collect(),
        None => HashSet::new(),
    };

    // The legacy rename is decided first: everything below plans against the
    // renamed world (the write itself waits for the ignore check).
    let migration = if lineup
        .categories
        .iter()
        .any(|category| category == crate::lineup_file::SINGLE_GROUP_CATEGORY)
    {
        decide_migration(&world, &lineup.prefix, &owned_groups)
    } else {
        let (old, new) = migration_names(&lineup.prefix);
        Migration {
            old,
            new,
            id: None,
            action: MigrationAction::None,
            reason: "category_detail is not \"none\": nothing to migrate".to_string(),
        }
    };
    let mut planned_world = world.clone();
    if migration.action == MigrationAction::Rename {
        if let Some(id) = migration.id {
            if let Some(group) = planned_world.groups.iter_mut().find(|group| group.id == id) {
                group.name = migration.new.clone();
            }
        }
    }
    let mut decisions = decide_sync(&planned_world, &lineup, settings, &owned.channels)?;

    // Profiles resolve case-insensitively (LU); unknown names warn and are
    // skipped. Existing owned channels never gain rows: only creates carry ids.
    let (profile_ids, unknown_profiles) = resolve_profile_ids(&planned_world, settings);
    let profile_note = if unknown_profiles.is_empty() {
        String::new()
    } else {
        format!(
            " Unknown channel profile(s) skipped: {}.",
            unknown_profiles.join(", ")
        )
    };

    // A target group the operator ignored is refused before anything is
    // created, naming the groups rather than creating half the plan.
    let mut targets: Vec<String> = Vec::new();
    for group in &decisions.groups {
        if !targets.contains(&group.name) {
            targets.push(group.name.clone());
        }
    }
    let blocked: Vec<String> = targets
        .into_iter()
        .filter(|name| crate::categorize::is_ignored_target(name, &base.ignore_groups))
        .collect();
    if !blocked.is_empty() {
        bail!(
            "Sync would create or write into group(s) listed in 'Channel Groups to \
             Ignore': {}. Change the lineup's groups or remove them from the ignore list.",
            blocked.join(", ")
        );
    }

    // The legacy rename runs only once the plan is known to go ahead: a refused
    // run (ignored target groups above) writes nothing at all.
    let migration_row = |status: &str, reason: String| RunRow {
        subject: Subject::Group,
        channel_id: migration.id.unwrap_or(0),
        channel_name: migration.new.clone(),
        channel_group: migration.new.clone(),
        channel_number: None,
        method: "sync".to_string(),
        field: "name".to_string(),
        before_value: Some(migration.old.clone()),
        after_value: Some(migration.new.clone()),
        status: status.to_string(),
        reason,
        owner_key: Some(group_owner_key(&lineup.file_name, &migration.new)),
        ..RunRow::default()
    };
    let mut migration_rows: Vec<RunRow> = Vec::new();
    let mut migration_note = String::new();
    match migration.action {
        MigrationAction::None => {
            migration_note = format!("Group migration: {}.", migration.reason);
        }
        MigrationAction::Skipped => {
            migration_note = format!("Group migration skipped: {}.", migration.reason);
            migration_rows.push(migration_row(
                channel_ops::RowStatus::Planned.as_str(),
                migration.reason.clone(),
            ));
        }
        MigrationAction::Rename => {
            if dry_run {
                migration_note = format!("Group migration: {}.", migration.reason);
                migration_rows.push(migration_row(
                    channel_ops::RowStatus::Planned.as_str(),
                    migration.reason.clone(),
                ));
            } else if let Some(id) = migration.id {
                match client.rename_group(id, &migration.new).await {
                    Ok(_) => {
                        migration_note = format!(
                            "Group migration: renamed {:?} to {:?}.",
                            migration.old, migration.new
                        );
                        migration_rows.push(migration_row(
                            channel_ops::RowStatus::Ok.as_str(),
                            migration.reason.clone(),
                        ));
                        // Ownership is by id, so the renamed group stays owned
                        // (its key keeps the name it was created under).
                    }
                    Err(error) => {
                        migration_note =
                            format!("Group migration failed: {error:#}; the sync carries on.");
                        migration_rows.push(migration_row(
                            channel_ops::RowStatus::Failed.as_str(),
                            format!("{error:#}"),
                        ));
                    }
                }
            }
        }
    }

    let existing: HashSet<String> = planned_world
        .groups
        .iter()
        .map(|group| group.name.clone())
        .collect();
    let create_plan =
        plan_sync_creates(&planned_world, &owned.channels, &mut decisions, profile_ids)?;

    let creator: Arc<dyn ChannelCreator> = match creator {
        Some(creator) => creator,
        None => {
            let client = context.dispatcharr()?;
            Arc::new(RestChannelCreator::new(
                client.clone(),
                config.dispatcharr.concurrency,
            ))
        }
    };
    let create_report = if dry_run {
        None
    } else {
        Some(channel_ops::apply_creates(creator, &create_plan, context, action_id).await)
    };

    // The Set plan runs against the world augmented with the groups this run
    // just created (same names, real ids) — the channels themselves are still
    // the live state the preview read.
    let mut edited_world = planned_world.clone();
    if let Some(report) = create_report.as_ref() {
        for (group, outcome) in create_plan.groups.iter().zip(report.groups.iter()) {
            if let Some(id) = outcome.id {
                if group.existing_id.is_none()
                    && !edited_world.groups.iter().any(|known| known.id == id)
                {
                    edited_world.groups.push(ChannelGroup {
                        id,
                        name: group.name.clone(),
                    });
                }
            }
        }
    }
    // The channels just created hold their numbers too: a renumber below must
    // not land on one (the global number check reads this world).
    if let Some(report) = create_report.as_ref() {
        for (planned, outcome) in create_plan.channels.iter().zip(report.channels.iter()) {
            if let Some(id) = outcome.id {
                edited_world.channels.push(Channel {
                    id,
                    name: planned.name.clone(),
                    channel_number: planned.number.map(|number| serde_json::json!(number)),
                    ..Channel::default()
                });
            }
        }
    }
    let group_ids: HashMap<String, i64> = edited_world
        .groups
        .iter()
        .map(|group| (group.name.clone(), group.id))
        .collect();
    let (number_values, group_values, refused_moves) = edit_values(&decisions, &group_ids);
    let refused: HashMap<i64, &str> = refused_moves
        .iter()
        .map(|(id, reason)| (*id, reason.as_str()))
        .collect();
    let touched: Vec<i64> = decisions
        .rows
        .iter()
        .filter_map(|row| row.channel_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    // The id bound is the rail: host `[scope]` never silently narrows a sync,
    // because a skipped owned channel would orphan it from its lineup.
    let mut selection = Selection::in_scope(&ScopeConfig::default());
    selection.channel_ids = Some(touched);
    let mut ops: Vec<Op> = Vec::new();
    if !number_values.is_empty() {
        ops.push(Op::Set {
            field: Field::Number,
            values: number_values,
        });
    }
    if !group_values.is_empty() {
        ops.push(Op::Set {
            field: Field::Group,
            values: group_values,
        });
    }
    let edit_plan = if ops.is_empty() {
        channel_ops::Plan::default()
    } else {
        channel_ops::plan_with_options(
            &edited_world,
            &selection,
            &ops,
            &PlanOptions {
                number_conflicts: true,
                ..PlanOptions::default()
            },
        )?
    };
    let applied = if dry_run {
        None
    } else {
        Some(channel_ops::apply(client, &edit_plan, context, action_id).await)
    };

    // Final per-row outcomes: the decisions plus what the plans did.
    let created_outcome: HashMap<&str, &channel_ops::CreatedOutcome> = match create_report.as_ref()
    {
        Some(report) => create_plan
            .channels
            .iter()
            .zip(report.channels.iter())
            .map(|(planned, outcome)| (planned.key.as_str(), outcome))
            .collect(),
        None => HashMap::new(),
    };
    let edited: HashMap<i64, &channel_ops::Change> = edit_plan
        .changes
        .iter()
        .map(|change| (change.channel_id, change))
        .collect();
    let edit_skipped: HashMap<i64, &str> = edit_plan
        .skipped
        .iter()
        .map(|skip| (skip.channel_id, skip.reason.as_str()))
        .collect();
    let mut outputs: Vec<SyncOutput> = Vec::with_capacity(decisions.rows.len());
    for row in &decisions.rows {
        let group_exists = if existing.contains(&row.group) {
            "Yes"
        } else {
            "No (will be created)"
        };
        let (id, number, status, reason) = match row.action {
            SyncAction::Create => {
                let key = channel_owner_key(&decisions.file, &row.name);
                let outcome = created_outcome.get(key.as_str()).copied();
                let created = outcome.and_then(|outcome| outcome.id);
                if dry_run {
                    (
                        String::new(),
                        format_number(&row.number),
                        "planned".to_string(),
                        format!(
                            "will create {:?} in {:?}{}",
                            row.name,
                            row.group,
                            match row.number {
                                Some(number) => format!(" at {number}"),
                                None => " (no lineup number)".to_string(),
                            },
                        ),
                    )
                } else {
                    match outcome {
                        Some(outcome)
                            if outcome.status == channel_ops::RowStatus::Ok
                                && created.is_some() =>
                        {
                            (
                                created.map(|id| id.to_string()).unwrap_or_default(),
                                format_number(&row.number),
                                "create".to_string(),
                                format!(
                                    "created {:?} in {:?} at {}",
                                    row.name,
                                    row.group,
                                    format_number(&row.number),
                                ),
                            )
                        }
                        Some(outcome) => (
                            String::new(),
                            format_number(&row.number),
                            "failed".to_string(),
                            if outcome.reason.is_empty() {
                                "the create failed".to_string()
                            } else {
                                outcome.reason.clone()
                            },
                        ),
                        None => (
                            String::new(),
                            format_number(&row.number),
                            "failed".to_string(),
                            "the create was not attempted".to_string(),
                        ),
                    }
                }
            }
            SyncAction::Keep => (
                row.channel_id.map(|id| id.to_string()).unwrap_or_default(),
                format_number(&row.current_number),
                "skipped".to_string(),
                row.reason.clone(),
            ),
            SyncAction::Renumber | SyncAction::Move | SyncAction::MoveRenumber => {
                let id = row.channel_id.unwrap_or(0);
                if let Some(reason) = refused.get(&id) {
                    (
                        id.to_string(),
                        format_number(&row.current_number),
                        "failed".to_string(),
                        (*reason).to_string(),
                    )
                } else if dry_run {
                    (
                        id.to_string(),
                        format_number(&row.number),
                        "planned".to_string(),
                        row.reason.clone(),
                    )
                } else {
                    match (edited.get(&id), edit_skipped.get(&id)) {
                        (Some(change), _) => (
                            id.to_string(),
                            format_number(&row.number),
                            match change.status {
                                channel_ops::RowStatus::Ok => "updated".to_string(),
                                channel_ops::RowStatus::Failed => "failed".to_string(),
                                channel_ops::RowStatus::Planned => "planned".to_string(),
                            },
                            if change.reason.is_empty() {
                                row.reason.clone()
                            } else {
                                change.reason.clone()
                            },
                        ),
                        (None, Some(reason)) => (
                            id.to_string(),
                            format_number(&row.current_number),
                            "skipped".to_string(),
                            (*reason).to_string(),
                        ),
                        (None, None) => (
                            id.to_string(),
                            format_number(&row.current_number),
                            "planned".to_string(),
                            row.reason.clone(),
                        ),
                    }
                }
            }
            SyncAction::Skipped => (
                row.channel_id.map(|id| id.to_string()).unwrap_or_default(),
                format_number(&row.number),
                "skipped".to_string(),
                row.reason.clone(),
            ),
        };
        outputs.push(SyncOutput {
            id,
            name: row.name.clone(),
            category: row.category.clone(),
            group: row.group.clone(),
            group_exists: group_exists.to_string(),
            number,
            status,
            reason,
            country: row.country.clone().unwrap_or_default(),
        });
    }

    let mut created = 0usize;
    let mut updated = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    for output in &outputs {
        match output.status.as_str() {
            "create" => created += 1,
            "updated" => updated += 1,
            "failed" => failed += 1,
            "planned" => {}
            _ => skipped += 1,
        }
    }
    let planned = outputs
        .iter()
        .filter(|output| output.status == "planned")
        .count();
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
        "sync_preview"
    } else {
        "sync_applied"
    };
    let title = if dry_run {
        "Sync Preview Report"
    } else {
        "Sync Applied Report"
    };
    let report = Report::new(&context.exports_dir(), app, stem, title)
        .channels_processed(decisions.rows.len())
        .result("Lineup", &lineup.file_name)
        .result("Numbering", settings.channel_numbering.clone())
        .result("Group migration", migration_note.trim().to_string())
        .result(
            "Channels written",
            if dry_run {
                "none, this was a preview".to_string()
            } else {
                format!("{created} created, {updated} updated")
            },
        )
        .result("Skipped entries", skipped)
        .write(
            context.config(),
            &schema,
            &values,
            &COLUMNS_SYNC,
            |writer| {
                for output in &outputs {
                    writer.write_record([
                        output.id.clone(),
                        csv_report::safe_cell(&output.name),
                        csv_report::safe_cell(&output.category),
                        csv_report::safe_cell(&output.group),
                        output.group_exists.clone(),
                        output.number.clone(),
                        output.status.clone(),
                        csv_report::safe_cell(&output.reason),
                        csv_report::safe_cell(&output.country),
                    ])?;
                }
                Ok(())
            },
        )?;

    // The migration row runs first, so it comes first: shift every later row
    // past it (the store keys rows by run + ordinal, and an overlap would drop
    // the create rows the undo lists).
    let mut rows = channel_ops::create_and_edit_run_rows(
        &create_plan,
        create_report.as_ref(),
        &edit_plan,
        applied.as_ref(),
    );
    for row in rows.iter_mut() {
        row.ordinal += migration_rows.len() as i64;
    }
    let mut all = migration_rows;
    all.extend(rows);
    context.record_rows(&all)?;

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
            "Sync preview: {planned} entr(ies) to sync into {} group(s) ({} new), \
             {skipped} skipped.{profile_note}",
            decisions.groups.len(),
            groups_created,
        )
    } else {
        let (written, apply_failed) = applied
            .as_ref()
            .map(|report| (report.ok, report.failed))
            .unwrap_or((0, 0));
        format!(
            "Sync apply: {created} created, {updated} updated ({written} edit(s) written, \
             {apply_failed} failed), {skipped} skipped, {groups_created} group(s) created.\
             {profile_note}"
        )
    };
    summary.push_str(&format!(" — lineup {}", lineup.file_name));
    let mut built = OutcomeBuilder::new(summary)
        .count("channels", decisions.rows.len() as i64)
        .count("created", created as i64)
        .count("updated", updated as i64)
        .count("planned", planned as i64)
        .count("groups_created", groups_created as i64)
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
    if let Some(note) = channel_ops::cancelled_note(context, &edit_plan, applied.as_ref()) {
        built = built.cancelled().note(note);
    }
    Ok(built.build())
}

/// One row of the sync CSV.
struct SyncOutput {
    id: String,
    name: String,
    category: String,
    group: String,
    group_exists: String,
    number: String,
    status: String,
    reason: String,
    country: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_core::config::CrosstalkConfig;
    use crosstalk_core::dispatcharr::{Channel, ChannelProfile};
    use crosstalk_core::jobs::VecSink;
    use crosstalk_core::store::{RunStatus, Store};
    use crosstalk_core::subapp::Registry;
    use std::sync::atomic::AtomicBool;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// The default settings on the test lineup (`US_Combined`): the run-level
    /// tests seed ownership under that file's keys.
    fn test_settings() -> crate::settings::LineupSettings {
        crate::settings::LineupSettings {
            lineup_file: "US_Combined_lineup.json".to_string(),
            ..crate::settings::LineupSettings::default()
        }
    }

    fn test_lineup() -> crate::lineup_file::Lineup {
        crate::lineup_file::load_lineup("US_Combined", "normal", "").expect("loads")
    }

    fn world_with(channels: Vec<Channel>, groups: Vec<ChannelGroup>) -> World {
        World {
            channels,
            groups,
            ..World::default()
        }
    }

    fn channel(id: i64, name: &str, number: Option<f64>, group: Option<i64>) -> Channel {
        Channel {
            id,
            name: name.to_string(),
            channel_number: number.map(|number| serde_json::json!(number)),
            channel_group_id: group,
            ..Channel::default()
        }
    }

    #[test]
    fn owner_keys_name_the_file_and_entry_only() {
        // The channel key carries no folded category (step 13 review): the same
        // entry under two details keys identically, so a detail change moves the
        // channel instead of re-creating it.
        let normal = crate::lineup_file::load_lineup("US_Combined", "normal", "").expect("loads");
        let simple = crate::lineup_file::load_lineup("US_Combined", "simple", "").expect("loads");
        let entry = normal.entries.first().expect("entry");
        let folded = simple
            .entries
            .iter()
            .find(|folded| folded.name == entry.name)
            .expect("same entry under simple");
        assert_ne!(entry.category, folded.category, "the fold moves it");
        assert_eq!(
            channel_owner_key(&normal.file_name, &entry.name),
            channel_owner_key(&simple.file_name, &folded.name),
            "the key ignores the folded category"
        );
        assert_eq!(
            channel_owner_key("US_Combined_lineup.json", "CNN"),
            "lineup:US_Combined_lineup.json:CNN"
        );
        assert_eq!(
            group_owner_key("US_Combined_lineup.json", "DIRECTV: News"),
            "lineup:group:US_Combined_lineup.json:DIRECTV: News"
        );
        assert_eq!(
            group_owner_prefix("US_Combined_lineup.json"),
            "lineup:group:US_Combined_lineup.json:"
        );
        // The named scope only counts channel keys under the file prefix: group
        // keys never enter it.
        assert!(!group_owner_key("US_Combined_lineup.json", "DIRECTV: News")
            .starts_with(&crate::owner_key_prefix("US_Combined").expect("prefix")));
    }

    /// A shipped file with a name in two categories (26 cases across the files).
    fn duplicate_lineup() -> crate::lineup_file::Lineup {
        for short in crate::lineup_file::available_lineups() {
            let lineup = crate::lineup_file::load_lineup(short, "normal", "").expect("loads");
            let mut seen: HashSet<&str> = HashSet::new();
            if lineup
                .entries
                .iter()
                .any(|entry| !seen.insert(entry.name.as_str()))
            {
                return lineup;
            }
        }
        panic!("the shipped files carry duplicate names");
    }

    #[test]
    fn first_run_creates_everything_owned() {
        let lineup = duplicate_lineup();
        let world = world_with(Vec::new(), Vec::new());
        let decisions =
            decide_sync(&world, &lineup, &test_settings(), &HashMap::new()).expect("decides");
        let expected: HashSet<&str> = lineup
            .entries
            .iter()
            .map(|entry| entry.group.as_str())
            .collect();
        let planned: HashSet<&str> = decisions
            .groups
            .iter()
            .map(|group| group.name.as_str())
            .collect();
        assert_eq!(planned, expected);
        // A name in two categories: one owned channel, first category wins.
        let dupes: Vec<&SyncRow> = decisions
            .rows
            .iter()
            .filter(|row| row.action == SyncAction::Skipped)
            .collect();
        assert!(!dupes.is_empty(), "{lineup:?} carries duplicates");
        for row in &dupes {
            assert!(row.reason.contains("first category wins"), "{row:?}");
        }
        let creates: Vec<&SyncRow> = decisions
            .rows
            .iter()
            .filter(|row| row.action == SyncAction::Create)
            .collect();
        assert_eq!(creates.len() + dupes.len(), lineup.entries.len());
        // `lineup` numbering: the file's numbers, `None` staying numberless.
        for row in &creates {
            let entry = lineup
                .entries
                .iter()
                .find(|entry| entry.name == row.name)
                .expect("entry");
            assert_eq!(
                row.number,
                entry.number.map(|number| number as f64),
                "{row:?}"
            );
        }
        let request = build_create_request(&decisions, Some(Vec::new()));
        assert_eq!(request.channels.len(), creates.len());
        assert!(request
            .channels
            .iter()
            .all(|channel| channel.streams.is_empty()));
        assert_eq!(
            request
                .channels
                .iter()
                .filter(|channel| channel.profile_ids == Some(Vec::new()))
                .count(),
            creates.len()
        );
    }

    #[test]
    fn rerun_is_a_no_op_and_auto_numbers_are_stable() {
        let mut settings = test_settings();
        settings.channel_numbering = "auto_next".to_string();
        let lineup = test_lineup();
        // First run against an empty world: every entry creates at 1..=N.
        let first = decide_sync(
            &world_with(Vec::new(), Vec::new()),
            &lineup,
            &settings,
            &HashMap::new(),
        )
        .expect("decides");
        let numbers: Vec<f64> = first
            .rows
            .iter()
            .filter_map(|row| {
                if row.action == SyncAction::Create {
                    row.number
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            numbers.len(),
            first
                .rows
                .iter()
                .filter(|row| row.action == SyncAction::Create)
                .count()
        );
        assert_eq!(numbers[0], 1.0);
        // The rerun world: every created channel live and owned, plus a foreign
        // channel squatting a low number (the LU renumber trigger).
        let mut owned: HashMap<String, i64> = HashMap::new();
        let mut channels: Vec<Channel> = Vec::new();
        let mut groups: Vec<ChannelGroup> = vec![ChannelGroup {
            id: 9,
            name: "Foreign".to_string(),
        }];
        for (next_id, row) in (100i64..).zip(
            first
                .rows
                .iter()
                .filter(|row| row.action == SyncAction::Create),
        ) {
            owned.insert(channel_owner_key(&lineup.file_name, &row.name), next_id);
            channels.push(channel(
                next_id,
                &row.name,
                row.number,
                Some(group_id_of(&mut groups, &row.group)),
            ));
        }
        channels.push(channel(7, "Squatter", Some(2.0), Some(9)));
        let world = world_with(channels, groups);
        let second = decide_sync(&world, &lineup, &settings, &owned).expect("decides");
        assert!(
            second
                .rows
                .iter()
                .all(|row| row.action != SyncAction::Create),
            "nothing new to create: {:?}",
            second
                .rows
                .iter()
                .filter(|row| row.action == SyncAction::Create)
                .collect::<Vec<_>>(),
        );
        assert!(
            second
                .rows
                .iter()
                .all(|row| matches!(row.action, SyncAction::Keep | SyncAction::Skipped)),
            "auto modes never renumber owned channels, even with a squatter: {:?}",
            second
                .rows
                .iter()
                .filter(|row| !matches!(row.action, SyncAction::Keep | SyncAction::Skipped))
                .collect::<Vec<_>>(),
        );
        // `lineup` mode renumbers only the owned channel whose file number moved.
        let settings = test_settings();
        assert_eq!(settings.channel_numbering, "lineup");
        // The world a `lineup`-mode sync made: every owned channel at its file
        // number (the auto world above holds 1..=N, which `lineup` mode would
        // rightly renumber wholesale).
        let mut world = world;
        for channel in world.channels.iter_mut() {
            if let Some(entry) = lineup
                .entries
                .iter()
                .find(|entry| entry.name == channel.name)
            {
                channel.channel_number =
                    entry.number.map(|number| serde_json::json!(number as f64));
            }
        }
        let steady = decide_sync(&world, &lineup, &settings, &owned).expect("decides");
        assert!(
            steady
                .rows
                .iter()
                .all(|row| matches!(row.action, SyncAction::Keep | SyncAction::Skipped)),
            "an unchanged file is a no-op in `lineup` mode too: {:?}",
            steady
                .rows
                .iter()
                .filter(|row| !matches!(row.action, SyncAction::Keep | SyncAction::Skipped))
                .collect::<Vec<_>>(),
        );
        let mut drifted = lineup.clone();
        let entry = drifted
            .entries
            .iter_mut()
            .find(|entry| entry.number.is_some())
            .expect("numbered");
        entry.number = Some(entry.number.unwrap_or(0) + 500);
        let entry_name = entry.name.clone();
        let third = decide_sync(&world, &drifted, &settings, &owned).expect("decides");
        let renumbers: Vec<&SyncRow> = third
            .rows
            .iter()
            .filter(|row| matches!(row.action, SyncAction::Renumber | SyncAction::MoveRenumber))
            .collect();
        assert_eq!(renumbers.len(), 1, "{renumbers:?}");
        assert_eq!(renumbers[0].name, entry_name);
    }

    fn group_id_of(groups: &mut Vec<ChannelGroup>, name: &str) -> i64 {
        if let Some(group) = groups.iter().find(|group| group.name == name) {
            return group.id;
        }
        let id = 1000 + groups.len() as i64;
        groups.push(ChannelGroup {
            id,
            name: name.to_string(),
        });
        id
    }

    #[test]
    fn category_detail_change_moves_owned_channels_without_recreating() {
        // Same entries, two folds: the owner key ignores the category, so the
        // second decision moves the owned channels instead of creating them.
        let normal = crate::lineup_file::load_lineup("US_Combined", "normal", "").expect("loads");
        let simple = crate::lineup_file::load_lineup("US_Combined", "simple", "").expect("loads");
        let settings = test_settings();
        let first = decide_sync(
            &world_with(Vec::new(), Vec::new()),
            &normal,
            &settings,
            &HashMap::new(),
        )
        .expect("decides");
        // Build the live world the first sync would have made (same groups).
        let mut owned: HashMap<String, i64> = HashMap::new();
        let mut groups: Vec<ChannelGroup> = Vec::new();
        let mut channels: Vec<Channel> = Vec::new();
        let mut group_ids: HashMap<String, i64> = HashMap::new();
        for (next_id, row) in (100i64..).zip(
            first
                .rows
                .iter()
                .filter(|row| row.action == SyncAction::Create),
        ) {
            let id = match group_ids.get(&row.group) {
                Some(id) => *id,
                None => {
                    let id = 500 + group_ids.len() as i64;
                    group_ids.insert(row.group.clone(), id);
                    groups.push(ChannelGroup {
                        id,
                        name: row.group.clone(),
                    });
                    id
                }
            };
            owned.insert(channel_owner_key(&normal.file_name, &row.name), next_id);
            channels.push(channel(next_id, &row.name, row.number, Some(id)));
        }
        let world = world_with(channels, groups);
        let second = decide_sync(&world, &simple, &settings, &owned).expect("decides");
        assert!(
            second
                .rows
                .iter()
                .all(|row| row.action != SyncAction::Create),
            "no creates after a detail change: {:?}",
            second
                .rows
                .iter()
                .filter(|row| row.action == SyncAction::Create)
                .collect::<Vec<_>>(),
        );
        let moves: Vec<&SyncRow> = second
            .rows
            .iter()
            .filter(|row| matches!(row.action, SyncAction::Move | SyncAction::MoveRenumber))
            .collect();
        assert!(!moves.is_empty(), "folded groups move their channels");
        for row in &moves {
            assert!(row.channel_id.is_some(), "moves are owned: {row:?}");
            assert_ne!(row.current_group.as_deref(), Some(row.group.as_str()));
        }
    }

    #[test]
    fn foreign_same_name_clash_is_reported_and_skipped() {
        let lineup = test_lineup();
        let entry = lineup.entries.first().expect("entry");
        // A foreign channel already holds the entry's name in its group.
        let world = world_with(
            vec![channel(7, &entry.name, Some(206.0), Some(10))],
            vec![ChannelGroup {
                id: 10,
                name: entry.group.clone(),
            }],
        );
        let decisions =
            decide_sync(&world, &lineup, &test_settings(), &HashMap::new()).expect("decides");
        let row = decisions
            .rows
            .iter()
            .find(|row| row.name == entry.name)
            .expect("row");
        assert_eq!(row.action, SyncAction::Skipped, "{row:?}");
        assert!(row.reason.contains("foreign"), "{row:?}");
        assert!(row.reason.contains("rather than adopted"), "{row:?}");
        // ... while the same name in ANOTHER group does not block the create
        // (LU's lookup is (name, group) too).
        let world = world_with(
            vec![channel(7, &entry.name, Some(206.0), Some(11))],
            vec![
                ChannelGroup {
                    id: 10,
                    name: entry.group.clone(),
                },
                ChannelGroup {
                    id: 11,
                    name: "Operator Group".to_string(),
                },
            ],
        );
        let decisions =
            decide_sync(&world, &lineup, &test_settings(), &HashMap::new()).expect("decides");
        let row = decisions
            .rows
            .iter()
            .find(|row| row.name == entry.name)
            .expect("row");
        assert_eq!(row.action, SyncAction::Create, "{row:?}");
    }

    #[test]
    fn profiles_resolve_case_insensitively_and_unknown_warns() {
        let world = World {
            profiles: vec![
                ChannelProfile {
                    id: 3,
                    name: "Living Room".to_string(),
                    channels: Vec::new(),
                },
                ChannelProfile {
                    id: 5,
                    name: "Kids".to_string(),
                    channels: Vec::new(),
                },
            ],
            ..World::default()
        };
        let mut settings = test_settings();
        assert_eq!(
            resolve_profile_ids(&world, &settings),
            (Some(Vec::new()), Vec::new())
        );
        settings.channel_profiles = "living room, KIDS, kids, Nope".to_string();
        let (ids, unknown) = resolve_profile_ids(&world, &settings);
        assert_eq!(ids, Some(vec![3, 5]));
        assert_eq!(unknown, vec!["Nope".to_string()]);
    }

    #[test]
    fn migration_renames_owned_prefix_all_only() {
        // `_migrate_single_group_name` naming, verbatim (prefix `DTV-` appends
        // directly and the new name strips the separator like `rstrip(" :_-/")`,
        // `DIRECTV` joins with `": "`, empty is the bare sentinel).
        assert_eq!(
            migration_names("DIRECTV"),
            ("DIRECTV: All".to_string(), "DIRECTV".to_string())
        );
        assert_eq!(
            migration_names("DTV-"),
            ("DTV-All".to_string(), "DTV".to_string())
        );
        assert_eq!(
            migration_names("US "),
            ("US All".to_string(), "US".to_string())
        );
        assert_eq!(migration_names(""), ("All".to_string(), "All".to_string()));
        // Owned pre-fix group: rename.
        let world = world_with(
            Vec::new(),
            vec![ChannelGroup {
                id: 40,
                name: "DIRECTV: All".to_string(),
            }],
        );
        let owned: HashSet<i64> = [40].into_iter().collect();
        let migration = decide_migration(&world, "DIRECTV", &owned);
        assert_eq!(migration.action, MigrationAction::Rename);
        assert_eq!(migration.id, Some(40));
        // Foreign pre-fix group: left alone (the fix over LU).
        let migration = decide_migration(&world, "DIRECTV", &HashSet::new());
        assert_eq!(migration.action, MigrationAction::Skipped);
        assert!(migration.reason.contains("foreign"), "{migration:?}");
        // Both names exist: left alone, like LU.
        let world = world_with(
            Vec::new(),
            vec![
                ChannelGroup {
                    id: 40,
                    name: "DIRECTV: All".to_string(),
                },
                ChannelGroup {
                    id: 41,
                    name: "DIRECTV".to_string(),
                },
            ],
        );
        let migration = decide_migration(&world, "DIRECTV", &owned);
        assert_eq!(migration.action, MigrationAction::Skipped);
        assert!(migration.reason.contains("both"), "{migration:?}");
        // No pre-fix group, or no prefix at all: nothing.
        let empty = world_with(Vec::new(), Vec::new());
        assert_eq!(
            decide_migration(&empty, "DIRECTV", &owned).action,
            MigrationAction::None
        );
        assert_eq!(
            decide_migration(&empty, "", &owned).action,
            MigrationAction::None
        );
    }

    #[test]
    fn global_number_conflicts_refuse_the_plan() {
        // A lineup number held by a foreign channel is refused (numbers are
        // global in Dispatcharr's outputs): the row keeps its old state and the
        // reason names the holder.
        let lineup = test_lineup();
        let entry = lineup
            .entries
            .iter()
            .find(|entry| entry.number.is_some())
            .expect("numbered");
        let number = entry.number.unwrap_or(0) as f64;
        let world = world_with(
            vec![channel(7, "Squatter", Some(number), Some(99))],
            vec![
                ChannelGroup {
                    id: 99,
                    name: "Foreign".to_string(),
                },
                ChannelGroup {
                    id: 10,
                    name: entry.group.clone(),
                },
            ],
        );
        let decisions =
            decide_sync(&world, &lineup, &test_settings(), &HashMap::new()).expect("decides");
        let request = build_create_request(&decisions, Some(Vec::new()));
        let plan = channel_ops::plan_creates(
            &world,
            &HashMap::new(),
            &request,
            &PlanOptions {
                number_conflicts: true,
                ..PlanOptions::default()
            },
        )
        .expect("plans");
        let refused: Vec<&channel_ops::CreateSkip> = plan
            .skipped
            .iter()
            .filter(|skip| skip.name == entry.name)
            .collect();
        assert_eq!(refused.len(), 1, "{refused:?}");
        assert!(refused[0].reason.contains("Squatter"), "{refused:?}");
    }

    // -- run-level tests (wiremock Dispatcharr, in-memory store) ---------------------

    struct FakeCreator {
        next: std::sync::Mutex<i64>,
        groups: std::sync::Mutex<Vec<ChannelGroup>>,
        channels: std::sync::Mutex<Vec<ChannelCreate>>,
    }

    // `number` and `group` are read through `Debug` in assertion messages.
    #[allow(dead_code)]
    #[derive(Clone, Debug)]
    struct ChannelCreate {
        name: String,
        number: Option<f64>,
        group: Option<i64>,
        profiles: Option<Vec<i64>>,
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

        async fn create_logo(
            &self,
            name: &str,
            url: &str,
        ) -> Result<crosstalk_core::dispatcharr::Logo> {
            Ok(crosstalk_core::dispatcharr::Logo {
                id: self.id(),
                name: name.to_string(),
                url: url.to_string(),
                ..crosstalk_core::dispatcharr::Logo::default()
            })
        }

        async fn create_channel(
            &self,
            channel: &crosstalk_core::dispatcharr::ChannelCreate,
        ) -> Result<Channel> {
            self.channels.lock().expect("lock").push(ChannelCreate {
                name: channel.name.clone(),
                number: channel.channel_number,
                group: channel.channel_group_id,
                profiles: channel.channel_profile_ids.clone(),
            });
            Ok(Channel {
                id: self.id(),
                name: channel.name.clone(),
                channel_number: channel
                    .channel_number
                    .map(|number| serde_json::json!(number)),
                channel_group_id: channel.channel_group_id,
                ..Channel::default()
            })
        }

        fn concurrency(&self) -> usize {
            8
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
                "/api/channels/profiles/",
                serde_json::to_value(&world.profiles).expect("profiles"),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(list(items))
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

    fn small_world() -> World {
        // Two channels the sync owns plus a foreign clash on the first entry.
        let lineup = test_lineup();
        let first = lineup.entries.first().expect("entry");
        let second = lineup.entries.get(1).expect("entry");
        world_with(
            vec![
                channel(
                    1,
                    &first.name,
                    first.number.map(|number| number as f64),
                    Some(10),
                ),
                channel(2, "Foreign Same Name", Some(999.0), Some(11)),
                channel(
                    3,
                    &second.name,
                    second.number.map(|number| number as f64),
                    Some(12),
                ),
            ],
            vec![
                ChannelGroup {
                    id: 10,
                    name: first.group.clone(),
                },
                // Distinct names only: the first two entries share a category,
                // and two groups of one name would make the move target a
                // coin toss.
                ChannelGroup {
                    id: 11,
                    name: "Foreign".to_string(),
                },
                ChannelGroup {
                    id: 12,
                    name: "Elsewhere".to_string(),
                },
            ],
        )
    }

    #[tokio::test]
    async fn preview_writes_nothing_but_the_csv() {
        let world = small_world();
        // No POST routes mounted: a preview that writes anything fails loudly
        // (the PATCH bulk mock only serves the apply path).
        let (server, config, _dir) = serve(&world).await;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let (context, _) = context_for(&config, &store, crate::ACTION_SYNC_PREVIEW, true);
        let outcome = run(&app, &context, &test_settings(), true)
            .await
            .expect("previews");
        assert!(
            outcome.summary.contains("Sync preview"),
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
            "id,channel_name,category,target_group,group_exists,channel_number,status,\
             reason,country"
        );
        // One row per entry (duplicates included, as skips).
        let lineup = test_lineup();
        assert_eq!(lines.count(), lineup.entries.len());
    }

    #[tokio::test]
    async fn apply_creates_moves_and_renumbers_owned_only() {
        let world = small_world();
        let (server, config, _dir) = serve(&world).await;
        let _ = server;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        // Seed ownership: channels 1 and 3 are ours (keys of the first two
        // entries); channel 2 is foreign.
        let lineup = test_lineup();
        let run_id = store
            .start_run(crate::APP_ID, crate::ACTION_SYNC_APPLY, false)
            .expect("run");
        for (channel_id, entry) in [(1, &lineup.entries[0]), (3, &lineup.entries[1])] {
            store
                .record_ownership(
                    "channel",
                    channel_id,
                    crate::APP_ID,
                    &channel_owner_key(&lineup.file_name, &entry.name),
                    run_id,
                )
                .expect("own");
        }
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("seeded"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let creator = Arc::new(FakeCreator::new(5000));
        let (context, run_id) = context_for(&config, &store, crate::ACTION_SYNC_APPLY, false);
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
            outcome.summary.contains("Sync apply"),
            "{}",
            outcome.summary
        );
        // Channel 3's entry group differs (Elsewhere vs the lineup group): it
        // moves. Channel 1 already matches: untouched (no rename — names_apply
        // owns names — and no number write).
        let posted = creator.channels.lock().expect("lock").clone();
        assert!(
            posted
                .iter()
                .all(|channel| channel.name != "Foreign Same Name"),
            "foreign channels are never adopted: {posted:?}"
        );
        // Ownership: created channels land under `lineup:<file>:<name>`.
        let owned = store.owned_keys(crate::APP_ID).expect("owned");
        assert!(
            owned.iter().any(|(kind, _, key)| kind == "channel"
                && key.starts_with("lineup:US_Combined_lineup.json:")),
            "{owned:?}"
        );
        // Undo reverses the moves/renumbers and hides the created channels;
        // created groups are left and listed.
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("applied"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        // The move landed: channel 3 is now in the second entry's group.
        let mut moved_world = world.clone();
        let second_group = lineup.entries[1].group.clone();
        let second_id = moved_world
            .groups
            .iter()
            .find(|group| group.name == second_group)
            .expect("second group")
            .id;
        moved_world
            .channels
            .iter_mut()
            .find(|channel| channel.id == 3)
            .expect("channel 3")
            .channel_group_id = Some(second_id);
        let registry = Registry::new(vec![Arc::new(crate::LineupApp::new())]);
        let undo = crosstalk_core::undo::plan_undo(&store, &moved_world, run_id, &registry)
            .expect("undo plans");
        assert!(
            undo.changes.iter().any(|change| change.channel_id == 3
                && matches!(change.reverse, channel_ops::SetValue::Group(Some(12)))),
            "the move of channel 3 reverses to Elsewhere: {undo:?}"
        );
        assert!(
            undo.left.iter().any(|left| left.kind == "group"),
            "created groups are left and listed: {undo:?}"
        );
    }

    #[tokio::test]
    async fn apply_passes_profile_ids_at_create_only() {
        let mut world = small_world();
        world.profiles = vec![ChannelProfile {
            id: 3,
            name: "Living Room".to_string(),
            channels: Vec::new(),
        }];
        let (server, config, _dir) = serve(&world).await;
        let _ = server;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let lineup = test_lineup();
        // Channels 1 and 3 are ours (Keep + Move); the rest creates.
        let run_id = store
            .start_run(crate::APP_ID, crate::ACTION_SYNC_APPLY, false)
            .expect("run");
        for (channel_id, entry) in [(1, &lineup.entries[0]), (3, &lineup.entries[1])] {
            store
                .record_ownership(
                    "channel",
                    channel_id,
                    crate::APP_ID,
                    &channel_owner_key(&lineup.file_name, &entry.name),
                    run_id,
                )
                .expect("own");
        }
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("seeded"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let mut settings = test_settings();
        settings.channel_profiles = "living room, Nope".to_string();
        let creator = Arc::new(FakeCreator::new(5000));
        let (context, _) = context_for(&config, &store, crate::ACTION_SYNC_APPLY, false);
        let outcome = run_with_creator(&app, &context, &settings, false, Some(creator.clone()))
            .await
            .expect("applies");
        assert!(outcome.summary.contains("Nope"), "{}", outcome.summary);
        // Every created channel carries the resolved profile id; the unknown
        // name warns (the summary) and is skipped. Existing owned channels
        // never gain membership rows: only creates carry ids (structural).
        let posted = creator.channels.lock().expect("lock").clone();
        assert!(!posted.is_empty(), "channels were created");
        for channel in &posted {
            assert_eq!(channel.profiles, Some(vec![3]), "{channel:?}");
        }
    }

    #[tokio::test]
    async fn named_scope_resolves_the_synced_channels() {
        let world = small_world();
        let (server, config, _dir) = serve(&world).await;
        let _ = server;
        let store = Store::in_memory().expect("store");
        let app = crate::LineupApp::new();
        let lineup = test_lineup();
        std::fs::write(
            config.config_dir.join("lineup.toml"),
            format!("lineup_file = {:?}\n", lineup.file_name),
        )
        .expect("seed");
        let run_id = store
            .start_run(crate::APP_ID, crate::ACTION_SYNC_APPLY, false)
            .expect("run");
        // Two owned channels of the configured file, one of another file.
        for (id, file, name) in [
            (
                1,
                lineup.file_name.as_str(),
                lineup.entries[0].name.as_str(),
            ),
            (
                3,
                lineup.file_name.as_str(),
                lineup.entries[1].name.as_str(),
            ),
            (9, "UK_Freeview_lineup.json", "BBC One"),
        ] {
            store
                .record_ownership(
                    "channel",
                    id,
                    crate::APP_ID,
                    &channel_owner_key(file, name),
                    run_id,
                )
                .expect("own");
        }
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("seeded"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        let resolved = app
            .resolve_named_scope(crate::NAMED_SCOPE_LINEUP, &config, &world, &store)
            .expect("served")
            .expect("resolves");
        assert_eq!(
            resolved,
            crosstalk_core::subapp::NamedScopeResolution::ChannelsWithHints {
                ids: vec![1, 3],
                hints: crosstalk_core::subapp::NamedScopeHints::with_country("US"),
            }
        );
    }
}
