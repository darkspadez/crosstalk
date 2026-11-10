//! `mirror_preview` / `mirror_apply`: Stream-Mapparr's `fill_mirror_backups_action`.
//!
//! Port of `Plugin.fill_mirror_backups_action` (`reference/Stream-Mapparr/Stream-Mapparr/plugin.py:9555`)
//! and the pure planner it delegates every rule to, `mirror_backups.py`
//! (`plan_mirror_fill`, `host_key`, `_insert_position`). The port is statement for statement
//! over a [`World`] instead of the ORM, which is why the planner is a pure function here too:
//! [`plan`] decides everything and touches nothing.
//!
//! # What a mirror is
//!
//! M3U accounts in one Dispatcharr server group can serve the SAME provider stream ids from
//! different provider hosts. Measured 2026-10-18 on one install (the planner's own docstring):
//! three accounts held 5,924 streams each and every provider stream id appeared on all three,
//! while only 5,031 names matched exactly between two of them. So a mirror is found by
//! **provider stream id, never by name** (`Stream.stream_id`), and the rules are:
//!
//!   * only accounts in a server group are sources or candidates;
//!   * only an ACTIVE candidate account is added from;
//!   * a stream from an account whose host the channel ALREADY has is not added — two accounts
//!     on one host would only add a second copy of the same failure;
//!   * the provider host is compared but never reported: on a real install it is the
//!     provider's name (hence [`host_key`] keeps only host and port, and the run rows carry
//!     account names);
//!   * nothing is ever removed. New streams go after the last stream whose account has a
//!     provider host, so a trailing custom placeholder stays last, where the operator put it.
//!
//! # Scale
//!
//! The plugin reads its three tables once and indexes mirrors by `(account_id, provider_id)`.
//! So does this: one hash map over the stream list for the whole run (`(account, provider id)
//! -> stream id`, lowest stream id wins, as the plugin's `order_by('id').setdefault`), one
//! entry per in-scope channel for its current list, and then one hash lookup per held
//! stream per candidate account. At 20k channels / 50k streams / 100 accounts that is
//! O(streams + channels x candidates-per-group), never a scan per channel.
//!
//! # What replaced the plugin's snapshot file
//!
//! The plugin refuses to write anything unless it first saved every changed channel's
//! previous order to `CHANNEL_SNAPSHOT_DIR/stream_mapparr_mirror_fill_<ts>.json`, and its
//! write loop skips a channel whose order changed since the plan read it (`select_for_update`
//! re-read). Crosstalk has no snapshot directory: the plan IS the write (`Op::Set` on
//! `Field::Streams`), and each channel's previous order lands in the run store as that row's
//! `before` value, which is the same evidence in the place every other write records it (and
//! what a restore reads). A channel changed during the run is therefore not re-read per
//! channel — the whole world is one snapshot taken before the plan — so the guarantees are:
//! what the run reports as `before` is what Dispatcharr held when the run started, and a
//! concurrent edit made after that is overwritten by the planned list rather than detected.
//! That is the price of not issuing 20k reads, and it is the same trade every other
//! channel-write action in this app makes.
//!
//! # What a dangling stream id does
//!
//! A channel whose current list holds a stream id the stream pool no longer has is PLANNED
//! like the plugin's dry run (from the rows its `ChannelStream ... stream__` join returns,
//! which drop the dangling row) but never WRITTEN: the plugin's write loop re-reads the
//! channel's rows under `select_for_update` and leaves alone any channel whose rows no
//! longer equal the plan's `old_order` — and a dangling row is exactly such a difference
//! (the locked re-read is a plain `ChannelStream` read, join-free, so it still sees the
//! dangling id). [`plan`] therefore puts such channels in [`MirrorOutcome::skipped`] with
//! the dry run's `after` list, and [`run`] writes every other entry only. Over REST the
//! stream list also omits streams on inactive M3U accounts, so a channel holding one of
//! those is left alone too — strictly less than the plugin writes there, never more, and
//! said so in the report.
//!
//! # Divergences
//!
//!   * The plugin's completion message counts `channels in scope` from its own load step and
//!     adds the channels that hold no stream at all to `no_source` (they never reach the
//!     planner). [`MirrorStats`] reports that number the same way, in one count.
//!   * A channel left alone for a dangling stream id keeps its list exactly as the run
//!     found it (dangling id included): the plugin refuses it at the locked re-read, and
//!     so does this, instead of rewriting the list without the dangling id.
//!   * The plugin writes `fill-mirror-backups.txt`; this writes the app's CSV (one row per
//!     added stream, account name included) with the plugin's readout counts in its
//!     preamble, so the file is the same information in the shape every other export has.

use crate::settings::StreamsSettings;
use anyhow::Result;
use crosstalk_core::channel_ops::{self, Field, Op, Plan, Selection, SetValue};
use crosstalk_core::config::ScopeConfig;
use crosstalk_core::csv_report::{self, Report};
use crosstalk_core::dispatcharr::{Channel, World, WorldKind, WorldKinds};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::scope::scope_channels;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use std::collections::{BTreeMap, HashMap, HashSet};

/// The report's title, without the preview/applied suffix.
const REPORT_TITLE: &str = "Fill Mirror Backups";

/// The CSV's columns: one row per added stream, so a reader sees which account each backup
/// came from (`fill-mirror-backups.txt`'s `+ acct7: CNN HD` lines).
const COLUMNS: [&str; 9] = [
    "channel_id",
    "channel_name",
    "channel_number",
    "channel_group",
    "stream_id",
    "stream_name",
    "account",
    "source_stream_id",
    "status",
];

/// `mirror_backups.host_key`: `host` or `host:port`, lower case, or `None` when the URL names
/// no host.
///
/// Credentials in the URL are dropped, because the host is what says whether two accounts are
/// behind the same provider. A malformed URL, an empty one and an unparseable port are all
/// `None` — the plugin's `(ValueError, TypeError, AttributeError)` branch — and a port of `0`
/// is omitted, as Python's `f"{host}:{port}" if port else host` does.
pub fn host_key(server_url: Option<&str>) -> Option<String> {
    let text = server_url?.trim();
    if text.is_empty() {
        return None;
    }
    // urlsplit's shape: `<scheme>://<netloc>/...`, or `//<netloc>/...`. Without an authority
    // there is no host at all (`urlsplit("not a url").hostname` is None).
    let after_scheme = match text.find("://") {
        Some(index) => {
            let scheme = &text[..index];
            let mut chars = scheme.chars();
            let starts_ok = chars
                .next()
                .is_some_and(|first| first.is_ascii_alphabetic());
            if !starts_ok
                || !scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
            {
                return None;
            }
            &text[index + 3..]
        }
        None => text.strip_prefix("//")?,
    };
    let end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let authority = &after_scheme[..end];
    // An empty netloc has no host (`http:///path`).
    if authority.is_empty() {
        return None;
    }
    // userinfo@host: the LAST `@` ends the credentials (what CPython's `_hostinfo` splits on).
    let host_port = match authority.rfind('@') {
        Some(index) => &authority[index + 1..],
        None => authority,
    };
    let (host, port) = if let Some(rest) = host_port.strip_prefix('[') {
        // An IPv6 literal: `[::1]` or `[::1]:8080`.
        let (host, after) = rest.split_once(']')?;
        (host, after.strip_prefix(':'))
    } else {
        match host_port.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (host_port, None),
        }
    };
    if host.is_empty() {
        return None;
    }
    match port {
        // `.port` raises ValueError for a non-numeric or out-of-range port, and the plugin
        // answers None for that whole URL.
        Some(port) if !port.is_empty() => {
            let value: u32 = port.parse().ok()?;
            if value > 65_535 {
                return None;
            }
            let host = host.to_lowercase();
            Some(if value == 0 {
                host
            } else {
                format!("{host}:{value}")
            })
        }
        // `host:` with nothing after it: CPython reads that as no port.
        _ => Some(host.to_lowercase()),
    }
}

/// One M3U account, as the planner needs it: `M3UAccount.objects.all().values(...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorAccount {
    pub id: i64,
    pub name: String,
    /// `server_group_id`, or `None` for an account outside every server group.
    pub group_id: Option<i64>,
    /// `is_active`: only an active account is ever added from.
    pub active: bool,
    /// [`host_key`] of the account's `server_url`.
    pub host: Option<String>,
}

/// One stream the plan would add.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MirrorAdd {
    /// The mirror stream's Dispatcharr id (the one the write puts in the list).
    pub stream_id: i64,
    /// The account it comes from.
    pub account_id: i64,
    /// The stream already on the channel whose provider id named this mirror.
    pub source_stream_id: i64,
}

/// One channel that gains at least one mirror stream.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelMirrors {
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_number: Option<f64>,
    pub channel_group: String,
    /// The channel's list as the run found it (the `old_order` the plugin snapshotted).
    pub before: Vec<i64>,
    /// The list the write would leave, mirrors inserted before the trailing no-host streams.
    pub after: Vec<i64>,
    pub added: Vec<MirrorAdd>,
}

/// Why a channel gained nothing, counted apart as the plugin's readout counts them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MirrorStats {
    /// Channels in the plan.
    pub gained: usize,
    /// Already holds a stream from every active host in its server group.
    pub covered: usize,
    /// Holds no stream from a server-group account with a provider stream id, so there is
    /// nothing to look a mirror up by (including the channels with no streams at all).
    pub no_source: usize,
    /// A host is missing but no account on it carries the mirror.
    pub no_mirror_found: usize,
}

/// What a mirror run decided. Nothing is written.
#[derive(Debug, Clone, Default)]
pub struct MirrorOutcome {
    /// In-scope channels, `len(channel_by_id)` in the plugin.
    pub channels: usize,
    /// The accounts, in id order, grouped or not (the readout lists them all).
    pub accounts: Vec<MirrorAccount>,
    /// Whether any account is in a server group at all: the plugin's early return.
    pub any_grouped_account: bool,
    /// One entry per channel that gains a stream, in channel-id order (the plugin's order).
    pub entries: Vec<ChannelMirrors>,
    /// Planned exactly like [`MirrorOutcome::entries`] but never written: the channel's
    /// current list holds a stream id the stream pool no longer has, so the plugin's locked
    /// re-read would find rows that differ from the plan's `old_order` and leave the channel
    /// alone — and so does [`run`]. The plugin's readout counts these channels (its plan
    /// includes them; only the write refuses them), so every readout count below reads them
    /// the same way: they are in `gained`, in `per_account`, and in [`MirrorOutcome::added_count`].
    pub skipped: Vec<ChannelMirrors>,
    pub stats: MirrorStats,
    /// Stream ids in-scope channels still hold that the stream pool no longer has.
    ///
    /// The plugin's `ChannelStream ... stream__` join never returns such a row, so the planner
    /// does not see it — and because the locked re-read before the write DOES see it, the
    /// plugin leaves a channel holding one alone instead of rewriting its list. Counted over
    /// the whole scope, because the report's job is to say the data is inconsistent; a channel
    /// that gains nothing is not rewritten either way, so only a channel that WOULD gain a
    /// mirror lands in [`MirrorOutcome::skipped`] over one of these.
    pub missing_streams: usize,
    /// The scope in words, for the report preamble.
    pub description: String,
}

impl MirrorOutcome {
    /// Mirror streams the plan would add, over every planned channel including the ones left
    /// alone ([`MirrorOutcome::skipped`): the plugin's readout counts them — its plan does not
    /// know the write will refuse them — so this matches the readout, not the write.
    pub fn added_count(&self) -> usize {
        self.entries
            .iter()
            .chain(self.skipped.iter())
            .map(|entry| entry.added.len())
            .sum()
    }

    /// Channels the plan would change, written or left alone (the plugin's `len(plan)`).
    pub fn planned_channels(&self) -> usize {
        self.entries.len() + self.skipped.len()
    }

    /// Added streams per account, in account-id order (the plugin's `per_account`): planned
    /// channels, including the ones left alone, because the readout is written from the plan.
    pub fn per_account(&self) -> BTreeMap<i64, usize> {
        let mut counts: BTreeMap<i64, usize> = BTreeMap::new();
        for entry in self.entries.iter().chain(self.skipped.iter()) {
            for add in &entry.added {
                *counts.entry(add.account_id).or_default() += 1;
            }
        }
        counts
    }

    /// The plugin's completion sentence (`9555`), counts and all.
    pub fn summary(&self) -> String {
        format!(
            "Of {} channels in scope: {} already have every active host, {} have no \
             server-group stream to copy from, {} miss a host whose accounts lack the same \
             stream.",
            self.channels, self.stats.covered, self.stats.no_source, self.stats.no_mirror_found
        )
    }
}

/// One stream of a channel's current list, resolved against the stream list.
///
/// The plugin's `channel_streams` rows are exactly this: the stream id the channel holds, the
/// account it came from and the provider's own id. A stream id the stream list does not have
/// is not a row in the plugin's join, so it is not one here either.
#[derive(Debug, Clone, Copy)]
struct HeldStream {
    id: i64,
    account_id: Option<i64>,
    provider_id: Option<i64>,
}

/// The accounts of a world, in id order (`sorted(accounts.items())`).
pub fn accounts_of(world: &World) -> Vec<MirrorAccount> {
    let mut accounts: Vec<MirrorAccount> = world
        .m3u_accounts
        .iter()
        .map(|account| MirrorAccount {
            id: account.id,
            name: account.name.clone(),
            group_id: account.server_group,
            active: account.is_active,
            host: host_key(account.server_url.as_deref()),
        })
        .collect();
    accounts.sort_by_key(|account| account.id);
    accounts
}

/// `mirror_index[(account_id, provider_id)] = stream_id`, lowest stream id per pair.
///
/// The plugin asks `Stream.objects.filter(m3u_account_id__in=grouped_ids,
/// stream_id__in=wanted).order_by('id')` and keeps the first row per pair; one pass over the
/// whole stream list with "keep the smaller id" is the same map for every channel at once, so
/// a run indexes 50k streams once instead of one query per scope.
fn mirror_index(
    world: &World,
    accounts: &BTreeMap<i64, &MirrorAccount>,
) -> HashMap<(i64, i64), i64> {
    let mut index: HashMap<(i64, i64), i64> = HashMap::new();
    for stream in &world.streams {
        let Some(account_id) = stream.m3u_account else {
            continue;
        };
        if !accounts.contains_key(&account_id) {
            continue;
        }
        let Some(provider_id) = stream.stream_id else {
            continue;
        };
        index
            .entry((account_id, provider_id))
            .and_modify(|existing| {
                if stream.id < *existing {
                    *existing = stream.id;
                }
            })
            .or_insert(stream.id);
    }
    index
}

/// `mirror_backups._insert_position`: the index just after the last stream whose account has
/// a provider host.
///
/// Trailing streams with no host — a custom placeholder, or a stream whose account is unknown
/// — stay last, where the operator placed them. The host is read whatever the account's
/// `active` says: this is about the channel's ORDER, not about who a backup comes from.
fn insert_position(streams: &[HeldStream], accounts: &BTreeMap<i64, &MirrorAccount>) -> usize {
    let mut position = 0;
    for (index, stream) in streams.iter().enumerate() {
        let has_host = stream
            .account_id
            .and_then(|id| accounts.get(&id))
            .is_some_and(|account| account.host.is_some());
        if has_host {
            position = index + 1;
        }
    }
    position
}

/// Plan a mirror run: scope, accounts, index, then the plugin's per-channel loop.
///
/// Pure: nothing is written and the world is not modified.
pub fn plan(world: &World, scope: &ScopeConfig) -> Result<MirrorOutcome> {
    world.require(WorldKind::Channels)?;
    // The provider id and the account of a stream are what a mirror IS; without the stream
    // list every channel would look like it has nothing to mirror from.
    world.require(WorldKind::Streams)?;
    world.require(WorldKind::M3uAccounts)?;

    let scoped = scope_channels(world, scope)?;
    let accounts_list = accounts_of(world);
    let by_id: BTreeMap<i64, &MirrorAccount> = accounts_list
        .iter()
        .map(|account| (account.id, account))
        .collect();
    let grouped_ids: Vec<i64> = accounts_list
        .iter()
        .filter(|account| account.group_id.is_some())
        .map(|account| account.id)
        .collect();

    let mut outcome = MirrorOutcome {
        channels: scoped.channels.len(),
        any_grouped_account: !grouped_ids.is_empty(),
        description: format!("channels in scope{}", scope_suffix(scope)),
        ..MirrorOutcome::default()
    };
    if grouped_ids.is_empty() {
        // The plugin's early return: no server group means no mirrors, and it says so
        // without reading a single channel.
        return Ok(outcome);
    }

    // `group_members`: server group -> its account ids, in id order.
    let mut group_members: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for account in accounts_list
        .iter()
        .filter(|account| account.group_id.is_some())
    {
        group_members
            .entry(account.group_id.expect("filtered"))
            .or_default()
            .push(account.id);
    }

    // `Stream.id -> (account, provider id)`, one lookup per held id.
    let stream_by_id: HashMap<i64, (Option<i64>, Option<i64>)> = world
        .streams
        .iter()
        .map(|stream| (stream.id, (stream.m3u_account, stream.stream_id)))
        .collect();
    let index = mirror_index(world, &by_id);

    // The plugin's `channel_streams` query: ordered by channel id, then by the channel's own
    // order. A channel with no rows is absent, and counted as `no_source` below.
    let mut ordered: Vec<&Channel> = scoped.channels.iter().collect();
    ordered.sort_by_key(|channel| channel.id);

    let mut rows_by_channel: HashMap<i64, Vec<HeldStream>> = HashMap::new();
    for channel in &ordered {
        if channel.streams.is_empty() {
            continue;
        }
        let mut rows: Vec<HeldStream> = Vec::with_capacity(channel.streams.len());
        for id in &channel.streams {
            match stream_by_id.get(id) {
                Some((account_id, provider_id)) => rows.push(HeldStream {
                    id: *id,
                    account_id: *account_id,
                    provider_id: *provider_id,
                }),
                // Not a row in the plugin's join: the plan never saw it, and the rewrite
                // drops it. Counted so the report can say the list cleaned one up.
                None => outcome.missing_streams += 1,
            }
        }
        rows_by_channel.insert(channel.id, rows);
    }

    let channels_without_streams = ordered
        .iter()
        .filter(|channel| !rows_by_channel.contains_key(&channel.id))
        .count();

    let mut stats = MirrorStats::default();
    for channel in &ordered {
        let Some(streams) = rows_by_channel.get(&channel.id) else {
            // A channel in scope with no stream at all never reaches the planner; the plugin
            // folds it into `no_source` after the fact.
            continue;
        };
        let mut held: HashSet<i64> = streams.iter().map(|stream| stream.id).collect();
        let mut hosts: HashSet<String> = streams
            .iter()
            .filter_map(|stream| {
                let account = by_id.get(&stream.account_id?)?;
                (account.active).then(|| account.host.clone()).flatten()
            })
            .collect();

        let mut added: Vec<MirrorAdd> = Vec::new();
        let mut source_groups: HashSet<i64> = HashSet::new();
        for source in streams {
            let (Some(provider_id), Some(source_account)) = (
                source.provider_id,
                source.account_id.and_then(|id| by_id.get(&id)),
            ) else {
                continue;
            };
            let Some(group_id) = source_account.group_id else {
                continue;
            };
            source_groups.insert(group_id);
            for candidate_id in group_members
                .get(&group_id)
                .map(Vec::as_slice)
                .unwrap_or(&[])
            {
                if Some(*candidate_id) == source.account_id {
                    continue;
                }
                let Some(candidate) = by_id.get(candidate_id) else {
                    continue;
                };
                let Some(host) = candidate.host.as_ref() else {
                    continue;
                };
                if !candidate.active || hosts.contains(host) {
                    continue;
                }
                let Some(mirror_id) = index.get(&(*candidate_id, provider_id)) else {
                    continue;
                };
                if !held.insert(*mirror_id) {
                    continue;
                }
                added.push(MirrorAdd {
                    stream_id: *mirror_id,
                    account_id: *candidate_id,
                    source_stream_id: source.id,
                });
                hosts.insert(host.clone());
            }
        }

        let before: Vec<i64> = channel.streams.clone();
        // A held id the pool no longer has: the plugin planned this channel from the joined
        // rows (which drop it) and then refused it at the locked re-read (which sees it).
        let dangling = streams.len() != channel.streams.len();
        if added.is_empty() {
            if source_groups.is_empty() {
                stats.no_source += 1;
            } else if source_groups.iter().any(|group_id| {
                group_members
                    .get(group_id)
                    .map(Vec::as_slice)
                    .unwrap_or(&[])
                    .iter()
                    .filter_map(|account_id| by_id.get(account_id))
                    .any(|account| {
                        account.active
                            && account
                                .host
                                .as_ref()
                                .is_some_and(|host| !hosts.contains(host))
                    })
            }) {
                stats.no_mirror_found += 1;
            } else {
                stats.covered += 1;
            }
            continue;
        }
        stats.gained += 1;
        let at = insert_position(streams, &by_id);
        // The mirrors go in as a block: `old_order[:at] + [ids] + old_order[at:]`.
        let resolved: Vec<i64> = streams.iter().map(|stream| stream.id).collect();
        let mut after: Vec<i64> = Vec::with_capacity(resolved.len() + added.len());
        after.extend_from_slice(&resolved[..at.min(resolved.len())]);
        after.extend(added.iter().map(|add| add.stream_id));
        after.extend_from_slice(&resolved[at.min(resolved.len())..]);
        let mirrors = ChannelMirrors {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_number: channel.display_number(),
            channel_group: world.group_name(channel.channel_group_id),
            before,
            after,
            added,
        };
        if dangling {
            // Planned, never written: the locked re-read would refuse this channel, so the
            // run leaves its list exactly as found instead of rewriting it without the
            // dangling id. Still `gained`, still in the readout: the plugin's plan counts it.
            outcome.skipped.push(mirrors);
        } else {
            outcome.entries.push(mirrors);
        }
    }
    // The plugin's `stats["no_source"] += len(channel_by_id) - len(channel_streams)`, which is
    // exactly the channels that hold no stream at all.
    stats.no_source += channels_without_streams;
    outcome.stats = stats;
    outcome.accounts = accounts_list;
    Ok(outcome)
}

/// The action: preview (write the CSV) or apply (insert the mirrors that would be added).
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    let client = context.dispatcharr()?;
    let action_id = format!(
        "streams.mirror_{}",
        if dry_run { "preview" } else { "apply" }
    );
    let scope = context.config().scope.clone();

    // Every slice a mirror fill reads: channels (their own lists), groups (report names),
    // profiles (the scope's profile filter), streams (provider ids, accounts, names) and the
    // M3U accounts (server groups, activity, hosts). No EPG, no stats.
    let kinds = WorldKinds::only(WorldKind::Channels)
        | WorldKind::Groups
        | WorldKind::Profiles
        | WorldKind::Streams
        | WorldKind::M3uAccounts;
    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.mirror",
        0,
        0,
        "loading Dispatcharr data",
    ));
    let world = client.load_world_with(kinds).await?;

    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.mirror",
        0,
        0,
        "planning mirror backups",
    ));
    let outcome = plan(&world, &scope)?;
    if !outcome.any_grouped_account {
        // The plugin's own wording (`9594`).
        return Ok(JobOutcome::new(
            "No M3U account is in a server group, so there are no mirrors to add.",
        ));
    }
    if outcome.channels == 0 {
        return Ok(JobOutcome::new(
            "No channels are in scope. Check the group and profile settings in crosstalk.toml.",
        ));
    }

    // Only the channels that gain a stream are a change, and only the ones without a
    // dangling id are written: a channel holding a stream id the pool no longer has is left
    // alone, as the plugin's locked re-read leaves it (`outcome.skipped`). A channel already
    // holding every mirror is not a row.
    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for entry in &outcome.entries {
        values.insert(entry.channel_id, SetValue::Streams(entry.after.clone()));
    }
    let op_plan = if values.is_empty() {
        Plan {
            description: outcome.description.clone(),
            ..Plan::default()
        }
    } else {
        let operations = vec![Op::Set {
            field: Field::Streams,
            values,
        }];
        channel_ops::plan(&world, &Selection::in_scope(&scope), &operations)?
    };

    let applied = if dry_run || op_plan.changes.is_empty() {
        None
    } else {
        Some(channel_ops::apply(client, &op_plan, context, &action_id).await)
    };

    let report = write_mirror_report(context, app, settings, &world, &outcome, applied.as_ref())?;
    context.record_rows(&channel_ops::run_rows(&op_plan, applied.as_ref()))?;

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let added = outcome.added_count();
    let gained = outcome.planned_channels();
    // A dry run counts the plan (the plugin's dry-run message counts what it WOULD add,
    // refusals included); a live run counts what it wrote.
    let written_added: usize = outcome.entries.iter().map(|entry| entry.added.len()).sum();
    let written_channels = outcome.entries.len();
    let mut builder = OutcomeBuilder::new(if added == 0 {
        format!("Nothing to add. {}", outcome.summary())
    } else if dry_run {
        format!(
            "Dry run: would add {added} mirror stream(s) to {gained} channel(s). Nothing was \
             changed. {}",
            outcome.summary()
        )
    } else if outcome.skipped.is_empty() {
        format!("Added {written_added} mirror stream(s) to {written_channels} channel(s).")
    } else {
        format!(
            "Added {written_added} mirror stream(s) to {written_channels} channel(s); {} \
             channel(s) left alone holding a stream id the stream list no longer has.",
            outcome.skipped.len()
        )
    })
    .count("selected", outcome.channels as i64)
    .count("gained", gained as i64)
    .count("mirror_streams", added as i64)
    .count("left_alone", outcome.skipped.len() as i64)
    .count("covered", outcome.stats.covered as i64)
    .count("no_source", outcome.stats.no_source as i64)
    .count("no_mirror_found", outcome.stats.no_mirror_found as i64)
    .count("missing_streams", outcome.missing_streams as i64);
    for (account_id, count) in outcome.per_account() {
        let name = outcome
            .accounts
            .iter()
            .find(|account| account.id == account_id)
            .map(|account| account.name.clone())
            .unwrap_or_else(|| format!("account {account_id}"));
        builder = builder.note(format!("{count} stream(s) from {name}"));
    }
    if let Some(applied) = applied.as_ref() {
        builder = builder
            .count("written", applied.ok as i64)
            .count("failed", applied.failed as i64);
        if applied.failed > 0 {
            builder = builder.note(format!("{} row(s) failed", applied.failed));
        }
    }
    if outcome.missing_streams > 0 {
        builder = builder.note(format!(
            "{} stream id(s) held by in-scope channels are no longer in the stream list; a \
             channel holding one is left alone, as the plugin's re-read leaves it",
            outcome.missing_streams
        ));
    }
    // The cancel rule (see crosstalk_core::jobs): whatever landed stays recorded, and the run
    // says it was cancelled rather than claiming every channel gained its mirror.
    if let Some(note) = channel_ops::cancelled_note(context, &op_plan, applied.as_ref()) {
        builder = builder.cancelled().note(note);
    }
    Ok(builder.report(Some(report)).build())
}

/// The CSV: one row per planned addition, plus the plugin's readout counts.
///
/// Channels left alone for a dangling stream id are in the CSV too, with the `skipped`
/// status: the plugin's readout lists their additions (its plan includes them), and the
/// status says why no write followed.
fn write_mirror_report(
    context: &JobContext,
    app: &dyn SubApp,
    settings: &StreamsSettings,
    world: &World,
    outcome: &MirrorOutcome,
    applied: Option<&channel_ops::ApplyReport>,
) -> Result<std::path::PathBuf> {
    let status_of = |channel_id: i64| -> (&'static str, &str) {
        if outcome
            .skipped
            .iter()
            .any(|entry| entry.channel_id == channel_id)
        {
            return (
                "skipped",
                "the channel holds a stream id the stream list no longer has; left alone",
            );
        }
        let Some(applied) = applied else {
            return ("planned", "");
        };
        match applied
            .changes
            .iter()
            .find(|change| change.channel_id == channel_id)
        {
            Some(change) => match change.status {
                channel_ops::RowStatus::Ok => ("ok", ""),
                channel_ops::RowStatus::Failed => ("failed", change.reason.as_str()),
                channel_ops::RowStatus::Planned => ("planned", ""),
            },
            None => ("not planned", ""),
        }
    };
    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = format!(
        "mirror_{}",
        if applied.is_some() {
            "applied"
        } else {
            "preview"
        }
    );
    let title = format!(
        "{REPORT_TITLE} {} Report",
        if applied.is_some() {
            "Applied"
        } else {
            "Preview"
        }
    );
    let stream_names: HashMap<i64, &str> = world
        .streams
        .iter()
        .map(|stream| (stream.id, stream.name.as_str()))
        .collect();
    let account_names: HashMap<i64, &str> = outcome
        .accounts
        .iter()
        .map(|account| (account.id, account.name.as_str()))
        .collect();
    let exports = context.exports_dir();
    let mut report = Report::new(&exports, app, &stem, &title)
        .channels_processed(outcome.channels)
        .result("Selection", &outcome.description)
        .result("Channels that gain a mirror", outcome.planned_channels())
        .result("Mirror streams planned", outcome.added_count())
        .result(
            "Channels left alone (dangling stream id)",
            outcome.skipped.len(),
        )
        .result("Already have every active host", outcome.stats.covered)
        .result("No server-group stream to copy", outcome.stats.no_source)
        .result(
            "Host missing, mirror not there",
            outcome.stats.no_mirror_found,
        )
        .result("Dangling stream ids in scope", outcome.missing_streams);
    if let Some(applied) = applied {
        report = report
            .result("Channels written", applied.ok)
            .result("Rows failed", applied.failed);
    }
    report = report.result(
        "Server group accounts",
        format!(
            "{} account(s) in a server group, {} in the table",
            outcome
                .accounts
                .iter()
                .filter(|account| account.group_id.is_some())
                .count(),
            outcome.accounts.len()
        ),
    );
    let per_account = outcome.per_account();
    report = report.result(
        "Added per account",
        if per_account.is_empty() {
            "none".to_string()
        } else {
            per_account
                .iter()
                .map(|(account_id, count)| {
                    format!(
                        "{}: {count}",
                        account_names
                            .get(account_id)
                            .copied()
                            .unwrap_or("unknown account")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        },
    );
    // The one thing the plugin's readout says about accounts it never added from: they are
    // listed with their name and whether they are active.
    report = report.result(
        "Accounts",
        outcome
            .accounts
            .iter()
            .map(|account| {
                format!(
                    "{} (group {}, {}, added {})",
                    account.name,
                    account
                        .group_id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    if account.active { "active" } else { "INACTIVE" },
                    per_account.get(&account.id).copied().unwrap_or(0)
                )
            })
            .collect::<Vec<_>>()
            .join("; "),
    );

    report.write(context.config(), &schema, &values, &COLUMNS, |writer| {
        for entry in outcome.entries.iter().chain(outcome.skipped.iter()) {
            let (status, reason) = status_of(entry.channel_id);
            for add in &entry.added {
                writer.write_record([
                    entry.channel_id.to_string(),
                    csv_report::safe_cell(&entry.channel_name),
                    entry
                        .channel_number
                        .map(|number| number.to_string())
                        .unwrap_or_default(),
                    csv_report::safe_cell(&entry.channel_group),
                    add.stream_id.to_string(),
                    csv_report::safe_cell(stream_names.get(&add.stream_id).copied().unwrap_or("")),
                    csv_report::safe_cell(
                        account_names
                            .get(&add.account_id)
                            .copied()
                            .unwrap_or("unknown account"),
                    ),
                    add.source_stream_id.to_string(),
                    if reason.is_empty() {
                        status.to_string()
                    } else {
                        format!("{status}: {reason}")
                    },
                ])?;
            }
        }
        Ok(())
    })
}

/// The scope in a sentence, for a plan built without a selection.
fn scope_suffix(scope: &ScopeConfig) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !scope.profiles.trim().is_empty() {
        parts.push(format!("in profile(s) {}", scope.profiles.trim()));
    }
    if !scope.groups.trim().is_empty() {
        parts.push(format!("in groups {}", scope.groups.trim()));
    }
    if !scope.ignore_groups.trim().is_empty() {
        parts.push(format!("outside groups {}", scope.ignore_groups.trim()));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(", {}", parts.join(" and "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::ServerFixture;
    use crate::{StreamsApp, ACTION_MIRROR_APPLY, ACTION_MIRROR_PREVIEW};
    use crosstalk_core::dispatcharr::{Channel, ChannelGroup, M3uAccount, Stream};

    /// One provider stream id served from three hosts, the shape a real install has.
    fn mirror_world() -> World {
        let account =
            |id: i64, name: &str, group: Option<i64>, active: bool, url: Option<&str>| M3uAccount {
                id,
                name: name.to_string(),
                is_active: active,
                server_group: group,
                server_url: url.map(str::to_string),
                ..M3uAccount::default()
            };
        let stream = |id: i64, name: &str, account: i64, provider: Option<i64>| Stream {
            id,
            name: name.to_string(),
            m3u_account: Some(account),
            stream_id: provider,
            ..Stream::default()
        };
        World {
            m3u_accounts: vec![
                // The locked account hand-made streams belong to: no host, no server group.
                account(1, "Custom", None, true, None),
                account(
                    6,
                    "Provider A",
                    Some(1),
                    true,
                    Some("http://a.example:80/get.php"),
                ),
                account(7, "Provider B", Some(1), true, Some("http://b.example/x")),
                // A second account on provider A's host: it adds nothing.
                account(
                    8,
                    "Provider A backup",
                    Some(1),
                    true,
                    Some("HTTP://A.EXAMPLE:80/other"),
                ),
                account(
                    9,
                    "Provider C (off)",
                    Some(1),
                    false,
                    Some("http://c.example/x"),
                ),
            ],
            groups: vec![ChannelGroup {
                id: 100,
                name: "News".to_string(),
            }],
            channels: vec![
                Channel {
                    id: 100,
                    name: "CNN".to_string(),
                    channel_number: Some(serde_json::json!(1)),
                    channel_group_id: Some(100),
                    // Host A only, with the placeholder last: it gains B's mirror.
                    streams: vec![10, 99],
                    ..Channel::default()
                },
                Channel {
                    id: 200,
                    name: "ESPN".to_string(),
                    channel_number: Some(serde_json::json!(2)),
                    channel_group_id: Some(100),
                    // Hosts A and B: covered.
                    streams: vec![11, 21],
                    ..Channel::default()
                },
                Channel {
                    id: 300,
                    name: "KFDA".to_string(),
                    channel_number: Some(serde_json::json!(3)),
                    channel_group_id: Some(100),
                    // The off account can still NAME a mirror.
                    streams: vec![40],
                    ..Channel::default()
                },
                Channel {
                    id: 400,
                    name: "Empty".to_string(),
                    channel_number: Some(serde_json::json!(4)),
                    channel_group_id: Some(100),
                    streams: Vec::new(),
                    ..Channel::default()
                },
            ],
            streams: vec![
                stream(10, "CNN HD", 6, Some(555)),
                stream(20, "CNN HD (B)", 7, Some(555)),
                stream(30, "CNN HD (A backup)", 8, Some(555)),
                stream(40, "CNN HD (C)", 9, Some(555)),
                stream(11, "ESPN", 6, Some(777)),
                stream(21, "ESPN (B)", 7, Some(777)),
                stream(99, "Could Not Dispatch", 1, None),
            ],
            ..World::default()
        }
    }

    fn scoped() -> ScopeConfig {
        ScopeConfig::default()
    }

    #[test]
    fn host_key_reduces_a_server_url_to_host_and_port() {
        // The plugin's own table (`reference/Stream-Mapparr/tests/test_mirror_backups.py`).
        assert_eq!(
            host_key(Some("http://Host.Example:8080/get.php?u=x")),
            Some("host.example:8080".to_string())
        );
        assert_eq!(
            host_key(Some("http://host.example")),
            Some("host.example".to_string())
        );
        assert_eq!(
            host_key(Some("https://user:pw@host.example/path")),
            Some("host.example".to_string())
        );
        assert_eq!(host_key(Some("")), None);
        assert_eq!(host_key(None), None);
        assert_eq!(host_key(Some("not a url")), None);
        // A different port is a different server.
        assert_ne!(
            host_key(Some("http://h.example:80/")),
            host_key(Some("http://h.example:8080/"))
        );
        // The edges Python's `.port` covers: a port of 0 is omitted, a bad one is None, an
        // IPv6 literal keeps its brackets' contents, and a bare `//host` has one.
        assert_eq!(
            host_key(Some("http://h.example:0/")),
            Some("h.example".into())
        );
        assert_eq!(host_key(Some("http://h.example:abc/")), None);
        assert_eq!(host_key(Some("http://h.example:70000/")), None);
        // Python's `.hostname` strips IPv6 brackets, so the key carries the bare address.
        assert_eq!(
            host_key(Some("http://[::1]:8080/x")),
            Some("::1:8080".into())
        );
        assert_eq!(host_key(Some("//h.example/x")), Some("h.example".into()));
        assert_eq!(host_key(Some("http:///path")), None);
    }

    #[test]
    fn a_channel_with_one_host_gains_the_other_hosts_mirror_before_the_placeholder() {
        let world = mirror_world();
        let outcome = plan(&world, &scoped()).expect("plan");
        let cnn = outcome
            .entries
            .iter()
            .find(|entry| entry.channel_id == 100)
            .expect("CNN gains a mirror");
        // Only B: A's second account is on a host CNN already has, C is switched off.
        assert_eq!(cnn.added.len(), 1);
        assert_eq!(cnn.added[0].stream_id, 20);
        assert_eq!(cnn.added[0].account_id, 7);
        assert_eq!(cnn.added[0].source_stream_id, 10);
        // Inserted after the provider stream, before the custom placeholder.
        assert_eq!(cnn.before, vec![10, 99]);
        assert_eq!(cnn.after, vec![10, 20, 99]);
    }

    #[test]
    fn the_plan_counts_why_each_channel_gained_nothing() {
        let world = mirror_world();
        let outcome = plan(&world, &scoped()).expect("plan");
        // ESPN holds both active hosts, KFDA gains from A and B (its source is the off
        // account, which can still name the mirror), Empty has no stream at all.
        let ids: Vec<i64> = outcome
            .entries
            .iter()
            .map(|entry| entry.channel_id)
            .collect();
        assert_eq!(ids, vec![100, 300], "channel-id order, as the plugin plans");
        assert_eq!(outcome.stats.gained, 2);
        assert_eq!(outcome.stats.covered, 1);
        assert_eq!(outcome.stats.no_mirror_found, 0);
        assert_eq!(outcome.stats.no_source, 1, "the empty channel");
        assert_eq!(outcome.channels, 4);
        assert!(outcome.summary().contains("1 have no server-group stream"));
    }

    #[test]
    fn mirrors_come_only_from_the_source_accounts_server_group() {
        let mut world = mirror_world();
        // A fourth host in ANOTHER server group: never a source for group 1.
        world.m3u_accounts.push(M3uAccount {
            id: 12,
            name: "Elsewhere".to_string(),
            server_group: Some(2),
            server_url: Some("http://d.example/x".to_string()),
            ..M3uAccount::default()
        });
        world.streams.push(Stream {
            id: 50,
            name: "CNN HD (D)".to_string(),
            m3u_account: Some(12),
            stream_id: Some(555),
            ..Stream::default()
        });
        let outcome = plan(&world, &scoped()).expect("plan");
        let added: Vec<i64> = outcome
            .entries
            .iter()
            .flat_map(|entry| entry.added.iter().map(|add| add.stream_id))
            .collect();
        assert!(!added.contains(&50));
    }

    #[test]
    fn a_second_account_on_one_host_adds_nothing() {
        let mut world = mirror_world();
        // CNN holds provider A's stream; accounts 6 and 8 share A's host.
        world.streams.push(Stream {
            id: 31,
            name: "CNN HD (A again)".to_string(),
            m3u_account: Some(8),
            stream_id: Some(555),
            ..Stream::default()
        });
        let outcome = plan(&world, &scoped()).expect("plan");
        let cnn = outcome
            .entries
            .iter()
            .find(|entry| entry.channel_id == 100)
            .expect("entry");
        assert_eq!(
            cnn.added
                .iter()
                .map(|add| add.stream_id)
                .collect::<Vec<_>>(),
            vec![20]
        );
    }

    #[test]
    fn no_server_group_means_nothing_to_do() {
        let mut world = mirror_world();
        for account in &mut world.m3u_accounts {
            account.server_group = None;
        }
        let outcome = plan(&world, &scoped()).expect("plan");
        assert!(!outcome.any_grouped_account);
        assert!(outcome.entries.is_empty());
    }

    #[test]
    fn a_stream_id_the_stream_list_no_longer_has_is_planned_but_left_alone() {
        let mut world = mirror_world();
        // CNN holds a row for a stream that is gone; the plugin's join never sees it, so its
        // dry run plans from `[10]` — and its write loop then refuses the channel, because
        // the locked re-read still sees `[10, 98]`. This run does the same: planned, never
        // written, the list left exactly as found.
        world.channels[0].streams = vec![10, 98];
        let outcome = plan(&world, &scoped()).expect("plan");
        assert!(outcome.entries.iter().all(|entry| entry.channel_id != 100));
        let cnn = outcome
            .skipped
            .iter()
            .find(|entry| entry.channel_id == 100)
            .expect("CNN is planned but left alone");
        assert_eq!(cnn.before, vec![10, 98], "the row keeps the real before");
        // The dry run's `after`: the dangling row was never in the join, so the plan is
        // computed from `[10]` alone, exactly as the plugin's dry run computes it.
        assert_eq!(cnn.after, vec![10, 20]);
        assert_eq!(
            cnn.added
                .iter()
                .map(|add| add.stream_id)
                .collect::<Vec<_>>(),
            vec![20]
        );
        // Still `gained`, still in the readout: the plugin's plan counts the channel, only
        // its write refuses it.
        assert_eq!(outcome.stats.gained, 2);
        assert_eq!(outcome.added_count(), 3);
        assert_eq!(outcome.per_account().get(&7), Some(&2));
        assert_eq!(outcome.missing_streams, 1);
    }

    #[test]
    fn a_channel_whose_only_sources_have_no_provider_id_is_no_source() {
        let world = mirror_world();
        let channel = world.channels[0].clone();
        let mut only_custom = world.clone();
        only_custom.channels = vec![Channel {
            streams: vec![99],
            ..channel.clone()
        }];
        let outcome = plan(&only_custom, &scoped()).expect("plan");
        assert_eq!(outcome.stats.no_source, 1);
        assert_eq!(outcome.stats.covered, 0);
    }

    #[test]
    fn a_host_missing_with_no_mirror_counts_as_no_mirror_found() {
        let mut world = mirror_world();
        // A provider stream id only A carries: B's host is missing, and B has no mirror for
        // this id to add.
        world.streams.push(Stream {
            id: 12,
            name: "CNN HD (A only)".to_string(),
            m3u_account: Some(6),
            stream_id: Some(888),
            ..Stream::default()
        });
        world.channels = vec![Channel {
            id: 300,
            name: "KFDA".to_string(),
            streams: vec![12],
            ..Channel::default()
        }];
        let outcome = plan(&world, &scoped()).expect("plan");
        assert_eq!(outcome.stats.no_mirror_found, 1);
        assert!(outcome.entries.is_empty());
    }

    #[tokio::test]
    async fn a_cancelled_apply_is_recorded_cancelled_and_keeps_what_it_did() {
        let world = mirror_world();
        let server = ServerFixture::start(&world).await;
        let store = server.store();
        let context = server.context_for_run(ACTION_MIRROR_APPLY, &store, false);
        let run_id = context.run_id().expect("run id");
        context.cancel();

        let outcome = StreamsApp::new()
            .dispatch(ACTION_MIRROR_APPLY, &context, false)
            .await
            .expect("a cancelled run still reports what it did");
        assert!(outcome.is_cancelled(), "{}", outcome.summary);
        assert!(
            outcome.summary.contains("cancelled by the operator"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("written"), Some(&0));
        assert_eq!(
            server
                .request_count("PATCH", "/api/channels/channels/edit/bulk/")
                .await,
            0,
            "nothing is written"
        );
        let rows = store.rows(run_id).expect("rows");
        assert!(!rows.is_empty());
        assert!(
            rows.iter()
                .all(|row| row.status == "failed" && row.reason.contains("cancelled")),
            "{rows:?}"
        );
    }

    #[tokio::test]
    async fn preview_writes_nothing_and_applied_inserts_the_mirror() {
        let world = mirror_world();
        let server = ServerFixture::start(&world).await;
        let store = server.store();
        let app = StreamsApp::new();

        let preview = app
            .dispatch(
                ACTION_MIRROR_PREVIEW,
                &server.context_for_run(ACTION_MIRROR_PREVIEW, &store, true),
                true,
            )
            .await
            .expect("preview");
        assert!(
            preview.summary.contains("would add 3 mirror stream(s)"),
            "{}",
            preview.summary
        );
        assert_eq!(preview.counts.get("gained"), Some(&2));
        assert_eq!(
            server
                .request_count("PATCH", "/api/channels/channels/edit/bulk/")
                .await,
            0,
            "a preview writes nothing"
        );

        let store = server.store();
        let applied = app
            .dispatch(
                ACTION_MIRROR_APPLY,
                &server.context_for_run(ACTION_MIRROR_APPLY, &store, false),
                false,
            )
            .await
            .expect("apply");
        assert!(
            applied
                .summary
                .starts_with("Added 3 mirror stream(s) to 2 channel(s)"),
            "{}",
            applied.summary
        );
        let bodies = server
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await;
        assert_eq!(bodies.len(), 1, "one bulk write");
        let rows = bodies[0].as_array().expect("an array of rows");
        // Only the two channels that change, and only their stream lists.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["streams"], serde_json::json!([10, 20, 99]));
        assert_eq!(rows[1]["id"], serde_json::json!(300));
    }

    #[tokio::test]
    async fn apply_leaves_a_channel_holding_a_dangling_stream_id_alone() {
        let mut world = mirror_world();
        // CNN holds stream 98, which the stream list no longer has: the plugin's locked
        // re-read refuses the channel, so the apply must not touch it — KFDA still gains.
        world.channels[0].streams = vec![10, 98];
        let server = ServerFixture::start(&world).await;
        let store = server.store();
        let applied = StreamsApp::new()
            .dispatch(
                ACTION_MIRROR_APPLY,
                &server.context_for_run(ACTION_MIRROR_APPLY, &store, false),
                false,
            )
            .await
            .expect("apply");
        assert!(
            applied
                .summary
                .starts_with("Added 2 mirror stream(s) to 1 channel(s)"),
            "{}",
            applied.summary
        );
        assert!(
            applied.summary.contains("1 channel(s) left alone"),
            "{}",
            applied.summary
        );
        let bodies = server
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await;
        assert_eq!(bodies.len(), 1, "one bulk write");
        let rows = bodies[0].as_array().expect("an array of rows");
        // Only KFDA: CNN keeps `[10, 98]` exactly as the run found it.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], serde_json::json!(300));
    }

    #[tokio::test]
    async fn apply_reports_when_no_account_is_in_a_server_group() {
        let mut world = mirror_world();
        for account in &mut world.m3u_accounts {
            account.server_group = None;
        }
        let server = ServerFixture::start(&world).await;
        let store = server.store();
        let outcome = StreamsApp::new()
            .dispatch(
                ACTION_MIRROR_APPLY,
                &server.context_for_run(ACTION_MIRROR_APPLY, &store, false),
                false,
            )
            .await
            .expect("run");
        assert!(
            outcome
                .summary
                .contains("No M3U account is in a server group"),
            "{}",
            outcome.summary
        );
        assert_eq!(
            server
                .request_count("PATCH", "/api/channels/channels/edit/bulk/")
                .await,
            0
        );
    }

    #[tokio::test]
    async fn a_run_with_nothing_to_add_says_why() {
        let mut world = mirror_world();
        // One grouped host only: every channel already holds it (or has nothing to copy).
        world
            .m3u_accounts
            .retain(|account| matches!(account.id, 1 | 6));
        world.channels.retain(|channel| channel.id != 400);
        let server = ServerFixture::start(&world).await;
        let store = server.store();
        let outcome = StreamsApp::new()
            .dispatch(
                ACTION_MIRROR_PREVIEW,
                &server.context_for_run(ACTION_MIRROR_PREVIEW, &store, true),
                true,
            )
            .await
            .expect("run");
        assert!(
            outcome.summary.starts_with("Nothing to add."),
            "{}",
            outcome.summary
        );
        // The plugin's counts, in its own sentence.
        assert!(
            outcome.summary.contains("2 already have every active host"),
            "{}",
            outcome.summary
        );
        assert!(
            outcome.summary.contains("1 have no server-group stream"),
            "{}",
            outcome.summary
        );
        assert_eq!(
            server
                .request_count("PATCH", "/api/channels/channels/edit/bulk/")
                .await,
            0
        );
    }

    #[tokio::test]
    async fn the_report_names_the_account_never_the_provider_host() {
        let world = mirror_world();
        let server = ServerFixture::start(&world).await;
        let store = server.store();
        let context = server.context_for_run(ACTION_MIRROR_PREVIEW, &store, true);
        let outcome = StreamsApp::new()
            .dispatch(ACTION_MIRROR_PREVIEW, &context, true)
            .await
            .expect("run");
        let report = outcome.report_path.expect("a report");
        let text = std::fs::read_to_string(&report).expect("read the report");
        assert!(text.contains("Provider B"), "{text}");
        assert!(text.contains("INACTIVE"), "{text}");
        for host in ["a.example", "b.example", "c.example"] {
            assert!(!text.contains(host), "{host} must never be reported");
        }
        assert!(text.contains("CNN"), "{text}");
    }
}
