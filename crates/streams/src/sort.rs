//! `sort_preview` / `sort_apply`: Stream-Mapparr's `sort_streams_action`, over the quality model.
//!
//! Port of `Plugin.sort_streams_action` (`reference/Stream-Mapparr/Stream-Mapparr/plugin.py:9804`)
//! and the per-channel ordering it calls (`_sort_streams_by_quality` 4739, `_zone_routed_map`
//! 4233, `_order_streams_for_zone` 4282, `_streams_for_channel` 4339, `_split_custom_tail` 9754).
//! The key itself is [`crate::quality`]; what this module adds is what only the action knows:
//!
//!   * which channels (the shared `[scope]`, which is the plugin's Channel Profile + Channel
//!     Groups + mode);
//!   * each channel's CURRENT list, from the channel list itself (`Channel.streams`, the
//!     `channelstream_set` order) rather than a per-channel query;
//!   * zone routing, which is why a sort keeps West feeds on West channels instead of undoing
//!     what a match just wrote (bug-068);
//!   * the custom tail: a hand-made fallback stream (`Could Not Dispatch`) is never ranked and
//!     stays after the provider ones (issue 70);
//!   * only the channels whose ORDER changes are written, as one `Op::Set` on `Field::Streams`.
//!
//! # What a sort can change besides the order
//!
//! Two things the plugin does that a reader does not expect, both kept because they are the
//! plugin's behaviour and both reported:
//!
//!   * **Zone routing DROPS the other zone's streams.** `_order_streams_for_zone` keeps only
//!     WEST streams on a WEST channel and only non-WEST streams everywhere else, rather than
//!     ranking the other zone last: if every East feed fails, a West feed on an East channel
//!     plays a different programme three hours behind. When that leaves nothing, every stream
//!     is kept (a channel with no streams is worse), which is what the plugin warns about.
//!   * **A stream id the stream list no longer has is dropped** from the channel's list
//!     (`Stream.DoesNotExist`, 9957-9959), so the write cleans up a dangling row.
//!
//! # Divergences
//!
//!   * **Country-first ordering is not here yet.** The plugin also partitions a channel's
//!     streams by `restrict_matching_to_country` (`_same_country_ids_for` 4432, bug-158), which
//!     needs the country channel database and the channel→country resolution the match
//!     collector owns. It arrives with that collector (phase 2 step 6); until then a sort
//!     orders by the quality key and the zone, whatever the setting says. With the setting off
//!     — the default — the two agree exactly.
//!   * **The plugin requires a Channel Profile** ("Profile name is required", 9853). The
//!     profile list is the shared `[scope] profiles` here, and an empty one means every
//!     channel, like every other action's scope.
//!   * **One log line per zone-routed channel that had to keep other-zone streams** becomes one
//!     counted warning per run: at 20k channels the per-channel wording is 20k lines.
//!   * **The CSV is the shared channel-operation report**, so a sort writes the same columns
//!     every other channel write does (before list, after list, status, reason) instead of the
//!     plugin's per-stream `tiers` / `throughput_mbps` / `edge_ips` columns. The tiers still
//!     drive the order and are summarised in the report's preamble.

use crate::quality::{self, QualityModel, StreamView, ThroughputSource};
use crate::selection::AccountSelection;
use crate::settings::StreamsSettings;
use anyhow::Result;
use chrono::Utc;
use crosstalk_core::channel_ops::{self, Field, Op, Plan, ReportSpec, Selection, SetValue};
use crosstalk_core::config::ScopeConfig;
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{World, WorldKind, WorldKinds};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use crosstalk_matching::country::{extract_zone, FeedZone};
use crosstalk_matching::normalize::Normalizer;
use std::collections::HashMap;

/// The feed zone of a channel or stream name, as `fuzzy_matcher.extract_zone` answers it.
fn zone_of(name: &str) -> FeedZone {
    extract_zone(name)
}

/// The report's title, without the preview/applied suffix.
const REPORT_TITLE: &str = "Sort Alternate Streams";

/// Everything a run resolves once, before any channel is looked at.
pub struct SortOptions {
    pub model: QualityModel,
    /// `keep_custom_streams`: rank only the provider streams and append the custom ones.
    pub keep_custom_streams: bool,
    /// `ignore_tags`, as `_zone_routed_map` normalises channel names with them.
    pub ignore_tags: Vec<String>,
    /// The normaliser the base-name comparison runs with.
    pub normalizer: Normalizer,
    /// `selected_m3us`, resolved to priorities. Sort never filters with it: a source the list
    /// does not name ranks last (999) rather than being dropped, unlike a match.
    pub accounts: AccountSelection,
}

impl SortOptions {
    /// The options `streams.toml` and the host's `[matching]` describe for this world.
    pub fn new(
        settings: &StreamsSettings,
        world: &World,
        matching: &crosstalk_core::config::MatchingConfig,
    ) -> Self {
        Self {
            model: QualityModel::from_settings(settings),
            keep_custom_streams: settings.keep_custom_streams,
            ignore_tags: settings.ignore_tag_list(),
            normalizer: quality::zone_normalizer(matching),
            accounts: crate::selection::select_accounts(
                &world.m3u_accounts,
                &settings.selected_m3us,
            ),
        }
    }
}

/// One channel's order, before and after.
#[derive(Debug, Clone)]
pub struct ChannelOrder {
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_group: String,
    pub before: Vec<i64>,
    pub after: Vec<i64>,
    /// The zone routing gave this channel, when it gave one.
    pub zone: Option<FeedZone>,
    /// The per-stream tier labels of the AFTER list, in order, as the CSV names them.
    pub tiers: Vec<&'static str>,
    /// Streams zone routing removed from this channel's list.
    pub dropped_by_zone: usize,
}

impl ChannelOrder {
    /// Whether this channel's list would be written.
    ///
    /// Comparison is the whole list, so a zone routing that only REMOVES a stream is a change
    /// even when the surviving order is untouched.
    pub fn changed(&self) -> bool {
        self.before != self.after
    }
}

/// What a sort run resolved for the whole scope.
#[derive(Debug, Clone, Default)]
pub struct SortResult {
    /// In-scope channels.
    pub channels: usize,
    /// Channels with more than one stream: the plugin only ever touched these.
    pub candidates: usize,
    /// One entry per channel with more than one stream, in world order.
    pub orders: Vec<ChannelOrder>,
    /// Channel stream ids the stream list no longer holds: dropped from the rewritten list.
    pub missing_streams: usize,
    /// Channels whose streams were all the other zone's, which therefore kept them.
    pub kept_other_zone: usize,
}

impl SortResult {
    pub fn changed(&self) -> impl Iterator<Item = &ChannelOrder> {
        self.orders.iter().filter(|order| order.changed())
    }

    pub fn changed_count(&self) -> usize {
        self.changed().count()
    }

    pub fn unchanged_count(&self) -> usize {
        self.orders.len() - self.changed_count()
    }

    /// Streams zone routing removed, over every channel.
    pub fn dropped_by_zone(&self) -> usize {
        self.orders.iter().map(|order| order.dropped_by_zone).sum()
    }

    /// Streams the model demoted as content-starved, in the lists it would write.
    pub fn placeholders(&self) -> usize {
        self.orders
            .iter()
            .filter(|order| order.changed())
            .flat_map(|order| order.tiers.iter())
            .filter(|label| **label == quality::CONTENT_STARVED_LABEL)
            .count()
    }
}

/// One channel reference's share of the run: the view the key sorts, the key itself, and the
/// zone the affinity filter reads — all from the one [`orders`] slot lookup.
type ScoredStream<'a> = (StreamView<'a>, quality::QualityKey, FeedZone);

/// `Plugin._zone_routed_map` (4233-4267): which channels should be ordered by feed zone.
///
/// A channel qualifies when it shares a zone-stripped base name with a sibling of a DIFFERENT
/// feed zone ("Starz Encore" and "STARZ Encore (W)"), or when it is itself unmarked — so a lone
/// unmarked channel leads with its East/national feed instead of any West alternates it picked
/// up. A lone MARKED channel is deliberately left out, which keeps brand words harmless: a lone
/// "Key West" is WEST but has no East sibling, so it is never reordered.
///
/// `channels` is the whole scoped set, not one channel: the qualification is a property of the
/// group.
///
/// When no channel carries a marker every channel qualifies through the unmarked rule, so the
/// base names are never normalised at all — the common case (and the whole 20k-channel scale
/// target) skips 20k normaliser runs with the identical result.
pub fn zone_routed(
    channels: &[crosstalk_core::dispatcharr::Channel],
    normalizer: &Normalizer,
    ignore_tags: &[String],
) -> HashMap<i64, FeedZone> {
    let zoned: Vec<(i64, FeedZone)> = channels
        .iter()
        .map(|channel| (channel.id, zone_of(&channel.name)))
        .collect();
    if zoned.iter().all(|(_, zone)| *zone == FeedZone::Default) {
        return zoned.into_iter().collect();
    }
    let tags: Vec<&str> = ignore_tags.iter().map(String::as_str).collect();
    let mut base_zones: HashMap<String, Vec<FeedZone>> = HashMap::new();
    let mut per_channel: Vec<(i64, String, FeedZone)> = Vec::with_capacity(channels.len());
    for (channel, (_, zone)) in channels.iter().zip(zoned) {
        // `_clean_channel_name(...).strip().lower()`
        let base = normalizer
            .normalize_name(&channel.name, &tags)
            .trim()
            .to_lowercase();
        per_channel.push((channel.id, base.clone(), zone));
        let zones = base_zones.entry(base).or_default();
        if !zones.contains(&zone) {
            zones.push(zone);
        }
    }
    per_channel
        .into_iter()
        .filter(|(_, base, zone)| {
            base_zones.get(base).is_some_and(|zones| zones.len() >= 2) || *zone == FeedZone::Default
        })
        .map(|(id, _, zone)| (id, zone))
        .collect()
}

/// `_zone_affinity_rank` (623-628): where a stream of this zone belongs for this channel.
///
/// Both tables exist, and the sort below keeps them, because an unmarked feed ranks WITH East
/// on either side (issue 55). It can only break a tie that zone filtering itself has already
/// resolved, which is why the result is the filtered list in quality order.
pub(crate) fn zone_affinity_rank(channel_zone: FeedZone, stream_zone: FeedZone) -> u8 {
    match channel_zone {
        // West channel: own zone first, East and generic after it.
        FeedZone::West => match stream_zone {
            FeedZone::West => 0,
            _ => 2,
        },
        // Default and East: East and generic first, West after them.
        _ => match stream_zone {
            FeedZone::West => 2,
            _ => 0,
        },
    }
}

/// `_order_streams_for_zone` (4282-4337): drop the other zone, then order by zone affinity.
///
/// Stable, so the quality order survives inside a rank. `keep_all_if_empty` is on, as the
/// plugin's callers pass for a sort: a channel whose every stream is the other zone keeps them
/// rather than going off the air. The plugin's per-channel warning about that becomes one
/// count for the run.
fn order_streams_for_zone<'a>(
    zoned: Vec<(FeedZone, StreamView<'a>)>,
    channel_zone: FeedZone,
    kept_other_zone: &mut usize,
) -> (Vec<StreamView<'a>>, usize) {
    if zoned.is_empty() {
        return (Vec::new(), 0);
    }
    // The zones arrive with the streams: one lookup per stream per RUN (see [`RunCache`]),
    // not two per channel, and `extract_zone` only scans the country tables for a name that
    // carries a marker.
    let own_zone: Vec<(FeedZone, StreamView<'_>)> = zoned
        .iter()
        .copied()
        .filter(|(stream_zone, _)| {
            if channel_zone == FeedZone::West {
                *stream_zone == FeedZone::West
            } else {
                *stream_zone != FeedZone::West
            }
        })
        .collect();
    let dropped = zoned.len() - own_zone.len();
    let kept = if own_zone.is_empty() {
        *kept_other_zone += 1;
        zoned
    } else {
        own_zone
    };
    let mut ranked: Vec<(u8, StreamView<'_>)> = kept
        .into_iter()
        .map(|(stream_zone, stream)| (zone_affinity_rank(channel_zone, stream_zone), stream))
        .collect();
    ranked.sort_by_key(|(rank, _)| *rank);
    (
        ranked.into_iter().map(|(_, stream)| stream).collect(),
        dropped,
    )
}

/// The whole pure plan: every in-scope channel's order before and after.
///
/// Nothing is written and the world is not modified. Built for 20k channels × dozens of
/// streams: the stream index is one hash map over the stream list, each channel's own list is
/// read from the channel row, and each sort is `O(k log k)` for that channel's `k` streams.
pub fn orders(
    world: &World,
    scope: &ScopeConfig,
    options: &SortOptions,
    throughput: &dyn ThroughputSource,
) -> Result<SortResult> {
    world.require(WorldKind::Channels)?;
    // A list is planned from the channel's own ids, and a stream the index cannot resolve is
    // dropped (as the plugin does). Without the slice every id looks unresolvable, so refuse
    // rather than empty every channel's list.
    world.require(WorldKind::Streams)?;

    let scoped = crosstalk_core::scope::scope_channels(world, scope)?;
    let zones = zone_routed(&scoped.channels, &options.normalizer, &options.ignore_tags);
    // One slot per stream: the view, its sort key and its zone, each computed once per RUN
    // and shared by every channel carrying the stream. Every input to the key and the zone
    // (stats, account priority, throughput entry, name) is a property of the stream and the
    // run's constants, never of the channel being sorted — so without this the sort would
    // parse the same `stream_stats` once per channel reference (200k times) instead of once
    // per stream, and one hash lookup per reference serves the view, the key and the zone.
    let mut slots: HashMap<i64, (StreamView<'_>, quality::QualityKey, FeedZone)> =
        HashMap::with_capacity(world.streams.len());
    for stream in &world.streams {
        let view = StreamView {
            id: stream.id,
            name: &stream.name,
            stats: Some(&stream.stream_stats),
            m3u_priority: options.accounts.priority(stream.m3u_account),
            is_custom: stream.is_custom,
        };
        let key = quality::key_of(&view, &options.model, throughput);
        slots.insert(stream.id, (view, key, zone_of(&stream.name)));
    }

    let mut result = SortResult {
        channels: scoped.channels.len(),
        ..SortResult::default()
    };
    for channel in &scoped.channels {
        if channel.streams.len() <= 1 {
            // The plugin's `if len(stream_ids) > 1`: one stream has one order. A channel with
            // no streams is not a sort candidate either.
            continue;
        }
        result.candidates += 1;

        // One hash lookup per channel reference serves the view, the key and the zone; a
        // stream id the stream list no longer holds is dropped (as the plugin does).
        let mut views: Vec<ScoredStream<'_>> = Vec::with_capacity(channel.streams.len());
        for id in &channel.streams {
            match slots.get(id) {
                Some(slot) => views.push(*slot),
                None => result.missing_streams += 1,
            }
        }
        // `_split_custom_tail` (9754-9762): a custom fallback stream is not ranked. With
        // `keep_custom_streams` off the plugin ranks everything and appends nothing.
        let (provider, custom): (Vec<ScoredStream<'_>>, Vec<ScoredStream<'_>>) =
            if options.keep_custom_streams {
                views
                    .iter()
                    .copied()
                    .partition(|(stream, _, _)| !stream.is_custom)
            } else {
                (views, Vec::new())
            };

        let mut kept_other_zone = 0usize;
        let mut dropped = 0usize;
        let zone = zones.get(&channel.id).copied();
        // The plugin's stable `sorted(streams, key=scored)` (4822): keys computed once per
        // run, ties keep the input order.
        let mut scored = provider;
        scored.sort_by_key(|scored| scored.1);
        let zoned: Vec<(FeedZone, StreamView<'_>)> = scored
            .into_iter()
            .map(|(stream, _, zone)| (zone, stream))
            .collect();
        let mut provider: Vec<StreamView<'_>>;
        if let Some(channel_zone) = zone {
            let (ordered, removed) =
                order_streams_for_zone(zoned, channel_zone, &mut kept_other_zone);
            provider = ordered;
            dropped = removed;
        } else {
            provider = zoned.into_iter().map(|(_, stream)| stream).collect();
        }
        result.kept_other_zone += kept_other_zone;
        provider.extend(custom.into_iter().map(|(stream, _, _)| stream));

        let after: Vec<i64> = provider.iter().map(|stream| stream.id).collect();
        let tiers = if after != channel.streams {
            // Only a list that would be written needs its labels: at 20k channels this is the
            // difference between 200k tier decisions and none. The labels come from the keys
            // the order already sorted on, so the report never re-reads the stats.
            provider
                .iter()
                .map(|stream| {
                    slots
                        .get(&stream.id)
                        .map(|(_, key, _)| key.tier_label())
                        .unwrap_or("unknown")
                })
                .collect()
        } else {
            Vec::new()
        };
        result.orders.push(ChannelOrder {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_group: world.group_name(channel.channel_group_id),
            before: channel.streams.clone(),
            after,
            zone,
            tiers,
            dropped_by_zone: dropped,
        });
    }

    if result.kept_other_zone > 0 {
        // The plugin logs one warning per channel here; one counted line is what stays readable
        // at this scale, and the CSV carries the per-channel outcome.
        tracing::warn!(
            "{} channel(s) had no stream marked for their zone; their other-zone streams were \
             kept rather than leaving them with none",
            result.kept_other_zone
        );
    }
    Ok(result)
}

/// The action: preview (write the CSV) or apply (write the orders that changed).
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    let client = context.dispatcharr()?;
    let action_id = format!("streams.sort_{}", if dry_run { "preview" } else { "apply" });
    let scope = context.config().scope.clone();

    // Every slice a sort reads: channels (their own ordered lists), groups (report names),
    // profiles (the scope's profile filter), streams (stats, accounts, custom flag) and the M3U
    // accounts (priority order). No EPG: a sort touches no guide.
    let kinds = WorldKinds::only(WorldKind::Channels)
        | WorldKind::Groups
        | WorldKind::Profiles
        | WorldKind::Streams
        | WorldKind::M3uAccounts;
    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.sort",
        0,
        0,
        "loading Dispatcharr data",
    ));
    let world = client.load_world_with(kinds).await?;

    let mut options = SortOptions::new(settings, &world, &context.config().matching);
    // One clock for the whole run, so every freshness check is against the same instant.
    options.model.now = Utc::now();
    // The store's `stream_health` is the throughput evidence (Stream-Mapparr read its own JSON
    // cache). A context without a store sorts as the plugin does with an empty cache: every
    // stream is "unknown" and the key falls back to resolution and frame rate.
    let throughput: Box<dyn ThroughputSource> = match context.store() {
        Some(store) => Box::new(quality::StoreThroughputSource::load(store)?),
        None => Box::new(quality::MemoryThroughputSource::new()),
    };

    let result = orders(&world, &scope, &options, throughput.as_ref())?;
    if result.channels == 0 {
        return Ok(JobOutcome::new(
            "No channels are in scope. Check the group and profile settings in crosstalk.toml.",
        ));
    }
    if result.candidates == 0 {
        return Ok(JobOutcome::new(
            "No channels found with multiple streams to sort.",
        ));
    }

    // Only the changed channels are named in the operation: a channel whose order is already
    // the sorted one is not a change, and a per-channel value it does not carry is not a row.
    let mut values: HashMap<i64, SetValue> = HashMap::new();
    for order in result.changed() {
        values.insert(order.channel_id, SetValue::Streams(order.after.clone()));
    }

    let plan = if values.is_empty() {
        Plan {
            description: format!(
                "every channel in scope{} — {} channel(s) with more than one stream, all \
                 already in order",
                scope_suffix(&scope),
                result.candidates
            ),
            ..Plan::default()
        }
    } else {
        let operations = vec![Op::Set {
            field: Field::Streams,
            values,
        }];
        channel_ops::plan(&world, &Selection::in_scope(&scope), &operations)?
    };

    let applied = if dry_run || plan.changes.is_empty() {
        None
    } else {
        Some(channel_ops::apply(client, &plan, context, &action_id).await)
    };

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = format!("sort_{}", if dry_run { "preview" } else { "applied" });
    let title = format!(
        "{REPORT_TITLE} {} Report",
        if dry_run { "Preview" } else { "Applied" }
    );
    let spec = ReportSpec::new(app, &schema, &values, &stem, &title)
        .note("Sort key", describe_key(&options.model))
        .note(
            "Throughput",
            if options.model.enable_throughput_sorting {
                format!(
                    "measured tiers, fresh within {} minute(s)",
                    options.model.probe_cache_ttl_minutes
                )
            } else {
                "not used: sorting by resolution and frame rate".to_string()
            },
        )
        .note(
            "Zone routing",
            format!(
                "{} channel(s) zoned, {} stream(s) dropped as the other zone, {} channel(s) \
                 kept other-zone streams",
                result
                    .orders
                    .iter()
                    .filter(|order| order.zone.is_some())
                    .count(),
                result.dropped_by_zone(),
                result.kept_other_zone
            ),
        )
        .note(
            "Already in order",
            format!("{} channel(s)", result.unchanged_count()),
        );
    let report = channel_ops::write_ops_report(context, &spec, &plan, applied.as_ref())?;

    context.record_rows(&channel_ops::run_rows(&plan, applied.as_ref()))?;

    let name = report
        .file_name()
        .map(|name| name.to_string_lossy().to_string());
    csv_report::prune_exports(
        &context.exports_dir(),
        &app.export_prefix(),
        context.config().exports.retention_days,
        name.as_deref(),
    );

    let changed = result.changed_count();
    let mut builder = OutcomeBuilder::new(if dry_run {
        format!("Dry run complete. Would sort {changed} channel(s) with multiple streams.")
    } else {
        format!("Sorted {changed} channel(s) with multiple streams.")
    })
    .count("selected", result.channels as i64)
    .count("candidates", result.candidates as i64)
    .count("changes", plan.changes.len() as i64)
    .count("already_in_order", result.unchanged_count() as i64)
    .count("dropped_by_zone", result.dropped_by_zone() as i64)
    .count("missing_streams", result.missing_streams as i64)
    .count("placeholders", result.placeholders() as i64);
    if let Some(applied) = applied.as_ref() {
        builder = builder
            .count("written", applied.ok as i64)
            .count("failed", applied.failed as i64);
        if applied.failed > 0 {
            builder = builder.note(format!("{} row(s) failed", applied.failed));
        }
    }
    if result.missing_streams > 0 {
        builder = builder.note(format!(
            "{} stream id(s) are no longer in the stream list and were dropped from the \
             rewritten order",
            result.missing_streams
        ));
    }
    // The cancel rule (see crosstalk_core::jobs): whatever landed stays recorded, and the run
    // says it was cancelled rather than claiming a whole sort.
    if let Some(note) = channel_ops::cancelled_note(context, &plan, applied.as_ref()) {
        builder = builder.cancelled().note(note);
    }
    Ok(builder.report(Some(report)).build())
}

/// The key in words, for the report's preamble.
fn describe_key(model: &QualityModel) -> String {
    let first = if model.prioritize_quality {
        "quality tier, then M3U account priority"
    } else {
        "M3U account priority, then quality tier"
    };
    let mut parts = vec![first.to_string()];
    if model.demote_content_starved {
        parts.push(format!(
            "placeholder streams last (below {} kbps at 720p+)",
            model.content_bitrate_floor_kbps
        ));
    }
    if !model.audio_channels_priority.is_empty() {
        parts.push(format!(
            "audio layout {}",
            model.audio_channels_priority.join(", ")
        ));
    }
    if !model.audio_codec_priority.is_empty() {
        parts.push(format!(
            "audio codec {}",
            model.audio_codec_priority.join(", ")
        ));
    }
    parts.push("higher resolution and frame rate".to_string());
    if model.bitrate_sort_step_kbps > 0.0 {
        parts.push(format!(
            "measured bitrate in {} kbps steps",
            model.bitrate_sort_step_kbps
        ));
    }
    parts.join("; ")
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
    use crate::test_support::{
        self, ServerFixture, CHANNEL_ONE_STREAM, CHANNEL_SD_FIRST, CHANNEL_SORTED, FALLBACK_FEED,
        HD_FEED, NO_STATS_FEED, SD_FEED, SLATE_FEED, UHD_FEED, WEST_FEED,
    };
    use crate::{StreamsApp, ACTION_SORT_APPLY, ACTION_SORT_PREVIEW};
    use crosstalk_core::dispatcharr::{Channel, Stream};
    use crosstalk_core::store::{RunStatus, StreamHealth};
    use crosstalk_core::subapp::SubApp;

    fn sort_options(world: &World, settings: &StreamsSettings) -> SortOptions {
        SortOptions::new(
            settings,
            world,
            &crosstalk_core::config::MatchingConfig::default(),
        )
    }

    fn scoped_world() -> World {
        test_support::sort_world()
    }

    fn empty_throughput() -> Box<dyn ThroughputSource> {
        Box::new(quality::MemoryThroughputSource::new())
    }

    #[test]
    fn zone_routing_covers_unmarked_channels_and_zone_siblings_only() {
        let world = scoped_world();
        let normalizer =
            quality::zone_normalizer(&crosstalk_core::config::MatchingConfig::default());
        // `ESPN` and `ESPN2` are unmarked, so both are routed; `Local` is unmarked too.
        let zones = zone_routed(&world.channels, &normalizer, &[]);
        assert_eq!(zones.len(), 3);

        // A sibling pair of a different zone: both routed, each to its own zone.
        let channels = vec![
            Channel {
                id: 1,
                name: "STARZ Encore".to_string(),
                ..Channel::default()
            },
            Channel {
                id: 2,
                name: "Starz Encore (W)".to_string(),
                ..Channel::default()
            },
        ];
        let zones = zone_routed(&channels, &normalizer, &[]);
        assert_eq!(zones.get(&1), Some(&FeedZone::Default));
        assert_eq!(zones.get(&2), Some(&FeedZone::West));

        // A LONE marked channel is left out on purpose: "Key West" is WEST but has no sibling.
        let lone = vec![Channel {
            id: 3,
            name: "Key West".to_string(),
            ..Channel::default()
        }];
        assert!(zone_routed(&lone, &normalizer, &[]).is_empty());

        // A lone MARKED East channel is left out too, and so is a lone unmarked one...
        let east = vec![Channel {
            id: 4,
            name: "HBO East".to_string(),
            ..Channel::default()
        }];
        assert!(zone_routed(&east, &normalizer, &[]).is_empty());
        // ...wait: an unmarked channel IS routed, see the first assertion. An East one is not.
        let default_only = vec![Channel {
            id: 5,
            name: "Cinemax".to_string(),
            ..Channel::default()
        }];
        assert_eq!(
            zone_routed(&default_only, &normalizer, &[]).get(&5),
            Some(&FeedZone::Default)
        );
    }

    #[test]
    fn the_order_puts_the_best_picture_first_and_the_fallback_last() {
        let world = scoped_world();
        let settings = StreamsSettings::default();
        let options = sort_options(&world, &settings);
        let result = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect("orders");

        assert_eq!(result.channels, 3);
        assert_eq!(
            result.candidates, 2,
            "a single-stream channel is not a candidate"
        );

        let first = result
            .orders
            .iter()
            .find(|order| order.channel_id == CHANNEL_SD_FIRST)
            .expect("the unsorted channel");
        // 4K, HD, SD, the stream with no stats, then the slate (1080p but demoted for
        // carrying 190 kbps), and the custom fallback last. The West feed was dropped: the
        // channel is unmarked, which zone routing treats as the East feed.
        assert_eq!(
            first.after,
            vec![
                UHD_FEED,
                HD_FEED,
                SD_FEED,
                NO_STATS_FEED,
                SLATE_FEED,
                FALLBACK_FEED
            ]
        );
        assert_eq!(first.zone, Some(FeedZone::Default));
        assert_eq!(first.dropped_by_zone, 1);
        assert!(first.changed());

        let sorted = result
            .orders
            .iter()
            .find(|order| order.channel_id == CHANNEL_SORTED)
            .expect("the sorted channel");
        assert!(!sorted.changed(), "{:?}", sorted.after);
        assert!(
            sorted.tiers.is_empty(),
            "an unchanged list carries no labels"
        );

        assert_eq!(result.changed_count(), 1);
        assert_eq!(result.unchanged_count(), 1);
        assert_eq!(result.dropped_by_zone(), 1);
    }

    #[test]
    fn zone_routing_drops_the_other_zone_and_counts_a_channel_left_with_none() {
        let mut world = scoped_world();
        // A West channel holding only non-West streams: the plugin keeps them rather than
        // leaving the channel empty, and says so once per run.
        world.channels.push(Channel {
            id: 9,
            name: "ESPN West".to_string(),
            streams: vec![SD_FEED, HD_FEED],
            ..Channel::default()
        });
        // A sibling so the West channel is zone-routed even without the DEFAULT rule.
        world.channels.push(Channel {
            id: 10,
            name: "ESPN".to_string(),
            streams: vec![SD_FEED, HD_FEED],
            ..Channel::default()
        });
        let options = sort_options(&world, &StreamsSettings::default());
        let result = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect("orders");
        let west = result
            .orders
            .iter()
            .find(|order| order.channel_id == 9)
            .expect("the west channel");
        assert_eq!(west.zone, Some(FeedZone::West));
        assert_eq!(west.after.len(), 2, "nothing of the West zone to keep");
        assert_eq!(west.dropped_by_zone, 2);
        assert_eq!(result.kept_other_zone, 1);

        // An East channel and its West sibling: the West stream is dropped from the East one.
        let mut world = scoped_world();
        world.streams.push(Stream {
            id: WEST_FEED,
            name: "ESPN West".to_string(),
            ..Stream::default()
        });
        world.channels = vec![
            Channel {
                id: 1,
                name: "ESPN".to_string(),
                streams: vec![HD_FEED, WEST_FEED],
                ..Channel::default()
            },
            Channel {
                id: 2,
                name: "ESPN West".to_string(),
                streams: vec![HD_FEED, WEST_FEED],
                ..Channel::default()
            },
        ];
        let options = sort_options(&world, &StreamsSettings::default());
        let result = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect("orders");
        let east = result
            .orders
            .iter()
            .find(|order| order.channel_id == 1)
            .unwrap();
        assert_eq!(east.after, vec![HD_FEED]);
        let west = result
            .orders
            .iter()
            .find(|order| order.channel_id == 2)
            .unwrap();
        assert_eq!(west.after, vec![WEST_FEED]);
    }

    #[test]
    fn the_custom_tail_is_kept_out_of_the_ranking_unless_the_setting_says_otherwise() {
        let world = scoped_world();
        let mut settings = StreamsSettings::default();
        let options = sort_options(&world, &settings);
        let result = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect("orders");
        let first = result
            .orders
            .iter()
            .find(|order| order.channel_id == CHANNEL_SD_FIRST)
            .unwrap();
        assert_eq!(first.after.last(), Some(&FALLBACK_FEED));

        // Issue 70's switch: with it off the custom stream is ranked like any other, which
        // puts it with the other statless streams instead of last.
        settings.keep_custom_streams = false;
        let options = sort_options(&world, &settings);
        let result = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect("orders");
        let first = result
            .orders
            .iter()
            .find(|order| order.channel_id == CHANNEL_SD_FIRST)
            .unwrap();
        assert!(
            first.after.len() == 6,
            "the fallback is ranked with the rest: {:?}",
            first.after
        );
    }

    #[test]
    fn a_stream_id_the_world_no_longer_holds_is_dropped_from_the_written_list() {
        let mut world = scoped_world();
        // The channel still points at a stream the stream list does not carry: the plugin
        // skips it (`Stream.DoesNotExist`) and the rewrite cleans it up.
        world.channels[0].streams.push(999);
        let options = sort_options(&world, &StreamsSettings::default());
        let result = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect("orders");
        assert_eq!(result.missing_streams, 1);
        let first = result
            .orders
            .iter()
            .find(|o| o.channel_id == CHANNEL_SD_FIRST)
            .unwrap();
        assert!(!first.after.contains(&999));
    }

    #[test]
    fn m3u_priority_orders_two_equal_streams_by_account() {
        let mut world = scoped_world();
        // The same picture on two accounts: only the account priority can separate them.
        world.streams.push(Stream {
            id: 30,
            name: "ESPN Copy".to_string(),
            m3u_account: Some(test_support::PROVIDER_B),
            stream_stats: serde_json::json!({"width": 1280, "height": 720, "source_fps": 50}),
            ..Stream::default()
        });
        world.streams.push(Stream {
            id: 31,
            name: "ESPN Copy".to_string(),
            m3u_account: Some(test_support::PROVIDER_A),
            stream_stats: serde_json::json!({"width": 1280, "height": 720, "source_fps": 50}),
            ..Stream::default()
        });
        world.channels = vec![Channel {
            id: 1,
            name: "ESPN".to_string(),
            streams: vec![30, 31],
            ..Channel::default()
        }];
        let mut settings = StreamsSettings {
            enable_throughput_sorting: false,
            prioritize_quality: false,
            // Backup Provider first, so stream 30 leads even though 31 is on the other account.
            selected_m3us: "Backup Provider, *".to_string(),
            ..StreamsSettings::default()
        };
        let options = sort_options(&world, &settings);
        let result = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect("orders");
        assert_eq!(result.orders[0].after, vec![30, 31]);

        // Flipped, the other account leads. Nothing is dropped: sort ranks unlisted sources
        // last rather than filtering them (unlike a match).
        settings.selected_m3us = "Main Provider, *".to_string();
        let options = sort_options(&world, &settings);
        let result = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect("orders");
        assert_eq!(result.orders[0].after, vec![31, 30]);
    }

    #[test]
    fn a_sort_needs_the_stream_list_and_refuses_a_partial_world() {
        let mut world = scoped_world();
        world.loaded = crosstalk_core::dispatcharr::WorldKinds::only(
            crosstalk_core::dispatcharr::WorldKind::Channels,
        );
        let options = sort_options(&world, &StreamsSettings::default());
        let error = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect_err("a world without streams cannot be sorted");
        assert!(format!("{error}").contains("Streams"), "{error}");
    }

    #[tokio::test]
    async fn preview_writes_the_report_and_changes_nothing() {
        let world = scoped_world();
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run(ACTION_SORT_PREVIEW, &store, true);

        let outcome = StreamsApp::new()
            .run(ACTION_SORT_PREVIEW, &context, true)
            .await
            .expect("preview");
        assert!(
            outcome.summary.contains("Would sort 1 channel(s)"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("candidates"), Some(&2));
        assert_eq!(outcome.counts.get("already_in_order"), Some(&1));
        assert_eq!(outcome.counts.get("dropped_by_zone"), Some(&1));
        assert!(outcome.report_path.is_some());
        assert!(
            fixture
                .bodies("PATCH", "/api/channels/channels/edit/bulk/")
                .await
                .is_empty(),
            "a preview writes nothing"
        );

        // The report is the shared channel-operation CSV, so a reader of any write finds the
        // same columns; the `after` value is the ordered list.
        let report = std::fs::read_to_string(outcome.report_path.unwrap()).expect("report");
        assert!(report.contains("streams"), "{report}");
        // The `after` value prints stream NAMES when the run loaded them, so the order is
        // readable without a second lookup.
        assert!(report.contains("ESPN 4K, ESPN HD, ESPN SD"), "{report}");
        assert!(report.contains("Sort key"), "{report}");
    }

    #[tokio::test]
    async fn a_cancelled_apply_is_recorded_cancelled_and_keeps_what_it_did() {
        // The cancel rule (see crosstalk_core::jobs): a cancelled run keeps its counts, its
        // report and its rows, and is never recorded as a success — phase 1 recorded a
        // half-written apply as succeeded, which is what this replaces.
        let world = scoped_world();
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run(ACTION_SORT_APPLY, &store, false);
        let run_id = context.run_id().expect("run id");
        context.cancel();

        let outcome = StreamsApp::new()
            .run(ACTION_SORT_APPLY, &context, false)
            .await
            .expect("a cancelled run still reports what it did");
        assert!(outcome.is_cancelled(), "{}", outcome.summary);
        assert!(
            outcome.summary.contains("cancelled by the operator"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("written"), Some(&0));
        assert!(
            outcome.report_path.is_some(),
            "the report is what says which rows were left unwritten"
        );
        assert!(fixture
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await
            .is_empty());
        let rows = store.rows(run_id).expect("rows");
        assert!(
            !rows.is_empty(),
            "the planned rows are the record of the cancel"
        );
        assert!(
            rows.iter()
                .all(|row| row.status == "failed" && row.reason.contains("cancelled")),
            "{rows:?}"
        );
    }

    #[tokio::test]
    async fn apply_writes_only_the_channels_whose_order_changed() {
        let world = scoped_world();
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run(ACTION_SORT_APPLY, &store, false);

        let outcome = StreamsApp::new()
            .run(ACTION_SORT_APPLY, &context, false)
            .await
            .expect("apply");
        assert!(
            outcome.summary.contains("Sorted 1 channel(s)"),
            "{}",
            outcome.summary
        );
        assert_eq!(outcome.counts.get("written"), Some(&1));

        let bodies = fixture
            .bodies("PATCH", "/api/channels/channels/edit/bulk/")
            .await;
        assert_eq!(bodies.len(), 1, "one batched write");
        let rows = bodies[0].as_array().expect("rows");
        assert_eq!(
            rows.len(),
            1,
            "only the changed channel is written: {rows:?}"
        );
        assert_eq!(rows[0]["id"], CHANNEL_SD_FIRST);
        assert_eq!(
            rows[0]["streams"],
            serde_json::json!([
                UHD_FEED,
                HD_FEED,
                SD_FEED,
                NO_STATS_FEED,
                SLATE_FEED,
                FALLBACK_FEED
            ])
        );

        // The run rows are the channel-operation rows, with the before and after lists.
        let run_id = context.run_id().expect("run");
        let rows = store.rows(run_id).expect("rows");
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].field, "streams");
        assert_eq!(rows[0].status, "ok");
        assert_eq!(
            rows[0].before_value.as_deref(),
            Some(
                format!("{SD_FEED},{WEST_FEED},{FALLBACK_FEED},{NO_STATS_FEED},{SLATE_FEED},{HD_FEED},{UHD_FEED}")
                    .as_str()
            )
        );
        assert_eq!(
            rows[0].after_value.as_deref(),
            Some(
                format!(
                    "{UHD_FEED},{HD_FEED},{SD_FEED},{NO_STATS_FEED},{SLATE_FEED},{FALLBACK_FEED}"
                )
                .as_str()
            )
        );
        let run = store.run(run_id).expect("run record").expect("recorded");
        assert_eq!(run.status, RunStatus::Running);
    }

    #[tokio::test]
    async fn apply_sorts_on_the_stores_fresh_measurements_and_ignores_stale_ones() {
        // The store's `stream_health` is the throughput evidence. A fresh insufficient
        // measurement demotes the 4K feed; a stale one is "unknown" and changes nothing.
        let world = scoped_world();
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let now = Utc::now();
        let row = |stream_id: i64, mbps: Option<f64>, seconds_ago: i64| StreamHealth {
            stream_id,
            probed_at: Some(
                (now - chrono::Duration::seconds(seconds_ago))
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            ),
            class: "alive".to_string(),
            error_type: None,
            throughput_mbps: mbps,
            throughput_at: Some(
                (now - chrono::Duration::seconds(seconds_ago))
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            ),
            edge_ip: None,
            consecutive_failures: 0,
        };
        // The 4K feed delivers 1 Mbit/s against a nominal ~25: insufficient, so it drops to
        // behind the HD feed even though its picture is bigger.
        store
            .upsert_stream_health(&row(UHD_FEED, Some(1.0), 60))
            .expect("row");
        store
            .upsert_stream_health(&row(HD_FEED, Some(30.0), 60))
            .expect("row");

        let context = fixture.context_for_run(ACTION_SORT_PREVIEW, &store, true);
        let outcome = StreamsApp::new()
            .run(ACTION_SORT_PREVIEW, &context, true)
            .await
            .expect("preview");
        assert_eq!(outcome.counts.get("candidates"), Some(&2));

        let run_id = context.run_id().expect("run");
        let rows = store.rows(run_id).expect("rows");
        let first = rows
            .iter()
            .find(|row| row.channel_id == CHANNEL_SD_FIRST)
            .expect("the changed channel");
        let after = first.after_value.as_deref().expect("after");
        let order: Vec<i64> = after.split(',').map(|id| id.parse().expect("id")).collect();
        let position = |id: i64| order.iter().position(|candidate| *candidate == id);
        assert_eq!(
            position(HD_FEED),
            Some(0),
            "the healthy feed leads: {after}"
        );
        assert_eq!(
            position(SD_FEED),
            Some(1),
            "so does its resolution tier: {after}"
        );
        assert!(
            position(UHD_FEED) > position(SD_FEED),
            "the insufficient 4K feed is demoted behind it: {after}"
        );

        // The same measurement, stale: tier "unknown", so the 4K feed leads again. The first
        // run is finished first: the store allows one running run per app.
        store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some("preview"),
                &serde_json::json!({}),
                None,
            )
            .expect("finish");
        store
            .upsert_stream_health(&row(UHD_FEED, Some(1.0), 7200))
            .expect("row");
        let context = fixture.context_for_run(ACTION_SORT_PREVIEW, &store, true);
        StreamsApp::new()
            .run(ACTION_SORT_PREVIEW, &context, true)
            .await
            .expect("preview");
        let run_id = context.run_id().expect("run");
        let rows = store.rows(run_id).expect("rows");
        let first = rows
            .iter()
            .find(|row| row.channel_id == CHANNEL_SD_FIRST)
            .expect("the changed channel");
        let after = first.after_value.as_deref().expect("after");
        let order: Vec<i64> = after.split(',').map(|id| id.parse().expect("id")).collect();
        let position = |id: i64| order.iter().position(|candidate| *candidate == id);
        assert_eq!(
            position(HD_FEED),
            Some(0),
            "the fresh healthy feed still leads: {after}"
        );
        assert!(
            position(UHD_FEED) < position(SD_FEED),
            "a stale measurement must not demote the 4K feed: {after}"
        );
    }

    #[tokio::test]
    async fn one_stream_channels_are_not_sorted_and_say_so() {
        let mut world = scoped_world();
        world
            .channels
            .retain(|channel| channel.id == CHANNEL_ONE_STREAM);
        let fixture = ServerFixture::start(&world).await;
        let store = fixture.store();
        let context = fixture.context_for_run(ACTION_SORT_PREVIEW, &store, true);
        let outcome = StreamsApp::new()
            .run(ACTION_SORT_PREVIEW, &context, true)
            .await
            .expect("preview");
        assert!(
            outcome
                .summary
                .contains("No channels found with multiple streams to sort"),
            "{}",
            outcome.summary
        );
    }

    #[test]
    fn only_in_scope_channels_are_sorted() {
        // TVE holds the two multi-stream channels; Cable holds the single-stream one.
        let world = scoped_world();
        let options = sort_options(&world, &StreamsSettings::default());
        let scope = ScopeConfig {
            groups: "Cable".to_string(),
            ..ScopeConfig::default()
        };
        let result = orders(&world, &scope, &options, empty_throughput().as_ref()).expect("orders");
        assert_eq!(result.channels, 1);
        assert_eq!(result.candidates, 0, "the one Cable channel has one stream");
        assert!(result.orders.is_empty());

        let scope = ScopeConfig {
            groups: "TVE".to_string(),
            ..ScopeConfig::default()
        };
        let result = orders(&world, &scope, &options, empty_throughput().as_ref()).expect("orders");
        assert_eq!(result.channels, 2);
        assert_eq!(result.candidates, 2);
        assert_eq!(result.changed_count(), 1);
    }

    /// A synthetic world of `channels` × 10 streams, for the scale tests below.
    fn synthetic_world(channels: usize) -> World {
        let per_channel = 10i64;
        let mut world = World {
            m3u_accounts: test_support::accounts(),
            ..World::default()
        };
        let shapes = [
            serde_json::json!({"width": 3840, "height": 2160, "source_fps": 50,
                               "video_bitrate": 9000}),
            serde_json::json!({"width": 1920, "height": 1080, "source_fps": 60,
                               "video_bitrate": 2656}),
            serde_json::json!({"width": 1280, "height": 720, "source_fps": 50,
                               "video_bitrate": 1500}),
            serde_json::json!({"resolution": "1920x1080", "source_fps": 50}),
            serde_json::json!({}),
        ];
        for index in 0..channels {
            let channel_id = index as i64 + 1;
            let mut ids = Vec::with_capacity(per_channel as usize);
            for offset in 0..per_channel {
                let stream_id = channel_id * 100 + offset;
                ids.push(stream_id);
                world.streams.push(Stream {
                    id: stream_id,
                    name: format!("Channel {index} feed {offset}"),
                    m3u_account: Some(1 + offset % 3),
                    is_custom: offset == per_channel - 1,
                    stream_stats: shapes[(index + offset as usize) % shapes.len()].clone(),
                    ..Stream::default()
                });
            }
            world.channels.push(Channel {
                id: channel_id,
                name: format!("Channel {index}"),
                channel_group_id: None,
                streams: ids,
                ..Channel::default()
            });
        }
        world
    }

    /// The scale guard the suite always runs: a few hundred channels, so an accidental
    /// `O(channels × streams)` loop (a scan of the stream list per channel) shows up as a
    /// timeout rather than as a slow production run.
    #[test]
    fn a_thousand_channels_of_ten_streams_plan_quickly() {
        let world = synthetic_world(1_000);
        let options = sort_options(&world, &StreamsSettings::default());
        let started = std::time::Instant::now();
        let result = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect("orders");
        let elapsed = started.elapsed();
        assert_eq!(result.candidates, 1_000);
        assert_eq!(result.orders.len(), 1_000);
        // Generous: this runs in a DEBUG build. A quadratic plan at this size takes minutes.
        let bound = std::time::Duration::from_secs(60);
        assert!(
            elapsed < bound,
            "planning {} streams took {elapsed:?}, over the {bound:?} bound",
            world.streams.len()
        );
        eprintln!(
            "sort bench (guard): 1000 channels, {} streams, {:?}",
            world.streams.len(),
            elapsed
        );
    }

    /// The plan's own scale target: 20k channels × 10 streams.
    ///
    /// Ignored because even a DEBUG build pays ~1.4s for it (a release build ~0.1s): the
    /// per-stream zone regexes used to dominate, and debug builds of `fancy_regex` are slow.
    /// What keeps it fast is doing every per-stream decision once per stream per run — the
    /// quality key and the zone are cached by stream id and shared by every channel carrying
    /// the stream — and skipping the name normaliser entirely when no channel carries a zone
    /// marker. Run it with:
    ///
    /// ```text
    /// cargo test --release -p crosstalk-streams --lib -- --ignored sorting_twenty_thousand
    /// ```
    ///
    /// The bound is what catches an accidental `O(channels × streams)` loop: an inner scan of
    /// the 200k-stream list per channel is 4·10⁹ comparisons, which takes minutes even in
    /// release.
    #[test]
    #[ignore = "20k x 10 streams: ~1.4s in a debug build, ~0.1s in release"]
    fn sorting_twenty_thousand_channels_of_ten_streams_stays_fast() {
        let world = synthetic_world(20_000);
        assert_eq!(world.streams.len(), 200_000);
        let options = sort_options(&world, &StreamsSettings::default());
        let started = std::time::Instant::now();
        let result = orders(
            &world,
            &ScopeConfig::default(),
            &options,
            empty_throughput().as_ref(),
        )
        .expect("orders");
        let elapsed = started.elapsed();
        assert_eq!(result.candidates, 20_000);
        assert_eq!(result.orders.len(), 20_000);
        let bound = std::time::Duration::from_secs(120);
        assert!(
            elapsed < bound,
            "planning {} streams took {elapsed:?}, over the {bound:?} bound",
            world.streams.len()
        );
        eprintln!(
            "sort bench: 20k channels, {} streams, {} changed, {:?}",
            world.streams.len(),
            result.changed_count(),
            elapsed
        );
    }
}
