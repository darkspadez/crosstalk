//! `visibility_preview` / `visibility_apply`: Stream-Mapparr's `manage_channel_visibility_action`.
//!
//! Port of `Plugin.manage_channel_visibility_action`
//! (`reference/Stream-Mapparr/Stream-Mapparr/plugin.py:10175`), which is short and does one
//! thing: **disable every channel in the profile, then enable the first channel of each name
//! group that has at least one stream and is not attached to another channel** (the plugin's
//! own docstring). It exists because a lineup with several feeds per programme shows the same
//! programme several times in the guide; disabling the duplicates in the profile is what makes
//! the guide show one entry per programme.
//!
//! The end state is the whole behaviour, and that is what this module computes:
//!
//! ```text
//! for every name group of the in-scope channels, in `_sort_channels_by_priority` order:
//!     the first channel with >= 1 stream that is not attached  -> enabled
//!     every other channel in the group                          -> disabled
//! ```
//!
//! Two properties of the plugin matter for a port:
//!
//!   * **the grouping is Match & Assign's** (one identity family key per group), not a
//!     copy: with the country filter on, two countries never share a group, and
//!     collapsing them would enable one country's channel and disable the other's
//!     (bug-158 review I1). The grouping is reused here through
//!     [`crate::matching::build_groups`], the same function a match run groups with; step
//!     9 keys it by identity family, so visibility and match group the same channels
//!     together.
//!   * **the channel set is the profile(s) loaded by the plugin's own load step**, which is the
//!     channels ENABLED in the selected profiles. So a channel a previous run disabled is not
//!     in the set any more and cannot be re-enabled by a later run: crosstalk reads the same
//!     set (the shared `[scope]` profiles filter — see [`crosstalk_core::scope`]) and gets the
//!     same result.
//!
//! # The write: the diff, not the two bulk updates
//!
//! The plugin issues two `ChannelProfileMembership.objects.filter(...).update(enabled=...)`
//! statements, one per step, so every channel in scope is written whether or not it changed.
//! Crosstalk writes the END STATE as per-channel [`Field::Membership`] values through
//! [`channel_ops`], which records a skip for a channel already holding the value it asks for —
//! so the same end state is reached with a write only where a membership actually moves, and
//! a preview is a pure read of the same function.
//!
//! # Profiles: which one is written
//!
//! Stream-Mapparr's `profile_name` is a comma-separated list. `load_process_channels_action`
//! resolves it into `profile_ids` and keeps **the first** (`profile_id = profile_ids[0]`,
//! plugin.py:7596), which is the only one the visibility action ever writes: its two updates
//! filter `channel_profile_id=profile_id`. The rest of the list still widens the channel set,
//! because that is the filter the load step used (`channel_profile_id__in=profile_ids`).
//!
//! crosstalk does the same selection: the FIRST profile named in `[scope] profiles` that
//! exists is the one whose membership is written, and every named profile contributes
//! channels. A winner enabled in a second profile but not in the first is the one case where
//! the plugin's "enable" step does anything, and crosstalk **withholds** that enable (see the
//! divergences below). With a single profile (the plugin's own default shape) the enable step
//! is a no-op and this action only ever disables, as the plugin's does.
//!
//! A scope with no profile named is refused with the plugin's own words ("Profile name is
//! required"), because there is no membership to write and "every profile" would be a
//! different, much broader action.
//!
//! # `attached_channel_id`
//!
//! The plugin skips a channel with `attached_channel_id` set, because an attached channel is
//! not the channel a guide plays. Dispatcharr's own `Channel` model has no such column
//! (`apps/channels/models.py`), and the serializer never sends the key, so on a stock install
//! every channel is unattached and the rule never fires — which is exactly what
//! [`Channel::attached_channel_id`] being `None` says. The field is read anyway, so a build
//! that carries it gets the plugin's rule.
//!
//! # Divergences
//!
//!   * The plugin reads the channel set from its `processed_data` file, a snapshot of the last
//!     Load/Process Channels run, and refuses to run at all when that file is missing. A
//!     crosstalk run reads Dispatcharr live, so a channel created since the last load takes
//!     part — same semantics, fresher input.
//!   * The plugin's `ignore_tags` and `ignore_*` flags come from that same snapshot; here they
//!     are `streams.toml`'s `ignore_tags` and `[matching]`'s four tag flags (the live values,
//!     which is what the plugin's own bug-139 fix wanted).
//!   * The plugin reports `Enabled N of M channels` where N counts the winners it *tried* to
//!     enable, including a channel whose membership row only exists in another profile. The
//!     report here counts the winners and the membership writes separately.
//!   * **A winner that is not enabled in the host profile is never enabled.** The plugin's
//!     `update(enabled=True)` reaches an existing (disabled) row and enables it, but cannot
//!     reach a channel with NO row there, which stays hidden. Over REST the two look the same
//!     (a profile lists only its enabled channels), and the only write, `PATCH
//!     .../bulk-update/`, CREATES a missing row — which would put a channel into a profile the
//!     operator curated without it (a `start_empty` profile, or channels created for chosen
//!     profiles only: `apps/channels/api_views.py` and `signals.py` both leave rows out). So
//!     crosstalk withholds every such enable, reports the channels in the preamble and the
//!     `enable_withheld` count, and only ever disables: its writes are a subset of the
//!     plugin's. The bridge (an ORM `update()`) is what restores the plugin's enable step
//!     exactly. A rowless loser agrees on `False` either way, and the plan writes nothing for
//!     it (it already reads as disabled).

use crate::matching::{self, MatchOptions};
use crate::settings::StreamsSettings;
use anyhow::{bail, Result};
use crosstalk_core::channel_ops::{self, Field, Op, ReportSpec, Selection, SetValue};
use crosstalk_core::config::{MatchingConfig, ScopeConfig};
use crosstalk_core::csv_report;
use crosstalk_core::dispatcharr::{ChannelProfile, World, WorldKind, WorldKinds};
use crosstalk_core::jobs::JobContext;
use crosstalk_core::scope::scope_channels;
use crosstalk_core::subapp::{JobOutcome, OutcomeBuilder, SubApp};
use crosstalk_core::wildcard::parse_tokens;
use std::collections::{HashMap, HashSet};

/// The report's title, without the preview/applied suffix.
const REPORT_TITLE: &str = "Manage Channel Visibility";

/// What a visibility run decides. Nothing is written.
#[derive(Debug, Clone, Default)]
pub struct VisibilityOutcome {
    /// The profile whose membership is written: the first named one that exists.
    pub profile_id: i64,
    pub profile_name: String,
    /// In-scope channels.
    pub channels: usize,
    /// Name groups the channels fell into.
    pub groups: usize,
    /// The channels the plugin's latch picks, one per name group, in group order. A winner
    /// in [`Self::withheld`] is not enabled by this run.
    pub winners: Vec<i64>,
    /// Winners that are not enabled in the host profile right now: the plugin would enable
    /// them (when a disabled row exists), crosstalk leaves them as they are, because the REST
    /// write would create a row the plugin cannot. Their end state is `false`.
    pub withheld: Vec<i64>,
    /// In-scope channels that are enabled in the host profile right now.
    pub enabled_before: usize,
    /// Channels left out of the winner race because `attached_channel_id` is set.
    pub attached_skipped: usize,
    /// Name groups the end state leaves with no enabled channel: every channel in one has no
    /// stream, or is attached to another channel, and they all go dark (the plugin's latch
    /// never fires either).
    pub groups_without_winner: usize,
    /// The end state, one entry per in-scope channel: enabled (`true`) or disabled.
    pub end_state: HashMap<i64, bool>,
    /// The scope in words, for the report preamble.
    pub description: String,
}

impl VisibilityOutcome {
    /// The channels the end state disables (including withheld winners, which stay off).
    pub fn disabled(&self) -> usize {
        self.end_state.values().filter(|enabled| !**enabled).count()
    }

    /// The channels the end state leaves enabled: the winners minus the withheld ones.
    pub fn enabled(&self) -> usize {
        self.end_state.values().filter(|enabled| **enabled).count()
    }

    /// The per-channel membership values to write: the end state minus the withheld winners,
    /// which get no value (so no row and no write) and are reported instead.
    pub fn values(&self) -> HashMap<i64, SetValue> {
        let withheld: HashSet<i64> = self.withheld.iter().copied().collect();
        self.end_state
            .iter()
            .filter(|(channel_id, _)| !withheld.contains(channel_id))
            .map(|(channel_id, enabled)| (*channel_id, SetValue::Flag(*enabled)))
            .collect()
    }

    /// The plugin's completion sentence (`10273`).
    pub fn summary(&self) -> String {
        format!(
            "Visibility managed. Enabled {} of {} channels.",
            self.winners.len(),
            self.channels
        )
    }
}

/// The profile whose membership a visibility run writes: Stream-Mapparr's `profile_ids[0]`.
///
/// Resolution is the scope's own: names are trimmed and compared case-insensitively, and the
/// FIRST name that matches is the host — so `[scope] profiles = "Kids, All"` writes `Kids`
/// while both profiles contribute channels, exactly as `profile_id = profile_ids[0]` does.
/// A scope that names nothing is refused with the plugin's wording ("Profile name is
/// required"), because the action's whole subject is one profile's membership.
pub fn host_profile<'a>(world: &'a World, scope: &ScopeConfig) -> Result<&'a ChannelProfile> {
    let wanted = parse_tokens(&scope.profiles);
    if wanted.is_empty() {
        bail!(
            "Profile name is required: name the Channel Profile whose visibility this should \
             manage in [scope] profiles. Stream-Mapparr refuses without one too."
        );
    }
    for name in &wanted {
        if let Some(profile) = world
            .profiles
            .iter()
            .find(|profile| profile.name.trim().eq_ignore_ascii_case(name.trim()))
        {
            return Ok(profile);
        }
    }
    let available: Vec<&str> = world
        .profiles
        .iter()
        .map(|profile| profile.name.as_str())
        .collect();
    bail!(
        "None of the specified Channel Profiles were found: {}. Available profiles: {}",
        wanted.join(", "),
        if available.is_empty() {
            "(none)".to_string()
        } else {
            available.join(", ")
        }
    )
}

/// Plan a visibility run: the scope, the profile, the groups, the end state.
///
/// Pure: nothing is written and the world is not modified. The grouping and the priority
/// order come from [`crate::matching`], so the groups a visibility run enables one channel in
/// are exactly the groups a match run assigns streams to.
pub fn plan(
    world: &World,
    scope: &ScopeConfig,
    settings: &StreamsSettings,
    matching_config: &MatchingConfig,
) -> Result<VisibilityOutcome> {
    plan_with_options(world, scope, &MatchOptions::new(settings, matching_config))
}

/// [`plan`], with the match options supplied instead of resolved from settings.
///
/// The options carry the grouping's inputs: the normaliser (its `[matching]` tag flags), the
/// ignore tags, the channel database and the country filter. A run resolves them from the
/// settings; the parity harness hands over a fixture database, exactly as
/// [`crate::matching::MatchOptions::with_database`] lets a match harness.
pub fn plan_with_options(
    world: &World,
    scope: &ScopeConfig,
    options: &MatchOptions,
) -> Result<VisibilityOutcome> {
    world.require(WorldKind::Channels)?;
    world.require(WorldKind::Profiles)?;

    let host = host_profile(world, scope)?;
    let scoped = scope_channels(world, scope)?;

    let group_names: HashMap<i64, String> = world
        .groups
        .iter()
        .map(|group| (group.id, group.name.clone()))
        .collect();
    let tags: Vec<String> = options.ignore_tags.clone();
    let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
    // One grouping for match and visibility (step 9): the identity family key — the same
    // call Match & Assign makes, through the same function. The normalizer is the
    // unified engine's (one normalizer), not the retired SM profile's.
    let groups = matching::build_groups(
        &scoped.channels,
        &options.database,
        options.engine.normalizer(),
        &tag_refs,
        options.restrict_matching_to_country,
        &group_names,
        &options.engine,
    );

    // One set, not a `Vec::contains` per channel: at 20k channels the vector scan would be a
    // quadratic pass over the profile.
    let host_enabled: HashSet<i64> = host.channels.iter().copied().collect();
    let mut outcome = VisibilityOutcome {
        profile_id: host.id,
        profile_name: host.name.clone(),
        channels: scoped.channels.len(),
        groups: groups.len(),
        enabled_before: scoped
            .channels
            .iter()
            .filter(|channel| host_enabled.contains(&channel.id))
            .count(),
        description: format!("channels in scope{}", scope_suffix(scope)),
        ..VisibilityOutcome::default()
    };

    for group in &groups {
        // `_sort_channels_by_priority`: quality tag, then channel number.
        let sorted = matching::sort_channels_by_priority(group, &scoped.channels);
        let mut enabled_in_group = false;
        for position in sorted {
            let channel = &scoped.channels[position];
            if channel.attached_channel_id.is_some() {
                // An attached channel is not the channel the guide plays: it is never made
                // visible, and it does not take the group's one slot either.
                outcome.attached_skipped += 1;
                continue;
            }
            if channel.streams.is_empty() || enabled_in_group {
                continue;
            }
            outcome.winners.push(channel.id);
            if !host_enabled.contains(&channel.id) {
                // Rowless or disabled in the host: REST cannot tell which, and enabling a
                // rowless channel creates its row. See the module docs.
                outcome.withheld.push(channel.id);
            }
            enabled_in_group = true;
        }
        if !enabled_in_group {
            outcome.groups_without_winner += 1;
        }
    }

    // The end state: the winners enabled, everything else in scope disabled. Written for
    // every channel so the plan can report each one's transition; `channel_ops` drops the
    // ones that already hold their value. The winner lookup is a set: a `Vec::contains` per
    // channel would be quadratic at 20k channels.
    let withheld: HashSet<i64> = outcome.withheld.iter().copied().collect();
    let winners: HashSet<i64> = outcome
        .winners
        .iter()
        .copied()
        .filter(|id| !withheld.contains(id))
        .collect();
    for channel in &scoped.channels {
        outcome
            .end_state
            .insert(channel.id, winners.contains(&channel.id));
    }
    Ok(outcome)
}

/// The action: preview (write the CSV) or apply (write the membership diff).
pub async fn run(
    app: &dyn SubApp,
    context: &JobContext,
    settings: &StreamsSettings,
    dry_run: bool,
) -> Result<JobOutcome> {
    let client = context.dispatcharr()?;
    let action_id = format!(
        "streams.visibility_{}",
        if dry_run { "preview" } else { "apply" }
    );
    let scope = context.config().scope.clone();

    // Channels (their lists and numbers), groups (the grouping key reads group names for a
    // channel's country), profiles (the scope filter AND the membership being written). No
    // streams: a count is all this action asks of a stream list.
    let kinds = WorldKinds::only(WorldKind::Channels) | WorldKind::Groups | WorldKind::Profiles;
    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.visibility",
        0,
        0,
        "loading Dispatcharr data",
    ));
    let world = client.load_world_with(kinds).await?;

    context.progress(&crosstalk_core::jobs::ProgressEvent::new(
        "streams.visibility",
        0,
        0,
        "grouping channels",
    ));
    let outcome = plan(&world, &scope, settings, &context.config().matching)?;
    if outcome.channels == 0 {
        return Ok(JobOutcome::new(
            "No channels are in scope. Check the group and profile settings in crosstalk.toml.",
        ));
    }

    let operations = vec![Op::Set {
        field: Field::Membership(outcome.profile_id),
        values: outcome.values(),
    }];
    let op_plan = channel_ops::plan(&world, &Selection::in_scope(&scope), &operations)?;

    let applied = if dry_run || op_plan.changes.is_empty() {
        None
    } else {
        Some(channel_ops::apply(client, &op_plan, context, &action_id).await)
    };

    let schema = crate::settings::schema();
    let values = crate::settings::to_values(settings);
    let stem = format!("visibility_{}", if dry_run { "preview" } else { "applied" });
    let title = format!(
        "{REPORT_TITLE} {} Report",
        if dry_run { "Preview" } else { "Applied" }
    );
    let spec = ReportSpec::new(app, &schema, &values, &stem, &title)
        .note("Selection", outcome.description.clone())
        .note(
            "Profile written",
            format!("{} (id {})", outcome.profile_name, outcome.profile_id),
        )
        .note(
            "Name groups",
            format!(
                "{} group(s); {} with no channel that has a stream",
                outcome.groups, outcome.groups_without_winner
            ),
        )
        .note(
            "End state",
            format!(
                "{} channel(s) enabled, {} disabled ({} enabled before this run)",
                outcome.enabled(),
                outcome.disabled(),
                outcome.enabled_before
            ),
        )
        .note(
            "Enables withheld",
            if outcome.withheld.is_empty() {
                "none: every winner is already enabled in the profile written".to_string()
            } else {
                let shown: Vec<String> = outcome
                    .withheld
                    .iter()
                    .take(50)
                    .map(|id| id.to_string())
                    .collect();
                format!(
                    "{} winner(s) not enabled in {:?} were left as they are (enabling one \
                     over REST would add it to the profile): channel id(s) {}{}",
                    outcome.withheld.len(),
                    outcome.profile_name,
                    shown.join(", "),
                    if outcome.withheld.len() > shown.len() {
                        ", …"
                    } else {
                        ""
                    }
                )
            },
        )
        .note(
            "Attached channels",
            if outcome.attached_skipped == 0 {
                "none: no channel carries attached_channel_id".to_string()
            } else {
                format!(
                    "{} channel(s) skipped, an attached channel is never made visible",
                    outcome.attached_skipped
                )
            },
        );
    let report = channel_ops::write_ops_report(context, &spec, &op_plan, applied.as_ref())?;
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

    let changed = op_plan.changes.len();
    let mut builder = OutcomeBuilder::new(if dry_run {
        format!(
            "Dry run complete. Would enable {} and disable {} channel(s) in profile {:?}.",
            outcome
                .end_state
                .values()
                .filter(|enabled| **enabled)
                .count(),
            outcome
                .end_state
                .values()
                .filter(|enabled| !**enabled)
                .count(),
            outcome.profile_name
        )
    } else {
        outcome.summary()
    })
    .count("selected", outcome.channels as i64)
    .count("groups", outcome.groups as i64)
    .count("enabled", outcome.enabled() as i64)
    .count("enable_withheld", outcome.withheld.len() as i64)
    .count("disabled", outcome.disabled() as i64)
    .count("enabled_before", outcome.enabled_before as i64)
    .count("attached_skipped", outcome.attached_skipped as i64)
    .count(
        "groups_without_winner",
        outcome.groups_without_winner as i64,
    )
    .count("changes", changed as i64);
    if let Some(applied) = applied.as_ref() {
        builder = builder
            .count("written", applied.ok as i64)
            .count("failed", applied.failed as i64);
        if applied.failed > 0 {
            builder = builder.note(format!("{} row(s) failed", applied.failed));
        }
    }
    if outcome.groups_without_winner > 0 {
        builder = builder.note(format!(
            "{} name group(s) had no channel with a stream, so every channel in them is \
             disabled",
            outcome.groups_without_winner
        ));
    }
    if !outcome.withheld.is_empty() {
        builder = builder.note(format!(
            "{} winner(s) are not enabled in profile {:?} and were left as they are: \
             crosstalk only disables over REST (see the report preamble)",
            outcome.withheld.len(),
            outcome.profile_name
        ));
    }
    // The cancel rule (see crosstalk_core::jobs): whatever landed stays recorded, and the run
    // says it was cancelled rather than claiming the profile is in its final state.
    if let Some(note) = channel_ops::cancelled_note(context, &op_plan, applied.as_ref()) {
        builder = builder.cancelled().note(note);
    }
    Ok(builder.report(Some(report)).build())
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
    use crate::{StreamsApp, ACTION_VISIBILITY_APPLY, ACTION_VISIBILITY_PREVIEW};
    use crosstalk_core::config::CrosstalkConfig;
    use crosstalk_core::dispatcharr::{Channel, ChannelGroup, ChannelProfile};

    /// Three name groups: a duplicate pair (ESPN), a singleton with streams, a group where
    /// every channel is empty, and an attached channel that must never be enabled.
    fn visibility_world() -> World {
        let channel = |id: i64, name: &str, number: f64, streams: Vec<i64>| Channel {
            id,
            name: name.to_string(),
            channel_number: Some(serde_json::json!(number)),
            channel_group_id: Some(100),
            streams,
            ..Channel::default()
        };
        let mut attached = channel(50, "TNT", 9.0, vec![7]);
        attached.attached_channel_id = Some(51);
        World {
            groups: vec![ChannelGroup {
                id: 100,
                name: "US: Cable".to_string(),
            }],
            channels: vec![
                // Same group, two feeds: the [HD] tagged one wins on priority, not the number.
                channel(1, "ESPN", 2.0, vec![10, 11]),
                channel(2, "ESPN", 1.0, vec![12]),
                channel(3, "ESPN [HD]", 5.0, vec![13]),
                // A singleton with a stream: enabled.
                channel(4, "CNN", 3.0, vec![14]),
                // Both empty: the group goes dark.
                channel(6, "Bravo", 4.0, Vec::new()),
                channel(7, "Bravo", 6.0, Vec::new()),
                // The attached one, alone in its group: like every channel in a group with no
                // eligible winner, it stays disabled.
                attached,
                channel(51, "TNT", 10.0, Vec::new()),
            ],
            profiles: vec![ChannelProfile {
                id: 4,
                name: "All".to_string(),
                channels: vec![1, 2, 3, 4, 6, 7, 50, 51],
            }],
            ..World::default()
        }
    }

    fn scope() -> ScopeConfig {
        ScopeConfig {
            profiles: "All".to_string(),
            ..ScopeConfig::default()
        }
    }

    fn options() -> (StreamsSettings, MatchingConfig) {
        let _ = CrosstalkConfig::default();
        (StreamsSettings::default(), MatchingConfig::default())
    }

    #[test]
    fn the_first_eligible_channel_of_each_group_is_the_one_left_enabled() {
        let world = visibility_world();
        let (settings, matching) = options();
        let outcome = plan(&world, &scope(), &settings, &matching).expect("plan");
        // ESPN's group: [HD] wins on the quality tag before the channel number is read.
        assert!(outcome.winners.contains(&3), "{:?}", outcome.winners);
        assert!(
            !outcome.winners.contains(&2),
            "the lowest number does not win"
        );
        assert!(outcome.winners.contains(&4), "CNN has a stream");
        // Bravo is all-empty and TNT's only member with a stream is attached.
        for id in [6, 7, 50, 51] {
            assert_eq!(outcome.end_state.get(&id), Some(&false), "channel {id}");
        }
        assert_eq!(outcome.groups, 4, "ESPN, CNN, Bravo, TNT");
        assert_eq!(outcome.groups_without_winner, 2, "Bravo and TNT");
        assert_eq!(outcome.attached_skipped, 1);
        assert_eq!(outcome.enabled_before, 8);
        assert_eq!(outcome.disabled(), 6);
    }

    #[test]
    fn the_attached_channel_does_not_take_the_groups_one_slot() {
        let mut world = visibility_world();
        // The attached channel sorts first and is eligible; the plain channel behind it must
        // still be the group's winner.
        world.channels[6].channel_number = Some(serde_json::json!(0.5));
        world.channels[6].streams = vec![7];
        world.channels[7].streams = vec![8];
        let (settings, matching) = options();
        let outcome = plan(&world, &scope(), &settings, &matching).expect("plan");
        assert!(outcome.winners.contains(&51), "{:?}", outcome.winners);
        assert!(!outcome.winners.contains(&50));
    }

    #[test]
    fn a_scope_with_one_name_group_and_no_stream_disables_everything() {
        let mut world = visibility_world();
        for channel in &mut world.channels {
            channel.streams.clear();
        }
        let (settings, matching) = options();
        let outcome = plan(&world, &scope(), &settings, &matching).expect("plan");
        assert!(outcome.winners.is_empty());
        assert!(outcome.end_state.values().all(|enabled| !enabled));
        assert_eq!(outcome.groups_without_winner, 4, "every group goes dark");
    }

    #[test]
    fn a_scope_with_no_profile_is_refused_with_the_plugins_words() {
        let world = visibility_world();
        let error = host_profile(&world, &ScopeConfig::default()).expect_err("refused");
        assert!(
            format!("{error}").contains("Profile name is required"),
            "{error}"
        );
    }

    #[test]
    fn the_first_named_profile_is_the_one_written() {
        let mut world = visibility_world();
        world.profiles.push(ChannelProfile {
            id: 7,
            name: "Kids".to_string(),
            channels: vec![1, 2, 3, 4],
        });
        let scope = ScopeConfig {
            profiles: "Kids, All".to_string(),
            ..ScopeConfig::default()
        };
        let host = host_profile(&world, &scope).expect("host");
        assert_eq!(host.id, 7);
        let (settings, matching) = options();
        let outcome = plan(&world, &scope, &settings, &matching).expect("plan");
        assert_eq!(outcome.profile_id, 7);
        // The channel set is the UNION of the named profiles: TNT, Bravo and channel 51 are
        // enabled in "All" only, and they still take part.
        assert_eq!(outcome.channels, 8);
        // All four "Kids" channels are enabled in the host profile, and only ESPN [HD] and CNN
        // survive: the rest move to disabled.
        assert_eq!(outcome.enabled_before, 4);
        assert!(outcome.withheld.is_empty(), "{:?}", outcome.withheld);
    }

    #[test]
    fn a_winner_not_enabled_in_the_host_profile_is_withheld_not_enabled() {
        // "Kids" lacks ESPN [HD] (3), the ESPN group's winner, which is enabled in "All"
        // only. The plugin would enable it if a disabled row exists and leave it hidden if
        // none does; REST cannot tell the two apart and its write would CREATE the row, so
        // crosstalk leaves it as it is.
        let mut world = visibility_world();
        world.profiles.push(ChannelProfile {
            id: 7,
            name: "Kids".to_string(),
            channels: vec![1, 2, 4],
        });
        let scope = ScopeConfig {
            profiles: "Kids, All".to_string(),
            ..ScopeConfig::default()
        };
        let (settings, matching) = options();
        let outcome = plan(&world, &scope, &settings, &matching).expect("plan");
        assert!(outcome.winners.contains(&3), "{:?}", outcome.winners);
        assert_eq!(outcome.withheld, vec![3]);
        assert_eq!(outcome.end_state.get(&3), Some(&false));
        assert_eq!(
            outcome.end_state.get(&4),
            Some(&true),
            "CNN is enabled already"
        );
        assert_eq!(outcome.enabled(), 1);
    }

    #[tokio::test]
    async fn a_cancelled_apply_is_recorded_cancelled_and_keeps_what_it_did() {
        let world = visibility_world();
        let mut server = ServerFixture::start(&world).await;
        server.config.scope.profiles = "All".to_string();
        let store = server.store();
        let context = server.context_for_run(ACTION_VISIBILITY_APPLY, &store, false);
        let run_id = context.run_id().expect("run id");
        context.cancel();

        let outcome = StreamsApp::new()
            .dispatch(ACTION_VISIBILITY_APPLY, &context, false)
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
                .request_count("PATCH", "/api/channels/profiles/4/channels/bulk-update/")
                .await,
            0,
            "nothing is written"
        );
        // Each planned row says it was not applied; the `skipped` rows (already enabled) are
        // not failures and keep their own reason.
        let rows = store.rows(run_id).expect("rows");
        let planned: Vec<_> = rows.iter().filter(|row| row.status != "skipped").collect();
        assert!(!planned.is_empty(), "{rows:?}");
        assert!(
            planned
                .iter()
                .all(|row| row.status == "failed" && row.reason.contains("cancelled")),
            "{rows:?}"
        );
    }

    #[tokio::test]
    async fn preview_writes_nothing_and_apply_writes_the_diff() {
        let world = visibility_world();
        let mut server = ServerFixture::start(&world).await;
        server.config.scope.profiles = "All".to_string();
        let store = server.store();
        let app = StreamsApp::new();
        // The membership endpoint the write uses.
        let context = server.context_for_run(ACTION_VISIBILITY_PREVIEW, &store, true);
        let preview = app
            .dispatch(ACTION_VISIBILITY_PREVIEW, &context, true)
            .await
            .expect("preview");
        assert_eq!(preview.counts.get("enabled"), Some(&2));
        assert_eq!(preview.counts.get("disabled"), Some(&6));
        assert_eq!(
            server
                .request_count("PATCH", "/api/channels/profiles/4/channels/bulk-update/")
                .await,
            0,
            "a preview writes nothing"
        );

        let store = server.store();
        let context = server.context_for_run(ACTION_VISIBILITY_APPLY, &store, false);
        let applied = app
            .dispatch(ACTION_VISIBILITY_APPLY, &context, false)
            .await
            .expect("apply");
        assert!(
            applied.summary.contains("Enabled 2 of 8 channels"),
            "{}",
            applied.summary
        );
        // The diff, not the plugin's two bulk updates: one request, and every row in it is a
        // disable — the two winners already hold their value.
        let bodies = server
            .bodies("PATCH", "/api/channels/profiles/4/channels/bulk-update/")
            .await;
        assert_eq!(bodies.len(), 1, "one membership update for the profile");
        let rows = bodies[0]["channels"].as_array().expect("a channels array");
        assert_eq!(rows.len(), 6, "the six channels that change");
        assert!(
            rows.iter()
                .all(|row| row["enabled"] == serde_json::json!(false)),
            "{rows:?}"
        );
        let ids: Vec<i64> = rows
            .iter()
            .map(|row| row["channel_id"].as_i64().expect("a channel id"))
            .collect();
        assert!(!ids.contains(&3) && !ids.contains(&4), "{ids:?}");
    }

    #[tokio::test]
    async fn applying_without_a_profile_in_scope_is_refused() {
        let world = visibility_world();
        let server = ServerFixture::start(&world).await;
        let store = server.store();
        let context = server.context_for_run(ACTION_VISIBILITY_APPLY, &store, false);
        let error = StreamsApp::new()
            .dispatch(ACTION_VISIBILITY_APPLY, &context, false)
            .await
            .expect_err("refused");
        assert!(
            format!("{error}").contains("Profile name is required"),
            "{error:#}"
        );
    }
}
