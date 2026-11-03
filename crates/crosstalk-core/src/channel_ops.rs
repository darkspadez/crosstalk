//! Channel operations: one selection, a list of operations, and the changes between them.
//!
//! This is the primitive the plugin repeated five times. `add_bad_epg_suffix` (2474),
//! `remove_epg_assignments` (2427), `remove_epg_from_hidden` (2562), `remove_epg_by_regex`
//! (2915) and `remove_all_epg_from_groups` (2968) in
//! `reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/plugin.py` are each a channel query
//! followed by a bulk write; the later phases (streams triage and restore, lineup clean and
//! names, events visibility) are the same shape again. So it lives here, once, with a dry run
//! and a CSV for free.
//!
//! Three properties are deliberate:
//!
//!   * [`plan`] is PURE. It re-reads every selected channel's current name, group and guide
//!     from the [`World`] it is handed, so a scan from an hour ago cannot add a suffix that is
//!     already there or clear a guide that has been reassigned since. Nothing is cached
//!     between planning and applying beyond what the plan itself carries;
//!   * the selection is the app's `[scope]` plus the action's own filter
//!     ([`Selection`]), resolved through [`crate::scope`] — the wildcard, case-sensitivity and
//!     mutual-exclusion rules are the scope's, not a second copy;
//!   * every change keeps its `before` value, in the CSV and in the run rows
//!     ([`run_rows`] fills the store's `field`/`before_value`/`after_value` columns), which is
//!     what a later `restore` reverses. `Delete` is deliberately absent: deleting channels is
//!     out of scope.
//!
//! Divergences from the plugin, each deliberate:
//!
//!   * `remove_epg_from_hidden` also DELETES the `ProgramData` rows of each entry, which blanks
//!     the guide of every *visible* channel sharing it. crosstalk only unassigns, so a shared
//!     entry keeps its programmes for the channels still on it;
//!   * the plugin's suffix and clear actions apply the ids stored by the last scan without
//!     re-checking the scope, so a settings change between scan and cleanup lets them touch a
//!     channel outside `[scope]`. A [`Selection`] is always intersected with the scope, and an
//!     id that is gone or out of scope is reported in [`Plan::skipped`];
//!   * a scan-derived selection also carries the guide the scan saw ([`Selection::expected_epg`]
//!     via [`Selection::from_scan_rows`]); a channel whose guide changed since is skipped
//!     rather than cleared;
//!   * a channel with an EPG *override* is skipped by [`Op::ClearEpg`]: clearing the channel's
//!     own column would not change what is shown, and the guide action already treats an
//!     override the same way when matching.
//!
//! Without [`PlanOptions::suffix_present_skips_rest`] the operations are independent: a
//! rename that is a no-op is reported as a skip and the rest still run, which is what
//! `triage`/`visibility` need when only one of their operations applies to a channel. The
//! `tag_missing` preset opts into the plugin's coupling instead: `add_bad_epg_suffix` with
//! `remove_epg_with_suffix` skips an already-suffixed channel ENTIRELY, so it keeps its
//! guide too (2508-2514).

use crate::config::ScopeConfig;
use crate::csv_report::{self, Report};
use crate::dispatcharr::{
    Channel, ChannelEdit, DispatcharrClient, EpgEntry, Set, World, WorldKind,
};
use crate::jobs::JobContext;
use crate::scope::{scope_channels, ScopedChannels};
use crate::store::RunRow;
use crate::subapp::{SettingsSchema, SettingsValues, SubApp};
use crate::wildcard::parse_tokens;
use anyhow::{bail, Context, Result};
use regex::Regex;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

/// The columns a channel-ops report carries.
pub const COLUMNS: [&str; 9] = [
    "channel_id",
    "channel_name",
    "channel_number",
    "group",
    "field",
    "before",
    "after",
    "status",
    "reason",
];

/// The placeholders a [`RenameKind::Template`] understands. Anything else between braces is
/// left exactly as written, so a template may also carry `{…}` meant for another tool.
pub const TEMPLATE_PLACEHOLDERS: [&str; 5] = ["{name}", "{group}", "{number}", "{epg}", "{id}"];

/// Progress is published at most this often, like the plugin's own two-second flush.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(2);

/// What a plan may touch: the app's `[scope]`, plus the action's own filter.
///
/// The three scope strings are the `[scope]` values the run is held to, and they are resolved
/// by [`crate::scope::scope_channels`] — wildcards, case-sensitive group names, the
/// "cannot use both" rule and the error wording are all the scope's. The rest are the
/// selections the plugin expressed with its own queries: the previous scan's result set
/// ([`Selection::from_scan_rows`]), a profile's disabled memberships
/// ([`Selection::hidden_in`], `remove_epg_from_hidden`), a regex over the assigned guide's
/// NAME (`remove_epg_by_regex`), and channels carrying a guide at all.
///
/// A field left empty is no filter, so `Selection::default()` is "everything the scope
/// allows".
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Selection {
    /// Only channels in these groups, comma-separated, `*`/`?` wildcards. The `[scope]` value.
    pub groups: String,
    /// Exclude channels in these groups. The `[scope]` value; cannot be combined with `groups`.
    pub ignore_groups: String,
    /// Only channels ENABLED in these profiles, comma-separated. The `[scope]` value.
    pub profiles: String,
    /// Only channels DISABLED in these profiles, comma-separated, by name.
    ///
    /// This is `remove_epg_from_hidden`'s selection (2562): the plugin reads
    /// `channel_profile_name` here to mean "look for disabled memberships in this profile",
    /// which is why it is a separate field from `profiles` — the same setting answers two
    /// different questions depending on the action.
    pub hidden_in_profiles: String,
    /// Only channels whose `hidden_from_output` already equals this.
    pub hidden_from_output: Option<bool>,
    /// Only channels whose assigned guide's name matches this pattern.
    ///
    /// `remove_epg_by_regex` (2915-2946) compiles the setting with `re.compile` and calls
    /// `.search` on `channel.epg_data.name`: CASE-SENSITIVE and UNANCHORED, so `US` matches
    /// `US: HBO` and `hbo` matches nothing. The GUIDE's name is matched, not the channel's.
    /// The pattern is compiled by [`regex::Regex`], which is not Python's `re`; see
    /// [`compile_pattern`] for the gaps.
    pub epg_name_regex: Option<String>,
    /// Only these channel ids, typically a previous run's result set.
    pub channel_ids: Vec<i64>,
    /// Only channels that have a guide assigned (`epg_data__isnull=False`, the pre-filter of
    /// `remove_all_epg_from_groups` and `remove_epg_by_regex`).
    pub has_epg: bool,
    /// The guide a previous scan saw, per channel id: `Some(id)` for an assignment, `None` for
    /// none. A channel whose current assignment differs is SKIPPED, so a stale scan cannot
    /// clear a guide that has since been reassigned. Empty means no check.
    pub expected_epg: HashMap<i64, Option<i64>>,
}

impl Selection {
    /// The selection an app's `[scope]` alone describes: every dimension of it, no extra
    /// filter.
    pub fn in_scope(scope: &ScopeConfig) -> Self {
        Self {
            groups: scope.groups.clone(),
            ignore_groups: scope.ignore_groups.clone(),
            profiles: scope.profiles.clone(),
            ..Self::default()
        }
    }

    /// A selection of the channels DISABLED in these profiles, by name.
    ///
    /// The scope's group settings still apply (an operator's `[scope].groups` is a bound on
    /// everything a run may write); the scope's profile setting does not, because for this
    /// selection the profile list IS the filter — see [`Self::hidden_in_profiles`].
    ///
    /// Callers must reject a blank `profiles` themselves: the plugin errors when the setting
    /// is empty (2574-2578), while an empty [`Self::hidden_in_profiles`] here means "no
    /// filter" rather than "filter to nothing".
    pub fn hidden_in(profiles: &str, scope: &ScopeConfig) -> Self {
        Self {
            groups: scope.groups.clone(),
            ignore_groups: scope.ignore_groups.clone(),
            hidden_in_profiles: profiles.to_string(),
            ..Self::default()
        }
    }

    /// A selection of these ids, carrying the guide each one was last seen on.
    ///
    /// `rows` is a previous run's rows: `channel_id` is the channel, `epg_data_id` the guide
    /// the scan found it on (a scan row records the assignment it examined there, with
    /// `current_epg_name` as its name). The expectation is what makes a stale result set safe
    /// to re-plan.
    pub fn from_scan_rows(rows: &[RunRow], scope: &ScopeConfig) -> Self {
        let mut selection = Self::in_scope(scope);
        for row in rows {
            if !selection.channel_ids.contains(&row.channel_id) {
                selection.channel_ids.push(row.channel_id);
            }
            selection
                .expected_epg
                .insert(row.channel_id, row.epg_data_id);
        }
        selection
    }

    /// The selection in words, for the report preamble and the log.
    fn describe(&self, scope_description: &str) -> String {
        let mut parts: Vec<String> = Vec::new();
        let scoped = scope_description.trim();
        if !scoped.is_empty() {
            parts.push(scoped.to_string());
        }
        if !self.hidden_in_profiles.trim().is_empty() {
            parts.push(format!(
                "disabled in profile(s) {}",
                quote_list(&parse_tokens(&self.hidden_in_profiles))
            ));
        }
        match self.hidden_from_output {
            Some(true) => parts.push("hidden from output".to_string()),
            Some(false) => parts.push("shown in output".to_string()),
            None => {}
        }
        if self.has_epg {
            parts.push("with a guide assigned".to_string());
        }
        if let Some(pattern) = &self.epg_name_regex {
            parts.push(format!("whose guide name matches {pattern:?}"));
        }
        if !self.channel_ids.is_empty() {
            parts.push(format!("{} named channel id(s)", self.channel_ids.len()));
        }
        if !self.expected_epg.is_empty() {
            parts.push("still on the guide the scan saw".to_string());
        }
        if parts.is_empty() {
            "every channel".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// What to do to every selected channel.
///
/// There is no `Delete`: the plugins' permanent deletes are out of scope for crosstalk.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    Rename {
        kind: RenameKind,
    },
    /// Move every selected channel into this group.
    MoveGroup {
        group_id: i64,
    },
    /// Enable or disable every selected channel in this profile.
    SetMembership {
        profile_id: i64,
        enabled: bool,
    },
    /// Set the global `hidden_from_output` flag.
    SetHiddenFromOutput {
        hidden: bool,
    },
    /// Unassign the guide entry (`epg_data_id = null`).
    ClearEpg,
}

impl Op {
    /// The operation without its parameters, for a report's `method` column.
    pub fn kind(&self) -> OpKind {
        match self {
            Self::Rename { .. } => OpKind::Rename,
            Self::MoveGroup { .. } => OpKind::MoveGroup,
            Self::SetMembership { .. } => OpKind::SetMembership,
            Self::SetHiddenFromOutput { .. } => OpKind::SetHiddenFromOutput,
            Self::ClearEpg => OpKind::ClearEpg,
        }
    }

    /// The field this operation writes, for a skip that never got as far as planning one.
    fn field(&self) -> Field {
        match self {
            Self::Rename { .. } => Field::Name,
            Self::MoveGroup { .. } => Field::Group,
            Self::SetMembership { profile_id, .. } => Field::Membership(*profile_id),
            Self::SetHiddenFromOutput { .. } => Field::HiddenFromOutput,
            Self::ClearEpg => Field::Epg,
        }
    }
}

/// How a [`Op::Rename`] builds the new name.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RenameKind {
    /// Appended, unless the name already ends with it (`add_bad_epg_suffix`, 2503).
    Suffix(String),
    /// Prepended, unless the name already starts with it.
    Prefix(String),
    /// Rendered from [`TEMPLATE_PLACEHOLDERS`].
    Template(String),
}

impl RenameKind {
    /// Whether the name already carries this rename, in which case the plugin skips the
    /// channel instead of adding it twice (2503, `current_name.endswith(bad_epg_suffix)`).
    fn already(&self, current: &str) -> bool {
        match self {
            Self::Suffix(text) => current.ends_with(text.as_str()),
            Self::Prefix(text) => current.starts_with(text.as_str()),
            Self::Template(_) => false,
        }
    }

    /// The new name, or `None` when it would be empty (a blank name is not a rename).
    fn produce(&self, current: &str, values: &Placeholders) -> Option<String> {
        let name = match self {
            Self::Suffix(text) => format!("{current}{text}"),
            Self::Prefix(text) => format!("{text}{current}"),
            Self::Template(template) => render_template(template, values),
        };
        if name.trim().is_empty() {
            None
        } else {
            Some(name)
        }
    }
}

/// One field a change writes, as the report and a restore name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Name,
    Group,
    Epg,
    HiddenFromOutput,
    /// A profile membership; the id is the profile.
    Membership(i64),
}

impl Field {
    /// The writable field's name, as the API spells it.
    pub fn label(&self) -> String {
        match self {
            Self::Name => "name".to_string(),
            Self::Group => "channel_group_id".to_string(),
            Self::Epg => "epg_data_id".to_string(),
            Self::HiddenFromOutput => "hidden_from_output".to_string(),
            Self::Membership(profile_id) => format!("profile_membership:{profile_id}"),
        }
    }

    /// The inverse of [`Self::label`], for a `restore` reading back the store's `field`
    /// column. `None` is a row no restore understands (a matching row's empty field, or a
    /// label from a newer build), which the caller skips and reports rather than failing
    /// the whole restore over.
    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "name" => Some(Self::Name),
            "channel_group_id" => Some(Self::Group),
            "epg_data_id" => Some(Self::Epg),
            "hidden_from_output" => Some(Self::HiddenFromOutput),
            _ => label
                .strip_prefix("profile_membership:")
                .and_then(|id| id.parse().ok())
                .map(Self::Membership),
        }
    }
}

/// An operation without its parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Rename,
    MoveGroup,
    SetMembership,
    SetHiddenFromOutput,
    ClearEpg,
}

impl OpKind {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Rename => "rename",
            Self::MoveGroup => "move_group",
            Self::SetMembership => "set_membership",
            Self::SetHiddenFromOutput => "set_hidden_from_output",
            Self::ClearEpg => "clear_epg",
        }
    }
}

/// One field's value, as a plan records it.
///
/// Ids are kept next to the names they print as: a report shows a guide or a group by NAME,
/// while the write — and a later restore of it — needs the ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// A channel name.
    Name(String),
    /// A channel group; `None` is "No Group".
    Group(Option<(i64, String)>),
    /// An EPG entry; `None` is an unassigned guide.
    Epg(Option<(i64, String)>),
    /// `hidden_from_output`, or a profile membership's `enabled`.
    Flag(bool),
}

impl Value {
    /// How the report prints it: names, not ids, and `yes`/`no` for a flag.
    pub fn display(&self) -> String {
        match self {
            Self::Name(name) => name.clone(),
            Self::Group(Some((_, name))) => name.clone(),
            Self::Group(None) => "No Group".to_string(),
            Self::Epg(Some((_, name))) => name.clone(),
            Self::Epg(None) => "No EPG".to_string(),
            Self::Flag(flag) => if *flag { "yes" } else { "no" }.to_string(),
        }
    }

    /// The machine value a restore reads back: an id, `none`, or `true`/`false`.
    pub fn wire(&self) -> String {
        match self {
            Self::Name(name) => name.clone(),
            Self::Group(Some((id, _))) | Self::Epg(Some((id, _))) => id.to_string(),
            Self::Group(None) | Self::Epg(None) => "none".to_string(),
            Self::Flag(flag) => flag.to_string(),
        }
    }
}

/// How a row came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowStatus {
    /// Planned, not applied (a preview, or a plan nobody applied).
    Planned,
    /// The write was confirmed.
    Ok,
    /// The write failed; [`Change::reason`] carries the server's message.
    Failed,
}

impl RowStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }
}

/// One field's planned transition on one channel.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    pub channel_id: i64,
    /// The channel's name at plan time; a restore writes it back for the `name` field.
    pub channel_name: String,
    pub channel_number: Option<f64>,
    /// The channel's group name at plan time.
    pub channel_group: String,
    pub op: OpKind,
    pub field: Field,
    pub before: Value,
    pub after: Value,
    pub status: RowStatus,
    /// Set by [`apply`] when the write failed.
    pub reason: String,
}

impl Change {
    /// A planned change of one field, with the identity the report and a restore need.
    fn planned(
        channel: &Channel,
        group: &str,
        op: OpKind,
        field: Field,
        before: Value,
        after: Value,
    ) -> Self {
        Self {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_number: channel.display_number(),
            channel_group: group.to_string(),
            op,
            field,
            before,
            after,
            status: RowStatus::Planned,
            reason: String::new(),
        }
    }
}

/// An operation that would change nothing, or a selected channel that is gone or out of scope.
#[derive(Debug, Clone, PartialEq)]
pub struct Skip {
    pub channel_id: i64,
    pub channel_name: String,
    pub channel_number: Option<f64>,
    pub channel_group: String,
    pub op: Option<OpKind>,
    pub field: Option<Field>,
    pub reason: String,
}

impl Skip {
    /// A channel the selection named that Dispatcharr no longer has: there is no name or
    /// group to report, only the id.
    fn missing(channel_id: i64) -> Self {
        Self {
            channel_id,
            channel_name: String::new(),
            channel_number: None,
            channel_group: String::new(),
            op: None,
            field: None,
            reason: "the channel no longer exists in Dispatcharr".to_string(),
        }
    }

    fn of(channel: &Channel, group: &str, reason: impl Into<String>) -> Self {
        Self {
            channel_id: channel.id,
            channel_name: channel.name.clone(),
            channel_number: channel.display_number(),
            channel_group: group.to_string(),
            op: None,
            field: None,
            reason: reason.into(),
        }
    }

    fn with_op(mut self, op: OpKind, field: Field) -> Self {
        self.op = Some(op);
        self.field = Some(field);
        self
    }
}

/// One channel the selection resolved to.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedChannel {
    pub id: i64,
    pub name: String,
    pub number: Option<f64>,
    pub group: String,
}

/// What [`plan`] produced: what it selected, what it would do, and what it left alone.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Plan {
    pub selected: Vec<SelectedChannel>,
    /// One entry per field change, in channel then operation order.
    pub changes: Vec<Change>,
    /// No-op operations and stale ids, with the reason each was left alone.
    pub skipped: Vec<Skip>,
    /// The scope and selection in words, for the report preamble and the log.
    pub description: String,
}

/// What [`apply`] did, row by row.
#[derive(Debug, Clone, Default)]
pub struct ApplyReport {
    /// The plan's changes, in the same order, with their status filled in.
    pub changes: Vec<Change>,
    pub ok: usize,
    pub failed: usize,
}

impl ApplyReport {
    pub fn all_ok(&self) -> bool {
        self.failed == 0
    }
}

/// How [`plan`] couples its operations, beyond the selection and the operations themselves.
///
/// Everything defaults off: operations are independent and each one is planned on its own,
/// which is what `triage`/`visibility` need when only one of their operations applies to a
/// channel. The `tag_missing` preset turns the flag on (see below).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct PlanOptions {
    /// When a [`RenameKind::Suffix`] rename is skipped because the name already ends with
    /// the suffix, skip every later operation on that channel too, so the channel gets
    /// NEITHER change.
    ///
    /// This is `add_bad_epg_suffix` with `remove_epg_with_suffix` (plugin.py:2508-2514):
    /// the plugin `continue`s past an already-suffixed channel entirely, so it keeps its
    /// guide. Only a suffix rename that is a no-op because the suffix is already present
    /// arms it — a prefix/template no-op, or an empty-name refusal, leaves later operations
    /// alone. The guard applies in list order: the preset lists the rename first, and an
    /// earlier operation that already planned a change has that change dropped (reported as
    /// a skip) when the guard fires.
    pub suffix_present_skips_rest: bool,
}

/// Plan a selection and a list of operations against the world.
///
/// Pure: nothing is written and the world is not modified. Each operation is evaluated
/// against the channel's CURRENT state, so it is safe to run over a result set from an older
/// scan; operations on the same field chain (a prefix then a suffix produce one change whose
/// `after` is the final name), and an operation that changes nothing is recorded in
/// [`Plan::skipped`] rather than as a no-op write.
///
/// The default [`PlanOptions`]: operations are independent. Callers that need the plugin's
/// suffix/clear coupling use [`plan_with_options`].
pub fn plan(world: &World, selection: &Selection, ops: &[Op]) -> Result<Plan> {
    plan_with_options(world, selection, ops, &PlanOptions::default())
}

/// [`plan`], with explicit [`PlanOptions`].
///
/// This is the entry point the `tag_missing` preset uses: it plans
/// `[Rename(Suffix), ClearEpg]` with `suffix_present_skips_rest`, so an already-tagged
/// channel is skipped entirely and keeps its guide, exactly like the plugin.
pub fn plan_with_options(
    world: &World,
    selection: &Selection,
    ops: &[Op],
    options: &PlanOptions,
) -> Result<Plan> {
    if ops.is_empty() {
        bail!("a channel plan needs at least one operation");
    }
    let regex = match selection.epg_name_regex.as_deref() {
        // A blank pattern matches every guide, which is never what an operator meant; the
        // plugin refuses one too (2920-2921).
        Some(pattern) if pattern.trim().is_empty() => bail!(
            "an empty EPG name pattern would match every guide; set a pattern or leave the \
             filter unset"
        ),
        Some(pattern) => Some(compile_pattern(pattern)?),
        None => None,
    };
    require_world(world, selection, ops)?;

    let scope = ScopeConfig {
        profiles: selection.profiles.clone(),
        groups: selection.groups.clone(),
        ignore_groups: selection.ignore_groups.clone(),
    };
    let scoped = scope_channels(world, &scope)?;
    let mut skipped: Vec<Skip> = Vec::new();
    let selected = narrow(world, &scoped, selection, regex.as_ref(), &mut skipped)?;

    let mut changes: Vec<Change> = Vec::new();
    let mut selected_rows: Vec<SelectedChannel> = Vec::with_capacity(selected.len());
    for channel in selected {
        let group = world.group_name(channel.channel_group_id);
        selected_rows.push(SelectedChannel {
            id: channel.id,
            name: channel.name.clone(),
            number: channel.display_number(),
            group: group.clone(),
        });

        let mut planned: Vec<Change> = Vec::new();
        // The state the operations see: each one sees the result of the ones before it, while
        // a change's `before` stays what the world reported.
        let mut current = Working::of(channel);
        // Armed by an already-present suffix rename when the options couple it to the rest
        // of the list: the suffix the channel already carries.
        let mut guarded: Option<String> = None;
        for op in ops {
            if let Some(suffix) = guarded.as_ref() {
                skipped.push(
                    Skip::of(
                        channel,
                        &group,
                        format!(
                            "already tagged with {suffix:?}, so this {} is skipped too",
                            op.kind().label()
                        ),
                    )
                    .with_op(op.kind(), op.field()),
                );
                continue;
            }
            apply_op(
                world,
                channel,
                &group,
                op,
                &mut current,
                &mut planned,
                &mut skipped,
                options,
                &mut guarded,
            );
        }
        if let Some(suffix) = guarded {
            // The guard fired after an earlier operation already planned a change (the
            // preset lists the rename first, so this is a hand-built order): the plugin
            // skips such a channel entirely, so neither change survives.
            if !planned.is_empty() {
                planned.clear();
                skipped.push(Skip::of(
                    channel,
                    &group,
                    format!(
                        "already tagged with {suffix:?}: an earlier operation in this plan \
                         had planned a change, which is dropped too"
                    ),
                ));
            }
        }
        // Two operations that cancel out are not a change; say so rather than write it.
        planned.retain(|change| {
            if change.before != change.after {
                return true;
            }
            skipped.push(
                Skip::of(
                    channel,
                    &group,
                    format!(
                        "the operations cancel out: {} keeps the value it had",
                        change.field.label()
                    ),
                )
                .with_op(change.op, change.field),
            );
            false
        });
        changes.extend(planned);
    }

    Ok(Plan {
        description: selection.describe(&scoped.filter_description),
        selected: selected_rows,
        changes,
        skipped,
    })
}

/// Apply a plan, one request per endpoint, recording every row's outcome.
///
/// The writes are batched the way the API wants them: a rename, a group move and a hide are
/// three fields of one `edit/bulk` row, memberships go one call per profile, and the guides
/// are cleared through `batch-set-epg`. `action` is the id the progress events are published
/// under; cancellation is checked between batches, so a cancelled run stops at the last batch
/// it finished rather than mid-request.
///
/// A failing batch marks ITS rows failed with the client's message, which names the chunk and
/// how many rows before it were confirmed — the same partial-progress wording
/// `batch_set_epg` and `edit_channels` already use. It does not stop the other endpoints: a
/// failed profile update must not abandon the EPG clear the operator asked for too.
pub async fn apply(
    client: &DispatcharrClient,
    plan: &Plan,
    context: &JobContext,
    action: &str,
) -> ApplyReport {
    let mut changes = plan.changes.clone();
    let total = changes.len() as u64;
    let mut progress = context.paced(action, total, PROGRESS_INTERVAL);
    let mut done = 0u64;

    // 1. Names, groups and visibility: one `edit/bulk` request.
    let mut edits: BTreeMap<i64, ChannelEdit> = BTreeMap::new();
    let mut edit_rows: Vec<usize> = Vec::new();
    for (index, change) in plan.changes.iter().enumerate() {
        if !matches!(
            change.field,
            Field::Name | Field::Group | Field::HiddenFromOutput
        ) {
            continue;
        }
        edit_rows.push(index);
        let edit = edits
            .entry(change.channel_id)
            .or_insert_with(|| ChannelEdit::new(change.channel_id));
        match (&change.field, &change.after) {
            (Field::Name, Value::Name(name)) => edit.name = Some(name.clone()),
            (Field::Group, Value::Group(group)) => {
                edit.channel_group_id = group.as_ref().map_or(Set::Null, |(id, _)| Set::Value(*id));
            }
            (Field::HiddenFromOutput, Value::Flag(hidden)) => {
                edit.hidden_from_output = Some(*hidden);
            }
            // A field this endpoint does not carry; the plan never produces one.
            _ => {}
        }
    }
    if !edit_rows.is_empty() {
        if context.is_cancelled() {
            mark(&mut changes, &edit_rows, RowStatus::Failed, CANCELLED);
        } else {
            let payload: Vec<ChannelEdit> = edits.into_values().collect();
            progress.set(done, &format!("editing {} channel(s)", payload.len()));
            match client.edit_channels(&payload).await {
                Ok(_) => mark(&mut changes, &edit_rows, RowStatus::Ok, ""),
                Err(error) => mark(
                    &mut changes,
                    &edit_rows,
                    RowStatus::Failed,
                    &format!("{error:#}"),
                ),
            }
        }
        done += edit_rows.len() as u64;
    }

    // 2. Profile memberships: one call per profile.
    let mut memberships: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    for (index, change) in plan.changes.iter().enumerate() {
        if let Field::Membership(profile_id) = change.field {
            memberships.entry(profile_id).or_default().push(index);
        }
    }
    for (profile_id, rows) in memberships {
        if context.is_cancelled() {
            mark(&mut changes, &rows, RowStatus::Failed, CANCELLED);
        } else {
            let payload: Vec<(i64, bool)> = rows
                .iter()
                .filter_map(|index| {
                    match (plan.changes[*index].channel_id, &plan.changes[*index].after) {
                        (channel_id, Value::Flag(enabled)) => Some((channel_id, *enabled)),
                        _ => None,
                    }
                })
                .collect();
            progress.set(done, &format!("updating profile {profile_id} membership"));
            match client.set_profile_membership(profile_id, &payload).await {
                Ok(_) => mark(&mut changes, &rows, RowStatus::Ok, ""),
                Err(error) => mark(
                    &mut changes,
                    &rows,
                    RowStatus::Failed,
                    &format!("{error:#}"),
                ),
            }
        }
        done += rows.len() as u64;
    }

    // 3. Guides: one `batch-set-epg` call, which chunks and retries internally.
    let mut epg_rows: Vec<usize> = Vec::new();
    let mut associations: Vec<(i64, Option<i64>)> = Vec::new();
    for (index, change) in plan.changes.iter().enumerate() {
        if let (Field::Epg, Value::Epg(after)) = (&change.field, &change.after) {
            epg_rows.push(index);
            associations.push((change.channel_id, after.as_ref().map(|(id, _)| *id)));
        }
    }
    if !epg_rows.is_empty() {
        if context.is_cancelled() {
            mark(&mut changes, &epg_rows, RowStatus::Failed, CANCELLED);
        } else {
            progress.set(done, &format!("clearing {} guide(s)", associations.len()));
            match client.batch_set_epg(&associations).await {
                Ok(_) => mark(&mut changes, &epg_rows, RowStatus::Ok, ""),
                Err(error) => mark(
                    &mut changes,
                    &epg_rows,
                    RowStatus::Failed,
                    &format!("{error:#}"),
                ),
            }
        }
    }

    let ok = changes
        .iter()
        .filter(|change| change.status == RowStatus::Ok)
        .count();
    let failed = changes
        .iter()
        .filter(|change| change.status == RowStatus::Failed)
        .count();
    progress.finish(format!("{ok} written, {failed} failed"));
    ApplyReport {
        changes,
        ok,
        failed,
    }
}

/// What a batch of rows gets when the run was cancelled before it ran.
const CANCELLED: &str = "not applied: the run was cancelled";

/// What a channel-ops report needs from its caller: the app it belongs to and the settings
/// the preamble names.
pub struct ReportSpec<'a> {
    pub app: &'a dyn SubApp,
    pub schema: &'a SettingsSchema,
    pub values: &'a SettingsValues,
    /// The file stem, e.g. `clear_groups_preview`.
    pub stem: &'a str,
    /// The report's title, e.g. "Clear EPG in Groups Preview Report".
    pub title: &'a str,
}

/// Write the plan (or what applying it did) as a CSV report.
///
/// Every planned change is a row, then every skip, so the file answers both "what happened"
/// and "what did it leave alone, and why" — the plugin's `already_dummy`/`already has the
/// suffix` lines, in one shape shared by all five actions.
pub fn write_ops_report(
    context: &JobContext,
    spec: &ReportSpec<'_>,
    plan: &Plan,
    applied: Option<&ApplyReport>,
) -> Result<PathBuf> {
    let rows: &[Change] = applied.map_or(&plan.changes, |report| report.changes.as_slice());
    let exports_dir = context.exports_dir();
    let report = Report::new(&exports_dir, spec.app, spec.stem, spec.title)
        .channels_processed(plan.selected.len())
        .result("Selection", &plan.description)
        .result("Changes planned", plan.changes.len())
        .result(
            "Changes written",
            match applied {
                Some(report) => report.ok.to_string(),
                None => "none, this was a preview".to_string(),
            },
        )
        .result("Skipped", plan.skipped.len());

    report.write(
        context.config(),
        spec.schema,
        spec.values,
        &COLUMNS,
        |writer| {
            for change in rows {
                writer.write_record([
                    change.channel_id.to_string(),
                    csv_report::safe_cell(&change.channel_name),
                    number_cell(change.channel_number),
                    csv_report::safe_cell(&change.channel_group),
                    change.field.label(),
                    csv_report::safe_cell(&change.before.display()),
                    csv_report::safe_cell(&change.after.display()),
                    change.status.as_str().to_string(),
                    csv_report::safe_cell(&change.reason),
                ])?;
            }
            for skip in &plan.skipped {
                writer.write_record([
                    skip.channel_id.to_string(),
                    csv_report::safe_cell(&skip.channel_name),
                    number_cell(skip.channel_number),
                    csv_report::safe_cell(&skip.channel_group),
                    skip.field.map(|field| field.label()).unwrap_or_default(),
                    String::new(),
                    String::new(),
                    "skipped".to_string(),
                    csv_report::safe_cell(&skip.reason),
                ])?;
            }
            Ok(())
        },
    )
}

/// The plan's rows in the store's shape, for `JobContext::record_rows`.
///
/// `field`/`before_value`/`after_value` are the generic slots a later `restore` reads: the
/// writable field's name and its two machine values (an id, `none`, or `true`/`false`). The
/// identity columns keep the state the run found, so a rename can be undone from its own row.
pub fn run_rows(plan: &Plan, applied: Option<&ApplyReport>) -> Vec<RunRow> {
    let changes: &[Change] = applied.map_or(&plan.changes, |report| report.changes.as_slice());
    let mut rows = Vec::with_capacity(changes.len() + plan.skipped.len());
    for (ordinal, change) in changes.iter().enumerate() {
        rows.push(RunRow {
            ordinal: ordinal as i64,
            channel_id: change.channel_id,
            channel_name: change.channel_name.clone(),
            channel_group: change.channel_group.clone(),
            channel_number: change.channel_number,
            // The guide the channel was on, which is what a restore of a clear puts back.
            current_epg_name: match &change.before {
                Value::Epg(Some((_, name))) => Some(name.clone()),
                _ => None,
            },
            epg_data_id: None,
            epg_name: None,
            epg_source: None,
            score: 0,
            method: change.op.label().to_string(),
            field: change.field.label(),
            before_value: Some(change.before.wire()),
            after_value: Some(change.after.wire()),
            status: change.status.as_str().to_string(),
            reason: change.reason.clone(),
            ai_decision: None,
        });
    }
    for (offset, skip) in plan.skipped.iter().enumerate() {
        rows.push(RunRow {
            ordinal: (changes.len() + offset) as i64,
            channel_id: skip.channel_id,
            channel_name: skip.channel_name.clone(),
            channel_group: skip.channel_group.clone(),
            channel_number: skip.channel_number,
            method: skip.op.map(|op| op.label().to_string()).unwrap_or_default(),
            field: skip.field.map(|field| field.label()).unwrap_or_default(),
            status: "skipped".to_string(),
            reason: skip.reason.clone(),
            ..RunRow::default()
        });
    }
    rows
}

/// Compile an EPG-name pattern, refusing the Python-only syntax with an explanation.
///
/// The plugin calls `re.compile(pattern)` with no flags and `.search(epg_name)`, so the
/// equivalent here is a case-sensitive, unanchored match. Most patterns mean the same thing in
/// both engines; the exceptions are Python's backreferences (`(a)\1`), lookaround (`(?=…)`,
/// `(?<=…)`, `(?<!…)`), conditionals and atomic groups, which [`regex::Regex`] does not
/// support. Those are rejected with this message rather than silently matching differently.
pub fn compile_pattern(pattern: &str) -> Result<Regex> {
    Regex::new(pattern).with_context(|| {
        format!(
            "invalid EPG name pattern {pattern:?}. The Rust `regex` engine is not Python's `re`: \
             backreferences (\\1), lookaround ((?=…), (?<=…)) and conditionals are not \
             supported, and a pattern that needs one has to be rewritten"
        )
    })
}

/// Fill a [`RenameKind::Template`].
///
/// One pass, so a channel literally named `{group}` is not substituted twice. Unknown
/// `{…}` tokens are copied through unchanged.
fn render_template(template: &str, values: &Placeholders) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let Some(close) = rest[open..].find('}') else {
            out.push_str(&rest[open..]);
            return out;
        };
        let token = &rest[open..open + close + 1];
        match token {
            "{name}" => out.push_str(&values.name),
            "{group}" => out.push_str(&values.group),
            "{number}" => out.push_str(&values.number),
            "{epg}" => out.push_str(&values.epg),
            "{id}" => out.push_str(&values.id),
            other => out.push_str(other),
        }
        rest = &rest[open + close + 1..];
    }
    out.push_str(rest);
    out
}

/// What a template can substitute.
struct Placeholders {
    name: String,
    group: String,
    number: String,
    epg: String,
    id: String,
}

/// A channel's mutable state while its operations are evaluated.
struct Working {
    name: String,
    group: Option<i64>,
    epg: Option<i64>,
    hidden: bool,
}

impl Working {
    fn of(channel: &Channel) -> Self {
        Self {
            name: channel.name.clone(),
            group: channel.channel_group_id,
            epg: channel.current_epg_id(),
            hidden: channel.hidden_from_output,
        }
    }
}

/// Evaluate one operation against a channel, recording a change or a skip.
///
/// `options` and `guarded` are the suffix/clear coupling of [`PlanOptions`]: when the
/// options arm it and this rename is a no-op because the suffix is already present, the
/// suffix is stored in `guarded` and the plan loop skips every later operation.
///
/// Nine parameters, like the AI gate's `ask`: one per thing an evaluation threads through,
/// and bundling them would only move the list behind a struct literal at this one call site.
#[allow(clippy::too_many_arguments)]
fn apply_op(
    world: &World,
    channel: &Channel,
    group: &str,
    op: &Op,
    current: &mut Working,
    planned: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
    options: &PlanOptions,
    guarded: &mut Option<String>,
) {
    let kind = op.kind();
    match op {
        Op::Rename { kind: rename } => {
            let name = current.name.clone();
            if rename.already(&name) {
                let reason = match rename {
                    RenameKind::Suffix(text) => format!("the name already ends with {text:?}"),
                    RenameKind::Prefix(text) => format!("the name already starts with {text:?}"),
                    RenameKind::Template(_) => String::new(),
                };
                skipped.push(Skip::of(channel, group, reason).with_op(kind, Field::Name));
                if options.suffix_present_skips_rest {
                    if let RenameKind::Suffix(suffix) = rename {
                        *guarded = Some(suffix.clone());
                    }
                }
                return;
            }
            let values = Placeholders {
                name: name.clone(),
                group: world.group_name(current.group),
                number: channel
                    .display_number()
                    .map(|number| format!("{number}"))
                    .unwrap_or_default(),
                epg: epg_values(world, current.epg)
                    .map(|(_, name)| name)
                    .unwrap_or_else(|| "No EPG".to_string()),
                id: channel.id.to_string(),
            };
            let Some(after) = rename.produce(&name, &values) else {
                skipped.push(
                    Skip::of(
                        channel,
                        group,
                        "the rename would produce an empty name, which would be rejected",
                    )
                    .with_op(kind, Field::Name),
                );
                return;
            };
            if after == name {
                skipped.push(
                    Skip::of(
                        channel,
                        group,
                        "the rename produces the name the channel already has",
                    )
                    .with_op(kind, Field::Name),
                );
                return;
            }
            record(
                planned,
                Change::planned(
                    channel,
                    group,
                    kind,
                    Field::Name,
                    Value::Name(name),
                    Value::Name(after.clone()),
                ),
            );
            current.name = after;
        }
        Op::MoveGroup { group_id } => {
            if current.group == Some(*group_id) {
                skipped.push(
                    Skip::of(
                        channel,
                        group,
                        format!(
                            "the channel is already in group {}",
                            quote_list(&[world.group_name(Some(*group_id))])
                        ),
                    )
                    .with_op(kind, Field::Group),
                );
                return;
            }
            record(
                planned,
                Change::planned(
                    channel,
                    group,
                    kind,
                    Field::Group,
                    Value::Group(group_values(world, current.group)),
                    Value::Group(group_values(world, Some(*group_id))),
                ),
            );
            current.group = Some(*group_id);
        }
        Op::SetMembership {
            profile_id,
            enabled,
        } => {
            let Some(profile) = world
                .profiles
                .iter()
                .find(|profile| profile.id == *profile_id)
            else {
                // `plan` validated the profile against the world; reaching here is a bug.
                return;
            };
            let now = profile.channels.contains(&channel.id);
            if now == *enabled {
                skipped.push(
                    Skip::of(
                        channel,
                        group,
                        format!(
                            "the channel is already {} in profile {}",
                            if *enabled { "enabled" } else { "disabled" },
                            quote_list(std::slice::from_ref(&profile.name))
                        ),
                    )
                    .with_op(kind, Field::Membership(*profile_id)),
                );
                return;
            }
            record(
                planned,
                Change::planned(
                    channel,
                    group,
                    kind,
                    Field::Membership(*profile_id),
                    Value::Flag(now),
                    Value::Flag(*enabled),
                ),
            );
        }
        Op::SetHiddenFromOutput { hidden } => {
            if current.hidden == *hidden {
                skipped.push(
                    Skip::of(
                        channel,
                        group,
                        format!(
                            "the channel is already {}",
                            if *hidden {
                                "hidden from output"
                            } else {
                                "shown in output"
                            }
                        ),
                    )
                    .with_op(kind, Field::HiddenFromOutput),
                );
                return;
            }
            record(
                planned,
                Change::planned(
                    channel,
                    group,
                    kind,
                    Field::HiddenFromOutput,
                    Value::Flag(current.hidden),
                    Value::Flag(*hidden),
                ),
            );
            current.hidden = *hidden;
        }
        Op::ClearEpg => {
            if current.epg.is_none() {
                skipped.push(
                    Skip::of(channel, group, "the channel has no guide assigned")
                        .with_op(kind, Field::Epg),
                );
                return;
            }
            if channel.has_epg_override() {
                skipped.push(
                    Skip::of(
                        channel,
                        group,
                        "an EPG override pins a different guide, so clearing the channel's own \
                 entry would not change what is shown",
                    )
                    .with_op(kind, Field::Epg),
                );
                return;
            }
            record(
                planned,
                Change::planned(
                    channel,
                    group,
                    kind,
                    Field::Epg,
                    Value::Epg(epg_values(world, current.epg)),
                    Value::Epg(None),
                ),
            );
            current.epg = None;
        }
    }
}

/// Record a change, folding it into an earlier change to the same field.
///
/// A second operation on the same field is not a second write: the API writes a field once,
/// so the plan keeps one row whose `after` is the final value.
fn record(planned: &mut Vec<Change>, change: Change) {
    match planned
        .iter_mut()
        .find(|existing| existing.field == change.field)
    {
        Some(existing) => {
            existing.after = change.after;
            existing.op = change.op;
        }
        None => planned.push(change),
    }
}

/// The group value for a group id, name included so the report needs no world.
fn group_values(world: &World, group: Option<i64>) -> Option<(i64, String)> {
    group.map(|id| (id, world.group_name(Some(id))))
}

/// The EPG entry value for an entry id, name included for the same reason.
fn epg_values(world: &World, entry: Option<i64>) -> Option<(i64, String)> {
    let id = entry?;
    let name = world
        .entries
        .iter()
        .find(|entry| entry.id == id)
        .map(|entry| entry.name.clone())
        .unwrap_or_else(|| format!("EPG {id}"));
    Some((id, name))
}

/// The guide entry of a channel, if it has one and the world carries entries.
fn entry_of<'a>(world: &'a World, channel: &Channel) -> Option<&'a EpgEntry> {
    let id = channel.current_epg_id()?;
    world.entries.iter().find(|entry| entry.id == id)
}

/// Fail early when the world was loaded without a slice the plan needs.
fn require_world(world: &World, selection: &Selection, ops: &[Op]) -> Result<()> {
    world.require(WorldKind::Channels)?;
    let needs_groups = !selection.groups.trim().is_empty()
        || !selection.ignore_groups.trim().is_empty()
        || ops.iter().any(|op| match op {
            Op::MoveGroup { .. } => true,
            // Without the groups slice a `{group}` renders as "No Group" for every
            // channel, which a preview would present as a real rename.
            Op::Rename {
                kind: RenameKind::Template(template),
            } => template.contains("{group}"),
            _ => false,
        });
    if needs_groups {
        world.require(WorldKind::Groups)?;
    }
    let needs_profiles = !selection.profiles.trim().is_empty()
        || !selection.hidden_in_profiles.trim().is_empty()
        || ops.iter().any(|op| matches!(op, Op::SetMembership { .. }));
    if needs_profiles {
        world.require(WorldKind::Profiles)?;
    }
    let needs_entries = selection.epg_name_regex.is_some()
        || ops.iter().any(|op| match op {
            Op::ClearEpg => true,
            Op::Rename {
                kind: RenameKind::Template(template),
            } => template.contains("{epg}"),
            _ => false,
        });
    if needs_entries {
        world.require(WorldKind::EpgEntries)?;
    }

    // A bad id in an operation is a configuration mistake an apply would turn into a 400 for
    // the whole chunk, so it fails here instead.
    for op in ops {
        match op {
            Op::Rename { kind } => match kind {
                RenameKind::Suffix(text) | RenameKind::Prefix(text) if text.is_empty() => bail!(
                    "a rename with an empty {} would change nothing; the plugin refuses an \
                     empty suffix too",
                    match kind {
                        RenameKind::Suffix(_) => "suffix",
                        _ => "prefix",
                    }
                ),
                RenameKind::Template(template) if template.trim().is_empty() => {
                    bail!("a rename template is empty")
                }
                _ => {}
            },
            Op::MoveGroup { group_id }
                if !world.groups.iter().any(|group| group.id == *group_id) =>
            {
                let available: Vec<String> = world
                    .groups
                    .iter()
                    .map(|group| format!("{} ({})", group.name, group.id))
                    .collect();
                bail!(
                    "no channel group with id {group_id}; available: {}",
                    if available.is_empty() {
                        "(none)".to_string()
                    } else {
                        available.join(", ")
                    }
                );
            }
            Op::SetMembership { profile_id, .. }
                if !world
                    .profiles
                    .iter()
                    .any(|profile| profile.id == *profile_id) =>
            {
                let available: Vec<String> = world
                    .profiles
                    .iter()
                    .map(|profile| format!("{} ({})", profile.name, profile.id))
                    .collect();
                bail!(
                    "no channel profile with id {profile_id}; available: {}",
                    if available.is_empty() {
                        "(none)".to_string()
                    } else {
                        available.join(", ")
                    }
                );
            }
            _ => {}
        }
    }
    Ok(())
}

/// The channels a selection resolves to, plus the skips worth reporting.
fn narrow<'a>(
    world: &World,
    scoped: &'a ScopedChannels,
    selection: &Selection,
    regex: Option<&Regex>,
    skipped: &mut Vec<Skip>,
) -> Result<Vec<&'a Channel>> {
    let mut channels: Vec<&Channel> = scoped.channels.iter().collect();

    if !selection.channel_ids.is_empty() {
        let wanted: HashSet<i64> = selection.channel_ids.iter().copied().collect();
        channels.retain(|channel| wanted.contains(&channel.id));

        // A stale result set is the case worth reporting: the operator should see that the
        // scan named a channel which is gone, or which the scope no longer covers.
        let in_scope: HashSet<i64> = scoped.channels.iter().map(|channel| channel.id).collect();
        let in_world: HashMap<i64, &Channel> = world
            .channels
            .iter()
            .map(|channel| (channel.id, channel))
            .collect();
        let mut reported: HashSet<i64> = HashSet::new();
        for id in &selection.channel_ids {
            if !reported.insert(*id) {
                continue;
            }
            match in_world.get(id) {
                None => skipped.push(Skip::missing(*id)),
                Some(channel) if !in_scope.contains(id) => skipped.push(Skip::of(
                    channel,
                    &world.group_name(channel.channel_group_id),
                    format!("outside the configured scope{}", scoped.filter_description),
                )),
                Some(_) => {}
            }
        }
    }

    if let Some(want) = selection.hidden_from_output {
        channels.retain(|channel| channel.hidden_from_output == want);
    }

    if !selection.hidden_in_profiles.trim().is_empty() {
        let hidden = hidden_channels(world, &selection.hidden_in_profiles)?;
        channels.retain(|channel| hidden.contains(&channel.id));
    }

    if selection.has_epg {
        channels.retain(|channel| channel.current_epg_id().is_some());
    }

    if let Some(regex) = regex {
        channels.retain(|channel| {
            entry_of(world, channel).is_some_and(|entry| regex.is_match(&entry.name))
        });
    }

    if !selection.expected_epg.is_empty() {
        let mut stale: Vec<Skip> = Vec::new();
        channels.retain(|channel| match selection.expected_epg.get(&channel.id) {
            None => true,
            Some(expected) if *expected == channel.current_epg_id() => true,
            Some(expected) => {
                let was = epg_values(world, *expected)
                    .map(|(_, name)| name)
                    .unwrap_or_else(|| "no guide".to_string());
                let now = epg_values(world, channel.current_epg_id())
                    .map(|(_, name)| name)
                    .unwrap_or_else(|| "no guide".to_string());
                stale.push(Skip::of(
                    channel,
                    &world.group_name(channel.channel_group_id),
                    format!(
                        "its guide changed since the scan (was {was}, now {now}), so it is no \
                         longer the channel the scan reported"
                    ),
                ));
                false
            }
        });
        skipped.extend(stale);
    }

    Ok(channels)
}

/// The channels DISABLED in any of these profiles, by profile name.
///
/// `remove_epg_from_hidden` (2581-2588) reads the disabled `ChannelProfileMembership` rows of
/// `channel_profile_name`. The profile serializer only lists the ENABLED ids, so a channel
/// absent from that list is treated as disabled — which is also how Dispatcharr itself treats
/// it. A profile name that does not exist is an error, with the plugin's wording.
///
/// The lookup is an EXACT match, like the plugin's `ChannelProfile.objects.get(name=pname)`:
/// `sports` does not find `Sports`. (The scope's own `profiles` filter IS case-insensitive,
/// like the plugin's scope filter at 1724-1727; the two actions genuinely differ here.)
fn hidden_channels(world: &World, profiles: &str) -> Result<HashSet<i64>> {
    let mut hidden: HashSet<i64> = HashSet::new();
    for name in parse_tokens(profiles) {
        let profile = world
            .profiles
            .iter()
            .find(|profile| profile.name == name)
            .with_context(|| format!("Channel profile '{name}' not found"))?;
        let enabled: HashSet<i64> = profile.channels.iter().copied().collect();
        hidden.extend(
            world
                .channels
                .iter()
                .map(|channel| channel.id)
                .filter(|id| !enabled.contains(id)),
        );
    }
    Ok(hidden)
}

/// Mark rows with an outcome.
fn mark(changes: &mut [Change], rows: &[usize], status: RowStatus, reason: &str) {
    for index in rows {
        changes[*index].status = status;
        changes[*index].reason = reason.to_string();
    }
}

fn number_cell(number: Option<f64>) -> String {
    number.map(|number| format!("{number}")).unwrap_or_default()
}

/// `'A', 'B'`, the way the scope's own prose lists things.
fn quote_list(items: &[String]) -> String {
    items
        .iter()
        .map(|item| format!("'{item}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::plan as plan_ops;
    use super::*;
    use crate::dispatcharr::ChannelProfile;
    use crate::store::RunRow;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn channel(id: i64, name: &str, group: Option<i64>, epg: Option<i64>) -> Channel {
        Channel {
            id,
            name: name.to_string(),
            channel_number: Some(serde_json::json!(id)),
            channel_group_id: group,
            epg_data_id: epg,
            ..Channel::default()
        }
    }

    fn world() -> World {
        World {
            channels: vec![
                channel(1, "ESPN", Some(10), Some(31)),
                channel(2, "CNN [BadEPG]", Some(10), Some(32)),
                channel(3, "PPV 1", Some(20), None),
                channel(4, "Télé-Québec", Some(20), Some(33)),
                channel(5, "Local", None, None),
            ],
            groups: vec![
                crate::dispatcharr::ChannelGroup {
                    id: 10,
                    name: "Sports".to_string(),
                },
                crate::dispatcharr::ChannelGroup {
                    id: 20,
                    name: "PPV Live".to_string(),
                },
            ],
            profiles: vec![
                ChannelProfile {
                    id: 1,
                    name: "All".to_string(),
                    channels: vec![1, 2, 3, 4, 5],
                },
                ChannelProfile {
                    id: 2,
                    name: "Favorites".to_string(),
                    channels: vec![1, 4],
                },
            ],
            entries: vec![
                EpgEntry {
                    id: 31,
                    name: "ESPN HD".to_string(),
                    ..EpgEntry::default()
                },
                EpgEntry {
                    id: 32,
                    name: "CNN [US]".to_string(),
                    ..EpgEntry::default()
                },
                EpgEntry {
                    id: 33,
                    name: "Náutica Ñ".to_string(),
                    ..EpgEntry::default()
                },
            ],
            ..World::default()
        }
    }

    fn scope() -> ScopeConfig {
        ScopeConfig::default()
    }

    fn selected(plan: &Plan) -> Vec<i64> {
        plan.selected.iter().map(|row| row.id).collect()
    }

    #[test]
    fn an_empty_scope_selects_everything() {
        let plan = plan_ops(&world(), &Selection::default(), &[Op::ClearEpg]).expect("plans");
        assert_eq!(selected(&plan), vec![1, 2, 3, 4, 5]);
        // Three of the five have a guide; the other two are reported as no-ops.
        assert_eq!(plan.changes.len(), 3);
        assert_eq!(plan.skipped.len(), 2);
    }

    #[test]
    fn the_scope_narrows_the_selection() {
        let mut scoped = scope();
        scoped.groups = "Spo*".to_string();
        let plan =
            plan_ops(&world(), &Selection::in_scope(&scoped), &[Op::ClearEpg]).expect("plans");
        assert_eq!(selected(&plan), vec![1, 2]);
        assert!(plan.description.contains("Sports"), "{}", plan.description);
    }

    #[test]
    fn a_hidden_selection_keeps_the_scope_groups_but_not_its_profiles() {
        // `[scope].groups` bounds every write, while `[scope].profiles` would be the wrong
        // filter here: hidden channels are by definition NOT enabled in a profile.
        let mut scoped = scope();
        scoped.groups = "Sports".to_string();
        scoped.profiles = "Favorites".to_string();

        let selection = Selection::hidden_in("Favorites", &scoped);
        assert_eq!(selection.profiles, "");
        let plan = plan_ops(&world(), &selection, &[Op::ClearEpg]).expect("plans");
        // Hidden in Favorites is {2, 3, 5}; the Sports scope leaves 2.
        assert_eq!(selected(&plan), vec![2]);
    }

    #[test]
    fn profile_scope_keeps_only_enabled_channels() {
        let mut scoped = scope();
        scoped.profiles = "favorites".to_string();
        let plan =
            plan_ops(&world(), &Selection::in_scope(&scoped), &[Op::ClearEpg]).expect("plans");
        assert_eq!(selected(&plan), vec![1, 4]);
    }

    #[test]
    fn hidden_in_profiles_selects_disabled_memberships() {
        // Everything but ESPN and Télé-Québec is disabled in Favorites.
        let selection = Selection::hidden_in("Favorites", &scope());
        let plan = plan_ops(&world(), &selection, &[Op::ClearEpg]).expect("plans");
        assert_eq!(selected(&plan), vec![2, 3, 5]);
        // CNN has a guide, PPV 1 and Local do not.
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].channel_id, 2);
    }

    #[test]
    fn an_unknown_hidden_profile_is_an_error() {
        let error = plan_ops(
            &world(),
            &Selection::hidden_in("Nope", &scope()),
            &[Op::ClearEpg],
        )
        .expect_err("rejects");
        assert!(
            format!("{error:#}").contains("Channel profile 'Nope' not found"),
            "{error:#}"
        );
    }

    #[test]
    fn a_hidden_profile_lookup_is_exact_like_the_plugin() {
        // The plugin reads the profile with `ChannelProfile.objects.get(name=pname)`:
        // `favorites` does not find `Favorites`. (The scope's own `profiles` filter is
        // case-insensitive, like the plugin's scope filter; this one follows the hidden
        // action instead.)
        let error = plan_ops(
            &world(),
            &Selection::hidden_in("favorites", &scope()),
            &[Op::ClearEpg],
        )
        .expect_err("rejects");
        assert!(
            format!("{error:#}").contains("Channel profile 'favorites' not found"),
            "{error:#}"
        );
    }

    #[test]
    fn hidden_and_visible_filters_read_the_channel_column() {
        let mut fixture = world();
        fixture.channels[0].hidden_from_output = true;

        let plan = plan_ops(
            &fixture,
            &Selection {
                hidden_from_output: Some(true),
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect("plans");
        assert_eq!(selected(&plan), vec![1]);

        let plan = plan_ops(
            &fixture,
            &Selection {
                hidden_from_output: Some(false),
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect("plans");
        assert_eq!(selected(&plan), vec![2, 3, 4, 5]);
    }

    #[test]
    fn has_epg_keeps_the_assigned_channels() {
        let plan = plan_ops(
            &world(),
            &Selection {
                has_epg: true,
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect("plans");
        assert_eq!(selected(&plan), vec![1, 2, 4]);
    }

    #[test]
    fn the_guide_name_regex_is_case_sensitive_and_unanchored() {
        // `remove_epg_by_regex` calls `re.compile(pattern).search(epg_name)`: "CNN" matches
        // "CNN [US]" anywhere, "cnn" matches nothing.
        let plan = plan_ops(
            &world(),
            &Selection {
                epg_name_regex: Some("CNN".to_string()),
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect("plans");
        assert_eq!(selected(&plan), vec![2]);

        let plan = plan_ops(
            &world(),
            &Selection {
                epg_name_regex: Some("cnn".to_string()),
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect("plans");
        assert!(plan.selected.is_empty());

        // Anchors and unicode behave like Python's.
        let plan = plan_ops(
            &world(),
            &Selection {
                epg_name_regex: Some("^N.*Ñ$".to_string()),
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect("plans");
        assert_eq!(selected(&plan), vec![4]);
    }

    #[test]
    fn a_blank_pattern_is_rejected() {
        let error = plan(
            &world(),
            &Selection {
                epg_name_regex: Some("   ".to_string()),
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect_err("rejects");
        assert!(
            format!("{error:#}").contains("would match every guide"),
            "{error:#}"
        );
    }

    #[test]
    fn a_python_only_pattern_is_rejected_with_the_reason() {
        // A backreference is Python-only; accepting it would match something else entirely.
        for pattern in [
            r"(ES)PN\1",
            "ES(?=PN)",
            "(?P<net>ES)PN(?P=net)",
            "ES(?<!X)PN",
        ] {
            let error = plan_ops(
                &world(),
                &Selection {
                    epg_name_regex: Some(pattern.to_string()),
                    ..Selection::default()
                },
                &[Op::ClearEpg],
            )
            .expect_err("rejects");
            let message = format!("{error:#}");
            assert!(message.contains("invalid EPG name pattern"), "{message}");
        }
    }

    #[test]
    fn an_escaped_pattern_compiles_and_matches_literally() {
        // Escaped parens are not a group, so they must not be mistaken for Python-only
        // syntax: `\(US\)` matches the guide literally named with parens.
        let plan = plan_ops(
            &world(),
            &Selection {
                epg_name_regex: Some(r"\(US\)".to_string()),
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect("plans");
        // "CNN [US]" carries `(US)` nowhere; what matters is that the pattern compiled
        // and selected by its literal meaning (nothing here).
        assert!(plan.selected.is_empty());
    }

    #[test]
    fn an_explicit_id_selection_reports_what_it_cannot_touch() {
        let mut fixture = world();
        fixture.channels.retain(|channel| channel.id != 4);
        let mut scoped = scope();
        scoped.groups = "Sports".to_string();

        let selection = Selection {
            channel_ids: vec![1, 3, 20],
            ..Selection::in_scope(&scoped)
        };
        let plan = plan_ops(&fixture, &selection, &[Op::ClearEpg]).expect("plans");
        // Only id 1 is in the world AND in scope.
        assert_eq!(selected(&plan), vec![1]);
        let reasons: Vec<&str> = plan
            .skipped
            .iter()
            .map(|skip| skip.reason.as_str())
            .collect();
        assert!(
            reasons
                .iter()
                .any(|reason| reason.contains("no longer exists")),
            "{reasons:?}"
        );
        assert!(
            reasons
                .iter()
                .any(|reason| reason.contains("outside the configured scope")),
            "{reasons:?}"
        );
    }

    #[test]
    fn a_stale_scan_does_not_clear_a_reassigned_channel() {
        // The scan saw id 1 on entry 31; it has since been moved to 32. Clearing it would
        // throw away the assignment somebody made after the scan.
        let mut fixture = world();
        fixture.channels[0].epg_data_id = Some(32);
        let rows = vec![RunRow {
            channel_id: 1,
            epg_data_id: Some(31),
            ..RunRow::default()
        }];
        let plan = plan_ops(
            &fixture,
            &Selection::from_scan_rows(&rows, &scope()),
            &[Op::ClearEpg],
        )
        .expect("plans");
        assert!(plan.changes.is_empty());
        assert_eq!(plan.skipped.len(), 1);
        assert!(
            plan.skipped[0]
                .reason
                .contains("guide changed since the scan"),
            "{}",
            plan.skipped[0].reason
        );
    }

    #[test]
    fn a_suffix_is_added_once_and_a_present_one_is_skipped() {
        let plan = plan_ops(
            &world(),
            &Selection::default(),
            &[Op::Rename {
                kind: RenameKind::Suffix(" [BadEPG]".to_string()),
            }],
        )
        .expect("plans");

        let names: HashMap<i64, &str> = plan
            .changes
            .iter()
            .filter_map(|change| match &change.after {
                Value::Name(name) => Some((change.channel_id, name.as_str())),
                _ => None,
            })
            .collect();
        assert_eq!(names[&1], "ESPN [BadEPG]");
        assert_eq!(names[&4], "Télé-Québec [BadEPG]");
        // CNN already carries it, and is reported rather than renamed twice.
        assert!(!names.contains_key(&2));
        assert!(plan
            .skipped
            .iter()
            .any(|skip| skip.channel_id == 2 && skip.reason.contains("already ends with")));

        // The same plan against the renamed world changes nothing: that is what makes a
        // stale result set safe to re-run.
        let mut after = world();
        for channel in &mut after.channels {
            if let Some(name) = names.get(&channel.id) {
                channel.name = (*name).to_string();
            }
        }
        let plan = plan_ops(
            &after,
            &Selection::default(),
            &[Op::Rename {
                kind: RenameKind::Suffix(" [BadEPG]".to_string()),
            }],
        )
        .expect("plans");
        assert!(plan.changes.is_empty());
    }

    #[test]
    fn a_coupled_suffix_and_clear_skips_an_already_tagged_channel_entirely() {
        // `add_bad_epg_suffix` with `remove_epg_with_suffix` (2508-2514): a channel that
        // already carries the suffix is skipped ENTIRELY, keeping its guide.
        let options = PlanOptions {
            suffix_present_skips_rest: true,
        };
        let plan = plan_with_options(
            &world(),
            &Selection::default(),
            &[
                Op::Rename {
                    kind: RenameKind::Suffix(" [BadEPG]".to_string()),
                },
                Op::ClearEpg,
            ],
            &options,
        )
        .expect("plans");

        // CNN already carries the suffix: neither a rename nor a clear.
        assert!(
            plan.changes.iter().all(|change| change.channel_id != 2),
            "{:#?}",
            plan.changes
        );
        let tagged: Vec<&Skip> = plan
            .skipped
            .iter()
            .filter(|skip| skip.channel_id == 2)
            .collect();
        // The rename no-op plus the coupled clear, which names the coupling.
        assert_eq!(tagged.len(), 2);
        assert!(tagged.iter().any(|skip| skip.field == Some(Field::Name)));
        assert!(tagged
            .iter()
            .any(|skip| skip.field == Some(Field::Epg) && skip.reason.contains("already tagged")));
        // The guide survives: no clear was even planned for the entry it is on.
        assert!(
            !plan
                .changes
                .iter()
                .any(|change| change.field == Field::Epg && change.before.wire() == "32"),
            "{:#?}",
            plan.changes
        );

        // Untouched channels get both changes.
        let espn: Vec<&Change> = plan
            .changes
            .iter()
            .filter(|change| change.channel_id == 1)
            .collect();
        assert_eq!(espn.len(), 2);
    }

    #[test]
    fn without_the_coupling_an_already_tagged_channel_is_still_cleared() {
        // The default is independent operations: the rename is a no-op skip and the clear
        // still runs. Only the `tag_missing` preset opts into the coupling above.
        let plan = plan_ops(
            &world(),
            &Selection::default(),
            &[
                Op::Rename {
                    kind: RenameKind::Suffix(" [BadEPG]".to_string()),
                },
                Op::ClearEpg,
            ],
        )
        .expect("plans");

        assert!(plan.skipped.iter().any(|skip| skip.channel_id == 2
            && skip.field == Some(Field::Name)
            && skip.reason.contains("already ends with")));
        let clear = plan
            .changes
            .iter()
            .find(|change| change.channel_id == 2 && change.field == Field::Epg)
            .expect("clear still planned");
        assert_eq!(clear.before.wire(), "32");
        assert_eq!(clear.after.wire(), "none");
    }

    #[test]
    fn the_coupling_drops_an_earlier_planned_change_too() {
        // The preset lists the rename first, so this is a hand-built order: the guard fires
        // after the move already planned, and neither change survives.
        let options = PlanOptions {
            suffix_present_skips_rest: true,
        };
        let plan = plan_with_options(
            &world(),
            &Selection {
                channel_ids: vec![2],
                ..Selection::default()
            },
            &[
                Op::MoveGroup { group_id: 20 },
                Op::Rename {
                    kind: RenameKind::Suffix(" [BadEPG]".to_string()),
                },
                Op::ClearEpg,
            ],
            &options,
        )
        .expect("plans");

        assert!(plan.changes.is_empty(), "{:#?}", plan.changes);
        assert!(
            plan.skipped
                .iter()
                .any(|skip| skip.reason.contains("already tagged")),
            "{:#?}",
            plan.skipped
        );
    }

    #[test]
    fn a_prefix_and_a_suffix_on_one_channel_chain_into_one_change() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![1],
                ..Selection::default()
            },
            &[
                Op::Rename {
                    kind: RenameKind::Prefix("[US] ".to_string()),
                },
                Op::Rename {
                    kind: RenameKind::Suffix(" [BadEPG]".to_string()),
                },
            ],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].before, Value::Name("ESPN".to_string()));
        assert_eq!(
            plan.changes[0].after,
            Value::Name("[US] ESPN [BadEPG]".to_string())
        );
    }

    #[test]
    fn a_template_fills_the_documented_placeholders_only() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![4],
                ..Selection::default()
            },
            &[Op::Rename {
                kind: RenameKind::Template("{name} ({group}) #{number} {epg} {id} {other}".into()),
            }],
        )
        .expect("plans");
        assert_eq!(
            plan.changes[0].after,
            Value::Name("Télé-Québec (PPV Live) #4 Náutica Ñ 4 {other}".to_string())
        );
    }

    #[test]
    fn an_empty_suffix_is_refused() {
        let error = plan_ops(
            &world(),
            &Selection::default(),
            &[Op::Rename {
                kind: RenameKind::Suffix(String::new()),
            }],
        )
        .expect_err("rejects");
        assert!(format!("{error:#}").contains("empty suffix"), "{error:#}");
    }

    #[test]
    fn moving_a_group_skips_a_channel_already_there() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![1, 3],
                ..Selection::default()
            },
            &[Op::MoveGroup { group_id: 20 }],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].channel_id, 1);
        assert_eq!(
            plan.changes[0].before,
            Value::Group(Some((10, "Sports".to_string())))
        );
        assert_eq!(
            plan.changes[0].after,
            Value::Group(Some((20, "PPV Live".to_string())))
        );
        // PPV 1 is already in group 20.
        assert!(plan
            .skipped
            .iter()
            .any(|skip| skip.channel_id == 3 && skip.reason.contains("already in group")));
    }

    #[test]
    fn an_unknown_group_is_refused() {
        let error = plan_ops(
            &world(),
            &Selection::default(),
            &[Op::MoveGroup { group_id: 99 }],
        )
        .expect_err("rejects");
        assert!(
            format!("{error:#}").contains("no channel group with id 99"),
            "{error:#}"
        );
    }

    #[test]
    fn membership_changes_only_when_it_differs() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![1, 3],
                ..Selection::default()
            },
            &[Op::SetMembership {
                profile_id: 2,
                enabled: false,
            }],
        )
        .expect("plans");
        // ESPN is enabled in Favorites, PPV 1 already is not.
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].field, Field::Membership(2));
        assert_eq!(plan.changes[0].before, Value::Flag(true));
        assert_eq!(plan.changes[0].after, Value::Flag(false));
        assert!(plan
            .skipped
            .iter()
            .any(|skip| skip.channel_id == 3 && skip.reason.contains("already disabled")));
    }

    #[test]
    fn visibility_changes_only_when_it_differs() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![1],
                ..Selection::default()
            },
            &[Op::SetHiddenFromOutput { hidden: true }],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].before, Value::Flag(false));
        assert_eq!(plan.changes[0].after, Value::Flag(true));

        let mut fixture = world();
        fixture.channels[0].hidden_from_output = true;
        let plan = plan_ops(
            &fixture,
            &Selection {
                channel_ids: vec![1],
                ..Selection::default()
            },
            &[Op::SetHiddenFromOutput { hidden: true }],
        )
        .expect("plans");
        assert!(plan.changes.is_empty());
        assert_eq!(plan.skipped.len(), 1);
    }

    #[test]
    fn clearing_a_guide_keeps_the_entry_it_had() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![1],
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect("plans");
        assert_eq!(
            plan.changes[0].before,
            Value::Epg(Some((31, "ESPN HD".to_string())))
        );
        assert_eq!(plan.changes[0].after, Value::Epg(None));
        assert_eq!(plan.changes[0].before.wire(), "31");
        assert_eq!(plan.changes[0].after.wire(), "none");
    }

    #[test]
    fn a_channel_with_an_epg_override_is_left_alone() {
        let mut fixture = world();
        fixture.channels[0].effective_epg_data_id = Some(32);
        let plan = plan_ops(
            &fixture,
            &Selection {
                channel_ids: vec![1],
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect("plans");
        assert!(plan.changes.is_empty());
        assert!(plan.skipped[0].reason.contains("override"));
    }

    #[test]
    fn a_profile_the_world_lacks_is_refused() {
        let error = plan_ops(
            &world(),
            &Selection::default(),
            &[Op::SetMembership {
                profile_id: 99,
                enabled: false,
            }],
        )
        .expect_err("rejects");
        assert!(
            format!("{error:#}").contains("no channel profile with id 99"),
            "{error:#}"
        );
    }

    #[test]
    fn a_lazily_loaded_world_says_which_slice_is_missing() {
        let mut fixture = world();
        fixture.loaded = WorldKind::Channels | WorldKind::Groups;
        let error =
            plan_ops(&fixture, &Selection::default(), &[Op::ClearEpg]).expect_err("rejects");
        assert!(format!("{error:#}").contains("EpgEntries"), "{error:#}");
    }

    #[test]
    fn a_group_template_needs_the_groups_slice() {
        // Without it every channel would render as "No Group" and the preview would
        // present that as a real rename.
        let mut fixture = world();
        fixture.loaded = WorldKind::Channels | WorldKind::EpgEntries;
        let error = plan_ops(
            &fixture,
            &Selection {
                channel_ids: vec![1],
                ..Selection::default()
            },
            &[Op::Rename {
                kind: RenameKind::Template("{name} ({group})".to_string()),
            }],
        )
        .expect_err("rejects");
        assert!(format!("{error:#}").contains("Groups"), "{error:#}");
    }

    #[test]
    fn a_field_label_round_trips_for_a_restore() {
        // A restore reads the store's `field` column back; every label `plan` writes must
        // parse, and anything else must be skippable rather than fatal.
        for field in [
            Field::Name,
            Field::Group,
            Field::Epg,
            Field::HiddenFromOutput,
            Field::Membership(2),
        ] {
            assert_eq!(Field::from_label(&field.label()), Some(field));
        }
        assert_eq!(Field::from_label(""), None);
        assert_eq!(Field::from_label("profile_membership:xx"), None);
    }

    // ---- apply -------------------------------------------------------------

    fn config(base_url: &str) -> crate::config::DispatcharrConfig {
        crate::config::DispatcharrConfig {
            base_url: base_url.to_string(),
            api_key: "test-key".to_string(),
            timeout_secs: 5,
            concurrency: 4,
            ..crate::config::DispatcharrConfig::default()
        }
    }

    #[tokio::test]
    async fn applying_batches_one_request_per_endpoint() {
        let server = MockServer::start().await;
        // One edit row carrying both fields of channel 1, and a second channel.
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .and(body_json(serde_json::json!([
                {"id": 1, "name": "ESPN [BadEPG]", "channel_group_id": 20},
                {"id": 4, "name": "Télé-Québec [BadEPG]"}
            ])))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"message": "ok", "channels": [{"id": 1}, {"id": 4}]}),
            ))
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/profiles/2/channels/bulk-update/"))
            .and(body_json(serde_json::json!({
                "channels": [
                    {"channel_id": 1, "enabled": false},
                    {"channel_id": 4, "enabled": false}
                ]
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"updated": 2, "created": 0})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/channels/channels/batch-set-epg/"))
            .and(body_json(serde_json::json!({
                "associations": [
                    {"channel_id": 1, "epg_data_id": null},
                    {"channel_id": 4, "epg_data_id": null}
                ]
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"success": true, "channels_updated": 1})),
            )
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![1, 4],
                ..Selection::default()
            },
            &[
                Op::Rename {
                    kind: RenameKind::Suffix(" [BadEPG]".to_string()),
                },
                Op::MoveGroup { group_id: 20 },
                Op::SetMembership {
                    profile_id: 2,
                    enabled: false,
                },
                Op::ClearEpg,
            ],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 7);

        let report = apply(&client, &plan, &context, "guide.tag_missing_apply").await;
        assert!(report.all_ok(), "{:#?}", report.changes);
        assert_eq!(report.ok, 7);
        assert_eq!(report.failed, 0);
        assert!(report
            .changes
            .iter()
            .all(|change| change.status == RowStatus::Ok));
    }

    #[tokio::test]
    async fn a_failing_batch_fails_its_rows_and_carries_the_message() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"message": "ok", "channels": [{"id": 1}]})),
            )
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/profiles/2/channels/bulk-update/"))
            .respond_with(
                ResponseTemplate::new(400).set_body_string("{\"detail\":\"invalid channel\"}"),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/channels/channels/batch-set-epg/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"success": true, "channels_updated": 1})),
            )
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![1],
                ..Selection::default()
            },
            &[
                Op::Rename {
                    kind: RenameKind::Suffix(" [BadEPG]".to_string()),
                },
                Op::SetMembership {
                    profile_id: 2,
                    enabled: false,
                },
                Op::ClearEpg,
            ],
        )
        .expect("plans");

        let report = apply(&client, &plan, &context, "guide.tag_missing_apply").await;
        assert!(!report.all_ok());
        assert_eq!(report.failed, 1);
        let membership = report
            .changes
            .iter()
            .find(|change| change.field == Field::Membership(2))
            .expect("membership row");
        assert_eq!(membership.status, RowStatus::Failed);
        assert!(membership.reason.contains("400"), "{}", membership.reason);
        // The other endpoints still ran.
        assert!(report
            .changes
            .iter()
            .filter(|change| change.field != Field::Membership(2))
            .all(|change| change.status == RowStatus::Ok));
    }

    #[tokio::test]
    async fn a_cancelled_run_writes_nothing() {
        let server = MockServer::start().await;
        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        context.cancel();
        let plan = plan_ops(&world(), &Selection::default(), &[Op::ClearEpg]).expect("plans");

        let report = apply(&client, &plan, &context, "guide.clear_groups_apply").await;
        assert_eq!(report.ok, 0);
        assert_eq!(report.failed, plan.changes.len());
        assert!(report
            .changes
            .iter()
            .all(|change| change.reason == CANCELLED));
        assert_eq!(
            server.received_requests().await.unwrap_or_default().len(),
            0
        );
    }

    #[tokio::test]
    async fn an_empty_plan_issues_no_writes() {
        // Every endpoint is guarded by its row list: a plan with nothing to do must not
        // touch the API at all (upstream answers 400 to empty payloads).
        let server = MockServer::start().await;
        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let plan = Plan::default();

        let report = apply(&client, &plan, &context, "guide.clear_groups_apply").await;
        assert!(report.all_ok());
        assert_eq!(report.ok, 0);
        assert_eq!(
            server.received_requests().await.unwrap_or_default().len(),
            0
        );
    }

    // ---- rows --------------------------------------------------------------

    #[test]
    fn run_rows_carry_what_a_restore_needs() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![1, 3],
                ..Selection::default()
            },
            &[
                Op::Rename {
                    kind: RenameKind::Suffix(" [BadEPG]".to_string()),
                },
                Op::ClearEpg,
            ],
        )
        .expect("plans");
        let rows = run_rows(&plan, None);

        let rename = rows
            .iter()
            .find(|row| row.field == "name")
            .expect("rename row");
        assert_eq!(rename.method, "rename");
        assert_eq!(rename.channel_name, "ESPN");
        assert_eq!(rename.before_value.as_deref(), Some("ESPN"));
        assert_eq!(rename.after_value.as_deref(), Some("ESPN [BadEPG]"));
        assert_eq!(rename.status, "planned");

        let clear = rows
            .iter()
            .find(|row| row.field == "epg_data_id")
            .expect("clear row");
        assert_eq!(clear.before_value.as_deref(), Some("31"));
        assert_eq!(clear.after_value.as_deref(), Some("none"));
        assert_eq!(clear.current_epg_name.as_deref(), Some("ESPN HD"));

        // No-ops are recorded too, so a run explains what it did not touch.
        let skip = rows
            .iter()
            .find(|row| row.status == "skipped")
            .expect("skip row");
        assert_eq!(skip.channel_id, 3);
        assert!(skip.reason.contains("no guide assigned"));
    }

    #[test]
    fn an_applied_report_turns_the_rows_into_outcomes() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![1],
                ..Selection::default()
            },
            &[Op::ClearEpg],
        )
        .expect("plans");
        let mut applied = ApplyReport {
            changes: plan.changes.clone(),
            ok: 1,
            failed: 0,
        };
        applied.changes[0].status = RowStatus::Ok;

        assert_eq!(run_rows(&plan, Some(&applied))[0].status, "ok");
        // Without a report the row stays "planned", which is what a dry run records.
        assert_eq!(run_rows(&plan, None)[0].status, "planned");
    }

    #[test]
    fn the_csv_carries_the_columns_and_the_skips() {
        use crate::subapp::{FieldKind, FieldSpec, FieldValue, SettingsSchema, SettingsValues};
        use crate::test_app::StubApp;

        let dir = tempfile::tempdir().expect("tempdir");
        let config = crate::config::CrosstalkConfig {
            config_dir: dir.path().to_path_buf(),
            ..crate::config::CrosstalkConfig::default()
        };
        let context = JobContext::for_tests(config);
        let schema = SettingsSchema::new(vec![FieldSpec::new(
            "bad_epg_suffix",
            "_s",
            "Bad EPG Suffix",
            FieldKind::Text,
            FieldValue::Text(" [BadEPG]".to_string()),
        )]);
        let values = SettingsValues::new();

        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: vec![1, 2],
                ..Selection::default()
            },
            &[Op::Rename {
                kind: RenameKind::Suffix(" [BadEPG]".to_string()),
            }],
        )
        .expect("plans");
        let app = StubApp::new();
        let spec = ReportSpec {
            app: &app,
            schema: &schema,
            values: &values,
            stem: "tag_missing_preview",
            title: "Tag Missing EPG Preview Report",
        };
        let path = write_ops_report(&context, &spec, &plan, None).expect("writes");

        let text = std::fs::read_to_string(&path).expect("read");
        assert!(text.contains(
            "channel_id,channel_name,channel_number,group,field,before,after,status,reason"
        ));
        assert!(text.contains("ESPN [BadEPG]"), "{text}");
        // The skip explains the channel that already carries the suffix.
        assert!(text.contains("skipped"), "{text}");
        assert!(text.contains("already ends with"), "{text}");
        assert!(
            text.contains("Changes written: none, this was a preview"),
            "{text}"
        );
    }

    #[test]
    fn the_selection_prints_itself() {
        let selection = Selection {
            groups: "Sports".to_string(),
            has_epg: true,
            epg_name_regex: Some("US".to_string()),
            ..Selection::default()
        };
        let plan = plan_ops(&world(), &selection, &[Op::ClearEpg]).expect("plans");
        assert!(plan.description.contains("Sports"), "{}", plan.description);
        assert!(
            plan.description.contains("guide assigned"),
            "{}",
            plan.description
        );
        assert!(plan.description.contains("\"US\""), "{}", plan.description);
    }
}
