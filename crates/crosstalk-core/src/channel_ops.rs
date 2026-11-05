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
//!   * a move to "No Group" ([`Op::MoveGroup`] with `group_id: None`) is planned and reported
//!     but cannot be APPLIED over REST: Dispatcharr's `ChannelSerializer` declares
//!     `channel_group_id` as a `PrimaryKeyRelatedField` without `allow_null`, so a null is a
//!     400 for the whole `edit/bulk` chunk. [`apply`] refuses those rows with that reason
//!     instead of sending a payload that fails. The column is nullable and the model allows
//!     it (deleting a group leaves its channels with none), so this is an API gap rather than
//!     a crosstalk limitation, and a bridge-backed writer can close it later.
//!
//! Without [`PlanOptions::suffix_present_skips_rest`] the operations are independent: a
//! rename that is a no-op is reported as a skip and the rest still run, which is what
//! `triage`/`visibility` need when only one of their operations applies to a channel. The
//! `tag_missing` preset opts into the plugin's coupling instead: `add_bad_epg_suffix` with
//! `remove_epg_with_suffix` skips an already-suffixed channel ENTIRELY, so it keeps its
//! guide too (2508-2514).
//!
//! # Per-channel values
//!
//! Most operations apply one value to every selected channel. Restore, visibility and stream
//! lists need the other shape — a different value per channel — and [`Op::Set`] is it: a map
//! from channel id to the value that one channel gets, with every channel the map does not
//! name left untouched.
//!
//! It is an [`Op`] rather than a `Plan` constructor on purpose. [`plan`] stays the ONE place
//! that re-reads the current state of each channel, decides what is a no-op and records the
//! before values a later restore reads; a plan built from changes would compute those
//! somewhere else, over a world that may already be stale, and the two paths would drift.
//! Being an op also means a per-channel write gets the selection, the scope intersection, the
//! CSV, the run rows and the batched apply for free — the same guarantees every other write
//! has.
//!
//! No-op dropping stays per channel: a channel the map names whose current value already
//! equals the requested one is reported in [`Plan::skipped`], exactly like a uniform op that
//! changes nothing, while a channel the map does not name gets no row at all (it is outside
//! the operation's target set rather than a no-op).
//!
//! # Stream lists
//!
//! [`Field::Streams`] carries a channel's whole ordered stream list, so stream assignment
//! gets the same plan/preview/CSV/run-row/restore treatment as a rename. The plan compares
//! against the order the channel already has ([`Channel::streams`]) and only a different list
//! becomes a change. Applying it goes through a [`StreamListWriter`] — a seam, not a detail:
//! the REST implementation writes hundreds of channels per `edit/bulk` request (falling back
//! to one PATCH per channel to attribute a rejected chunk), and a Dispatcharr plugin (the
//! bridge) can replace it behind the same trait if REST ever stops being fast enough.

use crate::config::ScopeConfig;
use crate::csv_report::{self, Report};
use crate::dispatcharr::{
    Channel, ChannelEdit, DispatcharrClient, EpgEntry, Set, Stream, World, WorldKind,
};
use crate::jobs::JobContext;
use crate::scope::{scope_channels, ScopedChannels};
use crate::store::{RunRow, Subject};
use crate::subapp::{SettingsSchema, SettingsValues, SubApp};
use crate::wildcard::parse_tokens;
use anyhow::{bail, Context, Result};
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
///
/// Kept in sync with [`crate::naming::CHANNEL_PLACEHOLDERS`]: the template engine renders
/// the value, this constant names the set for reports and settings help.
pub const TEMPLATE_PLACEHOLDERS: [&str; 5] = ["{name}", "{group}", "{number}", "{epg}", "{id}"];

/// Progress is published at most this often, like the plugin's own two-second flush.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(2);

/// Stream ids per `streams` wire value: the run-row form a restore parses back.
const STREAM_WIRE_SEPARATOR: char = ',';

/// How many entries of an ordered stream list the report prints before summarizing.
const STREAMS_SHOWN: usize = 6;

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
/// allows". The one exception is [`Self::channel_ids`]: an empty id LIST selects nothing.
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
    ///
    /// `None` is no id filter; `Some` of an EMPTY list selects NOTHING. The distinction is the
    /// safety rail: a scan that found no channel stores no rows, and reading that as "no
    /// filter" would hand every in-scope channel to `clear_missing`.
    pub channel_ids: Option<Vec<i64>>,
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
    ///
    /// Only [`Subject::Channel`] rows are read. A run's rows can now be about a provider
    /// stream or an EPG source, whose `channel_id` column holds an id from another table
    /// entirely: reading one of those as a channel id would aim the selection at whatever
    /// channel happens to carry that number.
    pub fn from_scan_rows(rows: &[RunRow], scope: &ScopeConfig) -> Self {
        let mut selection = Self::in_scope(scope);
        let mut ids: Vec<i64> = Vec::with_capacity(rows.len());
        let mut seen: HashSet<i64> = HashSet::with_capacity(rows.len());
        for row in rows.iter().filter(|row| row.subject == Subject::Channel) {
            if seen.insert(row.channel_id) {
                ids.push(row.channel_id);
            }
            selection
                .expected_epg
                .insert(row.channel_id, row.epg_data_id);
        }
        selection.channel_ids = Some(ids);
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
        if let Some(ids) = &self.channel_ids {
            parts.push(format!("{} named channel id(s)", ids.len()));
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
    /// Move every selected channel into this group; `None` is "No Group".
    MoveGroup {
        group_id: Option<i64>,
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
    /// Set one field to a value PER CHANNEL: what restore, visibility and stream lists need.
    ///
    /// A channel missing from `values` is left untouched and gets no row; a channel the map
    /// names whose current value already equals the requested one is reported as a skip, so
    /// no-op dropping keeps working per channel. Every [`SetValue`] has to match the field
    /// (a `Name` for `name`, a `Group` for `channel_group_id`, …) and [`plan`] refuses a
    /// mismatched pair rather than silently dropping it.
    Set {
        field: Field,
        // JSON and TOML objects only have string keys, so the map is deserialized by
        // `channel_values` rather than by serde's own map decoder.
        #[serde(deserialize_with = "channel_values")]
        values: HashMap<i64, SetValue>,
    },
}

/// Deserialize a per-channel value map from an object keyed by channel id.
///
/// serde's object keys are strings (both in JSON and in TOML), so a `HashMap<i64, _>` cannot
/// be decoded directly; the keys are parsed here instead. The in-code shape stays a map,
/// because that is what a restore and visibility build.
fn channel_values<'de, D>(deserializer: D) -> std::result::Result<HashMap<i64, SetValue>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: HashMap<String, SetValue> = HashMap::deserialize(deserializer)?;
    raw.into_iter()
        .map(|(key, value)| {
            let id = key.trim().parse::<i64>().map_err(|_| {
                serde::de::Error::custom(format!(
                    "channel id {key:?} in a per-channel set is not an integer"
                ))
            })?;
            Ok((id, value))
        })
        .collect()
}

/// The value one channel gets from [`Op::Set`], in the shape its field takes.
///
/// Separate from [`Value`] because these are the values a caller ASKS for, while [`Value`] is
/// what a plan reports (ids with the names they print as, so the report needs no world).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetValue {
    /// A channel name.
    Name(String),
    /// A channel group; `None` is "No Group".
    Group(Option<i64>),
    /// An EPG entry; `None` unassigns the guide.
    Epg(Option<i64>),
    /// `hidden_from_output`, or a profile membership's `enabled`.
    Flag(bool),
    /// An ordered stream list, replacing the channel's own.
    Streams(Vec<i64>),
}

impl SetValue {
    /// What this value is, in words, for a refusal's message.
    fn shape(&self) -> &'static str {
        match self {
            Self::Name(_) => "name",
            Self::Group(_) => "group",
            Self::Epg(_) => "EPG entry",
            Self::Flag(_) => "flag",
            Self::Streams(_) => "stream list",
        }
    }

    /// Whether this value can be written to that field. A `name` takes a [`Self::Name`] and
    /// nothing else; a membership and `hidden_from_output` both take a [`Self::Flag`].
    fn fits(&self, field: Field) -> bool {
        matches!(
            (field, self),
            (Field::Name, Self::Name(_))
                | (Field::Group, Self::Group(_))
                | (Field::Epg, Self::Epg(_))
                | (Field::HiddenFromOutput, Self::Flag(_))
                | (Field::Membership(_), Self::Flag(_))
                | (Field::Streams, Self::Streams(_))
        )
    }
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
            Self::Set { .. } => OpKind::Set,
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
            Self::Set { field, .. } => *field,
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
    ///
    /// Templates render through the shared engine ([`crate::naming::render_channel`]);
    /// suffix/prefix keep their own exact-match guards (the guide's `tag_missing` parity).
    fn produce(&self, current: &str, values: &crate::naming::ChannelValues) -> Option<String> {
        let name = match self {
            Self::Suffix(text) => format!("{current}{text}"),
            Self::Prefix(text) => format!("{text}{current}"),
            Self::Template(template) => crate::naming::render_channel(template, values),
        };
        if name.trim().is_empty() {
            None
        } else {
            Some(name)
        }
    }
}

/// One field a change writes, as the report and a restore name it.
///
/// The label is both the API's own field name (it is what a CSV row and a run row carry, and
/// what [`Self::from_label`] parses back) and the serialized form, so an [`Op::Set`] names its
/// field the same way a restore reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Field {
    Name,
    Group,
    Epg,
    HiddenFromOutput,
    /// A profile membership; the id is the profile.
    Membership(i64),
    /// The channel's ordered stream list.
    Streams,
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
            Self::Streams => "streams".to_string(),
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
            "streams" => Some(Self::Streams),
            _ => label
                .strip_prefix("profile_membership:")
                .and_then(|id| id.parse().ok())
                .map(Self::Membership),
        }
    }
}

impl Serialize for Field {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.label())
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let label = String::deserialize(deserializer)?;
        Self::from_label(&label).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "unknown channel field {label:?}; use name, channel_group_id, epg_data_id, \
                 hidden_from_output, streams or profile_membership:<id>"
            ))
        })
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
    /// A per-channel [`Op::Set`]. The report's `field` column says which field it wrote; this
    /// only says the value came per channel.
    Set,
}

impl OpKind {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Rename => "rename",
            Self::MoveGroup => "move_group",
            Self::SetMembership => "set_membership",
            Self::SetHiddenFromOutput => "set_hidden_from_output",
            Self::ClearEpg => "clear_epg",
            Self::Set => "set",
        }
    }
}

/// One stream of a plan's ordered list: the id the API writes, and the name it prints as when
/// the run loaded streams. The name is cosmetic — a world without the streams slice still
/// plans and applies the same list, it just prints ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedStream {
    pub id: i64,
    pub name: Option<String>,
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
    /// A channel's ordered stream list, in playback order.
    Streams(Vec<ListedStream>),
}

impl Value {
    /// How the report prints it: names, not ids, and `yes`/`no` for a flag.
    ///
    /// A long stream list is counted rather than printed whole: a report line is for reading,
    /// and a channel can carry dozens of streams. A short one prints its names (or ids, when
    /// the run loaded no streams).
    pub fn display(&self) -> String {
        match self {
            Self::Name(name) => name.clone(),
            Self::Group(Some((_, name))) => name.clone(),
            Self::Group(None) => "No Group".to_string(),
            Self::Epg(Some((_, name))) => name.clone(),
            Self::Epg(None) => "No EPG".to_string(),
            Self::Flag(flag) => if *flag { "yes" } else { "no" }.to_string(),
            Self::Streams(list) => display_streams(list),
        }
    }

    /// The machine value a restore reads back: an id, `none`, `true`/`false`, or the stream
    /// ids of a list (comma-separated, `none` for an empty one).
    pub fn wire(&self) -> String {
        match self {
            Self::Name(name) => name.clone(),
            Self::Group(Some((id, _))) | Self::Epg(Some((id, _))) => id.to_string(),
            Self::Group(None) | Self::Epg(None) => "none".to_string(),
            Self::Flag(flag) => flag.to_string(),
            Self::Streams(list) if list.is_empty() => "none".to_string(),
            Self::Streams(list) => list
                .iter()
                .map(|stream| stream.id.to_string())
                .collect::<Vec<_>>()
                .join(&STREAM_WIRE_SEPARATOR.to_string()),
        }
    }

    /// The stream ids of an ordered list, for the writer a stream change goes through.
    pub fn stream_ids(&self) -> Option<Vec<i64>> {
        match self {
            Self::Streams(list) => Some(list.iter().map(|stream| stream.id).collect()),
            _ => None,
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
    /// Resolve rename collisions through the shared engine
    /// ([`crate::naming::resolve_collisions`], Name Cleaner's suffix/numeric/skip
    /// strategies, against every channel name in the world) after all operations planned.
    ///
    /// Opt-in per plan: `None` (the default) plans exactly what the operations say, which
    /// is what the guide's `tag_missing` preset relies on for its parity. Lineup's
    /// clean/names set this; a `skip` reports the loser as a collision,
    /// `suffix`/`numeric` rewrite the loser's planned name.
    pub collision: Option<crate::naming::CollisionStrategy>,
    /// The separator `suffix` collisions join with (`" "`, Name Cleaner's
    /// `join_separator`). Ignored unless [`Self::collision`] is set.
    pub collision_sep: String,
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
    // Built once: a guide or stream lookup per channel against ~55k rows would be quadratic.
    // The stream index is only worth building when an operation writes a stream list.
    let needs_streams = ops.iter().any(|op| op.field() == Field::Streams);
    let lookups = Lookups::of(world, needs_streams);
    let mut skipped: Vec<Skip> = Vec::new();
    let selected = narrow(
        world,
        &lookups,
        &scoped,
        selection,
        regex.as_ref(),
        &mut skipped,
    )?;

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
                &lookups,
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

    if let Some(strategy) = options.collision {
        resolve_plan_collisions(
            world,
            &mut changes,
            &mut skipped,
            strategy,
            &options.collision_sep,
        );
    }

    Ok(Plan {
        description: selection.describe(&scoped.filter_description),
        selected: selected_rows,
        changes,
        skipped,
    })
}

/// Apply the opt-in collision strategy to a plan's rename changes.
///
/// Only [`Field::Name`] changes participate, against EVERY channel name in the world
/// (global, like Name Cleaner: see [`crate::naming`] for why not per group), minus the old
/// names the renames free — a rename frees its own slot for a sibling, exactly like the
/// engine's first pass. A resolved name rewrites the change's `after`; a `Skip` loser, and a
/// change the resolver turns back into its own current name, loses its change and gains a
/// `skipped` row. Channel-ops carries no cleaning info, so `suffix` has no removed quality to
/// re-append and behaves like `numeric`.
fn resolve_plan_collisions(
    world: &World,
    changes: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
    strategy: crate::naming::CollisionStrategy,
    sep: &str,
) {
    use crate::naming::PlannedName;
    let mut planned: Vec<PlannedName> = Vec::new();
    for change in changes.iter() {
        if change.field != Field::Name {
            continue;
        }
        let (Value::Name(old_name), Value::Name(new_name)) = (&change.before, &change.after) else {
            continue;
        };
        planned.push(PlannedName {
            id: change.channel_id,
            old_name: old_name.clone(),
            new_name: new_name.clone(),
            removed_tags: Vec::new(),
        });
    }
    if planned.is_empty() {
        return;
    }
    let existing: Vec<String> = world
        .channels
        .iter()
        .map(|channel| channel.name.clone())
        .collect();
    let resolved = crate::naming::resolve_collisions(&planned, &existing, strategy, sep);
    let resolved: HashMap<i64, crate::naming::ResolvedName> =
        resolved.into_iter().map(|row| (row.id, row)).collect();
    let mut kept: Vec<Change> = Vec::with_capacity(changes.len());
    for mut change in changes.drain(..) {
        let row = (change.field == Field::Name)
            .then(|| resolved.get(&change.channel_id))
            .flatten();
        let Some(row) = row else {
            kept.push(change);
            continue;
        };
        let reason = if row.skipped {
            Some(format!(
                "the rename to {:?} collides with another channel's name and the plan's \
                 collision strategy is skip, so the name is left as {:?}",
                row.final_name,
                change.before.display(),
            ))
        } else if Value::Name(row.final_name.clone()) == change.before {
            Some(format!(
                "the rename collides with another channel's name, and the collision \
                 strategy resolves it back to the current name {:?}, so nothing changes",
                change.before.display(),
            ))
        } else {
            None
        };
        match reason {
            Some(reason) => skipped.push(Skip {
                channel_id: change.channel_id,
                channel_name: change.channel_name.clone(),
                channel_number: change.channel_number,
                channel_group: change.channel_group.clone(),
                op: Some(change.op),
                field: Some(change.field),
                reason,
            }),
            None => {
                change.after = Value::Name(row.final_name.clone());
                kept.push(change);
            }
        }
    }
    *changes = kept;
}

/// Writes many channels' ordered stream lists: the seam every stream-list write goes through.
///
/// `edit/bulk` carries hundreds of channels per request, but the request PATTERN is still
/// the whole cost at ~20k channels — and a Dispatcharr plugin (the bridge, which can write
/// `ChannelStream` rows in one transaction) may replace REST entirely. Keeping that decision
/// behind a trait keeps it out of the plan, the report and every action: [`apply`] uses
/// [`RestStreamListWriter`]; [`apply_with_writer`] takes any implementation.
///
/// The batch form is the primitive on purpose. One invalid row rejects its whole `edit/bulk`
/// chunk, so per-channel results are what lets a caller attribute a failure to its channel
/// instead of failing hundreds of rows over one bad id: the REST writer falls back to one
/// PATCH per channel for a rejected chunk, and every entry of the answer is one channel's
/// outcome. `Ok` means the server confirmed the write; an `Err` is that channel's failure
/// and does not stop the others. The answer is in input order, one entry per channel.
#[async_trait::async_trait]
pub trait StreamListWriter: Send + Sync {
    /// Replace each channel's stream list, in playback order (index 0 plays first).
    async fn write_many(&self, lists: &[(i64, Vec<i64>)]) -> Vec<(i64, Result<()>)>;
}

/// The REST implementation: `edit/bulk` rows in chunks of 500, one PATCH per channel when a
/// chunk is rejected.
///
/// [`DispatcharrClient::bulk_set_channel_streams`] owns the chunking, the retry and the
/// fallback; this type only picks the client. Re-applying the same lists rewrites the same
/// orders, so a retried write is safe.
pub struct RestStreamListWriter {
    client: DispatcharrClient,
}

impl RestStreamListWriter {
    /// The writer [`apply`] uses.
    pub fn new(client: DispatcharrClient) -> Self {
        Self { client }
    }
}

impl std::fmt::Debug for RestStreamListWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestStreamListWriter")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl StreamListWriter for RestStreamListWriter {
    async fn write_many(&self, lists: &[(i64, Vec<i64>)]) -> Vec<(i64, Result<()>)> {
        self.client.bulk_set_channel_streams(lists).await
    }
}

/// Apply a plan, one request per endpoint, recording every row's outcome.
///
/// The writes are batched the way the API wants them: a rename, a group move and a hide are
/// three fields of one `edit/bulk` row, memberships go one call per profile, the guides are
/// cleared through `batch-set-epg`, and stream lists go through a [`StreamListWriter`] in one
/// batched write. `action` is the id the progress events are published under;
/// cancellation is checked between batches, so a cancelled run stops at the last batch it
/// finished rather than mid-request.
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
    let writer = RestStreamListWriter::new(client.clone());
    apply_with_writer(client, plan, context, action, &writer).await
}

/// [`apply`] with a caller-supplied [`StreamListWriter`].
///
/// The hook a bridge-backed writer needs; everything except the stream-list stage is the same
/// code [`apply`] runs.
pub async fn apply_with_writer(
    client: &DispatcharrClient,
    plan: &Plan,
    context: &JobContext,
    action: &str,
    writer: &dyn StreamListWriter,
) -> ApplyReport {
    let mut changes = plan.changes.clone();
    let total = changes.len() as u64;
    let mut progress = context.paced(action, total, PROGRESS_INTERVAL);
    let mut done = 0u64;

    // 1. Names, groups and visibility: one `edit/bulk` request.
    let mut edits: BTreeMap<i64, ChannelEdit> = BTreeMap::new();
    let mut edit_rows: Vec<usize> = Vec::new();
    let mut no_group_rows: Vec<usize> = Vec::new();
    for (index, change) in plan.changes.iter().enumerate() {
        if !matches!(
            change.field,
            Field::Name | Field::Group | Field::HiddenFromOutput
        ) {
            continue;
        }
        if change.field == Field::Group && matches!(&change.after, Value::Group(None)) {
            // The one channel edit the API cannot express, and one invalid row rejects its
            // whole chunk — so it is reported here rather than sent. A channel that also has a
            // rename in this plan keeps it: each field is its own row.
            no_group_rows.push(index);
            continue;
        }
        edit_rows.push(index);
        let edit = edits
            .entry(change.channel_id)
            .or_insert_with(|| ChannelEdit::new(change.channel_id));
        match (&change.field, &change.after) {
            (Field::Name, Value::Name(name)) => edit.name = Some(name.clone()),
            // A `None` group target was filtered out above; this endpoint cannot clear one.
            (Field::Group, Value::Group(Some((id, _)))) => {
                edit.channel_group_id = Set::Value(*id);
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
    if !no_group_rows.is_empty() {
        // A cancelled run says so, like every other batch it never reached; otherwise the
        // reason is the API's own gap.
        let reason = if context.is_cancelled() {
            CANCELLED
        } else {
            NO_GROUP_UNSUPPORTED
        };
        mark(&mut changes, &no_group_rows, RowStatus::Failed, reason);
        done += no_group_rows.len() as u64;
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

    // 3. Guides: one `batch-set-epg` call, which chunks and retries internally. A clear and a
    //    per-channel assignment are the same write, differing only in the id sent.
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
            progress.set(
                done,
                &format!("writing {} guide assignment(s)", associations.len()),
            );
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
        done += epg_rows.len() as u64;
    }

    // 4. Stream lists: one batched write through the writer, which is the seam a
    //    bridge-backed implementation replaces. A rejected chunk does not fail its siblings:
    //    the writer attributes failures per channel (the REST one retries a rejected chunk's
    //    rows one at a time), so every row below gets its own outcome, like the edit/bulk
    //    stage above gives its rows.
    let stream_rows: Vec<usize> = plan
        .changes
        .iter()
        .enumerate()
        .filter(|(_, change)| change.field == Field::Streams && change.after.stream_ids().is_some())
        .map(|(index, _)| index)
        .collect();
    if !stream_rows.is_empty() {
        if context.is_cancelled() {
            mark(&mut changes, &stream_rows, RowStatus::Failed, CANCELLED);
        } else {
            let lists: Vec<(i64, Vec<i64>)> = stream_rows
                .iter()
                .map(|index| {
                    let change = &plan.changes[*index];
                    (
                        change.channel_id,
                        change.after.stream_ids().unwrap_or_default(),
                    )
                })
                .collect();
            progress.set(done, &format!("writing {} stream list(s)", lists.len()));
            // One change per field per channel: `record` folds a second write to the same
            // field, so a channel id names exactly one row.
            let by_channel: HashMap<i64, usize> = stream_rows
                .iter()
                .map(|index| (plan.changes[*index].channel_id, *index))
                .collect();
            let mut answered: HashSet<usize> = HashSet::with_capacity(stream_rows.len());
            for (channel_id, result) in writer.write_many(&lists).await {
                if let Some(index) = by_channel.get(&channel_id) {
                    answered.insert(*index);
                    match result {
                        Ok(()) => {
                            changes[*index].status = RowStatus::Ok;
                            changes[*index].reason.clear();
                        }
                        Err(error) => {
                            changes[*index].status = RowStatus::Failed;
                            changes[*index].reason = format!("{error:#}");
                        }
                    }
                }
            }
            // A writer that answers fewer rows than it was given is a bug, not a success:
            // the unanswered rows fail loudly rather than sitting at `planned` forever.
            for index in &stream_rows {
                if !answered.contains(index) {
                    changes[*index].status = RowStatus::Failed;
                    changes[*index].reason = "the stream-list writer returned no result for \
                     this channel"
                        .to_string();
                }
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

/// Why a "No Group" move fails. See the module docs: the API cannot express it, so sending it
/// would fail the whole chunk it shares — and take the renames in that chunk down with it.
const NO_GROUP_UNSUPPORTED: &str =
    "not applied: Dispatcharr's channel edit cannot clear a channel group (its serializer \
     rejects a null channel_group_id), so this move has to be made in the Dispatcharr UI";

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
    /// Extra `Label: value` lines for the preamble. A preset uses them to say which result set
    /// it selected from (the guide's cleanup actions name the scan run and when it ran), so a
    /// report read weeks later explains where its channel list came from.
    pub notes: Vec<(String, String)>,
}

impl<'a> ReportSpec<'a> {
    pub fn new(
        app: &'a dyn SubApp,
        schema: &'a SettingsSchema,
        values: &'a SettingsValues,
        stem: &'a str,
        title: &'a str,
    ) -> Self {
        Self {
            app,
            schema,
            values,
            stem,
            title,
            notes: Vec::new(),
        }
    }

    /// Add a `Label: value` line to the preamble.
    pub fn note(mut self, label: impl Into<String>, value: impl Into<String>) -> Self {
        self.notes.push((label.into(), value.into()));
        self
    }
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
    let report = spec
        .notes
        .iter()
        .fold(report, |report, (label, value)| report.result(label, value));

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

/// What a channel-operations action says when the run was cancelled: `None` when it was not.
///
/// The rule itself lives in [`crate::jobs`] — a cancelled run keeps its counts, its report and
/// the rows that landed, and is never recorded as a success — so what is left to each action is
/// the wording. One wording, because four actions (the guide's cleanup presets and the streams
/// app's `sort`, `mirror` and `visibility`) all end the same way, and an operator comparing two
/// run pages should not have to work out why they read differently.
pub fn cancelled_note(
    context: &crate::jobs::JobContext,
    plan: &Plan,
    applied: Option<&ApplyReport>,
) -> Option<String> {
    if !context.is_cancelled() {
        return None;
    }
    let total = plan.changes.len();
    match applied {
        None => Some("cancelled by the operator; this preview wrote nothing".to_string()),
        Some(applied) if applied.ok == 0 => Some(
            "cancelled by the operator before any change was written; every row says why it \
             was not applied"
                .to_string(),
        ),
        Some(applied) => Some(format!(
            "cancelled by the operator after {} of {total} change(s) landed; the other {} were \
             not written",
            applied.ok,
            total.saturating_sub(applied.ok)
        )),
    }
}

/// The plan's rows in the store's shape, for `JobContext::record_rows`.
///
/// `field`/`before_value`/`after_value` are the generic slots a later `restore` reads: the
/// writable field's name and its two machine values (an id, `none`, the stream ids of a list,
/// or `true`/`false`). The identity columns keep the state the run found, so a rename can be
/// undone from its own row. Every row here is about a channel — that is what these operations
/// write — which is what `subject` says, so a reader of a mixed run cannot mistake one for a
/// stream or EPG source row.
pub fn run_rows(plan: &Plan, applied: Option<&ApplyReport>) -> Vec<RunRow> {
    let changes: &[Change] = applied.map_or(&plan.changes, |report| report.changes.as_slice());
    let mut rows = Vec::with_capacity(changes.len() + plan.skipped.len());
    for (ordinal, change) in changes.iter().enumerate() {
        rows.push(RunRow {
            ordinal: ordinal as i64,
            subject: Subject::Channel,
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
            subject: Subject::Channel,
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

/// What a per-channel template substitutes: the shared engine's
/// [`crate::naming::ChannelValues`], kept here under its old name so the call
/// sites read unchanged. The rendering itself moved to
/// [`crate::naming::render_channel`] (byte-identical output).
type Placeholders = crate::naming::ChannelValues;

/// A channel's mutable state while its operations are evaluated.
struct Working {
    name: String,
    group: Option<i64>,
    epg: Option<i64>,
    hidden: bool,
    streams: Vec<i64>,
}

impl Working {
    fn of(channel: &Channel) -> Self {
        Self {
            name: channel.name.clone(),
            group: channel.channel_group_id,
            epg: channel.current_epg_id(),
            hidden: channel.hidden_from_output,
            streams: channel.streams.clone(),
        }
    }
}

/// Evaluate one operation against a channel, recording a change or a skip.
///
/// `options` and `guarded` are the suffix/clear coupling of [`PlanOptions`]: when the
/// options arm it and this rename is a no-op because the suffix is already present, the
/// suffix is stored in `guarded` and the plan loop skips every later operation.
///
/// A per-channel [`Op::Set`] is resolved here, once, and each of its values goes through the
/// same handler a uniform operation does: a per-channel group move cannot drift from a
/// [`Op::MoveGroup`], and per-channel no-op dropping is the uniform one.
///
/// Ten parameters, like the AI gate's `ask`: one per thing an evaluation threads through,
/// and bundling them would only move the list behind a struct literal at this one call site.
#[allow(clippy::too_many_arguments)]
fn apply_op(
    world: &World,
    lookups: &Lookups<'_>,
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
                epg: epg_values(lookups, current.epg)
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
            plan_group(
                channel, group, kind, *group_id, world, current, planned, skipped,
            );
        }
        Op::SetMembership {
            profile_id,
            enabled,
        } => {
            plan_membership(
                channel,
                group,
                kind,
                *profile_id,
                *enabled,
                world,
                planned,
                skipped,
            );
        }
        Op::SetHiddenFromOutput { hidden } => {
            plan_hidden(channel, group, kind, *hidden, current, planned, skipped);
        }
        Op::ClearEpg => {
            plan_epg(
                channel, group, kind, None, lookups, current, planned, skipped,
            );
        }
        Op::Set { field, values } => {
            // A channel the map does not name is outside this operation's target set: no
            // change and no row, rather than one skip per untouched channel.
            let Some(value) = values.get(&channel.id) else {
                return;
            };
            match (field, value) {
                (Field::Name, SetValue::Name(name)) => {
                    plan_name(channel, group, kind, name, current, planned, skipped);
                }
                (Field::Group, SetValue::Group(target)) => {
                    plan_group(
                        channel, group, kind, *target, world, current, planned, skipped,
                    );
                }
                (Field::Epg, SetValue::Epg(target)) => {
                    plan_epg(
                        channel, group, kind, *target, lookups, current, planned, skipped,
                    );
                }
                (Field::HiddenFromOutput, SetValue::Flag(hidden)) => {
                    plan_hidden(channel, group, kind, *hidden, current, planned, skipped);
                }
                (Field::Membership(profile_id), SetValue::Flag(enabled)) => plan_membership(
                    channel,
                    group,
                    kind,
                    *profile_id,
                    *enabled,
                    world,
                    planned,
                    skipped,
                ),
                (Field::Streams, SetValue::Streams(ids)) => {
                    plan_streams(
                        channel, group, kind, ids, lookups, current, planned, skipped,
                    );
                }
                // `require_world` refuses a value that does not fit its field before a plan
                // starts, so reaching here is a bug rather than a configuration mistake.
                (_, value) => skipped.push(
                    Skip::of(
                        channel,
                        group,
                        format!(
                            "a value of type {} cannot be written to {}",
                            value.shape(),
                            field.label()
                        ),
                    )
                    .with_op(kind, *field),
                ),
            }
        }
    }
}

/// Plan a name this operation states outright: the per-channel form of a rename.
///
/// A rename renders one (suffix, prefix, template); this one is already a name. The two share
/// the refusals that matter — an empty name, or the name the channel already has — so a
/// restore of a name cannot write something a rename would have skipped.
fn plan_name(
    channel: &Channel,
    group: &str,
    kind: OpKind,
    name: &str,
    current: &mut Working,
    planned: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
) {
    if name.trim().is_empty() {
        // Dispatcharr rejects a blank name; a per-channel value could carry one.
        skipped.push(
            Skip::of(
                channel,
                group,
                "the name would be empty, which would be rejected",
            )
            .with_op(kind, Field::Name),
        );
        return;
    }
    if current.name == name {
        skipped.push(
            Skip::of(channel, group, "the channel already has this name")
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
            Value::Name(current.name.clone()),
            Value::Name(name.to_string()),
        ),
    );
    current.name = name.to_string();
}

/// Plan a group, from [`Op::MoveGroup`] or from a per-channel value.
///
/// A target of `None` is "No Group", which `edit/bulk` writes as a null `channel_group_id`.
#[allow(clippy::too_many_arguments)]
fn plan_group(
    channel: &Channel,
    group: &str,
    kind: OpKind,
    target: Option<i64>,
    world: &World,
    current: &mut Working,
    planned: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
) {
    if current.group == target {
        let reason = match target {
            Some(id) => format!(
                "the channel is already in group {}",
                quote_list(&[world.group_name(Some(id))])
            ),
            None => "the channel already has no group".to_string(),
        };
        skipped.push(Skip::of(channel, group, reason).with_op(kind, Field::Group));
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
            Value::Group(group_values(world, target)),
        ),
    );
    current.group = target;
}

/// Plan a profile membership.
#[allow(clippy::too_many_arguments)]
fn plan_membership(
    channel: &Channel,
    group: &str,
    kind: OpKind,
    profile_id: i64,
    enabled: bool,
    world: &World,
    planned: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
) {
    let Some(profile) = world
        .profiles
        .iter()
        .find(|profile| profile.id == profile_id)
    else {
        // `plan` validated the profile against the world; reaching here is a bug.
        return;
    };
    let now = profile.channels.contains(&channel.id);
    if now == enabled {
        skipped.push(
            Skip::of(
                channel,
                group,
                format!(
                    "the channel is already {} in profile {}",
                    if enabled { "enabled" } else { "disabled" },
                    quote_list(std::slice::from_ref(&profile.name))
                ),
            )
            .with_op(kind, Field::Membership(profile_id)),
        );
        return;
    }
    record(
        planned,
        Change::planned(
            channel,
            group,
            kind,
            Field::Membership(profile_id),
            Value::Flag(now),
            Value::Flag(enabled),
        ),
    );
}

/// Plan the global `hidden_from_output` flag.
fn plan_hidden(
    channel: &Channel,
    group: &str,
    kind: OpKind,
    hidden: bool,
    current: &mut Working,
    planned: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
) {
    if current.hidden == hidden {
        skipped.push(
            Skip::of(
                channel,
                group,
                format!(
                    "the channel is already {}",
                    if hidden {
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
            Value::Flag(hidden),
        ),
    );
    current.hidden = hidden;
}

/// Plan the guide: [`Op::ClearEpg`] is a `None` target, a per-channel value may assign one.
#[allow(clippy::too_many_arguments)]
fn plan_epg(
    channel: &Channel,
    group: &str,
    kind: OpKind,
    target: Option<i64>,
    lookups: &Lookups<'_>,
    current: &mut Working,
    planned: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
) {
    if current.epg == target {
        let reason = match target {
            Some(_) => "the channel is already on this guide".to_string(),
            None => "the channel has no guide assigned".to_string(),
        };
        skipped.push(Skip::of(channel, group, reason).with_op(kind, Field::Epg));
        return;
    }
    if target.is_none() && channel.has_epg_override() {
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
            Value::Epg(epg_values(lookups, current.epg)),
            Value::Epg(epg_values(lookups, target)),
        ),
    );
    current.epg = target;
}

/// Plan an ordered stream list: the channel's whole list, in playback order.
///
/// The comparison is against the order the channel already has, not against a set: swapping
/// two streams is a real change, because the order decides which one plays first.
#[allow(clippy::too_many_arguments)]
fn plan_streams(
    channel: &Channel,
    group: &str,
    kind: OpKind,
    ids: &[i64],
    lookups: &Lookups<'_>,
    current: &mut Working,
    planned: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
) {
    if current.streams == ids {
        skipped.push(
            Skip::of(
                channel,
                group,
                "the channel's stream list is already this one, in this order",
            )
            .with_op(kind, Field::Streams),
        );
        return;
    }
    record(
        planned,
        Change::planned(
            channel,
            group,
            kind,
            Field::Streams,
            Value::Streams(lookups.streams(&current.streams)),
            Value::Streams(lookups.streams(ids)),
        ),
    );
    current.streams = ids.to_vec();
}

/// A stream list as the report prints it: names when the run has them, ids otherwise, and a
/// count instead of the tail of a long one.
fn display_streams(list: &[ListedStream]) -> String {
    if list.is_empty() {
        return "no streams".to_string();
    }
    let shown: Vec<String> = list
        .iter()
        .take(STREAMS_SHOWN)
        .map(|stream| match &stream.name {
            Some(name) => name.clone(),
            None => stream.id.to_string(),
        })
        .collect();
    let mut text = shown.join(", ");
    if list.len() > STREAMS_SHOWN {
        text.push_str(&format!(", … ({} total)", list.len()));
    }
    text
}

/// Parse a `streams` wire value back into ids, for a restore.
///
/// `none` (and an empty string) is the EMPTY list: a channel with no streams is a real value a
/// restore may have to put back, which is not the same as "no value was recorded".
pub fn parse_stream_ids(wire: &str) -> Option<Vec<i64>> {
    let wire = wire.trim();
    if wire.is_empty() || wire.eq_ignore_ascii_case("none") {
        return Some(Vec::new());
    }
    wire.split(STREAM_WIRE_SEPARATOR)
        .map(|id| id.trim().parse::<i64>().ok())
        .collect()
}

/// The rows of a run that wrote one field, in run order — what a later restore reverses.
///
/// Selected by the store's `field` column, never by `channel_id` alone: a run can carry rows
/// about several subjects (a probe's streams, a watchdog's EPG sources), and only the channel
/// rows of the field asked for are something a channel operation can put back. `status` is not
/// filtered here; a caller decides for itself whether a `planned` (preview) row counts.
pub fn rows_for_field(rows: &[RunRow], field: Field) -> Vec<&RunRow> {
    let label = field.label();
    rows.iter()
        .filter(|row| row.subject == Subject::Channel && row.field == label)
        .collect()
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

/// Guide entries and streams by id, for the per-channel lookups of one plan.
///
/// Both are optional detail rather than state: a guide name is printed in the report, and a
/// stream name in a stream list. A world loaded without one of those slices plans and applies
/// the same changes, it just prints the id instead of the name.
struct Lookups<'a> {
    entries: HashMap<i64, &'a EpgEntry>,
    streams: HashMap<i64, &'a Stream>,
}

impl<'a> Lookups<'a> {
    /// Index the world's guide entries, and its streams when the plan needs them.
    fn of(world: &'a World, with_streams: bool) -> Self {
        Self {
            entries: world
                .entries
                .iter()
                .map(|entry| (entry.id, entry))
                .collect(),
            streams: if with_streams {
                world
                    .streams
                    .iter()
                    .map(|stream| (stream.id, stream))
                    .collect()
            } else {
                HashMap::new()
            },
        }
    }

    /// The ordered list as a plan records it: ids, with the names the world knows.
    fn streams(&self, ids: &[i64]) -> Vec<ListedStream> {
        ids.iter()
            .map(|id| ListedStream {
                id: *id,
                name: self
                    .streams
                    .get(id)
                    .map(|stream| stream.name.clone())
                    .filter(|name| !name.is_empty()),
            })
            .collect()
    }
}

/// The group value for a group id, name included so the report needs no world.
fn group_values(world: &World, group: Option<i64>) -> Option<(i64, String)> {
    group.map(|id| (id, world.group_name(Some(id))))
}

/// The EPG entry value for an entry id, name included for the same reason.
fn epg_values(lookups: &Lookups<'_>, entry: Option<i64>) -> Option<(i64, String)> {
    let id = entry?;
    let name = lookups
        .entries
        .get(&id)
        .map(|entry| entry.name.clone())
        .unwrap_or_else(|| format!("EPG {id}"));
    Some((id, name))
}

/// The guide entry of a channel, if it has one and the world carries entries.
fn entry_of<'a>(lookups: &Lookups<'a>, channel: &Channel) -> Option<&'a EpgEntry> {
    let id = channel.current_epg_id()?;
    lookups.entries.get(&id).copied()
}

/// Fail early when the world was loaded without a slice the plan needs.
fn require_world(world: &World, selection: &Selection, ops: &[Op]) -> Result<()> {
    world.require(WorldKind::Channels)?;
    let needs_groups = !selection.groups.trim().is_empty()
        || !selection.ignore_groups.trim().is_empty()
        || ops.iter().any(|op| match op {
            // A `None` target is "No Group", which needs no group list to name.
            Op::MoveGroup { group_id } => group_id.is_some(),
            // Without the groups slice a `{group}` renders as "No Group" for every
            // channel, which a preview would present as a real rename.
            Op::Rename {
                kind: RenameKind::Template(template),
            } => template.contains("{group}"),
            Op::Set {
                field: Field::Group,
                ..
            } => true,
            _ => false,
        });
    if needs_groups {
        world.require(WorldKind::Groups)?;
    }
    let needs_profiles = !selection.profiles.trim().is_empty()
        || !selection.hidden_in_profiles.trim().is_empty()
        || ops.iter().any(|op| {
            matches!(
                op,
                Op::SetMembership { .. }
                    | Op::Set {
                        field: Field::Membership(_),
                        ..
                    }
            )
        });
    if needs_profiles {
        world.require(WorldKind::Profiles)?;
    }
    let needs_entries = selection.epg_name_regex.is_some()
        || ops.iter().any(|op| match op {
            Op::ClearEpg => true,
            Op::Rename {
                kind: RenameKind::Template(template),
            } => template.contains("{epg}"),
            Op::Set {
                field: Field::Epg, ..
            } => true,
            _ => false,
        });
    if needs_entries {
        world.require(WorldKind::EpgEntries)?;
    }
    // A stream list is planned from the channel's own `streams` ids, which come with the
    // channel list; the streams slice only adds the names a report prints, so it is not
    // required here and a run without it writes exactly the same list.

    // A bad group or profile id in an operation is a configuration mistake an apply would turn
    // into a 400 for the whole chunk, so it fails here instead. Stream and guide ids are left
    // to the write: the plan runs without those slices, and a bad one fails its own row.
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
            Op::MoveGroup { group_id } => require_group(world, *group_id)?,
            Op::SetMembership { profile_id, .. } => require_profile(world, *profile_id)?,
            Op::Set { field, values } => {
                // A value of the wrong shape for its field would silently write nothing, so
                // it is refused here with the channel that carried it.
                let mut groups: Vec<i64> = Vec::new();
                for (channel_id, value) in values {
                    if !value.fits(*field) {
                        bail!(
                            "channel {channel_id}: a value of type {} cannot be written to \
                             {}; a Set writes values of the field's own shape",
                            value.shape(),
                            field.label()
                        );
                    }
                    match (field, value) {
                        (Field::Group, SetValue::Group(Some(group_id))) => groups.push(*group_id),
                        (Field::Membership(profile_id), _) => require_profile(world, *profile_id)?,
                        _ => {}
                    }
                }
                for group_id in groups {
                    require_group(world, Some(group_id))?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Fail when a group id is not in the world, naming the ones that are.
fn require_group(world: &World, group_id: Option<i64>) -> Result<()> {
    let Some(group_id) = group_id else {
        // "No Group" is a group id of null, which no group list has to contain.
        return Ok(());
    };
    if world.groups.iter().any(|group| group.id == group_id) {
        return Ok(());
    }
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
    )
}

/// Fail when a profile id is not in the world, naming the ones that are.
fn require_profile(world: &World, profile_id: i64) -> Result<()> {
    if world
        .profiles
        .iter()
        .any(|profile| profile.id == profile_id)
    {
        return Ok(());
    }
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
    )
}

/// The channels a selection resolves to, plus the skips worth reporting.
fn narrow<'a>(
    world: &World,
    lookups: &Lookups<'_>,
    scoped: &'a ScopedChannels,
    selection: &Selection,
    regex: Option<&Regex>,
    skipped: &mut Vec<Skip>,
) -> Result<Vec<&'a Channel>> {
    let mut channels: Vec<&Channel> = scoped.channels.iter().collect();

    if let Some(ids) = &selection.channel_ids {
        let wanted: HashSet<i64> = ids.iter().copied().collect();
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
        for id in ids {
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
            entry_of(lookups, channel).is_some_and(|entry| regex.is_match(&entry.name))
        });
    }

    if !selection.expected_epg.is_empty() {
        let mut stale: Vec<Skip> = Vec::new();
        channels.retain(|channel| match selection.expected_epg.get(&channel.id) {
            None => true,
            Some(expected) if *expected == channel.current_epg_id() => true,
            Some(expected) => {
                let was = epg_values(lookups, *expected)
                    .map(|(_, name)| name)
                    .unwrap_or_else(|| "no guide".to_string());
                let now = epg_values(lookups, channel.current_epg_id())
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
            streams: vec![
                crate::dispatcharr::Stream {
                    id: 100,
                    name: "ESPN HD (primary)".to_string(),
                    ..crate::dispatcharr::Stream::default()
                },
                crate::dispatcharr::Stream {
                    id: 101,
                    name: "ESPN HD (backup)".to_string(),
                    ..crate::dispatcharr::Stream::default()
                },
                crate::dispatcharr::Stream {
                    id: 102,
                    name: "CNN (primary)".to_string(),
                    ..crate::dispatcharr::Stream::default()
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
    fn an_empty_id_list_selects_nothing() {
        // A scan that found nothing stores no rows; its selection must be empty, not "every
        // channel in scope".
        let world = world();
        for selection in [
            Selection {
                channel_ids: Some(Vec::new()),
                ..Selection::default()
            },
            Selection::from_scan_rows(&[], &scope()),
        ] {
            let plan = plan_ops(&world, &selection, &[Op::ClearEpg]).expect("plan");
            assert!(plan.selected.is_empty(), "{selection:?}");
            assert!(plan.changes.is_empty());
        }
    }

    #[test]
    fn an_explicit_id_selection_reports_what_it_cannot_touch() {
        let mut fixture = world();
        fixture.channels.retain(|channel| channel.id != 4);
        let mut scoped = scope();
        scoped.groups = "Sports".to_string();

        let selection = Selection {
            channel_ids: Some(vec![1, 3, 20]),
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
            ..PlanOptions::default()
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
            ..PlanOptions::default()
        };
        let plan = plan_with_options(
            &world(),
            &Selection {
                channel_ids: Some(vec![2]),
                ..Selection::default()
            },
            &[
                Op::MoveGroup { group_id: Some(20) },
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
    fn collisions_are_opt_in_and_global() {
        let ops = [Op::Rename {
            kind: RenameKind::Template("Same".to_string()),
        }];
        let selection = Selection {
            channel_ids: Some(vec![1, 3]),
            ..Selection::default()
        };
        // Default: no collision strategy, both renames planned verbatim (guide
        // parity: `tag_missing` never opts in).
        let plain = plan_ops(&world(), &selection, &ops).expect("plans");
        assert_eq!(plain.changes.len(), 2);
        assert!(plain
            .changes
            .iter()
            .all(|change| change.after == Value::Name("Same".to_string())));
        // Opt-in numeric: different groups (Sports vs PPV Live) collide too, because
        // Dispatcharr's outputs list channels flat.
        let options = PlanOptions {
            collision: Some(crate::naming::CollisionStrategy::Numeric),
            collision_sep: " ".to_string(),
            ..PlanOptions::default()
        };
        let plan = plan_with_options(&world(), &selection, &ops, &options).expect("plans");
        let mut afters: Vec<String> = plan
            .changes
            .iter()
            .map(|change| change.after.display())
            .collect();
        afters.sort();
        assert_eq!(afters, vec!["Same (2)".to_string(), "Same (3)".to_string()]);
        // Same group as well: channels 1 and 2 both land on "Same".
        let selection = Selection {
            channel_ids: Some(vec![1, 2]),
            ..Selection::default()
        };
        let plan = plan_with_options(&world(), &selection, &ops, &options).expect("plans");
        let mut afters: Vec<String> = plan
            .changes
            .iter()
            .map(|change| change.after.display())
            .collect();
        afters.sort();
        // Both deferred the unique pass (two claimants), so id order goes numeric.
        assert_eq!(afters, vec!["Same (2)".to_string(), "Same (3)".to_string()]);
        // Skip reports the loser instead of renaming it.
        let options = PlanOptions {
            collision: Some(crate::naming::CollisionStrategy::Skip),
            ..PlanOptions::default()
        };
        let plan = plan_with_options(&world(), &selection, &ops, &options).expect("plans");
        // Both rows claimed one name, so both deferred and both report a collision.
        assert!(plan.changes.is_empty(), "{:#?}", plan.changes);
        assert_eq!(
            plan.skipped
                .iter()
                .filter(|skip| skip.reason.contains("collision"))
                .count(),
            2
        );
    }

    #[test]
    fn a_prefix_and_a_suffix_on_one_channel_chain_into_one_change() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: Some(vec![1]),
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
                channel_ids: Some(vec![4]),
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
                channel_ids: Some(vec![1, 3]),
                ..Selection::default()
            },
            &[Op::MoveGroup { group_id: Some(20) }],
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
            &[Op::MoveGroup { group_id: Some(99) }],
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
                channel_ids: Some(vec![1, 3]),
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
                channel_ids: Some(vec![1]),
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
                channel_ids: Some(vec![1]),
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
                channel_ids: Some(vec![1]),
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
                channel_ids: Some(vec![1]),
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
                channel_ids: Some(vec![1]),
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
            Field::Streams,
        ] {
            assert_eq!(Field::from_label(&field.label()), Some(field));
        }
        assert_eq!(Field::from_label(""), None);
        assert_eq!(Field::from_label("profile_membership:xx"), None);
        assert_eq!(Field::from_label("no_such_field"), None);
    }

    // ---- per-channel values and stream lists -------------------------------

    fn set_op(field: Field, values: &[(i64, SetValue)]) -> Op {
        Op::Set {
            field,
            values: values.iter().cloned().collect(),
        }
    }

    #[test]
    fn a_per_channel_set_writes_only_the_channels_it_names() {
        // Visibility's shape: enable one channel, disable the other, and a channel the map
        // does not name gets neither a change nor a skip.
        let plan = plan_ops(
            &world(),
            &Selection::default(),
            &[set_op(
                Field::HiddenFromOutput,
                &[(1, SetValue::Flag(false)), (2, SetValue::Flag(true))],
            )],
        )
        .expect("plans");
        assert_eq!(plan.selected.len(), 5);
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].channel_id, 2);
        assert_eq!(plan.changes[0].before, Value::Flag(false));
        assert_eq!(plan.changes[0].after, Value::Flag(true));
        // ESPN is already shown, which is a no-op for the one channel the map names.
        let skipped: Vec<i64> = plan.skipped.iter().map(|skip| skip.channel_id).collect();
        assert_eq!(skipped, vec![1]);
        assert!(plan.skipped[0].reason.contains("already shown"));
    }

    #[test]
    fn a_per_channel_set_drops_no_ops_per_channel() {
        // Restore's shape: the values come from a run, and a channel that already has the
        // value it is asked for must not be written again.
        let plan = plan_ops(
            &world(),
            &Selection::default(),
            &[set_op(
                Field::Name,
                &[
                    (1, SetValue::Name("ESPN [recovered]".to_string())),
                    (4, SetValue::Name("Télé-Québec".to_string())),
                ],
            )],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].channel_id, 1);
        assert_eq!(plan.changes[0].op.label(), "set");
        assert_eq!(plan.changes[0].field, Field::Name);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].channel_id, 4);
        assert!(plan.skipped[0].reason.contains("already has this name"));
    }

    #[test]
    fn a_per_channel_set_writes_a_group_and_can_clear_the_guide() {
        let plan = plan_ops(
            &world(),
            &Selection::default(),
            &[set_op(
                Field::Group,
                &[(1, SetValue::Group(None)), (3, SetValue::Group(Some(10)))],
            )],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 2);
        let espn = plan
            .changes
            .iter()
            .find(|change| change.channel_id == 1)
            .expect("ESPN moves");
        assert_eq!(espn.before, Value::Group(Some((10, "Sports".to_string()))));
        assert_eq!(espn.after, Value::Group(None));
        // `none` is what `edit/bulk` turns into a null `channel_group_id`.
        assert_eq!(espn.after.wire(), "none");

        let plan = plan_ops(
            &world(),
            &Selection::default(),
            &[set_op(
                Field::Epg,
                &[(1, SetValue::Epg(None)), (3, SetValue::Epg(Some(31)))],
            )],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 2);
        assert!(plan.changes.iter().any(|change| change.channel_id == 3
            && change.after == Value::Epg(Some((31, "ESPN HD".to_string())))));
        // PPV 1 already has no guide, and the map asks for none.
        assert!(!plan.changes.iter().any(|change| change.channel_id == 5));
    }

    #[test]
    fn a_per_channel_membership_set_reads_the_flag_per_profile() {
        let plan = plan_ops(
            &world(),
            &Selection::default(),
            &[set_op(
                Field::Membership(2),
                &[(1, SetValue::Flag(false)), (3, SetValue::Flag(true))],
            )],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 2);
        assert!(plan
            .changes
            .iter()
            .all(|change| change.field == Field::Membership(2)));
    }

    #[test]
    fn a_per_channel_set_refuses_a_value_of_the_wrong_shape() {
        let error = plan_ops(
            &world(),
            &Selection::default(),
            &[set_op(Field::Name, &[(1, SetValue::Group(Some(10)))])],
        )
        .expect_err("rejects");
        let message = format!("{error:#}");
        assert!(
            message.contains("channel 1: a value of type group cannot be written to name"),
            "{message}"
        );
    }

    #[test]
    fn a_per_channel_set_refuses_a_group_the_world_lacks() {
        let error = plan_ops(
            &world(),
            &Selection::default(),
            &[set_op(Field::Group, &[(1, SetValue::Group(Some(99)))])],
        )
        .expect_err("rejects");
        assert!(
            format!("{error:#}").contains("no channel group with id 99"),
            "{error:#}"
        );
    }

    #[test]
    fn a_per_channel_set_of_nothing_touches_nothing() {
        // A restore with no rows has nothing to put back; that is an empty plan, not an
        // error and not "every channel".
        let plan = plan_ops(
            &world(),
            &Selection::default(),
            &[Op::Set {
                field: Field::Name,
                values: HashMap::new(),
            }],
        )
        .expect("plans");
        assert_eq!(plan.selected.len(), 5);
        assert!(plan.changes.is_empty());
        assert!(plan.skipped.is_empty());
    }

    #[test]
    fn a_per_channel_op_parses_from_json() {
        // The JSON harness (`examples/channel_ops_io.rs`) and a future config both spell an op
        // this way: the field by its API label, the values as an object keyed by channel id.
        let op: Op = serde_json::from_value(serde_json::json!({
            "op": "set",
            "field": "streams",
            "values": {"1": {"streams": [101, 100]}, "3": {"streams": []}}
        }))
        .expect("parses");
        assert_eq!(op.kind(), OpKind::Set);
        assert_eq!(op.field(), Field::Streams);
        match &op {
            Op::Set { values, .. } => {
                assert_eq!(values.get(&1), Some(&SetValue::Streams(vec![101, 100])));
                assert_eq!(values.get(&3), Some(&SetValue::Streams(Vec::new())));
            }
            other => panic!("unexpected op {other:?}"),
        }

        // A move to "No Group", and the label that names a profile membership.
        let op: Op = serde_json::from_value(serde_json::json!({
            "op": "move_group", "group_id": null
        }))
        .expect("parses");
        assert_eq!(op, Op::MoveGroup { group_id: None });
        let op: Op = serde_json::from_value(serde_json::json!({
            "op": "set",
            "field": "profile_membership:2",
            "values": {"1": {"flag": false}}
        }))
        .expect("parses");
        assert_eq!(op.field(), Field::Membership(2));
    }

    #[test]
    fn an_unknown_field_label_is_refused() {
        let error = serde_json::from_value::<Op>(serde_json::json!({
            "op": "set", "field": "logo_id", "values": {}
        }))
        .expect_err("rejects");
        assert!(
            error.to_string().contains("unknown channel field"),
            "{error}"
        );
        // A channel id that is not a number is refused rather than silently dropped.
        let error = serde_json::from_value::<Op>(serde_json::json!({
            "op": "set", "field": "name", "values": {"espn": {"name": "x"}}
        }))
        .expect_err("rejects");
        assert!(error.to_string().contains("is not an integer"), "{error}");
    }

    #[test]
    fn moving_a_channel_to_no_group_targets_a_null_group() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: Some(vec![1, 5]),
                ..Selection::default()
            },
            &[Op::MoveGroup { group_id: None }],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].channel_id, 1);
        assert_eq!(plan.changes[0].after, Value::Group(None));
        assert_eq!(plan.changes[0].after.wire(), "none");
        // Local has no group already.
        assert!(plan
            .skipped
            .iter()
            .any(|skip| skip.channel_id == 5 && skip.reason.contains("already has no group")));
    }

    #[test]
    fn a_stream_list_is_compared_in_order_not_as_a_set() {
        let mut fixture = world();
        fixture.channels[0].streams = vec![100, 101];
        let reordered = set_op(Field::Streams, &[(1, SetValue::Streams(vec![101, 100]))]);
        let plan = plan_ops(&fixture, &Selection::default(), &[reordered]).expect("plans");
        assert_eq!(plan.changes.len(), 1);
        let change = &plan.changes[0];
        assert_eq!(change.field, Field::Streams);
        // The report prints the NAMES the world knows, not the ids.
        assert_eq!(
            change.before.display(),
            "ESPN HD (primary), ESPN HD (backup)"
        );
        assert_eq!(
            change.after.display(),
            "ESPN HD (backup), ESPN HD (primary)"
        );
        // The machine value is the ids, which is what a restore parses back.
        assert_eq!(change.before.wire(), "100,101");
        assert_eq!(change.after.wire(), "101,100");
        assert_eq!(parse_stream_ids(&change.after.wire()), Some(vec![101, 100]));

        // The same list in the same order is not a change.
        let plan = plan_ops(
            &fixture,
            &Selection::default(),
            &[set_op(
                Field::Streams,
                &[(1, SetValue::Streams(vec![100, 101]))],
            )],
        )
        .expect("plans");
        assert!(plan.changes.is_empty());
        assert!(plan.skipped[0].reason.contains("already this one"));
    }

    #[test]
    fn a_stream_list_prints_ids_when_the_world_loaded_no_streams() {
        // `load_world_with` leaves an unrequested slice empty, so this is what a run without
        // `WorldKind::Streams` actually hands the plan: the same changes, printed by id.
        let mut fixture = world();
        fixture.streams.clear();
        fixture.loaded = crate::dispatcharr::WorldKinds::only(WorldKind::Channels);
        let plan = plan_ops(
            &fixture,
            &Selection::default(),
            &[set_op(
                Field::Streams,
                &[(1, SetValue::Streams(vec![100, 101]))],
            )],
        )
        .expect("plans");
        assert_eq!(plan.changes[0].after.display(), "100, 101");
        // An empty list is a real value ("the channel has no streams"), not a missing one.
        assert_eq!(Value::Streams(Vec::new()).display(), "no streams");
        assert_eq!(Value::Streams(Vec::new()).wire(), "none");
        assert_eq!(parse_stream_ids("none"), Some(Vec::new()));
        assert_eq!(parse_stream_ids(""), Some(Vec::new()));
        assert_eq!(parse_stream_ids("1, 2"), Some(vec![1, 2]));
        assert_eq!(parse_stream_ids("1,x"), None);
    }

    #[test]
    fn a_long_stream_list_is_counted_rather_than_printed_whole() {
        let ids: Vec<i64> = (1..=9).collect();
        let value = Value::Streams(
            ids.iter()
                .map(|id| ListedStream {
                    id: *id,
                    name: None,
                })
                .collect(),
        );
        assert_eq!(value.display(), "1, 2, 3, 4, 5, 6, … (9 total)");
        assert_eq!(value.wire(), "1,2,3,4,5,6,7,8,9");
    }

    #[test]
    fn rows_are_selected_by_field_and_subject() {
        let rows = vec![
            RunRow {
                ordinal: 0,
                subject: Subject::Channel,
                channel_id: 1,
                field: "epg_data_id".to_string(),
                before_value: Some("31".to_string()),
                ..RunRow::default()
            },
            RunRow {
                ordinal: 1,
                subject: Subject::Channel,
                channel_id: 2,
                field: "epg_data_id".to_string(),
                ..RunRow::default()
            },
            // A watchdog row: an EPG SOURCE id in `channel_id`, and the same field label.
            RunRow {
                ordinal: 2,
                subject: Subject::EpgSource,
                channel_id: 1,
                field: "epg_source".to_string(),
                ..RunRow::default()
            },
            RunRow {
                ordinal: 3,
                subject: Subject::Channel,
                channel_id: 3,
                field: "name".to_string(),
                ..RunRow::default()
            },
        ];
        let guides: Vec<i64> = rows_for_field(&rows, Field::Epg)
            .iter()
            .map(|row| row.channel_id)
            .collect();
        assert_eq!(guides, vec![1, 2]);
        assert_eq!(rows_for_field(&rows, Field::Name).len(), 1);
        assert!(rows_for_field(&rows, Field::Streams).is_empty());
    }

    #[test]
    fn a_scan_selection_ignores_rows_about_other_subjects() {
        // A probe or watchdog row's `channel_id` is an id from another table; reading it as a
        // channel would aim the selection at whatever channel carries that number.
        let rows = vec![
            RunRow {
                ordinal: 0,
                subject: Subject::Channel,
                channel_id: 7,
                epg_data_id: Some(31),
                ..RunRow::default()
            },
            RunRow {
                ordinal: 1,
                subject: Subject::EpgSource,
                channel_id: 8,
                ..RunRow::default()
            },
            RunRow {
                ordinal: 2,
                subject: Subject::Stream,
                channel_id: 9,
                ..RunRow::default()
            },
        ];
        let selection = Selection::from_scan_rows(&rows, &scope());
        assert_eq!(selection.channel_ids, Some(vec![7]));
        assert_eq!(selection.expected_epg.len(), 1);
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
                channel_ids: Some(vec![1, 4]),
                ..Selection::default()
            },
            &[
                Op::Rename {
                    kind: RenameKind::Suffix(" [BadEPG]".to_string()),
                },
                Op::MoveGroup { group_id: Some(20) },
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
                channel_ids: Some(vec![1]),
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
    async fn a_cancelled_run_says_exactly_how_much_landed() {
        // The wording four actions share: a preview, a run that wrote nothing, and a run that
        // landed some of its rows. Each names what the operator is looking at, because the
        // run's status says "cancelled" and the rows say which half.
        let server = MockServer::start().await;
        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let plan = plan_ops(
            &world(),
            &Selection::default(),
            &[Op::Set {
                field: Field::HiddenFromOutput,
                values: [
                    (1, SetValue::Flag(true)),
                    (3, SetValue::Flag(true)),
                    (4, SetValue::Flag(true)),
                ]
                .into_iter()
                .collect(),
            }],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 3);

        // Not cancelled: nothing to say.
        let live = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        assert_eq!(cancelled_note(&live, &plan, None), None);

        // A preview that was cancelled wrote nothing by definition.
        let cancelled = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        cancelled.cancel();
        let note = cancelled_note(&cancelled, &plan, None).expect("a cancelled preview says so");
        assert!(note.contains("preview wrote nothing"), "{note}");

        // A run cancelled before its first write.
        let report = apply(&client, &plan, &cancelled, "guide.clear_groups_apply").await;
        assert_eq!(report.ok, 0);
        let note = cancelled_note(&cancelled, &plan, Some(&report)).expect("a note");
        assert!(note.contains("before any change was written"), "{note}");

        // A run cancelled after a batch landed names both halves — which is the case phase 1
        // recorded as a success and so hid.
        let partial = crate::channel_ops::ApplyReport {
            ok: 1,
            failed: 2,
            ..report
        };
        let note = cancelled_note(&cancelled, &plan, Some(&partial)).expect("a note");
        assert!(note.contains("after 1 of 3 change(s) landed"), "{note}");
        assert!(note.contains("the other 2 were not written"), "{note}");
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

    /// A [`StreamListWriter`] that records the batches it was asked to write, for the routing
    /// tests.
    #[derive(Default)]
    struct RecordingWriter {
        batches: std::sync::Mutex<Vec<StreamBatch>>,
        /// Channels whose write fails, to check that a failure is per channel.
        failing: Vec<i64>,
        /// Channels left unanswered, to check that a short answer fails loudly.
        unanswered: Vec<i64>,
    }

    /// One `write_many` call, as the writer received it.
    type StreamBatch = Vec<(i64, Vec<i64>)>;

    impl RecordingWriter {
        fn new() -> Self {
            Self::default()
        }

        fn batches(&self) -> Vec<StreamBatch> {
            self.batches.lock().expect("calls lock").clone()
        }
    }

    #[async_trait::async_trait]
    impl StreamListWriter for RecordingWriter {
        async fn write_many(&self, lists: &[(i64, Vec<i64>)]) -> Vec<(i64, Result<()>)> {
            self.batches
                .lock()
                .expect("calls lock")
                .push(lists.to_vec());
            lists
                .iter()
                .filter(|(id, _)| !self.unanswered.contains(id))
                .map(|(id, _)| {
                    if self.failing.contains(id) {
                        (*id, Err(anyhow::anyhow!("stream {id} was rejected")))
                    } else {
                        (*id, Ok(()))
                    }
                })
                .collect()
        }
    }

    fn stream_world() -> World {
        let mut fixture = world();
        fixture.channels[0].streams = vec![100, 101];
        fixture.channels[2].streams = vec![102];
        fixture
    }

    #[tokio::test]
    async fn stream_lists_go_through_the_writer_in_one_batch() {
        // The writer is batched: one call carrying every channel's list, in plan order.
        let server = MockServer::start().await;
        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let plan = plan_ops(
            &stream_world(),
            &Selection::default(),
            &[set_op(
                Field::Streams,
                &[
                    (1, SetValue::Streams(vec![101, 100])),
                    (3, SetValue::Streams(vec![102, 100])),
                ],
            )],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 2);

        let writer = RecordingWriter::new();
        let report =
            apply_with_writer(&client, &plan, &context, "streams.sort_apply", &writer).await;
        assert!(report.all_ok(), "{:#?}", report.changes);
        assert_eq!(
            writer.batches(),
            vec![vec![(1, vec![101, 100]), (3, vec![102, 100])]]
        );
        // Nothing was sent to the API: the writer owns that decision.
        assert_eq!(
            server.received_requests().await.unwrap_or_default().len(),
            0
        );
    }

    #[tokio::test]
    async fn a_stream_write_that_fails_fails_only_its_own_row() {
        let server = MockServer::start().await;
        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let plan = plan_ops(
            &stream_world(),
            &Selection::default(),
            &[set_op(
                Field::Streams,
                &[
                    (1, SetValue::Streams(vec![101])),
                    (3, SetValue::Streams(vec![100])),
                ],
            )],
        )
        .expect("plans");
        let writer = RecordingWriter {
            failing: vec![1],
            ..RecordingWriter::new()
        };

        let report =
            apply_with_writer(&client, &plan, &context, "streams.sort_apply", &writer).await;
        assert_eq!(report.ok, 1);
        assert_eq!(report.failed, 1);
        let failed = report
            .changes
            .iter()
            .find(|change| change.channel_id == 1)
            .expect("failed row");
        assert_eq!(failed.status, RowStatus::Failed);
        assert!(failed.reason.contains("was rejected"), "{}", failed.reason);
        let written = report
            .changes
            .iter()
            .find(|change| change.channel_id == 3)
            .expect("ok row");
        assert_eq!(written.status, RowStatus::Ok);
    }

    #[tokio::test]
    async fn a_cancelled_stream_stage_writes_nothing_and_says_so() {
        let server = MockServer::start().await;
        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        context.cancel();
        let plan = plan_ops(
            &stream_world(),
            &Selection::default(),
            &[set_op(Field::Streams, &[(1, SetValue::Streams(vec![101]))])],
        )
        .expect("plans");

        let writer = RecordingWriter::new();
        let report =
            apply_with_writer(&client, &plan, &context, "streams.sort_apply", &writer).await;
        assert!(writer.batches().is_empty());
        assert_eq!(report.failed, 1);
        assert_eq!(report.changes[0].reason, CANCELLED);
    }

    #[tokio::test]
    async fn a_channel_the_writer_never_answers_fails_loudly() {
        // A short answer is a bug in the writer, not a success: the row fails with the
        // writer's name on it rather than sitting at `planned` forever.
        let server = MockServer::start().await;
        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let plan = plan_ops(
            &stream_world(),
            &Selection::default(),
            &[set_op(
                Field::Streams,
                &[
                    (1, SetValue::Streams(vec![101])),
                    (3, SetValue::Streams(vec![100])),
                ],
            )],
        )
        .expect("plans");
        let writer = RecordingWriter {
            unanswered: vec![3],
            ..RecordingWriter::new()
        };

        let report =
            apply_with_writer(&client, &plan, &context, "streams.sort_apply", &writer).await;
        assert_eq!(report.ok, 1);
        assert_eq!(report.failed, 1);
        let silent = report
            .changes
            .iter()
            .find(|change| change.channel_id == 3)
            .expect("unanswered row");
        assert_eq!(silent.status, RowStatus::Failed);
        assert!(silent.reason.contains("no result"), "{}", silent.reason);
    }

    #[tokio::test]
    async fn the_default_apply_writes_stream_lists_in_one_bulk_request() {
        // [`apply`] uses the REST writer, which is batched: two channels' lists go out as
        // two `edit/bulk` rows in ONE request, not one PATCH per channel.
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .and(body_json(serde_json::json!([
                {"id": 1, "streams": [101, 100]},
                {"id": 3, "streams": [102, 100]}
            ])))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"message": "ok", "channels": [{"id": 1}, {"id": 3}]}),
            ))
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let plan = plan_ops(
            &stream_world(),
            &Selection::default(),
            &[set_op(
                Field::Streams,
                &[
                    (1, SetValue::Streams(vec![101, 100])),
                    (3, SetValue::Streams(vec![102, 100])),
                ],
            )],
        )
        .expect("plans");

        let report = apply(&client, &plan, &context, "streams.sort_apply").await;
        assert!(report.all_ok(), "{:#?}", report.changes);
        assert_eq!(
            server.received_requests().await.unwrap_or_default().len(),
            1
        );
    }

    #[tokio::test]
    async fn per_channel_values_and_a_group_move_share_one_bulk_edit() {
        // A per-channel name and a group move are `edit/bulk` rows like any other: the
        // per-channel part only decides WHICH channel gets which value.
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .and(body_json(serde_json::json!([
                {"id": 1, "name": "ESPN [recovered]", "channel_group_id": 20},
                {"id": 2, "channel_group_id": 20},
                {"id": 5, "channel_group_id": 20}
            ])))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "message": "ok",
                "channels": [{"id": 1}, {"id": 2}, {"id": 5}]
            })))
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let plan = plan_ops(
            &world(),
            &Selection::default(),
            &[
                set_op(
                    Field::Name,
                    &[(1, SetValue::Name("ESPN [recovered]".to_string()))],
                ),
                Op::MoveGroup { group_id: Some(20) },
            ],
        )
        .expect("plans");

        let writer = RecordingWriter::new();
        let report =
            apply_with_writer(&client, &plan, &context, "guide.restore_apply", &writer).await;
        assert!(report.all_ok(), "{:#?}", report.changes);
        // Channels 3 and 4 are already in group 20 and are skipped; the other three moved, and
        // channel 1 also took the per-channel name.
        assert_eq!(report.ok, 4);
        assert_eq!(report.failed, 0);
        assert!(writer.batches().is_empty());
    }

    #[tokio::test]
    async fn a_move_to_no_group_is_reported_rather_than_sent() {
        // `ChannelSerializer.channel_group_id` has no `allow_null`, so a null is a 400 for the
        // whole `edit/bulk` chunk. One bad row must not take the renames in that chunk down
        // with it, so the move is reported and the rest of the plan still lands.
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .and(body_json(serde_json::json!([
                {"id": 1, "name": "ESPN [recovered]"}
            ])))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"message": "ok", "channels": [{"id": 1}]})),
            )
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: Some(vec![1]),
                ..Selection::default()
            },
            &[
                set_op(
                    Field::Name,
                    &[(1, SetValue::Name("ESPN [recovered]".to_string()))],
                ),
                Op::MoveGroup { group_id: None },
            ],
        )
        .expect("plans");
        // The plan itself is fine: the move is a change, recorded with its "none" target.
        assert_eq!(plan.changes.len(), 2);
        assert!(plan
            .changes
            .iter()
            .any(|change| change.field == Field::Group && change.after.wire() == "none"));

        let writer = RecordingWriter::new();
        let report =
            apply_with_writer(&client, &plan, &context, "guide.restore_apply", &writer).await;
        assert_eq!(report.ok, 1);
        assert_eq!(report.failed, 1);
        let moved = report
            .changes
            .iter()
            .find(|change| change.field == Field::Group)
            .expect("group row");
        assert_eq!(moved.status, RowStatus::Failed);
        assert!(
            moved.reason.contains("cannot clear a channel group"),
            "{}",
            moved.reason
        );
        // The rename in the same plan was written.
        assert_eq!(
            report
                .changes
                .iter()
                .find(|change| change.field == Field::Name)
                .expect("name row")
                .status,
            RowStatus::Ok
        );
    }

    // ---- rows --------------------------------------------------------------

    #[test]
    fn run_rows_carry_what_a_restore_needs() {
        let plan = plan_ops(
            &world(),
            &Selection {
                channel_ids: Some(vec![1, 3]),
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
                channel_ids: Some(vec![1]),
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
    fn every_channel_row_says_it_is_about_a_channel() {
        // A restore reads a run's rows by subject as well as by field, so a channel operation
        // must say which it is: the watchdog's rows share the row shape but not the subject.
        let plan = plan_ops(&world(), &Selection::default(), &[Op::ClearEpg]).expect("plans");
        let rows = run_rows(&plan, None);
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|row| row.subject == Subject::Channel));
        // The skip rows carry the same field (a channel with no guide to clear), so the
        // planned and written rows are the ones with a value on both sides.
        let written = rows_for_field(&rows, Field::Epg)
            .iter()
            .filter(|row| row.status != "skipped")
            .count();
        assert_eq!(written, plan.changes.len());
    }

    #[test]
    fn a_stream_row_carries_ids_a_restore_can_put_back() {
        let plan = plan_ops(
            &stream_world(),
            &Selection::default(),
            &[set_op(
                Field::Streams,
                &[(1, SetValue::Streams(vec![101, 100]))],
            )],
        )
        .expect("plans");
        let rows = run_rows(&plan, None);
        let row = rows_for_field(&rows, Field::Streams)[0];
        assert_eq!(row.field, "streams");
        assert_eq!(row.method, "set");
        assert_eq!(row.before_value.as_deref(), Some("100,101"));
        assert_eq!(row.after_value.as_deref(), Some("101,100"));
        // What a restore does with the before value.
        assert_eq!(
            parse_stream_ids(row.before_value.as_deref().expect("value")),
            Some(vec![100, 101])
        );
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
                channel_ids: Some(vec![1, 2]),
                ..Selection::default()
            },
            &[Op::Rename {
                kind: RenameKind::Suffix(" [BadEPG]".to_string()),
            }],
        )
        .expect("plans");
        let app = StubApp::new();
        let spec = ReportSpec::new(
            &app,
            &schema,
            &values,
            "tag_missing_preview",
            "Tag Missing EPG Preview Report",
        )
        .note("Scan used", "an older scan");
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
