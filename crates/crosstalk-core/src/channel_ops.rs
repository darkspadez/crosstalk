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
    Channel, ChannelCreate, ChannelEdit, ChannelGroup, DispatcharrClient, EpgEntry, Logo, Set,
    Stream, World, WorldKind,
};
use crate::jobs::JobContext;
use crate::scope::{scope_channels, ScopedChannels};
use crate::store::{CreateIntent, RunRow, Subject};
use crate::subapp::{SettingsSchema, SettingsValues, SubApp};
use crate::wildcard::parse_tokens;
use anyhow::{bail, Context, Result};
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
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
#[derive(Debug, Clone, PartialEq, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Deserialize)]
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
    /// A channel number (Dispatcharr's float; `None` clears it).
    Number(Option<f64>),
    /// A logo; `None` clears it. A logo created by the same plan is referenced through the
    /// create stage's [`LogoRef`], not here: a `Set` value must name an object that already
    /// exists, while a planned logo has no id until the create stage runs.
    Logo(Option<i64>),
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
            Self::Number(_) => "number",
            Self::Logo(_) => "logo",
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
                | (Field::Number, Self::Number(_))
                | (Field::Logo, Self::Logo(_))
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
    ///
    /// Step 8 (migration list): the suffix check is the shared vocabulary rule
    /// ([`has_trailing`](crosstalk_matching::tags::has_trailing): case-insensitive and
    /// bracket-aware), not the plugin's exact `ends_with`. `CNN [badepg]` and
    /// `CNN (BadEPG)` already count as tagged, which changes Janitor parity for those
    /// case/bracket variants (oracle diff, not a gate). The append itself stays verbatim.
    fn already(&self, current: &str) -> bool {
        match self {
            Self::Suffix(text) => crosstalk_matching::tags::has_trailing(current, text),
            Self::Prefix(text) => current.starts_with(text.as_str()),
            Self::Template(_) => false,
        }
    }

    /// The new name, or `None` when it would be empty (a blank name is not a rename).
    ///
    /// Templates render through the shared engine ([`crate::naming::render_channel`]);
    /// the suffix/prefix append stays verbatim. The presence check above is the shared
    /// vocabulary rule, so a rename never double-appends.
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
/// field the same way a restore reads it. `Number` is Dispatcharr's float channel number
/// (`channel_number`); `Logo` is the logo id (`logo_id`), `None` clearing each.
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
    /// The channel number.
    Number,
    /// The channel's logo.
    Logo,
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
            Self::Number => "channel_number".to_string(),
            Self::Logo => "logo_id".to_string(),
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
            "channel_number" => Some(Self::Number),
            "logo_id" => Some(Self::Logo),
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
                 hidden_from_output, streams, channel_number, logo_id or profile_membership:<id>"
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
#[derive(Debug, Clone, PartialEq)]
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
    /// A channel number; `None` clears it.
    Number(Option<f64>),
    /// A logo; `None` clears it.
    Logo(Option<(i64, String)>),
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
            Self::Number(Some(number)) => format!("{number}"),
            Self::Number(None) => "No number".to_string(),
            Self::Logo(Some((_, name))) => name.clone(),
            Self::Logo(None) => "No logo".to_string(),
        }
    }

    /// The machine value a restore reads back: an id, `none`, `true`/`false`, the stream
    /// ids of a list (comma-separated, `none` for an empty one), or the number itself.
    pub fn wire(&self) -> String {
        match self {
            Self::Name(name) => name.clone(),
            Self::Group(Some((id, _))) | Self::Epg(Some((id, _))) | Self::Logo(Some((id, _))) => {
                id.to_string()
            }
            Self::Group(None) | Self::Epg(None) | Self::Logo(None) => "none".to_string(),
            Self::Flag(flag) => flag.to_string(),
            Self::Number(Some(number)) => format!("{number}"),
            Self::Number(None) => "none".to_string(),
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
    /// `suffix`/`numeric` rewrite the loser's planned name. Planned creates' names go
    /// through the same pass (see [`plan_creates`]).
    pub collision: Option<crate::naming::CollisionStrategy>,
    /// The separator `suffix` collisions join with (`" "`, Name Cleaner's
    /// `join_separator`). Ignored unless [`Self::collision`] is set.
    pub collision_sep: String,
    /// Per-channel removed quality tags for the `suffix` collision strategy: channel id
    /// to the quality the cleaning stripped from that channel, uppercased (`["HD"]`).
    ///
    /// Name Cleaner's `suffix` strategy re-appends the FIRST removed segment
    /// (`base + sep + rq[0]`, plugin.py:250-254) before falling back to a number; the
    /// shared resolver ([`crate::naming::PlannedName::removed_tags`]) already takes one
    /// list per planned name, and this is how a channel-ops plan supplies it — one map,
    /// still one resolver. Channels absent here resolve with no removed tag (pure
    /// numeric). Ignored unless [`Self::collision`] is set. Lineup's clean fills this
    /// from the pipe segments its pipeline dropped.
    pub collision_removed: HashMap<i64, Vec<String>>,
    /// Refuse a number that is already used by any channel.
    ///
    /// Opt-in per plan, off by default. When on, a planned number that is already used by
    /// another channel — in ANY group, because Dispatcharr's numbers are one global
    /// namespace in practice (see [`drop_number_conflicts`]) — or claimed by an earlier
    /// planned number in the same plan is dropped and reported as a skip, instead of landing
    /// a duplicate through a REST or bulk write (neither runs `Channel.clean`). This covers
    /// both a [`Field::Number`] change here and a [`NewChannel`] number in a create plan. A
    /// channel cleared to no number never conflicts; a channel being renumbered away vacates
    /// its old number for the rest of the plan (unless its own renumber is refused).
    pub number_conflicts: bool,
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
    // The stream and logo indexes are only worth building when an operation reads them.
    let needs_streams = ops.iter().any(|op| op.field() == Field::Streams);
    let needs_logos = ops.iter().any(|op| op.field() == Field::Logo);
    let lookups = Lookups::of(world, needs_streams, needs_logos);
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
            &options.collision_removed,
        );
    }

    if options.number_conflicts {
        drop_number_conflicts(world, &mut changes, &mut skipped);
    }

    Ok(Plan {
        description: selection.describe(&scoped.filter_description),
        selected: selected_rows,
        changes,
        skipped,
    })
}

/// Drop planned numbers that are already used, when [`PlanOptions::number_conflicts`] is on.
///
/// Numbers are checked GLOBALLY, not per group. Dispatcharr's only per-group check is
/// `Channel.clean`, which REST and bulk writes never call; everything that hands out or
/// reads a number treats it as one global namespace: `get_next_available_channel_number`
/// reserves every channel's number (`compact_numbering.build_reserved_set`), the
/// from-stream create refuses any reserved number (`channel_number_is_reserved`), the
/// provider auto-sync seeds its `used_numbers` from every channel, and the outputs list
/// channels flat by number (HDHomeRun `GuideNumber`, M3U `tvg-chno`, the default `tvg_id`).
/// Two channels sharing a number in different groups are one channel to a player.
///
/// A [`Field::Number`] change whose number is held by another channel loses its change and
/// gains a skip row naming the holder; an earlier planned number in the same plan holds the
/// same way. A channel whose own number is being cleared or moved vacates it for the rest of
/// the plan — unless its own change is refused, in which case it keeps the number, so the
/// pass repeats until no refusal frees or re-takes a number (each round re-takes at least
/// one, so it ends).
fn drop_number_conflicts(world: &World, changes: &mut Vec<Change>, skipped: &mut Vec<Skip>) {
    let name_of = |id: i64| -> String {
        world
            .channels
            .iter()
            .find(|channel| channel.id == id)
            .map(|channel| format!("channel {} ({})", channel.id, channel.name))
            .unwrap_or_else(|| format!("channel {id}"))
    };
    let planned_number = |change: &Change| match (&change.field, &change.after) {
        (Field::Number, Value::Number(number)) => Some(*number),
        _ => None,
    };
    // The channels whose number this plan moves: their old number is vacated, while their
    // change stands.
    let mut vacating: HashSet<i64> = changes
        .iter()
        .filter(|change| planned_number(change).is_some())
        .map(|change| change.channel_id)
        .collect();
    loop {
        let mut held: HashMap<u64, i64> = HashMap::new();
        for channel in &world.channels {
            if vacating.contains(&channel.id) {
                continue;
            }
            if let Some(number) = channel_number_of(channel) {
                held.entry(number_key(number)).or_insert(channel.id);
            }
        }
        let mut refused: Vec<(usize, i64)> = Vec::new();
        for (index, change) in changes.iter().enumerate() {
            let Some(Some(number)) = planned_number(change) else {
                // Not a number change, or a clear (which never conflicts).
                continue;
            };
            match held.get(&number_key(number)) {
                Some(holder) if *holder != change.channel_id => refused.push((index, *holder)),
                _ => {
                    held.insert(number_key(number), change.channel_id);
                }
            }
        }
        // A refused channel keeps its old number: if it was counted as vacated, take it back
        // and plan again, because a later claim may have landed on it.
        let retaken: Vec<i64> = refused
            .iter()
            .map(|(index, _)| changes[*index].channel_id)
            .filter(|id| vacating.remove(id))
            .collect();
        if !retaken.is_empty() {
            continue;
        }
        let refused: HashMap<usize, i64> = refused.into_iter().collect();
        let mut kept: Vec<Change> = Vec::with_capacity(changes.len());
        for (index, change) in changes.drain(..).enumerate() {
            let Some(holder) = refused.get(&index) else {
                kept.push(change);
                continue;
            };
            let number = planned_number(&change).flatten().unwrap_or_default();
            skipped.push(Skip {
                channel_id: change.channel_id,
                channel_name: change.channel_name.clone(),
                channel_number: change.channel_number,
                channel_group: change.channel_group.clone(),
                op: Some(change.op),
                field: Some(change.field),
                reason: format!(
                    "channel number {number} is already used by {} (numbers are global in \
                     Dispatcharr's outputs), so this number is skipped",
                    name_of(*holder),
                ),
            });
        }
        *changes = kept;
        return;
    }
}

/// The identity of a channel number for conflict checks: its bits, with `-0.0` folded onto
/// `0.0` (they print and compare equal upstream).
fn number_key(number: f64) -> u64 {
    if number == 0.0 {
        0.0f64.to_bits()
    } else {
        number.to_bits()
    }
}

/// The next `n` free channel numbers, counting up from `start`.
///
/// Mirrors Dispatcharr's `get_next_available_channel_number` (`apps/channels/models.py`,
/// over `_next_available_number` in `apps/m3u/tasks.py`): the smallest numbers `>= start`,
/// one step at a time, that no channel holds in ANY group — upstream reserves numbers
/// globally (see [`drop_number_conflicts`]), so a per-group pool would hand out numbers
/// the server and the players already consider taken. `start` is a float because numbers
/// are; a fractional start counts `1.5, 2.5, …`, like the endpoint's walk.
///
/// Only the raw `Channel.channel_number` values in the loaded world count. Dispatcharr also
/// lets a `ChannelOverride` pin a different effective number per channel, but overrides are
/// not exposed over REST — a caller that knows pins (a bridge-backed world) must merge them
/// into the world's numbers first, or a handed-out number can still collide at apply time
/// (which [`PlanOptions::number_conflicts`] then reports as a skip).
pub fn next_free_numbers(world: &World, start: f64, n: usize) -> Vec<f64> {
    let used: HashSet<u64> = world
        .channels
        .iter()
        .filter_map(channel_number_of)
        .map(number_key)
        .collect();
    let mut free = Vec::with_capacity(n);
    let mut candidate = start;
    while free.len() < n {
        if !used.contains(&number_key(candidate)) {
            free.push(candidate);
        }
        candidate += 1.0;
    }
    free
}

/// Apply the opt-in collision strategy to a plan's rename changes.
///
/// Only [`Field::Name`] changes participate, against EVERY channel name in the world
/// (global, like Name Cleaner: see [`crate::naming`] for why not per group), minus the old
/// names the renames free — a rename frees its own slot for a sibling, exactly like the
/// engine's first pass. A resolved name rewrites the change's `after`; a `Skip` loser, and a
/// change the resolver turns back into its own current name, loses its change and gains a
/// `skipped` row. The per-channel removed quality comes from
/// [`PlanOptions::collision_removed`] (lineup's clean fills it from the pipe segments its
/// pipeline dropped); channels absent there resolve with no removed tag (pure numeric).
fn resolve_plan_collisions(
    world: &World,
    changes: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
    strategy: crate::naming::CollisionStrategy,
    sep: &str,
    removed: &HashMap<i64, Vec<String>>,
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
            removed_tags: removed.get(&change.channel_id).cloned().unwrap_or_default(),
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
            Field::Name | Field::Group | Field::HiddenFromOutput | Field::Number | Field::Logo
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
            // Both are nullable upstream (`allow_null`), so a cleared value sends a null.
            (Field::Number, Value::Number(number)) => {
                edit.channel_number = number.map_or(Set::Null, Set::Value);
            }
            (Field::Logo, Value::Logo(logo)) => {
                edit.logo_id = logo.as_ref().map_or(Set::Null, |(id, _)| Set::Value(*id));
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
            owner_key: None,
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

// ---- Create stage ------------------------------------------------------------
//
// Edits change objects that already exist; creates make new ones. The two stay separate on
// purpose: a create needs an id that does not exist until the POST answers, so it cannot
// be one more [`Op`] over selected channels. Instead a [`CreateRequest`] names the groups,
// logos and channels to make, [`plan_creates`] dedups it against the world (and against the
// app's own earlier creates, through the ownership table), and [`apply_creates`] posts it
// in dependency order — groups, then logos, then channels — through a [`ChannelCreator`].
//
// Scale shaped the split: thousands of channel POSTs go out concurrently (bounded by the
// creator's concurrency), while planning stays pure and synchronous over the loaded world.
// A preview therefore lists every object without touching the network, and an apply records
// one run row per object with its new id ([`create_run_rows`]).
//
// Deduplication is get_or_create, per object kind:
//
// * a group whose exact name is already in the world reuses that id. Upstream names are
//   unique and compared exactly (case-sensitive `TextField(unique=True)`), and the REST
//   create answers 400 for an existing name (DRF's `UniqueValidator` runs before the
//   serializer's get_or_create), so the client recovers a failed group create by looking
//   the exact name up;
// * a logo whose exact URL is already in the world reuses that id (`Logo.objects.get_or_create(url=…)`
//   in `from-stream`);
// * a channel is never created twice for one key: the key is app-defined
//   (`lineup:<file>:<category>:<name>` for lineup) and the plan consults the objects the
//   app already owns. A same-named channel the app does NOT own is left alone — adoption
//   is an explicit [`Store::record_ownership`](crate::store::Store::record_ownership), not
//   a side effect of planning.
//
// Crash safety: [`apply_creates`] records an intent (what it is about to post) before each
// stage's POSTs and turns it into ownership when the answer lands. A process that dies in
// between leaves the intent, and [`load_owned`] (what a caller runs before planning)
// adopts the object that landed — matched by exactly what was posted, newer than anything
// the plan's world held — or drops the intent when nothing did. It also forgets owned ids
// that are gone from Dispatcharr (a user deleted them), so their keys are created again.
// Hiding a channel (undo) keeps its ownership: a hidden channel is still in the world.
//
// [`PlanOptions::collision`] also guards planned create names (global scope, same engine
// and strategies as renames; a create carries no removed quality tags, so `suffix` falls
// back to numeric), and [`PlanOptions::number_conflicts`] also refuses a planned number
// that is already used by any channel (numbers are global, see [`drop_number_conflicts`]).
/// A group to create, by exact name.
#[derive(Debug, Clone, PartialEq)]
pub struct NewGroup {
    /// The group's name. Compared exactly (case-sensitive) against the world.
    pub name: String,
    /// The app-defined ownership key, recorded on the create row and in the ownership table.
    pub key: String,
}

/// A logo to create, by URL (Dispatcharr's identity for a logo).
#[derive(Debug, Clone, PartialEq)]
pub struct NewLogo {
    pub name: String,
    /// Compared exactly against the world's logo URLs.
    pub url: String,
    /// The app-defined ownership key, recorded on the create row and in the ownership table.
    pub key: String,
}

/// Which group a new channel belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupRef {
    /// An existing group by id.
    Existing(i64),
    /// A group from this request, by position in [`CreateRequest::groups`].
    Planned(usize),
}

/// Which logo a new channel carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogoRef {
    /// An existing logo by id.
    Existing(i64),
    /// A logo from this request, by position in [`CreateRequest::logos`].
    Planned(usize),
}

/// A channel to create.
#[derive(Debug, Clone, PartialEq)]
pub struct NewChannel {
    pub name: String,
    /// Dispatcharr assigns the next free number when this is `None`. An auto-numbering
    /// caller hands out [`next_free_numbers`] and sets one per channel instead.
    pub number: Option<f64>,
    /// `None` leaves the group to the server (which creates the "Default Group").
    pub group: Option<GroupRef>,
    pub logo: Option<LogoRef>,
    /// Ordered stream ids, playback order first.
    pub streams: Vec<i64>,
    /// Dispatcharr's three-way meaning, spelled out because getting it wrong is silent:
    /// omitted adds the channel to every profile, `[]` to none, `[0]` is the explicit
    /// "all profiles" sentinel.
    pub profile_ids: Option<Vec<i64>>,
    pub tvg_id: Option<String>,
    pub epg_data_id: Option<i64>,
    /// The app-defined ownership key: the dedup identity of this channel. The plan refuses
    /// to create a second channel for a key the app already owns.
    pub key: String,
}

/// Everything one run wants to create, for one app.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CreateRequest {
    /// Whose objects these are: the ownership rows' app.
    pub app: String,
    pub groups: Vec<NewGroup>,
    pub logos: Vec<NewLogo>,
    pub channels: Vec<NewChannel>,
}

/// What a create row is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreatedKind {
    Group,
    Logo,
    Channel,
}

impl CreatedKind {
    /// The ownership-table and subject form: `group`, `logo`, `channel`.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Group => "group",
            Self::Logo => "logo",
            Self::Channel => "channel",
        }
    }

    /// The run-row subject carrying this kind's id.
    pub fn subject(&self) -> Subject {
        match self {
            Self::Group => Subject::Group,
            Self::Logo => Subject::Logo,
            Self::Channel => Subject::Channel,
        }
    }
}

/// A group the plan will create (or reuse).
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedGroup {
    pub name: String,
    pub key: String,
    /// Set when the world already has this exact name: no POST, the id is reused.
    pub existing_id: Option<i64>,
}

/// A logo the plan will create (or reuse).
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedLogo {
    pub name: String,
    pub url: String,
    pub key: String,
    /// Set when the world already has this exact URL: no POST, the id is reused.
    pub existing_id: Option<i64>,
}

/// A planned channel's resolved group: an existing id, or a group this plan creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannedGroupRef {
    Existing(i64),
    /// Position in [`CreatePlan::groups`].
    New(usize),
}

/// A planned channel's resolved logo: an existing id, or a logo this plan creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannedLogoRef {
    Existing(i64),
    /// Position in [`CreatePlan::logos`].
    New(usize),
}

/// A channel the plan will create, with its refs resolved to existing ids or to objects
/// this same plan creates.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedChannel {
    pub name: String,
    pub number: Option<f64>,
    pub group: Option<PlannedGroupRef>,
    pub logo: Option<PlannedLogoRef>,
    pub streams: Vec<i64>,
    pub profile_ids: Option<Vec<i64>>,
    pub tvg_id: Option<String>,
    pub epg_data_id: Option<i64>,
    pub key: String,
    /// The group's name, for the report and the run row (the id may not exist yet).
    pub group_name: String,
}

/// One requested object the plan refused, with the reason.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSkip {
    pub kind: CreatedKind,
    /// The name it would have been created with.
    pub name: String,
    pub key: String,
    pub reason: String,
}

/// What [`plan_creates`] produced: the objects to create, in dependency order, and the
/// ones it refused.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CreatePlan {
    /// Whose objects these are: the ownership rows' app.
    pub app: String,
    /// Groups to create (or reuse), in request order.
    pub groups: Vec<PlannedGroup>,
    /// Logos to create (or reuse), in request order.
    pub logos: Vec<PlannedLogo>,
    /// Channels to create, in request order, refs resolved.
    pub channels: Vec<PlannedChannel>,
    /// Requested objects the plan refused, with the reason each was left alone.
    pub skipped: Vec<CreateSkip>,
    /// The plan in words, for the report preamble and the log.
    pub description: String,
    /// The highest id of each kind in the world the plan was made from: an object that
    /// lands from this plan's POSTs has a higher id, which is what lets a later reconcile
    /// tell it from a user's own same-named object (see [`load_owned`]).
    pub id_floor: IdFloor,
}

/// The highest group, logo and channel id a world held (`0` for none).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IdFloor {
    pub group: i64,
    pub logo: i64,
    pub channel: i64,
}

impl IdFloor {
    fn of(world: &World) -> Self {
        Self {
            group: world.groups.iter().map(|group| group.id).max().unwrap_or(0),
            logo: world.logos.iter().map(|logo| logo.id).max().unwrap_or(0),
            channel: world
                .channels
                .iter()
                .map(|channel| channel.id)
                .max()
                .unwrap_or(0),
        }
    }

    fn get(&self, kind: CreatedKind) -> i64 {
        match kind {
            CreatedKind::Group => self.group,
            CreatedKind::Logo => self.logo,
            CreatedKind::Channel => self.channel,
        }
    }
}

impl CreatePlan {
    /// Whether the plan creates nothing at all (it may still carry skips worth reporting).
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty() && self.logos.is_empty() && self.channels.is_empty()
    }

    /// How many POSTs an apply would send: groups and logos without an `existing_id`, plus
    /// every channel.
    pub fn planned_creates(&self) -> usize {
        self.groups
            .iter()
            .filter(|group| group.existing_id.is_none())
            .count()
            + self
                .logos
                .iter()
                .filter(|logo| logo.existing_id.is_none())
                .count()
            + self.channels.len()
    }
}

/// Plan groups, logos and channels to create, deduped against the world and the app's own
/// earlier creates.
///
/// Pure: nothing is written. `owned` maps the app's owner keys to the channel ids they
/// already made ([`load_owned`]) — a key whose id is still in the world means an earlier
/// run already created this channel, so the request is reported as a skip instead of a
/// duplicate; a key whose id is gone (deleted upstream) is created again.
///
/// The world must carry every slice the dedup reads (groups for groups, logos for logos,
/// channels for channels): planning against a slice that was not loaded would read "none"
/// and post duplicates. Groups dedup by exact name and logos by exact URL against the
/// world; a second request entry for the same name or URL is skipped as already planned.
///
/// A [`GroupRef::Planned`]/[`LogoRef::Planned`] that names no entry is a caller bug and
/// fails the whole plan; a channel whose ref points at a skipped group or logo is skipped
/// with the reason (its dependency is not being created), which is what lets a partial
/// failure refuse exactly the channels that lost their group.
///
/// [`PlanOptions::number_conflicts`] refuses a planned number already used in its group
/// (or claimed by an earlier planned create), and [`PlanOptions::collision`] resolves
/// planned channel names through the shared engine (global scope, same as renames).
pub fn plan_creates(
    world: &World,
    owned: &HashMap<String, i64>,
    request: &CreateRequest,
    options: &PlanOptions,
) -> Result<CreatePlan> {
    if !request.groups.is_empty() {
        world.require(WorldKind::Groups)?;
    }
    if !request.logos.is_empty() {
        world.require(WorldKind::Logos)?;
    }
    if !request.channels.is_empty() {
        world.require(WorldKind::Channels)?;
    }
    for channel in &request.channels {
        match channel.group {
            Some(GroupRef::Planned(index)) if index >= request.groups.len() => bail!(
                "channel {:?} (key {:?}) refers to planned group {index}, but the request \
                 only names {} group(s)",
                channel.name,
                channel.key,
                request.groups.len(),
            ),
            _ => {}
        }
        match channel.logo {
            Some(LogoRef::Planned(index)) if index >= request.logos.len() => bail!(
                "channel {:?} (key {:?}) refers to planned logo {index}, but the request \
                 only names {} logo(s)",
                channel.name,
                channel.key,
                request.logos.len(),
            ),
            _ => {}
        }
    }

    let mut plan = CreatePlan {
        app: request.app.clone(),
        id_floor: IdFloor::of(world),
        ..CreatePlan::default()
    };
    let live_channels: HashSet<i64> = world.channels.iter().map(|channel| channel.id).collect();

    // Groups: exact names reuse the world's id; a repeated name is already planned.
    let mut group_slot: Vec<GroupSlot> = Vec::with_capacity(request.groups.len());
    let mut seen_groups: HashMap<&str, usize> = HashMap::new();
    for group in &request.groups {
        if group.name.trim().is_empty() {
            plan.skipped.push(CreateSkip {
                kind: CreatedKind::Group,
                name: group.name.clone(),
                key: group.key.clone(),
                reason: "the group name is empty, which would be rejected".to_string(),
            });
            group_slot.push(GroupSlot::Skipped);
            continue;
        }
        if let Some(existing) = world.groups.iter().find(|known| known.name == group.name) {
            group_slot.push(GroupSlot::Existing(existing.id));
            plan.groups.push(PlannedGroup {
                name: group.name.clone(),
                key: group.key.clone(),
                existing_id: Some(existing.id),
            });
            continue;
        }
        if seen_groups.contains_key(group.name.as_str()) {
            plan.skipped.push(CreateSkip {
                kind: CreatedKind::Group,
                name: group.name.clone(),
                key: group.key.clone(),
                reason: "a group with this name is already planned in this request".to_string(),
            });
            group_slot.push(GroupSlot::Skipped);
            continue;
        }
        seen_groups.insert(group.name.as_str(), plan.groups.len());
        group_slot.push(GroupSlot::New(plan.groups.len()));
        plan.groups.push(PlannedGroup {
            name: group.name.clone(),
            key: group.key.clone(),
            existing_id: None,
        });
    }

    // Logos: exact URLs reuse the world's id; a repeated URL is already planned.
    let mut logo_slot: Vec<LogoSlot> = Vec::with_capacity(request.logos.len());
    let mut seen_logos: HashMap<&str, usize> = HashMap::new();
    for logo in &request.logos {
        if logo.url.trim().is_empty() {
            plan.skipped.push(CreateSkip {
                kind: CreatedKind::Logo,
                name: logo.name.clone(),
                key: logo.key.clone(),
                reason: "the logo URL is empty, which would be rejected".to_string(),
            });
            logo_slot.push(LogoSlot::Skipped);
            continue;
        }
        if let Some(existing) = world.logos.iter().find(|known| known.url == logo.url) {
            logo_slot.push(LogoSlot::Existing(existing.id));
            plan.logos.push(PlannedLogo {
                name: logo.name.clone(),
                url: logo.url.clone(),
                key: logo.key.clone(),
                existing_id: Some(existing.id),
            });
            continue;
        }
        if seen_logos.contains_key(logo.url.as_str()) {
            plan.skipped.push(CreateSkip {
                kind: CreatedKind::Logo,
                name: logo.name.clone(),
                key: logo.key.clone(),
                reason: "a logo with this URL is already planned in this request".to_string(),
            });
            logo_slot.push(LogoSlot::Skipped);
            continue;
        }
        seen_logos.insert(logo.url.as_str(), plan.logos.len());
        logo_slot.push(LogoSlot::New(plan.logos.len()));
        plan.logos.push(PlannedLogo {
            name: logo.name.clone(),
            url: logo.url.clone(),
            key: logo.key.clone(),
            existing_id: None,
        });
    }

    // Channels: owned keys and repeated keys are already made; empty names are refused;
    // refs resolve to existing ids or to this plan's own objects.
    let mut seen_keys: HashSet<&str> = HashSet::new();
    for channel in &request.channels {
        let skip = |reason: String| CreateSkip {
            kind: CreatedKind::Channel,
            name: channel.name.clone(),
            key: channel.key.clone(),
            reason,
        };
        if channel.name.trim().is_empty() {
            plan.skipped.push(skip(
                "the channel name is empty, which would be rejected".to_string(),
            ));
            continue;
        }
        if let Some(id) = owned
            .get(&channel.key)
            .filter(|id| live_channels.contains(id))
        {
            plan.skipped.push(skip(format!(
                "key {:?} was already created as channel {id}, so no duplicate is made",
                channel.key,
            )));
            continue;
        }
        if !seen_keys.insert(channel.key.as_str()) {
            plan.skipped.push(skip(format!(
                "key {:?} is already planned in this request, so no duplicate is made",
                channel.key,
            )));
            continue;
        }
        let group = match channel.group {
            None => None,
            Some(GroupRef::Existing(id)) => Some(PlannedGroupRef::Existing(id)),
            Some(GroupRef::Planned(index)) => {
                match resolve_group_slot(&request.groups, &group_slot, index) {
                    Some(slot) => Some(slot),
                    None => {
                        plan.skipped.push(skip(format!(
                            "its group {:?} is not being created, so the channel is refused",
                            request.groups[index].name,
                        )));
                        continue;
                    }
                }
            }
        };
        let logo = match channel.logo {
            None => None,
            Some(LogoRef::Existing(id)) => Some(PlannedLogoRef::Existing(id)),
            Some(LogoRef::Planned(index)) => {
                match resolve_logo_slot(&request.logos, &logo_slot, index) {
                    Some(slot) => Some(slot),
                    None => {
                        plan.skipped.push(skip(format!(
                            "its logo {:?} is not being created, so the channel is refused",
                            request.logos[index].name,
                        )));
                        continue;
                    }
                }
            }
        };
        let group_name = match group {
            None => "No Group".to_string(),
            Some(PlannedGroupRef::Existing(id)) => world.group_name(Some(id)),
            Some(PlannedGroupRef::New(index)) => plan.groups[index].name.clone(),
        };
        plan.channels.push(PlannedChannel {
            name: channel.name.clone(),
            number: channel.number,
            group,
            logo,
            streams: channel.streams.clone(),
            profile_ids: channel.profile_ids.clone(),
            tvg_id: channel.tvg_id.clone(),
            epg_data_id: channel.epg_data_id,
            key: channel.key.clone(),
            group_name,
        });
    }

    if options.number_conflicts {
        drop_create_number_conflicts(world, &mut plan);
    }

    if let Some(strategy) = options.collision {
        resolve_create_collisions(world, &mut plan, strategy, &options.collision_sep);
    }

    plan.description = format!(
        "creates for {}: {} group(s), {} logo(s) and {} channel(s) to create, {} skipped",
        request.app,
        plan.groups
            .iter()
            .filter(|group| group.existing_id.is_none())
            .count(),
        plan.logos
            .iter()
            .filter(|logo| logo.existing_id.is_none())
            .count(),
        plan.channels.len(),
        plan.skipped.len(),
    );
    Ok(plan)
}

/// Where one requested group ended up: an existing id, a slot in the plan, or skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupSlot {
    Existing(i64),
    New(usize),
    Skipped,
}

/// Where one requested logo ended up: an existing id, a slot in the plan, or skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogoSlot {
    Existing(i64),
    New(usize),
    Skipped,
}

/// Follow a planned group ref to what the channel actually gets: an existing id, or the
/// plan slot to create. A ref to a skipped group follows the first request entry with the
/// same name (a repeated name is the only way to be skipped), which is always plannable.
fn resolve_group_slot(
    groups: &[NewGroup],
    slots: &[GroupSlot],
    index: usize,
) -> Option<PlannedGroupRef> {
    match slots[index] {
        GroupSlot::Existing(id) => Some(PlannedGroupRef::Existing(id)),
        GroupSlot::New(slot) => Some(PlannedGroupRef::New(slot)),
        GroupSlot::Skipped => {
            let name = groups[index].name.as_str();
            groups
                .iter()
                .zip(slots.iter())
                .find(|(group, _)| group.name.as_str() == name)
                .and_then(|(_, slot)| match slot {
                    GroupSlot::Existing(id) => Some(PlannedGroupRef::Existing(*id)),
                    GroupSlot::New(slot) => Some(PlannedGroupRef::New(*slot)),
                    GroupSlot::Skipped => None,
                })
        }
    }
}

/// Follow a planned logo ref to what the channel actually gets, like [`resolve_group_slot`]
/// (a repeated URL is the only way to be skipped).
fn resolve_logo_slot(
    logos: &[NewLogo],
    slots: &[LogoSlot],
    index: usize,
) -> Option<PlannedLogoRef> {
    match slots[index] {
        LogoSlot::Existing(id) => Some(PlannedLogoRef::Existing(id)),
        LogoSlot::New(slot) => Some(PlannedLogoRef::New(slot)),
        LogoSlot::Skipped => {
            let url = logos[index].url.as_str();
            logos
                .iter()
                .zip(slots.iter())
                .find(|(logo, _)| logo.url.as_str() == url)
                .and_then(|(_, slot)| match slot {
                    LogoSlot::Existing(id) => Some(PlannedLogoRef::Existing(*id)),
                    LogoSlot::New(slot) => Some(PlannedLogoRef::New(*slot)),
                    LogoSlot::Skipped => None,
                })
        }
    }
}

/// Drop planned creates whose number is already used.
///
/// The [`PlanOptions::number_conflicts`] pass for creates: same global rule as
/// [`drop_number_conflicts`], over the world's numbers plus the plan's own earlier claims
/// (a second planned create of one number loses, whichever group either lands in).
fn drop_create_number_conflicts(world: &World, plan: &mut CreatePlan) {
    let mut held: HashMap<u64, String> = HashMap::new();
    for channel in &world.channels {
        if let Some(number) = channel_number_of(channel) {
            held.entry(number_key(number))
                .or_insert_with(|| format!("channel {} ({})", channel.id, channel.name));
        }
    }
    let mut kept: Vec<PlannedChannel> = Vec::with_capacity(plan.channels.len());
    for channel in plan.channels.drain(..) {
        let Some(number) = channel.number else {
            kept.push(channel);
            continue;
        };
        match held.get(&number_key(number)) {
            Some(holder) => plan.skipped.push(CreateSkip {
                kind: CreatedKind::Channel,
                name: channel.name.clone(),
                key: channel.key.clone(),
                reason: format!(
                    "channel number {number} is already used by {holder} (numbers are \
                     global in Dispatcharr's outputs), so this create is skipped",
                ),
            }),
            None => {
                held.insert(
                    number_key(number),
                    format!("channel {:?} (planned in this run)", channel.name),
                );
                kept.push(channel);
            }
        }
    }
    plan.channels = kept;
}

/// Resolve planned create names through the opt-in collision strategy.
///
/// Same engine and global scope as renames ([`resolve_plan_collisions`]): single-claimant
/// free names keep, the rest resolve per strategy in request order. A create carries no old
/// name and no removed quality tags, so `suffix` falls back to numeric. A `skip` loser, and
/// a name the resolver turns back into another planned name's slot... loses its create and
/// gains a skip row; a resolved name rewrites the planned channel.
fn resolve_create_collisions(
    world: &World,
    plan: &mut CreatePlan,
    strategy: crate::naming::CollisionStrategy,
    sep: &str,
) {
    use crate::naming::PlannedName;
    if plan.channels.is_empty() {
        return;
    }
    let planned: Vec<PlannedName> = plan
        .channels
        .iter()
        .enumerate()
        .map(|(index, channel)| PlannedName {
            id: index as i64,
            old_name: String::new(),
            new_name: channel.name.clone(),
            removed_tags: Vec::new(),
        })
        .collect();
    let existing: Vec<String> = world
        .channels
        .iter()
        .map(|channel| channel.name.clone())
        .collect();
    let resolved = crate::naming::resolve_collisions(&planned, &existing, strategy, sep);
    let resolved: HashMap<i64, crate::naming::ResolvedName> =
        resolved.into_iter().map(|row| (row.id, row)).collect();
    let mut kept: Vec<PlannedChannel> = Vec::with_capacity(plan.channels.len());
    for (index, mut channel) in plan.channels.drain(..).enumerate() {
        let Some(row) = resolved.get(&(index as i64)) else {
            kept.push(channel);
            continue;
        };
        if row.skipped {
            plan.skipped.push(CreateSkip {
                kind: CreatedKind::Channel,
                name: channel.name.clone(),
                key: channel.key.clone(),
                reason: format!(
                    "the name {:?} collides with another channel's name and the plan's \
                     collision strategy is skip, so the channel is not created",
                    row.final_name,
                ),
            });
            continue;
        }
        channel.name = row.final_name.clone();
        kept.push(channel);
    }
    plan.channels = kept;
}

/// How a create plan reaches Dispatcharr: the seam a bridge-backed writer replaces.
///
/// The REST implementation ([`RestChannelCreator`]) posts one object per request, which is
/// the only shape the API offers for creates; a Dispatcharr plugin (the bridge) can replace
/// it behind the same trait. Every method returns the created object, with its new id.
/// `concurrency` bounds how many POSTs fly at once (thousands of channels at sync scale).
#[async_trait::async_trait]
pub trait ChannelCreator: Send + Sync {
    /// Create a group. An implementation answers the existing group when the exact name
    /// already exists (the REST one recovers upstream's 400 by looking the name up).
    async fn create_group(&self, name: &str) -> Result<ChannelGroup>;
    /// Create a logo from a name and a URL (the URL is unique upstream).
    async fn create_logo(&self, name: &str, url: &str) -> Result<Logo>;
    /// Create a channel; Dispatcharr assigns the next free number when the payload names
    /// none, and creates the "Default Group" when it names no group.
    async fn create_channel(&self, channel: &ChannelCreate) -> Result<Channel>;
    /// How many POSTs may fly at once.
    fn concurrency(&self) -> usize;
}

/// The REST implementation: one POST per object, bounded concurrency, in dependency order.
///
/// Group and logo names/URLs are unique upstream: a failed group or logo POST is looked up
/// by its exact name/URL and the landed object answered, so a lost response never fails a
/// create that worked ([`DispatcharrClient::create_group`]). Channel POSTs are NOT retried:
/// a re-sent create makes a second channel with the same name, so a lost response surfaces
/// as a failure rather than repeating ([`DispatcharrClient::create_channel`] makes the same
/// choice), and the intent recorded before the POST lets the next run's [`load_owned`]
/// adopt the channel if it did land.
pub struct RestChannelCreator {
    client: DispatcharrClient,
    concurrency: usize,
}

impl RestChannelCreator {
    /// The creator [`apply_creates`] uses: this client, this concurrency bound.
    pub fn new(client: DispatcharrClient, concurrency: usize) -> Self {
        Self {
            client,
            concurrency,
        }
    }
}

impl std::fmt::Debug for RestChannelCreator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestChannelCreator")
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ChannelCreator for RestChannelCreator {
    async fn create_group(&self, name: &str) -> Result<ChannelGroup> {
        self.client.create_group(name).await
    }

    async fn create_logo(&self, name: &str, url: &str) -> Result<Logo> {
        self.client.create_logo(name, url).await
    }

    async fn create_channel(&self, channel: &ChannelCreate) -> Result<Channel> {
        self.client.create_channel(channel).await
    }

    fn concurrency(&self) -> usize {
        self.concurrency
    }
}

/// One created (or refused) object.
#[derive(Debug, Clone, PartialEq)]
pub struct CreatedOutcome {
    pub kind: CreatedKind,
    pub name: String,
    pub key: String,
    /// The new id. `None` when the object was never created (refused or failed).
    pub id: Option<i64>,
    /// Whether this apply actually created the object (a reused group or logo is `Ok` but
    /// not created).
    pub created: bool,
    /// The channel's group name (planned or resolved); a group's own name; `""` for a logo.
    pub group: String,
    pub status: RowStatus,
    pub reason: String,
}

/// What [`apply_creates`] did, stage by stage, in plan order.
#[derive(Debug, Clone, Default)]
pub struct CreateReport {
    pub groups: Vec<CreatedOutcome>,
    pub logos: Vec<CreatedOutcome>,
    pub channels: Vec<CreatedOutcome>,
    pub ok: usize,
    pub failed: usize,
}

impl CreateReport {
    pub fn all_ok(&self) -> bool {
        self.failed == 0
    }

    /// How many objects this apply actually created (reused groups/logos excluded).
    pub fn created(&self) -> usize {
        self.groups
            .iter()
            .chain(self.logos.iter())
            .chain(self.channels.iter())
            .filter(|outcome| outcome.created && outcome.status == RowStatus::Ok)
            .count()
    }
}

/// Apply a create plan: groups, then logos, then channels, through the creator.
///
/// Each stage posts concurrently (bounded by the creator's concurrency) and cancellation is
/// checked between requests, so a cancelled run stops at the last object it finished rather
/// than mid-request. A group or logo that fails refuses exactly the channels that ref it
/// (their outcome says which dependency failed); every other object is still attempted.
/// Successful creates are recorded in the ownership table (through the context's store and
/// run, when the run has them) under the plan's app and each object's key; a stored row is
/// what lets the next run's plan dedup instead of duplicating.
///
/// `action` is the id the progress events are published under.
pub async fn apply_creates(
    creator: Arc<dyn ChannelCreator>,
    plan: &CreatePlan,
    context: &JobContext,
    action: &str,
) -> CreateReport {
    let mut report = CreateReport::default();
    let total = (plan.groups.len() + plan.logos.len() + plan.channels.len()) as u64;
    let mut progress = context.paced(action, total, PROGRESS_INTERVAL);
    let mut done = 0u64;

    // 1. Groups: reused ids need no POST; the rest go out concurrently.
    let mut group_posts: Vec<(usize, String)> = Vec::new();
    for (index, group) in plan.groups.iter().enumerate() {
        match group.existing_id {
            Some(id) => report.groups.push(CreatedOutcome {
                kind: CreatedKind::Group,
                name: group.name.clone(),
                key: group.key.clone(),
                id: Some(id),
                created: false,
                group: group.name.clone(),
                status: RowStatus::Ok,
                reason: format!("group {:?} already exists as id {id}", group.name),
            }),
            None => {
                report.groups.push(CreatedOutcome {
                    kind: CreatedKind::Group,
                    name: group.name.clone(),
                    key: group.key.clone(),
                    id: None,
                    created: false,
                    group: group.name.clone(),
                    status: RowStatus::Planned,
                    reason: String::new(),
                });
                group_posts.push((index, group.name.clone()));
            }
        }
    }
    if !group_posts.is_empty() {
        let phase = format!("creating {} group(s)", group_posts.len());
        progress.set(done, &phase);
        let intents = group_posts
            .iter()
            .map(|(index, name)| {
                intent(
                    plan,
                    context,
                    CreatedKind::Group,
                    &plan.groups[*index].key,
                    name,
                )
            })
            .collect::<Vec<_>>();
        let results = match record_intents(context, &intents) {
            Ok(()) => {
                run_concurrent(
                    group_posts,
                    creator.concurrency(),
                    context,
                    (&mut progress, done, &phase),
                    |name| {
                        let creator = Arc::clone(&creator);
                        async move { creator.create_group(&name).await }
                    },
                )
                .await
            }
            Err(reason) => refuse_all(&group_posts, &reason),
        };
        for (index, outcome) in report.groups.iter_mut().enumerate() {
            if outcome.status != RowStatus::Planned {
                continue;
            }
            match results.get(&index) {
                Some(Ok(created)) => {
                    outcome.id = Some(created.id);
                    outcome.created = true;
                    outcome.status = RowStatus::Ok;
                    record_ownership(context, plan, outcome, CreatedKind::Group);
                }
                Some(Err(error)) => {
                    outcome.status = RowStatus::Failed;
                    outcome.reason = format!("{error:#}");
                }
                None => {
                    outcome.status = RowStatus::Failed;
                    outcome.reason = unanswered(context);
                }
            }
        }
    }
    done += plan.groups.len() as u64;
    // 2. Logos: same shape, after the groups (a logo never depends on a group, but the
    //    order keeps the report and the progress reading the way the plan does).
    let mut logo_posts: Vec<(usize, (String, String))> = Vec::new();
    for (index, logo) in plan.logos.iter().enumerate() {
        match logo.existing_id {
            Some(id) => report.logos.push(CreatedOutcome {
                kind: CreatedKind::Logo,
                name: logo.name.clone(),
                key: logo.key.clone(),
                id: Some(id),
                created: false,
                group: String::new(),
                status: RowStatus::Ok,
                reason: format!("logo URL {:?} already exists as id {id}", logo.url),
            }),
            None => {
                report.logos.push(CreatedOutcome {
                    kind: CreatedKind::Logo,
                    name: logo.name.clone(),
                    key: logo.key.clone(),
                    id: None,
                    created: false,
                    group: String::new(),
                    status: RowStatus::Planned,
                    reason: String::new(),
                });
                logo_posts.push((index, (logo.name.clone(), logo.url.clone())));
            }
        }
    }
    if !logo_posts.is_empty() {
        let phase = format!("creating {} logo(s)", logo_posts.len());
        progress.set(done, &phase);
        let intents = logo_posts
            .iter()
            .map(|(index, (name, url))| CreateIntent {
                url: Some(url.clone()),
                ..intent(
                    plan,
                    context,
                    CreatedKind::Logo,
                    &plan.logos[*index].key,
                    name,
                )
            })
            .collect::<Vec<_>>();
        let results = match record_intents(context, &intents) {
            Ok(()) => {
                run_concurrent(
                    logo_posts,
                    creator.concurrency(),
                    context,
                    (&mut progress, done, &phase),
                    |(name, url)| {
                        let creator = Arc::clone(&creator);
                        async move { creator.create_logo(&name, &url).await }
                    },
                )
                .await
            }
            Err(reason) => refuse_all(&logo_posts, &reason),
        };
        for (index, outcome) in report.logos.iter_mut().enumerate() {
            if outcome.status != RowStatus::Planned {
                continue;
            }
            match results.get(&index) {
                Some(Ok(created)) => {
                    outcome.id = Some(created.id);
                    outcome.created = true;
                    outcome.status = RowStatus::Ok;
                    record_ownership(context, plan, outcome, CreatedKind::Logo);
                }
                Some(Err(error)) => {
                    outcome.status = RowStatus::Failed;
                    outcome.reason = format!("{error:#}");
                }
                None => {
                    outcome.status = RowStatus::Failed;
                    outcome.reason = unanswered(context);
                }
            }
        }
    }
    done += plan.logos.len() as u64;
    // 3. Channels: refs resolve against the two stages above; a failed dependency refuses
    //    the channel with the reason instead of posting it.
    let mut channel_posts: Vec<(usize, ChannelCreate)> = Vec::new();
    for (index, channel) in plan.channels.iter().enumerate() {
        let mut refused: Option<String> = None;
        let group_id = match channel.group {
            None => None,
            Some(PlannedGroupRef::Existing(id)) => Some(id),
            Some(PlannedGroupRef::New(group)) => match report.groups.get(group) {
                Some(outcome) if outcome.status == RowStatus::Ok => outcome.id,
                Some(outcome) => {
                    refused = Some(format!(
                        "group {:?} could not be created ({}), so the channel is refused",
                        outcome.name,
                        if outcome.reason.is_empty() {
                            "it was not attempted".to_string()
                        } else {
                            outcome.reason.clone()
                        },
                    ));
                    None
                }
                None => {
                    refused = Some(
                        "its group is not in this plan, so the channel is refused".to_string(),
                    );
                    None
                }
            },
        };
        let logo_id = match channel.logo {
            None => None,
            Some(PlannedLogoRef::Existing(id)) => Some(id),
            Some(PlannedLogoRef::New(logo)) => match report.logos.get(logo) {
                Some(outcome) if outcome.status == RowStatus::Ok => outcome.id,
                Some(outcome) => {
                    refused = Some(format!(
                        "logo {:?} could not be created ({}), so the channel is refused",
                        outcome.name,
                        if outcome.reason.is_empty() {
                            "it was not attempted".to_string()
                        } else {
                            outcome.reason.clone()
                        },
                    ));
                    None
                }
                None => {
                    refused =
                        Some("its logo is not in this plan, so the channel is refused".to_string());
                    None
                }
            },
        };
        match refused {
            Some(reason) => report.channels.push(CreatedOutcome {
                kind: CreatedKind::Channel,
                name: channel.name.clone(),
                key: channel.key.clone(),
                id: None,
                created: false,
                group: channel.group_name.clone(),
                status: RowStatus::Failed,
                reason,
            }),
            None => {
                let payload = ChannelCreate {
                    name: channel.name.clone(),
                    channel_number: channel.number,
                    channel_group_id: group_id,
                    tvg_id: channel.tvg_id.clone(),
                    tvc_guide_stationid: None,
                    epg_data_id: channel.epg_data_id,
                    logo_id,
                    streams: if channel.streams.is_empty() {
                        None
                    } else {
                        Some(channel.streams.clone())
                    },
                    channel_profile_ids: channel.profile_ids.clone(),
                };
                report.channels.push(CreatedOutcome {
                    kind: CreatedKind::Channel,
                    name: channel.name.clone(),
                    key: channel.key.clone(),
                    id: None,
                    created: false,
                    group: channel.group_name.clone(),
                    status: RowStatus::Planned,
                    reason: String::new(),
                });
                channel_posts.push((index, payload));
            }
        }
    }
    if !channel_posts.is_empty() {
        let phase = format!("creating {} channel(s)", channel_posts.len());
        progress.set(done, &phase);
        // The channel intents carry the resolved group id and the number as posted: what a
        // reconcile matches a landed channel by.
        let intents = channel_posts
            .iter()
            .map(|(index, payload)| CreateIntent {
                group_id: payload.channel_group_id,
                number: payload.channel_number,
                ..intent(
                    plan,
                    context,
                    CreatedKind::Channel,
                    &plan.channels[*index].key,
                    &payload.name,
                )
            })
            .collect::<Vec<_>>();
        let results = match record_intents(context, &intents) {
            Ok(()) => {
                run_concurrent(
                    channel_posts,
                    creator.concurrency(),
                    context,
                    (&mut progress, done, &phase),
                    |payload| {
                        let creator = Arc::clone(&creator);
                        async move { creator.create_channel(&payload).await }
                    },
                )
                .await
            }
            Err(reason) => refuse_all(&channel_posts, &reason),
        };
        for (index, outcome) in report.channels.iter_mut().enumerate() {
            if outcome.status != RowStatus::Planned {
                continue;
            }
            match results.get(&index) {
                Some(Ok(created)) => {
                    outcome.id = Some(created.id);
                    outcome.created = true;
                    outcome.status = RowStatus::Ok;
                    record_ownership(context, plan, outcome, CreatedKind::Channel);
                }
                Some(Err(error)) => {
                    outcome.status = RowStatus::Failed;
                    outcome.reason = format!("{error:#}");
                }
                None => {
                    outcome.status = RowStatus::Failed;
                    outcome.reason = unanswered(context);
                }
            }
        }
    }

    let ok = report
        .groups
        .iter()
        .chain(report.logos.iter())
        .chain(report.channels.iter())
        .filter(|outcome| outcome.status == RowStatus::Ok)
        .count();
    let failed = report
        .groups
        .iter()
        .chain(report.logos.iter())
        .chain(report.channels.iter())
        .filter(|outcome| outcome.status == RowStatus::Failed)
        .count();
    progress.finish(format!("{ok} created or reused, {failed} failed"));
    CreateReport {
        groups: report.groups,
        logos: report.logos,
        channels: report.channels,
        ok,
        failed,
    }
}

/// The intent for one POST of `kind` under `key`: what a reconcile matches the landed
/// object by. The caller fills the kind-specific columns (a logo's URL, a channel's group
/// and number).
fn intent(
    plan: &CreatePlan,
    context: &JobContext,
    kind: CreatedKind,
    key: &str,
    name: &str,
) -> CreateIntent {
    CreateIntent {
        kind: kind.label().to_string(),
        app: plan.app.clone(),
        owner_key: key.to_string(),
        name: name.to_string(),
        group_id: None,
        number: None,
        url: None,
        after_id: plan.id_floor.get(kind),
        run_id: context.run_id().unwrap_or(0),
    }
}

/// Record a stage's intents before its POSTs go out, when the run has a store.
///
/// A store that cannot record them refuses the whole stage (the `Err` is the reason): a
/// POST whose answer might be lost with no intent behind it is exactly the orphan the
/// intent exists to prevent. A run without a store (unit tests, a caller outside the
/// runner) has no ownership to keep and posts as is.
fn record_intents(
    context: &JobContext,
    intents: &[CreateIntent],
) -> std::result::Result<(), String> {
    let (Some(store), Some(_)) = (context.store(), context.run_id()) else {
        return Ok(());
    };
    store
        .record_intents(intents)
        .map_err(|error| format!("not sent: the create intent could not be recorded ({error:#})"))
}

/// Every post of a stage refused with one reason, in [`run_concurrent`]'s answer shape.
fn refuse_all<T, R>(posts: &[(usize, T)], reason: &str) -> HashMap<usize, Result<R>> {
    posts
        .iter()
        .map(|(index, _)| (*index, Err(anyhow::anyhow!("{reason}"))))
        .collect()
}

/// Why a post has no answer: the run was cancelled before it went out, or its task ended
/// without one (a panic in the creator).
fn unanswered(context: &JobContext) -> String {
    if context.is_cancelled() {
        CANCELLED.to_string()
    } else {
        "failed: the request ended without an answer".to_string()
    }
}

/// Record one successful create in the ownership table, when the run has a store.
///
/// Settles the intent recorded before the POST (ownership row in, intent out, one
/// transaction). A run without a store (unit tests, and any caller outside the runner) skips
/// this: the row is still recorded by [`create_run_rows`], only the cross-run dedup entry is
/// missing. A store write that fails does NOT lose the object — it fails the outcome with
/// the id it got, so the run page says the object exists but is not tracked; the intent
/// stays, and the next [`load_owned`] adopts the object.
fn record_ownership(
    context: &JobContext,
    plan: &CreatePlan,
    outcome: &mut CreatedOutcome,
    kind: CreatedKind,
) {
    let (Some(store), Some(run_id)) = (context.store(), context.run_id()) else {
        return;
    };
    let Some(id) = outcome.id else {
        return;
    };
    if let Err(error) = store.settle_intent(kind.label(), id, &plan.app, &outcome.key, run_id) {
        outcome.status = RowStatus::Failed;
        outcome.reason =
            format!("created as id {id} but ownership could not be recorded: {error:#}");
    }
}

/// What [`load_owned`] found and did before a create plan.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OwnedState {
    /// Owner key -> live channel id, the `owned` argument of [`plan_creates`].
    pub channels: HashMap<String, i64>,
    /// Objects an interrupted run posted but never recorded, adopted now:
    /// `(kind, id, owner_key)`.
    pub recovered: Vec<(String, i64, String)>,
    /// Intents whose object never landed, dropped: `(kind, owner_key)`.
    pub dropped: Vec<(String, String)>,
    /// Owned ids gone from Dispatcharr (deleted by a user), forgotten: `(kind, id, key)`.
    pub forgotten: Vec<(String, i64, String)>,
}

/// Reconcile and list what `app` owns, against a freshly loaded world, before planning
/// creates.
///
/// 1. **Intents** an earlier run left unsettled (it died between a POST and its ownership
///    row) are looked up in the world by exactly what was posted: a group by exact name, a
///    logo by exact URL, a channel by exact name + group + number (when one was posted),
///    always among objects with an id ABOVE the plan's floor (Dispatcharr's ids only grow,
///    so a user's pre-existing same-named object can never match) and that nobody owns. The
///    newest match is adopted under the intent's key and run; no match means the POST never
///    landed, and the intent is dropped. An intent of a run that is still going is left to
///    that run, and a kind the world did not load is left for a later reconcile.
/// 2. **Stale ownership**: an owned id missing from a loaded slice was deleted upstream
///    (ids are never reused), so its row is forgotten and its key can be created again.
///    Hidden objects are still in the world, so undo's hide keeps ownership.
/// 3. The owned channels' keys, for [`plan_creates`].
///
/// A context without a store owns nothing and changes nothing.
pub fn load_owned(context: &JobContext, world: &World, app: &str) -> Result<OwnedState> {
    let mut state = OwnedState::default();
    let Some(store) = context.store() else {
        return Ok(state);
    };
    let loaded = |kind: &str| match kind {
        "group" => world.has(WorldKind::Groups),
        "logo" => world.has(WorldKind::Logos),
        "channel" => world.has(WorldKind::Channels),
        _ => false,
    };
    for intent in store.pending_intents(app)? {
        if !loaded(&intent.kind) {
            continue;
        }
        if Some(intent.run_id) != context.run_id() {
            let running = store
                .run(intent.run_id)?
                .is_some_and(|run| run.status.is_running());
            if running {
                continue;
            }
        }
        let mut candidates: Vec<i64> = match intent.kind.as_str() {
            "group" => world
                .groups
                .iter()
                .filter(|group| group.id > intent.after_id && group.name == intent.name)
                .map(|group| group.id)
                .collect(),
            "logo" => world
                .logos
                .iter()
                .filter(|logo| {
                    logo.id > intent.after_id && Some(logo.url.as_str()) == intent.url.as_deref()
                })
                .map(|logo| logo.id)
                .collect(),
            _ => world
                .channels
                .iter()
                .filter(|channel| {
                    channel.id > intent.after_id
                        && channel.name == intent.name
                        && channel.channel_group_id == intent.group_id
                        && intent.number.is_none_or(|number| {
                            channel_number_of(channel).map(number_key) == Some(number_key(number))
                        })
                })
                .map(|channel| channel.id)
                .collect(),
        };
        candidates.sort_unstable_by(|a, b| b.cmp(a));
        let mut adopted = None;
        for id in candidates {
            if !store.is_owned(&intent.kind, id)? {
                adopted = Some(id);
                break;
            }
        }
        match adopted {
            Some(id) => {
                store.settle_intent(&intent.kind, id, app, &intent.owner_key, intent.run_id)?;
                state.recovered.push((intent.kind, id, intent.owner_key));
            }
            None => {
                store.drop_intent(&intent.kind, app, &intent.owner_key)?;
                state.dropped.push((intent.kind, intent.owner_key));
            }
        }
    }
    let live = |kind: &str, id: i64| match kind {
        "group" => world.groups.iter().any(|group| group.id == id),
        "logo" => world.logos.iter().any(|logo| logo.id == id),
        _ => world.channels.iter().any(|channel| channel.id == id),
    };
    for (kind, id, key) in store.owned_keys(app)? {
        if !loaded(&kind) {
            if kind == "channel" {
                state.channels.insert(key, id);
            }
            continue;
        }
        if !live(&kind, id) {
            store.forget_ownership(&kind, id)?;
            state.forgotten.push((kind, id, key));
            continue;
        }
        if kind == "channel" {
            state.channels.insert(key, id);
        }
    }
    Ok(state)
}

/// Post every item concurrently, bounded by `concurrency`, checking cancellation between
/// requests and publishing progress as answers land.
///
/// The answer maps each item's index to its result. An index with NO answer was never
/// started: the run was cancelled before its POST went out, and the caller fails it as
/// cancelled. In-flight requests finish (cancelling a POST mid-flight cannot un-send it),
/// which is what "cancel mid-create records landed rows" means. `progress` is the run's
/// reporter, the count done before this stage and the stage's phase line.
async fn run_concurrent<T, F, Fut, R>(
    items: Vec<(usize, T)>,
    concurrency: usize,
    context: &JobContext,
    progress: (&mut crate::jobs::PacedProgress, u64, &str),
    f: F,
) -> HashMap<usize, R>
where
    T: Send + 'static,
    F: Fn(T) -> Fut,
    Fut: std::future::Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    use tokio::sync::Semaphore;
    use tokio::task::JoinSet;
    let (progress, done, phase) = progress;
    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut set: JoinSet<(usize, R)> = JoinSet::new();
    let mut results = HashMap::with_capacity(items.len());
    // A task that panicked has no answer: its index stays out, like a never-started one.
    let collect = |finished: std::result::Result<(usize, R), tokio::task::JoinError>,
                   results: &mut HashMap<usize, R>| {
        if let Ok((index, result)) = finished {
            results.insert(index, result);
        }
    };
    for (index, item) in items {
        if context.is_cancelled() {
            break;
        }
        // Drain finished answers while waiting for a permit, so progress moves with them.
        let permit = loop {
            if let Ok(permit) = semaphore.clone().try_acquire_owned() {
                break Some(permit);
            }
            tokio::select! {
                permit = semaphore.clone().acquire_owned() => break permit.ok(),
                Some(finished) = set.join_next() => {
                    collect(finished, &mut results);
                    progress.set(done + results.len() as u64, phase);
                }
            }
        };
        let Some(permit) = permit else {
            break;
        };
        // The future is built BEFORE spawning so `f` itself needs no `'static` bound: only
        // the future it returns is sent to the task.
        let future = f(item);
        set.spawn(async move {
            let _permit = permit;
            (index, future.await)
        });
    }
    while let Some(finished) = set.join_next().await {
        collect(finished, &mut results);
        progress.set(done + results.len() as u64, phase);
    }
    results
}

/// The create plan's rows in the store's shape, for `JobContext::record_rows`.
///
/// One row per created object — groups and logos first, then channels, then the plan's
/// skips — with `subject` saying what the id belongs to, `field` always `created`, `before`
/// empty and `after` the new id. The owner key rides along, so the next run's plan finds
/// the object it already made. A preview (`applied` is `None`) records every planned object
/// as `planned`; an apply records each outcome's own status.
///
/// Ordinals start at 0: a caller that also records edit rows shifts those after these (see
/// streams' triage), because `run_id` + `ordinal` is the row's key.
pub fn create_run_rows(plan: &CreatePlan, applied: Option<&CreateReport>) -> Vec<RunRow> {
    let mut rows = Vec::new();
    let mut ordinal = 0i64;
    // Groups, then logos, then channels: the dependency order the apply ran in.
    for (index, group) in plan.groups.iter().enumerate() {
        let outcome = applied.and_then(|report| report.groups.get(index));
        rows.push(RunRow {
            ordinal,
            subject: Subject::Group,
            channel_id: outcome.and_then(|outcome| outcome.id).unwrap_or(0),
            channel_name: group.name.clone(),
            channel_group: group.name.clone(),
            channel_number: None,
            method: "create".to_string(),
            field: "created".to_string(),
            before_value: None,
            after_value: outcome
                .and_then(|outcome| outcome.id)
                .map(|id| id.to_string()),
            status: outcome
                .map(|outcome| outcome.status.as_str())
                .unwrap_or(RowStatus::Planned.as_str())
                .to_string(),
            reason: outcome
                .map(|outcome| outcome.reason.clone())
                .unwrap_or_default(),
            owner_key: Some(group.key.clone()),
            ..RunRow::default()
        });
        ordinal += 1;
    }
    for (index, logo) in plan.logos.iter().enumerate() {
        let outcome = applied.and_then(|report| report.logos.get(index));
        rows.push(RunRow {
            ordinal,
            subject: Subject::Logo,
            channel_id: outcome.and_then(|outcome| outcome.id).unwrap_or(0),
            channel_name: logo.name.clone(),
            channel_group: String::new(),
            channel_number: None,
            method: "create".to_string(),
            field: "created".to_string(),
            before_value: None,
            after_value: outcome
                .and_then(|outcome| outcome.id)
                .map(|id| id.to_string()),
            status: outcome
                .map(|outcome| outcome.status.as_str())
                .unwrap_or(RowStatus::Planned.as_str())
                .to_string(),
            reason: outcome
                .map(|outcome| outcome.reason.clone())
                .unwrap_or_default(),
            owner_key: Some(logo.key.clone()),
            ..RunRow::default()
        });
        ordinal += 1;
    }
    for (index, channel) in plan.channels.iter().enumerate() {
        let outcome = applied.and_then(|report| report.channels.get(index));
        rows.push(RunRow {
            ordinal,
            subject: Subject::Channel,
            channel_id: outcome.and_then(|outcome| outcome.id).unwrap_or(0),
            channel_name: channel.name.clone(),
            channel_group: channel.group_name.clone(),
            channel_number: channel.number,
            method: "create".to_string(),
            field: "created".to_string(),
            before_value: None,
            after_value: outcome
                .and_then(|outcome| outcome.id)
                .map(|id| id.to_string()),
            status: outcome
                .map(|outcome| outcome.status.as_str())
                .unwrap_or(RowStatus::Planned.as_str())
                .to_string(),
            reason: outcome
                .map(|outcome| outcome.reason.clone())
                .unwrap_or_default(),
            owner_key: Some(channel.key.clone()),
            ..RunRow::default()
        });
        ordinal += 1;
    }
    for skip in &plan.skipped {
        rows.push(RunRow {
            ordinal,
            subject: skip.kind.subject(),
            channel_id: 0,
            channel_name: skip.name.clone(),
            channel_group: String::new(),
            channel_number: None,
            method: "create".to_string(),
            field: skip.kind.label().to_string(),
            status: "skipped".to_string(),
            reason: skip.reason.clone(),
            owner_key: Some(skip.key.clone()),
            ..RunRow::default()
        });
        ordinal += 1;
    }
    rows
}

/// Write the create plan (or what applying it did) as a CSV report.
///
/// The same [`COLUMNS`] as [`write_ops_report`]: one row per created object with the new id
/// as `after`, then the plan's skips. `field` is always `created` for a created object.
pub fn write_create_report(
    context: &JobContext,
    spec: &ReportSpec<'_>,
    plan: &CreatePlan,
    applied: Option<&CreateReport>,
) -> Result<PathBuf> {
    let exports_dir = context.exports_dir();
    let planned = plan.planned_creates();
    let report = Report::new(&exports_dir, spec.app, spec.stem, spec.title)
        .channels_processed(plan.channels.len())
        .result("Selection", &plan.description)
        .result("Creates planned", planned)
        .result(
            "Creates written",
            match applied {
                Some(report) => report.created().to_string(),
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
            // (id, name, number, group, field, before, after, status, reason), owned: the
            // ids only exist after an apply, the names always do.
            let mut outcome_rows: Vec<CreateCsvRow> = Vec::new();
            // Each section zips the plan with the report's outcomes in order; a missing
            // outcome (a report from an older build) reads as not yet attempted.
            let groups: Vec<Option<&CreatedOutcome>> = match applied {
                Some(report) => report.groups.iter().map(Some).collect(),
                None => vec![None; plan.groups.len()],
            };
            for (group, outcome) in plan.groups.iter().zip(groups.iter()) {
                let (id, status, reason) = outcome_cells(outcome);
                outcome_rows.push((
                    id.clone(),
                    group.name.clone(),
                    None,
                    group.name.clone(),
                    "created".to_string(),
                    String::new(),
                    id.clone(),
                    status,
                    reason,
                ));
            }
            let logos: Vec<Option<&CreatedOutcome>> = match applied {
                Some(report) => report.logos.iter().map(Some).collect(),
                None => vec![None; plan.logos.len()],
            };
            for (logo, outcome) in plan.logos.iter().zip(logos.iter()) {
                let (id, status, reason) = outcome_cells(outcome);
                outcome_rows.push((
                    id.clone(),
                    logo.name.clone(),
                    None,
                    String::new(),
                    "created".to_string(),
                    String::new(),
                    id.clone(),
                    status,
                    reason,
                ));
            }
            let channels: Vec<Option<&CreatedOutcome>> = match applied {
                Some(report) => report.channels.iter().map(Some).collect(),
                None => vec![None; plan.channels.len()],
            };
            for (channel, outcome) in plan.channels.iter().zip(channels.iter()) {
                let (id, status, reason) = outcome_cells(outcome);
                outcome_rows.push((
                    id.clone(),
                    channel.name.clone(),
                    channel.number,
                    channel.group_name.clone(),
                    "created".to_string(),
                    String::new(),
                    id.clone(),
                    status,
                    reason,
                ));
            }
            for (id, name, number, group, field, before, after, status, reason) in &outcome_rows {
                writer.write_record([
                    id.clone(),
                    csv_report::safe_cell(name),
                    number_cell(*number),
                    csv_report::safe_cell(group),
                    field.clone(),
                    csv_report::safe_cell(before),
                    csv_report::safe_cell(after),
                    status.clone(),
                    csv_report::safe_cell(reason),
                ])?;
            }
            for skip in &plan.skipped {
                writer.write_record([
                    "0".to_string(),
                    csv_report::safe_cell(&skip.name),
                    String::new(),
                    String::new(),
                    skip.kind.label().to_string(),
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

/// One create row for the CSV: (id, name, number, group, field, before, after, status,
/// reason). All owned strings: the ids only exist after an apply, the names always do.
type CreateCsvRow = (
    String,
    String,
    Option<f64>,
    String,
    String,
    String,
    String,
    String,
    String,
);

/// One outcome's CSV cells: the id (empty before it exists), the status and the reason.
fn outcome_cells(outcome: &Option<&CreatedOutcome>) -> (String, String, String) {
    match outcome {
        Some(outcome) => (
            outcome.id.map(|id| id.to_string()).unwrap_or_default(),
            outcome.status.as_str().to_string(),
            outcome.reason.clone(),
        ),
        None => (
            String::new(),
            RowStatus::Planned.as_str().to_string(),
            String::new(),
        ),
    }
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
    number: Option<f64>,
    logo: Option<i64>,
}

impl Working {
    fn of(channel: &Channel) -> Self {
        Self {
            name: channel.name.clone(),
            group: channel.channel_group_id,
            epg: channel.current_epg_id(),
            hidden: channel.hidden_from_output,
            streams: channel.streams.clone(),
            number: channel_number_of(channel),
            logo: channel.logo_id,
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
                (Field::Number, SetValue::Number(number)) => {
                    plan_number(channel, group, kind, *number, current, planned, skipped);
                }
                (Field::Logo, SetValue::Logo(logo)) => {
                    plan_logo(
                        channel, group, kind, *logo, lookups, current, planned, skipped,
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

/// Plan a channel number, from a per-channel value.
///
/// Dispatcharr stores the number as a float; the comparison is exact, because `5` and `5.5`
/// are different numbers and the API echoes back what it was given. A channel already on the
/// number is a no-op skip, like every other field.
fn plan_number(
    channel: &Channel,
    group: &str,
    kind: OpKind,
    number: Option<f64>,
    current: &mut Working,
    planned: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
) {
    if current.number == number {
        let reason = match number {
            Some(number) => format!("the channel already has number {number}"),
            None => "the channel already has no number".to_string(),
        };
        skipped.push(Skip::of(channel, group, reason).with_op(kind, Field::Number));
        return;
    }
    record(
        planned,
        Change::planned(
            channel,
            group,
            kind,
            Field::Number,
            Value::Number(current.number),
            Value::Number(number),
        ),
    );
    current.number = number;
}

/// Plan a logo, from a per-channel value: an id, or `None` to clear it.
///
/// The names are cosmetic (a world loaded without the logos slice plans and applies the same
/// change, it just prints the id), exactly like stream lists.
#[allow(clippy::too_many_arguments)]
fn plan_logo(
    channel: &Channel,
    group: &str,
    kind: OpKind,
    logo: Option<i64>,
    lookups: &Lookups<'_>,
    current: &mut Working,
    planned: &mut Vec<Change>,
    skipped: &mut Vec<Skip>,
) {
    if current.logo == logo {
        let reason = match logo {
            Some(_) => "the channel already has this logo".to_string(),
            None => "the channel already has no logo".to_string(),
        };
        skipped.push(Skip::of(channel, group, reason).with_op(kind, Field::Logo));
        return;
    }
    record(
        planned,
        Change::planned(
            channel,
            group,
            kind,
            Field::Logo,
            Value::Logo(logo_values(lookups, current.logo)),
            Value::Logo(logo_values(lookups, logo)),
        ),
    );
    current.logo = logo;
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

/// Parse a `channel_number` wire value back into a number, for a restore.
///
/// `none` (and an empty string) is the CLEARED number: a channel with no number is a real
/// value a restore may have to put back, which is not the same as "no value was recorded".
/// `None` is a value that parses as neither, which the caller skips and reports.
pub fn parse_number_wire(wire: &str) -> Option<Option<f64>> {
    let wire = wire.trim();
    if wire.is_empty() || wire.eq_ignore_ascii_case("none") {
        return Some(None);
    }
    wire.parse::<f64>().ok().map(Some)
}

/// Parse a `logo_id` wire value back into a logo id, for a restore.
///
/// `none` (and an empty string) is the CLEARED logo, like [`parse_number_wire`]; `None` is
/// unparseable.
pub fn parse_logo_wire(wire: &str) -> Option<Option<i64>> {
    let wire = wire.trim();
    if wire.is_empty() || wire.eq_ignore_ascii_case("none") {
        return Some(None);
    }
    wire.parse::<i64>().ok().map(Some)
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

/// Guide entries, streams and logos by id, for the per-channel lookups of one plan.
///
/// All three are optional detail rather than state: a guide name is printed in the report,
/// a stream name in a stream list, a logo name in a logo change. A world loaded without one
/// of those slices plans and applies the same changes, it just prints the id instead of the
/// name.
struct Lookups<'a> {
    entries: HashMap<i64, &'a EpgEntry>,
    streams: HashMap<i64, &'a Stream>,
    logos: HashMap<i64, &'a Logo>,
}

impl<'a> Lookups<'a> {
    /// Index the world's guide entries, and its streams and logos when the plan needs them.
    fn of(world: &'a World, with_streams: bool, with_logos: bool) -> Self {
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
            logos: if with_logos {
                world.logos.iter().map(|logo| (logo.id, logo)).collect()
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

/// The logo value for a logo id, name included so the report needs no world.
fn logo_values(lookups: &Lookups<'_>, logo: Option<i64>) -> Option<(i64, String)> {
    let id = logo?;
    let name = lookups
        .logos
        .get(&id)
        .map(|logo| logo.name.clone())
        .unwrap_or_else(|| format!("Logo {id}"));
    Some((id, name))
}

/// The channel number as an f64, for the working state a number plan compares against.
///
/// Dispatcharr answers the number as a JSON number or, on some builds, a string; anything
/// else (including null) is no number.
fn channel_number_of(channel: &Channel) -> Option<f64> {
    match channel.channel_number.as_ref()? {
        serde_json::Value::Number(number) => number.as_f64(),
        serde_json::Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
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
    fn a_suffix_check_is_case_insensitive_and_bracket_aware() {
        // Step 8 (migration list): the shared rule is `tags::has_trailing`, not the
        // plugin's exact `ends_with`. `CNN [badepg]` (case) and `CNN (BadEPG)` (brackets)
        // already count as tagged — a Janitor-parity change for those variants (oracle
        // diff, not a gate). The append itself stays verbatim.
        let mut tagged = world();
        tagged.channels[0].name = "CNN [badepg]".to_string();
        tagged.channels[2].name = "CNN (BadEPG)".to_string();
        let plan = plan_ops(
            &tagged,
            &Selection::default(),
            &[Op::Rename {
                kind: RenameKind::Suffix(" [BadEPG]".to_string()),
            }],
        )
        .expect("plans");
        for id in [1, 3] {
            assert!(
                plan.skipped
                    .iter()
                    .any(|skip| skip.channel_id == id && skip.reason.contains("already ends with")),
                "channel {id} is skipped as tagged: {:#?}",
                plan.skipped
            );
        }
        assert!(
            plan.changes.iter().all(|change| change.field != Field::Name
                || (change.channel_id != 1 && change.channel_id != 3)),
            "{:#?}",
            plan.changes
        );
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
            "op": "set", "field": "tvg_id", "values": {}
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

    // ---- Field::Number / Field::Logo -------------------------------------------

    fn numbered(id: i64, number: f64, logo: Option<i64>) -> Channel {
        Channel {
            id,
            name: format!("Channel {id}"),
            channel_number: Some(serde_json::json!(number)),
            channel_group_id: Some(10),
            logo_id: logo,
            ..Channel::default()
        }
    }

    fn numbered_world() -> World {
        World {
            channels: vec![numbered(1, 5.0, Some(70)), numbered(2, 6.0, None)],
            groups: vec![crate::dispatcharr::ChannelGroup {
                id: 10,
                name: "Sports".to_string(),
            }],
            // Logo 70 exists for the name the report prints; 71 does not (the plan still
            // works, printing the id).
            logos: vec![crate::dispatcharr::Logo {
                id: 70,
                name: "ESPN".to_string(),
                url: "http://logos/espn.png".to_string(),
                ..crate::dispatcharr::Logo::default()
            }],
            ..World::default()
        }
    }

    #[test]
    fn number_and_logo_set_plan_noop_report_and_restore_round_trip() {
        let mut values = HashMap::new();
        values.insert(1, SetValue::Number(Some(7.0)));
        values.insert(2, SetValue::Number(Some(6.0)));
        let plan = plan_ops(
            &numbered_world(),
            &Selection::default(),
            &[Op::Set {
                field: Field::Number,
                values,
            }],
        )
        .expect("plans");
        // Channel 2 already has 6: a no-op skip, not a write.
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].channel_id, 1);
        assert_eq!(plan.changes[0].field, Field::Number);
        assert_eq!(
            plan.changes[0].before,
            Value::Number(Some(5.0)),
            "the before value is the world's number"
        );
        assert_eq!(plan.changes[0].after, Value::Number(Some(7.0)));
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].channel_id, 2);

        let mut values = HashMap::new();
        values.insert(1, SetValue::Logo(Some(71)));
        values.insert(2, SetValue::Logo(None));
        let plan = plan_ops(
            &numbered_world(),
            &Selection::default(),
            &[Op::Set {
                field: Field::Logo,
                values,
            }],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(
            plan.changes[0].after,
            Value::Logo(Some((71, "Logo 71".to_string()))),
            "an unknown logo prints its id"
        );
        assert_eq!(plan.skipped.len(), 1, "clearing an empty logo is a no-op");

        // Wire round-trips the way a restore parses them back.
        assert_eq!(Value::Number(Some(7.0)).wire(), "7");
        assert_eq!(Value::Number(None).wire(), "none");
        assert_eq!(Value::Logo(Some((71, "X".to_string()))).wire(), "71");
        assert_eq!(Value::Logo(None).wire(), "none");
        assert_eq!(parse_number_wire("7"), Some(Some(7.0)));
        assert_eq!(parse_number_wire("none"), Some(None));
        assert_eq!(parse_number_wire("abc"), None);
        assert_eq!(parse_logo_wire("71"), Some(Some(71)));
        assert_eq!(parse_logo_wire("none"), Some(None));
        assert_eq!(parse_logo_wire(""), Some(None));
        assert_eq!(parse_logo_wire("abc"), None);
        // Field labels round-trip for the restore reader.
        assert_eq!(Field::from_label("channel_number"), Some(Field::Number));
        assert_eq!(Field::from_label("logo_id"), Some(Field::Logo));
        assert_eq!(Field::Number.label(), "channel_number");
        assert_eq!(Field::Logo.label(), "logo_id");
    }

    #[tokio::test]
    async fn number_and_logo_apply_through_the_bulk_edit() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/api/channels/channels/edit/bulk/"))
            .and(body_json(serde_json::json!([
                {"id": 1, "channel_number": 7.0, "logo_id": 71},
                {"id": 2, "channel_number": null}
            ])))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"message": "ok", "channels": [{"id": 1}, {"id": 2}]}),
            ))
            .mount(&server)
            .await;
        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let mut values = HashMap::new();
        values.insert(1, SetValue::Number(Some(7.0)));
        values.insert(2, SetValue::Number(None));
        let mut logos = HashMap::new();
        logos.insert(1, SetValue::Logo(Some(71)));
        let plan = plan_ops(
            &numbered_world(),
            &Selection::default(),
            &[
                Op::Set {
                    field: Field::Number,
                    values,
                },
                Op::Set {
                    field: Field::Logo,
                    values: logos,
                },
            ],
        )
        .expect("plans");
        // Two number changes and one logo change fold into one edit row per channel.
        assert_eq!(plan.changes.len(), 3);
        let report = apply(&client, &plan, &context, "test.numbers").await;
        assert!(report.all_ok(), "{:#?}", report.changes);
        // A cleared logo sends an explicit null, like a cleared guide.
        let mut clears = HashMap::new();
        clears.insert(1, SetValue::Logo(None));
        let plan = plan_ops(
            &numbered_world(),
            &Selection::default(),
            &[Op::Set {
                field: Field::Logo,
                values: clears,
            }],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 1);
    }

    // ---- Create stage ----------------------------------------------------------

    fn create_world() -> World {
        World {
            channels: vec![
                numbered(1, 5.0, Some(70)),
                numbered(2, 9.0, None),
                Channel {
                    id: 3,
                    name: "ESPN".to_string(),
                    channel_group_id: None,
                    ..Channel::default()
                },
            ],
            groups: vec![crate::dispatcharr::ChannelGroup {
                id: 10,
                name: "Sports".to_string(),
            }],
            logos: vec![crate::dispatcharr::Logo {
                id: 70,
                name: "ESPN".to_string(),
                url: "http://logos/espn.png".to_string(),
                ..crate::dispatcharr::Logo::default()
            }],
            ..World::default()
        }
    }

    fn group_ref(index: usize) -> Option<GroupRef> {
        Some(GroupRef::Planned(index))
    }

    /// An in-memory [`ChannelCreator`]: ids count up from `next`, names in `fail` fail.
    struct FakeCreator {
        next: std::sync::Mutex<i64>,
        fail_groups: std::collections::HashSet<String>,
        created_channels: std::sync::Mutex<Vec<String>>,
        concurrency: usize,
    }

    impl FakeCreator {
        fn new(next: i64) -> Self {
            Self {
                next: std::sync::Mutex::new(next),
                fail_groups: std::collections::HashSet::new(),
                created_channels: std::sync::Mutex::new(Vec::new()),
                concurrency: 8,
            }
        }

        fn failing_group(name: &str) -> Self {
            let mut creator = Self::new(100);
            creator.fail_groups.insert(name.to_string());
            creator
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
            if self.fail_groups.contains(name) {
                anyhow::bail!("group {name:?} failed");
            }
            Ok(ChannelGroup {
                id: self.id(),
                name: name.to_string(),
            })
        }

        async fn create_logo(&self, name: &str, url: &str) -> Result<Logo> {
            Ok(Logo {
                id: self.id(),
                name: name.to_string(),
                url: url.to_string(),
                ..crate::dispatcharr::Logo::default()
            })
        }

        async fn create_channel(&self, channel: &ChannelCreate) -> Result<Channel> {
            self.created_channels
                .lock()
                .expect("lock")
                .push(channel.name.clone());
            Ok(Channel {
                id: self.id(),
                name: channel.name.clone(),
                channel_group_id: channel.channel_group_id,
                logo_id: channel.logo_id,
                ..Channel::default()
            })
        }

        fn concurrency(&self) -> usize {
            self.concurrency
        }
    }

    fn request(channels: Vec<NewChannel>) -> CreateRequest {
        CreateRequest {
            app: "lineup".to_string(),
            groups: vec![NewGroup {
                name: "News".to_string(),
                key: "lineup:file:cat:group".to_string(),
            }],
            logos: vec![NewLogo {
                name: "CNN".to_string(),
                url: "http://logos/cnn.png".to_string(),
                key: "lineup:logo:cnn".to_string(),
            }],
            channels,
        }
    }

    fn cnn(number: Option<f64>) -> NewChannel {
        NewChannel {
            name: "CNN".to_string(),
            number,
            group: group_ref(0),
            logo: Some(LogoRef::Planned(0)),
            streams: vec![11, 12],
            profile_ids: Some(vec![1]),
            tvg_id: Some("cnn.us".to_string()),
            epg_data_id: Some(44),
            key: "lineup:file:news:cnn".to_string(),
        }
    }

    #[tokio::test]
    async fn creates_plan_dedup_resolve_and_apply_with_ownership() {
        // The world already has group Sports and logo espn.png: both dedup to existing ids.
        let mut existing = request(vec![cnn(Some(7.0))]);
        existing.groups.push(NewGroup {
            name: "Sports".to_string(),
            key: "lineup:file:cat:sports".to_string(),
        });
        existing.logos.push(NewLogo {
            name: "ESPN copy".to_string(),
            url: "http://logos/espn.png".to_string(),
            key: "lineup:logo:espn".to_string(),
        });
        let plan = plan_creates(
            &create_world(),
            &HashMap::new(),
            &existing,
            &PlanOptions::default(),
        )
        .expect("plans");
        assert_eq!(plan.groups.len(), 2);
        assert_eq!(plan.groups[0].existing_id, None, "News is new");
        assert_eq!(
            plan.groups[1].existing_id,
            Some(10),
            "Sports reuses the world's id"
        );
        assert_eq!(plan.logos[1].existing_id, Some(70), "logo dedups by URL");
        assert_eq!(plan.channels.len(), 1);
        // Refs resolve: the new group is index 0 in the plan, the logo too.
        assert_eq!(
            plan.channels[0].group,
            Some(PlannedGroupRef::New(0)),
            "a ref to the new group"
        );
        assert_eq!(plan.channels[0].logo, Some(PlannedLogoRef::New(0)));
        assert_eq!(plan.channels[0].group_name, "News");

        let store = crate::store::Store::in_memory().expect("store");
        let run_id = store
            .start_run("lineup", "create_apply", false)
            .expect("start");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default()).for_run(
            "lineup",
            "create_apply",
            run_id,
            store.clone(),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        let creator: std::sync::Arc<dyn ChannelCreator> =
            std::sync::Arc::new(FakeCreator::new(100));
        let report = apply_creates(creator, &plan, &context, "lineup.create_apply").await;
        assert!(report.all_ok(), "{report:#?}");
        // Groups: News created as 100, Sports reused as 10. Logos: cnn as 101, espn reused
        // as 70. The channel takes the created group and logo ids.
        assert_eq!(report.groups[0].id, Some(100));
        assert!(report.groups[0].created);
        assert!(!report.groups[1].created);
        assert_eq!(report.channels[0].id, Some(102));
        // Ownership: every created object is recorded under the app and its key.
        assert!(store.is_owned("group", 100).expect("query"));
        assert!(store.is_owned("logo", 101).expect("query"));
        assert!(store.is_owned("channel", 102).expect("query"));
        assert_eq!(
            store.owner_of("channel", 102).expect("query"),
            Some((
                "lineup".to_string(),
                "lineup:file:news:cnn".to_string(),
                run_id
            )),
        );
        // Reused objects are NOT adopted: only what this run made is owned.
        assert!(!store.is_owned("group", 10).expect("query"));
        // Run rows: one per created object with the new id and the owner key.
        let rows = create_run_rows(&plan, Some(&report));
        assert_eq!(rows.len(), 5, "2 groups + 2 logos + 1 channel");
        let channel_row = rows
            .iter()
            .find(|row| row.subject == Subject::Channel)
            .expect("channel row");
        assert_eq!(channel_row.after_value.as_deref(), Some("102"));
        assert_eq!(channel_row.field, "created");
        assert_eq!(
            channel_row.owner_key.as_deref(),
            Some("lineup:file:news:cnn")
        );
        store.append_rows(run_id, &rows).expect("append");
        assert_eq!(store.rows(run_id).expect("rows").len(), 5);
    }

    #[tokio::test]
    async fn a_failed_group_refuses_its_channels_but_not_its_siblings() {
        let other = NewChannel {
            name: "BBC".to_string(),
            number: None,
            group: Some(GroupRef::Existing(10)),
            logo: None,
            streams: Vec::new(),
            profile_ids: None,
            tvg_id: None,
            epg_data_id: None,
            key: "lineup:file:news:bbc".to_string(),
        };
        let plan = plan_creates(
            &create_world(),
            &HashMap::new(),
            &request(vec![cnn(None), other]),
            &PlanOptions::default(),
        )
        .expect("plans");
        assert_eq!(plan.channels.len(), 2);

        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let creator: std::sync::Arc<dyn ChannelCreator> =
            std::sync::Arc::new(FakeCreator::failing_group("News"));
        let report = apply_creates(creator, &plan, &context, "lineup.create_apply").await;
        assert!(!report.all_ok());
        assert_eq!(report.failed, 2, "the group and its channel");
        assert_eq!(report.groups[0].status, RowStatus::Failed);
        let refused = &report.channels[0];
        assert_eq!(refused.status, RowStatus::Failed);
        assert!(
            refused.reason.contains("News") && refused.reason.contains("could not be created"),
            "the refusal names the lost group: {}",
            refused.reason
        );
        // The sibling in the existing group still lands.
        assert_eq!(report.channels[1].status, RowStatus::Ok);
        assert_eq!(report.channels[1].id, Some(101));
    }

    #[tokio::test]
    async fn an_owned_key_is_never_created_twice() {
        let mut owned = HashMap::new();
        owned.insert("lineup:file:news:cnn".to_string(), 1);
        let plan = plan_creates(
            &create_world(),
            &owned,
            &request(vec![cnn(Some(7.0))]),
            &PlanOptions::default(),
        )
        .expect("plans");
        assert!(
            plan.channels.is_empty(),
            "the owned channel is not re-created"
        );
        assert_eq!(plan.skipped.len(), 1);
        assert!(plan.skipped[0]
            .reason
            .contains("already created as channel 1"));

        // A cancelled run sends no POSTs and fails every row as cancelled.
        let plan = plan_creates(
            &create_world(),
            &HashMap::new(),
            &request(vec![cnn(Some(7.0))]),
            &PlanOptions::default(),
        )
        .expect("plans");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        context.cancel();
        let creator: std::sync::Arc<dyn ChannelCreator> =
            std::sync::Arc::new(FakeCreator::new(100));
        let report = apply_creates(creator, &plan, &context, "lineup.create_apply").await;
        assert_eq!(report.ok, 0);
        assert_eq!(
            report.failed, 3,
            "the group, the logo and the refused channel"
        );
        assert!(
            report
                .groups
                .iter()
                .all(|outcome| outcome.reason == CANCELLED),
            "nothing was started: {report:#?}"
        );
    }

    /// A creator that cancels the run from inside the first POST: the group lands, the
    /// stages after it do not start.
    struct CancelOnGroup {
        flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
        next: std::sync::Mutex<i64>,
    }

    #[async_trait::async_trait]
    impl ChannelCreator for CancelOnGroup {
        async fn create_group(&self, name: &str) -> Result<ChannelGroup> {
            self.flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(ChannelGroup {
                id: {
                    let mut next = self.next.lock().expect("lock");
                    let id = *next;
                    *next += 1;
                    id
                },
                name: name.to_string(),
            })
        }

        async fn create_logo(&self, name: &str, url: &str) -> Result<Logo> {
            Ok(Logo {
                id: 999,
                name: name.to_string(),
                url: url.to_string(),
                ..crate::dispatcharr::Logo::default()
            })
        }

        async fn create_channel(&self, channel: &ChannelCreate) -> Result<Channel> {
            Ok(Channel {
                id: 998,
                name: channel.name.clone(),
                ..Channel::default()
            })
        }

        fn concurrency(&self) -> usize {
            1
        }
    }

    #[test]
    fn number_conflicts_skip_used_numbers_in_edits_and_creates() {
        // The world holds 5 and 9 in group 10; 9 sits on channel 2.
        let options = PlanOptions {
            number_conflicts: true,
            ..PlanOptions::default()
        };
        let mut values = HashMap::new();
        values.insert(1, SetValue::Number(Some(9.0)));
        let plan = plan_with_options(
            &create_world(),
            &Selection::default(),
            &[Op::Set {
                field: Field::Number,
                values,
            }],
            &options,
        );
        let plan = plan.expect("plans");
        assert!(
            plan.changes.is_empty(),
            "9 is held by channel 2 in group 10"
        );
        assert_eq!(plan.skipped.len(), 1);
        assert!(plan.skipped[0].reason.contains("already used by channel 2"));

        // Off by default: the same plan writes.
        let mut values = HashMap::new();
        values.insert(1, SetValue::Number(Some(9.0)));
        let plan = plan_ops(
            &create_world(),
            &Selection::default(),
            &[Op::Set {
                field: Field::Number,
                values,
            }],
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 1);

        // Creates: 9 is taken (whatever the group), 10 in the NEW group is free once, and
        // clearing never conflicts.
        let taken = NewChannel {
            name: "Taken".to_string(),
            number: Some(9.0),
            group: Some(GroupRef::Existing(10)),
            logo: None,
            streams: Vec::new(),
            profile_ids: None,
            tvg_id: None,
            epg_data_id: None,
            key: "lineup:k:taken".to_string(),
        };
        let mut free = taken.clone();
        free.name = "Free".to_string();
        free.key = "lineup:k:free".to_string();
        free.group = group_ref(0);
        free.number = Some(10.0);
        let mut second = free.clone();
        second.name = "Second".to_string();
        second.key = "lineup:k:second".to_string();
        let mut clearing = taken.clone();
        clearing.name = "Clearing".to_string();
        clearing.key = "lineup:k:clearing".to_string();
        clearing.number = None;
        let plan = plan_creates(
            &create_world(),
            &HashMap::new(),
            &CreateRequest {
                app: "lineup".to_string(),
                groups: vec![NewGroup {
                    name: "News".to_string(),
                    key: "lineup:k:g".to_string(),
                }],
                logos: Vec::new(),
                channels: vec![taken, free, second, clearing],
            },
            &options,
        )
        .expect("plans");
        assert_eq!(
            plan.channels
                .iter()
                .map(|channel| channel.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Free", "Clearing"],
            "the taken number and the second claim of it are skipped"
        );
        assert_eq!(plan.skipped.len(), 2);
        assert!(plan.skipped[0].reason.contains("already used by channel 2"));
    }

    #[test]
    fn collision_strategies_guard_planned_create_names() {
        // The world already has ESPN (id 3, no group): global scope, like renames.
        let named = |name: &str, key: &str| NewChannel {
            name: name.to_string(),
            number: None,
            group: None,
            logo: None,
            streams: Vec::new(),
            profile_ids: None,
            tvg_id: None,
            epg_data_id: None,
            key: key.to_string(),
        };
        let requested = || CreateRequest {
            app: "lineup".to_string(),
            groups: Vec::new(),
            logos: Vec::new(),
            channels: vec![named("espn", "k:1"), named("BBC", "k:2")],
        };
        let numeric = PlanOptions {
            collision: Some(crate::naming::CollisionStrategy::Numeric),
            collision_sep: " ".to_string(),
            ..PlanOptions::default()
        };
        let plan =
            plan_creates(&create_world(), &HashMap::new(), &requested(), &numeric).expect("plans");
        // Case-insensitive: "espn" collides with the world's "ESPN" and counts up.
        assert_eq!(plan.channels[0].name, "espn (2)");
        assert_eq!(plan.channels[1].name, "BBC");

        let skip = PlanOptions {
            collision: Some(crate::naming::CollisionStrategy::Skip),
            collision_sep: " ".to_string(),
            ..PlanOptions::default()
        };
        let plan =
            plan_creates(&create_world(), &HashMap::new(), &requested(), &skip).expect("plans");
        assert_eq!(plan.channels.len(), 1);
        assert_eq!(plan.channels[0].name, "BBC");
        assert_eq!(plan.skipped.len(), 1);
        assert!(plan.skipped[0]
            .reason
            .contains("collision strategy is skip"));

        // Off by default: the colliding name plans as-is.
        let plan = plan_creates(
            &create_world(),
            &HashMap::new(),
            &requested(),
            &PlanOptions::default(),
        )
        .expect("plans");
        assert_eq!(plan.channels.len(), 2);
    }

    #[test]
    fn invalid_refs_fail_the_create_plan() {
        let mut channel = cnn(None);
        channel.group = Some(GroupRef::Planned(7));
        let error = plan_creates(
            &create_world(),
            &HashMap::new(),
            &request(vec![channel]),
            &PlanOptions::default(),
        )
        .expect_err("a dangling group ref is a caller bug");
        assert!(
            format!("{error:#}").contains("planned group 7"),
            "{error:#}"
        );
    }

    #[test]
    fn next_free_numbers_skips_used_and_counts_up() {
        // The world holds 5 and 9 (group 10); numbers are global, so every group shares them.
        assert_eq!(
            next_free_numbers(&create_world(), 1.0, 4),
            vec![1.0, 2.0, 3.0, 4.0]
        );
        assert_eq!(
            next_free_numbers(&create_world(), 5.0, 3),
            vec![6.0, 7.0, 8.0],
            "5 is held, so counting starts past it"
        );
        assert_eq!(
            next_free_numbers(&create_world(), 8.0, 2),
            vec![8.0, 10.0],
            "9 is held in group 10, which an ungrouped create cannot reuse either"
        );
        // Fractional starts step by whole numbers, mirroring the endpoint's walk.
        assert_eq!(next_free_numbers(&create_world(), 4.5, 2), vec![4.5, 5.5]);
    }

    #[test]
    fn number_conflicts_are_global_and_a_refused_renumber_keeps_its_number() {
        let options = PlanOptions {
            number_conflicts: true,
            ..PlanOptions::default()
        };
        let mut world = create_world();
        // Channel 3 (ESPN) is ungrouped; 9 is held by channel 2 in group 10.
        let mut values = HashMap::new();
        values.insert(3, SetValue::Number(Some(9.0)));
        let plan = plan_with_options(
            &world,
            &Selection::default(),
            &[Op::Set {
                field: Field::Number,
                values,
            }],
            &options,
        )
        .expect("plans");
        assert!(plan.changes.is_empty(), "9 is taken in ANOTHER group too");
        assert!(plan.skipped[0].reason.contains("already used by channel 2"));

        // Channel 1 (5) asks for 9 (refused: held by 2), channel 3 asks for 5. Channel 1
        // keeps 5 because its own move was refused, so channel 3's claim must lose too.
        world.channels[2].channel_number = None;
        let mut values = HashMap::new();
        values.insert(1, SetValue::Number(Some(9.0)));
        values.insert(3, SetValue::Number(Some(5.0)));
        let plan = plan_with_options(
            &world,
            &Selection::default(),
            &[Op::Set {
                field: Field::Number,
                values,
            }],
            &options,
        )
        .expect("plans");
        assert!(
            plan.changes.is_empty(),
            "neither renumber may land: {:?}",
            plan.changes
        );
        assert_eq!(plan.skipped.len(), 2);

        // A swap stays legal: both vacate, both claim the other's old number.
        let mut values = HashMap::new();
        values.insert(1, SetValue::Number(Some(9.0)));
        values.insert(2, SetValue::Number(Some(5.0)));
        let plan = plan_with_options(
            &world,
            &Selection::default(),
            &[Op::Set {
                field: Field::Number,
                values,
            }],
            &options,
        )
        .expect("plans");
        assert_eq!(plan.changes.len(), 2, "{:?}", plan.skipped);
    }

    /// A store-backed context for one run of `app`.
    fn run_context(store: &crate::store::Store, app: &str) -> (JobContext, i64) {
        let run_id = store.start_run(app, "create_apply", false).expect("start");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default()).for_run(
            app,
            "create_apply",
            run_id,
            store.clone(),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        (context, run_id)
    }

    #[tokio::test]
    async fn a_crash_between_post_and_ownership_is_adopted_not_duplicated() {
        let store = crate::store::Store::in_memory().expect("store");
        let world = create_world();
        let plan = plan_creates(
            &world,
            &HashMap::new(),
            &request(vec![cnn(Some(7.0))]),
            &PlanOptions::default(),
        )
        .expect("plans");
        assert_eq!(plan.id_floor.channel, 3);
        // The run posts, then dies before any answer is recorded: its intents remain.
        let (context, run_id) = run_context(&store, "lineup");
        let intents = vec![
            intent(
                &plan,
                &context,
                CreatedKind::Group,
                "lineup:file:cat:group",
                "News",
            ),
            CreateIntent {
                group_id: Some(100),
                number: Some(7.0),
                ..intent(
                    &plan,
                    &context,
                    CreatedKind::Channel,
                    "lineup:file:news:cnn",
                    "CNN",
                )
            },
            intent(&plan, &context, CreatedKind::Logo, "lineup:logo:cnn", "CNN"),
        ];
        store.record_intents(&intents).expect("intents");
        store
            .finish_run(
                run_id,
                crate::store::RunStatus::Failed,
                None,
                &serde_json::json!({}),
                None,
            )
            .expect("finish");

        // What landed upstream: group 100 and channel 102 (the logo never did). A user's
        // own same-named channel 2 predates the plan and must not be adopted.
        let mut after = world.clone();
        after.groups.push(ChannelGroup {
            id: 100,
            name: "News".to_string(),
        });
        after.channels[1].name = "CNN".to_string();
        after.channels[1].channel_group_id = Some(100);
        after.channels.push(Channel {
            id: 102,
            name: "CNN".to_string(),
            channel_group_id: Some(100),
            channel_number: Some(serde_json::json!(7.0)),
            ..Channel::default()
        });
        let (next, _) = run_context(&store, "lineup");
        let owned = load_owned(&next, &after, "lineup").expect("reconciles");
        let mut recovered = owned.recovered.clone();
        recovered.sort();
        assert_eq!(
            recovered,
            vec![
                (
                    "channel".to_string(),
                    102,
                    "lineup:file:news:cnn".to_string()
                ),
                (
                    "group".to_string(),
                    100,
                    "lineup:file:cat:group".to_string()
                ),
            ]
        );
        assert_eq!(
            owned.dropped,
            vec![("logo".to_string(), "lineup:logo:cnn".to_string())]
        );
        assert!(store.pending_intents("lineup").expect("query").is_empty());
        assert_eq!(owned.channels.get("lineup:file:news:cnn"), Some(&102));
        // The rerun's plan finds the adopted channel instead of posting a second one.
        let plan = plan_creates(
            &after,
            &owned.channels,
            &request(vec![cnn(Some(7.0))]),
            &PlanOptions::default(),
        )
        .expect("plans");
        assert!(plan.channels.is_empty(), "no duplicate: {plan:#?}");
        assert!(!store.is_owned("channel", 2).expect("query"));
    }

    #[tokio::test]
    async fn a_deleted_owned_channel_is_forgotten_and_created_again() {
        let store = crate::store::Store::in_memory().expect("store");
        let (context, run_id) = run_context(&store, "lineup");
        store
            .record_ownership("channel", 55, "lineup", "lineup:file:news:cnn", run_id)
            .expect("record");
        // 55 is not in the world: a user deleted it.
        let owned = load_owned(&context, &create_world(), "lineup").expect("loads");
        assert_eq!(
            owned.forgotten,
            vec![(
                "channel".to_string(),
                55,
                "lineup:file:news:cnn".to_string()
            )]
        );
        assert!(owned.channels.is_empty());
        assert!(!store.is_owned("channel", 55).expect("query"));
        // A stale id handed straight to the plan does not block the create either.
        let mut stale = HashMap::new();
        stale.insert("lineup:file:news:cnn".to_string(), 55);
        let plan = plan_creates(
            &create_world(),
            &stale,
            &request(vec![cnn(Some(7.0))]),
            &PlanOptions::default(),
        )
        .expect("plans");
        assert_eq!(plan.channels.len(), 1);
        // A HIDDEN owned channel (undo hides) is still in the world and stays owned.
        let mut world = create_world();
        world.channels[0].hidden_from_output = true;
        store
            .record_ownership("channel", 1, "lineup", "lineup:k:one", run_id)
            .expect("record");
        let owned = load_owned(&context, &world, "lineup").expect("loads");
        assert_eq!(owned.channels.get("lineup:k:one"), Some(&1));
    }

    #[tokio::test]
    async fn an_apply_settles_its_intents_into_ownership() {
        let store = crate::store::Store::in_memory().expect("store");
        let (context, _) = run_context(&store, "lineup");
        let plan = plan_creates(
            &create_world(),
            &HashMap::new(),
            &request(vec![cnn(Some(7.0))]),
            &PlanOptions::default(),
        )
        .expect("plans");
        let creator: std::sync::Arc<dyn ChannelCreator> =
            std::sync::Arc::new(FakeCreator::new(100));
        let report = apply_creates(creator, &plan, &context, "lineup.create_apply").await;
        assert!(report.all_ok(), "{report:#?}");
        assert!(store.pending_intents("lineup").expect("query").is_empty());
        assert_eq!(store.owned_keys("lineup").expect("query").len(), 3);
    }

    #[test]
    fn a_create_plan_needs_the_slices_it_dedups_against() {
        let mut world = create_world();
        world.loaded = crate::dispatcharr::WorldKinds::ALL.without(WorldKind::Logos);
        let error = plan_creates(
            &world,
            &HashMap::new(),
            &request(vec![cnn(None)]),
            &PlanOptions::default(),
        )
        .expect_err("logos were not loaded");
        assert!(
            format!("{error:#}").to_lowercase().contains("logo"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn the_rest_creator_posts_one_object_per_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/channels/groups/"))
            .and(body_json(serde_json::json!({"name": "News"})))
            .respond_with(
                ResponseTemplate::new(201)
                    .set_body_json(serde_json::json!({"id": 21, "name": "News"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/channels/logos/"))
            .and(body_json(
                serde_json::json!({"name": "CNN", "url": "http://logos/cnn.png"}),
            ))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "id": 71,
                "name": "CNN",
                "url": "http://logos/cnn.png",
            })))
            .mount(&server)
            .await;
        // `channel_profile_ids` keeps Dispatcharr's three-way meaning: omitted is all
        // profiles, `[]` is none, `[0]` the explicit all-profiles sentinel.
        Mock::given(method("POST"))
            .and(path("/api/channels/channels/"))
            .and(body_json(serde_json::json!({
                "name": "CNN",
                "channel_number": 7.0,
                "channel_group_id": 21,
                "tvg_id": "cnn.us",
                "epg_data_id": 44,
                "logo_id": 71,
                "streams": [11, 12],
                "channel_profile_ids": [1],
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "id": 102,
                "name": "CNN",
            })))
            .mount(&server)
            .await;

        let client = DispatcharrClient::new(&config(&server.uri())).expect("client");
        let creator = RestChannelCreator::new(client, 4);
        assert_eq!(creator.concurrency(), 4);
        let group = creator.create_group("News").await.expect("group");
        assert_eq!(group.id, 21);
        let logo = creator
            .create_logo("CNN", "http://logos/cnn.png")
            .await
            .expect("logo");
        assert_eq!(logo.id, 71);
        // A channel POST carries the resolved ids, the ordered streams and the profile
        // semantics verbatim.
        let channel = creator
            .create_channel(&ChannelCreate {
                name: "CNN".to_string(),
                channel_number: Some(7.0),
                channel_group_id: Some(21),
                tvg_id: Some("cnn.us".to_string()),
                tvc_guide_stationid: None,
                epg_data_id: Some(44),
                logo_id: Some(71),
                streams: Some(vec![11, 12]),
                channel_profile_ids: Some(vec![1]),
            })
            .await
            .expect("channel");
        assert_eq!(channel.id, 102);
    }

    #[test]
    fn ownership_is_idempotent_and_queried_by_app_and_key() {
        use crate::store::Subject;
        let store = crate::store::Store::in_memory().expect("store");
        assert!(!store.is_owned("channel", 102).expect("query"));
        store
            .record_ownership("channel", 102, "lineup", "lineup:f:news:cnn", 7)
            .expect("record");
        assert!(store.is_owned("channel", 102).expect("query"));
        // A repeat records nothing new: the first owner wins, and the loser is told.
        assert!(!store
            .record_ownership("channel", 102, "streams", "other", 8)
            .expect("repeat"));
        assert_eq!(
            store.owner_of("channel", 102).expect("query"),
            Some(("lineup".to_string(), "lineup:f:news:cnn".to_string(), 7)),
        );
        assert_eq!(
            store
                .owned_ids("lineup", "lineup:f:news:cnn")
                .expect("query"),
            vec![("channel".to_string(), 102)],
        );
        store
            .record_ownership("group", 100, "lineup", "lineup:g:news", 7)
            .expect("record");
        assert_eq!(
            store.owned_keys("lineup").expect("query"),
            vec![
                ("channel".to_string(), 102, "lineup:f:news:cnn".to_string()),
                ("group".to_string(), 100, "lineup:g:news".to_string()),
            ],
        );
        // The subject wire forms match the ownership kinds, so a create row and its
        // ownership entry name the object the same way.
        assert_eq!(Subject::Group.as_str(), CreatedKind::Group.label());
        assert_eq!(Subject::Logo.as_str(), CreatedKind::Logo.label());
        assert_eq!(Subject::Channel.as_str(), CreatedKind::Channel.label());
    }

    #[tokio::test]
    async fn cancel_mid_create_records_landed_rows() {
        let plan = plan_creates(
            &create_world(),
            &HashMap::new(),
            &request(vec![cnn(Some(7.0))]),
            &PlanOptions::default(),
        )
        .expect("plans");
        let context = JobContext::for_tests(crate::config::CrosstalkConfig::default());
        let creator: std::sync::Arc<dyn ChannelCreator> = std::sync::Arc::new(CancelOnGroup {
            flag: context.cancel_flag(),
            next: std::sync::Mutex::new(100),
        });
        let report = apply_creates(creator, &plan, &context, "lineup.create_apply").await;
        assert_eq!(report.groups[0].status, RowStatus::Ok, "the group landed");
        assert_eq!(report.groups[0].id, Some(100));
        assert_eq!(
            report.logos[0].status,
            RowStatus::Failed,
            "the logo stage never started"
        );
        assert_eq!(report.logos[0].reason, CANCELLED);
        assert_eq!(
            report.channels[0].status,
            RowStatus::Failed,
            "the channel lost its logo"
        );
        assert!(report.channels[0].reason.contains("could not be created"));
        // The landed row keeps its id; the rest say why they did not land.
        let rows = create_run_rows(&plan, Some(&report));
        assert_eq!(rows[0].status, "ok");
        assert_eq!(rows[0].after_value.as_deref(), Some("100"));
        assert_eq!(rows[1].status, "failed");
    }

    #[test]
    fn the_create_csv_lists_every_object_and_the_skips() {
        use crate::subapp::{SettingsSchema, SettingsValues};
        use crate::test_app::StubApp;

        let dir = tempfile::tempdir().expect("tempdir");
        let config = crate::config::CrosstalkConfig {
            config_dir: dir.path().to_path_buf(),
            ..crate::config::CrosstalkConfig::default()
        };
        let context = JobContext::for_tests(config);
        let app = StubApp::new();
        let values = SettingsValues::default();
        let schema = SettingsSchema::default();
        let mut plan = plan_creates(
            &create_world(),
            &HashMap::new(),
            &request(vec![cnn(Some(7.0))]),
            &PlanOptions::default(),
        )
        .expect("plans");
        plan.skipped.push(CreateSkip {
            kind: CreatedKind::Channel,
            name: "Dupe".to_string(),
            key: "k:dupe".to_string(),
            reason: "already created".to_string(),
        });
        let spec = ReportSpec::new(
            &app,
            &schema,
            &values,
            "create_preview",
            "Create Preview Report",
        );
        let path = write_create_report(&context, &spec, &plan, None).expect("writes");
        let text = std::fs::read_to_string(&path).expect("read");
        assert!(
            text.contains(
                "channel_id,channel_name,channel_number,group,field,before,after,status,reason"
            ),
            "{text}"
        );
        assert!(text.contains("CNN"), "{text}");
        assert!(text.contains("created"), "{text}");
        assert!(text.contains("planned"), "{text}");
        assert!(text.contains("Dupe"), "the skip is listed: {text}");
        assert!(text.contains("Creates planned: 3"), "{text}");
    }
}
