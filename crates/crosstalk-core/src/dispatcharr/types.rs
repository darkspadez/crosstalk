//! The shapes Dispatcharr's REST API reads and writes.
//!
//! Everything here is a projection of a DRF serializer, not of a Django model: only the
//! fields the sub-apps reason about are named, and every one of them tolerates absence
//! (`#[serde(default)]`). That tolerance is deliberate. Dispatcharr ships most days, a build
//! older or newer than the one this was written against must not turn a missing column into a
//! parse error, and a field the client does not need is never worth failing a run over.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// A channel.
///
/// `streams` is the channel's ordered stream ids: the list serializer fills it from the
/// `channelstream_set` prefetch either way (`get_stream_ids`), so the channel list carries it
/// without asking for `include_streams=true`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Channel {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub channel_number: Option<serde_json::Value>,
    #[serde(default)]
    pub channel_group_id: Option<i64>,
    /// The EPG entry assigned on the channel's own column.
    #[serde(default)]
    pub epg_data_id: Option<i64>,
    /// An override can pin a different EPG without changing the column. Newer builds only.
    #[serde(default)]
    pub effective_epg_data_id: Option<i64>,
    /// `Channel.tvg_id` is nullable upstream, and a single null would otherwise fail the
    /// whole listing.
    #[serde(default, deserialize_with = "null_to_empty")]
    pub tvg_id: String,
    #[serde(default)]
    pub logo_id: Option<i64>,
    /// Hidden channels are still listed by the client (`visibility_filter=all`); this is how
    /// the visibility actions tell them apart.
    #[serde(default)]
    pub hidden_from_output: bool,
    /// The channel's streams, in playback order.
    ///
    /// Defaulted here because write answers and test fixtures carry partial channels, but
    /// REQUIRED in the channel listing (`DispatcharrClient::channels`): stream-list writes
    /// REPLACE the list, so a listing that left the key out must fail rather than read as "no
    /// streams" (tidy would hide every channel, and a sort or match would write a list built
    /// on an empty one).
    #[serde(default)]
    pub streams: Vec<i64>,
    /// The channel this one is attached to, when the build has the column.
    ///
    /// Dispatcharr's own `Channel` model has no such field (checked against
    /// `apps/channels/models.py`), and its serializer therefore never sends the key — so this
    /// stays `None` on a stock install. Stream-Mapparr reads it defensively
    /// (`_get_all_channels` adds it only when `Channel._meta.get_field('attached_channel')`
    /// exists) for one rule: an attached channel is never made visible by
    /// `manage_channel_visibility`, because it is not the channel the guide plays. The field
    /// is named here so a build that does carry it gets the plugin's rule instead of silently
    /// enabling both halves of an attached pair; `None` is the honest answer for a build that
    /// does not.
    #[serde(default)]
    pub attached_channel_id: Option<i64>,
}

impl Channel {
    /// The EPG entry this channel is on, by its own column.
    ///
    /// The raw column is what the plugin compares against and what `batch-set-epg` writes, so
    /// it is what a run must reason about. Reporting the override here would make a preview
    /// claim a channel is on one guide while the write changes another.
    pub fn current_epg_id(&self) -> Option<i64> {
        self.epg_data_id
    }

    /// Whether an override is pinning a different EPG. Such a channel is skipped and
    /// reported, because writing its column would change nothing the operator can see.
    pub fn has_epg_override(&self) -> bool {
        match (self.epg_data_id, self.effective_epg_data_id) {
            (Some(raw), Some(effective)) => raw != effective,
            (None, Some(_)) => true,
            _ => false,
        }
    }

    /// The channel number as the reports print it.
    pub fn display_number(&self) -> Option<f64> {
        match self.channel_number.as_ref()? {
            serde_json::Value::Number(number) => number.as_f64(),
            serde_json::Value::String(text) => text.parse().ok(),
            _ => None,
        }
    }
}

/// A channel group.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ChannelGroup {
    pub id: i64,
    #[serde(default)]
    pub name: String,
}

/// A channel profile.
///
/// `channels` is the list of channel ids ENABLED in the profile, which Dispatcharr's profile
/// serializer computes server-side (`ChannelProfileSerializer.get_channels`). That makes the
/// profile scope filter a single request rather than one per channel.
///
/// `channels` is REQUIRED: a profile without the key would read as "enables nothing", which
/// `clear_hidden` turns into "every channel is disabled" and the profile scope into "no
/// channel is in scope".
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ChannelProfile {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    pub channels: Vec<i64>,
}

/// One EPG source.
///
/// `source_type` separates the three kinds Dispatcharr knows (`xmltv`,
/// `schedules_direct` and `dummy`); a dummy source has no upstream guide to refresh, so
/// everything that refreshes, times or horizons a source has to branch on it.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct EpgSource {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_true")]
    pub is_active: bool,
    /// Higher wins when two candidates score the same.
    #[serde(default)]
    pub priority: i64,
    /// `xmltv`, `schedules_direct` or `dummy`. Empty when a build does not say.
    #[serde(default)]
    pub source_type: String,
    /// `idle`, `fetching`, `parsing`, `success`, `error` or `disabled`.
    #[serde(default)]
    pub status: String,
    /// The last status message: a parse result or the error that stopped the refresh.
    #[serde(default)]
    pub last_message: Option<String>,
    /// When the source was last *successfully* refreshed; null until the first one.
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    /// Source-specific configuration. For a dummy source this holds the template settings
    /// (`title_pattern`, `time_pattern`, `title_template`, `program_duration`, `timezone`, …);
    /// the client passes it through opaquely because the keys belong to the events phase.
    #[serde(default)]
    pub custom_properties: Value,
    /// Minutes between scheduled refreshes, `0` for manual only.
    #[serde(default)]
    pub refresh_interval: i64,
    /// Whether any channel maps guide data from this source, which the list annotates
    /// server-side. The watchdog needs it: a source nothing watches has no horizon worth
    /// checking, and this answers that without loading every EPG entry.
    #[serde(default)]
    pub has_channels: bool,
}

impl EpgSource {
    /// A custom dummy source: its programmes are generated, not fetched.
    pub fn is_dummy(&self) -> bool {
        self.source_type == "dummy"
    }

    /// A source whose last refresh failed.
    pub fn is_broken(&self) -> bool {
        self.status == "error"
    }

    /// A source in the middle of a refresh, so its status is not settled yet.
    pub fn is_refreshing(&self) -> bool {
        matches!(self.status.as_str(), "fetching" | "parsing")
    }
}

/// An EPG entry: a channel inside a guide source.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct EpgEntry {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    /// `EPGData.tvg_id` is nullable upstream, and a single null would otherwise fail the
    /// whole listing.
    #[serde(default, deserialize_with = "null_to_empty")]
    pub tvg_id: String,
    #[serde(default)]
    pub epg_source: Option<i64>,
    /// The guide entry's icon (`EPGData.icon_url`, what Channel-Mapparr reads as
    /// `epg_data__icon_url` for its EPG logo fallback). Builds that omit the key
    /// deserialize to `None`, and the tier is skipped for those entries.
    #[serde(default)]
    pub icon_url: Option<String>,
}

/// One provider stream.
///
/// The fields the streams phase reasons about are `name` (what the matcher scores),
/// `tvg_id` (the guide id it can inherit), `m3u_account` and `channel_group` (where it came
/// from), `stream_id` (the provider's own id, which survives a credential change),
/// `stream_stats` (what a probe wrote) and `url` (what a probe fetches).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Stream {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    /// The provider's own stream id (`Stream.stream_id`, an Xtream `stream_id` or a provider
    /// row id). Stable across a credential change, which is what makes it useful for mirror
    /// fills: the same stream on two accounts shares it.
    #[serde(default)]
    pub stream_id: Option<i64>,
    /// A hand-made stream rather than one an M3U account imported. Its name, url and guide id
    /// stay editable, and match/restore must not drop it.
    #[serde(default)]
    pub is_custom: bool,
    /// Nullable upstream.
    #[serde(default, deserialize_with = "null_to_empty")]
    pub tvg_id: String,
    #[serde(default)]
    pub m3u_account: Option<i64>,
    #[serde(default)]
    pub channel_group: Option<i64>,
    /// The URL a player (and a probe) fetches. `StreamSerializer` only sends it to an
    /// administrator and omits the key for everyone else, so this is `None` unless the run's
    /// key is an admin's — which a probe needs.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub logo_url: Option<String>,
    /// The provider's own channel number, when the playlist carried one.
    #[serde(default)]
    pub stream_chno: Option<serde_json::Value>,
    /// Probe output (ffprobe) or whatever wrote it. Opaque: the shape is the probe's, not
    /// Dispatcharr's, so the client neither interprets nor rewrites it.
    #[serde(default)]
    pub stream_stats: Value,
    #[serde(default)]
    pub stream_stats_updated_at: Option<DateTime<Utc>>,
    /// The provider no longer lists this stream.
    #[serde(default)]
    pub is_stale: bool,
    #[serde(default)]
    pub is_adult: bool,
    #[serde(default)]
    pub is_radio: bool,
}

/// One M3U account: where streams come from.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct M3uAccount {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default = "default_true")]
    pub is_active: bool,
    /// `STD` for a playlist URL or an uploaded file, `XC` for Xtream Codes.
    #[serde(default)]
    pub account_type: String,
    /// The system `custom` account is locked: its streams are hand-made, not provider ones.
    #[serde(default)]
    pub locked: bool,
    #[serde(default)]
    pub max_streams: Option<i64>,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub status: String,
    /// The provider's base URL, nullable upstream (an uploaded playlist has none).
    #[serde(default)]
    pub server_url: Option<String>,
    /// The `ServerGroup` this account belongs to, as its id: `M3UAccount.server_group` is a
    /// foreign key and the serializer sends the primary key. Accounts sharing one are mirrors
    /// of each other, which is what the mirror actions group by.
    #[serde(default)]
    pub server_group: Option<i64>,
}

/// One logo.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Logo {
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub url: String,
    /// How many channels carry this logo, as the list annotates it.
    #[serde(default)]
    pub channel_count: i64,
    #[serde(default)]
    pub is_used: bool,
}

/// An installed plugin, as the plugin list reports it.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PluginInfo {
    pub key: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub enabled: bool,
    /// The config row exists but the plugin's code is gone.
    #[serde(default)]
    pub missing: bool,
    /// What the plugin says it can run; the bridge is useless without its action.
    #[serde(default)]
    pub actions: Vec<PluginActionInfo>,
}

impl PluginInfo {
    /// Whether the plugin exposes an action with this id.
    pub fn has_action(&self, id: &str) -> bool {
        self.actions.iter().any(|action| action.id == id)
    }
}

/// One action a plugin declares.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PluginActionInfo {
    pub id: String,
    #[serde(default)]
    pub label: String,
}

/// A Celery job Dispatcharr queued for us, as the bulk endpoints report it.
///
/// There is no task-status route for these; the effect shows up in the resource the task
/// writes, so the caller re-reads that (the channel list, the source) rather than the task.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueuedTask {
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub message: String,
    /// How many streams the task was given, when the endpoint says.
    #[serde(default)]
    pub stream_count: Option<i64>,
}

/// A field in a write payload: unchanged unless set, and `Null` sends an explicit `null`.
///
/// `Option<T>` cannot express the difference on the wire, and Dispatcharr's bulk edit uses
/// exactly that difference: an absent `channel_group_id` leaves the group alone, while an
/// explicit `null` moves the channel to "No Group". `Set` keeps the two apart without
/// `Option<Option<T>>`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Set<T> {
    /// Omitted from the payload.
    #[default]
    Unchanged,
    /// Sent as the value.
    Value(T),
    /// Sent as `null`, which clears a nullable column.
    Null,
}

impl<T> Set<T> {
    /// Whether the field is left out of the payload. Used by `skip_serializing_if`.
    pub fn is_unchanged(&self) -> bool {
        matches!(self, Self::Unchanged)
    }

    /// Whether the field is sent at all.
    pub fn is_set(&self) -> bool {
        !self.is_unchanged()
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            Self::Value(value) => Some(value),
            _ => None,
        }
    }
}

impl<T: Serialize> Serialize for Set<T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            // Every field that holds a `Set` skips this variant, so reaching the serializer
            // means a payload type forgot the attribute. Erroring beats writing a `null` that
            // silently clears a column.
            Self::Unchanged => Err(serde::ser::Error::custom(
                "Set::Unchanged must be skipped with `skip_serializing_if`, never serialized",
            )),
            Self::Value(value) => value.serialize(serializer),
            Self::Null => serializer.serialize_none(),
        }
    }
}

/// One row of a `PATCH /channels/channels/edit/bulk/` payload.
///
/// The endpoint takes a bare JSON array of these and applies all of them in one transaction
/// through `ChannelSerializer(partial=True)`, so any writable channel field is legal here.
/// The struct names the ones the sub-apps set; add a field when a phase needs one.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ChannelEdit {
    pub id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Set::is_unchanged")]
    pub channel_number: Set<f64>,
    /// `Set::Value(group)` moves the channel. `Set::Null` would be a move to "No Group", which
    /// this API cannot express: `ChannelSerializer.channel_group_id` is a
    /// `PrimaryKeyRelatedField` without `allow_null`, so DRF rejects a null with "This field
    /// may not be null." and `edit/bulk` then rejects the row's whole chunk. Kept because the
    /// wire shape is right for a build (or a plugin) that does allow it; `channel_ops::apply`
    /// refuses such a row rather than sending it.
    #[serde(default, skip_serializing_if = "Set::is_unchanged")]
    pub channel_group_id: Set<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tvg_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tvc_guide_stationid: Option<String>,
    /// `Set::Null` unassigns the guide.
    #[serde(default, skip_serializing_if = "Set::is_unchanged")]
    pub epg_data_id: Set<i64>,
    #[serde(default, skip_serializing_if = "Set::is_unchanged")]
    pub logo_id: Set<i64>,
    #[serde(default, skip_serializing_if = "Set::is_unchanged")]
    pub stream_profile_id: Set<i64>,
    /// `true` hides the channel from output; the bulk edit handles the compact-numbering
    /// transitions either way.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hidden_from_output: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_adult: Option<bool>,
    /// Replaces the channel's whole stream list, the array index being the playback order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streams: Option<Vec<i64>>,
}

impl ChannelEdit {
    /// An edit that changes nothing but the channel it names.
    pub fn new(id: i64) -> Self {
        Self {
            id,
            ..Self::default()
        }
    }

    /// A rename.
    pub fn rename(id: i64, name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::new(id)
        }
    }

    /// A move to another group (`None` = "No Group").
    ///
    /// See [`Self::channel_group_id`]: a `None` target sends the null the field is shaped for,
    /// which Dispatcharr's serializer rejects today.
    pub fn move_to_group(id: i64, group: Option<i64>) -> Self {
        Self {
            channel_group_id: group.map_or(Set::Null, Set::Value),
            ..Self::new(id)
        }
    }

    /// A hide/show.
    pub fn visible(id: i64, visible: bool) -> Self {
        Self {
            hidden_from_output: Some(!visible),
            ..Self::new(id)
        }
    }

    /// An ordered stream list.
    pub fn with_streams(id: i64, streams: impl IntoIterator<Item = i64>) -> Self {
        Self {
            streams: Some(streams.into_iter().collect()),
            ..Self::new(id)
        }
    }
}

/// A `POST /channels/channels/` payload.
///
/// `channel_profile_ids` keeps Dispatcharr's own three-way meaning, and it is spelled out
/// because getting it wrong is silent: omitted adds the channel to every profile, `[]` adds
/// it to none, and `[0]` is the explicit "all profiles" sentinel.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ChannelCreate {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_number: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_group_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tvg_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tvc_guide_stationid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epg_data_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logo_id: Option<i64>,
    /// Ordered stream ids.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streams: Option<Vec<i64>>,
    /// Omitted = all profiles, `[]` = none, `[0]` = all (explicit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_profile_ids: Option<Vec<i64>>,
}

impl ChannelCreate {
    /// A channel with just a name; everything else takes Dispatcharr's defaults (a number is
    /// auto-assigned, the "Default Group" is created if the payload names no group).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }
}

/// A `POST /channels/channels/edit/bulk-regex/` payload.
///
/// The rewrite happens server-side, in Python's `regex` module: `find` is a Python pattern
/// (JavaScript `(?<name>…)` groups are translated), `replace` accepts JavaScript tokens
/// (`$1`, `$&`, `$<name>`) which the view translates, and the only flag understood is `i`
/// (ignore case). A rename the pattern does not match, or that would empty the name, is
/// skipped upstream rather than reported.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ChannelRegexRename {
    pub channel_ids: Vec<i64>,
    pub find: String,
    #[serde(default)]
    pub replace: String,
    /// `"i"` for case-insensitive; anything else is ignored upstream.
    #[serde(default)]
    pub flags: String,
}

impl ChannelRegexRename {
    /// A case-sensitive find/replace over the given channels.
    pub fn new(channel_ids: Vec<i64>, find: impl Into<String>, replace: impl Into<String>) -> Self {
        Self {
            channel_ids,
            find: find.into(),
            replace: replace.into(),
            flags: String::new(),
        }
    }

    /// Make the pattern case-insensitive.
    pub fn ignoring_case(mut self) -> Self {
        self.flags = "i".to_string();
        self
    }
}

/// A `POST /channels/channels/from-stream/bulk/` request.
///
/// `starting_channel_number` follows the endpoint's modes: `None` uses the provider numbers,
/// `Some(0)` the lowest free numbers, and `Some(n)` numbers from `n` upwards.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StreamsToChannels {
    pub stream_ids: Vec<i64>,
    /// Omitted = all profiles, `[]` = none, `[0]` = all (explicit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_profile_ids: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub starting_channel_number: Option<i64>,
}

/// A create or update payload for one EPG source.
///
/// Every field is optional and omitted when unset, so the same type serves `POST` (where
/// `name` and `source_type` are required, and checked by the client before the request) and
/// `PATCH` (where an absent field is left alone). `password` is write-only upstream: it is
/// accepted and never read back.
#[derive(Debug, Clone, Default, Serialize)]
pub struct EpgSourceEdit {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `xmltv`, `schedules_direct` or `dummy`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_active: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_interval: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_properties: Option<Value>,
}

impl EpgSourceEdit {
    /// A custom dummy source: generated programmes, no upstream to fetch.
    ///
    /// `custom_properties` carries the template keys the generator reads
    /// (`title_pattern`, `time_pattern`, `title_template`, `program_duration`, `timezone`,
    /// `category`, …). A dummy source with no `title_pattern` falls back to Dispatcharr's
    /// standard placeholder blocks, which is a valid thing to create.
    pub fn dummy(name: impl Into<String>, custom_properties: Value) -> Self {
        Self {
            name: Some(name.into()),
            source_type: Some("dummy".to_string()),
            custom_properties: Some(custom_properties),
            ..Self::default()
        }
    }
}

/// The fields a refresh changes, read with one `GET /epg/sources/{id}/`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct EpgSourceState {
    pub id: i64,
    /// `idle`, `fetching`, `parsing`, `success`, `error` or `disabled`.
    #[serde(default)]
    pub status: String,
    /// Set when the refresh last *succeeded*, so a failed attempt leaves it untouched.
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_message: Option<String>,
}

impl EpgSourceState {
    /// Whether anything a refresh writes has changed since `before`.
    ///
    /// Both fields are needed: a refresh that fails moves `status` to `error` without
    /// touching `updated_at`, and one that succeeds changes both.
    pub fn changed_since(&self, before: &Self) -> bool {
        self.status != before.status || self.updated_at != before.updated_at
    }
}

/// What a poll saw happen to a refresh.
#[derive(Debug, Clone)]
pub enum EpgRefresh {
    /// The source's status or last-refreshed time moved; carries the new state.
    Changed(EpgSourceState),
    /// The timeout lapsed with nothing changed: the Celery task may still be running.
    TimedOut(EpgSourceState),
}

impl EpgRefresh {
    /// The state the poll last read, changed or not.
    pub fn state(&self) -> &EpgSourceState {
        match self {
            Self::Changed(state) | Self::TimedOut(state) => state,
        }
    }
}

/// How far one source's guide reaches.
///
/// The variants exist because the cheapest route cannot tell "no programmes" from "no
/// programmes in the bracket", and the watchdog must not read the second as the first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpgHorizon {
    /// No programme on the source ends at or after the bracket's start.
    Empty,
    /// The latest programme end time, to the minute and rounded down.
    At(DateTime<Utc>),
    /// Programmes still end at or after the bracket's end; the search cap hid the horizon.
    Beyond(DateTime<Utc>),
}

/// How a programme-data check is answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HasPrograms {
    Yes,
    No,
}

/// The programme-data window: programmes overlapping `[start, end)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgramWindow {
    /// ISO 8601 instant, sent as `end_after` (`end_time >= start`).
    pub start: String,
    /// ISO 8601 instant, sent as `start_before` (`start_time <= end`).
    pub end: String,
}

impl ProgramWindow {
    /// A window from now to now plus `hours`.
    pub fn ahead(hours: u32) -> Self {
        let now = chrono::Utc::now();
        let end = now + chrono::Duration::hours(i64::from(hours));
        Self {
            start: now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            end: end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }
    }
}

/// One slice of Dispatcharr a [`World`] can carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorldKind {
    Channels,
    Groups,
    Profiles,
    EpgSources,
    EpgEntries,
    Streams,
    M3uAccounts,
    Logos,
}

impl WorldKind {
    const fn bit(self) -> u16 {
        match self {
            Self::Channels => 1 << 0,
            Self::Groups => 1 << 1,
            Self::Profiles => 1 << 2,
            Self::EpgSources => 1 << 3,
            Self::EpgEntries => 1 << 4,
            Self::Streams => 1 << 5,
            Self::M3uAccounts => 1 << 6,
            Self::Logos => 1 << 7,
        }
    }
}

/// A set of [`WorldKind`]s: which slices of Dispatcharr to load, and which one a [`World`]
/// carries.
///
/// Build one with `|` (`WorldKind::Channels | WorldKind::Groups`) or from the named sets
/// below. Loading per kind matters because the slices differ by orders of magnitude: an
/// install's channels and streams are tens of thousands of rows, EPG entries a few thousand,
/// and a lineup run that only renames channels does not need the guide at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorldKinds(u16);

impl WorldKinds {
    /// The five slices unit-tested and reasoned about together: what [`World`] always meant
    /// before it could be selected, and what `load_world()` still fetches.
    pub const DEFAULT: Self = Self(
        WorldKind::Channels.bit()
            | WorldKind::Groups.bit()
            | WorldKind::Profiles.bit()
            | WorldKind::EpgSources.bit()
            | WorldKind::EpgEntries.bit(),
    );

    /// Every slice, streams and logos included.
    pub const ALL: Self = Self(0xff);

    /// No slice at all: a world that reads nothing.
    pub const NONE: Self = Self(0);

    /// One slice on its own.
    pub fn only(kind: WorldKind) -> Self {
        Self(kind.bit())
    }

    /// Add a slice.
    pub fn with(self, kind: WorldKind) -> Self {
        Self(self.0 | kind.bit())
    }

    /// Without a slice.
    pub fn without(self, kind: WorldKind) -> Self {
        Self(self.0 & !kind.bit())
    }

    /// Whether the set asks for this slice.
    pub fn contains(self, kind: WorldKind) -> bool {
        self.0 & kind.bit() != 0
    }

    /// Whether the set asks for nothing.
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl std::ops::BitOr for WorldKind {
    type Output = WorldKinds;

    fn bitor(self, other: Self) -> WorldKinds {
        WorldKinds(self.bit() | other.bit())
    }
}

impl std::ops::BitOr<WorldKind> for WorldKinds {
    type Output = WorldKinds;

    fn bitor(self, other: WorldKind) -> WorldKinds {
        self.with(other)
    }
}

impl std::ops::BitOr for WorldKinds {
    type Output = WorldKinds;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl Default for WorldKinds {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Everything a run loads from Dispatcharr.
///
/// A slice that was not requested is empty, which is indistinguishable from "there are none"
/// unless you ask: [`World::has`] reports what was loaded, and [`World::require`] fails with
/// that in the message. Build one through [`crate::dispatcharr::DispatcharrClient::load_world_with`];
/// a hand-built world (tests, fixtures) claims every slice, because a caller filling the
/// fields by hand has them.
#[derive(Debug, Clone)]
pub struct World {
    pub channels: Vec<Channel>,
    pub groups: Vec<ChannelGroup>,
    pub profiles: Vec<ChannelProfile>,
    pub sources: Vec<EpgSource>,
    pub entries: Vec<EpgEntry>,
    pub streams: Vec<Stream>,
    pub m3u_accounts: Vec<M3uAccount>,
    pub logos: Vec<Logo>,
    /// Which slices this world actually asked for.
    pub loaded: WorldKinds,
}

impl Default for World {
    fn default() -> Self {
        Self {
            channels: Vec::new(),
            groups: Vec::new(),
            profiles: Vec::new(),
            sources: Vec::new(),
            entries: Vec::new(),
            streams: Vec::new(),
            m3u_accounts: Vec::new(),
            logos: Vec::new(),
            loaded: WorldKinds::ALL,
        }
    }
}

impl World {
    /// Whether this world carries a slice.
    pub fn has(&self, kind: WorldKind) -> bool {
        self.loaded.contains(kind)
    }

    /// Fail with the kind named when a world was loaded without it, rather than silently
    /// treating "not loaded" as "none".
    pub fn require(&self, kind: WorldKind) -> anyhow::Result<()> {
        if self.has(kind) {
            return Ok(());
        }
        anyhow::bail!(
            "this run loaded no {:?}; ask for it with WorldKinds/load_world_with",
            kind
        )
    }

    /// Source id -> source.
    pub fn source_map(&self) -> HashMap<i64, &EpgSource> {
        self.sources
            .iter()
            .map(|source| (source.id, source))
            .collect()
    }

    /// M3U account id -> account.
    pub fn m3u_account_map(&self) -> HashMap<i64, &M3uAccount> {
        self.m3u_accounts
            .iter()
            .map(|account| (account.id, account))
            .collect()
    }

    /// Stream id -> stream.
    pub fn stream_map(&self) -> HashMap<i64, &Stream> {
        self.streams
            .iter()
            .map(|stream| (stream.id, stream))
            .collect()
    }

    pub fn group_name(&self, id: Option<i64>) -> String {
        match id.and_then(|id| self.groups.iter().find(|group| group.id == id)) {
            Some(group) => group.name.clone(),
            None => "No Group".to_string(),
        }
    }

    /// A copy holding only these channels, for a workflow step whose scope override
    /// resolves to channel ids (see [`crate::jobs::JobContext::scoped_channels`]).
    ///
    /// Groups, streams, profiles, entries and accounts are untouched: matching still reads
    /// the pools, only the channel universe shrinks. Profile memberships are left as they
    /// are — every read of them is a per-channel test (enabled, visible, hidden), never a
    /// universe — so a step scoped to ids sees exactly those channels. A named scope
    /// REPLACES the host scope: pair this with an empty scope table, never with the host
    /// one (which would filter the ids back down to it).
    pub fn retaining_channels(&self, limit: &std::collections::HashSet<i64>) -> Self {
        let mut world = self.clone();
        world.channels.retain(|channel| limit.contains(&channel.id));
        world
    }
}

/// One programme airing right now on an EPG entry, as `/epg/current-programs/` answers it.
///
/// The endpoint is asked per EPG entry and answers a FLAT array, so each programme carries the
/// `epg_data_id` it belongs to. Two shapes arrive in the same array: a real programme (with a
/// title and a time span) and the endpoint's `{"epg_data_id": N, "parsing": true}` marker for
/// an entry whose guide index is still being built. A caller that treats the marker as "no
/// programme" would drop every placeholder channel on a busy instance.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CurrentProgram {
    /// The EPG entry this programme belongs to. Always present; the rest is not.
    #[serde(default)]
    pub epg_data_id: i64,
    #[serde(default, deserialize_with = "null_to_empty")]
    pub title: String,
    #[serde(default, deserialize_with = "null_to_empty")]
    pub sub_title: String,
    #[serde(default)]
    pub start_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub end_time: Option<DateTime<Utc>>,
    /// The guide id the programme was parsed under, when the source carried one.
    #[serde(default, deserialize_with = "null_to_empty")]
    pub tvg_id: String,
    /// The answer was "this entry's index is being rebuilt", not "nothing is airing".
    #[serde(default)]
    pub parsing: bool,
}

impl CurrentProgram {
    /// Whether this is the endpoint's "index rebuilding" marker rather than a programme.
    pub fn is_parsing(&self) -> bool {
        self.parsing
    }

    /// Whether it carries a title to match a placeholder name against.
    pub fn has_title(&self) -> bool {
        !self.parsing && !self.title.trim().is_empty()
    }
}

/// A programme row. The check only needs to know whether one exists, so the fields are not
/// read; `id` is named so a renamed column cannot turn a real row into a parse error.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ProgramRow {
    #[serde(default, rename = "id")]
    pub(crate) _id: Option<i64>,
}

/// A paginated programme search answer.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PaginatedPrograms {
    #[serde(default)]
    pub(crate) results: Vec<ProgramRow>,
}

fn default_true() -> bool {
    true
}

pub(crate) fn null_to_empty<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}
