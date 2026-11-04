//! Plan a synthetic visibility run from stdin and print the end state, for
//! `scripts/visibility_parity.py`.
//!
//! One JSON object per line, one line back per case. The case carries a whole tiny world — the
//! channels (numbers, groups, stream counts, `attached_channel_id`), the channel groups, the
//! profiles (with the channels ENABLED in each) and the channel-database entries the grouping
//! reads for a channel's country, plus the scope and the tag/country settings — because "which
//! channel of this name group stays visible" is a property of the channel set, not of one
//! channel.
//!
//! The harness drives Stream-Mapparr's real `manage_channel_visibility_action` over the same
//! case (with `ChannelProfileMembership` faked and the plugin's own `_load_channels_data`
//! reading the case's database), so this binary is the only Rust in the comparison.
//!
//! ```text
//! echo '{"channels":[{"id":1,"name":"ESPN","streams":[10]}],"profiles":[...]}' \
//!   | cargo run -p crosstalk-streams --example visibility_io
//! ```

use crosstalk_core::config::{MatchingConfig, ScopeConfig};
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, ChannelProfile, World};
use crosstalk_streams::matching::{ChannelDatabase, DatabaseEntry};
use crosstalk_streams::settings::StreamsSettings;
use crosstalk_streams::visibility;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Clone, Deserialize)]
struct ChannelInput {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    group: Option<i64>,
    #[serde(default)]
    number: Option<f64>,
    /// How many streams the channel carries: `ChannelStream` rows in the plugin's query, and
    /// `Channel.streams` here. The ids do not matter to this action, only the count.
    #[serde(default)]
    streams: Vec<i64>,
    /// `Channel.attached_channel_id`, when the build has the column.
    #[serde(default)]
    attached_channel_id: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct GroupInput {
    id: i64,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ProfileInput {
    id: i64,
    #[serde(default)]
    name: String,
    /// The channels ENABLED in the profile.
    #[serde(default)]
    channels: Vec<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct DatabaseInput {
    name: String,
    code: String,
    #[serde(default)]
    callsign: Option<String>,
}

/// The settings of one case, in `streams.toml`'s and `crosstalk.toml`'s own names.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct SettingsInput {
    ignore_tags: String,
    restrict_matching_to_country: bool,
    ignore_quality: bool,
    ignore_regional: bool,
    ignore_geographic: bool,
    ignore_misc: bool,
}

impl Default for SettingsInput {
    fn default() -> Self {
        Self {
            ignore_tags: String::new(),
            restrict_matching_to_country: false,
            ignore_quality: true,
            ignore_regional: true,
            ignore_geographic: true,
            ignore_misc: true,
        }
    }
}

/// One case: a world, a scope and the settings to group it with.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct Case {
    channels: Vec<ChannelInput>,
    groups: Vec<GroupInput>,
    profiles: Vec<ProfileInput>,
    /// The channel database the grouping resolves countries and callsigns from.
    channels_data: Vec<DatabaseInput>,
    settings: SettingsInput,
    /// `[scope] profiles` — a comma-separated list, which is the profile (or profiles)
    /// visibility reads.
    profile_name: String,
    /// `[scope] groups`.
    scope_groups: String,
    /// `[scope] ignore_groups`.
    ignore_groups: String,
}

impl Case {
    fn world(&self) -> World {
        World {
            channels: self
                .channels
                .iter()
                .map(|channel| Channel {
                    id: channel.id,
                    name: channel.name.clone(),
                    channel_group_id: channel.group,
                    channel_number: channel
                        .number
                        .map(|number| json!(number))
                        .or(Some(Value::Null)),
                    streams: channel.streams.clone(),
                    attached_channel_id: channel.attached_channel_id,
                    ..Channel::default()
                })
                .collect(),
            groups: self
                .groups
                .iter()
                .map(|group| ChannelGroup {
                    id: group.id,
                    name: group.name.clone(),
                })
                .collect(),
            profiles: self
                .profiles
                .iter()
                .map(|profile| ChannelProfile {
                    id: profile.id,
                    name: profile.name.clone(),
                    channels: profile.channels.clone(),
                })
                .collect(),
            ..World::default()
        }
    }

    fn scope(&self) -> ScopeConfig {
        ScopeConfig {
            profiles: self.profile_name.clone(),
            groups: self.scope_groups.clone(),
            ignore_groups: self.ignore_groups.clone(),
        }
    }

    fn settings(&self) -> StreamsSettings {
        StreamsSettings {
            ignore_tags: self.settings.ignore_tags.clone(),
            restrict_matching_to_country: self.settings.restrict_matching_to_country,
            ..StreamsSettings::default()
        }
    }

    fn matching(&self) -> MatchingConfig {
        MatchingConfig {
            ignore_quality_tags: self.settings.ignore_quality,
            ignore_regional_tags: self.settings.ignore_regional,
            ignore_geographic_tags: self.settings.ignore_geographic,
            ignore_misc_tags: self.settings.ignore_misc,
            ..MatchingConfig::default()
        }
    }

    fn database(&self) -> ChannelDatabase {
        ChannelDatabase::from_entries(self.channels_data.iter().map(|entry| DatabaseEntry {
            name: entry.name.clone(),
            code: entry.code.clone(),
            callsign: entry.callsign.clone(),
        }))
    }

    /// The settings with this case's database instead of the shipped ones: the harness supplies
    /// the entries, so the two sides group over the same data.
    fn options(&self) -> crosstalk_streams::matching::MatchOptions {
        crosstalk_streams::matching::MatchOptions::with_database(
            &self.settings(),
            &self.matching(),
            Some(self.database()),
        )
    }
}

fn main() -> anyhow::Result<()> {
    // A panic here would be a Rust bug, so let it fail loudly: the harness reports it.
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut line = String::new();
    while std::io::BufRead::read_line(&mut reader, &mut line)? > 0 {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let case: Case = serde_json::from_str(trimmed)?;
        let answer = run_case(&case)?;
        println!("{}", serde_json::to_string(&answer)?);
        line.clear();
    }
    Ok(())
}

fn run_case(case: &Case) -> anyhow::Result<Value> {
    let world = case.world();
    // The case's database and tag flags, in the options the plan groups with.
    let outcome = visibility::plan_with_options(&world, &case.scope(), &case.options())?;

    let mut end_state: Vec<Value> = outcome
        .end_state
        .iter()
        .map(|(channel_id, enabled)| json!([channel_id, enabled]))
        .collect();
    end_state.sort_by_key(|entry| entry.get(0).and_then(Value::as_i64).unwrap_or_default());
    Ok(json!({
        "profile_id": outcome.profile_id,
        "profile_name": outcome.profile_name,
        "channels": outcome.channels,
        "groups": outcome.groups,
        "winners": outcome.winners,
        "end_state": end_state,
        "enabled_before": outcome.enabled_before,
        "attached_skipped": outcome.attached_skipped,
        "groups_without_winner": outcome.groups_without_winner,
    }))
}
