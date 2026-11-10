//! Plan a synthetic mirror fill from stdin and print the plan, for `scripts/mirror_parity.py`.
//!
//! One JSON object per line, one line back per case. The case carries a whole tiny world — the
//! M3U accounts (with their server groups, activity and `server_url`s), the channels (with
//! their current ordered stream lists), the streams (with their account and provider stream id)
//! and the channel groups — because "is this channel covered on every active host" is a
//! property of the account table, not of one channel.
//!
//! The harness drives Stream-Mapparr's real `fill_mirror_backups_action` over the same case
//! (with the ORM tables faked the way the plugin's own tests fake them), so this binary is the
//! only Rust in the comparison.
//!
//! ```text
//! echo '{"accounts":[{"id":6,"name":"A","group":1,"url":"http://a.example"}],...}' \
//!   | cargo run -p crosstalk-streams --example mirror_io
//! ```

use crosstalk_core::config::ScopeConfig;
use crosstalk_core::dispatcharr::{Channel, ChannelGroup, M3uAccount, Stream, World};
use crosstalk_streams::mirror;
use serde::Deserialize;
use serde_json::{json, Value};

/// A channel of the fixture world.
#[derive(Debug, Clone, Deserialize)]
struct ChannelInput {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    group: Option<i64>,
    #[serde(default)]
    number: Option<f64>,
    /// The channel's current list, in playback order.
    #[serde(default)]
    streams: Vec<i64>,
}

/// An M3U account of the fixture world.
#[derive(Debug, Clone, Deserialize)]
struct AccountInput {
    id: i64,
    #[serde(default)]
    name: String,
    /// `server_group_id`.
    #[serde(default)]
    group: Option<i64>,
    #[serde(default = "default_true")]
    active: bool,
    /// `server_url`.
    #[serde(default)]
    url: Option<String>,
}

fn default_true() -> bool {
    true
}

/// A stream of the fixture world.
#[derive(Debug, Clone, Deserialize)]
struct StreamInput {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    account: Option<i64>,
    /// The provider's own stream id.
    #[serde(default)]
    provider_id: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct GroupInput {
    id: i64,
    #[serde(default)]
    name: String,
}

/// One case: a world and the scope to plan it over.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct Case {
    channels: Vec<ChannelInput>,
    accounts: Vec<AccountInput>,
    streams: Vec<StreamInput>,
    groups: Vec<GroupInput>,
    /// `[scope] profiles`. Empty means every channel, which the harness uses whenever the
    /// case does not test the profile filter (the plugin resolves that filter itself, through
    /// `_channels_in_scope`, which the harness stubs to the case's channel list).
    profiles: String,
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
            m3u_accounts: self
                .accounts
                .iter()
                .map(|account| M3uAccount {
                    id: account.id,
                    name: account.name.clone(),
                    is_active: account.active,
                    server_group: account.group,
                    server_url: account.url.clone(),
                    ..M3uAccount::default()
                })
                .collect(),
            streams: self
                .streams
                .iter()
                .map(|stream| Stream {
                    id: stream.id,
                    name: stream.name.clone(),
                    m3u_account: stream.account,
                    stream_id: stream.provider_id,
                    ..Stream::default()
                })
                .collect(),
            ..World::default()
        }
    }

    fn scope(&self) -> ScopeConfig {
        ScopeConfig {
            profiles: self.profiles.clone(),
            groups: self.scope_groups.clone(),
            ignore_groups: self.ignore_groups.clone(),
        }
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
    let outcome = mirror::plan(&world, &case.scope())?;

    let mut entries = Vec::new();
    for entry in &outcome.entries {
        entries.push(json!({
            "channel_id": entry.channel_id,
            "before": entry.before,
            "after": entry.after,
            "added": entry.added.iter().map(|add| json!({
                "stream_id": add.stream_id,
                "account_id": add.account_id,
                "source_stream_id": add.source_stream_id,
            })).collect::<Vec<_>>(),
        }));
    }
    Ok(json!({
        "channels": outcome.channels,
        "any_grouped_account": outcome.any_grouped_account,
        "entries": entries,
        "skipped": outcome.skipped.iter().map(|entry| json!({
            "channel_id": entry.channel_id,
            "before": entry.before,
            "after": entry.after,
            "added": entry.added.iter().map(|add| json!({
                "stream_id": add.stream_id,
                "account_id": add.account_id,
                "source_stream_id": add.source_stream_id,
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "stats": {
            "gained": outcome.stats.gained,
            "covered": outcome.stats.covered,
            "no_source": outcome.stats.no_source,
            "no_mirror_found": outcome.stats.no_mirror_found,
        },
        "missing_streams": outcome.missing_streams,
        "per_account": outcome.per_account().into_iter()
            .map(|(account_id, count)| json!([account_id, count]))
            .collect::<Vec<_>>(),
    }))
}
