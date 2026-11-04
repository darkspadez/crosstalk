//! Plan a synthetic triage or restore run from stdin, for `scripts/triage_parity.py`.
//!
//! One JSON object per line, one line back per case. A `triage` case carries a small world —
//! channels (name, group), channel groups, and the STREAM rows the probe recorded per channel
//! — plus the `[triage]` switches and formats; the verdicts are derived here through the real
//! [`crosstalk_streams::probe::verdict_for`] and [`crosstalk_streams::probe::format_for`], so
//! the harness compares the plugin's predicates with the port rather than with a fixture's
//! idea of them. A `restore` case carries the same world, the same probe rows, and the
//! `triage_apply` run rows the restore reads.
//!
//! ```text
//! echo '{"kind":"triage","channels":[{"id":1,"name":"ESPN"}],"streams":[[1,[{"class":"dead"}]]]}' \
//!   | cargo run -p crosstalk-streams --example triage_io
//! ```

use crosstalk_core::dispatcharr::{Channel, ChannelGroup, World};
use crosstalk_core::store::{RunRow, Subject};
use crosstalk_streams::probe::classify::Class;
use crosstalk_streams::probe::{self, RecordedVerdict, StreamVerdict};
use crosstalk_streams::settings::StreamsSettings;
use crosstalk_streams::{restore, triage};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Clone, Deserialize)]
struct ChannelInput {
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    group: Option<i64>,
    /// The name the probe recorded for this channel, when it differs from the live one (the
    /// plugin's results row keeps the name the check saw).
    #[serde(default)]
    probe_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GroupInput {
    id: i64,
    #[serde(default)]
    name: String,
}

/// One probe row, in the plugin's own vocabulary (`status`, `error_type`, `framerate_num`,
/// `format`), which is what `check_stream` records.
#[derive(Debug, Clone, Deserialize)]
struct StreamInput {
    #[serde(default = "alive")]
    class: String,
    #[serde(default)]
    error_type: Option<String>,
    #[serde(default)]
    framerate: f64,
    #[serde(default)]
    format: Option<String>,
}

fn alive() -> String {
    "alive".to_string()
}

/// One `triage_apply` run row, as the store carries it.
#[derive(Debug, Clone, Deserialize)]
struct RowInput {
    channel_id: i64,
    #[serde(default)]
    field: String,
    #[serde(default)]
    before: Option<String>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default = "ok")]
    status: String,
}

fn ok() -> String {
    "ok".to_string()
}

/// The settings of one case, in `streams.toml`'s own names.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct SettingsInput {
    rename_dead_channels: bool,
    rename_blank_channels: bool,
    rename_low_framerate_channels: bool,
    add_video_format_suffix: bool,
    move_dead_channels: bool,
    move_blank_channels: bool,
    move_low_framerate_channels: bool,
    dead_rename_format: Option<String>,
    black_screen_rename_format: Option<String>,
    low_framerate_rename_format: Option<String>,
    move_to_group_name: Option<String>,
    move_black_screen_group: Option<String>,
    move_low_framerate_group: Option<String>,
    video_format_suffixes: Option<String>,
}

impl SettingsInput {
    fn resolve(&self) -> StreamsSettings {
        let defaults = StreamsSettings::default();
        StreamsSettings {
            rename_dead_channels: self.rename_dead_channels,
            rename_blank_channels: self.rename_blank_channels,
            rename_low_framerate_channels: self.rename_low_framerate_channels,
            add_video_format_suffix: self.add_video_format_suffix,
            move_dead_channels: self.move_dead_channels,
            move_blank_channels: self.move_blank_channels,
            move_low_framerate_channels: self.move_low_framerate_channels,
            dead_rename_format: self
                .dead_rename_format
                .clone()
                .unwrap_or(defaults.dead_rename_format),
            black_screen_rename_format: self
                .black_screen_rename_format
                .clone()
                .unwrap_or(defaults.black_screen_rename_format),
            low_framerate_rename_format: self
                .low_framerate_rename_format
                .clone()
                .unwrap_or(defaults.low_framerate_rename_format),
            move_to_group_name: self
                .move_to_group_name
                .clone()
                .unwrap_or(defaults.move_to_group_name),
            move_black_screen_group: self
                .move_black_screen_group
                .clone()
                .unwrap_or(defaults.move_black_screen_group),
            move_low_framerate_group: self
                .move_low_framerate_group
                .clone()
                .unwrap_or(defaults.move_low_framerate_group),
            video_format_suffixes: self
                .video_format_suffixes
                .clone()
                .unwrap_or(defaults.video_format_suffixes),
            ..defaults
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Case {
    #[serde(default = "kind")]
    kind: String,
    #[serde(default)]
    channels: Vec<ChannelInput>,
    #[serde(default)]
    groups: Vec<GroupInput>,
    /// `[channel_id, [stream row, …]]` pairs: the probe's own rows, per channel.
    #[serde(default)]
    streams: Vec<(i64, Vec<StreamInput>)>,
    /// The `triage_apply` rows a restore reads, in run order.
    #[serde(default)]
    history: Vec<RowInput>,
    #[serde(default)]
    settings: SettingsInput,
}

fn kind() -> String {
    "triage".to_string()
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
                    channel_number: Some(json!(channel.id)),
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
            ..World::default()
        }
    }

    /// The verdicts the port derives from the probe's own rows.
    fn verdicts(&self) -> Vec<RecordedVerdict> {
        let mut verdicts = Vec::new();
        for (channel_id, rows) in &self.streams {
            if rows.is_empty() {
                continue;
            }
            let parsed: Vec<StreamVerdict> = rows
                .iter()
                .enumerate()
                .map(|(index, row)| {
                    StreamVerdict::from_parts(
                        index as i64,
                        class_of(&row.class),
                        row.error_type.as_deref().unwrap_or(""),
                        row.framerate,
                        format_tier(row.format.as_deref()),
                    )
                })
                .collect();
            let judged: Vec<&StreamVerdict> = parsed.iter().collect();
            let channel = self
                .channels
                .iter()
                .find(|channel| channel.id == *channel_id);
            verdicts.push(RecordedVerdict {
                channel_id: *channel_id,
                channel_name: channel
                    .and_then(|channel| channel.probe_name.clone())
                    .or_else(|| channel.map(|channel| channel.name.clone()))
                    .unwrap_or_default(),
                channel_group: String::new(),
                verdict: probe::verdict_for(&judged),
                format: probe::format_for(&judged).map(str::to_string),
            });
        }
        verdicts.sort_by_key(|verdict| verdict.channel_id);
        verdicts
    }

    /// The stored rows a restore reads.
    fn history(&self) -> Vec<RunRow> {
        self.history
            .iter()
            .enumerate()
            .map(|(ordinal, row)| RunRow {
                ordinal: ordinal as i64,
                subject: Subject::Channel,
                channel_id: row.channel_id,
                field: row.field.clone(),
                before_value: row.before.clone(),
                after_value: row.after.clone(),
                status: row.status.clone(),
                ..RunRow::default()
            })
            .collect()
    }
}

/// The plugin's `status` vocabulary to [`Class`].
fn class_of(class: &str) -> Class {
    match class.to_ascii_lowercase().as_str() {
        "dead" | "blank" | "frozen" | "silent" | "placeholder" => Class::Dead,
        "skipped" => Class::Skipped,
        _ => Class::Alive,
    }
}

/// A format tier, as the probe records it, mapped to the `&'static str` it stores.
fn format_tier(format: Option<&str>) -> &'static str {
    match format
        .unwrap_or("Unknown")
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "UHD" => "UHD",
        "FHD" => "FHD",
        "HD" => "HD",
        "SD" => "SD",
        _ => "Unknown",
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut line = String::new();
    while {
        line.clear();
        std::io::BufRead::read_line(&mut reader, &mut line)? > 0
    } {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let case: Case = serde_json::from_str(trimmed)?;
        let answer = run_case(&case);
        println!("{}", serde_json::to_string(&answer)?);
    }
    Ok(())
}

fn run_case(case: &Case) -> Value {
    let world = case.world();
    let settings = case.settings.resolve();
    let verdicts = case.verdicts();
    match case.kind.as_str() {
        "restore" => {
            let outcome = restore::plan(&world, &settings, &verdicts, &case.history());
            json!({
                "renames": outcome
                    .renames
                    .iter()
                    .map(|rename| json!([rename.channel_id, rename.to, rename.source.as_str()]))
                    .collect::<Vec<Value>>(),
                "moves": outcome
                    .moves
                    .iter()
                    .map(|movement| json!([movement.channel_id, movement.from, movement.to]))
                    .collect::<Vec<Value>>(),
                "skipped": outcome
                    .skipped
                    .iter()
                    .map(|skip| json!([skip.channel_id, skip.reason]))
                    .collect::<Vec<Value>>(),
                "missing_groups": outcome.missing_groups,
                "recovered": outcome.recovered,
            })
        }
        _ => match triage::plan(&world, &settings, &verdicts) {
            Ok(outcome) => json!({
                "error": Value::Null,
                "renames": outcome
                    .renames
                    .iter()
                    .map(|rename| json!([rename.channel_id, rename.to]))
                    .collect::<Vec<Value>>(),
                "suffixes": outcome
                    .suffixes
                    .iter()
                    .map(|suffix| json!([suffix.channel_id, suffix.to]))
                    .collect::<Vec<Value>>(),
                "moves": outcome
                    .moves
                    .iter()
                    .map(|movement| json!([movement.channel_id, movement.group]))
                    .collect::<Vec<Value>>(),
                "skipped": outcome
                    .skipped
                    .iter()
                    .map(|skip| json!([skip.channel_id, skip.reason]))
                    .collect::<Vec<Value>>(),
            }),
            Err(error) => json!({"error": format!("{error}")}),
        },
    }
}
