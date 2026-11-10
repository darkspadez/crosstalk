//! Replay recorded ffprobe/ffmpeg output through the probe and print what it decided, for
//! `scripts/probe_parity.py`.
//!
//! One JSON object per line in, one line back per case. A case is one recorded probe: the URL,
//! the settings the probe ran with, and what the real tools printed. The harness drives IPTV
//! Checker's own `check_stream` over the same recording (with `subprocess.run` stubbed) and
//! compares the two answers, argument for argument and field for field.
//!
//! Two kinds of line:
//!
//! ```text
//! {"kind": "probe", "url": ..., "settings": {...}, "ffprobe": {...}, "ffmpeg": {...}|null}
//! {"kind": "aggregate", "channels": [{"id":1,"streams":[{"class":"dead","error_type":"Timeout",
//!                                                   "framerate":0.0,"format":"Unknown"}]}]}
//! ```
//!
//! The second is the per-channel verdict, which is a pure function of the stream verdicts: the
//! harness feeds both sides the same parts and compares the verdict and the format.

use crosstalk_streams::probe::classify::Class;
use crosstalk_streams::probe::tool::{FakeToolRunner, ToolOutput};
use crosstalk_streams::probe::{
    channel_verdict_name, probe_stream, ProbeConfig, ProbeTarget, StreamVerdict, Throttle,
};
use crosstalk_streams::settings::StreamsSettings;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// One recorded tool output, in `subprocess.CompletedProcess`'s shape.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct Recorded {
    returncode: Option<i32>,
    stdout: String,
    stderr: String,
    /// The plugin's `subprocess.TimeoutExpired`.
    timed_out: bool,
}

impl Recorded {
    fn output(&self) -> ToolOutput {
        ToolOutput {
            status: self.returncode,
            stdout: self.stdout.clone(),
            stderr: self.stderr.clone(),
            timed_out: self.timed_out,
            cancelled: false,
        }
    }
}

/// One case's settings, in `streams.toml`'s names (which are also the plugin's).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct SettingsInput {
    timeout: Option<u32>,
    probe_timeout: Option<u32>,
    dead_connection_retries: Option<u32>,
    streamlink_hosts: Option<String>,
    ffprobe_path: Option<String>,
    ffmpeg_path: Option<String>,
    black_screen_detection: Option<bool>,
    black_screen_sample_seconds: Option<u32>,
    black_screen_min_black_seconds: Option<u32>,
    black_screen_ffmpeg_timeout: Option<u32>,
    frozen_video_detection: Option<bool>,
    frozen_video_min_seconds: Option<u32>,
    silent_audio_detection: Option<bool>,
    silent_audio_max_db: Option<i32>,
    placeholder_file_detection: Option<bool>,
}

impl SettingsInput {
    fn settings(&self) -> StreamsSettings {
        // A probe fixture never measures throughput: that is the action's own window, not the
        // classification path this compares.
        let mut settings = StreamsSettings {
            enable_throughput_sorting: false,
            ..StreamsSettings::default()
        };
        if let Some(value) = self.timeout {
            settings.timeout = value;
        }
        if let Some(value) = self.probe_timeout {
            settings.probe_timeout = value;
        }
        if let Some(value) = self.dead_connection_retries {
            settings.dead_connection_retries = value;
        }
        // The plugin's own default is the shipped list; an empty string there means "no list",
        // which the plugin cannot express, so only an explicit list is passed through.
        settings.streamlink_hosts = self.streamlink_hosts.clone().unwrap_or_default();
        if let Some(value) = &self.ffprobe_path {
            settings.ffprobe_path = value.clone();
        }
        if let Some(value) = &self.ffmpeg_path {
            settings.ffmpeg_path = value.clone();
        }
        if let Some(value) = self.black_screen_detection {
            settings.black_screen_detection = value;
        }
        if let Some(value) = self.black_screen_sample_seconds {
            settings.black_screen_sample_seconds = value;
        }
        if let Some(value) = self.black_screen_min_black_seconds {
            settings.black_screen_min_black_seconds = value;
        }
        if let Some(value) = self.black_screen_ffmpeg_timeout {
            settings.black_screen_ffmpeg_timeout = value;
        }
        if let Some(value) = self.frozen_video_detection {
            settings.frozen_video_detection = value;
        }
        if let Some(value) = self.frozen_video_min_seconds {
            settings.frozen_video_min_seconds = value;
        }
        if let Some(value) = self.silent_audio_detection {
            settings.silent_audio_detection = value;
        }
        if let Some(value) = self.silent_audio_max_db {
            settings.silent_audio_max_db = value;
        }
        if let Some(value) = self.placeholder_file_detection {
            settings.placeholder_file_detection = value;
        }
        settings
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct ProbeCase {
    name: String,
    url: String,
    settings: SettingsInput,
    ffprobe: Recorded,
    ffmpeg: Option<Recorded>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct StreamPart {
    class: String,
    #[serde(default)]
    error_type: String,
    #[serde(default)]
    framerate: f64,
    #[serde(default)]
    format: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct ChannelInput {
    id: i64,
    streams: Vec<StreamPart>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Line {
    // Boxed: a recorded case carries two tool outputs, and the aggregate variant is a few
    // vectors, so the two arms would otherwise differ in size by an order of magnitude.
    Probe(Box<ProbeCase>),
    Aggregate { channels: Vec<ChannelInput> },
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut line = String::new();
    while std::io::BufRead::read_line(&mut reader, &mut line)? > 0 {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let case: Line = serde_json::from_str(trimmed)?;
        let answer = match case {
            Line::Probe(case) => run_probe(&case)?,
            Line::Aggregate { channels } => run_aggregate(&channels),
        };
        println!("{}", serde_json::to_string(&answer)?);
        line.clear();
    }
    Ok(())
}

fn run_probe(case: &ProbeCase) -> anyhow::Result<Value> {
    let settings = case.settings.settings();
    let config = ProbeConfig::from_settings(&settings);

    let mut tools = FakeToolRunner::new().answering("ffprobe", &case.url, case.ffprobe.output());
    if let Some(ffmpeg) = &case.ffmpeg {
        tools = tools.answering("ffmpeg", &case.url, ffmpeg.output());
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let verdict: StreamVerdict = runtime.block_on(async {
        probe_stream(
            &tools,
            &config,
            &Throttle::new(),
            &Arc::new(AtomicBool::new(false)),
            &ProbeTarget {
                stream_id: 1,
                stream_name: &case.name,
                account: 1,
                url: &case.url,
            },
        )
        .await
        .expect("the fake runner always answers")
    });

    // The command lines the probe actually built, which is the first thing the harness compares:
    // two ports that agree on the answer but run different flags are one provider ban apart.
    let calls = tools.calls();
    let argv = json!({
        "ffprobe": calls
            .iter()
            .find(|spec| spec.program_name() == "ffprobe")
            .map(|spec| std::iter::once(spec.program.clone()).chain(spec.args.clone()).collect::<Vec<String>>()),
        "ffmpeg": calls
            .iter()
            .find(|spec| spec.program_name() == "ffmpeg")
            .map(|spec| std::iter::once(spec.program.clone()).chain(spec.args.clone()).collect::<Vec<String>>()),
    });

    let error = if verdict.error.is_empty() {
        Value::Null
    } else {
        Value::String(verdict.error.clone())
    };
    Ok(json!({
        "name": case.name,
        "argv": argv,
        "status": verdict.class.label(),
        "health_class": verdict.health_class,
        "error_type": verdict.error_type,
        "format": verdict.format,
        "framerate_num": verdict.framerate,
        "error": error,
        "retry_count": verdict.retries,
        "metadata": metadata_of(&verdict),
        "extra": extra_of(&verdict),
    }))
}

/// The plugin's `dispatcharr_metadata`: twelve keys with nulls on Alive, all nulls on Dead, and
/// `{}` on Skipped (where the plugin writes nothing at all).
fn metadata_of(verdict: &StreamVerdict) -> Value {
    match (verdict.class, verdict.facts.as_ref()) {
        (Class::Alive, Some(facts)) => Value::Object(facts.dispatcharr_metadata()),
        (Class::Skipped, _) => Value::Object(Map::new()),
        (Class::Dead, _) => {
            let nulls: Map<String, Value> = [
                "video_codec",
                "resolution",
                "width",
                "height",
                "source_fps",
                "pixel_format",
                "video_bitrate",
                "audio_codec",
                "sample_rate",
                "audio_channels",
                "audio_bitrate",
                "stream_type",
            ]
            .into_iter()
            .map(|key| (key.to_string(), Value::Null))
            .collect();
            Value::Object(nulls)
        }
        (Class::Alive, None) => Value::Object(Map::new()),
    }
}

/// The plugin's `ffprobe_data`: the evidence block, with only the keys it measured.
fn extra_of(verdict: &StreamVerdict) -> Value {
    let extra = &verdict.extra;
    let mut map = Map::new();
    if let Some(value) = extra.frame_count {
        map.insert("frame_count".to_string(), json!(value));
    }
    if let Some(value) = &extra.first_frame_pts {
        map.insert("first_frame_pts".to_string(), value.clone());
    }
    if let Some(value) = extra.packet_count {
        map.insert("packet_count".to_string(), json!(value));
    }
    if let Some(value) = extra.calculated_bitrate_kbps {
        map.insert("calculated_bitrate_kbps".to_string(), json!(value));
    }
    if let Some(value) = extra.container_duration_seconds {
        map.insert("container_duration_seconds".to_string(), json!(value));
    }
    if let Some(value) = extra.container_bitrate_kbps {
        map.insert("container_bitrate_kbps".to_string(), json!(value));
    }
    if let Some(value) = extra.mean_volume_db {
        map.insert("mean_volume_db".to_string(), json!(value));
    }
    Value::Object(map)
}

fn run_aggregate(channels: &[ChannelInput]) -> Value {
    let mut verdicts = Map::new();
    for channel in channels {
        let streams: Vec<StreamVerdict> = channel
            .streams
            .iter()
            .map(|part| {
                StreamVerdict::from_parts(
                    1,
                    Class::parse(&part.class).unwrap_or(Class::Dead),
                    &part.error_type,
                    part.framerate,
                    static_format(&part.format),
                )
            })
            .collect();
        let borrowed: Vec<&StreamVerdict> = streams.iter().collect();
        let format = crosstalk_streams::probe::format_for(&borrowed);
        verdicts.insert(
            channel.id.to_string(),
            json!({
                "verdict": channel_verdict_name(&borrowed),
                "format": format,
            }),
        );
    }
    json!({ "channels": verdicts })
}

/// The five format labels, read off the shared quality model so the harness spells them
/// the same way the run does (`QualityTier::parse_label` over `IC_FORMAT_LABELS`).
fn static_format(text: &str) -> &'static str {
    use crosstalk_core::quality::QualityTier;
    QualityTier::parse_label(text)
        .map(|tier| tier.ic_label())
        .unwrap_or("Unknown")
}
