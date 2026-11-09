//! The one test that runs the REAL tools.
//!
//! Everything else in the probe is driven through `ToolRunner`, which is how the suite covers a
//! dozen provider behaviours without a provider. This test is the counterweight: it proves the
//! command lines are something a real ffprobe accepts and that the parse of a REAL document ends
//! up Alive, so a faithful fixture cannot hide a typo in a flag.
//!
//! It is opt-in (`#[ignore]`) because it needs ffprobe and ffmpeg installed and takes a few
//! seconds:
//!
//! ```text
//! cargo test -p crosstalk-streams --test probe_real -- --ignored --nocapture
//! ```
//!
//! Set `CROSSTALK_FFPROBE` / `CROSSTALK_FFMPEG` to point at them explicitly; otherwise the usual
//! Homebrew and Linux locations are searched. It opens one connection: to a local `wiremock`
//! server, never to a provider.

use crosstalk_streams::probe::tool::{ProcessRunner, ToolRunner};
use crosstalk_streams::probe::{probe_stream, ProbeConfig, ProbeTarget, Throttle};
use crosstalk_streams::settings::StreamsSettings;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The first of `candidates` that exists, or `None` when the test has to skip.
fn find_tool(env: &str, candidates: &[&str]) -> Option<PathBuf> {
    if let Ok(path) = std::env::var(env) {
        let path = PathBuf::from(path);
        return path.is_file().then_some(path);
    }
    candidates
        .iter()
        .map(PathBuf::from)
        .find(|path| path.is_file())
}

fn ffmpeg() -> Option<PathBuf> {
    find_tool(
        "CROSSTALK_FFMPEG",
        &[
            "/opt/homebrew/bin/ffmpeg",
            "/usr/local/bin/ffmpeg",
            "/usr/bin/ffmpeg",
        ],
    )
}

fn ffprobe() -> Option<PathBuf> {
    find_tool(
        "CROSSTALK_FFPROBE",
        &[
            "/opt/homebrew/bin/ffprobe",
            "/usr/local/bin/ffprobe",
            "/usr/bin/ffprobe",
        ],
    )
}

/// Make a two-second HD feed with ffmpeg's own sources.
fn make_feed(ffmpeg: &Path, directory: &Path) -> PathBuf {
    let path = directory.join("feed.ts");
    let status = std::process::Command::new(ffmpeg)
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=1280x720:rate=25:duration=2",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000:duration=2",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-shortest",
            "-f",
            "mpegts",
        ])
        .arg(&path)
        .status()
        .expect("ffmpeg runs");
    assert!(status.success(), "ffmpeg could not build the feed");
    path
}

#[tokio::test]
#[ignore = "needs real ffprobe and ffmpeg: cargo test -- --ignored"]
async fn a_real_ffprobe_parses_a_real_feed_served_over_http() {
    let (Some(ffprobe), Some(ffmpeg)) = (ffprobe(), ffmpeg()) else {
        eprintln!("skipping: ffprobe/ffmpeg not found (set CROSSTALK_FFPROBE / CROSSTALK_FFMPEG)");
        return;
    };
    let directory = tempfile::tempdir().expect("tempdir");
    let feed = make_feed(&ffmpeg, directory.path());
    let bytes = std::fs::read(&feed).expect("read the feed");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
        .mount(&server)
        .await;
    let url = format!("{}/live.ts", server.uri());

    let mut settings = StreamsSettings {
        enable_throughput_sorting: false,
        stream_check_delay: 0,
        ..StreamsSettings::default()
    };
    settings.ffprobe_path = ffprobe.display().to_string();
    settings.ffmpeg_path = ffmpeg.display().to_string();
    // The real command takes `probe_timeout + 8 + 5` seconds; a local 2 s feed returns long
    // before that, and a short connection timeout keeps a hung ffprobe from stalling the test.
    settings.timeout = 5;
    settings.probe_timeout = 10;
    settings.dead_connection_retries = 0;
    let config = ProbeConfig::from_settings(&settings);
    ProcessRunner::new(Arc::new(AtomicBool::new(false)))
        .preflight(&config.ffprobe_path, None)
        .expect("ffprobe is installed");

    let tools = ProcessRunner::new(Arc::new(AtomicBool::new(false)));
    let verdict = tokio::time::timeout(
        Duration::from_secs(60),
        probe_stream(
            &tools,
            &config,
            &Throttle::new(),
            &Arc::new(AtomicBool::new(false)),
            &ProbeTarget {
                stream_id: 1,
                stream_name: "Local Feed",
                account: 1,
                url: &url,
            },
        ),
    )
    .await
    .expect("the probe returns within a minute")
    .expect("the probe runs");

    assert_eq!(
        verdict.class,
        crosstalk_streams::probe::classify::Class::Alive,
        "{} ({})",
        verdict.error,
        verdict.error_type
    );
    let facts = verdict.facts.as_ref().expect("facts");
    assert_eq!(facts.resolution, "1280x720");
    assert_eq!(facts.video_codec.as_deref(), Some("h264"));
    assert_eq!(facts.format_label(), "HD");
    assert_eq!(facts.source_fps, 25.0);
    assert_eq!(facts.stream_type.as_deref(), Some("mpegts"));
    assert!(
        facts.audio_codec.is_some(),
        "the feed carries an audio track"
    );
}

/// The `-report` hole, against a real ffprobe: run in a scratch working directory, the
/// command the probe builds for a URL spelled `-report` must not turn it into the option that
/// writes `ffprobe-*.log` there. Not `#[ignore]`d: it skips itself when ffprobe is absent.
#[test]
fn a_real_ffprobe_never_reads_a_url_as_an_option() {
    let Some(ffprobe) = ffprobe() else {
        eprintln!("skipping: ffprobe not found (set CROSSTALK_FFPROBE)");
        return;
    };
    let settings = StreamsSettings {
        ffprobe_path: ffprobe.display().to_string(),
        timeout: 2,
        probe_timeout: 2,
        ..Default::default()
    };
    let config = ProbeConfig::from_settings(&settings);

    let cwd = tempfile::tempdir().expect("tempdir");
    for url in ["-report", "file:/etc/passwd", "concat:a|b"] {
        // The probe never runs these at all (the scheme allow-list); this is the second line
        // of defence, the command line itself, exercised as if the first had let them through.
        let spec = crosstalk_streams::probe::ffprobe_command(&config, url);
        let output = std::process::Command::new(&spec.program)
            .args(&spec.args)
            .current_dir(cwd.path())
            .env_remove("FFREPORT")
            .output()
            .expect("ffprobe runs");
        assert!(
            !output.status.success(),
            "{url}: ffprobe must refuse it ({})",
            String::from_utf8_lossy(&output.stderr)
        );
        let reports: Vec<String> = std::fs::read_dir(cwd.path())
            .expect("read the scratch dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("ffprobe-") && name.ends_with(".log"))
            .collect();
        assert!(reports.is_empty(), "{url}: wrote {reports:?}");
    }
}
