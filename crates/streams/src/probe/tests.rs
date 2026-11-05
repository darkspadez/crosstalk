//! The probe's tests: selection, classification, verdicts and the whole action.
//!
//! Every provider interaction is a recorded [`FakeToolRunner`] answer or a [`FakeMeter`] entry,
//! so nothing here opens a network connection. The ffprobe/ffmpeg JSON and stderr in this file
//! is the shape the real tools produce (captured in `crates/streams/tests/fixtures/probe/`, and
//! replayed through the plugin by `scripts/probe_parity.py`).

use super::*;
use crate::probe::throughput::FakeMeter;
use crate::probe::tool::{FakeToolRunner, ToolOutput};
use crate::test_support::{
    probe_world, Fixture, ServerFixture, PROBE_CHANNEL, PROBE_CHANNEL_TWO, PROBE_FEED_HIDDEN,
    PROBE_FEED_ONE, PROBE_FEED_ORPHAN, PROBE_FEED_THREE, PROBE_FEED_TWO, PROBE_URL_HIDDEN,
    PROBE_URL_ONE, PROBE_URL_ORPHAN, PROBE_URL_THREE, PROBE_URL_TWO, PROVIDER_A, PROVIDER_B,
};
use crate::{StreamsApp, ACTION_PROBE, APP_ID};
use crosstalk_core::dispatcharr::Stream;
use crosstalk_core::store::RunStatus;
use serde_json::json;

// ---------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------

/// Settings a test can wait on: no spacing, no rate cap, probe everything, no throughput.
fn settings() -> StreamsSettings {
    StreamsSettings {
        stream_check_delay: 0,
        probe_rate_per_minute: 600,
        probe_freshness_hours: 0,
        enable_throughput_sorting: false,
        ..StreamsSettings::default()
    }
}

/// The JSON a real `ffprobe -print_format json` produced for an mpegts feed.
fn alive_stdout(width: i64, height: i64, fps: &str) -> String {
    json!({
        "streams": [
            {"index": 0, "codec_name": "h264", "codec_type": "video", "width": width,
             "height": height, "pix_fmt": "yuv420p", "r_frame_rate": fps,
             "bit_rate": "5000000"},
            {"index": 1, "codec_name": "aac", "codec_type": "audio", "sample_rate": "48000",
             "channels": 2, "bit_rate": "128000"}
        ],
        "format": {"format_name": "mpegts"}
    })
    .to_string()
}

fn alive(width: i64, height: i64, fps: &str) -> ToolOutput {
    ToolOutput::new(0, alive_stdout(width, height, fps), "")
}

fn dead(stderr: &str) -> ToolOutput {
    ToolOutput::new(1, "", stderr)
}

fn runner() -> FakeToolRunner {
    FakeToolRunner::new()
}

fn engines(tools: FakeToolRunner) -> ProbeEngines {
    ProbeEngines {
        tools: std::sync::Arc::new(tools),
        meter: std::sync::Arc::new(FakeMeter::new()),
    }
}

fn target<'a>(url: &'a str, id: i64, name: &'a str) -> ProbeTarget<'a> {
    ProbeTarget {
        stream_id: id,
        stream_name: name,
        account: PROVIDER_A,
        url,
    }
}

fn no_cancel() -> std::sync::Arc<AtomicBool> {
    std::sync::Arc::new(AtomicBool::new(false))
}

/// Probe one URL with a fresh throttle.
async fn probe(tools: &FakeToolRunner, config: &ProbeConfig, url: &str, id: i64) -> StreamVerdict {
    probe_stream(
        tools,
        config,
        &Throttle::new(),
        &no_cancel(),
        &target(url, id, "ESPN One"),
    )
    .await
    .expect("the fake runner answers")
}

fn content_config() -> ProbeConfig {
    ProbeConfig {
        content: ContentOptions {
            want_black: true,
            want_freeze: true,
            want_audio: false,
            sample_seconds: 6,
            min_black_seconds: 3,
            frozen_min_seconds: 4,
            audio_threshold_db: -70,
            timeout: Duration::from_secs(20),
        },
        ..ProbeConfig::from_settings(&settings())
    }
}

// ---------------------------------------------------------------------------------------
// Selection
// ---------------------------------------------------------------------------------------

#[test]
fn every_stream_of_an_in_scope_channel_is_selected_once() {
    let world = probe_world();
    let selection = select(
        &world,
        &ScopeConfig::default(),
        &settings(),
        &HashMap::new(),
        Utc::now(),
    )
    .expect("selection");
    // Three channels in scope (the default scope is every channel), four distinct streams:
    // stream 103 is on two channels and is still one probe.
    assert_eq!(selection.channels.len(), 3);
    // Ordered by (account, stream id): the two first-account feeds, the hidden channel's feed
    // (also the first account), then the second account's.
    assert_eq!(
        selection.stream_ids(),
        vec![
            PROBE_FEED_ONE,
            PROBE_FEED_TWO,
            PROBE_FEED_HIDDEN,
            PROBE_FEED_THREE
        ]
    );
    assert_eq!(selection.skipped_no_url, 0);
    assert_eq!(selection.unattached, 0);
    // The channel's own list is kept in playback order, which the verdicts and the CSV need.
    assert_eq!(
        selection.channels[0].streams,
        vec![PROBE_FEED_ONE, PROBE_FEED_TWO, PROBE_FEED_THREE]
    );
}

#[test]
fn primary_only_takes_the_first_stream_of_each_channel() {
    let world = probe_world();
    let primary_only = StreamsSettings {
        check_alternative_streams: false,
        ..settings()
    };
    let selection = select(
        &world,
        &ScopeConfig::default(),
        &primary_only,
        &HashMap::new(),
        Utc::now(),
    )
    .expect("selection");
    assert_eq!(
        selection.stream_ids(),
        vec![PROBE_FEED_ONE, PROBE_FEED_HIDDEN, PROBE_FEED_THREE]
    );
    assert_eq!(
        selection.channels[0].streams,
        vec![PROBE_FEED_ONE],
        "the channel's considered list is the primary only"
    );
}

#[test]
fn only_visible_channels_drops_a_channel_disabled_in_every_profile() {
    let world = probe_world();
    let visible_only = StreamsSettings {
        only_visible_channels: true,
        ..settings()
    };
    let selection = select(
        &world,
        &ScopeConfig::default(),
        &visible_only,
        &HashMap::new(),
        Utc::now(),
    )
    .expect("selection");
    assert_eq!(selection.channels.len(), 2);
    assert!(!selection.stream_ids().contains(&PROBE_FEED_HIDDEN));
}

#[test]
fn an_unattached_stream_is_probed_only_when_the_setting_asks() {
    let world = probe_world();
    let on = StreamsSettings {
        probe_unattached_streams: true,
        ..settings()
    };
    let selection = select(
        &world,
        &ScopeConfig::default(),
        &on,
        &HashMap::new(),
        Utc::now(),
    )
    .expect("selection");
    assert_eq!(selection.unattached, 1);
    assert!(
        selection.stream_ids().contains(&PROBE_FEED_ORPHAN),
        "{:?}",
        selection.stream_ids()
    );
    // The hidden channel's stream is attached, so it is not also counted as unattached.
    assert_eq!(selection.stream_ids().len(), 5);

    // The setting is off by default and the orphan is then never probed.
    let selection = select(
        &world,
        &ScopeConfig::default(),
        &settings(),
        &HashMap::new(),
        Utc::now(),
    )
    .expect("selection");
    assert!(!selection.stream_ids().contains(&PROBE_FEED_ORPHAN));
}

#[test]
fn a_fresh_stream_is_skipped_and_a_stale_one_is_due() {
    let world = probe_world();
    let now = Utc::now();
    let health = |class: &str, seconds_ago: i64| StreamHealth {
        stream_id: PROBE_FEED_ONE,
        probed_at: Some(
            (now - chrono::Duration::seconds(seconds_ago))
                .to_rfc3339_opts(SecondsFormat::Secs, true),
        ),
        class: class.to_string(),
        ..StreamHealth::default()
    };
    let fresh_window = StreamsSettings {
        probe_freshness_hours: 24,
        ..settings()
    };

    // Probed an hour ago: inside the window, skipped.
    let mut map = HashMap::new();
    map.insert(PROBE_FEED_ONE, health("alive", 3_600));
    let selection =
        select(&world, &ScopeConfig::default(), &fresh_window, &map, now).expect("select");
    assert_eq!(selection.skipped_fresh, 1);
    assert!(!selection.stream_ids().contains(&PROBE_FEED_ONE));

    // Two days ago: due again.
    let mut map = HashMap::new();
    map.insert(PROBE_FEED_ONE, health("alive", 2 * 86_400));
    let selection =
        select(&world, &ScopeConfig::default(), &fresh_window, &map, now).expect("select");
    assert_eq!(selection.skipped_fresh, 0);
    assert!(selection.stream_ids().contains(&PROBE_FEED_ONE));

    // A throughput-only row has no `probed_at`, and a row with no class was never judged:
    // neither is fresh.
    let mut map = HashMap::new();
    map.insert(
        PROBE_FEED_ONE,
        StreamHealth {
            stream_id: PROBE_FEED_ONE,
            probed_at: None,
            throughput_mbps: Some(20.0),
            ..StreamHealth::default()
        },
    );
    let selection =
        select(&world, &ScopeConfig::default(), &fresh_window, &map, now).expect("select");
    assert!(selection.stream_ids().contains(&PROBE_FEED_ONE));

    // 0 hours means every stream is due, whatever the store says.
    let mut map = HashMap::new();
    map.insert(PROBE_FEED_ONE, health("alive", 60));
    let selection =
        select(&world, &ScopeConfig::default(), &settings(), &map, now).expect("select");
    assert!(selection.stream_ids().contains(&PROBE_FEED_ONE));
}

#[test]
fn a_stream_without_a_url_is_counted_and_never_queued() {
    let mut world = probe_world();
    for stream in &mut world.streams {
        if stream.id == PROBE_FEED_ONE {
            stream.url = None;
        }
    }
    let selection = select(
        &world,
        &ScopeConfig::default(),
        &settings(),
        &HashMap::new(),
        Utc::now(),
    )
    .expect("selection");
    assert!(!selection.stream_ids().contains(&PROBE_FEED_ONE));
    // Counted once, even though the channel names it once.
    assert_eq!(selection.skipped_no_url, 1);
}

#[test]
fn a_stream_id_the_stream_list_no_longer_holds_is_reported_as_missing() {
    let mut world = probe_world();
    // The channel still points at 999, which the stream list does not carry.
    world.channels[0].streams.push(999);
    let selection = select(
        &world,
        &ScopeConfig::default(),
        &settings(),
        &HashMap::new(),
        Utc::now(),
    )
    .expect("selection");
    assert_eq!(selection.skipped_missing, 1);
    assert_eq!(
        selection.skipped_no_url, 0,
        "a missing stream is not a URL-less one"
    );
    assert!(!selection.stream_ids().contains(&999));
}

#[test]
fn the_queue_is_ordered_by_account_then_stream() {
    let world = probe_world();
    let selection = select(
        &world,
        &ScopeConfig::default(),
        &settings(),
        &HashMap::new(),
        Utc::now(),
    )
    .expect("selection");
    let order: Vec<(i64, i64)> = selection
        .queue
        .iter()
        .map(|entry| (entry.account, entry.stream_id))
        .collect();
    assert_eq!(
        order,
        vec![
            (PROVIDER_A, PROBE_FEED_ONE),
            (PROVIDER_A, PROBE_FEED_TWO),
            (PROVIDER_A, PROBE_FEED_HIDDEN),
            (PROVIDER_B, PROBE_FEED_THREE),
        ]
        .into_iter()
        .filter(|(_, id)| selection.stream_ids().contains(id))
        .collect::<Vec<_>>()
    );
    // Running it twice over one world gives one order.
    let again = select(
        &world,
        &ScopeConfig::default(),
        &settings(),
        &HashMap::new(),
        Utc::now(),
    )
    .expect("selection");
    assert_eq!(selection.queue, again.queue);
}

#[test]
fn a_checkpoint_resumes_exactly_what_it_left() {
    let queue = vec![
        QueueEntry {
            account: 1,
            stream_id: 11,
        },
        QueueEntry {
            account: 1,
            stream_id: 12,
        },
        QueueEntry {
            account: 2,
            stream_id: 13,
        },
    ];
    let fingerprint = "abc";
    let checkpoint = json!({"remaining": [13, 11], "fingerprint": "abc"});
    assert_eq!(
        resume(queue.clone(), Some(&checkpoint), fingerprint),
        vec![
            QueueEntry {
                account: 2,
                stream_id: 13
            },
            QueueEntry {
                account: 1,
                stream_id: 11
            },
        ],
        "the checkpoint's own order wins, and stream 12 was already probed"
    );

    // A settings change discards it, as the plugin's fingerprint does.
    assert_eq!(resume(queue.clone(), Some(&checkpoint), "different"), queue);

    // A checkpoint whose ids are all gone falls back to a fresh pass.
    let void = json!({"remaining": [999], "fingerprint": "abc"});
    assert!(resume(queue.clone(), Some(&void), fingerprint).is_empty());

    // No checkpoint at all.
    assert_eq!(resume(queue.clone(), None, fingerprint), queue);

    // A checkpoint written by an older build has no fingerprint: it is discarded, not trusted.
    let legacy = json!({"remaining": [11]});
    assert_eq!(resume(queue.clone(), Some(&legacy), fingerprint), queue);

    // The fingerprint ignores settings that change nothing about what is probed.
    let fixture = Fixture::new();
    let baseline_settings = settings();
    let baseline = probe_fingerprint(&baseline_settings, &fixture.config.scope);
    let mut renamed = baseline_settings.clone();
    renamed.dead_rename_format = "{name} [BROKEN]".to_string();
    assert_eq!(
        baseline,
        probe_fingerprint(&renamed, &fixture.config.scope),
        "a triage format does not change what a probe measures"
    );
    let mut deafened = baseline_settings;
    deafened.silent_audio_detection = true;
    assert_ne!(
        baseline,
        probe_fingerprint(&deafened, &fixture.config.scope),
        "arming a detector does"
    );
}

// ---------------------------------------------------------------------------------------
// One stream
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn a_live_feed_is_alive_with_the_facts_the_plugin_writes() {
    let tools = runner().answering(
        "ffprobe",
        "http://provider.test/a.ts",
        alive(1920, 1080, "60/1"),
    );
    let config = ProbeConfig::from_settings(&settings());
    let verdict = probe(&tools, &config, "http://provider.test/a.ts", 7).await;

    assert_eq!(verdict.class, Class::Alive);
    assert_eq!(verdict.error_type, "N/A");
    assert_eq!(verdict.health_class, "alive");
    assert_eq!(verdict.format, "FHD");
    assert_eq!(verdict.framerate, 60.0);
    let facts = verdict.facts.as_ref().expect("facts");
    assert_eq!(facts.resolution, "1920x1080");
    assert_eq!(facts.video_codec.as_deref(), Some("h264"));
    assert_eq!(facts.audio_channels.as_deref(), Some("stereo"));
    assert_eq!(
        verdict.merged_stats(&json!({})),
        json!({
            "video_codec": "h264",
            "resolution": "1920x1080",
            "width": 1920,
            "height": 1080,
            "source_fps": 60.0,
            "pixel_format": "yuv420p",
            "video_bitrate": 5000,
            "audio_codec": "aac",
            "sample_rate": 48000,
            "audio_channels": "stereo",
            "audio_bitrate": 128.0,
            "stream_type": "mpegts",
        })
    );
    // One connection, one ffprobe command, and the same flags the plugin builds.
    assert_eq!(tools.calls().len(), 1);
    assert_eq!(tools.calls()[0].program_name(), "ffprobe");
    assert_eq!(
        tools.calls()[0].args.last().unwrap(),
        "http://provider.test/a.ts"
    );
}

#[tokio::test]
async fn a_provider_404_is_dead_and_the_url_never_reaches_the_report() {
    let url = "http://provider.test/secret/token/12345.ts";
    let tools = runner().answering(
        "ffprobe",
        url,
        dead(&format!(
            "[in#0 @ 0x1] Error opening input: Server returned 404 Not Found\n{url}: Server \
             returned 404 Not Found\n"
        )),
    );
    let config = ProbeConfig::from_settings(&settings());
    let verdict = probe(&tools, &config, url, 7).await;

    assert_eq!(verdict.class, Class::Dead);
    assert_eq!(verdict.error_type, "404 Not Found");
    assert_eq!(verdict.health_class, "dead");
    assert!(
        !verdict.error.contains("12345"),
        "the URL is masked: {}",
        verdict.error
    );
    assert!(
        verdict.error.contains("[Stream ID: 7]"),
        "{}",
        verdict.error
    );
    // A 404 is not retryable, so it cost exactly one connection.
    assert_eq!(verdict.retries, 0);
    // Dead clears the stats (the plugin's `{}`), it does not merge.
    assert_eq!(
        verdict.merged_stats(&json!({"width": 1920, "height": 1080})),
        json!({})
    );
    assert!(verdict.writes_stats());
}

#[tokio::test]
async fn a_timeout_is_retried_in_the_plugins_set() {
    // Two attempts: the first times out, the second answers. The retry succeeds, so the stream
    // ends Alive with one retry recorded.
    let tools = runner().answering_sequence(
        "ffprobe",
        "http://provider.test/a.ts",
        vec![
            ToolOutput {
                status: None,
                stdout: String::new(),
                stderr: String::new(),
                timed_out: true,
                cancelled: false,
            },
            alive(1280, 720, "50/1"),
        ],
    );
    let config = ProbeConfig::from_settings(&settings());
    let verdict = probe(&tools, &config, "http://provider.test/a.ts", 7).await;
    assert_eq!(verdict.class, Class::Alive);
    assert_eq!(verdict.retries, 1);
    assert_eq!(tools.calls().len(), 2);

    // A 403 is not retried at all.
    let tools = runner().answering(
        "ffprobe",
        "http://provider.test/b.ts",
        dead("http://provider.test/b.ts: Server returned 403 Forbidden\n"),
    );
    let verdict = probe(&tools, &config, "http://provider.test/b.ts", 8).await;
    assert_eq!(verdict.class, Class::Dead);
    assert_eq!(verdict.error_type, "403 Forbidden");
    assert_eq!(tools.calls().len(), 1);
}

#[tokio::test]
async fn a_rate_limited_stream_is_skipped_and_is_never_retried() {
    let tools = runner().answering(
        "ffprobe",
        "http://provider.test/a.ts",
        dead("http://provider.test/a.ts: Server returned 429 Too Many Requests\n"),
    );
    let config = ProbeConfig::from_settings(&settings());
    let throttle = Throttle::new();
    let verdict = probe_stream(
        &tools,
        &config,
        &throttle,
        &no_cancel(),
        &target("http://provider.test/a.ts", 7, "ESPN"),
    )
    .await
    .expect("answers");

    assert_eq!(verdict.class, Class::Skipped, "a 429 is not evidence");
    assert_eq!(verdict.error_type, "Rate Limited");
    assert_eq!(verdict.health_class, "skipped");
    assert_eq!(verdict.retries, 0);
    assert_eq!(
        tools.calls().len(),
        1,
        "a retry is what the 429 asked us not to do"
    );
    // A skipped stream never has its stats written: `0x0` would read as dead to every reader.
    assert!(!verdict.writes_stats());
}

#[tokio::test]
async fn an_audio_only_stream_is_skipped_not_dead() {
    let tools = runner().answering(
        "ffprobe",
        "http://provider.test/radio.mp3",
        ToolOutput::new(
            0,
            json!({
                "streams": [{"index": 0, "codec_type": "audio", "codec_name": "mp3"}],
                "format": {"format_name": "mp3"}
            })
            .to_string(),
            "",
        ),
    );
    let config = ProbeConfig::from_settings(&settings());
    let verdict = probe(&tools, &config, "http://provider.test/radio.mp3", 9).await;
    assert_eq!(verdict.class, Class::Skipped);
    assert_eq!(verdict.error_type, "No Video Stream");
    assert!(!verdict.writes_stats());
}

#[tokio::test]
async fn a_streamlink_host_is_skipped_without_running_anything() {
    let tools = runner();
    let config = ProbeConfig::from_settings(&settings());
    let verdict = probe(&tools, &config, "https://www.twitch.tv/somechannel", 9).await;
    assert_eq!(verdict.class, Class::Skipped);
    assert_eq!(verdict.error_type, "Skipped");
    assert!(verdict.error.contains("Streamlink-only"));
    assert!(tools.calls().is_empty(), "nothing was run");
}

#[tokio::test]
async fn a_black_screen_is_dead_blank_and_wins_over_frozen() {
    let url = "http://provider.test/a.ts";
    let tools = runner()
        .answering("ffprobe", url, alive(1920, 1080, "50/1"))
        .answering(
            "ffmpeg",
            url,
            ToolOutput::new(
                0,
                "",
                "[blackdetect @ 0x1] black_start:0 black_end:6 black_duration:6\n\
                 [Parsed_freezedetect_1 @ 0x2] lavfi.freezedetect.freeze_start: 0.0666670\n",
            ),
        );
    let config = content_config();
    let verdict = probe(&tools, &config, url, 7).await;
    assert_eq!(verdict.class, Class::Dead);
    assert_eq!(verdict.error_type, "Black Screen");
    assert_eq!(verdict.health_class, "blank");
    // The plugin's `_dead()` returns `default_return`: a content death reports `N/A` and 0 fps
    // and keeps only its evidence block.
    assert_eq!(verdict.format, "N/A");
    assert_eq!(verdict.framerate, 0.0);
    assert_eq!(
        verdict.extra.packet_count, None,
        "no packets in this fixture"
    );
    // The ffmpeg pass ran once, after the ffprobe pass.
    let calls = tools.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].program_name(), "ffmpeg");
    assert!(calls[1]
        .args
        .iter()
        .any(|arg| arg.contains("freezedetect=n=-60dB:d=4")));
}

#[tokio::test]
async fn a_frozen_picture_is_dead_frozen_when_black_detection_is_off() {
    let url = "http://provider.test/a.ts";
    let tools = runner()
        .answering("ffprobe", url, alive(1280, 720, "50/1"))
        .answering(
            "ffmpeg",
            url,
            ToolOutput::new(0, "", "lavfi.freezedetect.freeze_start: 0.0\n"),
        );
    let mut config = content_config();
    config.content.want_black = false;
    let verdict = probe(&tools, &config, url, 7).await;
    assert_eq!(verdict.error_type, "Frozen Video");
    assert_eq!(verdict.health_class, "frozen");
}

#[tokio::test]
async fn a_silent_audio_track_is_dead_silent_only_when_the_track_exists() {
    let url = "http://provider.test/a.ts";
    let tools = runner()
        .answering("ffprobe", url, alive(1280, 720, "50/1"))
        .answering(
            "ffmpeg",
            url,
            ToolOutput::new(0, "", "mean_volume: -91.0 dB\n"),
        );
    let mut config = content_config();
    config.content.want_black = false;
    config.content.want_freeze = false;
    config.content.want_audio = true;
    let verdict = probe(&tools, &config, url, 7).await;
    assert_eq!(verdict.error_type, "Silent Audio");
    assert_eq!(verdict.health_class, "silent");
    assert_eq!(verdict.extra.mean_volume_db, Some(-91.0));

    // A stream with no audio track is never "silent": the detector is dropped for it, and no
    // ffmpeg pass runs at all.
    let video_only = json!({
        "streams": [{"index": 0, "codec_type": "video", "width": 1280, "height": 720,
                     "r_frame_rate": "50/1"}],
        "format": {"format_name": "mpegts"}
    })
    .to_string();
    let tools = runner().answering("ffprobe", url, ToolOutput::new(0, video_only, ""));
    let verdict = probe(&tools, &config, url, 7).await;
    assert_eq!(verdict.class, Class::Alive);
    assert_eq!(tools.calls().len(), 1, "no detector could speak for it");
}

#[tokio::test]
async fn a_finite_duration_is_a_placeholder_file_when_the_detection_is_on() {
    let url = "http://provider.test/slate.ts";
    let stdout = json!({
        "streams": [{"index": 0, "codec_type": "video", "width": 1920, "height": 1080,
                     "r_frame_rate": "25/1", "bit_rate": "193000"}],
        "format": {"format_name": "mpegts", "duration": "600.0", "bit_rate": "192953"}
    })
    .to_string();
    let tools = runner().answering("ffprobe", url, ToolOutput::new(0, stdout, ""));

    // Off by default: the evidence is recorded and the stream stays Alive.
    let config = ProbeConfig::from_settings(&settings());
    let verdict = probe(&tools, &config, url, 7).await;
    assert_eq!(verdict.class, Class::Alive);
    assert_eq!(verdict.extra.container_duration_seconds, Some(600.0));
    assert_eq!(verdict.extra.container_bitrate_kbps, Some(193));

    // Armed: a live stream never reports a duration, so this is a file.
    let mut settings = settings();
    settings.placeholder_file_detection = true;
    let config = ProbeConfig::from_settings(&settings);
    let verdict = probe(&tools, &config, url, 7).await;
    assert_eq!(verdict.class, Class::Dead);
    assert_eq!(verdict.error_type, "Placeholder File");
    assert_eq!(verdict.health_class, "placeholder");
    assert!(verdict.error.contains("600.0s"), "{}", verdict.error);
}

#[tokio::test]
async fn a_killed_invocation_is_a_cancellation_and_never_a_verdict() {
    // The operator cancelled mid-probe: `ProcessRunner` kills the child and says so. IPTV
    // Checker called this `Cancelled` and excluded it from its counts, because a killed probe
    // says nothing about the stream — so neither a retry nor a health row follows.
    let tools = runner().answering(
        "ffprobe",
        "http://provider.test/a.ts",
        ToolOutput {
            status: None,
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
            cancelled: true,
        },
    );
    let config = ProbeConfig::from_settings(&settings());
    let verdict = probe(&tools, &config, "http://provider.test/a.ts", 7).await;
    assert_eq!(verdict.class, Class::Skipped);
    assert_eq!(verdict.error_type, classify::CANCELLED);
    assert!(!verdict.writes_stats());
    assert_eq!(tools.calls().len(), 1, "a cancelled probe is not retried");
}

#[tokio::test]
async fn an_ffmpeg_pass_that_fails_leaves_the_stream_alive() {
    let url = "http://provider.test/a.ts";
    let tools = runner()
        .answering("ffprobe", url, alive(1280, 720, "50/1"))
        .answering(
            "ffmpeg",
            url,
            ToolOutput::new(1, "", "Connection refused\n"),
        );
    let config = content_config();
    let verdict = probe(&tools, &config, url, 7).await;
    assert_eq!(verdict.class, Class::Alive, "fail-open");
    assert_eq!(verdict.extra.mean_volume_db, None);
}

// ---------------------------------------------------------------------------------------
// Verdicts
// ---------------------------------------------------------------------------------------

fn verdict(class: Class, error_type: &str, framerate: f64) -> StreamVerdict {
    let mut verdict = StreamVerdict::new(
        &ProbeTarget {
            stream_id: 1,
            stream_name: "ESPN",
            account: 1,
            url: "http://provider.test/a.ts",
        },
        class,
        error_type,
    );
    verdict.framerate = framerate;
    verdict
}

#[test]
fn a_channel_is_dead_only_when_every_stream_is_dead() {
    let dead = verdict(Class::Dead, "Timeout", 0.0);
    let alive = verdict(Class::Alive, "N/A", 50.0);
    let skipped = verdict(Class::Skipped, "Rate Limited", 0.0);
    assert_eq!(verdict_for(&[&dead, &dead]), ChannelVerdict::Dead);
    // A backup that works means the channel plays: the plugin's measured 2026-08-05 bug.
    assert_eq!(verdict_for(&[&dead, &alive]), ChannelVerdict::Alive);
    // Skipped is not evidence in either direction.
    assert_eq!(
        verdict_for(&[&dead, &skipped]),
        ChannelVerdict::Inconclusive
    );
    assert_eq!(verdict_for(&[&skipped]), ChannelVerdict::Skipped);
    assert_eq!(verdict_for(&[]), ChannelVerdict::Inconclusive);
}

#[test]
fn a_blank_channel_is_separate_from_a_dead_one() {
    let black = verdict(Class::Dead, "Black Screen", 0.0);
    let timed_out = verdict(Class::Dead, "Timeout", 0.0);
    assert_eq!(verdict_for(&[&black, &black]), ChannelVerdict::Blank);
    // One black screen beside a timeout matches neither predicate, so no action applies.
    assert_eq!(
        verdict_for(&[&black, &timed_out]),
        ChannelVerdict::Inconclusive
    );
}

#[test]
fn a_slow_channel_is_every_alive_stream_below_the_threshold() {
    let slow = verdict(Class::Alive, "N/A", 20.0);
    let fast = verdict(Class::Alive, "N/A", 50.0);
    let dead = verdict(Class::Dead, "Timeout", 0.0);
    assert_eq!(verdict_for(&[&slow, &slow]), ChannelVerdict::LowFrameRate);
    assert_eq!(verdict_for(&[&slow, &fast]), ChannelVerdict::Alive);
    // A dead stream does not veto the slow-framerate rename: the plugin looked only at the
    // Alive rows (`among=lambda r: r['status'] == 'Alive'`).
    assert_eq!(verdict_for(&[&slow, &dead]), ChannelVerdict::LowFrameRate);
    // A stream with no measured framerate is not slow.
    let unmeasured = verdict(Class::Alive, "N/A", 0.0);
    assert_eq!(verdict_for(&[&unmeasured]), ChannelVerdict::Alive);
}

#[test]
fn the_format_comes_from_the_last_alive_stream() {
    let mut first = verdict(Class::Alive, "N/A", 50.0);
    first.format = "SD";
    let mut second = verdict(Class::Alive, "N/A", 50.0);
    second.format = "UHD";
    let dead = verdict(Class::Dead, "Timeout", 0.0);
    assert_eq!(format_for(&[&first, &dead, &second]), Some("UHD"));
    assert_eq!(format_for(&[&dead]), None);

    // Every verdict name round-trips, which is how triage reads them.
    for verdict in ChannelVerdict::ALL {
        assert_eq!(ChannelVerdict::parse(verdict.as_str()), Some(verdict));
    }
    assert_eq!(ChannelVerdict::parse("nonsense"), None);
}

#[test]
fn triage_can_read_the_verdicts_back_out_of_the_run_rows() {
    // The contract between the probe and triage: two fields, one channel, one reader.
    let rows = vec![
        RunRow {
            subject: Subject::Channel,
            channel_id: 7,
            channel_name: "ESPN".to_string(),
            channel_group: "TVE".to_string(),
            field: VERDICT_FIELD.to_string(),
            after_value: Some("dead".to_string()),
            ..RunRow::default()
        },
        RunRow {
            subject: Subject::Channel,
            channel_id: 8,
            channel_name: "ESPN2".to_string(),
            field: VERDICT_FIELD.to_string(),
            after_value: Some("low_frame_rate".to_string()),
            ..RunRow::default()
        },
        RunRow {
            subject: Subject::Channel,
            channel_id: 8,
            channel_name: "ESPN2".to_string(),
            field: FORMAT_FIELD.to_string(),
            after_value: Some("HD".to_string()),
            ..RunRow::default()
        },
        // A stream row is not a channel verdict, and neither is a row with junk in it.
        RunRow {
            subject: Subject::Stream,
            channel_id: 7,
            field: VERDICT_FIELD.to_string(),
            after_value: Some("alive".to_string()),
            ..RunRow::default()
        },
        RunRow {
            subject: Subject::Channel,
            channel_id: 9,
            field: VERDICT_FIELD.to_string(),
            after_value: Some("nonsense".to_string()),
            ..RunRow::default()
        },
    ];
    let verdicts = recorded_verdicts(&rows);
    assert_eq!(verdicts.len(), 2);
    assert_eq!(verdicts[0].channel_id, 7);
    assert_eq!(verdicts[0].verdict, ChannelVerdict::Dead);
    assert_eq!(verdicts[0].channel_group, "TVE");
    assert_eq!(verdicts[0].format, None, "a dead channel has no format row");
    assert_eq!(verdicts[1].verdict, ChannelVerdict::LowFrameRate);
    assert_eq!(verdicts[1].format.as_deref(), Some("HD"));
}

/// A verdict row, as a finished probe run records it.
fn verdict_row(ordinal: i64, channel_id: i64, channel_name: &str, verdict: &str) -> RunRow {
    RunRow {
        ordinal,
        subject: Subject::Channel,
        channel_id,
        channel_name: channel_name.to_string(),
        status: verdict.to_string(),
        field: VERDICT_FIELD.to_string(),
        after_value: Some(verdict.to_string()),
        ..RunRow::default()
    }
}

#[tokio::test]
async fn the_result_set_spans_a_resumed_sequence() {
    // A windowed pass split across three runs: a paused first window, a paused second
    // window that re-judged one of its channels, and the succeeded run that drained the
    // queue. The result set is the union, newest winning per channel — the plugin's
    // accumulated results.json. The older succeeded run is the previous complete result
    // set and is not read.
    let server = ServerFixture::start(&probe_world()).await;
    let store = server.store();
    let finish = |run_id: i64, status: RunStatus| {
        store
            .finish_run(run_id, status, None, &serde_json::json!({}), None)
            .expect("finish")
    };

    let old = store.start_run(APP_ID, ACTION, false).expect("start");
    store
        .append_rows(old, &[verdict_row(0, 9, "Old", "dead")])
        .expect("append");
    finish(old, RunStatus::Succeeded);

    let first = store.start_run(APP_ID, ACTION, false).expect("start");
    store
        .append_rows(
            first,
            &[
                verdict_row(0, 7, "ESPN", "dead"),
                verdict_row(1, 8, "CNN", "alive"),
            ],
        )
        .expect("append");
    finish(first, RunStatus::Paused);

    let second = store.start_run(APP_ID, ACTION, false).expect("start");
    store
        .append_rows(second, &[verdict_row(0, 8, "CNN", "dead")])
        .expect("append");
    finish(second, RunStatus::Succeeded);

    let context = server.context_for_run(ACTION, &store, false);
    let probe = last_verdicts(&context).expect("a result set");
    assert_eq!(probe.run_id, second);
    assert_eq!(probe.run_ids, vec![second, first]);
    let by_id: std::collections::HashMap<i64, &RecordedVerdict> = probe
        .verdicts
        .iter()
        .map(|verdict| (verdict.channel_id, verdict))
        .collect();
    assert_eq!(by_id.len(), 2, "{by_id:?}");
    assert_eq!(by_id[&7].verdict, ChannelVerdict::Dead);
    assert_eq!(
        by_id[&8].verdict,
        ChannelVerdict::Dead,
        "the newest verdict wins per channel"
    );
    assert!(
        probe.describe().contains("(across 2 runs)"),
        "{}",
        probe.describe()
    );
    finish(context.run_id().expect("run id"), RunStatus::Succeeded);
}

#[tokio::test]
async fn a_dry_run_probe_is_a_result_set_too() {
    // A --dry-run probe records the same verdict rows and only skips the Dispatcharr write,
    // so it counts as the newest succeeded run exactly like an apply does.
    let server = ServerFixture::start(&probe_world()).await;
    let store = server.store();
    let run = store.start_run(APP_ID, ACTION, true).expect("start");
    store
        .append_rows(run, &[verdict_row(0, 7, "ESPN", "dead")])
        .expect("append");
    store
        .finish_run(
            run,
            RunStatus::Succeeded,
            None,
            &serde_json::json!({}),
            None,
        )
        .expect("finish");
    let context = server.context_for_run(ACTION, &store, false);
    let probe = last_verdicts(&context).expect("a result set");
    assert_eq!(probe.run_id, run);
    assert_eq!(probe.verdicts.len(), 1);
    assert_eq!(probe.verdicts[0].verdict, ChannelVerdict::Dead);
    store
        .finish_run(
            context.run_id().expect("run id"),
            RunStatus::Succeeded,
            None,
            &serde_json::json!({}),
            None,
        )
        .expect("finish");
}

#[test]
fn the_limiter_caps_an_account_at_its_own_max_streams() {
    // The fixture's accounts publish max_streams 2 and 1; the setting asks for more.
    let world = probe_world();
    let raised = StreamsSettings {
        probe_per_account_limit: 8,
        ..settings()
    };
    let limiter = build_limiter(&raised, &world);
    assert_eq!(limiter.permits_for(&account_key(PROVIDER_A)), 2);
    assert_eq!(limiter.permits_for(&account_key(PROVIDER_B)), 1);
    // A key with no cap keeps the setting.
    assert_eq!(limiter.permits_for(&account_key(99)), 8);
    assert_eq!(limiter.config().min_spacing, Duration::ZERO);
    assert_eq!(
        limiter.config().rate_per_minute,
        0,
        "no global rate paces the ffprobe checks: neither plugin capped its stream checks per \
         minute (IPTV Checker is paced by its workers, its per-stream delay and its 429 backoff)"
    );
    // The global rate lives on the throughput downloads only, which is the one place
    // Stream-Mapparr used it (`probe_throughput_action`).
    assert_eq!(
        build_throughput_limiter(&raised).config().rate_per_minute,
        600
    );
    assert_eq!(raised.probe_per_account_limit, 8);
}

#[test]
fn the_throttle_trips_on_a_burst_of_429s_and_only_then() {
    let throttle = Throttle::new();
    for _ in 0..THROTTLE_TRIP_THRESHOLD - 1 {
        throttle.record_hit();
    }
    assert_eq!(
        throttle.cooldown(),
        Duration::ZERO,
        "four hits do not trip the guard"
    );
    throttle.record_hit();
    assert!(
        throttle.cooldown() > Duration::from_secs(50),
        "the fifth trips a one-minute cooldown"
    );
    // A hit inside the cooldown does not extend it.
    let before = throttle.cooldown();
    throttle.record_hit();
    assert!(throttle.cooldown() <= before);
}

// ---------------------------------------------------------------------------------------
// The action
// ---------------------------------------------------------------------------------------

/// A runner that answers every probe fixture URL as a healthy feed.
fn healthy_runner() -> FakeToolRunner {
    runner()
        .answering("ffprobe", PROBE_URL_ONE, alive(1920, 1080, "60/1"))
        .answering("ffprobe", PROBE_URL_TWO, alive(1280, 720, "50/1"))
        .answering("ffprobe", PROBE_URL_THREE, alive(720, 576, "25/1"))
        .answering("ffprobe", PROBE_URL_HIDDEN, alive(1280, 720, "50/1"))
        .answering("ffprobe", PROBE_URL_ORPHAN, alive(1280, 720, "50/1"))
}

async fn run_action(context: JobContext, dry_run: bool, engines: ProbeEngines) -> JobOutcome {
    run_with(&StreamsApp::new(), &context, &settings(), dry_run, engines)
        .await
        .expect("the probe runs")
}

#[tokio::test]
async fn the_global_rate_never_paces_the_ffprobe_checks() {
    // Regression test for a misapplied Stream-Mapparr setting: `probe_rate_per_minute`
    // throttled its throughput downloads only (`probe_throughput_action`), never the stream
    // checks. At 1 download/minute and no per-account spacing, four ffprobe checks with the
    // downloads off must finish in seconds: under the old wiring the fourth check alone would
    // have waited three minutes for its start.
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let mut paced = settings();
    paced.stream_check_delay = 0;
    paced.probe_rate_per_minute = 1;
    paced.enable_throughput_sorting = false;
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        run_with(
            &StreamsApp::new(),
            &context,
            &paced,
            false,
            engines(healthy_runner()),
        ),
    )
    .await
    .expect("the run finishes in seconds, not minutes")
    .expect("the probe runs");
    assert_eq!(outcome.counts.get("probed"), Some(&4));
}

#[tokio::test]
async fn the_action_probes_every_due_stream_and_record_health_and_stats() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let run_id = context.run_id().expect("run");
    let outcome = run_action(context, false, engines(healthy_runner())).await;

    assert!(
        outcome.summary.contains("Probed 4 stream(s)"),
        "{}",
        outcome.summary
    );
    assert_eq!(outcome.counts.get("alive"), Some(&4));
    assert_eq!(outcome.counts.get("dead"), Some(&0));
    assert_eq!(outcome.counts.get("health_written"), Some(&4));
    assert!(outcome.report_path.is_some());
    assert_eq!(
        outcome.status,
        crosstalk_core::subapp::OutcomeStatus::Succeeded
    );

    // `stream_health`: one row per stream, with the fine class and no error type.
    let health = store
        .stream_health(PROBE_FEED_ONE)
        .expect("row")
        .expect("some");
    assert_eq!(health.class, "alive");
    assert_eq!(health.error_type, None);
    assert_eq!(health.consecutive_failures, 0);
    assert!(health.probed_at.is_some());

    // `stream_stats`: merged onto the empty stats each fixture stream carries.
    let bodies = fixture
        .bodies("PATCH", &format!("/api/channels/streams/{PROBE_FEED_ONE}/"))
        .await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    let stats = &bodies[0]["stream_stats"];
    assert_eq!(stats["resolution"], "1920x1080");
    assert_eq!(stats["video_codec"], "h264");
    assert_eq!(outcome.counts.get("stats_written"), Some(&4));

    // Run rows: one per stream with the stream subject, then the channel verdicts.
    let rows = store.rows(run_id).expect("rows");
    let stream_rows: Vec<&RunRow> = rows
        .iter()
        .filter(|row| row.field == STREAM_FIELD)
        .collect();
    assert_eq!(stream_rows.len(), 4);
    assert!(stream_rows.iter().all(|row| row.subject == Subject::Stream));
    // The stats write is recorded on each stream's row, like every other write.
    assert!(
        stream_rows.iter().all(|row| row.method == "stats_written"),
        "{stream_rows:?}"
    );
    // The merge base was re-read right before the write, not taken from the world loaded
    // when the run began: the stream list is fetched twice.
    assert!(
        fixture.request_count("GET", "/api/channels/streams/").await >= 2,
        "the stats base is re-read before the write"
    );
    assert!(stream_rows
        .iter()
        .any(|row| row.channel_id == PROBE_FEED_ONE && row.status == "Alive"));

    let verdicts: Vec<&RunRow> = rows
        .iter()
        .filter(|row| row.field == VERDICT_FIELD)
        .collect();
    assert_eq!(verdicts.len(), 3, "one per in-scope channel");
    assert!(verdicts
        .iter()
        .all(|row| row.after_value.as_deref() == Some("alive")));
    let formats: Vec<&RunRow> = rows
        .iter()
        .filter(|row| row.field == FORMAT_FIELD)
        .collect();
    assert_eq!(formats.len(), 3);
    // Channel 1's last alive stream is the 576-line feed, which is SD.
    assert!(formats
        .iter()
        .any(|row| row.channel_id == PROBE_CHANNEL && row.after_value.as_deref() == Some("SD")));

    // The report carries IPTV Checker's columns.
    let report = std::fs::read_to_string(outcome.report_path.expect("report")).expect("report");
    for column in [
        "error_type",
        "framerate_num",
        "ffprobe_monitoring_seconds",
        "throughput_mbps",
        "container_duration_seconds",
    ] {
        assert!(report.contains(column), "{column} missing:\n{report}");
    }
    assert!(report.contains("ESPN One"), "{report}");
    assert!(
        report.contains("Dead by error type") || report.contains("Results"),
        "{report}"
    );

    // The queue drained, so there is no checkpoint to resume.
    assert_eq!(
        store.load_checkpoint(APP_ID, ACTION).expect("checkpoint"),
        None
    );
    // And the run is still running: the host finishes it.
    let run = store.run(run_id).expect("run record").expect("recorded");
    assert_eq!(run.status, RunStatus::Running);
}

#[tokio::test]
async fn the_action_clears_a_dead_streams_stats_and_counts_its_failure() {
    // The stream already has the stats a previous probe wrote, so the clear is observable.
    let mut world = probe_world();
    for stream in &mut world.streams {
        if stream.id == PROBE_FEED_TWO {
            stream.stream_stats = json!({"width": 1280, "height": 720, "resolution": "1280x720"});
        }
    }
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let run_id = context.run_id().expect("run");
    let engines = engines(
        runner()
            .answering("ffprobe", PROBE_URL_ONE, alive(1920, 1080, "60/1"))
            .answering("ffprobe", PROBE_URL_TWO, dead("Stream not accessible\n"))
            .answering("ffprobe", PROBE_URL_THREE, alive(720, 576, "25/1"))
            .answering("ffprobe", PROBE_URL_HIDDEN, alive(1280, 720, "50/1")),
    );
    let outcome = run_action(context, false, engines).await;

    assert_eq!(outcome.counts.get("dead"), Some(&1));
    assert_eq!(outcome.counts.get("alive"), Some(&3));
    let health = store
        .stream_health(PROBE_FEED_TWO)
        .expect("row")
        .expect("some");
    assert_eq!(health.class, "dead");
    assert_eq!(health.error_type.as_deref(), Some("Stream Unreachable"));
    assert_eq!(health.consecutive_failures, 1);

    let bodies = fixture
        .bodies("PATCH", &format!("/api/channels/streams/{PROBE_FEED_TWO}/"))
        .await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    assert_eq!(
        bodies[0]["stream_stats"],
        json!({}),
        "a dead stream's stats are cleared, not merged"
    );

    // The channel with the dead stream is still `alive`: its other two streams are alive.
    let rows = store.rows(run_id).expect("rows");
    let verdict = rows
        .iter()
        .find(|row| row.field == VERDICT_FIELD && row.channel_id == PROBE_CHANNEL)
        .expect("the channel verdict");
    assert_eq!(verdict.after_value.as_deref(), Some("alive"));
}

#[tokio::test]
async fn the_action_counts_a_dead_channel_when_all_its_streams_fail() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let run_id = context.run_id().expect("run");
    let engines = engines(
        runner()
            .answering("ffprobe", PROBE_URL_ONE, dead("Connection refused\n"))
            .answering("ffprobe", PROBE_URL_TWO, dead("Connection refused\n"))
            .answering("ffprobe", PROBE_URL_THREE, dead("Connection refused\n"))
            .answering("ffprobe", PROBE_URL_HIDDEN, alive(1280, 720, "50/1")),
    );
    let outcome = run_action(context, false, engines).await;
    assert_eq!(outcome.counts.get("channels_dead"), Some(&2));
    let rows = store.rows(run_id).expect("rows");
    let dead: Vec<&RunRow> = rows
        .iter()
        .filter(|row| row.field == VERDICT_FIELD && row.after_value.as_deref() == Some("dead"))
        .collect();
    assert_eq!(dead.len(), 2);
    assert!(
        dead.iter().any(|row| row.channel_id == PROBE_CHANNEL_TWO),
        "{rows:?}"
    );
    // The reason names the error type, so an operator sees why without the CSV.
    assert!(
        dead[0].reason.contains("Connection Refused"),
        "{}",
        dead[0].reason
    );
    assert_eq!(outcome.counts.get("dead_connection_refused"), Some(&3));
}

#[tokio::test]
async fn a_cancelled_probe_in_flight_leaves_no_health_row_and_no_stats_write() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let run_id = context.run_id().expect("run");
    // Feed one's invocation is killed: nothing is claimed for it. The others are fine.
    let engines = engines(
        runner()
            .answering(
                "ffprobe",
                PROBE_URL_ONE,
                ToolOutput {
                    status: None,
                    stdout: String::new(),
                    stderr: String::new(),
                    timed_out: false,
                    cancelled: true,
                },
            )
            .answering("ffprobe", PROBE_URL_TWO, alive(1280, 720, "50/1"))
            .answering("ffprobe", PROBE_URL_THREE, alive(720, 576, "25/1"))
            .answering("ffprobe", PROBE_URL_HIDDEN, alive(1280, 720, "50/1")),
    );
    let outcome = run_action(context, false, engines).await;
    assert_eq!(outcome.counts.get("probed"), Some(&3));
    assert_eq!(outcome.counts.get("remaining"), Some(&1));
    assert_eq!(store.stream_health(PROBE_FEED_ONE).expect("row"), None);
    assert!(fixture
        .bodies("PATCH", &format!("/api/channels/streams/{PROBE_FEED_ONE}/"))
        .await
        .is_empty());
    // No CSV row for it either: the report is a record of what was measured.
    let report = std::fs::read_to_string(outcome.report_path.expect("report")).expect("report");
    assert!(!report.contains("ESPN One"), "{report}");
    assert!(report.contains("ESPN Two"), "{report}");
    let rows = store.rows(run_id).expect("rows");
    assert!(!rows.iter().any(|row| row.channel_id == PROBE_FEED_ONE));
    // And the checkpoint holds it, so the next run picks it up.
    let checkpoint = store
        .load_checkpoint(APP_ID, ACTION)
        .expect("checkpoint")
        .expect("saved");
    assert_eq!(checkpoint["remaining"], json!([PROBE_FEED_ONE]));
}

#[tokio::test]
async fn a_dry_run_records_health_and_writes_no_stream_stats() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let context = fixture.context_for_run(ACTION_PROBE, &store, true);
    let outcome = run_action(context, true, engines(healthy_runner())).await;

    assert!(
        outcome.summary.contains("No stream stats were written"),
        "{}",
        outcome.summary
    );
    assert_eq!(outcome.counts.get("stats_written"), Some(&0));
    assert!(
        fixture
            .bodies("PATCH", "/api/channels/streams/101/")
            .await
            .is_empty(),
        "a dry run writes nothing to Dispatcharr"
    );
    // What it measured is still recorded: freshness and triage read health, not stats.
    assert_eq!(
        store
            .stream_health(PROBE_FEED_ONE)
            .expect("row")
            .expect("some")
            .class,
        "alive"
    );
    assert!(outcome.report_path.is_some());
}

#[tokio::test]
async fn a_stream_whose_stats_would_not_change_is_not_rewritten() {
    let mut world = probe_world();
    // Exactly what the probe would write for feed one.
    for stream in &mut world.streams {
        if stream.id == PROBE_FEED_ONE {
            stream.stream_stats = json!({
                "video_codec": "h264",
                "resolution": "1920x1080",
                "width": 1920,
                "height": 1080,
                "source_fps": 60.0,
                "pixel_format": "yuv420p",
                "video_bitrate": 5000,
                "audio_codec": "aac",
                "sample_rate": 48000,
                "audio_channels": "stereo",
                "audio_bitrate": 128.0,
                "stream_type": "mpegts",
            });
        }
    }
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let outcome = run_action(context, false, engines(healthy_runner())).await;
    assert_eq!(outcome.counts.get("stats_unchanged"), Some(&1));
    assert_eq!(outcome.counts.get("stats_written"), Some(&3));
    assert!(fixture
        .bodies("PATCH", &format!("/api/channels/streams/{PROBE_FEED_ONE}/"))
        .await
        .is_empty());
}

#[tokio::test]
async fn an_unchanged_alive_stream_keeps_its_throughput_and_a_failed_measurement_replaces_it() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let now = Utc::now();
    for id in [PROBE_FEED_ONE, PROBE_FEED_TWO] {
        store
            .upsert_stream_health(&StreamHealth {
                stream_id: id,
                probed_at: None,
                class: String::new(),
                error_type: None,
                throughput_mbps: Some(25.0),
                throughput_at: Some(now.to_rfc3339_opts(SecondsFormat::Secs, true)),
                edge_ip: Some("edge.test".to_string()),
                consecutive_failures: 0,
            })
            .expect("row");
    }

    // Throughput on, and the meter measures feed one but fails feed two.
    let mut settings = settings();
    settings.enable_throughput_sorting = true;
    let meter = FakeMeter::new();
    meter.answering(
        PROBE_URL_ONE,
        ThroughputMeasure::measured(30.0, Some("edge2.test".to_string())),
    );
    // PROBE_URL_TWO has no answer, so it measures nothing (a failed probe).
    let engines = ProbeEngines {
        tools: std::sync::Arc::new(healthy_runner()),
        meter: std::sync::Arc::new(meter),
    };
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);

    run_with(&StreamsApp::new(), &context, &settings, false, engines)
        .await
        .expect("probe");

    let one = store
        .stream_health(PROBE_FEED_ONE)
        .expect("row")
        .expect("some");
    assert_eq!(one.throughput_mbps, Some(30.0));
    assert_eq!(one.edge_ip.as_deref(), Some("edge2.test"));
    assert_eq!(one.class, "alive");

    let two = store
        .stream_health(PROBE_FEED_TWO)
        .expect("row")
        .expect("some");
    assert_eq!(
        two.throughput_mbps, None,
        "a failed measurement replaces the old one, so it is re-probed next time"
    );
    assert_eq!(two.class, "alive");
}

#[tokio::test]
async fn a_cancelled_probe_keeps_what_it_did_and_checkpoints_the_rest() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let run_id = context.run_id().expect("run");
    context.cancel();
    let outcome = run_action(context, false, engines(healthy_runner())).await;

    assert!(outcome.is_cancelled(), "{}", outcome.summary);
    assert_eq!(outcome.counts.get("probed"), Some(&0));
    assert!(outcome.summary.contains("cancelled by the operator"));
    // The whole queue is kept for the next run.
    let checkpoint = store
        .load_checkpoint(APP_ID, ACTION)
        .expect("checkpoint")
        .expect("saved");
    assert_eq!(checkpoint["remaining"].as_array().expect("ids").len(), 4);
    assert!(checkpoint["fingerprint"].is_string());
    // Nothing was written to Dispatcharr, and no health row claims a verdict.
    assert!(fixture
        .bodies("PATCH", "/api/channels/streams/101/")
        .await
        .is_empty());
    assert_eq!(store.stream_health(PROBE_FEED_ONE).expect("row"), None);
    assert_eq!(store.rows(run_id).expect("rows").len(), 0);
}

#[tokio::test]
async fn a_closed_window_pauses_and_the_next_run_finishes_the_backlog() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();

    // First window: already closed, so nothing is probed and the queue is checkpointed.
    let context = fixture
        .context_for_run(ACTION_PROBE, &store, false)
        .with_deadline(Some(Utc::now() - chrono::Duration::seconds(1)));
    let outcome = run_action(context, false, engines(healthy_runner())).await;
    assert!(outcome.is_paused(), "{}", outcome.summary);
    let checkpoint = store
        .load_checkpoint(APP_ID, ACTION)
        .expect("checkpoint")
        .expect("saved");
    let remaining: Vec<i64> = checkpoint["remaining"]
        .as_array()
        .expect("ids")
        .iter()
        .filter_map(Value::as_i64)
        .collect();
    assert_eq!(remaining.len(), 4);

    // The host records the paused run before the next one starts.
    let run_id = store.recent_runs_for(APP_ID, 1).expect("runs")[0].id;
    store
        .finish_run(run_id, RunStatus::Paused, Some("paused"), &json!({}), None)
        .expect("finish");

    // Second window: resumes exactly what the checkpoint holds.
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let outcome = run_action(context, false, engines(healthy_runner())).await;
    assert!(!outcome.is_paused(), "{}", outcome.summary);
    assert_eq!(outcome.counts.get("probed"), Some(&4));
    assert_eq!(
        store.load_checkpoint(APP_ID, ACTION).expect("checkpoint"),
        None
    );
}

#[tokio::test]
async fn a_resumed_run_probes_only_what_the_checkpoint_left() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let settings = settings();
    let fingerprint = probe_fingerprint(&settings, &fixture.config.scope);
    let run_id = store.start_run(APP_ID, ACTION_PROBE, false).expect("run");
    store
        .save_checkpoint(
            APP_ID,
            ACTION_PROBE,
            &json!({"remaining": [PROBE_FEED_THREE], "fingerprint": fingerprint}),
        )
        .expect("checkpoint");
    store
        .finish_run(run_id, RunStatus::Paused, Some("paused"), &json!({}), None)
        .expect("finish");

    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let outcome = run_action(context, false, engines(healthy_runner())).await;
    assert_eq!(outcome.counts.get("probed"), Some(&1));
    assert_eq!(
        store
            .stream_health(PROBE_FEED_THREE)
            .expect("row")
            .expect("some")
            .class,
        "alive"
    );
    // The streams the checkpoint does not hold were already done: nothing new is claimed.
    assert_eq!(store.stream_health(PROBE_FEED_ONE).expect("row"), None);
}

#[tokio::test]
async fn a_settings_change_since_the_checkpoint_starts_a_fresh_pass() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let run_id = store.start_run(APP_ID, ACTION_PROBE, false).expect("run");
    store
        .save_checkpoint(
            APP_ID,
            ACTION_PROBE,
            &json!({"remaining": [PROBE_FEED_THREE], "fingerprint": "stale"}),
        )
        .expect("checkpoint");
    store
        .finish_run(run_id, RunStatus::Paused, Some("paused"), &json!({}), None)
        .expect("finish");

    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let outcome = run_action(context, false, engines(healthy_runner())).await;
    assert_eq!(outcome.counts.get("probed"), Some(&4), "a fresh pass");
}

#[tokio::test]
async fn a_run_with_nothing_due_says_so_and_clears_the_checkpoint() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let now = Utc::now();
    for stream in &world.streams {
        store
            .upsert_stream_health(&StreamHealth {
                stream_id: stream.id,
                probed_at: Some(
                    (now - chrono::Duration::minutes(5)).to_rfc3339_opts(SecondsFormat::Secs, true),
                ),
                class: "alive".to_string(),
                ..StreamHealth::default()
            })
            .expect("row");
    }
    let run_id = store.start_run(APP_ID, ACTION_PROBE, false).expect("run");
    store
        .save_checkpoint(APP_ID, ACTION_PROBE, &json!({"remaining": [1]}))
        .expect("checkpoint");
    store
        .finish_run(run_id, RunStatus::Succeeded, Some("done"), &json!({}), None)
        .expect("finish");

    let mut settings = settings();
    settings.probe_freshness_hours = 24;
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let outcome = run_with(
        &StreamsApp::new(),
        &context,
        &settings,
        false,
        engines(healthy_runner()),
    )
    .await
    .expect("probe");
    assert!(
        outcome.summary.starts_with("Nothing to probe"),
        "{}",
        outcome.summary
    );
    assert_eq!(outcome.counts.get("skipped_fresh"), Some(&4));
    assert_eq!(
        store.load_checkpoint(APP_ID, ACTION).expect("checkpoint"),
        None
    );
    assert!(fixture.server.received_requests().await.is_some());
}

#[tokio::test]
async fn a_tool_that_cannot_start_aborts_the_run_instead_of_killing_every_stream() {
    // IPTV Checker classified a spawn failure as `Other`/Dead for every stream, which cleared
    // every stream's stats. A host fault has to stop the run instead.
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let engines = engines(runner().failing("ffprobe not found at /usr/local/bin/ffprobe"));
    let error = run_with(&StreamsApp::new(), &context, &settings(), false, engines)
        .await
        .expect_err("the run fails");
    assert!(
        format!("{error:#}").contains("ffprobe not found"),
        "{error:#}"
    );
}

#[tokio::test]
async fn a_channel_out_of_scope_is_never_probed() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let mut config = fixture.config.clone();
    config.scope.groups = "Cable".to_string();
    let context = JobContext::new(
        std::sync::Arc::new(config),
        std::sync::Arc::new(crosstalk_core::jobs::VecSink::new()),
    )
    .with_dispatcharr(fixture.client())
    .for_run(
        APP_ID,
        ACTION_PROBE,
        store.start_run(APP_ID, ACTION_PROBE, false).expect("run"),
        store.clone(),
        std::sync::Arc::new(AtomicBool::new(false)),
    );
    let outcome = run_action(context, false, engines(healthy_runner())).await;
    assert_eq!(outcome.counts.get("selected"), Some(&1), "Cable only");
    assert_eq!(outcome.counts.get("probed"), Some(&1));
    assert_eq!(store.stream_health(PROBE_FEED_ONE).expect("row"), None);
}

#[tokio::test]
async fn a_channel_whose_only_stream_is_skipped_is_never_dead() {
    let world = probe_world();
    let fixture = ServerFixture::start(&world).await;
    let store = fixture.store();
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let run_id = context.run_id().expect("run");
    let engines = engines(
        runner()
            .answering(
                "ffprobe",
                PROBE_URL_ONE,
                dead("http://provider-a.test/live/one.ts: Server returned 429\n"),
            )
            .answering("ffprobe", PROBE_URL_TWO, alive(1280, 720, "50/1"))
            .answering("ffprobe", PROBE_URL_THREE, alive(720, 576, "25/1"))
            .answering("ffprobe", PROBE_URL_HIDDEN, alive(1280, 720, "50/1")),
    );
    run_action(context, false, engines).await;

    let rows = store.rows(run_id).expect("rows");
    let verdict = rows
        .iter()
        .find(|row| row.field == VERDICT_FIELD && row.channel_id == PROBE_CHANNEL)
        .expect("verdict");
    assert_eq!(verdict.after_value.as_deref(), Some("alive"));
    // The 429 stream itself is Skipped in the store, so a filter never calls it dead.
    let health = store
        .stream_health(PROBE_FEED_ONE)
        .expect("row")
        .expect("some");
    assert_eq!(health.class, "skipped");
}

/// A stream object the way the store and the sort see it.
#[test]
fn a_verdict_carries_the_url_masked_error_and_the_plugins_wording() {
    let verdict = StreamVerdict::new(
        &ProbeTarget {
            stream_id: 3,
            stream_name: "ESPN",
            account: 1,
            url: "http://provider.test/x?token=secret",
        },
        Class::Dead,
        "404 Not Found",
    );
    assert_eq!(verdict.health_class, "dead");
    assert_eq!(verdict.class.label(), "Dead");
    assert_eq!(Class::parse("dead"), Some(Class::Dead));
    assert_eq!(
        Stream::default().url,
        None,
        "a stream with no URL is what the selection skips"
    );
}
