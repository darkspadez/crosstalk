//! The data-at-rest guarantee, end to end (audit C-B1, C-S4).
//!
//! A provider stream URL is a credential (`/live/<user>/<pass>/<id>.ts`, `?token=`,
//! `user:pass@`), so nothing crosstalk keeps may carry more of one than its host. This drives
//! every streams action that reads or writes stream data (probe, match, mirror, sort, triage,
//! restore) against one run store on disk, with the tools' stderr and a Dispatcharr error body
//! echoing credentialed URLs back, and then reads every byte left under the configuration
//! directory: the SQLite file and its WAL (every table, including freed pages) and every CSV.

use crate::probe::throughput::FakeMeter;
use crate::probe::tool::{FakeToolRunner, ToolOutput};
use crate::probe::{self, ProbeEngines};
use crate::settings::StreamsSettings;
use crate::test_support::{probe_world, ServerFixture, PROBE_FEED_ONE, PROBE_FEED_TWO};
use crate::{
    StreamsApp, ACTION_MATCH_APPLY, ACTION_MIRROR_APPLY, ACTION_PROBE, ACTION_RESTORE_APPLY,
    ACTION_SORT_APPLY, ACTION_TRIAGE_APPLY,
};
use anyhow::Result;
use crosstalk_core::store::{RunStatus, Store, Subject};
use crosstalk_core::subapp::{JobOutcome, SubApp};
use crosstalk_core::World;
use serde_json::json;
use std::path::{Path, PathBuf};
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

/// What must never be stored: the credential parts of the URLs below, and any URL path.
const MARKERS: [&str; 5] = ["USERMARK", "PASSMARK", "TOKENMARK", "token=", "/live/"];

/// A stream URL carrying a credential in each place providers put one.
fn secret_url(stream_id: i64) -> String {
    format!(
        "http://USERMARK:PASSMARK@provider-a.test/live/USERMARK/PASSMARK/{stream_id}.ts\
         ?token=TOKENMARK"
    )
}

/// The probe world with every stream and account URL credentialed.
fn secret_world() -> World {
    let mut world = probe_world();
    for stream in &mut world.streams {
        stream.url = Some(secret_url(stream.id));
    }
    for account in &mut world.m3u_accounts {
        account.server_url = Some(
            "http://USERMARK:PASSMARK@provider-a.test/get.php?username=USERMARK\
             &password=PASSMARK&token=TOKENMARK"
                .to_string(),
        );
    }
    world
}

fn alive() -> ToolOutput {
    let stdout = json!({
        "streams": [
            {"index": 0, "codec_name": "h264", "codec_type": "video", "width": 1920,
             "height": 1080, "pix_fmt": "yuv420p", "r_frame_rate": "50/1",
             "bit_rate": "5000000"},
            {"index": 1, "codec_name": "aac", "codec_type": "audio", "sample_rate": "48000",
             "channels": 2, "bit_rate": "128000"}
        ],
        "format": {"format_name": "mpegts"}
    })
    .to_string();
    ToolOutput::new(0, stdout, "")
}

/// What ffprobe prints when a stream fails part-way through an HLS playlist: the probed URL,
/// a variant and a segment on another host, a redirect target, and the URL-encoded spelling.
fn dead_stderr(url: &str) -> String {
    format!(
        "[hls @ 0x1] Opening 'http://edge.provider-a.test:8080/hls/USERMARK/PASSMARK/v1.m3u8\
         ?token=TOKENMARK' for reading\n\
         [http @ 0x2] redirected to http://USERMARK:PASSMARK@cdn.provider-a.test/x?token=TOKENMARK\n\
         [hls @ 0x1] Failed to open segment http://edge.provider-a.test:8080/hls/USERMARK/\
         PASSMARK/seg-1.ts?token=TOKENMARK\n\
         request http%3A%2F%2FUSERMARK%3APASSMARK%40provider-a.test%2Flive%2FUSERMARK\
         %2FPASSMARK%2F1.ts%3Ftoken%3DTOKENMARK failed\n\
         {url}: Server returned 403 Forbidden (access denied)"
    )
}

fn engines(world: &World) -> ProbeEngines {
    let mut tools = FakeToolRunner::new();
    for stream in &world.streams {
        let url = stream.url.clone().unwrap_or_default();
        let output = if stream.id == PROBE_FEED_ONE {
            ToolOutput::new(1, "", dead_stderr(&url))
        } else {
            alive()
        };
        tools = tools.answering("ffprobe", &url, output);
    }
    ProbeEngines {
        tools: std::sync::Arc::new(tools),
        meter: std::sync::Arc::new(FakeMeter::new()),
    }
}

fn probe_settings() -> StreamsSettings {
    StreamsSettings {
        stream_check_delay: 0,
        probe_rate_per_minute: 600,
        probe_freshness_hours: 0,
        enable_throughput_sorting: false,
        ..StreamsSettings::default()
    }
}

/// Record how a run ended, the way the host's runner does (a failure keeps its message).
fn finish(store: &Store, run_id: i64, result: &Result<JobOutcome>) {
    match result {
        Ok(outcome) => store
            .finish_run(
                run_id,
                RunStatus::Succeeded,
                Some(&outcome.summary),
                &serde_json::to_value(&outcome.counts).expect("counts"),
                outcome
                    .report_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .as_deref(),
            )
            .expect("finish"),
        Err(error) => store
            .finish_run(
                run_id,
                RunStatus::Failed,
                Some(&format!("{error:#}")),
                &json!({}),
                None,
            )
            .expect("finish"),
    }
}

fn files_under(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).expect("readable directory") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            files_under(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[tokio::test]
async fn no_streams_action_leaves_a_provider_url_at_rest() {
    let world = secret_world();
    let fixture = ServerFixture::start(&world).await;
    // A stats write that fails with the stream object echoed back, URL and all (C-S1).
    Mock::given(method("PATCH"))
        .and(path(format!("/api/channels/streams/{PROBE_FEED_TWO}/")))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "id": PROBE_FEED_TWO,
            "url": secret_url(PROBE_FEED_TWO),
            "detail": format!("invalid stats for {}", secret_url(PROBE_FEED_TWO)),
        })))
        .with_priority(1)
        .mount(&fixture.server)
        .await;
    let store = Store::open(fixture.config.data_dir().join("crosstalk.db")).expect("store");
    let app = StreamsApp::new();

    // The probe, with fake tools.
    let context = fixture.context_for_run(ACTION_PROBE, &store, false);
    let probe_run = context.run_id().expect("run");
    let probed = probe::run_with(&app, &context, &probe_settings(), false, engines(&world)).await;
    finish(&store, probe_run, &probed);
    let probed = probed.expect("the probe runs");
    assert!(probed.report_path.is_some(), "the probe wrote its CSV");
    let rows = store.rows(probe_run).expect("rows");
    let stream_row = |id: i64| {
        rows.iter()
            .find(|row| row.subject == Subject::Stream && row.channel_id == id)
            .unwrap_or_else(|| panic!("a row for stream {id}: {rows:?}"))
    };
    let dead = stream_row(PROBE_FEED_ONE);
    assert!(dead.reason.contains("[Stream ID: 101]"), "{}", dead.reason);
    assert!(
        dead.reason.contains("http://edge.provider-a.test:8080"),
        "the segment's host is kept: {}",
        dead.reason
    );
    assert!(
        stream_row(PROBE_FEED_TWO)
            .reason
            .contains("stats write failed"),
        "the echoed error body reached the row: {:?}",
        stream_row(PROBE_FEED_TWO)
    );

    // Everything after it, on the same store, each as the host would record it.
    let mut failed = Vec::new();
    for action in [
        ACTION_MATCH_APPLY,
        ACTION_MIRROR_APPLY,
        ACTION_SORT_APPLY,
        ACTION_TRIAGE_APPLY,
        ACTION_RESTORE_APPLY,
    ] {
        let context = fixture.context_for_run(action, &store, false);
        let run_id = context.run_id().expect("run");
        let result = app.run(action, &context, false).await;
        finish(&store, run_id, &result);
        if let Err(error) = &result {
            failed.push(format!("{action}: {error:#}"));
        }
    }
    // Not the subject here, but a failure would mean less was exercised: say so.
    if !failed.is_empty() {
        eprintln!("actions that failed (their errors are stored and checked too): {failed:?}");
    }

    // Every byte at rest: the run store (all tables, its WAL) and every report.
    let mut files = Vec::new();
    files_under(&fixture.config.config_dir, &mut files);
    let mut everything = Vec::new();
    let mut reports = 0;
    for file in &files {
        let bytes = std::fs::read(file).expect("readable file");
        for marker in MARKERS {
            assert!(
                !contains(&bytes, marker.as_bytes()),
                "{marker:?} is stored in {}",
                file.display()
            );
        }
        if file.extension().is_some_and(|extension| extension == "csv") {
            reports += 1;
        }
        everything.extend(bytes);
    }
    assert!(reports >= 1, "no report was written: {files:?}");
    // The control: the scan does see stored row text, so the absence above means something.
    assert!(
        contains(&everything, b"[Stream ID: 101]"),
        "the masked error is stored where the scan can see it"
    );
    assert!(contains(&everything, b"edge.provider-a.test"));
}
