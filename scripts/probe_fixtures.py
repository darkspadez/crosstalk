#!/usr/bin/env python3
"""Record the probe's fixtures from the REAL ffprobe and ffmpeg.

This is how `crates/streams/tests/fixtures/probe/corpus.json` is made. It generates synthetic
media with ffmpeg's `lavfi` sources, serves it from a throwaway local HTTP server (127.0.0.1, an
ephemeral port) and records what ffprobe and ffmpeg actually print when the probe's exact command
lines are run against it — covering the cases the parsers care about:

  * an alive MPEG-TS feed (HD, SD, UHD), the case the media facts come from;
  * a live HLS feed, the case with NO bitrate in the stream or the container, which is why the
    packet fallback (30+ packets) exists at all;
  * an audio-only stream (classified Skipped, not Dead);
  * a black screen, a frozen picture and a silent audio track (the one ffmpeg content pass);
  * a fixed-duration file (the placeholder-file fingerprint: a live stream reports no duration);
  * the provider failures: 404, 403, 429, 500, connection refused, timeout, invalid data.

Two things about the output:

  * **the media is not stored.** Only what the tools printed is: the fixture is text. The script
    regenerates the media on demand, so nothing large is committed.
  * **the `stdout` is a PROJECTION of ffprobe's JSON**, keeping every field the parser reads
    (`streams[].codec_type/codec_name/width/height/pix_fmt/r_frame_rate/bit_rate/sample_rate/
    channels/channel_layout/index`, `format.format_name/duration/bit_rate`, and each packet's
    `stream_index/size/duration_time`). A full `-show_packets` document for 8 s of video is a few
    hundred kilobytes per case; the projection is a few, and every VALUE in it is what the tool
    really printed. `--full` keeps the documents whole for a one-off inspection.

Usage:
    python3 -I scripts/probe_fixtures.py [--out DIR] [--full] [--keep]

Requires ffmpeg and ffprobe on PATH (the image has them; /opt/homebrew/bin on macOS). It needs no
network: every URL is a local server or a closed local port.
"""
from __future__ import annotations

import argparse
import json
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DEFAULT_OUT = REPO / "crates" / "streams" / "tests" / "fixtures" / "probe"

# The probe's constants, from IPTV Checker's `check_stream` (5014-5057) and `_analyze_stream_content`
# (4878-4895). Written out here so this script records against the same command lines the Rust
# builds, without importing the plugin (it generates fixtures; `probe_parity.py` is what compares).
USER_AGENT = "VLC/3.0.21 LibVLC/3.0.21"
PROBESIZE = 10_000_000
# crosstalk's hardening (README, Differences): network protocols only, the URL always `-i`.
PROTOCOL_WHITELIST = "http,https,tls,tcp,udp,rtp,rtmp,rtmps,rtsp,srt,crypto,hls,httpproxy"
ANALYZE_SECONDS = 8
TIMEOUT_BUFFER_SECONDS = 5

# Every field the parser reads, and nothing else (see the module docstring).
STREAM_FIELDS = ("index", "codec_type", "codec_name", "width", "height", "pix_fmt",
                 "r_frame_rate", "bit_rate", "sample_rate", "channels", "channel_layout")
FORMAT_FIELDS = ("format_name", "duration", "bit_rate")
PACKET_FIELDS = ("stream_index", "size", "duration_time")
# Enough that the 30-VIDEO-packet floor is reached after the projection (the document
# interleaves audio and video), so the packet-derived bitrate is exercised rather than skipped.
MAX_PACKETS = 80


# ---------------------------------------------------------------------------------------
# Media, made with ffmpeg's own sources
# ---------------------------------------------------------------------------------------

def build_media(directory: Path, ffmpeg: str) -> dict[str, Path]:
    """Generate the synthetic library. Short clips: the fixtures only need the shape."""
    files: dict[str, Path] = {}

    def run(name: str, args: list[str]) -> None:
        path = directory / name
        completed = subprocess.run([ffmpeg, "-hide_banner", "-loglevel", "error", "-y", *args,
                                    str(path)], capture_output=True, text=True)
        if completed.returncode != 0:
            raise SystemExit(f"ffmpeg could not build {name}:\n{completed.stderr[-3000:]}")
        files[name] = path

    # A live-looking feed: moving picture plus a tone, H.264 in MPEG-TS.
    def feed(name: str, size: str, rate: str, seconds: str, extra: list[str] | None = None) -> None:
        run(name, [
            "-f", "lavfi", "-i", f"testsrc2=size={size}:rate={rate}:duration={seconds}",
            "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate=48000:duration={seconds}",
            "-c:v", "libx264", "-preset", "ultrafast", "-b:v", "600k", "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "64k", "-shortest",
            *(extra or []),
            "-f", "mpegts",
        ])

    feed("alive_hd.ts", "1920x1080", "50", "2")
    feed("alive_sd.ts", "720x576", "25", "2")
    feed("alive_uhd.ts", "3840x2160", "25", "1")
    # Below the plugin's 24 fps threshold, so the low-framerate rename has a fixture.
    feed("alive_slow.ts", "640x360", "15", "2")
    # A picture that never changes, but is NOT black (red luma is well above the black threshold):
    # freezedetect fires, blackdetect must not.
    run("frozen.ts", [
        "-f", "lavfi", "-i", "color=c=red:size=640x360:rate=25:duration=6",
        "-f", "lavfi", "-i", "sine=frequency=440:duration=6",
        "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-shortest", "-f", "mpegts",
    ])
    # A pure black screen with a tone: blackdetect AND freezedetect fire, which is exactly the
    # precedence the content pass has to resolve.
    run("black.ts", [
        "-f", "lavfi", "-i", "color=c=black:size=640x360:rate=25:duration=6",
        "-f", "lavfi", "-i", "sine=frequency=440:duration=6",
        "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-shortest", "-f", "mpegts",
    ])
    # Moving picture, digital silence: volumedetect reports about -91 dB.
    run("silent.ts", [
        "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=25:duration=4",
        "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
        "-t", "4", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-f", "mpegts",
    ])
    # A finite FILE: the provider placeholder fingerprint is the container duration, and MPEG-TS
    # does not report one (a live TS and a TS file look identical), so the fixture is an MP4 —
    # which is also the shape the plugin's measured placeholder had (a 10-minute file reused
    # under many content ids).
    run("slate.mp4", [
        "-f", "lavfi", "-i", "color=c=black:size=1920x1080:rate=25:duration=3",
        "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
        "-an", "-f", "mp4",
    ])
    # Audio only: a radio station, which the plugin classifies Skipped.
    run("radio.mp3", [
        "-f", "lavfi", "-i", "sine=frequency=440:duration=2", "-c:a", "libmp3lame", "-b:a", "64k",
        "-f", "mp3",
    ])
    # Moving picture with NO audio track: the silent detector must be dropped for it (there is
    # nothing to measure), which the content pass expresses as `-an` instead of `-af
    # volumedetect`.
    run("video_only.ts", [
        "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=25:duration=4",
        "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
        "-an", "-f", "mpegts",
    ])
    # Anything that is not media at all: "invalid data found".
    text = directory / "not-media.ts"
    text.write_text("this is not a transport stream\n")
    files["not-media.ts"] = text

    # A live HLS feed: no per-stream bitrate and no container bitrate, so the packet fallback is
    # what gives it one.
    hls = directory / "hls"
    hls.mkdir()
    completed = subprocess.run([
        ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=25:duration=4",
        "-f", "lavfi", "-i", "sine=frequency=440:duration=4",
        "-c:v", "libx264", "-preset", "ultrafast", "-b:v", "600k", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "64k", "-shortest",
        "-f", "hls", "-hls_time", "2", "-hls_list_size", "0",
        "-hls_segment_filename", str(hls / "seg%d.ts"), str(hls / "playlist.m3u8"),
    ], capture_output=True, text=True)
    if completed.returncode != 0:
        raise SystemExit(f"ffmpeg could not build the HLS feed:\n{completed.stderr[-3000:]}")
    files["hls/playlist.m3u8"] = hls / "playlist.m3u8"
    for index, segment in enumerate(sorted(hls.glob("seg*.ts"))):
        files[f"hls/seg{segment.name.removeprefix('seg')}"] = segment
    return files


# ---------------------------------------------------------------------------------------
# A local HTTP server: media, status codes and one slow endpoint
# ---------------------------------------------------------------------------------------

class Server:
    """A throwaway provider: serves the media, answers status codes, and stalls once."""

    def __init__(self, root: Path):
        self.root = root
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *args, **kwargs):  # keep the output about mismatches only
                pass

            def do_GET(self):  # noqa: N802 (http.server's naming)
                outer.handle(self)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def handle(self, handler: BaseHTTPRequestHandler) -> None:
        path = handler.path.split("?")[0]
        if path.startswith("/status/"):
            code = int(path.rsplit("/", 1)[1])
            body = f"status {code}\n".encode()
            handler.send_response(code)
            handler.send_header("Content-Type", "text/plain")
            handler.send_header("Content-Length", str(len(body)))
            handler.end_headers()
            handler.wfile.write(body)
            return
        if path == "/slow":
            # The read timeout's fixture: accept, then stall past ffprobe's `-timeout`.
            time.sleep(30)
            return
        target = (self.root / path.lstrip("/")).resolve()
        if not target.is_file() or self.root.resolve() not in target.parents:
            handler.send_response(404)
            handler.end_headers()
            return
        body = target.read_bytes()
        handler.send_response(200)
        handler.send_header("Content-Type", "application/octet-stream")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


# ---------------------------------------------------------------------------------------
# The command lines, as the probe builds them
# ---------------------------------------------------------------------------------------

def ffprobe_command(ffprobe: str, url: str, timeout: int, probe_timeout: int,
                    analyze: int) -> list[str]:
    return [
        ffprobe,
        "-print_format", "json",
        "-user_agent", USER_AGENT,
        "-timeout", str(timeout * 1_000_000),
        "-analyzeduration", str(probe_timeout * 1_000_000),
        "-probesize", str(PROBESIZE),
        "-loglevel", "error",
        "-show_streams",
        "-show_packets",
        "-show_format",
        "-read_intervals", f"%+{analyze}",
        "-protocol_whitelist", PROTOCOL_WHITELIST,
        "-i", url,
    ]


def ffmpeg_command(ffmpeg: str, url: str, timeout: int, settings: dict) -> list[str]:
    sample = int(settings.get("black_screen_sample_seconds", 6))
    min_black = int(settings.get("black_screen_min_black_seconds", 3))
    want_freeze = bool(settings.get("frozen_video_detection", False))
    want_audio = bool(settings.get("silent_audio_detection", False))
    filters = [f"blackdetect=d={min_black}:pic_th=0.98"]
    if want_freeze:
        wanted = int(settings.get("frozen_video_min_seconds", 4))
        # `_effective_freeze_seconds`: a window at or beyond the sample can never mature.
        frozen = max(1, min(wanted, sample - 1))
        filters.append(f"freezedetect=n=-60dB:d={frozen}")
    command = [
        ffmpeg,
        "-hide_banner", "-nostats", "-loglevel", "info",
        "-user_agent", USER_AGENT,
        "-rw_timeout", str(timeout * 1_000_000),
        "-protocol_whitelist", PROTOCOL_WHITELIST,
        "-i", url,
        "-t", str(sample),
    ]
    if want_audio:
        command.extend(["-af", "volumedetect"])
    else:
        command.append("-an")
    command.extend(["-vf", ",".join(filters), "-f", "null", "-"])
    return command


def run(command: list[str], timeout: float) -> dict:
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        return {"returncode": completed.returncode, "stdout": completed.stdout,
                "stderr": completed.stderr}
    except subprocess.TimeoutExpired:
        return {"returncode": None, "timed_out": True, "stdout": "", "stderr": ""}


# ---------------------------------------------------------------------------------------
# Projection
# ---------------------------------------------------------------------------------------

def project_stdout(stdout: str) -> str:
    """ffprobe's JSON, keeping the fields the parser reads (see the module docstring)."""
    try:
        document = json.loads(stdout)
    except ValueError:
        return stdout
    trimmed: dict = {}
    if isinstance(document.get("streams"), list):
        trimmed["streams"] = [
            {key: stream[key] for key in STREAM_FIELDS if key in stream}
            for stream in document["streams"]
        ]
    if isinstance(document.get("format"), dict):
        trimmed["format"] = {
            key: document["format"][key] for key in FORMAT_FIELDS if key in document["format"]
        }
    if isinstance(document.get("frames"), list):
        trimmed["frames"] = document["frames"][:1]
    if isinstance(document.get("packets"), list):
        trimmed["packets"] = [
            {key: packet[key] for key in PACKET_FIELDS if key in packet}
            for packet in document["packets"][:MAX_PACKETS]
        ]
    return json.dumps(trimmed, indent=1)


# ---------------------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------------------

def corpus_cases(base: str, refused_url: str) -> list[dict]:
    """Every case: what to fetch, what the settings are, and whether the content pass runs."""
    timeout_settings = {"timeout": 10, "probe_timeout": 20}
    quiet = {"black_screen_detection": False, "frozen_video_detection": False,
             "silent_audio_detection": False, "placeholder_file_detection": False}
    detectors = {"black_screen_detection": True, "frozen_video_detection": True,
                 "silent_audio_detection": True, "black_screen_sample_seconds": 6,
                 "black_screen_min_black_seconds": 3, "frozen_video_min_seconds": 4,
                 "silent_audio_max_db": -70, "black_screen_ffmpeg_timeout": 30}
    cases: list[dict] = []

    def add(name: str, url: str, settings: dict, content: bool, note: str) -> None:
        cases.append({
            "name": name,
            "url": url,
            "note": note,
            "settings": {
                **timeout_settings,
                **quiet,
                **settings,
                # Fixed in the build, but the plugin takes them from settings: pass the values
                # the Rust uses so the two command lines can be compared argument for argument.
                "ffprobe_analysis_duration": ANALYZE_SECONDS,
                "ffprobe_flags": "-show_streams,-show_packets,-loglevel error",
                "ffprobe_path": "ffprobe",
                "ffmpeg_path": "ffmpeg",
                "streamlink_hosts": "",
            },
            "content": content,
        })

    add("alive_hd", f"{base}/alive_hd.ts", {}, False,
        "an alive HD MPEG-TS feed: stream bit_rate present")
    add("alive_sd", f"{base}/alive_sd.ts", {}, False,
        "an alive SD feed: the format ladder's HD/SD boundary from a real width")
    add("alive_uhd", f"{base}/alive_uhd.ts", {}, False, "an alive 4K feed")
    add("alive_slow", f"{base}/alive_slow.ts", {}, False,
        "an alive feed below 24 fps: the low-framerate rename's fixture")
    add("hls_live", f"{base}/hls/playlist.m3u8", {}, False,
        "a live HLS feed: no bitrate anywhere, so the 30-packet fallback supplies one")
    add("radio", f"{base}/radio.mp3", {}, False,
        "audio only: exit 0 with no video stream, which the plugin calls Skipped")
    add("video_only_content", f"{base}/video_only.ts", detectors, True,
        "no audio track with every detector armed: silence is never claimed and the pass "
        "decodes no audio")
    add("radio_content", f"{base}/radio.mp3", detectors, True,
        "detectors armed on an audio-only stream: not alive, so no content pass runs")
    add("alive_hd_content", f"{base}/alive_hd.ts", detectors, True,
        "all three detectors armed on a healthy feed: one clean pass, no verdict")
    add("black", f"{base}/black.ts", detectors, True,
        "a black screen: blackdetect and freezedetect both fire, black must win")
    add("frozen", f"{base}/frozen.ts", {**detectors, "black_screen_detection": False}, True,
        "a still picture that is not black: only freezedetect fires")
    add("silent", f"{base}/silent.ts",
        {**detectors, "black_screen_detection": False, "frozen_video_detection": False}, True,
        "a moving picture with a silent track: volumedetect's -91 dB")
    add("placeholder", f"{base}/slate.mp4", {**detectors, "placeholder_file_detection": True},
        True, "a finite file: the container duration is the placeholder fingerprint")
    add("placeholder_off", f"{base}/slate.mp4", {}, False,
        "the same file with the detector off: Alive, with the duration recorded as evidence")

    date = {"timeout": 10, "probe_timeout": 20}
    add("not_found", f"{base}/status/404", date, False, "HTTP 404")
    add("forbidden", f"{base}/status/403", date, False, "HTTP 403")
    add("rate_limited", f"{base}/status/429", date, False, "HTTP 429, which is never Dead")
    add("server_error", f"{base}/status/500", date, False, "HTTP 500")
    add("invalid_data", f"{base}/not-media.ts", date, False, "'invalid data found'")
    add("refused", refused_url, date, False, "connection refused by a closed local port")
    add("timeout", f"{base}/slow", {"timeout": 2, "probe_timeout": 2}, False,
        "a provider that accepts and stalls: ffprobe's own -timeout fires")
    return cases


def record(case: dict, ffprobe: str, ffmpeg: str) -> dict:
    # The corpus keeps the bare names (`ffprobe`, `ffmpeg`) so it is machine independent; the
    # recorded command lines are built with the resolved paths and then normalised back.
    settings = case["settings"]
    probe_timeout = int(settings.get("probe_timeout", 20))
    analyze = int(settings.get("ffprobe_analysis_duration", ANALYZE_SECONDS))
    command = ffprobe_command(ffprobe, case["url"], int(settings.get("timeout", 10)),
                              probe_timeout, analyze)
    recorded = run(command, probe_timeout + analyze + TIMEOUT_BUFFER_SECONDS + 10)
    if recorded.get("stdout") and not case.get("full"):
        recorded["stdout"] = project_stdout(recorded["stdout"])
    case["ffprobe"] = recorded
    if case["content"]:
        content_command = ffmpeg_command(ffmpeg, case["url"], int(settings.get("timeout", 10)),
                                         settings)
        case["ffmpeg"] = run(content_command, 60)
    else:
        case["ffmpeg"] = None
    return case


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--full", action="store_true",
                        help="keep ffprobe's whole JSON instead of the projection")
    parser.add_argument("--keep", action="store_true", help="keep the generated media")
    args = parser.parse_args()

    ffprobe = shutil.which("ffprobe")
    ffmpeg = shutil.which("ffmpeg")
    if not ffprobe or not ffmpeg:
        print("ffprobe and ffmpeg are both required (the image has them)", file=sys.stderr)
        return 2

    work = Path(tempfile.mkdtemp(prefix="probe-fixtures-"))
    try:
        print(f"building media in {work} ...", file=sys.stderr)
        build_media(work, ffmpeg)
        server = Server(work)
        try:
            # A closed port: `connect()` refuses, which is the "Connection refused" fixture.
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                closed_port = probe.getsockname()[1]
            cases = corpus_cases(server.base, f"http://127.0.0.1:{closed_port}/refused/live.ts")
            for case in cases:
                case["full"] = args.full
                print(f"  recording {case['name']} ...", file=sys.stderr)
                record(case, ffprobe, ffmpeg)
                case.pop("full", None)
        finally:
            server.close()
    finally:
        if args.keep:
            print(f"media kept in {work}", file=sys.stderr)
        else:
            shutil.rmtree(work, ignore_errors=True)

    # The port changes every run, so the recorded URLs are rewritten to one stable host: the
    # fixtures are committed, and two regenerations must differ only when the tools differ.
    for case in cases:
        case["url"] = stable_url(case["url"])
        # A recorded message can quote the resolved binary path; keep the corpus portable.
        for tool in ("ffprobe", "ffmpeg"):
            recorded = case.get(tool)
            if not recorded:
                continue
            for field in ("stdout", "stderr"):
                recorded[field] = (
                    stable_url(recorded[field])
                    .replace(ffprobe, "ffprobe")
                    .replace(ffmpeg, "ffmpeg")
                )

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    corpus = out / "corpus.json"
    corpus.write_text(json.dumps({
        "generator": "scripts/probe_fixtures.py",
        "note": "Recorded with the real ffprobe/ffmpeg against synthetic media served from a "
                "local HTTP server. See the generator's docstring for what is kept.",
        "tools": {"ffprobe": version(ffprobe), "ffmpeg": version(ffmpeg)},
        "channels": [
            {"id": 1, "name": "ESPN", "cases": ["alive_hd", "not_found"]},
            {"id": 2, "name": "ESPN2", "cases": ["alive_sd", "rate_limited"]},
            {"id": 3, "name": "HBO", "cases": ["not_found", "forbidden"]},
            {"id": 4, "name": "BLACK", "cases": ["black", "black"]},
            {"id": 5, "name": "MIXED", "cases": ["black", "not_found"]},
            {"id": 6, "name": "SLOW", "cases": ["alive_slow", "alive_slow"]},
            {"id": 7, "name": "ALLSKIPPED", "cases": ["rate_limited", "radio"]},
            {"id": 8, "name": "NOAUDIO", "cases": ["video_only_content", "radio_content"]},
        ],
        "cases": cases,
    }, indent=1) + "\n")
    total = sum(len(case["ffprobe"]["stdout"]) + len(case["ffprobe"]["stderr"])
                + (len(case["ffmpeg"]["stderr"]) if case.get("ffmpeg") else 0)
                for case in cases)
    print(f"wrote {corpus.relative_to(REPO)}: {len(cases)} case(s), {total // 1024} KiB of tool "
          f"output", file=sys.stderr)
    return 0


def version(binary: str) -> str:
    completed = subprocess.run([binary, "-version"], capture_output=True, text=True)
    return completed.stdout.splitlines()[0] if completed.stdout else "unknown"


def stable_url(text: str) -> str:
    """Rewrite `http://127.0.0.1:<port>` to one stable host, so the corpus is reproducible."""
    import re

    return re.sub(r"http://127\.0\.0\.1:\d+", "http://probe.test", text)


if __name__ == "__main__":
    sys.exit(main())
