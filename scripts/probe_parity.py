#!/usr/bin/env python3
"""Parity: crosstalk's probe against IPTV Checker's own `check_stream`, field for field.

The probe is IPTV Checker's stream check, ported. This is how that claim is checked: it imports
the plugin's `plugin.py` from `reference/` (with the Django stubs every harness in this repo
installs), replaces `subprocess.run` with a recorder that replays outputs captured from the REAL
ffprobe and ffmpeg (`scripts/probe_fixtures.py`), and drives:

  * `Plugin.check_stream` — the command lines it builds, the JSON parse, the bitrate fallbacks,
    the audio layout mapping, the stream type, the container evidence, the placeholder check,
    the one ffmpeg content pass, the error classification and the URL masking;
  * `Plugin._analyze_stream_content` — reached through `check_stream`, and separately for the
    fail-open cases;
  * `Plugin._channels_where` with `_is_dead_nonblack` / `_is_black_screen` / `_is_low_framerate_row`
    — the per-channel verdicts `triage` will act on;
  * `Plugin._update_dispatcharr_metadata`'s two rules (all-null clears the stats, otherwise the
    nulls are dropped), applied to the metadata `check_stream` returned.

The Rust side is `cargo run -p crosstalk-streams --example probe_io`, which replays the same
recordings through the port. Nothing of the Rust is mirrored here: the plugin's own code decides
the expected answer.

Two cases are constructed rather than recorded, because they cannot be captured from a tool:

  * **process_timeout** — ffprobe runs past its cap. The plugin sees `subprocess.TimeoutExpired`;
    the example sees `timed_out: true`. Both must say `Timeout` with the same wording.
  * **streamlink** — a Streamlink-only host, where no subprocess runs at all.

Usage:
    python3 -I scripts/probe_parity.py [--show N] [--binary PATH]

Exit status is 1 when any field differs, so it can gate CI.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

# The harness imports the plugin in-process; don't leave __pycache__ behind.
sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parent.parent
PLUGIN_INNER = REPO / "reference" / "Dispatcharr-IPTV-Checker-Plugin" / "iptv_checker"
FIXTURES = REPO / "crates" / "streams" / "tests" / "fixtures" / "probe" / "corpus.json"
PKG = "iptv_checker_probe_parity"

# The plugin's own defaults, passed explicitly so both sides agree about what "unset" means.
DEFAULT_STREAMLINK_HOSTS = "youtube.com, youtu.be, twitch.tv, kick.com"
DEFAULT_FFPROBE_FLAGS = "-show_streams,-show_packets,-loglevel error"
ANALYZE_SECONDS = 8

# What `_update_dispatcharr_metadata` does with a metadata dict, cited: 5398-5450. An all-null
# dict clears `stream_stats` to `{}` (the dead path), otherwise the nulls are dropped and the
# rest merges. Applied to BOTH sides' metadata dicts, so the comparison is of the plugin's own
# dictionary, not of a reimplementation.
def metadata_effect(metadata: dict | None) -> dict:
    if not metadata:
        return {"kind": "untouched"}
    if all(value is None for value in metadata.values()):
        return {"kind": "clear"}
    return {"kind": "merge", "stats": {k: v for k, v in metadata.items() if v is not None}}


# ---------------------------------------------------------------------------------------
# Loading the plugin without Django
# ---------------------------------------------------------------------------------------

def load_plugin():
    import logging

    django = types.ModuleType("django")
    django_utils = types.ModuleType("django.utils")
    django_tz = types.ModuleType("django.utils.timezone")
    import datetime

    django_tz.now = lambda: datetime.datetime.now(datetime.timezone.utc)
    django_utils.timezone = django_tz
    django_db = types.ModuleType("django.db")
    django_db.transaction = MagicMock(name="transaction")
    django_db.models = MagicMock(name="models")
    sys.modules.update({
        "django": django,
        "django.utils": django_utils,
        "django.utils.timezone": django_tz,
        "django.db": django_db,
    })

    apps = types.ModuleType("apps")
    apps.__path__ = []
    apps_channels = types.ModuleType("apps.channels")
    apps_channels.__path__ = []
    models = types.ModuleType("apps.channels.models")
    for name in ("Channel", "ChannelGroup", "ChannelProfile",
                 "ChannelProfileMembership", "ChannelStream", "Stream"):
        setattr(models, name, MagicMock(name=name))
    sys.modules.update({
        "apps": apps,
        "apps.channels": apps_channels,
        "apps.channels.models": models,
    })

    core = types.ModuleType("core")
    core.__path__ = []
    core_utils = types.ModuleType("core.utils")
    core_utils.send_websocket_update = MagicMock(name="send_websocket_update")
    sys.modules.update({"core": core, "core.utils": core_utils})

    if str(PLUGIN_INNER) not in sys.path:
        sys.path.insert(0, str(PLUGIN_INNER))
    if PKG not in sys.modules:
        package = types.ModuleType(PKG)
        package.__path__ = [str(PLUGIN_INNER)]
        sys.modules[PKG] = package
    import importlib.util

    spec = importlib.util.spec_from_file_location(f"{PKG}.plugin", PLUGIN_INNER / "plugin.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"{PKG}.plugin"] = module
    spec.loader.exec_module(module)
    # The plugin logs per stream at INFO; the mismatch report is what matters here.
    logging.disable(logging.CRITICAL)
    return module


class Recorder:
    """`subprocess.run`, replaced: records the command and replays a recorded output."""

    def __init__(self, case: dict):
        self.case = case
        self.calls: list[list[str]] = []

    def __call__(self, command, **kwargs):
        self.calls.append(list(command))
        program = Path(command[0]).name
        if program == "ffprobe":
            return self._result(self.case["ffprobe"])
        if program == "ffmpeg":
            recorded = self.case.get("ffmpeg")
            if recorded is None:
                raise FileNotFoundError(command[0])
            return self._result(recorded)
        raise AssertionError(f"unexpected command: {command}")

    @staticmethod
    def _result(recorded: dict):
        if recorded.get("timed_out"):
            raise TimeoutExpired()
        return types.SimpleNamespace(
            returncode=recorded["returncode"],
            stdout=recorded["stdout"],
            stderr=recorded["stderr"],
        )


class TimeoutExpired(Exception):
    """`subprocess.TimeoutExpired`, as `check_stream` catches it."""


def plugin_answer(plugin, case: dict) -> dict:
    """Run the plugin's real `check_stream` over the case's recording."""
    settings = dict(case["settings"])
    settings.setdefault("streamlink_hosts", "")
    # The plugin reads these from settings; the corpus records the values the Rust uses so the
    # command lines are comparable argument for argument.
    settings.setdefault("ffprobe_analysis_duration", ANALYZE_SECONDS)
    settings.setdefault("ffprobe_flags", DEFAULT_FFPROBE_FLAGS)
    settings.setdefault("ffprobe_path", "ffprobe")
    settings.setdefault("ffmpeg_path", "ffmpeg")
    if not settings.get("streamlink_hosts"):
        # `_streamlink_host_suffixes`: an empty setting falls back to the shipped list.
        settings["streamlink_hosts"] = DEFAULT_STREAMLINK_HOSTS
    settings["dead_connection_retries"] = 0

    recorder = Recorder(case)
    original_run = plugin.subprocess.run
    original_timeout = plugin.subprocess.TimeoutExpired
    plugin.subprocess.run = recorder
    plugin.subprocess.TimeoutExpired = TimeoutExpired
    try:
        inst = plugin.Plugin.__new__(plugin.Plugin)
        inst._rate_limit_guard = plugin.RateLimitGuard()
        inst._stop_event = MagicMock(name="stop_event")
        inst._stop_event.is_set.return_value = False
        result = plugin.Plugin.check_stream(
            inst,
            {"stream_url": case["url"], "channel_name": case["name"], "stream_id": 1},
            int(settings.get("timeout", 10)),
            0,
            _NullLogger(),
            skip_retries=True,
            settings=settings,
            retry_attempt=0,
        )
    finally:
        plugin.subprocess.run = original_run
        plugin.subprocess.TimeoutExpired = original_timeout

    # The plugin's ffprobe command is built in `check_stream`; the ffmpeg one in
    # `_analyze_stream_content`, and only when a detector is armed and the stream is Alive.
    argv = {"ffprobe": None, "ffmpeg": None}
    for call in recorder.calls:
        program = Path(call[0]).name
        if program in argv and argv[program] is None:
            argv[program] = call

    return {
        "argv": argv,
        "status": result.get("status"),
        "error_type": result.get("error_type"),
        "format": result.get("format"),
        "framerate_num": float(result.get("framerate_num") or 0),
        "error": result.get("error") or None,
        "retry_count": result.get("retry_count"),
        "metadata": result.get("dispatcharr_metadata") or {},
        "extra": result.get("ffprobe_data") or {},
    }


class _NullLogger:
    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def plugin_channel_verdict(plugin, streams: list[dict]) -> dict:
    """`_channels_where` with the plugin's own predicates, per channel.

    Driven over synthetic rows so the aggregation is the plugin's, not this script's: each
    channel's rows carry the plugin's own result dicts (`status`, `error_type`,
    `framerate_num`).
    """
    channels = {}
    for index, parts in enumerate(streams):
        rows = []
        for part in parts:
            rows.append({
                "channel_id": index + 1,
                "channel_name": f"channel {index + 1}",
                "status": part["status"],
                "error_type": part["error_type"],
                "framerate_num": part["framerate_num"],
                "format": part["format"],
            })
        dead = plugin.Plugin._channels_where(rows, plugin.Plugin._is_dead_nonblack)
        blank = plugin.Plugin._channels_where(rows, plugin.Plugin._is_black_screen)
        slow = plugin.Plugin._channels_where(
            rows, plugin.Plugin._is_low_framerate_row,
            among=lambda row: row.get("status") == "Alive")
        alive = any(row["status"] == "Alive" for row in rows)
        skipped = all(row["status"] == "Skipped" for row in rows)
        if dead:
            verdict = "dead"
        elif blank:
            verdict = "blank"
        elif slow:
            verdict = "low_frame_rate"
        elif alive:
            verdict = "alive"
        elif skipped:
            verdict = "skipped"
        else:
            verdict = "inconclusive"
        # `add_video_format_suffix_action` (3732): the LAST alive row's format wins.
        fmt = None
        for row in rows:
            if row["status"] == "Alive":
                fmt = row["format"]
        channels[index + 1] = {"verdict": verdict, "format": fmt}
    return channels


# ---------------------------------------------------------------------------------------
# Constructed cases
# ---------------------------------------------------------------------------------------

def constructed_cases() -> list[dict]:
    """The two cases no tool can record: a process timeout and a Streamlink-only host."""
    timeout_settings = {
        "timeout": 10, "probe_timeout": 20, "dead_connection_retries": 0,
        "ffprobe_path": "ffprobe", "ffmpeg_path": "ffmpeg",
        "streamlink_hosts": DEFAULT_STREAMLINK_HOSTS,
        "ffprobe_analysis_duration": ANALYZE_SECONDS,
        "ffprobe_flags": DEFAULT_FFPROBE_FLAGS,
        "black_screen_detection": False, "frozen_video_detection": False,
        "silent_audio_detection": False, "placeholder_file_detection": False,
    }
    return [
        {
            "name": "process_timeout",
            "url": "http://probe.test/stall.ts",
            "settings": dict(timeout_settings),
            "ffprobe": {"returncode": None, "timed_out": True, "stdout": "", "stderr": ""},
            "ffmpeg": None,
        },
        {
            "name": "streamlink",
            "url": "https://www.youtube.com/watch?v=abc",
            "settings": {**timeout_settings, "streamlink_hosts": DEFAULT_STREAMLINK_HOSTS},
            "ffprobe": {"returncode": 0, "stdout": "", "stderr": ""},
            "ffmpeg": None,
        },
    ]


# ---------------------------------------------------------------------------------------
# Driving the Rust side
# ---------------------------------------------------------------------------------------

def run_rust(lines: list[dict], binary: str) -> list[dict]:
    payload = "\n".join(json.dumps(line) for line in lines)
    completed = subprocess.run([binary], input=payload, capture_output=True, text=True)
    if completed.returncode != 0:
        raise SystemExit(
            f"the Rust example failed ({completed.returncode}):\n{completed.stderr[-4000:]}"
        )
    answers = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    if len(answers) != len(lines):
        raise SystemExit(f"Rust produced {len(answers)} answers for {len(lines)} lines")
    return answers


# ---------------------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------------------

def compare(name: str, expected: dict, got: dict, show: bool) -> list[str]:
    problems: list[str] = []
    for field in ("status", "error_type", "format", "error", "extra"):
        if expected.get(field) != got.get(field):
            problems.append(f"{field}: plugin={expected.get(field)!r} rust={got.get(field)!r}")
    want_effect = metadata_effect(expected.get("metadata"))
    have_effect = metadata_effect(got.get("metadata"))
    if want_effect != have_effect:
        problems.append(f"metadata: plugin={want_effect!r} rust={have_effect!r}")
    if abs(float(expected.get("framerate_num") or 0) - float(got.get("framerate_num") or 0)) > 1e-9:
        problems.append(
            f"framerate_num: plugin={expected.get('framerate_num')} rust={got.get('framerate_num')}"
        )
    if expected.get("retry_count") != got.get("retry_count"):
        problems.append(
            f"retry_count: plugin={expected.get('retry_count')} rust={got.get('retry_count')}"
        )
    for tool in ("ffprobe", "ffmpeg"):
        want = (expected.get("argv") or {}).get(tool)
        have = (got.get("argv") or {}).get(tool)
        # The plugin resolves nothing: both sides are handed `ffprobe`/`ffmpeg`, so the argv is
        # comparable verbatim.
        if want != have:
            problems.append(f"{tool} command line:\n    plugin={want}\n    rust  ={have}")
    if problems and show:
        print(f"[{name}]")
        for problem in problems:
            print(f"  {problem}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--show", type=int, default=10, help="mismatching cases to print")
    parser.add_argument(
        "--binary",
        default=str(REPO / "target" / "debug" / "examples" / "probe_io"),
        help="path to the probe_io example binary",
    )
    args = parser.parse_args()

    if not PLUGIN_INNER.is_dir():
        print(
            f"reference plugin not found at {PLUGIN_INNER}; "
            "run `git submodule update --init` to check it out",
            file=sys.stderr,
        )
        return 2

    print(f"building {args.binary} ...", file=sys.stderr)
    subprocess.run(
        ["cargo", "build", "-q", "-p", "crosstalk-streams", "--example", "probe_io"],
        cwd=REPO,
        check=True,
    )

    corpus = json.loads(FIXTURES.read_text())
    cases = corpus["cases"] + constructed_cases()
    # The plugin's worker always calls `check_stream(..., skip_retries=True)`, so its own retry
    # passes are one level up: both sides compare ONE attempt. The setting is pinned for both.
    for case in cases:
        case["settings"]["dead_connection_retries"] = 0
    plugin = load_plugin()

    expected = []
    for case in cases:
        expected.append((case["name"], plugin_answer(plugin, case), case))

    # The Rust answers for the same recordings, then the channel verdicts both sides compute
    # from the plugin's own rows.
    probe_lines = [{"kind": "probe", **case} for case in cases]
    answers = run_rust(probe_lines, args.binary)

    shown = 0
    mismatches = 0
    for (name, want, _case), got in zip(expected, answers):
        problems = compare(name, want, got, shown < args.show)
        if problems:
            mismatches += 1
            shown += 1

    # Per-channel verdicts: the same stream verdicts, aggregated by the plugin's predicates and
    # by `verdict_for`.
    by_name = {case["name"]: answer for case, answer in zip(cases, answers)}
    channels = []
    channel_rows = []
    for channel in corpus["channels"]:
        parts = []
        for case_name in channel["cases"]:
            answer = by_name[case_name]
            parts.append(answer)
            channel_rows.append(list(channel))
        channels.append({"id": channel["id"], "streams": [
            {
                "class": answer["status"].lower(),
                "error_type": answer["error_type"],
                "framerate": answer["framerate_num"],
                "format": answer["format"],
            }
            for answer in parts
        ]})
    aggregate = run_rust([{"kind": "aggregate", "channels": channels}], args.binary)[0]
    wanted = plugin_channel_verdict(
        plugin,
        [[
            {
                "status": answer["status"],
                "error_type": answer["error_type"],
                "framerate_num": answer["framerate_num"],
                "format": answer["format"],
            }
            for answer in (by_name[name] for name in channel["cases"])
        ] for channel in corpus["channels"]],
    )
    for channel in corpus["channels"]:
        want = wanted[channel["id"]]
        got = aggregate["channels"][str(channel["id"])]
        if want != got:
            mismatches += 1
            if shown < args.show:
                print(f"[channel {channel['id']} {channel['name']}]")
                print(f"  plugin={want}")
                print(f"  rust  ={got}")
            shown += 1

    fields = len(cases) * 8 + len(corpus["channels"]) * 2
    print(
        f"[probe] {mismatches} mismatch(es) over {len(cases)} recorded case(s) and "
        f"{len(corpus['channels'])} channel verdict(s), {fields} field(s) compared"
    )
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
