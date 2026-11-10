#!/usr/bin/env python3
"""Parity: crosstalk's stream sort against Stream-Mapparr's own `_sort_streams_by_quality`.

The streams app's sort is the plugin's quality model, ported. This script is how that claim is
checked: it imports Stream-Mapparr's `plugin.py` from `reference/` (with the same Django stubs
its own test suite installs, `reference/Stream-Mapparr/tests/conftest.py`) and drives the REAL
methods over fixtures — `_sort_streams_by_quality`, `_zone_routed_map`, `_order_streams_for_zone`
via `_streams_for_channel`, `_split_custom_tail`, `_resolve_m3u_priorities` — then compares the
resulting per-channel orders with `cargo run -p crosstalk-streams --example sort_io`.

Nothing of the Rust is mirrored here: the plugin's own code decides the expected answer.

What the corpus covers, because these are the places a port drifts:

  * `stream_stats` shapes: `width`/`height` as integers, as numeric strings, as garbage;
    `resolution` as "1920x1080"/"1920X1080"/"1920x1080p"/garbage; no stats at all; unknown
    keys; `video_bitrate` vs `ffmpeg_output_bitrate` (and both disagreeing);
    `audio_channels`/`audio_codec` in every spelling the priority lists match.
  * both `prioritize_quality` orders, `demote_content_starved` on and off, bitrate steps 0,
    100 and 500, and the plugin's shipped audio priority lists.
  * the throughput cache: fresh, stale, failed (a null measurement), missing, with and without
    its own `nominal_bitrate_mbps`, and each of healthy / marginal / insufficient.
  * M3U priority lists: empty, named only, named with a `*` wildcard, and a source the list
    does not mention at all.
  * zone routing: a DEFAULT channel (most of a real lineup), an East/West sibling pair, a lone
    marked channel that must NOT be routed, and a channel whose every stream is the other zone.
  * custom streams: `keep_custom_streams` on and off.

Two deliberate differences are kept out of the corpus, because the plugin does not have an
answer to compare against — it raises:
  * `source_fps` and `nominal_bitrate_mbps` as non-numeric strings: the plugin compares them as
    numbers (`fps >= 30`, `nominal <= 0`) and the whole sort errors. crosstalk reads 0.
  * `stream_stats` that is not an object at all: the plugin's `stats.get(...)` raises. crosstalk
    treats it as "no facts", like its own `_stats_dimensions` does.

Usage:
    python3 -I scripts/sort_parity.py [--show N] [--cases N] [--seed N]

Exit status is 1 when any order differs, so it can gate CI.
"""
from __future__ import annotations

import argparse
import random
import subprocess
import sys
import types
from datetime import datetime, timedelta, timezone as dt_timezone
from pathlib import Path

# The harness imports the plugin in-process; don't leave __pycache__ behind in the
# reference checkout on every run.
sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parent.parent
PLUGIN_INNER = REPO / "reference" / "Stream-Mapparr" / "Stream-Mapparr"
PKG = "stream_mapparr_sort_parity"


# ---------------------------------------------------------------------------------------
# Loading the plugin without Django, exactly as its own test suite does
# ---------------------------------------------------------------------------------------

def _install_runtime_stubs() -> None:
    """`reference/Stream-Mapparr/tests/conftest.py:37-95`, which is the plugin's own recipe.

    The plugin imports Django's timezone, the Dispatcharr ORM models and one websocket helper
    at module load. None of them is reached by the sort: only `timezone.now()` is, and the
    real one is stubbed to an aware UTC clock.
    """
    from unittest.mock import MagicMock

    # `pytz` is imported at module load and only used for timezone lookups the sort never
    # reaches, so a stdlib-backed stub is enough when the environment has no copy.
    if "pytz" not in sys.modules:
        try:
            import pytz  # noqa: F401
        except ImportError:
            pytz_stub = types.ModuleType("pytz")
            pytz_stub.utc = dt_timezone.utc
            pytz_stub.timezone = lambda name: dt_timezone.utc
            pytz_stub.all_timezones = []
            sys.modules["pytz"] = pytz_stub

    django = types.ModuleType("django")
    django_utils = types.ModuleType("django.utils")
    django_tz = types.ModuleType("django.utils.timezone")
    django_tz.now = lambda: datetime.now(dt_timezone.utc)
    django_tz.localtime = lambda dt=None: dt if dt is not None else django_tz.now()
    django_utils.timezone = django_tz
    django_db = types.ModuleType("django.db")
    django_db.transaction = MagicMock(name="transaction")
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


def _load_from_path(module_name: str, file_name: str):
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_INNER / file_name)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_plugin():
    """plugin.py as a package submodule (its relative imports need the package), plus a matcher."""
    if str(PLUGIN_INNER) not in sys.path:
        sys.path.insert(0, str(PLUGIN_INNER))
    _install_runtime_stubs()
    if PKG not in sys.modules:
        package = types.ModuleType(PKG)
        package.__path__ = [str(PLUGIN_INNER)]
        sys.modules[PKG] = package
    if f"{PKG}.fuzzy_matcher" not in sys.modules:
        _load_from_path(f"{PKG}.fuzzy_matcher", "fuzzy_matcher.py")
    if f"{PKG}.country" not in sys.modules:
        _load_from_path(f"{PKG}.country", "country.py")
    plugin = _load_from_path(f"{PKG}.plugin", "plugin.py")
    import logging

    # The plugin logs at INFO per channel; the mismatch output is what matters here.
    logging.disable(logging.CRITICAL)
    fuzzy_matcher = sys.modules[f"{PKG}.fuzzy_matcher"]
    matcher = fuzzy_matcher.FuzzyMatcher(plugin_dir=str(PLUGIN_INNER), match_threshold=85)
    return plugin, matcher


# ---------------------------------------------------------------------------------------
# The plugin's own answer for one case
# ---------------------------------------------------------------------------------------

def plugin_orders(plugin, matcher, case: dict) -> dict:
    """Run the plugin's real sort over the case and return {channel_id: [ids]}.

    Drives `sort_streams_action`'s per-channel body (10012-10046) with a fake `self`: the
    method's inputs are attributes it reads with `getattr`, which is exactly how the plugin's
    own tests build an instance (`test_sort_prioritize_quality.py:_bare`).
    """
    settings = case["settings"]
    inst = plugin.Plugin.__new__(plugin.Plugin)
    inst.fuzzy_matcher = matcher
    inst._throughput_state_primed = True
    inst.saved_settings = {
        "audio_channels_priority": settings.get("audio_channels_priority", ""),
        "audio_codec_priority": settings.get("audio_codec_priority", ""),
    }
    # The plugin's own resolvers, exactly as `sort_streams_action` runs them (9818-9842): a
    # floor of 0 means the default, a non-numeric margin means the default, and a step of 0
    # stays 0 because it is the documented "off".
    inst._prioritize_quality = bool(settings.get("prioritize_quality", False))
    inst._throughput_sorting_enabled = bool(settings.get("enable_throughput_sorting", True))
    inst._throughput_cache = case.get("throughput") or {}
    inst._bitrate_safety_margin = float(settings.get("bitrate_safety_margin", 1.10))
    inst._probe_cache_ttl_minutes = int(settings.get("probe_cache_ttl_minutes", 30))
    inst._content_starved_enabled = plugin.Plugin._resolve_demote_content_starved(settings)
    inst._content_starved_floor_kbps = plugin.Plugin._resolve_content_starved_floor(settings)
    inst._bitrate_sort_step_kbps = plugin.Plugin._resolve_bitrate_sort_step(settings)

    tags = plugin.Plugin._parse_tags(settings.get("ignore_tags", ""))
    zone_routed = inst._zone_routed_map(
        [{"id": channel["id"], "name": channel["name"]} for channel in case["channels"]],
        tags,
        bool(settings.get("ignore_quality", True)),
        bool(settings.get("ignore_regional", True)),
        bool(settings.get("ignore_geographic", True)),
        bool(settings.get("ignore_misc", True)),
    )

    name_to_id = {account["name"]: account["id"] for account in case.get("accounts", [])}
    priorities = plugin.Plugin._resolve_m3u_priorities(settings.get("selected_m3us", ""), name_to_id)
    keep_custom = bool(settings.get("keep_custom_streams", True))
    by_id = {stream["id"]: stream for stream in case["streams"]}

    orders = {}
    for channel in case["channels"]:
        ids = channel["streams"]
        if len(ids) <= 1:
            continue
        streams = []
        for stream_id in ids:
            stream = by_id.get(stream_id)
            if stream is None:
                # `Stream.DoesNotExist`: the plugin warns and leaves it out of the list.
                continue
            streams.append({
                "id": stream["id"],
                "name": stream["name"],
                "stats": stream.get("stats") or {},
                "_m3u_priority": plugin.Plugin._m3u_priority_for(
                    stream.get("account"), priorities),
                "is_custom": bool(stream.get("custom", False)),
            })
        if keep_custom:
            provider, custom = plugin.Plugin._split_custom_tail(streams)
        else:
            provider, custom = streams, []
        sorted_streams = inst._sort_streams_by_quality(provider)
        sorted_streams = inst._streams_for_channel(
            sorted_streams, channel["id"], zone_routed, None) + custom
        before = [stream["id"] for stream in streams]
        after = [stream["id"] for stream in sorted_streams]
        orders[channel["id"]] = {"before": before, "after": after,
                                 "changed": before != after}
    return orders


# ---------------------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------------------

def iso(delta_seconds: float) -> str:
    """A timestamp `delta_seconds` from now, in the aware-ISO form the plugin writes."""
    return (datetime.now(dt_timezone.utc) + timedelta(seconds=delta_seconds)).isoformat()


def entry(mbps, age_seconds=None, nominal=None, edge=None):
    payload = {
        "throughput_mbps": mbps,
        "throughput_measured_at": iso(-age_seconds) if age_seconds is not None else None,
    }
    if nominal is not None:
        payload["nominal_bitrate_mbps"] = nominal
    if edge is not None:
        payload["edge_ip"] = edge
    return payload


# Stats shapes the port has to read exactly like `_stats_dimensions` / `_stats_bitrate_kbps`.
STATS_SHAPES = [
    ("no stats", {}),
    ("1080p60 integers", {"width": 1920, "height": 1080, "source_fps": 60}),
    ("720p30 integers", {"width": 1280, "height": 720, "source_fps": 30.0}),
    ("720p25 (fps tier only)", {"width": 1280, "height": 720, "source_fps": 25}),
    ("SD 576p50", {"width": 720, "height": 576, "source_fps": 50}),
    ("4K50", {"width": 3840, "height": 2160, "source_fps": 50}),
    ("4K29.97", {"width": 3840, "height": 2160, "source_fps": 29.97002997}),
    ("resolution string only", {"resolution": "1920x1080", "source_fps": 50}),
    ("resolution upper X", {"resolution": "1280X720", "source_fps": 50}),
    ("resolution with suffix", {"resolution": "3840x2160p", "source_fps": 60}),
    ("resolution garbage", {"resolution": "unknown", "source_fps": 50}),
    # `source_fps` is left out of the string shapes on purpose: the plugin compares it as a
    # number and the whole sort raises on "60", so there is no answer to compare (see the
    # module docstring).
    ("width/height strings", {"width": "1920", "height": "1080", "source_fps": 60}),
    ("width garbage", {"width": "wide", "height": "tall", "source_fps": 50}),
    ("width zero height set", {"width": 0, "height": 1080, "source_fps": 50}),
    ("width only", {"width": 1920, "source_fps": 50}),
    ("height only", {"height": 1080, "source_fps": 50}),
    ("audio 5.1 eac3", {"width": 1920, "height": 1080, "source_fps": 50,
                        "audio_channels": 6, "audio_codec": "eac3"}),
    ("audio 7.1 truehd", {"width": 1920, "height": 1080, "source_fps": 50,
                          "audio_channels": "8", "audio_codec": "truehd"}),
    ("audio stereo aac", {"width": 1920, "height": 1080, "source_fps": 50,
                          "audio_channels": 2, "audio_codec": "aac"}),
    ("audio mono mp2", {"width": 1280, "height": 720, "source_fps": 50,
                        "audio_channels": 1, "audio_codec": "mp2"}),
    ("audio float channels", {"width": 1280, "height": 720, "source_fps": 50,
                              "audio_channels": 6.0, "audio_codec": "AC3"}),
    ("bitrate measured", {"width": 1280, "height": 720, "source_fps": 50,
                          "video_bitrate": 2656}),
    ("bitrate ffmpeg only", {"width": 1280, "height": 720, "source_fps": 50,
                             "ffmpeg_output_bitrate": 3010}),
    ("bitrate both disagree", {"width": 1280, "height": 720, "source_fps": 50,
                               "video_bitrate": 124, "ffmpeg_output_bitrate": 389}),
    ("bitrate string", {"width": 1280, "height": 720, "source_fps": 50,
                        "video_bitrate": "1499"}),
    ("bitrate garbage", {"width": 1280, "height": 720, "source_fps": 50,
                         "video_bitrate": "fast"}),
    ("bitrate zero", {"width": 1280, "height": 720, "source_fps": 50,
                      "video_bitrate": 0, "ffmpeg_output_bitrate": 0}),
    ("bitrate negative", {"width": 1280, "height": 720, "source_fps": 50,
                          "video_bitrate": -5}),
    ("slate: 1080p low bitrate", {"width": 1920, "height": 1080, "source_fps": 50,
                                  "video_bitrate": 190}),
    ("720p at the floor", {"width": 1280, "height": 720, "source_fps": 50,
                           "video_bitrate": 300}),
    ("480p low bitrate (below floor height)", {"width": 854, "height": 480, "source_fps": 50,
                                               "video_bitrate": 100}),
    ("unknown keys", {"width": 1920, "height": 1080, "source_fps": 50,
                      "iam": "not a stat", "video_codec": "hevc"}),
    ("1280x719 (one pixel shy of HD)", {"width": 1280, "height": 719, "source_fps": 60}),
    ("1279x720", {"width": 1279, "height": 720, "source_fps": 60}),
    ("1080p at 29.97", {"width": 1920, "height": 1080, "source_fps": 29.97002997}),
    ("1080p at 30", {"width": 1920, "height": 1080, "source_fps": 30}),
]

STREAM_NAMES = [
    "ESPN", "ESPN HD", "ESPN 4K", "ESPN (E)", "ESPN (W)", "ESPN West", "ESPN East",
    "HBO", "HBO (W)", "Key West", "Cinemax", "TNT HD", "SD: Something", "PPV EVENT 01",
]

AUDIO_SETTINGS = [
    ("", ""),
    ("7.1, 5.1, stereo, mono", "eac3, ac3, aac, mp2"),
    ("stereo", "aac"),
    ("6, 8", "truehd"),
]

CHANNEL_SETS = [
    # (comment, channels)
    ("single default channel", [{"id": 1, "name": "ESPN"}]),
    ("East/West siblings", [{"id": 1, "name": "ESPN"}, {"id": 2, "name": "ESPN (W)"}]),
    ("lone marked channel", [{"id": 1, "name": "Key West"}]),
    ("west sibling of a differently-cased base", [
        {"id": 1, "name": "STARZ Encore"}, {"id": 2, "name": "Starz Encore (W)"}]),
    ("east and west and default", [
        {"id": 1, "name": "HBO East"}, {"id": 2, "name": "HBO West"}, {"id": 3, "name": "HBO"}]),
    ("two unrelated channels", [{"id": 1, "name": "ESPN"}, {"id": 2, "name": "Key West"}]),
]

THROUGHPUT_SETS = [
    ("no measurements", {}),
    ("one fresh healthy", "healthy"),
    ("one stale", "stale"),
    ("one failed probe", "failed"),
    ("mixed fresh tiers", "mixed"),
]


def throughput_for(flavour, stream_ids):
    """A cache keyed by stream id, in the plugin's own JSON shape."""
    ids = list(stream_ids)

    def at(index):
        return str(ids[index % len(ids)])

    if flavour in ({}, None):
        return {}
    if flavour == "healthy":
        # 20 Mbps against a ~4 Mbps nominal is comfortably healthy.
        return {at(0): entry(20.0, age_seconds=60, nominal=4.0, edge="edge-a.example")}
    if flavour == "stale":
        return {at(0): entry(20.0, age_seconds=3600, nominal=4.0)}
    if flavour == "failed":
        return {at(0): entry(None, age_seconds=60, nominal=4.0)}
    # mixed: healthy, marginal, insufficient, an entry with no nominal of its own, and a fresh
    # one whose nominal has to be estimated from the stats.
    cache = {}
    kinds = [
        entry(20.0, age_seconds=60, nominal=4.0),
        entry(5.0, age_seconds=60, nominal=4.0),
        entry(1.0, age_seconds=60, nominal=4.0),
        entry(3.0, age_seconds=60),
        entry(0.5, age_seconds=60, nominal=16.0),
    ]
    for offset, payload in enumerate(kinds):
        cache[at(offset)] = payload
    return cache


def stream_pool(seed: int, limit: int | None = None) -> list[dict]:
    """The streams every case draws from: each stats shape with a name from the name list."""
    rng = random.Random(seed)
    pool = []
    stream_id = 1000
    for index, (label, stats) in enumerate(STATS_SHAPES):
        name = STREAM_NAMES[index % len(STREAM_NAMES)]
        pool.append({
            "id": stream_id,
            "name": name,
            "account": rng.choice([None, 1, 2, 3]),
            "custom": False,
            "stats": stats,
            "label": label,
        })
        stream_id += 1
    # A few custom streams, which are never ranked and stay last.
    for name in ("Could Not Dispatch", "Local Insert"):
        pool.append({"id": stream_id, "name": name, "account": 9, "custom": True,
                     "stats": {}, "label": "custom"})
        stream_id += 1
    return pool[:limit] if limit else pool


def build_cases(limit: int | None = None) -> list[dict]:
    """A grid over the settings, the channel sets and the throughput flavours.

    Every stream of the pool is on every channel (a channel with 40 alternates is a real
    shape), and the four settings that change the key are swept rather than sampled: the
    throughput switch, the quality-first switch, the bitrate step and the demote rule.
    """
    cases = []
    pool = stream_pool(seed=1)
    ids = [stream["id"] for stream in pool]
    accounts = [
        {"id": 1, "name": "Main", "active": True},
        {"id": 2, "name": "Backup", "active": True},
        {"id": 3, "name": "Third", "active": True},
        {"id": 9, "name": "Custom", "active": True},
    ]
    m3u_options = ["", "Main, *", "*, Backup", "Backup, Main", "Retired"]

    for channel_comment, channels in CHANNEL_SETS:
        for throughput_enabled in (True, False):
            for prioritize in (True, False):
                for step in (0, 100, 500):
                    for flavour_label, flavour in THROUGHPUT_SETS:
                        for demote in (True, False):
                            audio = AUDIO_SETTINGS[len(cases) % len(AUDIO_SETTINGS)]
                            m3us = m3u_options[len(cases) % len(m3u_options)]
                            # The last case of a set is already in the sorted order, which is
                            # what "already in order" and "only changed orders are written"
                            # have to see at least once per shape.
                            streams = list(ids)
                            if len(cases) % 4 == 3:
                                streams = list(reversed(ids))
                            cases.append({
                                "comment": f"{channel_comment} / throughput={throughput_enabled} "
                                           f"prioritize={prioritize} step={step} "
                                           f"demote={demote} cache={flavour_label} "
                                           f"m3u={m3us!r} audio={audio}",
                                "settings": {
                                    "prioritize_quality": prioritize,
                                    "enable_throughput_sorting": throughput_enabled,
                                    "bitrate_sort_step_kbps": step,
                                    "demote_content_starved": demote,
                                    # 0 must resolve to the default floor on both sides.
                                    "content_bitrate_floor_kbps": (300, 500, 0)[len(cases) % 3],
                                    "bitrate_safety_margin": 1.10,
                                    "probe_cache_ttl_minutes": 30,
                                    "audio_channels_priority": audio[0],
                                    "audio_codec_priority": audio[1],
                                    "keep_custom_streams": len(cases) % 5 != 0,
                                    "selected_m3us": m3us,
                                    "ignore_tags": "",
                                },
                                "channels": [
                                    {**channel, "group": 100 + offset, "streams": streams}
                                    for offset, channel in enumerate(channels)
                                ],
                                "streams": [{key: stream[key] for key in
                                             ("id", "name", "account", "custom", "stats")}
                                            for stream in pool],
                                "accounts": accounts,
                                "groups": [{"id": 100 + offset, "name": f"Group {offset}"}
                                           for offset in range(len(channels))],
                                "throughput": throughput_for(flavour, ids),
                            })
    if limit:
        cases = cases[:limit]
    return cases


def random_cases(count: int, seed: int) -> list[dict]:
    """Randomised fixtures: the same shapes, thrown together instead of gridded."""
    rng = random.Random(seed)
    pool = stream_pool(seed=7)
    cases = []
    for index in range(count):
        stream_count = rng.randint(2, 7)
        channels = []
        # A custom stream buried mid-list, not just trailing/leading: `_split_custom_tail`
        # filters every custom stream wherever it sits, and so must the port.
        customs = [stream for stream in pool if stream["custom"]]
        extras = [stream for stream in pool if not stream["custom"]]
        def channel_ids():
            if customs and rng.random() < 0.5:
                picked = [stream["id"] for stream in rng.sample(extras, stream_count)]
                picked.insert(len(picked) // 2, customs[0]["id"])
                return picked
            return [stream["id"] for stream in rng.sample(pool, stream_count)]
        for channel_index in range(rng.randint(1, 3)):
            marked = rng.choice(["ESPN", "ESPN", "HBO East", "HBO West", "Key West",
                                 "STARZ Encore", "Starz Encore (W)", "Cinemax", "TNT"])
            # A channel's stream list is an order, and sorted lists are what a run has to
            # leave alone as often as reorder; shuffle so both cases appear.
            ids = channel_ids()
            rng.shuffle(ids)
            channels.append({
                "id": channel_index + 1,
                "name": marked if channel_index else f"{marked} HD",
                "group": 100,
                "streams": ids,
            })
        settings = {
            "prioritize_quality": rng.random() < 0.5,
            "enable_throughput_sorting": rng.random() < 0.7,
            "bitrate_sort_step_kbps": rng.choice([0, 100, 500, 1000]),
            "demote_content_starved": rng.random() < 0.7,
            "content_bitrate_floor_kbps": rng.choice([200, 300, 500]),
            "bitrate_safety_margin": rng.choice([1.0, 1.10, 1.5]),
            # 0 is honoured, not defaulted: the plugin only falls back on a non-number.
            "probe_cache_ttl_minutes": rng.choice([0, 15, 30, 60]),
            "audio_channels_priority": rng.choice([value[0] for value in AUDIO_SETTINGS]),
            "audio_codec_priority": rng.choice([value[1] for value in AUDIO_SETTINGS]),
            "keep_custom_streams": rng.random() < 0.8,
            "selected_m3us": rng.choice(["", "Main, *", "*, Backup", "Third, Main"]),
            "ignore_tags": rng.choice(["", "SD", "HD, UHD"]),
            "ignore_quality": rng.random() < 0.8,
            "ignore_regional": rng.random() < 0.8,
            "ignore_geographic": rng.random() < 0.8,
            "ignore_misc": rng.random() < 0.8,
        }
        cache = {}
        for stream in rng.sample(pool, rng.randint(0, len(pool))):
            kind = rng.random()
            nominal = rng.choice([None, 1.5, 2.5, 4.0, 6.0, 16.0])
            age = rng.choice([10, 60, 600, 1800, 3600, 7200])
            if kind < 0.15:
                mbps = None
            elif kind < 0.3:
                mbps = 0.0
            elif kind < 0.35:
                # A negative delivery rate: below every nominal, so insufficient on both.
                mbps = round(rng.uniform(-5.0, -0.1), 2)
            else:
                mbps = round(rng.uniform(0.2, 30.0), 2)
            cache[str(stream["id"])] = entry(mbps, age_seconds=age, nominal=nominal)
        cases.append({
            "comment": f"random #{index}",
            "settings": settings,
            "channels": channels,
            "streams": [{key: stream[key] for key in
                         ("id", "name", "account", "custom", "stats")} for stream in pool],
            "accounts": [
                {"id": 1, "name": "Main", "active": True},
                {"id": 2, "name": "Backup", "active": True},
                {"id": 3, "name": "Third", "active": True},
                {"id": 9, "name": "Custom", "active": True},
            ],
            "groups": [{"id": 100, "name": "Group"}],
            "throughput": cache,
        })
    return cases


# ---------------------------------------------------------------------------------------
# Driving the Rust side
# ---------------------------------------------------------------------------------------

def run_rust(cases: list[dict], binary: str) -> list[dict]:
    import json

    requests = "\n".join(json.dumps(case) for case in cases)
    completed = subprocess.run([binary], input=requests, capture_output=True, text=True)
    if completed.returncode != 0:
        raise SystemExit(
            f"the Rust example failed ({completed.returncode}):\n{completed.stderr[-4000:]}"
        )
    answers = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    if len(answers) != len(cases):
        raise SystemExit(f"Rust produced {len(answers)} answers for {len(cases)} cases")
    return answers


# ---------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--show", type=int, default=10, help="mismatches to print")
    parser.add_argument("--cases", type=int, default=None, help="cap the gridded corpus")
    parser.add_argument("--random", type=int, default=120, help="randomised cases to add")
    parser.add_argument("--seed", type=int, default=20261101, help="seed for the randomised cases")
    parser.add_argument(
        "--binary",
        default=str(REPO / "target" / "debug" / "examples" / "sort_io"),
        help="path to the sort_io example binary",
    )
    args = parser.parse_args()

    if not PLUGIN_INNER.is_dir():
        print(
            f"reference plugin not found at {PLUGIN_INNER}; "
            "run `git submodule update --init` to check it out",
            file=sys.stderr,
        )
        return 2

    # Always rebuild: a stale local binary would otherwise be silently compared.
    print(f"building {args.binary} ...", file=sys.stderr)
    subprocess.run(
        ["cargo", "build", "-q", "-p", "crosstalk-streams", "--example", "sort_io"],
        cwd=REPO,
        check=True,
    )

    plugin, matcher = load_plugin()
    cases = build_cases(args.cases) + random_cases(args.random, args.seed)
    answers = run_rust(cases, args.binary)

    mismatches = 0
    compared_channels = 0
    changed_channels = 0
    for case, answer in zip(cases, answers):
        expected = plugin_orders(plugin, matcher, case)
        got_orders = answer["orders"]
        if set(expected) != set(int(key) for key in got_orders):
            mismatches += 1
            print(f"[{case['comment']}] channel set differs: "
                  f"python={sorted(expected)} rust={sorted(int(k) for k in got_orders)}")
            continue
        for channel_id, want in expected.items():
            compared_channels += 1
            changed_channels += int(want["changed"])
            got = got_orders[str(channel_id)]
            if want["after"] != got["after"] or want["before"] != got["before"]:
                mismatches += 1
                if mismatches <= args.show:
                    print(f"[{case['comment']}] channel {channel_id}")
                    print(f"  settings={case['settings']}")
                    print(f"  streams={[(s['id'], s['name'], s['stats']) for s in case['streams']
                                         if s['id'] in want['after']]}")
                    print(f"  throughput={case['throughput']}")
                    print(f"  python={want['after']} (before {want['before']})")
                    print(f"  rust  ={got['after']} (before {got['before']})")
        got_changed = sorted(answer["changed"])
        want_changed = sorted(cid for cid, order in expected.items() if order["changed"])
        if got_changed != want_changed:
            mismatches += 1
            if mismatches <= args.show:
                print(f"[{case['comment']}] changed set differs: "
                      f"python={want_changed} rust={got_changed}")

    print(
        f"[sort] {mismatches} mismatch(es) over {len(cases)} case(s), "
        f"{compared_channels} channel(s) compared, {changed_channels} of them reordered"
    )
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
