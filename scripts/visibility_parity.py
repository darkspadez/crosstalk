#!/usr/bin/env python3
"""Parity: crosstalk's `visibility_*` against Stream-Mapparr's channel-visibility action.

The streams app's visibility action is the plugin's, ported. This script is how that claim is
checked: it imports the plugin's `plugin.py` from `reference/` with the Django stubs its own
test suite installs (`reference/Stream-Mapparr/tests/conftest.py`), then drives the REAL
`manage_channel_visibility_action` end to end over generated fixture worlds — with
`ChannelProfileMembership` faked as a table of `(profile, channel) -> enabled` rows — and
compares the resulting membership with what
`cargo run -p crosstalk-streams --example visibility_io` planned.

Nothing of the Rust is mirrored here: the plugin's own code decides the expected answer.

What runs for real, per case:

  * the plugin's `processed_data` file (the snapshot its load step writes), which is what the
    action actually reads: `profile_id`, the channel dicts, `ignore_tags` and the four
    `ignore_*` flags;
  * the action's two bulk `ChannelProfileMembership` updates, against a fake table that holds
    one row per (profile, channel) pair, as Dispatcharr does;
  * `_build_channel_groups` — the SAME grouping Match & Assign uses, country-qualified when
    the filter is on — with `_load_channels_data` stubbed to the case's database entries
    (there is no plugin directory to read here);
  * `_sort_channels_by_priority` and the "first channel with >= 1 stream, not attached" latch.

The channel set is the load step's, reproduced here exactly as `load_process_channels_action`
computes it (the union of the enabled memberships of every named profile, then the group
filter), because the action takes that set from `processed_data` and never resolves it itself.

Two things are stubbed, and both are database reads rather than behaviour: the ROW FETCH of
`_load_channels_data` (the case's entries stand in for the shipped `*_channels.json` files) and
the progress/websocket notifications.

Usage:
    python3 -I scripts/visibility_parity.py [--show N] [--random N] [--seed N]

Exit status is 1 when any membership differs, so it can gate CI.
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import re
import subprocess
import sys
import tempfile
import types
from datetime import datetime, timezone as dt_timezone
from pathlib import Path

sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parent.parent
PLUGIN_INNER = REPO / "reference" / "Stream-Mapparr" / "Stream-Mapparr"
PKG = "stream_mapparr_visibility_parity"


# ---------------------------------------------------------------------------------------
# Loading the plugin without Django, exactly as its own test suite does
# ---------------------------------------------------------------------------------------

def _install_runtime_stubs() -> None:
    """`reference/Stream-Mapparr/tests/conftest.py:37-95`, which is the plugin's own recipe."""
    from unittest.mock import MagicMock

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
    # `django.db` must be a PACKAGE: the action does `from django.db.models import Count` for
    # its one stream-count aggregate.
    django_db = types.ModuleType("django.db")
    django_db.__path__ = []
    django_db.transaction = MagicMock(name="transaction")
    django_db_models = types.ModuleType("django.db.models")
    django_db_models.Count = lambda field: field
    django_db.models = django_db_models
    sys.modules.update({
        "django": django,
        "django.utils": django_utils,
        "django.utils.timezone": django_tz,
        "django.db": django_db,
        "django.db.models": django_db_models,
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
    logging.disable(logging.CRITICAL)
    fuzzy_matcher = sys.modules[f"{PKG}.fuzzy_matcher"]
    matcher = fuzzy_matcher.FuzzyMatcher(plugin_dir=str(PLUGIN_INNER), match_threshold=85)
    return plugin, matcher


# ---------------------------------------------------------------------------------------
# A fake ChannelProfileMembership table
# ---------------------------------------------------------------------------------------

class FakeMembership:
    """`ChannelProfileMembership.objects.filter(...).update(enabled=...)`.

    Dispatcharr keeps one row per (profile, channel) pair, which is what makes the plugin's
    "disable everything, then enable one per group" reach any channel; the table below is
    therefore seeded densely for every profile the case names.
    """

    enabled = {}   # (profile_id, channel_id) -> bool

    @classmethod
    def reset(cls, profile_ids, channel_ids):
        cls.enabled = {(profile_id, channel_id): True
                       for profile_id in profile_ids for channel_id in channel_ids}

    @classmethod
    def set(cls, profile_id, channel_ids, value):
        for channel_id in channel_ids:
            if (profile_id, channel_id) in cls.enabled:
                cls.enabled[(profile_id, channel_id)] = value


class FakeMembershipQuerySet:
    def __init__(self, kw):
        self._kw = kw

    def update(self, **values):
        if "enabled" in values:
            FakeMembership.set(self._kw["channel_profile_id"],
                               self._kw["channel_id__in"], bool(values["enabled"]))
        return len(self._kw["channel_id__in"])


class FakeMembershipManager:
    def filter(self, **kw):
        return FakeMembershipQuerySet(kw)


class FakeStreamCounts:
    """`ChannelStream.objects.filter(...).values('channel_id').annotate(count=Count('id'))`.

    The action's ONE stream query: how many `ChannelStream` rows each channel has. A channel
    with no rows is absent from the answer, which is what the plugin's `.get(ch_id, 0)` then
    reads as zero.
    """

    counts = {}   # channel id -> rows

    @classmethod
    def answer(cls, channel_ids):
        return [{"channel_id": channel_id, "count": cls.counts[channel_id]}
                for channel_id in channel_ids if cls.counts.get(channel_id)]


class FakeStreamCountQuerySet:
    def __init__(self, kw):
        self._kw = kw

    def values(self, *fields):
        return self

    def annotate(self, **kwargs):
        return FakeStreamCounts.answer(self._kw.get("channel_id__in") or [])


class FakeStreamCountManager:
    def filter(self, **kw):
        return FakeStreamCountQuerySet(kw)


# ---------------------------------------------------------------------------------------
# The plugin's own answer for one case
# ---------------------------------------------------------------------------------------

def _named_profiles(case):
    """The profiles `[scope] profiles` names, in the order it names them (case-insensitive)."""
    by_name = {profile["name"].strip().upper(): profile for profile in case["profiles"]}
    found = []
    for token in (case.get("profile_name") or "").split(","):
        token = token.strip()
        if not token:
            continue
        profile = by_name.get(token.upper())
        if profile is not None:
            found.append(profile)
    return found


def loaded_channels(case):
    """`load_process_channels_action`'s channel set, which is what `processed_data` carries.

    The enabled memberships of EVERY named profile (not just the first: the load step filters
    `channel_profile_id__in=profile_ids`), then the group filter. A channel enabled in no named
    profile is not in the snapshot and no action can see it.
    """
    profiles = _named_profiles(case)
    enabled = set()
    for profile in profiles:
        enabled.update(profile.get("channels") or [])
    group_names = {group["id"]: group["name"] for group in case["groups"]}
    selected = [name.strip() for name in (case.get("scope_groups") or "").split(",")
                if name.strip()]
    ignored = [name.strip() for name in (case.get("ignore_groups") or "").split(",")
               if name.strip()]
    kept = []
    for channel in case["channels"]:
        if channel["id"] not in enabled:
            continue
        group_name = group_names.get(channel.get("group"))
        if selected and group_name not in selected:
            continue
        if ignored and group_name in ignored:
            continue
        kept.append(channel)
    return kept


def plugin_answer(plugin, matcher, case: dict, workdir: Path) -> dict:
    """Run the plugin's real action over the case; return the host profile's membership.

    Returns the end state plus the pairs the case left without a membership row: Django's
    `update()` touches existing rows only, so a missing row stays missing, while
    Dispatcharr's bulk-update endpoint CREATES it (`created` in its answer). That is the one
    place the two sides can differ, and the harness reports it separately."""
    logger = logging.getLogger("visibility_parity")
    profiles = _named_profiles(case)
    if not profiles:
        raise SystemExit(f"the case names no profile that exists: {case['profile_name']!r}")
    host = profiles[0]

    FakeMembership.reset([profile["id"] for profile in profiles],
                         [channel["id"] for channel in case["channels"]])
    # Sparse rows: Dispatcharr creates a membership with every channel and every profile,
    # but a channel made with an explicit empty profile list (or a profile made
    # `start_empty`) has pairs with NO row. The case names host pairs to drop; the fake
    # `update()` below then no-ops on them, exactly as Django does.
    dropped = set()
    for channel_id in case.get("drop_host_rows") or []:
        dropped.add((host["id"], channel_id))
        FakeMembership.enabled.pop((host["id"], channel_id), None)
    FakeMembership.objects = FakeMembershipManager()
    FakeStreamCounts.counts = {channel["id"]: len(channel["streams"])
                               for channel in case["channels"]}
    FakeStreamCounts.objects = FakeStreamCountManager()
    plugin.ChannelProfileMembership = FakeMembership
    plugin.ChannelStream = FakeStreamCounts

    group_names = {group["id"]: group["name"] for group in case["groups"]}
    channels = []
    for channel in loaded_channels(case):
        row = {
            "id": channel["id"],
            "name": channel["name"],
            "channel_number": channel.get("number"),
            # `_get_all_channels` always carries the group name: the country resolver reads it.
            "channel_group__name": group_names.get(channel.get("group")),
        }
        if channel.get("attached_channel_id") is not None:
            row["attached_channel_id"] = channel["attached_channel_id"]
        channels.append(row)

    settings = dict(case.get("settings") or {})
    processed = {
        "profile_id": host["id"],
        "profile_ids": [profile["id"] for profile in profiles],
        "channels": channels,
        "ignore_tags": plugin.Plugin._parse_tags(settings.get("ignore_tags", "")),
        "ignore_quality": bool(settings.get("ignore_quality", True)),
        "ignore_regional": bool(settings.get("ignore_regional", True)),
        "ignore_geographic": bool(settings.get("ignore_geographic", True)),
        "ignore_misc": bool(settings.get("ignore_misc", True)),
    }
    path = workdir / "processed_data.json"
    path.write_text(json.dumps(processed), encoding="utf-8")

    P = plugin.Plugin
    inst = P.__new__(P)
    inst.version = "test"
    # The grouping normalises through the matcher, exactly as `_clean_channel_name` does in
    # production. The threshold is not read by this action; the shipped matcher is.
    inst.fuzzy_matcher = matcher
    inst.BUG_REPORT_DIR = str(workdir / "readout")
    inst.processed_data_file = str(path)
    # The one stubbed read: the plugin dir's shipped databases stand in for the case's entries.
    # `_load_channels_data` stamps `_country_code` from the file's country, and the entries
    # themselves are the raw JSON objects (keyed `channel_name`), so the case's flat shape is
    # translated into the plugin's here.
    inst._load_channels_data = lambda logger, settings=None: [
        {"channel_name": entry["name"], "_country_code": entry["code"],
         **({"callsign": entry["callsign"]} if entry.get("callsign") else {})}
        for entry in case.get("channels_data") or []
    ]
    inst._send_progress_update = lambda *args, **kwargs: None
    inst._trigger_frontend_refresh = lambda settings, logger: True

    result = inst.manage_channel_visibility_action(settings, logger)
    if result.get("status") != "success":
        raise SystemExit(f"the plugin's visibility action failed: {result}")

    scope_ids = [channel["id"] for channel in loaded_channels(case)]
    end_state = {channel_id: FakeMembership.enabled.get((host["id"], channel_id), False)
                 for channel_id in scope_ids}
    match = result["message"]
    return {"profile_id": host["id"], "end_state": end_state, "message": match,
            "dropped": sorted(channel_id for _, channel_id in dropped)}


# ---------------------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------------------

GROUP_NAMES = {100: "US: Cable", 101: "UK: Cable", 102: "US: Sports"}


def channel(channel_id, name, streams=1, group=100, number=None, attached=None):
    """A channel with `streams` ChannelStream rows (ids are irrelevant to this action)."""
    return {
        "id": channel_id,
        "name": name,
        "group": group,
        "number": channel_id if number is None else number,
        "streams": list(range(channel_id * 100, channel_id * 100 + streams)),
        "attached_channel_id": attached,
    }


def profile(profile_id, name, channels):
    return {"id": profile_id, "name": name, "channels": channels}


def groups():
    return [{"id": gid, "name": name} for gid, name in GROUP_NAMES.items()]


def worlds():
    """The fixture worlds, one per shape the action has a branch for."""
    cases = []

    def case(comment, channels, profiles, **extra):
        payload = {
            "comment": comment,
            "channels": channels,
            "profiles": profiles,
            "groups": groups(),
            "channels_data": [],
            "settings": {},
            "profile_name": profiles[0]["name"] if profiles else "",
            "scope_groups": "",
            "ignore_groups": "",
        }
        payload.update(extra)
        cases.append(payload)

    all_of = lambda channels: [row["id"] for row in channels]

    # The plugin's own documented behaviour: one channel per name group survives.
    channels = [
        channel(1, "ESPN", streams=2, number=2),
        channel(2, "ESPN", streams=1, number=1),
        channel(3, "CNN", streams=1, number=3),
        channel(4, "CNN", streams=3, number=4),
    ]
    case("one channel per name group", channels, [profile(4, "All", all_of(channels))])

    # Quality tags outrank the channel number, as `_sort_channels_by_priority` reads them.
    channels = [
        channel(1, "ESPN", number=1),
        channel(2, "ESPN [SD]", number=2),
        channel(3, "ESPN [HD]", number=5),
        channel(4, "ESPN [4K]", number=9),
    ]
    case("quality tags outrank the channel number", channels,
         [profile(4, "All", all_of(channels))])

    # A group where every channel is empty goes dark.
    channels = [
        channel(1, "Bravo", streams=0, number=1),
        channel(2, "Bravo", streams=0, number=2),
        channel(3, "CNN", streams=1, number=3),
    ]
    case("a group with no streams at all", channels, [profile(4, "All", all_of(channels))])

    # An attached channel is never made visible, and it does not take the group's slot.
    channels = [
        channel(1, "TNT", number=0.5, attached=2),
        channel(2, "TNT", number=9),
        channel(3, "USA", number=1, attached=4),
        channel(4, "USA", number=2, streams=0),
    ]
    case("attached channels", channels, [profile(4, "All", all_of(channels))])

    # An attached channel alone in its group: the group goes dark.
    channels = [
        channel(1, "AMC", number=1, attached=2),
        channel(2, "Syfy", number=2),
    ]
    case("an attached channel alone in its group", channels,
         [profile(4, "All", all_of(channels))])

    # Country-split groups: with the filter on, `cnn@@US` and `cnn@@UK` are two groups, so one
    # channel of each country survives.
    channels = [
        channel(1, "CNN", number=1, group=100),
        channel(2, "CNN", number=2, group=101),
    ]
    case("country-split groups", channels, [profile(4, "All", all_of(channels))],
         channels_data=[{"name": "CNN", "code": "US"}],
         settings={"restrict_matching_to_country": True})
    # The same two channels with the filter OFF collapse into one group.
    case("country split off", channels, [profile(4, "All", all_of(channels))],
         channels_data=[{"name": "CNN", "code": "US"}],
         settings={"restrict_matching_to_country": False})

    # An ambiguous country (two entries disagreeing) falls back to the group/name signal.
    channels = [
        channel(1, "CNN", number=1, group=100),
        channel(2, "CNN", number=2, group=101),
    ]
    case("an ambiguous country", channels, [profile(4, "All", all_of(channels))],
         channels_data=[{"name": "CNN", "code": "US"}, {"name": "CNN", "code": "UK"}],
         settings={"restrict_matching_to_country": True},
         groups=[{"id": 100, "name": "US: Cable"}, {"id": 101, "name": "UK: Cable"}])

    # An OTA callsign from the database key (`OTA_<callsign>`), and two channels sharing it.
    channels = [
        channel(1, "ABC - AL Montgomery", number=1),
        channel(2, "ABC Montgomery", number=2),
    ]
    case("an OTA callsign group key", channels, [profile(4, "All", all_of(channels))],
         channels_data=[{"name": "ABC - AL Montgomery", "code": "US", "callsign": "WNCF"}])

    # ignore_tags folds names together: with "backup" ignored, both are one group.
    channels = [
        channel(1, "ESPN", number=1),
        channel(2, "ESPN backup", number=2),
    ]
    case("ignore_tags folds two names into one group", channels,
         [profile(4, "All", all_of(channels))],
         settings={"ignore_tags": "backup"})
    case("ignore_tags off leaves them apart", channels,
         [profile(4, "All", all_of(channels))], settings={"ignore_tags": ""})

    # Two profiles: the first is written, both widen the channel set.
    channels = [
        channel(1, "ESPN", number=1),
        channel(2, "ESPN", number=2),
        channel(3, "CNN", number=3),
        channel(4, "Bravo", number=4, streams=0),
    ]
    case("two profiles, the first is written", channels,
         [profile(4, "Kids", [1, 2]), profile(5, "All", [1, 2, 3, 4])],
         profile_name="Kids, All")
    # The winner is enabled in the second profile only: the write enables it in the first.
    channels = [
        channel(1, "ESPN", number=1),
        channel(2, "ESPN", number=2),
        channel(3, "CNN", number=3),
    ]
    case("a winner the host profile had disabled", channels,
         [profile(4, "Kids", [2, 3]), profile(5, "All", [1, 2, 3])],
         profile_name="Kids, All")

    # A channel with no number sorts last within its group (999999).
    channels = [
        channel(1, "MAX", number=None),
        channel(2, "MAX", number=50),
    ]
    case("a channel with no number", channels, [profile(4, "All", all_of(channels))])

    # The group filter, applied by the load step the snapshot comes from.
    channels = [
        channel(1, "ESPN", number=1, group=100),
        channel(2, "ESPN", number=2, group=102),
        channel(3, "CNN", number=3, group=101),
    ]
    case("a group filter", channels, [profile(4, "All", all_of(channels))],
         scope_groups="US: Sports")
    case("an ignored group", channels, [profile(4, "All", all_of(channels))],
         ignore_groups="US: Sports")

    # The same name in two different groups is ONE group: grouping reads the name only.
    channels = [
        channel(1, "PPV EVENT", number=1, group=100),
        channel(2, "PPV EVENT", number=2, group=102),
    ]
    case("the same name in two groups", channels, [profile(4, "All", all_of(channels))])

    # Realistic duplicates: several feeds of one programme, one with an HD tag.
    channels = [
        channel(1, "HBO", streams=4, number=1),
        channel(2, "HBO West", streams=2, number=2),
        channel(3, "HBO East", streams=3, number=3),
        channel(4, "HBO [HD]", streams=1, number=4),
        channel(5, "Cinemax", streams=0, number=5),
    ]
    case("several feeds of one programme", channels, [profile(4, "All", all_of(channels))])

    # Sparse membership rows: channels 2 and 4 have NO row in the host profile (a channel
    # made with an explicit empty profile list). Django's `update()` no-ops on them: the
    # loser stays rowless (False either way) while the winner stays rowless too — hidden —
    # where Dispatcharr's bulk-update endpoint would CREATE the row. The harness counts that
    # winner separately as the one documented divergence.
    channels = [
        channel(1, "ESPN", streams=2, number=2),
        channel(2, "ESPN", streams=1, number=1),
        channel(3, "CNN", streams=1, number=3),
        channel(4, "CNN", streams=3, number=4),
    ]
    case("channels with no membership row in the host profile", channels,
         [profile(4, "All", all_of(channels))], drop_host_rows=[2, 4])

    # The same sparse shape across two profiles: the winner enabled in the second profile
    # only, with no row in the host. The plugin's enable step cannot reach it (no row);
    # the Rust end state names it enabled, which is what the bulk-update write would create.
    channels = [
        channel(1, "ESPN", number=1),
        channel(2, "ESPN", number=2),
        channel(3, "CNN", number=3),
    ]
    case("a winner with no row in the host profile", channels,
         [profile(4, "Kids", [2, 3]), profile(5, "All", [1, 2, 3])],
         profile_name="Kids, All", drop_host_rows=[1])
    return cases


WORDS = ["ESPN", "CNN", "TNT", "HBO", "MAX", "Bravo", "USA", "PPV EVENT", "AMC"]
SUFFIXES = ["", " HD", " [4K]", " [SD]", " [HD]", " West", " East", " 2"]
PREFIXES = ["", "US: ", "UK: "]


def random_case(rng, index):
    """A randomly shaped world: names from the same vocabulary on both sides."""
    channels = []
    for channel_index in range(rng.randint(1, 5)):
        name = rng.choice(PREFIXES) + rng.choice(WORDS) + rng.choice(SUFFIXES)
        channels.append(channel(
            channel_index + 1, name,
            streams=rng.choice([0, 0, 1, 1, 2, 3]),
            group=rng.choice([100, 101, 102]),
            number=rng.choice([None, 0.5, 1, 2, 5, 9]),
            attached=(channel_index + 2) if rng.random() < 0.15 else None))
    ids = [row["id"] for row in channels]
    profiles = [profile(4, "All", ids)]
    if rng.random() < 0.3:
        # A second profile, sometimes the host: the channel set is the union either way.
        extra = rng.sample(ids, rng.randint(0, len(ids)))
        profiles = [profile(5, "Kids", extra), profiles[0]]
    settings = {
        "ignore_quality": rng.random() < 0.8,
        "ignore_regional": rng.random() < 0.8,
        "ignore_geographic": rng.random() < 0.8,
        "ignore_misc": rng.random() < 0.8,
        "ignore_tags": rng.choice(["", "West", "East, West", "backup"]),
        "restrict_matching_to_country": rng.random() < 0.4,
    }
    channels_data = []
    if rng.random() < 0.4:
        channels_data.append({"name": rng.choice(WORDS), "code": rng.choice(["US", "UK"])})
    # Sparse membership rows: with a dense table every pair has a row and Django's
    # `update()` always lands; dropping random host pairs exercises the no-row shape
    # (disable no-ops, enable cannot reach) that separates the ORM from the bulk-update
    # endpoint, which CREATES the missing row.
    drop_host_rows = []
    if rng.random() < 0.5 and len(ids) > 1:
        drop_host_rows = [channel_id for channel_id in ids
                          if rng.random() < 0.25]
    return {
        "comment": f"random #{index}",
        "channels": channels,
        "profiles": profiles,
        "groups": groups(),
        "channels_data": channels_data,
        "settings": settings,
        "profile_name": ", ".join(row["name"] for row in profiles),
        "scope_groups": "",
        "ignore_groups": "",
        "drop_host_rows": drop_host_rows,
    }


# ---------------------------------------------------------------------------------------
# Driving the Rust side
# ---------------------------------------------------------------------------------------

def run_rust(cases, binary):
    requests = "\n".join(json.dumps(case) for case in cases)
    completed = subprocess.run([binary], input=requests, capture_output=True, text=True)
    if completed.returncode != 0:
        raise SystemExit(
            f"the Rust example failed ({completed.returncode}):\n{completed.stderr[-4000:]}")
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
    parser.add_argument("--random", type=int, default=120, help="randomised cases to add")
    parser.add_argument("--seed", type=int, default=20261104, help="seed for random cases")
    parser.add_argument("--binary",
                        default=str(REPO / "target" / "debug" / "examples" / "visibility_io"),
                        help="path to the visibility_io example binary")
    args = parser.parse_args()

    if not PLUGIN_INNER.is_dir():
        print(f"reference plugin not found at {PLUGIN_INNER}; run `git submodule update --init`",
              file=sys.stderr)
        return 2

    print(f"building {args.binary} ...", file=sys.stderr)
    subprocess.run(
        ["cargo", "build", "-q", "-p", "crosstalk-streams", "--example", "visibility_io"],
        cwd=REPO, check=True)

    plugin, matcher = load_plugin()
    rng = random.Random(args.seed)
    cases = worlds() + [random_case(rng, index) for index in range(args.random)]
    answers = run_rust(cases, args.binary)

    mismatches = 0
    compared = 0
    enabled_total = 0
    no_row_enables = 0
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        for case, answer in zip(cases, answers):
            expected = plugin_answer(plugin, matcher, case, workdir)
            end_state = {channel_id: enabled
                         for channel_id, enabled in answer["end_state"]}
            if set(expected["end_state"]) != set(end_state):
                mismatches += 1
                if mismatches <= args.show:
                    print(f"[{case['comment']}] channel set differs: "
                          f"python={sorted(expected['end_state'])} "
                          f"rust={sorted(end_state)}")
                continue
            for channel_id, want in expected["end_state"].items():
                compared += 1
                enabled_total += int(want)
                got = end_state[channel_id]
                if got != want:
                    # The one documented divergence: the channel has NO membership row in
                    # the host profile and the end state names it enabled. Django's
                    # `update(enabled=True)` cannot reach a row that does not exist, so the
                    # plugin leaves it rowless (hidden); Dispatcharr's bulk-update endpoint
                    # CREATES the row (its answer counts it under `created`), so the Rust
                    # write would enable it. A loser with no row agrees on False either way
                    # — and the Rust plan writes nothing for it — so only the enable side
                    # can land here.
                    if not want and got and channel_id in (expected.get("dropped") or []):
                        no_row_enables += 1
                        continue
                    mismatches += 1
                    if mismatches <= args.show:
                        print(f"[{case['comment']}] channel {channel_id} "
                              f"({case['channels'][channel_id - 1]['name'] if channel_id - 1 < len(case['channels']) else '?'})")
                        print(f"  channels={case['channels']}")
                        print(f"  profiles={case['profiles']}")
                        print(f"  settings={case['settings']}")
                        print(f"  dropped host rows={expected.get('dropped')}")
                        print(f"  python={want} rust={got}")
            # The plugin's own sentence counts the winners it TRIED to enable — including a
            # channel with no membership row (a dropped row: the enable `update()` no-ops,
            # so the end state stays False). The Rust winners agree minus exactly those.
            divergent = {channel_id for channel_id in (expected.get("dropped") or [])
                         if not expected["end_state"][channel_id]
                         and end_state.get(channel_id)}
            want_enabled = sum(1 for value in expected["end_state"].values() if value)
            _match = re.search(r"Enabled (\d+) of (\d+) channels", expected["message"])
            if _match is None:
                raise SystemExit(f"the plugin's message changed shape: {expected['message']}")
            if (int(_match.group(1)), int(_match.group(2))) != \
                    (want_enabled + len(divergent), len(expected["end_state"])):
                raise SystemExit(f"the plugin's message disagrees with its end state: "
                                 f"{expected['message']} vs {expected['end_state']}")
            if sorted(set(answer["winners"]) - divergent) != sorted(
                    channel_id for channel_id, value in expected["end_state"].items() if value):
                mismatches += 1
                if mismatches <= args.show:
                    print(f"[{case['comment']}] winners differ: "
                          f"python={sorted(cid for cid, v in expected['end_state'].items() if v)} "
                          f"rust={sorted(answer['winners'])}")

    print(f"[visibility] {mismatches} mismatch(es) over {len(cases)} case(s), "
          f"{compared} channel(s) compared, {enabled_total} left enabled, "
          f"{no_row_enables} no-row enable(s) (the documented bulk-update-creates "
          f"divergence)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
