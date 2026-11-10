#!/usr/bin/env python3
"""Parity: crosstalk's `match_*` against Stream-Mapparr's own match pipeline.

The streams app's matching is Stream-Mapparr's Match & Assign, ported — grouping, the SM
collector, dedupe, same-country, zone routing, `visible_channel_limit`, and the
overwrite/append write list. This script is how that claim is checked: it imports the
plugin's `plugin.py` from `reference/` with the Django stubs its own test suite installs
(`reference/Stream-Mapparr/tests/conftest.py`), drives the REAL methods over generated
fixture worlds, and compares each channel's final stream list with the Rust side
(`cargo run -p crosstalk-streams --example match_io`).

Nothing of the Rust is mirrored here: the plugin's own code decides the expected answer.

What is driven, per case, in the plugin's own order:

  * `_build_channel_groups` (6820) — the grouping key, OTA and country-qualified branches;
  * `_sort_channels_by_priority` (8217) — quality tag then channel number;
  * `_zone_routed_map` (4233) — which channels are ordered by feed zone;
  * `_match_streams_to_channel` (5808) — the collector itself: the OTA callsign path with
    corroboration, the fuzzy winner as a gate, `_collect_alias_streams`, the token/substring
    family with its number guards, the country filter, and `_finalize_streams`;
  * `_channels_to_update_for_group` (4266) — `visible_channel_limit` plus zone siblings;
  * `_streams_to_assign` / `_same_country_ids_for` (4372/4405) — per-channel zone and
    country ordering, and issue 55's "leave a channel alone";
  * `_write_channel_streams` (9764) — overwrite/append and the kept custom tail, run against
    a fake `ChannelStream` table so the resulting row order is the plugin's own.

And for `ota_only` cases, `match_us_ota_only_action` (9156) end to end with the plugin's ORM
accessors stubbed to the fixture.

The EPG path is driven for real too: `_apply_epg_resolution_to_streams`,
`_resolve_epg_matching_settings`, the channel-side substitution and
`_collect_epg_watch_streams` all run, with only the two ORM lookups
(`ProgramData.objects.filter(epg_id=…)`, `EPGData.objects.filter(tvg_id=…)`) stubbed to
the case's fixture. A case's `epg` block supplies the raw programme titles and the
EPG entry ids; the plugin's own cleanup rules, skip titles, "Next Event" date gate and
token containment then decide the answer, exactly as in production.

The one stubbed predicate is `_filter_working_streams`: it applies the plugin's real
`_stream_stats_says_dead` to each stream's stats, plus the case's `dead` list (the probe
verdicts crosstalk reads from its store when the filter is on). A case with a `dead` list
is a run with `filter_dead_streams` on; both sides then exclude exactly the union.

Usage:
    python3 -I scripts/match_parity.py [--show N] [--random N] [--seed N]

Exit status is 1 when any channel's list differs, so it can gate CI.
"""
from __future__ import annotations

import argparse
import itertools
import json
import logging
import random
import subprocess
import sys
import types
from pathlib import Path

sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parent.parent
PLUGIN_INNER = REPO / "reference" / "Stream-Mapparr" / "Stream-Mapparr"
PKG = "stream_mapparr_match_parity"

# What the EPG ORM stubs answer for the case being run: `{"programmes": {epg_id: raw title},
# "entries": {tvg_id: epg_data_id}}`. Raw titles, so the plugin's own gates decide the rest.
EPG_FIXTURE = {"programmes": {}, "entries": {}}

# The local instant the "Next Event: X at TIME on DATE" gate compares against, so a fixture
# with a dated waiting placeholder is not a test that passes on one day of the year.
EPG_NOW = None

SENSITIVITIES = [("relaxed", 70), ("normal", 80), ("strict", 90), ("exact", 95)]


# ---------------------------------------------------------------------------------------
# Loading the plugin without Django, exactly as its own test suite does
# ---------------------------------------------------------------------------------------

def _install_runtime_stubs() -> None:
    """`reference/Stream-Mapparr/tests/conftest.py:_install_runtime_stubs`."""
    from datetime import datetime, timezone as dt_timezone
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

    # The EPG path's two ORM lookups. Everything around them — the cleanup rules, skip
    # titles, the "Next Event" date gate, the token containment — is the plugin's own code;
    # only the row fetch is a fixture, because there is no database here.
    apps_epg = types.ModuleType("apps.epg")
    apps_epg.__path__ = []
    epg_models = types.ModuleType("apps.epg.models")
    sys.modules.update({"apps.epg": apps_epg, "apps.epg.models": epg_models})
    apps.epg = apps_epg
    apps_epg.models = epg_models

    class _EpgAnswer:
        """One `values_list(...).first()`: the row the fixture says exists."""

        def __init__(self, answer):
            self._answer = answer

        def order_by(self, *args, **kwargs):
            return self

        def values_list(self, *args, **kwargs):
            return self

        def first(self):
            return self._answer

    class _EpgManager:
        """`ProgramData.objects` / `EPGData.objects`, resolved against the current case."""

        def filter(self, **kwargs):
            if "epg_id" in kwargs:
                return _EpgAnswer(EPG_FIXTURE["programmes"].get(kwargs["epg_id"]))
            if "tvg_id" in kwargs:
                return _EpgAnswer(EPG_FIXTURE["entries"].get(kwargs["tvg_id"]))
            return _EpgAnswer(None)

    class ProgramData:
        objects = _EpgManager()

    class EPGData:
        objects = _EpgManager()

    epg_models.ProgramData = ProgramData
    epg_models.EPGData = EPGData

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
# A fake ChannelStream table, so `_write_channel_streams` runs for real
# ---------------------------------------------------------------------------------------

class FakeRow:
    """One `ChannelStream` row: the plugin builds these with keyword arguments."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeQuerySet:
    def __init__(self, table, selection):
        self._table = table
        self._selection = selection

    def order_by(self, key):
        self._selection = sorted(self._selection, key=lambda row: getattr(row, key))
        return self

    def values_list(self, field, flat=False):
        values = [getattr(row, field) for row in self._selection]
        return values if flat else [(value,) for value in values]

    def delete(self):
        doomed = {id(row) for row in self._selection}
        self._table.rows = [row for row in self._table.rows if id(row) not in doomed]
        return len(self._selection)

    def exists(self):
        return bool(self._selection)


class FakeChannelStreamTable:
    """`ChannelStream`: both the row constructor and `ChannelStream.objects`.

    Rows are appended in creation order, which is what makes the overwrite and append cases
    comparable: overwrite deletes then writes one row per stream in list order, append writes
    only the new ones (the plugin gives them `order` values that collide with the existing
    rows — Dispatcharr's own behaviour — so the comparison below reads the appended SEQUENCE,
    not a re-sort of colliding keys).
    """

    def __init__(self, rows=None):
        self.rows = list(rows or [])

    def __call__(self, **kwargs):
        """`ChannelStream(channel_id=..., stream_id=..., order=...)` builds one row."""
        return FakeRow(**kwargs)

    @property
    def objects(self):
        return self

    def filter(self, channel_id=None, stream__is_custom=None):
        selection = [row for row in self.rows if row.channel_id == channel_id]
        if stream__is_custom is not None:
            custom = {stream_id for stream_id in self.custom_ids}
            selection = [row for row in selection if
                         (row.stream_id in custom) == bool(stream__is_custom)]
        return FakeQuerySet(self, selection)

    def bulk_create(self, rows):
        self.rows.extend(rows)
        return rows

    def ordered_ids(self, channel_id):
        """The channel's list as `ChannelStream.objects.filter(channel_id=...).order_by('order')`."""
        rows = [row for row in self.rows if row.channel_id == channel_id]
        return [row.stream_id for row in sorted(rows, key=lambda row: row.order)]

    def creation_order_ids(self, channel_id):
        return [row.stream_id for row in self.rows if row.channel_id == channel_id]


# ---------------------------------------------------------------------------------------
# The plugin's own answer for one case
# ---------------------------------------------------------------------------------------

def _threshold(settings) -> int:
    return dict(SENSITIVITIES)[settings.get("match_sensitivity", "normal")]


def _instance(plugin, matcher, case, table):
    """A `Plugin` instance with the attributes the match pipeline reads from `self`."""
    P = plugin.Plugin
    settings = case["settings"]
    inst = P.__new__(P)
    inst.fuzzy_matcher = matcher
    matcher.match_threshold = _threshold(settings)
    # `_ensure_matcher_and_aliases` (4114): the plugin's own alias table plus custom entries.
    inst._alias_map = P._build_alias_map(inst, settings, settings.get("channel_database"))
    # Throughput/quality state, primed exactly as `sort_streams_action` primes it (the match
    # actions only resolve the sort settings they need, and `_sort_streams_by_quality` reads
    # these attributes).
    inst._throughput_state_primed = True
    inst.saved_settings = {
        "audio_channels_priority": settings.get("audio_channels_priority", ""),
        "audio_codec_priority": settings.get("audio_codec_priority", ""),
    }
    inst._prioritize_quality = bool(settings.get("prioritize_quality", False))
    inst._throughput_sorting_enabled = bool(settings.get("enable_throughput_sorting", True))
    inst._throughput_cache = case.get("throughput") or {}
    inst._bitrate_safety_margin = float(settings.get("bitrate_safety_margin", 1.10))
    inst._probe_cache_ttl_minutes = int(settings.get("probe_cache_ttl_minutes", 30))
    inst._content_starved_enabled = P._resolve_demote_content_starved(settings)
    inst._content_starved_floor_kbps = P._resolve_content_starved_floor(settings)
    inst._bitrate_sort_step_kbps = P._resolve_bitrate_sort_step(settings)
    # The dead filter: the plugin's real `_stream_stats_says_dead` over the stream's stats,
    # plus the case's probe-verdict list (what crosstalk reads from the store). Both sides
    # exclude exactly that union, so the stub is a stand-in for the database read only.
    dead = set(case.get("dead") or [])

    def filter_working_streams(streams, logger):
        kept = [stream for stream in streams
                if stream["id"] not in dead
                and not inst._stream_stats_says_dead(stream.get("stats"))]
        # `_filter_working_streams` keeps the whole list when every stream is dead: removing
        # them all would leave nothing to match.
        return kept or list(streams)

    inst._filter_working_streams = filter_working_streams
    # `_write_channel_streams` resolves `ChannelStream` from the module globals.
    pinned = plugin.ChannelStream
    plugin.ChannelStream = table
    return inst, pinned


def _stream_dicts(case, plugin):
    """The case's streams in the dict shape the plugin's actions build (processed_data)."""
    group_names = {group["id"]: group["name"] for group in case.get("groups", [])}
    streams = []
    for stream in case["streams"]:
        streams.append({
            "id": stream["id"],
            "name": stream["name"],
            "channel_group": stream.get("group"),
            "channel_group__name": group_names.get(stream.get("group")),
            "m3u_account": stream.get("account"),
            "url": stream.get("url"),
            "stats": stream.get("stats") or {},
            "is_custom": bool(stream.get("custom", False)),
            "tvg_id": stream.get("tvg_id") or "",
        })
    return streams


def _selected_streams(inst, case, plugin):
    """The stream list `load_process_channels_action` (7540) hands the matcher.

    Both filters are the plugin's own helpers — `_filter_by_group_ids` and
    `_apply_m3u_priorities` — so the selection is its code, not a reimplementation. It has to
    run at all because the collector is handed the POST-selection list: a stream the stream
    scope excluded is not in `processed_data['streams']`, so the plugin never considers it.

    A stream on a SWITCHED-OFF account is a shape production does not produce: Dispatcharr's
    streams endpoint excludes those server-side (which is why crosstalk's selection drops them
    again as defense in depth), so the fixtures carry none.
    """
    P = plugin.Plugin
    settings = case["settings"]
    streams = _stream_dicts(case, plugin)
    group_name_to_id = {group["name"]: group["id"] for group in case.get("groups", [])}
    selected_groups = [name.strip() for name in
                       (settings.get("selected_stream_groups") or "").split(",") if name.strip()]
    if selected_groups:
        wanted = [group_name_to_id[name] for name in selected_groups
                  if name in group_name_to_id]
        if wanted:
            streams = P._filter_by_group_ids(
                streams, wanted, settings.get("stream_group_filter_mode", "include"),
                "channel_group")
    selected_m3us = (settings.get("selected_m3us") or "").strip()
    if selected_m3us:
        names = {account["name"]: account["id"] for account in case.get("accounts", [])}
        priorities = P._resolve_m3u_priorities(selected_m3us, names)
        if not priorities["priority_map"] and priorities["other_priority"] is None:
            for stream in streams:
                stream["_m3u_priority"] = P.M3U_UNLISTED_PRIORITY
        else:
            streams = P._apply_m3u_priorities(streams, priorities)
    else:
        for stream in streams:
            stream["_m3u_priority"] = P.M3U_UNLISTED_PRIORITY
    return streams


def _label_streams(streams, table_ids):
    return [stream["name"] for stream in streams]


def plugin_answer(plugin, matcher, case):
    """Run the plugin's real pipeline over the case; return {channel_id: {...}}."""
    P = plugin.Plugin
    settings = case["settings"]
    logger = logging.getLogger("match_parity")
    # Dispatcharr always persists the EPG block with its defaults, and the plugin reads the
    # pattern/cleanup keys as-is (only the skip-title, schedule and watch lists fall back).
    # Fill the absent keys so both sides see the shipped defaults, which is what an installed
    # plugin actually runs with.
    settings = dict(settings)
    for key in ("epg_placeholder_name_patterns", "epg_title_cleanup_rules",
                "epg_skip_titles", "epg_channel_schedule_cleanup_rules",
                "epg_watch_source_streams"):
        settings.setdefault(key, getattr(plugin.PluginConfig, f"DEFAULT_{key.upper()}"))
    # The EPG fixture: the raw titles and entry ids, plus the local day the date gate
    # compares against. Anything the case does not say is absent (no current programme).
    global EPG_FIXTURE, EPG_NOW
    epg = case.get("epg") or {}
    EPG_FIXTURE = {
        "programmes": dict(epg.get("programmes") or {}),
        "entries": dict(epg.get("entries") or {}),
    }
    from datetime import datetime, timezone as dt_timezone
    month, day = (int(part) for part in (epg.get("today") or "8-5").split("-"))
    EPG_NOW = datetime(2026, month, day, 10, 0, tzinfo=dt_timezone.utc)
    # A case with `now` + `timezone` pins the run to a real instant in a non-UTC zone
    # (an event near midnight, where the zone-local day differs from UTC's). The plugin's
    # gate reads `_epg_local_now` — Dispatcharr's zone, not UTC — and the Rust side reads
    # the same zone-local day from the case's `today`, so the two must agree exactly here.
    epg_zone = epg.get("timezone")
    epg_now = epg.get("now")
    if epg_zone and epg_now:
        from zoneinfo import ZoneInfo
        instant = datetime.fromisoformat(str(epg_now).replace("Z", "+00:00"))
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=dt_timezone.utc)
        EPG_NOW = instant.astimezone(ZoneInfo(epg_zone))

    # The fixture rows: every channel's current list, one row per stream.
    table = FakeChannelStreamTable()
    streams_by_id = {stream["id"]: stream for stream in case["streams"]}
    for channel in case["channels"]:
        for order, stream_id in enumerate(channel["streams"]):
            table.rows.append(FakeRow(channel_id=channel["id"], stream_id=stream_id, order=order))
    table.custom_ids = {stream["id"] for stream in case["streams"] if stream.get("custom")}

    inst, pinned = _instance(plugin, matcher, case, table)
    inst._epg_local_now = lambda: EPG_NOW
    # Dispatcharr's zone, as the plugin reads it in-process: the gate's `_epg_local_now`
    # consults this, so a zone-pinned case stubs it to the same value the Rust side
    # derived `today` from. Without a pinned zone this is UTC, like the default EPG_NOW.
    inst._dispatcharr_timezone = lambda settings=None: epg.get("timezone") or "UTC"
    try:
        if settings.get("ota_only"):
            return {"assignments": _ota_answer(plugin, inst, case, table, streams_by_id),
                    "country": None}

        tags = P._parse_tags(settings.get("ignore_tags", ""))
        iq = bool(settings.get("ignore_quality", True))
        ir = bool(settings.get("ignore_regional", True))
        ig = bool(settings.get("ignore_geographic", True))
        im = bool(settings.get("ignore_misc", True))
        streams = _selected_streams(inst, case, plugin)
        rules, _report = inst._resolve_stream_regex_rules(settings)
        plugin._apply_regex_rules_to_streams(streams, rules, logger)
        # The EPG stream side, in the plugin's own order: after the regex rules, overriding
        # their match_name for placeholder streams only.
        epg_settings = inst._resolve_epg_matching_settings(settings)
        epg_title_cache = {}
        inst._apply_epg_resolution_to_streams(streams, settings, logger, epg_title_cache)
        # `precompute_normalizations` exactly as both actions call it, so the candidate cache
        # carries the run's tags and flags.
        matcher.precompute_normalizations(
            [s["match_name"] for s in streams], tags,
            ignore_quality=iq, ignore_regional=ir, ignore_geographic=ig, ignore_misc=im)

        restrict = inst._resolve_restrict_matching_to_country(settings)
        allow_same = bool(settings.get("allow_same_name_streams", False))
        overwrite = bool(settings.get("overwrite_streams", True))
        keep_custom = P._resolve_keep_custom_streams(settings)
        limit = int(settings.get("visible_channel_limit", 1))
        # A case with a `dead` list is a run with `filter_dead_streams` on.
        filter_dead = bool(settings.get("filter_dead_streams", False))
        channels_data = [{"channel_name": entry["name"], "_country_code": entry["code"],
                          **({"callsign": entry["callsign"]} if entry.get("callsign") else {})}
                         for entry in case.get("channels_data", [])]
        channels = [{
            "id": channel["id"],
            "name": channel["name"],
            "channel_number": channel.get("number", 999999),
            "channel_group__name": {group["id"]: group["name"]
                                    for group in case.get("groups", [])}.get(channel.get("group")),
            "streams": channel["streams"],
            "epg_data_id": channel.get("epg_data_id"),
        } for channel in case["channels"]]

        channel_info_cache = {}
        groups = inst._build_channel_groups(
            channels, channels_data, logger, tags, iq, ir, ig, im, restrict, channel_info_cache)
        zone_routed = inst._zone_routed_map(
            [channel for group in groups.values() for channel in group], tags, iq, ir, ig, im)
        stream_country_memo = (
            inst._build_stream_country_memo(
                streams, inst._resolve_prefix_country_overrides(settings))
            if restrict else None)
        country_stats = {"engaged": 0, "skipped_unknown_channel": 0,
                         "foreign_dropped": 0, "unknown_kept": 0}

        answers = {}
        for group_channels in groups.values():
            sorted_channels = inst._sort_channels_by_priority(group_channels)
            matched, _cleaned, _names, reason, _database = inst._match_streams_to_channel(
                sorted_channels[0], streams, logger, tags, iq, ir, ig, im, channels_data,
                filter_dead, restrict, allow_same, country_stats=country_stats,
                stream_country_memo=stream_country_memo, channel_info_cache=channel_info_cache,
                epg_settings=epg_settings, epg_title_cache=epg_title_cache)
            to_update = inst._channels_to_update_for_group(sorted_channels, limit, zone_routed)
            for channel in sorted_channels:
                wanted = channel in to_update
                before = table.ordered_ids(channel["id"])
                entry = {
                    "before": before,
                    "after": before,
                    "matched": len(matched),
                    "keep_existing": not wanted,
                    "updated": wanted,
                    "will_write": False,
                    "reason": reason,
                    "zone": zone_routed.get(channel["id"], "DEFAULT") if
                            channel["id"] in zone_routed else None,
                }
                answers[channel["id"]] = entry
            for channel in to_update:
                same_country_ids = inst._same_country_ids_for(
                    channel, matched, channels_data, logger, restrict,
                    stream_country_memo=stream_country_memo,
                    channel_info_cache=channel_info_cache, ignore_tags=tags)
                streams_for_channel, keep_existing = inst._streams_to_assign(
                    matched, channel["id"], zone_routed, same_country_ids)
                entry = answers[channel["id"]]
                entry["keep_existing"] = keep_existing
                if keep_existing or not matched:
                    entry["after"] = entry["before"]
                    entry["will_write"] = False
                    continue
                stream_ids = [stream["id"] for stream in streams_for_channel]
                inst._write_channel_streams(
                    channel["id"], stream_ids, overwrite, keep_custom, logger)
                if overwrite:
                    entry["after"] = table.ordered_ids(channel["id"])
                else:
                    entry["after"] = table.creation_order_ids(channel["id"])
                entry["will_write"] = entry["after"] != entry["before"]
        return {"assignments": answers, "country": country_stats}
    finally:
        plugin.ChannelStream = pinned


def _ota_answer(plugin, inst, case, table, streams_by_id):
    """`match_us_ota_only_action`, end to end, with the plugin's ORM accessors stubbed."""
    P = plugin.Plugin
    # The shipped EPG defaults, as in `plugin_answer`: Dispatcharr persists the block
    # with its defaults, so production OTA runs see them. Without this the plugin side
    # resolves no placeholder (absent patterns mean `enabled and bool([])` is False)
    # while the Rust side — whose input fills the same defaults — does, and the two
    # disagree on every EPG-on OTA case.
    settings = dict(case["settings"])
    for key in ("epg_placeholder_name_patterns", "epg_title_cleanup_rules",
                "epg_skip_titles", "epg_channel_schedule_cleanup_rules",
                "epg_watch_source_streams"):
        settings.setdefault(key, getattr(plugin.PluginConfig, f"DEFAULT_{key.upper()}"))
    logger = logging.getLogger("match_parity")
    channels = [{
        "id": channel["id"],
        "name": channel["name"],
        "channel_number": channel.get("number", 999999),
        "channel_group__name": {group["id"]: group["name"]
                                for group in case.get("groups", [])}.get(channel.get("group")),
        "epg_data_id": channel.get("epg_data_id"),
    } for channel in case["channels"]]
    streams = _selected_streams(inst, case, plugin)
    rules, _report = inst._resolve_stream_regex_rules(settings)
    plugin._apply_regex_rules_to_streams(streams, rules, logger)

    profile = {"id": 1, "name": "All"}
    inst._get_all_profiles = lambda logger: [profile]
    inst._get_all_channels = lambda logger: channels
    inst._get_all_streams = lambda logger: streams
    inst._get_all_groups = lambda logger: []
    inst._get_m3u_name_map = lambda logger: {}
    inst.validate_settings_action = lambda settings, logger: {"status": "success"}
    inst._send_progress_update = lambda *args, **kwargs: None
    inst._record_streams_matched = lambda *args, **kwargs: None
    inst._prune_exports_after_write = lambda *args, **kwargs: None
    inst._trigger_frontend_refresh = lambda *args, **kwargs: None
    inst._label_streams = lambda matched, logger: [stream["name"] for stream in matched]
    before_by_channel = {channel["id"]: table.ordered_ids(channel["id"]) for channel in channels}
    inst._generate_csv_header_comment = lambda *args, **kwargs: "# parity\n"
    membership = sys.modules["apps.channels.models"].ChannelProfileMembership
    membership.objects.filter.return_value.values_list.return_value = [
        channel["id"] for channel in channels]

    import tempfile
    from pathlib import Path
    exports = Path(tempfile.mkdtemp())
    original = P.EXPORTS_DIR if hasattr(P, "EXPORTS_DIR") else None
    P.EXPORTS_DIR = str(exports)
    try:
        answer = inst.match_us_ota_only_action(dict(settings, dry_run_mode=False), logger)
    finally:
        if original is not None:
            P.EXPORTS_DIR = original
    if answer.get("status") != "success":
        raise SystemExit(f"the plugin's OTA action failed: {answer}")

    answers = {}
    for channel in channels:
        before = before_by_channel[channel["id"]]
        after = table.ordered_ids(channel["id"])
        answers[channel["id"]] = {
            "before": before,
            "after": after,
            "matched": None,          # the action does not report a per-channel match count
            "keep_existing": False,
            "updated": after != before,
            "will_write": after != before,
            "reason": None,
            "zone": None,
        }
    return answers


# ---------------------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------------------

def _iso(delta_seconds: float) -> str:
    from datetime import datetime, timedelta, timezone as dt_timezone
    return (datetime.now(dt_timezone.utc) + timedelta(seconds=delta_seconds)).isoformat()


def _entry(mbps, age_seconds=None, nominal=None):
    payload = {
        "throughput_mbps": mbps,
        "throughput_measured_at": _iso(-age_seconds) if age_seconds is not None else None,
    }
    if nominal is not None:
        payload["nominal_bitrate_mbps"] = nominal
    return payload


STATS = [
    {"width": 3840, "height": 2160, "source_fps": 50, "video_bitrate": 9000},
    {"width": 1920, "height": 1080, "source_fps": 60, "video_bitrate": 2656},
    {"width": 1280, "height": 720, "source_fps": 50, "video_bitrate": 1500},
    {"width": 720, "height": 576, "source_fps": 25},
    {},
]

ACCOUNTS = [
    {"id": 1, "name": "Main", "active": True},
    {"id": 2, "name": "Backup", "active": True},
    # Switched off: production never yields its streams (see `_selected_streams`).
    {"id": 3, "name": "Retired", "active": False},
    {"id": 9, "name": "Custom", "active": True},
]

GROUPS = [
    {"id": 100, "name": "TVE"},
    {"id": 101, "name": "Cable"},
    {"id": 102, "name": "US: Sports"},
    {"id": 103, "name": "UK: Sports"},
]


def stream(stream_id, name, account=1, group=100, custom=False, url=None, stats=None,
           tvg_id=""):
    return {
        "id": stream_id,
        "name": name,
        "account": account,
        "group": group,
        "custom": custom,
        "url": url if url is not None else f"http://example/{stream_id}",
        "stats": STATS[stream_id % len(STATS)] if stats is None else stats,
        # The guide identifier the EPG placeholder path joins on.
        "tvg_id": tvg_id,
    }


def channel(channel_id, name, streams=None, group=100, number=None, epg_data_id=None):
    return {
        "id": channel_id,
        "name": name,
        "group": group,
        "number": number if number is not None else channel_id,
        "streams": streams or [],
        # The EPG record the channel's own column points at.
        "epg_data_id": epg_data_id,
    }


def worlds():
    """The fixture worlds, one per shape the collector has a branch for."""
    cases = []

    # 1. A plain name family across two accounts, an HD tag, and a same-named duplicate.
    cases.append({
        "comment": "name family across accounts",
        "channels": [channel(1, "ESPN", []), channel(2, "ESPN2", [])],
        "streams": [
            stream(1001, "ESPN"), stream(1002, "ESPN HD"), stream(1003, "ESPN [4K]"),
            stream(1004, "ESPN", account=2), stream(1005, "ESPN2"), stream(1006, "ESPN2 HD"),
            stream(1007, "CNN", account=1),
        ],
    })

    # 2. Numbered family: issue #62's "Event 1 must not collect Event 12".
    cases.append({
        "comment": "numbered event family",
        "channels": [channel(1, "NebulaOne Event 1"),
                     channel(2, "NebulaOne Event 12"),
                     channel(3, "NebulaOne Event 2")],
        "streams": [stream(2000 + number, "NebulaOne Event %d HD (WEB)" % number)
                    for number in range(1, 21)],
    })

    # 3. Aliases: a channel whose streams are only reachable through the alias table.
    cases.append({
        "comment": "alias force-include",
        "channels": [channel(1, "MSNBC"), channel(2, "Antena 3")],
        "streams": [
            stream(3001, "MS Now"), stream(3002, "MSNBC HD"), stream(3003, "Antena3"),
            stream(3004, "Antena 3 HD"),
        ],
    })

    # 4. OTA callsigns: a paren callsign, a corroborated common-word callsign, a suffix.
    cases.append({
        "comment": "OTA callsigns",
        "channels": [channel(1, "ABC - AL Montgomery (WNCF)"),
                     channel(2, "KING Seattle"), channel(3, "CW - Chicago (WGN)"),
                     channel(4, "24/7 KING OF THE HILL")],
        "streams": [
            stream(4001, "WNCF"), stream(4002, "WNCF-DT"), stream(4003, "ABC 32 Montgomery"),
            stream(4004, "KING 5 Seattle NBC"), stream(4005, "KING OF THE HILL"),
            stream(4006, "WGN Chicago"), stream(4007, "WGN9"),
        ],
        "channels_data": [{"name": "ABC - AL Montgomery", "code": "US"}],
    })

    # 5. Zones: an East/West sibling pair, and a lone marked channel that must not route.
    cases.append({
        "comment": "zone routing",
        "channels": [channel(1, "Starz Encore"), channel(2, "STARZ Encore (W)"),
                     channel(3, "Key West")],
        "streams": [
            stream(5001, "Starz Encore"), stream(5002, "Starz Encore (W)"),
            stream(5003, "STARZ Encore West"), stream(5004, "Key West"),
        ],
    })

    # 6. visible_channel_limit with a shared name in two groups and a quality tag.
    cases.append({
        "comment": "visible channel limit",
        "channels": [channel(1, "ESPN [HD]", group=100, number=2),
                     channel(2, "ESPN", group=101, number=1),
                     channel(3, "ESPN [4K]", group=100, number=3)],
        "streams": [stream(6001, "ESPN"), stream(6002, "ESPN HD"), stream(6003, "ESPN 4K")],
        "settings": {"visible_channel_limit": 2},
    })

    # 7. Custom streams: kept last, dropped when the setting is off.
    cases.append({
        "comment": "custom streams",
        "channels": [channel(1, "ESPN", [6001, 7001])],
        "streams": [
            stream(6001, "ESPN HD"), stream(7001, "Could Not Dispatch", account=9, custom=True),
            stream(7002, "Local Insert", account=9, custom=True),
        ],
    })

    # 8. Countries: a US channel and a UK channel whose names collapse, plus prefixes.
    cases.append({
        "comment": "country restriction",
        "channels": [channel(1, "CNN", group=102), channel(2, "CNN", group=103)],
        "streams": [
            stream(8001, "CNN", group=100), stream(8002, "US: CNN", group=100),
            stream(8003, "UK: CNN", group=101), stream(8004, "CNN International", group=101),
        ],
        "channels_data": [{"name": "CNN", "code": "US"}],
        "settings": {"restrict_matching_to_country": True},
    })

    # 9. A channel whose every match is a foreign feed: the filter empties the group.
    cases.append({
        "comment": "everything foreign",
        "channels": [channel(1, "CNN", group=103)],
        "streams": [stream(8001, "UK: CNN"), stream(8002, "US: CNN")],
        "settings": {"restrict_matching_to_country": True,
                     "stream_prefix_countries": "UK=UK"},
    })

    # 10. Substring family: "euro news" vs "euronews", and a longer sibling that must not join.
    cases.append({
        "comment": "word-break family",
        "channels": [channel(1, "Euro News"), channel(2, "UnXplained Zone"),
                     channel(3, "Premier Sports 1")],
        "streams": [
            stream(9001, "euronews"), stream(9002, "Euronews HD"),
            stream(9003, "un xplained zone"), stream(9004, "UnXplained Zone HD"),
            stream(9005, "Premier Sports 1"), stream(9006, "Premier Sports 2"),
        ],
    })

    # 11. Same-name streams from one account, with the URL joining the key.
    cases.append({
        "comment": "same-name streams",
        "channels": [channel(1, "DAZN F1", [])],
        "streams": [
            stream(10001, "DAZN F1", url="http://example/a"),
            stream(10002, "DAZN F1", url="http://example/b"),
            stream(10003, "DAZN F1", url="http://example/a"),
        ],
    })

    # 12. Append mode onto a channel that already holds streams, with a kept custom tail.
    cases.append({
        "comment": "append mode",
        "channels": [channel(1, "ESPN", [11001, 11009])],
        "streams": [
            stream(11001, "ESPN", account=2), stream(11002, "ESPN HD"),
            stream(11009, "Could Not Dispatch", account=9, custom=True),
        ],
        "settings": {"overwrite_streams": False},
    })

    # 13. An ineligible account and a stream group filter.
    cases.append({
        "comment": "stream selection",
        "channels": [channel(1, "ESPN", [])],
        "streams": [
            stream(12001, "ESPN", account=2), stream(12002, "ESPN HD", account=1),
            stream(12003, "ESPN 4K", account=2, group=101),
        ],
        "settings": {"selected_m3us": "Main, *", "selected_stream_groups": "TVE",
                     "stream_group_filter_mode": "include"},
    })

    # 14. Regex rules: a rule that reshapes the name matching sees.
    cases.append({
        "comment": "regex rules",
        "channels": [channel(1, "ESPN", [])],
        "streams": [stream(13001, "ESPN ▎ HD"), stream(13002, "ESPN VIP HD")],
        "settings": {"stream_name_regex_rules": r'[["\\s*▎\\s*", " "], ["\\bVIP\\b", ""]]'},
    })

    # 15. Ignore tags on both sides.
    cases.append({
        "comment": "ignore tags",
        "channels": [channel(1, "ESPN"), channel(2, "TNT")],
        "streams": [stream(14001, "ESPN [DEAD]"), stream(14002, "ESPN backup"),
                    stream(14003, "TNT [DEAD] HD")],
        "settings": {"ignore_tags": "[DEAD], backup"},
    })

    # 16. Dead streams: the group matched nothing else, so nothing may be written. The
    # filter reads every stats shape the plugin's test accepts: empty, zero width/height
    # and a 0x0 resolution, plus the verdict list.
    cases.append({
        "comment": "dead streams",
        "channels": [channel(1, "ESPN", [14002]), channel(2, "CNN", [])],
        "streams": [
            stream(15001, "ESPN", stats={}), stream(15002, "ESPN HD"),
            stream(15004, "ESPN", stats={"width": 0, "height": 720}),
            stream(15005, "ESPN", stats={"resolution": "0x0"}),
            stream(14002, "ESPN backup"), stream(15003, "CNN"),
        ],
        "dead": [15001],
    })

    # 21. Punctuation and non-ASCII names: the normalisation both sides share must agree
    # before any narrowing is even asked.
    cases.append({
        "comment": "punctuation and unicode",
        "channels": [channel(1, "Caf\u00e9 M\u00fcller"), channel(2, "Discovery+"),
                     channel(3, "24/7 South Park")],
        "streams": [
            stream(20001, "Cafe Muller"), stream(20002, "Caf\u00e9 M\u00fcller HD"),
            stream(20003, "Discovery Plus"), stream(20004, "Discovery+ HD"),
            stream(20005, "24/7 South Park"), stream(20006, "South Park"),
        ],
    })

    # 22. EPG placeholder matching, channel side: a generic slot channel matched under the
    # programme that is airing on it, and the same substitution on the stream side.
    cases.append({
        "comment": "epg placeholder channel and stream",
        "channels": [channel(1, "PPV EVENT 04", [], epg_data_id=7)],
        "streams": [
            stream(30001, "WWE SummerSlam", tvg_id="wwe.us"),
            stream(30002, "WWE SummerSlam HD", tvg_id="wwe.us"),
            stream(30003, "PPV EVENT 04", tvg_id="ppv04.us"),
            stream(30004, "PPV EVENT 09", tvg_id="ppv09.us"),
        ],
        "settings": {"epg_placeholder_matching_enabled": True},
        "epg": {
            "entries": {"ppv04.us": 7, "ppv09.us": 8},
            "programmes": {7: "WWE SummerSlam"},
        },
    })

    # 23. The case the feature exists for: the CHANNEL carries the real name and the
    # provider's stream is the generic slot, joined through tvg_id -> EPG entry.
    cases.append({
        "comment": "epg placeholder stream only",
        "channels": [channel(1, "WWE SummerSlam")],
        "streams": [
            stream(30101, "PPV EVENT 04", tvg_id="ppv04.us"),
            stream(30102, "PPV EVENT 09", tvg_id="ppv09.us"),
            stream(30103, "CNN", tvg_id="cnn.us"),
        ],
        "settings": {"epg_placeholder_matching_enabled": True},
        "epg": {
            "entries": {"ppv04.us": 7, "ppv09.us": 8, "cnn.us": 9},
            # An idle slot and a skip title are both no signal.
            "programmes": {7: "WWE SummerSlam", 8: "No Event Today", 9: "Signing Off"},
        },
    })

    # 24. The watched-stream rule: a real, permanently-named stream whose current programme
    # contains the whole target channel name is force-included as an alternate, without
    # changing its own identity.
    cases.append({
        "comment": "epg watch source streams",
        "channels": [channel(1, "WWE SummerSlam"), channel(2, "ESPN"), channel(3, "Max")],
        "streams": [
            stream(30201, "ESPN", tvg_id="espn.us"),
            stream(30202, "ESPN2"),
            stream(30203, "WWE SummerSlam HD"),
            stream(30204, "CNN"),
        ],
        "settings": {"epg_placeholder_matching_enabled": True,
                     "epg_watch_source_streams": "ESPN"},
        "epg": {
            "entries": {"espn.us": 8},
            "programmes": {8: "WWE SummerSlam Special"},
        },
    })

    # 25. The channel's inline schedule, a "Next Event" wrapper dated today, and one dated
    # another day (which must not be reported as today's event).
    cases.append({
        "comment": "epg schedule cleanup and next-event dates",
        "channels": [channel(1, "WWE SummerSlam | Monday @ 5"),
                     channel(2, "PPV EVENT 04", epg_data_id=7),
                     channel(3, "PPV EVENT 05", epg_data_id=8)],
        "streams": [stream(30301, "WWE SummerSlam"), stream(30302, "WWE SummerSlam HD")],
        "settings": {"epg_placeholder_matching_enabled": True},
        "epg": {
            "entries": {},
            "programmes": {
                7: "Next Event: WWE SummerSlam at 05:00PM on Aug 5",
                8: "Next Event: WWE SummerSlam at 05:00PM on Aug 9",
            },
            "today": "8-5",
        },
    })

    # 26. A custom pattern list and custom cleanup rules: the plugin's own resolvers decide
    # what is eligible and what the title becomes.
    cases.append({
        "comment": "epg custom patterns and cleanup",
        "channels": [channel(1, "SLOT 7", [], epg_data_id=7)],
        "streams": [stream(30401, "Main Event", tvg_id="main.us"),
                    stream(30402, "SLOT 7", tvg_id="slot7.us")],
        "settings": {
            "epg_placeholder_matching_enabled": True,
            "epg_placeholder_name_patterns": r"^SLOT \d+$",
            "epg_title_cleanup_rules": r'[["\\s*\\[LIVE\\]\\s*", ""]]',
            "epg_skip_titles": "Off Air",
        },
        "epg": {
            "entries": {"main.us": 7, "slot7.us": 7},
            "programmes": {7: "[LIVE] Main Event"},
        },
    })

    # 27. The feature enabled with an empty pattern list: nothing is a placeholder, so the
    # names match exactly as they are written.
    cases.append({
        "comment": "epg enabled with no patterns",
        "channels": [channel(1, "PPV EVENT 04", [], epg_data_id=7)],
        "streams": [stream(30501, "WWE SummerSlam", tvg_id="wwe.us"),
                    stream(30502, "PPV EVENT 04", tvg_id="ppv04.us")],
        "settings": {"epg_placeholder_matching_enabled": True,
                     "epg_placeholder_name_patterns": ""},
        "epg": {"entries": {"ppv04.us": 7, "wwe.us": 9}, "programmes": {7: "WWE SummerSlam"}},
    })

    # 28. An unanchored pattern is not a placeholder: eligibility is a whole-name fullmatch,
    # not a substring search, so "PPV" must not turn "PPV EVENT 04" into "WWE SummerSlam".
    # The slot then matches its own literal stream, on both sides.
    cases.append({
        "comment": "epg unanchored pattern is not a placeholder",
        "channels": [channel(1, "PPV EVENT 04", [], epg_data_id=7)],
        "streams": [
            stream(30601, "WWE SummerSlam", tvg_id="wwe.us"),
            stream(30602, "PPV EVENT 04", tvg_id="ppv04.us"),
        ],
        "settings": {"epg_placeholder_matching_enabled": True,
                     "epg_placeholder_name_patterns": "PPV"},
        "epg": {"entries": {"ppv04.us": 7, "wwe.us": 9},
                "programmes": {7: "WWE SummerSlam"}},
    })

    # 29. Skip titles compare case-insensitively: a shouting idle slot is still idle, and
    # the channel falls back to its literal name on both sides.
    cases.append({
        "comment": "epg skip title case",
        "channels": [channel(1, "WWE SummerSlam")],
        "streams": [
            stream(30701, "PPV EVENT 04", tvg_id="ppv04.us"),
            stream(30702, "PPV EVENT 09", tvg_id="ppv09.us"),
        ],
        "settings": {"epg_placeholder_matching_enabled": True},
        "epg": {"entries": {"ppv04.us": 7, "ppv09.us": 8},
                "programmes": {7: "NO EVENT TODAY", 8: "signing off"}},
    })

    # 30. The midnight boundary: a programme dated yesterday is not today's event. One slot
    # resolves (dated today) and its sibling does not (dated yesterday), in the same pass.
    cases.append({
        "comment": "epg next-event midnight boundary",
        "channels": [channel(1, "PPV EVENT 04", [], epg_data_id=7),
                     channel(2, "PPV EVENT 05", [], epg_data_id=8)],
        "streams": [stream(30801, "WWE SummerSlam"),
                    stream(30802, "WWE SummerSlam HD")],
        "settings": {"epg_placeholder_matching_enabled": True},
        "epg": {
            "entries": {},
            "programmes": {
                7: "Next Event: WWE SummerSlam at 11:59PM on Aug 5",
                8: "Next Event: WWE SummerSlam at 12:01AM on Aug 6",
            },
            "today": "8-6",
        },
    })

    # 31. The watched-stream token floor: a one-token channel ("Max") is too generic to
    # force-include on, even when the watched stream is airing exactly that word. The
    # two-token channel in the same pass is unaffected and still matches by name.
    cases.append({
        "comment": "epg watch single-token channel",
        "channels": [channel(1, "Max"), channel(2, "WWE SummerSlam")],
        "streams": [
            stream(30901, "ESPN", tvg_id="espn.us"),
            stream(30902, "MAX 100"),
            stream(30903, "WWE SummerSlam Special"),
        ],
        "settings": {"epg_placeholder_matching_enabled": True,
                     "epg_watch_source_streams": "ESPN"},
        "epg": {
            "entries": {"espn.us": 8},
            "programmes": {8: "Max Championship Special"},
        },
    })

    # 32. OTA with EPG on: the stream side still resolves (a placeholder slot airing the
    # callsign matches by its resolved title) while the channel side stays raw — OTA
    # extracts the callsign from the written name, never the programme title.
    cases.append({
        "comment": "us ota only with epg on",
        "channels": [channel(1, "ABC - AL Montgomery (WNCF)"),
                     channel(2, "Not A Station")],
        "streams": [
            stream(31001, "PPV EVENT 04", tvg_id="ppv04.us"),
            stream(31002, "WNCF-DT2"),
        ],
        "settings": {"ota_only": True, "epg_placeholder_matching_enabled": True},
        "epg": {"entries": {"ppv04.us": 7}, "programmes": {7: "WNCF"}},
    })

    # 33. The zone-local day, ahead of UTC: 00:30 on Aug 6 in Pacific/Auckland is still
    # Aug 5 in UTC. A title dated Aug 6 IS today's event in Dispatcharr's zone (and the
    # run must resolve it), while the Aug 5 one is yesterday's and must not resolve —
    # a UTC-based gate would answer exactly backwards on both. Both sides read the same
    # zone: the plugin through the stubbed `_dispatcharr_timezone`, the Rust side through
    # the `today` the harness derived from that zone for the same instant.
    cases.append({
        "comment": "epg next-event date in a zone ahead of UTC",
        "channels": [channel(1, "PPV EVENT 04", [], epg_data_id=7),
                     channel(2, "PPV EVENT 05", [], epg_data_id=8)],
        "streams": [stream(31101, "WWE SummerSlam"),
                    stream(31102, "WWE SummerSlam HD")],
        "settings": {"epg_placeholder_matching_enabled": True},
        "epg": {
            "timezone": "Pacific/Auckland",
            "now": "2026-08-05T12:30:00Z",
            "today": "8-6",
            "entries": {},
            "programmes": {
                7: "Next Event: WWE SummerSlam at 12:30AM on Aug 6",
                8: "Next Event: WWE SummerSlam at 11:59PM on Aug 5",
            },
        },
    })

    # 34. The same, behind UTC: 22:30 on Aug 5 in America/New_York is Aug 6 in UTC.
    # The Aug 5 title resolves and the Aug 6 one does not — again backwards under UTC.
    cases.append({
        "comment": "epg next-event date in a zone behind UTC",
        "channels": [channel(1, "PPV EVENT 04", [], epg_data_id=7),
                     channel(2, "PPV EVENT 05", [], epg_data_id=8)],
        "streams": [stream(31201, "WWE SummerSlam"),
                    stream(31202, "WWE SummerSlam HD")],
        "settings": {"epg_placeholder_matching_enabled": True},
        "epg": {
            "timezone": "America/New_York",
            "now": "2026-08-06T02:30:00Z",
            "today": "8-5",
            "entries": {},
            "programmes": {
                7: "Next Event: WWE SummerSlam at 10:30PM on Aug 5",
                8: "Next Event: WWE SummerSlam at 12:30AM on Aug 6",
            },
        },
    })

    # 17. US OTA only: upper-case callsigns, a lowercase one that must be ignored, and a
    # channel without a station.
    cases.append({
        "comment": "us ota only",
        "channels": [channel(1, "ABC - AL Montgomery (WNCF)"),
                     channel(2, "KING Seattle"), channel(3, "Not A Station")],
        "streams": [
            stream(16001, "WNCF"), stream(16002, "WNCF-DT2"), stream(16003, "wncf"),
            stream(16004, "KING 5 Seattle NBC"), stream(16005, "KING OF THE HILL"),
        ],
        "settings": {"ota_only": True},
    })

    # 19. Zones AND countries at once: the country tier is the OUTER key of the zone reorder.
    cases.append({
        "comment": "zone routing under the country filter",
        "channels": [channel(1, "Starz Encore", group=102), channel(2, "STARZ Encore (W)", group=102)],
        "streams": [
            stream(18001, "US: Starz Encore"), stream(18002, "US: Starz Encore (W)"),
            stream(18003, "Starz Encore West"), stream(18004, "UK: Starz Encore"),
        ],
        "channels_data": [{"name": "Starz Encore", "code": "US"}],
        "settings": {"restrict_matching_to_country": True},
    })

    # 20. A visible_channel_limit wider than the group, over three same-named channels.
    cases.append({
        "comment": "limit wider than the group",
        "channels": [channel(1, "TNT", group=100, number=3),
                     channel(2, "TNT", group=101, number=1)],
        "streams": [stream(19001, "TNT"), stream(19002, "TNT HD")],
        "settings": {"visible_channel_limit": 5},
    })

    # 18. A channel matching nothing at all: bug-063's "never clear a working channel".
    cases.append({
        "comment": "no match leaves the channel alone",
        "channels": [channel(1, "Totally Unmatched", [17001, 17002])],
        "streams": [stream(17001, "ESPN"), stream(17002, "CNN")],
    })

    return cases


def variations(case):
    """The setting matrix every world is swept over."""
    base = dict(case.get("settings") or {})
    grid = {
        ("match_sensitivity",): [name for name, _ in SENSITIVITIES],
        ("overwrite_streams",): [True, False],
        ("keep_custom_streams",): [True, False],
        ("allow_same_name_streams",): [False, True],
        ("restrict_matching_to_country",): [base.get("restrict_matching_to_country", False),
                                            not base.get("restrict_matching_to_country", False)],
    }
    keys = list(grid)
    for combo in itertools.product(*(grid[key] for key in keys)):
        settings = dict(base)
        settings.update({key[0]: value for key, value in zip(keys, combo)})
        yield settings


def build_cases(limit=None):
    cases = []
    for world in worlds():
        for settings in variations(world):
            merged = {**world.get("settings", {}), **settings}
            # A case with a `dead` list is a run with `filter_dead_streams` on.
            merged["filter_dead_streams"] = bool(world.get("dead"))
            cases.append({
                "comment": "%s / %s" % (world["comment"],
                                        ", ".join(f"{key}={settings[key]}" for key in
                                                  ("match_sensitivity", "overwrite_streams",
                                                   "keep_custom_streams",
                                                   "allow_same_name_streams",
                                                   "restrict_matching_to_country"))),
                "settings": merged,
                "channels": world["channels"],
                "streams": world["streams"],
                "accounts": ACCOUNTS,
                "groups": GROUPS,
                "channels_data": world.get("channels_data", []),
                "throughput": {},
                "dead": world.get("dead", []),
                # The EPG fixture, in both shapes: `epg` for the plugin's stubbed ORM
                # lookups, and the flat lists `match_io` reads.
                "epg": world.get("epg") or {},
                "epg_entries": [
                    {"id": epg_id, "tvg_id": tvg_id}
                    for tvg_id, epg_id in ((world.get("epg") or {}).get("entries") or {}).items()
                ],
                "current_programs": {
                    str(epg_id): title
                    for epg_id, title in
                    ((world.get("epg") or {}).get("programmes") or {}).items()
                },
                "epg_today": (world.get("epg") or {}).get("today") or "8-5",
            })
    if limit:
        cases = cases[:limit]
    return cases


NAME_WORDS = ["ESPN", "CNN", "TNT", "Fox Sports", "Sky Sports", "Premier Sports",
              "Euro News", "UnXplained Zone", "MSNBC", "Antena 3", "DAZN F1",
              "NebulaOne Event", "Starz Encore", "HBO", "Cinemax", "MAX", "PPV EVENT",
              "Three", "3", "F1", "TF1", "Discovery+", "BBC One", "WGN", "KING"]
SUFFIXES = ["", " HD", " [4K]", " 1", " 2", " 12", " (W)", " West", " East", " F1",
            " backup", " [DEAD]", " (WNCF)", " Network", " (KING)", " 5 Seattle", " DT2"]
PREFIXES = ["", "US: ", "UK: ", "GO: ", "HD ", "24/7 ", "[CA] ", "FR - "]


def random_case(rng, index):
    """A randomly shaped world: names from the same vocabulary on both sides."""
    def make_name():
        return rng.choice(PREFIXES) + rng.choice(NAME_WORDS) + rng.choice(SUFFIXES)

    channels = []
    used = set()
    for channel_index in range(rng.randint(1, 4)):
        name = make_name()
        channel_id = channel_index + 1
        streams = [rng.randint(20000, 20060) for _ in range(rng.randint(0, 2))]
        channels.append(channel(channel_id, name, streams,
                                group=rng.choice([100, 101, 102, 103]),
                                number=rng.choice([None, 1, 2, 5])))
        used.add(name)
    stream_pool = []
    for stream_index in range(rng.randint(1, 8)):
        name = make_name()
        account = rng.choice([1, 1, 2, 9])
        custom = account == 9
        stream_pool.append(stream(20000 + stream_index, name, account=account,
                                  group=rng.choice([100, 101, 102, 103]), custom=custom))
    settings = {
        "match_sensitivity": rng.choice([name for name, _ in SENSITIVITIES]),
        "overwrite_streams": rng.random() < 0.7,
        "keep_custom_streams": rng.random() < 0.7,
        "allow_same_name_streams": rng.random() < 0.4,
        "restrict_matching_to_country": rng.random() < 0.4,
        "visible_channel_limit": rng.choice([1, 1, 2, 3]),
        "ignore_tags": rng.choice(["", "backup", "[DEAD], backup"]),
        "selected_m3us": rng.choice(["", "Main, *", "*, Backup"]),
        "prioritize_quality": rng.random() < 0.5,
        "ignore_quality": rng.random() < 0.8,
        "ignore_regional": rng.random() < 0.8,
        "ignore_geographic": rng.random() < 0.8,
        "ignore_misc": rng.random() < 0.8,
    }
    # EPG placeholder matching, sometimes on: a placeholder slot among the names, one guide
    # entry per stream that carries an identifier, and a current programme for the slot.
    # The adversarial branches below mirror the explicit worlds 28-31: an unanchored pattern
    # (fullmatch must refuse it), mixed-case skip titles, a today that sits on either side of
    # a dated waiting title, and a one-token channel that must never trip the watch rule.
    epg_block = {}
    if rng.random() < 0.5:
        settings["epg_placeholder_matching_enabled"] = True
        pattern_roll = rng.random()
        slot = f"PPV EVENT {rng.randint(1, 20):02d}"
        if pattern_roll >= 0.85:
            settings["epg_placeholder_name_patterns"] = "PPV"
        elif pattern_roll >= 0.7:
            settings["epg_placeholder_name_patterns"] = "^SLOT \\d+$"
            slot = f"SLOT {rng.randint(1, 20)}"
        # Otherwise the shipped defaults stand on both sides (the key stays absent).
        channels.append(channel(len(channels) + 1, slot, [], group=100, epg_data_id=7))
        if rng.random() < 0.3:
            settings["epg_skip_titles"] = rng.choice([
                "SIGNING OFF,no event today,No GAME today",
                "Signing Off,No Game Today",
            ])
        today = rng.choice(["8-4", "8-5", "8-5", "8-6"])
        event = rng.choice(["WWE SummerSlam", "Max Championship Special"])
        entries = {}
        for position, stream_row in enumerate(stream_pool):
            if rng.random() < 0.4:
                tvg_id = f"slot{position}.us"
                stream_row["tvg_id"] = tvg_id
                entries[tvg_id] = 7 + (position % 3)
        epg_block = {
            "entries": entries,
            "programmes": {
                7: rng.choice([event, "PPV EVENT 04", "nO eVeNt ToDaY",
                               f"Next Event: {event} at 11:59PM on Aug 5",
                               f"Next Event: {event} at 12:01AM on Aug 6",
                               "[LIVE] Main Event"]),
                8: f"{event} Special",
                9: "Signing Off",
            },
            "today": today,
        }
        if rng.random() < 0.5:
            settings["epg_watch_source_streams"] = rng.choice(["ESPN", "WWE SummerSlam"])
        if rng.random() < 0.3:
            # A one-token channel: too generic to gate a watched-stream force-include on.
            channels.append(channel(len(channels) + 1, "Max", [], group=100))
    dead = [stream["id"] for stream in stream_pool if rng.random() < 0.15]
    # Randomly deaden some streams' STATS too (empty, zero, or 0x0), so the stats branch
    # of the dead filter is exercised, not just the verdict list.
    for stream_row in stream_pool:
        roll = rng.random()
        if roll < 0.08:
            stream_row["stats"] = {}
        elif roll < 0.12:
            stream_row["stats"] = {"width": 0, "height": 0}
        elif roll < 0.15:
            stream_row["stats"] = {"resolution": "0x0"}
    # A case with a `dead` list is a run with `filter_dead_streams` on.
    settings["filter_dead_streams"] = bool(dead)
    throughput = {}
    for stream_row in stream_pool:
        if rng.random() < 0.3:
            throughput[str(stream_row["id"])] = _entry(
                rng.choice([None, 0.5, 3.0, 20.0]), age_seconds=rng.choice([60, 3600]),
                nominal=rng.choice([None, 2.5, 4.0]))
    return {
        "comment": f"random #{index}",
        "settings": settings,
        "channels": channels,
        "streams": stream_pool,
        "accounts": ACCOUNTS,
        "groups": GROUPS,
        "channels_data": [{"name": "CNN", "code": "US"}] if rng.random() < 0.3 else [],
        "throughput": throughput,
        "dead": dead,
        "epg": epg_block,
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
    parser.add_argument("--show", type=int, default=8, help="mismatches to print")
    parser.add_argument("--report", action="store_true",
                        help="report mode (step 9): print the summary and exit 0 "
                        "whatever is found, for the oracle diff report. The plugin is "
                        "the oracle; differences trace to decisions rows.")
    parser.add_argument("--cases", type=int, default=None, help="cap the swept corpus")
    parser.add_argument("--random", type=int, default=120, help="randomised cases to add")
    parser.add_argument("--seed", type=int, default=20261102, help="seed for the random cases")
    parser.add_argument("--binary",
                        default=str(REPO / "target" / "debug" / "examples" / "match_io"),
                        help="path to the match_io example binary")
    args = parser.parse_args()

    if not PLUGIN_INNER.is_dir():
        print(f"reference plugin not found at {PLUGIN_INNER}; run `git submodule update --init`",
              file=sys.stderr)
        return 2

    print(f"building {args.binary} ...", file=sys.stderr)
    subprocess.run(["cargo", "build", "-q", "-p", "crosstalk-streams", "--example", "match_io"],
                   cwd=REPO, check=True)

    plugin, matcher = load_plugin()
    cases = build_cases(args.cases) + random_case_all(args.random, args.seed)
    answers = run_rust(cases, args.binary)

    mismatches = 0
    compared = 0
    written = 0
    for case, answer in zip(cases, answers):
        expected_run = plugin_answer(plugin, matcher, case)
        expected = expected_run["assignments"]
        got = {int(row["channel_id"]): row for row in answer["assignments"]}
        if set(expected) != set(got):
            mismatches += 1
            if mismatches <= args.show:
                print(f"[{case['comment']}] channel set differs: "
                      f"python={sorted(expected)} rust={sorted(got)}")
            continue
        for channel_id, want in expected.items():
            compared += 1
            written += int(want["will_write"])
            mine = got[channel_id]
            if want["after"] != mine["after"] or want["before"] != mine["before"]:
                mismatches += 1
                if mismatches <= args.show:
                    print(f"[{case['comment']}] channel {channel_id} ({mine['reason']})")
                    print(f"  settings={case['settings']}")
                    print(f"  streams={[(s['id'], s['name'], s['account'], s.get('group')) for s in case['streams']]}")
                    print(f"  before python={want['before']} rust={mine['before']}")
                    print(f"  after  python={want['after']} rust={mine['after']}")
                    print(f"  matched python={want['matched']} rust={mine['matched']} "
                          f"keep_existing python={want['keep_existing']} rust={mine['keep_existing']}")
                continue
            if want["will_write"] != mine["will_write"]:
                mismatches += 1
                if mismatches <= args.show:
                    print(f"[{case['comment']}] channel {channel_id}: will_write "
                          f"python={want['will_write']} rust={mine['will_write']}")
        # The country counters are the plugin's own aggregation, compared per run.
        if expected_run["country"] is not None:
            if expected_run["country"] != answer["country"]:
                mismatches += 1
                if mismatches <= args.show:
                    print(f"[{case['comment']}] country counters differ: "
                          f"python={expected_run['country']} rust={answer['country']}")

    print(f"[match] {mismatches} mismatch(es) over {len(cases)} case(s), "
          f"{compared} channel(s) compared, {written} of them written")
    if args.report:
        print("[report] exit 0: differences are oracle diffs, not failures")
        return 0
    return 1 if mismatches else 0


def random_case_all(count, seed):
    rng = random.Random(seed)
    return [random_case(rng, index) for index in range(count)]


if __name__ == "__main__":
    sys.exit(main())
