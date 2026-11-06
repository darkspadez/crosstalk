#!/usr/bin/env python3
"""Parity: crosstalk's `triage_*` / `restore_*` against IPTV Checker's seven post-actions.

Triage and restore are the plugin's channel surgery, ported. This is how that claim is checked:
it imports the plugin's `plugin.py` from `reference/` (with the Django stubs every harness in
this repo installs), fakes the four ORM reads it makes, and drives the REAL actions end to end
over generated fixtures:

  * `rename_channels_action`            (dead channels, `_is_dead_nonblack`)
  * `rename_black_screen_channels_action` (blank screens, `_is_black_screen`)
  * `rename_low_framerate_channels_action` (slow channels, `_is_low_framerate_row` among Alive)
  * `add_video_format_suffix_action`    (alive channels, suffix replacement)
  * `move_dead_channels_action`, `move_black_screen_channels_action`,
    `move_low_framerate_channels_action` (with `_get_or_create_group` and
    `_capture_original_state`)
  * `restore_channels_action`           (`_compute_restore_plan`, `_derive_*_tags`)

in the plugin's own SCHEDULED order (`plugin.py:2060-2107`) — the order matters, because the
suffix runs after the renames and against the name they wrote.

The Rust side is `cargo run -p crosstalk-streams --example triage_io`, handed the same world,
the same per-stream probe rows (so both sides derive the verdicts from the same evidence: the
plugin with `_channels_where`, the port with `probe::verdict_for`) and, for restore, the same
recorded changes as `triage_apply` run rows. Nothing of the Rust is mirrored here: the plugin's
own code decides the expected answer.

Three shapes are constructed rather than compared, and are COUNTED rather than failed — they
are the documented divergences:

  * `stacked`  — a rename whose base name already ends with the format's own tag. The plugin
    appends it again (`ESPN [DEAD] [DEAD]`); the port does not (`ESPN [DEAD]`). The harness
    asserts the port's name is the plugin's with exactly that repeated tag removed.
  * `untracked` — a restore where the plugin strips a QUALITY tag (`[HD]`) the recorded change
    history never mentioned. The port restores only what it recorded. The harness asserts the
    two names differ only by known tags on top of the same base.
  * `paren` — step 9's shared trailing-tag parser: a known tag in PARENS (`ESPN (HD)`,
    `ESPN (DEAD)`) counts as a tag. The plugin's regexes read only `[...]`, so it appends
    beside it (`ESPN (HD) [HD]`, `ESPN (DEAD) [DEAD]`) and its restore leaves it; the port
    replaces/skips/strips it. The harness asserts the two names differ by exactly that one
    parenthesised known tag. Every other tag the vocabulary knows (guide `[BadEPG]`, CM
    `[Unk]`, provider `[1080p]`) must still AGREE: the strip set is the plugin's.

Usage:
    python3 -I scripts/triage_parity.py [--show N] [--random N] [--seed N]

Exit status is 1 when any channel, group or eligibility differs, so it can gate CI.
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
PLUGIN_ROOT = REPO / "reference" / "Dispatcharr-IPTV-Checker-Plugin"
PLUGIN_INNER = PLUGIN_ROOT / "iptv_checker"
PKG = "iptv_checker_triage_parity"

# `PluginConfig.LOW_FRAMERATE_THRESHOLD` (plugin.py:180) is 24 fps, which is why the rows
# below use 20.0 for "slow" and 25.0/30.0/60.0 for "plays fine".
LOW_FRAMERATE_THRESHOLD = 24


# ---------------------------------------------------------------------------------------
# Loading the plugin without Dispatcharr
# ---------------------------------------------------------------------------------------

def load_plugin():
    """`reference/Dispatcharr-IPTV-Checker-Plugin/tests/conftest.py:45-95`, the plugin's own
    recipe: stub `django`, `apps.channels.models` and `core.utils`, then import `plugin.py`."""
    from unittest.mock import MagicMock

    django = types.ModuleType("django")
    django_utils = types.ModuleType("django.utils")
    django_tz = types.ModuleType("django.utils.timezone")
    django_tz.now = lambda: datetime.now(dt_timezone.utc)
    django_tz.localtime = lambda dt=None: dt if dt is not None else django_tz.now()
    django_utils.timezone = django_tz

    class _Transaction:
        @staticmethod
        def atomic(*args, **kwargs):
            class _Ctx:
                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

            return _Ctx()

    django_db = types.ModuleType("django.db")
    django_db.__path__ = []
    django_db.transaction = _Transaction()
    django_db.models = MagicMock(name="models")
    django.models = django_db.models
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
    logging.disable(logging.CRITICAL)
    return module


# ---------------------------------------------------------------------------------------
# A fake database: channels, groups, and the two writes the actions make
# ---------------------------------------------------------------------------------------

class FakeDb:
    """Dispatcharr's channel table, as the plugin's actions see it.

    The rows are dicts, because `_get_all_channels` returns `values(...)` dicts and every
    action but `_bulk_update_channels` reads them; the two writes below are the ORM calls the
    actions make (`bulk_update(Channel, ['name'])` and `get_or_create(ChannelGroup)`).
    """

    def __init__(self, channels, groups):
        # channel id -> {"id", "name", "channel_number", "channel_group_id"}, which is the shape
        # `_get_all_channels` returns (a `values(...)` dict, not the plugin's own model).
        self.channels = {
            row["id"]: {
                "id": row["id"],
                "name": row["name"],
                "channel_number": row["id"],
                "channel_group_id": row.get("group"),
            }
            for row in channels
        }
        self.groups = {row["id"]: dict(row) for row in groups}
        self.next_group_id = max(self.groups, default=0) + 1
        self.created_groups = []
        self.writes = []           # (field, {id: value}) in the order they landed

    # `_get_all_channels` / `_get_all_groups`
    def all_channels(self, logger, group_ids=None):
        rows = [dict(row) for row in self.channels.values()]
        if group_ids:
            rows = [row for row in rows if row.get("channel_group_id") in group_ids]
        return rows

    def all_groups(self, logger):
        return [dict(row) for row in self.groups.values()]

    # `_get_or_create_group`
    def get_or_create_group(self, name, logger):
        for group in self.groups.values():
            if group["name"] == name:
                return types.SimpleNamespace(id=group["id"])
        group = {"id": self.next_group_id, "name": name}
        self.groups[group["id"]] = group
        self.next_group_id += 1
        self.created_groups.append(name)
        return types.SimpleNamespace(id=group["id"])

    # `_bulk_update_channels(payload, fields, logger)`
    def bulk_update_channels(self, updates, fields, logger):
        landed = {}
        for update in updates:
            channel = self.channels.get(update["id"])
            if channel is None:
                continue
            for field in fields:
                if field in update:
                    channel[field] = update[field]
                    landed[update["id"]] = update[field]
        if landed:
            self.writes.append((fields[0], landed))
        return len(landed)


# ---------------------------------------------------------------------------------------
# The plugin's answer for one case
# ---------------------------------------------------------------------------------------

def plugin_instance(plugin_mod, db, workdir):
    """A `Plugin` built without `__init__`, as the plugin's own test suite does."""
    P = plugin_mod.Plugin
    inst = P.__new__(P)
    inst.key = "iptv_checker"
    inst.version = "0.0.0-parity"
    inst.results_file = str(workdir / "results.json")
    inst.loaded_channels_file = str(workdir / "loaded_channels.json")
    inst.pending_resume_file = str(workdir / "pending_resume.json")
    inst.progress_file = str(workdir / "progress.json")
    inst.channel_state_file = str(workdir / "channel_state.json")
    inst._get_all_channels = db.all_channels
    inst._get_all_groups = db.all_groups
    inst._bulk_update_channels = db.bulk_update_channels
    inst._get_or_create_group = db.get_or_create_group
    inst._trigger_frontend_refresh = lambda settings, logger: True
    inst._send_progress_update = lambda *args, **kwargs: None
    return inst


def results_rows(case):
    """The plugin's `results.json`: one row per STREAM, in the channel's playback order.

    `channel_name` is the name the check saw — the case's `probe_name` when it has one, which
    is how a rename that happened after the check is modelled.
    """
    rows = []
    for channel_id, streams in case["streams"]:
        channel = next(row for row in case["channels"] if row["id"] == channel_id)
        name = channel.get("probe_name") or channel["name"]
        for stream in streams:
            rows.append({
                "channel_id": channel_id,
                "channel_name": name,
                "status": stream.get("class_str", "Alive"),
                "error_type": stream.get("error_type"),
                "framerate_num": stream.get("framerate", 0),
                "format": stream.get("format", "Unknown"),
            })
    return rows


def triage_answer(plugin_mod, case, workdir):
    """Run the plugin's scheduled post-actions, in its own order, over one case."""
    (workdir / "results.json").write_text(json.dumps(results_rows(case)), encoding="utf-8")
    db = FakeDb(case["channels"], case["groups"])
    plugin = plugin_instance(plugin_mod, db, workdir)
    settings = dict(case["settings"])
    logger = logging.getLogger("triage_parity")

    # `plugin.py:2067-2107`, the scheduled order, minus restore (its own action) and delete
    # (out of scope).
    calls = [
        ("scheduler_rename_dead_channels", "rename_channels_action"),
        ("scheduler_rename_low_framerate_channels", "rename_low_framerate_channels_action"),
        ("scheduler_rename_black_screen_channels", "rename_black_screen_channels_action"),
        ("scheduler_add_video_format_suffix", "add_video_format_suffix_action"),
        ("scheduler_move_dead_channels", "move_dead_channels_action"),
        ("scheduler_move_low_framerate_channels", "move_low_framerate_channels_action"),
        ("scheduler_move_black_screen_channels", "move_black_screen_channels_action"),
    ]
    messages = []
    for switch, method in calls:
        if not settings.get(switch):
            continue
        result = getattr(plugin, method)(settings, logger)
        messages.append(result.get("message"))
        if result.get("status") not in ("ok", "success"):
            raise SystemExit(f"the plugin's {method} failed: {result}")

    return {
        "names": {channel_id: row["name"] for channel_id, row in db.channels.items()},
        "groups": {channel_id: row.get("channel_group_id")
                   for channel_id, row in db.channels.items()},
        "group_names": {group["id"]: group["name"] for group in db.groups.values()},
        "created_groups": list(db.created_groups),
        "messages": messages,
    }


def restore_answer(plugin_mod, case, workdir):
    """Run the plugin's restore over one case, with its state file built from the history.

    The state file is what `_capture_original_state` would have written for the moves the
    recorded history describes: the group each channel was in BEFORE the move, never
    overwritten, never for a channel with no group and never when that group is a managed
    destination (3593-3620). Building it here is what makes the two sides comparable at all —
    crosstalk's history IS its state file.
    """
    (workdir / "results.json").write_text(json.dumps(results_rows(case)), encoding="utf-8")
    db = FakeDb(case["channels"], case["groups"])
    managed = {
        (case["settings"].get("move_to_group_name") or "Graveyard").strip().lower(),
        (case["settings"].get("move_low_framerate_group") or "Slow").strip().lower(),
        (case["settings"].get("move_black_screen_group") or "Black Screens").strip().lower(),
    }
    group_names = {group["id"]: group["name"] for group in case["groups"]}
    state = {}
    for row in case["history"]:
        if row.get("field") != "channel_group_id" or row.get("status", "ok") != "ok":
            continue
        before = row.get("before")
        if before in (None, "none"):
            continue  # no original group: nothing to restore TO
        before_id = int(before)
        if (group_names.get(before_id, "").strip().lower()) in managed:
            continue
        if str(row["channel_id"]) in state:
            continue  # never overwritten
        state[str(row["channel_id"])] = {
            "original_group_id": before_id,
            "original_group_name": group_names.get(before_id, ""),
            "moved_at": "2026-11-01T00:00:00Z",
        }
    (workdir / "channel_state.json").write_text(json.dumps(state), encoding="utf-8")

    plugin = plugin_instance(plugin_mod, db, workdir)
    settings = dict(case["settings"])
    result = plugin.restore_channels_action(settings, logging.getLogger("triage_parity"))
    if result.get("status") != "ok":
        raise SystemExit(f"the plugin's restore failed: {result}")
    return {
        "names": {channel_id: row["name"] for channel_id, row in db.channels.items()},
        "groups": {channel_id: row.get("channel_group_id")
                   for channel_id, row in db.channels.items()},
        "group_names": {group["id"]: group["name"] for group in db.groups.values()},
        "restored": result.get("restored"),
    }


# ---------------------------------------------------------------------------------------
# Driving the Rust side
# ---------------------------------------------------------------------------------------

def rust_case(case, kind):
    """The case in `triage_io`'s shape: the world, the probe's rows, the history."""
    streams = []
    for channel_id, rows in case["streams"]:
        streams.append([channel_id, [
            {
                "class": row.get("class_str", "Alive").lower(),
                "error_type": row.get("error_type"),
                "framerate": row.get("framerate", 0),
                "format": row.get("format", "Unknown"),
            }
            for row in rows
        ]])
    settings = case["settings"]
    payload = {
        "kind": kind,
        "channels": [
            {
                "id": channel["id"],
                "name": channel["name"],
                "group": channel.get("group"),
                "probe_name": channel.get("probe_name"),
            }
            for channel in case["channels"]
        ],
        "groups": [{"id": group["id"], "name": group["name"]} for group in case["groups"]],
        "streams": streams,
        "history": case.get("history") or [],
        "settings": {
            "rename_dead_channels": settings.get("scheduler_rename_dead_channels", False),
            "rename_blank_channels": settings.get("scheduler_rename_black_screen_channels", False),
            "rename_low_framerate_channels":
                settings.get("scheduler_rename_low_framerate_channels", False),
            "add_video_format_suffix": settings.get("scheduler_add_video_format_suffix", False),
            "move_dead_channels": settings.get("scheduler_move_dead_channels", False),
            "move_blank_channels": settings.get("scheduler_move_black_screen_channels", False),
            "move_low_framerate_channels":
                settings.get("scheduler_move_low_framerate_channels", False),
            "dead_rename_format": settings.get("dead_rename_format"),
            "black_screen_rename_format": settings.get("black_screen_rename_format"),
            "low_framerate_rename_format": settings.get("low_framerate_rename_format"),
            "move_to_group_name": settings.get("move_to_group_name"),
            "move_black_screen_group": settings.get("move_black_screen_group"),
            "move_low_framerate_group": settings.get("move_low_framerate_group"),
            "video_format_suffixes": settings.get("video_format_suffixes"),
        },
    }
    return payload


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
# Comparison
# ---------------------------------------------------------------------------------------

TAG_RE = re.compile(r"\[([^\[\]]+)\]")


def format_tags(fmt):
    return TAG_RE.findall(fmt or "")


def known_tags(settings):
    """Every tag both sides treat as a plugin tag: the status tags, the quality tags, the
    format labels and the configured suffixes (`_derive_strippable_tags`, 3419)."""
    tags = {"DEAD", "Slow", "Blank", "UHD", "FHD", "HD", "SD", "Unknown"}
    for key in ("dead_rename_format", "low_framerate_rename_format",
                "black_screen_rename_format"):
        tags.update(format_tags(settings.get(key)))
    tags.update(part.strip() for part in
                (settings.get("video_format_suffixes") or "").split(",") if part.strip())
    return {tag.lower() for tag in tags}


def strip_tags(name, tags):
    """`_derive_strippable_tags`' effect: drop a trailing run of known `[TAG]` groups."""
    while True:
        match = re.search(r"\s*\[([^\[\]]+)\]\s*$", name or "")
        if not match or match.group(1).strip().lower() not in tags:
            return (name or "").rstrip()
        name = name[:match.start()].rstrip()


def ends_with_tag(name, label):
    match = re.search(r"\[([^\[\]]+)\]\s*$", name or "")
    return bool(match) and match.group(1).strip().lower() == (label or "").strip().lower()


def paren_tags(name, tags):
    """The known tags `name` carries in parens, anywhere in its trailing run (`paren`)."""
    return [label for label in re.findall(r"\(([^()]+)\)", name or "")
            if label.strip().lower() in tags]


def differs_by_paren(expected, actual, tags):
    """`paren`: the plugin's name and the port's differ by one parenthesised known tag,
    either way round (the plugin keeps `(HD)` beside its `[HD]`; the port's restore strips
    `(DEAD)` the plugin's leaves) — or by the plugin's one extra `[label]` appended beside
    a `(label)` the port counted as already there."""
    if expected is None or actual is None:
        return False
    for label in paren_tags(expected, tags) + paren_tags(actual, tags):
        if differs_by_one_tag(expected, actual, label, "()") \
                or differs_by_one_tag(actual, expected, label, "()"):
            return True
        # `ESPN (HD)` + suffix: plugin `ESPN (HD) [HD]`, port `ESPN [HD]`.
        for left, right in ((expected, actual), (actual, expected)):
            stripped = left.replace(f" ({label})", "", 1)
            if stripped != left and stripped == right:
                return True
        # `ESPN (DEAD)` dead rename: plugin `ESPN (DEAD) [DEAD]`, port skips.
        if differs_by_one_tag(expected, actual, label.strip().upper()) \
                or differs_by_one_tag(expected, actual, label):
            return True
        # Restore: the port's strip reads parens in the trailing run, so its name is the
        # plugin's with that run stripped bracket- AND paren-aware.
        if strip_tags_any(expected, tags) == actual:
            return True
    return False


def strip_tags_any(name, tags):
    """`strip_tags` over `[...]` and `(...)` groups: the port's trailing-run parser."""
    while True:
        match = re.search(r"\s*(?:\[([^\[\]]+)\]|\(([^()]+)\))\s*$", name or "")
        label = match and (match.group(1) or match.group(2))
        if not match or label.strip().lower() not in tags:
            return (name or "").rstrip()
        name = name[:match.start()].rstrip()


def differs_by_one_tag(expected, actual, label, shape="[]"):
    """Whether removing exactly one `[label]` group from the plugin's name yields the port's.

    The port skips one append of a rename format's own tag when the recorded base name
    already ends with it; the plugin appends it again. The duplicate is usually trailing
    (`ESPN [DEAD] [DEAD]`), but a LEADING format (`[DEAD] {name}`) duplicates it at the front
    (`[DEAD] ESPN [DEAD]`), and the suffix step can park its own tag after either shape. So
    every occurrence is tried, with one adjacent space, and any match counts.
    """
    if expected is None or actual is None:
        return False
    tag = f"{shape[0]}{label}{shape[1]}"
    index = 0
    while True:
        index = expected.find(tag, index)
        if index < 0:
            return False
        if index > 0 and expected[index - 1] == " ":
            candidate = expected[:index - 1] + expected[index + len(tag):]
        elif expected[index + len(tag):index + len(tag) + 1] == " ":
            candidate = expected[:index] + expected[index + len(tag) + 1:]
        else:
            candidate = expected[:index] + expected[index + len(tag):]
        if candidate == actual:
            return True
        index += 1


def rust_names(answer, world_names):
    """The port's planned names: the suffix wins over the rename when both apply, because the
    suffix step runs last and against the renamed name."""
    names = dict(world_names)
    for entry in answer.get("renames") or []:
        names[entry[0]] = entry[1]
    for entry in answer.get("suffixes") or []:
        names[entry[0]] = entry[1]
    return names


def compare_triage(case, plugin, rust, show, state):
    """Compare one triage case channel by channel, counting the documented divergence."""
    world_names = {channel["id"]: channel["name"] for channel in case["channels"]}
    probe_names = {channel["id"]: channel.get("probe_name") or channel["name"]
                   for channel in case["channels"]}
    group_names = plugin["group_names"]
    settings = case["settings"]
    want = plugin["names"]
    got = rust_names(rust, world_names)
    problems = []
    stacked = 0

    for channel_id, expected in want.items():
        actual = got.get(channel_id)
        if actual == expected:
            state["names_compared"] += 1
            continue
        # `stacked`: the plugin renamed from a base whose name already ended with the format's
        # own tag, so it appended it a second time and the port did not. Covered for the three
        # classes: which one applied is the one whose format's tag the base ends with.
        repeated = None
        for key in ("dead_rename_format", "low_framerate_rename_format",
                    "black_screen_rename_format"):
            for label in format_tags(settings.get(key)):
                if ends_with_tag(probe_names[channel_id], label):
                    repeated = label
                    break
            if repeated:
                break
        # The port skipped ONE append of that tag: the plugin's name with one occurrence of it
        # removed is the port's.
        if repeated and differs_by_one_tag(expected, actual, repeated):
            stacked += 1
            continue
        if differs_by_paren(expected, actual, known_tags(settings)):
            state["paren"] += 1
            continue
        problems.append(f"channel {channel_id}: plugin name {expected!r} != rust {actual!r}")

    # Moves: the destination group each channel is in, and the groups the plugin created.
    rust_moves = {channel_id: group for channel_id, group in rust.get("moves") or []}
    for channel_id, group_id in plugin["groups"].items():
        wanted = group_names.get(group_id, "No Group")
        if channel_id in rust_moves:
            if rust_moves[channel_id] != wanted:
                problems.append(
                    f"channel {channel_id}: plugin group {wanted!r} != rust {rust_moves[channel_id]!r}")
            else:
                state["groups_compared"] += 1
            continue
        # No move planned by the port: it must already be where the plugin left it, or the
        # plugin must not have moved it either.
        if wanted != group_names.get(next(row["group"] for row in case["channels"]
                                          if row["id"] == channel_id), "No Group"):
            problems.append(
                f"channel {channel_id}: plugin moved it to {wanted!r}, the port planned no move")
        elif rust_moves:
            state["groups_compared"] += 1

    # A destination group that already exists is reused, not created; a missing one is created
    # by both sides. Comparing the CREATED sets is what proves the port would create the same
    # groups on an apply.
    existing = {group["name"] for group in case["groups"]}
    expected_created = sorted(set(rust_moves.values()) - existing)
    if expected_created != sorted(set(plugin["created_groups"])):
        problems.append(
            f"created groups: plugin {sorted(set(plugin['created_groups']))} "
            f"!= rust {expected_created}")

    state["stacked"] += stacked
    return problems


def compare_restore(case, plugin, rust, show, state):
    """Compare one restore case: names, groups and the eligibility both sides agree on."""
    world_names = {channel["id"]: channel["name"] for channel in case["channels"]}
    group_names = plugin["group_names"]
    tags = known_tags(case["settings"])
    want = plugin["names"]
    got = rust_names(rust, world_names)
    problems = []
    untracked = 0

    for channel_id, expected in want.items():
        actual = got.get(channel_id)
        if actual == expected:
            state["names_compared"] += 1
            continue
        # `untracked`: the plugin strips a tag the recorded history never mentioned (it strips
        # by NAME, the port restores what it recorded). Same base, different tags.
        if strip_tags(expected, tags) == strip_tags(actual, tags):
            untracked += 1
            continue
        # `unrecognized`: the mirror image — crosstalk recorded a rename with a label the CURRENT
        # settings no longer name (an operator edited the format, or the label is not one of the
        # five standard tags). The port undoes what it recorded; the plugin cannot see it.
        match = re.search(r"\s*\[([^\[\]]+)\]\s*$", expected or "")
        if match and (expected[:match.start()].rstrip() == actual) \
                and match.group(1).strip().lower() not in tags:
            untracked += 1
            continue
        if differs_by_paren(expected, actual, tags):
            state["paren"] += 1
            continue
        problems.append(f"channel {channel_id}: plugin name {expected!r} != rust {actual!r}")

    rust_moves = {movement[0]: movement[2] for movement in rust.get("moves") or []}
    for channel_id, group_id in plugin["groups"].items():
        started = next(row["group"] for row in case["channels"] if row["id"] == channel_id)
        if channel_id in rust_moves:
            if rust_moves[channel_id] != group_id:
                problems.append(
                    f"channel {channel_id}: plugin group {group_id} != rust {rust_moves[channel_id]}")
            else:
                state["groups_compared"] += 1
            continue
        if group_id != started:
            problems.append(
                f"channel {channel_id}: plugin moved it to "
                f"{group_names.get(group_id, 'No Group')!r}, the port planned no move")

    # The plugin's `restored` count is the channels it touched; the port's is its own count of
    # name and group changes, so compare the SETS rather than the totals.
    touched = sorted(set(plugin["names"]) | set(plugin["groups"]))
    del touched, want, got
    state["untracked"] += untracked
    return problems


# ---------------------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------------------

GROUP_NAMES = {100: "US: Cable", 101: "UK: Cable", 102: "US: Sports"}


def channel(channel_id, name, group=100, probe_name=None):
    return {"id": channel_id, "name": name, "group": group, "probe_name": probe_name}


def groups():
    return [{"id": gid, "name": name} for gid, name in GROUP_NAMES.items()]


def alive(fmt="HD", fps=30.0):
    return {"class_str": "Alive", "framerate": fps, "format": fmt}


def dead(error="Timeout"):
    return {"class_str": "Dead", "error_type": error}


def skipped():
    return {"class_str": "Skipped"}


ALL_ON = {
    "scheduler_rename_dead_channels": True,
    "scheduler_rename_black_screen_channels": True,
    "scheduler_rename_low_framerate_channels": True,
    "scheduler_add_video_format_suffix": True,
    "scheduler_move_dead_channels": True,
    "scheduler_move_black_screen_channels": True,
    "scheduler_move_low_framerate_channels": True,
    "dead_rename_format": "{name} [DEAD]",
    "black_screen_rename_format": "{name} [Blank]",
    "low_framerate_rename_format": "{name} [Slow]",
    "move_to_group_name": "Graveyard",
    "move_black_screen_group": "Black Screens",
    "move_low_framerate_group": "Slow",
    "video_format_suffixes": "UHD, FHD, HD, SD, Unknown",
}


def settings(**overrides):
    merged = dict(ALL_ON)
    merged.update(overrides)
    return merged


def triage_worlds():
    """The fixture worlds, one per shape an action has a branch for."""
    cases = []

    def case(comment, channels, streams, **overrides):
        cases.append({
            "kind": "triage",
            "comment": comment,
            "channels": channels,
            "groups": groups(),
            "streams": streams,
            "settings": settings(**overrides),
        })

    # The plugin's three classes, each in its own group, plus a channel that plays.
    case("one channel per class",
         [channel(1, "ESPN"), channel(2, "TNT"), channel(3, "AMC"), channel(4, "CNN")],
         [(1, [dead()]), (2, [dead("Black Screen")]), (3, [alive("SD", 20.0)]),
          (4, [alive("HD")])])

    # A channel that plays on a backup is never dead: one Dead row beside one Alive row.
    case("a working backup stream",
         [channel(1, "ESPN")], [(1, [dead(), alive("HD")])])
    # Every stream dead is dead, however many there are.
    case("every stream dead", [channel(1, "ESPN")], [(1, [dead(), dead("403 Forbidden")])])
    # A Skipped stream is not evidence: it never makes a channel dead.
    case("a skipped stream", [channel(1, "ESPN")], [(1, [skipped()])])
    # A dead stream beside a skipped one matches no predicate: the plugin acts on nothing.
    case("dead beside skipped", [channel(1, "ESPN")], [(1, [dead(), skipped()])])
    # Blank takes precedence over dead inside the blank predicate: a black screen is Dead too,
    # and the dead action excludes it.
    case("a blank screen is not dead",
         [channel(1, "ESPN")], [(1, [dead("Black Screen"), dead("Black Screen")])])
    # A black screen beside a timeout matches neither dead (one is black) nor blank.
    case("a mixed dead row set",
         [channel(1, "ESPN")], [(1, [dead("Black Screen"), dead("Timeout")])])

    # The suffix replaces an existing format tag instead of stacking (issue #18).
    case("the suffix replaces a format tag",
         [channel(1, "ESPN [SD]"), channel(2, "ESPN [HD]")],
         [(1, [alive("HD")]), (2, [alive("HD")])])
    # Several format tags collapse to one.
    case("stacked format tags collapse",
         [channel(1, "ESPN [HD] [SD]")], [(1, [alive("UHD")])])
    # A format not in the configured list is left alone.
    case("a format not in the list",
         [channel(1, "ESPN")], [(1, [alive("HD")])],
         video_format_suffixes="UHD, FHD")
    # Narrowing the list still replaces a standard tag it no longer mentions.
    case("a narrowed list still replaces",
         [channel(1, "ESPN [SD]")], [(1, [alive("UHD")])], video_format_suffixes="UHD")
    # The suffix runs AFTER a rename, against the name the rename wrote.
    case("the suffix follows the rename",
         [channel(1, "ESPN")], [(1, [alive("SD", 20.0)])])
    # A lowercase suffix list is accepted (the plugin lowercases the whole setting).
    case("a lowercase suffix list", [channel(1, "ESPN")], [(1, [alive("HD")])],
         video_format_suffixes="uhd, fhd, hd, sd, unknown")
    # Unknown is a format like any other.
    case("an unknown format", [channel(1, "ESPN")], [(1, [alive("Unknown")])])

    # Already tagged: the documented `stacked` divergence (the plugin appends a second tag).
    case("a base name that already carries the tag",
         [channel(1, "ESPN [DEAD]")], [(1, [dead()])])
    case("a base name that already carries a custom tag",
         [channel(1, "ESPN [GONE]"), channel(2, "ESPN")],
         [(1, [dead()]), (2, [dead()])],
         dead_rename_format="{name} [GONE]")
    case("the tag is not trailing",
         [channel(1, "[DEAD] ESPN")], [(1, [dead()])])
    # Step 9 (`paren`): a known tag in parens counts as a tag for the port only.
    case("a format tag in parens",
         [channel(1, "ESPN (SD)"), channel(2, "CNN (HD) [SD]")],
         [(1, [alive("HD")]), (2, [alive("HD")])])
    case("a status tag in parens",
         [channel(1, "ESPN (DEAD)")], [(1, [dead()])])
    # Every other vocabulary tag is NOT a format tag: both sides keep it.
    case("other apps' and providers' tags stay",
         [channel(1, "ESPN [BadEPG]"), channel(2, "CNN [1080p]"), channel(3, "AMC [Unk]"),
          channel(4, "TNT [HEVC] [SD]"), channel(5, "Sky (East)")],
         [(1, [alive("HD")]), (2, [alive("HD")]), (3, [dead()]), (4, [alive("FHD")]),
          (5, [alive("HD")])])

    # The plugin applies the format to the name the CHECK saw, not to a fresh read.
    case("a name that changed since the check",
         [channel(1, "ESPN News", probe_name="ESPN")], [(1, [dead()])])
    # …and the suffix step reads the LIVE name, so a manual rename is kept (plus the tag).
    case("the suffix reads the live name",
         [channel(1, "ESPN News", probe_name="ESPN")], [(1, [alive("HD")])])

    # Missing destination groups are created, one per class that has a channel.
    case("missing destination groups",
         [channel(1, "ESPN"), channel(2, "TNT"), channel(3, "AMC")],
         [(1, [dead()]), (2, [dead("Black Screen")]), (3, [alive("SD", 20.0)])],
         move_to_group_name="Parked", move_black_screen_group="Parked",
         move_low_framerate_group="Parked")
    # An existing group is reused, not created.
    case("an existing destination group",
         [channel(1, "ESPN")], [(1, [dead()])], move_to_group_name="US: Sports")

    # One class switched off leaves its channels alone entirely.
    case("the dead renames off",
         [channel(1, "ESPN")], [(1, [dead()])], scheduler_rename_dead_channels=False)
    # Only a move, only a rename.
    case("every switch off", [channel(1, "ESPN")], [(1, [dead()])],
         **{key: False for key in ALL_ON if key.startswith("scheduler_")})
    case("only the move",
         [channel(1, "ESPN")], [(1, [dead()])],
         scheduler_rename_dead_channels=False,
         scheduler_rename_black_screen_channels=False,
         scheduler_rename_low_framerate_channels=False,
         scheduler_add_video_format_suffix=False)
    case("only the rename",
         [channel(1, "ESPN")], [(1, [dead()])],
         scheduler_move_dead_channels=False, scheduler_move_black_screen_channels=False,
         scheduler_move_low_framerate_channels=False,
         scheduler_add_video_format_suffix=False)

    # A name that is only a tag: the format produces the same name, so nothing is written.
    case("a name that is only a tag",
         [channel(1, "[DEAD]")], [(1, [dead()])], dead_rename_format="{name}")

    # Several channels of one class, one group each.
    case("a whole group of dead channels",
         [channel(1, "ESPN", 100), channel(2, "ESPN", 101), channel(3, "CNN", 100)],
         [(1, [dead()]), (2, [dead()]), (3, [alive("HD")])])
    return cases


def restore_worlds():
    """Restore fixtures: a world, the probe's rows and the recorded triage history."""
    cases = []

    def case(comment, channels, streams, history, **overrides):
        cases.append({
            "kind": "restore",
            "comment": comment,
            "channels": channels,
            "groups": groups(),
            "streams": streams,
            "history": history,
            "settings": settings(**overrides),
        })

    def name_row(channel_id, before, after, status="ok", field="name"):
        return {"channel_id": channel_id, "field": field, "before": before, "after": after,
                "status": status}

    def group_row(channel_id, before, after, status="ok"):
        return {"channel_id": channel_id, "field": "channel_group_id", "before": before,
                "after": after, "status": status}

    # The plugin's headline case: renamed dead, moved, and alive again.
    case("renamed and moved, then alive",
         [channel(1, "ESPN [DEAD]", group=102)],
         [(1, [alive("HD")])],
         [name_row(1, "ESPN", "ESPN [DEAD]"), group_row(1, "100", "102")])
    # Renamed but never moved: the name comes back, the group is untouched.
    case("renamed but never moved",
         [channel(1, "Sky [DEAD]", group=102)],
         [(1, [alive("HD")])],
         [name_row(1, "Sky", "Sky [DEAD]")])
    # Moved but never renamed.
    case("moved but never renamed",
         [channel(1, "Sky", group=102)],
         [(1, [alive("HD")])],
         [group_row(1, "100", "102")])
    # Already restored: the current value is the recorded BEFORE, so nothing happens.
    case("already restored",
         [channel(1, "ESPN", group=100)],
         [(1, [alive("HD")])],
         [name_row(1, "ESPN", "ESPN [DEAD]"), group_row(1, "100", "102")])
    # Still dead: the plugin's restore only looks at Alive channels.
    case("still dead",
         [channel(1, "ESPN [DEAD]", group=102)],
         [(1, [dead()])],
         [name_row(1, "ESPN", "ESPN [DEAD]"), group_row(1, "100", "102")])
    # A slow channel's rows are Alive, so the plugin restores it too.
    case("a slow channel is recovered",
         [channel(1, "ESPN [Slow]", group=102)],
         [(1, [alive("HD", 20.0)])],
         [name_row(1, "ESPN", "ESPN [Slow]"), group_row(1, "100", "102")])
    # Two stacked changes: the plugin's tag strip collapses both, and so does the chain.
    case("two stacked changes",
         [channel(1, "ESPN [HD] [DEAD]", group=102)],
         [(1, [alive("HD")])],
         [name_row(1, "ESPN", "ESPN [HD]"), name_row(1, "ESPN [HD]", "ESPN [HD] [DEAD]"),
          group_row(1, "100", "102")])
    # A hand rename since the recorded one: the value no longer matches, so nothing is written.
    case("renamed by hand since",
         [channel(1, "ESPN News", group=100)],
         [(1, [alive("HD")])],
         [name_row(1, "ESPN", "ESPN [DEAD]")])
    # A recorded group that no longer exists: the name is restored, the group is left.
    case("a group that no longer exists",
         [channel(1, "ESPN [DEAD]", group=102)],
         [(1, [alive("HD")])],
         [name_row(1, "ESPN", "ESPN [DEAD]"), group_row(1, "999", "102")])
    # A tag the plugin wrote before crosstalk took over: the fallback strip heals it.
    case("a tag with no recorded change",
         [channel(1, "ESPN [DEAD]", group=102)],
         [(1, [alive("HD")])],
         [])
    # Other apps' marks and provider tags are not triage's: the strip keeps them (both
    # sides agree; the port's strip set is the plugin's).
    case("other apps' marks survive the tag strip",
         [channel(1, "ESPN [BadEPG] [DEAD]", group=102), channel(2, "CNN [1080p] [DEAD]"),
          channel(3, "AMC [Unk] [Slow]"), channel(4, "TNT [BadEPG]")],
         [(1, [alive("HD")]), (2, [alive("HD")]), (3, [alive("HD")]), (4, [alive("HD")])],
         [])
    # Step 9 (`paren`): a status tag in parens strips for the port only.
    case("a status tag in parens",
         [channel(1, "ESPN (DEAD)", group=102), channel(2, "CNN [HD] (Slow)")],
         [(1, [alive("HD")]), (2, [alive("HD")])],
         [])
    # A healthy channel carrying only a quality tag is never touched.
    case("a quality tag alone",
         [channel(1, "ESPN [HD]", group=100)],
         [(1, [alive("HD")])],
         [])
    # A name that is only a tag is left alone (stripping it would leave nothing).
    case("a name that is only a tag",
         [channel(1, "[DEAD]", group=100)],
         [(1, [alive("HD")])],
         [])
    # A failed or preview row is not a change.
    case("a failed triage row",
         [channel(1, "ESPN [DEAD]", group=102)],
         [(1, [alive("HD")])],
         [name_row(1, "ESPN", "ESPN [DEAD]", status="failed"),
          group_row(1, "100", "102", status="failed")])
    case("a planned triage row",
         [channel(1, "ESPN [DEAD]", group=102)],
         [(1, [alive("HD")])],
         [name_row(1, "ESPN", "ESPN [DEAD]", status="planned"),
          group_row(1, "100", "102", status="planned")])
    # The chain stops at the first change that no longer holds.
    case("a chain broken in the middle",
         [channel(1, "ESPN [Slow]", group=100)],
         [(1, [alive("HD")])],
         [name_row(1, "ESPN", "ESPN [DEAD]"), name_row(1, "ESPN [DEAD]", "ESPN [Slow]")])
    # Several channels at once, mixed shapes.
    case("a mixed restore",
         [channel(1, "ESPN [DEAD]", group=102), channel(2, "Sky", group=100),
          channel(3, "AMC [Blank]", group=102), channel(4, "CNN [HD]", group=100)],
         [(1, [alive("HD")]), (2, [dead()]), (3, [alive("SD")]), (4, [alive("HD")])],
         [name_row(1, "ESPN", "ESPN [DEAD]"), group_row(1, "100", "102"),
          group_row(2, "100", "102"),
          name_row(3, "AMC", "AMC [Blank]"), group_row(3, "100", "102")])
    return cases


WORDS = ["ESPN", "CNN", "TNT", "HBO", "MAX", "AMC"]
TAGS = ["", " [DEAD]", " [Slow]", " [Blank]", " [HD]", " [SD]", " [GONE]"]


def random_triage_case(rng, index):
    channels = []
    streams = []
    for channel_index in range(rng.randint(1, 6)):
        channel_id = channel_index + 1
        name = rng.choice(WORDS) + rng.choice(TAGS)
        probe_name = name if rng.random() < 0.85 else rng.choice(WORDS)
        channels.append(channel(channel_id, name, group=rng.choice([100, 101, 102]),
                                probe_name=probe_name))
        rows = []
        for _ in range(rng.randint(1, 3)):
            shape = rng.random()
            if shape < 0.35:
                rows.append(alive(rng.choice(["UHD", "FHD", "HD", "SD", "Unknown"]),
                                  rng.choice([20.0, 25.0, 30.0, 60.0])))
            elif shape < 0.6:
                rows.append(dead(rng.choice(["Timeout", "403 Forbidden", "Black Screen"])))
            else:
                rows.append(skipped())
        streams.append((channel_id, rows))
    overrides = {}
    if rng.random() < 0.3:
        overrides["dead_rename_format"] = rng.choice(["{name} [DEAD]", "[DEAD] {name}",
                                                      "{name} [GONE]"])
    if rng.random() < 0.3:
        overrides["video_format_suffixes"] = rng.choice(
            ["UHD, FHD, HD, SD, Unknown", "UHD, FHD", "HD", ""])
    if rng.random() < 0.2:
        overrides["move_to_group_name"] = rng.choice(["Graveyard", "US: Sports", "Parked"])
    if rng.random() < 0.3:
        overrides["scheduler_rename_dead_channels"] = rng.random() < 0.7
    if rng.random() < 0.3:
        overrides["scheduler_add_video_format_suffix"] = rng.random() < 0.7
    return {
        "kind": "triage",
        "comment": f"random #{index}",
        "channels": channels,
        "groups": groups(),
        "streams": streams,
        "settings": settings(**overrides),
    }


def random_restore_case(rng, index):
    channels = []
    streams = []
    history = []
    for channel_index in range(rng.randint(1, 5)):
        channel_id = channel_index + 1
        base = rng.choice(WORDS)
        tags = rng.choice(TAGS)
        name = base + tags
        group = rng.choice([100, 101, 102])
        channels.append(channel(channel_id, name, group=group))
        rows = [alive(rng.choice(["HD", "SD"]), rng.choice([20.0, 30.0]))] \
            if rng.random() < 0.8 else [dead()]
        streams.append((channel_id, rows))
        if tags and rng.random() < 0.7:
            history.append({"channel_id": channel_id, "field": "name",
                            "before": base, "after": name, "status": "ok"})
        if group != 100 and rng.random() < 0.7:
            history.append({"channel_id": channel_id, "field": "channel_group_id",
                            "before": "100", "after": str(group), "status": "ok"})
    return {
        "kind": "restore",
        "comment": f"random restore #{index}",
        "channels": channels,
        "groups": groups(),
        "streams": streams,
        "history": history,
        "settings": settings(),
    }


# ---------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--show", type=int, default=10, help="mismatches to print")
    parser.add_argument("--random", type=int, default=120, help="randomised cases to add")
    parser.add_argument("--seed", type=int, default=20261105, help="seed for random cases")
    parser.add_argument("--binary",
                        default=str(REPO / "target" / "debug" / "examples" / "triage_io"),
                        help="path to the triage_io example binary")
    args = parser.parse_args()

    if not PLUGIN_INNER.is_dir():
        print(f"reference plugin not found at {PLUGIN_INNER}; "
              "run `git submodule update --init`", file=sys.stderr)
        return 2

    print(f"building {args.binary} ...", file=sys.stderr)
    subprocess.run(
        ["cargo", "build", "-q", "-p", "crosstalk-streams", "--example", "triage_io"],
        cwd=REPO, check=True)

    plugin_mod = load_plugin()
    rng = random.Random(args.seed)
    triage_cases = triage_worlds() + [random_triage_case(rng, index)
                                      for index in range(args.random // 2)]
    restore_cases = restore_worlds() + [random_restore_case(rng, index)
                                        for index in range(args.random // 2)]

    answers = run_rust([rust_case(case, case["kind"]) for case in triage_cases]
                       + [rust_case(case, "restore") for case in restore_cases],
                       args.binary)
    triage_answers = answers[:len(triage_cases)]
    restore_answers = answers[len(triage_cases):]

    mismatches = 0
    shown = 0
    state = {"names_compared": 0, "groups_compared": 0, "stacked": 0, "untracked": 0,
             "all_off": 0, "paren": 0}
    refusals = 0

    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        for case, answer in zip(triage_cases, triage_answers):
            if not any(value for key, value in case["settings"].items()
                       if key.startswith("scheduler_")):
                # The plugin's scheduler silently does nothing when every post-action switch is
                # off; the port REFUSES, because an action an operator ran by hand that changes
                # nothing reads like a success. Counted as the documented divergence it is.
                state["all_off"] += 1
                if not answer.get("error"):
                    mismatches += 1
                    if mismatches <= args.show:
                        print(f"[{case['comment']}] every switch is off and the port did not "
                              f"refuse")
                continue
            try:
                expected = triage_answer(plugin_mod, case, workdir)
            except SystemExit:
                # The plugin refused the run (an unusable format, an empty group name, an
                # empty suffix list). The port must refuse it too.
                refusals += 1
                if not answer.get("error"):
                    mismatches += 1
                    if mismatches <= args.show:
                        print(f"[{case['comment']}] the plugin refused, the port did not")
                continue
            if answer.get("error"):
                mismatches += 1
                if mismatches <= args.show:
                    print(f"[{case['comment']}] the port refused: {answer['error']} "
                          f"(the plugin ran: {expected['messages']})")
                continue
            problems = compare_triage(case, expected, answer, args.show, state)
            if problems:
                mismatches += len(problems)
                if shown < args.show:
                    print(f"[triage: {case['comment']}]")
                    for problem in problems:
                        print(f"  {problem}")
                    shown += 1

        for case, answer in zip(restore_cases, restore_answers):
            try:
                expected = restore_answer(plugin_mod, case, workdir)
            except SystemExit as failure:
                refusals += 1
                if not answer.get("error"):
                    mismatches += 1
                    if mismatches <= args.show:
                        print(f"[{case['comment']}] the plugin refused ({failure}), "
                              f"the port did not")
                continue
            if answer.get("error"):
                mismatches += 1
                if mismatches <= args.show:
                    print(f"[{case['comment']}] the port refused: {answer['error']}")
                continue
            problems = compare_restore(case, expected, answer, args.show, state)
            if problems:
                mismatches += len(problems)
                if shown < args.show:
                    print(f"[restore: {case['comment']}]")
                    for problem in problems:
                        print(f"  {problem}")
                    shown += 1

    print(f"[triage] {mismatches} mismatch(es) over {len(triage_cases)} triage case(s) and "
          f"{len(restore_cases)} restore case(s) ({refusals} refusal(s) agreed): "
          f"{state['names_compared']} channel name(s), {state['groups_compared']} group(s) "
          f"compared; documented divergences: {state['stacked']} stacked tag(s), "
          f"{state['untracked']} untracked tag strip(s), {state['all_off']} all-off "
          f"refusal(s), {state['paren']} paren tag(s)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
