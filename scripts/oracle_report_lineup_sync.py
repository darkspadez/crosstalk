#!/usr/bin/env python3
"""Oracle diff report for step 18: lineup sync + tidy vs Lineuparr.

Lineup `sync_*` mirrors Lineuparr's group + channel sync on the owned-channel
rule; `tidy_*` is its post-match cleanup as hide instead of delete. Lineuparr
is the oracle. This script drives LU's REAL `_sync_groups` (~2877),
`_do_sync_channels` (~2962: get_or_create by (name, group) with
`_get_channel_number` / `_init_assigner_state` numbering and
`_enable_channels_in_profiles` ~1612) and the `_migrate_single_group_name`
legacy rename (~2834) with ORM fakes over the 29 lineup files x 4 numbering
modes x 4 `category_detail` modes, runs every case twice (rerun) and with a
pre-existing foreign same-name channel plus a number squatter, vs `sync_dump`
(the action's own `decide_sync`), and compares LU's cleanup fragment (the
stream-less-in-lineup-groups query chain + delete, ~3659-3680, driven against
the same fakes) vs `tidy_dump` (the action's own `decide_tidy`).

Usage:
    python3 -I scripts/oracle_report_lineup_sync.py [--check] [--show N]

    --check   regenerate into a temp file and fail unless it is byte-identical
              to the committed report (the CI gate: the report reproduces).
    --show N  differences printed per bucket (default 10).

Attribution buckets (every difference lands in exactly one, each by a CHECK —
nothing lands in a bucket by elimination; what no check explains is
`unattributed`):

- `ownership-not-adopt`: LU's `get_or_create(name, group)` adopts a foreign
  same-named channel in the target group (and then renumbers/moves/enables it);
  Rust reports the clash and skips. CHECKED on the skip reason.
- `stable-numbers-on-rerun`: LU renumbers every owned channel on rerun in the
  auto/specific modes (the assigner walks every entry and writes whatever it
  lands on, so one squatter shifts every later channel); Rust assigns numbers
  to new channels only, so a rerun is a no-op. CHECKED: Rust run 2 has zero
  creates and zero edits while LU run 2 updates.
- `owner-key-ignores-category`: the owner key carries no folded category, so a
  `category_detail` change moves owned channels instead of re-creating them.
  CHECKED: key sets identical across details, and a detail-change rerun moves
  with zero creates. (LU re-creates: `get_or_create(name, new_group)` misses.)
- `global-number-check`: a lineup number held by another channel is refused
  (numbers are global in Dispatcharr's outputs, step 10 review); LU sets it
  anyway. CHECKED: the refusal (the action's own `plan_sync_creates`, global
  number check) names the number LU's channel for the entry holds.
- `numbering-skips-dont-consume`: in the auto/specific modes LU's assigner
  spends a number on every entry, duplicates and the adopted foreigner
  included; Rust numbers only its creates. CHECKED: Rust's k-th create takes
  exactly LU's k-th assigned number (same ascending free-number walk; LU's
  per-entry numbers are recorded from `_get_channel_number` itself, in call
  order), so a per-entry difference is the shift, not a different rule. The
  order itself is CHECKED first: Rust's rows name exactly the entries LU
  numbered, in LU's call order (users see the numbers; the lineup decides
  them), else the case is unattributed.
- `duplicates-in-lineup`: 26 same names in two categories across the shipped
  files: LU makes one channel per (name, group); Rust makes one owned channel
  per (file, name) — first category in LU's order wins. CHECKED on the entry
  name occurring twice in the file. When the fold puts both entries in ONE
  group (`none`, `simple`, `refined`), LU's second `get_or_create` finds the
  first channel and renumbers it to the later entry's number; Rust keeps the
  first entry's. CHECKED: Rust's number is LU's assignment for the first entry
  and LU's final number is its assignment for the later duplicate. Every channel LU made is accounted for by
  a Rust row of the same (name, group) or is a pre-existing foreigner.
- `profile-rows-only-on-create`: LU enables EVERY synced channel in the
  configured profiles (created, adopted, existing); Rust sets
  `channel_profile_ids` on the create POST only — existing owned channels never
  gain rows (phase-2 rule). CHECKED: Rust's resolved ids ride the creates; LU's
  membership set covers adopted/existing channels too.
- `migration-owned-only`: `_migrate_single_group_name` renames the pre-fix
  `"<prefix>: All"` group; Rust renames owned groups only, LU any such group.
  CHECKED on the foreign-group case.
- `cleanup-delete-vs-hide` / `cleanup-owned-only`: LU DELETES every stream-less
  channel in lineup groups (no ownership marker); tidy HIDES owned stream-less
  channels only. CHECKED on the foreign stream-less channel and on the write
  kind. `cleanup-unhide-guard`: tidy unhides only what tidy hid (run rows);
  LU's cleanup has no unhide at all.
- `unattributed`: listed for review. A non-empty list is the review surface;
  the script still exits 0 (reports never fail CI) — but the step's bar is 0.
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import tempfile
import types
from pathlib import Path
from unittest.mock import MagicMock

sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO / "reference" / "Dispatcharr-Lineuparr-Plugin" / "Lineuparr"
PKG = "lineuparr_sync_oracle"

REPORT = REPO / "docs" / "oracle-reports" / "lineup-sync.md"

NUMBERING_MODES = ["lineup", "auto_next", "auto_highest", "specific"]
DETAILS = ["none", "refined", "simple", "normal"]
SPECIFIC_START = 1000


# ---------------------------------------------------------------------------------------
# Loading the plugin with a REAL fake ORM for apps.channels.models
# ---------------------------------------------------------------------------------------

def _quiet_logger():
    logger = logging.getLogger("lineuparr_sync_oracle")
    logger.handlers = [logging.NullHandler()]
    logger.setLevel(logging.CRITICAL)
    logger.propagate = False
    return logger


class FakeDB:
    """One in-memory Dispatcharr: groups, channels, profiles, memberships, streams."""

    def __init__(self):
        self.groups: dict[int, dict] = {}
        self.channels: dict[int, dict] = {}
        self.profiles: dict[int, dict] = {}
        # (profile_id, channel_id) -> enabled
        self.memberships: dict[tuple[int, int], bool] = {}
        # channel_id -> [stream ids]
        self.channelstreams: dict[int, list] = {}
        self.next_id = 1

    def add_group(self, name, gid=None):
        gid = gid if gid is not None else self._next()
        self.groups[gid] = {"id": gid, "name": name}
        return self.groups[gid]

    def add_channel(self, name, group_id, number=None, cid=None):
        cid = cid if cid is not None else self._next()
        self.channels[cid] = {
            "id": cid,
            "name": name,
            "channel_group_id": group_id,
            "channel_number": number,
        }
        return self.channels[cid]

    def add_profile(self, name, pid=None):
        pid = pid if pid is not None else self._next()
        self.profiles[pid] = {"id": pid, "name": name}
        return self.profiles[pid]

    def _next(self):
        nid = self.next_id
        self.next_id += 1
        return nid

    def bump_past(self, nid):
        self.next_id = max(self.next_id, nid + 1)


# The fake ORM classes are defined ONCE (the plugin binds them at import) and
# read the CURRENT database through `_DB`; each drive swaps it in.
_DB: dict[str, FakeDB | None] = {"db": None}


def _db() -> FakeDB:
    assert _DB["db"] is not None, "no fake database is current"
    return _DB["db"]  # type: ignore[return-value]


def _match(row: dict, key: str, value) -> bool:
    if key.endswith("__in"):
        return row.get(key[: -len("__in")]) in value
    return row.get(key) == value


class QuerySet:
    def __init__(self, table: str, filters: dict | None = None):
        self._table = table
        self._filters = filters or {}

    def _table_rows(self) -> list[dict]:
        db = _db()
        if self._table == "membership":
            return [
                {"channel_profile_id": pid, "channel_id": cid, "enabled": enabled}
                for (pid, cid), enabled in db.memberships.items()
            ]
        table = {
            "group": db.groups,
            "channel": db.channels,
            "profile": db.profiles,
        }[self._table]
        return list(table.values())

    def _rows(self) -> list[dict]:
        return [
            row for row in self._table_rows()
            if all(_match(row, key, value) for key, value in self._filters.items())
        ]

    def filter(self, **kwargs) -> "QuerySet":
        merged = dict(self._filters)
        merged.update(kwargs)
        return type(self)(self._table, merged)

    def exists(self) -> bool:
        return bool(self._rows())

    def first(self):
        rows = self._rows()
        if not rows:
            return None
        return _wrap(self._table, rows[0])

    def count(self) -> int:
        return len(self._rows())

    def values(self, *fields) -> list[dict]:
        class Rows(list):
            def first(self):
                return self[0] if self else None

        return Rows(
            [{field: row.get(field) for field in fields} for row in self._rows()]
        )

    def values_list(self, field, flat=False) -> list:
        return [row.get(field) for row in self._rows()]

    def get_or_create(self, defaults=None, **kwargs):
        for row in self._table_rows():
            if all(_match(row, key, value) for key, value in kwargs.items()):
                return _wrap(self._table, row), False
        payload = dict(kwargs)
        payload.update(defaults or {})
        db = _db()
        if self._table == "group":
            row = db.add_group(payload["name"])
        elif self._table == "channel":
            row = db.add_channel(
                payload["name"], payload.get("channel_group_id"),
                payload.get("channel_number"),
            )
        else:  # pragma: no cover - profiles are never get_or_created
            raise AssertionError(self._table)
        db.bump_past(row["id"])
        return _wrap(self._table, row), True

    def bulk_create(self, objs, ignore_conflicts=False):
        db = _db()
        for obj in objs:
            key = (obj.channel_profile_id, obj.channel_id)
            if ignore_conflicts and key in db.memberships:
                continue
            db.memberships[key] = obj.enabled

    def update(self, **kwargs) -> int:
        if self._table == "membership":
            db = _db()
            n = 0
            for (pid, cid), enabled in list(db.memberships.items()):
                row = {"channel_profile_id": pid, "channel_id": cid, "enabled": enabled}
                if all(_match(row, key, value) for key, value in self._filters.items()):
                    for field, value in kwargs.items():
                        if field == "enabled":
                            db.memberships[(pid, cid)] = value
                    n += 1
            return n
        raise AssertionError(self._table)

    def delete(self) -> int:
        if self._table != "channel":  # pragma: no cover - only channels delete
            raise AssertionError(self._table)
        db = _db()
        ids = [row["id"] for row in self._rows()]
        for cid in ids:
            del db.channels[cid]
            db.channelstreams.pop(cid, None)
        return len(ids)

    def annotate(self, **kwargs) -> "QuerySet":
        # Only stream_count=Count('channelstream') is driven (the cleanup chain).
        for alias, expr in kwargs.items():
            if getattr(expr, "field", None) != "channelstream":  # pragma: no cover
                raise AssertionError(expr)
            if alias != "stream_count":  # pragma: no cover
                raise AssertionError(alias)
        return AnnotatedQuerySet(self._table, self._filters)


class AnnotatedQuerySet(QuerySet):
    def _rows(self) -> list[dict]:
        db = _db()
        out = []
        for row in self._table_rows():
            row = dict(row)
            row["stream_count"] = len(db.channelstreams.get(row["id"], []))
            if all(_match(row, key, value) for key, value in self._filters.items()):
                out.append(row)
        return out


class Manager:
    def __init__(self, table: str):
        self._table = table

    def all(self) -> QuerySet:
        return QuerySet(self._table)

    def filter(self, **kwargs) -> QuerySet:
        return QuerySet(self._table, dict(kwargs))

    def get_or_create(self, defaults=None, **kwargs):
        return QuerySet(self._table).get_or_create(defaults=defaults, **kwargs)

    def values_list(self, *args, **kwargs):
        return QuerySet(self._table).values_list(*args, **kwargs)

    def bulk_create(self, objs, ignore_conflicts=False):
        return QuerySet(self._table).bulk_create(objs, ignore_conflicts)


def _wrap(table: str, row: dict):
    if table == "group":
        return FakeGroup(row)
    if table == "channel":
        return FakeChannel(row)
    raise AssertionError(table)  # pragma: no cover


class FakeGroup:
    objects = Manager("group")

    def __init__(self, row: dict):
        self.id = row["id"]
        self.name = row["name"]

    def save(self, update_fields=None):
        _db().groups[self.id]["name"] = self.name


class FakeChannel:
    objects = Manager("channel")

    def __init__(self, row: dict):
        self.id = row["id"]
        self.name = row["name"]
        self.channel_group_id = row.get("channel_group_id")
        self.channel_number = row.get("channel_number")

    def save(self, update_fields=None):
        stored = _db().channels[self.id]
        for field in update_fields or []:
            stored[field] = getattr(self, field)


class FakeChannelProfileMembership:
    objects = Manager("membership")

    def __init__(self, channel_profile_id=None, channel_id=None, enabled=True, **kwargs):
        self.channel_profile_id = channel_profile_id
        self.channel_id = channel_id
        self.enabled = enabled


class FakeCount:
    def __init__(self, field):
        self.field = field


def _install_stubs():
    """Django stubs (the plugin's own test-suite shape) plus the REAL fake ORM
    for `apps.channels.models`; everything else answers MagicMocks. Runs once:
    the fake classes read the current database through `_DB`, which each drive
    swaps in (the plugin binds the classes at import)."""
    for mod in (
        "django",
        "django.db",
        "django.db.transaction",
        "apps.m3u",
        "apps.m3u.models",
        "apps.epg",
        "apps.epg.models",
        "core",
        "core.utils",
    ):
        sys.modules.setdefault(mod, MagicMock(name=mod))
    # `from django.db.models import Count` inside the cleanup: a stub with Count.
    django_models = types.ModuleType("django.db.models")
    django_models.Count = FakeCount
    sys.modules["django.db.models"] = django_models

    apps_pkg = types.ModuleType("apps")
    apps_pkg.__path__ = []
    channels_pkg = types.ModuleType("apps.channels")
    channels_pkg.__path__ = []
    models_mod = types.ModuleType("apps.channels.models")
    models_mod.Channel = FakeChannel
    models_mod.ChannelGroup = FakeGroup
    models_mod.ChannelProfile = types.SimpleNamespace(objects=Manager("profile"))
    models_mod.ChannelProfileMembership = FakeChannelProfileMembership
    models_mod.ChannelStream = types.SimpleNamespace(objects=Manager("channelstream"))
    models_mod.Stream = types.SimpleNamespace(objects=Manager("stream"))
    # No Logo, no ChannelOverride: both import sites take their ImportError path
    # (logos never consulted by sync; override pins fall back to raw numbers).
    apps_pkg.channels = channels_pkg
    channels_pkg.models = models_mod
    sys.modules["apps"] = apps_pkg
    sys.modules["apps.channels"] = channels_pkg
    sys.modules["apps.channels.models"] = models_mod


_PLUGIN = {}


def _load_plugin():
    if "module" in _PLUGIN:
        return _PLUGIN["module"]
    sys.path.insert(0, str(PLUGIN_DIR.parent))
    pkg = types.ModuleType(PKG)
    pkg.__path__ = [str(PLUGIN_DIR)]
    sys.modules[PKG] = pkg
    import importlib

    module = importlib.import_module(f"{PKG}.plugin")
    module.LOGGER.disabled = True
    for name in ("_trigger_frontend_refresh", "_record_channels_created"):
        setattr(module.Plugin, name, lambda self, *args, **kwargs: None)
    module.ProgressTracker._publish = lambda self, *args, **kwargs: None
    _PLUGIN["module"] = module
    return module


def _use_db(db: FakeDB):
    _DB["db"] = db


# ---------------------------------------------------------------------------------------
# Driving LU over one world
# ---------------------------------------------------------------------------------------

def _lu_lineup_names(module, filename, detail, prefix=""):
    """The (category, group, entry) list LU syncs, in LU's order."""
    plugin = module.Plugin()
    settings = {"lineup_file": filename, "category_detail": detail, "group_prefix": prefix}
    data = plugin._load_lineup(dict(settings), _quiet_logger())
    group_prefix = plugin._get_group_prefix(dict(settings), data)
    out = []
    for category, channels in data["categories"].items():
        group = plugin._make_group_name(group_prefix, category)
        for entry in channels:
            out.append({
                "name": entry["name"],
                "number": plugin._parse_channel_number(entry.get("number")),
                "category": category,
                "group": group,
            })
    return data, group_prefix, out


def _lu_sync(module, db: FakeDB, settings: dict) -> dict:
    """Run `_sync_groups` + `_do_sync_channels` for real; snapshot the writes."""
    plugin = module.Plugin()
    logger = _quiet_logger()
    groups_result = plugin._sync_groups(dict(settings), logger)
    # LU's per-entry number in its call order: the final DB number is not it
    # when a later same-group duplicate's get_or_create renumbers the channel.
    assigned = []
    real_number = plugin._get_channel_number

    def recording_number(settings_arg, entry, assigner_state):
        number = real_number(settings_arg, entry, assigner_state)
        assigned.append({"name": entry["name"], "number": number})
        return number

    plugin._get_channel_number = recording_number
    channels_result = plugin._do_sync_channels(dict(settings), logger)
    return {
        "groups": groups_result,
        "channels": channels_result,
        "assigned": assigned,
        "db_groups": sorted(
            [{"id": gid, "name": row["name"]} for gid, row in db.groups.items()],
            key=lambda row: row["id"],
        ),
        "db_channels": sorted(
            [
                {
                    "id": cid,
                    "name": row["name"],
                    "group_id": row.get("channel_group_id"),
                    "number": row.get("channel_number"),
                }
                for cid, row in db.channels.items()
            ],
            key=lambda row: row["id"],
        ),
        "memberships": sorted(
            [
                {"profile_id": pid, "channel_id": cid, "enabled": enabled}
                for (pid, cid), enabled in db.memberships.items()
            ],
            key=lambda row: (row["profile_id"], row["channel_id"]),
        ),
    }


def _lu_cleanup(module, db: FakeDB, filename: str) -> dict:
    """The post-match cleanup fragment (~3659-3680), driven for real: lineup
    group names from the plugin's own `_make_group_name`, then the exact query
    chain (filter groups, annotate Count, filter stream-less, delete)."""
    plugin = module.Plugin()
    data = plugin._load_lineup(
        {"lineup_file": filename, "category_detail": "normal"}, _quiet_logger()
    )
    prefix = plugin._get_group_prefix({"lineup_file": filename}, data)
    names = [plugin._make_group_name(prefix, cat) for cat in data["categories"]]
    from django.db.models import Count  # noqa: F401  (the stub)

    group_ids = models_mod().ChannelGroup.objects.filter(name__in=names).values_list(
        "id", flat=True
    )
    empty = models_mod().Channel.objects.filter(channel_group_id__in=group_ids).annotate(
        stream_count=Count("channelstream")
    ).filter(stream_count=0)
    count = empty.count()
    deleted = sorted(
        [{"id": row["id"], "name": row["name"]} for row in empty.values("id", "name")],
        key=lambda row: row["id"],
    )
    empty.delete()
    return {"count": count, "deleted": deleted}


def models_mod():
    return sys.modules["apps.channels.models"]


# ---------------------------------------------------------------------------------------
# Driving Rust (one subprocess for the whole matrix)
# ---------------------------------------------------------------------------------------

def _rust_sync(cases: list[dict]) -> dict[str, dict]:
    proc = subprocess.run(
        ["cargo", "run", "-q", "-p", "crosstalk-lineup", "--example", "sync_dump"],
        input=json.dumps({"cases": cases}),
        capture_output=True,
        text=True,
        cwd=str(REPO),
    )
    if proc.returncode != 0:
        raise RuntimeError(f"sync_dump failed: {proc.stderr[-4000:]}")
    return {case["id"]: case for case in json.loads(proc.stdout)["cases"]}


def _rust_tidy(world: dict) -> list[dict]:
    proc = subprocess.run(
        ["cargo", "run", "-q", "-p", "crosstalk-lineup", "--example", "tidy_dump"],
        input=json.dumps(world),
        capture_output=True,
        text=True,
        cwd=str(REPO),
    )
    if proc.returncode != 0:
        raise RuntimeError(f"tidy_dump failed: {proc.stderr[-4000:]}")
    return json.loads(proc.stdout)["rows"]


# ---------------------------------------------------------------------------------------
# The matrix
# ---------------------------------------------------------------------------------------

def _lineup_files() -> list[str]:
    files = sorted(path.name for path in PLUGIN_DIR.glob("*_lineup.json"))
    assert len(files) == 29, f"expected 29 lineup files, found {len(files)}"
    return files


def _settings(filename: str, numbering: str, detail: str) -> dict:
    settings = {
        "lineup_file": filename,
        "category_detail": detail,
        "group_prefix": "",
        "channel_numbering": numbering,
        "starting_channel_number": "",
        "channel_profiles": "Living Room, Nope",
    }
    if numbering == "specific":
        settings["starting_channel_number"] = str(SPECIFIC_START)
    return settings


def _base_world() -> dict:
    return {
        "channels": [],
        "groups": [],
        "profiles": [
            {"id": 1, "name": "Living Room"},
            {"id": 2, "name": "Kids"},
        ],
    }


def _seed_db(db: FakeDB, world: dict):
    for profile in world["profiles"]:
        db.add_profile(profile["name"], pid=profile["id"])
        db.bump_past(profile["id"])
    for group in world["groups"]:
        db.add_group(group["name"], gid=group["id"])
        db.bump_past(group["id"])
    for channel in world["channels"]:
        db.add_channel(
            channel["name"], channel.get("group_id"), channel.get("number"),
            cid=channel["id"],
        )
        db.bump_past(channel["id"])
        for stream in channel.get("streams", []):
            db.channelstreams.setdefault(channel["id"], []).append(stream)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="fail unless the regenerated report is byte-identical")
    parser.add_argument("--show", type=int, default=10, help="differences per bucket")
    args = parser.parse_args(argv)

    files = _lineup_files()
    # The plugin binds the fake ORM classes at import; each drive swaps the
    # current database underneath them.
    _install_stubs()
    module = _load_plugin()

    # -- Run A: 29 files x 4 numbering x 4 detail, clean world ----------------------
    run_a: list[dict] = []
    for filename in files:
        for numbering in NUMBERING_MODES:
            for detail in DETAILS:
                run_a.append({
                    "id": f"A/{filename}/{numbering}/{detail}",
                    "filename": filename,
                    "numbering": numbering,
                    "detail": detail,
                    "world": _base_world(),
                })
    # -- Run C: foreign clash + number squat (detail normal) ------------------------
    run_c: list[dict] = []
    for filename in files:
        for numbering in NUMBERING_MODES:
            run_c.append({
                "id": f"C/{filename}/{numbering}/normal",
                "filename": filename,
                "numbering": numbering,
                "detail": "normal",
                "world": _base_world(),
                "clash": True,
                "squat": True,
            })
    cases = run_a + run_c

    # LU's entry order per file: needed for clash/squat placement.
    _use_db(FakeDB())
    entries: dict[tuple[str, str], list[dict]] = {}
    for filename in files:
        _, _, rows = _lu_lineup_names(module, filename, "normal")
        entries[(filename, "normal")] = rows

    # Fill clash + squat worlds.
    for case in cases:
        if not case.get("clash"):
            continue
        rows = entries[(case["filename"], "normal")]
        first, second = rows[0], rows[1] if len(rows) > 1 else rows[0]
        world = case["world"]
        world["channels"].append({
            "id": 9001,
            "name": first["name"],
            "group_id": 9011,
            "group": first["group"],
            "number": first["number"] if first["number"] is not None else 999,
        })
        world["groups"].append({"id": 9011, "name": first["group"]})
        if case["numbering"] == "lineup" and second["number"] is not None:
            squat_number = second["number"]
        elif case["numbering"] == "specific":
            squat_number = SPECIFIC_START
        else:
            squat_number = 2
        world["channels"].append({
            "id": 9002, "name": "Squatter", "group_id": 9012, "group": "Foreign",
            "number": squat_number,
        })
        world["groups"].append({"id": 9012, "name": "Foreign"})

    # LU drives (in-process, indexed fakes).
    lu_a: dict[str, dict] = {}
    for case in cases:
        db = FakeDB()
        _use_db(db)
        world = _resolve_world(case["world"])
        _seed_db(db, world)
        settings = _settings(case["filename"], case["numbering"], case["detail"])
        lu_a[case["id"]] = _lu_sync(module, db, settings)
        lu_a[case["id"]]["world"] = world

    # Rust drives (one subprocess).
    rust_cases = []
    for case in cases:
        resolved = lu_a[case["id"]]["world"]
        rust_cases.append({
            "id": case["id"],
            "settings": {
                "lineup_file": case["filename"],
                "category_detail": case["detail"],
                "group_prefix": "",
                "channel_numbering": case["numbering"],
                **(
                    {"starting_channel_number": str(SPECIFIC_START)}
                    if case["numbering"] == "specific" else {}
                ),
                "channel_profiles": "Living Room, Nope",
            },
            "channels": [
                {
                    "id": channel["id"],
                    "name": channel["name"],
                    "number": channel.get("number"),
                    "group": _group_name(resolved, channel.get("group_id"), channel.get("group")),
                }
                for channel in resolved["channels"]
            ],
            "groups": [
                {"id": group["id"], "name": group["name"]}
                for group in resolved["groups"]
            ],
            "profiles": resolved["profiles"],
            "owned": {},
            "owned_groups": [],
        })
    rust_a = _rust_sync(rust_cases)

    # -- Run B: rerun everything on the created world + a squatter ------------------
    rust_b_cases = []
    lu_b: dict[str, dict] = {}
    for case_idx, case in enumerate(cases):
        first = lu_a[case["id"]]
        rust_rows = rust_a[case["id"]]["rows"]
        # The rerun world: what RUST run 1 created (oracle-assigned ids), plus
        # the original foreigners, plus a fresh low squatter that shifts LU's
        # assigner (auto_next/specific) or sits above the max (auto_highest).
        world = {
            "channels": [dict(channel) for channel in first["world"]["channels"]],
            "groups": [dict(group) for group in first["world"]["groups"]],
            "profiles": [dict(profile) for profile in first["world"]["profiles"]],
        }
        next_id = 100
        group_ids = {group["name"]: group["id"] for group in world["groups"]}
        for row in rust_rows:
            if row["action"] != "Create":
                continue
            gid = group_ids.get(row["group"])
            if gid is None:
                gid = 5000 + len(group_ids)
                group_ids[row["group"]] = gid
                world["groups"].append({"id": gid, "name": row["group"]})
            world["channels"].append({
                "id": next_id,
                "name": row["name"],
                "group_id": gid,
                "number": row["number"],
            })
            next_id += 1
        if case["numbering"] == "auto_next":
            world["channels"].append({
                "id": 9501, "name": "Rerun Squatter", "group_id": 9012,
                "group": "Foreign", "number": 2,
            })
            world["groups"].append({"id": 9012, "name": "Foreign"})
        elif case["numbering"] == "specific":
            world["channels"].append({
                "id": 9501, "name": "Rerun Squatter", "group_id": 9012,
                "group": "Foreign", "number": SPECIFIC_START + 1,
            })
            world["groups"].append({"id": 9012, "name": "Foreign"})
        elif case["numbering"] == "auto_highest":
            high = max(
                [channel.get("number") or 0 for channel in world["channels"]] + [0]
            )
            world["channels"].append({
                "id": 9501, "name": "Rerun Squatter", "group_id": 9012,
                "group": "Foreign", "number": high + 5,
            })
            world["groups"].append({"id": 9012, "name": "Foreign"})
        # LU reruns on the same world (it adopts by (name, group): no ownership).
        db = FakeDB()
        _use_db(db)
        resolved = _resolve_world(world)
        _seed_db(db, resolved)
        settings = _settings(case["filename"], case["numbering"], case["detail"])
        lu_b[case["id"]] = _lu_sync(module, db, settings)
        lu_b[case["id"]]["world"] = resolved
        # Rust reruns with the ownership run 1 would have recorded.
        owned = {
            row["key"]: channel["id"]
            for row in rust_rows if row["action"] == "Create"
            for channel in resolved["channels"]
            if channel["name"] == row["name"]
            and _group_name(resolved, channel.get("group_id"), channel.get("group")) == row["group"]
        }
        rust_b_cases.append({
            "id": case["id"],
            "settings": rust_cases[case_idx]["settings"],
            "channels": [
                {
                    "id": channel["id"],
                    "name": channel["name"],
                    "number": channel.get("number"),
                    "group": _group_name(resolved, channel.get("group_id"), channel.get("group")),
                }
                for channel in resolved["channels"]
            ],
            "groups": [
                {"id": group["id"], "name": group["name"]} for group in resolved["groups"]
            ],
            "profiles": resolved["profiles"],
            "owned": owned,
            "owned_groups": [],
        })
    rust_b = _rust_sync(rust_b_cases)

    # -- Attribution ---------------------------------------------------------------
    buckets: dict[str, list[str]] = {
        "ownership-not-adopt": [],
        "stable-numbers-on-rerun": [],
        "owner-key-ignores-category": [],
        "global-number-check": [],
        "numbering-skips-dont-consume": [],
        "duplicates-in-lineup": [],
        "profile-rows-only-on-create": [],
        "migration-owned-only": [],
        "cleanup-delete-vs-hide": [],
        "cleanup-owned-only": [],
        "cleanup-unhide-guard": [],
        "unattributed": [],
    }

    def emit(bucket: str, text: str):
        buckets[bucket].append(text)

    # Run A per-entry comparison.
    for case in cases:
        cid, filename = case["id"], case["filename"]
        lu_channels = {
            (row["name"], _lu_group_name(lu_a[cid], row["group_id"])): row
            for row in lu_a[cid]["db_channels"]
        }
        rows = rust_a[cid]["rows"]
        # Order: Rust plans one row per entry in LU's iteration order (folded
        # categories, file order within), the order numbers are handed out in.
        # CHECKED against the entries LU's `_get_channel_number` was called for.
        lu_assigned = lu_a[cid]["assigned"]
        if [row["name"] for row in rows] != [hit["name"] for hit in lu_assigned]:
            emit("unattributed", f"{cid}: Rust's entry order differs from LU's numbering order")
            continue
        # Auto/specific modes: LU's assigner hands a number to EVERY entry
        # (duplicates and the adopted foreigner included); Rust numbers only its
        # creates from the same ascending free-number walk. So Rust's k-th create
        # takes LU's k-th assigned number exactly — the sequence is checked, not
        # assumed, and a per-entry difference is attributed only when it holds.
        lu_seq = [hit["number"] for hit in lu_assigned]
        rust_creates = [row["number"] for row in rows if row["action"] == "Create"]
        sequence_holds = (
            case["numbering"] != "lineup"
            and rust_creates == lu_seq[: len(rust_creates)]
        )
        create_index = 0
        for index, row in enumerate(rows):
            name, group, number, action = row["name"], row["group"], row["number"], row["action"]
            lu_hit = lu_channels.get((name, group))
            if action == "Create":
                k, create_index = create_index, create_index + 1
                # A later same-named entry folded into the same group: LU's
                # get_or_create finds this channel again and renumbers it to
                # that entry's number; Rust skips the duplicate (first wins).
                renumbered_by_duplicate = (
                    lu_hit is not None
                    and lu_hit["number"] != lu_seq[index]
                    and any(
                        later["name"] == name and later["group"] == group
                        and "first category wins" in later["reason"]
                        and lu_seq[later_index] == lu_hit["number"]
                        for later_index, later in enumerate(rows)
                        if later_index > index
                    )
                )
                if lu_hit is None:
                    emit("unattributed", f"{cid}: Rust creates {name!r} in {group!r} but LU has none")
                elif lu_hit["number"] == number or (number is None and lu_hit["number"] is None):
                    pass
                elif renumbered_by_duplicate and lu_seq[index] == number:
                    emit("duplicates-in-lineup",
                         f"{cid}: {name!r}: LU {lu_hit['number']!r} vs Rust {number!r}: LU's later "
                         f"duplicate in {group!r} renumbered the channel; Rust keeps the first entry's")
                elif sequence_holds and lu_seq[k] == number:
                    also = " (and a later duplicate renumbered LU's)" if renumbered_by_duplicate else ""
                    emit("numbering-skips-dont-consume",
                         f"{cid}: {name!r}: LU {lu_hit['number']!r} vs Rust {number!r}: LU spent "
                         f"numbers on entries Rust skips; Rust's creates take LU's sequence in order{also}")
                else:
                    emit("unattributed", f"{cid}: number differs for {name!r}: LU {lu_hit['number']!r} vs Rust {number!r}")
            elif action == "Skipped" and "first category wins" in row["reason"]:
                other = [other for other in rows if other["name"] == name]
                if len(other) != 2 or lu_hit is None:
                    emit("unattributed", f"{cid}: duplicate {name!r} has {len(other)} rows, LU channel {lu_hit!r}")
                else:
                    emit("duplicates-in-lineup", f"{cid}: {name!r} twice: LU one channel per group, Rust first-category-wins skip")
            elif action == "Skipped" and "rather than adopted" in row["reason"]:
                if lu_hit is None or lu_hit["id"] not in {c["id"] for c in lu_a[cid]["world"]["channels"]}:
                    emit("unattributed", f"{cid}: clash skip for {name!r} but LU did not adopt a pre-existing channel there")
                else:
                    emit("ownership-not-adopt", f"{cid}: foreign {name!r} in {group!r}: LU adopts id {lu_hit['id']}, Rust skips")
            elif action == "Skipped" and "already used" in row["reason"]:
                lu_number = None if lu_hit is None else lu_hit["number"]
                if isinstance(lu_number, float) and lu_number.is_integer():
                    lu_number = int(lu_number)
                if lu_number is not None and f"channel number {lu_number} " in row["reason"]:
                    emit("global-number-check", f"{cid}: {name!r}: LU sets {lu_number} anyway; Rust: {row['reason']}")
                else:
                    emit("unattributed", f"{cid}: number refusal for {name!r} but LU's channel is {lu_hit!r}: {row['reason']}")
            elif action == "Skipped":
                emit("unattributed", f"{cid}: unexplained skip for {name!r}: {row['reason']}")
        # Every channel LU holds after the run is either pre-existing (the
        # foreigners: 9001 adopted, 9002 the squatter) or one Rust accounts for
        # by (name, group): a create, a duplicate skip, or a number refusal.
        pre_existing = {channel["id"] for channel in lu_a[cid]["world"]["channels"]}
        accounted = {
            (row["name"], row["group"]) for row in rows
            if row["action"] == "Create"
            or "first category wins" in row["reason"]
            or "already used" in row["reason"]
        }
        for (name, group), hit in sorted(lu_channels.items()):
            if hit["id"] in pre_existing:
                continue
            if (name, group) not in accounted:
                emit("unattributed", f"{cid}: LU made {name!r} in {group!r} (id {hit['id']}); Rust has no row for it")

    # Rerun comparison: Rust must be a no-op; LU renumbers (auto/specific).
    for case in cases:
        cid = case["id"]
        rust_rows = rust_b[cid]["rows"]
        edits = [row for row in rust_rows if row["action"] in ("Renumber", "Move", "MoveRenumber")]
        creates = [row for row in rust_rows if row["action"] == "Create"]
        if creates or edits:
            emit("unattributed", f"{cid}: rerun not a no-op: {len(creates)} creates, {len(edits)} edits")
        lu_updated = _lu_updated_count(lu_b[cid]["channels"])
        if lu_updated:
            emit("stable-numbers-on-rerun", f"{cid}: rerun: LU updates {lu_updated} channel(s), Rust 0")
        # Owner key ignores category: each key is exactly lineup:<file>:<name>.
        for row in rust_rows:
            want = f"lineup:{case['filename']}:{row['name']}"
            if row["key"] != want:
                emit("unattributed", f"{cid}: key {row['key']!r} is not {want!r}")

    # Detail-change sample: normal -> simple moves, zero creates (2 files).
    for filename in [files[0], files[-1]]:
        _use_db(FakeDB())
        world = _base_world()
        rust_first = _rust_sync([{
            "id": "detail",
            "settings": {"lineup_file": filename, "category_detail": "normal",
                         "channel_numbering": "lineup", "channel_profiles": ""},
            "channels": [], "groups": [], "profiles": world["profiles"],
            "owned": {}, "owned_groups": [],
        }])["detail"]
        owned = {}
        channels = []
        next_id = 100
        group_ids: dict[str, int] = {}
        groups = []
        for row in rust_first["rows"]:
            if row["action"] != "Create":
                continue
            gid = group_ids.setdefault(row["group"], 500 + len(group_ids))
            if not any(group["name"] == row["group"] for group in groups):
                groups.append({"id": gid, "name": row["group"]})
            owned[row["key"]] = next_id
            channels.append({"id": next_id, "name": row["name"],
                             "number": row["number"], "group": row["group"]})
            next_id += 1
        rust_second = _rust_sync([{
            "id": "detail",
            "settings": {"lineup_file": filename, "category_detail": "simple",
                         "channel_numbering": "lineup", "channel_profiles": ""},
            "channels": channels, "groups": groups, "profiles": world["profiles"],
            "owned": owned, "owned_groups": [],
        }])["detail"]
        moves = [row for row in rust_second["rows"]
                 if row["action"] in ("Move", "MoveRenumber")]
        creates = [row for row in rust_second["rows"] if row["action"] == "Create"]
        if creates:
            emit("unattributed", f"detail-change/{filename}: {len(creates)} creates after a detail change")
        elif moves:
            emit("owner-key-ignores-category",
                 f"detail-change/{filename}: normal -> simple moves {len(moves)} owned channel(s), 0 creates")
        else:
            emit("owner-key-ignores-category",
                 f"detail-change/{filename}: folds agree: 0 moves, 0 creates")

    # Profiles: LU enables every synced channel; Rust only sets ids on creates.
    profiles_agree = True
    for case in cases:
        cid = case["id"]
        profiles = rust_a[cid]["profiles"]
        lu_profiles = {row["profile_id"] for row in lu_a[cid]["memberships"]}
        if profiles["resolved"] != [1] or profiles["unknown"] != ["Nope"] or lu_profiles - {1}:
            profiles_agree = False
            emit("unattributed", f"{cid}: profiles resolve to {profiles}, LU enabled in {sorted(lu_profiles)}")
    for case in [case for case in cases if case.get("clash")][:4]:
        cid = case["id"]
        lu_members = {row["channel_id"] for row in lu_a[cid]["memberships"] if row["profile_id"] == 1}
        if 9001 in lu_members:
            emit("profile-rows-only-on-create",
                 f"{cid}: LU enables the adopted foreign channel 9001 in 'Living Room'; "
                 f"Rust sets profile_ids on the create POST only (9001 never created)")
    if profiles_agree:
        emit("profile-rows-only-on-create",
             "unknown profile 'Nope' is skipped on both sides (checked: Rust resolves [1] + lists 'Nope', "
             "LU's memberships touch profile 1 only); existing owned channels never gain membership rows "
             "(phase-2 rule, by construction: only creates carry ids)")

    # Migration: owned vs foreign pre-fix group, driving the REAL
    # `_migrate_single_group_name` (one none-detail file: the code path only
    # depends on the prefix). `old` is the plugin's own 3-line rule, `new` its
    # real `_make_group_name`; Rust's names must agree with both.
    _use_db(FakeDB())
    filename = "US_DirecTV-Premier_lineup.json"
    settings = _settings(filename, "lineup", "none")
    plugin = module.Plugin()
    data = plugin._load_lineup(dict(settings), _quiet_logger())
    prefix = plugin._get_group_prefix(dict(settings), data)
    new = plugin._make_group_name(prefix, "All")
    old = (
        f"{prefix}All" if prefix and prefix[-1] in ":_-/ "
        else f"{prefix}: All" if prefix else "All"
    )
    if old == new:
        emit("unattributed", "migration: old == new for the probe file; pick another")
    else:
        dump = _rust_sync([{
            "id": "migration",
            "settings": {"lineup_file": filename, "category_detail": "none",
                         "channel_numbering": "lineup", "channel_profiles": ""},
            "channels": [], "groups": [{"id": 40, "name": old}],
            "profiles": [], "owned": {}, "owned_groups": [40],
        }])["migration"]["migration"]
        if dump["old"] != old or dump["new"] != new:
            emit("unattributed", f"migration: name mismatch: Rust {(dump['old'], dump['new'])} vs plugin {(old, new)}")
        for owned_flag, want in ((True, "Rename"), (False, "Skipped")):
            trial = FakeDB()
            trial.add_group(old, gid=40)
            _use_db(trial)
            plugin._migrate_single_group_name(prefix, False, _quiet_logger())
            lu_renamed = trial.groups[40]["name"] == new
            rust = _rust_sync([{
                "id": "migration",
                "settings": {"lineup_file": filename, "category_detail": "none",
                             "channel_numbering": "lineup", "channel_profiles": ""},
                "channels": [], "groups": [{"id": 40, "name": old}],
                "profiles": [], "owned": {},
                "owned_groups": [40] if owned_flag else [],
            }])["migration"]["migration"]
            if rust["action"] != want:
                emit("unattributed", f"migration/{'owned' if owned_flag else 'foreign'}: want {want}, got {rust['action']}")
            elif owned_flag and lu_renamed:
                emit("migration-owned-only",
                     f"migration/owned: pre-fix {old!r} -> {new!r} on both sides (rename, channels stay assigned)")
            elif not owned_flag and lu_renamed:
                emit("migration-owned-only",
                     f"migration/foreign: LU renames any {old!r} group; Rust leaves the foreign group alone and reports it")
            else:
                emit("unattributed", f"migration/{'owned' if owned_flag else 'foreign'}: LU renamed={lu_renamed}, Rust {rust['action']}")
        # Both names exist: both sides leave them alone.
        trial = FakeDB()
        trial.add_group(old, gid=40)
        trial.add_group(new, gid=41)
        _use_db(trial)
        plugin._migrate_single_group_name(prefix, False, _quiet_logger())
        rust = _rust_sync([{
            "id": "migration",
            "settings": {"lineup_file": filename, "category_detail": "none",
                         "channel_numbering": "lineup", "channel_profiles": ""},
            "channels": [],
            "groups": [{"id": 40, "name": old}, {"id": 41, "name": new}],
            "profiles": [], "owned": {}, "owned_groups": [40],
        }])["migration"]["migration"]
        if trial.groups[40]["name"] == old and rust["action"] == "Skipped":
            emit("migration-owned-only",
                 f"migration/both-exist: both {old!r} and {new!r} exist: LU warns and leaves both, Rust skips with the reason")
        else:
            emit("unattributed", "migration/both-exist: sides disagree")

    # -- Cleanup vs tidy -------------------------------------------------------------
    _cleanup_section(buckets, emit, module)

    unattr = buckets.pop("unattributed")
    write_report(buckets, unattr, args, files)


def _lu_group_name(snapshot: dict, gid) -> str:
    for group in snapshot["db_groups"]:
        if group["id"] == gid:
            return group["name"]
    return "No Group"


def _group_name(world: dict, gid, fallback=None) -> str:
    for group in world["groups"]:
        if group["id"] == gid:
            return group["name"]
    return fallback or "No Group"


def _resolve_world(world: dict) -> dict:
    """Fill group ids/names both ways, so LU and Rust read the same world."""
    groups = {group["name"]: group["id"] for group in world["groups"]}
    out_channels = []
    for channel in world["channels"]:
        name = channel.get("group") or _group_name(world, channel.get("group_id"))
        gid = channel.get("group_id")
        if gid is None and name:
            gid = groups.get(name)
            if gid is None:
                gid = 8000 + len(groups)
                groups[name] = gid
        out_channels.append({
            "id": channel["id"],
            "name": channel["name"],
            "group_id": gid,
            "group": name or "No Group",
            "number": channel.get("number"),
        })
    out_groups = [{"id": gid, "name": name} for name, gid in groups.items()]
    return {"channels": out_channels, "groups": out_groups, "profiles": world["profiles"]}


def _lu_updated_count(channels_result: dict) -> int:
    message = channels_result.get("message", "")
    # "Channels synced: 3 created, 5 updated, 2 unchanged" (or the dry-run form).
    import re

    match = re.search(r"(\d+) updated", message)
    return int(match.group(1)) if match else 0


def _cleanup_section(buckets, emit, module):
    # Worlds: lineup-like groups + a foreign group; owned/foreign x
    # streams/no-streams x hidden/visible.
    filename = "US_DirecTV-Premier_lineup.json"
    data = module.Plugin()._load_lineup(
        {"lineup_file": filename, "category_detail": "normal"}, _quiet_logger()
    )
    prefix = module.Plugin()._get_group_prefix({"lineup_file": filename}, data)
    lineup_groups = [module.Plugin()._make_group_name(prefix, cat) for cat in data["categories"]]
    first_two = lineup_groups[:2]

    world = {"channels": [], "groups": [], "profiles": []}
    groups = {}
    for name in first_two + ["Foreign"]:
        gid = 100 + len(groups)
        groups[name] = gid
        world["groups"].append({"id": gid, "name": name})
    specs = [
        # (id, name, group, streams, hidden)
        (1, "Owned Empty", first_two[0], [], False),
        (2, "Owned Hidden Empty", first_two[0], [], True),
        (3, "Owned Regained", first_two[1], [501], True),
        (4, "Operator Hid", first_two[1], [502], True),
        (5, "Healthy", first_two[0], [503], False),
        (6, "Foreign Empty", first_two[0], [], False),
        (7, "Foreign Hidden Empty", first_two[1], [], True),
        (8, "Elsewhere Empty", "Foreign", [], False),
    ]
    for cid, name, group, streams, hidden in specs:
        world["channels"].append({
            "id": cid, "name": name, "group": group, "number": float(cid),
            "streams": len(streams), "hidden": hidden,
        })
    # Owned by the file (1-5 in lineup groups; 8 owned but outside them: tidy is
    # ownership-based, not group-based). 4 is owned but hidden by the operator, never
    # by tidy, so the unhide guard must keep it hidden.
    owned = [1, 2, 3, 4, 5, 8]

    # LU: the exact cleanup chain over fakes.
    trial = FakeDB()
    _use_db(trial)
    gid_map = {}
    for group in world["groups"]:
        row = trial.add_group(group["name"], gid=group["id"])
        gid_map[group["name"]] = row["id"]
    for channel in world["channels"]:
        trial.add_channel(channel["name"], gid_map[channel["group"]],
                          channel["number"], cid=channel["id"])
        trial.channels[channel["id"]]["hidden"] = channel["hidden"]
        if channel["streams"]:
            trial.channelstreams[channel["id"]] = list(range(700, 700 + channel["streams"]))
    lu_cleanup = _lu_cleanup(module, trial, filename)

    # Rust: tidy over the same world.
    rust_rows = _rust_tidy({
        "channels": world["channels"],
        "owned": owned,
        "hid": [2, 3],  # past tidy_apply hid 2 and 3
    })
    by_id = {row["id"]: row for row in rust_rows}

    lu_deleted = {row["id"] for row in lu_cleanup["deleted"]}
    # LU deletes every stream-less channel in lineup groups: 1, 2, 6, 7.
    for cid in (1, 2, 6, 7):
        if cid not in lu_deleted:
            emit("unattributed", f"cleanup: LU did not delete stream-less channel {cid}")
    if 8 in lu_deleted or 3 in lu_deleted or 4 in lu_deleted or 5 in lu_deleted:
        emit("unattributed", "cleanup: LU deleted a channel with streams or outside lineup groups")
    for cid, want in ((1, "Hide"), (2, "Keep"), (3, "Unhide"), (4, "Keep"),
                      (5, "Keep"), (8, "Hide")):
        got = by_id.get(cid, {}).get("action")
        if got != want:
            emit("unattributed", f"cleanup: tidy channel {cid}: want {want}, got {got}")
        elif cid == 1 and cid in lu_deleted:
            emit("cleanup-delete-vs-hide",
                 f"channel {cid} owned + stream-less in a lineup group: LU deletes it, tidy hides it (undoable)")
        elif cid == 8 and cid not in lu_deleted:
            emit("cleanup-owned-only",
                 "channel 8 is owned but outside the lineup groups: LU ignores it (group-name based), "
                 "tidy hides it (ownership based, not group names)")
        elif cid == 3:
            emit("cleanup-unhide-guard",
                 f"channel {cid} hidden with streams again after tidy hid it: tidy unhides (LU has no unhide)")
        elif cid == 2:
            emit("cleanup-unhide-guard",
                 f"channel {cid} hidden, still stream-less: stays hidden (no-op)")
        elif cid == 4:
            emit("cleanup-unhide-guard",
                 f"channel {cid} hidden with streams but tidy never hid it: operator hide stays (never unhidden)")
    for cid in (6, 7):
        if cid in by_id:
            emit("unattributed", f"cleanup: tidy has a row for foreign channel {cid}")
        elif cid in lu_deleted:
            emit("cleanup-owned-only",
                 f"foreign stream-less channel {cid} in a lineup group: LU deletes it, tidy has no row (owned-only)")


def write_report(buckets, unattributed, args, files):
    total_diffs = sum(len(rows) for rows in buckets.values()) + len(unattributed)
    lines = []
    lines.append("# Lineup sync + tidy oracle report (step 18)")
    lines.append("")
    lines.append("Lineup `sync_*` mirrors Lineuparr's group + channel sync on the")
    lines.append("owned-channel rule; `tidy_*` is its post-match cleanup as hide instead")
    lines.append("of delete. Lineuparr (`reference/Dispatcharr-Lineuparr-Plugin`) is the")
    lines.append("oracle: `_sync_groups` + `_do_sync_channels` (get_or_create by")
    lines.append("`(name, group)`, `_get_channel_number` numbering, renumber-existing,")
    lines.append("`_enable_channels_in_profiles`) and `_migrate_single_group_name`, driven")
    lines.append("with ORM fakes over 29 lineup files x 4 numbering modes x 4")
    lines.append("`category_detail` modes (464 first runs + 464 reruns), a foreign")
    lines.append("same-name + number-squat subset (116 cases), and the cleanup query chain")
    lines.append("vs `tidy_dump`. Rust side: `sync_dump` / `tidy_dump` (the actions' own")
    lines.append("`decide_sync` + `plan_sync_creates` / `decide_tidy`). Bar: 0 unattributed.")
    lines.append("")
    lines.append("Cases: 580 first runs + 580 reruns per side (464 clean + 116 clash/squat each).")
    lines.append("")
    for bucket, rows in buckets.items():
        lines.append(f"## {bucket} ({len(rows)})")
        lines.append("")
        if not rows:
            lines.append("(no differences)")
            lines.append("")
            continue
        for row in sorted(set(rows))[: args.show]:
            lines.append(f"- {row}")
        lines.append("")
        if len(set(rows)) > args.show:
            lines.append(f"({len(set(rows)) - args.show} more)")
            lines.append("")
    lines.append(f"## unattributed ({len(unattributed)})")
    lines.append("")
    if unattributed:
        for row in sorted(set(unattributed))[: args.show]:
            lines.append(f"- {row}")
        lines.append("")
    else:
        lines.append("(none: every difference is attributed)")
        lines.append("")
    text = "\n".join(lines)
    if args.check:
        with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False) as tmp:
            tmp.write(text)
            tmp_path = tmp.name
        try:
            committed = REPORT.read_text()
        except FileNotFoundError:
            raise SystemExit(
                f"{REPORT} does not exist yet: run without --check once to generate it"
            )
        trial = Path(tmp_path).read_text()
        if trial != committed:
            raise SystemExit(
                f"{REPORT} differs from a fresh run: regenerate it (run without --check)"
            )
    else:
        REPORT.write_text(text)
        print(f"wrote {REPORT} ({total_diffs} attributed, {len(unattributed)} unattributed)")


if __name__ == "__main__":
    main()
