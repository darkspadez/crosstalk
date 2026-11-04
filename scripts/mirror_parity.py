#!/usr/bin/env python3
"""Parity: crosstalk's `mirror_*` against Stream-Mapparr's own Fill Mirror Backups.

The streams app's mirror fill is the plugin's, ported. This script is how that claim is
checked: it imports the plugin's `plugin.py` from `reference/` with the Django stubs its own
test suite installs (`reference/Stream-Mapparr/tests/conftest.py`), then drives the REAL
`fill_mirror_backups_action` end to end over generated fixture worlds — with `M3UAccount`,
`ChannelStream` and `Stream` faked the way `tests/test_fill_mirror_action.py` fakes them — and
compares what the action wrote with what `cargo run -p crosstalk-streams --example mirror_io`
planned.

Nothing of the Rust is mirrored here: the plugin's own code decides the expected answer.

What runs for real, per case:

  * `host_key` (the pure module's URL reduction) over every `server_url` in the case;
  * `fill_mirror_backups_action` itself: the account table read, the "no server group means
    nothing to do" early return, `_channels_in_scope` (stubbed to the case's channels — the
    scope is the host's shared job and is covered by `channel_ops_parity.py`), the
    `ChannelStream` read, the provider-id `Stream` index (`order_by('id')`, first row wins),
    `plan_mirror_fill` (in the pure module, imported by the action), the `no_source`
    adjustment for channels with no rows at all, the snapshot guard, the `select_for_update`
    write with its "changed meanwhile" skip, and the readout file;
  * `_write_mirror_fill_readout`, whose counts and per-account "added N" lines are parsed back
    here and compared with the Rust plan's own counts.

Two things the plugin does that have no Rust counterpart by design:

  * the snapshot file — crosstalk records each channel's previous order in the run store, so
    there is nothing to compare and the fixture only asserts the write itself;
  * the per-channel `select_for_update` re-read. Crosstalk plans and writes ONE world
    snapshot, so it never notices a channel that moved underneath it. The plugin's re-read
    compares the rows it locked against the rows its `ChannelStream ... stream__` JOIN
    planned from, which means a channel holding a `ChannelStream` row whose `Stream` is gone
    looks "changed meanwhile" to it and is left alone. One fixture has exactly that shape:
    the Rust plan carries such channels in `skipped` with the dry run's `after` (compared
    here), and never writes them (asserted here by their absence from `entries`, and by the
    Rust apply test) — the refusal both sides agree on.

Usage:
    python3 -I scripts/mirror_parity.py [--show N] [--random N] [--seed N]

Exit status is 1 when any plan differs, so it can gate CI.
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
PKG = "stream_mapparr_mirror_parity"


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
    django_db = types.ModuleType("django.db")

    class _Atomic:
        """`transaction.atomic()` as a no-op context manager: the fake tables are not atomic."""

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    django_db.transaction = types.SimpleNamespace(atomic=lambda: _Atomic())
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
    m3u = types.ModuleType("apps.m3u")
    m3u.__path__ = []
    m3u_models = types.ModuleType("apps.m3u.models")
    m3u_models.M3UAccount = MagicMock(name="M3UAccount")
    sys.modules.update({
        "apps": apps,
        "apps.channels": apps_channels,
        "apps.channels.models": models,
        "apps.m3u": m3u,
        "apps.m3u.models": m3u_models,
    })

    core = types.ModuleType("core")
    core.__path__ = []
    core_utils = types.ModuleType("core.utils")
    from unittest.mock import MagicMock as _MagicMock
    core_utils.send_websocket_update = _MagicMock(name="send_websocket_update")
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
    return plugin


# ---------------------------------------------------------------------------------------
# Fake ORM tables, following tests/test_fill_mirror_action.py
# ---------------------------------------------------------------------------------------

class FakeAccount:
    """`M3UAccount.objects.all().values('id', 'name', 'server_group_id', 'is_active',
    'server_url')`."""

    rows = []

    @classmethod
    def select(cls, kw, fields):
        return [{field: row.get(field) for field in fields} for row in cls.rows]


class FakeStream:
    """`Stream.objects.filter(m3u_account_id__in=..., stream_id__in=...).order_by('id')
    .values_list('m3u_account_id', 'stream_id', 'id')`."""

    rows = []  # [(id, account_id, provider_id, name)]

    @classmethod
    def select(cls, kw, fields):
        out = []
        for pk, account, provider, name in sorted(cls.rows):
            if "m3u_account_id__in" in kw and account not in kw["m3u_account_id__in"]:
                continue
            if "stream_id__in" in kw:
                # SQL `IN (NULL, ...)` matches nothing, and the plugin's `wanted` set never
                # holds a None, so this only guards a malformed case.
                if provider is None or provider not in kw["stream_id__in"]:
                    continue
            if "id__in" in kw and pk not in kw["id__in"]:
                continue
            row = {"id": pk, "name": name, "m3u_account_id": account, "stream_id": provider}
            out.append(tuple(row[field] for field in fields))
        return out


class FakeChannelStream:
    """`ChannelStream`: the row constructor plus the two query shapes the action uses.

    `select_for_update` is a no-op (nothing else mutates the store), and a row whose stream is
    not in the stream table is dropped, which is what the plugin's `stream__` join does.
    """

    streams = {}          # channel id -> [stream pk] in order
    changed_under = {}    # channel id -> order swapped in on the locked re-read
    stream_rows = {}      # stream pk -> (account id, provider id)

    def __init__(self, channel_id, stream_id, order):
        self.channel_id, self.stream_id, self.order = channel_id, stream_id, order

    @classmethod
    def select(cls, kw, fields):
        if "channel_id" in kw:
            cid = kw["channel_id"]
            if cid in cls.changed_under:
                cls.streams[cid] = cls.changed_under.pop(cid)
            return [(sid,) for sid in cls.streams.get(cid, [])]
        out = []
        for cid in sorted(kw.get("channel_id__in", cls.streams)):
            for sid in cls.streams.get(cid, []):
                if sid not in cls.stream_rows:
                    # The inner join to Stream drops a dangling row entirely.
                    continue
                account, provider = cls.stream_rows[sid]
                row = {"channel_id": cid, "stream_id": sid,
                       "stream__m3u_account_id": account, "stream__stream_id": provider}
                out.append(tuple(row[field] for field in fields))
        return out

    @classmethod
    def delete(cls, kw):
        cls.streams[kw["channel_id"]] = []

    @classmethod
    def bulk_create(cls, rows):
        for row in sorted(rows, key=lambda row: row.order):
            cls.streams.setdefault(row.channel_id, []).append(row.stream_id)

    @classmethod
    def ordered_ids(cls, channel_id):
        return list(cls.streams.get(channel_id, []))


class FakeQuerySet:
    def __init__(self, model):
        self._model = model
        self._kw = {}

    def filter(self, **kw):
        self._kw.update(kw)
        return self

    def order_by(self, *args):
        return self

    def select_for_update(self):
        return self

    def all(self):
        return self

    def values(self, *fields):
        return self._model.select(self._kw, fields)

    def values_list(self, *fields, flat=False):
        rows = self._model.select(self._kw, fields)
        return [row[0] for row in rows] if flat else rows

    def delete(self):
        self._model.delete(self._kw)

    def bulk_create(self, rows):
        self._model.bulk_create(rows)


class FakeManager:
    """`<Model>.objects`: a FRESH queryset per call, so one filter never leaks into the next."""

    def __init__(self, model):
        self._model = model

    def all(self):
        return FakeQuerySet(self._model)

    def filter(self, **kw):
        return FakeQuerySet(self._model).filter(**kw)

    def select_for_update(self):
        return FakeQuerySet(self._model)

    def bulk_create(self, rows):
        return self._model.bulk_create(rows)

    def delete(self):
        return self._model.delete({})


# The managers the action reaches through the module-level model names.
FakeAccount.objects = FakeManager(FakeAccount)
FakeStream.objects = FakeManager(FakeStream)
FakeChannelStream.objects = FakeManager(FakeChannelStream)


# ---------------------------------------------------------------------------------------
# The plugin's own answer for one case
# ---------------------------------------------------------------------------------------

def plugin_answer(plugin, case: dict, workdir: Path) -> dict:
    """Run the plugin's real action over the case; return the plan and what it wrote."""
    live = _run_action(plugin, case, workdir, dry_run=False)
    answer = {
        "after": live["after"],
        "message": live["message"],
        "readout": live["readout"],
        "skipped": live["skipped"],
    }
    if live["skipped"]:
        # The plugin meant to write those channels and refused at the locked re-read, because
        # the rows it read back are not the rows its join planned from (a ChannelStream row
        # whose Stream no longer exists is exactly that). Its PLAN is still the dry run's, so
        # that is what the Rust side is compared with for those channels.
        dry = _run_action(plugin, case, workdir, dry_run=True)
        answer["plan_readout"] = dry["readout"]
    return answer


def _run_action(plugin, case: dict, workdir: Path, dry_run: bool) -> dict:
    logger = logging.getLogger("mirror_parity")

    FakeAccount.rows = [
        {"id": account["id"], "name": account["name"],
         "server_group_id": account.get("group"), "is_active": account.get("active", True),
         "server_url": account.get("url")}
        for account in case["accounts"]
    ]
    FakeStream.rows = [
        (stream["id"], stream.get("account"), stream.get("provider_id"), stream["name"])
        for stream in case["streams"]
    ]
    FakeChannelStream.streams = {
        channel["id"]: list(channel["streams"]) for channel in case["channels"]
    }
    FakeChannelStream.changed_under = {}
    FakeChannelStream.stream_rows = {
        stream["id"]: (stream.get("account"), stream.get("provider_id"))
        for stream in case["streams"]
    }

    plugin.ChannelStream = FakeChannelStream
    plugin.Stream = FakeStream
    sys.modules["apps.m3u.models"].M3UAccount = FakeAccount
    plugin.PluginConfig.CHANNEL_SNAPSHOT_DIR = str(workdir / "snapshots")

    P = plugin.Plugin
    inst = P.__new__(P)
    inst.version = "test"
    inst.BUG_REPORT_DIR = str(workdir / "readout")
    # The scope half of the action: profiles and groups are the host's shared job, and the
    # case already says which channels a run would see.
    channels = [
        {"id": channel["id"], "name": channel["name"],
         "channel_number": channel.get("number", 999999)}
        for channel in case["channels"]
    ]
    inst._channels_in_scope = lambda settings, logger: (channels, None)
    inst._trigger_frontend_refresh = lambda settings, logger: True

    result = inst.fill_mirror_backups_action({"dry_run_mode": dry_run}, logger)
    if result.get("status") != "success":
        raise SystemExit(f"the plugin's mirror action failed: {result}")

    after = {channel["id"]: FakeChannelStream.ordered_ids(channel["id"])
             for channel in case["channels"]}
    skipped = []
    if not dry_run:
        match = re.search(r"(\d+) channel\(s\) changed while this ran", result["message"])
        if match and int(match.group(1)):
            skipped = [channel["id"] for channel in case["channels"]
                       if FakeChannelStream.ordered_ids(channel["id"]) == channel["streams"]]
    readout = {}
    if result.get("file"):
        readout = _parse_readout(Path(result["file"]))
    return {"after": after, "message": result["message"], "readout": readout,
            "skipped": skipped}


_COUNT_LINES = {
    "Channels in scope": "channels",
    "Channels that gain a mirror": "gained",
    "Mirror streams planned": "mirror_streams",
    "Already have every active host": "covered",
    "No server-group stream to copy": "no_source",
    "Host missing, mirror not there": "no_mirror_found",
}

_ACCOUNT_LINE = re.compile(r"^  (?P<name>.+?)  group (?P<group>\S+)  "
                           r"(?P<state>active|INACTIVE)  added (?P<added>\d+)$")


def _parse_readout(path: Path) -> dict:
    """The plugin's readout file: the counts it states, its per-account tallies, and the
    `Additions, by channel` block (channel name -> [(account, stream name)])."""
    counts = {}
    per_account = {}
    additions = {}
    current = None
    for line in path.read_text(encoding="utf-8").splitlines():
        for label, key in _COUNT_LINES.items():
            if line.startswith(label):
                counts[key] = int(line.split(":")[-1].strip())
        match = _ACCOUNT_LINE.match(line)
        if match:
            per_account[match.group("name")] = int(match.group("added"))
            continue
        if line.startswith("      + "):
            if current is not None:
                account_name, _, stream_name = line[8:].partition(": ")
                additions.setdefault(current, []).append((account_name, stream_name))
            continue
        if line.startswith("  ") and not line.startswith("      ") and line.strip():
            # `  {channel_number}  {channel_name}`: the channel the additions below belong to.
            parts = line[2:].split("  ", 1)
            current = parts[1] if len(parts) == 2 else parts[0]
    return {"counts": counts, "per_account": per_account, "additions": additions}


# ---------------------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------------------

HOSTS = ["http://a.example/x", "http://b.example/x", "http://a.example:8080/x",
         "https://user:pw@c.example/x", None, "", "not a url"]


def account(account_id, name, group, active=True, url=None):
    return {"id": account_id, "name": name, "group": group, "active": active, "url": url}


def stream(stream_id, name, account_id, provider_id):
    return {"id": stream_id, "name": name, "account": account_id,
            "provider_id": provider_id}


def channel(channel_id, name, streams, group=100, number=None):
    return {"id": channel_id, "name": name, "group": group,
            "number": channel_id if number is None else number, "streams": streams}


GROUPS = [{"id": 100, "name": "News"}, {"id": 101, "name": "Sports"}]


def base_accounts():
    """The shapes the planner's own tests pin, plus a provider host on two accounts."""
    return [
        account(1, "custom", None),
        account(6, "acct6", 1, url="http://a.example:80/x"),
        account(7, "acct7", 1, url="http://b.example:80/x"),
        account(8, "acct8", 1, url="http://a.example:80/y"),
        account(9, "acct9", 1, active=False, url="http://c.example:80/x"),
    ]


def base_streams():
    return [
        stream(10, "CNN HD", 6, 555),
        stream(20, "CNN HD (B)", 7, 555),
        stream(30, "CNN HD (A backup)", 8, 555),
        stream(40, "CNN HD (C)", 9, 555),
        stream(11, "ESPN", 6, 777),
        stream(21, "ESPN (B)", 7, 777),
        stream(99, "Could Not Dispatch", 1, None),
    ]


def worlds():
    """The fixture worlds, one per shape the planner has a branch for."""
    cases = []

    def case(comment, channels, accounts=None, streams=None, **extra):
        payload = {
            "comment": comment,
            "accounts": accounts if accounts is not None else base_accounts(),
            "streams": streams if streams is not None else base_streams(),
            "channels": channels,
            "groups": GROUPS,
        }
        payload.update(extra)
        cases.append(payload)

    # The three rows the plugin's own action test asserts, end to end.
    case("the action's own three channels", [
        channel(100, "CNN", [10, 99]),
        channel(200, "ESPN", [11, 21, 99]),
        channel(300, "KFDA", [40]),
    ])
    # Mirrors on inactive accounts: a stream from one is not a backup, and one is never added.
    case("a stream from an inactive account names the mirror but is not one", [
        channel(100, "CNN", [40]),
    ])
    case("an inactive account's stream does not count as a host", [
        channel(100, "CNN", [10, 40]),
    ])
    # A host the channel already has through a second account on the same URL host.
    case("two accounts on one host add one mirror", [
        channel(100, "CNN", [10]),
    ], streams=base_streams() + [stream(31, "CNN HD (A again)", 8, 555)])
    # Missing host: an account in the group with no server_url.
    case("an account with no host is never added from", [
        channel(100, "CNN", [10]),
    ], accounts=base_accounts() + [account(12, "acct12", 1, url=None)],
        streams=base_streams() + [stream(50, "CNN HD (no host)", 12, 555)])
    # The trailing stream with no host stays last; a custom stream in the middle does not move.
    case("custom streams interleaved", [
        channel(100, "CNN", [99, 10, 11]),
        channel(200, "ESPN", [10, 99, 30]),
    ])
    # No provider ids: nothing to look a mirror up by.
    case("channels with no provider ids", [
        channel(100, "CNN", [99]),
        channel(200, "ESPN", []),
    ])
    # One account serving two streams for the same provider id: the lowest stream id wins.
    case("a duplicate provider id on one account", [
        channel(100, "CNN", [10]),
    ], streams=base_streams() + [stream(5, "CNN HD (dupe)", 7, 555)])
    # Two server groups: only the source's own group is consulted.
    case("two server groups", [
        channel(100, "CNN", [10]),
    ], accounts=base_accounts() + [account(13, "other", 2, url="http://d.example/x")],
        streams=base_streams() + [stream(60, "CNN HD (D)", 13, 555)])
    # A source account outside every server group.
    case("a source outside every server group", [
        channel(100, "CNN", [70]),
    ], accounts=base_accounts() + [account(14, "loose", None, url="http://e.example/x")],
        streams=base_streams() + [stream(70, "CNN HD (loose)", 14, 555)])
    # Ports: 80 and 8080 on one hostname are different servers.
    case("a different port is a different server", [
        channel(100, "CNN", [10]),
    ], accounts=[account(6, "acct6", 1, url="http://a.example:80/x"),
                 account(7, "acct7", 1, url="http://a.example:8080/x")],
        streams=[stream(10, "CNN HD", 6, 555), stream(20, "CNN HD (8080)", 7, 555)])
    # Credentials in the URL are dropped, so they are the same host.
    case("credentials do not make a new host", [
        channel(100, "CNN", [10]),
    ], accounts=[account(6, "acct6", 1, url="http://a.example/x"),
                 account(7, "acct7", 1, url="https://user:pw@a.example/y")],
        streams=[stream(10, "CNN HD", 6, 555), stream(20, "CNN HD (pw)", 7, 555)])
    # Every host covered.
    case("covered on every active host", [
        channel(100, "CNN", [10, 20, 99]),
    ])
    # A host missing with no mirror on it.
    case("a host missing with no mirror there", [
        channel(100, "CNN", [11]),
    ], streams=[stream(11, "ESPN", 6, 777)])
    # No account in a server group at all: the early return.
    case("no server group at all", [
        channel(100, "CNN", [10]),
    ], accounts=[account(1, "custom", None), account(6, "acct6", None, url="http://a.example/x")])
    # A channel-stream row whose stream is gone: the plugin's join drops it, and the rewrite
    # that follows deletes it.
    case("a dangling channel-stream row", [
        channel(100, "CNN", [10, 98]),
    ])
    # A channel with no streams at all, counted as no_source.
    case("a channel with no streams", [
        channel(100, "CNN", [10]),
        channel(200, "Empty", []),
    ])
    # Three hosts in one group, one of them inactive.
    case("three hosts, one inactive", [
        channel(100, "CNN", [10]),
    ], accounts=[account(6, "acct6", 1, url="http://a.example/x"),
                 account(7, "acct7", 1, url="http://b.example/x"),
                 account(8, "acct8", 1, active=False, url="http://c.example/x")],
        streams=[stream(10, "CNN HD", 6, 555), stream(20, "CNN HD (B)", 7, 555),
                 stream(30, "CNN HD (C)", 8, 555)])
    # A stream whose account is unknown to the account table.
    case("a stream whose account is unknown", [
        channel(100, "CNN", [10, 97]),
    ], streams=base_streams() + [stream(97, "Mystery", 55, 555)])
    # The same provider id held twice on the channel: nothing is added twice.
    case("a mirror already on the channel", [
        channel(100, "CNN", [10, 20]),
    ])
    return cases


def random_case(rng, index):
    """A randomly shaped world, thrown together instead of gridded."""
    accounts = [account(1, "custom", None)]
    grouped = rng.randint(0, 4)
    for offset in range(grouped):
        accounts.append(account(
            6 + offset, f"acct{6 + offset}",
            rng.choice([1, 1, 2, None]),
            active=rng.random() < 0.8,
            url=rng.choice(HOSTS)))
    streams = []
    providers = [500, 501, 502]
    for offset, account_row in enumerate(accounts):
        for provider in providers:
            if rng.random() < 0.55:
                streams.append(stream(
                    10 + offset * 10 + providers.index(provider),
                    f"S{provider} from {account_row['name']}",
                    account_row["id"], provider))
    custom = stream(999, "Could Not Dispatch", 1, None)
    streams.append(custom)
    channels = []
    for channel_index in range(rng.randint(1, 4)):
        pool = [row["id"] for row in streams]
        picked = rng.sample(pool, rng.randint(0, min(4, len(pool))))
        if rng.random() < 0.3:
            picked.insert(rng.randint(0, len(picked)), custom["id"])
        if rng.random() < 0.15:
            picked.append(9000 + channel_index)   # a dangling id
        channels.append(channel(
            100 + channel_index, f"Channel {channel_index}", picked,
            group=rng.choice([100, 101]), number=rng.choice([None, 1, 2, 5])))
    return {
        "comment": f"random #{index}",
        "accounts": accounts,
        "streams": streams,
        "channels": channels,
        "groups": GROUPS,
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


def _stream_names(case):
    return {row["id"]: row["name"] for row in case["streams"]}


def _account_names(case):
    return {row["id"]: row["name"] for row in case["accounts"]}


# ---------------------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--show", type=int, default=8, help="mismatches to print")
    parser.add_argument("--random", type=int, default=120, help="randomised cases to add")
    parser.add_argument("--seed", type=int, default=20261103, help="seed for random cases")
    parser.add_argument("--binary",
                        default=str(REPO / "target" / "debug" / "examples" / "mirror_io"),
                        help="path to the mirror_io example binary")
    args = parser.parse_args()

    if not PLUGIN_INNER.is_dir():
        print(f"reference plugin not found at {PLUGIN_INNER}; run `git submodule update --init`",
              file=sys.stderr)
        return 2

    print(f"building {args.binary} ...", file=sys.stderr)
    subprocess.run(["cargo", "build", "-q", "-p", "crosstalk-streams", "--example", "mirror_io"],
                   cwd=REPO, check=True)

    plugin = load_plugin()
    # The plugin's own answer compares against the Rust answer, which reads no settings beyond
    # the scope; both sides get the same empty scope profile filter (the case's whole channel
    # list).
    rng = random.Random(args.seed)
    cases = worlds() + [random_case(rng, index) for index in range(args.random)]
    for case in cases:
        case.setdefault("profiles", "")
        case.setdefault("scope_groups", "")
        case.setdefault("ignore_groups", "")
    answers = run_rust(cases, args.binary)

    mismatches = 0
    compared = 0
    added_total = 0
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        for case, answer in zip(cases, answers):
            expected = plugin_answer(plugin, case, workdir)
            got_entries = {row["channel_id"]: row for row in answer["entries"]}
            # Planned but left alone: the plugin's dry run plans these channels (its join
            # drops the dangling row) and its write refuses them (the locked re-read sees
            # the dangling row). The Rust plan carries them in `skipped` with the dry run's
            # `after`; they must never be written, which is exactly what the plugin did.
            got_skipped_ids = {row["channel_id"] for row in answer.get("skipped") or []}
            got_entries.update(
                {row["channel_id"]: row for row in answer.get("skipped") or []}
            )
            names = _stream_names(case)
            accounts = _account_names(case)
            skipped = set(expected.get("skipped") or [])
            plan_additions = (expected.get("plan_readout") or {}).get("additions") or {}

            # 1. What the plugin WROTE, per channel: the fake ChannelStream table after the
            # action is the action's own `delete()` + `bulk_create()`, so it is the plugin's
            # answer for the order, not a reimplementation of the planner.
            for channel_row in case["channels"]:
                compared += 1
                channel_id = channel_row["id"]
                original = channel_row["streams"]
                wrote = expected["after"].get(channel_id, original)
                got = got_entries.get(channel_id)
                if wrote != original:
                    if got is None:
                        mismatches += 1
                        if mismatches <= args.show:
                            print(f"[{case['comment']}] channel {channel_id}: the plugin "
                                  f"wrote {wrote} but the Rust plan leaves it alone")
                        continue
                    added_total += len(got["added"])
                    if wrote != got["after"]:
                        mismatches += 1
                        if mismatches <= args.show:
                            print(f"[{case['comment']}] channel {channel_id} order")
                            print(f"  accounts={case['accounts']}")
                            print(f"  streams={case['streams']}")
                            print(f"  python={wrote} (before {original})")
                            print(f"  rust  ={got['after']} (before {got['before']})")
                        continue
                    if got["before"] != original:
                        mismatches += 1
                        if mismatches <= args.show:
                            print(f"[{case['comment']}] channel {channel_id}: before "
                                  f"python={original} rust={got['before']}")
                    # The additions, by stream and by ACCOUNT NAME: the readout's own wording.
                    want_added = [(accounts[stream_row["account"]],
                                   names[stream_row["id"]])
                                  for stream_row in case["streams"]
                                  if stream_row["id"] in wrote
                                  and stream_row["id"] not in original]
                    got_added = [(accounts[row["account_id"]], names[row["stream_id"]])
                                 for row in got["added"]]
                    if want_added != got_added:
                        mismatches += 1
                        if mismatches <= args.show:
                            print(f"[{case['comment']}] channel {channel_id} additions: "
                                  f"python={want_added} rust={got_added}")
                elif got is not None and channel_id in skipped:
                    # The plugin PLANNED this channel and refused at the locked re-read (see
                    # `plugin_answer`); its dry-run readout carries the plan it refused, and
                    # that is what the Rust plan must equal.
                    want_added = plan_additions.get(channel_row["name"])
                    got_added = [(accounts[row["account_id"]], names[row["stream_id"]])
                                 for row in got["added"]]
                    if want_added != got_added:
                        mismatches += 1
                        if mismatches <= args.show:
                            print(f"[{case['comment']}] channel {channel_id}: the plugin's "
                                  f"dry run planned {want_added}, rust={got_added}")
                    # ... and the Rust side must refuse it too: a refused channel in
                    # `entries` would be WRITTEN, which is the one thing the plugin did not
                    # do. (`skipped` is every unchanged channel here, so only channels the
                    # dry run planned are proof of a refusal.)
                    if want_added and channel_id not in got_skipped_ids:
                        mismatches += 1
                        if mismatches <= args.show:
                            print(f"[{case['comment']}] channel {channel_id}: the plugin "
                                  f"refused this channel at the locked re-read but the Rust "
                                  f"plan would write it")
                elif got is not None:
                    mismatches += 1
                    if mismatches <= args.show:
                        print(f"[{case['comment']}] channel {channel_id}: the Rust plan "
                              f"adds {[row['stream_id'] for row in got['added']]} but the "
                              f"plugin wrote nothing")

            # 2. The readout's counts, which are the plugin's own whole-run tally. The plan
            # counts every planned channel — including the ones the write refuses — so the
            # Rust `skipped` entries count here, exactly as they do in the readout.
            readout = expected["readout"].get("counts") or {}
            stats = answer["stats"]
            planned = list(answer["entries"]) + list(answer.get("skipped") or [])
            mine_counts = {
                "channels": answer["channels"],
                "gained": stats["gained"],
                "mirror_streams": sum(len(row["added"]) for row in planned),
                "covered": stats["covered"],
                "no_source": stats["no_source"],
                "no_mirror_found": stats["no_mirror_found"],
            }
            for key, want in readout.items():
                if mine_counts.get(key) != want:
                    mismatches += 1
                    if mismatches <= args.show:
                        print(f"[{case['comment']}] readout count {key}: "
                              f"python={want} rust={mine_counts.get(key)}")
            # 3. Per-account additions, as the readout states them.
            want_per_account = {name: count for name, count in
                                (expected["readout"].get("per_account") or {}).items()
                                if count}
            mine_per_account = {accounts[account_id]: count
                                for account_id, count in answer["per_account"]}
            if want_per_account != mine_per_account:
                mismatches += 1
                if mismatches <= args.show:
                    print(f"[{case['comment']}] per-account additions: "
                          f"python={want_per_account} rust={mine_per_account}")
            # 4. "No account is in a server group" is the plugin's early return.
            any_grouped = any(row.get("group") is not None for row in case["accounts"])
            if any_grouped != answer["any_grouped_account"]:
                mismatches += 1
                if mismatches <= args.show:
                    print(f"[{case['comment']}] grouped-account verdict: "
                          f"python={any_grouped} rust={answer['any_grouped_account']}")

    print(f"[mirror] {mismatches} mismatch(es) over {len(cases)} case(s), "
          f"{compared} channel(s) compared, {added_total} mirror stream(s) added")
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
