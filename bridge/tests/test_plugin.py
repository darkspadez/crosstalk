"""Tests for the crosstalk bridge plugin, without Django.

Run with ``python3 -I -m pytest bridge/tests`` or ``python3 -I -m unittest discover -s
bridge/tests``. Django and Dispatcharr's models are replaced by a small in-memory fake installed
into ``sys.modules`` (the way scripts/ stubs the reference plugins), so the plugin's real ORM
code runs against tables these tests can read back. ``vectors.json`` next to this file is the
cross-language contract: crosstalk's Rust tests check the same file.
"""

import copy
import importlib.util
import itertools
import json
import logging
import os
import sys
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
PLUGIN_PATH = os.path.join(os.path.dirname(HERE), "plugin.py")
VECTORS_PATH = os.path.join(HERE, "vectors.json")


def _load_plugin():
    spec = importlib.util.spec_from_file_location("crosstalk_bridge_plugin", PLUGIN_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bridge = _load_plugin()
# The refusals log warnings by design; keep them off the test output.
logging.getLogger("plugins.crosstalk_bridge").addHandler(logging.NullHandler())

SECRET = "0123456789abcdef" * 4
OLD_SECRET = "fedcba9876543210" * 4
NOW = 1_800_000_000


# ---------------------------------------------------------------------------------------------
# A fake ORM: just the calls plugin.py makes.
# ---------------------------------------------------------------------------------------------


class IntegrityError(Exception):
    pass


TABLES = {}


class QuerySet:
    def __init__(self, model, ids=None):
        self.model = model
        self._ids = ids

    def _table(self):
        return TABLES.setdefault(self.model.__name__, {})

    def _rows(self):
        table = self._table()
        ids = sorted(table) if self._ids is None else [i for i in self._ids if i in table]
        return [table[i] for i in ids]

    def filter(self, **lookups):
        keep = []
        for row in self._rows():
            if all(_matches(row, key, value) for key, value in lookups.items()):
                keep.append(row.id)
        return QuerySet(self.model, keep)

    def only(self, *fields):
        return self

    def __iter__(self):
        return iter([copy.copy(row) for row in self._rows()])

    def values_list(self, *fields, flat=False):
        rows = [tuple(getattr(row, field) for field in fields) for row in self._rows()]
        return [row[0] for row in rows] if flat else rows

    def exists(self):
        return bool(self._rows())

    def update(self, **values):
        rows = self._rows()
        for row in rows:
            for key, value in values.items():
                setattr(row, self.model.ALIASES.get(key, key), value)
        return len(rows)

    def delete(self):
        table = self._table()
        rows = self._rows()
        for row in rows:
            del table[row.id]
        return len(rows), {}

    def bulk_update(self, objects, fields, batch_size=None):
        table = self._table()
        for obj in objects:
            for field in fields:
                setattr(table[obj.id], field, copy.deepcopy(getattr(obj, field)))

    def bulk_create(self, objects, batch_size=None):
        for obj in objects:
            self._insert(obj)
        return objects

    def create(self, **values):
        return self._insert(self.model(**values))

    def _insert(self, obj):
        table = self._table()
        unique = self.model.UNIQUE
        if unique:
            key = tuple(getattr(obj, field) for field in unique)
            for row in table.values():
                if tuple(getattr(row, field) for field in unique) == key:
                    raise IntegrityError("duplicate %s" % (key,))
        obj.id = max(table, default=0) + 1
        table[obj.id] = copy.copy(obj)
        return obj


def _matches(row, key, value):
    field, _, lookup = key.partition("__")
    actual = getattr(row, field)
    if lookup == "":
        return actual == value
    if lookup == "in":
        return actual in set(value)
    if lookup == "gte":
        return actual >= value
    if lookup == "lt":
        return actual < value
    raise AssertionError("the fake ORM has no lookup %r" % lookup)


class Manager:
    def __get__(self, instance, owner):
        return QuerySet(owner)


class Model:
    FIELDS = {}
    UNIQUE = ()
    ALIASES = {}
    objects = Manager()

    def __init__(self, **values):
        self.id = values.pop("id", None)
        for field, default in self.FIELDS.items():
            setattr(self, field, values.pop(field, copy.deepcopy(default)))
        assert not values, "unknown fields %s" % sorted(values)


class Channel(Model):
    FIELDS = {"name": "", "channel_group_id": None}
    ALIASES = {"channel_group": "channel_group_id"}


class Stream(Model):
    FIELDS = {"name": "", "stream_stats": None, "stream_stats_updated_at": None}


class ChannelStream(Model):
    FIELDS = {"channel_id": None, "stream_id": None, "order": 0}
    UNIQUE = ("channel_id", "stream_id")


class EPGSource(Model):
    FIELDS = {"name": ""}


class EPGData(Model):
    FIELDS = {"tvg_id": None, "name": "", "icon_url": None, "epg_source_id": None}
    UNIQUE = ("tvg_id", "epg_source_id")


class ProgramData(Model):
    FIELDS = {
        "epg_id": None,
        "start_time": None,
        "end_time": None,
        "title": "",
        "sub_title": None,
        "description": None,
        "tvg_id": None,
        "custom_properties": None,
    }


class Atomic:
    def __enter__(self):
        self.snapshot = copy.deepcopy(TABLES)
        return self

    def __exit__(self, kind, error, trace):
        if kind is not None:
            TABLES.clear()
            TABLES.update(self.snapshot)
        return False


ROLLUPS = []


def _install_fakes():
    def module(name, **attributes):
        made = types.ModuleType(name)
        made.__dict__.update(attributes)
        sys.modules[name] = made
        return made

    transaction = types.SimpleNamespace(atomic=Atomic)
    module("django")
    module("django.db", transaction=transaction)
    module("apps")
    module("apps.channels")
    module("apps.channels.models", Channel=Channel, Stream=Stream, ChannelStream=ChannelStream)
    module("apps.channels.utils", rollup_catchup_for_channels=ROLLUPS.append)
    module("apps.epg")
    module("apps.epg.models", EPGSource=EPGSource, EPGData=EPGData, ProgramData=ProgramData)


_install_fakes()


def seed():
    TABLES.clear()
    del ROLLUPS[:]
    for channel_id, group in ((1, 10), (2, 10), (3, None)):
        TABLES.setdefault("Channel", {})[channel_id] = Channel(
            id=channel_id, name="ch%d" % channel_id, channel_group_id=group
        )
    for stream_id in (11, 12, 13, 14):
        TABLES.setdefault("Stream", {})[stream_id] = Stream(id=stream_id, name="s%d" % stream_id)
    for row_id, (channel_id, stream_id, order) in enumerate(((1, 11, 0), (1, 12, 1)), start=1):
        TABLES.setdefault("ChannelStream", {})[row_id] = ChannelStream(
            id=row_id, channel_id=channel_id, stream_id=stream_id, order=order
        )
    TABLES.setdefault("EPGSource", {})[5] = EPGSource(id=5, name="generated")
    TABLES.setdefault("EPGSource", {})[6] = EPGSource(id=6, name="xmltv")


def stream_list(channel_id):
    rows = [row for row in TABLES.get("ChannelStream", {}).values() if row.channel_id == channel_id]
    return [row.stream_id for row in sorted(rows, key=lambda row: row.order)]


# ---------------------------------------------------------------------------------------------
# Helpers to sign requests the way crosstalk does.
# ---------------------------------------------------------------------------------------------


NONCES = itertools.count(1)


class Signer:
    def __init__(self, secret=SECRET):
        self.secret = secret

    def params(self, op, payload, ts=NOW, nonce=None):
        envelope = {
            "v": 1,
            "ts": ts,
            "nonce": nonce or "%032x" % next(NONCES),
            "op": op,
            "payload": payload,
        }
        return {"envelope": envelope, "mac": bridge.sign(self.secret, envelope)}


def settings(**extra):
    values = {"secret": SECRET, "previous_secret": "", "grace_until": "", "epg_sources": "5"}
    values.update(extra)
    return {"settings": values}


BRIDGE_VARIABLES = (bridge.ENV_SECRET, bridge.ENV_PREVIOUS_SECRET, bridge.ENV_GRACE_UNTIL)


class BridgeTest(unittest.TestCase):
    def setUp(self):
        # Whatever the machine running the tests has set, the tests start without it.
        environment = mock.patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        for variable in BRIDGE_VARIABLES:
            os.environ.pop(variable, None)
        seed()
        self.plugin = bridge.Plugin(clock=lambda: NOW)
        self.signer = Signer()

    def run_op(self, op, payload, context=None, **kwargs):
        return self.plugin.run(op, self.signer.params(op, payload, **kwargs), context or settings())

    def assertRefused(self, answer, code):
        self.assertFalse(answer["ok"], answer)
        self.assertEqual(answer["applied"], 0)
        self.assertEqual(len(answer["errors"]), 1, answer)
        self.assertIsNone(answer["errors"][0]["id"])
        self.assertTrue(answer["errors"][0]["reason"].startswith(code), answer)


class Verification(BridgeTest):
    def test_a_good_mac_is_accepted_and_ping_reports_the_contract(self):
        answer = self.run_op("ping", {})
        self.assertTrue(answer["ok"], answer)
        self.assertEqual(answer["version"], bridge.VERSION)
        self.assertEqual(answer["protocol"], 1)
        self.assertEqual(answer["capabilities"], list(bridge.OPS))
        self.assertEqual(answer["key"], "current")
        self.assertEqual(answer["configured"], "settings")
        self.assertEqual(answer["epg_sources"], [5])
        self.assertEqual(answer["limits"]["max_channels"], 2000)

    def test_a_bad_mac_is_refused(self):
        params = self.signer.params("ping", {})
        params["mac"] = "0" * 64
        self.assertRefused(self.plugin.run("ping", params, settings()), "bad_mac")

    def test_a_request_signed_with_another_secret_is_refused(self):
        params = Signer("x" * 64).params("clear_channel_groups", {"channel_ids": [1]})
        answer = self.plugin.run("clear_channel_groups", params, settings())
        self.assertRefused(answer, "bad_mac")
        self.assertEqual(TABLES["Channel"][1].channel_group_id, 10)

    def test_a_tampered_payload_is_refused(self):
        params = self.signer.params("clear_channel_groups", {"channel_ids": [1]})
        params["envelope"]["payload"]["channel_ids"] = [1, 2]
        self.assertRefused(self.plugin.run("clear_channel_groups", params, settings()), "bad_mac")
        self.assertEqual(TABLES["Channel"][2].channel_group_id, 10)

    def test_an_unconfigured_or_short_secret_refuses_everything(self):
        for secret in ("", "short"):
            answer = self.run_op("ping", {}, context=settings(secret=secret))
            self.assertRefused(answer, "not_configured")

    def test_an_expired_or_future_timestamp_is_refused(self):
        for ts in (NOW - 301, NOW + 301):
            self.assertRefused(self.run_op("ping", {}, ts=ts), "stale")
        self.assertTrue(self.run_op("ping", {}, ts=NOW - 300)["ok"])
        self.assertTrue(self.run_op("ping", {}, ts=NOW + 300)["ok"])

    def test_a_replayed_nonce_is_refused(self):
        params = self.signer.params("clear_channel_groups", {"channel_ids": [1]})
        first = self.plugin.run("clear_channel_groups", params, settings())
        self.assertTrue(first["ok"])
        TABLES["Channel"][1].channel_group_id = 10
        again = self.plugin.run("clear_channel_groups", params, settings())
        self.assertRefused(again, "replayed")
        self.assertEqual(TABLES["Channel"][1].channel_group_id, 10)

    def test_an_unsigned_flood_does_not_burn_nonces(self):
        params = self.signer.params("ping", {}, nonce="ab" * 16)
        forged = copy.deepcopy(params)
        forged["mac"] = "1" * 64
        self.assertRefused(self.plugin.run("ping", forged, settings()), "bad_mac")
        self.assertTrue(self.plugin.run("ping", params, settings())["ok"])

    def test_an_unknown_op_is_refused_even_when_signed(self):
        answer = self.run_op("delete_channels", {"channel_ids": [1]})
        self.assertRefused(answer, "unknown_op")
        self.assertIsNone(answer["op"])
        self.assertIn(1, TABLES["Channel"])

    def test_the_action_must_be_the_signed_op(self):
        params = self.signer.params("ping", {})
        self.assertRefused(
            self.plugin.run("clear_channel_groups", params, settings()), "bad_request"
        )

    def test_malformed_params_are_refused(self):
        good = self.signer.params("ping", {})
        cases = [
            {},
            {"envelope": good["envelope"]},
            dict(good, extra=1),
            dict(good, mac=good["mac"].upper()),
            {"envelope": dict(good["envelope"], extra=1), "mac": good["mac"]},
            "not an object",
        ]
        for params in cases:
            self.assertRefused(self.plugin.run("ping", params, settings()), "bad_request")

    def test_a_signed_envelope_with_a_bad_nonce_or_version_is_refused(self):
        for field, value in (("nonce", "XYZ"), ("v", 2), ("ts", "1800000000")):
            envelope = {"v": 1, "ts": NOW, "nonce": "c" * 32, "op": "ping", "payload": {}}
            envelope[field] = value
            params = {"envelope": envelope, "mac": bridge.sign(SECRET, envelope)}
            self.assertRefused(self.plugin.run("ping", params, settings()), "bad_request")

    def test_rotation_grace_accepts_the_previous_secret_until_its_deadline(self):
        old = Signer(OLD_SECRET)
        during = settings(previous_secret=OLD_SECRET, grace_until=str(NOW + 60))
        answer = self.plugin.run("ping", old.params("ping", {}), during)
        self.assertTrue(answer["ok"], answer)
        self.assertEqual(answer["key"], "previous")
        self.assertEqual(answer["grace"], "active")
        # The new secret works during the grace too.
        self.assertEqual(self.plugin.run("ping", self.signer.params("ping", {}), during)["key"],
                         "current")
        iso = settings(previous_secret=OLD_SECRET, grace_until="2027-02-01T00:00:00Z")
        self.assertTrue(self.plugin.run("ping", old.params("ping", {}), iso)["ok"])

        for after in (
            settings(previous_secret=OLD_SECRET, grace_until=str(NOW - 1)),
            settings(previous_secret=OLD_SECRET, grace_until=""),
            settings(previous_secret=OLD_SECRET, grace_until="next tuesday"),
        ):
            self.assertRefused(self.plugin.run("ping", old.params("ping", {}), after), "bad_mac")

    def test_the_environment_secret_wins_over_the_setting(self):
        os.environ[bridge.ENV_SECRET] = SECRET
        stale = settings(secret=OLD_SECRET)
        answer = self.plugin.run("ping", self.signer.params("ping", {}), stale)
        self.assertTrue(answer["ok"], answer)
        self.assertEqual(answer["configured"], "env")
        # The setting is not a second key: it is ignored while the variable is set.
        refused = self.plugin.run("ping", Signer(OLD_SECRET).params("ping", {}), stale)
        self.assertRefused(refused, "bad_mac")

    def test_a_blank_environment_variable_falls_back_to_the_setting(self):
        os.environ[bridge.ENV_SECRET] = "   "
        answer = self.run_op("ping", {})
        self.assertTrue(answer["ok"], answer)
        self.assertEqual(answer["configured"], "settings")

    def test_a_short_environment_secret_refuses_rather_than_falling_back(self):
        os.environ[bridge.ENV_SECRET] = "short"
        answer = self.run_op("ping", {})
        self.assertRefused(answer, "not_configured")
        self.assertIn("configured: env", answer["errors"][0]["reason"])
        del os.environ[bridge.ENV_SECRET]
        unset = self.run_op("ping", {}, context=settings(secret=""))
        self.assertIn("configured: none", unset["errors"][0]["reason"])

    def test_rotation_grace_from_the_environment(self):
        os.environ[bridge.ENV_SECRET] = SECRET
        os.environ[bridge.ENV_PREVIOUS_SECRET] = OLD_SECRET
        os.environ[bridge.ENV_GRACE_UNTIL] = str(NOW + 60)
        old = Signer(OLD_SECRET)
        answer = self.plugin.run("ping", old.params("ping", {}), settings(secret=""))
        self.assertTrue(answer["ok"], answer)
        self.assertEqual(answer["key"], "previous")
        self.assertEqual(answer["grace"], "active")
        os.environ[bridge.ENV_GRACE_UNTIL] = str(NOW - 60)
        expired = self.plugin.run("ping", Signer(OLD_SECRET).params("ping", {}), settings())
        self.assertRefused(expired, "bad_mac")

    def test_ping_never_echoes_a_secret(self):
        from_env = {
            bridge.ENV_SECRET: SECRET,
            bridge.ENV_PREVIOUS_SECRET: OLD_SECRET,
            bridge.ENV_GRACE_UNTIL: str(NOW + 60),
        }
        for environment in ({}, from_env):
            os.environ.update(environment)
            context = settings(previous_secret=OLD_SECRET, grace_until=str(NOW + 60))
            answer = self.plugin.run("ping", self.signer.params("ping", {}), context)
            text = json.dumps(answer)
            self.assertTrue(answer["ok"], answer)
            self.assertNotIn(SECRET, text)
            self.assertNotIn(OLD_SECRET, text)
            self.assertIn(answer["configured"], ("env", "settings"))

    def test_the_secret_and_mac_never_reach_the_log(self):
        with self.assertLogs("plugins.crosstalk_bridge", level="INFO") as logs:
            params = self.signer.params("clear_channel_groups", {"channel_ids": [1]})
            self.plugin.run("clear_channel_groups", params, settings())
            forged = dict(params, mac="2" * 64)
            self.plugin.run("clear_channel_groups", forged, settings())
        text = "\n".join(logs.output)
        self.assertNotIn(SECRET, text)
        self.assertNotIn(params["mac"], text)
        self.assertIn("bad_mac", text)


class Limits(BridgeTest):
    def test_oversize_batches_are_refused_whole(self):
        cap = bridge.LIMITS["max_channels"]
        answer = self.run_op("clear_channel_groups", {"channel_ids": list(range(1, cap + 2))})
        self.assertRefused(answer, "too_large")
        self.assertEqual(TABLES["Channel"][1].channel_group_id, 10)

        lists = [{"channel_id": 1, "stream_ids": list(range(1, bridge.LIMITS["max_list_len"] + 2))}]
        self.assertRefused(self.run_op("set_stream_lists", {"lists": lists}), "too_large")

        many = [{"channel_id": n, "stream_ids": list(range(1, 1001))} for n in range(1, 22)]
        self.assertRefused(self.run_op("set_stream_lists", {"lists": many}), "too_large")

        stats = [
            {"stream_id": n, "stream_stats": {}, "updated_at": NOW}
            for n in range(1, bridge.LIMITS["max_streams"] + 2)
        ]
        self.assertRefused(self.run_op("set_stream_stats", {"stats": stats}), "too_large")

        wide = {"epg_source_id": 5, "window": {"start": 0, "end": 15 * 86400}, "entries": []}
        self.assertRefused(self.run_op("upsert_epg", wide), "too_large")

    def test_a_schema_violation_is_refused_whole(self):
        cases = [
            ("clear_channel_groups", {"channel_ids": [1, 1]}),
            ("clear_channel_groups", {"channel_ids": [True]}),
            ("clear_channel_groups", {"ids": [1]}),
            ("set_stream_lists", {"lists": [{"channel_id": 1}]}),
            ("set_stream_lists", {"lists": [{"channel_id": 1, "stream_ids": [11, 11]}]}),
            ("set_stream_stats", {"stats": [{"stream_id": 11, "stream_stats": [1],
                                             "updated_at": NOW}]}),
            ("ping", {"anything": 1}),
        ]
        for op, payload in cases:
            self.assertRefused(self.run_op(op, payload), "invalid")
        self.assertEqual(stream_list(1), [11, 12])


class Writes(BridgeTest):
    def test_set_stream_lists_replaces_in_order_with_per_channel_errors(self):
        payload = {
            "lists": [
                {"channel_id": 1, "stream_ids": [13, 11]},
                {"channel_id": 2, "stream_ids": [12, 99]},
                {"channel_id": 3, "stream_ids": [14]},
                {"channel_id": 404, "stream_ids": [11]},
            ]
        }
        answer = self.run_op("set_stream_lists", payload)
        self.assertTrue(answer["ok"], answer)
        self.assertEqual(answer["applied"], 2)
        self.assertEqual(answer["skipped"], [])
        errors = {error["id"]: error["reason"] for error in answer["errors"]}
        self.assertEqual(sorted(errors), [2, 404])
        self.assertTrue(errors[2].startswith("not_found: no stream 99"))
        self.assertEqual(stream_list(1), [13, 11])
        self.assertEqual(stream_list(2), [])
        self.assertEqual(stream_list(3), [14])
        self.assertEqual(sorted(ROLLUPS[0]), [1, 3])

    def test_set_stream_lists_is_idempotent(self):
        payload = {"lists": [{"channel_id": 1, "stream_ids": [12, 14, 11]},
                             {"channel_id": 2, "stream_ids": []}]}
        first = self.run_op("set_stream_lists", payload)
        state = copy.deepcopy(TABLES)
        second = self.run_op("set_stream_lists", payload)
        self.assertEqual(first["applied"], 1)
        self.assertEqual(first["skipped"], [2])
        self.assertEqual(second["applied"], 0)
        self.assertEqual(second["skipped"], [1, 2])
        self.assertEqual(_dump(TABLES), _dump(state))
        self.assertEqual(stream_list(1), [12, 14, 11])

    def test_an_empty_list_clears_a_channel(self):
        answer = self.run_op("set_stream_lists", {"lists": [{"channel_id": 1, "stream_ids": []}]})
        self.assertEqual(answer["applied"], 1)
        self.assertEqual(stream_list(1), [])
        # Streams themselves are never deleted.
        self.assertEqual(sorted(TABLES["Stream"]), [11, 12, 13, 14])

    def test_set_stream_stats_writes_and_is_idempotent(self):
        payload = {"stats": [
            {"stream_id": 11, "stream_stats": {"width": 1920, "source_fps": 29.97},
             "updated_at": NOW},
            {"stream_id": 12, "stream_stats": None, "updated_at": NOW},
            {"stream_id": 77, "stream_stats": {}, "updated_at": NOW},
        ]}
        first = self.run_op("set_stream_stats", payload)
        self.assertTrue(first["ok"])
        # Stream 12 already held null stats but no timestamp, so it is written too.
        self.assertEqual(first["applied"], 2)
        self.assertEqual(first["errors"], [{"id": 77, "reason": "not_found: no such stream"}])
        self.assertEqual(TABLES["Stream"][11].stream_stats, {"width": 1920, "source_fps": 29.97})
        self.assertEqual(int(TABLES["Stream"][11].stream_stats_updated_at.timestamp()), NOW)
        second = self.run_op("set_stream_stats", payload)
        self.assertEqual(second["applied"], 0)
        self.assertEqual(second["skipped"], [11, 12])

    def test_clear_channel_groups_with_per_id_errors_and_idempotency(self):
        answer = self.run_op("clear_channel_groups", {"channel_ids": [1, 3, 404]})
        self.assertTrue(answer["ok"])
        self.assertEqual(answer["applied"], 1)
        self.assertEqual(answer["skipped"], [3])
        self.assertEqual(answer["errors"], [{"id": 404, "reason": "not_found: no such channel"}])
        self.assertIsNone(TABLES["Channel"][1].channel_group_id)
        self.assertEqual(TABLES["Channel"][2].channel_group_id, 10)
        again = self.run_op("clear_channel_groups", {"channel_ids": [1, 3, 404]})
        self.assertEqual(again["applied"], 0)
        self.assertEqual(again["skipped"], [1, 3])
        self.assertEqual(sorted(TABLES["Channel"]), [1, 2, 3])

    def test_upsert_epg_replaces_one_window_and_is_idempotent(self):
        window = {"start": NOW, "end": NOW + 7200}
        payload = {
            "epg_source_id": 5,
            "window": window,
            "entries": [
                {"tvg_id": "gen.1", "name": "Generated One", "icon_url": None, "programs": [
                    {"start": NOW, "end": NOW + 3600, "title": "First"},
                    {"start": NOW + 3600, "end": NOW + 7200, "title": "Second",
                     "description": "d", "custom_properties": {"live": True}},
                ]},
                {"tvg_id": "gen.2", "name": "Bad", "icon_url": None, "programs": [
                    {"start": NOW + 9000, "end": NOW + 9600, "title": "Outside"},
                ]},
            ],
        }
        first = self.run_op("upsert_epg", payload)
        self.assertTrue(first["ok"], first)
        self.assertEqual(first["applied"], 1)
        self.assertEqual(first["errors"][0]["id"], "gen.2")
        self.assertEqual(len(TABLES["EPGData"]), 1)
        self.assertEqual(len(TABLES["ProgramData"]), 2)
        state = _dump(TABLES)
        second = self.run_op("upsert_epg", payload)
        self.assertEqual(second["applied"], 1)
        self.assertEqual(_dump(TABLES), state)

        # A programme before the window survives a later replace of the window.
        entry = next(iter(TABLES["EPGData"].values()))
        TABLES["ProgramData"][999] = ProgramData(
            id=999, epg_id=entry.id, start_time=bridge._utc(NOW - 3600),
            end_time=bridge._utc(NOW), title="Earlier", tvg_id="gen.1",
        )
        payload["entries"] = [payload["entries"][0]]
        payload["entries"][0]["programs"] = [{"start": NOW, "end": NOW + 7200, "title": "Long"}]
        payload["entries"][0]["name"] = "Renamed"
        third = self.run_op("upsert_epg", payload)
        self.assertEqual(third["applied"], 1)
        titles = sorted(row.title for row in TABLES["ProgramData"].values())
        self.assertEqual(titles, ["Earlier", "Long"])
        self.assertEqual(next(iter(TABLES["EPGData"].values())).name, "Renamed")

    def test_upsert_epg_only_writes_sources_the_operator_listed(self):
        payload = {"epg_source_id": 6, "window": {"start": NOW, "end": NOW + 60}, "entries": []}
        self.assertRefused(self.run_op("upsert_epg", payload), "not_allowed")
        self.assertRefused(
            self.run_op("upsert_epg", dict(payload, epg_source_id=5),
                        context=settings(epg_sources="")),
            "not_allowed",
        )
        missing = dict(payload, epg_source_id=7)
        self.assertRefused(
            self.run_op("upsert_epg", missing, context=settings(epg_sources="5,7")), "not_found"
        )

    def test_a_database_failure_rolls_the_whole_request_back(self):
        class Exploding(bridge.OrmStore):
            def clear_channel_groups(self, channel_ids):
                super().clear_channel_groups(channel_ids)
                raise IntegrityError("boom")

        plugin = bridge.Plugin(store=Exploding(), clock=lambda: NOW)
        params = self.signer.params("clear_channel_groups", {"channel_ids": [1, 2]})
        answer = plugin.run("clear_channel_groups", params, settings())
        self.assertRefused(answer, "internal")
        self.assertIn("nothing was written", answer["errors"][0]["reason"])
        self.assertEqual(TABLES["Channel"][1].channel_group_id, 10)


def _dump(tables):
    return {
        name: {row_id: sorted(vars(row).items()) for row_id, row in rows.items()}
        for name, rows in tables.items()
    }


class Manifest(unittest.TestCase):
    def test_the_manifest_matches_the_code(self):
        with open(os.path.join(os.path.dirname(HERE), "plugin.json"), encoding="utf-8") as fh:
            manifest = json.load(fh)
        self.assertEqual(manifest["version"], bridge.VERSION)
        self.assertEqual(manifest["actions"], [])
        fields = {field["id"]: field for field in manifest["fields"]}
        for secret in ("secret", "previous_secret"):
            self.assertEqual(fields[secret]["type"], "string")
            self.assertEqual(fields[secret]["input_type"], "password")
        self.assertIn("grace_until", fields)
        self.assertIn("epg_sources", fields)
        for field in manifest["fields"]:
            self.assertIn(field["type"], {"string", "number", "boolean", "select", "text", "info"})


class Vectors(unittest.TestCase):
    """The cross-language contract: crates/crosstalk-core/src/bridge.rs checks the same file."""

    @classmethod
    def setUpClass(cls):
        with open(VECTORS_PATH, encoding="utf-8") as fh:
            cls.vectors = json.load(fh)

    def test_canonical_forms(self):
        self.assertGreater(len(self.vectors["canonical"]), 10)
        for case in self.vectors["canonical"]:
            value = json.loads(case["json"])
            self.assertEqual(bridge.canonical_json(value), case["canonical"], case["name"])

    def test_values_without_a_canonical_form_are_refused(self):
        for case in self.vectors["rejects"]:
            with self.assertRaises((bridge.CanonicalError, ValueError), msg=case["name"]):
                bridge.canonical_json(json.loads(case["json"]))

    def test_signatures(self):
        for case in self.vectors["signatures"]:
            envelope = json.loads(case["envelope"])
            self.assertEqual(bridge.canonical_json(envelope), case["canonical"], case["name"])
            self.assertEqual(bridge.sign(case["secret"], envelope), case["mac"], case["name"])

    def test_signed_vectors_verify_in_the_plugin(self):
        for case in self.vectors["signatures"]:
            envelope = json.loads(case["envelope"])
            params = {"envelope": envelope, "mac": case["mac"]}
            context = settings(secret=case["secret"])
            plugin = bridge.Plugin(clock=lambda ts=envelope["ts"]: ts)
            answer = plugin.run(envelope["op"], params, context)
            # The MAC holds; anything refused after that is about the payload, not the signature.
            for error in answer["errors"]:
                self.assertFalse(error["reason"].startswith("bad_mac"), case["name"])


if __name__ == "__main__":
    unittest.main()
