#!/usr/bin/env python3
"""Parity: crosstalk's clean pipeline against Channel Name Cleaner's own methods.

The lineup app's `clean_*` is Name Cleaner's pipeline, ported — the fixed rule order
(unicode marks, NFKC, junk-only brackets, pipe segments with removed-quality memory,
+1, custom regex, smart casing, whitespace), the protect list, the scope, and the
collision strategies. This script is how that claim is checked: it imports the
plugin's `plugin.py` from `reference/` with Django stubbed (the module only needs
`apps.channels.models.Channel` and `django.db.transaction` at import), drives the
REAL `_compute_changes` over a corpus, and compares every row with the Rust side
(`cargo run -p crosstalk-lineup --example clean_dump`, the same `compute_rows` +
`plan_clean` the action runs).

Nothing of the Rust is mirrored here: the plugin's own code decides the expected
answer — statuses (`protected` / `nochange` / `change` / `collision`), final names
(post-collision), fired rules and removed quality.

What is driven, per case, in the plugin's own order:

  * `_rules` + `_parse_custom_rules` — the switches, the separator, the custom lines;
  * `_is_protected` — groups (casefolded exact), foreign scripts, leading `&`, regex;
  * `_clean` — the eight steps with `step()` change tracking;
  * `_compute_changes` — scope (`selected_groups` / `ignore_groups`, casefolded
    exact), then `_resolve_collisions` (suffix / numeric / skip against every name).

The corpus: lineup names (two shipped lineups), merged DB names (US + UK, incl.
`& TV`, `1+1 International`), provider-variant names from `data/corpus/pairs.jsonl`,
NC test-like cases (pipe junk, superscripts, brackets with `(Moldova)` surviving,
`&TV`, Cyrillic, `+1`, casing, custom rules, whitespace-only) and collision
scenarios. The corpus keeps existing names unique and spells scope groups in exact
case on both sides, so the run measures the pipeline — not the two DOCUMENTED
divergences (`plans/phase-3-lineup.md` step 14): the unified scope resolves group
names with `*`/`?` wildcards, case-sensitively, where NC matches casefolded exact
with no wildcards; and the shared resolver counts occupant slots where NC's set
arithmetic freed a name another channel still held (plus the `skip` cascade fix).
Both fixes are pinned by Rust unit tests instead.

Usage:
    python3 -I scripts/clean_parity.py [--show N]

Exit status is 1 when any row differs, so it gates CI.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO / "reference" / "dispatcharr-channel-name-cleaner" / "channel_name_cleaner"
PKG = "nc_clean_parity"


# --------------------------------------------------------------------------------------
# Loading the plugin without Django
# --------------------------------------------------------------------------------------

class FakeGroup:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeChannel:
    def __init__(self, id: int, name: str, group: str) -> None:
        self.id = id
        self.name = name
        self.channel_group_id = 1 if group else None
        self.channel_group = FakeGroup(group) if group else None


# The corpus channels the fake ORM answers with. Replaced per case BEFORE the drive
# (the plugin module binds `Channel` once at import, so the manager reads this
# global instead of being rebuilt per case).
_FAKE_CHANNELS: list = []


def set_fake_channels(channels: list) -> None:
    global _FAKE_CHANNELS
    _FAKE_CHANNELS = [
        FakeChannel(c["id"], c["name"], c.get("group", "")) for c in channels
    ]


def _install_stubs() -> None:
    """Stub the two Django/ORM imports `plugin.py` needs, backed by `_FAKE_CHANNELS`.

    `Channel.objects` answers `select_related(...).iterator()` (the channels with
    their group objects, like the ORM) and `values_list("name", flat=True)` (every
    name, like the collision pass reads). `django.db.transaction` is never touched
    by `_compute_changes`, only imported. Install ONCE before importing the plugin;
    per-case channels go through [`set_fake_channels`].
    """

    class FakeManager:
        def select_related(self, *args: object) -> "FakeManager":
            return self

        def iterator(self) -> object:
            return iter(_FAKE_CHANNELS)

        def values_list(self, *args: object, **kwargs: object) -> list:
            return [c.name for c in _FAKE_CHANNELS]

    fake_models = types.ModuleType("apps.channels.models")
    fake_models.Channel = types.SimpleNamespace(objects=FakeManager())  # type: ignore[attr-defined]

    for name in (
        "django",
        "django.db",
        "django.db.transaction",
        "apps",
        "apps.channels",
        "apps.channels.models",
    ):
        sys.modules.pop(name, None)
    sys.modules["django"] = MagicMock(name="django")
    sys.modules["django.db"] = MagicMock(name="django.db")
    sys.modules["django.db.transaction"] = MagicMock(name="transaction")
    apps = types.ModuleType("apps")
    apps_channels = types.ModuleType("apps.channels")  # type: ignore[attr-defined]
    sys.modules["apps"] = apps
    sys.modules["apps.channels"] = apps_channels
    sys.modules["apps.channels.models"] = fake_models


def _load_plugin() -> object:
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        f"{PKG}.plugin", PLUGIN_DIR / "plugin.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"{PKG}.plugin"] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------------------
# Corpus
# --------------------------------------------------------------------------------------

NC_CASES: list[tuple[str, str]] = [
    # (name, group) — NC test-like cases.
    # A trailing bare country segment keeps its bar (phase 3 review divergence, see
    # `_country_segment_divergence`): NC writes `BBC One UK`, crosstalk `BBC One | UK`.
    ("BBC One | UK", "UK"),
    ("Sky Sports | HD | UK", "UK"),
    ("UK | Sky Sports | HD", "UK"),
    ("Sky Atlantic | FHD |", "Sports"),
    ("Sky Atlantic | HD |", "News"),
    ("Rte One +1 | HD |", "News"),
    ("007 ᴴᴰ", "Movies"),
    ("ESPN (1080p)", "Sports"),
    ("ESPN [VIP]", "Sports"),
    ("CNN (Moldova)", "News"),
    ("Sky Cinema Disney [FHD] | MULTI |", "Movies"),
    ("&TV | HD |", "News"),
    ("&flix", "Movies"),
    ("РТР-Планета | HD |", "News"),
    ("Channel 4+1", "News"),
    ("1+1 International", "News"),
    ("bbc news", "News"),
    ("BBC news 24", "News"),
    ("  ESPN   HD  ", "Sports"),
    ("ITV2 +1 | 4K |", "News"),
    ("Sky Sports+ | Event 03", "Sports"),
    ("Télé-Québec [BadEPG]", "News"),
    ("| HD |", "News"),
    ("ESPN", "Sports"),
    ("Discovery (720p / HEVC)", "Docs"),
    ("HBO [H.265] | BACKUP |", "Movies"),
    ("FOX Sports (Central)", "Sports"),
    ("Comedy Central | SD |", "News"),
]

PROTECTED_GROUPS = "Todays Live Events, Info | Service Status"


def _lineup_names() -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for file in ("UK_Freeview_lineup.json", "US_DirecTV-Premier_lineup.json"):
        data = json.loads((REPO / "data" / "merged" / "lineups" / file).read_text())
        for category, entries in data.get("categories", {}).items():
            for entry in entries[:12]:
                out.append((entry["name"], category))
    return out


def _merged_db_names() -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for file in ("US.json", "UK.json"):
        data = json.loads((REPO / "data" / "merged" / "channels" / file).read_text())
        for row in data.get("channels", [])[:25]:
            out.append((row["channel_name"], row.get("category", "DB") or "DB"))
    return out


#: The country codes the engine keeps delimited in a pipe collapse (decided N15,
#: `tags::is_pipe_country_code`), mirrored so the oracle can name the one documented
#: divergence of the pipe step.
PIPE_COUNTRY_CODES = {"US", "USA", "UK", "CA", "AU", "FR", "DE", "ES", "IT", "NL", "BR", "MX", "IN"}


def _country_segment_divergence(nc_new: str, rust_new: str) -> bool:
    """Phase 3 review: a trailing bare country-code pipe segment keeps its bar in
    crosstalk (`BBC One | UK`, the engine's own N15 collapse, so the cleaned name
    keeps its identity) where NC joins it with the separator (`BBC One UK`). True
    when that is the ONLY difference between the two outputs."""
    head, sep, code = rust_new.rpartition(" | ")
    if not sep or code.upper() not in PIPE_COUNTRY_CODES:
        return False
    return nc_new.endswith(code) and nc_new[: -len(code)].rstrip(" -:|") == head.rstrip(" -:|")


def _corpus_variant_names(limit: int = 40) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    with open(REPO / "data" / "corpus" / "pairs.jsonl", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                pair = json.loads(line)
            except json.JSONDecodeError:
                continue
            for key in ("query", "candidate"):
                name = (pair.get(key) or "").strip()
                # Case-insensitive dedup: two channels sharing a casefolded name with
                # only one renamed away is NC's slot-counting bug (the step 7 review
                # fix the shared resolver carries) — the oracle measures the pipeline,
                # not that documented divergence, so the trigger stays out of the corpus.
                if name and name.casefold() not in seen and len(name) <= 60:
                    seen.add(name.casefold())
                    out.append((name, "Corpus"))
            if len(out) >= limit:
                return out
    return out


def _corpus() -> list[dict]:
    channels: list[dict] = []
    next_id = 1

    def add(name: str, group: str) -> None:
        nonlocal next_id
        channels.append({"id": next_id, "name": name, "group": group})
        next_id += 1

    for name, group in NC_CASES:
        add(name, group)
    for name, group in _lineup_names():
        add(f"{name} | HD |", group)
    for name, group in _merged_db_names():
        add(name, group)
    for name, group in _corpus_variant_names():
        add(name, group)
    # Collision scenarios: the Sky pair is already in NC_CASES (ids 1-2, distinct
    # removed segments); a kept name blocks a single claimant (numeric fallback).
    add("ESPN 2", "Sports")
    add("ESPN 2 | SD |", "News")
    # Scope probes: one channel per scoped group (exact case on both sides).
    add("Scoped One | FHD |", "Sports")
    add("Scoped Two | FHD |", "News")
    add("Live Event Tonight", "Todays Live Events")
    add("Status Screen | HD |", "Info | Service Status")
    return channels


CASES: list[dict] = [
    {
        "name": "defaults",
        "settings": {},
        "scope": {},
    },
    {
        "name": "all-rules-on",
        "settings": {
            "unicode_nfkc": True,
            "normalize_plus_one": True,
            "fix_casing": True,
            "join_separator": " - ",
        },
        "scope": {},
    },
    {
        "name": "custom-rules",
        "settings": {"custom_rules": "Rte => RTE\nSky Sports+ => Sky Sports Plus"},
        "scope": {},
    },
    {
        # Python `re.sub` replacement syntax: group references and a literal `$`.
        "name": "custom-backrefs",
        "settings": {
            "custom_rules": "^(\\w+) One\\b => \\1 1\n"
            "(?P<net>Sky) Sports => \\g<net> Sport\n"
            "Rte => R$1E"
        },
        "scope": {},
    },
    {
        "name": "protect-regex",
        "settings": {"protect_regex": "^(PPV|Event)\\b"},
        "scope": {},
    },
    {
        "name": "numeric",
        "settings": {"collision_strategy": "numeric"},
        "scope": {},
    },
    {
        "name": "skip",
        "settings": {"collision_strategy": "skip"},
        "scope": {},
    },
    {
        "name": "scope-include",
        "settings": {},
        "scope": {"groups": "Sports"},
    },
    {
        "name": "scope-exclude",
        "settings": {},
        "scope": {"ignore_groups": "News"},
    },
]


# --------------------------------------------------------------------------------------
# Driving both sides
# --------------------------------------------------------------------------------------

def _nc_rows(plugin: object, channels: list[dict], case: dict) -> list[dict]:
    settings = {
        "strip_unicode_quality": True,
        "unicode_nfkc": False,
        "strip_bracketed_junk": True,
        "strip_pipe_junk": True,
        "normalize_plus_one": False,
        "fix_casing": False,
        "normalize_whitespace": True,
        "join_separator": " ",
        "collision_strategy": "suffix",
        "protected_groups": PROTECTED_GROUPS,
        "protect_foreign_scripts": True,
        "protect_leading_symbol": True,
        "protect_regex": "",
        "custom_rules": "",
        "selected_groups": (case["scope"].get("groups", "")),
        "ignore_groups": (case["scope"].get("ignore_groups", "")),
    }
    settings.update(case["settings"])
    set_fake_channels(channels)
    instance = object.__new__(plugin.Plugin)  # type: ignore[attr-defined]
    rows, _counts = instance._compute_changes(settings)
    return [
        {
            "id": row["id"],
            "group": row["group"],
            "status": row["status"],
            "old": row["old"],
            "new": row["new"],
            "rules": sorted(row["tags"]),
            "removed": row["rq"],
        }
        for row in rows
    ]


def _rust_rows(channels: list[dict], case: dict) -> list[dict]:
    payload = {
        "channels": channels,
        "scope": case["scope"],
        "settings": case["settings"],
    }
    proc = subprocess.run(
        ["cargo", "run", "-q", "-p", "crosstalk-lineup", "--example", "clean_dump"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        cwd=str(REPO),
    )
    if proc.returncode != 0:
        raise RuntimeError(f"clean_dump failed: {proc.stderr[-4000:]}")
    return json.loads(proc.stdout)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--show", type=int, default=10)
    args = parser.parse_args()

    _install_stubs()
    plugin = _load_plugin()
    channels = _corpus()
    mismatches: list[str] = []
    total = 0
    country_segment = 0
    for case in CASES:
        expected = {row["id"]: row for row in _nc_rows(plugin, channels, case)}
        got = {row["id"]: row for row in _rust_rows(channels, case)}
        for id, want in sorted(expected.items()):
            total += 1
            have = got.get(id)
            if have is None:
                mismatches.append(f"[{case['name']}] id={id}: missing from Rust output")
                continue
            # Scope filters before cleaning: a channel NC's scope drops never reaches
            # the Rust dump either (same set) — both lists carry every in-scope row.
            # The one documented divergence: crosstalk keeps a trailing bare
            # country-code pipe segment delimited (`BBC One | UK`). When that is the
            # only difference in the resulting name, the row's status/rules/removed
            # follow from it (a name left as-is is `nochange`, the pipe rule did not
            # fire), so the whole row counts once as that divergence.
            have_new = have["new"] or have["old"]
            if have["new"] != want["new"] and _country_segment_divergence(want["new"], have_new):
                rest = [k for k in ("old", "group") if have[k] != want[k]]
                if not rest:
                    country_segment += 1
                    continue
            for key in ("status", "old", "new", "group", "rules", "removed"):
                if have[key] != want[key]:
                    mismatches.append(
                        f"[{case['name']}] id={id} {key}: NC={want[key]!r} rust={have[key]!r} "
                        f"(old={want['old']!r})"
                    )
        extra = sorted(set(got) - set(expected))
        for id in extra:
            mismatches.append(f"[{case['name']}] id={id}: extra Rust row {got[id]!r}")

    print(f"clean_parity: {total} rows over {len(CASES)} cases, {len(mismatches)} mismatches"
          f" ({country_segment} documented country-segment divergence(s))")
    for line in mismatches[: args.show]:
        print(f"  MISMATCH {line}")
    if len(mismatches) > args.show:
        print(f"  ... and {len(mismatches) - args.show} more")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
