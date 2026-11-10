#!/usr/bin/env python3
"""Parity: crosstalk's lineup loader against Lineuparr's own `_load_lineup` pipeline.

The lineup app's file loading is Lineuparr's loader, ported — the `CC_` name-prefix
rewrite, per-entry and per-category country resolution, the filename country,
`category_detail` folding with the SIMPLIFIED/REFINED maps, `_make_group_name` /
`_get_group_prefix` group names, and `_parse_channel_number` numbers. This script is
how that claim is checked: it imports the plugin's `plugin.py` from `reference/` with
the Django stubs its own test suite installs
(`reference/Dispatcharr-Lineuparr-Plugin/tests/conftest.py`), drives the REAL methods
over every shipped lineup file, and compares each entry with the Rust side
(`cargo run -p crosstalk-lineup --example lineup_dump`).

Nothing of the Rust is mirrored here: the plugin's own code decides the expected answer.

What is driven, per case, in the plugin's own order:

  * `_load_lineup` (790) — file read, `_rewrite_country_prefixes` (825) and
    `_apply_category_detail` (878), with `category_detail` from the case;
  * `_parse_lineup_filename` (925) — the filename country;
  * `_resolve_category_country` (935) — the per-category country, filename fallback;
  * `_resolve_entry_country` (858) — the per-entry country, entry first;
  * `_get_group_prefix` (993) — the group prefix, from the case or the package name;
  * `_parse_channel_number` (1009) — ints as-is, ranges to their first number;
  * `_make_group_name` (1123) — the group name, with the `All` sentinel rule.

The Cartesian product is 29 files x 4 `category_detail` modes x 4 group prefixes
(blank/auto, `none`, `DTV-`, `US `) = 464 cases. The one harness-side normalization is
the unparseable-number fallback: no shipped file exercises `_parse_channel_number`'s
"return raw and let Django reject it" branch (29 files hold ints and dash ranges
only), so a non-int result is compared as null on both sides.

Usage:
    python3 -I scripts/lineup_file_parity.py [--show N]

Exit status is 1 when any entry differs, so it can gate CI.
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO / "reference" / "Dispatcharr-Lineuparr-Plugin" / "Lineuparr"
PKG = "lineuparr_file_parity"

DETAILS = ["none", "refined", "simple", "normal"]
PREFIXES = ["", "none", "DTV-", "US "]


# ---------------------------------------------------------------------------------------
# Loading the plugin without Django, exactly as its own test suite does
# ---------------------------------------------------------------------------------------

def _install_stubs() -> None:
    """`reference/Dispatcharr-Lineuparr-Plugin/tests/conftest.py`, inlined.

    The plugin is imported, never run: every Django/ORM import becomes a MagicMock,
    and only the pure loader pipeline (`_load_lineup` and the helpers above, which
    touch no model) is driven.
    """
    for mod in (
        "django",
        "django.db",
        "django.db.transaction",
        "apps",
        "apps.channels",
        "apps.channels.models",
        "apps.m3u",
        "apps.m3u.models",
        "apps.epg",
        "apps.epg.models",
        "core",
        "core.utils",
    ):
        sys.modules.setdefault(mod, MagicMock(name=mod))
    # `transaction.atomic` used as a decorator/context manager elsewhere in the file.
    import django.db
    django.db.transaction = MagicMock(name="transaction")
    django.db.transaction.atomic = MagicMock(
        return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock())
    )


def _load_plugin():
    _install_stubs()
    sys.path.insert(0, str(PLUGIN_DIR.parent))
    pkg = types.ModuleType(PKG)
    pkg.__path__ = [str(PLUGIN_DIR)]
    sys.modules[PKG] = pkg
    import importlib

    module = importlib.import_module(f"{PKG}.plugin")
    # `_load_lineup` logs one line per load at INFO: 464 cases would bury the diff.
    module.LOGGER.disabled = True
    return module


# ---------------------------------------------------------------------------------------
# One case through the plugin's own pipeline
# ---------------------------------------------------------------------------------------

def _quiet_logger():
    logger = logging.getLogger("lineuparr_file_parity")
    logger.handlers = [logging.NullHandler()]
    logger.setLevel(logging.CRITICAL)
    logger.propagate = False
    return logger


def plugin_case(plugin_module, filename: str, detail: str, prefix: str) -> dict:
    """Run the plugin's `_load_lineup` pipeline for one case.

    Returns the same shape `lineup_dump` prints: file, package, country, prefix,
    detail, categories (file order) and entries (file order).
    """
    plugin = plugin_module.Plugin()
    logger = _quiet_logger()
    settings = {
        "lineup_file": filename,
        "category_detail": detail,
        "group_prefix": prefix,
    }
    data = plugin._load_lineup(dict(settings), logger)
    lineup_cc, _provider = plugin._parse_lineup_filename(filename)
    group_prefix = plugin._get_group_prefix(dict(settings), data)

    categories: list[str] = []
    entries: list[dict] = []
    for category, channels in data["categories"].items():
        categories.append(category)
        category_cc = plugin._resolve_category_country(category, lineup_cc)
        for entry in channels:
            entries.append({
                "name": entry.get("name"),
                "number": _plugin_number(plugin, entry.get("number")),
                "category": category,
                "group": plugin._make_group_name(group_prefix, category),
                "country": plugin._resolve_entry_country(entry, category_cc),
                "aliases": list(entry.get("aliases") or []),
            })
    return {
        "file": filename,
        "package": data.get("package"),
        "country": lineup_cc,
        "prefix": group_prefix,
        "detail": detail,
        "categories": categories,
        "entries": entries,
    }


def _plugin_number(plugin, raw):
    """`_parse_channel_number`, with the harness-only null fallback (see module doc)."""
    number = plugin._parse_channel_number(raw)
    if isinstance(number, bool) or not isinstance(number, int):
        return None
    return number


# ---------------------------------------------------------------------------------------
# The Rust side
# ---------------------------------------------------------------------------------------

def build_rust() -> Path:
    result = subprocess.run(
        ["cargo", "build", "--locked", "-p", "crosstalk-lineup", "--example", "lineup_dump"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stdout, file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        raise SystemExit(f"cargo build failed (exit {result.returncode})")
    binary = REPO / "target" / "debug" / "examples" / "lineup_dump"
    if not binary.exists():
        raise SystemExit(f"expected example binary at {binary}")
    return binary


def rust_case(binary: Path, filename: str, detail: str, prefix: str) -> dict:
    short = filename.removesuffix("_lineup.json")
    result = subprocess.run(
        [str(binary), "--file", short, "--detail", detail, "--prefix", prefix],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"lineup_dump failed for {filename} x {detail} x {prefix!r}:\n{result.stderr}"
        )
    return json.loads(result.stdout)


# ---------------------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------------------

def iter_cases(plugin_module):
    names = sorted(
        path.name
        for path in PLUGIN_DIR.glob("*_lineup.json")
    )
    if len(names) != 29:
        raise SystemExit(f"expected 29 lineup files, found {len(names)}")
    for filename in names:
        for detail in DETAILS:
            for prefix in PREFIXES:
                yield filename, detail, prefix


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--show", type=int, default=0,
                        help="print the first N compared cases and stop (0 = all)")
    args = parser.parse_args()

    plugin_module = _load_plugin()
    binary = build_rust()
    mismatches = 0
    compared = 0
    for filename, detail, prefix in iter_cases(plugin_module):
        expected = plugin_case(plugin_module, filename, detail, prefix)
        got = rust_case(binary, filename, detail, prefix)
        compared += 1
        if args.show and compared <= args.show:
            print(f"--- {filename} x {detail} x {prefix!r} "
                  f"({len(expected['entries'])} entries) ---")
            print(json.dumps(got, sort_keys=True)[:400])
        if json.dumps(expected, sort_keys=True) != json.dumps(got, sort_keys=True):
            mismatches += 1
            print(f"MISMATCH: {filename} x {detail} x {prefix!r}")
            for key in ("package", "country", "prefix", "categories"):
                if expected.get(key) != got.get(key):
                    print(f"  {key}: plugin={expected.get(key)!r} rust={got.get(key)!r}")
            for index, (want, have) in enumerate(
                zip(expected["entries"], got["entries"], strict=False)
            ):
                if want != have:
                    print(f"  entry {index}: plugin={want!r} rust={have!r}")
                    if index > 4:
                        print("  ... (truncated)")
                        break
            if len(expected["entries"]) != len(got["entries"]):
                print(f"  entry count: plugin={len(expected['entries'])} "
                      f"rust={len(got['entries'])}")
    print(f"lineup_file_parity: {compared} cases, {mismatches} mismatches")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
