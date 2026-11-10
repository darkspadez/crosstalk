#!/usr/bin/env python3
"""Build the merged phase-3 data set from the four plugins' shipped tables.

Reads every plugin's channel databases, station tables and alias tables out of
``reference/`` and writes one deduplicated union under ``data/merged/``:

    data/merged/channels/<CC>.json   one union database per country
    data/merged/stations.json        one station table + callsign allowlist
    data/merged/aliases.json         one alias table (+ per-country overrides)
    data/merged/lineups/             Lineuparr's 29 lineup files, byte copies
    data/merged/PROVENANCE.json      machine-readable provenance/conflict log

``docs/data-merge-report.md`` is the human-readable companion report.

Dedup key decisions (see the report for the evidence behind each):

* Channels: the verbatim ``channel_name`` string. Verified across all inputs
  that no two entries sharing a name ever disagree on ``category``/``type``
  (the only cross-plugin field is SM's ``zones`` array), and that every
  in-file duplicate is an identical row, so verbatim keys are lossless.
  Case-only variants (``Afro``/``AFRO``) DO exist and carry different
  category/type; the verbatim key keeps them apart, exactly as the source
  files do, and they are listed in PROVENANCE (``case_variants``) for the
  normalizer decisions of step 5. Duplicates collapse into one entry with a
  sorted ``sources`` list.
* Field conflicts: newest source wins. File versions are ``YYYY-MM-DD`` dates
  with an optional ``-vN`` suffix; comparison is on the parsed tuple.
* Stations: Channel-Mapparr's table, as shipped. SM's ``networks.json`` is
  byte-identical to CM's own previous table (CM commit 2d34bc3), which CM's
  ``build_networks_json.py`` took as input when it rebuilt from the dated
  2026-08-10 FCC LMS dump: every SM row CM lacks is one that build dropped
  as a cancelled licence, and every differing field is the dump's newer
  value (that build never replaces a specific network with a vague one). So
  CM wins every field and SM-only rows are dropped, both listed. The build
  refuses to run if SM's file stops being that predecessor.
* Aliases: the union. An alias value listed under two or more canonicals is
  kept under every one of them (the tables are canonical -> accepted names,
  so a shared alias is how all four plugins already behave) and recorded in
  ``alias_conflicts.json``: ``agreed`` when every plugin that carries a
  claiming canonical lists the alias under it, ``disputed`` otherwise, with a
  ``self_name`` hint when the alias casefold-equals exactly one claimant.
  Step 5 decides each for reverse (alias -> canonical) lookups. Country
  alias tables merge independently per country code.

The build is deterministic and idempotent: outputs are compact JSON
(``separators=(",", ":")``, UTF-8, LF terminator), object keys and lists are
emitted in sorted order, entries carry no timestamps, and ``--check``
reproduces every byte. Run with ``python3 -I`` so the pinned plugin checkouts
are never imported as live code — ``aliases.py`` files are parsed with
``ast``, never executed.

Usage:
    python3 -I scripts/data/build_merged.py [--root DIR] [--check]

    --root DIR   repository root (default: derived from this script's path)
    --check      rebuild into a temp dir and fail if data/merged/ differs
"""

from __future__ import annotations

import argparse
import ast
import collections
import hashlib
import json
import os
import re
import sys
import tempfile

PLUGINS = ("JAN", "SM", "CM", "LU")

CHANNEL_PLUGIN_DIRS = {
    "JAN": "reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor",
    "SM": "reference/Stream-Mapparr/Stream-Mapparr",
    "CM": "reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr",
}

JAN_DIR = "reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor"
SM_DIR = "reference/Stream-Mapparr/Stream-Mapparr"
CM_DIR = "reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr"
CM_CORRECTIONS = (
    "reference/Dispatcharr-Channel-Maparr-Plugin/scripts/networks_corrections.json"
)
LU_DIR = "reference/Dispatcharr-Lineuparr-Plugin/Lineuparr"

OUT_DIR = "data/merged"
REPORT_PATH = "docs/data-merge-report.md"

# sha256 of SM's networks.json == CM's networks.json at 2d34bc3, the table
# CM's build_networks_json.py rebuilt from (its "previous file", 1915 rows).
# The stations policy rests on that identity; see merge_stations.
SM_STATIONS_PREDECESSOR_SHA256 = (
    "1ddc72f8f763a107eba4870f177060aac46e6557f6c0395af37f7d224db8d2a2"
)

VERSION_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:-v(\d+))?$")


def parse_version(text):
    """Parse a ``YYYY-MM-DD[-vN]`` header into a comparable tuple."""
    if not isinstance(text, str):
        return (0,)
    match = VERSION_RE.match(text.strip())
    if not match:
        return (0,)
    year, month, day, variant = match.groups()
    return (int(year), int(month), int(day), int(variant or 0))


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def write_compact(path, obj):
    """Write deterministic compact JSON with a trailing newline."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(
            obj, handle, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        handle.write("\n")


def load_alias_tables(path):
    """Parse CHANNEL_ALIASES / COUNTRY_ALIASES out of an aliases.py file.

    The file is parsed with ``ast`` and only top-level plain-dict assignments
    are read, so plugin code can never execute (this script runs with -I and
    must not import the plugins).
    """
    with open(path, encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=path)
    tables = {}
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id in ("CHANNEL_ALIASES", "COUNTRY_ALIASES")
        ):
            tables[node.targets[0].id] = ast.literal_eval(node.value)
    return tables.get("CHANNEL_ALIASES", {}), tables.get("COUNTRY_ALIASES", {})


def merge_channels(root):
    """Union all plugins' per-country channel databases keyed by channel_name."""
    per_country = {}  # CC -> {"files": {plug: {...}}, "entries": {name: {...}}}
    inputs = []
    for plug, plug_dir in CHANNEL_PLUGIN_DIRS.items():
        names = sorted(
            name
            for name in os.listdir(os.path.join(root, plug_dir))
            if name.endswith("_channels.json")
        )
        for name in names:
            rel = os.path.join(plug_dir, name)
            data = read_json(os.path.join(root, rel))
            code = data.get("country_code") or name.split("_")[0]
            version = data.get("version")
            inputs.append(
                {
                    "path": rel,
                    "sha256": sha256_file(os.path.join(root, rel)),
                    "header": version,
                    "kind": "channels",
                    "plugin": plug,
                }
            )
            slot = per_country.setdefault(
                code, {"files": {}, "entries": {}, "country_names": {}, "extra": {}}
            )
            slot["files"][plug] = {
                "path": rel,
                "version": version,
                "count": len(data.get("channels", [])),
            }
            slot["country_names"][plug] = data.get("country_name")
            # Free-text file headers (NL/PL description, PL source URLs).
            slot["extra"][plug] = {
                field: data[field] for field in ("description", "sources") if field in data
            }
            for entry in data.get("channels", []):
                key = entry["channel_name"]
                node = slot["entries"].setdefault(
                    key,
                    {
                        "channel_name": key,
                        "sources": set(),
                        "field_votes": {},
                    }
                )
                node["sources"].add(plug)
                for field in ("category", "type"):
                    votes = node["field_votes"].setdefault(field, {})
                    votes.setdefault(entry.get(field), set()).add(plug)
                if "zones" in entry:
                    votes = node["field_votes"].setdefault("zones", {})
                    votes.setdefault(tuple(entry.get("zones") or []), set()).add(plug)

    outputs = {}
    provenance = {}
    for code in sorted(per_country):
        slot = per_country[code]
        # Newest source wins the header (country name + version).
        ranked = sorted(
            slot["files"].items(),
            key=lambda item: (parse_version(item[1]["version"]), item[0]),
        )
        winner = ranked[-1][0]
        newest_version = slot["files"][winner]["version"]
        merged = []
        field_conflicts = []
        for name in sorted(slot["entries"]):
            node = slot["entries"][name]
            record = {
                "channel_name": name,
                "category": None,
                "type": None,
                "sources": sorted(node["sources"]),
            }
            for field in ("category", "type"):
                votes = node["field_votes"].get(field, {})
                values = [v for v in votes if v is not None]
                if len(values) > 1:
                    # Genuine conflict: newest contributing file wins.
                    order = sorted(
                        (
                            plug
                            for plugs in votes.values()
                            for plug in plugs
                        ),
                        key=lambda p: (parse_version(slot["files"][p]["version"]), p),
                    )
                    picked_plug = order[-1]
                    picked = next(
                        v for v, plugs in votes.items() if picked_plug in plugs
                    )
                    field_conflicts.append(
                        {
                            "channel_name": name,
                            "field": field,
                            "values": {
                                json.dumps(v): sorted(plugs)
                                for v, plugs in votes.items()
                            },
                            "winner": picked,
                        }
                    )
                    record[field] = picked
                elif values:
                    record[field] = values[0]
            zone_votes = node["field_votes"].get("zones", {})
            if zone_votes:
                zone_lists = [list(v) for v in zone_votes]
                if len(zone_lists) > 1:
                    order = sorted(
                        (
                            plug
                            for plugs in zone_votes.values()
                            for plug in plugs
                        ),
                        key=lambda p: (parse_version(slot["files"][p]["version"]), p),
                    )
                    picked_plug = order[-1]
                    picked = next(
                        list(v)
                        for v, plugs in zone_votes.items()
                        if picked_plug in plugs
                    )
                    field_conflicts.append(
                        {
                            "channel_name": name,
                            "field": "zones",
                            "values": {
                                json.dumps(list(v)): sorted(plugs)
                                for v, plugs in zone_votes.items()
                            },
                            "winner": picked,
                        }
                    )
                    record["zones"] = picked
                else:
                    record["zones"] = zone_lists[0]
            merged.append(record)
        total_in = sum(info["count"] for info in slot["files"].values())
        # Rows read minus distinct names out: exact dupes within/across files.
        duplicates = total_in - len(merged)
        folded = collections.defaultdict(list)
        for name in sorted(slot["entries"]):
            folded[name.casefold().strip()].append(name)
        case_variants = sorted(names for names in folded.values() if len(names) > 1)
        unique_to = {}
        for plug in slot["files"]:
            unique_to[plug] = sorted(
                name
                for name, node in slot["entries"].items()
                if node["sources"] == {plug}
            )
        outputs[code] = {
            "country_code": code,
            "country_name": slot["country_names"][winner],
            **slot["extra"][winner],
            "version": newest_version,
            "merged_from": {
                plug: {
                    "file": info["path"],
                    "version": info["version"],
                }
                for plug, info in sorted(slot["files"].items())
            },
            "channels": merged,
        }
        provenance[code] = {
            "inputs": slot["files"],
            "rows_in": {plug: info["count"] for plug, info in slot["files"].items()},
            "rows_out": len(merged),
            "duplicates_merged": duplicates,
            "field_conflicts": field_conflicts,
            "case_variants": case_variants,
            "unique_to": {plug: names for plug, names in unique_to.items()},
            "unique_counts": {plug: len(names) for plug, names in unique_to.items()},
        }
    return outputs, provenance, inputs


STATION_FIELDS = (
    "network_affiliation",
    "community_served_city",
    "community_served_state",
    "tv_virtual_channel",
    "facility_id",
    "active_ind",
    "station_class",
)


def merge_stations(root):
    """CM's FCC table (+ supplemental + corrections), checked against SM's.

    SM's table is CM's pre-rebuild table (pinned by sha256 below), so it is
    not an independent source: CM's rebuild kept every predecessor station
    except cancelled licences and replaced fields only with the dump's newer
    values. CM therefore wins every field and SM-only rows are dropped; both
    are recorded so step 5/6 can see exactly what SM matching loses.
    """
    inputs = []

    def track(rel, kind, plugin):
        inputs.append(
            {
                "path": rel,
                "sha256": sha256_file(os.path.join(root, rel)),
                "header": None,
                "kind": kind,
                "plugin": plugin,
            }
        )

    cm_rel = os.path.join(CM_DIR, "networks.json")
    sup_rel = os.path.join(CM_DIR, "networks_supplemental.json")
    sm_rel = os.path.join(SM_DIR, "networks.json")
    track(cm_rel, "stations", "CM")
    track(sup_rel, "stations", "CM")
    track(sm_rel, "stations", "SM")
    track(CM_CORRECTIONS, "station-corrections", "CM")
    jan_rel = os.path.join(JAN_DIR, "us_station_callsigns.json")
    track(jan_rel, "callsign-allowlist", "JAN")

    sm_sha = sha256_file(os.path.join(root, sm_rel))
    if sm_sha != SM_STATIONS_PREDECESSOR_SHA256:
        raise SystemExit(
            f"{sm_rel} changed (sha256 {sm_sha}); it is no longer CM's "
            "predecessor table, so the CM-wins/drop-SM-only station policy must "
            "be re-reviewed before rebuilding"
        )
    cm_table = read_json(os.path.join(root, cm_rel))
    sup_table = read_json(os.path.join(root, sup_rel))
    sm_table = read_json(os.path.join(root, sm_rel))
    corrections = read_json(os.path.join(root, CM_CORRECTIONS))
    jan_allow = read_json(os.path.join(root, jan_rel))

    # Apply the shipped corrections file on top of CM's table. Upstream ships
    # it already applied, in which case this is a verifying no-op; if the
    # table ever regresses to the pre-fix dump, the expects/fields pair
    # applies the fix. Anything else fails loudly instead of shipping stale.
    by_callsign = {row["callsign"]: dict(row) for row in cm_table}
    for fix in corrections:
        call = fix["callsign"]
        if call not in by_callsign:
            raise SystemExit(f"corrections entry {call} matches no CM station")
        fields = fix.get("fields", {})
        if all(by_callsign[call].get(f, "") == v for f, v in fields.items()):
            continue
        for field, wanted in fix.get("expects", {}).items():
            actual = by_callsign[call].get(field, "")
            if actual != wanted:
                raise SystemExit(
                    f"corrections precondition failed for {call}.{field}: "
                    f"expected {wanted!r}, table has {actual!r}"
                )
        by_callsign[call].update(fields)
        by_callsign[call]["corrected"] = fix.get("reason", "")
    for row in sup_table:
        if row["callsign"] in by_callsign:
            raise SystemExit(f"supplemental {row['callsign']} duplicates CM table")
        entry = dict(row)
        entry["supplemental"] = row.get("source", "")
        by_callsign[row["callsign"]] = entry

    sm_by_call = {row["callsign"]: row for row in sm_table}
    conflicts = []
    sm_only = []
    for call in sorted(set(by_callsign) | set(sm_by_call)):
        if call in by_callsign and call in sm_by_call:
            cm_row, sm_row = by_callsign[call], sm_by_call[call]
            for field in STATION_FIELDS:
                if field not in sm_row:
                    # A column SM's table does not carry (station_class);
                    # CM-only, not a conflict.
                    continue
                cm_val, sm_val = cm_row.get(field, ""), sm_row.get(field, "")
                if cm_val != sm_val:
                    conflicts.append(
                        {
                            "callsign": call,
                            "field": field,
                            "kind": (
                                "case"
                                if str(cm_val).casefold() == str(sm_val).casefold()
                                else "value"
                            ),
                            "CM": cm_val,
                            "SM": sm_val,
                            "winner": "CM",
                        }
                    )
        elif call in sm_by_call:
            # Dropped by CM's rebuild as a cancelled licence (42 are the FCC's
            # D-prefixed republications of dead callsigns, plus KVTV). Not
            # resurrected; listed in provenance.
            sm_only.append(call)

    cm_calls = {r["callsign"] for r in cm_table}
    sup_calls = {r["callsign"] for r in sup_table}
    stations = {}
    for call in sorted(by_callsign):
        row = by_callsign[call]
        record = {"callsign": call}
        for field in STATION_FIELDS:
            if field in row:
                record[field] = row[field]
        for note in ("carried_over", "corrected", "supplemental"):
            if row.get(note):
                record[note] = row[note]
        sources = []
        if call in cm_calls or call in sup_calls:
            sources.append("CM")
        if call in sup_calls:
            record["from_supplemental"] = True
        if call in sm_by_call:
            sources.append("SM")
        record["sources"] = sorted(sources)
        stations[call] = record

    allowlist = sorted(set(jan_allow.get("callsigns", [])))
    provenance = {
        "rows_in": {
            "CM": len(cm_table),
            "CM-supplemental": len(sup_table),
            "SM": len(sm_table),
        },
        "rows_out": len(stations),
        "field_conflicts": conflicts,
        "field_conflict_count": len(conflicts),
        "field_conflict_counts": {
            f"{field}/{kind}": sum(
                1 for c in conflicts if c["field"] == field and c["kind"] == kind
            )
            for field, kind in sorted({(c["field"], c["kind"]) for c in conflicts})
        },
        "sm_predecessor_sha256": SM_STATIONS_PREDECESSOR_SHA256,
        "sm_only_dropped": sorted(sm_only),
        "sm_only_count": len(sm_only),
        "cm_only_count": len([c for c in stations if c not in sm_by_call]),
        "allowlist_in": len(allowlist),
    }
    output = {"stations": stations, "callsign_allowlist": allowlist}
    return output, provenance, inputs


# ---------------------------------------------------------------------------
# Tag vocabulary (phase 3 step 7).
#
# One table of tag kinds, each entry carrying the plugins that use it:
#
# * quality: NC's ``_QUALITY`` picture tokens that describe the feed
#   (``reference/dispatcharr-channel-name-cleaner/channel_name_cleaner/plugin.py:39``)
#   plus its ``\d{3,4}p`` bracket forms (:42) and ``UNICODE_QUALITY``
#   superscript marks (:46); IC's ``STANDARD_QUALITY_TAGS``
#   (``reference/Dispatcharr-IPTV-Checker-Plugin/iptv_checker/plugin.py:3325``);
#   CM's ignored-tag picture tokens in ``DEFAULT_IGNORED_TAGS``
#   (``reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/plugin.py:104``);
#   LU's name-fallback tags in ``_get_quality_tier``
#   (``reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/plugin.py:1651``).
# * junk: NC's ``_QUALITY`` tokens that describe packaging rather than the
#   picture (VIP, MULTI, BACKUP, GEO, LOW, DUAL, HQ, LQ), same line as above.
# * status: crosstalk/IC/CM tags: IC's ``STANDARD_STATUS_TAGS``
#   (``plugin.py:3324``: DEAD, Slow, Blank), CM's ``[Unk]``
#   (``DEFAULT_UNKNOWN_SUFFIX``, ``plugin.py:103``) and ``[Slow]``/``[Dead]``
#   ignored tags (:104), the guide's ``[BadEPG]`` (``crates/guide/src/settings.rs``
#   default ``bad_epg_suffix``).
# * regional: SM's ``extract_tags`` regions
#   (``reference/Stream-Mapparr/Stream-Mapparr/fuzzy_matcher.py``:
#   East/West/Pacific/Central/Mountain/Atlantic), re-attached by CM's
#   ``build_final_channel_name`` (``fuzzy_matcher.py:1506``).
# * timeshift: the ``+N`` markers LU's and EPG Janitor's ``_PLUS_SHIFT_RE``
#   (``Lineuparr/fuzzy_matcher.py:46``, ``EPG-Janitor/fuzzy_matcher.py:99``)
#   and NC's ``normalize_plus_one`` (``plugin.py:179``) handle.
# * country_prefix: the anchored ``CC:``/``[CC]``/``(CC)``/``CC_`` marker
#   codes, union of SM's ``KNOWN_COUNTRY_CODES``
#   (``reference/Stream-Mapparr/Stream-Mapparr/country.py:29``) and LU's
#   ``_KNOWN_COUNTRY_CODES``
#   (``reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/fuzzy_matcher.py:82``).
#
# Tokens NC lumps together but the vocabulary splits (VIP/MULTI/... are junk,
# not quality) are recorded under their vocabulary kind with source NC: the
# normalizer keeps consuming its own decided subsets (step 6, N01-N14), and
# this table is the superset the writers (cleaning, tagging, templates,
# logos) classify with.
# ---------------------------------------------------------------------------

# SM's country vocabulary (country.py:29 KNOWN_COUNTRY_CODES), cited above.
SM_KNOWN_COUNTRY_CODES = [
    "AL", "AR", "AT", "AU", "AZ", "BE", "BG", "BR", "CA", "CH",
    "CO", "CR", "CY", "CZ", "DE", "DK", "DO", "ES", "FI", "FR",
    "GR", "HR", "HU", "IE", "IL", "IN", "IR", "IT", "JP", "KR",
    "MK", "MX", "NL", "NO", "NZ", "PH", "PL", "PT", "RO", "RS",
    "RU", "SE", "TH", "TR", "UK", "US",
]

# LU's country vocabulary (fuzzy_matcher.py:82 _KNOWN_COUNTRY_CODES).
LU_KNOWN_COUNTRY_CODES = [
    "US", "UK", "CA", "AU", "DE", "FR", "IT", "ES", "NL", "BR",
    "MX", "IN", "IE", "SE", "NO", "DK", "PT", "PL", "AT", "CH",
    "BE", "FI", "TR", "GR", "IR", "AL", "BG", "RO", "RU", "AZ",
    "HR", "TH", "RS", "MK", "IL", "CO", "CR", "CY", "JP", "KR",
    "CZ", "HU", "NZ", "PH", "VN", "PK", "SI", "ET", "QA",
]

TAG_KINDS = {
    "quality": "picture/format tokens: IC format suffixes, NC picture marks, "
    "CM ignored picture tags, LU tier name tags; including resolution "
    "(1080p/720p/\\d{3,4}p), 4K/8K, superscript marks, HEVC/RAW",
    "junk": "packaging tokens NC strips as junk-only segments: "
    "VIP/MULTI/BACKUP/GEO/LOW/DUAL/HQ/LQ",
    "status": "state tags writers append: IC/CM [DEAD]/[Slow]/[Blank], "
    "CM [Unk], guide [BadEPG], IC format suffixes also live under quality",
    "regional": "feed-region words SM extracts and CM re-attaches: "
    "East/West/Pacific/Central/Mountain/Atlantic",
    "timeshift": "time-shift markers (+1/+2) LU/EJ route on and NC normalizes",
    "country_prefix": "anchored CC:/[CC]/(CC)/CC_/bare markers; the code list "
    "is the SM+LU union, delimiter forms are recognized by the classifier",
}


def merge_tags():
    """Build the unified tag vocabulary table."""
    entries = []  # (token, kind)
    sources = {}  # token -> set(sources)

    def add(token, kind, *plugs):
        entries.append((token, kind))
        sources.setdefault((token, kind), set()).update(plugs)

    # Quality picture tokens (NC _QUALITY picture subset + resolution +
    # superscripts; IC STANDARD_QUALITY_TAGS; CM ignored picture tags; LU
    # name-fallback tags).
    for token in ["HD", "FHD", "UHD", "4K", "8K", "HEVC", "H.265",
                  "H.264", "RAW", "1080p", "720p", "480p", "2160p",
                  "FHD1080", "SD",
                  # UNICODE_QUALITY (plugin.py:51), codepoint for codepoint:
                  # ᵁᴴᴰ ᴴᴰ ᴿᴬᵂ ᴴᴱᵛᶜ ⁴ᴷ ᔕᗪ.
                  "\u1d41\u1d34\u1d30", "\u1d34\u1d30",
                  "\u1d3f\u1d2c\u1d42", "\u1d34\u1d31\u1d5b\u1d9c",
                  "\u2074\u1d37", "\u1515\u15ea"]:
        add(token, "quality", "NC")
    for token in ["UHD", "FHD", "HD", "SD", "Unknown"]:
        add(token, "quality", "IC")
    for token in ["4K", "FHD", "HD", "SD", "Unknown"]:
        add(token, "quality", "CM")
    for token in ["4K", "UHD", "FHD", "1080", "HD", "SD"]:
        add(token, "quality", "LU")
    # Junk packaging tokens (NC _QUALITY non-picture subset).
    for token in ["VIP", "MULTI", "MULTI-AUDIO", "MULTIAUDIO", "HQ", "LQ", "LOW",
                  "DUAL", "GEO", "BACKUP"]:
        add(token, "junk", "NC")
    # Status tags (IC STANDARD_STATUS_TAGS; CM unknown/ignored; guide).
    for token in ["DEAD", "Slow", "Blank"]:
        add(token, "status", "IC")
    add("DEAD", "status", "CM")
    add("Slow", "status", "CM")
    add("Unk", "status", "CM")
    add("BadEPG", "status", "guide")
    # Regional words (SM extract_tags; CM build_final_channel_name).
    for token in ["East", "West", "Pacific", "Central", "Mountain",
                  "Atlantic"]:
        add(token, "regional", "SM", "CM")
    # Timeshift markers (LU/EJ _PLUS_SHIFT_RE; NC normalize_plus_one).
    for token in ["+1", "+2"]:
        add(token, "timeshift", "EJ", "NC", "LU")
    # Country prefix codes: the SM + LU union.
    sm_codes = set(SM_KNOWN_COUNTRY_CODES)
    lu_codes = set(LU_KNOWN_COUNTRY_CODES)
    for code in sorted(sm_codes | lu_codes):
        plugs = []
        if code in sm_codes:
            plugs.append("SM")
        if code in lu_codes:
            plugs.append("LU")
        add(code, "country_prefix", *plugs)

    seen = set()
    tags = []
    for token, kind in entries:
        if (token, kind) in seen:
            continue
        seen.add((token, kind))
        tags.append({
            "token": token,
            "kind": kind,
            "sources": sorted(sources[(token, kind)]),
        })
    tags.sort(key=lambda item: (item["kind"], item["token"].casefold()))
    counts = {}
    for item in tags:
        counts[item["kind"]] = counts.get(item["kind"], 0) + 1
    provenance = {
        "tags_out": len(tags),
        "counts": counts,
    }
    return {"kinds": TAG_KINDS, "tags": tags}, provenance


def merge_alias_scope(labeled_tables):
    """Merge one alias scope; returns (merged, conflicts).

    Every claim is kept: the tables map a canonical to the names it accepts,
    so an alias under two canonicals means both accept it, which is what each
    plugin does today. Multi-canonical aliases are returned as conflicts for
    step 5 to decide reverse (alias -> canonical) lookups. A conflict is
    ``agreed`` when every plugin that carries a claiming canonical at all
    lists the alias under it (a plugin lacking the canonical is silent, not
    dissenting), else ``disputed``.
    """
    claims = {}  # alias -> {canonical: set(plugins)}
    carriers = {}  # canonical -> set(plugins that have the key)
    for plug, table in labeled_tables:
        for canonical, variants in table.items():
            carriers.setdefault(canonical, set()).add(plug)
            for variant in variants:
                claims.setdefault(variant, {}).setdefault(canonical, set()).add(plug)
    merged = {}
    conflicts = []
    for alias in sorted(claims):
        canonicals = claims[alias]
        for canonical, plugs in canonicals.items():
            merged.setdefault(canonical, []).append(
                {"alias": alias, "sources": sorted(plugs)}
            )
        if len(canonicals) == 1:
            continue
        dissent = {
            canonical: sorted(carriers[canonical] - plugs)
            for canonical, plugs in sorted(canonicals.items())
            if carriers[canonical] - plugs
        }
        folded = alias.casefold().strip()
        matching = [c for c in canonicals if c.casefold().strip() == folded]
        conflicts.append(
            {
                "alias": alias,
                "claimed_by": {
                    canonical: sorted(plugs)
                    for canonical, plugs in sorted(canonicals.items())
                },
                "status": "disputed" if dissent else "agreed",
                # Plugins carrying the canonical but NOT listing this alias.
                "omitted_by": dissent,
                "self_name": matching[0] if len(matching) == 1 else None,
            }
        )
    for canonical in merged:
        merged[canonical] = sorted(merged[canonical], key=lambda item: item["alias"])
    return merged, conflicts


def merge_aliases(root):
    """Union the four alias tables plus the per-country override tables."""
    inputs = []
    tables = {}
    country_tables = {}
    specs = [
        ("JAN", os.path.join(JAN_DIR, "aliases.py")),
        ("SM", os.path.join(SM_DIR, "aliases.py")),
        ("CM", os.path.join(CM_DIR, "aliases.py")),
        ("LU", os.path.join(LU_DIR, "aliases.py")),
    ]
    for plug, rel in specs:
        inputs.append(
            {
                "path": rel,
                "sha256": sha256_file(os.path.join(root, rel)),
                "header": None,
                "kind": "aliases",
                "plugin": plug,
            }
        )
        global_table, country = load_alias_tables(os.path.join(root, rel))
        tables[plug] = global_table
        for code, table in country.items():
            country_tables.setdefault(code, {})[plug] = table

    merged, conflicts = merge_alias_scope([(plug, tables[plug]) for plug in PLUGINS])
    merged_countries = {}
    country_conflicts = {}
    for code in sorted(country_tables):
        scope = [
            (plug, country_tables[code][plug]) for plug in sorted(country_tables[code])
        ]
        country_merged, country_conf = merge_alias_scope(scope)
        merged_countries[code] = country_merged
        country_conflicts[code] = country_conf

    rows_in = {plug: sum(len(v) for v in tables[plug].values()) for plug in PLUGINS}
    canon_in = {plug: len(tables[plug]) for plug in PLUGINS}
    provenance = {
        "canonicals_in": canon_in,
        "aliases_in": rows_in,
        "canonicals_out": len(merged),
        "aliases_out": sum(len(v) for v in merged.values()),
        "conflict_count": len(conflicts),
        "disputed_count": sum(1 for c in conflicts if c["status"] == "disputed"),
        "self_name_count": sum(1 for c in conflicts if c["self_name"]),
        "countries": {
            code: {
                "canonicals_out": len(merged_countries[code]),
                "aliases_out": sum(len(v) for v in merged_countries[code].values()),
                "conflict_count": len(country_conflicts[code]),
            }
            for code in merged_countries
        },
    }
    output = {"aliases": merged, "country_aliases": merged_countries}
    # Step 5's input: every multi-canonical alias, global and per country.
    detail = {
        "conflicts": conflicts,
        "country_conflicts": country_conflicts,
    }
    return output, provenance, detail, inputs


def copy_lineups(root, out_dir):
    """Copy Lineuparr's lineup files verbatim; returns provenance + total."""
    names = sorted(
        name
        for name in os.listdir(os.path.join(root, LU_DIR))
        if name.endswith("_lineup.json")
    )
    dest_dir = os.path.join(out_dir, "lineups")
    os.makedirs(dest_dir, exist_ok=True)
    files = []
    total_channels = 0
    total_categories = 0
    for name in names:
        src = os.path.join(root, LU_DIR, name)
        with open(src, "rb") as handle:
            blob = handle.read()
        with open(os.path.join(dest_dir, name), "wb") as handle:
            handle.write(blob)
        data = json.loads(blob.decode("utf-8"))
        channels = sum(len(items) for items in data.get("categories", {}).values())
        total_channels += channels
        total_categories += len(data.get("categories", {}))
        files.append(
            {
                "path": os.path.join(LU_DIR, name),
                "sha256": hashlib.sha256(blob).hexdigest(),
                "header": data.get("date"),
                "channels": channels,
                "categories": sorted(data.get("categories", {}).keys()),
            }
        )
    # Remove stale files that are no longer upstream (idempotent clean copy).
    for name in os.listdir(dest_dir):
        if name.endswith(".json") and name not in names:
            os.remove(os.path.join(dest_dir, name))
    return (
        {
            "files": files,
            "file_count": len(files),
            "channels": total_channels,
            "categories": total_categories,
        },
        total_channels,
    )


LINEUPS_README = """# Merged lineup files

Lineuparr's {file_count} lineup files, copied byte-for-byte from
`reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/*_lineup.json` by
`scripts/data/build_merged.py` (hashes in `../PROVENANCE.json`). Do not
hand-edit: rerunning the script reproduces this directory byte for byte
(`build_merged.py --check`, enforced in CI).

## Schema

Each file is one object with `package` (display name), `date` (`YYYY-MM-DD`
file version), `description`, `source`, `notes` (free text; `source`/`notes`
may be absent), and `categories` mapping a category name to a channel list.
A channel always has `name` (verbatim lineup name) and `number` (lineup
channel number); optional extras seen in the wild: `aliases` (accepted
alternate names, e.g. TZ files), `addon` (package tag, e.g. AU/CA/US provider
files), `notes` (free text, e.g. part-time or regional notes), `epg_ids`
(ephemeral guide ids on a few entries). {file_count} files, {channels}
channels total (see `../PROVENANCE.json` `lineups` and
`docs/data-merge-report.md`).

## Country

The owning country is the filename's `CC_` prefix (`US_`, `UK_`, `DO_`,
`CA_`, `AU_`, `ES_`, `FR_`, `NL_`, `PE_`, `QA_`, `TZ_`). `US_Combined` is a
de-duplicated union of three US provider lineups (DIRECTV Premier, DISH
Top 250, Verizon FiOS); `TZ_Combined` likewise for Tanzania.
"""


def write_lineups_readme(out_dir, file_count, channels):
    """Emit the lineup schema doc; part of the reproducible output."""
    path = os.path.join(out_dir, "lineups", "README.md")
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            LINEUPS_README.format(file_count=file_count, channels=channels)
        )


def build(root, out_dir):
    """Run the full merge; returns (provenance, total_bytes)."""
    os.makedirs(os.path.join(out_dir, "channels"), exist_ok=True)
    channels, channels_prov, channel_inputs = merge_channels(root)
    for code, payload in channels.items():
        write_compact(os.path.join(out_dir, "channels", f"{code}.json"), payload)
    # Remove country files with no upstream source left (none today; hygiene).
    live = {f"{code}.json" for code in channels}
    for name in os.listdir(os.path.join(out_dir, "channels")):
        if name.endswith(".json") and name not in live:
            os.remove(os.path.join(out_dir, "channels", name))

    stations, stations_prov, station_inputs = merge_stations(root)
    write_compact(os.path.join(out_dir, "stations.json"), stations)

    aliases, aliases_prov, aliases_detail, alias_inputs = merge_aliases(root)
    write_compact(os.path.join(out_dir, "aliases.json"), aliases)
    write_compact(os.path.join(out_dir, "alias_conflicts.json"), aliases_detail)

    tags, tags_prov = merge_tags()
    write_compact(os.path.join(out_dir, "tags.json"), tags)

    lineups_prov, _ = copy_lineups(root, out_dir)
    write_lineups_readme(out_dir, lineups_prov["file_count"], lineups_prov["channels"])

    provenance = {
        "inputs": channel_inputs
        + station_inputs
        + alias_inputs
        + [
            {
                "path": item["path"],
                "sha256": item["sha256"],
                "header": item["header"],
                "kind": "lineup",
                "plugin": "LU",
            }
            for item in lineups_prov["files"]
        ],
        "channels": channels_prov,
        "stations": stations_prov,
        "aliases": aliases_prov,
        "tags": tags_prov,
        "lineups": lineups_prov,
    }

    # Size of everything except PROVENANCE.json itself (which records it).
    total_bytes = 0
    for dirpath, _, filenames in os.walk(out_dir):
        for name in filenames:
            if dirpath == out_dir and name == "PROVENANCE.json":
                continue
            total_bytes += os.path.getsize(os.path.join(dirpath, name))
    provenance["total_bytes"] = total_bytes
    write_compact(os.path.join(out_dir, "PROVENANCE.json"), provenance)
    return provenance, total_bytes, aliases_detail


def render_report(provenance, total_bytes, conflicts):
    """Render docs/data-merge-report.md; every number comes from the build."""
    lines = []
    add = lines.append
    channels = provenance["channels"]
    stations = provenance["stations"]
    aliases = provenance["aliases"]
    lineups = provenance["lineups"]

    total_in = sum(sum(v["rows_in"].values()) for v in channels.values())
    total_out = sum(v["rows_out"] for v in channels.values())
    dupes = sum(v["duplicates_merged"] for v in channels.values())
    chan_conflicts = sum(len(v["field_conflicts"]) for v in channels.values())
    case_total = sum(len(v["case_variants"]) for v in channels.values())
    sin = stations["rows_in"]
    ain = aliases["aliases_in"]
    fcc = stations["field_conflict_counts"]

    # Version relationship of CM against JAN/SM, per country.
    older, equal = [], []
    for code in sorted(channels):
        inputs = channels[code]["inputs"]
        if "CM" not in inputs:
            continue
        others = [parse_version(i["version"]) for p, i in inputs.items() if p != "CM"]
        if not others:
            continue
        mine = parse_version(inputs["CM"]["version"])
        (older if mine < max(others) else equal).append(code)

    add("# Merged data set: provenance and conflict report")
    add("")
    add("Phase 3 step 4 (`plans/phase-3-lineup.md`): one merged data set built by")
    add("`scripts/data/build_merged.py` from the four plugins' shipped tables.")
    add("Generated file; rerunning the script reproduces `data/merged/` and this")
    add("report byte for byte (`build_merged.py --check`, enforced in CI).")
    add("")
    add("## Summary")
    add("")
    add(
        f"- Channel databases: {len(channels)} countries, "
        f"{total_in} rows in -> {total_out} entries out "
        f"({dupes} exact duplicates merged, {chan_conflicts} field conflicts, "
        f"{case_total} case-variant groups kept apart)."
    )
    add(
        f"- Station table: CM {sin['CM']} + supplemental {sin['CM-supplemental']} "
        f"-> {stations['rows_out']} stations out; SM's {sin['SM']} rows are CM's "
        f"predecessor table: {stations['field_conflict_count']} differing fields "
        f"(CM wins all), {stations['sm_only_count']} SM-only rows dropped as "
        "cancelled licences."
    )
    add(
        f"- Callsign allowlist: {stations['allowlist_in']} Janitor callsigns folded in."
    )
    add(
        f"- Alias table: {sum(ain.values())} alias rows in "
        f"(JAN {ain['JAN']}, SM {ain['SM']}, CM {ain['CM']}, LU {ain['LU']}) -> "
        f"{aliases['aliases_out']} (canonical, alias) rows out across "
        f"{aliases['canonicals_out']} canonicals, every claim kept. "
        f"{aliases['conflict_count']} aliases sit under two or more canonicals "
        f"({aliases['disputed_count']} disputed between plugins, "
        f"{aliases['conflict_count'] - aliases['disputed_count']} agreed by all; "
        f"{aliases['self_name_count']} with a self-name hint), listed in "
        "`data/merged/alias_conflicts.json` for step 5."
    )
    for code, info in aliases["countries"].items():
        add(
            f"  - {code}: {info['aliases_out']} aliases, "
            f"{info['conflict_count']} multi-canonical."
        )
    add(
        f"- Lineup files: {lineups['file_count']} files, "
        f"{lineups['channels']} channels, copied verbatim."
    )
    size_mb = total_bytes / (1024 * 1024)
    add(
        f"- Size of `data/merged/` (excluding `PROVENANCE.json`): {total_bytes} "
        f"bytes ({size_mb:.2f} MiB)."
    )
    add("")
    add("## Dedup key decisions")
    add("")
    add("- Channels: the verbatim `channel_name` string. Evidence: across all")
    add("  plugin files no two entries sharing a name disagree on")
    add("  `category`/`type` (the only cross-plugin field difference is SM's")
    add("  `zones` array, which JAN/CM simply lack), and every in-file duplicate")
    add("  is an identical row. Case-only variants exist and are NOT duplicates:")
    add("  e.g. US `Afro` (Entertainment/National) vs `AFRO` (United")
    add("  States/Regional); they stay separate entries, as in the sources, and")
    add("  are listed under `channels.<CC>.case_variants` in `PROVENANCE.json`")
    add("  (the normalizer decisions of step 5 decide how they fold). A")
    add("  normalised key here would merge distinct entries. Duplicates collapse")
    add("  into one entry carrying a sorted `sources` list.")
    add("- Field conflicts: newest source wins, comparing each file's")
    add("  `YYYY-MM-DD[-vN]` version header. JAN and SM share every version and")
    add("  every name set. CM is older in " + ", ".join(older) + " and equal in")
    add("  " + ", ".join(equal) + "; everywhere its names are a strict subset")
    add("  with identical field values. In practice this only selects the header.")
    add("- Stations: Channel-Mapparr's table as shipped (FCC LMS dump of")
    add("  2026-08-10, plus `networks_supplemental.json` and")
    add("  `networks_corrections.json`; corrections are verified before applying,")
    add("  so a stale table fails the build). Evidence for the per-field rule:")
    add("  SM's `networks.json` (last changed 2025-10-01) is byte-identical to")
    add("  CM's table at `2d34bc3`, the \"previous file\" CM's")
    add("  `scripts/build_networks_json.py` rebuilt from. That build keeps every")
    add("  predecessor station unless its licence is cancelled, takes each field")
    add("  from the dated dump, and refuses to replace a specific network with a")
    add("  vague one (none of the differing affiliations has CM vague and SM")
    add("  specific). So every differing field is the newer FCC value and CM")
    add("  wins all fields; SM curated nothing CM lacks. The SM-only rows are the")
    add("  rebuild's cancelled-licence drops (FCC `D`-prefixed republications of")
    add("  dead callsigns, plus KVTV) and are not resurrected. The build pins")
    add("  SM's sha256 and stops if it changes. Janitor's allowlist callsigns are")
    add("  folded in as the `callsign_allowlist` array; every one resolves to a")
    add("  merged station under the core `normalize_callsign` base form (pinned")
    add("  by a test in `crosstalk-matching::data::merged`).")
    add("- Aliases: union of every claim. The tables map a canonical to the")
    add("  names it accepts, so an alias under two canonicals (`FS 1` under `FS1`")
    add("  and `Fox Sports 1`) means both accept it; all four plugins do exactly")
    add("  that today, and dropping such aliases would change behaviour every")
    add("  plugin agrees on. They are listed in `alias_conflicts.json`: `agreed`")
    add("  when every plugin carrying a claiming canonical lists the alias under")
    add("  it, `disputed` otherwise (`omitted_by` names the dissenters), with a")
    add("  `self_name` hint when the alias casefold-equals exactly one claimant.")
    add("  Step 5 decides each for reverse (alias -> canonical) lookups. Country")
    add("  tables merge independently per country code.")
    add("")
    add("## Size")
    add("")
    add(f"`data/merged/` totals {total_bytes} bytes ({size_mb:.2f} MiB) without")
    us = channels.get("US", {}).get("rows_out", 0)
    add(f"`PROVENANCE.json`; the US database dominates ({us} entries). Output")
    add("is compact JSON (no pretty-print). `Dockerfile` already does")
    add("`COPY ./data`, so the merged set ships in the image unchanged. The")
    add("Rust loaders (`crosstalk-matching::data::merged`) embed the tables with")
    add("`include_str!` and parse lazily (`OnceLock`); until step 6 retires the")
    add("per-plugin tables (`data/channels`, `data/stream-mapparr`, ~8.8 MiB")
    add("already embedded) nothing outside tests references the merged")
    add("constants, so release binaries do not carry them yet.")
    add("")
    add("## Channels per country")
    add("")
    add(
        "| CC | files | rows in | rows out | duplicates | field conflicts "
        "| case groups | unique to |"
    )
    add(
        "|----|-------|---------|----------|------------|-----------------"
        "|-------------|-----------|"
    )
    for code in sorted(channels):
        prov = channels[code]
        files = ",".join(sorted(prov["inputs"]))
        rows_in = sum(prov["rows_in"].values())
        uniq = ",".join(
            f"{plug}:{count}"
            for plug, count in sorted(prov["unique_counts"].items())
            if count
        ) or "—"
        add(
            f"| {code} | {files} | {rows_in} | {prov['rows_out']} | "
            f"{prov['duplicates_merged']} | {len(prov['field_conflicts'])} | "
            f"{len(prov['case_variants'])} | {uniq} |"
        )
    add("")
    for code in sorted(channels):
        prov = channels[code]
        if prov["field_conflicts"]:
            add(f"### {code} channel field conflicts")
            add("")
            for conflict in prov["field_conflicts"]:
                add(
                    f"- `{conflict['channel_name']}` "
                    f"{conflict['field']}: {conflict['values']} "
                    f"-> winner `{conflict['winner']}`"
                )
            add("")
    add("Entries unique to one plugin are listed per country in")
    add("`PROVENANCE.json` (`channels.<CC>.unique_to`). CM contributes no name")
    add("of its own anywhere; the only single-source databases are SM's")
    add("DO/PE/PT. Examples:")
    add("")
    for code in sorted(channels):
        prov = channels[code]
        for plug in sorted(prov["unique_to"]):
            names = prov["unique_to"][plug]
            if names:
                sample = ", ".join(f"`{n}`" for n in names[:8])
                extra = f" (+{len(names) - 8} more)" if len(names) > 8 else ""
                add(f"- {code} only in {plug} ({len(names)}): {sample}{extra}")
    add("")
    add("## Station field differences (CM wins, both values kept)")
    add("")
    add(
        f"{stations['field_conflict_count']} differing fields between CM and its "
        "predecessor (SM's file), by field/kind: "
        + ", ".join(f"{k} {v}" for k, v in fcc.items())
        + ". `case` = differs only in letter case. Sample:"
    )
    add("")
    value_rows = [c for c in stations["field_conflicts"] if c["kind"] == "value"]
    for conflict in value_rows[:25]:
        add(
            f"- `{conflict['callsign']}` {conflict['field']}: "
            f"SM `{conflict['SM']}` -> CM `{conflict['CM']}`"
        )
    if stations["field_conflict_count"] > 25:
        add(
            f"- ... all {stations['field_conflict_count']} in `PROVENANCE.json` "
            "(`stations.field_conflicts`)"
        )
    add("")
    add(
        f"SM-only stations dropped ({stations['sm_only_count']}): "
        + ", ".join(f"`{c}`" for c in stations["sm_only_dropped"])
        + "."
    )
    add("")
    add("## Multi-canonical aliases (kept under every claimant; step 5 decides)")
    add("")
    add(
        f"{aliases['conflict_count']} alias values sit under two or more "
        f"canonicals; {aliases['disputed_count']} are disputed between plugins. "
        "Machine-readable: `data/merged/alias_conflicts.json`."
    )
    add("")
    add("| alias | status | claimants (plugins) | omitted by | self-name |")
    add("|-------|--------|---------------------|------------|-----------|")
    for conflict in conflicts["conflicts"]:
        claims = "; ".join(
            f"`{canon}` ({','.join(plugs)})"
            for canon, plugs in conflict["claimed_by"].items()
        )
        omitted = "; ".join(
            f"`{canon}`: {','.join(plugs)}"
            for canon, plugs in conflict["omitted_by"].items()
        ) or "—"
        self_name = f"`{conflict['self_name']}`" if conflict["self_name"] else "—"
        add(
            f"| `{conflict['alias']}` | {conflict['status']} | {claims} | "
            f"{omitted} | {self_name} |"
        )
    for code, items in conflicts["country_conflicts"].items():
        for conflict in items:
            claims = "; ".join(
                f"`{canon}` ({','.join(plugs)})"
                for canon, plugs in conflict["claimed_by"].items()
            )
            add(f"| `{conflict['alias']}` ({code}) | {conflict['status']} | {claims} | | |")
    add("")
    add("## Inputs")
    add("")
    add("| file | sha256 | header |")
    add("|------|--------|--------|")
    for item in provenance["inputs"]:
        short = item["sha256"][:16] + "..."
        add(f"| {item['path']} | {short} | {item['header'] or '—'} |")
    add("")
    return "\n".join(lines)


def read_bytes(path):
    with open(path, "rb") as handle:
        return handle.read()


def compare_trees(built, expected):
    """List byte-level differences between two directory trees."""
    problems = []
    for dirpath, _, filenames in os.walk(built):
        for name in filenames:
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, built)
            other = os.path.join(expected, rel)
            if not os.path.exists(other):
                problems.append(f"missing file: {rel}")
            elif read_bytes(full) != read_bytes(other):
                problems.append(f"content differs: {rel}")
    for dirpath, _, filenames in os.walk(expected):
        for name in filenames:
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, expected)
            if not os.path.exists(os.path.join(built, rel)):
                problems.append(f"stale extra file: {rel}")
    return sorted(problems)


def render_report_text(provenance, total_bytes, conflicts):
    report = render_report(provenance, total_bytes, conflicts)
    return report if report.endswith("\n") else report + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build the merged data set.")
    parser.add_argument("--root", default=None)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    if not sys.flags.isolated:
        # The plugin checkouts sit under the root; -I keeps any of their
        # modules from shadowing the stdlib if the cwd or path changes.
        print("run with python3 -I", file=sys.stderr)
        return 2
    root = args.root or os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    out_dir = os.path.join(root, OUT_DIR)
    report_path = os.path.join(root, REPORT_PATH)
    if args.check:
        with tempfile.TemporaryDirectory() as tmp:
            staged = os.path.join(tmp, "merged")
            provenance, total_bytes, conflicts = build(root, staged)
            problems = compare_trees(staged, out_dir)
            report = render_report_text(provenance, total_bytes, conflicts)
            if not os.path.exists(report_path):
                problems.append(f"missing file: {REPORT_PATH}")
            elif read_bytes(report_path) != report.encode("utf-8"):
                problems.append(f"content differs: {REPORT_PATH}")
            if problems:
                print(
                    "data/merged/ is stale; rerun build_merged.py:", file=sys.stderr
                )
                for problem in problems:
                    print(f"  {problem}", file=sys.stderr)
                return 1
            print(
                "data/merged/ and the report reproduce byte for byte "
                f"({total_bytes} bytes + PROVENANCE.json)"
            )
            return 0
    provenance, total_bytes, conflicts = build(root, out_dir)
    report = render_report_text(provenance, total_bytes, conflicts)
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(report)
    print(f"wrote {out_dir} ({total_bytes} bytes), {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
