#!/usr/bin/env python3
"""Oracle diff report for step 17: lineup create vs Channel-Mapparr.

Lineup `create_*` makes channels on the unified engine (one identity per
stream, one channel per identity family carrying the family's streams; one
channel per OTA station with no streams attached); Channel-Mapparr is the
oracle. This script runs CM's real `import_m3u_streams` decision path (the
`oracles.py` CM path — its `match_broadcast_channel` OTA ladder, its
`get_candidates` + `fuzzy_match` premium path — plus its group/category filters,
`_detect_duplicate_channels` and max+1 numbering, ported verbatim with line
citations) and its real `channel_seeder.build_seed_plan` /
`allocate_channel_numbers` (imported directly — pure and Django-free) against
the Rust side (`cargo run -p crosstalk-lineup --example create_dump`, the same
`decide_streams` / `decide_stations` the action plans from), attributes every
difference, and writes `docs/oracle-reports/lineup-create.md`.

Usage:
    python3 -I scripts/oracle_report_lineup_create.py [--check] [--show N]

    --check   regenerate into a temp file and fail unless it is byte-identical
              to the committed report (the CI gate: the report reproduces).
    --show N  differences printed per bucket (default 15).

CM's streams path per stream (`plugin.py:3342-4041`), with defaults
(`channel_databases = "US"`, normal sensitivity = 80):

1. `_fetch_streams_from_m3u_sources`: streams of the named M3U accounts in
   priority order (empty = every active account); `m3u_group_filter` BEFORE
   matching (resolved group name, case-insensitive).
2. `_match_streams_to_categories`: OTA first (category `Broadcast`), else
   premium (the same `get_candidates` + `fuzzy_match` calls the names oracle
   drives — an exact name scores 100 — with the DB record's category);
3. `m3u_category_filter` AFTER matching (case-insensitive).
4. `_ensure_category_groups_exist`: `m3u_custom_group_name` or one group per
   category.
5. `_import_matched_streams`: ONE channel per matched STREAM at max+1 step 1.0
   with that stream attached; `_detect_duplicate_channels` (`Name [N]`, then
   `Name [M3U-id]`) against live channels AND the batch — so a rerun imports
   every channel AGAIN under a new suffix.
6. `create_channels_from_streams`: unattached streams in `seed_source_groups`,
   OTA stations only, grouped by proposed name (`build_seed_plan`: existing
   names skip case-insensitively, `seed_exclude_networks` excludes, the rest
   are unresolved), numbers from `seed_start_number` via
   `allocate_channel_numbers` (skip used), no streams attached. A rerun finds
   the names taken and creates nothing (the seeder, unlike the import, does not
   duplicate).

Attribution buckets (every difference lands in exactly one, each by a CHECK —
nothing lands in a bucket by elimination; what no check explains is
`unattributed`):

- Invariant first (the phase goal: the same answer in every app), CHECKED
  per stream: create never creates from a name `names` would not name
  (blocked callsign, unmatched, an OTA template missing a field — whatever
  the unknown suffix); a created family's channel is the `names_dump` target
  of its head (`names::decide_target`, the one naming decision); its category
  is the `categorize_dump` category of its head (premium) or `Broadcast`
  (OTA); a family with no category is created only in a custom group. A
  violation is a bug and lands in `unattributed`.
- `family-grouping`: CM imports one channel per STREAM (duplicates across
  providers get `[M3U-id]` suffixes); Rust makes one channel per identity
  family carrying the family's streams. CHECKED: the stream's Rust family has
  more than one stream (or the stream joins a creatable head's family).
- `naming-via-names`: CM imports the raw stream name; create names the
  channel through names. CHECKED: CM's name is the stream name and Rust's is
  the names target.
- `category-via-categorize`: the category differs and Rust's is categorize's
  (or `Broadcast` for OTA), or Rust skips because categorize gives no
  category. CHECKED against `categorize_dump`; the categorize report
  attributes categorize vs CM.
- `country-category`: decision A — CM's category is a country label
  (`is_country`, the categorize oracle's one definition) and Rust has none.
- `engine-via-names`: CM's matcher and names' one decision disagree on
  whether the stream matches at all. CHECKED: Rust's decision equals
  `names_dump`'s target kind; docs/oracle-reports/lineup-names.md attributes
  CM vs names.
- `rerun-dedupe`: Rust skips because the identity was already created (owned
  key) or a live channel has the same identity. CHECKED on the skip note.
  CM's rerun duplicates are counted in the case header.
- `numbering`: the planned numbers are not max+1 step 1 skipping used
  (streams) / from `seed_start_number` skipping used (stations). CHECKED.
- `collision`: a same-named live channel exists and CM suffix-dedupes while
  the shared guard renames/skips. CHECKED on the live names.
- `seed-parity`: stations-mode structural diffs (grouping, skip, exclude,
  unresolved, attach) NOT explained by the checks above.
- `unattributed`: listed for review. A non-empty list is the review surface;
  the script still exits 0 (reports never fail CI) — but the step's bar is 0.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import subprocess
import sys

sys.dont_write_bytecode = True

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "scripts", "corpus"))
import oracles  # noqa: E402

sys.path.insert(0, os.path.join(REPO, "scripts"))
from oracle_report_lineup_categorize import is_country  # noqa: E402

CM_DIR = os.path.join(REPO, "reference", "Dispatcharr-Channel-Maparr-Plugin", "Channel-Maparr")
sys.path.insert(0, CM_DIR)
import channel_seeder  # noqa: E402
from station_affiliation import primary_network  # noqa: E402

REPORT = os.path.join(REPO, "docs", "oracle-reports", "lineup-create.md")
CM_THRESHOLD = 80
RUST_CUT = 95
OTA_FORMAT = "{NETWORK} - {STATE} {CITY} ({CALLSIGN})"
OTA_IMPORT_CATEGORY = "Broadcast"

_STREAM_NETWORKS = frozenset({
    "ABC", "CBS", "NBC", "FOX", "CW", "PBS", "ION",
    "MYTV", "MYNETWORKTV", "TELEMUNDO", "UNIVISION", "UNIMAS",
})


# ---------------------------------------------------------------------------
# CM's OTA helpers, ported verbatim (plugin.py citations in each docstring)
# ---------------------------------------------------------------------------

def extract_stream_network(channel_name):
    """`Plugin._extract_stream_network` (plugin.py:1745)."""
    if not channel_name:
        return None
    s = channel_name.strip()
    m_geo = re.match(r'^\s*[\[(]?([A-Za-z]{2,12})[\])]?\s*[:|]\s*(.*)$', s)
    if m_geo and m_geo.group(1).upper() not in _STREAM_NETWORKS:
        s = m_geo.group(2)
    m = re.match(r'([A-Za-z]{2,12})', s)
    if not m:
        return None
    token = m.group(1).upper()
    return token if token in _STREAM_NETWORKS else None


def parse_network_affiliation(network_affiliation):
    """`Plugin._parse_network_affiliation` (plugin.py:1694)."""
    if not network_affiliation:
        return None
    s = network_affiliation.strip()
    s = re.sub(r'^D\d+-', '', s)
    s = re.sub(r'^[KW][A-Z]{3,4}(?:-(?:TV|CD|LP|DT|LD))?\s+D\d+\s*-\s*', '', s)
    s = re.sub(r'\b(?:CH|CHANNEL)\s*\d+(?:\.\d+)?', ' ', s, flags=re.IGNORECASE)
    s = re.sub(r'\([^)]*\)', ' ', s)
    tokens = [t for t in re.split(r'[;,/&.]|\s+', s) if t.strip()]
    while tokens and re.fullmatch(
            r'[KW][A-Z]{2,3}(?:-(?:TV|CD|LP|DT|LD)\d*)?', tokens[0].upper()):
        tokens.pop(0)
    if not tokens:
        return None
    network = re.sub(
        r'\s+(?:Television\s+)?Network\s*$', '', tokens[0], flags=re.IGNORECASE).strip()
    network = network.upper()
    return network if network else None


def normalize_callsign(callsign):
    """`FuzzyMatcher.normalize_callsign`: strip the broadcast suffix."""
    return re.sub(r'-(?:TV|CD|LP|DT|LD)$', '', callsign or '')


def format_ota_name(station_data, format_string, callsign, network_override=None):
    """`Plugin._format_ota_name` (plugin.py:1772)."""
    required_fields = re.findall(r'\{(\w+)\}', format_string)
    network_raw = (station_data.get('network_affiliation') or '').strip()
    network = network_override or parse_network_affiliation(network_raw)
    city = (station_data.get('community_served_city') or '').title()
    state = (station_data.get('community_served_state') or '').upper()
    display_callsign = normalize_callsign(callsign)
    replacements = {
        'NETWORK': network,
        'CITY': city,
        'STATE': state,
        'CALLSIGN': display_callsign,
    }
    for field in required_fields:
        if field not in replacements or not replacements[field]:
            return None
    result = format_string
    for field, value in replacements.items():
        result = result.replace('{%s}' % field, value)
    return result


# ---------------------------------------------------------------------------
# CM decision paths
# ---------------------------------------------------------------------------

def cm_setup():
    """The CM oracle with its default US database (plugin.py:1665)."""
    oracle = oracles.get("cm")
    oracle.matcher.reload_databases(["US"])
    return oracle


def cm_premium_match(matcher, name, ignored):
    """CM's premium winner for `name` (oracles.py CM path, plugin.py:2008-2037)."""
    pre = matcher.get_candidates(name, ignored)
    if pre is None:
        ordered = list(matcher.premium_channels)
    else:
        members = set(pre)
        ordered = [candidate for candidate in matcher.premium_channels
                   if candidate in members]
    if not ordered:
        return (None, 0, None)
    return matcher.fuzzy_match(name, ordered, ignored)


def cm_match_stream(oracle, name, market_fallback=False):
    """One stream through `_match_streams_to_categories` (plugin.py:3396).

    Returns `(category, match_type, match_method, winner, callsign)`; category
    is None for unmatched streams.
    """
    ignored = list(oracles.RUN_IGNORED_TAGS["cm"])
    matcher = oracle.matcher
    stream_name = (name or "").strip()
    if not stream_name:
        return (None, "", "No match", None, None)
    network = extract_stream_network(stream_name)
    callsign, ota_station = matcher.match_broadcast_channel(
        stream_name, network=network, allow_market_fallback=market_fallback)
    if ota_station:
        return (OTA_IMPORT_CATEGORY, "Broadcast (OTA)",
                "Callsign: %s" % callsign, None, callsign)
    if callsign:
        # A found callsign blocks premium (plugin.py:2008) and has no station:
        # unmatched, like the rename path's blocked row.
        return (None, "none", "No match", None, callsign)
    # Premium: the same `get_candidates` + `fuzzy_match` calls the names oracle
    # drives (an exact name scores 100, a normalized one near it).
    matched, score, match_type = cm_premium_match(matcher, stream_name, ignored)
    if matched and score >= CM_THRESHOLD:
        record = next(
            (ch for ch in matcher.premium_channels_full
             if ch.get("channel_name") == matched), None)
        category = (record or {}).get("category") or "Entertainment"
        return (category, (record or {}).get("type") or "National",
                "Fuzzy: %s%% (%s)" % (score, match_type), matched, None)
    return (None, "", "No match", None, None)


def cm_detect_duplicate(channel_name, existing_names):
    """`Plugin._detect_duplicate_channels` (plugin.py:3773), verbatim."""
    lowered = {name.lower() for name in existing_names}
    if channel_name.lower() not in lowered:
        return (False, channel_name)
    counter = 1
    while True:
        unique_name = "%s [%d]" % (channel_name, counter)
        if unique_name.lower() not in lowered:
            return (True, unique_name)
        counter += 1


def cm_import(streams, matched_by_category, existing_channels, start_number):
    """`Plugin._import_matched_streams` (plugin.py:3798): one channel per STREAM.

    `streams` maps id -> {"name": ..., "m3u": ...}; `matched_by_category` maps
    category -> [stream ids]; `existing_channels` is a list of live names.
    Returns ([created], ...): created rows carry the final name, number, group
    and stream id, in CM's order (categories sorted, streams by name, priority).
    """
    existing = list(existing_channels)
    number = start_number
    created = []
    for category in sorted(matched_by_category):
        by_name = {}
        for sid in matched_by_category[category]:
            by_name.setdefault(streams[sid]["name"], []).append(sid)
        for base_name in sorted(by_name):
            # Priority order within one name (lower first), like the plugin.
            for sid in sorted(by_name[base_name],
                              key=lambda i: streams[i]["priority"]):
                dup, unique = cm_detect_duplicate(base_name, existing)
                if dup:
                    unique = "%s [M3U-%s-%s]" % (
                        base_name, streams[sid]["m3u"], sid)
                    _, unique = cm_detect_duplicate(unique, existing)
                created.append({"stream_id": sid, "channel_name": unique,
                                "number": number, "category": category,
                                "is_duplicate": dup})
                existing.append(unique)
                number += 1.0
    return created


def cm_seed_resolve(matcher, stream_name, ota_format, fallback, skip_networks, memo):
    """`Plugin._seed_resolver` (plugin.py:4530): (proposed, station)."""
    if stream_name not in memo:
        network = extract_stream_network(stream_name)
        callsign, station = matcher.match_broadcast_channel(
            stream_name, network=network, allow_market_fallback=fallback)
        name = None
        if station:
            name = format_ota_name(station, ota_format, callsign,
                                   network_override=network)
        memo[stream_name] = (name, station)
    return memo[stream_name]


def cm_seed_exclude(matcher, stream_name, skip_networks, memo):
    """The `exclude` half of `_seed_resolver`: primary network or a name word."""
    if not skip_networks:
        return False
    _, station = memo[stream_name]
    on_record = set()
    primary = primary_network((station or {}).get("network_affiliation", ""))
    if primary:
        on_record.add(primary)
    in_name = {word for word in re.split(r"[^A-Za-z]+", stream_name.upper()) if word}
    return bool(skip_networks & (on_record | in_name))


# ---------------------------------------------------------------------------
# Worlds
# ---------------------------------------------------------------------------


def representative_streams():
    """A representative stream world (seeded): the same station on two M3U
    accounts (CM's duplicate case), premium names plain and decorated, OTA
    station names, unmatched names — across two stream groups."""
    rng = random.Random(17)
    groups = ["Sports", "US| OTA"]
    accounts = ["Main Provider", "Backup Provider"]
    streams = []
    next_id = [101]

    def add(name, group=None, account=None):
        streams.append({
            "id": next_id[0], "name": name,
            "group": group if group is not None else rng.choice(groups),
            "m3u_account": account if account is not None else rng.choice(accounts),
        })
        next_id[0] += 1

    # The same station carried once per M3U account: CM imports N channels,
    # Rust imports one carrying every stream.
    for name in ["CNN", "CNN HD", "ESPN", "ESPN HD",
                 "CBS - AL Birmingham (WIAT)",
                 "CBS - AL Birmingham (WIAT) HD"]:
        add(name, "Sports", "Main Provider")
        add(name, "Sports", "Backup Provider")
    popular = ["Fox News Channel", "MSNBC", "HBO", "TNT", "USA Network",
               "A&E", "Discovery Channel", "Nickelodeon", "Syfy",
               "NFL Network", "The Weather Channel", "AMC"]
    for name in popular:
        add(name)
        add(rng.choice(["US: %s HD", "%s [FHD]", "%s (West)", "%s"]) % name)
    for name in ["Zzqz No Such Channel 999", "Qqx Obsolete Feed 12 HD",
                 "US: Zzqz HD", "KXYZ fakecast"]:
        add(name)
    return streams


def disputed_streams(limit=24):
    """Stream names mined from `data/corpus/pairs.jsonl` where CM disagrees
    with the label or the other plugins — exactly where a create diff hides."""
    with open(os.path.join(REPO, "data", "corpus", "pairs.jsonl"), encoding="utf-8") as fh:
        pairs = [json.loads(line) for line in fh]
    disputed = [
        pair for pair in pairs
        if pair["verdicts"].get("cm") is not None and (
            (pair["label"] == "different") == bool(pair["verdicts"]["cm"])
            or not all(pair["verdicts"].get(k) == pair["verdicts"]["cm"]
                       for k in ("janitor", "sm", "lu") if pair["verdicts"].get(k) is not None))
    ]
    disputed.sort(key=lambda pair: (pair["generator"], pair["query"], pair["candidate"]))
    streams = []
    seen = set()
    for pair in disputed[:limit]:
        for key in ("query", "candidate"):
            name = (pair.get(key) or "").strip()
            if name and name not in seen and len(name) <= 60:
                seen.add(name)
                streams.append({"id": 20000 + len(streams), "name": name,
                                "group": "Disputed", "m3u_account": "Main Provider"})
    return streams


def seed_world():
    """Streams for stations mode: two WIAT feeds (one attached), a national
    name, a blocked callsign, and a Telemundo station for the exclude case."""
    return [
        {"id": 201, "name": "CBS - AL Birmingham (WIAT)",
         "group": "US| CBS", "m3u_account": "Main Provider"},
        {"id": 202, "name": "CBS - AL Birmingham (WIAT) HD",
         "group": "US| CBS", "m3u_account": "Main Provider"},
        {"id": 203, "name": "ESPN HD",
         "group": "US| CBS", "m3u_account": "Main Provider"},
        {"id": 204, "name": "ABC (WZZZ)",
         "group": "US| CBS", "m3u_account": "Main Provider"},
    ]


# ---------------------------------------------------------------------------
# Rust side
# ---------------------------------------------------------------------------

def run_dump(example, payload):
    proc = subprocess.run(
        ["cargo", "run", "-q", "-p", "crosstalk-lineup", "--example", example],
        input=json.dumps(payload), capture_output=True, text=True, cwd=REPO)
    if proc.returncode != 0:
        raise RuntimeError("%s failed: %s" % (example, proc.stderr[-4000:]))
    return json.loads(proc.stdout)


def create_dump(world, settings, mode):
    """The action's own decisions over a stream world (no store, no network)."""
    groups = sorted({s["group"] for s in world} | {"Existing"})
    holder_streams = [201] if mode == "stations" else []
    payload = {
        "mode": mode,
        "channels": [{"id": 1, "name": "Holder", "group": "Existing",
                        "number": 50.0, "streams": holder_streams}],
        "groups": [{"id": 900 + i, "name": name} for i, name in enumerate(groups)],
        "streams": world,
        "accounts": [{"id": 1, "name": "Main Provider"},
                       {"id": 2, "name": "Backup Provider"}],
        "settings": settings,
        "streams_settings": {},
    }
    if mode == "stations":
        payload["settings"] = dict(settings, **{
            "seed_source_groups": "US| CBS",
            "seed_target_group": "OTA",
        })
    return run_dump("create_dump", payload)


def names_targets(names):
    """`names_dump` decisions for stream names: `(target_kind, target_name)` from
    the one naming decision (`names::decide_target`) names renames by."""
    channels = [{"id": 30000 + i, "name": name, "group": "Test"}
                for i, name in enumerate(names)]
    rows = run_dump("names_dump", {"channels": channels, "scope": {}, "settings": {}})
    return {row["old"]: (row["target_kind"], row["target_name"]) for row in rows}


def categorize_labels(names):
    """`categorize_dump` categories for stream names (None = no category)."""
    oracle_rows = run_dump(
        "categorize_dump",
        {"channels": [{"id": 40000 + i, "name": name, "group": "Test"}
                        for i, name in enumerate(names)],
         "scope": {}, "settings": {}})
    out = {}
    for row in oracle_rows:
        out[row["old"]] = row.get("category")
    return out


# ---------------------------------------------------------------------------
# Attribution: streams mode
# ---------------------------------------------------------------------------

STREAM_BUCKETS = ("family-grouping", "naming-via-names", "category-via-categorize",
                  "country-category", "engine-via-names", "rerun-dedupe",
                  "numbering", "collision", "unattributed")

# The kinds of the one naming decision create may create from.
CREATABLE_KINDS = ("ota", "premium")


def create_matches_names_and_categorize(rust_rows, names_map, cat_map):
    """The invariant (the main goal): create decides through names and
    categorize, never beside them. Per stream, CHECKED:

    - a stream names would not name (blocked callsign, unmatched, an OTA
      template missing a field) is never created, whatever the unknown suffix;
    - a created family's channel is the names target of its head stream;
    - a created family's category is categorize's for the head (premium) or
      `Broadcast` (OTA, this mode's mapping), and a premium family categorize
      gives no category is created only in a custom group.

    Returns the violations (each lands in `unattributed`: a violation is a bug,
    never a difference to explain).
    """
    heads = {}
    for row in rust_rows:
        if row["is_head"]:
            heads[row["family"]] = row
    violations = []
    for row in rust_rows:
        if not row["will_import"]:
            continue
        head = heads.get(row["family"])
        if head is None:
            violations.append((row, "an imported row has no family head"))
            continue
        head_kind, head_target = names_map.get(head["stream_name"], (None, None))
        if head_kind not in CREATABLE_KINDS:
            violations.append((row, "created from a %s name decision" % head_kind))
            continue
        if row["channel_name"] != head_target:
            violations.append((row, "channel %r != names target %r" % (
                row["channel_name"], head_target)))
            continue
        expected = (OTA_IMPORT_CATEGORY if head_kind == "ota"
                    else (cat_map.get(head["stream_name"]) or ""))
        if row["category"] != expected:
            violations.append((row, "category %r != categorize %r" % (
                row["category"], expected)))
            continue
        if not expected and not row["target_group"]:
            violations.append((row, "no category and no custom group, yet created"))
    return violations


def attribute_streams(cm, rust_rows, names_map, cat_map, live_names):
    """Attribute every stream's difference from CM, each by a CHECK.

    `cm` maps stream id -> {category|None, channel_name|None (CM's final name
    incl. suffixes), number|None, matched}. `rust_rows` is the create_dump rows
    list. Every aspect that differs (imported or not, channel name, category)
    must be explained; an aspect no check explains is `unattributed`.
    Per-stream numbers are not compared (CM numbers in category/name order,
    Rust in family order); the case-wide numbering check covers them.
    Returns (buckets, agreements).
    """
    rust = {row["stream_id"]: row for row in rust_rows}
    heads = {row["family"]: row for row in rust_rows if row["is_head"]}
    # A skipped family has no head row: its decision is its first stream's
    # (rows are in candidate order, the order the family was decided in).
    first = {}
    for row in rust_rows:
        first.setdefault(row["family"], row)
    buckets = {bucket: [] for bucket in STREAM_BUCKETS}
    agreements = 0
    lowered_live = {name.lower() for name in live_names}
    for row, why in create_matches_names_and_categorize(rust_rows, names_map, cat_map):
        buckets["unattributed"].append({
            "stream": row["stream_name"], "cm": cm.get(row["stream_id"]),
            "rust": {"violation": why}})
    violated = {item["stream"] for item in buckets["unattributed"]}
    for sid, decision in cm.items():
        row = rust[sid]
        if row["stream_name"] in violated:
            continue
        kind, target = names_map.get(row["stream_name"], (None, None))
        item = {"stream": row["stream_name"],
                "cm": decision, "rust": {
                    "will_import": row["will_import"],
                    "channel": row["channel_name"],
                    "category": row["category"],
                    "names": kind}}
        cm_imports = decision["channel_name"] is not None
        if not cm_imports and not row["will_import"]:
            agreements += 1
            continue
        if cm_imports and not row["will_import"]:
            note = row["note"]
            lead = first[row["family"]]["stream_name"]
            lead_kind = names_map.get(lead, (None, None))[0]
            if note.startswith(("key ", "a channel with the same identity")):
                # The rerun / foreign same-identity dedupe (the fix over CM).
                buckets["rerun-dedupe"].append(item)
            elif lead_kind not in CREATABLE_KINDS:
                # Names gives no target (CM's matcher does): create == names,
                # and docs/oracle-reports/lineup-names.md attributes CM vs names.
                buckets["engine-via-names"].append(item)
            elif "no group to create in" in note and not cat_map.get(lead):
                # Categorize gives no category: a country label (decision A) or
                # a family carrying none (the categorize report attributes it).
                if is_country(decision["category"]):
                    buckets["country-category"].append(item)
                else:
                    buckets["category-via-categorize"].append(item)
            else:
                buckets["unattributed"].append(item)
            continue
        if not cm_imports and row["will_import"]:
            # CM's matcher finds nothing; names' one decision does (the head's
            # target, checked above): the names report attributes CM vs names.
            if kind in CREATABLE_KINDS:
                buckets["engine-via-names"].append(item)
            else:
                # A family member names would not name joins the family's
                # channel (same identity family as a creatable head).
                head = heads.get(row["family"])
                head_kind = names_map.get(head["stream_name"], (None, None))[0] \
                    if head else None
                bucket = "family-grouping" if head_kind in CREATABLE_KINDS \
                    else "unattributed"
                buckets[bucket].append(item)
            continue
        # Both import: explain every aspect that differs.
        cm_name = decision["channel_name"]
        family_size = sum(1 for r in rust_rows if r["family"] == row["family"])
        explained = []
        if cm_name != row["channel_name"]:
            if family_size > 1:
                explained.append("family-grouping")
            elif row["stream_name"].lower() in lowered_live:
                explained.append("collision")
            elif cm_name == row["stream_name"] and row["channel_name"] == target:
                # CM imports the raw stream name; create names via names.
                explained.append("naming-via-names")
            else:
                explained.append("unattributed")
        if decision["category"] != row["category"]:
            if is_country(decision["category"]) and \
                    row["category"] == (cat_map.get(row["stream_name"]) or ""):
                explained.append("country-category")
            elif row["category"] == (OTA_IMPORT_CATEGORY if kind == "ota"
                                     else cat_map.get(row["stream_name"])):
                explained.append("category-via-categorize")
            else:
                explained.append("unattributed")
        if not explained:
            agreements += 1
        elif "unattributed" in explained:
            buckets["unattributed"].append(item)
        else:
            buckets[explained[0]].append(item)
    return buckets, agreements


def run_streams_case(oracle, world, live_numbers, live_names, show):
    matcher = oracle.matcher
    ignored = list(oracles.RUN_IGNORED_TAGS["cm"])
    matcher.build_token_index(matcher.premium_channels, ignored)
    by_id = {s["id"]: dict(s, priority=0,
                             m3u={"Main Provider": 1, "Backup Provider": 2}.get(
                                 s["m3u_account"], 0))
             for s in world}
    start = (max(live_numbers) + 1.0) if live_numbers else 1.0
    # CM side: match every stream, filter no categories out (defaults), group
    # by category, import one channel per stream at max+1.
    matched = {}
    for sid, stream in by_id.items():
        category, match_type, method, winner, callsign = cm_match_stream(
            oracle, stream["name"])
        if category is None:
            matched[sid] = {"channel_name": None, "number": None,
                            "category": None, "matched": False}
        else:
            matched[sid] = {"category": category, "match_type": match_type,
                            "method": method, "matched": True}
    by_category = {}
    for sid, decision in matched.items():
        if decision["matched"]:
            by_category.setdefault(decision["category"], []).append(sid)
    created = cm_import(by_id, by_category, live_names, start)
    cm = {sid: {"channel_name": None, "number": None, "category": None,
                "matched": False} for sid in by_id}
    for row in created:
        cm[row["stream_id"]] = {
            "channel_name": row["channel_name"], "number": row["number"],
            "category": row["category"], "matched": True}
    # Rerun: CM imports over the world PLUS its own first-run channels.
    rerun_names = list(live_names) + [row["channel_name"] for row in created]
    rerun_max = list(live_numbers) + [row["number"] for row in created]
    rerun = cm_import(by_id, by_category, rerun_names, max(rerun_max) + 1.0)
    cm_rerun_duplicates = sum(1 for row in rerun if row["is_duplicate"])
    # Rust side.
    dump = create_dump(world, {}, "streams")
    rust_rows = dump["rows"]
    names_map = names_targets([s["name"] for s in world])
    cat_map = categorize_labels([s["name"] for s in world])
    buckets, agreements = attribute_streams(
        cm, rust_rows, names_map, cat_map, live_names)
    # The numbering CHECK: planned numbers are exactly the first free numbers
    # above the max (order-independent — CM numbers in category/name order,
    # Rust in pool-priority order, so positions never coincide).
    expected_numbers = sorted(channel_seeder.allocate_channel_numbers(
        set(live_numbers), len(dump["request"]["channels"]), None))
    got_numbers = sorted(c["number"] for c in dump["request"]["channels"])
    numbering_ok = expected_numbers == got_numbers
    if not numbering_ok:
        buckets["numbering"].append({"stream": "(case numbers)",
                                       "cm": expected_numbers, "rust": got_numbers})
    return {"channels": len(world), "agreements": agreements,
            "buckets": buckets, "cm_created": len(created),
            "cm_rerun_duplicates": cm_rerun_duplicates,
            "rust_families": len(dump["request"]["channels"]),
            "rust_numbers": [c["number"] for c in dump["request"]["channels"]],
            "cm_numbers": [c["number"] for c in created],
            "start": start}


# ---------------------------------------------------------------------------
# Attribution: stations mode
# ---------------------------------------------------------------------------

SEED_BUCKETS = ("merged-data", "seed-parity", "numbering", "unattributed")


def merged_station(callsign):
    """The merged station row for a callsign (full key, then base spelling)."""
    path = os.path.join(REPO, "data", "merged", "stations.json")
    with open(path, encoding="utf-8") as fh:
        stations = json.load(fh)["stations"]
    if callsign in stations:
        return stations[callsign]
    base = normalize_callsign(callsign)
    for key in sorted(stations):
        if normalize_callsign(key) == base:
            return stations[key]
    return None


STATION_FIELDS = ("network_affiliation", "community_served_city",
                  "community_served_state")


def run_seed_case(oracle, world, show):
    matcher = oracle.matcher
    memo = {}
    skip = set()
    resolve = lambda name: cm_seed_resolve(  # noqa: E731
        matcher, name, OTA_FORMAT, False, skip, memo)[0]
    exclude = lambda name: cm_seed_exclude(  # noqa: E731
        matcher, name, skip, memo)
    stream_rows = [{"id": s["id"], "name": s["name"], "m3u_account_id": 1}
                   for s in world if s["id"] not in (201,)]
    # Stream 201 is attached (Holder carries it): `_collect_unattached_streams`
    # drops it before the seeder ever sees it, like the dump's Holder row does.
    plan = channel_seeder.build_seed_plan(
        stream_rows, ["Holder"], resolve, exclude)
    used = {50.0}
    numbers = channel_seeder.allocate_channel_numbers(used, len(plan.create), None)
    cm_create = {item.proposed_name: (item, number)
                 for item, number in zip(plan.create, numbers)}
    # Rust side (same candidates: group "US| CBS", stream 201 attached).
    dump = create_dump(world, {}, "stations")
    rows = {row["proposed"]: row for row in dump["rows"] if row["proposed"]}
    buckets = {bucket: [] for bucket in SEED_BUCKETS}
    agreements = 0
    # The invariant: a seeded station is named exactly as names renames each of
    # its source streams (`names::decide_ota_target`, the OTA half of the one
    # naming decision), and an unresolved stream is one names gives no OTA name.
    names_map = names_targets(sorted({n for row in dump["rows"]
                                      for n in row["source_names"]}))
    for row in dump["rows"]:
        for source in row["source_names"]:
            kind, target = names_map.get(source, (None, None))
            if row["action"] == "unresolved":
                ok = kind != "ota"
            else:
                ok = kind == "ota" and target == row["proposed"]
            if not ok:
                buckets["unattributed"].append({
                    "station": row["proposed"] or source,
                    "violation": "names says %s %r" % (kind, target)})
    for name, (item, number) in cm_create.items():
        row = rows.get(name)
        if row is None or row["action"] != "create":
            # Same station, different rendering: verified only when the station
            # ROWS differ (CM's older FCC table vs the merged newest-union table).
            rust_names = [r["proposed"] for r in dump["rows"] if r["action"] == "create"]
            merged_hit = False
            for rust_name in rust_names:
                m_callsign = re.search(r"\(([A-Z0-9-]{4,})\)\s*$", rust_name or "")
                c_callsign = re.search(r"\(([A-Z0-9-]{4,})\)\s*$", name or "")
                if m_callsign and c_callsign and \
                        normalize_callsign(m_callsign.group(1)) == \
                        normalize_callsign(c_callsign.group(1)):
                    mine = merged_station(m_callsign.group(1)) or {}
                    theirs = dict(memo.get(next(iter(item.source_names), ""), (None, {}))[1]
                                  or {})
                    if any((mine.get(f) or "").strip().upper()
                            != (theirs.get(f) or "").strip().upper()
                            for f in STATION_FIELDS):
                        merged_hit = True
            buckets["merged-data" if merged_hit else "seed-parity"].append(
                {"station": name, "cm": "create",
                 "rust": (row or {}).get("action")})
            continue
        if row["number"] != number:
            buckets["numbering"].append({"station": name, "cm": number,
                                           "rust": row["number"]})
            continue
        if sorted(row["stream_ids"]) != sorted(item.stream_ids):
            buckets["seed-parity"].append({"station": name, "cm": item.stream_ids,
                                             "rust": row["stream_ids"]})
            continue
        agreements += 1
    for row in dump["rows"]:
        if row["action"] == "create" and row["proposed"] not in cm_create:
            buckets["seed-parity"].append({"station": row["proposed"],
                                             "cm": "skip", "rust": "create"})
        elif row["action"] not in ("create",):
            # Skips/exclusions/unresolved agree structurally when CM's plan has
            # no corresponding create; anything else is surfaced.
            names = [i.proposed_name for i in list(plan.skip) + list(plan.excluded)]
            unresolved = [i.source_names for i in plan.unresolved]
            flat_unresolved = {n for names_list in unresolved for n in names_list}
            if row["action"] == "skip, name already used" \
                    and row["proposed"] not in names:
                buckets["seed-parity"].append({"station": row["proposed"],
                                                 "cm": "create", "rust": row["action"]})
            elif row["action"] == "skip, network excluded" \
                    and row["proposed"] not in names:
                buckets["seed-parity"].append({"station": row["proposed"],
                                                 "cm": "create", "rust": row["action"]})
            elif row["action"] == "unresolved" \
                    and not (set(row["source_names"]) & flat_unresolved):
                buckets["seed-parity"].append({"station": row["source_names"],
                                                 "cm": "resolved", "rust": "unresolved"})
    return {"stations": len(cm_create), "agreements": agreements,
            "buckets": buckets, "cm_numbers": numbers,
            "rust_numbers": [row["number"] for row in dump["rows"]
                               if row["action"] == "create"]}


def render_streams(result, show):
    lines = ["- %d/%d streams agree on the import decision" % (result["agreements"],
                                                                  result["channels"]),
             "- CM creates %d channels (one per stream); Rust plans %d (one per family)" % (
                 result["cm_created"], result["rust_families"]),
             "- CM rerun duplicates: %d (every channel imported AGAIN under a suffix);" % (
                 result["cm_rerun_duplicates"]),
             "  Rust rerun creates: 0 (owned `lineup:create:<identity key>` keys + the",
             "  same-family check against live channels — the fix over CM, pinned by the",
             "  `streams_apply_creates_one_channel_per_family_then_rerun_creates_nothing` test).",
             "- Numbers: CM %s vs Rust %s (max+1 step 1, skipping used)" % (
                 result["cm_numbers"], result["rust_numbers"]),
             "", "### Differences by attribution", ""]
    for bucket in STREAM_BUCKETS:
        lines.append("- `%s`: %d" % (bucket, len(result["buckets"].get(bucket, []))))
    lines += [""]
    for bucket in STREAM_BUCKETS:
        items = result["buckets"].get(bucket, [])
        if not items:
            continue
        lines += ["#### %s (%d)" % (bucket, len(items)), ""]
        for item in items[:show]:
            lines.append("- `%s`: CM %r vs Rust %r" % (
                item["stream"], item["cm"], item["rust"]))
        if len(items) > show:
            lines.append("  ... and %d more" % (len(items) - show))
        lines += [""]
    return lines


def render_seed(result, show):
    lines = ["- %d/%d stations agree on the seed decision" % (result["agreements"],
                                                                 result["stations"]),
             "- Numbers: CM %s vs Rust %s (from max+1, skipping used; no streams attached)" % (
                 result["cm_numbers"], result["rust_numbers"]),
             "", "### Differences by attribution", ""]
    for bucket in SEED_BUCKETS:
        lines.append("- `%s`: %d" % (bucket, len(result["buckets"].get(bucket, []))))
    lines += [""]
    for bucket in SEED_BUCKETS:
        items = result["buckets"].get(bucket, [])
        if not items:
            continue
        lines += ["#### %s (%d)" % (bucket, len(items)), ""]
        for item in items[:show]:
            lines.append("- %r" % (item,))
        if len(items) > show:
            lines.append("  ... and %d more" % (len(items) - show))
        lines += [""]
    return lines


def render(streams_result, disputed_result, seed_result, show):
    lines = [
        "<!-- Generated by scripts/oracle_report_lineup_create.py; do not edit. -->",
        "<!-- Reproduce with: python3 -I scripts/oracle_report_lineup_create.py -->",
        "# Lineup create oracle diff report (step 17)",
        "",
        "Lineup `create_*` makes channels on the unified engine (one identity per",
        "stream, one channel per identity family carrying the family's streams; one",
        "channel per OTA station with no streams attached); Channel-Mapparr is the",
        "oracle (default US database, normal sensitivity = %d). Every difference" % CM_THRESHOLD,
        "below traces to the one-channel-per-family rule, a documented fix, or the",
        "merged data; the unattributed list is the review surface (bar: 0).",
        "",
        "Three worlds: REPRESENTATIVE (seeded — the same station on two M3U",
        "accounts, premium names plain and decorated, OTA station names, unmatched",
        "names, across two stream groups), DISPUTED (stream names mined from",
        "`data/corpus/pairs.jsonl` where CM disagrees with the label or the other",
        "plugins), and SEED (two WIAT feeds with one attached, a national name, a",
        "blocked callsign). Rust runs are `create_dump` (the action's own",
        "`decide_streams` / `decide_stations`), cross-checked per stream against",
        "`names_dump` (naming) and `categorize_dump` (category).",
        "",
        "## Case `streams`: representative stream world",
        "",
    ]
    lines += render_streams(streams_result, show)
    lines += ["## Case `streams-disputed`: disputed stream names", ""]
    lines += render_streams(disputed_result, show)
    lines += ["## Case `stations`: seed world", ""]
    lines += render_seed(seed_result, show)
    lines += [
        "## Bucket notes",
        "",
        "- `family-grouping`: CM imports one channel per STREAM (`Name [M3U-id]`",
        "  suffixes for cross-provider duplicates); Rust makes one channel per",
        "  identity family carrying the family's streams in the streams app's",
        "  quality order (M3U priority first by default) —",
        "  exactly what `streams.match` would then attach. The documented divergence",
        "  (the seeder already groups this way).",
        "- `rerun-dedupe`: CM's second run re-imports every channel under a new",
        "  suffix (its dedupe only sees names); Rust's rerun creates nothing (owned",
        "  `lineup:create:<identity key>` keys plus the same-family check against",
        "  live channels — the fix over CM, counted in the case header).",
        "- Invariant (checked per stream before any bucket): create decides",
        "  through `names::decide_target` (the one naming decision) and",
        "  `categorize::decide_category`, so a created channel's name is the",
        "  `names_dump` target of its family head and its category the",
        "  `categorize_dump` category (or `Broadcast` for OTA); a name names would",
        "  not name is never created. A violation lands in `unattributed`.",
        "- `naming-via-names`: CM imports the raw stream name; create names the",
        "  channel exactly as `names_*` would rename it.",
        "- `category-via-categorize`: Rust's category (or its skip for having",
        "  none) is categorize's; docs/oracle-reports/lineup-categorize.md",
        "  attributes categorize vs CM.",
        "- `country-category`: decision A — CM imports into a country-named group",
        "  (`United States`, ...); a country label is no category, so with no",
        "  custom import group Rust skips the family with that reason.",
        "- `engine-via-names`: CM's matcher and names' one decision disagree on",
        "  whether the stream matches at all (blocked callsign, cut, aliases);",
        "  create follows names, and docs/oracle-reports/lineup-names.md",
        "  attributes CM vs names.",
        "- `numbering`: max+1 step 1 skipping used (streams) / from",
        "  `seed_start_number` skipping used (stations) — the same walk as",
        "  `allocate_channel_numbers` and `next_free_numbers`.",
        "- `collision`: a same-named live channel exists — CM suffix-dedupes",
        "  (`Name [N]`), the shared resolver renames or skips per",
        "  `collision_strategy` (global, like every rename).",
        "- `seed-parity`: stations-mode structure CM and Rust build the same way",
        "  (`build_seed_plan` grouping/skip/exclude, no attached streams); rows here",
        "  are the review surface for that mode. `merged-data` there is the same",
        "  station whose FCC rows differ between CM's older tables and",
        "  `data/merged/` (checked field by field).",
        "",
    ]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="regenerate and fail unless byte-identical to the report")
    parser.add_argument("--show", type=int, default=15, help="differences per bucket")
    args = parser.parse_args(argv)

    oracle = cm_setup()
    world = representative_streams()
    disputed = disputed_streams()
    # One live channel the world already holds (number pin for max+1).
    live_numbers = [50.0]
    live_names = ["Holder"]
    streams_result = run_streams_case(oracle, world, live_numbers, live_names, args.show)
    disputed_result = run_streams_case(
        oracle, disputed, live_numbers, live_names, args.show)
    disputed_result["case"] = "streams-disputed"
    seed_result = run_seed_case(oracle, seed_world(), args.show)

    text = render(streams_result, disputed_result, seed_result, args.show)
    if args.check:
        with open(REPORT, encoding="utf-8") as fh:
            committed = fh.read()
        if committed != text:
            print("%s is stale: regenerate with" % REPORT, file=sys.stderr)
            print("  python3 -I scripts/oracle_report_lineup_create.py", file=sys.stderr)
            return 1
        total = (streams_result["channels"] + disputed_result["channels"]
                 + seed_result["stations"])
        agree = (streams_result["agreements"] + disputed_result["agreements"]
                 + seed_result["agreements"])
        unatt = sum(len(result["buckets"].get("unattributed", []))
                    for result in (streams_result, disputed_result, seed_result))
        print("%s reproduces (%d/%d agree, %d unattributed)" % (REPORT, agree, total, unatt))
        return 0

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w", encoding="utf-8") as fh:
        fh.write(text)
    print("wrote %s" % REPORT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
