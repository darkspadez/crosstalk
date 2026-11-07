#!/usr/bin/env python3
"""Oracle diff report for step 15: lineup names vs Channel-Mapparr.

Lineup `names_*` computes canonical channel names on the unified engine (one
identity per channel, one ranker + one cut, tag re-attachment from the shared
vocabulary); Channel-Mapparr is the oracle. This script runs CM's real
`load_and_process_channels` decision path (the `oracles.py` CM path — its
`match_broadcast_channel` OTA ladder, its `extract_tags` + `fuzzy_match` premium
path — plus its OTA formatting `_format_ota_name`/`_extract_stream_network`,
ported verbatim with line citations) against the Rust side
(`cargo run -p crosstalk-lineup --example names_dump`, the same
`decide_heuristic` the action plans from), attributes every difference, and
writes `docs/oracle-reports/lineup-names.md`.

Usage:
    python3 -I scripts/oracle_report_lineup_names.py [--check] [--show N]

    --check   regenerate into a temp file and fail unless it is byte-identical
              to the committed report (the CI gate: the report reproduces).
    --show N  differences printed per bucket (default 15).

The CM decision path per channel (`plugin.py:1976-2060`), with defaults
(`channel_databases = "US"`, normal sensitivity = 80, `ota_format` default,
market fallback off, unknown suffix `" [Unk]"`, tag on):

1. `network = _extract_stream_network(name)`; `callsign, station =
   match_broadcast_channel(name, network, allow_market_fallback)`.
2. If a callsign was found: a station names it through `_format_ota_name`
   (Renamed, or Skipped for missing fields); no station is Skipped
   (`Callsign X not in channel databases`); premium is NEVER tried.
3. Else premium: `extract_tags` (regional/extra/quality over the ignored list),
   `get_candidates` pre-filter, `fuzzy_match` winner, `build_final_channel_name`.
   A hit renames; a miss is Skipped (`No match found`).
4. `new == current` is Skipped (`Already in correct format`).
5. `rename_unknown_channels` appends the suffix to every Skipped row (a second
   action in CM; one pass here).

Attribution buckets (every difference lands in exactly one, each by a CHECK —
nothing lands in a bucket by elimination; what no check explains is
`unattributed`):

- `ota-format`: both sides OTA with different callsigns (different ladders).
- `ota-vs-premium`: one side OTA and the other not, or exactly one side's ladder
  reads a callsign at all (CM's regex ladder vs the merged allowlist).
- `premium-canonical`: different DB winners, verified one of four ways: both
  winners are in both DBs and differ; Rust's winner is a decided alias row;
  CM's own `fuzzy_match` rejects Rust's winner (< 80); or the unified ranker
  never scores CM's winner even with no cut (decided guards, country rule).
- `cm-prefilter`: CM's own scorer accepts Rust's winner but CM's
  `get_candidates` token pre-filter never offers it.
- `merged-data`: a winner absent from the other side's DB, or the same
  callsign whose station ROWS differ (CM's older tables vs the merged union).
- `cut`: CM's winner is in the unified ranker's no-cut shortlist with a score
  below the host cut (verified from `names_dump`'s `floor`).
- `tag-reattachment`: same base, different tags — including CM re-attaching a
  tag the base already holds (`K28NN-D (CBS) (CBS)`), which Rust's never-double
  rule leaves as already correct.
- `unknown-tag`: unmatched handling, two documented fixes over CM: only truly
  unmatched rows are tagged (CM's `rename_unknown_channels` tags every
  `Skipped` row — already-correct and OTA missing-field rows included — so CM
  turns `CBS` into `CBS [Unk]`), and an already-suffixed name is never doubled.
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

REPORT = os.path.join(REPO, "docs", "oracle-reports", "lineup-names.md")
CM_THRESHOLD = 80
RUST_CUT = 95
OTA_FORMAT = "{NETWORK} - {STATE} {CITY} ({CALLSIGN})"
UNKNOWN_SUFFIX = " [Unk]"

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
# CM decision path
# ---------------------------------------------------------------------------

def cm_setup(us_only=True):
    """The CM oracle with its default US database (the `channel_databases = "US"`
    default, plugin.py:1665)."""
    oracle = oracles.get("cm")
    if us_only:
        oracle.matcher.reload_databases(["US"])
    return oracle


def cm_premium_match(matcher, name, ignored):
    """CM's premium winner for `name` (`oracles.py` CM path, plugin.py:2008-2037).

    The token index is built once per case by the caller (plugin.py:1945-1946);
    the per-channel calls are `get_candidates` + `fuzzy_match`. Ties break by
    DB order (the `oracles.py` rule), never by set iteration order — the plugin
    itself is hash-seed dependent there, which would make this report
    unreproducible (`--check` gates byte-identity).
    """
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


def cm_decide(oracle, name, ota_format, market_fallback, tag_unmatched, unknown_suffix):
    """One channel through CM's `load_and_process_channels` + `rename_unknown`.

    Returns `((final_new, status, matcher, method, reason), detail)` where status
    is `Renamed`/`Skipped` after the unknown-tagging step, mirroring what the two
    CM actions leave behind, and `detail` carries the premium winner/score for
    attribution (the same `fuzzy_match` call, not a second one).
    """
    ignored = list(oracles.RUN_IGNORED_TAGS["cm"])
    matcher = oracle.matcher
    detail = {"base": None, "score": None}
    network = extract_stream_network(name)
    callsign, station = matcher.match_broadcast_channel(
        name, network=network, allow_market_fallback=market_fallback)
    detail["callsign"] = callsign
    if callsign:
        if station:
            detail["station"] = station
            new = format_ota_name(station, ota_format, callsign, network_override=network)
            if new and new != name:
                return ((new, "Renamed", "Broadcast (OTA)", "OTA - Callsign Match",
                         "OTA callsign %s resolves to a station" % callsign), detail)
            # OTA Skipped rows still reach `rename_unknown_channels`, which tags
            # every Skipped row (plugin.py:2325).
            base = (name, "Skipped", "none", "No Match",
                    "Already in correct format" if new
                    else "Missing required fields for OTA format")
        else:
            detail["callsign_blocked"] = callsign
            base = (name, "Skipped", "none", "No Match",
                    "Callsign %s not in channel databases" % callsign)
    else:
        regional, extra_tags, quality_tags = matcher.extract_tags(name, ignored)
        matched, score, match_type = cm_premium_match(matcher, name, ignored)
        detail = {"base": matched, "score": score}
        if matched:
            new = matcher.build_final_channel_name(matched, regional, extra_tags, quality_tags)
            if new == name:
                base = (name, "Skipped", "none", "No Match", "Already in correct format")
            else:
                method = "premium match %s%% %s" % (score, match_type)
                base = (new, "Renamed", "Premium/Cable", method, method)
        else:
            base = (name, "Skipped", "none", "No Match", "No match found in channels.json")
    detail["pre_reason"] = base[4]
    if base[1] == "Skipped" and tag_unmatched and (unknown_suffix or "").strip():
        # `rename_unknown_channels`: CM appends blindly (the double-tag bug the
        # Rust side fixes via apply_suffix).
        return ((base[0] + unknown_suffix, "Renamed", "unknown", "Unknown Suffix",
                 "No match found in channels.json; tagged with the unknown suffix"),
                detail)
    return (base, detail)


# ---------------------------------------------------------------------------
# Worlds
# ---------------------------------------------------------------------------

def merged_us_names():
    path = os.path.join(REPO, "data", "merged", "channels", "US.json")
    with open(path, encoding="utf-8") as fh:
        return [c["channel_name"] for c in json.load(fh)["channels"]]


def merged_stations_with_network(limit=12):
    path = os.path.join(REPO, "data", "merged", "stations.json")
    with open(path, encoding="utf-8") as fh:
        stations = json.load(fh)["stations"]
    picked = []
    for callsign in ("WABC-TV", "KSDK", "WFOR", "KGO", "WREX", "WSIL", "KABC", "WBBJ"):
        if callsign in stations:
            picked.append(stations[callsign])
    for callsign, station in stations.items():
        if len(picked) >= limit:
            break
        if station.get("network_affiliation") and station.get("community_served_city") \
                and station not in picked:
            picked.append(station)
    return picked


def representative_world():
    """A representative world (seeded): OTA callsigns in provider decorations,
    premium DB names with provider decorations, popular national networks in
    provider decorations, unmatched names."""
    rng = random.Random(15)
    channels = []
    next_id = [1]

    def add(name):
        channels.append({"id": next_id[0], "name": name, "group": "Test"})
        next_id[0] += 1

    for station in merged_stations_with_network():
        callsign = station["callsign"]
        base = re.sub(r'-(?:TV|CD|LP|DT|LD)$', '', callsign)
        network = parse_network_affiliation(station.get("network_affiliation") or "") or "ABC"
        city = (station.get("community_served_city") or "SPRINGFIELD").title()
        add("%s (%s)" % (network, base))
        add("%s-TV" % base if base == callsign else callsign)
        add("US: %s (%s) HD" % (network, base))
        add("%s %s-DT" % (network, base))
        add("%s (%s) [HD]" % (network, base))
    # Unknown callsigns and market references.
    add("ABC (WZZZ)")
    add("US: ABC 9 HD [SYRACUSE]")
    add("CBS 4 HD DENVER CO")
    add("FOX 9 PLUS HD")

    db = merged_us_names()
    pool = [n for n in db if n and len(n) <= 40]
    pool.sort()
    for name in rng.sample(pool, 60):
        add(name)
    for name in rng.sample(pool, 40):
        deco = rng.choice([0, 1, 2, 3, 4, 5])
        if deco == 0:
            add("%s HD" % name)
        elif deco == 1:
            add("%s [FHD]" % name)
        elif deco == 2:
            add("US: %s" % name)
        elif deco == 3:
            add("%s (Backup)" % name)
        elif deco == 4:
            add(name.upper())
        else:
            add("%s [VIP]" % name)
    # The channels a real lineup is mostly made of: popular national networks in
    # the provider decorations seen in the corpus (prefixes, quality, feed zone,
    # writer tags). The DB sample above is dominated by LPTV rows; these are where
    # most user-visible renames happen.
    popular = ["ESPN", "ESPN2", "CNN", "Fox News Channel", "MSNBC", "HBO", "HBO 2",
               "Cinemax", "Showtime", "Starz", "TNT", "TBS", "USA Network", "FX", "AMC",
               "A&E", "History", "Discovery Channel", "TLC", "HGTV", "Food Network",
               "Nickelodeon", "Disney Channel", "Cartoon Network", "Comedy Central",
               "Bravo", "Syfy", "Lifetime", "Hallmark Channel", "NFL Network", "NBA TV",
               "MLB Network", "Golf Channel", "CNBC", "The Weather Channel", "BET", "MTV",
               "Paramount Network", "Freeform", "National Geographic"]
    decorations = ["US: %s HD", "USA | %s FHD", "%s East", "%s (West)", "%s [4K]",
                   "%s HD (Backup)", "US| %s ᴴᴰ", "%s"]
    for name in popular:
        for template in rng.sample(decorations, 2):
            add(template % name)
    for name in ["Zzqz No Such Channel 999", "Qqx Obsolete Feed 12 HD",
                 "Zzqz Archive [HD]", "Qqx Test Pattern", "Zzqz No Such Channel 999 [Unk]",
                 "KXYZ fakecast", "US: Zzqz HD", "Oldies 1047"]:
        add(name)
    return channels


def disputed_set(limit=30):
    """Names the plugins dispute: pairs where CM's verdict differs from the label
    or from every other plugin's — exactly where a naming diff can hide."""
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
    channels = []
    seen = set()
    for pair in disputed[:limit]:
        for key in ("query", "candidate"):
            name = (pair.get(key) or "").strip()
            if name and name not in seen and len(name) <= 60:
                seen.add(name)
                channels.append({"id": 10000 + len(channels), "name": name, "group": "Disputed"})
    return channels


# ---------------------------------------------------------------------------
# Rust side
# ---------------------------------------------------------------------------

def rust_dump(channels, settings):
    payload = {"channels": channels, "scope": {}, "settings": settings}
    proc = subprocess.run(
        ["cargo", "run", "-q", "-p", "crosstalk-lineup", "--example", "names_dump"],
        input=json.dumps(payload), capture_output=True, text=True, cwd=REPO)
    if proc.returncode != 0:
        raise RuntimeError("names_dump failed: %s" % proc.stderr[-4000:])
    return {row["id"]: row for row in json.loads(proc.stdout)}


CASES = [
    {"name": "defaults", "settings": {},
     "ota_format": OTA_FORMAT, "market_fallback": False,
     "tag_unmatched": True, "unknown_suffix": UNKNOWN_SUFFIX},
    {"name": "market-fallback", "settings": {"ota_market_fallback": True},
     "ota_format": OTA_FORMAT, "market_fallback": True,
     "tag_unmatched": True, "unknown_suffix": UNKNOWN_SUFFIX},
]


def merged_station(callsign):
    """The merged station row for a callsign (full key, then the base spelling)."""
    stations = _MERGED_STATIONS
    if callsign in stations:
        return stations[callsign]
    base = normalize_callsign(callsign)
    for key in sorted(stations):
        if normalize_callsign(key) == base:
            return stations[key]
    return None


with open(os.path.join(REPO, "data", "merged", "stations.json"), encoding="utf-8") as _fh:
    _MERGED_STATIONS = json.load(_fh)["stations"]

STATION_FIELDS = ("network_affiliation", "community_served_city", "community_served_state")


def attribute(case, cm, rust, rust_db, cm_db_set, cm_pair_score, cm_prefilter_keeps):
    """One bucket per difference, each backed by a check (see the module docstring).

    Nothing falls into a bucket by elimination: a difference no check explains
    is `unattributed`. `cm_pair_score(leader)` is CM's own `fuzzy_match` of the
    channel against ONE candidate (0 when CM rejects it); `cm_prefilter_keeps(c)`
    is whether CM's `get_candidates` token pre-filter lets candidate `c` through.
    """
    cm_new, cm_status, cm_matcher = cm["new"], cm["status"], cm["matcher"]
    rust_new, rust_status = rust["new"], rust["status"]
    if cm_new == rust_new:
        return None
    cm_ota = cm_matcher == "Broadcast (OTA)"
    rust_ota = rust["matcher"] == "Broadcast (OTA)"
    if cm_ota and rust_ota:
        if cm.get("callsign") and cm["callsign"] == rust.get("callsign"):
            # Same callsign, different rendering: verified only when the station
            # ROWS differ (CM's older FCC table vs the merged newest-union table).
            mine = merged_station(rust["callsign"]) or {}
            theirs = cm.get("station") or {}
            if any((mine.get(f) or "").strip().upper() != (theirs.get(f) or "").strip().upper()
                   for f in STATION_FIELDS):
                return "merged-data"
            return "unattributed"
        return "ota-format"
    if cm_ota != rust_ota:
        return "ota-vs-premium"
    # A callsign exactly one side's ladder reads blocks premium (or OTA-skips) on
    # that side only: the ladders differ (CM's regex ladder vs the merged
    # allowlist — e.g. digit-led `K10OG-D`/`K04QP-D` shapes).
    if bool(cm.get("callsign")) != bool(rust.get("callsign")):
        return "ota-vs-premium"
    cm_matched = cm_status == "Renamed" and cm_matcher == "Premium/Cable"
    rust_matched = rust_status == "Renamed" and rust["matcher"] == "Premium/Cable"
    cm_base = cm.get("base")
    leader = rust.get("leader")
    rust_correct = rust.get("reason") == "Already in correct format"

    def different_winners():
        # Both winners exist in both DBs and differ: a ranking difference, not data.
        if (cm_base and cm_base not in rust_db) or (leader and leader not in cm_db_set):
            return "merged-data"
        if cm_base and leader and cm_base != leader:
            return "premium-canonical"
        return None

    if cm_matched and rust_matched:
        if cm_base and cm_base == leader:
            return "tag-reattachment"
        return different_winners() or "unattributed"
    if cm_matched:
        # CM renames, Rust leaves the name.
        if leader and cm_base == leader and rust_correct:
            # Same winner; Rust's target IS the current name (CM re-attached a tag
            # the base already holds, or kept an ignored one): the never-double /
            # ignored-strip rules, not the cut.
            return "tag-reattachment"
        if leader and rust_correct:
            return different_winners() or "unattributed"
        if leader is None:
            if cm_base and cm_base not in rust_db:
                return "merged-data"
            floor = {name: score for name, score in rust.get("floor") or []}
            if cm_base in floor and floor[cm_base] < RUST_CUT:
                return "cut"
            if cm_base and cm_base not in floor:
                # The unified ranker never scores CM's winner (decided guards,
                # country rule, alias rows): a ranking difference.
                return "premium-canonical"
        return "unattributed"
    if rust_matched:
        # Rust renames, CM leaves the name (or tags it).
        if leader and leader not in cm_db_set:
            return "merged-data"
        if leader and (rust.get("method") == "Alias" or cm_pair_score(leader) < CM_THRESHOLD):
            # A decided alias row, or a pair CM's own scorer rejects: the unified
            # ranker's call, verified on CM's side.
            return "premium-canonical"
        if leader and not cm_prefilter_keeps(leader):
            return "cm-prefilter"
        if leader and cm_base and cm_base != leader:
            return different_winners() or "unattributed"
        return "unattributed"
    # Both unmatched (or both already correct) with different finals: the unknown
    # handling. Verified per documented fix.
    suffix = case["unknown_suffix"]
    old = rust["old"]
    if cm_new == old + suffix and rust_new == old:
        pre = cm.get("pre_reason") or ""
        if pre == rust.get("reason") and not pre.startswith("No match"):
            # The same non-unmatched Skipped row (already correct, OTA missing
            # fields) that CM's `rename_unknown_channels` tags anyway.
            return "unknown-tag"
        if old.rstrip().endswith(suffix.strip()):
            return "unknown-tag"  # CM double-appends
        if rust_correct and leader:
            # Rust has the family (the name is its own DB entry); CM finds nothing
            # at 80 and tags it: CM lacks the winner, or CM's scorer rejects it.
            if leader not in cm_db_set:
                return "merged-data"
            if cm_pair_score(leader) < CM_THRESHOLD:
                return "premium-canonical"
            if not cm_prefilter_keeps(leader):
                # CM's own scorer accepts the winner, but its token pre-filter
                # never offers it (e.g. `A&E` tokenizes to nothing it indexes).
                return "cm-prefilter"
    return "unattributed"


def run_case(oracle, channels, case, show):
    ignored = list(oracles.RUN_IGNORED_TAGS["cm"])
    # The token index CM's run path builds once per load (plugin.py:1945-1946):
    # same calls, hoisted out of the per-channel loop.
    matcher = oracle.matcher
    matcher.build_token_index(matcher.premium_channels, ignored)
    rust_db = set(merged_us_names())
    cm_db_set = set(matcher.premium_channels)

    def cm_pair_score_for(name):
        def score(candidate):
            hit, pair_score, _ = matcher.fuzzy_match(name, [candidate], ignored)
            return pair_score if hit else 0
        return score

    def cm_prefilter_for(name):
        def keeps(candidate):
            pre = matcher.get_candidates(name, ignored)
            return pre is None or candidate in set(pre)
        return keeps

    rust_rows = rust_dump(channels, case["settings"])
    cm_rows = {}
    cm_details = {}
    for channel in channels:
        final, detail = cm_decide(oracle, channel["name"], case["ota_format"],
                                  case["market_fallback"], case["tag_unmatched"],
                                  case["unknown_suffix"])
        cm_rows[channel["id"]] = final
        cm_details[channel["id"]] = detail

    buckets = {bucket: [] for bucket in BUCKETS}
    agreements = 0
    # Per-channel CM winner/score for attribution (a second fuzzy_match would
    # double the cost; reuse the premium path's own calls on demand only for
    # mismatches).
    for channel in channels:
        cid = channel["id"]
        cm = cm_rows[cid]
        rust = rust_rows[cid]
        if cm[0] == rust["new"]:
            agreements += 1
            continue
        bucket = attribute(case, _cm_view(cm, cm_details[cid]), rust, rust_db, cm_db_set,
                           cm_pair_score_for(channel["name"]), cm_prefilter_for(channel["name"]))
        buckets.setdefault(bucket, buckets["unattributed"]).append({
            "channel": channel["name"],
            "cm": {"new": cm[0], "status": cm[1], "matcher": cm[2], "method": cm[3]},
            "rust": {"new": rust["new"], "status": rust["status"],
                     "matcher": rust["matcher"], "method": rust["method"]},
        })
    return {"case": case["name"], "channels": len(channels), "agreements": agreements,
            "buckets": buckets}


def _cm_view(cm, enriched):
    return {"new": cm[0], "status": cm[1], "matcher": cm[2],
            "base": enriched["base"], "score": enriched["score"],
            "callsign": enriched.get("callsign"),
            "callsign_blocked": enriched.get("callsign_blocked"),
            "station": enriched.get("station"),
            "pre_reason": enriched.get("pre_reason")}


BUCKETS = ("ota-format", "ota-vs-premium", "premium-canonical", "cm-prefilter",
           "merged-data", "cut", "tag-reattachment", "unknown-tag", "unattributed")


def render_buckets(result, show):
    lines = ["- %d/%d channels agree on the final name" % (result["agreements"],
                                                            result["channels"]),
             "", "### Differences by attribution", ""]
    for bucket in BUCKETS:
        lines.append("- `%s`: %d" % (bucket, len(result["buckets"].get(bucket, []))))
    lines += [""]
    for bucket in BUCKETS:
        items = result["buckets"].get(bucket, [])
        if not items:
            continue
        lines += ["#### %s (%d)" % (bucket, len(items)), ""]
        for item in items[:show]:
            lines.append("- `%s`: CM %r (%s) vs Rust %r (%s)" % (
                item["channel"], item["cm"]["new"], item["cm"]["matcher"],
                item["rust"]["new"], item["rust"]["matcher"]))
        if len(items) > show:
            lines.append("  ... and %d more" % (len(items) - show))
        lines += [""]
    return lines


def render(results, show):
    lines = [
        "<!-- Generated by scripts/oracle_report_lineup_names.py; do not edit. -->",
        "<!-- Reproduce with: python3 -I scripts/oracle_report_lineup_names.py -->",
        "# Lineup names oracle diff report (step 15)",
        "",
        "Lineup `names_*` computes canonical channel names on the unified engine (one",
        "identity per channel, one ranker at the host `[matching]` cut — default strict =",
        "%d — plus CM's tag re-attachment from the shared vocabulary); Channel-Mapparr is" % RUST_CUT,
        "the oracle (default US database, normal sensitivity = %d). Every difference below" % CM_THRESHOLD,
        "traces to a `data/corpus/decisions.json` row, the merged data, the unified family",
        "rule, or a documented fix; the unattributed list is the review surface (bar: 0).",
        "",
        "Two worlds: REPRESENTATIVE (seeded — OTA callsigns in provider decorations,",
        "premium DB names with provider decorations, 40 popular national networks in",
        "provider decorations, unmatched names) and DISPUTED (names",
        "mined from `data/corpus/pairs.jsonl` where CM disagrees with the label or the",
        "other plugins). Two settings cases: `defaults` and `market-fallback` (CM's",
        "`ota_market_fallback` on, both sides).",
        "",
        "## Inputs",
        "",
        "- CM oracle: `reference/Dispatcharr-Channel-Maparr-Plugin` "
        "`load_and_process_channels` decision path via `scripts/corpus/oracles.py` "
        "(OTA ladder + `extract_tags`/`fuzzy_match` premium path) with `_format_ota_name` "
        "and `_extract_stream_network` as cited; `rename_unknown_channels` folds into the",
        "  same pass (tagged Skipped rows compare as Renamed, like the Rust side).",
        "- Rust: `cargo run -p crosstalk-lineup --example names_dump` (the action's own",
        "  `decide_heuristic` over the merged US DB at the host cut, plus the same",
        "  ranker's no-cut `floor` shortlist that verifies a `cut` attribution).",
        "- Unknown handling: CM appends blindly (the re-tag bug); Rust never double-appends",
        "  (`apply_suffix`, the documented fix over `current_name + suffix`).",
        "",
    ]
    for result in results:
        title = result["case"]
        world = "disputed set" if "disputed" in title else "representative world"
        lines += ["## Case `%s`: %s" % (title, world), ""]
        lines += render_buckets(result, show)
    lines += [
        "## Bucket notes",
        "",
        "- `ota-format`: both sides OTA with different callsigns — different ladders.",
        "  (No rows in this run: the ladders agree on every representative callsign.)",
        "- `ota-vs-premium`: one side OTA and the other not, or exactly one side's",
        "  ladder reads a callsign (CM's regex ladder vs the merged allowlist).",
        "- `premium-canonical`: different DB winners, each verified: both winners in",
        "  both DBs and different; Rust's winner a decided alias row; CM's own",
        "  `fuzzy_match` rejecting Rust's winner (< %d); or the unified ranker never" % CM_THRESHOLD,
        "  scoring CM's winner even with no cut (decided guards, country rule).",
        "- `cm-prefilter`: CM's own scorer accepts Rust's winner, but its",
        "  `get_candidates` token pre-filter never offers it.",
        "- `merged-data`: a winner absent from the other side's DB, or the same",
        "  callsign whose station rows differ (CM's older tables vs `data/merged/`).",
        "- `cut`: CM's winner sits in the unified ranker's no-cut shortlist below the",
        "  host cut (%d) — verified from `names_dump`'s `floor`, never assumed." % RUST_CUT,
        "- `tag-reattachment`: same base, different tags — incl. CM re-attaching a tag",
        "  the base already holds (`K28NN-D (CBS) (CBS)`), which the never-double rule",
        "  leaves as already correct.",
        "- `unknown-tag`: only truly unmatched rows are tagged, never twice (two",
        "  documented fixes over CM, which tags every `Skipped` row — already-correct",
        "  and OTA missing-field rows included — and appends blindly).",
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
    world = representative_world()
    disputed = disputed_set()
    results = []
    for case in CASES:
        result = run_case(oracle, world, case, args.show)
        results.append(result)
    # The disputed set runs once (defaults): it exists to show kinds, not rates.
    disputed_result = run_case(oracle, disputed, CASES[0], args.show)
    disputed_result["case"] = "defaults-disputed"
    results.append(disputed_result)

    text = render(results, args.show)
    if args.check:
        with open(REPORT, encoding="utf-8") as fh:
            committed = fh.read()
        if committed != text:
            print("%s is stale: regenerate with" % REPORT, file=sys.stderr)
            print("  python3 -I scripts/oracle_report_lineup_names.py", file=sys.stderr)
            return 1
        total = sum(r["channels"] for r in results)
        agree = sum(r["agreements"] for r in results)
        unatt = sum(len(r["buckets"].get("unattributed", [])) for r in results)
        print("%s reproduces (%d/%d agree, %d unattributed)" % (REPORT, agree, total, unatt))
        return 0

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w", encoding="utf-8") as fh:
        fh.write(text)
    print("wrote %s" % REPORT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
