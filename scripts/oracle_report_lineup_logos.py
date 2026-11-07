#!/usr/bin/env python3
"""Oracle diff report for step 16: lineup logos vs Channel-Mapparr and Lineuparr.

Lineup `logos_*` assigns each in-scope channel one logo through the unified
resolver (EPG icon -> Logo Manager exact name -> tv-logos -> network logo ->
default, in the configured `logo_sources` order); Channel-Mapparr (`apply_tv_logos` /
`replace_tv_logos`) and Lineuparr (`apply_logo_match`) are the oracles. This
script runs both plugins' real logo tiers — CM's and LU's real
`logo_matcher` functions (`normalize_*`, `match_channel_to_logo`,
`match_station_logo`, `network_logo`, `build_logo_url`) loaded from
`reference/`, with CM's tier order transcribed from `_assign_tv_logos`
(station-first, fuzzy, then EPG icon and network logo for channels with no
logo) and LU's from `_do_apply_logo_match` (EPG icon, Logo Manager,
tv-logos) — against the Rust side (`cargo run -p crosstalk-lineup --example
logos_dump`, the same `decide_logo` the action plans from), over the SAME
recorded tree fixture (`scripts/data/tv_logos_tree.json`, parsed into per-dir
file lists exactly like `TvLogosIndex::from_tree`: direct `blob` children of
`countries/<dir>`), attributes every difference, and writes
`docs/oracle-reports/lineup-logos.md`.

Usage:
    python3 -I scripts/oracle_report_lineup_logos.py [--check] [--show N]

    --check   regenerate into a temp file and fail unless it is byte-identical
              to the committed report (the CI gate: the report reproduces).
    --show N  differences printed per bucket (default 15).

CM per channel (`plugin.py:2544-2700`), thresholds 85 (apply) / 80 (replace at
the default normal sensitivity):

1. Station-format names take the station file first — each searched country's
   station directory, then its country directory.
2. Else the best fuzzy filename match at threshold across the searched
   countries (`channel_databases`, here `US` unless the case says otherwise).
3. Only for channels with no logo: the assigned EPG entry's icon, then the
   station's network logo (`<network>-<suffix>.png`, else the white variant).

LU per channel (`plugin.py:3170-3260`): channels with a logo are skipped;
otherwise EPG icon, then Logo Manager exact name, then the fuzzy match in the
single lineup country (`US` here) at 85. No station tier, no network tier.

The comparison is on the final logo URL (a channel keeping the logo it already
carries compares equal on both sides). CM is the primary oracle (the wider
tiers); LU rows are shown for context, and a row matching exactly one oracle
attributes against the other.

Attribution buckets (every difference lands in exactly one, each by a CHECK —
nothing lands in a bucket by elimination; what no check explains is
`unattributed`):

- `replace-scope`: one side skips a channel with a logo because of its
  replace/apply flag (verified: the channel carries a logo and the flags
  differ).
- `lu-order`: Rust's winner is its EPG icon or Logo Manager hit (verified on
  the dump's own tier evidence) while CM took a station/fuzzy tv-logos file —
  the unified order is LU's (EPG icon and Logo Manager before tv-logos), and CM
  has no Logo Manager tier.
- `network-logo`: Rust's network-logo tier (CM's fallback, ported into
  `core::logos`: orchestrator decision B) fires where LU, which has no such
  tier, finds nothing (verified: Rust's URL is what CM's real `network_logo`
  returns for the station's network word over Rust's searched dirs).
- `zone-feed`: Rust matched a zone feed (`Showtime (W)`) on its zone-free name,
  because zone feeds share their brand's logo (verified: the engine reads a
  zone, and the other side's OWN matcher, given the zone-free name, picks
  Rust's file from the same dir).
- `station-format`: one side takes the station file while the other fuzzies or
  misses (verified: the name is station-shaped and the station file carries the
  callsign and network words) — LU has no station tier at all.
- `country-dirs`: the winners live in directories only one side searched
  (verified: the winner's directory is absent from the other side's searched
  dirs — CM's `channel_databases` list vs the identity/override/enabled
  countries; LU's single lineup country). Includes the network fallback: each
  side's network logo comes from its own first searched dir that has one.
- `similarity`: same tv-logos fuzzy tier, different files (or one side missing),
  because the metrics disagree (verified: the unified engine similarity from
  the dump's per-file scores accepts what the plugins' `fuzz.ratio` refuses,
  or the reverse — each recomputed in this process with the same function the
  loaded matchers use).
- `normalization`: same tier, different files, because the normalizers disagree
  (verified: under one `fuzz.ratio`, the plugin-normalized pair ranks the
  plugin file first while the Rust-normalized pair from the dump ranks the
  Rust file first).
- `threshold`: CM's replace threshold (80) accepts a file the unified 85
  refuses (verified: the plugin ratio sits in [80, 85) while the unified
  similarity sits below 85).
- `order`: both winners come from tiers both sides consult, in a different
  position (verified: both tiers fire on the Rust side's own evidence, and the
  configured `logo_sources` order puts Rust's tier first) — or the other side's
  winning tier is CM's network fallback and the configured `logo_sources`
  leaves the `network` token out (verified on the case's own order).
- `default`: the unified default-logo safety net fires where the tv-logos
  actions have nothing (verified: the Rust source is `default` and the plugin
  side resolves no URL).
- `unattributed`: listed for review. A non-empty list is the review surface;
  the script still exits 0 (reports never fail CI) — but the step's bar is 0.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import random
import subprocess
import sys

sys.dont_write_bytecode = True

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TREE_FIXTURE = os.path.join(REPO, "scripts", "data", "tv_logos_tree.json")
REPORT = os.path.join(REPO, "docs", "oracle-reports", "lineup-logos.md")
LOGO_MATCH_THRESHOLD = 85
CM_REPLACE_THRESHOLD = 80
RUST_CUT = 85

CM_MATCHER_PATH = os.path.join(
    REPO, "reference", "Dispatcharr-Channel-Maparr-Plugin", "Channel-Maparr",
    "logo_matcher.py")
LU_MATCHER_PATH = os.path.join(
    REPO, "reference", "Dispatcharr-Lineuparr-Plugin", "Lineuparr", "logo_matcher.py")


def load_matcher(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cm_logo = load_matcher(CM_MATCHER_PATH, "cm_logo_matcher")
lu_logo = load_matcher(LU_MATCHER_PATH, "lu_logo_matcher")


# ---------------------------------------------------------------------------
# The recorded tree, parsed like TvLogosIndex::from_tree (direct blob children
# of countries/<dir> with image extensions; anything else skipped).
# ---------------------------------------------------------------------------

_IMAGE_EXTS = (".png", ".svg", ".jpg", ".jpeg", ".gif", ".webp")


def tree_dirs(payload):
    index = {}
    for entry in payload.get("tree", []):
        if entry.get("type") != "blob":
            continue
        path = entry.get("path") or ""
        if "\\" in path:
            continue
        parts = path.split("/")
        if len(parts) < 3 or parts[0] != "countries":
            continue
        if any(not part or part in (".", "..") for part in parts):
            continue
        filename, dirs = parts[-1], parts[1:-1]
        if not filename.lower().endswith(_IMAGE_EXTS):
            continue
        index.setdefault("/".join(dirs), []).append(filename)
    for files in index.values():
        files.sort()
    return index


with open(TREE_FIXTURE, encoding="utf-8") as fh:
    TREE = tree_dirs(json.load(fh))

# Channel-Mapparr's country map, as the oracle side reads it (plugin.py): the
# step-9 fixes (NO -> nordic/norway, GB -uk suffix, percent encoding) live in
# the Rust `core::logos`, so the oracle keeps CM's originals here and the diffs
# they cause attribute to country-dirs/normalization.
CM_COUNTRY_DIR_MAP = {
    "US": "united-states", "CA": "canada", "UK": "united-kingdom",
    "GB": "united-kingdom", "AU": "australia", "DE": "germany",
    "FR": "france", "IT": "italy", "ES": "spain", "MX": "mexico",
    "BR": "brazil", "IN": "india", "IE": "ireland", "NL": "netherlands",
    "NO": "norway", "PL": "poland",
}
CM_STATION_SUBDIRS = {"united-states": "united-states/us-local"}


def raw_url(repo, branch, directory, filename):
    return "https://raw.githubusercontent.com/%s/%s/countries/%s/%s" % (
        repo, branch, directory, filename)


# ---------------------------------------------------------------------------
# Fixtures: the world both sides decide over
# ---------------------------------------------------------------------------

MANAGER = [
    {"id": 71, "name": "ESPN", "url": "https://example.com/manager-espn.png"},
    {"id": 72, "name": "Fallback", "url": "https://example.com/default.png"},
    {"id": 70, "name": "HBO", "url": "https://example.com/hbo.png"},
    {"id": 90, "name": "Stale Logo", "url": "https://example.com/stale-food-network.png"},
]
ENTRIES = [
    {"id": 40, "name": "CNN entry", "icon_url": "https://example.com/epg-cnn.png"},
    {"id": 41, "name": "No icon entry", "icon_url": ""},
]


def representative_world():
    """Channels exercising every tier, the order setting, replace vs apply,
    station formats, country dirs, and the no-match default — then a broad
    seeded half (popular networks plain and decorated, merged US content
    names, OTA station names, UK names) over the real recorded tree."""
    channels = []
    next_id = [1]

    def add(name, group="Test", logo_id=None, epg_id=None):
        channels.append({"id": next_id[0], "name": name, "group": group,
                         "logo_id": logo_id, "epg_id": epg_id})
        next_id[0] += 1

    add("CNN", epg_id=40)  # EPG icon tier (also has cnn-us.png for tv-logos)
    add("ESPN")  # Logo Manager exact name (also has espn-us.png)
    add("Food Network")  # tv-logos fuzzy
    add("CBS - AL Birmingham (WIAT)")  # station file, us-local
    add("NBC - LA Alexandria (KALB)")  # station file picks the NBC feed
    add("BBC One")  # GB: the repo names the file -uk
    add("NRK1")  # NO: nordic/norway (CM maps NO to a dir that does not exist)
    add("A&E")  # punctuation: CM deletes it, Rust spaces it
    add("E4")  # hyphenated between letter and digit (e-4)
    add("ITV2 +1", epg_id=41)  # timeshift: the -plus file
    add("Zzqz No Such Channel 999")  # no match anywhere: the default logo
    add("HBO", logo_id=70)  # has a logo: skipped unless replacing
    add("Food Network Archive", logo_id=90)  # stale logo: replace reassigns
    add("Discovery Channel")  # plain fuzzy, both sides

    # The broad half (seeded, over the REAL recorded tree): what an operator's
    # lineup actually holds, so a wrong logo shows up here before a user sees it.
    rng = random.Random(16)
    for name in POPULAR:
        add(name)
        add("US: %s HD" % name)
    content = sorted(name for name, category in merged_entries("US")
                     if category and category != "United States")
    for name in rng.sample(content, 80):
        add(name)
    for name in rng.sample(content, 30):
        add(rng.choice(["%s HD", "%s [FHD]", "US: %s", "%s (Backup)"]) % name)
    for name in ota_station_names(rng, 30):
        add(name)
    uk = sorted(name for name, _category in merged_entries("UK"))
    for name in rng.sample(uk, 20):
        add(name)
    return channels


POPULAR = ["ESPN2", "CNN", "Fox News Channel", "MSNBC", "HBO 2", "Cinemax", "Showtime",
           "Starz", "TNT", "TBS", "USA Network", "FX", "AMC", "History", "TLC", "HGTV",
           "Nickelodeon", "Disney Channel", "Cartoon Network", "Comedy Central", "Bravo",
           "Syfy", "Lifetime", "Hallmark Channel", "NFL Network", "NBA TV", "MLB Network",
           "Golf Channel", "CNBC", "The Weather Channel", "BET", "MTV",
           "National Geographic", "Paramount Network", "Freeform", "Investigation Discovery",
           "Animal Planet", "Travel Channel", "Fox Sports 1", "ESPNews"]


def merged_entries(code):
    path = os.path.join(REPO, "data", "merged", "channels", "%s.json" % code)
    with open(path, encoding="utf-8") as fh:
        return [(c["channel_name"], c.get("category") or "")
                for c in json.load(fh)["channels"] if c.get("channel_name")]


def ota_station_names(rng, limit):
    """Station names in CM's default OTA template (`{NETWORK} - {STATE} {CITY}
    ({CALLSIGN})`, what `names` writes), from the merged station table — full-power
    big-four/PBS/CW affiliates, seeded. Not drawn from the tree, so a station with
    no file is as likely as one with."""
    with open(os.path.join(REPO, "data", "merged", "stations.json"), encoding="utf-8") as fh:
        stations = json.load(fh)["stations"]
    picked = []
    for callsign in sorted(stations):
        station = stations[callsign]
        network = (station.get("network_affiliation") or "").split(",")[0].strip().upper()
        city = (station.get("community_served_city") or "").strip().title()
        state = (station.get("community_served_state") or "").strip().upper()
        base = callsign.split("-")[0]
        if network in ("ABC", "CBS", "NBC", "FOX", "PBS", "CW") and city and state \
                and len(base) == 4 and base[0] in "KW":
            picked.append("%s - %s %s (%s)" % (network, state, city, base))
    return rng.sample(picked, min(limit, len(picked)))


# ---------------------------------------------------------------------------
# CM decision (transcribed tier order, real matcher functions)
# ---------------------------------------------------------------------------

def cm_lists(country_codes):
    filelists = []
    for cc in country_codes:
        directory = CM_COUNTRY_DIR_MAP.get(cc)
        if not directory:
            continue
        files = TREE.get(directory, [])
        filelists.append((cc.lower(), directory, files))
    stations = {}
    for _suffix, directory, _files in filelists:
        station_dir = CM_STATION_SUBDIRS.get(directory)
        if station_dir and station_dir not in stations:
            stations[directory] = (station_dir, TREE.get(station_dir, []))
    return filelists, stations


def cm_dirs(filelists, stations):
    return [directory for _suffix, directory, _files in filelists] + [
        station_dir for station_dir, _files in stations.values()]


def cm_decide(channel, epg_icons, manager_by_name, filelists, stations, threshold,
              replace):
    """One channel through CM's `_assign_tv_logos` tier order (plugin.py:2544)."""
    name = channel["name"]
    has_logo = channel.get("logo_id") not in (None, 0, "0")
    if has_logo and not replace:
        return {"url": None, "tier": "skip", "file": None, "dir": None}
    current_url = channel.get("logo_url")
    matched_url, tier, found = None, None, (None, None)

    station = cm_logo.parse_station_name(name)
    if station:
        for _suffix, directory, _files in filelists:
            station_dir, station_files = stations.get(directory, (None, ()))
            station_file = cm_logo.match_station_logo(station, station_files or [])
            if station_file:
                matched_url = raw_url("tv-logo/tv-logos", "main", station_dir, station_file)
                tier, found = "station", (station_file, station_dir)
                break
    if not matched_url:
        for suffix, directory, files in filelists:
            matched_file = cm_logo.match_channel_to_logo(
                name, files, suffix, threshold=threshold)
            if matched_file:
                matched_url = raw_url("tv-logo/tv-logos", "main", directory, matched_file)
                tier, found = "fuzzy", (matched_file, directory)
                break
    if not matched_url and not has_logo:
        matched_url = epg_icons.get(channel["id"])
        if matched_url:
            tier = "epg"
    if not matched_url and not has_logo and station:
        for suffix, directory, files in filelists:
            network_file = cm_logo.network_logo(station[0], files, suffix)
            if network_file:
                matched_url = raw_url("tv-logo/tv-logos", "main", directory, network_file)
                tier, found = "network", (network_file, directory)
                break
    if not matched_url:
        return {"url": None, "tier": "none", "file": None, "dir": None}
    if current_url and current_url == matched_url:
        return {"url": matched_url, "tier": "already", "file": found[0], "dir": found[1]}
    return {"url": matched_url, "tier": tier, "file": found[0], "dir": found[1]}


# ---------------------------------------------------------------------------
# LU decision (transcribed tier order, real matcher functions)
# ---------------------------------------------------------------------------

def lu_decide(channel, epg_icons, manager_by_name, tv_files, country_suffix,
              country_dir):
    """One channel through LU's `_do_apply_logo_match` tiers (plugin.py:3170)."""
    name = channel["name"]
    if channel.get("logo_id") not in (None, 0, "0"):
        return {"url": None, "tier": "skip", "file": None, "dir": None}
    icon = epg_icons.get(channel["id"])
    if icon:
        return {"url": icon, "tier": "epg", "file": None, "dir": None}
    if name.lower() in manager_by_name:
        return {"url": manager_by_name[name.lower()], "tier": "manager",
                "file": None, "dir": None}
    if tv_files:
        matched_file = lu_logo.match_channel_to_logo(name, tv_files, country_suffix)
        if matched_file:
            return {"url": raw_url("tv-logo/tv-logos", "main", country_dir, matched_file),
                    "tier": "fuzzy", "file": matched_file, "dir": country_dir}
    return {"url": None, "tier": "none", "file": None, "dir": None}


# ---------------------------------------------------------------------------
# Rust side
# ---------------------------------------------------------------------------

def rust_dump(channels, settings):
    payload = {
        "channels": channels,
        "groups": [{"id": 10, "name": "Test"}],
        "logos": MANAGER,
        "entries": ENTRIES,
        "scope": {},
        "settings": settings,
        "tree": json.load(open(TREE_FIXTURE, encoding="utf-8")),
    }
    proc = subprocess.run(
        ["cargo", "run", "-q", "-p", "crosstalk-lineup", "--example", "logos_dump"],
        input=json.dumps(payload), capture_output=True, text=True, cwd=REPO)
    if proc.returncode != 0:
        raise RuntimeError("logos_dump failed: %s" % proc.stderr[-4000:])
    return {row["id"]: row for row in json.loads(proc.stdout)}


CASES = [
    {"name": "apply", "settings": {"default_logo": "Fallback"},
     "cm_countries": ["US"], "replace": False, "threshold": LOGO_MATCH_THRESHOLD},
    {"name": "replace", "settings": {"default_logo": "Fallback",
                                     "replace_existing": True},
     "cm_countries": ["US"], "replace": True, "threshold": CM_REPLACE_THRESHOLD},
    {"name": "order", "settings": {"default_logo": "Fallback",
                                   "logo_sources": "tv-logos, epg, logo_manager"},
     "cm_countries": ["US"], "replace": False, "threshold": LOGO_MATCH_THRESHOLD},
    {"name": "countries", "settings": {"default_logo": "",
                                      "tv_logos_countries": "GB, NO"},
     "cm_countries": ["US"], "replace": False, "threshold": LOGO_MATCH_THRESHOLD},
]


def fuzz_ratio(a, b):
    """The function the loaded matchers use: rapidfuzz when the environment has
    it (production), the difflib fallback otherwise (what
    `scripts/corpus/oracles.py` documents for this tree). Both matcher modules
    import it the same way, so one function serves both sides."""
    return cm_logo.fuzz.ratio(a, b)


def side_scores(name, files, suffix, mod):
    """The side's OWN score of its normalized channel against every normalized
    file, best first (`mod` is the side's own `logo_matcher`: CM's and LU's
    normalizers and scorers differ). CM scores with `_score` (spaced and
    space-removed ratio, the higher) and reads a timeshift as `<name> plus`
    first, then the plain name; LU scores plain `fuzz.ratio`. Each file keeps
    the best of the variants the side tries."""
    scorer = getattr(mod, "_score", None) or fuzz_ratio
    variants = [mod.normalize_channel_name(name)]
    if hasattr(mod, "split_timeshift"):
        base, timeshift = mod.split_timeshift(name)
        if timeshift:
            plain = mod.normalize_channel_name(base)
            variants = ["%s plus" % plain, plain]
    variants = [variant for variant in variants if variant]
    best = {}
    for filename in files:
        norm_file = mod.normalize_logo_filename(filename, suffix)
        if norm_file and variants:
            best[filename] = max(scorer(variant, norm_file) for variant in variants)
    return sorted(best.items(), key=lambda item: (-item[1], item[0]))


def station_words_ok(name, filename):
    """The station check: the name is station-shaped and the file carries the
    callsign and network words."""
    station = cm_logo.parse_station_name(name)
    if not station:
        return False
    words = filename.lower().rsplit(".", 1)[0].split("-")
    return station[0] in words and station[1] in words


RUST_ORDER_DEFAULT = ["epg_icon", "logo_manager", "tv_logos", "network"]
SOURCE_OF_TOKEN = {"epg": "epg_icon", "logo_manager": "logo_manager",
                   "tv-logos": "tv_logos", "network": "network", "default": "default"}


def rust_order(case):
    raw = case["settings"].get("logo_sources") or ""
    order = [SOURCE_OF_TOKEN[token.strip().lower()] for token in raw.split(",")
             if token.strip()]
    return order or list(RUST_ORDER_DEFAULT)


def attribute(channel, rust, other, other_name, other_dirs, case, mod, pool, lu):
    """One bucket per difference against one oracle side, each backed by a check
    (see the module docstring). `other_dirs` is the side's searched directories;
    `mod` is the side's own `logo_matcher` and `pool` its (suffix, dir, files)
    for score recomputation; `lu` is LU's row (the decided order's source)."""
    # The side's own threshold: CM replace follows sensitivity (80), LU is 85.
    threshold = case["threshold"] if other_name == "CM" else LOGO_MATCH_THRESHOLD
    has_logo = channel.get("logo_id") not in (None, 0, "0")
    if has_logo:
        # One side keeps the carried logo because of its replace/apply flag
        # (verified on each side's own skip: Rust's skip reason, the oracle's
        # `skip` tier).
        rust_skipped = rust["url"] is None and \
            rust["reason"].startswith("the channel already has a logo")
        if rust_skipped and other["url"]:
            return "replace-scope"
        if rust["url"] and other.get("tier") == "skip":
            return "replace-scope"
    rust_source = rust.get("source")
    if other_name == "CM" and other.get("tier") in ("station", "fuzzy") and (
            (rust_source == "epg_icon" and rust.get("epg_icon") == rust["url"]) or
            (rust_source == "logo_manager" and rust.get("manager_hit") == rust["url"])):
        # The unified order is LU's (decided step 7): EPG icon and Logo Manager
        # come before tv-logos, where CM goes station/fuzzy first and has no
        # Logo Manager tier at all (verified on Rust's own tier evidence).
        return "lu-order"
    rust_hit = rust.get("tv_hit") or {}
    rust_searched = {item["dir"] for item in rust.get("searched") or []}
    other_tier, other_file, other_dir = other.get("tier"), other.get("file"), \
        other.get("dir")

    if rust_source == "network":
        # CM's network fallback, ported (decision B): verified with CM's real
        # `network_logo` on the station's network word over Rust's own searched
        # country dirs (the URL Rust picked IS that file); LU has no such tier.
        station = cm_logo.parse_station_name(channel["name"])
        cm_url = None
        for item in rust.get("searched") or []:
            files = TREE.get(item["dir"], [])
            suffix = "uk" if item["code"] == "GB" else item["code"].lower()
            found = station and cm_logo.network_logo(station[0], files, suffix)
            if found and not cm_url:
                cm_url = raw_url("tv-logo/tv-logos", "main", item["dir"], found)
        if not cm_url or cm_url != rust["url"]:
            return "unattributed"
        if other_name == "LU" and other_tier in ("none", "skip"):
            return "network-logo"
        if other_name == "CM" and other_tier == "network" and \
                other_dir not in rust_searched:
            # Both sides take the network logo, each from its own first country
            # dir with one; CM's never was in Rust's searched list.
            return "country-dirs"
        return "unattributed"
    if other_tier == "network" and rust_source in (None, "default"):
        # CM's network fallback fires where Rust's does not: the configured
        # `logo_sources` leaves the `network` token out (verified on the case's
        # own order), or Rust never searched CM's network dir.
        if "network" not in rust_order(case):
            return "order"
        if other_dir not in rust_searched:
            return "country-dirs"
        return "unattributed"
    if rust_source == "tv_logos" and rust.get("zone_free") and \
            rust_hit.get("file") and other_tier in ("fuzzy", "none") and \
            rust_hit.get("dir") in set(other_dirs):
        # A zone feed shares its brand's logo: Rust matched the zone-free name.
        # Verified with the side's OWN matcher: given the zone-free name, it picks
        # Rust's file from the same dir.
        for suffix, directory, files in pool:
            if directory == rust_hit["dir"] and mod.match_channel_to_logo(
                    rust["zone_free"], files, suffix) == rust_hit["file"]:
                return "zone-feed"

    if rust_source in (None, "tv_logos", "default") and (
            other_tier in ("station", "fuzzy") or (rust_hit and other_tier == "none")):
        # Station-format first: one side takes the station file while the other
        # fuzzies or misses (LU has no station tier at all).
        if not rust_hit.get("file") and other_tier == "station":
            # Rust missed where the oracle station-hits: the tv tier never saw
            # the file (a dir gap) or refused it (a metric gap).
            if other_dir not in rust_searched:
                return "country-dirs"
            return "unattributed"
        if rust_hit.get("score") is None and rust_hit.get("file") and \
                other_tier == "fuzzy":
            if station_words_ok(channel["name"], rust_hit["file"]):
                return "station-format"
            return "unattributed"
        if rust_hit.get("score") is not None and other_tier == "station":
            if station_words_ok(channel["name"], other_file or ""):
                return "station-format"
            return "unattributed"
        # Country dirs: the winner lives where the other side never looked.
        if other_file and other_dir and other_dir not in rust_searched:
            return "country-dirs"
        if rust_hit.get("file") and other_tier == "none" and \
                rust_hit.get("dir") not in set(other_dirs):
            return "country-dirs"
        # Same tier (fuzzy), different files — or one side missing: the metrics
        # or the normalizers disagree.
        norm_channel = rust.get("norm_channel") or ""
        scored = {item["file"]: item for item in rust.get("tv") or []}
        rust_file = rust_hit.get("file")
        cm_files, cm_suffix = [], "us"
        for suffix, directory, files in pool:
            if directory == (rust_hit.get("dir") if rust_hit else other_dir):
                cm_files, cm_suffix = files, suffix
        cm_ranked = {name: score for name, score in
                     side_scores(channel["name"], cm_files, cm_suffix, mod)}
        if rust_file and other_file and rust_file != other_file:
            rust_scored = scored.get(rust_file, {}).get("score", 0)
            other_scored = scored.get(other_file, {}).get("score", 0)
            if rust_scored >= RUST_CUT > other_scored and \
                    cm_ranked.get(other_file, 0) >= cm_ranked.get(rust_file, 0):
                # Each side's own scorer prefers its own file. Same ratio
                # function, Rust-normalized names: does the winner flip?
                rust_norm = sorted(
                    ((name, fuzz_ratio(norm_channel, scored[name]["norm_file"]))
                     for name in cm_ranked if name in scored),
                    key=lambda item: (-item[1], item[0]))
                plugin_norm = sorted(cm_ranked.items(), key=lambda item: (-item[1], item[0]))
                if rust_norm and rust_norm[0][0] == rust_file and \
                        plugin_norm and plugin_norm[0][0] == other_file:
                    return "normalization"
                return "similarity"
            if threshold <= cm_ranked.get(other_file, 0) < RUST_CUT and \
                    other_scored < RUST_CUT:
                return "threshold"
            return "similarity" if (rust_scored >= RUST_CUT) != (
                cm_ranked.get(other_file, 0) >= threshold) else "unattributed"
        if rust_file and not other_file:
            if rust_hit.get("dir") in set(other_dirs) and \
                    scored.get(rust_file, {}).get("score", 0) >= RUST_CUT:
                cm_of_rust = cm_ranked.get(rust_file, 0)
                if cm_of_rust < threshold:
                    return "similarity"
            return "unattributed"
        if other_file and not rust_file:
            unified = scored.get(other_file, {}).get("score")
            cm_of_other = cm_ranked.get(other_file, 0)
            if unified is not None and unified < RUST_CUT and \
                    cm_of_other >= threshold:
                if threshold < RUST_CUT and cm_of_other < RUST_CUT:
                    return "threshold"
                return "similarity"
            return "unattributed"
        return "unattributed"

    tier_of = {"station": "tv_logos", "fuzzy": "tv_logos", "epg": "epg_icon",
               "manager": "logo_manager", "network": "network", "none": None,
               "already": None, "skip": None}
    evidence = {"epg_icon": bool(rust.get("epg_icon")),
                "logo_manager": bool(rust.get("manager_hit")),
                "tv_logos": bool(rust_hit.get("file"))}
    other_source = tier_of.get(other_tier)
    if other_source and rust_source and other_source != rust_source and \
            {other_source, rust_source} <= set(evidence):
        # Both tiers fire on the Rust side too (its own evidence), and Rust's
        # configured order puts its winner's tier first.
        order = rust_order(case)
        if evidence[other_source] and evidence[rust_source] and \
                rust_source in order and other_source in order and \
                order.index(rust_source) < order.index(other_source):
            return "order"
    if rust_source == "default" and not other.get("url"):
        return "default"
    return "unattributed"


def current_url(channel, manager_urls):
    logo_id = channel.get("logo_id")
    if logo_id in (None, 0, "0"):
        return None
    return manager_urls.get(logo_id)


def run_case(channels, case, show):
    manager_by_name = {item["name"].lower(): item["url"] for item in MANAGER}
    manager_urls = {item["id"]: item["url"] for item in MANAGER}
    epg_icons = {item["id"]: item["icon_url"] for item in ENTRIES
                 if item.get("icon_url")}
    entry_by_channel = {channel["id"]: epg_icons.get(channel.get("epg_id"))
                        for channel in channels}
    filelists, stations = cm_lists(case["cm_countries"])
    lu_dir = "united-states"
    lu_files = TREE.get(lu_dir, [])
    lu_dirs = [lu_dir]

    for channel in channels:
        if channel.get("logo_id") in manager_urls:
            channel["logo_url"] = manager_urls[channel["logo_id"]]

    rust_rows = rust_dump(channels, case["settings"])
    buckets = {bucket: [] for bucket in BUCKETS}
    agreements = 0
    for channel in channels:
        rust = rust_rows[channel["id"]]
        cm = cm_decide(channel, entry_by_channel, manager_by_name, filelists,
                       stations, case["threshold"], case["replace"])
        lu = lu_decide(channel, entry_by_channel, manager_by_name, lu_files,
                       "us", lu_dir)
        rust_url = rust["url"] or current_url(channel, manager_urls)
        cm_url = cm["url"] or current_url(channel, manager_urls)
        lu_url = lu["url"] or current_url(channel, manager_urls)
        if rust_url == cm_url and rust_url == lu_url:
            agreements += 1
            continue
        # CM is the primary oracle; a row matching exactly one oracle attributes
        # against the other, with that side's own pool and normalizers.
        if rust_url == cm_url:
            other, other_dirs, name, mod, pool = \
                lu, lu_dirs, "LU", lu_logo, [("us", lu_dir, lu_files)]
        else:
            other, other_dirs, name, mod, pool = \
                cm, cm_dirs(filelists, stations), "CM", cm_logo, filelists
        bucket = attribute(channel, rust, other, name, other_dirs, case, mod, pool, lu)
        buckets.setdefault(bucket, buckets["unattributed"]).append({
            "channel": channel["name"],
            "oracle": name,
            "cm": {"url": cm["url"], "tier": cm["tier"]},
            "lu": {"url": lu["url"], "tier": lu["tier"]},
            "rust": {"url": rust["url"], "source": rust["source"]},
        })
    return {"case": case["name"], "channels": len(channels), "agreements": agreements,
            "buckets": buckets}


BUCKETS = ("replace-scope", "lu-order", "network-logo", "zone-feed",
           "station-format", "country-dirs", "similarity", "normalization", "threshold", "order",
           "default", "unattributed")


def short(url):
    if not url:
        return "none"
    return url.rsplit("/", 1)[-1]


def render_buckets(result, show):
    lines = ["- %d/%d channels agree with both oracles on the final logo" % (
        result["agreements"], result["channels"]),
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
            lines.append("- `%s` vs %s: CM %s (%s) / LU %s (%s) vs Rust %s (%s)" % (
                item["channel"], item["oracle"], short(item["cm"]["url"]),
                item["cm"]["tier"], short(item["lu"]["url"]), item["lu"]["tier"],
                short(item["rust"]["url"]), item["rust"]["source"]))
        if len(items) > show:
            lines.append("  ... and %d more" % (len(items) - show))
        lines += [""]
    return lines


def render(results, show):
    lines = [
        "<!-- Generated by scripts/oracle_report_lineup_logos.py; do not edit. -->",
        "<!-- Reproduce with: python3 -I scripts/oracle_report_lineup_logos.py -->",
        "# Lineup logos oracle diff report (step 16)",
        "",
        "Lineup `logos_*` assigns each in-scope channel one logo through the unified",
        "resolver (EPG icon -> Logo Manager exact name -> tv-logos -> network logo",
        "-> default, in the configured `logo_sources` order) over the same recorded",
        "tree both plugin sides read; Channel-Mapparr and Lineuparr are the oracles. CM is the",
        "primary oracle (the wider tiers); a row matching exactly one oracle attributes",
        "against the other. Every difference below traces to the tier order, the",
        "unified similarity, or a documented fix; the unattributed list is the review",
        "surface (bar: 0).",
        "",
        "One world: REPRESENTATIVE — one channel per tier, station formats, country",
        "dirs, the replace/apply split, the order setting, unmatched names; then a",
        "broad seeded half (popular networks plain and `US: X HD`, merged US content",
        "names plain and decorated, OTA station names in the `names` template, UK",
        "names) over the REAL recorded tree (`scripts/data/tv_logos_tree.json`,",
        "tv-logos `main` recorded 2026-11-07).",
        "Four settings cases: `apply` (channels with a logo skipped, threshold 85),",
        "`replace` (every channel considered, CM threshold 80 at the default normal",
        "sensitivity), `order` (`logo_sources = tv-logos, epg, logo_manager`, CM/LU",
        "keep their fixed orders), `countries` (`tv_logos_countries = GB, NO` with",
        "no default, CM/LU searching `US` only).",
        "",
        "## Inputs",
        "",
        "- CM oracle: `reference/Dispatcharr-Channel-Maparr-Plugin` `_assign_tv_logos`",
        "  tier order with the plugin's real `logo_matcher` functions",
        "  (`parse_station_name`, `match_station_logo`, `match_channel_to_logo`,",
        "  `network_logo`, `build_logo_url`) over the recorded tree parsed into",
        "  per-directory file lists exactly like `TvLogosIndex::from_tree` (direct",
        "  `blob` children of `countries/<dir>`). CM's original `NO -> norway` map",
        "  and `lower()` suffix strip are kept on the oracle side: the step-9 fixes",
        "  (`nordic/norway`, the `-uk` suffix, percent-encoding) live in `core::logos`,",
        "  so the diffs they cause attribute to `country-dirs`/`normalization`.",
        "- LU oracle: `reference/Dispatcharr-Lineuparr-Plugin` `_do_apply_logo_match`",
        "  tiers with LU's real `logo_matcher` functions over the single",
        "  `united-states` directory (the lineup country). LU has no replace mode: it",
        "  always skips channels with a logo.",
        "- Rust: `cargo run -p crosstalk-lineup --example logos_dump` (the action's own",
        "  `decide_logo`; the tv-logos match runs on the channel name without its",
        "  leading country marker (`US: CNN HD` -> `CNN HD`), a zone feed's fuzzy match",
        "  on its zone-free name (`Showtime (W)` -> `Showtime`) — with the per-file",
        "  unified scores that verify a `similarity` attribution both ways).",
        "- Plugin-side scores are each side's own: CM's `_score` (spaced and",
        "  space-removed ratio, `<name> plus` first for a timeshift), LU's `fuzz.ratio`;",
        "  thresholds are each side's own (CM replace 80, LU always 85).",
        "- EPG icons and Logo Manager entries are fixtures both sides share (see the",
        "  script): no network anywhere; the tree is recorded, never fetched.",
        "",
    ]
    for result in results:
        lines += ["## Case `%s`" % result["case"], ""]
        lines += render_buckets(result, show)
    lines += [
        "## Bucket notes",
        "",
        "- `replace-scope`: one side keeps the carried logo because of its",
        "  replace/apply flag (LU always skips; CM apply skips, CM replace",
        "  reconsiders) — verified on the channel's carried logo.",
        "- `lu-order`: Rust's winner is its EPG icon or Logo Manager hit while CM took",
        "  a tv-logos file — the unified order is LU's (decided in step 7: EPG icon and",
        "  Logo Manager before tv-logos) and CM has no Logo Manager tier.",
        "- `network-logo`: the station's network logo (CM's fallback, ported into",
        "  `core::logos`, orchestrator decision B; channels with no logo only, like",
        "  CM) where LU has no such tier — verified with CM's real `network_logo`.",
        "- `zone-feed`: a zone feed (`Showtime (W)`) takes its brand's logo — the",
        "  tv-logos match runs on the zone-free name (the engine's own zone reading),",
        "  unless the full name's file itself names the zone — verified with the",
        "  other side's own matcher on the zone-free name.",
        "- `station-format`: one side takes the station file while the other fuzzies",
        "  or misses (verified on the callsign/network words) — LU has no station",
        "  tier at all.",
        "- `country-dirs`: the winners live in directories only one side searched",
        "  (CM's `channel_databases` list vs the identity/override/enabled countries),",
        "  network fallback included (each side's first searched dir with one).",
        "- `similarity`: same tv-logos fuzzy tier, different files — each side's own",
        "  scorer prefers its own file (the unified engine similarity from the dump,",
        "  the plugins' `fuzz.ratio` recomputed here).",
        "- `normalization`: same tier, different files, and the winner flips with the",
        "  normalizer — under one `fuzz.ratio`, the plugin-normalized pair ranks the",
        "  plugin file first while the Rust-normalized pair from the dump ranks the",
        "  Rust file first.",
        "- `threshold`: CM's replace threshold (80) accepts a file the unified 85",
        "  refuses — the plugin ratio sits in [80, 85) while the unified similarity",
        "  sits below 85.",
        "- `order`: both winners come from tiers both sides consult, in a different",
        "  position — both tiers fire on the Rust side's own evidence and the",
        "  configured `logo_sources` order puts Rust's first; or CM's network fallback",
        "  wins where the configured `logo_sources` leaves the `network` token out.",
        "- `default`: the unified default-logo safety net fires where the tv-logos",
        "  actions have nothing — verified on the Rust `default` source with no",
        "  plugin URL.",
        "",
    ]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="regenerate and fail unless byte-identical to the report")
    parser.add_argument("--show", type=int, default=15, help="differences per bucket")
    args = parser.parse_args(argv)

    world = representative_world()
    results = [run_case(world, case, args.show) for case in CASES]

    text = render(results, args.show)
    if args.check:
        with open(REPORT, encoding="utf-8") as fh:
            committed = fh.read()
        if committed != text:
            print("%s is stale: regenerate with" % REPORT, file=sys.stderr)
            print("  python3 -I scripts/oracle_report_lineup_logos.py", file=sys.stderr)
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
