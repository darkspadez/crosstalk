#!/usr/bin/env python3
"""Oracle diff report for step 16: lineup categorize vs Channel-Mapparr.

Lineup `categorize_*` moves each in-scope channel into the group named by its
DB category, classified on the unified engine (one ranker + one host cut);
Channel-Mapparr is the oracle. This script runs CM's real
`category_groups_dry_run` decision path (the `oracles.py` CM path — its
normalized-exact then fuzzy premium classification over CM's own DBs — plus
the category map `plugin.py` builds from `premium_channels_full`), against the
Rust side (`cargo run -p crosstalk-lineup --example categorize_dump`, the same
`decide_category` the action plans from), attributes every difference, and
writes `docs/oracle-reports/lineup-categorize.md`.

Usage:
    python3 -I scripts/oracle_report_lineup_categorize.py [--check] [--show N]

    --check   regenerate into a temp file and fail unless it is byte-identical
              to the committed report (the CI gate: the report reproduces).
    --show N  differences printed per bucket (default 15).

The CM decision path per channel (`plugin.py:3230-3260`), with defaults
(`channel_databases = "US"`, normal sensitivity = 80):

1. `norm_lower` of the channel name in the premium category map
   (`channel_name.lower()` -> category) is `Premium (Exact)`.
2. Else `get_candidates` token pre-filter + `fuzzy_match` winner, looked up in
   the map, is `Premium (Fuzzy - score: N)`.
3. A winner with no category is no classification (the map only holds entries
   that have one); no match is no classification. There is deliberately no
   callsign branch (plugin.py:3165-3177): OTA names take the premium path.
4. The comparison is on the category only: the move (`category` != current
   group) derives deterministically on both sides.

Attribution buckets (every difference lands in exactly one, each by a CHECK —
nothing lands in a bucket by elimination; what no check explains is
`unattributed`):

- `ota`: the Rust side refuses to classify an OTA identity (its callsign is a
  merged station, verified against `data/merged/stations.json`): CM has no
  callsign branch, and the unified ranker would otherwise reach the station's
  DB entry, whose category is the country (`United States`).
- `country-category`: decision A (fix over CM) — a category naming a country
  (any merged DB's `country_name`/`country_code`, e.g. `United States`) is no
  category. Verified: CM's category is such a label, or the Rust family's
  merged entries carry only such labels.
- `merged-data`: CM's winner is absent from the merged DB, or Rust's leader is
  absent from CM's `premium_channels`, or both sides pick the same entry and
  the two DBs give it different categories (CM's older per-country tables vs
  the merged newest-union tables).
- `cm-prefilter`: CM's own scorer accepts Rust's leader (>= 80) but its
  `get_candidates` token pre-filter never offers it.
- `cut`: CM's winner scores below the host cut on the unified ranker with no
  cut (verified from `categorize_dump`'s `probe_score` over the whole no-cut
  shortlist).
- `premium-canonical`: different DB winners, both present in both DBs, verified
  one of two ways: CM's own `fuzzy_match` rejects Rust's leader (< 80), or the
  unified ranker never scores CM's winner even with no cut (`probe_score` null).
- `no-category`: one side's winner carries no category (verified in that
  side's DB) while the other's does — the map only holds categorized entries.
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

REPORT = os.path.join(REPO, "docs", "oracle-reports", "lineup-categorize.md")
CM_THRESHOLD = 80
RUST_CUT = 95


# ---------------------------------------------------------------------------
# CM decision path
# ---------------------------------------------------------------------------

def cm_setup():
    """The CM oracle with its default US database (plugin.py:1665)."""
    oracle = oracles.get("cm")
    oracle.matcher.reload_databases(["US"])
    return oracle


def cm_category_match(matcher, name, ignored, category_map):
    """CM's premium classification for `name` (plugin.py:3230-3260).

    Returns `(category | None, match_type, winner | None)`. Ties break by DB
    order (the `oracles.py` rule), never by set iteration order.
    """
    norm_lower, _ = matcher._get_cached_norm(name, ignored)
    if norm_lower and norm_lower in category_map:
        matched_name, category = category_map[norm_lower]
        return (category, "Premium (Exact)", matched_name)
    pre = matcher.get_candidates(name, ignored)
    if pre is None:
        ordered = list(matcher.premium_channels)
    else:
        members = set(pre)
        ordered = [candidate for candidate in matcher.premium_channels
                   if candidate in members]
    if not ordered:
        return (None, "No Match", None)
    matched, _score, _match_type = matcher.fuzzy_match(name, ordered, ignored)
    if matched and matched.lower() in category_map:
        matched_name, category = category_map[matched.lower()]
        return (category, "Premium (Fuzzy)", matched_name)
    return (None, "No Match", matched)


def cm_decide(oracle, name, category_map):
    ignored = list(oracles.RUN_IGNORED_TAGS["cm"])
    matcher = oracle.matcher
    category, match_type, winner = cm_category_match(matcher, name, ignored, category_map)
    return {"category": category, "match_type": match_type, "winner": winner}


def cm_category_map(oracle):
    """CM's `category_map_premium`: `channel_name.lower()` -> (name, category)."""
    mapping = {}
    for channel_data in oracle.matcher.premium_channels_full:
        name = (channel_data.get("channel_name") or "").strip()
        category = (channel_data.get("category") or "").strip()
        if name and category:
            mapping[name.lower()] = (name, category)
    return mapping


# ---------------------------------------------------------------------------
# Worlds
# ---------------------------------------------------------------------------

def merged_us_entries():
    path = os.path.join(REPO, "data", "merged", "channels", "US.json")
    with open(path, encoding="utf-8") as fh:
        return [(c["channel_name"], c.get("category") or "")
                for c in json.load(fh)["channels"] if c.get("channel_name")]


def representative_world():
    """A representative world (seeded): categorized DB names plain and decorated
    (content categories, plus a regional/station slice), OTA station names,
    unmatched names, across a few groups."""
    rng = random.Random(16)
    groups = ["Test", "News", "Sports", "Movies"]
    scoped_groups = ["News", "Test"]
    channels = []
    next_id = [1]

    def add(name, group=None):
        group = group or rng.choice(groups)
        channels.append({"id": next_id[0], "name": name, "group": group})
        next_id[0] += 1

    # Stratified: 30,654 of the US DB's entries are stations/regional feeds whose
    # category is the country (`United States`); a flat sample would be ~95% of
    # those and barely exercise the content categories a move is for.
    entries = sorted((name, category) for name, category in merged_us_entries() if category)
    content = [entry for entry in entries if entry[1] != "United States"]
    regional = [entry for entry in entries if entry[1] == "United States"]
    for name, _category in rng.sample(content, 60) + rng.sample(regional, 20):
        add(name)
    for name, _category in rng.sample(content, 40):
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
    popular = ["ESPN", "ESPN2", "CNN", "Fox News Channel", "MSNBC", "HBO", "HBO 2",
               "Cinemax", "Showtime", "Starz", "TNT", "TBS", "USA Network", "FX", "AMC",
               "A&E", "History", "Discovery Channel", "TLC", "HGTV", "Food Network",
               "Nickelodeon", "Disney Channel", "Cartoon Network", "Comedy Central",
               "Bravo", "Syfy", "Lifetime", "Hallmark Channel", "NFL Network", "NBA TV",
               "MLB Network", "Golf Channel", "CNBC", "The Weather Channel", "BET", "MTV",
               "National Geographic"]
    for name in popular:
        add("US: %s HD" % name)
        add(name)
    # OTA station names: CM gives them no callsign branch, so they take the
    # premium path (or nothing) like any other name.
    for station in ["CBS - AL Birmingham (WIAT)", "NBC - LA Alexandria (KALB)",
                    "ABC (WABC) HD", "US: CBS 7 Lingfield"]:
        add(station)
    for name in ["Zzqz No Such Channel 999", "Qqx Obsolete Feed 12 HD",
                 "Zzqz Archive [HD]", "Oldies 1047"]:
        add(name)
    return channels, scoped_groups


def disputed_set(limit=30):
    """Names the plugins dispute: pairs where CM's verdict differs from the label
    or from every other plugin's — exactly where a classification diff can hide."""
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
                channels.append({"id": 10000 + len(channels), "name": name,
                                 "group": "Disputed"})
    return channels


# ---------------------------------------------------------------------------
# Rust side
# ---------------------------------------------------------------------------

def rust_dump(channels, settings, probes):
    channels = [dict(channel, probe=probes.get(channel["id"])) for channel in channels]
    payload = {"channels": channels, "scope": {}, "settings": settings}
    proc = subprocess.run(
        ["cargo", "run", "-q", "-p", "crosstalk-lineup", "--example", "categorize_dump"],
        input=json.dumps(payload), capture_output=True, text=True, cwd=REPO)
    if proc.returncode != 0:
        raise RuntimeError("categorize_dump failed: %s" % proc.stderr[-4000:])
    return {row["id"]: row for row in json.loads(proc.stdout)}


CASES = [
    {"name": "defaults", "settings": {}},
    {"name": "scoped", "settings": {"category_groups": "News, Test"}},
]


def normalize_callsign(callsign):
    """`FuzzyMatcher.normalize_callsign`: strip the broadcast suffix."""
    return re.sub(r"-(?:TV|CD|LP|DT|LD)$", "", callsign or "")


with open(os.path.join(REPO, "data", "merged", "stations.json"), encoding="utf-8") as _fh:
    _STATION_KEYS = set(json.load(_fh)["stations"])
_STATION_BASES = {normalize_callsign(key) for key in _STATION_KEYS}


def is_station(callsign):
    """The merged station table holds the callsign (full key, or the base
    spelling either way) — the Rust `names::station_for_callsign` lookup."""
    if not callsign:
        return False
    upper = callsign.strip().upper()
    return upper in _STATION_KEYS or normalize_callsign(upper) in _STATION_KEYS \
        or upper in _STATION_BASES or normalize_callsign(upper) in _STATION_BASES


def _country_labels():
    labels = set()
    directory = os.path.join(REPO, "data", "merged", "channels")
    for filename in sorted(os.listdir(directory)):
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(directory, filename), encoding="utf-8") as fh:
            payload = json.load(fh)
        labels.add(filename[:-5].lower())
        for key in ("country_name", "country_code"):
            if payload.get(key):
                labels.add(payload[key].strip().lower())
    return labels


COUNTRY_LABELS = _country_labels()


def is_country(category):
    """Decision A (fix over CM): a category naming a country is no category."""
    return bool(category) and category.strip().lower() in COUNTRY_LABELS


def _merged_categories_by_lower():
    out = {}
    for name, category in merged_us_entries():
        out.setdefault(name.lower(), set()).add(category)
    return out


MERGED_CATEGORIES = _merged_categories_by_lower()


def attribute(cm, rust, rust_db, merged_cat, cm_db_set, cm_cat_of, cm_pair_score,
              cm_prefilter_keeps):
    """One bucket per difference, each backed by a check (see the module docstring)."""
    cm_cat, rust_cat = cm["category"], rust["category"]
    if cm_cat == rust_cat:
        return None
    if rust["match_type"] == "OTA":
        return "ota" if is_station(rust["callsign"]) else "unattributed"
    if is_country(rust_cat):
        return "unattributed"  # decision A says this never happens: review it
    if is_country(cm_cat):
        # CM moves into a country-named group; the unified side reads that
        # label as no category and takes the identity family's real category
        # (or none).
        return "country-category"
    leader = rust["match_name"]
    if rust_cat is None and leader and \
            all(not c or is_country(c) for c in MERGED_CATEGORIES.get(leader.lower(), {""})):
        # Rust's family carries only country labels (verified in the merged DB).
        return "country-category"
    cm_base = cm["winner"]
    if (cm_base and cm_base not in rust_db) or (leader and leader not in cm_db_set):
        return "merged-data"
    if leader and cm_base and leader == cm_base:
        # Same entry, different category: the two DBs disagree on it.
        if merged_cat.get(leader, "") != cm_cat_of(cm_base):
            return "merged-data"
        return "unattributed"
    probe = rust.get("probe_score")
    if rust_cat is None:
        if leader is None:
            # Rust has no family at the cut; CM classifies.
            if probe is not None and probe < RUST_CUT:
                return "cut"
            if cm_base and probe is None:
                return "premium-canonical"
            return "unattributed"
        if not merged_cat.get(leader) and cm_cat_of(cm_base or ""):
            return "no-category"
        return "unattributed"
    if cm_cat is None:
        # CM classifies nothing while the unified ranker has a family.
        if cm_base and not cm_cat_of(cm_base) and merged_cat.get(leader):
            return "no-category"
        if cm_pair_score(leader) < CM_THRESHOLD:
            return "premium-canonical"
        if not cm_prefilter_keeps(leader):
            return "cm-prefilter"
        return "unattributed"
    # Both classify, differently, from different winners.
    if cm_pair_score(leader) < CM_THRESHOLD or probe is None:
        return "premium-canonical"
    if probe < RUST_CUT:
        return "cut"
    return "unattributed"


def run_case(oracle, channels, category_map, case, show, groups_filter=None):
    ignored = list(oracles.RUN_IGNORED_TAGS["cm"])
    matcher = oracle.matcher
    matcher.build_token_index(matcher.premium_channels, ignored)
    rust_db = set(name for name, _category in merged_us_entries())
    merged_cat = dict(merged_us_entries())
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

    if groups_filter:
        # CM's category scope, transcribed: exact case-sensitive group names, no
        # wildcards (`group_scope.py` `include_wildcards=False` branch). A token
        # naming no group fails, like `GroupScopeError`.
        tokens = [token.strip() for token in groups_filter.split(",") if token.strip()]
        available = {channel["group"] for channel in channels}
        if not any(token in available for token in tokens):
            raise RuntimeError(
                "None of the groups named in 'Category Organization Groups' could be "
                "found: %s" % ", ".join(tokens))
        channels = [channel for channel in channels if channel["group"] in tokens]

    cm_rows = {channel["id"]: cm_decide(oracle, channel["name"], category_map)
               for channel in channels}
    probes = {cid: cm["winner"] for cid, cm in cm_rows.items() if cm["winner"]}
    rust_rows = rust_dump(channels, case["settings"], probes)

    def cm_cat_of(name):
        return (category_map.get(name.lower()) or (None, ""))[1]

    buckets = {bucket: [] for bucket in BUCKETS}
    agreements = 0
    for channel in channels:
        cid = channel["id"]
        cm = cm_rows[cid]
        rust = rust_rows[cid]
        if cm["category"] == rust["category"]:
            agreements += 1
            continue
        bucket = attribute(cm, rust, rust_db, merged_cat, cm_db_set, cm_cat_of,
                           cm_pair_score_for(channel["name"]), cm_prefilter_for(channel["name"]))
        buckets.setdefault(bucket, buckets["unattributed"]).append({
            "channel": channel["name"],
            "cm": {"category": cm["category"], "match_type": cm["match_type"],
                   "winner": cm["winner"]},
            "rust": {"category": rust["category"], "match_type": rust["match_type"],
                     "match_name": rust["match_name"]},
        })
    return {"case": case["name"], "channels": len(channels), "agreements": agreements,
            "buckets": buckets}


BUCKETS = ("ota", "country-category", "merged-data", "cm-prefilter", "cut", "premium-canonical",
           "no-category", "unattributed")


def render_buckets(result, show):
    lines = ["- %d/%d channels agree on the category" % (result["agreements"],
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
                item["channel"], item["cm"]["category"], item["cm"]["match_type"],
                item["rust"]["category"], item["rust"]["match_type"]))
        if len(items) > show:
            lines.append("  ... and %d more" % (len(items) - show))
        lines += [""]
    return lines


def render(results, show):
    lines = [
        "<!-- Generated by scripts/oracle_report_lineup_categorize.py; do not edit. -->",
        "<!-- Reproduce with: python3 -I scripts/oracle_report_lineup_categorize.py -->",
        "# Lineup categorize oracle diff report (step 16)",
        "",
        "Lineup `categorize_*` moves each in-scope channel into the group named by its",
        "DB category, classified on the unified engine (one ranker at the host",
        "`[matching]` cut — default strict = %d); Channel-Mapparr is" % RUST_CUT,
        "the oracle (default US database, normal sensitivity = %d). CM's organize" % CM_THRESHOLD,
        "path has no callsign branch (`plugin.py:3165-3177`) and classifies by name",
        "only; the unified side never classifies an OTA identity (a merged-station",
        "callsign — the `names` OTA test), so no `Broadcast` or country group is",
        "created for a station. Every difference below traces to the merged data, the",
        "unified ranker, or a decided rule; the unattributed list is the review",
        "surface (bar: 0).",
        "",
        "Two worlds: REPRESENTATIVE (seeded — categorized DB names plain and decorated,",
        "popular national networks, OTA station names, unmatched names, across a few",
        "groups) and DISPUTED (names mined from `data/corpus/pairs.jsonl` where CM",
        "disagrees with the label or the other plugins). Two settings cases: `defaults`",
        "and `scoped` (`category_groups = \"News, Test\"`, CM's exact-name category scope,",
        "both sides).",
        "",
        "## Inputs",
        "",
        "- CM oracle: `reference/Dispatcharr-Channel-Maparr-Plugin`",
        "  `category_groups_dry_run` decision path via `scripts/corpus/oracles.py`",
        "  (normalized-exact then `get_candidates` + `fuzzy_match` premium path) over",
        "  CM's own DBs, with the category map `plugin.py` builds from",
        "  `premium_channels_full`. The comparison is on the category only: the move",
        "  derives deterministically from it on both sides.",
        "- Rust: `cargo run -p crosstalk-lineup --example categorize_dump` (the action's",
        "  own `decide_category` over the merged US DB at the host cut, plus the same",
        "  ranker's no-cut score of CM's winner, `probe_score`, over the whole",
        "  shortlist, which verifies a `cut` or `premium-canonical` attribution).",
        "- Scope: the dump applies `category_groups` exactly like CM (exact,",
        "  case-sensitive, fail-closed); `ignore_groups` is empty in both cases.",
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
        "- `ota`: the channel's identity callsign is a merged station (verified in",
        "  `data/merged/stations.json`), so the unified side leaves it where it is",
        "  (strictly safer: the station entries' DB category is the country).",
        "- `country-category`: a fix over CM (orchestrator decision A) — a category",
        "  naming a country (`United States`, `United Kingdom`, ... any merged DB's",
        "  `country_name`/`country_code`) is no category: the merged DBs mark",
        "  station and regional-feed entries with their country, so CM would move",
        "  those channels into a country-named group. The unified side never does",
        "  (it takes the identity family's real category, or leaves the channel).",
        "  Verified on CM's label or on the Rust family's merged entries.",
        "- `merged-data`: a winner absent from the other side's DB, or the same",
        "  winner categorized differently by the two DBs (CM's older per-country",
        "  tables vs `data/merged/`).",
        "- `cm-prefilter`: CM's own scorer accepts Rust's leader, but its",
        "  `get_candidates` token pre-filter never offers it.",
        "- `cut`: CM's winner scores below the host cut (%d) on the unified ranker" % RUST_CUT,
        "  with no cut — verified from `categorize_dump`'s `probe_score`, never assumed.",
        "- `premium-canonical`: different DB winners, both present in both DBs, verified",
        "  one of two ways: CM's own `fuzzy_match` rejects Rust's leader (< %d), or" % CM_THRESHOLD,
        "  the unified ranker never scores CM's winner even with no cut.",
        "- `no-category`: one side's winner carries no category in its own DB",
        "  (verified there) while the other's does — the category map only holds",
        "  categorized entries.",
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
    category_map = cm_category_map(oracle)
    world, scoped_groups = representative_world()
    disputed = disputed_set()
    results = []
    results.append(run_case(oracle, world, category_map, CASES[0], args.show))
    results.append(run_case(oracle, world, category_map, CASES[1], args.show,
                            groups_filter="News, Test"))
    # The disputed set runs once (defaults): it exists to show kinds, not rates.
    disputed_result = run_case(oracle, disputed, category_map, CASES[0], args.show)
    disputed_result["case"] = "defaults-disputed"
    results.append(disputed_result)

    text = render(results, args.show)
    if args.check:
        with open(REPORT, encoding="utf-8") as fh:
            committed = fh.read()
        if committed != text:
            print("%s is stale: regenerate with" % REPORT, file=sys.stderr)
            print("  python3 -I scripts/oracle_report_lineup_categorize.py", file=sys.stderr)
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
