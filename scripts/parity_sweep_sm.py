#!/usr/bin/env python3
"""Parity sweep: the Rust matcher's Stream-Mapparr profile against the plugin's own code.

`MatchProfile::StreamMapparr` is a flag set on the one matcher engine, not a copy of it.
This script is how those flags are checked beyond the fixed golden corpus: it imports
Stream-Mapparr's own `fuzzy_matcher.py` and `country.py` from `reference/` (neither needs
Django), generates a corpus that concentrates on the places the profile diverges from
EPG Janitor's, and compares every primitive.

The corpus has four parts:

  * an adversarial list aimed at this profile: zone words (including the single-time-zone
    country guard), designators (`F1`, `TF1`, `UHD 1`), alias folding (case and
    whitespace variants), Unicode, and the provider/tag shapes both plugins normalise;
  * every channel name and callsign in Stream-Mapparr's sixteen channel databases and its
    `networks.json` station table;
  * generated zone names, so a bare `East`/`West`/`Pacific` is exercised in and out of a
    single-time-zone country;
  * generated alias queries (case/whitespace variants of real alias keys).

Usage:
    python3 -I scripts/parity_sweep_sm.py [--limit N] [--kinds ...] [--tags] [--show N]
    python3 -I scripts/parity_sweep_sm.py --kinds callsign,zone,country --limit 3000

Exit status is 1 when any mismatch is found, so it can gate CI.
"""
from __future__ import annotations

import argparse
import itertools
import json
import logging
import os
import subprocess
import sys

# The sweep imports the plugin in-process; don't leave __pycache__ behind in the
# reference checkout on every run.
sys.dont_write_bytecode = True

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLUGIN_INNER = os.path.join(REPO, "reference", "Stream-Mapparr", "Stream-Mapparr")

PROFILE = "stream-mapparr"

ADVERSARIAL = [
    # Zone words: bare, parenthesized, abbreviated, and decorations after the marker.
    "HBO East", "HBO West", "HBO (W)", "HBO (E)", "HBO (WEST)", "HBO (EAST)",
    "ESPN Pacific", "ESPN (Pacific)", "ESPN (PT)", "ESPN West HD", "HBO East 4K",
    "HBO West HD 4K", "(W) HBO", "HBO WESTERN", "HBO E!", "W Network",
    "E! Entertainment Television", "West Palm Beach", "WEST HASTINGS, NE",
    "WEST HARTFORD, CT", "West TV", "Slow TV", "ESPN West (W)",
    # Zone words in a single-time-zone country: regions, not feeds.
    "BBC One East", "BBC One West", "UK: BBC One East", "UK: BBC One West",
    "UK: Sky Sports West", "DE: Sky Sport Ost", "FR: France 2 West",
    "NL: NPO 1 West", "IN: Star Plus East", "US: HBO West", "CA: TSN West",
    "CNN Brasil West", "AU: Fox Sports West", "NO: NRK 1 Vest",
    # Designators the shared core splits apart.
    "Sky Sports F1", "Sky Sports F 1", "Sky Sports F1 UHD", "Sky Sports UHD 1",
    "TF1", "TF 1", "E4", "M6", "ITV 4", "ITV4", "CITV", "CBeebies",
    "High Street TV 1", "That's 60s", "at 49ers", "Fox Sports 1", "Fox Sports 2",
    "BBC 1", "BBC One", "BBC 2", "BBC Two", "Sky Sports News 13",
    "Sky Sports News 14", "ESPN 2", "HBO 2", "5USA", "5 USA",
    # Alias folding: case and whitespace variants of real keys.
    "msnbc", "MSNBC", " msnbc ", "MS Now", "ms now", "MS NOW ",
    "golf channel", "GOLF Channel", "Golf Channel ", "nat geo wild", "Nat Geo Wild",
    "cnn en español", "CNN En Español", "unimás", "UniMás", "galavisión",
    # Normalisation shapes (the shared core, not Janitor's override).
    "US: USA Network HD", "US| ESPN", "[US] CNN", "UK: BBC One", "FR - Canal+ FHD",
    "FR beIN SPORTS MAX", "┃US┃ ESPN", "RK: beIN Sports Xtra", "GO: ESPN",
    "ADULT: Something", "PRIME: Something", "USFHD SOMETHING", "UKSD: Sky Sports",
    "USA  ABC", "USA Network", "US Open", "US Armenian TV", "IN PROGRESS: Something",
    "NO LIMIT TV", "MEO: Benfica TV", "NOW: Sky Sports", "(NOW) Sky Sports",
    # Quality/tag shapes where the core and the plugin's override disagree.
    "Discovery Channel 4K", "ESPN 4K HDR", "HBO HD", "ESPN [FHD]", "TNT UHD RAW",
    "ESPN 1080p", "ESPN 720P", "Volume 100 I", "Channel 4", "CNN [HD] USA",
    "HD:", "4K: Channel", "SKY NEWS FHD rec", "TNT HEVC", "ESPN HDR",
    # Regional/misc/geographic markers.
    "Comedy Central", "The Atlantic", "Big Ten Network (Southern California) Alternate",
    "Penthouse (TEN) On Demand", "(D1) CBS", "(PRIME) FOX News", "(A) Channel",
    "(CX) Channel", "NBA League Pass (Spurs)", "ESPN2 (Pittsburgh)",
    # Callsign shapes.
    "ABC (WABC)", "KGTV (ABC)", "WWL", "(WWL)", "KING 5", "WAVE 3", "WOOD TV8",
    "WHO 13", "WABC-DT", "WXYZ-TV", "KSVI (ABC)", "KILN (ABC)", "WHIP (FOX)",
    "WAVE (NBC)", "D1-WABC", "US KING 5",
    # Suffix/prefix rules: the core strips trailing Network/Channel/TV unconditionally.
    "Justice Network", "True Crime Network", "West TV", "Slow TV", "The Playground",
    "DMX: The Playground", "Justice Central.TV", "Racing.com", "97.2 Radio",
    "U&Yesterday", "A&E", "MeTV", "truTV", "NewsNation", "getTV", "DangerTV",
    # Stylised Unicode and emoji.
    "\u1d3f\u1d2c\u1d42", "WEATHERNATION \u1d3f\u1d2c\u1d42",
    "Gold \u1d3f\u1d2c\u1d42", "beIN SP\u26bdRTS", "BE\u26bd",
    "\ufe0fHULU", "\u266c Channel", "\u263e Channel", "\u2605 CNN \u2605",
    "\u25c9 CNN", "Channel \u00b2", "Ч\u200b\u200b CNN",
    # Non-Latin scripts and Turkish/Kelvin case folding.
    "Россия 1", "beИN SPORTS", "日本 NHK", "العربية", "हिंदी समाचार",
    "US \u0131WABC", "US\u0131KGTV (ABC)", "(WABC-\u0130)", "WABC (AB\u0131)",
    "ABC WABC.\u0131o", "network \u0131on", "\u0130ND", "\u212aGTV (ABC)",
    "D\u0131:ITV114", "BBC Tr\u0131:avel (12)",
    # Whitespace oddities and junk.
    "\x1c", "Network\x1c", "  spaced   out  ", "Tab\there", "New\nline",
    "US \x1c|\x1c BBC 1", "", " ", "()", "(", ")", "[", "]", "-", "::", "|||", "+",
    "++1", "MoreMax", "Cinemax", "Cinemax Action Max", "ActionMax",
]

TAG_SETS = [
    ["HD"],
    ["[HD]"],
    ["(East)"],
    ["quality"],
    ["regional"],
    ["geographic"],
    ["misc"],
    ["News"],
    ["F 1"],
    ["[FOO]"],
    [""],
    ["HD", "CNN"],
    ["\u0131"],
]

# Operator prefix maps for the country kind, including malformed lines the parser must
# degrade on rather than raise.
OVERRIDE_RAWS = [
    "",
    "NOW=GB",
    "NOW=GB, MEO=PT",
    "NOW=GB\nMEO=PT\nPPT=ZZ\n# comment\n\n=US\nBAD",
]


def add_generated(names: list[str], seen: set[str]) -> None:
    """Zone and alias queries the shipped databases do not contain."""

    def add(name: str) -> None:
        if name and name not in seen:
            seen.add(name)
            names.append(name)

    countries = ["", "UK: ", "US: ", "DE: ", "IN: ", "CA: ", "NO: ", "BR: ", "FR: "]
    stems = ["HBO", "ESPN", "BBC One", "Sky Sports", "TSN", "Fox Sports", "NPO 1"]
    markers = ["East", "West", "Pacific", "(E)", "(W)", "(PT)", "(Pacific)", "EAST", "WEST"]
    for country in countries:
        for stem in stems:
            for marker in markers:
                add(f"{country}{stem} {marker}")
                add(f"{country}{stem} {marker} HD")

    for stem in ["Sky Sports", "Fox Sports", "TF", "E", "M", "ITV", "BBC", "5"]:
        for digit in ["1", "2", "3", "12"]:
            add(f"{stem}{digit}")
            add(f"{stem} {digit}")
    for name in ["F1", "TF1", "E4", "M6", "SHOW 1"]:
        for prefix in ["", "UK: ", "Sky Sports ", "Racing "]:
            add(f"{prefix}{name}")
            add(f"{prefix}{name} UHD")


def corpus(limit: int | None) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    def add(name: str) -> None:
        if name and name not in seen:
            seen.add(name)
            names.append(name)

    for entry in ADVERSARIAL:
        add(entry)
    add_generated(names, seen)
    if os.path.isdir(PLUGIN_INNER):
        for entry in sorted(os.listdir(PLUGIN_INNER)):
            if not entry.endswith("_channels.json"):
                continue
            with open(os.path.join(PLUGIN_INNER, entry), encoding="utf-8") as fh:
                payload = json.load(fh)
            for channel in payload.get("channels", []):
                add(channel.get("channel_name") or "")
                add(channel.get("callsign") or "")
        stations_path = os.path.join(PLUGIN_INNER, "networks.json")
        if os.path.exists(stations_path):
            with open(stations_path, encoding="utf-8") as fh:
                for station in json.load(fh):
                    add(station.get("callsign") or "")
    if limit:
        names = names[:limit]
    return names


def load_plugin():
    sys.path.insert(0, PLUGIN_INNER)
    import country  # noqa: PLC0415
    import fuzzy_matcher  # noqa: PLC0415
    from aliases import CHANNEL_ALIASES, COUNTRY_ALIASES  # noqa: PLC0415

    # Stream-Mapparr's logger is chatty at INFO, and loading sixteen databases plus the
    # station table would bury the mismatch output.
    logging.disable(logging.CRITICAL)
    matcher = fuzzy_matcher.FuzzyMatcher(plugin_dir=PLUGIN_INNER, match_threshold=85)
    return fuzzy_matcher, country, CHANNEL_ALIASES, COUNTRY_ALIASES, matcher


def merged_aliases(channel_aliases, country_aliases, country) -> dict:
    """`Plugin._build_alias_map`'s merge (plugin.py:3937), without Django.

    Built-ins first, then the selected country's overrides extending each key's list with
    duplicates removed. Custom aliases are a host setting and are not swept.
    """
    alias_map = {key: list(value) for key, value in channel_aliases.items()}
    if country:
        for key, value in country_aliases.get(str(country).upper(), {}).items():
            alias_map[key] = list(dict.fromkeys(alias_map.get(key, []) + list(value)))
    return alias_map


def run_rust(cases: list[dict], binary: str) -> list[dict]:
    requests = "\n".join(json.dumps(case) for case in cases)
    completed = subprocess.run(
        [binary], input=requests, capture_output=True, text=True, check=True
    )
    answers = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    if len(answers) != len(cases):
        raise SystemExit(f"Rust produced {len(answers)} answers for {len(cases)} cases")
    return answers


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="cap the corpus size")
    parser.add_argument("--tags", action="store_true", help="also sweep ignored tags")
    parser.add_argument("--show", type=int, default=25, help="mismatches to print")
    parser.add_argument(
        "--pool",
        type=int,
        default=60,
        help="candidate pool size per match/score/alias case",
    )
    parser.add_argument(
        "--pools",
        type=int,
        default=1,
        help="number of candidate pools, sampled at different offsets",
    )
    parser.add_argument(
        "--kinds",
        default="normalize,callsign,alias,match,score,zone,country,classify",
        help=(
            "comma-separated primitives: normalize, callsign, alias, match, score, zone, "
            "country, classify"
        ),
    )
    parser.add_argument(
        "--binary",
        default=os.path.join(REPO, "target", "debug", "examples", "normalize_io"),
        help="path to the normalize_io example binary",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)

    if not os.path.isdir(PLUGIN_INNER):
        print(
            f"reference plugin not found at {PLUGIN_INNER}; "
            "run `git submodule update --init` to check it out",
            file=sys.stderr,
        )
        return 2

    # Always rebuild: a stale local binary would otherwise be silently compared.
    print(f"building {args.binary} ...", file=sys.stderr)
    subprocess.run(
        ["cargo", "build", "-q", "-p", "crosstalk-matching", "--example", "normalize_io"],
        cwd=REPO,
        check=True,
    )

    names = corpus(args.limit)
    plugin = load_plugin()
    kinds = [kind.strip() for kind in args.kinds.split(",") if kind.strip()]

    status = 0
    for kind in kinds:
        if kind == "normalize":
            status |= sweep_normalize(names, plugin, args)
        elif kind == "callsign":
            status |= sweep_per_name("callsign", names, plugin, args)
        elif kind == "zone":
            status |= sweep_per_name("zone", names, plugin, args)
        elif kind == "country":
            status |= sweep_country(names, plugin, args)
        elif kind == "classify":
            status |= sweep_classify(plugin, args)
        elif kind == "alias":
            status |= sweep_alias(names, plugin, args)
        elif kind in ("match", "score"):
            status |= sweep_match(kind, names, plugin, args)
        else:
            print(f"unknown kind {kind!r}", file=sys.stderr)
            status |= 2
    return status


def sweep_normalize(names: list[str], plugin, args) -> int:
    _, _, _, _, matcher = plugin
    flag_combos = list(itertools.product([True, False], repeat=4))
    tag_sets = TAG_SETS if args.tags else [[]]

    cases = []
    for quality, regional, geographic, misc in flag_combos:
        for tags in tag_sets:
            for name in names:
                cases.append(
                    {
                        "profile": PROFILE,
                        "kind": "normalize",
                        "name": name,
                        "ignore_quality": quality,
                        "ignore_regional": regional,
                        "ignore_geographic": geographic,
                        "ignore_misc": misc,
                        "tags": tags,
                    }
                )
    # `remove_cinemax` is matching_core's opt-in; Stream-Mapparr sets it for a channel
    # whose name mentions `max`, so both settings are compared on those names.
    cinemax_names = [name for name in names if "max" in name.lower()][:20]
    for remove_cinemax in (False, True):
        for name in cinemax_names:
            cases.append(
                {
                    "profile": PROFILE,
                    "kind": "normalize",
                    "name": name,
                    "ignore_quality": True,
                    "ignore_regional": True,
                    "ignore_geographic": True,
                    "ignore_misc": True,
                    "remove_cinemax": remove_cinemax,
                    "tags": [],
                }
            )
    print(
        f"[normalize] {len(names)} names x {len(flag_combos)} flag combos x "
        f"{len(tag_sets)} tag sets + {2 * len(cinemax_names)} cinemax = "
        f"{len(cases)} cases",
        file=sys.stderr,
    )
    answers = run_rust(cases, args.binary)

    mismatches = []
    for case, answer in zip(cases, answers):
        expected = matcher.normalize_name(
            case["name"],
            user_ignored_tags=set(case["tags"]),
            ignore_quality=case["ignore_quality"],
            ignore_regional=case["ignore_regional"],
            ignore_geographic=case["ignore_geographic"],
            ignore_misc=case["ignore_misc"],
            remove_cinemax=bool(case.get("remove_cinemax")),
        )
        got = answer.get("normalized")
        if got != expected:
            mismatches.append((case, expected, got))

    print(f"[normalize] {len(mismatches)} mismatches out of {len(cases)} cases")
    for case, expected, got in mismatches[: args.show]:
        print(
            f"  name={case['name']!r} flags="
            f"{(case['ignore_quality'], case['ignore_regional'], case['ignore_geographic'], case['ignore_misc'])}"
            f" tags={case['tags']}\n    python={expected!r}\n    rust  ={got!r}"
        )
    if len(mismatches) > args.show:
        print(f"  ... and {len(mismatches) - args.show} more")
    return 1 if mismatches else 0


def sweep_per_name(kind: str, names: list[str], plugin, args) -> int:
    fuzzy_matcher, _, _, _, matcher = plugin
    cases = [{"profile": PROFILE, "kind": kind, "name": name} for name in names]
    answers = run_rust(cases, args.binary)

    mismatches = []
    for name, answer in zip(names, answers):
        if kind == "callsign":
            expected_call, expected_high = matcher._compute_callsign_with_confidence(name)
            expected = {"callsign": expected_call, "high_confidence": expected_high}
            got = {
                "callsign": answer.get("callsign"),
                "high_confidence": answer.get("high_confidence"),
            }
        else:
            expected = fuzzy_matcher.FuzzyMatcher.extract_zone(name)
            got = answer.get("zone")
        if got != expected:
            mismatches.append((name, expected, got))

    print(f"[{kind}] {len(mismatches)} mismatches out of {len(cases)} cases")
    for name, expected, got in mismatches[: args.show]:
        print(f"  name={name!r}\n    python={expected!r}\n    rust  ={got!r}")
    if len(mismatches) > args.show:
        print(f"  ... and {len(mismatches) - args.show} more")
    return 1 if mismatches else 0


def sweep_country(names: list[str], plugin, args) -> int:
    _, country_module, _, _, _ = plugin
    queries = names + [
        "USA Networks",
        "GBR Entertainment",
        "Canada",
        "Norway",
        "United States of America",
        "Great Britain TV",
        "NOW: Sky Sports",
        "(NOW) Sky Sports",
        "Right now on Sky",
    ]
    cases = []
    for index, name in enumerate(queries):
        cases.append(
            {
                "profile": PROFILE,
                "kind": "country",
                "name": name,
                "overrides_raw": OVERRIDE_RAWS[index % len(OVERRIDE_RAWS)],
            }
        )
    answers = run_rust(cases, args.binary)

    mismatches = []
    for case, answer in zip(cases, answers):
        overrides = country_module.parse_prefix_country_overrides(case["overrides_raw"])
        expected = (
            country_module.country_from_name(case["name"]),
            country_module.country_from_group(case["name"]),
            country_module.country_from_prefix_overrides(case["name"], overrides),
        )
        got = (answer.get("from_name"), answer.get("from_group"), answer.get("override"))
        if got != expected:
            mismatches.append((case, expected, got))

    print(f"[country] {len(mismatches)} mismatches out of {len(cases)} cases")
    for case, expected, got in mismatches[: args.show]:
        print(
            f"  name={case['name']!r} overrides={case['overrides_raw']!r}\n"
            f"    python={expected!r}\n    rust  ={got!r}"
        )
    if len(mismatches) > args.show:
        print(f"  ... and {len(mismatches) - args.show} more")
    return 1 if mismatches else 0


def sweep_classify(plugin, args) -> int:
    _, country_module, _, _, _ = plugin
    codes = [None, "US", "UK", "CA", "AU", "DE", "FR", "BR", "MX", "DO", "PE", "JP", "ZA"]
    cases = [
        {"kind": "classify", "channel": channel, "stream": stream}
        for channel in codes
        for stream in codes
    ]
    answers = run_rust(cases, args.binary)

    mismatches = []
    for case, answer in zip(cases, answers):
        expected = country_module.classify(case["channel"], case["stream"])
        if answer.get("relationship") != expected:
            mismatches.append((case, expected, answer.get("relationship")))

    print(f"[classify] {len(mismatches)} mismatches out of {len(cases)} cases")
    for case, expected, got in mismatches[: args.show]:
        print(
            f"  channel={case['channel']!r} stream={case['stream']!r}\n"
            f"    python={expected!r}\n    rust  ={got!r}"
        )
    return 1 if mismatches else 0


def sweep_alias(names: list[str], plugin, args) -> int:
    _, _, channel_aliases, country_aliases, matcher = plugin
    pool_size = min(args.pool, len(names))
    pool = names[:pool_size]
    # The plugin pre-normalises the stream list once per run; without the cache every
    # alias_lookup call re-normalises the whole pool and the sweep takes a minute.
    matcher.precompute_normalizations(pool)
    queries: list[str] = []
    seen: set[str] = set()
    for key in channel_aliases:
        for variant in (key, key.lower(), f"  {key} ", key.upper()):
            if variant not in seen:
                seen.add(variant)
                queries.append(variant)
        for alias in channel_aliases[key][:1]:
            if alias not in seen:
                seen.add(alias)
                queries.append(alias)
    for name in names[:50]:
        if name not in seen:
            seen.add(name)
            queries.append(name)

    cases = []
    for country in (None, "DO", "PE"):
        for query in queries:
            cases.append(
                {
                    "profile": PROFILE,
                    "kind": "alias",
                    "query": query,
                    "candidates": pool,
                    "tags": [],
                    "alias_country": country,
                }
            )
    print(
        f"[alias] {len(queries)} queries x 3 alias tables (none/DO/PE) x "
        f"{pool_size} candidates = {len(cases)} cases",
        file=sys.stderr,
    )
    answers = run_rust(cases, args.binary)

    mismatches = []
    for case, answer in zip(cases, answers):
        alias_map = merged_aliases(channel_aliases, country_aliases, case["alias_country"])
        expected = matcher.alias_lookup(case["query"], case["candidates"], alias_map)
        if answer.get("hits") != expected:
            mismatches.append((case, expected, answer.get("hits")))

    print(f"[alias] {len(mismatches)} mismatches out of {len(cases)} cases")
    for case, expected, got in mismatches[: args.show]:
        print(
            f"  query={case['query']!r} country={case['alias_country']!r}\n"
            f"    python={expected}\n    rust  ={got}"
        )
    if len(mismatches) > args.show:
        print(f"  ... and {len(mismatches) - args.show} more")
    return 1 if mismatches else 0


def sweep_match(kind: str, names: list[str], plugin, args) -> int:
    _, _, _, _, matcher = plugin
    pool_size = min(args.pool, len(names))
    queries = names[: args.limit] if args.limit else names
    tag_sets = [set(), {"quality", "regional", "geographic", "misc"}]

    pools = []
    for p in range(max(1, args.pools)):
        offset = (p * 7919) % max(1, len(names) - pool_size)
        pools.append([names[(offset + i * 31) % len(names)] for i in range(pool_size)])

    # Pool-major order: `precompute_normalizations` is called when the pool changes, so
    # grouping by pool keeps that to once per pool rather than once per case.
    cases = []
    for tags in tag_sets:
        for pool in pools:
            for query in queries:
                cases.append(
                    {
                        "profile": PROFILE,
                        "kind": kind,
                        "query": query,
                        "candidates": pool,
                        "tags": sorted(tags),
                    }
                )
    # `remove_cinemax` is the core's opt-in that Stream-Mapparr passes only for a channel
    # whose name mentions `max`; exercise both settings on those names. The pool always
    # carries the query itself plus a literal `Cinemax`: the flag must never strip the
    # query (the plugin cleans the channel without it), so a `Cinemax` query still
    # exact-matches either way, and a sampled pool without one would let a regression
    # that strips the query hide behind mutual `None`s.
    cinemax_names = [name for name in names if "max" in name.lower()][:20]
    for remove_cinemax in (False, True):
        for query in cinemax_names:
            pool = list(dict.fromkeys([query, "Cinemax", "MoreMax"] + pools[0]))[:60]
            cases.append(
                {
                    "profile": PROFILE,
                    "kind": kind,
                    "query": query,
                    "candidates": pool,
                    "tags": [],
                    "remove_cinemax": remove_cinemax,
                }
            )
    print(
        f"[{kind}] {len(queries)} queries x {len(pools)} pool(s) of {pool_size} x "
        f"{len(tag_sets)} tag sets + {2 * len(cinemax_names)} cinemax = {len(cases)} cases",
        file=sys.stderr,
    )
    answers = run_rust(cases, args.binary)

    mismatches = []
    last_pool = None
    for case, answer in zip(cases, answers):
        # Mirror the plugin: `precompute_normalizations` is called once per run with NO
        # tags (and no `remove_cinemax`), so the candidate side never sees either.
        pool_key = tuple(case["candidates"])
        if pool_key != last_pool:
            matcher.precompute_normalizations(case["candidates"])
            last_pool = pool_key
        tags = set(case.get("tags") or [])
        remove_cinemax = bool(case.get("remove_cinemax"))
        if kind == "match":
            name, score, match_type = matcher.fuzzy_match(
                case["query"], case["candidates"], tags, remove_cinemax=remove_cinemax
            )
            expected = None if name is None else [name, score, match_type]
            got = answer.get("match")
        else:
            name, score = matcher.find_best_match(
                case["query"], case["candidates"], tags, remove_cinemax=remove_cinemax
            )
            expected = None if name is None else [name, score]
            got = answer.get("best_match")
        if got != expected:
            mismatches.append((case, expected, got))

    print(f"[{kind}] {len(mismatches)} mismatches out of {len(cases)} cases")
    for case, expected, got in mismatches[: args.show]:
        print(
            f"  query={case['query']!r} tags={case.get('tags')} "
            f"cinemax={case.get('remove_cinemax')}\n"
            f"    python={expected!r}\n    rust  ={got!r}"
        )
    if len(mismatches) > args.show:
        print(f"  ... and {len(mismatches) - args.show} more")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
