#!/usr/bin/env python3
"""Parity sweep: compare the Rust normaliser against the Python plugin.

The Rust matcher is a port of `reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/
fuzzy_matcher.py`, and the contract is identical output. This script is how that is
checked beyond the fixed golden corpus: it builds a large corpus (every name in the
shipped channel databases plus adversarial Unicode, tags and prefixes), runs both
implementations over every flag combination, and reports mismatches.

Usage:
    python3 scripts/parity_sweep.py [--limit N] [--tags] [--show N]

Exit status is 1 when any mismatch is found, so it can gate CI.
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLUGIN_INNER = os.path.join(REPO, "reference", "Dispatcharr-EPG-Janitor-Plugin", "EPG-Janitor")
DATA = os.path.join(REPO, "data")

ADVERSARIAL = [
    # Provider prefixes, country tags and delimiters.
    "US: USA Network HD", "US| ESPN", "[US] CNN", "UK: BBC One", "UK| ITV 1",
    "┃US┃ ESPN", "│UK│ Network", "USA  ABC", "USA Network", "US Open",
    "US Open Tennis", "US Armenian TV", "GER TV", "FR beIN SPORTS MAX",
    "USFHD SOMETHING", "UKSD: Sky Sports", "(US) TNT", "TNT ┃US┃",
    "RK: beIN Sports Xtra", "TUBI: Something", "PLUTO TV Movies",
    # Quality and resolution markers.
    "Discovery Channel 4K", "HBO HD", "ESPN [FHD]", "Cinemax HD", "TNT UHD RAW",
    "ESPN 1080p", "ESPN 720P", "Foo 10800p", "Channel 4", "Volume 100 I",
    "CNN [HD] USA", "HD:", "4K: Channel", "SKY NEWS FHD rec",
    # Regional and time-shift markers.
    "HBO East", "HBO West", "HBO (W)", "Fox Sports West", "ESPN Pacific",
    "Comedy Central", "The Atlantic", "ITV2 +1", "Discovery+", "Disney+",
    "Paramount+", "Canal+Sport 360", "Sportsnet (P)", "HBO West Feed",
    # Numbers, ordinals, camel case and dots.
    "BBC Three", "BBC 3", "Three Angels Broadcasting Network", "ESPN 2", "HBO 2",
    "JusticeCentral.TV", "DangerTV", "NewsNation", "getTV", "MeTV", "truTV",
    "Racing.com", "97.2 Radio", "U&Yesterday", "E! Entertainment Television",
    # Parenthesised groups and callsigns.
    "(PRIME) FOX News", "(D1) CBS", "ABC (WABC)", "KGTV (ABC)", "WWL", "(WWL)",
    "KING 5", "WAVE 3", "WOOD TV8", "WHO 13", "WABC-DT", "WXYZ-TV",
    "Big Ten Network (Southern California) Alternate", "Penthouse (TEN) On Demand",
    "NBA League Pass (Spurs)", "ESPN2 (Pittsburgh)", "WFTX-DT6 (IMYS) (abc)",
    "(A) Channel", "(CX) Channel", "HBO (E)", "HBO (Pacific)",
    # Stylised Unicode and emoji.
    "\u1d3f\u1d2c\u1d42", "WEATHERNATION \u1d3f\u1d2c\u1d42",
    "Gold \u1d3f\u1d2c\u1d42", "beIN SP\u26bdRTS", "BE\u26bd",
    "\ufe0fHULU", "\u266c Channel", "\u263e Channel", "\u2605 CNN \u2605",
    "\U0001f174\U0001f182\U0001f17f\U0001f175", "\u2460 Channel", "Channel \u00b2",
    # Non-Latin scripts.
    "Россия 1", "beИN SPORTS", "日本 NHK", "العربية", "हिंदी समाचार",
    # Turkish dotless/dotted i and the Kelvin sign: Python's (?i) matches these wherever
    # `i`/`I` can match, Rust's simple case folding does not.
    "US \u0131WABC", "US\u0131KGTV (ABC)", "(WABC-\u0130)", "WABC (AB\u0131)",
    "ABC WABC.\u0131o", "network \u0131on", "\u0130ND", "\u212aGTV (ABC)",
    "D\u0131:ITV114", "BBC Tr\u0131:avel (12)",
    # Angle brackets: Python's re.escape leaves them alone, and an escaped `<` would be
    # a word boundary in the Rust engine, so a `<HD>` tag must still be strippable.
    "Foo <HD> Bar", "a>b channel",
    # Accents: NFKD splits these into letter + combining mark, which is where the two
    # regex engines disagree about word boundaries unless the port compensates.
    "T\u00e9l\u00e9-Qu\u00e9bec", "\u00e9HD ", "\u00e7ten", "M\u00fcnchen TV",
    "Sport \u00bd", "Sport \u2153", "D\u0131:ITV114", "BBC Tr\u0131:avel (12)",
    # Whitespace oddities, including the C0 separators Python treats as whitespace.
    "\x1c", "Network\x1c", "  spaced   out  ", "Tab\there", "New\nline",
    "\x1cFR \u2503NL\u2503 \u0280\u1d00\u1d21\u01c5",
    # Junk that must not crash.
    "", " ", "()", "(", ")", "[", "]", "-", "::", "|||", "+", "++1",
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
    ["<HD>"],
    ["\u0131"],
]


def corpus(limit: int | None) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    def add(name: str) -> None:
        if name not in seen:
            seen.add(name)
            names.append(name)

    for entry in ADVERSARIAL:
        add(entry)
    channels_dir = os.path.join(DATA, "channels")
    if os.path.isdir(channels_dir):
        for entry in sorted(os.listdir(channels_dir)):
            if not entry.endswith(".json"):
                continue
            with open(os.path.join(channels_dir, entry), encoding="utf-8") as fh:
                payload = json.load(fh)
            for channel in payload.get("channels", []):
                name = channel.get("channel_name") or ""
                if name:
                    add(name)
                callsign = channel.get("callsign")
                if callsign:
                    add(callsign)
    if limit:
        names = names[:limit]
    return names


def load_python_matcher():
    sys.path.insert(0, PLUGIN_INNER)
    import fuzzy_matcher  # noqa: PLC0415

    return fuzzy_matcher.FuzzyMatcher()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="cap the corpus size")
    parser.add_argument("--tags", action="store_true", help="also sweep ignored tags")
    parser.add_argument("--show", type=int, default=25, help="mismatches to print")
    parser.add_argument(
        "--kinds",
        default="normalize",
        help="comma-separated primitives: normalize, callsign, location, network",
    )
    parser.add_argument(
        "--binary",
        default=os.path.join(REPO, "target", "debug", "examples", "normalize_io"),
        help="path to the normalize_io example binary",
    )
    args = parser.parse_args()

    if not os.path.exists(args.binary):
        print(f"building {args.binary} ...", file=sys.stderr)
        subprocess.run(
            ["cargo", "build", "-q", "-p", "crosstalk-matching", "--example", "normalize_io"],
            cwd=REPO,
            check=True,
        )

    names = corpus(args.limit)
    kinds = [kind.strip() for kind in args.kinds.split(",") if kind.strip()]

    status = 0
    for kind in kinds:
        status |= sweep(kind, names, args)
    return status


def sweep(kind: str, names: list[str], args: argparse.Namespace) -> int:
    """Run one primitive across the corpus and report mismatches."""
    matcher = load_python_matcher()
    if kind == "normalize":
        return sweep_normalize(names, matcher, args)
    if kind in ("callsign", "location", "network"):
        return sweep_per_name(kind, names, matcher, args)
    print(f"unknown kind {kind!r}", file=sys.stderr)
    return 2


def run_rust(cases: list[dict], binary: str) -> list[dict]:
    requests = "\n".join(json.dumps(case) for case in cases)
    completed = subprocess.run(
        [binary], input=requests, capture_output=True, text=True, check=True
    )
    answers = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    if len(answers) != len(cases):
        raise SystemExit(f"Rust produced {len(answers)} answers for {len(cases)} cases")
    return answers


def sweep_per_name(kind: str, names: list[str], matcher, args) -> int:
    cases = [{"kind": kind, "name": name} for name in names]
    answers = run_rust(cases, args.binary)

    mismatches = []
    for name, answer in zip(names, answers):
        if kind == "callsign":
            # Compare the confidence flag too: eight of the divergences found in review
            # differed only in the flag, so comparing the callsign alone would hide them.
            expected_call, expected_high = matcher._compute_callsign_with_confidence(name)
            expected = {"callsign": expected_call, "high_confidence": expected_high}
            got = {
                "callsign": answer.get("callsign"),
                "high_confidence": answer.get("high_confidence"),
            }
        elif kind == "network":
            expected = python_network(name)
            got = answer.get("network")
        else:
            expected = python_location(name)
            got = {"state": answer.get("state"), "city": answer.get("city")}
        if got != expected:
            mismatches.append((name, expected, got, None))

    print(f"[{kind}] {len(mismatches)} mismatches out of {len(cases)} cases")
    for name, expected, got, _ in mismatches[: args.show]:
        print(f"  name={name!r}\n    python={expected!r}\n    rust  ={got!r}")
    if len(mismatches) > args.show:
        print(f"  ... and {len(mismatches) - args.show} more")
    return 1 if mismatches else 0


def python_network(name: str):
    """The network keyword probe from `Plugin._find_best_epg_match`."""
    import re

    match = re.search(r"\b(ABC|NBC|CBS|FOX|PBS|CW|ION|MNT|IND)\b", name, re.IGNORECASE)
    return match.group(1).upper() if match else None


def python_location(name: str) -> dict:
    """`Plugin._extract_location`, which lives in plugin.py and needs no Django."""
    import re

    if not name:
        return {"state": None, "city": None}
    match = re.search(r"-\s*([A-Z]{2})\s+([A-Za-z\s]+?)(?:\s*\(|$)", name)
    if match:
        return {"state": match.group(1), "city": match.group(2).strip()}
    match = re.search(r"(?:\([A-Z]{4}\)\s*)?([A-Z]{2})\s+([A-Za-z\s]+)", name)
    if match:
        city = re.sub(r"\s*\(.*$", "", match.group(2)).strip()
        if city and len(city) > 2:
            return {"state": match.group(1), "city": city}
    match = re.search(r"\b([A-Z]{2})\b", name)
    if match:
        states = {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
            "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
            "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
            "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
        }
        if match.group(1) in states:
            return {"state": match.group(1), "city": None}
    return {"state": None, "city": None}


def sweep_normalize(names: list[str], matcher, args) -> int:
    flag_combos = list(itertools.product([True, False], repeat=4))
    tag_sets = TAG_SETS if args.tags else [[]]

    cases = []
    for quality, regional, geographic, misc in flag_combos:
        for tags in tag_sets:
            for name in names:
                cases.append((name, (quality, regional, geographic, misc), tags))
    print(f"[normalize] {len(names)} names x {len(flag_combos)} flag combos x "
          f"{len(tag_sets)} tag sets = {len(cases)} cases", file=sys.stderr)

    answers = run_rust(
        [
            {
                "name": name,
                "ignore_quality": flags[0],
                "ignore_regional": flags[1],
                "ignore_geographic": flags[2],
                "ignore_misc": flags[3],
                "tags": tags,
            }
            for name, flags, tags in cases
        ],
        args.binary,
    )

    mismatches = []
    for (name, (quality, regional, geographic, misc), tags), answer in zip(cases, answers):
        expected = matcher.normalize_name(
            name,
            user_ignored_tags=set(tags),
            ignore_quality=quality,
            ignore_regional=regional,
            ignore_geographic=geographic,
            ignore_misc=misc,
        )
        got = answer.get("normalized")
        if got != expected:
            mismatches.append((name, (quality, regional, geographic, misc), tags, expected, got))

    print(f"{len(mismatches)} mismatches out of {len(cases)} cases")
    for name, flags, tags, expected, got in mismatches[: args.show]:
        print(f"  name={name!r} flags={flags} tags={tags}\n    python={expected!r}\n    rust  ={got!r}")
    if len(mismatches) > args.show:
        print(f"  ... and {len(mismatches) - args.show} more")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
