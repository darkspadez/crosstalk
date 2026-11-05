#!/usr/bin/env python3
"""Build the labelled phase-3 matching corpus (plan step 5).

Reads the merged data set (``data/merged/``), the plugins' own tests and golden
baselines, and the parity-script fixture worlds; generates provider-style
variants of canonical names; runs every plugin's own decision path
(``scripts/corpus/oracles.py``) over every pair; and labels each pair:

* (a) construction truth, no voting:
  - a synthetic provider spelling of canonical X is ``same`` as X when it
    keeps the feed and ``different`` when it does not: `X West`, `X Pacific`,
    `X (W)` and `X +1` are other channels (feed key ``BASE_FEED``); every form
    is one real names carry (``FORMS``, counted in ``STATS.json``), drawn
    uniformly so no plugin's table is favoured;
  - near-miss canonicals (curated, plus same-first-token pairs that are
    certainly distinct, ``distinct_channels``), real channels some
    normalizer folds together (``_collisions``), other stations of the same
    network or market, and an alias of a near miss are ``different``;
  - two spellings of one OTA station, and an undisputed alias of the merged
    table in either direction, are ``same``;
* (b) real pairs no generator labels: all four oracles agree -> that label
  (``unanimous``);
* (c) disputed real pairs -> majority of oracles (ties -> ``unknown``,
  excluded from scoring). The four are not independent (three share the
  core), so ``decide.py`` never ablates on these labels;
* (d) ``--ai`` with keys in the environment -> the ``[ai.verify]`` chain via
  ``crates/crosstalk-core/examples/label_pair.rs`` (wrapping
  ``AiService::decide_yes_no``) relabels disputed pairs it settles. Without
  keys the run keeps majority labels and records that the AI path was
  skipped; the example's own tests pin it with a fake provider (no network).

Every pair records its generator and the per-oracle verdicts. Construction
truth and majority-vote labels are kept separate in ``STATS.json`` so a reader
can see how much rests on voting.

Outputs (deterministic: seeded RNG, sorted, no timestamps)::

    data/corpus/pairs.jsonl   query, candidate, label, label_source, generator,
                              per-oracle verdicts, rule attribution
    data/corpus/STATS.json    size by source and label source, oracle agreement,
                              runtimes, seed, provenance (cited files)

Usage:
    python3 -I scripts/corpus/build.py [--out DIR] [--pairs N] [--seed N]
    python3 -I scripts/corpus/build.py --check [--out DIR] [--pairs N] [--seed N]
    python3 -I scripts/corpus/build.py --subset [--out DIR]
    python3 -I scripts/corpus/build.py --ai [...]

    --out DIR    corpus directory (default: data/corpus)
    --pairs N    target pair count (default: 30000)
    --seed N     RNG seed (default: 20261105)
    --subset     deterministic CI subset: --pairs 2000, fixed seed 7
    --check      rebuild into a temp dir and fail if --out differs
    --ai         relabel disputed pairs through the AI chain when keys for
                 [ai.verify] are present in the environment (CROSSTALK_AI__...
                 overrides of crosstalk.toml); without keys the run keeps
                 majority labels and records that the AI path was skipped.

Run with ``python3 -I`` (no pip installs): the only third-party import is the
sibling ``oracles`` module, loaded from this directory.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import random
import re
import subprocess
import sys
import tempfile
import time
import unicodedata

sys.dont_write_bytecode = True

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import oracles  # noqa: E402
from disagreements import _rule_hints  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MERGED = os.path.join(REPO, "data", "merged")

DEFAULT_SEED = 20261105
DEFAULT_PAIRS = 30000
SUBSET_PAIRS = 2000
SUBSET_SEED = 7

TAGS = ("janitor", "sm", "cm", "lu")

#: Test files mined for real names (string literals that look like channel
#: names). Cited in STATS.json provenance so every name traces to its file.
TEST_FILES = [
    "reference/Dispatcharr-EPG-Janitor-Plugin/tests/test_fuzzy_matcher.py",
    "reference/Dispatcharr-EPG-Janitor-Plugin/tests/test_station_callsigns.py",
    "reference/Dispatcharr-EPG-Janitor-Plugin/tests/test_regressions.py",
    "reference/Dispatcharr-EPG-Janitor-Plugin/tests/test_matcher_golden.py",
    "reference/Stream-Mapparr/tests/test_designator_tokens.py",
    "reference/Stream-Mapparr/tests/test_fuzzy_matcher.py",
    "reference/Stream-Mapparr/tests/test_ota_callsign_fallback.py",
    "reference/Stream-Mapparr/tests/test_alias_key_case.py",
    "reference/Stream-Mapparr/tests/test_country_detection.py",
    "reference/Stream-Mapparr/tests/test_zone_single_timezone.py",
    "reference/Dispatcharr-Channel-Maparr-Plugin/tests/test_broadcast_ota.py",
    "reference/Dispatcharr-Channel-Maparr-Plugin/tests/test_fuzzy_matcher.py",
    "reference/Dispatcharr-Lineuparr-Plugin/tests/test_country_filter.py",
    "reference/Dispatcharr-Lineuparr-Plugin/tests/test_comedy_tv_disambiguation.py",
    "reference/Dispatcharr-Lineuparr-Plugin/tests/test_eastern_region_matching.py",
    "reference/Dispatcharr-Lineuparr-Plugin/tests/test_sportsnet_pacific.py",
    "reference/Dispatcharr-Lineuparr-Plugin/tests/test_dotted_name_normalization.py",
    "reference/Dispatcharr-Lineuparr-Plugin/tests/test_alias_callsign_guard.py",
]

#: Golden baselines (JSON fixtures the plugins' own suites assert against).
GOLDEN_FILES = [
    "reference/Dispatcharr-EPG-Janitor-Plugin/tests/matcher_golden_baseline.json",
    "reference/Dispatcharr-Channel-Maparr-Plugin/tests/matcher_golden_baseline.json",
]

#: Parity-script fixture worlds mined for names where readily extractable.
PARITY_FILES = [
    "scripts/match_parity.py",
    "scripts/sort_parity.py",
]

#: Near-miss canonical pairs: same family, different channel. Variants of one
#: side paired with the other are construction-truth ``different`` (and the
#: bare pair too). Real provider confusions the unified engine must not make.
#: (`ABC News` vs `ABC News Live` is deliberately NOT here: the shared core
#: documents it as one channel, `matching_core.py:63-66`, and labelling it
#: different would decide row T01 by fiat.)
NEAR_MISS_BASE = [
    ("BBC One", "BBC Two"),
    ("BBC One", "BBC News"),
    ("ESPN", "ESPN 2"),
    ("ESPN", "ESPN News"),
    ("ABC News", "BBC News"),
    ("Sky Sports F1", "Sky Sports News"),
    # SM issue #50 (`tests/test_designator_tokens.py:92-95`), a real report.
    ("Sky Sports F1", "Sky Sports 1"),
    ("Sky Sports F1 UHD", "Sky Sports UHD 1"),
    ("HBO", "HBO Family"),
    ("HBO", "HBO Comedy"),
    ("Cinemax", "MoreMax"),
    ("Cinemax", "ActionMax"),
    ("Fox News", "Fox Sports"),
    ("Sky Sports 1", "Sky Sports 2"),
    ("ITV 1", "ITV 2"),
    ("Nickelodeon", "Nick Jr"),
    ("CNN", "HLN"),
    ("BBC Four", "BBC Alba"),
    ("Comedy Central", "Comedy TV"),
    ("Justice Network", "Justice Central"),
]


# ---------------------------------------------------------------------------
# name collection
# ---------------------------------------------------------------------------

def _looks_like_name(text: str) -> bool:
    """Heuristic for string literals that are channel names, not messages."""
    if not (2 <= len(text) <= 60):
        return False
    if not re.search(r"[A-Za-z]", text):
        return False
    if "/" in text or "\\" in text or "\n" in text:
        return False
    if text.startswith(("http", "test_", "Test ", "assert", "mock", "Mock")):
        return False
    if re.search(r"\.(py|json|toml|yaml|js|css|html)$", text):
        return False
    # Sentences, identifiers and format strings are messages, not names:
    # `The setting matrix every world is swept over.`, `core.utils`, `US:`.
    if text.endswith((".", ":", ";", ",")) or len(text.split()) > 7:
        return False
    if re.fullmatch(r"[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)+", text):
        return False
    if re.search(r"[{}%]|\b(got|want|should|must|expected)\b", text):
        return False
    # Format fixtures (`1080p at 30`, `1280X720`) are not channels.
    if re.match(r"^\d{3,4}[pPiI]\b|^\d+\s*[xX]\s*\d+$", text):
        return False
    # A name has a space, a digit, or channel-ish punctuation; or is a short
    # all-caps token (callsign) or a CamelCase word.
    return bool(
        re.search(r"[ 0-9+&|()\[\]:.!★▶©™+]", text)
        or (text.isupper() and len(text) <= 8)
        or re.search(r"[a-z][A-Z]", text)
    )


def _strings_from_python(path: str, cap: int) -> list[str]:
    """String literals from a Python file that look like channel names."""
    try:
        with open(path, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
    except (OSError, SyntaxError):
        return []
    found: list[str] = []
    seen: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            text = node.value.strip()
            if text not in seen and _looks_like_name(text):
                seen.add(text)
                found.append(text)
                if len(found) >= cap:
                    break
    return sorted(found)


def _strings_from_json(path: str, cap: int) -> list[str]:
    """String values from a JSON golden baseline that look like names."""
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return []
    found: set[str] = set()

    def walk(value) -> None:
        if isinstance(value, str):
            text = value.strip()
            if _looks_like_name(text):
                found.add(text)
        elif isinstance(value, list):
            for item in value:
                walk(item)
        elif isinstance(value, dict):
            for item in value.values():
                walk(item)

    walk(data)
    return sorted(found)[:cap]


def _merged_channel_names() -> list[str]:
    """Every verbatim channel name in the merged per-country DBs, sorted."""
    names: set[str] = set()
    channels_dir = os.path.join(MERGED, "channels")
    for entry in sorted(os.listdir(channels_dir)):
        if not entry.endswith(".json"):
            continue
        with open(os.path.join(channels_dir, entry), encoding="utf-8") as handle:
            data = json.load(handle)
        for row in data["channels"]:
            names.add(row["channel_name"])
    return sorted(names)


def _lineup_names() -> list[str]:
    """Every channel name in the merged lineup files, sorted."""
    names: set[str] = set()
    lineups_dir = os.path.join(MERGED, "lineups")
    for entry in sorted(os.listdir(lineups_dir)):
        if not entry.endswith(".json"):
            continue
        with open(os.path.join(lineups_dir, entry), encoding="utf-8") as handle:
            data = json.load(handle)
        categories = data.get("categories", {})
        if isinstance(categories, dict):
            for channels in categories.values():
                for channel in channels:
                    if isinstance(channel, dict) and channel.get("name"):
                        names.add(str(channel["name"]))
        elif isinstance(categories, list):
            for channel in categories:
                if isinstance(channel, dict) and channel.get("name"):
                    names.add(str(channel["name"]))
    return sorted(names)


def _alias_data() -> dict:
    with open(os.path.join(MERGED, "aliases.json"), encoding="utf-8") as handle:
        return json.load(handle)


def _alias_names() -> tuple[list[str], list[str]]:
    """(canonical names, alias variant spellings) from the merged alias table."""
    data = _alias_data()
    canonicals = sorted(data["aliases"])
    variants: set[str] = set()
    for canonical, entries in data["aliases"].items():
        for entry in entries:
            variants.add(entry["alias"])
    for country, table in data.get("country_aliases", {}).items():
        for canonical, entries in table.items():
            if canonical not in canonicals:
                canonicals.append(canonical)
            for entry in entries:
                variants.add(entry["alias"])
    return sorted(set(canonicals)), sorted(variants)


def _disputed_aliases() -> set[str]:
    """Aliases two canonicals claim (``alias_conflicts.json`` ``disputed``)."""
    with open(os.path.join(MERGED, "alias_conflicts.json"), encoding="utf-8") as handle:
        conflicts = json.load(handle)
    return {entry["alias"] for entry in conflicts["conflicts"]
            if entry["status"] == "disputed"}


def _stride_sample(names: list[str], want: int) -> list[str]:
    """Deterministic even sample: every k-th name of the sorted list."""
    if len(names) <= want:
        return list(names)
    step = len(names) / want
    return [names[int(index * step)] for index in range(want)]


def _evidence_names() -> list[str]:
    """Real provider-side spellings: every merged name plus every name-like
    string literal in all four plugins' test suites (real stream names from
    issues and fixtures). Used ONLY to count how often each synthetic form
    occurs for real; never as canonicals."""
    names = set(_merged_channel_names()) | set(_lineup_names())
    names |= set(_alias_names()[1])
    tests_root = os.path.join(REPO, "reference")
    for plugin in sorted(os.listdir(tests_root)):
        tests = os.path.join(tests_root, plugin, "tests")
        if not os.path.isdir(tests):
            continue
        for entry in sorted(os.listdir(tests)):
            if entry.endswith(".py"):
                names.update(_strings_from_python(os.path.join(tests, entry), 10 ** 6))
    return sorted(names)


# ---------------------------------------------------------------------------
# synthetic provider variants
# ---------------------------------------------------------------------------

#: Feed key of a synthetic variant relative to its canonical: (zone, shift).
#: East and unmarked are one feed (the US convention SM's `extract_zone`
#: docstring and LU's region filter both encode: "regionless defaults to
#: East"); West, Pacific and their abbreviations are the other feed (SM folds
#: Pacific into WEST). A bare `(P)` is left out: LU reads it as Pacific
#: (`test_sportsnet_pacific.py`) but SM calls it too ambiguous to be a zone
#: (`fuzzy_matcher.py:71-73`; `RTPi (P)` is Portugal), so it decides nothing.
#: A `+1` time-shift is a different channel (its own guide; JAN and LU reject
#: shift vs non-shift, `fuzzy_matcher.py` `_PLUS_SHIFT_RE`). Two names are the
#: same channel iff they share the canonical AND the feed key.
BASE_FEED = ("default", False)

#: Provider forms per family: (form id, template, feed zone or None, evidence
#: regex). A form joins the generator only when its evidence regex matches at
#: least one real name (``_evidence_names``); every kept form of a family is
#: then equally likely. So each plugin's distinctive vocabulary item (HEVC/HDR
#: in the core, the `HD:` colon form in Janitor, glued `UKHD:` in the core,
#: bare `UK ` prefixes, ...) is tested exactly as often as the shared ones,
#: and only if providers really write it: no generator mirrors one plugin's
#: table. Counts land in STATS.json (`generator_forms`).
FORMS: dict[str, list[tuple[str, str, str | None, str]]] = {
    "quality_suffix": [
        ("HD", "{} HD", None, r"\sHD$|\sHD\s"),
        ("SD", "{} SD", None, r"\sSD$|\sSD\s"),
        ("FHD", "{} FHD", None, r"\sFHD\b(?![\]\)])"),
        ("UHD", "{} UHD", None, r"\sUHD\b(?![\]\)])"),
        ("4K", "{} 4K", None, r"\s4K\b(?![\]\)])"),
        ("8K", "{} 8K", None, r"\s8K\b(?![\]\)])"),
        ("HEVC", "{} HEVC", None, r"\bHEVC\b"),
        ("HDR", "{} HDR", None, r"\bHDR\b"),
        ("[HD]", "{} [HD]", None, r"\[HD\]"),
        ("[FHD]", "{} [FHD]", None, r"\[FHD\]"),
        ("(HD)", "{} (HD)", None, r"\(HD\)"),
        ("(UHD)", "{} (UHD)", None, r"\(UHD\)"),
        ("1080p", "{} 1080p", None, r"\b\d{3,4}[pi]\b"),
        ("HD:", "HD: {}", None, r"\b(?:HD|FHD|UHD|4K):\s"),
    ],
    "region": [
        ("East", "{} East", "default", r"\sEast$"),
        ("West", "{} West", "west", r"\sWest$"),
        ("(E)", "{} (E)", "default", r"\(E\)"),
        ("(W)", "{} (W)", "west", r"\(W\)"),
        ("Pacific", "{} Pacific", "west", r"\sPacific$"),
        ("(East)", "{} (East)", "default", r"\(East\)"),
        ("(West)", "{} (West)", "west", r"\(West\)"),
        ("(Pacific)", "{} (Pacific)", "west", r"\(Pacific\)"),
        ("Eastern", "{} Eastern", "default", r"\sEastern$"),
    ],
    "plus_one": [
        (" +1", "{} +1", "shift", r"\s\+1\b"),
        ("+1", "{}+1", "shift", r"\w\+1\b"),
    ],
    "country_prefix": [
        ("US:", "US: {}", None, r"^US:\s"),
        ("UK:", "UK: {}", None, r"^UK:\s"),
        ("CA:", "CA: {}", None, r"^CA:\s"),
        ("US |", "US | {}", None, r"^US \| "),
        ("UK -", "UK - {}", None, r"^UK - "),
        ("(US)", "(US) {}", None, r"^\(US\) "),
        ("[UK]", "[UK] {}", None, r"^\[UK\] "),
        ("US bare", "US {}", None, r"^US [A-Z]"),
        ("UK bare", "UK {}", None, r"^UK [A-Z]"),
        ("CA bare", "CA {}", None, r"^CA [A-Z]"),
        ("UKHD:", "UKHD: {}", None, r"^(?:US|UK)(?:SD|HD|FHD|UHD):?\s"),
        ("CA FR:", "CA FR: {}", None, r"^[A-Z]{2} [A-Z]{2}:\s"),
        ("| US", "{} | US", None, r"\|\s*US$"),
    ],
    "platform_prefix": [
        ("RK:", "RK: {}", None, r"^RK:\s"),
        ("GO:", "GO: {}", None, r"^GO:\s"),
        ("TUBI:", "TUBI: {}", None, r"^TUBI:\s"),
        ("PLUTO:", "PLUTO: {}", None, r"^PLUTO:\s"),
        ("PRIME:", "PRIME: {}", None, r"^PRIME:\s"),
    ],
    "decorative_unicode": [
        ("◉", "{} ◉", None, r"◉"),
        ("★", "★ {} ★", None, r"★"),
        ("ᴴᴰ", "{} ᴴᴰ", None, r"ᴴᴰ"),
        ("²", "{} ²", None, r"²"),
        ("▶", "▶ {}", None, r"▶"),
    ],
    "zero_width": [
        ("ZWSP", "{}\u200b", None, "[\u200b\u200c\u200d\u2060\ufeff]"),
    ],
}


def form_evidence(names: list[str]) -> dict[str, dict[str, int]]:
    """Real-name count per form (the inclusion test for ``FORMS``)."""
    counts: dict[str, dict[str, int]] = {}
    for family, forms in FORMS.items():
        counts[family] = {}
        for form_id, _template, _zone, pattern in forms:
            regex = re.compile(pattern)
            counts[family][form_id] = sum(1 for name in names if regex.search(name))
    return counts


_STATION_RE = re.compile(r"\([KW][A-Z]{2,3}(-[A-Z0-9]+)?\)|^[KW][A-Z]{2,3}(-[A-Z0-9]+)?\b")
_MARKER_RE = re.compile(
    r"\b(east|eastern|west|pacific)\b|\(\s*(e|w|p|pt)\s*\)|\+\s?\d\b", re.IGNORECASE)


def _number_word_variants(name: str) -> list[str]:
    """Swap number words <-> digits where the name has one."""
    swaps = [
        ("One", "1"), ("Two", "2"), ("Three", "3"), ("Four", "4"),
        ("Five", "5"), ("Six", "6"), ("Seven", "7"), ("Eight", "8"),
    ]
    out = []
    for word, digit in swaps:
        if re.search(rf"\b{word}\b", name):
            out.append(re.sub(rf"\b{word}\b", digit, name))
        if re.search(rf"\b{digit}\b", name):
            out.append(re.sub(rf"\b{digit}\b", word, name))
    return out


def _variants_of(canonical: str, rng: random.Random,
                 forms: dict[str, list[tuple]]) -> list[tuple[str, str, tuple]]:
    """Provider-style spellings of one canonical name.

    Returns ``[(variant, transform, feed)]`` with ``feed`` the (zone, shift)
    key relative to the canonical (``BASE_FEED`` = the same channel). One form
    per applicable family, drawn uniformly from the evidenced forms.
    Zone and shift markers are only added to canonicals that carry none (and
    are not broadcast stations), so no variant stacks `(W) (East)`.
    """
    out: list[tuple[str, str, tuple]] = []
    # US broadcast stations carry no East/West feeds and no +1 channels, so
    # zone and shift forms are never added to a callsign-bearing name.
    marked = bool(_MARKER_RE.search(canonical) or _STATION_RE.search(canonical))

    def add(variant: str, transform: str, feed: tuple = BASE_FEED) -> None:
        if variant and variant != canonical:
            out.append((variant, transform, feed))

    for family, family_forms in forms.items():
        if not family_forms:
            continue
        if family in ("region", "plus_one") and marked:
            continue
        form_id, template, zone, _pattern = rng.choice(family_forms)
        feed = BASE_FEED
        if zone == "west":
            feed = ("west", False)
        elif zone == "shift":
            feed = ("default", True)
        add(template.format(canonical), f"{family}={form_id}", feed)
    if "&" in canonical:
        add(canonical.replace("&", "and"), "amp_and")
    elif re.search(r"\band\b", canonical, re.IGNORECASE):
        add(re.sub(r"\band\b", "&", canonical, flags=re.IGNORECASE), "amp_and")
    if " " in canonical:
        add(canonical.replace(" ", ""), "camel_glue")
    for swapped in _number_word_variants(canonical)[:1]:
        add(swapped, "number_word")
    if re.search(r"\sTV$", canonical):
        add(re.sub(r"\s*TV$", ".TV", canonical), "dot_glue")
    suffix = re.search(r"\s(Channel|TV|Network)$", canonical)
    if suffix and len(canonical[:suffix.start()].split()) >= 1:
        add(canonical[:suffix.start()], "suffix_drop")
    add(canonical.upper() if rng.random() < 0.5 else canonical.lower(), "case_fold")
    # Shuffle so the [:4] cut in build_pairs does not starve later transforms.
    rng.shuffle(out)
    return out


# ---------------------------------------------------------------------------
# near-miss filtering
# ---------------------------------------------------------------------------

#: Tokens that never tell two channels apart: quality, country/platform tags,
#: generic suffixes, East (the default feed). Used ONLY to drop candidate
#: near-miss pairs that might be one channel; never to label a pair `same`.
_JUNK_TOKENS = frozenset("""
hd sd fhd uhd 4k 8k hdr hevc fd raw 1080p 1080i 720p 2160p 480p backup
tv channel network the east eastern live
us usa uk gb ca au fr de es it nl br mx mex in ie nz za pt pl do pe qa tz
fra ger deu esp ita gbr can aus bra arg col chl
rk go tubi pluto xumo plex stirr freevee glance prime
""".split())
_NUMBER_WORDS = {"one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
                 "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10"}


def _fold(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch)).lower()


def _core_tokens(name: str) -> tuple[frozenset, str]:
    """Distinguishing tokens outside brackets, and their glued form (in order,
    trailing glued junk such as `TV`/`HD` removed: `BabyFirstTV` = `BabyFirst`)."""
    outside = re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", name)
    folded = _fold(outside).replace("&", " and ")
    tokens = [_NUMBER_WORDS.get(tok, tok)
              for tok in re.findall(r"[a-z0-9]+", folded)]
    kept = [tok for tok in tokens if tok not in _JUNK_TOKENS]
    glued = "".join(kept)
    while True:
        trimmed = re.sub(r"(tv|hd|channel|network)$", "", glued)
        if trimmed == glued or not trimmed:
            break
        glued = trimmed
    return frozenset(kept), glued


def _zone_of(name: str) -> str:
    """Feed zone a real name declares (trailing/parenthesized markers only)."""
    if re.search(r"\(\s*(w|west|pt|pacific)\s*\)"
                 r"|\b(west|pacific)\b(\s+(hd|sd|fhd|uhd|4k|raw))*\s*$",
                 name, re.IGNORECASE):
        return "west"
    return "default"


def _abbreviates(short: str, long: str) -> bool:
    """`jr`/`junior`, `intl`/`international`, `mystery`/`mysteries`: one token
    standing for another. Digit-bearing tokens only ever equal themselves."""
    if short == long:
        return True
    if any(ch.isdigit() for ch in short + long):
        return False
    if len(short) >= 5 and len(long) >= 5 and short[:5] == long[:5]:
        return True
    if short[0] != long[0] or len(short) >= len(long):
        return False
    letters = iter(long)
    return all(ch in letters for ch in short)


def _unexplained(tokens: frozenset, other: frozenset) -> set:
    return {tok for tok in tokens
            if not any(_abbreviates(tok, o) or _abbreviates(o, tok) for o in other)}


def distinct_channels(left: str, right: str) -> bool:
    """True when two real names are certainly different channels.

    Conservative by design (a pair that fails is dropped, never labelled):
    the names declare different feeds; or EACH side carries a distinguishing
    token the other lacks (case, accents, number words, `&`/`and`, glued
    spacing, abbreviations and junk tokens folded away); or one side only adds
    a NUMBER (`ESPN` vs `ESPN 2`). One side only adding words (`E!` vs `E!
    Entertainment Television`, `HBO` vs `HBO Family`) or only brackets (`CNBC`
    vs `CNBC (DETROIT)`, `ABC` vs `ABC (WABC)`) is ambiguous and dropped; the
    curated ``NEAR_MISS_BASE`` covers the subset cases that matter."""
    left_tokens, left_glued = _core_tokens(left)
    right_tokens, right_glued = _core_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    if _zone_of(left) != _zone_of(right):
        return True
    if left_tokens == right_tokens or left_glued == right_glued:
        return False
    joined = ["".join(_NUMBER_WORDS.get(tok, tok)
                      for tok in re.findall(r"[a-z0-9]+", _fold(side)))
              for side in (left, right)]
    if joined[0] == joined[1]:
        return False
    left_extra = _unexplained(left_tokens, right_tokens)
    right_extra = _unexplained(right_tokens, left_tokens)
    if left_extra and right_extra:
        return True
    return any(tok.isdigit() for tok in left_extra | right_extra)


# ---------------------------------------------------------------------------
# station (OTA) names
# ---------------------------------------------------------------------------

def _station_forms(station: dict) -> list[tuple[str, str]]:
    """Real OTA spellings of one station: `ABC 7 (KGO)` (JAN `plugin.py`
    comment, SM tests), `KING 5` (SM `test_ota_callsign_fallback.py:139`),
    `KGO-TV`, the bare callsign, CM's rename template `{NETWORK} - {STATE}
    {CITY} ({CALLSIGN})` and `US: ABC 25 (WPBF) West Palm Beach` (lineup
    style)."""
    callsign = station["callsign"]
    network = station["network_affiliation"].split(",")[0].strip()
    number = station["tv_virtual_channel"].split(".")[0]
    city = station["community_served_city"].title()
    state = station["community_served_state"]
    base = callsign.split("-")[0]
    forms = [
        (f"{network} {number} ({base})", "ota_net_num_call"),
        (f"{base} {number}", "ota_call_num"),
        (base, "ota_call"),
        (f"{network} - {state} {city} ({base})", "ota_cm_template"),
        (f"US: {network} {number} ({base}) {city}", "ota_lineup"),
    ]
    if "-" not in callsign and len(base) == 4:
        forms.append((f"{base}-TV", "ota_call_tv"))
    return forms


def _denylisted_station_pairs(rng: random.Random,
                              names: list[str]) -> list[tuple[str, str, str, str]]:
    """Real stations whose callsign is a common word the core denylists
    (`KING`, `WAVE`, `WOOD`, `WHO`, ... `matching_core.py:740-750`): two
    spellings of the station are `same`; a real channel name using the word as
    a word (`King of the Hill`-style DB names) against the station is
    `different`. Both sides of the rescue question, from real data only."""
    core = sys.modules.get("oracle_lu.matching_core")
    if core is None:
        oracles.get("lu")
        core = sys.modules["oracle_lu.matching_core"]
    denylist = core.FuzzyMatcherCore._CALLSIGN_DENYLIST
    with open(os.path.join(MERGED, "stations.json"), encoding="utf-8") as handle:
        stations = json.load(handle)["stations"]
    out = []
    for key, row in sorted(stations.items()):
        base = row["callsign"].split("-")[0]
        if base not in denylist or not row.get("network_affiliation") \
                or not row.get("tv_virtual_channel"):
            continue
        forms = _station_forms(row)
        rng.shuffle(forms)
        for (left, lgen), (right, rgen) in zip(forms, forms[1:]):
            out.append((left, right, "same", f"station:denylisted:{lgen}+{rgen}"))
        word = re.compile(rf"\b{base}\b", re.IGNORECASE)
        # The word as a word, never as an upper-case callsign token (`KING
        # (NBC)`, `KING-DT2` and `Nat Geo WILD` are left out: ambiguous).
        users = [name for name in names
                 if word.search(name) and not re.search(rf"\b{base}\b", name)]
        for name in users[:3]:
            right, rgen = rng.choice(forms)
            out.append((name, right, "different", f"station:word:{rgen}"))
    return out


def _station_pairs(rng: random.Random, limit: int) -> list[tuple[str, str, str, str]]:
    """Construction pairs over US full-power stations with a network and a
    virtual channel: two spellings of one station are `same`; the same network
    and number in another market, or another station of the same market, is
    `different` (the confusions the callsign ladder exists to prevent)."""
    with open(os.path.join(MERGED, "stations.json"), encoding="utf-8") as handle:
        stations = json.load(handle)["stations"]
    usable = [row for _key, row in sorted(stations.items())
              if row.get("network_affiliation") and row.get("tv_virtual_channel")
              and re.fullmatch(r"[KW][A-Z]{2,3}(-(TV|DT))?", row["callsign"])
              and row["network_affiliation"].split(",")[0].strip()
              not in ("Independent", "")]
    picked = _stride_sample(usable, limit)
    by_net_num: dict[tuple, list[dict]] = {}
    by_market: dict[tuple, list[dict]] = {}
    for row in usable:
        network = row["network_affiliation"].split(",")[0].strip()
        by_net_num.setdefault((network, row["tv_virtual_channel"].split(".")[0]),
                              []).append(row)
        by_market.setdefault((row["community_served_state"],
                              row["community_served_city"]), []).append(row)
    out = []
    for row in picked:
        forms = _station_forms(row)
        rng.shuffle(forms)
        for (left, lgen), (right, rgen) in zip(forms, forms[1:3]):
            out.append((left, right, "same", f"station:same:{lgen}+{rgen}"))
        network = row["network_affiliation"].split(",")[0].strip()
        number = row["tv_virtual_channel"].split(".")[0]
        others = [other for other in by_net_num[(network, number)]
                  if other["callsign"].split("-")[0] != row["callsign"].split("-")[0]]
        others += [other for other in by_market[(row["community_served_state"],
                                                 row["community_served_city"])]
                   if other["callsign"].split("-")[0] != row["callsign"].split("-")[0]]
        for other in others[:2]:
            left, lgen = rng.choice(forms)
            right, rgen = rng.choice(_station_forms(other))
            out.append((left, right, "different", f"station:different:{lgen}+{rgen}"))
    return out


# ---------------------------------------------------------------------------
# corpus assembly
# ---------------------------------------------------------------------------

def collect_names() -> tuple[dict[str, list[str]], list[str]]:
    """Collect canonical-name pools per source. Returns (pools, provenance)."""
    pools: dict[str, list[str]] = {}
    provenance: list[str] = []
    channels = _merged_channel_names()
    pools["db"] = _stride_sample(channels, 1500)
    provenance.append(f"data/merged/channels/*.json ({len(channels)} names)")
    lineups = _lineup_names()
    pools["lineup"] = _stride_sample(lineups, 600)
    provenance.append(f"data/merged/lineups/*.json ({len(lineups)} names)")
    canonicals, variants = _alias_names()
    pools["alias_canonical"] = canonicals
    pools["alias_variant"] = _stride_sample(variants, 400)
    provenance.append(f"data/merged/aliases.json ({len(canonicals)} canonicals)")
    provenance.append("data/merged/stations.json (OTA station forms)")
    test_names: set[str] = set()
    for path in TEST_FILES:
        full = os.path.join(REPO, path)
        for name in _strings_from_python(full, 200):
            test_names.add(name)
        provenance.append(path)
    for path in GOLDEN_FILES:
        full = os.path.join(REPO, path)
        for name in _strings_from_json(full, 400):
            test_names.add(name)
        provenance.append(path)
    pools["tests"] = sorted(test_names)
    fixture_names: set[str] = set()
    for path in PARITY_FILES:
        full = os.path.join(REPO, path)
        for name in _strings_from_python(full, 300):
            fixture_names.add(name)
        provenance.append(path)
    pools["parity"] = sorted(fixture_names)
    provenance.append("reference/*/tests/*.py (form evidence only)")
    return pools, provenance


def _near_miss_auto(canonicals: list[str], rng: random.Random,
                    cap: int) -> list[tuple[str, str]]:
    """Canonical pairs sharing a first token that are certainly distinct
    channels (``distinct_channels``): sibling feeds, same family."""
    by_first: dict[str, list[str]] = {}
    for name in canonicals:
        first = name.split()[0].upper() if name.split() else name.upper()
        by_first.setdefault(first, []).append(name)
    pairs: set[tuple[str, str]] = set()
    for group in by_first.values():
        if 2 <= len(group) <= 16:
            for left in group:
                for right in group:
                    if left < right and distinct_channels(left, right):
                        pairs.add((left, right))
    ordered = sorted(pairs)
    rng.shuffle(ordered)
    return ordered[:cap]


def _collisions(names: list[str], rng: random.Random,
                cap: int) -> list[tuple[str, str, str]]:
    """Distinct real channels that some plugin's normalizer folds together.

    Every oracle normalizes every name; pairs sharing a normalized form under
    ANY oracle and certainly distinct (``distinct_channels``) are construction
    `different`. Taking the union over all four normalizers keeps the
    generator neutral: each plugin's over-stripping is tested, none is
    favoured. Returns ``(left, right, colliding oracle tags)``."""
    groups: dict[tuple[str, str], list[str]] = {}
    for tag in TAGS:
        oracle = oracles.get(tag)
        for name in names:
            try:
                norm = oracle.normalize(name)
            except Exception:  # noqa: BLE001
                continue
            key = re.sub(r"[\s&\-]+", "", (norm or "").lower())
            if len(key) >= 2:
                groups.setdefault((tag, key), []).append(name)
    found: dict[tuple[str, str], set[str]] = {}
    for (tag, _key), group in sorted(groups.items()):
        if len(group) > 12:
            continue
        for left in group:
            for right in group:
                if left < right and distinct_channels(left, right):
                    found.setdefault((left, right), set()).add(tag)
    ordered = sorted(found)
    rng.shuffle(ordered)
    return [(left, right, "+".join(sorted(found[(left, right)])))
            for left, right in ordered[:cap]]


def _token_overlap_close(names: list[str], rng: random.Random,
                          cap: int) -> list[tuple[str, str]]:
    """Distinct raw names sharing a token: cheap real-pair candidates."""
    token_index: dict[str, list[str]] = {}
    for name in names:
        for token in set(re.findall(r"[A-Za-z0-9]+", name.upper())):
            if len(token) >= 3:
                token_index.setdefault(token, []).append(name)
    pairs: set[tuple[str, str]] = set()
    for group in token_index.values():
        if 2 <= len(group) <= 24:
            for left in group:
                for right in group:
                    if left < right and left.lower() != right.lower():
                        pairs.add((left, right))
    ordered = sorted(pairs)
    rng.shuffle(ordered)
    return ordered[:cap]


def build_pairs(seed: int, target: int) -> tuple[list[dict], dict]:
    """Assemble pair specs: (query, candidate, label|None, generator).

    ``label`` is set for construction truth; ``None`` means "ask the oracles".
    """
    rng = random.Random(seed)
    pools, provenance = collect_names()
    evidence = form_evidence(_evidence_names())
    forms = {family: [form for form in family_forms
                      if evidence[family][form[0]] > 0]
             for family, family_forms in FORMS.items()}
    specs: list[tuple[str, str, str | None, str]] = []
    seen: set[tuple[str, str]] = set()

    def emit(query: str, candidate: str, label, generator: str) -> None:
        if not query or not candidate or query == candidate:
            return
        key = (query, candidate)
        if key in seen:
            return
        seen.add(key)
        specs.append((query, candidate, label, generator))

    canonicals = sorted({n for key, pool in pools.items()
                         if key != "alias_variant" for n in pool})

    # (a1) construction: synthetic variants of each canonical, labelled by the
    # feed key (same canonical and same feed -> same; a West/Pacific or +1
    # variant against the unmarked canonical -> different).
    variant_of: dict[str, list[tuple[str, str, tuple]]] = {}
    for name in canonicals:
        picked = _variants_of(name, rng, forms)[:4]
        variant_of[name] = picked
        for variant, transform, feed in picked:
            emit(variant, name, "same" if feed == BASE_FEED else "different",
                 f"synth:{transform}")
        if len(picked) >= 2:
            (left, lt, lf), (right, rt, rf) = picked[0], picked[1]
            emit(left, right, "same" if lf == rf else "different",
                 f"synth:{lt}+{rt}")

    # (a2) construction different: near-miss canonicals, bare and via variants
    # that keep the feed (a West variant of a near miss is doubly different).
    near_miss = list(NEAR_MISS_BASE)
    near_miss += _near_miss_auto(
        pools["db"] + pools["lineup"] + pools["alias_canonical"], rng,
        max(400, int(target * 0.15)))
    for left, right in near_miss:
        emit(left, right, "different", "near-miss:bare")
        for variant, _transform, _feed in variant_of.get(left, [])[:1]:
            emit(variant, right, "different", "near-miss:variant")
        for variant, _transform, _feed in variant_of.get(right, [])[:1]:
            emit(left, variant, "different", "near-miss:variant")

    # (a3) construction different: real channels some normalizer collapses.
    collision_names = sorted(set(pools["db"]) | set(_lineup_names())
                             | set(pools["alias_canonical"]))
    for left, right, tags in _collisions(collision_names, rng, int(target * 0.08)):
        emit(left, right, "different", f"collision:{tags}")

    # (a4) construction: OTA station spellings (callsign ladder evidence).
    for left, right, label, generator in _station_pairs(rng, 500):
        emit(left, right, label, generator)
    for left, right, label, generator in _denylisted_station_pairs(
            rng, sorted(set(_merged_channel_names()) | set(_lineup_names()))):
        emit(left, right, label, generator)

    # (a5) aliases: the merged table is the plugins' human-curated claim that
    # a spelling names a canonical; undisputed claims are construction `same`
    # in BOTH directions (a lookup must resolve the alias from either side);
    # disputed ones go to the oracles. An alias of a near-miss canonical is
    # construction `different` from the other canonical.
    alias_data = _alias_data()
    disputed = _disputed_aliases()
    aliases_of: dict[str, list[str]] = {}
    for canonical, entries in sorted(alias_data["aliases"].items()):
        for entry in entries:
            alias = entry["alias"]
            aliases_of.setdefault(canonical, []).append(alias)
            label = None if alias in disputed else "same"
            emit(alias, canonical, label, "alias:variant")
            emit(canonical, alias, label, "alias:reverse")
    for country, table in sorted(alias_data.get("country_aliases", {}).items()):
        for canonical, entries in sorted(table.items()):
            for entry in entries:
                emit(entry["alias"], canonical, None, f"alias:variant:{country}")
    for left, right in near_miss:
        for alias in aliases_of.get(right, [])[:2]:
            if alias not in disputed and distinct_channels(left, alias):
                emit(left, alias, "different", "alias:near-miss")
        for alias in aliases_of.get(left, [])[:2]:
            if alias not in disputed and distinct_channels(alias, right):
                emit(alias, right, "different", "alias:near-miss")

    # Real pairs: case-only variants (kept apart by the merge's verbatim key).
    with open(os.path.join(MERGED, "PROVENANCE.json"), encoding="utf-8") as handle:
        provenance_data = json.load(handle)
    for country, info in sorted(provenance_data["channels"].items()):
        for group in info.get("case_variants", []):
            for left in group:
                for right in group:
                    if left < right:
                        emit(left, right, None, "case:variant")

    # Real pairs: close names across sources (lineup vs DB vs tests).
    cross = sorted(set(pools["lineup"]) | set(pools["tests"][:800])
                   | set(pools["db"]) | set(pools["parity"]))
    for left, right in _token_overlap_close(cross, rng, target):
        emit(left, right, None, "real:close")

    # Budget the target across pair kinds so construction truth cannot crowd
    # out the real pairs: ~45% construction same, ~30% construction different
    # (near misses, feed variants, collisions, other stations), the rest real
    # pairs (unanimous/majority labels). Each group is shuffled; the cut is
    # deterministic for a fixed seed.
    same = [spec for spec in specs if spec[2] == "same"]
    near = [spec for spec in specs if spec[2] == "different"]
    real = [spec for spec in specs if spec[2] is None]
    for group in (same, near, real):
        rng.shuffle(group)
    want_same = int(target * 0.45)
    want_near = int(target * 0.30)
    specs = (same[:want_same] + near[:want_near]
             + real[:max(0, target - want_same - want_near)])
    rng.shuffle(specs)
    stats = {
        "construction_specs": len(same),
        "near_miss_specs": len(near),
        "real_specs": len(real),
        "kept": {"same": min(len(same), want_same),
                 "different": min(len(near), want_near),
                 "real": min(len(real),
                             max(0, target - want_same - want_near))},
        "generator_forms": {family: {form_id: count
                                     for form_id, count in sorted(counts.items())}
                            for family, counts in sorted(evidence.items())},
        "provenance": provenance,
    }
    return [
        {"query": query, "candidate": candidate, "truth": label,
         "generator": generator}
        for query, candidate, label, generator in specs
    ], stats


# ---------------------------------------------------------------------------
# labelling
# ---------------------------------------------------------------------------

def _majority(verdicts: dict[str, bool | None]) -> str:
    """Majority label over valid oracle verdicts; ties -> unknown."""
    votes = [value for value in verdicts.values() if value is not None]
    if len(votes) < 2:
        return "unknown"
    same = sum(1 for value in votes if value)
    if same > len(votes) / 2:
        return "same"
    if same < len(votes) / 2:
        return "different"
    return "unknown"


#: Generator -> inventory rows the pair directly exercises. The generator IS
#: the attribution for synthetic pairs: the transform names the rule under
#: test far more precisely than substring hints can (which stay unioned in).
GENERATOR_ROWS: dict[str, list[str]] = {
    "synth:quality_suffix": ["N01"],
    "synth:country_prefix": ["N04", "N12", "N14"],
    "synth:platform_prefix": ["N04"],
    "synth:region": ["N02", "N10", "Z01", "Z04"],
    "synth:plus_one": ["T07"],
    "synth:decorative_unicode": ["N09"],
    "synth:zero_width": ["N13"],
    "synth:amp_and": ["N05"],
    "synth:dot_glue": ["N06"],
    "synth:suffix_drop": ["N07"],
    "synth:number_word": [],
    "synth:camel_glue": [],
    "synth:case_fold": [],
    "alias:variant": ["A01", "A02", "A03"],
    "alias:reverse": ["A01", "A02", "A03"],
    "alias:near-miss": ["A01", "A02"],
}

_REGION_WORDS = frozenset({
    "EAST", "WEST", "NORTH", "SOUTH", "PACIFIC", "ATLANTIC",
    "CENTRAL", "MOUNTAIN", "EASTERN", "WESTERN", "E", "W", "P",
})
_DESIGNATOR_RE = re.compile(r"^[A-Z]{1,3}[0-9]{1,2}$")


def _near_miss_rows(left: str, right: str) -> list[str]:
    """Rows a near-miss pair separates, from how the two names differ.

    Near-miss pairs are what the guards exist to separate (shared prefix,
    different channel), so the token-level difference attributes the rule:
    digit disagreements -> the number guards, designator splits -> N08,
    one-sided region words -> the zone rows, bare otherwise -> the overlap
    guard itself."""
    rows = ["T01"]
    left_tokens = set(re.findall(r"[A-Za-z0-9]+", left.upper()))
    right_tokens = set(re.findall(r"[A-Za-z0-9]+", right.upper()))
    left_digits = {token for token in left_tokens if token.isdigit()}
    right_digits = {token for token in right_tokens if token.isdigit()}
    if left_digits != right_digits:
        rows += ["T06", "T07"]
    left_des = {token for token in left_tokens if _DESIGNATOR_RE.match(token)}
    right_des = {token for token in right_tokens
                 if _DESIGNATOR_RE.match(token)}
    if left_des != right_des:
        rows += ["N08"]
    left_regions = left_tokens & _REGION_WORDS
    right_regions = right_tokens & _REGION_WORDS
    if left_regions != right_regions:
        rows += ["N02", "Z04"]
    return rows


def label_specs(specs: list[dict]) -> tuple[list[dict], dict]:
    """Run every oracle over every spec; attach verdicts and labels."""
    oracles_by_tag = oracles.all_oracles()
    pairs: list[dict] = []
    errors: dict[str, int] = {}
    agreement = {"agree": 0, "split": 0}
    started = time.time()
    for index, spec in enumerate(specs):
        query, candidate = spec["query"], spec["candidate"]
        verdicts: dict[str, bool | None] = {}
        for tag in TAGS:
            try:
                verdicts[tag] = bool(
                    oracles_by_tag[tag].same(query, candidate))
            except Exception:  # noqa: BLE001 - one bad oracle must not kill the run
                verdicts[tag] = None
                errors[tag] = errors.get(tag, 0) + 1
        valid = [value for value in verdicts.values() if value is not None]
        if len(set(valid)) <= 1 and valid:
            agreement["agree"] += 1
        else:
            agreement["split"] += 1
        if spec["truth"] is not None:
            label, source = spec["truth"], "construction"
        elif len(set(valid)) <= 1 and valid:
            label, source = ("same" if valid[0] else "different"), "unanimous"
        else:
            label, source = _majority(verdicts), "majority"
        generator_rows: set[str] = set()
        for transform in spec["generator"].split(":", 1)[1].split("+"):
            family = spec["generator"].split(":")[0]
            generator_rows.update(
                GENERATOR_ROWS.get(f"{family}:{transform.split('=')[0]}", []))
        # Alias spellings that ARE callsigns exercise the alias-callsign
        # rescue (A04): e.g. an alias `WWOR` carried by a stream name.
        if spec["generator"].split(":")[0] == "alias" and re.match(
                r"^[A-Z0-9]{3,8}(-(TV|DT))?$", query.upper()):
            generator_rows.add("A04")
        hints = sorted(set(_rule_hints(query)) | set(_rule_hints(candidate))
                         | generator_rows
                         | (set(_near_miss_rows(query, candidate))
                            if spec["generator"].startswith("near-miss:")
                            else set()))
        if hints == ["unattributed"]:
            hints = []
        pairs.append({
            "query": query,
            "candidate": candidate,
            "label": label,
            "label_source": source,
            "generator": spec["generator"],
            "verdicts": verdicts,
            "attribution": hints,
        })
        if (index + 1) % 5000 == 0:
            print(f"  labelled {index + 1}/{len(specs)} "
                  f"({time.time() - started:.0f}s)", file=sys.stderr)
    pairs.sort(key=lambda pair: (pair["query"], pair["candidate"]))
    stats = {
        "agreement": agreement,
        "oracle_errors": errors,
        "labelling_seconds": round(time.time() - started, 1),
    }
    return pairs, stats


# ---------------------------------------------------------------------------
# AI relabelling (--ai, keys present only)
# ---------------------------------------------------------------------------

def _ai_keys_present() -> bool:
    """Whether any CROSSTALK_AI__... override is set (keys are never on disk)."""
    return any(key.startswith("CROSSTALK_AI__") for key in os.environ)


def relabel_with_ai(pairs: list[dict]) -> tuple[list[dict], dict]:
    """Replace disputed-pair majority labels with the [ai.verify] chain.

    Pipes ``{"query", "candidate"}`` JSONL through
    ``crates/crosstalk-core/examples/label_pair.rs`` (``decide_yes_no``); only
    pairs whose label_source is ``majority`` are relabelled. No keys in the
    environment (the normal case, including this run) -> keep majority labels
    and record the skip.
    """
    disputed = [pair for pair in pairs
                if pair["label_source"] == "majority"]
    info: dict = {"disputed": len(disputed), "relabeled": 0, "skipped": None}
    if not disputed:
        return pairs, info
    if not _ai_keys_present():
        info["skipped"] = ("no CROSSTALK_AI__... keys in the environment; "
                           "majority labels kept")
        print(f"  --ai: {info['skipped']}", file=sys.stderr)
        return pairs, info
    example = os.path.join(
        REPO, "crates", "crosstalk-core", "examples", "label_pair.rs")
    if not os.path.exists(example):
        info["skipped"] = "label_pair example missing; majority labels kept"
        return pairs, info
    payload = "\n".join(
        json.dumps({"query": pair["query"], "candidate": pair["candidate"]},
                   sort_keys=True)
        for pair in disputed)
    try:
        completed = subprocess.run(
            ["cargo", "run", "--quiet", "--locked", "-p", "crosstalk-core",
             "--example", "label_pair"],
            input=payload + "\n", capture_output=True, text=True, cwd=REPO,
            timeout=3600)
    except (OSError, subprocess.TimeoutExpired) as exc:
        info["skipped"] = f"AI chain failed ({exc}); majority labels kept"
        return pairs, info
    if completed.returncode != 0:
        # The chain's stderr stays on the terminal: a provider error can echo
        # request details, and STATS.json is committed.
        print(completed.stderr[-2000:], file=sys.stderr)
        info["skipped"] = (f"AI chain exited {completed.returncode}; "
                           f"majority labels kept")
        return pairs, info
    answers = {}
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        answer = json.loads(line)
        answers[(answer["query"], answer["candidate"])] = answer
    for pair in disputed:
        answer = answers.get((pair["query"], pair["candidate"]))
        # Only a link that settled the question (probability past the accept
        # or reject threshold) relabels; an undecided chain keeps the vote.
        if not answer or answer.get("probability") is None or not answer.get("decided"):
            continue
        pair["label"] = ("same" if answer["probability"] >= 0.5
                         else "different")
        pair["label_source"] = "ai"
        pair["ai"] = {key: answer.get(key) for key in
                      ("probability", "provider", "model")}
        info["relabeled"] += 1
    return pairs, info


# ---------------------------------------------------------------------------
# output
# ---------------------------------------------------------------------------

def _write_pairs(path: str, pairs: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for pair in pairs:
            handle.write(json.dumps(pair, sort_keys=True,
                                    ensure_ascii=False) + "\n")


def _stats(pairs: list[dict], seed: int, target: int, extra: dict,
           ai_info: dict, elapsed: float) -> dict:
    by_generator: dict[str, int] = {}
    by_label: dict[str, int] = {}
    by_source: dict[str, int] = {}
    for pair in pairs:
        family = pair["generator"].split(":")[0]
        by_generator[family] = by_generator.get(family, 0) + 1
        by_label[pair["label"]] = by_label.get(pair["label"], 0) + 1
        by_source[pair["label_source"]] = by_source.get(pair["label_source"], 0) + 1
    return {
        "pairs": len(pairs),
        "target": target,
        "seed": seed,
        "by_generator": dict(sorted(by_generator.items())),
        "by_label": dict(sorted(by_label.items())),
        "by_label_source": dict(sorted(by_source.items())),
        "construction_vs_votes": {
            "construction": by_source.get("construction", 0),
            "unanimous": by_source.get("unanimous", 0),
            "majority": by_source.get("majority", 0),
            "ai": by_source.get("ai", 0),
            "unknown_excluded": by_label.get("unknown", 0),
        },
        "spec_counts": {
            "construction_specs": extra.get("construction_specs", 0),
            "near_miss_specs": extra.get("near_miss_specs", 0),
            "real_specs": extra.get("real_specs", 0),
            "kept": extra.get("kept", {}),
        },
        # Real-name count per synthetic form; forms at 0 are not generated.
        "generator_forms": extra.get("generator_forms", {}),
        "provenance": extra.get("provenance", []),
        "ai": ai_info,
        "build_seconds": round(elapsed, 1),
    }


def build(out_dir: str, target: int, seed: int, use_ai: bool) -> dict:
    """Full build into ``out_dir``. Returns the STATS dict."""
    started = time.time()
    os.makedirs(out_dir, exist_ok=True)
    print(f"collecting names (seed={seed}, target={target})...", file=sys.stderr)
    specs, extra = build_pairs(seed, target)
    print(f"  {len(specs)} pair specs "
          f"({extra['construction_specs']} construction, "
          f"{extra['real_specs']} real)", file=sys.stderr)
    print("running oracles...", file=sys.stderr)
    pairs, label_stats = label_specs(specs)
    ai_info: dict = {"disputed": 0, "relabeled": 0,
                     "skipped": "ai not requested"}
    if use_ai:
        pairs, ai_info = relabel_with_ai(pairs)
    _write_pairs(os.path.join(out_dir, "pairs.jsonl"), pairs)
    stats = _stats(pairs, seed, target, extra, ai_info, time.time() - started)
    stats.update(label_stats)
    with open(os.path.join(out_dir, "STATS.json"), "w",
              encoding="utf-8") as handle:
        json.dump(stats, handle, indent=2, sort_keys=True,
                  ensure_ascii=False)
        handle.write("\n")
    return stats


def _check(out_dir: str, target: int, seed: int, use_ai: bool) -> int:
    """Rebuild into a temp dir and fail if ``out_dir`` differs."""
    if use_ai and _ai_keys_present():
        print("--check refuses --ai with keys present: the AI chain is not "
              "bitwise reproducible; rerun --check without --ai.",
              file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory(prefix="corpus-check-") as tmp:
        build(tmp, target, seed, use_ai=False)
        for name in ("pairs.jsonl", "STATS.json"):
            want = os.path.join(out_dir, name)
            got = os.path.join(tmp, name)
            if not os.path.exists(want):
                print(f"--check: {want} missing; run build.py first "
                      f"(without --check).", file=sys.stderr)
                return 1
            with open(want, "rb") as handle:
                old = handle.read()
            with open(got, "rb") as handle:
                new = handle.read()
            if name == "STATS.json":
                old_stats = json.loads(old)
                new_stats = json.loads(new)
                # Runtimes/oracle-error timing vary; everything else must match.
                for key in ("build_seconds", "labelling_seconds"):
                    old_stats.pop(key, None)
                    new_stats.pop(key, None)
                # STATS.json records its own build time; the pair payload (all
                # labels, verdicts, generators) is compared byte for byte.
                old = json.dumps(old_stats, indent=2, sort_keys=True,
                                 ensure_ascii=False).encode() + b"\n"
                new = json.dumps(new_stats, indent=2, sort_keys=True,
                                 ensure_ascii=False).encode() + b"\n"
            if old != new:
                print(f"--check: {name} differs; rerun build.py (without "
                      f"--check).", file=sys.stderr)
                return 1
    print(f"--check: {out_dir} reproduces byte for byte "
          f"(timings excluded).")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default=os.path.join(REPO, "data", "corpus"))
    parser.add_argument("--pairs", type=int, default=DEFAULT_PAIRS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--subset", action="store_true",
                        help="deterministic CI subset: 2000 pairs, seed 7")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--ai", action="store_true")
    args = parser.parse_args(argv)

    target = SUBSET_PAIRS if args.subset else args.pairs
    seed = SUBSET_SEED if args.subset else args.seed
    if args.check:
        return _check(args.out, target, seed, args.ai)
    stats = build(args.out, target, seed, args.ai)
    print(json.dumps(stats["construction_vs_votes"], indent=2,
                     sort_keys=True))
    print(f"wrote {stats['pairs']} pairs to {args.out} "
          f"in {stats['build_seconds']}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
