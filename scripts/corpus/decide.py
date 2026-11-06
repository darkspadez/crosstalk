#!/usr/bin/env python3
"""Decide every matching-rules inventory row on the labelled corpus (step 5).

For each row in ``docs/matching-rules-inventory.md`` (one per inventory row):

* rows a single rule swap can isolate are decided by ABLATION
  (``ablate.py``): the row's variants are swapped one at a time into one base
  engine and scored on the construction-labelled pairs where the swap changes
  the verdict; interacting rows are searched jointly and the whole
  configuration settled by coordinate descent;
* T10/D02 come from a cut sweep of that final engine;
* rows whose mechanism cannot fire in a pair decision (no lineup country, no
  channel number, opt-ins off) are ``no-evidence`` and take the strictest
  variant; rows the plan fixes say so;
* the remaining whole-path rows keep a whole-oracle comparison on
  substring-hinted pairs and are never better than ``weak``.

Metric: cost = 2*FP + FN (a wrong attach/assign/rename is worse than a miss).

Outputs (deterministic: sorted, no timestamps)::

    data/corpus/decisions.json       row id -> chosen variant (+ evidence),
                                     the engine configuration, aliases, cases
    docs/matching-decisions.md       per row: variants, costs, example pairs,
                                     chosen variant, confidence

The script also decides every ``disputed`` alias in
``data/merged/alias_conflicts.json`` (oracle ``same(alias, claimant)`` votes,
weak) and every case-variant group in ``data/merged/PROVENANCE.json`` (fold).

Usage:
    python3 -I scripts/corpus/decide.py [--corpus DIR] [--decisions PATH] [--doc PATH]
    python3 -I scripts/corpus/decide.py --check [--corpus DIR] [--decisions PATH] [--doc PATH]

    --check      regenerate into a temp dir and fail if either output differs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile

sys.dont_write_bytecode = True

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ablate  # noqa: E402
import oracles  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MERGED = os.path.join(REPO, "data", "merged")
DEFAULT_CORPUS = os.path.join(REPO, "data", "corpus")
DEFAULT_DECISIONS = os.path.join(REPO, "data", "corpus", "decisions.json")
DEFAULT_DOC = os.path.join(REPO, "docs", "matching-decisions.md")

TAGS = ("janitor", "sm", "cm", "lu")
JAN, SM, CM, LU = TAGS
ALL = list(TAGS)
CORE = [SM, LU]

#: Row -> ({variant: [oracle tags]}, strictest variant, short rule title).
#: Variant groupings follow docs/matching-rules-inventory.md column by column;
#: the strictest variant is the one that merges the fewest names (fewest
#: conflations), and wins every no-evidence row.
VARIANTS: dict[str, tuple[dict[str, list[str]], str, str]] = {
    "N01": ({"core-strip": [SM, LU], "janitor": [JAN], "cm-keep": [CM]},
            "cm-keep", "Quality vocabulary (HDR/HEVC)"),
    "N02": ({"janitor": [JAN], "core": [SM], "cm-keep": [CM], "lu-strip": [LU]},
            "cm-keep", "Bare zone words (Pacific/Central/East/West)"),
    "N03": ({"janitor": [JAN], "core": [SM, CM, LU]},
            "core", "Misc parenthesized leftovers ((A), (CX), (...))"),
    "N04": ({"janitor": [JAN], "core": [SM, LU], "cm-strict": [CM]},
            "cm-strict", "Provider-prefix table (bare/glued/category tags)"),
    "N05": ({"janitor-split": [JAN], "keep": [SM, CM, LU]},
            "keep", "`&` vs `and`"),
    "N06": ({"janitor-letters": [JAN], "core-wordchar": [SM, CM, LU]},
            "janitor-letters", "Dot handling (JusticeCentral.TV, 97.2)"),
    "N07": ({"janitor-floor": [JAN], "unconditional": [SM, CM, LU]},
            "janitor-floor", "Suffix-strip floor (Network/Channel/TV)"),
    "N08": ({"sm-rejoin": [SM], "split": [JAN, CM, LU]},
            "sm-rejoin", "Designator repair (F1 vs F 1)"),
    "N09": ({"strip": [SM, LU], "keep": [JAN, CM]},
            "keep", "Decorative-Unicode category strip"),
    "N10": ({"lu-strip": [LU], "keep": [JAN, SM, CM]},
            "keep", "Bare-region-strip flag"),
    "N11": ({"sm-strip": [SM], "keep": [JAN, CM, LU]},
            "keep", "Cinemax strip"),
    "N12": ({"after-hyphen": [JAN, CM], "first": [SM, LU]},
            "after-hyphen", "Prefix strip vs hyphen normalization order"),
    "N13": ({"strip": [SM, LU], "keep": [JAN, CM]},
            "keep", "Invisible-format (Cf) strip"),
    "N14": ({"janitor-anywhere": [JAN], "cm-double-tag": [CM], "core": [SM, LU]},
            "core", "Geographic (country tag anywhere) table"),
    "C01": ({"janitor": [JAN], "core-known": [SM], "cm-lookup": [CM],
             "core-naked": [LU]},
            "janitor", "Callsign ladder shape"),
    "C02": ({"janitor": [JAN], "sm": [SM], "cm": [CM], "lu-none": [LU],
             "merged": []},
            "lu-none", "Known-callsign allowlist source"),
    "C03": ({"janitor": [JAN], "sm": [SM], "cm": [CM], "lu-none": [LU]},
            "lu-none", "Parenthesized denylist rescue"),
    "C04": ({"sm-rescue": [SM], "none": [JAN, CM, LU]},
            "none", "OTA-number rescue at the loose position"),
    "C05": ({"shared": ALL}, "shared", "Callsign normalization"),
    "C06": ({"janitor": [JAN], "sm-bypass": [SM], "cm-ota-path": [CM],
             "lu-anchor": [LU]},
            "janitor", "What a callsign decides at the app level"),
    "A01": ({"janitor": [JAN], "sm": [SM], "cm": [CM], "lu": [LU], "identity": []},
            "sm", "Alias lookup rule (exact/folded/fuzzy)"),
    "A02": ({"cm-index": [CM], "none": [JAN, SM, LU]},
            "cm-index", "Reverse / token pre-index"),
    "A03": ({"merged-union": ALL}, "merged-union",
            "Shipped alias tables (union in step 4)"),
    "A04": ({"lu-rescue": [LU], "none": [JAN, SM, CM]},
            "none", "Alias-declared callsign rescue"),
    "T01": ({"janitor": [JAN], "cm": [CM], "core": [LU], "sm-never": [SM]},
            "core", "Majority-overlap guard + distinctive definition"),
    "T02": ({"always": [JAN, CM, LU], "sm-never": [SM]},
            "always", "Basic-mode token floor"),
    "T03": ({"janitor": [JAN], "sm": [SM], "cm": [CM], "lu": [LU]},
            "cm", "fuzzy_match stage structure"),
    "T04": ({"guarded": [CM, LU], "unguarded": [JAN, SM]},
            "guarded", "Exact 0.97 tier guard"),
    "T05": ({"scaled": [JAN, CM, LU], "sm-never": [SM]},
            "scaled", "Length-scaled threshold"),
    "T06": ({"jan-lu": [JAN, LU], "cm": [CM], "sm": [SM]},
            "cm", "Trailing-number guard"),
    "T07": ({"janitor": [JAN], "cm": [CM], "sm-designator": [SM], "lu-none": [LU],
             "janitor-designator": []},
            "sm-designator", "Digit-token guard"),
    "T08": ({"janitor": [JAN], "sm": [SM], "cm": [CM], "lu-absent": [LU]},
            "cm", "find_best_match shape"),
    "T09": ({"janitor": [JAN], "lu": [LU], "sm-collector": [SM],
             "cm-dead": [CM]},
            "sm-collector", "match_all_streams shape"),
    "T10": ({"janitor-95": [JAN], "sm-85": [SM], "cm-lu-80": [CM, LU]},
            "janitor-95", "Sensitivity presets / decision threshold"),
    "T11": ({"sm-flagged": [SM], "ignore-tags-only": [JAN, CM, LU]},
            "sm-flagged", "Precompute cache flag memory"),
    "T12": ({"janitor": [JAN], "sm": [SM], "cm": [CM], "lu": [LU]},
            "lu", "Tie-break on equal score"),
    "Z01": ({"sm-zone": [SM], "none": [JAN, CM, LU]},
            "sm-zone", "Feed-zone detection and use"),
    "Z02": ({"sm-guard": [SM], "none": [JAN, CM, LU]},
            "none", "One-time-zone guard"),
    "Z03": ({"sm-expand": [SM], "none": [JAN, CM, LU]},
            "none", "Zone expansion of channel DBs"),
    "Z04": ({"janitor": [JAN], "lu": [LU], "none": [SM, CM]},
            "lu", "East/West/Pacific post-match filter"),
    "Z05": ({"lu-skip": [LU], "none": [JAN, SM, CM]},
            "lu-skip", "M3U group-header skip"),
    "Z07": ({"sm-strict": [SM], "lu-pacific": [LU]},
            "sm-strict", "Bare (P) Pacific reading"),
    "Z08": ({"strip-tags": [], "no-strip": ALL},
            "no-strip", "Zone word followed by a trailing tag"),
    "Z06": ({"lu-twins": [LU], "none": [JAN, SM, CM]},
            "none", "Upgrade-quality channel/stream segregation"),
    "G01": ({"sm": [SM], "lu": [LU], "none": [JAN, CM]},
            "lu", "Country vocabulary"),
    "G02": ({"sm": [SM], "lu": [LU], "none": [JAN, CM]},
            "lu", "Detection entry points"),
    "G03": ({"lu-strict": [LU], "sm-failopen": [SM], "none": [JAN, CM]},
            "lu-strict", "Cross-border rule"),
    "G04": ({"lu-counter": [LU], "none": [JAN, SM, CM]},
            "lu-counter", "Filter accounting"),
    "D01": ({"merged-union": ALL}, "merged-union",
            "Shipped channel data (union in step 4)"),
    "D02": ({"janitor-95": [JAN], "sm-85": [SM], "cm-lu-80": [CM, LU]},
            "janitor-95", "Decision threshold the run path applies"),
    "D03": ({"lu-boost": [LU], "janitor-dead": [JAN], "none": [SM, CM]},
            "none", "Channel-number boost"),
    "D04": ({"jan-lu-anchor": [JAN, LU], "none": [SM, CM]},
            "jan-lu-anchor", "Callsign anchor in match_all_streams"),
    "D05": ({"janitor": [JAN], "sm": [SM], "cm": [CM], "lu-none": [LU]},
            "sm", "OTA broadcast path"),
    "D06": ({"janitor": [JAN], "na": [SM, CM, LU]},
            "janitor", "EPG-side scoring (sole implementation)"),
    "D07": ({"sm-collector": [SM], "lu-attach-all": [LU], "na": [JAN, CM]},
            "sm-collector", "Stream-side selection"),
    "D08": ({"janitor": [JAN], "sm": [SM], "cm": [CM], "lu": [LU]},
            "janitor", "Name-write rules incl unknown tagging"),
    "D09": ({"sm-both": [SM], "janitor": [JAN], "cm": [CM], "lu": [LU]},
            "lu", "extract_zone vs extract_tags split"),
    "D10": ({"cm": [CM], "na": [JAN, SM, LU]},
            "cm", "Channel-naming target selection (sole implementation)"),
    "R01": ({"fallback": ALL}, "fallback",
            "Similarity arithmetic (fallback vs rapidfuzz)"),
}

#: Rows decided by single-rule ablation (``ablate.py``): the row's variants
#: are swapped into one base engine and scored on construction truth.
ABLATED = ("N01", "N02", "N03", "N04", "N05", "N06", "N07", "N08", "N09",
           "N11", "N12", "N13", "N14", "T01", "T06", "T07", "A01", "A04")

#: Rows read off an ablation pseudo-row: row -> (pseudo-row, pseudo-variant ->
#: row variant). ZONE is Z01 and Z04 searched together; CALLSIGN is the whole
#: ladder (shape, allowlist, denylist rescue, OTA-number rescue); N10 is
#: whether N02's bare-region strip is on; A02 is whether A01 uses CM's index.
DERIVED: dict[str, tuple[str, dict[str, str]]] = {
    "N10": ("N02", {"core": "keep", "janitor": "keep", "cm-keep": "keep",
                    "lu-strip": "lu-strip"}),
    "Z01": ("ZONE", {v: rows["Z01"] for v, rows in ablate.ZONE_ROWS.items()}),
    "Z02": ("ZONE", {v: rows["Z02"] for v, rows in ablate.ZONE_ROWS.items()}),
    "Z04": ("ZONE", {v: rows["Z04"] for v, rows in ablate.ZONE_ROWS.items()}),
    # D09 follows the zone rule it serves: SM's routing needs extract_zone
    # beside extract_tags; LU's/JAN's filters read the original names.
    "D09": ("ZONE", {v: rows["D09"] for v, rows in ablate.ZONE_ROWS.items()}),
    "C01": ("CALLSIGN", {v: rows["C01"] for v, rows in ablate.CALLSIGN_ROWS.items()}),
    "C02": ("CALLSIGN", {v: rows["C02"] for v, rows in ablate.CALLSIGN_ROWS.items()}),
    "C03": ("CALLSIGN", {v: rows["C03"] for v, rows in ablate.CALLSIGN_ROWS.items()}),
    "C04": ("CALLSIGN", {v: rows["C04"] for v, rows in ablate.CALLSIGN_ROWS.items()}),
    "A02": ("A01", {"lu": "none", "janitor": "none", "sm": "none", "cm": "cm-index",
                    "identity": "cm-index"}),
}

#: Rows whose mechanism cannot fire in the corpus context at all, so no pair
#: can be evidence for them (the old whole-oracle "strong" for G01 was other
#: rules' splits attributed by substring).
INACTIVE: dict[str, str] = {
    "G01": ("Country detection never runs in a pair decision: LU's filter "
            "needs a lineup country and SM's restrict_matching_to_country is "
            "off by default, so no verdict reads the vocabulary. Splits on "
            "country-tagged pairs come from the prefix tables (N04/N14)."),
    "G02": "Same as G01: no entry point is called without a lineup country.",
    "G03": "Same as G01: the cross-border rule needs a configured country.",
    "G04": "Reporting only: a counter changes no verdict.",
    "D03": ("The boost needs a lineup channel number; pairs carry none "
            "(Janitor's run path passes None too)."),
    "Z06": "The quality_aware opt-in is off on every shipped default.",
    "T11": "Cache staleness needs repeated runs with changing flags.",
    "R01": ("Every oracle and the ablation base run the pure-Python fallback "
            "(no rapidfuzz), so the two arithmetics never meet."),
}

#: Orchestrator-settled conflicts (Appendix C): decided by the orchestrator,
#: recorded here so decide.py --check reproduces them. Row -> (variant, reason).
ORCHESTRATED: dict[str, tuple[str, str]] = {
    "Z08": ("strip-tags", "Orchestrator decision (phase 3 step 9, step-9 review): "
            "the zone is read AFTER stripping the trailing run of writer tags "
            "(`tags::strip_zone_ignorable` over the shared `tags.json` vocabulary: "
            "quality, junk and status brackets and bare words, plus `+N`, which stays "
            "a timeshift; regional words, unknown groups and country markers stop the "
            "run) in the engine's routing zone only: Rust `extract_zone_noguard` and "
            "the decided Python `sm_zone_noguard` (byte-identical over every corpus "
            "and merged-channel name). SM's guarded `extract_zone` transcription is "
            "unchanged. Evidence: the step-9 review's open issue — "
            "`HBO West (Backup)`, `HBO West [4K]` and `ITV1 (Central West) HD` read as "
            "zone Default, so `score(\"HBO\", \"HBO West (Backup)\") = 100` and ten "
            "West feeds join the default family in `docs/oracle-reports/streams.md` "
            "(the only unattributed row). No plugin variant strips trailing tags "
            "before zone detection (SM's own `_ZONE_*_RE` tails cover only its "
            "quality list), so there is no variant to ablate. Corpus effect of the "
            "strip under the decided configuration: engine cost 856 -> 853 (FN "
            "788 -> 785, FP 34 unchanged); four corpus names change zone, all "
            "toward their marker (`Antenna TV Pacific [HD]`, `FanDuel Sports West "
            "[HD]`, `Nickelodeon East [FHD]`, `US: STARZ WEST HD [FHD]`). "
            "`West Ham TV` / `Westworld` stay Default (no word boundary)."),
    "Z07": ("sm-strict", "Orchestrator decision (phase 3 step 6, Appendix C): "
            "`(P)` is NOT a Pacific zone marker (Stream-Mapparr's stricter reading; "
            "SM `_ZONE_PACIFIC_RE` matches bare PACIFIC and `(Pacific)`/`(PT)` only). "
            "The normalizer strips `(P)` as misc noise, so `Sportsnet (P)` == `Sportsnet` "
            "and != `Sportsnet Pacific`; Lineuparr's `(P)` = Pacific test expectations "
            "are a known conflict (Appendix C).")
}

#: The plan or another decision fixes these; the row records the consequence.
#: Row -> (variant, reason).
BY_DESIGN: dict[str, tuple[str, str]] = {
    "A03": ("merged-union", "Step 4 built one alias table (union, conflicts "
            "listed); the ablation runs on it."),
    "D01": ("merged-union", "Step 4 built one channel DB per country; pairs "
            "cannot re-decide the candidate universe."),
    "C05": ("shared", "Every ladder already calls the same normalize_callsign."),
    "T09": ("lu", "The ablation base IS Lineuparr's match_all_streams (alias -> "
            "exact -> substring -> token-sort, per-candidate guards, region "
            "and country filters), so every ablated row was measured in this "
            "shape. Janitor's is the same shape plus the dead D03 boost."),
    "D08": ("cm", "The plan keeps Channel-Mapparr's cleanup at full parity, "
            "including its unknown tagging (`[Unk]`), in the one template "
            "engine (step 7), with the documented fix that a suffix a name "
            "already ends with is never re-appended; CM's "
            "build_final_channel_name also re-attaches regional/extra/bracket "
            "tags. The strictest alternative (JAN, no unknown tagging) would "
            "drop a lineup feature the plan requires."),
    "D07": ("sm-collector", "The plan's selection rule is the best identity "
            "family (goal reset); of the shipped variants only SM's collector "
            "attaches a family gated on one winner. LU's attach-all attaches "
            "every candidate above the cut, several identities at once."),
}

#: Ablated rows whose positive evidence is labelled by the very data the
#: variants read, so recall is won by construction: never better than weak.
CAPPED: dict[str, str] = {
    "A01": ("The `same` alias pairs are labelled by the merged alias table "
            "the lookups read, so a lookup that consults it in both directions "
            "wins recall by construction; only the false-positive side "
            "(near-miss aliases, zone variants) is independent evidence."),
    "A02": "Same circularity as A01.",
    "D09": ("Read off the zone rule for coherence; display re-attachment "
            "itself is not measured."),
    "Z02": ("The evidence rests on the corpus labelling a single-time-zone "
            "region (`UK: BBC One West`) as its own channel, the semantic "
            "SM's guard was written against (its comment: the plain BBC One "
            "channel carrying both regions is fine)."),
}

#: Per-row notes appended to the generated reason.
NOTES: dict[str, str] = {
    "T02": (" The basic-mode floor is in the ablation base (LU's "
            "match_all_streams, always majority); no rule swap isolates it "
            "from T01, whose guard it parameterises."),
    "T03": " Stage order is fixed by the base (T09); one candidate cannot show it.",
    "T04": (" The 0.97 exact tier is guarded in the base (LU); an unguarded "
            "exact tier is how `Golf Channel` false positives happen."),
    "T05": (" Length-scaled thresholds are in the base (LU) at every stage."),
    "T08": " find_best_match is not on the base's path; CM's in-loop guards match it.",
    "C06": (" App-level use of a callsign (JAN +50 structural score, SM's "
            "bypass, CM's OTA path, LU's anchor) is a whole-path rule; the "
            "ablation base keeps LU's anchor (D04)."),
    "D04": " The anchor is in the ablation base; the CALLSIGN group measures it with each ladder.",
    "D06": (" Sole implementation (Janitor's EPG scorer); guide migrates "
            "onto identity families in step 8. Kept."),
    "D10": (" Sole implementation (CM's premium rename path); lineup names "
            "are built on it in step 15. Kept."),
    "Z02": (" Measured as SM's routing with and without "
            "`_country_has_one_timezone`."),
    "D09": (" Coherence, not a measurement of display re-attachment: the "
            "split follows the zone rule chosen in ZONE (SM's routing reads "
            "`extract_zone`; LU's and JAN's filters read original names)."),
    "Z03": " Zone expansion changes the candidate universe, not one pair.",
    "Z05": (" No separator-style names occur in the corpus; the base skips "
            "them. Skipping a header can never drop a real channel."),
    "N01": (" Rests on positives only: a pair differing only by a quality "
            "tag is the same channel by construction (the plan keeps quality "
            "as an identity attribute, not identity), so no negative exists; "
            "the deciding forms are HEVC (15 real names) and HDR (1)."),
    "N04": (" Negatives for bare-country stripping cannot be built safely: "
            "the plugins' own example (`UK Gold`) was the old name of `Gold`, "
            "and the near-miss filter drops pairs differing only in country "
            "tags, so this row leans to the table that strips more; weak."),
    "N08": (" Coherence: designator repair only matters to a designator "
            "guard (rejoining RAISES `Sky Sports F1 UHD` vs `Sky Sports UHD "
            "1` to 0.923; the guard refuses it), so it is preferred exactly "
            "when T07 reads designator tokens (`ablate.preference_for`)."),
    "T07": (" `janitor-designator` is not a plugin variant: JAN's "
            "disagreement shape over SM's designator tokens (with N08's "
            "repair), the one guard that keeps SM issue #50 apart without "
            "SM's numbered-vs-unnumbered rejections (Appendix C)."),
    "N12": (" Coherence: with N04 = core, whose patterns expect the hyphen "
            "still in place (`matching_core.py:349-353`), the core order is "
            "preferred (`ablate.preference_for`)."),
    "A04": (" Measured in the alias group: LU's alias-declared callsign "
            "rescue never fires on the corpus."),
    "A01": (" Every shipped lookup resolves only ONE direction (JAN/SM/LU "
            "key on the query, CM's reverse index maps the query to a "
            "canonical), which is most of the cost; `identity` (both names "
            "through CM's reverse index, the plan's resolve) fixes the misses "
            "but its normalized keys collide across channels (the false "
            "positives). Step 6's identity resolve needs collision-checked "
            "keys before it can replace the lookup."),
}


#: The plugins' own test assertions, restated as pair verdicts: the decided
#: engine is checked against every plugin's regression intent, not only the
#: corpus. (query, candidate, expected same, source).
EXPECTATIONS = [
    ("Sportsnet Pacific", "Sportsnet", False, "LU tests/test_sportsnet_pacific.py:21-31"),
    ("Sportsnet Pacific", "Sportsnet East", False, "LU tests/test_sportsnet_pacific.py:21-31"),
    ("Sportsnet Pacific", "Sportsnet West", False, "LU tests/test_sportsnet_pacific.py:21-31"),
    ("Sportsnet (P)", "Sportsnet", False, "LU tests/test_sportsnet_pacific.py:33-42"),
    ("Sportsnet (P)", "Sportsnet Pacific", True, "LU tests/test_sportsnet_pacific.py:33-42"),
    ("Sportsnet", "Sportsnet Pacific", False, "LU tests/test_sportsnet_pacific.py:46-56"),
    ("Sportsnet", "Sportsnet West", False, "LU tests/test_sportsnet_pacific.py:46-56"),
    ("Food Network", "Food Network Eastern", True, "LU tests/test_eastern_region_matching.py:32"),
    ("Food Network", "Food Network East", True, "LU tests/test_eastern_region_matching.py:36"),
    ("Food Network", "Food Network (Eastern)", True, "LU tests/test_eastern_region_matching.py:40"),
    ("ABC News Live", "BBC News", False, "LU tests/test_alias_callsign_guard.py:33-37"),
    ("ABC News Live", "CBC News", False, "LU tests/test_alias_callsign_guard.py:39-43"),
    ("Comedy TV", "(US) Comedy Central (S)", False, "LU tests/test_comedy_tv_disambiguation.py:3-8"),
    ("Comedy TV", "USA: Comedy TV", True, "LU tests/test_comedy_tv_disambiguation.py:21-34"),
    ("Sky Sports F1 UHD", "Sky Sports UHD 1", False, "SM tests/test_designator_tokens.py:92-95"),
    ("Sky Sports F1 UHD", "Sky Sports F1 HD", True, "SM tests/test_designator_tokens.py:98-101"),
    ("Fox Sports 1", "Fox Sports 2", False, "SM tests/test_designator_tokens.py:104-107"),
    ("Fox Sports 1", "Fox Sports 1 HD", True, "SM tests/test_designator_tokens.py:110-112"),
    ("CNN", "CNN HD", True, "SM tests/test_designator_tokens.py:115-118"),
    ("Sky Sports F1", "Sky Sports", False, "SM tests/test_designator_tokens.py:121-124"),
]


def check_expectations(config: dict, alias_map: dict) -> list[dict]:
    """Each plugin-test expectation under the decided engine and each oracle."""
    engine = ablate.Engine(config, ablate.THRESHOLDS[0], alias_map)
    oracles_by_tag = oracles.all_oracles()
    out = []
    for query, candidate, expected, source in EXPECTATIONS:
        row = {"query": query, "candidate": candidate, "expected": expected,
               "source": source, "engine": engine.same(query, candidate)}
        for tag in TAGS:
            row[tag] = bool(oracles_by_tag[tag].same(query, candidate))
        out.append(row)
    return out


#: Combinations the decisions must satisfy to be ONE engine. decide.py fails
#: rather than write a table that violates one. (statement, check over the
#: row -> variant map).
COHERENCE_CHECKS = [
    ("Zones have one rule: feed separation is SM's routing (Z01) or a "
     "post-match filter (Z04), never both, and D09 keeps what that rule reads "
     "(`extract_zone` for routing).",
     lambda r: not (r["Z01"] == "sm-zone" and r["Z04"] != "none")
     and (r["D09"] == "sm-both") == (r["Z01"] == "sm-zone")),
    ("Bare East/West are stripped for scoring (N02) only when a zone rule "
     "enforces feeds afterwards; N10 is N02's bare-region switch.",
     lambda r: (r["N02"] == "cm-keep" or r["Z01"] != "none" or r["Z04"] != "none")
     and (r["N10"] == "lu-strip") == (r["N02"] == "lu-strip")),
    ("`(P)` reads as no zone (Z07 `sm-strict`) only under SM's routing (Z01 "
     "`sm-zone`) with no post-match filter reading `(p)` (Z04 `none`).",
     lambda r: r.get("Z07") != "sm-strict"
     or (r["Z01"] == "sm-zone" and r["Z04"] == "none")),
    ("Trailing tags strip before the zone read (Z08 `strip-tags`) only under SM's "
     "unguarded routing (Z01 `sm-zone`, Z02 `none`: `sm_zone_noguard` is the one "
     "zone read that strips) with no post-match filter reading original names "
     "(Z04 `none`).",
     lambda r: r.get("Z08") != "strip-tags"
     or (r["Z01"] == "sm-zone" and r["Z02"] == "none" and r["Z04"] == "none")),
    ("The prefix order fits the prefix table (N12 `first` with the core's N04).",
     lambda r: r["N04"] != "core" or r["N12"] == "first"),
    ("Designator repair (N08) exactly when the digit guard (T07) reads "
     "designator tokens.",
     lambda r: (r["N08"] == "sm-rejoin") == (r["T07"] in ("sm-designator",
                                                          "janitor-designator"))),
    ("One callsign ladder: C01-C04 are one CALLSIGN configuration; the anchor "
     "(D04) reads high-confidence callsigns only, so the loose OTA-number "
     "rescue (C04, low confidence) has nothing to feed.",
     lambda r: r["D04"] == "jan-lu-anchor"
     and not (r["C04"] == "sm-rescue" and r["C02"] == "lu-none")),
    ("The ranker is LU's match_all_streams shape (T09), which carries the "
     "always-majority floor (T02), the guarded exact tier (T04), scaled "
     "thresholds (T05), the header skip (Z05) and LU's country rule (G01-G03, "
     "strict only once a country is configured).",
     lambda r: r["T09"] == "lu" and r["T02"] == "always" and r["T04"] == "guarded"
     and r["T05"] == "scaled" and r["Z05"] == "lu-skip" and r["G03"] == "lu-strict"),
    ("Aliases resolve through one index: `identity` (A01) is CM's reverse "
     "index (A02) over the merged table (A03).",
     lambda r: r["A01"] != "identity" or r["A02"] == "cm-index"),
]


def check_coherence(rows: dict) -> list[dict]:
    variants = {row: decision["variant"] for row, decision in rows.items()}
    results = [{"statement": text, "holds": bool(check(variants))}
               for text, check in COHERENCE_CHECKS]
    broken = [item["statement"] for item in results if not item["holds"]]
    if broken:
        raise RuntimeError("decisions are not one engine: " + " | ".join(broken))
    return results


# ---------------------------------------------------------------------------
# whole-oracle scoring (rows no ablation isolates)
# ---------------------------------------------------------------------------

def _variant_prediction(verdicts: dict, members: list[str]):
    """Majority of the member oracles; None on abstain (no/error votes, tie)."""
    votes = [verdicts[tag] for tag in members if verdicts.get(tag) is not None]
    if not votes:
        return None
    same = sum(1 for vote in votes if vote)
    if same * 2 == len(votes):
        return None
    return same * 2 > len(votes)


def _scores(predictions: list, labels: list) -> dict:
    """TP/FP/FN/TN and precision/recall/F1 with FP weighted 2x."""
    tp = sum(1 for pred, lab in zip(predictions, labels)
             if pred and lab == "same")
    fp = sum(1 for pred, lab in zip(predictions, labels)
             if pred and lab == "different")
    fn = sum(1 for pred, lab in zip(predictions, labels)
             if not pred and lab == "same")
    tn = sum(1 for pred, lab in zip(predictions, labels)
             if not pred and lab == "different")
    precision = tp / (tp + 2 * fp) if (tp + 2 * fp) else (1.0 if not fp else 0.0)
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    f1 = (2 * precision * recall / (precision + recall)
          if (precision + recall) else 0.0)
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "precision": round(precision, 4), "recall": round(recall, 4),
            "f1": round(f1, 4)}


def decide_row_whole(row: str, pairs: list[dict]) -> dict:
    """Whole-oracle comparison for a row no ablation isolates.

    Each variant is its plugins' WHOLE decision path on the pairs whose
    names substring-hint at the row (``disagreements._rule_hints``). Whole
    paths differ in many rules at once, so a split pair is not evidence for
    THIS row: the result is never better than ``weak``, and with fewer than 10
    split pairs the strictest variant stands."""
    variants, strictest, _title = VARIANTS[row]
    evidence = [
        pair for pair in pairs
        if row in pair.get("attribution", [])
        and pair["label"] in ("same", "different")
        and len({pair["verdicts"].get(tag) for tag in TAGS
                 if pair["verdicts"].get(tag) is not None}) > 1
    ]
    scored: dict[str, dict] = {}
    positives: dict[str, int] = {}
    for name, members in variants.items():
        predictions: list[bool] = []
        labels: list[str] = []
        for pair in evidence:
            prediction = _variant_prediction(pair["verdicts"], members)
            if prediction is None:
                continue
            predictions.append(prediction)
            labels.append(pair["label"])
        result = _scores(predictions, labels)
        result["n"] = len(predictions)
        scored[name] = result
        positives[name] = sum(1 for pred in predictions if pred)
    ranked = sorted(scored, key=lambda name: (
        -scored[name]["f1"],
        positives[name] + (0 if name == strictest else 10 ** 9)))
    margin = (scored[ranked[0]]["f1"] - scored[ranked[1]]["f1"]
              if len(ranked) > 1 else 0.0)
    majority = sum(1 for pair in evidence if pair["label_source"] == "majority")
    if len(evidence) < 10:
        chosen, confidence = strictest, "no-evidence"
        reason = (f"Whole-path comparison only: fewer than 10 split pairs "
                  f"({len(evidence)}); default `{strictest}` (strictest).")
    else:
        chosen, confidence = ranked[0], "weak"
        reason = (f"Whole-path comparison only ({len(evidence)} split pairs, "
                  f"{majority} majority-voted, F1 margin {margin:.3f}): the "
                  f"plugins' whole decision paths differ in many rules at "
                  f"once, so this cannot isolate the row; weak at best.")
    return {"variant": chosen, "confidence": confidence, "reason": reason,
            "method": "whole-path", "evidence_pairs": len(evidence),
            "majority_pairs": majority, "scores": scored,
            "strictest": strictest}


# ---------------------------------------------------------------------------
# ablation-backed decisions
# ---------------------------------------------------------------------------

#: `strong` needs at least this many discriminating construction pairs, a
#: winner at least STRONG_RATIO cheaper than the runner-up (and by at least
#: STRONG_GAP), the same winner at both base cuts, and most of the deciding
#: pairs drawn from forms providers really write (EVIDENCED_SHARE).
STRONG_PAIRS = 20
STRONG_RATIO = 0.8
STRONG_GAP = 3
EVIDENCED_SHARE = 0.5
#: A synthetic form counts as evidenced when real names carry it this often.
FORM_FLOOR = 5


def _evidenced(pair: dict, form_counts: dict) -> bool:
    """A pair rests on real naming: not synthetic, or every synthetic form in
    it occurs at least FORM_FLOOR times in real names."""
    family, _, rest = pair["generator"].partition(":")
    if family != "synth":
        return True
    for transform in rest.split("+"):
        name, _, form = transform.partition("=")
        if form and form_counts.get(name, {}).get(form, 0) < FORM_FLOOR:
            return False
    return True


def _confidence(table80: dict, table90: dict, winner: str, disc: int,
                evidenced_share: float, preference: list[str]) -> tuple[str, str]:
    """(confidence, reason) for one row from its cost tables."""
    if disc < ablate.MIN_EVIDENCE:
        if winner == preference[0] or preference[0] not in table80:
            return "no-evidence", (f"{disc} discriminating construction pairs "
                                   f"(< {ablate.MIN_EVIDENCE}): strictest variant.")
        return "weak", (f"only {disc} discriminating construction pairs "
                        f"(< {ablate.MIN_EVIDENCE}), but they favour "
                        f"`{winner}` over the strictest `{preference[0]}` by "
                        f"more than noise (cost {table80[winner]['cost']} vs "
                        f"{table80[preference[0]]['cost']}).")
    others = [variant for variant in table80 if variant != winner]
    runner = min(others, key=lambda v: (table80[v]["cost"], preference.index(v)
                                        if v in preference else 99))
    win_cost, run_cost = table80[winner]["cost"], table80[runner]["cost"]

    def best(table):
        return min(table, key=lambda v: (table[v]["cost"],
                                         preference.index(v) if v in preference else 99))
    stable = best(table90) == winner or (
        table90[winner]["cost"] - min(c["cost"] for c in table90.values())
        <= ablate.NOISE)
    clear = (run_cost - win_cost >= STRONG_GAP
             and win_cost <= STRONG_RATIO * run_cost)
    facts = (f"{disc} discriminating construction pairs; cost (2*FP+FN) "
             f"{win_cost} vs {run_cost} for runner-up `{runner}`; "
             f"{'same' if stable else 'DIFFERENT'} winner at cut 90; "
             f"{evidenced_share:.0%} of the deciding pairs on evidenced forms.")
    if disc >= STRONG_PAIRS and clear and stable and evidenced_share >= EVIDENCED_SHARE:
        return "strong", facts
    return "weak", facts


def decide_ablated(row: str, reports: dict, pairs: list[dict],
                   form_counts: dict) -> dict:
    """Decision for an ablated or derived row from the final-config reports."""
    report = reports[row]
    t80, t90 = ablate.THRESHOLDS
    at80, at90 = report["per_threshold"][t80], report["per_threshold"][t90]
    deciding = at80["deciding"]
    share = (sum(1 for index in deciding if _evidenced(pairs[index], form_counts))
             / len(deciding)) if deciding else 0.0
    confidence, facts = _confidence(at80["table"], at90["table"], report["variant"],
                                    at80["discriminating"], share,
                                    report["preference"])
    if row in CAPPED and confidence == "strong":
        confidence = "weak"
        facts += f" Capped at weak: {CAPPED[row]}"
    if row in DERIVED:
        parent = DERIVED[row][0]
        method = f"ablation via {parent}"
        reps = ", ".join(f"`{variant}` = `{parent}={pseudo}`"
                         for variant, pseudo in sorted(report["representatives"].items()))
        facts += f" Each variant is its cheapest `{parent}` configuration ({reps})."
    else:
        method = "ablation"
    return {"variant": report["variant"], "confidence": confidence,
            "reason": f"Ablation on the base engine: {facts}",
            "method": method, "evidence_pairs": at80["discriminating"],
            "evidenced_share": round(share, 4),
            "costs": {str(th): {v: dict(c) for v, c in
                                sorted(report["per_threshold"][th]["table"].items())}
                      for th in (t80, t90)},
            "examples": at80["examples"], "strictest": report["preference"][0]}


def decide_threshold(sweep: dict) -> dict:
    """T10/D02 from the final configuration's cut sweep."""
    variants = {"janitor-95": 95, "sm-85": 85, "cm-lu-80": 80}
    best_cut = min(sweep, key=lambda t: (sweep[t]["cost"], -t))
    costs = {name: sweep[cut] for name, cut in variants.items()}
    winner = min(costs, key=lambda n: (costs[n]["cost"], -variants[n]))
    ordered = sorted(costs, key=lambda n: costs[n]["cost"])
    runner = ordered[1] if ordered[0] == winner else ordered[0]
    gap = costs[runner]["cost"] - costs[winner]["cost"]
    clear = gap >= STRONG_GAP and costs[winner]["cost"] <= STRONG_RATIO * costs[runner]["cost"]
    line = ", ".join(f"{cut}: {sweep[cut]['cost']} (FP {sweep[cut]['fp']}, "
                     f"FN {sweep[cut]['fn']})" for cut in sorted(sweep))
    reason = (f"Cut sweep of the final engine over every construction pair, "
              f"cost 2*FP+FN: {line}. Best cut {best_cut}; of the shipped "
              f"defaults `{winner}` is cheapest (runner-up `{runner}` by "
              f"{gap}). The cut is on the base engine's score (Janitor's 95 "
              f"is on its own confidence scale with structural bonuses), so "
              f"step 6 sets the one `[matching]` threshold on this sweep; the "
              f"row only rules out the cuts that cost most.")
    return {"variant": winner, "confidence": "strong" if clear else "weak",
            "reason": reason, "method": "threshold sweep",
            "evidence_pairs": sum(sweep[min(sweep)][k] for k in ("tp", "fp", "fn", "tn")),
            "sweep": {str(cut): dict(cost) for cut, cost in sorted(sweep.items())},
            "best_cut": best_cut, "strictest": "janitor-95"}


# ---------------------------------------------------------------------------
# alias + case decisions
# ---------------------------------------------------------------------------

def decide_aliases() -> dict:
    """Decide every disputed alias by oracle same(alias, claimant) votes.

    The four oracles are not independent voters (three run the shared core,
    and every vote is string similarity, not knowledge of rebrands such as
    WGN America -> NewsNation), so every decided alias is ``weak``: step 6
    keeps the decided claimant first and the AI rerun re-decides."""
    with open(os.path.join(MERGED, "alias_conflicts.json"),
              encoding="utf-8") as handle:
        conflicts = json.load(handle)
    oracles_by_tag = oracles.all_oracles()
    decisions: dict[str, dict] = {}
    for entry in sorted(conflicts["conflicts"], key=lambda e: e["alias"]):
        if entry["status"] != "disputed":
            continue
        alias = entry["alias"]
        claimants = sorted(entry["claimed_by"])
        votes: dict[str, dict[str, bool]] = {}
        for claimant in claimants:
            votes[claimant] = {}
            for tag in TAGS:
                try:
                    votes[claimant][tag] = bool(
                        oracles_by_tag[tag].same(alias, claimant))
                except Exception:  # noqa: BLE001
                    votes[claimant][tag] = None  # type: ignore[assignment]
        totals = {claimant: sum(1 for tag in TAGS if votes[claimant][tag])
                  for claimant in claimants}
        best = max(totals.values()) if totals else 0
        winners = [claimant for claimant, total in totals.items()
                   if total == best]
        if len(winners) == 1 and best >= 2:
            canonical, status = winners[0], "decided"
        else:
            canonical, status = None, "unresolved"
        decisions[alias] = {
            "canonical": canonical,
            "status": status,
            "confidence": "weak" if status == "decided" else "no-evidence",
            "votes": {claimant: {tag: votes[claimant][tag] for tag in TAGS}
                      for claimant in claimants},
            "claimed_by": entry["claimed_by"],
        }
    country = sorted(conflicts.get("country_conflicts", []),
                     key=lambda e: e["alias"] if isinstance(e, dict) else str(e))
    return {"aliases": decisions, "country_conflicts": country}


def decide_case_groups(pairs: list[dict]) -> dict:
    """Case-variant groups: fold (no canonical rewrite).

    Every normalizer lowercases before it compares, so case never reaches the
    matcher; the merge's verbatim key already keeps both spellings. Evidence:
    the corpus case:variant pairs, which should be unanimously same.
    """
    with open(os.path.join(MERGED, "PROVENANCE.json"),
              encoding="utf-8") as handle:
        provenance = json.load(handle)
    groups = []
    for country, info in sorted(provenance["channels"].items()):
        for group in info.get("case_variants", []):
            groups.append({"country": country,
                           "variants": sorted(group),
                           "decision": "fold"})
    case_pairs = [pair for pair in pairs
                  if pair["generator"] == "case:variant"]
    unanimous_same = sum(1 for pair in case_pairs
                         if pair["label_source"] == "unanimous"
                         and pair["label"] == "same")
    return {
        "decision": "fold",
        "groups": len(groups),
        "reason": ("case never reaches the matcher (every normalizer folds "
                   "it); both spellings stay in the DB, no rewrite."),
        "case_pairs": len(case_pairs),
        "unanimous_same": unanimous_same,
        "confidence": ("strong" if unanimous_same >= 20 else "weak"),
        "groups_sample": groups[:20],
    }


# ---------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------

def _load_pairs(corpus_dir: str) -> list[dict]:
    pairs = []
    with open(os.path.join(corpus_dir, "pairs.jsonl"),
              encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                pairs.append(json.loads(line))
    return pairs


def _engine_vs_plugins(ablation, config: dict, sweep: dict) -> dict:
    """The final engine against each plugin, on the same construction pairs."""
    t80 = ablate.THRESHOLDS[0]
    out = {"engine": dict(sweep[t80])}
    labels = [pair["label"] for pair in ablation.pairs]
    for tag in TAGS:
        verdicts = [bool(pair["verdicts"].get(tag)) for pair in ablation.pairs]
        out[tag] = ablate._cost(verdicts, labels)
    return out


def decide_all(corpus_dir: str) -> dict:
    """Decide every inventory row plus aliases and case groups."""
    pairs = _load_pairs(corpus_dir)
    with open(os.path.join(corpus_dir, "STATS.json"), encoding="utf-8") as handle:
        form_counts = json.load(handle).get("generator_forms", {})
    ablation = ablate.Ablation(pairs)
    run = ablation.run()
    config = run["config"]
    reports = ablate.row_reports(run, ablation.pairs, DERIVED)
    sweep = ablate.threshold_sweep(ablation, config)
    rows = {}
    for row in VARIANTS:
        if row in INACTIVE:
            strictest = VARIANTS[row][1]
            rows[row] = {"variant": strictest, "confidence": "no-evidence",
                         "method": "inactive", "evidence_pairs": 0,
                         "reason": (f"No pair can be evidence: {INACTIVE[row]} "
                                    f"Default `{strictest}` (strictest)."),
                         "strictest": strictest}
        elif row in ORCHESTRATED:
            variant, why = ORCHESTRATED[row]
            rows[row] = {"variant": variant, "confidence": "orchestrator",
                         "method": "orchestrated", "evidence_pairs": 0,
                         "reason": why, "strictest": VARIANTS[row][1]}
        elif row in BY_DESIGN:
            variant, why = BY_DESIGN[row]
            rows[row] = {"variant": variant, "confidence": "no-evidence",
                         "method": "by design", "evidence_pairs": 0,
                         "reason": f"By design: {why}", "strictest": variant}
        elif row in ABLATED or row in DERIVED:
            rows[row] = decide_ablated(row, reports, ablation.pairs, form_counts)
        elif row in ("T10", "D02"):
            rows[row] = decide_threshold(sweep)
        else:
            rows[row] = decide_row_whole(row, pairs)
        rows[row]["reason"] += NOTES.get(row, "")
    for row, decision in rows.items():
        if decision["variant"] not in VARIANTS[row][0]:
            raise RuntimeError(f"{row}: decided variant {decision['variant']!r} "
                               f"is not an inventory variant")
    alias_decisions = decide_aliases()
    case = decide_case_groups(pairs)
    counts = {level: sum(1 for d in rows.values() if d["confidence"] == level)
              for level in ("strong", "weak", "no-evidence", "orchestrator")}
    construction = sum(1 for pair in pairs if pair["label_source"] == "construction")
    coherence = check_coherence(rows)
    expectations = check_expectations(config, ablation.alias_map)
    return {
        "rows": rows,
        "coherence": coherence,
        "expectations": expectations,
        "engine": {"config": dict(sorted(config.items())),
                   "rounds": run["rounds"],
                   "vs_plugins": _engine_vs_plugins(ablation, config, sweep)},
        "aliases": alias_decisions["aliases"],
        "country_conflicts": alias_decisions["country_conflicts"],
        "case_groups": case,
        "meta": {"pairs": len(pairs), "construction_pairs": construction,
                 "rows_decided": counts},
    }


# ---------------------------------------------------------------------------
# outputs
# ---------------------------------------------------------------------------

def _cost_line(cost: dict) -> str:
    return (f"{cost['cost']} (TP {cost['tp']}, FP {cost['fp']}, "
            f"FN {cost['fn']}, TN {cost['tn']})")


def _metric_line(scores: dict) -> str:
    return (f"P {scores['precision']:.3f} / R {scores['recall']:.3f} / "
            f"F1 {scores['f1']:.3f} "
            f"(TP {scores['tp']}, FP {scores['fp']}x2, "
            f"FN {scores['fn']}, TN {scores['tn']}, n {scores['n']})")


def render_doc(decisions: dict) -> str:
    """Render docs/matching-decisions.md from the decisions object."""
    lines = []
    add = lines.append
    meta = decisions["meta"]
    counts = meta["rows_decided"]
    add("# Matching decisions: one engine, chosen on the corpus")
    add("")
    add("Plan step 5 of `plans/phase-3-lineup.md` (goal reset: best-of by a "
        "labelled corpus, labels automatic). Each of the "
        f"{len(decisions['rows'])} rows of "
        "`docs/matching-rules-inventory.md` (plus orchestrator-settled "
        "Appendix C rows) is one rule where the four "
        "matchers disagree; this table picks the unified engine's variant per "
        "row and records the evidence. Step 6 implements "
        "`data/corpus/decisions.json` (row id -> variant). Generated by "
        "`scripts/corpus/decide.py`; do not edit.")
    add("")
    add("## Method")
    add("")
    add("- **Labels.** `data/corpus/pairs.jsonl` (sizes in "
        "`data/corpus/STATS.json`). Construction truth: a synthetic provider "
        "spelling of canonical X is X when it keeps the feed, and a "
        "different channel when it does not (`X West`/`X Pacific`/`X (W)` and "
        "`X +1` are not `X`; `X East` is, the US convention SM and LU both "
        "encode); near-miss canonicals, real channels some normalizer folds "
        "together, other stations of the same network or market, and an "
        "alias of a near-miss are `different`; undisputed aliases of the "
        "merged table (human-curated) are `same` in both directions. Real "
        "pairs no generator labels get oracle labels: unanimous, else "
        "majority of four (ties `unknown`). The four oracles are not "
        "independent (JAN, CM and LU share the core; CM's override is "
        "sourced from LU), so majority labels are never used to decide a "
        "row by ablation; they only feed the whole-path rows, which are "
        "weak at best.")
    add("- **Generators are neutral.** Every synthetic form of a family (quality "
        "tags, prefixes, zones, `+1`, decoration, invisible characters) is "
        "included only if real names carry it (`STATS.json` "
        "`generator_forms`: counts over the merged DBs, lineups, aliases and "
        "every plugin test file) and then drawn uniformly, so each plugin's "
        "distinctive table entry is tested exactly as often as the shared "
        "ones. Strong rows need most of their deciding pairs on forms seen at "
        f"least {FORM_FLOOR} times for real.")
    add("- **Attribution by ablation.** Whole-path oracles differ in many rules "
        "at once, so a split between two plugins is not evidence for one "
        "rule. `scripts/corpus/ablate.py` builds one base engine (Lineuparr's "
        "`match_all_streams` over the shared core, in LU's isolated "
        "namespace), swaps ONE rule at a time to each plugin's own table, "
        "statement or function, and scores only the pairs where the swap "
        "changes the verdict, on construction labels only (unanimous and "
        "majority labels include the base's own vote). Interacting rows are "
        "searched jointly (zone words with the zone filter; prefix table, "
        "prefix order and geographic table; digit guards with designator "
        "repair; the whole callsign ladder; alias lookup with the "
        "alias-callsign rescue) and the configuration is settled by "
        "coordinate descent, so the decisions are one engine.")
    add(f"- **Cost** = 2*FP + FN on the discriminating pairs (a wrong "
        f"attach/assign/rename is worse than a miss). Ties and rows with "
        f"fewer than {ablate.MIN_EVIDENCE} discriminating pairs take the "
        f"strictest variant (unless those few pairs favour another by more "
        f"than noise: then `weak`); costs within {ablate.NOISE} of the cheapest "
        f"count as a tie. **strong**: >= {STRONG_PAIRS} discriminating "
        f"pairs, winner >= {round((1 - STRONG_RATIO) * 100)}% and >= {STRONG_GAP} "
        f"cheaper than the runner-up, the same winner at base cuts "
        f"{ablate.THRESHOLDS[0]} and {ablate.THRESHOLDS[1]}, and "
        f"evidenced forms; **weak** otherwise; **no-evidence** below "
        f"{ablate.MIN_EVIDENCE} pairs, for inactive mechanisms, and for rows "
        f"the plan fixes by design; **orchestrator** for conflicts the corpus "
        f"cannot settle (the plugins' own tests disagree, Appendix C), decided "
        f"by the orchestrating agent and recorded in `decide.py` "
        f"`ORCHESTRATED` with the reason.")
    add("- **Rows no ablation isolates** (whole decision paths: C06, D04-D06, "
        "D10, T02-T05, T08, T12, Z03, Z05) keep the whole-oracle "
        "comparison on substring-hinted pairs and are capped at `weak`.")
    add("")
    add(f"Decided {len(decisions['rows'])} rows: **{counts['strong']} strong / "
        f"{counts['weak']} weak / {counts['no-evidence']} no-evidence / "
        f"{counts['orchestrator']} orchestrator** over "
        f"{meta['pairs']} pairs ({meta['construction_pairs']} construction). "
        f"Coordinate descent converged in {decisions['engine']['rounds']} "
        f"rounds.")
    add("")
    add("## The engine against each plugin")
    add("")
    add(f"The decided configuration on the base engine (cut "
        f"{ablate.THRESHOLDS[0]}) and each plugin's own run path, on every "
        f"construction pair (cost 2*FP+FN):")
    add("")
    add("| matcher | cost |")
    add("|---------|------|")
    for name, cost in decisions["engine"]["vs_plugins"].items():
        add(f"| {name} | {_cost_line(cost)} |")
    add("")
    add("Engine configuration (ablation rows): " + ", ".join(
        f"`{row}={variant}`" for row, variant in decisions["engine"]["config"].items()) + ".")
    add("")
    add("## Coherence")
    add("")
    add("Combinations checked so the rows are one engine (`decide.py` fails "
        "instead of writing a table that breaks one):")
    add("")
    for item in decisions["coherence"]:
        add(f"- {'holds' if item['holds'] else 'VIOLATED'}: {item['statement']}")
    add("")
    add("## Summary")
    add("")
    add("| id | rule | chosen | method | evidence | confidence |")
    add("|----|------|--------|--------|----------|------------|")
    for row in sorted(decisions["rows"]):
        decision = decisions["rows"][row]
        add(f"| {row} | {VARIANTS[row][2]} | {decision['variant']} | "
            f"{decision['method']} | {decision['evidence_pairs']} | "
            f"{decision['confidence']} |")
    add("")
    for row in sorted(decisions["rows"]):
        decision = decisions["rows"][row]
        variants, _strictest, title = VARIANTS[row]
        add(f"## {row}: {title}")
        add("")
        add(f"Chosen: **{decision['variant']}** ({decision['confidence']}). "
            f"{decision['reason']}")
        add("")
        if "costs" in decision:
            t80, t90 = (str(t) for t in ablate.THRESHOLDS)
            add(f"| variant | cost at {t80} | cost at {t90} |")
            add("|---------|------------|------------|")
            for name in sorted(decision["costs"][t80]):
                mark = " <-- chosen" if name == decision["variant"] else ""
                at90 = decision["costs"][t90].get(name)
                add(f"| {name} | {_cost_line(decision['costs'][t80][name])}{mark} | "
                    f"{_cost_line(at90) if at90 else 'n/a'} |")
            add("")
            for other, shown in sorted(decision.get("examples", {}).items()):
                if not shown:
                    continue
                add(f"*Where `{decision['variant']}` and `{other}` differ:*")
                for item in shown:
                    verdict = "right" if item["winner_right"] else "wrong"
                    add(f"- `{item['query']}` vs `{item['candidate']}` "
                        f"(label {item['label']}, {item['generator']}; "
                        f"chosen is {verdict})")
            add("")
        elif "scores" in decision:
            add("| variant | oracles | whole-path metric |")
            add("|---------|---------|-------------------|")
            for name, members in variants.items():
                mark = " <-- chosen" if name == decision["variant"] else ""
                add(f"| {name} | {', '.join(members) or '-'} | "
                    f"{_metric_line(decision['scores'][name])}{mark} |")
            add("")
        elif "sweep" in decision:
            add("| cut | cost |")
            add("|-----|------|")
            for cut, cost in decision["sweep"].items():
                add(f"| {cut} | {_cost_line(cost)} |")
            add("")
    add("## Appendix A: disputed aliases")
    add("")
    add("Each `disputed` alias in `data/merged/alias_conflicts.json` by "
        "`same(alias, claimant)` votes across the four oracles (>= 2 votes "
        "and a unique top wins; else unresolved, every claimant kept). The "
        "votes are string similarity from matchers sharing one core, blind "
        "to rebrands (WGN America became NewsNation), so every decided alias "
        "is weak and the AI rerun re-decides them.")
    add("")
    for alias in sorted(decisions["aliases"]):
        entry = decisions["aliases"][alias]
        votes = ", ".join(
            f"{claimant}: {sum(1 for tag in TAGS if entry['votes'][claimant][tag])}/4"
            for claimant in sorted(entry["votes"]))
        add(f"- `{alias}` -> "
            f"{('`' + entry['canonical'] + '`') if entry['canonical'] else '*unresolved*'} "
            f"({entry['status']}, {entry['confidence']}; {votes})")
    add("")
    add("## Appendix B: case-variant groups")
    add("")
    case = decisions["case_groups"]
    add(f"Decision for all {case['groups']} groups: **{case['decision']}**. "
        f"{case['reason']} Corpus case:variant pairs: {case['case_pairs']} "
        f"({case['unanimous_same']} unanimously same; {case['confidence']}).")
    add("")
    add("## Appendix C: the plugins' own test expectations")
    add("")
    add("Regression assertions from the plugin suites, restated as pair "
        "verdicts and run through the decided engine and each plugin's run "
        "path (`ok` = as the test expects). A failure here is a known "
        "conflict between plugins' intents, not hidden by the corpus.")
    settled = [row for row in sorted(ORCHESTRATED) if row in decisions["rows"]]
    if settled:
        add("")
        add("Conflicts settled by an orchestrator decision (the engine's `FAIL` "
            "on those assertions is the decision, not a defect): "
            + "; ".join(f"{row} ({VARIANTS[row][2]}: `{decisions['rows'][row]['variant']}`)"
                        for row in settled) + ".")
    add("")
    add("| query | candidate | expected | engine | janitor | sm | cm | lu | source |")
    add("|-------|-----------|----------|--------|---------|----|----|----|--------|")
    for item in decisions["expectations"]:
        def mark(value):
            return "ok" if value == item["expected"] else "**FAIL**"
        add(f"| `{item['query']}` | `{item['candidate']}` | "
            f"{'same' if item['expected'] else 'different'} | {mark(item['engine'])} | "
            + " | ".join(mark(item[tag]) for tag in TAGS)
            + f" | {item['source']} |")
    add("")
    return "\n".join(lines)


def write_outputs(decisions: dict, decisions_path: str, doc_path: str) -> None:
    # decisions.json carries everything except rendered examples (doc-only).
    serializable = {
        "rows": {row: {key: value for key, value in decision.items()
                       if key != "examples"}
                 for row, decision in decisions["rows"].items()},
        "engine": decisions["engine"],
        "coherence": decisions["coherence"],
        "expectations": decisions["expectations"],
        "aliases": decisions["aliases"],
        "country_conflicts": decisions["country_conflicts"],
        "case_groups": {key: value for key, value in decisions["case_groups"].items()
                        if key != "groups_sample"},
        "meta": decisions["meta"],
    }
    with open(decisions_path, "w", encoding="utf-8") as handle:
        json.dump(serializable, handle, indent=2, sort_keys=True,
                  ensure_ascii=False)
        handle.write("\n")
    doc = render_doc(decisions)
    with open(doc_path, "w", encoding="utf-8") as handle:
        handle.write(doc if doc.endswith("\n") else doc + "\n")


def _check(corpus_dir: str, decisions_path: str, doc_path: str) -> int:
    with tempfile.TemporaryDirectory(prefix="decide-check-") as tmp:
        tmp_json = os.path.join(tmp, "decisions.json")
        tmp_doc = os.path.join(tmp, "matching-decisions.md")
        decisions = decide_all(corpus_dir)
        write_outputs(decisions, tmp_json, tmp_doc)
        for want, got, name in ((decisions_path, tmp_json, "decisions.json"),
                                (doc_path, tmp_doc, "matching-decisions.md")):
            if not os.path.exists(want):
                print(f"--check: {want} missing; run decide.py first "
                      f"(without --check).", file=sys.stderr)
                return 1
            with open(want, encoding="utf-8") as handle:
                old = handle.read()
            with open(got, encoding="utf-8") as handle:
                new = handle.read()
            if old != new:
                print(f"--check: {name} differs; rerun decide.py (without "
                      f"--check).", file=sys.stderr)
                return 1
    print(f"--check: {decisions_path} and {doc_path} reproduce byte for byte.")
    return 0


def _inventory_rows() -> list[str]:
    """Row ids parsed from the inventory doc (coverage gate)."""
    import re

    rows = []
    with open(os.path.join(REPO, "docs", "matching-rules-inventory.md"),
              encoding="utf-8") as handle:
        for line in handle:
            match = re.match(r"^\| ([A-Z]+\d+) \|", line)
            if match:
                rows.append(match.group(1))
    return rows


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--corpus", default=DEFAULT_CORPUS)
    parser.add_argument("--decisions", default=DEFAULT_DECISIONS)
    parser.add_argument("--doc", default=DEFAULT_DOC)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)

    missing = sorted(set(_inventory_rows()) - set(VARIANTS))
    if missing:
        print(f"decide.py covers no variant for inventory rows: {missing}",
              file=sys.stderr)
        return 2
    extra = sorted(set(VARIANTS) - set(_inventory_rows()))
    if extra:
        print(f"decide.py has rows not in the inventory: {extra}",
              file=sys.stderr)
        return 2

    if args.check:
        return _check(args.corpus, args.decisions, args.doc)
    decisions = decide_all(args.corpus)
    write_outputs(decisions, args.decisions, args.doc)
    meta = decisions["meta"]["rows_decided"]
    print(f"decided {len(decisions['rows'])} rows: "
          f"{meta['strong']} strong / {meta['weak']} weak / "
          f"{meta['no-evidence']} no-evidence / "
          f"{meta['orchestrator']} orchestrator; "
          f"{len(decisions['aliases'])} disputed aliases, "
          f"{decisions['case_groups']['groups']} case groups")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
