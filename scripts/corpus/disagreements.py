#!/usr/bin/env python3
"""Run all four oracles over a name list and report where they disagree.

Step 3 of plans/phase-3-lineup.md. Step 5 feeds this report to build the
disputed set of the labelled corpus: every pair/name on which the plugins'
OWN code disagrees is a candidate for labelling, grouped per inventory rule
(``docs/matching-rules-inventory.md``) where the cause is attributable.

Usage:
    python3 -I scripts/corpus/disagreements.py [--input names.txt] [--threshold N]
    python3 -I scripts/corpus/disagreements.py --self-test

``--self-test`` checks that the four plugins load side by side without sharing
modules (identity and mutation checks), that each oracle runs its plugin's
decision path (one known case per path), and a handful of normalizer, callsign,
alias and country cases. Each expectation cites the plugin source or test it
comes from. It runs in about a second and is wired into the CI parity job as a
hard gate.

``match-top1`` compares each plugin's first decision: Janitor's assigned EPG
entry (confidence >= 95), SM's first attached stream (quality order), CM's
rename target and LU's top-scored stream. ``--threshold`` overrides every
plugin's cut at once: Janitor's assignment cut, the others' matcher threshold.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.dont_write_bytecode = True

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import oracles  # noqa: E402

#: Built-in sample: names chosen so each exercises at least one inventory rule.
SAMPLE = [
    # Regional / zones (N02, Z01-Z04).
    "HBO East", "HBO West", "HBO (W)", "ESPN Pacific", "ESPN (Pacific)",
    "Comedy Central", "The Atlantic", "Fox Sports West", "BBC One West",
    "UK: BBC One West", "US: HBO West", "Sportsnet Pacific", "HBO",
    # Provider prefixes / geographic / prefix order (N04, N12, N14, G01).
    "US: USA Network HD", "USA Network", "US Open", "FR beIN SPORTS MAX",
    "UKSD: Sky Sports", "RK: beIN Sports Xtra", "TUBI: Something",
    "US Armenian TV", "GER TV", "(US) TNT", "UK - BBC One", "PRIME: FOX",
    "Fox News US Edition", "CA FR: TVA",
    # Quality / decoration / invisible chars (N01, N09, N13).
    "Discovery Channel 4K", "ESPN [FHD]", "TNT UHD RAW", "ESPN 1080p",
    "Volume 100 I", "Channel 4", "TNT HEVC", "ESPN \u2605", "BB\u200bC One",
    # Dots / CamelCase / & / suffixes / designators (N05-N08).
    "JusticeCentral.TV", "DangerTV", "MeTV", "U&Yesterday", "BBC Three",
    "ESPN 2", "HBO 2", "Sky Sports F1", "Sky Sports F 1", "TF1", "TF 1",
    "Sky Sports UHD 1", "Sky Sports News 13", "Sky Sports News 14", "West TV",
    # Callsigns (C01-C06, D04).
    "ABC (WABC)", "KGTV (ABC)", "KING 5", "WAVE 3", "WOOD TV8", "WHO 13",
    "KAGN (ABC)", "KILN (ABC)", "WABC-DT", "WWL", "(WAVE)", "WAVE (NBC)",
    # Aliases (A01-A03).
    "MSNBC", "msnbc", "MS Now", "Golf Channel", "CNN", "TeleMundo", "Telemundo HD",
    # Cinemax / misc / time-shift (N11, N03, T07).
    "Cinemax", "MoreMax", "ActionMax", "(PRIME) FOX News", "(CX) Channel",
    "ITV2 +1", "Nickelodeon+1", "Discovery+",
    # Country-tagged feeds (G01-G03).
    "NOW: Sky Sports", "USA Networks", "GBR Entertainment", "AR: beIN DRAMA HD",
]


def _rule_hints(name: str) -> list[str]:
    """Inventory rules a name plausibly exercises (substring heuristics)."""
    upper = (name or "").upper()
    hints: list[str] = []
    if any(tok in upper for tok in ("EAST", "WEST", "(W)", "(E)", "PACIFIC", "(PT)",
                                    "CENTRAL", "ATLANTIC", "MOUNTAIN")):
        hints += ["N02", "Z01", "Z04"]
    if any(tok in upper for tok in ("US:", "UK:", "FR ", "USA", "TUBI:", "RK:",
                                    "PLUTO", "(US)", "GER ", "ARMENIAN", "OPEN",
                                    "PRIME:", " - ", " US ", "CA FR")):
        hints += ["N04", "N12", "N14", "G01"]
    if any(tok in upper for tok in ("4K", "8K", "UHD", "FHD", "[HD]", "1080P",
                                    "720P", "HEVC", "HDR")):
        hints += ["N01"]
    if not (name or "").isascii():
        hints += ["N09", "N13"]
    if "MAX" in upper:
        hints += ["N11"]
    if any(tok in upper for tok in ("(CX)", "(PRIME)", "(A)")):
        hints += ["N03"]
    if "+1" in upper or "+ 1" in upper:
        hints += ["T07"]
    if any(tok in upper for tok in ("WABC", "KGTV", "KING 5", "WAVE", "WOOD", "WHO 13",
                                    "KAGN", "KILN", "WWL", "-DT")):
        hints += ["C01", "C02", "C03", "C04", "C06"]
    if any(tok in upper for tok in ("F1", "F 1", "TF1", "TF 1", "ESPN 2", "HBO 2",
                                    "NEWS 1", "UHD 1")):
        hints += ["N08", "T06", "T07"]
    if any(tok in upper for tok in ("MSNBC", "MS NOW", "TELEMUNDO")):
        hints += ["A01", "A02"]
    if any(tok in upper for tok in ("GOLF CHANNEL", " TV", "NETWORK")):
        hints += ["N07"]
    if ".TV" in upper or "METV" in upper or "&" in (name or "") \
            or "THREE" in upper or "DANGER" in upper or "JUSTICE" in upper:
        hints += ["N05", "N06"]
    if "NOW:" in upper or "GBR" in upper or "AR:" in upper:
        hints += ["G01", "G02", "G03"]
    return list(dict.fromkeys(hints)) or ["unattributed"]


def run_disagreements(names, threshold_override=None):
    """Run every oracle over ``names``; return a list of disagreement records."""
    oracles_by_tag = oracles.all_oracles()
    tags = list(oracles_by_tag)
    records = []

    norms = {}
    for tag, oracle in oracles_by_tag.items():
        for name in names:
            try:
                norms[(tag, name)] = oracle.normalize(name)
            except Exception as exc:  # noqa: BLE001
                norms[(tag, name)] = f"<error {type(exc).__name__}: {exc}>"

    for name in names:
        values = {tag: norms[(tag, name)] for tag in tags}
        if len(set(values.values())) > 1:
            records.append(
                {"rule": _rule_hints(name), "kind": "normalize",
                 "name": name, "values": values}
            )

    callsigns = {}
    for tag, oracle in oracles_by_tag.items():
        for name in names:
            try:
                callsigns[(tag, name)] = oracle.callsign(name)
            except Exception as exc:  # noqa: BLE001
                callsigns[(tag, name)] = f"<error {type(exc).__name__}: {exc}>"
    for name in names:
        values = {tag: callsigns[(tag, name)] for tag in tags}
        if len(set(repr(value) for value in values.values())) > 1:
            records.append(
                {"rule": _rule_hints(name), "kind": "callsign",
                 "name": name, "values": values}
            )

    for name in names:
        values = {}
        for tag, oracle in oracles_by_tag.items():
            try:
                values[tag] = oracle.alias(name)
            except Exception as exc:  # noqa: BLE001
                values[tag] = f"<error {type(exc).__name__}: {exc}>"
        if len(set(repr(value) for value in values.values())) > 1:
            records.append(
                {"rule": _rule_hints(name), "kind": "alias",
                 "name": name, "values": values}
            )

    for query in names:
        tops = {}
        for tag, oracle in oracles_by_tag.items():
            threshold = threshold_override
            if threshold is None:
                threshold = oracles.DEFAULT_THRESHOLDS[tag]
            try:
                ranked = oracle.match(
                    query, [c for c in names if c != query], threshold
                )
                tops[tag] = ranked[0] if ranked else None
            except Exception as exc:  # noqa: BLE001
                tops[tag] = f"<error {type(exc).__name__}: {exc}>"
        if len(set(repr(top) for top in tops.values())) > 1:
            records.append(
                {"rule": _rule_hints(query), "kind": "match-top1",
                 "name": query, "values": tops}
            )
    return records


def print_report(records) -> None:
    """Print disagreement records grouped by inventory rule."""
    by_rule: dict[str, list] = {}
    for record in records:
        for rule in record["rule"]:
            by_rule.setdefault(rule, []).append(record)
    print(f"{len(records)} disagreements over the name list")
    for rule in sorted(by_rule):
        group = by_rule[rule]
        print(f"\n## {rule}: {len(group)}")
        for record in group:
            print(f"  [{record['kind']}] {record['name']!r}")
            for tag, value in record["values"].items():
                print(f"    {tag:8s} {value!r}")


def self_test() -> int:
    """Check each oracle loads and answers known cases. Returns exit status."""
    failures = []

    def check(label, got, want):
        status = "ok" if got == want else "FAIL"
        print(f"  [{status}] {label}: got {got!r}, want {want!r}")
        if got != want:
            failures.append(label)

    def check_in(label, needle, haystack):
        status = "ok" if needle in haystack else "FAIL"
        print(f"  [{status}] {label}: {needle!r} in {haystack!r}")
        if needle not in haystack:
            failures.append(label)

    try:
        all_oracles = oracles.all_oracles()
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: loading oracles: {type(exc).__name__}: {exc}")
        return 1
    print(f"loaded oracles: {sorted(all_oracles)}")
    janitor, sm, cm, lu = (all_oracles[tag] for tag in ("janitor", "sm", "cm", "lu"))

    print("isolation:")
    # Four copies of matching_core/aliases/fuzzy_matcher (and two plugin.py)
    # load side by side: each plugin's classes must come from its own files.
    shared = ("matching_core", "aliases", "fuzzy_matcher")
    tags = list(all_oracles)
    for i, left in enumerate(tags):
        for right in tags[i + 1:]:
            for name in shared:
                check(f"{left}/{right} distinct {name}",
                      sys.modules[f"oracle_{left}.{name}"] is sys.modules[f"oracle_{right}.{name}"],
                      False)
    for tag, oracle in all_oracles.items():
        core = sys.modules[f"oracle_{tag}.matching_core"]
        check(f"{tag} matcher built on its own core",
              issubclass(type(oracle.matcher), core.FuzzyMatcherCore)
              and oracle.fm_mod.FuzzyMatcherCore is core.FuzzyMatcherCore, True)
        # Oracles run the pure-Python Levenshtein fallback (inventory R01).
        check(f"{tag} core without rapidfuzz", core._USE_RAPIDFUZZ, False)
    for bare in ("matching_core", "aliases", "fuzzy_matcher", "country", "plugin"):
        check(f"no bare {bare!r} module left behind", bare in sys.modules, False)
    check("sm plugin.py uses its own matcher and country module",
          (sys.modules["oracle_sm.plugin"].FuzzyMatcher is sys.modules["oracle_sm.fuzzy_matcher"].FuzzyMatcher
           and sys.modules["oracle_sm.plugin"].country_detect is sys.modules["oracle_sm.country"]), True)
    check("janitor plugin.py uses its own matcher and aliases",
          (sys.modules["oracle_janitor.plugin"].FuzzyMatcher is sys.modules["oracle_janitor.fuzzy_matcher"].FuzzyMatcher
           and sys.modules["oracle_janitor.plugin"].CHANNEL_ALIASES
           is sys.modules["oracle_janitor.aliases"].CHANNEL_ALIASES), True)
    # Mutation: deny-list a real callsign in Janitor's core only. Janitor stops
    # extracting it at end-of-name; the other three copies are untouched.
    janitor_core = sys.modules["oracle_janitor.matching_core"].FuzzyMatcherCore
    saved_deny = janitor_core._CALLSIGN_DENYLIST
    try:
        janitor_core._CALLSIGN_DENYLIST = saved_deny | {"WFAA"}
        for oracle in all_oracles.values():
            oracle.matcher._callsign_cache.clear()
        got = {tag: oracle.callsign("Dallas WFAA")[0] for tag, oracle in all_oracles.items()}
        check("mutating janitor's core changes janitor only",
              got, {"janitor": None, "sm": "WFAA", "cm": "WFAA", "lu": "WFAA"})
    finally:
        janitor_core._CALLSIGN_DENYLIST = saved_deny
        for oracle in all_oracles.values():
            oracle.matcher._callsign_cache.clear()
    check("janitor restored", janitor.callsign("Dallas WFAA")[0], "WFAA")
    lu_aliases = sys.modules["oracle_lu.aliases"].CHANNEL_ALIASES
    lu_aliases["__oracle_probe__"] = ["probe"]
    try:
        check("mutating lu's alias table changes lu only",
              [("__oracle_probe__" in sys.modules[f"oracle_{tag}.aliases"].CHANNEL_ALIASES)
               for tag in tags], [tag == "lu" for tag in tags])
    finally:
        del lu_aliases["__oracle_probe__"]

    print("normalize:")
    # Parity-sweep adversarial shape: a country prefix plus quality tags must
    # clean to the bare channel on every oracle.
    for tag, oracle in all_oracles.items():
        check(f"{tag} strips prefix+quality", oracle.normalize("US: HBO HD"), "HBO")
    # SM tests/test_fuzzy_matcher.py:89-92 (bug-066): bare "Central" is a brand word.
    check("sm keeps CENTRAL", "CENTRAL" in sm.normalize("Comedy Central").upper(), True)
    # Janitor's REGIONAL_PATTERNS strip bare Central (fuzzy_matcher.py:101-111).
    check("janitor strips CENTRAL", "CENTRAL" in janitor.normalize("Comedy Central").upper(), False)
    # Lineuparr opts into bare-region stripping (fuzzy_matcher.py:351;
    # tests/test_sportsnet_pacific.py scores "Sportsnet Pacific" on it).
    check("lu strips PACIFIC", "PACIFIC" in lu.normalize("ESPN Pacific").upper(), False)
    # CM keeps bare zone words (fuzzy_matcher.py:111-130, parenthesized only)
    # and promotes (W) to a word (:842-843).
    check("cm keeps WEST", cm.normalize("HBO (W)"), "HBO West")
    # Prefix strip before (core, matching_core.py:352) vs after (JAN :658,
    # CM :854) hyphen normalization: "UK - " survives on JAN/CM (row N15).
    check("UK - prefix", {tag: o.normalize("UK - BBC One") for tag, o in all_oracles.items()},
          {"janitor": "UK BBC 1", "sm": "BBC 1", "cm": "UK BBC 1", "lu": "BBC 1"})

    print("callsign:")
    # Janitor's five-priority ladder promotes a leading callsign before a
    # parenthesized network only for a KNOWN station: tests/test_station_callsigns.py
    # :98-106 (KAGN) and :109-115 (KILN stays low confidence).
    check("janitor KAGN high-confidence", janitor.callsign("KAGN (ABC)"), ("KAGN", True))
    check("janitor KILN low-confidence", janitor.callsign("KILN (ABC)")[1], False)
    # Core OTA-number rescue (matching_core.py:818-830) needs a known-callsign
    # set: SM supplies one (fuzzy_matcher.py:323; tests/test_ota_callsign_fallback.py
    # :139). LU has no channel DB, so its set stays None (fuzzy_matcher.py:353-357)
    # and CM's own ladder has no rescue at loose positions (:640-654).
    check("KING 5 rescue", {tag: o.callsign("KING 5")[0] for tag, o in all_oracles.items()},
          {"janitor": None, "sm": "KING", "cm": None, "lu": None})

    print("alias:")
    # SM tests/test_alias_key_case.py:39-56: exact-then-folded key lookup.
    check("sm folds msnbc", sm.alias("msnbc") == sm.alias("MSNBC") != [], True)
    # Janitor strips the lookup key (fuzzy_matcher.py:1079-1082).
    check("janitor strips lookup key", janitor.alias("MSNBC ") == janitor.alias("MSNBC"), True)
    # CM reverse index round-trip over its own shipped table (:684-706).
    cm_keys = [key for key in cm.alias_map if cm.alias_map[key]]
    if cm_keys:
        key = cm_keys[0]
        check_in("cm reverse index round-trip", key, cm.alias(cm.alias_map[key][0]))
    # LU verbatim lookup (fuzzy_matcher.py:460).
    lu_keys = [key for key in lu.alias_map if lu.alias_map[key]]
    if lu_keys:
        key = lu_keys[0]
        check("lu verbatim alias", lu.alias(key), lu.alias_map[key])

    print("match (decision paths):")
    # SM tests/test_designator_tokens.py:92-101: "Sky Sports F1 is not Sky Sports 1."
    got = sm.matcher.find_best_match("Sky Sports F1 UHD", ["Sky Sports UHD 1"])
    check("sm F1 != UHD 1", (got[0] if got else None), None)
    got = sm.matcher.find_best_match("Sky Sports F1 UHD", ["Sky Sports F1 HD"])
    check("sm F1 == F1 HD", (got[0] if got else None), "Sky Sports F1 HD")
    # Every oracle pairs a name with itself.
    for tag, oracle in all_oracles.items():
        check(f"{tag} same(CNN, CNN)", oracle.same("CNN", "CNN"), True)
    # SM: fuzzy_match's token-sort says 100, but the collector's family rule
    # compares the cleaned names UNSORTED (plugin.py:6010-6139), so the stream
    # is not attached. The oracle is the collector, not fuzzy_match.
    check("sm fuzzy_match alone pairs Sky News/News Sky",
          sm.matcher.fuzzy_match("Sky News", ["News Sky"])[0], "News Sky")
    check("sm collector does not attach News Sky", sm.same("Sky News", "News Sky"), False)
    # SM zone step (plugin.py:4233-4262, 4300-4316): an unmarked channel is
    # zone-routed and drops West feeds; a lone West channel is not routed, so
    # it keeps the East feed.
    check("sm unmarked channel drops West",
          [c for c, _, _ in sm.match("HBO", ["HBO East", "HBO West"])], ["HBO East"])
    check("sm lone West channel keeps East", sm.same("HBO West", "HBO East"), True)
    # SM OTA path (plugin.py:5939-5967): a resolved callsign attaches every
    # stream carrying it, whatever the rest of the name says.
    check("sm callsign path",
          sm.match("ABC - NY New York (WABC)", ["WABC"])[0][2], "Callsign match")
    # Janitor's zoned filter keeps opposite zones apart
    # (fuzzy_matcher.py:1418-1428; tests/test_fuzzy_matcher.py:640-648) ...
    check("janitor zoned filter", janitor.same("HBO West", "HBO East"), False)
    # ... but its run path passes "regional" as an ignore tag (plugin.py:1127-1135),
    # which switches the regionless West filter off
    # (tests/test_fuzzy_matcher.py:693-704), so HBO may take the West guide.
    check("janitor regionless filter off on run path", janitor.same("HBO", "HBO West"), True)
    # Janitor assigns at the automatch confidence threshold (95, plugin.py:220);
    # a structural-only State + City score of 50 is found but not assigned.
    check("janitor structural score below the cut",
          janitor.match("USA Network", ["USA Networks"], 0)[0][1:], (50, "State + City"))
    check("janitor does not assign below 95", janitor.same("USA Network", "USA Networks"), False)
    # CM: any extracted callsign sends the name down the OTA path and it never
    # reaches premium matching (plugin.py:1979-2007).
    check("cm callsign blocks premium match", cm.same("ABC (WABC)", "ABC"), False)
    # CM's token-index pre-filter (fuzzy_matcher.py:268-350) drops a candidate
    # sharing no >=2-char token, even one fuzzy_match itself would accept.
    check("cm fuzzy_match alone pairs TeleMundo",
          cm.matcher.fuzzy_match("TeleMundo", ["Telemundo HD"], cm.ignored_tags)[0], "Telemundo HD")
    check("cm token pre-filter rejects TeleMundo", cm.same("TeleMundo", "Telemundo HD"), False)
    # LU attaches every match_all_streams result; the matcher runs AT the
    # threshold, so the 85 issue-#50 pair attaches at 80 and not at 90.
    check("lu attaches F1/UHD 1 at 80", lu.same("Sky Sports F1 UHD", "Sky Sports UHD 1"), True)
    check("lu rejects F1/UHD 1 at 90", lu.same("Sky Sports F1 UHD", "Sky Sports UHD 1", 90), False)

    print("country:")
    # SM country.py vs LU detect_stream_country vocabularies (AR divergence):
    # SM tests/test_country_detection.py:21; LU tests/test_country_filter.py:419.
    sm_country = sys.modules["oracle_sm.country"]
    lu_fm = sys.modules["oracle_lu.fuzzy_matcher"]
    check("sm country AR", sm_country.country_from_name("AR: beIN DRAMA HD"), "AR")
    check("lu stream country AR", lu_fm.detect_stream_country("AR: beIN DRAMA HD"), None)
    check("lu stream country US", lu_fm.detect_stream_country("US: ESPN"), "US")

    if failures:
        print(f"\nSELF-TEST FAILED: {len(failures)} case(s): {failures}")
        return 1
    print("\nSELF-TEST PASSED")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=None, help="file with one name per line")
    parser.add_argument(
        "--threshold", type=int, default=None,
        help="decision cut for match-top1 (default: each plugin's own; see oracles.DEFAULT_THRESHOLDS)",
    )
    parser.add_argument("--self-test", action="store_true", help="run the self-test")
    args = parser.parse_args()

    if args.self_test:
        return self_test()

    if args.input:
        with open(args.input, encoding="utf-8") as handle:
            names = [line.strip() for line in handle if line.strip()]
    else:
        names = SAMPLE
    print(f"{len(names)} names x 4 oracles", file=sys.stderr)
    print_report(run_disagreements(names, args.threshold))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
