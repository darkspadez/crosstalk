#!/usr/bin/env python3
"""Parity: crosstalk's three read-only tools against Stream-Mapparr's own code.

The streams app ships `check_countries`, `scan_placeholders` and `test_rules` as read-only
tools, ported from three Stream-Mapparr actions. This script is how that claim is checked: it
imports the plugin from `reference/` with the Django stubs its own test suite installs (through
`scripts/match_parity.py`, which already does that), imports `placeholder_scan.py` directly
because that module is plain Python with no Django, and compares the answers over generated
fixtures with the Rust side (`cargo run -p crosstalk-streams --example tools_io`).

Nothing of the Rust is mirrored here: the plugin's own code decides the expected answer.

What is driven:

  * `check_stream_countries_action` (plugin.py:2608), end to end, with its `_get_all_streams`
    and `_readout_dir` stubbed to the fixture: the message it renders, and per stream the real
    `_country_label_disagreement` classmethod.
  * `scan_placeholder_names_action` (2703) over `placeholder_scan.scan_families` /
    `render_report`: the family list, each suggested pattern, and the action's headline lines.
    Every suggested pattern is also compiled here with Python's `re` — a suggestion has to
    compile in the engine the setting feeds as well as in Rust's.
  * `_resolve_stream_regex_rules` plus `_apply_regex_rules_to_streams`: the per-rule validation
    report and the changed names, which is what `test_rules` (6575) reports.

Usage:
    python3 -I scripts/tools_parity.py [--show N]

Exit status is 1 when any answer differs, so it can gate CI.
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import re
import subprocess
import sys
import tempfile
import types
from pathlib import Path

sys.dont_write_bytecode = True

REPO = Path(__file__).resolve().parent.parent
PLUGIN_INNER = REPO / "reference" / "Stream-Mapparr" / "Stream-Mapparr"
BINARY = REPO / "target" / "debug" / "examples" / "tools_io"


class _Logger:
    def _record(self, msg, *args, **kwargs):
        pass

    info = debug = warning = error = _record


def load_plugin():
    """The plugin and its matcher, loaded the way `match_parity.py` loads them."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import match_parity  # noqa: E402  (installs the Django stubs when it loads the plugin)

    plugin, matcher = match_parity.load_plugin()
    # `placeholder_scan.py` is pure stdlib: import it directly, as its own tests do.
    if str(PLUGIN_INNER) not in sys.path:
        sys.path.insert(0, str(PLUGIN_INNER))
    import placeholder_scan  # noqa: E402

    return plugin, matcher, placeholder_scan


# ---------------------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------------------

GROUP_LABELS = [
    "UK| NEWS", "US: SPORT", "us| kids", "NEWS", "No Group", "DE: FILM", "FR - DOCS",
    " CA: SPORTS ", "UK", "US",
]

TVG_IDS = [
    "skynews.uk", "espn.us", "aljazeera.qa", "bbc.co.uk", "12345", "", "channel.info",
    "SKYNEWS.UK", "x.de", "x..us", "one.us", "x.usa",
]

PLACEHOLDER_NAMES = [
    "PPV EVENT 04", "PPV EVENT 05", "PPV EVENT 12", "PPV EVENT 4", "PPV EVENT 04 HD",
    "MAX 100", "MAX 101", "MAX 102", "MAX 103", "MAX 1", "MAX 2", "MAX 100 4K", "Movie 8k",
    "US: CINEMANIA 4K", "Triller TV | Event 07", "Triller TV | Event 12",
    "LIVE EVENT 04 - 11am", "LIVE EVENT 05 - 11pm", "HBO 1", "HBO 2", "HBO 3",
    "UK: BBC RED BUTTON 1", "UK: BBC RED BUTTON 2", "KARAOKE 1", "KARAOKE 2",
    "Event #7", "Event #12", "Event 7", "X264K", "X264L", "X264M",
    "Caf\u00e9 M\u00fcller 1", "Caf\u00e9 M\u00fcller 2", "Caf\u00e9 M\u00fcller 3",
    "\u25ce Box 1", "\u25ce Box 2", "\u25ce Box 3", "CNN", "", "  ",
]

RULE_SETTINGS = [
    "",
    r'[["\\s*\u258e\\s*", " "]]',
    r'[["^UK.*", ""]]',
    r'[["(unclosed", "x"]]',
    r'[["^PPV EVENT (\\d+)$", "PPV \\1"]]',
    r'[["\\\\s*\\\\|\\\\s*Event\\\\s*", " Event "], ["\\\\bHD\\\\b", ""]]',
    r'[["a", "b"], ["b", "c"]]',
    r'[[\"x\", \"' + ("x" * 60) + r'\"]]',
    "not json at all",
    # A rule that strips a zero-width space: the samples must show the escape, not a blank.
    r'[["\u200b", ""]]',
]


def country_cases(rng, count=24):
    cases = []
    for index in range(count):
        streams = []
        for stream_index in range(rng.randint(0, 12)):
            streams.append({
                "id": stream_index + 1,
                "name": rng.choice(["Sky News", "CNN", "Al Jazeera", "=formula", "BBC One"])
                        + ("" if stream_index == 0 else f" {stream_index}"),
                "group": rng.choice([None, 100, 101, 102]),
                "tvg_id": rng.choice(TVG_IDS),
            })
        groups = [
            {"id": 100, "name": rng.choice(GROUP_LABELS)},
            {"id": 101, "name": rng.choice(GROUP_LABELS)},
            {"id": 102, "name": rng.choice(GROUP_LABELS)},
        ]
        cases.append({"tool": "check_countries", "streams": streams, "groups": groups,
                      "comment": f"countries #{index}"})
    # One case with nothing comparable at all, so the "nothing to compare" wording is checked.
    cases.append({"tool": "check_countries", "streams": [], "groups": [],
                  "comment": "countries, no streams"})
    return cases


def scan_cases(rng, count=12):
    cases = []
    # The full vocabulary once, then randomised subsets, then the pattern-coverage matrix.
    for index in range(count):
        names = PLACEHOLDER_NAMES if index == 0 else rng.sample(
            PLACEHOLDER_NAMES, rng.randint(1, len(PLACEHOLDER_NAMES)))
        streams = [
            {"name": name, "tvg_id": rng.choice(["", "slot.us"])}
            for name in names
        ]
        patterns = rng.choice([
            "",
            r"^PPV EVENT \d+$",
            r"^MAX \d+$",
            r"^PPV EVENT \d+$" + "\n" + r"^MAX \d+$",
            "MAX",
        ])
        cases.append({
            "tool": "scan_placeholders",
            "streams": streams,
            "settings": {
                "epg_placeholder_name_patterns": patterns,
                "epg_placeholder_matching_enabled": rng.random() < 0.5,
            },
            "comment": f"scan #{index}",
        })
    return cases


def rule_cases(rng, count=18):
    cases = []
    names = [
        "ESPN \u258e HD", "UK BBC 1", "PPV EVENT 04", "Triller TV | Event 07",
        "Sky Sports 1 HD", "CNN", "x", "", "Caf\u00e9 M\u00fcller", "A" * 600,
        "a" * 200, "ESPN VIP HD", "=formula", "US: Starz Encore (W)",
        # Invisible format characters from several Unicode blocks: the samples
        # must escape every C* category, not just the zero-width space and controls.
        "ESPN\u200bHD", "X\u070fY", "X\u180bY",
    ]
    for index in range(count):
        settings = RULE_SETTINGS[index % len(RULE_SETTINGS)]
        streams = [
            {"id": position + 1, "name": rng.choice(names)}
            for position in range(rng.randint(0, 10))
        ]
        cases.append({
            "tool": "test_rules",
            "streams": streams,
            "settings": {"stream_name_regex_rules": settings},
            "comment": f"rules #{index}",
        })
    return cases


def template_cases():
    return [{
        "tool": "template_of",
        "streams": [{"name": name} for name in PLACEHOLDER_NAMES],
        "comment": "template splitter",
    }, {
        "tool": "scan_with_cap",
        "streams": [{"name": name, "tvg_id": ""} for name in PLACEHOLDER_NAMES],
        "max_name_len": 10,
        "comment": "scan with the name cap",
    }]


# ---------------------------------------------------------------------------------------
# The plugin's own answers
# ---------------------------------------------------------------------------------------

def country_answer(plugin, matcher, case, workdir):
    """`check_stream_countries_action` end to end, plus the real classmethod per stream."""
    P = plugin.Plugin
    group_names = {group["id"]: group["name"] for group in case.get("groups", [])}
    streams = [{
        "name": stream["name"],
        "channel_group__name": group_names.get(stream.get("group")),
        "tvg_id": stream["tvg_id"],
    } for stream in case["streams"]]

    inst = P.__new__(P)
    inst.version = "parity"
    inst.fuzzy_matcher = matcher
    inst._get_all_streams = lambda logger: streams
    inst._readout_dir = lambda *args, **kwargs: str(workdir)
    answer = inst.check_stream_countries_action({}, _Logger())
    assert answer.get("status") == "success", answer

    rows = {}
    for stream in streams:
        rows[stream["name"], stream["channel_group__name"] or ""] = list(
            P._country_label_disagreement(stream))
    return {"message": answer["message"], "rows": rows}


def scan_answer(plugin, matcher, scan, case, workdir):
    """`scan_placeholder_names_action` end to end, plus the module's own data structures."""
    P = plugin.Plugin
    settings = case.get("settings") or {}
    streams = [{"name": stream["name"], "tvg_id": stream["tvg_id"]}
               for stream in case["streams"]]
    inst = P.__new__(P)
    inst.version = "parity"
    inst.fuzzy_matcher = matcher
    inst._get_all_streams = lambda logger: streams
    inst._readout_dir = lambda *args, **kwargs: str(workdir)
    answer = inst.scan_placeholder_names_action(dict(settings), _Logger())
    assert answer.get("status") == "success", answer

    patterns = inst._resolve_epg_placeholder_patterns(settings)
    families = scan.scan_families(
        streams, patterns, max_name_len=plugin.PluginConfig.REGEX_NAME_MAX_LEN)
    return {"message": answer["message"], "families": families,
            "patterns": len(patterns)}


def rules_answer(plugin, matcher, case):
    """`_resolve_stream_regex_rules` plus `_apply_regex_rules_to_streams`, and the
    action's own fitted summary on the success path (the message the operator reads)."""
    P = plugin.Plugin
    settings = case.get("settings") or {}
    inst = P.__new__(P)
    inst.fuzzy_matcher = matcher
    rules, report = inst._resolve_stream_regex_rules(settings)
    streams = [{"name": stream["name"], "id": stream["id"]} for stream in case["streams"]]
    counters = plugin._apply_regex_rules_to_streams(streams, rules, _Logger())
    changed = {
        stream["id"]: stream["match_name"]
        for stream in streams if stream.get("match_name") != stream["name"]
    }
    # The samples exactly as the action builds them: input order, first twenty, with the
    # plugin's own invisibles escape.
    samples = []
    for stream in streams:
        if stream.get("match_name") == stream["name"]:
            continue
        before = plugin._escape_invisibles((stream.get("name") or "")[:80])
        after = stream["match_name"][:80]
        after = ("(empty — stream becomes unmatchable)"
                 if not after.strip() else plugin._escape_invisibles(after))
        samples.append(f"{before} -> {after}")
    samples = samples[:plugin.PluginConfig.REGEX_TEST_SAMPLES]
    # The fitted summary, driven end to end so the wording (including the `1,234`
    # thousands separators) is the plugin's own, not a reimplementation of it.
    action_streams = [{"name": stream["name"], "id": stream["id"]}
                      for stream in case["streams"]]
    action_inst = P.__new__(P)
    action_inst.fuzzy_matcher = matcher
    action_inst._get_all_streams = lambda logger: action_streams
    action_answer = action_inst.test_regex_rules_action(dict(settings), _Logger())
    return {
        "report": [{"index": entry["index"], "status": entry["status"]} for entry in report],
        "rules": len(rules),
        "changed": changed,
        "samples": samples,
        "action_status": action_answer.get("status"),
        "action_message": action_answer.get("message"),
        "counters": {key: counters[key] for key in
                     ("changed", "skipped_long", "growth_reverted", "emptied", "budget_tripped")},
    }


def template_answer(scan, case):
    templates = {}
    for stream in case["streams"]:
        template, numbers = scan.template_of(stream["name"])
        templates[stream["name"]] = {
            "template": template,
            "numbers": list(numbers),
            "suggested": scan.suggested_pattern(template),
        }
    return templates


def scan_capped_answer(plugin, scan, case):
    """The scan's containment: a name over the cap is skipped and counted, not pattern-matched."""
    inst = plugin.Plugin.__new__(plugin.Plugin)
    settings = case.get("settings") or {}
    raw = settings.get("epg_placeholder_name_patterns")
    if raw is None:
        raw = plugin.PluginConfig.DEFAULT_EPG_PLACEHOLDER_NAME_PATTERNS
    patterns = inst._resolve_epg_placeholder_patterns(
        {"epg_placeholder_name_patterns": raw})
    streams = [{"name": stream["name"], "tvg_id": stream["tvg_id"]}
               for stream in case["streams"]]
    stats = {}
    families = scan.scan_families(streams, patterns, max_name_len=case["max_name_len"],
                                  stats=stats)
    return {"families": families, "stats": stats}


# ---------------------------------------------------------------------------------------
# Driving the Rust side
# ---------------------------------------------------------------------------------------

def run_rust(cases):
    payload = "\n".join(json.dumps(case) for case in cases)
    completed = subprocess.run([str(BINARY)], input=payload, capture_output=True, text=True)
    if completed.returncode != 0:
        raise SystemExit(
            f"the Rust example failed ({completed.returncode}):\n{completed.stderr[-4000:]}")
    answers = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    if len(answers) != len(cases):
        raise SystemExit(f"Rust produced {len(answers)} answers for {len(cases)} cases")
    return answers


FAMILY_KEYS = ("template", "count", "distinct_numbers", "example", "with_epg_id",
               "covered", "uncovered", "suggested")


def compare_scan(mine, theirs, mismatch, show, label):
    want = [{key: family[key] for key in FAMILY_KEYS} for family in theirs]
    got = [{key: family[key] for key in FAMILY_KEYS} for family in mine]
    if want != got:
        mismatch(f"[{label}] family lists differ")
        for left, right in zip(want, got):
            if left != right:
                print(f"    python={left}\n    rust  ={right}")
        if len(want) != len(got):
            print(f"    python has {len(want)} famil(ies), rust {len(got)}")
        return 1
    # Every suggested pattern has to compile in BOTH engines: Python's `re` here, Rust's
    # `regex` in the crate's own tests.
    for family in got:
        try:
            re.compile(family["suggested"], re.IGNORECASE)
        except re.error as error:  # pragma: no cover - a port bug
            mismatch(f"[{label}] suggested pattern does not compile in Python: "
                     f"{family['suggested']!r}: {error}")
            return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--show", type=int, default=8, help="mismatches to print")
    parser.add_argument("--seed", type=int, default=20261103, help="seed for the fixtures")
    args = parser.parse_args()

    if not PLUGIN_INNER.is_dir():
        print(f"reference plugin not found at {PLUGIN_INNER}; run `git submodule update --init`",
              file=sys.stderr)
        return 2

    print(f"building {BINARY} ...", file=sys.stderr)
    subprocess.run(["cargo", "build", "-q", "-p", "crosstalk-streams", "--example", "tools_io"],
                   cwd=REPO, check=True)

    plugin, matcher, scan = load_plugin()
    logging.disable(logging.CRITICAL)
    rng = random.Random(args.seed)
    cases = country_cases(rng) + scan_cases(rng) + rule_cases(rng) + template_cases()
    answers = run_rust(cases)

    mismatches = 0
    compared = 0

    def mismatch(message):
        nonlocal mismatches
        mismatches += 1
        if mismatches <= args.show:
            print(message)

    workdir = Path(tempfile.mkdtemp(prefix="crosstalk-tools-parity-"))
    for case, answer in zip(cases, answers):
        label = case["comment"]
        tool = case["tool"]
        if tool == "check_countries":
            expected = country_answer(plugin, matcher, case, workdir)
            compared += 1
            if expected["message"] != answer["message"]:
                mismatch(f"[{label}] message differs\n"
                         f"    python={expected['message']!r}\n    rust  ={answer['message']!r}")
            mine = {(row["name"], row["group"]): [row["group_code"], row["epg_code"]]
                    for row in answer["rows"]}
            if expected["rows"] != mine:
                mismatch(f"[{label}] per-stream country signals differ")
                for key in sorted(set(expected["rows"]) | set(mine), key=str):
                    if expected["rows"].get(key) != mine.get(key):
                        print(f"    {key}: python={expected['rows'].get(key)} "
                              f"rust={mine.get(key)}")
            if expected["message"].count("of") and (
                    f"{answer['agree']} of {answer['both']}" not in expected["message"]):
                mismatch(f"[{label}] counters do not match the plugin's own message")

        elif tool == "scan_placeholders":
            expected = scan_answer(plugin, matcher, scan, case, workdir)
            compared += 1
            mismatches += compare_scan(answer["families"], expected["families"], mismatch,
                                       args.show, label)
            # The headline, line for line, as long as the plugin's toast did not truncate it
            # (`_fit_toast` drops whole lines from the end past the budget).
            if len(expected["message"]) < plugin.PluginConfig.TOAST_BUDGET_CHARS:
                want_lines = expected["message"].split("\n")
                if want_lines != answer["headline"]:
                    mismatch(f"[{label}] headline differs\n"
                             f"    python={want_lines}\n    rust  ={answer['headline']}")
            if answer["patterns"] != expected["patterns"]:
                mismatch(f"[{label}] pattern count differs: python={expected['patterns']} "
                         f"rust={answer['patterns']}")

        elif tool == "test_rules":
            expected = rules_answer(plugin, matcher, case)
            compared += 1
            got_report = [{"index": entry["index"], "status": entry["status"]}
                          for entry in answer["report"]]
            if expected["report"] != got_report:
                mismatch(f"[{label}] rule report differs\n"
                         f"    python={expected['report']}\n    rust  ={got_report}")
            if expected["rules"] != answer["rules"]:
                mismatch(f"[{label}] usable rule count differs: python={expected['rules']} "
                         f"rust={answer['rules']}")
            got_changed = {row["id"]: row["after"] for row in answer["changed"]}
            if expected["changed"] != got_changed:
                mismatch(f"[{label}] changed names differ")
                for key in sorted(set(expected["changed"]) | set(got_changed)):
                    if expected["changed"].get(key) != got_changed.get(key):
                        print(f"    stream {key}: python={expected['changed'].get(key)!r} "
                              f"rust={got_changed.get(key)!r}")
            for key in ("skipped_long", "growth_reverted", "emptied", "budget_tripped"):
                if expected["counters"][key] != answer["counters"][key]:
                    mismatch(f"[{label}] counter {key}: python={expected['counters'][key]} "
                             f"rust={answer['counters'][key]}")
            # The samples, in the plugin's input order: the flagship case here is invisible
            # junk, so an escape the Rust side misses shows up as this exact diff.
            if expected["samples"] != answer["samples"]:
                mismatch(f"[{label}] samples differ\n"
                         f"    python={expected['samples']}\n    rust  ={answer['samples']}")
            # The fitted summary, while it fits unclipped: past the toast budget the plugin
            # drops lines from the end, which the run summary deliberately keeps, so only
            # the unclipped form has to be identical (and its first line always has to be).
            if expected["action_status"] == "success":
                fitted = expected["action_message"]
                if "message" not in answer:
                    mismatch(f"[{label}] the Rust answer carries no summary message")
                elif len(fitted) < plugin.PluginConfig.TOAST_BUDGET_CHARS \
                        and "(+" not in fitted:
                    if fitted != answer["message"]:
                        mismatch(f"[{label}] summary differs\n"
                                 f"    python={fitted!r}\n    rust  ={answer['message']!r}")
                elif fitted.split("\n")[0] != answer["message"].split("\n")[0]:
                    mismatch(f"[{label}] summary first line differs\n"
                             f"    python={fitted.split(chr(10))[0]!r}\n"
                             f"    rust  ={answer['message'].split(chr(10))[0]!r}")

        elif tool == "template_of":
            expected = template_answer(scan, case)
            compared += 1
            if expected != answer["templates"]:
                mismatch(f"[{label}] template splitter differs")
                for key in expected:
                    if expected[key] != answer["templates"].get(key):
                        print(f"    {key!r}: python={expected[key]} "
                              f"rust={answer['templates'].get(key)}")

        elif tool == "scan_with_cap":
            expected = scan_capped_answer(plugin, scan, case)
            compared += 1
            mismatches += compare_scan(answer["families"], expected["families"], mismatch,
                                       args.show, label)
            if expected["stats"]["skipped_long"] != answer["stats"]["skipped_long"]:
                mismatch(f"[{label}] skipped_long: python={expected['stats']['skipped_long']} "
                         f"rust={answer['stats']['skipped_long']}")
            if expected["stats"]["scanned"] != answer["stats"]["scanned"]:
                mismatch(f"[{label}] scanned: python={expected['stats']['scanned']} "
                         f"rust={answer['stats']['scanned']}")

        else:
            raise SystemExit(f"unknown tool in the corpus: {tool}")

    print(f"[tools] {mismatches} mismatch(es) over {compared} case(s), "
          f"{len(cases)} fixture(s)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
