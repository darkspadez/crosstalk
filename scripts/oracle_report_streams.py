#!/usr/bin/env python3
"""Oracle diff report for step 9: streams on identity families vs Stream-Mapparr.

Phase 3 step 9 migrates streams (match, sort, mirror, visibility, probe format
tiers, triage tags) onto the unified matching layer: one ranker (the engine's
``rank`` to the best identity family), one selection rule (streams attaches the
whole family, all members ranked, above the one ``[matching]`` cut) and one
cache shared with guide. Stream-Mapparr stops being the target and becomes the
oracle that reports differences.

This script runs the streams parity drivers in report mode plus streams
end-to-end fixtures (the unified engine via ``crates/streams/examples/match_io``
against Stream-Mapparr's real ``_match_streams_to_channel`` pipeline via
``scripts/match_parity.py``), attributes every difference to a
``data/corpus/decisions.json`` row, a documented fix or the unified selection
rule, and writes ``docs/oracle-reports/streams.md``.

The representative world (``fixture_world``) carries a KNOWN ANSWER and is run
with the country filter off and on: several provider variants per lineup name,
foreign duplicates, alphabetical siblings. Every difference is counted by
DIRECTION (streams only the unified engine writes vs only the plugin writes),
and each side's attached streams are scored right/wrong against the answer. An
extra stream the answer calls wrong is ``unattributed`` whatever its bucket:
that is a write the evidence does not back, the write-safety surface.

Usage:
    python3 -I scripts/oracle_report_streams.py [--check] [--show N]

    --check   regenerate into a temp file and fail unless it is byte-identical to the
              committed report (the CI gate: the report reproduces).
    --show N  differences printed per bucket (default 10).

Attribution buckets (every difference lands in exactly one):

- ``C07-callsign-first``: the channel names a station in parentheses
  (``ABC (WABC)``) and the unified family carries that callsign while the
  plugin's picks do not (typically the bare national ``ABC`` streams). Decided C07
  ``callsign-first`` (orchestrator): same-callsign streams outrank the national
  ones; the national family is the fallback only when no station stream exists.
- ``callsign-anchor`` (C06/D04): the channel or the plugin's picks carry a callsign
  the engine's anchor does not promote (low-confidence or uncorroborated on one
  side). SM's regex path with corroboration is retired; station-format names still
  resolve through the merged allowlist (C02/D05).
- ``alias+selection`` (A01/D07): an alias-table hit — the merged reverse index is
  inside the engine, so an alias hit is a family member at the cut or nothing.
- ``cut-or-family`` (D02/T10/D07): the engine leaves the group empty below the cut
  (no family clears 95) while SM's fuzzy scale takes it, with no callsign signal.
- ``decided-rows``: different winners with both sides on fuzzy-side methods
  (unified scoring per the corpus decisions N/T/G/C/A series; the control is
  ``engine_parity.py`` + the corpus gates, which pin the decided configuration).
- ``zone-routing`` (Z01 ``sm-zone``): a feed-zone marker separates the sides — the
  unified engine routes the query's zone while SM scores across zones.
- ``order``: the same stream set in a different playback order (quality sort vs the
  collector's order — the finalize tail both sides share, up to ties).
- ``unattributed``: a difference no rule above explains, or (representative
  world) one that writes a stream the known answer calls wrong. Listed for
  review; a non-empty list does not fail the script (reports never fail CI).

The parity sections run the retired-profile-adjacent drivers in report mode and
the unchanged-semantics drivers as gates: match and visibility differ (report,
exit 0); sort, mirror, probe, tools and triage still agree (gates, exit 0
expected). A non-zero gate section fails this script — that is a port bug, not a
migration diff — and lands in ``unattributed``.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys

sys.dont_write_bytecode = True

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "scripts"))
import match_parity  # noqa: E402

REPORT = os.path.join(REPO, "docs", "oracle-reports", "streams.md")

ZONE_RE = re.compile(r"\(w\)|\(e\)|\(west\)|\(east\)|\bwest\b|\beast\b|\bpacific\b|\(pt\)",
                     re.IGNORECASE)
CALLSIGN_RE = re.compile(r"\(([KW][A-Z0-9-]{2,})\)")


def station_set() -> set:
    """Merged station-table bases (C02), for the callsign-signal check."""
    path = os.path.join(REPO, "data", "merged", "stations.json")
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    bases = set()
    for key in (data.get("stations") or {}):
        upper = key.upper()
        bases.add(upper)
        bases.add(upper.split("-")[0])
    return bases


def run(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    return (proc.stdout or "") + (proc.stderr or "")


def run_gate(script: str, args: list[str]) -> dict:
    """Run a parity gate script; return its summary line and exit status."""
    cmd = [sys.executable, script, *args]
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    out = (proc.stdout or "") + (proc.stderr or "")
    return {"cmd": " ".join([script, *args]), "exit": proc.returncode, "raw": out}


def parity_section() -> dict:
    """match + visibility in report mode; sort/mirror/probe/tools/triage as gates."""
    section: dict = {}
    section["match"] = run_gate(
        "scripts/match_parity.py",
        ["--report", "--cases", "60", "--random", "20", "--show", "3"])
    section["visibility"] = run_gate(
        "scripts/visibility_parity.py",
        ["--report", "--random", "20", "--show", "3"])
    section["sort"] = run_gate("scripts/sort_parity.py", ["--random", "10"])
    section["mirror"] = run_gate("scripts/mirror_parity.py", ["--random", "10"])
    section["probe"] = run_gate("scripts/probe_parity.py", [])
    section["tools"] = run_gate("scripts/tools_parity.py", [])
    section["triage"] = run_gate("scripts/triage_parity.py", ["--random", "10"])
    return section


def summary_line(raw: str, tag: str) -> str:
    for line in raw.splitlines():
        if line.startswith(f"[{tag}]") and "mismatch" in line:
            return line.strip()
    return "(no summary line)"


COUNTRIES = ("US", "UK", "CA")

#: Decided C07 probes (documented, not sampled), appended to the representative world:
#: network-led station channels over streams holding both the station and the bare
#: national feed, plus one with no station stream (the national fallback, whose known
#: answer is the national feed). ``(channel, truth base)`` and ``(stream, truth base)``.
C07_CHANNELS = (
    ("ABC (WABC)", "WABC"),
    ("US: NBC (KSDK) HD", "KSDK"),
    ("CBS (WFOR) [HD]", "CBS"),
)
C07_STREAMS = (
    ("ABC", "ABC"), ("ABC HD", "ABC"), ("WABC-TV", "WABC"),
    ("ABC 7 New York (WABC) HD", "WABC"),
    ("NBC", "NBC"), ("US: NBC HD", "NBC"), ("NBC 5 St. Louis KSDK", "KSDK"),
    ("KSDK", "KSDK"),
    ("CBS", "CBS"), ("CBS HD", "CBS"),
)


def fixture_world() -> tuple[list[dict], list[dict]]:
    """Deterministic REPRESENTATIVE world with a known answer (sections 2a/2b).

    Lineup: seeded anchor names from the US/UK/CA channel databases, each with up to
    three alphabetical SIBLINGS sharing its first word (``ESPN``/``ESPN 2``/``ESPNews``
    is how real lineups look, and siblings are where an over-wide family would attach
    the wrong channel). Streams: every lineup name in two to five provider variants —
    ``HD``/``FHD``/``[4K]``/``(Backup)``, its own ``<CC>:`` prefix, and a FOREIGN
    ``<CC>:`` duplicate for a quarter of them (the country filter's whole subject) —
    several streams per name, as a real provider listing carries. Channels: 85% of the
    names, provider-decorated (verbatim, ``HD``, ``[FHD]``, ``<CC>:`` of its own
    country, ``(Backup)``, upper case, ``HD`` leading), a fifth of them in a
    ``US: Sports``/``UK: Sports`` group of their own country; plus database names absent
    from the listing as true negatives.

    Every record carries its truth: the database name it came from (``base``) and the
    country its decoration states (``country``; a channel's group country is truth
    only with the country filter on).
    """
    channels_dir = os.path.join(REPO, "data", "channels")
    import random
    rng = random.Random(20261106)
    names: list[tuple[str, str]] = []
    seen: set[str] = set()
    for code, anchors in (("US", 70), ("UK", 25), ("CA", 15)):
        with open(os.path.join(channels_dir, f"{code}_channels.json"), encoding="utf-8") as fh:
            listing = [c.get("channel_name") for c in json.load(fh)["channels"]
                       if c.get("channel_name")]
        for start in sorted(rng.sample(range(len(listing)), anchors)):
            first = listing[start].split()[0].lower()
            for name in listing[start:start + 4]:
                if name.split()[0].lower() != first:
                    break
                if name.lower() not in seen:
                    seen.add(name.lower())
                    names.append((name, code))
    streams: list[dict] = []
    decorations = ("{n}", "{n} HD", "{n} FHD", "{n} [4K]", "{n} (Backup)")
    listed = names[: int(len(names) * 0.85)]
    absent = names[int(len(names) * 0.85):]
    for name, code in listed:
        variants = rng.sample(decorations, rng.randint(2, 4))
        for index, pattern in enumerate(variants):
            label = pattern.format(n=name)
            own = index == 0 or rng.random() < 0.35
            if own and rng.random() < 0.5:
                streams.append({"name": f"{code}: {label}", "base": name, "country": code})
            else:
                streams.append({"name": label, "base": name, "country": None})
        if rng.random() < 0.25:
            foreign = rng.choice([other for other in COUNTRIES if other != code])
            streams.append({"name": f"{foreign}: {rng.choice(decorations).format(n=name)}",
                            "base": name, "country": foreign})
    rng.shuffle(streams)
    channels: list[dict] = []
    for name, code in listed + absent:
        pick = rng.random()
        country = None
        if pick < 0.2:
            label = name
        elif pick < 0.35:
            label = f"{name} HD"
        elif pick < 0.45:
            label = f"{name} [FHD]"
        elif pick < 0.6:
            label, country = f"{code}: {name}", code
        elif pick < 0.7:
            label = f"{name} (Backup)"
        elif pick < 0.8:
            label = name.upper()
        else:
            label = f"HD {name}"
        group, group_country = 100, None
        if code in ("US", "UK") and rng.random() < 0.2:
            group, group_country = (102, "US") if code == "US" else (103, "UK")
        channels.append({"name": label, "base": name, "country": country,
                         "group": group, "group_country": group_country})
    # Decided C07 probes, appended after the seeded draws (the sample is unchanged).
    for label, base in C07_STREAMS:
        streams.append({"name": label, "base": base, "country": None})
    for label, base in C07_CHANNELS:
        channels.append({"name": label, "base": base, "country": None,
                         "group": 100, "group_country": None})
    return channels, streams


TRUTH_QUALITY = re.compile(r"(?:\s+(?:hd|fhd|uhd|sd|4k))+$")


def canonical(name: str, alias_table: dict[str, str]) -> str:
    """Truth equivalence: the merged alias table's canonical (A01), case-folded, with a
    trailing quality word dropped — the databases list `BBC Alba` and `BBC Alba HD` as
    two rows of one channel, and a provider's `HD` is a feed attribute, not identity."""
    folded = (name or "").strip().lower()
    folded = alias_table.get(folded, folded).strip().lower()
    return TRUTH_QUALITY.sub("", folded)


def right_for(channel: dict, stream: dict, alias_table: dict[str, str],
              restrict: bool) -> bool:
    """The known answer: the stream carries the channel's database name (alias-equal)
    and no country that contradicts the channel's (its name's, or with the filter on its
    group's; an unstated country on either side is compatible, fail-open)."""
    if canonical(channel["base"], alias_table) != canonical(stream["base"], alias_table):
        return False
    want = channel["country"] or (channel["group_country"] if restrict else None)
    return want is None or stream["country"] is None or stream["country"] == want


def disputed_names() -> tuple[list[str], list[str]]:
    """Disputed fixture (section 4): pairs Stream-Mapparr takes but another plugin or
    the label disputes — exactly where a migration diff can hide — plus the pair
    candidates and DB names as the stream pool."""
    with open(os.path.join(REPO, "data", "corpus", "pairs.jsonl"), encoding="utf-8") as fh:
        pairs = [json.loads(line) for line in fh]
    disputed = [
        pair for pair in pairs
        if pair["verdicts"].get("sm")
        and (
            pair["label"] == "different"
            or not all(pair["verdicts"].get(key, False) for key in ("janitor", "cm", "lu"))
        )
    ]
    disputed.sort(key=lambda pair: (pair["generator"], pair["query"], pair["candidate"]))
    small = [pair for pair in disputed if pair["generator"].split(":")[0] in
             ("near-miss", "collision", "case")]
    big = [pair for pair in disputed if pair not in small]
    wanted = ("alias", "station", "real")
    picked = list(small)
    for family in wanted:
        picked.extend(pair for pair in big if pair["generator"].startswith(family + ":"))
    picked = picked[:60]
    entries: list[str] = []
    seen: set[str] = set()

    def add(name: str) -> None:
        if name and name not in seen:
            seen.add(name)
            entries.append(name)

    for pair in picked:
        add(pair["candidate"])
    channels_dir = os.path.join(REPO, "data", "channels")
    for code, count in (("US", 60), ("UK", 15), ("CA", 10)):
        with open(os.path.join(channels_dir, f"{code}_channels.json"), encoding="utf-8") as fh:
            for channel in json.load(fh)["channels"][:count]:
                add(channel.get("channel_name"))
                if len(entries) >= 150:
                    break
    queries = list(dict.fromkeys(pair["query"] for pair in picked))
    return queries, entries


def aliases() -> dict[str, str]:
    """Merged alias table variant -> canonical (A01), for attribution."""
    path = os.path.join(REPO, "data", "merged", "aliases.json")
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    table: dict[str, str] = {}
    for canonical, rows in (data.get("aliases") or {}).items():
        for row in rows or []:
            alias = (row.get("alias") or "").strip().lower()
            if alias and alias not in table:
                table[alias] = canonical
    return table


def make_case(comment: str, channels: list[dict], streams: list[dict],
              restrict: bool = False) -> dict:
    channel_rows = [
        match_parity.channel(i + 1, row["name"], [], group=row.get("group", 100), number=i + 1)
        for i, row in enumerate(channels)
    ]
    stream_rows = [
        match_parity.stream(20000 + i, row["name"], account=1, group=100)
        for i, row in enumerate(streams)
    ]
    return {
        "comment": comment,
        # Both sides cut at 95 here (SM `exact` = 95, host `strict` = 95 via the
        # example's preset mapping): cut-scale diffs are D02, not harness skew.
        "settings": {
            "match_sensitivity": "exact",
            "overwrite_streams": True,
            "keep_custom_streams": True,
            "allow_same_name_streams": True,
            "restrict_matching_to_country": restrict,
            "visible_channel_limit": 1,
            "ignore_tags": "",
            "selected_m3us": "",
            "prioritize_quality": False,
            "ignore_quality": True,
            "ignore_regional": True,
            "ignore_geographic": True,
            "ignore_misc": True,
        },
        "channels": channel_rows,
        "streams": stream_rows,
        "accounts": match_parity.ACCOUNTS,
        "groups": match_parity.GROUPS,
        "channels_data": [],
        "throughput": {},
        "dead": [],
        "epg": {},
        "epg_entries": [],
        "current_programs": {},
        "epg_today": "8-5",
    }


def upper_tokens(name: str) -> set[str]:
    return {token for token in re.findall(r"[A-Z0-9]{4,}", name.upper())}


def attribute(channel: str, python_after: list, rust_after: list, rust_reason: str,
              id_to_name: dict, alias_table: dict[str, str], stations: set) -> str:
    """One bucket per difference (see the module docstring for the rules); a difference
    no rule explains is ``unattributed``."""
    python_names = [id_to_name.get(i, "") for i in python_after]
    rust_names = [id_to_name.get(i, "") for i in rust_after]
    if set(python_after) == set(rust_after) and python_after != rust_after:
        return "order"
    paren_station = CALLSIGN_RE.search(channel)
    if paren_station:
        # Decided C07: the unified family carries the channel's callsign, the
        # plugin's picks do not.
        word = re.compile(rf"\b{re.escape(paren_station.group(1).split('-')[0])}\b")
        if any(word.search(name) for name in rust_names) and not any(
                word.search(name) for name in python_names):
            return "C07-callsign-first"
    chan_tokens = upper_tokens(channel)
    pick_tokens = set().union(*(upper_tokens(name) for name in python_names))
    # Callsign-shaped agreement (a merged station-table base on both sides), not just
    # any shared word: `BBC`/`ONE`/`SOUTH` are names, `WNCF`/`WABC` are stations.
    station_shared = (chan_tokens & pick_tokens) & stations
    paren = CALLSIGN_RE.search(channel) or any(
        CALLSIGN_RE.search(name) for name in python_names)
    callsign_signal = bool(paren) or bool(station_shared)
    zones = lambda names: {bool(ZONE_RE.search(name)) for name in names}  # noqa: E731
    if not rust_after and python_after:
        # SM-only: what did the plugin see that the family drops?
        if callsign_signal:
            return "callsign-anchor"
        channel_zoned = bool(ZONE_RE.search(channel))
        picks_zoned = {bool(ZONE_RE.search(name)) for name in python_names}
        if picks_zoned == {True} and not channel_zoned:
            return "zone-routing"
        return "cut-or-family"
    if rust_after and not python_after:
        if "alias" in rust_reason:
            return "alias+selection"
        if "callsign" in rust_reason:
            return "callsign-anchor"
        return "decided-rows"
    if zones([channel, *python_names]) != zones([channel, *rust_names]):
        return "zone-routing"
    if "alias" in rust_reason:
        return "alias+selection"
    if "callsign" in rust_reason:
        return "callsign-anchor"
    chan_alias = alias_table.get(channel.strip().lower())
    if chan_alias and any(
            (name or "").strip().lower() in alias_table
            and alias_table[(name or "").strip().lower()] == chan_alias
            for name in python_names + rust_names):
        return "alias+selection"
    if set(python_after) & set(rust_after):
        # Both sides took the channel's own name; they differ on which neighbours ride
        # along — the family rule (D07) against SM's containment/number guards
        # (T03/T05/T06), scored per the corpus decisions.
        return "decided-rows"
    return "unattributed"


def end_to_end(channels: list[dict], streams: list[dict], comment: str,
               alias_table: dict[str, str], stations: set, restrict: bool = False,
               truth: bool = False) -> dict:
    """One match case through the plugin and through match_io; per-channel verdicts,
    the direction of every difference (streams only unified attaches vs only SM does)
    and, for a world with a known answer, which side is right."""
    plugin, matcher = match_parity.load_plugin()
    binary = os.path.join(REPO, "target", "debug", "examples", "match_io")
    subprocess.run(["cargo", "build", "-q", "-p", "crosstalk-streams",
                    "--example", "match_io"], cwd=REPO, check=True)
    case = make_case(comment, channels, streams, restrict)
    expected_run = match_parity.plugin_answer(plugin, matcher, case)
    expected = expected_run["assignments"]
    answers = match_parity.run_rust([case], binary)
    got = {int(row["channel_id"]): row for row in answers[0]["assignments"]}
    id_to_name = {s["id"]: s["name"] for s in case["streams"]}
    stream_of = {20000 + i: row for i, row in enumerate(streams)}
    rows = []
    matched_python = matched_rust = agree = 0
    buckets: dict[str, int] = {}
    direction = {"extra_channels": 0, "extra_streams": 0, "fewer_channels": 0,
                 "fewer_streams": 0}
    score = {"python_right": 0, "python_wrong": 0, "rust_right": 0, "rust_wrong": 0,
             "extra_right": 0, "extra_wrong": 0, "fewer_right": 0, "fewer_wrong": 0,
             "available": 0}
    wrong_extras: list[dict] = []
    for cid, channel in enumerate(channels, start=1):
        python_after = expected[cid]["after"]
        mine = got[cid]
        rust_after = mine["after"]
        extra = [i for i in rust_after if i not in set(python_after)]
        fewer = [i for i in python_after if i not in set(rust_after)]
        if extra:
            direction["extra_channels"] += 1
            direction["extra_streams"] += len(extra)
        if fewer:
            direction["fewer_channels"] += 1
            direction["fewer_streams"] += len(fewer)
        if truth:
            right = lambda i: right_for(channel, stream_of[i], alias_table, restrict)  # noqa: E731
            score["available"] += sum(1 for i in stream_of if right(i))
            for side, after in (("python", python_after), ("rust", rust_after)):
                for i in after:
                    score[f"{side}_{'right' if right(i) else 'wrong'}"] += 1
            for i in extra:
                score[f"extra_{'right' if right(i) else 'wrong'}"] += 1
                if not right(i):
                    wrong_extras.append({"channel": channel["name"],
                                         "stream": id_to_name[i],
                                         "reason": mine["reason"]})
            for i in fewer:
                score[f"fewer_{'right' if right(i) else 'wrong'}"] += 1
        if python_after:
            matched_python += 1
        if rust_after:
            matched_rust += 1
        if python_after == rust_after:
            agree += 1
            continue
        bucket = attribute(channel["name"], python_after, rust_after, mine["reason"],
                           id_to_name, alias_table, stations)
        if truth and extra and any(not right_for(channel, stream_of[i], alias_table, restrict)
                                   for i in extra):
            # A stream only the unified engine writes, and the known answer says it is
            # not this channel's: the write-safety review surface, whatever the bucket.
            bucket = "unattributed"
        buckets[bucket] = buckets.get(bucket, 0) + 1
        rows.append({
            "channel": channel["name"],
            "bucket": bucket,
            "python": [id_to_name.get(i, i) for i in python_after],
            "rust": [id_to_name.get(i, i) for i in rust_after],
            "reason": mine["reason"],
        })
    rows.sort(key=lambda row: (row["bucket"], row["channel"]))
    return {
        "channels": len(channels),
        "streams": len(streams),
        "matched_python": matched_python,
        "matched_rust": matched_rust,
        "agree": agree,
        "buckets": buckets,
        "rows": rows,
        "direction": direction,
        "truth": score if truth else None,
        "wrong_extras": wrong_extras,
    }


def render(parity: dict, worlds: list[tuple[str, str, dict]], show: int) -> str:
    lines = []
    lines.append("<!-- Generated by scripts/oracle_report_streams.py; do not edit. -->")
    lines.append("<!-- Reproduce with: python3 -I scripts/oracle_report_streams.py -->")
    lines.append("# Streams oracle diff report (step 9)")
    lines.append("")
    lines.append("Streams runs the unified matching layer (one ranker to the best identity")
    lines.append("family, one selection rule — streams attaches the whole family — one cut at")
    lines.append("`[matching]` sensitivity, here 95); Stream-Mapparr is the oracle.")
    lines.append("Every difference below traces to a `data/corpus/decisions.json` row, a")
    lines.append("documented fix or the unified selection rule; the unattributed list is the")
    lines.append("review surface.")
    lines.append("")
    lines.append("Section 2 is REPRESENTATIVE (seeded, not mined, with a known answer: read its")
    lines.append("agreement rate as the migration's real-world size, and its right/wrong table")
    lines.append("as which side is correct), run with the country filter off and on. Section 3")
    lines.append("is mined from the pairs Stream-Mapparr takes but another plugin or the label")
    lines.append("disputes, so its agreement rate is low by construction and it exists to show")
    lines.append("every kind of difference, not how often they happen. Every section counts the")
    lines.append("DIRECTION of each difference: streams only the unified engine writes (extra)")
    lines.append("and streams only Stream-Mapparr writes (fewer). An extra stream the known")
    lines.append("answer says is wrong is unattributed by rule: a write the evidence does not")
    lines.append("back.")
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    lines.append("- cut (host `[matching]` sensitivity at strict, SM `exact`): 95")
    rep = worlds[0][2]
    dis = worlds[-1][2]
    lines.append(f"- representative world: {rep['channels']} channels x {rep['streams']} streams")
    lines.append("  (lineup: seeded anchors from the US/UK/CA channel databases plus up to three")
    lines.append("  alphabetical siblings each — `ESPN`/`ESPN 2`/`ESPNews`; streams: two to five")
    lines.append("  provider variants per listed name — `HD`, `FHD`, `[4K]`, `(Backup)`, its own")
    lines.append("  `<CC>:` — and a foreign `<CC>:` duplicate for a quarter of them; channels:")
    lines.append("  85% of the names provider-decorated, a fifth of the US/UK ones in a")
    lines.append("  `US: Sports`/`UK: Sports` group, plus the rest as true negatives; plus three")
    lines.append("  documented C07 probes: `ABC (WABC)` and `US: NBC (KSDK) HD` over streams")
    lines.append("  holding the station and the bare national feed — answer: the station — and")
    lines.append("  `CBS (WFOR) [HD]` with no WFOR stream — answer: the national `CBS`, the")
    lines.append("  fallback)")
    lines.append(f"- disputed fixture: {dis['channels']} queries x {dis['streams']} streams")
    lines.append("  (queries mined from `data/corpus/pairs.jsonl`: pairs Stream-Mapparr takes")
    lines.append("  but another plugin disputes or the label disputes; entries are the pair")
    lines.append("  candidates plus DB names)")
    lines.append("")
    lines.append("## 1. parity drivers (report mode + gates)")
    lines.append("")
    lines.append("`match` and `visibility` run in report mode (they differ — the migration);")
    lines.append("`sort`, `mirror`, `probe`, `tools` and `triage` keep their semantics (probe")
    lines.append("parsing, sort key, mirror plan, tools) and stay gates at 0.")
    lines.append("")
    for key in ("match", "visibility", "sort", "mirror", "probe", "tools", "triage"):
        sec = parity[key]
        lines.append(f"- `{key}`: exit {sec['exit']}: {summary_line(sec['raw'], key)}")
    lines.append("")
    for num, title, data in worlds:
        lines.append(f"## {num}. streams end-to-end, {title}: unified engine vs Stream-Mapparr")
        lines.append("")
        lines.append(f"- channels: {data['channels']}, streams: {data['streams']}")
        lines.append(f"- plugin matched: {data['matched_python']}, "
                     f"unified matched: {data['matched_rust']}")
        lines.append(f"- agreement (identical after-lists): {data['agree']}/{data['channels']} "
                     f"({100.0 * data['agree'] / max(data['channels'], 1):.1f}%)")
        d = data["direction"]
        lines.append(f"- direction: {d['extra_channels']} channel(s) where unified attaches "
                     f"extra streams ({d['extra_streams']} stream(s)); "
                     f"{d['fewer_channels']} where it attaches fewer "
                     f"({d['fewer_streams']} stream(s))")
        if data["buckets"]:
            lines.append("- attribution:")
            for bucket in sorted(data["buckets"]):
                lines.append(f"  - `{bucket}`: {data['buckets'][bucket]}")
        else:
            lines.append("- attribution: no differences.")
        lines.append("")
        truth = data["truth"]
        if truth:
            lines.append("Against the known answer (streams of the channel's own database name, "
                         "no contradicting country):")
            lines.append("")
            lines.append("| | Stream-Mapparr | unified |")
            lines.append("|---|---|---|")
            lines.append(f"| right streams attached (of {truth['available']}) | "
                         f"{truth['python_right']} | {truth['rust_right']} |")
            lines.append(f"| wrong streams attached | {truth['python_wrong']} | "
                         f"{truth['rust_wrong']} |")
            lines.append("")
            lines.append(f"- extra (only unified): {truth['extra_right']} right, "
                         f"{truth['extra_wrong']} wrong")
            lines.append(f"- fewer (only Stream-Mapparr): {truth['fewer_right']} right "
                         f"(missed), {truth['fewer_wrong']} wrong (dropped)")
            lines.append("- reading it: a family holds ONE country (G03, `identity.rs`), so")
            lines.append("  for a channel whose own country is unknown the unified engine keeps")
            lines.append("  the unmarked feeds plus ONE country's and drops the other countries'")
            lines.append("  duplicates the plugin piles on — most of the right streams missed")
            lines.append("  are those (a write of fewer streams, never of a wrong one). Any wrong")
            lines.append("  extra is listed here and in section 4.")
            if data["wrong_extras"]:
                lines.append("- wrong extras (the write-safety surface):")
                for row in data["wrong_extras"][:show]:
                    lines.append(f"  - `{row['channel']}` <- `{row['stream']}` "
                                 f"({row['reason']})")
            lines.append("")
        if data["rows"]:
            lines.append(f"### differences (showing up to {show} per bucket)")
            lines.append("")
            by_bucket: dict[str, list] = {}
            for row in data["rows"]:
                by_bucket.setdefault(row["bucket"], []).append(row)
            for bucket in sorted(by_bucket):
                lines.append(f"#### `{bucket}` ({len(by_bucket[bucket])})")
                lines.append("")
                for row in by_bucket[bucket][:show]:
                    lines.append(f"- channel `{row['channel']}`")
                    lines.append(f"  - plugin: {row['python']}")
                    lines.append(f"  - unified: {row['rust']}")
                    lines.append(f"  - reason: {row['reason']}")
                lines.append("")
        else:
            lines.append("No differences.")
            lines.append("")
    unattributed = [row for _, _, data in worlds for row in data["rows"]
                    if row["bucket"] == "unattributed"]
    lines.append("## 4. unattributed (review surface)")
    lines.append("")
    if unattributed:
        for row in unattributed[:show * 3]:
            lines.append(f"- channel `{row['channel']}`: "
                         f"plugin {row['python']} vs unified {row['rust']} "
                         f"({row['reason']})")
    else:
        lines.append("Empty: every difference traces to a bucket above, and no extra stream")
        lines.append("contradicts the known answer.")
    lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="regenerate into a temp file and fail unless byte-identical")
    parser.add_argument("--show", type=int, default=10)
    args = parser.parse_args()

    parity = parity_section()
    # Gate sections must be 0: a non-zero here is a port bug, not a migration diff.
    failed = [key for key in ("sort", "mirror", "probe", "tools", "triage")
              if parity[key]["exit"] != 0]
    if failed:
        print(f"gate sections failed: {failed}", file=sys.stderr)
        for key in failed:
            print(parity[key]["raw"][-2000:], file=sys.stderr)
        return 1
    table = aliases()
    station_bases = station_set()
    rep_channels, rep_streams = fixture_world()
    rep_off = end_to_end(rep_channels, rep_streams, "representative", table, station_bases,
                         restrict=False, truth=True)
    rep_on = end_to_end(rep_channels, rep_streams, "representative, filter on", table,
                        station_bases, restrict=True, truth=True)
    dis_channels, dis_streams = disputed_names()
    dis = end_to_end([{"name": name} for name in dis_channels],
                     [{"name": name} for name in dis_streams], "disputed", table,
                     station_bases)
    worlds = [
        ("2a", "representative world, country filter off", rep_off),
        ("2b", "representative world, country filter on", rep_on),
        ("3", "disputed world", dis),
    ]
    text = render(parity, worlds, args.show)
    if args.check:
        with open(REPORT, encoding="utf-8") as fh:
            committed = fh.read()
        if committed != text:
            print(f"{REPORT} differs; regenerate without --check", file=sys.stderr)
            return 1
        print(f"{REPORT} reproduces")
        return 0
    with open(REPORT, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(f"wrote {REPORT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
