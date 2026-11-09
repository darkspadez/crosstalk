#!/usr/bin/env python3
"""Oracle diff report for step 8: guide on identity families vs EPG Janitor.

Phase 3 step 8 migrates guide (match, heal, scan, cleanup) onto the unified matching
layer: one ranker (the engine's ``rank`` to the best identity family), one selection
rule (the family's first entry with programme data), one cut (``[matching]``
sensitivity). Janitor stops being the target and becomes the oracle that reports
differences.

This script runs the guide-path parity drivers in report mode plus a guide end-to-end
fixture (the unified engine via ``crates/guide/examples/oracle_io`` against Janitor's
real ``_find_best_epg_match`` via ``scripts/corpus/oracles.py``), attributes every
difference to a ``data/corpus/decisions.json`` row, a documented fix or the unified
selection rule, and writes ``docs/oracle-reports/guide.md``.

Usage:
    python3 -I scripts/oracle_report_guide.py [--check] [--show N]

    --check   regenerate into a temp file and fail unless it is byte-identical to the
              committed report (the CI gate: the report reproduces).
    --show N  differences printed per bucket (default 15).

Attribution buckets (every difference lands in exactly one):

- ``C07-callsign-first``: the channel names a station in parentheses
  (``ABC (WABC)``) and the unified winner carries that callsign while Janitor's
  does not (typically the bare national ``ABC`` on the stripped key). Decided C07
  ``callsign-first`` (orchestrator): a same-callsign entry outranks the national
  one; the national entry is the fallback only when the listing has no station.
- ``D06-retired``: Janitor's winner is structural-only (its method names
  Callsign/State/City/Network with no Alias/Fuzzy side) while the unified engine
  decides by family. D06 kept the structural score as the sole EPG implementation
  (no-evidence); step 8 retires it — callsign agreement now decides at family level
  through the engine anchor (C06/D04).
- ``D02/T10-cut``: both sides agree on the winner but one side leaves it below its
  cut (Janitor's confidence carries structural bonuses; the engine score does not —
  the cut sweep in the D02 reason), or only one side matches at all on the same
  top candidate.
- ``alias+selection`` (A01/D07): alias-driven disagreement — Janitor's Alias hit vs
  the unified reverse-index family, including families split by the one-country rule
  (G03, bug-158/161).
- ``decided-rows``: different winners with both sides on fuzzy-side methods
  (unified scoring per the corpus decisions N/T/G/C/A series; the control is
  ``engine_parity.py`` + the corpus gates, which pin the decided configuration).
- ``selection-rule``: same score, different winner — the family cut and its stable
  order (T12 ``lu``) vs the oracle's per-entry stable score sort.
- ``zone-routing`` (Z01 ``sm-zone``): Janitor scores across feed zones while the
  unified engine routes the query's zone; the other-zone entry never enters the family.
- ``unattributed``: listed for review. A non-empty list does not fail the script
  (reports never fail CI); it is the review surface.

The parity-sweep (match/score) and sim sections run the retired-profile tooling
against Janitor and are expected to stay at zero: guide no longer uses that code in
production, but the profile stays in the crate as oracle tooling, so a mismatch there
is a port bug, not a migration diff, and lands in ``unattributed``.
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
sys.path.insert(0, os.path.join(REPO, "scripts", "corpus"))
import oracles  # noqa: E402

REPORT = os.path.join(REPO, "docs", "oracle-reports", "guide.md")
THRESHOLD = 95

STRUCTURAL_WORDS = ("callsign", "state", "city", "network")
FUZZY_WORDS = ("fuzzy", "alias", "exact", "substring")
#: A parenthesised station callsign on the channel (`ABC (WABC)`, `NBC (KSDK-TV)`).
PAREN_CALLSIGN = re.compile(r"\(([KW][A-Z]{2,3})(?:-(?:TV|CD|LP|DT|LD))?\)")

#: Decided C07 probes (documented, not sampled): a network-led station channel over a
#: listing holding both the station and the bare national entry, plus one whose
#: station is absent (the national fallback). ``(channel, expected entry)``.
C07_PROBES = (
    ("ABC (WABC)", "ABC 7 New York (WABC)"),
    ("US: NBC (KSDK) HD", "NBC 5 St. Louis KSDK"),
    ("CBS (WFOR) [HD]", "CBS"),
)
C07_ENTRIES = ("ABC", "NBC", "CBS", "ABC 7 New York (WABC)", "NBC 5 St. Louis KSDK")

def run(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
    return (proc.stdout or "") + (proc.stderr or "")


def sweep_section() -> dict:
    """parity_sweep match/score in report mode: counts, never failing."""
    out = run([
        sys.executable, "-I", "scripts/parity_sweep.py",
        "--kinds", "match,score", "--limit", "300", "--pool", "60", "--report",
    ])
    section = {"kinds": {}, "raw": out}
    for line in out.splitlines():
        # e.g. "[match] 0 mismatches out of 3600 cases" and the [report] echo.
        if line.startswith(("[match]", "[score]")) and "mismatches out of" in line:
            kind = line[1:line.index("]")]
            mismatches = int(line.split("mismatches")[0].split()[-1])
            total = int(line.split("out of")[-1].split()[0])
            section["kinds"][kind] = {"mismatches": mismatches, "cases": total}
    return section


def sim_section() -> dict:
    """sim_parity.sh --quick in report mode: per-file verdicts, never failing."""
    out = run(["./scripts/sim_parity.sh", "--quick", "--report"])
    files: dict[str, str] = {}
    for line in out.splitlines():
        parts = line.split()
        # e.g. "    [default] per_stream_matches.csv identical"
        if len(parts) == 3 and parts[0].startswith("[") and parts[2] in ("identical", "DIFFERS"):
            files[f"{parts[0]} {parts[1]}"] = parts[2]
    unified = next((line.strip() for line in out.splitlines()
                    if line.startswith("[unified]")), None)
    return {"files": files, "unified": unified, "raw": out}


def fixture_names() -> tuple[list[str], list[str]]:
    """Deterministic end-to-end fixture: queries mined from the labelled corpus pairs
    Janitor takes but another plugin disputes (or the label disputes) — exactly where a
    migration diff can hide — plus the pair candidates and DB names as the entry pool."""
    with open(os.path.join(REPO, "data", "corpus", "pairs.jsonl"), encoding="utf-8") as fh:
        pairs = [json.loads(line) for line in fh]
    disputed = [
        pair for pair in pairs
        if pair["verdicts"].get("janitor")
        and (
            pair["label"] == "different"
            or not all(pair["verdicts"].get(key, False) for key in ("cm", "lu", "sm"))
        )
    ]
    # Whole generator families first (near-miss/collision/case are small), then a stable
    # head of the big families. Sort for determinism: no dict-order dependence.
    disputed.sort(key=lambda pair: (pair["generator"], pair["query"], pair["candidate"]))
    small = [pair for pair in disputed if pair["generator"].split(":")[0] in
             ("near-miss", "collision", "case")]
    big = [pair for pair in disputed if pair not in small]
    wanted = ("alias", "station", "real")
    picked = list(small)
    for family in wanted:
        picked.extend(pair for pair in big if pair["generator"].startswith(family + ":"))
    picked = picked[:48]

    entries: list[str] = []
    seen = set()

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
                if len(entries) >= 130:
                    break
    # One row per query: the same query from two pairs runs identically over one pool.
    queries = list(dict.fromkeys(pair["query"] for pair in picked))
    # One synthetic structural probe (documented, not mined): a low-confidence shared
    # callsign with the same state and city scores Janitor's structural 100
    # (`Callsign + State + City + Network`) while the unified engine — whose anchor needs
    # high confidence on both sides (C06/D04) — scores it below the cut. This is the
    # retired D06 mechanism in isolation; without it the bucket would sit at zero.
    add("XYZ WABC - IL Springfield")
    queries.append("ABC WABC - IL Springfield")
    return queries, entries


def janitor_is_structural_only(method: str | None) -> bool:
    method = (method or "").lower()
    return any(word in method for word in STRUCTURAL_WORDS) and not any(
        word in method for word in FUZZY_WORDS
    )


def callsign_first_split(query: str, j_name: str | None, u_name: str | None) -> bool:
    """Decided C07: the unified winner carries the channel's parenthesised callsign
    and Janitor's winner does not."""
    match = PAREN_CALLSIGN.search(query)
    if not match or u_name is None:
        return False
    word = re.compile(rf"\b{match.group(1)}\b")
    return bool(word.search(u_name)) and not (j_name and word.search(j_name))


def attribute(query: str, janitor: tuple | None, unified: tuple | None,
              zone_split: bool) -> str:
    """One bucket per difference (see the module docstring for the rules).

    ``zone_split`` comes from the engine itself (``oracle_io``'s ``probe_zone_split``:
    the Z01 guard separates the query from Janitor's winner), never from a word list."""
    j_name, j_score, j_method = janitor if janitor else (None, 0, None)
    u_name, u_score, u_method = unified if unified else (None, 0, None)
    if j_name != u_name and callsign_first_split(query, j_name, u_name):
        return "C07-callsign-first"
    if j_name is not None and u_name is not None and j_name != u_name:
        if janitor_is_structural_only(j_method):
            return "D06-retired"
        # Janitor scores across feed zones (including its alias hits); the unified engine
        # routes (Z01 `sm-zone`), so a winner the engine's zone guard separates from the
        # query is a zone diff — even when either method says `Alias`.
        if zone_split:
            return "zone-routing"
        j_alias = "alias" in (j_method or "").lower()
        # Any unified alias resolve (the reverse-index family, A01) on a different
        # winner is an alias-table disagreement — even when Janitor's own method only
        # says `Fuzzy` (its components collapse exact/substring hits into `Fuzzy`).
        u_alias = (u_method or "").lower() == "alias"
        if j_alias or u_alias:
            return "alias+selection"
        # Same score, different winner: the stable order (T12 `lu`), not the scoring.
        if j_score == u_score:
            return "selection-rule"
        j_fuzzy = any(word in (j_method or "").lower() for word in FUZZY_WORDS)
        u_fuzzy = (u_method or "").lower() in ("fuzzy", "exact", "substring", "callsign")
        if j_fuzzy and u_fuzzy:
            return "decided-rows"
        return "selection-rule"
    # Same winner (or one side empty) with different match/no-match: the cut.
    if j_name == u_name:
        return "D02/T10-cut"
    # One side matches, the other does not, on different tops.
    if janitor is not None and unified is None:
        if janitor_is_structural_only(j_method):
            return "D06-retired"
        if zone_split:
            return "zone-routing"
        if "alias" in (j_method or "").lower():
            return "alias+selection"
        return "D02/T10-cut"
    if unified is not None and janitor is None:
        if "alias" in (u_method or "").lower():
            return "alias+selection"
        return "D02/T10-cut"
    return "unattributed"


def representative_names() -> tuple[list[str], list[str], list[str | None]]:
    """A representative guide world, NOT mined from disputes (the bias check on the
    disputed fixture): EPG entries are a seeded sample of the shipped channel databases'
    display names (US/UK/CA, as a Schedules Direct/XMLTV listing names them); channels
    are provider-decorated copies of a sample of those entries, built the way
    ``sim_parity.sh`` builds its M3U (`HD`, `[FHD]`, `<CC>: ` prefix, `(Backup)`,
    upper case, verbatim), plus database names absent from the listing (true
    negatives: any match there is a false positive).

    Returns ``(queries, entries, expected)``: ``expected[i]`` is the entry the channel
    was decorated from, or None for a true negative."""
    import random

    rng = random.Random(8)
    pool: list[tuple[str, str]] = []
    seen = set()
    for code in ("US", "UK", "CA"):
        path = os.path.join(REPO, "data", "channels", f"{code}_channels.json")
        with open(path, encoding="utf-8") as fh:
            for channel in json.load(fh)["channels"]:
                name = (channel.get("channel_name") or "").strip()
                if name and name not in seen:
                    seen.add(name)
                    pool.append((code, name))
    pool.sort()
    picked = rng.sample(pool, 660)
    listing, absent = picked[:600], picked[600:]
    entries = [name for _, name in listing]
    decorations = (
        lambda code, name: name,
        lambda code, name: f"{name} HD",
        lambda code, name: f"{name} [FHD]",
        lambda code, name: f"{code}: {name}",
        lambda code, name: f"{name} (Backup)",
        lambda code, name: name.upper(),
    )
    queries: list[str] = []
    expected: list[str | None] = []
    for index, (code, name) in enumerate(rng.sample(listing, 240)):
        queries.append(decorations[index % len(decorations)](code, name))
        expected.append(name)
    for code, name in absent:
        queries.append(f"{code}: {name}")
        expected.append(None)
    # Decided C07 probes, appended after the seeded draws (the sample is unchanged).
    for name in C07_ENTRIES:
        if name not in entries:
            entries.append(name)
    for query, want in C07_PROBES:
        queries.append(query)
        expected.append(want)
    return queries, entries, expected


def compare(queries: list[str], entries: list[str], show: int,
            expected: list[str | None] | None = None) -> dict:
    """Run one world through the unified engine (`oracle_io`) and Janitor's real
    ``_find_best_epg_match``, bucket every winner difference, and (when ``expected`` is
    given) score both sides against the known answer."""
    entry_ids = {name: 1000 + index for index, name in enumerate(entries)}
    janitor = oracles.get("janitor")
    janitor_rows = []
    for query in queries:
        ranked = janitor.match(query, entries, THRESHOLD)
        janitor_rows.append((ranked[0][0], ranked[0][1], ranked[0][2]) if ranked else None)

    request = {
        "threshold": THRESHOLD,
        "channels": [
            dict({"id": index + 1, "name": name},
                 **({"probe": janitor_rows[index][0]} if janitor_rows[index] else {}))
            for index, name in enumerate(queries)
        ],
        "entries": [{"id": entry_ids[name], "name": name} for name in entries],
    }
    proc = subprocess.run(
        [os.path.join(REPO, "target", "debug", "examples", "oracle_io")],
        input=json.dumps(request), cwd=REPO, capture_output=True, text=True, check=True,
    )
    rust_rows = {
        row["channel_id"]: row for row in json.loads(proc.stdout)["rows"]
    }

    buckets: dict[str, list[dict]] = {
        "C07-callsign-first": [],
        "D06-retired": [], "D02/T10-cut": [], "alias+selection": [],
        "decided-rows": [], "selection-rule": [], "zone-routing": [],
        "unattributed": [],
    }
    agreements = 0
    truth = {"positives": 0, "negatives": 0,
             "janitor_hit": 0, "unified_hit": 0,
             "janitor_wrong": 0, "unified_wrong": 0,
             "janitor_fp": 0, "unified_fp": 0}
    for index, query in enumerate(queries):
        j = janitor_rows[index]
        row = rust_rows[index + 1]
        u = None
        if row["winner_id"] is not None:
            u_name = entries[row["winner_id"] - 1000]
            u = (u_name, row["score"], row["method"])
        if expected is not None:
            want = expected[index]
            if want is None:
                truth["negatives"] += 1
                truth["janitor_fp"] += j is not None
                truth["unified_fp"] += u is not None
            else:
                truth["positives"] += 1
                truth["janitor_hit"] += bool(j and j[0] == want)
                truth["unified_hit"] += bool(u and u[0] == want)
                truth["janitor_wrong"] += bool(j and j[0] != want)
                truth["unified_wrong"] += bool(u and u[0] != want)
        j_id = entry_ids[j[0]] if j else None
        if j_id == row["winner_id"]:
            agreements += 1
            continue
        bucket = attribute(query, j, u, bool(row.get("probe_zone_split")))
        buckets.setdefault(bucket, buckets["unattributed"]).append({
            "query": query,
            "janitor": {"name": j[0] if j else None, "score": j[1] if j else 0,
                        "method": j[2] if j else None},
            "unified": {"name": u[0] if u else None, "score": u[1] if u else 0,
                        "method": u[2] if u else None},
        })
    return {
        "threshold": THRESHOLD,
        "queries": len(queries),
        "entries": len(entries),
        "agreements": agreements,
        "buckets": buckets,
        "truth": truth if expected is not None else None,
        "show": show,
    }


def build_oracle_io() -> None:
    subprocess.run(
        ["cargo", "build", "--locked", "-q", "-p", "crosstalk-guide",
         "--example", "oracle_io"],
        cwd=REPO,
        check=True,
    )


def fixture_section(show: int) -> dict:
    """Guide end-to-end on the disputed-pair fixture (where a migration diff can hide)."""
    queries, entries = fixture_names()
    return compare(queries, entries, show)


def representative_section(show: int) -> dict:
    """Guide end-to-end on the representative world (the bias check)."""
    queries, entries, expected = representative_names()
    return compare(queries, entries, show, expected)


BUCKETS = ("C07-callsign-first", "D06-retired", "D02/T10-cut", "alias+selection", "decided-rows",
           "selection-rule", "zone-routing", "unattributed")


def render_buckets(result: dict) -> list[str]:
    lines = [
        f"- {result['agreements']}/{result['queries']} queries agree on the winner",
        "",
        "### Differences by attribution",
        "",
    ]
    buckets = result["buckets"]
    for bucket in BUCKETS:
        lines.append(f"- `{bucket}`: {len(buckets.get(bucket, []))}")
    lines += [""]
    show = result["show"]
    for bucket in BUCKETS:
        items = buckets.get(bucket, [])
        if not items:
            continue
        lines += [f"#### {bucket} ({len(items)})", ""]
        for item in items[:show]:
            janitor = item["janitor"]
            unified = item["unified"]
            lines.append(
                f"- `{item['query']}`: janitor "
                f"{janitor['name']!r} ({janitor['score']}, {janitor['method']}) vs "
                f"unified {unified['name']!r} ({unified['score']}, {unified['method']})",
            )
        if len(items) > show:
            lines.append(f"  ... and {len(items) - show} more")
        lines += [""]
    return lines


def render(sweep: dict, sim: dict, fixture: dict, representative: dict) -> str:
    lines = [
        "<!-- Generated by scripts/oracle_report_guide.py; do not edit. -->",
        "<!-- Reproduce with: python3 -I scripts/oracle_report_guide.py -->",
        "# Guide oracle diff report (step 8)",
        "",
        "Guide runs the unified matching layer (one ranker to the best identity family,",
        "one selection rule — the family's first entry with programme data — one cut at",
        f"`[matching]` sensitivity, here {fixture['threshold']}); EPG Janitor is the oracle.",
        "Every difference below traces to a `data/corpus/decisions.json` row, a documented",
        "fix or the unified selection rule; the unattributed list is the review surface.",
        "",
        "Two end-to-end worlds: section 3 is REPRESENTATIVE (seeded, not mined — read its",
        "agreement rate as the migration's real-world size); section 4 is mined from the",
        "pairs where the plugins disagree, so its agreement rate is low by construction",
        "and it exists to show every kind of difference, not how often they happen.",
        "",
        "## Inputs",
        "",
        f"- cut (host `[matching]` sensitivity at strict): {fixture['threshold']}",
        f"- representative world: {representative['queries']} channels x "
        f"{representative['entries']} entries",
        "  (entries: a seeded sample of the US/UK/CA channel-database display names;",
        "  channels: provider-decorated copies of 240 of them — `HD`, `[FHD]`, `<CC>: `,",
        "  `(Backup)`, upper case, verbatim — plus 60 database names absent from the",
        "  listing as true negatives), plus three documented C07 probes: `ABC (WABC)` and",
        "  `US: NBC (KSDK) HD` over a listing holding the station and the bare national",
        "  entry (expected: the station), `CBS (WFOR) [HD]` with no WFOR entry (expected:",
        "  the national `CBS`, the fallback)",
        f"- disputed fixture: {fixture['queries']} queries x {fixture['entries']} entries",
        "  (queries mined from `data/corpus/pairs.jsonl`: pairs Janitor takes but another",
        "  plugin or the label disputes; entries are the pair candidates plus DB names;",
        "  plus one synthetic structural probe for the retired D06 mechanism)",
        "- Janitor oracle: `reference/Dispatcharr-EPG-Janitor-Plugin` `_find_best_epg_match`",
        "  with `allow_without_programs=True` (the oracle's own residual: every entry is",
        "  treated as carrying programme data, same as the harness)",
        "- `zone-routing` is attributed from the engine itself (`oracle_io`",
        "  `probe_zone_split`: the Z01 guard separates the channel from Janitor's winner),",
        "  never from a word list",
        "",
        "## 1. parity_sweep match/score (report mode)",
        "",
        "The retired EpgJanitor profile against Janitor's matcher as oracle tooling: guide",
        "no longer runs this code in production. Non-zero here is a port bug, not a",
        "migration diff.",
        "",
    ]
    if sweep["kinds"]:
        for kind in ("match", "score"):
            info = sweep["kinds"].get(kind, {})
            lines.append(
                f"- `{kind}`: {info.get('mismatches', '?')} mismatches out of "
                f"{info.get('cases', '?')} cases",
            )
    else:
        lines.append("- (could not parse the sweep summary; raw output retained below)")
    lines += ["", "## 2. sim_parity (report mode)", ""]
    if sim["files"]:
        bad = sum(1 for verdict in sim["files"].values() if verdict != "identical")
        lines.append(f"- {len(sim['files'])} file comparisons, {bad} differing")
        for name, verdict in sorted(sim["files"].items()):
            if verdict != "identical":
                lines.append(f"  - DIFFERS: {name}")
    else:
        lines.append("- (could not parse the sim summary; raw output retained below)")
    lines.append("- the comparisons above pin the Janitor ORACLE (`--features oracle`, "
                 "`simulate-streams --oracle janitor`); the production simulator runs the "
                 "unified engine:")
    lines.append(f"  - `{sim.get('unified') or '(no [unified] line)'}`")
    lines += [
        "  - reading the `[unified]` line: the simulator inverts channel-as-query results",
        "    into one `best_channel` per stream, ties in channel order, as `match_sim.py`",
        "    does. `only the Python tool passes` is the D02/T10 cut (`strict` 95 here",
        "    versus the tool's 80) and the decided normalizer rows (accents kept, N01-N15);",
        "    `different channel` splits in two: the `also pass to the Python tool's",
        "    channel` part is two spellings of ONE identity (decided A05 `canonical-key`:",
        "    `Cinemax Action`/`ActionMax`, `HBO2`/`HBO Hits`, `MGM+`/`Epix`, `GSN`/`Game",
        "    Show Network`, `CTN`/`Christian Television Network`), where the engine's family",
        "    holds the stream for both channels (streams attaches it to both; guide, ranking",
        "    with the channel as the query, leads with the channel's own spelling — the T12",
        "    same-spelling tie-break) and the column shows the first in channel order; the",
        "    rest are zone routing (Z01/Z08: `A&E (Pacific)` leaves bare `A&E`), accents",
        "    and punctuation kept by the decided normalizer (`UniMás`, `Real America’s",
        "    Voice`), the country marker read (`ScreenPix US`) and the alias table's own",
        "    canonicals (`WGN America` -> `NewsNation`). Known wrong family in the alias",
        "    class: `Al Jazeera (Arabic)` keys as `Al Jazeera English` because the decided",
        "    normalizer drops the parenthetical and the merged alias table lists bare `Al",
        "    Jazeera` under the English channel (phase 3 review notes, LEFT).",
    ]
    truth = representative["truth"]
    lines += [
        "",
        "## 3. guide end-to-end, representative world: unified engine vs Janitor",
        "",
        "Against the known answer (the entry each channel was decorated from):",
        "",
        "| | Janitor | unified |",
        "|---|---|---|",
        f"| decorated channels matched to their source ({truth['positives']}) | "
        f"{truth['janitor_hit']} | {truth['unified_hit']} |",
        f"| decorated channels matched to a different entry | "
        f"{truth['janitor_wrong']} | {truth['unified_wrong']} |",
        f"| absent channels matched to anything ({truth['negatives']}, false positives) | "
        f"{truth['janitor_fp']} | {truth['unified_fp']} |",
        "",
        "A \"different entry\" is not always wrong (a listing can carry the same channel",
        "twice under two names); the buckets below say why each one differs. The",
        "decorations are the provider noise both matchers are built to strip, so this",
        "world measures how often the migration changes an everyday answer; the hard",
        "cases (typos, zones, regional splits, callsign-only names) are section 4 and the",
        "labelled corpus (`docs/matching-decisions.md`).",
        "",
    ]
    lines += render_buckets(representative)
    lines += [
        "## 4. guide end-to-end, disputed fixture: unified engine vs Janitor",
        "",
    ]
    lines += render_buckets(fixture)
    lines += [
        "## Bucket notes",
        "",
        "- `C07-callsign-first` (orchestrator): a network-led station channel",
        "  (`ABC (WABC)`) takes the entry carrying its callsign; Janitor takes the bare",
        "  national entry on the stripped key. The national entry is still the answer",
        "  when the listing has no entry for the station (the fallback).",
        "- `D06-retired`: Janitor's structural callsign/state/city/network score (whole-path",
        "  comparison, no-evidence, sole implementation) is retired in step 8; callsign",
        "  agreement decides at family level through the engine anchor (C06/D04 `lu-anchor`).",
        "- `D02/T10-cut`: one cut at `[matching]` sensitivity (default strict = 95).",
        "  Janitor's confidence carries structural bonuses; the engine score does not, so",
        "  the two cuts admit different winners even when both rank the same top.",
        "- `alias+selection` (A01 `identity`, D07 `sm-collector`): alias lookups resolve",
        "  through CM's reverse index over the merged table, and the engine returns the",
        "  winner's whole family — including the one-country-per-family split (G03).",
        "- `decided-rows`: fuzzy-side disagreements under the decided configuration",
        "  (normalizer N01–N14, guards T01–T12, stages T03/T09, countries G01–G04); the",
        "  control is `scripts/corpus/engine_parity.py` + the corpus gates.",
        "- `selection-rule`: the family cut and its stable order (T12) vs the oracle's",
        "  per-entry stable score sort.",
        "- `zone-routing` (Z01 `sm-zone`): Janitor scores across feed zones (a `(Pacific)`",
        "  variant wins for a regionless query); the unified engine's zone guard (WEST vs",
        "  not-WEST on the routed zone) keeps the other-zone entry out of the family.",
        "  Reviewed (step 8): every row is a US East/West or Pacific feed split except the",
        "  BBC One regions (`BBC One West HD`, `BBC One South West HD`), where a UK region",
        "  word routes as WEST too — still the Z01 guard, and still a different channel",
        "  (a regional opt-out) from the plain `BBC One` the query names.",
        "",
    ]
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="regenerate and fail unless byte-identical to the report")
    parser.add_argument("--show", type=int, default=15, help="differences per bucket")
    args = parser.parse_args(argv)

    sweep = sweep_section()
    sim = sim_section()
    build_oracle_io()
    representative = representative_section(args.show)
    fixture = fixture_section(args.show)
    text = render(sweep, sim, fixture, representative)

    if args.check:
        with open(REPORT, encoding="utf-8") as fh:
            committed = fh.read()
        if committed != text:
            print(f"{REPORT} is stale: regenerate with", file=sys.stderr)
            print("  python3 -I scripts/oracle_report_guide.py", file=sys.stderr)
            return 1
        print(f"{REPORT} reproduces (representative: "
              f"{representative['agreements']}/{representative['queries']} agree; "
              f"disputed: {fixture['queries']} queries, "
              f"{fixture['agreements']} agreements, "
              f"{sum(len(items) for items in fixture['buckets'].values())} differences "
              f"attributed, "
              f"{len(fixture['buckets'].get('unattributed', []))} unattributed)")
        return 0

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(f"wrote {REPORT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
