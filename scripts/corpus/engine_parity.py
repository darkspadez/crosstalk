#!/usr/bin/env python3
"""Corpus gate for the unified Rust engine (plan step 6).

Runs the decided Python configuration from ``ablate.py`` (``Engine`` built from
``data/corpus/decisions.json`` ``engine.config``) and the Rust engine
(``crates/crosstalk-matching/examples/engine_io.rs``) over the whole corpus --
and over the normalize / callsign / alias kinds -- comparing:

* normalize keys,
* callsigns (suffix-stripped base + positional confidence),
* alias hits (reverse-index canonicals),
* identities (family key: key, zone, timeshift, callsign; plus country),
* ``same`` verdicts,
* family membership and scores (``rank``): over small distractor sets AND over
  real candidate pools (every corpus name sharing a normalized token with the
  query, the shape a provider's channel list has).

The Python family is ``match_all_streams`` (the decided configuration) with
``Engine.same``'s post-filters, then the step-6 identity family cut (the plan's
"best identity family", which ablate.py has no notion of): the top's family
key (normalize key, ``sm_zone_noguard``, ``+N`` shift, callsign base) and ONE
country (the query's Lineuparr stream marker, else the top's, else the first
known among its key-mates; unknown is a wildcard; callsign families ignore
country; US<->MX shared feeds stay compatible) -- written from the identity
rule in ``crates/crosstalk-matching/src/identity.rs``, not from the Rust code.

Target: 0 mismatches. Any residual must be explained (float rounding R01 is
avoided by construction: both sides port the pure-Python fallback arithmetic).

Also scores the Rust engine on the construction labels and asserts its cost
(2*FP+FN) equals ``decide.py``'s reported engine cost (``engine.vs_plugins``).

Usage:
    python3 -I scripts/corpus/engine_parity.py [--limit N] [--kinds same,family]
        [--threshold 80] [--family-queries N] [--show N]

    --limit N         check only the first N corpus pairs (CI subset).
    --kinds ...       comma list of: normalize,callsign,alias,same,family.
    --threshold T     decision cut both engines score at (default 80: the cut
                      decisions.json reports the engine cost at).
    --family-queries N  distractor-set rank checks to run (default 400).
    --pool-queries N    real-pool rank checks to run (default 1500).
    --pool-size N       real-pool cap per query (default 60).
    --show N          mismatches printed per kind (default 10).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ablate  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CORPUS = os.path.join(REPO, "data", "corpus")
ENGINE_IO = ["cargo", "run", "--locked", "-q", "-p", "crosstalk-matching",
             "--example", "engine_io"]
CHUNK = 2000
#: Pipe decorations the `pipe` kind wraps every corpus name in (decided N15).
PIPE_SHAPES = ("{}", "{} | HD |", "HD | {}", "UK | {} | FHD", "{} | US", "RK: US | {}",
               "| {} | [4K] |", "{} | 1080p | +1")

#: Worker-local decided engine for --jobs parallelism (built once per process).
_WORKER: dict = {}


def _init_worker(config: dict, threshold: int, alias_map: dict) -> None:
    _WORKER["engine"] = ablate.Engine(config, threshold, alias_map)
    _WORKER["threshold"] = threshold


def _same_job(job: tuple[int, str, str]) -> tuple[int, bool]:
    index, query, candidate = job
    return index, _WORKER["engine"].same(query, candidate)


def _family_job(job: tuple[int, str, list[str]]) -> tuple[int, list]:
    index, query, candidates = job
    engine = _WORKER["engine"]
    return index, python_family(engine, _WORKER["threshold"], query, candidates)


def load_decisions() -> dict:
    with open(os.path.join(CORPUS, "decisions.json"), encoding="utf-8") as handle:
        return json.load(handle)


def load_pairs(limit: int | None) -> list[dict]:
    pairs = []
    with open(os.path.join(CORPUS, "pairs.jsonl"), encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                pairs.append(json.loads(line))
    if limit is not None:
        pairs = pairs[:limit]
    return pairs


class RustEngine:
    """Batching client for the engine_io example (one process, many lines)."""

    def __init__(self, threshold: int):
        self.threshold = threshold
        self.proc = subprocess.Popen(
            ENGINE_IO, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            text=True, cwd=REPO)
        assert self.proc.stdin and self.proc.stdout

    def batch(self, requests: list[dict]) -> list[dict]:
        """Send every request and read every response.

        Requests are written from a thread: a chunk of large rank requests is
        bigger than the pipe buffers, and writing it all before reading would
        deadlock once the engine blocks on a full stdout pipe."""
        import threading
        assert self.proc.stdin and self.proc.stdout
        stdin = self.proc.stdin

        def write():
            for start in range(0, len(requests), CHUNK):
                stdin.write("".join(json.dumps(r) + "\n"
                                    for r in requests[start:start + CHUNK]))
                stdin.flush()

        writer = threading.Thread(target=write, daemon=True)
        writer.start()
        out = [json.loads(self.proc.stdout.readline()) for _ in requests]
        writer.join()
        return out

    def close(self):
        assert self.proc.stdin
        self.proc.stdin.close()
        self.proc.wait()


def normalize_callsign_base(callsign: str | None) -> str | None:
    if not callsign:
        return None
    return re.sub(r"-(?:TV|CD|LP|DT|LD)$", "", callsign)


def python_alias_resolve(engine: ablate.Engine, name: str) -> str | None:
    """A01 `identity` resolve, as ablate.Engine._install_alias builds it."""
    index = engine.matcher._reverse_alias_index
    norm = (engine.matcher.normalize_name(name, []) or "").lower()
    if not norm:
        return None
    return index.get(norm) or index.get(re.sub(r"[\s&\-]+", "", norm))


def python_identity(engine: ablate.Engine, name: str) -> dict:
    """The identity fields a family compares on, from the decided configuration."""
    with ablate._Swapped(engine.mods["core"], engine.swaps):
        raw, _high = engine.matcher._extract_callsign_with_confidence(name)
    return {
        "key": engine.normalize(name).lower(),
        "zone": ablate.sm_zone_noguard(engine.mods, name),
        "timeshift": bool(ablate._PLUS_SHIFT_RE.search(name)),
        "callsign": normalize_callsign_base(raw),
        # Decided N15: the country reads the pipe-collapsed name, like the Rust
        # `lineuparr_stream_country` does internally.
        "country": engine.mods["lu"].detect_stream_country(
            ablate.collapse_pipe_segments(name)),
    }


def _family_key(identity: dict) -> tuple:
    return (identity["key"], identity["zone"], identity["timeshift"],
            identity["callsign"])


def _countries_compatible(engine: ablate.Engine, a, b, exempt: bool, key: str) -> bool:
    """identity.rs `countries_compatible`, via Lineuparr's own shared-feed table."""
    if exempt or a is None or b is None or a == b:
        return True
    lu = engine.mods["lu"]
    shared = lu._CROSS_BORDER_SHARED.get(frozenset({a, b}))
    return bool(shared and lu._fold_key(key) in shared)


def python_family(engine: ablate.Engine, threshold: int, query: str,
                  candidates: list[str]) -> list[tuple[str, int, str]]:
    """Family membership + scores from the decided Python configuration.

    Each candidate scored by ``Engine.score`` (the decided configuration's
    pair verdict: ``match_all_streams`` on that one candidate plus the
    post-filters), kept at the cut, ranked as ``match_all_streams`` ranks
    (score desc, then original-token overlap with the query, stable in
    candidate order -- T12 `lu`) with the step-9 tie-break in front of the
    overlap (a candidate whose identity key IS the query's wins a tie, so a
    reordered name scoring 100 by token sort cannot become the top), then the
    identity family cut.

    Not one ``match_all_streams`` call over the whole set: the base patches
    leave LU's regionless branch in place, which over a SET drops West/Pacific
    candidates whenever a regionless one survives -- a residue of the patching
    (ZONE is the post-filter), not part of the decided configuration.
    """
    query_tokens = set(re.findall(r"[a-z0-9]+", (query or "").lower()))
    query_key = engine.normalize(query).lower()
    scored = []
    for position, name in enumerate(candidates):
        hit = engine.score(query, name)
        if hit is None or hit[0] < threshold:
            continue
        overlap = len(query_tokens & set(re.findall(r"[a-z0-9]+", name.lower())))
        equal = engine.normalize(name).lower() == query_key
        scored.append((-hit[0], -int(equal), -overlap, position, name, hit[0], hit[1]))
    scored.sort()
    family = [(name, score, method) for *_key, name, score, method in scored]
    if not family:
        return family
    ids = {name: python_identity(engine, name) for name, _s, _m in family}
    query_country = engine.mods["lu"].detect_stream_country(
        ablate.collapse_pipe_segments(query))

    def compatible(country, name):
        identity = ids[name]
        return _countries_compatible(engine, country, identity["country"],
                                     identity["callsign"] is not None, identity["key"])

    family = [entry for entry in family if compatible(query_country, entry[0])]
    if not family:
        return family
    top_key = _family_key(ids[family[0][0]])
    mates = [entry for entry in family if _family_key(ids[entry[0]]) == top_key]
    family_country = query_country or next(
        (ids[name]["country"] for name, _s, _m in mates if ids[name]["country"]), None)
    return [entry for entry in mates if compatible(family_country, entry[0])]


def report(kind: str, total: int, mismatches: list[str], show: int) -> bool:
    print(f"  {kind}: {total} checked, {len(mismatches)} mismatches", flush=True)
    for mismatch in mismatches[:show]:
        print(f"    MISMATCH {mismatch}", flush=True)
    return not mismatches


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--kinds",
                        default="normalize,callsign,alias,identity,pipe,same,family")
    parser.add_argument("--threshold", type=int, default=80)
    parser.add_argument("--family-queries", type=int, default=400)
    parser.add_argument("--pool-queries", type=int, default=1500)
    parser.add_argument("--pool-size", type=int, default=60)
    parser.add_argument("--show", type=int, default=10)
    parser.add_argument("--jobs", type=int, default=0,
                        help="Python-side workers for same/family (0 = serial)")
    args = parser.parse_args(argv)

    decisions = load_decisions()
    config = decisions["engine"]["config"]
    expected_cost = decisions["engine"]["vs_plugins"]["engine"]["cost"]
    kinds = {kind.strip() for kind in args.kinds.split(",") if kind.strip()}

    pairs = load_pairs(args.limit)
    print(f"engine_parity: {len(pairs)} pairs at cut {args.threshold} "
          f"(expected construction cost {expected_cost})", flush=True)

    alias_map = ablate._merged_alias_map()
    engine = ablate.Engine(config, args.threshold, alias_map)
    rust = RustEngine(args.threshold)
    ok = True

    names = sorted({name for pair in pairs for name in (pair["query"], pair["candidate"])})

    if "normalize" in kinds:
        requests = [{"kind": "normalize", "name": name, "threshold": args.threshold}
                    for name in names]
        mismatches = []
        for name, response in zip(names, rust.batch(requests)):
            expected = engine.normalize(name)
            if response.get("normalized") != expected:
                mismatches.append(f"normalize {name!r}: rust {response.get('normalized')!r} "
                                  f"!= python {expected!r}")
        ok &= report("normalize", len(names), mismatches, args.show)

    if "callsign" in kinds:
        requests = [{"kind": "callsign", "name": name, "threshold": args.threshold}
                    for name in names]
        mismatches = []
        for name, response in zip(names, rust.batch(requests)):
            with ablate._Swapped(engine.mods["core"], engine.swaps):
                raw, high = engine.matcher._extract_callsign_with_confidence(name)
            expected = (normalize_callsign_base(raw), bool(high))
            got = (response.get("normalized"), bool(response.get("high_confidence")))
            if got != expected:
                mismatches.append(f"callsign {name!r}: rust {got!r} != python {expected!r} "
                                  f"(raw {raw!r})")
        ok &= report("callsign", len(names), mismatches, args.show)

    if "alias" in kinds:
        requests = [{"kind": "alias", "name": name, "threshold": args.threshold}
                    for name in names]
        mismatches = []
        for name, response in zip(names, rust.batch(requests)):
            with ablate._Swapped(engine.mods["core"], engine.swaps):
                expected = python_alias_resolve(engine, name)
            if response.get("canonical") != expected:
                mismatches.append(f"alias {name!r}: rust {response.get('canonical')!r} "
                                  f"!= python {expected!r}")
        ok &= report("alias", len(names), mismatches, args.show)

    if "identity" in kinds:
        requests = [{"kind": "identity", "name": name, "threshold": args.threshold}
                    for name in names]
        mismatches = []
        for name, response in zip(names, rust.batch(requests)):
            expected = python_identity(engine, name)
            got = {field: response.get(field) for field in expected}
            if got != expected:
                diff = {field: (got[field], expected[field]) for field in expected
                        if got[field] != expected[field]}
                mismatches.append(f"identity {name!r}: (rust, python) {diff!r}")
        ok &= report("identity", len(names), mismatches, args.show)

    if "pipe" in kinds:
        # Decided N15: the collapse itself, on every name bare and in the pipe
        # shapes providers use (junk segments, a leading/trailing country code).
        battery = sorted({shape.format(name) for name in names for shape in PIPE_SHAPES})
        requests = [{"kind": "pipe", "name": name} for name in battery]
        mismatches = []
        for name, response in zip(battery, rust.batch(requests)):
            expected = ablate.collapse_pipe_segments(name)
            if response.get("collapsed") != expected:
                mismatches.append(f"pipe {name!r}: rust {response.get('collapsed')!r} "
                                  f"!= python {expected!r}")
        ok &= report("pipe", len(battery), mismatches, args.show)

    rust_same: list[bool] = []
    if "same" in kinds:
        requests = [{"kind": "same", "query": pair["query"], "candidate": pair["candidate"],
                     "threshold": args.threshold} for pair in pairs]
        responses = rust.batch(requests)
        if args.jobs and len(pairs) > 200:
            from concurrent.futures import ProcessPoolExecutor
            jobs = [(index, pair["query"], pair["candidate"])
                    for index, pair in enumerate(pairs)]
            with ProcessPoolExecutor(max_workers=args.jobs,
                                      initializer=_init_worker,
                                      initargs=(config, args.threshold, alias_map)) as pool:
                expected_verdicts = dict(pool.map(_same_job, jobs, chunksize=50))
        else:
            expected_verdicts = {index: engine.same(pair["query"], pair["candidate"])
                                 for index, pair in enumerate(pairs)}
        mismatches = []
        for index, (pair, response) in enumerate(zip(pairs, responses)):
            expected = expected_verdicts[index]
            got = bool(response.get("same"))
            rust_same.append(got)
            if got != expected:
                mismatches.append(f"same {pair['query']!r} vs {pair['candidate']!r} "
                                  f"(label {pair.get('label')}/{pair.get('label_source')}): "
                                  f"rust {got} (score {response.get('score')} "
                                  f"{response.get('method')}) != python {expected}")
        ok &= report("same", len(pairs), mismatches, args.show)
    else:
        rust_same = []

    if "family" in kinds:
        family_jobs: list[tuple[str, str, list[str]]] = []
        truthy: dict[str, list[str]] = {}
        for pair in pairs:
            truthy.setdefault(pair["query"], []).append(pair["candidate"])
        queries = list(truthy)
        # (a) Deterministic distractors: other pairs' candidates, round-robin.
        pool = [pair["candidate"] for pair in pairs]
        for query in queries[:args.family_queries]:
            candidates = [c for c in pool if c != query][:7]
            for name in truthy[query]:
                if name not in candidates:
                    candidates.append(name)
            # Unique names only: the Python path keys matches by NAME (one entry
            # per name), while the Rust rank answers per candidate index (two
            # streams sharing a name are two family members). Dedup here so the
            # comparison is membership + scores, not index cardinality.
            family_jobs.append(("distractor", query, list(dict.fromkeys(candidates[:8]))))
        # (b) Real pools: every corpus name sharing a normalized token with the
        # query (a provider list's shape: many near-misses, country/zone/shift
        # siblings), capped deterministically, plus the query's own candidates.
        by_token: dict[str, list[str]] = {}
        for name in names:
            for token in set(engine.normalize(name).lower().split()):
                by_token.setdefault(token, []).append(name)
        step = max(1, len(queries) // max(1, args.pool_queries))
        for query in queries[::step][:args.pool_queries]:
            shared: list[str] = []
            for token in sorted(set(engine.normalize(query).lower().split())):
                shared.extend(by_token.get(token, ()))
            shared = sorted(dict.fromkeys(n for n in shared if n != query))
            # Spread the cap across the sorted pool instead of its head.
            if len(shared) > args.pool_size:
                stride = len(shared) / args.pool_size
                shared = [shared[int(i * stride)] for i in range(args.pool_size)]
            candidates = list(dict.fromkeys(shared + truthy[query]))
            family_jobs.append(("pool", query, candidates))
        responses = rust.batch([{"kind": "rank", "query": query,
                                 "candidates": candidates,
                                 "threshold": args.threshold}
                                for _shape, query, candidates in family_jobs])
        jobs = [(i, query, candidates)
                for i, (_shape, query, candidates) in enumerate(family_jobs)]
        if args.jobs and len(jobs) > 20:
            from concurrent.futures import ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=args.jobs,
                                      initializer=_init_worker,
                                      initargs=(config, args.threshold, alias_map)) as pool_exec:
                expected_families = dict(pool_exec.map(_family_job, jobs, chunksize=10))
        else:
            expected_families = {i: python_family(engine, args.threshold, query, candidates)
                                 for i, query, candidates in jobs}
        for shape in ("distractor", "pool"):
            mismatches = []
            checked = members = multi = 0
            for i, ((job_shape, query, candidates), response) in enumerate(
                    zip(family_jobs, responses)):
                if job_shape != shape:
                    continue
                expected = expected_families[i]
                got = [(candidates[entry[0]], entry[1], entry[2])
                       for entry in response.get("family", [])]
                checked += 1
                members += len(expected)
                multi += len(expected) > 1
                if [entry[0] for entry in got] != [entry[0] for entry in expected]:
                    mismatches.append(f"family[{shape}] {query!r}: rust "
                                      f"{[e[0] for e in got]!r} != python "
                                      f"{[e[0] for e in expected]!r}")
                    continue
                for (g_name, g_score, _), (_e_name, e_score, _) in zip(got, expected):
                    if g_score != e_score:
                        mismatches.append(f"family[{shape}] {query!r} {g_name!r}: rust "
                                          f"score {g_score} != python {e_score}")
                        break
            ok &= report(f"family[{shape}] ({members} members, {multi} multi-member)",
                         checked, mismatches, args.show)

    # Construction cost of the RUST engine at this cut.
    construction = [pair for pair in pairs
                    if pair.get("label_source") == "construction"
                    and pair.get("label") in ("same", "different")]
    if construction and "same" in kinds:
        index = {id(pair): verdict for pair, verdict in zip(pairs, rust_same)}
        tp = sum(1 for pair in construction
                 if index[id(pair)] and pair["label"] == "same")
        fp = sum(1 for pair in construction
                 if index[id(pair)] and pair["label"] == "different")
        fn = sum(1 for pair in construction
                 if not index[id(pair)] and pair["label"] == "same")
        tn = sum(1 for pair in construction
                 if not index[id(pair)] and pair["label"] == "different")
        cost = 2 * fp + fn
        print(f"  construction cost (rust): {cost} (TP {tp}, FP {fp}, FN {fn}, TN {tn}) "
              f"over {len(construction)} pairs", flush=True)
        if args.limit is not None:
            print("  (subset run: cost assert skipped)", flush=True)
        elif args.threshold != ablate.THRESHOLDS[0]:
            print(f"  (cut {args.threshold}: decide.py reports the cost at cut "
                  f"{ablate.THRESHOLDS[0]}; cost assert skipped)", flush=True)
        elif cost != expected_cost:
            print(f"  COST DIVERGENCE: rust {cost} != decide.py {expected_cost}", flush=True)
            ok = False

    rust.close()
    print("engine_parity: " + ("OK" if ok else "MISMATCHES"), flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
