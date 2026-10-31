# Crosstalk — Performance Audit (Baseline)

**Date:** 2026-10-31 · **Machine:** Apple M2 Pro, 12 cores, 16 GB · **Build:** release, `cargo 1.99.1`
**Companion to:** `AUDIT.md` (finding M8). This document is the *baseline*: every number below is
the pre-remediation value the roadmap (AUDIT.md §8, Phase 5) must beat, measured with the
harness described in §5.

> **Why now and not after remediation:** Phase 1–4 of the roadmap (heal walk, duplicate-name
> fix, concurrent validation) all change the workload. Measuring only afterwards could never
> prove the performance work helped — or catch it hurting. Measure → implement → re-measure.

---

## 1. Verdict

**Scoring is not the bottleneck. Serial network validation is — by an order of magnitude.**

At your target scale (20k channels × 5–20k guide entries), the rayon scoring stage takes
**~6–12 seconds**. The programme-data validation stage that follows it, done as serial
`await`s, takes **~100–200 seconds** at realistic LAN latency if most channels need a distinct
check. The AI gate has the same shape. Everything else — SQLite, web pages, startup, index
build — is noise by comparison.

| Stage | 20k channels × 5k entries | 20k × 20k | Dominated by |
|---|---|---|---|
| CandidateIndex build | 0.19 s | 0.69 s | normalize (35 µs/name) |
| Scoring (rayon, hard path) | ~10 s | ~12 s | Levenshtein allocations |
| Scoring (sequential, `simulate`) | ~35 s (extrap.) | ~140 s (extrap.) | single thread |
| **Programme validation (serial, today)** | **50–200 s** | **50–200 s** | **HTTP RTT × distinct entries** |
| Programme validation (concurrent @8, target) | ~7–25 s | ~7–25 s | client semaphore |
| AI gate (serial, today, if 10% of channels qualify) | ~100–600 s | same | provider RTT |
| SQLite record 20k rows | 0.04 s | — | — |
| **Total automatch today (typical)** | **~1–3 min** | **~2–4 min** | **validation** |
| **Total after Phase 5 (target)** | **< 30 s** | **< 45 s** | scoring |

So: **do the perf audit now (done), implement in roadmap order, re-audit after Phase 5.**
The one perf fix that should *not* wait for Phase 5 is concurrent validation — it's a
correctness-adjacent reliability issue too (serial awaits stretch the window in which a
cancel/shutdown can strand a run, AUDIT H5).

---

## 2. Measured baselines

### 2.1 Primitives (harness: temp crate against `crosstalk-matching`, real US/UK/CA corpus, 35,349 unique names)

| Operation | Throughput | Per call | Notes |
|---|---|---|---|
| `normalize_name` | 36,600 /s | 27 µs | Expensive for a string function; regex-table driven |
| `normalize_name` + 4 janitor tags | 27,900 /s | 36 µs | Tag path costs +33% (tag-regex cache lock ×4/call) |
| `calculate_similarity` (short names) | 1,690,000 /s | 0.6 µs | |
| `calculate_similarity` (long, similar) | 292,000 /s | 3.4 µs | Full DP, no early exit benefit |
| `CandidateIndex::build` | ~29,000 names/s | 35 µs/name | 100→13 ms, 1k→35 ms, 5k→188 ms, 20k→693 ms |

### 2.2 Pipeline (queries × candidates, pairs/s)

| Shape | Sequential | Rayon (12 threads) | Speedup |
|---|---|---|---|
| `match_all_streams`, disjoint names, 5q × 20k | 2.42 M pairs/s | — | — |
| `match_all_streams`, hard path (HD variants), 200q × 5k | 1.95 M pairs/s | — | — |
| `score_candidates`, 10q × 5k | 2.57 M pairs/s | 9.15 M pairs/s | 3.6× |
| `score_candidates`, hard path, 200q × 5k | — | 10.3 M pairs/s | — |
| `score_candidates`, hard path, 100q × 20k | — | 32.8 M pairs/s* | — |

\* Higher pairs/s at 20k candidates because most pairs die at the length/guard pre-checks;
per-channel cost is what matters: **0.29–0.48 ms/channel at 5k entries → 20k channels ≈ 6–10 s;
≈12 s at 20k entries.**

**Rayon only achieves 2.0–3.6× on 12 cores.** Suspects, in likely order: (a) allocation churn
synchronising the allocator, (b) the two `Mutex`-guarded regex caches in the pipeline
(`pipeline.rs:975-990` `contains_abbrev`, `:822-833` channel-boost) held across `is_match`,
(c) short per-query work with high variance. Worth a profile before Phase 5, not a guess.

### 2.3 End-to-end `crosstalk simulate` (release binary, sequential — no rayon in this path)

| Corpus | Wall | Peak RSS |
|---|---|---|
| 500 × 500 | 0.77 s | 33 MB |
| 2,000 × 2,000 | 2.13 s | 36 MB |
| 5,000 × 5,000 | 13.7 s | 37 MB |
| 35,349 × 9,573 (prior session) | ~129 s | — |

Memory is flat and healthy. **`simulate` not using rayon is itself a finding** (P4): the CLI
oracle is 3–4× slower than it needs to be, which discourages running the big parity corpus.

### 2.4 Validation latency (the actual bottleneck)

Local fake server, 5 ms artificial delay (≈9 ms real RTT with overhead):
**20 sequential programme checks = 188 ms.**

| Distinct checks needed | Serial (today) | Concurrent @8 (target) |
|---|---|---|
| 1,000 | ~9 s | ~1.2 s |
| 5,000 | ~45 s | ~6 s |
| 20,000 | ~180 s | ~23 s |

Automatch stage 2 (`automatch.rs:198-327`), heal step 1 (`heal.rs:138-161`) and the AI gate
(`automatch.rs:241`) all `await` one request at a time inside the per-channel loop. The
client's semaphore (`dispatcharr.rs`, cap 8) can never be contended by a single serial caller
— it only helps if callers are spawned concurrently. Memoisation (`DashMap` per
`(tvg_id, source, window)`) caps the damage at *distinct* entries, which on a first run
against a fresh source is close to *every* matched channel.

### 2.5 Store, daemon, artifacts (all healthy)

| Item | Measured |
|---|---|
| `append_rows` 20k rows, one transaction | 36 ms (40×500 chunks: 57 ms) |
| `rows()` read 20k | 8.7 ms |
| DB size, 20k rows | 2.3 MB (~113 B/row, before AI payloads) |
| Daemon startup → `/health` | 36 ms |
| Page latency (dashboard / app / settings / runs / API) | 0.7–3.3 ms |
| Release binary | 21 MB |
| Docker image | 46 MB |
| Peak RSS, 5k×5k simulate | 37 MB |

SQLite is not a problem at this scale. The M1 retention issue in AUDIT.md is about *growth*
(15-day prune per Q16), not speed.

---

## 3. Findings (prioritized)

### P1 — Serial programme validation dominates every real run
`automatch.rs:198+`, `heal.rs:138+`. See §2.4. **Fix in Phase 3, not Phase 5:**
`buffer_unordered(client_concurrency)` over channels needing checks; keep memoisation.
*Acceptance: 5,000 distinct checks < 10 s against a 5 ms-delay fake.*

### P2 — AI gate is serial and multiplies P1 when enabled
`automatch.rs:241` awaits one provider request per qualifying channel. At Jev-class latency
(70–500 ms per the TypeSafe docs), 2,000 qualifying channels = **2–17 minutes** serial.
*Fix: same concurrent fan-out, bounded by the provider's `max_concurrency` (8).*
*Acceptance: 500 AI decisions < 30 s against a 200 ms-delay fake.*

### P3 — Heal blocks a Tokio worker for its whole scoring loop
`heal.rs:190-268` runs O(broken × entries) synchronously in `async fn` — unlike automatch's
`spawn_blocking`+rayon (`automatch.rs:161`). On the daemon this stalls an HTTP worker for
the duration (page latency above was measured *without* a heal running).
*Fix: `spawn_blocking` + rayon over broken channels, same as automatch.*
*Acceptance: `/health` stays < 50 ms during a 5k-broken-channel heal.*

### P4 — `simulate` (the parity oracle) is single-threaded
`crates/crosstalk/src/simulate.rs` loops channels sequentially: 25M pairs in 13.7 s where
the rayon path would take ~4 s. The oracle being slow is why the big corpus is run rarely.
*Fix: rayon over channels (results are per-stream reductions — needs an ordered merge to
stay byte-identical).* ⚠️ Byte-identical output is the oracle's whole value: parallelise
only with a determinism test. *Acceptance: 5k×5k < 5 s, output `cmp`-identical.*

### P5 — Allocation churn in the similarity hot loop caps rayon scaling
`similarity.rs:29-30` collects both strings into `Vec<char>` per call (+ 2 row vecs), up to
3 calls/pair + 1/alias. `score_candidates` adds per-pair `to_uppercase()`
(`pipeline.rs:889-890,902`) and rebuilds the query token set per matched candidate
(`pipeline.rs:966-973`, called at `:800`). Rayon scaling of 2–3.6× on 12 cores says the
ceiling is allocation/locking, not DP math.
*Fix: precompute `Vec<char>` (and lengths) in `CandidateIndex`; hoist query-side sets out of
the candidate loop; replace the two mutex-guarded regex caches with precompiled statics.*
*Acceptance: rayon speedup ≥ 6× on the hard-path bench; hard-path ≥ 15 M pairs/s.*

### P6 — CandidateIndex is rebuilt from scratch every run (0.7 s @ 20k entries)
Plus query-side `normalize` per channel (27–36 µs × 20k ≈ 0.6–0.7 s). Not dominant, but
it's pure repeated work when the entry list hasn't changed between runs.
*Fix (optional, after P5): cache the index keyed by (entry-set hash, flags, alias hash) in
the daemon.* *Acceptance: second consecutive run's build stage < 50 ms.*

### P7 — No progress during automatch scoring stage
`automatch.rs:161-176`: the rayon stage emits nothing until it finishes (the `counter`
atomic is incremented and never read). At 20k×20k that's ~12 s of UI silence — tolerable;
at sequential speeds it wouldn't be. *Fix: report from the atomic every ~500 channels via
the existing paced sink.*

### Non-findings (checked, healthy)
Binary size (21 MB), image size (46 MB), startup (36 ms), page latency (≤3.3 ms idle),
SQLite write/read at 20k rows, peak memory (37 MB @ 5k×5k), index build (<0.7 s @ 20k).
No perf work needed in any of these.

---

## 4. Re-audit protocol (after roadmap Phase 5)

Re-run, on the same machine class, and compare against §2:

1. `/tmp/perfbench` harness (primitives + hard-path pipeline) — targets: normalize ≥ 60k/s
   with tags, rayon speedup ≥ 6×, hard-path score ≥ 15 M pairs/s.
2. `crosstalk simulate` 5k×5k — target < 5 s, output still `cmp`-identical to Python.
3. Fake-Dispatcharr latency test (5 ms delay) — 5k distinct programme checks < 10 s;
   500 AI decisions (200 ms delay) < 30 s; `/health` < 50 ms during a heal.
4. End-to-end automatch on the live instance (AUDIT roadmap Phase 7): 20k channels,
   target < 30 s preview wall time.

If a target is missed, the re-audit files it as a new finding rather than re-litigating
this document.

## 5. Harness provenance

Benchmarks were run from a throwaway crate in `/tmp/perfbench` (path deps on
`crosstalk-matching`/`crosstalk-core`), release profile, on the machine above. It is *not*
committed — **recommendation: commit it as `crates/crosstalk-matching/benches/` (criterion)
in Phase 5**, or the re-audit in §4 will have the same stale-binary problem as
`parity_sweep.py` (AUDIT H3). Numbers from a single run each; treat ±20% as noise.
