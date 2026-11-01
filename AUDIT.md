# Crosstalk — Full Project Audit

**Date:** 2026-10-31 (system clock)
**Scope:** All 5 crates (18,840 LOC Rust), config, Docker/compose, CI, scripts, README/PLAN
**Method:** Three independent read-only audit passes (security/web, matcher/parity/performance, infra/ops) plus first-hand re-verification of every headline claim.
**Baseline health:** `cargo test --workspace` — **312 passed, 0 failed**. `cargo clippy -- -D warnings` — clean. `cargo fmt --check` — clean.

---

## 1. Executive verdict

**The matcher core is genuinely good. The product around it is not yet safe to run unattended.**

The ported pipeline (`crosstalk-matching`) is a faithful, well-tested port and the byte-identical
`match_sim` oracle does pass — in the one configuration it tests. Everything the previous session
claimed about *primitives* is true. Almost everything it implied about *end-to-end behaviour* is
unproven, and in a few places false.

Ship-readiness by use case:

| Use case | Verdict |
|---|---|
| `crosstalk simulate` (offline tuning) | ✅ Ship it |
| `crosstalk run janitor *_preview` by hand, loopback | ✅ Ship it |
| `crosstalk run janitor *_apply --yes` by hand | ⚠️ Usable with backups; see H4 |
| Daemon + web UI on loopback, no token (shipped default) | ❌ **Not safe** — see H1 (CSRF) |
| Daemon on LAN / scheduled applies unattended | ❌ **Not yet** — H1, H4, H5, M3 |
| Docker per README quick-start | ❌ **Broken as written** — M6 |

The single most important sentence in this audit: **the parity gate that justifies the whole
rewrite cannot be run from a clean checkout and never runs in CI** (H3). The second:
**a tokenless loopback daemon can be driven to overwrite EPG assignments by any web page the
operator visits** (H1).

---

## 2. Scorecard

| Area | Grade | One-line summary |
|---|---|---|
| Matcher primitives (`similarity`, `tokens`, `normalize`, `callsign`) | A− | Verified against Python; excellent Unicode/`pyre` work |
| Pipeline (`match_all_streams`, scoring) | B+ | Sound; duplicate-name flaw (H7); perf ceiling (M8) |
| Auto-match action | C+ | Diverges from plugin in walk/reporting; AI path changes contract |
| Scan & heal action | **D** | Low-confidence logic is dead code (H6) — materially wrong output |
| Parity evidence / test infrastructure | **D** | Oracle narrow; sweep unreproducible; CI gate always skips (H3) |
| Dispatcharr client | B | Good GET path; write path fragile (H4) |
| Config system | C | Brilliant env-secret refusal; silent typos undo it (H8) |
| Web UI / auth | C− | Escaping good; CSRF + stale-config + no headers (H1, M4) |
| Runner / store / scheduler | C | Panic safety real; prune dead, shutdown incomplete (H5, M1) |
| AI layer | B− | Clean trait/backends; gate semantics + serial calls need work (M5) |
| Docker / packaging | C− | Works when hand-driven; README path fails; context leak (M6, M7) |
| Docs (README/PLAN) | C | Mostly accurate; parity and Docker claims overbroad |

---

## 3. HIGH — fix before any unattended or networked use

### H1. CSRF: any web page can trigger a destructive apply on a tokenless loopback daemon
- **Where:** `crates/crosstalk-web/src/routes.rs:24,30,32,33` (all POSTs), `start_action`
  (`routes.rs:228-252`); `crates/crosstalk-web/src/auth.rs:91-92` (no token ⇒ pass-through);
  confirm is client-side only (`templates/app.html:38`).
- **Verified:** No CSRF token, `Origin`/`Referer` check, or server-side confirm anywhere. An
  omitted `dry_run` form field on a channel-touching action means **apply** (`dry_run=false`).
- **Exploit:** Operator runs the shipped default (loopback, no token) and visits any page. The
  page auto-submits a cross-origin form POST to
  `http://127.0.0.1:8730/apps/janitor/actions/automatch_apply`. It's a CORS "simple request" —
  no preflight, response readability irrelevant, the side effect is the attack. It can first
  POST `/settings/janitor` (e.g. threshold → 1). DNS-rebinding and any local process work too;
  there is no `Host` allowlist.
- **Partial mitigation:** With a token, the cookie is `SameSite=Strict`, which blunts cross-site
  cookie POSTs in modern browsers. The no-token mode (the default the README leads with) has
  nothing. Contrast: the CLI refuses applies without `--yes` — the web path is *weaker* than
  the CLI path, which is backwards.
- **Fix:** Per-session CSRF token on every form (incl. htmx), reject POSTs without it; require
  a server-side `confirm=true` for `Write`/`Destructive` actions mirroring `--yes`.

### H2. Heal's `SKIPPED_LOW_CONFIDENCE` path is dead code — heal reports are materially wrong
- **Where:** `crates/janitor/src/heal.rs:223` (`if candidate.score < threshold { break; }`),
  making the `SkippedLowConfidence` branch at `heal.rs:255` unreachable.
- **Verified by reading:** Candidates are score-descending, so the loop breaks before ever
  recording a sub-threshold candidate. A channel whose only working replacement scores 60–94 is
  reported as `NO_REPLACEMENT_FOUND`, confidence 0, no replacement name — and the summary counts
  ("replacements found", "would be applied") inherit the error.
- **Python does the opposite:** `_find_best_epg_match` returns the first candidate *with
  programmes regardless of threshold* (`plugin.py:1252-1289`); heal then labels it
  `SKIPPED_LOW_CONFIDENCE` / `REPLACEMENT_PREVIEW` with its real score (`plugin.py:1497-1534`).
- **Why tests missed it:** The only test of this logic (`heal.rs:725`) calls `reason_for` on a
  hand-built row. No end-to-end heal test has a sub-threshold replacement.
- **Fix:** Walk all candidates; remember the first programme-carrying one; apply the threshold
  only when deciding `Healed` vs `SkippedLowConfidence`. Add an E2E test at score ~80.

### H3. The parity infrastructure cannot be run from a clean checkout and never runs in CI
- **Verified first-hand:**
  - `scripts/parity_sweep.py:161-172` builds `--example normalize_io`, but
    `crates/crosstalk-matching/examples/` **does not exist** (`git ls-files` confirms it was
    never committed). The sweep "passes" on this machine only because a stale binary from the
    build session still sits in `target/debug/examples/` — provenance unverifiable.
  - `.github/workflows/ci.yml:58-72`: the parity job is gated on
    `hashFiles('reference/.../match_sim.py') != ''`, but at audit time `reference/` was
git-ignored, so the condition was **always false in CI** and the job always skipped (fixed
in Phase 2: `reference/` is now a pinned submodule and the job runs unconditionally).
`parity_sweep.py` isn't in CI at all.
  - `scripts/sim_parity.sh` tests exactly one configuration: `--threshold 80` (plugin default
    is 85), default flags, deduped names, no custom aliases/countries, no duplicate candidate
    names, no programme validation.
- **Consequence:** README's "parity … is checked rather than asserted" (README.md:198-199) is
  currently untrue in any reproducible sense. The primitives *are* verified (golden fixture in
  `cargo test`); the claim as written covers far more than that.
- **Fix:** Commit the `normalize_io` example (or a `xtask`-style bin), vendor a small fixture
  corpus into the repo so the gate doesn't depend on the (then git-ignored) `reference/`,
and run it unconditionally in CI. [Phase 2: done via the plugin submodule + committed
`normalize_io` + unconditional CI parity job.]

### H4. The one write call (`batch_set_epg`) is the least reliable call in the system
- **Where:** `crates/crosstalk-core/src/dispatcharr.rs:639+`; callers
  `automatch.rs:398-404`, `heal.rs:286-293`.
- **Verified:** GETs get retry + `Retry-After` + one 401 refresh. The POST gets **none** of
  that, and all associations go in **one request** under the default 30 s timeout. A JWT lapse
  or a Dispatcharr restart at the end of a 20-minute apply fails the run *after* minutes of
  work; a server-side-successful-but-timed-out write is recorded as "failed" in SQLite while
  the guide assignments actually changed. (Re-running is mostly idempotent — the audit trail
  lying is the real harm.)
- **Fix:** Mirror the GET retry/refresh on the POST; chunk associations (e.g. 500/request);
  treat "timeout after send" as a distinct outcome and re-read state before declaring failure.

### H5. Graceful shutdown never cancels in-flight runs
- **Where:** `crates/crosstalk/src/main.rs:210-215` — after `axum::serve` drains, only
  `scheduler.shutdown()` is called. `JobRunner::shutdown()` (`runner.rs:258`), which sets every
  cancel flag, has **no caller in the serve path** (grep-verified).
- **Consequence:** `docker stop` during an apply drops the Tokio runtime with runs detached in
  `tokio::spawn`; a run can die mid-`batch_set_epg` and is only repaired post-hoc on next boot
  ("failed / daemon restarted", `store.rs` migrate). This is exactly the ambiguous state the
  (otherwise good) cancel machinery exists to prevent.
- **Fix:** In `serve`: on shutdown signal → `runner.shutdown()`, wait for `running()` to drain
  with a timeout, *then* shut down the scheduler and return.

### H6. Duplicate EPG names lose their fuzzy score for every entry after the first
- **Where:** `crates/crosstalk-matching/src/pipeline.rs:351-357` (`representative()` collapses
  by name), `:570,588-589,701` (match set keyed by representative), vs `:856-863,908`
  (`score_candidates` looks the fuzzy map up by raw index).
- **Effect:** Two guide entries with the same name (routine: same channel in two sources) —
  only the first gets the fuzzy 85–100; the rest fall back to structural-only, often 0. Python
  keys fuzzy scores **by name** (`plugin.py:1147-1153`), so all duplicates inherit it. This
  changes winners in both automatch and heal (e.g. first duplicate is in a dead source, second
  has the programmes).
- **Why tests missed it:** Every parity corpus dedupes names (`parity_sweep.py` uses a
  `seen`-set; `match_sim` fixtures dedupe), so the case is structurally invisible to the suite.
- **Fix:** Key the fuzzy lookup in `score_candidates` by representative (or store scores per
  representative), and add duplicate-name cases to the sweep corpus.

### H7. Config typos are silently ignored — `crosstalk validate` says "settings valid"
- **Where:** No `deny_unknown_fields` anywhere; all structs are `#[serde(default)]`
  (`config.rs` throughout; `janitor/src/settings.rs:51-53`).
- **Verified by the infra audit empirically:** `portt = 9999`, `check_hourss = 99` pass
  validation and silently fall back to defaults. For a tool whose entire UX is "edit TOML,
  check with validate", a misspelled `automatch_confidence_threshold` or `bind` failing
  silently is a safety bug, not a UX nit.
- **Fix:** `#[serde(deny_unknown_fields)]` on all config structs (or at minimum a
  warn-on-unknown-key pass that `validate` reports as an error).

### H8. Scheduler + UI runs are not mutually exclusive with the CLI, and scheduled applies are always armed
- **Where:** `scheduler.rs:152-160` fires `automatch_apply` with `dry_run=false` and no
  confirmation — a permanent `--yes`. Combined with H5 and the per-process `busy` lock
  (`runner.rs`), a `crosstalk run` CLI invocation and a daemon run on the same install have
  **no shared lock at all** (the lock is in-memory per process; SQLite has no `busy_timeout`,
  `store.rs:163`).
- This is configuration trust (only settable via TOML), so it's accepted-risk **if documented**.
  It isn't. One line in the operator docs + a startup log listing armed destructive schedules
  would close most of it.

---

## 4. MEDIUM — fix in the next iteration

### M1. `Store::prune` is dead code — run history grows forever
`store.rs:460` implements pruning with cascades; grep shows **zero callers** outside its own
tests. Only CSV files are pruned. Every scheduled run permanently adds one `run_rows` row per
channel plus full AI request/response JSON into `/config/data/crosstalk.db`. On a
multi-thousand-channel install with nightly schedules this is unbounded growth in the config
volume. *Fix: call it after each run (e.g. keep 90 days / ≥20 runs), and expose the knob.*

### M2. Host settings saved in the UI do nothing until restart — and the UI says "Saved."
`routes.rs:460-469` writes the file; the daemon keeps using the startup
`Arc<CrosstalkConfig>` snapshot. Matching toggles/databases changed in the UI silently don't
affect subsequent runs (janitor settings *are* re-read per run — the asymmetry makes it worse).
*Fix: reload config on save and swap the `Arc`, or label host settings "requires restart".*

### M3. Config write-back downgrades file permissions on secret-bearing files
`config.rs:689-693`: `std::fs::write` to `crosstalk.toml.tmp` (0644 & ~umask) then `rename`
over the target, discarding a `chmod 600` the operator set on a file that can inline the UI
token and Dispatcharr admin key. Also: fixed temp name (concurrent saves collide) and no
fsync. *Fix: create the temp file `0600` (or copy the existing mode), unique temp name, fsync
file + directory.*

### M4. No browser security headers; login has no throttling and no expiry
Router (`routes.rs:42-45`) sets no CSP / `frame-ancestors` / `nosniff` / `Referrer-Policy`;
clickjacking a logged-in operator is partially blunted only by the JS confirm. `/login`
(`routes.rs:150-172`) has no rate limit/lockout; the single all-powerful token never expires,
cookie has no `Max-Age`. `tokens_match` (`auth.rs:62-71`) early-returns on length mismatch
despite its comment claiming otherwise (content compare *is* constant-time for equal lengths).
*Fix: small header layer (htmx is self-hosted, strict CSP is feasible), login backoff, correct
the comment or use `subtle`.*

### M5. AI gate: correct shape, four defects inside
1. **Threshold contract change (by design, undocumented as a parity break):** an AI-accepted
   candidate is assigned with heuristic score 60–94 while
   `automatch_confidence_threshold = 95` (`automatch.rs:232-296`). When AI is on, the
   threshold no longer means what the settings page says.
2. **AI pick with no programmes blocks the heuristic fallback:** the if/else at
   `automatch.rs:263-296` skips the candidate walk entirely, leaving a channel unmatched even
   when candidate #2 was ≥ threshold with programmes.
3. **Decisions are recorded twice** (`automatch.rs:254` and `:292`/`:325`). Currently harmless
   only because `record_ai_decision` is `INSERT OR REPLACE` on `(run_id, ordinal)` — it's a
   wasted write and a landmine if that ever becomes `INSERT`.
4. **The model is told a different callsign than the matcher used:** `ai_gate.rs:130-133`
   extracts with `KnownCallsigns::default()` (empty) while the run uses the merged FCC+DB
   allowlist. Also: heal has no AI at all, and AI calls are **serial** awaits in the channel
   loop, so the provider `max_concurrency = 8` is never used — a large rescue pass is N
   sequential round trips.
*Fix: fall through to the heuristic walk when the AI pick fails validation; record once;
build the request with the run's allowlist; batch AI calls with the existing semaphore.*

### M6. Docker: README quick-start fails; build context can leak live secrets
- `README.md:44-50` omits that compose **requires** `CROSSTALK_TOKEN` and
  `DISPATCHARR_API_KEY` (`${VAR:?…}` in `docker-compose.example.yml:31,34`) and the config-file
  copies — following the README alone errors out immediately.
- `.dockerignore` doesn't exclude `config/*.toml` or `.env`; `Dockerfile:16` `COPY config
  ./config` sends an operator's real (secret-bearing) config into every build context. (It
  doesn't reach the final image, but it leaves the host trust boundary: remote builders,
  BuildKit caches.)
- No `HEALTHCHECK` despite a purpose-built `/health`. CI's docker smoke test authenticates via
  `CROSSTALK_TOKEN`, a different env path than compose actually ships
  (`CROSSTALK_SERVER__TOKEN`), so the shipped mechanism is untested in CI.

### M7. `.gitignore` misses the secret files the docs tell users to create
Verified with `git check-ignore`: **`.env` and `docker-compose.yml` are not ignored** — one
`git add -A` away from committing live credentials, in the exact layout the compose example
prescribes. `scripts/__pycache__/*.pyc` is tracked. `/exports/` is root-anchored, so exports
under `config/exports/` (the real layout) aren't ignored either.

### M8. Performance ceiling is allocation churn, and heal blocks the async runtime
- `similarity.rs:36-38` allocates two `Vec<char>` + two rows **per comparison**, called up to
  3× per (channel × candidate) pair plus per alias. `CandidateIndex` precomputes strings but
  not char vectors. There is no token/inverted-index pre-filter before Levenshtein, so scoring
  is O(channels × entries) with heavy constant factors. The cited full-corpus sim (35k × 9.5k)
  took ~2 minutes single-threaded in release — acceptable for a nightly job, not for an
  interactive UI run.
- `pipeline.rs:966-973` rebuilds the query token `HashSet` **for every matched candidate**;
  `index.name(candidate).to_uppercase()` per pair (`:902`).
- **Heal runs its entire O(broken × entries) loop synchronously inside `async fn`**
  (`heal.rs:190-268`) — it stalls a Tokio worker for the duration, unlike automatch's
  `spawn_blocking`+rayon. Automatch stage-2 validation is also serial awaits despite the module
  doc claiming client-concurrency bounding (only the memo cache helps repeats).
*Fix: precompute `Vec<char>` in `CandidateIndex`; hoist the query token set; move heal into
`spawn_blocking`; validate candidates with `buffer_unordered(concurrency)`.*

### M9. Automatch candidate walk diverges from the plugin's reporting (assignment outcome mostly same)
- Python returns the first candidate *with programmes* irrespective of threshold and reports
  **its** score/method; Rust only walks ≥ threshold and, when nothing validates, reports
  `ranked.first()`'s score with the reason "Best match (100%) carries no programme data…"
  (`automatch.rs:330-337`) — a misleading report row (nothing was matched at 100%).
- Rust skips `epg_source == None` candidates (`automatch.rs:306-308`, `heal.rs:230-232`);
  Python validates by EPG id and would accept a sourceless entry with programmes.
- Rust skips EPG-override channels (`automatch.rs:216-224`); the plugin re-matches them.
  Deliberate, safer — but it is a parity break and should be documented as one, in the CSV
  reason (it is) **and** in the README parity claim (it isn't).

---

## 5. LOW — hygiene and polish

| # | Finding | Where |
|---|---|---|
| L1 | CSV download reads `report_path` from DB unconfined (not remotely exploitable today; confine to `exports_dir` before any future feature writes user-influenced paths) | `routes.rs:306-337` |
| L2 | CSV formula injection: Dispatcharr-controlled names starting `= + - @` written raw into exports | `csv_report.rs:172`, `automatch.rs:536` |
| L3 | Plaintext `http://` to a non-loopback Dispatcharr/AI accepted with no warning (admin key in cleartext) | `config.rs:440-444` |
| L4 | Error responses leak `{error:#}` chains + absolute paths to the caller | `routes.rs:246,420,466` |
| L5 | `channels()/groups()/profiles()/epg_sources()` are envelope-intolerant (bare `Vec<T>` only), unlike the hardened `epg_entries()` | `dispatcharr.rs` |
| L6 | SQLite: no `busy_timeout`; UI reads serialize behind long row-append transactions on the single mutex connection | `store.rs:163` |
| L7 | ≥7 unused workspace deps (`tokio-util`, `tokio-stream`, `futures`, `include_dir`, `mime_guess`, `globset`, `thiserror`); no `cargo audit`/`deny` in CI; actions pinned to mutable tags; Dockerfile "dependencies first" cache comment is inaccurate (no cargo-chef) | `Cargo.toml`, `ci.yml`, `Dockerfile:13` |
| L8 | PLAN's "SSE progress" shipped as 2s/5s polling; "Scheduler UI (enable per action)" shipped read-only — PLAN checkboxes overclaim | `run.html`, PLAN.md Phase 4 |
| L9 | Report filenames have 1-second resolution in `csv_report.rs:277` — wait, this was fixed to `%3f` ms during integration; verify the fix is in the committed tree before relying on it | `csv_report.rs` |
| L10 | `number_word_as_digit` diverges from Python on `FİVE` (U+0130) — Rust returns the word, Python raises `KeyError`. Rust is safer; it's still an unflagged divergence on exactly the Turkish-i inputs `pyre.rs` goes to lengths to match | `normalize.rs:~575` |
| L11 | `score_candidates`' `exclude: Option<usize>` is an index, Python's is an entry id — API trap for the next sub-app author | `pipeline.rs:845` |
| L12 | Login cookie lacks `Secure` (fine for HTTP-only model; note for any TLS-terminating deployment) | `routes.rs:164` |

---

## 6. What is genuinely good (keep, and say so)

- **`pyre.rs`** — the Python-regex emulation (`\b`, `\s` incl. `\x1c-\x1f`, dotless-ı widening,
  class-aware `pythonize`) is the best work in the repo and is properly tested.
- **Env-secret hygiene in config save** — refusing to save an env-merged config
  (`config.rs:540-549`) with a regression test is a control most projects never build.
- **Askama output is escaped everywhere** — no `|safe`, no autoescape-off; XSS surface is clean.
- **Runner panic safety is real** — `RunSlot` drop guard, busy-lock under one reservation,
  poisoned-mutex recovery, crash-stranded runs repaired at startup. Shutdown (H5) is the only
  hole in an otherwise careful design.
- **JWT handling** — in-memory only, refresh-on-401 (GET path), errors truncated, never logs
  secrets. **Repo history is clean** — no real credentials in tree or history (fixtures only).
- **The golden fixture runs in `cargo test`** — primitives parity is genuinely gated, even if
  the broader sweep isn't (H3).

---

## 7. Recommended remediation order

**Weekend pass (H-items, all small diffs):**
1. H2 heal walk fix + E2E test at score 80 *(correctness, ~30 lines)*
2. H6 duplicate-name fuzzy keying + sweep corpus duplicates *(correctness, ~20 lines)*
3. H1 CSRF tokens + server-side confirm *(security, ~1 day)*
4. H5 `runner.shutdown()` + drain wait in `serve` *(5 lines)*
5. H7 `deny_unknown_fields` *(one attribute × N structs + fixing fallout)*
6. M3 config-save `0600` *(one function)*
7. M7 `.gitignore`/`.dockerignore` additions + untrack the `.pyc` *(minutes)*

**Next iteration:**
8. H3 commit `normalize_io`, vendor fixtures, ungate CI parity job
9. H4 POST retry/chunking
10. M1 wire `Store::prune`; M2 config reload or "restart required" label
11. M5 AI-gate fallthrough + single record + allowlist + concurrent calls
12. M8 performance: char-vector precompute, heal in `spawn_blocking`

**Backlog:** M4/M6/L-items, `cargo audit` + `cargo machete` in CI, HEALTHCHECK, README
corrections (Docker steps, parity-claim scoping, SSE→polling, override-skip disclosure).

---

## 8. Decision log (owner answers, 2026-10-31)

| # | Question | Decision |
|---|---|---|
| Q1 | Trust model | **Tokenless loopback stays supported** — fix H1 with CSRF tokens + server-side confirm for destructive actions. Token/LAN modes unchanged. |
| Q2 | Parity contract | **Split contract** (answered "2 and 3"): `match_all_streams` and below stay strict-parity; automatch/heal become Rust-native products. README parity claims get rewritten to say exactly this. |
| Q3 | Fix scope | **Full remediation roadmap** (Section 7, all phases), not just the weekend pass. |
| Q4 | AI authority | AI may **promote within the verify band**, never veto a heuristic win. Fix M5 defects (fallback walk, single record, same allowlist, concurrent calls). Heal stays heuristic-only, documented. |
| Q5 | Heal semantics | **Plugin-style reporting**: sub-threshold working replacements are recorded (`SKIPPED_LOW_CONFIDENCE` / `REPLACEMENT_PREVIEW`) but never applied. H2 is a bug, fix it. |
| Q6 | Scale target | **Large: 20k+ channels, interactive UI runs.** Full M8 performance work is in scope (char-vector precompute, token hoisting, concurrent validation, heal off the async runtime, scoring progress). |
| Q7 | Config strictness | **Hard error on unknown keys** (`deny_unknown_fields`); validate and daemon startup fail loudly. Downgrade friction accepted. |
| Q8 | Config reload | **Hot reload**: saving host settings swaps the in-memory config; schedules re-arm without restart. |
| Q9/Q13 | AI access | **No Jev key; other decision models available. Both backends matter**: a native decision-model backend is primary, OpenAI-compatible is the offline fallback. Live-verify both; TypeSafe/Jev stays as-is until a key exists. ⚠️ Open item: the exact decision-model provider/API shape still needs naming before that work starts. |
| Q10/Q14 | Live testing | **Full running Dispatcharr instance, backups first.** Unrestricted preview/apply comparison against the Python plugin is approved after a DB backup. |
| Q11/Q15 | Distribution | **Docker image is the product, public GHCR** (`ghcr.io/<owner>/crosstalk`, versioned tags + `latest`). Static/macOS binaries are nice-to-have, not release blockers — PLAN/README promises get corrected to match. |
| Q12 | Parity gate | **Git submodule the plugin repo**; CI checks out the real plugin and runs both implementations. Accept the coupling; pin the submodule to a known-good commit and bump deliberately. |
| Q16 | History retention | **Keep 15 days** of run history (minimum-runs floor still to be set; 10/app recommended). Wire `Store::prune` after each run. |

### Revised roadmap (supersedes Section 7 ordering where they conflict)

1. **Safety weekend pass** — H2 heal walk, H6 duplicate names, H1 CSRF+confirm, H5 shutdown drain, H7 strict config, M3 `0600` config saves, M7 ignore files. Nothing unattended before this lands.
2. **Parity made real** — submodule the plugin (Q12), commit `normalize_io`, extend the gate to duplicate names + non-default flags; rewrite README parity claims to the Q2 split contract. (Automatch/heal-shaped parity walks were dropped per the Q2 split contract: actions are Rust-native, so matching-core walks are covered by the match/score sweep kinds plus janitor unit/E2E tests instead.)3. **Write-path reliability** — H4 POST retry/refresh/chunking; M1 prune at 15 days (Q16); L6 `busy_timeout`.
4. **Hot reload** — config swap + schedule re-arm on save (Q8); M2 resolved by construction.
5. **Performance for 20k+ interactive** (Q6) — char-vector precompute, query-set hoisting, concurrent programme validation, heal in `spawn_blocking`/rayon, scoring-stage progress.
6. **AI hardening** (Q4/Q9/Q13) — fallthrough walk, single decision record, run allowlist in requests, concurrent AI calls; live-verify the decision-model + local backends once the provider is named.
7. **Live-instance verification** (Q14) — back up Dispatcharr, run preview row-for-row vs the Python plugin on the full instance, then applies; file every divergence as an issue before fixing.
8. **Docker product** (Q11/Q15) — GHCR publish pipeline, HEALTHCHECK, PUID/PGID smoke test in CI, README quick-start that actually works, `.dockerignore` secret exclusion.
9. **Backlog** — M4 headers/login throttling, M6 compose/registry docs, L-items, `cargo audit`/`machete`, unused-dep removal.

**Still open (blocking roadmap items 6 only):** name the non-Jev decision-model provider and
its API shape (Q13 follow-up). Everything else can start now.
