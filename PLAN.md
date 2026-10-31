# Crosstalk: Rust host for Dispatcharr tools, with Janitor as the first sub-app

## Context

EPG Janitor is a Python plugin that runs inside Dispatcharr's process and uses the Django
ORM directly. Its value is the matching pipeline: weighted structural scoring over
callsign / state / city / network, plus a four-stage fuzzy name pipeline (alias, exact,
substring, token-sort Levenshtein) with sibling-number, time-shift, regional and
callsign-anchor guards. That pipeline beats Dispatcharr's built-in mapper.

Goal: rebuild it as `crosstalk`, a long-running Rust daemon that hosts multiple sub-apps.
`janitor` is the first: a fast, multi-threaded, accurate EPG matcher for Dispatcharr.
Because matching is a yes/no decision, an optional AI verifier using TypeSafe's System One
models (Jev) is wired in at the host level so any sub-app can use it.

### Decisions (from the planning rounds)

| Topic | Decision |
|---|---|
| Deployment | Standalone Rust daemon, talks to Dispatcharr over its REST API |
| Run mode | Long-running daemon from day one: REST API + scheduler + full web UI |
| UI stack | Server-rendered Rust: Askama templates + htmx, SSE for live progress |
| UI features v1 | Run, monitor, review results, edit settings (writes TOML back) |
| Auth | Bind localhost by default; optional bearer token; UI login page sets cookie |
| Packaging | Docker image + static Linux (musl) binary; macOS binary for dev |
| Scope v1 | Matching only: auto-match preview/apply, scan & heal preview/apply, scope filters, normalisation toggles, aliases, CSV export + retention, validate, progress. Cleanup actions, watchdog, usage counter are out |
| Parity | Strict parity with the Python matcher first; behaviour changes only behind opt-in flags |
| AI role | Heuristics primary. AI verifies borderline scores and rescues unmatched channels. High-confidence heuristic matches never hit the AI |
| AI providers | TypeSafe System One (native) + OpenAI-compatible fallback behind one trait |
| Model selection | `janitor.toml` names `ai.provider` and `ai.model` literally; `crosstalk.toml` holds provider endpoints, keys, concurrency |
| Plugin model | Compile-time crates in one Cargo workspace, `SubApp` trait, static registry |
| Shared options | `crosstalk.toml [matching]`: normalisation toggles, enabled country DBs, custom aliases. `janitor.toml`: thresholds, check window, scope filters, heal sources, AI gates |
| Storage | Embedded SQLite (`crosstalk.db`) for runs, results, AI decisions. CSVs still written |
| Repo | `git init` at crosstalk root; Python plugin moves to `reference/` and is git-ignored |

## Source of truth for parity (Python plugin)

All paths relative to `reference/Dispatcharr-EPG-Janitor-Plugin/` after the move.

- `EPG-Janitor/fuzzy_matcher.py` (1476 lines): the matcher Janitor actually uses. It
  overrides `normalize_name` and `_compute_callsign_with_confidence` from the core, so
  port THIS file's versions, not `matching_core.py`'s. Key pieces:
  - pattern tables: `QUALITY_PATTERNS`, `RESOLUTION_PATTERNS`, `REGIONAL_PATTERNS`,
    `REGIONAL_EAST_WEST_PATTERNS`, `GEOGRAPHIC_PATTERNS`, `PROVIDER_PREFIX_PATTERNS`,
    `MISC_PATTERNS`, `NUM_WORDS`, `_PLUS_SHIFT_RE`, `_LEADING_BAR_TAG_RE`
  - `normalize_name` (lines 568-755), `_compute_callsign_with_confidence` (5-priority
    ladder, 478-550), `_get_known_callsigns` (FCC file + DB-derived), `alias_match`,
    `match_all_streams` (1245-1476), `_channel_number_boost`, `precompute_normalizations`
- `EPG-Janitor/matching_core.py`: `calculate_similarity` (1 - distance / max_len, on code
  points), `_length_scaled_threshold`, `_has_token_overlap` (subset / divergent / numeric
  guards), `process_string_for_matching` (NFKD, strip Mn, keep alnum and `+`, sort
  tokens), `_strip_stylized_tokens`, `_normalize_emoji`, `_CALLSIGN_DENYLIST`,
  `_is_distinctive`, `_DECORATOR_CATS`
- `EPG-Janitor/plugin.py`: `_find_best_epg_match` (1077-1300: structural 50/30/20/+10 vs
  fuzzy, strict `>` tie preference for structural label, candidate walk with program-data
  validation), `_extract_location`, `_auto_match_channels` (583-1000),
  `_scan_and_heal_worker` (1327-1688), `_get_filtered_epg_data` + `_order_by_priority`
  (482-560), `_build_alias_map`, `_validate_and_filter_groups` (1694-1827),
  `_generate_match_reason`, `_generate_heal_reason`, `_generate_csv_header_comments`,
  `_csv_exports_to_delete` / `_retention_days`, CSV column lists for both reports.
  Constants: `FUZZY_MATCH_THRESHOLD = 85`, thresholds default 95, `check_hours` 12.
- `EPG-Janitor/wildcard_match.py`: `expand_patterns` (glob vs literal, ci_plain flag).
- `EPG-Janitor/aliases.py`: `CHANNEL_ALIASES` (~200 entries) -> ship as JSON.
- Data: `*_channels.json` (US, UK, CA, AU, DE, FR, ES, NL, NO, IN, BR, MX),
  `us_station_callsigns.json`.
- Tests to port as fixtures: `tests/matcher_golden_baseline.json`,
  `tests/test_fuzzy_matcher.py` (978 lines), `tests/test_regressions.py`,
  `tests/test_matcher_parity_port.py`, `tests/test_normalization_port.py`,
  `tests/test_wildcard_match.py`, `tests/test_csv_retention.py`, `tests/test_csv_header.py`.
- `tools/match_sim.py`: offline simulator; its CSV outputs are the side-by-side oracle.

## Dispatcharr REST API surface (verified against Dispatcharr `main`)

| Need | Endpoint | Notes |
|---|---|---|
| Auth | `POST /api/accounts/token/` (JWT) or header `X-API-Key: <key>` | Prefer API key; JWT with refresh as fallback |
| Channels | `GET /api/channels/channels/` | Unpaginated when no `page`/`page_size`; has `name`, `channel_number`, `channel_group_id`, `epg_data_id`, override/effective fields |
| Groups | `GET /api/channels/groups/` | name -> id for group filters |
| Profiles | `GET /api/channels/profiles/` | profile membership for `channel_profile_name` filter |
| EPG sources | `GET /api/epg/sources/` | `id`, `name`, `is_active`, `priority` |
| EPG entries | `GET /api/epg/epgdata/` | `id`, `tvg_id`, `name`, `epg_source`; paginated, walk all pages |
| Program-data check | `GET /api/epg/programs/search/?tvg_id=&epg_source=&end_after=<now>&start_before=<now+N h>&page_size=1&fields=id` | No `epg_id` filter, so key by (tvg_id, source id). Lazy per winning candidate, cached, bounded concurrency |
| Apply | `POST /api/channels/channels/batch-set-epg/` body `{associations:[{channel_id, epg_data_id}]}` | Returns `channels_updated`; Dispatcharr queues the programme parse itself |

Risk: the program-data check is the only part that is cheap in-process and costly over
HTTP. Mitigation: validate candidates lazily in score order, memoise per (tvg_id, source),
run checks through a semaphore (default 8), and only check the head of each candidate list.

## AI layer: TypeSafe System One (verified against docs.typesafe.ai)

- `POST https://api.typesafe.ai/v1/systemone`, `Authorization: Bearer <key>`;
  `GET /v1/models` lists names. Models: `jev-1.14.0`, `jev-1.13.0`; aliases `jev-latest`,
  `jev-preview`. 64k tokens per request, 32k per question. Input billed, output free.
- Request `{model, state: string|object|array, questions: {id: Question}}`. Types: `noul`
  (yes/no -> probability 0..1), `choice` (-> `choice`, `probabilities`, `confidence`),
  `score`. All questions evaluated in parallel against one state; 70-500 ms end to end.
- Errors: 429 / 529 -> exponential backoff honouring `retry-after`. Public endpoint
  rate-limits around 8 concurrent requests, so the client needs a semaphore.
- Jaggedness to design around: literal reading, no arithmetic / counting, large
  irrelevant state hurts. Keep sibling-number, time-shift and region guards in code; send
  only the shortlist; write precise criteria.

### How Janitor uses it (one request per channel that needs AI)

State object: `channel` (name, group, number, heuristic-extracted callsign, state, city,
network) and `candidates[]` (id, name, source name, heuristic score, method). Shortlist =
top-N (default 5) survivors of the code-side guards.

Questions in that one request:
- `same_<i>` Noul per candidate: "Does `candidates[i]` refer to the same television
  channel as `channel`?" with `criteria.true/false` spelling out boundary cases
  (numbered siblings, +1 time-shift, East/West feeds, regional affiliates differ).
- `best` Choice over candidate ids plus `none`.
- Companion Nouls for the reason column: `same_network`, `same_region_feed`.

Code combines: accept candidate i only if `best == i`, `same_i >= accept_threshold`
(0.8) and Choice `confidence >= min_confidence` (0.5). Reject outright when
`same_i <= reject_threshold` (0.3). Otherwise leave unmatched and label the row
"AI undecided". Every AI decision (request id, model, probabilities, latency, tokens) is
stored in SQLite and shown in the UI.

Two gates in `janitor.toml`:
- `ai.verify_band = [lo, hi]` (default `[80, 94]`): best heuristic score inside the band
  and below the confidence threshold -> ask AI; an accepted answer promotes the row to
  "AI verified" with score = heuristic score, method suffixed `+ AI`.
- `ai.rescue = true`: no heuristic candidate reached the band but there are candidates
  with score >= `ai.rescue_floor` (default 60) -> ask AI over that shortlist.

Provider abstraction in `crosstalk-core::ai`: trait `Decider { async fn decide(&self,
req: DecisionRequest) -> Result<DecisionResponse> }` with `Noul`, `Choice`, `Score`
questions. Backends: `typesafe` (native) and `openai_compat` (emulates via JSON-schema
structured output: probabilities are approximated, so confidence gating is weaker and the
UI says so). Sub-apps never talk HTTP directly.

## Approach

### Workspace layout

```
crosstalk/
  Cargo.toml                      # [workspace] members = crates/*
  rust-toolchain.toml             # stable
  Dockerfile, docker-compose.example.yml
  config/
    crosstalk.example.toml
    janitor.example.toml
  data/                           # shipped read-only data, embedded with include_dir
    channels/US_channels.json ... MX_channels.json
    us_station_callsigns.json
    aliases.json                  # converted from aliases.py
  crates/
    crosstalk/                    # bin: CLI (`serve`, `validate`, `run <app> <action>`), wiring
    crosstalk-core/               # lib: config, SubApp trait + registry, Dispatcharr client,
                                  #      ai (Decider + backends), jobs (runner, progress, scheduler),
                                  #      store (SQLite), csv, wildcard, logging
    crosstalk-matching/           # lib: pure matcher (normalise, similarity, callsign, alias,
                                  #      match_all_streams, structural scoring, location). No IO.
    crosstalk-web/                # lib: axum router, Askama templates, htmx partials, SSE, auth
    janitor/                      # lib: SubApp impl: auto-match, heal, CSV reports, settings schema
  reference/                      # git-ignored; the Python plugin lives here
  PLAN.md
```

### Crate: crosstalk-matching (pure, parity-critical)

- `normalize.rs`: port of `FuzzyMatcher.normalize_name` with all pattern tables, in the
  exact order of the Python. Patterns using lookaround (`(?<=[A-Za-z])\.(?=[A-Za-z])`,
  `(?!network\b|open\b)`, `(?!\s+open\b)`, `(?!EST\)|...)`) go through `fancy-regex`;
  everything else through `regex`. Unicode helpers: `unicode-normalization` (NFKD),
  `unicode-general-category` (`Cf`, `Mn`, `So`/`No`/`Lm`/`Sk`), `unicode_names2`
  (SUPERSCRIPT / SUBSCRIPT / SMALL CAPITAL / MODIFIER LETTER name tests).
- `similarity.rs`: Levenshtein on `char`s with `1 - d / max_len`, `min_ratio` early-exit
  semantics identical to Python (inclusive at the cutoff), plus `length_scaled_threshold`.
- `tokens.rs`: `process_string_for_matching`, `has_token_overlap` with majority /
  subset / divergent / numeric guards, `trailing_number`, `is_distinctive`.
- `callsign.rs`: denylist, 5-priority ladder, `KnownCallsigns` (FCC file + DB-derived),
  `normalize_callsign`, `extract_location`, network keyword.
- `alias.rs`: alias map type, `alias_match`.
- `pipeline.rs`: `Matcher` struct holding normalisation flags, alias map, known callsigns
  and the per-candidate caches (`CandidateIndex` built once per run: normalised lower,
  nospace, processed, callsign+confidence, location). `match_all_streams` and
  `find_best_match` (structural vs fuzzy, max, strict `>` tie rule) returning a ranked
  `Vec<Candidate { idx, score, components }>` so the caller does program-data validation.
- `Matcher` is `Send + Sync`; the index is `Arc`; per-query work is pure. Parallelism is
  `rayon::par_iter` over channels in the sub-app, not inside the matcher.
- Feature flags for later behaviour changes live in a `MatchOptions` struct with every
  flag defaulting to Python behaviour.

### Crate: crosstalk-core

- `config/`: `CrosstalkConfig` (`[server]` bind, port, auth token; `[dispatcharr]` url,
  api_key or username/password, timeout, concurrency; `[ai.providers.<name>]` kind
  (`typesafe`|`openai_compat`), base_url, api_key (or `api_key_env`), max_concurrency,
  timeout; `[matching]` ignore_quality/regional/geographic/misc, enabled_databases,
  custom_aliases (inline table or path); `[jobs]` worker_threads, exports_dir, data_dir;
  `[schedules]` per sub-app action cron strings). Loaded with `figment` (TOML + env
  overrides). Write-back preserves comments via `toml_edit`.
- `subapp.rs`: `trait SubApp { fn id(); fn name(); fn settings_schema() -> SettingsSchema;
  fn load_settings(&Path) -> Result<Box<dyn Any>>; fn actions() -> Vec<ActionSpec{id,
  label, kind: Read|Write|Destructive, confirm}>; async fn run(&self, action, ctx:
  JobCtx) -> Result<JobOutcome> }`. `SettingsSchema` drives both the UI forms and the CSV
  preamble, mirroring the Python `fields` list (section, label, help text, type, default).
- `dispatcharr/`: `reqwest` client with API-key or JWT auth, typed models, pagination
  walker, `has_programs(tvg_id, source_id, window)` with a `DashMap` memo and semaphore,
  `batch_set_epg`, retry/backoff.
- `ai/`: `Decider` trait, `typesafe.rs`, `openai_compat.rs`, shared retry/semaphore,
  decision logging hook.
- `jobs/`: `JobRunner` (one job at a time per sub-app, cancellation token, progress
  channel -> SSE + store), `Scheduler` (`tokio-cron-scheduler`), `JobCtx` (config, clients,
  store handle, progress sink, dry_run flag).
- `store/`: `rusqlite` (bundled) with migrations: `runs`, `run_rows`, `ai_decisions`,
  `settings_history`. Query helpers for the UI.
- `csv.rs`: preamble generator (`# ...` comment block from `SettingsSchema`), writer,
  retention pruning (`exports_to_delete` pure function ported from Python).
- `wildcard.rs`: port of `expand_patterns` (glob via `globset` case-insensitive, literal
  with `ci_plain`).

### Crate: janitor

- `settings.rs`: `JanitorSettings` (profile names, selected/ignore groups, EPG sources
  to match, check_hours, automatch threshold, allow_epg_without_programs, heal fallback
  sources, heal threshold, csv_retention_days, `ai { enabled, provider, model,
  verify_band, rescue, rescue_floor, shortlist, accept/reject/min_confidence }`) plus
  the schema with the Python labels and help text.
- `scope.rs`: resolve groups / ignore groups / profiles / EPG sources against the API
  (validation errors identical in meaning to `_validate_and_filter_groups`), order EPG
  entries by source priority, drop inactive sources.
- `automatch.rs`: build `CandidateIndex` once; `par_iter` channels -> ranked candidates
  -> async validation stage (program-data check, then AI gate) -> rows; CSV with the
  Python column set plus `ai_decision`; apply via `batch_set_epg`; record run.
- `heal.rs`: find broken (assigned but no programmes in window), search replacements
  excluding the current id, statuses `NO_REPLACEMENT_FOUND` / `REPLACEMENT_PREVIEW` /
  `HEALED` / `SKIPPED_LOW_CONFIDENCE`, CSV columns as Python.
- `reasons.rs`: `match_reason` / `heal_reason` text ported verbatim, with AI suffixes.
- `lib.rs`: `JanitorApp: SubApp` with actions `validate`, `automatch_preview`,
  `automatch_apply` (destructive, confirm), `heal_preview`, `heal_apply` (write, confirm).

### Crate: crosstalk-web

- axum router mounted by the daemon: `/` dashboard (sub-apps, last runs, scheduler),
  `/apps/janitor` (actions with the Python colour semantics: blue read, green write-no-
  assignment, orange write, red destructive + confirm), `/runs/:id` (progress via SSE,
  results table with filters, reason column, AI panel, CSV download), `/settings`
  (crosstalk + per-app forms generated from `SettingsSchema`, validation errors inline),
  `/login`. JSON API under `/api/v1/` for the same operations (used by htmx and curl).
- Askama templates + a small handwritten stylesheet; htmx for partial updates, SSE for
  live progress. No Node toolchain.

### Crate: crosstalk (bin)

- `crosstalk serve --config /config` (daemon), `crosstalk validate`, `crosstalk run janitor
  automatch --preview` (one-shot, same code path, for cron users and tests),
  `crosstalk simulate --channels x.csv --streams y.m3u` (port of `match_sim.py` for the
  parity oracle).

## Files to create / modify

New repository, so everything is new. The critical paths:

- `Cargo.toml`, `rust-toolchain.toml`, `.gitignore` (ignores `reference/`, `target/`,
  `*.db`, `config/*.toml` except examples)
- `crates/crosstalk-matching/src/{lib,normalize,similarity,tokens,callsign,alias,pipeline}.rs`
- `crates/crosstalk-matching/tests/{golden,fuzzy_matcher_port,regressions}.rs` +
  `tests/fixtures/matcher_golden_baseline.json`
- `crates/crosstalk-core/src/{lib,config,subapp,dispatcharr,ai,jobs,store,csv,wildcard}.rs`
- `crates/janitor/src/{lib,settings,scope,automatch,heal,reasons}.rs`
- `crates/crosstalk-web/src/{lib,routes,auth,sse}.rs`, `templates/*.html`, `static/app.css`
- `crates/crosstalk/src/main.rs`
- `data/channels/*.json`, `data/us_station_callsigns.json`, `data/aliases.json`
- `config/crosstalk.example.toml`, `config/janitor.example.toml`
- `Dockerfile`, `docker-compose.example.yml`, `README.md`

## Reuse

- Port, do not redesign: every regex table and the exact substitution order in
  `fuzzy_matcher.py::normalize_name`; `match_all_streams`; `_has_token_overlap`;
  `_compute_callsign_with_confidence`; `_find_best_epg_match`; `_extract_location`;
  `_generate_match_reason` / `_generate_heal_reason`; CSV preamble and column lists;
  `_csv_exports_to_delete`; `wildcard_match.expand_patterns`.
- Data files copied byte-for-byte; `aliases.py` converted to JSON by a one-off script.
- `tests/matcher_golden_baseline.json` becomes a Rust fixture unchanged.
- Rust crates: `tokio`, `axum`, `askama`, `reqwest`, `serde`, `toml_edit`, `figment`,
  `rusqlite` (bundled), `rayon`, `regex`, `fancy-regex`, `unicode-normalization`,
  `unicode-general-category`, `unicode_names2`, `globset`, `tokio-cron-scheduler`,
  `tracing`, `clap`, `include_dir`, `dashmap`.

## Steps

### Phase 0: repository
- [x] `git init`, move the Python plugin to `reference/`, add `.gitignore`, workspace
      `Cargo.toml`, `rust-toolchain.toml`, CI stub (fmt, clippy, test).

### Phase 1: crosstalk-matching with parity proof
- [x] Port `similarity.rs`, `tokens.rs`; golden tests for `calculate_similarity` and
      token guards from `matcher_golden_baseline.json`.
- [x] Port `normalize.rs` (all tables, lookaround patterns via `fancy-regex`, Unicode
      helpers); golden tests for `normalize_name` + cases from `test_normalization_port.py`.
- [x] Port `callsign.rs` (ladder, denylist, known-callsign loading, location, network);
      golden tests for `extract_callsign`.
- [x] Port `alias.rs` and `pipeline.rs` (`CandidateIndex`, `match_all_streams`,
      `find_best_match`); port the behavioural cases from `test_fuzzy_matcher.py` and
      `test_regressions.py` as Rust tests.
- [x] Build `crosstalk simulate` and diff its per-stream CSV against `tools/match_sim.py`
      on the same inputs. Exit criterion: identical scores and methods on the full table.

### Phase 2: crosstalk-core
- [x] Config loading + write-back, `SubApp` trait + registry, `SettingsSchema`.
- [x] Dispatcharr client (auth, channels, groups, profiles, sources, epgdata pagination,
      program search memo, batch-set-epg) with a mock server test (`wiremock`).
- [x] SQLite store + migrations; job runner with progress channel; scheduler.
- [ ] CSV preamble/writer/retention port with the `test_csv_retention.py` cases.
- [ ] `wildcard.rs` with `test_wildcard_match.py` cases.
- [ ] AI: `Decider` trait, TypeSafe backend (retry, semaphore, decision log), OpenAI-compat
      backend; tests against recorded responses.

### Phase 3: janitor
- [ ] Settings + schema, scope resolution, EPG ordering by priority.
- [ ] Auto-match preview/apply with rayon matching stage and async validation stage.
- [ ] Scan & heal preview/apply.
- [ ] AI verify-band and rescue gates; reason text; CSV `ai_decision` column.
- [ ] `crosstalk run janitor ...` one-shot path and `validate`.

### Phase 4: web UI and daemon
- [ ] axum app, auth middleware, login page, dashboard.
- [ ] Janitor action page with colour semantics and confirm dialogs.
- [ ] Run page: SSE progress, results table, AI panel, CSV download.
- [ ] Settings pages generated from schemas, TOML write-back with validation.
- [ ] Scheduler UI (enable per action, cron string, next run).

### Phase 5: packaging
- [ ] Multi-stage Dockerfile (musl static), compose example beside Dispatcharr,
      `/config` volume layout, README with first-run walkthrough.

## Verification

- `cargo test --workspace`: golden baseline, ported matcher tests, CSV/wildcard tests,
  Dispatcharr client against wiremock, AI backends against recorded fixtures.
- Parity oracle: `crosstalk simulate` vs `python3 tools/match_sim.py` on the same
  channels CSV and M3U; `diff` of `per_stream_matches.csv` must be empty before any
  opt-in flag is added.
- Live preview against a real Dispatcharr: `crosstalk run janitor automatch --preview`
  writes a CSV, changes nothing, and the daemon's run page shows the same rows. Compare
  row-for-row with a Python plugin preview CSV from the same instance.
- Live apply on a small test group, then confirm `channels_updated` in the API response,
  the Dispatcharr UI, and the stored run.
- AI: run preview with `ai.enabled = true` on a group with known borderline names; check
  the AI panel shows probabilities, that no row above the confidence threshold called the
  AI, and that rescue only fired for rows with no heuristic match.
- Throughput: time a preview over the full channel list with 1, 4 and all cores; the
  matching stage should scale near-linearly, the validation stage should be bounded by the
  configured HTTP concurrency.
- Docker: `docker compose up`, UI reachable on the mapped port only with the token,
  config and exports persist across container restarts.
