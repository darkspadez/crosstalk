# Phase 2: the `streams` sub-app

Status: ready for review. Parent plan: `plans/plugin-consolidation.md` (phase 2
table, cross-cutting rules, Phase 1 review notes). Phase 1 is committed on this branch
(`c10f665..afe0d1e`, 619 tests).

## Context

`streams` owns which streams a channel has, their order, and whether they work. It replaces
Stream-Mapparr (21 actions) and IPTV Checker (23) with 8 action groups on top of the phase-1
shared layer.
Lineuparr's stream half (3 actions) follows in phase 3. Upstream pins at research time:
Stream-Mapparr `04dd471`, IPTV Checker `14f17ea` (both 2026-11-01).

## Decisions
- One phase 2 (no 2a/2b split); one Opus review at the end.
- **Why this port exists:** none of the plugins run on cedro today. Janitor was too slow at
  this scale, so the goal is the same behaviour, fast and concurrent at ~20k channels,
  ~50k streams, ~100 sources. There is no cut-over pressure; testing on a real (or test)
  instance happens after all phases. So: output parity with the plugins, **algorithms and
  request patterns free to change** for speed; scale is a first-class requirement with
  benchmarks, not an afterthought.
- Lineuparr's stream half moves to **phase 3** (it needs lineup files). Phase 2 ports
  Stream-Mapparr + IPTV Checker; `streams.match` gets a lineup mode in phase 3.
- Sort: Stream-Mapparr's quality model is the one model (Lineuparr's ordering may differ
  slightly; noted as a divergence in phase 3).
- Host webhook (Dispatcharr Connect → crosstalk) and named workflows are built in phase 2.
- Probing runs on the same host as Dispatcharr (aphrodite), against provider URLs directly,
  like IPTV Checker, with a per-account connection cap.
- No live cedro access during execution: fixtures and mocks only. The bridge-vs-REST timing
  decision is made by the user running a provided timing command later; the plan must work
  over REST without the bridge.
- `probe` covers streams attached to in-scope channels by default (primary + alternates,
  configurable); unattached streams only on request. Skips streams probed within a
  freshness window; resumable windows work through a backlog over several nights.
- Probe concurrency: **1 connection per M3U account by default**, never above the account's
  `max_streams`; raise per account in settings. Total parallelism grows with the number of
  accounts.
- Stream-list writes go over **REST behind a `StreamListWriter` seam** with bounded
  concurrency; a benchmark command lets the user time it on a test machine. The bridge
  plugs in behind the same seam later if needed. No bridge in phase 2.

## Actions

| crosstalk action | Merged from |
|---|---|
| `match_preview` / `match_apply` | SM `preview_changes`, `add_streams_to_channels`, `match_us_ota_only` (LU's stream half: phase 3) |
| `sort_preview` / `sort_apply` | SM `sort_streams` (LU `resort_streams`: phase 3) |
| `mirror_preview` / `mirror_apply` | SM `fill_mirror_backups` |
| `probe` | IPTV `load_groups` + `check_streams`, SM `probe_throughput` |
| `triage_preview` / `triage_apply` | IPTV rename/move dead, blank, low-fps, `add_video_format_suffix` |
| `restore_preview` / `restore_apply` | IPTV `restore_channels` |
| `visibility_preview` / `visibility_apply` | SM `manage_channel_visibility` |
| `check_countries`, `scan_placeholders`, `test_rules` | SM read-only tools |
| `validate`, `status` | every app has them |

Not ported: IC `delete_dead_channels` (out of scope), email/report/bug actions, scheduler
housekeeping (`update_schedule`, `cleanup_*`, `check_scheduler_status`, `reset_progress`,
`clear_operation_lock`), IC `view_*`/`export_results` (host run page + CSV), IC `cancel_check`
(host cancel). SM `on_m3u_refresh` / `on_iptv_checker_scan` become the host webhook and a
workflow.

## Phase-1 gaps phase 2 must close first (from the review notes)
- Per-channel values in channel ops (restore, per-channel stream lists): `Op::Set` or a public
  plan-from-changes constructor; `MoveGroup` to "No Group".
- Multi-select match walk (Stream-Mapparr attaches every matching stream); `Candidate` for `Stream`.
- Run rows selected by `field`, never `channel_id` alone (restore).
- Cancel semantics for long jobs (probe), settled once for every app.

## Research findings (SM = Stream-Mapparr `plugin.py`, IC = IPTV Checker, LU = Lineuparr)

The research ran on a small model; line numbers are leads, not proof. Every implementing step
re-derives the behaviour from source with its own citations and parity harness.

### Stream-Mapparr match/assign
- Channels: enabled in the profile, group include/exclude filter. Channels are **grouped by
  normalized name** (`_build_channel_groups` ~6820, OTA by callsign in theory, by name in
  practice); matching runs once per group and `visible_channel_limit` (default 1) decides how
  many channels of a group receive the streams.
- Streams: M3U accounts (`selected_m3us`, ordered = priority), stream group include/exclude,
  regex rename rules, ignore tags, optional dead-stream filter, optional country restriction.
- Per group (`_match_streams_to_channel` ~5808): OTA callsign regex path first; otherwise
  `fuzzy_match` picks a **winning normalized name**, then every stream whose cleaned name
  equals it, plus alias force-includes, plus token/substring rules with number guards and a
  threshold-dependent strictness, is collected. So it is **multi-select by name family**, not
  "top-N by score": `match_run::walk` does not fit; streams needs its own collector over
  the shared matcher.
- Then dedupe (unless `allow_same_name_streams`), same-country first, zone routing
  (East/West/Pacific), quality sort. Write: replace (`overwrite_streams`, default on) or append;
  `keep_custom_streams` (default on) re-appends custom streams after the new list.
- EPG placeholder matching (off by default): placeholder-named channels/streams matched by the
  currently airing programme title.
- Matcher: SM's `fuzzy_matcher.py` has zone expansion, designator-token repair, alias-key
  folding and a single-timezone-country guard that Janitor's lacks (my function diff;
  the research report had this backwards). These become `MatchProfile::StreamMapparr` flags.

### Quality and sort
- SM key (`_sort_streams_by_quality` ~4739): `(content_starved, throughput_tier, quality_tier,
  m3u_priority, audio_channels_rank, audio_codec_rank, -height, -fps, bitrate_step)`, with
  quality and m3u priority swapped when `prioritize_quality` is off. Quality tier: HD+30fps /
  HD or 30fps / SD / unknown, from `stream_stats`. Throughput tier from SM's own probe cache.
- LU key: tier 4K/FHD/HD/SD/low/unknown from `stream_stats.height` or name tags, then
  m3u priority (or the reverse). Simpler and different.

### Throughput probe (SM `probe_throughput`)
HTTP download for `probe_duration_seconds` (8), Mbit/s vs a nominal bitrate from resolution;
cache with TTL 30 min in a JSON file; `probe_rate_per_minute` 6; 1 s per-account spacing.

### Mirror backups (SM `mirror_backups.py`)
Accounts sharing `server_group` are mirrors; a channel's provider stream id is looked up on
every other active mirror account and inserted after the last live stream, before custom
streams. Needs `server_url`/`server_group` on `M3uAccount` (in the serializer; **not yet in
the client type**) and the provider stream id on `Stream`.

### Visibility (SM `manage_channel_visibility`)
Disables every channel in the profile, then enables the first channel of each name group
that has a stream. Per-channel membership values: needs the per-channel op.

### IPTV Checker probe
- Selection: channel groups include/exclude, primary or all streams, only-visible option.
- `ffprobe -print_format json -user_agent VLC… -timeout -analyzeduration -probesize 10M
  -show_streams -show_packets` (flags configurable); bitrate from stream, then format, then
  packets (≥30). Optional one `ffmpeg` decode pass for black (`blackdetect`), frozen
  (`freezedetect`), silent (`volumedetect`), plus a placeholder-file check. Fail-open.
- Classes: alive / dead / skipped (429, audio-only, streamlink hosts); dead content classes.
  A channel is dead only if every stream is dead; skipped is never dead.
- Writes `Stream.stream_stats` (video/audio codec, resolution, width, height, fps, pixel format,
  bitrates, channels, sample rate, stream type): merge non-null on alive, **clear to `{}` on
  dead**. Over REST this is read-merge-write of `stream_stats` per stream.
- Parallel workers (2), per-stream delay (2 s), 3 retries, global throttle on 429.
- Windowed schedules: cron start + duration/end time, resume file, settings re-read per window.
- Post-actions: rename (`{name} [DEAD]`/`[Blank]`/`[Slow]`), move to group (Graveyard…),
  format suffix (UHD/FHD/HD/SD), restore (from `channel_state.json`), delete (out of scope).
- Hook: fires SM's `on_iptv_checker_scan` after a full scheduled scan.

### Lineuparr stream half
`match_all_streams` per lineup channel: alias → exact → substring → token-sort, with lineup /
category / channel country filtering, US↔MX cross-border exceptions, quality-aware twin
matching (4K channel ↔ 4K streams), bare region words stripped for scoring. Replace (and
delete channels left empty) or append (`preserve_existing_streams`). Its inputs (lineup
country, category, channel number) come from the lineup JSON file, which is phase 3's.

## Approach

### Data the app keeps itself
A `stream_health` table in the crosstalk store, one row per stream: last probe time, class
(alive/dead/skipped/blank/frozen/silent/placeholder), error type, measured throughput and
its time, consecutive failures. `stream_stats` in Dispatcharr stays the shared truth for
media facts (players and Dispatcharr read it); crosstalk's table holds what only crosstalk
uses. Sort reads both. Replaces SM's throughput JSON cache and IC's loaded-channels file.

### Shared-layer additions (core)
- channel_ops: per-channel values (`Op::Set { field, values }` or a public
  plan-from-changes), `MoveGroup(None)`, a `Streams` field (ordered list) so stream-list
  writes get plan/preview/CSV/run rows/restore like every other write.
- `Candidate for Stream`; a stream selection module (accounts, stream groups, regex rules)
  next to `scope::sources`.
- Host: resumable windowed jobs, per-account concurrency limiter, minimal workflows
  (`probe → sort_apply`), and the Dispatcharr Connect webhook trigger.
- Client: `M3uAccount.server_url/server_group`, stream provider id, `stream_stats` PATCH.

### Matcher
`MatchProfile::StreamMapparr` (and `Lineuparr`, see questions) as flag sets on the one
engine, each with its own parity sweep reusing `scripts/parity_sweep.py`'s harness.

### Image
`ffmpeg` (includes `ffprobe`) in the runtime image; measure the size and say it in the README.

### Action details
- **match**: scope channels → name groups (SM's grouping) → per group, the SM collector
  (OTA callsign path, fuzzy winner name family, aliases, token/substring rules, optional EPG
  placeholder titles via `/api/epg/current-programs/`) → dedupe, same-country, zone routing
  → sort (shared quality model) → replace or append, keep custom streams, first
  `visible_channel_limit` channels per group. Preview = plan + CSV; apply = channel ops
  `Streams` field through `StreamListWriter`. Per-group matching runs on rayon like guide.
- **sort**: same quality model over each channel's current list; only changed orders written.
- **mirror**: SM `plan_mirror_fill` over `server_group` accounts; inserts via the same writer.
- **visibility**: SM name groups → per-channel membership values (enable the first channel
  with a stream, disable the rest) via the per-channel op; only real changes are written
  (SM disables everything then re-enables; crosstalk writes the diff, same end state).
- **probe**: selection (scope channels → their streams, primary or all; skip fresh) →
  per-account limiter → `ffprobe` (+ optional `ffmpeg` content pass, + optional throughput
  download) → classify → `stream_health` row + `stream_stats` read-merge-write (clear on dead).
  Read-only toward channels, so it is one action (ActionKind::Write: it writes stream stats),
  not a pair; a `--dry-run` skips the Dispatcharr write and only records health.
- **triage**: last probe's per-channel verdict (dead only if every stream dead; skipped never
  dead) → per-class rename template + move group from settings, and format suffix for alive
  channels → channel ops. `before` values land in run rows.
- **restore**: channels whose newest verdict is alive and that carry an unreversed triage
  change → per-channel values from those run rows (by `field`) → channel ops.
- **read-only tools**: `check_countries`, `scan_placeholders`, `test_rules` write a CSV and a
  summary; no Dispatcharr writes.

### Settings layout
`crosstalk.toml` keeps scope, matching, exports, AI. `streams.toml`: stream selection
(accounts in priority order, stream groups include/exclude, regex rules, ignore tags, tag
handling), match (sensitivity preset, replace/append, keep custom, allow same name, country
restriction + prefix countries, visible channel limit, EPG placeholder block), sort (quality
first, audio priorities, bitrate step, content floor, demote starved, throughput sorting),
probe (scope, freshness hours, timeouts, retries, per-account limits, content detection and
thresholds, throughput duration/TTL, streamlink hosts, ffprobe/ffmpeg paths), triage
(per-class rename template and group, format suffixes). Defaults are the plugins' defaults.
SM's `rate_limiting` maps to the write concurrency; scheduling settings become schedules,
windows and workflows.

### Host features
- **Windowed, resumable jobs**: a schedule entry may carry `window = "4h"` or
  `until = "06:00"`; the job checkpoints progress in the store and the next window resumes.
  The cancel/partial-result semantics are settled here once for every app (phase-1 note).
- **Per-key concurrency limiter** in core (used per M3U account; reusable).
- **Workflows**: `[workflows.<name>] steps = ["streams.probe", "streams.sort_apply"]`,
  schedulable as `"workflow.<name>"`, stops at the first failure, one run record per step
  plus a workflow record.
- **Webhook**: `POST /hooks/dispatcharr` with the bearer token (Connect webhook sends custom
  headers); `[hooks] m3u_refresh = ["streams.match_apply"]`, `epg_refresh = [...]`;
  actions or workflows; debounced (several M3U accounts refreshing at once trigger one run);
  unknown events are logged and ignored.
- **Run row subject**: run rows gain a `subject` kind (channel / stream / epg_source) so probe
  and watchdog rows stop overloading `channel_id`; migration backfills watchdog rows.

## Files to modify
- New crate `crates/streams` (package `crosstalk-streams`): `lib.rs`, `settings.rs`,
  `selection.rs`, `matching.rs` (collector), `quality.rs` (sort model), `mirror.rs`,
  `visibility.rs`, `probe/{mod,ffprobe,content,throughput,classify}.rs`, `triage.rs`,
  `restore.rs`, `tools.rs`, `test_support.rs`.
- `crates/crosstalk-matching`: `MatchProfile::StreamMapparr` flags (zones, designator repair,
  alias folding, one-timezone guard, token/number guards), country detection module, SM data
  differences.
- `crates/crosstalk-core`: `channel_ops.rs` (per-channel values, `MoveGroup(None)`, `Streams`
  field, `StreamListWriter`), `match_run.rs` (`Candidate for Stream`), new `limiter.rs`,
  `workflow.rs`, `hooks.rs`, `store.rs` (`stream_health`, run-row subject, job checkpoints),
  `scheduler.rs` (windows, workflows), `config.rs` (`[workflows]`, `[hooks]`), `dispatcharr/`
  (`M3uAccount.server_url/server_group`, stream provider id, `update_stream_stats`,
  current programmes).
- `crates/crosstalk-web`: hook route, workflow display, stream-shaped run rows.
- `crates/crosstalk`: registry, `preview_counterpart` pairs, CLI `run workflow.<name>`,
  `streams` benchmark subcommand.
- `Dockerfile` (ffmpeg), `config/streams.example.toml`, `config/crosstalk.example.toml`,
  README (actions, differences table, image size), `THIRD_PARTY_NOTICES.md` (SM + IC),
  `reference/` submodules for Stream-Mapparr and IPTV Checker, `scripts/upstream_drift.sh`
  (multiple references), parity scripts.

## Reuse
- Matcher: `Matcher::from_config`, `MatchProfile`, `CandidateIndex`, `match_run::score_names`
  (rayon off the runtime), alias/callsign/normalize modules.
- `core::scope::scope_channels`; `scope::sources::select` (M3U accounts as `SourceKind`).
- `channel_ops` plan/apply/run_rows/write_ops_report for every channel write, including
  stream lists; `Selection::channel_ids = Some(ids)` for result sets (never empty-as-all).
- `csv_report::Report`, `OutcomeBuilder`, `JobRunner`, scheduler, store, cross-process guard.
- Dispatcharr client: `streams()`, `channel_streams()`, `set_channel_streams()`,
  `m3u_accounts()`, `edit_channels()`, `set_profile_membership()`, `load_world_with`.
- Parity tooling: `scripts/parity_sweep.py` pattern, `channel_ops_parity.py`, `sim_parity.sh`.

## Steps
Each step: DeepSeek implements → gate → Muse reviews and fixes → gate → commit.
- [ ] 1. References: add Stream-Mapparr and IPTV Checker as pinned submodules; extend
  `upstream_drift.sh` to every reference; notices.
- [ ] 2. Core gaps: channel_ops per-channel values, `MoveGroup(None)`, `Streams` field +
  `StreamListWriter` (concurrent REST); run-row `subject`; client additions; parity harness
  still 0 mismatches.
- [ ] 3. Matcher: `MatchProfile::StreamMapparr` + country detection; parity sweep against SM's
  `fuzzy_matcher.py` (normalize, alias, callsign, match/score kinds).
- [ ] 4. `streams` crate skeleton: settings/schema/example, selection (accounts, stream groups,
  regex rules), registry, CLI pairs, `validate`/`status`.
- [ ] 5. Quality model + `sort_*` with parity harness vs SM `_sort_streams_by_quality`.
- [ ] 6. `match_*`: SM collector, grouping, dedupe, country, zones, OTA path, append/replace,
  keep custom; parity harness vs SM `_match_streams_to_channel` on fixture worlds; benchmark
  at 20k channels / 50k streams (synthetic).
- [ ] 7. EPG placeholder matching (SM's optional path) + `check_countries`,
  `scan_placeholders`, `test_rules`.
- [ ] 8. `mirror_*` and `visibility_*` with parity harnesses.
- [ ] 9. Host: per-key limiter, windowed resumable jobs + checkpoints, cancel semantics for
  every app (incl. watchdog's queued refreshes).
- [ ] 10. `probe`: ffprobe/ffmpeg invocation and parsing (fixture JSON/stderr from real tools
  in tests), classification parity vs IC, throughput mode, `stream_health`, stats write;
  ffmpeg in the image.
- [ ] 11. `triage_*` and `restore_*` on channel ops, from probe verdicts and run rows.
- [ ] 12. Host: workflows, Dispatcharr webhook, schedules for workflows; web display.
- [ ] 13. Docs: README actions + differences + image size + scale numbers, PLAN pointers,
  example configs.
- [ ] Phase end: Opus 5.5 review and fixes over the whole phase.

## Verification
- Gates every step: fmt, clippy `-D warnings`, `cargo test --locked --workspace`, all existing
  parity scripts (sim, sweeps, channel ops) plus the new ones; `actionlint`.
- Parity harnesses (Python, `-I`, plugin code imported or line-cited reimplementation over
  fixtures, never mirroring the Rust): matcher sweep for `StreamMapparr`, match collector,
  sort key, mirror plan, visibility, probe classification (from captured ffprobe/ffmpeg
  outputs), triage/restore selection. Each runs in CI if under a minute.
- Scale: a `crosstalk streams bench` (or test-only bench) on a synthetic world of 20k
  channels / 50k streams / 100 accounts: match and sort plan time, memory, and the number
  of write requests; recorded in the README. No O(channels × streams) loops without an index.
- Probe without network: ffprobe/ffmpeg are invoked through a trait; tests use recorded
  outputs (alive, dead, 429, audio-only, black, frozen, silent, placeholder). One opt-in
  test runs real `ffprobe` against a local file served by a test HTTP server.
- Webhook/workflow: route tests (auth, debounce, unknown event), workflow stop-on-failure.
- Live: none during execution; the user tests on a test instance after all phases, with
  the benchmark command for the bridge decision.

## Execution
Same as phase 1: DeepSeek V4.1 Flash (neuralwatt) implements each step, Muse Spark 1.3
Contributor reviews and fixes after each step, Opus 5.5 reviews and fixes at the end of the
phase. The orchestrator runs gates and owns commits; nothing is pushed without approval.

## Execution notes
- Step 2: Dispatcharr's `edit/bulk` accepts `streams` per row (ordered, replaces the list, one
  transaction, whole chunk 400s on one bad id). `RestStreamListWriter` uses it in 500-row
  chunks with per-channel fallback: ~42 requests for 20k channels instead of 20k.
- Step 2: a channel's group cannot be cleared over REST (`channel_group_id` has no
  `allow_null`). `MoveGroup(None)` plans and records, but apply refuses it per row. Restore of a
  channel whose recorded group was "No Group" needs the bridge; README divergences must say so.
