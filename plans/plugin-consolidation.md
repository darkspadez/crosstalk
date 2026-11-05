# Consolidating the Dispatcharr plugins into a few crosstalk sub-apps

Status: draft, open questions at the bottom. Action inventory: `plans/dispatcharr-plugins.md`.

## Context

cedro runs 9 plugins (115 actions). Ported 1:1, that would make 9 sub-apps that each
re-implement the same things: fuzzy matching, "rename, move or hide this set of
channels", placeholder EPG, scheduling, progress, CSV export, email reports. The user
wants a few sub-apps, each defined by **what it manages**, with the shared 75% living
once in the host or in shared crates.

Janitor today is 1:1 for matching only. `PLAN.md:26` deliberately left out the cleanup
actions and the watchdog in v1. The 9 missing actions are `scan_missing_epg`,
`export_results`, `add_bad_epg_suffix`, `remove_epg_assignments`, `remove_epg_from_hidden`,
`remove_epg_by_regex`, `remove_all_epg_from_groups`, `clear_csv_exports` and
`watchdog_run_check_now`. Most of them become shared channel operations below rather
than Janitor-only code.

## Where the overlap actually is

| Underlying function | Who does it today |
|---|---|
| Fuzzy name matching (normalize → alias → exact → substring → token) | Janitor (channel→EPG), Stream-Mapparr (stream→channel), Channel-Mapparr (channel→canonical name), Lineuparr (lineup→stream, →EPG, →logo). `matching_core.py` is byte-identical across the 4 |
| Country channel DBs + FCC callsign table | Janitor, Stream-Mapparr, Channel-Mapparr, Lineuparr |
| Assign EPG to channels | Janitor auto-match/heal, Lineuparr `apply_epg_match` |
| Remove EPG from channels | Janitor ×4 `remove_*`, Event Managarr `remove_epg_from_hidden` (same action name as Janitor's) |
| Placeholder / generated EPG | Sports Editor `fill_epg` + sport templates, Event Managarr dummy EPG |
| Rewrite programme data | Sports Editor virtual EPG transforms |
| Assign + order streams on channels | Stream-Mapparr, Lineuparr `apply_stream_match`/`resort_streams` |
| Probe stream health/quality | IPTV Checker (ffprobe), Stream-Mapparr `probe_throughput`; quality feeds stream ordering in Stream-Mapparr + Lineuparr |
| Rename channels | Channel-Mapparr, Name Cleaner, IPTV Checker (dead/blank/low-fps tags), Sports Editor, Janitor bad-EPG suffix |
| Move channels between groups | Channel-Mapparr categories, IPTV Checker dead/blank/low-fps, Lineuparr sync |
| Hide/show channels | Event Managarr, Stream-Mapparr `manage_channel_visibility`, Sports Editor past-date hide |
| Create channels/groups | Channel-Mapparr (from streams/M3U), Lineuparr (from lineup) |
| Logos | Channel-Mapparr (default, tv-logos), Lineuparr (EPG icon, Logo Manager, tv-logos) |
| Event-name date/time parsing | Event Managarr, Sports Editor |
| Schedule, progress, status, lock, validate, CSV, clear CSV, email report | every plugin (crosstalk host already does schedule/progress/lock/run history/CSV) |

## Approach (draft)

### Host and shared layers (not sub-apps)

- **crosstalk-matching** (exists): grows the per-plugin matcher variants as options on
  one engine, each still parity-tested against its plugin.
- **Channel ops** in crosstalk-core: one typed "selection → operation" primitive:
  select channels (groups, profile, regex, a previous run's result set), then rename
  (prefix/suffix/template), move to group, hide/show, remove EPG, delete. Every
  cleanup-style action in every plugin is a preset of this. It has dry run and CSV
  for free, and an undo record (Name Cleaner's backup/undo, generalized).
- **Generated EPG** in crosstalk-core: a placeholder/templated programme source used by
  fill, events and sports.
- **Host features**: schedules, progress, status, run lock, validate, CSV retention
  (replaces every `clear_csv_exports`), reports + optional notifications, and an inbound
  Dispatcharr Connect webhook (`m3u_refresh`, `epg_refresh`) that can trigger any action.
- **Workflows**: a host-level chain of actions across sub-apps (Lineuparr's `full_sync`,
  the Plugin-Workflow guide's Checker → names → streams → EPG order).

### Sub-apps: 4, by what they manage

| Sub-app | Manages | Absorbs |
|---|---|---|
| **guide** (today's janitor) | EPG assignment and programme data | Janitor (all 15), Lineuparr `apply_epg_match`, Sports Editor transforms + fill, watchdog |
| **streams** | Which streams a channel has, in what order, and whether they work | Stream-Mapparr, IPTV Checker, Lineuparr `apply_stream_match`/`resort_streams` |
| **lineup** | The channel list itself: names, numbers, groups, logos, creation | Channel-Mapparr, Channel Name Cleaner, Lineuparr `sync_channels`/`apply_logo_match` |
| **events** | Time-bound event channels | Event Channel Managarr, Sports Editor sport rename + templates |

Lineuparr disappears as a unit: its 3 halves land in lineup/streams/guide, and
`full_sync` becomes a workflow. IPTV Checker's rename/move/delete become channel-ops
presets fed by its result set.

Dropped as host-internal: `cleanup_periodic_tasks`, `cleanup_orphaned_tasks`,
`check_scheduler_status`, `clear_operation_lock`, `reset_progress`, `restart_dispatcharr`,
`clear_parse_counters`, `report_a_bug`. Reservoarr is not portable (runs per playback
inside Dispatcharr's container).

### Access (decided)
REST by default; ORM bridge plugin for writes the API lacks; Connect webhook for events;
optional read-only DB role for heavy reads.

### Decisions so far
- 4 sub-apps: guide / streams / lineup / events.
- `janitor` is renamed to `guide` cleanly: crate, id, `guide.toml`, CLI, UI, schedule keys,
  CSV prefix, docs. **No `janitor.toml` fallback** (v0.1.0 is a day old, single user).
  The upstream plugin keeps its name where it is a parity reference
  (`reference/Dispatcharr-EPG-Janitor-Plugin`, parity scripts).
- Order: phase 1 = shared channel ops + finish guide. Then streams, lineup, events.
- Further renames (clean break, nothing released is in use):
  - crates: `crates/janitor` → `crates/guide`, package `crosstalk-guide`; later
    `crosstalk-streams`, `crosstalk-lineup`, `crosstalk-events`.
  - setting `automatch_confidence_threshold` → `match_confidence_threshold`.
  - shared scope moves to `crosstalk.toml` now: `[scope] profiles, groups, ignore_groups`
    (was `channel_profile_name`, `selected_groups`, `ignore_groups`) and
    `[exports] retention_days` (was `csv_retention_days`). `guide.toml` keeps only guide
    settings; an app may override a shared value only where it genuinely differs.
  - run history: one-time store migration rewrites `app = 'janitor'` → `'guide'` and
    `automatch_*` → `match_*` in `runs` and `settings_history`.
  - CLI `crosstalk simulate` → `crosstalk simulate-streams` (it simulates stream→channel
    matching, ported from `match_sim.py`).
  - docs are rewritten in place, no archive: `PLAN.md` describes the four-app setup and points
    here; `AUDIT.md` / `PERFORMANCE_AUDIT.md` keep their findings but drop Janitor-as-the-app
    wording. Doc comments citing EPG Janitor as a *parity reference* stay.

## Shared layer extracted in phase 1

Goal: phases 2–4 only write what is genuinely theirs. Anything two apps will need moves out
of the guide crate now, while there is exactly one caller to keep green.

### One fuzzy matcher, edited once
- `crosstalk-matching` stays the only matcher. Plugin variants are **named profiles on one
  engine** (`MatchProfile::EpgJanitor` now; `StreamMapparr`, `ChannelMapparr`, `Lineuparr`
  added as flag sets in later phases), not copies. A fix to normalize, aliases, callsigns,
  tokens or scoring therefore reaches every app at once.
- Where plugins genuinely differ (Janitor's 5-char subset guard vs the core's 4, zone
  handling, designator tokens) the difference is one flag on `MatchOptions` with the plugin
  named in its doc comment, and each profile keeps its own parity sweep.
- Decision point after phase 3: once parity against the plugins no longer matters, collapse
  profiles to one tuned default and keep only the flags users actually change.
- Matcher construction from host config (`matcher_aliases`, `known_callsigns`, `tag_list`,
  today in `janitor/src/lib.rs`) moves to `crosstalk-matching` as one
  `Matcher::from_config(&MatchingConfig, MatchProfile)`.

### Moved from the guide crate to `crosstalk-core`
| From | To | Why it is shared |
|---|---|---|
| `scope.rs` `scope_channels` + profile scope | `core::scope` (reads `[scope]`) | every app selects channels by profile/groups/ignore groups |
| `scope.rs` `scope_entries` (source filter) | `core::scope::sources` generic over source kind | guide filters EPG sources, streams filters M3U accounts the same way |
| `automatch.rs` `walk_for_channel` loop (rank → threshold → tie-break → AI gate → reason) | `core::match_run`, generic over a `Candidate` (id, name, tags, extra context) | guide (channel→EPG), streams (stream→channel), lineup (lineup→stream, →logo) run the same loop |
| `ai_gate.rs` shortlist / request / combine / ask | `core::ai_gate`, generic over `Candidate` | AI verification of a borderline match is the same for any target |
| `settings.rs` `AiSettings` | `[ai]` in `crosstalk.toml`, per-app override | one AI policy, as with scope |
| `settings.rs` `whole_number`, `clamp_u8`, `clamp_unit` | `core::subapp` settings helpers | every schema needs them |
| `write_report` + `outcome()` (automatch, heal) | `core::csv_report` row writer + `JobOutcome` builders | every action writes a CSV and a summary |
| `MatchRow` reason text | stays guide-specific; the reason *builder* is shared | |

Guide keeps only: EPG-specific selection (programme-window checks), heal, the cleanup
presets, the watchdog, and its settings.

### Dispatcharr client completed once
`crosstalk-core/src/dispatcharr.rs` gains every read and write the later phases need, typed
and tested, so phases 2–4 add no client code unless a new endpoint appears:
streams, M3U accounts, logos, channel groups (CRUD), channel create / `from-stream/bulk`,
`edit/bulk`, `edit/bulk-regex`, profile membership bulk-update, channel stream list write,
EPG sources (incl. dummy) CRUD, EPG refresh, plugin run (for the bridge). `World` loads
lazily per kind so an app only fetches what it uses.
- Scope: live sports schedule feed is **in** (events phase). Out: IPTV Checker
  permanent deletes, Newsflasharr email, generalized undo. Every write keeps dry run + CSV.

## Phase 1: shared channel ops + finish guide

### What the 9 missing Janitor actions really do (from `plugin.py`)
| Plugin action | Selection | Operation | guide action |
|---|---|---|---|
| `scan_missing_epg` | in-scope channels with EPG assigned and no programmes in the window | read-only report (stores result set) | `scan` |
| `export_results` | last scan | CSV | dropped: every run already writes its CSV |
| `add_bad_epg_suffix` | last scan's result set | append suffix if absent; optionally clear EPG | `tag_missing` |
| `remove_epg_assignments` | last scan's result set | clear EPG | `clear_missing` |
| `remove_epg_from_hidden` | channels disabled in the configured profile(s) | clear EPG. **Deliberate divergence:** the plugin also deletes the programme rows of each entry, which blanks visible channels sharing it; crosstalk only unassigns | `clear_hidden` |
| `remove_epg_by_regex` | channels matching a regex within groups | clear EPG | `clear_by_regex` |
| `remove_all_epg_from_groups` | every channel in the groups | clear EPG | `clear_groups` |
| `clear_csv_exports` | — | delete exports | host: one "clear exports" in the UI over retention |
| `watchdog_run_check_now` | active non-dummy sources with mapped channels, minus excludes | refresh sources that are `error` or whose horizon < now + N h; re-read; classify recovered/still broken | `watchdog` (schedulable) |

The plugin uses a results JSON file from the last scan. crosstalk uses the scan run's
`run_rows`. There is no age limit: the confirm dialog shows when the scan ran and its count,
and `plan` re-reads every channel's current name and EPG first, so a stale scan cannot
double-suffix or clear a channel that has since been reassigned.

### REST coverage (checked in Dispatcharr source, `apps/channels/api_views.py`, `apps/epg`)
No bridge plugin is needed for phase 1:
- rename / move group: `PATCH /api/channels/channels/edit/bulk/` (list of `{id, name?, channel_group_id?, …}`, one transaction)
- regex rename: `POST /api/channels/channels/edit/bulk-regex/` (for lineup later)
- hide/show: `PATCH /api/channels/profiles/{id}/channels/bulk-update/`
- clear EPG: `POST /api/channels/channels/batch-set-epg/` with `epg_data_id: null` (the existing `batch_set_epg` client already chunks and retries)
- refresh a source: `POST /api/epg/import/` `{id}` (Celery, async: poll the source's status)
- source horizon for the watchdog (checked): no "latest end per source" route exists (`/api/epg/programs/` is unfiltered and unpaginated; `/programs/search/` orders by start only). The client probes `/programs/search/?epg_source=&end_after=&page_size=1` (one request answers "horizon < now + N h") and bisects it to the minute when the exact horizon is wanted (~16 requests). No DB role needed.
- hide/show (checked) is two different things: profile membership (`bulk-update`) and global `hidden_from_output` (channel field via `edit/bulk`). Channel ops expose both.

### Channel ops (new, `crosstalk-core/src/channel_ops.rs`)
- `Selection`: groups (wildcards via `wildcard.rs`), profile-hidden, name regex, explicit ids
  from a prior run's `run_rows`; always intersected with the app's scope.
- `Op`: `Rename{suffix|prefix|template}`, `MoveGroup`, `SetVisible(bool, profile)`, `ClearEpg`.
  `Delete` is not built (out of scope).
- `plan(world, selection, ops) -> Vec<Change>` is pure: no-op changes dropped (e.g. suffix
  already present). `apply(client, changes)` batches per endpoint and records per-row results.
- Dry run = plan + CSV, using the existing `csv_report`. Apply = plan, confirm, apply, CSV.
- `csv_report::EXPORT_PREFIX` becomes per-app (`guide_`, `streams_`, …), and the preamble
  title comes from the app name.

## Files to modify (phase 1)
- `crates/janitor/` → `crates/guide/` (package `guide`); `Cargo.toml` workspace members/deps
- `crates/guide/src/lib.rs`: id/name, new actions; `settings.rs`: suffix, remove-with-suffix,
  regex, clear groups, watchdog settings; new `scan.rs`, `cleanup.rs`, `watchdog.rs`
- `crates/crosstalk-core/src/channel_ops.rs` (new), `dispatcharr.rs` (bulk edit, membership
  bulk-update, clear EPG, refresh source, source horizon), `csv_report.rs` (per-app prefix),
  `config.rs` + `scheduler.rs` (schedule key examples/tests)
- `crates/crosstalk/src/{main,registry}.rs`, `crates/crosstalk-web/src/{state,tests}.rs`
- `config/janitor.example.toml` → `config/guide.example.toml`; `crosstalk.example.toml`
- `README.md`, `PLAN.md` (v1 scope note), `docker-compose.example.yml` if it names the file,
  `scripts/{parity_sweep.py,sim_parity.sh}` only where they name the crosstalk app

## Reuse
- `crosstalk-core`: `Dispatcharr::batch_set_epg` (chunking/retry), `load_world`, `wildcard.rs`,
  `csv_report`, `store::{start_run, append_rows, rows}`, `JobRunner`, `scheduler`, `ActionKind` +
  `with_confirm`
- `janitor/scope.rs` (group/profile/source scope resolution) for every selection
- `janitor/automatch.rs` / `heal.rs` programme-window check (`has_programs`) for `scan`

## Steps (phase 1)
- [ ] Rename `janitor` → `guide` (package `crosstalk-guide`), `automatch_*` → `match_*`, `match_confidence_threshold`, `simulate-streams`; store migration for run history (one commit, tests green, no behaviour change)
- [ ] Move shared scope/exports/AI settings to `crosstalk.toml` (`[scope]`, `[exports]`, `[ai]`); update example configs
- [ ] Extract to core: `scope`, generic `match_run` + `Candidate`, `ai_gate`, settings helpers, report/outcome builders; `Matcher::from_config` + `MatchProfile::EpgJanitor` in matching. Parity sweeps and golden tests unchanged
- [ ] Per-app CSV prefix + preamble title
- [ ] Dispatcharr client: complete endpoint set above (bulk edit, membership, clear EPG, refresh, horizon, streams, M3U, logos, groups, create, dummy sources, plugin run); lazy `World`
- [ ] `channel_ops`: Selection, Op, pure `plan`, `apply`, CSV rows; unit tests
- [ ] guide `scan` (stores result set in `run_rows`)
- [ ] guide `tag_missing`, `clear_missing`, `clear_hidden`, `clear_by_regex`, `clear_groups`
- [ ] guide `watchdog` (port `epg_watchdog.py` decision logic verbatim + its tests)
- [ ] Host "clear exports" button
- [ ] Docs: README actions table + divergences table, example config, PLAN.md scope line
- [ ] `LICENSE` (MIT) + `THIRD_PARTY_NOTICES.md` (EPG Janitor, PiratesIRC) + `reference/` pin note

## Verification
- `cargo test --workspace`, clippy, fmt; CI on Namespace.
- Parity: for each new action, fixture worlds (channels/groups/profiles/EPG) run through both
  the plugin's selection logic (extracted into a Python harness like `scripts/parity_sweep.py`)
  and `channel_ops::plan`; selected ids and resulting payloads must be identical.
- Watchdog: port the plugin's watchdog unit cases as Rust tests.
- Live: dry-run every new action against cedro and compare counts with the plugin's own
  dry run / scan on the same data; then one apply on a test group.

## Execution (when approved)
- Implementer: DeepSeek V4.1 Flash on neuralwatt (`neuralwatt/deepseek-v4.1-flash`), one
  subagent per step. If neuralwatt times out the way GLM did in the bench, stop and ask
  rather than switching provider silently.
- Step reviewer: Muse Spark 1.3 Contributor (`meta/muse-spark-1.3-contributor`) after each
  step. It reviews against this plan and the plugin source **and fixes what it finds in the
  same pass**, then reports what it found and what it changed (BLOCKER / SHOULD FIX / NIT,
  each marked fixed or left with a reason).
- Phase reviewer: Opus 5.5 (`claude-opus-5-5`) at the end of each phase. It reviews the whole
  phase diff (cross-step consistency, parity, divergences table, docs) and fixes what it finds.
- Loop per step: implement → gate (`cargo test --workspace`, clippy, fmt) → Muse review + fix
  → gate again → commit. Per phase: all steps → Opus review + fix → gate → parity +
  live dry run on cedro → phase commit. A reviewer fix that fails the gate goes back to the
  same reviewer once, then to the user. The orchestrator (this session) runs the gates and
  owns commits; nothing is pushed without approval.

## Future phases: merges, names, and what each needs

Each future phase gets its own detailed step list when the previous one lands. What is
fixed now: which plugin actions merge into which crosstalk action, the names, the shared
pieces each phase adds, and the cross-cutting rules below.

### Naming conventions (all sub-apps)
- Sub-app ids `guide`, `streams`, `lineup`, `events`. Settings `<id>.toml`, schedule keys
  `<id>.<action>`, CSV prefix `<id>_`, CLI `crosstalk run <id> <action>`.
- An action that writes is a pair `<verb>_preview` / `<verb>_apply` (today's convention).
  Read-only actions are a single id. Verbs are shared across apps where the meaning is the
  same: `match`, `sort`, `clean`, `logos`, `visibility`, `scan`.
- Every app keeps `validate` and `status`. Plugin-side housekeeping (Celery task cleanup,
  scheduler status, lock clearing, progress reset, restart, bug report) is never ported:
  the host owns schedules, locks, progress and run history.
- Decided: `automatch_*` → `match_*` in phase 1's rename, so `guide.match` reads like
  `streams.match` (schedule key `guide.match_apply`). `heal_*` keeps its name. The phase-1 cleanup actions also follow the pair rule:
  `tag_missing_*`, `clear_missing_*`, `clear_hidden_*`, `clear_by_regex_*`, `clear_groups_*`.

### Phase 2: `streams` (Stream-Mapparr + IPTV Checker + Lineuparr's stream half)

> Detailed plan: `plans/phase-2-streams.md`. Changed there: Lineuparr's stream half moves to
> phase 3 (it needs the lineup files); Stream-Mapparr's sort model is the one model; the
> webhook and workflows are built in phase 2; no bridge (REST behind a writer seam).

| crosstalk action | Merged from | Notes |
|---|---|---|
| `match_preview` / `match_apply` | SM `preview_changes`, `add_streams_to_channels`, `match_us_ota_only`; LU `preview_stream_match`, `apply_stream_match` | OTA-only becomes a scope option. Replace vs append (`overwrite_streams` / LU `preserve_existing_streams`) and country restriction are options, not separate actions |
| `sort_preview` / `sort_apply` | SM `sort_streams`, LU `resort_streams` | One quality model (name tags + probe stats + throughput) |
| `mirror_preview` / `mirror_apply` | SM `fill_mirror_backups` | |
| `probe` | IPTV `load_groups` + `check_streams`, SM `probe_throughput` | Writes stream stats. Throughput is a probe mode. Cancel/progress/results are host features |
| `triage_preview` / `triage_apply` | IPTV rename/move dead, blank-screen, low-fps (6 actions), `add_video_format_suffix` | Channel-ops presets over the last probe's classes; per-class op in settings |
| `restore_preview` / `restore_apply` | IPTV `restore_channels` | Needs triage to record each channel's original group/name in `run_rows` |
| `visibility_preview` / `visibility_apply` | SM `manage_channel_visibility` | Enable/disable by stream count, via channel ops |
| `check_countries`, `scan_placeholders`, `test_rules` | SM `check_stream_countries`, `scan_placeholder_names`, `test_regex_rules` | Read-only reports/tools |

- **Not ported:** IPTV `delete_dead_channels` (out of scope), all email actions, SM
  `report_a_bug`. IPTV `view_table`/`view_results`/`export_results` = the host run page + CSV.
- **Event hooks:** SM `on_m3u_refresh` → Connect webhook trigger.
  SM `on_iptv_checker_scan` / `wait_for_iptv_checker` → a host workflow `probe → sort_apply`.
  The two plugins coordinating through a hook becomes one app.
- **Shared pieces added:** the Stream-Mapparr matcher variant as `MatchOptions` (its
  `fuzzy_matcher.py` adds zones, alias folding and designator tokens; parity pass like
  Janitor's); a country-detection module (SM `stream_prefix_countries`/`restrict_matching_to_country`,
  LU `detect_stream_country`) in `crosstalk-matching`; the stream quality model in `crosstalk-core`.
- **Host features added:** resumable windowed jobs (IPTV's overnight windows that pause and
  resume) and per-app concurrency/rate limits for probing.
- **Image:** `ffprobe` in the runtime image (alpine `ffmpeg` package; measure the size cost).
- **Writes:** channel stream lists are writable over REST (`streams` on the channel serializer,
  ordered), but one channel per request. Stream `stream_stats` is writable over REST (checked: `PATCH /api/channels/streams/{id}/`, also
  for provider streams), so bulk stats may not need the bridge. At ~21k channels/streams these are the first **bridge** candidates (bulk
  assign, bulk stats). Decide from a timed run, not up front.
- **Probe semantics to keep exactly:** skipped is never dead; a channel is dead only if every
  stream failed.

### Phase 3: `lineup` (Channel-Mapparr + Channel Name Cleaner + Lineuparr's channel half)

> Detailed plan: `plans/phase-3-lineup.md`. **Goal reset (2026-11-04):** apps must share one
> implementation and give the same answer, so phase 3 opens by collapsing the per-plugin matcher
> profiles into one engine (rules chosen on a labelled corpus), one merged data set, a shared
> channel identity, one tag vocabulary / quality model / template engine / logo resolver, and
> migrates guide and streams onto it. Plugin parity becomes an oracle report, not a gate. Also:
> host-level undo, ownership of created channels, hide instead of delete, AI escalation chain
> (Jev → LLM). This supersedes "One fuzzy matcher, edited once"'s per-plugin profiles.

| crosstalk action | Merged from | Notes |
|---|---|---|
| `clean_preview` / `clean_apply` | NC `preview`, `apply` | Junk stripping with protect-list and collision strategy. Also runs as the first stage of `names_*` |
| `names_preview` / `names_apply` | CM `load_and_process_channels`, `rename_channels`, `rename_unknown_channels` | Canonical names (FCC OTA + country DBs, `{NETWORK}` templates). Tagging unmatched channels is an option |
| `categorize_preview` / `categorize_apply` | CM `organize_by_category` | |
| `logos_preview` / `logos_apply` | CM `apply_logos`, `apply_tv_logos`, `replace_tv_logos`; LU `apply_logo_match` | One source order (EPG icon → Logo Manager → tv-logos → default); `replace_existing` option |
| `create_preview` / `create_apply` | CM `import_m3u_streams`, `create_channels_from_streams` | Mode: by category or one per broadcast station |
| `sync_preview` / `sync_apply` | LU `sync_channels` | Build groups/channels/numbers from a lineup file; adds and renumbers, never deletes |
| `test_name` | NC `test_name` | Read-only tool |
| workflow `full_sync` | LU `full_sync` | Host workflow: `lineup.sync_apply` → `streams.match_apply` (lineup mode) → `guide.match_apply` → `lineup.logos_apply` → enable in profile |

- **Lost from Name Cleaner:** `undo_last` (generalized undo is out of scope). The old → new
  CSV of every apply remains; a manual revert would come from it.
- **Data:** Channel-Mapparr's country files and FCC table differ from Janitor's copies
  (byte diff earlier). Rule: parity-test each action against *its* plugin's data first, then
  unify the tables in one deliberate, listed change. Lineup files (~30) vendored under
  `data/lineups/`.
- **Network fetches:** tv-logos repo index, cached on disk with a TTL; shared fetch cache in core.
- **Writes:** channel create, group create, logo create, number changes: all REST
  (`channels`, `from-stream/bulk`, `groups`, `logos`, `edit/bulk`).

### Phase 4: `events` (Event Channel Managarr + Sports Editor's sports half)

| crosstalk action | Merged from | Notes |
|---|---|---|
| `visibility_preview` / `visibility_apply` | ECM `dry_run`, `run_now`; SE past-date hide | Ordered rule list (past date, too far ahead, no date, wrong weekday, placeholder, custom regex), duplicate-event hiding |
| `guide_preview` / `guide_apply` | ECM managed dummy EPG + per-group sources | Dispatcharr custom dummy sources, created and seeded over REST |
| `sports_preview` / `sports_apply` | SE `sports_editor_rename_now`, `sports_editor_epg_now` | Live schedule feed (api.tickarr.com) + game-thumbs logos, Pregame/Live/Postgame blocks |
| — | ECM `remove_epg_from_hidden` | Same as `guide.clear_hidden_*` (identical meaning): not duplicated |
| — | SE `release_orphaned_hidden` | Not ported: crosstalk records what it hid in `run_rows` from the start |

- **Shared pieces added:** an event-name parser (US, Swedish pipe, `@` layouts, time zones)
  used by visibility and sports; the generated-EPG layer.
- **Writes:** dummy sources are REST. Sports EPG blocks are programme rows on a source:
  **bridge** (EPGData is read-only over REST and there is no bulk programme write).
- **Triggers:** ECM/SE `on_m3u_refresh` → Connect webhook.
- **Attribution:** SE's template design credits Pharaoh-Labs' Teamarr (used with permission)
  and game-thumbs (MIT). Carry both credits into the notices.

### Deferred into `guide`, after the bridge exists
SE `setup`, `apply_now`, `preview`, `sample`, `test_rule`, `teardown` become `guide`
`transform_*` (cleaned virtual copies of EPG sources) and `fill_*` (SE `fill_epg`, placeholder
schedule for channels with no EPG). Both write EPG entries and programme rows, so both wait
for the bridge. `clear_parse_counters` and `restart_dispatcharr` are not ported.

### The bridge plugin (`crosstalk-bridge`)
- Lives in this repo (`bridge/`), built and attached to releases by `build.yml` as a
  Dispatcharr plugin zip, versioned with crosstalk.
- Invoked through Dispatcharr's existing `POST /api/plugins/plugins/<key>/run/` with a JSON
  payload, so it adds no URL routes and reuses Dispatcharr's admin auth.
- Only ORM writes the API cannot do, each idempotent and batched: bulk stream assignment,
  bulk stream stats, EPG source + EPGData + programme replace for one source, guide-cache
  invalidation. No reads, no scheduling, no matching. All logic stays in crosstalk.
- crosstalk checks the bridge's version on startup and disables the actions that need it
  when it is missing or older, with the reason in the UI.
- Built only when the first phase needs it (phase 2 if the timed run says so, otherwise phase 4).

### Cross-cutting rules for every phase
- **Licensing:** all nine plugins are MIT (SE states it in its README only). MIT needs the
  copyright notice kept with ported code: add `LICENSE` (crosstalk declares MIT in
  `Cargo.toml` but ships no file) and `THIRD_PARTY_NOTICES.md` listing each ported plugin.
  This is already owed for Janitor, so it goes into phase 1.
- **Upstream drift:** PiratesIRC commits most days. Each ported plugin is a pinned submodule
  under `reference/`; a `scripts/upstream_drift.sh` lists upstream commits since the pin that
  touch ported files, run before each phase and on a schedule in CI.
- **Divergences:** every deliberate difference from a plugin (e.g. `clear_hidden` never
  deleting programmes) goes in one README table, with the reason.
- **Cut-over on cedro:** never run a plugin and its crosstalk replacement writing the same
  thing. Per phase: crosstalk previews alongside the plugin → compare → disable the plugin's
  schedules and refresh hooks → enable crosstalk's → uninstall the plugin after a quiet week.
- **Shared settings move to the host:** scope (profiles, groups, ignore groups), match
  sensitivity, custom aliases, country DBs, rate limiting, CSV retention and schedules are
  repeated across plugins today. They live once in `crosstalk.toml` (where `[matching]`
  already holds normalization, country DBs and aliases), and an app overrides one only
  when it really differs.

## Open questions
None. Future phases get their own step lists and questions when they start.

## Phase 1 review notes

Findings from the end-of-phase review that later phases must build on (fixed items are in the
"Phase 1 review fixes" commit; these are what remains true or open):

- **`Selection::channel_ids` is `Option<Vec<i64>>`.** `None` = no id filter, `Some(vec![])` =
  select nothing. Before the fix an empty scan made `clear_missing_apply` select every in-scope
  channel. Any phase that feeds a prior run's result set (streams `triage`/`restore`, events
  `visibility`) must go through `Some(ids)`, never an empty `Vec` meaning "all".
- **Channel ops are uniform per selection.** Every `Op` applies the same value to every selected
  channel. Per-channel values (streams `restore` from `run_rows` before/after, lineup `names_*`
  canonical names, numbers, logos) have no `Op` yet, and `Change::planned` is private. Phase 2
  needs either a per-channel op (`Op::Set { field, values: HashMap<i64, Value> }`) or a public
  plan-from-changes constructor. `MoveGroup` cannot target "No Group", and there is no
  `SetEpg`, `SetLogo`, `SetNumber` or stream-list field. `ChannelEdit` in the client already
  carries all of them.
- **`match_run::walk` picks one winner.** Stream-Mapparr attaches *every* matching stream
  (replace/append), so streams needs a multi-select walk next to `walk`, or it can call
  `score_names` and threshold the ranked list itself. `Candidate` and `ai_gate` are already
  generic. No `Candidate` impl for `Stream` exists yet.
- **Run rows are channel-shaped.** The watchdog stores source ids in `channel_id` with
  `field = "epg_source"`, and a scan row has an empty `field`. A restore must select rows by
  `Field::from_label(field)` (which returns `None` for both), never by `channel_id` alone.
- **Cancel semantics.** A cleanup apply cancelled before any write is now recorded `cancelled`.
  One cancelled after a batch landed stays `succeeded`, with the unwritten rows `failed:
  cancelled`. The watchdog still bails on cancel without recording rows for refreshes it
  already queued. Phase 2's resumable probe jobs should settle this once for every app.
- **Cross-process guard.** The partial unique index stops two live runs per app across daemon
  and CLI. A daemon *restart* while a CLI run is live still retires that run's row, so for that
  run the guard is gone. A shared (CLI) open of a pre-index database with duplicate stale
  `running` rows now skips the index until the daemon retires them.
- **`status` cannot show a run in progress.** It is itself a run, so the one-run-per-app guard
  refuses it while another guide run is live. Its description still claims otherwise. Fix it
  with an offline/unguarded action kind when the host gains one.
- **Programme checks cost one request per distinct EPG entry.** This affects `scan`, `match` and
  `heal`, which is thousands of requests at cedro scale where the plugin needs one SQL query.
  This is the first read worth moving to the optional read-only DB role.
- **CLI `--dry-run` redirect is a hard-coded pair map** (`preview_counterpart` in
  `crates/crosstalk/src/main.rs`). A test fails when a registered `_apply` lacks an entry, so
  each phase adds its pairs there.
- **v0.1.0 exports** were named `epg_janitor_*`, the plugin's own prefix, so crosstalk's
  retention and `exports clear` never touch them. Delete them on cedro by hand once.
