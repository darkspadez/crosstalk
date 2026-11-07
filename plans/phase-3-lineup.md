# Phase 3: the `lineup` sub-app

Status: ready for review. Parent plan: `plans/plugin-consolidation.md` (phase 3 table,
cross-cutting rules). Builds on phase 2 (`plans/phase-2-streams.md`, especially "Phase 2 review
notes"). Base: branch `plan/phase-2-the-streams-sub-app` at `bf1af38` (phase 1 + 2, 1169 tests).

## Context

`lineup` owns the channel list itself: names, numbers, groups, logos, and creating channels.
It replaces Channel-Mapparr (13 actions), Channel Name Cleaner (5) and Lineuparr (11), and
Lineuparr's stream half lands here as a lineup mode of `streams.match` (moved from phase 2).
Upstream pins at planning time: Channel-Mapparr `45ba787`, Lineuparr `9f6a5ff`, Name Cleaner
`d68b195` (all equal to upstream HEAD on 2026-11-04).

Goal (reset, see below): one multi-threaded system whose apps complement each other, with
no duplicated logic and the same answer from every app. Phase 3 therefore **starts by
unifying the matching layer, data, identity, tags, quality, templates and logos**, migrates
guide and streams onto it, and builds lineup on it. Plugins are oracles, not targets.

## Actions

| crosstalk action | Kind | Merged from |
|---|---|---|
| `clean_preview` / `clean_apply` | Read / Write | NC `preview`, `apply` |
| `names_preview` / `names_apply` | Read / Write | CM `load_and_process_channels`, `rename_channels`, `rename_unknown_channels` (tag unmatched = option) |
| `categorize_preview` / `categorize_apply` | Read / Write | CM `organize_by_category` |
| `logos_preview` / `logos_apply` | Read / Write | CM `apply_logos`, `apply_tv_logos`, `replace_tv_logos` (`replace_existing` option); LU `apply_logo_match` (tier order as a source list) |
| `create_preview` / `create_apply` | Read / Write | CM `import_m3u_streams` (mode `streams`), `create_channels_from_streams` (mode `stations`) |
| `sync_preview` / `sync_apply` | Read / Write | LU `sync_groups` + `sync_channels` |
| `tidy_preview` / `tidy_apply` | Read / Destructive | LU's post-match cleanup, as hide |
| `test_name` | Read | NC `test_name` (+ shows CM's canonical name) |
| `validate`, `status` | Read | every app |
| `streams.match` lineup mode | (streams) | LU `preview_stream_match`, `apply_stream_match` via SM's collector |
| `guide.match` over the lineup scope | (guide) | replaces LU `apply_epg_match` |
| workflow `full_sync` (example config) | — | LU `full_sync`: `lineup.sync_apply` → `streams.match_apply` (lineup) → `guide.match_apply` (lineup) → `lineup.logos_apply` (lineup) → `lineup.tidy_apply` |
| host `undo` | Write | NC `undo_last`, generalized |

Not ported: CM/LU `email_report*`, `clear_csv_exports` (host), `plugin_status` (status),
usage counters, LU `resort_streams` (`streams.sort_apply` with lineup scope), LU's matcher and
EPG scorer (see Decisions).

## Known gaps from phase 2 review notes
- Channel ops need `Field::Number`, `Field::Logo` and a create stage (channels, groups, logos)
  with rows; decide the create shape once.
- `MoveGroup(None)` not applicable over REST.
- Lineuparr's stream half must reuse `matching::build_groups` / the collector, not a second walk.
- Chains (`full_sync`) are workflows, not hook targets.

## Research findings (CM = Channel-Mapparr, LU = Lineuparr, NC = Name Cleaner)

Line numbers are leads from the research pass; each step re-derives with its own citations.

### Shared facts
- `matching_core.py` is the same pinned file (sha `33ca1c61…231d`) in all five matcher plugins.
- Dispatcharr `Channel.name` is **not unique** (`apps/channels/models.py:542`); channel number
  uniqueness per group is only checked in `Channel.clean()` (`:648`), which bulk/ORM writes skip.
- Both CM and LU fetch the tv-logos index from GitHub anonymously, uncached between runs (CM:
  recursive git tree; LU: contents API, silently capped at ~1000 entries). Logos are stored as
  `Logo(name, url=raw.githubusercontent…)`, never downloaded.
- Both phone home usage counts by default (`plugin-stats.dpas.workers.dev`): not ported.

### Channel-Mapparr (13 actions; ~4.9k-line plugin.py + matcher, market index, seeder)
- **Names**: OTA first (US only): callsign ladder P1–P4 with a word-callsign denylist,
  FCC `networks.json` (~6.8k stations, differs from SM's 1.9k table) + supplemental, network
  override from the stream-style prefix, template default `{NETWORK} - {STATE} {CITY} ({CALLSIGN})`
  (callsign suffix stripped), optional market fallback. Premium otherwise: alias reverse index →
  exact → substring → token-sort with Janitor-style majority guards at every stage and a
  trailing-number guard; result re-attaches regional/extra/bracket tags. Presets 70/80/90/95.
  Unmatched → `" [Unk]"` suffix (re-tags already-tagged names: plugin bug).
- `load_and_process_channels` computes and saves a JSON; `rename_*` replay it (no re-match).
- **Categorize**: category from the premium DB record; group = category name, get_or_create;
  one bulk move of channels whose group differs. OTA has no category here.
- **Logos**: default logo by name; tv-logos match (station-format files first, then fuzzy
  ratio ≥85 or the sensitivity on replace, then EPG icon, then network logo); replace writes a
  JSON backup first.
- **Create**: `import_m3u_streams` = one channel per matched stream (name, number from max+1,
  group per category), linking that stream, name-suffix dedupe (re-runs duplicate);
  `create_channels_from_streams` = one channel per OTA station from unattached streams in source
  groups, no streams attached, numbers from a start skipping used ones, case-insensitive name
  skip.
- **Matcher**: CM's `normalize_name` differs from Janitor's in ~8 places (quality/prefix/regional/
  misc/geo tables, `&`, dots, suffix strip), its own callsign ladder and staged `fuzzy_match`
  with guards; its own (older, deduplicated) country DBs, larger FCC table, ~230 aliases. A
  `MatchProfile::ChannelMapparr` needs a parametrised normalizer variant + a match entry point,
  plus OTA-naming code that does not exist in Rust yet (affiliation parse, name format, market
  index, tag re-attachment).

### Channel Name Cleaner (5 actions, 481 lines)
Fixed rule order (unicode marks, NFKC, junk-only brackets, pipe segments, `+1`, custom regex,
casing, whitespace); protect list (groups, foreign scripts, leading `&`, regex); collision
strategies suffix / numeric / skip against existing names; CSV backup then bulk rename;
undo restores the last apply where the name is unchanged. Separate from CM's normaliser (that
only makes match keys); runs as its own write stage.

### Lineuparr (11 actions; 29 lineup files, 7,485 channels, 756 KB)
- Lineup JSON: `categories: {cat: [{name, number, aliases?, ...}]}`; country from `CC_` name
  prefix → category name → filename. `category_detail` folds categories.
- **sync_channels**: group `"<prefix>: <category>"` get_or_create; channel get_or_create by
  `(name, group)` (adopts a user's same-named channel); numbering modes lineup / auto_next /
  auto_highest / specific — the auto modes **renumber every existing channel on each rerun**;
  enables channels in named profiles (creates membership rows).
- **Stream match**: its own per-candidate scorer `match_all_streams` (alias + callsign rescue,
  exact/substring/token-sort with core majority guards, number boost, callsign anchor, country
  filter with US↔MX exceptions and a provider-group pass, East/West/Pacific filter); attaches
  **every** candidate above threshold; order `(tier, m3u_priority)`. Not SM's collector.
  Replace mode then **deletes every stream-less channel in groups matching the lineup's group
  names** (no ownership marker, ignores the single-channel filter).
- **EPG match**: the same `match_all_streams` over EPG entries (programme-data pool first,
  country-first), top hit wins, never replaces an assigned guide. Not Janitor's algorithm.
- **Logos**: EPG icon → Logo Manager name → tv-logos (simple ratio ≥85).
- `full_sync`: groups → channels → streams → EPG → logos; continues after failures in the
  last three steps.
- Matcher: core normaliser + `_STRIP_BARE_REGION` (crosstalk's profile table wrongly says no
  plugin sets it), core `_has_token_overlap` (crosstalk ports only Janitor's), no known
  callsigns; a third entry point next to Janitor's `match_all_streams`. Country detector differs
  from SM's (`AR` excluded, `QA/VN/PK/SI/ET` added, `(PLUTO X)`), so the `CountryDetector` seam
  needs a Lineuparr implementation with category/prefix/cross-border rules.

## Goal reset (2026-11-04, user)

The point of the port is one multi-threaded Rust system whose apps **work with and complement
each other**, with **no duplicated logic** and **the same answer from every app** for the same
input. Per-plugin profiles (`EpgJanitor`, `StreamMapparr`, and the planned `ChannelMapparr`)
contradict that: guide and streams already normalize and match the same name differently.
Plugin parity stops being the target; it becomes an *oracle* used to choose and validate the
unified behaviour.

Decided:
- **Unify first**, at the start of phase 3: one normalizer, callsign ladder, alias set,
  channel/station database, country detector and quality model; guide and streams move onto
  it; lineup is built on it. No `ChannelMapparr` profile is written; the existing profiles
  are retired from production code.
- **Best-of by a labelled corpus**: every rule where the plugins disagree is decided by which
  variant scores best on labelled real name pairs; each choice and its evidence goes in one
  decisions table. Plugin harnesses remain as oracles that report differences.
- **One merged data set**: newest, deduplicated union per country; one station table (CM's
  FCC table + SM's); one alias table (union, conflicts listed); one lineup-file set.
- **Shared channel identity**: one module resolves channels, streams and EPG entries to an
  identity (canonical name, callsign, network, country, category, region/zone, quality tier),
  computed once per run and cached in the store; every app matches on identities;
  `lineup.names` writes canonical names the other apps then see.
- **One ranker and one selection rule**: the matcher returns the best identity family (all
  candidates resolving to the same identity as the top one, ranked) for every app. One
  threshold/sensitivity in `[matching]`. Apps differ only in what the target holds: guide
  assigns the family's first entry with programme data (data validation, not selection),
  streams attaches the whole family, lineup writes the family's canonical name.
- **Labels fully automatic**: pairs all plugins agree on are auto-labelled; disputed pairs are
  labelled by plugin majority vote now, and by the `[ai.verify]` chain (Jev → LLM) whenever
  keys are in the environment (`CROSSTALK_AI__...`; never written to disk). The labelling
  script can be rerun later with keys, which re-decides the rules table. Labels + reasons are
  committed under `data/corpus/`.
- **Also unified**: one tag vocabulary (NC junk, CM ignored tags, IC format suffixes, triage
  tags, `[Unk]`); one quality model (SM sort key, IC format tiers, LU tiers) fed by probe data;
  one rename/template engine + collision resolver (triage templates, OTA format, unknown
  suffix, clean output); one logo resolver (EPG icon → Logo Manager → tv-logos → default).
- **Guide and streams change in phase 3**: each migration step publishes an oracle diff
  report (where output now differs from Janitor / Stream-Mapparr / IPTV Checker and why).
  The phase 1/2 branches stay as they are for reference.

## Decisions
- Branch: `plan/phase-3-lineup-sub-app` off phase 2 (`bf1af38`); pushed as its own branch
  when done; `main` untouched until the user's test-machine run.
- **Generic undo** from run rows: reverse a chosen past apply run's changes that are still in
  effect (restore's chain rule: current == recorded `after`). Covers Name Cleaner's
  `undo_last` and every lineup write. **Host feature for every app** (guide, streams, lineup):
  `crosstalk undo <run-id>` and an "Undo this run" button on a finished apply's run page,
  preview first, then apply (itself a recorded, undoable run).
- **Creates are a channel-ops stage with rows**: preview lists every channel / group / logo to
  be created; apply records one row per created object with its new id. Still no deletes.
- **Lineuparr's "delete channels left with no streams" becomes hide** via
  `hidden_from_output` (edit/bulk; no membership-row trap), recorded and undoable.
- **tv-logos index fetched over the network**, cached in the data dir with a TTL (24 h),
  unauthenticated, used offline between refreshes. Dispatcharr logos are created by URL.
- Lineup sync and Channel-Mapparr-style cleanup are equally important: full parity for both.
- *(Superseded by the goal reset: one merged data set, no per-plugin tables.)*
- **Lineuparr's matcher is not ported.** Its stream half becomes a lineup mode of
  `streams.match` over the unified engine; its EPG match is replaced by `guide.match`.
  `sync` and Lineuparr's logo tiers are ported (oracle-checked), the tiers through the unified
  logo resolver.
- **Ownership**: channels (and groups) crosstalk's `sync`/`create` made are recorded in the
  store. Hide-when-empty, renumbering and re-sync touch only owned channels; a pre-existing
  same-named channel can be adopted for streams/guide but is never hidden or renumbered.
- **Workflow steps can override scope**: a step is a string or a table
  `{action = "streams.match_apply", scope = "lineup", continue_on_failure = true}`;
  `scope = "lineup"` = the channels lineup owns for its configured lineup, or a table like
  `{groups = "DIRECTV: *"}`. `continue_on_failure` lets full_sync carry on past a failed
  match/guide/logo step (sync failing still stops); the workflow ends failed with each step
  listed.
- **`lineup.tidy_preview/apply`** hides owned lineup channels left with no streams
  (`hidden_from_output`), opt-in like Lineuparr's replace mode; full_sync runs it after
  `streams.match`.
- Lineup mode of `streams.match` **restricts to the lineup's country** by default.
- **Fixes over the plugins** (strictly safer, documented): owned channels keep their number
  on re-sync (LU renumbered every rerun); never re-append a suffix a name already ends with
  (CM `[Unk]`); a stream with an owned channel is not imported again (CM duplicated); only owned
  channels are adopted by `(name, group)`, a foreign clash is reported and skipped (LU adopted).
- **Improvements**: one cached tv-logos recursive-tree fetch (24 h) for every country (CM
  fetched per country, LU's contents API truncates at ~1000 files); Name Cleaner's collision
  strategy guards every rename/create (no two same-named channels in a group); sync/create
  never assign a number already used by any channel (reported; numbers are global, step 10
  review); every apply plans from live
  state (CM replays a saved JSON).
- **Creates**: one POST per channel over REST with bounded concurrency behind a
  `ChannelCreator` seam (bridge-ready), timed by `crosstalk bench`.
- **Undo**: any finished apply run; reverses only rows whose current value still equals what
  that run wrote; undo runs are undoable (redo). Created channels are hidden on undo; created
  groups/logos are left and listed.
- Name Cleaner's scheduler is dropped: `[schedules]` / `[hooks] m3u_refresh = ["lineup.clean_apply"]`.
- **AI for names**: optional AI gate for borderline premium renames (off by default), plus an
  **escalation chain** in `[ai.verify]` so a Jev (typesafe) model that cannot decide hands the
  question to an LLM (`openai_compat`). Host feature: guide and streams get the chain too.

## Approach

### Shared-layer additions (core)
- **channel_ops**: `Field::Number`, `Field::Logo` (+ `SetValue` arms, `edit/bulk` mapping);
  a **create stage**: `Op::Create { channels: Vec<NewChannel> }` (+ groups, logos) planned in
  preview, applied through a `ChannelCreator` seam (REST: concurrent POSTs, write_concurrency),
  one run row per created object with its new id (`subject` channel / group / logo, field
  `created`). Groups/logos created first, their ids fed to the channel creates.
- **Ownership** table in the store: `(kind, object id, app, key, run id)`, written by create
  rows; queries "owned channels of lineup X".
- **Name collision guard** in channel_ops: one shared resolver (NC's suffix / numeric / skip)
  over the plan's resulting names per group; used by every rename and create.
- **Number conflict check** for creates/renumbers, global (step 10 review).
- **Undo** (`core::undo`): reads a run's rows, keeps rows still in effect (current == after),
  builds `Op::Set` per field + hide for created channels, previewed and applied as a normal
  run (`app = <app>`, action `undo`, parent link to the undone run). CLI `crosstalk undo
  <run> [--yes]`, web button on finished apply runs.
- **Workflow step tables**: `steps` entries are a string or `{action, scope, continue_on_failure}`;
  `JobContext` carries a scope override; `scope = "lineup"` resolves through lineup's owned
  channels for its configured lineup.
- **AI chain**: `[ai.verify] chain = [{provider, model}, ...]` (old single pair = shorthand);
  `ai_gate::ask` escalates on undecided/failed; run rows/audit record which model decided.
- **Fetch cache** (`core::fetch_cache`): TTL file cache in the data dir for the tv-logos tree.

### Unified matching layer (replaces per-plugin profiles)
- **Rule inventory**: every point where Janitor, Stream-Mapparr, Channel-Mapparr, Lineuparr
  (and the shared core) disagree: normalizer tables (quality, prefixes, regional, misc, geo),
  `&`, dots, suffix strip, designator repair, bare-region strip, token-overlap guard (4 vs 5
  chars), callsign ladder + allowlist + denylist + OTA-number rescue, alias lookup (exact /
  folded / reverse index / fuzzy), stage structure and guards (majority, length-scaled,
  trailing number, designator), zones, country detection vocabulary.
- **Corpus** (`data/corpus/`): real names from the plugins' tests and golden baselines, the
  channel DBs and station table, the 29 lineup files, Janitor's/SM's fixture worlds; pairs
  generated by running every plugin's matcher (oracles) and keeping where they disagree;
  labels per the decision above. Labeller is a script (`scripts/corpus/`), reproducible.
- **Decision**: for each inventory row, score each variant (precision/recall on labelled
  pairs, plus false-positive weight) and pick one; `docs/matching-decisions.md` records the
  row, the variants, the scores and the choice. The engine has **one** code path; remaining
  flags are only operator-facing settings (`[matching]`).
- **Identity** (`crosstalk-matching::identity`): `resolve(name, context) -> Identity`
  (canonical name, callsign, network, country, category, region/zone, tags, quality tier);
  `rank(query, candidates) -> Vec<Family>` over identities; one `CandidateIndex`. Cached per
  run and persisted (`identity_cache` table keyed by normalized input + data version).
- **OTA naming** (affiliation parse, network override, `{NETWORK} - {STATE} {CITY}
  ({CALLSIGN})` template, market fallback) and tag re-attachment are identity/template
  features available to every app, not lineup-only code.
- Oracle harnesses: the existing per-plugin harnesses switch to *report* mode (diff counts by
  reason, never failing CI); new property tests and the labelled corpus are the gate.

### Lineup files and sync
Vendor the 29 lineup files under `data/lineups/` (756 KB). Sync parity against LU's real
`_sync_groups`/`_sync_channels` with ORM fakes, plus the documented fixes (ownership, stable
numbers, no foreign adoption, number conflicts). Profile membership set at create via
`channel_profile_ids`; existing owned channels never gain rows (phase-2 rule).

### Streams lineup mode
`streams.match` gains `mode = "lineup"` (setting + workflow scope): the target channels are
lineup's owned channels, matched to stream identity families like any other match, restricted
to the lineup's country.

## Files to modify
- New crate `crates/lineup` (`crosstalk-lineup`): `lib.rs`, `settings.rs`, `clean.rs`,
  `names.rs`, `categorize.rs`, `logos.rs`, `create.rs`, `sync.rs`, `tidy.rs`, `lineup_file.rs`,
  `test_support.rs`, examples for harnesses.
- `crates/crosstalk-matching`: one engine (`normalize.rs`, `callsign.rs`, `alias.rs`,
  `tokens.rs`, `pipeline.rs` collapsed to the decided rules), new `identity.rs`, `ota_name.rs`,
  `tags.rs`, `country.rs` (one detector), `profile.rs` removed from production; merged
  `data/` (per-country DBs, stations, aliases, `lineups/`, `corpus/`), `docs/matching-decisions.md`.
- `crates/guide`, `crates/streams`: migrated onto identity families, the shared quality model,
  tags and templates; oracle report modes for their harnesses.
- `crates/crosstalk-core`: `channel_ops.rs`, new `undo.rs`, `ownership` in `store.rs`,
  `workflow.rs` + `config.rs` (step tables, AI chain), `ai_gate.rs`, `ai.rs`, new
  `fetch_cache.rs`, `dispatcharr/` (create logo/channel/group already exist; logo list paging).
- `crates/streams`: lineup mode in `matching.rs`/settings.
- `crates/crosstalk`: registry, CLI `undo`, pairs, bench (create timing);
  `crates/crosstalk-web`: undo button, step tables display.
- `reference/` submodules for Channel-Mapparr, Lineuparr, Name Cleaner; `upstream_drift.sh`,
  `THIRD_PARTY_NOTICES.md`; `config/lineup.example.toml`, `crosstalk.example.toml`
  (full_sync workflow, AI chain); README; parity scripts.

## Reuse
- `channel_ops` plan/apply/run rows/`write_ops_report`/`cancelled_note`; `Op::Set` per channel;
  restore's chain rule (`crates/streams/src/restore.rs`) for undo.
- `core::scope`, `csv_report::Report`, `OutcomeBuilder`, runner/limiter/workflows/hooks.
- Matcher primitives, `CandidateIndex`, `MatchProfile` pattern from `StreamMapparr`.
- Streams `matching::build_groups` / collector / `MatchOptions::with_database`.
- Client `create_group` (get_or_create), `create_channel`, `create_logo`, `logos()`,
  `edit_channels`.
- Harness loaders: `scripts/sort_parity.py` / `match_parity.py` Django-stub pattern.

## Steps
Each step: Muse Spark 1.3 Contributor implements → gate → Opus 5.5 reviews and fixes → gate → commit.

**A. Unification (guide and streams migrate here)**
- [ ] 1. References: pin Channel-Mapparr (`45ba787`), Lineuparr (`9f6a5ff`), Name Cleaner
  (`d68b195`); drift lists; notices.
- [ ] 2. AI chain with escalation (`[ai.verify] chain`), used by the labeller and at runtime.
- [ ] 3. Rule inventory + oracle runners for all four matchers (Python, from `reference/`).
- [ ] 4. Merged data set (`data/`: one DB per country, one station table, one alias table,
  lineup files) with a provenance/conflict report.
- [ ] 5. Corpus build + automatic labelling (majority now; chain if keys present) +
  `docs/matching-decisions.md`.
- [ ] 6. Unified engine: one normalizer/ladder/alias/guards path per the decisions; identity
  resolve + rank families; identity cache; retire `MatchProfile` from production code;
  corpus gate.
- [ ] 7. Unified tag vocabulary, quality model, template engine + collision resolver, logo
  resolver (core / matching), with the fetch cache.
- [ ] 8. Migrate guide onto identity families (match, heal, scan, cleanup); oracle diff report
  vs Janitor.
- [x] 9. Migrate streams (match, sort, mirror, visibility, probe format tiers, triage tags);
  oracle diff report vs Stream-Mapparr / IPTV Checker.

**B. Host features**
- [ ] 10. Channel ops: `Field::Number`/`Field::Logo`, create stage + `ChannelCreator`,
  ownership table, number-conflict check (collision resolver from step 7).
- [ ] 11. Host undo (core + CLI + web) for every app.
- [ ] 12. Workflow step tables (scope override, continue_on_failure).

**C. Lineup app (built on A + B)**
- [ ] 13. Lineup crate skeleton: settings, lineup files + loader (country, category_detail),
  registry, validate/status.
- [ ] 14. `clean_*` + `test_name` (unified tags + collision resolver); oracle vs NC.
- [ ] 15. `names_*` (identity canonical names, OTA template, unknown tagging, AI gate);
  oracle vs CM.
- [ ] 16. `categorize_*`, `logos_*` (unified logo resolver); oracle vs CM and LU.
- [ ] 17. `create_*` (streams / stations modes); oracle vs CM.
- [ ] 18. `sync_*` + `tidy_*` + ownership; oracle vs LU; fixes documented.
- [ ] 19. `streams.match` lineup mode + `full_sync` example workflow.
- [ ] 20. Docs: README (one matcher, decisions doc, every app's actions, differences, undo, AI
  chain), example configs, bench numbers.
- [ ] Phase end: Fable 5.1 review and fixes over the whole phase (cross-app consistency: the
  same name must yield the same identity and family in guide, streams and lineup).

## Verification
- Every step: fmt, clippy `-D warnings`, `cargo test --locked --workspace`, all existing parity
  harnesses, new ones, `actionlint`, `shellcheck`.
- **Gate = the labelled corpus + cross-app consistency tests** (one name → one identity in
  every app; property tests on the engine). Plugin harnesses drive the plugins' own Python as
  oracles and publish diff reports by reason; CI shows them, they never fail it.
- Each migration step's oracle diff report is reviewed by Opus 5.5 (and Fable 5.1 at phase end): every
  difference must trace to a recorded decision row, a documented fix, or the unified selection
  rule.
- Undo: round-trip tests (apply → undo → state equals before; a later hand edit survives undo;
  undo of undo).
- Scale: bench at 20k channels for names/clean/categorize/logos plans; lineup of 500 channels
  sync; create timing via `bench --live` later.
- Live: none during execution (user's test machine after all phases).

## Execution
Changed from phases 1–2 (user, 2026-11-04):
- Implementer: **Muse Spark 1.3 Contributor** (`meta/muse-spark-1.3-contributor`), one
  subagent per step.
- Step reviewer: **Opus 5.5** (`claude-opus-5-5`) after each step; reviews against this plan,
  the decisions table and the oracle reports, and fixes what it finds in the same pass.
- Phase reviewer: **Fable 5.1** (`claude-fable-5-1`) at the end of the phase; whole-phase,
  cross-app consistency, and fixes what it finds (provider `claude-bridge`, Fable 5.1 1M).
- The orchestrator runs gates and owns commits; nothing is pushed without approval.

## Step 8/9 migration list (from the step 7 review, 2026-11-05)

Step 7 moved only pure extractions (byte-identical output, parity gates 0): probe
`stream_format` → `core::quality::stream_format_of`; channel-ops templates →
`core::naming::render_channel`; triage's standard five format tags and restore's
`STANDARD_QUALITY_TAGS` → `core::quality::IC_FORMAT_LABELS`; restore's status tags →
`tags::tokens_from(Status, "IC")`. Everything below CHANGES output and belongs to the step
named; each needs its oracle diff report line.

**Changed decision (step 7 review): the collision resolver is GLOBAL, not per group.**
Dispatcharr's outputs list channels flat (HDHomeRun `lineup.json` has no group; XMLTV
display-names are flat; players use `group-title` only as a filter), so two `ESPN`s in
different groups are indistinguishable in a guide. The resolver only changes the name a
*planned* rename lands on, never another channel, so global scope is safe. It matches Name
Cleaner (the oracle for `clean_*`), with two documented fixes: slots are counted (NC's set
arithmetic freed a name another channel still held) and a `skip` loser's old name stays
occupied (cascade to a fixpoint). Step 10's create stage and step 14/15 use it as is.

Step 8 (guide):
- [x] `channel_ops::RenameKind::Suffix` presence check is exact `ends_with`
  (`crates/crosstalk-core/src/channel_ops.rs`, `RenameKind::already`); the shared rule is
  `naming::apply_suffix` / `tags::has_trailing` (case-insensitive, bracket-aware: `CNN
  [badepg]` and `CNN (BadEPG)` count as tagged). Moving guide's `tag_missing` onto it changes
  Janitor parity for case/bracket variants.
- [x] `crates/guide/src/settings.rs` `bad_epg_suffix` default `" [BadEPG]"` is a literal; the
  token is the vocabulary's `status`/`BadEPG` row. Derive the default from it.
- [x] `crosstalk-matching::identity` builds identity tags with `country::extract_tags` (SM's,
  D09) — a second tag extractor beside `tags::split_tags`. They differ on bare
  `Central`/`Atlantic` (SM strips `Comedy Central` → `Comedy`; the vocabulary does not), on
  `Middle East`/`Far East`, on `+N` shifts and on bracket-only quality. Move identity onto
  `tags` (bump `ENGINE_VERSION` / identity cache version; the corpus gate must still pass,
  so D09's regional choice may need a decision row) — this is cross-app, do it first.
- [x] Not to migrate: the normalizer's comparison tables (`normalize.rs` quality pattern incl.
  `FD`/`Backup`, `country.rs` `NON_COUNTRY_PREFIXES`) — decided rows N01–N14, comparison
  only; `tags` is the writing vocabulary and must not feed the match path.

Step 8 review (2026-11-05):
- The default cut stays `strict` = 95 (D02/T10 `janitor-95`; presets low/medium/high/strict
  = 70/80/90/95).
- AI gate kept live at the defaults: every family member clears the cut, so a channel
  with an EMPTY family is re-ranked by the same engine and index at the gate's floor
  (`UnifiedEngine::with_threshold` + `shortlist_index_in`, the ranker without the family
  cut; `match_run::score_shortlists`; `RunEngine::shortlist_engine`, cut =
  min(rescue_floor, verify_band_low) when `[ai.verify]` is enabled). Verify band and
  rescue reach the chain; only a model accept can promote (the walk still accepts nothing
  below the cut). Streams adopts the same in step 9.
- Identities persist in one batch per run (`CachedIdentities::warm`: one read pass, one
  write transaction, rayon, off the runtime) instead of one autocommit per name.
- Guide's CSV preamble names the `[matching]` cut (`Report::setting`).
- Retired `match_confidence_threshold` / `heal_confidence_threshold` fail at load naming
  `[matching] sensitivity` (same clean-break rule as the phase-1 moved settings).

Step 9 (streams):
- [x] `triage::ends_with_tag` (`crates/streams/src/triage.rs`) → `tags::has_trailing`
  (accepts `(DEAD)` and literal affixes; today only `[DEAD]`).
- [x] `triage::FormatTags` strip regex and `restore::trailing_tag_pattern`
  (`crates/streams/src/restore.rs`) → `tags::strip` with kinds; needs an extension for
  operator-configured tokens (`video_format_suffixes`, custom rename-format labels are not in
  the vocabulary). Output changes: parens and stacked mixed runs strip.
- [x] Restore's standard status set: IC's three today; the vocabulary's `status` kind also has
  CM `Unk` and guide `BadEPG`. Decide whether restore strips those (likely NO for `BadEPG`:
  that is guide's mark, not triage's).
- [x] `triage::render_format` (`{name}` only, format trimmed) → `naming::render_channel`
  (`{group}`/`{number}`/`{epg}`/`{id}` substitute; no trim). Output changes for formats
  carrying those tokens.
- [x] `triage::format_tags` (bracket labels of a format) — keep, but move beside
  `naming` so lineup's suffix rules read the same parser.
- [x] Examples `crates/streams/examples/probe_io.rs` and `triage_io.rs` hard-code the five-label
  map; use `QualityTier::parse_label` / `IC_FORMAT_LABELS`.
- [x] Stream-Mapparr's sort key (`crates/streams/src/quality.rs`) stays THE stream ordering;
  lineup mode reads tiers via `QualityTier::from_dimensions` (height-first), which is NOT
  IC's width rule — triage's suffix keeps `stream_format_of` (width) unless a decision row
  says otherwise.
- Logos (step 16 oracle): the resolver's similarity is the unified engine's, not
  rapidfuzz `ratio`; channel-name punctuation becomes a space (CM deletes it); `NO` →
  `nordic/norway` and `GB` → `-uk` suffix are fixes over CM; URLs are percent-encoded.

Step 9 review (2026-11-05):
- One matcher in the binary: `simulate-streams` runs the unified engine (a stream passes
  as a family member at the cut); the Janitor port is `--features oracle` +
  `--oracle janitor`, still byte-identical to `match_sim.py` (`sim_parity.sh` gate, its
  `[unified]` line is the report). Streams' SM collector is `cfg(test)` only (the dead
  `oracle` feature removed); `match_run::score_names` and `MatchingConfig::matcher`
  removed. `scripts/one_matcher_check.sh` (CI) proves it from the symbol table: 0
  `profile::`/`CandidateIndex`/`pipeline::Matcher` symbols in the production binary.
- Grouping key = family key + the country the leader's family is ranked under (the
  filter's `_channel_country_code`, DB + group name, when on; the name marker when off):
  restores bug-158 for group-name countries and stops `UK: CNN`'s family being written to
  `US: CNN` with the filter off. One helper (`query_country`) for key and collector.
- Engine tie-break (cross-app): identity-equal candidates win a tie before T12's overlap
  (a reordered name scoring 100 by token sort made the other channel the top).
  Mirrored in `engine_parity.py`.
- Restore/triage strip SETS stay the plugin's (IC status + format labels, rename labels,
  suffixes) over the shared parser: `[BadEPG]`, `[Unk]`, `[1080p]` etc. never strip.
  `render_format` substitutes real channel values, keeps the format trim, never writes a
  blank name. `triage_parity.py` covers parens (`paren` divergence) and other apps' tags.
- AI gate: accepts go through the cut's country exclusion; audit row = leader's report
  row; task prefix only with a configured chain (guide's rule); no double country count.
- OPEN (step 6 rule, needs a decision): Z01 `sm-zone` zone extraction ignores a zone word
  followed by a trailing tag (`HBO West (Backup)`, `HBO West [4K]`, `ITV1 (Central West)
  HD` → zone Default), so West feeds join the default family in guide and streams. The
  streams oracle report lists it as unattributed.
- RESOLVED (follow-up fix): new orchestrator row Z08 — the zone is read AFTER stripping
  trailing writer tags with the shared tag vocabulary (`tags::strip_zone_ignorable`:
  quality/junk/status + `+1`, which stays a timeshift; regional words never strip), in
  both the Rust engine (`extract_zone_noguard`) and the decided Python configuration
  (`sm_zone_noguard` over the `data/merged/tags.json` vocabulary, the one shared
  definition). `ENGINE_VERSION` 2 → 3 (identity cache invalidation); `decide.py`
  re-run (engine cost 856 → 853, FP unchanged); streams report section 4 now empty
  (10 wrong extras gone, `ITV1 (Central East/West)` families separate); guide report
  +2 agreements. `West Ham TV` / `Westworld` stay Default. Coherence checks hold.
- Follow-up review: the strip was reverted out of the guarded `extract_zone` (SM's
  transcription, the `parity_sweep_sm.py` zone oracle; it disagreed with SM on
  `Antenna TV Pacific [HD]`, and the CI sample only missed it by chance). The Python mirror
  now trims/splits on Rust's whitespace and never peels to whitespace-only (` HD`), and
  matches Rust byte for byte over 79,807 corpus/merged/synthetic names. The Z08 coherence
  check also requires Z02 `none`. `UnifiedEngine::cached` (used in production via
  `MatchOptions::new`) is bounded to 8 most-recently-used configs.

Step 10 review (2026-11-06):
- **Changed decision: channel numbers are GLOBAL, not per group.** Dispatcharr's only per-group
  check is `Channel.clean`, which REST/bulk writes never call. Everything else is global:
  `get_next_available_channel_number` → `compact_numbering.build_reserved_set` (every channel
  + override pins), `channel_number_is_reserved`, the auto-sync `used_numbers` seed, and the
  outputs (HDHomeRun `GuideNumber`, M3U `tvg-chno`, default `tvg_id` = number). The conflict
  check (edits and creates) and `next_free_numbers` (no group argument) are global. A refused
  renumber keeps its old number (the pass repeats until stable), so a swap is legal but a
  claim on a number whose holder could not move is refused.
- **Crash safety**: `apply_creates` records an `ownership_intent` row per POST (what was posted
  + the plan world's highest id of that kind) before each stage, and settles it into
  `ownership` in one transaction when the answer lands. `channel_ops::load_owned` (run before
  planning) adopts an unsettled intent's object — exact name (+ group + number for channels,
  URL for logos), id above the floor, unowned, newest — or drops the intent; intents of a
  still-running run are left alone. It also forgets owned ids missing from a loaded slice
  (deleted upstream; ids are never reused), so their keys are created again; hidden channels
  stay owned (undo). `plan_creates` ignores an owned id not in the world and requires the
  slices it dedups against.
- Group creates: `POST /channels/groups/` answers 400 for an existing name (DRF
  `UniqueValidator` on `TextField(unique=True)`, exact, case-sensitive) — not get_or_create.
  The client recovers a failed group create by exact-name lookup, a failed logo create by
  `?name=` + exact URL. Channel POSTs stay single-attempt.
- `run_concurrent` publishes progress per answer; an unanswered post that was not cancelled
  says so instead of "cancelled".

Step 13 review (2026-11-06):
- **One place per setting.** Lineup settings hold only what lineup does. Retired at load,
  each naming its new home (`settings::MOVED`): `epg_sources` → guide's
  `epg_sources_to_match`; `refresh_epg_after_match` → a `guide.watchdog_apply` workflow
  step; `preserve_existing_streams` → streams' `overwrite_streams` (inverted; the
  replace-mode delete is the `lineup.tidy_apply` step); CM `m3u_sources` → streams'
  `selected_m3us`; `single_channel_name` → a workflow step scope; NC `test_name` → the
  `test_name` action's own input (step 14 needs an action input, not a saved setting);
  `rate_limiting` → `[dispatcharr] concurrency` (lineup has no `write_concurrency`).
  Never-shipped `tidy_after_sync` and `write_concurrency` removed outright (tidy is a
  workflow step after `streams.match_apply`, per the decision). LEFT for a later call:
  streams' own `write_concurrency` beside the host cap.
- Named scope `lineup` = channel ids owned under `lineup:<full filename>:` for the
  CONFIGURED `lineup_file` (`lineup::owner_key_prefix`; either spelling of the setting).
  Switching files leaves the old lineup's channels owned but out of scope. Empty before
  any sync → the step touches nothing (the bound replaces the host scope). A settings
  file that fails to load or names an unknown lineup fails the step.
  `SubApp::resolve_named_scope` now receives the run's `CrosstalkConfig`.
- Step 18: the key tail after `lineup:<file>:` must not include the folded category, or
  changing `category_detail` re-keys (and re-creates) every channel.
- Loader: category order comes from an order-keeping serde visitor (`OrderedObject`,
  first position / last value like `json.load`), not a text scanner; serde_json's
  `preserve_order` would reorder every map the workspace serializes. Filename country is
  LU's `^([A-Z]{2})_(.+)_lineup\.json$` exactly; a missing `package` gives no auto prefix.
- Country: `lineuparr_category_country` is a new G02 entry point on the decided `lu`
  vocabulary; no guide/streams caller changes.

Step 14 review (2026-11-06):
- **Pipeline is NC's, token lists are the vocabulary's.** `clean.rs` follows
  `plugin.py:148-191` step for step (unicode marks, NFKC, junk-only brackets, pipe
  segments with removed-quality memory, +1, custom regex, smart casing, whitespace)
  with NC's `step()` fire-only-on-change tracking. No list is re-declared: pipe and
  bracket checks read `tags::is_pipe_junk_segment` / `tags::is_bracket_junk_inner`
  (NC's exact sets, `sources` containing `"NC"` so IC's `Unknown` and LU's bare
  `1080` never strip), the unicode marks are the non-ASCII members of
  `tokens_from(Quality, "NC")`, and the bracket-inner `/,&+` shape — the one
  structural pattern `tags.json` cannot enumerate — was added to the vocabulary as
  `is_bracket_junk_inner` (one definition of junk). `+1` is `([A-Za-z0-9])\+1\b`
  (Rust `regex` has no look-behind; byte-identical), custom/protect patterns compile
  through the shared `compile_pattern` rejection (invalid lines warn-and-skip like
  NC's `LOGGER.warning`), and `protect_regex` is empty-means-off like NC's.
  `smart_title` is `_smart_title` verbatim (including `1+1` → `1 +1`: NC's documented
  risk, setting off by default).
- **Scope is the unified scope.** Clean plans over `JobContext::base_selection`
  (host `[scope]` or the step override, `scope = "lineup`" included), resolved with
  the host wildcard/case rules — not NC's casefolded-exact group match. Documented
  in `clean.rs` and the only scope divergence; `protected_groups` stays NC-exact
  (casefolded, no wildcards) because it is a protect list, not a scope.
- **Collisions ride the one shared resolver.** `PlanOptions.collision =
  settings.collision_strategy` with `collision_sep = join_separator` and the pipe
  step's removed list per channel through the new
  `PlanOptions.collision_removed` map (`PlannedName.removed_tags` needed no naming
  change — it was already per-channel). NC's `suffix`-first-removed-tag rule is
  preserved, with the step 7 fixes (counted slots, `skip` cascade to a fixpoint).
  Global, like NC. No-op dropping is the plan's own; the CSV is NC's columns
  (`id,group,status,old_name,new_name,rules`) with `|`-joined fired rules; apply is
  one bulk edit; NC's plugin-dir backup CSVs are not ported — undo is the host's
  from the run rows (round trip tested).
- **Action input is a host mechanism, one string.** `ActionSpec::with_input`
  (Read-only, hard-panics otherwise), `JobContext::input` +
  `RunOptions::input`, `--input` on `crosstalk run` (refused for actions without
  one), a text field on the web action button, and a nullable `runs.action_input`
  column (migration in `add_run_columns`, `set_run_input` right after `insert_run`)
  so the run page shows what was tried. `test_name` is Read + offline, reports the
  cleaned name, fired rules, protection hit, and the unified identity
  (canonical/callsign/zone/country) for the input — no Dispatcharr calls.
- **Oracle gates at 0.** `scripts/clean_parity.py` drives NC's real
  `_compute_changes` (Django stubbed, channel table faked) over lineup names,
  merged DB names, corpus provider variants, NC test-like cases and collision
  scenarios x 8 settings/scope variants (2398 rows), against `clean_dump` (the same
  `compute_rows` + `plan_clean` the action runs): 0 mismatches. The corpus keeps
  existing names unique case-insensitively so the run measures the pipeline, not
  the slot-count fix; scope groups are spelled in exact case on both sides
  (wildcard/casefold matching differs by design). CI runs it.
- **Identity invariant (review).** A sweep of 24.9k names (corpus pairs, every merged
  lineup name bare and `| HD |`-decorated, probes) x {defaults, all rules on} found that
  clean never moves timeshift or callsign. Zone and country move only where the engine
  misreads the uncleaned name: pipe residue (`Cinemax East | HD |` keys as `cinemax | |`,
  default zone; East once cleaned) and names that are only a country code (`USA | HD |`,
  `FR [HD]`). Cleaning moves those toward the right reading. `UK | Sky Sports | HD` ->
  `UK Sky Sports` keeps country UK. Pinned by `clean_keeps_identity_on_the_corpus`
  (every 6th lineup name x 8 decorations x {defaults, `+1` on}: identity of
  clean(decorated) == identity of the undecorated form) and
  `clean_never_moves_timeshift_callsign_zone_or_country_of_a_clean_name` (the
  exceptions listed by name). `test_name` now shows the identity after clean too, and
  names any field the rename moves (`identity_drift`).
- **Pipe residue: resolved (N15 `pipe-segments`, N14 stays `core`, `ENGINE_VERSION`
  4).** Before keying and before the zone/country reads the engine splits on `|`, drops
  empty and pure-junk segments (`tags::is_pipe_junk_segment`, the one definition) and
  joins the rest with a space, except beside a core country code, which keeps a
  delimiter the core's tables already strip: `RK: US | CNN` -> `RK: US: CNN` (keys
  `cnn`), `UK | Sky Sports | HD` -> `UK: Sky Sports` (country UK), `BBC One | UK` keeps
  its bar (keys `bbc 1`). `Cinemax East | HD |` keys `cinemax`, zone East;
  `Sky Sports | Premier League` keeps both parts; `USA | HD |` (USA Network) has no
  country. A plain-space join stranded the country word (`RK: US CNN`, `BBC One UK`)
  and flipped N14 to Janitor's bare `USA?\s` anywhere strip. That strip keys
  `National Geographic USA FHD` and `HD National Geographic USA` differently (the
  streams oracle lost that channel's family). It also strips the word `us` (`This Is
  Us TV`), and an end-of-name variant would merge `5 USA` with `5` (UK DB). With the
  delimiter join, N14's ablation is core 4 vs `janitor-anywhere` 6 (9 pairs, core).
  Engine cost 853 -> 844 at cut 80 and 844 -> 835 at cut 95. Both oracle reports are
  byte-identical to before N15. The guide +7 match_sim agreement under
  `janitor-anywhere` was agreement with Janitor's own bare-USA rule. Rust/Python
  collapse parity is the `pipe` kind of `engine_parity.py` (every corpus name x 8 pipe
  shapes). `decoration_keeps_the_family_key` pins X / `X FHD` / `X [4K]` / `HD X` /
  `X | HD |` on one family key.
- **Custom replacements are Python templates.** NC's `pat.sub(rep, s)` reads `\1` /
  `\g<name>` as groups and `$` as text; a verbatim copy into Rust `regex` inverted both.
  `python_replacement` translates the template and refuses what Python refuses (bad
  escape, missing group, trailing `\`; octal unsupported). The oracle gained a
  `custom-backrefs` case (9 cases, 2742 rows, 0 mismatches).
- **Action input is checked by the runner.** `subapp::check_action_input` (called by
  `start_with` and `run_to_completion_with`, so CLI, web and API all hit it) refuses an
  input for an action that declares none, more than `MAX_ACTION_INPUT_CHARS` (256), or
  one with control characters (no forged lines in the summary, run page or logs). The
  web field carries `maxlength`, the run page shows the stored input (askama-escaped),
  and a failed `set_run_input` is logged rather than dropped. `test_name` stays guarded
  (step 13 pins `status` as the one unguarded action).
- **For the step 20 README (clean differences from NC):** scope is the unified host
  scope (wildcards, case-sensitive, unknown group refused) where NC casefolded exact
  names (`clean_scope_is_the_unified_scope_not_nc_casefold`); collisions count slots,
  so renaming a case twin away does not free a name a kept channel holds
  (`a_kept_name_stays_taken_when_a_case_twin_is_renamed_away`), and a `skip` loser
  keeps its old name occupied (`naming::a_skipped_rename_keeps_its_old_name_occupied`);
  `protected_groups` uses `to_lowercase`, not `casefold`; custom/protect patterns are
  Rust `regex` (no backreferences or lookaround in the pattern); no backup CSVs (host
  undo); `fix_casing` (off by default) changes the matching key of camel-case names
  (`AccuWeather` -> `Accuweather`: `accu weather` -> `accuweather`), so it can cost
  guide/streams matches, which `test_name` shows; `normalize_plus_one` rewrites
  `1+1 International` to `1 +1 International` (NC's own risk).

Step 15 review (2026-11-07):
- **Target names are the unified engine's, per path with CM citations.**
  `crates/lineup/src/names.rs` resolves every in-scope channel through the same
  `RunEngine` + identity cache guide/streams run on, then: OTA when the identity
  callsign is in the merged station table (target = `render_ota(ota_fields(...))`
  with the `_extract_stream_network` override; missing placeholder refuses like
  `_format_ota_name` returning `None`; opt-in `ota_market_fallback` via the new
  shared `crosstalk-matching::market` port of `market_index.py` against the same
  merged stations); premium = the best identity family's DB leader (merged DBs
  for the enabled `[matching]` countries, at the one cut) + CM's tag
  re-attachment rebuilt from the shared tag vocabulary (ignored = quality/status
  kinds = CM's default ignored list); unmatched = `unknown_suffix` through
  `apply_suffix` when `tag_unmatched_channels`, else left alone. A callsign with
  no station row blocks premium and still tags (CM's `rename_unknown_channels`
  tags every Skipped row). The AI gate (lineup `ai_gate_enabled` AND host
  `[ai.verify]`, task `"names"`, off by default) re-ranks empty-family channels
  at the gate floor and only a model accept renames, through the gate's country
  rule. `names_preview`/`names_apply` plan from live state (`Op::Set Name` per
  channel, collision resolver on, no-op drops), CSV = CM's rename columns + the
  identity fields + reason, apply = one bulk edit, undo = the host's from run rows.
- **Identity invariant (review).** Premium renames always re-resolve into the same
  family (pinned by `renames_keep_the_identity_family`). The OTA template is the
  documented exception: it changes the normalize key (`13 WREX` -> `NBC - IL
  Rockford (WREX)`), so `same_family` does not hold — but the callsign is kept,
  and callsign identity ignores country (bug-161), so the broadcast entity is the
  same station under a different display shape (asserted, listed by name).
- **No-double re-attachment (fix over CM).** `build_final_channel_name` never
  re-attaches a tag the DB base already carries (`KSPT-DT3 (HEROICN)` stays
  itself; `truTV (Pacific)` gains no second Pacific) — pinned by
  `reattachment_never_doubles_what_the_base_holds`. Same never-double rule as
  `apply_suffix` for `[Unk]`.
- **Deterministic station lookup.** The base-callsign -> full-key map is built
  over sorted keys: two full spellings sharing a base (`KAZT-CD` vs `KAZT-TV`)
  always resolve the same way. A `HashMap`-iteration-ordered first-wins map made
  the oracle report flip between runs (caught by `--check`).
- **Oracle gates at 0 unattributed.** `scripts/oracle_report_lineup_names.py`
  drives CM's real `load_and_process_channels` path (oracles.py CM path, DB-order
  ties like `oracles.py`, plus verbatim `_extract_stream_network` /
  `_parse_network_affiliation` / `_format_ota_name`) + `rename_unknown` folding
  (every CM Skipped row, OTA ones included), over a representative world (station
  decorations, DB names with provider decorations, 40 popular national networks
  in provider decorations, unmatched) x {defaults, market-fallback} plus a
  disputed set, vs `names_dump` (the action's own `decide_heuristic` + the
  ranker's no-cut `floor`): 304/555 agree, 0 unattributed. Every bucket is a
  CHECK, never elimination: `ota-format`/`ota-vs-premium` (ladders: one side OTA,
  or one side reads a callsign), `premium-canonical` (both winners in both DBs
  and different; a decided alias row; CM's own `fuzzy_match` rejecting Rust's
  winner; or the ranker never scoring CM's winner), `cm-prefilter` (CM's scorer
  accepts Rust's winner, its `get_candidates` pre-filter drops it: `A&E`),
  `merged-data` (winner absent from the other DB, or differing station rows),
  `cut` (CM's winner in the `floor` shortlist below the host cut — 0 rows: the
  10 rows the first draft called `cut` were CM doubling a tag the base holds,
  `K28NN-D (CBS) (CBS)`, now `tag-reattachment`), `unknown-tag` (two documented
  fixes: only truly unmatched rows tag, never twice). CI runs the report
  non-failing with `--check` gating byte-reproducibility (added to `ci.yml`).
- **Review fixes (step 15 review).** (1) BLOCKER: with `ota_market_fallback` on,
  a printed callsign with no station (`ABC (WZZZ)`) fell through to premium and
  renamed to the national `ABC`; it now tries the market and otherwise blocks
  premium like CM (`market_fallback_never_turns_a_blocked_callsign_premium`).
  (2) The AI gate keyed borderline on the row's wording, so with the default
  `[Unk]` tagging it never fired; `decide_heuristic` now returns the borderline
  flag itself, unanswered outcomes are ignored, and an accept onto the current
  name skips (`ai_gate_runs_on_tagged_unmatched_rows_and_only_there`). (3) A base
  stating a feed zone gains no second regional word (`Cinemax Action (Pacific)
  West` -> `Cinemax Action (Pacific)`). (4) names and clean fought on DB bases
  carrying ignored tags (`BX Culture` -> `BX Culture (HD)` -> clean -> ...): the
  base drops its ignored (quality/status) groups; `names_and_clean_do_not_fight`
  pins idempotence and a joint fixpoint in both orders (the order still picks
  between same-family endpoints: `ESPN [HD]` names to `ESPN HD`, cleaned first
  to `ESPN`). (5) `parse_network_affiliation` splits on `:` too (fix over CM:
  `NBC: ION` rendered `NBC: - CA Eureka (KIEM)`, which re-renamed next run).
- **Cross-app finding (engine, LEFT for the phase review).** Network-led callsign
  names (`ABC (WABC)`, `NBC (KSDK)`) rank on the stripped key first, so guide and
  streams give them the bare national entry (`ABC`, Exact 100) before the rename
  and the station (Callsign 95) after it. Callsign-led names keep the station.
  `ota_renames_keep_the_guide_and_streams_families` pins both, so an engine change
  to callsign precedence flips it on purpose.
- **For the step 20 README (names differences from CM):** one engine/identity/cut
  everywhere (CM's 80 vs strict 95 is the `cut` bucket); merged newest-union data
  vs CM's older per-country DB (`merged-data`); no saved-JSON replay (live plans);
  the collision guard on every rename (CM has none); only unmatched rows tag and
  never twice; a callsign CM's ladder reads but the merged allowlist does not
  (digit-led shapes) blocks premium on CM's side only (`ota-vs-premium`); the DB
  base drops ignored quality tags (`BX Culture (HD)` -> `BX Culture`, so names
  and clean agree); one regional word at most; `NBC: ION` reads as `NBC`.
- **RESOLVED (follow-up engine fix): new orchestrator row C07 `callsign-first`.**
  When the query's identity has a high-confidence callsign (the engine's existing
  confidence) that exists in the merged station table, a candidate with the same
  callsign outranks any non-callsign match, and a candidate with a DIFFERENT known
  callsign is excluded (the C06/D04 anchor's high/high hard-reject, extended to any
  known candidate callsign — `score_core` step 2a); a candidate with no callsign
  (the bare national `ABC`) only wins when no same-callsign candidate survives the
  cut in the pool (`ranked_survivors` orders same-callsign first, then score — the
  fallback keeps today's behaviour for pools lacking the station). Identity
  families stay consistent: callsign identities already ignore country (bug-161),
  unchanged. No plugin variant ranks callsign-first, so there is no variant to
  ablate; mirrored exactly in the decided Python configuration
  (`ablate.Engine.score` + `engine_parity.python_family` via `callsign_first_base` /
  `candidate_known_base` over the merged table). `ENGINE_VERSION` 4 → 5 (identity
  cache invalidation; `-eng:N` assertions fixed); `decide.py` re-run — engine cost
  844 → 844 at cut 80 (FP 34, FN 776 unchanged) and 835 → 835 at cut 95 (FP 8,
  FN 819 unchanged; the old anchor already rejected the corpus pairs this extends),
  coherence checks hold. All three oracle reports regenerate byte-identically (0
  deltas: no fixture sets a network-led station query against a pool holding both
  the station and the bare national entry). `ota_renames_keep_the_guide_and_streams_families`
  now pins the station family both before and after the rename, plus the national
  fallback and the different-callsign exclusion; `13 WREX` unchanged; names without
  a callsign never trigger the rule (engine property test).
- **C07 review (2026-11-07).** False-exclusion audit over every name in the merged
  channel DBs, lineups, alias tables and corpus (60,745): 2,940 carry a
  high-confidence known callsign; no pair any of them matched before C07 (cut 95
  through the Rust engine, HEAD vs working tree, every word-sharing candidate with
  a known callsign; cut 80 through the Python mirror with the rule off, 517,447
  pairs sharing half their words) carries a different known callsign, so ignoring
  candidate-side confidence excludes nothing today and the rule stays as decided.
  Candidate-side low-confidence hits on known stations are real stations
  (`KOMO News`, `WGN America`, `NFL on KRQE`, `★ KCOP (MNT) ★`); the only words are
  `Tyne & Wear TV` (WEAR) and `Hong Kong Fight Club` (KONG), which no station query
  comes near. Fallback kept on evidence: Janitor's real `_find_best_epg_match`
  assigns the bare national entry (`ABC`, Fuzzy 100) to `ABC (WABC)` with or
  without the station listed, so the fallback IS Janitor's answer; a network feed's
  guide is right for the network day and wrong only for local blocks, which beats
  no guide, and nothing asks for an AI/rescue gate instead (revisit on operator
  evidence). Oracle reports: the guide and streams representative worlds now carry
  three documented C07 probes (`ABC (WABC)`, `US: NBC (KSDK) HD` with station and
  national both present; `CBS (WFOR) [HD]` with no station, answer the national
  `CBS`) and a `C07-callsign-first` bucket. Decide row: the non-chosen variant is
  named `score-first` (what every plugin does), not `lu-anchor`; the coherence
  check also requires the D04 anchor C07 extends.

### Step 16 implementation notes (Muse Spark, 2026-11-07, unreviewed).
- `categorize.rs`: category = the channel's best identity family leader looked up
  in a merged-DB category map (last country wins, entries without a category never
  enter it — CM's `category_map_premium` verbatim). No callsign branch, like CM
  (`plugin.py:3165-3172`, dead-map comment): OTA station names take the premium path
  or stay put, so no `Broadcast` group is ever created here (that string only exists
  in CM's stream-import path). Moves are `Op::Set Group` per channel through the
  shared planner; missing groups go through the create stage (`NewGroup`, exact-name
  dedup, owned `lineup:category:<name>`, intents + `load_owned` reconcile). Scope is
  the host scope restricted by `category_groups` (exact, case-sensitive, fail-closed
  like CM's `_resolve_group_scope` with `include_wildcards = False`); targets under
  `[scope].ignore_groups` are refused with the reason (CM's `is_ignored_name_tokens`).
  CSV = CM's 8 preview columns + status/reason + identity fields. Collision/n/a.
- `logos.rs`: tiers consulted in `logo_sources` order (default
  `epg, logo_manager, tv-logos`; `default` names the tier explicitly, otherwise the
  configured `default_logo` is the final safety net). EPG icon of the assigned entry
  (new `EpgEntry.icon_url`, `None` on older builds), Logo Manager exact
  case-insensitive name, tv-logos over `tv_logos_countries` else the identity country
  else the enabled `[matching]` countries (CM station-first then fuzzy across every
  searched country; LU's single country is the one-code case). The fuzzy match runs
  on the identity canonical name (`resolve` per channel through the same `RunEngine`
  guide/streams run on). Threshold stays 85 in both modes (CM's replace followed
  match sensitivity; documented as a fix: the logo similarity is not a sensitivity
  decision). New URLs become `NewLogo` rows (exact-URL dedup, owned
  `lineup:logo:<url>`), then `Op::Set Logo`; CM's replace-mode JSON backup is the
  run rows' before values instead (host undo). `load_tv_index` fetches the one cached
  recursive tree only when the tv tier is consulted and a candidate needs it; tests
  and the oracle dump inject the index (no network).
- Oracles: `scripts/oracle_report_lineup_categorize.py` (CM's real
  normalized-exact + `get_candidates`/`fuzzy_match` path over CM's own DBs vs
  `categorize_dump`, incl. the no-cut `floor` for `cut`) and
  `scripts/oracle_report_lineup_logos.py` (CM + LU real `logo_matcher` functions
  over the SAME recorded tree `scripts/data/tv_logos_tree.json` vs `logos_dump`
  with per-file unified scores). Both follow the names oracle's shape (representative
  + disputed worlds, `--check` byte-reproducibility, every bucket a CHECK). CI runs
  both `--check` gates after the names one.
- For the step 20 README (categorize/logos differences from CM/LU): one engine/cut
  everywhere (CM's 80 vs strict 95 is the `cut` bucket); merged newest-union data vs
  CM's older tables (`merged-data`); no saved-JSON replay, no whole-run errors for a
  missing default (per-channel skips); never double-moves (already-correct is a skip);
  EPG-first order vs CM's station-first (`order`); no network-logo tier
  (`network-logo`); GB `-uk` and `nordic/norway` are fixes over CM; URLs are
  percent-encoded; unknown `logo_sources` tokens and zero-match `category_groups`
  fail the run; created groups/logos are owned, left in place by undo and listed.
- NOT RUN HERE: the sandbox blocked shell execution, so `cargo fmt/clippy/test`,
  both oracle generations (`python3 scripts/oracle_report_lineup_{categorize,logos}.py`
  to write `docs/oracle-reports/lineup-{categorize,logos}.md` — `--check` fails until
  the reports are generated once), `clean_parity`, `lineup_file_parity`,
  `one_matcher_check.sh` and `actionlint` still need the orchestrator's gate run.

Step 16 review (2026-11-07, Opus 5.5; cargo blocked in the reviewer's sandbox, so the
orchestrator runs the gate and generates both reports):
- **BLOCKER fixed: created rows were overwritten.** categorize/logos recorded the create
  rows and the edit rows both from ordinal 0; `run_rows` is keyed `(run_id, ordinal)` with
  `INSERT OR REPLACE`, so every `created` group/logo row was replaced by a move/assign row
  (undo listed nothing, ownership audit lost). One helper,
  `channel_ops::create_and_edit_run_rows`, now shifts the edit rows past the create rows;
  categorize, logos and streams' triage (which did it by hand) all use it, pinned in
  `creates_plan_dedup_resolve_and_apply_with_ownership`.
- **OTA is never categorized** (`ota` bucket). CM's name-only path is `No Match` for an
  OTA-template name (`CBS - AL Birmingham (WIAT)`), but the unified ranker reaches the
  station entry through C07 (`WIAT (CBS)`), whose DB category is the COUNTRY (30,654 of the
  US DB's entries say `United States`). A channel whose identity callsign is a merged
  station — the same OTA test `names` uses — stays put. Strictly safer than CM where CM
  classifies a network-led name by its bare network (`ABC (WABC)` → `Broadcast`). Regional
  NON-station names still move to `United States` exactly like CM (parity; revisit with
  the user if a country-named group is unwanted).
- `Premium (Exact)` is CM's exact (the channel's normalize key IS the leader's, at 100),
  whichever engine stage scored it (CNN is an alias-stage 100 in the engine).
- **Logos: one resolver.** The tier walk, order parsing (`LogoSource::parse_order`,
  `DEFAULT_ORDER`), multi-country station-first search (`search_tv_logos`) and the default
  safety net moved into `core::logos::resolve_logo` (which had no production caller and a
  fixed order); lineup's `LogoTier`/`parse_logo_sources`/`search_tv` are gone and
  `decide_logo` maps the resolver's answer to its row.
- **Oracle honesty.** `scripts/data/tv_logos_tree.json` was a 22-entry hand-made fixture;
  it is now the real tree (tv-logos `main` sha `d32e347b`, recorded 2026-11-07, the 5,892
  blobs under the `COUNTRY_DIR_MAP` directories), and the logos world gained a broad
  seeded half (popular networks plain/decorated, merged US content names, OTA station
  names in the `names` template, UK names). Logos attribution fixes: `replace-scope`
  requires Rust's own skip reason; new `lu-order` (Rust = LU on a tier LU consults before
  tv-logos, CM differs); `order` verifies both tiers fire on Rust's evidence and the
  configured order; the dump's `searched` lists station directories (a station miss read
  as `country-dirs` otherwise). Categorize: `cut`/`premium-canonical` read the CM
  winner's no-cut score over the WHOLE shortlist (`probe_score`; the top-5 `floor` could
  misread a 6th-place winner as never scored); same-winner/different-category is checked
  as `merged-data`; `no-category` checks either side; the world is stratified (content
  categories vs the `United States` regional slice).
- **Decision A (orchestrator): country-named categories are no category.** A DB category
  equal to any merged DB's `country_name`/`country_code` (`United States`, `United
  Kingdom`, `Brazil`, `US`, ...) never enters the category map
  (`categorize::is_country_category`), so categorize never creates or fills a
  country-named group (fix over CM, writes less). The family's category is its first
  member (rank order) the map holds, so a regional duplicate (`CNN HD` -> `United
  States`, `HUNGRY` vs `Hungry`) never hides the real entry, whatever the DB order.
  Oracle bucket `country-category` (checked on CM's label or the Rust family's merged
  entries). Names/identity untouched.
- **Decision B (orchestrator): CM's network-logo fallback is a `core::logos` tier**
  (`LogoSource::Network`, token `network`, `network_logo` / `search_network_logo`): a
  station-format name with no station file takes `<network>-<suffix>.png` (else the white
  variant), only for a channel with NO logo, like CM. Default order follows CM, which
  consults it last: `epg, logo_manager, tv-logos, network`, then the default safety net.
  Oracle bucket `network-logo` (vs LU, which has no such tier; checked with CM's real
  `network_logo`).
- **tv-logos match name** is the channel name without its leading delimited country
  marker (`logos::tv_match_name` over the new
  `country::strip_delimited_country_marker`), not the identity's alias canonical: the
  canonicals are DB spellings (`HISTORY Channel, The`, `Paramount+ with SHOWTIME EAST`)
  that missed the right file (`Showtime`, `History` fell to the default).
- Logos oracle: plugin-side scores are each side's own (CM `_score` with the `+1` plus
  variant, LU `fuzz.ratio`) and each side's own threshold (LU always 85); `lu-order`
  is checked on Rust's own EPG/manager evidence against a CM tv-logos pick.

Open for the phase review (orchestrator, after C07)
- A stream or entry named only by its callsign (`KSDK`, `WABC-TV`) does not join the station's family for a network-led channel (`US: NBC (KSDK) HD`); Stream-Mapparr attaches it. Safe (writes less), attributed to `callsign-anchor` in docs/oracle-reports/streams.md. Likely cause: a bare callsign name resolves without a high-confidence callsign identity. Decide whether a name that is entirely a known station callsign should resolve to that station's callsign identity.

Step 17 implementation notes (Muse Spark, 2026-11-07, unreviewed; sandbox blocked shell execution, so `cargo fmt/clippy/test`, the oracle generation (`python3 scripts/oracle_report_lineup_create.py` to write `docs/oracle-reports/lineup-create.md` — `--check` fails until the report is generated once), `one_matcher_check.sh` and `actionlint` still need the orchestrator's gate run).
- `create.rs`: `create_preview` (Read) / `create_apply` (Write, confirm), both modes through `plan_creates`/`apply_creates` (groups first) with `load_owned` reconciled before planning, `ChannelCreator` seam (`run_with_creator` for tests), collision guard + global number check on every plan (`PlanOptions { collision, number_conflicts }`), ownership keys `lineup:create:<identity key>` (channels) and `lineup:create:group:<name>` (groups), CSVs mirroring CM's preview columns.
- Streams mode: candidates from streams' `StreamPool` (the step-13 `selected_m3us` home) + `m3u_group_filter` before matching (resolved group, case-insensitive); identity per stream; target name = `names::decide_heuristic` on the match name; category = `categorize::decide_category` (country labels none) except OTA-station identities map to CM's `Broadcast` import category (documented: categorize never classifies OTA, the import path is the only place CM writes it); `m3u_category_filter` after matching; group = custom or category; ignored targets fail the run like CM's `GroupScopeError`; unmatched streams reported, never created (no AI gate in create).
- ONE channel per identity family carrying the family's streams in pool-priority order (documented divergence: CM makes one channel per matched STREAM with `[M3U-id]` suffixes; the seeder already groups this way, and it is exactly what `streams.match` would then attach). Rerun creates nothing: owned keys dedupe plus a same-family check against live channels (foreign same-identity reported, never created) — the fix over CM's duplicate-on-rerun, pinned by `streams_apply_creates_one_channel_per_family_then_rerun_creates_nothing`.
- Stations mode: unattached streams in exact `seed_source_groups`, OTA stations only (callsign station, same OTA render names uses incl. market fallback; blocked callsigns never go premium), grouped by proposed name sorted like `build_seed_plan`, case-insensitive name skip, `seed_exclude_networks` via a verbatim port of `station_affiliation.station_networks`/`primary_network` (first network named, not a substring) plus name words, one channel per station in `seed_target_group` with numbers from `seed_start_number` via `next_free_numbers` (global, same walk as `allocate_channel_numbers`), no streams attached (as CM). A missing target group is created owned (CM errored; documented); an ambiguous one still fails like CM.
- Undo is the host's: created channels hide (pinned by `undo_hides_created_channels_and_lists_created_groups` via `undo::plan_undo`), created groups left and listed. Intent recovery pinned (`intent_recovery_adopts_the_landed_channel`). Consistency pinned (`created_names_re_resolve_to_the_identity_they_were_made_from`: premium same-family, OTA callsign kept).
- Oracle `scripts/oracle_report_lineup_create.py`: CM's real import decision path (oracles.py CM matcher + verbatim group/category filters, `_detect_duplicate_channels`, max+1) and the REAL `channel_seeder` planner, run twice to show CM's duplicate-on-rerun vs ours, vs `create_dump` (the action's own decides) cross-checked per stream against `names_dump`/`categorize_dump`; buckets `family-grouping`/`rerun-dedupe`/`naming-via-names`/`category-via-categorize`/`numbering`/`collision`/`seed-parity`/`merged-data`, bar 0 unattributed. CI runs `--check` after the logos one.
- For the step 20 README (create differences from CM): one channel per family (not per stream); no duplicate on rerun (owned keys + family check); collision guard instead of `[N]`/`[M3U-id]` suffixes; global number check; no AI gate (unmatched reported); missing stations target created owned; group filter/pool from streams' selection.

Step 17 review (Opus 5.5, 2026-11-07; cargo blocked in the reviewer sandbox, gate + report regen by the orchestrator)
- **BLOCKER fixed: create made channels from names' unknown-suffix rename.** Create read `decide_heuristic`'s row and took any `Renamed` row as a match, so with `tag_unmatched_channels` (default on) an unmatched stream or a station-less callsign (`ABC (WZZZ)`) became a `<name> [Unk]` channel whenever a custom import group was set (and was reported as "no group" otherwise — the failing `streams_mode_skips_unmatched_and_filters_categories`). Fix: `names::decide_target` -> `NameTarget { Ota, BlockedCallsign, Premium, Unmatched }`, the ONE naming decision; `decide_heuristic` is now only the CSV wording over it, and create creates only from `Ota { name: Some }` / `Premium`. Pinned by `streams_mode_never_creates_what_names_would_only_tag`.
- **SHOULD FIX fixed: duplicated OTA logic.** Stations mode had its own `render_station` plus the market-fallback branch copied twice. Now `names::decide_ota_target` (the OTA half of `decide_target`: callsign station, opt-in market fallback, blocked callsign) is called by names, by streams-mode create (through `decide_target`) and by stations-mode create. Category: premium targets go through `categorize::decide_category`, OTA targets map to `Broadcast` (the import path's one category), so `decide_category`'s own OTA branch and names agree by construction.
- No category (step-16 decision A: a country label is none) and no `m3u_custom_group_name`: the family is skipped with that reason (CM imported into the country-named group). Code, docs and `streams_mode_skips_a_family_with_no_category_unless_a_custom_group_is_set` agree.
- The other failing test was a wrong expectation: the test world already holds `Sports` (id 10), which `plan_creates` reuses by exact name; only `News` is created. The test now pins the reuse (ESPN lands in group 10).
- Family stream order is the streams app's: `quality_order` = `quality::sort_streams` over `StreamPool::quality_view` (new; `streams.match` now builds its keys through the same view) with the store's throughput (`quality::throughput_source`, new, used by match/sort/create). Not a second rule. Pinned by `family_streams_follow_the_streams_quality_order`.
- Crate direction: lineup -> streams is kept (StreamPool + quality order are the streams app's; moving ~1k lines of selection to core buys nothing). Rule, written in `crates/lineup/Cargo.toml`: streams never depends on lineup; step 19's lineup mode reads lineup's channels via core (store ownership keys `owned_keys("lineup")`) and takes the country from its own setting/workflow scope. If step 19 needs lineup-file parsing, that parser moves to core then. Cargo refuses the cycle, so the rule fails the build if broken.
- Oracle: `names_dump` now dumps `target_kind`/`target_name` (from `decide_target`); the create oracle checks the invariant first (create never creates from a name names would not name; channel == names target of the family head; category == categorize's, or `Broadcast` for OTA; a violation is unattributed) and attributes every remaining CM difference by an explicit check: `family-grouping`, `naming-via-names` (CM imports the raw stream name), `category-via-categorize`, `country-category` (the categorize oracle's `is_country`, imported, one definition), `engine-via-names` (names' decision vs CM's matcher; the names report attributes it), `rerun-dedupe`, `numbering`, `collision`. Stations: every seeded name equals names' OTA target of each source stream.

Step 18 implementation notes (Muse Spark, 2026-11-07, unreviewed; sandbox blocked shell execution, so `cargo fmt/clippy/test`, the oracle generation (`python3 scripts/oracle_report_lineup_sync.py` to write `docs/oracle-reports/lineup-sync.md` — `--check` fails until the report is generated once), `one_matcher_check.sh` and `actionlint` still need the orchestrator's gate run).
- `sync.rs`: `sync_preview` (Read) / `sync_apply` (Write, confirm). Pure `decide_sync` (one row per entry in LU's order): groups (prefix + category, get_or_create via the create stage, owned `lineup:group:<file>:<group>`); owned channels (key `lineup:<file>:<name>`, no category — step 13 review) keep their number unless the file number changed in `lineup` mode (renumber owned only, global conflict check via `PlanOptions { number_conflicts }`), move groups on `category_detail` change, never rename (names_apply owns names — LU renamed nothing either); new entries check the foreign same-name in the target group first (reported + skipped: the fix over LU's `get_or_create` adoption), else create with the lineup entry name (NOT names_apply's template — sync mirrors the provider file), the file/assigned number, no streams (step 19 attaches them), profile ids at create via `channel_profile_ids` (case-insensitive like LU, unknown warns; existing owned channels never gain rows — phase-2 rule). Auto modes assign ONLY new channels (`next_free_numbers` global: `auto_next` from 1, `auto_highest` above max, `specific` from the starting number); `lineup` takes the file's number (`None` stays numberless). Duplicates-in-lineup (26 cases): first category in LU's order wins, later same-named entries skip. `_migrate_single_group_name` (verbatim old/new naming) renames owned groups only (foreign left + reported; both-exist left + warned, like LU). Ignored target groups fail the run before anything is created (categorize's rule). Id-bound `Selection` (host scope never narrows a sync). Migration row runs first (ordinal shift past it, same key discipline as `create_and_edit_run_rows`).
- `tidy.rs`: `tidy_preview` (Read) / `tidy_apply` (Destructive, confirm). Pure `decide_tidy` over owned live channels of the configured file: no streams + visible -> hide; hidden + streams + `field_history(lineup, [tidy_apply], [hidden_from_output], 100)` hid it -> unhide; hidden + streams but tidy never hid it -> keep (operator hide, never unhidden). Host scope ignored by design: tidy is the garbage collector for what sync owns (documented in the scope result line). One `Op::Set HiddenFromOutput` plan; hide instead of delete (undo reverses both directions).
- Oracle `scripts/oracle_report_lineup_sync.py`: LU's real `_sync_groups` + `_do_sync_channels` (plus `_enable_channels_in_profiles`) with ORM fakes (indexed `FakeDB`: groups/channels/profiles/memberships/channelstreams; `Count` stub for `django.db.models`; MagicMocks elsewhere, conftest shape) over 29 files x 4 numbering x 4 detail (464 first runs + 464 reruns on the Rust-created world + a squatter that shifts LU's assigner) plus a 116-case clash/squat subset (foreign same-name on the first entry, number squat), vs `sync_dump` (new example: single case or `{"cases": [...]}` batch in one process; decides + migration + resolved profiles) and `tidy_dump` (new example). Buckets `ownership-not-adopt` / `stable-numbers-on-rerun` / `owner-key-ignores-category` (key equality + the detail-change sample: normal -> simple moves, 0 creates) / `global-number-check` / `duplicates-in-lineup` / `profile-rows-only-on-create` (adopted foreign 9001 in Living Room) / `migration-owned-only` (driving the REAL `_migrate_single_group_name`: owned renames, foreign renames on LU's side only, both-exist skipped on both) / `cleanup-delete-vs-hide` + `cleanup-owned-only` + `cleanup-unhide-guard` (the exact cleanup query chain driven against the fakes vs `tidy_dump`: LU deletes stream-less 1, 2, 6, 7; tidy hides owned 1, 8, unhides 3, keeps the rest), bar 0 unattributed. CI runs `--check` after the create one.
- Tests: create/rerun no-op (auto_next stable with squatter; `lineup` renumbers exactly the drifted owned channel), detail-change moves without re-creating, foreign clash skipped (same name elsewhere still creates, like LU's (name, group) lookup), profile ids at create only, global conflict refusal naming the holder, migration owned-only/both-exist/none, preview writes nothing, named scope `lineup` resolves the synced channels (settings file seeded to the test lineup), tidy hide/unhide/operator-hide guard + `hid_by_tidy_runs` exclusions (preview rows, running runs, non-ok rows) + undo of tidy (hide reverses, unhide reverses, operator channel untouched) + undo of sync (move reverses to Elsewhere, created groups left+listed).
- For the step 20 README (sync/tidy differences from LU): owned channels keep their number on re-sync (LU renumbered every rerun); only owned channels adopted by (name, group) — a foreign same-name clash is reported and skipped (LU adopted); number-conflict check global (LU set duplicates); profile rows only at create (LU enabled every synced channel); deletes are hide (undoable); tidy unhides only what tidy hid (run rows — never an operator hide); the owner key ignores the folded category (detail changes move, never re-create); sync names are the lineup entry names, not `names_apply`'s template (names owns display names; sync mirrors the provider file).

Step 18 review (Opus 5.5, 2026-11-07; cargo blocked in the reviewer sandbox, gate + report regen by the orchestrator)
- **The 7 failing tests were test bugs, not code bugs.** Rerun no-op and tidy both work: (a) `sync` run-level tests ran the DEFAULT `lineup_file` (DirecTV-Premier) while seeding/expecting `US_Combined` keys and row counts (preview 351 vs 463, apply with no move) — `test_settings()` now names `US_Combined`; (b) the "463 renumbers" came from switching an `auto_next`-numbered world (1..=N) to `lineup` mode, which rightly renumbers all — the test now builds a lineup-mode world, asserts it is a no-op, then drifts one number; (c) `migration_names("DTV-")` is `("DTV-All", "DTV")` (`_make_group_name` strips the separator); (d) tidy tests seeded ownership under a file other than the configured one; (e) `hid_by_tidy` test started a second run while the preview was still running (one running run per app), and its rows all had ordinal 0 (`INSERT OR REPLACE` on (run, ordinal) kept only the last). `small_world` also gave two groups the same name (the first two entries share a category).
- **Fixed (SHOULD FIX):** the `"<prefix>: All"` rename ran BEFORE the ignored-groups refusal (a refused run could still write) — the rename now runs after the check. The edit plan's global number check did not see the channels this run just created (a renumber could land on a fresh create's number) — created channels join `edited_world`. `hid_by_tidy_runs` now takes the LATEST tidy write per channel (tidy hid → tidy unhid → operator hid again is the operator's hide, never unhidden). The create planning + refusal-to-skip step is `sync::plan_sync_creates`, shared by the action and `sync_dump` (the dump previously showed `decide_sync` only, so the oracle never saw a global-number refusal).
- **Fixed (NIT):** `decide_migration` lost its unused `_file` param (callers pre-narrow `owned_groups`); the migration's `record_ownership` call was a no-op (`INSERT OR IGNORE` on (kind, id)) under a misleading "ownership follows the name" comment — removed, ownership is by id.
- **Oracle:** KeyError fixed (`_rust_sync` returns cases by id; the migration case is `["migration"]["migration"]`). Catch-all emits removed: the profile line, `cleanup-owned-only` (6, 7, 8) and `cleanup-delete-vs-hide` now emit only on their check; a dead "LU channels Rust never made" loop became a real check (every LU channel is pre-existing or matches a Rust create / duplicate skip / number refusal by (name, group)). `global-number-check` checks that LU's channel holds the refused number. New bucket `numbering-skips-dont-consume`: in auto/specific modes LU spends a number on every entry (duplicates, the adopted foreigner), Rust only on creates — checked as "Rust's k-th create == LU's k-th assigned number".
- Verified by reading: sync matches owned channels by key -> id (`owned.get(key)` then the world by id), so a `names_apply` rename never re-creates; the edit selection is bound to owned ids only; tidy reads only channel keys under the configured file's prefix. Duplication: groups/channels through `plan_creates`/`apply_creates`, numbers through `next_free_numbers` + `PlanOptions::number_conflicts`, edits through `plan_with_options`, rows through `create_and_edit_run_rows`, group names through `make_group_name`.
- **Second pass (Opus 5.5): the 3,925 auto-mode "number differs" rows were an oracle measurement bug, not a numbering-order bug.** Rust already numbers in LU's order: `decide_sync` walks `lineup.entries` (the loader's fold = `_apply_category_detail`'s order, file order within) and hands `next_free_numbers` out in row order; `build_create_request` / `plan_creates` keep that order and never renumber. The check read LU's number per entry from the FINAL DB channel by (name, group), but when the fold puts a duplicate name in one group (`none`/`simple`/`refined`), LU's second `get_or_create` finds the first channel and renumbers it (`Acento TV HD` is entry 36 in Nacionales and entry 198 in HD: LU's channel ends at 198, Rust's keeps 36), so the sequence check failed for the whole case and every later shifted create went unattributed (`A&E HD` 206 vs 199 = seven skipped duplicates earlier). The oracle now records LU's per-entry number from `_get_channel_number` in call order; it first CHECKS Rust's rows name exactly those entries in that order (the user-visible numbering order), then attributes a first-entry number LU's later same-group duplicate overwrote to `duplicates-in-lineup` (checked: Rust = LU's first-entry assignment, LU final = the duplicate's assignment) and the shift to `numbering-skips-dont-consume` (k-th create == LU's k-th call). No Rust change.
- LEFT (NIT): sync's foreign-clash check is per `(name, group)` (LU's lookup; pinned by a test) while `create` uses the core global collision guard — a deliberate difference, worth a line in the step-20 README. The owned-group migration only fires for a group sync itself created as `"<prefix>: All"` (a literal `All` category); a foreign LU-era group is always left and reported. The cleanup oracle transcribes LU's query chain (it is inline in the stream-match action) rather than calling a plugin method.

Step 19 implementation notes (Muse Spark, 2026-11-07, unreviewed; sandbox blocked shell execution, so `cargo fmt/clippy/test`, the oracle `--check` gates, `one_matcher_check.sh` and `actionlint` still need the orchestrator's gate run).
- **Hint design (core, no crate dependency).** `NamedScopeResolution` gains `ChannelsWithHints { ids, hints }` beside the bare `Channels` (existing constructors untouched); `NamedScopeHints { country: Option<String> }` is the one hint, with `none()` / `with_country()` constructors and `ids()` / `hints()` readers. `ResolvedScope` carries `scope_hints`, and `JobContext::scope_hints(world)` exposes them — the same world-needing path as `resolve_scope`, refused outside the runner like it. Streams never depends on lineup: lineup sets the hint, core carries it, streams reads it (the `lineup -> streams` production edge is unchanged; Cargo still refuses the cycle). `Channels` (no hints) keeps its meaning everywhere it is constructed (fakes, other apps).
- **Lineup side.** `owned_lineup_channels` returns `ChannelsWithHints` with the configured file's country (`filename_country` over the normalized full filename, the same spelling the ownership prefix pins, so the country can never disagree with the channel set). A file without a two-letter prefix carries no hint (a streams step over it then matches like an ordinary run; every vendored file has one). Per-entry countries (`CC_` prefixes, category countries) are NOT folded in: the hint is the default. Existing scope tests now pin the hinted shape (`US` for the default file, `UK` after switching to `UK_Freeview`).
- **Streams change.** New `streams.toml` setting `lineup_mode_country_restriction` (default true, schema + values + example docs). `MatchOptions.lineup_country: Option<String>` (None in `with_database`; the pure `plan()` path enables lineup mode only through it, so parity harnesses are byte-identical). `run()` sets it from `resolved.scope_hints.country` and turns `restrict_matching_to_country` on alongside it when the setting allows; off matches like an ordinary run. `effective_channel_country` is the one rule (grouping key, collector, zone routing, AI-gate accept path all read it): hint is the default, a channel whose identity names a different known country keeps its own, callsign identities stay exempt (bug-161), `None` without a hint. `build_groups` takes the hint (visibility passes `None`); the oracle `collect_with` path ignores it (SM had no lineup mode). Report names lineup mode in Match mode + Country filter notes.
- **Workflow.** `config/crosstalk.example.toml` `full_sync` rewritten in the `[[workflows.full_sync.steps]]` table form (one shape, no duplicate-key risk): sync -> streams.match (lineup scope) -> guide.match (lineup scope) -> logos.apply (lineup scope) -> tidy, `continue_on_failure` on the three middle steps; optional names/categorize steps commented with the sync-names-vs-canonical-names explanation (sync mirrors the provider file; names would rename to canonical display names).
- **E2E test** (`crates/lineup/tests/full_sync_workflow.rs`, guide as a test-only dev-dep): wiremock Dispatcharr with a stateful mock (listings read live state; creates/edits/associations applied) + real registry + real runner over UK_Freeview. Pins: sync creates owned channels (BBC One #1, BBC Two #2); match attaches only the unknown-country feed to the owned channel (US-marked same-name stream dropped, prefixed US stream nowhere, foreign ESPN untouched with its stream/epg/logo/visibility intact); guide assigns only the owned channel; logos resolves the entry icon; tidy hides the stream-less owned channel only; cross-app consistency via the shared `identity_cache` (one-version rows for the channel, stream and entry names, one family key); rerun creates/edits nothing and changes no state (review: guide now skips unchanged assignments, so the rerun writes nothing at all); undo of each step reverses it (created channels end hidden).
- **For the step 20 README (lineup-mode differences from LU):** the channel set is the owned-id bound (LU matched within group names); the country default is the lineup file's country (LU filtered per-channel with US<->MX exceptions and a provider-group pass, plus an East/West/Pacific feed filter streams keeps via zones); a foreign-identity entry keeps its own country; `attach-everything-above-threshold` is the identity family at the one cut; no replace-mode delete (that is `tidy_apply`).
- **Review (Opus 5.5, 2026-11-07), fixed in place:**
  - BLOCKER (FIXED) — guide writes were not undoable. `guide.match_apply` / `heal_apply` wrote through `batch_set_epg` and recorded only matching-shaped rows, so host undo refused them ("recorded no channel write that landed") despite the "undo for every app" decision. Both now write through `channel_ops` (`Op::Set { field: Epg }` over an id-bound `Selection`, one shared `matching::write_assignments`): one `epg_data_id` row per write (before = previous entry or `none`, after = the assigned id, `ok`/`failed` from the batch), appended AFTER the match/heal rows (ordinals continue), so the CSV, the match rows and the guide oracle report are unchanged. A failed batch still records its rows (failed) before the run fails with the client's message. Undo re-checks current == after and an Epg `none` before clears. Pinned by `an_apply_records_undoable_rows_and_skips_unchanged_assignments` and the e2e's guide undo. `streams.match_apply` already recorded `channel_ops` stream-list rows (undoable; the e2e undoes it).
  - SHOULD FIX (FIXED) — guide re-sent every unchanged assignment on every apply (20k channels = 20k associations and a programme-refresh queue per rerun). The channel-ops plan skips a channel already on its match; the e2e rerun now asserts zero writes of any kind.
  - SHOULD FIX (FIXED) — `streams.visibility_apply` with `scope = "lineup"` grouped WITHOUT the lineup hint (the comment claimed visibility never runs a lineup step; the id bound path exists for exactly that). `MatchOptions::apply_scope_hints` is now the one rule, called by match and visibility; unit-tested (on / setting off / no hint).
  - SHOULD FIX (FIXED) — e2e coverage claims that did not measure what they said: the "US-marked same-name stream dropped by lineup mode" was dropped by the same-account same-name rule (it now comes from a second account); a control test (`without_lineup_mode_the_foreign_streams_attach`) proves both US streams attach with the setting off; the foreign channel starts empty (an unscoped match/tidy would fill or hide it); the cross-app check compared `BBC One` with itself (the guide entry is now `BBC1`); the test now runs the `full_sync` block parsed out of `config/crosstalk.example.toml` (asserting its five steps, scopes and `continue_on_failure`), so the shipped example is what is exercised — before, the commented example was never loaded by any test.
  - NIT (FIXED) — the lineup-mode report note had a literal `\n` + indentation inside the string; doc for a country-less lineup file said it behaved like the explicit filter (it behaves like an ordinary run); `short` held the full filename.
  - NIT (LEFT) — `NamedScopeResolution::ids()` / `hints()` are used only by tests. `channel_ops::apply` marks a whole EPG batch failed when a later chunk fails although earlier chunks landed (shared with cleanup); undo then skips those landed rows. A per-chunk outcome from `batch_set_epg` would fix both callers. Observation for the phase review: `BBC One UK` does not join `BBC One`'s family (country-qualified identity), so a guide entry spelled that way stays unmatched.
  - Verified: no streams -> lineup edge (hint lives in core); hints never widen the set (the id bound is unchanged, hints ride beside it); `effective_channel_country` = hint default, own identity country wins when different, callsign identities exempt at all three callers (grouping key, `group_country`, `same_country_ids_for`); setting default `true` in code, schema and `streams.example.toml`.
