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
  never assign a number already used in the group (reported); every apply plans from live
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
- **Number conflict check** for creates/renumbers within a group.
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
- [ ] 9. Migrate streams (match, sort, mirror, visibility, probe format tiers, triage tags);
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
- `triage::ends_with_tag` (`crates/streams/src/triage.rs`) → `tags::has_trailing`
  (accepts `(DEAD)` and literal affixes; today only `[DEAD]`).
- `triage::FormatTags` strip regex and `restore::trailing_tag_pattern`
  (`crates/streams/src/restore.rs`) → `tags::strip` with kinds; needs an extension for
  operator-configured tokens (`video_format_suffixes`, custom rename-format labels are not in
  the vocabulary). Output changes: parens and stacked mixed runs strip.
- Restore's standard status set: IC's three today; the vocabulary's `status` kind also has
  CM `Unk` and guide `BadEPG`. Decide whether restore strips those (likely NO for `BadEPG`:
  that is guide's mark, not triage's).
- `triage::render_format` (`{name}` only, format trimmed) → `naming::render_channel`
  (`{group}`/`{number}`/`{epg}`/`{id}` substitute; no trim). Output changes for formats
  carrying those tokens.
- `triage::format_tags` (bracket labels of a format) — keep, but move beside
  `naming` so lineup's suffix rules read the same parser.
- Examples `crates/streams/examples/probe_io.rs` and `triage_io.rs` hard-code the five-label
  map; use `QualityTier::parse_label` / `IC_FORMAT_LABELS`.
- Stream-Mapparr's sort key (`crates/streams/src/quality.rs`) stays THE stream ordering;
  lineup mode reads tiers via `QualityTier::from_dimensions` (height-first), which is NOT
  IC's width rule — triage's suffix keeps `stream_format_of` (width) unless a decision row
  says otherwise.
- Logos (step 16 oracle): the resolver's similarity is the unified engine's, not
  rapidfuzz `ratio`; channel-name punctuation becomes a space (CM deletes it); `NO` →
  `nordic/norway` and `GB` → `-uk` suffix are fixes over CM; URLs are percent-encoded.
