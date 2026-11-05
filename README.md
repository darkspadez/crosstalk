# crosstalk

A Rust daemon that hosts tools for [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr).
One binary, one config directory, one scheduler and one web UI serve several sub-apps, each
defined by what it manages rather than by the plugin it replaces. Two ship today:

- **guide** — a fast, multi-threaded EPG matcher that assigns guides to channels, repairs or
  removes assignments that carry no programme data, and keeps the EPG sources themselves fresh.
- **streams** — which streams a channel has, the order they play in, and whether they work:
  match & assign, sort alternates, fill mirror backups, probe, triage and restore.

They replace three Python plugins — EPG Janitor, Stream-Mapparr and IPTV Checker — which run
inside Dispatcharr and read its database through the Django ORM. crosstalk is a separate process
that talks to Dispatcharr over its REST API, which means it survives Dispatcharr upgrades, runs
its matching across every core, and can serve several tools from one binary.

## What it does

- **Auto-match** scores every in-scope channel against every eligible guide entry, using
  callsign, state, city and network signals alongside a four-stage name pipeline (alias, exact,
  substring, token-sort fuzzy), with guards that keep numbered siblings, `+1` time-shifts and
  East/West feeds apart.
- **Scan & heal** finds assignments that carry no programme data and replaces them, touching
  only assignments it can prove are empty.
- **Scan** reports which in-scope channels have a guide but no programme data in the check
  window. It changes nothing, and its result set is what the cleanup actions work from.
- **Clean up** marks or removes the guides a scan found empty, as five presets over the same
  channel-ops primitive: append a bad-EPG suffix (`tag_missing_*`), unassign (`clear_missing_*`),
  unassign where the guide's name matches a REGEX (`clear_by_regex_*`), unassign across whole
  groups (`clear_groups_*`), and unassign from channels disabled in scope (`clear_hidden_*`).
  Each one previews before it writes, and each one re-reads the channels first, so a stale scan
  can only be out of date — never wrong about a channel.
- **Watchdog** audits every active EPG source that has channels mapped to it, refreshes the
  ones that are errored or whose guide ends inside a threshold, then re-reads them and records
  whether that helped. Dispatcharr's own refresh has no retry and no freshness awareness, so a
  source that starts failing — or whose guide simply runs out — stays broken until someone
  notices. A schedule is what arms the watchdog, which makes the action its interval.
- **Match & assign streams** gives every in-scope channel the streams that match its name —
  Stream-Mapparr's collector (name groups, the OTA callsign path, the fuzzy winner as a gate,
  alias force-includes, the token/substring family), then same-country and feed-zone rules and
  the quality sort, written as a replace or an append with custom streams kept last. Only the
  channels whose list actually changes are written.
- **Sort alternates** orders every channel's streams by the one quality model (resolution,
  frame rate, bitrate, measured throughput, account priority, audio). Zone routing keeps a West
  feed on a West channel instead of ranking it last, and a hand-made fallback stays last.
- **Fill mirror backups** gives each channel the same provider stream id from every active host
  in its M3U account's server group, inserted after the last provider stream.
- **Manage channel visibility** leaves exactly one channel per name group enabled in a profile —
  the first with a stream — and disables the rest.
- **Probe** ffprobes every stream that is due, optionally decodes it once for a black, frozen or
  silent picture, measures its delivery, and records both the media facts players read
  (`stream_stats`) and crosstalk's own health table. Never re-probes a stream inside its
  freshness window, and a run that outlives its window pauses and resumes. See
  [Probing streams](#probing-streams).
- **Triage & restore** act on the last probe: rename dead, blank and slow channels and move them
  to their groups, tag the ones that play with their format — then put a recovered channel's name
  and group back from the run store, with no state file to lose.
- **Optionally asks an AI** about the cases the heuristics are unsure of — and never about the
  ones they are sure of. See [AI verification](#ai-verification).

## Roadmap

Two more sub-apps are planned, again defined by what they manage:

- **lineup** (phase 3) — the channel list itself: names, numbers, groups, logos, creation. It
  also takes Lineuparr's **stream half**: matching a lineup file's channels to streams
  (`match_all_streams`, alias → exact → substring → token-sort, country twins and the quality
  twin rule) is a third mode of `streams.match`, not a separate app, because it writes the same
  stream lists this app already owns.
- **events** (phase 4) — time-bound event channels: create on a schedule, retire afterwards.

Each lands as its own phase of [`plans/plugin-consolidation.md`](plans/plugin-consolidation.md)
(`streams` was [phase 2](plans/phase-2-streams.md)), which also maps every action of the
remaining Dispatcharr plugins onto those four apps and records what is deliberately not ported.
Nothing here promises a date.

## Quick start

```bash
# 1. Configure
cp config/crosstalk.example.toml config/crosstalk.toml
cp config/guide.example.toml config/guide.toml
cp config/streams.example.toml config/streams.toml

# 2. Point it at Dispatcharr and give it a key
#    config/crosstalk.toml: base_url = "http://dispatcharr:9191"
#    and either api_key = "..." or the CROSSTALK_DISPATCHARR_API_KEY environment variable

# 3. Check the configuration
crosstalk validate --config config

# 4. Preview a run — this writes a CSV and changes nothing
crosstalk run guide match_preview --config config

# 5. Apply it, once the preview looks right
crosstalk run guide match_apply --config config --yes

# 6. Or run the daemon and use the web UI
crosstalk serve --config config
```

With Docker:

```bash
cp docker-compose.example.yml docker-compose.yml
cp config/crosstalk.example.toml config/crosstalk.toml
cp config/guide.example.toml config/guide.toml
cp config/streams.example.toml config/streams.toml

# Point config/crosstalk.toml at Dispatcharr. From inside the container
# "localhost" is the container itself, so use a reachable address:
#   base_url = "http://dispatcharr:9191"

# Compose refuses to start without both of these: a long random UI token and
# your Dispatcharr API key (from Dispatcharr's own UI).
export CROSSTALK_TOKEN="$(openssl rand -hex 24)"
export DISPATCHARR_API_KEY="..."

docker compose up -d --build
docker compose logs -f crosstalk
```

Prebuilt images for `linux/amd64` and `linux/arm64` live at
`ghcr.io/darkspadez/crosstalk`, so you can `docker pull` instead of building:
versioned tags plus `latest` from tagged releases (`v*`), and `edge` from every push
to `main`. Each release also attaches static Linux binaries (amd64/arm64) with a
`SHA256SUMS` file on the GitHub Releases page.

The image carries **ffmpeg** (which includes ffprobe), because `streams.probe` runs the
real tools: IPTV Checker's stream check is two subprocesses per stream, not an HTTP
request. Alpine installs them in `/usr/bin`, which is what `streams.toml` defaults to
(`ffprobe_path`, `ffmpeg_path`); change those two settings to point at a Homebrew or
`/usr/local/bin` install. Measured on arm64 (`docker build`, then `docker images` and
`docker history`):

| Image | `docker images` | ffmpeg layer |
|---|---|---|
| without ffmpeg | 52.2 MB | — |
| with ffmpeg (this image) | 212 MB | 113 MB |

(`docker images` reports a whole unpacked chain, so the two image sizes do not subtract to
the layer size; the ffmpeg layer is what `docker history` measures.)

So the probe costs about **113 MB** of image, almost all of it Alpine's codec libraries
(`libavcodec` alone is 13.3 MB, and the encoders it pulls in — x265 6.3 MB, aom 6.6 MB,
SVT-AV1 3.8 MB — are another 17 MB, with `/usr/lib` alone 99.7 MB once ffmpeg is in).
Removing ffmpeg is not an option for a probe that must run where Dispatcharr runs; the
alternative would be a second image, which the plan rejected.

The UI listens on <http://localhost:8730>.

> **Back up your Dispatcharr database first.** An apply rewrites guide assignments on channels.

## Commands

| Command | What it does |
|---|---|
| `crosstalk serve --config <dir>` | Run the daemon: REST API, scheduler and web UI |
| `crosstalk validate --config <dir>` | Check the configuration files and each app's settings |
| `crosstalk run <app> <action> [--dry-run] [--yes] [--fresh]` | Run one action once and exit (`streams probe --dry-run` measures everything and writes no stream stats) |
| `crosstalk run workflow <name> [--dry-run] [--yes] [--fresh]` | Run one workflow once and exit (`run workflow.<name>` works too); `--dry-run` runs every writing step as its preview |
| `crosstalk exports clear [--app <id>] [--yes]` | Delete the CSV reports the sub-apps wrote (the UI's **Clear exports** button does the same) |
| `crosstalk bench streams --synthetic [--config <dir>] [--channels N --streams N --accounts N]` | Time the streams plans on a world built in memory: no network, no Dispatcharr. See [Scale](#scale) |
| `crosstalk bench streams --live [--config <dir>] [--write-timing --channel-ids <ids> --yes]` | Time the same plans against the configured Dispatcharr, read-only unless `--write-timing` is asked for twice |
| `crosstalk simulate-streams --channels <csv> --streams <m3u>` | Offline stream-to-channel simulation, for tuning without touching a live install |

`crosstalk run` refuses an action that changes data unless `--yes` is passed, because a
one-shot run has no dialog to confirm in. `crosstalk exports clear` works the same way: it
prints how many reports it found and stops without `--yes`.

Clearing exports only ever deletes the sub-apps' own files, named by each app's prefix
(`guide_…`), so a directory shared with the plugins keeps theirs, and reports written in the
last couple of minutes are kept in case a run is still using one.

## Configuration

Two files, split by who owns the option:

- **`crosstalk.toml`** (host): the Dispatcharr connection, AI providers, the HTTP listener, the
  thread pool, the directories, the schedules — and every option the sub-apps must agree on:
  - `[matching]` — the normalisation toggles, which country databases to load, and custom
    aliases. Two apps disagreeing about whether `[HD]` is stripped would give two different
    answers for the same channel.
  - `[scope]` — `profiles`, `groups` and `ignore_groups`: which channels a run may touch.
    "Which channels am I responsible for" is the same question for every sub-app.
  - `[exports]` — `retention_days`, how long CSV reports are kept.
  - `[ai.verify]` — when a borderline match is worth asking a model about, and how its answer
    is read. The endpoints and keys stay under `[ai.providers.<name>]` in the same file.
- **`guide.toml`** (sub-app): what is genuinely the guide's own — the EPG sources to match,
  the check window, the auto-match and heal confidence thresholds, the heal fallback sources,
  the cleanup suffix and REGEX, and the watchdog's threshold, timeout, exclude list and
  recovery logging.
- **`streams.toml`** (sub-app): the stream half of the scope — M3U accounts in priority order,
  stream groups, the stream-name regex rules — then matching, sorting, probing and triage,
  including the seven triage switches (all off by default: an unattended rename is a choice an
  operator makes, not one a fresh install makes for them).
  Still shared, so not repeated there: Stream-Mapparr's tag handling is `[matching]`, its
  channel profile and groups are `[scope]` (with its exclude mode being `[scope]
  ignore_groups`), and both plugins' schedules, windows and "run after a refresh" triggers
  are `[schedules]`, `[workflows]` and `[hooks]`.

The rest of `crosstalk.toml` is `[server]` (`bind`, `port`, `token`/`token_env`), `[dispatcharr]`
(`base_url`, an API key or a JWT fallback, `timeout_secs`, `concurrency`), `[ai.providers.<name>]`
(the endpoints, keys and concurrency caps), `[jobs]` (worker threads, the exports and data
directories, `run_history_days`) and `[schedules]` (see [Scheduling](#scheduling)).
[`config/crosstalk.example.toml`](config/crosstalk.example.toml),
[`config/guide.example.toml`](config/guide.example.toml) and
[`config/streams.example.toml`](config/streams.example.toml) document every key with its why.

The split is a clean break: `guide.toml` rejects the old `channel_profile_name`,
`selected_groups`, `ignore_groups`, `csv_retention_days` and `[ai]` keys, and the error says
where each one moved. Two files claiming one setting would be worse than a loud failure.

Every value can be overridden by an environment variable named `CROSSTALK_<SECTION>__<KEY>`,
with a double underscore between levels:

```bash
CROSSTALK_SERVER__PORT=9100
CROSSTALK_DISPATCHARR__API_KEY=...
CROSSTALK_AI__PROVIDERS__TYPESAFE__API_KEY=...
CROSSTALK_SCOPE__PROFILES="All, Favorites"
CROSSTALK_EXPORTS__RETENTION_DAYS=30
CROSSTALK_AI__VERIFY__ENABLED=true
```

Secrets should live in the environment, not the file: the daemon refuses to save a
configuration that had environment overrides applied, so a token cannot leak into a file by
accident.

The web UI writes settings back through `toml_edit`, preserving your comments and ordering.
Host settings saved in the UI hot-reload: the next run uses them and schedules re-arm, with
no daemon restart (a run already in progress finishes with the settings it started with).

## Actions

Every action is named `<app>.<action>`, and every action that writes channel data is a
`<verb>_preview` / `<verb>_apply` pair: `--dry-run` on the apply runs the preview, and a
schedule key names the half you mean. Button colour is the consequence, the same convention the
Python plugins used: **blue** reads, **orange** writes data but removes no assignment, **red**
can move an assignment. Orange and red always confirm.

`validate` and `status` exist in both apps. `validate` is local — it has to work on a host that
cannot reach Dispatcharr yet, which is exactly when an operator runs it — and `status` reads the
run store, is the one action that stays clickable while a run is live, and reports that run's
progress.

### guide

`crosstalk run guide <action>`: a fast, multi-threaded EPG matcher and the maintenance actions
around it.

| Action | Colour | What it does |
|---|---|---|
| `validate` | blue | Check the settings for consistency, without touching Dispatcharr |
| `status` | blue | Show what the last run did, and whether a run is in progress |
| `match_preview` | blue | Score and write a report; changes nothing |
| `match_apply` | **red** | Assign guides, overwriting an existing assignment when a new match clears the threshold |
| `heal_preview` | blue | Look for replacements for empty assignments; changes nothing |
| `heal_apply` | **orange** | Replace empty assignments. Never removes an assignment it cannot prove is empty |
| `scan` | blue | Find in-scope channels whose guide carries no programme data in `check_hours`. Read-only; stores its result set for the cleanup actions and writes a CSV |
| `tag_missing_preview` | blue | Show which of the last scan's channels would get the bad-EPG suffix; changes nothing |
| `tag_missing_apply` | **red** | Append `bad_epg_suffix` to the last scan's channels, and unassign their guide when `remove_epg_with_suffix` is on. A channel that already carries the suffix is left entirely alone |
| `clear_missing_preview` | blue | Show which of the last scan's channels would lose their guide; changes nothing |
| `clear_missing_apply` | **red** | Unassign the guide of the channels the last scan found with no programme data |
| `clear_hidden_preview` | blue | Show which channels disabled in `[scope] profiles` would lose their guide; changes nothing |
| `clear_hidden_apply` | **red** | Unassign the guide of every channel disabled in `[scope] profiles`. Never deletes programme data |
| `clear_by_regex_preview` | blue | Show which channels would lose their guide because the guide's name matches `epg_regex_to_remove` |
| `clear_by_regex_apply` | **red** | Unassign the guide where its name matches the REGEX, within `[scope]` |
| `clear_groups_preview` | blue | Show every channel in `[scope] groups` that would lose its guide |
| `clear_groups_apply` | **red** | Unassign the guide of EVERY channel in `[scope] groups` |
| `watchdog_preview` | blue | Audit every active EPG source with channels mapped to it and report which are stale — errored, or with a guide ending inside `watchdog_horizon_threshold_hours`. Queues nothing |
| `watchdog_apply` | **orange** | Refresh each stale source on Dispatcharr, then re-read it and record whether that helped (`recovered` / `still_broken`). Never renames or unassigns a channel |

The cleanup actions select from the **last finished `scan`** rather than from a results file:
there is no age limit,
because each one re-reads every channel's current name and guide before writing, so a stale
scan can only be out of date — never double-tagged, and never clearing a guide that was
reassigned since. Their confirmation text is static, so the run summary, the log and the CSV
preamble name the scan run and how many channels it found.

The watchdog has no on/off switch and no interval setting: it is an action, so a schedule is
its interval (`"guide.watchdog_apply" = "0 */6 * * *"`, and a schedule naming the preview half
stays a preview). It refreshes sources **one at a time**, waiting for each, because a refresh is
a full XMLTV fetch and parse on Dispatcharr's Celery worker; a wait that lapses is reported as
"did not settle" rather than as a failed refresh. Staleness itself costs one request per source —
"does programme data reach `now + threshold`?" — and only a source the run is about to refresh
pays for more: a bisection back to 1970 answers both "carries no guide at all" (two requests, and
such a source is never refreshed, as in the plugin) and "when exactly does it end" (~28 indexed
counts: the cutoff probe, the 1970 probe, its re-probe inside the bracket and ~25 bisection
steps — because the report and the recovered/still-broken rule both need the value).

### streams

`crosstalk run streams <action>`: which streams a channel has, the order they play in, and
whether they work. Stream-Mapparr's verbs and IPTV Checker's buttons, as `validate` and `status`,
six preview/apply pairs (`match`, `sort`, `mirror`, `triage`, `restore`, `visibility`), one
`probe` action and three read-only tools. `crates/streams/src/lib.rs` declares them, and the
table below is in that order.

| Action | Colour | What it does |
|---|---|---|
| `validate` | blue | Check `streams.toml` for consistency, without touching Dispatcharr. Reports the threshold the sensitivity preset resolves to and how many regex rules are usable |
| `status` | blue | Show what the last run did, and what is running now — including a probe's progress. Stays clickable while a run is live |
| `match_preview` | blue | Match every in-scope channel against the selected streams and write a CSV of what each channel would carry. Changes nothing |
| `match_apply` | **red** | Give every in-scope channel its matched streams. With Overwrite Existing Streams on (the default) this REPLACES each matched channel's list; with Keep Custom Streams Last on, hand-made streams stay last. A channel that matches nothing is left alone |
| `sort_preview` | blue | Order every in-scope channel's streams by the quality model and the feed zone, and write a CSV. Changes nothing |
| `sort_apply` | **orange** | Rewrite the stream order of every channel whose alternates would reorder. Zone routing can drop the other zone's streams from a list |
| `mirror_preview` | blue | Work out each channel's missing backups from the other hosts in its M3U account's server group, and write a CSV of the additions. Changes nothing |
| `mirror_apply` | **orange** | Insert each channel's backup stream(s) after its last provider stream and before any trailing custom stream. Only ever adds, never reorders |
| `probe` | **orange** | Probe every stream that is due: ffprobe it, optionally decode it once for a black, frozen or silent picture, measure its delivery, and write the result into the stream's `stream_stats` and crosstalk's health table. `--dry-run` runs the whole probe and skips the Dispatcharr write |
| `triage_preview` | blue | Read the last probe's per-channel verdicts and work out every rename, group move and format suffix the triage switches in `streams.toml` ask for, as a CSV. Creates no group, changes nothing |
| `triage_apply` | **red** | Rename dead, blank and slow channels, move them to their groups (creating a missing one), and tag the channels that play with their format. Every change is recorded so restore can undo it |
| `restore_preview` | blue | Find the channels the last probe found playing again that still carry a triage change, and write a CSV of the name and group it would put back. Changes nothing |
| `restore_apply` | **orange** | Put back the name and group of every recovered channel that still carries a triage change — or strip the plugin's own tag from a name crosstalk has no record of |
| `visibility_preview` | blue | Work out which channel of each name group stays enabled in the profile (the first with a stream) and write a CSV of every membership that would change. Changes nothing |
| `visibility_apply` | **red** | Leave exactly one channel per name group enabled in the profile and disable every other in-scope channel. A group where no channel has a stream goes dark |
| `check_countries` | blue | Compare the country prefix on every stream's group name with the two-letter suffix on its guide identifier, and write a CSV of where they disagree. No provider connection, no change |
| `scan_placeholders` | blue | Find numbered stream-name families (`MAX 100`, `PPV EVENT 04`) that none of the placeholder patterns covers, and suggest a regex for each. Changes no setting |
| `test_rules` | blue | Show what the stream-name regex rules would do to every stream name, with sample changes and the per-rule validation report. Unscoped, as the plugin's action is: M3U and stream-group scoping do not apply |

Three things about the streams actions worth knowing before the first apply:

- **`probe` is the one writing action that is not a pair.** It writes stream stats and
  crosstalk's health table, never a channel's data, so "what would change" is a number the run
  only learns by measuring. `--dry-run` runs the whole probe, records health, the run rows and
  the CSV, and writes no stream stat.
- **`triage_apply` refuses unless at least one triage switch is on.** The plugins' seven
  scheduled post-action toggles are the action's whole instruction set, they all default OFF
  (as IPTV Checker's do), and a bare apply names them instead of reporting a no-op run.
- **Every apply writes only what changes.** A channel already holding its planned stream list, a
  channel already in its destination group, a membership already at its end state: each is a
  skipped row, not a write. The previews show the same rows.

The plugins' own schedules and run-after-a-refresh triggers are host features here: put the
order in a [workflow](#workflows) — `steps = ["streams.probe", "streams.restore_apply",
"streams.triage_apply"]` is IPTV Checker's own nightly order — and let
[`[hooks]`](#dispatcharr-events) start a match after an M3U refresh. Windows and freshness are
covered under [Probing streams](#probing-streams) and [Windows and resumable
jobs](#windows-and-resumable-jobs).

## Probing streams

`streams.probe` is the only action that opens a connection to a provider, and the only one that
can run for hours. What it does, and what it costs:

- **ffprobe, then optionally ffmpeg.** IPTV Checker's stream check is two subprocesses per
  stream, not an HTTP request. `ffprobe` reads the container and stream facts (codec,
  resolution, frame rate, bitrates); with `black_screen_detection`, `frozen_video_detection` or
  `silent_audio_detection` on, one `ffmpeg` pass asks for a black, frozen or silent picture.
  Both binaries ship in the image; `ffprobe_path` and `ffmpeg_path` default to `/usr/bin`, and a
  missing tool REFUSES the run instead of marking every stream dead (which is what the plugin
  did, clearing every `stream_stats` row on the way).
- **One connection per M3U account by default.** Each account is a lane
  (`probe_per_account_limit`, default 1), never above the account's own `max_streams`, with
  `stream_check_delay` (2 s) between starts on that account. Total parallelism therefore grows
  with the number of accounts, which is what a per-account connection cap is for; the plugin's
  global `parallel_workers = 2` is gone. `probe_rate_per_minute` (default 6) throttles the
  **throughput downloads only** — a download is a second connection to the same provider, so it
  also holds a lane permit. Stream checks themselves are paced by the lane, not by a global rate.
- **A window, and a checkpoint.** A probe that cannot finish in its slot stops at the window's
  end, is recorded **paused**, and the next firing continues the same queue. A cancel does the
  same: what landed stays, and the rest is still due tomorrow. See [Windows and resumable
  jobs](#windows-and-resumable-jobs).
- **Freshness.** A stream probed within `probe_freshness_hours` (default 24) is skipped, from
  the store's `stream_health` row rather than a resume file. That is what makes a backlog drain
  over several nights without re-probing yesterday's work, and it survives a settings change.
- **What it probes.** The in-scope channels' streams (all of them, or the primaries only with
  `check_alternative_streams` off), optionally only channels visible in a profile, optionally
  the streams attached to no channel at all (`probe_unattached_streams`, which IPTV Checker had
  no way to do). Each stream is probed once however many channels carry it.
- **Throughput** is measured when `enable_throughput_sorting` is on: an 8 s download
  (`probe_duration_seconds`) compared with the nominal bitrate for that resolution, cached for
  `probe_cache_ttl_minutes` (30) and never trusted when it failed.
- **What it writes.** `Stream.stream_stats` (merged on alive, cleared to `{}` on dead, untouched
  on skipped) and one `stream_health` row per stream; the per-channel verdicts are run rows,
  which is what triage reads.

Two numbers to plan a window with, both from the settings above: at the defaults a full pass
over 20k channels / 50k streams takes roughly **20–40 minutes of wall time** across 100
accounts — each account serialises its own ~500 streams over one connection, starts are
`stream_check_delay` (2 s) apart, and each check costs its own few seconds — and the throughput
downloads alone take far longer: **~139 hours** for 50k streams at 6 per minute (50,000 ÷ 6 ÷
60). That is why throughput is a switch: turn it on when the backlogs it feeds matter, and
expect it to drain over several nights in a window.

## Scale

The plan makes scale a first-class requirement — ~20k channels, ~50k streams, ~100 M3U accounts
— and `crosstalk bench streams` is how the numbers below were taken. `--synthetic` builds that
world in memory and times the four plans plus the write plan behind each; `--live` does the same
against your Dispatcharr and also times the world load.

```bash
# The world the numbers below came from: no network, no Dispatcharr.
crosstalk bench streams --synthetic

# A different size, if your install is not 20k/50k.
crosstalk bench streams --synthetic --channels 5000 --streams 20000 --accounts 40

# Against the configured Dispatcharr. Read-only.
crosstalk bench streams --live
```

Release build, Apple M2 Pro (12 cores, arm64, macOS), one run (`cargo build --release`,
`--synthetic` at the defaults):

```
synthetic world: 20,000 channel(s), 50,000 stream(s), 100 account(s)
  build the world                0.024s
  match       setup    0.042s  plan    2.685s  write plan    0.014s    4,990 write(s)    10 request(s)  20,000 channel(s) in 5,000 name group(s), 4,990 matched at least one stream, 50,000 stream(s) in the pool
  sort        setup         -  plan    0.726s  write plan    0.027s   20,000 write(s)    40 request(s)  20,000 channel(s), 20,000 with more than one stream, 0 already in order, 7,220 stream(s) dropped as the other zone
  mirror      setup         -  plan    0.121s  write plan    0.033s   20,000 write(s)    40 request(s)  20,000 channel(s), 20,000 would gain a mirror, 200,000 mirror stream(s), 0 left alone for a dangling id
  visibility  setup    0.004s  plan    0.310s  write plan    0.026s   15,000 write(s)    30 request(s)  profile "All": 20,000 channel(s) in 5,000 name group(s), 5,000 winner(s), 20,000 enabled before
```

So, for a 20k-channel / 50k-stream library:

| What | Cost | Note |
|---|---|---|
| `match_preview` / `match_apply` plan | **≈ 2.5–2.7 s** | Five thousand name groups, 50k streams in the pool, on every core. `visible_channel_limit` (default 1) decides how many channels of a group are written, which is why this world writes ~5,000 lists rather than 20,000 |
| `sort_preview` / `sort_apply` plan | **≈ 0.7 s** | 20k channels × 10 streams = 200k references. The synthetic families carry zone suffixes, so this exercises zone routing (the name normaliser over 20k channel names); a zone-free world plans the same lists in ~0.1 s, which is what the step-5 scale test measures |
| `mirror_preview` / `mirror_apply` plan | **≈ 0.13 s** | One pass over the accounts and streams, one per channel |
| `visibility_preview` / `visibility_apply` plan | **≈ 0.3 s** | Name grouping is the match collector's, without the stream collection behind it |
| write requests for 20k changed channels | **40** | `edit/bulk` carries 500 channels per request, so a whole-library apply costs tens of requests, not 20,000. A rejected chunk costs one PATCH per channel in it (see [Differences](#differences-from-the-plugins)) |

A debug build is 10–50× slower on exactly this code and says so in its own output; quote the
release numbers. The point of the two-request-pattern bench is the bridge question: whether REST
is fast enough for stream-list writes on your box, or whether a Dispatcharr plugin should write
`ChannelStream` rows directly. Time it on the machine that will run it:

```bash
crosstalk bench streams --live --write-timing --channel-ids 12,13,14,15,16 --yes
```

That reads back those channels' stream lists just before writing and rewrites each **to the value
just read** — an idempotent no-op, so nothing changes — once per channel and once as one
`edit/bulk` chunk, then extrapolates
both to 20,000 channels. The rule the plan states: **the bridge is unnecessary if a 20k-channel
apply fits in 120 seconds**, which is what the bench's recommendation line tells you once it has
measured. Until that has been run on the target host, crosstalk writes over REST and the
`StreamListWriter` seam is where a bridge would plug in.

## AI verification

Matching is a yes/no decision, so the model is asked a yes/no question — not for a paragraph of
prose. Two things happen only:

1. **Verify** — the best heuristic score lands in the configured band (default 80–94). The score
   is plausible and could be wrong, which is where a second opinion is worth a request.
2. **Rescue** — nothing reached the band, but a candidate cleared the rescue floor (default 60).

The contract, precisely:

- A heuristic result at or above `match_confidence_threshold` (default 95) never
  reaches the model. The model may promote inside the band, but it never re-checks or
  vetoes a heuristic win.
- Promotion happens only inside the verify band or above the rescue floor — nowhere else.
- An AI-accepted pick is still validated for programme data. When every same-name copy
  of the pick is empty, the channel is left unmatched and the reason says so
  (`The AI accepted … but it carries no programme data …`); there is no fallback to
  other candidates, which by construction all scored below the threshold.
- Heal never consults the model: it is heuristic-only by design.

The default provider is [TypeSafe](https://docs.typesafe.ai), whose System One models answer
typed questions with calibrated probabilities: one request carries the channel, a shortlist and
every question, and the answers come back together. Any OpenAI-compatible endpoint can be used
instead, but a chat model asked for a probability is not the same thing as a decision model
returning one, and the UI says so (`calibrated: false`).

```toml
# crosstalk.toml
[ai.providers.typesafe]
kind = "typesafe"
base_url = "https://api.typesafe.ai/v1"
api_key_env = "TYPESAFE_API_KEY"
max_concurrency = 8          # the public endpoint rate-limits around eight

[ai.verify]                  # the policy every sub-app shares
enabled = true
provider = "typesafe"
model = "jev-latest"         # pin a version to keep answers stable across releases
verify_band_low = 80
verify_band_high = 94
rescue = true
rescue_floor = 60
shortlist = 5
accept_threshold = 0.8       # accept at or above this yes-probability …
reject_threshold = 0.3       # … drop at or below this; between the two, leave unmatched
min_confidence = 0.5
```

Every AI decision is stored with the run: the model that answered, the request, the response,
the probabilities and the latency.

## Scheduling

```toml
# crosstalk.toml
[schedules]
"guide.watchdog_apply" = "0 */6 * * *"
"guide.scan" = "30 */6 * * *"
"guide.match_apply" = "0 4 * * *"
"guide.heal_apply" = "0 6 * * *"
"guide.clear_missing_apply" = "30 6 * * *"
```

Five fields (`minute hour day month weekday`) or six (with seconds). Schedules fire in **UTC**,
and the dashboard shows each one's next run so you can see it was understood. A schedule naming
a preview action stays a preview. A cleanup schedule reads the last finished `scan`, so a
`guide.scan` entry ahead of it in the day is what makes it do anything: with no finished scan it
refuses to run and says so.

### Windows and resumable jobs

A job that cannot finish in one slot — a probe over 50k streams against provider connection
limits — runs inside a **window**. An entry becomes a table instead of a bare expression:

```toml
[schedules]
"guide.match_apply" = "0 4 * * *"
# Four hours from each firing: stops at 03:00, resumes at 23:00 the next night.
"streams.probe" = { cron = "0 23 * * *", window = "4h" }
# Until 06:00, wrapping past midnight when it has already passed; read in its own zone.
"streams.probe_weekend" = { cron = "0 22 * * 5", until = "06:00", timezone = "America/New_York" }
```

`window` and `until` are exclusive; `until` defaults to UTC, like the expression, and a time
that has already passed means tomorrow (so a 23:00 start until 06:00 is eight hours, not minus
seventeen). A time the clocks skip ends at the first instant that exists.

A windowed run checks the clock between work units. When the window closes it saves its place
and stops with the run recorded **paused** — distinct from `succeeded` (the job finished) and
from `cancelled` (someone stopped it). The next firing of the same action loads the checkpoint
and continues, so a backlog drains over several nights. The dashboard shows each schedule's
window and the run page says what a paused run will do.

Checkpoints survive a cancel too: cancelling at 03:00 because the host is needed lets tomorrow
continue where today stopped. To start a backlog over — after changing the scope, say — ask for
a fresh start: `crosstalk run streams probe --fresh`, or the **start fresh** checkbox on the app
page. Settings are re-read on every run, so a change takes effect at the next window.

Every run explains itself in the history: `cancelled` runs keep the rows, the counts and the CSV
they produced, and each unwritten row says it was not applied, so a half-done apply is never
recorded as a success.

## Workflows

A workflow is a named sequence of actions the host runs in order, because the plugins that
preceded crosstalk called into each other: Stream-Mapparr's own "probe then sort" sequence, or
its hand-off after IPTV Checker's scan.

```toml
# crosstalk.toml
[workflows.nightly]
steps = ["streams.probe", "streams.sort_apply"]
# A new pass starts every step's backlog from the beginning. Off by default, which resumes.
fresh = false

[schedules]
# A workflow is scheduled like an action, under its own name space.
"workflow.nightly" = { cron = "0 23 * * *", window = "4h" }
```

Each step is an ordinary run: its own app's one-run-per-app slot, its own run page, its own
CSV, its own checkpoint. The workflow itself is one more run record (app `workflow`, action =
the name) and every step run points at it, so the workflow's run page lists the steps it ran.
Steps run one at a time and the sequence **stops at the first step that fails or is cancelled**.
A step that pauses at its window's end pauses the workflow; the next firing resumes *that* step,
with the step's own checkpoint picking up its backlog, and continues from there. Cancelling the
workflow cancels the step it is inside. The window's deadline reaches every step, so a workflow
inside a four-hour window never runs past it. One workflow runs at a time.

Run one by hand with `crosstalk run workflow nightly` (or `crosstalk run workflow.nightly`),
from the dashboard's **Workflows** card, or from a hook.

## Dispatcharr events

Dispatcharr's Connect integration can post to crosstalk when something happens — an M3U account
finished refreshing, an EPG source did — and each event can trigger an action or a workflow:

```toml
# crosstalk.toml
[hooks]
debounce_secs = 60
rate_per_minute = 60
m3u_refresh = ["streams.match_apply"]
epg_refresh = ["workflow.nightly"]
```

Setting it up, in Dispatcharr's own UI:

1. **Settings → Connect → Add Connection**, type **Webhook**.
2. URL: `http://<crosstalk host>:8730/hooks/dispatcharr` — add `?event=m3u_refresh` (or the
   event you are subscribing) if you do not use a payload template, because Dispatcharr's own
   payload carries no event name. The URL is *exact*: keep the path, and note the port is
   `server.port`.
3. Header: `Authorization: Bearer <server.token>`. The route accepts **only** this: a session
   cookie is refused (a browser session must not be able to trigger an apply by visiting a
   URL), and a daemon with no `server.token` refuses every delivery rather than accept
   unauthenticated work.
4. Add `Content-Type: application/json` and a per-event **payload template** such as
   `{"event": "m3u_refresh", "account": "{{ account_name }}"}` if you want a JSON body; without
   one Dispatcharr posts the event's details form-encoded (`account_name=…&streams_created=…`),
   which crosstalk reads too.
5. Subscribe the events: `m3u_refresh`, `epg_refresh`, and any others you configured.

How a delivery is handled:

- **Auth**: `Authorization: Bearer <server.token>`, never a cookie; the route is exempt from
  CSRF only for that reason. Deliveries are rate limited per address
  (`hooks.rate_per_minute` over a minute, `429` with `Retry-After`).
- **Which event**: an `?event=` query parameter, then an `event` or `type` field in the body,
  then — because Dispatcharr's default payload names none — the payload's own keys
  (`account_name`/`total_processed` is `m3u_refresh`, `source_name`/`programs` is
  `epg_refresh`), and only for an event the configuration names.
- **Debounce**: several M3U accounts finishing together is one logical "the playlists changed",
  so every delivery of the same event restarts a `debounce_secs` timer and the run happens once
  after the burst settles.
- **Busy target**: if the target app (or the workflow slot) is busy, exactly **one** follow-up
  run is queued for when it frees — Stream-Mapparr's dirty-flag behaviour, so a late account is
  not dropped and a busy app is not started twenty times.
- **Answer**: `202` immediately, because Connect waits ten seconds and a match can take minutes;
  the work runs in the background. An event the configuration does not name is logged and
  answered `202` too — "I do not act on this" is not a delivery failure.

```bash
curl -X POST http://127.0.0.1:8730/hooks/dispatcharr \
  -H "Authorization: Bearer $CROSSTALK_SERVER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"event": "m3u_refresh", "account_name": "test"}'
```

## Web UI

- Dashboard: apps, workflows with their steps, last run and a **Run** button, the hook table and
  the Dispatcharr setup, schedules with their window and next run, recent runs.
- Workflow run page: the steps it ran, each linking to its own run page; a step's page links
  back to the workflow.

- App page: actions with their colours, a confirmation for the ones that write, a **start
  fresh** checkbox on the writing ones, the run in progress with a cancel button. A read-only
  `status` stays clickable while a run is live and reports that run's progress.
- Run page: live status, the full result table with the reason for every row, the AI decision
  where there was one, and the CSV download.
- Settings: every option with the help text the form shows, written back to the TOML files.
  The host form carries the shared options — scope, CSV retention and the AI policy — plus
  the matching toggles; each app's form carries only what that app owns.

The UI binds to `127.0.0.1` by default. To expose it, set `server.bind` and a `server.token`;
the daemon refuses to start on a non-loopback address without a token. A browser signs in at
`/login` and gets an `HttpOnly` cookie; a script sends `Authorization: Bearer <token>`.

Bots and scripts talk to the same routes the forms do, with two differences from a browser:

- Scripts send `Authorization: Bearer <token>` on every request and are CSRF-exempt: a
  cross-site page cannot forge that header, so a request carrying it is by definition not
  a forged browser form.
- Browsers send the `crosstalk_token` cookie plus the `csrf_token` hidden field (or the
  `x-csrf-token` header); without the synchroniser token a POST is rejected with 403.
- With no token configured, only loopback `Host` values reach the daemon at all: a
  DNS-rebinding page's hostname may resolve to 127.0.0.1, but its foreign `Host` header
  is rejected before anything trusts it. With a token configured the Host check is skipped
  (reverse proxies use foreign Host values by design) and the bearer token is the
  protection; a POST whose `Origin` disagrees with `Host` is still rejected as a forgery.

## How matching works

For each channel and candidate guide entry, two independent scores are computed and the better
one wins:

- **Structural**: a shared callsign is 50 points, a shared state 30, a matching city 20 more, and
  a shared network keyword 10 on top of a signal that already fired.
- **Fuzzy**: aliases first, then an exact match after normalisation, then a substring match with
  a length-ratio and token-overlap guard, then token-sort Levenshtein with a length-scaled
  threshold.

On top of that: a shared high-confidence callsign floors the score at 95, a disagreeing one
rejects the candidate outright, numbered siblings and `+1` time-shifts never cross-match, and
East/West/Pacific feeds are kept apart.

The matching core is a faithful port of the Python matcher, and that is checked rather
than asserted — but only for the core. The split contract is:

- **Strict parity**: the matching primitives (normalisation, callsign/location/network
extraction) and the full `match_all_streams` pipeline are byte-identical ports.
`crosstalk simulate-streams` produces **byte-identical** reports to the plugin's own offline
tool across four configurations: default flags and `--custom-aliases` (which change matching
output), `--no-ignore-quality` (which catches Rust wrongly applying the flag), and
`--countries US` (which proves flag acceptance and call-sign DB merge parity). Duplicate
stream names in the sim corpus exercise parser dedupe — they are removed before matching,
so duplicate *candidates* reach the matcher in the match/heal unit tests for the H6
duplicate-name fix, not via sim. `scripts/parity_sweep.py` compares the primitives over
hundreds of thousands of cases in a full local run (no `--limit`); the bounded CI sweeps
cover ~4,800 normalize, ~900 per-name (callsign/location/network), and ~1,200
match/score cases. All three gates run in CI against the plugin pinned as a submodule in
`reference/`.
- **Rust-native actions**: `match`, `heal` and the cleanup presets reuse the parity-checked
pipeline but make their own product decisions. They are not ports, and row-for-row equality
with the plugin is not claimed for them. `scripts/channel_ops_parity.py` compares the
*selection* and the resulting payloads of every cleanup action against the plugin's own
logic, which is the part where equality is claimed.

## Differences from the plugins

crosstalk is a port, not a fork, and every place where it behaves differently is listed here
with the reason. The matching pipeline is not in this table: it is a strict port, and the gates
above hold it to byte equality.

Which plugin a row is about is in its wording: a row naming a `guide.*` action (or an unprefixed
guide action such as `scan`, `heal` or `watchdog_apply`) is a difference from EPG Janitor, and a
row naming a `streams.*` action is a difference from Stream-Mapparr or IPTV Checker. A row about
a host feature every app shares (`Clear exports`, CSV quoting) says so instead.

| Difference | Reason |
|---|---|
| Stream selection matches account and stream-group names exactly | Stream-Mapparr looks both up in an exact, case-sensitive `{name: id}` map with no wildcards, and so does crosstalk — unlike `[scope] groups`, which accepts `*` and `?`. A token with a `*` in it therefore matches no stream group and lands in the unknown-names warning instead of silently selecting a family the plugin would have fallen back past. The account list's ORDER is the priority, and a source it does not name is kept and sorts last (999). |
| `streams.sort_*` does not partition by country | Stream-Mapparr's bug-158 fix makes the country tier the OUTER key when `restrict_matching_to_country` is on (`_same_country_ids_for`). A sort here orders by the quality key and the feed zone only, whatever the setting says; with the setting off — the default — the two agree exactly. `match` does partition by country. |
| A sort's CSV is the shared channel-operation report | The same columns every other channel write writes (before list, after list, status, reason) instead of the plugin's per-stream `tiers` / `throughput_mbps` / `edge_ips` columns; the tiers are summarised in the report's preamble instead. |
| `streams.sort_*` needs no Channel Profile | The plugin refuses without one ("Profile name is required"). The profile list is `[scope] profiles`, and an empty one means every channel, like every other action's scope. |
| A sort tolerates the stats shapes that crash the plugin | `source_fps` written as a numeric string, and a throughput cache entry whose numbers are strings, make the plugin's whole sort action raise. crosstalk reads the number it can read and falls back to 0 (or to the re-estimated nominal bitrate) for the rest — the answers the plugin gives for the same stat written numerically. A probe never writes either shape; only a hand-edited cache could. |
| One counted warning replaces a log line per channel | The plugin logs one line per zone-routed channel that had to keep other-zone streams. At 20k channels that is 20k lines in one run. |
| `streams.match_apply` only writes channels whose list changed | The plugin deletes and recreates every matched channel's list; crosstalk plans a stream-list write only where the list actually differs. Same end state, so a second run over an unchanged library writes nothing. |
| Stream-Mapparr's US-OTA-only mode is a setting, not a second action | The plugin ships it as its own button with a different candidate rule: per CHANNEL (no name grouping, no `visible_channel_limit`), US callsigns only, upper-case callsign occurrences in stream names, and no country filter or zone routing. Here it is `ota_only` on the same pair, because it is the same write with a narrower rule. |
| `streams.match` has no AI gate | The plugin has none, and a stream match is not a one-of-N decision with a confidence band to second-guess: it is a name family, collected once per group. The AI policy in `[ai.verify]` is the guide's. |
| `streams.mirror_apply` leaves a channel holding a dangling stream id alone | The plugin plans such a channel from its join (which drops the id) and then refuses it at the locked re-read (which still sees it). crosstalk plans it the same way and never writes it, instead of rewriting the list without the id. The CSV still lists the planned additions with a `skipped` status, and the readout counts match the plugin's. |
| `streams.mirror_*` reads streams from one loaded pool | The stream list over REST omits streams on inactive M3U accounts, so a channel holding one of those is left alone too — strictly less than the plugin writes there, never more. |
| `streams.mirror_apply` records the previous order in the run store, not a snapshot file | The plugin refuses to write unless it first saved every changed channel's order to a snapshot file, and skips a channel that moved mid-run. crosstalk plans and writes one world snapshot: what the run reports as `before` is what Dispatcharr held when the run started, and a concurrent edit after that is overwritten rather than detected. Same trade as every other channel-write action. |
| `streams.visibility_apply` writes only the memberships that move | The plugin disables every channel in scope and re-enables the winners — two bulk updates touching rows that end up unchanged. crosstalk writes the end state as per-channel values, so a channel already holding its value is a skipped row, not a write. Same end state. |
| `streams.visibility_apply` writes the first named profile that exists | Stream-Mapparr's `profile_name` list resolves to `profile_ids[0]` for the write while every named profile widens the channel set; crosstalk does exactly that. With a single profile the action only ever disables. |
| `streams.visibility_apply` creates the membership row a winner is missing | The plugin's two `update(enabled=...)` statements touch existing rows only, so a channel with no row in the profile stays hidden even as the winner. The membership write goes through `PATCH .../bulk-update/`, which creates a missing row, so a rowless winner becomes visible. Rowless losers agree either way and are never written. In practice every channel has a row in every profile; the parity harness covers the sparse shape with the divergence counted. |
| `streams.visibility_*` reads Dispatcharr live, not the last load snapshot | The plugin reads its `processed_data` file and refuses to run without it. A channel created since the last load takes part here. |
| `streams.probe` is one action with `--dry-run`, not a pair | It writes stream stats and crosstalk's health table, never a channel's data, so "what would change" is a number the run only learns by measuring. `--dry-run` runs the whole probe, records health, the CSV and the per-channel verdicts, and writes no stream stats. |
| `streams.probe` caps connections per M3U account, not workers | IPTV Checker's global `parallel_workers` (2) is gone: `probe_per_account_limit` (default 1) connections per account, never above that account's `max_streams`, with `stream_check_delay` between starts on it. Total parallelism grows with the number of accounts, which is the limit a provider actually enforces. |
| `probe_rate_per_minute` throttles throughput downloads only | That is the one place Stream-Mapparr used it (a global cap around the HTTP downloads). Neither plugin capped its stream CHECKS per minute — IPTV Checker is paced by its workers, its per-stream delay and its 429 backoff — so neither does the probe. A download also holds a lane permit, because it is a second connection to the same provider. |
| `streams.probe` skips a stream probed within `probe_freshness_hours` | The plugin's idea of progress was a JSON resume file per window and its results file; it had no notion of "this stream was fine an hour ago", so every window re-probed everything except what the resume file named. crosstalk reads the store's `stream_health` instead, which also makes an interrupted run resume for free. |
| `streams.probe` checkpoints its queue in the run store | The plugin kept a resume JSON file per window, so a lost or invalidated file re-probed everything. Crosstalk checkpoints the queue with the run: a probe that hits its window's end is recorded PAUSED and the next firing continues it, a cancel keeps the place too, and `--fresh` (or **start fresh** on the app page) is how an operator starts the backlog over. |
| `streams.probe` retries per stream, not in global passes | Same retryable error types and the same count, spaced `stream_check_delay × 3` apart, but a retry happens beside the stream that failed instead of after re-walking the whole queue. At 50k streams the plugin's pass-based loop re-queued the failures for a second full sweep. |
| `streams.probe` probes each stream once, however many channels carry it | The plugin's loaded-channels file repeated a stream once per channel, so a stream on four channels cost four probes and left four result rows. |
| `streams.probe` can probe streams attached to no channel | `probe_unattached_streams`, off by default. IPTV Checker had no way to check an orphan stream, and a provider's new stream is exactly that until a match runs. |
| `streams.probe` refuses to run without ffprobe | A missing binary made the plugin classify every stream `Other`/Dead and clear every `stream_stats` row — the worst possible outcome of a probe. crosstalk checks the two paths before the first stream and refuses the run, naming the setting. |
| `streams.probe` leaves an unchanged `stream_stats` alone | Same end state; a nightly run over 50k streams should not be 50k PATCHes. The run counts what it wrote and what it left. |
| `streams.probe` records a cancelled probe as neither a verdict nor a count | A killed invocation (cancel, shutdown) is excluded from the run's records, as the plugin excluded its `Cancelled` rows — but it also writes no health row, so the stream stays due for the next run instead of looking dead. |
| `streams.probe` measures throughput when `enable_throughput_sorting` is on | Stream-Mapparr's separate `probe_throughput` action, folded in: same 8 s download, same 64 KiB-in-under-a-second guard, same "a failed measurement is never fresh". It costs a second connection per stream, so turning the sort's throughput tier off turns the download off too. |
| `streams.probe` writes a fixed CSV column set | The plugin emitted dynamic `ffprobe_*` columns per run. crosstalk keeps its base twelve columns and adds the evidence as fixed ones (`container_duration_seconds`, `calculated_bitrate_kbps`, `packet_count`, `mean_volume_db`, `throughput_mbps`, `edge_ip`), so two runs' reports diff. |
| The ffprobe analysis window is a constant | `ffprobe_analysis_duration` is retired with the other keys that moved, so the window is the port's own constant; the two timeouts that decide a stream's fate (`timeout` for the connection, `probe_timeout` for stream data) are settings. |
| `streams.probe`'s per-channel verdicts are run rows, not a results file | `triage` reads the `probe_verdict` and `probe_format` rows of the last probe run. The plugin read `results.json` and refused to act without it. A `--dry-run` probe counts like any other: it records the same verdict rows and only skips the Dispatcharr write. A windowed pass split across nights reads as one: the newest succeeded run plus the finished runs back to the previous succeeded one, newest verdict winning per channel (the plugin's accumulated `results.json`). |
| `streams.triage_*` is one action with seven switches | The plugin had seven buttons, each its own confirmation, and a scheduler block that gated the same six renames/moves plus the suffix. Here they are seven switches in `streams.toml` — `rename_dead_channels`, `rename_blank_channels`, `rename_low_framerate_channels`, `add_video_format_suffix`, `move_dead_channels`, `move_blank_channels`, `move_low_framerate_channels` — that `triage_apply` reads. **All seven default OFF**, as the plugin's scheduler toggles do, so an apply with nothing switched on REFUSES and names them instead of reporting a no-op run. The plugin's scheduled order is a workflow: `["streams.probe", "streams.restore_apply", "streams.triage_apply"]`. |
| `streams.triage_apply` does not tag a channel twice | The plugin renames from the name the last check recorded, so a channel that stays dead becomes `ESPN [DEAD]` on Monday and `ESPN [DEAD] [DEAD]` on Wednesday, and only Restore collapses the stack. crosstalk appends nothing when the recorded name already ends with the format's own bracket label. Same after a restore; the parity harness counts this shape and asserts the tag is the only difference. |
| `streams.triage_apply` writes only the values that change | The plugin bulk-updates every channel of a class, including one already carrying the tag and one already in the destination group. `channel_ops` reports those as skipped rows, so the same end state costs fewer writes. |
| The triage destination group is created only on an apply | The plugin's `get_or_create` runs when the move action runs, so a dry run creates nothing either — but its "preview" was the dry-run switch inside the write. crosstalk's preview plans the move against a placeholder id carrying the real group name, so the CSV shows `Graveyard` and `POST /channels/groups/` is never sent. |
| `streams.restore_*` reverses crosstalk's own run rows, not `channel_state.json` | The plugin kept a state file of the group each channel came from and an in-memory tag list derived from the current settings. crosstalk records every rename and move with its `before` value in the run store, and a recorded change is still in effect when the channel's current value equals the change's `after`. No file to lose, no last-writer-wins between processes, and the record survives a settings change. |
| `streams.restore_*` undoes what it recorded, by value | The plugin strips every known trailing tag by NAME, so it also removes a `[HD]` the operator typed by hand, and it cannot see a label the current formats no longer name. crosstalk restores the recorded value (so a hand-typed tag survives) and falls back to the plugin's tag strip only for a channel with no recorded change at all — which is how a channel the plugin tagged before crosstalk took over is healed. |
| `streams.restore_*` leaves the format suffix alone | The plugin's settings text says restore does not strip the video format suffix, and its regex can. crosstalk never reverts a recorded suffix row, but a suffix stacked on top of a recorded mark does not block the mark's restore either: the trailing format run is stripped to follow the chain (as the plugin's own strip does) and dropped with the mark. Undoing the suffix nightly would flap against the next triage run. |
| `streams.restore_*` reads the newest 50 `triage_apply` runs | The plugin's `channel_state.json` kept one entry per channel until it was cleared by a restore, however old. crosstalk reads the channel rows of the newest 50 finished `triage_apply` runs (and `[jobs] run_history_days` prunes the table underneath): a change older than either falls back to what the name itself says, so a very old mark still loses its tag but a very old move may keep its group. |
| A recorded group that no longer exists is a skipped row | The plugin warns that the destination is gone (`missing_group_ids`). crosstalk restores the channel's NAME, leaves the group untouched, and says why in the row. |
| A restore back to "No Group" is planned and refused | Dispatcharr's channel serializer rejects a null `channel_group_id`, so a channel triage moved OUT of "No Group" cannot be moved back over REST. The row is planned, reported, and fails with that reason. The plugin never recorded such a channel at all (`_compute_capture_state` skips a channel with no group). The bridge closes this later. |
| A finite container duration is the only placeholder fingerprint | The plugin's `placeholder_file_detection` needs ffprobe to report a container duration, and a live MPEG-TS never does — so the fingerprint only fires on a container that carries one (HLS, MP4), which is what its measured placeholder was. The evidence is recorded for every stream either way. |
| `streams.match_preview` drops the plugin's threshold-analysis advice rows | Preview also re-ran the pipeline at 5-point thresholds and collected by exact match against the winning stream name; the plugin itself notes that path disagrees with the real matcher. Those rows were advice, never a write. |
| With `filter_dead_streams` on, match also excludes store probe verdicts | The plugin only reads `stream_stats` (empty, 0 width/height, `0x0`); crosstalk reproduces that test exactly and additionally skips streams the probe recorded as `dead` in the store — the same verdict, kept where a rerun can see it. |
| EPG placeholder titles come from one batched read | The plugin queries the programme table per EPG record and memoizes per pass (~8600 queries in one pass before the cache existed). crosstalk resolves the record ids a run can actually reach — a placeholder name on a channel, a placeholder or watched name on a stream — and reads them in one chunked `POST /epg/current-programs/`. The cleanup rules, skip titles, "Next Event" gate and watch containment are the plugin's own. |
| The `tvg_id` join lists every EPG entry | A stream carries only the guide identifier, so joining it to a record id lists `GET /epg/epgdata/` (`ceil(n / 1000)` requests — ~55 at 55k entries). Not narrowed: `EPGData` has no `tvg_id` filter, and the programme search is not equivalent (multi-source `tvg_id`s, no `parsing` marker, no dummy/index fallback). |
| The "Next Event" day is Dispatcharr's date | The plugin reads General Settings -> Time Zone in-process. Over REST the same value is read once per run from `GET /api/core/settings/` (`system_settings.value.time_zone`), so the "at TIME on DATE" clause is compared against today in Dispatcharr's zone on both sides. When that read fails the run degrades to the host's date and says so in the report, rather than refusing the match. |
| A guide that cannot be read leaves names as written | The plugin's lookup failures were per-record and swallowed; over REST the read is one request, so a failure is logged and reported in the CSV preamble and the run proceeds with the raw names. Refusing the match over one auxiliary read would be worse than the plugin. |
| The read-only tools write a CSV, not a text readout | The plugin wrote a `.txt` into its own readout directory and put a clipped headline in a toast. crosstalk writes `<app>_<tool>_<timestamp>.csv` into `[exports]` under the same retention rule as every other app, and the headline is the run summary. |
| `streams.match_apply`'s append mode renumbers the channel's list | The plugin's append gives the new rows order values that collide with the existing rows; crosstalk writes the same membership in the same playback order, numbered from zero. |
| `guide.match_apply` skips a channel whose guide was set by hand | A channel whose guide was set by hand in Dispatcharr keeps it; the plugin re-matches and overwrites. Skipping is safer, and the CSV reason says so. |
| AI may promote inside the verify band | With the AI gate on, an AI-accepted candidate can be assigned while its heuristic score (60–94) sits below `match_confidence_threshold` (default 95). The threshold still gates every heuristic assignment: the model never re-checks or vetoes one, and heal never asks it at all. |
| Heal reports a below-threshold replacement without applying it | The row keeps the plugin's `SKIPPED_LOW_CONFIDENCE` / `REPLACEMENT_PREVIEW` wording and the real score, so a preview shows what was nearly a match; the write still needs the threshold. |
| Candidates with no EPG source are never replacements | Heal validates by `tvg_id` and source, so an entry without a source cannot be checked; the plugin validates by EPG id alone and can accept one. |
| Match reports only walk candidates at or above the threshold | When nothing is applied, the row still carries the top-ranked candidate's score and method: “below threshold” when no candidate qualified for a programme check, “carries no programme data” when a checked one was empty. The plugin checks every scored candidate and reports the first programme-carrying one whatever its score, so the same channel can show a lower score with programmes behind it. |
| Programme data that cannot be read is “unknown”, never “missing” | The plugin's check is one SQL query and cannot fail per channel; over REST one can. A channel the scan could not check is left out of the result set (counted as `unchecked` in the summary and the CSV), so no cleanup can tag or clear it. |
| Cleanup selections come from the run store, not a results file | `scan` stores its result set as run rows: queryable, kept with its CSV, and not overwritten by an unrelated run. There is no age limit for the same reason, and no `export_results` action — every run already writes its CSV. |
| Every selection is intersected with `[scope]` again at apply time | The plugin applied the ids its scan had stored, so a settings change in between could reach a channel outside the scope. An id that is gone or out of scope is reported as skipped. |
| A channel whose guide changed since the scan is skipped | The plan re-reads every channel, so an old scan can only be out of date — never double-tagged, and never clearing a guide that was reassigned. |
| `clear_hidden` only unassigns | The plugin also deleted each entry's `ProgramData` rows, which blanks the guide of every *visible* channel sharing that entry. crosstalk never deletes programme data. |
| `clear_hidden` keeps `[scope] groups` / `ignore_groups` | The plugin selected every disabled membership of the profile whatever its group. crosstalk still bounds it by the groups every other action is held to; `[scope] profiles` is the profile to look in, so it is not also applied as a visible-channels filter. |
| `clear_groups` refuses an empty `[scope] groups` | The plugin treats the same empty filter as “every channel”, which makes one press unassign every guide in the install. |
| Clearing a guide skips a channel with an EPG override | Clearing the channel's own column would not change what is shown, which is the same reason match skips one. |
| `clear_by_regex` accepts Rust `regex`, not Python `re` | Backreferences and lookaround are refused with an explanation rather than silently matching something else, and the pattern is matched case-sensitively and unanchored as in the plugin. |
| `Clear exports` is a host command and a button, not an action | One retention rule over every app's `<app>_*.csv` replaces each plugin's `clear_csv_exports`. Only crosstalk's own files are touched, so a directory shared with the plugins keeps theirs. |
| The watchdog is armed by a schedule | `watchdog_enabled` and `watchdog_check_interval_hours` are gone: the host owns schedules, so `"guide.watchdog_apply" = "0 */6 * * *"` is the interval — and a schedule naming `watchdog_preview` stays a preview. |
| The watchdog is a preview/apply pair, and the apply is **orange** | The plugin's single button audited and refreshed in one press, and was painted green although it rewrites guide data. Here the audit is a read, the refresh confirms, and neither touches a channel. |
| A queued refresh is waited for, with a timeout | The plugin's refresh ran inline, so it had nothing to wait for. crosstalk polls until the source settles and reports a wait that lapsed as “did not settle” rather than as a failed refresh. |
| The guide horizon costs requests, not one SQL aggregate | Over REST there is no “latest programme per source” route, so staleness is one probe per source and the exact horizon about 28 indexed counts — paid only by a source the run is about to refresh. |
| “Has mapped channels” comes from the channel list | channel → its own EPG entry → that entry's source is exactly the plugin's ORM query, and reusing the world the run already loaded costs no extra request. |
| A recovery is a log line and a run row | crosstalk has no Dispatcharr System Events. `watchdog_log_on_recovery` keeps its meaning, every source gets a row either way, and a re-read that fails counts as “no improvement” where the plugin distinguished a vanished source from a database error. |
| The watchdog wants an administrator's API key | The programme search only returns rows on guide data a channel the key's user can reach is mapped to, so a lesser key reads sources short or empty — and its refresh POSTs answer 403. The run warns when the key is not an administrator's; the plugin, inside Django, had no key at all. |
| CSV cells that look like formulas are quoted | A cell starting with `=`, `+`, `-` or `@` gets a leading single quote, so such names differ from the plugin's CSV. It is a spreadsheet display instruction: a text editor shows the quote as part of the value. |
| The plugin's action ids and its usage counter are not carried over | `janitor` became `guide` and `automatch_*` became `match_*`, and the plugin's own ids appear in no schedule, URL or CSV. Anonymous usage reporting is not ported: a run is recorded in crosstalk's own store and sent nowhere. |
| The streams plugins' actions crosstalk does not carry | IPTV Checker's `delete_dead_channels` (deletes are out of scope), `view_*` / `export_results` (the run page and its CSV), `cancel_check` (the run page's cancel) and both plugins' notification, report and bug actions are not actions. Neither is their scheduler housekeeping (`update_schedule`, `cleanup_*`, `check_scheduler_status`, `reset_progress`, `clear_operation_lock`): schedules, windows and run-after-a-refresh triggers are `[schedules]`, `[workflows]` and `[hooks]`, CSV retention is `[exports]`, and the operation lock and progress files are the run store. Stream-Mapparr's usage counting is not ported, and a retired key in `streams.toml` is refused with the pointer to where its meaning went. |

## Development

```bash
cargo test --workspace          # unit, route and mocked-Dispatcharr tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all

./scripts/sim_parity.sh --quick # the oracle: Rust vs the Python tool, byte for byte
python3 scripts/parity_sweep.py --kinds normalize,callsign,location,network --limit 300
python3 scripts/parity_sweep.py --kinds match,score --limit 300 --pool 60
python3 scripts/parity_sweep_sm.py --limit 300   # the StreamMapparr matcher profile, vs its own code
python3 -I scripts/channel_ops_parity.py   # the cleanup selections and payloads
python3 -I scripts/sort_parity.py          # the streams quality model, 840 cases
python3 -I scripts/match_parity.py         # the streams collector, EPG placeholder fixtures
python3 -I scripts/mirror_parity.py        # Fill Mirror Backups, against the plugin's own action
python3 -I scripts/visibility_parity.py    # Manage Channel Visibility, likewise
python3 -I scripts/probe_parity.py         # probe: commands, parsing and classification vs IC
python3 -I scripts/tools_parity.py         # check_countries, scan_placeholders, test_rules
python3 -I scripts/triage_parity.py        # the seven post-actions and Restore Recovered Channels

# The scale numbers in the README, and the bridge decision: time the plans on this machine.
cargo run --release -p crosstalk -- bench streams --synthetic
cargo run --release -p crosstalk -- bench streams --live --write-timing --channel-ids <ids> --yes

./scripts/upstream_drift.sh     # what each reference changed since its pin, in files we port
```

The Python plugins live in `reference/` as pinned git submodules — EPG Janitor (the guide
app), Stream-Mapparr and IPTV Checker (the streams app, from phase 2 on) — so the parity
gates always compare against those exact commits. A fresh clone needs
`git submodule update --init` to get them. See [How matching works](#how-matching-works) for
what is strict-parity and what is deliberately Rust-native, and `scripts/upstream_drift.sh`
— which also runs on the weekly CI schedule — for when upstream has moved past a pin; it
reports each reference separately, against that reference's own list of ported files
(`THIRD_PARTY_NOTICES.md`).

### Layout

```
crates/crosstalk-matching   pure, IO-free matcher: normalisation, similarity, callsigns, pipeline
crates/crosstalk-core       host: config, sub-app trait, Dispatcharr client, AI, store, runner, CSV
crates/guide                the guide sub-app: scope, matching, heal, scan, cleanup presets, watchdog
crates/streams              the streams sub-app: stream selection, settings, match, sort, mirror,
                            probe, triage, restore, visibility, and the bench behind the numbers
                            in Scale
crates/crosstalk-web        axum + Askama UI and JSON API
crates/crosstalk            the binary: CLI, registry, daemon
data/                       embedded channel databases, FCC callsigns, aliases
reference/                  the pinned upstream plugins the parity gates compare against
scripts/                    the parity gates and the upstream-drift report
```

A new sub-app is a crate implementing `SubApp` plus one line in `crates/crosstalk/src/registry.rs`.

## Licence

crosstalk is MIT; see [LICENSE](LICENSE). The code and data ported from the Dispatcharr
plugins (EPG Janitor, Stream-Mapparr and IPTV Checker) are credited, with each upstream
licence text and pinned reference commit, in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) — both files ship in the release tarball and
in the container image, under `/usr/share/doc/crosstalk/`.
