# crosstalk

A Rust daemon that hosts tools for [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr).
One binary, one config directory, one scheduler and one web UI serve several sub-apps, each
defined by what it manages rather than by the plugin it replaces. Three ship today:

- **guide** — a fast, multi-threaded EPG matcher that assigns guides to channels, repairs or
  removes assignments that carry no programme data, and keeps the EPG sources themselves fresh.
- **streams** — which streams a channel has, the order they play in, and whether they work:
  match & assign, sort alternates, fill mirror backups, probe, triage and restore.
- **lineup** — the channel list itself: names, numbers, groups, logos, creation (clean, names,
  categorize, logos, create, sync and tidy preview/apply pairs plus `test_name`), with
  Lineuparr's stream half as a lineup mode of `streams.match` and its EPG half as `guide.match`
  over the lineup scope.

They replace six Python plugins — EPG Janitor, Stream-Mapparr, IPTV Checker,
Channel-Mapparr, Channel Name Cleaner and Lineuparr (Lineuparr's own matcher is not ported:
its stream half is a lineup mode of `streams.match`, its EPG half is `guide.match`) — which run
inside Dispatcharr and read its database through the Django ORM. crosstalk is a separate process
that talks to Dispatcharr over its REST API, which means it survives Dispatcharr upgrades, runs
its matching across every core, and can serve several tools from one binary.

One matching engine serves every app: a channel name resolves to one shared channel identity
(canonical name, callsign, network, country, category, region/zone, quality tier), computed once
per run and cached in the store, and every app matches on identities — guide assigns the
family's first entry carrying programme data, streams attaches the whole family, lineup writes
the family's canonical name. The same name gives the same identity and the same family in every
app. Plugin behaviour is an oracle used to choose and validate the unified rules, not a target:
every deliberate difference is listed under [Differences](#differences-from-the-plugins) with its
reason, and the per-migration measurements are in `docs/oracle-reports/`.

## What it does

- **Auto-match** scores every in-scope channel against every eligible guide entry with the
  one unified matching engine (one ranker to the best identity family at the one `[matching]`
  cut) and assigns the family's first entry carrying programme data, with guards that keep
  numbered siblings, `+1` time-shifts and East/West feeds apart.
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
  the one unified matching engine (one ranker to the best identity family per channel group at
  the one `[matching]` cut; the whole family attaches), then same-country and feed-zone rules
  and the quality sort, written as a replace or an append with custom streams kept last. Only
  the channels whose list actually changes are written.
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
- **Clean names** (`lineup.clean_*`) runs Channel Name Cleaner's fixed rule order over every
  in-scope channel — pipe and bracket junk first, then the riskier normalizations behind their
  switches — with the shared collision resolver so two channels never land on one name.
- **Canonical names** (`lineup.names_*`) resolves every in-scope channel through the same engine
  guide and streams run on: OTA stations take the `{NETWORK} - {STATE} {CITY} ({CALLSIGN})`
  template, premium channels take the best identity family's database leader plus its tags, and
  only truly unmatched channels are tagged ` [Unk]` (never twice). An optional AI gate re-ranks
  borderline renames; every rename is undoable.
- **Categorize** (`lineup.categorize_*`) moves each channel into the group named by its database
  category (creating missing groups, never re-moving what is already correct); **logos**
  (`lineup.logos_*`) assigns each channel one logo through the unified resolver (EPG icon → Logo
  Manager name → tv-logos → network → default), creating missing logos first.
- **Create** (`lineup.create_*`) makes channels from streams (one channel per identity family,
  carrying its streams) or from OTA stations (one channel per station, no streams attached),
  never duplicating a rerun; **sync** (`lineup.sync_*`) mirrors the configured lineup file's
  groups and channels onto owned channels only (stable numbers on re-sync, foreign same-name
  clashes reported and skipped); **tidy** (`lineup.tidy_*`) hides owned channels left with no
  streams and unhides exactly the ones tidy hid when streams return.
- **Try one name** (`lineup.test_name --input "..."`) cleans a single name and shows the fired
  rules, the protection hit and the unified identity every app will match it as — plus the
  identity after clean, and any field the rename moves.
- **Optionally asks an AI** about the cases the heuristics are unsure of — and never about the
  ones they are sure of. See [AI verification](#ai-verification).

## Roadmap

One more sub-app is planned, defined by what it manages:

- **events** (phase 4) — time-bound event channels: create on a schedule, retire afterwards.
  It takes Event Channel Managarr plus the sports half of Sports Editor; the bridge decision
  and live testing (a 20k-channel apply timed on the target host, and proving the run against
  the configured Dispatcharr before a library is touched) ride with it.

Each lands as its own phase of [`plans/plugin-consolidation.md`](plans/plugin-consolidation.md)
(`streams` was [phase 2](plans/phase-2-streams.md), `lineup` was
[phase 3](plans/phase-3-lineup.md)), which also maps every action of the remaining Dispatcharr
plugins onto those four apps and records what is deliberately not ported.
Nothing here promises a date.

## Quick start

```bash
# 1. Configure
cp config/crosstalk.example.toml config/crosstalk.toml
cp config/guide.example.toml config/guide.toml
cp config/streams.example.toml config/streams.toml
cp config/lineup.example.toml config/lineup.toml

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
cp config/lineup.example.toml config/lineup.toml

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

The compose file publishes the port on `127.0.0.1` only and runs the container with a
read-only root filesystem, a `/tmp` tmpfs, no Linux capabilities and
`no-new-privileges`; the daemon writes only `/config`. For access from other machines, put
a TLS reverse proxy in front of it rather than publishing `0.0.0.0:8730`.

The image carries **ffmpeg** (which includes ffprobe), because `streams.probe` runs the
real tools: IPTV Checker's stream check is two subprocesses per stream, not an HTTP
request. Alpine installs them in `/usr/bin`, which is what `streams.toml` defaults to
(`ffprobe_path`, `ffmpeg_path`); change those two settings in `streams.toml` (the web form does
not show them) to point at a Homebrew or `/usr/local/bin` install. Measured on arm64 (`docker build`, then `docker images` and
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

### Releases: how they are built and how to verify them

A `v*` tag runs CI (`ci.yml`) and the build (`build.yml`) on the same commit. Nothing is
published until that CI run has passed: the build's `ci-gate` job waits up to 60 minutes
for it, and the image and the GitHub Release both depend on it. Release binaries and the
release image are built without any shared build cache (pull requests use a separate one),
and the tag must equal the workspace version (`cargo metadata`).

Every image pushed to GHCR carries an SBOM and SLSA provenance (`mode=max`), and the image
digest and each release tarball carry a signed GitHub build-provenance attestation. Verify
with the GitHub CLI before you run anything:

```bash
# A release tarball (download it and SHA256SUMS from the release first)
sha256sum -c SHA256SUMS --ignore-missing
gh attestation verify crosstalk-v0.2.0-linux-amd64.tar.gz --repo darkspadez/crosstalk

# The image (by tag or, better, by digest)
gh attestation verify oci://ghcr.io/darkspadez/crosstalk:0.2.0 --repo darkspadez/crosstalk
docker buildx imagetools inspect ghcr.io/darkspadez/crosstalk:0.2.0 \
  --format '{{ json .SBOM }}'   # the SPDX SBOM BuildKit attached
```

A successful `gh attestation verify` proves the artifact was built by this repository's
`build.yml` from the tagged commit. Releases before these attestations existed (v0.1.0)
cannot be verified this way. Each tarball and the image (`/usr/share/doc/crosstalk/`) also
carry `THIRD_PARTY_LICENSES`, the licence text of every Rust crate compiled in.

## Commands

| Command | What it does |
|---|---|
| `crosstalk serve --config <dir>` | Run the daemon: REST API, scheduler and web UI |
| `crosstalk validate --config <dir>` | Check the configuration files and each app's settings |
| `crosstalk run <app> <action> [--dry-run] [--yes] [--fresh] [--input "<text>"]` | Run one action once and exit (`streams probe --dry-run` measures everything and writes no stream stats; `--input` is only for actions that declare one, today `lineup test_name`) |
| `crosstalk run workflow <name> [--dry-run] [--yes] [--fresh]` | Run one workflow once and exit (`run workflow.<name>` works too); `--dry-run` runs every writing step as its preview |
| `crosstalk undo <run-id> [--dry-run] [--yes] [--config <dir>]` | Reverse a finished apply run's rows that are still in effect (`--dry-run` previews; the apply needs `--yes`; the undo is itself a recorded, undoable run). Refuses previews, reads, running/paused runs, workflow runs (it lists their step runs to undo one at a time, newest first) and runs that recorded nothing undoable. Created channels end hidden; created groups/logos are left and listed |
| `crosstalk exports clear [--app <id>] [--yes]` | Delete the CSV reports the sub-apps wrote (the UI's **Clear exports** button does the same) |
| `crosstalk bridge secret [--force] [--config <dir>]` | Generate the [bridge plugin](#bridge-plugin)'s shared secret, write it to `crosstalk.toml` `[bridge] secret` (0600) and print it once. Refuses when one is set (`--force` overwrites) or when `[bridge] secret_env` supplies it |
| `crosstalk bridge rotate [--config <dir>]` | Replace the bridge secret and print the order to roll it into the plugin without breaking it |
| `crosstalk ownership forget-deleted --app <id> [--config <dir>]` | Forget the objects an app created that Dispatcharr no longer has, however many. A run refuses to forget more than max(25, 5%) of what the app owns at once (a short listing, or a run against the wrong Dispatcharr, must never make the next create duplicate an install); after a deliberate mass deletion in Dispatcharr that refusal names this command. Each missing id is confirmed with its own GET and only a 404 forgets it; an empty listing is still refused |
| `crosstalk bridge ping [--config <dir>]` | Send the bridge plugin a signed ping (even while `[bridge] enabled = false`) and say whether this crosstalk can use it |
| `crosstalk bench streams --synthetic [--config <dir>] [--channels N --streams N --accounts N]` | Time the streams plans on a world built in memory: no network, no Dispatcharr. See [Scale](#scale) |
| `crosstalk bench streams --live [--config <dir>] [--write-timing --channel-ids <ids> --yes]` | Time the same plans against the configured Dispatcharr, read-only unless `--write-timing` is asked for twice |
| `crosstalk bench lineup --synthetic [--config <dir>] [--channels N --lineup-channels N --streams N --tv-logos-per-country N] [--json]` | Time the lineup plans (clean, names, categorize, logos, sync, create, tidy, undo) on a world built in memory, with a file-backed identity cache. See [Scale](#scale) |
| `crosstalk bench guide --synthetic [--config <dir>] [--channels N --entries N --sources N --delay-ms N] [--json]` | Time the guide plans (match, scan, heal) against an in-process fake Dispatcharr and count every request. See [Scale](#scale) |
| `crosstalk simulate-streams --channels <csv> --streams <m3u> [--threshold 95]` | Offline stream-to-channel simulation on the unified engine (what guide and streams would take: a stream passes as a member of the channel's best identity family at the cut), for tuning the `[matching]` cut without touching a live install. `families.csv` lists every channel whose family takes each stream (two spellings of one identity both do); `best_channel` is the first in channel order |

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
  - `[matching]` — `sensitivity` (the ONE decision cut every app matches at: `strict`/`high`/
    `medium`/`low` = 95/90/80/70, or a bare number), the normalisation toggles, which country
    databases to load, and custom aliases. Two apps disagreeing about whether `[HD]` is
    stripped would give two different answers for the same channel.
  - `[scope]` — `profiles`, `groups` and `ignore_groups`: which channels a run may touch.
    "Which channels am I responsible for" is the same question for every sub-app.
  - `[exports]` — `retention_days`, how long CSV reports are kept.
  - `[ai.verify]` — when a borderline match is worth asking a model about, and how its answer
    is read: `provider`/`model` as a one-link shorthand, or `chain = [{provider, model}, ...]`
    to escalate undecided questions from the Jev decision model to an LLM fallback. The
    endpoints and keys stay under `[ai.providers.<name>]` in the same file.
- **`guide.toml`** (sub-app): what is genuinely the guide's own — the EPG sources to match,
  the check window, the heal fallback sources, the cleanup suffix and REGEX, and the watchdog's
  threshold, timeout, exclude list and recovery logging. The old per-action thresholds are
  retired: a file still naming `match_confidence_threshold` / `heal_confidence_threshold` fails
  at load pointing at `[matching] sensitivity`.
- **`streams.toml`** (sub-app): the stream half of the scope — M3U accounts in priority order,
  stream groups, the stream-name regex rules — then matching, sorting, probing and triage,
  including the seven triage switches (all off by default: an unattended rename is a choice an
  operator makes, not one a fresh install makes for them), plus `lineup_mode_country_restriction`
  (default on) for `streams.match` in lineup mode.
  Still shared, so not repeated there: Stream-Mapparr's tag handling is `[matching]`, its
  channel profile and groups are `[scope]` (with its exclude mode being `[scope]
  ignore_groups`), and both plugins' schedules, windows and "run after a refresh" triggers
  are `[schedules]`, `[workflows]` and `[hooks]`.
- **`lineup.toml`** (sub-app): what the lineup app decides for itself — the clean rule switches,
  the OTA template and unknown/tag switches, `category_groups`, the logo sources and default,
  the create modes and numbering, and which of the 29 vendored lineup files to mirror
  (see [`config/lineup.example.toml`](config/lineup.example.toml)). The decision cut is the host's
  `[matching] sensitivity`; every moved key (`epg_sources`, `m3u_sources`, `single_channel_name`,
  `preserve_existing_streams`, `rate_limiting`, schedules, `test_name`, CSV retention, usage
  counters) fails at load naming its new home.

The rest of `crosstalk.toml` is `[server]` (`bind`, `port`, `token`/`token_env`,
`trusted_proxies`, `public_url`, `secure_cookies`, `hsts`; see
[Running behind a reverse proxy](#running-behind-a-reverse-proxy)), `[dispatcharr]`
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
# The escalation chain arrives as JSON: the file syntax has no list-of-tables form.
CROSSTALK_AI__VERIFY__CHAIN='[{"provider":"typesafe","model":"jev-latest"},{"provider":"llm","model":"kimi-k3"}]'
```

Secrets should live in the environment, not the file: the daemon refuses to save a
configuration that had environment overrides applied, so a token cannot leak into a file by
accident.

### Secrets and data at rest

- **Precedence.** A secret can be written inline (`token`, `api_key`, `password`) or named
  through its `*_env` variable (`token_env = "CROSSTALK_TOKEN"`, the default). When both are
  set the **environment wins**, so a token rotates without editing the file; `validate` warns
  that the file's value is shadowed. A blank variable falls back to the file. The
  `CROSSTALK_<SECTION>__<KEY>` overrides of `token`, `api_key`, `password`, `secret`,
  `username`, `base_url` and `public_url` are taken verbatim: `CROSSTALK_SERVER__TOKEN=0123456`
  stays `0123456`. The bridge secret works the same way (`[bridge] secret` /
  `secret_env = "CROSSTALK_BRIDGE_SECRET"`); the settings page shows only whether it is set and
  where from, and never changes it.
- **`base_url` credentials.** A `user:pass@` in `dispatcharr.base_url` (for a proxy in front of
  Dispatcharr) is sent as HTTP basic auth and never printed: `validate`, errors and logs show
  the URL without it.
- **File permissions.** The run store (`<data_dir>/crosstalk.db` and SQLite's `-wal`/`-shm`)
  is made `0600` and its directory `0700` whenever it is opened; CSV reports are written
  `0600` into a `0700` exports directory; a settings save writes `0600`. `validate` and
  `serve` warn when `crosstalk.toml` or an app's `.toml` is readable by other accounts AND
  holds an inline secret (`chmod 600` it, or move the secret to the environment). Unix only.
- **What is stored.** Provider stream URLs are credentials (`/live/<user>/<pass>/…`,
  `?token=`), so none is stored: run rows, `stream_health`, run summaries, CSV reports and
  logs keep the stream id and at most `scheme://host[:port]` of any URL a tool or Dispatcharr
  printed (`[Stream ID: 101]: Server returned 403 Forbidden`). Dispatcharr error bodies quoted
  in errors have their `url`/`password`/`username`/`token`/`api_key` values blanked. A logo
  URL keeps its path in reports but loses its userinfo and query; the exact URL lives only in
  the create intent that needs it to find the logo again.
- **AI requests** carry names, numbers, callsigns, places, scores and ids: never a URL, an
  account name or a key (tested per gate, and the audit trail in `ai_decisions` is the
  request as sent).
- **Logs.** `reqwest`, `hyper` and `hyper_util` stay at `info` even under `RUST_LOG=debug`,
  because at `debug` they log request and redirect URLs; name one in `RUST_LOG` to opt in.
  Configuration sections print secrets as `<redacted>` in debug output.

The web UI writes settings back through `toml_edit`, preserving your comments and ordering.
Host settings saved in the UI hot-reload: the next run uses them and schedules re-arm, with
no daemon restart (a run already in progress finishes with the settings it started with).

## Bridge plugin

`crosstalk-bridge` is a small Dispatcharr plugin, shipped as `crosstalk-bridge-<version>.zip`
with every release, for the four writes Dispatcharr's REST API cannot do well or at all:

| Write | Without the bridge | With it |
|---|---|---|
| Channels' ordered stream lists (`streams` match, sort and mirror applies, and their undo) | `edit/bulk`, one PATCH per channel for a rejected chunk | `ChannelStream` rows rewritten in one transaction, 2,000 channels per request |
| `stream_stats` after a probe | one PATCH per changed stream | 5,000 streams per request |
| Moving a channel to "No Group" (triage restore, undo of a group move) | impossible: the channel serializer refuses a null group, so the row is refused with a reason | one `UPDATE` |
| Guide entries and programmes for generated guides (phase 4) | no endpoint | upsert by `(tvg_id, source)`, programmes replaced inside one window |

It is **off by default** and nothing changes until it is on: everything works over REST, and
the "No Group" rows say they need the bridge.

**Install.** Download the zip and `SHA256SUMS` from the release (and
`gh attestation verify crosstalk-bridge-<version>.zip --repo darkspadez/crosstalk` if you like),
import it in Dispatcharr (**Plugins → Import**; it installs as `crosstalk_bridge`), run
`crosstalk bridge secret`, give the printed secret to the plugin, enable the plugin, set
`[bridge] enabled = true`, and check with `crosstalk bridge ping`. The plugin's own
[README](bridge/README.md) walks through it.

**The secret.** Every request goes through Dispatcharr's plugin-run API with the Dispatcharr key
crosstalk already uses, and its body is signed: HMAC-SHA256 over a canonical JSON envelope
(operation, payload, timestamp, nonce) with a secret only crosstalk and the plugin hold. The
plugin refuses anything unsigned, older than 5 minutes, replayed, or not one of its five
operations (`ping` and the four writes). The secret is generated by crosstalk from the OS
CSPRNG, stored in `crosstalk.toml` (0600) or its environment variable, never shown in the UI and
never logged. On the Dispatcharr side, prefer `CROSSTALK_BRIDGE_SECRET` in Dispatcharr's
environment to the plugin's Secret field: a setting is stored in Dispatcharr's database and
returned by `GET /api/plugins/plugins/` to anyone with plugin access, the environment is not.

**Rotation.** `crosstalk bridge rotate` writes a new secret and prints the order: the old secret
goes into the plugin's Previous secret with a grace time, the new one into Secret, crosstalk
restarts, then the previous one is cleared. Both are accepted only until the grace time.

**At start-up** `serve` (and `run`, `undo`) pings the plugin once. Only a plugin that verifies
the signature, speaks protocol 1, is at least `[bridge] min_version` and offers all four writes
is used; otherwise the log says why and the run falls back to REST where REST can do the write.
`validate` pings too and prints the verdict.

**What it can never do.** Read data, delete a channel, stream, group, EPG source or guide entry,
change names, numbers, logos, EPG assignments, users or settings, or run anything else: the five
operations are an allow-list in the plugin's code, every write is idempotent, and guide writes
only reach the EPG sources the operator lists in the plugin. A leaked bridge secret is limited to
those writes; a leaked Dispatcharr admin key was already the bigger problem (it can read a
secret stored in plugin settings, and install plugins of its own). The full contract, canonical
JSON, limits and threat model: [docs/bridge.md](docs/bridge.md).

## Actions

Every action is named `<app>.<action>`, and every action that writes channel data is a
`<verb>_preview` / `<verb>_apply` pair: `--dry-run` on the apply runs the preview, and a
schedule key names the half you mean. Button colour is the consequence, the same convention the
Python plugins used: **blue** reads, **orange** writes data but removes no assignment, **red**
can move an assignment. Orange and red always confirm.

`validate` and `status` exist in every app. `validate` is local — it has to work on a host that
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
| `validate` | blue | Check `streams.toml` for consistency, without touching Dispatcharr. Reports the one `[matching]` cut and how many regex rules are usable |
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
| `visibility_apply` | **red** | Leave exactly one channel per name group enabled in the profile and disable every other in-scope channel. A group where no channel has a stream goes dark, and a winner not already enabled in the profile is left as it is (never enabled over REST) |
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

### lineup

`crosstalk run lineup <action>`: the channel list itself — Channel-Mapparr's names, categories,
logos and creation, Channel Name Cleaner's cleanup, Lineuparr's sync and post-match tidy — as
seven preview/apply pairs plus `test_name`. `crates/lineup/src/lib.rs` declares them, and the
table below is in that order. `streams.match` in lineup mode and `guide.match` over the lineup
scope are Lineuparr's other two halves; they live in their own apps and are listed here so the
mapping is complete.

| Action | Colour | What it does |
|---|---|---|
| `validate` | blue | Check `lineup.toml` and the configured lineup file for consistency, without touching Dispatcharr |
| `status` | blue | Show what the last run did, and what is running now. Stays clickable while a run is live |
| `clean_preview` | blue | Compute before → after for every in-scope channel under Name Cleaner's fixed rule order and write a CSV. Changes nothing |
| `clean_apply` | **orange** | Rename the channels the preview would change (collisions handled per On Collision). Undoable through the host undo |
| `test_name` | blue | Run the clean rules plus the unified identity against one name from `--input`. Local, changes nothing; refuses runs without an input |
| `names_preview` | blue | Compute the canonical name for every in-scope channel (OTA template, premium canonical + tags, unknown suffix) and write a CSV. Changes nothing |
| `names_apply` | **orange** | Rename the channels the preview would change (live plans, never a saved replay; collisions guarded). Undoable through the host undo |
| `categorize_preview` | blue | Compute the category group each in-scope channel moves to (creating missing groups) and write a CSV. Changes nothing |
| `categorize_apply` | **orange** | Create the missing category groups and move the channels the preview would move. Never re-moves what is already correct. Undoable through the host undo |
| `logos_preview` | blue | Compute the logo each in-scope channel takes (EPG icon, Logo Manager, tv-logos, network, default) and write a CSV. Changes nothing |
| `logos_apply` | **orange** | Create the missing logos and assign the logos the preview would assign. Undoable through the host undo |
| `create_preview` | blue | Compute the channels (and groups) the streams or stations mode would create and write a CSV. Changes nothing |
| `create_apply` | **orange** | Create the channels (and groups) the preview would create: one channel per identity family (streams mode) or per OTA station (stations mode). Created channels hide on undo; created groups/logos are left and listed |
| `sync_preview` | blue | Compute the groups, channels, renumbers and moves the configured lineup file implies and write a CSV. Changes nothing |
| `sync_apply` | **orange** | Create the missing lineup groups and channels, then renumber and move the owned channels that drifted. Owned channels only; renames never (names owns display names). Undoable through the host undo |
| `tidy_preview` | blue | Compute which owned lineup channels would hide (no streams) or unhide (streams again after tidy hid them) and write a CSV. Changes nothing |
| `tidy_apply` | **red** | Hide the owned lineup channels left with no streams; unhide only the ones tidy itself hid. Owned channels only. Undoable through the host undo |
| `streams.match_apply` with `scope = "lineup"` | **red** | Lineuparr's stream half: match streams to the owned channels of the configured lineup file, restricted to the lineup file's country by default (off with `lineup_mode_country_restriction = false`). One identity family attaches per channel at the one cut |
| `guide.match_apply` with `scope = "lineup"` | **red** | Lineuparr's EPG half: assign guides to the owned channels of the configured lineup file |

Host features every app shares: `crosstalk undo <run-id>` (plus an **Undo this run** button on
a finished apply's run page) previews then reverses a run's still-in-effect rows — `Set` fields
back to their before values, unhide what tidy hid, hide what create made — and refuses previews,
reads, running/paused runs, and runs that recorded nothing undoable; [workflows](#workflows) with
per-step `scope` (including the named `lineup` scope: the channels lineup owns for its configured
file) and `continue_on_failure`, of which `full_sync` (`lineup.sync_apply` → `streams.match_apply`
→ `guide.match_apply` → `lineup.logos_apply` → `lineup.tidy_apply`) is the shipped example;
the [Dispatcharr webhook](#dispatcharr-events) (`m3u_refresh`, `epg_refresh`);
[windows](#windows-and-resumable-jobs) with pause and resume; and `--input` for `test_name`
(the web action button carries the same field).

## Probing streams

`streams.probe` is the only action that opens a connection to a provider, and the only one that
can run for hours. What it does, and what it costs:

- **ffprobe, then optionally ffmpeg.** IPTV Checker's stream check is two subprocesses per
  stream, not an HTTP request. `ffprobe` reads the container and stream facts (codec,
  resolution, frame rate, bitrates); with `black_screen_detection`, `frozen_video_detection` or
  `silent_audio_detection` on, one `ffmpeg` pass asks for a black, frozen or silent picture.
  Both binaries ship in the image; `ffprobe_path` and `ffmpeg_path` default to `/usr/bin`, are
  set in `streams.toml` only, must be absolute and named for the tool, and a tool that is not
  executable or is world-writable is refused like a missing one. A missing tool REFUSES the run instead of marking every stream dead (which is what the plugin
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
`--synthetic` at the defaults), re-taken after phase 3 (unified engine in every plan):

```
synthetic world: 20,000 channel(s), 50,000 stream(s), 100 account(s)
  build the world                0.026s
  match       setup    0.115s  plan    1.225s  write plan    0.013s    4,990 write(s)    10 request(s)  20,000 channel(s) in 5,000 name group(s), 4,990 matched at least one stream, 50,000 stream(s) in the pool
  sort        setup         -  plan    0.358s  write plan    0.023s   20,000 write(s)    40 request(s)  20,000 channel(s), 20,000 with more than one stream, 0 already in order, 7,220 stream(s) dropped as the other zone
  mirror      setup         -  plan    0.116s  write plan    0.030s   20,000 write(s)    40 request(s)  20,000 channel(s), 20,000 would gain a mirror, 200,000 mirror stream(s), 0 left alone for a dangling id
  visibility  setup    0.004s  plan    0.195s  write plan    0.024s   15,000 write(s)    30 request(s)  profile "All": 20,000 channel(s) in 5,000 name group(s), 5,000 winner(s), 20,000 enabled before
```

So, for a 20k-channel / 50k-stream library:

| What | Cost | Note |
|---|---|---|
| `match_preview` / `match_apply` plan | **≈ 1.2 s** | Five thousand name groups, 50k streams in the pool, on every core. `visible_channel_limit` (default 1) decides how many channels of a group are written, which is why this world writes ~5,000 lists rather than 20,000 |
| `sort_preview` / `sort_apply` plan | **≈ 0.36 s** | 20k channels × 10 streams = 200k references. The synthetic families carry zone suffixes, so this exercises zone routing (the name normaliser over 20k channel names); a zone-free world plans the same lists in ~0.1 s, which is what the step-5 scale test measures |
| `mirror_preview` / `mirror_apply` plan | **≈ 0.12 s** | One pass over the accounts and streams, one per channel |
| `visibility_preview` / `visibility_apply` plan | **≈ 0.2 s** | Name grouping is the identity family key, without the stream collection behind it |
| write requests for 20k changed channels | **40** | `edit/bulk` carries 500 channels per request, so a whole-library apply costs tens of requests, not 20,000. A rejected chunk costs one PATCH per channel in it (see [Differences](#differences-from-the-plugins)) |

The lineup plans have their own bench (`crosstalk bench lineup --synthetic`, phase 3 review):
20,000 channels named from the merged US database in the shapes providers write them (`US: X
HD`, `X [FHD]`, `X | HD |`, `HD X`, every tenth a station callsign) in 100 groups, plus the first
500 entries of the configured lineup file (`US_DirecTV-Premier` has 351) synced twice — once
onto an empty world, once onto the world that sync leaves. Same machine and build as above:

```
synthetic lineup world: 20,000 channel(s), 500 lineup entr(y/ies)
  build the world                0.022s
  engine + merged index          2.016s
  clean       setup         -  plan    0.030s  write plan    0.017s    9,004 write(s)    19 request(s)
  names       setup    2.016s  plan    7.747s  write plan    0.029s   17,143 write(s)    35 request(s)
  categorize  setup    2.016s  plan    2.479s  write plan    0.004s    1,105 write(s)     3 request(s)
  logos       setup    2.016s  plan    0.022s  write plan    0.003s        1 write(s)     1 request(s)
  sync (first run)             plan    0.001s  write plan    0.002s      361 write(s)   361 request(s)
  sync (rerun)                 plan    0.001s  write plan    0.000s        0 write(s)     0 request(s)
```

| What | Cost | Note |
|---|---|---|
| engine + merged index | **≈ 2 s** | Built once per run and shared by `names`, `categorize` and `logos`: the unified engine over the merged tables plus the identity index of the enabled databases' ~30k names |
| `clean_preview` / `clean_apply` plan | **≈ 0.03 s** | Name Cleaner's pipeline is string work; the collision resolver over 20k names is the write plan |
| `names_preview` / `names_apply` plan | **≈ 7.7 s** | One identity resolve and one family rank per channel against the merged index, on every core. Before the review this walk was serial and ranked each channel twice (≈ 31 s), and the fuzzy stage re-tokenised both sides of every pair (≈ 38 s parallel) |
| `categorize_preview` / `categorize_apply` plan | **≈ 2.5 s** | The same resolve and rank; the category map is one lookup per family leader |
| `logos_preview` / `logos_apply` plan | **≈ 0.02 s** | Without a tv-logos index (a synthetic bench never fetches) the EPG, Logo Manager and default tiers are lookups; the tv-logos fuzzy tier adds one similarity pass per candidate file per channel |
| `sync_preview` / `sync_apply` plan, 351 entries | **≈ 1 ms** | Decide + create plan with the global number check; the rerun over the synced world plans nothing. Creates are one POST each (361 requests for 16 groups and 345 channels), which `bench streams --live --write-timing` prices on your box |
| `create_preview` / `create_apply` plan, 50k streams | **≈ 1.0 s** | Streams mode over the streams bench's world: 20k live channel identities (one batch warm, then rayon), one decision per stream on rayon, the foreign-family check through a family-key index, the create plan with the collision and number checks. One POST per created object |
| `tidy_preview` / `tidy_apply` plan | **≈ 2 ms** | 10k owned channels judged, the tidy history read from the store (visibility rows only, indexed) |
| host `undo` plan of a names apply | **≈ 0.03 s** | The names plan recorded as a finished apply in the file store, planned back against the world it leaves |

The audit's performance pass (docs/audit/E-perf.md) re-took these numbers with the identity cache
on a FILE store (the batch warm pays real commits), a synthetic tv-logos tree of 1,500 files per
country (so the `logos` row now includes the fuzzy tier) and an SLA line: every plan, setup
included, under 10 s at 20k channels. `--json` prints the same report as one JSON document.

| What (audit re-take) | Cost | Note |
|---|---|---|
| engine + merged index | **≈ 0.4 s** (was ≈ 2 s) | Index views now built on rayon |
| `names_preview` / `names_apply` plan | **≈ 2.2 s** on a cold file store (was ≈ 7.7 s with no store) | Batch warm (one read pass, one write transaction) before the rayon stage |
| `categorize_preview` / `categorize_apply` plan | **≈ 0.9 s** (was ≈ 2.5 s) | Same warm, off the async runtime in the action |
| `logos_preview` / `logos_apply` plan, tv-logos tier | **≈ 0.44 s** | Filenames normalised once per directory, exact hits by hash, early-exit similarity at the 85 cut |
| SLA line (`bench lineup`) | **PASS, slowest names 2.7 s** | PASS/FAIL against 10 s |

The guide plans have their own bench (`crosstalk bench guide --synthetic`, audit E-S9): 20,000
channels and 55,000 EPG entries in 20 sources, served by an in-process fake Dispatcharr that
answers the programme search, the per-source horizon probe and `POST /epg/current-programs/`
after `--delay-ms` per request and counts every request by kind. One source in ten has nothing in
the window, one entry in seven carries no programmes, and most of the rest are airing now.

```bash
crosstalk bench guide --synthetic
crosstalk bench guide --synthetic --delay-ms 20 --json
```

| What | Cost | Requests | Note |
|---|---|---|---|
| `match_preview` / `match_apply` plan | **≈ 6.9 s** at 2 ms per request (scoring ≈ 2.9 s, walks ≈ 2.3 s) | 5,534 (20 probes, 722 batches, 4,790 searches) | Every family member a walk could ask about is settled first: one horizon probe per source (an empty source answers "no" for all its entries) and one airing-now batch per 50 entries; only the rest are searched one by one |
| `scan` plan | **≈ 1.6 s** | 3,251 (15 probes, 260 batches, 2,976 searches) | Same pre-seed over the assigned entries |
| `heal_preview` / `heal_apply` plan | **≈ 3.1 s** | 3,985 (35 probes, 358 batches, 3,593 searches) | Pre-seed for the detection and again for the replacement walks; detection runs in batches with progress and cancel points |
| SLA line (`bench guide`) | **PASS, slowest match 6.9 s** | | PASS/FAIL against 10 s |

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
measured. Without the [bridge plugin](#bridge-plugin) crosstalk writes over REST; with it
enabled and verified at start-up, the `StreamListWriter` seam writes through the bridge
instead.

## AI verification

Matching is a yes/no decision, so the model is asked a yes/no question — not for a paragraph of
prose. Two things happen only:

1. **Verify** — the best heuristic score lands in the configured band (default 80–94). The score
   is plausible and could be wrong, which is where a second opinion is worth a request.
2. **Rescue** — nothing reached the band, but a candidate cleared the rescue floor (default 60).

The contract, precisely:

- A heuristic result at or above the `[matching]` cut (default strict = 95) never
  reaches the model. The model may promote inside the band, but it never re-checks or
  vetoes a heuristic win. Guide's identity families all clear the cut or are empty, so
  a channel with an empty family is re-ranked by the same engine at the gate's floor
  (`rescue_floor`, or `verify_band_low` with rescue off) and those sub-cut candidates
  are what the model sees.
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
returning one, and the UI says so (`calibrated: false`). When the Jev model cannot decide, the
question escalates down the `[ai.verify]` chain to an LLM fallback:

```toml
# crosstalk.toml
[ai.providers.typesafe]
kind = "typesafe"
base_url = "https://api.typesafe.ai/v1"
api_key_env = "TYPESAFE_API_KEY"
max_concurrency = 8          # the public endpoint rate-limits around eight

# [ai.providers.llm]
# kind = "openai_compat"         # the escalation fallback: a chat model for what Jev leaves undecided
# base_url = "http://localhost:11434/v1"
# api_key_env = "OPENAI_API_KEY"
# max_concurrency = 4

[ai.verify]                  # the policy every sub-app shares
enabled = true
provider = "typesafe"
model = "jev-latest"         # pin a version to keep answers stable across releases
# Escalation chain: asked in order, each link handling what the earlier ones could not
# decide. Empty means the provider/model pair above, as one link.
# chain = [{ provider = "typesafe", model = "jev-latest" }, { provider = "llm", model = "kimi-k3" }]
verify_band_low = 80
verify_band_high = 94
rescue = true
rescue_floor = 60
shortlist = 5
accept_threshold = 0.8       # accept at or above this yes-probability …
reject_threshold = 0.3       # … drop at or below this; between the two, leave unmatched
min_confidence = 0.5
```

Guide asks with task `epg` and streams with `streams` (each only when a `chain` is configured, so a
shorthand install sends exactly the request it sent before the chain existed), lineup names
always with `names`; every ask writes one `ai_decisions` row naming the model that decided. The corpus labeller (`scripts/corpus/`) can
be re-run with keys in the environment (`CROSSTALK_AI__...`) so the `[ai.verify]` chain re-decides
the disputed labels — never written to disk.

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
Steps run one at a time and the sequence **stops at the first step that fails or is cancelled**
(unless the failed step sets `continue_on_failure`, see below).
A workflow holds no app between steps: if a hook or a schedule started a run of the next step's
app in the gap, the step waits for it (at most 30 minutes, never past the window) instead of
failing.
A step that pauses at its window's end pauses the workflow; the next firing resumes *that* step,
with the step's own checkpoint picking up its backlog, and continues from there. Cancelling the
workflow cancels the step it is inside. The window's deadline reaches every step, so a workflow
inside a four-hour window never runs past it. One workflow runs at a time.

Run one by hand with `crosstalk run workflow nightly` (or `crosstalk run workflow.nightly`),
from the dashboard's **Workflows** card, or from a hook.

### Step tables: a scope per step, and carrying on past a failure

A step is a bare `"<app>.<action>"` string or a table with `action`, `scope` and
`continue_on_failure`. TOML does not allow a line break inside an inline `{ ... }` table, so
keep each one on one line, or write the steps as an array of tables:

```toml
# The shipped Lineuparr replacement (see config/crosstalk.example.toml): sync the channel
# list, match streams and guides over exactly those channels, assign logos, then hide the
# owned channels left with no streams. `scope = "lineup"` is lineup's owned channels for
# its configured file; `continue_on_failure` carries the sequence past a failed guide/logo
# step. A failed sync or stream match stops it: tidy hides the owned channels left with no
# streams, so it must never run after a stream match that did not happen.
[workflows.full_sync]
steps = [
  "lineup.sync_apply",
  { action = "streams.match_apply", scope = "lineup" },
  { action = "guide.match_apply", scope = "lineup", continue_on_failure = true },
  { action = "lineup.logos_apply", scope = "lineup", continue_on_failure = true },
  "lineup.tidy_apply",
]

# The same steps as an array of tables (every step is then a table):
# [[workflows.full_sync.steps]]
# action = "lineup.sync_apply"
#
# [[workflows.full_sync.steps]]
# action = "streams.match_apply"
# scope = "lineup"
```

- `scope = { profiles = ..., groups = ..., ignore_groups = ... }` **replaces** `[scope]` for that
  step. It is not intersected with it.
- `scope = "<name>"` is a *named* scope that an app provides. `lineup` means the channels lineup
  owns for its configured lineup file (empty until a sync or create records ownership). It also **replaces**
  `[scope]`: the step reads exactly those channels, even ones `[scope]` excludes. A named scope
  that no app in the build provides is refused when the workflow starts, and
  `crosstalk validate` reports it too. Actions whose subject is one profile
  (`streams.visibility_*`, `guide.clear_hidden_*`) still write the profile named in
  `[scope] profiles` when a named scope picks the channels.
- A step's scope only ever comes from this file. No hook, schedule or web request can pass a
  run a different scope, so a scope wider than `[scope]` exists only where you wrote one.
- `continue_on_failure = true` lets the workflow carry on when that step fails. A paused or
  cancelled step still stops the workflow, and so does a step that could not start at all.
  A workflow that carried on past a failure is recorded **failed**, and its summary lists each
  step's outcome. Leave the flag off a step that later steps depend on, like `lineup.sync_apply`.

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
  CSRF only for that reason. A wrong token counts toward the same per-client lockout as a
  wrong sign-in (ten in five minutes, then `429`). Deliveries are rate limited per client
  (`hooks.rate_per_minute` over a minute, `429` with `Retry-After`); bodies over 64 KiB are
  refused with `413`.
- **Which event**: an `?event=` query parameter, then an `event` or `type` field in the body,
  then — because Dispatcharr's default payload names none — the payload's own counters
  (`total_processed`/`streams_created`/`streams_updated`/`elapsed_time` is `m3u_refresh`,
  `programs`/`channels`/`skipped_programs`/`unmapped_channels` is `epg_refresh`; never the bare
  `account_name`/`source_name`, which the error events carry too), and only for an event the
  configuration names.
- **Debounce**: several M3U accounts finishing together is one logical "the playlists changed",
  so every delivery of the same event restarts a `debounce_secs` timer and the run happens once
  after the burst settles — or `max_settle_secs` after its first delivery (default five
  debounce windows, at most 900 s), whichever is sooner, so deliveries that never pause still
  run their targets.
- **Busy target**: if the target app (or the workflow slot) is busy, exactly **one** follow-up
  run is queued for when it frees — Stream-Mapparr's dirty-flag behaviour, so a late account is
  not dropped and a busy app is not started twenty times. The follow-up waits at most 30
  minutes (a multi-hour probe outlasts it; the drop is logged and the next event triggers
  normally).
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
  fresh** checkbox on the writing ones, an **Undo this run** button on a finished apply's run
  page, the run in progress with a cancel button, a text field on actions that declare an input
  (`lineup test_name`). A read-only `status` stays clickable while a run is live and reports that
  run's progress.
- Run page: live status, the full result table with the reason for every row, the AI decision
  where there was one, and the CSV download.
- Settings: every option with the help text the form shows, written back to the TOML files.
  The host form carries the shared options — scope, CSV retention and the AI policy — plus
  the matching toggles; each app's form carries only what that app owns.

The UI binds to `127.0.0.1` by default. To expose it, set `server.bind` and a `server.token`
of at least 32 bytes (`openssl rand -hex 32`); the daemon refuses to start on a non-loopback
address without one, or with a shorter one. A browser signs in at `/login` with the token and
gets a **session** cookie (`crosstalk_session`, `HttpOnly; SameSite=Strict`, plus `Secure`
over https): a random id held in memory, never the token itself, which expires after 12 hours
idle or 7 days at most, ends on **Sign out**, and ends on a token rotation or a daemon restart.
A script sends `Authorization: Bearer <token>`; that header is the only way the token itself
travels.

Bots and scripts talk to the same routes the forms do, with two differences from a browser:

- Scripts send `Authorization: Bearer <token>` on every request and are CSRF-exempt: a
  cross-site page cannot forge that header, so a request carrying it is by definition not
  a forged browser form.
- Browsers send the session cookie plus the session's own `csrf_token` hidden field (or the
  `x-csrf-token` header) on everything but GET/HEAD/OPTIONS; without it the request is
  rejected with 403.

What every request meets, in order:

1. **Limits**: 30 s per request (`408`), 256 requests in flight (`503`), and a body bound per
   route (sign-in 4 KiB, webhook 64 KiB, settings 256 KiB, everything else 64 KiB; `413`).
2. **Auth**, before anything reads the body or checks CSRF. Ten wrong tokens in five minutes —
   on the sign-in form or as a Bearer header on any route, the webhook included — lock that
   client out with `429` and `Retry-After`, even for the right token, until the window passes.
   A wrong token is always `401` (never a `403` that would say "the token was fine, CSRF was
   not"), and unauthenticated answers say nothing more than `unauthorized`.
3. **Host and Origin**: with no token configured, only loopback `Host` values reach the daemon
   (a DNS-rebinding page's foreign `Host` is rejected), and any request carrying proxy headers
   (`Forwarded`, `X-Forwarded-*`, `X-Real-IP`) is refused with `401`: a tokenless daemon must
   not sit behind a proxy. On every write, an `Origin` header must be the daemon's own origin
   — scheme, host and port: `server.public_url` when set, else what a trusted proxy's
   `X-Forwarded-Proto`/`X-Forwarded-Host` say, else `http://` plus `Host`.
4. **CSRF**, as above.

Every response carries a strict Content-Security-Policy, `X-Frame-Options: DENY`,
`Referrer-Policy: same-origin`, `Cross-Origin-Opener-Policy: same-origin`, a
`Permissions-Policy` switching off the device APIs, and `Cache-Control: no-store` (the embedded
`/static/*` assets are cached for a day). `Strict-Transport-Security` is sent when a trusted
proxy says the request came over https, or always with `server.hsts = true`.

### Running behind a reverse proxy

The recommended internet-facing setup: crosstalk on `127.0.0.1`, a TLS-terminating proxy in
front, and that proxy listed as trusted.

```toml
[server]
bind = "127.0.0.1"           # only the proxy on this host reaches the daemon
port = 8730
token_env = "CROSSTALK_TOKEN" # at least 32 bytes: openssl rand -hex 32
trusted_proxies = ["127.0.0.1", "::1"]
public_url = "https://crosstalk.example.com"
# secure_cookies = true / hsts = true force those on even without X-Forwarded-Proto.
```

- `trusted_proxies` lists the addresses (or CIDR ranges, IPv4 or IPv6) the proxy connects
  **from**. In Docker that is the proxy container's network, e.g. `["172.16.0.0/12"]`. Only
  a request whose socket peer is in the list has its `X-Forwarded-For` read (the rightmost
  address that is not itself a trusted proxy, so a client cannot spoof its way past the
  limiter), then `X-Real-IP`; anyone else is judged by its socket address. IPv6 clients are
  counted per `/64`. Setting it requires a token.
- The proxy must **set** (not append to a client-supplied) `X-Forwarded-Proto`, and should
  pass `X-Forwarded-For` and `X-Forwarded-Host`. `X-Forwarded-Proto: https` from a trusted
  proxy marks the session cookie `Secure` and turns on HSTS.
- `public_url` is only needed when the proxy rewrites `Host` and does not send
  `X-Forwarded-Host`; it is what a browser's `Origin` is compared against.

Caddy (sets `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Forwarded-Host` itself):

```caddyfile
crosstalk.example.com {
    reverse_proxy 127.0.0.1:8730
}
```

nginx:

```nginx
server {
    listen 443 ssl;
    server_name crosstalk.example.com;
    # ssl_certificate ... ; ssl_certificate_key ... ;

    client_max_body_size 256k;

    location / {
        proxy_pass http://127.0.0.1:8730;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Host  $host;
        proxy_set_header X-Real-IP         $remote_addr;
    }
}
```

The webhook goes through the same proxy and the same token: point Dispatcharr at
`https://crosstalk.example.com/hooks/dispatcharr` with the `Authorization: Bearer` header. There
is no separate webhook token (a deliberate choice: one token, hardened).

## How matching works

One matcher, one identity, one cut — shared by every app. A channel, stream or guide-entry name
resolves to one shared identity (canonical name, callsign, network, country, category,
region/zone, quality tier), computed once per run and cached in the store (`identity_cache`,
keyed by input + an identity version covering the merged data, the decisions, the engine version
and the normalisation/alias settings, so any of those changing misses every old row). The ranker
returns the best identity family — every candidate resolving to the same identity as the top
one, ranked — and apps differ only in what the target holds: guide assigns the family's first
entry carrying programme data (data validation, not selection), streams attaches the whole family,
lineup writes the family's canonical name. The same name gives the same identity and the same
family in guide, streams and lineup.

Every rule where the plugins disagreed was decided on a labelled corpus, not by porting one
plugin's variant:

- `docs/matching-rules-inventory.md` lists the 61 points where Janitor, Stream-Mapparr,
  Channel-Mapparr, Lineuparr (and the once-shared core) disagree: normalizer tables, `&`/dots/
  suffix handling, designator repair, bare-region strips, token-overlap guards, the callsign
  ladder + allowlist/denylist + OTA-number rescue, alias lookup shape, stage structure and
  guards, zones, country detection.
- `data/corpus/` holds the labelled pairs (plugin-unanimous pairs auto-labelled; disputes by
  majority now, by the `[ai.verify]` chain when keys are present) and `docs/matching-decisions.md`
  records each row's variants, scores and choice (cost = 2·FP + FN; decided engine: cost 844 at
  cut 80 vs Janitor 3242 / SM 4002 / CM 8619 / LU 1720).
- Orchestrator-settled rows the corpus cannot decide live in the same table as `orchestrator`
  rows: **Z07** (bare `(P)` reads as no zone), **Z08** (the zone is read after stripping trailing
  writer tags with the shared tag vocabulary), **N15** (pipe segments split/join before keying
  and before the zone/country reads), **C07** (a query with a high-confidence known-station
  callsign prefers the same-callsign candidate and excludes a different known callsign; the
  bare national entry survives as fallback), **C08** (names carrying the same high-confidence
  station callsign — `KSDK`, `KSDK HD`, `13 WREX`, `NBC (KSDK)`, `NBC 5 St. Louis KSDK` — are
  one identity family whatever spelling leads the pool; subchannels such as `KSDK-DT2` stay
  apart), **C09** (the callsign ladder reads the name with its trailing writer tags stripped,
  so `KSDK HD` is as confident as `KSDK`; a `+N` shift is never stripped) and **A05** (a name
  the alias table resolves keys as its canonical, so `CNN USA` is in `CNN`'s family).

One `[matching] sensitivity` (`strict`/`high`/`medium`/`low` = 95/90/80/70, or a bare number) is
the decision cut everywhere: guide assigns above it, streams attaches above it, lineup names
above it. What the plugins did per-app (`match_confidence_threshold`, `match_sensitivity`) is
retired and fails at load pointing at it. Borderline and empty results escalate through the
`[ai.verify] chain` (Jev decision model first, LLM fallback next; per-app tasks `epg`,
`streams`, `names`) — and only a model accept promotes. Plugin output is the oracle that chose
and validates these rules, never the target: the per-migration measurements are in
`docs/oracle-reports/` (headline numbers below), each with a 0-unattributed bar.

## Differences from the plugins

crosstalk is a port, not a fork, and every place where it behaves differently is listed here
with the reason and whether it writes less or more. The `App` column names the oracle: `guide`
rows differ from EPG Janitor, `streams` rows from Stream-Mapparr (SM) or IPTV Checker (IC),
`lineup` rows from Channel-Mapparr (CM), Channel Name Cleaner (NC) or Lineuparr (LU), and
`engine` rows hold for every app because one matcher serves all three. Plugin output is an oracle
used to choose and validate the unified rules, not a target; the per-migration measurements are
in `docs/oracle-reports/`.

| App | Difference | Reason (writes less / more / same) |
|---|---|---|
| engine | One ranker, one cut, one identity everywhere | The best identity family at the one `[matching]` cut (`strict` 95 default); per-app thresholds retired and refused at load. Same answer from every app for the same name (writes same, consistently) |
| engine | Merged newest-union data, not per-plugin tables | One channel DB per country, one station table, one alias table, one tag vocabulary, one lineup set (`data/merged/`). A name one plugin's older table lacks now resolves, and streams' country filter reads a channel's country from the same merged tables (phase 3 review: the plugin read its own per-country files), so `[matching] enabled_databases` accepts every merged country, `DO`/`PE`/`PL`/`PT` included. Approved by the goal reset ("one merged data set") (writes more where the union knows more) |
| engine | Identity families are the selection rule | Guide assigns the family's first entry with programme data, streams attaches the whole family, lineup writes the family's canonical name. Same evidence, three writes (guide and lineup: one value per channel, as before; streams: the whole family, see the streams rows) |
| engine | Callsign-first (C07): a known-station callsign outranks the bare national entry | `ABC (WABC)` with the station listed joins the station, not bare `ABC`; without the station the national entry is the fallback. Fixes network-led names ranking the stripped key first (writes different: the station's entry instead of the national one) |
| engine | Same-station callsign names are one family (C08); a trailing tag never demotes a callsign (C09); alias members key as their canonical (A05); ties go to the query's own zone | A stream or entry carrying the station's callsign (`KSDK`, `KSDK HD`, `WABC-TV`, `13 WREX`, `NBC 5 St. Louis KSDK`) joins `NBC (KSDK)` / `NBC - IL Rockford (WREX)`'s family whatever spelling leads the pool, where before it joined only when no network-led spelling was present (the `callsign-anchor` rows of the streams report; Stream-Mapparr attached it). Subchannels (`KSDK-DT2`, `WREX 13.2`) keep their own identity, and a `+N` feed keeps its low-confidence read, so no new pair matches on the corpus (cost unchanged). `CNN USA` (an alias of `CNN`) is in `CNN`'s family beside the exact spelling, so two channels spelled as two names of one identity (`Cinemax Action` and `ActionMax`, `HBO2` and `HBO Hits`) both take a stream spelled either way. At equal score the candidate in the query's own zone leads (`UK: BBC One HD` -> `BBC One`, not `BBC One East HD`), then the candidate spelled as the query (`DISCOVERY` -> `Discovery`, not `Discovery Channel`), then the token overlap. The names OTA template therefore keeps the renamed channel inside the family it came from, and `names`, `categorize`, `create`, guide and streams give one answer per entity (`crates/lineup/tests/cross_app_identity.rs`) (writes more: the callsign and alias streams/entries Stream-Mapparr also attached; writes different: the same-zone DB spelling) |
| engine | Pipe segments split before keying (N15) | `Cinemax East \| HD \|` keys as `cinemax` with zone East; a core country code beside a bar keeps a delimiter. Fixes decoration changing the family key (writes same-or-more) |
| engine | Language words in brackets stay in the name (N16) | The plugins strip every parenthetical, so `Al Jazeera (Arabic)` keyed as bare `Al Jazeera`, which all four alias tables give to Al Jazeera English. crosstalk keeps a bracketed full language word (`(Arabic)`, `(French)`, `[Español]`; 38 words, accents folded) as a plain word, so the Arabic feed and CPAC's French feed are their own channels. Two-letter codes such as `(PT)` strip as before. 30 of 61,028 known names change (writes fewer: a language feed no longer joins the other language's channel) |
| engine | Trailing writer tags strip before the zone read (Z08) | `HBO West [4K]` and `ITV1 (Central East) HD` route by their feed zone instead of joining the default family. Fixes cross-feed families (writes same-or-more) |
| engine | One tag vocabulary, one quality model, one template/collision engine, one logo resolver | NC junk, CM ignored tags, IC format suffixes, triage and `[Unk]` tags in one table; one quality tier feed; one rename/template + global collision guard; one logo tier walk. No app re-implements another's rule (writes same, consistently) |
| engine | Identity cache per run, persisted | `identity_cache` rows keyed by input + identity version (data, decisions, engine version, normalisation/alias settings); one warm pass per run. No output effect (writes same) |
| engine | One cached tv-logos tree fetch (24 h TTL), unauthenticated | CM fetched the whole recursive tree once per country searched, LU listed each country directory through the contents API (which caps at 1,000 entries); crosstalk fetches one recursive tree and serves it offline between refreshes. Only a body that parses as a tree with at least one logo is cached or used (an error page or `{}` fails the fetch, or falls back to the last good copy), the body is capped at 32 MiB, and a tree GitHub marked truncated never replaces a logo a channel already has (the report says so). Same files, fewer requests (writes same; less from a truncated tree) |
| guide | `match_apply` skips a channel whose guide was set by hand | A channel whose guide was set by hand in Dispatcharr keeps it; the plugin re-matches and overwrites. Skipping is safer, and the CSV reason says so (writes less) |
| guide | AI may promote inside the verify band | With the AI gate on, an AI-accepted candidate can be assigned while its heuristic score sits below the `[matching]` cut. The cut still gates every heuristic assignment: the model never re-checks or vetoes one, and heal never asks it at all. A channel whose identity family is empty at the cut (default strict = 95) is re-ranked by the same engine at the gate's floor, so the verify band and rescue candidates still reach the `[ai.verify]` chain. Plan decision: the AI gate and the `[ai.verify]` escalation chain are a host feature every app gets (writes more where the model accepts) |
| guide | Match walks the identity family, not the full ranked list | Guide ranks the channel's identity family (unified engine, `[matching]` cut) and assigns the first member with programme data. When nothing is applied, the row still carries the top family member's score and method: “below threshold” when the family is empty, “carries no programme data” when a checked member was empty. The plugin checks every scored candidate and reports the first programme-carrying one whatever its score, so the same channel can show a lower score with programmes behind it — see the oracle report (`docs/oracle-reports/guide.md`) (writes less: below-cut programme carriers stay unmatched) |
| guide | Heal reports `NO_REPLACEMENT_FOUND`, never a low-confidence row | Below-cut candidates never enter the identity family (writes less) |
| guide | Candidates with no EPG source are never replacements | Heal validates by `tvg_id` and source, so an entry without a source cannot be checked; the plugin validates by EPG id alone and can accept one (writes less) |
| guide | `match_apply`/`heal_apply` record undoable channel-ops rows | Writes go through `channel_ops` (`Set Epg`), so host undo reverses them; the CSV and match rows are unchanged (writes same) |
| guide | `match_apply` skips unchanged assignments | The plan skips a channel already on its match instead of re-sending every association per apply (writes less) |
| guide | Programme data that cannot be read is “unknown”, never “missing” | The plugin's check is one SQL query and cannot fail per channel; over REST one can. A channel the scan could not check is left out of the result set (counted as `unchecked` in the summary and the CSV), so no cleanup can tag or clear it (writes less) |
| guide | Cleanup selections come from the run store, not a results file | `scan` stores its result set as run rows: queryable, kept with its CSV, and not overwritten by an unrelated run. There is no age limit for the same reason, and no `export_results` action — every run already writes its CSV (writes same) |
| guide | Every selection is intersected with `[scope]` again at apply time | The plugin applied the ids its scan had stored, so a settings change in between could reach a channel outside the scope. An id that is gone or out of scope is reported as skipped (writes less) |
| guide | A channel whose guide changed since the scan is skipped | The plan re-reads every channel, so an old scan can only be out of date — never double-tagged, and never clearing a guide that was reassigned (writes less) |
| guide | Tag-suffix presence is bracket-aware and case-insensitive | `CNN [badepg]` and `CNN (BadEPG)` count as tagged through the shared vocabulary, where the plugin's exact `ends_with` re-tagged case/bracket variants (writes less) |
| guide | `clear_hidden` only unassigns | The plugin also deleted each entry's `ProgramData` rows, which blanks the guide of every *visible* channel sharing that entry. crosstalk never deletes programme data (writes less) |
| guide | `clear_hidden` counts "not enabled" as disabled, and refuses a sparse profile | The plugin selected only membership rows with `enabled=False`. Dispatcharr's REST API lists only a profile's ENABLED channels, so crosstalk cannot tell a disabled row from no row at all: a channel with no membership row (a profile created empty, or channels added after it) is cleared too. To keep that from reaching a whole install, a profile that leaves more channels out than both 25 and its own enabled list is refused, and every preview says so (writes more, bounded) |
| guide | `clear_hidden` keeps `[scope] groups` / `ignore_groups` | The plugin selected every disabled membership of the profile whatever its group. crosstalk still bounds it by the groups every other action is held to (writes less) |
| guide | `clear_groups` refuses an empty `[scope] groups` | The plugin treats the same empty filter as “every channel”, which makes one press unassign every guide in the install (writes less) |
| guide | Clearing a guide skips a channel with an EPG override | Clearing the channel's own column would not change what is shown, which is the same reason match skips one (writes less) |
| guide | `clear_by_regex` accepts Rust `regex`, not Python `re` | Backreferences and lookaround are refused with an explanation rather than silently matching something else, and the pattern is matched case-sensitively and unanchored as in the plugin (writes same-or-less) |
| guide | The watchdog is armed by a schedule | `watchdog_enabled` and `watchdog_check_interval_hours` are gone: the host owns schedules, so `"guide.watchdog_apply" = "0 */6 * * *"` is the interval — and a schedule naming `watchdog_preview` stays a preview (writes same) |
| guide | The watchdog is a preview/apply pair, and the apply is **orange** | The plugin's single button audited and refreshed in one press, and was painted green although it rewrites guide data. Here the audit is a read, the refresh confirms, and neither touches a channel (writes same) |
| guide | A queued refresh is waited for, with a timeout | The plugin's refresh ran inline, so it had nothing to wait for. crosstalk polls until the source settles and reports a wait that lapsed as “did not settle” rather than as a failed refresh (writes same) |
| guide | The guide horizon costs requests, not one SQL aggregate | Over REST there is no “latest programme per source” route, so staleness is one probe per source and the exact horizon about 28 indexed counts — paid only by a source the run is about to refresh (writes same) |
| guide | “Has mapped channels” comes from the channel list | channel → its own EPG entry → that entry's source is exactly the plugin's ORM query, and reusing the world the run already loaded costs no extra request (writes same) |
| guide | A recovery is a log line and a run row | crosstalk has no Dispatcharr System Events. `watchdog_log_on_recovery` keeps its meaning, every source gets a row either way, and a re-read that fails counts as “no improvement” where the plugin distinguished a vanished source from a database error (writes same) |
| guide | The watchdog wants an administrator's API key | The programme search only returns rows on guide data a channel the key's user can reach is mapped to, so a lesser key reads sources short or empty — and its refresh POSTs answer 403. The run warns when the key is not an administrator's; the plugin, inside Django, had no key at all (writes same) |
| guide | The plugin's action ids and its usage counter are not carried over | `janitor` became `guide` and `automatch_*` became `match_*`, and the plugin's own ids appear in no schedule, URL or CSV. Anonymous usage reporting is not ported: a run is recorded in crosstalk's own store and sent nowhere (writes same) |
| streams | Stream selection matches account and stream-group names exactly | Stream-Mapparr looks both up in an exact, case-sensitive `{name: id}` map with no wildcards, and so does crosstalk — unlike `[scope] groups`, which accepts `*` and `?`. A token with a `*` in it therefore matches no stream group and lands in the unknown-names warning instead of silently selecting a family the plugin would have fallen back past. The account list's ORDER is the priority, and a source it does not name is kept and sorts last (999) (writes same) |
| streams | `streams.sort_*` does not partition by country | Stream-Mapparr's bug-158 fix makes the country tier the OUTER key when `restrict_matching_to_country` is on (`_same_country_ids_for`). A sort here orders by the quality key and the feed zone only, whatever the setting says; with the setting off — the default — the two agree exactly. `match` does partition by country (writes a different order with the country filter on; the same with it off) |
| streams | A sort's CSV is the shared channel-operation report | The same columns every other channel write writes (before list, after list, status, reason) instead of the plugin's per-stream `tiers` / `throughput_mbps` / `edge_ips` columns; the tiers are summarised in the report's preamble instead (writes same) |
| streams | `streams.sort_*` needs no Channel Profile | The plugin refuses without one ("Profile name is required"). The profile list is `[scope] profiles`, and an empty one means every channel, like every other action's scope — the parent plan's rule that scope lives once in the host and an app never re-declares it (writes more: runs where the plugin refused) |
| streams | A sort tolerates the stats shapes that crash the plugin | `source_fps` written as a numeric string, and a throughput cache entry whose numbers are strings, make the plugin's whole sort action raise. crosstalk reads the number it can read and falls back to 0 (or to the re-estimated nominal bitrate) for the rest — the answers the plugin gives for the same stat written numerically. A probe never writes either shape; only a hand-edited cache could. Acceptable because the order written is exactly the plugin's own answer for a correctly typed stat, where the plugin wrote nothing at all (writes more: sorts where the plugin raised) |
| streams | One counted warning replaces a log line per channel | The plugin logs one line per zone-routed channel that had to keep other-zone streams. At 20k channels that is 20k lines in one run (writes same) |
| streams | `streams.match_apply` only writes channels whose list changed | The plugin deletes and recreates every matched channel's list; crosstalk plans a stream-list write only where the list actually differs. Same end state, so a second run over an unchanged library writes nothing (writes less, same end state) |
| streams | Stream-Mapparr's US-OTA-only mode is a setting, not a second action | The plugin ships it as its own button with a different candidate rule: per CHANNEL (no name grouping, no `visible_channel_limit`), US callsigns only, upper-case callsign occurrences in stream names, and no country filter or zone routing. Here it is `ota_only` on the same pair, because it is the same write with a narrower rule (writes same) |
| streams | `streams.mirror_apply` leaves a channel holding a dangling stream id alone | The plugin plans such a channel from its join (which drops the id) and then refuses it at the locked re-read (which still sees it). crosstalk plans it the same way and never writes it, instead of rewriting the list without the id. The CSV still lists the planned additions with a `skipped` status, and the readout counts match the plugin's (writes less) |
| streams | `streams.mirror_*` reads streams from one loaded pool | The stream list over REST omits streams on inactive M3U accounts, so a channel holding one of those is left alone too — strictly less than the plugin writes there, never more (writes less) |
| streams | `streams.mirror_apply` records the previous order in the run store, not a snapshot file | The plugin refuses to write unless it first saved every changed channel's order to a snapshot file, and skips a channel that moved mid-run. crosstalk plans and writes one world snapshot: what the run reports as `before` is what Dispatcharr held when the run started, and a concurrent edit after that is overwritten rather than detected. Same trade as every other channel-write action (writes same; a concurrent edit is overwritten, not detected) |
| streams | `streams.visibility_apply` writes only the memberships that move | The plugin disables every channel in scope and re-enables the winners — two bulk updates touching rows that end up unchanged. crosstalk writes the end state as per-channel values, so a channel already holding its value is a skipped row, not a write. Same end state (writes less, same end state) |
| streams | `streams.visibility_apply` writes the first named profile that exists | Stream-Mapparr's `profile_name` list resolves to `profile_ids[0]` for the write while every named profile widens the channel set; crosstalk does exactly that. With a single profile the action only ever disables (writes same) |
| streams | `streams.visibility_apply` never enables a channel that is not already enabled in the profile | The plugin enables each winner with `update(enabled=True)`, which reaches a disabled row but not a channel with no row in the profile (a `start_empty` profile, or a channel created for chosen profiles only). Over REST a profile lists only its enabled channels, so the two cases look the same, and the only write, `PATCH .../bulk-update/`, *creates* a missing row. So crosstalk withholds every such enable (the report lists them, `enable_withheld` counts them) and only ever disables: its writes are a subset of the plugin's. This only matters with several profiles in `[scope] profiles` (a winner enabled in the second but not the first); with one profile the plugin's enable step is a no-op too. Profile membership is not one of the [bridge plugin](#bridge-plugin)'s writes, so this stays (writes less) |
| streams | `streams.visibility_*` reads Dispatcharr live, not the last load snapshot | The plugin reads its `processed_data` file and refuses to run without it. A channel created since the last load takes part here — the plan's improvement that every apply plans from live state, never from a saved snapshot (writes more: channels created since the last load) |
| streams | `streams.probe` is one action with `--dry-run`, not a pair | It writes stream stats and crosstalk's health table, never a channel's data, so "what would change" is a number the run only learns by measuring. `--dry-run` runs the whole probe, records health, the CSV and the per-channel verdicts, and writes no stream stats (writes same) |
| streams | `streams.probe` caps connections per M3U account, not workers | IPTV Checker's global `parallel_workers` (2) is gone: `probe_per_account_limit` (default 1) connections per account, never above that account's `max_streams`, with `stream_check_delay` between starts on it. Total parallelism grows with the number of accounts, which is the limit a provider actually enforces (writes same) |
| streams | `probe_rate_per_minute` throttles throughput downloads only | That is the one place Stream-Mapparr used it (a global cap around the HTTP downloads). Neither plugin capped its stream CHECKS per minute — IPTV Checker is paced by its workers, its per-stream delay and its 429 backoff — so neither does the probe. A download also holds a lane permit, because it is a second connection to the same provider (writes same) |
| streams | `streams.probe` skips a stream probed within `probe_freshness_hours` | The plugin's idea of progress was a JSON resume file per window and its results file; it had no notion of "this stream was fine an hour ago", so every window re-probed everything except what the resume file named. crosstalk reads the store's `stream_health` instead, which also makes an interrupted run resume for free (writes less) |
| streams | `streams.probe` checkpoints its queue in the run store | The plugin kept a resume JSON file per window, so a lost or invalidated file re-probed everything. Crosstalk checkpoints the queue with the run: a probe that hits its window's end is recorded PAUSED and the next firing continues it, a cancel keeps the place too, and `--fresh` (or **start fresh** on the app page) is how an operator starts the backlog over (writes same) |
| streams | `streams.probe` retries per stream, not in global passes | Same retryable error types and the same count, spaced `stream_check_delay × 3` apart, but a retry happens beside the stream that failed instead of after re-walking the whole queue. At 50k streams the plugin's pass-based loop re-queued the failures for a second full sweep (writes same) |
| streams | `streams.probe` probes each stream once, however many channels carry it | The plugin's loaded-channels file repeated a stream once per channel, so a stream on four channels cost four probes and left four result rows (writes less) |
| streams | `streams.probe` can probe streams attached to no channel | `probe_unattached_streams`, off by default. IPTV Checker had no way to check an orphan stream, and a provider's new stream is exactly that until a match runs. Phase 2 plan: unattached streams are probed only on request (writes more with `probe_unattached_streams` on) |
| streams | `streams.probe` refuses to run without ffprobe | A missing binary made the plugin classify every stream `Other`/Dead and clear every `stream_stats` row — the worst possible outcome of a probe. crosstalk checks the two paths before the first stream and refuses the run, naming the setting (writes less) |
| streams | `streams.probe` opens only network URLs, and the command line is not the plugin's | IPTV Checker handed Dispatcharr's URL to ffprobe as the last positional argument, so a URL like `-report`, `file:/etc/passwd`, `/etc/passwd` or `concat:a\|b` read local files or wrote a log into the working directory. crosstalk probes only `http`, `https`, `rtmp`, `rtmps`, `rtsp`, `rtp`, `udp` and `srt` URLs (anything else is SKIPPED as `Unsupported URL` with no tool run, never dead, so triage leaves it alone), passes the URL as `-i <url>`, and adds `-protocol_whitelist http,https,tls,tcp,udp,rtp,rtmp,rtmps,rtsp,srt,crypto,hls,httpproxy,data` (ffmpeg's own default nested whitelist for an http input plus the other allow-listed schemes, so an inline `data:` HLS key still reads; nothing local) to both tools. The one documented parity exception for the command line: `scripts/probe_parity.py` compares the plugin's argv with these flags added (writes less: no verdict for a non-network URL) |
| streams | `ffprobe_path` / `ffmpeg_path` are file-only | The plugin's settings form edited them; here they are absent from the web form and a web save naming them is refused, because every probe executes them. The file's value must be absolute and named `ffprobe`/`ffmpeg` (or `.exe`), and the probe refuses one that is not executable or is world-writable (writes same) |
| streams | `streams.probe` leaves an unchanged `stream_stats` alone | Same end state; a nightly run over 50k streams should not be 50k PATCHes. The run counts what it wrote and what it left (writes less, same end state) |
| streams | `streams.probe` records a cancelled probe as neither a verdict nor a count | A killed invocation (cancel, shutdown) is excluded from the run's records, as the plugin excluded its `Cancelled` rows — but it also writes no health row, so the stream stays due for the next run instead of looking dead (writes less) |
| streams | `streams.probe` measures throughput when `enable_throughput_sorting` is on | Stream-Mapparr's separate `probe_throughput` action, folded in: same 8 s download, same 64 KiB-in-under-a-second guard, same "a failed measurement is never fresh". It costs a second connection per stream, so turning the sort's throughput tier off turns the download off too (writes same) |
| streams | `streams.probe` writes a fixed CSV column set | The plugin emitted dynamic `ffprobe_*` columns per run. crosstalk keeps its base twelve columns and adds the evidence as fixed ones (`container_duration_seconds`, `calculated_bitrate_kbps`, `packet_count`, `mean_volume_db`, `throughput_mbps`, `edge_ip`), so two runs' reports diff (writes same) |
| streams | The ffprobe analysis window is a constant | `ffprobe_analysis_duration` is retired with the other keys that moved, so the window is the port's own constant; the two timeouts that decide a stream's fate (`timeout` for the connection, `probe_timeout` for stream data) are settings (writes same) |
| streams | `streams.probe`'s per-channel verdicts are run rows, not a results file | `triage` reads the `probe_verdict` and `probe_format` rows of the last probe run. The plugin read `results.json` and refused to act without it. A `--dry-run` probe counts like any other: it records the same verdict rows and only skips the Dispatcharr write. A windowed pass split across nights reads as one: the newest succeeded run plus the finished runs back to the previous succeeded one, newest verdict winning per channel (the plugin's accumulated `results.json`) (writes same) |
| streams | `streams.triage_*` is one action with seven switches | The plugin had seven buttons, each its own confirmation, and a scheduler block that gated the same six renames/moves plus the suffix. Here they are seven switches in `streams.toml` — `rename_dead_channels`, `rename_blank_channels`, `rename_low_framerate_channels`, `add_video_format_suffix`, `move_dead_channels`, `move_blank_channels`, `move_low_framerate_channels` — that `triage_apply` reads. **All seven default OFF**, as the plugin's scheduler toggles do, so an apply with nothing switched on REFUSES and names them instead of reporting a no-op run. The plugin's scheduled order is a workflow: `["streams.probe", "streams.restore_apply", "streams.triage_apply"]` (writes same) |
| streams | `streams.triage_apply` does not tag a channel twice | The plugin renames from the name the last check recorded, so a channel that stays dead becomes `ESPN [DEAD]` on Monday and `ESPN [DEAD] [DEAD]` on Wednesday, and only Restore collapses the stack. crosstalk appends nothing when the recorded name already ends with the format's own bracket label. Same after a restore; the parity harness counts this shape and asserts the tag is the only difference (writes less) |
| streams | `streams.triage_apply` writes only the values that change | The plugin bulk-updates every channel of a class, including one already carrying the tag and one already in the destination group. `channel_ops` reports those as skipped rows, so the same end state costs fewer writes (writes less, same end state) |
| streams | The triage destination group is created only on an apply | The plugin's `get_or_create` runs when the move action runs, so a dry run creates nothing either — but its "preview" was the dry-run switch inside the write. crosstalk's preview plans the move against a placeholder id carrying the real group name, so the CSV shows `Graveyard` and `POST /channels/groups/` is never sent (writes less: a preview creates nothing) |
| streams | `streams.restore_*` reverses crosstalk's own run rows, not `channel_state.json` | The plugin kept a state file of the group each channel came from and an in-memory tag list derived from the current settings. crosstalk records every rename and move with its `before` value in the run store, and a recorded change is still in effect when the channel's current value equals the change's `after`. No file to lose, no last-writer-wins between processes, and the record survives a settings change (writes same) |
| streams | `streams.restore_*` undoes what it recorded, by value | The plugin strips every known trailing tag by NAME, so it also removes a `[HD]` the operator typed by hand, and it cannot see a label the current formats no longer name. crosstalk restores the recorded value (so a hand-typed tag survives) and falls back to the plugin's tag strip only for a channel with no recorded change at all — which is how a channel the plugin tagged before crosstalk took over is healed (writes less: a hand-typed tag survives) |
| streams | `streams.restore_*` leaves the format suffix alone | The plugin's settings text says restore does not strip the video format suffix, and its regex can. crosstalk never reverts a recorded suffix row, but a suffix stacked on top of a recorded mark does not block the mark's restore either: the trailing format run is stripped to follow the chain (as the plugin's own strip does) and dropped with the mark. Undoing the suffix nightly would flap against the next triage run (writes less) |
| streams | `streams.restore_*` reads the newest 50 `triage_apply` runs | The plugin's `channel_state.json` kept one entry per channel until it was cleared by a restore, however old. crosstalk reads the channel rows of the newest 50 finished `triage_apply` runs (and `[jobs] run_history_days` prunes the table underneath): a change older than either falls back to what the name itself says, so a very old mark still loses its tag but a very old move may keep its group (writes less) |
| streams | A recorded group that no longer exists is a skipped row | The plugin warns that the destination is gone (`missing_group_ids`). crosstalk restores the channel's NAME, leaves the group untouched, and says why in the row (writes less) |
| streams | A restore back to "No Group" is planned and refused | Dispatcharr's channel serializer rejects a null `channel_group_id`, so a channel triage moved OUT of "No Group" cannot be moved back over REST. The row is planned, reported, and fails with that reason. The plugin never recorded such a channel at all (`_compute_capture_state` skips a channel with no group). With the [bridge plugin](#bridge-plugin) enabled the move is written (writes same: REST refuses the write) |
| streams | A finite container duration is the only placeholder fingerprint | The plugin's `placeholder_file_detection` needs ffprobe to report a container duration, and a live MPEG-TS never does — so the fingerprint only fires on a container that carries one (HLS, MP4), which is what its measured placeholder was. The evidence is recorded for every stream either way (writes same) |
| streams | `streams.match_preview` drops the plugin's threshold-analysis advice rows | Preview also re-ran the pipeline at 5-point thresholds and collected by exact match against the winning stream name; the plugin itself notes that path disagrees with the real matcher. Those rows were advice, never a write (writes same) |
| streams | `streams.match` runs the unified matching engine, not Stream-Mapparr's collector | One ranker (the engine's best identity family per channel group), one selection rule (streams attaches the whole family, all members ranked, above the one `[matching]` cut) and one cache shared with guide. Retired: SM's normalized-name groups (identity family keys), the OTA callsign regex path (the engine's callsign anchor), alias force-includes (the merged reverse index), the fuzzy gate with the token/substring family, and the per-app `match_sensitivity` (a file still naming it fails at load, pointing at `[matching] sensitivity`). Every difference from the oracle is attributed in `docs/oracle-reports/streams.md` (writes different: see the oracle report) |
| streams | AI may promote a borderline stream group | With the AI gate on, a group with no family at the `[matching]` cut is re-ranked by the same engine and index at the gate's floor, and the verify-band / rescue candidates go to the `[ai.verify]` chain with task `streams` — the same pattern as guide. Only a model accept attaches streams (the accepted candidate's identity family at the floor, through the same country filter as the cut); the heuristic walk still accepts nothing below the cut. Each ask writes one `ai_decisions` row on the group leader's report row. Plan decision: the AI gate and chain are a host feature every app gets (writes more where the model accepts) |
| streams | Channel groups carry the family's country, filter on or off | Grouping is the identity family key plus the country the leader's family is ranked under: the country filter's `_channel_country_code` (database and channel-group name) when the filter is on, else the name's own `<CC>:` marker. `UK: CNN` and `US: CNN` never share a group, so one country's family is never written to the other's channel, and a bare `CNN` in a `US:` and a `UK:` group stay apart with the filter on (bug-158), in match and visibility alike (writes less) |
| streams | `streams.triage`/`restore` tags and rename formats run on the unified vocabulary | Tag checks and strips read the shared tag table, not per-set regexes: `(DEAD)` counts as trailing alongside `[DEAD]`, parens strip, and stacked mixed runs strip in one pass. Rename formats render through the shared template engine: `{group}`/`{number}`/`{epg}`/`{id}` substitute the channel's own values now (EPG entries are loaded only when a format names `{epg}`); the format is still trimmed, and a blank rendering is never written. The strip SETS stay the plugin's (the IC status and format labels, the rename labels, `video_format_suffixes`): guide's `BadEPG`, CM's `Unk` and provider tags like `[1080p]`/`[HEVC]` are never stripped — other apps' marks, not triage's (writes different: parens strip, formats substitute real values) |
| streams | With `filter_dead_streams` on, match also excludes store probe verdicts | The plugin only reads `stream_stats` (empty, 0 width/height, `0x0`); crosstalk reproduces that test exactly and additionally skips streams the probe recorded as `dead` in the store — the same verdict, kept where a rerun can see it (writes less) |
| streams | EPG placeholder titles come from one batched read | The plugin queries the programme table per EPG record and memoizes per pass (~8600 queries in one pass before the cache existed). crosstalk resolves the record ids a run can actually reach — a placeholder name on a channel, a placeholder or watched name on a stream — and reads them in one chunked `POST /epg/current-programs/`. The cleanup rules, skip titles, "Next Event" gate and watch containment are the plugin's own (writes same) |
| streams | The `tvg_id` join lists every EPG entry | A stream carries only the guide identifier, so joining it to a record id lists `GET /epg/epgdata/` (`ceil(n / 1000)` requests — ~55 at 55k entries). Not narrowed: `EPGData` has no `tvg_id` filter, and the programme search is not equivalent (multi-source `tvg_id`s, no `parsing` marker, no dummy/index fallback) (writes same) |
| streams | The "Next Event" day is Dispatcharr's date | The plugin reads General Settings -> Time Zone in-process. Over REST the same value is read once per run from `GET /api/core/settings/` (`system_settings.value.time_zone`), so the "at TIME on DATE" clause is compared against today in Dispatcharr's zone on both sides. When that read fails the run degrades to the host's date and says so in the report, rather than refusing the match (writes same) |
| streams | A guide that cannot be read leaves names as written | The plugin's lookup failures were per-record and swallowed; over REST the read is one request, so a failure is logged and reported in the CSV preamble and the run proceeds with the raw names. Refusing the match over one auxiliary read would be worse than the plugin (writes same) |
| streams | The read-only tools write a CSV, not a text readout | The plugin wrote a `.txt` into its own readout directory and put a clipped headline in a toast. crosstalk writes `<app>_<tool>_<timestamp>.csv` into `[exports]` under the same retention rule as every other app, and the headline is the run summary (writes same) |
| streams | `streams.match_apply`'s append mode renumbers the channel's list | The plugin's append gives the new rows order values that collide with the existing rows; crosstalk writes the same membership in the same playback order, numbered from zero (writes same membership and order) |
| lineup | `clean_*` runs the unified host scope, not NC's casefold match | NC matched group names casefolded-exact; crosstalk plans over `[scope]` (or the step override, `scope = "lineup"` included) with host wildcard/case rules, and refuses unknown groups. `protected_groups` stays exact-name, case-insensitive and wildcard-free like NC's, because it is a protect list, not a scope (it lowercases where NC casefolded, which differs only on letters like `ß`) (writes same-or-less) |
| lineup | `clean_*` keeps a trailing country-code pipe segment delimited | NC joins every surviving `\|` segment with the separator, so `BBC One \| UK` becomes `BBC One UK` — a name the engine keys `bbc 1 uk`, no longer the `BBC One` its guide entry and streams key. crosstalk keeps the bar (`BBC One \| UK`), exactly as the engine's own pipe collapse reads it (N15), so a cleaned name keeps its identity; leading codes join as NC joins them (`UK Sky Sports`, read as country UK either way). The one code list is the engine's (`tags::is_pipe_country_code`); `clean_parity.py` counts these rows as the documented `country-segment` divergence (writes different: one bar kept) |
| lineup | Collisions are counted, not set-arithmeticked | NC's set arithmetic could free a name another channel still holds; crosstalk counts slots, and a `skip` loser keeps its old name occupied (cascade to a fixpoint). Global, like NC — Dispatcharr lists channels flat (writes less: no double-takes) |
| lineup | No backup CSVs; undo is the host's | NC wrote backup CSVs into the plugin dir; crosstalk reverses any apply from its run rows (`crosstalk undo`, Undo button). Created channels hide on undo; created groups/logos are left and listed (writes same, reverses more) |
| lineup | `fix_casing` can move the matching key | Off by default, as NC's; when on, `AccuWeather` → `Accuweather` re-keys (`accu weather` → `accuweather`), which can cost guide/streams matches — `test_name` shows it (writes same-or-less) |
| lineup | Custom/protect patterns are Rust `regex` | No backreferences or lookaround in the pattern (Python `re` had them); invalid lines warn-and-skip like NC's `LOGGER.warning` (writes same-or-less) |
| lineup | `names_*` targets are the unified engine's, per path with CM citations | OTA when the identity callsign is in the merged station table (template render; missing placeholder refuses; opt-in market fallback), else the best identity family's merged-DB leader + shared-vocabulary tag re-attachment, else `unknown_suffix` only when truly unmatched. No saved-JSON replay: every plan is live. Every difference is attributed in `docs/oracle-reports/lineup-names.md` (writes different) |
| lineup | Tag re-attachment never doubles what the base holds | `KSPT-DT3 (HEROICN)` stays itself; CM appended a second `(CBS)` to `K28NN-D (CBS)`. Same never-double rule as `apply_suffix` for `[Unk]` (writes less) |
| lineup | One regional word at most; `NBC: ION` reads as `NBC` | A base stating a feed zone gains no second regional word; the affiliation parse splits on `:` too (fix over CM, whose render re-renamed every run) (writes less, stable) |
| lineup | Digit-led callsign shapes do not block premium | A callsign CM's ladder reads but the merged station allowlist does not (digit-led shapes) blocks the premium path on CM's side only; here the name takes the premium path like any other (`ota-vs-premium` in the names report). A consequence of the corpus-decided allowlist row (C02 `merged`: the merged station table is the one callsign source), not a separate rule (writes more: a premium rename where CM left the name) |
| lineup | The DB base drops ignored quality tags | `BX Culture (HD)` names to `BX Culture`, so `names` and `clean` reach one fixpoint instead of renaming each other's output every run (writes different, stable) |
| lineup | `categorize_*` never classifies an OTA identity | CM's name-only path returns No Match for OTA template names while the unified ranker reaches the station through C07; a channel whose identity callsign is a merged station stays put (country-named categories are no category either — a DB category equal to any merged country name/code never enters the map). Strictly safer than CM, which classified `ABC (WABC)` by its bare network (writes less) |
| lineup | EPG-first logo order; the network-logo tier only for channels with no logo | Unified order `epg, logo_manager, tv-logos, network` (default), then the `default_logo` safety net; the network fallback only answers for a channel with no logo, like CM. CM consulted tv-logos (station, then fuzzy) before EPG and network; LU consulted tv-logos only in its own country at ratio ≥ 85 (writes different: another tier wins where both have a logo) |
| lineup | Logo similarity is the unified engine's, with directory fixes and percent-encoded URLs | One similarity (`calculate_similarity_chars`, threshold 85 in every mode) instead of each plugin's own fuzzy ratio (CM's replace mode followed its match sensitivity); fixes over CM: Norway is read from `nordic/norway` and British files' `-uk` suffix strips for `GB`; URLs are percent-encoded. The directory fixes correct a plugin bug (CM searched a directory tv-logos does not have, so it found nothing); recorded in the step 9 and step 16 review notes (writes different near the threshold; more where a fixed directory now finds a file) |
| lineup | Unknown `logo_sources` tokens and zero-match `category_groups` fail the run | Per-channel skips elsewhere, but a misspelled tier or a scope matching nothing is a configuration mistake, reported loudly (writes less) |
| lineup | `create_*` makes one channel per family, never one per stream | CM made one channel per matched stream with `[M3U-id]` suffixes and duplicated every channel on rerun; crosstalk makes one channel per identity family (streams mode) or per OTA station (stations mode), deduped by owned keys + a same-family check against live channels. A family with no category (country-named categories are none) and no custom import group is skipped with the reason, where CM imported into the country-named group (writes less, reruns write nothing) |
| lineup | Collision guard and global number check on every create | CM's `[N]` suffixes are the shared resolver; numbers are global (Dispatcharr enforces uniqueness nowhere REST reaches), so a claim on a held number is refused naming the holder (writes less) |
| lineup | No AI gate in create; unmatched reported, never created | Borderline streams stay unmatched even with the names AI gate on (writes less) |
| lineup | Missing stations target group is created owned; ambiguous still fails | CM errored on a missing target group; crosstalk creates it through the one create stage every lineup write uses (plan decision: creates are a channel-ops stage with rows — previewed, owned, listed by undo), like categorize and sync create theirs. An ambiguous group still fails like CM (writes more where CM errored) |
| lineup | `sync_*` keeps owned numbers on re-sync; only owned channels renumber | LU renumbered every channel in auto modes on every rerun; crosstalk renumbers only owned channels whose file number changed in `lineup` mode, through the global conflict check; auto modes number new channels only (writes less) |
| lineup | Foreign same-name clashes are reported and skipped, never adopted | LU's `(name, group)` get_or_create adopted a user's same-named channel (and its streams/guide); crosstalk creates only where no foreign channel holds the name, else skips with the reason. Same-name-elsewhere still creates, like LU's lookup (writes less) |
| lineup | Profile rows only at create; deletes are hide | LU enabled every synced channel in the named profiles on every sync; crosstalk sets membership at create only. LU's replace-mode delete of stream-less channels is `tidy_apply` (hide, undoable), which unhides only what tidy hid (writes less, reverses all) |
| lineup | Owner key ignores the folded category | `category_detail` changes move channels between groups instead of re-creating them; sync names are the lineup entry names, not `names_apply`'s template (names owns display names; sync mirrors the provider file) (writes less) |
| lineup | `streams.match` lineup mode is an owned-id bound with a country default | LU matched within group names with per-channel US↔MX exceptions and a provider-group pass; crosstalk matches exactly the owned channels of the configured file, defaulting to the lineup file's country (a foreign-identity entry keeps its own; callsign identities exempt), attaching the identity family at the one cut (writes same-or-less, scoped) |
| lineup | Lineuparr's matcher is not ported | Its stream half is `streams.match` lineup mode over the unified engine; its EPG half is `guide.match`. LU's own scorers run only inside the Python oracle scripts (`scripts/corpus/oracles.py`) behind the reports (writes per the unified rules) |
| host | `crosstalk undo <run-id>` reverses still-in-effect rows | Any finished apply run: `Set` fields back to before-values where current equals after, unhide what tidy hid, hide what create made. Refuses previews, reads, running/paused runs and runs with nothing undoable; the undo is itself a recorded, undoable run (writes reverse) |
| host | Workflow steps override scope and survive failure by flag | A step is `"<app>.<action>"` or `{action, scope, continue_on_failure}`; `scope` (a table or a named scope like `lineup`) replaces `[scope]` for that step; `continue_on_failure` carries `full_sync` past a failed guide/logo step (a failed sync or stream match still stops it, so tidy never hides channels a failed match left without streams), and the workflow ends failed listing each step (writes as the steps write) |
| host | CSV cells that look like formulas are quoted | A cell whose first non-whitespace character is `=`, `+`, `-` or `@` (or the full-width `＝ ＋ － ＠`), or that starts with a tab or a line break, gets a leading single quote, so such names differ from the plugin's CSV. It is a spreadsheet display instruction: a text editor shows the quote as part of the value. The `#` preamble lines replace control characters (a newline in a channel name) with a space, so every preamble line stays one `#` comment line. Reports are written as UTF-8 without a BOM (writes same) |
| host | The plugins' action ids and usage counters are not carried over | `janitor` became `guide` and `automatch_*` became `match_*`; CM/LU/NC ids (`load_and_process_channels`, `sync_channels`, `preview`, `email_report_now`) appear in no schedule, URL or CSV. Anonymous usage reporting and phone-home counts are not ported: a run is recorded in crosstalk's own store and sent nowhere (writes same) |
| host | The plugins' actions crosstalk does not carry | IPTV Checker's `delete_dead_channels` (deletes are out of scope), `view_*` / `export_results` (the run page and its CSV), `cancel_check` (the run page's cancel); CM/LU `email_report*`, LU `resort_streams` (a `streams.sort_apply` lineup step), LU `plugin_status` (`status`), NC's scheduler (`[schedules]`/`[hooks]`); every plugin's notification, scheduler housekeeping and `clear_csv_exports` (host `exports clear`). A retired key is refused with the pointer to where its meaning went (writes same) |

## Oracle reports

Each migration step published the diff between the unified behaviour and the plugin it replaces.
Every difference traces to a `data/corpus/decisions.json` row, a documented fix or the unified
selection rule; the unattributed list is the review surface (bar: 0 everywhere). Section 2 of the
guide/streams reports is the representative world (seeded, not mined — read its agreement rate as
the migration's real-world size); the disputed sections are mined from pairs where the plugins
disagree, so their agreement is low by construction.

| Report | Oracle | Headline numbers |
|---|---|---|
| `docs/oracle-reports/guide.md` | EPG Janitor `_find_best_epg_match` | Representative: 300/303 queries agree on the winner; disputed: 9/46 agree; `unattributed`: 0 |
| `docs/oracle-reports/streams.md` | Stream-Mapparr collector | Representative: 144/225 agree with the filter off (64.0%), 159/225 with it on (70.7%); disputed: 30/55 (54.5%); triage gate 0 mismatches; `unattributed`: 0 |
| `docs/oracle-reports/lineup-names.md` | Channel-Mapparr loader + rename path | Representative: 145/252 agree (`defaults` and `market-fallback`); disputed: 14/51; `unattributed`: 0 |
| `docs/oracle-reports/lineup-categorize.md` | Channel-Mapparr organize path | Representative: 147/204 agree (`defaults`), 75/108 (`scoped`); disputed: 32/51; `unattributed`: 0 |
| `docs/oracle-reports/lineup-logos.md` | Channel-Mapparr + Lineuparr logo tiers over the recorded tree | 70/254 agree with both oracles (`apply`), 69/254 (`replace`), 70/254 (`order`), 96/254 (`countries`); `unattributed`: 0 |
| `docs/oracle-reports/lineup-create.md` | Channel-Mapparr import + seeder | Representative: 20/40 streams agree on the import decision (CM makes 30 channels, Rust 15 — one per family; CM rerun duplicates 30, Rust 0); disputed: 18/40 (CM 26, Rust 19); stations seed: 1/1; `unattributed`: 0 |
| `docs/oracle-reports/lineup-sync.md` | Lineuparr `_sync_groups` + `_do_sync_channels` | 580 first runs + 580 reruns per side: `ownership-not-adopt` 116, `stable-numbers-on-rerun` 447, `global-number-check` 983, `duplicates-in-lineup` 742; `unattributed`: 0 |

## Development

```bash
cargo test --workspace          # unit, route and mocked-Dispatcharr tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all

./scripts/sim_parity.sh --quick # the Janitor oracle (--features oracle) vs the Python tool, byte for byte
./scripts/one_matcher_check.sh  # no retired profile matcher links into the production binary
python3 -I scripts/parity_sweep.py --kinds normalize,callsign,location,network --limit 300
python3 -I scripts/parity_sweep.py --kinds match,score --limit 300 --pool 60
python3 scripts/parity_sweep_sm.py --limit 300   # the StreamMapparr matcher profile, vs its own code
python3 -I scripts/channel_ops_parity.py   # the cleanup selections and payloads
python3 -I scripts/sort_parity.py          # the streams quality model, 840 cases
python3 -I scripts/match_parity.py         # the streams collector, EPG placeholder fixtures
python3 -I scripts/mirror_parity.py        # Fill Mirror Backups, against the plugin's own action
python3 -I scripts/visibility_parity.py    # Manage Channel Visibility, likewise
python3 -I scripts/probe_parity.py         # probe: commands, parsing and classification vs IC
python3 -I scripts/tools_parity.py         # check_countries, scan_placeholders, test_rules
python3 -I scripts/triage_parity.py        # the seven post-actions and Restore Recovered Channels
python3 -I scripts/clean_parity.py         # lineup clean vs Channel Name Cleaner's own pipeline
python3 -I scripts/lineup_file_parity.py   # the lineup-file loader vs Lineuparr's
python3 -I scripts/oracle_report_lineup_names.py --check   # likewise _categorize, _logos, _create, _sync;
                                                         # without --check each rewrites docs/oracle-reports/

# The scale numbers in the README, and the bridge decision: time the plans on this machine.
cargo run --release -p crosstalk -- bench streams --synthetic
cargo run --release -p crosstalk -- bench streams --live --write-timing --channel-ids <ids> --yes

./scripts/upstream_drift.sh     # what each reference changed since its pin, in files we port
```

The Python plugins live in `reference/` as pinned git submodules — EPG Janitor (the guide
app), Stream-Mapparr and IPTV Checker (the streams app, from phase 2 on), Channel-Mapparr,
Lineuparr and Channel Name Cleaner (the lineup app, from phase 3 on) — so the parity
gates always compare against those exact commits. A fresh clone needs
`git submodule update --init` to get them. See [How matching works](#how-matching-works) for
what is strict-parity and what is deliberately Rust-native, and `scripts/upstream_drift.sh`
— which also runs on the weekly CI schedule — for when upstream has moved past a pin; it
reports each reference separately, against that reference's own list of ported files
(`THIRD_PARTY_NOTICES.md`).

### Layout

```
crates/crosstalk-matching   pure, IO-free matcher: normalisation, similarity, callsigns, identity, pipeline
crates/crosstalk-core       host: config, sub-app trait, Dispatcharr client, AI, store, runner, CSV
crates/guide                the guide sub-app: scope, matching, heal, scan, cleanup presets, watchdog
crates/streams              the streams sub-app: stream selection, settings, match, sort, mirror,
                            probe, triage, restore, visibility, lineup mode, and the bench behind the numbers
                            in Scale
crates/lineup               the lineup sub-app: settings, lineup files, clean, names, categorize,
                            logos, create, sync, tidy, test_name, the named `lineup` scope
crates/crosstalk-web        axum + Askama UI and JSON API
crates/crosstalk            the binary: CLI, registry, daemon
data/                       merged channel databases, stations, aliases, tags, lineups (`data/merged/`)
reference/                  the pinned upstream plugins the oracle reports compare against
scripts/                    the oracle reports, parity gates, corpus labeller and upstream-drift report
```

A new sub-app is a crate implementing `SubApp` plus one line in `crates/crosstalk/src/registry.rs`.

## Licence

crosstalk is MIT; see [LICENSE](LICENSE). The code and data ported from the Dispatcharr
plugins (EPG Janitor, Stream-Mapparr and IPTV Checker, and from phase 3 Channel-Mapparr,
Lineuparr and Channel Name Cleaner) are credited, with each upstream licence text and pinned
reference commit, in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md), which also covers the Alpine ffmpeg package
in the image (GPL). The licence texts of the Rust crates compiled in are generated at build
time into `THIRD_PARTY_LICENSES` (`about.toml`; the allowed licences are `deny.toml`'s list).
All three files ship in the release tarball and in the container image, under
`/usr/share/doc/crosstalk/`.
