# crosstalk

A Rust daemon that hosts tools for [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr).
One binary, one config directory, one scheduler and one web UI serve several sub-apps, each
defined by what it manages rather than by the plugin it replaces. **guide** is the one that
ships today: a fast, multi-threaded EPG matcher that assigns guides to channels, repairs or
removes assignments that carry no programme data, and keeps the EPG sources themselves fresh.

It replaces the Python EPG Janitor plugin, which runs inside Dispatcharr and reads its database
through the Django ORM. crosstalk is a separate process that talks to Dispatcharr over its REST
API, which means it survives Dispatcharr upgrades, runs its matching across every core, and can
serve several tools from one binary.

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
- **Optionally asks an AI** about the cases the heuristics are unsure of — and never about the
  ones they are sure of. See [AI verification](#ai-verification).

## Roadmap

Three more sub-apps are planned, again defined by what they manage: **streams** (which streams
have a channel, in what order, and whether they work), **lineup** (the channel list itself:
names, numbers, groups, logos, creation) and **events** (time-bound event channels). Each lands
as its own phase of [`plans/plugin-consolidation.md`](plans/plugin-consolidation.md), which
also maps every action of the remaining Dispatcharr plugins onto those four apps and records
what is deliberately not ported. Nothing here promises a date.

## Quick start

```bash
# 1. Configure
cp config/crosstalk.example.toml config/crosstalk.toml
cp config/guide.example.toml config/guide.toml

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

The UI listens on <http://localhost:8730>.

> **Back up your Dispatcharr database first.** An apply rewrites guide assignments on channels.

## Commands

| Command | What it does |
|---|---|
| `crosstalk serve --config <dir>` | Run the daemon: REST API, scheduler and web UI |
| `crosstalk validate --config <dir>` | Check the configuration files and each app's settings |
| `crosstalk run <app> <action> [--dry-run] [--yes]` | Run one action once and exit |
| `crosstalk exports clear [--app <id>] [--yes]` | Delete the CSV reports the sub-apps wrote (the UI's **Clear exports** button does the same) |
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
  stream groups, the stream-name regex rules — then matching, sorting, probing and triage.
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

Every action below belongs to the `guide` app (`crosstalk run guide <action>`). The `streams`
app declares its own list in `crates/streams/src/lib.rs` and currently exposes `validate`,
`status` and the `match_*` and `sort_*` pairs; its `mirror_*`, `probe`, `triage_*`,
`restore_*` and `visibility_*` pairs arrive with the rest of phase 2.

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

Button colour is the consequence, and it is the same convention the Python plugin used: blue
reads, orange writes data but removes no assignment, red can move an assignment. Orange and red always confirm.

Every action that writes is a `<verb>_preview` / `<verb>_apply` pair, so `--dry-run` on the
apply runs the preview and a schedule key names the half you mean. The cleanup actions select
from the **last finished `scan`** rather than from a results file: there is no age limit,
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

## Web UI

- Dashboard: apps, schedules and their next run, recent runs.
- App page: actions with their colours, a confirmation for the ones that write, the run in
  progress with a cancel button.
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
above hold it to byte equality. Rows up to the watchdog are differences from EPG Janitor;
the `streams.match` rows are differences from Stream-Mapparr.

| Difference | Reason |
|---|---|
| `streams.match_apply` only writes channels whose list changed | The plugin deletes and recreates every matched channel's list; crosstalk plans a stream-list write only where the list actually differs. Same end state, so a second run over an unchanged library writes nothing. |
| `streams.match_preview` drops the plugin's threshold-analysis advice rows | Preview also re-ran the pipeline at 5-point thresholds and collected by exact match against the winning stream name; the plugin itself notes that path disagrees with the real matcher. Those rows were advice, never a write. |
| With `filter_dead_streams` on, match also excludes store probe verdicts | The plugin only reads `stream_stats` (empty, 0 width/height, `0x0`); crosstalk reproduces that test exactly and additionally skips streams the probe recorded as `dead` in the store — the same verdict, kept where a rerun can see it. |
| Appending streams renumbers the channel's list | The plugin's append gives the new rows order values that collide with the existing rows; crosstalk writes the same membership in the same playback order, numbered from zero. |
| EPG-override channels are skipped by `match` | A channel whose guide was set by hand in Dispatcharr keeps it; the plugin re-matches and overwrites. Skipping is safer, and the CSV reason says so. |
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

## Development

```bash
cargo test --workspace          # unit, route and mocked-Dispatcharr tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all

./scripts/sim_parity.sh --quick # the oracle: Rust vs the Python tool, byte for byte
python3 scripts/parity_sweep.py --kinds normalize,callsign,location,network --limit 300
python3 scripts/parity_sweep.py --kinds match,score --limit 300 --pool 60
python3 -I scripts/channel_ops_parity.py   # the cleanup selections and payloads

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
crates/streams              the streams sub-app: stream selection, settings, and (phase 2) match, sort,
                            mirror, probe, triage, restore, visibility
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
