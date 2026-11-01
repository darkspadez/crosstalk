# crosstalk

A Rust daemon that hosts tools for [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr),
with **EPG Janitor** as its first sub-app: a fast, multi-threaded EPG matcher that assigns
guides to channels and repairs assignments that carry no programme data.

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
- **Optionally asks an AI** about the cases the heuristics are unsure of — and never about the
  ones they are sure of. See [AI verification](#ai-verification).

## Quick start

```bash
# 1. Configure
cp config/crosstalk.example.toml config/crosstalk.toml
cp config/janitor.example.toml config/janitor.toml

# 2. Point it at Dispatcharr and give it a key
#    config/crosstalk.toml: base_url = "http://dispatcharr:9191"
#    and either api_key = "..." or the CROSSTALK_DISPATCHARR_API_KEY environment variable

# 3. Check the configuration
crosstalk validate --config config

# 4. Preview a run — this writes a CSV and changes nothing
crosstalk run janitor automatch_preview --config config

# 5. Apply it, once the preview looks right
crosstalk run janitor automatch_apply --config config --yes

# 6. Or run the daemon and use the web UI
crosstalk serve --config config
```

With Docker:

```bash
cp docker-compose.example.yml docker-compose.yml
cp config/crosstalk.example.toml config/crosstalk.toml
cp config/janitor.example.toml config/janitor.toml

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

Tagged releases (`v*`) publish versioned images plus `latest` to
`ghcr.io/darkspadez/crosstalk`, so you can `docker pull` instead of building.

The UI listens on <http://localhost:8730>.

> **Back up your Dispatcharr database first.** An apply rewrites guide assignments on channels.

## Commands

| Command | What it does |
|---|---|
| `crosstalk serve --config <dir>` | Run the daemon: REST API, scheduler and web UI |
| `crosstalk validate --config <dir>` | Check the configuration files and each app's settings |
| `crosstalk run <app> <action> [--dry-run] [--yes]` | Run one action once and exit |
| `crosstalk simulate --channels <csv> --streams <m3u>` | Offline stream-to-channel simulation, for tuning without touching a live install |

`crosstalk run` refuses an action that changes data unless `--yes` is passed, because a
one-shot run has no dialog to confirm in.

## Configuration

Two files, split by who owns the option:

- **`crosstalk.toml`** (host): the Dispatcharr connection, AI providers, the HTTP listener, the
  thread pool, the directories, and the **shared name-matching options** — the normalisation
  toggles, which country databases to load, and custom aliases. Those are shared because two
  apps disagreeing about whether `[HD]` is stripped would give two different answers for the
  same channel.
- **`janitor.toml`** (sub-app): scope filters, confidence thresholds, the AI gate, CSV
  retention.

Every value can be overridden by an environment variable named `CROSSTALK_<SECTION>__<KEY>`,
with a double underscore between levels:

```bash
CROSSTALK_SERVER__PORT=9100
CROSSTALK_DISPATCHARR__API_KEY=...
CROSSTALK_AI__PROVIDERS__TYPESAFE__API_KEY=...
```

Secrets should live in the environment, not the file: the daemon refuses to save a
configuration that had environment overrides applied, so a token cannot leak into a file by
accident.

The web UI writes settings back through `toml_edit`, preserving your comments and ordering.
Host settings saved in the UI hot-reload: the next run uses them and schedules re-arm, with
no daemon restart (a run already in progress finishes with the settings it started with).

## Actions

| Action | Colour | What it does |
|---|---|---|
| `validate` | blue | Check the settings for consistency, without touching Dispatcharr |
| `status` | blue | Show what the last run did |
| `automatch_preview` | blue | Score and write a report; changes nothing |
| `automatch_apply` | **red** | Assign guides, overwriting an existing assignment when a new match clears the threshold |
| `heal_preview` | blue | Look for replacements for empty assignments; changes nothing |
| `heal_apply` | **orange** | Replace empty assignments. Never removes an assignment it cannot prove is empty |

Button colour is the consequence, and it is the same convention the Python plugin used: blue
reads, orange writes channel data, red can move an assignment. Orange and red always confirm.

## AI verification

Matching is a yes/no decision, so the model is asked a yes/no question — not for a paragraph of
prose. Two things happen only:

1. **Verify** — the best heuristic score lands in the configured band (default 80–94). The score
   is plausible and could be wrong, which is where a second opinion is worth a request.
2. **Rescue** — nothing reached the band, but a candidate cleared the rescue floor (default 60).

The contract, precisely:

- A heuristic result at or above `automatch_confidence_threshold` (default 95) never
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
```

```toml
# janitor.toml
[ai]
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
"janitor.automatch_apply" = "0 4 * * *"
"janitor.heal_apply" = "0 6 * * *"
```

Five fields (`minute hour day month weekday`) or six (with seconds). Schedules fire in **UTC**,
and the dashboard shows each one's next run so you can see it was understood. A schedule naming
a preview action stays a preview.

## Web UI

- Dashboard: apps, schedules and their next run, recent runs.
- App page: actions with their colours, a confirmation for the ones that write, the run in
  progress with a cancel button.
- Run page: live status, the full result table with the reason for every row, the AI decision
  where there was one, and the CSV download.
- Settings: every option with the help text the form shows, written back to the TOML files.

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
`crosstalk simulate` produces **byte-identical** reports to the plugin's own offline tool
across four configurations: default flags and `--custom-aliases` (which change matching
output), `--no-ignore-quality` (which catches Rust wrongly applying the flag), and
`--countries US` (which proves flag acceptance and call-sign DB merge parity). Duplicate
stream names in the sim corpus exercise parser dedupe — they are removed before matching,
so duplicate *candidates* reach the matcher in the automatch/heal unit tests for the H6
duplicate-name fix, not via sim. `scripts/parity_sweep.py` compares the primitives over
hundreds of thousands of cases in a full local run (no `--limit`); the bounded CI sweeps
cover ~4,800 normalize, ~900 per-name (callsign/location/network), and ~1,200
match/score cases. Both gates run in CI against the plugin pinned as a submodule in
`reference/`.
- **Rust-native actions**: `automatch` and `heal` reuse the parity-checked pipeline but
make their own product decisions. They are not ports, and row-for-row equality with the
plugin is not claimed for them.

Deliberate divergences from the plugin (actions only, never the pipeline):

- **EPG-override channels are skipped**: a channel whose guide was set by hand is left
alone by automatch; the plugin re-matches it. Skipping is safer, and the CSV reason says so.
- **AI promotion within the verify band**: with the AI gate on, an AI-accepted candidate can
be assigned while its heuristic score (60–94) sits below `automatch_confidence_threshold`
(default 95). The threshold still gates every heuristic assignment; AI may promote inside
the band but never veto a heuristic win, and heal never consults AI at all.
- **Heal preview statuses are plugin-style**: a working replacement below the threshold is
reported as `SKIPPED_LOW_CONFIDENCE` / `REPLACEMENT_PREVIEW` with its real score, but
never applied.
- **Sourceless candidates are skipped**: entries with no EPG source are not considered as
replacements; the plugin validates by EPG id and can accept one.
- **Automatch reporting walks only validated candidates**: when nothing validates, the row
reports the top-ranked candidate and says no programme data was found, where the plugin
reports the first programme-carrying candidate regardless of threshold.

## Development

```bash
cargo test --workspace          # unit, route and mocked-Dispatcharr tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all

./scripts/sim_parity.sh --quick # the oracle: Rust vs the Python tool, byte for byte
python3 scripts/parity_sweep.py --kinds normalize,callsign,location,network --limit 300
python3 scripts/parity_sweep.py --kinds match,score --limit 300 --pool 60
```

The Python plugin lives in `reference/` as a pinned git submodule: the parity gates compare
against that exact commit. See [How matching works](#how-matching-works) for what is
strict-parity and what is deliberately Rust-native.

### Layout

```
crates/crosstalk-matching   pure, IO-free matcher: normalisation, similarity, callsigns, pipeline
crates/crosstalk-core       host: config, sub-app trait, Dispatcharr client, AI, store, runner, CSV
crates/janitor              the EPG matcher sub-app: scope, auto-match, heal, AI gate, settings
crates/crosstalk-web        axum + Askama UI and JSON API
crates/crosstalk            the binary: CLI, registry, daemon
data/                       embedded channel databases, FCC callsigns, aliases
```

A new sub-app is a crate implementing `SubApp` plus one line in `crates/crosstalk/src/registry.rs`.

## Licence

MIT.
