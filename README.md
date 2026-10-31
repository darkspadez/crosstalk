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
docker compose up -d
```

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

A high-confidence match never reaches the model.

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

The port is faithful to the Python matcher, and that is checked rather than asserted: the
`crosstalk simulate` command produces **byte-identical** reports to the plugin's own offline
tool, and `scripts/parity_sweep.py` compares the primitives over hundreds of thousands of cases.

## Development

```bash
cargo test --workspace          # unit, route and mocked-Dispatcharr tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all

./scripts/sim_parity.sh --quick # the oracle: Rust vs the Python tool, byte for byte
python3 scripts/parity_sweep.py --kinds normalize,callsign,location,network,match,score
```

The Python plugin lives in `reference/` (git-ignored) as the source of truth for parity.

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
