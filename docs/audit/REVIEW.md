# Fable review of the audit series (d5195b5..e9313f5)

The end-of-phase review of the seven audit fix commits (write safety, web/auth, secrets,
inputs, performance, supply chain, bridge), done on branch `audit/review` from e9313f5. The
decisions in `plans/security-audit.md` were taken as binding; nothing a decision settled was
changed. Everything below was read in full in the diff, and the parts that write to
Dispatcharr or admit a request were traced end to end against the code as it stands after the
fixes, not against the reports.

## What was checked

**Write safety (first priority).** Every Dispatcharr write path after the fixes:

- `channel_ops::apply` / `apply_with_writer` / `apply_with_writers` / `apply_recorded` and the
  live re-check (`changed_since_plan` / `moved_since_plan`): the `before` each field is compared
  against is the same raw column the plan read (`channel.name`, `channel_group_id`,
  `hidden_from_output`, `channel_number_of` through `number_key`, `logo_id`, the raw
  `epg_data_id`, the ordered `streams` list). Dispatcharr 0.33.0's channel list serializer
  orders `streams` by `ChannelStream.order` (prefetch `order_by("order")`), so two listings of
  an unchanged channel agree and a sort plan is not refused as "changed". Memberships are not in
  the listing and are correctly left out. A cancelled run skips the listing and writes nothing.
  Undo's reversal plan is built from a freshly loaded world and goes through `apply_recorded`,
  so its `before` values are live too.
- `edit_channels_each` / `set_profile_membership_each` / `bulk_set_channel_streams`: a 400
  (whole chunk rejected, nothing landed) is retried one row at a time; any other failure fails
  only its chunk; cancellation is read before every request; rows are attributed by channel id,
  so a channel carrying two rows of one edit gets one answer for both.
- `apply_recorded`'s checkpoints: rows are recorded before the first write and re-recorded
  after each stage with the same ordinals. A crash between stages leaves the landed stages `ok`
  and the rest `planned`, which undo reads correctly (only `ok` rows). A failure recording the
  final rows surfaces as the run's error after the writes landed, with the last checkpoint in
  place.
- `load_owned`: the 404 confirm is sound against 0.33.0 (the channel, group and logo detail
  routes apply no filter an admin listing does not; a hidden channel is in the listing because
  of `visibility_filter=all`, so it never reaches the GET). Dry runs write nothing. The empty-
  listing refusal and the `max_forget` cap are both kept (see fixed, below).
- `clear_hidden`'s guard, the redirect policy (`Policy::none()`, any 3xx is an error on GET and
  write), `rename_group` at `Retry::Once`, `retire_dead_runs` (a live CLI run with a fresh
  heartbeat on the same machine keeps its row and slot; a reused or dead pid, a stale or
  missing heartbeat retires), the S7 re-check before `clear_missing`/`tag_missing`, the S6
  scope-warning refusal, the N7 tidy/undo history rule.
- Bridge writes: the plugin's `set_stream_lists` deletes only the rows of streams no longer in a
  channel's list, keeps a kept stream's row, runs in one `transaction.atomic()` per request;
  `clear_channel_groups` is one `UPDATE`; `upsert_epg` is gated on the operator's source list,
  replaces only `[window.start, window.end)` of the named entries and never deletes an
  `EPGData` row. The Rust client fails a chunk whose answer does not add up ("outcome
  unknown"), fails only its own rows on a failed chunk, and never sends a partial list for a
  channel (a list over 1,000 streams fails on its own, unsent). `ChannelStream`, `Stream`
  (`stream_stats`, `stream_stats_updated_at`), `EPGData` (`tvg_id`, `name`, `icon_url`,
  `epg_source`) and `ProgramData` (`tvg_id`, `sub_title`, `custom_properties`, `title` 255)
  match the 0.33.0 models; `rollup_catchup_for_channels(channel_ids)` exists with that
  signature; the run route passes `params` through DRF unchanged and the plugin gets
  `context["settings"]` merged with the manifest defaults.

**Web layer as internet-facing.** Read in full and exercised against a local daemon with a
temporary config (`trusted_proxies = ["127.0.0.1"]`, a 43-byte token), with curl:

- gate order (limiter check, then credential, before Host/Origin/CSRF/body): 401 with
  `WWW-Authenticate: Bearer` for anonymous; ten wrong Bearers anywhere lock the client out of
  everything, the eleventh answers 429 even with the right token, `Retry-After` is sent; a
  forwarded client from the trusted proxy has its own bucket; a wrong `POST /login` and a wrong
  Bearer POST without CSRF are 401, never 403;
- sessions: a 64-hex id in `crosstalk_session; HttpOnly; SameSite=Strict`, `Secure` and HSTS
  only when the trusted proxy says https (or configured); the legacy `crosstalk_token` cookie
  is refused (401, or a 303 to `/login` for a browser) and expired on login; logout removes the
  session server-side (a later request is 401); a session's forms carry their own CSRF token
  and another session's token is refused; the hook refuses a session (401) and takes a Bearer
  (202); a foreign `Origin` on a write is 403 for Bearer and session alike; `/login` body over
  4 KiB is 413; `Cache-Control: no-store` everywhere but `/static/*` (`public, max-age=86400`);
  CSP, COOP, Permissions-Policy, X-Frame-Options present;
- limiter keying (`client_ip`: rightmost non-trusted XFF hop, else X-Real-IP, only from a
  trusted peer; IPv6 by /64; 10k-client cap evicting the least recently seen tenth; one sweep
  per window), `trusted_proxies` parsing (own matcher, v4+v6, mapped v4 canonicalised,
  validated at load and on reload), `security_errors` applied by `validate`, `serve` and the
  settings-save reload, `public_url` origin normalisation (default ports made explicit, case
  folded), webhook `max_settle` and the log cap, body limits per route, 30 s timeout, 256 in
  flight.

**Secrets.** `redact::urls_to_hosts` / `url_without_secrets` coverage (userinfo with `/` or `?`
in a password, `%3A%2F%2F` spellings, IPv6 hosts, non-host authorities replaced whole);
`safe_body` on every quoted Dispatcharr body; the manual `Debug` impls; `resolve_secret`
precedence (environment wins, blank falls back) and `raw_string_overrides`; the settings save
loading the file-only config so an environment secret is never written back; the bridge
secret's status line, `Secret` field kind never read from a form, `bridge secret`/`rotate`
refusing when the variable supplies it; the plugin logging only op, counts and refusal codes;
`ping` answering only `configured: env|settings|none`; `reqwest`/`hyper` held at `info`.

**Inputs.** The scheme allow-list and `-i`, the protocol whitelist against ffmpeg's behaviour
(see fixed: `data`), `fetch_validated` (invalid never stored or served fresh, stale fallback,
32 MiB cap checked from `Content-Length` and while streaming), the property tests: the webhook
readers assert round trip, form/JSON agreement, non-UTF-8 to empty, trimmed non-empty bounded
names free of control characters, shape resolution only to configured events; `parse_lineup`,
`try_from_tree`/`build_logo_url` and the CSV preamble/`safe_cell` properties assert the
invariants D-inputs lists (20, 16 and 9 `prop_assert`s respectively).

**Performance.** `score_core`: every change is a precomputation of a per-pair quantity
(`nospace` as a char filter pinned against the old regex over every scalar value, `norm_chars`,
`processed_chars`, `anchor_base` = `normalize_callsign(base)` kept separate because the strip
is not idempotent, `first_base` once per query, stack rows in the similarity with a heap
fallback tested at the boundary); no comparison or threshold moved, and the parity sweeps and
oracle reports in CI gate the scores. `seed_programs`: a source with nothing ending at or after
`window.start` has no programme in the window for any entry, which is exactly `has_programs`'s
answer for that source (its search filters on `epg_source` and `end_after`); an airing
programme with the entry's own `tvg_id` whose span overlaps `[start, end]` is a row the search
would find; parsing markers, spanless programmes and errors settle nothing; dummy sources and
sources without a type stay on the per-entry search; memo keys are the same
`(tvg_id, source, start, end)`. E-B2: the family warm overlaps the network stages and is
awaited before the run returns, so the rows are stored before a caller can read them
(`full_sync_workflow` reads them back).

**Supply chain / bridge.** `ci-gate` polls the CI run for the same SHA, push event and tag ref
with `actions: read`; PR caches are isolated by tag and tag builds run without a cache
volume; attestation permissions are on `image` and `release` only; every `uses:` is SHA
pinned. Canonical JSON: both implementations agree on code-point key order, `-0` as `0`,
integers outside 2^53 refused, ECMAScript float layout (`1e+21`, `1e-7`, `0.000001`),
lowercase `\u00xx` for other controls, literal non-BMP; NaN cannot arrive (DRF's parser
refuses it, serde never produces it); duplicate keys cannot reach either side's serializer
(both parse to a map first, last value wins on both). Chunking: at most 2,000 channels /
20,000 stream refs / 5,000 streams / 2,000 entries / 50,000 programmes and under 1 MB of
serialized items per request, `index - start >= max_items` closing a chunk at exactly the cap.

**Consistency.** README claims against code (token floor, cookie, trusted proxies,
`max_settle`, whitelist, bridge commands), the one-matcher rule (lineup clean uses
`streams::selection::translate_replacement`; no second translator), `cargo fmt`, `clippy
-D warnings`, no leftover `<measured>`; the one `TODO` left was in `deny.toml` and was wrong.

## What was fixed (commits on `audit/review`)

| Commit | Area | Problem | Fix |
|---|---|---|---|
| 85425f6 | write safety | The B2 `max_forget` cap (more than max(25, 5%) of an app's owned ids missing refuses the reconcile) had no exit: after a deliberate mass deletion in Dispatcharr, every lineup create/categorize/logos/sync run refused for good. | The refusal names `crosstalk ownership forget-deleted --app <app>`, which runs the same reconcile without the per-run cap: each missing id is still GET-confirmed, only a 404 forgets, an empty listing is still refused, never a dry run. `JobContext::with_store`. Tests: the message names the command; the unbounded reconcile forgets the 404s, keeps what answers, refuses an empty listing; CLI parse. |
| 1b1da66 | bridge | Nonce TTL was exactly 2 x window: a request stamped at now + 300 still passes the timestamp check at now + 600, when the nonce had just expired, so that one second allowed a replay. | TTL = 2 x window + 1. Test: replayed at now + 600 is `replayed`, at now + 601 `stale`. `docs/bridge.md` updated. |
| ad5a682 | inputs | `-protocol_whitelist` left out `data`: ffmpeg's own default nested whitelist for an http input includes it, and some HLS streams carry their AES key or a sub-playlist as an inline `data:` URI, so those read as dead. | `data` added (reaches no file, no network) to the constant, the parity script and the docs; the test pins every local protocol out and ffmpeg's http default in. Probe parity: 0 mismatches over 23 cases. |
| 92a59c1 | supply chain / docs | The CI docker smoke jobs start the daemon on `0.0.0.0` with the 8-byte `ci-token`, which the daemon now refuses (A2), so the docker job would fail on the first push of the series. `deny.toml` had a TODO saying the basic-toml ignore goes with the askama upgrade; the upgrade landed and `askama_derive` 0.16.1 still pulls it. B/E/F status notes said "uncommitted" / "numbers pending" / "DEFERRED". | 41-byte token in both smoke runs; the ignore keeps a reason that says what it is (build-time config parsing of this repository's own file); F-S8 marked PARTIAL with the `cargo tree` evidence; headers corrected. |
| (the last commit on `audit/review`) | docs | | README command row; this file. |

Gates run on the final tree: `cargo fmt --all -- --check`, `cargo clippy --locked --workspace
--all-targets -- -D warnings`, `cargo test --locked --workspace` (all green; lineup 148 tests in
184 s), `python3 -I -m unittest discover -s bridge/tests` (35 tests), `python3 -I
scripts/probe_parity.py` (0 mismatches). The four code commits touch disjoint files and each
was tested with its own targeted tests before the full run; the full gate ran once on the
final tree rather than once per commit. `actionlint` reports only the custom `nscloud-*`
runner labels (no `actionlint.yaml` lists them), as before the series. Nothing was pushed.

## What was left, and why

- **`retire_dead_runs` and two containers on one data volume.** `host_id` is the kernel boot id,
  which every container on a host shares, and a container's daemon is pid 1. A daemon starting
  in one container while a `crosstalk run` is live at pid 1 in a sibling container on the same
  volume would retire that live run. Sharing one store between containers is not a supported
  layout (README: one machine), the fix (hostname in `host_id`) would delay retiring a crashed
  daemon's rows by up to five minutes after a container is recreated, which blocks the app's
  first scheduled run; left as a documented residual.
- **A crashed CLI run blocks its app until a daemon starts.** `open_shared` never retires; with
  heartbeats it now could, safely. Pre-existing behaviour, out of scope for a review commit.
- **`forgive` after a correct Bearer pops the newest hit**, which under a concurrent wrong
  attempt from the same client key could be that attempt's. Same client, one event, no effect
  on anyone else.
- **CSRF `body_has_token` is a byte scan over a body up to 256 KiB** per form POST; linear and
  bounded, fine.
- **The `data` protocol in a `-protocol_whitelist` also admits `data:` as a top-level input**,
  but the scheme allow-list in `is_probeable_url` refuses `data:` before any tool runs, so only
  nested references can use it.
- **E-perf open items** (per-run `country_from_name` memo, `probe write_stats` barrier, undo
  reading rows twice) are performance nits under the SLA; left.

## Decisions I would revisit

- **A9, the one-token rule for the webhook.** ADOPTED afterwards in `3753591`: `[hooks] token`
  is a hook-only credential (see `docs/audit/A-web.md`, A9). The original note: Decided and
  respected. The webhook now goes through the same gate and limiter and refuses sessions, which is the strongest shape one
  token allows. But Dispatcharr Connect stores that token in its own database and sends it on
  every delivery; a leak there is the master token. A second, hook-only credential with the
  same limiter would confine that leak to "trigger a configured refresh". Worth revisiting once
  the first real install is behind a proxy.
- **Sessions in memory.** ADOPTED afterwards in `d00a1ec` on
  `fix/web-sessions-hook` ("web: signed session cookies that survive a restart"): signed
  cookies under `<data_dir>/session.key`, sign-outs persisted in SQLite, `crosstalk sessions
  revoke-all` (see `docs/audit/A-web.md`, A5). The original note: A restart signs every
  browser out, which is the safe default and was chosen knowingly; for a daemon that is restarted by every settings change that needs one, a
  signed session cookie (HMAC of id + expiry under a startup key persisted in the data dir)
  would survive restarts without storing anything a leak could use.
- **The `max_forget` cap as a hard refusal.** Kept, with the new exit, because the decision
  weights wrong writes over availability. An alternative that needs no operator action is to
  forget at most `cap` confirmed-404 ids per run and report the rest kept: it converges after a
  mass deletion, but a scheduled run against the wrong instance would then leak `cap`
  duplicates per run instead of none, which is why it was not chosen.

## Residual risks for a first run against a real Dispatcharr

1. **`ChannelProfile.channels` and `Channel.streams` are now required in the listings.** 0.33.0
   sends both; an older or forked build that omits either fails every lineup/guide run that
   loads profiles or channels with a clear error rather than a wrong write. Check `validate`
   first.
2. **The live re-check lists every channel once more before each apply** (one paginated read
   of ~20k channels at scale). Cheap against the SLA, but on a slow proxy it adds the listing's
   latency to every apply, and a listing that fails makes the apply write nothing (by design).
3. **The bridge's unverified assumptions** (docs/bridge.md "Assumptions"): a per-process Django
   cache would make nonces per worker (replays bounded by idempotency and the 5-minute window);
   the run route's permission; `DATA_UPLOAD_MAX_MEMORY_SIZE` and the image's proxy limit on a
   1 MB body; bulk ORM paths skipping `post_save` (the catch-up roll-up is called explicitly,
   the websocket refresh is not). Start with `crosstalk bridge ping`, then one small
   `streams.sort_apply`, and reload the Dispatcharr page to see the lists.
4. **`seed_programs` trusts `GET /epg/programs/search/?epg_source=<id>&end_after=<start>`
   returning an empty page only when the source really has nothing in the window.** A build
   that ignores the `epg_source` filter makes the probe return more, never less, so the
   direction is safe; a build that answers an empty page for a transient reason during a
   refresh memoises "no" for that source for the run, exactly as the per-entry search would
   have. `clear_missing` re-checks per channel in its own run before writing.
5. **Behind a proxy that does not send `X-Forwarded-Proto`**, cookies lack `Secure` and HSTS is
   not sent unless `secure_cookies`/`hsts` are set; the README says so, but a first deployment
   should set both explicitly.
6. **Undo of a group rename is still not possible** (N1, PARTIAL): a `rename_group` that lands
   cannot be reversed by `crosstalk undo`.
