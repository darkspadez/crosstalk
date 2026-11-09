# The crosstalk bridge: contract

`crosstalk-bridge` is a Dispatcharr plugin (`bridge/` in this repository, shipped as
`crosstalk-bridge-<version>.zip` with every release) that performs four bulk writes
Dispatcharr's REST API cannot, and nothing else. crosstalk is the only intended caller. This
document is the contract between the two: `bridge/plugin.py` and
`crates/crosstalk-core/src/bridge.rs` implement it, and `bridge/tests/vectors.json` pins the
parts both must compute identically.

Protocol version: **1**. Plugin version: **0.1.0**.

## Why a plugin

| Write | Over REST | Through the bridge |
|---|---|---|
| A channel's ordered stream list | `edit/bulk` rows (falls back to one PATCH per channel for a rejected chunk) | `ChannelStream` rows rewritten in one transaction, up to 2,000 channels per request |
| `Stream.stream_stats` + `stream_stats_updated_at` | one PATCH per stream | 5,000 streams per request, one `bulk_update` |
| `Channel.channel_group` = NULL ("No Group") | impossible: `ChannelSerializer.channel_group_id` has no `allow_null`, so a null is a 400 | one `UPDATE` |
| `EPGData` + `ProgramData` for a generated guide | no endpoint creates programme rows | upsert by `(tvg_id, epg_source)`, programmes replaced inside one window |

Everything else crosstalk does stays on REST.

## Transport

A request is Dispatcharr's own plugin-run call, made with the `[dispatcharr]` credential crosstalk
already uses:

```
POST {base_url}/api/plugins/plugins/{plugin_key}/run/
{"action": "<op>", "params": {"envelope": {...}, "mac": "<hex>"}}
```

Dispatcharr runs `Plugin.run(action, params, context)` in-process and answers
`{"success": true, "result": <response>}`. A plugin that is not installed answers 404, a disabled
one 403; crosstalk reports either as "the bridge is unavailable".

`plugin_key` is the plugin's folder name as Dispatcharr derived it. The zip import lowercases the
folder name and turns `-` and spaces into `_`, so the release zip installs as
**`crosstalk_bridge`**, the default `[bridge] plugin_key`. A folder copied in by hand as
`crosstalk-bridge` keeps its `-`; crosstalk's start-up ping tries the other spelling after a 404
and says which one answered.

## The envelope

```json
{
  "v": 1,
  "ts": 1800000000,
  "nonce": "00112233445566778899aabbccddeeff",
  "op": "set_stream_lists",
  "payload": { ... }
}
```

| Field | Rule |
|---|---|
| `v` | the integer `1` |
| `ts` | integer unix seconds when crosstalk signed it |
| `nonce` | 16 random bytes from the OS CSPRNG, 32 lowercase hex digits, never reused |
| `op` | one of the operations below, and equal to the request's `action` |
| `payload` | an object; its shape is the operation's |

The envelope has exactly these five keys; anything else is refused.

`mac` is the lowercase hex HMAC-SHA256 of **the canonical JSON of the envelope** (UTF-8 bytes),
keyed by the UTF-8 bytes of the shared secret with surrounding whitespace removed. It is 64
characters.

### Canonical JSON

"Canonical JSON" here is one exact byte string per JSON value, so both sides compute the same
MAC from the same data however it was serialized on the wire:

1. No whitespace anywhere outside strings.
2. `null`, `true`, `false` as themselves.
3. Objects: members sorted by key, comparing keys as sequences of Unicode code points
   (equivalently, their UTF-8 bytes), written `{"k":v,...}`. Keys are strings; duplicates cannot
   occur (the value has already been parsed). This differs from RFC 8785, which sorts by UTF-16
   code units: `"\uffff"` sorts before `"😀"` here.
4. Arrays: elements in order, `[a,b,...]`.
5. Strings: `"`, then each character: `"` as `\"`, `\` as `\\`, U+0008 as `\b`, U+0009 as `\t`,
   U+000A as `\n`, U+000C as `\f`, U+000D as `\r`, any other character below U+0020 as `\u00xx`
   with lowercase hex, and every other character (including `/`, U+007F, U+2028, U+2029 and
   everything outside the BMP) as itself in UTF-8; then `"`. A lone surrogate has no canonical
   form.
6. Integers (a JSON number without fraction or exponent, as a parser reads it) in
   ±(2^53 − 1): plain decimal, `-` for negatives, no leading zeros, `-0` as `0`. Outside that
   range there is no canonical form.
7. Any other number: a finite IEEE 754 double, written the way ECMAScript's
   `Number.prototype.toString` writes it (RFC 8785 §3.2.2.3): the shortest decimal digits that
   read back as the same double, then — with `n` the decimal exponent such that the value is
   `0.d1d2… × 10^n` and `k` the digit count — `digits + zeros` when `k ≤ n ≤ 21`, `d.ddd` when
   `0 < n ≤ 21`, `0.000ddd` when `−6 < n ≤ 0`, and `d.ddde±x` otherwise. So `2.0` is `2`, `-0.0`
   is `0`, `1e21` is `1e+21`, `1e-7` is `1e-7`, `0.000001` stays as written. NaN and infinities
   have no canonical form.
8. Nesting deeper than 32 levels has no canonical form.

A value without a canonical form is refused (`bad_request`); crosstalk never sends one.

`bridge/tests/vectors.json` holds canonical forms (key order, escapes, number thresholds,
extremes), values that must be refused, and five signed envelopes with their MACs, one per
operation. It was generated once from `bridge/plugin.py` 0.1.0, its number layouts were checked
against a JavaScript engine, and it is frozen: the Python tests and the Rust tests both check it,
so a change to either implementation that drifts from the other fails a test.

## Verification, in order

The plugin checks each request in this order and refuses the whole request at the first failure
(see "Responses"):

1. `params` is exactly `{envelope, mac}`; `mac` is 64 lowercase hex digits; the envelope has
   exactly its five keys. (`bad_request`)
2. A secret is configured and at least 32 characters. (`not_configured`)
3. The MAC matches under the current secret, or under the previous secret while the rotation
   grace is active. Every candidate is computed and compared with `hmac.compare_digest`, so the
   time taken does not reveal which (if any) matched. (`bad_mac`)
4. `v` is 1, `ts` is an integer within **±300 seconds** of the plugin's clock. (`bad_request`,
   `stale`)
5. `op` equals the request's `action` (`bad_request`) and is on the allow-list (`unknown_op`).
6. The nonce is 32 lowercase hex digits and has not been seen inside the window. (`bad_request`,
   `replayed`) A nonce is recorded only after the MAC holds, so unsigned traffic cannot fill the
   cache, and before the payload is validated, so a request that fails validation still cannot be
   replayed. Nonces live in Django's cache (`cache.add`, shared by every worker when Dispatcharr's
   cache is Redis) for 601 s (one second past the last moment a request stamped at the far edge of the window could still pass the timestamp check), and always in a per-process LRU of up to 200,000 entries as well.
7. The payload matches the operation's schema and limits. (`invalid`, `too_large`)
8. The operation runs inside one `transaction.atomic()`.

## Operations (the allow-list)

There are exactly five. Every write is idempotent: applying the same request twice leaves the
same state (the second answer reports the rows as `skipped`). Ids are positive integers and may
not repeat within a request.

### `ping`

Payload `{}`. Writes nothing. Answers, besides the common fields:

```json
{"protocol": 1, "capabilities": ["ping", "set_stream_lists", "set_stream_stats",
 "clear_channel_groups", "upsert_epg"], "limits": {...}, "key": "current",
 "configured": "env", "grace": "none", "epg_sources": [12]}
```

`key` is which of the plugin's secrets verified the request (`current` or `previous`);
`configured` is where the current secret comes from (`env`, `settings`, `none`) and nothing more:
no secret, digest, length or prefix is ever in an answer; `grace` is the rotation state (`none`,
`active`, `expired`, `invalid`); `epg_sources` are the source ids `upsert_epg` may write.

### `set_stream_lists`

```json
{"lists": [{"channel_id": 7, "stream_ids": [30, 20, 10]}, {"channel_id": 8, "stream_ids": []}]}
```

Replaces each named channel's `ChannelStream` rows so its streams are exactly `stream_ids`, with
`order` 0, 1, 2… in list order. Rows for streams no longer listed are deleted (the only delete
this operation makes, and the exact meaning of "replace"); kept streams keep their row with a
new `order`; new streams get a new row. An empty list leaves the channel with no streams.
Streams and channels themselves are never created or deleted. Afterwards Dispatcharr's own
`rollup_catchup_for_channels` runs for the changed channels (a bulk write skips the signal that
normally does it).

Per channel: an unknown channel, or a list naming an unknown stream, is an error for that channel
and the channel is not touched; a channel already holding exactly that list is skipped.

### `set_stream_stats`

```json
{"stats": [{"stream_id": 30, "stream_stats": {"width": 1920, "source_fps": 29.97},
            "updated_at": 1800000000}]}
```

Sets `Stream.stream_stats` to the given object (or `null`) and `stream_stats_updated_at` to
`updated_at` (unix seconds, UTC), with one `bulk_update`. The object replaces the stored one:
crosstalk merges before it sends (the probe re-reads the current stats right before the write),
so the write itself is idempotent. Per stream: unknown is an error; identical stats and
timestamp is skipped.

### `clear_channel_groups`

```json
{"channel_ids": [7, 8, 9]}
```

Sets `Channel.channel_group` to NULL ("No Group") with one `UPDATE`. Per channel: unknown is an
error; already in no group is skipped. A `ChannelOverride` group, if any, is not touched (the
same column the REST edit writes).

### `upsert_epg` (for phase 4's generated guides)

```json
{"epg_source_id": 12,
 "window": {"start": 1800000000, "end": 1800086400},
 "entries": [{"tvg_id": "crosstalk.event.1", "name": "Match", "icon_url": null,
              "programs": [{"start": 1800000000, "end": 1800003600, "title": "Kick-off",
                            "sub_title": null, "description": "…",
                            "custom_properties": {"live": true}}]}]}
```

- Refused whole (`not_allowed`) unless `epg_source_id` is listed in the plugin's `epg_sources`
  setting: the operator decides which sources the bridge may write programme rows into. Empty
  means no guide writes at all. An unknown source is `not_found`.
- Each entry is the `EPGData` row keyed by `(tvg_id, epg_source)` (Dispatcharr's own unique
  constraint): created when missing, `name` and `icon_url` updated when different.
- **Replace by window**: for each entry, its `ProgramData` rows whose `start_time` is in
  `[window.start, window.end)` are deleted and the given programmes inserted. Programmes starting
  outside the window, and entries not named, are untouched. Every given programme must start
  inside the window; an entry with one that does not is an error and is left untouched.
- `window.end − window.start` is at most 14 days. Times are unix seconds, UTC.
- No `EPGData` row is ever deleted, and no channel's guide assignment changes (assigning a
  generated entry to a channel stays on REST, `batch-set-epg`).

## Responses

Every operation answers:

```json
{"ok": true, "op": "set_stream_lists", "applied": 2, "skipped": [8],
 "errors": [{"id": 404, "reason": "not_found: no such channel"}], "version": "0.1.0"}
```

- `ok: false` means **nothing was written**: the request was refused before it ran, or the
  transaction rolled back. `applied` is 0 and `errors` holds one entry with `"id": null` whose
  `reason` starts with a stable code: `bad_request`, `not_configured`, `bad_mac`, `stale`,
  `replayed`, `unknown_op`, `invalid`, `too_large`, `not_allowed`, `not_found`, or `internal`
  (an exception; its class name only, and "nothing was written").
- `ok: true`: every id the request named is in exactly one place — counted in `applied`
  (written), listed in `skipped` (already in the requested state, nothing written), or listed in
  `errors` with its reason (left untouched). crosstalk checks that the three add up to the number
  of ids it sent and fails the whole chunk as "outcome unknown" when they do not.
- Reasons never contain the secret, a MAC or payload values beyond ids.

## Limits and chunking

| Limit | Value |
|---|---|
| channels per `set_stream_lists` / `clear_channel_groups` request | 2,000 |
| streams in one channel's list | 1,000 |
| stream ids across one `set_stream_lists` request | 20,000 |
| streams per `set_stream_stats` request | 5,000 |
| canonical size of one `stream_stats` object | 16 KiB |
| entries per `upsert_epg` request | 2,000 |
| programmes per `upsert_epg` request | 50,000 |
| `upsert_epg` window | 14 days |

A request over a limit is refused whole (`too_large`). crosstalk chunks every write to stay inside
them and also keeps each request body under 1 MB (Django refuses bodies over
`DATA_UPLOAD_MAX_MEMORY_SIZE`, 2.5 MB by default, and a reverse proxy may stop sooner). A row that
cannot fit a request by itself (a list of 1,001 streams) fails on its own without being sent. A
failed chunk fails only its own rows.

## Secret handling

- **Generated by crosstalk**: `crosstalk bridge secret` reads 32 bytes from `/dev/urandom`
  (never a weaker fallback), writes the 64-hex-digit secret to `[bridge] secret` in
  `crosstalk.toml` through the comment-preserving save (the file is written `0600`), and prints it
  once. It refuses when a secret is already set (`bridge rotate` replaces one) and when
  `[bridge] secret_env` names a variable that is set: the environment wins over the file, so a
  secret written there would never be used, and an environment-supplied secret is never written
  back to the file.
- **crosstalk side**: `[bridge] secret`, or the variable `secret_env` names (default
  `CROSSTALK_BRIDGE_SECRET`), which wins when set and not blank. `Debug` output prints
  `<redacted>`; the settings page shows only "set in crosstalk.toml", "set (from $VAR)" or "not
  set", in a disabled password control, and a settings save never changes it; `validate` warns
  when both the file and the variable hold one and when a group/world-readable `crosstalk.toml`
  holds it.
- **Plugin side**, either of:
  - the plugin's **Secret** setting (a masked password field), or
  - **`CROSSTALK_BRIDGE_SECRET` in Dispatcharr's environment**, which wins over the setting when
    set and not blank (likewise `CROSSTALK_BRIDGE_PREVIOUS_SECRET` and
    `CROSSTALK_BRIDGE_GRACE_UNTIL` for the rotation fields).
- Nothing logs the secret or a MAC: the plugin logs the operation, counts and refusal codes; the
  client's errors carry the plugin's reasons, which never include either.

### Keeping the secret out of Dispatcharr's database

A secret pasted into the plugin's settings is stored in `PluginConfig.settings`, and Dispatcharr's
`GET /api/plugins/plugins/` returns every plugin's settings verbatim to any caller with plugin
access. Setting the secret in Dispatcharr's environment instead keeps it out of the database,
out of database backups, and out of that listing. With Docker Compose, give both services the
same value from one place (an `.env` file next to the compose file, mode 0600):

```yaml
services:
  dispatcharr:
    environment:
      CROSSTALK_BRIDGE_SECRET: "${CROSSTALK_BRIDGE_SECRET:?set CROSSTALK_BRIDGE_SECRET}"
      # During a rotation only:
      # CROSSTALK_BRIDGE_PREVIOUS_SECRET: "${CROSSTALK_BRIDGE_PREVIOUS_SECRET}"
      # CROSSTALK_BRIDGE_GRACE_UNTIL: "2026-11-10T12:00:00Z"
  crosstalk:
    environment:
      CROSSTALK_BRIDGE_SECRET: "${CROSSTALK_BRIDGE_SECRET:?set CROSSTALK_BRIDGE_SECRET}"
```

Generate the value with `openssl rand -hex 32` (or run `crosstalk bridge secret` once and move
the printed value into `.env`, then blank `[bridge] secret`). Leave the plugin's Secret setting
empty. `crosstalk bridge ping` reports `plugin secret configured: env` when this is in effect.

### Rotation

`crosstalk bridge rotate` generates a new secret, writes it to `[bridge] secret`, prints it once,
and prints the order that never breaks the bridge:

1. In the plugin, move the current secret into **Previous secret** and set **Accept the previous
   secret until** to a time a day out (ISO 8601 UTC or unix seconds) — or set
   `CROSSTALK_BRIDGE_PREVIOUS_SECRET` / `CROSSTALK_BRIDGE_GRACE_UNTIL`.
2. Put the new secret in **Secret** (or `CROSSTALK_BRIDGE_SECRET`). From here the plugin accepts
   both, so the running crosstalk, still signing with the old one, keeps working.
3. Restart crosstalk; it signs with the new one.
4. `crosstalk bridge ping`, then clear the previous secret.

The previous secret is accepted only while the grace time is in the future and parses; empty,
past or unparsable means refused (fail closed). The start-up ping says when crosstalk's secret
was accepted only as the previous one.

## crosstalk's side

- `[bridge] enabled = false` by default. Nothing is sent to the plugin while it is off.
- `serve`, `run`, `undo` and workflow runs ping the plugin once at start-up when enabled. The
  bridge is used for the rest of the process only if the signed ping succeeds, the protocol is
  1, the version is at least `[bridge] min_version`, and the four writes are in `capabilities`.
  Otherwise the reason is logged (`bridge: NOT used this run, …`) and the bridge-backed paths
  fall back: stream lists and stream stats go over REST as before, and a move to "No Group" is
  refused per row with a reason saying it needs the bridge. A settings save that changes the
  key or the secret also falls back to REST until the next restart re-checks it: crosstalk only
  signs with the key and secret its start-up ping verified.
- `validate` pings too and prints the verdict (a warning, not an error: runs still work on REST).
- `crosstalk bridge ping` pings with the configured secret even while `enabled` is false, so an
  install can be checked before it is switched on.
- Where it plugs in: `BridgeClient` implements `channel_ops::StreamListWriter` (stream lists) and
  `channel_ops::GroupClearer` (No Group); the probe's `stream_stats` write uses
  `BridgeClient::set_stream_stats`; `BridgeClient::upsert_epg` is ready for phase 4. Lineup's
  channel creation stays on REST (`ChannelCreator` has no bridge implementation: creating
  channels is not one of the four writes).

## Threat model

**Who holds what.**

| Party | Holds |
|---|---|
| crosstalk | the Dispatcharr admin API key (`[dispatcharr]`), the bridge secret (`[bridge]` or its variable) |
| Dispatcharr | the bridge secret (plugin setting, or its environment) |
| Network path / reverse proxy between them | sees both in transit unless `base_url` is https |
| Anyone with the Dispatcharr admin key | everything the REST API offers, which includes **reading the plugin's settings** |

**What a leaked Dispatcharr admin key can do with the bridge.** Without the bridge secret it can
invoke the plugin-run route but every bridge request is refused (`bad_mac`), so the bridge adds
nothing to it. Be clear about the limit, though: the same key can already rewrite or delete
channels, streams, groups and guides through REST, upload and enable a plugin of its own (which
runs arbitrary code in Dispatcharr), and **read the bridge secret from `GET
/api/plugins/plugins/`** when the secret is stored in the plugin's settings. The HMAC is therefore
not a defence against a leaked admin key; that key is already the higher privilege. Keeping the
secret in Dispatcharr's environment (above) removes the "read it from the listing" step, which
matters for read-only exposure of that listing (a log, a backup, a support bundle), not against
an attacker who can install plugins.

**What the HMAC does protect against.** A request body captured in transit or in a log (a
proxy's request logging, a debug dump) cannot be replayed after 5 minutes or ever twice within
them, cannot be altered (any change breaks the MAC), and cannot be re-aimed at another operation
(the `op` is signed and must match the action). A click on the plugin in Dispatcharr's UI, a
Connect event, or another tool calling the run route without the secret does nothing.

**What a leaked bridge secret can do.** Combined with a credential that can reach the plugin-run
route (the secret alone reaches nothing), only the five operations, within their limits:

- reorder or replace channels' stream lists, including emptying them (streams are not deleted
  and can be put back);
- overwrite streams' `stream_stats` (cosmetic metadata players and crosstalk's sort read);
- move channels to "No Group";
- create or rename guide entries and replace programmes inside a 14-day window, **only in the EPG
  sources the operator listed** in `epg_sources`.

It cannot read anything, delete a channel, stream, group, source, guide entry or profile, change
names, numbers, logos, EPG assignments, users, settings or other plugins, run tasks, or reach the
file system. Every one of its writes is reversible by re-running crosstalk (or an undo of the run
that recorded the before values). Rotate the secret if it leaks.

**What the bridge cannot do even when everything works as designed.** The allow-list is the code:
an op that is not one of the five is refused after verification, before anything is imported.
There is no generic ORM call, no eval, no file access, and no read path.

## Assumptions about Dispatcharr (v0.33.0) not verified against a running instance

These were read from Dispatcharr 0.33.0's source, not exercised against a live server:

1. The run route passes `params` through DRF's `JSONParser` unchanged, so a float arrives as the
   same double crosstalk serialized (Python's `json` is correctly rounded; so is serde_json's
   writer). The canonical-JSON vectors cover the layout, not the parser.
2. `DATA_UPLOAD_MAX_MEMORY_SIZE` (2.5 MB default) bounds a plugin-run body; the client stays under
   1 MB. Not checked: whether Dispatcharr raises or lowers it, or what uWSGI/nginx in the official
   image allow.
3. Django's cache is shared across uWSGI workers (Redis) in the official image. If it is a
   per-process cache, a nonce is only checked within one worker, so a captured request could be
   replayed once per worker inside the 5-minute window. Every operation is idempotent, so a
   replay re-applies the same state; it could revert a newer change made by someone else within
   those 5 minutes.
4. `bulk_update` / `bulk_create` / queryset `delete` on `ChannelStream`, `Stream`, `EPGData`,
   `ProgramData` behave as in stock Django with PostgreSQL; signals Dispatcharr hangs on
   `post_save` of these models are skipped by the bulk paths (the catch-up roll-up is called
   explicitly; websocket UI refresh notifications are not sent, so an open Dispatcharr page may
   need a reload).
5. A generated guide's programmes are read from `ProgramData` for the source type the operator
   picks for it (phase 4 decides which). Dummy sources may generate their programmes rather than
   read rows; that is why `upsert_epg` is gated on an explicit `epg_sources` list.
6. The plugin-run route requires the same admin permission as the other plugin routes
   (`permissions_for_method`, not read in this audit).
7. `PluginConfig.settings` for a `string` field with `"input_type": "password"` is masked in the
   UI but stored and listed in clear (stated by Dispatcharr's own `Plugins.md`).
