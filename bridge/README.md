# crosstalk bridge (Dispatcharr plugin)

The four bulk writes [crosstalk](https://github.com/darkspadez/crosstalk) needs that
Dispatcharr's REST API cannot do:

- replace channels' ordered stream lists (`ChannelStream` rows), 2,000 channels per request;
- set streams' `stream_stats` and `stream_stats_updated_at`, 5,000 streams per request;
- move channels to "No Group" (`channel_group` NULL), which the REST channel edit refuses;
- create guide entries and replace their programmes inside a time window, for crosstalk's
  generated guides, only in EPG sources you list.

Nothing else. It has no buttons, reads nothing back, never deletes a channel, stream, group,
source or guide entry, and refuses every request that is not signed with the secret crosstalk
generated. The full contract and threat model are in
[docs/bridge.md](https://github.com/darkspadez/crosstalk/blob/main/docs/bridge.md).

## Install

1. Download `crosstalk-bridge-<version>.zip` (and `SHA256SUMS`) from the crosstalk release that
   matches your crosstalk, and check it: `sha256sum -c SHA256SUMS --ignore-missing`. Releases
   also carry a GitHub build-provenance attestation:
   `gh attestation verify crosstalk-bridge-<version>.zip --repo darkspadez/crosstalk`.
2. In Dispatcharr: **Plugins → Import**, choose the zip. It installs as `crosstalk_bridge`
   (Dispatcharr turns `-` into `_`), which is crosstalk's default `[bridge] plugin_key`.
3. On the crosstalk host: `crosstalk bridge secret`. It writes a new secret to
   `crosstalk.toml` `[bridge] secret` (mode 0600) and prints it once.
4. Give the plugin that secret, one of two ways:
   - **Settings**: paste it into the plugin's **Secret** field and save; or
   - **Environment** (recommended): set `CROSSTALK_BRIDGE_SECRET` in Dispatcharr's environment
     and leave the field empty. See below.
5. Enable the plugin (Dispatcharr asks you to confirm you trust it: it runs inside Dispatcharr).
6. Set `[bridge] enabled = true` in `crosstalk.toml`, then `crosstalk bridge ping`. It should
   print `bridge: crosstalk-bridge 0.1.0 ready as "crosstalk_bridge"`. Restart the daemon.
7. Only if you use crosstalk's generated guides: put the EPG source ids crosstalk may write into
   in **EPG sources guide writes may use**. Empty refuses every guide write.

## Keeping the secret out of Dispatcharr's database

A secret typed into the settings is stored in Dispatcharr's database, and
`GET /api/plugins/plugins/` returns every plugin's settings to anyone with plugin access. When
`CROSSTALK_BRIDGE_SECRET` is set (and not blank) in Dispatcharr's environment the plugin uses it
instead and ignores the setting, so the secret is in neither the database, its backups, nor that
listing. `CROSSTALK_BRIDGE_PREVIOUS_SECRET` and `CROSSTALK_BRIDGE_GRACE_UNTIL` do the same for
the rotation fields. With Docker Compose and an `.env` file (mode 0600) holding the value:

```yaml
services:
  dispatcharr:
    environment:
      CROSSTALK_BRIDGE_SECRET: "${CROSSTALK_BRIDGE_SECRET:?set CROSSTALK_BRIDGE_SECRET}"
  crosstalk:
    environment:
      CROSSTALK_BRIDGE_SECRET: "${CROSSTALK_BRIDGE_SECRET:?set CROSSTALK_BRIDGE_SECRET}"
```

`crosstalk bridge ping` reports `plugin secret configured: env` when this is in effect. A ping
answer only ever says `env`, `settings` or `none`; it never contains the secret.

## Rotating the secret

`crosstalk bridge rotate` makes a new secret, writes it to `crosstalk.toml` and prints the steps:
move the current secret into **Previous secret** with **Accept the previous secret until** a day
out, put the new one in **Secret**, restart crosstalk, ping, then clear **Previous secret**. The
previous secret is refused once that time has passed or when it is empty or unreadable.

## What it can never do

Read data back; delete channels, streams, groups, EPG sources or guide entries; change names,
numbers, logos, EPG assignments, users or settings; run tasks; touch files; or do anything when
the request is unsigned, older than five minutes, replayed, or not one of its five operations.
A leaked secret is limited to the four writes above (and only the EPG sources you listed); a
leaked Dispatcharr admin key was already more powerful than the bridge. See the threat model in
docs/bridge.md.

## Logs

One line per request: the operation, how many rows were written, unchanged and failed, or the
refusal code (`bad_mac`, `stale`, `replayed`...). Never the secret, a signature or a payload.

## Development

`python3 -I -m unittest discover -s bridge/tests` (or `python3 -I -m pytest bridge/tests`) from
the crosstalk checkout. The tests fake Django's ORM; `bridge/tests/vectors.json` is shared with
crosstalk's Rust tests and pins the canonical JSON and signatures both sides compute.

MIT licensed, like crosstalk (`LICENSE`).
