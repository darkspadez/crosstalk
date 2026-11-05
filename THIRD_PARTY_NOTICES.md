# Third-party notices

crosstalk is MIT-licensed; see [LICENSE](LICENSE). It also contains code and data ported from
the Dispatcharr plugins below, whose licence requires their copyright notices to travel with
the port.

## EPG Janitor (Dispatcharr-EPG-Janitor-Plugin)

- Upstream: <https://github.com/PiratesIRC/Dispatcharr-EPG-Janitor-Plugin>
- Licence: MIT, Copyright (c) 2026 PiratesIRC (full text below)
- What was ported, and where it lives:
  - `EPG-Janitor/fuzzy_matcher.py`, `matching_core.py`, `aliases.py` and
    `wildcard_match.py` → `crates/crosstalk-matching`: the matcher itself (normalisation,
    similarity, tokens, callsigns and locations, aliases, `match_all_streams`).
  - `EPG-Janitor/plugin.py` → `crates/guide` (`match_*`, `heal_*`, `scan` and the five
    cleanup presets) and `crates/crosstalk-core` (channel operations, report and outcome
    builders, the CSV preamble). The guide settings schema follows `EPG-Janitor/plugin.json`
    (field ids, labels and help text).
  - `EPG-Janitor/epg_watchdog.py` → `crates/guide/src/watchdog.rs`.
  - the vendored data files, listed next.
- Vendored data: `data/channels/*_channels.json` (AU, BR, CA, DE, ES, FR, IN, MX, NL, NO, UK,
  US) and `data/us_station_callsigns.json` are byte-for-byte copies of the plugin's own files.
  `data/aliases.json` is the plugin's `aliases.py` `CHANNEL_ALIASES` table converted to JSON
  with the same entries. All of them are embedded in the binary at compile time rather than
  read from the plugin at run time.

### Reference pin

The plugin is a git submodule at `reference/Dispatcharr-EPG-Janitor-Plugin`, pinned so the
parity gates always compare against one exact revision:

    f5bcafa44dfd0db2b43195cd68c1d33bf22c0d7b   Release 1.26.2971931

`git submodule status` in a checkout is authoritative. `scripts/upstream_drift.sh` lists what
upstream has changed since that commit in the files crosstalk ports.

### Licence text, verbatim from the plugin's `LICENSE`

MIT License

Copyright (c) 2026 PiratesIRC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Stream-Mapparr

- Upstream: <https://github.com/PiratesIRC/Stream-Mapparr>
- Licence: MIT, Copyright (c) 2025 Pirates IRC (full text below)
- Ported from, and where it lives: these are the reference files for the `streams` sub-app,
  ported in phase 2 of `plans/plugin-consolidation.md` (`plans/phase-2-streams.md`). This list
  is the one `scripts/upstream_drift.sh` watches for this reference, and it is the same list
  below: a file credited here is a file whose upstream change may matter.
  - `Stream-Mapparr/plugin.py` → `crates/streams`: `match_*` (the per-name-group stream
    collector, replace/append, keep custom streams, visible-channel limit), `sort_*` (the
    quality model), `mirror_*` (`fill_mirror_backups`), `visibility_*`
    (`manage_channel_visibility`), `probe`'s throughput mode (`probe_throughput`),
    `check_countries` and `test_rules`, and the stream-selection half of `scope::sources`.
  - `Stream-Mapparr/fuzzy_matcher.py`, `matching_core.py` and `aliases.py` →
    `crates/crosstalk-matching`: `MatchProfile::StreamMapparr` on the shared matcher (zone
    expansion, designator-token repair, alias-key folding, the single-timezone-country
    guard) plus the alias tables.
  - `Stream-Mapparr/country.py` → `crates/crosstalk-matching`: country detection and the
    country prefix/restriction rules.
  - `Stream-Mapparr/mirror_backups.py` → `crates/streams/src/mirror.rs`: `plan_mirror_fill`
    over accounts sharing a `server_group`.
  - `Stream-Mapparr/placeholder_scan.py` → `crates/streams/src/placeholder_scan.rs`: the
    numbered-family scan behind `scan_placeholders`. The EPG-placeholder matching path is
    `plugin.py`'s, not this module's (`crates/streams/src/epg.rs`).
  - `Stream-Mapparr/plugin.json` → `crates/streams/src/settings.rs`: the `streams.toml`
    schema follows the plugin's field ids, labels and help text.
  - `Stream-Mapparr/*_channels.json` (AU, BR, CA, DE, DO, ES, FR, IN, MX, NL, NO, PE, PL, PT,
    UK, US) and `Stream-Mapparr/networks.json` → the embedded country channel databases and
    the network-name table the matcher's country and network detection use. Only the files
    that differ from the EPG Janitor copies are duplicated, under
    `data/stream-mapparr/` (DO, PE, PL, PT, US and `networks.json`); the eleven shared
    databases are read from `data/channels/`. `data/stream-mapparr/aliases.json` and
    `country_aliases.json` are its `aliases.py` `CHANNEL_ALIASES` and `COUNTRY_ALIASES`
    tables converted to JSON with the same entries.

### Reference pin

The plugin is a git submodule at `reference/Stream-Mapparr`, pinned so the parity harnesses
always compare against one exact revision:

    04dd47149a1215140522b224a1ed7e7d9ed7b81f   Release 1.26.3052124

`git submodule status` in a checkout is authoritative. `scripts/upstream_drift.sh` lists what
upstream has changed since that commit in the files crosstalk ports.

### Licence text, verbatim from the plugin's `LICENSE`

MIT License

Copyright (c) 2025 Pirates IRC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## IPTV Checker (Dispatcharr-IPTV-Checker-Plugin)

- Upstream: <https://github.com/PiratesIRC/Dispatcharr-IPTV-Checker-Plugin>
- Licence: MIT, Copyright (c) 2025 Pirates IRC (full text below)
- Ported from, and where it lives: the probe, triage and restore halves of the same `streams`
  sub-app, ported in phase 2 (`plans/phase-2-streams.md`). This list is the one
  `scripts/upstream_drift.sh` watches for this reference, and it is the same list below: a file
  credited here is a file whose upstream change may matter.
  - `iptv_checker/plugin.py` → `crates/streams`: `probe` (`load_groups` + `check_streams`:
    `ffprobe`/`ffmpeg` invocation, parsing and classification, `stream_stats` writes,
    per-account concurrency and the 429 throttle), `triage_*` (dead / blank / low-fps
    renames, move to group, `add_video_format_suffix`) and `restore_*`
    (`restore_channels`).
  - `iptv_checker/plugin.json` → `crates/streams/src/settings.rs`: the probe and triage
    sections of `streams.toml` follow the plugin's field ids, labels and help text.

### Reference pin

The plugin is a git submodule at `reference/Dispatcharr-IPTV-Checker-Plugin`, pinned so the
parity harnesses always compare against one exact revision:

    14f17ea562363e080343b25db3f3e48a15fc2c9d   Release v1.26.3052213

`git submodule status` in a checkout is authoritative. `scripts/upstream_drift.sh` lists what
upstream has changed since that commit in the files crosstalk ports.

### Licence text, verbatim from the plugin's `LICENSE`

MIT License

Copyright (c) 2025 Pirates IRC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Channel-Mapparr (Dispatcharr-Channel-Maparr-Plugin)

- Upstream: <https://github.com/PiratesIRC/Dispatcharr-Channel-Maparr-Plugin>
- Licence: MIT, Copyright (c) 2025 Pirates IRC (full text below)
- Ported from, and where it lives: these are the reference files for the `lineup` sub-app,
  planned in phase 3 (`plans/phase-3-lineup.md`). This list is the one
  `scripts/upstream_drift.sh` watches for this reference, and it is the same list
  below: a file credited here is a file whose upstream change may matter.
  - `Channel-Maparr/plugin.py` → `crates/lineup` (`names_*`, `categorize_*`, `logos_*`,
    `create_*`).
  - `Channel-Maparr/fuzzy_matcher.py`, `matching_core.py` and `aliases.py` →
    `crates/crosstalk-matching`: the unified matcher (normalisation, callsign ladder,
    alias tables) and the channel database behind it.
  - `Channel-Maparr/logo_matcher.py` → the unified logo resolver.
  - `Channel-Maparr/channel_seeder.py` → `create_*` (the stations mode).
  - `Channel-Maparr/market_index.py` and `station_affiliation.py` → OTA naming (market
    fallback, affiliation parse).
  - `Channel-Maparr/group_scope.py` and `wildcard_match.py` → lineup scope handling
    (group scope and its wildcard patterns).
  - `Channel-Maparr/plugin.json` → `crates/lineup/src/settings.rs`: the `lineup.toml`
    schema follows the plugin's field ids, labels and help text.
  - `Channel-Maparr/*_channels.json` (AU, BR, CA, DE, ES, FR, IN, MX, NL, NO, PL, UK,
    US) and `Channel-Maparr/networks*.json` (`networks.json`,
    `networks_supplemental.json`) → the merged channel database and network tables the
    unified matcher uses.

### Reference pin

The plugin is a git submodule at `reference/Dispatcharr-Channel-Maparr-Plugin`, pinned so
the parity harnesses always compare against one exact revision:

    45ba787f55094f01d6b78ebaff8a35e772200846   Release 1.26.2971911

`git submodule status` in a checkout is authoritative. `scripts/upstream_drift.sh` lists what
upstream has changed since that commit in the files crosstalk ports.

### Licence text, verbatim from the plugin's `LICENSE`

MIT License

Copyright (c) 2025 Pirates IRC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Lineuparr (Dispatcharr-Lineuparr-Plugin)

- Upstream: <https://github.com/PiratesIRC/Dispatcharr-Lineuparr-Plugin>
- Licence: MIT, Copyright (c) 2026 Pirates IRC (full text below)
- Ported from, and where it lives: these are the reference files for the `lineup` sub-app,
  planned in phase 3 (`plans/phase-3-lineup.md`). This list is the one
  `scripts/upstream_drift.sh` watches for this reference, and it is the same list
  below: a file credited here is a file whose upstream change may matter.
  - `Lineuparr/plugin.py` → `crates/lineup` (`sync_*`, `tidy_*`, the `logos` tier order
    through the unified logo resolver) and the lineup mode of `streams.match`.
  - `Lineuparr/fuzzy_matcher.py`, `matching_core.py` and `aliases.py` →
    `crates/crosstalk-matching`: the unified matcher's alias and candidate scoring behind
    the lineup mode's oracle comparisons.
  - `Lineuparr/logo_matcher.py` → the unified logo resolver.
  - `Lineuparr/plugin.json` → `crates/lineup/src/settings.rs`: the `lineup.toml`
    schema follows the plugin's field ids, labels and help text.
  - `Lineuparr/*_lineup.json` (the 29 lineup files) → the vendored lineup files under
    `data/merged/lineups/` (see the merged-data section below).

### Reference pin

The plugin is a git submodule at `reference/Dispatcharr-Lineuparr-Plugin`, pinned so the
parity harnesses always compare against one exact revision:

    9f6a5ff8c0290b19bfffcf56d047052194730ac5   Release v1.26.3052143

`git submodule status` in a checkout is authoritative. `scripts/upstream_drift.sh` lists what
upstream has changed since that commit in the files crosstalk ports.

### Licence text, verbatim from the plugin's `LICENSE`

MIT License

Copyright (c) 2026 Pirates IRC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Channel Name Cleaner (dispatcharr-channel-name-cleaner)

- Upstream: <https://github.com/noodlemctwoodle/dispatcharr-channel-name-cleaner>
- Licence: MIT, Copyright (c) 2026 noodlemctwoodle (full text below)
- Ported from, and where it lives: these are the reference files for the `lineup` sub-app,
  planned in phase 3 (`plans/phase-3-lineup.md`). This list is the one
  `scripts/upstream_drift.sh` watches for this reference, and it is the same list
  below: a file credited here is a file whose upstream change may matter.
  - `channel_name_cleaner/plugin.py` → `crates/lineup` (`clean_*`, `test_name`, the
    generic host `undo`).
  - `channel_name_cleaner/plugin.json` → `crates/lineup/src/settings.rs`: the clean
    section of `lineup.toml` follows the plugin's field ids, labels and help text.

### Reference pin

The plugin is a git submodule at `reference/dispatcharr-channel-name-cleaner`, pinned so
the parity harnesses always compare against one exact revision:

    d68b195bab6a9ef8e540954eea74620bed0f42a5   Rewrite README: full docs (features, install, usage, settings, safety); no emojis

`git submodule status` in a checkout is authoritative. `scripts/upstream_drift.sh` lists what
upstream has changed since that commit in the files crosstalk ports.

### Licence text, verbatim from the plugin's `LICENSE`

MIT License

Copyright (c) 2026 noodlemctwoodle

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Merged data set (`data/merged/`)

Phase 3 (`plans/phase-3-lineup.md`, step 4) replaces the per-plugin tables with one
merged data set, built by `scripts/data/build_merged.py` from the pinned checkouts
above and reproducible byte for byte (`build_merged.py --check`, enforced in CI).
Provenance per input file (path, sha256, header date), duplicates merged, field
conflicts with both values and the winner, case-variant names, and per-plugin
uniques are logged in `data/merged/PROVENANCE.json`, multi-canonical aliases in
`data/merged/alias_conflicts.json`; the human-readable companion is
`docs/data-merge-report.md`. Nothing here is newly authored: every record traces to
one of the plugin files credited in the sections above.

- `data/merged/channels/<CC>.json` (16 countries): the union of EPG Janitor's 12
  `*_channels.json` files, Stream-Mapparr's 16 (adds DO/PE/PL/PT plus a `zones`
  array on 31 US entries) and Channel-Mapparr's 13 (shares PL with Stream-Mapparr;
  otherwise a same-or-older subset that contributes no name of its own),
  deduplicated on the verbatim `channel_name` with a sorted `sources` list per
  entry (case-only variants such as `Afro`/`AFRO` stay separate, as upstream). Newest file version wins
  header and field conflicts. Loaded behind `crosstalk-matching::data::merged`
  (not wired into matching yet); the existing profiles keep their current files.
- `data/merged/stations.json`: Channel-Mapparr's `networks.json` (6,839 FCC
  stations, 2026-08-10 LMS dump) plus `networks_supplemental.json` (WGCL) with
  `scripts/networks_corrections.json` applied (WBMA-LD), keyed by full callsign
  with per-entry `sources`. Stream-Mapparr's `networks.json` (1,915) is
  byte-identical to Channel-Mapparr's pre-rebuild table, so it contributes no
  record: its 269 differing fields resolve to Channel-Mapparr's newer values and
  its 43 extra rows (cancelled licences that rebuild dropped) stay dropped; both
  are listed in the provenance. EPG Janitor's `us_station_callsigns.json` is
  folded in as the `callsign_allowlist` array (3,037 callsigns, every one resolving to a merged
  station under the core `normalize_callsign` base form).
- `data/merged/aliases.json`: the union of the four `aliases.py`
  `CHANNEL_ALIASES` tables (Janitor 204 canonicals, Stream-Mapparr 198,
  Channel-Mapparr 226, Lineuparr 297) as canonical -> aliases with per-alias
  `sources`, plus the per-country `COUNTRY_ALIASES` unions (DO/PE from
  Stream-Mapparr and Lineuparr; AU/FR/CA from Lineuparr). Every claim is kept;
  the 136 alias values listed under two or more canonicals (12 disputed between
  plugins) are recorded in `data/merged/alias_conflicts.json` for the corpus
  step to decide.
- `data/merged/lineups/` (29 files, 7,485 channels): Lineuparr's `*_lineup.json`
  files copied verbatim (schema in `data/merged/lineups/README.md`).

`Dockerfile` already does `COPY ./data`, so the merged set ships in the image
with no Dockerfile change.

## The parity gates and the drift report, over all six references

The parity gates in `scripts/` compare crosstalk against each plugin at its pinned commit, and
`README.md` lists every place where crosstalk deliberately behaves differently.
`scripts/upstream_drift.sh` reports, per reference and with that reference's own list of
ported paths, what upstream has changed since the pin — the six lists are the file lists in
the sections above, so a file credited here is a file the drift report watches.
