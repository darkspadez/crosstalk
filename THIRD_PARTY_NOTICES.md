# Third-party notices

crosstalk is MIT-licensed; see [LICENSE](LICENSE). It also contains code and data ported from
the Dispatcharr plugin below, whose licence requires its copyright notice to travel with the
port.

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

The parity gates in `scripts/` compare crosstalk against the plugin at the pinned commit, and
`README.md` lists every place where crosstalk deliberately behaves differently.

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
