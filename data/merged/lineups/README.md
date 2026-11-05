# Merged lineup files

Lineuparr's 29 lineup files, copied byte-for-byte from
`reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/*_lineup.json` by
`scripts/data/build_merged.py` (hashes in `../PROVENANCE.json`). Do not
hand-edit: rerunning the script reproduces this directory byte for byte
(`build_merged.py --check`, enforced in CI).

## Schema

Each file is one object with `package` (display name), `date` (`YYYY-MM-DD`
file version), `description`, `source`, `notes` (free text; `source`/`notes`
may be absent), and `categories` mapping a category name to a channel list.
A channel always has `name` (verbatim lineup name) and `number` (lineup
channel number); optional extras seen in the wild: `aliases` (accepted
alternate names, e.g. TZ files), `addon` (package tag, e.g. AU/CA/US provider
files), `notes` (free text, e.g. part-time or regional notes), `epg_ids`
(ephemeral guide ids on a few entries). 29 files, 7485
channels total (see `../PROVENANCE.json` `lineups` and
`docs/data-merge-report.md`).

## Country

The owning country is the filename's `CC_` prefix (`US_`, `UK_`, `DO_`,
`CA_`, `AU_`, `ES_`, `FR_`, `NL_`, `PE_`, `QA_`, `TZ_`). `US_Combined` is a
de-duplicated union of three US provider lineups (DIRECTV Premier, DISH
Top 250, Verizon FiOS); `TZ_Combined` likewise for Tanzania.
