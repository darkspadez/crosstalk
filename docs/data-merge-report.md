# Merged data set: provenance and conflict report

Phase 3 step 4 (`plans/phase-3-lineup.md`): one merged data set built by
`scripts/data/build_merged.py` from the four plugins' shipped tables.
Generated file; rerunning the script reproduces `data/merged/` and this
report byte for byte (`build_merged.py --check`, enforced in CI).

## Summary

- Channel databases: 16 countries, 130719 rows in -> 43971 entries out (86748 exact duplicates merged, 0 field conflicts, 125 case-variant groups kept apart).
- Station table: CM 6839 + supplemental 1 -> 6840 stations out; SM's 1915 rows are CM's predecessor table: 269 differing fields (CM wins all), 43 SM-only rows dropped as cancelled licences.
- Callsign allowlist: 3037 Janitor callsigns folded in.
- Alias table: 2610 alias rows in (JAN 569, SM 585, CM 652, LU 804) -> 865 (canonical, alias) rows out across 316 canonicals, every claim kept. 136 aliases sit under two or more canonicals (12 disputed between plugins, 124 agreed by all; 67 with a self-name hint), listed in `data/merged/alias_conflicts.json` for step 5.
  - AU: 6 aliases, 0 multi-canonical.
  - CA: 2 aliases, 0 multi-canonical.
  - DO: 208 aliases, 0 multi-canonical.
  - FR: 2 aliases, 0 multi-canonical.
  - PE: 125 aliases, 0 multi-canonical.
- Lineup files: 29 files, 7485 channels, copied verbatim.
- Size of `data/merged/` (excluding `PROVENANCE.json`): 7145129 bytes (6.81 MiB).

## Dedup key decisions

- Channels: the verbatim `channel_name` string. Evidence: across all
  plugin files no two entries sharing a name disagree on
  `category`/`type` (the only cross-plugin field difference is SM's
  `zones` array, which JAN/CM simply lack), and every in-file duplicate
  is an identical row. Case-only variants exist and are NOT duplicates:
  e.g. US `Afro` (Entertainment/National) vs `AFRO` (United
  States/Regional); they stay separate entries, as in the sources, and
  are listed under `channels.<CC>.case_variants` in `PROVENANCE.json`
  (the normalizer decisions of step 5 decide how they fold). A
  normalised key here would merge distinct entries. Duplicates collapse
  into one entry carrying a sorted `sources` list.
- Field conflicts: newest source wins, comparing each file's
  `YYYY-MM-DD[-vN]` version header. JAN and SM share every version and
  every name set. CM is older in AU, CA, ES, UK, US and equal in
  BR, DE, FR, IN, MX, NL, NO, PL; everywhere its names are a strict subset
  with identical field values. In practice this only selects the header.
- Stations: Channel-Mapparr's table as shipped (FCC LMS dump of
  2026-08-10, plus `networks_supplemental.json` and
  `networks_corrections.json`; corrections are verified before applying,
  so a stale table fails the build). Evidence for the per-field rule:
  SM's `networks.json` (last changed 2025-10-01) is byte-identical to
  CM's table at `2d34bc3`, the "previous file" CM's
  `scripts/build_networks_json.py` rebuilt from. That build keeps every
  predecessor station unless its licence is cancelled, takes each field
  from the dated dump, and refuses to replace a specific network with a
  vague one (none of the differing affiliations has CM vague and SM
  specific). So every differing field is the newer FCC value and CM
  wins all fields; SM curated nothing CM lacks. The SM-only rows are the
  rebuild's cancelled-licence drops (FCC `D`-prefixed republications of
  dead callsigns, plus KVTV) and are not resurrected. The build pins
  SM's sha256 and stops if it changes. Janitor's allowlist callsigns are
  folded in as the `callsign_allowlist` array; every one resolves to a
  merged station under the core `normalize_callsign` base form (pinned
  by a test in `crosstalk-matching::data::merged`).
- Aliases: union of every claim. The tables map a canonical to the
  names it accepts, so an alias under two canonicals (`FS 1` under `FS1`
  and `Fox Sports 1`) means both accept it; all four plugins do exactly
  that today, and dropping such aliases would change behaviour every
  plugin agrees on. They are listed in `alias_conflicts.json`: `agreed`
  when every plugin carrying a claiming canonical lists the alias under
  it, `disputed` otherwise (`omitted_by` names the dissenters), with a
  `self_name` hint when the alias casefold-equals exactly one claimant.
  Step 5 decides each for reverse (alias -> canonical) lookups. Country
  tables merge independently per country code.

## Size

`data/merged/` totals 7145129 bytes (6.81 MiB) without
`PROVENANCE.json`; the US database dominates (31792 entries). Output
is compact JSON (no pretty-print). `Dockerfile` already does
`COPY ./data`, so the merged set ships in the image unchanged. The
Rust loaders (`crosstalk-matching::data::merged`) embed the tables with
`include_str!` and parse lazily (`OnceLock`); until step 6 retires the
per-plugin tables (`data/channels`, `data/stream-mapparr`, ~8.8 MiB
already embedded) nothing outside tests references the merged
constants, so release binaries do not carry them yet.

## Channels per country

| CC | files | rows in | rows out | duplicates | field conflicts | case groups | unique to |
|----|-------|---------|----------|------------|-----------------|-------------|-----------|
| AU | CM,JAN,SM | 402 | 176 | 226 | 0 | 0 | — |
| BR | CM,JAN,SM | 2102 | 672 | 1430 | 0 | 0 | — |
| CA | CM,JAN,SM | 9884 | 3289 | 6595 | 0 | 1 | — |
| DE | CM,JAN,SM | 6563 | 2097 | 4466 | 0 | 9 | — |
| DO | SM | 306 | 306 | 0 | 0 | 0 | SM:306 |
| ES | CM,JAN,SM | 1867 | 652 | 1215 | 0 | 3 | — |
| FR | CM,JAN,SM | 2645 | 869 | 1776 | 0 | 1 | — |
| IN | CM,JAN,SM | 213 | 71 | 142 | 0 | 0 | — |
| MX | CM,JAN,SM | 4078 | 1222 | 2856 | 0 | 5 | — |
| NL | CM,JAN,SM | 459 | 153 | 306 | 0 | 0 | — |
| NO | CM,JAN,SM | 282 | 94 | 188 | 0 | 0 | — |
| PE | SM | 201 | 201 | 0 | 0 | 0 | SM:201 |
| PL | CM,SM | 764 | 382 | 382 | 0 | 0 | — |
| PT | SM | 264 | 264 | 0 | 0 | 0 | SM:264 |
| UK | CM,JAN,SM | 5484 | 1731 | 3753 | 0 | 4 | — |
| US | CM,JAN,SM | 95205 | 31792 | 63413 | 0 | 102 | — |

Entries unique to one plugin are listed per country in
`PROVENANCE.json` (`channels.<CC>.unique_to`). CM contributes no name
of its own anywhere; the only single-source databases are SM's
DO/PE/PT. Examples:

- DO only in SM (306): `3ABN`, `A&E HD`, `A&E Mundo`, `ABC HD`, `AMC`, `AMC HD`, `AXN`, `AXN HD` (+298 more)
- PE only in SM (201): `A&E`, `A&E HD`, `ALACOCINA TV`, `ALACOCINA TV HD`, `AMC`, `AMC HD`, `AMC Series HD`, `ARIRANG` (+193 more)
- PT only in SM (264): `1+1 INTERNATIONAL`, `1+1 UNITED NEWS`, `180 MUSIC`, `24 KITCHEN`, `24H`, `3SAT`, `A BOLA TV`, `AFRO MUSIC` (+256 more)

## Station field differences (CM wins, both values kept)

269 differing fields between CM and its predecessor (SM's file), by field/kind: community_served_city/case 1, community_served_city/value 3, community_served_state/value 1, facility_id/value 1, network_affiliation/case 39, network_affiliation/value 223, tv_virtual_channel/value 1. `case` = differs only in letter case. Sample:

- `K17JI-D` network_affiliation: SM `3 ANGELS BROADCASTING NETWORK` -> CM `3ABN`
- `KAIT` network_affiliation: SM `ABC, NBC, CW` -> CM `ABC/NBC/CW`
- `KAKE` network_affiliation: SM `ABC, MeTV` -> CM `ABC`
- `KATC` network_affiliation: SM `ABC; CW; GRIT; COURT TV; BOUNCE` -> CM `ABC; LAFF; GRIT; COURT TV; BOUNCE`
- `KATN` network_affiliation: SM `ABC` -> CM `ABC, CW`
- `KAUU` network_affiliation: SM `My Network, CBS and Antenna` -> CM `My Network TV (Main); CBS; Antenna TV`
- `KAZD` network_affiliation: SM `Spectrum News 1` -> CM `West`
- `KBJR-TV` network_affiliation: SM `NBC, CBS, My9, H&I` -> CM `NBC, CBS, My9, GSN`
- `KBNT-CD` network_affiliation: SM `Independent` -> CM `Univision`
- `KBTV-CD` network_affiliation: SM `SONLIFE BROADCASTING NETWORK` -> CM `SONLIFE`
- `KBTV-TV` network_affiliation: SM `DABL` -> CM `ROAR`
- `KBZK` network_affiliation: SM `CBS, MTN, Grit, Ion, Court TV, Scripps News, Laff` -> CM `CBS, MTN, Grit, Ion, Court TV, Scripps News`
- `KCBD` network_affiliation: SM `NBC and DABL` -> CM `NBC`
- `KCEB` network_affiliation: SM `CRTV NETWORK` -> CM `CRTV`
- `KCTV` network_affiliation: SM `CBS 5.1/The 365 5.2/This TV & Start 5.3/Quest 5.4/The Outlaw5.5` -> CM `CBS 5.1/The 365 5.2/Start 5.3/Quest 5.4/The Outlaw5.5`
- `KEMO-TV` network_affiliation: SM `VISION LATINA` -> CM `TIMELESS TV`
- `KESQ-TV` network_affiliation: SM `ABC, CBS, FOX, CW, Telemundo` -> CM `ABC`
- `KFJX` network_affiliation: SM `FOX, CW, START TV, DABL` -> CM `FOX, CW, START TV, (DABL through September 30, 2025)`
- `KFPX-TV` network_affiliation: SM `ION, Court TV, Laff, Mystery, IONPlus, SCRIPPS News, Jewelry TV, HSN, HSN2` -> CM `ION Court TV Laff Mystery IONPlus Busted Game Show Central HSN HSN2`
- `KFXA` network_affiliation: SM `DABL` -> CM `DABL & ROAR`
- `KGBS-CD` network_affiliation: SM `TBD NETWORK` -> CM `CRTV`
- `KGWC-TV` network_affiliation: SM `CBS` -> CM `CBS/ABC`
- `KGWL-TV` network_affiliation: SM `CBS` -> CM `CBS/ABC`
- `KGWN-TV` network_affiliation: SM `CBS / NBC / CW` -> CM `CBS/NBC/CW`
- `KGWR-TV` network_affiliation: SM `CBS` -> CM `CBS/ABC`
- ... all 269 in `PROVENANCE.json` (`stations.field_conflicts`)

SM-only stations dropped (43): `DDWAZE-TV`, `DDWDHS`, `DDWSJU-TV`, `DDWVTA`, `DDWYCC`, `DKABY-TV`, `DKBEO`, `DKCBU`, `DKCCO-TV`, `DKCFG`, `DKCPM`, `DKDYW`, `DKEFB`, `DKEJB`, `DKFYF`, `DKGHZ`, `DKJRW`, `DKOBG-TV`, `DKOCT`, `DKOVT`, `DKSWT`, `DKTUW`, `DKTVG-TV`, `DKWNV`, `DKWWF`, `DWAGT`, `DWBKI-TV`, `DWCDC-TV`, `DWCGV-TV`, `DWCMZ-TV`, `DWFXI`, `DWFXS-DT`, `DWHTV`, `DWIFR`, `DWKDH`, `DWMEI`, `DWMSY-TV`, `DWNYJ-TV`, `DWNYS-TV`, `DWPBO`, `DWSBN-TV`, `DWVIF`, `KVTV`.

## Multi-canonical aliases (kept under every claimant; step 5 decides)

136 alias values sit under two or more canonicals; 12 are disputed between plugins. Machine-readable: `data/merged/alias_conflicts.json`.

| alias | status | claimants (plugins) | omitted by | self-name |
|-------|--------|---------------------|------------|-----------|
| `5 Star Max` | agreed | `Cinemax Classics` (LU,SM); `Five Star Max` (LU,SM) | — | — |
| `5StarMax` | agreed | `Cinemax Classics` (LU,SM); `Five Star Max` (LU,SM) | — | — |
| `Action Max` | agreed | `Action Max` (LU,SM); `Cinemax Action` (LU,SM) | — | `Action Max` |
| `ActionMax` | agreed | `Action Max` (LU,SM); `Cinemax Action` (LU,SM) | — | — |
| `Alibi` | agreed | `U&alibi` (CM,JAN,LU); `alibi` (CM,LU) | — | `alibi` |
| `Animal Planet` | agreed | `Animal Planet` (CM,JAN,LU,SM); `CTV Wild Channel` (LU) | — | `Animal Planet` |
| `BTN` | agreed | `Big 10 Network` (CM,JAN,LU,SM); `Big Ten Network` (CM,JAN,LU,SM) | — | — |
| `Big 10 Network` | agreed | `Big 10 Network` (CM,JAN,LU,SM); `Big Ten Network` (CM,JAN,LU,SM) | — | `Big 10 Network` |
| `Big Ten` | agreed | `Big 10 Network` (CM,JAN,LU,SM); `Big Ten Network` (CM,JAN,LU,SM) | — | — |
| `Big Ten Network` | agreed | `Big 10 Network` (CM,JAN,LU,SM); `Big Ten Network` (CM,JAN,LU,SM) | — | `Big Ten Network` |
| `Bloomberg` | agreed | `Bloomberg TV` (CM,JAN,LU,SM); `Bloomberg Television` (CM,JAN,LU,SM) | — | — |
| `Bloomberg TV` | agreed | `Bloomberg TV` (CM,JAN,LU,SM); `Bloomberg Television` (CM,JAN,LU,SM) | — | `Bloomberg TV` |
| `Bloomberg Television` | agreed | `Bloomberg TV` (CM,JAN,LU,SM); `Bloomberg Television` (CM,JAN,LU,SM) | — | `Bloomberg Television` |
| `CN` | agreed | `Cartoon Network` (CM,JAN,LU,SM); `Cartoon Network East` (CM,JAN,LU,SM) | — | — |
| `CNN HLN` | disputed | `HLN` (CM,LU,SM); `HLN Headline News Network` (CM,LU,SM) | `HLN`: JAN | — |
| `CNN Headline News` | disputed | `HLN` (CM,LU,SM); `HLN Headline News Network` (CM,LU,SM) | `HLN`: JAN | — |
| `Cartoon Network` | agreed | `Cartoon Network` (CM,JAN,LU,SM); `Cartoon Network East` (CM,JAN,LU,SM) | — | `Cartoon Network` |
| `Cinemax Action` | agreed | `Action Max` (LU,SM); `Cinemax Action` (LU,SM) | — | `Cinemax Action` |
| `Cinemax Classics` | agreed | `Cinemax Classics` (LU,SM); `Five Star Max` (LU,SM) | — | `Cinemax Classics` |
| `Cinemax Hits` | agreed | `Cinemax Hits HD` (CM,JAN,LU,SM); `More Max` (CM,JAN,LU,SM) | — | — |
| `DIY Network` | agreed | `DIY` (CM,JAN,LU,SM); `Magnolia Network` (CM,JAN,LU,SM) | — | — |
| `Dave` | agreed | `Dave` (CM,LU); `U&Dave` (CM,JAN,LU) | — | `Dave` |
| `Discovery Science` | agreed | `CTV Nature Channel` (LU); `Discovery Science` (CM,JAN,LU); `Science` (CM,JAN,LU,SM) | — | `Discovery Science` |
| `Disney Jr` | agreed | `Disney Jr HD` (CM,JAN,LU,SM); `Disney Junior` (CM,JAN,LU,SM) | — | — |
| `Disney Junior` | agreed | `Disney Jr HD` (CM,JAN,LU,SM); `Disney Junior` (CM,JAN,LU,SM) | — | `Disney Junior` |
| `E Entertainment` | agreed | `E!` (CM,JAN,LU,SM); `E! Entertainment Television` (CM,JAN,LU,SM) | — | — |
| `E!` | agreed | `E!` (CM,JAN,LU,SM); `E! Entertainment Television` (CM,JAN,LU,SM) | — | `E!` |
| `E! Entertainment` | agreed | `E!` (CM,JAN,LU,SM); `E! Entertainment Television` (CM,JAN,LU,SM) | — | — |
| `E! Entertainment Television` | agreed | `E!` (CM,JAN,LU,SM); `E! Entertainment Television` (CM,JAN,LU,SM) | — | `E! Entertainment Television` |
| `EPIX` | agreed | `EPIX 1` (CM,JAN,LU,SM); `MGM+` (CM,JAN,LU,SM) | — | — |
| `EPIX 1` | agreed | `EPIX 1` (CM,JAN,LU,SM); `MGM+` (CM,JAN,LU,SM) | — | `EPIX 1` |
| `EPIX Hits` | agreed | `EPIX Hits` (CM,JAN,LU,SM); `MGM+ Hits` (LU,SM) | — | `EPIX Hits` |
| `Epix` | agreed | `EPIX 1` (CM,JAN,LU,SM); `MGM+` (CM,JAN,LU,SM) | — | — |
| `FS 1` | agreed | `FS1` (CM,JAN,LU,SM); `Fox Sports 1` (CM,JAN,LU,SM) | — | — |
| `FS 2` | agreed | `FS2` (CM,JAN,LU,SM); `Fox Sports 2` (CM,JAN,LU,SM) | — | — |
| `FS1` | agreed | `FS1` (CM,JAN,LU,SM); `Fox Sports 1` (CM,JAN,LU,SM) | — | `FS1` |
| `FS2` | agreed | `FS2` (CM,JAN,LU,SM); `Fox Sports 2` (CM,JAN,LU,SM) | — | `FS2` |
| `FX Movie Channel` | agreed | `FX Movie Channel` (CM,JAN,LU,SM); `FXM` (JAN,LU,SM) | — | `FX Movie Channel` |
| `FXM` | agreed | `FX Movie Channel` (CM,JAN,LU,SM); `FXM` (JAN,LU,SM) | — | `FXM` |
| `FXM Retro` | agreed | `FX Movie Channel` (CM,JAN,LU,SM); `FXM` (JAN,LU,SM) | — | — |
| `FXMovie` | agreed | `FX Movie Channel` (CM,JAN,LU,SM); `FXM` (JAN,LU,SM) | — | — |
| `Five Star Max` | agreed | `Cinemax Classics` (LU,SM); `Five Star Max` (LU,SM) | — | `Five Star Max` |
| `Food Network` | agreed | `Food Network` (CM,JAN,LU,SM); `SBS Food` (JAN) | — | `Food Network` |
| `Fox Movie Channel` | agreed | `FX Movie Channel` (CM,JAN,LU,SM); `FXM` (JAN,LU,SM) | — | — |
| `Fox Sport 1` | agreed | `FS1` (CM,JAN,LU,SM); `Fox Sports 1` (CM,JAN,LU,SM) | — | — |
| `Fox Sport 2` | agreed | `FS2` (CM,JAN,LU,SM); `Fox Sports 2` (CM,JAN,LU,SM) | — | — |
| `Fox Sports 1` | agreed | `FS1` (CM,JAN,LU,SM); `Fox Sports 1` (CM,JAN,LU,SM) | — | `Fox Sports 1` |
| `Fox Sports 2` | agreed | `FS2` (CM,JAN,LU,SM); `Fox Sports 2` (CM,JAN,LU,SM) | — | `Fox Sports 2` |
| `GAC` | disputed | `Great American Country` (LU); `Great American Family` (LU) | `Great American Country`: SM; `Great American Family`: SM | — |
| `GETTV` | disputed | `GET` (LU,SM); `Great Entertainment Television` (LU,SM); `getTV` (LU,SM) | `getTV`: CM,JAN | `getTV` |
| `GOLF` | agreed | `GOLF Channel` (CM,JAN,LU,SM); `Golf Channel` (CM,JAN,LU,SM) | — | — |
| `Get TV` | agreed | `GET` (LU,SM); `Great Entertainment Television` (LU,SM); `getTV` (CM,JAN,LU,SM) | — | — |
| `Gold` | agreed | `Gold` (CM,LU); `U&GOLD` (CM,JAN,LU) | — | `Gold` |
| `Golf Ch` | agreed | `GOLF Channel` (CM,JAN,LU,SM); `Golf Channel` (CM,JAN,LU,SM) | — | — |
| `Golf Channel` | agreed | `GOLF Channel` (CM,JAN,LU,SM); `Golf Channel` (CM,JAN,LU,SM) | — | — |
| `Great American Country` | agreed | `Great American Country` (LU,SM); `Great American Family` (LU,SM) | — | `Great American Country` |
| `Great American Family` | agreed | `Great American Country` (LU,SM); `Great American Family` (LU,SM) | — | `Great American Family` |
| `Great Entertainment Television` | disputed | `GET` (LU,SM); `Great Entertainment Television` (LU,SM); `getTV` (LU,SM) | `getTV`: CM,JAN | `Great Entertainment Television` |
| `HBO 2` | agreed | `HBO 2` (CM,JAN,LU,SM); `HBO Hits HD East` (CM,JAN,LU,SM) | — | `HBO 2` |
| `HBO Drama` | agreed | `HBO Drama HD East` (CM,JAN,LU,SM); `HBO Signature` (CM,JAN,LU,SM) | — | — |
| `HBO Hits` | agreed | `HBO 2` (CM,JAN,LU,SM); `HBO Hits HD East` (CM,JAN,LU,SM) | — | — |
| `HBO Movies` | agreed | `HBO Movies HD` (CM,JAN,LU,SM); `HBO Zone` (CM,JAN,LU,SM) | — | — |
| `HBO Movies HD` | agreed | `HBO Movies HD` (CM,JAN,LU,SM); `HBO Zone` (CM,JAN,LU,SM) | — | `HBO Movies HD` |
| `HBO Signature` | agreed | `HBO Drama HD East` (CM,JAN,LU,SM); `HBO Signature` (CM,JAN,LU,SM) | — | `HBO Signature` |
| `HBO Zone` | agreed | `HBO Movies HD` (CM,JAN,LU,SM); `HBO Zone` (CM,JAN,LU,SM) | — | `HBO Zone` |
| `HBO2` | agreed | `HBO 2` (CM,JAN,LU,SM); `HBO Hits HD East` (CM,JAN,LU,SM) | — | — |
| `HLN` | agreed | `HLN` (CM,JAN,LU,SM); `HLN Headline News Network` (CM,LU,SM) | — | `HLN` |
| `Hallmark Mystery` | agreed | `Hallmark Movies & Mysteries` (CM,JAN,LU,SM); `Hallmark Mystery` (CM,JAN,LU,SM) | — | `Hallmark Mystery` |
| `Headline News` | agreed | `HLN` (CM,JAN,LU,SM); `HLN Headline News Network` (CM,LU,SM) | — | — |
| `Headline News Network` | disputed | `HLN` (CM,LU,SM); `HLN Headline News Network` (CM,LU,SM) | `HLN`: JAN | — |
| `Justice Central` | disputed | `Justice Central.TV` (LU,SM); `Justice Network` (LU,SM); `True Crime Network` (LU,SM) | `Justice Network`: JAN | — |
| `Justice Central TV` | disputed | `Justice Central.TV` (LU,SM); `Justice Network` (LU,SM); `True Crime Network` (LU,SM) | `Justice Network`: JAN | — |
| `Justice Central.TV` | disputed | `Justice Central.TV` (LU,SM); `Justice Network` (LU,SM); `True Crime Network` (LU,SM) | `Justice Network`: JAN | `Justice Central.TV` |
| `Justice Network` | agreed | `Justice Central.TV` (LU,SM); `Justice Network` (JAN,LU,SM); `True Crime Network` (LU,SM) | — | `Justice Network` |
| `LMN` | agreed | `LMN` (CM,JAN,LU,SM); `Lifetime Movie Network` (CM,JAN,LU,SM) | — | `LMN` |
| `LMN HD` | agreed | `LMN` (CM,JAN,LU,SM); `Lifetime Movie Network` (CM,JAN,LU,SM) | — | — |
| `Lifetime Movie Network` | agreed | `LMN` (CM,JAN,LU,SM); `Lifetime Movie Network` (CM,JAN,LU,SM) | — | `Lifetime Movie Network` |
| `MGM Hits` | agreed | `EPIX Hits` (CM,JAN,LU,SM); `MGM+ Hits` (LU,SM) | — | — |
| `MGM+` | agreed | `EPIX 1` (CM,JAN,LU,SM); `MGM+` (CM,JAN,LU,SM) | — | `MGM+` |
| `MGM+ East` | agreed | `EPIX 1` (CM,JAN,LU,SM); `MGM+` (CM,JAN,LU,SM) | — | — |
| `MGM+ Hits` | agreed | `EPIX Hits` (CM,JAN,LU,SM); `MGM+ Hits` (LU,SM) | — | `MGM+ Hits` |
| `MS Now` | agreed | `MS Now` (CM,JAN,LU,SM); `MSNBC` (CM,JAN,LU,SM) | — | `MS Now` |
| `MSNBC` | agreed | `MS Now` (CM,JAN,LU,SM); `MSNBC` (CM,JAN,LU,SM) | — | `MSNBC` |
| `MSNBC Now` | agreed | `MS Now` (CM,JAN,LU,SM); `MSNBC` (CM,JAN,LU,SM) | — | — |
| `Magnolia` | agreed | `DIY` (CM,JAN,LU,SM); `Magnolia Network` (CM,JAN,LU,SM) | — | — |
| `Magnolia Network` | agreed | `DIY` (CM,JAN,LU,SM); `Magnolia Network` (CM,JAN,LU,SM) | — | `Magnolia Network` |
| `More Max` | agreed | `Cinemax Hits HD` (CM,JAN,LU,SM); `More Max` (CM,JAN,LU,SM) | — | `More Max` |
| `MoreMax` | agreed | `Cinemax Hits HD` (CM,JAN,LU,SM); `More Max` (CM,JAN,LU,SM) | — | — |
| `Movie Channel Xtra` | agreed | `The Movie Channel Xtra` (CM,JAN,LU,SM); `The Movie Channel Xtra (E)` (CM,JAN,LU,SM) | — | — |
| `NBC GOLF` | agreed | `GOLF Channel` (CM,JAN,LU,SM); `Golf Channel` (CM,JAN,LU,SM) | — | — |
| `NBC Golf Channel` | agreed | `GOLF Channel` (CM,JAN,LU,SM); `Golf Channel` (CM,JAN,LU,SM) | — | — |
| `Nat Geo` | agreed | `National Geographic` (CM,JAN,LU,SM); `National Geographic Channel` (CM,JAN,LU,SM) | — | — |
| `Nat Geo Wild` | agreed | `Nat Geo WILD` (CM,JAN,LU,SM); `Nat Geo Wild` (CM,JAN,LU,SM) | — | — |
| `NatGeo` | agreed | `National Geographic` (CM,JAN,LU,SM); `National Geographic Channel` (CM,JAN,LU,SM) | — | — |
| `NatGeo Wild` | agreed | `Nat Geo WILD` (CM,JAN,LU,SM); `Nat Geo Wild` (CM,JAN,LU,SM) | — | — |
| `National Geographic` | agreed | `National Geographic` (CM,JAN,LU,SM); `National Geographic Channel` (CM,JAN,LU,SM) | — | `National Geographic` |
| `National Geographic Wild` | agreed | `Nat Geo WILD` (CM,JAN,LU,SM); `Nat Geo Wild` (CM,JAN,LU,SM) | — | — |
| `News Nation` | agreed | `NewsNation` (CM,JAN,LU,SM); `WGN America` (LU,SM) | — | — |
| `NewsNation` | agreed | `NewsNation` (CM,JAN,LU,SM); `WGN America` (LU,SM) | — | `NewsNation` |
| `Nick` | agreed | `Nick/Nick at Nite (E)` (CM,JAN); `Nickelodeon` (LU,SM); `Nickelodeon East` (CM,JAN,LU,SM) | — | — |
| `Nick at Nite` | agreed | `Nick/Nick at Nite (E)` (CM,JAN); `Nickelodeon` (LU,SM) | — | — |
| `Nickelodeon` | agreed | `Nick/Nick at Nite (E)` (CM,JAN); `Nickelodeon` (LU,SM); `Nickelodeon East` (CM,JAN,LU,SM) | — | `Nickelodeon` |
| `Nickelodeon East` | agreed | `Nick/Nick at Nite (E)` (CM,JAN); `Nickelodeon` (LU,SM); `Nickelodeon East` (CM,JAN,LU,SM) | — | `Nickelodeon East` |
| `OWN` | agreed | `OWN` (CM,JAN,LU,SM); `OWN: Oprah Winfrey Network` (CM,JAN,LU,SM) | — | `OWN` |
| `Oprah Winfrey Network` | agreed | `OWN` (CM,JAN,LU,SM); `OWN: Oprah Winfrey Network` (CM,JAN,LU,SM) | — | — |
| `Oxygen True Crime` | agreed | `Oxygen` (CM,JAN,LU,SM); `Oxygen True Crime` (CM,JAN,LU,SM) | — | `Oxygen True Crime` |
| `RDS` | agreed | `Réseau des Sports (RDS)` (CM); `Réseau des Sports (RDS) HD` (CM) | — | — |
| `RDS 1` | agreed | `Réseau des Sports (RDS)` (CM); `Réseau des Sports (RDS) HD` (CM) | — | — |
| `RDS 2` | agreed | `RDS2` (CM); `RDS2 HD` (CM) | — | — |
| `RDS 2 HD` | agreed | `RDS2` (CM); `RDS2 HD` (CM) | — | — |
| `RDS HD` | agreed | `Réseau des Sports (RDS)` (CM); `Réseau des Sports (RDS) HD` (CM) | — | — |
| `RDS INFO` | agreed | `Réseau des Sports Info` (CM); `Réseau des Sports Info HD` (CM) | — | — |
| `RDS Info` | agreed | `Réseau des Sports Info` (CM); `Réseau des Sports Info HD` (CM) | — | — |
| `RDS2` | agreed | `RDS2` (CM); `RDS2 HD` (CM) | — | `RDS2` |
| `Reelz` | agreed | `REELZ` (LU,SM); `ReelzChannel` (CM,JAN,LU,SM) | — | `REELZ` |
| `ReelzChannel` | agreed | `REELZ` (LU,SM); `ReelzChannel` (CM,JAN,LU,SM) | — | `ReelzChannel` |
| `Showtime` | agreed | `Paramount+ with SHOWTIME EAST` (CM,JAN,LU,SM); `Showtime (E)` (CM,JAN,LU,SM) | — | — |
| `Showtime 2` | agreed | `SHOWTIME 2 East` (CM,JAN,LU,SM); `Showtime 2` (CM,JAN,LU,SM) | — | `Showtime 2` |
| `Showtime 2 (East)` | agreed | `SHOWTIME 2 East` (CM,JAN,LU,SM); `Showtime 2` (CM,JAN,LU,SM) | — | — |
| `Showtime 2 East` | agreed | `SHOWTIME 2 East` (CM,JAN,LU,SM); `Showtime 2` (CM,JAN,LU,SM) | — | `SHOWTIME 2 East` |
| `Showtime East` | agreed | `Paramount+ with SHOWTIME EAST` (CM,JAN,LU,SM); `Showtime (E)` (CM,JAN,LU,SM) | — | — |
| `TCM` | agreed | `TCM` (CM,JAN,LU,SM); `Turner Classic Movies` (LU,SM) | — | `TCM` |
| `TMC Xtra` | agreed | `The Movie Channel Xtra` (CM,JAN,LU,SM); `The Movie Channel Xtra (E)` (CM,JAN,LU,SM) | — | — |
| `TUDN` | agreed | `TUDN` (CM,JAN,LU,SM); `Univision Deportes` (CM,LU,SM) | — | `TUDN` |
| `Tastemade` | agreed | `Tastemade` (LU,SM); `Tastemade Home` (CM,JAN,LU,SM) | — | `Tastemade` |
| `True Crime Network` | agreed | `Justice Central.TV` (LU,SM); `Justice Network` (JAN,LU,SM); `True Crime Network` (LU,SM) | — | `True Crime Network` |
| `Turner Classic Movies` | agreed | `TCM` (CM,JAN,LU,SM); `Turner Classic Movies` (LU,SM) | — | `Turner Classic Movies` |
| `UP TV` | agreed | `UP` (LU,SM); `UPTV` (CM,JAN,LU,SM) | — | — |
| `UPTV` | agreed | `UP` (LU,SM); `UPTV` (CM,JAN,LU,SM) | — | `UPTV` |
| `US GOLF` | agreed | `GOLF Channel` (CM,JAN,LU,SM); `Golf Channel` (CM,JAN,LU,SM) | — | — |
| `Univision Deportes` | agreed | `TUDN` (CM,JAN,LU,SM); `Univision Deportes` (CM,LU,SM) | — | `Univision Deportes` |
| `Univision Deportes Network` | disputed | `TUDN` (CM,LU,SM); `Univision Deportes` (CM,LU,SM) | `TUDN`: JAN | — |
| `WGN` | disputed | `NewsNation` (LU,SM); `WGN America` (LU,SM) | `NewsNation`: CM,JAN | — |
| `WGN America` | disputed | `NewsNation` (LU,SM); `WGN America` (LU,SM) | `NewsNation`: CM,JAN | `WGN America` |
| `Yesterday` | agreed | `U&YESTERDAY` (CM,JAN,LU); `Yesterday` (CM,LU) | — | `Yesterday` |
| `getTV` | agreed | `GET` (LU,SM); `Great Entertainment Television` (LU,SM); `getTV` (CM,JAN,LU,SM) | — | `getTV` |

## Inputs

| file | sha256 | header |
|------|--------|--------|
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/AU_channels.json | dba640173ef6a411... | 2026-05-17 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/BR_channels.json | 17e5cfed0a51352e... | 2025-12-08 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/CA_channels.json | 7da444b4e725c1d5... | 2026-05-17 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/DE_channels.json | cdc09150c7331617... | 2025-12-08 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/ES_channels.json | d7a2470ffd1c865c... | 2026-05-17 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/FR_channels.json | 7b52e05cf8c64d78... | 2025-12-08 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/IN_channels.json | 2ce773818e4a7a41... | 2025-11-10 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/MX_channels.json | 90d2c07b47b8b32e... | 2025-12-08 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/NL_channels.json | 6afcb9785a803e12... | 2025-12-14-v2 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/NO_channels.json | da5b8fd7c91e3612... | 2026-06-10 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/UK_channels.json | a4d27d7327e336e6... | 2026-05-17 |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/US_channels.json | 54f8962d14a0a57e... | 2026-05-17 |
| reference/Stream-Mapparr/Stream-Mapparr/AU_channels.json | dba640173ef6a411... | 2026-05-17 |
| reference/Stream-Mapparr/Stream-Mapparr/BR_channels.json | 17e5cfed0a51352e... | 2025-12-08 |
| reference/Stream-Mapparr/Stream-Mapparr/CA_channels.json | 7da444b4e725c1d5... | 2026-05-17 |
| reference/Stream-Mapparr/Stream-Mapparr/DE_channels.json | cdc09150c7331617... | 2025-12-08 |
| reference/Stream-Mapparr/Stream-Mapparr/DO_channels.json | 66cf93cf3f2cabde... | 2026-07-27 |
| reference/Stream-Mapparr/Stream-Mapparr/ES_channels.json | d7a2470ffd1c865c... | 2026-05-17 |
| reference/Stream-Mapparr/Stream-Mapparr/FR_channels.json | 7b52e05cf8c64d78... | 2025-12-08 |
| reference/Stream-Mapparr/Stream-Mapparr/IN_channels.json | 2ce773818e4a7a41... | 2025-11-10 |
| reference/Stream-Mapparr/Stream-Mapparr/MX_channels.json | 90d2c07b47b8b32e... | 2025-12-08 |
| reference/Stream-Mapparr/Stream-Mapparr/NL_channels.json | 6afcb9785a803e12... | 2025-12-14-v2 |
| reference/Stream-Mapparr/Stream-Mapparr/NO_channels.json | da5b8fd7c91e3612... | 2026-06-10 |
| reference/Stream-Mapparr/Stream-Mapparr/PE_channels.json | 3e98c45438652ccb... | 2026-07-24 |
| reference/Stream-Mapparr/Stream-Mapparr/PL_channels.json | e28e502f5443d2db... | 2026-10-06 |
| reference/Stream-Mapparr/Stream-Mapparr/PT_channels.json | 0d312a8ed588c924... | 2026-09-01 |
| reference/Stream-Mapparr/Stream-Mapparr/UK_channels.json | a4d27d7327e336e6... | 2026-05-17 |
| reference/Stream-Mapparr/Stream-Mapparr/US_channels.json | 3a9464d0c376033c... | 2026-05-17 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/AU_channels.json | f184b0ffbd17c04e... | 2025-11-10 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/BR_channels.json | d5d6a942c4f4b1cb... | 2025-12-08 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/CA_channels.json | be84c9e568957460... | 2025-12-08 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/DE_channels.json | 4da7b94e6548500d... | 2025-12-08 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/ES_channels.json | a0c1e238256507b1... | 2025-12-08 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/FR_channels.json | 57e99bcc38d960cf... | 2025-12-08 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/IN_channels.json | 2ce773818e4a7a41... | 2025-11-10 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/MX_channels.json | 79c2c2511dc7ad59... | 2025-12-08 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/NL_channels.json | 6afcb9785a803e12... | 2025-12-14-v2 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/NO_channels.json | da5b8fd7c91e3612... | 2026-06-10 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/PL_channels.json | e28e502f5443d2db... | 2026-10-06 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/UK_channels.json | 4a11a81b6d8c08e4... | 2025-12-08 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/US_channels.json | 456b61dce2feb5d1... | 2025-12-08 |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/networks.json | c9561186e4c73c23... | — |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/networks_supplemental.json | a66d5591e68789a7... | — |
| reference/Stream-Mapparr/Stream-Mapparr/networks.json | 1ddc72f8f763a107... | — |
| reference/Dispatcharr-Channel-Maparr-Plugin/scripts/networks_corrections.json | 5b6252fb6fed1192... | — |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/us_station_callsigns.json | f220286a42b48f08... | — |
| reference/Dispatcharr-EPG-Janitor-Plugin/EPG-Janitor/aliases.py | c502c3e4780a8453... | — |
| reference/Stream-Mapparr/Stream-Mapparr/aliases.py | 18597df004c31308... | — |
| reference/Dispatcharr-Channel-Maparr-Plugin/Channel-Maparr/aliases.py | b686347213729edc... | — |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/aliases.py | 756d3bafc79a93ab... | — |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/AU_Foxtel_lineup.json | 383f1387ad2d17d0... | 2026-04-10 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/CA_Telus-Optik_lineup.json | 6ea3cd178ea0c6cc... | 2026-04-05 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/DO_Altice_lineup.json | 05cfdc5db91f4551... | 2018-02-28 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/DO_Claro_lineup.json | 628e526f183cd186... | 2018-03-02 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/DO_Combined_lineup.json | 36e3991e75fb9a55... | 2026-07-27 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/ES_Movistar_lineup.json | 67fcff5a767afebb... | 2026-05-09 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/FR_CanalPlus_TNT_lineup.json | 52535fb39d0dbdf4... | 2026-06-01 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/FR_CanalPlus_lineup.json | 7d895d2861a334d4... | 2026-06-01 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/NL_KPN_lineup.json | 18c657f89f463d08... | 2026-07-01 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/NL_ODIDO_lineup.json | 08e7b3016cefdb6e... | 2026-04-05 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/PE_Claro_lineup.json | 09d20dcf447b67d2... | 2026-07-24 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/QA_beIN-MENA_lineup.json | f54d011293ed1be2... | 2026-09-07 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/TZ_Azam-TV_lineup.json | a69aa06f8057dbb0... | 2026-10-14 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/TZ_Combined_lineup.json | 9f3efbbaaffe1265... | 2026-10-14 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/TZ_DStv-Premium_lineup.json | 1ede098dc4096e98... | 2026-10-14 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/UK_Combined_lineup.json | 00ce2746d1d7f8fc... | 2026-05-22 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/UK_Freeview_lineup.json | 86f21075e6e68101... | 2026-05-20 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/UK_SkyTV_ENG_full_lineup.json | f4d091bfb0d514c8... | 2026-05-04 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/UK_SkyTV_ENG_simple_lineup.json | d1550cbeb44d9a15... | 2026-05-04 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/UK_SkyTV_lineup.json | ffac4886539b127e... | 2026-04-05 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/US_Combined_lineup.json | 28218382c7c2c830... | 2026-05-22 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/US_DISH-Top250_lineup.json | e14a023dcbc19b6a... | 2026-04-05 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/US_DirecTV-Premier_lineup.json | b35b79cf69f36fa4... | 2026-04-05 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/US_Optimum_lineup.json | cb56d66cac4d1b05... | 2026-01-01 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/US_Spectrum-Tampa-Bay-All_lineup.json | 3a5e6dd55417ffa8... | 2026-08-18 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/US_Spectrum-Tampa-Bay-No-Spanish_lineup.json | 4ab541f7d27d5a61... | 2026-08-18 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/US_Spectrum-Tampa-Bay-Spanish-Only_lineup.json | d8f8af1760172391... | 2026-08-18 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/US_Verizon-FIOS-All-11743_lineup.json | 075bf2e75787c416... | 2026-07-19 |
| reference/Dispatcharr-Lineuparr-Plugin/Lineuparr/US_Verizon-FIOS_lineup.json | 7e64fba86ed25d78... | 2026-04-05 |
