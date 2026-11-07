# Lineup sync + tidy oracle report (step 18)

Lineup `sync_*` mirrors Lineuparr's group + channel sync on the
owned-channel rule; `tidy_*` is its post-match cleanup as hide instead
of delete. Lineuparr (`reference/Dispatcharr-Lineuparr-Plugin`) is the
oracle: `_sync_groups` + `_do_sync_channels` (get_or_create by
`(name, group)`, `_get_channel_number` numbering, renumber-existing,
`_enable_channels_in_profiles`) and `_migrate_single_group_name`, driven
with ORM fakes over 29 lineup files x 4 numbering modes x 4
`category_detail` modes (464 first runs + 464 reruns), a foreign
same-name + number-squat subset (116 cases), and the cleanup query chain
vs `tidy_dump`. Rust side: `sync_dump` / `tidy_dump` (the actions' own
`decide_sync` + `plan_sync_creates` / `decide_tidy`). Bar: 0 unattributed.

Cases: 580 first runs + 580 reruns per side (464 clean + 116 clash/squat each).

## ownership-not-adopt (116)

- C/AU_Foxtel_lineup.json/auto_highest/normal: foreign 'Sky News' in 'Foxtel: News': LU adopts id 9001, Rust skips
- C/AU_Foxtel_lineup.json/auto_next/normal: foreign 'Sky News' in 'Foxtel: News': LU adopts id 9001, Rust skips
- C/AU_Foxtel_lineup.json/lineup/normal: foreign 'Sky News' in 'Foxtel: News': LU adopts id 9001, Rust skips
- C/AU_Foxtel_lineup.json/specific/normal: foreign 'Sky News' in 'Foxtel: News': LU adopts id 9001, Rust skips
- C/CA_Telus-Optik_lineup.json/auto_highest/normal: foreign 'Al Jazeera English' in 'TELUS: News': LU adopts id 9001, Rust skips
- C/CA_Telus-Optik_lineup.json/auto_next/normal: foreign 'Al Jazeera English' in 'TELUS: News': LU adopts id 9001, Rust skips
- C/CA_Telus-Optik_lineup.json/lineup/normal: foreign 'Al Jazeera English' in 'TELUS: News': LU adopts id 9001, Rust skips
- C/CA_Telus-Optik_lineup.json/specific/normal: foreign 'Al Jazeera English' in 'TELUS: News': LU adopts id 9001, Rust skips
- C/DO_Altice_lineup.json/auto_highest/normal: foreign 'Caribbean TV HD' in 'Altice: Nacionales': LU adopts id 9001, Rust skips
- C/DO_Altice_lineup.json/auto_next/normal: foreign 'Caribbean TV HD' in 'Altice: Nacionales': LU adopts id 9001, Rust skips

(106 more)

## stable-numbers-on-rerun (447)

- A/AU_Foxtel_lineup.json/auto_highest/none: rerun: LU updates 139 channel(s), Rust 0
- A/AU_Foxtel_lineup.json/auto_highest/normal: rerun: LU updates 139 channel(s), Rust 0
- A/AU_Foxtel_lineup.json/auto_highest/refined: rerun: LU updates 139 channel(s), Rust 0
- A/AU_Foxtel_lineup.json/auto_highest/simple: rerun: LU updates 139 channel(s), Rust 0
- A/AU_Foxtel_lineup.json/auto_next/none: rerun: LU updates 139 channel(s), Rust 0
- A/AU_Foxtel_lineup.json/auto_next/normal: rerun: LU updates 139 channel(s), Rust 0
- A/AU_Foxtel_lineup.json/auto_next/refined: rerun: LU updates 139 channel(s), Rust 0
- A/AU_Foxtel_lineup.json/auto_next/simple: rerun: LU updates 139 channel(s), Rust 0
- A/AU_Foxtel_lineup.json/specific/none: rerun: LU updates 139 channel(s), Rust 0
- A/AU_Foxtel_lineup.json/specific/normal: rerun: LU updates 139 channel(s), Rust 0

(437 more)

## owner-key-ignores-category (2)

- detail-change/AU_Foxtel_lineup.json: normal -> simple moves 125 owned channel(s), 0 creates
- detail-change/US_Verizon-FIOS_lineup.json: normal -> simple moves 167 owned channel(s), 0 creates

## global-number-check (983)

- A/CA_Telus-Optik_lineup.json/lineup/none: 'Sportsnet Vancouver Hockey': LU sets 909 anyway; Rust: channel number 909 is already used by channel "Sportsnet Oilers" (planned in this run) (numbers are global in Dispatcharr's outputs), so this create is skipped
- A/CA_Telus-Optik_lineup.json/lineup/normal: 'Sportsnet Vancouver Hockey': LU sets 909 anyway; Rust: channel number 909 is already used by channel "Sportsnet Oilers" (planned in this run) (numbers are global in Dispatcharr's outputs), so this create is skipped
- A/CA_Telus-Optik_lineup.json/lineup/refined: 'Sportsnet Vancouver Hockey': LU sets 909 anyway; Rust: channel number 909 is already used by channel "Sportsnet Oilers" (planned in this run) (numbers are global in Dispatcharr's outputs), so this create is skipped
- A/CA_Telus-Optik_lineup.json/lineup/simple: 'Sportsnet Vancouver Hockey': LU sets 909 anyway; Rust: channel number 909 is already used by channel "Sportsnet Oilers" (planned in this run) (numbers are global in Dispatcharr's outputs), so this create is skipped
- A/DO_Claro_lineup.json/lineup/none: 'CB 24': LU sets 555 anyway; Rust: channel number 555 is already used by channel "FXM" (planned in this run) (numbers are global in Dispatcharr's outputs), so this create is skipped
- A/DO_Claro_lineup.json/lineup/none: 'Caribbean TV HD': LU sets 534 anyway; Rust: channel number 534 is already used by channel "France 24" (planned in this run) (numbers are global in Dispatcharr's outputs), so this create is skipped
- A/DO_Claro_lineup.json/lineup/none: 'Distrito Comedia': LU sets 209 anyway; Rust: channel number 209 is already used by channel "Az Corazón" (planned in this run) (numbers are global in Dispatcharr's outputs), so this create is skipped
- A/DO_Claro_lineup.json/lineup/none: 'Pasiones HD': LU sets 678 anyway; Rust: channel number 678 is already used by channel "HOLA TV HD" (planned in this run) (numbers are global in Dispatcharr's outputs), so this create is skipped
- A/DO_Claro_lineup.json/lineup/normal: 'CB 24': LU sets 555 anyway; Rust: channel number 555 is already used by channel "FXM" (planned in this run) (numbers are global in Dispatcharr's outputs), so this create is skipped
- A/DO_Claro_lineup.json/lineup/normal: 'Caribbean TV HD': LU sets 534 anyway; Rust: channel number 534 is already used by channel "France 24" (planned in this run) (numbers are global in Dispatcharr's outputs), so this create is skipped

(973 more)

## numbering-skips-dont-consume (27180)

- A/DO_Altice_lineup.json/auto_highest/none: 'A&E HD': LU 206 vs Rust 199.0: LU spent numbers on entries Rust skips; Rust's creates take LU's sequence in order
- A/DO_Altice_lineup.json/auto_highest/none: 'AMC HD': LU 211 vs Rust 204.0: LU spent numbers on entries Rust skips; Rust's creates take LU's sequence in order
- A/DO_Altice_lineup.json/auto_highest/none: 'AXN HD': LU 202 vs Rust 195.0: LU spent numbers on entries Rust skips; Rust's creates take LU's sequence in order
- A/DO_Altice_lineup.json/auto_highest/none: 'C-Music HD': LU 187 vs Rust 184.0: LU spent numbers on entries Rust skips; Rust's creates take LU's sequence in order
- A/DO_Altice_lineup.json/auto_highest/none: 'CDN HD': LU 196 vs Rust 190.0: LU spent numbers on entries Rust skips; Rust's creates take LU's sequence in order
- A/DO_Altice_lineup.json/auto_highest/none: 'CDN SportsMax HD': LU 194 vs Rust 188.0: LU spent numbers on entries Rust skips; Rust's creates take LU's sequence in order
- A/DO_Altice_lineup.json/auto_highest/none: 'Canal de las Estrellas HD': LU 191 vs Rust 187.0: LU spent numbers on entries Rust skips; Rust's creates take LU's sequence in order
- A/DO_Altice_lineup.json/auto_highest/none: 'Cinecanal HD': LU 208 vs Rust 201.0: LU spent numbers on entries Rust skips; Rust's creates take LU's sequence in order
- A/DO_Altice_lineup.json/auto_highest/none: 'Cinemax HD': LU 210 vs Rust 203.0: LU spent numbers on entries Rust skips; Rust's creates take LU's sequence in order
- A/DO_Altice_lineup.json/auto_highest/none: 'Comedy Central HD': LU 214 vs Rust 207.0: LU spent numbers on entries Rust skips; Rust's creates take LU's sequence in order

(27170 more)

## duplicates-in-lineup (742)

- A/DO_Altice_lineup.json/auto_highest/none: 'Acento TV HD' twice: LU one channel per group, Rust first-category-wins skip
- A/DO_Altice_lineup.json/auto_highest/none: 'Acento TV HD': LU 198 vs Rust 36.0: LU's later duplicate in 'Altice' renumbered the channel; Rust keeps the first entry's
- A/DO_Altice_lineup.json/auto_highest/none: 'Azteca Corazón HD' twice: LU one channel per group, Rust first-category-wins skip
- A/DO_Altice_lineup.json/auto_highest/none: 'Azteca Corazón HD': LU 192 vs Rust 124.0: LU's later duplicate in 'Altice' renumbered the channel; Rust keeps the first entry's
- A/DO_Altice_lineup.json/auto_highest/none: 'Comunión TV HD' twice: LU one channel per group, Rust first-category-wins skip
- A/DO_Altice_lineup.json/auto_highest/none: 'Comunión TV HD': LU 193 vs Rust 63.0: LU's later duplicate in 'Altice' renumbered the channel; Rust keeps the first entry's
- A/DO_Altice_lineup.json/auto_highest/none: 'Film & Arts HD' twice: LU one channel per group, Rust first-category-wins skip
- A/DO_Altice_lineup.json/auto_highest/none: 'Film & Arts HD': LU 178 vs Rust 144.0: LU's later duplicate in 'Altice' renumbered the channel; Rust keeps the first entry's
- A/DO_Altice_lineup.json/auto_highest/none: 'NHK HD' twice: LU one channel per group, Rust first-category-wins skip
- A/DO_Altice_lineup.json/auto_highest/none: 'NHK HD': LU 171 vs Rust 69.0: LU's later duplicate in 'Altice' renumbered the channel; Rust keeps the first entry's

(732 more)

## profile-rows-only-on-create (5)

- C/AU_Foxtel_lineup.json/auto_highest/normal: LU enables the adopted foreign channel 9001 in 'Living Room'; Rust sets profile_ids on the create POST only (9001 never created)
- C/AU_Foxtel_lineup.json/auto_next/normal: LU enables the adopted foreign channel 9001 in 'Living Room'; Rust sets profile_ids on the create POST only (9001 never created)
- C/AU_Foxtel_lineup.json/lineup/normal: LU enables the adopted foreign channel 9001 in 'Living Room'; Rust sets profile_ids on the create POST only (9001 never created)
- C/AU_Foxtel_lineup.json/specific/normal: LU enables the adopted foreign channel 9001 in 'Living Room'; Rust sets profile_ids on the create POST only (9001 never created)
- unknown profile 'Nope' is skipped on both sides (checked: Rust resolves [1] + lists 'Nope', LU's memberships touch profile 1 only); existing owned channels never gain membership rows (phase-2 rule, by construction: only creates carry ids)

## migration-owned-only (3)

- migration/both-exist: both 'DIRECTV: All' and 'DIRECTV' exist: LU warns and leaves both, Rust skips with the reason
- migration/foreign: LU renames any 'DIRECTV: All' group; Rust leaves the foreign group alone and reports it
- migration/owned: pre-fix 'DIRECTV: All' -> 'DIRECTV' on both sides (rename, channels stay assigned)

## cleanup-delete-vs-hide (1)

- channel 1 owned + stream-less in a lineup group: LU deletes it, tidy hides it (undoable)

## cleanup-owned-only (3)

- channel 8 is owned but outside the lineup groups: LU ignores it (group-name based), tidy hides it (ownership based, not group names)
- foreign stream-less channel 6 in a lineup group: LU deletes it, tidy has no row (owned-only)
- foreign stream-less channel 7 in a lineup group: LU deletes it, tidy has no row (owned-only)

## cleanup-unhide-guard (3)

- channel 2 hidden, still stream-less: stays hidden (no-op)
- channel 3 hidden with streams again after tidy hid it: tidy unhides (LU has no unhide)
- channel 4 hidden with streams but tidy never hid it: operator hide stays (never unhidden)

## unattributed (0)

(none: every difference is attributed)
