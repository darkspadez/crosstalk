# Dispatcharr plugins on cedro: what each one does

Source: each plugin's `plugin.json` (Reservoarr: `plugin.py`), shallow clones taken 2026-11-02.
"Event" actions run automatically on a Dispatcharr event rather than from a button.

## Overview

| Plugin | Author | Purpose | Actions | crosstalk status |
|---|---|---|---|---|
| EPG Janitor | PiratesIRC | Find channels whose EPG has no programme data; auto-match and heal EPG assignments | 15 | Ported: 6 of 15 |
| Stream-Mapparr | PiratesIRC | Fuzzy-match streams to channels, order alternates by quality | 21 | — (recommended next) |
| Channel-Mapparr | PiratesIRC | Standardize channel names (FCC OTA + 13 country lists), categories, logos, channels from streams | 13 | — |
| Lineuparr | PiratesIRC | Mirror a real provider lineup as groups/channels, attach streams, EPG, logos | 11 | — |
| IPTV Checker | PiratesIRC | ffprobe every stream; act on dead, blank, low-fps channels | 23 | — |
| Event Channel Managarr | PiratesIRC | Hide/show event channels by date in the name; dummy EPG for them | 9 | — |
| EPG & Sports Editor | jstevenscl | Cleaned virtual EPG copies, placeholder EPG, sports channel renames from a live schedule | 16 | — |
| Channel Name Cleaner | noodlemctwoodle | Strip provider junk from channel names with undo | 5 | — |
| Reservoarr | brko7 | Installs a RAM delay-buffer stream profile for Plex | 2 | Not portable (runs per playback inside Dispatcharr) |

## EPG Janitor (15)

| Action | What it does | crosstalk |
|---|---|---|
| `validate_settings` | Validate settings and DB connectivity | ✅ (offline settings check) |
| `get_summary` | Running job progress or last scan's summary | ✅ Status / Last Results |
| `preview_auto_match` | Dry-run EPG auto-match with programme-data validation | ✅ |
| `apply_auto_match` | Assign EPG by weighted scoring | ✅ |
| `scan_and_heal_dry_run` | Find broken EPG assignments, search replacements | ✅ |
| `scan_and_heal_apply` | Fix broken EPG assignments | ✅ |
| `scan_missing_epg` | Find channels with EPG assigned but no programme data | — |
| `export_results` | Export last scan to CSV | — (crosstalk writes CSVs per run instead) |
| `add_bad_epg_suffix` | Suffix channels with missing programme data (optionally drop their EPG) | — |
| `remove_epg_assignments` | Remove EPG from channels with missing programme data | — |
| `remove_epg_from_hidden` | Remove EPG from channels hidden in a profile | — |
| `remove_epg_by_regex` | Remove EPG from channels matching a regex within groups | — |
| `remove_all_epg_from_groups` | Remove all EPG from given groups | — |
| `clear_csv_exports` | Delete the plugin's CSV exports | — |
| `watchdog_run_check_now` | Refresh EPG sources that errored or are about to run dry | — |

## Stream-Mapparr (21)

| Action | What it does |
|---|---|
| `on_m3u_refresh` | Event: Match & Assign after each successful M3U refresh (if enabled) |
| `on_iptv_checker_scan` | Event (from IPTV Checker): run scheduled steps after a scan |
| `validate_settings` | Check DB, profiles, groups, channel databases |
| `update_schedule` | Save settings, restart the scheduler |
| `cleanup_periodic_tasks` | Remove orphaned Celery tasks from old versions |
| `preview_changes` | Dry-run CSV of the match |
| `add_streams_to_channels` | Match & assign streams to channels |
| `sort_streams` | Sort existing alternate streams by quality |
| `fill_mirror_backups` | Add each channel's matching stream from mirrored M3U accounts |
| `match_us_ota_only` | Match US OTA channels by callsign only |
| `probe_throughput` | Measure sustained throughput of assigned streams (feeds sorting) |
| `manage_channel_visibility` | Enable/disable channels by stream count |
| `view_check_progress` | Progress/ETA of the running operation |
| `view_last_results` | Summary of the last operation |
| `test_regex_rules` | Preview stream-name regex rules (read-only) |
| `check_stream_countries` | Report streams whose group country disagrees with their EPG suffix |
| `scan_placeholder_names` | Find uncovered placeholder name families, suggest a regex |
| `email_report_now` | Email HTML + CSV report via Newsflasharr |
| `clear_csv_exports` | Delete CSV exports |
| `report_a_bug` | Write a ready-to-paste bug report |
| `clear_operation_lock` | Clear a stuck operation lock |

## Channel-Mapparr (13)

| Action | What it does |
|---|---|
| `validate_settings` | Check DB, channel databases, settings |
| `load_and_process_channels` | Scan groups, compute standardized names |
| `rename_channels` | Apply standardized names (Dry Run → CSV) |
| `rename_unknown_channels` | Suffix unmatched OTA/premium channels |
| `apply_logos` | Default logo for channels without one |
| `apply_tv_logos` | Per-channel logos fuzzy-matched from tv-logo/tv-logos |
| `replace_tv_logos` | Same, also replacing existing logos |
| `organize_by_category` | Move channels into category groups |
| `import_m3u_streams` | Create channels from M3U streams by category (background) |
| `create_channels_from_streams` | One channel per broadcast station for unattached streams |
| `plugin_status` | Live progress/ETA |
| `email_report_now` | Email report via Newsflasharr |
| `clear_csv_exports` | Delete CSV exports |

## Lineuparr (11)

| Action | What it does |
|---|---|
| `validate_settings` | Check settings, show lineup summary and profiles |
| `plugin_status` | Progress or last result |
| `preview_stream_match` | Dry run: stream per lineup channel + confidence, CSV + HTML |
| `full_sync` | All steps: groups/channels, streams, guides, logos, enable in profile |
| `sync_channels` | Create/update groups and channels from the lineup (no deletes) |
| `apply_stream_match` | Attach matched streams in quality order (replaces unless Preserve is on; deletes empty channels) |
| `apply_epg_match` | Assign guides to channels without one |
| `apply_logo_match` | Logos from EPG icon, Logo Manager, or tv-logos |
| `resort_streams` | Re-order streams by newest quality data (e.g. after IPTV Checker) |
| `clear_csv_exports` | Delete `lineuparr_*.csv` |
| `email_report` | Send the newest report via Newsflasharr |

## IPTV Checker (23)

| Action | What it does |
|---|---|
| `validate_settings` | Validate settings, DB, groups |
| `update_schedule` | Apply schedule (empty clears it) |
| `reset_progress` | Clear windowed-schedule resume state |
| `check_scheduler_status` | Scheduler thread diagnostics |
| `load_groups` | Load channels from groups |
| `check_streams` | Start the stream check |
| `view_progress` | Progress/ETA |
| `cancel_check` | Cancel the running check |
| `view_results` | Last check summary |
| `view_table` | Detailed results table |
| `export_results` | Results CSV |
| `email_report` | HTML report via Newsflasharr |
| `rename_channels` | Rename dead channels |
| `move_dead_channels` | Move dead channels to a group |
| `rename_black_screen_channels` | Rename blank-screen channels |
| `move_black_screen_channels` | Move blank-screen channels to a group |
| `rename_low_framerate_channels` | Rename <30 fps channels |
| `move_low_framerate_channels` | Move <30 fps channels to a group |
| `add_video_format_suffix` | Add `[HD]`/`[FHD]` etc. to alive channels |
| `restore_channels` | Undo tags/moves for channels alive again |
| `delete_dead_channels` | Permanently delete dead channels |
| `cleanup_orphaned_tasks` | Remove orphaned Celery tasks |
| `clear_csv_exports` | Delete CSV exports |

## Event Channel Managarr (9)

| Action | What it does |
|---|---|
| `validate_configuration` | Save settings, check regexes/profiles/groups/schedule |
| `update_schedule` | Save settings, re-arm scheduler |
| `dry_run` | CSV of channels that would be hidden/shown |
| `run_now` | Scan and apply visibility now |
| `on_m3u_refresh` | Event: rescan after M3U refresh (if enabled) |
| `remove_epg_from_hidden` | Clear EPG from hidden channels |
| `clear_csv_exports` | Delete CSV exports |
| `cleanup_periodic_tasks` | Remove orphaned Celery tasks |
| `check_scheduler_status` | Scheduler status, next/last run |

## EPG & Sports Editor (16)

| Action | What it does |
|---|---|
| `setup` | Create virtual EPG sources, first transform, reassign channels |
| `apply_now` | Re-run transforms on all enabled sources |
| `preview` | Dry run: before/after of changed programmes |
| `sample` | Show raw title/sub-title/description samples |
| `test_rule` | Test one pattern/replacement |
| `scan` | List channels with no EPG, by group |
| `fill_epg` | Placeholder repeating EPG for Fill Groups |
| `sports_editor_rename_now` | Apply sports rename rules to auto-created channels |
| `sports_editor_epg_now` | Match channels to live sports schedule: rename, logo, Pregame/Live/Postgame EPG |
| `release_orphaned_hidden` | Unhide channels older versions lost track of |
| `status` | Sources, virtual EPG counts, rules |
| `teardown` | Delete virtual EPGs, reassign channels back |
| `clear_parse_counters` | Clear stuck EPG refresh parse counters |
| `restart_dispatcharr` | Reload Dispatcharr's backend |
| `on_epg_refresh` | Event: keep virtual copies current after EPG refresh |
| `on_m3u_refresh` | Event: move auto-synced channels after M3U refresh |

## Channel Name Cleaner (5)

| Action | What it does |
|---|---|
| `preview` | Dry run before → after, CSV |
| `test_name` | Run rules on one name |
| `apply` | Rename (collision strategy, backup first) |
| `undo_last` | Restore from latest backup (repeatable) |
| `status` | Last preview/apply/undo summary |

## Reservoarr (2)

| Action | What it does |
|---|---|
| `generate_profile` | Create/update the `reservoarr` stream profile |
| `reinstall` | Re-copy `reservoarr.py` into `/data/reservoarr/` |
