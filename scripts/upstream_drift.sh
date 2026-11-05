#!/usr/bin/env bash
#
# upstream_drift.sh -- what have the reference plugins changed since their pinned commits?
#
# crosstalk ports the Dispatcharr plugins file by file (see THIRD_PARTY_NOTICES.md), and
# upstream commits most days. Each plugin is a git submodule pinned to one commit, so the
# question worth asking is: which commits since that pin touch a file crosstalk actually
# ported? Run it before each phase of plans/plugin-consolidation.md, and on the weekly CI
# schedule.
#
# Informational only, by design: it always exits 0, it needs no credentials for a public
# repository, and a missing checkout or a failed fetch is reported rather than becoming a
# build failure. It does need network access for the fetch (skip it with --no-fetch).
#
# Usage: scripts/upstream_drift.sh [--no-fetch]

set -uo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
reference_root=${CROSSTALK_REFERENCE_DIR:-"$repo_root/reference"}

# Every reference checkout crosstalk ports from, in the order the phases landed them. Each
# name is a submodule under the reference directory and a case in ported_paths_for below.
readonly -a references=(
  "Dispatcharr-EPG-Janitor-Plugin"
  "Stream-Mapparr"
  "Dispatcharr-IPTV-Checker-Plugin"
  "Dispatcharr-Channel-Maparr-Plugin"
  "Dispatcharr-Lineuparr-Plugin"
  "dispatcharr-channel-name-cleaner"
)

# ported_paths_for <reference> fills `ported` with every file crosstalk ports from that
# reference, relative to the submodule root. Keep this in step with THIRD_PARTY_NOTICES.md:
# a file named there is a file whose upstream change may matter. Pathspecs, so a glob such
# as Stream-Mapparr/*_channels.json is matched by git itself.
ported_paths_for() {
  case $1 in
    Dispatcharr-EPG-Janitor-Plugin)
      ported=(
        "EPG-Janitor/fuzzy_matcher.py"
        "EPG-Janitor/matching_core.py"
        "EPG-Janitor/aliases.py"
        "EPG-Janitor/wildcard_match.py"
        "EPG-Janitor/epg_watchdog.py"
        "EPG-Janitor/plugin.py"
        # The settings schema follows the plugin's field ids, labels and help text.
        "EPG-Janitor/plugin.json"
        "EPG-Janitor/AU_channels.json"
        "EPG-Janitor/BR_channels.json"
        "EPG-Janitor/CA_channels.json"
        "EPG-Janitor/DE_channels.json"
        "EPG-Janitor/ES_channels.json"
        "EPG-Janitor/FR_channels.json"
        "EPG-Janitor/IN_channels.json"
        "EPG-Janitor/MX_channels.json"
        "EPG-Janitor/NL_channels.json"
        "EPG-Janitor/NO_channels.json"
        "EPG-Janitor/UK_channels.json"
        "EPG-Janitor/US_channels.json"
        "EPG-Janitor/us_station_callsigns.json"
      )
      ;;
    Stream-Mapparr)
      ported=(
        "Stream-Mapparr/fuzzy_matcher.py"
        "Stream-Mapparr/matching_core.py"
        "Stream-Mapparr/aliases.py"
        "Stream-Mapparr/country.py"
        "Stream-Mapparr/mirror_backups.py"
        "Stream-Mapparr/placeholder_scan.py"
        "Stream-Mapparr/plugin.py"
        # The settings schema follows the plugin's field ids, labels and help text.
        "Stream-Mapparr/plugin.json"
        # The channel databases and the network-name table: country and network detection
        # read them, so a change there changes what crosstalk detects.
        "Stream-Mapparr/*_channels.json"
        "Stream-Mapparr/networks.json"
      )
      ;;
    Dispatcharr-IPTV-Checker-Plugin)
      ported=(
        "iptv_checker/plugin.py"
        # The settings schema follows the plugin's field ids, labels and help text.
        "iptv_checker/plugin.json"
      )
      ;;
    Dispatcharr-Channel-Maparr-Plugin)
      ported=(
        "Channel-Maparr/plugin.py"
        "Channel-Maparr/fuzzy_matcher.py"
        "Channel-Maparr/matching_core.py"
        "Channel-Maparr/aliases.py"
        "Channel-Maparr/logo_matcher.py"
        "Channel-Maparr/channel_seeder.py"
        "Channel-Maparr/market_index.py"
        "Channel-Maparr/station_affiliation.py"
        "Channel-Maparr/group_scope.py"
        "Channel-Maparr/wildcard_match.py"
        # The settings schema follows the plugin's field ids, labels and help text.
        "Channel-Maparr/plugin.json"
        "Channel-Maparr/*_channels.json"
        "Channel-Maparr/networks*.json"
      )
      ;;
    Dispatcharr-Lineuparr-Plugin)
      ported=(
        "Lineuparr/plugin.py"
        "Lineuparr/fuzzy_matcher.py"
        "Lineuparr/matching_core.py"
        "Lineuparr/aliases.py"
        "Lineuparr/logo_matcher.py"
        # The settings schema follows the plugin's field ids, labels and help text.
        "Lineuparr/plugin.json"
        "Lineuparr/*_lineup.json"
      )
      ;;
    dispatcharr-channel-name-cleaner)
      ported=(
        "channel_name_cleaner/plugin.py"
        # The settings schema follows the plugin's field ids, labels and help text.
        "channel_name_cleaner/plugin.json"
      )
      ;;
    *)
      ported=()
      ;;
  esac
}

usage() {
  cat <<'EOF'
Usage: scripts/upstream_drift.sh [--no-fetch]

For each reference plugin: lists the commits upstream has made since the pinned submodule
commit that touch a file crosstalk ported. Informational: always exits 0.

  --no-fetch   use the remote state already in the submodules, do not git fetch
  -h, --help   print this help

CROSSTALK_REFERENCE_DIR overrides the directory holding the reference checkouts
(default: reference/ next to this script).
EOF
}

fetch=1
for argument in "$@"; do
  case $argument in
    --no-fetch) fetch=0 ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      printf 'upstream-drift: unknown argument %s (try --help)\n' "$argument" >&2
      exit 0
      ;;
  esac
done

describe() {
  git -C "$1" log -1 --format='%h %s' "$2" 2>/dev/null || printf '%s' "$2"
}

# report_reference <name>: pin, upstream tip and drifting commits for one submodule. Prints
# a diagnostic and returns 0 (not an error) when the checkout or the remote is unavailable.
report_reference() {
  local name=$1
  local reference_path="reference/$name"
  local reference_dir="$reference_root/$name"

  if [ ! -e "$reference_dir/.git" ]; then
    printf 'upstream-drift: no reference checkout at %s\n' "$reference_dir" >&2
    # shellcheck disable=SC2016 # backticks are literal here, not command substitution.
    printf 'upstream-drift: run `git submodule update --init %s` first\n' "$reference_path" >&2
    return 0
  fi

  if [ "$fetch" = 1 ] && ! git -C "$reference_dir" fetch --quiet origin; then
    # shellcheck disable=SC2016 # backticks are literal here, not command substitution.
    printf 'upstream-drift: `git fetch` failed for %s; reporting the remote state already\n' \
      "$reference_path" >&2
    printf 'upstream-drift: present instead\n' >&2
  fi

  local pin
  pin=$(git -C "$reference_dir" rev-parse --verify --quiet HEAD) || {
    printf 'upstream-drift: %s has no HEAD\n' "$reference_dir" >&2
    return 0
  }

  # origin/HEAD is what `git submodule update --init` leaves behind; the branches are the
  # fallback for a checkout that predates it or was fetched by hand.
  local upstream_ref=""
  local candidate
  for candidate in origin/HEAD origin/main origin/master FETCH_HEAD; do
    if git -C "$reference_dir" rev-parse --verify --quiet "$candidate" >/dev/null; then
      upstream_ref=$candidate
      break
    fi
  done

  if [ -z "$upstream_ref" ]; then
    # shellcheck disable=SC2016 # backticks are literal here, not command substitution.
    printf 'upstream-drift: %s has no remote-tracking branch; is `origin` fetched?\n' \
      "$reference_path" >&2
    return 0
  fi

  local upstream
  upstream=$(git -C "$reference_dir" rev-parse --verify --quiet "$upstream_ref")
  local -a ported
  ported_paths_for "$name"

  printf 'reference:  %s (%s)\n' "$reference_path" \
    "$(git -C "$reference_dir" remote get-url origin)"
  printf 'pin:        %s\n' "$(describe "$reference_dir" "$pin")"
  printf 'upstream:   %s %s\n' "$upstream_ref" "$(describe "$reference_dir" "$upstream")"
  printf '\n'

  if [ "$pin" = "$upstream" ]; then
    printf 'No drift: the pin is the tip of %s.\n' "$upstream_ref"
    return 0
  fi

  local commits
  commits=$(git -C "$reference_dir" rev-list --count "$pin..$upstream" 2>/dev/null) || commits="?"
  printf '%s commit(s) upstream since the pin.\n\n' "$commits"

  local drift
  drift=$(git --no-pager -C "$reference_dir" log --oneline "$pin..$upstream" -- "${ported[@]}")
  if [ -z "$drift" ]; then
    printf 'None of them touch a ported file.\n'
  else
    printf 'Touching a ported file:\n'
    printf '%s\n' "$drift"
    printf '\nReview each against "Differences from the plugins" in README.md and against\n'
    printf 'THIRD_PARTY_NOTICES.md, then move the pin in one commit with the parity gates green.\n'
  fi
}

first=1
for name in "${references[@]}"; do
  if [ "$first" = 1 ]; then
    first=0
  else
    printf '\n'
  fi
  printf '== %s ==\n' "$name"
  report_reference "$name"
done

exit 0
