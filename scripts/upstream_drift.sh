#!/usr/bin/env bash
#
# upstream_drift.sh -- what has the reference plugin changed since the pinned commit?
#
# crosstalk ports EPG Janitor file by file (see THIRD_PARTY_NOTICES.md), and upstream commits
# most days. The plugin is a git submodule pinned to one commit, so the question worth asking
# is: which commits since that pin touch a file crosstalk actually ported? Run it before each
# phase of plans/plugin-consolidation.md, and on the weekly CI schedule.
#
# Informational only, by design: it always exits 0, it needs no credentials for a public
# repository, and a missing checkout or a failed fetch is reported rather than becoming a
# build failure. It does need network access for the fetch (skip it with --no-fetch).
#
# Usage: scripts/upstream_drift.sh [--no-fetch]

set -uo pipefail

readonly reference_path="reference/Dispatcharr-EPG-Janitor-Plugin"

# Every file crosstalk ports, relative to the submodule root. Keep this in step with
# THIRD_PARTY_NOTICES.md: a file named there is a file whose upstream change may matter.
readonly -a ported_paths=(
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

usage() {
  cat <<'EOF'
Usage: scripts/upstream_drift.sh [--no-fetch]

Lists the commits upstream EPG Janitor has made since the pinned submodule commit that touch
a file crosstalk ported. Informational: always exits 0.

  --no-fetch   use the remote state already in the submodule, do not git fetch
  -h, --help   print this help
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

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
reference_dir=${CROSSTALK_REFERENCE_DIR:-"$repo_root/$reference_path"}

if [ ! -e "$reference_dir/.git" ]; then
  printf 'upstream-drift: no reference checkout at %s\n' "$reference_dir" >&2
  # shellcheck disable=SC2016 # backticks are literal here, not command substitution.
  printf 'upstream-drift: run `git submodule update --init %s` first\n' "$reference_path" >&2
  exit 0
fi

if [ "$fetch" = 1 ] && ! git -C "$reference_dir" fetch --quiet origin; then
  # shellcheck disable=SC2016 # backticks are literal here, not command substitution.
  printf 'upstream-drift: `git fetch` failed; reporting the remote state already present\n' >&2
fi

pin=$(git -C "$reference_dir" rev-parse --verify --quiet HEAD) || {
  printf 'upstream-drift: %s has no HEAD\n' "$reference_dir" >&2
  exit 0
}

# origin/HEAD is what `git submodule update --init` leaves behind; the branches are the
# fallback for a checkout that predates it or was fetched by hand.
upstream_ref=""
for candidate in origin/HEAD origin/main origin/master FETCH_HEAD; do
  if git -C "$reference_dir" rev-parse --verify --quiet "$candidate" >/dev/null; then
    upstream_ref=$candidate
    break
  fi
done

if [ -z "$upstream_ref" ]; then
  # shellcheck disable=SC2016 # backticks are literal here, not command substitution.
  printf 'upstream-drift: %s has no remote-tracking branch; is `origin` fetched?\n' \
    "$reference_dir" >&2
  exit 0
fi

upstream=$(git -C "$reference_dir" rev-parse --verify --quiet "$upstream_ref")

describe() {
  git -C "$reference_dir" log -1 --format='%h %s' "$1" 2>/dev/null || printf '%s' "$1"
}

printf 'reference:  %s (%s)\n' "$reference_path" "$(git -C "$reference_dir" remote get-url origin)"
printf 'pin:        %s\n' "$(describe "$pin")"
printf 'upstream:   %s %s\n' "$upstream_ref" "$(describe "$upstream")"
printf '\n'

if [ "$pin" = "$upstream" ]; then
  printf 'No drift: the pin is the tip of %s.\n' "$upstream_ref"
  exit 0
fi

commits=$(git -C "$reference_dir" rev-list --count "$pin..$upstream" 2>/dev/null) || commits="?"
printf '%s commit(s) upstream since the pin.\n\n' "$commits"

drift=$(git --no-pager -C "$reference_dir" log --oneline "$pin..$upstream" -- "${ported_paths[@]}")
if [ -z "$drift" ]; then
  printf 'None of them touch a ported file.\n'
else
  printf 'Touching a ported file:\n'
  printf '%s\n' "$drift"
  printf '\nReview each against "Differences from the plugins" in README.md and against\n'
  printf 'THIRD_PARTY_NOTICES.md, then move the pin in one commit with the parity gates green.\n'
fi

exit 0
