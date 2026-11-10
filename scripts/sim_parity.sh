#!/usr/bin/env bash
# Parity oracle: `crosstalk simulate-streams` versus the Python plugin's tools/match_sim.py.
#
# Builds a channels CSV and a provider M3U from the shipped channel databases, runs both
# simulators over the same inputs, and compares the report files byte for byte. Any
# difference is a port bug, so this is the gate before the guide sub-app is wired up.
#
# Several configurations are compared, not just the defaults: the default flags, a
# --no-ignore-quality run, a --custom-aliases run, and a --countries US run. The stream
# corpus also carries duplicate candidate names (the same name in two groups), which both
# tools de-duplicate by name before matching.
#
# Usage:
#   scripts/sim_parity.sh [--quick] [--keep]
#
#   --quick   smaller corpus, for a fast local check (default is the full corpus)
#   --keep    keep the work directory and print its path
#
# Requires python3 and the reference checkout at reference/Dispatcharr-EPG-Janitor-Plugin.
set -euo pipefail

# The sweep imports the plugin in-process, which would otherwise leave __pycache__
# directories behind in the reference checkout on every run.
export PYTHONDONTWRITEBYTECODE=1

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLUGIN="$REPO/reference/Dispatcharr-EPG-Janitor-Plugin"
QUICK=0
KEEP=0
for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=1 ;;
    --keep) KEEP=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

if [ ! -f "$PLUGIN/tools/match_sim.py" ]; then
  echo "reference plugin not found at $PLUGIN" >&2
  if [ ! -e "$PLUGIN" ] || [ -z "$(ls -A "$PLUGIN" 2>/dev/null)" ]; then
    echo 'reference/ is empty; run `git submodule update --init` to check it out' >&2
  fi
  exit 2
fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/crosstalk-sim-XXXXXX")"
cleanup() {
  if [ "$KEEP" -eq 1 ]; then
    echo "work directory: $WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

echo "==> building release binary"
cargo build --release -q -p crosstalk

echo "==> generating inputs in $WORK"
python3 - "$REPO" "$WORK" "$QUICK" <<'PY'
import csv, json, os, random, sys

repo, work, quick = sys.argv[1], sys.argv[2], int(sys.argv[3])
random.seed(7)

names, seen = [], set()

def add(name):
    if name and name not in seen:
        seen.add(name)
        names.append(name)

channels_dir = os.path.join(repo, "data", "channels")
for code in ("US", "UK", "CA"):
    with open(os.path.join(channels_dir, f"{code}_channels.json"), encoding="utf-8") as fh:
        for channel in json.load(fh)["channels"]:
            add(channel.get("channel_name"))
with open(os.path.join(channels_dir, "US_channels.json"), encoding="utf-8") as fh:
    for channel in json.load(fh)["channels"][:400]:
        add(channel.get("callsign"))

if quick:
    names = names[:1200]

with open(os.path.join(work, "channels.csv"), "w", newline="", encoding="utf-8") as fh:
    fh.write("# crosstalk guide - parity fixture\n")
    writer = csv.writer(fh)
    writer.writerow(["channel_id", "channel_name", "channel_number", "channel_group"])
    for index, name in enumerate(names):
        writer.writerow([index + 1, name, index + 1, "Test"])

streams = []

def variants(name):
    out = [name, f"{name} HD", f"{name} [FHD]", f"US: {name}", f"{name} (Backup)", name.replace(" ", "")]
    if name.startswith("BBC"):
        out.append(name.replace("BBC", "BBC US"))
    return out

source = names[:400] if quick else names[:1500]
for name in source:
    streams.extend(variants(name))
for name in random.sample(names, min(300, len(names))):
    cut = max(1, len(name) // 2)
    streams.extend([name[:cut] + name[cut + 1:], name + " +1", name.upper()])

streams = list(dict.fromkeys(streams))

# Duplicate candidate names: the same stream name in two groups. Both tools
# de-duplicate by query name before matching, so these rows exercise the dedupe
# path rather than the matcher, and must still compare identical.
dupes = streams[:150] if quick else streams[:400]
with open(os.path.join(work, "streams.m3u"), "w", encoding="utf-8") as fh:
    fh.write("#EXTM3U\n")
    for index, name in enumerate(streams):
        group = ["News", "Sports", "Movies"][index % 3]
        fh.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" group-title="{group}",{name}\n')
        fh.write(f"http://example.invalid/{index}\n")
    for index, name in enumerate(dupes):
        fh.write(f'#EXTINF:-1 tvg-id="dupe-{index}" tvg-name="{name}" group-title="Dupes",{name}\n')
        fh.write(f"http://example.invalid/dupe-{index}\n")

# A small custom-aliases file: one replaced entry, one disabled entry, one new key.
# Both tools merge these over the built-in table, replacing per key.
with open(os.path.join(work, "custom_aliases.json"), "w", encoding="utf-8") as fh:
    json.dump({
        "ESPN": ["ESPN Deportes Custom"],
        "CNN": [],
        "ZZZ New Channel": ["ZZZNC"],
    }, fh)

print(f"    {len(names)} channels, {len(streams)} streams (+{len(dupes)} duplicate-name rows)")
PY

# Compare one configuration: extra args are passed to both tools verbatim, so the
# flag spellings must agree (they do: --threshold/--no-ignore-*/--custom-aliases/
# --countries exist on both sides with the same meaning).
compare_case() {
  local label="$1"; shift
  echo "==> case: $label ${*:-<defaults>}"
  python3 "$PLUGIN/tools/match_sim.py" \
    --channels "$WORK/channels.csv" \
    --streams "$WORK/streams.m3u" \
    --threshold 80 \
    --out "$WORK/py-$label" "$@" >/dev/null

  "$REPO/target/release/crosstalk" simulate-streams \
    --channels "$WORK/channels.csv" \
    --streams "$WORK/streams.m3u" \
    --threshold 80 \
    --out "$WORK/rs-$label" "$@" >/dev/null

  local py_dir rs_dir
  py_dir="$(ls -d "$WORK"/py-"$label"/*/ | head -1)"
  rs_dir="$(ls -d "$WORK"/rs-"$label"/*/ | head -1)"

  for file in per_stream_matches.csv unmatched.csv low_confidence.csv top_n.csv summary.txt; do
    printf '    [%s] %-26s ' "$label" "$file"
    if cmp -s "$py_dir/$file" "$rs_dir/$file"; then
      echo "identical"
    else
      echo "DIFFERS"
      diff "$py_dir/$file" "$rs_dir/$file" | head -20 || true
      status=1
    fi
  done
}

status=0
compare_case default
compare_case no-ignore-quality --no-ignore-quality
compare_case custom-aliases --custom-aliases "$WORK/custom_aliases.json"
compare_case countries-US --countries US

if [ "$status" -eq 0 ]; then
  echo "==> parity OK"
else
  echo "==> parity FAILED" >&2
fi
exit "$status"
