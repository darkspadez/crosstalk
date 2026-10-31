#!/usr/bin/env bash
# Parity oracle: `crosstalk simulate` versus the Python plugin's tools/match_sim.py.
#
# Builds a channels CSV and a provider M3U from the shipped channel databases, runs both
# simulators over the same inputs, and compares the report files byte for byte. Any
# difference is a port bug, so this is the gate before the janitor sub-app is wired up.
#
# Usage:
#   scripts/sim_parity.sh [--quick] [--keep]
#
#   --quick   smaller corpus, for a fast local check (default is the full corpus)
#   --keep    keep the work directory and print its path
#
# Requires python3 and the reference checkout at reference/Dispatcharr-EPG-Janitor-Plugin.
set -euo pipefail

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
    fh.write("# EPG Janitor - parity fixture\n")
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
with open(os.path.join(work, "streams.m3u"), "w", encoding="utf-8") as fh:
    fh.write("#EXTM3U\n")
    for index, name in enumerate(streams):
        group = ["News", "Sports", "Movies"][index % 3]
        fh.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" group-title="{group}",{name}\n')
        fh.write(f"http://example.invalid/{index}\n")

print(f"    {len(names)} channels, {len(streams)} streams")
PY

echo "==> python match_sim"
python3 "$PLUGIN/tools/match_sim.py" \
  --channels "$WORK/channels.csv" \
  --streams "$WORK/streams.m3u" \
  --threshold 80 \
  --out "$WORK/py-out" >/dev/null

echo "==> crosstalk simulate"
"$REPO/target/release/crosstalk" simulate \
  --channels "$WORK/channels.csv" \
  --streams "$WORK/streams.m3u" \
  --threshold 80 \
  --out "$WORK/rs-out" >/dev/null

PY_DIR="$(ls -d "$WORK"/py-out/*/ | head -1)"
RS_DIR="$(ls -d "$WORK"/rs-out/*/ | head -1)"

status=0
for file in per_stream_matches.csv unmatched.csv low_confidence.csv top_n.csv summary.txt; do
  printf '    %-26s ' "$file"
  if cmp -s "$PY_DIR/$file" "$RS_DIR/$file"; then
    echo "identical"
  else
    echo "DIFFERS"
    diff "$PY_DIR/$file" "$RS_DIR/$file" | head -20 || true
    status=1
  fi
done

if [ "$status" -eq 0 ]; then
  echo "==> parity OK"
else
  echo "==> parity FAILED" >&2
fi
exit "$status"
