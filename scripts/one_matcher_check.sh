#!/usr/bin/env bash
# One matcher in the binary (phase 3 goal reset, steps 6/9): the production `crosstalk`
# binary must link no retired plugin-profile matcher — no `profile::MatchProfile`, no
# `pipeline::CandidateIndex`, no `pipeline::Matcher`. Those live on in `crosstalk-matching`
# as ORACLE tooling (parity tests, `--features oracle`), and the linker drops whatever
# production code never reaches, so the symbol table is the proof: a production path
# that still calls the old matcher makes this fail.
#
# Usage: scripts/one_matcher_check.sh
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"
cargo build -q --locked -p crosstalk
PATTERN='crosstalk_matching::(profile::|pipeline::CandidateIndex|pipeline::Matcher)'
count="$(nm -C target/debug/crosstalk | grep -cE "$PATTERN" || true)"
if [ "$count" -ne 0 ]; then
  echo "one-matcher check FAILED: $count retired-matcher symbol(s) in the production binary:" >&2
  nm -C target/debug/crosstalk | grep -E "$PATTERN" | head -20 >&2
  exit 1
fi
engine="$(nm -C target/debug/crosstalk | grep -c 'crosstalk_matching::engine::UnifiedEngine' || true)"
echo "one-matcher check OK: 0 retired-matcher symbols, $engine unified-engine symbol(s)"
