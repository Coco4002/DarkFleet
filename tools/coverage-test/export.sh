#!/usr/bin/env bash
# Dumps everything recorded in ais.raw so far to data/ais-raw-<UTC time>.jsonl.gz.
# Safe to run during the test; it only reads. Reads each partition from its first
# retained offset up to the end offset at the moment the export starts.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # stop Git Bash on Windows from rewriting /opt/... paths
cd "$(dirname "$0")"
mkdir -p data
out="data/ais-raw-$(date -u +%Y%m%dT%H%M%SZ).jsonl"

fail() { echo "export failed: $*" >&2; rm -f "$out" "$out.gz"; exit 1; }
# Remove partial output if interrupted or if any command fails unexpectedly.
trap 'rc=$?; [ "$rc" -eq 0 ] || rm -f "$out" "$out.gz"' EXIT
trap 'exit 130' INT TERM

kafka() { docker compose exec -T kafka "$@" </dev/null; }

# Prints "partition offset" lines; fails loudly if the lookup fails or returns nothing.
offsets() {
  local raw
  raw=$(kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic ais.raw --time "$1") \
    || fail "could not read offsets (--time $1)"
  raw=$(printf '%s\n' "$raw" | tr -d '\r' | grep -E '^ais\.raw:[0-9]+:[0-9]+$' | awk -F: '{print $2, $3}') || true
  [ -n "$raw" ] || fail "no offsets returned for ais.raw (--time $1)"
  printf '%s\n' "$raw"
}

starts=$(offsets -2)
ends=$(offsets -1)
[ "$(wc -l <<<"$starts")" -eq "$(wc -l <<<"$ends")" ] || fail "start and end offsets list different partitions"

declare -A start
while read -r p o; do start[$p]=$o; done <<<"$starts"

echo "Exporting ais.raw to $out.gz ..."
: > "$out"
expected=0
while read -r p end; do
  [ -n "${start[$p]+x}" ] || fail "partition $p has an end offset but no start offset"
  count=$(( end - start[$p] ))
  echo "  partition $p: $count messages"
  expected=$(( expected + count ))
  [ "$count" -gt 0 ] || continue
  kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic ais.raw \
    --partition "$p" --offset "${start[$p]}" --max-messages "$count" >> "$out" \
    || fail "consumer failed on partition $p"
done <<<"$ends"

got=$(wc -l < "$out")
[ "$got" -eq "$expected" ] || fail "expected $expected messages but wrote $got"
gzip "$out"
echo "Done: $got messages, $(du -h "$out.gz" | cut -f1)"
