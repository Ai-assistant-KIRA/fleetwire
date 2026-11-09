#!/usr/bin/env bash
# Run every Go fuzz target in the module for a short time.
# `go test -fuzz` accepts one target per run, so this finds them all and runs
# them one by one. Usage: scripts/fuzz-smoke.sh [duration]   (default 30s)
set -euo pipefail

fuzztime="${1:-30s}"
cd "$(dirname "$0")/.."

mapfile -t targets < <(grep -rhoE --include='*_test.go' '^func (Fuzz[A-Za-z0-9_]+)' . \
  --with-filename | sed -E 's#^\./##; s#:func # #' | sort -u)

if [ "${#targets[@]}" -eq 0 ]; then
  echo "no fuzz targets found"
  exit 0
fi

for entry in "${targets[@]}"; do
  file="${entry%% *}"
  name="${entry##* }"
  pkg="./$(dirname "$file")"
  echo "=== fuzz $pkg $name ($fuzztime)"
  go test -run='^$' -fuzz="^${name}\$" -fuzztime="$fuzztime" "$pkg"
done
