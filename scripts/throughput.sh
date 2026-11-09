#!/bin/bash
# Throughput search: raise the offered load in steps and record whether
# ingest keeps up.
#
#   scripts/throughput.sh [DEVICESxREADINGS ...]
#
# Each step runs DEVICES load-generator devices that collect READINGS
# readings every second, so a step offers DEVICES batches/s and
# DEVICES*READINGS readings/s. After STEP seconds (default 180) it reads from
# Prometheus the rates over the last two minutes, the consumer lag and its
# slope, and the p99 delay from collection to commit. A step keeps up when
# ingest stores at least 95% of the offered readings and the lag is not
# growing.
#
# The table goes to stdout and to docs/results/<time>-<commit>-throughput.md
# (OUT overrides the path).
#
# Needs the Compose stack (make up) and bin/ (make build). Devices are
# enrolled once and resumed from their state directory in later steps.
set -euo pipefail
cd "$(dirname "$0")/.."

steps=("$@")
[ ${#steps[@]} -gt 0 ] || steps=(250x1 500x1 1000x1 1500x1 2000x1 500x4 500x16 500x64)
step_s=${STEP:-180}
state=${STATE_DIR:-.cache/throughput}
# -dirty when tracked files differ from the commit, as in the chaos tables.
commit=$(git rev-parse --short=12 HEAD)$([ -z "$(git status --porcelain --untracked-files=no)" ] || echo -dirty)
out=${OUT:-docs/results/$(date -u +%Y-%m-%dT%H%M%SZ)-${commit}-throughput.md}
prom=http://127.0.0.1:9090
ca=deploy/compose/secrets/ca-bundle.pem
api=https://localhost:8443

q() { # instant query, one number ("nan" when there is no data)
  curl -fsS --get "$prom/api/v1/query" --data-urlencode "query=$1" |
    jq -r '.data.result[0].value[1] // "nan"'
}

if [ ! -s "$state/tenant-token" ]; then
  mkdir -p "$state"
  op=$(make -s operator-token NAME="throughput-$(date +%s)" 2>/dev/null)
  tenant=$(bin/fwctl -url $api -ca-file $ca -token "$op" -o json tenant create -name "throughput-$(date +%H%M%S)" | jq -r .id)
  bin/fwctl -url $api -ca-file $ca -token "$op" -o json token create -tenant "$tenant" -name throughput \
    -scopes devices:read,devices:admin,readings:read | jq -r .token > "$state/tenant-token"
fi

search() {
  echo "# Throughput search, $(date -u '+%Y-%m-%d %H:%M UTC')"
  echo
  echo "Machine: $(nproc) vCPU $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs), $(free -g | awk '/Mem:/ {print $2}') GiB RAM; load generator and backend on the same machine."
  echo "Commit: ${commit}, written by \`scripts/throughput.sh\`, ${step_s} s per step, rates over the last 2 minutes of each step."
  echo
  echo "| Devices | Offered batches/s | Offered readings/s | Stored readings/s | Lag (records) | Lag slope (records/s) | p99 delay | Kept up |"
  echo "|---:|---:|---:|---:|---:|---:|---:|---|"
  for step in "${steps[@]}"; do
    devices=${step%x*} readings=${step#*x}
    FLEETWIRE_TENANT_TOKEN=$(cat "$state/tenant-token") bin/fleetwire-loadgen run \
      -devices "$devices" -state-dir "$state/devices" -interval 1s -readings "$readings" \
      -duration "${step_s}s" -drain-timeout 1s -metrics-listen 172.31.250.1:9466 \
      > "$state/loadgen-$step.log" 2>&1 || true
    offered=$(q 'sum(rate(fleetwire_agent_readings_collected_total[2m]))')
    stored=$(q 'sum(rate(fleetwire_ingest_readings_stored_total[2m]))')
    lag=$(q 'sum(fleetwire_ingest_consumer_lag)')
    slope=$(q 'deriv(sum(fleetwire_ingest_consumer_lag)[2m:10s])')
    p99=$(q 'histogram_quantile(0.99, sum(rate(fleetwire_ingest_e2e_delay_seconds_bucket[2m])) by (le))')
    kept=$(awk -v s="$slope" -v st="$stored" -v o="$offered" 'BEGIN { print (st >= 0.95 * o && s <= 1) ? "yes" : "no" }')
    printf '| %d | %d | %.0f | %.0f | %.0f | %.1f | %.2f s | %s |\n' \
      "$devices" "$devices" "$offered" "$stored" "$lag" "$slope" "$p99" "$kept"
    # Let ingest drain before the next step, so steps do not overlap.
    for _ in $(seq 1 60); do
      [ "$(q 'sum(fleetwire_ingest_consumer_lag)')" = "0" ] && break
      sleep 5
    done
  done
}

mkdir -p "$(dirname "$out")"
search | tee "$out"
echo "written to $out" >&2
