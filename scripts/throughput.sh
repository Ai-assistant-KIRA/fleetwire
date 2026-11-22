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
# enrolled once and resumed from their state directory (STATE_DIR, default
# .cache/throughput; put it on tmpfs to keep the devices' fsyncs off the
# database's disk) in later steps.
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

mkdir -p "$state"
if [ ! -s "$state/tenant-token" ]; then
  # Tokens go to fwctl in its environment, not on the command line, where
  # every local user can read them in the process list.
  op=$(make -s operator-token NAME="throughput-$(date +%s)" 2>/dev/null)
  tenant=$(FWCTL_TOKEN=$op bin/fwctl -url $api -ca-file $ca -o json tenant create -name "throughput-$(date +%H%M%S)" | jq -r .id)
  (
    umask 077
    FWCTL_TOKEN=$op bin/fwctl -url $api -ca-file $ca -o json token create -tenant "$tenant" -name throughput \
      -scopes devices:read,devices:admin,readings:read | jq -r .token > "$state/tenant-token"
  )
fi
# Owner only, also for a token an older version of this script wrote 0644.
chmod 0600 "$state/tenant-token"
# As in the chaos tables: on one machine every device's fsyncs reach the
# same disk as PostgreSQL's, unless the state is on tmpfs.
if [ "$(stat -f -c %T "$state")" = tmpfs ]; then
  where="on tmpfs ($state): the devices' fsyncs do not reach a disk"
else
  where="on disk ($state), shared by every device"
fi

search() {
  echo "# Throughput search, $(date -u '+%Y-%m-%d %H:%M UTC')"
  echo
  echo "Machine: $(nproc) vCPU $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs), $(free -g | awk '/Mem:/ {print $2}') GiB RAM; load generator and backend on the same machine."
  echo "Commit: ${commit}, written by \`scripts/throughput.sh\`, ${step_s} s per step, rates over the last 2 minutes of each step."
  echo "Device state: ${where}."
  echo
  echo "| Devices | Offered batches/s | Offered readings/s | Stored readings/s | Lag (records) | Lag slope (records/s) | p99 delay | Kept up |"
  echo "|---:|---:|---:|---:|---:|---:|---:|---|"
  for step in "${steps[@]}"; do
    devices=${step%x*} readings=${step#*x}
    # A non-zero exit is a crash, a bad flag, or (since the load generator
    # checks it) a device that did not start: the step did not offer the
    # load it names, whatever Prometheus shows.
    rc=0
    FLEETWIRE_TENANT_TOKEN=$(cat "$state/tenant-token") bin/fleetwire-loadgen run \
      -devices "$devices" -state-dir "$state/devices" -interval 1s -readings "$readings" \
      -duration "${step_s}s" -drain-timeout 1s -metrics-listen 172.31.250.1:9466 \
      > "$state/loadgen-$step.log" 2>&1 || rc=$?
    offered=$(q 'sum(rate(fleetwire_agent_readings_collected_total[2m]))')
    stored=$(q 'sum(rate(fleetwire_ingest_readings_stored_total[2m]))')
    lag=$(q 'sum(fleetwire_ingest_consumer_lag)')
    slope=$(q 'deriv(sum(fleetwire_ingest_consumer_lag)[2m:10s])')
    p99=$(q 'histogram_quantile(0.99, sum(rate(fleetwire_ingest_e2e_delay_seconds_bucket[2m])) by (le))')
    if [ "$rc" -ne 0 ]; then
      kept="no: load generator exited $rc, see $state/loadgen-$step.log"
    else
      # No offered load ("nan" or 0) is not keeping up: 0 >= 0.95 * 0.
      kept=$(awk -v s="$slope" -v st="$stored" -v o="$offered" \
        'BEGIN { print (o + 0 > 0 && st + 0 >= 0.95 * (o + 0) && s + 0 <= 1) ? "yes" : "no" }')
    fi
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
# The table keeps every step; the run fails if a load generator did.
if grep -q 'load generator exited' "$out"; then exit 1; fi
