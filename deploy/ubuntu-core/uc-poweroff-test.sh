#!/usr/bin/env bash
# Power-off test (R12.3): collect fast, hard-reset the VM through QMP
# (no clean shutdown, like pulling the plug) N times at random moments, then
# let the agent drain and run the checker for the device.
#
#   UC_USER=you RESETS=10 deploy/ubuntu-core/uc-poweroff-test.sh
set -euo pipefail
cd "$(dirname "$0")"
WORK=${UC_WORK:-work}
N=${RESETS:-10}
SSH=(ssh -p 8022 -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 "${UC_USER:?set UC_USER}@localhost")

qmp() {
  printf '{"execute":"qmp_capabilities"}\n{"execute":"%s"}\n' "$1" | socat - UNIX-CONNECT:"$WORK/qmp.sock" >/dev/null
}
wait_ssh() {
  for _ in $(seq 1 180); do
    "${SSH[@]}" true 2>/dev/null && return 0
    sleep 5
  done
  echo "guest did not come back" >&2
  return 1
}

"${SSH[@]}" "sudo snap set fleetwire-agent collect.interval=200ms wal.sync=interval wal.sync-interval=1s"
for i in $(seq 1 "$N"); do
  sleep $((5 + RANDOM % 26))
  echo "reset $i/$N"
  qmp system_reset
  wait_ssh
done
"${SSH[@]}" "sudo snap set fleetwire-agent collect.interval=1h"
echo "draining"
for _ in $(seq 1 120); do
  st=$("${SSH[@]}" "sudo fleetwire-agent status -json")
  last=$(jq -r .last_seq <<<"$st")
  acked=$(jq -r .acked <<<"$st")
  [ "$last" = "$acked" ] && break
  sleep 5
done
id=$(jq -r .device_id <<<"$st")
gaps=$(jq -r '[.gaps[]? | "\(.from)-\(.to)"] | join(",")' <<<"$st")
echo "device $id last_seq $last acked $acked gaps [$gaps]"
cd ../..
bin/fleetwire-checker -device "$id" -last-seq "$last" -gaps "$gaps" -json-out "docs/results/uc-poweroff-$(date -u +%Y%m%dT%H%M%SZ).json"
