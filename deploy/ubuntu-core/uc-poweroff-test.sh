#!/usr/bin/env bash
# Power-off test: collect fast, hard-reset the VM through QMP
# (no clean shutdown, like pulling the plug) N times at random moments, then
# let the agent drain and run the checker for the device.
#
#   UC_USER=you RESETS=10 deploy/ubuntu-core/uc-poweroff-test.sh
#
# Against the Ubuntu Server stand-in (server-vm.sh):
#   UC_USER=fleetwire UC_SSH_KEY=$PWD/deploy/ubuntu-core/work/id_ed25519 deploy/ubuntu-core/uc-poweroff-test.sh
#
# The table goes to docs/results/<time>-<commit>-poweroff.md, the checker's
# report next to it as .json.
set -euo pipefail
cd "$(dirname "$0")"
WORK=${UC_WORK:-work}
N=${RESETS:-10}
SSH=(ssh -p 8022 -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
if [ -n "${UC_SSH_KEY:-}" ]; then
  SSH+=(-i "$UC_SSH_KEY")
fi
SSH+=("${UC_USER:?set UC_USER}@localhost")

qmp() {
  printf '{"execute":"qmp_capabilities"}\n{"execute":"%s"}\n' "$1" | socat - UNIX-CONNECT:"$WORK/qmp.sock" >/dev/null
}
# arm64 kernels write no "model name" to /proc/cpuinfo; lscpu names Arm
# cores, and the architecture is the last resort.
cpu_model() {
  local m
  m=$(awk -F': *' '/^model name/ { print $2; exit }' /proc/cpuinfo 2>/dev/null || true)
  [ -n "$m" ] || m=$(LC_ALL=C lscpu 2>/dev/null | awk -F': *' '/^Model name/ { print $2; exit }' || true)
  printf '%s\n' "${m:-$(uname -m)}"
}
wait_ssh() {
  for _ in $(seq 1 180); do
    "${SSH[@]}" true 2>/dev/null && return 0
    sleep 5
  done
  echo "guest did not come back" >&2
  return 1
}

stamp=$(date -u +%Y-%m-%dT%H%M%SZ)
commit=$(git rev-parse --short=12 HEAD)$([ -z "$(git status --porcelain --untracked-files=no)" ] || echo -dirty)
# shellcheck disable=SC2016 # expanded by the guest's shell, not here
guest=$("${SSH[@]}" '. /etc/os-release && echo "$PRETTY_NAME, kernel $(uname -r)"')
snap=$("${SSH[@]}" "snap list fleetwire-agent | awk 'NR == 2 {print \$2 \" rev \" \$3}'")

"${SSH[@]}" "sudo snap set fleetwire-agent collect.interval=200ms wal.sync=interval wal.sync-interval=1s"
started=$(date +%s)
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
minutes=$(( ($(date +%s) - started) / 60 ))
id=$(jq -r .device_id <<<"$st")
gaps=$(jq -r '[.gaps[]? | "\(.from)-\(.to)"] | join(",")' <<<"$st")
echo "device $id last_seq $last acked $acked gaps [$gaps]"
cd ../..
out=docs/results/$stamp-$commit-poweroff
set +e
summary=$(bin/fleetwire-checker -device "$id" -last-seq "$last" -gaps "$gaps" -json-out "$out.json")
rc=$?
set -e
echo "$summary"
case $rc in
0) result=PASS ;;
1) result=FAIL ;;
*) echo "checker could not run (exit $rc)" >&2; exit "$rc" ;;
esac
chmod 0644 "$out.json"
cat > "$out.md" <<EOF
# Power-off test $stamp

- Commit: \`$commit\`
- Host: $(nproc) vCPU $(cpu_model), $(free -g | awk '/Mem:/ {print $2}') GiB RAM, QEMU accel $([ -w /dev/kvm ] && echo kvm || echo tcg)
- Guest: $guest; snap fleetwire-agent $snap
- Command: \`RESETS=$N deploy/ubuntu-core/uc-poweroff-test.sh\` (written by the script)
- Load: a collection every 200 ms, 11 readings each (cpu 1, memory 3, load 3, uptime 1, disk 3) plus one per thermal zone, \`wal.sync=interval\` (1 s); QMP \`system_reset\` $N times, 5 to 30 s after the guest was reachable again; $minutes minutes in all

| Device | Resets | Last seq | Acked | Reported gaps | Checker | Result |
|---|---:|---:|---:|---|---|---|
| \`$id\` | $N | $last | $acked | ${gaps:-none} | $(grep -m1 '^devices ' <<<"$summary") | $result |
EOF
echo "written to $out.md"
[ "$result" = PASS ]
