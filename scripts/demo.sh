#!/usr/bin/env bash
# make demo: create a tenant, enroll real agents against the local stack,
# let them send for a while, and show where to look.
#   scripts/demo.sh         start (DEMO_DEVICES=5, DEMO_SECONDS=60)
#   scripts/demo.sh stop    stop the demo agents
# Needs: the stack (make up), bin/ (make build), jq.
set -euo pipefail
cd "$(dirname "$0")/.."

STATE=deploy/compose/.state/demo
SECRETS=deploy/compose/secrets
COMPOSE="docker compose -f deploy/compose/compose.yaml"
API=https://localhost:8443
CA=$SECRETS/ca-bundle.pem
N=${DEMO_DEVICES:-5}
DURATION=${DEMO_SECONDS:-60}

if [ "${1:-}" = "stop" ]; then
  for p in "$STATE"/agent-*/pid; do
    [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null || true
  done
  echo "demo agents stopped"
  exit 0
fi

command -v jq >/dev/null || { echo "demo.sh needs jq" >&2; exit 1; }
"$0" stop >/dev/null
mkdir -p "$STATE"
chmod 700 "$STATE"

# The token goes to fwctl in its environment: on the command line every
# local user could read it in the process list.
fw() { local tok=$1; shift; FWCTL_TOKEN=$tok bin/fwctl -url "$API" -ca-file "$CA" "$@"; }

echo "== operator token"
op=$($COMPOSE run --rm --no-deps -T migrate create-operator-token -name "demo-$(date +%s)" 2>/dev/null)
echo "== tenant"
tenant=$(fw "$op" -o json tenant create -name "demo-$(date +%H%M%S)" | jq -r .id)
tok=$(fw "$op" -o json token create -tenant "$tenant" -name demo-admin \
  -scopes tenant:admin,devices:read,devices:admin,readings:read,exports:read,exports:write,audit:read | jq -r .token)
umask 077
printf '%s\n' "$tok" > "$STATE/tenant-token"
mapfile -t enroll < <(fw "$tok" -o json enroll-token create -count "$N" | jq -r '.[].token')

echo "== starting $N agents"
for i in $(seq 1 "$N"); do
  d=$STATE/agent-$i
  rm -rf "$d"
  mkdir -p "$d"
  # The config file keeps the one-time token out of the process list.
  jq -n --arg url https://localhost:8444 --arg token "${enroll[$((i - 1))]}" --rawfile ca "$CA" \
    --arg name "demo-$i" --arg listen "127.0.0.1:$((9470 + i))" \
    '{enroll: {url: $url, token: $token, "ca-bundle": $ca, name: $name},
      collect: {interval: "2s"}, metrics: {listen: $listen}}' > "$d/agent.json"
  nohup bin/fleetwire-agent run -data-dir "$d" -config "$d/agent.json" > "$d/agent.log" 2>&1 &
  echo $! > "$d/pid"
done

echo "== collecting for ${DURATION}s"
sleep "$DURATION"
fw "$tok" device list
first=$(fw "$tok" -o json device list | jq -r '.[0].id')
echo "== the latest readings of $first"
fw "$tok" device readings "$first" -limit 12
for i in $(seq 1 "$N"); do
  bin/fleetwire-agent status -data-dir "$STATE/agent-$i" -set "metrics.listen=127.0.0.1:$((9470 + i))" | grep -E "last_seq|acked" | paste -sd' ' | sed "s/^/agent-$i: /"
done

cat <<EOF

The agents keep sending until you run: make demo-stop
  Tenant token:  $STATE/tenant-token
  API:           FWCTL_TOKEN=\$(cat $STATE/tenant-token) bin/fwctl -url $API -ca-file $CA device list
  Grafana:       http://localhost:3000   (dashboard "fleetwire")
  Prometheus:    http://localhost:9090
  Jaeger:        http://localhost:16686  (service fleetwire-ingest)
  Agent logs:    $STATE/agent-N/agent.log
EOF
