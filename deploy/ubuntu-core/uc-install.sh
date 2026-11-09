#!/usr/bin/env bash
# Install the agent snap on the Ubuntu Core VM and enroll it against the
# Compose stack on this host. Inside the guest, the host is 10.0.2.2.
#
#   UC_USER=you deploy/ubuntu-core/uc-install.sh fleetwire-agent_*.snap FWE_TOKEN
set -euo pipefail
cd "$(dirname "$0")/../.."
SNAP_FILE=${1:?snap file}
TOKEN=${2:?enrollment token (fwe_...)}
SSH=(ssh -p 8022 -o StrictHostKeyChecking=accept-new "${UC_USER:?set UC_USER to the console-conf user}@localhost")
scp -P 8022 "$SNAP_FILE" deploy/compose/secrets/ca-bundle.pem "${UC_USER}@localhost:"
"${SSH[@]}" "sudo snap install --dangerous ./$(basename "$SNAP_FILE")"
for plug in system-observe hardware-observe network-bind; do
  "${SSH[@]}" "sudo snap connect fleetwire-agent:$plug" || true
done
"${SSH[@]}" "sudo snap set fleetwire-agent enroll.url=https://10.0.2.2:8444 broker.url=tls://10.0.2.2:18883 \
  enroll.token=$TOKEN enroll.name=uc24-vm enroll.ca-bundle=\"\$(cat ca-bundle.pem)\""
sleep 10
"${SSH[@]}" "snap services fleetwire-agent; sudo fleetwire-agent status"
