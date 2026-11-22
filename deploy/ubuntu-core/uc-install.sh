#!/usr/bin/env bash
# Install the agent snap on the Ubuntu Core VM and enroll it against the
# Compose stack on this host. Inside the guest, the host is 10.0.2.2.
#
#   UC_USER=you deploy/ubuntu-core/uc-install.sh fleetwire-agent_*.snap FWE_TOKEN
#
# UC_SSH_KEY picks the SSH key (server-vm.sh creates one in its work
# directory).
set -euo pipefail
cd "$(dirname "$0")/../.."
SNAP_FILE=${1:?snap file}
TOKEN=${2:?enrollment token (fwe_...)}
USER_HOST="${UC_USER:?set UC_USER to the console-conf user}@localhost"
OPTS=(-o StrictHostKeyChecking=accept-new)
if [ -n "${UC_SSH_KEY:-}" ]; then
  OPTS+=(-i "$UC_SSH_KEY")
fi
# ssh joins its arguments into one string for the guest's shell, so every
# argument is quoted with printf %q first: a token, a name or a file name
# stays one argument, never part of the sudo command line. %q quotes for
# bash, the login shell of the console-conf and server-vm.sh users.
guest() {
  local cmd
  printf -v cmd '%q ' "$@"
  ssh -p 8022 "${OPTS[@]}" "$USER_HOST" "$cmd"
}
snap_name=$(basename "$SNAP_FILE")
scp -P 8022 "${OPTS[@]}" "$SNAP_FILE" "$USER_HOST:"
guest sudo snap install --dangerous "./$snap_name"
for plug in system-observe hardware-observe network-bind; do
  guest sudo snap connect "fleetwire-agent:$plug" || true
done
# The CA bundle goes as a value, read here; it no longer needs a copy in
# the guest.
guest sudo snap set fleetwire-agent \
  enroll.url=https://10.0.2.2:8444 broker.url=tls://10.0.2.2:18883 \
  "enroll.token=$TOKEN" "enroll.name=${UC_NAME:-uc24-vm}" \
  "enroll.ca-bundle=$(cat deploy/compose/secrets/ca-bundle.pem)"
sleep 10
guest snap services fleetwire-agent
guest sudo fleetwire-agent status
