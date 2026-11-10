#!/usr/bin/env bash
# Boot Ubuntu Server 24.04 (amd64 cloud image) in QEMU, laid out like
# uc-vm.sh: UEFI, a QMP socket for hard resets, guest SSH on localhost:8022,
# the same work directory. uc-install.sh and uc-poweroff-test.sh then run
# unchanged against it with UC_USER=fleetwire UC_SSH_KEY=<work>/id_ed25519.
#
# This is the stand-in for Ubuntu Core when its first boot cannot be done
# (console-conf needs an Ubuntu One account): the agent runs as the same
# strictly confined snap, on a real kernel and ext4, and the reset is just as
# hard. What it does not cover is Ubuntu Core's own boot and writable layout.
#
#   deploy/ubuntu-core/server-vm.sh download   fetch the image, write the cloud-init seed
#   deploy/ubuntu-core/server-vm.sh start      boot in the background
#   deploy/ubuntu-core/server-vm.sh console    attach to the serial console
#   deploy/ubuntu-core/server-vm.sh stop
#
# Needs qemu-system-x86, ovmf, cloud-image-utils (cloud-localds) and socat.
set -euo pipefail
cd "$(dirname "$0")"
WORK=${UC_WORK:-work}
BASE_URL=https://cloud-images.ubuntu.com/releases/noble/release
IMG_NAME=ubuntu-24.04-server-cloudimg-amd64.img
IMG=$WORK/$IMG_NAME
DISK=$WORK/server.qcow2
mkdir -p "$WORK"

ovmf_code() {
  for f in /usr/share/OVMF/OVMF_CODE_4M.fd /usr/share/OVMF/OVMF_CODE.fd; do
    [ -f "$f" ] && { echo "$f"; return; }
  done
  echo "no OVMF firmware; install the ovmf package" >&2
  exit 1
}

case "${1:-}" in
download)
  if [ ! -f "$IMG" ]; then
    curl -fL "$BASE_URL/$IMG_NAME" -o "$IMG.part"
    curl -fL "$BASE_URL/SHA256SUMS" -o "$WORK/SHA256SUMS.server"
    expected=$(awk -v f="*$IMG_NAME" '$2 == f || $2 == substr(f, 2) {print $1}' "$WORK/SHA256SUMS.server")
    [ -n "$expected" ] || { echo "$IMG_NAME is not in SHA256SUMS" >&2; exit 1; }
    echo "$expected  $IMG.part" | sha256sum -c -
    mv "$IMG.part" "$IMG"
  fi
  # A copy-on-write disk over the pristine image: `rm work/server.qcow2`
  # and download again for a fresh machine.
  [ -f "$DISK" ] || qemu-img create -q -f qcow2 -F qcow2 -b "$(basename "$IMG")" "$DISK" 12G
  # A key for this VM only; the guest accepts nothing else.
  [ -f "$WORK/id_ed25519" ] || ssh-keygen -q -t ed25519 -N '' -C fleetwire-server-vm -f "$WORK/id_ed25519"
  cat > "$WORK/user-data" <<EOF
#cloud-config
users:
  - name: fleetwire
    sudo: "ALL=(ALL) NOPASSWD:ALL"
    shell: /bin/bash
    ssh_authorized_keys:
      - $(cat "$WORK/id_ed25519.pub")
ssh_pwauth: false
EOF
  printf 'instance-id: fleetwire-server-vm\nlocal-hostname: fleetwire-server-vm\n' > "$WORK/meta-data"
  cloud-localds "$WORK/seed.img" "$WORK/user-data" "$WORK/meta-data"
  cp -n /usr/share/OVMF/OVMF_VARS_4M.fd "$WORK/OVMF_VARS.server.fd" 2>/dev/null || cp -n /usr/share/OVMF/OVMF_VARS.fd "$WORK/OVMF_VARS.server.fd"
  echo "disk ready: $DISK; ssh: ssh -i $WORK/id_ed25519 -p 8022 fleetwire@localhost"
  ;;
start)
  accel=tcg
  [ -w /dev/kvm ] && accel=kvm
  qemu-system-x86_64 \
    -machine q35,accel="$accel" -smp 2 -m 2048 \
    -drive if=pflash,format=raw,readonly=on,file="$(ovmf_code)" \
    -drive if=pflash,format=raw,file="$WORK/OVMF_VARS.server.fd" \
    -drive file="$DISK",format=qcow2,if=virtio,cache=none,aio=threads \
    -drive file="$WORK/seed.img",format=raw,if=virtio,readonly=on \
    -netdev user,id=net0,hostfwd=tcp:127.0.0.1:8022-:22 -device virtio-net-pci,netdev=net0 \
    -qmp unix:"$WORK/qmp.sock",server=on,wait=off \
    -serial unix:"$WORK/console.sock",server=on,wait=off \
    -display none -daemonize -pidfile "$WORK/qemu.pid"
  echo "booting (accel=$accel); console: $0 console"
  ;;
console)
  echo "detach with Ctrl-] then q (socat) or Ctrl-C"
  socat -,raw,echo=0,escape=0x1d UNIX-CONNECT:"$WORK/console.sock"
  ;;
stop)
  [ -f "$WORK/qemu.pid" ] && kill "$(cat "$WORK/qemu.pid")" && rm -f "$WORK/qemu.pid"
  ;;
*)
  echo "usage: $0 download|start|console|stop" >&2
  exit 2
  ;;
esac
