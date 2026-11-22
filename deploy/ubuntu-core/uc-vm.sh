#!/usr/bin/env bash
# Boot Ubuntu Core 24 (amd64) in QEMU with UEFI, a QMP socket for hard
# resets, and port forwards: guest SSH on localhost:8022.
#
#   deploy/ubuntu-core/uc-vm.sh download   fetch and unpack the image once
#   deploy/ubuntu-core/uc-vm.sh start      boot in the background
#   deploy/ubuntu-core/uc-vm.sh console    attach to the serial console
#   deploy/ubuntu-core/uc-vm.sh stop
#
# First boot runs console-conf on the serial console: it needs an Ubuntu One
# account e-mail with an SSH key registered at login.ubuntu.com, and creates
# a user with that key. After that: ssh -p 8022 <user>@localhost.
# Without /dev/kvm QEMU falls back to software emulation (TCG); it works,
# but booting takes several minutes.
set -euo pipefail
cd "$(dirname "$0")"
WORK=${UC_WORK:-work}
IMG_URL=https://cdimage.ubuntu.com/ubuntu-core/24/stable/current/ubuntu-core-24-amd64.img.xz
IMG=$WORK/ubuntu-core-24-amd64.img
mkdir -p "$WORK"

ovmf_code() {
  for f in /usr/share/OVMF/OVMF_CODE_4M.secboot.fd /usr/share/OVMF/OVMF_CODE_4M.fd /usr/share/OVMF/OVMF_CODE.fd; do
    [ -f "$f" ] && { echo "$f"; return; }
  done
  echo "no OVMF firmware; install the ovmf package" >&2
  exit 1
}

case "${1:-}" in
download)
  if [ ! -f "$IMG" ]; then
    curl -fL "$IMG_URL" -o "$IMG.xz"
    curl -fL "${IMG_URL%/*}/SHA256SUMS" -o "$WORK/SHA256SUMS"
    # The exact file name, in text (" name") or binary ("*name") mode, as
    # server-vm.sh does: cdimage lists "*ubuntu-core-24-amd64.img.xz", and
    # a regex would also match other names that end the same way.
    expected=$(awk -v f="*$(basename "$IMG").xz" '$2 == f || $2 == substr(f, 2) {print $1}' "$WORK/SHA256SUMS")
    [ -n "$expected" ] || { echo "$(basename "$IMG").xz is not in SHA256SUMS" >&2; exit 1; }
    echo "$expected  $IMG.xz" | sha256sum -c -
    xz -dk "$IMG.xz"
    qemu-img resize -f raw "$IMG" 8G
  fi
  cp -n /usr/share/OVMF/OVMF_VARS_4M.fd "$WORK/OVMF_VARS.fd" 2>/dev/null || cp -n /usr/share/OVMF/OVMF_VARS.fd "$WORK/OVMF_VARS.fd"
  echo "image ready: $IMG"
  ;;
start)
  accel=tcg
  [ -w /dev/kvm ] && accel=kvm
  qemu-system-x86_64 \
    -machine q35,accel="$accel" -smp 2 -m 2048 \
    -drive if=pflash,format=raw,readonly=on,file="$(ovmf_code)" \
    -drive if=pflash,format=raw,file="$WORK/OVMF_VARS.fd" \
    -drive file="$IMG",format=raw,if=virtio,cache=none,aio=threads \
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
