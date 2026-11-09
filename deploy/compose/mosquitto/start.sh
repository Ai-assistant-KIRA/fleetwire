#!/bin/sh
# Runs Mosquitto and reloads its CRL when the API replaces crl.pem.
# Mosquitto 2.x rereads crlfile on SIGHUP (checked in task 0.2). The API
# writes the file by rename, so the inode changes on every publish.
set -u
conf=/mosquitto/config/mosquitto.conf
if [ "$(id -u)" = "0" ]; then
  # Started as root (a throwaway VM where the dev secrets belong to root):
  # stay root, or the 0400 keys become unreadable after Mosquitto drops
  # privileges and the next CRL reload fails. On a workstation the stack
  # runs as the developer's own user and this does not apply.
  { cat "$conf"; echo "user root"; } > /tmp/mosquitto.conf
  conf=/tmp/mosquitto.conf
fi
mosquitto -c "$conf" &
pid=$!
trap 'kill -TERM "$pid" 2>/dev/null; wait "$pid"; exit $?' TERM INT
last=$(stat -c '%i-%Y' /crl/crl.pem 2>/dev/null || echo none)
while kill -0 "$pid" 2>/dev/null; do
  sleep 2 &
  wait $!
  cur=$(stat -c '%i-%Y' /crl/crl.pem 2>/dev/null || echo none)
  if [ "$cur" != "$last" ]; then
    echo "crl.pem changed; reloading Mosquitto"
    kill -HUP "$pid"
    last=$cur
  fi
done
wait "$pid"
