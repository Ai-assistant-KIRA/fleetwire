# Run the agent on Ubuntu Core 24 and test power loss

## Build the snap

On an Ubuntu machine with LXD:

```console
$ sudo snap install snapcraft --classic
$ sudo snap install lxd && sudo lxd init --auto
$ make snap            # runs snapcraft pack in an LXD container
```

## Boot Ubuntu Core 24 in QEMU

```console
$ deploy/ubuntu-core/uc-vm.sh download   # image from cdimage.ubuntu.com, checksum verified
$ deploy/ubuntu-core/uc-vm.sh start      # UEFI (OVMF), QMP socket, SSH on localhost:8022
$ deploy/ubuntu-core/uc-vm.sh console    # serial console for first boot
```

**First boot needs a person.** console-conf asks for the e-mail of an
Ubuntu One account that has an SSH key registered at
<https://login.ubuntu.com>, and creates a user with that key. After that:

```console
$ ssh -p 8022 <user>@localhost
```

Without `/dev/kvm` (for example on a cloud VM without nested
virtualisation) QEMU falls back to software emulation. It works, but the
first boot takes several minutes.

## Install and enroll

With the Compose stack running on the host (`make up`) and an enrollment
token (`fwctl enroll-token create`):

```console
$ UC_USER=<user> deploy/ubuntu-core/uc-install.sh fleetwire-agent_*.snap fwe_...
```

Inside the guest, the host is `10.0.2.2`; the script sets `enroll.url` and
`broker.url` to it and passes the dev CA bundle.

## Who the agent runs as and where its state is

snapd starts the daemon as root. Its command chain prepares the state
directory, then runs the agent as the `snap_daemon` system user (uid and
gid 584788) through `setpriv` from the core24 base, which leaves the
process no capabilities. `sudo fleetwire-agent ...` runs as `snap_daemon`
too.

The agent keeps its state in `$SNAP_DATA/state`, that is
`/var/snap/fleetwire-agent/current/state`, mode 0700, owned by
`snap_daemon`:

| Path | Holds |
|---|---|
| `state/identity/` | `key.pem` (the device key, mode 0600), `cert.pem`, `ca.pem`, `device.json` |
| `state/wal/` | the segments and `state.json` (see [the device's local log](../explanation/wal.md)) |

The install and post-refresh hooks create the directory as root before
snapd starts the daemon, and the daemon's command chain checks it again at
each start: it recreates a deleted directory and gives back one that root
owns. The state cannot be in `$SNAP_DATA` itself: snapd creates that
directory owned by root, and the snap's AppArmor profile does not let even
root change its owner.

Without sudo, `fleetwire-agent status` asks the daemon on
`127.0.0.1:9465`. When the daemon is stopped, the command reads the files,
and only `snap_daemon` can: use `sudo fleetwire-agent status`. As the
daemon is not root, a `metrics.listen` port below 1024 does not work.

## Refresh from v0.1.1

v0.1.1 and earlier ran the daemon as root and kept `identity/` and `wal/`
directly in `$SNAP_DATA`. snapd copies `$SNAP_DATA` to the new revision on
a refresh, and the new revision's post-refresh hook moves both into
`state/` and gives it to `snap_daemon` before snapd starts the daemon. The
device keeps its identity and every reading in its log, sent or not, and
continues with the next seq. The hook assembles the new directory as
`state.partial` with renames and renames it to `state` last, so a crash or
power cut during the move loses nothing, and the next run finishes it. If
the hook fails, snapd rolls the refresh back, and the old revision
continues on its own copy of `$SNAP_DATA`.

## Revert to v0.1.1

`snap revert` copies no data and runs no refresh hook. The old revision
gets back its own copy of `$SNAP_DATA` and its configuration as they were
at the refresh, so v0.1.1 finds `identity/` and `wal/` where it left them
and runs as root, as before. That copy lacks what happened after the
refresh:

- Readings collected since then stay in the newer revision's directory,
  `/var/snap/fleetwire-agent/<revision>/state/wal`. v0.1.1 does not read
  it, and snapd deletes it with that revision at the next refresh.
- The old copy hands out seqs that the newer revision already sent. When
  the server's ack arrives, the agent logs `server ack is ahead of the
  device's next seq` and moves its seq forward, but the readings it wrote
  before that carry seqs the server already stored, and ingest drops them
  as duplicates.
- A certificate renewed after the refresh is not in the old copy. The
  older certificate stays valid until it expires, and the agent renews it
  before then.

To revert without losing readings, empty the log first and start the old
revision past the last seq:

1. Stop collecting, and wait until `acked` equals `last_seq`. This needs
   the broker to be reachable. The new setting restarts the daemon, which
   collects once at start:

   ```console
   $ sudo snap set fleetwire-agent collect.interval=1h
   $ sudo fleetwire-agent status | grep -E 'last_seq|acked'
   last_seq:       1110
   acked:          1110
   ```

2. Revert with the daemon stopped. It stays disabled after the revert:

   ```console
   $ sudo snap stop --disable fleetwire-agent.daemon
   $ sudo snap revert fleetwire-agent
   ```

3. Write the old copy's `state.json` with that seq as the ack and the
   highest seq, as in [recovering a corrupt state file](recover-device-state.md),
   but at v0.1.1's path, then start the daemon:

   ```console
   $ W=1110
   $ printf '{"version":1,"acked":%d,"max_seq_ever":%d,"pending_gaps":[]}\n' $W $W |
       sudo tee /var/snap/fleetwire-agent/current/wal/state.json >/dev/null
   $ sudo chmod 0600 /var/snap/fleetwire-agent/current/wal/state.json
   $ sudo snap start --enable fleetwire-agent.daemon
   ```

The agent logs `local log recovered` with `next_seq` 1111, and
`collect.interval` is back to its value from before the refresh. A revert
between two later revisions works the same way, with the file at
`state/wal/state.json` and given to `snap_daemon` as in the recovery
how-to.

## The power-off test

```console
$ UC_USER=<user> RESETS=10 deploy/ubuntu-core/uc-poweroff-test.sh
```

The script sets `collect.interval=200ms` and `wal.sync=interval`, then ten
times waits 5 to 30 s and sends QMP `system_reset`: a hard reset with no
clean shutdown. Each collection writes 11 readings from the built-in
collectors, plus one per thermal zone, so a VM without thermal zones
writes about 55 readings/s while the agent runs. The recorded run's 9,089
readings come to about 165 s of collecting; the rest of its 17 minutes
went to rebooting under software emulation and to the final drain.

After the last boot the script waits until the agent's last seq is
acked, reads `fleetwire-agent status -json` over SSH, and runs the checker
for that device. The pass rule is the same as the chaos suite: 0 missing,
0 duplicates. The table goes to `docs/results/<time>-<commit>-poweroff.md`.

The scripts need `qemu-system-x86`, `ovmf`, `socat` and `jq` on the host.

## Without an Ubuntu One account: Ubuntu Server

`server-vm.sh` boots the Ubuntu Server 24.04 cloud image with the same
layout (UEFI, QMP socket, SSH on localhost:8022). cloud-init creates the
user `fleetwire` with a key made for this VM, so no person is needed:

```console
$ sudo apt install cloud-image-utils socat
$ deploy/ubuntu-core/server-vm.sh download
$ deploy/ubuntu-core/server-vm.sh start
$ export UC_USER=fleetwire UC_SSH_KEY=$PWD/deploy/ubuntu-core/work/id_ed25519 UC_NAME=server-vm
$ deploy/ubuntu-core/uc-install.sh fleetwire-agent_*.snap fwe_...
$ deploy/ubuntu-core/uc-poweroff-test.sh
```

The agent runs as the same strictly confined snap, on a real kernel and
ext4, and the reset is as hard. What this does not test is Ubuntu Core's
own boot and its writable partition layout.
