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

## The power-off test

```console
$ UC_USER=<user> RESETS=10 deploy/ubuntu-core/uc-poweroff-test.sh
```

The script sets `collect.interval=200ms` and `wal.sync=interval`, then ten
times waits 5 to 30 s and sends QMP `system_reset`: a hard reset with no
clean shutdown. After the last boot it waits until the agent's last seq is
acked, reads `fleetwire-agent status -json` over SSH, and runs the checker
for that device. The pass rule is the same as the chaos suite: 0 missing,
0 duplicates.
