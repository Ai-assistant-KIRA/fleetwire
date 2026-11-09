# Enroll a device

## 1. Get an enrollment token

With a tenant token that has `devices:admin`:

```console
$ fwctl enroll-token create -count 1 -ttl 15m
TOKEN                                                     EXPIRES_AT
fwe_...                                                   2026-11-09T06:15:00Z
```

Or as the operator for a tenant: `fwctl enroll-token create -tenant <id>`.
A token enrolls one device, once.

## 2a. On Ubuntu Core (snap)

```console
$ sudo snap install fleetwire-agent            # or --dangerous ./fleetwire-agent_*.snap
$ sudo snap connect fleetwire-agent:hardware-observe
$ sudo snap set fleetwire-agent \
    enroll.url=https://api.example:8444 \
    enroll.token=fwe_... \
    enroll.name=line-3-gateway
$ sudo fleetwire-agent status
```

For a private CA add `enroll.ca-bundle="$(cat ca-bundle.pem)"`. The
configure hook validates every value with the daemon's own rules; a bad
value is refused by `snap set`.

| Interface | Why |
|---|---|
| `network` | connect to the broker and the enrollment API |
| `network-bind` | serve `/metrics` and `/status` on `127.0.0.1:9465` |
| `system-observe` | read `/proc/stat`, `/proc/meminfo`, `/proc/loadavg`, `/proc/uptime` |
| `hardware-observe` | read `/sys/class/thermal` (not auto-connected on every system) |

## 2b. Anywhere else

```console
$ fleetwire-agent run -data-dir /var/lib/fleetwire-agent \
    -set enroll.url=https://api.example:8444 -set enroll.token=fwe_...
```

The agent creates its key on first start (it never leaves the device),
sends a CSR with the token, and stores the certificate, the CA bundle and
the broker URL it receives. The token is not needed after that.

## If enrollment fails

- `snap get`/`fleetwire-agent status` shows "enrollment refused": the token
  is unknown, expired or already used. Create a new one and set it.
- The token was consumed but the response never arrived (network cut at the
  wrong moment): the server has a device that never connects. Revoke it and
  enroll again with a new token.

## Re-enrolling

A device whose certificate expired (offline more than 30 days) must be
enrolled again. Stop the agent, delete `identity/` and `wal/` in its data
directory, set a new token, start it. It becomes a new device with a new
seq space; readings it had not sent are lost.
