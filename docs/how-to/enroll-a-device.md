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

Until enrollment succeeds, `fleetwire-agent status` shows no `device_id`,
and `status -json` has `"enrolled": false`. The snap's health and the
agent's log say why:

```console
$ snap info fleetwire-agent
...
health:
  status:   blocked
  message:  "enrollment refused: set a new enroll.token"
...
$ sudo snap logs -n 20 fleetwire-agent | grep enrollment
```

- Health `blocked`, "enrollment refused: set a new enroll.token" (and
  `blocked` in the Notes column of `snap list`): the API answered with a
  4xx error other than 429, or `enroll.ca-bundle` holds no certificate, so
  a retry cannot succeed. The log line "enrollment refused; waiting for a
  new token" carries the reason: `HTTP 401 invalid_token` when the token
  is unknown, expired or already used, `HTTP 403 tenant_inactive` when the
  tenant is destroyed or being destroyed. Create a new token and set it;
  the configure hook restarts the agent, which tries again at once.
- Health `waiting`, "enrollment failed; retrying": the API was unreachable
  or answered 429 or 5xx. The agent retries every 30 s on its own; the log
  line "enrollment failed; retrying" has the error.
- The token was consumed but the response never arrived (network cut at the
  wrong moment): the server has a device that never connects. Revoke it and
  enroll again with a new token.

Outside a snap there is no health; the same messages go to the log on
standard error.

## Re-enrolling

A device whose certificate expired (offline more than 30 days) must be
enrolled again. Stop the agent, delete `identity/` and `wal/` in its data
directory (`/var/snap/fleetwire-agent/current/state` in the snap), set a
new token, start it. It becomes a new device with a new seq space;
readings it had not sent are lost.
