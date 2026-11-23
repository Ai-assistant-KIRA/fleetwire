# fleetwire

A telemetry pipeline for fleets of Ubuntu Core devices that stores every
reading exactly once, through network cuts, broker and database outages,
process kills and power loss, and keeps each tenant's data apart and
encrypted with the tenant's own keys.

> For every device, every seq that left the device is, after the system
> recovers, either stored exactly once, or covered by a gap that the device
> itself reported. No seq is stored twice. No seq disappears without a
> report.

The agent keeps each reading on disk until an end-to-end acknowledgement
says it is stored. Every hop in between (MQTT QoS 1, Kafka, the consumer
group) may lose or repeat messages, and the agent resends until the ack
comes. Ingest removes the repeats under a per-device row lock, and a
PostgreSQL exclusion constraint makes storing a seq twice impossible.
[The delivery guarantee](docs/explanation/delivery-guarantee.md) has the
argument and its limits.

Written by Reda Alaarabi. The documentation (tutorial, how-to guides,
reference, explanations and ADRs) is published at
<https://ai-assistant-kira.github.io/fleetwire/>.

```mermaid
flowchart LR
  subgraph Device["Device: strict snap on Ubuntu Core 24"]
    C[Collectors] --> W[(Local log)] --> S[Uplink]
  end
  S -- "MQTT v5, mTLS" --> M[Mosquitto]
  M -- "shared subscription" --> B[Bridge x2]
  B -- "acks=all" --> K[(Kafka x3)]
  K --> I[Ingest x2]
  I -- "one transaction per batch,\nencrypted with tenant keys" --> P[(PostgreSQL\nrow-level security)]
  I -- "end-to-end ack" --> M --> S
  A[API, jobs, CRL] --> P
  S -- "enroll, renew" --> A
```

## Results

Every scenario runs the real stack (`make up`) with thousands of real agent
instances in one process, injects the fault (or kills the process), keeps
the load on, and then reads everything back from the database, decrypted.
A scenario passes only with 0 missing and 0 duplicate seqs. Recovery is the
time from the end of the fault until every device is connected again and
the p99 delay from collection to commit, over readings stored after the
fault, is under 5 s, both holding for 30 s; for the agent kill it includes
reopening every device. Prometheus scrapes every 5 s, so no recovery can
come out below about 5 to 10 s
([how it is measured](docs/how-to/run-failure-tests.md)).

All runs used one AWS EC2 m7i-flex.xlarge (4 vCPU Intel Xeon Platinum
8488C, 16 GB RAM, Ubuntu 24.04) for the load generator, the whole stack and
the observability tools, at commit `8ffb129828b1`. The tables, with the
exact commands, are in [`docs/results/`](docs/results/).

**2,000 devices**, one reading every 10 s each, device state on disk
(`make chaos`, [table](docs/results/2026-11-23T161203Z-8ffb129828b1.md)):

| Scenario | Readings | Missing | Duplicates | Recovery | Result |
|---|---:|---:|---:|---:|---|
| network cut, 60 s | 50,034 | 0 | 0 | 1m16s | PASS |
| network degraded (300 ± 200 ms and 512 KB/s both ways, 5% resets), 120 s | 52,610 | 0 | 0 | 6s | PASS |
| Mosquitto restart | 28,559 | 0 | 0 | 26s | PASS |
| one Kafka broker killed, 60 s | 40,552 | 0 | 0 | 6s | PASS |
| all Kafka brokers stopped, 60 s | 47,208 | 0 | 0 | 57s | PASS |
| PostgreSQL killed, 60 s | 41,728 | 0 | 0 | 36s | PASS |
| bridge SIGKILL, 30 s | 34,601 | 0 | 0 | 28s | PASS |
| ingest SIGKILL, 30 s | 34,626 | 0 | 0 | 26s | PASS |
| agent SIGKILL (all 2,000 at once) | 28,240 | 0 | 0 | 29s | PASS |

**10,000 devices**, one reading every 10 s each (1,000 readings/s), device
state on tmpfs (see Limitations,
[table](docs/results/2026-11-23T164507Z-8ffb129828b1.md)):

| Scenario | Readings | Missing | Duplicates | Recovery | Result |
|---|---:|---:|---:|---:|---|
| network cut, 60 s | 805,536 | 0 | 0 | > 10m | PASS |
| network degraded, 120 s | 843,254 | 0 | 0 | > 10m | PASS |
| Mosquitto restart | 730,269 | 0 | 0 | > 10m | PASS |
| one Kafka broker killed, 60 s | 781,093 | 0 | 0 | > 10m | PASS |
| all Kafka brokers stopped, 60 s | 797,762 | 0 | 0 | > 10m | PASS |
| PostgreSQL killed, 60 s | 787,799 | 0 | 0 | > 10m | PASS |
| bridge SIGKILL, 30 s | 751,169 | 0 | 0 | > 10m | PASS |
| ingest SIGKILL, 30 s | 751,241 | 0 | 0 | > 10m | PASS |
| agent SIGKILL (all 10,000 at once) | 698,046 | 0 | 0 | > 10m | PASS |

Every reading was stored exactly once, but not on time. The host was
saturated (load average 100 to 115 on its 4 vCPUs through the suite), and
no scenario met the recovery rule within the runner's 10-minute wait
("> 10m"). The rate is not the limit: the throughput search below kept up
with 2,000 batches/s from 2,000 devices. The cost is in the number of
devices, each with its own TLS connection, local log and state file, all
on the same 4 vCPUs as the stack.

**Power loss on Ubuntu Core 24**: 10 hard resets (QMP `system_reset`) of
an Ubuntu Core 24 VM (Canonical's signed `ubuntu-core-24-amd64` model)
running the strict agent snap as `snap_daemon`, at random moments, with a
collection every 200 ms (11 readings from the built-in collectors, about
55 readings/s) and `wal.sync=interval`: 6,293 readings, all acknowledged,
0 missing, 0 duplicates
([table](docs/results/2026-11-23T195718Z-b0047ec26411-poweroff.md)). The
same device had first been refreshed from v0.1.1, which moved its state to
`$SNAP_DATA/state` without losing a reading.

| Target | Measured |
|---|---|
| Correctness: 0 missing, 0 duplicates in every failure scenario | Met: 9 of 9 at 2,000 devices, 9 of 9 at 10,000, and 10 power cuts on Ubuntu Core 24 |
| Scale: 10,000 devices, one reading every 10 s, on one dev VM | All 10,000 ran and every reading was stored once, with the devices' state on tmpfs; on time only up to fewer devices (see above) |
| Latency: p99 from collection to commit under 5 s at normal load | Met: p99 2.49 s from 250 to 1,500 batches/s in the throughput search, most of it the agent's own 1 s batch wait and 1 s fsync interval; 4.48 s at 2,000/s. Not met at 10,000 devices on this host |
| Throughput: highest rate before the lag keeps growing | At least 2,000 batches/s of one reading, the highest step tried; in batches of 64, 32,000 readings/s ([table](docs/results/2026-11-23T185446Z-8ffb129828b1-throughput.md)) |
| Agent: under 30 MB RSS, binary under 20 MB | 16.9 to 19.3 MB RSS (the snap daemon on Ubuntu Core 24, first 7 minutes); binary 16.3 MB, snap 5.2 MB |

## Quick start

You need Linux with Docker (Compose v2), Go (the version in `go.mod`) and
`make`.

```console
$ make demo        # dev CA, stack, migrations, 5 real agents, 60 s of data
$ make e2e         # end-to-end test against the stack
$ make chaos SCENARIOS=network-cut,ingest-kill DEVICES=200
```

`make demo` prints the tenant token and where to look: the API (with
`fwctl`), Grafana on http://localhost:3000, Prometheus and Jaeger. The
[tutorial](docs/tutorial/run-a-fleet.md) walks through it, and
[Run the failure tests](docs/how-to/run-failure-tests.md) covers the suite.

On a device, build the snap (`make snap`, snapcraft with LXD) and install it;
it is not in the Snap Store yet:

```console
$ sudo snap install --dangerous ./fleetwire-agent_*.snap
$ sudo snap set fleetwire-agent enroll.url=https://api.example.com:8444 enroll.token=fwe_...
```

On Kubernetes, the Juju charms deploy the backend with Charmed PostgreSQL
and Charmed Kafka: [Deploy with Juju](docs/how-to/deploy-with-juju.md).

Tagged releases attach Linux binaries, `fwctl` for Linux and macOS, the
amd64 and arm64 agent snaps, SPDX SBOMs and a signed checksum file, and
push the API, bridge and ingest images to GHCR. [Verify a release](docs/how-to/verify-a-release.md) shows
how to check the signatures.

## What is in the repository

| Path | What |
|---|---|
| `cmd/fleetwire-agent`, `internal/agent`, `internal/wal` | the device agent, its local log and uplink |
| `cmd/fleetwire-bridge`, `internal/bridge` | Mosquitto to Kafka, acking only after Kafka confirms |
| `cmd/fleetwire-ingest`, `internal/ingest`, `internal/coverage` | exactly-once storage, encryption, acks |
| `cmd/fleetwire-api`, `internal/api`, `internal/jobs`, `api/openapi.yaml` | tenant, operator and device API; retention, exports, destroy |
| `internal/store` | PostgreSQL schema, row-level security, migrations |
| `cmd/fwctl` | the CLI |
| `cmd/fleetwire-loadgen`, `-checker`, `-chaos`, `scripts/throughput.sh` | load generator, checker, failure suite, throughput search |
| `snap/`, `deploy/ubuntu-core/` | the agent snap and the VM scripts for the power-off test |
| `charms/` | Juju charms, the bundle and its integration test |
| `deploy/compose/`, `deploy/prometheus/`, `deploy/grafana/` | development stack, SLO rules, dashboard |
| `docs/` | Sphinx docs (Diátaxis) and the ADRs |

## Limitations

- **10,000 devices are more than the test host serves on time.** Every
  reading was stored once, but with the load generator, the stack and the
  observability tools on 4 vCPUs, no failure scenario got back to a p99
  under 5 s within 10 minutes. Even that needs the devices' state on tmpfs:
  each simulated device fsyncs its local log and state file, as a real
  device does on its own disk, and on one machine those fsyncs, about 3,000
  a second, share the disk with PostgreSQL's commits. On the earlier
  UpCloud test VM, with the state on disk, ingest stored about 770 of the
  1,000 readings/s offered. The tmpfs runs do not test power loss; the
  power-off test does.
- **Ubuntu Core ran under QEMU without KVM.** The power-off test used a
  real Ubuntu Core 24 image, but under software emulation on a cloud host
  without `/dev/kvm`, not on device hardware. Core's first boot needs an
  Ubuntu One account, so that one step (console-conf) was done by hand;
  the install, enrollment and resets are scripted.
- **The agent snap is not in the Snap Store.** Build it with `make snap`
  and install it with `--dangerous`. Each release also attaches the amd64
  and arm64 snaps and their SBOMs, which the signed `checksums.txt` covers.
- **One API unit on Kubernetes.** The local keyring keeps the tenant KEKs
  on the unit's storage. A Vault or KMS keyring backend would lift this.
- **Revocation at the broker lags on Kubernetes** by up to the
  update-status interval (5 minutes by default). Ingest drops a revoked
  device's data at once; in Compose the broker reloads its CRL within
  seconds.
- **Charmed PostgreSQL must be dedicated to fleetwire**: the runtime
  logins' `charmed_dml` role can read and write every database of the
  cluster.
- **Row-level security guards against tenant-scoping mistakes, not a
  compromised service**: any session of the runtime role can choose a
  tenant. What such a service cannot do is change the schema, drop the
  policies or rewrite the audit log.
- **Destroying a tenant deletes its KEK file**, which is not a secure erase
  on an SSD; production should use a KMS with real key destruction.
- **The database cannot aggregate readings**: they are encrypted with
  tenant keys, so the API decrypts them for every query.
- **Latency uses the device's clock** (collection time to commit), so a
  device with a wrong clock skews the delay SLO;
  `fleetwire_ingest_clock_skew_seconds` shows how far off devices are.
- **The load generator's devices share one kernel, network stack and
  disk**, and their reading values are synthetic; see
  [how it differs from a real device](docs/explanation/load-generator.md).

## Future work

- A Vault Transit or cloud KMS keyring backend: real key destruction, and
  more than one API unit on Kubernetes. The `Keyring` interface is ready.
- Over-the-air agent updates beyond what snap refreshes give.
- A web frontend; aggregation and downsampling of readings.
- Rocks for the workloads and deb packages for non-snap systems.
- Multi-region replication.

## License

Apache License 2.0. Copyright Reda Alaarabi.
