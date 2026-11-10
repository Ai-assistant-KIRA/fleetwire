# Run the failure tests

The chaos runner checks the delivery guarantee under nine faults. A scenario
passes only with **0 missing and 0 duplicates**.

```console
$ make chaos                                           # all scenarios, 2,000 devices
$ make chaos SCENARIOS=network-cut,ingest-kill DEVICES=200
$ bin/fleetwire-chaos list
```

`make chaos` builds the binaries, starts the stack and runs
`bin/fleetwire-chaos run`; call the binary directly for the other flags
(`-steady`, `-after`, `-wal-sync`, `-interval`).

Each scenario: wait for a healthy stack, create a fresh tenant, start the
load generator, keep steady load (`-steady`, 60 s), inject the fault, hold
it, recover, measure the recovery time, keep load for `-after` (60 s), stop
the load generator (it drains and writes `results.jsonl`), and run the
checker.

| Scenario | Fault | Recovery |
|---|---|---|
| `network-cut` | Toxiproxy disables the device-to-broker proxy for 60 s | enable |
| `network-degraded` | 300 ms ± 200 ms latency both ways, 512 KB/s, `reset_peer` on 5% of connections, 120 s | remove the toxics |
| `mosquitto-restart` | `docker compose restart mosquitto` | automatic |
| `kafka-broker-kill` | SIGKILL `kafka-2`, 60 s | start it |
| `kafka-outage` | stop all three brokers, 60 s | start them |
| `postgres-outage` | SIGKILL `postgres`, 60 s | start it |
| `bridge-kill` | SIGKILL `bridge-1`, 30 s | start it |
| `ingest-kill` | SIGKILL `ingest-1`, 30 s | start it |
| `agent-kill` | SIGKILL the whole load generator | restart it with the same state directory |

**Recovery time** is the time from the end of the fault until the p99
end-to-end delay of newly stored readings (from Prometheus, 20 s window) is
under 5 s again. It includes the devices' reconnect backoff (full jitter, up
to 60 s), so it is often close to a minute after long outages.

Results go to `docs/results/<time>-<commit>.md` and `.json`. Raw logs,
`results.jsonl` and checker reports go to `docs/results/raw/` (git-ignored).
Each device's local log and keys are deleted once the checker has read
them; they are about nine files per device.

## Large runs on one machine

With thousands of devices, the load generator and the whole stack share one
machine, so the harness itself costs a large part of it. Two settings of
the host matter:

- Turn off Docker's userland proxy, which otherwise copies every byte of
  every device connection through a `docker-proxy` process (on the test VM
  it took about 15% of a core and 180 MB at 10,000 devices):

  ```console
  $ echo '{"userland-proxy": false}' | sudo tee /etc/docker/daemon.json
  $ sudo systemctl restart docker
  ```

- Leave memory for the devices: each load-generator device needs about
  150 KB, and Toxiproxy about the same per connection. A machine that swaps
  measures its disk, not fleetwire.
- Keep the devices' local state off the disk the database uses. Each
  device fsyncs its log and its state file, as a real device does on its
  own disk; on one machine thousands of them share one disk with
  PostgreSQL's commit flushes. At 10,000 devices on the test VM, ingest
  stored 770 readings/s of the 1,000/s offered with the state on disk, and
  kept up (1,066/s while it drained the backlog) with the state on tmpfs.
  The table records which (the "Device state" line):

  ```console
  $ sudo mkdir -p /mnt/fw-devices && sudo mount -t tmpfs -o size=1g,mode=0700 tmpfs /mnt/fw-devices
  $ make chaos DEVICES=10000 STATE_DIR=/mnt/fw-devices
  ```

  10,000 devices need about 250 MB there; each scenario's state is deleted
  after its check.

## Checking one device by hand

```console
$ bin/fleetwire-checker -device <id> -last-seq <n> [-gaps 100-180]
```

It reads the device's chunks from PostgreSQL as the runtime role, decrypts
them with the keyring, and exits 1 if any seq is missing or duplicated.
