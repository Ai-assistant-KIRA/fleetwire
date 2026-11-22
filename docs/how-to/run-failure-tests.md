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
load generator and wait until it has started every device, keep steady load
(`-steady`, 60 s), inject the fault, hold it, recover, measure the recovery
time, keep load until `-after` (60 s) has passed since the fault ended,
stop the load generator (it drains and writes `results.jsonl`), and run the
checker. A device that does not start (at the beginning, or when agent-kill
restarts the load generator) fails the scenario, and so does a checker
report with fewer devices than the run asked for.

| Scenario | Fault | Recovery |
|---|---|---|
| `network-cut` | Toxiproxy disables the device-to-broker proxy for 60 s | enable |
| `network-degraded` | 300 ms ± 200 ms latency and 512 KB/s per connection, both ways; `reset_peer` on 5% of connections; 120 s | remove the toxics |
| `mosquitto-restart` | `docker compose restart mosquitto` | automatic |
| `kafka-broker-kill` | SIGKILL `kafka-2`, 60 s | start it |
| `kafka-outage` | stop all three brokers, 60 s | start them |
| `postgres-outage` | SIGKILL `postgres`, 60 s | start it |
| `bridge-kill` | SIGKILL `bridge-1`, 30 s | start it |
| `ingest-kill` | SIGKILL `ingest-1`, 30 s | start it |
| `agent-kill` | SIGKILL the whole load generator | restart it with the same state directory |

**Recovery time** is the time from the end of the fault until every device
is connected again and the p99 end-to-end delay of newly stored readings is
under 5 s, both holding for 30 s; it is measured to the start of those 30 s.
The p99 comes from Prometheus over a 20 s window that never reaches back
before the end of the fault, so readings stored while the stack was still
healthy cannot make a recovery look instant. Prometheus scrapes every 5 s,
so the runner cannot see a recovery faster than about 5 to 10 s. For
`agent-kill` the time includes reopening every device. It also includes the
devices' reconnect backoff (full jitter, up to 60 s), so it is often close
to a minute after long outages.

The runner queries that p99 every 2 s from the end of the fault and stops
at the first value under 5 s. The 20 s window can still hold mostly
readings committed before or during the fault, so the first query can pass
before a backlog reaches the database. A recovery of `0s` in the recorded
tables says that the first query passed, not that the backlog was already
stored.

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
  stored about 770 readings/s of the 1,000/s offered with the state on
  disk. With the state on tmpfs it stored every reading once, but the VM
  was still saturated: 870 to 990 readings/s in the steady minute before
  each fault, a p99 delay of about 30 s, and recovery times of minutes
  ([table](../results/2026-11-10T030712Z-add94424b241.md)). The table
  records where the state was (the "Device state" line):

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
