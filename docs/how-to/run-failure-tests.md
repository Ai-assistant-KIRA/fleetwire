# Run the failure tests

The chaos runner checks the delivery guarantee under nine faults. A scenario
passes only with **0 missing and 0 duplicates**.

```console
$ make up build
$ bin/fleetwire-chaos run -devices 2000              # all scenarios
$ bin/fleetwire-chaos run -scenario network-cut,ingest-kill -devices 200
$ bin/fleetwire-chaos list
```

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

## Checking one device by hand

```console
$ bin/fleetwire-checker -device <id> -last-seq <n> [-gaps 100-180]
```

It reads the device's chunks from PostgreSQL as the runtime role, decrypts
them with the keyring, and exits 1 if any seq is missing or duplicated.
