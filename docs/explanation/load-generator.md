# The load generator and how it differs from a real device

`fleetwire-loadgen` runs thousands of device instances in one process.
Each instance is an `agent.Device`, the same type `fleetwire-agent` runs:

- its own local log directory and state file on disk (`state-dir/<n>/wal`),
- its own ECDSA key and certificate, from the real enrollment API,
- its own mutual-TLS MQTT connection, through Toxiproxy to Mosquitto,
- the real uplink, batching, ack handling, resends and gap reporting.

**Only the reading values are generated** (`collect.Synthetic`: a sine wave
plus noise). Nothing else is simulated or mocked.

## Differences from a real device

- All instances share one kernel, one disk and one network stack. fsync cost
  is shared, so large runs may use `-wal-sync interval`; the results table
  records the mode used.
- There is no snap confinement, no Ubuntu Core, and no real power loss. The
  Ubuntu Core power-off test (`deploy/ubuntu-core/uc-poweroff-test.sh`)
  covers those.
- Instances use smaller log segments (256 KiB) than the agent's default
  (4 MiB), because each writes little and thousands of 4 MiB files only
  waste disk.
- Every instance fsyncs its log and its state file, as a real device does,
  but on one machine they all reach the same disk, which the database uses
  too. Large runs can put the state directory on tmpfs (`-state-dir`),
  where an fsync costs nothing; that also means those runs do not test
  durability against power loss, which the power-off test covers. The
  results tables say where the state was.
- Killing the load generator (`agent-kill` scenario) kills every instance at
  once with SIGKILL, which is harsher than one device crashing.

## Output

On SIGTERM the generator stops collecting, waits until every instance has
its last seq acked (or the drain timeout passes), and writes
`state-dir/results.jsonl`: one line per device with its ID, last seq, acked
seq and every gap it reported. The checker reads that file.
