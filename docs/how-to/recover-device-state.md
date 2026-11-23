# Recover a device's local log

The agent repairs most damage to its local log when it starts (see
[the device's local log](../explanation/wal.md)). It stops and waits for
you only when `wal/state.json` cannot be read. The data directory is
`/var/snap/fleetwire-agent/current/state` in the snap (`$SNAP_DATA/state`;
up to v0.1.1 it was `$SNAP_DATA` itself), and `-data-dir` (default
`/var/lib/fleetwire-agent`) elsewhere.

## What the agent repairs on its own

| Found at start | What the agent does | Log message |
|---|---|---|
| A torn record at the end of the newest segment (crash or power cut) | Cuts the file there. Those records were never durable, so they never left the device. | `wal tail truncated after crash` |
| A bad record inside an older segment | Moves the rest of that segment to `<segment>.seg.corrupt` and reports the seqs it can no longer read as a `CORRUPTION` gap | `wal segment corrupt; tail quarantined` |
| A segment with a bad header | Renames the whole file to `<segment>.seg.corrupt`; the gap is the same | `wal segment has a bad header; quarantined` |
| Seqs that should be on disk but are not (files deleted behind the agent's back) | Reports them as a `LOST` gap | `wal: seqs are not recoverable; reporting them as a gap` |

`fleetwire-agent status` lists a gap under `pending_gaps` until an ack
covers it, and `fwctl device gaps <id>` shows what the server recorded.
The agent keeps `.corrupt` files for inspection and does not count them
against `wal.max-size`; delete them when you no longer need them.

## The agent stops: `state.json` is corrupt

`state.json` holds the acked seq, the highest seq that was ever durable,
and the gaps the device still has to report. The agent replaces it
atomically, so a power cut leaves the old or the new version; a failing
disk or a bad hand edit can still break it. You see:

- snap health `error`, "cannot open state: see the agent log", and snapd
  restarting the daemon every 5 s;
- `fleetwire-agent: open local log: wal: state file is corrupt: ...` in
  `sudo snap logs fleetwire-agent`;
- `fleetwire-agent status` failing with `state unreadable: wal: state file
  is corrupt: ...`.

Starting from an empty state is safe only if the segments on disk still
hold the highest seq the device ever sent. The agent cannot tell whether
they do, and a lower next seq would give new readings seqs the server
already holds; ingest would drop them as duplicates. The server's
watermark settles it, so the repair writes a new `state.json` that starts
from it.

1. Stop the agent so that a reboot cannot start it with the file missing,
   and keep the damaged file:

   ```console
   $ D=/var/snap/fleetwire-agent/current/state
   $ sudo snap stop --disable fleetwire-agent.daemon
   $ sudo mv $D/wal/state.json $D/wal/state.json.bad
   $ sudo cat $D/identity/device.json
   {"device_id":"01a1c33d-1f19-7ee1-95fb-7f9e10839acd","broker_url":"...","api_url":"..."}
   ```

2. On a machine with `fwctl` and a tenant token with `devices:read`, read
   the device's watermark. Every seq up to it is stored, or recorded as a
   gap, on the server:

   ```console
   $ fwctl -o json device show 01a1c33d-1f19-7ee1-95fb-7f9e10839acd | jq .acked_seq
   41871
   ```

3. On the device, write the new state with the watermark as both the ack
   and the highest seq, give the file to `snap_daemon`, the user the
   daemon runs as, then start the agent:

   ```console
   $ W=41871
   $ printf '{"version":1,"acked":%d,"max_seq_ever":%d,"pending_gaps":[]}\n' $W $W |
       sudo tee $D/wal/state.json >/dev/null
   $ sudo chown snap_daemon:snap_daemon $D/wal/state.json
   $ sudo chmod 0600 $D/wal/state.json
   $ sudo snap start --enable fleetwire-agent.daemon
   ```

   Leave out `device_id`; the agent fills it in from `identity/`. A file
   that root owns with mode 0600 stops the daemon with `permission
   denied`.

The agent now hands out seqs above both the watermark and the last record
on disk, so no seq is reused. Any seq it sent above the watermark is
still on disk, because it deletes a segment only once a saved ack covers
it; it resends those, and ingest drops the ones it already has. Seqs above
the watermark that are no longer on disk, such as readings the disk budget
dropped (the old file listed them as `DISK_BUDGET`), go to the server as a
`LOST` gap, and the server records the part it has not stored. Watch
`fleetwire-agent status` until `acked` reaches `last_seq`.

If you cannot reach the API, leave the agent stopped. The alternative is
to [enroll it again](enroll-a-device.md#re-enrolling) as a new device,
which loses the readings it had not sent.
