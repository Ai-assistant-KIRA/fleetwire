# ADR-0013: Run the snap's daemon as snap_daemon, with its state in $SNAP_DATA/state

- Status: accepted
- Date: 2026-11-23

## Context

snapd starts every snap daemon as root. Up to v0.1.1 the agent stayed
root: a flaw in its TLS, MQTT or protobuf handling would have run with
root's rights inside the sandbox, and every file it wrote belonged to root.

snapd offers one unprivileged user for daemons, `snap_daemon` (uid and gid
584788 on every system), through `system-usernames`. The snap still starts
as root and must drop to the user itself; snapd's documentation shows
`setpriv --clear-groups --reuid snap_daemon --regid snap_daemon`. Checked
on snapd 2.78 and in its policy sources:

- With `system-usernames`, the AppArmor profile adds `capability setuid`,
  `setgid` and `chown`, and the seccomp filter allows `setuid`, `setresuid`,
  `setgroups(0, ...)` and `chown` only to that user and to root. Root in the
  snap never has `CAP_DAC_OVERRIDE`, `CAP_DAC_READ_SEARCH` or `CAP_FOWNER`:
  it cannot enter a directory of mode 0700 that `snap_daemon` owns, and
  cannot change the mode of a file it does not own.
- The profile allows writes to `/var/snap/<snap>/<revision>/**`, not to the
  directory itself, so chown of `$SNAP_DATA` is denied even to root. The
  first attempt failed on exactly that, because the agent kept `identity/`
  and `wal/` directly in `$SNAP_DATA`.
- The install hook and the post-refresh hook run before snapd starts the
  services; the configure hook runs after. A failing post-refresh hook rolls
  the refresh back. `snap revert` skips the data copy and runs neither the
  pre-refresh nor the post-refresh hook (`overlord/snapstate/snap.go`).
- Non-root processes may run `snapctl get`, `snapctl services` and
  `snapctl set-health`, which is all the daemon uses.

## Decision

- The snap declares `system-usernames: {snap_daemon: shared}`.
- The agent's identity and local log move to `$SNAP_DATA/state`, mode 0700,
  owned by `snap_daemon`. Outside the snap, `-data-dir` is unchanged.
- `fleetwire-agent prepare-state`, run as root, creates that directory or
  moves `identity/` and `wal/` of v0.1.1 into it (`internal/agent/statedir`).
  It assembles the directory as `state.partial` with renames, changes the
  owner of every entry before the directory that holds it and fsyncs each
  change, and renames the result to `state`. A crash at any step leaves each
  file in its old or its new place, and the next run finishes the move. A
  `state` next to the old entries is refused, not merged.
- The install and post-refresh hooks run `prepare-state`, so a failed move
  rolls the refresh back to the old revision, whose copy of `$SNAP_DATA` is
  untouched. The daemon's command chain runs it again before each start:
  after a revert, a snapshot restore or a deleted state directory no hook
  runs.
- The daemon's command chain then drops to `snap_daemon` with
  `fleetwire-agent drop-privileges`, which calls `setgroups(0, NULL)`,
  `setresgid` and `setresuid` with every id set to 584788, checks the
  result, and execs the daemon. The `fleetwire-agent` command uses the
  same chain when run with sudo, so `status` can read the files and no
  command leaves a root-owned file the daemon cannot open. The daemon
  refuses to run as root in the snap.

## Alternatives considered

- **Stay root.** Simplest, and what v0.1.1 did; it gives every parsing bug
  root's rights.
- **`setpriv` from the core24 base**, the path snapd's documentation shows.
  It worked on snapd 2.78, but snapd's default AppArmor template allows a
  snap to execute `/usr/bin/setpriv` only from 2.68 on, and Ubuntu Core 24
  ships snapd 2.63: on a real Ubuntu Core 24 device the daemon failed with
  `exec: setpriv: Permission denied` and restarted every 5 s.
  `drop-privileges` makes the same calls in a process of its own that
  execs the daemon, so the daemon's code still never runs as root.
- **Drop privileges in the daemon's own process** (`syscall.Setuid` after
  the move). It works with the same seccomp rules, but the move, the Go
  runtime's start-up and the daemon would share one process that starts
  as root.
- **Keep the state in `$SNAP_COMMON`.** snapd does not version it, so a
  revert would not rewind the log. But v0.1.1 reads `$SNAP_DATA`, a later
  refresh from a reverted v0.1.1 would find two states to reconcile, and
  an older revision would have to read whatever format a newer one wrote.
- **Move the files in the daemon only, without hooks.** One place instead
  of three, but a failed move would leave the new revision restarting
  every 5 s instead of rolling the refresh back.
- **The `_daemon_` user** (uid 584792), which snapd 2.61 and later offer in
  place of `snap_daemon`. It needs snapd 2.61 on every device; switching
  later means another ownership change.

## Consequences

- The daemon has no capabilities and owns only its state directory. A
  `metrics.listen` port below 1024 no longer works.
- Without sudo, `fleetwire-agent status` still works through the daemon's
  endpoint, but cannot read the files when the daemon is stopped.
- An administrator who writes a file into `state/` as root must give it to
  `snap_daemon`, as the recovery how-to shows.
- A revert to v0.1.1 runs on that revision's copy of `$SNAP_DATA` from the
  time of the refresh. Readings collected since stay in the newer
  revision's directory, and seqs the server already stored are handed out
  again until its ack moves the seq forward; ingest drops those readings as
  duplicates. [Run on Ubuntu Core](../how-to/run-on-ubuntu-core.md#revert-to-v011)
  gives a revert procedure that loses nothing. The same holds for any
  revert between two later revisions.
