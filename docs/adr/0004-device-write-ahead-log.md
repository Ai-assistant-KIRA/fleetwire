# ADR-0004: Device write-ahead log, durability modes, send only durable records

- Status: accepted
- Date: 2026-11-09

## Context

A device must not lose readings when the network, the backend, or the power
fails (R2). It also must never give two different readings the same sequence
number once either has left the device (R1.6), because ingest drops a seq it
already has as a duplicate. The agent runs on small Ubuntu Core devices with
flash storage, where every fsync costs wear and latency.

## Decision

- Each reading is appended to a segmented log in `$SNAP_DATA/wal/` before it
  can be sent. Segments hold records `length | crc32c | seq | payload`; the
  CRC covers the seq and the payload; seqs inside a segment are consecutive.
- Three sync modes: `always` (fsync after each append), `interval` (fsync
  every N ms, default 1000), `never` (page cache only). The uplink reads only
  records at or below the **durable** seq. In `always` and `interval` modes a
  reading lost in a power cut therefore never left the device, so its seq can
  be handed out again without conflict.
- Agent state (`acked`, `max_seq_ever`, pending gaps) lives in `state.json`,
  replaced by write-temp, fsync, rename, fsync-dir.
- A segment is deleted only when the **persisted** ack covers all of it
  (invariant W1). The disk budget drops the oldest segment and records the
  unacked part as a gap, persisted before the delete.
- On start, recovery truncates a torn tail in the last segment, quarantines
  damage in a middle segment as `*.corrupt` and reports a `CORRUPTION` gap, and
  computes the next seq as one past the highest seq known from the segments,
  the ack, `max_seq_ever` and the gaps.
- **Change from the first design draft:** any seq at or below that highest seq
  which is neither on disk, acked, nor in a gap is reported as a new gap with
  reason `LOST`. Without this, a hole the device skipped silently would stop
  the server watermark forever and the device would resend until its disk
  budget dropped data. `max_seq_ever` tracks the highest *durable* seq, so a
  normal power cut never produces a `LOST` gap; it only appears when files
  were removed or damaged behind the agent's back.
- Records are written with one `write` per record rather than through a
  buffered writer. The load generator runs thousands of logs in one process,
  and a 64 KiB buffer per log would cost hundreds of megabytes.

## Alternatives considered

- **SQLite or BoltDB on the device.** Both work, but bring a second storage
  engine with its own durability rules into a snap that should stay small, and
  hide exactly what is fsynced when. The log needs only append, read from seq,
  and delete-oldest.
- **Send first, persist on failure.** Loses whatever was in memory at a crash
  or power cut.
- **fsync on every append only.** Simple, but costly on flash for a 10 s
  collect interval with many collectors; `interval` mode bounds the loss to the
  last second of readings that never left the device.

## Consequences

- Correctness under power loss holds for `always` and `interval`, not for
  `never`; the docs state this.
- Recovery reads the whole log at start (at most the 256 MiB budget).
- A corrupt `state.json` stops the agent with a clear error instead of
  guessing, because starting from zero state could reuse seqs before the
  server's watermark is known. This is an operator action:
  [Recover a device's local log](../how-to/recover-device-state.md).
- Tests: torn tail at every byte (P3), bit flips in middle segments, budget
  gaps, deletion bounded by the persisted ack (P4), and a property test that
  mixes appends, sends, acks, checkpoints, power cuts and restarts and checks
  that no sent seq is ever reused or lost without a gap (P5).
