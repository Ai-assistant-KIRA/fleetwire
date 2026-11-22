# The delivery guarantee

> For every device, every seq that left the device is, after the system
> recovers, either stored exactly once, or covered by a gap that the device
> itself reported. No seq is stored twice. No seq disappears without a
> report.

A seq *leaves the device* when it is put in an MQTT publish. The agent only
publishes readings that are durable on its disk (in the `always` and
`interval` sync modes), so a reading lost in a power cut never left the
device, and its seq is free to be used again.

## How it holds

The proof has three parts.

1. **The device never forgets a sent seq until the end-to-end ack covers
   it.** Readings stay in the local log until the ack says "every seq up to
   N is stored or a reported gap". Segments are deleted only below the
   *persisted* ack (`internal/wal`), so a crash between
   receiving an ack and saving it only causes a resend.

2. **Everything between the device and the database may lose or repeat.**
   MQTT QoS 1, the bridge, Kafka and the ingest consumer group are all
   at-least-once. The device covers losses by resending after an ack
   timeout or a reconnect (go-back-N from the acked seq).

3. **Ingest removes repeats at the only place it can be done safely.** For
   one batch, ingest locks the device row, computes the seqs not yet
   covered (`coverage.Uncovered`), stores only those, and moves the
   watermark (`coverage.Advance`). The lock serializes concurrent ingest
   instances; the order of batches does not matter; and a PostgreSQL
   exclusion constraint makes two overlapping chunks impossible.

The device also guards its own seq counter: on start, the next seq is one
past the highest seq it can prove was handed out (the log, the persisted
ack, `max_seq_ever`, the pending gaps), and any hole in that range is
reported as a `LOST` gap instead of being reused. When the server's ack is
ahead of the device (a restored disk), the device jumps past it.

## What is not promised

- In `wal.sync=never` mode, a power cut can lose readings that were already
  sent but are no longer on disk; the guarantee covers process crashes only.
- A device that is offline long enough to fill its disk budget drops its
  oldest readings and reports them as a `DISK_BUDGET` gap. The data is lost,
  but not silently: the gap is stored and visible in the API.
- A device whose certificate expires while offline must be enrolled again,
  as a new device; its unsent readings are not carried over.

## How it is checked

- Property tests: the coverage logic against a bitmap model, the local log
  with simulated power cuts, encryption with every sealed field tampered,
  and tenant isolation in SQL and through the API.
- An ingest test applies random batches and gaps, each up to three times, in
  random order, from four concurrent workers, and reads the decrypted
  database back.
- The failure suite runs the whole stack under nine faults and passes only
  with 0 missing and 0 duplicates (see `docs/results/`).
