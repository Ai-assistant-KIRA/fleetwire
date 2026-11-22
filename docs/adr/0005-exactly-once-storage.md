# ADR-0005: Exactly-once storage with coverage ranges, a row lock per device, and an exclusion constraint

- Status: accepted
- Date: 2026-11-09

## Context

Delivery from device to database is at least once: the device resends until
an end-to-end ack arrives, MQTT QoS 1 and Kafka may repeat messages, and two
bridges may forward one device's messages in any order (R6.8). Storage must
still hold each seq exactly once (R6.3), and the ack must say exactly how far
the stored data is complete (R6.5).

## Decision

- Per device, the database keeps `devices.acked_through` (every seq at or
  below it is stored or a reported gap) and `device_ranges`, the merged ranges
  above it that are already covered. `device_ranges` is normally empty; it
  only has rows while batches arrive out of order.
- Ingest computes, for each batch, the parts not yet covered
  (`coverage.Uncovered`), stores only those, records the uncovered parts of
  reported gaps, and moves the watermark over every range that now touches it
  (`coverage.Advance`). The interval logic is pure Go in `internal/coverage`,
  shared with the checker and tested with property tests against a bitmap
  model (P1, P2) and a fuzz test.
- The device row is locked `FOR UPDATE` at the start of the transaction, so
  two ingest instances working on repeats of the same batch serialize.
- `chunks` has `EXCLUDE USING gist (device_id WITH =, int8range(seq_lo, seq_hi, '[]') WITH &&)`,
  so overlapping chunks are impossible even if the application has a bug.
- Seqs are `bigint`, and PostgreSQL stores `int8range(lo, hi, '[]')` as
  `[lo, hi+1)`, so the highest seq it can store is 2^63-2. Validation
  refuses a higher seq before the batch reaches the database, and the batch
  goes to the dead-letter topic with reason `seq_overflow`.
- A seq is covered exactly once in total: if a gap report arrives first, a
  later batch with the same seq is not stored; if the batch arrives first,
  only the rest of the gap is recorded.
- **Change from the first design draft:** `device_ranges` does not keep a
  `kind` column. The permanent records are `chunks` (stored) and
  `device_gaps` (gaps); `device_ranges` is only a cache of their union above
  the watermark, and merging across kinds keeps it at one row per hole.
- One batch is one transaction, sent as two pipelines (`store.DB.IngestBatch`):
  the reads with `BEGIN`, then the writes with `COMMIT`. Sent one statement
  at a time it took eight round trips, during which the row lock was held.
- **Change after the 10,000-device runs:** `chunks.key_id` has no foreign
  key to `tenant_keys` (migration 00004). The check locked the tenant's one
  active key row `FOR KEY SHARE` in every ingest transaction, so all of a
  tenant's concurrent transactions shared that row's lock through
  multixacts. With 10,000 devices of one tenant on the test VM, ingest
  stored 505 readings/s with the constraint and 787/s without it, 64
  workers each. The key still exists for every chunk: ingest seals only
  with keys it read from `tenant_keys`, and key rows are never deleted.

## Alternatives considered

- **Kafka exactly-once transactions.** They cover Kafka to Kafka, not the
  device to PostgreSQL path, and do not help with device resends.
- **`ON CONFLICT DO NOTHING` per reading.** Works, but needs one row per
  reading and cannot compute a watermark without scanning.
- **Order by seq in a single consumer per device.** Order is not guaranteed
  with two bridges and Kafka retries; requiring it would make correctness
  depend on the transport.

## Consequences

- Repeats cost one cheap transaction and no storage.
- The per-device lock serializes a device's batches; different devices run in
  parallel.
- Readings that reach the server after the device declared their seqs a gap
  are not stored. The device made that decision, and the checker counts those
  seqs as gaps, not missing.
- Without the foreign key, the database no longer refuses a chunk whose key
  row is missing. Only a bug could write one; it would fail to decrypt, and
  the checker and the API report that. The composite key to
  `devices (tenant_id, id)` stays: the same transaction already holds that
  row's update lock, so it adds no contention.
