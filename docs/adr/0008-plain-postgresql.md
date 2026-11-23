# ADR-0008: Plain PostgreSQL instead of TimescaleDB

- Status: accepted
- Date: 2026-11-09

## Context

Readings are time series, which usually suggests a time-series database.
But every metric name, label and value is encrypted with the tenant's key
(ADR-0007), so the database sees opaque bytes plus a few plain columns:
device, seq range, first and last collect time, reading count.

## Decision

Use plain PostgreSQL 16. Store one row per stored seq range ("chunk", up to
one uplink batch: at most `uplink.batch-size` readings, 100 by default,
and one collection when the device is caught up). Index chunks by
`(tenant_id, device_id, max_collected_at)` for queries and
`(tenant_id, max_collected_at)` for retention. The API selects the chunks
that overlap a time window, decrypts them, and filters by time and metric
in Go.

## Alternatives considered

- **TimescaleDB hypertables.** Their value is in-database aggregation,
  compression of numeric columns and fast time-bucket queries; none of that
  works on ciphertext. It would add an extension dependency, which charmed
  and managed PostgreSQL do not always provide.
- **One row per reading.** Ten times more rows, indexes and WAL for the
  same data, and per-row encryption overhead.
- **Encrypt only values, keep metric names in clear.** Faster filtering by
  metric, but metric names and labels can reveal what a tenant measures.

## Consequences

- About one row per batch. A caught-up device sends each collection as
  its own batch, because `uplink.batch-wait` (1 s) is shorter than the
  collect interval. So 10,000 devices at one reading every 10 seconds
  write about 1,000 rows per second, one reading each, and a real agent's
  collection (11 readings from the built-in collectors on a machine
  without thermal zones) goes into one row. (Corrected on 2026-11-22: this
  said 10 rows per second.) The throughput search of 2026-11-10 measured
  both shapes: ingest kept up with 1,000 rows/s of one reading each, and
  with 500 rows/s of 64 readings each, 32,000 readings/s
  ([table](../results/2026-11-10T044759Z-add94424b241-throughput.md)).
  The search of 2026-11-23, on a 4 vCPU EC2 host, kept up with 2,000 rows/s
  of one reading each, its highest step
  ([table](../results/2026-11-23T185446Z-8ffb129828b1-throughput.md)).
- Queries filtered by metric decrypt whole chunks; a narrow query over a
  long window costs more CPU than an indexed lookup would.
- Aggregations (averages, downsampling) happen in the client or a future
  service that holds the tenant key, never in SQL.
