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
one uplink batch, normally about 100 readings). Index chunks by
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

- Row counts stay low (about one per batch), so 10,000 devices at one
  reading every 10 seconds is roughly 10 rows per second.
- Queries filtered by metric decrypt whole chunks; a narrow query over a
  long window costs more CPU than an indexed lookup would.
- Aggregations (averages, downsampling) happen in the client or a future
  service that holds the tenant key, never in SQL.
