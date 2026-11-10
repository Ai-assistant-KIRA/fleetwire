# Metrics

No metric has a per-device label: 10,000 devices would explode cardinality.
Per-device state is in the API (`GET /v1/devices/{id}`).

## Agent (`fleetwire_agent_*`, also exported by the load generator)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `readings_collected_total` | counter | collector | readings written to the local log |
| `collector_errors_total` | counter | collector | failures, timeouts, invalid readings |
| `readings_dropped_total` | counter | reason | readings dropped by the disk budget (reported as gaps) |
| `wal_append_errors_total` | counter | | readings that could not be written |
| `wal_bytes`, `wal_segments` | gauge | | size of the local log |
| `readings_sent_total`, `batches_sent_total` | counter | | published, resends included |
| `publish_errors_total` | counter | | publishes that failed or timed out |
| `readings_acked_total` | counter | | seqs newly covered by end-to-end acks |
| `unacked_readings` | gauge | | written but not yet acked |
| `resend_total` | counter | | go-back-N restarts after an ack timeout |
| `seq_jump_total` | counter | | server ack ahead of the device (R3.8) |
| `cert_renewals_total` | counter | result | certificate renewals |
| `mqtt_connects_total`, `mqtt_connect_errors_total`, `mqtt_disconnects_total`, `mqtt_connected` | | | MQTT connection state |

## Bridge (`fleetwire_bridge_*`)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `forwarded_total` | counter | result (`ok`, `error`, `rejected`) | messages handled |
| `pending_acks` | gauge | | received, not yet confirmed by Kafka |
| `produce_seconds` | histogram | | MQTT receive to Kafka confirmation |
| `last_produce_timestamp_seconds` | gauge | | last confirmed Kafka write |

## Ingest (`fleetwire_ingest_*`)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `batches_total` | counter | result (`stored`, `duplicate`, `dropped_revoked`, `dropped_unknown`, `dlq`) | batches processed |
| `readings_stored_total` | counter | | readings stored for the first time |
| `gap_seqs_total` | counter | | seqs recorded as device-reported gaps |
| `e2e_delay_seconds` | histogram (0.1 to 60 s, fine between 1 and 5 s) | | collection (device clock) to commit |
| `tx_seconds` | histogram | | one batch transaction |
| `consumer_lag` | gauge | partition | records between the committed offset and the log end, for the partitions this instance owns (every 10 s) |
| `db_retries_total` | counter | | batch transactions retried |
| `exclusion_violations_total` | counter | | batches whose chunk or gap overlapped stored data; must stay 0 |
| `clock_skew_seconds` | histogram | | abs(bridge receive time - device send time) |
| `negative_delay_total` | counter | | readings timestamped in the future |
| `acks_published_total`, `ack_publish_errors_total`, `pending_acks` | | | end-to-end ack publishing |

## API (`fleetwire_api_*`, `fleetwire_jobs_*`, `fleetwire_crl_*`)

| Metric | Type | Labels |
|---|---|---|
| `fleetwire_api_requests_total` | counter | listener, route, code |
| `fleetwire_api_request_seconds` | histogram | listener, route |
| `fleetwire_jobs_total` | counter | kind, result |
| `fleetwire_jobs_running` | gauge | |
| `fleetwire_crl_age_seconds` | gauge | |

Every binary also exports Go runtime and process metrics and
`fleetwire_build_info{service,version,commit}`.
