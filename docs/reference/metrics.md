# Metrics

No metric has a per-device label: 10,000 devices would explode cardinality.
Per-device state is in the API (`GET /v1/devices/{id}`).

## Agent (`fleetwire_agent_*`, also exported by the load generator)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `readings_collected_total` | counter | collector | readings written to the local log |
| `collector_errors_total` | counter | collector | failures, timeouts, invalid readings |
| `readings_dropped_total` | counter | reason (`disk_budget`) | readings dropped by the disk budget (reported as gaps) |
| `wal_append_errors_total` | counter | | readings that could not be written |
| `wal_bytes`, `wal_segments` | gauge | | size of the local log |
| `readings_sent_total`, `batches_sent_total` | counter | | published, resends included |
| `publish_errors_total` | counter | | publishes that failed or timed out |
| `readings_acked_total` | counter | | seqs newly covered by end-to-end acks |
| `unacked_readings` | gauge | | written but not yet acked |
| `resend_total` | counter | | go-back-N restarts after an ack timeout |
| `seq_jump_total` | counter | | server ack ahead of the device (R3.8) |
| `cert_renewals_total` | counter | result (`ok`, `error`) | certificate renewal attempts |
| `mqtt_*` | | | the connection to the broker: see [MQTT connections](#mqtt-connections) |

## Bridge (`fleetwire_bridge_*`)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `forwarded_total` | counter | result (`ok`, `error`, `rejected`) | messages handled |
| `pending_acks` | gauge | | received, not yet confirmed by Kafka |
| `produce_seconds` | histogram | | MQTT receive to Kafka confirmation |
| `last_produce_timestamp_seconds` | gauge | | last confirmed Kafka write |
| `mqtt_*` | | | the shared subscription's connection: see [MQTT connections](#mqtt-connections) |

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
| `acks_published_total` | counter | | end-to-end acks published |
| `ack_publish_errors_total` | counter | | ack publishes that failed (they are retried) |
| `pending_acks` | gauge | | devices with an ack not yet published |
| `mqtt_*` | | | the ack publisher's connection: see [MQTT connections](#mqtt-connections) |

## API (`fleetwire_api_*`, `fleetwire_jobs_*`, `fleetwire_crl_*`)

| Metric | Type | Labels |
|---|---|---|
| `fleetwire_api_requests_total` | counter | listener, route, code |
| `fleetwire_api_request_seconds` | histogram | listener, route |
| `fleetwire_jobs_total` | counter | kind (`retention`, `export`, `device_purge`, `tenant_destroy`), result (`success`, `error`) |
| `fleetwire_jobs_running` | gauge | |
| `fleetwire_crl_age_seconds` | gauge | |

## MQTT connections

The agent, the bridge and ingest's ack publisher export the same four
metrics for their connection to Mosquitto, each under its own prefix:
`fleetwire_agent_mqtt_*`, `fleetwire_bridge_mqtt_*` and
`fleetwire_ingest_mqtt_*`. In the load generator they add up over all its
devices.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `mqtt_connects_total` | counter | | successful connections |
| `mqtt_connect_errors_total` | counter | | failed connection attempts |
| `mqtt_disconnects_total` | counter | | connections lost |
| `mqtt_connected` | gauge | | connections up now: 0 or 1 per process, one per connected device in the load generator |

## Load generator (`fleetwire_loadgen_*`)

Served with `fleetwire-loadgen -metrics-listen`, next to the agent and
`fleetwire_agent_mqtt_*` metrics of its devices.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `devices_running` | gauge | | device instances running now |
| `devices_started_total` | counter | | device instances started |
| `device_failures_total` | counter | | device instances that did not start, or stopped with an error |

Every binary also exports Go runtime and process metrics and
`fleetwire_build_info{service,version,commit}`.
