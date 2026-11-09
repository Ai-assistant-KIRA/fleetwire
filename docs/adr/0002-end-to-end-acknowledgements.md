# ADR-0002: End-to-end acknowledgements; no single hop is trusted

- Status: accepted
- Date: 2026-11-09

## Context

A reading crosses five hops between collection and storage: the device's
local log, MQTT to Mosquitto, the bridge, Kafka, and the ingest transaction
in PostgreSQL. Each hop has its own acknowledgement (MQTT PUBACK, Kafka
`acks=all`, a database commit), and each can lose or repeat a message during
a failure: a broker restart, a killed process between "done" and "ack", a
network cut after the write but before the reply. The promise is that every
reading that left a device is stored exactly once (R6, R12).

## Decision

- The device keeps every reading in its local log until an **end-to-end
  ack** says it is stored. The ack is published by ingest after the
  database commit, on `fw/v1/d/{id}/ack`, retained, and carries one number:
  every seq up to N is stored or a gap the device reported.
- Every hop in between is at-least-once and may repeat. The device resends
  after an ack timeout or a reconnect (go-back-N from the acked seq).
- Repeats are removed only at the end, by ingest's coverage check under a
  per-device row lock and by an exclusion constraint (ADR-0005).
- A PUBACK from Mosquitto changes nothing on the device: it only means the
  broker has the message.

This is the end-to-end argument (Saltzer, Reed and Clark): only the two ends
can make the guarantee, so the middle does not have to be perfect, only
eventually available.

## Alternatives considered

- **Trust MQTT QoS 2 and Kafka transactions.** Exactly-once inside each
  system does not compose across the bridge and the database, and QoS 2
  costs an extra round trip per message.
- **Ack at the broker (delete on PUBACK).** Loses everything Mosquitto
  accepted but could not forward before a crash.
- **Per-reading acks.** More messages for no extra safety; a watermark is
  enough because storage tracks ranges.

## Consequences

- Correctness does not depend on Mosquitto persistence, Kafka retention or
  bridge restarts; those only change how much is resent.
- The device needs disk for everything unacked; the disk budget turns
  overflow into reported gaps instead of silent loss (ADR-0004).
- A lost ack costs a resend of an already stored range, which ingest turns
  into a no-op and a fresh ack.
- The failure suite (R12) checks this claim by reading the stored data back.
