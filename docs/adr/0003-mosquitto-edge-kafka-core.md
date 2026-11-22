# ADR-0003: Mosquitto at the edge, Kafka in the core

- Status: accepted
- Date: 2026-11-09

## Context

Devices need a protocol that is cheap on small hardware, keeps a session
across flaky links, and authenticates each device with a client certificate.
The backend needs a durable, replayable log that several ingest instances
can share, that absorbs bursts, and that keeps data while the database is
down.

## Decision

- Devices speak MQTT v5 over mutual TLS to Mosquitto. The ACL pins each
  device to its own uplink and ack topics.
- A bridge moves messages into Kafka (`fleetwire.uplink.v1`, 24 partitions,
  replication 3, `min.insync.replicas` 2), keyed by device ID, acking MQTT
  only after Kafka confirms (`acks=all`, idempotent producer).
- Bridges share one shared subscription (`$share/fleetwire-bridge/...`) with
  persistent sessions, so any number can run and messages wait in Mosquitto
  while all are down.
- Ingest instances form one Kafka consumer group and commit offsets only
  after the database commit.
- In the Compose stack: Mosquitto 2.1, Kafka 4.1 in KRaft mode with three
  combined broker/controller nodes, two bridges, two ingest instances.

## Alternatives considered

- **Devices write to Kafka directly** (REST proxy or Kafka clients). Kafka
  clients are heavy for devices, and per-device certificates and topic ACLs
  are awkward at fleet scale.
- **MQTT only, ingest subscribes to Mosquitto.** No replay, no buffer while
  the database is down beyond Mosquitto's in-memory queues, and scaling
  ingest means more shared-subscription members, each holding unacked
  messages.
- **Mosquitto's own Kafka bridge plugin.** Not part of Mosquitto; the
  ack-after-Kafka ordering is the property that matters and is easier to
  check in code we own.

## Consequences

- Two systems to operate. In production they come from the Kafka charm and
  the Mosquitto charm (ADR-0012).
- A bridge share that is offline keeps its messages queued in Mosquitto
  until it returns (see [third-party facts](../reference/third-party-facts.md));
  the chaos runner restarts killed bridges, and the per-client queue limit
  is raised.
- Message order is not guaranteed end to end with two bridges; ingest does
  not need it (ADR-0005).
