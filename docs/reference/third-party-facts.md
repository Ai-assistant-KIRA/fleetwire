# Third-party facts the design depends on

Each item was checked before the code that relies on it was written. The
answer, the versions, and how it was checked are recorded so a reviewer can
repeat the check. Checked on 2026-11-09 on the Ubuntu build VM.

## 1. Does Mosquitto reload `crlfile` on SIGHUP?

**Yes** (Mosquitto 2.1.2, `eclipse-mosquitto:2` image).

Experiment: broker with `require_certificate true` and `crlfile`; a client
certificate was accepted; the certificate was revoked and the CRL file
replaced; the client was still accepted (no reload yet); after
`docker kill -s HUP` the broker logged `Reloading config.` and refused the
client; after a restart it still refused it.

Consequences:

- Broker-level revocation takes effect at the next SIGHUP. In Compose a small
  watcher in the Mosquitto container sends SIGHUP when `crl.pem` changes; the
  Kubernetes charm does the same through Pebble.
- A reload does not drop connections that are already open. A revoked device
  that stays connected keeps its session until it reconnects. Ingest drops its
  data from the moment of revocation (layer 1 in ADR-0010), so no data from it
  is stored either way.
- OpenSSL rejects every client once the CRL's `nextUpdate` has passed, so the
  API republishes the CRL daily and an alert fires when it is older than 48 h.
- OpenSSL checks the CRL of the leaf certificate's issuer only. Devices and
  services therefore use **separate listeners**: the device listener trusts
  the device CA and checks the CRL; the service listener trusts the service CA
  and has no CRL.

## 2. Shared subscriptions and offline persistent sessions

Mosquitto 2.1.2. Two subscribers, `b1` and `b2`, each with clean start false
and session expiry 3600 s, subscribed to `$share/g/fw/v1/d/+/up` at QoS 1.

| Situation | Result |
|---|---|
| `b1` offline, `b2` online, 20 messages published | `b2` received 10; `b1` received the other 10 when it reconnected |
| Both offline, 20 messages, broker restarted (persistence on) | `b1` got 10, `b2` got 10 after reconnecting |

So Mosquitto keeps distributing round-robin to **offline** group members and
queues their share in their session. Nothing is lost, but a killed bridge's
share waits until that bridge comes back. The chaos runner restarts a killed
bridge, and the per-client queue limit is raised (`max_queued_messages`,
`max_queued_bytes`) so an outage of a few minutes fits. If a queue overflows,
Mosquitto drops QoS 1 messages for that client; the device still has them in
its log and resends after the ack timeout, so the end-to-end guarantee holds.

## 3. paho.golang manual acknowledgement

`github.com/eclipse/paho.golang` v0.23.0, read from the module source.

- `paho.ClientConfig.EnableManualAcknowledgment` turns it on;
  `(*paho.Client).Ack(*paho.Publish)` marks a message as handled. With
  autopaho, the client is in `paho.PublishReceived.Client`.
- **Order is enforced by the library.** `acksTracker` records publishes in
  arrival order, and a ticker (`SendAcksInterval`, default 50 ms) sends
  PUBACKs only for the leading run of handled messages. A message whose Kafka
  write has not been confirmed blocks every PUBACK after it, which is exactly
  the backpressure the bridge needs. The bridge therefore does not keep its
  own FIFO; it calls `Ack` from the Kafka produce callback.
- The tracker is reset on reconnect. A produce callback that fires after a
  reconnect gets `ErrPacketNotFound`; the broker redelivers that message, it
  is produced again, and ingest removes the duplicate.
- Nothing else resets it, so a message that is never acked holds back every
  later PUBACK for as long as the connection lasts. franz-go v1.22.2 retries
  produce errors itself without limit and fails a record only for errors it
  cannot retry, such as a topic still unknown after four metadata attempts
  (`UnknownTopicRetries`) or a denied authorization. The bridge therefore
  writes such a message again with backoff until Kafka takes it. It stays in
  `fleetwire_bridge_pending_acks`, and readiness fails while any message is
  being written again. If the connection drops meanwhile, the bridge stops
  writing that copy, because the broker redelivers it on the next
  connection.
- `markAsAcked` scans the tracker linearly, so the bridge caps the number of
  unacknowledged messages with the MQTT v5 Receive Maximum (1,000).

## 4. `snapctl get -d` and `snapctl set-health` from a strict daemon

From the snapd source (`overlord/hookstate/ctlcmd/health.go`): `set-health`
"can be called from any hook, and even from the apps themselves". Rules: the
message must be 7 to 70 characters (longer is cut), a code must be 3 to 30
characters, and `okay` takes no message. `snapctl get -d` prints the snap's
configuration as JSON and works from apps; it is checked again on a real
core24 install in task 7.1.

## 5. Ubuntu Core 24 in QEMU

- Image: `https://cdimage.ubuntu.com/ubuntu-core/24/stable/current/ubuntu-core-24-amd64.img.xz`,
  booted with OVMF (`OVMF_CODE_4M.secboot.fd`).
- First boot runs console-conf, which needs an **Ubuntu One account with an
  SSH key** to create the first user. The scripts take the account e-mail as
  input; this step cannot be automated without such an account or a custom
  signed model.
- The build VM (UpCloud, AMD EPYC under KVM) does **not** expose `/dev/kvm`,
  so QEMU runs with TCG software emulation. Boot is slow; the power-off test
  records the time it takes.

## 6. Charmed Kafka and PostgreSQL on Kubernetes

From the Charmhub API and Canonical's docs:

- `kafka-k8s`: channels `4/stable` (Apache Kafka 4, KRaft; the charm runs
  `roles=broker` and/or `roles=controller`) and `3/stable` (ZooKeeper). The
  bundle uses `4/stable`. Clients use the `kafka_client` interface.
- `postgresql-k8s`: `16/stable`. Clients use the `postgresql_client`
  interface (`DatabaseRequires` from `data_platform_libs`).
- Integration tests: Canonical moved from pytest-operator to **Jubilant**
  (with `pytest-jubilant`); the Kafka K8s charm itself has migrated. The
  charms here use Jubilant.

## 7. 10,000 mTLS MQTT connections from one process

Measured in task 8.4 with the load generator and recorded in
`docs/results/`. Settings in place before the run: `nofile` 1,048,576 for
all users, `fs.nr_open` 1,048,576, local port range 10240-65535, and
Mosquitto `max_connections -1` with a matching container `ulimit`.
