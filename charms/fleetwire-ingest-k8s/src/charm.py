#!/usr/bin/env python3
# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""Kubernetes charm for fleetwire-ingest.

Every event runs one reconcile step: read the PKI secret and the four
relations, create the topics on the leader, and write the Pebble layer.
collect-status reports what is still missing.

Ingest logs in to PostgreSQL with its own relation user, requested with the
extra user role charmed_dml: a member of the group the API's migrations give
the row-level security policies and grants to, and never the schema owner.
"""

from __future__ import annotations

import hashlib
import logging
import urllib.request
from dataclasses import dataclass
from urllib.parse import quote

import ops
from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires, KafkaRequires

logger = logging.getLogger(__name__)

CONTAINER = "ingest"
SERVICE = "fleetwire-ingest"
BINARY = "/usr/local/bin/service"
PKI = "/etc/fleetwire/pki"
WORKLOAD_UID = 65532
ADMIN_PORT = 9090
PEER = "ingest-peers"
DATABASE_NAME = "fleetwire"
RUNTIME_ROLE = "charmed_dml"
# One wildcard grant covers the uplink topic and its dead-letter topic; the
# producer role includes CREATE, which ensure-topics needs.
TOPICS = "fleetwire.*"
GROUP_PREFIX = "fleetwire-"
PKI_FILES = {"tls-cert": "ingest.pem", "tls-key": "ingest.key", "ca-bundle": "ca-bundle.pem"}
# Database connections beyond one per worker, for tenant key and device
# lookups in their own transactions and the readiness probe. 16 workers get
# 20, the binary's default.
SPARE_CONNS = 4


def readyz(port: int) -> bool:
    """Asks the workload's admin listener whether it is ready.

    The charm and the workload share the pod's network, so localhost reaches
    it. Pebble's own ready check starts out "up" before the first probe, so
    it cannot tell a service that just started from a ready one.
    """
    try:
        with urllib.request.urlopen(f"http://localhost:{port}/readyz", timeout=3) as resp:
            return resp.status == 200
    except OSError:
        return False


def files_digest(files: dict[str, str]) -> str:
    """SHA-256 over the paths and contents of the files written for the service.

    Ingest reads its certificates and CA files once, at start. The digest goes
    into the layer's environment, so a rotated certificate changes the layer
    and replan restarts the service with the new files.
    """
    h = hashlib.sha256()
    for path in sorted(files):
        data = files[path].encode()
        h.update(path.encode() + b"\0" + len(data).to_bytes(8, "big") + data)
    return h.hexdigest()


@dataclass(frozen=True)
class Kafka:
    """Connection details from the kafka_client relation."""

    endpoints: str
    username: str
    password: str
    tls_ca: str | None
    group_prefix: str


@dataclass(frozen=True)
class Database:
    """Connection details from the postgresql_client relation."""

    url: str
    tls_ca: str | None


class FleetwireIngestCharm(ops.CharmBase):
    """Runs fleetwire-ingest."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)
        self.kafka = KafkaRequires(
            self,
            relation_name="kafka",
            topic=TOPICS,
            extra_user_roles="consumer,producer",
            consumer_group_prefix=GROUP_PREFIX,
        )
        self.database = DatabaseRequires(
            self, relation_name="database", database_name=DATABASE_NAME, extra_user_roles=RUNTIME_ROLE
        )
        for event in (
            self.on.ingest_pebble_ready,
            self.on.config_changed,
            self.on.upgrade_charm,
            self.on.leader_elected,
            self.on.update_status,
            self.on.secret_changed,
            self.on[PEER].relation_changed,
            self.on.api_relation_changed,
            self.on.api_relation_broken,
            self.on.mqtt_relation_changed,
            self.on.mqtt_relation_broken,
            self.kafka.on.topic_created,
            self.kafka.on.bootstrap_server_changed,
            self.on.kafka_relation_broken,
            self.database.on.database_created,
            self.database.on.endpoints_changed,
            self.on.database_relation_broken,
        ):
            self.framework.observe(event, self._reconcile)
        self.framework.observe(self.on.collect_unit_status, self._on_collect_status)

    @property
    def _container(self) -> ops.Container:
        return self.unit.get_container(CONTAINER)

    # ---- inputs ------------------------------------------------------------

    def _pki(self) -> dict[str, str] | None:
        secret_id = str(self.config.get("pki-secret") or "")
        if not secret_id:
            return None
        try:
            return self.model.get_secret(id=secret_id).get_content(refresh=True)
        except (ops.SecretNotFoundError, ops.ModelError) as e:
            logger.warning("cannot read the PKI secret %s: %s", secret_id, e)
            return None

    def _kafka(self) -> Kafka | None:
        for data in self.kafka.fetch_relation_data().values():
            endpoints, user, password = data.get("endpoints"), data.get("username"), data.get("password")
            if endpoints and user and password:
                tls_ca = data.get("tls-ca") if data.get("tls") == "enabled" else None
                return Kafka(endpoints, user, password, tls_ca, data.get("consumer-group-prefix") or GROUP_PREFIX)
        return None

    def _database(self) -> Database | None:
        for data in self.database.fetch_relation_data().values():
            endpoints, user, password = data.get("endpoints"), data.get("username"), data.get("password")
            if not (endpoints and user and password):
                continue
            tls_ca = data.get("tls-ca") if str(data.get("tls", "")).lower() == "true" else None
            mode = f"sslmode=verify-full&sslrootcert={PKI}/postgresql-ca.pem" if tls_ca else "sslmode=prefer"
            host, db = endpoints.split(",")[0], data.get("database") or DATABASE_NAME
            return Database(f"postgres://{quote(user, safe='')}:{quote(password, safe='')}@{host}/{db}?{mode}", tls_ca)
        return None

    def _keyring_url(self) -> str | None:
        """The API's keyring listener, once the API has migrated the database."""
        rel = self.model.get_relation("api")
        if rel is None or rel.app is None:
            return None
        data = rel.data[rel.app]
        if data.get("schema-ready") != "true":
            return None
        return data.get("keyring-url") or None

    def _mqtt_url(self) -> str | None:
        rel = self.model.get_relation("mqtt")
        if rel is None or rel.app is None:
            return None
        return rel.data[rel.app].get("service-url") or None

    # ---- statuses ----------------------------------------------------------

    def _on_collect_status(self, event: ops.CollectStatusEvent) -> None:
        pki = self._pki()
        if not self.config.get("pki-secret"):
            event.add_status(ops.BlockedStatus("set pki-secret to the ID of the ingest certificate secret"))
        elif pki is None:
            event.add_status(
                ops.BlockedStatus(f"cannot read pki-secret: run juju grant-secret <secret> {self.app.name}")
            )
        elif missing := sorted(k for k in PKI_FILES if not pki.get(k)):
            event.add_status(ops.BlockedStatus("pki-secret lacks " + ", ".join(missing)))
        if (workers := int(self.config["workers"])) < 1:
            event.add_status(ops.BlockedStatus(f"workers must be at least 1, not {workers}"))
        for endpoint, other, ready, waiting in (
            ("database", "postgresql-k8s", self._database, "waiting for database relation data"),
            ("kafka", "kafka-k8s", self._kafka, "waiting for kafka relation data"),
            ("api", "fleetwire-api-k8s", self._keyring_url, "waiting for the API to migrate the database"),
            ("mqtt", "mosquitto-k8s", self._mqtt_url, "waiting for mqtt relation data"),
        ):
            if not self.model.get_relation(endpoint):
                event.add_status(
                    ops.BlockedStatus(f"integrate with {other}: juju integrate {self.app.name}:{endpoint} {other}")
                )
            elif ready() is None:
                event.add_status(ops.WaitingStatus(waiting))
        if not self.unit.is_leader() and (kafka := self._kafka()) is not None and not self._topics_ready(kafka):
            event.add_status(ops.WaitingStatus("waiting for the leader to create the Kafka topics"))
        if not self._container.can_connect():
            event.add_status(ops.WaitingStatus("waiting for the workload container"))
        elif self._service_ready():
            event.add_status(ops.ActiveStatus())
        else:
            event.add_status(ops.WaitingStatus("waiting for ingest to become ready"))

    def _service_ready(self) -> bool:
        services = self._container.get_services(SERVICE)
        return SERVICE in services and services[SERVICE].is_running() and readyz(ADMIN_PORT)

    # ---- reconcile ---------------------------------------------------------

    def _reconcile(self, event: ops.EventBase) -> None:
        self.unit.set_ports(ADMIN_PORT)
        if isinstance(event, ops.RelationBrokenEvent):
            # Without one of its relations ingest cannot store or ack data
            # correctly; it stops until the relation is back.
            if self._container.can_connect() and SERVICE in self._container.get_plan().services:
                self._container.stop(SERVICE)
            return
        pki, kafka, db = self._pki(), self._kafka(), self._database()
        keyring, mqtt = self._keyring_url(), self._mqtt_url()
        if pki is None or any(not pki.get(k) for k in PKI_FILES) or int(self.config["workers"]) < 1:
            return
        if kafka is None or db is None or keyring is None or mqtt is None or not self._container.can_connect():
            return
        digest = self._write_files(pki, kafka, db)
        env = self._environment(kafka, db, keyring, mqtt)
        if self.unit.is_leader():
            if not self._ensure_topics(kafka, env):
                return
        elif not self._topics_ready(kafka):
            # Only the leader creates the topics. A unit that started first
            # would join the consumer group before the topics exist.
            return
        # Not read by ingest: a new value makes replan restart it.
        env["FLEETWIRE_CONFIG_SHA256"] = digest
        self._container.add_layer(SERVICE, self._layer(env), combine=True)
        self._container.replan()

    def _write_files(self, pki: dict[str, str], kafka: Kafka, db: Database) -> str:
        """Writes the service's files and returns their digest."""
        files = {f"{PKI}/{name}": pki[key] for key, name in PKI_FILES.items()}
        if kafka.tls_ca:
            files[f"{PKI}/kafka-ca.pem"] = kafka.tls_ca
        if db.tls_ca:
            files[f"{PKI}/postgresql-ca.pem"] = db.tls_ca
        own = {"user_id": WORKLOAD_UID, "group_id": WORKLOAD_UID, "make_dirs": True, "permissions": 0o600}
        for path, content in files.items():
            self._container.push(path, content, **own)
        return files_digest(files)

    def _environment(self, kafka: Kafka, db: Database, keyring: str, mqtt: str) -> dict[str, str]:
        env = {
            "FLEETWIRE_DATABASE_URL": db.url,
            "FLEETWIRE_KAFKA_BROKERS": kafka.endpoints,
            "FLEETWIRE_KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
            "FLEETWIRE_KAFKA_USERNAME": kafka.username,
            "FLEETWIRE_KAFKA_PASSWORD": kafka.password,
            "FLEETWIRE_KAFKA_TLS": "true" if kafka.tls_ca else "false",
            # The consumer group must start with the prefix Kafka granted.
            "FLEETWIRE_KAFKA_GROUP": kafka.group_prefix + "ingest",
            "FLEETWIRE_KAFKA_REPLICATION": str(self.config["kafka-replication"]),
            "FLEETWIRE_INGEST_WORKERS": str(self.config["workers"]),
            # Each worker holds a connection for its transaction; a smaller
            # pool would leave workers waiting for one.
            "FLEETWIRE_DATABASE_MAX_CONNS": str(int(self.config["workers"]) + SPARE_CONNS),
            "FLEETWIRE_MQTT_URL": mqtt,
            "FLEETWIRE_MQTT_CERT_FILE": f"{PKI}/ingest.pem",
            "FLEETWIRE_MQTT_KEY_FILE": f"{PKI}/ingest.key",
            "FLEETWIRE_MQTT_CA_FILE": f"{PKI}/ca-bundle.pem",
            "FLEETWIRE_KEYRING_URL": keyring,
            "FLEETWIRE_ADMIN_LISTEN": f":{ADMIN_PORT}",
            # Stable per unit: the ack publisher's MQTT client ID uses it.
            "FLEETWIRE_INSTANCE_ID": self.unit.name.replace("/", "-"),
            "FLEETWIRE_LOG_LEVEL": str(self.config["log-level"]),
        }
        if kafka.tls_ca:
            env["FLEETWIRE_KAFKA_TLS_CA_FILE"] = f"{PKI}/kafka-ca.pem"
        return env

    def _topics_key(self, kafka: Kafka) -> str:
        """Names the Kafka, login and replication setting the topics are for."""
        key = f"{kafka.endpoints}|{kafka.username}|{self.config['kafka-replication']}"
        return hashlib.sha256(key.encode()).hexdigest()

    def _topics_ready(self, kafka: Kafka) -> bool:
        """Whether the leader has recorded topics for this Kafka in the peer data."""
        rel = self.model.get_relation(PEER)
        return rel is not None and rel.data[self.app].get("topics") == self._topics_key(kafka)

    def _ensure_topics(self, kafka: Kafka, env: dict[str, str]) -> bool:
        """Creates the topics once per Kafka and replication setting (leader only)."""
        rel = self.model.get_relation(PEER)
        if rel is None:
            return False
        if self._topics_ready(kafka):
            return True
        done = self._topics_key(kafka)
        self.unit.status = ops.MaintenanceStatus("creating Kafka topics")
        try:
            self._container.exec([BINARY, "ensure-topics"], environment=env, timeout=600).wait_output()
        except ops.pebble.ExecError as e:
            logger.error("ensure-topics failed (exit %s): %s", e.exit_code, e.stderr)
            return False
        except ops.pebble.ChangeError as e:
            logger.error("ensure-topics could not run: %s", e)
            return False
        rel.data[self.app]["topics"] = done
        return True

    @staticmethod
    def _layer(env: dict[str, str]) -> ops.pebble.LayerDict:
        return {
            "summary": "fleetwire ingest",
            "services": {
                SERVICE: {
                    "override": "replace",
                    "summary": "fleetwire-ingest run",
                    "command": f"{BINARY} run",
                    "startup": "enabled",
                    "user-id": WORKLOAD_UID,
                    "group-id": WORKLOAD_UID,
                    "environment": env,
                    "on-check-failure": {"alive": "restart"},
                }
            },
            "checks": {
                "ready": {
                    "override": "replace",
                    "level": "ready",
                    "period": "10s",
                    "threshold": 3,
                    "http": {"url": f"http://localhost:{ADMIN_PORT}/readyz"},
                },
                "alive": {
                    "override": "replace",
                    "level": "alive",
                    "period": "10s",
                    "threshold": 6,
                    "http": {"url": f"http://localhost:{ADMIN_PORT}/healthz"},
                },
            },
        }


if __name__ == "__main__":  # pragma: nocover
    ops.main(FleetwireIngestCharm)
