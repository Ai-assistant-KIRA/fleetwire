#!/usr/bin/env python3
# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""Kubernetes charm for fleetwire-bridge.

Every event runs one reconcile step: read the PKI secret and both
relations and write the Pebble layer. collect-status reports what is
still missing.
"""

from __future__ import annotations

import hashlib
import logging
import urllib.request
from dataclasses import dataclass

import ops
from charms.data_platform_libs.v0.data_interfaces import KafkaRequires

logger = logging.getLogger(__name__)

CONTAINER = "bridge"
SERVICE = "fleetwire-bridge"
BINARY = "/usr/local/bin/service"
PKI = "/etc/fleetwire/pki"
WORKLOAD_UID = 65532
ADMIN_PORT = 9090
# The bridge only writes the uplink topic; ingest creates it.
TOPIC = "fleetwire.uplink.v1"
PKI_FILES = {"tls-cert": "bridge.pem", "tls-key": "bridge.key", "ca-bundle": "ca-bundle.pem"}


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

    The bridge reads its certificates once, at start. The digest goes into
    the layer's environment, so a rotated certificate changes the layer and
    replan restarts the service with the new files.
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


class FleetwireBridgeCharm(ops.CharmBase):
    """Runs fleetwire-bridge."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)
        self.kafka = KafkaRequires(self, relation_name="kafka", topic=TOPIC, extra_user_roles="producer")
        for event in (
            self.on.bridge_pebble_ready,
            self.on.config_changed,
            self.on.upgrade_charm,
            self.on.update_status,
            self.on.secret_changed,
            self.on.mqtt_relation_changed,
            self.on.mqtt_relation_broken,
            self.kafka.on.topic_created,
            self.kafka.on.bootstrap_server_changed,
            self.on.kafka_relation_broken,
        ):
            self.framework.observe(event, self._reconcile)
        self.framework.observe(self.on.collect_unit_status, self._on_collect_status)

    @property
    def _container(self) -> ops.Container:
        return self.unit.get_container(CONTAINER)

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
                return Kafka(endpoints, user, password, tls_ca)
        return None

    def _mqtt_url(self) -> str | None:
        rel = self.model.get_relation("mqtt")
        if rel is None or rel.app is None:
            return None
        return rel.data[rel.app].get("service-url") or None

    def _config_problem(self) -> str | None:
        receive = int(self.config["receive-maximum"])
        if not 1 <= receive <= 65535:
            return f"receive-maximum must be 1 to 65535, not {receive}"
        return None

    def _on_collect_status(self, event: ops.CollectStatusEvent) -> None:
        pki = self._pki()
        if not self.config.get("pki-secret"):
            event.add_status(ops.BlockedStatus("set pki-secret to the ID of the bridge certificate secret"))
        elif pki is None:
            event.add_status(
                ops.BlockedStatus("cannot read pki-secret: run juju grant-secret <secret> " + self.app.name)
            )
        elif missing := sorted(k for k in PKI_FILES if not pki.get(k)):
            event.add_status(ops.BlockedStatus("pki-secret lacks " + ", ".join(missing)))
        if problem := self._config_problem():
            event.add_status(ops.BlockedStatus(problem))
        for endpoint, other, ready in (("kafka", "kafka-k8s", self._kafka), ("mqtt", "mosquitto-k8s", self._mqtt_url)):
            if not self.model.get_relation(endpoint):
                event.add_status(
                    ops.BlockedStatus(f"integrate with {other}: juju integrate {self.app.name}:{endpoint} {other}")
                )
            elif ready() is None:
                event.add_status(ops.WaitingStatus(f"waiting for {endpoint} relation data"))
        if not self._container.can_connect():
            event.add_status(ops.WaitingStatus("waiting for the workload container"))
        elif self._service_ready():
            event.add_status(ops.ActiveStatus())
        else:
            event.add_status(ops.WaitingStatus("waiting for the bridge to become ready"))

    def _service_ready(self) -> bool:
        services = self._container.get_services(SERVICE)
        return SERVICE in services and services[SERVICE].is_running() and readyz(ADMIN_PORT)

    def _reconcile(self, event: ops.EventBase) -> None:
        # Nothing connects to the bridge through its Kubernetes service. The
        # admin listener (plain HTTP) stays off it: the checks use localhost,
        # and metrics are scraped from the pod.
        self.unit.set_ports()
        if isinstance(event, ops.RelationBrokenEvent):
            if self._container.can_connect() and SERVICE in self._container.get_plan().services:
                self._container.stop(SERVICE)
            return
        pki, kafka, mqtt = self._pki(), self._kafka(), self._mqtt_url()
        if pki is None or any(not pki.get(k) for k in PKI_FILES) or kafka is None or mqtt is None:
            return
        if self._config_problem() or not self._container.can_connect():
            return
        files = {f"{PKI}/{name}": pki[key] for key, name in PKI_FILES.items()}
        if kafka.tls_ca:
            files[f"{PKI}/kafka-ca.pem"] = kafka.tls_ca
        own = {"user_id": WORKLOAD_UID, "group_id": WORKLOAD_UID, "make_dirs": True, "permissions": 0o600}
        for path, content in files.items():
            self._container.push(path, content, **own)
        env = {
            "FLEETWIRE_MQTT_URL": mqtt,
            "FLEETWIRE_MQTT_CERT_FILE": f"{PKI}/bridge.pem",
            "FLEETWIRE_MQTT_KEY_FILE": f"{PKI}/bridge.key",
            "FLEETWIRE_MQTT_CA_FILE": f"{PKI}/ca-bundle.pem",
            "FLEETWIRE_KAFKA_BROKERS": kafka.endpoints,
            "FLEETWIRE_KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
            "FLEETWIRE_KAFKA_USERNAME": kafka.username,
            "FLEETWIRE_KAFKA_PASSWORD": kafka.password,
            "FLEETWIRE_KAFKA_TLS": "true" if kafka.tls_ca else "false",
            "FLEETWIRE_BRIDGE_RECEIVE_MAXIMUM": str(self.config["receive-maximum"]),
            "FLEETWIRE_ADMIN_LISTEN": f":{ADMIN_PORT}",
            "FLEETWIRE_INSTANCE_ID": self.unit.name.replace("/", "-"),
            "FLEETWIRE_LOG_LEVEL": str(self.config["log-level"]),
            # Not read by the bridge: a new value makes replan restart it.
            "FLEETWIRE_CONFIG_SHA256": files_digest(files),
        }
        if kafka.tls_ca:
            env["FLEETWIRE_KAFKA_TLS_CA_FILE"] = f"{PKI}/kafka-ca.pem"
        layer: ops.pebble.LayerDict = {
            "summary": "fleetwire bridge",
            "services": {
                SERVICE: {
                    "override": "replace",
                    "summary": "fleetwire-bridge run",
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
        self._container.add_layer(SERVICE, layer, combine=True)
        self._container.replan()


if __name__ == "__main__":  # pragma: nocover
    ops.main(FleetwireBridgeCharm)
