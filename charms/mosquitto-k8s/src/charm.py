#!/usr/bin/env python3
# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""Kubernetes charm for Eclipse Mosquitto as fleetwire's broker.

Every event runs one reconcile step: render the configuration and ACL,
write the certificates, fetch the device CRL from the API, and write the
Pebble layer. A changed CRL is loaded with SIGHUP, which Mosquitto 2.x
handles by rereading crlfile without dropping connections.
"""

from __future__ import annotations

import hashlib
import logging
import socket
import ssl
import urllib.error
import urllib.request

import ops

logger = logging.getLogger(__name__)

CONTAINER = "mosquitto"
SERVICE = "mosquitto"
CONF_DIR = "/mosquitto/config"
CERTS = "/mosquitto/certs"
CRL_FILE = "/mosquitto/crl/crl.pem"
# The storage mount is root-owned; Mosquitto writes into a subdirectory
# that Pebble creates for the mosquitto user (the image has no chown step).
DATA = "/mosquitto/data/db"
# The user the eclipse-mosquitto image drops privileges to.
MOSQUITTO_UID = 1883
DEVICE_PORT, SERVICE_PORT = 8883, 8884
PKI_FILES = {
    "tls-cert": "mosquitto.pem",
    "tls-key": "mosquitto.key",
    "device-trust": "device-trust.pem",
    "service-trust": "service-trust.pem",
    "ca-bundle": "ca-bundle.pem",
}
CRL_PEM_HEADER = b"-----BEGIN X509 CRL-----"

# Each listener has its own ACL. On the device listener the username is the
# certificate CN, which the API sets to the server-generated device UUID, and
# only pattern rules apply: a certificate from the online device CA that
# carries a service CN still reaches only "its own" device topics.
DEVICE_ACL = """\
pattern write fw/v1/d/%u/up
pattern read fw/v1/d/%u/ack
"""
# On the service listener only the bridge and ingest have rights; a device
# certificate that reaches it can do nothing.
SERVICE_ACL = """\
user fleetwire-bridge
topic read fw/v1/d/+/up
user fleetwire-ingest
topic write fw/v1/d/+/ack
"""
ACL_FILES = {"acl-devices": DEVICE_ACL, "acl-services": SERVICE_ACL}


def render_config(max_queued_messages: int) -> str:
    """mosquitto.conf with the device and service listeners.

    Two listeners, because OpenSSL checks only the CRL of the leaf's issuer:
    devices and services have different issuing CAs. per_listener_settings
    gives each its own ACL; Mosquitto 2.1 deprecates the option (removal is
    planned for 3.0) but has no other way to do that, and it must come before
    the other security settings.
    """
    return f"""\
per_listener_settings true
persistence true
persistence_location {DATA}/
autosave_interval 10
max_queued_messages {max_queued_messages}
max_queued_bytes 268435456
max_inflight_messages 1000
max_packet_size 270000
max_keepalive 300
set_tcp_nodelay true
log_dest stdout
log_type error
log_type warning
log_type notice
connection_messages false

listener {DEVICE_PORT}
protocol mqtt
max_connections -1
listener_allow_anonymous false
acl_file {CONF_DIR}/acl-devices
cafile {CERTS}/device-trust.pem
certfile {CERTS}/mosquitto.pem
keyfile {CERTS}/mosquitto.key
crlfile {CRL_FILE}
require_certificate true
use_identity_as_username true
tls_version tlsv1.2

listener {SERVICE_PORT}
protocol mqtt
listener_allow_anonymous false
acl_file {CONF_DIR}/acl-services
cafile {CERTS}/service-trust.pem
certfile {CERTS}/mosquitto.pem
keyfile {CERTS}/mosquitto.key
require_certificate true
use_identity_as_username true
tls_version tlsv1.2
"""


def fetch_crl(url: str, ca_bundle: str) -> bytes:
    """Fetches the CRL over TLS verified against ca_bundle.

    The API signs the list with the Device Issuing CA and OpenSSL checks
    that signature when Mosquitto loads it, so the charm only checks the
    format here.
    """
    ctx = ssl.create_default_context(cadata=ca_bundle)
    with urllib.request.urlopen(url, context=ctx, timeout=10) as resp:  # noqa: S310 (https only, below)
        body = resp.read(4 << 20)
    if not body.startswith(CRL_PEM_HEADER):
        raise ValueError("the answer is not a PEM CRL")
    return body


def listening(port: int) -> bool:
    """Whether Mosquitto accepts TCP connections on port (same pod network)."""
    try:
        with socket.create_connection(("localhost", port), timeout=3):
            return True
    except OSError:
        return False


class MosquittoCharm(ops.CharmBase):
    """Runs Mosquitto for fleetwire."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)
        for event in (
            self.on.mosquitto_pebble_ready,
            self.on.config_changed,
            self.on.upgrade_charm,
            self.on.leader_elected,
            self.on.update_status,
            self.on.secret_changed,
            self.on.mqtt_relation_joined,
            self.on.crl_relation_changed,
        ):
            self.framework.observe(event, self._reconcile)
        self.framework.observe(self.on.collect_unit_status, self._on_collect_status)

    @property
    def _container(self) -> ops.Container:
        return self.unit.get_container(CONTAINER)

    @property
    def _service_url(self) -> str:
        return f"tls://{self.app.name}.{self.model.name}.svc.cluster.local:{SERVICE_PORT}"

    def _pki(self) -> dict[str, str] | None:
        secret_id = str(self.config.get("pki-secret") or "")
        if not secret_id:
            return None
        try:
            return self.model.get_secret(id=secret_id).get_content(refresh=True)
        except (ops.SecretNotFoundError, ops.ModelError) as e:
            logger.warning("cannot read the PKI secret %s: %s", secret_id, e)
            return None

    def _crl_url(self) -> str | None:
        rel = self.model.get_relation("crl")
        if rel is None or rel.app is None:
            return None
        url = rel.data[rel.app].get("crl-url") or ""
        return url if url.startswith("https://") else None

    def _has_crl(self) -> bool:
        return self._container.can_connect() and self._container.exists(CRL_FILE)

    def _on_collect_status(self, event: ops.CollectStatusEvent) -> None:
        pki = self._pki()
        if not self.config.get("pki-secret"):
            event.add_status(ops.BlockedStatus("set pki-secret to the ID of the broker certificate secret"))
        elif pki is None:
            event.add_status(
                ops.BlockedStatus("cannot read pki-secret: run juju grant-secret <secret> " + self.app.name)
            )
        elif missing := sorted(k for k in PKI_FILES if not pki.get(k)):
            event.add_status(ops.BlockedStatus("pki-secret lacks " + ", ".join(missing)))
        if int(self.config["max-queued-messages"]) < 1:
            event.add_status(ops.BlockedStatus("max-queued-messages must be at least 1"))
        if not self.model.get_relation("crl"):
            event.add_status(
                ops.BlockedStatus(
                    f"integrate with fleetwire-api-k8s: juju integrate {self.app.name}:crl fleetwire-api-k8s"
                )
            )
        elif self._crl_url() is None:
            event.add_status(ops.WaitingStatus("waiting for the CRL address from fleetwire-api-k8s"))
        if not self._container.can_connect():
            event.add_status(ops.WaitingStatus("waiting for the workload container"))
        elif not self._has_crl():
            event.add_status(ops.WaitingStatus("waiting for the first CRL from the API"))
        elif self._service_ready():
            event.add_status(ops.ActiveStatus())
        else:
            event.add_status(ops.WaitingStatus("waiting for Mosquitto to listen"))

    def _service_ready(self) -> bool:
        services = self._container.get_services(SERVICE)
        return SERVICE in services and services[SERVICE].is_running() and listening(SERVICE_PORT)

    def _reconcile(self, _: ops.EventBase) -> None:
        self.unit.set_ports(DEVICE_PORT, SERVICE_PORT)
        if self.unit.is_leader():
            for rel in self.model.relations["mqtt"]:
                rel.data[self.app]["service-url"] = self._service_url
        pki, crl_url = self._pki(), self._crl_url()
        if pki is None or any(not pki.get(k) for k in PKI_FILES) or crl_url is None:
            return
        max_queued = int(self.config["max-queued-messages"])
        if max_queued < 1 or not self._container.can_connect():
            return
        c = self._container
        own = {"user_id": MOSQUITTO_UID, "group_id": MOSQUITTO_UID, "make_dirs": True}
        conf = render_config(max_queued)
        c.push(f"{CONF_DIR}/mosquitto.conf", conf, permissions=0o644, **own)
        for name, acl in ACL_FILES.items():
            c.push(f"{CONF_DIR}/{name}", acl, permissions=0o644, **own)
        for key, name in PKI_FILES.items():
            c.push(f"{CERTS}/{name}", pki[key], permissions=0o600, **own)
        c.make_dir(DATA, make_parents=True, permissions=0o700, user_id=MOSQUITTO_UID, group_id=MOSQUITTO_UID)
        crl_changed = self._sync_crl(crl_url, pki["ca-bundle"])
        if not self._has_crl():
            return  # Mosquitto refuses to start without its crlfile.
        # The hash makes a changed configuration or certificate change the
        # layer, so replan restarts Mosquitto; a new CRL needs only SIGHUP.
        parts = [conf, *ACL_FILES.values(), *(pki[k] for k in sorted(PKI_FILES))]
        digest = hashlib.sha256("".join(parts).encode()).hexdigest()
        layer: ops.pebble.LayerDict = {
            "summary": "mosquitto",
            "services": {
                SERVICE: {
                    "override": "replace",
                    "summary": "Eclipse Mosquitto",
                    "command": f"mosquitto -c {CONF_DIR}/mosquitto.conf",
                    "startup": "enabled",
                    "environment": {"FLEETWIRE_CONFIG_SHA256": digest},
                }
            },
            "checks": {
                "listening": {
                    "override": "replace",
                    "level": "ready",
                    "period": "10s",
                    "threshold": 3,
                    "tcp": {"port": SERVICE_PORT},
                },
            },
        }
        running = SERVICE in c.get_services() and c.get_services()[SERVICE].is_running()
        c.add_layer(SERVICE, layer, combine=True)
        c.replan()
        if crl_changed and running:
            c.send_signal("SIGHUP", SERVICE)
            logger.info("loaded a new CRL into Mosquitto")

    def _sync_crl(self, url: str, ca_bundle: str) -> bool:
        """Writes the API's current CRL if it differs; True when it did."""
        try:
            crl = fetch_crl(url, ca_bundle)
        except (OSError, ValueError, urllib.error.URLError) as e:
            # The broker keeps the CRL it has; the next update-status retries.
            logger.warning("cannot fetch the CRL from %s: %s", url, e)
            return False
        if self._has_crl():
            with self._container.pull(CRL_FILE, encoding=None) as f:
                if f.read() == crl:
                    return False
        self._container.push(
            CRL_FILE, crl, make_dirs=True, permissions=0o644, user_id=MOSQUITTO_UID, group_id=MOSQUITTO_UID
        )
        return True


if __name__ == "__main__":  # pragma: nocover
    ops.main(MosquittoCharm)
