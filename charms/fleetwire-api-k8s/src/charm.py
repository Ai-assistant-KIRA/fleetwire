#!/usr/bin/env python3
# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""Kubernetes charm for fleetwire-api.

Every event runs the same reconcile step: read the configuration, the PKI
secret and the two database relations, run the migrations on the leader
when they are due, write the Pebble layer, and publish what ingest and the
broker need. Statuses are computed separately by collect-status, so they
always describe the current state rather than the last event.

The API relates to Charmed PostgreSQL twice. Charmed PostgreSQL switches a
plain relation user to the database's owner role at login, which is right
for migrations and wrong for a service that must stay inside row-level
security. So `database` (no extra roles) is the owner, used only by
`migrate` and `create-operator-token`, and `runtime-database` (extra user
role charmed_dml) is the service's login, a member of the group the
migrations give the policies and grants to.
"""

from __future__ import annotations

import hashlib
import logging
import urllib.request
from dataclasses import dataclass
from urllib.parse import quote

import ops
from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires

logger = logging.getLogger(__name__)

CONTAINER = "api"
SERVICE = "fleetwire-api"
BINARY = "/usr/local/bin/service"
DATA = "/var/lib/fleetwire"
PKI = "/etc/fleetwire/pki"
# The distroless images run as nonroot (65532); files are written for it.
WORKLOAD_UID = 65532
PUBLIC_PORT, DEVICE_PORT, INTERNAL_PORT, ADMIN_PORT = 8443, 8444, 8445, 9090
DATABASE_NAME = "fleetwire"
# The Charmed PostgreSQL group every runtime relation user joins
# (extra-user-roles); the migrations write the policies and grants for it.
RUNTIME_ROLE = "charmed_dml"
OWNER_ENDPOINT, RUNTIME_ENDPOINT = "database", "runtime-database"
PEER = "api-peers"
# PEM entries of the PKI secret, written as files under PKI.
PKI_FILES = {
    "tls-cert": "api.pem",
    "tls-key": "api.key",
    "ca-bundle": "ca-bundle.pem",
    "device-trust": "device-trust.pem",
    "device-ca-cert": "device-ca.pem",
    "device-ca-key": "device-ca.key",
    "service-trust": "service-trust.pem",
}
ROOT_KEY_FILE = "keyring-root.key"


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


@dataclass(frozen=True)
class Database:
    """Connection details from a postgresql_client relation."""

    host_port: str
    database: str
    username: str
    password: str
    tls_ca: str | None

    @property
    def url(self) -> str:
        """A PostgreSQL URL, verifying the server when TLS is on."""
        mode = f"sslmode=verify-full&sslrootcert={PKI}/postgresql-ca.pem" if self.tls_ca else "sslmode=prefer"
        user, password = quote(self.username, safe=""), quote(self.password, safe="")
        return f"postgres://{user}:{password}@{self.host_port}/{self.database}?{mode}"


def credentials(requirer: DatabaseRequires) -> Database | None:
    """The relation's connection details, once the database exists."""
    for data in requirer.fetch_relation_data().values():
        endpoints, user, password = data.get("endpoints"), data.get("username"), data.get("password")
        if endpoints and user and password:
            tls_ca = data.get("tls-ca") if str(data.get("tls", "")).lower() == "true" else None
            return Database(endpoints.split(",")[0], data.get("database") or DATABASE_NAME, user, password, tls_ca)
    return None


class FleetwireApiCharm(ops.CharmBase):
    """Runs fleetwire-api."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)
        self.owner_db = DatabaseRequires(self, relation_name=OWNER_ENDPOINT, database_name=DATABASE_NAME)
        self.runtime_db = DatabaseRequires(
            self, relation_name=RUNTIME_ENDPOINT, database_name=DATABASE_NAME, extra_user_roles=RUNTIME_ROLE
        )
        observe = self.framework.observe
        for event in (
            self.on.api_pebble_ready,
            self.on.config_changed,
            self.on.leader_elected,
            self.on.update_status,
            self.on.secret_changed,
            self.on[PEER].relation_changed,
            self.on.ingest_relation_joined,
            self.on.ingest_relation_changed,
            self.on.crl_relation_joined,
            self.owner_db.on.database_created,
            self.owner_db.on.endpoints_changed,
            self.runtime_db.on.database_created,
            self.runtime_db.on.endpoints_changed,
        ):
            observe(event, self._reconcile)
        observe(self.on.upgrade_charm, self._on_upgrade)
        observe(self.on[OWNER_ENDPOINT].relation_broken, self._on_owner_broken)
        observe(self.on[RUNTIME_ENDPOINT].relation_broken, self._on_runtime_broken)
        observe(self.on.create_operator_token_action, self._on_create_operator_token)
        observe(self.on.collect_unit_status, self._on_collect_status)

    # ---- state the charm reads -------------------------------------------

    @property
    def _container(self) -> ops.Container:
        return self.unit.get_container(CONTAINER)

    @property
    def _service_host(self) -> str:
        return f"{self.app.name}.{self.model.name}.svc.cluster.local"

    def _pki(self) -> dict[str, str] | None:
        """The PKI secret's content, or None when it is not set or readable."""
        secret_id = str(self.config.get("pki-secret") or "")
        if not secret_id:
            return None
        try:
            return self.model.get_secret(id=secret_id).get_content(refresh=True)
        except (ops.SecretNotFoundError, ops.ModelError) as e:
            logger.warning("cannot read the PKI secret %s: %s", secret_id, e)
            return None

    def _pki_problem(self, pki: dict[str, str] | None) -> str | None:
        if not self.config.get("pki-secret"):
            return "set pki-secret to the ID of the PKI secret (see the deploy guide)"
        if pki is None:
            return f"cannot read pki-secret: run juju grant-secret <secret> {self.app.name}"
        missing = sorted(k for k in [*PKI_FILES, "keyring-root-key"] if not pki.get(k))
        if missing:
            return "pki-secret lacks " + ", ".join(missing)
        root = pki["keyring-root-key"].strip()
        if len(root) != 64 or any(c not in "0123456789abcdefABCDEF" for c in root):
            return "pki-secret keyring-root-key must be 64 hex characters (32 bytes)"
        return None

    def _peer_data(self) -> ops.RelationDataContent | None:
        rel = self.model.get_relation(PEER)
        return rel.data[self.app] if rel else None

    # ---- statuses ----------------------------------------------------------

    def _on_collect_status(self, event: ops.CollectStatusEvent) -> None:
        if (planned := self.app.planned_units()) > 1:
            event.add_status(
                ops.BlockedStatus(
                    f"scale to 1 unit: the local keyring keeps the tenant keys on one unit (planned {planned})"
                )
            )
        if problem := self._pki_problem(self._pki()):
            event.add_status(ops.BlockedStatus(problem))
        if not self.config.get("device-broker-url"):
            event.add_status(ops.BlockedStatus("set device-broker-url (the broker address devices use)"))
        for endpoint, requirer in ((OWNER_ENDPOINT, self.owner_db), (RUNTIME_ENDPOINT, self.runtime_db)):
            if not self.model.get_relation(endpoint):
                event.add_status(
                    ops.BlockedStatus(
                        f"integrate with postgresql-k8s: juju integrate {self.app.name}:{endpoint} postgresql-k8s"
                    )
                )
            elif credentials(requirer) is None:
                event.add_status(ops.WaitingStatus(f"waiting for the {endpoint} relation"))
        if not self._container.can_connect():
            event.add_status(ops.WaitingStatus("waiting for the workload container"))
        elif not self._migrated():
            event.add_status(ops.WaitingStatus("waiting for migrations on the leader"))
        elif self._service_ready():
            event.add_status(ops.ActiveStatus())
        else:
            event.add_status(ops.WaitingStatus("waiting for the API to become ready"))

    def _service_ready(self) -> bool:
        services = self._container.get_services(SERVICE)
        return SERVICE in services and services[SERVICE].is_running() and readyz(ADMIN_PORT)

    # ---- reconcile ---------------------------------------------------------

    def _on_upgrade(self, event: ops.EventBase) -> None:
        # A new image may bring migrations: forget that the schema is current.
        self._forget_migrations()
        self._reconcile(event)

    def _on_owner_broken(self, _: ops.EventBase) -> None:
        # The next owner relation may point at another database: migrate
        # again when it arrives. The service keeps running on its own login.
        self._forget_migrations()

    def _on_runtime_broken(self, _: ops.EventBase) -> None:
        # The service's login is deleted with the relation.
        if self._container.can_connect() and SERVICE in self._container.get_plan().services:
            self._container.stop(SERVICE)

    def _forget_migrations(self) -> None:
        if not self.unit.is_leader():
            return
        if (peer := self._peer_data()) is not None:
            peer.pop("migrated", None)
        for rel in self.model.relations["ingest"]:
            rel.data[self.app].pop("schema-ready", None)

    def _reconcile(self, _: ops.EventBase) -> None:
        self.unit.set_ports(PUBLIC_PORT, DEVICE_PORT, INTERNAL_PORT, ADMIN_PORT)
        pki, owner, runtime = self._pki(), credentials(self.owner_db), credentials(self.runtime_db)
        if pki is None or self._pki_problem(pki) or owner is None or runtime is None:
            return
        if not self._container.can_connect():
            return
        if self.app.planned_units() > 1 or not self.config.get("device-broker-url"):
            return
        self._write_files(pki, runtime.tls_ca or owner.tls_ca)
        due = self.unit.is_leader() and not self._migrated(owner)
        if due and not self._migrate(owner):
            return
        if not self._migrated():
            return
        self._container.add_layer(SERVICE, self._layer(runtime), combine=True)
        self._container.replan()
        if self.unit.is_leader():
            self._publish()

    def _on_create_operator_token(self, event: ops.ActionEvent) -> None:
        """Mints an operator token with the owner credentials only the charm holds.

        The token is returned once, in the task results; anyone who can read
        this model's tasks can read it, as with any credential action.
        """
        owner = credentials(self.owner_db)
        if owner is None or not self._migrated():
            event.fail("the database is not ready or not migrated yet")
            return
        if not self._container.can_connect():
            event.fail("the workload container is not reachable")
            return
        cmd = [BINARY, "create-operator-token", "-name", str(event.params["name"])]
        try:
            out, _ = self._container.exec(
                cmd, environment={"FLEETWIRE_DATABASE_OWNER_URL": owner.url}, timeout=60
            ).wait_output()
        except ops.pebble.ExecError as e:
            event.fail(f"create-operator-token failed: {str(e.stderr).strip()}")
            return
        event.set_results({"token": out.strip()})

    def _write_files(self, pki: dict[str, str], pg_ca: str | None) -> None:
        c = self._container
        own = {"user_id": WORKLOAD_UID, "group_id": WORKLOAD_UID, "make_dirs": True, "permissions": 0o600}
        for key, name in PKI_FILES.items():
            c.push(f"{PKI}/{name}", pki[key], **own)
        c.push(f"{PKI}/{ROOT_KEY_FILE}", bytes.fromhex(pki["keyring-root-key"].strip()), **own)
        if pg_ca:
            c.push(f"{PKI}/postgresql-ca.pem", pg_ca, **own)
        for sub in ("keyring", "crl", "exports"):
            c.make_dir(
                f"{DATA}/{sub}", make_parents=True, permissions=0o700, user_id=WORKLOAD_UID, group_id=WORKLOAD_UID
            )

    @staticmethod
    def _fingerprint(owner: Database) -> str:
        return hashlib.sha256(
            f"{owner.host_port}|{owner.database}|{owner.username}|{RUNTIME_ROLE}".encode()
        ).hexdigest()

    def _migrated(self, owner: Database | None = None) -> bool:
        """Whether the schema is current: for this owner relation, if given."""
        peer = self._peer_data()
        done = peer.get("migrated") if peer is not None else None
        if not done:
            return False
        return owner is None or done == self._fingerprint(owner)

    def _migrate(self, owner: Database) -> bool:
        self.unit.status = ops.MaintenanceStatus("running database migrations")
        env = {"FLEETWIRE_DATABASE_OWNER_URL": owner.url, "FLEETWIRE_DATABASE_RUNTIME_ROLE": RUNTIME_ROLE}
        try:
            out, _ = self._container.exec([BINARY, "migrate"], environment=env, timeout=600).wait_output()
        except ops.pebble.ExecError as e:
            # Neither stream carries credentials: the command redacts secrets.
            logger.error("migrations failed (exit %s): %s", e.exit_code, e.stderr)
            return False
        except ops.pebble.ChangeError as e:
            logger.error("migrations could not run: %s", e)
            return False
        logger.info("migrations: %s", out.strip())
        peer = self._peer_data()
        if peer is not None:
            peer["migrated"] = self._fingerprint(owner)
        return True

    def _layer(self, runtime: Database) -> ops.pebble.LayerDict:
        device_api = str(self.config.get("device-api-url") or f"https://{self._service_host}:{DEVICE_PORT}")
        env = {
            "FLEETWIRE_DATABASE_URL": runtime.url,
            "FLEETWIRE_API_PUBLIC_LISTEN": f":{PUBLIC_PORT}",
            "FLEETWIRE_API_DEVICE_LISTEN": f":{DEVICE_PORT}",
            "FLEETWIRE_API_INTERNAL_LISTEN": f":{INTERNAL_PORT}",
            # Inside the pod network, for Prometheus and the Pebble checks.
            "FLEETWIRE_API_ADMIN_LISTEN": f":{ADMIN_PORT}",
            "FLEETWIRE_API_TLS_CERT_FILE": f"{PKI}/api.pem",
            "FLEETWIRE_API_TLS_KEY_FILE": f"{PKI}/api.key",
            "FLEETWIRE_DEVICE_TRUST_FILE": f"{PKI}/device-trust.pem",
            "FLEETWIRE_DEVICE_CA_CERT_FILE": f"{PKI}/device-ca.pem",
            "FLEETWIRE_DEVICE_CA_KEY_FILE": f"{PKI}/device-ca.key",
            "FLEETWIRE_CA_BUNDLE_FILE": f"{PKI}/ca-bundle.pem",
            "FLEETWIRE_SERVICE_TRUST_FILE": f"{PKI}/service-trust.pem",
            "FLEETWIRE_DEVICE_BROKER_URL": str(self.config["device-broker-url"]),
            "FLEETWIRE_DEVICE_API_URL": device_api,
            "FLEETWIRE_KEYRING_DIR": f"{DATA}/keyring",
            "FLEETWIRE_KEYRING_ROOT_KEY_FILE": f"{PKI}/{ROOT_KEY_FILE}",
            "FLEETWIRE_CRL_FILE": f"{DATA}/crl/crl.pem",
            "FLEETWIRE_EXPORT_DIR": f"{DATA}/exports",
            "FLEETWIRE_LOG_LEVEL": str(self.config["log-level"]),
            "FLEETWIRE_JOB_WORKERS": str(self.config["job-workers"]),
            "FLEETWIRE_API_RATE_LIMIT": str(self.config["rate-limit"]),
        }
        return {
            "summary": "fleetwire API",
            "services": {
                SERVICE: {
                    "override": "replace",
                    "summary": "fleetwire-api serve",
                    "command": f"{BINARY} serve",
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

    def _publish(self) -> None:
        """Tells ingest where the keyring is and that the schema exists, and the broker where the CRL is."""
        for rel in self.model.relations["ingest"]:
            rel.data[self.app].update(
                {"keyring-url": f"https://{self._service_host}:{INTERNAL_PORT}", "schema-ready": "true"}
            )
        for rel in self.model.relations["crl"]:
            rel.data[self.app]["crl-url"] = f"https://{self._service_host}:{DEVICE_PORT}/v1/crl"


if __name__ == "__main__":  # pragma: nocover
    ops.main(FleetwireApiCharm)
