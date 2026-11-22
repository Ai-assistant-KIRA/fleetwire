# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""State-transition tests for the mosquitto-k8s charm (ops.testing)."""

from __future__ import annotations

from pathlib import Path

import pytest
from ops import testing

import charm
from charm import CONTAINER, SERVICE, MosquittoCharm, render_config

PKI = {
    "tls-cert": "BROKER CERT",
    "tls-key": "BROKER KEY",
    "device-trust": "DEVICE TRUST",
    "service-trust": "SERVICE TRUST",
    "ca-bundle": "CA BUNDLE",
}
CRL_V1 = b"-----BEGIN X509 CRL-----\nv1\n-----END X509 CRL-----\n"
CRL_V2 = b"-----BEGIN X509 CRL-----\nv2\n-----END X509 CRL-----\n"
CRL_URL = "https://fleetwire-api-k8s.test.svc.cluster.local:8444/v1/crl"


@pytest.fixture
def ctx() -> testing.Context:
    return testing.Context(MosquittoCharm)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """The container's /mosquitto, kept between runs so later events see earlier files."""
    return tmp_path


class FakeAPI:
    """Stands in for the API's CRL endpoint."""

    def __init__(self, crl: bytes | None = CRL_V1):
        self.crl = crl
        self.calls: list[tuple[str, str]] = []

    def __call__(self, url: str, ca_bundle: str) -> bytes:
        """Answers like charm.fetch_crl."""
        self.calls.append((url, ca_bundle))
        if self.crl is None:
            raise OSError("connection refused")
        return self.crl


@pytest.fixture
def api(monkeypatch) -> FakeAPI:
    fake = FakeAPI()
    monkeypatch.setattr(charm, "fetch_crl", fake)
    monkeypatch.setattr(charm, "listening", lambda port: False)
    return fake


@pytest.fixture
def signals(monkeypatch) -> list:
    sent: list = []
    monkeypatch.setattr(charm.ops.Container, "send_signal", lambda self, sig, *names: sent.append((sig, names)))
    return sent


def setup(root: Path, *, crl=True, crl_url=CRL_URL, mqtt=True, pki=None, config=None, secret=None):
    secrets, relations, cfg = [], [], dict(config or {})
    if pki is not False:
        s = secret or testing.Secret(tracked_content=pki or PKI)
        secrets.append(s)
        cfg.setdefault("pki-secret", s.id)
    if crl:
        relations.append(
            testing.Relation(
                "crl",
                interface="fleetwire_crl",
                remote_app_name="fleetwire-api-k8s",
                remote_app_data={"crl-url": crl_url} if crl_url else {},
            )
        )
    if mqtt:
        relations.append(testing.Relation("mqtt", interface="fleetwire_mqtt", remote_app_name="fleetwire-bridge-k8s"))
    container = testing.Container(
        CONTAINER, can_connect=True, mounts={"mosquitto": testing.Mount(location="/mosquitto", source=root)}
    )
    return testing.State(
        leader=True,
        config=cfg,
        secrets=secrets,
        relations=relations,
        containers=[container],
        model=testing.Model(name="test"),
    )


def test_renders_both_listeners_and_starts(ctx, root, api):
    out = ctx.run(ctx.on.config_changed(), setup(root))
    conf = (root / "config/mosquitto.conf").read_text()
    assert conf == render_config(1000000)
    assert "listener 8883" in conf
    assert "crlfile /mosquitto/crl/crl.pem" in conf
    assert "listener 8884" in conf
    assert (root / "config/acl-devices").read_text() == charm.DEVICE_ACL
    assert (root / "config/acl-services").read_text() == charm.SERVICE_ACL
    assert (root / "certs/mosquitto.key").read_text() == "BROKER KEY"
    assert (root / "crl/crl.pem").read_bytes() == CRL_V1
    # The CRL is fetched over TLS verified against the bundle from the secret.
    assert api.calls == [(CRL_URL, "CA BUNDLE")]
    assert SERVICE in out.get_container(CONTAINER).plan.services
    assert out.unit_status == testing.WaitingStatus("waiting for Mosquitto to listen")


def listener_sections(conf: str) -> dict[str, list[str]]:
    """The options of each listener block, by port; "global" holds the rest."""
    sections: dict[str, list[str]] = {"global": []}
    current = "global"
    for line in conf.splitlines():
        if line.startswith("listener "):
            current = line.split()[1]
            sections[current] = []
        elif line.strip():
            sections[current].append(line)
    return sections


def test_each_listener_has_its_own_acl():
    sections = listener_sections(render_config(1000000))
    # Mosquitto refuses per_listener_settings after any security option.
    assert sections["global"][0] == "per_listener_settings true"
    assert not any(o.startswith(("acl_file", "allow_anonymous")) for o in sections["global"])
    assert "acl_file /mosquitto/config/acl-devices" in sections["8883"]
    assert "crlfile /mosquitto/crl/crl.pem" in sections["8883"]
    assert "acl_file /mosquitto/config/acl-services" in sections["8884"]
    for port in ("8883", "8884"):
        assert "listener_allow_anonymous false" in sections[port]
        assert "require_certificate true" in sections[port]
        assert "use_identity_as_username true" in sections[port]


def test_devices_get_pattern_rules_only_and_services_only_their_own():
    # A device certificate with a service CN must get no service rights on
    # 8883, and a device certificate on 8884 must get nothing.
    assert all(line.startswith("pattern ") for line in charm.DEVICE_ACL.splitlines())
    users = [line.split()[1] for line in charm.SERVICE_ACL.splitlines() if line.startswith("user ")]
    assert users == ["fleetwire-bridge", "fleetwire-ingest"]
    assert "pattern" not in charm.SERVICE_ACL


def test_publishes_the_service_listener_to_clients(ctx, root, api):
    out = ctx.run(ctx.on.config_changed(), setup(root))
    mqtt = next(r for r in out.relations if r.endpoint == "mqtt")
    assert mqtt.local_app_data["service-url"] == "tls://mosquitto-k8s.test.svc.cluster.local:8884"


def test_does_not_start_without_a_crl(ctx, root, api):
    api.crl = None
    out = ctx.run(ctx.on.config_changed(), setup(root))
    assert SERVICE not in out.get_container(CONTAINER).plan.services
    assert out.unit_status == testing.WaitingStatus("waiting for the first CRL from the API")


def test_a_new_crl_is_loaded_with_sighup(ctx, root, api, signals):
    first = ctx.run(ctx.on.config_changed(), setup(root))
    assert signals == []  # the first start reads the file itself
    second = ctx.run(ctx.on.update_status(), first)
    assert signals == []  # the same list again: nothing to do
    api.crl = CRL_V2
    ctx.run(ctx.on.update_status(), second)
    assert signals == [("SIGHUP", (SERVICE,))]
    assert (root / "crl/crl.pem").read_bytes() == CRL_V2


def test_an_unreachable_api_keeps_the_current_crl(ctx, root, api, signals):
    first = ctx.run(ctx.on.config_changed(), setup(root))
    api.crl = None
    out = ctx.run(ctx.on.update_status(), first)
    assert (root / "crl/crl.pem").read_bytes() == CRL_V1
    assert SERVICE in out.get_container(CONTAINER).plan.services
    assert signals == []


def test_a_config_change_restarts_mosquitto(ctx, root, api):
    secret = testing.Secret(tracked_content=PKI)
    first = ctx.run(ctx.on.config_changed(), setup(root, secret=secret))
    before = first.get_container(CONTAINER).plan.services[SERVICE].environment
    out = ctx.run(ctx.on.config_changed(), setup(root, secret=secret, config={"max-queued-messages": 5000}))
    after = out.get_container(CONTAINER).plan.services[SERVICE].environment
    # A different layer makes replan restart Mosquitto with the new file.
    assert before != after
    assert "max_queued_messages 5000" in (root / "config/mosquitto.conf").read_text()


@pytest.mark.parametrize(
    ("args", "status"),
    [
        ({"pki": False}, testing.BlockedStatus("set pki-secret to the ID of the broker certificate secret")),
        (
            {"pki": {"tls-cert": "C", "tls-key": "K"}},
            testing.BlockedStatus("pki-secret lacks ca-bundle, device-trust, service-trust"),
        ),
        (
            {"crl": False},
            testing.BlockedStatus(
                "integrate with fleetwire-api-k8s: juju integrate mosquitto-k8s:crl fleetwire-api-k8s"
            ),
        ),
        ({"crl_url": ""}, testing.WaitingStatus("waiting for the CRL address from fleetwire-api-k8s")),
        (
            {"crl_url": "http://api/v1/crl"},
            testing.WaitingStatus("waiting for the CRL address from fleetwire-api-k8s"),
        ),
        ({"config": {"max-queued-messages": 0}}, testing.BlockedStatus("max-queued-messages must be at least 1")),
    ],
)
def test_reports_what_is_missing(ctx, root, api, args, status):
    out = ctx.run(ctx.on.config_changed(), setup(root, **args))
    assert out.unit_status == status
    assert SERVICE not in out.get_container(CONTAINER).plan.services


def test_active_when_listening(ctx, root, api, monkeypatch):
    monkeypatch.setattr(charm, "listening", lambda port: port == 8884)
    out = ctx.run(ctx.on.config_changed(), setup(root))
    assert out.unit_status == testing.ActiveStatus()
