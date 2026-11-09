# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""State-transition tests for the fleetwire-bridge charm (ops.testing)."""

from __future__ import annotations

import ops
import pytest
from ops import testing

import charm
from charm import CONTAINER, SERVICE, TOPIC, FleetwireBridgeCharm

PKI = {"tls-cert": "BRIDGE CERT", "tls-key": "BRIDGE KEY", "ca-bundle": "CA BUNDLE"}
KAFKA = {
    "endpoints": "kafka-k8s-0.kafka-k8s-endpoints:9092",
    "username": "relation-12",
    "password": "kafka-password",
    "tls": "disabled",
    "topic": TOPIC,
}


@pytest.fixture
def ctx() -> testing.Context:
    return testing.Context(FleetwireBridgeCharm)


@pytest.fixture(autouse=True)
def not_ready(monkeypatch):
    monkeypatch.setattr(charm, "readyz", lambda port: False)


def setup(*, kafka=None, mqtt=True, pki=None, config=None):
    secrets, relations, cfg = [], [], dict(config or {})
    if pki is not False:
        s = testing.Secret(tracked_content=pki or PKI)
        secrets.append(s)
        cfg.setdefault("pki-secret", s.id)
    if kafka is not False:
        relations.append(
            testing.Relation(
                "kafka",
                interface="kafka_client",
                remote_app_name="kafka-k8s",
                remote_app_data=KAFKA if kafka is None else kafka,
            )
        )
    if mqtt:
        relations.append(
            testing.Relation(
                "mqtt",
                interface="fleetwire_mqtt",
                remote_app_name="mosquitto-k8s",
                remote_app_data={"service-url": "tls://mosquitto-k8s.test.svc:8884"},
            )
        )
    return testing.State(
        leader=True,
        config=cfg,
        secrets=secrets,
        relations=relations,
        containers=[testing.Container(CONTAINER, can_connect=True)],
        model=testing.Model(name="test"),
    )


def test_requests_produce_rights_on_the_uplink_topic(ctx):
    st = setup(kafka={})
    kafka = next(r for r in st.relations if r.endpoint == "kafka")
    out = ctx.run(ctx.on.relation_created(kafka), st)
    data = out.get_relation(kafka.id).local_app_data
    assert data["topic"] == "fleetwire.uplink.v1"
    assert data["extra-user-roles"] == "producer"


def test_starts_with_kafka_and_the_broker(ctx):
    out = ctx.run(ctx.on.config_changed(), setup())
    env = out.get_container(CONTAINER).plan.services[SERVICE].environment
    assert env["FLEETWIRE_KAFKA_BROKERS"] == KAFKA["endpoints"]
    assert env["FLEETWIRE_KAFKA_SASL_MECHANISM"] == "SCRAM-SHA-512"
    assert env["FLEETWIRE_KAFKA_PASSWORD"] == "kafka-password"
    assert env["FLEETWIRE_MQTT_URL"] == "tls://mosquitto-k8s.test.svc:8884"
    assert env["FLEETWIRE_BRIDGE_RECEIVE_MAXIMUM"] == "1000"
    assert out.get_container(CONTAINER).plan.services[SERVICE].user_id == 65532
    fs = out.get_container(CONTAINER).get_filesystem(ctx)
    assert (fs / "etc/fleetwire/pki/bridge.pem").read_text() == "BRIDGE CERT"
    assert out.unit_status == testing.WaitingStatus("waiting for the bridge to become ready")


def test_active_when_the_bridge_reports_ready(ctx, monkeypatch):
    monkeypatch.setattr(charm, "readyz", lambda port: True)
    out = ctx.run(ctx.on.config_changed(), setup())
    assert out.unit_status == testing.ActiveStatus()


@pytest.mark.parametrize(
    ("args", "status"),
    [
        (
            {"kafka": False},
            testing.BlockedStatus("integrate with kafka-k8s: juju integrate fleetwire-bridge-k8s:kafka kafka-k8s"),
        ),
        (
            {"mqtt": False},
            testing.BlockedStatus(
                "integrate with mosquitto-k8s: juju integrate fleetwire-bridge-k8s:mqtt mosquitto-k8s"
            ),
        ),
        ({"kafka": {}}, testing.WaitingStatus("waiting for kafka relation data")),
        ({"pki": False}, testing.BlockedStatus("set pki-secret to the ID of the bridge certificate secret")),
        ({"config": {"receive-maximum": 0}}, testing.BlockedStatus("receive-maximum must be 1 to 65535, not 0")),
        (
            {"config": {"receive-maximum": 70000}},
            testing.BlockedStatus("receive-maximum must be 1 to 65535, not 70000"),
        ),
    ],
)
def test_reports_what_is_missing(ctx, args, status):
    out = ctx.run(ctx.on.config_changed(), setup(**args))
    assert out.unit_status == status
    assert SERVICE not in out.get_container(CONTAINER).plan.services


def test_losing_the_broker_stops_the_bridge(ctx):
    running = ctx.run(ctx.on.config_changed(), setup())
    mqtt = next(r for r in running.relations if r.endpoint == "mqtt")
    out = ctx.run(ctx.on.relation_broken(mqtt), running)
    assert out.get_container(CONTAINER).service_statuses[SERVICE] == ops.pebble.ServiceStatus.INACTIVE


def test_a_rotated_certificate_restarts_the_bridge(ctx):
    st = setup()
    running = ctx.run(ctx.on.config_changed(), st)
    secret = next(iter(running.secrets))
    rotated = testing.Secret(
        id=secret.id, tracked_content=secret.tracked_content, latest_content={**PKI, "tls-cert": "NEW CERT"}
    )
    st2 = testing.State(
        leader=True,
        config=running.config,
        relations=running.relations,
        secrets=[rotated],
        containers=running.containers,
        model=running.model,
    )
    out = ctx.run(ctx.on.secret_changed(rotated), st2)
    fs = out.get_container(CONTAINER).get_filesystem(ctx)
    assert (fs / "etc/fleetwire/pki/bridge.pem").read_text() == "NEW CERT"
