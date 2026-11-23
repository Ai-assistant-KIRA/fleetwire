# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""COS Lite relation of the mosquitto charm: log forwarding to Loki."""

from __future__ import annotations

import json

import pytest
from ops import pebble, testing

from charm import CONTAINER, MosquittoCharm

LOKI_URL = "http://loki-k8s-0.loki-k8s-endpoints.cos.svc.cluster.local:3100/loki/api/v1/push"


@pytest.fixture
def ctx() -> testing.Context:
    return testing.Context(MosquittoCharm)


def state(*relations: testing.RelationBase, **kw) -> testing.State:
    kw.setdefault("containers", [testing.Container(CONTAINER, can_connect=True)])
    kw.setdefault("leader", True)
    return testing.State(relations=relations, model=testing.Model(name="test"), **kw)


def loki(units: int = 1) -> testing.Relation:
    return testing.Relation(
        "logging",
        interface="loki_push_api",
        remote_app_name="loki-k8s",
        remote_units_data={
            n: {"endpoint": json.dumps({"url": LOKI_URL.replace("loki-k8s-0", f"loki-k8s-{n}")})} for n in range(units)
        },
    )


def test_logs_go_to_every_loki_unit(ctx):
    rel = loki(units=2)
    out = ctx.run(ctx.on.relation_changed(rel, remote_unit=0), state(rel))
    targets = out.get_container(CONTAINER).plan.log_targets
    assert sorted(targets) == ["loki-k8s/0", "loki-k8s/1"]
    assert targets["loki-k8s/1"].location == LOKI_URL.replace("loki-k8s-0", "loki-k8s-1")
    assert targets["loki-k8s/0"].services == ["all"]
    assert targets["loki-k8s/0"].labels["juju_application"] == "mosquitto-k8s"


def test_log_forwarding_starts_with_the_container(ctx):
    rel = loki()
    out = ctx.run(ctx.on.pebble_ready(testing.Container(CONTAINER, can_connect=True)), state(rel))
    assert out.get_container(CONTAINER).plan.log_targets["loki-k8s/0"].location == LOKI_URL


def test_log_forwarding_stops_when_loki_leaves(ctx):
    rel = loki()
    target = {"override": "replace", "type": "loki", "location": LOKI_URL, "services": ["all"]}
    forwarding = testing.Container(
        CONTAINER,
        can_connect=True,
        layers={f"{CONTAINER}-log-forwarding": pebble.Layer({"log-targets": {"loki-k8s/0": target}})},
    )
    out = ctx.run(ctx.on.relation_broken(rel), state(rel, containers=[forwarding]))
    assert out.get_container(CONTAINER).plan.log_targets["loki-k8s/0"].services == ["-all"]


def test_no_metrics_endpoint(ctx):
    # Mosquitto serves no Prometheus metrics; the charm offers logs only.
    assert "metrics-endpoint" not in ctx.charm_spec.meta.get("provides", {})
