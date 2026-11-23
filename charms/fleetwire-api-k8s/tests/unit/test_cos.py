# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""COS Lite relations of the fleetwire-api charm: metrics, alert rules, dashboard and logs."""

from __future__ import annotations

import importlib.util
import json
import pathlib

import pytest
import yaml
from cosl import LZMABase64
from ops import pebble, testing

from charm import CONTAINER, FleetwireApiCharm

CHARM_DIR = pathlib.Path(__file__).resolve().parents[2]
REPO = CHARM_DIR.parents[1]
LOKI_URL = "http://loki-k8s-0.loki-k8s-endpoints.cos.svc.cluster.local:3100/loki/api/v1/push"


@pytest.fixture
def ctx(tmp_path: pathlib.Path) -> testing.Context:
    # The libraries read the rules and the dashboard under the charm
    # directory; a virtual one with the real src/ keeps the test from
    # writing into it.
    (tmp_path / "src").symlink_to(CHARM_DIR / "src")
    return testing.Context(FleetwireApiCharm, charm_root=tmp_path)


def state(*relations: testing.RelationBase, **kw) -> testing.State:
    kw.setdefault("containers", [testing.Container(CONTAINER, can_connect=True)])
    kw.setdefault("leader", True)
    return testing.State(relations=relations, model=testing.Model(name="test"), **kw)


def scrape(**kw) -> testing.Relation:
    return testing.Relation("metrics-endpoint", interface="prometheus_scrape", remote_app_name="prometheus-k8s", **kw)


def loki() -> testing.Relation:
    return testing.Relation(
        "logging",
        interface="loki_push_api",
        remote_app_name="loki-k8s",
        remote_units_data={0: {"endpoint": json.dumps({"url": LOKI_URL})}},
    )


def rule_names(rules: dict) -> set[str]:
    return {r.get("alert") or r["record"] for g in rules["groups"] for r in g["rules"]}


def test_scrape_job_reads_the_admin_listener_on_the_pod(ctx):
    rel = scrape()
    out = ctx.run(ctx.on.relation_joined(rel, remote_unit=0), state(rel))
    rel_out = out.get_relation(rel.id)
    (job,) = json.loads(rel_out.local_app_data["scrape_jobs"])
    assert job["job_name"] == "fleetwire-api"
    assert job["metrics_path"] == "/metrics"
    assert job["static_configs"] == [{"targets": ["*:9090"]}]
    # The rules select on the job names of deploy/prometheus/prometheus.yml.
    assert job["relabel_configs"] == [{"target_label": "job", "replacement": "fleetwire-api"}]
    # "*" becomes this address: the pod's, as no Kubernetes service has 9090.
    assert rel_out.local_unit_data["prometheus_scrape_unit_address"] == "192.0.2.0"
    assert json.loads(rel_out.local_app_data["scrape_metadata"])["application"] == "fleetwire-api-k8s"


def test_alert_rules_are_the_api_share_of_deploy_prometheus_rules(ctx):
    rel = scrape()
    out = ctx.run(ctx.on.relation_joined(rel, remote_unit=0), state(rel))
    rules = json.loads(out.get_relation(rel.id).local_app_data["alert_rules"])
    shipped = yaml.safe_load((CHARM_DIR / "src/prometheus_alert_rules/fleetwire.yml").read_text())
    # The library adds its generic HostDown and HostMetricsMissing rules.
    assert rule_names(rules) == rule_names(shipped) | {"HostDown", "HostMetricsMissing"}
    assert {
        "fleetwire:api_errors:ratio_rate5m",
        "FleetwireAPIBudgetBurnFast",
        "FleetwireCRLStale",
        "FleetwireCRLNotPublished",
        "FleetwireServiceDown",
    } <= rule_names(rules)
    assert "FleetwireFreshnessBudgetBurnFast" not in rule_names(rules)
    # Prometheus restricts each expression to the labels' application.
    for group in rules["groups"]:
        for rule in group["rules"]:
            assert rule["labels"]["juju_application"] == "fleetwire-api-k8s"
            assert rule["labels"]["juju_model"] == "test"


def test_only_the_leader_publishes_the_job(ctx):
    rel = scrape()
    out = ctx.run(ctx.on.relation_joined(rel, remote_unit=0), state(rel, leader=False))
    rel_out = out.get_relation(rel.id)
    assert "scrape_jobs" not in rel_out.local_app_data
    assert rel_out.local_unit_data["prometheus_scrape_unit_address"] == "192.0.2.0"


def test_dashboard_is_the_fleetwire_dashboard(ctx):
    rel = testing.Relation("grafana-dashboard", interface="grafana_dashboard", remote_app_name="grafana-k8s")
    out = ctx.run(ctx.on.relation_created(rel), state(rel))
    templates = json.loads(out.get_relation(rel.id).local_app_data["dashboards"])["templates"]
    (template,) = templates.values()
    dashboard = json.loads(LZMABase64.decompress(template["content"]))
    source = json.loads((REPO / "deploy/grafana/dashboards/fleetwire.json").read_text())
    assert dashboard["title"] == source["title"]
    assert [p["title"] for p in dashboard["panels"]] == [p["title"] for p in source["panels"]]
    assert "charm: fleetwire-api-k8s" in dashboard["tags"]
    # Grafana adds the Juju topology variables and points the panels at its
    # own Prometheus data source.
    assert template["inject_dropdowns"] is True


def test_logs_go_to_every_loki_unit(ctx):
    rel = loki()
    out = ctx.run(ctx.on.relation_changed(rel, remote_unit=0), state(rel))
    target = out.get_container(CONTAINER).plan.log_targets["loki-k8s/0"]
    assert (target.type, target.location, target.services) == ("loki", LOKI_URL, ["all"])
    assert target.labels["juju_application"] == "fleetwire-api-k8s"


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


def test_copies_match_deploy_sources():
    spec = importlib.util.spec_from_file_location("cos_files", REPO / "charms/cos_files.py")
    assert spec is not None
    assert spec.loader is not None
    cos_files = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cos_files)
    stale = cos_files.drifted(CHARM_DIR.name)
    assert not stale, f"out of date: {stale}; run charms/cos_files.py"
