# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""State-transition tests for the fleetwire-api charm (ops.testing)."""

from __future__ import annotations

import dataclasses

import ops
import pytest
from ops import testing

import charm
from charm import BINARY, CONTAINER, PEER, SERVICE, FleetwireApiCharm

ROOT_KEY = "ab" * 32
PKI = {
    "tls-cert": "API CERT",
    "tls-key": "API KEY",
    "ca-bundle": "CA BUNDLE",
    "device-trust": "DEVICE TRUST",
    "device-ca-cert": "DEVICE CA",
    "device-ca-key": "DEVICE CA KEY",
    "service-trust": "SERVICE TRUST",
    "keyring-root-key": ROOT_KEY,
}
ENDPOINTS = "postgresql-k8s-primary.test.svc.cluster.local:5432"
OWNER = {"endpoints": ENDPOINTS, "database": "fleetwire", "username": "relation_id_7", "password": "owner-pw"}
RUNTIME = {"endpoints": ENDPOINTS, "database": "fleetwire", "username": "relation_id_9", "password": "runtime-pw"}


@pytest.fixture
def ctx() -> testing.Context:
    return testing.Context(FleetwireApiCharm)


@pytest.fixture(autouse=True)
def not_ready(monkeypatch):
    """The workload's /readyz answers no unless a test says otherwise."""
    monkeypatch.setattr(charm, "readyz", lambda port: False)


def pki_secret(**changes: str) -> testing.Secret:
    content = {k: v for k, v in {**PKI, **changes}.items() if v}
    return testing.Secret(tracked_content=content)


def owner_db(data: dict[str, str] | None = None) -> testing.Relation:
    return testing.Relation(
        "database",
        interface="postgresql_client",
        remote_app_name="postgresql-k8s",
        remote_app_data=OWNER if data is None else data,
    )


def runtime_db(data: dict[str, str] | None = None) -> testing.Relation:
    return testing.Relation(
        "runtime-database",
        interface="postgresql_client",
        remote_app_name="postgresql-k8s",
        remote_app_data=RUNTIME if data is None else data,
    )


def container(migrate_code: int = 0) -> testing.Container:
    return testing.Container(
        CONTAINER,
        can_connect=True,
        execs={
            testing.Exec(
                [BINARY, "migrate"],
                return_code=migrate_code,
                stdout="applied 00001 00001_schema.sql (1s)\n",
                stderr="" if migrate_code == 0 else "connection refused",
            )
        },
    )


def state(secret: testing.Secret | None = None, *, config: dict | None = None, relations=None, **kw) -> testing.State:
    """Everything ready unless a test changes it."""
    secrets = [secret] if secret else []
    cfg = {"device-broker-url": "tls://broker.example:8883"}
    if secret:
        cfg["pki-secret"] = secret.id
    cfg.update(config or {})
    rels = [testing.PeerRelation(PEER), *(relations if relations is not None else [owner_db(), runtime_db()])]
    kw.setdefault("containers", [container()])
    kw.setdefault("leader", True)
    return testing.State(config=cfg, secrets=secrets, relations=rels, model=testing.Model(name="test"), **kw)


def env_of(out: testing.State) -> dict[str, str]:
    return out.get_container(CONTAINER).plan.services[SERVICE].environment


def peer_of(out: testing.State) -> testing.PeerRelation:
    return next(r for r in out.relations if r.endpoint == PEER)


def test_blocked_until_the_pki_secret_is_set(ctx):
    out = ctx.run(ctx.on.config_changed(), state())
    assert out.unit_status == testing.BlockedStatus("set pki-secret to the ID of the PKI secret (see the deploy guide)")
    assert SERVICE not in out.get_container(CONTAINER).plan.services


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"device-ca-key": ""}, "pki-secret lacks device-ca-key"),
        ({"tls-cert": "", "service-trust": ""}, "pki-secret lacks service-trust, tls-cert"),
        ({"keyring-root-key": "abc"}, "pki-secret keyring-root-key must be 64 hex characters (32 bytes)"),
        ({"keyring-root-key": "zz" * 32}, "pki-secret keyring-root-key must be 64 hex characters (32 bytes)"),
    ],
)
def test_incomplete_pki_secret_blocks_with_the_fix(ctx, changes, message):
    out = ctx.run(ctx.on.config_changed(), state(pki_secret(**changes)))
    assert out.unit_status == testing.BlockedStatus(message)


@pytest.mark.parametrize(
    ("relations", "status"),
    [
        (
            [runtime_db()],
            testing.BlockedStatus(
                "integrate with postgresql-k8s: juju integrate fleetwire-api-k8s:database postgresql-k8s"
            ),
        ),
        (
            [owner_db()],
            testing.BlockedStatus(
                "integrate with postgresql-k8s: juju integrate fleetwire-api-k8s:runtime-database postgresql-k8s"
            ),
        ),
        ([owner_db({}), runtime_db()], testing.WaitingStatus("waiting for the database relation")),
        ([owner_db(), runtime_db({})], testing.WaitingStatus("waiting for the runtime-database relation")),
    ],
)
def test_needs_both_database_relations(ctx, relations, status):
    out = ctx.run(ctx.on.config_changed(), state(pki_secret(), relations=relations))
    assert out.unit_status == status
    assert not ctx.exec_history.get(CONTAINER)
    assert SERVICE not in out.get_container(CONTAINER).plan.services


def test_blocked_without_a_device_broker_url(ctx):
    out = ctx.run(ctx.on.config_changed(), state(pki_secret(), config={"device-broker-url": ""}))
    assert out.unit_status == testing.BlockedStatus("set device-broker-url (the broker address devices use)")


def test_leader_migrates_as_owner_and_the_service_runs_as_its_own_login(ctx):
    out = ctx.run(ctx.on.config_changed(), state(pki_secret()))

    (call,) = ctx.exec_history[CONTAINER]
    assert call.command == [BINARY, "migrate"]
    assert call.environment == {
        "FLEETWIRE_DATABASE_OWNER_URL": f"postgres://relation_id_7:owner-pw@{ENDPOINTS}/fleetwire?sslmode=prefer",
        # The policies and grants go to the group every runtime login is in.
        "FLEETWIRE_DATABASE_RUNTIME_ROLE": "charmed_dml",
    }
    env = env_of(out)
    assert env["FLEETWIRE_DATABASE_URL"] == f"postgres://relation_id_9:runtime-pw@{ENDPOINTS}/fleetwire?sslmode=prefer"
    # The owner's credentials never reach the service.
    assert "relation_id_7" not in str(env)
    assert "owner-pw" not in str(env)
    assert env["FLEETWIRE_DEVICE_BROKER_URL"] == "tls://broker.example:8883"
    assert env["FLEETWIRE_DEVICE_API_URL"] == "https://fleetwire-api-k8s.test.svc.cluster.local:8444"
    assert env["FLEETWIRE_API_INTERNAL_LISTEN"] == ":8445"
    assert out.unit_status == testing.WaitingStatus("waiting for the API to become ready")


def test_requests_the_runtime_login_with_the_charmed_dml_role(ctx):
    rt = runtime_db({})
    out = ctx.run(ctx.on.relation_created(rt), state(pki_secret(), relations=[owner_db({}), rt]))
    data = out.get_relation(rt.id).local_app_data
    assert data["database"] == "fleetwire"
    assert data["extra-user-roles"] == "charmed_dml"


def test_requests_the_owner_without_extra_roles(ctx):
    own = owner_db({})
    out = ctx.run(ctx.on.relation_created(own), state(pki_secret(), relations=[own, runtime_db({})]))
    data = out.get_relation(own.id).local_app_data
    assert data["database"] == "fleetwire"
    assert "extra-user-roles" not in data


def test_active_once_the_api_reports_ready(ctx, monkeypatch):
    asked = []
    monkeypatch.setattr(charm, "readyz", lambda port: asked.append(port) or True)
    out = ctx.run(ctx.on.config_changed(), state(pki_secret()))
    assert out.unit_status == testing.ActiveStatus()
    assert asked == [9090]


def test_workload_files_are_written_for_the_service_user(ctx):
    out = ctx.run(ctx.on.config_changed(), state(pki_secret()))
    fs = out.get_container(CONTAINER).get_filesystem(ctx)
    assert (fs / "etc/fleetwire/pki/api.pem").read_text() == "API CERT"
    assert (fs / "etc/fleetwire/pki/device-ca.key").read_text() == "DEVICE CA KEY"
    assert (fs / "etc/fleetwire/pki/keyring-root.key").read_bytes() == bytes.fromhex(ROOT_KEY)
    for sub in ("keyring", "crl", "exports"):
        assert (fs / "var/lib/fleetwire" / sub).is_dir()


def rotated(running: testing.State, **changes: str) -> testing.State:
    """The running state with the PKI secret's latest revision changed."""
    secret = next(iter(running.secrets))
    return dataclasses.replace(running, secrets=[dataclasses.replace(secret, latest_content={**PKI, **changes})])


@pytest.mark.parametrize(
    "changes",
    [
        {"tls-cert": "NEW API CERT"},
        {"device-ca-key": "NEW DEVICE CA KEY"},
        {"keyring-root-key": "cd" * 32},
    ],
)
def test_a_rotated_secret_restarts_the_api(ctx, changes):
    running = ctx.run(ctx.on.config_changed(), state(pki_secret()))
    before = env_of(running)
    st = rotated(running, **changes)
    out = ctx.run(ctx.on.secret_changed(next(iter(st.secrets))), st)
    after = env_of(out)
    # The API reads these files once. Pebble's replan restarts a service
    # whose definition changed, and only the digest of the files changes it.
    assert after["FLEETWIRE_CONFIG_SHA256"] != before["FLEETWIRE_CONFIG_SHA256"]
    assert {k: v for k, v in after.items() if k != "FLEETWIRE_CONFIG_SHA256"} == {
        k: v for k, v in before.items() if k != "FLEETWIRE_CONFIG_SHA256"
    }


def test_a_new_postgresql_ca_restarts_the_api(ctx):
    rels = [
        owner_db({**OWNER, "tls": "True", "tls-ca": "PG CA 1"}),
        runtime_db({**RUNTIME, "tls": "True", "tls-ca": "PG CA 1"}),
    ]
    running = ctx.run(ctx.on.config_changed(), state(pki_secret(), relations=rels))
    renewed = [
        dataclasses.replace(r, remote_app_data={**r.remote_app_data, "tls-ca": "PG CA 2"})
        if r.endpoint in ("database", "runtime-database")
        else r
        for r in running.relations
    ]
    out = ctx.run(ctx.on.update_status(), dataclasses.replace(running, relations=renewed))
    assert env_of(out)["FLEETWIRE_CONFIG_SHA256"] != env_of(running)["FLEETWIRE_CONFIG_SHA256"]


def test_an_unchanged_secret_keeps_the_api_running(ctx):
    running = ctx.run(ctx.on.config_changed(), state(pki_secret()))
    out = ctx.run(ctx.on.update_status(), running)
    assert out.get_container(CONTAINER).plan == running.get_container(CONTAINER).plan


def test_failed_migrations_keep_the_service_down(ctx):
    out = ctx.run(ctx.on.config_changed(), state(pki_secret(), containers=[container(migrate_code=1)]))
    assert SERVICE not in out.get_container(CONTAINER).plan.services
    assert "migrated" not in peer_of(out).local_app_data
    assert out.unit_status == testing.WaitingStatus("waiting for migrations on the leader")


def test_migrations_run_once_per_owner_and_again_after_an_upgrade(ctx):
    first = ctx.run(ctx.on.config_changed(), state(pki_secret()))
    assert len(ctx.exec_history[CONTAINER]) == 1
    second = ctx.run(ctx.on.update_status(), first)
    assert len(ctx.exec_history[CONTAINER]) == 1
    ctx.run(ctx.on.upgrade_charm(), second)
    assert len(ctx.exec_history[CONTAINER]) == 2


def test_tells_ingest_and_the_broker_where_things_are(ctx):
    ingest = testing.Relation("ingest", interface="fleetwire_ingest", remote_app_name="fleetwire-ingest-k8s")
    crl = testing.Relation("crl", interface="fleetwire_crl", remote_app_name="mosquitto-k8s")
    out = ctx.run(ctx.on.config_changed(), state(pki_secret(), relations=[owner_db(), runtime_db(), ingest, crl]))
    assert out.get_relation(ingest.id).local_app_data == {
        "keyring-url": "https://fleetwire-api-k8s.test.svc.cluster.local:8445",
        "schema-ready": "true",
    }
    assert out.get_relation(crl.id).local_app_data["crl-url"] == (
        "https://fleetwire-api-k8s.test.svc.cluster.local:8444/v1/crl"
    )


def test_ingest_is_not_told_the_schema_exists_before_migrations(ctx):
    ingest = testing.Relation("ingest", interface="fleetwire_ingest", remote_app_name="fleetwire-ingest-k8s")
    st = state(pki_secret(), relations=[owner_db(), runtime_db(), ingest], containers=[container(migrate_code=1)])
    out = ctx.run(ctx.on.config_changed(), st)
    assert "schema-ready" not in out.get_relation(ingest.id).local_app_data


def test_database_tls_is_verified_when_postgresql_offers_it(ctx):
    rels = [
        owner_db({**OWNER, "tls": "True", "tls-ca": "PG CA"}),
        runtime_db({**RUNTIME, "tls": "True", "tls-ca": "PG CA"}),
    ]
    out = ctx.run(ctx.on.config_changed(), state(pki_secret(), relations=rels))
    url = env_of(out)["FLEETWIRE_DATABASE_URL"]
    assert url.endswith("?sslmode=verify-full&sslrootcert=/etc/fleetwire/pki/postgresql-ca.pem")
    fs = out.get_container(CONTAINER).get_filesystem(ctx)
    assert (fs / "etc/fleetwire/pki/postgresql-ca.pem").read_text() == "PG CA"


def test_credentials_in_a_relation_secret_are_used(ctx):
    # Charmed PostgreSQL on Juju 3 sends the user and password in a secret.
    user = testing.Secret(tracked_content={"username": "relation_id_11", "password": "from-secret"})
    own = owner_db({"endpoints": ENDPOINTS, "database": "fleetwire", "secret-user": user.id})
    st = state(pki_secret(), relations=[own, runtime_db()])
    st = testing.State(
        config=st.config,
        relations=st.relations,
        containers=st.containers,
        leader=True,
        secrets=[*st.secrets, user],
        model=st.model,
    )
    ctx.run(ctx.on.config_changed(), st)
    (call,) = ctx.exec_history[CONTAINER]
    assert call.environment["FLEETWIRE_DATABASE_OWNER_URL"].startswith("postgres://relation_id_11:from-secret@")


def test_more_than_one_unit_is_refused(ctx):
    out = ctx.run(ctx.on.config_changed(), state(pki_secret(), planned_units=2))
    assert out.unit_status == testing.BlockedStatus(
        "scale to 1 unit: the local keyring keeps the tenant keys on one unit (planned 2)"
    )
    assert not ctx.exec_history.get(CONTAINER)


def test_losing_the_runtime_login_stops_the_service(ctx):
    running = ctx.run(ctx.on.config_changed(), state(pki_secret()))
    rt = next(r for r in running.relations if r.endpoint == "runtime-database")
    out = ctx.run(ctx.on.relation_broken(rt), running)
    assert out.get_container(CONTAINER).service_statuses[SERVICE] == ops.pebble.ServiceStatus.INACTIVE


def test_losing_the_owner_forgets_the_migrations_but_keeps_serving(ctx):
    ingest = testing.Relation("ingest", interface="fleetwire_ingest", remote_app_name="fleetwire-ingest-k8s")
    running = ctx.run(ctx.on.config_changed(), state(pki_secret(), relations=[owner_db(), runtime_db(), ingest]))
    own = next(r for r in running.relations if r.endpoint == "database")
    out = ctx.run(ctx.on.relation_broken(own), running)
    assert "migrated" not in peer_of(out).local_app_data
    assert "schema-ready" not in out.get_relation(ingest.id).local_app_data
    assert out.get_container(CONTAINER).service_statuses[SERVICE] == ops.pebble.ServiceStatus.ACTIVE


def test_non_leader_waits_for_the_leader(ctx):
    out = ctx.run(ctx.on.config_changed(), state(pki_secret(), leader=False))
    assert not ctx.exec_history.get(CONTAINER)
    assert out.unit_status == testing.WaitingStatus("waiting for migrations on the leader")


def test_create_operator_token_action_returns_the_token_once(ctx):
    running = ctx.run(ctx.on.config_changed(), state(pki_secret()))
    token = "fwo_" + "0" * 32 + "_" + "a" * 52
    c = running.get_container(CONTAINER)
    c = testing.Container(
        CONTAINER,
        can_connect=True,
        layers=c.layers,
        service_statuses=c.service_statuses,
        execs={testing.Exec([BINARY, "create-operator-token"], stdout=token + "\n")},
    )
    st = testing.State(
        leader=True,
        config=running.config,
        relations=running.relations,
        secrets=running.secrets,
        containers=[c],
        model=running.model,
    )
    ctx.run(ctx.on.action("create-operator-token", params={"name": "ops-alice"}), st)
    assert ctx.action_results == {"token": token}
    (call,) = ctx.exec_history[CONTAINER][-1:]
    assert call.command == [BINARY, "create-operator-token", "-name", "ops-alice"]
    assert call.environment["FLEETWIRE_DATABASE_OWNER_URL"].startswith("postgres://relation_id_7:owner-pw@")


def test_create_operator_token_fails_before_migrations(ctx):
    st = state(pki_secret(), relations=[owner_db({}), runtime_db()])
    with pytest.raises(testing.ActionFailed, match="not ready or not migrated"):
        ctx.run(ctx.on.action("create-operator-token", params={"name": "ops"}), st)
