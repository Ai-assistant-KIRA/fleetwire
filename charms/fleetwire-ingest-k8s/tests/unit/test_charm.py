# Copyright 2026 Reda Alaarabi
# SPDX-License-Identifier: Apache-2.0
"""State-transition tests for the fleetwire-ingest charm (ops.testing)."""

from __future__ import annotations

import dataclasses

import ops
import pytest
from ops import testing

import charm
from charm import BINARY, CONTAINER, PEER, SERVICE, FleetwireIngestCharm

PKI = {"tls-cert": "INGEST CERT", "tls-key": "INGEST KEY", "ca-bundle": "CA BUNDLE"}
KAFKA = {
    "endpoints": "kafka-k8s-0.kafka-k8s-endpoints:9092",
    "username": "relation-11",
    "password": "fake-kafka-password",
    "tls": "disabled",
    "consumer-group-prefix": "fleetwire-",
    "topic": "fleetwire.*",
}
DB = {
    "endpoints": "postgresql-k8s-primary.test.svc.cluster.local:5432",
    "database": "fleetwire",
    "username": "relation_id_12",
    "password": "ingest-db-password",
}
API = {"keyring-url": "https://fleetwire-api-k8s.test.svc:8445", "schema-ready": "true"}


@pytest.fixture
def ctx() -> testing.Context:
    return testing.Context(FleetwireIngestCharm)


@pytest.fixture(autouse=True)
def not_ready(monkeypatch):
    monkeypatch.setattr(charm, "readyz", lambda port: False)


def setup(*, kafka=None, db=None, api=None, mqtt=True, pki=None, leader=True, topics_code=0, config=None, peer=None):
    """Builds a state with every input ready unless a test turns one off."""
    secrets, relations = [], [testing.PeerRelation(PEER, local_app_data=peer or {})]
    cfg = dict(config or {})
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
    if db is not False:
        relations.append(
            testing.Relation(
                "database",
                interface="postgresql_client",
                remote_app_name="postgresql-k8s",
                remote_app_data=DB if db is None else db,
            )
        )
    if api is not False:
        relations.append(
            testing.Relation(
                "api",
                interface="fleetwire_ingest",
                remote_app_name="fleetwire-api-k8s",
                remote_app_data=API if api is None else api,
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
    container = testing.Container(
        CONTAINER, can_connect=True, execs={testing.Exec([BINARY, "ensure-topics"], return_code=topics_code)}
    )
    return testing.State(
        leader=leader,
        config=cfg,
        secrets=secrets,
        relations=relations,
        containers=[container],
        model=testing.Model(name="test"),
    )


def env_of(out: testing.State) -> dict[str, str]:
    return out.get_container(CONTAINER).plan.services[SERVICE].environment


def read(ctx: testing.Context, out: testing.State, path: str) -> str:
    return (out.get_container(CONTAINER).get_filesystem(ctx) / path.lstrip("/")).read_text()


def test_starts_with_its_database_login_kafka_keyring_and_broker(ctx):
    out = ctx.run(ctx.on.config_changed(), setup())
    env = env_of(out)
    assert env["FLEETWIRE_DATABASE_URL_FILE"] == "/etc/fleetwire/secrets/database-url"
    assert read(ctx, out, env["FLEETWIRE_DATABASE_URL_FILE"]) == (
        "postgres://relation_id_12:ingest-db-password@postgresql-k8s-primary.test.svc.cluster.local:5432/fleetwire"
        "?sslmode=prefer"
    )
    assert read(ctx, out, env["FLEETWIRE_KAFKA_PASSWORD_FILE"]) == "fake-kafka-password"
    assert env["FLEETWIRE_KAFKA_BROKERS"] == KAFKA["endpoints"]
    assert env["FLEETWIRE_KAFKA_SASL_MECHANISM"] == "SCRAM-SHA-512"
    assert env["FLEETWIRE_KAFKA_USERNAME"] == "relation-11"
    assert env["FLEETWIRE_KAFKA_TLS"] == "false"
    # The group carries the prefix Kafka granted READ on.
    assert env["FLEETWIRE_KAFKA_GROUP"] == "fleetwire-ingest"
    assert env["FLEETWIRE_KEYRING_URL"] == "https://fleetwire-api-k8s.test.svc:8445"
    assert "FLEETWIRE_KEYRING_DIR" not in env
    assert env["FLEETWIRE_MQTT_URL"] == "tls://mosquitto-k8s.test.svc:8884"
    assert env["FLEETWIRE_INSTANCE_ID"] == "fleetwire-ingest-k8s-0"
    fs = out.get_container(CONTAINER).get_filesystem(ctx)
    assert (fs / "etc/fleetwire/pki/ingest.key").read_text() == "INGEST KEY"
    assert out.unit_status == testing.WaitingStatus("waiting for ingest to become ready")


def test_the_passwords_stay_out_of_the_plan_and_the_topic_command(ctx):
    out = ctx.run(ctx.on.config_changed(), setup())
    c = out.get_container(CONTAINER)
    # Anyone who can run `pebble plan` sees the layer.
    plan = c.plan.to_yaml()
    assert DB["password"] not in plan
    assert KAFKA["password"] not in plan
    (call,) = ctx.exec_history[CONTAINER]
    assert KAFKA["password"] not in str(call.environment)
    fs = c.get_filesystem(ctx)
    for name in ("database-url", "kafka-password"):
        assert (fs / "etc/fleetwire/secrets" / name).stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("relation", ["database", "kafka"])
def test_a_new_password_restarts_ingest(ctx, relation):
    running = ctx.run(ctx.on.config_changed(), setup())
    renewed = [
        dataclasses.replace(r, remote_app_data={**r.remote_app_data, "password": "new-password"})
        if r.endpoint == relation
        else r
        for r in running.relations
    ]
    out = ctx.run(ctx.on.update_status(), dataclasses.replace(running, relations=renewed))
    assert env_of(out)["FLEETWIRE_CONFIG_SHA256"] != env_of(running)["FLEETWIRE_CONFIG_SHA256"]


def test_opens_no_port_on_the_service(ctx):
    st = dataclasses.replace(setup(), opened_ports=[testing.TCPPort(9090)])
    out = ctx.run(ctx.on.config_changed(), st)
    assert not out.opened_ports


def test_leader_creates_the_topics_once(ctx, monkeypatch):
    # Records the password file's content when the command starts.
    passwords = []
    real_exec = ops.Container.exec

    def exec_reading_the_password(self, command, **kw):
        passwords.append(self.pull(kw["environment"]["FLEETWIRE_KAFKA_PASSWORD_FILE"]).read())
        return real_exec(self, command, **kw)

    monkeypatch.setattr(ops.Container, "exec", exec_reading_the_password)
    first = ctx.run(ctx.on.config_changed(), setup())
    calls = ctx.exec_history[CONTAINER]
    assert [c.command for c in calls] == [[BINARY, "ensure-topics"]]
    assert calls[0].environment["FLEETWIRE_KAFKA_REPLICATION"] == "3"
    assert (calls[0].user_id, calls[0].group_id) == (65532, 65532)
    assert passwords == ["fake-kafka-password"]
    # It needs Kafka only: no database login, keyring or MQTT identity.
    unneeded = ("FLEETWIRE_DATABASE", "FLEETWIRE_KEYRING", "FLEETWIRE_MQTT")
    assert not [k for k in calls[0].environment if k.startswith(unneeded)]
    ctx.run(ctx.on.update_status(), first)
    assert len(ctx.exec_history[CONTAINER]) == 1


def test_failed_topic_creation_keeps_ingest_down(ctx):
    out = ctx.run(ctx.on.config_changed(), setup(topics_code=1))
    assert SERVICE not in out.get_container(CONTAINER).plan.services


def peer_of(out: testing.State) -> testing.PeerRelation:
    return next(r for r in out.relations if r.endpoint == PEER)


def test_non_leaders_wait_for_the_leader_to_create_the_topics(ctx):
    out = ctx.run(ctx.on.config_changed(), setup(leader=False))
    assert not ctx.exec_history.get(CONTAINER)
    assert SERVICE not in out.get_container(CONTAINER).plan.services
    assert out.unit_status == testing.WaitingStatus("waiting for the leader to create the Kafka topics")


def test_non_leaders_start_when_the_leader_records_the_topics(ctx):
    leader = ctx.run(ctx.on.config_changed(), setup())
    published = dict(peer_of(leader).local_app_data)
    st = setup(leader=False, peer=published)
    # The leader, unit 1, wrote the topics into the peer application data.
    peer = dataclasses.replace(next(r for r in st.relations if r.endpoint == PEER), peers_data={1: {}})
    st = dataclasses.replace(st, relations=[peer, *(r for r in st.relations if r.endpoint != PEER)])
    out = ctx.run(ctx.on.relation_changed(peer, remote_unit=1), st)
    # Only the leader ran ensure-topics.
    assert len(ctx.exec_history[CONTAINER]) == 1
    assert SERVICE in out.get_container(CONTAINER).plan.services
    assert out.unit_status == testing.WaitingStatus("waiting for ingest to become ready")


def test_topics_for_another_kafka_do_not_count(ctx):
    leader = ctx.run(ctx.on.config_changed(), setup())
    published = dict(peer_of(leader).local_app_data)
    out = ctx.run(ctx.on.config_changed(), setup(leader=False, peer=published, config={"kafka-replication": 1}))
    assert SERVICE not in out.get_container(CONTAINER).plan.services


@pytest.mark.parametrize("changes", [{"tls-cert": "NEW INGEST CERT"}, {"ca-bundle": "NEW CA BUNDLE"}])
def test_a_rotated_certificate_restarts_ingest(ctx, changes):
    running = ctx.run(ctx.on.config_changed(), setup())
    before = env_of(running)
    secret = next(iter(running.secrets))
    rotated = dataclasses.replace(secret, latest_content={**PKI, **changes})
    out = ctx.run(ctx.on.secret_changed(rotated), dataclasses.replace(running, secrets=[rotated]))
    after = env_of(out)
    # Ingest reads these files once. Pebble's replan restarts a service whose
    # definition changed, and only the digest of the files changes it here.
    assert after["FLEETWIRE_CONFIG_SHA256"] != before["FLEETWIRE_CONFIG_SHA256"]
    assert {k: v for k, v in after.items() if k != "FLEETWIRE_CONFIG_SHA256"} == {
        k: v for k, v in before.items() if k != "FLEETWIRE_CONFIG_SHA256"
    }


def test_a_new_database_ca_restarts_ingest(ctx):
    running = ctx.run(ctx.on.config_changed(), setup(db={**DB, "tls": "True", "tls-ca": "PG CA 1"}))
    renewed = [
        dataclasses.replace(r, remote_app_data={**r.remote_app_data, "tls-ca": "PG CA 2"})
        if r.endpoint == "database"
        else r
        for r in running.relations
    ]
    out = ctx.run(ctx.on.update_status(), dataclasses.replace(running, relations=renewed))
    assert env_of(out)["FLEETWIRE_CONFIG_SHA256"] != env_of(running)["FLEETWIRE_CONFIG_SHA256"]


def test_an_unchanged_secret_keeps_ingest_running(ctx):
    running = ctx.run(ctx.on.config_changed(), setup())
    out = ctx.run(ctx.on.update_status(), running)
    assert out.get_container(CONTAINER).plan == running.get_container(CONTAINER).plan


@pytest.mark.parametrize(("workers", "conns"), [(None, "20"), (64, "68"), (1, "5")])
def test_the_connection_pool_follows_the_workers(ctx, workers, conns):
    config = {} if workers is None else {"workers": workers}
    env = env_of(ctx.run(ctx.on.config_changed(), setup(config=config)))
    assert env["FLEETWIRE_INGEST_WORKERS"] == str(workers or 16)
    assert env["FLEETWIRE_DATABASE_MAX_CONNS"] == conns


def test_no_workers_is_refused(ctx):
    out = ctx.run(ctx.on.config_changed(), setup(config={"workers": 0}))
    assert out.unit_status == testing.BlockedStatus("workers must be at least 1, not 0")
    assert SERVICE not in out.get_container(CONTAINER).plan.services
    assert not ctx.exec_history.get(CONTAINER)


def test_kafka_tls_uses_the_relation_ca(ctx):
    out = ctx.run(ctx.on.config_changed(), setup(kafka={**KAFKA, "tls": "enabled", "tls-ca": "KAFKA CA"}))
    (call,) = ctx.exec_history[CONTAINER]
    assert call.environment["FLEETWIRE_KAFKA_TLS_CA_FILE"] == "/etc/fleetwire/pki/kafka-ca.pem"
    env = env_of(out)
    assert env["FLEETWIRE_KAFKA_TLS"] == "true"
    assert env["FLEETWIRE_KAFKA_TLS_CA_FILE"] == "/etc/fleetwire/pki/kafka-ca.pem"
    fs = out.get_container(CONTAINER).get_filesystem(ctx)
    assert (fs / "etc/fleetwire/pki/kafka-ca.pem").read_text() == "KAFKA CA"


def test_another_group_prefix_is_honoured(ctx):
    out = ctx.run(ctx.on.config_changed(), setup(kafka={**KAFKA, "consumer-group-prefix": "relation-11-"}))
    assert env_of(out)["FLEETWIRE_KAFKA_GROUP"] == "relation-11-ingest"


@pytest.mark.parametrize(
    ("missing", "status"),
    [
        (
            {"kafka": False},
            testing.BlockedStatus("integrate with kafka-k8s: juju integrate fleetwire-ingest-k8s:kafka kafka-k8s"),
        ),
        (
            {"api": False},
            testing.BlockedStatus(
                "integrate with fleetwire-api-k8s: juju integrate fleetwire-ingest-k8s:api fleetwire-api-k8s"
            ),
        ),
        (
            {"mqtt": False},
            testing.BlockedStatus(
                "integrate with mosquitto-k8s: juju integrate fleetwire-ingest-k8s:mqtt mosquitto-k8s"
            ),
        ),
        (
            {"db": False},
            testing.BlockedStatus(
                "integrate with postgresql-k8s: juju integrate fleetwire-ingest-k8s:database postgresql-k8s"
            ),
        ),
        ({"db": {}}, testing.WaitingStatus("waiting for database relation data")),
        ({"kafka": {}}, testing.WaitingStatus("waiting for kafka relation data")),
        # The API has not migrated the database yet, so there are no tables.
        (
            {"api": {"keyring-url": "https://fleetwire-api-k8s.test.svc:8445"}},
            testing.WaitingStatus("waiting for the API to migrate the database"),
        ),
        ({"pki": False}, testing.BlockedStatus("set pki-secret to the ID of the ingest certificate secret")),
        ({"pki": {"tls-cert": "C"}}, testing.BlockedStatus("pki-secret lacks ca-bundle, tls-key")),
    ],
)
def test_reports_what_is_missing(ctx, missing, status):
    out = ctx.run(ctx.on.config_changed(), setup(**missing))
    assert out.unit_status == status
    assert SERVICE not in out.get_container(CONTAINER).plan.services


def test_active_when_ingest_reports_ready(ctx, monkeypatch):
    monkeypatch.setattr(charm, "readyz", lambda port: port == 9090)
    out = ctx.run(ctx.on.config_changed(), setup())
    assert out.unit_status == testing.ActiveStatus()


def test_losing_kafka_stops_ingest(ctx):
    st = setup()
    running = ctx.run(ctx.on.config_changed(), st)
    kafka = next(r for r in running.relations if r.endpoint == "kafka")
    out = ctx.run(ctx.on.relation_broken(kafka), running)
    assert out.get_container(CONTAINER).service_statuses[SERVICE] == ops.pebble.ServiceStatus.INACTIVE


def with_api(st: testing.State, data: dict[str, str]) -> tuple[testing.Relation, testing.State]:
    api = next(r for r in st.relations if r.endpoint == "api")
    api = dataclasses.replace(api, remote_app_data=data)
    return api, dataclasses.replace(st, relations=[api, *(r for r in st.relations if r.endpoint != "api")])


def test_ingest_stops_while_the_api_has_migrations_due_and_starts_after(ctx):
    running = ctx.run(ctx.on.config_changed(), setup())
    api, st = with_api(running, {"keyring-url": API["keyring-url"]})
    paused = ctx.run(ctx.on.relation_changed(api), st)
    assert paused.get_container(CONTAINER).service_statuses[SERVICE] == ops.pebble.ServiceStatus.INACTIVE
    assert paused.unit_status == testing.WaitingStatus("waiting for the API to migrate the database")
    # Stopping again on the next hook is not an error.
    ctx.run(ctx.on.update_status(), paused)
    api, st = with_api(paused, API)
    out = ctx.run(ctx.on.relation_changed(api), st)
    assert out.get_container(CONTAINER).service_statuses[SERVICE] == ops.pebble.ServiceStatus.ACTIVE


def test_database_tls_is_verified_when_postgresql_offers_it(ctx):
    out = ctx.run(ctx.on.config_changed(), setup(db={**DB, "tls": "True", "tls-ca": "PG CA"}))
    assert read(ctx, out, env_of(out)["FLEETWIRE_DATABASE_URL_FILE"]).endswith(
        "?sslmode=verify-full&sslrootcert=/etc/fleetwire/pki/postgresql-ca.pem"
    )
    fs = out.get_container(CONTAINER).get_filesystem(ctx)
    assert (fs / "etc/fleetwire/pki/postgresql-ca.pem").read_text() == "PG CA"


def test_requests_a_login_in_the_runtime_group(ctx):
    st = setup(db={})
    db = next(r for r in st.relations if r.endpoint == "database")
    out = ctx.run(ctx.on.relation_created(db), st)
    data = out.get_relation(db.id).local_app_data
    # charmed_dml: not the schema owner, and a member of the group the API's
    # migrations give the row-level security policies to.
    assert data["database"] == "fleetwire"
    assert data["extra-user-roles"] == "charmed_dml"


def test_losing_the_database_stops_ingest(ctx):
    running = ctx.run(ctx.on.config_changed(), setup())
    db = next(r for r in running.relations if r.endpoint == "database")
    out = ctx.run(ctx.on.relation_broken(db), running)
    assert out.get_container(CONTAINER).service_statuses[SERVICE] == ops.pebble.ServiceStatus.INACTIVE


def test_requests_consume_and_produce_rights_on_the_fleetwire_topics(ctx):
    st = setup(kafka={})
    kafka = next(r for r in st.relations if r.endpoint == "kafka")
    out = ctx.run(ctx.on.relation_created(kafka), st)
    data = out.get_relation(kafka.id).local_app_data
    # One wildcard covers the uplink and dead-letter topics; producer
    # includes CREATE for ensure-topics.
    assert data["topic"] == "fleetwire.*"
    assert data["extra-user-roles"] == "consumer,producer"
    assert data["consumer-group-prefix"] == "fleetwire-"
