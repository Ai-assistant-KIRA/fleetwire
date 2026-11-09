# ADR-0012: Deploy the backend with Juju charms on Kubernetes

- Status: accepted
- Date: 2026-11-09

## Context

Operators need to deploy, configure, relate and upgrade the backend (API,
bridge, ingest, broker) together with PostgreSQL and Kafka, without copying
connection strings and passwords between systems (R14). On Kubernetes each
service runs in its own pod, so two things the Compose stack gets from a
shared host are missing: a keyring directory that the API and ingest both
read, and a CRL file that the API writes and Mosquitto reads.

Charmed PostgreSQL also shapes the database access. A plain relation user
is switched to the database's owner role at login (a login hook runs
`SET ROLE charmed_<database>_owner`), so a service that connected with it
would own the tables and could disable row-level security. And relation
users cannot create roles: on a live Charmed PostgreSQL 16 the relation user
has no `CREATEROLE`, and the charm accepts only `charmed_stats`,
`charmed_read`, `charmed_dml`, `charmed_admin` or `superuser` as extra user
roles.

## Decision

- **Four sidecar charms** written with `ops`: `fleetwire-api-k8s`,
  `fleetwire-ingest-k8s`, `fleetwire-bridge-k8s` and `mosquitto-k8s`. Each
  reconciles on every event: it reads its config, its secret and its
  relations, writes the files and the Pebble layer, and `replan` restarts the
  workload only when the layer changed. Statuses come from collect-status:
  `blocked` names the missing relation or secret and the command that fixes
  it, `waiting` means relation data or the workload is not ready, `active`
  means the workload's own `/readyz` (or the broker's port) answers.
- **Canonical's charms** for the data stores: `postgresql-k8s` 16/stable
  over `postgresql_client`, `kafka-k8s` 4/stable (KRaft) over `kafka_client`,
  both through `data_platform_libs`. Kafka requires SASL/SCRAM-SHA-512, and
  TLS when it is related to a certificates provider; bridge and ingest take
  both from the relation. The bridge asks for producer rights on
  `fleetwire.uplink.v1`; ingest asks for consumer and producer rights on
  `fleetwire.*` (the uplink and dead-letter topics, and CREATE for
  `ensure-topics`) and uses the granted consumer-group prefix.
- **Two kinds of database login.** The API's `database` relation (no extra
  roles) is the schema owner; the leader uses it only to run
  `fleetwire-api migrate` and `create-operator-token` in its workload
  container. The services log in through their own relations requested with
  the extra user role `charmed_dml`: the API's `runtime-database` and
  ingest's `database`. Those users are not admins of the database, so the
  login hook leaves them as they are, and they cannot switch to the owner.
  Migrate runs with `FLEETWIRE_DATABASE_RUNTIME_ROLE=charmed_dml`, so the
  policies and grants name the group the runtime users belong to (ADR-0006).
  Ingest never sees the owner's credentials, and it starts only once the API
  says, over the `ingest` relation, that the schema exists. A broken
  `runtime-database` relation stops the API; a broken `database` relation
  makes the leader migrate again when one returns.
- **The KEKs stay with the API.** The local keyring lives on the API unit's
  storage. Ingest uses the remote keyring backend: the API serves wrap and
  unwrap on an internal listener (`:8445`), mutual TLS with service
  certificates, and admits only `fleetwire-ingest`. Data keys cross the
  wire; KEKs never do. With one keyring directory, the API application runs
  one unit, and the charm blocks with the reason if it is scaled up.
- **The broker fetches the CRL.** The API serves its latest CRL at
  `GET /v1/crl` on the device listener. The Mosquitto charm gets that URL over
  the `crl` relation, fetches it over TLS verified against the CA bundle on
  every update-status hook and relation change, writes it only when it
  changed, and sends SIGHUP through Pebble.
- **Own interfaces** for what only fleetwire needs: `fleetwire_mqtt`
  (Mosquitto publishes its service listener URL), `fleetwire_ingest` (keyring
  URL and schema readiness) and `fleetwire_crl` (CRL URL).
- **PostgreSQL settings**: the bundle enables the charm's `btree_gist` plugin
  (the chunk exclusion constraint needs it) and gives fleetwire a PostgreSQL
  of its own, because `charmed_dml` reads and writes every database of the
  cluster that its members can connect to.
- **Secrets**: CA keys, the keyring root key and service certificates are
  Juju user secrets, one per application with only what that workload needs;
  charm config holds the secret ID. Files land in the workload owned by the
  workload's user, mode 0600.
- **Operator bootstrap**: the API charm's `create-operator-token` action runs
  the command with the owner credentials only the charm holds.
- **Tests**: `ops.testing` state-transition unit tests for every charm, and a
  Jubilant integration test that deploys the bundle on MicroK8s, enrolls a
  real agent through port-forwards and checks that every acknowledged
  reading is stored.

## Alternatives considered

- **Helm charts.** Templates without a model of relations: database and Kafka
  credentials would be copied by hand or through an external secret store.
- **Migrate creates a login role (`fleetwire_app`) with a generated
  password.** Built first, and refused by Charmed PostgreSQL: relation users
  have no `CREATEROLE` (`permission denied to create role`).
- **Services log in as the owner relation user and `SET ROLE` down to a
  less privileged role.** Row-level security would hold for correct code,
  but a compromised service could `RESET ROLE` and act as the owner.
- **Requesting `superuser`.** It would allow creating roles, and make every
  API pod a cluster superuser.
- **Keyring files on a shared volume.** Juju storage belongs to one unit; a
  ReadWriteMany volume outside Juju would hold every tenant's KEK where both
  pods, and its backups, can read it.
- **One charm for API and ingest.** One pod would share the directory, but
  the two scale and fail differently, and R14 names separate charms.
- **The API pushes the CRL into the relation data.** It works for small
  lists but puts a growing blob in every relation change; a URL keeps the
  relation small and lets other brokers use the same endpoint.
- **A Vault Transit or cloud KMS keyring.** The right production answer,
  because it also removes the one-unit limit; it is future work behind the
  same `Keyring` interface.

## Consequences

- `charmed_dml` is wider than fleetwire's own grants: it includes
  `pg_read_all_data` and `pg_write_all_data` on every database its members
  can connect to. Row-level security still applies to fleetwire's tables,
  but the runtime logins could read other applications' databases on a
  shared PostgreSQL, hence a dedicated one.
- The API application is limited to one unit until a shared keyring
  backend exists. Its pod restarting delays ingest only once ingest's cached
  data keys expire (5 minutes); batches wait and are retried, they are not
  dead-lettered.
- Revocation at the broker lags by up to the update-status interval
  (5 minutes by default). Ingest drops a revoked device's data from the
  moment of revocation (ADR-0010, layer 1), so nothing from it is stored in
  that window.
- Devices outside the cluster need the broker's device port and the API's
  device port exposed (a LoadBalancer or NodePort service); the charms do not
  create those.
- Charm code is Python, the rest is Go: two toolchains in CI.
