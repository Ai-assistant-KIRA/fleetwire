# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This release adds database migrations 00005 to 00008. The API charm and
`make up` run them; elsewhere, run `fleetwire-api migrate` before starting
the new services.

### Added

- Secret settings can be read from files: `FLEETWIRE_DATABASE_URL_FILE`,
  `FLEETWIRE_DATABASE_OWNER_URL_FILE` and `FLEETWIRE_KAFKA_PASSWORD_FILE`.
  The services refuse a file that group or others can access, and refuse
  to start when a variable and its `_FILE` variable are both set.
- `FLEETWIRE_API_AUTH_FAILURE_RATE_LIMIT` and `_BURST` (failed token
  lookups per client network) and `FLEETWIRE_API_MAX_ACTIVE_EXPORTS`
  (exports one tenant can have pending or running, default 4).
- `fwctl version`, `fwctl token show` and `fwctl job show`.
- Alerts `FleetwireCRLNotPublished`, `FleetwireTargetDown` and
  `FleetwireServiceDown`, so a stopped service no longer silences the other
  alerts. `make rules-test` fails when an alert has no unit test.
- Metrics `fleetwire_api_enroll_refusals_total{outcome}` and
  `fleetwire_loadgen_device_open_failures_total`, and a `/status` endpoint
  on the load generator's metrics listener.
- How-to guides for a device whose state file is corrupt and for backing up
  the keyring.
- The release attaches the amd64 and arm64 agent snaps with SPDX SBOMs and
  lists them in the signed `checksums.txt`; the verification guide shows how
  to check a snap.
- CI runs shellcheck on every shell script (`make shellcheck`), lints the
  bundle integration test, scans the service images with Grype and builds a
  GoReleaser snapshot.

- COS Lite integration for the charms: the api, ingest and bridge charms
  provide `metrics-endpoint` with the fleetwire alert rules split per
  service, the api charm provides `grafana-dashboard`, and all four forward
  their workload logs to Loki over `logging` (Juju 3.4 or later). The
  integration test checks all of it when `FLEETWIRE_COS=1` is set.
### Changed

- The charms write the database URLs and the Kafka password to 0600 files
  and pass the `_FILE` variables, so `pebble plan` no longer shows them. A
  new password restarts the service. The schema owner's URL exists on disk
  only while `migrate` or `create-operator-token` runs.
- Finished jobs are deleted 30 days after they finish, after which
  `GET /v1/jobs/{id}` answers 404, and enrollment tokens 7 days after they
  expire (migration 00008).
- `GET /v1/jobs/{id}` needs the read scope of the job's kind: `exports:read`,
  `devices:read` for device purges, `tenant:admin` for retention and tenant
  destroy. `last_error` is a fixed message; the cause is in the server log.
- The bridge writes a message Kafka refused again, with backoff, until Kafka
  takes it. `fleetwire_bridge_forwarded_total{result="error"}` counts each
  failed attempt, `fleetwire_bridge_pending_acks` includes the message, and
  readiness fails while any write is retried.
- `fleetwire_loadgen_device_failures_total` no longer counts devices that
  never opened, and the load generator exits with an error when a device did
  not start.
- A chaos recovery time now starts only from samples taken after the fault
  and needs every device connected and the p99 under 5 s for 30 s; for
  agent-kill it includes reopening every device. The Devices column comes
  from the checker. Results recorded before this change used the old method.
- Explicit `{"count": 0}` and `{"ttl_seconds": 0}` get 400 instead of the
  default, names refuse control characters, and an empty chunked body counts
  as no body.
- The charms are built for arm64 as well as amd64, and the bundle and charms
  point at the 0.1.1 images. A tag release fails when they name another
  version.
- Bump github.com/jackc/pgx/v5 to 5.13.0, github.com/pressly/goose/v3 to
  3.30.1, github.com/twmb/franz-go to 1.22.3, golang.org/x/time to 0.16.0,
  golang.org/x/sync to 0.25.0 and actions/setup-go to 7.1.0.
- The docs build with Sphinx 9.2.0, myst-parser 5.2.0, furo 2025.12.19 and
  sphinxcontrib-mermaid 2.1.1 on Python 3.13, and fail on a broken internal
  cross-reference.
- The API reference and OpenAPI description document the rate-limit order,
  job scopes, job and token pruning, and export limits, and the docs and
  code comments no longer cite planning files outside the repository.

- The agent snap's daemon runs as the `snap_daemon` user with no
  capabilities instead of root, and `sudo fleetwire-agent` runs as that
  user too. The identity and local log move from `$SNAP_DATA` to
  `$SNAP_DATA/state`; the install and post-refresh hooks create or migrate
  it, and the daemon repairs it before every start. A revert to 0.1.1 runs
  on that revision's copy of the data; the Ubuntu Core how-to gives the
  procedure that loses nothing.
- In the snap, `fleetwire-agent status` needs sudo to read the files while
  the daemon is stopped, `metrics.listen` can no longer use a port below
  1024, and a rewritten `state.json` must belong to `snap_daemon`.
- Retention deletes an export 30 days after it expires, or 30 days after it
  was created if it failed; `GET /v1/exports/{id}` then answers 404.
- The bundle pins `postgresql-k8s` revision 957 (`16/stable`) and
  `kafka-k8s` revision 111 (`4/stable`), and the charms pin every Python
  dependency with `charm-strict-dependencies`.
### Fixed

- The agent's uplink no longer wakes every millisecond while it is
  disconnected or its window is full, and no longer republishes the same
  batch in a loop when more gaps are pending than one batch carries.
- The agent reconnects to the broker after a certificate renewal, so the new
  certificate is used at once, and closes the HTTP connection of each enroll
  or renew attempt.
- A failed save of a disk-budget drop leaves the local log's state as it
  was instead of reporting sent seqs as lost.
- Ingest refuses seqs above 2^63-2 with the dead-letter reason
  `seq_overflow` instead of retrying them against PostgreSQL.
- Ingest counts each dead-lettered batch once. Batches that failed
  validation counted twice in the dead-letter burn-rate alerts.
- Ingest reports unready only during database or keyring outages, not while
  one device sends batches that fail.
- A device's retained ack no longer moves back when two ack workers or two
  partition owners publish for the same device.
- The data key manager keeps keyring calls out of database transactions.
- A running export can no longer leave decrypted files behind a tenant
  destroy, each export attempt writes its own temp file, and failed exports
  show as failed instead of running.
- A tenant destroy or device purge that failed for good can be requested
  again, and the 202 answer carries the ID of the job doing the work instead
  of the nil UUID.
- A panic in a job no longer stops the API, a shutdown hands running jobs
  back without counting an attempt, and a job whose worker dies every time
  is failed after its last attempt.
- A readings page decrypts a bounded number of chunks and may come back
  short or empty with `next_cursor`; a query past the deadline gets 503
  `timeout`. Lists no longer end with an empty page.
- `HEAD` on an export download sends the headers without reading the file.
- On shutdown the API drains both listeners in parallel before stopping the
  CRL publisher and the token-use recorder.
- `fleetwire-api serve` refuses a rate-limit burst of 0, an enrollment-token
  TTL outside 1m to 24h, and NaN or infinite numbers.
- MQTT keep alive and session expiry below a second no longer turn into 0.
- The bridge and ingest release their tracer and Kafka client when they fail
  to start, and the bridge's last-produce gauge starts at the process start
  time.
- The api, ingest and bridge charms restart their service when a
  certificate, key or CA changes.
- `mosquitto-k8s` refuses a second unit, checks both listeners, restarts a
  broker that stops accepting, and no longer sends SIGHUP to a broker that
  replan just restarted.
- `fleetwire-ingest-k8s` sizes its connection pool from `workers`, starts
  other units only after the leader created the topics, and stops ingest
  while the API has migrations due.
- `FleetwireBridgeNotProducing` is evaluated per replica and no longer pages
  on a cold start or after an idle spell; `FleetwireCRLStale` no longer pages
  at every API start, and the dashboard's CRL panel ignores the boot value.
- `fwctl` rejects flags a command does not take, treats everything after
  `--` as arguments, prints an empty list as `[]`, asks the API for
  `-limit` items, and no longer cuts off downloads after 10 minutes.
- `fleetwire-agent status` shows the version and reaches a status endpoint
  that listens on a wildcard address.
- Version lines leave out the build date when there is none, and the snap's
  agent binary carries its commit and build date.
- The snap builds from a tree without git or with deleted tracked files, and
  its configure hook fails when `snapctl` fails.
- The checker fails a device with stored seqs above its last seq
  (`beyond_last_seq`), and the chaos table's Gap seqs column is no longer
  always 0.
- A chaos scenario fails when devices do not start, and network-degraded
  throttles upstream bandwidth too.
- `throughput.sh` records a step as failed when the load generator fails,
  and result tables name the CPU on Arm hosts.
- `uc-vm.sh` matches the image's checksum line by exact file name.
- CI no longer cancels runs on `main`, the snap and e2e workflows cancel
  superseded pull request runs, and every job has a timeout.
- The docs give the correct row rate in ADR-0008 and reading rate of the
  power-off test, no longer claim ingest kept up with 10,000 devices on
  tmpfs, say where an enrollment refusal shows up, list every metric, and
  note the keyring listener in ADR-0011. The OpenAPI description carries
  version 0.1.1 and declares mutual TLS for `POST /v1/renew`.

- The agent snap no longer causes an AppArmor denial for
  `/proc/<pid>/net/netstat` on every metrics scrape; it leaves out the two
  process network counters, which the snap could never read.
- An export attempt that stalled past its lease during a tenant destroy can
  no longer recreate the tenant's export directory.
- Token use still queued when the API stops is recorded, and a revocation
  triggered just before the CRL publisher stops is published.
### Security

- Each client network (IPv4 address or IPv6 /64) gets a budget of failed
  token lookups, checked before PostgreSQL is asked, and a token's rate
  limit applies before its scopes. `GET /v1/crl` has a per-network limit,
  enrollment limits use IPv6 /64, and refused enrollments write at most one
  audit entry per token or network every 10 minutes.
- Enrollment and renewal lock the tenant and device rows, so neither can
  race a revoke or tenant destroy into an unrevoked certificate missing from
  the CRL (migration 00005).
- `-h` and flag errors no longer print secrets set in the environment.
- Each Mosquitto listener has its own ACL, so a device certificate with a
  service name gets no service rights on the device listener.
- The bridge and ingest require a TLS broker URL, and the API refuses broker
  and API URLs for devices that do not use TLS.
- Export downloads past `expires_at` get 410 `expired`, export files are
  opened and deleted only inside the export directory, and a tenant can have
  a limited number of active exports (409 `too_many_exports`).
- API responses carry `X-Content-Type-Options: nosniff` and
  `Cache-Control: no-store`.
- `fwctl` refuses plain `http://` except to a loopback address.
- The Kubernetes services no longer carry the admin listener, and the deploy
  guide exposes only ports 8883, 8443 and 8444. The charms' one-off commands
  run as the image's nonroot user, and `ensure-topics` gets only the Kafka
  settings.
- Compose mounts only the secrets each service reads, drops every
  container's capabilities (PostgreSQL keeps the five its entrypoint needs),
  sets no-new-privileges, and gives all but Kafka a read-only root. Docker
  builds no longer copy private keys.
- `uc-install.sh` quotes every argument it passes to the guest, and the demo
  and throughput scripts keep API tokens out of process lists and
  world-readable files.
- The release workflow pins cosign, syft and GoReleaser, publishes only a
  commit that passes CI and the end-to-end test, and the Dockerfiles pin
  their base images by digest.
- A destroyed tenant's unwrapped data keys leave the memory of every
  process within 5.5 minutes of its KEK being destroyed: the API drops them
  at once, ingest at the tenant's next batch, and a sweep every 30 s
  removes expired keys.
## [0.1.1] - 2026-11-22

### Fixed

- Stamp the packaged agent binary with the snap's version, so `fleetwire-agent
  status` and build-info metrics identify the installed release.
- Exclude the generated Ubuntu Core disk from Docker and Snapcraft build
  contexts. Running `uc-vm.sh download` before `make up` or `make snap` no
  longer copies the sparse 8 GiB image into service or snap builds.

## [0.1.0] - 2026-11-21

The first release.

### Added

- `fleetwire-agent`, a strict snap for Ubuntu Core 24: collectors for CPU,
  memory, load, uptime, disk and thermal zones; a local write-ahead log with
  CRC-checked records, three sync modes, a disk budget and recovery that
  reports unexplained holes as gaps; an uplink with batching, an in-flight
  window, end-to-end acks and go-back-N resends; enrollment and certificate
  renewal; `status` and `config check` commands.
- `fleetwire-bridge`: Mosquitto shared subscription to Kafka with
  `acks=all`, acknowledging MQTT messages only after Kafka confirms them.
- `fleetwire-ingest`: exactly-once storage per device seq with a coverage
  model, a per-device row lock and an exclusion constraint; per-tenant
  envelope encryption; end-to-end acks; a dead-letter topic with reasons.
- `fleetwire-api`: tenant, operator and device API (OpenAPI 3.1 in
  `api/openapi.yaml`), enrollment and renewal, revocation with a CRL,
  readings and gaps with signed cursors, exports with `Content-Digest`,
  retention, device purge, tenant destroy by key destruction, an append-only
  audit log, and a keyring listener for ingest in Kubernetes.
- PostgreSQL schema with forced row-level security on every tenant table and
  a runtime role that cannot bypass it.
- `fwctl`, the operator and tenant CLI.
- Test tools: `fleetwire-loadgen` (thousands of real agent instances in one
  process), `fleetwire-checker` (reads everything back and proves the
  guarantee), `fleetwire-chaos` (nine failure scenarios and a results
  table, with the devices' state optionally on tmpfs), the throughput
  search, and QEMU scripts for the power-off test on Ubuntu Core 24 or an
  Ubuntu Server 24.04 stand-in.
- Compose development stack with three Kafka brokers, two bridges, two
  ingest instances, Toxiproxy, Prometheus, Grafana and Jaeger; `make demo`.
- SLO recording rules, multi-window burn-rate alerts with promtool tests, and
  a Grafana dashboard.
- Juju charms for Kubernetes (`fleetwire-api-k8s`, `fleetwire-ingest-k8s`,
  `fleetwire-bridge-k8s`, `mosquitto-k8s`) and a bundle with Charmed
  PostgreSQL and Charmed Kafka.
- CI with pinned actions, release builds with GoReleaser, SPDX SBOMs and
  cosign keyless signatures, and Sphinx documentation in the Diátaxis layout.

[Unreleased]: https://github.com/Ai-assistant-KIRA/fleetwire/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/Ai-assistant-KIRA/fleetwire/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Ai-assistant-KIRA/fleetwire/releases/tag/v0.1.0
