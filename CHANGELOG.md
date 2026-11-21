# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-11-21

### Fixed

- Stamp the packaged agent binary with the snap's version, so `fleetwire-agent
  status` and build-info metrics identify the installed release.
- Exclude the generated Ubuntu Core disk from Docker build contexts. Running
  `uc-vm.sh download` before `make up` no longer sends the sparse 8 GiB image
  into each service build.

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
