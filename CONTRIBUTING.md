# Contributing to fleetwire

Thanks for helping. This project makes a strong promise (every reading
stored exactly once, tenants kept apart), so changes come with evidence.

## Before you start

- Open an issue for anything bigger than a small fix, so the design can be
  discussed before code is written.
- Read the [architecture](docs/explanation/architecture.md), the
  [delivery guarantee](docs/explanation/delivery-guarantee.md) and the
  [ADRs](docs/adr/).

## Development setup

You need Go (version in `go.mod`), Docker with Compose v2, `make`, and for
some targets `buf`, `golangci-lint`, `promtool`, Python 3.12 (charms) and
Python 3.13 or later (`make docs`, as Sphinx 9 needs it; pick the
interpreter with `make docs PYTHON=python3.13`).

```console
$ make lint test          # format, vet, lint, unit and integration tests
$ make test-race          # with the race detector
$ make fuzz-smoke         # every fuzz target for 30 s
$ make up demo            # the full stack and five real agents
$ make e2e                # end-to-end test against the stack
$ make rules-test         # Prometheus rules and their unit tests
```

Integration tests start PostgreSQL with testcontainers; they skip when no
Docker daemon is available.

## Rules for changes

- **Tests that fail without the change.** Bug fixes come with a test that
  reproduces the bug.
- **Errors are wrapped with context and never ignored.** No `TODO`s, dead
  code or placeholder features.
- **Comments explain why**, not what the code does.
- **A design decision updates its ADR** (`docs/adr/`) in the same commit.
  A reversed decision gets a new ADR that supersedes the old one.
- **Changes to the wire format** (`proto/`) must pass `buf breaking`.
- **Changes to the guarantee** (local log, uplink, coverage, ingest) also
  run the failure suite: `bin/fleetwire-chaos run -devices 200`.
- Keep commits focused, with a subject in the imperative mood.

## Pull requests

CI runs formatting, vet, golangci-lint, tests with the race detector, fuzz
smoke runs, govulncheck, `buf lint`/`buf breaking`, charm lint and unit
tests, Prometheus rule tests, the docs build with a link check, and image
builds. Add the `e2e` label to run the Compose end-to-end test and two
failure scenarios on the PR.

By contributing you agree that your contributions are licensed under the
Apache License 2.0.
