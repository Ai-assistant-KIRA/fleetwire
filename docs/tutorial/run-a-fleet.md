# Tutorial: run a fleet on one machine

This tutorial starts the whole backend in Docker Compose, enrolls five real
agents, and shows their readings. It takes about ten minutes on a machine
with 4 CPUs and 8 GB of RAM.

## What you need

- Linux (tested on Ubuntu 24.04 and 26.04), Docker with Compose v2, Go 1.27,
  `make`, `jq`.

## 1. Start the stack

```console
$ make up
```

This creates a development CA in `deploy/compose/secrets/` (never committed),
builds the service images, and starts PostgreSQL, three Kafka brokers,
Mosquitto, Toxiproxy, two bridges, two ingest instances, the API,
Prometheus, Grafana and Jaeger. It returns when every container is healthy.

## 2. Run the demo

```console
$ make demo
```

The demo:

1. mints an operator token (`fleetwire-api create-operator-token`),
2. creates a tenant and a tenant admin token with `fwctl`,
3. asks for five enrollment tokens,
4. starts five `fleetwire-agent` processes with their own state directories,
5. waits a minute and prints the devices and the latest readings.

You will see each device's `ACKED_SEQ`: every seq up to that number is
stored. The agents print their own view with `fleetwire-agent status`.

## 3. Look around

- Grafana: <http://localhost:3000>, dashboard **fleetwire**.
- Jaeger: <http://localhost:16686>, service `fleetwire-ingest`, to follow one
  batch from the bridge to the database to the ack.
- The API: `bin/fwctl -url https://localhost:8443 -ca-file deploy/compose/secrets/ca-bundle.pem -token "$(cat deploy/compose/.state/demo/tenant-token)" device list`

## 4. Break something

Stop the database for 30 seconds and watch the agents keep collecting:

```console
$ docker compose -f deploy/compose/compose.yaml kill postgres
$ sleep 30
$ docker compose -f deploy/compose/compose.yaml start postgres
```

The `ACKED_SEQ` values stop moving, then catch up; nothing is lost. To prove
it at scale, run the failure suite: [how-to](../how-to/run-failure-tests.md).

## 5. Clean up

```console
$ make demo-stop
$ make reset
```
