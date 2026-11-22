# fleetwire

A telemetry pipeline for fleets of Ubuntu Core devices that stores every
reading exactly once, through network cuts, broker crashes, Kafka and
database outages, and power loss, and keeps each tenant's data apart,
encrypted with the tenant's own keys.

The documentation follows the [Diátaxis](https://diataxis.fr/) layout.

```{toctree}
:caption: Tutorial
:maxdepth: 1

tutorial/run-a-fleet
```

```{toctree}
:caption: How-to guides
:maxdepth: 1

how-to/enroll-a-device
how-to/recover-device-state
how-to/operate
how-to/run-failure-tests
how-to/run-on-ubuntu-core
how-to/deploy-with-juju
how-to/verify-a-release
```

```{toctree}
:caption: Reference
:maxdepth: 1

reference/configuration
reference/api
reference/metrics
reference/cli
reference/third-party-facts
results/index
```

```{toctree}
:caption: Explanation
:maxdepth: 1

explanation/architecture
explanation/delivery-guarantee
explanation/wal
explanation/encryption-and-tenancy
explanation/slos
explanation/load-generator
explanation/threat-model
adr/index
```
