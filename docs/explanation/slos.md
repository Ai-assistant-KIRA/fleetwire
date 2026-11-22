# Service-level objectives

| SLO | Objective (30 days) | SLI |
|---|---|---|
| Freshness | 99% of readings stored within 5 s of collection | `fleetwire_ingest_e2e_delay_seconds` bucket `le="5.0"` / count |
| Ingest correctness | 99.99% of batches not sent to the dead-letter topic | `fleetwire_ingest_batches_total{result="dlq"}` / all |
| API availability | 99.9% of requests not answered with 5xx | `fleetwire_api_requests_total{code=~"5.."}` / all |

Two details in the rules matter. Prometheus 3 stores bucket bounds in float
form, so the 5 s bucket is `le="5.0"`; a rule written with `le="5"` matches
nothing and its alert can never fire. And a ratio whose error series does
not exist yet would be missing instead of 0, so the services create every
known label value at start and the API rule falls back to `vector(0)` when
no 5xx series exists. The promtool tests check both cases.

The end-to-end delay is measured at ingest as commit time minus the
device's collection timestamp, so it includes time spent in the device's
local log while the network was down. Device clocks can be wrong: the
absolute difference between the device's send time and the bridge's receive
time is exported as `fleetwire_ingest_clock_skew_seconds`, and negative
delays are clamped to 0 and counted.

## Alerts

Burn-rate alerts (`deploy/prometheus/rules/fleetwire.yml`), on recording
rules for 5 m, 30 m, 1 h and 6 h error ratios:

| Severity | Condition | Meaning |
|---|---|---|
| page | error ratio over 1 h **and** 5 m above 14.4 x budget | 2% of the 30-day budget gone in an hour |
| ticket | error ratio over 6 h **and** 30 m above 6 x budget | 5% gone in six hours |

The short window makes an alert stop soon after the problem ends.

Symptom alerts without an SLO:

- `FleetwireCRLStale` (page): no CRL published for 48 h (the broker rejects
  every device when the 7-day CRL expires). An API process reports a huge
  age until its first publish; the rule counts such a replica by its uptime
  instead, so a restart does not page.
- `FleetwireCRLNotPublished` (ticket): an API replica has not published the
  CRL in the 15 minutes since it started.
- `FleetwireBridgeNotProducing` (page): on one bridge replica, messages wait
  and the last confirmed Kafka write is over 2 minutes old, and this has
  lasted 2 minutes.
- `FleetwireConsumerLagGrowing` (ticket): lag above 1,000 and still growing
  for 10 minutes.
- `FleetwireExclusionViolation` (page): the database rejected an overlapping
  chunk; this must never happen.

`make rules-test` checks the rules with `promtool check rules` and runs the
unit tests in `deploy/prometheus/tests/`.
