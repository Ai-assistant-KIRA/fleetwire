# ADR-0010: 30-day device certificates and two-layer revocation

- Status: accepted
- Date: 2026-11-09

## Context

An operator must be able to block one device at once. TLS revocation
alone is slow and fragile: a broker only learns about a revocation when it
reloads its CRL, it keeps connections that are already open, and an expired
CRL makes OpenSSL reject every client.

## Decision

- Device certificates are valid 30 days and renewed over mutual TLS from two
  thirds of their lifetime on. Renewal must keep the same key, and the
  device must be active.
- **Layer 1, ingest (authoritative, immediate).** Revoking sets
  `devices.status = 'revoked'` and revokes the device's certificate rows in
  the same transaction as the audit entry. Ingest reads the status under the
  device row lock for every batch, so the first batch processed after the
  commit is dropped and counted.
- **Layer 2, broker (connection level).** The API rebuilds the CRL after each
  revocation and every 24 hours, signs it with the Device Issuing CA with a
  `nextUpdate` of 7 days, and writes it atomically. Mosquitto 2.1.2 reloads
  `crlfile` on SIGHUP (checked, see `docs/reference/third-party-facts.md`), and
  a watcher next to the broker sends SIGHUP when the file changes.
- The CRL number is the signing time in nanoseconds, so it grows across API
  replicas and restarts without shared state.
- Devices and services use separate broker listeners. The device listener
  trusts the device CA chain and checks the CRL. The service listener trusts
  the service CA chain, has no CRL, and is reachable only on the internal
  network, so a revoked device cannot bypass layer 2 through it.
- Certificate rows stay until the certificate expires, even after the device
  or its tenant is deleted, so a revoked certificate stays on the CRL for as
  long as it could be presented.

## Alternatives considered

- **OCSP.** Needs a responder with high availability and is not supported by
  Mosquitto.
- **Long-lived certificates plus CRL only.** A missed CRL reload leaves a
  stolen key usable for years.
- **Broker plugin that queries the API.** Immediate, but couples the broker's
  availability to the API's and needs custom plugin code.

## Consequences

- A revoked device that keeps its TCP connection open can still publish until
  it reconnects; layer 1 makes sure none of that data is stored.
- If CRL publishing stops for 7 days, the broker rejects every device. The
  `fleetwire_crl_age_seconds` metric has an alert at 48 hours.
- A device offline for more than 30 days cannot renew and must be enrolled
  again; this is documented as an operator action.
