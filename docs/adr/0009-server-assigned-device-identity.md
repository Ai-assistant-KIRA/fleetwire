# ADR-0009: Server-assigned device identity; the tenant never comes from the device

- Status: accepted
- Date: 2026-11-09

## Context

Every authorization decision in the pipeline keys on the device ID: the
broker ACL pins topics to it, ingest finds the tenant through it, and the API
scopes data by it. If a device could choose its ID, one stolen enrollment
token would let an attacker claim another device's name, or a service name
such as `fleetwire-ingest`, which the broker ACL allows to publish acks for
every device (R4.3, R4.8, R4.9).

## Decision

- The device creates its own ECDSA P-256 key and never sends it anywhere
  (R4.1). It sends a CSR with a one-time enrollment token.
- The API generates the device ID (UUIDv7) and signs a certificate with
  `CN=<id>` and the SAN `URI:urn:fleetwire:device:<id>`. The CSR subject is
  ignored; only its public key and signature are used.
- `pki.DeviceID` accepts a certificate only when the CN is a UUID and the URI
  SAN matches it, so a certificate from another issuer under the same root
  cannot pass as a device on the API's renewal endpoint.
- The tenant of a device is read from the `devices` table on every batch
  (`device_for_ingest`). Nothing in the uplink payload names a tenant.
- Enrollment tokens are 256-bit random secrets with a `fwe_` prefix, stored as
  SHA-256 hashes, valid 15 minutes by default, and consumed atomically by a
  SECURITY DEFINER function under a row lock. Unknown, expired and used
  tokens get the same response; every refusal is written to the audit log,
  under the token's tenant when the token is known.
- The enrollment response carries the broker URL and the CA bundle, so a
  device needs only the API URL, a token, and (for a private CA) the bundle
  to trust the API itself.

## Alternatives considered

- **Device-chosen IDs (serial number, MAC).** Convenient for inventory, but
  they make spoofing a question of guessing a name. Inventory names go in the
  free-text `name` field instead.
- **Pre-provisioned certificates.** Avoids the enrollment endpoint, but the
  private key then exists outside the device.
- **Tenant in the topic** (`fw/v1/t/{tenant}/d/{id}/up`). Redundant with the
  registry and one more field to trust or validate.

## Consequences

- A leaked enrollment token can enroll at most one rogue device, for 15
  minutes, into the token's tenant, and the audit log shows it.
- Re-enrolling a device gives it a new identity and a new seq space; the old
  device's unsent readings are not carried over. The how-to guide says so.
