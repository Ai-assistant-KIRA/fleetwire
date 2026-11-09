# ADR-0007: Envelope encryption with per-tenant KEKs outside the database

- Status: accepted
- Date: 2026-11-09

## Context

Tenants must be able to destroy their data, including copies in database
backups (R9.5), and every stored reading must be encrypted with the tenant's
own key (R9.4). The key design must allow a KMS or Vault backend later.

## Decision

- Three levels: a **root key** (32 bytes, file mode 0400) seals one **KEK per
  tenant**, stored as a file in the keyring directory; the KEK wraps the
  tenant's **data keys (DEKs)**, which are stored wrapped in
  `tenant_keys.wrapped_dek`; the DEK seals each chunk with AES-256-GCM.
- The keyring is an interface (`Create`, `Wrap`, `Unwrap`, `Destroy`) with
  two backends: local files, and a remote client of the API's keyring
  listener for deployments where ingest cannot read the API's directory
  (Kubernetes, ADR-0012). The remote client only wraps and unwraps; KEKs are
  created and destroyed by the API alone, and never leave it.
- Destroying a tenant deletes its KEK file and fsyncs the directory. From then
  on, no wrapped DEK of that tenant can be opened, wherever it was copied.
- Each chunk's AAD is `"fleetwire/chunk/v1" | tenant | device | seq_lo | seq_hi | key_id`
  in fixed-width binary, so a ciphertext moved to another row fails to open.
- Nonces are 96-bit random. A DEK is rotated after 2^24 seals or 90 days,
  which keeps the chance of a nonce repeat far below the GCM limit for random
  nonces.
- Not encrypted, because queries and retention need them: tenant, device, seq
  range, min and max collect time, reading count. Metric names, labels and
  values are encrypted.
- The API cursor signing key is derived from the root key with HKDF-SHA256 and
  a purpose label, so no second secret has to be deployed and the root key is
  never used for two purposes.

## Alternatives considered

- **Wrapped DEKs and KEKs both in the database.** A backup would contain
  everything needed to decrypt, so destroying a tenant would not reach old
  backups.
- **PostgreSQL TDE or pgcrypto.** TDE is all-or-nothing per cluster and cannot
  destroy one tenant; pgcrypto puts the keys in SQL statements and logs.
- **One key per device.** More keys to rotate and cache, with no gain for the
  tenant-level destroy requirement.

## Consequences

- The keyring directory must be backed up separately and with a short
  retention, or a destroyed tenant's KEK survives in a keyring backup. The ops
  docs say so.
- On SSDs, deleting a file is not a secure erase. Production should use a KMS
  backend with real key destruction; that backend is future work.
- With the remote backend ingest holds data keys in memory (cached for 5
  minutes) but no KEK. An unreachable API delays new batches once the cache
  expires; ingest retries them instead of dead-lettering.
- Readings cannot be aggregated inside PostgreSQL, which is one reason for
  ADR-0008 (no TimescaleDB).
- The visible metadata reveals how many readings a device sent and when. That
  is accepted.
