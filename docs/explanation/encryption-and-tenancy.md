# Encryption and tenant isolation

## Keys

```
root key (32 bytes, owner-only file)
  └── per-tenant KEK, one sealed file in the keyring directory
        └── tenant data keys (DEKs), stored wrapped in tenant_keys
              └── AES-256-GCM seals each chunk of readings
```

- Each chunk's additional authenticated data is
  `"fleetwire/chunk/v1" | tenant | device | seq_lo | seq_hi | key_id`, fixed
  width. A ciphertext copied to another row, tenant, device or seq range
  fails to open.
- Nonces are 96 random bits. A data key seals at most 2^24 chunks or lives
  90 days, then a new one is made under a tenant row lock.
- Unwrapped data keys are cached in memory for up to 5 minutes after they
  are unwrapped. Every process that caches them removes expired ones every
  30 seconds, whether or not anything asks for them again.
- Two keyring backends: the directory itself, which the API and ingest share
  on one host, and in Kubernetes a client of the API's keyring listener.
  There ingest holds data keys while it uses them, never a KEK or the root
  key, and an unreachable API only delays batches.
- Plain columns (needed for queries and retention): tenant, device, seq
  range, first and last collect time, reading count. Metric names, labels
  and values are encrypted.

## Destroying a tenant

`DELETE /v1/admin/tenants/{id}` (or `fwctl tenant destroy`) first, in one
transaction, marks the tenant `destroying`, revokes every device and deletes
every wrapped data key; then deletes the tenant's KEK file. From that moment
no copy of the tenant's readings can be decrypted, including database
backups taken earlier. A restartable job then deletes the rows and export
files and marks the tenant `destroyed`. The tenant row and the audit log
stay.

Unwrapped data keys of the tenant leave memory as follows. The API process
that handles the destroy drops them at once. Ingest drops them at the
tenant's first batch after the destroy, when it finds the tenant no longer
active. Any other process, such as another API replica, drops them when they
expire. No unwrap works once the KEK is gone, so 5½ minutes after the KEK is
destroyed (5 minutes in the cache plus one 30-second sweep) no process holds
a data key of the tenant, even if no more batches arrive. An unwrap that was
already under way at that moment can add the time it takes to finish; the
remote keyring client gives up after 10 seconds.

**Limits, stated plainly:**

- Deleting a file on an SSD is not a secure erase. Production should use a
  KMS or Vault backend with real key destruction. The `Keyring` interface
  exists for that; only the local file backend is built.
- Back up the keyring directory separately, with a short retention, or a
  destroyed tenant's KEK survives in an old keyring backup
  ([Back up the keyring](../how-to/back-up-the-keyring.md)).
- Export files are plaintext for up to 24 hours. Put the export directory
  on an encrypted volume.
- Dropping a data key removes it from the caches; Go does not zero the
  memory. Its bytes stay in the process's heap until the memory is reused,
  readable by root or in a core dump.

## Tenant isolation

- Every tenant table has `tenant_id` and row-level security, enabled and
  forced. The policy compares it with a transaction-local setting.
- If the setting is missing, no row matches: forgetting it fails closed.
- Child rows (chunks, gaps, ranges) reference `devices (tenant_id, id)`, so
  a row cannot belong to another tenant's device.
- The few lookups that happen before the tenant is known (ingest by device
  ID, enrollment by token hash, authentication by token ID) are SECURITY
  DEFINER functions with a fixed search path that return only the columns
  they need.
- Tests try to read and change another tenant's data in SQL and through
  every API endpoint, and prove both fail (`internal/store`, `internal/api`).
