# Back up the keyring

Reading a tenant's data takes three pieces, kept in three places
([encryption and tenancy](../explanation/encryption-and-tenancy.md)):

| Piece | Compose | Kubernetes | Changes when |
|---|---|---|---|
| Root key, 32 bytes (`FLEETWIRE_KEYRING_ROOT_KEY_FILE`) | `deploy/compose/secrets/keyring-root.key` | the Juju secret in the API charm's `pki-secret` option, key `keyring-root-key` | never |
| One KEK file per tenant, `<tenant-id>.kek`, sealed with the root key (`FLEETWIRE_KEYRING_DIR`) | `deploy/compose/.state/keyring/` | `/var/lib/fleetwire/keyring` on the API unit's `data` storage | a tenant is created (new file) or destroyed (file deleted) |
| Wrapped data keys | `fleetwire.tenant_keys` in PostgreSQL | the same | a data key rotates (2^24 chunks or 90 days), a tenant is destroyed |

Without the root key no KEK opens, and without a tenant's KEK file none
of its readings do. Keyring backups kept too long have the opposite
problem: destroying a tenant does not reach them.

## Retention

Destroying a tenant clears its wrapped data keys in the database and
deletes its KEK file. A database backup from before the destroy still has
the wrapped keys; a keyring backup from before it still has the KEK. Both
together, with the root key, open the tenant's readings again. So:

- Keep keyring backups only as long as a destroyed tenant's data may stay
  recoverable. The destroy is complete when the last keyring backup taken
  before it is deleted.
- Database backups can keep their usual retention: without the KEK, the
  wrapped keys in them open nothing.
- Store the root key apart from the keyring backups. A KEK file is sealed
  with the root key, so a keyring archive alone opens nothing, and the
  root key alone has no KEK to open.

## When to back up

The API writes a KEK file once, when it creates the tenant, and never
changes it. A tenant created after the last backup has no copy, so back
up right after `fwctl tenant create`, and on a schedule as well. The root
key never changes: copy it once, to somewhere outside the deployment.

## Compose or a single host

```console
$ tar -C deploy/compose/.state/keyring --exclude='*.tmp' \
    -czf "keyring-$(date -u +%Y%m%dT%H%M%SZ).tar.gz" .
```

The copy is consistent while the API runs. The API writes each KEK to a
`.tmp` file, fsyncs it and links it into place, so every `.kek` file is
complete; the `.tmp` files are the ones still being written.

To restore:

1. Note which tenants are destroyed or being destroyed: `fwctl tenant list`
   shows `destroying` or `destroyed` in `STATUS`.
2. Stop the services that read the keyring:
   `docker compose -f deploy/compose/compose.yaml stop api ingest-1 ingest-2`.
3. Unpack the archive into the keyring directory and delete the `.kek`
   files of the tenants from step 1, so the restore does not bring their
   keys back. Files stay mode 0600, the directory 0700.
4. Start the services again with `start` instead of `stop`.

A KEK file that does not match the root key fails at its first use with
`keyring: cannot open KEK for tenant ... (wrong root key?)`.

## Kubernetes

The charm has no backup action yet, and the workload image has no shell,
so `juju ssh --container api` cannot run `tar`. Back up the API unit's
`data` volume with your storage's volume snapshots, under the keyring's
short retention: the volume also holds the CRL and the export files, and
those hold plaintext readings until they expire. The root key is the
`keyring-root-key` value of the API's PKI secret; keep a copy of it
outside the Juju model.
