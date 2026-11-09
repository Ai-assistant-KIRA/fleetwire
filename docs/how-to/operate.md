# Operate fleetwire

## Bootstrap the first operator token

Operator tokens are created with the schema owner's credentials, from the
operator's shell; the runtime role cannot create them.

```console
$ FLEETWIRE_DATABASE_OWNER_URL=postgres://... fleetwire-api create-operator-token -name alice
fwo_...
```

In the Compose stack: `make operator-token`.

## Tenants

```console
$ fwctl tenant create -name acme -retention-days 90
$ fwctl token create -tenant <id> -name acme-admin -scopes tenant:admin,devices:admin,devices:read,readings:read,exports:read,exports:write,audit:read
$ fwctl tenant list
```

## Destroy a tenant

```console
$ fwctl tenant destroy <id> -yes
```

The call returns after the irreversible part: the tenant's devices are
revoked, its wrapped data keys deleted and its KEK removed from the
keyring. Rows are deleted by a background job; `fwctl tenant list` shows
`destroying`, then `destroyed`. See
[encryption and tenancy](../explanation/encryption-and-tenancy.md) for what
this does and does not cover.

## Revoke or delete a device

```console
$ fwctl device revoke <id>      # stop accepting its data now
$ fwctl device delete <id> -yes # revoke, then delete all its readings
```

Ingest drops the device's data from the moment of the revoke. The broker
refuses its next connection once Mosquitto has reloaded the CRL (the API
republishes it at once; the Compose stack's watcher sends SIGHUP).

## Rotate the CAs

1. Create the new root and issuing CAs.
2. Ship a CA bundle that holds **both** roots to devices and services
   (devices pick up the bundle at their next renewal and use it from their
   next start).
3. Switch the API to the new device issuing CA; new and renewed device
   certificates come from it.
4. Switch the broker and API server certificates.
5. After 30 days (one device certificate lifetime) remove the old root.

## Export data

```console
$ fwctl export create -device <id>
$ fwctl export status <export-id>
$ fwctl export download <export-id> -out data.ndjson.gz
```

`download` checks the file against the `Content-Digest` header before it
keeps it.

## Audit

```console
$ fwctl audit list                # tenant token: the tenant's log
$ fwctl -token "$OPERATOR" audit list   # operator: everything
```
