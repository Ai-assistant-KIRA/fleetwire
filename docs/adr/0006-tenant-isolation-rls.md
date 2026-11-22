# ADR-0006: Tenant isolation with forced row-level security and SECURITY DEFINER lookups

- Status: accepted
- Date: 2026-11-09

## Context

Each tenant must only ever see its own data. Application checks alone
fail the day one query forgets its `WHERE tenant_id = ...`. The same database
also serves a few lookups that happen before the tenant is known: ingest
learns the tenant from the device ID, enrollment from the token, the API from
the token ID.

## Decision

- Every tenant table has `tenant_id` and row-level security, enabled and
  **forced**. The policy for the runtime role compares `tenant_id` with
  `fleetwire.current_tenant()`, which reads the transaction-local setting
  `fleetwire.tenant_id`.
- `current_tenant()` wraps `current_setting(..., true)` in `NULLIF(..., '')`.
  Once a placeholder setting has been used on a connection, PostgreSQL returns
  an empty string instead of NULL; without `NULLIF` every query on a reused
  pooled connection would fail with a cast error instead of matching nothing.
- The policies and grants name one **runtime role**. In Compose and the
  tests it is `fleetwire_app` (`NOBYPASSRLS`, `NOINHERIT`, owns nothing),
  created by the deployment. `fleetwire-api migrate` takes the name as a
  parameter (`FLEETWIRE_DATABASE_RUNTIME_ROLE`), because Charmed PostgreSQL
  cannot create roles for an application: there the runtime role is the
  charm's `charmed_dml` group and the services log in as relation users that
  belong to it (ADR-0012). Policies on a group apply to its members.
- At start the services call `CheckRuntimeRole` and refuse to run as a role
  that is a superuser, can bypass RLS, or owns the tables.
- `store.InTenantTx` is the only way to reach tenant data. It runs
  `set_config('fleetwire.tenant_id', $1, true)`; the `true` makes it local to
  the transaction, so it cannot leak to the next borrower of the connection.
- Pre-tenant lookups are SECURITY DEFINER functions owned by the migration
  role, with `search_path = pg_catalog, fleetwire, pg_temp`, `EXECUTE`
  revoked from `PUBLIC` and granted to the runtime role only, each returning
  the minimum columns. `device_for_ingest` also takes the device row lock.
- Because RLS is forced, the owner is subject to it too. The owner gets an
  explicit permissive policy (`TO CURRENT_USER USING (true)`) instead of the
  `BYPASSRLS` attribute, because only a superuser can grant `BYPASSRLS` and
  managed or charmed PostgreSQL does not give out a superuser. The owner is
  never a runtime role.
- Child tables reference `devices (tenant_id, id)` with composite foreign
  keys, so a chunk, gap or range cannot belong to a device of another tenant.
- The audit log has separate read and write policies: a tenant reads only its
  rows; a row with `tenant_id IS NULL` (fleet-wide action) can only be written
  outside a tenant transaction, and is read through `operator_audit()`.
- Operator tokens are not tenant data: the runtime role has no privilege on
  that table at all, and new operator tokens are minted only with the owner's
  credentials from the operator's shell.

## Alternatives considered

- **Schema or database per tenant.** Strong isolation, but migrations and
  connection pools multiply with the tenant count, and cross-tenant operator
  views need dynamic SQL.
- **Application-side filtering only.** One missed `WHERE` leaks data; RLS
  makes the safe path the only path.
- **`BYPASSRLS` owner for the definer functions.** Simpler policies, but
  needs a superuser and does not work on managed PostgreSQL.

## Consequences

- Forgetting to set the tenant returns no rows instead of everything.
- Every query on tenant tables pays for the policy predicate; the indexes
  lead with `tenant_id` or are selective on `device_id`.
- Integration tests prove the isolation at the SQL level: reads, updates,
  deletes and inserts across tenants, the missing-tenant case on a reused
  connection, audit immutability for both roles, and single use of enrollment
  tokens under concurrency.
- The definer functions are a small, reviewed attack surface; adding one is a
  security-relevant change.
- The isolation is against mistakes in tenant scoping, not against a
  compromised service: any session of the runtime role can set
  `fleetwire.tenant_id` to any tenant. What a compromised service cannot do
  is alter the schema, drop the policies, or rewrite the audit log, because
  it is not the owner.
