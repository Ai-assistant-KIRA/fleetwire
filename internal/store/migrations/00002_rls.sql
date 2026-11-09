-- Row-level security (R7, ADR-0006).
--
-- {{runtime_role}} is the role the services run as; store.Migrate replaces
-- it with the configured name, quoted, before goose reads this file. It is
-- fleetwire_app in Compose and in the tests, and charmed_dml on Charmed
-- PostgreSQL, where every runtime relation user is a member of that group.
-- Policies on a role apply to its members, so a group works as well as a
-- login role.
--
-- The runtime role sees only rows of the tenant set with
-- set_config('fleetwire.tenant_id', ..., true) in the current transaction.
-- If the setting is missing, current_tenant() is NULL and no row matches, so
-- forgetting to set it fails closed.
--
-- RLS is forced, so it applies to the owner too. The owner (the role running
-- these migrations) gets an explicit permissive policy instead of BYPASSRLS,
-- because BYPASSRLS can only be granted by a superuser and managed or
-- charmed PostgreSQL does not hand one out. The owner is never used at
-- runtime; it owns the SECURITY DEFINER lookups in 00003.
-- +goose Up
-- The runtime role must exist before this runs; the deployment creates it
-- (the Compose init script, the test setup, or Charmed PostgreSQL).

-- current_setting(..., true) returns '' rather than NULL once the
-- placeholder has been used in a session, hence the NULLIF.
CREATE FUNCTION fleetwire.current_tenant() RETURNS uuid
    LANGUAGE sql STABLE PARALLEL SAFE
    AS $$ SELECT NULLIF(pg_catalog.current_setting('fleetwire.tenant_id', true), '')::uuid $$;

ALTER TABLE fleetwire.tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.tenants TO {{runtime_role}}
    USING (id = fleetwire.current_tenant())
    WITH CHECK (id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.tenants TO CURRENT_USER USING (true) WITH CHECK (true);

ALTER TABLE fleetwire.tenant_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.tenant_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.tenant_keys TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant())
    WITH CHECK (tenant_id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.tenant_keys TO CURRENT_USER USING (true) WITH CHECK (true);

ALTER TABLE fleetwire.devices ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.devices FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.devices TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant())
    WITH CHECK (tenant_id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.devices TO CURRENT_USER USING (true) WITH CHECK (true);

ALTER TABLE fleetwire.device_ranges ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.device_ranges FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.device_ranges TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant())
    WITH CHECK (tenant_id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.device_ranges TO CURRENT_USER USING (true) WITH CHECK (true);

ALTER TABLE fleetwire.device_gaps ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.device_gaps FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.device_gaps TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant())
    WITH CHECK (tenant_id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.device_gaps TO CURRENT_USER USING (true) WITH CHECK (true);

ALTER TABLE fleetwire.chunks ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.chunks FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.chunks TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant())
    WITH CHECK (tenant_id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.chunks TO CURRENT_USER USING (true) WITH CHECK (true);

ALTER TABLE fleetwire.device_certs ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.device_certs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.device_certs TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant())
    WITH CHECK (tenant_id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.device_certs TO CURRENT_USER USING (true) WITH CHECK (true);

ALTER TABLE fleetwire.enrollment_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.enrollment_tokens FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.enrollment_tokens TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant())
    WITH CHECK (tenant_id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.enrollment_tokens TO CURRENT_USER USING (true) WITH CHECK (true);

ALTER TABLE fleetwire.api_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.api_tokens FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.api_tokens TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant())
    WITH CHECK (tenant_id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.api_tokens TO CURRENT_USER USING (true) WITH CHECK (true);

ALTER TABLE fleetwire.jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.jobs TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant())
    WITH CHECK (tenant_id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.jobs TO CURRENT_USER USING (true) WITH CHECK (true);

ALTER TABLE fleetwire.exports ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.exports FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON fleetwire.exports TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant())
    WITH CHECK (tenant_id = fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.exports TO CURRENT_USER USING (true) WITH CHECK (true);

-- The audit log: a tenant reads only its own rows. Rows without a tenant
-- (fleet-wide operator actions, failed enrollments with unknown tokens) can
-- only be written outside a tenant transaction and are read only through
-- operator_audit().
ALTER TABLE fleetwire.audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.audit_log FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_read ON fleetwire.audit_log FOR SELECT TO {{runtime_role}}
    USING (tenant_id = fleetwire.current_tenant());
CREATE POLICY tenant_write ON fleetwire.audit_log FOR INSERT TO {{runtime_role}}
    WITH CHECK (tenant_id IS NOT DISTINCT FROM fleetwire.current_tenant());
CREATE POLICY owner_access ON fleetwire.audit_log TO CURRENT_USER USING (true) WITH CHECK (true);

-- Least privilege for the runtime role: only what the services use.
GRANT USAGE ON SCHEMA fleetwire TO {{runtime_role}};
GRANT SELECT, INSERT, UPDATE ON fleetwire.tenants TO {{runtime_role}};
GRANT SELECT, INSERT, UPDATE ON fleetwire.tenant_keys TO {{runtime_role}};
GRANT SELECT, INSERT, UPDATE, DELETE ON fleetwire.devices TO {{runtime_role}};
GRANT SELECT, INSERT, UPDATE, DELETE ON fleetwire.device_ranges TO {{runtime_role}};
GRANT SELECT, INSERT, DELETE ON fleetwire.device_gaps TO {{runtime_role}};
GRANT SELECT, INSERT, DELETE ON fleetwire.chunks TO {{runtime_role}};
GRANT SELECT, INSERT, UPDATE ON fleetwire.device_certs TO {{runtime_role}};
GRANT SELECT, INSERT, UPDATE, DELETE ON fleetwire.enrollment_tokens TO {{runtime_role}};
GRANT SELECT, INSERT, UPDATE, DELETE ON fleetwire.api_tokens TO {{runtime_role}};
GRANT SELECT, INSERT, UPDATE, DELETE ON fleetwire.jobs TO {{runtime_role}};
GRANT SELECT, INSERT, UPDATE, DELETE ON fleetwire.exports TO {{runtime_role}};
GRANT SELECT, INSERT ON fleetwire.audit_log TO {{runtime_role}};

-- +goose Down
REVOKE ALL ON ALL TABLES IN SCHEMA fleetwire FROM {{runtime_role}};
REVOKE USAGE ON SCHEMA fleetwire FROM {{runtime_role}};
DROP POLICY tenant_isolation ON fleetwire.tenants;
DROP POLICY owner_access ON fleetwire.tenants;
DROP POLICY tenant_isolation ON fleetwire.tenant_keys;
DROP POLICY owner_access ON fleetwire.tenant_keys;
DROP POLICY tenant_isolation ON fleetwire.devices;
DROP POLICY owner_access ON fleetwire.devices;
DROP POLICY tenant_isolation ON fleetwire.device_ranges;
DROP POLICY owner_access ON fleetwire.device_ranges;
DROP POLICY tenant_isolation ON fleetwire.device_gaps;
DROP POLICY owner_access ON fleetwire.device_gaps;
DROP POLICY tenant_isolation ON fleetwire.chunks;
DROP POLICY owner_access ON fleetwire.chunks;
DROP POLICY tenant_isolation ON fleetwire.device_certs;
DROP POLICY owner_access ON fleetwire.device_certs;
DROP POLICY tenant_isolation ON fleetwire.enrollment_tokens;
DROP POLICY owner_access ON fleetwire.enrollment_tokens;
DROP POLICY tenant_isolation ON fleetwire.api_tokens;
DROP POLICY owner_access ON fleetwire.api_tokens;
DROP POLICY tenant_isolation ON fleetwire.jobs;
DROP POLICY owner_access ON fleetwire.jobs;
DROP POLICY tenant_isolation ON fleetwire.exports;
DROP POLICY owner_access ON fleetwire.exports;
DROP POLICY tenant_read ON fleetwire.audit_log;
DROP POLICY tenant_write ON fleetwire.audit_log;
DROP POLICY owner_access ON fleetwire.audit_log;
ALTER TABLE fleetwire.tenants DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.tenant_keys DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.devices DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.device_ranges DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.device_gaps DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.chunks DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.device_certs DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.enrollment_tokens DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.api_tokens DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.jobs DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.exports DISABLE ROW LEVEL SECURITY;
ALTER TABLE fleetwire.audit_log DISABLE ROW LEVEL SECURITY;
DROP FUNCTION fleetwire.current_tenant();
