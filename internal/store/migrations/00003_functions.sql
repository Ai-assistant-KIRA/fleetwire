-- SECURITY DEFINER lookups for the few operations that happen before the
-- tenant is known. Each runs as the owner with a fixed
-- search_path, returns only the columns its caller needs, and can be
-- executed by the runtime role only.

-- +goose Up

-- +goose StatementBegin
CREATE FUNCTION fleetwire.device_for_ingest(p_device uuid)
    RETURNS TABLE (tenant_id uuid, device_status text, tenant_status text, acked_through bigint)
    LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    -- The row lock is held by the caller's transaction until it ends. It
    -- serializes all ingest work for one device across ingest instances.
    SELECT d.tenant_id, d.status, t.status, d.acked_through
    FROM fleetwire.devices d
    JOIN fleetwire.tenants t ON t.id = d.tenant_id
    WHERE d.id = p_device
    FOR UPDATE OF d
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION fleetwire.device_lookup(p_device uuid)
    RETURNS TABLE (tenant_id uuid, device_status text, tenant_status text)
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    SELECT d.tenant_id, d.status, t.status
    FROM fleetwire.devices d
    JOIN fleetwire.tenants t ON t.id = d.tenant_id
    WHERE d.id = p_device
$$;
-- +goose StatementEnd

-- Consumes a one-time enrollment token atomically. outcome is one of ok,
-- unknown, expired, used, tenant_inactive. tenant_id is set for every
-- outcome except unknown, so failures can be audited under the tenant.
-- +goose StatementBegin
CREATE FUNCTION fleetwire.consume_enrollment_token(p_hash bytea, p_device uuid)
    RETURNS TABLE (tenant_id uuid, outcome text)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
DECLARE
    tok fleetwire.enrollment_tokens%ROWTYPE;
    t_status text;
BEGIN
    SELECT * INTO tok FROM fleetwire.enrollment_tokens WHERE token_hash = p_hash FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT NULL::uuid, 'unknown'::text;
        RETURN;
    END IF;
    SELECT status INTO t_status FROM fleetwire.tenants WHERE id = tok.tenant_id;
    IF tok.used_at IS NOT NULL THEN
        RETURN QUERY SELECT tok.tenant_id, 'used'::text;
    ELSIF tok.expires_at <= now() THEN
        RETURN QUERY SELECT tok.tenant_id, 'expired'::text;
    ELSIF t_status <> 'active' THEN
        RETURN QUERY SELECT tok.tenant_id, 'tenant_inactive'::text;
    ELSE
        UPDATE fleetwire.enrollment_tokens SET used_at = now(), used_by_device = p_device WHERE id = tok.id;
        RETURN QUERY SELECT tok.tenant_id, 'ok'::text;
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION fleetwire.api_token_lookup(p_id uuid)
    RETURNS TABLE (tenant_id uuid, token_hash bytea, scopes text[], revoked_at timestamptz, tenant_status text)
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    SELECT k.tenant_id, k.token_hash, k.scopes, k.revoked_at, t.status
    FROM fleetwire.api_tokens k
    JOIN fleetwire.tenants t ON t.id = k.tenant_id
    WHERE k.id = p_id
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION fleetwire.operator_token_lookup(p_id uuid)
    RETURNS TABLE (name text, token_hash bytea, revoked_at timestamptz)
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    SELECT name, token_hash, revoked_at FROM fleetwire.operator_tokens WHERE id = p_id
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION fleetwire.touch_operator_token(p_id uuid)
    RETURNS void
    LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    UPDATE fleetwire.operator_tokens SET last_used_at = now() WHERE id = p_id
$$;
-- +goose StatementEnd

-- Leases up to p_n runnable jobs. SKIP LOCKED lets several workers claim in
-- parallel without blocking each other; a job whose lease expired (its
-- worker died) becomes runnable again.
-- +goose StatementBegin
CREATE FUNCTION fleetwire.claim_jobs(p_kinds text[], p_n int, p_lease interval, p_worker text)
    RETURNS SETOF fleetwire.jobs
    LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    WITH c AS (
        SELECT id FROM fleetwire.jobs
        WHERE status IN ('queued', 'running')
          AND kind = ANY (p_kinds)
          AND run_after <= now()
          AND (locked_until IS NULL OR locked_until < now())
        ORDER BY run_after
        LIMIT p_n
        FOR UPDATE SKIP LOCKED
    )
    UPDATE fleetwire.jobs j
    SET status = 'running', locked_until = now() + p_lease, locked_by = p_worker, attempts = j.attempts + 1
    FROM c
    WHERE j.id = c.id
    RETURNING j.*
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION fleetwire.list_active_tenants()
    RETURNS TABLE (id uuid, retention_days int)
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    SELECT id, retention_days FROM fleetwire.tenants WHERE status = 'active' ORDER BY id
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION fleetwire.list_tenants(p_after uuid, p_limit int)
    RETURNS TABLE (id uuid, name text, status text, retention_days int, created_at timestamptz, destroyed_at timestamptz)
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    SELECT id, name, status, retention_days, created_at, destroyed_at
    FROM fleetwire.tenants
    WHERE p_after IS NULL OR id > p_after
    ORDER BY id
    LIMIT p_limit
$$;
-- +goose StatementEnd

-- Fleet-wide audit entries, newest first, for the operator API.
-- +goose StatementBegin
CREATE FUNCTION fleetwire.operator_audit(p_before bigint, p_limit int)
    RETURNS SETOF fleetwire.audit_log
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    SELECT * FROM fleetwire.audit_log
    WHERE p_before IS NULL OR id < p_before
    ORDER BY id DESC
    LIMIT p_limit
$$;
-- +goose StatementEnd

-- Every revoked certificate that has not expired yet, for the CRL.
-- +goose StatementBegin
CREATE FUNCTION fleetwire.revoked_device_certs()
    RETURNS TABLE (serial text, revoked_at timestamptz)
    LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    SELECT serial, revoked_at FROM fleetwire.device_certs
    WHERE revoked_at IS NOT NULL AND not_after > now()
    ORDER BY serial
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION fleetwire.delete_expired_device_certs()
    RETURNS bigint
    LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    WITH d AS (DELETE FROM fleetwire.device_certs WHERE not_after < now() - interval '1 day' RETURNING 1)
    SELECT count(*) FROM d
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION fleetwire.device_for_ingest(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.device_lookup(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.consume_enrollment_token(bytea, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.api_token_lookup(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.operator_token_lookup(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.touch_operator_token(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.claim_jobs(text[], int, interval, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.list_active_tenants() FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.list_tenants(uuid, int) FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.operator_audit(bigint, int) FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.revoked_device_certs() FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.delete_expired_device_certs() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION fleetwire.device_for_ingest(uuid) TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.device_lookup(uuid) TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.consume_enrollment_token(bytea, uuid) TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.api_token_lookup(uuid) TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.operator_token_lookup(uuid) TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.touch_operator_token(uuid) TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.claim_jobs(text[], int, interval, text) TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.list_active_tenants() TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.list_tenants(uuid, int) TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.operator_audit(bigint, int) TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.revoked_device_certs() TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.delete_expired_device_certs() TO {{runtime_role}};

-- The audit log is append-only even for the owner. Grants alone would allow
-- a later mistaken GRANT UPDATE to break that; the trigger does not.
-- +goose StatementBegin
CREATE FUNCTION fleetwire.audit_log_is_append_only() RETURNS trigger
    LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only (% blocked)', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END
$$;
-- +goose StatementEnd

CREATE TRIGGER audit_log_no_update_delete
    BEFORE UPDATE OR DELETE ON fleetwire.audit_log
    FOR EACH ROW EXECUTE FUNCTION fleetwire.audit_log_is_append_only();
CREATE TRIGGER audit_log_no_truncate
    BEFORE TRUNCATE ON fleetwire.audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION fleetwire.audit_log_is_append_only();

-- +goose Down
DROP TRIGGER audit_log_no_truncate ON fleetwire.audit_log;
DROP TRIGGER audit_log_no_update_delete ON fleetwire.audit_log;
DROP FUNCTION fleetwire.audit_log_is_append_only();
DROP FUNCTION fleetwire.delete_expired_device_certs();
DROP FUNCTION fleetwire.revoked_device_certs();
DROP FUNCTION fleetwire.operator_audit(bigint, int);
DROP FUNCTION fleetwire.list_tenants(uuid, int);
DROP FUNCTION fleetwire.list_active_tenants();
DROP FUNCTION fleetwire.claim_jobs(text[], int, interval, text);
DROP FUNCTION fleetwire.touch_operator_token(uuid);
DROP FUNCTION fleetwire.operator_token_lookup(uuid);
DROP FUNCTION fleetwire.api_token_lookup(uuid);
DROP FUNCTION fleetwire.consume_enrollment_token(bytea, uuid);
DROP FUNCTION fleetwire.device_lookup(uuid);
DROP FUNCTION fleetwire.device_for_ingest(uuid);
