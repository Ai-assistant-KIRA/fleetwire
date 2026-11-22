-- Row locks for certificate issuance.
--
-- Renewal and enrollment read the device's and the tenant's state and then
-- store a new certificate. Without locks, under READ COMMITTED, a device
-- revoke or a tenant destroy that committed between the read and the insert
-- went unnoticed: the new certificate was stored unrevoked, never reached
-- the CRL, and the broker accepted it for its whole lifetime.
--
-- Both now lock what they read until they commit. The tenant row is locked
-- first, FOR SHARE, so its status cannot change before the commit; then the
-- device row, FOR UPDATE. A tenant destroy takes the tenant row FOR UPDATE
-- and then updates the devices, the same order, so the two cannot deadlock.
-- A revoke or destroy that comes second waits, and its certificate update
-- then sees the new certificate and revokes it too.

-- +goose Up

-- lock_device is device_lookup with the locks above, for renewal. A device
-- never moves to another tenant, so the first, unlocked read only picks the
-- tenant row to lock.
-- +goose StatementBegin
CREATE FUNCTION fleetwire.lock_device(p_device uuid)
    RETURNS TABLE (tenant_id uuid, device_status text, tenant_status text)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
DECLARE
    v_tenant uuid;
    v_tenant_status text;
    v_device_status text;
BEGIN
    SELECT d.tenant_id INTO v_tenant FROM fleetwire.devices d WHERE d.id = p_device;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    SELECT t.status INTO v_tenant_status FROM fleetwire.tenants t WHERE t.id = v_tenant FOR SHARE;
    SELECT d.status INTO v_device_status FROM fleetwire.devices d WHERE d.id = p_device FOR UPDATE;
    IF NOT FOUND THEN
        -- Deleted by a tenant destroy while this waited for the lock.
        RETURN;
    END IF;
    RETURN QUERY SELECT v_tenant, v_device_status, v_tenant_status;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION fleetwire.lock_device(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION fleetwire.lock_device(uuid) TO {{runtime_role}};

-- Unchanged from 00003 except for FOR SHARE on the tenant row, which the
-- caller's transaction holds while it inserts the device and certificate.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION fleetwire.consume_enrollment_token(p_hash bytea, p_device uuid)
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
    SELECT status INTO t_status FROM fleetwire.tenants WHERE id = tok.tenant_id FOR SHARE;
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

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION fleetwire.consume_enrollment_token(p_hash bytea, p_device uuid)
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
DROP FUNCTION fleetwire.lock_device(uuid);
