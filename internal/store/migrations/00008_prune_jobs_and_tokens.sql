-- Pruning of finished jobs and expired enrollment tokens.
--
-- Both tables only grew: the scheduler queues a retention job per tenant
-- per hour (8,760 rows per tenant a year), and every enrollment token stayed
-- after it was used or expired. The scheduler now calls these functions,
-- fleet-wide like delete_expired_device_certs, so the rows of destroyed
-- tenants go as well. The retention periods are fixed here rather than
-- passed in, so the runtime role cannot widen what they delete.
--
-- Jobs: 30 days after they finished. GET /v1/jobs/{id} answers 404 after
-- that; the audit log keeps what the jobs did. A job that is still queued or
-- running is never deleted, and the dedupe keys of deleted jobs are of
-- hours, exports and destroyed tenants or devices that cannot come back.
--
-- Enrollment tokens: 7 days after they expired, used or not. An expired
-- token can never be used, but until it is deleted an attempt with it is
-- still audited under its tenant.

-- +goose Up
CREATE INDEX jobs_finished ON fleetwire.jobs (finished_at) WHERE status IN ('succeeded', 'failed');
CREATE INDEX enrollment_tokens_expiry ON fleetwire.enrollment_tokens (expires_at);

-- +goose StatementBegin
CREATE FUNCTION fleetwire.prune_finished_jobs(p_limit int)
    RETURNS bigint
    LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    WITH d AS (
        DELETE FROM fleetwire.jobs WHERE id IN (
            SELECT id FROM fleetwire.jobs
            WHERE status IN ('succeeded', 'failed') AND finished_at < now() - interval '30 days'
            LIMIT p_limit)
        RETURNING 1)
    SELECT count(*) FROM d
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION fleetwire.prune_enrollment_tokens(p_limit int)
    RETURNS bigint
    LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    WITH d AS (
        DELETE FROM fleetwire.enrollment_tokens WHERE id IN (
            SELECT id FROM fleetwire.enrollment_tokens
            WHERE expires_at < now() - interval '7 days'
            LIMIT p_limit)
        RETURNING 1)
    SELECT count(*) FROM d
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION fleetwire.prune_finished_jobs(int) FROM PUBLIC;
REVOKE ALL ON FUNCTION fleetwire.prune_enrollment_tokens(int) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION fleetwire.prune_finished_jobs(int) TO {{runtime_role}};
GRANT EXECUTE ON FUNCTION fleetwire.prune_enrollment_tokens(int) TO {{runtime_role}};

-- +goose Down
DROP FUNCTION fleetwire.prune_enrollment_tokens(int);
DROP FUNCTION fleetwire.prune_finished_jobs(int);
DROP INDEX fleetwire.enrollment_tokens_expiry;
DROP INDEX fleetwire.jobs_finished;
