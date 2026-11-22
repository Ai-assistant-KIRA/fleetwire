-- claim_jobs stops at max_attempts.
--
-- A job is marked failed for good by RetryJob, which runs only when the
-- attempt returns. A job whose worker died during every attempt (the job
-- crashed or ran the process out of memory) never got there: each lease
-- expiry made it runnable again, attempts grew past max_attempts, and the
-- job ran forever. claim_jobs now fails such a job instead of claiming it,
-- and with it the export of an export job, which would otherwise stay
-- running forever.

-- +goose Up

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION fleetwire.claim_jobs(p_kinds text[], p_n int, p_lease interval, p_worker text)
    RETURNS SETOF fleetwire.jobs
    LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, fleetwire, pg_temp
AS $$
    WITH spent AS (
        SELECT id FROM fleetwire.jobs
        WHERE status IN ('queued', 'running')
          AND kind = ANY (p_kinds)
          AND attempts >= max_attempts
          AND run_after <= now()
          AND (locked_until IS NULL OR locked_until < now())
        FOR UPDATE SKIP LOCKED
    ), failed AS (
        UPDATE fleetwire.jobs j
        SET status = 'failed', finished_at = now(), locked_until = NULL, locked_by = NULL,
            last_error = 'the worker stopped during the last attempt'
        FROM spent
        WHERE j.id = spent.id
        RETURNING j.tenant_id, j.kind, j.payload ->> 'export_id' AS export_id
    )
    -- The CASE casts the export ID only when it is a well-formed UUID, so a
    -- malformed payload cannot make every claim fail.
    UPDATE fleetwire.exports e
    SET status = 'failed', error = 'the export failed; create a new one'
    FROM failed f
    WHERE f.kind = 'export'
      AND e.id = CASE WHEN f.export_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                      THEN f.export_id::uuid END
      AND e.tenant_id = f.tenant_id
      AND e.status IN ('pending', 'running');

    WITH c AS (
        SELECT id FROM fleetwire.jobs
        WHERE status IN ('queued', 'running')
          AND kind = ANY (p_kinds)
          AND attempts < max_attempts
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

-- +goose Down

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION fleetwire.claim_jobs(p_kinds text[], p_n int, p_lease interval, p_worker text)
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
