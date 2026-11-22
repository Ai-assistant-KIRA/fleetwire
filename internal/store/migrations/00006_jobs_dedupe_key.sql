-- A dedupe key no longer outlives a job that failed for good.
--
-- dedupe_key was unique across every job ever queued. When a tenant destroy
-- or device purge failed for good, its key stayed taken, every later request
-- to queue it again was dropped, and the tenant or device was stuck. The key
-- is now unique only among jobs that are queued, running or succeeded: a
-- failed job can be queued again, while the retention job of an hour that
-- already ran still holds that hour's key, so retention does not run again
-- on every scheduler tick.

-- +goose Up
ALTER TABLE fleetwire.jobs DROP CONSTRAINT jobs_dedupe_key_key;
CREATE UNIQUE INDEX jobs_dedupe_key ON fleetwire.jobs (dedupe_key)
    WHERE status IN ('queued', 'running', 'succeeded');

-- +goose Down
-- The old constraint covers failed jobs as well, and a failed job may now
-- share its key with a newer one, so failed jobs give up their keys first.
DROP INDEX fleetwire.jobs_dedupe_key;
UPDATE fleetwire.jobs SET dedupe_key = NULL WHERE status = 'failed';
ALTER TABLE fleetwire.jobs ADD CONSTRAINT jobs_dedupe_key_key UNIQUE (dedupe_key);
