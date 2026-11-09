package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const jobCols = `id, tenant_id, kind, status, payload, result, attempts, max_attempts, last_error,
	run_after, locked_until, locked_by, dedupe_key, created_at, finished_at`

// EnqueueJob adds a job for the current tenant. With a dedupe key, a second
// job with the same key is ignored and EnqueueJob returns false.
func (t *Tx) EnqueueJob(ctx context.Context, id uuid.UUID, kind string, payload any, dedupeKey string, runAfter time.Time) (bool, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("encode job payload: %w", err)
	}
	var key *string
	if dedupeKey != "" {
		key = &dedupeKey
	}
	tag, err := t.tx.Exec(ctx, `
		INSERT INTO fleetwire.jobs (id, tenant_id, kind, payload, dedupe_key, run_after)
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (dedupe_key) DO NOTHING`,
		id, t.tenant, kind, body, key, runAfter)
	if err != nil {
		return false, fmt.Errorf("enqueue job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ClaimJobs leases up to n runnable jobs of the given kinds for worker.
// It works without a tenant context.
func (t *Tx) ClaimJobs(ctx context.Context, kinds []string, n int, lease time.Duration, worker string) ([]Job, error) {
	v, err := many[Job](t.tx.Query(ctx, `SELECT `+jobCols+` FROM fleetwire.claim_jobs($1, $2, $3, $4)`,
		kinds, n, lease, worker))
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	return v, nil
}

// GetJob returns one job of the current tenant.
func (t *Tx) GetJob(ctx context.Context, id uuid.UUID) (Job, error) {
	v, err := one[Job](t.tx.Query(ctx, `SELECT `+jobCols+` FROM fleetwire.jobs WHERE id = $1`, id))
	if err != nil {
		return Job{}, fmt.Errorf("get job: %w", err)
	}
	return v, nil
}

// ExtendJobLease keeps a long job leased to worker. It fails with
// ErrNotFound if the lease was lost to another worker.
func (t *Tx) ExtendJobLease(ctx context.Context, id uuid.UUID, worker string, lease time.Duration) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.jobs SET locked_until = now() + $3
		WHERE id = $1 AND locked_by = $2 AND status = 'running'`, id, worker, lease)
	if err != nil {
		return fmt.Errorf("extend job lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// FinishJob marks a job succeeded with its result.
func (t *Tx) FinishJob(ctx context.Context, id uuid.UUID, worker string, result any) error {
	body, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode job result: %w", err)
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.jobs SET status = 'succeeded', result = $3, finished_at = now(), locked_until = NULL
		WHERE id = $1 AND locked_by = $2 AND status = 'running'`, id, worker, body)
	if err != nil {
		return fmt.Errorf("finish job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// RetryJob releases a failed attempt. The job runs again after retryAt, or
// is marked failed for good when it has used all its attempts.
func (t *Tx) RetryJob(ctx context.Context, id uuid.UUID, worker, errMsg string, retryAt time.Time) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.jobs
		SET last_error = $3, locked_until = NULL, locked_by = NULL, run_after = $4,
		    status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
		    finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END
		WHERE id = $1 AND locked_by = $2`, id, worker, errMsg, retryAt)
	if err != nil {
		return fmt.Errorf("retry job: %w", err)
	}
	return nil
}

// HasPendingJob reports whether the current tenant has an unfinished job
// of kind (used to avoid starting a second tenant destroy).
func (t *Tx) HasPendingJob(ctx context.Context, kind string) (bool, error) {
	var ok bool
	err := t.tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM fleetwire.jobs WHERE kind = $1 AND status IN ('queued', 'running'))`, kind).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("pending job: %w", err)
	}
	return ok, nil
}

const exportCols = `id, tenant_id, device_id, status, file_path, sha256, bytes, reading_count, error,
	created_at, completed_at, expires_at`

// InsertExport creates a pending export for the current tenant.
func (t *Tx) InsertExport(ctx context.Context, id uuid.UUID, deviceID *uuid.UUID) (Export, error) {
	v, err := one[Export](t.tx.Query(ctx, `
		INSERT INTO fleetwire.exports (id, tenant_id, device_id) VALUES ($1, $2, $3) RETURNING `+exportCols,
		id, t.tenant, deviceID))
	if err != nil {
		return Export{}, fmt.Errorf("insert export: %w", err)
	}
	return v, nil
}

// GetExport returns one export of the current tenant.
func (t *Tx) GetExport(ctx context.Context, id uuid.UUID) (Export, error) {
	v, err := one[Export](t.tx.Query(ctx, `SELECT `+exportCols+` FROM fleetwire.exports WHERE id = $1`, id))
	if err != nil {
		return Export{}, fmt.Errorf("get export: %w", err)
	}
	return v, nil
}

// SetExportRunning marks an export as started.
func (t *Tx) SetExportRunning(ctx context.Context, id uuid.UUID) error {
	if _, err := t.tx.Exec(ctx, `UPDATE fleetwire.exports SET status = 'running' WHERE id = $1`, id); err != nil {
		return fmt.Errorf("export running: %w", err)
	}
	return nil
}

// CompleteExport records a finished export file.
func (t *Tx) CompleteExport(ctx context.Context, id uuid.UUID, path string, sum []byte, size, readings int64, expires time.Time) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.exports SET status = 'ready', file_path = $2, sha256 = $3, bytes = $4,
			reading_count = $5, completed_at = now(), expires_at = $6, error = NULL
		WHERE id = $1`, id, path, sum, size, readings, expires)
	if err != nil {
		return fmt.Errorf("complete export: %w", err)
	}
	return nil
}

// FailExport records why an export failed.
func (t *Tx) FailExport(ctx context.Context, id uuid.UUID, msg string) error {
	if _, err := t.tx.Exec(ctx, `UPDATE fleetwire.exports SET status = 'failed', error = $2 WHERE id = $1`, id, msg); err != nil {
		return fmt.Errorf("fail export: %w", err)
	}
	return nil
}

// ExpiredExports lists ready exports of the current tenant past their expiry.
func (t *Tx) ExpiredExports(ctx context.Context, now time.Time, limit int) ([]Export, error) {
	v, err := many[Export](t.tx.Query(ctx, `
		SELECT `+exportCols+` FROM fleetwire.exports
		WHERE status = 'ready' AND expires_at < $1 ORDER BY expires_at LIMIT $2`, now, limit))
	if err != nil {
		return nil, fmt.Errorf("expired exports: %w", err)
	}
	return v, nil
}

// ExportFiles lists every export file path of the current tenant.
func (t *Tx) ExportFiles(ctx context.Context) ([]string, error) {
	rows, err := t.tx.Query(ctx, `SELECT file_path FROM fleetwire.exports WHERE file_path IS NOT NULL`)
	if err != nil {
		return nil, fmt.Errorf("export files: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("export files: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// MarkExportExpired records that an export file was deleted.
func (t *Tx) MarkExportExpired(ctx context.Context, id uuid.UUID) error {
	if _, err := t.tx.Exec(ctx, `UPDATE fleetwire.exports SET status = 'expired', file_path = NULL WHERE id = $1`, id); err != nil {
		return fmt.Errorf("expire export: %w", err)
	}
	return nil
}
