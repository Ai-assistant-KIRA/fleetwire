package jobs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

// TestDestroyRevokesDevicesLeftActive: a device that is still active when
// the destroy job runs (it enrolled past the API's first step) is revoked
// by the job, even though the tenant is already destroying.
func TestDestroyRevokesDevicesLeftActive(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	_, serial := e.device(tenant)
	e.inTenant(tenant, func(ctx context.Context, tx *store.Tx) error {
		return tx.SetTenantStatus(ctx, store.TenantDestroying)
	})
	job := e.enqueue(tenant, store.JobTenantDestroy, map[string]any{"tenant_id": tenant}, "")
	e.runJobs()
	if j := e.job(tenant, job); j.Status != store.JobSucceeded {
		t.Fatalf("destroy job %s: %s", j.Status, lastError(j))
	}
	if st := e.tenantStatus(tenant); st != store.TenantDestroyed {
		t.Fatalf("tenant %s, want destroyed", st)
	}
	if !e.revoked(serial) {
		t.Fatal("the certificate of a device left active is not on the CRL after the destroy")
	}
}

// TestExportFailedForGoodFailsTheExport: when an export job uses its last
// attempt, the export is marked failed instead of staying running.
func TestExportFailedForGoodFailsTheExport(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	exp, job := e.export(tenant)
	e.asOwner(`UPDATE fleetwire.jobs SET max_attempts = 1 WHERE id = $1`, job)
	// A file where the export directory should be makes every attempt fail.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	e.w.cfg.ExportDir = blocker
	e.runJobs()
	if j := e.job(tenant, job); j.Status != store.JobFailed {
		t.Fatalf("job %s, want failed", j.Status)
	}
	got := e.getExport(tenant, exp)
	if got.Status != store.ExportFailed || got.Error == nil || *got.Error != exportFailed {
		t.Fatalf("export %s (error %v), want failed with %q", got.Status, got.Error, exportFailed)
	}
}

// TestDestroyCanBeQueuedAgainAfterItFailed: a destroy job that failed for
// good no longer holds its dedupe key, so asking again queues a new job,
// and it finishes the destroy.
func TestDestroyCanBeQueuedAgainAfterItFailed(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	key := "tenant_destroy:" + tenant.String()
	first := e.enqueue(tenant, store.JobTenantDestroy, map[string]any{"tenant_id": tenant}, key)
	if again := e.enqueue(tenant, store.JobTenantDestroy, map[string]any{"tenant_id": tenant}, key); again != first {
		t.Fatalf("a second request queued job %s next to the queued %s", again, first)
	}
	e.asOwner(`UPDATE fleetwire.jobs SET status = 'failed', finished_at = now() WHERE id = $1`, first)
	second := e.enqueue(tenant, store.JobTenantDestroy, map[string]any{"tenant_id": tenant}, key)
	if second == first {
		t.Fatal("the failed job still holds the dedupe key")
	}
	e.runJobs()
	if j := e.job(tenant, second); j.Status != store.JobSucceeded {
		t.Fatalf("new destroy job %s: %s", j.Status, lastError(j))
	}
	if st := e.tenantStatus(tenant); st != store.TenantDestroyed {
		t.Fatalf("tenant %s, want destroyed", st)
	}
	if again := e.enqueue(tenant, store.JobTenantDestroy, map[string]any{"tenant_id": tenant}, key); again != second {
		t.Fatalf("a succeeded job must keep its key: got %s, want %s", again, second)
	}
}
