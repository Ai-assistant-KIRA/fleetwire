package jobs

import (
	"context"
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
