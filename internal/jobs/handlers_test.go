package jobs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

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

// TestRetentionDeletesOnlyFilesInTheExportDir: an expired export whose
// recorded path points outside the export directory is marked expired, but
// the file it names is not deleted.
func TestRetentionDeletesOnlyFilesInTheExportDir(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	inside := filepath.Join(e.exportDir, tenant.String(), "inside.ndjson.gz")
	outside := filepath.Join(t.TempDir(), "precious")
	for _, p := range []string{inside, outside} {
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// The second one escapes through a relative path, the third is absolute.
	paths := []string{inside, filepath.Join(e.exportDir, "..", filepath.Base(filepath.Dir(outside)), "precious"), outside}
	var ids []uuid.UUID
	for _, p := range paths {
		id, _ := e.export(tenant)
		e.asOwner(`UPDATE fleetwire.exports SET status = 'ready', file_path = $2, expires_at = now() - interval '1 hour'
			WHERE id = $1`, id, p)
		ids = append(ids, id)
	}
	// The export jobs see a ready export and do nothing.
	e.enqueue(tenant, store.JobRetention, struct{}{}, "")
	e.runJobs()
	if _, err := os.Stat(inside); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the expired export inside the export dir is still there: %v", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Errorf("retention deleted a file outside the export dir: %v", err)
	}
	for _, id := range ids {
		if got := e.getExport(tenant, id); got.Status != store.ExportExpired {
			t.Errorf("export %s is %s, want expired", id, got.Status)
		}
	}
}

// TestRetentionDeletesOldExports: retention deletes exports 30 days after
// they expired or failed, and a file a deleted export still names goes
// first. Younger ones stay.
func TestRetentionDeletesOldExports(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	leftover := filepath.Join(e.exportDir, tenant.String(), "leftover.ndjson.gz")
	if err := os.MkdirAll(filepath.Dir(leftover), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leftover, []byte("decrypted readings"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, set string
		args      []any
		deleted   bool
	}{
		{"expired 31 days ago", `status = 'expired', expires_at = now() - interval '31 days'`, nil, true},
		{"expired 31 days ago, file left", `status = 'expired', file_path = $2, expires_at = now() - interval '31 days'`,
			[]any{leftover}, true},
		{"expired 29 days ago", `status = 'expired', expires_at = now() - interval '29 days'`, nil, false},
		{"failed 31 days ago", `status = 'failed', error = 'x', created_at = now() - interval '31 days'`, nil, true},
		{"failed a day ago", `status = 'failed', error = 'x', created_at = now() - interval '1 day'`, nil, false},
		{"ready", `status = 'ready', expires_at = now() + interval '1 hour'`, nil, false},
	}
	ids := make([]uuid.UUID, len(cases))
	for i, c := range cases {
		ids[i], _ = e.export(tenant)
		e.asOwner(`UPDATE fleetwire.exports SET `+c.set+` WHERE id = $1`, append([]any{ids[i]}, c.args...)...)
	}
	// The export jobs find nothing to do.
	job := e.enqueue(tenant, store.JobRetention, struct{}{}, "")
	e.runJobs()
	if j := e.job(tenant, job); j.Status != store.JobSucceeded {
		t.Fatalf("retention %s: %s", j.Status, lastError(j))
	}
	for i, c := range cases {
		err := e.db.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
			_, err := tx.GetExport(ctx, ids[i])
			return err
		})
		switch {
		case c.deleted && !errors.Is(err, store.ErrNotFound):
			t.Errorf("%s: not deleted (%v)", c.name, err)
		case !c.deleted && err != nil:
			t.Errorf("%s: %v, want it kept", c.name, err)
		}
	}
	if _, err := os.Stat(leftover); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the file of a deleted export is still there (%v)", err)
	}
}

// TestExportAttemptsDoNotShareAFile: an export does not write into or
// delete a temp file another attempt of the same export is writing.
func TestExportAttemptsDoNotShareAFile(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	dev, _ := e.device(tenant)
	e.readings(tenant, dev, 1, 50, time.Now())
	exp, job := e.export(tenant)
	// Every attempt used this one path before.
	other := filepath.Join(e.exportDir, tenant.String(), exp.String()+".ndjson.gz.tmp")
	if err := os.MkdirAll(filepath.Dir(other), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(other, []byte("another attempt"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.runJobs()
	if j := e.job(tenant, job); j.Status != store.JobSucceeded {
		t.Fatalf("export job %s: %s", j.Status, lastError(j))
	}
	if got, err := os.ReadFile(other); err != nil || string(got) != "another attempt" {
		t.Fatalf("the other attempt's file now holds %q (%v)", got, err)
	}
	if n := e.exportLines(tenant, exp); n != 50 {
		t.Fatalf("export has %d readings, want 50", n)
	}
}

// TestRetentionRemovesStaleTempFiles: temp files no attempt wrote to for an
// hour are deleted; fresh ones and finished exports stay.
func TestRetentionRemovesStaleTempFiles(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	dir := filepath.Join(e.exportDir, tenant.String())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * staleTempAge)
	files := map[string]bool{ // name: kept
		"a.ndjson.gz.123.tmp": false,
		"b.ndjson.gz.456.tmp": true,
		"c.ndjson.gz":         true,
	}
	for name, kept := range files {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if !kept || !strings.HasSuffix(name, ".tmp") {
			if err := os.Chtimes(p, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	job := e.enqueue(tenant, store.JobRetention, struct{}{}, "")
	e.runJobs()
	if j := e.job(tenant, job); j.Status != store.JobSucceeded {
		t.Fatalf("retention %s: %s", j.Status, lastError(j))
	}
	for name, kept := range files {
		_, err := os.Stat(filepath.Join(dir, name))
		if kept && err != nil {
			t.Errorf("%s was deleted", name)
		}
		if !kept && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s is still there (%v)", name, err)
		}
	}
}

// TestExportFinishingDuringADestroyLeavesNoFile: an export that finishes
// while the API's first destroy step is uncommitted waits for it, keeps no
// file, and the destroy then removes the tenant's export directory.
func TestExportFinishingDuringADestroyLeavesNoFile(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	dev, _ := e.device(tenant)
	e.readings(tenant, dev, 1, 20, time.Now())
	exp, expJob := e.export(tenant)

	locked, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	// A failing test must still end the held transaction, or closing the
	// pool waits for its connection forever.
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	destroyed := make(chan error, 1)
	go func() {
		destroyed <- e.db.InTenantTx(context.WithoutCancel(t.Context()), tenant, func(ctx context.Context, tx *store.Tx) error {
			if _, err := tx.LockTenant(ctx); err != nil {
				return err
			}
			if err := tx.SetTenantStatus(ctx, store.TenantDestroying); err != nil {
				return err
			}
			if _, err := tx.RevokeAllDevices(ctx); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	select {
	case <-locked:
	case err := <-destroyed:
		t.Fatalf("first destroy step: %v", err)
	}
	exported := make(chan error, 1)
	go func() {
		_, err := e.w.RunOnce(t.Context())
		exported <- err
	}()
	waitForLockWait(t, exported)
	releaseOnce.Do(func() { close(release) })
	if err := <-destroyed; err != nil {
		t.Fatal(err)
	}
	if err := <-exported; err != nil {
		t.Fatal(err)
	}
	if j := e.job(tenant, expJob); j.Status != store.JobSucceeded || !strings.Contains(string(j.Result), skipTenantGone) {
		t.Fatalf("export job %s, result %s; want succeeded and skipped", j.Status, j.Result)
	}
	if got := e.getExport(tenant, exp); got.FilePath != nil {
		t.Fatalf("export %s records file %s", got.Status, *got.FilePath)
	}

	e.enqueue(tenant, store.JobTenantDestroy, map[string]any{"tenant_id": tenant}, "")
	e.runJobs()
	if st := e.tenantStatus(tenant); st != store.TenantDestroyed {
		t.Fatalf("tenant %s, want destroyed", st)
	}
	if _, err := os.Stat(filepath.Join(e.exportDir, tenant.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the destroyed tenant's export directory is still there (%v)", err)
	}
}

// TestDestroyWaitsForARunningExport: while an export job of the tenant is
// running, the destroy does not finish, so the export's temp file cannot
// outlive it; once no export runs, the directory goes and the destroy ends.
func TestDestroyWaitsForARunningExport(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	_, expJob := e.export(tenant)
	// Another worker is writing this export right now.
	e.asOwner(`UPDATE fleetwire.jobs SET status = 'running', attempts = 1, locked_by = 'elsewhere',
		locked_until = now() + interval '1 hour' WHERE id = $1`, expJob)
	temp := filepath.Join(e.exportDir, tenant.String(), "x.ndjson.gz.1.tmp")
	if err := os.MkdirAll(filepath.Dir(temp), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(temp, []byte("decrypted readings"), 0o600); err != nil {
		t.Fatal(err)
	}
	destroyJob := e.enqueue(tenant, store.JobTenantDestroy, map[string]any{"tenant_id": tenant}, "")
	e.runJobs()
	if j := e.job(tenant, destroyJob); j.Status != store.JobQueued || !strings.Contains(lastError(j), "still running") {
		t.Fatalf("destroy job %s (%s), want queued to wait for the export", j.Status, lastError(j))
	}
	if st := e.tenantStatus(tenant); st != store.TenantDestroying {
		t.Fatalf("tenant %s, want destroying", st)
	}
	e.asOwner(`UPDATE fleetwire.jobs SET status = 'succeeded', finished_at = now(), locked_until = NULL WHERE id = $1`, expJob)
	e.asOwner(`UPDATE fleetwire.jobs SET run_after = now() WHERE id = $1`, destroyJob)
	e.runJobs()
	if st := e.tenantStatus(tenant); st != store.TenantDestroyed {
		t.Fatalf("tenant %s, want destroyed", st)
	}
	if _, err := os.Stat(temp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an export temp file outlived the destroy (%v)", err)
	}
}

// TestStalledExportDoesNotRecreateTheExportDir: an export attempt stalls
// past its lease once it has started. Meanwhile another worker takes the
// job over and gives the export up, because the destroy has started, and
// the destroy runs to the end. When the stalled attempt goes on, the
// destroyed tenant's export directory must not come back.
func TestStalledExportDoesNotRecreateTheExportDir(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	dev, _ := e.device(tenant)
	e.readings(tenant, dev, 1, 10, time.Now())
	exp, expJob := e.export(tenant)
	var destroyJob uuid.UUID
	e.w.exportStarted = func() {
		e.w.exportStarted = nil
		e.inTenant(tenant, func(ctx context.Context, tx *store.Tx) error {
			return tx.SetTenantStatus(ctx, store.TenantDestroying)
		})
		e.asOwner(`UPDATE fleetwire.jobs SET status = 'succeeded', locked_by = 'elsewhere', locked_until = NULL,
			finished_at = now() WHERE id = $1`, expJob)
		e.asOwner(`UPDATE fleetwire.exports SET status = 'failed', error = $2 WHERE id = $1`, exp, skipTenantGone)
		destroyJob = e.enqueue(tenant, store.JobTenantDestroy, map[string]any{"tenant_id": tenant}, "")
		e.runJobs()
	}
	e.runJobs()
	if destroyJob == uuid.Nil {
		t.Fatal("the export attempt never started")
	}
	if j := e.job(tenant, destroyJob); j.Status != store.JobSucceeded {
		t.Fatalf("destroy job %s: %s", j.Status, lastError(j))
	}
	if st := e.tenantStatus(tenant); st != store.TenantDestroyed {
		t.Fatalf("tenant %s, want destroyed", st)
	}
	if _, err := os.Stat(filepath.Join(e.exportDir, tenant.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the stalled attempt made the destroyed tenant's export directory again (%v)", err)
	}
}

// TestExportDoesNotStartForADestroyingTenant: an export job that runs after
// the destroy started fails its export and writes nothing.
func TestExportDoesNotStartForADestroyingTenant(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	exp, job := e.export(tenant)
	e.inTenant(tenant, func(ctx context.Context, tx *store.Tx) error {
		return tx.SetTenantStatus(ctx, store.TenantDestroying)
	})
	e.runJobs()
	if j := e.job(tenant, job); j.Status != store.JobSucceeded {
		t.Fatalf("export job %s: %s", j.Status, lastError(j))
	}
	if got := e.getExport(tenant, exp); got.Status != store.ExportFailed || got.Error == nil || *got.Error != skipTenantGone {
		t.Fatalf("export %s (%v), want failed: %s", got.Status, got.Error, skipTenantGone)
	}
	if _, err := os.Stat(filepath.Join(e.exportDir, tenant.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an export directory was created for a destroying tenant (%v)", err)
	}
}
