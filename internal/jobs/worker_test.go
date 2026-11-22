package jobs

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

// TestPanicFailsOnlyTheAttempt: a handler that panics fails its attempt
// like an error, and the worker goes on.
func TestPanicFailsOnlyTheAttempt(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	job := e.enqueue(tenant, store.JobRetention, struct{}{}, "")
	e.w.handler = func(context.Context, store.Job) (any, error) { panic("bad job") }
	ran, err := e.w.RunOnce(t.Context())
	if err != nil || !ran {
		t.Fatalf("RunOnce = %v, %v", ran, err)
	}
	j := e.job(tenant, job)
	if j.Status != store.JobQueued || j.Attempts != 1 || !strings.Contains(lastError(j), "panicked: bad job") {
		t.Fatalf("job after a panic: status %s, attempts %d, error %q", j.Status, j.Attempts, lastError(j))
	}
}

// TestShutdownReleasesTheJob: a job cut short by shutdown goes back to the
// queue at once, without using up an attempt or recording an error.
func TestShutdownReleasesTheJob(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	job := e.enqueue(tenant, store.JobRetention, struct{}{}, "")
	ctx, stop := context.WithCancel(t.Context())
	e.w.handler = func(ctx context.Context, _ store.Job) (any, error) {
		stop()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if ran, err := e.w.RunOnce(ctx); err != nil || !ran {
		t.Fatalf("RunOnce = %v, %v", ran, err)
	}
	j := e.job(tenant, job)
	if j.Status != store.JobQueued || j.Attempts != 0 || j.LastError != nil || j.LockedBy != nil {
		t.Fatalf("job after shutdown: status %s, attempts %d, error %q, locked by %v",
			j.Status, j.Attempts, lastError(j), j.LockedBy)
	}
	if time.Until(j.RunAfter) > time.Second {
		t.Fatalf("released job runs after %s, want at once", j.RunAfter)
	}
}

// TestResultIsRecordedDuringShutdown: a job that finished just as the
// worker was told to stop is recorded as succeeded, not failed.
func TestResultIsRecordedDuringShutdown(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	job := e.enqueue(tenant, store.JobRetention, struct{}{}, "")
	ctx, stop := context.WithCancel(t.Context())
	e.w.handler = func(context.Context, store.Job) (any, error) {
		stop()
		return map[string]int{"done": 1}, nil
	}
	if ran, err := e.w.RunOnce(ctx); err != nil || !ran {
		t.Fatalf("RunOnce = %v, %v", ran, err)
	}
	if j := e.job(tenant, job); j.Status != store.JobSucceeded {
		t.Fatalf("job %s (%s), want succeeded", j.Status, lastError(j))
	}
}

// TestLeaseIsKeptWhileTheJobRuns: a job that runs for several lease periods
// keeps its lease and is recorded as succeeded.
func TestLeaseIsKeptWhileTheJobRuns(t *testing.T) {
	e := newEnv(t, Config{Lease: 300 * time.Millisecond})
	tenant := e.tenant()
	job := e.enqueue(tenant, store.JobRetention, struct{}{}, "")
	e.w.handler = func(ctx context.Context, _ store.Job) (any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
			return struct{}{}, nil
		}
	}
	// A second worker polls the whole time: it must never get the job.
	other := newEnv(t, Config{})
	other.w.handler = func(_ context.Context, j store.Job) (any, error) {
		if j.ID == job {
			t.Error("a second worker claimed a job whose lease was being renewed")
		}
		return nil, nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if ran, err := e.w.RunOnce(t.Context()); err != nil || !ran {
			t.Errorf("RunOnce = %v, %v", ran, err)
		}
	}()
	for {
		select {
		case <-done:
			if j := e.job(tenant, job); j.Status != store.JobSucceeded || j.LockedUntil != nil {
				t.Fatalf("job %s, locked until %v; want succeeded and unlocked", j.Status, j.LockedUntil)
			}
			return
		case <-time.After(50 * time.Millisecond):
			if _, err := other.w.RunOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestJobThatKeepsKillingItsWorkerFails: an export job whose worker died
// during every attempt is failed, with its export, once its attempts are
// used up, instead of being claimed again.
func TestJobThatKeepsKillingItsWorkerFails(t *testing.T) {
	e := newEnv(t, Config{})
	tenant := e.tenant()
	exp, job := e.export(tenant)
	// What a dead worker leaves behind: the last attempt claimed, the lease
	// run out.
	e.asOwner(`UPDATE fleetwire.jobs SET status = 'running', attempts = max_attempts, locked_by = 'dead',
		locked_until = now() - interval '1 minute' WHERE id = $1`, job)
	e.asOwner(`UPDATE fleetwire.exports SET status = 'running' WHERE id = $1`, exp)
	e.w.handler = func(_ context.Context, j store.Job) (any, error) {
		if j.ID == job {
			t.Error("a job with no attempts left was claimed")
		}
		return nil, nil
	}
	e.runJobs()
	if j := e.job(tenant, job); j.Status != store.JobFailed || j.FinishedAt == nil {
		t.Fatalf("job %s, finished %v; want failed", j.Status, j.FinishedAt)
	}
	if got := e.getExport(tenant, exp); got.Status != store.ExportFailed {
		t.Fatalf("export %s, want failed", got.Status)
	}
}
