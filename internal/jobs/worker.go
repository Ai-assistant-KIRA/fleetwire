// Package jobs runs fleetwire's background jobs inside fleetwire-api:
// retention, exports, device purges and tenant destroys.
//
// Jobs are rows in PostgreSQL, claimed with FOR UPDATE SKIP LOCKED and a
// lease, so any number of API replicas can run workers. A job whose worker
// dies is picked up again when its lease runs out, so every handler is
// idempotent: it checks what is already done and continues from there.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/crl"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
)

// Config tunes the worker.
type Config struct {
	Workers   int
	ExportDir string
	WorkerID  string
	// Lease is how long a claim lasts without renewal (default 2 min).
	Lease time.Duration
	// Poll is the wait between claims when the queue is empty (default 2 s).
	Poll time.Duration
	// ExportTTL is how long a finished export stays downloadable (24 h).
	ExportTTL time.Duration
	// ScheduleEvery is how often the retention schedule is checked (5 min).
	ScheduleEvery time.Duration
	// BatchSize is the number of rows deleted per transaction (5,000).
	BatchSize int
}

// Deps are the worker's collaborators.
type Deps struct {
	DB       *store.DB
	Keyring  keyring.Keyring
	Keys     *tenantkey.Manager
	CRL      *crl.Publisher
	Log      *slog.Logger
	Registry prometheus.Registerer
}

// Worker claims and runs jobs.
type Worker struct {
	cfg     Config
	d       Deps
	total   *prometheus.CounterVec
	running prometheus.Gauge
	now     func() time.Time
	// handler runs one attempt of a job; tests replace it.
	handler func(ctx context.Context, j store.Job) (any, error)
	// exportStarted, when set, runs once an export attempt has checked the
	// tenant and marked the export running; tests stall attempts there.
	exportStarted func()
}

// NewWorker returns a worker.
func NewWorker(cfg Config, d Deps) *Worker {
	if cfg.Lease <= 0 {
		cfg.Lease = 2 * time.Minute
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 2 * time.Second
	}
	if cfg.ExportTTL <= 0 {
		cfg.ExportTTL = 24 * time.Hour
	}
	if cfg.ScheduleEvery <= 0 {
		cfg.ScheduleEvery = 5 * time.Minute
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}
	w := &Worker{
		cfg: cfg, d: d, now: time.Now,
		total: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleetwire_jobs_total", Help: "Finished job attempts by kind and result.",
		}, []string{"kind", "result"}),
		running: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fleetwire_jobs_running", Help: "Jobs running in this process.",
		}),
	}
	w.handler = w.handle
	d.Registry.MustRegister(w.total, w.running)
	return w
}

var allKinds = []string{store.JobRetention, store.JobExport, store.JobDevicePurge, store.JobTenantDestroy}

// Run schedules retention and runs jobs until ctx ends.
func (w *Worker) Run(ctx context.Context) error {
	if w.cfg.Workers == 0 {
		<-ctx.Done()
		return nil
	}
	var wg sync.WaitGroup
	wg.Go(func() { w.schedule(ctx) })
	for range w.cfg.Workers {
		wg.Go(func() { w.loop(ctx) })
	}
	wg.Wait()
	return nil
}

// schedule enqueues one retention job per active tenant per hour. Every
// replica tries; the dedupe key makes it happen once.
func (w *Worker) schedule(ctx context.Context) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := w.ScheduleRetention(ctx); err != nil && ctx.Err() == nil {
			w.d.Log.Error("retention scheduling failed", "err", err)
		}
		t.Reset(w.cfg.ScheduleEvery)
	}
}

// ScheduleRetention enqueues this hour's retention jobs, and deletes the
// fleet-wide rows nobody needs any more: expired certificates, jobs that
// finished more than 30 days ago, and enrollment tokens 7 days past their
// expiry.
func (w *Worker) ScheduleRetention(ctx context.Context) error {
	var tenants []store.ActiveTenant
	err := w.d.DB.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		var err error
		if tenants, err = tx.ListActiveTenants(ctx); err != nil {
			return err
		}
		_, err = tx.DeleteExpiredCerts(ctx)
		return err
	})
	if err != nil {
		return err
	}
	for _, prune := range []func(*store.Tx, context.Context, int) (int64, error){
		(*store.Tx).PruneFinishedJobs, (*store.Tx).PruneEnrollmentTokens,
	} {
		if err := w.pruneInBatches(ctx, prune); err != nil {
			return err
		}
	}
	hour := w.now().UTC().Format("2006-01-02T15")
	for _, t := range tenants {
		err := w.d.DB.InTenantTx(ctx, t.ID, func(ctx context.Context, tx *store.Tx) error {
			_, err := tx.EnqueueJob(ctx, uuid.Must(uuid.NewV7()), store.JobRetention, struct{}{},
				"retention:"+t.ID.String()+":"+hour, w.now())
			return err
		})
		if err != nil {
			return fmt.Errorf("enqueue retention for %s: %w", t.ID, err)
		}
	}
	return nil
}

func (w *Worker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		ran, err := w.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			w.d.Log.Error("job claim failed", "err", err)
		}
		if !ran {
			select {
			case <-ctx.Done():
			case <-time.After(w.cfg.Poll):
			}
		}
	}
}

// RunOnce claims and runs at most one job. It reports whether one ran.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	var claimed []store.Job
	err := w.d.DB.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		var err error
		claimed, err = tx.ClaimJobs(ctx, allKinds, 1, w.cfg.Lease, w.cfg.WorkerID)
		return err
	})
	if err != nil || len(claimed) == 0 {
		return false, err
	}
	w.execute(ctx, claimed[0])
	return true, nil
}

// recordTimeout bounds each database call that records how an attempt
// ended. Those calls are detached from the worker's context, so a shutdown
// does not lose the outcome of a job that just finished, but they must not
// hold the shutdown up for long either.
const recordTimeout = 15 * time.Second

func (w *Worker) execute(ctx context.Context, j store.Job) {
	w.running.Inc()
	defer w.running.Dec()
	log := w.d.Log.With("job_id", j.ID, "kind", j.Kind, "tenant_id", j.TenantID, "attempt", j.Attempts)
	jctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lost atomic.Bool
	var lease sync.WaitGroup
	lease.Go(func() { w.keepLease(jctx, cancel, j, &lost, log) })

	start := w.now()
	result, err := w.run(jctx, j, log)
	// Stop the lease extender before recording the outcome: it must not
	// race the finish, nor outlive the job and, at shutdown, the pool.
	cancel()
	lease.Wait()

	switch {
	case lost.Load():
		// Another worker holds the job now and records its outcome.
		w.total.WithLabelValues(j.Kind, "error").Inc()
		log.Warn("job lease lost to another worker; this attempt is abandoned", "err", err)
		return
	case err != nil && ctx.Err() != nil:
		// The worker is stopping, which is not the job's fault: hand it
		// back without using up an attempt.
		rerr := w.record(ctx, j.TenantID, func(ctx context.Context, tx *store.Tx) error {
			return tx.ReleaseJob(ctx, j.ID, w.cfg.WorkerID)
		})
		if rerr != nil {
			log.Error("job interrupted by shutdown; could not release it, the lease will expire", "err", err, "record_err", rerr)
			return
		}
		log.Info("job interrupted by shutdown; released", "err", err)
		return
	case err == nil:
		err = w.record(ctx, j.TenantID, func(ctx context.Context, tx *store.Tx) error {
			return tx.FinishJob(ctx, j.ID, w.cfg.WorkerID, result)
		})
		if err == nil {
			w.total.WithLabelValues(j.Kind, "success").Inc()
			log.Info("job done", "duration", w.now().Sub(start), "result", result)
			return
		}
		err = fmt.Errorf("record the result: %w", err)
	}
	w.total.WithLabelValues(j.Kind, "error").Inc()
	retry := w.now().Add(min(time.Duration(1<<min(j.Attempts, 10))*10*time.Second, time.Hour))
	msg := err.Error()
	var failed bool
	rerr := w.record(ctx, j.TenantID, func(ctx context.Context, tx *store.Tx) error {
		var err error
		if failed, err = tx.RetryJob(ctx, j.ID, w.cfg.WorkerID, msg, retry); err != nil || !failed {
			return err
		}
		return failJob(ctx, tx, j)
	})
	switch {
	case rerr != nil:
		log.Error("job failed; could not record the failure, the lease will expire", "err", err, "record_err", rerr)
	case failed:
		log.Error("job failed for good; no attempts left", "err", err)
	default:
		log.Error("job failed; will retry", "err", err, "retry_at", retry)
	}
}

// keepLease renews j's lease until ctx ends. If another worker took the job
// over (this one stalled past the lease), it sets lost and calls stop.
func (w *Worker) keepLease(ctx context.Context, stop context.CancelFunc, j store.Job, lost *atomic.Bool, log *slog.Logger) {
	t := time.NewTicker(w.cfg.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		err := w.d.DB.InTenantTx(ctx, j.TenantID, func(ctx context.Context, tx *store.Tx) error {
			return tx.ExtendJobLease(ctx, j.ID, w.cfg.WorkerID, w.cfg.Lease)
		})
		if errors.Is(err, store.ErrNotFound) {
			log.Warn("job lease lost; stopping")
			lost.Store(true)
			stop()
			return
		}
	}
}

// run calls the job's handler. A panic fails the attempt the way an error
// does, so one bad job cannot take the API process down with it.
func (w *Worker) run(ctx context.Context, j store.Job, log *slog.Logger) (result any, err error) {
	defer func() {
		if p := recover(); p != nil {
			log.Error("job handler panicked", "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("job handler panicked: %v", p)
		}
	}()
	return w.handler(ctx, j)
}

// record runs fn in a tenant transaction detached from ctx and bounded by
// recordTimeout.
func (w *Worker) record(ctx context.Context, tenant uuid.UUID, fn func(ctx context.Context, tx *store.Tx) error) error {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
	defer cancel()
	return w.d.DB.InTenantTx(rctx, tenant, fn)
}

// exportFailed is what a tenant sees on an export whose job failed for good.
// The cause is in the job's last error and the log.
const exportFailed = "the export failed; create a new one"

// failJob runs, in the transaction that marks j failed for good, what has
// to fail with it: an export job's export, which would otherwise stay
// running forever.
func failJob(ctx context.Context, tx *store.Tx, j store.Job) error {
	if j.Kind != store.JobExport {
		return nil
	}
	// With a malformed payload there is no export to fail; the payload
	// error is the job's last error.
	if id, ok := exportID(j); ok {
		return tx.FailExport(ctx, id, exportFailed)
	}
	return nil
}

// exportID reads the export an export job works on.
func exportID(j store.Job) (uuid.UUID, bool) {
	var p struct {
		ExportID uuid.UUID `json:"export_id"`
	}
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return uuid.Nil, false
	}
	return p.ExportID, true
}

func (w *Worker) handle(ctx context.Context, j store.Job) (any, error) {
	switch j.Kind {
	case store.JobRetention:
		return w.retention(ctx, j.TenantID)
	case store.JobExport:
		var p struct {
			ExportID uuid.UUID `json:"export_id"`
		}
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return nil, fmt.Errorf("export payload: %w", err)
		}
		return w.export(ctx, j.TenantID, p.ExportID)
	case store.JobDevicePurge:
		var p struct {
			DeviceID uuid.UUID `json:"device_id"`
		}
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return nil, fmt.Errorf("purge payload: %w", err)
		}
		return w.purgeDevice(ctx, j.TenantID, p.DeviceID)
	case store.JobTenantDestroy:
		return w.destroyTenant(ctx, j.TenantID)
	}
	return nil, fmt.Errorf("unknown job kind %q", j.Kind)
}

// pruneInBatches runs a fleet-wide prune, BatchSize rows per transaction,
// until a batch comes back short.
func (w *Worker) pruneInBatches(ctx context.Context, prune func(*store.Tx, context.Context, int) (int64, error)) error {
	for {
		var n int64
		err := w.d.DB.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
			var err error
			n, err = prune(tx, ctx, w.cfg.BatchSize)
			return err
		})
		if err != nil {
			return err
		}
		if n < int64(w.cfg.BatchSize) {
			return nil
		}
	}
}

// deleteInBatches calls del until it deletes nothing, one transaction per
// batch, so a large delete never holds locks for long.
func (w *Worker) deleteInBatches(ctx context.Context, tenant uuid.UUID, del func(ctx context.Context, tx *store.Tx) (int64, error)) (int64, error) {
	var total int64
	for {
		var n int64
		err := w.d.DB.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
			var err error
			n, err = del(ctx, tx)
			return err
		})
		if err != nil {
			return total, err
		}
		total += n
		if n == 0 {
			return total, nil
		}
	}
}
