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
	"sync"
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

// ScheduleRetention enqueues this hour's retention jobs.
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

func (w *Worker) execute(ctx context.Context, j store.Job) {
	w.running.Inc()
	defer w.running.Dec()
	log := w.d.Log.With("job_id", j.ID, "kind", j.Kind, "tenant_id", j.TenantID, "attempt", j.Attempts)
	jctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Keep the lease while the job runs; if another worker took it over
	// (this one stalled past the lease), stop.
	go func() {
		t := time.NewTicker(w.cfg.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-jctx.Done():
				return
			case <-t.C:
				err := w.d.DB.InTenantTx(jctx, j.TenantID, func(ctx context.Context, tx *store.Tx) error {
					return tx.ExtendJobLease(ctx, j.ID, w.cfg.WorkerID, w.cfg.Lease)
				})
				if errors.Is(err, store.ErrNotFound) {
					log.Warn("job lease lost; stopping")
					cancel()
					return
				}
			}
		}
	}()

	start := w.now()
	result, err := w.handle(jctx, j)
	if err == nil {
		err = w.d.DB.InTenantTx(ctx, j.TenantID, func(ctx context.Context, tx *store.Tx) error {
			return tx.FinishJob(ctx, j.ID, w.cfg.WorkerID, result)
		})
		if err == nil {
			w.total.WithLabelValues(j.Kind, "success").Inc()
			log.Info("job done", "duration", w.now().Sub(start), "result", result)
			return
		}
	}
	w.total.WithLabelValues(j.Kind, "error").Inc()
	retry := w.now().Add(min(time.Duration(1<<min(j.Attempts, 10))*10*time.Second, time.Hour))
	log.Error("job failed; will retry", "err", err, "retry_at", retry)
	rerr := w.d.DB.InTenantTx(context.WithoutCancel(ctx), j.TenantID, func(ctx context.Context, tx *store.Tx) error {
		return tx.RetryJob(ctx, j.ID, w.cfg.WorkerID, err.Error(), retry)
	})
	if rerr != nil {
		log.Error("could not record job failure; the lease will expire", "err", rerr)
	}
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
