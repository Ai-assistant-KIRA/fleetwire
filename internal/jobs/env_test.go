package jobs

import (
	"context"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/chunks"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crl"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store/storetest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
)

// env is a worker on a real PostgreSQL with the local keyring and the dev
// PKI. Workers is 0: tests run jobs one at a time with RunOnce.
type env struct {
	t         *testing.T
	db        *store.DB
	kr        *keyring.Local
	keys      *tenantkey.Manager
	w         *Worker
	exportDir string
}

func newEnv(t *testing.T, cfg Config) *env {
	t.Helper()
	pkiDir := t.TempDir()
	if _, err := devpki.Generate(pkiDir, devpki.Options{}); err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(filepath.Join(pkiDir, devpki.DeviceCACert), filepath.Join(pkiDir, devpki.DeviceCAKey))
	if err != nil {
		t.Fatal(err)
	}
	root, err := keyring.LoadRootKey(filepath.Join(pkiDir, devpki.KeyringRootKey))
	if err != nil {
		t.Fatal(err)
	}
	kr, err := keyring.NewLocal(t.TempDir(), root)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, db: storetest.AppDB(t), kr: kr, exportDir: t.TempDir()}
	log := slog.New(slog.DiscardHandler)
	e.keys = tenantkey.New(e.db, kr, log)
	pub := crl.NewPublisher(e.db, ca, filepath.Join(t.TempDir(), "crl.pem"), log)
	cfg.ExportDir = e.exportDir
	cfg.WorkerID = "test-" + uuid.NewString()[:8]
	e.w = NewWorker(cfg, Deps{DB: e.db, Keyring: kr, Keys: e.keys, CRL: pub, Log: log, Registry: prometheus.NewRegistry()})
	return e
}

// tenant creates an active tenant with its KEK and first data key.
func (e *env) tenant() uuid.UUID {
	e.t.Helper()
	id := uuid.Must(uuid.NewV7())
	if err := e.kr.Create(e.t.Context(), id); err != nil {
		e.t.Fatal(err)
	}
	e.inTenant(id, func(ctx context.Context, tx *store.Tx) error {
		if _, err := tx.CreateTenant(ctx, "t-"+id.String()[:8], 30); err != nil {
			return err
		}
		return e.keys.CreateFirstKey(ctx, tx)
	})
	return id
}

// device enrolls an active device with a certificate and returns its ID
// and certificate serial.
func (e *env) device(tenant uuid.UUID) (uuid.UUID, string) {
	e.t.Helper()
	id := uuid.Must(uuid.NewV7())
	n, err := pki.NewSerial()
	if err != nil {
		e.t.Fatal(err)
	}
	serial := pki.SerialString(n)
	e.inTenant(tenant, func(ctx context.Context, tx *store.Tx) error {
		if _, err := tx.InsertDevice(ctx, id, "dev"); err != nil {
			return err
		}
		return tx.SetDeviceCert(ctx, store.DeviceCert{Serial: serial, DeviceID: id, NotAfter: time.Now().Add(time.Hour)})
	})
	return id, serial
}

// readings stores n sealed readings of device, seqs lo to lo+n-1, collected at.
func (e *env) readings(tenant, device uuid.UUID, lo uint64, n int, at time.Time) {
	e.t.Helper()
	keyID, dek, err := e.keys.SealingKey(e.t.Context(), tenant)
	if err != nil {
		e.t.Fatal(err)
	}
	var rs []*fleetwirev1.StoredReading
	for i := range n {
		seq := lo + uint64(i)
		rs = append(rs, &fleetwirev1.StoredReading{Seq: seq, CollectedUnixNano: at.UnixNano(),
			Collector: "test", Metric: "cpu.utilization", Value: float64(seq)})
	}
	c, err := chunks.Seal(dek, tenant, device, keyID, rs)
	if err != nil {
		e.t.Fatal(err)
	}
	err = e.db.IngestBatch(e.t.Context(), device, tenant, func(store.IngestState) (store.IngestWrite, error) {
		return store.IngestWrite{Chunks: []store.Chunk{c}, UpdateProgress: true, AckedThrough: c.SeqHi, BatchAt: time.Now()}, nil
	})
	if err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) inTenant(tenant uuid.UUID, fn func(ctx context.Context, tx *store.Tx) error) {
	e.t.Helper()
	if err := e.db.InTenantTx(e.t.Context(), tenant, fn); err != nil {
		e.t.Fatal(err)
	}
}

// enqueue queues a job of kind for tenant and returns its ID.
func (e *env) enqueue(tenant uuid.UUID, kind string, payload any, dedupeKey string) uuid.UUID {
	e.t.Helper()
	id := uuid.Must(uuid.NewV7())
	e.inTenant(tenant, func(ctx context.Context, tx *store.Tx) error {
		_, err := tx.EnqueueJob(ctx, id, kind, payload, dedupeKey, time.Now())
		return err
	})
	return id
}

// runJobs runs runnable jobs until none is left.
func (e *env) runJobs() {
	e.t.Helper()
	for range 1000 {
		ran, err := e.w.RunOnce(e.t.Context())
		if err != nil {
			e.t.Fatal(err)
		}
		if !ran {
			return
		}
	}
	e.t.Fatal("jobs kept running")
}

func (e *env) job(tenant, id uuid.UUID) store.Job {
	e.t.Helper()
	var j store.Job
	e.inTenant(tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		j, err = tx.GetJob(ctx, id)
		return err
	})
	return j
}

// revoked reports whether serial is on the revocation list.
func (e *env) revoked(serial string) bool {
	e.t.Helper()
	var list []store.RevokedCert
	err := e.db.InTx(e.t.Context(), func(ctx context.Context, tx *store.Tx) error {
		var err error
		list, err = tx.RevokedCerts(ctx)
		return err
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return slices.ContainsFunc(list, func(r store.RevokedCert) bool { return r.Serial == serial })
}

func (e *env) tenantStatus(tenant uuid.UUID) string {
	e.t.Helper()
	var st string
	e.inTenant(tenant, func(ctx context.Context, tx *store.Tx) error {
		t, err := tx.GetTenant(ctx)
		st = t.Status
		return err
	})
	return st
}

// lastError is a job's last error, or "".
func lastError(j store.Job) string {
	if j.LastError == nil {
		return ""
	}
	return *j.LastError
}
