package jobs

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	tenants   []uuid.UUID
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
	// Every test shares one database, and a worker claims jobs of any
	// tenant: jobs a test leaves unfinished must not run in the next one.
	t.Cleanup(func() {
		if len(e.tenants) == 0 {
			return
		}
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, storetest.Start(t).OwnerURL)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, `UPDATE fleetwire.jobs SET status = 'failed', finished_at = now()
			WHERE tenant_id = ANY ($1) AND status IN ('queued', 'running')`, e.tenants); err != nil {
			t.Error(err)
		}
	})
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
	e.tenants = append(e.tenants, id)
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

// enqueue queues a job of kind for tenant and returns the ID EnqueueJob
// reports.
func (e *env) enqueue(tenant uuid.UUID, kind string, payload any, dedupeKey string) uuid.UUID {
	e.t.Helper()
	var id uuid.UUID
	e.inTenant(tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		id, err = tx.EnqueueJob(ctx, uuid.Must(uuid.NewV7()), kind, payload, dedupeKey, time.Now())
		return err
	})
	return id
}

// asOwner runs sql as the schema owner, for test setups the runtime role
// may not do.
func (e *env) asOwner(sql string, args ...any) {
	e.t.Helper()
	conn, err := pgx.Connect(e.t.Context(), storetest.Start(e.t).OwnerURL)
	if err != nil {
		e.t.Fatal(err)
	}
	defer conn.Close(context.WithoutCancel(e.t.Context()))
	if _, err := conn.Exec(e.t.Context(), sql, args...); err != nil {
		e.t.Fatalf("%s: %v", sql, err)
	}
}

// export creates a pending export of the whole tenant and queues its job,
// the way the API does. It returns the export and job IDs.
func (e *env) export(tenant uuid.UUID) (uuid.UUID, uuid.UUID) {
	e.t.Helper()
	id := uuid.Must(uuid.NewV7())
	e.inTenant(tenant, func(ctx context.Context, tx *store.Tx) error {
		_, err := tx.InsertExport(ctx, id, nil)
		return err
	})
	return id, e.enqueue(tenant, store.JobExport, map[string]any{"export_id": id}, "export:"+id.String())
}

// exportLines checks a ready export's file against its recorded digest and
// returns the number of readings in it.
func (e *env) exportLines(tenant, id uuid.UUID) int {
	e.t.Helper()
	exp := e.getExport(tenant, id)
	if exp.Status != store.ExportReady || exp.FilePath == nil {
		e.t.Fatalf("export %s is %s, want ready with a file", id, exp.Status)
	}
	data, err := os.ReadFile(*exp.FilePath)
	if err != nil {
		e.t.Fatal(err)
	}
	if sum := sha256.Sum256(data); !bytes.Equal(sum[:], exp.SHA256) {
		e.t.Fatalf("export file digest %x, recorded %x", sum, exp.SHA256)
	}
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		e.t.Fatal(err)
	}
	lines := 0
	sc := bufio.NewScanner(zr)
	for sc.Scan() {
		lines++
	}
	if err := sc.Err(); err != nil {
		e.t.Fatal(err)
	}
	return lines
}

func (e *env) getExport(tenant, id uuid.UUID) store.Export {
	e.t.Helper()
	var exp store.Export
	e.inTenant(tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		exp, err = tx.GetExport(ctx, id)
		return err
	})
	return exp
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

// waitForLockWait returns once a connection of this test waits for a row
// lock. It fails the test if done reports first, that is, if the other
// side went through without waiting.
func waitForLockWait(t *testing.T, done <-chan error) {
	t.Helper()
	super, err := pgx.Connect(t.Context(), storetest.Start(t).SuperURL)
	if err != nil {
		t.Fatal(err)
	}
	defer super.Close(context.WithoutCancel(t.Context()))
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("finished (err %v) without waiting for the other transaction", err)
		default:
		}
		var waiting int
		if err := super.QueryRow(t.Context(), `
			SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock'`,
			t.Name()).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("nothing of this test started waiting for a lock")
}
