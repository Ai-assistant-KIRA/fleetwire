package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store/storetest"
)

// heldTx is a transaction kept open between two steps of a test, to
// interleave it with another one.
type heldTx struct {
	release chan struct{}
	once    sync.Once
	done    chan error
}

// hold runs fn in a transaction and keeps the transaction open until
// commit is called. fn's error fails the test at once.
func hold(t *testing.T, db *store.DB, fn func(ctx context.Context, tx *store.Tx) error) *heldTx {
	t.Helper()
	h := &heldTx{release: make(chan struct{}), done: make(chan error, 1)}
	ready := make(chan error, 1)
	go func() {
		h.done <- db.InTx(context.WithoutCancel(t.Context()), func(ctx context.Context, tx *store.Tx) error {
			err := fn(ctx, tx)
			ready <- err
			if err != nil {
				return err
			}
			<-h.release
			return nil
		})
	}()
	// A test that fails before commit must not leave the goroutine waiting.
	t.Cleanup(func() { h.once.Do(func() { close(h.release) }) })
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *heldTx) commit(t *testing.T) {
	t.Helper()
	h.once.Do(func() { close(h.release) })
	if err := <-h.done; err != nil {
		t.Fatal(err)
	}
}

// async runs fn in its own transaction in the background.
func async(t *testing.T, db *store.DB, fn func(ctx context.Context, tx *store.Tx) error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- db.InTx(t.Context(), fn) }()
	return done
}

// waitForLockWait returns once a connection of this test waits for a row
// lock. It fails the test if done reports first, that is, if the other
// transaction went through without waiting.
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
			t.Fatalf("the transaction finished (err %v) without waiting for the other one", err)
		default:
		}
		var waiting int
		if err := super.QueryRow(t.Context(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE application_name = $1 AND wait_event_type = 'Lock'`,
			t.Name()).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no transaction of this test started waiting for a lock")
}

// certsOf returns the device's certificates and whether each is revoked.
func certsOf(t *testing.T, db *store.DB, tenant, device uuid.UUID) map[string]bool {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), storetest.Start(t).OwnerURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.WithoutCancel(t.Context()))
	rows, err := conn.Query(t.Context(), `
		SELECT serial, revoked_at IS NOT NULL FROM fleetwire.device_certs WHERE tenant_id = $1 AND device_id = $2`,
		tenant, device)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	var serial string
	var revoked bool
	_, err = pgx.ForEachRow(rows, []any{&serial, &revoked}, func() error {
		out[serial] = revoked
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// renewCert is the database side of a renewal, as the API runs it.
func renewCert(ctx context.Context, tx *store.Tx, device uuid.UUID, serial string) (store.DeviceRef, error) {
	ref, err := tx.LockDevice(ctx, device)
	if err != nil {
		return ref, err
	}
	if err := tx.SetTenant(ctx, ref.TenantID); err != nil {
		return ref, err
	}
	if ref.DeviceStatus != store.DeviceActive || ref.TenantStatus != store.TenantActive {
		return ref, nil
	}
	return ref, tx.SetDeviceCert(ctx, store.DeviceCert{Serial: serial, DeviceID: device, NotAfter: time.Now().Add(time.Hour)})
}

// TestRenewalSeesARevokeThatCommitsFirst: a renewal that reads the device
// while a revoke is uncommitted waits for it, and then refuses.
func TestRenewalSeesARevokeThatCommitsFirst(t *testing.T) {
	db := storetest.AppDB(t)
	f := newFixture(t, db)
	revoke := hold(t, db, func(ctx context.Context, tx *store.Tx) error {
		if err := tx.SetTenant(ctx, f.tenant); err != nil {
			return err
		}
		_, err := tx.RevokeDevice(ctx, f.device)
		return err
	})
	var ref store.DeviceRef
	serial := "renewed-" + uuid.NewString()
	done := async(t, db, func(ctx context.Context, tx *store.Tx) error {
		var err error
		ref, err = renewCert(ctx, tx, f.device, serial)
		return err
	})
	waitForLockWait(t, done)
	revoke.commit(t)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if ref.DeviceStatus != store.DeviceRevoked {
		t.Fatalf("the renewal read device status %q, want %q", ref.DeviceStatus, store.DeviceRevoked)
	}
	if _, issued := certsOf(t, db, f.tenant, f.device)[serial]; issued {
		t.Fatal("a certificate was stored for a revoked device")
	}
}

// TestRevokeAfterARenewalRevokesTheNewCert: a revoke that starts while a
// renewal is uncommitted waits, and then revokes the new certificate too.
func TestRevokeAfterARenewalRevokesTheNewCert(t *testing.T) {
	db := storetest.AppDB(t)
	f := newFixture(t, db)
	serial := "renewed-" + uuid.NewString()
	renew := hold(t, db, func(ctx context.Context, tx *store.Tx) error {
		ref, err := renewCert(ctx, tx, f.device, serial)
		if err == nil && ref.DeviceStatus != store.DeviceActive {
			t.Errorf("device status %q before the revoke", ref.DeviceStatus)
		}
		return err
	})
	done := async(t, db, func(ctx context.Context, tx *store.Tx) error {
		if err := tx.SetTenant(ctx, f.tenant); err != nil {
			return err
		}
		_, err := tx.RevokeDevice(ctx, f.device)
		return err
	})
	waitForLockWait(t, done)
	renew.commit(t)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	certs := certsOf(t, db, f.tenant, f.device)
	if _, ok := certs[serial]; !ok {
		t.Fatal("the renewed certificate was not stored")
	}
	for s, revoked := range certs {
		if !revoked {
			t.Errorf("certificate %s of the revoked device is not revoked", s)
		}
	}
}

// destroyFirstStep is the API's synchronous part of a tenant destroy.
func destroyFirstStep(ctx context.Context, tx *store.Tx, tenant uuid.UUID) error {
	if err := tx.SetTenant(ctx, tenant); err != nil {
		return err
	}
	if _, err := tx.LockTenant(ctx); err != nil {
		return err
	}
	if err := tx.SetTenantStatus(ctx, store.TenantDestroying); err != nil {
		return err
	}
	_, err := tx.RevokeAllDevices(ctx)
	return err
}

func newEnrollmentToken(t *testing.T, db *store.DB, tenant uuid.UUID) []byte {
	t.Helper()
	hash := []byte("tok-" + uuid.NewString())
	err := db.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		return tx.InsertEnrollmentToken(ctx, uuid.New(), hash, time.Now().Add(time.Hour), "test")
	})
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

// TestEnrollmentSeesADestroyThatCommitsFirst: an enrollment that reads the
// tenant while a destroy is uncommitted waits for it, and then refuses.
func TestEnrollmentSeesADestroyThatCommitsFirst(t *testing.T) {
	db := storetest.AppDB(t)
	f := newFixture(t, db)
	hash := newEnrollmentToken(t, db, f.tenant)
	destroy := hold(t, db, func(ctx context.Context, tx *store.Tx) error {
		return destroyFirstStep(ctx, tx, f.tenant)
	})
	var outcome string
	done := async(t, db, func(ctx context.Context, tx *store.Tx) error {
		var err error
		_, outcome, err = tx.ConsumeEnrollmentToken(ctx, hash, uuid.New())
		return err
	})
	waitForLockWait(t, done)
	destroy.commit(t)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if outcome != store.EnrollTenantInactive {
		t.Fatalf("enrollment outcome %q during a destroy, want %q", outcome, store.EnrollTenantInactive)
	}
}

// TestDestroyAfterAnEnrollmentRevokesTheNewDevice: a destroy that starts
// between token use and the device insert waits for the enrollment, and
// then revokes the new device and its certificate.
func TestDestroyAfterAnEnrollmentRevokesTheNewDevice(t *testing.T) {
	db := storetest.AppDB(t)
	f := newFixture(t, db)
	hash := newEnrollmentToken(t, db, f.tenant)
	device := uuid.New()
	serial := "enrolled-" + uuid.NewString()
	insert := make(chan struct{})
	enrolled := make(chan error, 1)
	consumed := make(chan string, 1)
	go func() {
		enrolled <- db.InTx(context.WithoutCancel(t.Context()), func(ctx context.Context, tx *store.Tx) error {
			tenant, outcome, err := tx.ConsumeEnrollmentToken(ctx, hash, device)
			consumed <- outcome
			if err != nil || outcome != store.EnrollOK {
				return err
			}
			<-insert
			if err := tx.SetTenant(ctx, tenant); err != nil {
				return err
			}
			if _, err := tx.InsertDevice(ctx, device, "new"); err != nil {
				return err
			}
			return tx.SetDeviceCert(ctx, store.DeviceCert{Serial: serial, DeviceID: device, NotAfter: time.Now().Add(time.Hour)})
		})
	}()
	t.Cleanup(func() {
		select {
		case <-insert:
		default:
			close(insert)
		}
	})
	if o := <-consumed; o != store.EnrollOK {
		t.Fatalf("outcome %q, want ok", o)
	}
	done := async(t, db, func(ctx context.Context, tx *store.Tx) error {
		return destroyFirstStep(ctx, tx, f.tenant)
	})
	waitForLockWait(t, done)
	close(insert)
	if err := <-enrolled; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	err := db.InTenantTx(t.Context(), f.tenant, func(ctx context.Context, tx *store.Tx) error {
		d, err := tx.GetDevice(ctx, device)
		if err == nil && d.Status != store.DeviceRevoked {
			t.Errorf("the device enrolled during the destroy is %q, want revoked", d.Status)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if revoked, ok := certsOf(t, db, f.tenant, device)[serial]; !ok || !revoked {
		t.Fatalf("the new device's certificate: stored %v, revoked %v; want both", ok, revoked)
	}
}
