package tenantkey

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store/storetest"
)

// probe is a keyring that counts calls, can hold Unwrap until released,
// and records a call made while the tenant's row is locked.
type probe struct {
	keyring.Keyring
	t       *testing.T
	owner   *pgxpool.Pool
	wraps   atomic.Int64
	unwraps atomic.Int64
	// gate, when set, holds every Unwrap until it is closed or the call's
	// context ends.
	gate chan struct{}
	// underLock counts keyring calls made while a transaction held the
	// tenant row FOR UPDATE or FOR SHARE.
	underLock atomic.Int64
}

func (p *probe) Wrap(ctx context.Context, tenant uuid.UUID, dek []byte) ([]byte, string, error) {
	p.wraps.Add(1)
	p.checkLock(ctx, tenant)
	return p.Keyring.Wrap(ctx, tenant, dek)
}

func (p *probe) Unwrap(ctx context.Context, tenant uuid.UUID, kekID string, wrapped []byte) ([]byte, error) {
	p.unwraps.Add(1)
	p.checkLock(ctx, tenant)
	if p.gate != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.Keyring.Unwrap(ctx, tenant, kekID, wrapped)
}

// checkLock tries to lock the tenant row without waiting. FOR NO KEY UPDATE
// conflicts with FOR UPDATE and FOR SHARE but not with the key-share locks
// of foreign key checks.
func (p *probe) checkLock(ctx context.Context, tenant uuid.UUID) {
	tx, err := p.owner.Begin(ctx)
	if err != nil {
		p.t.Error(err)
		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = tx.Exec(ctx, `SELECT 1 FROM fleetwire.tenants WHERE id = $1 FOR NO KEY UPDATE NOWAIT`, tenant)
	var pgErr *pgconn.PgError
	switch {
	case errors.As(err, &pgErr) && pgErr.Code == "55P03":
		p.underLock.Add(1)
	case err != nil:
		p.t.Error(err)
	}
}

type env struct {
	t  *testing.T
	db *store.DB
	kr *probe
}

func newEnv(t *testing.T) *env {
	t.Helper()
	root := make([]byte, keyring.RootKeySize)
	if _, err := rand.Read(root); err != nil {
		t.Fatal(err)
	}
	local, err := keyring.NewLocal(t.TempDir(), root)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := pgxpool.New(t.Context(), storetest.Start(t).OwnerURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	return &env{t: t, db: storetest.AppDB(t), kr: &probe{Keyring: local, t: t, owner: owner}}
}

func (e *env) manager() *Manager { return New(e.db, e.kr, slog.New(slog.DiscardHandler)) }

// tenant creates an active tenant with its first key, made by m.
func (e *env) tenant(m *Manager) uuid.UUID {
	e.t.Helper()
	id := uuid.Must(uuid.NewV7())
	if err := e.kr.Create(e.t.Context(), id); err != nil {
		e.t.Fatal(err)
	}
	err := e.db.InTenantTx(e.t.Context(), id, func(ctx context.Context, tx *store.Tx) error {
		if _, err := tx.CreateTenant(ctx, "t-"+id.String()[:8], 30); err != nil {
			return err
		}
		return m.CreateFirstKey(ctx, tx)
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

// destroy runs the database and keyring steps of a tenant destroy.
func (e *env) destroy(tenant uuid.UUID) {
	e.t.Helper()
	err := e.db.InTenantTx(e.t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		if err := tx.SetTenantStatus(ctx, store.TenantDestroying); err != nil {
			return err
		}
		return tx.DestroyTenantKeys(ctx)
	})
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.kr.Destroy(e.t.Context(), tenant); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) sealingKey(m *Manager, tenant uuid.UUID) uuid.UUID {
	e.t.Helper()
	id, dek, err := m.SealingKey(e.t.Context(), tenant)
	if err != nil {
		e.t.Fatal(err)
	}
	if len(dek) == 0 {
		e.t.Fatal("empty data key")
	}
	return id
}

// TestForgetDropsRetiredKeys: after a rotation, the retired key is still
// cached for opening old chunks; Forget must drop it with the active one.
func TestForgetDropsRetiredKeys(t *testing.T) {
	e := newEnv(t)
	m := e.manager()
	tenant := e.tenant(m)
	old := e.sealingKey(m, tenant)
	m.NoteSeals(tenant, old, MaxSeals)
	current := e.sealingKey(m, tenant)
	if current == old {
		t.Fatal("the key did not rotate after MaxSeals seals")
	}
	for _, k := range []uuid.UUID{old, current} {
		if _, err := m.Key(t.Context(), tenant, k); err != nil {
			t.Fatalf("key %s before the destroy: %v", k, err)
		}
	}
	e.destroy(tenant)
	m.Forget(tenant)
	for _, k := range []uuid.UUID{old, current} {
		if _, err := m.Key(t.Context(), tenant, k); !errors.Is(err, ErrDestroyed) {
			t.Errorf("key %s after Forget: %v, want ErrDestroyed", k, err)
		}
	}
	if _, _, err := m.SealingKey(t.Context(), tenant); !errors.Is(err, ErrDestroyed) {
		t.Errorf("sealing key after Forget: %v, want ErrDestroyed", err)
	}
	if n := m.pendingFor(tenant, old); n != 0 {
		t.Errorf("%d seals of the destroyed tenant still pending", n)
	}
}

// TestKeyIsNotHandedToAnotherTenant: a cached key of tenant A is not
// returned for tenant B, even when asked for by its ID.
func TestKeyIsNotHandedToAnotherTenant(t *testing.T) {
	e := newEnv(t)
	m := e.manager()
	a, b := e.tenant(m), e.tenant(m)
	keyA := e.sealingKey(m, a)
	if _, err := m.Key(t.Context(), b, keyA); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("tenant B asking for tenant A's key: %v, want not found", err)
	}
}

// TestReplicasRotatingAtOnceStoreOneKey: two managers (two ingest
// replicas) that both decide to rotate end up with the same new key.
func TestReplicasRotatingAtOnceStoreOneKey(t *testing.T) {
	e := newEnv(t)
	tenant := e.tenant(e.manager())
	m1, m2 := e.manager(), e.manager()
	old := e.sealingKey(m1, tenant)
	if e.sealingKey(m2, tenant) != old {
		t.Fatal("the replicas started with different keys")
	}
	m1.NoteSeals(tenant, old, MaxSeals)
	m2.NoteSeals(tenant, old, MaxSeals)
	var k1, k2 uuid.UUID
	var wg sync.WaitGroup
	wg.Go(func() { k1 = e.sealingKey(m1, tenant) })
	wg.Go(func() { k2 = e.sealingKey(m2, tenant) })
	wg.Wait()
	if k1 == old || k1 != k2 {
		t.Fatalf("after rotating at once: %s and %s (old %s), want one new key", k1, k2, old)
	}
	var active int
	err := e.db.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		k, err := tx.ActiveTenantKey(ctx)
		if err == nil && k.KeyID == k1 {
			active = 1
		}
		return err
	})
	if err != nil || active != 1 {
		t.Fatalf("the active key in the database is not %s (%v)", k1, err)
	}
}

// TestFlushedSealsCountTowardRotation: seals written by Flush still count
// on the cached key, so it rotates at MaxSeals.
func TestFlushedSealsCountTowardRotation(t *testing.T) {
	e := newEnv(t)
	m := e.manager()
	tenant := e.tenant(m)
	k := e.sealingKey(m, tenant)
	m.NoteSeals(tenant, k, MaxSeals-1)
	if err := m.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if e.sealingKey(m, tenant) != k {
		t.Fatal("rotated one seal early")
	}
	m.NoteSeals(tenant, k, 1)
	if e.sealingKey(m, tenant) == k {
		t.Fatal("did not rotate at MaxSeals")
	}
}
