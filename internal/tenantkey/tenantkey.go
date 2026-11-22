// Package tenantkey manages tenant data keys (DEKs): creating them, picking
// the one that seals new chunks, rotating it, and caching unwrapped keys for
// a short time so that every chunk does not cost a keyring call.
//
// Key rows are always committed in their own transaction before a chunk
// uses them, so a rolled-back ingest transaction can never leave a cached
// key that the database does not know.
//
// The keyring may be remote (ingest calls the API's keyring over HTTPS), so
// no keyring call is made while a transaction is open: a slow keyring would
// otherwise hold a pooled connection, and during a rotation the tenant row
// lock, for up to the keyring client's timeout.
package tenantkey

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/envelope"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/lru"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

// Rotation limits (ADR-0007). 2^24 random-nonce seals per key keeps the
// chance of a GCM nonce repeat far below the NIST bound.
const (
	MaxSeals = 1 << 24
	MaxAge   = 90 * 24 * time.Hour

	cacheTTL  = 5 * time.Minute
	cacheSize = 10_000
	activeTTL = time.Minute
	// refreshTimeout bounds a shared reload of a sealing key. The reload
	// runs detached from the caller that started it, so one caller giving
	// up does not fail the others waiting for it.
	refreshTimeout = 30 * time.Second
)

// ErrDestroyed means the tenant's keys were destroyed: its data is gone.
var ErrDestroyed = errors.New("tenantkey: tenant keys are destroyed")

type active struct {
	keyID       uuid.UUID
	dek         []byte
	createdAt   time.Time
	sealsAtLoad int64
	// flushed counts this process's seals written to the database since the
	// key was loaded. Flush adds to it in place, because adding the entry
	// again would restart its TTL: a tenant with steady traffic would never
	// reload its key and never see a rotation by another replica. A flush
	// that races a reload can be counted twice, which only makes the
	// rotation come earlier.
	flushed atomic.Int64
}

type sealKey struct {
	tenant, key uuid.UUID
}

// Manager is safe for concurrent use.
type Manager struct {
	db  *store.DB
	kr  keyring.Keyring
	log *slog.Logger
	// deks is keyed by tenant and key, so that a key is only ever handed out
	// for its own tenant and Forget can drop all of a tenant's keys.
	deks   *lru.Cache[sealKey, []byte]
	active *lru.Cache[uuid.UUID, *active]
	// reloads makes the ingest workers of one tenant, which all miss the
	// active key when it expires, share one reload.
	reloads singleflight.Group

	mu      sync.Mutex
	pending map[sealKey]int64
	now     func() time.Time
}

// New returns a manager.
func New(db *store.DB, kr keyring.Keyring, log *slog.Logger) *Manager {
	return &Manager{
		db:      db,
		kr:      kr,
		log:     log,
		deks:    lru.New[sealKey, []byte](cacheSize, cacheTTL),
		active:  lru.New[uuid.UUID, *active](cacheSize, activeTTL),
		pending: map[sealKey]int64{},
		now:     time.Now,
	}
}

// CreateFirstKey creates the tenant's data key inside the transaction that
// creates the tenant. The tenant's KEK must already exist in the keyring.
// The tenant row is new and uncommitted, so no other transaction waits for
// it during the keyring call.
func (m *Manager) CreateFirstKey(ctx context.Context, tx *store.Tx) error {
	k, err := m.newKey(ctx, tx.Tenant())
	if err != nil {
		return err
	}
	return tx.InsertTenantKey(ctx, k.id, k.kekID, k.wrapped)
}

type newKey struct {
	id      uuid.UUID
	dek     []byte
	wrapped []byte
	kekID   string
}

// newKey makes a data key and wraps it under the tenant's KEK.
func (m *Manager) newKey(ctx context.Context, tenant uuid.UUID) (newKey, error) {
	dek, err := envelope.NewKey()
	if err != nil {
		return newKey{}, err
	}
	wrapped, kekID, err := m.kr.Wrap(ctx, tenant, dek)
	if errors.Is(err, keyring.ErrNotFound) {
		return newKey{}, ErrDestroyed
	}
	if err != nil {
		return newKey{}, fmt.Errorf("wrap data key: %w", err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return newKey{}, fmt.Errorf("key id: %w", err)
	}
	return newKey{id: id, dek: dek, wrapped: wrapped, kekID: kekID}, nil
}

// SealingKey returns the key that seals new chunks of tenant, rotating it
// when it is too old or has sealed too many chunks.
func (m *Manager) SealingKey(ctx context.Context, tenant uuid.UUID) (uuid.UUID, []byte, error) {
	if a, ok := m.active.Get(tenant); ok && !m.needsRotation(tenant, a) {
		return a.keyID, a.dek, nil
	}
	ch := m.reloads.DoChan(tenant.String(), func() (any, error) {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), refreshTimeout)
		defer cancel()
		return m.reload(rctx, tenant)
	})
	select {
	case <-ctx.Done():
		return uuid.Nil, nil, fmt.Errorf("sealing key: %w", ctx.Err())
	case r := <-ch:
		if r.Err != nil {
			return uuid.Nil, nil, fmt.Errorf("sealing key: %w", r.Err)
		}
		a, ok := r.Val.(*active)
		if !ok {
			return uuid.Nil, nil, errors.New("sealing key: reload returned no key")
		}
		return a.keyID, a.dek, nil
	}
}

// reload reads the tenant's active key into the cache, or rotates it.
func (m *Manager) reload(ctx context.Context, tenant uuid.UUID) (*active, error) {
	var k store.TenantKey
	found := false
	err := m.db.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		t, err := tx.GetTenant(ctx)
		if err != nil {
			return err
		}
		if t.Status != store.TenantActive {
			return ErrDestroyed
		}
		k, err = tx.ActiveTenantKey(ctx)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		found = err == nil
		return err
	})
	if err != nil {
		return nil, err
	}
	if found {
		if a := loaded(k); !m.needsRotation(tenant, a) {
			return m.use(ctx, tenant, a, k)
		}
	}
	return m.rotate(ctx, tenant)
}

// rotate makes a new sealing key. The key is wrapped before any lock is
// taken; the transaction that stores it locks the tenant row and reads the
// active key again, so replicas rotating at once store one key between
// them and the others use it.
func (m *Manager) rotate(ctx context.Context, tenant uuid.UUID) (*active, error) {
	nk, err := m.newKey(ctx, tenant)
	if err != nil {
		return nil, err
	}
	var theirs *store.TenantKey
	err = m.db.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		t, err := tx.LockTenant(ctx)
		if err != nil {
			return err
		}
		if t.Status != store.TenantActive {
			return ErrDestroyed
		}
		k, err := tx.ActiveTenantKey(ctx)
		switch {
		case errors.Is(err, store.ErrNotFound):
		case err != nil:
			return err
		case !m.needsRotation(tenant, loaded(k)):
			// Another replica rotated first.
			theirs = &k
			return nil
		default:
			if err := tx.RetireTenantKey(ctx, k.KeyID); err != nil {
				return err
			}
			m.log.Info("rotating tenant data key", "tenant_id", tenant, "old_key_id", k.KeyID,
				"seals", k.SealCount+m.pendingFor(tenant, k.KeyID), "age", m.now().Sub(k.CreatedAt).Round(time.Hour))
		}
		return tx.InsertTenantKey(ctx, nk.id, nk.kekID, nk.wrapped)
	})
	if err != nil {
		return nil, err
	}
	if theirs != nil {
		return m.use(ctx, tenant, loaded(*theirs), *theirs)
	}
	a := &active{keyID: nk.id, dek: nk.dek, createdAt: m.now()}
	m.deks.Add(sealKey{tenant, a.keyID}, a.dek)
	m.active.Add(tenant, a)
	return a, nil
}

// use unwraps k into a, if it is not cached, and makes it the tenant's
// cached sealing key.
func (m *Manager) use(ctx context.Context, tenant uuid.UUID, a *active, k store.TenantKey) (*active, error) {
	// The key is usually still cached, so a reload of the active key every
	// minute does not call a remote keyring.
	dek, ok := m.deks.Get(sealKey{tenant, k.KeyID})
	if !ok || k.WrappedDEK == nil || k.DestroyedAt != nil {
		var err error
		if dek, err = m.unwrap(ctx, k); err != nil {
			return nil, err
		}
		m.deks.Add(sealKey{tenant, k.KeyID}, dek)
	}
	a.dek = dek
	m.active.Add(tenant, a)
	return a, nil
}

func loaded(k store.TenantKey) *active {
	return &active{keyID: k.KeyID, createdAt: k.CreatedAt, sealsAtLoad: k.SealCount}
}

func (m *Manager) needsRotation(tenant uuid.UUID, a *active) bool {
	seals := a.sealsAtLoad + a.flushed.Load() + m.pendingFor(tenant, a.keyID)
	return seals >= MaxSeals || m.now().Sub(a.createdAt) >= MaxAge
}

// Key returns the data key keyID of tenant, for opening chunks.
func (m *Manager) Key(ctx context.Context, tenant, keyID uuid.UUID) ([]byte, error) {
	if dek, ok := m.deks.Get(sealKey{tenant, keyID}); ok {
		return dek, nil
	}
	var k store.TenantKey
	err := m.db.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		var err error
		k, err = tx.TenantKey(ctx, keyID)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("data key %s: %w", keyID, err)
	}
	dek, err := m.unwrap(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("data key %s: %w", keyID, err)
	}
	m.deks.Add(sealKey{tenant, keyID}, dek)
	return dek, nil
}

func (m *Manager) unwrap(ctx context.Context, k store.TenantKey) ([]byte, error) {
	if k.WrappedDEK == nil || k.DestroyedAt != nil {
		return nil, ErrDestroyed
	}
	dek, err := m.kr.Unwrap(ctx, k.TenantID, k.KEKID, k.WrappedDEK)
	if errors.Is(err, keyring.ErrNotFound) {
		return nil, ErrDestroyed
	}
	if err != nil {
		return nil, fmt.Errorf("unwrap data key: %w", err)
	}
	return dek, nil
}

// Forget drops everything this process holds for a destroyed tenant: the
// sealing key, every other unwrapped key, retired ones included, and seal
// counts not flushed yet. A Key call that read the tenant's key row before
// the destroy committed can still cache a key after Forget; the destroy job
// calls Forget again once the tenant's KEK is gone and no unwrap can work.
func (m *Manager) Forget(tenant uuid.UUID) {
	m.active.Remove(tenant)
	m.deks.RemoveFunc(func(k sealKey) bool { return k.tenant == tenant })
	m.mu.Lock()
	for k := range m.pending {
		if k.tenant == tenant {
			delete(m.pending, k)
		}
	}
	m.mu.Unlock()
}

// NoteSeals records n chunks sealed with keyID. Counts are written to the
// database by Flush; a crash loses at most one flush interval of counts,
// which the 2^8 margin under the GCM bound absorbs.
func (m *Manager) NoteSeals(tenant, keyID uuid.UUID, n int64) {
	m.mu.Lock()
	m.pending[sealKey{tenant, keyID}] += n
	m.mu.Unlock()
}

func (m *Manager) pendingFor(tenant, keyID uuid.UUID) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pending[sealKey{tenant, keyID}]
}

// Flush writes pending seal counts to the database.
func (m *Manager) Flush(ctx context.Context) error {
	m.mu.Lock()
	batch := m.pending
	m.pending = map[sealKey]int64{}
	m.mu.Unlock()
	var errs []error
	for k, n := range batch {
		err := m.db.InTenantTx(ctx, k.tenant, func(ctx context.Context, tx *store.Tx) error {
			return tx.AddSealCount(ctx, k.key, n)
		})
		if err != nil {
			// Put the count back so the next flush retries it.
			m.NoteSeals(k.tenant, k.key, n)
			errs = append(errs, err)
			continue
		}
		if a, ok := m.active.Get(k.tenant); ok && a.keyID == k.key {
			a.flushed.Add(n)
		}
	}
	return errors.Join(errs...)
}
