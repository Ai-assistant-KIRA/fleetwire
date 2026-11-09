// Package tenantkey manages tenant data keys (DEKs): creating them, picking
// the one that seals new chunks, rotating it, and caching unwrapped keys for
// a short time so that every chunk does not cost a keyring call.
//
// Key rows are always committed in their own transaction before a chunk
// uses them, so a rolled-back ingest transaction can never leave a cached
// key that the database does not know.
package tenantkey

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

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
)

// ErrDestroyed means the tenant's keys were destroyed: its data is gone.
var ErrDestroyed = errors.New("tenantkey: tenant keys are destroyed")

type active struct {
	keyID       uuid.UUID
	dek         []byte
	createdAt   time.Time
	sealsAtLoad int64
}

type sealKey struct {
	tenant, key uuid.UUID
}

// Manager is safe for concurrent use.
type Manager struct {
	db     *store.DB
	kr     keyring.Keyring
	log    *slog.Logger
	deks   *lru.Cache[uuid.UUID, []byte]
	active *lru.Cache[uuid.UUID, *active]

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
		deks:    lru.New[uuid.UUID, []byte](cacheSize, cacheTTL),
		active:  lru.New[uuid.UUID, *active](cacheSize, activeTTL),
		pending: map[sealKey]int64{},
		now:     time.Now,
	}
}

// CreateFirstKey creates the tenant's data key inside the transaction that
// creates the tenant. The tenant's KEK must already exist in the keyring.
func (m *Manager) CreateFirstKey(ctx context.Context, tx *store.Tx) error {
	_, _, err := m.insertKey(ctx, tx)
	return err
}

func (m *Manager) insertKey(ctx context.Context, tx *store.Tx) (uuid.UUID, []byte, error) {
	dek, err := envelope.NewKey()
	if err != nil {
		return uuid.Nil, nil, err
	}
	wrapped, kekID, err := m.kr.Wrap(ctx, tx.Tenant(), dek)
	if err != nil {
		return uuid.Nil, nil, fmt.Errorf("wrap data key: %w", err)
	}
	keyID, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, nil, fmt.Errorf("key id: %w", err)
	}
	if err := tx.InsertTenantKey(ctx, keyID, kekID, wrapped); err != nil {
		return uuid.Nil, nil, err
	}
	return keyID, dek, nil
}

// SealingKey returns the key that seals new chunks of tenant, rotating it
// when it is too old or has sealed too many chunks.
func (m *Manager) SealingKey(ctx context.Context, tenant uuid.UUID) (uuid.UUID, []byte, error) {
	if a, ok := m.active.Get(tenant); ok && !m.needsRotation(a) {
		return a.keyID, a.dek, nil
	}
	var a *active
	err := m.db.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		// The tenant row lock makes concurrent replicas rotate once.
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
		default:
			a = &active{keyID: k.KeyID, createdAt: k.CreatedAt, sealsAtLoad: k.SealCount}
			if !m.needsRotation(a) {
				a.dek, err = m.unwrap(ctx, k)
				return err
			}
			if err := tx.RetireTenantKey(ctx, k.KeyID); err != nil {
				return err
			}
			m.log.Info("rotating tenant data key", "tenant_id", tenant, "old_key_id", k.KeyID,
				"seals", k.SealCount+m.pendingFor(tenant, k.KeyID), "age", m.now().Sub(k.CreatedAt).Round(time.Hour))
		}
		keyID, dek, err := m.insertKey(ctx, tx)
		if err != nil {
			return err
		}
		a = &active{keyID: keyID, dek: dek, createdAt: m.now()}
		return nil
	})
	if err != nil {
		return uuid.Nil, nil, fmt.Errorf("sealing key: %w", err)
	}
	m.active.Add(tenant, a)
	m.deks.Add(a.keyID, a.dek)
	return a.keyID, a.dek, nil
}

func (m *Manager) needsRotation(a *active) bool {
	return a.sealsAtLoad+m.pendingForKey(a.keyID) >= MaxSeals || m.now().Sub(a.createdAt) >= MaxAge
}

// Key returns the data key keyID of tenant, for opening chunks.
func (m *Manager) Key(ctx context.Context, tenant, keyID uuid.UUID) ([]byte, error) {
	if dek, ok := m.deks.Get(keyID); ok {
		return dek, nil
	}
	var dek []byte
	err := m.db.InTenantTx(ctx, tenant, func(ctx context.Context, tx *store.Tx) error {
		k, err := tx.TenantKey(ctx, keyID)
		if err != nil {
			return err
		}
		dek, err = m.unwrap(ctx, k)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("data key %s: %w", keyID, err)
	}
	m.deks.Add(keyID, dek)
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

// Forget drops every cached key of a destroyed tenant from this process.
func (m *Manager) Forget(tenant uuid.UUID) {
	if a, ok := m.active.Get(tenant); ok {
		m.deks.Remove(a.keyID)
	}
	m.active.Remove(tenant)
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

func (m *Manager) pendingForKey(keyID uuid.UUID) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for k, v := range m.pending {
		if k.key == keyID {
			n += v
		}
	}
	return n
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
			a2 := *a
			a2.sealsAtLoad += n
			m.active.Add(k.tenant, &a2)
		}
	}
	return errors.Join(errs...)
}
