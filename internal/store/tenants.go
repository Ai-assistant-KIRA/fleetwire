package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// CreateTenant inserts the tenant this transaction is scoped to.
func (t *Tx) CreateTenant(ctx context.Context, name string, retentionDays int) (Tenant, error) {
	v, err := one[Tenant](t.tx.Query(ctx, `
		INSERT INTO fleetwire.tenants (id, name, retention_days) VALUES ($1, $2, $3)
		RETURNING id, name, status, retention_days, created_at, destroyed_at`,
		t.tenant, name, retentionDays))
	if err != nil {
		return Tenant{}, fmt.Errorf("create tenant: %w", err)
	}
	return v, nil
}

// GetTenant returns the current tenant.
func (t *Tx) GetTenant(ctx context.Context) (Tenant, error) {
	v, err := one[Tenant](t.tx.Query(ctx, `
		SELECT id, name, status, retention_days, created_at, destroyed_at
		FROM fleetwire.tenants WHERE id = fleetwire.current_tenant()`))
	if err != nil {
		return Tenant{}, fmt.Errorf("get tenant: %w", err)
	}
	return v, nil
}

// SetRetention changes the current tenant's retention period.
func (t *Tx) SetRetention(ctx context.Context, days int) (Tenant, error) {
	v, err := one[Tenant](t.tx.Query(ctx, `
		UPDATE fleetwire.tenants SET retention_days = $1 WHERE id = fleetwire.current_tenant()
		RETURNING id, name, status, retention_days, created_at, destroyed_at`, days))
	if err != nil {
		return Tenant{}, fmt.Errorf("set retention: %w", err)
	}
	return v, nil
}

// SetTenantStatus moves the current tenant to status.
func (t *Tx) SetTenantStatus(ctx context.Context, status string) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.tenants
		SET status = $1, destroyed_at = CASE WHEN $1 = 'destroyed' THEN now() ELSE destroyed_at END
		WHERE id = fleetwire.current_tenant()`, status)
	if err != nil {
		return fmt.Errorf("set tenant status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListTenants pages through every tenant (operator view).
func (t *Tx) ListTenants(ctx context.Context, after uuid.UUID, limit int) ([]Tenant, error) {
	var a *uuid.UUID
	if after != uuid.Nil {
		a = &after
	}
	v, err := many[Tenant](t.tx.Query(ctx, `SELECT * FROM fleetwire.list_tenants($1, $2)`, a, limit))
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	return v, nil
}

// ActiveTenant is a tenant the scheduler works on.
type ActiveTenant struct {
	ID            uuid.UUID `db:"id"`
	RetentionDays int       `db:"retention_days"`
}

// ListActiveTenants returns every active tenant (scheduler view).
func (t *Tx) ListActiveTenants(ctx context.Context) ([]ActiveTenant, error) {
	v, err := many[ActiveTenant](t.tx.Query(ctx, `SELECT * FROM fleetwire.list_active_tenants()`))
	if err != nil {
		return nil, fmt.Errorf("list active tenants: %w", err)
	}
	return v, nil
}

// InsertTenantKey stores a new wrapped data key.
func (t *Tx) InsertTenantKey(ctx context.Context, keyID uuid.UUID, kekID string, wrapped []byte) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO fleetwire.tenant_keys (tenant_id, key_id, kek_id, wrapped_dek) VALUES ($1, $2, $3, $4)`,
		t.tenant, keyID, kekID, wrapped)
	if err != nil {
		return fmt.Errorf("insert tenant key: %w", err)
	}
	return nil
}

const tenantKeyCols = `tenant_id, key_id, kek_id, wrapped_dek, seal_count, created_at, retired_at, destroyed_at`

// ActiveTenantKey returns the newest key that may still seal new chunks.
func (t *Tx) ActiveTenantKey(ctx context.Context) (TenantKey, error) {
	v, err := one[TenantKey](t.tx.Query(ctx, `
		SELECT `+tenantKeyCols+` FROM fleetwire.tenant_keys
		WHERE tenant_id = fleetwire.current_tenant() AND retired_at IS NULL AND destroyed_at IS NULL
		ORDER BY created_at DESC LIMIT 1`))
	if err != nil {
		return TenantKey{}, fmt.Errorf("active tenant key: %w", err)
	}
	return v, nil
}

// TenantKey returns one key of the current tenant.
func (t *Tx) TenantKey(ctx context.Context, keyID uuid.UUID) (TenantKey, error) {
	v, err := one[TenantKey](t.tx.Query(ctx, `
		SELECT `+tenantKeyCols+` FROM fleetwire.tenant_keys
		WHERE tenant_id = fleetwire.current_tenant() AND key_id = $1`, keyID))
	if err != nil {
		return TenantKey{}, fmt.Errorf("tenant key: %w", err)
	}
	return v, nil
}

// AddSealCount adds n seals to a key's counter.
func (t *Tx) AddSealCount(ctx context.Context, keyID uuid.UUID, n int64) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.tenant_keys SET seal_count = seal_count + $2
		WHERE tenant_id = fleetwire.current_tenant() AND key_id = $1`, keyID, n)
	if err != nil {
		return fmt.Errorf("add seal count: %w", err)
	}
	return nil
}

// RetireTenantKey stops a key from sealing new chunks. It still opens old ones.
func (t *Tx) RetireTenantKey(ctx context.Context, keyID uuid.UUID) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.tenant_keys SET retired_at = now()
		WHERE tenant_id = fleetwire.current_tenant() AND key_id = $1 AND retired_at IS NULL`, keyID)
	if err != nil {
		return fmt.Errorf("retire tenant key: %w", err)
	}
	return nil
}

// LockTenant takes a row lock on the current tenant, to serialize key
// rotation and destroy steps across replicas.
func (t *Tx) LockTenant(ctx context.Context) (Tenant, error) {
	v, err := one[Tenant](t.tx.Query(ctx, `
		SELECT id, name, status, retention_days, created_at, destroyed_at
		FROM fleetwire.tenants WHERE id = fleetwire.current_tenant() FOR UPDATE`))
	if err != nil {
		return Tenant{}, fmt.Errorf("lock tenant: %w", err)
	}
	return v, nil
}

// ShareLockTenant reads the current tenant and keeps its status from
// changing until the transaction ends, without blocking other readers of
// the row. A tenant destroy (LockTenant) waits for it.
func (t *Tx) ShareLockTenant(ctx context.Context) (Tenant, error) {
	v, err := one[Tenant](t.tx.Query(ctx, `
		SELECT id, name, status, retention_days, created_at, destroyed_at
		FROM fleetwire.tenants WHERE id = fleetwire.current_tenant() FOR SHARE`))
	if err != nil {
		return Tenant{}, fmt.Errorf("share-lock tenant: %w", err)
	}
	return v, nil
}

// DestroyTenantKeys deletes every wrapped data key of the current tenant,
// keeping the rows as tombstones.
func (t *Tx) DestroyTenantKeys(ctx context.Context) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.tenant_keys SET wrapped_dek = NULL, destroyed_at = coalesce(destroyed_at, now())
		WHERE tenant_id = fleetwire.current_tenant()`)
	if err != nil {
		return fmt.Errorf("destroy tenant keys: %w", err)
	}
	return nil
}
