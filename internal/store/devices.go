package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

const deviceCols = `id, tenant_id, name, status, acked_through, last_seen_at, last_batch_at,
	cert_serial, cert_not_after, created_at, revoked_at`

// InsertDevice registers a new device of the current tenant.
func (t *Tx) InsertDevice(ctx context.Context, id uuid.UUID, name string) (Device, error) {
	v, err := one[Device](t.tx.Query(ctx, `
		INSERT INTO fleetwire.devices (id, tenant_id, name) VALUES ($1, $2, $3)
		RETURNING `+deviceCols, id, t.tenant, name))
	if err != nil {
		return Device{}, fmt.Errorf("insert device: %w", err)
	}
	return v, nil
}

// GetDevice returns one device of the current tenant.
func (t *Tx) GetDevice(ctx context.Context, id uuid.UUID) (Device, error) {
	v, err := one[Device](t.tx.Query(ctx, `SELECT `+deviceCols+` FROM fleetwire.devices WHERE id = $1`, id))
	if err != nil {
		return Device{}, fmt.Errorf("get device: %w", err)
	}
	return v, nil
}

// ListDevices pages through the current tenant's devices by ID.
// An empty status means every status.
func (t *Tx) ListDevices(ctx context.Context, status string, after uuid.UUID, limit int) ([]Device, error) {
	v, err := many[Device](t.tx.Query(ctx, `
		SELECT `+deviceCols+` FROM fleetwire.devices
		WHERE ($1 = '' OR status = $1) AND id > $2
		ORDER BY id LIMIT $3`, status, after, limit))
	if err != nil {
		return nil, fmt.Errorf("list devices: %w", err)
	}
	return v, nil
}

// RevokeDevice marks an active device revoked and revokes its certificates.
// It reports false if the device was not active.
func (t *Tx) RevokeDevice(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.devices SET status = 'revoked', revoked_at = now()
		WHERE id = $1 AND status = 'active'`, id)
	if err != nil {
		return false, fmt.Errorf("revoke device: %w", err)
	}
	if _, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.device_certs SET revoked_at = now()
		WHERE device_id = $1 AND revoked_at IS NULL`, id); err != nil {
		return false, fmt.Errorf("revoke device certs: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RevokeAllDevices revokes every active device of the current tenant.
func (t *Tx) RevokeAllDevices(ctx context.Context) (int64, error) {
	tag, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.devices SET status = 'revoked', revoked_at = now() WHERE status = 'active'`)
	if err != nil {
		return 0, fmt.Errorf("revoke devices: %w", err)
	}
	if _, err := t.tx.Exec(ctx, `UPDATE fleetwire.device_certs SET revoked_at = now() WHERE revoked_at IS NULL`); err != nil {
		return 0, fmt.Errorf("revoke certs: %w", err)
	}
	return tag.RowsAffected(), nil
}

// MarkDeviceDeleted sets a revoked device to deleted after its data is gone.
func (t *Tx) MarkDeviceDeleted(ctx context.Context, id uuid.UUID) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.devices SET status = 'deleted', acked_through = 0, last_seen_at = NULL, last_batch_at = NULL
		WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark device deleted: %w", err)
	}
	return nil
}

// DeleteDevices deletes up to limit device rows of the current tenant whose
// data is already gone (tenant destroy).
func (t *Tx) DeleteDevices(ctx context.Context, limit int) (int64, error) {
	tag, err := t.tx.Exec(ctx, `
		DELETE FROM fleetwire.devices WHERE id IN (SELECT id FROM fleetwire.devices LIMIT $1)`, limit)
	if err != nil {
		return 0, fmt.Errorf("delete devices: %w", err)
	}
	return tag.RowsAffected(), nil
}

// DeviceLookup finds a device's tenant and state without a tenant context.
func (t *Tx) DeviceLookup(ctx context.Context, id uuid.UUID) (DeviceRef, error) {
	v, err := one[DeviceRef](t.tx.Query(ctx, `SELECT * FROM fleetwire.device_lookup($1)`, id))
	if err != nil {
		return DeviceRef{}, fmt.Errorf("device lookup: %w", err)
	}
	return v, nil
}

// SetDeviceCert records the device's current certificate.
func (t *Tx) SetDeviceCert(ctx context.Context, c DeviceCert) error {
	if _, err := t.tx.Exec(ctx, `
		INSERT INTO fleetwire.device_certs (serial, tenant_id, device_id, not_after) VALUES ($1, $2, $3, $4)`,
		c.Serial, t.tenant, c.DeviceID, c.NotAfter); err != nil {
		return fmt.Errorf("insert device cert: %w", err)
	}
	if _, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.devices SET cert_serial = $2, cert_not_after = $3 WHERE id = $1`,
		c.DeviceID, c.Serial, c.NotAfter); err != nil {
		return fmt.Errorf("update device cert: %w", err)
	}
	return nil
}

// RevokedCerts lists revoked, unexpired certificates of every tenant (CRL).
func (t *Tx) RevokedCerts(ctx context.Context) ([]RevokedCert, error) {
	v, err := many[RevokedCert](t.tx.Query(ctx, `SELECT * FROM fleetwire.revoked_device_certs()`))
	if err != nil {
		return nil, fmt.Errorf("revoked certs: %w", err)
	}
	return v, nil
}

// DeleteExpiredCerts removes certificate rows that can no longer be used.
func (t *Tx) DeleteExpiredCerts(ctx context.Context) (int64, error) {
	var n int64
	if err := t.tx.QueryRow(ctx, `SELECT fleetwire.delete_expired_device_certs()`).Scan(&n); err != nil {
		return 0, fmt.Errorf("delete expired certs: %w", err)
	}
	return n, nil
}
