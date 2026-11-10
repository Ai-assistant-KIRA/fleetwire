package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Chunks, gaps and covered ranges are written only by DB.IngestBatch.

// ListGaps pages through a device's recorded gaps in seq order.
func (t *Tx) ListGaps(ctx context.Context, deviceID uuid.UUID, afterFrom uint64, limit int) ([]DeviceGap, error) {
	v, err := many[DeviceGap](t.tx.Query(ctx, `
		SELECT tenant_id, device_id, from_seq, to_seq, reason, reported_at FROM fleetwire.device_gaps
		WHERE device_id = $1 AND from_seq > $2 ORDER BY from_seq LIMIT $3`, deviceID, afterFrom, limit))
	if err != nil {
		return nil, fmt.Errorf("list gaps: %w", err)
	}
	return v, nil
}

// CountGaps returns the number of gap rows and gap seqs of a device.
func (t *Tx) CountGaps(ctx context.Context, deviceID uuid.UUID) (rows, seqs int64, err error) {
	err = t.tx.QueryRow(ctx, `
		SELECT count(*), coalesce(sum(to_seq - from_seq + 1), 0)::bigint
		FROM fleetwire.device_gaps WHERE device_id = $1`, deviceID).Scan(&rows, &seqs)
	if err != nil {
		return 0, 0, fmt.Errorf("count gaps: %w", err)
	}
	return rows, seqs, nil
}

const chunkCols = `tenant_id, device_id, seq_lo, seq_hi, reading_count, min_collected_at, max_collected_at,
	key_id, nonce, ciphertext, received_at`

// ChunksBySeq pages through all of a device's chunks in seq order, starting
// after seq afterSeq. Used by exports and the checker.
func (t *Tx) ChunksBySeq(ctx context.Context, deviceID uuid.UUID, afterSeq uint64, limit int) ([]Chunk, error) {
	v, err := many[Chunk](t.tx.Query(ctx, `
		SELECT `+chunkCols+` FROM fleetwire.chunks
		WHERE device_id = $1 AND seq_lo > $2 ORDER BY seq_lo LIMIT $3`, deviceID, afterSeq, limit))
	if err != nil {
		return nil, fmt.Errorf("chunks by seq: %w", err)
	}
	return v, nil
}

// ChunksInWindow pages through a device's chunks that may hold readings
// collected in [from, to], in seq order, starting at chunks whose seq_hi is
// above afterSeq (a cursor can stop inside a chunk).
func (t *Tx) ChunksInWindow(ctx context.Context, deviceID uuid.UUID, from, to time.Time, afterSeq uint64, limit int) ([]Chunk, error) {
	v, err := many[Chunk](t.tx.Query(ctx, `
		SELECT `+chunkCols+` FROM fleetwire.chunks
		WHERE tenant_id = fleetwire.current_tenant() AND device_id = $1
		  AND max_collected_at >= $2 AND min_collected_at <= $3 AND seq_hi > $4
		ORDER BY seq_lo LIMIT $5`, deviceID, from, to, afterSeq, limit))
	if err != nil {
		return nil, fmt.Errorf("chunks in window: %w", err)
	}
	return v, nil
}

// DeleteChunksBefore deletes up to limit chunks of the current tenant whose
// newest reading is older than cutoff (retention).
func (t *Tx) DeleteChunksBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tag, err := t.tx.Exec(ctx, `
		DELETE FROM fleetwire.chunks WHERE ctid IN (
			SELECT ctid FROM fleetwire.chunks
			WHERE tenant_id = fleetwire.current_tenant() AND max_collected_at < $1 LIMIT $2)`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("delete old chunks: %w", err)
	}
	return tag.RowsAffected(), nil
}

// DeleteGapsBefore deletes up to limit gap records reported before cutoff.
func (t *Tx) DeleteGapsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tag, err := t.tx.Exec(ctx, `
		DELETE FROM fleetwire.device_gaps WHERE ctid IN (
			SELECT ctid FROM fleetwire.device_gaps
			WHERE tenant_id = fleetwire.current_tenant() AND reported_at < $1 LIMIT $2)`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("delete old gaps: %w", err)
	}
	return tag.RowsAffected(), nil
}

// DeleteDeviceData deletes up to limit chunks of one device, and once none
// are left, its gaps and ranges. It returns the number of chunks deleted.
func (t *Tx) DeleteDeviceData(ctx context.Context, deviceID uuid.UUID, limit int) (int64, error) {
	tag, err := t.tx.Exec(ctx, `
		DELETE FROM fleetwire.chunks WHERE ctid IN (
			SELECT ctid FROM fleetwire.chunks WHERE device_id = $1 LIMIT $2)`, deviceID, limit)
	if err != nil {
		return 0, fmt.Errorf("delete device chunks: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return tag.RowsAffected(), nil
	}
	if _, err := t.tx.Exec(ctx, `DELETE FROM fleetwire.device_gaps WHERE device_id = $1`, deviceID); err != nil {
		return 0, fmt.Errorf("delete device gaps: %w", err)
	}
	if _, err := t.tx.Exec(ctx, `DELETE FROM fleetwire.device_ranges WHERE device_id = $1`, deviceID); err != nil {
		return 0, fmt.Errorf("delete device ranges: %w", err)
	}
	return 0, nil
}

// DeleteTenantData deletes up to limit rows from one of the current
// tenant's data tables, in an order that respects the foreign keys.
// It returns 0 when nothing is left.
func (t *Tx) DeleteTenantData(ctx context.Context, limit int) (int64, error) {
	for _, table := range []string{"chunks", "device_gaps", "device_ranges", "enrollment_tokens", "api_tokens", "exports"} {
		tag, err := t.tx.Exec(ctx, `
			DELETE FROM fleetwire.`+table+` WHERE ctid IN (
				SELECT ctid FROM fleetwire.`+table+` WHERE tenant_id = fleetwire.current_tenant() LIMIT $1)`, limit)
		if err != nil {
			return 0, fmt.Errorf("delete %s: %w", table, err)
		}
		if n := tag.RowsAffected(); n > 0 {
			return n, nil
		}
	}
	return t.DeleteDevices(ctx, limit)
}
