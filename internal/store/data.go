package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/coverage"
)

// DeviceRanges returns the covered ranges above the device's watermark.
func (t *Tx) DeviceRanges(ctx context.Context, deviceID uuid.UUID) ([]coverage.Range, error) {
	rows, err := t.tx.Query(ctx, `SELECT lo, hi FROM fleetwire.device_ranges WHERE device_id = $1 ORDER BY lo`, deviceID)
	if err != nil {
		return nil, fmt.Errorf("device ranges: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (coverage.Range, error) {
		var c coverage.Range
		err := r.Scan(&c.Lo, &c.Hi)
		return c, err
	})
	if err != nil {
		return nil, fmt.Errorf("device ranges: %w", err)
	}
	return out, nil
}

// ReplaceDeviceRanges stores the device's covered ranges above the watermark.
func (t *Tx) ReplaceDeviceRanges(ctx context.Context, deviceID uuid.UUID, ranges []coverage.Range) error {
	if _, err := t.tx.Exec(ctx, `DELETE FROM fleetwire.device_ranges WHERE device_id = $1`, deviceID); err != nil {
		return fmt.Errorf("clear device ranges: %w", err)
	}
	if len(ranges) == 0 {
		return nil
	}
	// COPY is faster but PostgreSQL refuses COPY FROM on tables with
	// row-level security, so the rows go in as one INSERT from arrays.
	los, his := make([]int64, len(ranges)), make([]int64, len(ranges))
	for i, r := range ranges {
		los[i], his[i] = int64(r.Lo), int64(r.Hi)
	}
	if _, err := t.tx.Exec(ctx, `
		INSERT INTO fleetwire.device_ranges (tenant_id, device_id, lo, hi)
		SELECT $1, $2, lo, hi FROM unnest($3::bigint[], $4::bigint[]) AS r(lo, hi)`,
		t.tenant, deviceID, los, his); err != nil {
		return fmt.Errorf("insert device ranges: %w", err)
	}
	return nil
}

// InsertChunk stores one encrypted chunk. The exclusion constraint rejects
// any overlap with chunks already stored for the device.
func (t *Tx) InsertChunk(ctx context.Context, c Chunk) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO fleetwire.chunks (tenant_id, device_id, seq_lo, seq_hi, reading_count,
			min_collected_at, max_collected_at, key_id, nonce, ciphertext)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		t.tenant, c.DeviceID, c.SeqLo, c.SeqHi, c.ReadingCount, c.MinCollectedAt, c.MaxCollectedAt,
		c.KeyID, c.Nonce, c.Ciphertext)
	if err != nil {
		return fmt.Errorf("insert chunk %d-%d: %w", c.SeqLo, c.SeqHi, err)
	}
	return nil
}

// InsertGap records a reported gap. A repeat of the same gap is a no-op.
func (t *Tx) InsertGap(ctx context.Context, g DeviceGap) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO fleetwire.device_gaps (tenant_id, device_id, from_seq, to_seq, reason)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT (device_id, from_seq) DO NOTHING`,
		t.tenant, g.DeviceID, g.FromSeq, g.ToSeq, g.Reason)
	if err != nil {
		return fmt.Errorf("insert gap %d-%d: %w", g.FromSeq, g.ToSeq, err)
	}
	return nil
}

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
