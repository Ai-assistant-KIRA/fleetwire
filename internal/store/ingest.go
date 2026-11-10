package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/coverage"
)

// Statements of the batch transaction. Tx.SetTenant and Tx.InsertAudit use
// the same set_config and audit insert.
const (
	sqlInsertChunk = `
		INSERT INTO fleetwire.chunks (tenant_id, device_id, seq_lo, seq_hi, reading_count,
			min_collected_at, max_collected_at, key_id, nonce, ciphertext)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	sqlInsertGap = `
		INSERT INTO fleetwire.device_gaps (tenant_id, device_id, from_seq, to_seq, reason)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT (device_id, from_seq) DO NOTHING`
	sqlUpdateProgress = `
		UPDATE fleetwire.devices SET acked_through = $2, last_seen_at = now(), last_batch_at = $3
		WHERE id = $1`
	sqlInsertAudit = `
		INSERT INTO fleetwire.audit_log (tenant_id, actor_type, actor_id, action, target_type, target_id, result, details)
		VALUES (fleetwire.current_tenant(), $1, $2, $3, $4, $5, $6, $7)`
	sqlDeviceRanges = `SELECT lo, hi FROM fleetwire.device_ranges WHERE device_id = $1 ORDER BY lo`
	sqlDeleteRanges = `DELETE FROM fleetwire.device_ranges WHERE device_id = $1`
	// COPY would be faster, but PostgreSQL refuses COPY FROM on tables with
	// row-level security, so the rows go in as one INSERT from arrays.
	sqlInsertRanges = `
		INSERT INTO fleetwire.device_ranges (tenant_id, device_id, lo, hi)
		SELECT $1, $2, lo, hi FROM unnest($3::bigint[], $4::bigint[]) AS r(lo, hi)`
	sqlSetTenant = `SELECT set_config('fleetwire.tenant_id', $1, true)`
)

// rollbackTimeout bounds the ROLLBACK after a failed batch transaction, so
// a dead connection cannot hold the caller; the pool then drops it.
const rollbackTimeout = 5 * time.Second

// IngestState is what the batch transaction reads, under the device's row
// lock and in the tenant's scope.
type IngestState struct {
	// Found is false when no device has the ID.
	Found  bool
	Device IngestDevice
	// Ranges are the covered ranges above the watermark (ADR-0005).
	Ranges []coverage.Range
}

// IngestWrite is what the batch transaction writes. It commits as a whole.
type IngestWrite struct {
	Chunks []Chunk
	Gaps   []DeviceGap
	// ReplaceRanges replaces the device's covered ranges with Ranges.
	ReplaceRanges bool
	Ranges        []coverage.Range
	// UpdateProgress sets the watermark and the time of the last batch.
	UpdateProgress bool
	AckedThrough   uint64
	BatchAt        time.Time
	Audit          *Audit
}

func (w IngestWrite) empty() bool {
	return len(w.Chunks) == 0 && len(w.Gaps) == 0 && !w.ReplaceRanges && !w.UpdateProgress && w.Audit == nil
}

// IngestBatch runs ingest's transaction for one batch in two round trips.
// The first pipeline begins the transaction, locks the device row
// (device_for_ingest), sets the tenant, and reads the covered ranges; decide
// turns that into the writes, and the second pipeline sends them with the
// COMMIT. Sent one statement at a time, the same transaction made eight
// round trips, and on a busy machine the database spent much of each
// transaction waiting for the next statement while holding the row lock.
//
// tenant is the device's tenant as the caller knows it; decide must refuse
// the batch if st.Device.TenantID differs. decide is called at most once,
// without the database: an error from it, or an empty IngestWrite, rolls
// the transaction back.
func (db *DB) IngestBatch(ctx context.Context, device, tenant uuid.UUID, decide func(st IngestState) (IngestWrite, error)) error {
	if tenant == uuid.Nil {
		return errors.New("store: tenant id must not be nil")
	}
	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	// Release destroys a connection that is not idle, so one that an error
	// below leaves inside a transaction never goes back to the pool; the
	// server rolls that transaction back when the connection closes.
	defer conn.Release()
	committed := false
	defer func() {
		if !committed {
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
			defer cancel()
			// An error here leaves the connection inside the transaction,
			// and Release discards it.
			_, _ = conn.Exec(rctx, "ROLLBACK")
		}
	}()

	st, err := readIngestState(ctx, conn, device, tenant)
	if err != nil {
		return err
	}
	w, err := decide(st)
	if err != nil || w.empty() {
		return err
	}
	if err := writeIngest(ctx, conn, device, tenant, w); err != nil {
		return err
	}
	committed = true
	return nil
}

func readIngestState(ctx context.Context, conn *pgxpool.Conn, device, tenant uuid.UUID) (IngestState, error) {
	b := &pgx.Batch{}
	b.Queue("BEGIN")
	b.Queue(`SELECT * FROM fleetwire.device_for_ingest($1)`, device)
	b.Queue(sqlSetTenant, tenant.String())
	b.Queue(sqlDeviceRanges, device)
	br := conn.SendBatch(ctx, b)
	st, err := scanIngestState(br)
	if cerr := br.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("read device state: %w", cerr)
	}
	return st, err
}

func scanIngestState(br pgx.BatchResults) (IngestState, error) {
	var st IngestState
	if _, err := br.Exec(); err != nil {
		return st, fmt.Errorf("begin: %w", err)
	}
	devs, err := pgx.CollectRows(must(br.Query()), pgx.RowToStructByName[IngestDevice])
	if err != nil {
		return st, fmt.Errorf("device for ingest: %w", err)
	}
	if len(devs) == 1 {
		st.Found, st.Device = true, devs[0]
	}
	if _, err := br.Exec(); err != nil {
		return st, fmt.Errorf("set tenant: %w", err)
	}
	st.Ranges, err = pgx.CollectRows(must(br.Query()), func(r pgx.CollectableRow) (coverage.Range, error) {
		var c coverage.Range
		err := r.Scan(&c.Lo, &c.Hi)
		return c, err
	})
	if err != nil {
		return st, fmt.Errorf("device ranges: %w", err)
	}
	return st, nil
}

// must drops the error of BatchResults.Query: it is also held by the rows,
// and CollectRows returns it.
func must(rows pgx.Rows, _ error) pgx.Rows { return rows }

func writeIngest(ctx context.Context, conn *pgxpool.Conn, device, tenant uuid.UUID, w IngestWrite) error {
	b := &pgx.Batch{}
	var what []string
	queue := func(desc, sql string, args ...any) {
		b.Queue(sql, args...)
		what = append(what, desc)
	}
	for _, c := range w.Chunks {
		queue(fmt.Sprintf("insert chunk %d-%d", c.SeqLo, c.SeqHi), sqlInsertChunk,
			tenant, device, c.SeqLo, c.SeqHi, c.ReadingCount, c.MinCollectedAt, c.MaxCollectedAt, c.KeyID, c.Nonce, c.Ciphertext)
	}
	for _, g := range w.Gaps {
		queue(fmt.Sprintf("insert gap %d-%d", g.FromSeq, g.ToSeq), sqlInsertGap, tenant, device, g.FromSeq, g.ToSeq, g.Reason)
	}
	if w.ReplaceRanges {
		queue("clear device ranges", sqlDeleteRanges, device)
		if len(w.Ranges) > 0 {
			los, his := make([]int64, len(w.Ranges)), make([]int64, len(w.Ranges))
			for i, r := range w.Ranges {
				los[i], his[i] = int64(r.Lo), int64(r.Hi)
			}
			queue("insert device ranges", sqlInsertRanges, tenant, device, los, his)
		}
	}
	if w.UpdateProgress {
		queue("update device progress", sqlUpdateProgress, device, w.AckedThrough, w.BatchAt)
	}
	if w.Audit != nil {
		details, err := w.Audit.detailsJSON()
		if err != nil {
			return err
		}
		a := w.Audit
		queue("insert audit entry", sqlInsertAudit, a.ActorType, a.ActorID, a.Action, a.TargetType, a.TargetID, a.Result, details)
	}
	queue("commit", "COMMIT")
	br := conn.SendBatch(ctx, b)
	err := execAll(br, what)
	if cerr := br.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("write batch: %w", cerr)
	}
	return err
}

// execAll reads every result. After a failed statement the server skips
// the rest of the pipeline, COMMIT included, and the transaction is left
// aborted for the caller to roll back.
func execAll(br pgx.BatchResults, what []string) error {
	for i, desc := range what {
		tag, err := br.Exec()
		if err != nil {
			return fmt.Errorf("%s: %w", desc, err)
		}
		// A COMMIT of a transaction that failed earlier answers ROLLBACK.
		if i == len(what)-1 && tag.String() != "COMMIT" {
			return fmt.Errorf("commit: the server answered %q", tag.String())
		}
	}
	return nil
}
