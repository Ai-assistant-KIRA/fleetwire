package store

import (
	"context"
	"fmt"
)

// LockActiveExports takes a transaction-scoped lock on the current
// tenant's exports and counts those pending or running. The lock lasts to
// the end of the transaction, so concurrent requests that check a cap and
// then insert cannot both pass. It is an advisory lock rather than a lock
// on the tenant row, so ingest's foreign-key checks on that row never wait
// for it.
func (t *Tx) LockActiveExports(ctx context.Context) (int, error) {
	if _, err := t.tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended('fleetwire.exports:' || fleetwire.current_tenant()::text, 0))`); err != nil {
		return 0, fmt.Errorf("lock exports: %w", err)
	}
	var n int
	if err := t.tx.QueryRow(ctx, `
		SELECT count(*) FROM fleetwire.exports WHERE status IN ('pending', 'running')`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count active exports: %w", err)
	}
	return n, nil
}
