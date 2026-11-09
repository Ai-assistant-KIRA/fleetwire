package store

import (
	"context"
	"encoding/json"
	"fmt"
)

// Audit is one entry to append. TenantID comes from the transaction: a
// tenant transaction writes a tenant entry, a transaction without a tenant
// writes a fleet-wide entry.
type Audit struct {
	ActorType  string
	ActorID    string
	Action     string
	TargetType string
	TargetID   string
	Result     string
	Details    map[string]any
}

// InsertAudit appends an audit entry.
func (t *Tx) InsertAudit(ctx context.Context, a Audit) error {
	details := []byte("{}")
	if len(a.Details) > 0 {
		var err error
		if details, err = json.Marshal(a.Details); err != nil {
			return fmt.Errorf("encode audit details: %w", err)
		}
	}
	_, err := t.tx.Exec(ctx, `
		INSERT INTO fleetwire.audit_log (tenant_id, actor_type, actor_id, action, target_type, target_id, result, details)
		VALUES (fleetwire.current_tenant(), $1, $2, $3, $4, $5, $6, $7)`,
		a.ActorType, a.ActorID, a.Action, a.TargetType, a.TargetID, a.Result, details)
	if err != nil {
		return fmt.Errorf("insert audit entry: %w", err)
	}
	return nil
}

const auditCols = `id, tenant_id, at, actor_type, actor_id, action, target_type, target_id, result, details`

// ListAudit pages through the current tenant's audit log, newest first.
// before = 0 starts at the newest entry.
func (t *Tx) ListAudit(ctx context.Context, before int64, limit int) ([]AuditEntry, error) {
	v, err := many[AuditEntry](t.tx.Query(ctx, `
		SELECT `+auditCols+` FROM fleetwire.audit_log
		WHERE tenant_id = fleetwire.current_tenant() AND ($1 = 0 OR id < $1)
		ORDER BY id DESC LIMIT $2`, before, limit))
	if err != nil {
		return nil, fmt.Errorf("list audit: %w", err)
	}
	return v, nil
}

// OperatorAudit pages through every audit entry, newest first.
func (t *Tx) OperatorAudit(ctx context.Context, before int64, limit int) ([]AuditEntry, error) {
	var b *int64
	if before > 0 {
		b = &before
	}
	v, err := many[AuditEntry](t.tx.Query(ctx, `SELECT `+auditCols+` FROM fleetwire.operator_audit($1, $2)`, b, limit))
	if err != nil {
		return nil, fmt.Errorf("operator audit: %w", err)
	}
	return v, nil
}
