package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// InsertEnrollmentToken stores a new enrollment token hash.
func (t *Tx) InsertEnrollmentToken(ctx context.Context, id uuid.UUID, hash []byte, expires time.Time, createdBy string) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO fleetwire.enrollment_tokens (id, tenant_id, token_hash, expires_at, created_by)
		VALUES ($1, $2, $3, $4, $5)`, id, t.tenant, hash, expires, createdBy)
	if err != nil {
		return fmt.Errorf("insert enrollment token: %w", err)
	}
	return nil
}

// ConsumeEnrollmentToken atomically uses a token for deviceID. It works
// without a tenant context and returns the token's tenant and the outcome.
// It holds the tenant row FOR SHARE until the transaction ends, so the
// tenant stays active while the caller stores the new device: a destroy
// waits, then revokes the device with the others.
func (t *Tx) ConsumeEnrollmentToken(ctx context.Context, hash []byte, deviceID uuid.UUID) (uuid.UUID, string, error) {
	var tenant *uuid.UUID
	var outcome string
	err := t.tx.QueryRow(ctx, `SELECT tenant_id, outcome FROM fleetwire.consume_enrollment_token($1, $2)`,
		hash, deviceID).Scan(&tenant, &outcome)
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("consume enrollment token: %w", err)
	}
	if tenant == nil {
		return uuid.Nil, outcome, nil
	}
	return *tenant, outcome, nil
}

// PruneEnrollmentTokens deletes up to limit enrollment tokens of every
// tenant that expired more than 7 days ago (migration 00008).
func (t *Tx) PruneEnrollmentTokens(ctx context.Context, limit int) (int64, error) {
	var n int64
	if err := t.tx.QueryRow(ctx, `SELECT fleetwire.prune_enrollment_tokens($1)`, limit).Scan(&n); err != nil {
		return 0, fmt.Errorf("prune enrollment tokens: %w", err)
	}
	return n, nil
}

//nolint:gosec // G101 false positive: a column list, not a credential.
const apiTokenCols = `id, tenant_id, name, token_hash, scopes, created_at, last_used_at, revoked_at`

// InsertAPIToken stores a new tenant API token hash.
func (t *Tx) InsertAPIToken(ctx context.Context, id uuid.UUID, name string, hash []byte, scopes []string) (APIToken, error) {
	v, err := one[APIToken](t.tx.Query(ctx, `
		INSERT INTO fleetwire.api_tokens (id, tenant_id, name, token_hash, scopes) VALUES ($1, $2, $3, $4, $5)
		RETURNING `+apiTokenCols, id, t.tenant, name, hash, scopes))
	if err != nil {
		return APIToken{}, fmt.Errorf("insert api token: %w", err)
	}
	return v, nil
}

// ListAPITokens pages through the current tenant's tokens.
func (t *Tx) ListAPITokens(ctx context.Context, after uuid.UUID, limit int) ([]APIToken, error) {
	v, err := many[APIToken](t.tx.Query(ctx, `
		SELECT `+apiTokenCols+` FROM fleetwire.api_tokens WHERE id > $1 ORDER BY id LIMIT $2`, after, limit))
	if err != nil {
		return nil, fmt.Errorf("list api tokens: %w", err)
	}
	return v, nil
}

// GetAPIToken returns one token of the current tenant.
func (t *Tx) GetAPIToken(ctx context.Context, id uuid.UUID) (APIToken, error) {
	v, err := one[APIToken](t.tx.Query(ctx, `SELECT `+apiTokenCols+` FROM fleetwire.api_tokens WHERE id = $1`, id))
	if err != nil {
		return APIToken{}, fmt.Errorf("get api token: %w", err)
	}
	return v, nil
}

// RevokeAPIToken revokes one token of the current tenant.
func (t *Tx) RevokeAPIToken(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := t.tx.Exec(ctx, `
		UPDATE fleetwire.api_tokens SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return false, fmt.Errorf("revoke api token: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// TouchAPIToken records that a token was used.
func (t *Tx) TouchAPIToken(ctx context.Context, id uuid.UUID) error {
	if _, err := t.tx.Exec(ctx, `UPDATE fleetwire.api_tokens SET last_used_at = now() WHERE id = $1`, id); err != nil {
		return fmt.Errorf("touch api token: %w", err)
	}
	return nil
}

// LookupAPIToken finds a tenant token by ID without a tenant context.
func (t *Tx) LookupAPIToken(ctx context.Context, id uuid.UUID) (APITokenAuth, error) {
	v, err := one[APITokenAuth](t.tx.Query(ctx, `SELECT * FROM fleetwire.api_token_lookup($1)`, id))
	if err != nil {
		return APITokenAuth{}, fmt.Errorf("lookup api token: %w", err)
	}
	return v, nil
}

// LookupOperatorToken finds an operator token by ID.
func (t *Tx) LookupOperatorToken(ctx context.Context, id uuid.UUID) (OperatorTokenAuth, error) {
	v, err := one[OperatorTokenAuth](t.tx.Query(ctx, `SELECT * FROM fleetwire.operator_token_lookup($1)`, id))
	if err != nil {
		return OperatorTokenAuth{}, fmt.Errorf("lookup operator token: %w", err)
	}
	return v, nil
}

// TouchOperatorToken records that an operator token was used.
func (t *Tx) TouchOperatorToken(ctx context.Context, id uuid.UUID) error {
	if _, err := t.tx.Exec(ctx, `SELECT fleetwire.touch_operator_token($1)`, id); err != nil {
		return fmt.Errorf("touch operator token: %w", err)
	}
	return nil
}

// CreateOperatorToken stores an operator token hash. Operator tokens are
// minted only with the owner's credentials, from the operator's shell; the
// runtime role has no privileges on the table, so a compromised service
// cannot create one.
func CreateOperatorToken(ctx context.Context, ownerURL string, id uuid.UUID, name string, hash []byte) error {
	conn, err := pgx.Connect(ctx, ownerURL)
	if err != nil {
		return fmt.Errorf("connect as owner: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `INSERT INTO fleetwire.operator_tokens (id, name, token_hash) VALUES ($1, $2, $3)`,
		id, name, hash); err != nil {
		return fmt.Errorf("insert operator token: %w", err)
	}
	return nil
}
