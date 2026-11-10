// Package store is the only code that talks SQL to PostgreSQL.
//
// Services connect as fleetwire_app, a role that owns nothing and cannot
// bypass row-level security. All tenant data is reached through
// DB.InTenantTx, which sets the tenant for that one transaction only
// (set_config with is_local = true), so the setting can never leak to the
// next user of a pooled connection.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a row does not exist or is not visible to the
// current tenant. The two cases are deliberately indistinguishable.
var ErrNotFound = errors.New("store: not found")

// DB is a pool of connections as the runtime role.
type DB struct {
	pool *pgxpool.Pool
}

// Options tune the pool.
type Options struct {
	MaxConns        int32
	ApplicationName string
	// Tracer, when set, traces every query (OpenTelemetry via otelpgx).
	Tracer pgx.QueryTracer
}

// Connect opens a pool. The URL must be for the runtime role.
func Connect(ctx context.Context, url string, opts Options) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if opts.MaxConns > 0 {
		cfg.MaxConns = opts.MaxConns
	}
	if opts.ApplicationName != "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = opts.ApplicationName
	}
	if opts.Tracer != nil {
		cfg.ConnConfig.Tracer = opts.Tracer
	}
	cfg.HealthCheckPeriod = 10 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	return &DB{pool: pool}, nil
}

// Close closes the pool.
func (db *DB) Close() { db.pool.Close() }

// Ping checks that a connection can be used.
func (db *DB) Ping(ctx context.Context) error {
	if err := db.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}
	return nil
}

// Tx is one database transaction.
type Tx struct {
	tx     pgx.Tx
	tenant uuid.UUID
}

// Tenant returns the tenant set on this transaction, or uuid.Nil.
func (t *Tx) Tenant() uuid.UUID { return t.tenant }

// SetTenant scopes the rest of the transaction to one tenant.
func (t *Tx) SetTenant(ctx context.Context, id uuid.UUID) error {
	if id == uuid.Nil {
		return errors.New("store: tenant id must not be nil")
	}
	if _, err := t.tx.Exec(ctx, sqlSetTenant, id.String()); err != nil {
		return fmt.Errorf("set tenant: %w", err)
	}
	t.tenant = id
	return nil
}

// InTx runs fn in a transaction without a tenant. Only the SECURITY DEFINER
// lookups and tenant-less audit entries work until SetTenant is called.
func (db *DB) InTx(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	err := pgx.BeginTxFunc(ctx, db.pool, pgx.TxOptions{}, func(ptx pgx.Tx) error {
		return fn(ctx, &Tx{tx: ptx})
	})
	if err != nil {
		return fmt.Errorf("transaction: %w", err)
	}
	return nil
}

// InTenantTx runs fn in a transaction scoped to tenant.
func (db *DB) InTenantTx(ctx context.Context, tenant uuid.UUID, fn func(ctx context.Context, tx *Tx) error) error {
	return db.InTx(ctx, func(ctx context.Context, tx *Tx) error {
		if err := tx.SetTenant(ctx, tenant); err != nil {
			return err
		}
		return fn(ctx, tx)
	})
}

// CurrentRole reports the role the pool connects as and whether it can
// bypass row-level security. Services check this at start and refuse to run
// as a privileged role.
func (db *DB) CurrentRole(ctx context.Context) (role string, bypassRLS, superuser bool, err error) {
	err = db.pool.QueryRow(ctx,
		`SELECT current_user, r.rolbypassrls, r.rolsuper FROM pg_roles r WHERE r.rolname = current_user`).
		Scan(&role, &bypassRLS, &superuser)
	if err != nil {
		return "", false, false, fmt.Errorf("read current role: %w", err)
	}
	return role, bypassRLS, superuser, nil
}

// CheckRuntimeRole fails if the pool's role could bypass row-level security
// or owns the fleetwire tables (R7.2).
func (db *DB) CheckRuntimeRole(ctx context.Context) error {
	role, bypass, super, err := db.CurrentRole(ctx)
	if err != nil {
		return err
	}
	if bypass || super {
		return fmt.Errorf("database role %q can bypass row-level security; connect as fleetwire_app", role)
	}
	var owns bool
	err = db.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'fleetwire' AND tableowner = current_user)`).Scan(&owns)
	if err != nil {
		return fmt.Errorf("check table ownership: %w", err)
	}
	if owns {
		return fmt.Errorf("database role %q owns the fleetwire tables; services must not run as the owner", role)
	}
	return nil
}

func one[T any](rows pgx.Rows, err error) (T, error) {
	var zero T
	if err != nil {
		return zero, err
	}
	v, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[T])
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, ErrNotFound
	}
	return v, err
}

func many[T any](rows pgx.Rows, err error) ([]T, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[T])
}
