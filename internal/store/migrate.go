package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"

	// Registers the "pgx" database/sql driver used by goose.
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrations embed.FS

// MigrationResult describes one applied migration.
type MigrationResult struct {
	Version  int64
	Source   string
	Duration string
}

// Migrate applies every pending migration. ownerURL must be for the role
// that owns the schema (never the runtime role). An advisory lock makes
// concurrent runs from several replicas safe.
func Migrate(ctx context.Context, ownerURL string) ([]MigrationResult, error) {
	db, err := sql.Open("pgx", ownerURL)
	if err != nil {
		return nil, fmt.Errorf("open owner connection: %w", err)
	}
	defer db.Close()
	// goose keeps its version table inside the schema, so the schema has to
	// exist before goose starts.
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS fleetwire`); err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}
	st, err := database.NewStore(database.DialectPostgres, "fleetwire.goose_db_version")
	if err != nil {
		return nil, fmt.Errorf("goose store: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, fmt.Errorf("goose locker: %w", err)
	}
	fsys, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return nil, fmt.Errorf("migrations fs: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectCustom, db, fsys, goose.WithStore(st), goose.WithSessionLocker(locker))
	if err != nil {
		return nil, fmt.Errorf("goose provider: %w", err)
	}
	results, err := p.Up(ctx)
	if err != nil {
		return nil, fmt.Errorf("apply migrations: %w", err)
	}
	out := make([]MigrationResult, 0, len(results))
	for _, r := range results {
		out = append(out, MigrationResult{Version: r.Source.Version, Source: r.Source.Path, Duration: r.Duration.String()})
	}
	return out, nil
}
