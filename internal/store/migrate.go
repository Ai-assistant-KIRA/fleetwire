package store

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"

	// Registers the "pgx" database/sql driver used by goose.
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrations embed.FS

// DefaultRuntimeRole is the role the services run as in Compose and in the
// tests. Charmed PostgreSQL cannot create roles for an application, so
// there the runtime role is its charmed_dml group (ADR-0012).
const DefaultRuntimeRole = "fleetwire_app"

// runtimeRolePlaceholder marks, in the migration files, where the runtime
// role's name goes.
const runtimeRolePlaceholder = "{{runtime_role}}"

var roleName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// MigrateOptions tune Migrate.
type MigrateOptions struct {
	// RuntimeRole gets the row-level security policies and the least
	// privilege grants: the services' login role, or a group their login
	// role is a member of. Default DefaultRuntimeRole.
	RuntimeRole string
}

// MigrationResult describes one applied migration.
type MigrationResult struct {
	Version  int64
	Source   string
	Duration string
}

// Migrate applies every pending migration. ownerURL must be for the role
// that owns the schema (never the runtime role). An advisory lock makes
// concurrent runs from several replicas safe.
func Migrate(ctx context.Context, ownerURL string, opts MigrateOptions) ([]MigrationResult, error) {
	p, db, err := newProvider(ctx, ownerURL, opts)
	if err != nil {
		return nil, err
	}
	defer db.Close()
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

// newProvider returns a goose provider for the rendered migrations and the
// owner connection it uses, which the caller closes.
func newProvider(ctx context.Context, ownerURL string, opts MigrateOptions) (*goose.Provider, *sql.DB, error) {
	if opts.RuntimeRole == "" {
		opts.RuntimeRole = DefaultRuntimeRole
	}
	fsys, err := renderMigrations(opts.RuntimeRole)
	if err != nil {
		return nil, nil, err
	}
	db, err := sql.Open("pgx", ownerURL)
	if err != nil {
		return nil, nil, fmt.Errorf("open owner connection: %w", err)
	}
	p, err := func() (*goose.Provider, error) {
		// goose keeps its version table inside the schema, so the schema
		// has to exist before goose starts.
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
		p, err := goose.NewProvider(goose.DialectCustom, db, fsys, goose.WithStore(st), goose.WithSessionLocker(locker))
		if err != nil {
			return nil, fmt.Errorf("goose provider: %w", err)
		}
		return p, nil
	}()
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return p, db, nil
}

// renderMigrations returns the migration files with the runtime role filled
// in. The name is checked and quoted, so it can never change the statements
// around it.
func renderMigrations(role string) (fs.FS, error) {
	if !roleName.MatchString(role) {
		return nil, fmt.Errorf("runtime role %q: want a lowercase PostgreSQL identifier (a-z, 0-9, _)", role)
	}
	src, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return nil, fmt.Errorf("migrations fs: %w", err)
	}
	entries, err := fs.ReadDir(src, ".")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}
	out := renderedFS{}
	quoted := []byte(pgx.Identifier{role}.Sanitize())
	for _, e := range entries {
		b, err := fs.ReadFile(src, e.Name())
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		out[e.Name()] = bytes.ReplaceAll(b, []byte(runtimeRolePlaceholder), quoted)
	}
	return out, nil
}

// renderedFS is a flat, read-only, in-memory file system: just enough for
// goose to list and read the migrations.
type renderedFS map[string][]byte

func (r renderedFS) Open(name string) (fs.File, error) {
	if name == "." {
		return &memDir{fs: r}, nil
	}
	b, ok := r[name]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &memFile{name: name, Reader: bytes.NewReader(b), size: int64(len(b))}, nil
}

func (r renderedFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name != "." {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
	}
	return (&memDir{fs: r}).ReadDir(-1)
}

type memFile struct {
	*bytes.Reader
	name string
	size int64
}

func (f *memFile) Stat() (fs.FileInfo, error) { return memInfo{name: f.name, size: f.size}, nil }
func (f *memFile) Close() error               { return nil }

type memDir struct {
	fs   renderedFS
	read bool
}

func (d *memDir) Stat() (fs.FileInfo, error) { return memInfo{name: ".", dir: true}, nil }
func (d *memDir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: ".", Err: fs.ErrInvalid}
}
func (d *memDir) Close() error { return nil }

func (d *memDir) ReadDir(int) ([]fs.DirEntry, error) {
	if d.read {
		return nil, nil
	}
	d.read = true
	out := make([]fs.DirEntry, 0, len(d.fs))
	for name, b := range d.fs {
		out = append(out, fs.FileInfoToDirEntry(memInfo{name: name, size: int64(len(b))}))
	}
	// fs.ReadDir sorts by name; goose orders by version anyway.
	return out, nil
}

type memInfo struct {
	name string
	size int64
	dir  bool
}

func (i memInfo) Name() string { return i.name }
func (i memInfo) Size() int64  { return i.size }
func (i memInfo) Mode() fs.FileMode {
	if i.dir {
		return fs.ModeDir | 0o555
	}
	return 0o444
}
func (i memInfo) ModTime() time.Time { return time.Time{} }
func (i memInfo) IsDir() bool        { return i.dir }
func (i memInfo) Sys() any           { return nil }
