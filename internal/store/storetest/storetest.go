// Package storetest starts a real PostgreSQL for integration tests.
//
// One container is started per test binary and migrated once. Tests stay
// independent by creating their own tenants with random IDs. The container
// is removed by testcontainers' reaper when the test binary exits.
package storetest

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

// Image is the PostgreSQL version used in tests, Compose and the charm bundle.
const Image = "postgres:16-alpine"

// Env holds connection URLs for the three roles.
type Env struct {
	SuperURL string
	OwnerURL string
	AppURL   string
}

var (
	once   sync.Once
	env    *Env
	errEnv error
)

// Start returns the shared database, starting it on first use. The test is
// skipped when no container runtime is available.
func Start(t *testing.T) *Env {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	once.Do(func() { env, errEnv = start() })
	if errEnv != nil {
		t.Fatalf("start postgres: %v", errEnv)
	}
	return env
}

func start() (*Env, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := tcpostgres.Run(ctx, Image,
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, err
	}
	super, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, err
	}
	if err := Bootstrap(ctx, super); err != nil {
		return nil, err
	}
	e := &Env{
		SuperURL: super,
		OwnerURL: withUser(super, "fleetwire_owner", "owner", "fleetwire"),
		AppURL:   withUser(super, "fleetwire_app", "app", "fleetwire"),
	}
	if _, err := store.Migrate(ctx, e.OwnerURL); err != nil {
		return nil, err
	}
	return e, nil
}

// Bootstrap creates the two roles and the database the way a deployment
// does (deploy/compose/postgres/init.sql does the same).
func Bootstrap(ctx context.Context, superURL string) error {
	conn, err := pgx.Connect(ctx, superURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	for _, q := range []string{
		`CREATE ROLE fleetwire_owner LOGIN PASSWORD 'owner'`,
		`CREATE ROLE fleetwire_app LOGIN PASSWORD 'app' NOINHERIT NOBYPASSRLS`,
		`CREATE DATABASE fleetwire OWNER fleetwire_owner`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	return nil
}

func withUser(raw, user, pass, db string) string {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	u.User = url.UserPassword(user, pass)
	u.Path = "/" + db
	return u.String()
}

// AppDB connects as the runtime role and closes the pool at test end.
func AppDB(t *testing.T) *store.DB {
	t.Helper()
	e := Start(t)
	db, err := store.Connect(context.Background(), e.AppURL, store.Options{ApplicationName: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db
}
