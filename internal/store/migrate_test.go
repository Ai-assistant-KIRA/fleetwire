package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store/storetest"
)

// releasedVersion is the newest migration shipped in a release (v0.1.1).
// Databases in the field are at this version or newer.
const releasedVersion = 4

// freshDatabase creates an empty database owned by the schema owner and
// returns owner and runtime-role URLs for it. It is dropped at test end.
func freshDatabase(t *testing.T) (ownerURL, appURL string) {
	t.Helper()
	ctx := t.Context()
	e := storetest.Start(t)
	super, err := pgx.Connect(ctx, e.SuperURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = super.Close(context.WithoutCancel(ctx)) }()
	name := "fleetwire_mig_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err := super.Exec(ctx, "CREATE DATABASE "+name+" OWNER fleetwire_owner"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		c, err := pgx.Connect(ctx, e.SuperURL)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = c.Close(ctx) }()
		if _, err := c.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	return withRole(t, e.OwnerURL, "fleetwire_owner", "owner", name), withRole(t, e.OwnerURL, "fleetwire_app", "app", name)
}

// TestMigrationsGoDownAndUpAgain applies every migration to an empty
// database, rolls the unreleased ones back to the released schema, and
// applies them again: every Down section must undo its Up.
func TestMigrationsGoDownAndUpAgain(t *testing.T) {
	owner, app := freshDatabase(t)
	ctx := t.Context()
	if _, err := store.Migrate(ctx, owner, store.MigrateOptions{}); err != nil {
		t.Fatalf("migrate an empty database: %v", err)
	}
	if err := store.MigrateTo(ctx, owner, store.MigrateOptions{}, releasedVersion); err != nil {
		t.Fatalf("roll back to the released schema: %v", err)
	}
	if _, err := store.Migrate(ctx, owner, store.MigrateOptions{}); err != nil {
		t.Fatalf("migrate again: %v", err)
	}
	db, err := store.Connect(ctx, app, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CheckRuntimeRole(ctx); err != nil {
		t.Fatal(err)
	}
	newFixture(t, db)
}

// upgraded creates a database at the released schema, runs setup on it as
// the owner, and migrates it to the newest schema. It returns the runtime
// role's pool.
func upgraded(t *testing.T, setup func(ctx context.Context, conn *pgx.Conn) error) *store.DB {
	t.Helper()
	owner, app := freshDatabase(t)
	ctx := t.Context()
	if err := store.MigrateTo(ctx, owner, store.MigrateOptions{}, releasedVersion); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := setup(ctx, conn); err != nil {
		t.Fatalf("setup on the released schema: %v", err)
	}
	_ = conn.Close(ctx)
	if _, err := store.Migrate(ctx, owner, store.MigrateOptions{}); err != nil {
		t.Fatalf("upgrade from the released schema: %v", err)
	}
	db, err := store.Connect(ctx, app, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db
}

// TestUpgradeFreesKeysOfFailedJobs: a job that failed for good before the
// upgrade gives up its dedupe key, and a queued one keeps it.
func TestUpgradeFreesKeysOfFailedJobs(t *testing.T) {
	tenant, failed, queued := uuid.New(), uuid.New(), uuid.New()
	db := upgraded(t, func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, `INSERT INTO fleetwire.tenants (id, name) VALUES ($1, 't')`, tenant); err != nil {
			return err
		}
		_, err := conn.Exec(ctx, `
			INSERT INTO fleetwire.jobs (id, tenant_id, kind, status, dedupe_key, finished_at)
			VALUES ($2, $1, 'tenant_destroy', 'failed', $4, now()),
			       ($3, $1, 'device_purge', 'queued', 'device_purge:x', NULL)`,
			tenant, failed, queued, "tenant_destroy:"+tenant.String())
		return err
	})
	err := db.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		fresh := uuid.New()
		got, err := tx.EnqueueJob(ctx, fresh, store.JobTenantDestroy, nil, "tenant_destroy:"+tenant.String(), time.Now())
		if err != nil {
			return err
		}
		if got != fresh {
			t.Errorf("destroy after a failed one: got job %s, want the new %s", got, fresh)
		}
		if got, err = tx.EnqueueJob(ctx, uuid.New(), store.JobDevicePurge, nil, "device_purge:x", time.Now()); err != nil {
			return err
		}
		if got != queued {
			t.Errorf("purge next to a queued one: got job %s, want the queued %s", got, queued)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
