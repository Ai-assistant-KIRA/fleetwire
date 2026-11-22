package store_test

import (
	"context"
	"strings"
	"testing"

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
