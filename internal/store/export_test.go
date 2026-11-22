package store

import (
	"context"
	"fmt"
)

// MigrateTo moves the schema up or down to version, for the migration tests.
func MigrateTo(ctx context.Context, ownerURL string, opts MigrateOptions, version int64) error {
	p, db, err := newProvider(ctx, ownerURL, opts)
	if err != nil {
		return err
	}
	defer db.Close()
	current, err := p.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("schema version: %w", err)
	}
	if version >= current {
		_, err = p.UpTo(ctx, version)
	} else {
		_, err = p.DownTo(ctx, version)
	}
	if err != nil {
		return fmt.Errorf("migrate to %d: %w", version, err)
	}
	return nil
}
