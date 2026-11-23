package api

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store/storetest"
)

// TestQueuedTouchesAreRecordedAtShutdown: token use still queued when the
// recorder is told to stop, as for the last requests before a shutdown,
// is recorded before the recorder returns.
func TestQueuedTouchesAreRecordedAtShutdown(t *testing.T) {
	db := storetest.AppDB(t)
	s := New(Config{}, Deps{DB: db, Log: slog.New(slog.DiscardHandler), Registry: prometheus.NewRegistry()})
	tenant := uuid.Must(uuid.NewV7())
	ids := make([]uuid.UUID, 20)
	err := db.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		if _, err := tx.CreateTenant(ctx, "touch-"+tenant.String()[:8], 30); err != nil {
			return err
		}
		for i := range ids {
			ids[i] = uuid.Must(uuid.NewV7())
			if _, err := tx.InsertAPIToken(ctx, ids[i], "t", []byte{byte(i)}, []string{"audit:read"}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		s.touches <- &principal{tokenID: id, tenant: tenant}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s.runTouches(ctx)
	err = db.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		for i, id := range ids {
			tok, err := tx.GetAPIToken(ctx, id)
			if err != nil {
				return err
			}
			if tok.LastUsedAt == nil {
				t.Errorf("token %d: its use, queued at shutdown, was not recorded", i)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
