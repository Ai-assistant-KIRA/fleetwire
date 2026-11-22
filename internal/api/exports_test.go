package api_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

// readyExport creates an export of a device with a few readings and runs
// the job, returning the export ID and its file path.
func readyExport(t *testing.T, h *apitest.Harness, name string) (string, uuid.UUID, string) {
	t.Helper()
	tenant, tok := h.CreateTenant(name)
	dev := h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	storeReadings(t, h, tenant, dev.ID, 1, 5, func(uint64) time.Time { return time.Now() }, "cpu.utilization")
	var exp struct {
		ID uuid.UUID `json:"id"`
	}
	resp, body := h.Do("POST", "/v1/exports", tok, map[string]any{"device_id": dev.ID})
	h.MustJSON(resp, body, http.StatusAccepted, &exp)
	h.RunJobs()
	var path string
	err := h.DB.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		e, err := tx.GetExport(ctx, exp.ID)
		if err == nil && (e.Status != store.ExportReady || e.FilePath == nil) {
			t.Fatalf("export %+v is not ready", e)
		}
		if err == nil {
			path = *e.FilePath
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return tok, exp.ID, path
}

// The file stays on disk until retention runs, but past expires_at it is
// no longer offered.
func TestExpiredExportIsNotDownloadable(t *testing.T) {
	h := apitest.New(t)
	tok, id, _ := readyExport(t, h, "late")
	asOwner(t, h, `UPDATE fleetwire.exports SET expires_at = now() - interval '1 second' WHERE id = $1`, id)
	resp, body := h.Do("GET", "/v1/exports/"+id.String()+"/download", tok, nil)
	problem(t, resp, body, http.StatusGone, "expired")
}

// A symbolic link in the export directory must not lead outside it, even
// though the path in the database still looks right.
func TestExportDownloadStaysInsideTheExportDirectory(t *testing.T) {
	h := apitest.New(t)
	tok, id, path := readyExport(t, h, "escape")
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("not an export"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	resp, body := h.Do("GET", "/v1/exports/"+id.String()+"/download", tok, nil)
	problem(t, resp, body, http.StatusInternalServerError, "internal")
}

func TestActiveExportsPerTenantAreCapped(t *testing.T) {
	h := apitest.New(t, func(c *api.Config) { c.MaxActiveExports = 2 })
	_, tok := h.CreateTenant("busy-exporter")
	for i := 0; i < 2; i++ {
		resp, body := h.Do("POST", "/v1/exports", tok, nil)
		h.MustJSON(resp, body, http.StatusAccepted, nil)
	}
	resp, body := h.Do("POST", "/v1/exports", tok, nil)
	problem(t, resp, body, http.StatusConflict, "too_many_exports")
	// Another tenant has its own cap.
	_, other := h.CreateTenant("other-exporter")
	resp, body = h.Do("POST", "/v1/exports", other, nil)
	h.MustJSON(resp, body, http.StatusAccepted, nil)
	// Finished exports no longer count.
	h.RunJobs()
	resp, body = h.Do("POST", "/v1/exports", tok, nil)
	h.MustJSON(resp, body, http.StatusAccepted, nil)
}
