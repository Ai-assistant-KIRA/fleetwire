package api_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
)

// asOwner runs one statement as the schema owner, which RLS does not limit.
func asOwner(t *testing.T, h *apitest.Harness, sql string, args ...any) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), h.Env.OwnerURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(t.Context())
	if _, err := conn.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func tokenWith(t *testing.T, h *apitest.Harness, admin string, scopes ...string) string {
	t.Helper()
	var out struct {
		Token string `json:"token"`
	}
	resp, body := h.Do("POST", "/v1/tokens", admin, map[string]any{"name": strings.Join(scopes, " "), "scopes": scopes})
	h.MustJSON(resp, body, http.StatusCreated, &out)
	return out.Token
}

// A job is readable with the read scope of what it works on, and its
// stored error text (SQL errors, file paths) never reaches the tenant.
func TestJobsNeedTheScopeOfTheirKindAndHideErrors(t *testing.T) {
	h := apitest.New(t)
	tenant, admin := h.CreateTenant("jobs")
	dev := h.Enroll(h.EnrollmentTokens(admin, 1)[0])
	storeReadings(t, h, tenant, dev.ID, 1, 5, func(uint64) time.Time { return time.Now() }, "cpu.utilization")
	var acc struct {
		StatusURL string `json:"status_url"`
	}
	resp, body := h.Do("DELETE", "/v1/devices/"+dev.ID.String(), admin, nil)
	h.MustJSON(resp, body, http.StatusAccepted, &acc)
	purge := strings.TrimPrefix(acc.StatusURL, "/v1/jobs/")
	var exp struct {
		ID uuid.UUID `json:"id"`
	}
	resp, body = h.Do("POST", "/v1/exports", admin, nil)
	h.MustJSON(resp, body, http.StatusAccepted, &exp)
	exportJob := uuid.NewString()
	asOwner(t, h, `UPDATE fleetwire.jobs SET id = $1 WHERE kind = 'export' AND payload->>'export_id' = $2`, exportJob, exp.ID.String())
	const secret = `pq: relation "fleetwire.chunks" /var/lib/fleetwire/exports/x.tmp`
	asOwner(t, h, `UPDATE fleetwire.jobs SET last_error = $1, attempts = 1 WHERE id IN ($2, $3)`, secret, purge, exportJob)

	devices := tokenWith(t, h, admin, "devices:read")
	exports := tokenWith(t, h, admin, "exports:read")
	for _, c := range []struct {
		job, token string
		status     int
	}{
		{purge, devices, http.StatusOK},
		{purge, exports, http.StatusForbidden},
		{exportJob, exports, http.StatusOK},
		{exportJob, devices, http.StatusForbidden},
	} {
		resp, body := h.Do("GET", "/v1/jobs/"+c.job, c.token, nil)
		if resp.StatusCode != c.status {
			t.Errorf("GET job %s: %d %s, want %d", c.job, resp.StatusCode, body, c.status)
			continue
		}
		if c.status != http.StatusOK {
			continue
		}
		var job struct {
			LastError string `json:"last_error"`
		}
		h.MustJSON(resp, body, http.StatusOK, &job)
		if job.LastError == "" || strings.Contains(string(body), "fleetwire.chunks") || strings.Contains(string(body), "/var/lib") {
			t.Errorf("job %s: last_error %q, want a fixed message without internal detail", c.job, job.LastError)
		}
	}

	asOwner(t, h, `UPDATE fleetwire.exports SET status = 'failed', error = $1 WHERE id = $2`, secret, exp.ID)
	var failed struct {
		Error string `json:"error"`
	}
	resp, body = h.Do("GET", "/v1/exports/"+exp.ID.String(), exports, nil)
	h.MustJSON(resp, body, http.StatusOK, &failed)
	if failed.Error == "" || strings.Contains(string(body), "/var/lib") {
		t.Errorf("failed export error %q, want a fixed message without internal detail", failed.Error)
	}
}
