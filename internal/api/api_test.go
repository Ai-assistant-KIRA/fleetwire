package api_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/chunks"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

// storeReadings writes n readings for seqs lo.. as one chunk, the way
// ingest does, so the API tests do not depend on the pipeline.
func storeReadings(t *testing.T, h *apitest.Harness, tenant, device uuid.UUID, lo uint64, n int, at func(seq uint64) time.Time, metric string) {
	t.Helper()
	keyID, dek, err := h.Keys.SealingKey(t.Context(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	var rs []*fleetwirev1.StoredReading
	for i := 0; i < n; i++ {
		seq := lo + uint64(i)
		rs = append(rs, &fleetwirev1.StoredReading{Seq: seq, CollectedUnixNano: at(seq).UnixNano(),
			Collector: "test", Metric: metric, Value: float64(seq)})
	}
	c, err := chunks.Seal(dek, tenant, device, keyID, rs)
	if err != nil {
		t.Fatal(err)
	}
	err = h.DB.IngestBatch(t.Context(), device, tenant, func(store.IngestState) (store.IngestWrite, error) {
		return store.IngestWrite{Chunks: []store.Chunk{c}, UpdateProgress: true, AckedThrough: c.SeqHi, BatchAt: time.Now()}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func problem(t *testing.T, resp *apitest.Response, body []byte, status int, code string) {
	t.Helper()
	if resp.StatusCode != status {
		t.Fatalf("status %d, want %d: %s", resp.StatusCode, status, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type %q, want application/problem+json", ct)
	}
	var p struct {
		Type, Title, Code, Instance string
		Status                      int
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Status != status || p.Code != code || p.Type == "" || p.Title == "" || !strings.HasPrefix(p.Instance, "urn:fleetwire:request:") {
		t.Fatalf("problem = %+v, want status %d code %s", p, status, code)
	}
}

func TestEnrollRenewRevoke(t *testing.T) {
	h := apitest.New(t)
	_, tok := h.CreateTenant("acme")
	enroll := h.EnrollmentTokens(tok, 1)[0]
	dev := h.Enroll(enroll)

	leaf, err := x509.ParseCertificate(dev.TLS.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if id, err := pki.DeviceID(leaf); err != nil || id != dev.ID {
		t.Fatalf("certificate identity %s, %v; want %s", id, err, dev.ID)
	}

	// The token is single use, and the refusal is audited.
	key, _ := pki.NewDeviceKey()
	csr, _ := pki.NewCSR(key, "x")
	resp, body := h.DoDevice(nil, "POST", "/v1/enroll", map[string]any{"token": enroll, "csr_pem": string(csr)})
	problem(t, resp, body, http.StatusUnauthorized, "invalid_token")
	resp, body = h.DoDevice(nil, "POST", "/v1/enroll", map[string]any{"token": "fwe_nonsense", "csr_pem": string(csr)})
	problem(t, resp, body, http.StatusUnauthorized, "invalid_token")
	var audit struct {
		Items []struct{ Action, Result string } `json:"items"`
	}
	resp, body = h.Do("GET", "/v1/audit", tok, nil)
	h.MustJSON(resp, body, http.StatusOK, &audit)
	var denied, enrolled bool
	for _, a := range audit.Items {
		denied = denied || (a.Action == "device.enroll" && a.Result == "denied")
		enrolled = enrolled || (a.Action == "device.enroll" && a.Result == "success")
	}
	if !denied || !enrolled {
		t.Fatalf("audit log misses enrollment entries: %+v", audit.Items)
	}

	// Renewal over mTLS keeps the key and returns a new certificate.
	csr2, _ := pki.NewCSR(dev.Key, "renew")
	var renewed struct {
		CertificatePEM string `json:"certificate_pem"`
	}
	resp, body = h.DoDevice(&dev.TLS, "POST", "/v1/renew", map[string]any{"csr_pem": string(csr2)})
	h.MustJSON(resp, body, http.StatusOK, &renewed)
	b, _ := pem.Decode([]byte(renewed.CertificatePEM))
	newLeaf, err := x509.ParseCertificate(b.Bytes)
	if err != nil || newLeaf.SerialNumber.Cmp(leaf.SerialNumber) == 0 || newLeaf.Subject.CommonName != dev.ID.String() {
		t.Fatalf("renewed cert %v serial %v", err, newLeaf.SerialNumber)
	}
	// A renewal with another key is refused; so is one without a certificate.
	other, _ := pki.NewDeviceKey()
	csr3, _ := pki.NewCSR(other, "swap")
	resp, body = h.DoDevice(&dev.TLS, "POST", "/v1/renew", map[string]any{"csr_pem": string(csr3)})
	problem(t, resp, body, http.StatusBadRequest, "invalid_request")
	resp, body = h.DoDevice(nil, "POST", "/v1/renew", map[string]any{"csr_pem": string(csr2)})
	problem(t, resp, body, http.StatusUnauthorized, "client_certificate_required")

	// Revoke: the device can no longer renew, and both its certificates
	// are on the CRL.
	resp, body = h.Do("POST", "/v1/devices/"+dev.ID.String()+"/revoke", tok, nil)
	h.MustJSON(resp, body, http.StatusOK, nil)
	resp, body = h.DoDevice(&dev.TLS, "POST", "/v1/renew", map[string]any{"csr_pem": string(csr2)})
	problem(t, resp, body, http.StatusForbidden, "device_inactive")
	if err := h.CRL.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	var revoked []store.RevokedCert
	err = h.DB.InTx(t.Context(), func(ctx context.Context, tx *store.Tx) error {
		var err error
		revoked, err = tx.RevokedCerts(ctx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, r := range revoked {
		found[r.Serial] = true
	}
	if !found[pki.SerialString(leaf.SerialNumber)] || !found[pki.SerialString(newLeaf.SerialNumber)] {
		t.Fatalf("revoked serials %v miss the device certificates", found)
	}
}

func TestErrorsAreProblemDetails(t *testing.T) {
	h := apitest.New(t)
	_, tok := h.CreateTenant("errs")
	resp, body := h.Do("GET", "/v1/nothing-here", tok, nil)
	problem(t, resp, body, http.StatusNotFound, "not_found")
	resp, body = h.Do("PUT", "/v1/devices", tok, nil)
	problem(t, resp, body, http.StatusMethodNotAllowed, "method_not_allowed")
	if !strings.Contains(resp.Header.Get("Allow"), "GET") {
		t.Fatalf("Allow header %q", resp.Header.Get("Allow"))
	}
	resp, body = h.Do("GET", "/v1/devices", "", nil)
	problem(t, resp, body, http.StatusUnauthorized, "unauthorized")
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("missing WWW-Authenticate")
	}
	resp, body = h.Do("GET", "/v1/devices", "fwt_00000000000000000000000000000000_"+strings.Repeat("a", 52), nil)
	problem(t, resp, body, http.StatusUnauthorized, "unauthorized")
	resp, body = h.Do("PATCH", "/v1/tenant", tok, map[string]any{"retention_days": 0})
	problem(t, resp, body, http.StatusBadRequest, "invalid_request")
	resp, body = h.Do("GET", "/v1/devices/not-a-uuid", tok, nil)
	problem(t, resp, body, http.StatusBadRequest, "invalid_request")
	resp, body = h.Do("GET", "/v1/devices?cursor=forged.cursor", tok, nil)
	problem(t, resp, body, http.StatusBadRequest, "invalid_request")
}

func TestRateLimit(t *testing.T) {
	h := apitest.New(t, func(c *api.Config) { c.RatePerSecond, c.RateBurst = 0.5, 2 })
	_, tok := h.CreateTenant("busy")
	var got429 *apitest.Response
	var body []byte
	for i := 0; i < 5 && got429 == nil; i++ {
		resp, b := h.Do("GET", "/v1/tenant", tok, nil)
		if resp.StatusCode == http.StatusTooManyRequests {
			got429, body = resp, b
		}
	}
	if got429 == nil {
		t.Fatal("no 429 after exceeding the burst")
	}
	problem(t, got429, body, http.StatusTooManyRequests, "rate_limited")
	if ra := got429.Header.Get("Retry-After"); ra == "" || ra == "0" {
		t.Fatalf("Retry-After = %q", ra)
	}
}

func TestScopesAndTokenKinds(t *testing.T) {
	h := apitest.New(t)
	tenant, admin := h.CreateTenant("scoped")
	var narrow struct {
		ID    uuid.UUID `json:"id"`
		Token string    `json:"token"`
	}
	resp, body := h.Do("POST", "/v1/tokens", admin, map[string]any{"name": "reader", "scopes": []string{"readings:read"}})
	h.MustJSON(resp, body, http.StatusCreated, &narrow)
	resp, body = h.Do("GET", "/v1/devices", narrow.Token, nil)
	problem(t, resp, body, http.StatusForbidden, "forbidden")
	resp, body = h.Do("GET", "/v1/admin/tenants", admin, nil)
	problem(t, resp, body, http.StatusForbidden, "forbidden")
	resp, body = h.Do("GET", "/v1/devices", h.OperatorToken, nil)
	problem(t, resp, body, http.StatusForbidden, "forbidden")
	// The token list never shows secrets; a revoked token stops working.
	resp, body = h.Do("GET", "/v1/tokens", admin, nil)
	h.MustJSON(resp, body, http.StatusOK, nil)
	if bytes.Contains(body, []byte("fwt_")) || bytes.Contains(body, []byte("hash")) {
		t.Fatalf("token list leaks secrets: %s", body)
	}
	resp, body = h.Do("DELETE", "/v1/tokens/"+narrow.ID.String(), admin, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("revoke: %d %s", resp.StatusCode, body)
	}
	resp, body = h.Do("GET", "/v1/devices/"+tenant.String()+"/readings", narrow.Token, nil)
	problem(t, resp, body, http.StatusUnauthorized, "unauthorized")
}

// TestTenantIsolationThroughAPI is P7 through the API: tenant B's token
// cannot see or change anything of tenant A through any endpoint.
func TestTenantIsolationThroughAPI(t *testing.T) {
	h := apitest.New(t)
	tenantA, tokA := h.CreateTenant("tenant-a")
	_, tokB := h.CreateTenant("tenant-b")
	devA := h.Enroll(h.EnrollmentTokens(tokA, 1)[0])
	storeReadings(t, h, tenantA, devA.ID, 1, 5, func(uint64) time.Time { return time.Now() }, "cpu.utilization")
	var exp struct {
		ID uuid.UUID `json:"id"`
	}
	resp, body := h.Do("POST", "/v1/exports", tokA, map[string]any{"device_id": devA.ID})
	h.MustJSON(resp, body, http.StatusAccepted, &exp)
	var tokenList struct {
		Items []struct {
			ID uuid.UUID `json:"id"`
		} `json:"items"`
	}
	resp, body = h.Do("GET", "/v1/tokens", tokA, nil)
	h.MustJSON(resp, body, http.StatusOK, &tokenList)

	d := "/v1/devices/" + devA.ID.String()
	for _, c := range []struct{ method, path string }{
		{"GET", d}, {"POST", d + "/revoke"}, {"DELETE", d}, {"GET", d + "/readings"}, {"GET", d + "/gaps"},
		{"GET", "/v1/exports/" + exp.ID.String()}, {"GET", "/v1/exports/" + exp.ID.String() + "/download"},
		{"GET", "/v1/tokens/" + tokenList.Items[0].ID.String()}, {"DELETE", "/v1/tokens/" + tokenList.Items[0].ID.String()},
	} {
		resp, body := h.Do(c.method, c.path, tokB, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s %s with tenant B's token: %d %s, want 404", c.method, c.path, resp.StatusCode, body)
		}
	}
	resp, body = h.Do("POST", "/v1/exports", tokB, map[string]any{"device_id": devA.ID})
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("tenant B exporting tenant A's device: %d %s", resp.StatusCode, body)
	}
	for _, path := range []string{"/v1/devices", "/v1/audit", "/v1/tokens"} {
		_, body := h.Do("GET", path, tokB, nil)
		if bytes.Contains(body, []byte(devA.ID.String())) || bytes.Contains(body, []byte(tenantA.String())) {
			t.Errorf("GET %s with tenant B's token mentions tenant A: %s", path, body)
		}
	}
	// A's device is untouched.
	var dev struct{ Status string }
	resp, body = h.Do("GET", d, tokA, nil)
	h.MustJSON(resp, body, http.StatusOK, &dev)
	if dev.Status != "active" {
		t.Fatalf("tenant A's device is %s", dev.Status)
	}
}

func TestReadingsPagingAndFilters(t *testing.T) {
	h := apitest.New(t)
	tenant, tok := h.CreateTenant("readings")
	dev := h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	at := func(seq uint64) time.Time { return base.Add(time.Duration(seq) * time.Second) }
	storeReadings(t, h, tenant, dev.ID, 1, 10, at, "cpu.utilization")
	storeReadings(t, h, tenant, dev.ID, 11, 10, at, "memory.used_bytes")
	storeReadings(t, h, tenant, dev.ID, 21, 10, at, "cpu.utilization")

	var seqs []uint64
	path := "/v1/devices/" + dev.ID.String() + "/readings?limit=7"
	for path != "" {
		var pg struct {
			Items []struct {
				Seq uint64 `json:"seq"`
			} `json:"items"`
			NextCursor string `json:"next_cursor"`
		}
		resp, body := h.Do("GET", path, tok, nil)
		h.MustJSON(resp, body, http.StatusOK, &pg)
		for _, it := range pg.Items {
			seqs = append(seqs, it.Seq)
		}
		path = ""
		if pg.NextCursor != "" {
			path = "/v1/devices/" + dev.ID.String() + "/readings?limit=7&cursor=" + pg.NextCursor
		}
	}
	if len(seqs) != 30 {
		t.Fatalf("paged through %d readings, want 30", len(seqs))
	}
	for i, s := range seqs {
		if s != uint64(i)+1 {
			t.Fatalf("readings out of order: %v", seqs)
		}
	}
	var pg struct {
		Items []struct {
			Seq    uint64 `json:"seq"`
			Metric string `json:"metric"`
		} `json:"items"`
	}
	from := at(15).UTC().Format(time.RFC3339)
	resp, body := h.Do("GET", "/v1/devices/"+dev.ID.String()+"/readings?metric=cpu.utilization&from="+from, tok, nil)
	h.MustJSON(resp, body, http.StatusOK, &pg)
	if len(pg.Items) != 10 || pg.Items[0].Seq != 21 {
		t.Fatalf("filtered readings = %+v", pg.Items)
	}
}

func TestExportDownloadAndRetention(t *testing.T) {
	h := apitest.New(t)
	tenant, tok := h.CreateTenant("exporter")
	dev := h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	old := time.Now().Add(-72 * time.Hour)
	storeReadings(t, h, tenant, dev.ID, 1, 20, func(uint64) time.Time { return old }, "cpu.utilization")
	storeReadings(t, h, tenant, dev.ID, 21, 30, func(uint64) time.Time { return time.Now() }, "cpu.utilization")

	var exp struct {
		ID          uuid.UUID `json:"id"`
		Status      string    `json:"status"`
		SHA256      string    `json:"sha256"`
		DownloadURL string    `json:"download_url"`
	}
	resp, body := h.Do("POST", "/v1/exports", tok, nil)
	h.MustJSON(resp, body, http.StatusAccepted, &exp)
	h.RunJobs()
	resp, body = h.Do("GET", "/v1/exports/"+exp.ID.String(), tok, nil)
	h.MustJSON(resp, body, http.StatusOK, &exp)
	if exp.Status != "ready" || exp.DownloadURL == "" {
		t.Fatalf("export = %+v", exp)
	}
	resp, file := h.Do("GET", exp.DownloadURL, tok, nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/gzip" {
		t.Fatalf("download: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	sum := sha256.Sum256(file)
	if want := "sha-256=:" + base64.StdEncoding.EncodeToString(sum[:]) + ":"; resp.Header.Get("Content-Digest") != want {
		t.Fatalf("Content-Digest %q, want %q", resp.Header.Get("Content-Digest"), want)
	}
	if fmt.Sprintf("%x", sum) != exp.SHA256 {
		t.Fatal("sha256 in the export record does not match the file")
	}
	zr, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(plain), "\n"); lines != 50 {
		t.Fatalf("export has %d lines, want 50", lines)
	}

	// Retention of 1 day removes the 72-hour-old chunk and keeps the new one.
	resp, body = h.Do("PATCH", "/v1/tenant", tok, map[string]any{"retention_days": 1})
	h.MustJSON(resp, body, http.StatusOK, nil)
	if err := h.Worker.ScheduleRetention(t.Context()); err != nil {
		t.Fatal(err)
	}
	h.RunJobs()
	err = h.DB.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		list, err := tx.ChunksBySeq(ctx, dev.ID, 0, 100)
		if err != nil {
			return err
		}
		if len(list) != 1 || list[0].SeqLo != 21 {
			t.Errorf("chunks after retention: %d, first %d", len(list), list[0].SeqLo)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteDevicePurgesData(t *testing.T) {
	h := apitest.New(t)
	tenant, tok := h.CreateTenant("purge")
	dev := h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	storeReadings(t, h, tenant, dev.ID, 1, 10, func(uint64) time.Time { return time.Now() }, "cpu.utilization")
	var acc struct {
		JobID     uuid.UUID `json:"job_id"`
		StatusURL string    `json:"status_url"`
	}
	resp, body := h.Do("DELETE", "/v1/devices/"+dev.ID.String(), tok, nil)
	h.MustJSON(resp, body, http.StatusAccepted, &acc)
	h.RunJobs()
	var job struct{ Status string }
	resp, body = h.Do("GET", acc.StatusURL, tok, nil)
	h.MustJSON(resp, body, http.StatusOK, &job)
	var d struct{ Status string }
	resp, body = h.Do("GET", "/v1/devices/"+dev.ID.String(), tok, nil)
	h.MustJSON(resp, body, http.StatusOK, &d)
	if job.Status != "succeeded" || d.Status != "deleted" {
		t.Fatalf("job %s, device %s", job.Status, d.Status)
	}
	resp, body = h.Do("GET", "/v1/devices/"+dev.ID.String()+"/readings", tok, nil)
	var pg struct{ Items []any }
	h.MustJSON(resp, body, http.StatusOK, &pg)
	if len(pg.Items) != 0 {
		t.Fatalf("%d readings left after purge", len(pg.Items))
	}
}

// TestDestroyTenantMakesOldCopiesUnreadable copies a chunk and its wrapped
// key out of the database before the destroy, the way a backup would, and
// checks that the copy cannot be decrypted afterwards.
func TestDestroyTenantMakesOldCopiesUnreadable(t *testing.T) {
	h := apitest.New(t)
	tenant, tok := h.CreateTenant("doomed")
	dev := h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	storeReadings(t, h, tenant, dev.ID, 1, 10, func(uint64) time.Time { return time.Now() }, "cpu.utilization")
	var backupChunk store.Chunk
	var backupKey store.TenantKey
	err := h.DB.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		list, err := tx.ChunksBySeq(ctx, dev.ID, 0, 1)
		if err != nil {
			return err
		}
		backupChunk = list[0]
		backupKey, err = tx.TenantKey(ctx, backupChunk.KeyID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	// Sanity: the backup is readable before the destroy.
	dek, err := h.Keyring.Unwrap(t.Context(), tenant, backupKey.KEKID, backupKey.WrappedDEK)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chunks.Open(dek, backupChunk); err != nil {
		t.Fatal(err)
	}

	resp, body := h.Do("DELETE", "/v1/admin/tenants/"+tenant.String(), h.OperatorToken, nil)
	h.MustJSON(resp, body, http.StatusAccepted, nil)
	resp, body = h.Do("GET", "/v1/tenant", tok, nil)
	problem(t, resp, body, http.StatusUnauthorized, "unauthorized")
	h.RunJobs()

	if _, err := h.Keyring.Unwrap(t.Context(), tenant, backupKey.KEKID, backupKey.WrappedDEK); !errors.Is(err, keyring.ErrNotFound) {
		t.Fatalf("unwrap of a pre-destroy copy: %v, want keyring.ErrNotFound", err)
	}
	var list struct {
		Items []struct {
			ID     uuid.UUID `json:"id"`
			Status string    `json:"status"`
		} `json:"items"`
	}
	resp, body = h.Do("GET", "/v1/admin/tenants?limit=1000", h.OperatorToken, nil)
	h.MustJSON(resp, body, http.StatusOK, &list)
	for _, it := range list.Items {
		if it.ID == tenant && it.Status != "destroyed" {
			t.Fatalf("tenant status %s, want destroyed", it.Status)
		}
	}
	err = h.DB.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		left, err := tx.ChunksBySeq(ctx, dev.ID, 0, 10)
		if len(left) != 0 {
			t.Errorf("%d chunks left after destroy", len(left))
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	// The audit log survives as the tombstone.
	resp, body = h.Do("GET", "/v1/admin/audit?limit=1000", h.OperatorToken, nil)
	h.MustJSON(resp, body, http.StatusOK, nil)
	if !bytes.Contains(body, []byte("tenant.destroyed")) {
		t.Fatal("no tenant.destroyed audit entry")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
