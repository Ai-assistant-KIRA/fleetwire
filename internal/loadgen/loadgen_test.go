package loadgen

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// A device that cannot be opened must not vanish from the run: it is
// counted, served at /status, and Run fails after writing the results of
// the devices that did run.
func TestDevicesThatDoNotStartFailTheRun(t *testing.T) {
	dir := t.TempDir()
	// No enrollment token can be fetched: the API is not listening, and
	// the CA bundle holds no certificate.
	r := New(Options{Devices: 3, StateDir: dir, RampPerSec: 1000, Duration: 200 * time.Millisecond,
		APIURL: "https://127.0.0.1:1", CABundle: []byte("not a certificate")},
		slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	err := r.Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "3 of 3 devices did not start") {
		t.Fatalf("Run() = %v, want an error about the devices that did not start", err)
	}
	if got := testutil.ToFloat64(r.m.OpenFailed); got != 3 {
		t.Fatalf("open failures metric = %v, want 3", got)
	}
	rs, rerr := ReadResults(filepath.Join(dir, "results.jsonl"))
	if rerr != nil || len(rs) != 0 {
		t.Fatalf("results.jsonl: %v, %v; want it written and empty", rs, rerr)
	}

	rec := httptest.NewRecorder()
	r.ServeStatus(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	var st Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st != (Stats{Devices: 3, OpenFailures: 3}) {
		t.Fatalf("/status = %+v", st)
	}
}
