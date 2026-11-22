package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
)

// fwctl drives a real API (real PostgreSQL, real TLS).
func TestFwctlAgainstRealAPI(t *testing.T) {
	h := apitest.New(t)
	env := map[string]string{
		"FWCTL_URL":     h.Public.URL,
		"FWCTL_CA_FILE": filepath.Join(h.PKIDir, devpki.CABundle),
	}
	fw := func(token string, args ...string) (string, error) {
		t.Helper()
		var out, errOut bytes.Buffer
		getenv := func(k string) string {
			if k == "FWCTL_TOKEN" {
				return token
			}
			return env[k]
		}
		err := run(t.Context(), args, getenv, &out, &errOut)
		return out.String(), err
	}
	mustJSON := func(token string, v any, args ...string) {
		t.Helper()
		out, err := fw(token, append([]string{"-o", "json"}, args...)...)
		if err != nil {
			t.Fatalf("fwctl %v: %v", args, err)
		}
		if err := json.Unmarshal([]byte(out), v); err != nil {
			t.Fatalf("fwctl %v output %q: %v", args, out, err)
		}
	}

	var tenant struct{ ID string }
	mustJSON(h.OperatorToken, &tenant, "tenant", "create", "-name", "cli-co", "-retention-days", "7")
	var tok struct{ Token string }
	mustJSON(h.OperatorToken, &tok, "token", "create", "-tenant", tenant.ID, "-name", "admin",
		"-scopes", "tenant:admin,devices:read,devices:admin,readings:read,exports:read,exports:write,audit:read")

	out, err := fw(tok.Token, "tenant", "show")
	if err != nil || !strings.Contains(out, "cli-co") || !strings.Contains(out, "RETENTION_DAYS") {
		t.Fatalf("tenant show: %q, %v", out, err)
	}
	var enroll []struct{ Token string }
	mustJSON(tok.Token, &enroll, "enroll-token", "create", "-count", "2")
	if len(enroll) != 2 || !strings.HasPrefix(enroll[0].Token, "fwe_") {
		t.Fatalf("enrollment tokens = %+v", enroll)
	}
	dev := h.Enroll(enroll[0].Token)

	out, err = fw(tok.Token, "device", "list")
	if err != nil || !strings.Contains(out, dev.ID.String()) {
		t.Fatalf("device list: %q, %v", out, err)
	}
	var shown struct {
		ID     string
		Status string
	}
	mustJSON(tok.Token, &shown, "device", "show", dev.ID.String())
	if shown.ID != dev.ID.String() || shown.Status != "active" {
		t.Fatalf("device show = %+v", shown)
	}
	// Flags may follow the positional argument.
	if _, err := fw(tok.Token, "device", "delete", dev.ID.String()); err == nil || !strings.Contains(err.Error(), "-yes") {
		t.Fatalf("delete without -yes: %v", err)
	}
	if _, err := fw(tok.Token, "device", "revoke", dev.ID.String()); err != nil {
		t.Fatal(err)
	}

	var exp struct{ ID, Status string }
	mustJSON(tok.Token, &exp, "export", "create")
	h.RunJobs()
	mustJSON(tok.Token, &exp, "export", "status", exp.ID)
	if exp.Status != "ready" {
		t.Fatalf("export status %s", exp.Status)
	}
	dest := filepath.Join(t.TempDir(), "export.ndjson.gz")
	out, err = fw(tok.Token, "export", "download", exp.ID, "-out", dest)
	if err != nil || !strings.Contains(out, "sha-256 verified") {
		t.Fatalf("export download: %q, %v", out, err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatal(err)
	}

	out, err = fw(tok.Token, "audit", "list")
	if err != nil || !strings.Contains(out, "device.revoke") || !strings.Contains(out, "export.create") {
		t.Fatalf("audit list: %q, %v", out, err)
	}
	out, err = fw(h.OperatorToken, "tenant", "list")
	if err != nil || !strings.Contains(out, tenant.ID) {
		t.Fatalf("tenant list: %q, %v", out, err)
	}

	// API errors come back as readable problem details.
	_, err = fw(tok.Token, "device", "show", "00000000-0000-0000-0000-000000000000")
	if err == nil || !strings.Contains(err.Error(), "404 not_found") {
		t.Fatalf("missing device: %v", err)
	}
	_, err = fw(tok.Token, "tenant", "list")
	if err == nil || !strings.Contains(err.Error(), "403 forbidden") {
		t.Fatalf("tenant token on operator endpoint: %v", err)
	}

	// An empty list is [] in JSON, not null.
	out, err = fw(tok.Token, "-o", "json", "device", "gaps", dev.ID.String())
	if err != nil || strings.TrimSpace(out) != "[]" {
		t.Fatalf("device gaps of a device without gaps: %q, %v", out, err)
	}
	var tokens []struct{ ID string }
	mustJSON(tok.Token, &tokens, "token", "list", "-limit", "1")
	if len(tokens) != 1 {
		t.Fatalf("token list -limit 1 = %+v", tokens)
	}
	var shownTok struct{ ID, Name string }
	mustJSON(tok.Token, &shownTok, "token", "show", tokens[0].ID)
	if shownTok.ID != tokens[0].ID || shownTok.Name == "" {
		t.Fatalf("token show = %+v", shownTok)
	}
	var del struct {
		JobID string `json:"job_id"`
	}
	mustJSON(tok.Token, &del, "device", "delete", dev.ID.String(), "-yes")
	var job struct{ ID, Kind, Status string }
	mustJSON(tok.Token, &job, "job", "show", del.JobID)
	if job.ID != del.JobID || job.Kind != "device_purge" {
		t.Fatalf("job show = %+v", job)
	}

	if _, err := fw(h.OperatorToken, "tenant", "destroy", tenant.ID, "-yes"); err != nil {
		t.Fatal(err)
	}
}

// The API takes ttl_seconds in whole seconds and rejects 0, so fwctl
// refuses a -ttl it would have to truncate (500ms would become 0) instead of
// sending another lifetime. Without -ttl it sends no ttl_seconds and the
// server's default applies.
func TestEnrollTokenTTLMustBeWholeSeconds(t *testing.T) {
	env := map[string]string{"FWCTL_URL": "https://127.0.0.1:1", "FWCTL_TOKEN": "fwt_unused"}
	var out, errOut bytes.Buffer
	err := run(t.Context(), []string{"enroll-token", "create", "-ttl", "500ms"}, func(k string) string { return env[k] }, &out, &errOut)
	if err == nil || !strings.Contains(err.Error(), "-ttl") {
		t.Fatalf("-ttl 500ms: %v, want an error about -ttl", err)
	}
}

// fwctlEnv runs fwctl with the given URL and a token, and no real API.
func fwctlEnv(t *testing.T, base string, args ...string) (string, error) {
	t.Helper()
	env := map[string]string{"FWCTL_URL": base, "FWCTL_TOKEN": "fwt_unused"}
	var out, errOut bytes.Buffer
	err := run(t.Context(), args, func(k string) string { return env[k] }, &out, &errOut)
	return out.String(), err
}

func TestCommandsRejectFlagsTheyDoNotTake(t *testing.T) {
	for _, args := range [][]string{
		{"device", "show", "ID", "-yes"},
		{"tenant", "list", "-name", "x"},
		{"export", "status", "ID", "-out", "f"},
	} {
		_, err := fwctlEnv(t, "https://127.0.0.1:1", args...)
		if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Errorf("fwctl %v: %v, want an undefined-flag error", args, err)
		}
	}
	// Every flag a command lists exists.
	all := flag.NewFlagSet("all", flag.ContinueOnError)
	for name, c := range (&cli{}).commands(all) {
		for _, f := range c.flags {
			if all.Lookup(f) == nil {
				t.Errorf("%s lists -%s, which is not defined", name, f)
			}
		}
	}
}

func TestParseInterleavedStopsAtDoubleDash(t *testing.T) {
	for _, c := range []struct {
		args []string
		pos  []string
		yes  bool
		out  string
	}{
		{[]string{"ID", "-yes"}, []string{"ID"}, true, ""},
		{[]string{"ID", "-yes", "--", "-not-a-flag", "x"}, []string{"ID", "-not-a-flag", "x"}, true, ""},
		{[]string{"--", "-yes"}, []string{"-yes"}, false, ""},
		{[]string{"-out", "--", "ID", "-yes"}, []string{"ID"}, true, "--"},
		{[]string{"ID", "--"}, []string{"ID"}, false, ""},
	} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		yes := fs.Bool("yes", false, "")
		out := fs.String("out", "", "")
		pos, err := parseInterleaved(fs, c.args)
		if err != nil || !slices.Equal(pos, c.pos) || *yes != c.yes || *out != c.out {
			t.Errorf("parseInterleaved(%q) = %q, -yes %v, -out %q, %v; want %q, %v, %q",
				c.args, pos, *yes, *out, err, c.pos, c.yes, c.out)
		}
	}
}

func TestPlainHTTPOnlyToLoopback(t *testing.T) {
	for base, ok := range map[string]bool{
		"https://api.example.com": true,
		"http://127.0.0.1:8080":   true,
		"http://[::1]:8080":       true,
		"http://localhost:8080":   true,
		"http://api.example.com":  false,
		"http://10.0.0.5:8443":    false,
		"ftp://api.example.com":   false,
		"api.example.com:8443":    false,
	} {
		if err := checkURL(base); (err == nil) != ok {
			t.Errorf("checkURL(%q) = %v, want ok %v", base, err, ok)
		}
	}
	if _, err := fwctlEnv(t, "http://api.example.com", "device", "list"); err == nil || !strings.Contains(err.Error(), "clear text") {
		t.Fatalf("plain http to a remote host: %v", err)
	}
}

func TestVersionNeedsNoAPI(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run(t.Context(), []string{"version"}, func(string) string { return "" }, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "dev (commit ") {
		t.Fatalf("version: %q", out.String())
	}
}

// -limit is the page size the API is asked for, up to the endpoint's
// largest; an empty list prints [] in JSON.
func TestLimitSetsThePageSize(t *testing.T) {
	var limits []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limits = append(limits, r.URL.Query().Get("limit"))
		_, _ = w.Write([]byte(`{"items": []}`))
	}))
	t.Cleanup(srv.Close)
	for _, c := range []struct {
		limit, want string
	}{{"5", "5"}, {"5000", "1000"}, {"0", "1000"}} {
		limits = nil
		out, err := fwctlEnv(t, srv.URL, "-o", "json", "device", "list", "-limit", c.limit)
		if err != nil || strings.TrimSpace(out) != "[]" {
			t.Fatalf("device list -limit %s: %q, %v", c.limit, out, err)
		}
		if len(limits) != 1 || limits[0] != c.want {
			t.Errorf("device list -limit %s asked for limit %v, want %s", c.limit, limits, c.want)
		}
	}
	if _, err := fwctlEnv(t, srv.URL, "device", "list", "-limit", "-1"); err == nil {
		t.Fatal("negative -limit accepted")
	}
}

// A download has no overall deadline, only one for a stall: a slow one
// that keeps receiving data finishes, one that stops receiving fails.
func TestDownloadStallTimeout(t *testing.T) {
	old := stallTimeout
	t.Cleanup(func() { stallTimeout = old })
	stallTimeout = 300 * time.Millisecond
	payload := bytes.Repeat([]byte("x"), 8)
	sum := sha256.Sum256(payload)
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Digest", "sha-256=:"+base64.StdEncoding.EncodeToString(sum[:])+":")
		w.WriteHeader(http.StatusOK)
		for i, b := range payload {
			if strings.HasSuffix(r.URL.Path, "/stall/download") && i == 4 {
				select {
				case <-r.Context().Done():
				case <-stop:
				}
				return
			}
			_, _ = w.Write([]byte{b})
			http.NewResponseController(w).Flush()
			time.Sleep(100 * time.Millisecond) // 800 ms in all, more than the stall timeout
		}
	}))
	t.Cleanup(srv.Close)
	dest := filepath.Join(t.TempDir(), "export")
	if out, err := fwctlEnv(t, srv.URL, "export", "download", "slow", "-out", dest); err != nil || !strings.Contains(out, "sha-256 verified") {
		t.Fatalf("slow download: %q, %v", out, err)
	}
	_, err := fwctlEnv(t, srv.URL, "export", "download", "stall", "-out", dest+"2")
	if !errors.Is(err, errStalled) {
		t.Fatalf("stalled download: %v, want errStalled", err)
	}
	if _, err := os.Stat(dest + "2"); !os.IsNotExist(err) {
		t.Fatalf("stalled download left a file: %v", err)
	}
}
