package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	if _, err := fw(h.OperatorToken, "tenant", "destroy", tenant.ID, "-yes"); err != nil {
		t.Fatal(err)
	}
}
