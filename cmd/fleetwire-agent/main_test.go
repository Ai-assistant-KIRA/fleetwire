package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

// With no daemon, status reads the files; it still names the version.
func TestOfflineStatusShowsTheVersion(t *testing.T) {
	dir := t.TempDir()
	l, _, err := wal.Open(wal.Options{Dir: filepath.Join(dir, "wal")})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	// Port 1 on loopback refuses the connection: no daemon answers.
	if err := run(t.Context(), []string{"status", "-data-dir", dir, "-set", "metrics.listen=127.0.0.1:1"}, &out, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "running:        false") || !strings.Contains(out.String(), "version:        dev") {
		t.Fatalf("status output:\n%s", out.String())
	}
}
