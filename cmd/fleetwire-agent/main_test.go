package main

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

func TestDialAddrReachesAWildcardListenerOnLoopback(t *testing.T) {
	for listen, want := range map[string]string{
		":9465":          "127.0.0.1:9465",
		"0.0.0.0:9465":   "127.0.0.1:9465",
		"[::]:9465":      "[::1]:9465",
		"127.0.0.1:9465": "127.0.0.1:9465",
		"10.0.0.5:9465":  "10.0.0.5:9465",
	} {
		if got := dialAddr(listen); got != want {
			t.Errorf("dialAddr(%q) = %q, want %q", listen, got, want)
		}
	}
}

func TestStatusAsksADaemonOnAWildcardAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"running": true, "version": "v9.9.9", "device_id": "d1", "last_seq": 3}`))
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	var out bytes.Buffer
	err = run(t.Context(), []string{"status", "-data-dir", t.TempDir(), "-set", "metrics.listen=:" + port}, &out, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "running:        true") || !strings.Contains(out.String(), "version:        v9.9.9") {
		t.Fatalf("status output:\n%s", out.String())
	}
}

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

// inSnap makes run see a snap environment: $SNAP, $SNAP_DATA and a
// snapctl that reports metrics.listen on a loopback port nothing answers
// on. It returns $SNAP_DATA.
func inSnap(t *testing.T) string {
	t.Helper()
	snap, data, bin := t.TempDir(), t.TempDir(), t.TempDir()
	script := "#!/bin/sh\necho '{\"metrics\": {\"listen\": \"127.0.0.1:1\"}}'\n"
	if err := os.WriteFile(filepath.Join(bin, "snapctl"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SNAP", snap)
	t.Setenv("SNAP_DATA", data)
	return data
}

func writeLog(t *testing.T, dir string, records int) {
	t.Helper()
	l, _, err := wal.Open(wal.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for i := range records {
		if _, err := l.Append(fmt.Appendf(nil, "reading %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

// In the snap the state is in $SNAP_DATA/state, not in $SNAP_DATA.
func TestSnapStatusReadsTheStateDirectory(t *testing.T) {
	data := inSnap(t)
	writeLog(t, filepath.Join(data, "state", "wal"), 3)
	writeLog(t, filepath.Join(data, "wal"), 7) // an old layout's log is not read
	var out bytes.Buffer
	if err := run(t.Context(), []string{"status"}, &out, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "running:        false") || !strings.Contains(out.String(), "last_seq:       3\n") {
		t.Fatalf("status output:\n%s", out.String())
	}
}

// Without sudo, a user cannot read the daemon's files; the error says what
// to do.
func TestSnapStatusWithoutPermissionSuggestsSudo(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads the files whatever their mode")
	}
	data := inSnap(t)
	st := filepath.Join(data, "state")
	writeLog(t, filepath.Join(st, "wal"), 1)
	if err := os.Chmod(st, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(st, 0o700) })
	err := run(t.Context(), []string{"status"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "run status with sudo") {
		t.Fatalf("status: %v", err)
	}
}

func TestPrepareStateRunsOnlyAsRootInTheSnap(t *testing.T) {
	err := run(t.Context(), []string{"prepare-state"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "only inside the snap") {
		t.Fatalf("outside the snap: %v", err)
	}
	if os.Geteuid() == 0 {
		t.Skip("running as root")
	}
	data := inSnap(t)
	err = run(t.Context(), []string{"prepare-state"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "must run as root") {
		t.Fatalf("as uid %d: %v", os.Getuid(), err)
	}
	if entries, _ := os.ReadDir(data); len(entries) != 0 {
		t.Fatalf("prepare-state changed $SNAP_DATA: %v", entries)
	}
}
