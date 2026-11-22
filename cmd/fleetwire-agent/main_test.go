package main

import (
	"bytes"
	"net"
	"net/http"
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
