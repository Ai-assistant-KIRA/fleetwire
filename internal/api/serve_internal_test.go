package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
)

// testTLS returns a server TLS config with the dev PKI's API certificate
// and a client config that trusts it.
func testTLS(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	dir := t.TempDir()
	if _, err := devpki.Generate(dir, devpki.Options{}); err != nil {
		t.Fatal(err)
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, devpki.APICert), filepath.Join(dir, devpki.APIKey))
	if err != nil {
		t.Fatal(err)
	}
	var roots *x509.CertPool
	if roots, err = pki.CertPool(filepath.Join(dir, devpki.CABundle)); err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
}

// events records what happened, in order, from any goroutine.
type events struct {
	mu   sync.Mutex
	list []string
}

func (e *events) note(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list = append(e.list, s)
}

func (e *events) index(s string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Index(e.list, s)
}

func (e *events) String() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return fmt.Sprint(e.list)
}

// heldListener serves one handler that holds every request until release
// is closed. shutdown is closed when the server's Shutdown starts.
type heldListener struct {
	name     string
	l        listener
	url      string
	started  chan struct{}
	release  chan struct{}
	shutdown chan struct{}
	once     sync.Once
}

// unblock lets the held requests finish.
func (h *heldListener) unblock() { h.once.Do(func() { close(h.release) }) }

func newHeldListener(t *testing.T, name string, tlsCfg *tls.Config, ev *events) *heldListener {
	t.Helper()
	h := &heldListener{name: name, started: make(chan struct{}, 1), release: make(chan struct{}), shutdown: make(chan struct{})}
	t.Cleanup(h.unblock)
	srv := &http.Server{
		TLSConfig:         tlsCfg.Clone(),
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			h.started <- struct{}{}
			<-h.release
			ev.note(name + " request done")
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	srv.RegisterOnShutdown(func() {
		ev.note(name + " shutdown")
		close(h.shutdown)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h.l = listener{srv: srv, ln: ln}
	h.url = "https://" + ln.Addr().String()
	return h
}

// get sends a request to h and reports how it ended on the returned channel.
func (h *heldListener) get(client *http.Client) <-chan error {
	done := make(chan error, 1)
	go func() {
		resp, err := client.Get(h.url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				err = fmt.Errorf("status %d", resp.StatusCode)
			}
		}
		done <- err
	}()
	return done
}

func await(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal(what)
	}
}

// TestShutdownDrainsBothListenersBeforeTheBackgroundStops: at shutdown,
// both listeners start draining at once, and the work that must outlive
// the requests (token use, the CRL publisher) stops only after the
// requests still running have finished.
func TestShutdownDrainsBothListenersBeforeTheBackgroundStops(t *testing.T) {
	serverTLS, clientTLS := testTLS(t)
	ev := &events{}
	pub, dev := newHeldListener(t, "public", serverTLS, ev), newHeldListener(t, "device", serverTLS, ev)
	stopped := make(chan struct{})
	background := func(ctx context.Context) {
		<-ctx.Done()
		ev.note("background stopped")
		close(stopped)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	go func() {
		served <- serve(ctx, []listener{pub.l, dev.l}, time.Minute, []func(context.Context){background})
	}()

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS}}
	defer client.CloseIdleConnections()
	var answers []<-chan error
	for _, h := range []*heldListener{pub, dev} {
		answers = append(answers, h.get(client))
		select {
		case <-h.started:
		case <-time.After(10 * time.Second):
			t.Fatalf("the %s request did not reach its handler", h.name)
		}
	}
	cancel()
	// Drained one after the other, the second listener's shutdown would
	// start only once the first one's request had finished.
	await(t, pub.shutdown, "the public listener's shutdown did not start while the device one drained")
	await(t, dev.shutdown, "the device listener's shutdown did not start while the public one drained")
	select {
	case <-stopped:
		t.Fatal("the background work stopped while requests were still running")
	default:
	}
	pub.unblock()
	dev.unblock()
	for i, a := range answers {
		if err := <-a; err != nil {
			t.Fatalf("request %d, running at shutdown, failed: %v", i, err)
		}
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after the listeners drained")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("serve returned before the background work stopped")
	}
	bg := ev.index("background stopped")
	for _, name := range []string{"public", "device"} {
		if i := ev.index(name + " request done"); i < 0 || i > bg {
			t.Fatalf("the background work stopped before the %s request finished: %s", name, ev)
		}
	}
}

// TestShutdownGivesUpOnAStuckRequest: a request that never finishes holds
// the shutdown for the drain budget only, and the background work stops
// then too.
func TestShutdownGivesUpOnAStuckRequest(t *testing.T) {
	serverTLS, clientTLS := testTLS(t)
	ev := &events{}
	stuck := newHeldListener(t, "public", serverTLS, ev)
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	go func() {
		served <- serve(ctx, []listener{stuck.l}, 100*time.Millisecond,
			[]func(context.Context){func(ctx context.Context) { <-ctx.Done(); close(stopped) }})
	}()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS}}
	defer client.CloseIdleConnections()
	stuck.get(client)
	select {
	case <-stuck.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the request did not reach its handler")
	}
	cancel()
	select {
	case err := <-served:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("serve: %v, want the drain budget exceeded", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a stuck request held the shutdown past the drain budget")
	}
	await(t, stopped, "the background work did not stop after the drain budget ran out")
}
