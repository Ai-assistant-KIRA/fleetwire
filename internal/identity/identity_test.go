package identity_test

import (
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/identity"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
)

// Each enrollment attempt builds its own HTTP client. When the attempt ends
// its kept-alive connection must be closed; otherwise a device retrying
// every 30 s piled up open connections, each with two goroutines, until
// the server dropped them.
func TestEnrollAttemptsLeaveNoConnectionOpen(t *testing.T) {
	var mu sync.Mutex
	open := map[net.Conn]bool{}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"status":503,"code":"unavailable"}`, http.StatusServiceUnavailable)
	}))
	srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
		mu.Lock()
		defer mu.Unlock()
		if s == http.StateNew {
			open[c] = true
		}
		if s == http.StateClosed || s == http.StateHijacked {
			delete(open, c)
		}
	}
	srv.StartTLS()
	defer srv.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	id, err := identity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := id.Enroll(t.Context(), srv.URL, "fwe_test", "dev", ca); err == nil || errors.Is(err, identity.ErrPermanent) {
			t.Fatalf("Enroll against a 503 = %v, want a retryable error", err)
		}
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		mu.Lock()
		n := len(open)
		mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connections still open after three enrollment attempts", n)
		}
	}
}

func TestEnrollRenewAndReload(t *testing.T) {
	h := apitest.New(t, func(c *api.Config) {})
	_, tok := h.CreateTenant("ident")
	bundle, err := os.ReadFile(filepath.Join(h.PKIDir, devpki.CABundle))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	id, err := identity.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if id.Enrolled() {
		t.Fatal("new identity claims to be enrolled")
	}
	info, _ := os.Stat(filepath.Join(dir, "key.pem"))
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("key mode %v", info.Mode().Perm())
	}
	if err := id.Enroll(t.Context(), h.Device.URL, "fwe_"+"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "dev", bundle); !errors.Is(err, identity.ErrPermanent) {
		t.Fatalf("bad token: %v, want ErrPermanent", err)
	}
	if err := id.Enroll(t.Context(), h.Device.URL, h.EnrollmentTokens(tok, 1)[0], "dev", bundle); err != nil {
		t.Fatal(err)
	}
	first := id.Leaf()
	if !id.Enrolled() || first.Subject.CommonName != id.Info().DeviceID.String() || id.Info().BrokerURL == "" {
		t.Fatalf("after enroll: %+v", id.Info())
	}
	if at := identity.RenewAt(first); at.Before(first.NotBefore.Add(19*24*time.Hour)) || at.After(first.NotAfter) {
		t.Fatalf("RenewAt = %v for %v..%v", at, first.NotBefore, first.NotAfter)
	}

	if err := id.Renew(t.Context()); err != nil {
		t.Fatal(err)
	}
	if id.Leaf().SerialNumber.Cmp(first.SerialNumber) == 0 {
		t.Fatal("renewal kept the old certificate")
	}
	// A restart loads the renewed certificate and the same key.
	again, err := identity.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if again.Info() != id.Info() || again.Leaf().SerialNumber.Cmp(id.Leaf().SerialNumber) != 0 {
		t.Fatal("reloaded identity differs")
	}

	resp, body := h.Do("POST", "/v1/devices/"+id.Info().DeviceID.String()+"/revoke", tok, nil)
	h.MustJSON(resp, body, 200, nil)
	if err := id.Renew(t.Context()); !errors.Is(err, identity.ErrPermanent) {
		t.Fatalf("renewal of a revoked device: %v, want ErrPermanent", err)
	}
}
