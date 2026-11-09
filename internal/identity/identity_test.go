package identity_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/identity"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
)

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
