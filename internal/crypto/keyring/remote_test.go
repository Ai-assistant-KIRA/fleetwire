package keyring_test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/envelope"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
)

type remoteEnv struct {
	local  *keyring.Local
	srv    *httptest.Server
	roots  *x509.CertPool
	ingest *keyring.Remote
	bridge *keyring.Remote // a real service, but not one allowed to use the keyring
}

// newRemoteEnv serves a local keyring the way fleetwire-api does: mutual TLS
// with client certificates checked against the service CA, and only
// fleetwire-ingest admitted.
func newRemoteEnv(t *testing.T) *remoteEnv {
	t.Helper()
	dir := t.TempDir()
	if _, err := devpki.Generate(dir, devpki.Options{}); err != nil {
		t.Fatal(err)
	}
	p := func(name string) string { return filepath.Join(dir, name) }
	root, err := envelope.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	local, err := keyring.NewLocal(t.TempDir(), root)
	if err != nil {
		t.Fatal(err)
	}
	serverCert, err := tls.LoadX509KeyPair(p(devpki.APICert), p(devpki.APIKey))
	if err != nil {
		t.Fatal(err)
	}
	services, err := pki.CertPool(p(devpki.ServiceTrust))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(keyring.NewServer(local, slog.New(slog.DiscardHandler), "fleetwire-ingest"))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12,
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: services}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	roots, err := pki.CertPool(p(devpki.CABundle))
	if err != nil {
		t.Fatal(err)
	}
	client := func(certFile, keyFile string) *keyring.Remote {
		cert, err := tls.LoadX509KeyPair(p(certFile), p(keyFile))
		if err != nil {
			t.Fatal(err)
		}
		r, err := keyring.NewRemote(srv.URL, &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: roots, MinVersion: tls.VersionTLS12})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	return &remoteEnv{local: local, srv: srv, roots: roots,
		ingest: client(devpki.IngestCert, devpki.IngestKey), bridge: client(devpki.BridgeCert, devpki.BridgeKey)}
}

func TestRemoteWrapsAndUnwrapsThroughTheAPI(t *testing.T) {
	ctx := t.Context()
	e := newRemoteEnv(t)
	tenant := uuid.New()
	if err := e.local.Create(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	dek, err := envelope.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	wrapped, kekID, err := e.ingest.Wrap(ctx, tenant, dek)
	if err != nil {
		t.Fatal(err)
	}
	// What ingest wrapped remotely, the API opens locally, and back.
	if got, err := e.local.Unwrap(ctx, tenant, kekID, wrapped); err != nil || !bytes.Equal(got, dek) {
		t.Fatalf("local Unwrap of a remote Wrap = %x, %v", got, err)
	}
	if got, err := e.ingest.Unwrap(ctx, tenant, kekID, wrapped); err != nil || !bytes.Equal(got, dek) {
		t.Fatalf("remote Unwrap = %x, %v", got, err)
	}
	if _, err := e.ingest.Unwrap(ctx, tenant, uuid.NewString(), wrapped); !errors.Is(err, keyring.ErrWrongKEK) {
		t.Fatalf("Unwrap with another KEK ID = %v, want ErrWrongKEK", err)
	}
	if _, _, err := e.ingest.Wrap(ctx, uuid.New(), dek); !errors.Is(err, keyring.ErrNotFound) {
		t.Fatalf("Wrap for an unknown tenant = %v, want ErrNotFound", err)
	}

	// Destroying the KEK at the API makes the remote side fail too.
	if err := e.local.Destroy(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ingest.Unwrap(ctx, tenant, kekID, wrapped); !errors.Is(err, keyring.ErrNotFound) {
		t.Fatalf("Unwrap after Destroy = %v, want ErrNotFound", err)
	}
	// Tenant keys are created and destroyed only by the API.
	if err := e.ingest.Create(ctx, uuid.New()); !errors.Is(err, keyring.ErrRemoteOnly) {
		t.Fatalf("remote Create = %v", err)
	}
	if err := e.ingest.Destroy(ctx, tenant); !errors.Is(err, keyring.ErrRemoteOnly) {
		t.Fatalf("remote Destroy = %v", err)
	}
}

func TestRemoteRefusesOtherServicesAndStrangers(t *testing.T) {
	ctx := t.Context()
	e := newRemoteEnv(t)
	tenant := uuid.New()
	if err := e.local.Create(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	dek, _ := envelope.NewKey()
	// The bridge has a valid service certificate but no business here. The
	// refusal is final, not a reason to retry.
	_, _, err := e.bridge.Wrap(ctx, tenant, dek)
	if err == nil || errors.Is(err, keyring.ErrUnavailable) {
		t.Fatalf("bridge Wrap = %v, want a permanent refusal", err)
	}
	// Without a client certificate the TLS handshake fails.
	plain := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: e.roots, MinVersion: tls.VersionTLS12}}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.srv.URL+"/internal/v1/keyring/unwrap", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := plain.Do(req); err == nil {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("request without a client certificate got %s: %s", resp.Status, body)
	}
	if _, err := keyring.NewRemote(e.srv.URL, &tls.Config{MinVersion: tls.VersionTLS12}); err == nil {
		t.Fatal("NewRemote accepted a config without a client certificate")
	}
	if _, err := keyring.NewRemote("http://api:8445", &tls.Config{Certificates: []tls.Certificate{{}}}); err == nil {
		t.Fatal("NewRemote accepted a plain http URL")
	}
}

func TestRemoteDownIsUnavailable(t *testing.T) {
	e := newRemoteEnv(t)
	e.srv.Close()
	_, err := e.ingest.Unwrap(t.Context(), uuid.New(), uuid.NewString(), []byte("x"))
	if !errors.Is(err, keyring.ErrUnavailable) {
		t.Fatalf("Unwrap with the API down = %v, want ErrUnavailable (so ingest retries)", err)
	}
}
