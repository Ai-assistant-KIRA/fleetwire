// Package apitest runs a real fleetwire API for tests: real PostgreSQL,
// the real dev PKI, real TLS listeners, and the real job worker. Nothing is
// mocked; only the network is loopback.
package apitest

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crl"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/jobs"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store/storetest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tokens"
)

// Harness is one running API.
type Harness struct {
	t             *testing.T
	Env           *storetest.Env
	DB            *store.DB
	PKIDir        string
	CA            *pki.CA
	Keyring       *keyring.Local
	Keys          *tenantkey.Manager
	CRL           *crl.Publisher
	Worker        *jobs.Worker
	ExportDir     string
	Public        *httptest.Server
	Device        *httptest.Server
	Roots         *x509.CertPool
	OperatorToken string
	Registry      *prometheus.Registry
	// Validate, when set, checks every response the helpers receive (the
	// OpenAPI contract tests plug in here).
	Validate func(t *testing.T, req *http.Request, resp *http.Response, body []byte)
}

// Option changes the API config before start.
type Option func(*api.Config)

// New starts an API and stops it at test end.
func New(t *testing.T, opts ...Option) *Harness {
	t.Helper()
	h := &Harness{t: t, Env: storetest.Start(t), DB: storetest.AppDB(t), PKIDir: t.TempDir(), ExportDir: t.TempDir()}
	if _, err := devpki.Generate(h.PKIDir, devpki.Options{}); err != nil {
		t.Fatal(err)
	}
	p := func(name string) string { return filepath.Join(h.PKIDir, name) }
	ca, err := pki.LoadCA(p(devpki.DeviceCACert), p(devpki.DeviceCAKey))
	if err != nil {
		t.Fatal(err)
	}
	h.CA = ca
	root, err := keyring.LoadRootKey(p(devpki.KeyringRootKey))
	if err != nil {
		t.Fatal(err)
	}
	if h.Keyring, err = keyring.NewLocal(t.TempDir(), root); err != nil {
		t.Fatal(err)
	}
	cursorKey, err := keyring.DeriveKey(root, "api-cursor")
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("FLEETWIRE_TEST_LOG") != "" {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	h.Keys = tenantkey.New(h.DB, h.Keyring, log)
	h.CRL = crl.NewPublisher(h.DB, ca, filepath.Join(t.TempDir(), "crl.pem"), log)
	h.Registry = prometheus.NewRegistry()

	if h.Roots, err = pki.CertPool(p(devpki.CABundle)); err != nil {
		t.Fatal(err)
	}
	bundle, err := os.ReadFile(p(devpki.CABundle))
	if err != nil {
		t.Fatal(err)
	}
	serverCert, err := tls.LoadX509KeyPair(p(devpki.APICert), p(devpki.APIKey))
	if err != nil {
		t.Fatal(err)
	}
	deviceCAs, err := pki.CertPool(p(devpki.DeviceTrust))
	if err != nil {
		t.Fatal(err)
	}
	cfg := api.Config{
		CABundlePEM:  bundle,
		BrokerURL:    "tls://broker.test:8883",
		DeviceAPIURL: "https://api.test:8444",
		ExportDir:    h.ExportDir,
		// High defaults so tests that are not about rate limits never hit them.
		RatePerSecond: 10_000, RateBurst: 10_000, EnrollPerSecond: 10_000, EnrollBurst: 10_000,
	}
	for _, o := range opts {
		o(&cfg)
	}
	srv := api.New(cfg, api.Deps{DB: h.DB, Keyring: h.Keyring, Keys: h.Keys, DeviceCA: ca, CRL: h.CRL,
		CursorKey: cursorKey, Log: log, Registry: h.Registry})
	h.Worker = jobs.NewWorker(jobs.Config{Workers: 0, ExportDir: h.ExportDir, WorkerID: "test-" + uuid.NewString()[:8]},
		jobs.Deps{DB: h.DB, Keyring: h.Keyring, Keys: h.Keys, CRL: h.CRL, Log: log, Registry: h.Registry})

	h.Public = httptest.NewUnstartedServer(srv.PublicHandler())
	h.Public.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12}
	h.Public.StartTLS()
	t.Cleanup(h.Public.Close)
	h.Device = httptest.NewUnstartedServer(srv.DeviceHandler())
	h.Device.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12,
		ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: deviceCAs}
	h.Device.StartTLS()
	t.Cleanup(h.Device.Close)

	id, secret, hash, err := tokens.NewKeyed(tokens.PrefixOperator)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateOperatorToken(context.Background(), h.Env.OwnerURL, id, "test-operator", hash); err != nil {
		t.Fatal(err)
	}
	h.OperatorToken = secret.Reveal()
	return h
}

// Client returns an HTTP client that trusts the dev root and, if cert is
// not nil, presents it.
func (h *Harness) Client(cert *tls.Certificate) *http.Client {
	cfg := &tls.Config{RootCAs: h.Roots, MinVersion: tls.VersionTLS12}
	if cert != nil {
		cfg.Certificates = []tls.Certificate{*cert}
	}
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: cfg}}
}

// Response is a finished HTTP exchange; the body is already read and closed.
type Response struct {
	StatusCode int
	Header     http.Header
	Method     string
	Path       string
}

// Do sends a JSON request to the public listener.
func (h *Harness) Do(method, path, token string, body any) (*Response, []byte) {
	h.t.Helper()
	return h.do(h.Client(nil), h.Public.URL, method, path, token, body)
}

// DoDevice sends a JSON request to the device listener.
func (h *Harness) DoDevice(cert *tls.Certificate, method, path string, body any) (*Response, []byte) {
	h.t.Helper()
	return h.do(h.Client(cert), h.Device.URL, method, path, "", body)
}

func (h *Harness) do(c *http.Client, base, method, path, token string, body any) (*Response, []byte) {
	h.t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(h.t.Context(), method, base+path, rd)
	if err != nil {
		h.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	if h.Validate != nil {
		h.Validate(h.t, req, resp, data)
	}
	return &Response{StatusCode: resp.StatusCode, Header: resp.Header, Method: method, Path: path}, data
}

// MustJSON decodes body into v after checking the status code.
func (h *Harness) MustJSON(resp *Response, body []byte, status int, v any) {
	h.t.Helper()
	if resp.StatusCode != status {
		h.t.Fatalf("%s %s: status %d, want %d: %s", resp.Method, resp.Path, resp.StatusCode, status, body)
	}
	if v != nil {
		if err := json.Unmarshal(body, v); err != nil {
			h.t.Fatalf("decode %s: %v", body, err)
		}
	}
}

// CreateTenant makes a tenant and a token with every scope.
func (h *Harness) CreateTenant(name string) (uuid.UUID, string) {
	h.t.Helper()
	var t struct {
		ID uuid.UUID `json:"id"`
	}
	resp, body := h.Do("POST", "/v1/admin/tenants", h.OperatorToken, map[string]any{"name": name})
	h.MustJSON(resp, body, http.StatusCreated, &t)
	var tok struct {
		Token string `json:"token"`
	}
	resp, body = h.Do("POST", "/v1/admin/tenants/"+t.ID.String()+"/tokens", h.OperatorToken,
		map[string]any{"name": "admin", "scopes": tokens.AllScopes})
	h.MustJSON(resp, body, http.StatusCreated, &tok)
	return t.ID, tok.Token
}

// EnrollmentTokens asks for n enrollment tokens with a tenant token.
func (h *Harness) EnrollmentTokens(tenantToken string, n int) []string {
	h.t.Helper()
	var out struct {
		Tokens []struct {
			Token string `json:"token"`
		} `json:"tokens"`
	}
	resp, body := h.Do("POST", "/v1/enrollment-tokens", tenantToken, map[string]any{"count": n})
	h.MustJSON(resp, body, http.StatusCreated, &out)
	list := make([]string, len(out.Tokens))
	for i, t := range out.Tokens {
		list[i] = t.Token
	}
	return list
}

// Device is an enrolled test device.
type Device struct {
	ID      uuid.UUID
	Key     *ecdsa.PrivateKey
	CertPEM []byte
	TLS     tls.Certificate
}

// Enroll creates a key and enrolls it with an enrollment token.
func (h *Harness) Enroll(enrollToken string) Device {
	h.t.Helper()
	key, err := pki.NewDeviceKey()
	if err != nil {
		h.t.Fatal(err)
	}
	csr, err := pki.NewCSR(key, "test-device")
	if err != nil {
		h.t.Fatal(err)
	}
	var out struct {
		DeviceID       uuid.UUID `json:"device_id"`
		CertificatePEM string    `json:"certificate_pem"`
	}
	resp, body := h.DoDevice(nil, "POST", "/v1/enroll", map[string]any{"token": enrollToken, "csr_pem": string(csr)})
	h.MustJSON(resp, body, http.StatusCreated, &out)
	keyPEM, err := pki.EncodePrivateKey(key)
	if err != nil {
		h.t.Fatal(err)
	}
	pair, err := tls.X509KeyPair([]byte(out.CertificatePEM), keyPEM)
	if err != nil {
		h.t.Fatal(err)
	}
	return Device{ID: out.DeviceID, Key: key, CertPEM: []byte(out.CertificatePEM), TLS: pair}
}

// RunJobs runs queued jobs until none is left.
func (h *Harness) RunJobs() {
	h.t.Helper()
	for {
		ran, err := h.Worker.RunOnce(h.t.Context())
		if err != nil {
			h.t.Fatal(err)
		}
		if !ran {
			return
		}
	}
}
