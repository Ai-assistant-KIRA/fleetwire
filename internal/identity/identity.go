// Package identity is the device's key, certificate and enrollment state.
//
// Files in the identity directory (0700):
//
//	key.pem      ECDSA P-256 private key, PKCS#8 (0600). Created on first
//	             start and never sent anywhere (R4.1).
//	cert.pem     current device certificate chain
//	ca.pem       CA bundle for the broker and the API
//	device.json  device ID, broker URL and API URL from enrollment
//
// The current certificate sits behind an atomic pointer that the TLS
// configs read through GetClientCertificate, so a renewal takes effect on
// the next handshake without restarting anything.
package identity

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/fsutil"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
)

const (
	keyFile    = "key.pem"
	certFile   = "cert.pem"
	caFile     = "ca.pem"
	deviceFile = "device.json"
)

// Info is what enrollment told the device.
type Info struct {
	DeviceID  uuid.UUID `json:"device_id"`
	BrokerURL string    `json:"broker_url"`
	APIURL    string    `json:"api_url"`
}

// ErrPermanent wraps enrollment and renewal failures that retrying cannot
// fix (bad token, device revoked); an operator has to act.
var ErrPermanent = errors.New("identity: permanent failure")

// Identity is safe for concurrent use.
type Identity struct {
	dir   string
	key   *ecdsa.PrivateKey
	cert  atomic.Pointer[tls.Certificate]
	roots atomic.Pointer[x509.CertPool]
	info  atomic.Pointer[Info]
	// HTTPTimeout bounds one enroll or renew call.
	HTTPTimeout time.Duration
}

// Open loads the identity in dir, creating the directory and the key on
// first use.
func Open(dir string) (*Identity, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("identity dir: %w", err)
	}
	id := &Identity{dir: dir, HTTPTimeout: 30 * time.Second}
	keyPEM, err := os.ReadFile(filepath.Join(dir, keyFile))
	switch {
	case errors.Is(err, os.ErrNotExist):
		k, err := pki.NewDeviceKey()
		if err != nil {
			return nil, err
		}
		pemBytes, err := pki.EncodePrivateKey(k)
		if err != nil {
			return nil, err
		}
		if err := fsutil.WriteAtomic(filepath.Join(dir, keyFile), pemBytes, 0o600); err != nil {
			return nil, err
		}
		id.key = k
	case err != nil:
		return nil, fmt.Errorf("read device key: %w", err)
	default:
		if id.key, err = pki.ParsePrivateKey(keyPEM); err != nil {
			return nil, fmt.Errorf("device key: %w", err)
		}
	}
	if err := id.loadEnrollment(); err != nil {
		return nil, err
	}
	return id, nil
}

func (id *Identity) loadEnrollment() error {
	infoJSON, err := os.ReadFile(filepath.Join(id.dir, deviceFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read device.json: %w", err)
	}
	var info Info
	if err := json.Unmarshal(infoJSON, &info); err != nil {
		return fmt.Errorf("device.json is corrupt: %w", err)
	}
	if info.DeviceID == uuid.Nil {
		return errors.New("device.json has no device ID")
	}
	certPEM, err := os.ReadFile(filepath.Join(id.dir, certFile))
	if err != nil {
		return fmt.Errorf("read device certificate: %w", err)
	}
	caPEM, err := os.ReadFile(filepath.Join(id.dir, caFile))
	if err != nil {
		return fmt.Errorf("read CA bundle: %w", err)
	}
	if err := id.install(certPEM, caPEM); err != nil {
		return err
	}
	id.info.Store(&info)
	return nil
}

// install parses and activates a certificate chain and CA bundle.
func (id *Identity) install(certPEM, caPEM []byte) error {
	keyPEM, err := pki.EncodePrivateKey(id.key)
	if err != nil {
		return err
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("device certificate does not match the device key: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return errors.New("CA bundle holds no certificates")
	}
	id.cert.Store(&pair)
	id.roots.Store(pool)
	return nil
}

// Enrolled reports whether the device has a certificate.
func (id *Identity) Enrolled() bool { return id.info.Load() != nil }

// Info returns the enrollment data, or the zero value before enrollment.
func (id *Identity) Info() Info {
	if p := id.info.Load(); p != nil {
		return *p
	}
	return Info{}
}

// Leaf returns the current certificate, or nil before enrollment.
func (id *Identity) Leaf() *x509.Certificate {
	if c := id.cert.Load(); c != nil {
		return c.Leaf
	}
	return nil
}

// ClientTLS returns a TLS config that presents the current certificate and
// trusts the enrollment CA bundle. serverName is the broker or API host.
func (id *Identity) ClientTLS(serverName string) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
		// Read on every handshake, so a renewed certificate is used from the
		// next connection on without restarting anything.
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			c := id.cert.Load()
			if c == nil {
				return nil, errors.New("device is not enrolled")
			}
			return c, nil
		},
		// The trust roots are fixed when the config is built. A changed CA
		// bundle (CA rotation) takes effect at the next agent start; the
		// rotation guide ships the old and new roots together.
		RootCAs: id.roots.Load(),
	}
}

type problem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func (id *Identity) post(ctx context.Context, c *http.Client, url string, body any, want int, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, id.HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != want {
		var p problem
		_ = json.Unmarshal(data, &p)
		msg := fmt.Errorf("POST %s: HTTP %d %s: %s", url, resp.StatusCode, p.Code, p.Detail)
		// 4xx except 429 will fail the same way next time.
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("%w: %w", ErrPermanent, msg)
		}
		return msg
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// Enroll trades a one-time token for a device certificate. caBundle, if not
// empty, is the only trust for the API's certificate; otherwise the system
// roots are used.
func (id *Identity) Enroll(ctx context.Context, apiURL, token, name string, caBundle []byte) error {
	csr, err := pki.NewCSR(id.key, "fleetwire-device")
	if err != nil {
		return err
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if len(caBundle) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBundle) {
			return fmt.Errorf("%w: enroll CA bundle holds no certificates", ErrPermanent)
		}
		tlsCfg.RootCAs = pool
	}
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}
	// The client lives for one attempt; its kept-alive connection would
	// otherwise stay open, with two goroutines, after every retry.
	defer c.CloseIdleConnections()
	var resp struct {
		DeviceID       uuid.UUID `json:"device_id"`
		CertificatePEM string    `json:"certificate_pem"`
		CABundlePEM    string    `json:"ca_bundle_pem"`
		BrokerURL      string    `json:"broker_url"`
		APIURL         string    `json:"api_url"`
	}
	body := map[string]string{"token": token, "csr_pem": string(csr), "name": name}
	if err := id.post(ctx, c, strings.TrimSuffix(apiURL, "/")+"/v1/enroll", body, http.StatusCreated, &resp); err != nil {
		return err
	}
	if err := id.install([]byte(resp.CertificatePEM), []byte(resp.CABundlePEM)); err != nil {
		return err
	}
	got, err := pki.DeviceID(id.Leaf())
	if err != nil {
		return fmt.Errorf("enrollment returned an unusable certificate: %w", err)
	}
	if got != resp.DeviceID {
		return fmt.Errorf("enrollment returned a certificate for device %s, not %s", got, resp.DeviceID)
	}
	// The certificate and bundle go to disk before device.json: device.json
	// is the marker that enrollment completed.
	if err := fsutil.WriteAtomic(filepath.Join(id.dir, certFile), []byte(resp.CertificatePEM), 0o644); err != nil {
		return err
	}
	if err := fsutil.WriteAtomic(filepath.Join(id.dir, caFile), []byte(resp.CABundlePEM), 0o644); err != nil {
		return err
	}
	info := Info{DeviceID: resp.DeviceID, BrokerURL: resp.BrokerURL, APIURL: resp.APIURL}
	if info.APIURL == "" {
		info.APIURL = apiURL
	}
	infoJSON, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("encode device.json: %w", err)
	}
	if err := fsutil.WriteAtomic(filepath.Join(id.dir, deviceFile), infoJSON, 0o644); err != nil {
		return err
	}
	id.info.Store(&info)
	return nil
}

// Renew asks the API for a new certificate for the same key, over mutual
// TLS with the current one.
func (id *Identity) Renew(ctx context.Context) error {
	info := id.Info()
	if info.DeviceID == uuid.Nil {
		return errors.New("identity: not enrolled")
	}
	csr, err := pki.NewCSR(id.key, info.DeviceID.String())
	if err != nil {
		return err
	}
	// An empty ServerName makes net/http verify against the URL's host.
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: id.ClientTLS("")}}
	defer c.CloseIdleConnections() // as in Enroll
	var resp struct {
		CertificatePEM string `json:"certificate_pem"`
		CABundlePEM    string `json:"ca_bundle_pem"`
	}
	if err := id.post(ctx, c, strings.TrimSuffix(info.APIURL, "/")+"/v1/renew", map[string]string{"csr_pem": string(csr)}, http.StatusOK, &resp); err != nil {
		return err
	}
	if err := id.install([]byte(resp.CertificatePEM), []byte(resp.CABundlePEM)); err != nil {
		return err
	}
	if err := fsutil.WriteAtomic(filepath.Join(id.dir, caFile), []byte(resp.CABundlePEM), 0o644); err != nil {
		return err
	}
	return fsutil.WriteAtomic(filepath.Join(id.dir, certFile), []byte(resp.CertificatePEM), 0o644)
}

// RenewAt is when renewal of cert should start: two thirds into its life.
func RenewAt(cert *x509.Certificate) time.Time {
	life := cert.NotAfter.Sub(cert.NotBefore)
	return cert.NotBefore.Add(life * 2 / 3)
}
