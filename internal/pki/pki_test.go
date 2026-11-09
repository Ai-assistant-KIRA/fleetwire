package pki_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
)

func devCA(t *testing.T) (string, *pki.CA) {
	t.Helper()
	dir := t.TempDir()
	if _, err := devpki.Generate(dir, devpki.Options{}); err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(filepath.Join(dir, devpki.DeviceCACert), filepath.Join(dir, devpki.DeviceCAKey))
	if err != nil {
		t.Fatal(err)
	}
	return dir, ca
}

func TestIssueDeviceCertVerifiesToRoot(t *testing.T) {
	dir, ca := devCA(t)
	key, _ := pki.NewDeviceKey()
	csrPEM, err := pki.NewCSR(key, "fleetwire-ingest") // a device asking to be a service
	if err != nil {
		t.Fatal(err)
	}
	csr, err := pki.ParseCSR(csrPEM)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	now := time.Now()
	cert, chainPEM, err := ca.IssueDeviceCert(csr, id, now)
	if err != nil {
		t.Fatal(err)
	}
	if cert.Subject.CommonName != id.String() {
		t.Fatalf("CN = %q: the CSR subject must be ignored", cert.Subject.CommonName)
	}
	if got, err := pki.DeviceID(cert); err != nil || got != id {
		t.Fatalf("DeviceID = %s, %v", got, err)
	}
	// X.509 times have one-second resolution.
	if d := cert.NotAfter.Sub(now) - pki.DeviceCertLifetime; d < -time.Second || d > time.Second || cert.SerialNumber.BitLen() != 128 {
		t.Fatalf("lifetime %v, serial bits %d", cert.NotAfter.Sub(now), cert.SerialNumber.BitLen())
	}
	roots, err := pki.CertPool(filepath.Join(dir, devpki.CABundle))
	if err != nil {
		t.Fatal(err)
	}
	inter := x509.NewCertPool()
	rest := chainPEM
	for {
		var b *pem.Block
		if b, rest = pem.Decode(rest); b == nil {
			break
		}
		c, _ := x509.ParseCertificate(b.Bytes)
		if !c.Equal(cert) {
			inter.AddCert(c)
		}
	}
	if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Fatalf("device cert does not verify to the root: %v", err)
	}
	if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err == nil {
		t.Fatal("device cert is usable as a server cert")
	}
}

func TestParseCSRRejectsBadInput(t *testing.T) {
	p384, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	der, _ := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, p384)
	if _, err := pki.ParseCSR(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})); !errors.Is(err, pki.ErrBadCSR) {
		t.Fatalf("P-384 CSR: %v", err)
	}
	key, _ := pki.NewDeviceKey()
	good, _ := pki.NewCSR(key, "x")
	b, _ := pem.Decode(good)
	b.Bytes[len(b.Bytes)-3] ^= 0xff // break the signature
	if _, err := pki.ParseCSR(pem.EncodeToMemory(b)); !errors.Is(err, pki.ErrBadCSR) {
		t.Fatalf("tampered CSR: %v", err)
	}
	if _, err := pki.ParseCSR([]byte("not pem")); !errors.Is(err, pki.ErrBadCSR) {
		t.Fatalf("garbage: %v", err)
	}
}

func TestCRL(t *testing.T) {
	_, ca := devCA(t)
	serial, _ := pki.NewSerial()
	now := time.Now()
	crlPEM, err := ca.BuildCRL([]pki.RevokedEntry{{Serial: pki.SerialString(serial), RevokedAt: now}}, now, 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := pem.Decode(crlPEM)
	crl, err := x509.ParseRevocationList(b.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := crl.CheckSignatureFrom(ca.Cert); err != nil {
		t.Fatalf("CRL signature: %v", err)
	}
	if len(crl.RevokedCertificateEntries) != 1 || crl.RevokedCertificateEntries[0].SerialNumber.Cmp(serial) != 0 {
		t.Fatalf("CRL entries = %v", crl.RevokedCertificateEntries)
	}
	if crl.NextUpdate.Sub(now) < 6*24*time.Hour {
		t.Fatalf("NextUpdate %v too soon", crl.NextUpdate)
	}
}

func TestDevPKIIsIdempotentAndProtective(t *testing.T) {
	dir := t.TempDir()
	created, err := devpki.Generate(dir, devpki.Options{})
	if err != nil || !created {
		t.Fatalf("first run: %v %v", created, err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, devpki.DeviceCAKey))
	if created, err := devpki.Generate(dir, devpki.Options{}); err != nil || created {
		t.Fatalf("second run: created=%v err=%v, want a no-op", created, err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, devpki.DeviceCAKey))
	if !bytes.Equal(before, after) {
		t.Fatal("second run replaced the CA key without --force")
	}
	for _, k := range []string{devpki.RootKey, devpki.DeviceCAKey, devpki.KeyringRootKey, devpki.MosquittoKey} {
		info, _ := os.Stat(filepath.Join(dir, k))
		if info.Mode().Perm() != 0o400 {
			t.Errorf("%s has mode %v, want 0400", k, info.Mode().Perm())
		}
	}
	if err := os.Remove(filepath.Join(dir, devpki.APIKey)); err != nil {
		t.Fatal(err)
	}
	if _, err := devpki.Generate(dir, devpki.Options{}); !errors.Is(err, devpki.ErrPartial) {
		t.Fatalf("partial set: %v, want ErrPartial", err)
	}
	if created, err := devpki.Generate(dir, devpki.Options{Force: true}); err != nil || !created {
		t.Fatalf("forced run: %v %v", created, err)
	}
}
