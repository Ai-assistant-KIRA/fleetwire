// Package pki issues device certificates and certificate revocation lists.
//
// A device's identity is a server-generated UUID in the certificate's
// common name (and a urn:fleetwire:device: URI SAN). The CSR's subject is
// ignored: a device can never choose its own ID, so it can never claim the
// name of another device or of a service (ADR-0009).
package pki

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// DeviceCertLifetime is the validity of a device certificate. Renewal starts
// at two thirds of it.
const DeviceCertLifetime = 30 * 24 * time.Hour

const deviceURIPrefix = "urn:fleetwire:device:"

// CA is a certificate authority that can sign.
type CA struct {
	Cert *x509.Certificate
	Key  crypto.Signer
	// ChainPEM is the CA certificate followed by its parents (not the root),
	// appended to issued certificates so peers can build the chain.
	ChainPEM []byte
}

// LoadCA reads a PEM certificate (optionally followed by its chain) and a
// PEM private key (PKCS#8 or SEC 1).
func LoadCA(certFile, keyFile string) (*CA, error) {
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate: %w", err)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read CA key: %w", err)
	}
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("%s: no PEM certificate", certFile)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}
	key, err := ParsePrivateKey(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", keyFile, err)
	}
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok || !pub.Equal(key.Public()) {
		return nil, errors.New("CA key does not match CA certificate")
	}
	if !cert.IsCA || cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, errors.New("certificate is not a signing CA")
	}
	return &CA{Cert: cert, Key: key, ChainPEM: certPEM}, nil
}

// ParsePrivateKey parses a PEM ECDSA key in PKCS#8 or SEC 1 form.
func ParsePrivateKey(keyPEM []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, errors.New("no PEM private key")
	}
	switch block.Type {
	case "PRIVATE KEY":
		k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse PKCS#8 key: %w", err)
		}
		ek, ok := k.(*ecdsa.PrivateKey)
		if !ok {
			return nil, errors.New("key is not ECDSA")
		}
		return ek, nil
	case "EC PRIVATE KEY":
		k, err := x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse EC key: %w", err)
		}
		return k, nil
	}
	return nil, fmt.Errorf("unsupported PEM block %q", block.Type)
}

// ErrBadCSR means the CSR is unusable for a device certificate.
var ErrBadCSR = errors.New("pki: invalid certificate signing request")

// ParseCSR decodes a PEM CSR, checks its signature, and requires an ECDSA
// P-256 public key.
func ParseCSR(csrPEM []byte) (*x509.CertificateRequest, error) {
	block, _ := pem.Decode(csrPEM)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, fmt.Errorf("%w: no PEM certificate request", ErrBadCSR)
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadCSR, err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("%w: signature: %w", ErrBadCSR, err)
	}
	pub, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok || pub.Curve != elliptic.P256() {
		return nil, fmt.Errorf("%w: key must be ECDSA P-256", ErrBadCSR)
	}
	return csr, nil
}

// NewSerial returns a random 128-bit serial number.
func NewSerial() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("pki: serial: %w", err)
	}
	// A zero or tiny serial is legal but looks like a bug in logs.
	return n.SetBit(n, 127, 1), nil
}

// SerialString formats a serial the way it is stored and logged.
func SerialString(n *big.Int) string { return strings.ToLower(n.Text(16)) }

// IssueDeviceCert signs a client certificate for deviceID with the CSR's
// public key. Only the key is taken from the CSR.
func (ca *CA) IssueDeviceCert(csr *x509.CertificateRequest, deviceID uuid.UUID, now time.Time) (*x509.Certificate, []byte, error) {
	serial, err := NewSerial()
	if err != nil {
		return nil, nil, err
	}
	uri, err := url.Parse(deviceURIPrefix + deviceID.String())
	if err != nil {
		return nil, nil, fmt.Errorf("pki: device uri: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: deviceID.String()},
		URIs:                  []*url.URL{uri},
		NotBefore:             now.Add(-5 * time.Minute), // tolerate small clock skew on devices
		NotAfter:              now.Add(DeviceCertLifetime),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, csr.PublicKey, ca.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("pki: sign device certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, fmt.Errorf("pki: parse issued certificate: %w", err)
	}
	out := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return cert, append(out, ca.ChainPEM...), nil
}

// DeviceID returns the device ID of a verified client certificate. The CN
// and the URI SAN must agree, so a certificate from another issuer that
// happens to chain to the same root cannot pass as a device.
func DeviceID(cert *x509.Certificate) (uuid.UUID, error) {
	id, err := uuid.Parse(cert.Subject.CommonName)
	if err != nil {
		return uuid.Nil, fmt.Errorf("pki: certificate CN %q is not a device ID", cert.Subject.CommonName)
	}
	for _, u := range cert.URIs {
		if u.String() == deviceURIPrefix+id.String() {
			return id, nil
		}
	}
	return uuid.Nil, errors.New("pki: certificate has no matching device URI")
}

// RevokedEntry is one CRL entry.
type RevokedEntry struct {
	Serial    string
	RevokedAt time.Time
}

// BuildCRL signs a CRL listing revoked. The CRL number is derived from the
// time, so it increases across restarts and replicas without shared state.
func (ca *CA) BuildCRL(revoked []RevokedEntry, now time.Time, validity time.Duration) ([]byte, error) {
	entries := make([]x509.RevocationListEntry, 0, len(revoked))
	for _, r := range revoked {
		n, ok := new(big.Int).SetString(r.Serial, 16)
		if !ok {
			return nil, fmt.Errorf("pki: bad serial %q", r.Serial)
		}
		entries = append(entries, x509.RevocationListEntry{SerialNumber: n, RevocationTime: r.RevokedAt.UTC()})
	}
	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:                    big.NewInt(now.UnixNano()),
		ThisUpdate:                now.Add(-time.Minute),
		NextUpdate:                now.Add(validity),
		RevokedCertificateEntries: entries,
	}, ca.Cert, ca.Key)
	if err != nil {
		return nil, fmt.Errorf("pki: sign CRL: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der}), nil
}

// NewDeviceKey creates a device's private key (ECDSA P-256).
func NewDeviceKey() (*ecdsa.PrivateKey, error) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("pki: generate key: %w", err)
	}
	return k, nil
}

// EncodePrivateKey encodes a key as PKCS#8 PEM.
func EncodePrivateKey(k *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		return nil, fmt.Errorf("pki: encode key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// NewCSR builds a CSR for key. The subject is informational only.
func NewCSR(key *ecdsa.PrivateKey, commonName string) ([]byte, error) {
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: commonName},
	}, key)
	if err != nil {
		return nil, fmt.Errorf("pki: create CSR: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}), nil
}

// CertPool reads every certificate in a PEM file into a pool.
func CertPool(file string) (*x509.CertPool, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("%s: no certificates", file)
	}
	return pool, nil
}
