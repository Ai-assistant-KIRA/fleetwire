// Package devpki creates the development certificate authorities and
// certificates for the Compose stack and the tests:
//
//	fleetwire Root CA (10 y)
//	├── Device Issuing CA (2 y)   signs device certificates and the CRL (used by fleetwire-api)
//	└── Service CA (2 y)          signs the Mosquitto and API server certs and the bridge/ingest client certs
//
// In production the root key stays offline. Private keys are written with
// mode 0400 and never leave the output directory, which is git-ignored.
package devpki

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
)

// Files written by Generate.
const (
	RootCert       = "root-ca.pem"
	RootKey        = "root-ca.key"
	DeviceCACert   = "device-ca.pem"
	DeviceCAKey    = "device-ca.key"
	ServiceCACert  = "service-ca.pem"
	ServiceCAKey   = "service-ca.key"
	DeviceTrust    = "device-trust.pem"  // device CA + root: verifies device client certs
	ServiceTrust   = "service-trust.pem" // service CA + root: verifies service client certs
	CABundle       = "ca-bundle.pem"     // root: what clients trust for servers
	MosquittoCert  = "mosquitto.pem"
	MosquittoKey   = "mosquitto.key"
	APICert        = "api.pem"
	APIKey         = "api.key"
	BridgeCert     = "bridge.pem"
	BridgeKey      = "bridge.key"
	IngestCert     = "ingest.pem"
	IngestKey      = "ingest.key"
	KeyringRootKey = "keyring-root.key"
	CRL            = "crl.pem"
)

// All lists every file Generate writes.
var All = []string{
	RootCert, RootKey, DeviceCACert, DeviceCAKey, ServiceCACert, ServiceCAKey, DeviceTrust, ServiceTrust,
	CABundle, MosquittoCert, MosquittoKey, APICert, APIKey, BridgeCert, BridgeKey, IngestCert, IngestKey,
	KeyringRootKey, CRL,
}

// Options for Generate.
type Options struct {
	// Force regenerates everything, replacing existing files.
	Force bool
	// Hosts are the DNS names and IPs the server certificates are valid for.
	Hosts []string
	Now   time.Time
}

// ErrPartial means some files exist and others do not.
var ErrPartial = errors.New("devpki: output directory has a partial set of files; use --force to regenerate")

// Generate writes the full set of files into dir. If every file already
// exists and Force is false, it does nothing and returns created=false.
func Generate(dir string, opts Options) (created bool, err error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	if len(opts.Hosts) == 0 {
		opts.Hosts = []string{"localhost", "127.0.0.1", "mosquitto", "toxiproxy", "api"}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, fmt.Errorf("create %s: %w", dir, err)
	}
	present := 0
	for _, f := range All {
		if _, err := os.Stat(filepath.Join(dir, f)); err == nil {
			present++
		}
	}
	switch {
	case present == len(All) && !opts.Force:
		return false, nil
	case present > 0 && !opts.Force:
		return false, ErrPartial
	}
	g := &gen{dir: dir, now: opts.Now}
	g.run(opts.Hosts)
	return g.err == nil, g.err
}

type gen struct {
	dir string
	now time.Time
	err error
}

func (g *gen) write(name string, data []byte, mode os.FileMode) {
	if g.err != nil {
		return
	}
	p := filepath.Join(g.dir, name)
	// Keys are 0400, so replace instead of writing over them.
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		g.err = fmt.Errorf("remove %s: %w", p, err)
		return
	}
	if err := os.WriteFile(p, data, mode); err != nil {
		g.err = fmt.Errorf("write %s: %w", p, err)
	}
}

func (g *gen) key() *ecdsa.PrivateKey {
	if g.err != nil {
		return nil
	}
	k, err := pki.NewDeviceKey()
	if err != nil {
		g.err = err
	}
	return k
}

func pemCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func (g *gen) sign(tmpl, parent *x509.Certificate, pub *ecdsa.PublicKey, signer *ecdsa.PrivateKey) (*x509.Certificate, []byte) {
	if g.err != nil {
		return nil, nil
	}
	serial, err := pki.NewSerial()
	if err != nil {
		g.err = err
		return nil, nil
	}
	tmpl.SerialNumber = serial
	if parent == nil {
		parent = tmpl
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, signer)
	if err != nil {
		g.err = fmt.Errorf("sign %s: %w", tmpl.Subject.CommonName, err)
		return nil, nil
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		g.err = err
		return nil, nil
	}
	return c, pemCert(der)
}

func caTemplate(cn string, now time.Time, years int, pathLen int) *x509.Certificate {
	return &x509.Certificate{
		Subject:               pkix.Name{CommonName: cn, Organization: []string{"fleetwire development"}},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(years, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            pathLen,
		MaxPathLenZero:        pathLen == 0,
	}
}

func leafTemplate(cn string, now time.Time, eku x509.ExtKeyUsage, hosts []string) *x509.Certificate {
	t := &x509.Certificate{
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(1, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{eku},
		BasicConstraintsValid: true,
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			t.IPAddresses = append(t.IPAddresses, ip)
		} else {
			t.DNSNames = append(t.DNSNames, h)
		}
	}
	return t
}

func (g *gen) keyPEM(k *ecdsa.PrivateKey) []byte {
	if g.err != nil {
		return nil
	}
	b, err := pki.EncodePrivateKey(k)
	if err != nil {
		g.err = err
	}
	return b
}

func (g *gen) run(hosts []string) {
	rootKey := g.key()
	if g.err != nil {
		return
	}
	root, rootPEM := g.sign(caTemplate("fleetwire Root CA", g.now, 10, 1), nil, &rootKey.PublicKey, rootKey)

	devKey := g.key()
	if g.err != nil {
		return
	}
	devCA, devPEM := g.sign(caTemplate("fleetwire Device Issuing CA", g.now, 2, 0), root, &devKey.PublicKey, rootKey)

	svcKey := g.key()
	if g.err != nil {
		return
	}
	svcCA, svcPEM := g.sign(caTemplate("fleetwire Service CA", g.now, 2, 0), root, &svcKey.PublicKey, rootKey)

	g.write(RootCert, rootPEM, 0o644)
	g.write(RootKey, g.keyPEM(rootKey), 0o400)
	g.write(DeviceCACert, devPEM, 0o644)
	g.write(DeviceCAKey, g.keyPEM(devKey), 0o400)
	g.write(ServiceCACert, svcPEM, 0o644)
	g.write(ServiceCAKey, g.keyPEM(svcKey), 0o400)
	g.write(DeviceTrust, slices.Concat(devPEM, rootPEM), 0o644)
	g.write(ServiceTrust, slices.Concat(svcPEM, rootPEM), 0o644)
	g.write(CABundle, rootPEM, 0o644)

	leaves := []struct {
		cert, key, cn string
		eku           x509.ExtKeyUsage
		hosts         []string
	}{
		{MosquittoCert, MosquittoKey, "mosquitto", x509.ExtKeyUsageServerAuth, hosts},
		{APICert, APIKey, "fleetwire-api", x509.ExtKeyUsageServerAuth, hosts},
		{BridgeCert, BridgeKey, "fleetwire-bridge", x509.ExtKeyUsageClientAuth, nil},
		{IngestCert, IngestKey, "fleetwire-ingest", x509.ExtKeyUsageClientAuth, nil},
	}
	for _, l := range leaves {
		k := g.key()
		if g.err != nil {
			return
		}
		_, p := g.sign(leafTemplate(l.cn, g.now, l.eku, l.hosts), svcCA, &k.PublicKey, svcKey)
		// Servers and clients send their chain, so peers that trust only
		// the root can verify them.
		g.write(l.cert, slices.Concat(p, svcPEM), 0o644)
		g.write(l.key, g.keyPEM(k), 0o400)
	}

	rootKeyring := make([]byte, 32)
	if _, err := rand.Read(rootKeyring); err != nil && g.err == nil {
		g.err = fmt.Errorf("keyring root key: %w", err)
	}
	g.write(KeyringRootKey, rootKeyring, 0o400)

	if g.err != nil {
		return
	}
	ca := &pki.CA{Cert: devCA, Key: devKey, ChainPEM: devPEM}
	crl, err := ca.BuildCRL(nil, g.now, 7*24*time.Hour)
	if err != nil {
		g.err = err
		return
	}
	g.write(CRL, crl, 0o644)
}
