package crl

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store/storetest"
)

type env struct {
	t    *testing.T
	db   *store.DB
	ca   *pki.CA
	path string
	pub  *Publisher
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	if _, err := devpki.Generate(dir, devpki.Options{}); err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(filepath.Join(dir, devpki.DeviceCACert), filepath.Join(dir, devpki.DeviceCAKey))
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, db: storetest.AppDB(t), ca: ca, path: filepath.Join(t.TempDir(), "crl.pem")}
	e.pub = NewPublisher(e.db, ca, e.path, slog.New(slog.DiscardHandler))
	return e
}

// cert stores a certificate of a new device of tenant, valid until
// notAfter, and revokes it when revoked is true. It returns the serial.
func (e *env) cert(tenant uuid.UUID, notAfter time.Time, revoked bool) string {
	e.t.Helper()
	n, err := pki.NewSerial()
	if err != nil {
		e.t.Fatal(err)
	}
	serial, device := pki.SerialString(n), uuid.New()
	err = e.db.InTenantTx(e.t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		if err := tx.SetDeviceCert(ctx, store.DeviceCert{Serial: serial, DeviceID: device, NotAfter: notAfter}); err != nil {
			return err
		}
		if revoked {
			_, err := tx.RevokeDevice(ctx, device)
			return err
		}
		return nil
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return serial
}

// parse checks a PEM CRL's signature against the CA and returns its
// serials and its validity.
func (e *env) parse(pemBytes []byte) (map[string]bool, *x509.RevocationList) {
	e.t.Helper()
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "X509 CRL" {
		e.t.Fatalf("not a PEM CRL: %q", pemBytes)
	}
	list, err := x509.ParseRevocationList(block.Bytes)
	if err != nil {
		e.t.Fatal(err)
	}
	if err := list.CheckSignatureFrom(e.ca.Cert); err != nil {
		e.t.Fatalf("CRL signature: %v", err)
	}
	serials := map[string]bool{}
	for _, r := range list.RevokedCertificateEntries {
		serials[pki.SerialString(r.SerialNumber)] = true
	}
	return serials, list
}

// TestPublishListsRevokedCerts: the CRL is signed by the device CA, lists
// every revoked certificate that has not expired and nothing else, and is
// written world-readable for the broker.
func TestPublishListsRevokedCerts(t *testing.T) {
	e := newEnv(t)
	if e.pub.Latest() != nil || e.pub.Age() < 100*365*24*time.Hour {
		t.Fatal("the publisher reports a CRL before the first publish")
	}
	tenant := uuid.New()
	revoked := e.cert(tenant, time.Now().Add(time.Hour), true)
	valid := e.cert(tenant, time.Now().Add(time.Hour), false)
	expired := e.cert(tenant, time.Now().Add(-time.Minute), true)
	before := time.Now()
	if err := e.pub.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(e.path)
	if err != nil {
		t.Fatal(err)
	}
	serials, list := e.parse(data)
	if !serials[revoked] {
		t.Error("a revoked certificate is missing from the CRL")
	}
	if serials[valid] || serials[expired] {
		t.Errorf("the CRL lists a valid (%v) or expired (%v) certificate", serials[valid], serials[expired])
	}
	if d := list.NextUpdate.Sub(before); d < Validity-time.Minute || d > Validity+time.Minute {
		t.Errorf("nextUpdate is %s after publishing, want %s", d, Validity)
	}
	info, err := os.Stat(e.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Errorf("CRL file mode %#o, want 0644", info.Mode().Perm())
	}
	if !bytes.Equal(e.pub.Latest(), data) {
		t.Error("Latest differs from the file written")
	}
	if age := e.pub.Age(); age < 0 || age > time.Minute {
		t.Errorf("Age %s right after publishing", age)
	}
	// No temp file is left next to the CRL.
	entries, err := os.ReadDir(filepath.Dir(e.path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("the CRL directory holds %d entries, want only the CRL", len(entries))
	}
}

// TestFailedPublishKeepsTheLastCRL: when the file cannot be written, the
// error is returned and the last good CRL stays what is served.
func TestFailedPublishKeepsTheLastCRL(t *testing.T) {
	e := newEnv(t)
	if err := e.pub.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	good := e.pub.Latest()
	e.pub.path = filepath.Join(t.TempDir(), "missing", "crl.pem")
	e.cert(uuid.New(), time.Now().Add(time.Hour), true)
	if err := e.pub.Publish(t.Context()); err == nil {
		t.Fatal("publishing into a missing directory succeeded")
	}
	if !bytes.Equal(e.pub.Latest(), good) {
		t.Fatal("a failed publish changed the served CRL")
	}
}

// TestRunPublishesAtStartAndOnTrigger: Run publishes right away, and again
// on Trigger long before its interval. Trigger never blocks.
func TestRunPublishesAtStartAndOnTrigger(t *testing.T) {
	e := newEnv(t)
	e.pub.Trigger()
	e.pub.Trigger() // a second pending trigger is dropped, not waited on
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.pub.Run(ctx, time.Hour)
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitFor := func(what string, ok func(map[string]bool) bool) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if b := e.pub.Latest(); b != nil {
				if serials, _ := e.parse(b); ok(serials) {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	waitFor("the first CRL", func(map[string]bool) bool { return true })
	serial := e.cert(uuid.New(), time.Now().Add(time.Hour), true)
	e.pub.Trigger()
	waitFor("the CRL with the new revocation", func(s map[string]bool) bool { return s[serial] })
}

// TestRunPublishesATriggerPendingWhenItStops: a revocation triggered just
// before Run is stopped, as by the last request before the API shuts down,
// is in the CRL when Run returns.
func TestRunPublishesATriggerPendingWhenItStops(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.pub.Run(ctx, time.Hour)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for e.pub.Latest() == nil {
		if time.Now().After(deadline) {
			t.Fatal("no first CRL")
		}
		time.Sleep(10 * time.Millisecond)
	}
	serial := e.cert(uuid.New(), time.Now().Add(time.Hour), true)
	e.pub.Trigger()
	cancel()
	<-done
	if serials, _ := e.parse(e.pub.Latest()); !serials[serial] {
		t.Fatal("the revocation triggered before Run stopped is not in the CRL")
	}
}
