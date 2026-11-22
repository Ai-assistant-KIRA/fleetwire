// Package e2e runs against the Compose stack (make up), with a real agent:
// enroll, send, store, ack, segment deletion, query, revoke, export and
// destroy, and a revoke while the agent is still connected and publishing.
// Set FLEETWIRE_E2E=1 to run it (make e2e does).
package e2e

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/collect"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/config"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/checker"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

const (
	apiURL    = "https://localhost:8443"
	deviceURL = "https://localhost:8444"
	brokerURL = "tls://127.0.0.1:18883"
	dbURL     = "postgres://fleetwire_app:app@127.0.0.1:5432/fleetwire?sslmode=disable"
)

func repo(t *testing.T, rel string) string {
	t.Helper()
	return filepath.Join(append([]string{"..", ".."}, strings.Split(rel, "/")...)...)
}

type client struct {
	t    *testing.T
	http *http.Client
}

func newClient(t *testing.T) *client {
	t.Helper()
	pem, err := os.ReadFile(repo(t, "deploy/compose/secrets/ca-bundle.pem"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	return &client{t: t, http: &http.Client{Timeout: time.Minute, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}}
}

func (c *client) do(method, path, token string, body any, want int, out any) http.Header {
	c.t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			c.t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(c.t.Context(), method, apiURL+path, rd)
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("%s %s: read body: %v", method, path, err)
	}
	if resp.StatusCode != want {
		c.t.Fatalf("%s %s: %d, want %d: %s", method, path, resp.StatusCode, want, data)
	}
	if out != nil {
		if b, ok := out.(*[]byte); ok {
			*b = data
		} else if err := json.Unmarshal(data, out); err != nil {
			c.t.Fatal(err)
		}
	}
	return resp.Header
}

func operatorToken(t *testing.T) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "docker", "compose", "-f", repo(t, "deploy/compose/compose.yaml"),
		"run", "--rm", "--no-deps", "-T", "migrate", "create-operator-token", "-name", "e2e-"+uuid.NewString()[:8])
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("create operator token: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func waitFor(t *testing.T, what string, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// brokerHandshake opens a TLS connection to the broker's device listener
// with cert and waits up to 2 s for the broker's verdict on it. With TLS 1.3
// the server checks the client certificate after the client has finished
// its side of the handshake, so a refusal arrives as an alert on the first
// read. It returns nil when the broker kept the connection open.
func brokerHandshake(ctx context.Context, cert tls.Certificate, roots *x509.CertPool) error {
	d := tls.Dialer{Config: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}}
	conn, err := d.DialContext(ctx, "tcp", strings.TrimPrefix(brokerURL, "tls://"))
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	_, err = conn.Read(make([]byte, 1))
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return nil
	}
	if err == nil {
		return errors.New("the broker sent data before an MQTT CONNECT")
	}
	return err
}

func TestEndToEnd(t *testing.T) {
	if os.Getenv("FLEETWIRE_E2E") == "" {
		t.Skip("set FLEETWIRE_E2E=1 with the stack running (make e2e)")
	}
	c := newClient(t)
	op := operatorToken(t)
	var tenant struct {
		ID uuid.UUID `json:"id"`
	}
	c.do("POST", "/v1/admin/tenants", op, map[string]any{"name": "e2e"}, 201, &tenant)
	var tok struct {
		Token string `json:"token"`
	}
	c.do("POST", "/v1/admin/tenants/"+tenant.ID.String()+"/tokens", op, map[string]any{"name": "e2e",
		"scopes": []string{"tenant:admin", "devices:read", "devices:admin", "readings:read", "exports:read", "exports:write", "audit:read"}}, 201, &tok)
	var enroll struct {
		Tokens []struct {
			Token string `json:"token"`
		} `json:"tokens"`
	}
	c.do("POST", "/v1/enrollment-tokens", tok.Token, map[string]any{"count": 1}, 201, &enroll)

	// A real agent, in this process, against the real stack.
	bundle, err := os.ReadFile(repo(t, "deploy/compose/secrets/ca-bundle.pem"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.EnrollURL, cfg.EnrollToken, cfg.EnrollCABundle = deviceURL, enroll.Tokens[0].Token, string(bundle)
	cfg.BrokerURL = brokerURL
	cfg.CollectInterval = 100 * time.Millisecond
	cfg.WALSegmentSize, cfg.WALMaxSize = 128<<10+64<<10, 64<<20
	cfg.WALSync = wal.SyncInterval
	cfg.WALSyncInterval = 200 * time.Millisecond
	cfg.BatchWait = 200 * time.Millisecond
	cfg.MetricsListen = ""
	dir := t.TempDir()
	dev, err := agent.Open(agent.Options{Dir: dir, Config: cfg, Metrics: agent.NewMetrics(prometheus.NewRegistry()),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Version: "e2e",
		Collectors: []collect.Collector{&collect.Synthetic{Readings: 40, Seed: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- dev.Run(ctx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("agent: %v", err)
		}
	}()

	waitFor(t, "enrollment", 30*time.Second, func() bool { return dev.DeviceID() != uuid.Nil })
	id := dev.DeviceID().String()
	// Enough data to roll several segments, then the acked ones must go.
	// A sealed segment is deleted as soon as an ack covers it, so with a
	// fast backend there may never be three at once: count the segment
	// files seen over time instead (one rolls every few seconds here).
	segments := func() []string {
		entries, err := os.ReadDir(filepath.Join(dir, "wal"))
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".seg") {
				names = append(names, e.Name())
			}
		}
		return names
	}
	seen := map[string]bool{}
	waitFor(t, "three segments written", 60*time.Second, func() bool {
		for _, n := range segments() {
			seen[n] = true
		}
		return len(seen) >= 3
	})
	dev.StopCollecting()
	waitFor(t, "every reading acked", 60*time.Second, dev.Drained)
	waitFor(t, "acked segments deleted", 30*time.Second, func() bool { return len(segments()) == 1 })
	st := dev.Status()
	t.Logf("device %s: last seq %d acked %d", id, st.LastSeq, st.Acked)

	var d struct {
		AckedSeq uint64 `json:"acked_seq"`
		Status   string `json:"status"`
	}
	c.do("GET", "/v1/devices/"+id, tok.Token, nil, 200, &d)
	if d.AckedSeq != st.LastSeq || d.Status != "active" {
		t.Fatalf("server says acked %d (%s), device says last seq %d", d.AckedSeq, d.Status, st.LastSeq)
	}
	// The API serves every stored reading back, decrypted, in order.
	var count uint64
	path := "/v1/devices/" + id + "/readings?limit=10000"
	for path != "" {
		var pg struct {
			Items []struct {
				Seq uint64 `json:"seq"`
			} `json:"items"`
			NextCursor string `json:"next_cursor"`
		}
		c.do("GET", path, tok.Token, nil, 200, &pg)
		for _, it := range pg.Items {
			count++
			if it.Seq != count {
				t.Fatalf("reading %d has seq %d", count, it.Seq)
			}
		}
		path = ""
		if pg.NextCursor != "" {
			path = "/v1/devices/" + id + "/readings?limit=10000&cursor=" + pg.NextCursor
		}
	}
	if count != st.LastSeq {
		t.Fatalf("API returned %d readings, device wrote %d", count, st.LastSeq)
	}

	// The checker agrees, reading the database directly.
	db, err := store.Connect(t.Context(), dbURL, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	root, err := keyring.LoadRootKey(repo(t, "deploy/compose/secrets/keyring-root.key"))
	if err != nil {
		t.Fatal(err)
	}
	kr, err := keyring.NewLocal(repo(t, "deploy/compose/.state/keyring"), root)
	if err != nil {
		t.Fatal(err)
	}
	ck := &checker.Checker{DB: db, Keys: tenantkey.New(db, kr, slog.New(slog.DiscardHandler))}
	if rep := ck.Run(t.Context(), []checker.Expectation{{DeviceID: dev.DeviceID(), LastSeq: st.LastSeq}}); !rep.OK() {
		t.Fatalf("checker: %+v", rep)
	}

	// Export, download, verify the digest and the line count.
	var exp struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		DownloadURL string `json:"download_url"`
	}
	c.do("POST", "/v1/exports", tok.Token, map[string]any{"device_id": id}, 202, &exp)
	waitFor(t, "export", 60*time.Second, func() bool {
		c.do("GET", "/v1/exports/"+exp.ID, tok.Token, nil, 200, &exp)
		return exp.Status == "ready"
	})
	var file []byte
	h := c.do("GET", exp.DownloadURL, tok.Token, nil, 200, &file)
	sum := sha256.Sum256(file)
	if h.Get("Content-Digest") != "sha-256=:"+base64.StdEncoding.EncodeToString(sum[:])+":" {
		t.Fatal("Content-Digest does not match the file")
	}
	zr, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	if n := uint64(bytes.Count(plain, []byte("\n"))); n != st.LastSeq {
		t.Fatalf("export has %d lines, want %d", n, st.LastSeq)
	}

	// The device's own certificate, for handshakes of our own with the
	// broker: accepted now, refused after the revoke.
	deviceCert, err := tls.LoadX509KeyPair(filepath.Join(dir, "identity", "cert.pem"), filepath.Join(dir, "identity", "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle) {
		t.Fatal("CA bundle holds no certificates")
	}
	if err := brokerHandshake(t.Context(), deviceCert, roots); err != nil {
		t.Fatalf("the broker refuses the device before the revoke: %v", err)
	}

	// Revoke: nothing more is stored, and the drop is audited.
	c.do("POST", "/v1/devices/"+id+"/revoke", tok.Token, nil, 200, nil)
	var audit []byte
	c.do("GET", "/v1/audit?limit=100", tok.Token, nil, 200, &audit)
	if !bytes.Contains(audit, []byte("device.revoke")) {
		t.Fatalf("revocation not audited: %s", audit)
	}
	// The broker refuses the revoked certificate once Mosquitto has
	// reloaded the CRL: the API publishes it at once, and the watcher in
	// the Mosquitto container sends SIGHUP within a few seconds.
	refused := time.Now().Add(30 * time.Second)
	for {
		err := brokerHandshake(t.Context(), deviceCert, roots)
		if err != nil && strings.Contains(err.Error(), "revoked") {
			break
		}
		if time.Now().After(refused) {
			t.Fatalf("the broker still accepts the revoked certificate after 30 s (last handshake: %v)", err)
		}
		time.Sleep(time.Second)
	}

	// Destroy the tenant: its key is gone, so nothing of it can be read.
	c.do("DELETE", "/v1/admin/tenants/"+tenant.ID.String(), op, nil, 202, nil)
	waitFor(t, "tenant destroyed", 60*time.Second, func() bool {
		var list struct {
			Items []struct {
				ID     uuid.UUID `json:"id"`
				Status string    `json:"status"`
			} `json:"items"`
			NextCursor string `json:"next_cursor"`
		}
		p := "/v1/admin/tenants?limit=1000"
		for p != "" {
			c.do("GET", p, op, nil, 200, &list)
			for _, it := range list.Items {
				if it.ID == tenant.ID {
					return it.Status == "destroyed"
				}
			}
			p = ""
			if list.NextCursor != "" {
				p = "/v1/admin/tenants?limit=1000&cursor=" + list.NextCursor
			}
		}
		return false
	})
	if _, err := os.Stat(filepath.Join(repo(t, "deploy/compose/.state/keyring"), tenant.ID.String()+".kek")); !os.IsNotExist(err) {
		t.Fatalf("tenant KEK still on disk: %v", err)
	}
}

// storedReadings pages through a device's readings in the API and returns
// how many there are and the highest seq.
func (c *client) storedReadings(token, id string) (count, maxSeq uint64) {
	c.t.Helper()
	path := "/v1/devices/" + id + "/readings?limit=10000"
	for path != "" {
		var pg struct {
			Items []struct {
				Seq uint64 `json:"seq"`
			} `json:"items"`
			NextCursor string `json:"next_cursor"`
		}
		c.do("GET", path, token, nil, 200, &pg)
		for _, it := range pg.Items {
			count++
			maxSeq = max(maxSeq, it.Seq)
		}
		path = ""
		if pg.NextCursor != "" {
			path = "/v1/devices/" + id + "/readings?limit=10000&cursor=" + pg.NextCursor
		}
	}
	return count, maxSeq
}

// ingestAdmin are the two ingest instances' admin listeners in Compose.
var ingestAdmin = []string{"http://127.0.0.1:9111", "http://127.0.0.1:9112"}

// ingestDropsRevoked sums fleetwire_ingest_batches_total{result="dropped_revoked"}
// over the ingest instances that answer.
func ingestDropsRevoked(t *testing.T) float64 {
	t.Helper()
	var sum float64
	answered := 0
	for _, base := range ingestAdmin {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"/metrics", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			continue
		}
		answered++
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "fleetwire_ingest_batches_total{") && strings.Contains(line, `result="dropped_revoked"`) {
				f := strings.Fields(line)
				v, err := strconv.ParseFloat(f[len(f)-1], 64)
				if err != nil {
					t.Fatalf("ingest metrics line %q: %v", line, err)
				}
				sum += v
			}
		}
	}
	if answered == 0 {
		t.Fatalf("no ingest admin listener answered (%v)", ingestAdmin)
	}
	return sum
}

// TestRevokeDropsReadingsFromAConnectedDevice covers revocation layer 1
// (R4.6). The broker checks the CRL only when a connection is made, so a
// device that stays connected keeps publishing after it is revoked; ingest
// must store none of it from the revoke's commit on, and audit the drop.
// TestEndToEnd stops its agent before the revoke and so never gets here.
func TestRevokeDropsReadingsFromAConnectedDevice(t *testing.T) {
	if os.Getenv("FLEETWIRE_E2E") == "" {
		t.Skip("set FLEETWIRE_E2E=1 with the stack running (make e2e)")
	}
	c := newClient(t)
	op := operatorToken(t)
	var tenant struct {
		ID uuid.UUID `json:"id"`
	}
	c.do("POST", "/v1/admin/tenants", op, map[string]any{"name": "e2e live revoke"}, 201, &tenant)
	var tok struct {
		Token string `json:"token"`
	}
	c.do("POST", "/v1/admin/tenants/"+tenant.ID.String()+"/tokens", op, map[string]any{"name": "e2e",
		"scopes": []string{"devices:read", "devices:admin", "readings:read", "audit:read"}}, 201, &tok)
	var enroll struct {
		Tokens []struct {
			Token string `json:"token"`
		} `json:"tokens"`
	}
	c.do("POST", "/v1/enrollment-tokens", tok.Token, map[string]any{"count": 1}, 201, &enroll)

	bundle, err := os.ReadFile(repo(t, "deploy/compose/secrets/ca-bundle.pem"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.EnrollURL, cfg.EnrollToken, cfg.EnrollCABundle = deviceURL, enroll.Tokens[0].Token, string(bundle)
	cfg.BrokerURL = brokerURL
	cfg.CollectInterval = 100 * time.Millisecond
	cfg.WALSync, cfg.WALSyncInterval = wal.SyncInterval, 200*time.Millisecond
	cfg.BatchWait = 200 * time.Millisecond
	cfg.MetricsListen = ""
	m := agent.NewMetrics(prometheus.NewRegistry())
	dev, err := agent.Open(agent.Options{Dir: t.TempDir(), Config: cfg, Metrics: m,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Version: "e2e",
		Collectors: []collect.Collector{&collect.Synthetic{Readings: 10, Seed: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- dev.Run(ctx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("agent: %v", err)
		}
	}()
	waitFor(t, "enrollment", 30*time.Second, func() bool { return dev.DeviceID() != uuid.Nil })
	id := dev.DeviceID().String()
	waitFor(t, "readings stored and acked", 60*time.Second, func() bool {
		st := dev.Status()
		return st.Connected && st.Acked >= 100
	})

	// Revoke while the agent keeps collecting. Ingest locks the device row
	// for every batch, so once the revoke has committed (the API answered)
	// nothing more can be stored: what is stored now is final.
	dropsBefore := ingestDropsRevoked(t)
	disconnects := testutil.ToFloat64(m.MQTT.Disconnects)
	c.do("POST", "/v1/devices/"+id+"/revoke", tok.Token, nil, 200, nil)
	batches, before := testutil.ToFloat64(m.Uplink.Batches), dev.Status()
	stored, storedMax := c.storedReadings(tok.Token, id)
	t.Logf("device %s revoked at last seq %d, acked %d; %d readings stored, up to seq %d", id, before.LastSeq, before.Acked, stored, storedMax)

	// The agent publishes on the connection it already has. Mosquitto
	// refuses the certificate only for new connections, after it reloads
	// the CRL a few seconds from now; these batches go out before that.
	waitFor(t, "batches published after the revoke", 30*time.Second, func() bool {
		return testutil.ToFloat64(m.Uplink.Batches) >= batches+3 && dev.Status().LastSeq > before.LastSeq
	})
	if testutil.ToFloat64(m.MQTT.Disconnects) != disconnects {
		t.Fatal("the agent lost its MQTT connection before it published after the revoke; this test needs the connection kept open")
	}

	// Ingest drops those batches and audits the drop for this device.
	waitFor(t, "ingest.drop audited for the device", 60*time.Second, func() bool {
		var audit struct {
			Items []struct {
				Action   string `json:"action"`
				TargetID string `json:"target_id"`
				Result   string `json:"result"`
			} `json:"items"`
		}
		c.do("GET", "/v1/audit?limit=100", tok.Token, nil, 200, &audit)
		for _, a := range audit.Items {
			if a.Action == "ingest.drop" && a.TargetID == id && a.Result == "denied" {
				return true
			}
		}
		return false
	})
	waitFor(t, "dropped_revoked batches counted by ingest", 30*time.Second, func() bool {
		return ingestDropsRevoked(t) > dropsBefore
	})
	// Give the rest of the batches published after the revoke time to pass
	// through the bridge, Kafka and ingest before counting again.
	waitFor(t, "more batches published after the revoke", 30*time.Second, func() bool {
		return testutil.ToFloat64(m.Uplink.Batches) >= batches+10
	})
	time.Sleep(5 * time.Second)
	after := dev.Status()
	if n, hi := c.storedReadings(tok.Token, id); n != stored || hi != storedMax {
		t.Fatalf("readings stored after the revoke: %d stored up to seq %d before, %d up to seq %d now (the device is at seq %d)",
			stored, storedMax, n, hi, after.LastSeq)
	}
	var d struct {
		AckedSeq uint64 `json:"acked_seq"`
		Status   string `json:"status"`
	}
	c.do("GET", "/v1/devices/"+id, tok.Token, nil, 200, &d)
	if d.Status != "revoked" || d.AckedSeq > storedMax {
		t.Fatalf("device after the revoke: %+v, %d readings stored up to seq %d", d, stored, storedMax)
	}
	if after.Acked > storedMax {
		t.Fatalf("the device was acked through seq %d, past the %d stored", after.Acked, storedMax)
	}
	t.Logf("after the revoke the device wrote up to seq %d; none of it was stored", after.LastSeq)
}
