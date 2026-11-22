// Package loadgen runs many devices in one process.
//
// Every instance is an agent.Device built from the real agent packages: its
// own local log on disk, its own key and certificate from the real
// enrollment API, and its own mutual-TLS MQTT connection. The only generated
// part is the reading values (collect.Synthetic). Differences from a real
// device are listed in docs/explanation/load-generator.md.
package loadgen

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/collect"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/config"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

// Options for a run.
type Options struct {
	Devices      int
	StateDir     string
	Interval     time.Duration
	Readings     int
	Duration     time.Duration
	RampPerSec   float64
	APIURL       string // public API, for enrollment tokens
	DeviceAPIURL string // device API, for enrollment
	TenantToken  string
	CABundle     []byte
	BrokerURL    string
	BatchSize    int
	BatchWait    time.Duration
	AckTimeout   time.Duration
	MaxInflight  int
	WALSync      wal.SyncMode
	WALSyncEvery time.Duration
	WALMaxSize   int64
	DrainTimeout time.Duration
	Version      string
}

// Result is one line of results.jsonl.
type Result struct {
	Index    int       `json:"index"`
	DeviceID string    `json:"device_id"`
	LastSeq  uint64    `json:"last_seq"`
	Acked    uint64    `json:"acked"`
	Gaps     []wal.Gap `json:"gaps"`
	Drained  bool      `json:"drained"`
}

// Metrics of the generator itself; the devices share agent.Metrics.
type Metrics struct {
	Running    prometheus.Gauge
	Enrolled   prometheus.Counter
	Failed     prometheus.Counter
	OpenFailed prometheus.Counter
}

// Stats counts the device instances of this process. The load generator
// serves them at /status, and the chaos runner reads them there rather
// than from Prometheus, which scrapes every few seconds and right after a
// restart can still hold the killed process's numbers.
type Stats struct {
	// Devices is the number of instances asked for (-devices).
	Devices int   `json:"devices"`
	Started int64 `json:"started"`
	Running int64 `json:"running"`
	// OpenFailures are instances that never ran: their state could not be
	// opened or no enrollment token could be fetched. They are missing
	// from results.jsonl, so the checker cannot see them.
	OpenFailures int64 `json:"open_failures"`
	// RunFailures are instances that stopped with an error.
	RunFailures int64 `json:"run_failures"`
}

// Runner owns the devices.
type Runner struct {
	opts    Options
	log     *slog.Logger
	agentM  *agent.Metrics
	m       Metrics
	mu      sync.Mutex
	devices []*agent.Device
	tokens  []string

	started, running, openFailed, runFailed atomic.Int64
}

// New prepares a runner.
func New(opts Options, log *slog.Logger, reg prometheus.Registerer) *Runner {
	r := &Runner{opts: opts, log: log, agentM: agent.NewMetrics(reg)}
	r.m = Metrics{
		Running:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "fleetwire_loadgen_devices_running", Help: "Device instances running."}),
		Enrolled: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_loadgen_devices_started_total", Help: "Device instances started."}),
		Failed:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_loadgen_device_failures_total", Help: "Device instances that stopped with an error."}),
		OpenFailed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_loadgen_device_open_failures_total",
			Help: "Device instances that could not be opened or given an enrollment token, and never ran."}),
	}
	reg.MustRegister(r.m.Running, r.m.Enrolled, r.m.Failed, r.m.OpenFailed)
	return r
}

// Stats returns the current counts.
func (r *Runner) Stats() Stats {
	return Stats{Devices: r.opts.Devices, Started: r.started.Load(), Running: r.running.Load(),
		OpenFailures: r.openFailed.Load(), RunFailures: r.runFailed.Load()}
}

// ServeStatus serves Stats as JSON (GET /status on the metrics listener).
func (r *Runner) ServeStatus(w http.ResponseWriter, _ *http.Request) {
	b, err := json.Marshal(r.Stats())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

// enrollmentToken returns a fresh token, fetching them from the API in
// batches of up to 1000.
func (r *Runner) enrollmentToken(ctx context.Context, need int) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.tokens) == 0 {
		n := min(max(need, 1), 1000)
		toks, err := fetchTokens(ctx, r.opts, n)
		if err != nil {
			return "", err
		}
		r.tokens = toks
	}
	t := r.tokens[0]
	r.tokens = r.tokens[1:]
	return t, nil
}

func httpClient(bundle []byte) (*http.Client, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(bundle) {
		return nil, errors.New("CA bundle holds no certificates")
	}
	return &http.Client{Timeout: time.Minute, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}, nil
}

func fetchTokens(ctx context.Context, o Options, n int) ([]string, error) {
	c, err := httpClient(o.CABundle)
	if err != nil {
		return nil, err
	}
	// Tokens live 24 hours so a long ramp never outlives them.
	body, err := json.Marshal(map[string]int{"count": n, "ttl_seconds": 86400})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(o.APIURL, "/")+"/v1/enrollment-tokens", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+o.TenantToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request enrollment tokens: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("enrollment tokens: HTTP %d: %s", resp.StatusCode, data)
	}
	var out struct {
		Tokens []struct {
			Token string `json:"token"`
		} `json:"tokens"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	list := make([]string, len(out.Tokens))
	for i, t := range out.Tokens {
		list[i] = t.Token
	}
	return list, nil
}

func (r *Runner) deviceConfig(token string) config.Config {
	c := config.Defaults()
	o := r.opts
	c.EnrollURL, c.EnrollToken, c.EnrollCABundle = o.DeviceAPIURL, token, string(o.CABundle)
	c.BrokerURL = o.BrokerURL
	c.CollectInterval = o.Interval
	c.WALSync, c.WALSyncInterval = o.WALSync, o.WALSyncEvery
	if o.WALMaxSize > 0 {
		c.WALMaxSize = o.WALMaxSize
	}
	// Small segments: a load-generator device writes little, and thousands
	// of 4 MiB files would only waste disk.
	c.WALSegmentSize = min(c.WALSegmentSize, 256<<10)
	if c.WALMaxSize < 2*c.WALSegmentSize {
		c.WALMaxSize = 2 * c.WALSegmentSize
	}
	c.BatchSize, c.BatchWait, c.AckTimeout, c.MaxInflight = o.BatchSize, o.BatchWait, o.AckTimeout, o.MaxInflight
	c.MetricsListen = ""
	return c
}

// Run starts the devices at the ramp rate, collects for Duration (or until
// ctx ends), then stops collecting, waits for the drain, and writes
// results.jsonl. It returns an error, after writing results.jsonl, when a
// device could not be opened: the run then has fewer devices than asked
// for, and the ones missing are not in the results.
func (r *Runner) Run(ctx context.Context) error {
	if err := os.MkdirAll(r.opts.StateDir, 0o700); err != nil {
		return err
	}
	runCtx, stopDevices := context.WithCancel(context.WithoutCancel(ctx))
	defer stopDevices()
	var wg sync.WaitGroup
	collectEnd := time.NewTimer(r.opts.Duration)
	defer collectEnd.Stop()

	tick := time.NewTicker(time.Duration(float64(time.Second) / r.opts.RampPerSec))
	defer tick.Stop()
	r.log.Info("starting devices", "devices", r.opts.Devices, "ramp_per_sec", r.opts.RampPerSec, "state_dir", r.opts.StateDir)
ramp:
	for i := 0; i < r.opts.Devices; i++ {
		select {
		case <-ctx.Done():
			break ramp
		case <-collectEnd.C:
			break ramp
		case <-tick.C:
		}
		dev, err := r.open(ctx, i)
		if err != nil {
			r.openFailed.Add(1)
			r.m.OpenFailed.Inc()
			r.log.Error("device did not start", "index", i, "err", err)
			continue
		}
		r.mu.Lock()
		r.devices = append(r.devices, dev)
		r.mu.Unlock()
		r.started.Add(1)
		r.running.Add(1)
		r.m.Enrolled.Inc()
		r.m.Running.Inc()
		wg.Go(func() {
			defer func() {
				r.running.Add(-1)
				r.m.Running.Dec()
			}()
			if err := dev.Run(runCtx); err != nil {
				r.runFailed.Add(1)
				r.m.Failed.Inc()
				r.log.Error("device stopped with an error", "index", i, "err", err)
			}
		})
	}
	r.log.Info("all devices started", "started", r.started.Load(), "failed", r.openFailed.Load())
	select {
	case <-ctx.Done():
	case <-collectEnd.C:
	}

	r.log.Info("stopping collection; draining", "timeout", r.opts.DrainTimeout)
	r.mu.Lock()
	devs := append([]*agent.Device(nil), r.devices...)
	r.mu.Unlock()
	for _, d := range devs {
		d.StopCollecting()
	}
	deadline := time.Now().Add(r.opts.DrainTimeout)
	for time.Now().Before(deadline) {
		left := 0
		for _, d := range devs {
			if !d.Drained() {
				left++
			}
		}
		if left == 0 {
			break
		}
		r.log.Info("waiting for acks", "devices_not_drained", left)
		time.Sleep(2 * time.Second)
	}
	results := make([]Result, len(devs))
	for i, d := range devs {
		st := d.Status()
		results[i] = Result{Index: i, DeviceID: st.DeviceID, LastSeq: st.LastSeq, Acked: st.Acked, Gaps: st.Gaps, Drained: d.Drained()}
	}
	stopDevices()
	wg.Wait()
	if err := writeResults(filepath.Join(r.opts.StateDir, "results.jsonl"), results); err != nil {
		return err
	}
	if n := r.openFailed.Load(); n > 0 {
		return fmt.Errorf("%d of %d devices did not start (see the log); results.jsonl lists only the %d that ran", n, r.opts.Devices, len(results))
	}
	return nil
}

// open restores device i from its state directory, or enrolls a new one.
func (r *Runner) open(ctx context.Context, i int) (*agent.Device, error) {
	dir := filepath.Join(r.opts.StateDir, strconv.Itoa(i))
	token := ""
	if _, err := os.Stat(filepath.Join(dir, "identity", "device.json")); errors.Is(err, os.ErrNotExist) {
		t, err := r.enrollmentToken(ctx, r.opts.Devices-i)
		if err != nil {
			return nil, err
		}
		token = t
	}
	return agent.Open(agent.Options{
		Dir: dir, Config: r.deviceConfig(token), Metrics: r.agentM, Logger: r.log.With("index", i),
		Collectors: []collect.Collector{&collect.Synthetic{Readings: r.opts.Readings, Seed: uint64(i)}},
		Version:    r.opts.Version, EnrollRetry: 5 * time.Second,
	})
}

func writeResults(path string, rs []Result) error {
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, r := range rs {
		if err := enc.Encode(r); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// ReadResults parses results.jsonl.
func ReadResults(path string) ([]Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Result
	dec := json.NewDecoder(f)
	for {
		var r Result
		if err := dec.Decode(&r); errors.Is(err, io.EOF) {
			return out, nil
		} else if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out = append(out, r)
	}
}
