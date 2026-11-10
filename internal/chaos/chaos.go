// Package chaos runs the failure scenarios against the Compose stack and
// writes the results table (R12.2, R12.4, design 15.3).
//
// Each scenario: wait for a healthy stack, create a fresh tenant, start the
// load generator, keep steady load, inject the fault, hold it, recover,
// measure how long until the p99 end-to-end delay is back under 5 s, keep
// load a while longer, stop the load generator (it drains and writes
// results.jsonl), and run the checker. A scenario passes only with 0
// missing and 0 duplicates.
package chaos

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/backoff"
)

// Options for a chaos run.
type Options struct {
	RepoDir  string
	BinDir   string
	OutDir   string
	Devices  int
	Interval time.Duration
	Readings int
	Ramp     string
	Steady   time.Duration
	After    time.Duration
	WALSync  string
	// StateDir holds the devices' local state when set, one subdirectory
	// per scenario (default: next to the scenario's logs in OutDir/raw).
	// Large runs put it on tmpfs; see stateLocation.
	StateDir      string
	APIURL        string
	DeviceAPIURL  string
	BrokerURL     string
	PrometheusURL string
	ToxiproxyURL  string
	LoadgenListen string
	Scenarios     []string
	Log           *slog.Logger
}

// Row is one line of the results table.
type Row struct {
	Scenario   string    `json:"scenario"`
	Devices    int       `json:"devices"`
	Readings   uint64    `json:"readings"`
	Stored     uint64    `json:"stored"`
	Missing    uint64    `json:"missing"`
	Duplicates uint64    `json:"duplicates"`
	GapSeqs    uint64    `json:"gap_seqs"`
	Unrecorded uint64    `json:"unrecorded_gap_seqs"`
	Recovery   string    `json:"recovery"`
	WALSync    string    `json:"wal_sync"`
	Pass       bool      `json:"pass"`
	Note       string    `json:"note,omitempty"`
	Started    time.Time `json:"started"`
	Duration   string    `json:"duration"`
}

type scenario struct {
	name    string
	hold    time.Duration
	inject  func(ctx context.Context, r *run) error
	recover func(ctx context.Context, r *run) error
}

// Scenarios lists every scenario in table order.
func Scenarios() []string {
	out := make([]string, len(all))
	for i, s := range all {
		out[i] = s.name
	}
	return out
}

var all = []scenario{
	{"network-cut", 60 * time.Second,
		func(ctx context.Context, r *run) error {
			return r.toxi(ctx, "POST", "/proxies/mqtt", `{"enabled": false}`)
		},
		func(ctx context.Context, r *run) error {
			return r.toxi(ctx, "POST", "/proxies/mqtt", `{"enabled": true}`)
		}},
	{"network-degraded", 120 * time.Second,
		func(ctx context.Context, r *run) error {
			for _, t := range []string{
				`{"name": "latency_down", "type": "latency", "stream": "downstream", "attributes": {"latency": 300, "jitter": 200}}`,
				`{"name": "latency_up", "type": "latency", "stream": "upstream", "attributes": {"latency": 300, "jitter": 200}}`,
				`{"name": "bandwidth", "type": "bandwidth", "stream": "downstream", "attributes": {"rate": 512}}`,
				`{"name": "reset", "type": "reset_peer", "stream": "upstream", "toxicity": 0.05, "attributes": {"timeout": 10000}}`,
			} {
				if err := r.toxi(ctx, "POST", "/proxies/mqtt/toxics", t); err != nil {
					return err
				}
			}
			return nil
		},
		func(ctx context.Context, r *run) error {
			var errs []error
			for _, n := range []string{"latency_down", "latency_up", "bandwidth", "reset"} {
				errs = append(errs, r.toxi(ctx, "DELETE", "/proxies/mqtt/toxics/"+n, ""))
			}
			return errors.Join(errs...)
		}},
	{"mosquitto-restart", 0,
		func(ctx context.Context, r *run) error { return r.compose(ctx, "restart", "mosquitto") },
		func(context.Context, *run) error { return nil }},
	{"kafka-broker-kill", 60 * time.Second,
		func(ctx context.Context, r *run) error { return r.compose(ctx, "kill", "-s", "KILL", "kafka-2") },
		func(ctx context.Context, r *run) error { return r.compose(ctx, "start", "kafka-2") }},
	{"kafka-outage", 60 * time.Second,
		func(ctx context.Context, r *run) error {
			return r.compose(ctx, "stop", "kafka-1", "kafka-2", "kafka-3")
		},
		func(ctx context.Context, r *run) error {
			return r.compose(ctx, "start", "kafka-1", "kafka-2", "kafka-3")
		}},
	{"postgres-outage", 60 * time.Second,
		func(ctx context.Context, r *run) error { return r.compose(ctx, "kill", "-s", "KILL", "postgres") },
		func(ctx context.Context, r *run) error { return r.compose(ctx, "start", "postgres") }},
	{"bridge-kill", 30 * time.Second,
		func(ctx context.Context, r *run) error { return r.compose(ctx, "kill", "-s", "KILL", "bridge-1") },
		func(ctx context.Context, r *run) error { return r.compose(ctx, "start", "bridge-1") }},
	{"ingest-kill", 30 * time.Second,
		func(ctx context.Context, r *run) error { return r.compose(ctx, "kill", "-s", "KILL", "ingest-1") },
		func(ctx context.Context, r *run) error { return r.compose(ctx, "start", "ingest-1") }},
	{"agent-kill", 10 * time.Second,
		func(_ context.Context, r *run) error { return r.loadgen.Process.Signal(syscall.SIGKILL) },
		func(ctx context.Context, r *run) error {
			_ = r.loadgen.Wait()
			return r.startLoadgen(ctx)
		}},
}

// repoPath joins a slash-separated repository path onto the repo root.
func (o Options) repoPath(rel string) string {
	return filepath.Join(append([]string{o.RepoDir}, strings.Split(rel, "/")...)...)
}

type run struct {
	o   Options
	dir string
	// devDir is the load generator's state directory.
	devDir    string
	tenantTok string
	loadgen   *exec.Cmd
	logFile   *os.File
	http      *http.Client
	operator  string
}

// Run executes the selected scenarios and writes the results.
func Run(ctx context.Context, o Options) ([]Row, error) {
	bundle, err := os.ReadFile(o.repoPath("deploy/compose/secrets/ca-bundle.pem"))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(bundle) {
		return nil, errors.New("no CA certificates")
	}
	base := &run{o: o, http: &http.Client{Timeout: time.Minute, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}}
	if base.operator, err = base.operatorToken(ctx); err != nil {
		return nil, fmt.Errorf("operator token: %w", err)
	}
	stamp := time.Now().UTC().Format("2006-01-02T150405Z")
	// The commit is read once, before the first scenario: a run takes about
	// an hour, and a checkout in the meantime must not rename the table or
	// split it in two.
	sha := gitSHA(ctx, o.RepoDir)
	rawDir := filepath.Join(o.OutDir, "raw", stamp)
	var rows []Row
	for _, name := range o.Scenarios {
		var sc *scenario
		for i := range all {
			if all[i].name == name {
				sc = &all[i]
			}
		}
		if sc == nil {
			return rows, fmt.Errorf("unknown scenario %q (known: %s)", name, strings.Join(Scenarios(), ", "))
		}
		r := *base
		r.dir = filepath.Join(rawDir, name)
		r.devDir = filepath.Join(r.dir, "devices")
		if o.StateDir != "" {
			r.devDir = filepath.Join(o.StateDir, stamp, name)
		}
		row, err := r.scenario(ctx, sc)
		if err != nil {
			row.Note = err.Error()
			o.Log.Error("scenario failed to run", "scenario", name, "err", err)
		}
		rows = append(rows, row)
		o.Log.Info("scenario done", "scenario", name, "pass", row.Pass, "missing", row.Missing, "duplicates", row.Duplicates, "recovery", row.Recovery)
		// A scenario that could not run is a FAIL row, not an abort: the
		// remaining scenarios still run.
		if werr := writeResults(o, stamp, sha, rows); werr != nil {
			return rows, werr
		}
		if ctx.Err() != nil {
			return rows, ctx.Err()
		}
	}
	return rows, nil
}

// The results are named so that the deferred duration lands in the row that
// is returned, not in a local copy.
func (r *run) scenario(ctx context.Context, sc *scenario) (row Row, err error) {
	row = Row{Scenario: sc.name, Devices: r.o.Devices, WALSync: r.o.WALSync, Started: time.Now().UTC(), Recovery: "-"}
	start := time.Now()
	defer func() { row.Duration = time.Since(start).Round(time.Second).String() }()
	if err := os.MkdirAll(r.dir, 0o700); err != nil {
		return row, err
	}
	if err := os.MkdirAll(r.devDir, 0o700); err != nil {
		return row, err
	}
	r.o.Log.Info("scenario start", "scenario", sc.name, "devices", r.o.Devices)
	if err := r.waitHealthy(ctx, 5*time.Minute); err != nil {
		return row, err
	}
	// Start from a clean network whatever an earlier scenario left behind.
	if err := r.toxi(ctx, http.MethodPost, "/reset", ""); err != nil {
		return row, fmt.Errorf("reset toxiproxy: %w", err)
	}
	if err := r.newTenant(ctx, sc.name); err != nil {
		return row, err
	}
	if err := r.startLoadgen(ctx); err != nil {
		return row, err
	}
	defer func() {
		if r.loadgen != nil && r.loadgen.ProcessState == nil {
			_ = r.loadgen.Process.Kill()
		}
		if r.logFile != nil {
			_ = r.logFile.Close()
		}
	}()
	if err := r.waitDevicesRunning(ctx); err != nil {
		return row, err
	}
	sleep(ctx, r.o.Steady)
	r.o.Log.Info("injecting fault", "scenario", sc.name, "hold", sc.hold)
	if err := sc.inject(ctx, r); err != nil {
		return row, fmt.Errorf("inject: %w", err)
	}
	sleep(ctx, sc.hold)
	if err := sc.recover(ctx, r); err != nil {
		return row, fmt.Errorf("recover: %w", err)
	}
	recovered := time.Now()
	if d, err := r.waitRecovered(ctx, 10*time.Minute); err == nil {
		row.Recovery = d.Round(time.Second).String()
	} else {
		row.Recovery = "> 10m"
		row.Note = err.Error()
	}
	if left := r.o.After - time.Since(recovered); left > 0 {
		sleep(ctx, left)
	}
	if err := r.stopLoadgen(); err != nil {
		return row, err
	}
	if err := r.waitHealthy(ctx, 5*time.Minute); err != nil {
		return row, err
	}
	row, err = r.check(ctx, row)
	if err != nil {
		return row, err
	}
	return row, r.pruneDevices()
}

// pruneDevices deletes the devices' local logs and keys once the checker
// has read results.jsonl. They are about nine files per device, so a
// 2,000-device suite left 160,000 files, and a few runs, copied into
// Docker build contexts as well, used up every inode of the test VM's
// disk. results.jsonl is kept next to the loadgen log and the checker's
// report in raw/.
func (r *run) pruneDevices() error {
	results, err := os.ReadFile(filepath.Join(r.devDir, "results.jsonl"))
	if err != nil {
		return err
	}
	//nolint:gosec // G703: both paths are this run's own directories, built above.
	if err := os.WriteFile(filepath.Join(r.dir, "results.jsonl"), results, 0o600); err != nil {
		return err
	}
	if err := os.RemoveAll(r.devDir); err != nil {
		return fmt.Errorf("prune device state: %w", err)
	}
	if r.o.StateDir != "" {
		// The run's directory under StateDir goes once its last scenario is
		// pruned; until then it is not empty.
		if err := os.Remove(filepath.Dir(r.devDir)); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("prune device state: %w", err)
		}
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (r *run) composeCmd(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", r.o.repoPath("deploy/compose/compose.yaml")}, args...)...) //nolint:gosec // G204: fixed program, arguments from this package
	cmd.Env = append(os.Environ(), "FW_UID="+strconv.Itoa(os.Getuid()), "FW_GID="+strconv.Itoa(os.Getgid()))
	return cmd
}

func (r *run) compose(ctx context.Context, args ...string) error {
	out, err := r.composeCmd(ctx, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return nil
}

func (r *run) operatorToken(ctx context.Context) (string, error) {
	cmd := r.composeCmd(ctx, "run", "--rm", "--no-deps", "-T", "migrate", "create-operator-token", "-name", "chaos-"+strconv.FormatInt(time.Now().Unix(), 10))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, stderr.String())
	}
	return strings.TrimSpace(string(out)), nil
}

func (r *run) api(ctx context.Context, method, path, token string, body any, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, r.o.APIURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, data)
	}
	return json.Unmarshal(data, out)
}

func (r *run) newTenant(ctx context.Context, name string) error {
	var t struct {
		ID string `json:"id"`
	}
	if err := r.api(ctx, "POST", "/v1/admin/tenants", r.operator, map[string]any{"name": "chaos " + name + " " + time.Now().UTC().Format(time.RFC3339)}, &t); err != nil {
		return err
	}
	var tok struct {
		Token string `json:"token"`
	}
	if err := r.api(ctx, "POST", "/v1/admin/tenants/"+t.ID+"/tokens", r.operator,
		map[string]any{"name": "chaos", "scopes": []string{"devices:admin", "devices:read", "readings:read"}}, &tok); err != nil {
		return err
	}
	r.tenantTok = tok.Token
	return nil
}

func (r *run) startLoadgen(ctx context.Context) error {
	if r.logFile == nil {
		f, err := os.OpenFile(filepath.Join(r.dir, "loadgen.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		r.logFile = f
	}
	cmd := exec.CommandContext(context.WithoutCancel(ctx), filepath.Join(r.o.BinDir, "fleetwire-loadgen"), "run", //nolint:gosec // G204: our own binary
		"-devices", strconv.Itoa(r.o.Devices), "-state-dir", r.devDir,
		"-interval", r.o.Interval.String(), "-readings", strconv.Itoa(r.o.Readings), "-duration", "24h",
		"-ramp", r.o.Ramp, "-api", r.o.APIURL, "-device-api", r.o.DeviceAPIURL, "-broker", r.o.BrokerURL,
		"-ca-file", r.o.repoPath("deploy/compose/secrets/ca-bundle.pem"),
		"-wal-sync", r.o.WALSync, "-drain-timeout", "10m", "-metrics-listen", r.o.LoadgenListen, "-log-level", "warn")
	cmd.Env = append(os.Environ(), "FLEETWIRE_TENANT_TOKEN="+r.tenantTok)
	cmd.Stdout, cmd.Stderr = r.logFile, r.logFile
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start loadgen: %w", err)
	}
	r.loadgen = cmd
	return nil
}

func (r *run) stopLoadgen() error {
	if err := r.loadgen.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	if err := r.loadgen.Wait(); err != nil {
		return fmt.Errorf("loadgen exited: %w (see %s)", err, filepath.Join(r.dir, "loadgen.log"))
	}
	return nil
}

func (r *run) check(ctx context.Context, row Row) (Row, error) {
	report := filepath.Join(r.dir, "checker.json")
	cmd := exec.CommandContext(ctx, filepath.Join(r.o.BinDir, "fleetwire-checker"), //nolint:gosec // G204: our own binary
		"-results", filepath.Join(r.devDir, "results.jsonl"), "-json-out", report,
		"-keyring-dir", r.o.repoPath("deploy/compose/.state/keyring"),
		"-keyring-root-key", r.o.repoPath("deploy/compose/secrets/keyring-root.key"))
	out, err := cmd.CombinedOutput()
	if werr := os.WriteFile(filepath.Join(r.dir, "checker.txt"), out, 0o600); werr != nil {
		return row, werr
	}
	var exit *exec.ExitError
	if err != nil && (!errors.As(err, &exit) || exit.ExitCode() != 1) {
		return row, fmt.Errorf("checker: %w: %s", err, out)
	}
	data, err := os.ReadFile(report)
	if err != nil {
		return row, err
	}
	var rep struct {
		Readings, Stored, Missing, Duplicates, GapSeqs uint64
		Unrecorded                                     uint64 `json:"unrecorded_gap_seqs"`
		Errors                                         int
	}
	if err := json.Unmarshal(data, &rep); err != nil {
		return row, err
	}
	row.Readings, row.Stored, row.Missing, row.Duplicates, row.GapSeqs, row.Unrecorded = rep.Readings, rep.Stored, rep.Missing, rep.Duplicates, rep.GapSeqs, rep.Unrecorded
	row.Pass = rep.Missing == 0 && rep.Duplicates == 0 && rep.Unrecorded == 0 && rep.Errors == 0 && rep.Readings > 0
	return row, nil
}

// toxi calls the Toxiproxy API and retries for about a minute. A fault
// that is never removed would break every later scenario, and under load
// a reused keep-alive connection can fail with EOF. Retrying is safe:
// setting a proxy's state is idempotent, adding a toxic that already exists
// answers 409, and deleting one that is gone answers 404; both count as done.
func (r *run) toxi(ctx context.Context, method, path, body string) error {
	bo := backoff.Backoff{Base: 500 * time.Millisecond, Max: 5 * time.Second}
	var err error
	for attempt := range 20 {
		if err = r.toxiOnce(ctx, method, path, body); err == nil {
			return nil
		}
		r.o.Log.Warn("toxiproxy call failed; retrying", "method", method, "path", path, "attempt", attempt, "err", err)
		if backoff.Sleep(ctx, bo.Delay(attempt)) != nil {
			break
		}
	}
	return err
}

func (r *run) toxiOnce(ctx context.Context, method, path, body string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, r.o.ToxiproxyURL+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("toxiproxy %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode < 300:
		return nil
	case method == http.MethodPost && resp.StatusCode == http.StatusConflict,
		method == http.MethodDelete && resp.StatusCode == http.StatusNotFound:
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("toxiproxy %s %s: HTTP %d: %s", method, path, resp.StatusCode, b)
}

type psEntry struct {
	Service string `json:"Service"`
	State   string `json:"State"`
	Health  string `json:"Health"`
}

func (r *run) waitHealthy(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		out, err := r.composeCmd(ctx, "ps", "--format", "json").Output()
		bad := []string{}
		if err == nil {
			for _, line := range bytes.Split(bytes.TrimSpace(out), []byte("\n")) {
				var e psEntry
				if json.Unmarshal(line, &e) != nil {
					continue
				}
				if e.State != "running" || (e.Health != "" && e.Health != "healthy") {
					bad = append(bad, e.Service+"="+e.State+"/"+e.Health)
				}
			}
		}
		if err == nil && len(bad) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("docker compose ps: %w", err)
			}
			return fmt.Errorf("stack not healthy after %s: %v", timeout, bad)
		}
		sleep(ctx, 3*time.Second)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (r *run) waitDevicesRunning(ctx context.Context) error {
	deadline := time.Now().Add(15 * time.Minute)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		v, err := r.promValue(ctx, `max(fleetwire_loadgen_devices_running)`)
		if err == nil && int(v) >= r.o.Devices {
			return nil
		}
		if r.loadgen.ProcessState != nil {
			return fmt.Errorf("loadgen exited during start")
		}
		sleep(ctx, 3*time.Second)
	}
	return fmt.Errorf("not all %d devices started in time", r.o.Devices)
}

// waitRecovered waits until the p99 end-to-end delay of newly stored
// readings is under 5 s.
func (r *run) waitRecovered(ctx context.Context, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	q := `histogram_quantile(0.99, sum(rate(fleetwire_ingest_e2e_delay_seconds_bucket[20s])) by (le))`
	for time.Since(start) < timeout && ctx.Err() == nil {
		v, err := r.promValue(ctx, q)
		if err == nil && !math.IsNaN(v) && v < 5 {
			return time.Since(start), nil
		}
		sleep(ctx, 2*time.Second)
	}
	return 0, fmt.Errorf("p99 delay not under 5s within %s", timeout)
}

func (r *run) promValue(ctx context.Context, query string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.o.PrometheusURL+"/api/v1/query?query="+url.QueryEscape(query), nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var body struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	if len(body.Data.Result) == 0 {
		return math.NaN(), nil
	}
	s, _ := body.Data.Result[0].Value[1].(string)
	return strconv.ParseFloat(s, 64)
}
