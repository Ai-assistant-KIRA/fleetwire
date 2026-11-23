// Command fleetwire-agent is the device agent.
//
//	fleetwire-agent [run]           collect, store and send readings (the daemon)
//	fleetwire-agent config check    validate the configuration (snap configure hook)
//	fleetwire-agent config docs     print the configuration reference
//	fleetwire-agent status [-json]  last seq, acked seq, gaps, certificate expiry
//	fleetwire-agent version
//	fleetwire-agent prepare-state   create or migrate $SNAP_DATA/state (snap hooks, as root)
//	fleetwire-agent drop-privileges CMD...  become snap_daemon, then exec CMD (snap command chain)
//
// Inside the snap, configuration comes from `snapctl get -d` and state lives
// in $SNAP_DATA/state, owned by the snap_daemon user the daemon runs as.
// Outside it, use -data-dir, -config file.json and -set key=value.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/collect"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/config"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/statedir"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/buildinfo"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/telemetry"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "fleetwire-agent:", err)
		os.Exit(1)
	}
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

type env struct {
	inSnap  bool
	dataDir string
	cfgFile string
	sets    multi
}

func (e *env) load(ctx context.Context) (config.Config, error) {
	if e.inSnap {
		return config.FromSnap(ctx)
	}
	return config.FromFile(e.cfgFile, e.sets)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cmd := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	if cmd == "config" {
		if len(args) == 0 {
			return errors.New("config needs a subcommand: check or docs")
		}
		cmd, args = "config "+args[0], args[1:]
	}
	if cmd == "drop-privileges" {
		// Before flag parsing: everything after it is the command to run.
		return dropPrivileges(args)
	}
	e := &env{inSnap: os.Getenv("SNAP") != "" && os.Getenv("SNAP_DATA") != ""}
	fs := flag.NewFlagSet("fleetwire-agent "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	if e.inSnap {
		e.dataDir = filepath.Join(os.Getenv("SNAP_DATA"), statedir.Name)
	} else {
		fs.StringVar(&e.dataDir, "data-dir", "/var/lib/fleetwire-agent", "State directory (identity and local log).")
		fs.StringVar(&e.cfgFile, "config", "", "JSON configuration file.")
		fs.Var(&e.sets, "set", "Override one key, for example -set collect.interval=1s (repeatable).")
	}
	jsonOut := fs.Bool("json", false, "Print JSON (status).")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch cmd {
	case "version":
		fmt.Fprintln(stdout, buildinfo.Get())
		return nil
	case "config docs":
		fmt.Fprint(stdout, config.Markdown())
		return nil
	case "config check":
		if _, err := e.load(ctx); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "configuration is valid")
		return nil
	case "status":
		return status(ctx, e, *jsonOut, stdout)
	case "run":
		return daemon(ctx, e, stderr)
	case "prepare-state":
		return prepareState(e, stderr) //nolint:contextcheck // it reports health with SnapHealth, which bounds its own snapctl call, as in daemon
	}
	return fmt.Errorf("unknown command %q", cmd)
}

func daemon(ctx context.Context, e *env, logOut io.Writer) error {
	if e.inSnap && os.Geteuid() == 0 {
		// Files it wrote would be root's, and the snap_daemon user could
		// not open them at the next start.
		return errors.New("the daemon must not run as root in the snap: start it with snap start, whose command chain drops to snap_daemon")
	}
	cfg, err := e.load(ctx)
	if err != nil {
		return err
	}
	lg, err := telemetry.NewLogger(logOut, "fleetwire-agent", cfg.LogLevel)
	if err != nil {
		return err
	}
	lg.Info("starting", "build", buildinfo.Get().String(), "data_dir", e.dataDir, "snap", e.inSnap)
	var health agent.Health = agent.NoHealth{}
	if e.inSnap {
		health = &agent.SnapHealth{Log: lg}
	}
	if err := os.MkdirAll(e.dataDir, 0o700); err != nil {
		health.Set("error", "data directory is not writable")
		return fmt.Errorf("data dir: %w", err)
	}
	reg := newRegistry(e.inSnap)
	m := agent.NewMetrics(reg)
	dev, err := agent.Open(agent.Options{
		Dir: e.dataDir, Config: cfg, Collectors: collect.Builtin("/", e.dataDir), Metrics: m,
		Logger: lg, Health: health, Version: buildinfo.Get().Version,
	})
	if err != nil {
		health.Set("error", "cannot open state: see the agent log")
		return err
	}
	if cfg.MetricsListen != "" {
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", cfg.MetricsListen)
		if err != nil {
			return fmt.Errorf("metrics listener: %w", err)
		}
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(struct {
				agent.Status
				Running bool `json:"running"`
			}{dev.Status(), true})
		})
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				lg.Error("metrics server failed", "err", err)
			}
		}()
		defer func() {
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		}()
	}
	err = dev.Run(ctx)
	lg.Info("stopped", "err", err, "status", dev.Status())
	return err
}

// status asks the running daemon; when none answers it inspects the files
// read-only, which is what the power-off test does right after a reboot.
func status(ctx context.Context, e *env, asJSON bool, out io.Writer) error {
	listen := "127.0.0.1:9465"
	if cfg, err := e.load(ctx); err == nil && cfg.MetricsListen != "" {
		listen = cfg.MetricsListen
	}
	var st map[string]any
	if body, err := getStatus(ctx, dialAddr(listen)); err == nil {
		if err := json.Unmarshal(body, &st); err != nil {
			return fmt.Errorf("decode status: %w", err)
		}
	} else {
		in, ierr := wal.Inspect(filepath.Join(e.dataDir, "wal"))
		if ierr != nil {
			hint := ""
			if errors.Is(ierr, fs.ErrPermission) {
				hint = " (the state belongs to the daemon's user: run status with sudo)"
			}
			return fmt.Errorf("agent not reachable (%w) and state unreadable%s: %w", err, hint, ierr)
		}
		// The daemon is this same binary, so its version is the
		// installed one.
		st = map[string]any{
			"running": false, "device_id": in.State.DeviceID, "last_seq": max(in.LastSeq, in.State.MaxSeqEver),
			"acked": in.State.Acked, "pending_gaps": in.State.PendingGaps, "gaps": in.State.GapHistory,
			"wal_bytes": in.Bytes, "wal_segments": in.Segments, "version": buildinfo.Get().Version,
		}
	}
	if asJSON {
		b, err := json.MarshalIndent(st, "", "  ")
		if err != nil {
			return fmt.Errorf("encode status: %w", err)
		}
		_, err = fmt.Fprintln(out, string(b))
		return err
	}
	for _, k := range []string{"running", "version", "device_id", "connected", "last_seq", "acked", "pending_gaps", "gaps", "wal_bytes", "wal_segments", "cert_not_after"} {
		if v, ok := st[k]; ok {
			fmt.Fprintf(out, "%-15s %v\n", k+":", v)
		}
	}
	return nil
}

// prepareState creates $SNAP_DATA/state for the snap_daemon user, moving
// the identity and log of v0.1.1 and earlier into it (see statedir). The
// install and post-refresh hooks run it as root before snapd starts the
// daemon, and the daemon's command chain runs it again before each start.
func prepareState(e *env, logOut io.Writer) error {
	if !e.inSnap {
		return errors.New("prepare-state works only inside the snap; outside it, -data-dir is used as it is")
	}
	if os.Geteuid() != 0 {
		return errors.New("prepare-state must run as root: the snap's hooks and daemon run it")
	}
	lg, err := telemetry.NewLogger(logOut, "fleetwire-agent", "info")
	if err != nil {
		return err
	}
	res, err := statedir.Prepare(os.Getenv("SNAP_DATA"), statedir.SnapDaemonID, statedir.SnapDaemonID)
	if err != nil {
		(&agent.SnapHealth{Log: lg}).Set("error", "cannot prepare the state directory: see the log")
		return fmt.Errorf("prepare state directory: %w", err)
	}
	switch {
	case len(res.Moved) > 0:
		lg.Info("moved the state into its own directory", "moved", res.Moved, "dir", e.dataDir, "owner_uid", statedir.SnapDaemonID)
	case res.Created:
		lg.Info("created the state directory", "dir", e.dataDir, "owner_uid", statedir.SnapDaemonID)
	case res.Repaired:
		lg.Warn("gave the state directory back to its owner", "dir", e.dataDir, "owner_uid", statedir.SnapDaemonID)
	}
	return nil
}

// dialAddr turns a listen address into one to connect to: a daemon that
// listens on every address (":9465", "0.0.0.0:9465", "[::]:9465") is
// reached on loopback.
func dialAddr(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return net.JoinHostPort(host, port)
}

func getStatus(ctx context.Context, listen string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listen+"/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status endpoint: HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}
