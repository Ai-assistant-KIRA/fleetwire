// Command fleetwire-agent is the device agent.
//
//	fleetwire-agent [run]           collect, store and send readings (the daemon)
//	fleetwire-agent config check    validate the configuration (snap configure hook)
//	fleetwire-agent config docs     print the configuration reference
//	fleetwire-agent status [-json]  last seq, acked seq, gaps, certificate expiry
//	fleetwire-agent version
//
// Inside the snap, configuration comes from `snapctl get -d` and state lives
// in $SNAP_DATA. Outside it, use -data-dir, -config file.json and -set key=value.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
	e := &env{inSnap: os.Getenv("SNAP") != "" && os.Getenv("SNAP_DATA") != ""}
	fs := flag.NewFlagSet("fleetwire-agent "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	if e.inSnap {
		e.dataDir = os.Getenv("SNAP_DATA")
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
	}
	return fmt.Errorf("unknown command %q", cmd)
}

func daemon(ctx context.Context, e *env, logOut io.Writer) error {
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
	reg := telemetry.NewRegistry("fleetwire-agent")
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
	if body, err := getStatus(ctx, listen); err == nil {
		if err := json.Unmarshal(body, &st); err != nil {
			return fmt.Errorf("decode status: %w", err)
		}
	} else {
		in, ierr := wal.Inspect(filepath.Join(e.dataDir, "wal"))
		if ierr != nil {
			return fmt.Errorf("agent not reachable (%w) and state unreadable: %w", err, ierr)
		}
		st = map[string]any{
			"running": false, "device_id": in.State.DeviceID, "last_seq": max(in.LastSeq, in.State.MaxSeqEver),
			"acked": in.State.Acked, "pending_gaps": in.State.PendingGaps, "gaps": in.State.GapHistory,
			"wal_bytes": in.Bytes, "wal_segments": in.Segments,
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
	for _, k := range []string{"running", "device_id", "connected", "last_seq", "acked", "pending_gaps", "gaps", "wal_bytes", "wal_segments", "cert_not_after"} {
		if v, ok := st[k]; ok {
			fmt.Fprintf(out, "%-15s %v\n", k+":", v)
		}
	}
	return nil
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
