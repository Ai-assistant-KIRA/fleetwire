// Command fleetwire-loadgen runs many real agent instances in one process.
//
//	fleetwire-loadgen run -devices 2000 -state-dir DIR -token fwt_... [flags]
//
// Restarting with the same -state-dir resumes every device from its own
// identity and local log, which is how the agent-kill scenario works.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/buildinfo"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/loadgen"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/telemetry"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "fleetwire-loadgen:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 0 && args[0] == "version" {
		fmt.Println(buildinfo.Get())
		return nil
	}
	if len(args) > 0 && args[0] == "run" {
		args = args[1:]
	}
	fs := flag.NewFlagSet("fleetwire-loadgen run", flag.ContinueOnError)
	var o loadgen.Options
	fs.IntVar(&o.Devices, "devices", 100, "Number of device instances.")
	fs.StringVar(&o.StateDir, "state-dir", "/var/lib/fleetwire-loadgen", "Directory with one subdirectory per device.")
	fs.DurationVar(&o.Interval, "interval", 10*time.Second, "Collection interval of every device.")
	fs.IntVar(&o.Readings, "readings", 1, "Readings per device per interval.")
	fs.DurationVar(&o.Duration, "duration", 10*time.Minute, "How long to collect (SIGTERM ends it earlier).")
	ramp := fs.String("ramp", "200/s", "Device start rate.")
	fs.StringVar(&o.APIURL, "api", "https://localhost:8443", "Public API URL (enrollment tokens).")
	fs.StringVar(&o.DeviceAPIURL, "device-api", "https://localhost:8444", "Device API URL (enrollment).")
	fs.StringVar(&o.TenantToken, "token", os.Getenv("FLEETWIRE_TENANT_TOKEN"), "Tenant token with devices:admin (or FLEETWIRE_TENANT_TOKEN).")
	caFile := fs.String("ca-file", "deploy/compose/secrets/ca-bundle.pem", "CA bundle for the API and the broker.")
	fs.StringVar(&o.BrokerURL, "broker", "tls://127.0.0.1:18883", "Broker URL (normally Toxiproxy).")
	fs.IntVar(&o.BatchSize, "batch-size", 100, "uplink.batch-size of every device.")
	fs.DurationVar(&o.BatchWait, "batch-wait", time.Second, "uplink.batch-wait of every device.")
	fs.DurationVar(&o.AckTimeout, "ack-timeout", 30*time.Second, "uplink.ack-timeout of every device.")
	fs.IntVar(&o.MaxInflight, "max-inflight", 1000, "uplink.max-inflight of every device.")
	sync := fs.String("wal-sync", "interval", "wal.sync of every device: always, interval or never.")
	fs.DurationVar(&o.WALSyncEvery, "wal-sync-interval", time.Second, "wal.sync-interval of every device.")
	fs.Int64Var(&o.WALMaxSize, "wal-max-size", 0, "wal.max-size in bytes (0: default).")
	fs.DurationVar(&o.DrainTimeout, "drain-timeout", 3*time.Minute, "How long to wait for acks after collection stops.")
	metrics := fs.String("metrics-listen", "", "Serve Prometheus metrics here, for example 172.31.250.1:9466.")
	level := fs.String("log-level", "warn", "Log level.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rate, ok := strings.CutSuffix(*ramp, "/s")
	r, err := strconv.ParseFloat(rate, 64)
	if !ok || err != nil || r <= 0 {
		return fmt.Errorf("-ramp must look like 200/s")
	}
	o.RampPerSec = r
	if o.WALSync, err = wal.ParseSyncMode(*sync); err != nil {
		return err
	}
	if o.TenantToken == "" {
		return errors.New("-token (or FLEETWIRE_TENANT_TOKEN) is required")
	}
	if o.CABundle, err = os.ReadFile(*caFile); err != nil {
		return fmt.Errorf("read CA bundle: %w", err)
	}
	o.Version = buildinfo.Get().Version
	log, err := telemetry.NewLogger(os.Stderr, "fleetwire-loadgen", *level)
	if err != nil {
		return err
	}
	reg := telemetry.NewRegistry("fleetwire-loadgen")
	runner := loadgen.New(o, log, reg)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *metrics != "" {
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", *metrics)
		if err != nil {
			return fmt.Errorf("metrics listener: %w", err)
		}
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = srv.Serve(ln) }()
		defer func() { _ = srv.Close() }()
	}
	return runner.Run(ctx)
}
