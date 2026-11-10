// Command fleetwire-chaos runs the failure scenarios against the Compose
// stack and writes the results table into docs/results/.
//
//	fleetwire-chaos run [-scenario network-cut,ingest-kill] [-devices 2000]
//	fleetwire-chaos list
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/chaos"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/telemetry"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "fleetwire-chaos:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 0 && args[0] == "list" {
		fmt.Println(strings.Join(chaos.Scenarios(), "\n"))
		return nil
	}
	if len(args) > 0 && args[0] == "run" {
		args = args[1:]
	}
	fs := flag.NewFlagSet("fleetwire-chaos run", flag.ContinueOnError)
	var o chaos.Options
	scen := fs.String("scenario", strings.Join(chaos.Scenarios(), ","), "Comma-separated scenarios (default: all).")
	fs.StringVar(&o.RepoDir, "repo", ".", "Repository root (for deploy/compose).")
	fs.StringVar(&o.BinDir, "bin", "bin", "Directory with fleetwire-loadgen and fleetwire-checker.")
	fs.StringVar(&o.OutDir, "out", "docs/results", "Where the results table goes.")
	fs.IntVar(&o.Devices, "devices", 2000, "Device instances per scenario.")
	fs.DurationVar(&o.Interval, "interval", 10*time.Second, "Collection interval per device.")
	fs.IntVar(&o.Readings, "readings", 1, "Readings per device per interval.")
	fs.StringVar(&o.Ramp, "ramp", "200/s", "Device start rate.")
	fs.DurationVar(&o.Steady, "steady", 60*time.Second, "Load before the fault.")
	fs.DurationVar(&o.After, "after", 60*time.Second, "Load after recovery.")
	fs.StringVar(&o.WALSync, "wal-sync", "interval", "wal.sync of the devices.")
	fs.StringVar(&o.StateDir, "state-dir", "", "Where the devices' local state goes (default: next to the logs in -out/raw). Put it on tmpfs for large runs; the table says which.")
	fs.StringVar(&o.APIURL, "api", "https://localhost:8443", "Public API URL.")
	fs.StringVar(&o.DeviceAPIURL, "device-api", "https://localhost:8444", "Device API URL.")
	fs.StringVar(&o.BrokerURL, "broker", "tls://127.0.0.1:18883", "Broker URL for devices (Toxiproxy).")
	fs.StringVar(&o.PrometheusURL, "prometheus", "http://127.0.0.1:9090", "Prometheus URL.")
	fs.StringVar(&o.ToxiproxyURL, "toxiproxy", "http://127.0.0.1:8474", "Toxiproxy API URL.")
	fs.StringVar(&o.LoadgenListen, "loadgen-metrics", "172.31.250.1:9466", "Where the load generator serves metrics for Prometheus.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, s := range strings.Split(*scen, ",") {
		if s = strings.TrimSpace(s); s != "" {
			o.Scenarios = append(o.Scenarios, s)
		}
	}
	log, err := telemetry.NewLogger(os.Stderr, "fleetwire-chaos", "info")
	if err != nil {
		return err
	}
	o.Log = log
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	rows, err := chaos.Run(ctx, o)
	fmt.Print(chaos.Table(rows))
	if err != nil {
		return err
	}
	for _, r := range rows {
		if !r.Pass {
			return fmt.Errorf("scenario %s failed", r.Scenario)
		}
	}
	return nil
}
