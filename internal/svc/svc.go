// Package svc holds the start-up code shared by the backend binaries:
// signal handling, logging, tracing, the admin server, and mTLS client
// configs for the broker.
package svc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/buildinfo"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/telemetry"
)

// Common settings of every backend service.
type Common struct {
	LogLevel         string  `env:"FLEETWIRE_LOG_LEVEL" flag:"log-level" default:"info" help:"debug, info, warn or error."`
	AdminListen      string  `env:"FLEETWIRE_ADMIN_LISTEN" flag:"admin-listen" default:"127.0.0.1:9090" help:"Health and metrics listener (plain HTTP; keep it private)."`
	OTLPEndpoint     string  `env:"FLEETWIRE_OTLP_ENDPOINT" flag:"otlp-endpoint" help:"OTLP/HTTP endpoint for traces. Empty disables tracing."`
	TraceSampleRatio float64 `env:"FLEETWIRE_TRACE_SAMPLE_RATIO" flag:"trace-sample-ratio" default:"0.1" help:"Fraction of new traces sampled."`
}

// Runtime is what Start sets up.
type Runtime struct {
	Log      *slog.Logger
	Registry *prometheus.Registry
	shutdown func(context.Context) error
}

// Start creates the logger, registry and tracer for service.
func Start(ctx context.Context, service string, c Common, logOut io.Writer) (*Runtime, error) {
	log, err := telemetry.NewLogger(logOut, service, c.LogLevel)
	if err != nil {
		return nil, err
	}
	log.Info("starting", "build", buildinfo.Get().String())
	shutdown, err := telemetry.InitTracing(ctx, service, c.OTLPEndpoint, c.TraceSampleRatio)
	if err != nil {
		return nil, err
	}
	return &Runtime{Log: log, Registry: telemetry.NewRegistry(service), shutdown: shutdown}, nil
}

// Serve runs main and the admin server until a signal arrives or either
// fails, then shuts both down.
func (r *Runtime) Serve(ctx context.Context, adminAddr string, probes map[string]telemetry.Probe, main func(ctx context.Context) error) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	admin, err := telemetry.NewAdminServer(ctx, adminAddr, r.Registry, probes)
	if err != nil {
		return err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(admin.Serve)
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(gctx), 10*time.Second)
		defer cancel()
		return admin.Shutdown(sctx)
	})
	g.Go(func() error {
		err := main(gctx)
		if err == nil && ctx.Err() == nil {
			return errors.New("service stopped unexpectedly")
		}
		return err
	})
	err = g.Wait()
	tctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if serr := r.shutdown(tctx); serr != nil {
		r.Log.Warn("trace exporter shutdown", "err", serr)
	}
	r.Log.Info("stopped", "err", err)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// BrokerTLS builds the mTLS config a service uses to reach Mosquitto.
func BrokerTLS(brokerURL, certFile, keyFile, caFile string) (*tls.Config, error) {
	u, err := url.Parse(brokerURL)
	if err != nil {
		return nil, fmt.Errorf("broker url: %w", err)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load broker client certificate: %w", err)
	}
	roots, err := pki.CertPool(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: roots, ServerName: u.Hostname()}, nil
}

// Healthcheck asks the local admin server's /readyz and fails unless it
// answers 200. Distroless images have no shell or curl, so container health
// checks run the service binary itself: `service healthcheck`.
func Healthcheck(ctx context.Context, adminListen string) error {
	host, port, err := net.SplitHostPort(adminListen)
	if err != nil {
		return fmt.Errorf("admin listen %q: %w", adminListen, err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, port)+"/readyz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("readyz: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("not ready: %s", strings.TrimSpace(string(body)))
	}
	return nil
}

// InstanceID returns a stable name for this process: the configured one,
// or the host name (the container name in Compose and Kubernetes).
func InstanceID(configured string) string {
	if configured != "" {
		return configured
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "default"
}
