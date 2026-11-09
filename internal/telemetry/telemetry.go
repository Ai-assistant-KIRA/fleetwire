// Package telemetry sets up logging, metrics and tracing the same way for
// every fleetwire binary: JSON logs through log/slog, a Prometheus registry,
// and OpenTelemetry traces exported over OTLP/HTTP.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/buildinfo"
)

// NewLogger returns a JSON logger with service and version on every line.
func NewLogger(w io.Writer, service, level string) (*slog.Logger, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: l})
	return slog.New(h).With("service", service, "version", buildinfo.Get().Version), nil
}

// NewRegistry returns a registry with Go runtime, process and build info
// metrics. Each binary registers its own metrics on it.
func NewRegistry(service string) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	info := buildinfo.Get()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "fleetwire_build_info",
			Help:        "Always 1; labels carry the build of the running binary.",
			ConstLabels: prometheus.Labels{"service": service, "version": info.Version, "commit": info.Commit},
		}, func() float64 { return 1 }),
	)
	return reg
}

// InitTracing installs a global tracer provider that exports to endpoint
// (an OTLP/HTTP URL such as http://jaeger:4318). With an empty endpoint,
// tracing stays a no-op. The sampler is parent-based, so a trace started by
// the bridge is kept or dropped as a whole.
func InitTracing(ctx context.Context, service, endpoint string, ratio float64) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(strings.TrimSuffix(endpoint, "/")+"/v1/traces"))
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName(service), semconv.ServiceVersion(buildinfo.Get().Version)))
	if err != nil {
		return nil, fmt.Errorf("trace resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Probe reports whether a dependency is usable. Nil means ready.
type Probe func(ctx context.Context) error

// AdminServer serves /healthz (process is up), /readyz (every probe passes)
// and /metrics on a listener that must not be exposed publicly.
type AdminServer struct {
	srv *http.Server
	ln  net.Listener
}

// NewAdminServer binds addr and builds the handlers.
func NewAdminServer(ctx context.Context, addr string, reg *prometheus.Registry, probes map[string]Probe) (*AdminServer, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("admin listener %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		var failed []string
		for name, p := range probes {
			if err := p(ctx); err != nil {
				failed = append(failed, name+": "+err.Error())
			}
		}
		w.Header().Set("Content-Type", "text/plain")
		if len(failed) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, strings.Join(failed, "\n")+"\n")
			return
		}
		_, _ = io.WriteString(w, "ready\n")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	return &AdminServer{
		ln:  ln,
		srv: &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
	}, nil
}

// Addr is the bound address (useful with port 0 in tests).
func (a *AdminServer) Addr() string { return a.ln.Addr().String() }

// Serve runs until Shutdown.
func (a *AdminServer) Serve() error {
	if err := a.srv.Serve(a.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("admin server: %w", err)
	}
	return nil
}

// Shutdown stops the server.
func (a *AdminServer) Shutdown(ctx context.Context) error {
	if err := a.srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("admin server shutdown: %w", err)
	}
	return nil
}
