package svc

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// With mqtt:// autopaho ignores the TLS config and connects in plain
// text, so the service URL is refused before any certificate is loaded.
func TestBrokerTLSRequiresATLSURL(t *testing.T) {
	if _, err := BrokerTLS("mqtt://mosquitto:8884", "cert.pem", "key.pem", "ca.pem"); err == nil || !strings.Contains(err.Error(), "TLS URL") {
		t.Fatalf("BrokerTLS(mqtt://) = %v, want a TLS URL error", err)
	}
	if _, err := BrokerTLS("tls://mosquitto:8884", "missing.pem", "missing.key", "ca.pem"); err == nil || !strings.Contains(err.Error(), "client certificate") {
		t.Fatalf("BrokerTLS(tls://) = %v, want it to get as far as loading the certificate", err)
	}
}

// The trace exporter is flushed once, whether Serve ends normally, fails
// to start its admin server, or never runs because a later startup step
// failed (the binaries defer Shutdown right after Start).
func TestShutdownFlushesTracesOnce(t *testing.T) {
	calls := 0
	r := &Runtime{Log: slog.New(slog.DiscardHandler), Registry: prometheus.NewRegistry(),
		shutdown: func(context.Context) error { calls++; return nil }}
	err := r.Serve(t.Context(), "127.0.0.1:-1", nil, func(context.Context) error { return nil })
	if err == nil {
		t.Fatal("Serve with a bad admin address succeeded")
	}
	r.Shutdown(t.Context())
	if calls != 1 {
		t.Fatalf("trace exporter shut down %d times, want 1", calls)
	}
}
