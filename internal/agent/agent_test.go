package agent

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/collect"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/config"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki/devpki"
)

type oneValue struct{}

func (oneValue) Name() string { return "one" }

func (oneValue) Collect(_ context.Context, emit collect.Emit) error {
	emit("test.value", 1, nil)
	return nil
}

// collectLoop logs append failures while Run enrolls the device, and
// enrollment replaces d.lg. The race detector catches a shared logger.
func TestEnrollingWhileAppendsFail(t *testing.T) {
	h := apitest.New(t)
	_, tok := h.CreateTenant("agent")
	bundle, err := os.ReadFile(filepath.Join(h.PKIDir, devpki.CABundle))
	if err != nil {
		t.Fatal(err)
	}
	c := config.Defaults()
	// One collection only, so the log mutex taken by later appends does not
	// order the collector's logging before enrollment.
	c.CollectInterval = time.Hour
	c.EnrollURL, c.EnrollToken, c.EnrollCABundle = h.Device.URL, h.EnrollmentTokens(tok, 1)[0], string(bundle)
	d, err := Open(Options{Dir: t.TempDir(), Config: c, Collectors: []collect.Collector{oneValue{}},
		Metrics: NewMetrics(prometheus.NewRegistry()), Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	// Every append to a closed log fails.
	if err := d.log.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_ = d.Run(ctx) // ends with the context: the test broker does not exist
	if !d.id.Enrolled() {
		t.Fatal("device did not enroll")
	}
}
