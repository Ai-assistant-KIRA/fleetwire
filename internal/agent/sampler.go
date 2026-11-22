package agent

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/agent/collect"
	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/uplinkpb"
)

// Sampler runs every collector once per interval, each in its own
// goroutine with a timeout, so one slow or failing collector never holds
// back the others.
type Sampler struct {
	collectors []collect.Collector
	timeout    time.Duration
	m          *Metrics
	log        *slog.Logger
	busy       []atomic.Bool
}

// NewSampler returns a sampler whose collectors each get timeout per call.
func NewSampler(cs []collect.Collector, timeout time.Duration, m *Metrics, log *slog.Logger) *Sampler {
	return &Sampler{collectors: cs, timeout: timeout, m: m, log: log, busy: make([]atomic.Bool, len(cs))}
}

type sample struct {
	metric string
	value  float64
	labels map[string]string
	at     time.Time
}

type result struct {
	samples []sample
	err     error
}

// Collect returns the valid readings of one round, in collector order.
func (s *Sampler) Collect(ctx context.Context) []*fleetwirev1.Reading {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	waits := make([]chan result, len(s.collectors))
	for i, c := range s.collectors {
		// A collector still running from the last round is skipped rather
		// than started twice, so a hung one cannot pile up goroutines.
		if !s.busy[i].CompareAndSwap(false, true) {
			s.m.CollectorErrors.WithLabelValues(c.Name()).Inc()
			s.log.Warn("collector still busy from the previous round; skipped", "collector", c.Name())
			continue
		}
		ch := make(chan result, 1)
		waits[i] = ch
		go func() {
			defer s.busy[i].Store(false)
			var out []sample
			err := c.Collect(ctx, func(m string, v float64, l map[string]string) {
				out = append(out, sample{metric: m, value: v, labels: l, at: time.Now()})
			})
			ch <- result{samples: out, err: err}
		}()
	}
	var readings []*fleetwirev1.Reading
	for i, ch := range waits {
		if ch == nil {
			continue
		}
		name := s.collectors[i].Name()
		var r result
		select {
		case r = <-ch:
		case <-ctx.Done():
			s.m.CollectorErrors.WithLabelValues(name).Inc()
			s.log.Warn("collector timed out", "collector", name, "timeout", s.timeout)
			continue
		}
		if r.err != nil {
			s.m.CollectorErrors.WithLabelValues(name).Inc()
			s.log.Warn("collector failed", "collector", name, "err", r.err)
		}
		// Samples emitted before an error are still good (one unreadable
		// thermal zone must not hide the others).
		for _, sm := range r.samples {
			rd := &fleetwirev1.Reading{CollectedUnixNano: sm.at.UnixNano(), Collector: name,
				Metric: sm.metric, Value: sm.value, Labels: sm.labels}
			if err := uplinkpb.ValidateReading(rd); err != nil {
				s.m.CollectorErrors.WithLabelValues(name).Inc()
				s.log.Error("collector emitted an invalid reading; dropped", "collector", name, "err", err)
				continue
			}
			s.m.Collected.WithLabelValues(name).Inc()
			readings = append(readings, rd)
		}
	}
	return readings
}
