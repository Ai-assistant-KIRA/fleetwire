package ingest

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/mqttconn"
)

// AckConfig configures the ack publisher.
type AckConfig struct {
	MQTTURL  string
	ClientID string
	TLS      *tls.Config
	Workers  int
}

type pendingAck struct {
	through uint64
	span    trace.SpanContext
}

// AckPublisher keeps only the highest unpublished watermark per device and
// publishes it as a retained QoS 1 message on fw/v1/d/{id}/ack. Memory is
// bounded by the number of devices with news. A lost ack is harmless: the
// device resends, ingest finds the range covered, and offers the ack again.
type AckPublisher struct {
	cfg       AckConfig
	log       *slog.Logger
	mu        sync.Mutex
	pending   map[uuid.UUID]pendingAck
	signal    chan struct{}
	cm        *autopaho.ConnectionManager
	connected atomic.Bool
	tracer    trace.Tracer
	published prometheus.Counter
	errors    prometheus.Counter
	queued    prometheus.Gauge
	mqtt      *mqttconn.Metrics
	wg        sync.WaitGroup
}

// NewAckPublisher returns a publisher; Start connects it.
func NewAckPublisher(cfg AckConfig, log *slog.Logger, reg prometheus.Registerer) *AckPublisher {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	a := &AckPublisher{
		cfg: cfg, log: log, pending: map[uuid.UUID]pendingAck{}, signal: make(chan struct{}, 1),
		tracer:    otel.Tracer("fleetwire/ingest"),
		published: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_ingest_acks_published_total", Help: "End-to-end acks published."}),
		errors:    prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_ingest_ack_publish_errors_total", Help: "Ack publishes that failed (retried)."}),
		queued:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fleetwire_ingest_pending_acks", Help: "Devices with an ack not yet published."}),
		mqtt:      mqttconn.NewMetrics(reg, "fleetwire_ingest"),
	}
	reg.MustRegister(a.published, a.errors, a.queued)
	return a
}

// Start connects to the broker and starts the workers.
func (a *AckPublisher) Start(ctx context.Context) error {
	cm, err := mqttconn.Start(ctx, mqttconn.Config{
		URL: a.cfg.MQTTURL, ClientID: a.cfg.ClientID, TLS: a.cfg.TLS, SessionExpiry: time.Hour,
		Logger: a.log, Metrics: a.mqtt,
		OnUp:   func(*autopaho.ConnectionManager, *paho.Connack) { a.connected.Store(true); a.wake() },
		OnDown: func() { a.connected.Store(false) },
	})
	if err != nil {
		return err
	}
	a.cm = cm
	for range a.cfg.Workers {
		a.wg.Go(func() { a.work(ctx) })
	}
	return nil
}

// Connected reports whether the broker connection is up.
func (a *AckPublisher) Connected() bool { return a.connected.Load() }

func (a *AckPublisher) wake() {
	select {
	case a.signal <- struct{}{}:
	default:
	}
}

// Offer queues an ack, replacing a lower queued one for the same device.
func (a *AckPublisher) Offer(span trace.SpanContext, device uuid.UUID, through uint64) {
	a.mu.Lock()
	if cur, ok := a.pending[device]; !ok || through > cur.through {
		a.pending[device] = pendingAck{through: through, span: span}
	}
	a.queued.Set(float64(len(a.pending)))
	a.mu.Unlock()
	a.wake()
}

func (a *AckPublisher) take() (uuid.UUID, pendingAck, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for d, p := range a.pending {
		delete(a.pending, d)
		a.queued.Set(float64(len(a.pending)))
		return d, p, true
	}
	return uuid.Nil, pendingAck{}, false
}

// requeue puts back an ack that failed, unless a newer one arrived.
func (a *AckPublisher) requeue(device uuid.UUID, p pendingAck) {
	a.mu.Lock()
	if cur, ok := a.pending[device]; !ok || cur.through < p.through {
		a.pending[device] = p
	}
	a.queued.Set(float64(len(a.pending)))
	a.mu.Unlock()
}

func (a *AckPublisher) work(ctx context.Context) {
	for ctx.Err() == nil {
		if !a.connected.Load() {
			select {
			case <-ctx.Done():
				return
			case <-a.signal:
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		dev, p, ok := a.take()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-a.signal:
			}
			continue
		}
		if err := a.publish(ctx, dev, p); err != nil {
			a.errors.Inc()
			a.requeue(dev, p)
			if !errors.Is(err, autopaho.ConnectionDownError) && ctx.Err() == nil {
				a.log.Warn("ack publish failed; will retry", "device_id", dev, "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		a.published.Inc()
		a.wake() // let another worker continue
	}
}

func (a *AckPublisher) publish(ctx context.Context, dev uuid.UUID, p pendingAck) error {
	sctx, span := a.tracer.Start(trace.ContextWithRemoteSpanContext(ctx, p.span), "ingest.ack_publish")
	defer span.End()
	payload, err := proto.Marshal(&fleetwirev1.Ack{AckedThrough: p.through, ServerUnixNano: time.Now().UnixNano()})
	if err != nil {
		return err
	}
	pctx, cancel := context.WithTimeout(sctx, 10*time.Second)
	defer cancel()
	_, err = a.cm.Publish(pctx, &paho.Publish{QoS: 1, Retain: true, Topic: "fw/v1/d/" + dev.String() + "/ack", Payload: payload})
	return err
}

// Drain waits up to d for queued acks to go out.
func (a *AckPublisher) Drain(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) && a.connected.Load() {
		a.mu.Lock()
		n := len(a.pending)
		a.mu.Unlock()
		if n == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}
