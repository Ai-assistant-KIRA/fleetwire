package ingest

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"slices"
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
	// partition is the uplink partition of the batch that produced the ack.
	partition int32
	span      trace.SpanContext
}

// busyAck is an ack a worker is publishing.
type busyAck struct {
	partition int32
	// released is set when the partition was given up meanwhile. If the
	// publish fails, the ack is dropped instead of queued again.
	released bool
}

// AckPublisher keeps only the highest unpublished watermark per device and
// publishes it as a retained QoS 1 message on fw/v1/d/{id}/ack. Memory is
// bounded by the number of devices with news. A lost ack is harmless: the
// device resends, ingest finds the range covered, and offers the ack again.
//
// The retained ack must never move back: a device whose disk was restored
// takes it as the server's watermark and moves its next seq past it
// (R3.8), and seqs reused below the true watermark would be dropped as
// duplicates. Within one instance, a device's acks are therefore published
// one at a time; a worker skips a device whose previous ack is still in
// flight. Across instances, the acks of a partition are published or
// dropped before the partition is given up (Release), so the next owner's
// acks for its devices come later.
type AckPublisher struct {
	cfg     AckConfig
	log     *slog.Logger
	mu      sync.Mutex
	pending map[uuid.UUID]pendingAck
	busy    map[uuid.UUID]busyAck
	// changed is closed and replaced whenever an ack leaves busy; Release
	// waits on it.
	changed    chan struct{}
	signal     chan struct{}
	publishAck func(context.Context, *paho.Publish) error
	connected  atomic.Bool
	tracer     trace.Tracer
	published  prometheus.Counter
	errors     prometheus.Counter
	queued     prometheus.Gauge
	mqtt       *mqttconn.Metrics
	wg         sync.WaitGroup
}

// NewAckPublisher returns a publisher; Start connects it.
func NewAckPublisher(cfg AckConfig, log *slog.Logger, reg prometheus.Registerer) *AckPublisher {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	a := &AckPublisher{
		cfg: cfg, log: log, pending: map[uuid.UUID]pendingAck{}, busy: map[uuid.UUID]busyAck{},
		changed: make(chan struct{}), signal: make(chan struct{}, 1),
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
	a.publishAck = func(ctx context.Context, p *paho.Publish) error {
		_, err := cm.Publish(ctx, p)
		return err
	}
	a.startWorkers(ctx)
	return nil
}

func (a *AckPublisher) startWorkers(ctx context.Context) {
	for range a.cfg.Workers {
		a.wg.Go(func() { a.work(ctx) })
	}
}

// Connected reports whether the broker connection is up.
func (a *AckPublisher) Connected() bool { return a.connected.Load() }

func (a *AckPublisher) wake() {
	select {
	case a.signal <- struct{}{}:
	default:
	}
}

// Offer queues an ack from a batch of the given uplink partition,
// replacing a lower queued one for the same device.
func (a *AckPublisher) Offer(span trace.SpanContext, device uuid.UUID, partition int32, through uint64) {
	a.mu.Lock()
	if cur, ok := a.pending[device]; !ok || through > cur.through {
		a.pending[device] = pendingAck{through: through, partition: partition, span: span}
	}
	a.queued.Set(float64(len(a.pending)))
	a.mu.Unlock()
	a.wake()
}

// take hands a queued ack to a worker. A device whose previous ack is still
// being published waits, so that one cannot land after this one.
func (a *AckPublisher) take() (uuid.UUID, pendingAck, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for d, p := range a.pending {
		if _, ok := a.busy[d]; ok {
			continue
		}
		delete(a.pending, d)
		a.busy[d] = busyAck{partition: p.partition}
		a.queued.Set(float64(len(a.pending)))
		return d, p, true
	}
	return uuid.Nil, pendingAck{}, false
}

// finish ends the publish of p. A failed ack goes back into the queue
// unless a newer one arrived or its partition was released meanwhile.
func (a *AckPublisher) finish(device uuid.UUID, p pendingAck, failed bool) {
	a.mu.Lock()
	b := a.busy[device]
	delete(a.busy, device)
	if failed && !b.released {
		if cur, ok := a.pending[device]; !ok || cur.through < p.through {
			a.pending[device] = p
		}
	}
	a.queued.Set(float64(len(a.pending)))
	close(a.changed)
	a.changed = make(chan struct{})
	a.mu.Unlock()
	// A newer ack for the same device may be waiting for this one.
	a.wake()
}

// Release is called before this instance gives up uplink partitions, in a
// rebalance or at shutdown. It waits up to wait for the partitions' queued
// and in-flight acks to be published, then drops what is left, so no older
// ack from here can overwrite one from the partitions' next owner. It does
// not wait while the broker connection is down. A dropped ack costs a
// resend from the device, which the next owner acks.
func (a *AckPublisher) Release(partitions []int32, wait time.Duration) {
	if len(partitions) == 0 {
		return
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	deadline := time.Now().Add(wait)
	for {
		a.mu.Lock()
		left := 0
		for _, p := range a.pending {
			if slices.Contains(partitions, p.partition) {
				left++
			}
		}
		for _, b := range a.busy {
			if slices.Contains(partitions, b.partition) {
				left++
			}
		}
		if left == 0 || !a.connected.Load() || !time.Now().Before(deadline) {
			dropped := 0
			for d, p := range a.pending {
				if slices.Contains(partitions, p.partition) {
					delete(a.pending, d)
					dropped++
				}
			}
			for d, b := range a.busy {
				if slices.Contains(partitions, b.partition) {
					b.released = true
					a.busy[d] = b
				}
			}
			a.queued.Set(float64(len(a.pending)))
			a.mu.Unlock()
			if dropped > 0 {
				a.log.Info("dropped acks of partitions this instance gave up; the devices resend and the next owner acks",
					"acks", dropped, "partitions", partitions)
			}
			return
		}
		changed := a.changed
		a.mu.Unlock()
		select {
		case <-changed:
		case <-timer.C:
		}
	}
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
		err := a.publish(ctx, dev, p)
		a.finish(dev, p, err != nil)
		if err != nil {
			a.errors.Inc()
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
	return a.publishAck(pctx, &paho.Publish{QoS: 1, Retain: true, Topic: "fw/v1/d/" + dev.String() + "/ack", Payload: payload})
}

// Drain waits up to d for queued and in-flight acks to go out.
func (a *AckPublisher) Drain(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) && a.connected.Load() {
		a.mu.Lock()
		n := len(a.pending) + len(a.busy)
		a.mu.Unlock()
		if n == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}
