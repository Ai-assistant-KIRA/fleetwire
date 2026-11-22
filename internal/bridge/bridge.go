// Package bridge moves device messages from Mosquitto into Kafka (R5).
//
// Each MQTT message is acknowledged to Mosquitto only after Kafka has
// confirmed the write with acks=all. paho sends PUBACKs strictly in arrival
// order (checked in task 0.2), so one slow write holds back every later
// PUBACK; once the receive maximum is reached Mosquitto stops sending and
// queues in the persistent session. That is the backpressure when Kafka is
// slow or down (R5.5).
//
// franz-go retries most produce errors itself and fails a record only for
// errors it cannot retry, such as a topic that stays unknown or a denied
// authorization. The bridge then writes the message again with backoff
// until Kafka takes it, so it keeps its place in the PUBACK order and the
// backpressure holds; readiness fails meanwhile.
package bridge

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/backoff"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/kafkahdr"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/mqttconn"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/uplinkpb"
)

// Config is the bridge's settings.
type Config struct {
	MQTTURL        string
	ClientID       string
	TLS            *tls.Config
	ShareGroup     string
	ReceiveMaximum uint16
	// Kafka holds the connection options (brokers, SASL, TLS) from
	// svc.Kafka.KafkaOpts; the producer settings are added here.
	Kafka []kgo.Opt
	Topic string
	// ReadyAfter: readiness fails when messages are pending and no write
	// has succeeded for this long (default 10 s).
	ReadyAfter time.Duration
}

// producer is what the bridge needs from *kgo.Client to write; tests
// replace it.
type producer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
}

// retryBackoff spaces the writes of a message that Kafka refused.
var retryBackoff = backoff.Backoff{Base: time.Second, Max: 30 * time.Second}

type metrics struct {
	forwarded   *prometheus.CounterVec
	pending     prometheus.Gauge
	produceSecs prometheus.Histogram
	lastOK      prometheus.Gauge
	mqtt        *mqttconn.Metrics
}

// Bridge is one bridge instance.
type Bridge struct {
	cfg       Config
	log       *slog.Logger
	m         metrics
	kafka     *kgo.Client
	prod      producer
	tracer    trace.Tracer
	connected atomic.Bool
	// gen counts lost MQTT connections. A message remembers the generation
	// it arrived in; once that connection is gone its PUBACK can no longer
	// be sent, and Mosquitto redelivers it on the next one.
	gen     atomic.Uint64
	pending atomic.Int64
	// retrying counts pending messages that Kafka refused at least once.
	retrying atomic.Int64
	lastOK   atomic.Int64
	started  time.Time
}

// New creates the Kafka producer. Nothing connects to MQTT until Run.
func New(cfg Config, log *slog.Logger, reg prometheus.Registerer) (*Bridge, error) {
	b := newBridge(cfg, log, reg)
	cl, err := kgo.NewClient(append(slices.Clip(b.cfg.Kafka),
		kgo.ClientID(b.cfg.ClientID),
		kgo.DefaultProduceTopic(b.cfg.Topic),
		// acks=all and idempotence are the franz-go defaults; stated here
		// because the guarantee depends on them (R5.3).
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProduceRequestTimeout(30*time.Second),
		kgo.MaxBufferedBytes(64<<20),
		kgo.MaxBufferedRecords(10_000),
	)...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	b.kafka, b.prod = cl, cl
	return b, nil
}

// newBridge sets up everything but the Kafka client.
func newBridge(cfg Config, log *slog.Logger, reg prometheus.Registerer) *Bridge {
	if cfg.ShareGroup == "" {
		cfg.ShareGroup = "fleetwire-bridge"
	}
	if cfg.ReceiveMaximum == 0 {
		cfg.ReceiveMaximum = 1000
	}
	if cfg.ReadyAfter <= 0 {
		cfg.ReadyAfter = 10 * time.Second
	}
	b := &Bridge{cfg: cfg, log: log, tracer: otel.Tracer("fleetwire/bridge"), started: time.Now()}
	b.m = metrics{
		forwarded: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fleetwire_bridge_forwarded_total",
			Help: "MQTT messages handled, by result: ok, rejected, or error for each Kafka write that failed and is retried."}, []string{"result"}),
		pending: prometheus.NewGauge(prometheus.GaugeOpts{Name: "fleetwire_bridge_pending_acks",
			Help: "Messages received but not yet confirmed by Kafka."}),
		produceSecs: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "fleetwire_bridge_produce_seconds",
			Help: "Time from MQTT receive to Kafka confirmation.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 16)}),
		lastOK: prometheus.NewGauge(prometheus.GaugeOpts{Name: "fleetwire_bridge_last_produce_timestamp_seconds",
			Help: "Unix time of the last confirmed Kafka write, or of the process start before the first one."}),
		mqtt: mqttconn.NewMetrics(reg, "fleetwire_bridge"),
	}
	reg.MustRegister(b.m.forwarded, b.m.pending, b.m.produceSecs, b.m.lastOK)
	// Every result exists from the start at 0, so rate() and alerts see the
	// series before the first error.
	for _, r := range []string{"ok", "error", "rejected"} {
		b.m.forwarded.WithLabelValues(r)
	}
	// Until the first write the start time stands in: a gauge at 0 reads
	// as a bridge that has not written since 1970.
	b.lastOK.Store(b.started.UnixNano())
	b.m.lastOK.Set(float64(b.started.Unix()))
	return b
}

const topicPrefix, topicSuffix = "fw/v1/d/", "/up"

// deviceFromTopic returns the device ID in fw/v1/d/{id}/up.
func deviceFromTopic(topic string) (uuid.UUID, bool) {
	rest, ok := strings.CutPrefix(topic, topicPrefix)
	if !ok {
		return uuid.Nil, false
	}
	id, ok := strings.CutSuffix(rest, topicSuffix)
	if !ok {
		return uuid.Nil, false
	}
	u, err := uuid.Parse(id)
	if err != nil || u.String() != id {
		return uuid.Nil, false
	}
	return u, true
}

func (b *Bridge) ack(pkt *paho.Publish, ack func(*paho.Publish) error) {
	if err := ack(pkt); err != nil && !errors.Is(err, paho.ErrPacketNotFound) {
		b.log.Warn("mqtt ack failed", "err", err)
	}
	// ErrPacketNotFound: the connection was replaced since the message
	// arrived. Mosquitto redelivers it, it is written to Kafka again, and
	// ingest removes the duplicate.
}

// message is one MQTT message on its way to Kafka.
type message struct {
	pkt      *paho.Publish
	ack      func(*paho.Publish) error
	dev      uuid.UUID
	received time.Time
	headers  []kgo.RecordHeader
	span     trace.Span
	gen      uint64 // connection generation it arrived in
	failures int
}

// handle is called by paho for each message, in arrival order. ack marks
// the message as handled on the connection it came from.
func (b *Bridge) handle(ctx context.Context, pkt *paho.Publish, ack func(*paho.Publish) error) {
	received := time.Now()
	dev, ok := deviceFromTopic(pkt.Topic)
	if !ok || len(pkt.Payload) > uplinkpb.MaxMessageBytes {
		// Only a broken ACL or broker limit lets such a message through.
		// It can never become valid, so it is acked and counted.
		b.m.forwarded.WithLabelValues("rejected").Inc()
		b.log.Error("rejecting message: bad topic or too large", "topic", pkt.Topic, "bytes", len(pkt.Payload))
		b.ack(pkt, ack)
		return
	}
	sctx, span := b.tracer.Start(ctx, "bridge.forward", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("fleetwire.device_id", dev.String()), attribute.Int("messaging.message.body.size", len(pkt.Payload))))
	rec := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: kafkahdr.DeviceID, Value: []byte(dev.String())},
		{Key: kafkahdr.ReceivedAt, Value: []byte(strconv.FormatInt(received.UnixNano(), 10))},
	}}
	otel.GetTextMapPropagator().Inject(sctx, kafkahdr.Carrier{Record: rec})
	b.m.pending.Inc()
	b.pending.Add(1)
	b.produce(ctx, &message{pkt: pkt, ack: ack, dev: dev, received: received, headers: rec.Headers, span: span, gen: b.gen.Load()})
}

// produce writes m to Kafka, and acks it once Kafka has confirmed the
// write. Every attempt gets a fresh record, so nothing franz-go set on the
// previous one (timestamp, partition) carries over.
func (b *Bridge) produce(ctx context.Context, m *message) {
	rec := &kgo.Record{Key: []byte(m.dev.String()), Value: m.pkt.Payload, Headers: m.headers}
	b.prod.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		if err == nil {
			now := time.Now()
			b.lastOK.Store(now.UnixNano())
			b.m.lastOK.Set(float64(now.Unix()))
			b.m.produceSecs.Observe(now.Sub(m.received).Seconds())
			b.m.forwarded.WithLabelValues("ok").Inc()
			b.ack(m.pkt, m.ack)
			b.finish(m)
			return
		}
		b.m.forwarded.WithLabelValues("error").Inc()
		m.span.RecordError(err)
		if ctx.Err() != nil {
			// Stopping: the message stays in the session for the next run.
			b.finish(m)
			return
		}
		if m.failures == 0 {
			b.retrying.Add(1)
			b.log.Error("kafka refused a message; it stays unacknowledged and is written again", "device_id", m.dev, "err", err)
		} else {
			b.log.Debug("kafka refused a message again", "device_id", m.dev, "attempt", m.failures+1, "err", err)
		}
		delay := retryBackoff.Delay(m.failures)
		m.failures++
		// The promise must not block, so the next attempt waits on a timer.
		time.AfterFunc(delay, func() { b.retry(ctx, m) })
	})
}

// retry writes m again, unless the bridge is stopping or the connection m
// came on is gone. In both cases Mosquitto still holds the message in the
// session and delivers it again.
func (b *Bridge) retry(ctx context.Context, m *message) {
	if ctx.Err() != nil || b.gen.Load() != m.gen {
		m.span.SetStatus(codes.Error, "not written to Kafka; Mosquitto redelivers it")
		b.finish(m)
		return
	}
	b.produce(ctx, m)
}

// finish removes m from pending.
func (b *Bridge) finish(m *message) {
	b.m.pending.Dec()
	b.pending.Add(-1)
	if m.failures > 0 {
		b.retrying.Add(-1)
	}
	m.span.End()
}

// connectionLost is called when the MQTT connection drops.
func (b *Bridge) connectionLost() {
	b.connected.Store(false)
	b.gen.Add(1)
}

// Run bridges until ctx ends, then flushes Kafka so the last messages get
// their PUBACKs before the connection closes.
func (b *Bridge) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	sub := "$share/" + b.cfg.ShareGroup + "/" + topicPrefix + "+" + topicSuffix
	cm, err := mqttconn.Start(runCtx, mqttconn.Config{
		URL: b.cfg.MQTTURL, ClientID: b.cfg.ClientID, TLS: b.cfg.TLS,
		SessionExpiry: 7 * 24 * time.Hour, ReceiveMaximum: b.cfg.ReceiveMaximum, ManualAck: true,
		Logger: b.log, Metrics: b.m.mqtt,
		OnUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			go b.subscribe(runCtx, cm, sub)
		},
		OnDown: b.connectionLost,
		OnPublish: func(pr paho.PublishReceived) (bool, error) {
			b.handle(runCtx, pr.Packet, pr.Client.Ack)
			return true, nil
		},
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer fcancel()
	if err := b.kafka.Flush(fctx); err != nil {
		b.log.Warn("kafka flush at shutdown did not finish; unacked messages will be redelivered", "err", err)
	}
	// Give paho's ack ticker (50 ms) time to send the final PUBACKs.
	time.Sleep(200 * time.Millisecond)
	dctx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer dcancel()
	_ = cm.Disconnect(dctx)
	cancel()
	b.kafka.Close()
	return nil
}

func (b *Bridge) subscribe(ctx context.Context, cm *autopaho.ConnectionManager, topic string) {
	bo := backoff.Backoff{Base: 500 * time.Millisecond, Max: 30 * time.Second}
	for attempt := 0; ctx.Err() == nil; attempt++ {
		sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		ack, err := cm.Subscribe(sctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}}})
		cancel()
		if err == nil && len(ack.Reasons) == 1 && ack.Reasons[0] < 0x80 {
			b.connected.Store(true)
			b.log.Info("subscribed", "topic", topic)
			return
		}
		if errors.Is(err, autopaho.ConnectionDownError) {
			return
		}
		b.log.Error("subscribe failed; retrying", "topic", topic, "err", err, "suback", ack)
		if backoff.Sleep(ctx, bo.Delay(attempt)) != nil {
			return
		}
	}
}

// Ready reports whether the bridge is subscribed and Kafka is accepting
// writes (R5.5).
func (b *Bridge) Ready(context.Context) error {
	if !b.connected.Load() {
		return errors.New("not subscribed to the broker")
	}
	if n := b.retrying.Load(); n > 0 {
		// Their PUBACKs, and every one after them, are held back.
		return fmt.Errorf("%d messages refused by Kafka are being written again", n)
	}
	if b.pending.Load() == 0 {
		return nil
	}
	since := time.Since(time.Unix(0, b.lastOK.Load()))
	if since > b.cfg.ReadyAfter {
		return fmt.Errorf("%d messages waiting for Kafka; no write confirmed for %s", b.pending.Load(), since.Round(time.Second))
	}
	return nil
}
