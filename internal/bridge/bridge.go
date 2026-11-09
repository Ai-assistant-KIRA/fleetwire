// Package bridge moves device messages from Mosquitto into Kafka (R5).
//
// Each MQTT message is acknowledged to Mosquitto only after Kafka has
// confirmed the write with acks=all. paho sends PUBACKs strictly in arrival
// order (checked in task 0.2), so one slow write holds back every later
// PUBACK; once the receive maximum is reached Mosquitto stops sending and
// queues in the persistent session. That is the backpressure when Kafka is
// slow or down (R5.5).
package bridge

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
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
	Brokers        []string
	Topic          string
	// ReadyAfter: readiness fails when messages are pending and no write
	// has succeeded for this long (default 10 s).
	ReadyAfter time.Duration
}

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
	tracer    trace.Tracer
	connected atomic.Bool
	pending   atomic.Int64
	lastOK    atomic.Int64
	started   time.Time
}

// New creates the Kafka producer. Nothing connects to MQTT until Run.
func New(cfg Config, log *slog.Logger, reg prometheus.Registerer) (*Bridge, error) {
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
			Help: "MQTT messages handled, by result (ok, error, rejected)."}, []string{"result"}),
		pending: prometheus.NewGauge(prometheus.GaugeOpts{Name: "fleetwire_bridge_pending_acks",
			Help: "Messages received but not yet confirmed by Kafka."}),
		produceSecs: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "fleetwire_bridge_produce_seconds",
			Help: "Time from MQTT receive to Kafka confirmation.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 16)}),
		lastOK: prometheus.NewGauge(prometheus.GaugeOpts{Name: "fleetwire_bridge_last_produce_timestamp_seconds",
			Help: "Unix time of the last confirmed Kafka write."}),
		mqtt: mqttconn.NewMetrics(reg, "fleetwire_bridge"),
	}
	reg.MustRegister(b.m.forwarded, b.m.pending, b.m.produceSecs, b.m.lastOK)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DefaultProduceTopic(cfg.Topic),
		// acks=all and idempotence are the franz-go defaults; stated here
		// because the guarantee depends on them (R5.3).
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProduceRequestTimeout(30*time.Second),
		kgo.MaxBufferedBytes(64<<20),
		kgo.MaxBufferedRecords(10_000),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	b.kafka = cl
	return b, nil
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

func (b *Bridge) ack(pr paho.PublishReceived) {
	if err := pr.Client.Ack(pr.Packet); err != nil && !errors.Is(err, paho.ErrPacketNotFound) {
		b.log.Warn("mqtt ack failed", "err", err)
	}
	// ErrPacketNotFound: the connection was replaced since the message
	// arrived. Mosquitto redelivers it, it is written to Kafka again, and
	// ingest removes the duplicate.
}

// handle is called by paho for each message, in arrival order.
func (b *Bridge) handle(ctx context.Context, pr paho.PublishReceived) {
	received := time.Now()
	dev, ok := deviceFromTopic(pr.Packet.Topic)
	if !ok || len(pr.Packet.Payload) > uplinkpb.MaxMessageBytes {
		// Only a broken ACL or broker limit lets such a message through.
		// It can never become valid, so it is acked and counted.
		b.m.forwarded.WithLabelValues("rejected").Inc()
		b.log.Error("rejecting message: bad topic or too large", "topic", pr.Packet.Topic, "bytes", len(pr.Packet.Payload))
		b.ack(pr)
		return
	}
	sctx, span := b.tracer.Start(ctx, "bridge.forward", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("fleetwire.device_id", dev.String()), attribute.Int("messaging.message.body.size", len(pr.Packet.Payload))))
	rec := &kgo.Record{
		Key:   []byte(dev.String()),
		Value: pr.Packet.Payload,
		Headers: []kgo.RecordHeader{
			{Key: kafkahdr.DeviceID, Value: []byte(dev.String())},
			{Key: kafkahdr.ReceivedAt, Value: []byte(strconv.FormatInt(received.UnixNano(), 10))},
		},
	}
	otel.GetTextMapPropagator().Inject(sctx, kafkahdr.Carrier{Record: rec})
	b.m.pending.Inc()
	b.pending.Add(1)
	b.kafka.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		b.m.pending.Dec()
		b.pending.Add(-1)
		defer span.End()
		if err != nil {
			b.m.forwarded.WithLabelValues("error").Inc()
			span.SetStatus(codes.Error, err.Error())
			if ctx.Err() == nil {
				// Not acked: it stays un-PUBACKed, which holds back the
				// acks after it and shows up in pending_acks and readiness.
				b.log.Error("kafka write failed; message left unacknowledged", "device_id", dev, "err", err)
			}
			return
		}
		now := time.Now()
		b.lastOK.Store(now.UnixNano())
		b.m.lastOK.Set(float64(now.Unix()))
		b.m.produceSecs.Observe(now.Sub(received).Seconds())
		b.m.forwarded.WithLabelValues("ok").Inc()
		b.ack(pr)
	})
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
		OnDown: func() { b.connected.Store(false) },
		OnPublish: func(pr paho.PublishReceived) (bool, error) {
			b.handle(runCtx, pr)
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
	if b.pending.Load() == 0 {
		return nil
	}
	last := b.lastOK.Load()
	since := time.Since(b.started)
	if last != 0 {
		since = time.Since(time.Unix(0, last))
	}
	if since > b.cfg.ReadyAfter {
		return fmt.Errorf("%d messages waiting for Kafka; no write confirmed for %s", b.pending.Load(), since.Round(time.Second))
	}
	return nil
}
