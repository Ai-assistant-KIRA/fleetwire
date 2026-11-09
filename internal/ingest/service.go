package ingest

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/backoff"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/kafkahdr"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
)

// Topic names and layout (design 7).
const (
	UplinkTopic = "fleetwire.uplink.v1"
	DLQTopic    = "fleetwire.uplink.dlq.v1"
	Group       = "fleetwire-ingest"
)

// Config is the ingest service's settings.
type Config struct {
	Brokers    []string
	Topic      string
	DLQ        string
	Group      string
	InstanceID string
	Workers    int
	MQTTURL    string
	TLS        *tls.Config
	AckWorkers int
	// ReadyAfter: readiness fails after the database has been failing this
	// long (default 10 s).
	ReadyAfter time.Duration
}

// Service consumes Kafka and stores batches.
type Service struct {
	cfg    Config
	log    *slog.Logger
	proc   *Processor
	keys   *tenantkey.Manager
	acks   *AckPublisher
	kafka  *kgo.Client
	m      *Metrics
	lag    *prometheus.GaugeVec
	tracer trace.Tracer
	// failingSince is the Unix nano time of the first database error in the
	// current run of failures, or 0.
	failingSince atomic.Int64
}

// New connects to Kafka (lazily) and prepares the ack publisher.
func New(cfg Config, db *store.DB, keys *tenantkey.Manager, log *slog.Logger, reg prometheus.Registerer) (*Service, error) {
	if cfg.Topic == "" {
		cfg.Topic = UplinkTopic
	}
	if cfg.DLQ == "" {
		cfg.DLQ = DLQTopic
	}
	if cfg.Group == "" {
		cfg.Group = Group
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 16
	}
	if cfg.ReadyAfter <= 0 {
		cfg.ReadyAfter = 10 * time.Second
	}
	m := NewMetrics(reg)
	s := &Service{cfg: cfg, log: log, keys: keys, m: m, proc: NewProcessor(db, keys, log, m),
		tracer: otel.Tracer("fleetwire/ingest")}
	s.lag = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "fleetwire_ingest_consumer_lag",
		Help: "Records behind the partition's high watermark after the last poll."}, []string{"partition"})
	reg.MustRegister(s.lag)
	s.acks = NewAckPublisher(AckConfig{MQTTURL: cfg.MQTTURL, ClientID: "fleetwire-ingest-" + cfg.InstanceID,
		TLS: cfg.TLS, Workers: cfg.AckWorkers}, log, reg)
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("fleetwire-ingest-" + cfg.InstanceID),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		// Offsets are committed only after the database commit (R6.4).
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500 * time.Millisecond),
		kgo.SessionTimeout(30 * time.Second),
		kgo.RebalanceTimeout(5 * time.Minute),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	s.kafka = cl
	return s, nil
}

// Run consumes until ctx ends.
func (s *Service) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	actx, stopAcks := context.WithCancel(context.WithoutCancel(ctx))
	if err := s.acks.Start(actx); err != nil {
		stopAcks()
		return err
	}
	wg.Go(func() { s.flushSeals(actx) })
	defer func() {
		s.kafka.Close()
		// Acks for everything committed so far get a moment to go out.
		s.acks.Drain(5 * time.Second)
		stopAcks()
		s.acks.wg.Wait()
		wg.Wait()
		if err := s.keys.Flush(context.WithoutCancel(ctx)); err != nil {
			s.log.Warn("final seal count flush failed", "err", err)
		}
	}()
	for {
		fetches := s.kafka.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(topic string, p int32, err error) {
			s.log.Warn("kafka fetch error", "topic", topic, "partition", p, "err", err)
		})
		var all []*kgo.Record
		var parts []kgo.FetchTopicPartition
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) > 0 {
				parts = append(parts, p)
				all = append(all, p.Records...)
			}
		})
		if err := s.processPartitions(ctx, parts); err != nil {
			// Shutting down mid-poll: nothing is committed, so the records
			// are delivered again to whoever owns the partitions next.
			s.kafka.AllowRebalance()
			return nil
		}
		if len(all) > 0 {
			if err := s.kafka.CommitRecords(ctx, all...); err != nil && ctx.Err() == nil {
				// Not fatal: the records are processed again later, and the
				// coverage check makes that a no-op.
				s.log.Warn("offset commit failed; records will be reprocessed", "err", err)
			}
		}
		for _, p := range parts {
			last := p.Records[len(p.Records)-1].Offset
			s.lag.WithLabelValues(strconv.Itoa(int(p.Partition))).Set(float64(max(p.HighWatermark-last-1, 0)))
		}
		s.kafka.AllowRebalance()
	}
}

// processPartitions handles partitions in parallel and the records of one
// partition in order. Order is not needed for correctness (R6.8); it keeps
// the device row lock uncontended in the common case.
func (s *Service) processPartitions(ctx context.Context, parts []kgo.FetchTopicPartition) error {
	sem := make(chan struct{}, s.cfg.Workers)
	var wg sync.WaitGroup
	var failed atomic.Bool
	for _, p := range parts {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			for _, r := range p.Records {
				if err := s.handle(ctx, r); err != nil {
					failed.Store(true)
					return
				}
			}
		})
	}
	wg.Wait()
	if failed.Load() {
		return context.Canceled
	}
	return nil
}

// handle processes one record until it is stored, dropped or dead-lettered.
// It only fails when ctx ends.
func (s *Service) handle(ctx context.Context, r *kgo.Record) error {
	pctx := otel.GetTextMapPropagator().Extract(ctx, kafkahdr.Carrier{Record: r})
	pctx, span := s.tracer.Start(pctx, "ingest.batch", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	device, err := uuid.Parse(string(r.Key))
	if err != nil {
		return s.deadLetter(ctx, r, "bad_key", "record key is not a device ID")
	}
	span.SetAttributes(attribute.String("fleetwire.device_id", device.String()))
	var received time.Time
	if v := kafkahdr.Get(r, kafkahdr.ReceivedAt); v != nil {
		if n, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			received = time.Unix(0, n)
		}
	}
	bo := backoff.Backoff{Base: 100 * time.Millisecond, Max: 10 * time.Second}
	permanentFailures := 0
	for attempt := 0; ; attempt++ {
		out, err := s.proc.Process(pctx, device, r.Value, received)
		if err == nil {
			s.failingSince.Store(0)
			s.m.Batches.WithLabelValues(string(out.Result)).Inc()
			span.SetAttributes(attribute.String("fleetwire.result", string(out.Result)), attribute.Int("fleetwire.stored", out.Stored))
			if out.Result == ResultDLQ {
				return s.deadLetter(ctx, r, out.Reason, out.Detail)
			}
			if out.Ack {
				s.acks.Offer(trace.SpanContextFromContext(pctx), device, out.AckThrough)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.m.DBRetries.Inc()
		s.failingSince.CompareAndSwap(0, time.Now().UnixNano())
		switch {
		case errors.Is(err, ErrOverlap):
			s.log.Error("exclusion constraint rejected a chunk; sending the batch to the DLQ (bug signal, the device will resend)", "device_id", device, "err", err)
			span.SetStatus(codes.Error, "overlap")
			return s.deadLetter(ctx, r, "exclusion_violation", err.Error())
		case store.IsTransient(err):
			if attempt%20 == 0 {
				s.log.Warn("database unavailable; retrying the batch", "device_id", device, "attempt", attempt, "err", err)
			}
		default:
			permanentFailures++
			s.log.Error("batch failed with a non-transient error", "device_id", device, "attempt", permanentFailures, "err", err)
			if permanentFailures >= 5 {
				span.SetStatus(codes.Error, err.Error())
				return s.deadLetter(ctx, r, "db_error", err.Error())
			}
		}
		if backoff.Sleep(ctx, bo.Delay(attempt)) != nil {
			return ctx.Err()
		}
	}
}

// deadLetter writes the record to the DLQ with the reason, retrying until
// Kafka accepts it. The batch is not acked, so a healthy agent resends it.
func (s *Service) deadLetter(ctx context.Context, r *kgo.Record, reason, detail string) error {
	s.m.Batches.WithLabelValues(string(ResultDLQ)).Inc()
	if len(detail) > 1024 {
		detail = detail[:1024]
	}
	rec := &kgo.Record{Topic: s.cfg.DLQ, Key: r.Key, Value: r.Value, Headers: append(append([]kgo.RecordHeader(nil), r.Headers...),
		kgo.RecordHeader{Key: kafkahdr.DLQReason, Value: []byte(reason)},
		kgo.RecordHeader{Key: kafkahdr.DLQDetail, Value: []byte(detail)})}
	bo := backoff.Backoff{Base: 200 * time.Millisecond, Max: 10 * time.Second}
	return backoff.Retry(ctx, bo, backoff.RetryOptions{}, func(ctx context.Context) error {
		return s.kafka.ProduceSync(ctx, rec).FirstErr()
	})
}

func (s *Service) flushSeals(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.keys.Flush(ctx); err != nil {
				s.log.Warn("seal count flush failed; retrying", "err", err)
			}
		}
	}
}

// Ready reports whether the database works and acks can be published.
func (s *Service) Ready(context.Context) error {
	if since := s.failingSince.Load(); since != 0 && time.Since(time.Unix(0, since)) > s.cfg.ReadyAfter {
		return fmt.Errorf("database failing for %s", time.Since(time.Unix(0, since)).Round(time.Second))
	}
	if !s.acks.Connected() {
		return errors.New("ack publisher not connected to the broker")
	}
	return nil
}

// EnsureTopics creates the uplink and DLQ topics if they do not exist.
func EnsureTopics(ctx context.Context, brokers []string, replication int16, log *slog.Logger) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	str := func(s string) *string { return &s }
	minISR := "2"
	if replication < 2 {
		minISR = "1"
	}
	for _, t := range []struct {
		name       string
		partitions int32
		retention  string
	}{
		{UplinkTopic, 24, strconv.FormatInt((7 * 24 * time.Hour).Milliseconds(), 10)},
		{DLQTopic, 3, strconv.FormatInt((30 * 24 * time.Hour).Milliseconds(), 10)},
	} {
		resp, err := adm.CreateTopics(ctx, t.partitions, replication, map[string]*string{
			"min.insync.replicas": str(minISR),
			"retention.ms":        str(t.retention),
		}, t.name)
		if err != nil {
			return fmt.Errorf("create topic %s: %w", t.name, err)
		}
		for _, r := range resp {
			switch {
			case r.Err == nil:
				log.Info("topic created", "topic", r.Topic, "partitions", t.partitions, "replication", replication)
			case errors.Is(r.Err, kerr.TopicAlreadyExists):
				log.Info("topic exists", "topic", r.Topic)
			default:
				return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
			}
		}
	}
	return nil
}
