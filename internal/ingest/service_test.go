package ingest

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/kafkahdr"
)

// fakeProc answers Process with fn, called with the 1-based call number.
type fakeProc struct {
	mu    sync.Mutex
	calls int
	fn    func(call int) (Outcome, error)
}

func (p *fakeProc) Process(context.Context, uuid.UUID, []byte, time.Time) (Outcome, error) {
	p.mu.Lock()
	p.calls++
	n := p.calls
	p.mu.Unlock()
	return p.fn(n)
}

// testService is a Service without Kafka, the database or the broker: the
// processor is fake and dead-lettered records are collected.
func testService(t *testing.T, proc processor) (*Service, func() []*kgo.Record) {
	t.Helper()
	reg := prometheus.NewRegistry()
	log := slog.New(slog.DiscardHandler)
	s := &Service{cfg: Config{DLQ: DLQTopic, ReadyAfter: 10 * time.Second}, log: log, proc: proc, m: NewMetrics(reg),
		acks: NewAckPublisher(AckConfig{}, log, reg), tracer: otel.Tracer("test")}
	s.acks.connected.Store(true)
	var mu sync.Mutex
	var dlq []*kgo.Record
	s.produceDLQ = func(_ context.Context, r *kgo.Record) error {
		mu.Lock()
		defer mu.Unlock()
		dlq = append(dlq, r)
		return nil
	}
	return s, func() []*kgo.Record {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(dlq)
	}
}

func dlqReason(r *kgo.Record) string { return string(kafkahdr.Get(r, kafkahdr.DLQReason)) }

// Each dead-lettered batch adds one to batches_total{result="dlq"}, which
// the ingest correctness SLO divides by the total. An invalid batch used to
// be counted twice.
func TestDeadLetteredBatchesAreCountedOnce(t *testing.T) {
	s, dlq := testService(t, &fakeProc{fn: func(int) (Outcome, error) {
		return Outcome{Result: ResultDLQ, Reason: "decode", Detail: "garbage"}, nil
	}})
	if err := s.handle(t.Context(), &kgo.Record{Key: []byte(uuid.NewString()), Value: []byte{0xff}}); err != nil {
		t.Fatal(err)
	}
	if err := s.handle(t.Context(), &kgo.Record{Key: []byte("not-a-device"), Value: []byte{0xff}}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(s.m.Batches.WithLabelValues(string(ResultDLQ))); got != 2 {
		t.Fatalf("batches_total{result=\"dlq\"} = %v after two dead-lettered batches", got)
	}
	recs := dlq()
	if len(recs) != 2 || dlqReason(recs[0]) != "decode" || dlqReason(recs[1]) != "bad_key" {
		t.Fatalf("DLQ records: %d", len(recs))
	}
}
