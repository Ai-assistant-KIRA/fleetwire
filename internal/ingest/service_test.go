package ingest

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
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

// Readiness follows outages of the database or the keyring, not batches
// that fail for their own reasons: those end in the DLQ, and one device
// sending them must not make the instance look broken.
func TestOnlyOutagesMakeIngestUnready(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		proc := &fakeProc{}
		s, dlq := testService(t, proc)
		proc.fn = func(int) (Outcome, error) {
			if s.failingSince.Load() != 0 {
				t.Error("a non-transient batch error marked the database as failing")
			}
			return Outcome{}, errors.New("value out of range for this batch")
		}
		if err := s.handle(t.Context(), &kgo.Record{Key: []byte(uuid.NewString())}); err != nil {
			t.Fatal(err)
		}
		if recs := dlq(); len(recs) != 1 || dlqReason(recs[0]) != "db_error" {
			t.Fatalf("a batch that keeps failing should end in the DLQ as db_error; got %d records", len(recs))
		}
		if err := s.Ready(t.Context()); err != nil {
			t.Fatalf("Ready after a failing batch: %v", err)
		}

		proc.fn = func(call int) (Outcome, error) {
			if call <= 8 {
				return Outcome{}, keyring.ErrUnavailable
			}
			if s.failingSince.Load() == 0 {
				t.Error("a keyring outage did not mark ingest as failing")
			}
			return Outcome{Result: ResultStored}, nil
		}
		proc.mu.Lock()
		proc.calls = 0
		proc.mu.Unlock()
		if err := s.handle(t.Context(), &kgo.Record{Key: []byte(uuid.NewString())}); err != nil {
			t.Fatal(err)
		}
		if s.failingSince.Load() != 0 {
			t.Fatal("a stored batch did not end the run of failures")
		}
	})
}

// Records of one device must stay in arrival order (it keeps the device's
// row lock uncontended), and nothing may be lost or repeated by the split.
func TestByDeviceKeepsEachDevicesOrder(t *testing.T) {
	var records []*kgo.Record
	for i, key := range []string{"a", "b", "a", "c", "b", "a", ""} {
		records = append(records, &kgo.Record{Key: []byte(key), Offset: int64(i)})
	}
	var got [][]int64
	for _, g := range byDevice(records) {
		var offsets []int64
		for _, r := range g {
			offsets = append(offsets, r.Offset)
		}
		got = append(got, offsets)
	}
	want := [][]int64{{0, 2, 5}, {1, 4}, {3}, {6}}
	if !slices.EqualFunc(got, want, slices.Equal[[]int64]) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
	if byDevice(nil) != nil {
		t.Fatal("no records should give no groups")
	}
}
