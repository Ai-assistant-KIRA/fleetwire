package bridge

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/kafkahdr"
)

// fakeKafka records produce calls; the test answers them with complete.
type fakeKafka struct {
	mu       sync.Mutex
	records  []*kgo.Record
	promises []func(*kgo.Record, error)
}

func (k *fakeKafka) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.records = append(k.records, r)
	k.promises = append(k.promises, promise)
}

// complete answers the oldest unanswered write with err, as franz-go's
// promise would.
func (k *fakeKafka) complete(t *testing.T, err error) {
	t.Helper()
	k.mu.Lock()
	if len(k.promises) == 0 {
		k.mu.Unlock()
		t.Fatal("no write is waiting for Kafka")
	}
	p, r := k.promises[0], k.records[len(k.records)-len(k.promises)]
	k.promises = k.promises[1:]
	k.mu.Unlock()
	p(r, err)
}

func (k *fakeKafka) writes() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.records)
}

// acks records the MQTT messages the bridge marked as handled.
type acks struct {
	mu  sync.Mutex
	got []*paho.Publish
}

func (a *acks) ack(p *paho.Publish) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.got = append(a.got, p)
	return nil
}

func (a *acks) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.got)
}

func testBridge(t *testing.T) (*Bridge, *fakeKafka) {
	t.Helper()
	b := newBridge(Config{Topic: "uplink"}, slog.New(slog.DiscardHandler), prometheus.NewRegistry())
	k := &fakeKafka{}
	b.prod = k
	b.connected.Store(true)
	return b, k
}

func uplink(dev uuid.UUID) *paho.Publish {
	return &paho.Publish{QoS: 1, Topic: "fw/v1/d/" + dev.String() + "/up", Payload: []byte("batch")}
}

func (b *Bridge) pendingGauge() float64 { return testutil.ToFloat64(b.m.pending) }

func (b *Bridge) forwarded(result string) float64 {
	return testutil.ToFloat64(b.m.forwarded.WithLabelValues(result))
}

// A message is acked to Mosquitto only once Kafka has confirmed it, and
// the record carries the device ID as key and header.
func TestMessageIsAckedAfterKafkaConfirms(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, k := testBridge(t)
		var a acks
		dev := uuid.New()
		time.Sleep(time.Minute)
		b.handle(t.Context(), uplink(dev), a.ack)
		if a.count() != 0 || b.pendingGauge() != 1 || k.writes() != 1 {
			t.Fatalf("before Kafka confirmed: %d acks, pending %v, %d writes", a.count(), b.pendingGauge(), k.writes())
		}
		rec := k.records[0]
		if string(rec.Key) != dev.String() || string(kafkahdr.Get(rec, kafkahdr.DeviceID)) != dev.String() {
			t.Fatalf("record key %q, device header %q", rec.Key, kafkahdr.Get(rec, kafkahdr.DeviceID))
		}
		k.complete(t, nil)
		if a.count() != 1 || b.pendingGauge() != 0 || b.forwarded("ok") != 1 {
			t.Fatalf("after Kafka confirmed: %d acks, pending %v, ok %v", a.count(), b.pendingGauge(), b.forwarded("ok"))
		}
		if got := testutil.ToFloat64(b.m.lastOK); got != float64(time.Now().Unix()) {
			t.Fatalf("last produce timestamp %v, want now (%d)", got, time.Now().Unix())
		}
		if err := b.Ready(t.Context()); err != nil {
			t.Fatalf("Ready: %v", err)
		}
	})
}

// A message that can never be valid is acked at once and never written.
func TestRejectedMessageIsAckedWithoutWriting(t *testing.T) {
	b, k := testBridge(t)
	var a acks
	b.handle(t.Context(), &paho.Publish{QoS: 1, Topic: "fw/v1/d/not-a-uuid/up"}, a.ack)
	if a.count() != 1 || k.writes() != 0 || b.forwarded("rejected") != 1 || b.pendingGauge() != 0 {
		t.Fatalf("%d acks, %d writes, rejected %v, pending %v", a.count(), k.writes(), b.forwarded("rejected"), b.pendingGauge())
	}
}

// A write Kafka refuses for good (franz-go gives up only on errors it cannot
// retry) leaves the message unacknowledged, so it keeps holding back the
// PUBACKs after it. It must stay in pending_acks, fail readiness, and be
// written again until Kafka takes it. Dropping it from pending made the
// bridge report ready while its MQTT session had stopped acknowledging
// anything.
func TestRefusedWriteStaysPendingAndIsWrittenAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, k := testBridge(t)
		var a acks
		b.handle(t.Context(), uplink(uuid.New()), a.ack)
		k.complete(t, errors.New("TOPIC_AUTHORIZATION_FAILED"))
		if a.count() != 0 || b.pendingGauge() != 1 || b.forwarded("error") != 1 {
			t.Fatalf("after a refused write: %d acks, pending %v, error %v", a.count(), b.pendingGauge(), b.forwarded("error"))
		}
		if err := b.Ready(t.Context()); err == nil {
			t.Fatal("Ready while a refused message is held back")
		}
		// The first retry comes within retryBackoff.Base, the second
		// within twice that.
		time.Sleep(retryBackoff.Base)
		synctest.Wait()
		if k.writes() != 2 {
			t.Fatalf("%d writes, want the message written again", k.writes())
		}
		k.complete(t, errors.New("TOPIC_AUTHORIZATION_FAILED"))
		time.Sleep(2 * retryBackoff.Base)
		synctest.Wait()
		if k.writes() != 3 {
			t.Fatalf("%d writes, want a third attempt", k.writes())
		}
		k.complete(t, nil)
		if a.count() != 1 || b.pendingGauge() != 0 || b.forwarded("ok") != 1 || b.forwarded("error") != 2 {
			t.Fatalf("after Kafka took it: %d acks, pending %v, ok %v, error %v", a.count(), b.pendingGauge(), b.forwarded("ok"), b.forwarded("error"))
		}
		if err := b.Ready(t.Context()); err != nil {
			t.Fatalf("Ready after the write succeeded: %v", err)
		}
	})
}

// A write that fails because the bridge is stopping is not retried: the
// message stays in Mosquitto's session for the next run.
func TestFailedWriteIsNotRetriedWhileStopping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, k := testBridge(t)
		ctx, cancel := context.WithCancel(t.Context())
		b.handle(ctx, uplink(uuid.New()), (&acks{}).ack)
		cancel()
		k.complete(t, context.Canceled)
		time.Sleep(time.Minute)
		synctest.Wait()
		if k.writes() != 1 || b.pendingGauge() != 0 || b.retrying.Load() != 0 {
			t.Fatalf("%d writes, pending %v, retrying %d; want one write and nothing left", k.writes(), b.pendingGauge(), b.retrying.Load())
		}
	})
}

// Once the connection a message came on is gone, its PUBACK cannot be sent
// and Mosquitto delivers it again on the next connection, so the bridge
// stops writing that copy.
func TestRetryStopsWhenTheConnectionIsLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, k := testBridge(t)
		var a acks
		b.handle(t.Context(), uplink(uuid.New()), a.ack)
		k.complete(t, errors.New("UNKNOWN_TOPIC_OR_PARTITION"))
		b.connectionLost()
		time.Sleep(time.Minute)
		synctest.Wait()
		if k.writes() != 1 || a.count() != 0 || b.pendingGauge() != 0 || b.retrying.Load() != 0 {
			t.Fatalf("%d writes, %d acks, pending %v, retrying %d; want the copy given up", k.writes(), a.count(), b.pendingGauge(), b.retrying.Load())
		}
	})
}

// Before the first write, the last-produce gauge holds the start time. At
// 0 it read as a bridge that had not written since 1970, so the alert on it
// could fire as soon as a new bridge had a write in flight. Readiness gives
// a new bridge the same ReadyAfter grace.
func TestColdStartCountsFromProcessStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, k := testBridge(t)
		if got := testutil.ToFloat64(b.m.lastOK); got != float64(time.Now().Unix()) {
			t.Fatalf("last produce timestamp %v at start, want the start time %d", got, time.Now().Unix())
		}
		b.handle(t.Context(), uplink(uuid.New()), (&acks{}).ack)
		if err := b.Ready(t.Context()); err != nil {
			t.Fatalf("Ready right after start with a write in flight: %v", err)
		}
		time.Sleep(b.cfg.ReadyAfter + time.Second)
		if err := b.Ready(t.Context()); err == nil {
			t.Fatal("Ready although no write was confirmed for longer than ReadyAfter")
		}
		k.complete(t, nil)
		b.connectionLost()
		if err := b.Ready(t.Context()); err == nil {
			t.Fatal("Ready while not subscribed")
		}
	})
}
