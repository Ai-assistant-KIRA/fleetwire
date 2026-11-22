package ingest

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
)

// ackBroker stands in for Mosquitto's retained ack topics. Publishes for a
// device in gates wait for one value from its gate: nil lets the publish
// through, an error fails it.
type ackBroker struct {
	mu       sync.Mutex
	started  map[uuid.UUID][]uint64 // every publish attempt, in order
	retained map[uuid.UUID][]uint64 // successful publishes, in order
	gates    map[uuid.UUID]chan error
}

func newAckBroker() *ackBroker {
	return &ackBroker{started: map[uuid.UUID][]uint64{}, retained: map[uuid.UUID][]uint64{}, gates: map[uuid.UUID]chan error{}}
}

func (b *ackBroker) gate(dev uuid.UUID) chan error {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan error)
	b.gates[dev] = ch
	return ch
}

func (b *ackBroker) publish(ctx context.Context, p *paho.Publish) error {
	dev := uuid.MustParse(strings.TrimSuffix(strings.TrimPrefix(p.Topic, "fw/v1/d/"), "/ack"))
	var a fleetwirev1.Ack
	if err := proto.Unmarshal(p.Payload, &a); err != nil {
		return err
	}
	b.mu.Lock()
	b.started[dev] = append(b.started[dev], a.GetAckedThrough())
	gate := b.gates[dev]
	b.mu.Unlock()
	if gate != nil {
		select {
		case err := <-gate:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	b.mu.Lock()
	b.retained[dev] = append(b.retained[dev], a.GetAckedThrough())
	b.mu.Unlock()
	return nil
}

func (b *ackBroker) attempts(dev uuid.UUID) []uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.started[dev])
}

func (b *ackBroker) stored(dev uuid.UUID) []uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.retained[dev])
}

// startAcks runs an ack publisher with workers against b, connected. Call
// it inside a synctest bubble; the workers stop when the test ends.
func startAcks(t *testing.T, b *ackBroker, workers int) *AckPublisher {
	t.Helper()
	a := NewAckPublisher(AckConfig{Workers: workers}, slog.New(slog.DiscardHandler), prometheus.NewRegistry())
	a.publishAck = b.publish
	a.connected.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	a.startWorkers(ctx)
	t.Cleanup(func() {
		cancel()
		a.wg.Wait()
	})
	return a
}

var noSpan trace.SpanContext

// Two workers must not publish acks of one device at the same time: the
// later publish could reach the broker first, and the retained ack would
// move back.
func TestAcksOfOneDeviceArePublishedInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newAckBroker()
		a := startAcks(t, b, 4)
		dev := uuid.New()
		gate := b.gate(dev)
		a.Offer(noSpan, dev, 0, 100)
		synctest.Wait()
		a.Offer(noSpan, dev, 0, 200)
		synctest.Wait()
		if got := b.attempts(dev); !slices.Equal(got, []uint64{100}) {
			t.Fatalf("publishes started %v while the ack for 100 was in flight, want only [100]", got)
		}
		gate <- nil
		gate <- nil
		synctest.Wait()
		if got := b.stored(dev); !slices.Equal(got, []uint64{100, 200}) {
			t.Fatalf("retained acks %v, want [100 200]", got)
		}
	})
}

// An ack whose publish failed is not published again over a newer one.
func TestFailedAckDoesNotReplaceANewerOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newAckBroker()
		a := startAcks(t, b, 2)
		dev := uuid.New()
		gate := b.gate(dev)
		a.Offer(noSpan, dev, 0, 100)
		synctest.Wait()
		a.Offer(noSpan, dev, 0, 200)
		gate <- errors.New("publish timed out")
		gate <- nil
		synctest.Wait()
		if got := b.attempts(dev); !slices.Equal(got, []uint64{100, 200}) {
			t.Fatalf("publish attempts %v, want [100 200]", got)
		}
		if got := b.stored(dev); !slices.Equal(got, []uint64{200}) {
			t.Fatalf("retained acks %v, want [200]", got)
		}
	})
}

// Release waits for the acks of the partitions being given up and drops
// whatever is not out by its deadline, so the next owner's acks for those
// devices cannot be overwritten by older ones from this instance. Acks of
// other partitions are not affected.
func TestReleaseFinishesThenDropsAPartitionsAcks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newAckBroker()
		a := startAcks(t, b, 2)
		devA, devB := uuid.New(), uuid.New()
		gateA := b.gate(devA)
		a.Offer(noSpan, devA, 1, 10)
		synctest.Wait()

		released := make(chan struct{})
		go func() {
			a.Release([]int32{1}, 5*time.Second)
			close(released)
		}()
		synctest.Wait()
		select {
		case <-released:
			t.Fatal("Release returned while an ack of the partition was in flight")
		default:
		}
		gateA <- nil
		synctest.Wait()
		select {
		case <-released:
		default:
			t.Fatal("Release still waiting after the partition's acks went out")
		}

		// A publish that hangs past the deadline: the queued ack behind it
		// is dropped, and the hanging one is not retried after it fails.
		a.Offer(noSpan, devA, 1, 11)
		synctest.Wait()
		a.Offer(noSpan, devA, 1, 12)
		a.Offer(noSpan, devB, 2, 20)
		start := time.Now()
		a.Release([]int32{1}, 5*time.Second)
		if waited := time.Since(start); waited != 5*time.Second {
			t.Fatalf("Release returned after %v, want its 5 s deadline", waited)
		}
		time.Sleep(10 * time.Second) // the publish of 11 times out
		synctest.Wait()
		if got := b.attempts(devA); !slices.Equal(got, []uint64{10, 11}) {
			t.Fatalf("publish attempts for the released partition %v, want [10 11]", got)
		}
		if got := b.stored(devB); !slices.Equal(got, []uint64{20}) {
			t.Fatalf("retained acks of the kept partition %v, want [20]", got)
		}

		// The partition comes back later: its acks flow again.
		a.Offer(noSpan, devA, 1, 13)
		gateA <- nil
		synctest.Wait()
		if got := b.stored(devA); !slices.Equal(got, []uint64{10, 13}) {
			t.Fatalf("retained acks after the partition came back %v, want [10 13]", got)
		}
	})
}

// With the broker connection down nothing can go out, so Release drops the
// partition's acks at once instead of holding up the rebalance.
func TestReleaseDoesNotWaitWhileDisconnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newAckBroker()
		a := startAcks(t, b, 1)
		a.connected.Store(false)
		dev := uuid.New()
		a.Offer(noSpan, dev, 3, 7)
		start := time.Now()
		a.Release([]int32{3}, 5*time.Second)
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("Release waited %v while disconnected", waited)
		}
		a.connected.Store(true)
		a.wake()
		time.Sleep(time.Second)
		synctest.Wait()
		if got := b.attempts(dev); len(got) != 0 {
			t.Fatalf("dropped ack was published after reconnecting: %v", got)
		}
	})
}
