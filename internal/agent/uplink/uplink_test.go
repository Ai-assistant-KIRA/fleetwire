package uplink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"

	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/uplinkpb"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

// memPub is an in-memory Publisher: a test double for the MQTT transport,
// used only to drive this one state machine. The e2e and chaos tests run
// the real broker.
type memPub struct {
	mu      sync.Mutex
	batches []*fleetwirev1.Batch
	fail    int // fail this many publishes with a timeout-like error
}

func (p *memPub) Publish(_ context.Context, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail > 0 {
		p.fail--
		return errors.New("publish timed out")
	}
	b, err := uplinkpb.DecodeAndValidate(payload)
	if err != nil {
		return err
	}
	p.batches = append(p.batches, b)
	return nil
}

func (p *memPub) take() []*fleetwirev1.Batch {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.batches
	p.batches = nil
	return out
}

type ranges [][2]uint64

func seqRanges(bs []*fleetwirev1.Batch) ranges {
	var out ranges
	for _, b := range bs {
		if len(b.GetReadings()) > 0 {
			out = append(out, [2]uint64{b.GetFirstSeq(), uplinkpb.LastSeq(b)})
		}
	}
	return out
}

func reading(seq uint64) []byte {
	b, err := proto.Marshal(&fleetwirev1.Reading{CollectedUnixNano: int64(seq) + 1, Collector: "test", Metric: "test.value", Value: float64(seq)})
	if err != nil {
		panic(err)
	}
	return b
}

func fill(t *testing.T, l *wal.Log, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := l.Append(reading(l.Next())); err != nil {
			t.Fatal(err)
		}
	}
}

func ack(t *testing.T, u *Uplink, n uint64) {
	t.Helper()
	b, _ := proto.Marshal(&fleetwirev1.Ack{AckedThrough: n})
	if err := u.HandleAck(b); err != nil {
		t.Fatal(err)
	}
}

type rig struct {
	dir  string
	log  *wal.Log
	pub  *memPub
	up   *Uplink
	m    *Metrics
	stop func()
}

func start(t *testing.T, cfg Config, walOpts wal.Options) *rig {
	t.Helper()
	if walOpts.Dir == "" {
		walOpts.Dir = t.TempDir()
	}
	walOpts.Sync = wal.SyncAlways
	l, _, err := wal.Open(walOpts)
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{dir: walOpts.Dir, log: l, pub: &memPub{}, m: NewMetrics(prometheus.NewRegistry())}
	r.up = New(cfg, l, r.pub, r.m, discard())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.up.Run(ctx) }()
	r.stop = func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
		if err := l.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}
	return r
}

// The tests turn the jitter off so timeouts land on exact seconds.
var base = Config{BatchSize: 10, BatchWait: time.Second, MaxInflight: 30, AckTimeout: 30 * time.Second, AgentVersion: "test",
	Rand: func() float64 { return 0 }}

func TestBatchesWindowAndAcks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := start(t, base, wal.Options{SegmentSize: 300})
		defer r.stop()
		fill(t, r.log, 100)
		synctest.Wait()
		if got := r.pub.take(); len(got) != 0 {
			t.Fatalf("sent %d batches before connecting", len(got))
		}
		r.up.Connected()
		synctest.Wait()
		// The window (30) allows three full batches.
		if got := seqRanges(r.pub.take()); len(got) != 3 || got[0] != [2]uint64{1, 10} || got[2] != [2]uint64{21, 30} {
			t.Fatalf("first batches = %v", got)
		}
		ack(t, r.up, 20)
		synctest.Wait()
		if got := seqRanges(r.pub.take()); len(got) != 2 || got[0] != [2]uint64{31, 40} || got[1] != [2]uint64{41, 50} {
			t.Fatalf("after ack 20: %v", got)
		}
		ack(t, r.up, 5) // old: ignored
		ack(t, r.up, 20)
		synctest.Wait()
		if r.log.Acked() != 20 || len(r.pub.take()) != 0 {
			t.Fatalf("old or repeated ack changed state: acked %d", r.log.Acked())
		}
		ack(t, r.up, 100)
		time.Sleep(time.Second)
		synctest.Wait()
		r.pub.take()
		st, err := wal.LoadState(r.dir)
		if err != nil || st.Acked != 100 {
			t.Fatalf("persisted state %+v, %v", st, err)
		}
		if st := r.log.Stats(); st.Segments != 1 {
			t.Fatalf("%d segments left after everything was acked", st.Segments)
		}
		if v := testutil.ToFloat64(r.m.Unacked); v != 0 {
			t.Fatalf("unacked gauge = %v", v)
		}
	})
}

func TestPartialBatchWaitsForBatchWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := start(t, base, wal.Options{})
		defer r.stop()
		r.up.Connected()
		fill(t, r.log, 3)
		synctest.Wait()
		if got := r.pub.take(); len(got) != 0 {
			t.Fatalf("partial batch sent at once: %v", seqRanges(got))
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if got := seqRanges(r.pub.take()); len(got) != 1 || got[0] != [2]uint64{1, 3} {
			t.Fatalf("after batch wait: %v", got)
		}
	})
}

func TestAckTimeoutResendsFromAcked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := start(t, base, wal.Options{})
		defer r.stop()
		r.up.Connected()
		fill(t, r.log, 20)
		synctest.Wait()
		if got := seqRanges(r.pub.take()); len(got) != 2 {
			t.Fatalf("initial send: %v", got)
		}
		ack(t, r.up, 10)
		synctest.Wait()
		time.Sleep(31 * time.Second)
		synctest.Wait()
		if got := seqRanges(r.pub.take()); len(got) != 1 || got[0] != [2]uint64{11, 20} {
			t.Fatalf("resend after timeout: %v", got)
		}
		if v := testutil.ToFloat64(r.m.Resends); v != 1 {
			t.Fatalf("resends = %v", v)
		}
	})
}

// A backend that is slow, not lossy, must not get a full resend from every
// device every AckTimeout: the wait doubles after each timeout, and an ack
// of resent data does not shrink it again (Karn's algorithm). Only the ack
// of a batch that was sent once does.
func TestAckTimeoutBacksOffUntilAFreshAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := base
		cfg.AckTimeout = 10 * time.Second // the cap is 80 s
		r := start(t, cfg, wal.Options{})
		defer r.stop()
		r.up.Connected()
		fill(t, r.log, 5)
		time.Sleep(time.Second)
		synctest.Wait()
		if got := seqRanges(r.pub.take()); len(got) != 1 {
			t.Fatalf("first send: %v", got)
		}
		// nextSend waits for the next publish and returns how long it took.
		nextSend := func() time.Duration {
			from := time.Now()
			for {
				time.Sleep(time.Second)
				synctest.Wait()
				if len(r.pub.take()) > 0 {
					return time.Since(from)
				}
			}
		}
		// The loop checks once a second, so each resend comes up to 1 s late.
		for i, seconds := range []int{10, 20, 40, 80, 80} {
			got, want := nextSend(), time.Duration(seconds)*time.Second
			if got < want || got > want+2*time.Second {
				t.Fatalf("resend %d came after %v, want about %v", i+1, got, want)
			}
		}
		// Progress on resent data says nothing about how slow the backend
		// is now.
		ack(t, r.up, 3)
		synctest.Wait()
		if got := nextSend(); got < 80*time.Second {
			t.Fatalf("an ack of resent data shrank the timeout: the resend came after %v, want 80 s", got)
		}
		// New readings, sent once and acked 2 s later: a real sample, and
		// the timeout goes back to its floor.
		ack(t, r.up, 5)
		fill(t, r.log, 5)
		time.Sleep(time.Second)
		synctest.Wait()
		if got := seqRanges(r.pub.take()); len(got) != 1 || got[0] != [2]uint64{6, 10} {
			t.Fatalf("new readings: %v", got)
		}
		time.Sleep(2 * time.Second)
		ack(t, r.up, 10)
		synctest.Wait()
		fill(t, r.log, 5)
		time.Sleep(time.Second)
		synctest.Wait()
		r.pub.take()
		if got := nextSend(); got > 12*time.Second {
			t.Fatalf("after a 2 s ack the timeout should be the 10 s floor; the resend came after %v", got)
		}
	})
}

// slowBackend stores what is published after a delay, never twice, and
// acks the contiguous prefix, like ingest behind a growing Kafka lag.
type slowBackend struct {
	delay  time.Duration
	drop   int // lose this many batches, as a lossy network would
	due    []dueBatch
	stored map[uint64]bool
	acked  uint64
}

type dueBatch struct {
	at          time.Time
	first, last uint64
}

// tick takes what the device published, stores what is due, and acks.
func (b *slowBackend) tick(t *testing.T, r *rig) {
	t.Helper()
	now := time.Now()
	for _, batch := range r.pub.take() {
		if len(batch.GetReadings()) == 0 {
			continue
		}
		if b.drop > 0 {
			b.drop--
			continue
		}
		b.due = append(b.due, dueBatch{at: now.Add(b.delay), first: batch.GetFirstSeq(), last: uplinkpb.LastSeq(batch)})
	}
	kept := b.due[:0]
	for _, d := range b.due {
		if d.at.After(now) {
			kept = append(kept, d)
			continue
		}
		for s := d.first; s <= d.last; s++ {
			b.stored[s] = true
		}
	}
	b.due = kept
	before := b.acked
	for b.stored[b.acked+1] {
		b.acked++
	}
	if b.acked > before {
		ack(t, r.up, b.acked)
	}
}

// With a fixed timeout, a backend whose ack delay grows past it gets every
// device's window again and again (the 10,000-device collapse). The
// timeout must follow the measured delay, so a slow but lossless backend
// gets each reading once, and shrink again when the backend is fast, so a
// lost batch is still resent soon.
func TestAckTimeoutFollowsTheAckDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := base
		cfg.AckTimeout = 10 * time.Second
		cfg.AckTimeoutMax = 120 * time.Second
		cfg.MaxInflight = 1000
		r := start(t, cfg, wal.Options{})
		defer r.stop()
		r.up.Connected()
		b := &slowBackend{delay: 25 * time.Second, stored: map[uint64]bool{}}
		run := func(seconds int) {
			for range seconds {
				fill(t, r.log, 1)
				synctest.Wait()
				b.tick(t, r)
				synctest.Wait()
				time.Sleep(time.Second)
			}
		}
		// Acks take 25 s, two and a half times the starting timeout.
		run(100)
		early := testutil.ToFloat64(r.m.Resends)
		if early == 0 || early > 3 {
			t.Fatalf("%v resends before the first ack delay was measured, want 1 to 3", early)
		}
		run(200)
		if late := testutil.ToFloat64(r.m.Resends); late != early {
			t.Fatalf("%v resends while the backend was slow but lossless, after the delay was known", late-early)
		}
		if b.acked < 270 {
			t.Fatalf("only %d of 300 readings acked", b.acked)
		}
		// The backend is fast again; then it loses one batch.
		b.delay = time.Second
		run(100)
		resends := testutil.ToFloat64(r.m.Resends)
		b.drop = 1
		run(15)
		if got := testutil.ToFloat64(r.m.Resends); got != resends+1 {
			t.Fatalf("a lost batch caused %v resends within 15 s, want 1", got-resends)
		}
		run(10)
		if want := r.log.Next() - 1; b.acked+2 < want {
			t.Fatalf("acked %d of %d after the resend", b.acked, want)
		}
	})
}

func TestReconnectGoesBackToAcked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := start(t, base, wal.Options{})
		defer r.stop()
		r.up.Connected()
		fill(t, r.log, 20)
		synctest.Wait()
		r.pub.take()
		ack(t, r.up, 10)
		r.up.Disconnected()
		fill(t, r.log, 5)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if got := r.pub.take(); len(got) != 0 {
			t.Fatalf("sent while disconnected: %v", seqRanges(got))
		}
		r.up.Connected()
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if got := seqRanges(r.pub.take()); len(got) != 2 || got[0] != [2]uint64{11, 20} || got[1] != [2]uint64{21, 25} {
			t.Fatalf("after reconnect: %v", got)
		}
	})
}

func TestPublishErrorsAreRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := start(t, base, wal.Options{})
		defer r.stop()
		r.pub.fail = 2
		r.up.Connected()
		fill(t, r.log, 10)
		synctest.Wait()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if got := seqRanges(r.pub.take()); len(got) != 1 || got[0] != [2]uint64{1, 10} {
			t.Fatalf("after retries: %v", got)
		}
		if v := testutil.ToFloat64(r.m.PublishErrors); v != 2 {
			t.Fatalf("publish errors = %v", v)
		}
	})
}

// TestServerAckAheadMovesSeq is R3.8: the server's retained ack is above
// the device's own last seq (for example after a disk restore).
func TestServerAckAheadMovesSeq(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := start(t, base, wal.Options{})
		defer r.stop()
		fill(t, r.log, 5)
		r.up.Connected()
		ack(t, r.up, 1000)
		synctest.Wait()
		if r.log.Next() != 1001 || testutil.ToFloat64(r.m.SeqJumps) != 1 {
			t.Fatalf("next = %d, jumps = %v", r.log.Next(), testutil.ToFloat64(r.m.SeqJumps))
		}
		if st, err := wal.LoadState(r.dir); err != nil || st.Acked != 1000 {
			t.Fatalf("jump not persisted at once: %+v %v", st, err)
		}
		r.pub.take()
		fill(t, r.log, 10)
		synctest.Wait()
		if got := seqRanges(r.pub.take()); len(got) != 1 || got[0] != [2]uint64{1001, 1010} {
			t.Fatalf("after the jump: %v", got)
		}
	})
}

func TestGapsAreReportedUntilAcked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := base
		cfg.MaxInflight = 1000
		cfg.BatchSize = 100
		r := start(t, cfg, wal.Options{SegmentSize: 400, MaxSize: 1600})
		defer r.stop()
		fill(t, r.log, 300) // the disk budget drops the oldest segments
		gaps := r.log.PendingGaps()
		if len(gaps) == 0 || gaps[0].From != 1 {
			t.Fatalf("expected a budget gap from seq 1, got %v", gaps)
		}
		r.up.Connected()
		synctest.Wait()
		sent := r.pub.take()
		if len(sent) == 0 || len(sent[0].GetGaps()) != 1 || sent[0].GetGaps()[0].GetToSeq() != gaps[0].To ||
			sent[0].GetGaps()[0].GetReason() != fleetwirev1.GapReason_GAP_REASON_DISK_BUDGET {
			t.Fatalf("first batch gaps = %v, want %v", sent[0].GetGaps(), gaps)
		}
		// The first readings sent start right after the gap.
		if sent[0].GetFirstSeq() != gaps[0].To+1 {
			t.Fatalf("first seq %d, gap ends at %d", sent[0].GetFirstSeq(), gaps[0].To)
		}
		for _, b := range sent[1:] {
			if len(b.GetGaps()) != 0 {
				var desc []string
				for _, x := range sent {
					desc = append(desc, fmt.Sprintf("[first=%d n=%d gaps=%v]", x.GetFirstSeq(), len(x.GetReadings()), x.GetGaps()))
				}
				t.Fatalf("gap repeated in the same round: %v (pending %v)", desc, r.log.PendingGaps())
			}
		}
		// Not acked: reported again after the ack timeout.
		time.Sleep(31 * time.Second)
		synctest.Wait()
		if again := r.pub.take(); len(again) == 0 || len(again[0].GetGaps()) != 1 {
			t.Fatal("gap not reported again after the ack timeout")
		}
		ack(t, r.up, 300)
		time.Sleep(31 * time.Second)
		synctest.Wait()
		for _, b := range r.pub.take() {
			if len(b.GetGaps()) != 0 {
				t.Fatal("acked gap reported again")
			}
		}
		if len(r.log.PendingGaps()) != 0 || len(r.log.GapHistory()) != 1 {
			t.Fatalf("pending %v history %v", r.log.PendingGaps(), r.log.GapHistory())
		}
	})
}

func TestEncodeBatchMatchesSchema(t *testing.T) {
	recs := []wal.Record{{Seq: 7, Payload: reading(7)}, {Seq: 8, Payload: reading(8)}}
	gaps := []wal.Gap{{From: 1, To: 3, Reason: wal.GapLost}, {From: 5, To: 6, Reason: wal.GapCorruption}}
	raw := EncodeBatch(recs, gaps, 1234, "v1.2.3")
	b, err := uplinkpb.DecodeAndValidate(raw)
	if err != nil {
		t.Fatal(err)
	}
	if b.GetFirstSeq() != 7 || len(b.GetReadings()) != 2 || b.GetReadings()[1].GetValue() != 8 ||
		b.GetSentUnixNano() != 1234 || b.GetAgentVersion() != "v1.2.3" || len(b.GetGaps()) != 2 ||
		b.GetGaps()[0].GetReason() != fleetwirev1.GapReason_GAP_REASON_LOST || b.GetGaps()[1].GetToSeq() != 6 {
		t.Fatalf("decoded = %v", b)
	}
	// Gap-only batches are valid too.
	if _, err := uplinkpb.DecodeAndValidate(EncodeBatch(nil, gaps, 1, "")); err != nil {
		t.Fatal(err)
	}
}
