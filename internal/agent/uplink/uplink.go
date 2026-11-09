// Package uplink sends durable readings from the local log and handles
// end-to-end acks (design 6.3, R3).
//
// It is go-back-N: on every reconnect and on every ack timeout it starts
// again right after the acked seq. Ingest stores out-of-order and repeated
// ranges correctly, so resending a stored range costs only bandwidth, and
// the device logic stays small enough to check by reading it. The in-flight
// window bounds that cost.
package uplink

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/uplinkpb"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

// Publisher sends one batch at QoS 1 and returns once the broker has it
// (PUBACK). A PUBACK only means the broker stored the message; delivery is
// confirmed by the end-to-end ack.
type Publisher interface {
	Publish(ctx context.Context, payload []byte) error
}

// ErrNotConnected tells the uplink to wait for the next connection.
var ErrNotConnected = errors.New("uplink: not connected")

// Config tunes the uplink.
type Config struct {
	BatchSize    int
	BatchWait    time.Duration
	MaxInflight  int
	AckTimeout   time.Duration
	AgentVersion string
	// CheckpointEvery coalesces state writes after acks (default 250 ms).
	CheckpointEvery time.Duration
	// PublishTimeout bounds one publish (default 30 s).
	PublishTimeout time.Duration
}

// Metrics may be shared by many uplinks (load generator); gauges are
// updated by deltas so the totals add up.
type Metrics struct {
	ReadingsSent  prometheus.Counter
	Batches       prometheus.Counter
	PublishErrors prometheus.Counter
	ReadingsAcked prometheus.Counter
	Resends       prometheus.Counter
	SeqJumps      prometheus.Counter
	Unacked       prometheus.Gauge
}

// NewMetrics registers the uplink metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ReadingsSent:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_agent_readings_sent_total", Help: "Readings published, resends included."}),
		Batches:       prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_agent_batches_sent_total", Help: "Batches published."}),
		PublishErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_agent_publish_errors_total", Help: "Publishes that failed or timed out."}),
		ReadingsAcked: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_agent_readings_acked_total", Help: "Seqs newly covered by end-to-end acks (gaps included)."}),
		Resends:       prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_agent_resend_total", Help: "Times the agent went back to the acked seq after an ack timeout."}),
		SeqJumps:      prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_agent_seq_jump_total", Help: "Times the server's ack was ahead of the device's next seq (R3.8)."}),
		Unacked:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "fleetwire_agent_unacked_readings", Help: "Readings written to the log but not yet acked end to end."}),
	}
	reg.MustRegister(m.ReadingsSent, m.Batches, m.PublishErrors, m.ReadingsAcked, m.Resends, m.SeqJumps, m.Unacked)
	return m
}

type span struct {
	first, last uint64
	sentAt      time.Time
}

// Uplink is one device's sender.
type Uplink struct {
	cfg Config
	log *wal.Log
	pub Publisher
	m   *Metrics
	lg  *slog.Logger

	// Written by the MQTT goroutines, read by the loop. conn packs the
	// connection generation and the connected bit into one word
	// (gen<<1 | connected), so the loop never sees a new connection's flag
	// together with the old generation.
	ackMax atomic.Uint64
	conn   atomic.Uint64
	wake   chan struct{}

	// Owned by the loop goroutine.
	seenGen        uint64
	nextToSend     uint64
	inflight       []span
	pendingSince   time.Time
	gapsSent       []wal.Gap
	gapsSentAt     time.Time
	retryAt        time.Time
	checkpointDue  bool
	lastCheckpoint time.Time
	unackedShown   int64
}

// New returns an uplink for log, publishing through pub.
func New(cfg Config, log *wal.Log, pub Publisher, m *Metrics, lg *slog.Logger) *Uplink {
	if cfg.CheckpointEvery <= 0 {
		cfg.CheckpointEvery = 250 * time.Millisecond
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 30 * time.Second
	}
	return &Uplink{cfg: cfg, log: log, pub: pub, m: m, lg: lg, wake: make(chan struct{}, 1)}
}

func (u *Uplink) poke() {
	select {
	case u.wake <- struct{}{}:
	default:
	}
}

// Connected is called after each (re)connection, once the ack subscription
// is in place.
func (u *Uplink) Connected() {
	for {
		s := u.conn.Load()
		if u.conn.CompareAndSwap(s, ((s>>1)+1)<<1|1) {
			break
		}
	}
	u.poke()
}

// Disconnected is called when the connection drops.
func (u *Uplink) Disconnected() {
	for {
		s := u.conn.Load()
		if u.conn.CompareAndSwap(s, s&^1) {
			break
		}
	}
	u.poke()
}

// HandleAck processes an ack message. It never blocks, so it is safe to
// call from the MQTT receive path. Old and repeated acks are ignored.
func (u *Uplink) HandleAck(payload []byte) error {
	var a fleetwirev1.Ack
	if err := proto.Unmarshal(payload, &a); err != nil {
		return err
	}
	for {
		cur := u.ackMax.Load()
		if a.GetAckedThrough() <= cur {
			return nil
		}
		if u.ackMax.CompareAndSwap(cur, a.GetAckedThrough()) {
			u.poke()
			return nil
		}
	}
}

// Run sends until ctx ends, then persists the latest ack.
func (u *Uplink) Run(ctx context.Context) error {
	u.nextToSend = u.log.Acked() + 1
	u.seenGen = u.conn.Load() >> 1
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		notify := u.log.Notify()
		u.step(ctx)
		timer.Reset(u.nextWake())
		select {
		case <-ctx.Done():
			u.updateUnacked()
			if err := u.log.Checkpoint(); err != nil && !errors.Is(err, wal.ErrClosed) {
				return err
			}
			return nil
		case <-u.wake:
		case <-notify:
		case <-tick.C:
		case <-timer.C:
		}
	}
}

func (u *Uplink) nextWake() time.Duration {
	now := time.Now()
	wake := now.Add(time.Second)
	if !u.pendingSince.IsZero() {
		wake = earliest(wake, u.pendingSince.Add(u.cfg.BatchWait))
	}
	if u.checkpointDue {
		wake = earliest(wake, u.lastCheckpoint.Add(u.cfg.CheckpointEvery))
	}
	if d := wake.Sub(now); d > 0 {
		return d
	}
	return time.Millisecond
}

func earliest(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func (u *Uplink) step(ctx context.Context) {
	now := time.Now()
	if gen := u.conn.Load() >> 1; gen != u.seenGen {
		// A new connection: go back to the acked seq and report gaps again.
		u.seenGen = gen
		u.restart()
	}
	u.applyAck()
	if len(u.inflight) > 0 && now.Sub(u.inflight[0].sentAt) > u.cfg.AckTimeout {
		u.lg.Debug("ack timeout; resending from the acked seq", "acked", u.log.Acked(), "oldest_sent", u.inflight[0].first)
		u.m.Resends.Inc()
		u.restart()
	}
	if u.checkpointDue && now.Sub(u.lastCheckpoint) >= u.cfg.CheckpointEvery {
		u.checkpoint()
	}
	u.send(ctx)
	u.updateUnacked()
}

func (u *Uplink) restart() {
	u.nextToSend = u.log.Acked() + 1
	u.inflight = nil
	u.gapsSent = nil
}

func (u *Uplink) applyAck() {
	n := u.ackMax.Load()
	before := u.log.Acked()
	if n <= before {
		return
	}
	res := u.log.Ack(n)
	if !res.Advanced {
		return
	}
	u.m.ReadingsAcked.Add(float64(n - before))
	i := 0
	for i < len(u.inflight) && u.inflight[i].last <= n {
		i++
	}
	u.inflight = u.inflight[i:]
	if u.nextToSend <= n {
		u.nextToSend = n + 1
	}
	u.checkpointDue = true
	if res.Jumped {
		// The server knows seqs this device does not remember handing out.
		// Persist the jump at once so a crash cannot undo it.
		u.lg.Error("server ack is ahead of the device's next seq; moving the seq forward so no seq is reused",
			"acked_through", n, "old_next_seq", res.OldNext)
		u.m.SeqJumps.Inc()
		u.checkpoint()
	}
}

func (u *Uplink) checkpoint() {
	if err := u.log.Checkpoint(); err != nil {
		u.lg.Error("saving agent state failed", "err", err)
		return
	}
	u.checkpointDue = false
	u.lastCheckpoint = time.Now()
}

// gapsCovered reports whether every pending gap was already sent.
func gapsCovered(pending, sent []wal.Gap) bool {
	for _, g := range pending {
		if !slices.Contains(sent, g) {
			return false
		}
	}
	return true
}

func (u *Uplink) send(ctx context.Context) {
	for ctx.Err() == nil {
		conn := u.conn.Load()
		if conn&1 == 0 || conn>>1 != u.seenGen {
			// Disconnected, or a new connection that the next step must
			// restart from the acked seq first.
			return
		}
		now := time.Now()
		if now.Before(u.retryAt) {
			return
		}
		acked := u.log.Acked()
		if u.nextToSend <= acked {
			u.nextToSend = acked + 1
		}
		durable := u.log.Durable()
		var avail uint64
		if u.nextToSend <= durable {
			avail = durable - u.nextToSend + 1
		}
		room := uint64(0)
		if inFlight := u.nextToSend - 1 - acked; inFlight < uint64(u.cfg.MaxInflight) {
			room = uint64(u.cfg.MaxInflight) - inFlight
		}
		pending := u.log.PendingGaps()
		// Gaps are resent when new ones appear, and again after an ack
		// timeout if the server has not covered them yet.
		sendGaps := len(pending) > 0 && (!gapsCovered(pending, u.gapsSent) || now.Sub(u.gapsSentAt) > u.cfg.AckTimeout)
		if avail == 0 {
			u.pendingSince = time.Time{}
		} else if u.pendingSince.IsZero() {
			u.pendingSince = now
		}
		batch := uint64(u.cfg.BatchSize)
		n := min(batch, room, avail)
		full := avail >= batch && room >= batch
		waited := n > 0 && now.Sub(u.pendingSince) >= u.cfg.BatchWait
		if !sendGaps && !full && !waited {
			return
		}
		var recs []wal.Record
		if n > 0 {
			var err error
			if recs, err = u.log.Read(u.nextToSend, int(n)); err != nil {
				u.lg.Error("reading the local log failed", "from", u.nextToSend, "err", err)
				u.retryAt = now.Add(time.Second)
				return
			}
		}
		if len(recs) == 0 && !sendGaps {
			return
		}
		gaps := pending
		if !sendGaps {
			gaps = nil
		} else if len(gaps) > uplinkpb.MaxGaps {
			gaps = gaps[:uplinkpb.MaxGaps]
		}
		payload := EncodeBatch(recs, gaps, now.UnixNano(), u.cfg.AgentVersion)
		pctx, cancel := context.WithTimeout(ctx, u.cfg.PublishTimeout)
		err := u.pub.Publish(pctx, payload)
		cancel()
		if err != nil {
			u.m.PublishErrors.Inc()
			if errors.Is(err, ErrNotConnected) {
				// Only if no new connection came up in the meantime.
				u.conn.CompareAndSwap(conn, conn&^1)
			} else if ctx.Err() == nil {
				u.lg.Debug("publish failed; retrying", "err", err)
			}
			u.retryAt = now.Add(time.Second)
			return
		}
		u.m.Batches.Inc()
		if sendGaps {
			u.gapsSent, u.gapsSentAt = slices.Clone(gaps), now
		}
		if len(recs) > 0 {
			first, last := recs[0].Seq, recs[len(recs)-1].Seq
			u.inflight = append(u.inflight, span{first: first, last: last, sentAt: now})
			u.nextToSend = last + 1
			u.m.ReadingsSent.Add(float64(len(recs)))
		}
		if u.nextToSend > durable {
			u.pendingSince = time.Time{}
		}
	}
}

func (u *Uplink) updateUnacked() {
	next, acked := u.log.Next(), u.log.Acked()
	var n int64
	if next > acked+1 {
		n = int64(next - acked - 1)
	}
	u.m.Unacked.Add(float64(n - u.unackedShown))
	u.unackedShown = n
}

// Release removes this uplink's share from shared gauges (load generator
// devices that stop).
func (u *Uplink) Release() {
	u.m.Unacked.Sub(float64(u.unackedShown))
	u.unackedShown = 0
}

var gapReasons = map[wal.GapReason]fleetwirev1.GapReason{
	wal.GapDiskBudget: fleetwirev1.GapReason_GAP_REASON_DISK_BUDGET,
	wal.GapCorruption: fleetwirev1.GapReason_GAP_REASON_CORRUPTION,
	wal.GapLost:       fleetwirev1.GapReason_GAP_REASON_LOST,
}

// EncodeBatch builds a fleetwire.v1.Batch directly from log records. The
// records already hold encoded Reading messages, and a repeated message
// field is just their concatenation with tags, so nothing is decoded and
// encoded again. The output is checked against the generated code in tests.
func EncodeBatch(recs []wal.Record, gaps []wal.Gap, sentUnixNano int64, agentVersion string) []byte {
	size := 32 + len(agentVersion) + 24*len(gaps)
	for _, r := range recs {
		size += len(r.Payload) + 4
	}
	b := make([]byte, 0, size)
	b = protowire.AppendTag(b, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, uplinkpb.SchemaVersion)
	if len(recs) > 0 {
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, recs[0].Seq)
	}
	for _, r := range recs {
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendBytes(b, r.Payload)
	}
	for _, g := range gaps {
		var gb []byte
		gb = protowire.AppendTag(gb, 1, protowire.VarintType)
		gb = protowire.AppendVarint(gb, g.From)
		gb = protowire.AppendTag(gb, 2, protowire.VarintType)
		gb = protowire.AppendVarint(gb, g.To)
		if r := gapReasons[g.Reason]; r != 0 {
			gb = protowire.AppendTag(gb, 3, protowire.VarintType)
			gb = protowire.AppendVarint(gb, uint64(r))
		}
		b = protowire.AppendTag(b, 4, protowire.BytesType)
		b = protowire.AppendBytes(b, gb)
	}
	b = protowire.AppendTag(b, 5, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(sentUnixNano))
	if agentVersion != "" {
		b = protowire.AppendTag(b, 6, protowire.BytesType)
		b = protowire.AppendString(b, agentVersion)
	}
	return b
}
