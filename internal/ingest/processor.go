// Package ingest stores uplink batches exactly once (R6, ADR-0005).
//
// For each batch, one transaction locks the device row, computes which seqs
// are not covered yet, stores only those (encrypted with the tenant's data
// key) and the uncovered parts of reported gaps, and moves the device's
// watermark. Repeats, reordering and concurrent instances are all safe
// because of the row lock, and an exclusion constraint makes an overlap
// impossible even if this code had a bug.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/chunks"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/coverage"
	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/lru"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/uplinkpb"
)

// Result of processing one batch.
type Result string

// Results. They are metric label values; do not rename them.
const (
	ResultStored         Result = "stored"
	ResultDuplicate      Result = "duplicate"
	ResultDroppedRevoked Result = "dropped_revoked"
	ResultDroppedUnknown Result = "dropped_unknown"
	ResultDLQ            Result = "dlq"
)

// Outcome describes what happened to one batch.
type Outcome struct {
	Result Result
	Device uuid.UUID
	// Ack is true when an end-to-end ack should be published.
	Ack        bool
	AckThrough uint64
	Stored     int
	GapSeqs    uint64
	// DLQ reason and detail, when Result is ResultDLQ.
	Reason string
	Detail string
}

// ErrOverlap means the exclusion constraint rejected a chunk: a bug in the
// coverage logic, never expected. The batch goes to the DLQ; no data is
// lost because the device resends.
var ErrOverlap = errors.New("ingest: chunk overlaps stored data")

// Metrics are the ingest metrics (R13.1).
type Metrics struct {
	Batches        *prometheus.CounterVec
	ReadingsStored prometheus.Counter
	GapSeqs        prometheus.Counter
	E2EDelay       prometheus.Histogram
	TxSeconds      prometheus.Histogram
	Skew           prometheus.Histogram
	NegativeDelay  prometheus.Counter
	DBRetries      prometheus.Counter
	Overlaps       prometheus.Counter
}

// NewMetrics registers the processor metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Batches: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fleetwire_ingest_batches_total",
			Help: "Batches processed, by result."}, []string{"result"}),
		ReadingsStored: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_ingest_readings_stored_total",
			Help: "Readings stored for the first time."}),
		GapSeqs: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_ingest_gap_seqs_total",
			Help: "Seqs recorded as device-reported gaps."}),
		E2EDelay: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "fleetwire_ingest_e2e_delay_seconds",
			Help: "Time from collection on the device (device clock) to commit, per newly stored reading.",
			// Fine around the 5 s objective, so p99 estimates there mean something.
			Buckets: []float64{0.1, 0.25, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10, 30, 60}}),
		TxSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "fleetwire_ingest_tx_seconds",
			Help: "Duration of one batch transaction.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 14)}),
		Skew: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "fleetwire_ingest_clock_skew_seconds",
			Help:    "Absolute difference between the device's send time and the bridge's receive time.",
			Buckets: []float64{0.01, 0.1, 0.5, 1, 5, 30, 300, 3600}}),
		NegativeDelay: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_ingest_negative_delay_total",
			Help: "Readings whose device timestamp was in the future (delay clamped to 0)."}),
		DBRetries: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_ingest_db_retries_total",
			Help: "Batch transactions retried after a database error."}),
		Overlaps: prometheus.NewCounter(prometheus.CounterOpts{Name: "fleetwire_ingest_exclusion_violations_total",
			Help: "Chunks rejected by the overlap constraint. Must stay 0."}),
	}
	reg.MustRegister(m.Batches, m.ReadingsStored, m.GapSeqs, m.E2EDelay, m.TxSeconds, m.Skew, m.NegativeDelay, m.DBRetries, m.Overlaps)
	// Every result exists from the start at 0, so the dead-letter SLI is a
	// number before the first failure instead of no data.
	for _, r := range []Result{ResultStored, ResultDuplicate, ResultDroppedRevoked, ResultDroppedUnknown, ResultDLQ} {
		m.Batches.WithLabelValues(string(r))
	}
	return m
}

// Processor runs the batch transaction.
type Processor struct {
	db      *store.DB
	keys    *tenantkey.Manager
	log     *slog.Logger
	m       *Metrics
	tenants *lru.Cache[uuid.UUID, uuid.UUID]
	audited *lru.Cache[uuid.UUID, time.Time]
	now     func() time.Time
}

// NewProcessor returns a processor.
func NewProcessor(db *store.DB, keys *tenantkey.Manager, log *slog.Logger, m *Metrics) *Processor {
	return &Processor{
		db: db, keys: keys, log: log, m: m,
		// A device's tenant never changes, so this cache needs no expiry.
		tenants: lru.New[uuid.UUID, uuid.UUID](200_000, 0),
		audited: lru.New[uuid.UUID, time.Time](100_000, 0),
		now:     time.Now,
	}
}

// auditEvery rate-limits drop audit entries per device (R6.6).
const auditEvery = 10 * time.Minute

var gapReasons = map[fleetwirev1.GapReason]string{
	fleetwirev1.GapReason_GAP_REASON_UNSPECIFIED: "UNSPECIFIED",
	fleetwirev1.GapReason_GAP_REASON_DISK_BUDGET: "DISK_BUDGET",
	fleetwirev1.GapReason_GAP_REASON_CORRUPTION:  "CORRUPTION",
	fleetwirev1.GapReason_GAP_REASON_LOST:        "LOST",
}

// Process handles one Kafka record. A returned error is a database or key
// error to retry; an invalid batch is an Outcome with ResultDLQ.
func (p *Processor) Process(ctx context.Context, device uuid.UUID, payload []byte, receivedAt time.Time) (Outcome, error) {
	out := Outcome{Device: device}
	batch, err := uplinkpb.DecodeAndValidate(payload)
	if err != nil {
		var ve *uplinkpb.ValidationError
		if !errors.As(err, &ve) {
			return out, err
		}
		out.Result, out.Reason, out.Detail = ResultDLQ, string(ve.Reason), ve.Detail
		return out, nil
	}
	if !receivedAt.IsZero() && batch.GetSentUnixNano() > 0 {
		skew := receivedAt.Sub(time.Unix(0, batch.GetSentUnixNano()))
		p.m.Skew.Observe(skew.Abs().Seconds())
	}

	tenant, known, err := p.tenantOf(ctx, device)
	if err != nil {
		return out, err
	}
	if !known {
		out.Result = ResultDroppedUnknown
		return out, p.auditDrop(ctx, device, "unknown device")
	}
	keyID, dek, err := p.keys.SealingKey(ctx, tenant)
	if errors.Is(err, tenantkey.ErrDestroyed) {
		out.Result = ResultDroppedRevoked
		return out, nil
	}
	if err != nil {
		return out, err
	}

	start := p.now()
	var stored []*fleetwirev1.StoredReading
	sealed := 0
	err = p.db.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		stored, sealed = nil, 0
		dev, err := tx.DeviceForIngest(ctx, device)
		if errors.Is(err, store.ErrNotFound) {
			out.Result = ResultDroppedUnknown
			return nil
		}
		if err != nil {
			return err
		}
		if dev.TenantID != tenant {
			return fmt.Errorf("device %s moved from tenant %s to %s; refusing", device, tenant, dev.TenantID)
		}
		if err := tx.SetTenant(ctx, tenant); err != nil {
			return err
		}
		if dev.DeviceStatus != store.DeviceActive || dev.TenantStatus != store.TenantActive {
			// Revocation is effective from its commit on (R4.6, layer 1).
			out.Result = ResultDroppedRevoked
			return p.auditDropTx(ctx, tx, device, "device "+dev.DeviceStatus+", tenant "+dev.TenantStatus)
		}
		wm := dev.AckedThrough
		covered, err := tx.DeviceRanges(ctx, device)
		if err != nil {
			return err
		}
		var added []coverage.Range
		if n := len(batch.GetReadings()); n > 0 {
			first := batch.GetFirstSeq()
			for _, r := range coverage.Uncovered(first, uplinkpb.LastSeq(batch), wm, covered) {
				rs := make([]*fleetwirev1.StoredReading, 0, r.Hi-r.Lo+1)
				for seq := r.Lo; seq <= r.Hi; seq++ {
					rd := batch.GetReadings()[seq-first]
					rs = append(rs, &fleetwirev1.StoredReading{Seq: seq, CollectedUnixNano: rd.GetCollectedUnixNano(),
						Collector: rd.GetCollector(), Metric: rd.GetMetric(), Value: rd.GetValue(), Labels: rd.GetLabels()})
				}
				c, err := chunks.Seal(dek, tenant, device, keyID, rs)
				if err != nil {
					return err
				}
				if err := tx.InsertChunk(ctx, c); err != nil {
					if store.IsExclusionViolation(err) {
						return fmt.Errorf("%w: device %s range %d-%d, watermark %d, covered %v: %w", ErrOverlap, device, r.Lo, r.Hi, wm, covered, err)
					}
					return err
				}
				sealed++
				stored = append(stored, rs...)
				added = append(added, r)
			}
		}
		var gapSeqs uint64
		for _, g := range batch.GetGaps() {
			for _, r := range coverage.Uncovered(g.GetFromSeq(), g.GetToSeq(), wm, slices.Concat(covered, added)) {
				if err := tx.InsertGap(ctx, store.DeviceGap{DeviceID: device, FromSeq: r.Lo, ToSeq: r.Hi, Reason: gapReasons[g.GetReason()]}); err != nil {
					return err
				}
				added = append(added, r)
				gapSeqs += r.Len()
			}
		}
		newWM, remaining := coverage.Advance(wm, covered, added)
		if len(added) > 0 {
			if err := tx.ReplaceDeviceRanges(ctx, device, remaining); err != nil {
				return err
			}
		}
		if err := tx.UpdateDeviceProgress(ctx, device, newWM, receivedAt); err != nil {
			return err
		}
		out.Ack, out.AckThrough, out.Stored, out.GapSeqs = true, newWM, len(stored), gapSeqs
		out.Result = ResultDuplicate
		if len(added) > 0 {
			out.Result = ResultStored
		}
		return nil
	})
	p.m.TxSeconds.Observe(p.now().Sub(start).Seconds())
	if err != nil {
		if errors.Is(err, ErrOverlap) {
			p.m.Overlaps.Inc()
		}
		return Outcome{Device: device}, err
	}
	if sealed > 0 {
		p.keys.NoteSeals(tenant, keyID, int64(sealed))
	}
	now := p.now()
	for _, r := range stored {
		d := now.Sub(time.Unix(0, r.GetCollectedUnixNano()))
		if d < 0 {
			p.m.NegativeDelay.Inc()
			d = 0
		}
		p.m.E2EDelay.Observe(d.Seconds())
	}
	p.m.ReadingsStored.Add(float64(out.Stored))
	p.m.GapSeqs.Add(float64(out.GapSeqs))
	return out, nil
}

func (p *Processor) tenantOf(ctx context.Context, device uuid.UUID) (uuid.UUID, bool, error) {
	if t, ok := p.tenants.Get(device); ok {
		return t, true, nil
	}
	var ref store.DeviceRef
	err := p.db.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		var err error
		ref, err = tx.DeviceLookup(ctx, device)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, err
	}
	p.tenants.Add(device, ref.TenantID)
	return ref.TenantID, true, nil
}

func (p *Processor) shouldAudit(device uuid.UUID) bool {
	if last, ok := p.audited.Get(device); ok && p.now().Sub(last) < auditEvery {
		return false
	}
	p.audited.Add(device, p.now())
	return true
}

func dropAudit(device uuid.UUID, why string) store.Audit {
	return store.Audit{ActorType: store.ActorSystem, ActorID: "ingest", Action: "ingest.drop",
		TargetType: "device", TargetID: device.String(), Result: store.ResultDenied,
		Details: map[string]any{"reason": why, "rate_limited": auditEvery.String()}}
}

func (p *Processor) auditDropTx(ctx context.Context, tx *store.Tx, device uuid.UUID, why string) error {
	if !p.shouldAudit(device) {
		return nil
	}
	return tx.InsertAudit(ctx, dropAudit(device, why))
}

// auditDrop writes a fleet-wide entry for a device the registry does not know.
func (p *Processor) auditDrop(ctx context.Context, device uuid.UUID, why string) error {
	if !p.shouldAudit(device) {
		return nil
	}
	return p.db.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		return tx.InsertAudit(ctx, dropAudit(device, why))
	})
}
