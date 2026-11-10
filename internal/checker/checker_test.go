package checker_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/checker"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/chunks"
	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/ingest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

func batch(t *testing.T, first, last uint64, gaps ...[2]uint64) []byte {
	t.Helper()
	b := &fleetwirev1.Batch{SchemaVersion: 1}
	if first > 0 {
		b.FirstSeq = first
		for s := first; s <= last; s++ {
			b.Readings = append(b.Readings, &fleetwirev1.Reading{CollectedUnixNano: time.Now().UnixNano(), Collector: "c", Metric: "m", Value: float64(s)})
		}
	}
	for _, g := range gaps {
		b.Gaps = append(b.Gaps, &fleetwirev1.Gap{FromSeq: g[0], ToSeq: g[1], Reason: fleetwirev1.GapReason_GAP_REASON_LOST})
	}
	raw, err := proto.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCheckerPassesAndCatchesProblems(t *testing.T) {
	h := apitest.New(t)
	tenant, tok := h.CreateTenant("checked")
	devs := []uuid.UUID{h.Enroll(h.EnrollmentTokens(tok, 1)[0]).ID, h.Enroll(h.EnrollmentTokens(tok, 1)[0]).ID}
	p := ingest.NewProcessor(h.DB, h.Keys, slog.New(slog.NewTextHandler(io.Discard, nil)), ingest.NewMetrics(prometheus.NewRegistry()))
	for _, d := range devs {
		for _, raw := range [][]byte{batch(t, 1, 50), batch(t, 61, 100, [2]uint64{51, 60}), batch(t, 1, 50)} {
			if _, err := p.Process(t.Context(), d, raw, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	c := &checker.Checker{DB: h.DB, Keys: h.Keys}
	good := c.Run(t.Context(), []checker.Expectation{
		{DeviceID: devs[0], LastSeq: 100, Gaps: []wal.Gap{{From: 51, To: 60, Reason: wal.GapLost}}},
		{DeviceID: devs[1], LastSeq: 100},
	})
	if !good.OK() || good.Readings != 200 || good.Stored != 180 || good.GapSeqs != 20 {
		t.Fatalf("clean data reported as %+v", good)
	}

	// A device that claims more seqs than were stored has missing ones,
	// and a claimed gap that never reached the server is caught.
	bad := c.Run(t.Context(), []checker.Expectation{{DeviceID: devs[0], LastSeq: 110, Gaps: []wal.Gap{{From: 105, To: 107}}}})
	if bad.OK() || bad.Missing != 10 || bad.LostReports != 3 || bad.Failed[0].MissingList[0] != (struct{ Lo, Hi uint64 }{101, 110}) {
		t.Fatalf("missing seqs not caught: %+v", bad)
	}

	// Plant a duplicate directly in the database. The exclusion constraint
	// forbids it, so this test removes the constraint for one insert: the
	// checker must catch duplicates on its own, not trust the constraint.
	owner, err := pgx.Connect(t.Context(), h.Env.OwnerURL)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanups run last-in first-out: the constraint is restored before
	// the connection closes.
	t.Cleanup(func() { _ = owner.Close(context.Background()) })
	if _, err := owner.Exec(t.Context(), `ALTER TABLE fleetwire.chunks DROP CONSTRAINT chunks_no_overlap`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = owner.Exec(ctx, `DELETE FROM fleetwire.chunks WHERE device_id = $1 AND seq_lo = 40`, devs[1])
		if _, err := owner.Exec(ctx, `ALTER TABLE fleetwire.chunks ADD CONSTRAINT chunks_no_overlap
			EXCLUDE USING gist (device_id WITH =, int8range(seq_lo, seq_hi, '[]') WITH &&)`); err != nil {
			t.Errorf("restore constraint: %v", err)
		}
	})
	keyID, dek, err := h.Keys.SealingKey(t.Context(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	var rs []*fleetwirev1.StoredReading
	for s := uint64(40); s <= 45; s++ {
		rs = append(rs, &fleetwirev1.StoredReading{Seq: s, CollectedUnixNano: time.Now().UnixNano(), Collector: "c", Metric: "m", Value: float64(s)})
	}
	ch, err := chunks.Seal(dek, tenant, devs[1], keyID, rs)
	if err != nil {
		t.Fatal(err)
	}
	err = h.DB.IngestBatch(t.Context(), devs[1], tenant, func(store.IngestState) (store.IngestWrite, error) {
		return store.IngestWrite{Chunks: []store.Chunk{ch}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	dupRep := c.Run(t.Context(), []checker.Expectation{{DeviceID: devs[1], LastSeq: 100}})
	if dupRep.OK() || dupRep.Duplicates != 6 || dupRep.Failed[0].DupList[0] != 40 {
		t.Fatalf("planted duplicates not caught: %+v", dupRep)
	}
}
