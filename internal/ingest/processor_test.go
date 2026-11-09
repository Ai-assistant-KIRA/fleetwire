package ingest_test

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/chunks"
	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/ingest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

type msg struct {
	first, last uint64
	gaps        [][2]uint64
}

func encode(t *testing.T, m msg) []byte {
	t.Helper()
	b := &fleetwirev1.Batch{SchemaVersion: 1, SentUnixNano: time.Now().UnixNano()}
	if m.last >= m.first && m.first > 0 {
		b.FirstSeq = m.first
		for s := m.first; s <= m.last; s++ {
			b.Readings = append(b.Readings, &fleetwirev1.Reading{CollectedUnixNano: time.Now().UnixNano(),
				Collector: "test", Metric: "test.value", Value: float64(s)})
		}
	}
	for _, g := range m.gaps {
		b.Gaps = append(b.Gaps, &fleetwirev1.Gap{FromSeq: g[0], ToSeq: g[1], Reason: fleetwirev1.GapReason_GAP_REASON_DISK_BUDGET})
	}
	raw, err := proto.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func setup(t *testing.T) (*apitest.Harness, *ingest.Processor, uuid.UUID, uuid.UUID, string) {
	t.Helper()
	h := apitest.New(t)
	tenant, tok := h.CreateTenant("ingest-" + uuid.NewString()[:6])
	dev := h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	p := ingest.NewProcessor(h.DB, h.Keys, slog.New(slog.NewTextHandler(io.Discard, nil)), ingest.NewMetrics(prometheus.NewRegistry()))
	return h, p, tenant, dev.ID, tok
}

// readBack decrypts every chunk of the device, the way the checker does.
func readBack(t *testing.T, h *apitest.Harness, tenant, device uuid.UUID) (stored map[uint64]int, gaps map[uint64]int, acked uint64) {
	t.Helper()
	stored, gaps = map[uint64]int{}, map[uint64]int{}
	err := h.DB.InTenantTx(t.Context(), tenant, func(ctx context.Context, tx *store.Tx) error {
		d, err := tx.GetDevice(ctx, device)
		if err != nil {
			return err
		}
		acked = d.AckedThrough
		var after uint64
		for {
			list, err := tx.ChunksBySeq(ctx, device, after, 100)
			if err != nil {
				return err
			}
			for _, c := range list {
				dek, err := h.Keys.Key(ctx, tenant, c.KeyID)
				if err != nil {
					return err
				}
				rs, err := chunks.Open(dek, c)
				if err != nil {
					return err
				}
				for _, r := range rs {
					stored[r.GetSeq()]++
					if r.GetValue() != float64(r.GetSeq()) {
						t.Errorf("seq %d holds value %v", r.GetSeq(), r.GetValue())
					}
				}
				after = c.SeqLo
			}
			if len(list) < 100 {
				break
			}
		}
		gl, err := tx.ListGaps(ctx, device, 0, 10000)
		for _, g := range gl {
			for s := g.FromSeq; s <= g.ToSeq; s++ {
				gaps[s]++
			}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return stored, gaps, acked
}

// TestExactlyOnceUnderRepeatsReorderAndConcurrency sends random batches and
// gap reports, each several times, in random order, from four concurrent
// workers, and checks P1 and P2 against the decrypted database content.
func TestExactlyOnceUnderRepeatsReorderAndConcurrency(t *testing.T) {
	h, p, tenant, device, _ := setup(t)
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d (set FLEETWIRE_SEED to replay)", seed)
	if v := os.Getenv("FLEETWIRE_SEED"); v != "" {
		var err error
		if seed, err = strconv.ParseUint(v, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	const maxSeq = 600
	var msgs []msg
	for lo := uint64(1); lo <= maxSeq; {
		n := uint64(1 + rng.IntN(40))
		hi := min(lo+n-1, maxSeq)
		m := msg{first: lo, last: hi}
		if rng.IntN(6) == 0 {
			// Report this range as a gap instead of sending it.
			m = msg{gaps: [][2]uint64{{lo, hi}}}
		}
		msgs = append(msgs, m)
		lo = hi + 1
	}
	var deliveries []msg
	for _, m := range msgs {
		for k := 1 + rng.IntN(3); k > 0; k-- {
			deliveries = append(deliveries, m)
		}
	}
	rng.Shuffle(len(deliveries), func(i, j int) { deliveries[i], deliveries[j] = deliveries[j], deliveries[i] })

	work := make(chan msg)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for m := range work {
				out, err := p.Process(t.Context(), device, encode(t, m), time.Now())
				if err != nil {
					// Keep draining so the sender never blocks.
					t.Error(err)
					continue
				}
				if out.Result != ingest.ResultStored && out.Result != ingest.ResultDuplicate {
					t.Errorf("result %s", out.Result)
				}
			}
		})
	}
	for _, d := range deliveries {
		work <- d
	}
	close(work)
	wg.Wait()

	stored, gaps, acked := readBack(t, h, tenant, device)
	for s := uint64(1); s <= maxSeq; s++ {
		if total := stored[s] + gaps[s]; total != 1 {
			t.Fatalf("seq %d covered %d times (stored %d, gap %d)", s, total, stored[s], gaps[s])
		}
	}
	if acked != maxSeq {
		t.Fatalf("watermark %d, want %d", acked, maxSeq)
	}
}

func TestOutOfOrderWatermark(t *testing.T) {
	h, p, tenant, device, _ := setup(t)
	steps := []struct {
		m    msg
		want uint64
	}{
		{msg{first: 11, last: 20}, 0},
		{msg{first: 31, last: 40}, 0},
		{msg{gaps: [][2]uint64{{21, 30}}}, 0},
		{msg{first: 1, last: 10}, 40},
		{msg{first: 1, last: 40}, 40}, // full repeat stores nothing
	}
	for i, s := range steps {
		out, err := p.Process(t.Context(), device, encode(t, s.m), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if !out.Ack || out.AckThrough != s.want {
			t.Fatalf("step %d: ack %v through %d, want %d", i, out.Ack, out.AckThrough, s.want)
		}
		if i == 4 && (out.Result != ingest.ResultDuplicate || out.Stored != 0) {
			t.Fatalf("repeat stored %d (%s)", out.Stored, out.Result)
		}
	}
	stored, gaps, _ := readBack(t, h, tenant, device)
	if len(stored) != 30 || len(gaps) != 10 {
		t.Fatalf("stored %d, gaps %d", len(stored), len(gaps))
	}
}

func TestRevokedAndUnknownDevicesAreDropped(t *testing.T) {
	h, p, tenant, device, tok := setup(t)
	resp, body := h.Do("POST", "/v1/devices/"+device.String()+"/revoke", tok, nil)
	h.MustJSON(resp, body, 200, nil)
	out, err := p.Process(t.Context(), device, encode(t, msg{first: 1, last: 5}), time.Now())
	if err != nil || out.Result != ingest.ResultDroppedRevoked || out.Ack {
		t.Fatalf("revoked: %+v, %v", out, err)
	}
	stored, _, _ := readBack(t, h, tenant, device)
	if len(stored) != 0 {
		t.Fatalf("revoked device stored %d readings", len(stored))
	}
	out, err = p.Process(t.Context(), uuid.New(), encode(t, msg{first: 1, last: 5}), time.Now())
	if err != nil || out.Result != ingest.ResultDroppedUnknown {
		t.Fatalf("unknown: %+v, %v", out, err)
	}
	// The drop is in the tenant's audit log.
	resp, body = h.Do("GET", "/v1/audit", tok, nil)
	h.MustJSON(resp, body, 200, nil)
	if !contains(body, "ingest.drop") {
		t.Fatalf("no ingest.drop audit entry: %s", body)
	}
}

func TestInvalidBatchGoesToDLQ(t *testing.T) {
	_, p, _, device, _ := setup(t)
	for raw, reason := range map[string]string{
		string([]byte{0xff, 0x01}): "decode",
		string(mustMarshal(t, &fleetwirev1.Batch{SchemaVersion: 9, FirstSeq: 1, Readings: []*fleetwirev1.Reading{{}}})): "schema_version",
	} {
		out, err := p.Process(t.Context(), device, []byte(raw), time.Now())
		if err != nil || out.Result != ingest.ResultDLQ || out.Reason != reason {
			t.Fatalf("got %+v, %v; want DLQ %s", out, err, reason)
		}
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func contains(b []byte, s string) bool { return strings.Contains(string(b), s) }
