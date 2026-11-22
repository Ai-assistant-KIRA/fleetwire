package api_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
)

// readAllPages follows next_cursor from query and returns how many pages
// it took and every seq returned.
func readAllPages(t *testing.T, h *apitest.Harness, tok string, device uuid.UUID, query string) (int, []uint64) {
	t.Helper()
	base := "/v1/devices/" + device.String() + "/readings?" + query
	path := base
	var seqs []uint64
	pages := 0
	for path != "" {
		if pages++; pages > 100 {
			t.Fatal("more than 100 pages")
		}
		var pg struct {
			Items []struct {
				Seq uint64 `json:"seq"`
			} `json:"items"`
			NextCursor string `json:"next_cursor"`
		}
		resp, body := h.Do("GET", path, tok, nil)
		h.MustJSON(resp, body, http.StatusOK, &pg)
		for _, it := range pg.Items {
			seqs = append(seqs, it.Seq)
		}
		path = ""
		if pg.NextCursor != "" {
			path = base + "&cursor=" + pg.NextCursor
		}
	}
	return pages, seqs
}

// A metric that matches nothing used to make one request decrypt every
// chunk the device ever stored. A page now stops at the scan budget and
// hands out a cursor, even when it found nothing.
func TestReadingsPageStopsAtTheScanBudget(t *testing.T) {
	h := apitest.New(t, func(c *api.Config) { c.ReadingsScanChunks = 2 })
	tenant, tok := h.CreateTenant("scan-budget")
	dev := h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	at := func(seq uint64) time.Time { return base.Add(time.Duration(seq) * time.Second) }
	for i := 0; i < 5; i++ {
		storeReadings(t, h, tenant, dev.ID, uint64(i*10+1), 10, at, "cpu.utilization")
	}
	// Five chunks at two per page.
	if pages, seqs := readAllPages(t, h, tok, dev.ID, "metric=memory.used_bytes"); pages != 3 || len(seqs) != 0 {
		t.Errorf("unmatched metric: %d pages, %d readings; want 3 empty pages", pages, len(seqs))
	}
	pages, seqs := readAllPages(t, h, tok, dev.ID, "limit=1000")
	if pages != 3 || len(seqs) != 50 {
		t.Fatalf("all readings: %d pages, %d readings; want 3 pages and 50 readings", pages, len(seqs))
	}
	for i, s := range seqs {
		if s != uint64(i)+1 {
			t.Fatalf("seqs out of order or repeated: %v", seqs)
		}
	}
}

// A page that is exactly full used to carry a cursor to an empty page.
func TestReadingsLastFullPageHasNoCursor(t *testing.T) {
	h := apitest.New(t)
	tenant, tok := h.CreateTenant("full-page")
	dev := h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	at := func(uint64) time.Time { return time.Now().Add(-time.Minute) }
	storeReadings(t, h, tenant, dev.ID, 1, 10, at, "cpu.utilization")
	storeReadings(t, h, tenant, dev.ID, 11, 10, at, "cpu.utilization")
	for limit, want := range map[string]int{"limit=10": 2, "limit=20": 1, "limit=5": 4} {
		if pages, seqs := readAllPages(t, h, tok, dev.ID, limit); pages != want || len(seqs) != 20 {
			t.Errorf("%s: %d pages, %d readings; want %d pages and 20 readings", limit, pages, len(seqs), want)
		}
	}
}
