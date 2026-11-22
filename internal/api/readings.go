package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/chunks"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

var metricRE = regexp.MustCompile(`^[a-z0-9_.]{1,64}$`)

// A filter that matches nothing in a long window would make one request
// decrypt every chunk the device ever stored. A page therefore reads at
// most Config.ReadingsScanChunks chunks and spends at most
// readingsScanTime on them; then it ends early, possibly empty, with a
// cursor that continues after the last chunk read.
const (
	defaultReadingsScanChunks = 2000
	readingsScanTime          = 5 * time.Second
	readingsDeadline          = 30 * time.Second
)

// readingsError turns a timeout of the readings deadline into a 503 the
// client can act on; anything else is an internal error.
func (s *Server) readingsError(r *http.Request, err error) *apiError {
	if errors.Is(err, context.DeadlineExceeded) && r.Context().Err() == nil {
		s.log.Warn("readings query timed out", "request_id", requestID(r.Context()), "err", err)
		return &apiError{status: http.StatusServiceUnavailable, code: "timeout",
			detail: "the readings query took too long; retry, or narrow the window"}
	}
	return s.internal(r, err)
}

// listReadings decrypts the device's chunks that overlap [from, to] and
// returns readings in seq order, which is the order they were collected.
// The cursor holds the last seq read and is bound to the query.
func (s *Server) listReadings(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	device, e := pathID(r)
	if e != nil {
		return e
	}
	q := r.URL.Query()
	to := s.now().UTC()
	var problems []string
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			problems = append(problems, "to must be RFC 3339")
		}
		to = t
	}
	from := to.Add(-24 * time.Hour)
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			problems = append(problems, "from must be RFC 3339")
		}
		from = t
	}
	metric := q.Get("metric")
	if metric != "" && !metricRE.MatchString(metric) {
		problems = append(problems, "metric must match ^[a-z0-9_.]{1,64}$")
	}
	if len(problems) == 0 && from.After(to) {
		problems = append(problems, "from must not be after to")
	}
	limit, le := limitParam(r, 1000, 10_000)
	if le != nil {
		problems = append(problems, le.detail)
	}
	if len(problems) > 0 {
		return errBadRequest("invalid readings query", problems...)
	}
	// The cursor carries the window it was issued for: with the default
	// window ("the last 24 hours"), "now" moves between pages.
	// The times are kept as time.Time: UnixNano cannot hold dates outside
	// 1678 to 2262, which clients send as open bounds.
	type cursor struct {
		AfterSeq uint64    `json:"s"`
		From     time.Time `json:"f"`
		To       time.Time `json:"t"`
		Metric   string    `json:"m"`
	}
	scope := fmt.Sprintf("readings:%s:%s", p.tenant, device)
	var cur cursor
	if c := q.Get("cursor"); c != "" {
		if err := s.cur.decode(scope, c, &cur); err != nil {
			return errBadRequest("invalid cursor")
		}
		explicit := (q.Get("from") != "" && !from.Equal(cur.From)) ||
			(q.Get("to") != "" && !to.Equal(cur.To)) || (metric != "" && metric != cur.Metric)
		if explicit {
			return errBadRequest("the cursor belongs to a different query")
		}
		from, to, metric = cur.From.UTC(), cur.To.UTC(), cur.Metric
	}

	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		_, err := tx.GetDevice(ctx, device)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound("device")
	}
	if err != nil {
		return s.internal(r, err)
	}

	// The deadline bounds the database and key calls; the scan budget below
	// normally ends the page long before it.
	ctx, cancel := context.WithTimeout(r.Context(), readingsDeadline)
	defer cancel()
	started := time.Now()
	const batch = 50
	out := page[readingJSON]{Items: make([]readingJSON, 0, min(limit, 1024))}
	after := cur.AfterSeq
	var more bool
	scanned := 0
scan:
	for {
		var list []store.Chunk
		// Short transactions: decryption happens outside, so a slow page
		// never holds a database connection.
		err := s.db.InTenantTx(ctx, p.tenant, func(ctx context.Context, tx *store.Tx) error {
			var err error
			list, err = tx.ChunksInWindow(ctx, device, from, to, after, batch)
			return err
		})
		if err != nil {
			return s.readingsError(r, err)
		}
		for _, c := range list {
			// Out of budget: end the page at the last chunk read. At least
			// one chunk is read per page, so paging always moves forward.
			if scanned > 0 && (scanned == s.cfg.ReadingsScanChunks || time.Since(started) > readingsScanTime) {
				more = true
				break scan
			}
			scanned++
			dek, err := s.keys.Key(ctx, p.tenant, c.KeyID)
			if err != nil {
				return s.readingsError(r, err)
			}
			readings, err := chunks.Open(dek, c)
			if err != nil {
				return s.internal(r, err)
			}
			for _, rd := range readings {
				if rd.GetSeq() <= after {
					continue
				}
				at := time.Unix(0, rd.GetCollectedUnixNano()).UTC()
				if at.Before(from) || at.After(to) || (metric != "" && rd.GetMetric() != metric) {
					continue
				}
				if len(out.Items) == limit {
					// A match beyond a full page, so another page follows.
					after, more = out.Items[limit-1].Seq, true
					break scan
				}
				out.Items = append(out.Items, readingJSON{Seq: rd.GetSeq(), CollectedAt: at, Collector: rd.GetCollector(),
					Metric: rd.GetMetric(), Value: rd.GetValue(), Labels: rd.GetLabels()})
			}
			after = c.SeqHi
		}
		if len(list) < batch {
			break
		}
	}
	if more {
		c, err := s.cur.encode(scope, cursor{AfterSeq: after, From: from, To: to, Metric: metric})
		if err != nil {
			return s.internal(r, err)
		}
		out.NextCursor = c
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) listGaps(w http.ResponseWriter, r *http.Request, p *principal) *apiError {
	device, e := pathID(r)
	if e != nil {
		return e
	}
	limit, e := limitParam(r, 100, 1000)
	if e != nil {
		return e
	}
	type cursor struct {
		AfterFrom uint64 `json:"f"`
	}
	scope := "gaps:" + p.tenant.String() + ":" + device.String()
	var cur cursor
	if c := r.URL.Query().Get("cursor"); c != "" {
		if err := s.cur.decode(scope, c, &cur); err != nil {
			return errBadRequest("invalid cursor")
		}
	}
	var gaps []store.DeviceGap
	err := s.db.InTenantTx(r.Context(), p.tenant, func(ctx context.Context, tx *store.Tx) error {
		if _, err := tx.GetDevice(ctx, device); err != nil {
			return err
		}
		var err error
		gaps, err = tx.ListGaps(ctx, device, cur.AfterFrom, limit)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound("device")
	}
	if err != nil {
		return s.internal(r, err)
	}
	out := page[gapJSON]{Items: make([]gapJSON, 0, len(gaps))}
	for _, g := range gaps {
		out.Items = append(out.Items, gapJSON{FromSeq: g.FromSeq, ToSeq: g.ToSeq, Reason: g.Reason, ReportedAt: g.ReportedAt})
	}
	if len(gaps) == limit {
		c, err := s.cur.encode(scope, cursor{AfterFrom: gaps[len(gaps)-1].FromSeq})
		if err != nil {
			return s.internal(r, err)
		}
		out.NextCursor = c
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}
