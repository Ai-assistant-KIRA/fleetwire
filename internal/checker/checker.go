// Package checker proves the delivery guarantee by reading the stored data
// back.
//
// For every device it decrypts every chunk and marks each stored seq in a
// bitmap, so a seq stored twice is seen even if the database constraint
// that forbids it were missing. Missing seqs are those from 1 to the
// device's last seq that are neither stored nor inside a gap recorded on
// the server. Every gap the device says it reported must also be covered.
// A stored seq above the device's last seq fails the check too: the device
// has lost the state that numbered it, and will reuse that seq.
package checker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/chunks"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/coverage"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

// Expectation is what a device says about itself.
type Expectation struct {
	DeviceID uuid.UUID
	LastSeq  uint64
	Gaps     []wal.Gap
}

// maxListed bounds the problems listed per device.
const maxListed = 20

// DeviceReport is the result for one device.
type DeviceReport struct {
	DeviceID    string           `json:"device_id"`
	LastSeq     uint64           `json:"last_seq"`
	Stored      uint64           `json:"stored"`
	Missing     uint64           `json:"missing"`
	Duplicates  uint64           `json:"duplicates"`
	GapSeqs     uint64           `json:"gap_seqs"`
	Beyond      uint64           `json:"beyond_last_seq"`
	LostReports uint64           `json:"unrecorded_gap_seqs"`
	MissingList []coverage.Range `json:"missing_ranges,omitempty"`
	DupList     []uint64         `json:"duplicate_seqs,omitempty"`
	Error       string           `json:"error,omitempty"`
}

// OK reports whether the device kept the guarantee.
func (d DeviceReport) OK() bool {
	return d.Missing == 0 && d.Duplicates == 0 && d.Beyond == 0 && d.LostReports == 0 && d.Error == ""
}

// Report is the result for a run.
type Report struct {
	Devices     int            `json:"devices"`
	Readings    uint64         `json:"readings"`
	Stored      uint64         `json:"stored"`
	Missing     uint64         `json:"missing"`
	Duplicates  uint64         `json:"duplicates"`
	GapSeqs     uint64         `json:"gap_seqs"`
	Beyond      uint64         `json:"beyond_last_seq"`
	LostReports uint64         `json:"unrecorded_gap_seqs"`
	Errors      int            `json:"errors"`
	Failed      []DeviceReport `json:"failed_devices,omitempty"`
}

// OK reports whether every device kept the guarantee.
func (r Report) OK() bool {
	return r.Missing == 0 && r.Duplicates == 0 && r.Beyond == 0 && r.LostReports == 0 && r.Errors == 0
}

// Checker reads devices back.
type Checker struct {
	DB   *store.DB
	Keys *tenantkey.Manager
	// Workers checks this many devices in parallel (default 8).
	Workers int
}

// Run checks every expectation.
func (c *Checker) Run(ctx context.Context, exps []Expectation) Report {
	workers := c.Workers
	if workers <= 0 {
		workers = 8
	}
	reports := make([]DeviceReport, len(exps))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range jobs {
				reports[i] = c.Device(ctx, exps[i])
			}
		})
	}
	for i := range exps {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	var r Report
	r.Devices = len(exps)
	for _, d := range reports {
		r.Readings += d.LastSeq
		r.Stored += d.Stored
		r.Missing += d.Missing
		r.Duplicates += d.Duplicates
		r.GapSeqs += d.GapSeqs
		r.Beyond += d.Beyond
		r.LostReports += d.LostReports
		if d.Error != "" {
			r.Errors++
		}
		if !d.OK() && len(r.Failed) < 100 {
			r.Failed = append(r.Failed, d)
		}
	}
	return r
}

// bitset over seqs 1..n.
type bitset []uint64

func newBitset(n uint64) bitset { return make(bitset, n/64+1) }

func (b bitset) has(i uint64) bool { return i/64 < uint64(len(b)) && b[i/64]&(1<<(i%64)) != 0 }

// set marks i and reports whether it was already set.
func (b bitset) set(i uint64) bool {
	was := b.has(i)
	b[i/64] |= 1 << (i % 64)
	return was
}

// Device checks one device.
func (c *Checker) Device(ctx context.Context, e Expectation) DeviceReport {
	rep := DeviceReport{DeviceID: e.DeviceID.String(), LastSeq: e.LastSeq}
	if err := c.device(ctx, e, &rep); err != nil {
		rep.Error = err.Error()
	}
	return rep
}

func (c *Checker) device(ctx context.Context, e Expectation, rep *DeviceReport) error {
	var ref store.DeviceRef
	err := c.DB.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		var err error
		ref, err = tx.DeviceLookup(ctx, e.DeviceID)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		// Nothing stored at all: every seq is missing.
		if e.LastSeq > 0 {
			rep.Missing = e.LastSeq
			rep.MissingList = []coverage.Range{{Lo: 1, Hi: e.LastSeq}}
		}
		return fmt.Errorf("device not registered")
	}
	if err != nil {
		return err
	}
	stored, gaps := newBitset(e.LastSeq), newBitset(e.LastSeq)
	dup := func(seq uint64) {
		rep.Duplicates++
		if len(rep.DupList) < maxListed {
			rep.DupList = append(rep.DupList, seq)
		}
	}
	var gapRows []store.DeviceGap
	err = c.DB.InTenantTx(ctx, ref.TenantID, func(ctx context.Context, tx *store.Tx) error {
		var after uint64
		for {
			list, err := tx.ChunksBySeq(ctx, e.DeviceID, after, 200)
			if err != nil {
				return err
			}
			for _, ch := range list {
				dek, err := c.Keys.Key(ctx, ref.TenantID, ch.KeyID)
				if err != nil {
					return err
				}
				readings, err := chunks.Open(dek, ch)
				if err != nil {
					return err
				}
				for _, r := range readings {
					seq := r.GetSeq()
					rep.Stored++
					if seq > e.LastSeq {
						rep.Beyond++
						continue
					}
					if stored.set(seq) {
						dup(seq)
					}
				}
				after = ch.SeqLo
			}
			if len(list) < 200 {
				break
			}
		}
		var err error
		gapRows, err = tx.ListGaps(ctx, e.DeviceID, 0, 1_000_000)
		return err
	})
	if err != nil {
		return err
	}
	for _, g := range gapRows {
		for s := g.FromSeq; s <= g.ToSeq && s <= e.LastSeq; s++ {
			rep.GapSeqs++
			if stored.has(s) || gaps.set(s) {
				// Covered twice: stored and declared a gap, or two gaps.
				dup(s)
			}
		}
	}
	var run *coverage.Range
	for s := uint64(1); s <= e.LastSeq; s++ {
		if stored.has(s) || gaps.has(s) {
			run = nil
			continue
		}
		rep.Missing++
		if run != nil && run.Hi == s-1 {
			run.Hi = s
			continue
		}
		if len(rep.MissingList) < maxListed {
			rep.MissingList = append(rep.MissingList, coverage.Range{Lo: s, Hi: s})
			run = &rep.MissingList[len(rep.MissingList)-1]
		} else {
			run = nil
		}
	}
	// Every gap the device reported must have reached the server, either as
	// a recorded gap or because those seqs were stored before the device
	// gave up on them.
	for _, g := range e.Gaps {
		for s := g.From; s <= g.To && s <= e.LastSeq; s++ {
			if !stored.has(s) && !gaps.has(s) {
				rep.LostReports++
			}
		}
	}
	return nil
}
