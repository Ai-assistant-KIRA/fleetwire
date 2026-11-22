package coverage

import (
	"math"
	"slices"
	"testing"

	"pgregory.net/rapid"
)

func TestMerge(t *testing.T) {
	got := Merge([]Range{{10, 12}, {1, 3}, {4, 5}, {7, 8}, {8, 9}, {20, 19}, {11, 11}})
	want := []Range{{1, 5}, {7, 12}}
	if !slices.Equal(got, want) {
		t.Fatalf("Merge = %v, want %v", got, want)
	}
}

func TestMergeAtMaxUint64(t *testing.T) {
	got := Merge([]Range{{math.MaxUint64 - 1, math.MaxUint64}, {math.MaxUint64, math.MaxUint64}, {5, math.MaxUint64 - 2}})
	want := []Range{{5, math.MaxUint64}}
	if !slices.Equal(got, want) {
		t.Fatalf("Merge = %v, want %v", got, want)
	}
}

func TestUncovered(t *testing.T) {
	tests := []struct {
		name          string
		lo, hi, wm    uint64
		covered, want []Range
	}{
		{"all new", 1, 10, 0, nil, []Range{{1, 10}}},
		{"below watermark", 1, 10, 10, nil, nil},
		{"straddles watermark", 5, 10, 7, nil, []Range{{8, 10}}},
		{"hole in the middle", 1, 10, 0, []Range{{3, 4}, {7, 8}}, []Range{{1, 2}, {5, 6}, {9, 10}}},
		{"fully covered", 3, 6, 0, []Range{{1, 10}}, nil},
		{"covered at both ends", 1, 10, 0, []Range{{1, 2}, {9, 12}}, []Range{{3, 8}}},
		{"empty input", 5, 4, 0, nil, nil},
		{"max seq", math.MaxUint64 - 3, math.MaxUint64, math.MaxUint64 - 2, nil, []Range{{math.MaxUint64 - 1, math.MaxUint64}}},
		{"max covered", 1, math.MaxUint64, 0, []Range{{2, math.MaxUint64}}, []Range{{1, 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Uncovered(tt.lo, tt.hi, tt.wm, tt.covered); !slices.Equal(got, tt.want) {
				t.Fatalf("Uncovered(%d, %d, %d, %v) = %v, want %v", tt.lo, tt.hi, tt.wm, tt.covered, got, tt.want)
			}
		})
	}
}

func TestAdvance(t *testing.T) {
	wm, rest := Advance(0, []Range{{5, 6}}, []Range{{1, 3}})
	if wm != 3 || !slices.Equal(rest, []Range{{5, 6}}) {
		t.Fatalf("Advance = %d %v", wm, rest)
	}
	wm, rest = Advance(wm, rest, []Range{{4, 4}, {9, 9}})
	if wm != 6 || !slices.Equal(rest, []Range{{9, 9}}) {
		t.Fatalf("Advance = %d %v", wm, rest)
	}
	wm, rest = Advance(math.MaxUint64-1, nil, []Range{{math.MaxUint64, math.MaxUint64}})
	if wm != math.MaxUint64 || len(rest) != 0 {
		t.Fatalf("Advance at max = %d %v", wm, rest)
	}
}

// server simulates the ingest state for one device using only this package,
// next to a bitmap model that knows nothing about ranges.
type server struct {
	watermark uint64
	covered   []Range
	stored    map[uint64]int // seq -> times stored
	gap       map[uint64]int // seq -> times recorded as a gap
}

func (s *server) apply(batch Range, gaps []Range) {
	var added []Range
	for _, r := range Uncovered(batch.Lo, batch.Hi, s.watermark, s.covered) {
		for q := r.Lo; q <= r.Hi; q++ {
			s.stored[q]++
		}
		added = append(added, r)
	}
	for _, g := range gaps {
		for _, r := range Uncovered(g.Lo, g.Hi, s.watermark, slices.Concat(s.covered, added)) {
			for q := r.Lo; q <= r.Hi; q++ {
				s.gap[q]++
			}
			added = append(added, r)
		}
	}
	s.watermark, s.covered = Advance(s.watermark, s.covered, added)
}

// TestPropertyExactlyOnce: any batches and gaps, applied in any order and
// any number of times, leave every seq of their union covered exactly once,
// and the watermark is the largest N with [1..N] fully covered.
func TestPropertyExactlyOnce(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		const maxSeq = 200
		gen := rapid.Custom(func(t *rapid.T) Range {
			lo := rapid.Uint64Range(1, maxSeq).Draw(t, "lo")
			n := rapid.Uint64Range(0, 30).Draw(t, "len")
			return Range{Lo: lo, Hi: min(lo+n, maxSeq)}
		})
		type msg struct {
			batch Range
			gaps  []Range
		}
		msgs := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) msg {
			return msg{batch: gen.Draw(t, "batch"), gaps: rapid.SliceOfN(gen, 0, 2).Draw(t, "gaps")}
		}), 1, 40).Draw(t, "msgs")
		// Deliver each message one or more times, in a random order.
		var deliveries []msg
		for _, m := range msgs {
			for i := rapid.IntRange(1, 3).Draw(t, "repeats"); i > 0; i-- {
				deliveries = append(deliveries, m)
			}
		}
		order := rapid.Permutation(deliveries).Draw(t, "order")

		s := &server{stored: map[uint64]int{}, gap: map[uint64]int{}}
		union := map[uint64]bool{}
		for _, m := range order {
			s.apply(m.batch, m.gaps)
			for _, r := range append([]Range{m.batch}, m.gaps...) {
				for q := r.Lo; q <= r.Hi; q++ {
					union[q] = true
				}
			}
			// The watermark property, after every step.
			var want uint64
			for union[want+1] {
				want++
			}
			if s.watermark != want {
				t.Fatalf("watermark = %d, want %d", s.watermark, want)
			}
			for _, r := range s.covered {
				if r.Lo <= s.watermark+1 {
					t.Fatalf("covered range %v touches watermark %d", r, s.watermark)
				}
			}
		}
		for q := uint64(1); q <= maxSeq; q++ {
			total := s.stored[q] + s.gap[q]
			switch {
			case union[q] && total != 1:
				t.Fatalf("seq %d covered %d times (stored %d, gap %d)", q, total, s.stored[q], s.gap[q])
			case !union[q] && total != 0:
				t.Fatalf("seq %d was never sent but is covered", q)
			}
		}
	})
}

func FuzzUncovered(f *testing.F) {
	f.Add(uint64(1), uint64(10), uint64(0), uint64(3), uint64(4), uint64(8), uint64(20))
	f.Add(uint64(math.MaxUint64-5), uint64(math.MaxUint64), uint64(math.MaxUint64-7), uint64(math.MaxUint64-2), uint64(math.MaxUint64-1), uint64(0), uint64(0))
	f.Fuzz(func(t *testing.T, lo, hi, wm, c1lo, c1hi, c2lo, c2hi uint64) {
		covered := []Range{{c1lo, c1hi}, {c2lo, c2hi}}
		out := Uncovered(lo, hi, wm, covered)
		for i, r := range out {
			if !r.Valid() || r.Lo < lo || r.Hi > hi || r.Lo <= wm {
				t.Fatalf("range %v out of bounds for [%d,%d] wm %d", r, lo, hi, wm)
			}
			if i > 0 && out[i-1].Hi >= r.Lo-1 {
				t.Fatalf("ranges %v and %v are not sorted and separated", out[i-1], r)
			}
			for _, c := range covered {
				if c.Valid() && r.Lo <= c.Hi && c.Lo <= r.Hi {
					t.Fatalf("output %v overlaps covered %v", r, c)
				}
			}
		}
		// Completeness: everything in [lo,hi] above wm is in out or covered.
		// Checked on a bounded window so the test stays fast.
		if lo > hi {
			return
		}
		start := max(lo, wm+1)
		if wm == math.MaxUint64 || start > hi {
			if len(out) != 0 {
				t.Fatalf("expected no output, got %v", out)
			}
			return
		}
		for q, n := start, 0; n < 512; q, n = q+1, n+1 {
			in := slices.ContainsFunc(out, func(r Range) bool { return r.Contains(q) }) ||
				slices.ContainsFunc(covered, func(r Range) bool { return r.Valid() && r.Contains(q) })
			if !in {
				t.Fatalf("seq %d is neither uncovered nor covered", q)
			}
			if q == hi {
				break
			}
		}
	})
}
