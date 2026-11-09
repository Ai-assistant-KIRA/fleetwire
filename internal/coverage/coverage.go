// Package coverage holds the interval logic behind exactly-once storage.
//
// For each device the backend keeps a watermark (every seq <= watermark is
// stored or a reported gap) and a short list of covered ranges above it.
// A batch is stored only where it is not covered yet, then the watermark is
// moved over any range that now touches it. The functions here are pure, so
// ingest and the checker share one implementation that is tested on its own.
//
// All ranges are inclusive and every function is safe at math.MaxUint64.
package coverage

import (
	"math"
	"slices"
)

// Range is an inclusive seq range with Lo <= Hi.
type Range struct {
	Lo uint64
	Hi uint64
}

// Valid reports whether Lo <= Hi.
func (r Range) Valid() bool { return r.Lo <= r.Hi }

// Contains reports whether seq is inside r.
func (r Range) Contains(seq uint64) bool { return r.Lo <= seq && seq <= r.Hi }

// Len is the number of seqs in r. It saturates at math.MaxUint64.
func (r Range) Len() uint64 {
	if !r.Valid() {
		return 0
	}
	if r.Lo == 0 && r.Hi == math.MaxUint64 {
		return math.MaxUint64
	}
	return r.Hi - r.Lo + 1
}

// touches reports whether b starts inside a or directly after it. a.Lo <= b.Lo.
func touches(a, b Range) bool {
	return a.Hi == math.MaxUint64 || b.Lo <= a.Hi+1
}

// Merge returns the union of rs as sorted ranges that neither overlap nor
// touch. Invalid ranges are ignored. The input is not modified.
func Merge(rs []Range) []Range {
	out := make([]Range, 0, len(rs))
	for _, r := range rs {
		if r.Valid() {
			out = append(out, r)
		}
	}
	if len(out) < 2 {
		return out
	}
	slices.SortFunc(out, func(a, b Range) int {
		switch {
		case a.Lo < b.Lo:
			return -1
		case a.Lo > b.Lo:
			return 1
		default:
			return 0
		}
	})
	merged := out[:1]
	for _, r := range out[1:] {
		last := &merged[len(merged)-1]
		if touches(*last, r) {
			last.Hi = max(last.Hi, r.Hi)
			continue
		}
		merged = append(merged, r)
	}
	return merged
}

// Uncovered returns the parts of [lo, hi] that are above watermark and not
// inside any covered range, in ascending order.
func Uncovered(lo, hi, watermark uint64, covered []Range) []Range {
	if lo > hi || hi <= watermark {
		return nil
	}
	if lo <= watermark {
		lo = watermark + 1 // watermark < hi, so this cannot overflow
	}
	var out []Range
	cur := lo
	for _, c := range Merge(covered) {
		if c.Hi < cur {
			continue
		}
		if c.Lo > hi {
			break
		}
		if c.Lo > cur {
			out = append(out, Range{Lo: cur, Hi: c.Lo - 1})
		}
		if c.Hi >= hi {
			return out
		}
		cur = c.Hi + 1 // c.Hi < hi
	}
	return append(out, Range{Lo: cur, Hi: hi})
}

// Advance merges add into covered and moves the watermark over every range
// that starts at or below watermark+1. It returns the new watermark and the
// merged ranges that remain above it.
func Advance(watermark uint64, covered []Range, add []Range) (uint64, []Range) {
	all := Merge(slices.Concat(covered, add))
	rest := make([]Range, 0, len(all))
	for _, r := range all {
		switch {
		case r.Hi <= watermark:
			// Already below the watermark.
		case r.Lo <= watermark+1: // watermark < r.Hi <= MaxUint64, no overflow
			watermark = r.Hi
		default:
			rest = append(rest, r)
		}
	}
	return watermark, rest
}

// Total is the number of seqs in rs, which must not overlap. It saturates.
func Total(rs []Range) uint64 {
	var n uint64
	for _, r := range rs {
		l := r.Len()
		if n > math.MaxUint64-l {
			return math.MaxUint64
		}
		n += l
	}
	return n
}
