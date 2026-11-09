package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func payload(seq uint64) []byte { return fmt.Appendf(nil, "reading-%d", seq) }

func open(t testing.TB, opts Options) (*Log, Recovery) {
	t.Helper()
	l, rec, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return l, rec
}

func appendN(t testing.TB, l *Log, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		seq := l.Next()
		got, err := l.Append(payload(seq))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if got != seq {
			t.Fatalf("Append returned seq %d, want %d", got, seq)
		}
	}
}

func readAll(t testing.TB, l *Log, from uint64) []Record {
	t.Helper()
	var out []Record
	for {
		recs, err := l.Read(from, 1000)
		if err != nil {
			t.Fatalf("Read(%d): %v", from, err)
		}
		if len(recs) == 0 {
			return out
		}
		out = append(out, recs...)
		from = recs[len(recs)-1].Seq + 1
	}
}

func checkPayloads(t testing.TB, recs []Record) {
	t.Helper()
	for _, r := range recs {
		if !bytes.Equal(r.Payload, payload(r.Seq)) {
			t.Fatalf("seq %d has payload %q", r.Seq, r.Payload)
		}
	}
}

func TestAppendReadAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	l, _ := open(t, Options{Dir: dir, Sync: SyncAlways, SegmentSize: 512})
	appendN(t, l, 500)
	if st := l.Stats(); st.Segments < 10 || st.DurableSeq != 500 {
		t.Fatalf("stats = %+v, want many segments and durable 500", st)
	}
	recs := readAll(t, l, 1)
	if len(recs) != 500 || recs[0].Seq != 1 || recs[499].Seq != 500 {
		t.Fatalf("read %d records", len(recs))
	}
	checkPayloads(t, recs)
	// Reads from the middle use the sparse index.
	if recs, _ := l.Read(333, 5); len(recs) != 5 || recs[0].Seq != 333 {
		t.Fatalf("Read(333) = %v", recs)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, rec := open(t, Options{Dir: dir, Sync: SyncAlways, SegmentSize: 512})
	defer l.Close()
	if rec.Records != 500 || rec.NextSeq != 501 || len(rec.NewGaps) != 0 {
		t.Fatalf("recovery = %+v", rec)
	}
}

func TestIntervalSyncAdvancesDurable(t *testing.T) {
	l, _ := open(t, Options{Dir: t.TempDir(), Sync: SyncInterval, SyncInterval: 10 * time.Millisecond})
	defer l.Close()
	notify := l.Notify()
	appendN(t, l, 3)
	select {
	case <-notify:
	case <-time.After(5 * time.Second):
		t.Fatal("durable seq did not advance")
	}
	if d := l.Durable(); d != 3 {
		t.Fatalf("Durable = %d, want 3", d)
	}
}

func TestOnlyDurableRecordsAreRead(t *testing.T) {
	l, _ := open(t, Options{Dir: t.TempDir(), Sync: SyncInterval, SyncInterval: time.Hour})
	defer l.Close()
	appendN(t, l, 5)
	if recs, _ := l.Read(1, 10); len(recs) != 0 {
		t.Fatalf("read %d records before fsync", len(recs))
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if recs, _ := l.Read(1, 10); len(recs) != 5 {
		t.Fatalf("read %d records after fsync, want 5", len(recs))
	}
}

// TestTornTailAtEveryByte is P3: after a cut at any byte of the last
// segment, recovery keeps exactly the records written completely before it.
func TestTornTailAtEveryByte(t *testing.T) {
	src := t.TempDir()
	l, _ := open(t, Options{Dir: src, Sync: SyncAlways})
	appendN(t, l, 20)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	seg := filepath.Join(src, segmentName(1))
	full, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	// Record boundaries, to know how many records fit before each cut.
	var ends []int
	for off := headerSize; off < len(full); {
		_, _, n, err := decodeRecord(full[off:])
		if err != nil {
			t.Fatal(err)
		}
		off += n
		ends = append(ends, off)
	}
	for cut := 0; cut <= len(full); cut++ {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, segmentName(1)), full[:cut], 0o600); err != nil {
			t.Fatal(err)
		}
		want := 0
		for _, e := range ends {
			if e <= cut {
				want++
			}
		}
		l, rec := open(t, Options{Dir: dir, Sync: SyncAlways})
		recs := readAll(t, l, 1)
		if len(recs) != want || rec.NextSeq != uint64(want)+1 || len(rec.NewGaps) != 0 {
			t.Fatalf("cut at %d: recovered %d records, next %d, gaps %v; want %d records", cut, len(recs), rec.NextSeq, rec.NewGaps, want)
		}
		checkPayloads(t, recs)
		// The log must stay appendable after recovery.
		appendN(t, l, 2)
		if got := readAll(t, l, 1); len(got) != want+2 {
			t.Fatalf("cut at %d: %d records after append, want %d", cut, len(got), want+2)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMiddleSegmentCorruptionBecomesGap(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Sync: SyncAlways, SegmentSize: 256}
	l, _ := open(t, opts)
	appendN(t, l, 60)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	segs, err := listSegments(dir)
	if err != nil || len(segs) < 3 {
		t.Fatalf("want at least 3 segments, got %d (%v)", len(segs), err)
	}
	mid, next := segs[1], segs[2]
	data, err := os.ReadFile(mid.path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip one payload bit in the third record of the middle segment.
	off := headerSize
	for i := 0; i < 2; i++ {
		_, _, n, _ := decodeRecord(data[off:])
		off += n
	}
	data[off+recordHeaderSize] ^= 0x01
	if err := os.WriteFile(mid.path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	l, rec := open(t, opts)
	defer l.Close()
	wantGap := Gap{From: mid.first + 2, To: next.first - 1, Reason: GapCorruption}
	if !slices.Equal(rec.NewGaps, []Gap{wantGap}) || !slices.Equal(l.PendingGaps(), []Gap{wantGap}) {
		t.Fatalf("gaps = %v / %v, want %v", rec.NewGaps, l.PendingGaps(), wantGap)
	}
	if _, err := os.Stat(mid.path + corruptSuffix); err != nil {
		t.Fatalf("corrupt tail not quarantined: %v", err)
	}
	// Reading stops at the hole; the next read skips past it.
	first, _ := l.Read(1, 1000)
	if got := first[len(first)-1].Seq; got != wantGap.From-1 {
		t.Fatalf("first range ends at %d, want %d", got, wantGap.From-1)
	}
	after, _ := l.Read(wantGap.From, 1000)
	if len(after) == 0 || after[0].Seq != next.first {
		t.Fatalf("read after hole starts at %v, want %d", after, next.first)
	}
	if rec.NextSeq != 61 {
		t.Fatalf("NextSeq = %d, want 61", rec.NextSeq)
	}
}

func TestBadHeaderInLastSegment(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Sync: SyncAlways, SegmentSize: 256}
	l, _ := open(t, opts)
	appendN(t, l, 30)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := listSegments(dir)
	last := segs[len(segs)-1]
	f, err := os.OpenFile(last.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("XXXX"), 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	l, rec := open(t, opts)
	defer l.Close()
	// The state file says seqs up to 30 were durable, so the unreadable
	// segment's seqs are reported, and new readings start after them.
	want := []Gap{{From: last.first, To: 30, Reason: GapCorruption}}
	if !slices.Equal(rec.NewGaps, want) || rec.NextSeq != 31 {
		t.Fatalf("recovery = %+v, want gaps %v and next 31", rec, want)
	}
}

func TestNewerFormatIsRefused(t *testing.T) {
	dir := t.TempDir()
	hdr := encodeHeader()
	hdr[6] = 2
	if err := os.WriteFile(filepath.Join(dir, segmentName(1)), hdr, 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err := Open(Options{Dir: dir})
	var ve *VersionError
	if !errors.As(err, &ve) || ve.Version != 2 {
		t.Fatalf("Open = %v, want VersionError", err)
	}
}

func TestDiskBudgetDropsOldestAsGap(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Sync: SyncAlways, SegmentSize: 256, MaxSize: 1024}
	l, _ := open(t, opts)
	appendN(t, l, 10)
	l.Ack(4)
	if err := l.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	appendN(t, l, 200)
	st := l.Stats()
	if st.Bytes > opts.MaxSize+opts.SegmentSize {
		t.Fatalf("log is %d bytes, budget %d", st.Bytes, opts.MaxSize)
	}
	gaps := l.PendingGaps()
	if len(gaps) != 1 || gaps[0].From != 5 || gaps[0].Reason != GapDiskBudget {
		t.Fatalf("gaps = %v, want one DISK_BUDGET gap starting at 5", gaps)
	}
	if st.DroppedTotal != gaps[0].To-gaps[0].From+1 {
		t.Fatalf("dropped %d, gap covers %d", st.DroppedTotal, gaps[0].To-gaps[0].From+1)
	}
	// Every seq above the ack is either readable or inside the gap.
	recs := readAll(t, l, 5)
	if recs[0].Seq != gaps[0].To+1 || recs[len(recs)-1].Seq != 210 {
		t.Fatalf("readable range %d-%d, gap %v", recs[0].Seq, recs[len(recs)-1].Seq, gaps)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// The gap survives a restart because it was persisted before the delete.
	l, _ = open(t, opts)
	defer l.Close()
	if got := l.PendingGaps(); !slices.Equal(got, gaps) {
		t.Fatalf("gaps after restart = %v, want %v", got, gaps)
	}
}

// TestCheckpointDeletesOnlyPersistedAcked is P4.
func TestCheckpointDeletesOnlyPersistedAcked(t *testing.T) {
	dir := t.TempDir()
	l, _ := open(t, Options{Dir: dir, Sync: SyncAlways, SegmentSize: 256})
	defer l.Close()
	appendN(t, l, 100)
	l.Ack(57)
	// Ack alone changes memory only: nothing may be deleted yet.
	if got := readAll(t, l, 1); got[0].Seq != 1 {
		t.Fatalf("segments deleted before checkpoint; first readable %d", got[0].Seq)
	}
	if err := l.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	st, err := LoadState(dir)
	if err != nil || st.Acked != 57 {
		t.Fatalf("persisted state %+v, %v", st, err)
	}
	got := readAll(t, l, 1)
	if got[0].Seq > 58 || got[len(got)-1].Seq != 100 {
		t.Fatalf("readable %d-%d after ack 57", got[0].Seq, got[len(got)-1].Seq)
	}
	segs, _ := listSegments(dir)
	for _, s := range segs[:len(segs)-1] {
		if s.first <= 1 {
			t.Fatalf("segment starting at %d should have been deleted", s.first)
		}
	}
}

func TestAckJumpMovesNextSeq(t *testing.T) {
	l, _ := open(t, Options{Dir: t.TempDir(), Sync: SyncAlways})
	defer l.Close()
	appendN(t, l, 5)
	res := l.Ack(900)
	if !res.Jumped || res.OldNext != 6 || l.Next() != 901 {
		t.Fatalf("Ack(900) = %+v, next %d", res, l.Next())
	}
	if seq, err := l.Append(payload(901)); err != nil || seq != 901 {
		t.Fatalf("Append after jump = %d, %v", seq, err)
	}
	if res := l.Ack(10); res.Advanced {
		t.Fatalf("old ack advanced: %+v", res)
	}
}

func TestLostSegmentsAreReported(t *testing.T) {
	dir := t.TempDir()
	l, _ := open(t, Options{Dir: dir, Sync: SyncAlways, SegmentSize: 256})
	appendN(t, l, 50)
	l.Ack(10)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Somebody wipes the segments but the state file survives.
	segs, _ := listSegments(dir)
	for _, s := range segs {
		if err := os.Remove(s.path); err != nil {
			t.Fatal(err)
		}
	}
	l, rec := open(t, Options{Dir: dir, Sync: SyncAlways})
	defer l.Close()
	want := []Gap{{From: 11, To: 50, Reason: GapLost}}
	if !slices.Equal(rec.NewGaps, want) || rec.NextSeq != 51 {
		t.Fatalf("recovery = %+v, want gap %v and next 51", rec, want)
	}
}

// powerCut simulates losing power: the log stops without any further
// fsync, and every byte that was not durable is gone.
func powerCut(t *testing.T, l *Log) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	close(l.stop)
	if l.active != nil {
		path := l.segs[len(l.segs)-1].path
		_ = l.active.Close()
		l.active = nil
		if err := os.Truncate(path, l.durableOff); err != nil {
			t.Fatal(err)
		}
	}
}

// TestNoSeqReuseAcrossCrashes is P5: across any mix of appends, sends, acks,
// checkpoints, power cuts and restarts, no seq that left the device is ever
// handed out again, and every sent seq stays on disk, acked, or in a gap.
func TestNoSeqReuseAcrossCrashes(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		mode := rapid.SampledFrom([]SyncMode{SyncAlways, SyncInterval}).Draw(rt, "mode")
		opts := Options{Dir: dir, Sync: mode, SyncInterval: time.Hour, SegmentSize: 200, MaxSize: 2000}
		l, _, err := Open(opts)
		if err != nil {
			rt.Fatal(err)
		}
		sent := map[uint64]bool{}
		var maxSent uint64
		check := func() {
			acked := l.Acked()
			gaps := l.PendingGaps()
			for seq := range sent {
				if seq <= acked || slices.ContainsFunc(gaps, func(g Gap) bool { return g.From <= seq && seq <= g.To }) {
					continue
				}
				recs, err := l.Read(seq, 1)
				if err != nil || len(recs) == 0 || recs[0].Seq != seq || !bytes.Equal(recs[0].Payload, payload(seq)) {
					rt.Fatalf("sent seq %d is not acked (%d), not in a gap (%v), and not readable", seq, acked, gaps)
				}
			}
		}
		for i := rapid.IntRange(1, 60).Draw(rt, "steps"); i > 0; i-- {
			switch rapid.IntRange(0, 6).Draw(rt, "op") {
			case 0, 1:
				for n := rapid.IntRange(1, 10).Draw(rt, "n"); n > 0; n-- {
					seq := l.Next()
					if seq <= maxSent {
						rt.Fatalf("seq %d handed out again (max sent %d)", seq, maxSent)
					}
					if _, err := l.Append(payload(seq)); err != nil {
						rt.Fatal(err)
					}
				}
			case 2: // the uplink sends whatever is durable
				if err := l.Sync(); err != nil {
					rt.Fatal(err)
				}
				for _, r := range readAll(t, l, l.Acked()+1) {
					sent[r.Seq] = true
					maxSent = max(maxSent, r.Seq)
				}
			case 3:
				if maxSent > 0 {
					l.Ack(rapid.Uint64Range(0, maxSent).Draw(rt, "ack"))
				}
			case 4:
				if err := l.Checkpoint(); err != nil {
					rt.Fatal(err)
				}
			case 5:
				powerCut(t, l)
				if l, _, err = Open(opts); err != nil {
					rt.Fatal(err)
				}
			case 6:
				if err := l.Close(); err != nil {
					rt.Fatal(err)
				}
				if l, _, err = Open(opts); err != nil {
					rt.Fatal(err)
				}
			}
			check()
			if l.Next() <= maxSent {
				rt.Fatalf("next seq %d would reuse a sent seq (max sent %d)", l.Next(), maxSent)
			}
		}
		if err := l.Close(); err != nil {
			rt.Fatal(err)
		}
	})
}

func TestSaveStateCrashAtEveryStep(t *testing.T) {
	old := State{Version: stateVersion, Acked: 10, MaxSeqEver: 12}
	next := State{Version: stateVersion, Acked: 20, MaxSeqEver: 25, PendingGaps: []Gap{{From: 21, To: 22, Reason: GapDiskBudget}}}
	errCrash := errors.New("crash")
	for _, step := range []string{"write", "fsync", "rename", "syncdir"} {
		t.Run(step, func(t *testing.T) {
			dir := t.TempDir()
			if err := SaveState(dir, old); err != nil {
				t.Fatal(err)
			}
			stateHook = func(s string) error {
				if s == step {
					return errCrash
				}
				return nil
			}
			t.Cleanup(func() { stateHook = nil })
			if err := SaveState(dir, next); !errors.Is(err, errCrash) {
				t.Fatalf("SaveState = %v", err)
			}
			stateHook = nil
			got, err := LoadState(dir)
			if err != nil {
				t.Fatalf("LoadState after crash at %s: %v", step, err)
			}
			if !got.equal(old) && !got.equal(next) {
				t.Fatalf("state after crash at %s is %+v: neither old nor new", step, got)
			}
			// A later save must still work over the leftover temp file.
			if err := SaveState(dir, next); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTornTempStateIsIgnored(t *testing.T) {
	dir := t.TempDir()
	want := State{Version: stateVersion, Acked: 3}
	if err := SaveState(dir, want); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateTmpFile), []byte(`{"version":1,"ack`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadState(dir)
	if err != nil || !got.equal(want) {
		t.Fatalf("LoadState = %+v, %v", got, err)
	}
}

func TestCorruptStateIsAnError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, stateFile), []byte("{nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(Options{Dir: dir}); !errors.Is(err, ErrCorruptState) {
		t.Fatalf("Open = %v, want ErrCorruptState", err)
	}
}

func TestParseSyncMode(t *testing.T) {
	for _, s := range []string{"always", "interval", "never"} {
		m, err := ParseSyncMode(s)
		if err != nil || m.String() != s {
			t.Fatalf("ParseSyncMode(%q) = %v, %v", s, m, err)
		}
	}
	if _, err := ParseSyncMode("sometimes"); err == nil {
		t.Fatal("expected an error")
	}
}

func FuzzRecordDecode(f *testing.F) {
	f.Add(appendRecord(nil, 1, []byte("hello")))
	f.Add(appendRecord(nil, 1<<40, nil))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		seq, p, n, err := decodeRecord(b)
		if err != nil {
			if !errors.Is(err, errShort) && !errors.Is(err, errCorrupt) {
				t.Fatalf("unexpected error %v", err)
			}
			return
		}
		if n > len(b) || len(p) > MaxPayload {
			t.Fatalf("size %d / payload %d out of bounds", n, len(p))
		}
		if !bytes.Equal(appendRecord(nil, seq, p), b[:n]) {
			t.Fatal("re-encoding a decoded record gives different bytes")
		}
	})
}
