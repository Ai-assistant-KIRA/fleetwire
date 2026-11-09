// Package wal is the device-side write-ahead log.
//
// Every reading is appended here before it may be sent, and only records
// that are durable (fsynced, depending on the sync mode) are handed to the
// uplink. Segments are deleted only once the state file holds an ack that
// covers them, so a seq that left the device is always either still on disk,
// covered by the persisted ack, or inside a gap that is reported to the
// server (invariant W1 in the design).
package wal

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/coverage"
)

// SyncMode controls when appended records become durable.
type SyncMode int

// Sync modes. See docs/explanation/wal.md for what each can lose.
const (
	SyncInterval SyncMode = iota
	SyncAlways
	SyncNever
)

func (m SyncMode) String() string {
	switch m {
	case SyncAlways:
		return "always"
	case SyncNever:
		return "never"
	default:
		return "interval"
	}
}

// ParseSyncMode parses "always", "interval" or "never".
func ParseSyncMode(s string) (SyncMode, error) {
	switch s {
	case "always":
		return SyncAlways, nil
	case "interval":
		return SyncInterval, nil
	case "never":
		return SyncNever, nil
	}
	return 0, fmt.Errorf("unknown wal sync mode %q (want always, interval or never)", s)
}

// Defaults from the requirements.
const (
	DefaultSegmentSize  = 4 << 20
	DefaultMaxSize      = 256 << 20
	DefaultSyncInterval = time.Second
)

// Options configure a Log.
type Options struct {
	Dir          string
	SegmentSize  int64
	MaxSize      int64
	Sync         SyncMode
	SyncInterval time.Duration
	Logger       *slog.Logger
}

func (o *Options) setDefaults() {
	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultSegmentSize
	}
	if o.MaxSize <= 0 {
		o.MaxSize = DefaultMaxSize
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = DefaultSyncInterval
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
}

// Record is one entry of the log.
type Record struct {
	Seq     uint64
	Payload []byte
}

// Recovery reports what Open found on disk.
type Recovery struct {
	Segments       int
	Records        uint64
	TruncatedBytes int64
	// NewGaps are gaps found during recovery: corrupt middle segments and
	// seqs that were handed out but are no longer on disk.
	NewGaps []Gap
	NextSeq uint64
}

// Stats is a snapshot for metrics and the status command.
type Stats struct {
	Bytes       int64
	Segments    int
	NextSeq     uint64
	DurableSeq  uint64
	Acked       uint64
	PendingGaps int
	// DroppedTotal counts readings dropped by the disk budget since Open.
	DroppedTotal uint64
}

// AckResult describes the effect of Ack.
type AckResult struct {
	// Advanced is false for an ack at or below the current one.
	Advanced bool
	// Jumped is true when the server's ack was at or above the next seq the
	// device would hand out, so the next seq moved past it (R3.8).
	Jumped bool
	// OldNext is the next seq before a jump.
	OldNext uint64
}

// ErrClosed is returned by operations on a closed Log.
var ErrClosed = errors.New("wal: log is closed")

// Log is a segmented write-ahead log. All methods are safe for concurrent use.
type Log struct {
	opts Options
	log  *slog.Logger

	mu         sync.Mutex
	segs       []*segment
	active     *os.File // write handle of the last segment
	next       uint64   // next seq to hand out
	written    uint64   // last seq written to the OS
	durable    uint64   // last seq known to be durable
	durableOff int64    // bytes of the last segment that hold durable records
	state      State    // in memory
	persisted  State    // last state written to disk
	dropped    uint64
	notify     chan struct{}
	buf        []byte
	closed     bool

	stop chan struct{}
	done chan struct{}
}

// Open opens or creates the log in opts.Dir and recovers it.
func Open(opts Options) (*Log, Recovery, error) {
	opts.setDefaults()
	if opts.Dir == "" {
		return nil, Recovery{}, errors.New("wal: Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, Recovery{}, fmt.Errorf("create wal dir: %w", err)
	}
	st, err := LoadState(opts.Dir)
	if err != nil {
		return nil, Recovery{}, err
	}
	l := &Log{
		opts:      opts,
		log:       opts.Logger,
		state:     st.clone(),
		persisted: st.clone(),
		notify:    make(chan struct{}),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	rec, err := l.recover()
	if err != nil {
		return nil, Recovery{}, err
	}
	if opts.Sync == SyncInterval {
		go l.syncLoop()
	} else {
		close(l.done)
	}
	return l, rec, nil
}

// recover scans every segment, repairs what it can, and computes the next seq.
func (l *Log) recover() (Recovery, error) {
	segs, err := listSegments(l.opts.Dir)
	if err != nil {
		return Recovery{}, err
	}
	var rec Recovery
	var keep []*segment
	// corruptFrom holds the first seq of each unreadable stretch. The stretch
	// ends where the next readable segment starts.
	var corruptFrom []uint64
	for i, s := range segs {
		var limit uint64
		if i+1 < len(segs) {
			limit = segs[i+1].first
		}
		last := i == len(segs)-1
		res, err := s.scan(limit)
		if err != nil {
			return Recovery{}, err
		}
		switch {
		case res.badHeader && res.fileSize <= headerSize && last:
			// Crash while creating the newest segment: it holds nothing.
			if err := os.Remove(s.path); err != nil {
				return Recovery{}, fmt.Errorf("remove empty segment: %w", err)
			}
			rec.TruncatedBytes += res.fileSize
			continue
		case res.badHeader:
			// The whole file is unreadable. Keep it for inspection.
			if err := os.Rename(s.path, s.path+corruptSuffix); err != nil {
				return Recovery{}, fmt.Errorf("quarantine segment: %w", err)
			}
			l.log.Error("wal segment has a bad header; quarantined", "segment", s.path)
			corruptFrom = append(corruptFrom, s.first)
			continue
		case res.stopErr != nil && last:
			// A torn write at the tail: cut it off. Those records were never
			// durable, so they never left the device.
			if err := truncateFile(s.path, res.validSize); err != nil {
				return Recovery{}, err
			}
			rec.TruncatedBytes += res.fileSize - res.validSize
			l.log.Warn("wal tail truncated after crash", "segment", s.path, "bytes", res.fileSize-res.validSize)
		case res.stopErr != nil:
			// Damage in the middle of the log. Keep the bytes for inspection;
			// the missing seqs become a CORRUPTION gap below.
			if err := quarantineTail(s.path, res.validSize); err != nil {
				return Recovery{}, err
			}
			l.log.Error("wal segment corrupt; tail quarantined", "segment", s.path, "valid_records", s.count)
			corruptFrom = append(corruptFrom, s.last()+1)
		}
		keep = append(keep, s)
		rec.Records += s.count
	}
	l.segs = keep
	rec.Segments = len(keep)
	if err := syncDir(l.opts.Dir); err != nil {
		return Recovery{}, err
	}

	// Every seq from 1 to the highest one known must be on disk, at or below
	// the ack, or inside a gap. Anything else is reported, never reused.
	// The newest segment can be empty (a power cut right after a roll), so
	// the last seq comes from the last segment that holds records.
	var lastWAL uint64
	var known []coverage.Range
	for _, s := range l.segs {
		if s.count > 0 {
			lastWAL = s.last()
			known = append(known, coverage.Range{Lo: s.first, Hi: s.last()})
		}
	}
	var maxGap uint64
	for _, g := range l.state.PendingGaps {
		known = append(known, coverage.Range{Lo: g.From, Hi: g.To})
		maxGap = max(maxGap, g.To)
	}
	top := max(lastWAL, l.state.Acked, l.state.MaxSeqEver, maxGap)
	report := func(lo, hi uint64, reason GapReason) {
		for _, r := range coverage.Uncovered(lo, hi, l.state.Acked, known) {
			g := Gap{From: r.Lo, To: r.Hi, Reason: reason}
			l.addGap(g)
			known = append(known, r)
			rec.NewGaps = append(rec.NewGaps, g)
			l.log.Error("wal: seqs are not recoverable; reporting them as a gap",
				"from", r.Lo, "to", r.Hi, "reason", string(reason))
		}
	}
	for _, from := range corruptFrom {
		to := top
		for _, s := range l.segs {
			if s.first > from {
				to = s.first - 1
				break
			}
		}
		report(from, to, GapCorruption)
	}
	report(1, top, GapLost)

	l.written = lastWAL
	l.durable = lastWAL
	l.next = top + 1
	if n := len(l.segs); n > 0 {
		last := l.segs[n-1]
		f, err := os.OpenFile(last.path, os.O_RDWR, 0)
		if err != nil {
			return Recovery{}, fmt.Errorf("open active segment: %w", err)
		}
		// Records that survived a process crash may still sit in the page
		// cache only. Make them durable before anything reads them.
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return Recovery{}, fmt.Errorf("fsync active segment: %w", err)
		}
		l.active = f
		l.durableOff = last.size
	}
	if len(rec.NewGaps) > 0 {
		if err := l.persistLocked(); err != nil {
			return Recovery{}, err
		}
	}
	rec.NextSeq = l.next
	return rec, nil
}

// addGap adds g to the pending gaps and the gap history.
func (l *Log) addGap(g Gap) {
	l.state.PendingGaps = mergeGaps(append(l.state.PendingGaps, g))
	h := mergeGaps(append(l.state.GapHistory, g))
	if len(h) > maxGapHistory {
		h = h[len(h)-maxGapHistory:]
	}
	l.state.GapHistory = h
}

// mergeGaps sorts gaps and merges touching ranges with the same reason.
func mergeGaps(gaps []Gap) []Gap {
	slices.SortFunc(gaps, func(a, b Gap) int {
		switch {
		case a.From < b.From:
			return -1
		case a.From > b.From:
			return 1
		default:
			return 0
		}
	})
	merged := gaps[:1]
	for _, g := range gaps[1:] {
		p := &merged[len(merged)-1]
		if g.Reason == p.Reason && g.From <= p.To+1 {
			p.To = max(p.To, g.To)
			continue
		}
		merged = append(merged, g)
	}
	return merged
}

// GapHistory returns every gap this log ever reported.
func (l *Log) GapHistory() []Gap {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.state.GapHistory)
}

// Inspection is a read-only view of a log directory.
type Inspection struct {
	State    State
	LastSeq  uint64 // last valid record on disk (0 if none)
	Segments int
	Bytes    int64
}

// Inspect reads a log directory without changing it, so it is safe while
// the agent is stopped and repairs nothing. A torn tail is simply not
// counted. It must not run while another process has the log open for
// writing, because that process may be mid-append.
func Inspect(dir string) (Inspection, error) {
	st, err := LoadState(dir)
	if err != nil {
		return Inspection{}, err
	}
	segs, err := listSegments(dir)
	if err != nil {
		return Inspection{}, err
	}
	in := Inspection{State: st}
	for i, s := range segs {
		var limit uint64
		if i+1 < len(segs) {
			limit = segs[i+1].first
		}
		if _, err := s.scan(limit); err != nil {
			return Inspection{}, err
		}
		in.Segments++
		in.Bytes += s.size
		if s.count > 0 {
			in.LastSeq = max(in.LastSeq, s.last())
		}
	}
	return in, nil
}

// Append writes payload as the record with the next seq and returns the seq.
func (l *Log) Append(payload []byte) (uint64, error) {
	if len(payload) > MaxPayload {
		return 0, fmt.Errorf("wal: payload of %d bytes exceeds %d", len(payload), MaxPayload)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	seq := l.next
	size := int64(recordHeaderSize + len(payload))
	act := l.activeSeg()
	if act == nil || seq != act.last()+1 || (act.count > 0 && act.size+size > l.opts.SegmentSize) {
		if err := l.roll(seq); err != nil {
			return 0, err
		}
		act = l.activeSeg()
	}
	l.buf = appendRecord(l.buf[:0], seq, payload)
	if _, err := l.active.WriteAt(l.buf, act.size); err != nil {
		// Cut off whatever part of the record reached the file, so a later
		// record is not written after a torn one.
		if terr := l.active.Truncate(act.size); terr != nil {
			return 0, fmt.Errorf("wal write failed (%w) and truncate failed: %w", err, terr)
		}
		return 0, fmt.Errorf("wal write: %w", err)
	}
	act.addIndex(seq, act.size)
	act.size += size
	act.count++
	l.written = seq
	l.next = seq + 1
	switch l.opts.Sync {
	case SyncAlways:
		if err := l.active.Sync(); err != nil {
			return seq, fmt.Errorf("wal fsync: %w", err)
		}
		l.markDurableLocked()
	case SyncNever:
		l.markDurableLocked()
	case SyncInterval:
		// The sync loop makes it durable.
	}
	return seq, nil
}

func (l *Log) activeSeg() *segment {
	if len(l.segs) == 0 || l.active == nil {
		return nil
	}
	return l.segs[len(l.segs)-1]
}

// markDurableLocked publishes everything written so far as durable.
func (l *Log) markDurableLocked() {
	if l.written <= l.durable && l.durableOff == l.activeSize() {
		return
	}
	l.durable = l.written
	l.durableOff = l.activeSize()
	close(l.notify)
	l.notify = make(chan struct{})
}

func (l *Log) activeSize() int64 {
	if act := l.activeSeg(); act != nil {
		return act.size
	}
	return 0
}

// roll seals the active segment and starts a new one whose first seq is first.
func (l *Log) roll(first uint64) error {
	if first <= l.written {
		// Segments must stay sorted and disjoint; reading depends on it.
		return fmt.Errorf("wal: refusing to start a segment at seq %d, seq %d is already written", first, l.written)
	}
	if act := l.activeSeg(); act != nil {
		if err := l.active.Sync(); err != nil {
			return fmt.Errorf("wal fsync before roll: %w", err)
		}
		l.markDurableLocked()
		if err := l.active.Close(); err != nil {
			return fmt.Errorf("close segment: %w", err)
		}
		l.active = nil
		if act.count == 0 {
			// An empty segment cannot be reused under another name.
			if err := os.Remove(act.path); err != nil {
				return fmt.Errorf("remove empty segment: %w", err)
			}
			l.segs = l.segs[:len(l.segs)-1]
		}
	}
	s := &segment{first: first, size: headerSize, path: filepath.Join(l.opts.Dir, segmentName(first))}
	f, err := os.OpenFile(s.path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create segment: %w", err)
	}
	if _, err := f.Write(encodeHeader()); err != nil {
		_ = f.Close()
		return fmt.Errorf("write segment header: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("fsync new segment: %w", err)
	}
	if err := syncDir(l.opts.Dir); err != nil {
		_ = f.Close()
		return err
	}
	l.segs = append(l.segs, s)
	l.active = f
	l.durableOff = headerSize
	l.enforceBudgetLocked()
	return nil
}

// enforceBudgetLocked drops the oldest sealed segments while the log is over
// its size budget. Seqs that were not acked yet become a DISK_BUDGET gap,
// which is persisted before the file is deleted (W1).
func (l *Log) enforceBudgetLocked() {
	for len(l.segs) > 1 && l.totalBytes() > l.opts.MaxSize {
		old := l.segs[0]
		var gap *Gap
		if old.count > 0 && old.last() > l.state.Acked {
			g := Gap{From: max(old.first, l.state.Acked+1), To: old.last(), Reason: GapDiskBudget}
			gap = &g
			l.addGap(g)
		}
		if err := l.persistLocked(); err != nil {
			// Better over budget than losing the record of a dropped range.
			l.log.Error("wal over budget but state could not be saved; keeping segment", "err", err)
			return
		}
		if err := os.Remove(old.path); err != nil {
			l.log.Error("wal could not delete segment", "segment", old.path, "err", err)
			return
		}
		l.segs = l.segs[1:]
		if gap != nil {
			n := gap.To - gap.From + 1
			l.dropped += n
			l.log.Warn("wal disk budget reached; dropped oldest unsent readings",
				"from", gap.From, "to", gap.To, "readings", n, "max_bytes", l.opts.MaxSize)
		}
	}
}

func (l *Log) totalBytes() int64 {
	var n int64
	for _, s := range l.segs {
		n += s.size
	}
	return n
}

func (l *Log) syncLoop() {
	defer close(l.done)
	t := time.NewTicker(l.opts.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			if err := l.Sync(); err != nil && !errors.Is(err, ErrClosed) {
				l.log.Error("wal periodic fsync failed", "err", err)
			}
		}
	}
}

// Sync makes every written record durable.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if l.active == nil || (l.written <= l.durable && l.durableOff == l.activeSize()) {
		return nil
	}
	if err := l.active.Sync(); err != nil {
		return fmt.Errorf("wal fsync: %w", err)
	}
	l.markDurableLocked()
	return nil
}

// Durable returns the highest durable seq (0 if none).
func (l *Log) Durable() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.durable
}

// Notify returns a channel that is closed the next time the durable seq moves.
func (l *Log) Notify() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.notify
}

// Read returns up to n durable records starting at from, in seq order and
// without holes. If from falls in a hole (a dropped range), the result starts
// at the first record after it.
func (l *Log) Read(from uint64, n int) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrClosed
	}
	if n <= 0 || from > l.durable {
		return nil, nil
	}
	i, ok := slices.BinarySearchFunc(l.segs, from, func(s *segment, seq uint64) int {
		switch {
		case s.count > 0 && s.last() < seq:
			return -1
		case s.first > seq:
			return 1
		default:
			return 0
		}
	})
	if !ok && i < len(l.segs) {
		from = l.segs[i].first
	}
	var out []Record
	for ; i < len(l.segs) && len(out) < n; i++ {
		s := l.segs[i]
		if s.count == 0 {
			continue
		}
		if len(out) > 0 && s.first != out[len(out)-1].Seq+1 {
			break // a hole: the caller sends the next range in another batch
		}
		limit := s.size
		var ra interface {
			ReadAt([]byte, int64) (int, error)
		}
		if i == len(l.segs)-1 && l.active != nil {
			limit = l.durableOff
			ra = l.active
		}
		start := from
		if len(out) > 0 {
			start = s.first
		}
		var recs []Record
		var err error
		if ra != nil {
			recs, err = s.readRange(ra, start, n-len(out), limit, l.durable)
		} else {
			recs, err = readSealed(s, start, n-len(out), limit, l.durable)
		}
		if err != nil {
			return out, err
		}
		out = append(out, recs...)
		if len(recs) == 0 || recs[len(recs)-1].Seq != s.last() {
			break
		}
	}
	return out, nil
}

func readSealed(s *segment, from uint64, n int, end int64, maxSeq uint64) ([]Record, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("open segment for read: %w", err)
	}
	defer f.Close()
	return s.readRange(f, from, n, end, maxSeq)
}

// Ack records in memory that every seq <= n is covered on the server.
// Checkpoint persists it and deletes the covered segments.
func (l *Log) Ack(n uint64) AckResult {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n <= l.state.Acked {
		return AckResult{}
	}
	res := AckResult{Advanced: true}
	l.state.Acked = n
	gaps := l.state.PendingGaps[:0]
	for _, g := range l.state.PendingGaps {
		if g.To > n {
			gaps = append(gaps, g)
		}
	}
	l.state.PendingGaps = gaps
	if n >= l.next {
		// The server has seqs this device does not remember handing out
		// (for example, the disk was restored from an old copy). New
		// readings must not reuse them, or ingest would drop them as
		// duplicates.
		res.Jumped, res.OldNext = true, l.next
		l.next = n + 1
	}
	return res
}

// Acked returns the in-memory ack.
func (l *Log) Acked() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state.Acked
}

// Next returns the seq the next Append will use.
func (l *Log) Next() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next
}

// PendingGaps returns the gaps not yet covered by an ack.
func (l *Log) PendingGaps() []Gap {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.state.PendingGaps)
}

// State returns a copy of the in-memory state.
func (l *Log) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state.clone()
}

// SetDeviceID records the identity this log belongs to.
func (l *Log) SetDeviceID(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state.DeviceID = id
}

// Checkpoint saves the state if it changed, then deletes every segment whose
// records are all covered by the saved ack.
func (l *Log) Checkpoint() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if err := l.persistLocked(); err != nil {
		return err
	}
	return l.deleteAckedLocked()
}

func (l *Log) persistLocked() error {
	l.state.MaxSeqEver = max(l.state.MaxSeqEver, l.durable)
	if l.state.equal(l.persisted) {
		return nil
	}
	if err := SaveState(l.opts.Dir, l.state); err != nil {
		return err
	}
	l.persisted = l.state.clone()
	return nil
}

// deleteAckedLocked removes sealed segments covered by the persisted ack.
// It uses l.persisted, never l.state, which is what makes W1 hold.
func (l *Log) deleteAckedLocked() error {
	removed := 0
	for len(l.segs) > 1 {
		s := l.segs[0]
		if s.count > 0 && s.last() > l.persisted.Acked {
			break
		}
		if err := os.Remove(s.path); err != nil {
			return fmt.Errorf("delete acked segment: %w", err)
		}
		l.segs = l.segs[1:]
		removed++
	}
	if removed > 0 {
		return syncDir(l.opts.Dir)
	}
	return nil
}

// Stats returns a snapshot for metrics.
func (l *Log) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Stats{
		Bytes:        l.totalBytes(),
		Segments:     len(l.segs),
		NextSeq:      l.next,
		DurableSeq:   l.durable,
		Acked:        l.state.Acked,
		PendingGaps:  len(l.state.PendingGaps),
		DroppedTotal: l.dropped,
	}
}

// Close makes everything durable, saves the state and releases the files.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.stop)
	var errs []error
	if l.active != nil {
		if err := l.active.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("wal final fsync: %w", err))
		} else {
			l.markDurableLocked()
		}
	}
	if err := l.persistLocked(); err != nil {
		errs = append(errs, err)
	}
	if l.active != nil {
		if err := l.active.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close active segment: %w", err))
		}
		l.active = nil
	}
	l.mu.Unlock()
	<-l.done
	return errors.Join(errs...)
}
