package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	segmentSuffix = ".seg"
	corruptSuffix = ".corrupt"
	// indexEvery is the spacing of the sparse seq -> offset index.
	indexEvery = 64
)

type indexEntry struct {
	seq uint64
	off int64
}

// segment is the in-memory view of one segment file.
type segment struct {
	first uint64 // from the file name
	count uint64 // valid records
	size  int64  // bytes, header included
	path  string
	index []indexEntry
}

// last is the seq of the last record, or first-1 when the segment is empty.
func (s *segment) last() uint64 { return s.first + s.count - 1 }

func (s *segment) addIndex(seq uint64, off int64) {
	if (seq-s.first)%indexEvery == 0 {
		s.index = append(s.index, indexEntry{seq: seq, off: off})
	}
}

// offsetFor returns an offset at or before the record for seq, and the seq
// of the record at that offset.
func (s *segment) offsetFor(seq uint64) (int64, uint64) {
	i := sort.Search(len(s.index), func(i int) bool { return s.index[i].seq > seq })
	if i == 0 {
		return headerSize, s.first
	}
	e := s.index[i-1]
	return e.off, e.seq
}

func segmentName(first uint64) string { return fmt.Sprintf("%020d%s", first, segmentSuffix) }

// listSegments returns the segment files in dir sorted by first seq.
func listSegments(dir string) ([]*segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list wal dir: %w", err)
	}
	var segs []*segment
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		first, err := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)
		if err != nil || first == 0 {
			return nil, fmt.Errorf("wal: unexpected file %q in %s", name, dir)
		}
		segs = append(segs, &segment{first: first, path: filepath.Join(dir, name)})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].first < segs[j].first })
	return segs, nil
}

// scanResult describes how far a segment is valid.
type scanResult struct {
	validSize int64 // bytes of header + valid records
	fileSize  int64
	badHeader bool  // header missing or not a WAL header
	stopErr   error // why the scan stopped before fileSize (nil if it did not)
}

// scan reads the segment, fills count, size and index for its valid prefix,
// and stops at the first record that is torn, corrupt, out of sequence, or
// reaches limit (the first seq of the next segment, 0 for none).
func (s *segment) scan(limit uint64) (scanResult, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return scanResult{}, fmt.Errorf("open segment: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return scanResult{}, fmt.Errorf("stat segment: %w", err)
	}
	res := scanResult{fileSize: info.Size()}
	r := bufio.NewReaderSize(f, 64<<10)

	hdr := make([]byte, headerSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		res.badHeader = true
		res.stopErr = errShort
		return res, nil
	}
	if err := checkHeader(hdr); err != nil {
		var ve *VersionError
		if errors.As(err, &ve) {
			return res, err
		}
		res.badHeader = true
		res.stopErr = err
		return res, nil
	}

	s.count, s.index = 0, s.index[:0]
	off := int64(headerSize)
	buf := make([]byte, recordHeaderSize, recordHeaderSize+1024)
	for {
		if _, err := io.ReadFull(r, buf[:recordHeaderSize]); err != nil {
			if !errors.Is(err, io.EOF) {
				res.stopErr = errShort
			}
			break
		}
		n := int(leUint32(buf[0:]))
		if n > MaxPayload {
			res.stopErr = errCorrupt
			break
		}
		if cap(buf) < recordHeaderSize+n {
			nb := make([]byte, recordHeaderSize, recordHeaderSize+n)
			copy(nb, buf[:recordHeaderSize])
			buf = nb
		}
		buf = buf[:recordHeaderSize+n]
		if _, err := io.ReadFull(r, buf[recordHeaderSize:]); err != nil {
			res.stopErr = errShort
			break
		}
		seq, _, size, err := decodeRecord(buf)
		if err != nil {
			res.stopErr = err
			break
		}
		if seq != s.first+s.count || (limit != 0 && seq >= limit) {
			res.stopErr = errCorrupt
			break
		}
		s.addIndex(seq, off)
		s.count++
		off += int64(size)
	}
	s.size = off
	res.validSize = off
	return res, nil
}

func leUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// readRange reads up to n records from the segment starting at seq from,
// never past byte offset end or seq maxSeq.
func (s *segment) readRange(r io.ReaderAt, from uint64, n int, end int64, maxSeq uint64) ([]Record, error) {
	off, seq := s.offsetFor(from)
	if off >= end {
		return nil, nil
	}
	// A small buffer: the load generator runs thousands of logs at once.
	br := bufio.NewReaderSize(io.NewSectionReader(r, off, end-off), 4096)
	var out []Record
	var hdr [recordHeaderSize]byte
	for len(out) < n && seq <= maxSeq {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return out, fmt.Errorf("read record header in %s: %w", s.path, err)
		}
		n := int(leUint32(hdr[0:]))
		if n > MaxPayload {
			return out, fmt.Errorf("read %s: %w", s.path, errCorrupt)
		}
		rec := make([]byte, recordHeaderSize+n)
		copy(rec, hdr[:])
		if _, err := io.ReadFull(br, rec[recordHeaderSize:]); err != nil {
			return out, fmt.Errorf("read record in %s: %w", s.path, err)
		}
		got, payload, _, err := decodeRecord(rec)
		if err != nil || got != seq {
			return out, fmt.Errorf("read %s at seq %d: %w", s.path, seq, errCorrupt)
		}
		if seq >= from {
			out = append(out, Record{Seq: seq, Payload: payload})
		}
		seq++
	}
	return out, nil
}

// quarantineTail copies bytes [off, end) of the segment to a .corrupt file
// for later inspection, then truncates the segment at off.
func quarantineTail(path string, off int64) error {
	src, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open corrupt segment: %w", err)
	}
	defer src.Close()
	dst, err := os.OpenFile(path+corruptSuffix, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create quarantine file: %w", err)
	}
	if _, err := io.Copy(dst, io.NewSectionReader(src, off, 1<<62)); err != nil {
		_ = dst.Close()
		return fmt.Errorf("copy corrupt tail: %w", err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		return fmt.Errorf("fsync quarantine file: %w", err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("close quarantine file: %w", err)
	}
	return truncateFile(path, off)
}

func truncateFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open for truncate: %w", err)
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return fmt.Errorf("truncate %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("fsync %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}
