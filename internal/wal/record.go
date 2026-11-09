package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// On-disk format, all integers little-endian:
//
//	segment header (16 bytes): magic "FWWAL\x00" | version u16 | reserved [8]byte
//	record:                    length u32 | crc32c u32 | seq u64 | payload [length]byte
//
// The CRC covers seq and payload, so a record that is intact but sits at the
// wrong position (an old seq left in place) is still caught by the
// consecutive-seq check, and a torn seq field is caught by the CRC.
const (
	headerSize       = 16
	recordHeaderSize = 16
	formatVersion    = 1

	// MaxPayload bounds one record. A corrupted length field larger than this
	// is treated as corruption instead of triggering a huge allocation.
	MaxPayload = 64 << 10
)

var (
	magic      = [6]byte{'F', 'W', 'W', 'A', 'L', 0}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)

	errShort   = errors.New("wal: record is incomplete")
	errCorrupt = errors.New("wal: record is corrupt")
)

func encodeHeader() []byte {
	b := make([]byte, headerSize)
	copy(b, magic[:])
	binary.LittleEndian.PutUint16(b[6:], formatVersion)
	return b
}

// checkHeader validates a segment header. A header from a newer agent is an
// error, not corruption: an older agent must refuse to touch it.
func checkHeader(b []byte) error {
	if len(b) < headerSize {
		return errShort
	}
	if [6]byte(b[:6]) != magic {
		return errCorrupt
	}
	if v := binary.LittleEndian.Uint16(b[6:]); v != formatVersion {
		return &VersionError{Version: v}
	}
	return nil
}

// VersionError reports a segment written in a format this build cannot read.
type VersionError struct{ Version uint16 }

func (e *VersionError) Error() string {
	return fmt.Sprintf("wal: segment format version %d is not supported (this agent reads version %d)", e.Version, formatVersion)
}

func recordCRC(seq uint64, payload []byte) uint32 {
	var s [8]byte
	binary.LittleEndian.PutUint64(s[:], seq)
	return crc32.Update(crc32.Checksum(s[:], castagnoli), castagnoli, payload)
}

// appendRecord appends the encoding of one record to dst.
func appendRecord(dst []byte, seq uint64, payload []byte) []byte {
	var h [recordHeaderSize]byte
	binary.LittleEndian.PutUint32(h[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(h[4:], recordCRC(seq, payload))
	binary.LittleEndian.PutUint64(h[8:], seq)
	dst = append(dst, h[:]...)
	return append(dst, payload...)
}

// decodeRecord parses the record at the start of b. It returns errShort when
// b ends inside the record and errCorrupt when the record cannot be valid.
// The returned payload aliases b.
func decodeRecord(b []byte) (seq uint64, payload []byte, size int, err error) {
	if len(b) < recordHeaderSize {
		return 0, nil, 0, errShort
	}
	n := binary.LittleEndian.Uint32(b[0:])
	if n > MaxPayload {
		return 0, nil, 0, errCorrupt
	}
	size = recordHeaderSize + int(n)
	if len(b) < size {
		return 0, nil, 0, errShort
	}
	seq = binary.LittleEndian.Uint64(b[8:])
	payload = b[recordHeaderSize:size]
	if binary.LittleEndian.Uint32(b[4:]) != recordCRC(seq, payload) {
		return 0, nil, 0, errCorrupt
	}
	return seq, payload, size, nil
}
