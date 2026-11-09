// Package chunks seals readings into chunk rows and opens them again.
//
// Ingest, the API, exports and the checker all use this one implementation,
// so a chunk is checked the same way everywhere: the AAD must match the row,
// and the readings inside must be exactly the row's seq range, in order.
package chunks

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/envelope"
	fleetwirev1 "github.com/Ai-assistant-KIRA/fleetwire/internal/gen/fleetwirev1"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
)

// ErrMismatch means a chunk decrypted but its content does not match its row.
var ErrMismatch = errors.New("chunks: chunk content does not match its row")

// Seal encrypts readings, which must be consecutive seqs, into a chunk row.
func Seal(dek []byte, tenant, device, keyID uuid.UUID, readings []*fleetwirev1.StoredReading) (store.Chunk, error) {
	if len(readings) == 0 {
		return store.Chunk{}, errors.New("chunks: no readings")
	}
	lo := readings[0].GetSeq()
	minT, maxT := readings[0].GetCollectedUnixNano(), readings[0].GetCollectedUnixNano()
	for i, r := range readings {
		if r.GetSeq() != lo+uint64(i) {
			return store.Chunk{}, fmt.Errorf("chunks: seq %d at position %d, want %d", r.GetSeq(), i, lo+uint64(i))
		}
		minT, maxT = min(minT, r.GetCollectedUnixNano()), max(maxT, r.GetCollectedUnixNano())
	}
	hi := lo + uint64(len(readings)) - 1
	plain, err := proto.MarshalOptions{Deterministic: true}.Marshal(&fleetwirev1.ChunkPayload{Readings: readings})
	if err != nil {
		return store.Chunk{}, fmt.Errorf("chunks: encode payload: %w", err)
	}
	nonce, ct, err := envelope.Seal(dek, envelope.ChunkAAD(tenant, device, lo, hi, keyID), plain)
	if err != nil {
		return store.Chunk{}, err
	}
	return store.Chunk{
		TenantID: tenant, DeviceID: device, SeqLo: lo, SeqHi: hi, ReadingCount: len(readings),
		MinCollectedAt: unixNano(minT), MaxCollectedAt: unixNano(maxT),
		KeyID: keyID, Nonce: nonce, Ciphertext: ct,
	}, nil
}

func unixNano(n int64) time.Time { return time.Unix(0, n).UTC() }

// Open decrypts a chunk row and checks that it holds exactly seqs
// SeqLo..SeqHi.
func Open(dek []byte, c store.Chunk) ([]*fleetwirev1.StoredReading, error) {
	plain, err := envelope.Open(dek, envelope.ChunkAAD(c.TenantID, c.DeviceID, c.SeqLo, c.SeqHi, c.KeyID), c.Nonce, c.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("chunk %s/%d-%d: %w", c.DeviceID, c.SeqLo, c.SeqHi, err)
	}
	var p fleetwirev1.ChunkPayload
	if err := proto.Unmarshal(plain, &p); err != nil {
		return nil, fmt.Errorf("chunk %s/%d-%d: decode: %w", c.DeviceID, c.SeqLo, c.SeqHi, err)
	}
	rs := p.GetReadings()
	if uint64(len(rs)) != c.SeqHi-c.SeqLo+1 {
		return nil, fmt.Errorf("%w: %d readings for range %d-%d", ErrMismatch, len(rs), c.SeqLo, c.SeqHi)
	}
	for i, r := range rs {
		if r.GetSeq() != c.SeqLo+uint64(i) {
			return nil, fmt.Errorf("%w: seq %d at position %d", ErrMismatch, r.GetSeq(), i)
		}
	}
	return rs, nil
}
