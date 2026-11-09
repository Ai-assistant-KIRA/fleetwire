// Package envelope seals stored readings with AES-256-GCM.
//
// Each chunk row is sealed with its tenant's data key and a random 96-bit
// nonce. The additional authenticated data binds the ciphertext to the row it
// belongs to (tenant, device, seq range, key ID), so copying a ciphertext to
// another row makes it fail to open instead of silently decrypting there.
package envelope

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

const (
	// KeySize is the data key length (AES-256).
	KeySize = 32
	// NonceSize is the GCM nonce length.
	NonceSize = 12

	chunkAADLabel = "fleetwire/chunk/v1"
)

// ErrOpen means the ciphertext, nonce, key or AAD do not match.
var ErrOpen = errors.New("envelope: cannot open sealed data")

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("envelope: key is %d bytes, want %d", len(key), KeySize)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	return aead, nil
}

// Seal encrypts plaintext under key with a fresh random nonce.
func Seal(key, aad, plaintext []byte) (nonce, ciphertext []byte, err error) {
	nonce = make([]byte, NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, fmt.Errorf("envelope: read nonce: %w", err)
	}
	ct, err := sealWithNonce(key, nonce, aad, plaintext)
	if err != nil {
		return nil, nil, err
	}
	return nonce, ct, nil
}

func sealWithNonce(key, nonce, aad, plaintext []byte) ([]byte, error) {
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	return aead.Seal(nil, nonce, plaintext, aad), nil
}

// Open decrypts and authenticates ciphertext.
func Open(key, aad, nonce, ciphertext []byte) ([]byte, error) {
	if len(nonce) != NonceSize {
		return nil, ErrOpen
	}
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	pt, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, ErrOpen
	}
	return pt, nil
}

// ChunkAAD builds the fixed-width AAD for one chunk row:
//
//	"fleetwire/chunk/v1" | tenant (16) | device (16) | seq_lo (8) | seq_hi (8) | key_id (16)
//
// Fixed widths mean no two different field sets can produce the same bytes.
func ChunkAAD(tenantID, deviceID uuid.UUID, seqLo, seqHi uint64, keyID uuid.UUID) []byte {
	b := make([]byte, 0, len(chunkAADLabel)+16+16+8+8+16)
	b = append(b, chunkAADLabel...)
	b = append(b, tenantID[:]...)
	b = append(b, deviceID[:]...)
	b = binary.BigEndian.AppendUint64(b, seqLo)
	b = binary.BigEndian.AppendUint64(b, seqHi)
	return append(b, keyID[:]...)
}

// NewKey returns a random data key.
func NewKey() ([]byte, error) {
	k := make([]byte, KeySize)
	if _, err := rand.Read(k); err != nil {
		return nil, fmt.Errorf("envelope: generate key: %w", err)
	}
	return k, nil
}
