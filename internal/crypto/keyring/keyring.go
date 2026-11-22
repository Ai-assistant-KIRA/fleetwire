// Package keyring holds the per-tenant key-encryption keys (KEKs) that wrap
// tenant data keys.
//
// The KEKs live outside PostgreSQL on purpose. The database holds only
// wrapped data keys, so once a tenant's KEK is destroyed, every copy of that
// tenant's readings, including database backups taken earlier, is
// unreadable. A KMS or Vault backend can implement the same interface; only
// the local file backend is built.
package keyring

import (
	"bytes"
	"context"
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/envelope"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/fsutil"
)

// Keyring wraps and unwraps tenant data keys.
type Keyring interface {
	// Create makes a new KEK for the tenant. It fails with ErrExists if one
	// is already there.
	Create(ctx context.Context, tenantID uuid.UUID) error
	// Wrap encrypts dek under the tenant's KEK.
	Wrap(ctx context.Context, tenantID uuid.UUID, dek []byte) (wrapped []byte, kekID string, err error)
	// Unwrap reverses Wrap. It fails with ErrNotFound after Destroy.
	Unwrap(ctx context.Context, tenantID uuid.UUID, kekID string, wrapped []byte) ([]byte, error)
	// Destroy removes the tenant's KEK for good. It is idempotent.
	Destroy(ctx context.Context, tenantID uuid.UUID) error
}

// Errors returned by Keyring implementations.
var (
	ErrNotFound = errors.New("keyring: no key for tenant")
	ErrExists   = errors.New("keyring: key already exists for tenant")
	ErrWrongKEK = errors.New("keyring: wrapped key belongs to another KEK")
)

// RootKeySize is the length of the local backend's root key.
const RootKeySize = 32

// LoadRootKey reads the 32-byte root key. The file must not be readable by
// group or others, so a misconfigured mount fails loudly at start.
func LoadRootKey(path string) ([]byte, error) {
	key, err := fsutil.ReadPrivate(path)
	if err != nil {
		return nil, fmt.Errorf("keyring root key: %w", err)
	}
	if len(key) != RootKeySize {
		return nil, fmt.Errorf("keyring root key %s is %d bytes, want %d", path, len(key), RootKeySize)
	}
	return key, nil
}

// DeriveKey derives an independent 32-byte key from the root key for another
// purpose (for example signing API cursors), using HKDF-SHA256 with the
// purpose as info, so the root key itself is never used twice.
func DeriveKey(root []byte, purpose string) ([]byte, error) {
	k, err := hkdf.Key(sha256.New, root, nil, "fleetwire/"+purpose, 32)
	if err != nil {
		return nil, fmt.Errorf("derive %s key: %w", purpose, err)
	}
	return k, nil
}

// Local stores one sealed KEK file per tenant in a directory.
//
// File layout: "FWKEK1" | kek_id (16) | nonce (12) | AES-GCM(root, kek) (48).
// Deleting the file destroys the tenant's data. On SSDs a file delete is not
// a secure erase; production deployments should use a KMS backend.
type Local struct {
	dir  string
	root []byte
}

var _ Keyring = (*Local)(nil)

var kekMagic = []byte("FWKEK1")

// NewLocal opens a local keyring in dir, creating the directory (0700).
func NewLocal(dir string, rootKey []byte) (*Local, error) {
	if len(rootKey) != RootKeySize {
		return nil, fmt.Errorf("keyring: root key is %d bytes, want %d", len(rootKey), RootKeySize)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("keyring dir: %w", err)
	}
	return &Local{dir: dir, root: bytes.Clone(rootKey)}, nil
}

func (l *Local) path(tenantID uuid.UUID) string {
	return filepath.Join(l.dir, tenantID.String()+".kek")
}

func kekAAD(tenantID, kekID uuid.UUID) []byte {
	return append(append([]byte("fleetwire/kek/v1"), tenantID[:]...), kekID[:]...)
}

func dekAAD(tenantID, kekID uuid.UUID) []byte {
	return append(append([]byte("fleetwire/dek/v1"), tenantID[:]...), kekID[:]...)
}

// Create implements Keyring.
func (l *Local) Create(_ context.Context, tenantID uuid.UUID) error {
	kek, err := envelope.NewKey()
	if err != nil {
		return err
	}
	kekID := uuid.New()
	nonce, ct, err := envelope.Seal(l.root, kekAAD(tenantID, kekID), kek)
	if err != nil {
		return err
	}
	data := bytes.Join([][]byte{kekMagic, kekID[:], nonce, ct}, nil)

	final := l.path(tenantID)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("keyring create: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("keyring write: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("keyring fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("keyring close: %w", err)
	}
	// Link fails if the target exists, which makes Create atomic and
	// never overwrites a live KEK.
	if err := os.Link(tmp, final); err != nil {
		_ = os.Remove(tmp)
		if errors.Is(err, fs.ErrExist) {
			return ErrExists
		}
		return fmt.Errorf("keyring link: %w", err)
	}
	if err := os.Remove(tmp); err != nil {
		return fmt.Errorf("keyring cleanup: %w", err)
	}
	return syncDir(l.dir)
}

func (l *Local) load(tenantID uuid.UUID) (kek []byte, kekID uuid.UUID, err error) {
	data, err := os.ReadFile(l.path(tenantID)) //nolint:gosec // G703: the file name is a formatted UUID, which has no path separator
	if errors.Is(err, fs.ErrNotExist) {
		return nil, uuid.Nil, ErrNotFound
	}
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("keyring read: %w", err)
	}
	const want = 6 + 16 + envelope.NonceSize + envelope.KeySize + 16
	if len(data) != want || !bytes.Equal(data[:6], kekMagic) {
		return nil, uuid.Nil, fmt.Errorf("keyring: KEK file for tenant %s is malformed", tenantID)
	}
	copy(kekID[:], data[6:22])
	nonce := data[22 : 22+envelope.NonceSize]
	kek, err = envelope.Open(l.root, kekAAD(tenantID, kekID), nonce, data[22+envelope.NonceSize:])
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("keyring: cannot open KEK for tenant %s (wrong root key?): %w", tenantID, err)
	}
	return kek, kekID, nil
}

// Wrap implements Keyring. The result is nonce | ciphertext.
func (l *Local) Wrap(_ context.Context, tenantID uuid.UUID, dek []byte) ([]byte, string, error) {
	kek, kekID, err := l.load(tenantID)
	if err != nil {
		return nil, "", err
	}
	nonce, ct, err := envelope.Seal(kek, dekAAD(tenantID, kekID), dek)
	if err != nil {
		return nil, "", err
	}
	return append(nonce, ct...), kekID.String(), nil
}

// Unwrap implements Keyring.
func (l *Local) Unwrap(_ context.Context, tenantID uuid.UUID, kekID string, wrapped []byte) ([]byte, error) {
	kek, have, err := l.load(tenantID)
	if err != nil {
		return nil, err
	}
	if have.String() != kekID {
		return nil, ErrWrongKEK
	}
	if len(wrapped) < envelope.NonceSize {
		return nil, envelope.ErrOpen
	}
	return envelope.Open(kek, dekAAD(tenantID, have), wrapped[:envelope.NonceSize], wrapped[envelope.NonceSize:])
}

// Destroy implements Keyring.
func (l *Local) Destroy(_ context.Context, tenantID uuid.UUID) error {
	err := os.Remove(l.path(tenantID))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("keyring destroy: %w", err)
	}
	return syncDir(l.dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("keyring open dir: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("keyring fsync dir: %w", err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("keyring close dir: %w", err)
	}
	return nil
}
