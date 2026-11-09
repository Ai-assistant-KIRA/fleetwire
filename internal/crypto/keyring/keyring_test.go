package keyring

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/envelope"
)

func newLocal(t *testing.T) (*Local, []byte) {
	t.Helper()
	root, err := envelope.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewLocal(t.TempDir(), root)
	if err != nil {
		t.Fatal(err)
	}
	return l, root
}

func TestWrapUnwrapDestroy(t *testing.T) {
	ctx := t.Context()
	l, _ := newLocal(t)
	tenant := uuid.New()
	if err := l.Create(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	if err := l.Create(ctx, tenant); !errors.Is(err, ErrExists) {
		t.Fatalf("second Create = %v, want ErrExists", err)
	}
	dek, _ := envelope.NewKey()
	wrapped, kekID, err := l.Wrap(ctx, tenant, dek)
	if err != nil {
		t.Fatal(err)
	}
	got, err := l.Unwrap(ctx, tenant, kekID, wrapped)
	if err != nil || !bytes.Equal(got, dek) {
		t.Fatalf("Unwrap = %x, %v", got, err)
	}
	// A wrapped key from one tenant does not open under another.
	other := uuid.New()
	if err := l.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Unwrap(ctx, other, kekID, wrapped); err == nil {
		t.Fatal("wrapped key opened under another tenant")
	}
	tampered := bytes.Clone(wrapped)
	tampered[len(tampered)-1] ^= 1
	if _, err := l.Unwrap(ctx, tenant, kekID, tampered); !errors.Is(err, envelope.ErrOpen) {
		t.Fatalf("tampered Unwrap = %v", err)
	}
	if _, err := l.Unwrap(ctx, tenant, uuid.NewString(), wrapped); !errors.Is(err, ErrWrongKEK) {
		t.Fatalf("wrong kek id Unwrap = %v", err)
	}

	if err := l.Destroy(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Unwrap(ctx, tenant, kekID, wrapped); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Unwrap after Destroy = %v, want ErrNotFound", err)
	}
	if err := l.Destroy(ctx, tenant); err != nil {
		t.Fatalf("Destroy is not idempotent: %v", err)
	}
	if _, err := os.Stat(l.path(tenant)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("KEK file still present: %v", err)
	}
}

func TestWrongRootKeyFails(t *testing.T) {
	ctx := t.Context()
	l, _ := newLocal(t)
	tenant := uuid.New()
	if err := l.Create(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	dek, _ := envelope.NewKey()
	wrapped, kekID, err := l.Wrap(ctx, tenant, dek)
	if err != nil {
		t.Fatal(err)
	}
	otherRoot, _ := envelope.NewKey()
	l2, err := NewLocal(l.dir, otherRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l2.Unwrap(ctx, tenant, kekID, wrapped); err == nil {
		t.Fatal("unwrap succeeded with the wrong root key")
	}
}

func TestLoadRootKeyChecks(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "root.key")
	if err := os.WriteFile(good, bytes.Repeat([]byte{7}, RootKeySize), 0o400); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRootKey(good); err != nil {
		t.Fatalf("LoadRootKey(0400) = %v", err)
	}
	open := filepath.Join(dir, "open.key")
	if err := os.WriteFile(open, bytes.Repeat([]byte{7}, RootKeySize), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRootKey(open); err == nil {
		t.Fatal("world-readable root key accepted")
	}
	short := filepath.Join(dir, "short.key")
	if err := os.WriteFile(short, []byte("tiny"), 0o400); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRootKey(short); err == nil {
		t.Fatal("short root key accepted")
	}
}

func TestDeriveKeyIsStableAndSeparated(t *testing.T) {
	root := bytes.Repeat([]byte{1}, RootKeySize)
	a1, _ := DeriveKey(root, "cursor")
	a2, _ := DeriveKey(root, "cursor")
	b, _ := DeriveKey(root, "other")
	if !bytes.Equal(a1, a2) || bytes.Equal(a1, b) || bytes.Equal(a1, root) || len(a1) != 32 {
		t.Fatal("DeriveKey must be deterministic, purpose-separated and differ from the root")
	}
}
