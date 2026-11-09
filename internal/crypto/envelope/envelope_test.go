package envelope

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestKnownAnswer uses the 256-bit vector with AAD from Go's own
// crypto/cipher tests, so a mistake in how this package drives GCM (key,
// nonce and AAD order) cannot pass unnoticed.
func TestKnownAnswer(t *testing.T) {
	key := unhex(t, "feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308")
	nonce := unhex(t, "e1934f5db57cc983e6b180e7")
	pt := unhex(t, "73ed042327f70fe9c572a61545eda8b2a0c6e1d6c291ef19248e973aee6c312012f490c2c6f6166f4a59431e182663fcaea05a")
	aad := unhex(t, "0a8a18a7150e940c3d87b38e73baee9a5c049ee21795663e264b694a949822b639092d0e67015e86363583fcf0ca645af9f43375f05fdb4ce84f411dcbca73c2220dea03a20115d2e51398344b16bee1ed7c499b353d6c597af8")
	want := unhex(t, "fc1ae2b5dcd2c4176c3f538b4c3cc21197f79e608cc3730167936382e4b1e5a7b75ae1678bcebd876705477eb0e0fdbbcda92fb9a0dc58c8d8f84fb590e0422e6077ef")
	got, err := sealWithNonce(key, nonce, aad, pt)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("seal = %x, %v", got, err)
	}
	back, err := Open(key, aad, nonce, want)
	if err != nil || !bytes.Equal(back, pt) {
		t.Fatalf("open = %x, %v", back, err)
	}
}

// TestTamperEveryField is P6: Open(Seal(x)) == x, and changing any AAD
// field, the nonce, the key or any ciphertext bit makes Open fail.
func TestTamperEveryField(t *testing.T) {
	key, err := NewKey()
	if err != nil {
		t.Fatal(err)
	}
	tenant, device, keyID := uuid.New(), uuid.New(), uuid.New()
	aad := ChunkAAD(tenant, device, 10, 20, keyID)
	pt := []byte("eleven readings worth of protobuf")
	nonce, ct, err := Seal(key, aad, pt)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Open(key, aad, nonce, ct); err != nil || !bytes.Equal(got, pt) {
		t.Fatalf("round trip = %q, %v", got, err)
	}

	other := uuid.New()
	for name, a := range map[string][]byte{
		"tenant": ChunkAAD(other, device, 10, 20, keyID),
		"device": ChunkAAD(tenant, other, 10, 20, keyID),
		"seq_lo": ChunkAAD(tenant, device, 11, 20, keyID),
		"seq_hi": ChunkAAD(tenant, device, 10, 21, keyID),
		"key_id": ChunkAAD(tenant, device, 10, 20, other),
	} {
		if _, err := Open(key, a, nonce, ct); !errors.Is(err, ErrOpen) {
			t.Errorf("changed %s: Open = %v, want ErrOpen", name, err)
		}
	}
	for i := range nonce {
		n := bytes.Clone(nonce)
		n[i] ^= 0x80
		if _, err := Open(key, aad, n, ct); !errors.Is(err, ErrOpen) {
			t.Errorf("nonce byte %d flipped: Open = %v", i, err)
		}
	}
	for bit := 0; bit < len(ct)*8; bit++ {
		c := bytes.Clone(ct)
		c[bit/8] ^= 1 << (bit % 8)
		if _, err := Open(key, aad, nonce, c); !errors.Is(err, ErrOpen) {
			t.Fatalf("ciphertext bit %d flipped: Open = %v", bit, err)
		}
	}
	wrong, _ := NewKey()
	if _, err := Open(wrong, aad, nonce, ct); !errors.Is(err, ErrOpen) {
		t.Errorf("wrong key: Open = %v", err)
	}
	if _, err := Open(key, aad, nonce[:8], ct); !errors.Is(err, ErrOpen) {
		t.Errorf("short nonce: Open = %v", err)
	}
}

func TestNoncesAreFresh(t *testing.T) {
	key, _ := NewKey()
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		n, _, err := Seal(key, nil, []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		if seen[string(n)] {
			t.Fatal("nonce repeated")
		}
		seen[string(n)] = true
	}
}

func TestChunkAADLayout(t *testing.T) {
	a := ChunkAAD(uuid.Nil, uuid.Nil, 1, 2, uuid.Nil)
	if len(a) != len(chunkAADLabel)+64 || !bytes.HasPrefix(a, []byte(chunkAADLabel)) {
		t.Fatalf("AAD = %x", a)
	}
}

func TestBadKeyLength(t *testing.T) {
	if _, _, err := Seal(make([]byte, 16), nil, nil); err == nil {
		t.Fatal("AES-128 key accepted")
	}
}
