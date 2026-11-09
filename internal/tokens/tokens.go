// Package tokens creates and parses fleetwire's bearer tokens.
//
//	fwe_<52 base32 chars>                 enrollment token (one use, 15 min)
//	fwt_<32 hex id>_<52 base32 chars>     tenant API token
//	fwo_<32 hex id>_<52 base32 chars>     operator token
//
// The secret part is 256 random bits, so a plain SHA-256 is enough to store
// it: there is nothing to brute-force. Lookup is by ID and the hash is
// compared in constant time. The fixed prefixes let secret scanners (GitHub,
// gitleaks) recognize a leaked token.
package tokens

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// Token prefixes.
const (
	PrefixEnrollment = "fwe_"
	PrefixTenant     = "fwt_"
	PrefixOperator   = "fwo_"
)

var b32 = base32.StdEncoding.WithPadding(base32.NoPadding)

// ErrMalformed means the string is not a well-formed fleetwire token.
var ErrMalformed = errors.New("tokens: malformed token")

// Secret is a token value. It never prints or logs itself; use Reveal to
// get the string, which should happen exactly once: when it is shown to the
// person who created it.
type Secret string

// String implements fmt.Stringer and hides the value.
func (Secret) String() string { return "<redacted>" }

// LogValue implements slog.LogValuer and hides the value.
func (Secret) LogValue() slog.Value { return slog.StringValue("<redacted>") }

// GoString hides the value from %#v.
func (Secret) GoString() string { return "<redacted>" }

// Reveal returns the token string.
func (s Secret) Reveal() string { return string(s) }

// Hash is the stored form of a token.
func Hash(token string) []byte {
	h := sha256.Sum256([]byte(token))
	return h[:]
}

// Equal compares two hashes in constant time.
func Equal(a, b []byte) bool { return subtle.ConstantTimeCompare(a, b) == 1 }

func randomPart() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("tokens: read random: %w", err)
	}
	return strings.ToLower(b32.EncodeToString(b)), nil
}

// NewEnrollment creates an enrollment token and its hash.
func NewEnrollment() (Secret, []byte, error) {
	r, err := randomPart()
	if err != nil {
		return "", nil, err
	}
	tok := PrefixEnrollment + r
	return Secret(tok), Hash(tok), nil
}

// NewKeyed creates a tenant or operator token with a lookup ID.
func NewKeyed(prefix string) (uuid.UUID, Secret, []byte, error) {
	if prefix != PrefixTenant && prefix != PrefixOperator {
		return uuid.Nil, "", nil, fmt.Errorf("tokens: unknown prefix %q", prefix)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, "", nil, fmt.Errorf("tokens: new id: %w", err)
	}
	r, err := randomPart()
	if err != nil {
		return uuid.Nil, "", nil, err
	}
	tok := prefix + hex.EncodeToString(id[:]) + "_" + r
	return id, Secret(tok), Hash(tok), nil
}

// Parsed is a token split into its parts.
type Parsed struct {
	Prefix string
	// ID is uuid.Nil for enrollment tokens.
	ID   uuid.UUID
	Hash []byte
}

func validSecret(s string) bool {
	b, err := b32.DecodeString(strings.ToUpper(s))
	return err == nil && len(b) == 32 && strings.ToLower(s) == s
}

// Parse checks the shape of a token and returns its parts.
func Parse(tok string) (Parsed, error) {
	switch {
	case strings.HasPrefix(tok, PrefixEnrollment):
		if !validSecret(tok[len(PrefixEnrollment):]) {
			return Parsed{}, ErrMalformed
		}
		return Parsed{Prefix: PrefixEnrollment, Hash: Hash(tok)}, nil
	case strings.HasPrefix(tok, PrefixTenant), strings.HasPrefix(tok, PrefixOperator):
		prefix, rest := tok[:4], tok[4:]
		idHex, secret, ok := strings.Cut(rest, "_")
		if !ok || len(idHex) != 32 || !validSecret(secret) {
			return Parsed{}, ErrMalformed
		}
		raw, err := hex.DecodeString(idHex)
		if err != nil {
			return Parsed{}, ErrMalformed
		}
		id, err := uuid.FromBytes(raw)
		if err != nil {
			return Parsed{}, ErrMalformed
		}
		return Parsed{Prefix: prefix, ID: id, Hash: Hash(tok)}, nil
	}
	return Parsed{}, ErrMalformed
}

// Tenant token scopes.
const (
	ScopeDevicesRead  = "devices:read"
	ScopeDevicesAdmin = "devices:admin"
	ScopeReadingsRead = "readings:read"
	ScopeExportsRead  = "exports:read"
	ScopeExportsWrite = "exports:write"
	ScopeAuditRead    = "audit:read"
	ScopeTenantAdmin  = "tenant:admin"
)

// AllScopes lists every tenant scope.
var AllScopes = []string{
	ScopeDevicesRead, ScopeDevicesAdmin, ScopeReadingsRead, ScopeExportsRead,
	ScopeExportsWrite, ScopeAuditRead, ScopeTenantAdmin,
}

// ValidateScopes rejects unknown or duplicate scopes.
func ValidateScopes(scopes []string) error {
	if len(scopes) == 0 {
		return errors.New("at least one scope is required")
	}
	seen := map[string]bool{}
	for _, s := range scopes {
		if !slices.Contains(AllScopes, s) {
			return fmt.Errorf("unknown scope %q", s)
		}
		if seen[s] {
			return fmt.Errorf("duplicate scope %q", s)
		}
		seen[s] = true
	}
	return nil
}
