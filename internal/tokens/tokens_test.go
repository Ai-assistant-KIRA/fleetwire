package tokens

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	for _, prefix := range []string{PrefixTenant, PrefixOperator} {
		id, secret, hash, err := NewKeyed(prefix)
		if err != nil {
			t.Fatal(err)
		}
		p, err := Parse(secret.Reveal())
		if err != nil || p.Prefix != prefix || p.ID != id || !Equal(p.Hash, hash) {
			t.Fatalf("Parse(%s...) = %+v, %v", prefix, p, err)
		}
	}
	secret, hash, err := NewEnrollment()
	if err != nil {
		t.Fatal(err)
	}
	p, err := Parse(secret.Reveal())
	if err != nil || p.Prefix != PrefixEnrollment || !Equal(p.Hash, hash) {
		t.Fatalf("Parse(enrollment) = %+v, %v", p, err)
	}
}

func TestMalformed(t *testing.T) {
	_, good, _, _ := NewKeyed(PrefixTenant)
	g := good.Reveal()
	for _, bad := range []string{
		"", "fwt_", "fwx_" + g[4:], g[:len(g)-1], g + "a", strings.ToUpper(g),
		"fwt_" + strings.Repeat("z", 32) + g[36:], "fwe_short", "Bearer " + g,
	} {
		if _, err := Parse(bad); !errors.Is(err, ErrMalformed) {
			t.Errorf("Parse(%q) = %v, want ErrMalformed", bad, err)
		}
	}
}

func TestSecretNeverPrints(t *testing.T) {
	_, s, _, _ := NewKeyed(PrefixOperator)
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("created", "token", s)
	for _, out := range []string{fmt.Sprint(s), fmt.Sprintf("%v %s %#v", s, s, s), buf.String()} {
		if strings.Contains(out, s.Reveal()) || !strings.Contains(out, "<redacted>") {
			t.Fatalf("token leaked or not redacted: %s", out)
		}
	}
}

func TestValidateScopes(t *testing.T) {
	if err := ValidateScopes([]string{ScopeReadingsRead, ScopeDevicesAdmin}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]string{nil, {"readings:write"}, {ScopeAuditRead, ScopeAuditRead}} {
		if err := ValidateScopes(bad); err == nil {
			t.Errorf("ValidateScopes(%v) accepted", bad)
		}
	}
}
