package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

// cursors are opaque to clients: base64url(JSON) + "." + base64url(HMAC).
// The HMAC covers a scope string (endpoint, tenant, device), so a cursor
// cannot be forged, edited, or replayed against another list.
type cursors struct{ key []byte }

type cursorBody struct {
	V     int             `json:"v"`
	Scope string          `json:"s"`
	Data  json.RawMessage `json:"d"`
}

var errBadCursor = errors.New("invalid cursor")

func (c cursors) mac(b []byte) []byte {
	m := hmac.New(sha256.New, c.key)
	m.Write(b)
	return m.Sum(nil)
}

func (c cursors) encode(scope string, v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(cursorBody{V: 1, Scope: scope, Data: data})
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding
	return enc.EncodeToString(body) + "." + enc.EncodeToString(c.mac(body)), nil
}

func (c cursors) decode(scope, s string, v any) error {
	enc := base64.RawURLEncoding
	b64, sig, ok := strings.Cut(s, ".")
	if !ok {
		return errBadCursor
	}
	body, err := enc.DecodeString(b64)
	if err != nil {
		return errBadCursor
	}
	want, err := enc.DecodeString(sig)
	if err != nil || !hmac.Equal(want, c.mac(body)) {
		return errBadCursor
	}
	var cb cursorBody
	if err := json.Unmarshal(body, &cb); err != nil || cb.V != 1 || cb.Scope != scope {
		return errBadCursor
	}
	if err := json.Unmarshal(cb.Data, v); err != nil {
		return errBadCursor
	}
	return nil
}
