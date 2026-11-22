package api_test

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
)

// postChunkedEmpty sends a POST whose empty body is chunked, so the server
// sees ContentLength -1. The Go client never sends that for an empty body,
// so the request is written by hand.
func postChunkedEmpty(t *testing.T, h *apitest.Harness, path, token string) int {
	t.Helper()
	u, err := url.Parse(h.Public.URL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tls.Dial("tcp", u.Host, &tls.Config{RootCAs: h.Roots, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\n"+
		"Content-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n0\r\n\r\n", path, u.Host, token)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Logf("POST %s: %d %s", path, resp.StatusCode, body)
	}
	return resp.StatusCode
}

// Bodies the spec marks optional may also be empty and chunked, as HTTP/2
// and streaming clients send them.
func TestEmptyChunkedOptionalBodies(t *testing.T) {
	h := apitest.New(t)
	_, tok := h.CreateTenant("chunked")
	if got := postChunkedEmpty(t, h, "/v1/exports", tok); got != http.StatusAccepted {
		t.Errorf("POST /v1/exports with an empty chunked body: %d, want 202", got)
	}
	if got := postChunkedEmpty(t, h, "/v1/enrollment-tokens", tok); got != http.StatusCreated {
		t.Errorf("POST /v1/enrollment-tokens with an empty chunked body: %d, want 201", got)
	}
}

// A field left out takes its default; an explicit 0 is out of range.
func TestEnrollmentTokenZeroCountIsRefused(t *testing.T) {
	h := apitest.New(t)
	_, tok := h.CreateTenant("zero-count")
	for _, body := range []map[string]any{{"count": 0}, {"ttl_seconds": 0}, {"count": -1}} {
		resp, b := h.Do("POST", "/v1/enrollment-tokens", tok, body)
		problem(t, resp, b, http.StatusBadRequest, "invalid_request")
	}
	if got := h.EnrollmentTokens(tok, 1); len(got) != 1 {
		t.Fatalf("got %d tokens", len(got))
	}
	var out struct {
		Tokens []any `json:"tokens"`
	}
	resp, b := h.Do("POST", "/v1/enrollment-tokens", tok, map[string]any{})
	h.MustJSON(resp, b, http.StatusCreated, &out)
	if len(out.Tokens) != 1 {
		t.Fatalf("empty object gave %d tokens, want 1", len(out.Tokens))
	}
}

// NUL used to reach PostgreSQL, which cannot store it, and come back as a
// 500. Every control character is now refused up front.
func TestNamesRefuseControlCharacters(t *testing.T) {
	h := apitest.New(t)
	tenant, tok := h.CreateTenant("names")
	for _, name := range []string{"a\x00b", "line\nbreak", "\ttab", "bell\a", "c1\u0085"} {
		resp, body := h.Do("POST", "/v1/admin/tenants", h.OperatorToken, map[string]any{"name": name})
		problem(t, resp, body, http.StatusBadRequest, "invalid_request")
		resp, body = h.Do("POST", "/v1/tokens", tok, map[string]any{"name": name, "scopes": []string{"audit:read"}})
		problem(t, resp, body, http.StatusBadRequest, "invalid_request")
		resp, body = h.Do("POST", "/v1/admin/tenants/"+tenant.String()+"/tokens", h.OperatorToken,
			map[string]any{"name": name, "scopes": []string{"audit:read"}})
		problem(t, resp, body, http.StatusBadRequest, "invalid_request")
		key, err := pki.NewDeviceKey()
		if err != nil {
			t.Fatal(err)
		}
		csr, err := pki.NewCSR(key, "x")
		if err != nil {
			t.Fatal(err)
		}
		resp, body = h.DoDevice(nil, "POST", "/v1/enroll", map[string]any{"token": h.EnrollmentTokens(tok, 1)[0],
			"csr_pem": string(csr), "name": name})
		problem(t, resp, body, http.StatusBadRequest, "invalid_request")
	}
	// Ordinary Unicode names still work.
	resp, body := h.Do("POST", "/v1/admin/tenants", h.OperatorToken, map[string]any{"name": " Société Générale ✓ "})
	h.MustJSON(resp, body, http.StatusCreated, nil)
}
