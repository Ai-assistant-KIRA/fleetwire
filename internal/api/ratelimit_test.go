package api_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/pki"
)

// counter reads one series of a counter from the harness registry.
func counter(t *testing.T, h *apitest.Harness, name, label, value string) float64 {
	t.Helper()
	families, err := h.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// Well-formed made-up tokens used to reach PostgreSQL on every request,
// with no limit at all. A client network whose lookups keep failing is now
// refused before the lookup.
func TestFailedLookupsAreLimitedPerNetwork(t *testing.T) {
	h := apitest.New(t, func(c *api.Config) { c.AuthFailPerSecond, c.AuthFailBurst = 0.001, 3 })
	_, tok := h.CreateTenant("guarded")
	fake := "fwt_00000000000000000000000000000000_" + strings.Repeat("a", 52)
	for i := 0; i < 3; i++ {
		resp, body := h.Do("GET", "/v1/tenant", fake, nil)
		problem(t, resp, body, http.StatusUnauthorized, "unauthorized")
	}
	resp, body := h.Do("GET", "/v1/tenant", fake, nil)
	problem(t, resp, body, http.StatusTooManyRequests, "rate_limited")
	if ra := resp.Header.Get("Retry-After"); ra == "" || ra == "0" {
		t.Fatalf("Retry-After = %q", ra)
	}
	// The check runs before any lookup, so until the budget refills it
	// holds for the whole network, valid tokens included.
	resp, body = h.Do("GET", "/v1/tenant", tok, nil)
	problem(t, resp, body, http.StatusTooManyRequests, "rate_limited")
}

// A token's rate limit applies before its scopes are checked, so it cannot
// make forbidden calls faster than allowed ones.
func TestRateLimitComesBeforeScopeCheck(t *testing.T) {
	h := apitest.New(t, func(c *api.Config) { c.RatePerSecond, c.RateBurst = 0.001, 2 })
	_, admin := h.CreateTenant("probe")
	var narrow struct {
		Token string `json:"token"`
	}
	resp, body := h.Do("POST", "/v1/tokens", admin, map[string]any{"name": "reader", "scopes": []string{"readings:read"}})
	h.MustJSON(resp, body, http.StatusCreated, &narrow)
	for i := 0; i < 2; i++ {
		resp, body := h.Do("GET", "/v1/devices", narrow.Token, nil)
		problem(t, resp, body, http.StatusForbidden, "forbidden")
	}
	resp, body = h.Do("GET", "/v1/devices", narrow.Token, nil)
	problem(t, resp, body, http.StatusTooManyRequests, "rate_limited")
}

// A wrong secret for a real token ID must not use up that token's budget.
func TestFailedLookupsDoNotSpendTheTokensBudget(t *testing.T) {
	h := apitest.New(t, func(c *api.Config) { c.RatePerSecond, c.RateBurst = 0.001, 3 })
	_, tok := h.CreateTenant("victim")
	id, _, ok := strings.Cut(strings.TrimPrefix(tok, "fwt_"), "_")
	if !ok {
		t.Fatalf("unexpected token format %q", tok)
	}
	wrong := "fwt_" + id + "_" + strings.Repeat("a", 52)
	for i := 0; i < 5; i++ {
		resp, body := h.Do("GET", "/v1/tenant", wrong, nil)
		problem(t, resp, body, http.StatusUnauthorized, "unauthorized")
	}
	resp, body := h.Do("GET", "/v1/tenant", tok, nil)
	h.MustJSON(resp, body, http.StatusOK, nil)
}

func TestCRLIsRateLimited(t *testing.T) {
	h := apitest.New(t, func(c *api.Config) { c.EnrollPerSecond, c.EnrollBurst = 0.001, 2 })
	if err := h.CRL.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if resp, body := h.DoDevice(nil, "GET", "/v1/crl", nil); resp.StatusCode != http.StatusOK {
			t.Fatalf("CRL request %d: %d %s", i, resp.StatusCode, body)
		}
	}
	resp, body := h.DoDevice(nil, "GET", "/v1/crl", nil)
	problem(t, resp, body, http.StatusTooManyRequests, "rate_limited")
}

// Refusals are audited (R4.4) at a bounded rate: once per window for a
// network sending tokens that match nothing, and once per window for each
// known token. The metric counts every refusal.
func TestEnrollmentRefusalsAreAuditedOncePerWindow(t *testing.T) {
	h := apitest.New(t)
	_, tok := h.CreateTenant("refusals")
	used := h.EnrollmentTokens(tok, 1)[0]
	h.Enroll(used)
	key, err := pki.NewDeviceKey()
	if err != nil {
		t.Fatal(err)
	}
	csr, err := pki.NewCSR(key, "x")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < 3; i++ {
		unknown := "fwe_" + strings.Repeat(string(rune('a'+i)), 52)
		for _, token := range []string{unknown, "not-a-token", used} {
			resp, body := h.DoDevice(nil, "POST", "/v1/enroll", map[string]any{"token": token, "csr_pem": string(csr)})
			problem(t, resp, body, http.StatusUnauthorized, "invalid_token")
		}
	}
	var audit struct {
		Items []struct {
			TenantID *uuid.UUID `json:"tenant_id"`
			At       time.Time  `json:"at"`
			Action   string     `json:"action"`
			Result   string     `json:"result"`
		} `json:"items"`
	}
	resp, body := h.Do("GET", "/v1/admin/audit?limit=1000", h.OperatorToken, nil)
	h.MustJSON(resp, body, http.StatusOK, &audit)
	var fleetWide, tenant int
	for _, a := range audit.Items {
		if a.Action != "device.enroll" || a.Result != "denied" || a.At.Before(start) {
			continue
		}
		if a.TenantID == nil {
			fleetWide++
		} else {
			tenant++
		}
	}
	if fleetWide != 1 || tenant != 1 {
		t.Fatalf("audit entries for 9 refusals: %d fleet-wide, %d for the tenant; want 1 and 1", fleetWide, tenant)
	}
	if got := counter(t, h, "fleetwire_api_enroll_refusals_total", "outcome", "unknown"); got != 6 {
		t.Errorf("unknown refusals counted %v, want 6", got)
	}
	if got := counter(t, h, "fleetwire_api_enroll_refusals_total", "outcome", "used"); got != 3 {
		t.Errorf("used refusals counted %v, want 3", got)
	}
}
