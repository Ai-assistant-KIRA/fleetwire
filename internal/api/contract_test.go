package api_test

import (
	"bytes"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/google/uuid"

	openapi "github.com/Ai-assistant-KIRA/fleetwire/api"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
)

// contract validates every exchange the harness sees against
// api/openapi.yaml and records which operations were exercised.
type contract struct {
	router routers.Router
	doc    *openapi3.T
	mu     sync.Mutex
	seen   map[string]bool
}

func newContract(t *testing.T, h *apitest.Harness) *contract {
	t.Helper()
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(openapi.Spec)
	if err != nil {
		t.Fatalf("load openapi.yaml: %v", err)
	}
	if err := doc.Validate(loader.Context); err != nil {
		t.Fatalf("openapi.yaml is not valid: %v", err)
	}
	// Route against the test listeners instead of the documented hosts.
	doc.Servers = openapi3.Servers{{URL: h.Public.URL}, {URL: h.Device.URL}}
	for _, item := range doc.Paths.Map() {
		for _, op := range item.Operations() {
			op.Servers = nil
		}
	}
	router, err := gorillamux.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	openapi3filter.RegisterBodyDecoder("application/problem+json", openapi3filter.RegisteredBodyDecoder("application/json"))
	openapi3filter.RegisterBodyDecoder("application/gzip", openapi3filter.FileBodyDecoder)
	openapi3filter.RegisterBodyDecoder("application/x-pem-file", openapi3filter.FileBodyDecoder)
	return &contract{router: router, doc: doc, seen: map[string]bool{}}
}

func (c *contract) validate(t *testing.T, req *http.Request, resp *http.Response, body []byte) {
	t.Helper()
	// FindRoute matches on the URL; the request body was already sent.
	r := req.Clone(req.Context())
	route, params, err := c.router.FindRoute(r)
	if err != nil {
		// Unknown paths and methods must still answer with a problem.
		if resp.Header.Get("Content-Type") != "application/problem+json" {
			t.Errorf("%s %s: not in openapi.yaml and not a problem response (%v)", req.Method, req.URL.Path, err)
		}
		return
	}
	c.mu.Lock()
	c.seen[route.Operation.OperationID] = true
	c.mu.Unlock()
	// A request the server accepted must be one the spec accepts too.
	// Refused requests are not checked: the tests send many of them on
	// purpose, and some (a bad cursor, say) are valid by the schema.
	if resp.StatusCode < 400 {
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				t.Fatal(err)
			}
			r.Body = body
		}
		rin := &openapi3filter.RequestValidationInput{Request: r, PathParams: params, Route: route,
			Options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc, MultiError: true}}
		if err := openapi3filter.ValidateRequest(req.Context(), rin); err != nil {
			t.Errorf("%s %s -> %d: the request does not match openapi.yaml: %v", req.Method, req.URL.Path, resp.StatusCode, err)
		}
	}
	in := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: r, PathParams: params, Route: route},
		Status:                 resp.StatusCode,
		Header:                 resp.Header,
		Body:                   io.NopCloser(bytes.NewReader(body)),
		Options:                &openapi3filter.Options{IncludeResponseStatus: true, MultiError: true},
	}
	if err := openapi3filter.ValidateResponse(req.Context(), in); err != nil {
		t.Errorf("%s %s -> %d does not match openapi.yaml: %v\nbody: %.500s", req.Method, req.URL.Path, resp.StatusCode, err, body)
	}
}

func (c *contract) unexercised() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, item := range c.doc.Paths.Map() {
		for _, op := range item.Operations() {
			if !c.seen[op.OperationID] {
				out = append(out, op.OperationID)
			}
		}
	}
	sort.Strings(out)
	return out
}

// TestResponsesMatchOpenAPI calls every documented operation, success and
// error paths, and validates each response, and each accepted request,
// against the spec (R8.5).
func TestResponsesMatchOpenAPI(t *testing.T) {
	h := apitest.New(t)
	c := newContract(t, h)
	h.Validate = c.validate

	tenant, tok := h.CreateTenant("contract")
	resp, body := h.Do("GET", "/v1/admin/tenants?limit=1", h.OperatorToken, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, body = h.Do("POST", "/v1/admin/tenants/"+tenant.String()+"/enrollment-tokens", h.OperatorToken, map[string]any{"count": 2})
	h.MustJSON(resp, body, 201, nil)
	resp, body = h.Do("GET", "/v1/tenant", tok, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, body = h.Do("PATCH", "/v1/tenant", tok, map[string]any{"retention_days": 7})
	h.MustJSON(resp, body, 200, nil)

	var created struct {
		ID uuid.UUID `json:"id"`
	}
	resp, body = h.Do("POST", "/v1/tokens", tok, map[string]any{"name": "reader", "scopes": []string{"readings:read"}})
	h.MustJSON(resp, body, 201, &created)
	resp, body = h.Do("GET", "/v1/tokens?limit=1", tok, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, body = h.Do("GET", "/v1/tokens/"+created.ID.String(), tok, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, _ = h.Do("DELETE", "/v1/tokens/"+created.ID.String(), tok, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("revoke token: %d", resp.StatusCode)
	}

	devA := h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	devB := h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	storeReadings(t, h, tenant, devA.ID, 1, 25, func(uint64) time.Time { return time.Now() }, "cpu.utilization")
	resp, body = h.Do("GET", "/v1/devices?limit=1", tok, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, body = h.Do("GET", "/v1/devices/"+devA.ID.String(), tok, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, body = h.Do("GET", "/v1/devices/"+devA.ID.String()+"/readings?limit=10", tok, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, body = h.Do("GET", "/v1/devices/"+devA.ID.String()+"/gaps", tok, nil)
	h.MustJSON(resp, body, 200, nil)

	// Before the first publish the CRL endpoint answers with a problem, then
	// with the signed list.
	if resp, body := h.DoDevice(nil, "GET", "/v1/crl", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("CRL before publishing: %d %s", resp.StatusCode, body)
	}
	if err := h.CRL.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if resp, body := h.DoDevice(nil, "GET", "/v1/crl", nil); resp.StatusCode != http.StatusOK || !bytes.HasPrefix(body, []byte("-----BEGIN X509 CRL-----")) {
		t.Fatalf("CRL: %d %.80s", resp.StatusCode, body)
	}

	csr := renewCSR(t, devA)
	resp, body = h.DoDevice(&devA.TLS, "POST", "/v1/renew", map[string]any{"csr_pem": csr})
	h.MustJSON(resp, body, 200, nil)

	var exp struct {
		ID uuid.UUID `json:"id"`
	}
	resp, body = h.Do("POST", "/v1/exports", tok, map[string]any{"device_id": devA.ID})
	h.MustJSON(resp, body, 202, &exp)
	h.RunJobs()
	resp, body = h.Do("GET", "/v1/exports/"+exp.ID.String(), tok, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, _ = h.Do("GET", "/v1/exports/"+exp.ID.String()+"/download", tok, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download: %d", resp.StatusCode)
	}

	resp, body = h.Do("POST", "/v1/devices/"+devA.ID.String()+"/revoke", tok, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, body = h.Do("POST", "/v1/admin/devices/"+devB.ID.String()+"/revoke", h.OperatorToken, nil)
	h.MustJSON(resp, body, 200, nil)
	var acc struct {
		StatusURL string `json:"status_url"`
	}
	resp, body = h.Do("DELETE", "/v1/devices/"+devA.ID.String(), tok, nil)
	h.MustJSON(resp, body, 202, &acc)
	h.RunJobs()
	resp, body = h.Do("GET", acc.StatusURL, tok, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, body = h.Do("GET", "/v1/audit?limit=2", tok, nil)
	h.MustJSON(resp, body, 200, nil)
	resp, body = h.Do("GET", "/v1/admin/audit?limit=2", h.OperatorToken, nil)
	h.MustJSON(resp, body, 200, nil)

	// Error responses are part of the contract too.
	for _, e := range []struct {
		method, path, token string
		body                any
		status              int
	}{
		{"GET", "/v1/devices/" + uuid.NewString(), tok, nil, 404},
		{"GET", "/v1/devices/not-a-uuid", tok, nil, 400},
		{"GET", "/v1/devices", "", nil, 401},
		{"GET", "/v1/admin/tenants", tok, nil, 403},
		{"PATCH", "/v1/tenant", tok, map[string]any{"retention_days": 99999}, 400},
		{"POST", "/v1/tokens", tok, map[string]any{"name": "x", "scopes": []string{"root"}}, 400},
		{"GET", "/v1/devices/" + devA.ID.String() + "/readings?cursor=bogus", tok, nil, 400},
		{"POST", "/v1/enrollment-tokens", tok, map[string]any{"count": 0}, 400},
		{"POST", "/v1/tokens", tok, map[string]any{"name": "a\x00b", "scopes": []string{"audit:read"}}, 400},
	} {
		resp, body := h.Do(e.method, e.path, e.token, e.body)
		if resp.StatusCode != e.status {
			t.Errorf("%s %s: %d, want %d: %s", e.method, e.path, resp.StatusCode, e.status, body)
		}
	}
	resp, body = h.DoDevice(nil, "POST", "/v1/enroll", map[string]any{"token": "fwe_" + strings.Repeat("a", 52), "csr_pem": csr})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("enroll with unknown token: %d %s", resp.StatusCode, body)
	}

	resp, body = h.Do("DELETE", "/v1/admin/tenants/"+tenant.String(), h.OperatorToken, nil)
	h.MustJSON(resp, body, 202, nil)

	if missing := c.unexercised(); len(missing) > 0 {
		t.Errorf("operations in openapi.yaml that the contract test never called: %v", missing)
	}
}

func renewCSR(t *testing.T, d apitest.Device) string {
	t.Helper()
	return string(mustCSR(t, d))
}
