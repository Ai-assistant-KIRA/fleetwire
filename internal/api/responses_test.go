package api_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/api/apitest"
)

func TestResponsesAreNotSniffedOrStored(t *testing.T) {
	h := apitest.New(t)
	_, tok := h.CreateTenant("headers")
	for _, c := range []struct{ path, token string }{
		{"/v1/tenant", tok},
		{"/v1/tenant", ""},
		{"/v1/no-such-route", tok},
	} {
		resp, body := h.Do("GET", c.path, c.token, nil)
		if resp.Header.Get("X-Content-Type-Options") != "nosniff" || resp.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("GET %s (%d %s): X-Content-Type-Options %q, Cache-Control %q", c.path, resp.StatusCode, body,
				resp.Header.Get("X-Content-Type-Options"), resp.Header.Get("Cache-Control"))
		}
	}
	// The CRL is public and may be cached, as long as it is revalidated.
	if err := h.CRL.Publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	resp, _ := h.DoDevice(nil, "GET", "/v1/crl", nil)
	if resp.Header.Get("Cache-Control") != "no-cache" || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("CRL headers %v", resp.Header)
	}
}

// A page that is exactly full used to carry a cursor to an empty page.
func TestListsEndWithoutAnEmptyPage(t *testing.T) {
	h := apitest.New(t)
	_, tok := h.CreateTenant("paging")
	tokenWith(t, h, tok, "audit:read")
	h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	h.Enroll(h.EnrollmentTokens(tok, 1)[0])
	for _, c := range []struct {
		path  string
		pages int
	}{
		{"/v1/tokens?limit=2", 1},
		{"/v1/tokens?limit=1", 2},
		{"/v1/devices?limit=2", 1},
		{"/v1/devices?limit=1", 2},
	} {
		pages := 0
		for path := c.path; path != ""; pages++ {
			var pg struct {
				Items      []any  `json:"items"`
				NextCursor string `json:"next_cursor"`
			}
			resp, body := h.Do("GET", path, tok, nil)
			h.MustJSON(resp, body, http.StatusOK, &pg)
			if len(pg.Items) == 0 {
				t.Errorf("%s: page %d is empty", c.path, pages+1)
			}
			path = ""
			if pg.NextCursor != "" {
				path = c.path + "&cursor=" + pg.NextCursor
			}
		}
		if pages != c.pages {
			t.Errorf("%s: %d pages, want %d", c.path, pages, c.pages)
		}
	}
}

// GET routes answer HEAD too. The download used to read the whole file
// for a response without a body, and the access log counted its bytes.
func TestExportHeadSendsHeadersOnly(t *testing.T) {
	h := apitest.New(t)
	tok, id, _ := readyExport(t, h, "head")
	req, err := http.NewRequestWithContext(t.Context(), http.MethodHead, h.Public.URL+"/v1/exports/"+id.String()+"/download", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := h.Client(nil).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") == "" || resp.Header.Get("Content-Digest") == "" {
		t.Fatalf("HEAD download: %d %v", resp.StatusCode, resp.Header)
	}
	var line string
	for _, l := range strings.Split(h.Logs(), "\n") {
		if strings.Contains(l, "method=HEAD") {
			line = l
		}
	}
	if !strings.Contains(line, " bytes=0 ") {
		t.Fatalf("access log for HEAD: %q, want bytes=0", line)
	}
}
