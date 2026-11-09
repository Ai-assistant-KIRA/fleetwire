// Command fwctl is the command-line client for the fleetwire API.
//
//	fwctl [global flags] <group> <command> [flags] [args]
//
// Global flags (or env): -url (FWCTL_URL), -token (FWCTL_TOKEN),
// -ca-file (FWCTL_CA_FILE), -o table|json (FWCTL_OUTPUT).
//
// The token decides which API is used: an operator token (fwo_) reaches the
// fleet-wide operator endpoints, a tenant token (fwt_) the tenant's own.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
)

const usage = `fwctl: command-line client for the fleetwire API

Usage: fwctl [-url URL] [-token TOKEN] [-ca-file FILE] [-o table|json] <group> <command> [flags] [args]

Operator token (fwo_...):
  tenant create -name NAME [-retention-days N]
  tenant list
  tenant destroy ID -yes
  token create -tenant ID -name NAME -scopes a,b      first token of a tenant
  enroll-token create -tenant ID [-count N] [-ttl 15m]
  device revoke ID
  audit list [-limit N]                               fleet-wide audit log

Tenant token (fwt_...):
  tenant show
  tenant set-retention DAYS
  token create -name NAME -scopes a,b
  token list
  token revoke ID
  enroll-token create [-count N] [-ttl 15m]
  device list [-status active|revoked|deleted]
  device show ID
  device revoke ID
  device delete ID -yes
  device readings ID [-from RFC3339] [-to RFC3339] [-metric NAME] [-limit N]
  device gaps ID
  export create [-device ID]
  export status ID
  export download ID -out FILE
  audit list [-limit N]

Environment: FWCTL_URL, FWCTL_TOKEN, FWCTL_CA_FILE, FWCTL_OUTPUT.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "fwctl:", err)
		os.Exit(1)
	}
}

type cli struct {
	base   string
	token  string
	output string
	http   *http.Client
	out    io.Writer
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	gfs := flag.NewFlagSet("fwctl", flag.ContinueOnError)
	gfs.SetOutput(stderr)
	gfs.Usage = func() { fmt.Fprint(stderr, usage) }
	base := gfs.String("url", getenv("FWCTL_URL"), "API base URL, for example https://localhost:8443.")
	token := gfs.String("token", getenv("FWCTL_TOKEN"), "Operator or tenant token.")
	caFile := gfs.String("ca-file", getenv("FWCTL_CA_FILE"), "CA bundle that signed the API certificate (default: system roots).")
	output := gfs.String("o", getenv("FWCTL_OUTPUT"), "Output format: table or json.")
	if err := gfs.Parse(args); err != nil {
		return err
	}
	rest := gfs.Args()
	if len(rest) < 2 {
		gfs.Usage()
		return errors.New("missing group and command")
	}
	if *output == "" {
		*output = "table"
	}
	if *output != "table" && *output != "json" {
		return fmt.Errorf("-o must be table or json, not %q", *output)
	}
	if *base == "" || *token == "" {
		return errors.New("-url and -token (or FWCTL_URL and FWCTL_TOKEN) are required")
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if *caFile != "" {
		pem, err := os.ReadFile(*caFile)
		if err != nil {
			return fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return fmt.Errorf("%s holds no certificates", *caFile)
		}
		tlsCfg.RootCAs = pool
	}
	c := &cli{
		base:   strings.TrimSuffix(*base, "/"),
		token:  *token,
		output: *output,
		http:   &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{TLSClientConfig: tlsCfg}},
		out:    stdout,
	}
	group, cmd, cargs := rest[0], rest[1], rest[2:]
	fs := flag.NewFlagSet("fwctl "+group+" "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	h, ok := c.commands(fs)[group+" "+cmd]
	if !ok {
		gfs.Usage()
		return fmt.Errorf("unknown command %q", group+" "+cmd)
	}
	pos, err := parseInterleaved(fs, cargs)
	if err != nil {
		return err
	}
	return h(ctx, pos)
}

// parseInterleaved lets flags follow positional arguments
// ("device delete ID -yes"); the flag package alone stops at the first one.
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos, args = append(pos, args[0]), args[1:]
	}
}

func (c *cli) operator() bool { return strings.HasPrefix(c.token, "fwo_") }

type command func(ctx context.Context, args []string) error

func (c *cli) commands(fs *flag.FlagSet) map[string]command {
	name := fs.String("name", "", "Name.")
	retention := fs.Int("retention-days", 0, "Retention in days (1 to 3650).")
	yes := fs.Bool("yes", false, "Confirm a destructive action.")
	tenant := fs.String("tenant", "", "Tenant ID (operator token only).")
	scopes := fs.String("scopes", "", "Comma-separated scopes.")
	count := fs.Int("count", 1, "Number of enrollment tokens.")
	ttl := fs.Duration("ttl", 0, "Enrollment token lifetime (default: server default, 15m).")
	status := fs.String("status", "", "Device status filter.")
	from := fs.String("from", "", "Start of the time window (RFC 3339).")
	to := fs.String("to", "", "End of the time window (RFC 3339).")
	metric := fs.String("metric", "", "Metric name filter.")
	limit := fs.Int("limit", 0, "Maximum number of items (0: all).")
	device := fs.String("device", "", "Device ID (default: the whole tenant).")
	outFile := fs.String("out", "", "File to write.")

	need := func(args []string, what string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected %s", what)
		}
		return nil
	}
	return map[string]command{
		"tenant create": func(ctx context.Context, _ []string) error {
			body := map[string]any{"name": *name}
			if *retention != 0 {
				body["retention_days"] = *retention
			}
			return c.show(ctx, "POST", "/v1/admin/tenants", body, []string{"id", "name", "status", "retention_days", "created_at"})
		},
		"tenant list": func(ctx context.Context, _ []string) error {
			return c.list(ctx, "/v1/admin/tenants?limit=1000", *limit, []string{"id", "name", "status", "retention_days", "created_at"})
		},
		"tenant destroy": func(ctx context.Context, args []string) error {
			if err := need(args, "a tenant ID"); err != nil {
				return err
			}
			if !*yes {
				return errors.New("destroying a tenant deletes its keys and data for good; add -yes to confirm")
			}
			return c.show(ctx, "DELETE", "/v1/admin/tenants/"+url.PathEscape(args[0]), nil, []string{"job_id"})
		},
		"tenant show": func(ctx context.Context, _ []string) error {
			return c.show(ctx, "GET", "/v1/tenant", nil, []string{"id", "name", "status", "retention_days", "created_at"})
		},
		"tenant set-retention": func(ctx context.Context, args []string) error {
			if err := need(args, "the retention in days"); err != nil {
				return err
			}
			days, err := strconv.Atoi(args[0])
			if err != nil {
				return fmt.Errorf("retention must be a number of days: %w", err)
			}
			return c.show(ctx, "PATCH", "/v1/tenant", map[string]any{"retention_days": days}, []string{"id", "retention_days"})
		},
		"token create": func(ctx context.Context, _ []string) error {
			body := map[string]any{"name": *name, "scopes": splitList(*scopes)}
			path := "/v1/tokens"
			if c.operator() {
				if *tenant == "" {
					return errors.New("-tenant is required with an operator token")
				}
				path = "/v1/admin/tenants/" + url.PathEscape(*tenant) + "/tokens"
			}
			return c.show(ctx, "POST", path, body, []string{"id", "name", "scopes", "token"})
		},
		"token list": func(ctx context.Context, _ []string) error {
			return c.list(ctx, "/v1/tokens?limit=1000", *limit, []string{"id", "name", "scopes", "created_at", "last_used_at", "revoked_at"})
		},
		"token revoke": func(ctx context.Context, args []string) error {
			if err := need(args, "a token ID"); err != nil {
				return err
			}
			if _, err := c.do(ctx, "DELETE", "/v1/tokens/"+url.PathEscape(args[0]), nil); err != nil {
				return err
			}
			fmt.Fprintln(c.out, "revoked", args[0])
			return nil
		},
		"enroll-token create": func(ctx context.Context, _ []string) error {
			body := map[string]any{"count": *count}
			if *ttl != 0 {
				body["ttl_seconds"] = int(ttl.Seconds())
			}
			path := "/v1/enrollment-tokens"
			if c.operator() {
				if *tenant == "" {
					return errors.New("-tenant is required with an operator token")
				}
				path = "/v1/admin/tenants/" + url.PathEscape(*tenant) + "/enrollment-tokens"
			}
			data, err := c.do(ctx, "POST", path, body)
			if err != nil {
				return err
			}
			var resp struct {
				Tokens []json.RawMessage `json:"tokens"`
			}
			if err := json.Unmarshal(data, &resp); err != nil {
				return fmt.Errorf("decode response: %w", err)
			}
			return c.print(resp.Tokens, []string{"token", "expires_at"}, false)
		},
		"device list": func(ctx context.Context, _ []string) error {
			q := url.Values{"limit": {"1000"}}
			if *status != "" {
				q.Set("status", *status)
			}
			return c.list(ctx, "/v1/devices?"+q.Encode(), *limit, []string{"id", "name", "status", "acked_seq", "last_seen_at", "cert_not_after"})
		},
		"device show": func(ctx context.Context, args []string) error {
			if err := need(args, "a device ID"); err != nil {
				return err
			}
			return c.show(ctx, "GET", "/v1/devices/"+url.PathEscape(args[0]), nil,
				[]string{"id", "name", "status", "acked_seq", "last_seen_at", "cert_not_after", "gap_count", "gap_seqs"})
		},
		"device revoke": func(ctx context.Context, args []string) error {
			if err := need(args, "a device ID"); err != nil {
				return err
			}
			path := "/v1/devices/" + url.PathEscape(args[0]) + "/revoke"
			if c.operator() {
				path = "/v1/admin/devices/" + url.PathEscape(args[0]) + "/revoke"
			}
			return c.show(ctx, "POST", path, nil, []string{"id", "status", "revoked_at"})
		},
		"device delete": func(ctx context.Context, args []string) error {
			if err := need(args, "a device ID"); err != nil {
				return err
			}
			if !*yes {
				return errors.New("deleting a device revokes it and deletes all its readings; add -yes to confirm")
			}
			return c.show(ctx, "DELETE", "/v1/devices/"+url.PathEscape(args[0]), nil, []string{"job_id", "status_url"})
		},
		"device readings": func(ctx context.Context, args []string) error {
			if err := need(args, "a device ID"); err != nil {
				return err
			}
			q := url.Values{"limit": {"10000"}}
			for k, v := range map[string]string{"from": *from, "to": *to, "metric": *metric} {
				if v != "" {
					q.Set(k, v)
				}
			}
			return c.list(ctx, "/v1/devices/"+url.PathEscape(args[0])+"/readings?"+q.Encode(), *limit,
				[]string{"seq", "collected_at", "collector", "metric", "value", "labels"})
		},
		"device gaps": func(ctx context.Context, args []string) error {
			if err := need(args, "a device ID"); err != nil {
				return err
			}
			return c.list(ctx, "/v1/devices/"+url.PathEscape(args[0])+"/gaps?limit=1000", *limit,
				[]string{"from_seq", "to_seq", "reason", "reported_at"})
		},
		"export create": func(ctx context.Context, _ []string) error {
			var body map[string]any
			if *device != "" {
				body = map[string]any{"device_id": *device}
			}
			return c.show(ctx, "POST", "/v1/exports", body, []string{"id", "device_id", "status", "created_at"})
		},
		"export status": func(ctx context.Context, args []string) error {
			if err := need(args, "an export ID"); err != nil {
				return err
			}
			return c.show(ctx, "GET", "/v1/exports/"+url.PathEscape(args[0]), nil,
				[]string{"id", "status", "reading_count", "bytes", "sha256", "expires_at", "error"})
		},
		"export download": func(ctx context.Context, args []string) error {
			if err := need(args, "an export ID"); err != nil {
				return err
			}
			if *outFile == "" {
				return errors.New("-out is required")
			}
			return c.download(ctx, "/v1/exports/"+url.PathEscape(args[0])+"/download", *outFile)
		},
		"audit list": func(ctx context.Context, _ []string) error {
			path := "/v1/audit?limit=1000"
			if c.operator() {
				path = "/v1/admin/audit?limit=1000"
			}
			return c.list(ctx, path, *limit, []string{"id", "at", "actor_type", "actor_id", "action", "target_type", "target_id", "result"})
		},
	}
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// apiError is a problem-details response.
type apiError struct {
	Status   int      `json:"status"`
	Code     string   `json:"code"`
	Detail   string   `json:"detail"`
	Instance string   `json:"instance"`
	Errors   []string `json:"errors"`
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Detail)
	for _, d := range e.Errors {
		msg += "\n  - " + d
	}
	if e.Instance != "" {
		msg += " (" + e.Instance + ")"
	}
	return msg
}

func (c *cli) request(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode request: %w", err)
		}
		rd = strings.NewReader(string(b))
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var p apiError
		if json.Unmarshal(data, &p) != nil || p.Status == 0 {
			return nil, fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
		}
		return nil, &p
	}
	return resp, nil
}

func (c *cli) do(ctx context.Context, method, path string, body any) ([]byte, error) {
	resp, err := c.request(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	return data, nil
}

// show prints one object.
func (c *cli) show(ctx context.Context, method, path string, body any, cols []string) error {
	data, err := c.do(ctx, method, path, body)
	if err != nil {
		return err
	}
	return c.print([]json.RawMessage{data}, cols, true)
}

// list follows next_cursor until the end, or until limit items.
func (c *cli) list(ctx context.Context, path string, limit int, cols []string) error {
	var items []json.RawMessage
	next := path
	for next != "" {
		data, err := c.do(ctx, "GET", next, nil)
		if err != nil {
			return err
		}
		var page struct {
			Items      []json.RawMessage `json:"items"`
			NextCursor string            `json:"next_cursor"`
		}
		if err := json.Unmarshal(data, &page); err != nil {
			return fmt.Errorf("decode page: %w", err)
		}
		items = append(items, page.Items...)
		next = ""
		if page.NextCursor != "" && (limit == 0 || len(items) < limit) {
			u, err := url.Parse(path)
			if err != nil {
				return fmt.Errorf("parse path: %w", err)
			}
			q := u.Query()
			q.Set("cursor", page.NextCursor)
			u.RawQuery = q.Encode()
			next = u.String()
		}
	}
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return c.print(items, cols, false)
}

// print writes items as a table or, with -o json, as JSON: one object for
// single results, an array for lists.
func (c *cli) print(items []json.RawMessage, cols []string, single bool) error {
	if c.output == "json" {
		var v any = items
		if single && len(items) == 1 {
			v = items[0]
		}
		out, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return fmt.Errorf("encode output: %w", err)
		}
		_, err = fmt.Fprintln(c.out, string(out))
		return err
	}
	tw := tabwriter.NewWriter(c.out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, strings.ToUpper(strings.Join(cols, "\t")))
	for _, raw := range items {
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.UseNumber() // seqs are uint64; float64 would round them
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			return fmt.Errorf("decode item: %w", err)
		}
		vals := make([]string, len(cols))
		for i, col := range cols {
			vals[i] = cell(m[col])
		}
		fmt.Fprintln(tw, strings.Join(vals, "\t"))
	}
	return tw.Flush()
}

func cell(v any) string {
	switch x := v.(type) {
	case nil:
		return "-"
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	case []any:
		parts := make([]string, len(x))
		for i, p := range x {
			parts[i] = cell(p)
		}
		return strings.Join(parts, ",")
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = k + "=" + cell(x[k])
		}
		return strings.Join(parts, ",")
	}
	return fmt.Sprint(v)
}

// download writes the export to path and checks it against the
// Content-Digest header (RFC 9530) before keeping it.
func (c *cli) download(ctx context.Context, path, dest string) error {
	resp, err := c.request(ctx, "GET", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	want, ok := strings.CutPrefix(resp.Header.Get("Content-Digest"), "sha-256=:")
	want, ok2 := strings.CutSuffix(want, ":")
	if !ok || !ok2 {
		return errors.New("response has no sha-256 Content-Digest; refusing to trust the download")
	}
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".fwctl-download-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), resp.Body)
	if err != nil {
		_ = tmp.Close()
		return fmt.Errorf("download: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if got := base64.StdEncoding.EncodeToString(h.Sum(nil)); got != want {
		return fmt.Errorf("checksum mismatch: got sha-256 %s, server says %s", got, want)
	}
	if err := os.Rename(tmp.Name(), dest); err != nil {
		return fmt.Errorf("save download: %w", err)
	}
	fmt.Fprintf(c.out, "wrote %s (%d bytes, sha-256 verified)\n", dest, n)
	return nil
}
