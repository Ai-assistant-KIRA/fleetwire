// Package config is the agent's configuration: one set of keys, read from
// `snapctl get -d` inside the snap, or from a JSON file and -set flags
// outside it, and checked by one validation path in both cases.
//
// Keys use the snap's dotted names (collect.interval, wal.max-size). In JSON
// they nest: {"wal": {"max-size": "256MiB"}}.
package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/tokens"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/uplinkpb"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

// Config is every agent setting.
type Config struct {
	EnrollURL       string
	EnrollToken     string
	EnrollCABundle  string
	EnrollName      string
	BrokerURL       string
	CollectInterval time.Duration
	WALSync         wal.SyncMode
	WALSyncInterval time.Duration
	WALMaxSize      int64
	WALSegmentSize  int64
	BatchSize       int
	BatchWait       time.Duration
	MaxInflight     int
	AckTimeout      time.Duration
	MetricsListen   string
	LogLevel        string
}

type key struct {
	name string
	def  string
	help string
	set  func(c *Config, v any) error
}

var keys = []key{
	{"enroll.url", "", "Device API URL used to enroll and renew, for example https://api.example:8444.",
		func(c *Config, v any) (err error) { c.EnrollURL, err = asString(v); return }},
	{"enroll.token", "", "One-time enrollment token (fwe_...). Used only until the device is enrolled.",
		func(c *Config, v any) (err error) { c.EnrollToken, err = asString(v); return }},
	{"enroll.ca-bundle", "", "PEM CA bundle that signed the API certificate. Empty means the system roots.",
		func(c *Config, v any) (err error) { c.EnrollCABundle, err = asString(v); return }},
	{"enroll.name", "", "Free-text device name shown in the API.",
		func(c *Config, v any) (err error) { c.EnrollName, err = asString(v); return }},
	{"broker.url", "", "Overrides the broker URL received at enrollment, for example tls://10.0.2.2:18883.",
		func(c *Config, v any) (err error) { c.BrokerURL, err = asString(v); return }},
	{"collect.interval", "10s", "Time between collections.",
		func(c *Config, v any) (err error) { c.CollectInterval, err = asDuration(v); return }},
	{"wal.sync", "interval", "When readings become durable: always, interval or never (see the WAL explanation for what each can lose).",
		func(c *Config, v any) error {
			s, err := asString(v)
			if err == nil {
				c.WALSync, err = wal.ParseSyncMode(s)
			}
			return err
		}},
	{"wal.sync-interval", "1s", "fsync period in interval mode.",
		func(c *Config, v any) (err error) { c.WALSyncInterval, err = asDuration(v); return }},
	{"wal.max-size", "256MiB", "Disk budget of the local log. Above it the oldest segment is dropped and reported as a gap.",
		func(c *Config, v any) (err error) { c.WALMaxSize, err = asSize(v); return }},
	{"wal.segment-size", "4MiB", "Size of one log segment file.",
		func(c *Config, v any) (err error) { c.WALSegmentSize, err = asSize(v); return }},
	{"uplink.batch-size", "100", "Maximum readings per batch.",
		func(c *Config, v any) (err error) { c.BatchSize, err = asInt(v); return }},
	{"uplink.batch-wait", "1s", "How long a durable reading may wait for a fuller batch.",
		func(c *Config, v any) (err error) { c.BatchWait, err = asDuration(v); return }},
	{"uplink.max-inflight", "1000", "Maximum readings sent but not acknowledged end to end.",
		func(c *Config, v any) (err error) { c.MaxInflight, err = asInt(v); return }},
	{"uplink.ack-timeout", "30s", "Shortest wait for an end-to-end ack before everything unacknowledged is resent. The wait follows the measured ack delay, as in TCP, and doubles after each timeout, up to 8x.",
		func(c *Config, v any) (err error) { c.AckTimeout, err = asDuration(v); return }},
	{"metrics.listen", "127.0.0.1:9465", "Address of the metrics and status endpoint. Empty disables it.",
		func(c *Config, v any) (err error) { c.MetricsListen, err = asString(v); return }},
	{"log.level", "info", "debug, info, warn or error.",
		func(c *Config, v any) (err error) { c.LogLevel, err = asString(v); return }},
}

// SnapGroups are the top-level snap config keys the agent reads.
var SnapGroups = []string{"enroll", "broker", "collect", "wal", "uplink", "metrics", "log"}

// Defaults returns the configuration with every default applied.
func Defaults() Config {
	var c Config
	for _, k := range keys {
		if k.def != "" {
			if err := k.set(&c, k.def); err != nil {
				panic(fmt.Sprintf("bad default for %s: %v", k.name, err))
			}
		}
	}
	return c
}

// Errors lists every problem found.
type Errors []error

func (e Errors) Error() string {
	msgs := make([]string, len(e))
	for i, err := range e {
		msgs[i] = "  - " + err.Error()
	}
	return fmt.Sprintf("%d configuration problem(s):\n%s", len(e), strings.Join(msgs, "\n"))
}

// Apply sets values from a flat map of dotted keys. Unknown keys are errors,
// so a typo never silently falls back to a default.
func (c *Config) Apply(values map[string]any) Errors {
	var errs Errors
	names := make([]string, 0, len(values))
	for n := range values {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		k, ok := lookup(n)
		if !ok {
			errs = append(errs, fmt.Errorf("%s: unknown key", n))
			continue
		}
		if err := k.set(c, values[n]); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", n, err))
		}
	}
	return errs
}

func lookup(name string) (key, bool) {
	for _, k := range keys {
		if k.name == name {
			return k, true
		}
	}
	return key{}, false
}

// Flatten turns nested JSON into dotted keys. Top-level groups that are
// empty strings are skipped: that is how `snapctl get -d` reports an unset
// key.
func Flatten(doc map[string]any) map[string]any {
	out := map[string]any{}
	var walk func(prefix string, v any, top bool)
	walk = func(prefix string, v any, top bool) {
		switch x := v.(type) {
		case map[string]any:
			for k, vv := range x {
				name := k
				if prefix != "" {
					name = prefix + "." + k
				}
				walk(name, vv, false)
			}
		case nil:
		case string:
			if top && x == "" {
				return
			}
			out[prefix] = x
		default:
			out[prefix] = x
		}
	}
	for k, v := range doc {
		walk(k, v, true)
	}
	return out
}

func decodeDoc(data []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// FromSnap reads the snap's configuration with `snapctl get -d`.
func FromSnap(ctx context.Context) (Config, error) {
	// Fixed program and arguments; nothing comes from outside.
	cmd := exec.CommandContext(ctx, "snapctl", append([]string{"get", "-d"}, SnapGroups...)...) //nolint:gosec // G204: constant command line
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return Config{}, fmt.Errorf("snapctl get: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	doc, err := decodeDoc(out)
	if err != nil {
		return Config{}, fmt.Errorf("parse snapctl output: %w", err)
	}
	c := Defaults()
	errs := c.Apply(Flatten(doc))
	errs = append(errs, c.Validate()...)
	if len(errs) > 0 {
		return Config{}, errs
	}
	return c, nil
}

// FromFile reads a JSON file (if path is not empty), then applies the
// -set overrides ("key=value"), then validates.
func FromFile(path string, sets []string) (Config, error) {
	c := Defaults()
	var errs Errors
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
		doc, err := decodeDoc(data)
		if err != nil {
			return Config{}, fmt.Errorf("parse %s: %w", path, err)
		}
		errs = append(errs, c.Apply(Flatten(doc))...)
	}
	overrides := map[string]any{}
	for _, s := range sets {
		k, v, ok := strings.Cut(s, "=")
		if !ok {
			errs = append(errs, fmt.Errorf("-set %q: want key=value", s))
			continue
		}
		overrides[strings.TrimSpace(k)] = v
	}
	errs = append(errs, c.Apply(overrides)...)
	if len(errs) == 0 {
		errs = append(errs, c.Validate()...)
	}
	if len(errs) > 0 {
		return Config{}, errs
	}
	return c, nil
}

// Validate checks ranges and cross-field rules and returns every problem.
func (c *Config) Validate() Errors {
	var errs Errors
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	durRange := func(name string, d, lo, hi time.Duration) {
		if d < lo || d > hi {
			bad("%s: %v is outside %v to %v", name, d, lo, hi)
		}
	}
	if c.EnrollURL != "" {
		if u, err := url.Parse(c.EnrollURL); err != nil || u.Scheme != "https" || u.Host == "" {
			bad("enroll.url: %q must be an https:// URL", c.EnrollURL)
		}
	}
	if c.EnrollToken != "" {
		if p, err := tokens.Parse(c.EnrollToken); err != nil || p.Prefix != tokens.PrefixEnrollment {
			bad("enroll.token: not an enrollment token (fwe_...)")
		}
		if c.EnrollURL == "" {
			bad("enroll.token is set but enroll.url is not")
		}
	}
	if c.EnrollCABundle != "" && !strings.Contains(c.EnrollCABundle, "-----BEGIN CERTIFICATE-----") {
		bad("enroll.ca-bundle: not a PEM certificate bundle")
	}
	if len(c.EnrollName) > 200 {
		bad("enroll.name: longer than 200 characters")
	}
	if c.BrokerURL != "" {
		if u, err := url.Parse(c.BrokerURL); err != nil || (u.Scheme != "tls" && u.Scheme != "mqtts" && u.Scheme != "ssl") || u.Port() == "" {
			bad("broker.url: %q must look like tls://host:port", c.BrokerURL)
		}
	}
	durRange("collect.interval", c.CollectInterval, 100*time.Millisecond, time.Hour)
	durRange("wal.sync-interval", c.WALSyncInterval, 10*time.Millisecond, time.Minute)
	if c.WALSegmentSize < 64<<10+wal.MaxPayload || c.WALSegmentSize > 256<<20 {
		bad("wal.segment-size: %d is outside 128KiB to 256MiB", c.WALSegmentSize)
	}
	if c.WALMaxSize < 2*c.WALSegmentSize || c.WALMaxSize > 64<<30 {
		bad("wal.max-size: %d must be at least twice wal.segment-size and at most 64GiB", c.WALMaxSize)
	}
	if c.BatchSize < 1 || c.BatchSize > uplinkpb.MaxReadings {
		bad("uplink.batch-size: %d is outside 1 to %d", c.BatchSize, uplinkpb.MaxReadings)
	}
	durRange("uplink.batch-wait", c.BatchWait, 10*time.Millisecond, time.Minute)
	if c.MaxInflight < c.BatchSize || c.MaxInflight > 100_000 {
		bad("uplink.max-inflight: %d must be at least uplink.batch-size and at most 100000", c.MaxInflight)
	}
	durRange("uplink.ack-timeout", c.AckTimeout, time.Second, 10*time.Minute)
	if c.MetricsListen != "" {
		if _, _, err := net.SplitHostPort(c.MetricsListen); err != nil {
			bad("metrics.listen: %q is not host:port", c.MetricsListen)
		}
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		bad("log.level: %q is not debug, info, warn or error", c.LogLevel)
	}
	return errs
}

// Markdown renders the reference table of every key.
func Markdown() string {
	var b strings.Builder
	b.WriteString("| Key | Default | Description |\n|---|---|---|\n")
	for _, k := range keys {
		def := k.def
		if def == "" {
			def = "-"
		}
		fmt.Fprintf(&b, "| `%s` | `%s` | %s |\n", k.name, def, k.help)
	}
	return b.String()
}

func asString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case json.Number:
		return x.String(), nil
	case bool:
		return strconv.FormatBool(x), nil
	}
	return "", fmt.Errorf("must be a string, not %T", v)
}

func asDuration(v any) (time.Duration, error) {
	s, ok := v.(string)
	if !ok {
		return 0, errors.New("must be a duration with a unit, for example 10s or 500ms")
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("must be a duration with a unit, for example 10s or 500ms")
	}
	return d, nil
}

func asInt(v any) (int, error) {
	s, err := asString(v)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("%q is not a whole number", s)
	}
	return n, nil
}

var sizeUnits = []struct {
	suffix string
	mult   int64
}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10}, {"B", 1}}

func asSize(v any) (int64, error) {
	s, err := asString(v)
	if err != nil {
		return 0, err
	}
	s = strings.TrimSpace(s)
	mult := int64(1)
	for _, u := range sizeUnits {
		if strings.HasSuffix(s, u.suffix) {
			s, mult = strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 || n > (1<<62)/mult {
		return 0, fmt.Errorf("%q is not a size like 268435456, 256MiB or 4MiB", v)
	}
	return n * mult, nil
}
