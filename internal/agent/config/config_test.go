package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

func TestDefaultsAreValid(t *testing.T) {
	c := Defaults()
	if errs := c.Validate(); len(errs) > 0 {
		t.Fatalf("defaults invalid: %v", errs)
	}
	if c.CollectInterval != 10*time.Second || c.WALSync != wal.SyncInterval || c.WALMaxSize != 256<<20 ||
		c.WALSegmentSize != 4<<20 || c.BatchSize != 100 || c.MaxInflight != 1000 || c.AckTimeout != 30*time.Second {
		t.Fatalf("defaults = %+v", c)
	}
}

// snapctl get -d returns typed JSON, with "" for groups that are not set.
func TestSnapDocument(t *testing.T) {
	doc, err := decodeDoc([]byte(`{"enroll": {"url": "https://api.test:8444", "token": ""}, "broker": "",
		"collect": {"interval": "200ms"}, "wal": {"max-size": 134217728, "sync": "always"},
		"uplink": {"batch-size": 50}, "metrics": "", "log": ""}`))
	if err != nil {
		t.Fatal(err)
	}
	c := Defaults()
	if errs := c.Apply(Flatten(doc)); len(errs) > 0 {
		t.Fatal(errs)
	}
	if errs := c.Validate(); len(errs) > 0 {
		t.Fatal(errs)
	}
	if c.EnrollURL != "https://api.test:8444" || c.CollectInterval != 200*time.Millisecond ||
		c.WALMaxSize != 128<<20 || c.WALSync != wal.SyncAlways || c.BatchSize != 50 {
		t.Fatalf("config = %+v", c)
	}
}

func TestFileAndOverrides(t *testing.T) {
	p := filepath.Join(t.TempDir(), "agent.json")
	if err := os.WriteFile(p, []byte(`{"wal": {"segment-size": "1MiB", "max-size": "8MiB"}, "log": {"level": "debug"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := FromFile(p, []string{"uplink.batch-size=10", "collect.interval=1s"})
	if err != nil {
		t.Fatal(err)
	}
	if c.WALSegmentSize != 1<<20 || c.WALMaxSize != 8<<20 || c.LogLevel != "debug" || c.BatchSize != 10 || c.CollectInterval != time.Second {
		t.Fatalf("config = %+v", c)
	}
}

func TestEveryProblemIsReported(t *testing.T) {
	_, err := FromFile("", []string{
		"collect.intervall=10s",      // typo
		"wal.sync=sometimes",         // bad enum
		"uplink.ack-timeout=30",      // no unit
		"wal.max-size=lots",          // not a size
		"enroll.token=fwe_not-valid", // checked by Validate, after the parse errors are fixed
	})
	var errs Errors
	if !errors.As(err, &errs) || len(errs) != 4 {
		t.Fatalf("got %v, want 4 parse problems", err)
	}
	for _, want := range []string{"collect.intervall: unknown key", "wal.sync", "uplink.ack-timeout", "wal.max-size"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in %v", want, err)
		}
	}
	_, err = FromFile("", []string{"enroll.token=fwe_not-valid", "uplink.batch-size=5000", "metrics.listen=nope", "log.level=loud"})
	// batch-size 5000 also breaks the max-inflight >= batch-size rule.
	if !errors.As(err, &errs) || len(errs) != 6 {
		t.Fatalf("got %v, want 6 validation problems (token, token without url, batch size, inflight, listen, level)", err)
	}
}

func TestSizes(t *testing.T) {
	for in, want := range map[string]int64{"4MiB": 4 << 20, "4M": 4 << 20, "512KiB": 512 << 10, "1GiB": 1 << 30, "1000": 1000, "10B": 10} {
		if got, err := asSize(in); err != nil || got != want {
			t.Errorf("asSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"-1", "4TB", "", "1.5MiB"} {
		if _, err := asSize(in); err == nil {
			t.Errorf("asSize(%q) accepted", in)
		}
	}
}
