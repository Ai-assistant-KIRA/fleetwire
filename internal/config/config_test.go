package config

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"
)

type sample struct {
	URL      string        `env:"T_URL" flag:"url" required:"true" secret:"true" help:"Database URL."`
	Interval time.Duration `env:"T_INTERVAL" flag:"interval" default:"10s" help:"How often."`
	Workers  int           `env:"T_WORKERS" flag:"workers" default:"4" help:"Worker count."`
	Brokers  []string      `env:"T_BROKERS" flag:"brokers" default:"a:1,b:2" help:"Kafka brokers."`
	Debug    bool          `env:"T_DEBUG" flag:"debug" help:"Debug mode."`
}

func (s *sample) Validate() []error {
	if s.Workers < 1 {
		return []error{errors.New("Workers must be at least 1")}
	}
	return nil
}

func TestPrecedenceDefaultsEnvFlags(t *testing.T) {
	t.Setenv("T_URL", "postgres://env")
	t.Setenv("T_WORKERS", "8")
	var c sample
	if err := Load(&c, flag.NewFlagSet("x", flag.ContinueOnError), []string{"-workers=16", "-debug"}); err != nil {
		t.Fatal(err)
	}
	if c.URL != "postgres://env" || c.Workers != 16 || c.Interval != 10*time.Second || !c.Debug ||
		strings.Join(c.Brokers, ";") != "a:1;b:2" {
		t.Fatalf("loaded %+v", c)
	}
}

func TestReportsEveryProblemAndRedactsSecrets(t *testing.T) {
	t.Setenv("T_INTERVAL", "soon")
	t.Setenv("T_WORKERS", "many")
	var c sample
	err := Load(&c, flag.NewFlagSet("x", flag.ContinueOnError), nil)
	var errs Errors
	if !errors.As(err, &errs) || len(errs) != 3 {
		t.Fatalf("Load = %v, want 3 problems (missing URL, bad interval, bad workers)", err)
	}
	t.Setenv("T_URL", "s3cr3t")
	t.Setenv("T_INTERVAL", "1s")
	t.Setenv("T_WORKERS", "0")
	err = Load(&c, flag.NewFlagSet("x", flag.ContinueOnError), nil)
	if err == nil || !strings.Contains(err.Error(), "Workers must be at least 1") {
		t.Fatalf("Validate not applied: %v", err)
	}
	t.Setenv("T_URL", "s3cr3t")
	t.Setenv("T_DEBUG", "maybe")
	err = Load(&c, flag.NewFlagSet("x", flag.ContinueOnError), nil)
	if err == nil || strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("error leaks or is missing: %v", err)
	}
}

// The flag package prints every flag's default on -h and after a bad flag.
// Values from the environment must not show up there.
func TestHelpAndFlagErrorsDoNotShowEnvironmentValues(t *testing.T) {
	t.Setenv("T_URL", "postgres://app:s3cr3t@db/fleetwire")
	t.Setenv("T_WORKERS", "8")
	for _, args := range [][]string{{"-h"}, {"-no-such-flag"}, {"-workers"}} {
		var out bytes.Buffer
		fs := flag.NewFlagSet("x", flag.ContinueOnError)
		fs.SetOutput(&out)
		err := Load(&sample{}, fs, args)
		if err == nil {
			t.Fatalf("Load(%v) succeeded, want a flag error", args)
		}
		if text := out.String() + err.Error(); strings.Contains(text, "s3cr3t") || strings.Contains(text, "default 8") {
			t.Errorf("Load(%v) printed an environment value:\n%s", args, text)
		}
		if !strings.Contains(out.String(), "(default 4)") {
			t.Errorf("Load(%v) usage misses the tag default:\n%s", args, out.String())
		}
	}
}

func TestMarkdown(t *testing.T) {
	md, err := Markdown(&sample{})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"`T_URL`", "`-interval`", "`10s`", "Secret: never logged."} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown misses %s:\n%s", want, md)
		}
	}
}

type Shared struct {
	Level string `env:"T_LEVEL" flag:"level" default:"info" help:"Level."`
}

type withEmbedded struct {
	Shared
	Name string `env:"T_NAME" flag:"name" default:"x" help:"Name."`
}

func TestEmbeddedStructsAndEmptyEnv(t *testing.T) {
	t.Setenv("T_LEVEL", "debug")
	t.Setenv("T_NAME", "")
	var c withEmbedded
	if err := Load(&c, flag.NewFlagSet("x", flag.ContinueOnError), nil); err != nil {
		t.Fatal(err)
	}
	if c.Level != "debug" || c.Name != "x" {
		t.Fatalf("loaded %+v: embedded field not read, or empty env replaced the default", c)
	}
}
