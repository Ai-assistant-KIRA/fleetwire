// Package config loads service settings from environment variables and
// command-line flags into a tagged struct.
//
// Every field declares its env var, flag, default and help text in struct
// tags, so the reference documentation is generated from the same source the
// code reads and cannot drift. Load reports every problem at once instead of
// stopping at the first, because a service that fails on one setting at a
// time costs an operator one restart per typo.
//
//	type Config struct {
//	    DatabaseURL string        `env:"FLEETWIRE_DATABASE_URL" flag:"database-url" required:"true" secret:"true" help:"..."`
//	    Interval    time.Duration `env:"FLEETWIRE_INTERVAL" flag:"interval" default:"10s" help:"..."`
//	}
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Errors collects every problem found while loading.
type Errors []error

func (e Errors) Error() string {
	msgs := make([]string, len(e))
	for i, err := range e {
		msgs[i] = "  - " + err.Error()
	}
	return fmt.Sprintf("%d configuration problem(s):\n%s", len(e), strings.Join(msgs, "\n"))
}

// Validator is implemented by configs that check cross-field rules after
// loading. It returns every problem it finds.
type Validator interface {
	Validate() []error
}

type field struct {
	value    reflect.Value
	name     string
	env      string
	flag     string
	def      string
	help     string
	required bool
	secret   bool
}

func fields(cfg any) ([]field, error) {
	v := reflect.ValueOf(cfg)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		return nil, errors.New("config: Load needs a pointer to a struct")
	}
	return structFields(v.Elem()), nil
}

// structFields lists tagged fields, descending into embedded structs so
// that shared settings (svc.Common) are loaded like the service's own.
func structFields(v reflect.Value) []field {
	t := v.Type()
	var out []field
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct && sf.IsExported() {
			out = append(out, structFields(v.Field(i))...)
			continue
		}
		if !sf.IsExported() || (sf.Tag.Get("env") == "" && sf.Tag.Get("flag") == "") {
			continue
		}
		out = append(out, field{
			value:    v.Field(i),
			name:     sf.Name,
			env:      sf.Tag.Get("env"),
			flag:     sf.Tag.Get("flag"),
			def:      sf.Tag.Get("default"),
			help:     sf.Tag.Get("help"),
			required: sf.Tag.Get("required") == "true",
			secret:   sf.Tag.Get("secret") == "true",
		})
	}
	return out
}

// Load fills cfg from defaults, then the environment, then args (flags win).
func Load(cfg any, fs *flag.FlagSet, args []string) error {
	fl, err := fields(cfg)
	if err != nil {
		return err
	}
	raw := make(map[string]string, len(fl))
	set := make(map[string]bool, len(fl))
	for _, f := range fl {
		if f.def != "" {
			raw[f.name] = f.def
		}
		// An empty variable counts as unset, so `FOO=` in a Compose file
		// or a shell does not wipe out a default.
		if v := os.Getenv(f.env); v != "" && f.env != "" {
			raw[f.name], set[f.name] = v, true
		}
	}
	flagVals := map[string]*rawValue{}
	for _, f := range fl {
		if f.flag == "" {
			continue
		}
		usage := f.help
		if f.env != "" {
			usage += " (env " + f.env + ")"
		}
		// flag.Var records the starting value as the default that -h and
		// every flag error print to stderr. Start from the tag default, not
		// the environment, so a secret set in the environment is never
		// printed; the environment still applies through raw.
		start := f.def
		if f.secret {
			start = ""
		}
		rv := &rawValue{s: start, isBool: f.value.Kind() == reflect.Bool}
		flagVals[f.name] = rv
		fs.Var(rv, f.flag, usage)
	}
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	fs.Visit(func(fv *flag.Flag) {
		for _, f := range fl {
			if f.flag == fv.Name {
				raw[f.name], set[f.name] = flagVals[f.name].s, true
			}
		}
	})

	var errs Errors
	for _, f := range fl {
		s, ok := raw[f.name]
		if !ok || s == "" {
			if f.required {
				errs = append(errs, fmt.Errorf("%s is required (env %s, flag -%s)", f.name, f.env, f.flag))
			}
			continue
		}
		if err := setValue(f.value, s); err != nil {
			shown := s
			if f.secret {
				shown = "<redacted>"
			}
			errs = append(errs, fmt.Errorf("%s (env %s): invalid value %q: %w", f.name, f.env, shown, err))
		}
	}
	if v, ok := cfg.(Validator); ok && len(errs) == 0 {
		errs = append(errs, v.Validate()...)
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// rawValue keeps a flag's text; parsing happens later so that every error
// is reported together. Bool fields accept "-name" without a value.
type rawValue struct {
	s      string
	isBool bool
}

func (r *rawValue) String() string { return r.s }

func (r *rawValue) Set(s string) error {
	r.s = s
	return nil
}

func (r *rawValue) IsBoolFlag() bool { return r.isBool }

func setValue(v reflect.Value, s string) error {
	if v.Type() == reflect.TypeFor[time.Duration]() {
		d, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		v.SetInt(int64(d))
		return nil
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(s)
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetUint(n)
	case reflect.Float64:
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		v.SetFloat(n)
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			return fmt.Errorf("unsupported slice type %s", v.Type())
		}
		var parts []string
		for _, p := range strings.Split(s, ",") {
			if p = strings.TrimSpace(p); p != "" {
				parts = append(parts, p)
			}
		}
		v.Set(reflect.ValueOf(parts))
	default:
		return fmt.Errorf("unsupported type %s", v.Type())
	}
	return nil
}

// Markdown renders the reference table for cfg.
func Markdown(cfg any) (string, error) {
	fl, err := fields(cfg)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("| Env var | Flag | Default | Required | Description |\n|---|---|---|---|---|\n")
	for _, f := range fl {
		def := f.def
		if def == "" {
			def = "-"
		}
		req := ""
		if f.required {
			req = "yes"
		}
		help := f.help
		if f.secret {
			help += " Secret: never logged."
		}
		fmt.Fprintf(&b, "| `%s` | `-%s` | `%s` | %s | %s |\n", f.env, f.flag, def, req, help)
	}
	return b.String(), nil
}
