// Command fleetwire-checker reads stored data back and proves that every
// seq of every device is stored exactly once or covered by a reported gap.
//
//	fleetwire-checker -results results.jsonl          load-generator run
//	fleetwire-checker -device ID -last-seq N [-gaps a-b,c-d]   one device (Ubuntu Core VM)
//
// It exits 1 when anything is missing or duplicated, or when a seq above
// the device's last seq is stored (the device lost its state).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/checker"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/crypto/keyring"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/loadgen"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/store"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/tenantkey"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

func main() {
	ok, err := run(os.Args[1:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fleetwire-checker:", err)
		os.Exit(2)
	}
	if !ok {
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) (bool, error) {
	fs := flag.NewFlagSet("fleetwire-checker", flag.ContinueOnError)
	results := fs.String("results", "", "results.jsonl written by the load generator.")
	device := fs.String("device", "", "Check one device instead.")
	lastSeq := fs.Uint64("last-seq", 0, "The device's last seq (with -device).")
	gaps := fs.String("gaps", "", "Gaps the device reported, a-b,c-d (with -device).")
	dbURL := fs.String("database-url", envOr("FLEETWIRE_DATABASE_URL", "postgres://fleetwire_app:app@127.0.0.1:5432/fleetwire?sslmode=disable"), "Runtime-role database URL.")
	krDir := fs.String("keyring-dir", envOr("FLEETWIRE_KEYRING_DIR", "deploy/compose/.state/keyring"), "Keyring directory.")
	rootKey := fs.String("keyring-root-key", envOr("FLEETWIRE_KEYRING_ROOT_KEY_FILE", "deploy/compose/secrets/keyring-root.key"), "Keyring root key.")
	jsonOut := fs.String("json-out", "", "Also write the full report as JSON here.")
	workers := fs.Int("workers", 8, "Devices checked in parallel.")
	if err := fs.Parse(args); err != nil {
		return false, err
	}
	exps, err := expectations(*results, *device, *lastSeq, *gaps)
	if err != nil {
		return false, err
	}
	ctx := context.Background()
	db, err := store.Connect(ctx, *dbURL, store.Options{ApplicationName: "fleetwire-checker", MaxConns: int32(*workers + 2)})
	if err != nil {
		return false, err
	}
	defer db.Close()
	if err := db.CheckRuntimeRole(ctx); err != nil {
		return false, err
	}
	root, err := keyring.LoadRootKey(*rootKey)
	if err != nil {
		return false, err
	}
	kr, err := keyring.NewLocal(*krDir, root)
	if err != nil {
		return false, err
	}
	keys := tenantkey.New(db, kr, slog.New(slog.DiscardHandler))
	sweepCtx, stopSweep := context.WithCancel(ctx)
	defer stopSweep()
	go keys.Run(sweepCtx)
	c := &checker.Checker{DB: db, Keys: keys, Workers: *workers}
	rep := c.Run(ctx, exps)
	fmt.Fprintf(out, "devices %d  readings %d  stored %d  missing %d  duplicates %d  gap seqs %d  beyond last seq %d  unrecorded gap seqs %d  errors %d\n",
		rep.Devices, rep.Readings, rep.Stored, rep.Missing, rep.Duplicates, rep.GapSeqs, rep.Beyond, rep.LostReports, rep.Errors)
	for _, f := range rep.Failed {
		fmt.Fprintf(out, "  %s: missing %d %v, duplicates %d %v, beyond last seq %d, unrecorded gaps %d %s\n",
			f.DeviceID, f.Missing, f.MissingList, f.Duplicates, f.DupList, f.Beyond, f.LostReports, f.Error)
	}
	if *jsonOut != "" {
		b, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return false, err
		}
		if err := os.WriteFile(*jsonOut, b, 0o600); err != nil {
			return false, err
		}
	}
	if rep.OK() {
		fmt.Fprintln(out, "PASS: every seq is stored exactly once or covered by a reported gap")
	} else {
		fmt.Fprintln(out, "FAIL")
	}
	return rep.OK(), nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func expectations(results, device string, lastSeq uint64, gaps string) ([]checker.Expectation, error) {
	if results != "" {
		rs, err := loadgen.ReadResults(results)
		if err != nil {
			return nil, err
		}
		out := make([]checker.Expectation, 0, len(rs))
		for _, r := range rs {
			id, err := uuid.Parse(r.DeviceID)
			if err != nil {
				return nil, fmt.Errorf("results line %d: device id %q", r.Index, r.DeviceID)
			}
			out = append(out, checker.Expectation{DeviceID: id, LastSeq: r.LastSeq, Gaps: r.Gaps})
		}
		return out, nil
	}
	if device == "" {
		return nil, fmt.Errorf("give -results or -device")
	}
	id, err := uuid.Parse(device)
	if err != nil {
		return nil, fmt.Errorf("-device: %w", err)
	}
	e := checker.Expectation{DeviceID: id, LastSeq: lastSeq}
	for _, part := range strings.Split(gaps, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		a, b, ok := strings.Cut(part, "-")
		from, err1 := strconv.ParseUint(a, 10, 64)
		to, err2 := strconv.ParseUint(b, 10, 64)
		if !ok || err1 != nil || err2 != nil || from == 0 || from > to {
			return nil, fmt.Errorf("-gaps: %q is not a-b", part)
		}
		e.Gaps = append(e.Gaps, wal.Gap{From: from, To: to})
	}
	return []checker.Expectation{e}, nil
}
