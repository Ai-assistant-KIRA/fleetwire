package chaos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// Machine describes the host the run happened on.
func Machine() string {
	cpu := "unknown CPU"
	if f, err := os.Open("/proc/cpuinfo"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if k, v, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(k) == "model name" {
				cpu = strings.TrimSpace(v)
				break
			}
		}
		_ = f.Close()
	}
	mem := ""
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		f := strings.Fields(strings.SplitN(string(b), "\n", 2)[0])
		if len(f) >= 2 {
			mem = f[1] + " kB RAM"
		}
	}
	osName := ""
	if b, err := os.ReadFile("/etc/os-release"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if v, ok := strings.CutPrefix(l, "PRETTY_NAME="); ok {
				osName = strings.Trim(v, `"`)
			}
		}
	}
	return fmt.Sprintf("%d vCPU %s, %s, %s", runtime.NumCPU(), cpu, mem, osName)
}

// gitSHA names the commit the results came from, marked -dirty when tracked
// files differ from it, so a table never claims a commit it did not test.
func gitSHA(ctx context.Context, repo string) string {
	ctx = context.WithoutCancel(ctx)
	out, err := exec.CommandContext(ctx, "git", "-C", repo, "rev-parse", "--short=12", "HEAD").Output() //nolint:gosec // G204: fixed program
	if err != nil {
		return "unknown"
	}
	sha := strings.TrimSpace(string(out))
	status, err := exec.CommandContext(ctx, "git", "-C", repo, "status", "--porcelain", "--untracked-files=no").Output() //nolint:gosec // G204: fixed program
	if err != nil || len(bytes.TrimSpace(status)) > 0 {
		sha += "-dirty"
	}
	return sha
}

// Table renders rows as Markdown.
func Table(rows []Row) string {
	var b strings.Builder
	b.WriteString("| Scenario | Devices | Readings | Missing | Duplicates | Gap seqs | Recovery | WAL sync | Result |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---|---|\n")
	for _, r := range rows {
		res := "PASS"
		if !r.Pass {
			res = "FAIL"
			if r.Note != "" {
				res += " (" + r.Note + ")"
			}
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %s | %s | %s |\n",
			r.Scenario, r.Devices, r.Readings, r.Missing, r.Duplicates, r.GapSeqs, r.Recovery, r.WALSync, res)
	}
	return b.String()
}

func writeResults(ctx context.Context, o Options, stamp string, rows []Row) error {
	if err := os.MkdirAll(o.OutDir, 0o750); err != nil {
		return err
	}
	sha := gitSHA(ctx, o.RepoDir)
	base := filepath.Join(o.OutDir, stamp+"-"+sha)
	cmdline := fmt.Sprintf("fleetwire-chaos run -devices %d -interval %s -readings %d -wal-sync %s -scenario %s",
		o.Devices, o.Interval, o.Readings, o.WALSync, strings.Join(o.Scenarios, ","))
	md := fmt.Sprintf("# Failure test results %s\n\n- Commit: %#q\n- Machine: %s\n- Command: %#q\n- Load: %d devices, %d reading(s) every %s each, steady %s before and %s after each fault\n\n%s\nRecovery is the time from the end of the fault until the p99 end-to-end delay of newly stored readings is under 5 s again. Raw logs, results.jsonl and checker reports are in `raw/%s/` (not committed).\n",
		stamp, sha, Machine(), cmdline, o.Devices, o.Readings, o.Interval, o.Steady, o.After, Table(rows), stamp)
	if err := os.WriteFile(base+".md", []byte(md), 0o644); err != nil { //nolint:gosec // published results
		return err
	}
	js, err := json.MarshalIndent(map[string]any{"commit": sha, "machine": Machine(), "command": cmdline, "rows": rows}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(base+".json", js, 0o644) //nolint:gosec // published results
}
