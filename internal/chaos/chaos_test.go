package chaos

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/checker"
	"github.com/Ai-assistant-KIRA/fleetwire/internal/loadgen"
)

// checkerRow runs a checker.Report through its JSON form, as the runner
// reads it from checker.json, and judges it for a run of rep.Devices.
func checkerRow(t *testing.T, rep checker.Report) Row {
	t.Helper()
	return checkerRowOf(t, rep, rep.Devices)
}

func checkerRowOf(t *testing.T, rep checker.Report, devices int) Row {
	t.Helper()
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	var cr checkerReport
	if err := json.Unmarshal(b, &cr); err != nil {
		t.Fatal(err)
	}
	return judge(Row{Scenario: "s"}, cr, devices)
}

func TestJudgeReadsEveryCheckerCount(t *testing.T) {
	row := checkerRow(t, checker.Report{Devices: 2, Readings: 100, Stored: 95, GapSeqs: 5})
	if !row.Pass || row.Readings != 100 || row.Stored != 95 || row.GapSeqs != 5 || row.Note != "" {
		t.Fatalf("clean report judged as %+v", row)
	}
}

func TestJudgeFailsOnSeqsBeyondTheLastSeq(t *testing.T) {
	row := checkerRow(t, checker.Report{Devices: 2, Readings: 100, Stored: 103, Beyond: 3})
	if row.Pass || row.Beyond != 3 || !strings.Contains(row.Note, "3 stored seqs above") {
		t.Fatalf("seqs beyond the last seq judged as %+v", row)
	}
}

func TestJudgeExplainsFailuresWithoutAColumn(t *testing.T) {
	row := checkerRow(t, checker.Report{Devices: 1, Readings: 10, Stored: 8, LostReports: 2, Errors: 1})
	if row.Pass || !strings.Contains(row.Note, "2 reported gap seqs not recorded") || !strings.Contains(row.Note, "checker errors on 1 devices") {
		t.Fatalf("judged as %+v", row)
	}
	if row := checkerRow(t, checker.Report{}); row.Pass || row.Note != "no readings" {
		t.Fatalf("empty report judged as %+v", row)
	}
}

// A device that never started is not in results.jsonl, so the checker
// passes the others; the device count must catch it.
func TestJudgeFailsWhenDevicesAreMissingFromTheResults(t *testing.T) {
	row := checkerRowOf(t, checker.Report{Devices: 1998, Readings: 1000, Stored: 1000}, 2000)
	if row.Pass || row.Devices != 1998 || !strings.Contains(row.Note, "results list 1998 of 2000 devices") {
		t.Fatalf("judged as %+v", row)
	}
}

func TestRampDone(t *testing.T) {
	for _, c := range []struct {
		st      loadgen.Stats
		done    bool
		problem string
	}{
		{loadgen.Stats{Devices: 10, Started: 4, Running: 4}, false, ""},
		{loadgen.Stats{Devices: 10, Started: 10, Running: 10}, true, ""},
		{loadgen.Stats{Devices: 10, Started: 8, Running: 8, OpenFailures: 1}, false, ""},
		{loadgen.Stats{Devices: 10, Started: 8, Running: 8, OpenFailures: 2}, true, "8 of 10 devices running (2 did not start, 0 stopped with an error)"},
		{loadgen.Stats{Devices: 10, Started: 10, Running: 9, RunFailures: 1}, true, "9 of 10 devices running (0 did not start, 1 stopped with an error)"},
	} {
		done, problem := rampDone(c.st, 10)
		if done != c.done || problem != c.problem {
			t.Errorf("rampDone(%+v) = %v, %q; want %v, %q", c.st, done, problem, c.done, c.problem)
		}
	}
}
