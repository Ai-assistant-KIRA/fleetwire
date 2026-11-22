package chaos

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

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

// The p99 window must never reach back before the fault ended: a 20 s
// window read right after the fault still holds the healthy minute before
// it, which is how recovery used to come out as 0 s.
func TestPostFaultWindow(t *testing.T) {
	end := time.Unix(1000, 0)
	for _, c := range []struct {
		after time.Duration
		want  time.Duration
		ok    bool
	}{
		{0, 0, false},
		{900 * time.Millisecond, 0, false},
		{7900 * time.Millisecond, 7 * time.Second, true},
		{20 * time.Second, 20 * time.Second, true},
		{5 * time.Minute, 20 * time.Second, true},
	} {
		w, ok := postFaultWindow(end.Add(c.after), end)
		if w != c.want || ok != c.ok {
			t.Errorf("%s after the fault: window %s, %v; want %s, %v", c.after, w, ok, c.want, c.ok)
		}
		if ok && end.Add(c.after).Add(-w).Before(end) {
			t.Errorf("%s after the fault: window %s reaches back before the fault", c.after, w)
		}
	}
	if q := p99Query(7 * time.Second); !strings.Contains(q, "[7s]") {
		t.Fatalf("p99Query = %s", q)
	}
}

func TestRecoveryTracker(t *testing.T) {
	end := time.Unix(1000, 0)
	nan := math.NaN()
	type obs struct {
		at        time.Duration // after the fault
		p99       float64
		connected float64
	}
	for _, c := range []struct {
		name string
		obs  []obs
		// want is the recovery reported at the last sample; -1: none yet.
		want time.Duration
	}{
		{"no data yet is not recovered", []obs{{2 * time.Second, nan, 100}, {40 * time.Second, nan, 100}}, -1},
		{"recovered from the first good sample once it held for 30 s",
			[]obs{{6 * time.Second, 2, 100}, {20 * time.Second, 2, 100}, {36 * time.Second, 3, 100}}, 6 * time.Second},
		{"not before the stretch is 30 s long", []obs{{6 * time.Second, 2, 100}, {35 * time.Second, 2, 100}}, -1},
		{"a slow sample starts the stretch over",
			[]obs{{6 * time.Second, 2, 100}, {20 * time.Second, 8, 100}, {24 * time.Second, 2, 100}, {54 * time.Second, 2, 100}}, 24 * time.Second},
		{"a device still away is not recovered, however fast the others are",
			[]obs{{6 * time.Second, 1, 99}, {60 * time.Second, 1, 99}}, -1},
		{"an unknown connection count is not recovered", []obs{{6 * time.Second, 1, nan}, {60 * time.Second, 1, nan}}, -1},
		{"a stall with no readings stored starts the stretch over",
			[]obs{{6 * time.Second, 2, 100}, {20 * time.Second, nan, 100}, {30 * time.Second, 2, 100}, {60 * time.Second, 2, 100}}, 30 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			tr := recoveryTracker{faultEnd: end, devices: 100, stable: 30 * time.Second}
			got, ok := time.Duration(-1), false
			for i, o := range c.obs {
				d, done := tr.add(recoverySample{at: end.Add(o.at), p99: o.p99, connected: o.connected})
				if done && i < len(c.obs)-1 {
					t.Fatalf("recovered at sample %d of %d", i+1, len(c.obs))
				}
				if done {
					got, ok = d, true
				}
			}
			if (c.want < 0 && ok) || (c.want >= 0 && got != c.want) {
				t.Fatalf("recovery %s (%v), want %s", got, ok, c.want)
			}
		})
	}
}
