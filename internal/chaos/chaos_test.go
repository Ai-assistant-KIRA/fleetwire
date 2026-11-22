package chaos

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/checker"
)

// checkerRow runs a checker.Report through its JSON form, as the runner
// reads it from checker.json, and judges it.
func checkerRow(t *testing.T, rep checker.Report) Row {
	t.Helper()
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	var cr checkerReport
	if err := json.Unmarshal(b, &cr); err != nil {
		t.Fatal(err)
	}
	return judge(Row{Scenario: "s"}, cr)
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
