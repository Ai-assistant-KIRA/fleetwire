package buildinfo

import (
	"strings"
	"testing"
)

func TestStringShortensCommitAndMarksDirty(t *testing.T) {
	got := Info{Version: "v1.2.3", Commit: "0123456789abcdef", Date: "2026-01-02", GoVersion: "go1.x", Modified: true}.String()
	want := "v1.2.3 (commit 0123456789ab-dirty, built 2026-01-02, go1.x)"
	if got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestGetKeepsLinkerValues(t *testing.T) {
	old := Version
	t.Cleanup(func() { Version = old })
	Version = "v9.9.9"
	if got := Get(); got.Version != "v9.9.9" || !strings.HasPrefix(got.GoVersion, "go") {
		t.Fatalf("Get() = %+v", got)
	}
}
