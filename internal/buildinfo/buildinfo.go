// Package buildinfo holds version information stamped into binaries at link time.
//
// The release pipeline sets the variables with
//
//	-ldflags "-X github.com/Ai-assistant-KIRA/fleetwire/internal/buildinfo.Version=v0.1.0 ..."
//
// When a binary is built without ldflags (go run, go test), the VCS data that
// the Go toolchain embeds is used instead, so a dev build still reports the
// commit it came from.
package buildinfo

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Set through -ldflags -X by the Makefile and GoReleaser.
var (
	Version = "dev"
	Commit  = ""
	Date    = ""
)

// Info is the version information of the running binary.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Date      string `json:"date"`
	GoVersion string `json:"go_version"`
	Modified  bool   `json:"modified,omitempty"`
}

// Get returns the stamped values, filling gaps from the embedded VCS data.
func Get() Info {
	info := Info{Version: Version, Commit: Commit, Date: Date, GoVersion: runtime.Version()}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			if info.Commit == "" {
				info.Commit = s.Value
			}
		case "vcs.time":
			if info.Date == "" {
				info.Date = s.Value
			}
		case "vcs.modified":
			info.Modified = s.Value == "true"
		}
	}
	return info
}

// String formats the info for --version output and startup logs.
func (i Info) String() string {
	commit := i.Commit
	if len(commit) > 12 {
		commit = commit[:12]
	}
	if commit == "" {
		commit = "unknown"
	}
	if i.Modified {
		commit += "-dirty"
	}
	// The snap stamps only Version and builds from a tree without .git, so
	// a release binary can have no date at all.
	built := ""
	if i.Date != "" {
		built = ", built " + i.Date
	}
	return fmt.Sprintf("%s (commit %s%s, %s)", i.Version, commit, built, i.GoVersion)
}
