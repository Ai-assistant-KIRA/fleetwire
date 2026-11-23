package statedir

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/wal"
)

// oldLayout writes what v0.1.1 leaves in $SNAP_DATA: an identity and a
// local log with records the server never acknowledged.
func oldLayout(t *testing.T, dataDir string) {
	t.Helper()
	id := filepath.Join(dataDir, "identity")
	if err := os.Mkdir(id, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, perm := range map[string]os.FileMode{"key.pem": 0o600, "cert.pem": 0o644, "ca.pem": 0o644, "device.json": 0o644} {
		if err := os.WriteFile(filepath.Join(id, name), []byte("contents of "+name), perm); err != nil {
			t.Fatal(err)
		}
	}
	l, _, err := wal.Open(wal.Options{Dir: filepath.Join(dataDir, "wal"), Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if _, err := l.Append(fmt.Appendf(nil, "reading %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	l.Ack(2)
	if err := l.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

// tree lists every entry below root with its mode and, for files, its
// contents.
func tree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == root {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		v := fi.Mode().String()
		if fi.Mode().IsRegular() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			v += " " + string(b)
		}
		out[rel] = v
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// under returns the entries of a tree() that lie below prefix, relative
// to it.
func under(entries map[string]string, prefix string) map[string]string {
	out := map[string]string{}
	for k, v := range entries {
		if rel, ok := strings.CutPrefix(k, prefix+string(filepath.Separator)); ok {
			out[rel] = v
		}
	}
	return out
}

func equal(t *testing.T, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d:\ngot  %v\nwant %v", len(got), len(want), got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: got %q, want %q", k, got[k], v)
		}
	}
}

// The test's own user stands in for snap_daemon: Prepare's owner changes
// then succeed without root.
func prepareAsSelf(dataDir string) (Result, error) {
	return Prepare(dataDir, os.Getuid(), os.Getgid())
}

func TestPrepareCreatesTheStateDirectory(t *testing.T) {
	dataDir := t.TempDir()
	res, err := prepareAsSelf(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || len(res.Moved) != 0 {
		t.Fatalf("result %+v, want Created and nothing moved", res)
	}
	equal(t, tree(t, dataDir), map[string]string{Name: "drwx------"})
	res, err = prepareAsSelf(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || res.Repaired || len(res.Moved) != 0 {
		t.Fatalf("second run changed something: %+v", res)
	}
}

func TestPrepareMovesTheOldLayoutWithEveryRecord(t *testing.T) {
	dataDir := t.TempDir()
	oldLayout(t, dataDir)
	before := tree(t, dataDir)
	inBefore, err := wal.Inspect(filepath.Join(dataDir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := prepareAsSelf(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || !slices.Equal(res.Moved, []string{"identity", "wal"}) {
		t.Fatalf("result %+v", res)
	}
	after := tree(t, dataDir)
	if after[Name] != "drwx------" {
		t.Errorf("state directory mode %q", after[Name])
	}
	equal(t, under(after, Name), before)
	for _, name := range []string{"identity", "wal", partial} {
		if _, err := os.Lstat(filepath.Join(dataDir, name)); !os.IsNotExist(err) {
			t.Errorf("%s is still in the data directory (%v)", name, err)
		}
	}
	// The log opens from its new place with the same unacked records.
	inAfter, err := wal.Inspect(filepath.Join(dataDir, Name, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if inAfter.LastSeq != 5 || inAfter.LastSeq != inBefore.LastSeq || inAfter.State.Acked != 2 {
		t.Fatalf("after the move: last seq %d, acked %d; before: last seq %d", inAfter.LastSeq, inAfter.State.Acked, inBefore.LastSeq)
	}
}

// A crash can stop Prepare between any two steps. Running it again
// finishes the job and loses nothing.
func TestPrepareFinishesAnInterruptedMove(t *testing.T) {
	for _, moved := range [][]string{{}, {"identity"}, {"identity", "wal"}} {
		t.Run(fmt.Sprint(len(moved)), func(t *testing.T) {
			dataDir := t.TempDir()
			oldLayout(t, dataDir)
			want := tree(t, dataDir)
			if err := os.Mkdir(filepath.Join(dataDir, partial), 0o700); err != nil {
				t.Fatal(err)
			}
			for _, name := range moved {
				if err := os.Rename(filepath.Join(dataDir, name), filepath.Join(dataDir, partial, name)); err != nil {
					t.Fatal(err)
				}
			}
			res, err := prepareAsSelf(dataDir)
			if err != nil {
				t.Fatal(err)
			}
			if !res.Created || len(res.Moved) != 2-len(moved) {
				t.Fatalf("result %+v", res)
			}
			equal(t, under(tree(t, dataDir), Name), want)
		})
	}
}

func TestPrepareRefusesToMixTwoStates(t *testing.T) {
	for name, setup := range map[string]func(t *testing.T, dataDir string){
		"state and the old layout": func(t *testing.T, dataDir string) {
			oldLayout(t, dataDir)
			if err := os.Mkdir(filepath.Join(dataDir, Name), 0o700); err != nil {
				t.Fatal(err)
			}
		},
		"state and a partial state": func(t *testing.T, dataDir string) {
			for _, d := range []string{Name, partial} {
				if err := os.Mkdir(filepath.Join(dataDir, d), 0o700); err != nil {
					t.Fatal(err)
				}
			}
		},
		"an old entry in both places": func(t *testing.T, dataDir string) {
			oldLayout(t, dataDir)
			if err := os.MkdirAll(filepath.Join(dataDir, partial, "wal"), 0o700); err != nil {
				t.Fatal(err)
			}
		},
		"state is a file": func(t *testing.T, dataDir string) {
			if err := os.WriteFile(filepath.Join(dataDir, Name), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			dataDir := t.TempDir()
			setup(t, dataDir)
			before := tree(t, dataDir)
			if _, err := prepareAsSelf(dataDir); err == nil {
				t.Fatal("Prepare succeeded")
			}
			equal(t, tree(t, dataDir), before)
		})
	}
}

// recorder stands in for root giving entries to another user: it records
// each owner change, and nothing counts as owned unless listed.
type recorder struct {
	chowned []string
	owned   map[string]bool
	modeAt  map[string]os.FileMode
}

func (r *recorder) preparer() *preparer {
	return &preparer{
		uid: SnapDaemonID, gid: SnapDaemonID,
		chown: func(path string, uid, gid int) error {
			if uid != SnapDaemonID || gid != SnapDaemonID {
				return fmt.Errorf("chown %s to %d:%d", path, uid, gid)
			}
			fi, err := os.Lstat(path)
			if err != nil {
				return err
			}
			r.modeAt[path] = fi.Mode().Perm()
			r.chowned = append(r.chowned, path)
			return nil
		},
		owned: func(fi fs.FileInfo) bool { return r.owned[fi.Name()] },
	}
}

func newRecorder(owned ...string) *recorder {
	r := &recorder{owned: map[string]bool{}, modeAt: map[string]os.FileMode{}}
	for _, o := range owned {
		r.owned[o] = true
	}
	return r
}

// Root loses access to a directory of mode 0700 once it gives it away, so
// everything inside must change owner first, and the mode must be set
// before.
func TestPrepareGivesEntriesAwayBeforeTheirDirectory(t *testing.T) {
	dataDir := t.TempDir()
	oldLayout(t, dataDir)
	r := newRecorder()
	if _, err := r.preparer().prepare(dataDir); err != nil {
		t.Fatal(err)
	}
	all := tree(t, filepath.Join(dataDir, Name))
	if len(r.chowned) != len(all)+1 {
		t.Fatalf("%d owner changes for %d entries and the directory: %v", len(r.chowned), len(all), r.chowned)
	}
	top := filepath.Join(dataDir, partial)
	if last := r.chowned[len(r.chowned)-1]; last != top {
		t.Fatalf("last owner change %s, want %s", last, top)
	}
	if r.modeAt[top] != 0o700 {
		t.Errorf("mode of %s at its owner change: %#o", top, r.modeAt[top])
	}
	for i, p := range r.chowned {
		for _, later := range r.chowned[i+1:] {
			if strings.HasPrefix(later, p+string(filepath.Separator)) {
				t.Errorf("%s changed owner after its directory %s", later, p)
			}
		}
	}
}

// A directory that already has the owner is done, and root may not be
// able to read it: Prepare does not enter it.
func TestPrepareSkipsADirectoryTheUserOwns(t *testing.T) {
	dataDir := t.TempDir()
	oldLayout(t, dataDir)
	// Unreadable, but still movable: moving a directory to another parent
	// needs write permission on it.
	if err := os.Chmod(filepath.Join(dataDir, "wal"), 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(dataDir, Name, "wal"), 0o700) })
	r := newRecorder("wal")
	if _, err := r.preparer().prepare(dataDir); err != nil {
		t.Fatal(err)
	}
	for _, p := range r.chowned {
		if strings.Contains(p, string(filepath.Separator)+"wal") {
			t.Errorf("changed the owner of %s", p)
		}
	}
	if len(r.chowned) != 6 { // identity, its 4 files, the directory
		t.Errorf("owner changes: %v", r.chowned)
	}
}

// An existing state directory that root still owns, after a crash or an
// administrator's copy, is handed over with mode 0700.
func TestPrepareRepairsTheOwnerOfAnExistingStateDirectory(t *testing.T) {
	dataDir := t.TempDir()
	st := filepath.Join(dataDir, Name)
	if err := os.MkdirAll(filepath.Join(st, "wal"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(st, 0o755); err != nil {
		t.Fatal(err)
	}
	r := newRecorder()
	res, err := r.preparer().prepare(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Repaired || res.Created {
		t.Fatalf("result %+v", res)
	}
	if !slices.Equal(r.chowned, []string{filepath.Join(st, "wal"), st}) || r.modeAt[st] != 0o700 {
		t.Fatalf("owner changes %v, mode of the state directory %#o", r.chowned, r.modeAt[st])
	}
}

// Root must not follow a link: it would give the target away.
func TestPrepareRefusesALink(t *testing.T) {
	dataDir := t.TempDir()
	oldLayout(t, dataDir)
	if err := os.Symlink("/etc/passwd", filepath.Join(dataDir, "wal", "link")); err != nil {
		t.Fatal(err)
	}
	r := newRecorder()
	_, err := r.preparer().prepare(dataDir)
	if err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("Prepare: %v", err)
	}
	for _, p := range r.chowned {
		if strings.HasSuffix(p, "link") {
			t.Errorf("changed the owner of %s", p)
		}
	}
}
