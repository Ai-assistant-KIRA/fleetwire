// Package statedir prepares the agent's state directory in the snap,
// $SNAP_DATA/state, for the snap_daemon system user the daemon runs as.
//
// snapd creates $SNAP_DATA owned by root, and the snap's AppArmor profile
// allows writes below $SNAP_DATA but not to $SNAP_DATA itself, so not even
// root in the snap can give that directory to another user. The agent's
// identity/ and wal/ therefore live in a subdirectory that root creates and
// hands over: the install and post-refresh hooks do it before snapd starts
// the daemon, and the daemon's command chain checks it again at each start.
// Releases up to v0.1.1 ran the daemon as root and kept identity/ and wal/
// directly in $SNAP_DATA; Prepare moves them into the subdirectory.
//
// Root in a strict snap has CAP_CHOWN but neither CAP_DAC_OVERRIDE nor
// CAP_FOWNER: it cannot enter a directory that snap_daemon owns with mode
// 0700, and cannot change the mode of a file it does not own. Prepare
// therefore sets the mode before it changes the owner, changes the owner
// of a directory only after everything inside it, and does not enter a
// directory that already has the new owner.
package statedir

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/Ai-assistant-KIRA/fleetwire/internal/fsutil"
)

const (
	// Name is the state directory inside $SNAP_DATA.
	Name = "state"
	// partial is where Prepare assembles a new state directory. It is
	// renamed to Name only when everything is inside it, so a state
	// directory that exists is always complete.
	partial = "state.partial"
	// SnapDaemonID is the uid and the gid of the snap_daemon user, the
	// same on every system: snapd assigns them for system-usernames.
	SnapDaemonID = 584788
)

// legacy are the entries that releases up to v0.1.1 kept directly in
// $SNAP_DATA.
var legacy = []string{"identity", "wal"}

// Result says what Prepare changed. It is zero when the state directory
// was already in place.
type Result struct {
	// Created is set when the state directory did not exist.
	Created bool
	// Moved lists the old entries moved into the new state directory.
	Moved []string
	// Repaired is set when an existing state directory, or something in
	// it, still belonged to another owner, and Prepare changed that.
	Repaired bool
}

// Prepare makes dataDir/state exist and belong to uid:gid with mode 0700,
// moving identity/ and wal/ from dataDir into it first if they are there.
// It must run as root, before the daemon starts.
//
// Each move is a rename within one file system, so a crash or power cut at
// any point leaves every file either in its old place or in the new one,
// and running Prepare again finishes the job. When dataDir/state exists
// and the old entries are there too, Prepare changes nothing and returns
// an error: they hold two separate states, and mixing them could give two
// readings the same seq.
func Prepare(dataDir string, uid, gid int) (Result, error) {
	p := preparer{uid: uid, gid: gid, chown: chownSync, owned: ownedBy(uid)}
	return p.prepare(dataDir)
}

type preparer struct {
	uid, gid int
	// chown gives one file or directory to uid:gid.
	chown func(path string, uid, gid int) error
	// owned reports whether an entry already belongs to uid.
	owned func(fs.FileInfo) bool
}

func (p *preparer) prepare(dataDir string) (Result, error) {
	var res Result
	final := filepath.Join(dataDir, Name)
	tmp := filepath.Join(dataDir, partial)
	finalExists, err := isDir(final)
	if err != nil {
		return res, err
	}
	tmpExists, err := isDir(tmp)
	if err != nil {
		return res, err
	}
	var old []string
	for _, name := range legacy {
		switch _, err := os.Lstat(filepath.Join(dataDir, name)); {
		case err == nil:
			old = append(old, name)
		case !errors.Is(err, fs.ErrNotExist):
			return res, err
		}
	}
	if finalExists {
		other := tmp
		if len(old) > 0 {
			other = filepath.Join(dataDir, old[0])
		}
		if len(old) > 0 || tmpExists {
			return res, fmt.Errorf("%s and %s both exist and hold separate states; keep one and move the other out of %s",
				final, other, dataDir)
		}
		// Normally a no-op. It finishes the job if a crash lost owner
		// changes that were not yet on disk.
		res.Repaired, err = p.handOver(final)
		return res, err
	}
	// Every conflict is found before anything moves.
	for _, name := range old {
		dst := filepath.Join(tmp, name)
		if _, err := os.Lstat(dst); err == nil {
			return res, fmt.Errorf("%s and %s both exist and hold separate states; keep one and move the other away",
				filepath.Join(dataDir, name), dst)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return res, err
		}
	}
	if !tmpExists {
		if err := os.Mkdir(tmp, 0o700); err != nil {
			return res, fmt.Errorf("create %s: %w", tmp, err)
		}
	}
	for _, name := range old {
		dst := filepath.Join(tmp, name)
		if err := os.Rename(filepath.Join(dataDir, name), dst); err != nil {
			return res, fmt.Errorf("move %s into %s: %w", name, tmp, err)
		}
		res.Moved = append(res.Moved, name)
	}
	// The renames must be on disk before the owner changes and the final
	// rename build on them.
	for _, d := range []string{tmp, dataDir} {
		if err := fsutil.SyncDir(d); err != nil {
			return res, err
		}
	}
	if _, err := p.handOver(tmp); err != nil {
		return res, err
	}
	if err := os.Rename(tmp, final); err != nil {
		return res, fmt.Errorf("rename %s to %s: %w", tmp, final, err)
	}
	if err := fsutil.SyncDir(dataDir); err != nil {
		return res, err
	}
	res.Created = true
	return res, nil
}

// handOver sets the mode of dir to 0700 and gives it, with everything in
// it, to uid:gid. It reports whether anything changed.
func (p *preparer) handOver(dir string) (bool, error) {
	fi, err := os.Lstat(dir)
	if err != nil {
		return false, err
	}
	if p.owned(fi) {
		return false, nil
	}
	// The mode first: once root no longer owns dir, it cannot change it.
	if fi.Mode().Perm() != 0o700 {
		if err := os.Chmod(dir, 0o700); err != nil { //nolint:gosec // G302: a directory, which needs the x bit; owner only
			return false, fmt.Errorf("chmod %s: %w", dir, err)
		}
	}
	return true, p.chownTree(dir)
}

// chownTree gives path and everything below it to uid:gid, the entries of
// a directory before the directory itself. A directory that already has
// the owner is not entered: root may be unable to, and the order means
// everything inside it was done first.
func (p *preparer) chownTree(path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if p.owned(fi) {
		return nil
	}
	// A link is refused rather than followed: root would give its target
	// away, wherever it points.
	if !fi.IsDir() && !fi.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file or a directory; the state directory holds only those", path)
	}
	if fi.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("give %s to uid %d: %w", path, p.uid, err)
		}
		for _, e := range entries {
			if err := p.chownTree(filepath.Join(path, e.Name())); err != nil {
				return err
			}
		}
	}
	return p.chown(path, p.uid, p.gid)
}

func isDir(path string) (bool, error) {
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !fi.IsDir() {
		return false, fmt.Errorf("%s is not a directory", path)
	}
	return true, nil
}

// ownedBy compares the owner only. Prepare sets owner and group together,
// and an entry the user owns is the daemon's, whatever its group.
func ownedBy(uid int) func(fs.FileInfo) bool {
	return func(fi fs.FileInfo) bool {
		st, ok := fi.Sys().(*syscall.Stat_t)
		return ok && int(st.Uid) == uid
	}
}

// chownSync changes the owner through a descriptor opened without
// following links, and fsyncs it so the change is on disk before the
// directory that holds the entry changes hands. Root cannot open a file
// of mode 0600 to fsync it any more once it belongs to another user.
func chownSync(path string, uid, gid int) error {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("give %s to uid %d: %w", path, uid, err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Chown(uid, gid); err != nil {
		return fmt.Errorf("give %s to uid %d: %w", path, uid, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", path, err)
	}
	return nil
}
