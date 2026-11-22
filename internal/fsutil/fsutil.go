// Package fsutil holds the file operations that need care: writes that a
// crash or power cut leaves either old or new, never a mix, and reads of
// files that hold secrets.
package fsutil

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ReadPrivate reads a file that holds a secret. It refuses a file its group
// or other users can access, so a key or password on a misconfigured mount
// fails at start instead of being used. The mode is checked on the opened
// file, so the file read is the file checked.
func ReadPrivate(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("%s has mode %#o; it must not be accessible by group or others (use 0400 or 0600)", path, perm)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return data, nil
}

// WriteAtomic replaces path with data: write a temp file in the same
// directory, fsync it, rename it over path, fsync the directory.
func WriteAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", path, err)
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if err := f.Chmod(perm); err != nil {
		_ = f.Close()
		return fmt.Errorf("chmod %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename to %s: %w", path, err)
	}
	ok = true
	return SyncDir(dir)
}

// SyncDir fsyncs a directory so that renames and new names in it survive a
// power cut.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("close dir %s: %w", dir, err)
	}
	return nil
}
