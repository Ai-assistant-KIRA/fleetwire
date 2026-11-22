package fsutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadPrivateRefusesFilesOthersCanAccess(t *testing.T) {
	dir := t.TempDir()
	for perm, ok := range map[os.FileMode]bool{
		0o400: true, 0o600: true, 0o700: true,
		0o640: false, 0o604: false, 0o610: false, 0o644: false,
	} {
		path := filepath.Join(dir, perm.String())
		if err := os.WriteFile(path, []byte("pa55word"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, perm); err != nil {
			t.Fatal(err)
		}
		data, err := ReadPrivate(path)
		if ok && (err != nil || string(data) != "pa55word") {
			t.Errorf("ReadPrivate(%#o) = %q, %v; want the content", perm, data, err)
		}
		if !ok {
			if err == nil {
				t.Errorf("ReadPrivate(%#o) accepted the file", perm)
			} else if strings.Contains(err.Error(), "pa55word") {
				t.Errorf("ReadPrivate(%#o) error shows the content: %v", perm, err)
			}
		}
	}
	if _, err := ReadPrivate(filepath.Join(dir, "absent")); err == nil {
		t.Error("ReadPrivate of a missing file succeeded")
	}
}
