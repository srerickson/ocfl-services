package testutil

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/srerickson/ocfl-go"
	"github.com/srerickson/ocfl-go/fs/local"
)

// make a copy of the reg-extension-dir-root fixture root in a temporary
// directory and return the *ocfl.Root for testing.
func FixtureRootCopy(t *testing.T, testData string) *ocfl.Root {
	t.Helper()
	fixtureRoot := `reg-extension-dir-root`
	fsys := LocalFS(t, filepath.Join(testData, fixtureRoot))
	root, err := ocfl.NewRoot(t.Context(), fsys, fixtureRoot)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// make a temporary local.FS for writing with contents of srcdir copied into it.
// The srcDirs are copied into directories in the local.FS with the same base
// name as the source. If a srcDir is "../../testdata/fixture-1", use
// fsys.OpenFile(`fixture-1/...`) to access the fixture content.
func LocalFS(t *testing.T, srcDirs ...string) *local.FS {
	t.Helper()
	tmpDir := t.TempDir()
	for _, srcDir := range srcDirs {
		dst := filepath.Join(tmpDir, path.Base(srcDir))
		if err := os.CopyFS(dst, os.DirFS(srcDir)); err != nil {
			t.Fatal(err)
		}
	}
	fsys, err := local.NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	return fsys
}

// TestDatabase

func DigestSHA256(b []byte) string {
	h := sha256.New()
	if _, err := h.Write(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}
