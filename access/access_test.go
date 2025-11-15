package access_test

import (
	"errors"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/carlmjohnson/be"
	"github.com/srerickson/ocfl-go"
	"github.com/srerickson/ocfl-go/digest"
	"github.com/srerickson/ocfl-services/access"
	"github.com/srerickson/ocfl-services/access/sqlite"
	"github.com/srerickson/ocfl-services/internal/testutil"
)

const fixtureObjectID = "ark:123/abc"

func TestService(t *testing.T) {
	_ = testService(t)
}

func TestRepo_SyncObject(t *testing.T) {

	t.Run("fixture", func(t *testing.T) {
		ctx := t.Context()
		r := testService(t)
		obj, err := r.SyncObject(ctx, fixtureObjectID)
		be.NilErr(t, err)
		be.Equal(t, fixtureObjectID, obj.ID())
	})

	t.Run("no found", func(t *testing.T) {
		ctx := t.Context()
		r := testService(t)
		_, err := r.SyncObject(ctx, "bad-id")
		be.Nonzero(t, err)
		be.True(t, errors.Is(err, access.ErrNotFound))
	})

	t.Run("refresh interval", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			r := testService(t)

			first, err := r.SyncObject(ctx, fixtureObjectID)
			be.NilErr(t, err)

			// not enough time to require a refresh
			time.Sleep(time.Second)

			second, err := r.SyncObject(ctx, fixtureObjectID)
			be.NilErr(t, err)

			// first and second have same indexed_at value
			be.True(t, first.IndexedAt().Equal(second.IndexedAt()))

			// trigger a refresh
			time.Sleep(access.RefreshInterval)

			third, err := r.SyncObject(ctx, fixtureObjectID)
			be.NilErr(t, err)

			diff := third.IndexedAt().Sub(first.IndexedAt())
			if diff < access.RefreshInterval {
				t.Error("indexed_at difference too small:", diff)
			}
		})
	})
}

func TestRepo_IndexRoot(t *testing.T) {
	t.Run("fixture", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			svc := testService(t)
			err := svc.IndexRoot(ctx)
			be.NilErr(t, err)
			time.Sleep(time.Second)
			syncedAt := time.Now()
			obj, err := svc.SyncObject(ctx, fixtureObjectID)
			be.NilErr(t, err)
			// The object was indexed during IndexRoot, not SyncObject
			be.True(t, obj.IndexedAt().Before(syncedAt))
		})
	})
}

func TestRepo_ReadVersionDir(t *testing.T) {

	t.Run("fixture", func(t *testing.T) {
		ctx := t.Context()
		svc := testService(t)
		got, err := svc.ReadVersionDir(ctx, fixtureObjectID, 1, ".")
		be.NilErr(t, err)
		want := []access.VersionDirEntry{
			&testStateDirEntry{
				name:    "a_file.txt",
				digest:  "43a43fe8a8a082d3b5343dfaf2fd0c8b8e370675b1f376e92e9994612c33ea255b11298269d72f797399ebb94edeefe53df243643676548f584fb8603ca53a0f",
				modVNum: 1,
				modtime: time.Date(2018, 12, 31, 18, 03, 04, 0, time.Local), // v1 created
				size:    20,
				isDir:   false,
			},
		}
		eqStateDirEntries(t, want, got)
	})

	t.Run("create and update object", func(t *testing.T) {
		ctx := t.Context()
		svc := testService(t)
		root := svc.Root()
		objID := "object-001"
		obj, err := root.NewObject(ctx, objID)
		be.NilErr(t, err)
		user := ocfl.User{Name: "test user name", Address: "test user address"}

		// v1
		v1Created := time.Date(2025, 1, 1, 12, 59, 0, 0, time.Local)
		content := map[string][]byte{
			"dir1/to_delete.txt": []byte("this file will be deleted"),
			"dir1/keep":          []byte("keep the directory"),
			"dir2/to_update.txt": []byte("this file will be updated"),
			"dir3/unchanged.txt": []byte("this file will stay the same"),
		}
		stage, err := ocfl.StageBytes(content, digest.SHA256)
		be.NilErr(t, err)
		_, err = obj.Update(ctx, stage, "version-1", user, ocfl.UpdateWithVersionCreated(v1Created))
		be.NilErr(t, err)

		// v2
		v2Created := v1Created.Add(24 * time.Hour)
		delete(content, "dir1/to_delete.txt")
		content["dir2/to_update.txt"] = []byte("this has been updated")
		content["dir4/new.txt"] = []byte("this is a new file")
		stage, err = ocfl.StageBytes(content, digest.SHA256)
		be.NilErr(t, err)
		_, err = obj.Update(ctx, stage, "version-2", user, ocfl.UpdateWithVersionCreated(v2Created))
		be.NilErr(t, err)

		t.Run("v2 root", func(t *testing.T) {
			got, err := svc.ReadVersionDir(ctx, objID, 2, ".")
			be.NilErr(t, err)
			want := []access.VersionDirEntry{
				&testStateDirEntry{name: "dir1", modVNum: 2, modtime: v2Created, isDir: true, size: 18},
				&testStateDirEntry{name: "dir2", modVNum: 2, modtime: v2Created, isDir: true, size: 21},
				&testStateDirEntry{name: "dir3", modVNum: 1, modtime: v1Created, isDir: true, size: 28},
				&testStateDirEntry{name: "dir4", modVNum: 2, modtime: v2Created, isDir: true, size: 18},
			}
			eqStateDirEntries(t, want, got)
		})

		t.Run("v1 dir1", func(t *testing.T) {
			got, err := svc.ReadVersionDir(ctx, objID, 1, "dir1")
			be.NilErr(t, err)
			want := []access.VersionDirEntry{
				&testStateDirEntry{name: "keep", modVNum: 1, modtime: v1Created, isDir: false, size: 18, digest: "9d9c02f9a18271f964a74db20eeabc711557aa4855c865a881ccd910d3593dd3"},
				&testStateDirEntry{name: "to_delete.txt", modVNum: 1, modtime: v1Created, isDir: false, size: 25, digest: "435ede928e96118f9e3d9fd4f7c44a03e0bc9b07e0ab658ec1c31e811695ed5f"},
			}
			eqStateDirEntries(t, want, got)
		})
	})

	t.Run("not a directory", func(t *testing.T) {
		ctx := t.Context()
		svc := testService(t)
		_, err := svc.ReadVersionDir(ctx, fixtureObjectID, 1, "a_file.txt")
		be.Nonzero(t, err)
		be.True(t, errors.Is(err, access.ErrNotFound))
	})

	t.Run("not existing", func(t *testing.T) {
		ctx := t.Context()
		svc := testService(t)
		_, err := svc.ReadVersionDir(ctx, fixtureObjectID, 1, "missing")
		be.Nonzero(t, err)
		be.True(t, errors.Is(err, access.ErrNotFound))
	})

}

func testService(t *testing.T) *access.Service {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_database.db")
	indexer, err := sqlite.NewDB(dbPath)
	if err != nil {
		t.Fatal("setting up test indexer: ", err)
	}
	t.Cleanup(func() {
		indexer.Close()
	})
	root := testutil.FixtureRootCopy(t, filepath.Join(`..`, `testdata`))
	rootName := "test-root"
	var logger *slog.Logger
	// logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	// 	Level: slog.LevelDebug,
	// }))
	return access.NewService(root, indexer, rootName, logger)
}

// testStateDirEntry is a simple implementation of StateDirEntry for testing
type testStateDirEntry struct {
	name    string
	digest  string
	modVNum int
	modtime time.Time
	size    int64
	isDir   bool
}

func (e *testStateDirEntry) Name() string       { return e.name }
func (e *testStateDirEntry) Digest() string     { return e.digest }
func (e *testStateDirEntry) ModVNum() int       { return e.modVNum }
func (e *testStateDirEntry) Modtime() time.Time { return e.modtime }
func (e *testStateDirEntry) Size() int64        { return e.size }
func (e *testStateDirEntry) HasSize() bool      { return e.Size() > 0 }
func (e *testStateDirEntry) IsDir() bool        { return e.isDir }

func eqStateDirEntries(t *testing.T, want, got []access.VersionDirEntry) {
	t.Helper()
	if !slices.EqualFunc(got, want, eqStateDirEntry) {
		maxLen := max(len(want), len(got))
		t.Error("unexpected result from ReadVersionDir")
		for i := range maxLen {
			if i >= len(want) {
				t.Logf("got[%d]:  name=%s digest=%s modVNum=%d "+
					"size=%d hasSize=%v isDir=%v (extra entry)",
					i, got[i].Name(), got[i].Digest(),
					got[i].ModVNum(), got[i].Size(),
					got[i].HasSize(), got[i].IsDir())
				continue
			}
			if i >= len(got) {
				t.Logf("want[%d]: name=%s digest=%s modVNum=%d "+
					"size=%d hasSize=%v isDir=%v (missing entry)",
					i, want[i].Name(), want[i].Digest(),
					want[i].ModVNum(), want[i].Size(),
					want[i].HasSize(), want[i].IsDir())
				continue
			}
			if !eqStateDirEntry(want[i], got[i]) {
				t.Logf("want[%d]: name=%s digest=%s modVNum=%d "+
					"modTime=%v size=%d hasSize=%v"+
					"isDir=%v",
					i, want[i].Name(), want[i].Digest(),
					want[i].ModVNum(), want[i].Modtime(),
					want[i].Size(), want[i].HasSize(),
					want[i].IsDir())
				t.Logf("got[%d]:  name=%s digest=%s modVNum=%d "+
					"modTime=%v size=%d hasSize=%v"+
					"isDir=%v",
					i, got[i].Name(), got[i].Digest(),
					got[i].ModVNum(), got[i].Modtime(),
					got[i].Size(), got[i].HasSize(),
					got[i].IsDir())
				continue
			}
		}
	}
}

func eqStateDirEntry(a, b access.VersionDirEntry) bool {
	return a.Name() == b.Name() &&
		a.Digest() == b.Digest() &&
		a.ModVNum() == b.ModVNum() &&
		a.Modtime().Equal(b.Modtime()) &&
		a.Size() == b.Size() &&
		a.HasSize() == b.HasSize() &&
		a.IsDir() == b.IsDir()
}
