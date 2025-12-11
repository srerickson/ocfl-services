package ocflite_test

import (
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/srerickson/ocfl-services/internal/ocflite"
	"github.com/srerickson/ocfl-services/internal/testutil"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// newConn returns an a connection to an in-memory sqlite database for testing
func newConn() (*sqlite.Conn, error) {
	conn, err := sqlite.OpenConn(":memory:", sqlite.OpenReadWrite|sqlite.OpenCreate)
	if err != nil {
		return nil, err
	}
	if err := ocflite.Migrate(conn); err != nil {
		return nil, err
	}
	return conn, err
}

// testConn returns sqlite connection for use in a test. The connection is
// closed as part of the test's Cleanup().
func testConn(t *testing.T) *sqlite.Conn {
	conn, err := newConn()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestObject(t *testing.T) {
	conn := testConn(t)
	rootName := "test"
	numObjects := 10
	objInputs := make([]*ocflite.Object, numObjects)
	for i := range numObjects {
		id := fmt.Sprintf("object-%d", i)
		objInputs[i] = createTestObjectWithContent(t, conn, rootName, id,
			map[string]string{
				"file-id": id,
				"file1":   "content-1",
			},
			map[string]string{
				"file1": "content-1",
				"file2": "content-2",
			},
			map[string]string{
				"new_file1": "content-1",
				"file2":     "new content-2",
			},
		)
	}
	count, err := ocflite.CountObjects(conn, rootName)
	if err != nil {
		t.Fatal("counting objects:", err)
	}
	if count != numObjects {
		t.Fatal("unexpected count: got", count)
	}

	t.Run("GetObjectBrief", func(t *testing.T) {
		for i := range numObjects {
			objInput := objInputs[i]
			objID := objInput.ID
			got, err := ocflite.GetObjectBrief(conn, rootName, objID)
			if err != nil {
				t.Fatal(err)
			}
			expect := ocflite.ObjectBrief{
				ID:              objID,
				StoragePath:     objInput.StoragePath,
				DigestAlgorithm: objInput.DigestAlgorithm,
				InventoryDigest: objInput.InventoryDigest,
				Head:            len(objInput.Versions),
				Vpadding:        objInput.Vpadding,
				CreatedAt:       objInput.Versions[0].Created,
				UpdatedAt:       objInput.Versions[2].Created,
				IndexedAt:       got.IndexedAt, // ignored
			}
			if *got != expect {
				t.Log("got   :", *got)
				t.Log("expect:", expect)
				t.Error("didn't return expected result")
			}
			if got.IndexedAt.IsZero() {
				t.Error("indexed_at is zero value")
			}
		}
	})

	t.Run("GetObjectBriefByPath", func(t *testing.T) {
		for i := range numObjects {
			objInput := objInputs[i]
			objID := objInput.ID
			got, err := ocflite.GetObjectBriefByPath(conn, rootName,
				objInput.StoragePath)
			if err != nil {
				t.Fatal(err)
			}
			expect := ocflite.ObjectBrief{
				ID:              objID,
				StoragePath:     objInput.StoragePath,
				DigestAlgorithm: objInput.DigestAlgorithm,
				InventoryDigest: objInput.InventoryDigest,
				Head:            len(objInput.Versions),
				Vpadding:        objInput.Vpadding,
				CreatedAt:       objInput.Versions[0].Created,
				UpdatedAt:       objInput.Versions[2].Created,
				IndexedAt:       got.IndexedAt, // ignored
			}
			if *got != expect {
				t.Log("got   :", *got)
				t.Log("expect:", expect)
				t.Error("didn't return expected result")
			}
			if got.IndexedAt.IsZero() {
				t.Error("indexed_at is zero value")
			}
		}
	})

	t.Run("GetObjectBriefByPath not found", func(t *testing.T) {
		_, err := ocflite.GetObjectBriefByPath(conn, rootName,
			"nonexistent/path")
		if err == nil {
			t.Error("expected error for nonexistent path")
		}
		if !errors.Is(err, ocflite.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("object files", func(t *testing.T) {
		for i := range numObjects {
			objInput := objInputs[i]
			objID := objInput.ID
			gotPaths, err := ocflite.ListObjectFiles(conn, rootName, objID)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(gotPaths, objInput.Manifest.AllPaths()) {
				t.Errorf("unexpected object file digests: %v", gotPaths)
			}
		}
	})

	t.Run("object file with size", func(t *testing.T) {
		for i := range numObjects {
			objInput := objInputs[i]
			objID := objInput.ID
			gotPaths, err := ocflite.ListObjectFiles(conn, rootName, objID)
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range gotPaths {
				file, err := ocflite.GetObjectFile(conn, rootName, objID, path)
				if err != nil {
					t.Fatal(err)
				}
				if !file.HasSize || file.Size < 1 {
					t.Error("file without size: ", file.ContentPath)
				}
			}
		}
	})
}

func TestUnsetObject(t *testing.T) {
	conn := testConn(t)
	rootName := "test"
	// create two objects with the same content, delete one of them.
	objID1, objID2 := "object-01", "object-02"
	content := map[string]string{
		"file": "content",
	}
	createTestObjectWithContent(t, conn, rootName, objID1, content)
	countsWithOneObject := map[string]int{
		"ocfl_objects":              0,
		"ocfl_object_files":         0,
		"ocfl_object_versions":      0,
		"ocfl_object_version_files": 0,
	}
	for tableName := range countsWithOneObject {
		count := countTable(t, conn, tableName)
		if count < 1 {
			t.Error("expected at least 1 row in table", tableName)
		}
		countsWithOneObject[tableName] = count
	}
	// create a second object with same content and confirm additional rows
	createTestObjectWithContent(t, conn, rootName, objID2, content)
	for tableName := range countsWithOneObject {
		count := countTable(t, conn, tableName)
		if count <= countsWithOneObject[tableName] {
			t.Error("expected additinal rows in table", tableName)
		}
	}
	if err := ocflite.UnsetObject(conn, rootName, objID2); err != nil {
		t.Fatal(err)
	}
	// new rown counts should match countsWithOneObject
	for tableName, expectCount := range countsWithOneObject {
		gotCount := countTable(t, conn, tableName)
		if gotCount != expectCount {
			t.Errorf("wrong count for %s, got=%d, expect=%d", tableName, gotCount, expectCount)
		}
	}
	// delete first object
	if err := ocflite.UnsetObject(conn, rootName, objID1); err != nil {
		t.Fatal(err)
	}
	// expect zero
	for name := range countsWithOneObject {
		expectCount := 0
		gotCount := countTable(t, conn, name)
		if gotCount != expectCount {
			t.Errorf("wrong count for %s, got=%d, expect=%d", name, gotCount, expectCount)
		}
	}

	t.Run("missing object", func(t *testing.T) {
		err := ocflite.UnsetObject(conn, rootName, "missing")
		if err != nil {
			t.Fatal("expected no error")
		}
	})

	t.Run("missing root", func(t *testing.T) {
		createTestObjectWithContent(t, conn, rootName, objID1, content)
		err := ocflite.UnsetObject(conn, "missing", objID1)
		if err != nil {
			t.Fatal("expected no error")
		}
	})
}

func TestObjectVersions(t *testing.T) {
	conn := testConn(t)
	rootName := "root-01"
	objID := "object-01"
	runVersionTest := func(t *testing.T, states []ocflite.PathMap) {
		t.Helper()
		for i := range states {
			head := i + 1
			t.Run(fmt.Sprintf("head-v%d", head), func(t *testing.T) {
				// create object with versions up to head
				createTestObject(t, conn, rootName, objID, states[:head]...)
				versions, err := ocflite.ListVersions(conn, rootName, objID)
				if err != nil {
					t.Error(err)
				}
				if l := len(versions); l != head {
					t.Errorf("wrong number of versions, got=%d, exp=%d", l, head)
				}
				for j := range head {
					prevState, err := ocflite.GetVersionState(conn, rootName, objID, j+1)
					if err != nil {
						t.Errorf("getting version %d state: %v", j+1, err)
					}
					if prevState.Hash() != versions[j].StateDigest {
						t.Errorf("unexpected state digest for version %d", j+1)
					}
					if !maps.Equal(prevState.PathMap(), states[j]) {
						t.Logf("got v%d state: %v", j+1, prevState)
						t.Logf("exp v%d state: %v", j+1, states[j])
						t.Error("state retrieved doesn't match expected value")
					}
				}
			})
		}
	}

	t.Run("initial version history", func(t *testing.T) {
		runVersionTest(t, []ocflite.PathMap{
			{
				"README.txt":        "digest1",
				"src/main.go":       "digest2",
				"src/utils/lib1.go": "digest3",
				"src/utils/lib2.go": "digest4",
				"docs/manual.md":    "digest5",
			},
			{
				"README.txt":        "digest1",
				"src/main.go":       "digest2",
				"src/utils/lib1.go": "digest3",
				// "src/utils/lib2.go": "digest4", // deleted
				"docs/outline.md": "digest5", // rename
			},
			{
				"README.txt":       "digest1",
				"src/main.go":      "digest6", // changed
				"src/utils/lib.go": "digest3",
				"config.json":      "digest5",
				"docs/manual.md":   "digest7", // file restored with new content
			},
			{
				//	everything deleted
			},
			{
				"README.txt":       "digest1", // everything restored
				"src/main.go":      "digest6",
				"src/utils/lib.go": "digest3",
				"config.json":      "digest5",
				"docs/manual.md":   "digest7",
			},
		})
	})

	t.Run("rebase version history", func(t *testing.T) {
		runVersionTest(t, []ocflite.PathMap{
			{"README.txt": "digest8"},
		})
	})

}

func TestReadVersionDir(t *testing.T) {

	t.Run("multiple objects same file", func(t *testing.T) {
		conn := testConn(t)
		rootName := "root"
		for i := range 9 {
			objID := fmt.Sprintf("object-%d", i)
			content := map[string]string{
				"index.txt": fmt.Sprintf("content-%d", i), // 9 conts
			}
			createTestObjectWithContent(t, conn, rootName, objID, content)
		}
		got, err := ocflite.ReadVersionDir(conn, rootName, "object-2", 1, ".")
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		if len(got) != 1 {
			t.Fatal("got wrong len:", len(got))
		}
		if s := got[0].Size; s != 9 {
			t.Fatal("got wrong size:", s)
		}
	})

	type test struct {
		versions  []ocflite.PathMap
		sizes     map[string]int64
		version   int
		directory string
		want      []*ocflite.VersionDirEntry
		wantErr   bool
	}
	tests := map[string]test{
		"root-v1": {
			versions: []ocflite.PathMap{
				{
					"readme.txt":        "digest1",
					"src/utils/lib1.go": "digest2",
					"src/utils/lib2.go": "digest3",
				},
			},
			version:   1,
			directory: ".",
			want: []*ocflite.VersionDirEntry{
				{Name: "readme.txt", ModVNum: 1, Digest: "digest1"},
				{Name: "src", ModVNum: 1, IsDir: true},
			},
		},
		"root-v1-size": {
			versions: []ocflite.PathMap{
				{
					"readme.txt":        "digest1",
					"src/utils/lib1.go": "digest2",
					"src/utils/lib2.go": "digest3",
				},
			},
			sizes: map[string]int64{
				"digest1": 1,
				"digest2": 2,
				"digest3": 3,
			},
			version:   1,
			directory: ".",
			want: []*ocflite.VersionDirEntry{
				{Name: "readme.txt", ModVNum: 1, Digest: "digest1", Size: 1, HasSize: true},
				{Name: "src", ModVNum: 1, IsDir: true, Size: 5, HasSize: true},
			},
		},
		"root-v1-empty": {
			versions:  []ocflite.PathMap{{}},
			version:   1,
			directory: ".",
			want:      []*ocflite.VersionDirEntry{},
		},
		"root-v2-empty": {
			versions: []ocflite.PathMap{
				{
					"readme.txt":        "digest1",
					"src/utils/lib1.go": "digest2",
					"src/utils/lib2.go": "digest3",
				},
				{},
			},
			version:   2,
			directory: ".",
			want:      []*ocflite.VersionDirEntry{},
		},
		"root-v2-deleted-file-in-sub-dir": {
			versions: []ocflite.PathMap{
				{
					"readme.txt":        "digest1",
					"src/utils/lib1.go": "digest2",
					"src/utils/lib2.go": "digest3",
				},
				{
					"readme.txt":        "digest1",
					"src/utils/lib1.go": "digest2",
					// "src/utils/lib2.go deleted
				},
			},
			version:   2,
			directory: "",
			want: []*ocflite.VersionDirEntry{
				{Name: "readme.txt", Digest: "digest1", ModVNum: 1},
				{Name: "src", ModVNum: 2, IsDir: true},
			},
		},
		"src-v2-deleted-file-in-sub-dir": {
			versions: []ocflite.PathMap{
				{
					"readme.txt":        "digest1",
					"src/utils/lib1.go": "digest2",
					"src/utils/lib2.go": "digest3",
				},
				{
					"readme.txt":        "digest1",
					"src/utils/lib1.go": "digest2",
					// "src/utils/lib2.go deleted
				},
			},
			version:   2,
			directory: "src",
			want: []*ocflite.VersionDirEntry{
				{Name: "utils", ModVNum: 2, IsDir: true},
			},
		},
		"src-v2-renamed-dir": {
			versions: []ocflite.PathMap{
				{
					"readme.txt":        "digest1",
					"src/utils/lib1.go": "digest2",
					"src/utils/lib2.go": "digest3",
				},
				{
					"readme.txt":         "digest1",
					"src/utils2/lib1.go": "digest2",
					"src/utils2/lib2.go": "digest3",
				},
			},
			version:   2,
			directory: "src",
			want: []*ocflite.VersionDirEntry{
				{Name: "utils2", ModVNum: 2, IsDir: true},
			},
		},
		"root-v2-new-file-in-subdir": {
			versions: []ocflite.PathMap{
				{
					"src/utils/lib1.go": "digest2",
				},
				{
					"src/utils2/lib1.go": "digest2",
					"src/utils2/lib2.go": "digest3",
				},
			},
			version:   2,
			directory: ".",
			want: []*ocflite.VersionDirEntry{
				{Name: "src", ModVNum: 2, IsDir: true},
			},
		},
		"dir-becomes-file": {
			versions: []ocflite.PathMap{
				{
					"src/lib1.go": "digest2",
					"src/lib2.go": "digest3",
				},
				{
					"src": "digest1",
				},
			},
			version:   2,
			directory: ".",
			want: []*ocflite.VersionDirEntry{
				{Name: "src", ModVNum: 2, IsDir: false, Digest: "digest1"},
			},
		},
		"root-v1-not-found": {
			versions: []ocflite.PathMap{
				{"readme.txt": "digest1"},
			},
			version:   1,
			directory: "missing",
			wantErr:   true,
		},
		"file-not-dir": {
			versions: []ocflite.PathMap{
				{"sub/readme.txt": "digest1"},
			},
			version:   1,
			directory: "sub/readme.txt",
			wantErr:   true,
		},
	}
	for tname, tt := range tests {
		t.Run(tname, func(t *testing.T) {
			conn := testConn(t)
			rootName := "root-01"
			objID := "object-01"
			createTestObject(t, conn, rootName, objID, tt.versions...)
			for digest, size := range tt.sizes {
				err := ocflite.SetObjectFileSize(conn, rootName, objID, digest, size)
				if err != nil {
					t.Fatal("setting size", err)
				}
			}
			entries, err := ocflite.ReadVersionDir(conn, rootName, objID, tt.version, tt.directory)
			if err != nil && !tt.wantErr {
				t.Fatal("unexpected error:", err)
			}
			if err == nil && tt.wantErr {
				for _, entry := range entries {
					t.Log("got unexpectd entry:", *entry)
				}
				t.Fatal("expected error but got none")
			}
			for _, want := range tt.want {
				// the modtime we expect is based on the modvnum
				want.Modtime = versionCreated(want.ModVNum)
			}
			dirEntriesEqual(t, tt.want, entries)
			for _, entry := range entries {
				if entry.IsDir {
					continue
				}
				// call stat for each file:check file info values match dir
				// entry values.
				fileName := path.Join(tt.directory, entry.Name)
				stat, err := ocflite.StatVersionFile(conn, rootName, objID, tt.version, fileName)
				if err != nil {
					t.Errorf("stat %s: %v", fileName, err)
				}
				if fileInfoAsDirEntry(stat) != *entry {
					t.Logf("from stat    %s: %v", fileName, stat)
					t.Logf("from readdir %s: %v", fileName, entry)
					t.Error("mismatch b/w readdir and stat for", fileName)
				}
			}
		})
	}
}

func TestListObjects(t *testing.T) {
	conn := testConn(t)
	rootName := "test-root"

	// Create 100 objects
	numObjects := 100
	for i := range numObjects {
		objID := fmt.Sprintf("object-%03d", i)
		createTestObject(t, conn, rootName, objID,
			ocflite.PathMap{"file.txt": fmt.Sprintf("digest-%d", i)},
		)
	}
	testPageSize := func(t *testing.T, pageSize int) {
		// Test pagination through all objects
		allObjects := make([]*ocflite.ObjectBrief, 0, numObjects)

		for offset := 0; offset < numObjects; offset += pageSize {
			result, err := ocflite.ListObjects(conn, rootName, pageSize, offset)
			if err != nil {
				t.Fatalf("ListObjects failed at offset %d: %v", offset, err)
			}
			allObjects = append(allObjects, result...)
			// Check that we got the right number of objects for this page
			expectedPageSize := pageSize
			if offset+pageSize > numObjects {
				expectedPageSize = numObjects - offset
			}
			if len(result) != expectedPageSize {
				t.Errorf("at offset %d: got %d objects, want %d",
					offset, len(sqlite.AuthResultDeny.String()), expectedPageSize)
			}
		}

		// Verify we got all objects
		if len(allObjects) != numObjects {
			t.Errorf("total objects retrieved = %d, want %d",
				len(allObjects), numObjects)
		}

		// Verify no duplicates and all IDs are present
		seenIDs := make(map[string]bool)
		for _, obj := range allObjects {
			if seenIDs[obj.ID] {
				t.Errorf("duplicate object ID found: %s", obj.ID)
			}
			seenIDs[obj.ID] = true
		}

		// Verify all expected IDs are present
		for i := range numObjects {
			expectedID := fmt.Sprintf("object-%03d", i)
			if !seenIDs[expectedID] {
				t.Errorf("missing object ID: %s", expectedID)
			}
		}
	}

	t.Run("pageSize-1", func(t *testing.T) { testPageSize(t, 1) })
	t.Run("pageSize-10", func(t *testing.T) { testPageSize(t, 10) })
	t.Run("pageSize-11", func(t *testing.T) { testPageSize(t, 11) })
	t.Run("pageSize-10", func(t *testing.T) { testPageSize(t, 101) })
	t.Run("pageSize-101", func(t *testing.T) { testPageSize(t, 101) })
}

func TestStatVersionFile(t *testing.T) {
	type test struct {
		versions []ocflite.PathMap
		sizes    map[string]int64
		version  int
		name     string
		want     *ocflite.VersionFileInfo
		wantErr  bool
	}

	tests := map[string]test{
		"v1-file": {
			versions: []ocflite.PathMap{
				{
					"dir/readme.txt":  "digest1",
					"dir2/readme.txt": "digest2",
				},
			},
			version: 1,
			name:    "dir/readme.txt",
			want: &ocflite.VersionFileInfo{
				Path:        "dir/readme.txt",
				Digest:      "digest1",
				ContentPath: "v1/content/dir/readme.txt",
				ModVnum:     1,
			},
		},
		"deleted-restored-file": {
			versions: []ocflite.PathMap{
				{"readme.txt": "digest1"},
				{},
				{"readme.txt": "digest2"},
				{
					"readme.txt":  "digest2",
					"another.txt": "digest3",
				},
			},
			sizes: map[string]int64{
				"digest1": 1,
				"digest2": 2,
				"digest3": 3,
			},
			version: 4,
			name:    "readme.txt",
			want: &ocflite.VersionFileInfo{
				Path:        "readme.txt",
				Digest:      "digest2",
				ContentPath: "v3/content/readme.txt",
				ModVnum:     3,
				Size:        2,
				HasSize:     true,
			},
		},
		"deleted-file": {
			versions: []ocflite.PathMap{
				{"readme.txt": "digest1"},
				{},
			},
			version: 2,
			name:    "readme.txt",
			wantErr: true,
		},

		"not-created-yet": {
			versions: []ocflite.PathMap{
				{},
				{"readme.txt": "digest1"},
			},
			version: 1,
			name:    "readme.txt",
			wantErr: true,
		},
		"version-not-exist": {
			versions: []ocflite.PathMap{
				{"readme.txt": "digest1"},
			},
			version: 2,
			name:    "readme.txt",
			wantErr: true,
		},
	}
	for tname, tt := range tests {
		t.Run(tname, func(t *testing.T) {
			conn := testConn(t)
			rootName := "root-01"
			objID := "object-01"
			createTestObject(t, conn, rootName, objID, tt.versions...)
			for digest, size := range tt.sizes {
				err := ocflite.SetObjectFileSize(conn, rootName, objID, digest, size)
				if err != nil {
					t.Fatal("setting size", err)
				}
			}
			gotInfo, err := ocflite.StatVersionFile(conn, rootName, objID, tt.version, tt.name)
			if err != nil && !tt.wantErr {
				t.Fatal("unexpected error:", err)
			}
			if err == nil && tt.wantErr {
				t.Fatal("expected error but got none")
			}
			if tt.want != nil {
				tt.want.Modtime = versionCreated(tt.want.ModVnum)
			}
			if tt.want != nil && *tt.want != *gotInfo {
				t.Logf("got : %v", *gotInfo)
				t.Logf("want: %v", *tt.want)
				t.Error("didn't get expected file info for", tt.name)
			}

		})
	}
}

func TestTouchObject(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := testConn(t)
		rootName := "test-root"
		objID := "test-object"
		createTestObject(t, conn, rootName, objID,
			ocflite.PathMap{"file.txt": "digest-1"},
		)
		initialBrief, err := ocflite.GetObjectBrief(conn, rootName, objID)
		if err != nil {
			t.Fatalf("Failed to get initial object brief: %v", err)
		}
		if initialBrief.IndexedAt.IsZero() {
			t.Fatal("Initial indexed_at is zero value")
		}
		// Wait a few seconds using synctest's time control
		time.Sleep(3 * time.Second)
		// Touch the object to update indexed_at
		updatedBrief, err := ocflite.TouchObject(conn, rootName, objID)
		if err != nil {
			t.Fatalf("Failed to touch object: %v", err)
		}
		// Verify the difference is at least 3 seconds
		diff := updatedBrief.IndexedAt.Sub(initialBrief.IndexedAt)
		if diff < 3*time.Second {
			t.Errorf("indexed_at difference too small: got %v, "+
				"want at least 3s", diff)
		}
	})
}

func TestGetVersionChanges(t *testing.T) {
	conn := testConn(t)
	rootName := "test"

	t.Run("basic changes", func(t *testing.T) {
		// v1: {a.txt: content1, b.txt: content2}
		// v2: {a.txt: content1, b.txt: content3, c.txt: content4}
		// Expected: b.txt modified, c.txt added
		obj := createTestObjectWithContent(t, conn, rootName, "obj1",
			map[string]string{
				"a.txt": "content1",
				"b.txt": "content2",
			},
			map[string]string{
				"a.txt": "content1",
				"b.txt": "content3",
				"c.txt": "content4",
			},
		)

		changes, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 1, 2)
		if err != nil {
			t.Fatal(err)
		}

		if changes.FromVnum != 1 || changes.ToVnum != 2 {
			t.Errorf("got FromVersion=%d ToVersion=%d, want 1, 2", changes.FromVnum, changes.ToVnum)
		}

		if len(changes.Changes) != 2 {
			t.Fatalf("got %d changes, want 2", len(changes.Changes))
		}

		// Results should be sorted by path
		if changes.Changes[0].Path != "b.txt" {
			t.Errorf("got first change path=%q, want 'b.txt'", changes.Changes[0].Path)
		}
		if changes.Changes[0].ModType != ocflite.FileModified {
			t.Errorf("got first change type=%v, want ChangeTypeModified", changes.Changes[0].ModType)
		}

		if changes.Changes[1].Path != "c.txt" {
			t.Errorf("got second change path=%q, want 'c.txt'", changes.Changes[1].Path)
		}
		if changes.Changes[1].ModType != ocflite.FileAdded {
			t.Errorf("got second change type=%v, want ChangeTypeAdded", changes.Changes[1].ModType)
		}
	})

	t.Run("from version 0", func(t *testing.T) {
		// v0 (no version) -> v2: all files in v2 should be added
		obj := createTestObjectWithContent(t, conn, rootName, "obj2",
			map[string]string{
				"a.txt": "content1",
			},
			map[string]string{
				"a.txt": "content1",
				"b.txt": "content2",
			},
		)

		changes, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 0, 2)
		if err != nil {
			t.Fatal(err)
		}

		if changes.FromVnum != 0 || changes.ToVnum != 2 {
			t.Errorf("got FromVersion=%d ToVersion=%d, want 0, 2", changes.FromVnum, changes.ToVnum)
		}

		if len(changes.Changes) != 2 {
			t.Fatalf("got %d changes, want 2", len(changes.Changes))
		}

		// All should be added
		for i, change := range changes.Changes {
			if change.ModType != ocflite.FileAdded {
				t.Errorf("change %d: got type=%v, want ChangeTypeAdded", i, change.ModType)
			}
		}

		// Check sorted
		if changes.Changes[0].Path != "a.txt" || changes.Changes[1].Path != "b.txt" {
			t.Errorf("changes not sorted correctly: got [%q, %q]", changes.Changes[0].Path, changes.Changes[1].Path)
		}
	})

	t.Run("all files added", func(t *testing.T) {
		// v1: empty, v2: has files
		obj := createTestObjectWithContent(t, conn, rootName, "obj3",
			map[string]string{},
			map[string]string{
				"a.txt": "content1",
				"b.txt": "content2",
			},
		)

		changes, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 1, 2)
		if err != nil {
			t.Fatal(err)
		}

		if len(changes.Changes) != 2 {
			t.Fatalf("got %d changes, want 2", len(changes.Changes))
		}

		for i, change := range changes.Changes {
			if change.ModType != ocflite.FileAdded {
				t.Errorf("change %d: got type=%v, want ChangeTypeAdded", i, change.ModType)
			}
		}
	})

	t.Run("all files deleted", func(t *testing.T) {
		// v1: has files, v2: empty
		obj := createTestObjectWithContent(t, conn, rootName, "obj4",
			map[string]string{
				"a.txt": "content1",
				"b.txt": "content2",
			},
			map[string]string{},
		)

		changes, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 1, 2)
		if err != nil {
			t.Fatal(err)
		}

		if len(changes.Changes) != 2 {
			t.Fatalf("got %d changes, want 2", len(changes.Changes))
		}

		for i, change := range changes.Changes {
			if change.ModType != ocflite.FileDeleted {
				t.Errorf("change %d: got type=%v, want ChangeTypeDeleted", i, change.ModType)
			}
		}
	})

	t.Run("same version", func(t *testing.T) {
		// v2 -> v2: should have no changes
		obj := createTestObjectWithContent(t, conn, rootName, "obj5",
			map[string]string{
				"a.txt": "content1",
			},
			map[string]string{
				"a.txt": "content1",
				"b.txt": "content2",
			},
		)

		changes, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 2, 2)
		if err != nil {
			t.Fatal(err)
		}

		if len(changes.Changes) != 0 {
			t.Errorf("got %d changes, want 0", len(changes.Changes))
		}

		if changes.FromVnum != 2 || changes.ToVnum != 2 {
			t.Errorf("got FromVersion=%d ToVersion=%d, want 2, 2", changes.FromVnum, changes.ToVnum)
		}
	})

	t.Run("reverse comparison", func(t *testing.T) {
		// v2 -> v1: added and deleted should be swapped
		obj := createTestObjectWithContent(t, conn, rootName, "obj6",
			map[string]string{
				"a.txt": "content1",
			},
			map[string]string{
				"b.txt": "content2",
			},
		)

		// Forward: v1 -> v2
		forward, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 1, 2)
		if err != nil {
			t.Fatal(err)
		}

		// Reverse: v2 -> v1
		reverse, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 2, 1)
		if err != nil {
			t.Fatal(err)
		}

		if len(forward.Changes) != 2 || len(reverse.Changes) != 2 {
			t.Fatalf("got %d forward and %d reverse changes, want 2, 2", len(forward.Changes), len(reverse.Changes))
		}

		// In forward: a.txt deleted, b.txt added
		// In reverse: a.txt added, b.txt deleted
		for _, change := range forward.Changes {
			if change.Path == "a.txt" && change.ModType != ocflite.FileDeleted {
				t.Errorf("forward: a.txt should be deleted")
			}
			if change.Path == "b.txt" && change.ModType != ocflite.FileAdded {
				t.Errorf("forward: b.txt should be added")
			}
		}

		for _, change := range reverse.Changes {
			if change.Path == "a.txt" && change.ModType != ocflite.FileAdded {
				t.Errorf("reverse: a.txt should be added")
			}
			if change.Path == "b.txt" && change.ModType != ocflite.FileDeleted {
				t.Errorf("reverse: b.txt should be deleted")
			}
		}
	})

	t.Run("skip intermediate versions", func(t *testing.T) {
		// v1: {a: d1, b: d2}
		// v2: {a: d1, c: d3}  // b deleted, c added
		// v3: {a: d4, c: d3}  // a modified
		// Compare v1 -> v3: a modified, b deleted, c added
		obj := createTestObjectWithContent(t, conn, rootName, "obj7",
			map[string]string{
				"a.txt": "content1",
				"b.txt": "content2",
			},
			map[string]string{
				"a.txt": "content1",
				"c.txt": "content3",
			},
			map[string]string{
				"a.txt": "content4",
				"c.txt": "content3",
			},
		)

		changes, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 1, 3)
		if err != nil {
			t.Fatal(err)
		}

		if len(changes.Changes) != 3 {
			t.Fatalf("got %d changes, want 3", len(changes.Changes))
		}

		// Verify each change
		changeMap := make(map[string]ocflite.ModType)
		for _, change := range changes.Changes {
			changeMap[change.Path] = change.ModType
		}

		if changeMap["a.txt"] != ocflite.FileModified {
			t.Errorf("a.txt should be modified")
		}
		if changeMap["b.txt"] != ocflite.FileDeleted {
			t.Errorf("b.txt should be deleted")
		}
		if changeMap["c.txt"] != ocflite.FileAdded {
			t.Errorf("c.txt should be added")
		}
	})

	t.Run("invalid from version", func(t *testing.T) {
		obj := createTestObjectWithContent(t, conn, rootName, "obj8",
			map[string]string{
				"a.txt": "content1",
			},
		)

		_, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, -1, 1)
		if err == nil {
			t.Error("expected error for negative fromVN")
		}
	})

	t.Run("invalid to version", func(t *testing.T) {
		obj := createTestObjectWithContent(t, conn, rootName, "obj9",
			map[string]string{
				"a.txt": "content1",
			},
		)

		_, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 1, 0)
		if err == nil {
			t.Error("expected error for toVN < 1")
		}

		_, err = ocflite.GetVersionChanges(conn, rootName, obj.ID, 1, 999)
		if err == nil {
			t.Error("expected error for non-existent toVN")
		}
	})

	t.Run("file rename appears as delete and add", func(t *testing.T) {
		// v1: {old/path.txt: digest1}
		// v2: {new/path.txt: digest1}  // same digest, different path
		content := "same content"
		obj := createTestObjectWithContent(t, conn, rootName, "obj10",
			map[string]string{
				"old/path.txt": content,
			},
			map[string]string{
				"new/path.txt": content,
			},
		)

		changes, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 1, 2)
		if err != nil {
			t.Fatal(err)
		}

		if len(changes.Changes) != 2 {
			t.Fatalf("got %d changes, want 2 (rename appears as delete+add)", len(changes.Changes))
		}

		changeMap := make(map[string]ocflite.ModType)
		for _, change := range changes.Changes {
			changeMap[change.Path] = change.ModType
		}

		if changeMap["new/path.txt"] != ocflite.FileAdded {
			t.Errorf("new/path.txt should be added")
		}
		if changeMap["old/path.txt"] != ocflite.FileDeleted {
			t.Errorf("old/path.txt should be deleted")
		}
	})

	t.Run("results are sorted", func(t *testing.T) {
		// Create object with multiple changes in non-alphabetical order
		obj := createTestObjectWithContent(t, conn, rootName, "obj11",
			map[string]string{
				"a.txt": "content1",
			},
			map[string]string{
				"z.txt": "content-z",
				"b.txt": "content-b",
				"m.txt": "content-m",
			},
		)

		changes, err := ocflite.GetVersionChanges(conn, rootName, obj.ID, 1, 2)
		if err != nil {
			t.Fatal(err)
		}

		// Verify sorted order
		for i := 1; i < len(changes.Changes); i++ {
			if changes.Changes[i-1].Path >= changes.Changes[i].Path {
				t.Errorf("changes not sorted: %q >= %q at positions %d, %d",
					changes.Changes[i-1].Path, changes.Changes[i].Path, i-1, i)
			}
		}
	})
}

func BenchmarkGetObject(b *testing.B) {
	scenarios := []struct {
		name     string
		numObjs  int
		numVers  int
		numFiles int
	}{
		{name: "10obj-10v-10f", numObjs: 10, numVers: 10, numFiles: 10},
		{name: "100obj-10v-10f", numObjs: 100, numVers: 10, numFiles: 10},
		{name: "1000obj-10v-10f", numObjs: 1000, numVers: 10, numFiles: 10},
	}
	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			conn, err := newConn()
			if err != nil {
				b.Fatal("db connection:", err)
			}
			defer conn.Close()
			rootName := "bench-root"
			ids, err := createBenchmarkObjects(conn, rootName, sc.numObjs, sc.numVers, sc.numFiles)
			if err != nil {
				b.Fatal("creating benchmark object:", err)
			}
			// Query the first object (representative of query performance)
			objID := ids[0]
			// Benchmark loop using new b.Loop() from Go 1.24
			for b.Loop() {
				_, err := ocflite.GetObjectBrief(conn, rootName, objID)
				if err != nil {
					b.Fatal("GetObjectBrief:", err)
				}
			}
		})
	}
}

func BenchmarkListVersionFiles(b *testing.B) {
	scenarios := []struct {
		name     string
		numObjs  int
		numVers  int
		numFiles int
		queryDir string
	}{
		{name: "10obj-10v-10f", numObjs: 10, numVers: 10, numFiles: 10, queryDir: "."},
		{name: "100obj-10v-10f", numObjs: 100, numVers: 10, numFiles: 10, queryDir: "."},
		{name: "1000obj-10v-10f", numObjs: 1000, numVers: 10, numFiles: 10, queryDir: "."},
	}
	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			conn, err := newConn()
			if err != nil {
				b.Fatal("db connection:", err)
			}
			defer conn.Close()
			rootName := "bench-root"
			ids, err := createBenchmarkObjects(conn, rootName, sc.numObjs, sc.numVers, sc.numFiles)
			if err != nil {
				b.Fatal("creating benchmark object:", err)
			}
			// Query the first object (representative of query performance)
			objID := ids[0]
			vn := sc.numVers
			// Benchmark loop using new b.Loop() from Go 1.24
			for b.Loop() {
				fileCount := 0
				for _, err := range ocflite.ListVersionFiles(conn, rootName, objID, vn, sc.queryDir) {
					if err != nil {
						b.Fatal("ListVersionFiles error:", err)
					}
					// Access file to prevent compiler optimizations
					fileCount++
				}
				// Verify we got results
				if fileCount == 0 {
					b.Fatal("no files returned from ListVersionFiles")
				}
			}
		})
	}
}

// dirEntriesEqual compares two slices of PathInfo and reports any differences
func dirEntriesEqual(t *testing.T, want, got []*ocflite.VersionDirEntry) {
	t.Helper()
	if len(want) != len(got) {
		t.Errorf("Length mismatch: expected %d entries, got %d", len(want), len(got))
		t.Logf("Expected: %+v", want)
		t.Logf("Actual: %+v", got)
		return
	}
	// Sort both slices by Name for consistent comparison
	slices.SortFunc(want, func(a, b *ocflite.VersionDirEntry) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	for i := range want {
		if *want[i] != *got[i] {
			t.Errorf("Entry %d mismatch:\n  expected: %+v\n  actual:   %+v", i, want[i], got[i])
		}
	}
}

func newTestObject(id string, states ...ocflite.PathMap) *ocflite.Object {
	manifest := ocflite.DigestMap{}
	versions := make([]*ocflite.Version, len(states))
	for i, state := range states {
		vn := i + 1
		for name, digest := range state {
			contentPath := fmt.Sprintf("v%d/content/%s", vn, name)
			manifest[digest] = append(manifest[digest], contentPath)
		}
		versions[i] = &ocflite.Version{
			State:    state.DigestMap(),
			UserName: fmt.Sprintf("user-name-%d-%s", vn, id),
			UserAddr: fmt.Sprintf("user-addr-%d-%s", vn, id),
			Message:  fmt.Sprintf("message-%d-%s", vn, id),
			Created:  versionCreated(vn),
		}
	}
	return &ocflite.Object{
		ID:              id,
		StoragePath:     "storage-path-" + id,
		InventoryDigest: "inventory-digest-" + id,
		DigestAlgorithm: "sha256",
		Vpadding:        3,
		Manifest:        manifest,
		Versions:        versions,
	}
}

func createTestObject(t *testing.T, conn *sqlite.Conn, root string, objID string, states ...ocflite.PathMap) *ocflite.Object {
	t.Helper()
	objInput := newTestObject(objID, states...)
	err := ocflite.SetObject(conn, root, objInput)
	if err != nil {
		t.Fatal("creating test object:", err)
	}
	return objInput
}

func createTestObjectWithContent(t *testing.T, conn *sqlite.Conn, root, objID string, contents ...map[string]string) *ocflite.Object {
	obj, err := createObjectWithContent(conn, root, objID, contents...)
	if err != nil {
		t.Fatal("creating test object", objID)
	}
	return obj
}

func createObjectWithContent(conn *sqlite.Conn, root, objID string, contents ...map[string]string) (*ocflite.Object, error) {
	states := make([]ocflite.PathMap, len(contents))
	sizes := map[string]int64{}
	for i, c := range contents {
		state := make(ocflite.PathMap, len(c))
		for name, conts := range c {
			digest := testutil.DigestSHA256([]byte(conts))
			state[name] = digest
			sizes[digest] = int64(len(conts))
		}
		states[i] = state
	}
	objInput := newTestObject(objID, states...)
	err := ocflite.SetObject(conn, root, objInput)
	if err != nil {
		return nil, err
	}
	for digest, size := range sizes {
		err := ocflite.SetObjectFileSize(conn, root, objID, digest, size)
		if err != nil {
			return nil, err
		}
	}
	return objInput, nil
}

func fileInfoAsDirEntry(info *ocflite.VersionFileInfo) ocflite.VersionDirEntry {
	return ocflite.VersionDirEntry{
		Name:    info.Path,
		Digest:  info.Digest,
		ModVNum: info.ModVnum,
		Modtime: info.Modtime,
		Size:    info.Size,
		HasSize: info.HasSize,
		IsDir:   false,
	}
}

func versionCreated(vn int) time.Time {
	baseTime := time.Date(2024, 1, 1, 1, 1, 1, 0, time.Local)
	return baseTime.Add(time.Hour * time.Duration(vn*24))
}

// func printDB(t *testing.T, conn *sqlite.Conn) {
// 	allColumns := func(stmt *sqlite.Stmt) []string {
// 		cols := stmt.DataCount()
// 		vals := make([]string, cols)
// 		for i := range cols {
// 			vals[i] = stmt.ColumnName(i) + ":" + stmt.ColumnText(i)
// 		}
// 		return vals
// 	}
// 	printTable := func(tableName string) {
// 		q := "select * from " + tableName
// 		sqlitex.ExecuteScript(conn, q, &sqlitex.ExecOptions{
// 			ResultFunc: func(stmt *sqlite.Stmt) error {
// 				t.Log(tableName, allColumns(stmt))
// 				return nil
// 			},
// 		})
// 	}
// 	allTables := []string{
// 		"ocfl_roots",
// 		"ocfl_objects",
// 		"ocfl_object_files",
// 		"ocfl_object_versions",
// 		"ocfl_object_version_files",
// 	}
// 	for _, table := range allTables {
// 		printTable(table)
// 	}
// }

func countTable(t *testing.T, conn *sqlite.Conn, table string) int {
	q := "select count(*) as count from " + table
	count := 0
	err := sqlitex.Execute(conn, q, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			count = int(stmt.GetInt64("count"))
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func createBenchmarkObjects(conn *sqlite.Conn, rootName string, numObjects int, numVersions int, numFiles int) ([]string, error) {
	// Create multiple objects in the repository
	ids := make([]string, numObjects)
	for obj := range numObjects {
		objID := fmt.Sprintf("bench-object-%d", obj)
		ids[obj] = objID
		// Create object with multiple versions containing files
		versions := make([]map[string]string, numVersions)
		for v := range numVersions {
			content := make(map[string]string, numFiles)
			for f := range numFiles {
				// Create varied paths including nested
				// directories
				filePath := fmt.Sprintf(
					"data/subdir/file-%d-%d.txt", v, f)
				fileContent := fmt.Sprintf(
					"content-obj%d-v%d-f%d", obj, v, f)
				content[filePath] = fileContent
			}
			versions[v] = content
		}

		_, err := createObjectWithContent(conn, rootName, objID, versions...)
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}
