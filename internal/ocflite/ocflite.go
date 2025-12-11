package ocflite

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"slices"
	"strings"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var ErrNotFound = errors.New("not found")

// ModType represents the type of change for a file between versions.
type ModType uint8

const (
	FileAdded ModType = iota
	FileModified
	FileDeleted
)

//go:embed migrate.sql
var migrateSQL string

//go:embed queries/*.sql
var queries embed.FS

// Object represents values needed to add a new object to the database.
type Object struct {
	ID              string // Object's ID
	StoragePath     string // path relative to object's FS (not storage root)
	InventoryDigest string // root inventory digest
	DigestAlgorithm string // root inventory alg
	Versions        []*Version
	Vpadding        int // padding for object's version numbering scheme
	Manifest        DigestMap
}

type ObjectBrief struct {
	ID              string    // ObjectID
	StoragePath     string    // path relative to object's FS (not storage root)
	DigestAlgorithm string    // root inventory alg
	InventoryDigest string    // root inventory digest
	Head            int       // object's most recent version number
	Vpadding        int       // padding for object's version numbering scheme
	CreatedAt       time.Time // timestamp for first version
	UpdatedAt       time.Time // timestamp for most recent version
	IndexedAt       time.Time // time for last database update
}

// ObjectFile represent a database entry for a content file from an OCFL object.
// Content files correspond to manifest entries in an OCFL inventory (but they
// may have file size).
type ObjectFile struct {
	ContentPath string // content path relative to object root
	Digest      string // content digest (sha512 or sh256)
	Size        int64  // content size in bytes
	HasSize     bool   // if false, Size isn't set.
}

// Version represents an entry in the ocfl object versions table.
type Version struct {
	State    DigestMap
	Message  string
	UserName string
	UserAddr string
	Created  time.Time
}

type VersionBrief struct {
	Vnum        int       // version number index (1,2,3..)
	Vpadding    int       // version number padding
	StateDigest string    // sha512 of version state
	Message     string    // version message
	UserName    string    // version user name (may be "")
	UserAddr    string    // version user address (may be "")
	Created     time.Time // created timestamp
}

// VersionDirEntry represents information about an item in a directory of an
// object's version state.
type VersionDirEntry struct {
	// Name is the base name for a file or directory
	Name string

	// Digest is the sha512 or sha256 for the content. This is an empty string
	// if IsDir is true.
	Digest string

	// ModVNum is the version number in which the file or directory
	// was last modified. For files, this means the version in
	// which the file was first created or its digest changed. For
	// directories, this means the version in which any file in the directory
	// (recursive) was created, updated, or deleted.
	ModVNum int

	// Modtime is the timestamp for the version in which the file or directory
	// was last modified.
	Modtime time.Time

	// Size is the number of bytes for a file (or all files) under
	// the directory. Size is only valid if HasSize is true.
	Size int64

	// HasSize is true of the Size is known.
	HasSize bool

	// IsDir true if the entry is a directory.
	IsDir bool
}

type VersionFileInfo struct {
	// Path is the logical path for the file in the version state
	Path string

	// ContentPath is the path for the content path, relative to the object.
	ContentPath string

	// Digest is the sha512 or sha256 for the content. This is an empty string
	// if IsDir is true.
	Digest string

	// ModVnum is the version number in which the file or directory was last
	// modified. For files, this means the version in which the file was first
	// created or its digest changed. For directories, this means the version in
	// which any file in the directory (recursive) was created, updated, or
	// deleted.
	ModVnum int

	// Modtime is the timestamp for the version in which the file or directory
	// was last modified.
	Modtime time.Time

	// Size is the number of bytes for a file (or all files) under the
	// directory. Size is only valid if HasSize is true.
	Size int64

	// HasSize is true of the Size is known.
	HasSize bool

	isDeleted bool
}

// FileChange represents a file that changed between versions.
type FileChange struct {
	// Path is the logical path in the version state
	Path string

	// ModType is the type of change (added/modified/deleted)
	ModType ModType
}

// VersionChanges represents the differences between two versions of an OCFL object.
type VersionChanges struct {
	FromVnum int           // lower version for comparison (0 == empty version state)
	ToVnum   int           // higher version for comparison
	Changes  []*FileChange // changed files sorted by name
}

// Migrate creates tables in a sqlite database used by the package
func Migrate(conn *sqlite.Conn) error {
	if err := sqlitex.ExecuteScript(conn, migrateSQL, nil); err != nil {
		return fmt.Errorf("initializing ocfl database tables: %w", err)
	}
	return nil
}

// MigrateSQL returns the sql query string for creating tables used by the
// package
func MigrateSQL() string {
	return migrateSQL
}

// GetRoots returns a slice of all the root names in the database. Roots are
// automatically created with SetObject.
func GetRoots(conn *sqlite.Conn) ([]string, error) {
	const q = `SELECT name FROM ocfl_roots`
	var roots []string
	err := sqlitex.Execute(conn, q, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			roots = append(roots, stmt.GetText("name"))
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return roots, nil
}

// SetObject is used to add or update objects in the database.
func SetObject(conn *sqlite.Conn, root string, obj *Object) error {
	const qname = `queries/upsert_object.sql`
	if err := setRoot(conn, root); err != nil {
		return nil
	}
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{
			root,
			obj.ID,
			obj.StoragePath,
			obj.Vpadding,
			obj.DigestAlgorithm,
			obj.InventoryDigest,
			time.Now().Unix(),
		},
	})
	if err != nil {
		return fmt.Errorf("setting object in database: %w", err)
	}
	// add manifest entries to the database
	err = setObjectFiles(conn, root, obj.ID, obj.Manifest)
	if err != nil {
		return fmt.Errorf("setting object files in database: %w", err)
	}
	err = setObjectVersions(conn, root, obj.ID, obj.Versions)
	if err != nil {
		return fmt.Errorf("setting object versions in database: %w", err)
	}
	return nil
}

// UnsetObject removes the object with the given ID from the index. It returns
// no error if the object doesn't exist to begin with.
func UnsetObject(conn *sqlite.Conn, root string, objID string) error {
	script := `queries/delete_object.sql`
	err := sqlitex.ExecuteScriptFS(conn, queries, script, &sqlitex.ExecOptions{
		Args: []any{root, objID},
	})
	if err != nil {
		return fmt.Errorf("deleting object: %w", err)
	}
	return nil
}

// TouchObject updates an indexed object's indexed at timestamp to the current
// time and returns the updated ObjectBrief.
func TouchObject(conn *sqlite.Conn, root string, objID string) (*ObjectBrief, error) {
	const qname = `queries/set_object_indexed_at.sql`
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{root, objID, time.Now().Unix()},
	})
	if err != nil {
		err = fmt.Errorf("updating object's indexed_at time: %w", err)
		return nil, err
	}
	return GetObjectBrief(conn, root, objID)
}

// GetObjectBrief returns basic information about an object in the database. It
// doesn't include the manifest or version details. It returns ErrNotFound if the
// object doesn't exist.
func GetObjectBrief(conn *sqlite.Conn, root string, objID string) (*ObjectBrief, error) {
	var o *ObjectBrief
	const qname = `queries/get_object.sql`
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{root, objID},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			o = &ObjectBrief{
				ID:              objID,
				StoragePath:     stmt.GetText("storage_path"),
				DigestAlgorithm: stmt.GetText("alg"),
				Head:            int(stmt.GetInt64("head")),
				Vpadding:        int(stmt.GetInt64("padding")),
				InventoryDigest: stmt.GetText("inventory_digest"),
				IndexedAt:       time.Unix(stmt.GetInt64("indexed_at"), 0),
				CreatedAt:       time.Unix(stmt.GetInt64("created_at"), 0),
				UpdatedAt:       time.Unix(stmt.GetInt64("updated_at"), 0),
			}
			return nil
		},
	})
	if err == nil && o == nil {
		err = fmt.Errorf("object with root=%q, object_id=%q: %w", root, objID, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

// GetObjectBriefByPath returns basic information about an object in the
// database using the storage path rather than the object ID. It doesn't
// include the manifest or version details. It returns ErrNotFound if the
// object doesn't exist.
func GetObjectBriefByPath(conn *sqlite.Conn, root string, storagePath string) (*ObjectBrief, error) {
	var o *ObjectBrief
	const qname = `queries/get_object_by_path.sql`
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{root, storagePath},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			o = &ObjectBrief{
				ID:              stmt.GetText("object_id"),
				StoragePath:     storagePath,
				DigestAlgorithm: stmt.GetText("alg"),
				Head:            int(stmt.GetInt64("head")),
				Vpadding:        int(stmt.GetInt64("padding")),
				InventoryDigest: stmt.GetText("inventory_digest"),
				IndexedAt:       time.Unix(stmt.GetInt64("indexed_at"), 0),
				CreatedAt:       time.Unix(stmt.GetInt64("created_at"), 0),
				UpdatedAt:       time.Unix(stmt.GetInt64("updated_at"), 0),
			}
			return nil
		},
	})
	if err == nil && o == nil {
		err = fmt.Errorf("object with root=%q, storage_path=%q: %w", root, storagePath, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

// ListObjects returns a paginated list of objects with up the pageSize entries,
// starting from a given offset.
func ListObjects(conn *sqlite.Conn, root string, pageSize int, offset int) ([]*ObjectBrief, error) {
	const qname = `queries/list_objects.sql`
	var objects []*ObjectBrief
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{root, pageSize, offset},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			obj := &ObjectBrief{
				ID:              stmt.GetText("object_id"),
				StoragePath:     stmt.GetText("storage_path"),
				DigestAlgorithm: stmt.GetText("alg"),
				Head:            int(stmt.GetInt64("head")),
				Vpadding:        int(stmt.GetInt64("padding")),
				InventoryDigest: stmt.GetText("inventory_digest"),
				IndexedAt:       time.Unix(stmt.GetInt64("indexed_at"), 0),
				CreatedAt:       time.Unix(stmt.GetInt64("created_at"), 0),
				UpdatedAt:       time.Unix(stmt.GetInt64("updated_at"), 0),
			}
			objects = append(objects, obj)
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	// Check if there are more results by trying to fetch one more item
	return objects, nil
}

func CountObjects(conn *sqlite.Conn, root string) (int, error) {
	q := `SELECT count(o.id) as count
			FROM ocfl_objects o JOIN ocfl_roots r ON o.root_id = r.id 
			WHERE r.name = ?`
	var count int
	err := sqlitex.Execute(conn, q, &sqlitex.ExecOptions{
		Args: []any{root},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			count = int(stmt.GetInt64("count"))
			return nil
		},
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// GetObjectFile returns the *ObjecFile record for the object's content
// path.
func GetObjectFile(conn *sqlite.Conn, root string, objID string, path string) (*ObjectFile, error) {
	var f *ObjectFile
	const qname = `queries/get_object_file.sql`
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{root, objID, path},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			f = &ObjectFile{
				Digest:      stmt.GetText("digest"),
				ContentPath: stmt.GetText("path"),
				Size:        stmt.GetInt64("size"),
			}
			if f.Size > -1 {
				f.HasSize = true
			}
			if !f.HasSize {
				f.Size = 0
			}
			return nil
		},
	})
	if err == nil && f == nil {
		err = fmt.Errorf("with root=%q, object_id=%q, path=%q: %w", root, objID, path, ErrNotFound)
		return nil, err
	}
	return f, nil
}

// ListObjectFiles returns a slice of all content paths associated with
// the object.
func ListObjectFiles(conn *sqlite.Conn, root string, objID string) ([]string, error) {
	var paths []string
	const qname = `queries/get_object_files.sql`
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{root, objID},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			paths = append(paths, stmt.GetText("path"))
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("listing object versions: %w", err)
	}
	return paths, nil
}

// SetObjectFileSize sets the size in bytes for the all content files in the
// object with the given digest.
func SetObjectFileSize(conn *sqlite.Conn, root, objID string, digest string, size int64) error {
	const qname = `queries/set_object_file_size.sql`
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{root, objID, digest, size},
	})
	if err != nil {
		return fmt.Errorf("setting object file size: %w", err)
	}
	return nil
}

func GetVersion(conn *sqlite.Conn, root, objID string, vn int) (*VersionBrief, error) {
	var v *VersionBrief
	qname := `queries/get_object_version.sql`
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{root, objID, vn},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			v = &VersionBrief{
				Vnum:        vn,
				Vpadding:    int(stmt.GetInt64("padding")),
				StateDigest: stmt.GetText("state_digest"),
				Message:     stmt.GetText("message"),
				UserName:    stmt.GetText("user_name"),
				UserAddr:    stmt.GetText("user_addr"),
				Created:     time.Unix(stmt.GetInt64("created_at"), 0),
			}
			return nil
		},
	})
	if v == nil && err == nil {
		err = ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("getting object version: %v", err)
	}
	return v, nil
}

// ListVersions returns a slice of values with information about versions in the
// object.
func ListVersions(conn *sqlite.Conn, root, objID string) ([]*VersionBrief, error) {
	args := []any{root, objID}
	const qname = `queries/list_object_versions.sql`
	var versions []*VersionBrief
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: args,
		ResultFunc: func(stmt *sqlite.Stmt) error {
			versions = append(versions, &VersionBrief{
				Vnum:        int(stmt.GetInt64("vnum")),
				Vpadding:    int(stmt.GetInt64("padding")),
				StateDigest: stmt.GetText("state_digest"),
				Message:     stmt.GetText("message"),
				UserName:    stmt.GetText("user_name"),
				UserAddr:    stmt.GetText("user_addr"),
				Created:     time.Unix(stmt.GetInt64("created_at"), 0),
			})
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("listing object versions: %w", err)
	}
	return versions, nil
}

// GetVersionState returns the DigestMap representing the state of the object
// version.
func GetVersionState(conn *sqlite.Conn, root string, objID string, vn int) (DigestMap, error) {
	state := make(DigestMap)
	for file, err := range ListVersionFiles(conn, root, objID, vn, ".") {
		if err != nil {
			return nil, err
		}
		if !file.isDeleted {
			state[file.Digest] = append(state[file.Digest], file.Path)
		}
	}
	return state, nil
}

// GetVersionChanges compares two versions and returns the changes between them.
// The changes include files that were added, modified, or deleted when moving
// from fromVN to toVN. If fromVN is 0, all files in toVN are considered added.
// If fromVN or toVN are invalid, an error is returned. If fromVN == toVN, an
// empty VersionChanges is returned with no error.
func GetVersionChanges(conn *sqlite.Conn, root string, objID string, fromVN int, toVN int) (*VersionChanges, error) {
	// Validate inputs
	if fromVN < 0 || toVN < 1 {
		return nil, fmt.Errorf("invalid version numbers: from=%d, to=%d", fromVN, toVN)
	}

	// Verify toVN exists
	if _, err := GetVersion(conn, root, objID, toVN); err != nil {
		return nil, fmt.Errorf("to version %d: %w", toVN, err)
	}

	// Verify fromVN exists (if not 0)
	if fromVN > 0 {
		if _, err := GetVersion(conn, root, objID, fromVN); err != nil {
			return nil, fmt.Errorf("from version %d: %w", fromVN, err)
		}
	}

	// Handle same version
	if fromVN == toVN {
		return &VersionChanges{
			FromVnum: fromVN,
			ToVnum:   toVN,
			Changes:  []*FileChange{},
		}, nil
	}

	// Get version states
	var fromPaths PathMap
	if fromVN == 0 {
		// fromVN == 0 represents "no version" - use empty PathMap
		fromPaths = make(PathMap)
	} else {
		fromState, err := GetVersionState(conn, root, objID, fromVN)
		if err != nil {
			return nil, fmt.Errorf("getting from state: %w", err)
		}
		fromPaths = fromState.PathMap()
	}

	toState, err := GetVersionState(conn, root, objID, toVN)
	if err != nil {
		return nil, fmt.Errorf("getting to state: %w", err)
	}
	toPaths := toState.PathMap()

	// Compare and categorize changes
	changes := &VersionChanges{
		FromVnum: fromVN,
		ToVnum:   toVN,
		Changes:  []*FileChange{},
	}

	// Find added and modified files
	for path, toDigest := range toPaths {
		fromDigest, existed := fromPaths[path]

		if !existed {
			// File was added
			changes.Changes = append(changes.Changes, &FileChange{
				Path:    path,
				ModType: FileAdded,
			})
		} else if fromDigest != toDigest {
			// File was modified
			changes.Changes = append(changes.Changes, &FileChange{
				Path:    path,
				ModType: FileModified,
			})
		}
	}

	// Find deleted files
	for path := range fromPaths {
		if _, exists := toPaths[path]; !exists {
			// File was deleted
			changes.Changes = append(changes.Changes, &FileChange{
				Path:    path,
				ModType: FileDeleted,
			})
		}
	}

	// Sort results for consistency
	slices.SortFunc(changes.Changes, func(a, b *FileChange) int {
		if a.Path < b.Path {
			return -1
		}
		if a.Path > b.Path {
			return 1
		}
		return 0
	})

	return changes, nil
}

// ReadVersionDir gets entries for a directory in an object state.
//
// TODO: move readdir and direntry implementations to access/sqlite.
func ReadVersionDir(conn *sqlite.Conn, root string, objID string, vn int, dir string) ([]*VersionDirEntry, error) {
	if dir == "" {
		dir = "."
	}
	if !fs.ValidPath(dir) {
		return nil, fmt.Errorf("invalid object version directory: %q", dir)
	}
	var entries []*VersionDirEntry
	for file, err := range ListVersionFiles(conn, root, objID, vn, dir) {
		if err != nil {
			return nil, fmt.Errorf("reading version directory %q: %w", dir, err)
		}
		// files are ordered by path, but some files are deleted.
		relPath := strings.TrimPrefix(file.Path, dir+"/")
		entryName, _, isDir := strings.Cut(relPath, "/")
		var lastEntry *VersionDirEntry
		if l := len(entries); l > 0 {
			lastEntry = entries[l-1]
		}
		// lastEntry can have the same name but a different type from entryName
		// if this is a deleted file.
		if lastEntry != nil && lastEntry.Name == entryName {
			// Add file stats to entry's aggregrated values.
			if lastEntry.ModVNum < file.ModVnum {
				lastEntry.ModVNum = file.ModVnum
				lastEntry.Modtime = file.Modtime
			}
			if !file.isDeleted {
				lastEntry.HasSize = lastEntry.HasSize && file.HasSize
				lastEntry.Size += file.Size
			}
			continue
		}
		// this is first entry or the last entry was for a different name.
		// Create a new entry if this is an existing file.
		if !file.isDeleted {
			newEntry := &VersionDirEntry{
				Name:    entryName,
				IsDir:   isDir,
				Digest:  file.Digest,
				ModVNum: file.ModVnum,
				Modtime: file.Modtime,
				Size:    file.Size,
				HasSize: file.HasSize,
			}
			if isDir {
				newEntry.Digest = ""
			}
			entries = append(entries, newEntry)
		}
	}
	// only the root directory can be empty (i.e., object is empty); otherwise,
	// it represents a "not found" error
	if len(entries) < 1 && dir != "." {
		return nil, fmt.Errorf("with object version directory: object_id=%q v=%d dir=%q: %w", objID, vn, dir, ErrNotFound)
	}
	return entries, nil

}

// StatVersionFile file information for the given name, which must be a
// file in the version state for the given object.
func StatVersionFile(conn *sqlite.Conn, root string, objID string, vn int, name string) (*VersionFileInfo, error) {
	var info *VersionFileInfo
	if !fs.ValidPath(name) || name == "." || name == "" {
		return info, fmt.Errorf("invalid version state file name: %q", name)
	}
	// check that the version exists: we need to do this because
	// get_object_version_file.sql treats vn as a max value for a file's
	// modification.
	if _, err := GetVersion(conn, root, objID, vn); err != nil {
		return nil, err
	}
	const qname = `queries/get_object_version_file.sql`
	args := []any{root, objID, vn, name}
	err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: args,
		ResultFunc: func(stmt *sqlite.Stmt) error {
			if stmt.GetBool("is_deleted") {
				return nil
			}
			info = &VersionFileInfo{
				Path:        name,
				Digest:      stmt.GetText("digest"),
				ContentPath: stmt.GetText("content_path"),
				ModVnum:     int(stmt.GetInt64("mod_vnum")),
				Modtime:     time.Unix(stmt.GetInt64("mod_time"), 0),
				Size:        stmt.GetInt64("size"),
				HasSize:     true,
			}
			if info.Size < 0 {
				info.Size = 0
				info.HasSize = false
			}
			return nil
		},
	})
	if info == nil && err == nil {
		err = ErrNotFound
	}
	if err != nil {
		return info, fmt.Errorf("stat version file %q: %w", name, err)
	}
	return info, nil
}

// SetObjectFiles adds an object's manifest to the database. Subsequent calls
// will update previously added files (if the digest has changed), or delete
// them (if they are no longer included in the manifest).
func setObjectFiles(conn *sqlite.Conn, root string, objID string, manifest DigestMap) error {
	const qname = `queries/upsert_object_file.sql`
	addedPaths := map[string]bool{}
	for path, digest := range manifest.Paths() {
		err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
			Args: []any{root, objID, path, digest, -1},
		})
		if err != nil {
			return fmt.Errorf("adding object file: %w", err)
		}
		addedPaths[path] = true
	}
	// delete any object files that may have previously been added to the
	// object, but aren't included now
	allPaths, err := ListObjectFiles(conn, root, objID)
	if err != nil {
		return fmt.Errorf("listing object files: %w", err)
	}
	for _, name := range allPaths {
		if addedPaths[name] {
			continue
		}
		if err := deleteObjectFile(conn, root, objID, name); err != nil {
			return err
		}
	}
	return nil
}

func deleteObjectFile(conn *sqlite.Conn, root string, objID string, path string) error {
	const qname = `queries/delete_object_file.sql`
	return sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{root, objID, path},
	})
}

// SetObjectVersions sets
func setObjectVersions(conn *sqlite.Conn, root string, objID string, versions []*Version) error {
	existing, err := ListVersions(conn, root, objID)
	if err != nil {
		return fmt.Errorf("listing existing versions: %w", err)
	}
	for i, ver := range versions {
		if err := setObjectVersion(conn, root, objID, i+1, ver); err != nil {
			return err
		}
	}
	// delete any higher versions that may exist
	for i := len(versions); i < len(existing); i++ {
		if err := deleteObjectVersion(conn, root, objID, i+1); err != nil {
			return err
		}
	}
	return nil
}

func setObjectVersion(conn *sqlite.Conn, root string, objID string, vn int, version *Version) error {
	var existingStateDigest string
	if existing, err := GetVersion(conn, root, objID, vn); err == nil {
		existingStateDigest = existing.StateDigest
	}
	newStateDigest := version.State.Hash()
	args := []any{
		root,
		objID,
		vn,
		newStateDigest,
		version.Created.Unix(),
		version.UserName,
		version.UserAddr,
		version.Message,
	}
	const qname = `queries/upsert_object_version.sql`
	if err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: args,
	}); err != nil {
		return fmt.Errorf("setting object version: %w", err)
	}
	if newStateDigest != existingStateDigest {
		if err := setObjectVersionState(conn, root, objID, vn, version.State); err != nil {
			return err
		}
	}
	return nil
}

// setObjectVersionState adds version state (map of filenames to digests) for the object
// version. version should be a positive integer and the previous versions
// should have already been added.
func setObjectVersionState(conn *sqlite.Conn, root string, objectID string, vn int, state DigestMap) error {
	statePM := state.PathMap()
	// delete any existing version files if present
	if err := deleteObjectVersionFiles(conn, root, objectID, vn); err != nil {
		return err
	}
	var prevPaths PathMap
	// Get previous version's files if not version 1
	if vn > 1 {
		prev, err := GetVersionState(conn, root, objectID, vn-1)
		if err != nil {
			return err
		}
		prevPaths = prev.PathMap()
	}
	// Insert new/changed files
	for path, digest := range statePM {
		if prevPaths != nil {
			prevDigest, existed := prevPaths[path]
			if existed && prevDigest == digest {
				continue
			}
		}
		err := insertVersionFile(conn, root, objectID, vn, digest, path, false)
		if err != nil {
			return err
		}
	}
	// Mark deleted files
	for prevPath, prevDigest := range prevPaths {
		if _, exists := statePM[prevPath]; !exists {
			err := insertVersionFile(conn, root, objectID, vn, prevDigest, prevPath, true)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func deleteObjectVersion(conn *sqlite.Conn, root string, objID string, vn int) error {
	if err := deleteObjectVersionFiles(conn, root, objID, vn); err != nil {
		return err
	}
	script := `queries/delete_object_version.sql`
	err := sqlitex.ExecuteFS(conn, queries, script, &sqlitex.ExecOptions{
		Args: []any{root, objID, vn},
	})
	if err != nil {
		return fmt.Errorf("deleting object version: %w", err)
	}
	return nil
}

func deleteObjectVersionFiles(conn *sqlite.Conn, root string, objID string, vn int) error {
	script := `queries/delete_object_version_files.sql`
	err := sqlitex.ExecuteFS(conn, queries, script, &sqlitex.ExecOptions{
		Args: []any{root, objID, vn},
	})
	if err != nil {
		return fmt.Errorf("deleting object version files: %w", err)
	}
	return nil
}

func ListVersionFiles(conn *sqlite.Conn, root string, objID string, vn int, dir string) iter.Seq2[*VersionFileInfo, error] {
	breakErr := errors.New("break")
	const qname = `queries/list_object_version_files.sql`
	return func(yield func(*VersionFileInfo, error) bool) {
		err := sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
			Args: []any{root, objID, vn, dir},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				vf := &VersionFileInfo{
					Path:        stmt.GetText("path"),
					ContentPath: stmt.GetText("content_path"),
					ModVnum:     int(stmt.GetInt64("mod_vnum")),
					Modtime:     time.Unix(stmt.GetInt64("mod_time"), 0),
					Size:        stmt.GetInt64("size"),
					HasSize:     true,
					Digest:      stmt.GetText("digest"),
					isDeleted:   stmt.GetBool("is_deleted"),
				}
				if vf.Size < 0 {
					vf.HasSize = false
					vf.Size = 0
				}
				if !yield(vf, nil) {
					return breakErr
				}
				return nil
			},
		})
		if err != nil && err != breakErr {
			yield(nil, err)
		}
	}
}

func insertVersionFile(conn *sqlite.Conn, root string, objID string, vn int, digest string, name string, isDeleted bool) error {
	const qname = `queries/insert_object_version_file.sql`
	return sqlitex.ExecuteFS(conn, queries, qname, &sqlitex.ExecOptions{
		Args: []any{root, objID, vn, digest, name, isDeleted},
	})
}

func setRoot(conn *sqlite.Conn, name string) error {
	const q = `INSERT INTO ocfl_roots (name) VALUES (?) ON CONFLICT(name) DO NOTHING;`
	return sqlitex.Execute(conn, q, &sqlitex.ExecOptions{
		Args: []any{name},
	})
}
