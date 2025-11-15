package access

import (
	"context"
	"time"

	"github.com/srerickson/ocfl-go"
)

// Database is an interface used to store OCFL object information in a database
// for fast access.
type Database interface {
	// GetObject returns indexed ObjecInfo for the object with the given ID.
	GetObject(ctx context.Context, rootID string, objID string) (ObjectInfo, error)

	// GetObjectByPath returns indexed ObjectInfo for the object with the given
	// storage path. The storage path is the object's path relative to its FS,
	// not its OCFL storage root!
	GetObjectByPath(ctx context.Context, rootID string, path string) (ObjectInfo, error)

	// SetObject adds obj to the index. If an object with the same ID already
	// exists in the index, it is replaced.
	SetObject(ctx context.Context, rootID string, obj *ocfl.Object) error

	// TouchObject updates the indexed_at timestamp for the object with the
	// given ID and returns new ObjectInfo.
	TouchObject(ctx context.Context, rootID string, objID string) (ObjectInfo, error)

	// UnsetObject removes an object from the index. No error is returned if the
	// object doesn't exist.
	UnsetObject(ctx context.Context, rootID string, objdID string) error

	// ListObjects returns a slice of objects representing representing a "page"
	// of results. The slice will have length of opts.Limit or less.
	ListObjects(ctx context.Context, rootID string, opts ListObjectOptions) ([]ObjectInfo, error)

	// GetObjectVersion returns VersionInfo for the object. If vn < 1, the
	// object's most recent version is used.
	GetObjectVersion(ctx context.Context, rootID string, objID string, vn int) (VersionInfo, error)

	// ReadObjectVersionDir returns listing for entries in an OCFL object
	// version's logical state. . If vn < 1,
	// the object's most recent version is used.
	ReadObjectVersionDir(ctx context.Context, rootID string, objID string, vn int, dir string) ([]VersionDirEntry, error)

	//StatObjectVersionFile returns information a file in an object version's logical state. If vn < 1,
	// the object's most recent version is used.
	StatObjectVersionFile(ctx context.Context, rootID string, objID string, vn int, name string) (VersionFileInfo, error)

	// Returns various counts for objects in a storage root
	Metrics(ctx context.Context, rootID string) (Metrics, error)
}

// Metrics includes counts for indexed objects in a storage root
type Metrics struct {
	NumObjects int
}

type ListObjectOptions struct {
	Offset int
	Limit  int
}

// ObjectInfo represents a hig-level summary of the object: it doesn't not
// include manifest or version states.
type ObjectInfo interface {
	ID() string              // ID is the unique identifier for the object
	StoragePath() string     // StoragePath is the path to the object within the storage root
	Head() ocfl.VNum         // Head is the current (head) version number ("v2")
	Alg() string             // object's digest algorithm
	InventoryDigest() string // object's root inventory digest
	CreatedAt() time.Time    // CreatedAt is the timestamp for the first object version
	UpdatedAt() time.Time    // UpdatedAt is the timestamp for the most recent object version
	IndexedAt() time.Time    // IndexedAt is when this record was last verified from the repository
}

type ContentFileInfo interface {
	ContentPath() string
	Digest() string
	HasSize() bool
	Size() int64
}

// VersionInfo represents high-level information about an object version: it
// doesn't include the version state.
type VersionInfo interface {
	Message() string
	UserName() string
	UserAddr() string
	Created() time.Time
}

type VersionFileInfo interface {
	Path() string
	ContentPath() string
	Digest() string
	ModVNum() int
	Modtime() time.Time
	Size() int64
	HasSize() bool
}

type VersionDirEntry interface {
	Name() string
	Digest() string
	ModVNum() int
	Modtime() time.Time
	Size() int64
	HasSize() bool
	IsDir() bool
}
