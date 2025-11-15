package sqlite

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"path"
	"runtime"
	"time"

	"github.com/srerickson/ocfl-go"
	"github.com/srerickson/ocfl-go/fs"
	"github.com/srerickson/ocfl-services/access"
	"github.com/srerickson/ocfl-services/internal/ocflite"
	"golang.org/x/sync/errgroup"
	"zombiezen.com/go/sqlite/sqlitemigration"
	"zombiezen.com/go/sqlite/sqlitex"
)

// the number of goroutines used to stat files to get file sizes.
const stat_concurrency = 4

// DB is a sqlite-base implementation of access.Database. It supports
// indexing and access quries for objects in OCFL repository using a sqlite
// database.
type DB struct {
	Pool *sqlitemigration.Pool
}

func NewDB(uri string) (*DB, error) {
	schema := sqlitemigration.Schema{
		Migrations: []string{
			ocflite.MigrateSQL(),
		},
	}
	opts := sqlitemigration.Options{}
	db := &DB{
		Pool: sqlitemigration.NewPool(uri, schema, opts),
	}
	return db, nil
}

func (db *DB) Metrics(ctx context.Context, rootID string) (m access.Metrics, err error) {
	conn, err := db.Pool.Take(ctx)
	if err != nil {
		return
	}
	defer db.Pool.Put(conn)
	m.NumObjects, err = ocflite.CountObjects(conn, rootID)
	return
}

func (db *DB) Close() error { return db.Pool.Close() }

func (db *DB) SetObject(ctx context.Context, rootID string, obj *ocfl.Object) error {
	if !obj.Exists() {
		return fmt.Errorf("cannot index non-persisted object: id=%q", obj.ID())
	}
	if err := db.setObject(ctx, rootID, obj); err != nil {
		return err
	}
	if err := db.setObjectFileSizes(ctx, rootID, obj); err != nil {
		return err
	}
	return nil
}

func (db *DB) UnsetObject(ctx context.Context, rootID string, objID string) (err error) {
	conn, err := db.Pool.Take(ctx)
	if err != nil {
		return
	}
	defer db.Pool.Put(conn)
	commit := sqlitex.Transaction(conn)
	defer commit(&err)
	err = ocflite.UnsetObject(conn, rootID, objID)
	return
}

func (db *DB) GetObject(ctx context.Context, rootID string, objID string) (access.ObjectInfo, error) {
	conn, err := db.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Pool.Put(conn)
	obj, err := ocflite.GetObjectBrief(conn, rootID, objID)
	if err != nil {
		if errors.Is(err, ocflite.ErrNotFound) {
			return nil, access.ErrNotFound
		}
		return nil, err
	}
	return &objectInfo{obj: obj}, nil
}

func (db *DB) GetObjectByPath(ctx context.Context, rootID string, path string) (access.ObjectInfo, error) {
	conn, err := db.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Pool.Put(conn)
	obj, err := ocflite.GetObjectBriefByPath(conn, rootID, path)
	if err != nil {
		if errors.Is(err, ocflite.ErrNotFound) {
			return nil, access.ErrNotFound
		}
		return nil, err
	}
	return &objectInfo{obj: obj}, nil
}

func (db *DB) GetObjectVersion(ctx context.Context, rootID string, objID string, vn int) (access.VersionInfo, error) {
	conn, err := db.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Pool.Put(conn)
	version, err := ocflite.GetVersion(conn, rootID, objID, vn)
	if err != nil {
		if errors.Is(err, ocflite.ErrNotFound) {
			return nil, access.ErrNotFound
		}
		return nil, err
	}
	return &versionInfo{ver: version}, nil
}

func (db *DB) ReadObjectVersionDir(ctx context.Context, rootID string, objID string, vn int, dir string) ([]access.VersionDirEntry, error) {
	conn, err := db.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Pool.Put(conn)
	entries, err := ocflite.ReadVersionDir(conn, rootID, objID, vn, dir)
	if err != nil {
		if errors.Is(err, ocflite.ErrNotFound) {
			return nil, access.ErrNotFound
		}
		return nil, err
	}
	result := make([]access.VersionDirEntry, len(entries))
	for i, entry := range entries {
		result[i] = &versionDirEntry{entry: entry}
	}
	return result, nil
}

func (db *DB) StatObjectVersionFile(ctx context.Context, rootID string, objID string, vn int, name string) (access.VersionFileInfo, error) {
	conn, err := db.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Pool.Put(conn)
	info, err := ocflite.StatVersionFile(conn, rootID, objID, vn, name)
	if err != nil {
		if errors.Is(err, ocflite.ErrNotFound) {
			return nil, access.ErrNotFound
		}
		return nil, err
	}
	return &versionFileInfo{info: info}, nil
}

func (db *DB) ListObjects(ctx context.Context, rootID string, opts access.ListObjectOptions) ([]access.ObjectInfo, error) {
	conn, err := db.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Pool.Put(conn)
	result, err := ocflite.ListObjects(conn, rootID, opts.Limit, opts.Offset)
	if err != nil {
		return nil, err
	}
	objects := make([]access.ObjectInfo, len(result))
	for i := range result {
		objects[i] = &objectInfo{obj: result[i]}
	}
	return objects, nil
}

func (db *DB) TouchObject(ctx context.Context, rootID string, objID string) (access.ObjectInfo, error) {
	conn, err := db.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Pool.Put(conn)
	obj, err := ocflite.TouchObject(conn, rootID, objID)
	if err != nil {
		return nil, err
	}
	return &objectInfo{obj: obj}, nil
}

func (db *DB) objectContentFiles(ctx context.Context, rootID string, objID string) iter.Seq2[access.ContentFileInfo, error] {
	return func(yield func(access.ContentFileInfo, error) bool) {
		// return map of digest to path for content without file size
		conn, err := db.Pool.Take(ctx)
		if err != nil {
			yield(nil, err)
			return
		}
		defer db.Pool.Put(conn)
		names, err := ocflite.ListObjectFiles(conn, rootID, objID)
		if err != nil {
			yield(nil, err)
			return
		}
		for _, name := range names {
			info, err := ocflite.GetObjectFile(conn, rootID, objID, name)
			if err != nil {
				if errors.Is(err, ocflite.ErrNotFound) {
					err = access.ErrNotFound
				}
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !yield(&objectFileInfo{info: info}, nil) {
				return
			}
		}
	}
}

func (db *DB) setObject(ctx context.Context, rootID string, obj *ocfl.Object) (err error) {
	conn, err := db.Pool.Take(ctx)
	if err != nil {
		return
	}
	defer db.Pool.Put(conn)
	commit := sqlitex.Transaction(conn)
	defer commit(&err)
	objInput := &ocflite.Object{
		ID:              obj.ID(),
		StoragePath:     obj.Path(),
		VPadding:        obj.Head().Padding(),
		DigestAlgorithm: obj.DigestAlgorithm().ID(),
		InventoryDigest: obj.InventoryDigest(),
		Manifest:        ocflite.DigestMap(obj.Manifest()),
	}
	objInput.Versions, err = objectVersions(obj)
	if err != nil {
		return
	}
	err = ocflite.SetObject(conn, rootID, objInput)
	if err != nil {
		return
	}
	return
}

func (db *DB) setObjectFileSizes(ctx context.Context, rootID string, obj *ocfl.Object) error {
	objID := obj.ID()
	missing := map[string]string{}
	for file, err := range db.objectContentFiles(ctx, rootID, objID) {
		if err != nil {
			return err
		}
		if _, exist := missing[file.Digest()]; !exist && !file.HasSize() {
			missing[file.Digest()] = path.Join(obj.Path(), file.ContentPath())
		}
	}
	// get file sizes
	sizes, err := batchStatFiles(ctx, obj.FS(), missing, stat_concurrency)
	if err != nil {
		return err
	}
	// add sizes to the index
	return func(sizes map[string]int64) (err error) {
		conn, err := db.Pool.Take(ctx)
		if err != nil {
			return err
		}
		defer db.Pool.Put(conn)
		commit := sqlitex.Transaction(conn)
		defer commit(&err)
		for digest, size := range sizes {
			err = ocflite.SetObjectFileSize(conn, rootID, objID, digest, size)
			if err != nil {
				err = fmt.Errorf("setting object file size: %w", err)
				return
			}
		}
		return
	}(sizes)
}

// ObjectVersions is a convenience function for returning a
// slice of *ObjectVersions from an ocfl.Object.
func objectVersions(obj *ocfl.Object) ([]*ocflite.Version, error) {
	objID := obj.ID()
	headNum := obj.Head().Num()
	versions := make([]*ocflite.Version, headNum)
	for i := range headNum {
		vn := i + 1
		objVer := obj.Version(vn)
		if objVer == nil {
			err := fmt.Errorf("object id=%q with missing version idx=%d", objID, vn)
			return nil, err
		}
		dbVer := &ocflite.Version{
			State:   ocflite.DigestMap(objVer.State()),
			Message: objVer.Message(),
			Created: objVer.Created(),
		}
		if objVer.User() != nil {
			dbVer.UserName = objVer.User().Name
			dbVer.UserAddr = objVer.User().Address
		}
		versions[i] = dbVer
	}
	return versions, nil
}

type objectInfo struct {
	obj *ocflite.ObjectBrief
}

func (o *objectInfo) ID() string              { return o.obj.ID }
func (o *objectInfo) StoragePath() string     { return o.obj.StoragePath }
func (o *objectInfo) Head() ocfl.VNum         { return ocfl.V(o.obj.Head, o.obj.VPadding) }
func (o *objectInfo) Alg() string             { return o.obj.DigestAlgorithm }
func (o *objectInfo) InventoryDigest() string { return o.obj.InventoryDigest }
func (o *objectInfo) CreatedAt() time.Time    { return o.obj.CreatedAt }
func (o *objectInfo) UpdatedAt() time.Time    { return o.obj.UpdatedAt }
func (o *objectInfo) IndexedAt() time.Time    { return o.obj.IndexedAt }

type versionInfo struct {
	ver *ocflite.VersionBrief
}

var _ access.VersionInfo = (*versionInfo)(nil)

func (v *versionInfo) Message() string    { return v.ver.Message }
func (v *versionInfo) UserName() string   { return v.ver.UserName }
func (v *versionInfo) UserAddr() string   { return v.ver.UserAddr }
func (v *versionInfo) Created() time.Time { return v.ver.Created }

// versionDirEntry wraps ocflite.VersionDirEntry to implement access.StateDirEntry
type versionDirEntry struct {
	entry *ocflite.VersionDirEntry
}

var _ access.VersionDirEntry = (*versionDirEntry)(nil)

func (e *versionDirEntry) Name() string       { return e.entry.Name }
func (e *versionDirEntry) Digest() string     { return e.entry.Digest }
func (e *versionDirEntry) ModVNum() int       { return e.entry.ModVNum }
func (e *versionDirEntry) Modtime() time.Time { return e.entry.Modtime }
func (e *versionDirEntry) Size() int64        { return e.entry.Size }
func (e *versionDirEntry) HasSize() bool      { return e.entry.HasSize }
func (e *versionDirEntry) IsDir() bool        { return e.entry.IsDir }

type versionFileInfo struct {
	info *ocflite.VersionFileInfo
}

var _ access.VersionFileInfo = (*versionFileInfo)(nil)

func (c *versionFileInfo) Path() string        { return c.info.Path }
func (c *versionFileInfo) Digest() string      { return c.info.Digest }
func (c *versionFileInfo) ContentPath() string { return c.info.ContentPath }
func (c *versionFileInfo) ModVNum() int        { return c.info.ModVnum }
func (c *versionFileInfo) Modtime() time.Time  { return c.info.Modtime }
func (c *versionFileInfo) Size() int64         { return c.info.Size }
func (c *versionFileInfo) HasSize() bool       { return c.info.HasSize }

type objectFileInfo struct {
	info *ocflite.ObjectFile
}

var _ access.ContentFileInfo = (*objectFileInfo)(nil)

func (c *objectFileInfo) Digest() string      { return c.info.Digest }
func (c *objectFileInfo) ContentPath() string { return c.info.ContentPath }
func (c *objectFileInfo) Size() int64         { return c.info.Size }
func (c *objectFileInfo) HasSize() bool       { return c.info.HasSize }

// FIXME: this feels out of place since it's not specific to sqlite.
func batchStatFiles(ctx context.Context, fsys fs.FS, files map[string]string, numWorkers int) (map[string]int64, error) {
	if numWorkers < 1 {
		numWorkers = runtime.GOMAXPROCS(0)
	}
	type input struct {
		path   string
		digest string
	}
	type output struct {
		digest string
		size   int64
	}
	grp, ctx := errgroup.WithContext(ctx)
	in := make(chan input, 1)
	out := make(chan output, 1)
	errCh := make(chan error, 1)
	defer close(errCh)
	// add input to in
	grp.Go(func() error {
		defer close(in)
		for digest, path := range files {
			work := input{
				digest: digest,
				path:   path,
			}
			select {
			case in <- work:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	// process work from in and send out
	for range numWorkers {
		grp.Go(func() error {
			for work := range in {
				info, err := fs.StatFile(ctx, fsys, work.path)
				if err != nil {
					return err
				}
				result := output{
					digest: work.digest,
					size:   info.Size(),
				}
				select {
				case out <- result:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}
	go func() {
		errCh <- grp.Wait()
		close(out)
	}()
	results := map[string]int64{}
	for result := range out {
		results[result.digest] = result.size
	}
	if err := <-errCh; err != nil {
		return nil, err
	}
	return results, nil
}
