package access

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"strings"
	"time"

	"github.com/srerickson/ocfl-go"
	"golang.org/x/sync/singleflight"
)

// min time between checking sidecar
const RefreshInterval = 20 * time.Second

var ErrNotFound = errors.New("not found")

type Service struct {
	root     *ocfl.Root
	rootID   string
	db       Database
	inflight singleflight.Group
	logger   *slog.Logger
}

// NewServices initializes a new *Service for accessing an indexed OCFL storage
// root.
func NewService(root *ocfl.Root, db Database, rootID string, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Service{
		root:   root,
		rootID: rootID,
		db:     db,
		logger: logger,
	}
}

// SyncObject updates objID in the database if necessary and returns ObjectInfo. If
// the object doesn't exist in the storage root, ErrNotFound is returned. If
// RefreshInterval has not passed since the object was last indexed, the
// existing index value is used. If the indexed object needs to be refreshed,
// the OCFL object's inventory sidecar is compared to check if a full inventory
// read is nessary.
func (s *Service) SyncObject(ctx context.Context, objID string) (ObjectInfo, error) {
	obj, err := s.db.GetObject(ctx, s.rootID, objID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if obj != nil && time.Now().Before(obj.IndexedAt().Add(RefreshInterval)) {
		// Not enough time has passed since last sync. Use current value.
		return obj, nil
	}
	val, err, _ := s.inflight.Do("obj:"+objID, func() (any, error) {
		return s.syncObject(ctx, objID, obj)
	})
	if err != nil {
		return nil, err
	}
	return val.(ObjectInfo), nil
}

// GetVersionInfo returns basic information about a version. If vn < 1, the
// object's most recent version is used.
func (s *Service) GetVersionInfo(ctx context.Context, objID string, vn int) (VersionInfo, error) {
	_, err := s.syncObjectCheckVersion(ctx, objID, vn)
	if err != nil {
		return nil, err
	}
	return s.db.GetObjectVersion(ctx, s.rootID, objID, vn)
}

func (s *Service) ListVersions(ctx context.Context, objID string) ([]VersionInfo, error) {
	if _, err := s.SyncObject(ctx, objID); err != nil {
		return nil, err
	}
	return s.db.ListObjectVersions(ctx, s.rootID, objID)
}

// IndexRoot indexes the all objects in the storage root. For duplicate calls,
// the duplicate caller waits for the original to complete and receives the same
// results.
func (s *Service) IndexRoot(ctx context.Context) error {
	_, err, _ := s.inflight.Do(s.rootID, func() (any, error) {
		for decl, err := range s.root.ObjectDeclarations(ctx) {
			if err != nil {
				s.logger.Error(err.Error())
				continue
			}
			objPath := path.Dir(decl.FullPath())
			objInfo, err := s.db.GetObjectByPath(ctx, s.rootID, objPath)
			if err != nil && !errors.Is(err, ErrNotFound) {
				s.logger.Error(err.Error(), "storage_path", objPath)
				continue
			}
			if _, err := s.syncObjectPath(ctx, objPath, objInfo); err != nil {
				s.logger.Error(err.Error(), "storage_path", objPath)
				continue
			}
		}
		return nil, nil
	})
	return err
}

func (s *Service) Logger() *slog.Logger { return s.logger }

// OpenVersionFile return an fs.File for reading the contents of a file in an
// object version. If vn is < 1, the object's most recent version is used.
func (s *Service) OpenVersionFile(ctx context.Context, objID string, vn int, name string) (fs.File, error) {
	obj, err := s.syncObjectCheckVersion(ctx, objID, vn)
	if err != nil {
		return nil, err
	}
	if vn < 1 {
		vn = obj.Head().Num()
	}
	info, err := s.db.StatObjectVersionFile(ctx, s.rootID, objID, vn, name)
	if err != nil {
		return nil, err
	}
	filePath := path.Join(obj.StoragePath(), info.ContentPath())
	f, err := s.root.FS().OpenFile(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// ReadVersionDir returns a slice of directory entries for the contents of the
// directory dir in the given object version's state.
func (s *Service) ReadVersionDir(ctx context.Context, objID string, vn int, dir string) ([]VersionDirEntry, error) {
	_, err := s.syncObjectCheckVersion(ctx, objID, vn)
	if err != nil {
		return nil, err
	}
	return s.db.ReadObjectVersionDir(ctx, s.rootID, objID, vn, dir)
}

// Root returns the service's OCFL Storage Root.
func (s *Service) Root() *ocfl.Root { return s.root }

// syncObject updates the index record for objID. The prev argument is optional
// -- if provided, the sidecar digest is to check if the ocfl has changed.
func (s *Service) syncObject(ctx context.Context, objID string, prev ObjectInfo) (ObjectInfo, error) {
	if prev != nil {
		// read the root inventory sidecar and check that its value matches
		// value from the database. If the values don't match, the object will
		// be reindexed.
		fsys := s.root.FS()
		path := prev.StoragePath()
		sidecar, err := ocfl.ReadInventorySidecar(ctx, fsys, path, prev.Alg())
		if err == nil && sidecar == prev.InventoryDigest() {
			s.logger.Info("object unchanged", "object_id", objID)
			return s.db.TouchObject(ctx, s.rootID, objID)
		}
		if err != nil {
			s.logger.Error(err.Error())
		}
	}
	s.logger.LogAttrs(ctx, slog.LevelDebug, "indexing object from root inventory",
		slog.String("object_id", objID))

	ocflObj, err := s.root.NewObject(ctx, objID,
		ocfl.ObjectMustExist(),
		ocfl.ObjectSkipRootSidecarValidation())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// the object's root inventory doesn't exist in the storage root.
			// Remove it from the index
			unsetErr := s.db.UnsetObject(ctx, s.rootID, objID)
			if unsetErr != nil {
				return nil, fmt.Errorf("while removing object from index: %w", err)
			}
			return nil, ErrNotFound
		}
		return nil, err
	}
	if err := s.db.SetObject(ctx, s.rootID, ocflObj); err != nil {
		return nil, err
	}
	return s.db.GetObject(ctx, s.rootID, objID)
}

// sync using path instead of ID
func (s *Service) syncObjectPath(ctx context.Context, objPath string, prev ObjectInfo) (ObjectInfo, error) {
	if prev != nil {
		// read the root inventory sidecar and check that its value matches
		// value from the database. If the values don't match, the object will
		// be reindexed.
		sidecar, err := ocfl.ReadInventorySidecar(ctx, s.root.FS(), objPath, prev.Alg())
		if err == nil && sidecar == prev.InventoryDigest() {
			s.logger.Info("object unchanged", "object_id", prev.ID())
			return s.db.TouchObject(ctx, s.rootID, prev.ID())
		}
		if err != nil {
			s.logger.Error(err.Error())
		}
	}
	s.logger.LogAttrs(ctx, slog.LevelDebug, "indexing object from root inventory",
		slog.String("object_path", objPath))
	objPathInRoot := strings.TrimPrefix(objPath, s.root.Path()) // objPath relative to root
	ocflObj, err := s.root.NewObjectDir(ctx, objPathInRoot,
		ocfl.ObjectMustExist(),
		ocfl.ObjectSkipRootSidecarValidation())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			if prev != nil {
				// the object's root inventory doesn't exist in the storage root.
				// Remove it from the index
				unsetErr := s.db.UnsetObject(ctx, s.rootID, prev.ID())
				if unsetErr != nil {
					return nil, fmt.Errorf("while removing object from index: %w", err)
				}
			}
			return nil, ErrNotFound // replace fs.ErrNotExists
		}
		return nil, err
	}
	if err := s.db.SetObject(ctx, s.rootID, ocflObj); err != nil {
		return nil, err
	}
	return s.db.GetObject(ctx, s.rootID, ocflObj.ID())
}

// syncObject and also check its version number against vn: return ErrNotFound
// if the existing object's Head is lower than vn.
func (s *Service) syncObjectCheckVersion(ctx context.Context, objID string, vn int) (ObjectInfo, error) {
	obj, err := s.SyncObject(ctx, objID)
	if err != nil {
		return nil, fmt.Errorf("with object_id=%q: %w", objID, err)
	}
	if vn < 1 {
		vn = obj.Head().Num()
	}
	if vn > obj.Head().Num() {
		// version not found
		return nil, fmt.Errorf("with object_id=%q, version=%d: %w", objID, vn, ErrNotFound)
	}
	return obj, nil
}
