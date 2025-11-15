package sqlite_test

import (
	"github.com/srerickson/ocfl-services/access"
	"github.com/srerickson/ocfl-services/access/sqlite"
)

var _ access.Database = (*sqlite.DB)(nil)
