package utils

import (
	"io/fs"
	"net/url"
	"path"

	"github.com/a-h/templ"
)

func LinkObjectFiles(objID string, version string, logicalPath string, isDir bool) templ.SafeURL {
	if objID == "" {
		return ""
	}
	if version == "" {
		version = "head"
	}
	objectPath := "/object/" + url.PathEscape(objID) + "/" + version + "/"
	if fs.ValidPath(logicalPath) {
		objectPath += path.Clean(logicalPath)
		if isDir {
			objectPath += "/" // trailing slash for directories
		}
	}
	return templ.URL(objectPath)
}

func LinkObjectHistory(objID string) templ.SafeURL {
	return templ.URL("/history/" + url.PathEscape(objID))
}
