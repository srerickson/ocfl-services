package server_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/carlmjohnson/be"
	"github.com/srerickson/ocfl-services/access"
	"github.com/srerickson/ocfl-services/access/sqlite"
	"github.com/srerickson/ocfl-services/internal/testutil"
	server "github.com/srerickson/ocfl-services/webui"
)

const fixtureObjectID = "ark:123/abc"

func testHandler(t *testing.T) http.Handler {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	db, err := sqlite.NewDB(dbPath)
	if err != nil {
		t.Fatal("setting up test db:", err)
	}
	t.Cleanup(func() { db.Close() })
	root := testutil.FixtureRootCopy(t, filepath.Join("..", "testdata"))
	svc := access.NewService(root, db, "test", nil)
	return server.New(svc)
}

func doRequest(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func objectPath(id, version, path string) string {
	p := "/object/" + url.PathEscape(id)
	if version != "" {
		p += "/" + version
	}
	if path != "" {
		p += "/" + path
	}
	return p
}

func historyPath(id, version string) string {
	p := "/history/" + url.PathEscape(id)
	if version != "" {
		p += "/" + version
	}
	return p
}

func TestHandleIndex(t *testing.T) {
	h := testHandler(t)

	t.Run("GET / returns HTML", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, "/")
		be.Equal(t, http.StatusOK, w.Code)
		be.In(t, "text/html", w.Header().Get("Content-Type"))
	})
}

func TestStaticFiles(t *testing.T) {
	h := testHandler(t)

	t.Run("GET /static/app.css returns CSS", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, "/static/app.css")
		be.Equal(t, http.StatusOK, w.Code)
		be.In(t, "text/css", w.Header().Get("Content-Type"))
	})

	t.Run("GET /static/app.js returns JS", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, "/static/app.js")
		be.Equal(t, http.StatusOK, w.Code)
		be.In(t, "javascript", w.Header().Get("Content-Type"))
	})

	t.Run("GET /static/nonexistent returns 404", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, "/static/nonexistent")
		be.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestObjectFilesRedirects(t *testing.T) {
	h := testHandler(t)

	tests := []struct {
		name    string
		path    string
		wantLoc string
	}{
		{
			name:    "object without version redirects to head/",
			path:    objectPath(fixtureObjectID, "", ""),
			wantLoc: "/head/",
		},
		{
			name:    "object with trailing slash redirects to head/",
			path:    objectPath(fixtureObjectID, "", "") + "/",
			wantLoc: "/head/",
		},
		{
			name:    "version without trailing slash redirects",
			path:    objectPath(fixtureObjectID, "v1", ""),
			wantLoc: "/",
		},
		{
			name:    "head without trailing slash redirects",
			path:    objectPath(fixtureObjectID, "head", ""),
			wantLoc: "/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := doRequest(t, h, http.MethodGet, tt.path)
			be.Equal(t, http.StatusFound, w.Code)
			loc := w.Header().Get("Location")
			be.True(t, strings.HasSuffix(loc, tt.wantLoc))
		})
	}
}

func TestObjectFilesDirectoryListing(t *testing.T) {
	h := testHandler(t)

	t.Run("GET object/id/head/ returns root listing", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "head", "")+"/")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		be.In(t, "a_file.txt", body)
		be.In(t, "README.md", body)
		be.In(t, "exampl", body)
	})

	t.Run("GET object/id/v1/ returns v1 listing", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v1", "")+"/")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		be.In(t, "a_file.txt", body)
		// v1 does not have README.md
		be.True(t, !strings.Contains(body, "README.md"))
	})

	t.Run("GET object/id/v2/ returns v2 listing with readme link", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v2", "")+"/")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		be.In(t, "README.md", body)
		be.In(t, "?render=1", body) // readme render link
	})

	t.Run("GET subdirectory listing", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v2", "exampl")+"/")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		be.In(t, "folder", body)
		be.In(t, "..", body) // parent directory link
	})

	t.Run("GET nested subdirectory listing", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v2", "exampl/folder")+"/")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		be.In(t, "justfile", body)
		be.In(t, "..", body) // parent directory link
	})

	t.Run("directories sorted before files", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v2", "")+"/")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		// "exampl" directory should appear before "a_file.txt" file
		dirIdx := strings.Index(body, "exampl")
		fileIdx := strings.Index(body, "a_file.txt")
		be.True(t, dirIdx < fileIdx)
	})
}

func TestObjectFilesVersionRefs(t *testing.T) {
	h := testHandler(t)

	t.Run("head resolves to latest version", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "head", "")+"/")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		// v2 has README.md
		be.In(t, "README.md", body)
	})

	t.Run("v1 resolves to version 1", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v1", "")+"/")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		be.In(t, "a_file.txt", body)
		be.True(t, !strings.Contains(body, "README.md"))
	})

	t.Run("v2 resolves to version 2", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v2", "")+"/")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		be.In(t, "README.md", body)
	})

	t.Run("v02 padded resolves correctly", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v02", "")+"/")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		be.In(t, "README.md", body)
	})

	t.Run("invalid version format returns 400", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "vX", "")+"/")
		be.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("invalid version format returns 400", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "invalid", "")+"/")
		be.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestObjectFilesDownload(t *testing.T) {
	h := testHandler(t)

	t.Run("GET file returns content with Content-Length", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v1", "a_file.txt"))
		be.Equal(t, http.StatusOK, w.Code)
		be.Equal(t, "20", w.Header().Get("Content-Length"))
		be.Equal(t, 20, w.Body.Len())
	})

	t.Run("HEAD file returns headers only", func(t *testing.T) {
		w := doRequest(t, h, http.MethodHead, objectPath(fixtureObjectID, "v1", "a_file.txt"))
		be.Equal(t, http.StatusOK, w.Code)
		be.Equal(t, "20", w.Header().Get("Content-Length"))
		be.Equal(t, 0, w.Body.Len())
	})

	t.Run("GET nested file returns content", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v2", "exampl/folder/justfile"))
		be.Equal(t, http.StatusOK, w.Code)
		be.True(t, w.Body.Len() > 0)
	})
}

func TestObjectFilesReadmeRender(t *testing.T) {
	h := testHandler(t)

	t.Run("render README.md returns HTML", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v2", "README.md")+"?render=1")
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		// markdown should be rendered to HTML
		be.True(t, len(body) > 0)
	})

	t.Run("render non-readme file returns 400", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v1", "a_file.txt")+"?render=1")
		be.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("render on directory returns 400", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v2", "")+"/?render=1")
		be.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("HEAD request ignores render parameter", func(t *testing.T) {
		w := doRequest(t, h, http.MethodHead, objectPath(fixtureObjectID, "v2", "README.md")+"?render=1")
		be.Equal(t, http.StatusOK, w.Code)
		// HEAD should return file headers, not rendered content
		be.True(t, w.Header().Get("Content-Length") != "")
	})
}

func TestObjectFilesNotFound(t *testing.T) {
	h := testHandler(t)

	t.Run("nonexistent object returns 404", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath("nonexistent", "head", "")+"/")
		be.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("missing file returns 404", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v1", "missing.txt"))
		be.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("missing directory returns 404", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v1", "missing")+"/")
		be.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("file not in version returns 404", func(t *testing.T) {
		// README.md exists in v2 but not v1
		w := doRequest(t, h, http.MethodGet, objectPath(fixtureObjectID, "v1", "README.md"))
		be.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestObjectHistory(t *testing.T) {
	h := testHandler(t)

	t.Run("GET history returns versions in reverse order", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath(fixtureObjectID, ""))
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		// Both versions should be listed
		be.In(t, "v1", body)
		be.In(t, "v2", body)
		// v2 should appear before v1 (reverse chronological) in the version-num spans
		v2Idx := strings.Index(body, `class="version-num">v2`)
		v1Idx := strings.Index(body, `class="version-num">v1`)
		be.True(t, v2Idx > 0)
		be.True(t, v1Idx > 0)
		be.True(t, v2Idx < v1Idx)
	})

	t.Run("history shows version metadata", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath(fixtureObjectID, ""))
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		// v1 metadata
		be.In(t, "A Person", body)
		be.In(t, "An version with one file", body)
		// v2 metadata
		be.In(t, "Seth", body)
	})

	t.Run("nonexistent object returns 404", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath("nonexistent", ""))
		be.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestVersionChanges(t *testing.T) {
	h := testHandler(t)

	t.Run("GET v1 changes shows added files", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath(fixtureObjectID, "v1"))
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		be.In(t, "a_file.txt", body)
	})

	t.Run("GET v2 changes shows added files", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath(fixtureObjectID, "v2"))
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		be.In(t, "README.md", body)
		be.In(t, "justfile", body)
	})

	t.Run("v1 has next navigation link", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath(fixtureObjectID, "v1"))
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		// v1 should have link to next version (v2)
		nextLink := `href="/history/` + url.PathEscape(fixtureObjectID) + `/v2"`
		be.In(t, nextLink, body)
	})

	t.Run("v2 has no next navigation link", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath(fixtureObjectID, "v2"))
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		// v2 is head, should not have next version link
		nextLink := `href="/history/` + url.PathEscape(fixtureObjectID) + `/v3"`
		be.True(t, !strings.Contains(body, nextLink))
	})

	t.Run("invalid version format returns 400", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath(fixtureObjectID, "vX"))
		be.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("invalid version format returns 400", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath(fixtureObjectID, "invalid"))
		be.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("nonexistent version returns 404", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath(fixtureObjectID, "v99"))
		be.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("nonexistent object returns 404", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath("nonexistent", "v1"))
		be.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestBuildFileTree(t *testing.T) {
	// Test the exported buildFileTree function indirectly through handler output
	h := testHandler(t)

	t.Run("file tree shows hierarchical structure", func(t *testing.T) {
		w := doRequest(t, h, http.MethodGet, historyPath(fixtureObjectID, "v2"))
		be.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		// Should show directory structure: exampl/folder/justfile
		be.In(t, "exampl", body)
		be.In(t, "folder", body)
		be.In(t, "justfile", body)
	})
}
