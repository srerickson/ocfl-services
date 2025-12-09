package server

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/a-h/templ"
	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/html"
	"github.com/gomarkdown/markdown/parser"
	"github.com/srerickson/ocfl-go"
	"github.com/srerickson/ocfl-services/access"
	pages "github.com/srerickson/ocfl-services/webui/template"
)

// max size for markdown files we will render
const maxMarkdownSize = 1024 * 1024 * 2 // 2 MiB

//go:embed static/dst/*
var staticFiles embed.FS

// New creates handler for serving from accessService's OCFL storage root.
func New(accessService *access.Service) http.Handler {
	mux := http.NewServeMux()

	// static files: css and js
	staticFS, _ := fs.Sub(staticFiles, "static/dst")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	// homepage
	mux.HandleFunc("GET /{$}", HandleIndex())

	// object view
	mux.HandleFunc("GET /object/{id}/{version}/{path...}", HandleGetObjectPath(accessService))
	mux.HandleFunc("GET /object/{id}/{version}", redirectToDefaultObjectPath)
	mux.HandleFunc("GET /object/{id}/", redirectToDefaultObjectPath)
	mux.HandleFunc("GET /object/{id}", redirectToDefaultObjectPath)

	// wrap with logging middleware
	return loggingMiddleware(accessService.Logger())(mux)
}

func HandleGetObjectPath(svc *access.Service) http.HandlerFunc {

	// request parameters
	type params struct {
		objID  string    // object id
		verRef string    // "head", "v1", "v002", ...
		ver    ocfl.VNum // version number, zero-value == most recent.
		path   string    // clean path for request: file or directory in version state
		isDir  bool      // if requested path is "." or ends with "/" this is true

		renderReadme bool // render readme file, don't download it
	}

	// returned error means "bad request"
	getParams := func(r *http.Request) (p *params, err error) {
		p = &params{
			objID:        r.PathValue("id"),
			verRef:       r.PathValue("version"),
			path:         r.PathValue("path"),
			renderReadme: r.URL.Query().Has("render"),
		}
		if p.verRef != "head" {
			// must be valid version number (v1, v002)
			err = ocfl.ParseVNum(p.verRef, &p.ver)
			if err != nil {
				return
			}
		}
		p.isDir = strings.HasSuffix(p.path, "/")
		p.path = path.Clean(p.path)
		if p.path == "." {
			p.isDir = true
		}
		if !fs.ValidPath(p.path) {
			err = fmt.Errorf("path %q: %w", p.path, fs.ErrInvalid)
		}
		// ignore render for HEAD requests
		if r.Method == http.MethodHead {
			p.renderReadme = false
		}
		// if render is set, path must be readme
		if p.renderReadme {
			if p.isDir || !isReadmeFile(p.path) {
				err = errors.New("invalid path for markdown rendering")
			}
		}
		return
	}

	// logErr logs errors and sets http response code.
	logErr := func(w http.ResponseWriter, r *http.Request, p *params, err error) {
		if err == nil {
			return
		}
		if errors.Is(err, access.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		logAttrs := []slog.Attr{
			slog.String("object_id", p.objID),
			slog.String("path", p.path),
			slog.String("version", p.verRef),
			slog.Bool("is_dir", p.isDir),
			slog.Bool("render", p.renderReadme),
		}
		svc.Logger().LogAttrs(r.Context(), slog.LevelError, err.Error(), logAttrs...)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// handle file requests: download file
	handleFile := func(p *params) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			f, err := svc.OpenVersionFile(ctx, p.objID, p.ver.Num(), p.path)
			if err != nil {
				logErr(w, r, p, err)
				return
			}
			defer f.Close()
			info, err := f.Stat()
			if err != nil {
				logErr(w, r, p, err)
				return
			}
			w.Header().Add("Content-Length", strconv.FormatInt(info.Size(), 10))
			if r.Method == http.MethodHead {
				return
			}
			if _, err := io.Copy(w, f); err != nil {
				logErr(w, r, p, err)
				return
			}
		}
	}

	// handle file requests: render README
	handleReadme := func(p *params) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			f, err := svc.OpenVersionFile(ctx, p.objID, p.ver.Num(), p.path)
			if err != nil {
				logErr(w, r, p, err)
				return
			}
			defer f.Close()
			info, err := f.Stat()
			if err != nil {
				logErr(w, r, p, err)
				return
			}
			if info.Size() > maxMarkdownSize {
				// TODO
				return
			}
			md, err := io.ReadAll(f)
			if err != nil {
				logErr(w, r, p, err)
				return
			}
			reader := bytes.NewReader(markdownToHTML(md))
			if _, err := io.Copy(w, reader); err != nil {
				logErr(w, r, p, err)
			}
		}
	}

	// handle directory requests
	handleDir := func(p *params) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			obj, err := svc.SyncObject(ctx, p.objID)
			if err != nil {
				logErr(w, r, p, err)
				return
			}
			if p.ver.Num() < 1 {
				p.ver = obj.Head()
			}
			ver, err := svc.GetVersionInfo(ctx, p.objID, p.ver.Num())
			if err != nil {
				logErr(w, r, p, err)
				return
			}
			entries, err := svc.ReadVersionDir(ctx, p.objID, p.ver.Num(), p.path)
			if err != nil {
				logErr(w, r, p, err)
				return
			}
			if r.Method == http.MethodHead {
				return
			}
			sortVersionDirEntries(entries)
			page := &pages.ObjectPage{
				ObjectID:         p.objID,
				CurrentPath:      p.path,
				VersionRef:       p.verRef,
				DigestAlgorithm:  obj.Alg(),
				DirectoryEntries: make([]*pages.DirectoryEntry, 0, len(entries)),
				Version: pages.VersionBrief{
					VNum:     p.ver,
					Created:  ver.Created(),
					Message:  ver.Message(),
					UserName: ver.UserName(),
					UserAddr: ver.UserAddr(),
				},
			}
			if page.CurrentPath != "." {
				parentDirEntry := &pages.DirectoryEntry{
					Name:  "..",
					Href:  "../",
					IsDir: true,
				}
				page.DirectoryEntries = append(page.DirectoryEntries, parentDirEntry)
			}
			for _, entry := range entries {
				href := entry.Name()
				if entry.IsDir() {
					href += "/"
				}
				page.DirectoryEntries = append(page.DirectoryEntries, &pages.DirectoryEntry{
					Name:    entry.Name(),
					Href:    templ.URL(href),
					Digest:  entry.Digest(),
					IsDir:   entry.IsDir(),
					Size:    entry.Size(),
					HasSize: entry.HasSize(),
					Modtime: entry.Modtime(),
				})
				// add readme to directory list
				if isReadmeFile(entry.Name()) && !entry.IsDir() {
					page.ReadmeHref = entry.Name() + "?render=1"
				}
			}
			pages.Object(page).Render(r.Context(), w)
		}
	}

	return func(w http.ResponseWriter, r *http.Request) {
		p, err := getParams(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var next http.HandlerFunc
		switch {
		case p.isDir:
			next = handleDir(p)
		case p.renderReadme:
			next = handleReadme(p)
		default:
			next = handleFile(p)
		}
		next(w, r)
	}
}

// isReadmeFile checks if the file has a markdown extension
func isReadmeFile(name string) bool {
	lower := path.Base(strings.ToLower(name))
	return lower == "readme.md" || lower == "readme.txt"
}

// markdownToHTML converts markdown content to HTML using gomarkdown
func markdownToHTML(md []byte) []byte {
	// Create markdown parser with extensions
	extensions := parser.CommonExtensions | parser.AutoHeadingIDs |
		parser.NoEmptyLineBeforeBlock
	p := parser.NewWithExtensions(extensions)
	doc := p.Parse(md)

	// Create HTML renderer with options
	htmlFlags := html.CommonFlags | html.HrefTargetBlank
	opts := html.RendererOptions{Flags: htmlFlags}
	renderer := html.NewRenderer(opts)
	return markdown.Render(doc, renderer)

}

func HandleIndex() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pages.Index().Render(r.Context(), w)
	}
}

func redirectToDefaultObjectPath(w http.ResponseWriter, r *http.Request) {
	version := r.PathValue("version")
	currentPath := r.PathValue("path")
	// since the object id may include escape sequences update both Path and
	// RawPath
	redirect := *r.URL
	if version == "" {
		// redirect url without version to head/
		redirect.Path = redirect.Path + "/head/"
		redirect.RawPath = redirect.RawPath + "/head/"
		http.Redirect(w, r, redirect.String(), http.StatusFound)
		return
	}
	if currentPath == "" {
		redirect.Path = redirect.Path + "/"
		redirect.RawPath = redirect.RawPath + "/"
		http.Redirect(w, r, redirect.String(), http.StatusFound)
		return
	}
}

// sort list of directory entries so that all sub-directories appear before
// files
func sortVersionDirEntries(entries []access.VersionDirEntry) {
	slices.SortFunc(entries, func(a, b access.VersionDirEntry) int {
		if a.IsDir() == b.IsDir() {
			return strings.Compare(a.Name(), b.Name())
		}
		if a.IsDir() {
			return -1
		}
		if b.IsDir() {
			return 1
		}
		return 0
	})
}
