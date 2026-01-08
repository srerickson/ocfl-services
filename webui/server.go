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
	"github.com/srerickson/ocfl-services/webui/template"
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

	// object files view
	mux.HandleFunc("GET /object/{id}/{version}/{path...}", HandleGetObjectFiles(accessService))
	mux.HandleFunc("GET /object/{id}/{version}", redirectToDefaultObjectFiles)
	mux.HandleFunc("GET /object/{id}/", redirectToDefaultObjectFiles)
	mux.HandleFunc("GET /object/{id}", redirectToDefaultObjectFiles)

	mux.HandleFunc("GET /history/{id}", HandleGetObjectHistory(accessService))
	mux.HandleFunc("GET /history/{id}/{version}", HandleGetVersionChanges(accessService))

	// wrap with logging middleware
	return loggingMiddleware(accessService.Logger())(mux)
}

func HandleGetObjectFiles(svc *access.Service) http.HandlerFunc {

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
			entries, err := svc.ReadVersionDir(ctx, p.objID, p.ver.Num(), p.path)
			if err != nil {
				logErr(w, r, p, err)
				return
			}
			if r.Method == http.MethodHead {
				return
			}
			sortVersionDirEntries(entries)
			page := &template.ObjectFiles{
				ObjectID:         p.objID,
				CurrentPath:      p.path,
				VersionRef:       p.verRef,
				VNum:             p.ver,
				DigestAlgorithm:  obj.Alg(),
				DirectoryEntries: make([]*template.DirectoryEntry, 0, len(entries)),
				// Version: template.VersionBrief{
				// 	VNum:     p.ver,
				// 	Created:  ver.Created(),
				// 	Message:  ver.Message(),
				// 	UserName: ver.UserName(),
				// 	UserAddr: ver.UserAddr(),
				// },
			}
			if page.CurrentPath != "." {
				parentDirEntry := &template.DirectoryEntry{
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
				page.DirectoryEntries = append(page.DirectoryEntries, &template.DirectoryEntry{
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
			template.ObjectFilesPage(page).Render(r.Context(), w)
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

// buildFileTree converts a flat list of file changes into a hierarchical tree structure
func buildFileTree(changes []access.VersionFileChange) *template.FileTreeNode {
	root := &template.FileTreeNode{
		Name:     "/",
		Path:     "",
		IsDir:    true,
		Children: make([]*template.FileTreeNode, 0),
	}

	for _, change := range changes {
		filePath := change.Path()
		modType := change.Type()

		// Split path into segments
		segments := strings.Split(strings.Trim(filePath, "/"), "/")
		if len(segments) == 0 || segments[0] == "" {
			continue
		}

		// Navigate/create directory nodes
		current := root
		for i, segment := range segments {
			isLastSegment := i == len(segments)-1

			if isLastSegment {
				// This is the file - add it as a leaf node
				fileNode := &template.FileTreeNode{
					Name:    segment,
					Path:    filePath,
					IsDir:   false,
					ModType: modType,
				}
				current.Children = append(current.Children, fileNode)
			} else {
				// This is a directory - find or create it
				var dirNode *template.FileTreeNode
				for _, child := range current.Children {
					if child.IsDir && child.Name == segment {
						dirNode = child
						break
					}
				}

				if dirNode == nil {
					// Create new directory node
					dirPath := strings.Join(segments[:i+1], "/")
					dirNode = &template.FileTreeNode{
						Name:     segment,
						Path:     dirPath,
						IsDir:    true,
						Children: make([]*template.FileTreeNode, 0),
					}
					current.Children = append(current.Children, dirNode)
				}

				current = dirNode
			}
		}
	}

	// Sort all children recursively (directories first, then files, alphabetically)
	var sortNode func(*template.FileTreeNode)
	sortNode = func(node *template.FileTreeNode) {
		if len(node.Children) == 0 {
			return
		}

		slices.SortFunc(node.Children, func(a, b *template.FileTreeNode) int {
			// Directories come before files
			if a.IsDir && !b.IsDir {
				return -1
			}
			if !a.IsDir && b.IsDir {
				return 1
			}
			// Within same type, sort alphabetically
			return strings.Compare(a.Name, b.Name)
		})

		// Recursively sort children
		for _, child := range node.Children {
			if child.IsDir {
				sortNode(child)
			}
		}
	}
	sortNode(root)

	return root
}

func HandleGetObjectHistory(svc *access.Service) http.HandlerFunc {
	logErr := func(w http.ResponseWriter, r *http.Request, id string, err error) {
		if err == nil {
			return
		}
		if errors.Is(err, access.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		logAttrs := []slog.Attr{
			slog.String("object_id", id),
		}
		svc.Logger().LogAttrs(r.Context(), slog.LevelError, err.Error(), logAttrs...)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := r.PathValue("id")
		versions, err := svc.ListVersions(ctx, id)
		if err != nil {
			logErr(w, r, id, err)
			return
		}
		slices.Reverse(versions) // most recent first
		page := &template.ObjectHistory{
			ObjectID: id,
			Versions: make([]*template.VersionBrief, len(versions)),
		}
		for i, v := range versions {
			page.Versions[i] = &template.VersionBrief{
				VNum:     v.VNum(),
				Created:  v.Created(),
				Message:  v.Message(),
				UserName: v.UserName(),
				UserAddr: v.UserAddr(),
			}
		}
		template.ObjectHistoryPage(page).Render(ctx, w)
	}
}

func HandleGetVersionChanges(svc *access.Service) http.HandlerFunc {
	logErr := func(w http.ResponseWriter, r *http.Request, id string, version string, err error) {
		if err == nil {
			return
		}
		if errors.Is(err, access.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		logAttrs := []slog.Attr{
			slog.String("object_id", id),
			slog.String("version", version),
		}
		svc.Logger().LogAttrs(r.Context(), slog.LevelError, err.Error(), logAttrs...)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := r.PathValue("id")
		version := r.PathValue("version")

		// Parse version number
		var vn ocfl.VNum
		err := ocfl.ParseVNum(version, &vn)
		if err != nil {
			http.Error(w, "invalid version format", http.StatusBadRequest)
			return
		}

		// Sync object and validate version exists
		obj, err := svc.SyncObject(ctx, id)
		if err != nil {
			logErr(w, r, id, version, err)
			return
		}

		if vn.Num() > obj.Head().Num() {
			logErr(w, r, id, version, fmt.Errorf("version %s: %w", version, access.ErrNotFound))
			return
		}

		// Get version metadata
		versionInfo, err := svc.GetVersionInfo(ctx, id, vn.Num())
		if err != nil {
			logErr(w, r, id, version, err)
			return
		}

		// Calculate version range for comparison
		fromV := max(vn.Num()-1, 0)
		toV := vn.Num()

		// Get file changes
		changes, err := svc.GetVersionChanges(ctx, id, fromV, toV)
		if err != nil {
			logErr(w, r, id, version, err)
			return
		}

		// Build file tree
		fileTree := buildFileTree(changes)

		// Prepare page data
		page := &template.VersionChanges{
			ObjectID: id,
			Version: &template.VersionBrief{
				VNum:     versionInfo.VNum(),
				Created:  versionInfo.Created(),
				Message:  versionInfo.Message(),
				UserName: versionInfo.UserName(),
				UserAddr: versionInfo.UserAddr(),
			},
			FileTree: fileTree,
		}

		if vn.Num() < obj.Head().Num() {
			page.NextVNum, _ = vn.Next()
		}
		if vn.Num() > 1 {
			page.PrevVNum = ocfl.V(vn.Num()-1, vn.Padding())
		}

		template.VersionChangesPage(page).Render(ctx, w)
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
		template.Index().Render(r.Context(), w)
	}
}

func redirectToDefaultObjectFiles(w http.ResponseWriter, r *http.Request) {
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
