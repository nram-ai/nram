package ui

import (
	"io/fs"
	"net/http"
	"strings"
)

// distRoot is the embedded dist/ subtree, the root for all served UI assets.
// Resolved once at package init; both Handler and DocsHandler read from it.
var distRoot = func() fs.FS {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic("ui: embedded dist directory not found: " + err.Error())
	}
	return sub
}()

// Handler returns an http.Handler that serves the embedded SPA.
// Static files are served directly; all other paths fall back to index.html
// to support client-side routing.
func Handler() http.Handler {
	fileServer := http.FileServer(http.FS(distRoot))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Clean the path: strip leading slash for fs lookup.
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}

		// Check if the file exists in the embedded filesystem.
		f, err := distRoot.Open(path)
		if err == nil {
			_ = f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}

		// For any path that doesn't match a static file, serve index.html
		// so that react-router can handle the route client-side.
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}

// DocsHandler serves the standalone public API reference page built by the UI
// pipeline at dist/docs/index.html. It is mounted at GET /docs so the page has
// a clean URL; the page's hashed JS/CSS assets under /assets are served by the
// SPA Handler. This is kept separate from Handler because that handler's SPA
// fallback rewrites unknown paths to index.html, and the embedded fs cannot
// open the "docs/" directory path directly (io/fs rejects the trailing slash),
// so /docs would otherwise fall through to the console shell.
func DocsHandler() http.Handler {
	page, err := fs.ReadFile(distRoot, "docs/index.html")
	if err != nil {
		panic("ui: embedded docs/index.html not found: " + err.Error())
	}

	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(page)
	})
}
