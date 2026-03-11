package ui

import (
	"io/fs"
	"net/http"
	"strings"
)

// Handler returns an http.Handler that serves the embedded SPA.
// Static files are served directly; all other paths fall back to index.html
// to support client-side routing.
func Handler() http.Handler {
	distRoot, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic("ui: embedded dist directory not found: " + err.Error())
	}

	fileServer := http.FileServer(http.FS(distRoot))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Clean the path — strip leading slash for fs lookup.
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}

		// Check if the file exists in the embedded filesystem.
		f, err := distRoot.Open(path)
		if err == nil {
			f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}

		// For any path that doesn't match a static file, serve index.html
		// so that react-router can handle the route client-side.
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}
