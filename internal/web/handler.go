package web

import (
	"bytes"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"
)

const contentSecurityPolicy = "default-src 'self'; base-uri 'none'; connect-src 'self'; font-src 'self'; frame-ancestors 'none'; img-src 'self' data:; object-src 'none'; script-src 'self'; style-src 'self'; form-action 'none'"

// NewHandler creates an embedded asset handler with SPA fallback routing.
// Only GET and HEAD are accepted. If index.html has not been built, SPA routes
// return Service Unavailable.
func NewHandler(assets fs.FS) http.Handler {
	return &assetHandler{assets: assets}
}

type assetHandler struct {
	assets fs.FS
}

func (h *assetHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	setSecurityHeaders(response.Header())

	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		response.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.assets == nil {
		http.Error(response, "web application is not built", http.StatusServiceUnavailable)
		return
	}

	requestedPath, ok := cleanAssetPath(request.URL.Path)
	if !ok {
		http.Error(response, "invalid path", http.StatusBadRequest)
		return
	}
	if requestedPath != "" && h.serveAsset(response, request, requestedPath) {
		return
	}
	if h.serveAsset(response, request, "index.html") {
		return
	}
	http.Error(response, "web application is not built", http.StatusServiceUnavailable)
}

// setSecurityHeaders applies browser hardening only to the Web asset handler.
// Connect RPC routes use separate handlers and response content types.
func setSecurityHeaders(header http.Header) {
	header.Set("Content-Security-Policy", contentSecurityPolicy)
	header.Set("Permissions-Policy", "camera=(), geolocation=(), microphone=()")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("X-Frame-Options", "DENY")
}

func (h *assetHandler) serveAsset(response http.ResponseWriter, request *http.Request, name string) bool {
	info, err := fs.Stat(h.assets, name)
	if err != nil || info.IsDir() {
		return false
	}
	contents, err := fs.ReadFile(h.assets, name)
	if err != nil {
		return false
	}
	if contentType := mime.TypeByExtension(path.Ext(name)); contentType != "" {
		response.Header().Set("Content-Type", contentType)
	}
	http.ServeContent(response, request, name, time.Time{}, bytes.NewReader(contents))
	return true
}

func cleanAssetPath(requestPath string) (string, bool) {
	if strings.ContainsRune(requestPath, '\\') || strings.ContainsRune(requestPath, '\x00') {
		return "", false
	}
	for _, segment := range strings.Split(requestPath, "/") {
		if segment == ".." {
			return "", false
		}
	}
	cleaned := path.Clean("/" + requestPath)
	name := strings.TrimPrefix(cleaned, "/")
	if name == "." || name == "" {
		return "", true
	}
	if !fs.ValidPath(name) {
		return "", false
	}
	return name, true
}
