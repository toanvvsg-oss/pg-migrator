package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"path"
	"strings"
)

// AuthMiddleware enforces optional static token authentication on /api/* endpoints
// (exempting exact /api/health). When expectedToken is empty/whitespace, it is a no-op.
func AuthMiddleware(expectedToken string, next http.Handler) http.Handler {
	expectedToken = strings.TrimSpace(expectedToken)
	if expectedToken == "" {
		return next
	}

	// Hash expected token once to fixed 32-byte digest so ConstantTimeCompare never leaks token length.
	expectedSum := sha256.Sum256([]byte(expectedToken))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cleanPath := path.Clean(r.URL.Path)
		if !isAPIPath(cleanPath) || cleanPath == "/api/health" {
			next.ServeHTTP(w, r)
			return
		}

		candidate := extractRequestToken(r)
		if candidate == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="pg-migrator"`)
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		gotSum := sha256.Sum256([]byte(candidate))
		if subtle.ConstantTimeCompare(gotSum[:], expectedSum[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="pg-migrator"`)
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func isAPIPath(path string) bool {
	return path == "/api" || strings.HasPrefix(path, "/api/")
}

// extractRequestToken extracts a token from Authorization: Bearer <token> or X-API-Key.
// If an Authorization header is present, it must use the Bearer scheme (never falling back to X-API-Key).
// Query parameters are intentionally ignored to avoid leaking credentials in access logs.
func extractRequestToken(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth != "" {
		if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") {
			return strings.TrimSpace(auth[7:])
		}
		return ""
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}
