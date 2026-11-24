package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"pg-migrator/internal/config"
)

func TestAuthMiddlewareDisabledWhenEmptyOrWhitespace(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	for _, tok := range []string{"", "   \t\n"} {
		h := AuthMiddleware(tok, okHandler)
		req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("token=%q: expected 200 when auth disabled, got %d", tok, rec.Code)
		}
	}
}

func TestAuthMiddlewareEnforcement(t *testing.T) {
	const secret = "top-secret-token-12345"

	srv := NewServer(nil, nil, &config.Config{APIAuthToken: secret})

	// Wrap with a test handler to verify middleware behavior directly as well
	okNext := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mw := AuthMiddleware(secret, okNext)

	t.Run("health_exempt_exact_match", func(t *testing.T) {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			req := httptest.NewRequest(method, "/api/health", nil)
			rec := httptest.NewRecorder()
			mw.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s /api/health: got %d, want 200", method, rec.Code)
			}
		}

		for _, bypassPath := range []string{"/api/healthz", "/api/health/sub"} {
			req := httptest.NewRequest(http.MethodGet, bypassPath, nil)
			rec := httptest.NewRecorder()
			mw.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("path %s: got %d, want 401", bypassPath, rec.Code)
			}
		}
	})

	t.Run("valid_credentials", func(t *testing.T) {
		cases := []struct {
			name   string
			header string
			val    string
		}{
			{"bearer", "Authorization", "Bearer " + secret},
			{"bearer_lowercase", "Authorization", "bearer " + secret},
			{"x_api_key", "X-API-Key", secret},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
				req.Header.Set(tc.header, tc.val)
				rec := httptest.NewRecorder()
				mw.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Fatalf("got %d, want 200", rec.Code)
				}
			})
		}
	})

	t.Run("rejected_credentials", func(t *testing.T) {
		cases := []struct {
			name    string
			url     string
			headers map[string]string
		}{
			{"missing_header", "/api/jobs", nil},
			{"wrong_short", "/api/jobs", map[string]string{"Authorization": "Bearer short"}},
			{"wrong_equal_len", "/api/jobs", map[string]string{"Authorization": "Bearer top-secret-token-12346"}},
			{"wrong_long", "/api/jobs", map[string]string{"Authorization": "Bearer " + secret + "-extra"}},
			{"empty_bearer", "/api/jobs", map[string]string{"Authorization": "Bearer "}},
			{"basic_with_valid_api_key", "/api/jobs", map[string]string{"Authorization": "Basic dXNlcjpwYXNz", "X-API-Key": secret}},
			{"query_param_not_accepted", "/api/jobs?api_key=" + secret, nil},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, tc.url, nil)
				for k, v := range tc.headers {
					req.Header.Set(k, v)
				}
				rec := httptest.NewRecorder()
				srv.ServeHTTP(rec, req)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("got %d, want 401", rec.Code)
				}
				if rec.Header().Get("WWW-Authenticate") == "" {
					t.Errorf("missing WWW-Authenticate header on 401")
				}
				if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
					t.Errorf("missing X-Content-Type-Options header on 401")
				}
				if rec.Header().Get("X-Frame-Options") != "DENY" {
					t.Errorf("missing X-Frame-Options header on 401")
				}
			})
		}
	})
}

func TestConfigLoadAPIAuthToken(t *testing.T) {
	t.Setenv("META_DB_URL", "postgres://u:p@localhost:5432/db")
	t.Setenv("APP_ENCRYPTION_KEY", "12345678901234567890")
	t.Setenv("API_AUTH_TOKEN", "  my-token-abc  ")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load failed: %v", err)
	}
	if cfg.APIAuthToken != "my-token-abc" {
		t.Fatalf("APIAuthToken = %q, want %q", cfg.APIAuthToken, "my-token-abc")
	}
}
