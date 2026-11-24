package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pg-migrator/internal/config"
	"pg-migrator/internal/migrator"
)

func TestJobActionEndpointsReturn409WhenLocked(t *testing.T) {
	cfg := &config.Config{}
	eng := migrator.NewEngine(nil, cfg)
	srv := NewServer(nil, eng, cfg)

	const jobID = "00000000-0000-0000-0000-000000000001"
	unlock, err := eng.TryLockJob(jobID, "cutover")
	if err != nil {
		t.Fatalf("TryLockJob failed: %v", err)
	}
	defer unlock()

	endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/jobs/" + jobID + "/schema"},
		{http.MethodPost, "/api/jobs/" + jobID + "/start-replication"},
		{http.MethodPost, "/api/jobs/" + jobID + "/verify"},
		{http.MethodPost, "/api/jobs/" + jobID + "/cutover"},
		{http.MethodPost, "/api/jobs/" + jobID + "/abort"},
		{http.MethodPost, "/api/jobs/" + jobID + "/preflight"},
		{http.MethodPost, "/api/jobs/" + jobID + "/fix-replica-identity"},
		{http.MethodDelete, "/api/jobs/" + jobID},
	}

	for _, ep := range endpoints {
		t.Run(ep.method+"_"+ep.path, func(t *testing.T) {
			req := httptest.NewRequest(ep.method, ep.path, nil)
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)

			if rec.Code != http.StatusConflict {
				t.Fatalf("got HTTP %d, want 409 Conflict (body=%s)", rec.Code, rec.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("invalid JSON error response: %v", err)
			}
			if !strings.Contains(body["error"], "cutover") {
				t.Errorf("expected error to mention 'cutover', got %q", body["error"])
			}
		})
	}
}

func TestHandleHealthNilStoreSafe(t *testing.T) {
	srv := NewServer(nil, nil, &config.Config{})
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got HTTP %d, want 503", rec.Code)
	}
}
