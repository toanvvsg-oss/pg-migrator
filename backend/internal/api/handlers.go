package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"pg-migrator/internal/config"
	"pg-migrator/internal/migrator"
	"pg-migrator/internal/store"
)

type Server struct {
	store   *store.Store
	engine  *migrator.Engine
	cfg     *config.Config
	mux     *http.ServeMux
	handler http.Handler
}

func NewServer(st *store.Store, eng *migrator.Engine, cfg *config.Config) *Server {
	s := &Server{
		store:  st,
		engine: eng,
		cfg:    cfg,
		mux:    http.NewServeMux(),
	}
	s.routes()
	token := ""
	if cfg != nil {
		token = cfg.APIAuthToken
	}
	s.handler = AuthMiddleware(token, s.mux)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Security headers on API responses (including 401 unauthorized responses)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	s.handler.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/lab/preset", s.handleLabPreset)
	s.mux.HandleFunc("GET /api/lab/status", s.handleLabStatus)
	s.mux.HandleFunc("POST /api/lab/toxiproxy", s.handleToxiproxy)
	s.mux.HandleFunc("POST /api/lab/workload/toggle", s.handleWorkloadToggle)
	s.mux.HandleFunc("POST /api/lab/workload/reset", s.handleWorkloadReset)
	s.mux.HandleFunc("POST /api/lab/pgbouncer/reset", s.handlePgBouncerReset)

	s.mux.HandleFunc("GET /api/jobs", s.handleListJobs)
	s.mux.HandleFunc("POST /api/jobs", s.handleCreateJob)
	s.mux.HandleFunc("GET /api/jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("DELETE /api/jobs/{id}", s.handleDeleteJob)

	s.mux.HandleFunc("POST /api/jobs/{id}/preflight", s.handlePreflight)
	s.mux.HandleFunc("POST /api/jobs/{id}/fix-replica-identity", s.handleFixReplicaIdentity)
	s.mux.HandleFunc("POST /api/jobs/{id}/schema", s.handleSchemaSync)
	s.mux.HandleFunc("POST /api/jobs/{id}/start-replication", s.handleStartReplication)
	s.mux.HandleFunc("POST /api/jobs/{id}/verify", s.handleVerify)
	s.mux.HandleFunc("POST /api/jobs/{id}/cutover", s.handleCutover)
	s.mux.HandleFunc("POST /api/jobs/{id}/abort", s.handleAbort)

	s.mux.HandleFunc("GET /api/jobs/{id}/metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /api/jobs/{id}/logs", s.handleLogs)
	s.mux.HandleFunc("GET /api/jobs/{id}/stream", s.handleStream)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func decodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	return dec.Decode(dst)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusServiceUnavailable, "pg-meta unavailable")
		return
	}
	if err := s.store.Ping(r.Context()); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "pg-meta unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"storage": "postgresql (pg-meta)",
		"time":    time.Now().UTC(),
	})
}

func (s *Server) acquireJobLock(w http.ResponseWriter, id, op string) (func(), bool) {
	if s.engine == nil {
		return func() {}, true
	}
	unlock, err := s.engine.TryLockJob(id, op)
	if err != nil {
		if errors.Is(err, migrator.ErrJobLocked) {
			writeErr(w, http.StatusConflict, err.Error())
			return nil, false
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	return unlock, true
}

// handleLabPreset returns non-secret Docker Lab topology defaults.
// Passwords are injected server-side when use_lab_credentials=true so secrets never touch browser JS.
func (s *Server) handleLabPreset(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"name":                 "Docker Lab: PG 15 (Cloud A) -> PG 16 (Cloud B)",
		"source_host":          "pg-source",
		"source_port":          5432,
		"source_db":            "sourcedb",
		"source_user":          "migrator",
		"source_sslmode":       "disable",
		"repl_source_host":     "toxiproxy",
		"repl_source_port":     15432,
		"target_host":          "pg-target",
		"target_port":          5432,
		"target_db":            "targetdb",
		"target_user":          "migrator",
		"target_sslmode":       "disable",
		"publication_name":     "pub_lab_migration",
		"subscription_name":    "sub_lab_migration",
		"wal_guardrail_mb":     4096,
		"has_lab_credentials":  s.cfg.LabSourcePassword != "" && s.cfg.LabTargetPassword != "",
	})
}

type createJobReq struct {
	Name              string `json:"name"`
	SourceHost        string `json:"source_host"`
	SourcePort        int    `json:"source_port"`
	SourceDB          string `json:"source_db"`
	SourceUser        string `json:"source_user"`
	SourcePassword    string `json:"source_password"`
	SourceSSLMode     string `json:"source_sslmode"`
	ReplSourceHost    string `json:"repl_source_host"`
	ReplSourcePort    int    `json:"repl_source_port"`
	TargetHost        string `json:"target_host"`
	TargetPort        int    `json:"target_port"`
	TargetDB          string `json:"target_db"`
	TargetUser        string `json:"target_user"`
	TargetPassword    string `json:"target_password"`
	TargetSSLMode     string `json:"target_sslmode"`
	PublicationName   string `json:"publication_name"`
	SubscriptionName  string `json:"subscription_name"`
	WALGuardrailMB    int    `json:"wal_guardrail_mb"`
	UseLabCredentials bool   `json:"use_lab_credentials"`
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.ListJobs(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobReq
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	if req.UseLabCredentials {
		if req.SourcePassword == "" {
			req.SourcePassword = s.cfg.LabSourcePassword
		}
		if req.TargetPassword == "" {
			req.TargetPassword = s.cfg.LabTargetPassword
		}
	}
	if req.Name == "" || req.SourceHost == "" || req.TargetHost == "" || req.SourceDB == "" || req.TargetDB == "" {
		writeErr(w, http.StatusBadRequest, "name, source, and target connection details are required")
		return
	}
	if req.SourcePort <= 0 {
		req.SourcePort = 5432
	}
	if req.TargetPort <= 0 {
		req.TargetPort = 5432
	}
	if req.PublicationName == "" {
		req.PublicationName = "pub_pg_migrator"
	}
	if req.SubscriptionName == "" {
		req.SubscriptionName = "sub_pg_migrator"
	}
	if err := migrator.ValidateIdentifier(req.PublicationName); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := migrator.ValidateIdentifier(req.SubscriptionName); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	job, err := s.store.CreateJob(r.Context(), &store.Job{
		Name:             req.Name,
		SourceHost:       req.SourceHost,
		SourcePort:       req.SourcePort,
		SourceDB:         req.SourceDB,
		SourceUser:       req.SourceUser,
		SourcePassword:   req.SourcePassword,
		SourceSSLMode:    req.SourceSSLMode,
		ReplSourceHost:   req.ReplSourceHost,
		ReplSourcePort:   req.ReplSourcePort,
		TargetHost:       req.TargetHost,
		TargetPort:       req.TargetPort,
		TargetDB:         req.TargetDB,
		TargetUser:       req.TargetUser,
		TargetPassword:   req.TargetPassword,
		TargetSSLMode:    req.TargetSSLMode,
		PublicationName:  req.PublicationName,
		SubscriptionName: req.SubscriptionName,
		WALGuardrailMB:   req.WALGuardrailMB,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = s.store.AppendLog(r.Context(), job.ID, "create", "info", fmt.Sprintf("Created job %q (%s:%d/%s -> %s:%d/%s)",
		job.Name, job.SourceHost, job.SourcePort, job.SourceDB, job.TargetHost, job.TargetPort, job.TargetDB))
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	unlock, ok := s.acquireJobLock(w, id, "delete")
	if !ok {
		return
	}
	defer unlock()

	s.engine.StopMonitor(id)
	if err := s.store.DeleteJob(r.Context(), id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": id})
}

func (s *Server) handlePreflight(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	unlock, ok := s.acquireJobLock(w, id, "preflight")
	if !ok {
		return
	}
	defer unlock()

	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	report, err := s.engine.RunPreflight(r.Context(), job)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleFixReplicaIdentity(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	unlock, ok := s.acquireJobLock(w, id, "fix-replica-identity")
	if !ok {
		return
	}
	defer unlock()

	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	fixed, err := s.engine.FixReplicaIdentity(r.Context(), job)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"fixed_tables": fixed})
}

func (s *Server) handleSchemaSync(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	unlock, ok := s.acquireJobLock(w, id, "schema")
	if !ok {
		return
	}
	defer unlock()

	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	res, err := s.engine.SyncSchema(r.Context(), job)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleStartReplication(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	unlock, ok := s.acquireJobLock(w, id, "replicate")
	if !ok {
		return
	}
	defer unlock()

	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	if err := s.engine.StartReplication(r.Context(), job); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "replicating"})
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	unlock, ok := s.acquireJobLock(w, id, "verify")
	if !ok {
		return
	}
	defer unlock()

	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	res, err := s.engine.VerifyData(ctx, job)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleCutover(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	unlock, ok := s.acquireJobLock(w, id, "cutover")
	if !ok {
		return
	}
	defer unlock()

	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	var req migrator.CutoverRequest
	_ = decodeJSON(r, &req)
	res, err := s.engine.ExecuteCutover(r.Context(), job, req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleAbort(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	unlock, ok := s.acquireJobLock(w, id, "abort")
	if !ok {
		return
	}
	defer unlock()

	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	var body struct {
		ResetLab bool `json:"reset_lab"`
	}
	_ = decodeJSON(r, &body)
	if err := s.engine.AbortMigration(r.Context(), job, body.ResetLab); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "aborted"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics, err := s.store.ListMetrics(r.Context(), r.PathValue("id"), 120)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, metrics)
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	logs, err := s.store.ListLogs(r.Context(), r.PathValue("id"), 200)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, logs)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	jobID := r.PathValue("id")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch, unsub := s.engine.Subscribe(jobID)
	defer unsub()

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			_, _ = fmt.Fprintf(w, "event: metric\ndata: %s\n\n", msg)
			flusher.Flush()
		case <-keepalive.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// --- Docker Lab Control Proxies (Toxiproxy, Workload Sim, PgBouncer) ---

func (s *Server) handleLabStatus(w http.ResponseWriter, r *http.Request) {
	client := &http.Client{Timeout: 2 * time.Second}
	out := map[string]any{}

	if s.cfg.WorkloadStatsURL != "" {
		if resp, err := client.Get(strings.TrimRight(s.cfg.WorkloadStatsURL, "/") + "/stats"); err == nil {
			var v any
			if json.NewDecoder(resp.Body).Decode(&v) == nil {
				out["workload"] = v
			}
			resp.Body.Close()
		}
	}
	if s.cfg.PgBouncerControlURL != "" {
		if resp, err := client.Get(strings.TrimRight(s.cfg.PgBouncerControlURL, "/") + "/status"); err == nil {
			var v any
			if json.NewDecoder(resp.Body).Decode(&v) == nil {
				out["pgbouncer"] = v
			}
			resp.Body.Close()
		}
	}
	if s.cfg.ToxiproxyURL != "" {
		if resp, err := client.Get(strings.TrimRight(s.cfg.ToxiproxyURL, "/") + "/proxies/pg_source_repl"); err == nil {
			var v any
			if json.NewDecoder(resp.Body).Decode(&v) == nil {
				out["toxiproxy"] = v
			}
			resp.Body.Close()
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleToxiproxy(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ToxiproxyURL == "" {
		writeErr(w, http.StatusBadRequest, "toxiproxy not configured")
		return
	}
	var req struct {
		LatencyMs int  `json:"latency_ms"`
		JitterMs  int  `json:"jitter_ms"`
		Enabled   bool `json:"enabled"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid payload")
		return
	}
	base := strings.TrimRight(s.cfg.ToxiproxyURL, "/") + "/proxies/pg_source_repl"
	client := &http.Client{Timeout: 4 * time.Second}

	// Update proxy enabled state (simulates cross-cloud network cut / restore)
	pBody, _ := json.Marshal(map[string]any{"enabled": req.Enabled})
	pReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, base, bytes.NewReader(pBody))
	pReq.Header.Set("Content-Type", "application/json")
	if resp, err := client.Do(pReq); err == nil {
		resp.Body.Close()
	}

	// Delete existing latency toxic if present
	dReq, _ := http.NewRequestWithContext(r.Context(), http.MethodDelete, base+"/toxics/cross_cloud_latency", nil)
	if resp, err := client.Do(dReq); err == nil {
		resp.Body.Close()
	}

	// Add latency toxic if > 0
	if req.LatencyMs > 0 {
		tBody, _ := json.Marshal(map[string]any{
			"name":       "cross_cloud_latency",
			"type":       "latency",
			"stream":     "downstream",
			"toxicity":   1.0,
			"attributes": map[string]any{"latency": req.LatencyMs, "jitter": req.JitterMs},
		})
		tReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, base+"/toxics", bytes.NewReader(tBody))
		tReq.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(tReq)
		if err != nil {
			writeErr(w, http.StatusBadGateway, err.Error())
			return
		}
		resp.Body.Close()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":    req.Enabled,
		"latency_ms": req.LatencyMs,
		"jitter_ms":  req.JitterMs,
	})
}

func (s *Server) handleWorkloadToggle(w http.ResponseWriter, r *http.Request) {
	s.proxySimplePost(w, r, s.cfg.WorkloadStatsURL, "/toggle")
}

func (s *Server) handleWorkloadReset(w http.ResponseWriter, r *http.Request) {
	s.proxySimplePost(w, r, s.cfg.WorkloadStatsURL, "/reset-stats")
}

func (s *Server) handlePgBouncerReset(w http.ResponseWriter, r *http.Request) {
	s.proxySimplePost(w, r, s.cfg.PgBouncerControlURL, "/reset")
}

func (s *Server) proxySimplePost(w http.ResponseWriter, r *http.Request, baseURL, path string) {
	if baseURL == "" {
		writeErr(w, http.StatusBadRequest, "lab service not configured")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
