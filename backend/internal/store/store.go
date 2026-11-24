package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"pg-migrator/internal/crypto"
)

//go:embed schema.sql
var schemaSQL string

type Job struct {
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	Status           string          `json:"status"`
	SourceHost       string          `json:"source_host"`
	SourcePort       int             `json:"source_port"`
	SourceDB         string          `json:"source_db"`
	SourceUser       string          `json:"source_user"`
	SourcePassword   string          `json:"-"` // never serialized to JSON
	SourceSSLMode    string          `json:"source_sslmode"`
	ReplSourceHost   string          `json:"repl_source_host"`
	ReplSourcePort   int             `json:"repl_source_port"`
	TargetHost       string          `json:"target_host"`
	TargetPort       int             `json:"target_port"`
	TargetDB         string          `json:"target_db"`
	TargetUser       string          `json:"target_user"`
	TargetPassword   string          `json:"-"` // never serialized to JSON
	TargetSSLMode    string          `json:"target_sslmode"`
	PublicationName  string          `json:"publication_name"`
	SubscriptionName string          `json:"subscription_name"`
	WALGuardrailMB   int             `json:"wal_guardrail_mb"`
	DeferredFKsSQL   string          `json:"-"`
	DeferredFKCount  int             `json:"deferred_fk_count"`
	FKsApplied       bool            `json:"fks_applied"`
	PreflightReport  json.RawMessage `json:"preflight_report,omitempty"`
	VerifyReport     json.RawMessage `json:"verify_report,omitempty"`
	CutoverReport    json.RawMessage `json:"cutover_report,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

type JobLog struct {
	ID        int64     `json:"id"`
	JobID     string    `json:"job_id"`
	Phase     string    `json:"phase"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

type MetricSample struct {
	ID               int64           `json:"id"`
	JobID            string          `json:"job_id"`
	RecordedAt       time.Time       `json:"recorded_at"`
	LagBytes         int64           `json:"lag_bytes"`
	WALRetainedBytes int64           `json:"wal_retained_bytes"`
	TablesTotal      int             `json:"tables_total"`
	TablesReady      int             `json:"tables_ready"`
	SourceSizeBytes  int64           `json:"source_size_bytes"`
	TargetSizeBytes  int64           `json:"target_size_bytes"`
	ReplicationState string          `json:"replication_state"`
	SlotActive       bool            `json:"slot_active"`
	TableStates      json.RawMessage `json:"table_states"`
}

type Store struct {
	pool *pgxpool.Pool
	box  *crypto.Box
}

func New(ctx context.Context, metaDBURL string, box *crypto.Box) (*Store, error) {
	var pool *pgxpool.Pool
	var err error
	for i := 0; i < 30; i++ {
		pool, err = pgxpool.New(ctx, metaDBURL)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				break
			} else {
				err = pingErr
				pool.Close()
			}
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("connect pg-meta: %w", err)
	}

	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate pg-meta schema: %w", err)
	}

	return &Store{pool: pool, box: box}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) CreateJob(ctx context.Context, j *Job) (*Job, error) {
	srcEnc, err := s.box.Encrypt(j.SourcePassword)
	if err != nil {
		return nil, err
	}
	tgtEnc, err := s.box.Encrypt(j.TargetPassword)
	if err != nil {
		return nil, err
	}

	if j.SourceSSLMode == "" {
		j.SourceSSLMode = "prefer"
	}
	if j.TargetSSLMode == "" {
		j.TargetSSLMode = "prefer"
	}
	if j.WALGuardrailMB <= 0 {
		j.WALGuardrailMB = 4096
	}

	row := s.pool.QueryRow(ctx, `
		INSERT INTO migration_jobs (
			name, status,
			source_host, source_port, source_db, source_user, source_password_enc, source_sslmode,
			repl_source_host, repl_source_port,
			target_host, target_port, target_db, target_user, target_password_enc, target_sslmode,
			publication_name, subscription_name, wal_guardrail_mb
		) VALUES (
			$1, 'created',
			$2, $3, $4, $5, $6, $7,
			$8, $9,
			$10, $11, $12, $13, $14, $15,
			$16, $17, $18
		)
		RETURNING id::text, created_at, updated_at
	`,
		j.Name,
		j.SourceHost, j.SourcePort, j.SourceDB, j.SourceUser, srcEnc, j.SourceSSLMode,
		j.ReplSourceHost, j.ReplSourcePort,
		j.TargetHost, j.TargetPort, j.TargetDB, j.TargetUser, tgtEnc, j.TargetSSLMode,
		j.PublicationName, j.SubscriptionName, j.WALGuardrailMB,
	)

	j.Status = "created"
	if err := row.Scan(&j.ID, &j.CreatedAt, &j.UpdatedAt); err != nil {
		return nil, err
	}
	return j, nil
}

func (s *Store) scanJob(row pgx.Row) (*Job, error) {
	var j Job
	var srcEnc, tgtEnc string
	var preflight, verify, cutover []byte

	err := row.Scan(
		&j.ID, &j.Name, &j.Status,
		&j.SourceHost, &j.SourcePort, &j.SourceDB, &j.SourceUser, &srcEnc, &j.SourceSSLMode,
		&j.ReplSourceHost, &j.ReplSourcePort,
		&j.TargetHost, &j.TargetPort, &j.TargetDB, &j.TargetUser, &tgtEnc, &j.TargetSSLMode,
		&j.PublicationName, &j.SubscriptionName, &j.WALGuardrailMB,
		&j.DeferredFKsSQL, &j.FKsApplied,
		&preflight, &verify, &cutover,
		&j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	j.SourcePassword, err = s.box.Decrypt(srcEnc)
	if err != nil {
		return nil, fmt.Errorf("decrypt source password: %w", err)
	}
	j.TargetPassword, err = s.box.Decrypt(tgtEnc)
	if err != nil {
		return nil, fmt.Errorf("decrypt target password: %w", err)
	}
	if len(preflight) > 0 {
		j.PreflightReport = json.RawMessage(preflight)
	}
	if len(verify) > 0 {
		j.VerifyReport = json.RawMessage(verify)
	}
	if len(cutover) > 0 {
		j.CutoverReport = json.RawMessage(cutover)
	}
	if j.DeferredFKsSQL != "" {
		// count statements roughly by counting "FOREIGN KEY"
		cnt := 0
		for i := 0; i+11 <= len(j.DeferredFKsSQL); i++ {
			if j.DeferredFKsSQL[i:i+11] == "FOREIGN KEY" {
				cnt++
			}
		}
		j.DeferredFKCount = cnt
	}
	return &j, nil
}

const jobSelectCols = `
	id::text, name, status,
	source_host, source_port, source_db, source_user, source_password_enc, source_sslmode,
	repl_source_host, repl_source_port,
	target_host, target_port, target_db, target_user, target_password_enc, target_sslmode,
	publication_name, subscription_name, wal_guardrail_mb,
	deferred_fks_sql, fks_applied,
	preflight_report, verify_report, cutover_report,
	created_at, updated_at
`

func (s *Store) GetJob(ctx context.Context, id string) (*Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobSelectCols+` FROM migration_jobs WHERE id = $1::uuid`, id)
	return s.scanJob(row)
}

func (s *Store) ListJobs(ctx context.Context) ([]*Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobSelectCols+` FROM migration_jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*Job, 0)
	for rows.Next() {
		j, err := s.scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (s *Store) DeleteJob(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM migration_jobs WHERE id = $1::uuid`, id)
	return err
}

func (s *Store) UpdateStatus(ctx context.Context, id, status string) error {
	_, err := s.pool.Exec(ctx, `UPDATE migration_jobs SET status = $2, updated_at = NOW() WHERE id = $1::uuid`, id, status)
	return err
}

func (s *Store) SavePreflightReport(ctx context.Context, id string, report any, status string) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE migration_jobs
		SET preflight_report = $2::jsonb, status = $3, updated_at = NOW()
		WHERE id = $1::uuid
	`, id, string(raw), status)
	return err
}

func (s *Store) SaveDeferredFKs(ctx context.Context, id, fkSQL string, applied bool) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE migration_jobs
		SET deferred_fks_sql = $2, fks_applied = $3, updated_at = NOW()
		WHERE id = $1::uuid
	`, id, fkSQL, applied)
	return err
}

func (s *Store) MarkFKsApplied(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE migration_jobs
		SET fks_applied = TRUE, updated_at = NOW()
		WHERE id = $1::uuid
	`, id)
	return err
}

func (s *Store) SaveVerifyReport(ctx context.Context, id string, report any) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE migration_jobs
		SET verify_report = $2::jsonb, updated_at = NOW()
		WHERE id = $1::uuid
	`, id, string(raw))
	return err
}

func (s *Store) SaveCutoverReport(ctx context.Context, id string, report any, status string) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE migration_jobs
		SET cutover_report = $2::jsonb, status = $3, updated_at = NOW()
		WHERE id = $1::uuid
	`, id, string(raw), status)
	return err
}

func (s *Store) AppendLog(ctx context.Context, jobID, phase, level, message string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO job_logs (job_id, phase, level, message)
		VALUES ($1::uuid, $2, $3, $4)
	`, jobID, phase, level, message)
	return err
}

func (s *Store) ListLogs(ctx context.Context, jobID string, limit int) ([]JobLog, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_id::text, phase, level, message, created_at
		FROM job_logs
		WHERE job_id = $1::uuid
		ORDER BY id DESC
		LIMIT $2
	`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	logs := make([]JobLog, 0)
	for rows.Next() {
		var l JobLog
		if err := rows.Scan(&l.ID, &l.JobID, &l.Phase, &l.Level, &l.Message, &l.CreatedAt); err != nil {
			return nil, err
		}
		logs = append(logs, l)
	}
	return logs, rows.Err()
}

func (s *Store) InsertMetric(ctx context.Context, m *MetricSample) error {
	tableJSON := string(m.TableStates)
	if tableJSON == "" {
		tableJSON = "[]"
	}
	return s.pool.QueryRow(ctx, `
		INSERT INTO metrics_samples (
			job_id, lag_bytes, wal_retained_bytes, tables_total, tables_ready,
			source_size_bytes, target_size_bytes, replication_state, slot_active, table_states
		) VALUES (
			$1::uuid, $2, $3, $4, $5,
			$6, $7, $8, $9, $10::jsonb
		)
		RETURNING id, recorded_at
	`,
		m.JobID, m.LagBytes, m.WALRetainedBytes, m.TablesTotal, m.TablesReady,
		m.SourceSizeBytes, m.TargetSizeBytes, m.ReplicationState, m.SlotActive, tableJSON,
	).Scan(&m.ID, &m.RecordedAt)
}

func (s *Store) ListMetrics(ctx context.Context, jobID string, limit int) ([]MetricSample, error) {
	if limit <= 0 || limit > 300 {
		limit = 90
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_id::text, recorded_at, lag_bytes, wal_retained_bytes,
		       tables_total, tables_ready, source_size_bytes, target_size_bytes,
		       replication_state, slot_active, table_states
		FROM (
			SELECT * FROM metrics_samples
			WHERE job_id = $1::uuid
			ORDER BY recorded_at DESC
			LIMIT $2
		) sub
		ORDER BY recorded_at ASC
	`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]MetricSample, 0)
	for rows.Next() {
		var m MetricSample
		var tbl []byte
		if err := rows.Scan(
			&m.ID, &m.JobID, &m.RecordedAt, &m.LagBytes, &m.WALRetainedBytes,
			&m.TablesTotal, &m.TablesReady, &m.SourceSizeBytes, &m.TargetSizeBytes,
			&m.ReplicationState, &m.SlotActive, &tbl,
		); err != nil {
			return nil, err
		}
		m.TableStates = json.RawMessage(tbl)
		out = append(out, m)
	}
	return out, rows.Err()
}
