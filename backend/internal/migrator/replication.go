package migrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"pg-migrator/internal/store"
)

func (e *Engine) StartReplication(ctx context.Context, j *store.Job) error {
	if err := ValidateIdentifier(j.PublicationName); err != nil {
		return err
	}
	if err := ValidateIdentifier(j.SubscriptionName); err != nil {
		return err
	}

	srcConn, err := connectSource(ctx, j)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer srcConn.Close(ctx)

	tgtConn, err := connectTarget(ctx, j)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer tgtConn.Close(ctx)

	// 1. Discover user tables on source (non-superuser friendly explicit table list)
	rows, err := srcConn.Query(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, c.relname
	`)
	if err != nil {
		return fmt.Errorf("list source tables: %w", err)
	}
	var tableIdents []string
	for rows.Next() {
		var sch, tbl string
		if err := rows.Scan(&sch, &tbl); err == nil {
			tableIdents = append(tableIdents, pgx.Identifier{sch, tbl}.Sanitize())
		}
	}
	rows.Close()
	if len(tableIdents) == 0 {
		return fmt.Errorf("no user tables found on source database")
	}

	// 2. Create Publication on Source (idempotent check)
	var pubExists bool
	_ = srcConn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", j.PublicationName).Scan(&pubExists)
	if !pubExists {
		pubSQL := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgx.Identifier{j.PublicationName}.Sanitize(), strings.Join(tableIdents, ", "))
		if _, err := srcConn.Exec(ctx, pubSQL); err != nil {
			e.log(ctx, j.ID, "replication", "error", "Failed to create publication: %v", err)
			return fmt.Errorf("create publication: %w", err)
		}
		e.log(ctx, j.ID, "replication", "info", "Created publication %q covering %d tables on source.", j.PublicationName, len(tableIdents))
	} else {
		e.log(ctx, j.ID, "replication", "warn", "Publication %q already exists on source; reusing.", j.PublicationName)
	}

	// 3. Create Subscription on Target
	replHost := j.SourceHost
	replPort := j.SourcePort
	if j.ReplSourceHost != "" {
		replHost = j.ReplSourceHost
	}
	if j.ReplSourcePort > 0 {
		replPort = j.ReplSourcePort
	}

	// Safely quote libpq conninfo values inside single quotes for CREATE SUBSCRIPTION
	connInfo := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		escapeConnVal(replHost),
		replPort,
		escapeConnVal(j.SourceDB),
		escapeConnVal(j.SourceUser),
		escapeConnVal(j.SourcePassword),
		escapeConnVal(j.SourceSSLMode),
	)

	var subExists bool
	_ = tgtConn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)", j.SubscriptionName).Scan(&subExists)
	if !subExists {
		subSQL := fmt.Sprintf(
			"CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s WITH (copy_data = true, create_slot = true, enabled = true)",
			pgx.Identifier{j.SubscriptionName}.Sanitize(),
			strings.ReplaceAll(connInfo, "'", "''"),
			pgx.Identifier{j.PublicationName}.Sanitize(),
		)
		if _, err := tgtConn.Exec(ctx, subSQL); err != nil {
			e.log(ctx, j.ID, "replication", "error", "Failed to create subscription: %v", err)
			return fmt.Errorf("create subscription: %w", err)
		}
		e.log(ctx, j.ID, "replication", "info", "Created subscription %q on target (streaming via %s:%d). Initial table copy started.",
			j.SubscriptionName, replHost, replPort)
	} else {
		e.log(ctx, j.ID, "replication", "warn", "Subscription %q already exists on target; attaching monitor.", j.SubscriptionName)
	}

	if err := e.store.UpdateStatus(ctx, j.ID, "replicating"); err != nil {
		return err
	}

	e.EnsureMonitorRunning(j.ID)
	return nil
}

func escapeConnVal(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	if strings.ContainsAny(v, " \t\n'") {
		return "'" + v + "'"
	}
	return v
}

func (e *Engine) EnsureMonitorRunning(jobID string) {
	e.mu.Lock()
	if _, exists := e.monitors[jobID]; exists {
		e.mu.Unlock()
		return
	}
	monCtx, cancel := context.WithCancel(context.Background())
	e.monitors[jobID] = cancel
	e.mu.Unlock()

	go e.monitorLoop(monCtx, jobID)
}

func (e *Engine) StopMonitor(jobID string) {
	e.mu.Lock()
	if cancel, ok := e.monitors[jobID]; ok {
		cancel()
		delete(e.monitors, jobID)
	}
	e.mu.Unlock()
}

func (e *Engine) ResumeActiveMonitors(ctx context.Context) {
	jobs, err := e.store.ListJobs(ctx)
	if err != nil {
		return
	}
	for _, j := range jobs {
		if j.Status == "replicating" || j.Status == "verified" {
			e.EnsureMonitorRunning(j.ID)
		}
	}
}

func (e *Engine) monitorLoop(ctx context.Context, jobID string) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	warnedWAL := false

	for {
		sample, err := e.collectSample(ctx, jobID)
		if err == nil && sample != nil {
			_ = e.store.InsertMetric(ctx, sample)

			// WAL Guardrail check
			j, jErr := e.store.GetJob(ctx, jobID)
			if jErr == nil && j.WALGuardrailMB > 0 {
				limitBytes := int64(j.WALGuardrailMB) * 1024 * 1024
				if sample.WALRetainedBytes > (limitBytes*80)/100 && !warnedWAL {
					warnedWAL = true
					e.log(ctx, jobID, "monitor", "warn", "WAL Guardrail WARNING: Source WAL retained (%.1f MB) exceeded 80%% of %d MB threshold!",
						float64(sample.WALRetainedBytes)/(1024*1024), j.WALGuardrailMB)
				} else if sample.WALRetainedBytes <= (limitBytes*60)/100 {
					warnedWAL = false
				}
			}

			if raw, mErr := json.Marshal(sample); mErr == nil {
				e.broadcast(jobID, raw)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (e *Engine) collectSample(ctx context.Context, jobID string) (*store.MetricSample, error) {
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	j, err := e.store.GetJob(pollCtx, jobID)
	if err != nil {
		return nil, err
	}
	if j.Status != "replicating" && j.Status != "verified" {
		return nil, nil
	}

	sample := &store.MetricSample{
		JobID: jobID,
	}

	// Source metrics: WAL retained, lag bytes, replication state, DB size
	srcConn, err := connectSource(pollCtx, j)
	if err == nil {
		defer srcConn.Close(pollCtx)
		_ = srcConn.QueryRow(pollCtx, "SELECT pg_database_size(current_database())").Scan(&sample.SourceSizeBytes)

		var lagBytes, walRetained int64
		var active bool
		err = srcConn.QueryRow(pollCtx, `
			SELECT
				COALESCE(active, false),
				COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint,
				COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint
			FROM pg_replication_slots
			WHERE slot_name = $1
		`, j.SubscriptionName).Scan(&active, &lagBytes, &walRetained)
		if err == nil {
			sample.SlotActive = active
			if lagBytes < 0 {
				lagBytes = 0
			}
			if walRetained < 0 {
				walRetained = 0
			}
			sample.LagBytes = lagBytes
			sample.WALRetainedBytes = walRetained
		}

		var repState string
		_ = srcConn.QueryRow(pollCtx, `
			SELECT COALESCE(state, '')
			FROM pg_stat_replication
			WHERE application_name = $1
			LIMIT 1
		`, j.SubscriptionName).Scan(&repState)
		if repState == "" {
			if sample.SlotActive {
				repState = "active"
			} else {
				repState = "disconnected"
			}
		}
		sample.ReplicationState = repState
	}

	// Target metrics: per-table subscription states & sizes
	tgtConn, err := connectTarget(pollCtx, j)
	if err == nil {
		defer tgtConn.Close(pollCtx)
		_ = tgtConn.QueryRow(pollCtx, "SELECT pg_database_size(current_database())").Scan(&sample.TargetSizeBytes)

		rows, qErr := tgtConn.Query(pollCtx, `
			SELECT
				n.nspname,
				c.relname,
				sr.srsubstate::text,
				pg_total_relation_size(c.oid)::bigint,
				GREATEST(c.reltuples, 0)::bigint
			FROM pg_subscription_rel sr
			JOIN pg_subscription s ON s.oid = sr.srsubid
			JOIN pg_class c ON c.oid = sr.srrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE s.subname = $1
			ORDER BY pg_total_relation_size(c.oid) DESC, n.nspname, c.relname
		`, j.SubscriptionName)
		if qErr == nil {
			defer rows.Close()
			states := make([]TableSyncState, 0)
			for rows.Next() {
				var ts TableSyncState
				if rows.Scan(&ts.Schema, &ts.Table, &ts.State, &ts.TargetBytes, &ts.EstRows) == nil {
					ts.StateLabel = subStateLabel(ts.State)
					sample.TablesTotal++
					if ts.State == "r" {
						sample.TablesReady++
					}
					states = append(states, ts)
				}
			}
			if raw, mErr := json.Marshal(states); mErr == nil {
				sample.TableStates = raw
			}
		}
	}

	return sample, nil
}

func subStateLabel(code string) string {
	switch code {
	case "i":
		return "Initializing"
	case "d":
		return "Copying Data"
	case "f":
		return "Finished Copy"
	case "s":
		return "Synchronized"
	case "r":
		return "Streaming (Ready)"
	default:
		return code
	}
}
