package migrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"pg-migrator/internal/store"
)

func (e *Engine) ExecuteCutover(ctx context.Context, j *store.Job, req CutoverRequest) (*CutoverReport, error) {
	if req.MaxWaitLagSeconds <= 0 {
		req.MaxWaitLagSeconds = 30
	}

	e.log(ctx, j.ID, "cutover", "info", "Initiating Guided Zero-Downtime Cutover (pgbouncer=%v, readonly=%v, deferred_fks=%v)...",
		req.UsePgBouncer, req.SetSourceReadOnly, req.ApplyDeferredFKs)

	srcConn, err := connectSource(ctx, j)
	if err != nil {
		return nil, fmt.Errorf("connect source: %w", err)
	}
	defer srcConn.Close(ctx)

	tgtConn, err := connectTarget(ctx, j)
	if err != nil {
		return nil, fmt.Errorf("connect target: %w", err)
	}
	defer tgtConn.Close(ctx)

	report := &CutoverReport{
		Steps: make([]CutoverStep, 0),
	}

	addStep := func(name, status string, start time.Time, detail string) {
		report.Steps = append(report.Steps, CutoverStep{
			Name:       name,
			Status:     status,
			DurationMs: time.Since(start).Milliseconds(),
			Detail:     detail,
		})
	}

	// Step 1: Verify subscription exists and ALL tables are in 'r' (ready/streaming) state BEFORE pausing traffic
	s1 := time.Now()
	var totalTables, readyTables int
	err = tgtConn.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE sr.srsubstate = 'r')
		FROM pg_subscription_rel sr
		JOIN pg_subscription s ON s.oid = sr.srsubid
		WHERE s.subname = $1
	`, j.SubscriptionName).Scan(&totalTables, &readyTables)
	if err != nil {
		return nil, fmt.Errorf("check subscription readiness: %w", err)
	}
	if totalTables == 0 {
		return nil, fmt.Errorf("subscription %q is missing or has 0 tables on target — run Schema Sync and Start Replication first", j.SubscriptionName)
	}
	if readyTables < totalTables {
		return nil, fmt.Errorf("%d/%d tables are still copying/initializing; wait until all tables are Streaming (r) before cutover", totalTables-readyTables, totalTables)
	}
	addStep("Verify Streaming Readiness", "ok", s1, fmt.Sprintf("All %d subscribed tables are in 'r' (Streaming) state", totalTables))

	// Step 2: Apply deferred Foreign Key constraints BEFORE pausing traffic (uses NOT VALID + VALIDATE)
	if req.ApplyDeferredFKs && !j.FKsApplied && strings.TrimSpace(j.DeferredFKsSQL) != "" {
		s2 := time.Now()
		cnt, fkErr := e.ApplyDeferredFKs(ctx, j)
		if fkErr != nil {
			addStep("Apply Deferred Foreign Keys", "warn", s2, fkErr.Error())
			e.log(ctx, j.ID, "cutover", "warn", "Deferred FK warning: %v", fkErr)
		} else {
			report.FKsApplied = cnt
			addStep("Apply Deferred Foreign Keys", "ok", s2, fmt.Sprintf("Applied & validated %d FK constraints online", cnt))
		}
	}

	// ---- DOWNTIME WINDOW STARTS HERE ----
	downtimeStart := time.Now()
	pgbPaused := false
	pgbSwitched := false
	sourceFrozen := false
	cutoverSucceeded := false

	// Safety defer: if cutover fails mid-flight, unfreeze source (unless PgBouncer already switched)
	// and resume PgBouncer so app is never stuck.
	defer func() {
		if !cutoverSucceeded {
			rbCtx, rbCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer rbCancel()

			if sourceFrozen && !pgbSwitched {
				e.unfreezeSource(rbCtx, srcConn, j)
			}
			if pgbPaused {
				if err := e.callPgBouncer(rbCtx, "/resume", nil); err != nil {
					e.log(rbCtx, j.ID, "cutover", "error", "Rollback PgBouncer /resume failed: %v", err)
				} else {
					e.log(rbCtx, j.ID, "cutover", "warn", "Rollback: PgBouncer resumed cleanly.")
				}
			}
		}
	}()

	// Step 3: Pause PgBouncer (queues live app queries cleanly without dropping TCP connections)
	if req.UsePgBouncer && e.cfg.PgBouncerControlURL != "" {
		s3 := time.Now()
		if err := e.callPgBouncer(ctx, "/pause", nil); err != nil {
			return nil, fmt.Errorf("pgbouncer pause failed: %w", err)
		}
		pgbPaused = true
		addStep("Pause PgBouncer Pool", "ok", s3, "Incoming app transactions queued cleanly in PgBouncer")
		e.log(ctx, j.ID, "cutover", "info", "PgBouncer PAUSE executed.")
	}

	// Step 4: Set Source DB to read-only (fail-closed) and quiesce active non-migrator sessions
	if req.SetSourceReadOnly {
		s4 := time.Now()
		roSQL := fmt.Sprintf("ALTER DATABASE %s SET default_transaction_read_only = on", pgx.Identifier{j.SourceDB}.Sanitize())
		if _, err := srcConn.Exec(ctx, roSQL); err != nil {
			e.log(ctx, j.ID, "cutover", "error", "Failed to set source read-only: %v", err)
			addStep("Freeze Source Writes", "fail", s4, err.Error())
			return nil, fmt.Errorf("set source read-only failed: %w", err)
		}
		sourceFrozen = true
		// Keep migrator's own session writable for teardown DDL (DROP PUBLICATION / pg_drop_replication_slot)
		_, _ = srcConn.Exec(ctx, "SET default_transaction_read_only = off")

		status, detail, qErr := e.quiesceSourceSessions(ctx, srcConn, pgbPaused)
		if qErr != nil {
			e.log(ctx, j.ID, "cutover", "error", "Source write-freeze quiesce failed: %v", qErr)
			addStep("Freeze Source Writes", "fail", s4, qErr.Error())
			return nil, qErr
		}
		addStep("Freeze Source Writes", status, s4, detail)
		e.log(ctx, j.ID, "cutover", "info", "Source write-freeze complete (%s): %s", status, detail)
	}

	// Step 5: Wait for WAL replication lag to hit 0 bytes
	s5 := time.Now()
	deadline := time.Now().Add(time.Duration(req.MaxWaitLagSeconds) * time.Second)
	var finalLag int64 = -1
	for time.Now().Before(deadline) {
		var lag int64
		qErr := srcConn.QueryRow(ctx, `
			SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
			FROM pg_replication_slots
			WHERE slot_name = $1
		`, j.SubscriptionName).Scan(&lag)
		if qErr == nil {
			finalLag = lag
			if lag <= 0 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if finalLag != 0 {
		if finalLag < 0 {
			return nil, fmt.Errorf("replication slot %q unavailable while draining WAL lag", j.SubscriptionName)
		}
		return nil, fmt.Errorf("replication lag did not drain to 0 within %ds (remaining: %d bytes)", req.MaxWaitLagSeconds, finalLag)
	}
	addStep("Drain WAL Lag to Zero", "ok", s5, "confirmed_flush_lsn caught up to current WAL LSN (0 bytes lag)")

	// Step 6: Sync ALL Sequences from Source to Target
	s6 := time.Now()
	srcSeqs := fetchSequences(ctx, srcConn)
	synced := 0
	for key, info := range srcSeqs {
		parts := strings.SplitN(key, ".", 2)
		fqSeq := pgx.Identifier{parts[0], parts[1]}.Sanitize()
		nextVal := info.val + req.SequenceBuffer
		if nextVal < 1 {
			nextVal = 1
		}
		if _, err := tgtConn.Exec(ctx, "SELECT setval($1::regclass, $2, $3)", fqSeq, nextVal, info.isCalled); err != nil {
			return nil, fmt.Errorf("sync sequence %s failed: %w", key, err)
		}
		synced++
	}
	report.SequencesSynced = synced
	addStep("Synchronize Sequences", "ok", s6, fmt.Sprintf("Synced %d sequences (+%d buffer) to target", synced, req.SequenceBuffer))
	e.log(ctx, j.ID, "cutover", "info", "Synchronized %d sequences to target.", synced)

	// Step 7: Drop Subscription on Target & Publication/Slot on Source
	s7 := time.Now()
	e.StopMonitor(j.ID)
	subIdent := pgx.Identifier{j.SubscriptionName}.Sanitize()
	pubIdent := pgx.Identifier{j.PublicationName}.Sanitize()

	_, _ = tgtConn.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s DISABLE", subIdent))
	_, _ = tgtConn.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s SET (slot_name = NONE)", subIdent))
	if _, err := tgtConn.Exec(ctx, fmt.Sprintf("DROP SUBSCRIPTION IF EXISTS %s", subIdent)); err != nil {
		e.log(ctx, j.ID, "cutover", "warn", "Drop subscription warning: %v", err)
	}
	_, _ = srcConn.Exec(ctx, "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1", j.SubscriptionName)
	_, _ = srcConn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubIdent))
	addStep("Teardown Replication & Drop Slot", "ok", s7, "Dropped subscription, publication, and freed source replication slot")

	// Step 8: Switch PgBouncer backend to Target & Resume
	if req.UsePgBouncer && e.cfg.PgBouncerControlURL != "" {
		s8 := time.Now()
		if err := e.callPgBouncer(ctx, "/switch", map[string]any{
			"host":   j.TargetHost,
			"port":   j.TargetPort,
			"dbname": j.TargetDB,
		}); err != nil {
			return nil, fmt.Errorf("pgbouncer switch failed: %w", err)
		}
		pgbSwitched = true
		if err := e.callPgBouncer(ctx, "/resume", nil); err != nil {
			return nil, fmt.Errorf("pgbouncer resume failed: %w", err)
		}
		pgbPaused = false
		addStep("Switch Traffic & Resume PgBouncer", "ok", s8, fmt.Sprintf("Switched PgBouncer backend to %s:%d/%s and resumed traffic", j.TargetHost, j.TargetPort, j.TargetDB))
	}

	cutoverSucceeded = true
	report.DowntimeMs = time.Since(downtimeStart).Milliseconds()
	report.CompletedAt = time.Now().UTC()

	if err := e.store.SaveCutoverReport(ctx, j.ID, report, "cutover_done"); err != nil {
		return nil, err
	}
	e.log(ctx, j.ID, "cutover", "info", "CUTOVER COMPLETE! Total write-freeze window: %dms | Sequences synced: %d", report.DowntimeMs, report.SequencesSynced)
	return report, nil
}

func (e *Engine) AbortMigration(ctx context.Context, j *store.Job, resetLab bool) error {
	e.StopMonitor(j.ID)
	e.log(ctx, j.ID, "abort", "warn", "Aborting migration and cleaning up replication objects...")

	subIdent := pgx.Identifier{j.SubscriptionName}.Sanitize()
	pubIdent := pgx.Identifier{j.PublicationName}.Sanitize()

	if tgtConn, err := connectTarget(ctx, j); err == nil {
		_, _ = tgtConn.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s DISABLE", subIdent))
		_, _ = tgtConn.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s SET (slot_name = NONE)", subIdent))
		_, _ = tgtConn.Exec(ctx, fmt.Sprintf("DROP SUBSCRIPTION IF EXISTS %s", subIdent))
		tgtConn.Close(ctx)
	}

	if srcConn, err := connectSource(ctx, j); err == nil {
		_, _ = srcConn.Exec(ctx, "SET default_transaction_read_only = off")
		_, _ = srcConn.Exec(ctx, "SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1", j.SubscriptionName)
		_, _ = srcConn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubIdent))
		_, _ = srcConn.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s SET default_transaction_read_only = off", pgx.Identifier{j.SourceDB}.Sanitize()))
		srcConn.Close(ctx)
	}

	if resetLab && e.cfg.PgBouncerControlURL != "" {
		_ = e.callPgBouncer(ctx, "/reset", nil)
	}

	_ = e.store.UpdateStatus(ctx, j.ID, "aborted")
	e.log(ctx, j.ID, "abort", "info", "Cleanup complete: replication slot released and job marked aborted.")
	return nil
}

func (e *Engine) callPgBouncer(ctx context.Context, path string, payload map[string]any) error {
	var body []byte
	if payload != nil {
		body, _ = json.Marshal(payload)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(e.cfg.PgBouncerControlURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("pgbouncer control %s returned HTTP %d", path, resp.StatusCode)
	}
	return nil
}

func (e *Engine) unfreezeSource(ctx context.Context, srcConn *pgx.Conn, j *store.Job) {
	unfreezeSQL := fmt.Sprintf("ALTER DATABASE %s SET default_transaction_read_only = off", pgx.Identifier{j.SourceDB}.Sanitize())

	tryExec := func(conn *pgx.Conn) error {
		if conn == nil || conn.IsClosed() {
			return fmt.Errorf("connection closed")
		}
		if _, err := conn.Exec(ctx, "SET default_transaction_read_only = off"); err != nil {
			return err
		}
		_, err := conn.Exec(ctx, unfreezeSQL)
		return err
	}

	if err := tryExec(srcConn); err == nil {
		e.log(ctx, j.ID, "cutover", "warn", "Rollback: Source database unfrozen (default_transaction_read_only = off).")
		return
	}

	freshConn, dialErr := connectSource(ctx, j)
	if dialErr != nil {
		e.log(ctx, j.ID, "cutover", "error", "Rollback: failed to reconnect to source to unfreeze read-only: %v", dialErr)
		return
	}
	defer freshConn.Close(ctx)

	if err := tryExec(freshConn); err != nil {
		e.log(ctx, j.ID, "cutover", "error", "Rollback: failed to unfreeze source database on fresh connection: %v", err)
		return
	}
	e.log(ctx, j.ID, "cutover", "warn", "Rollback: Source database unfrozen via fresh connection.")
}

func isInsufficientPrivilege(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42501"
}

// quiesceSourceSessions ensures no active/in-transaction non-migrator client backends remain on Source.
// It strictly excludes our own connection, walsender processes, and the migrator user (monitor/exporter).
func (e *Engine) quiesceSourceSessions(ctx context.Context, srcConn *pgx.Conn, pgbPaused bool) (string, string, error) {
	const countActiveSQL = `
		SELECT count(*)
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND backend_type = 'client backend'
		  AND usename IS DISTINCT FROM current_user
		  AND (state <> 'idle' OR xact_start IS NOT NULL)
	`
	const terminateActiveSQL = `
		SELECT count(*), count(*) FILTER (WHERE pg_terminate_backend(pid))
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND backend_type = 'client backend'
		  AND usename IS DISTINCT FROM current_user
		  AND (state <> 'idle' OR xact_start IS NOT NULL)
	`

	// Phase 1: Graceful drain of active/in-xact sessions (up to 2s, poll every 100ms).
	activeCount := 0
	drainDeadline := time.Now().Add(2 * time.Second)
	for {
		if err := srcConn.QueryRow(ctx, countActiveSQL).Scan(&activeCount); err != nil {
			return "fail", "", fmt.Errorf("check active source sessions: %w", err)
		}
		if activeCount == 0 || !time.Now().Before(drainDeadline) {
			break
		}
		select {
		case <-ctx.Done():
			return "fail", "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Phase 2: Terminate lingering active/in-xact writers and verify quiescence (up to 3s).
	terminatedActive := 0
	if activeCount > 0 {
		var matched, killed int
		if err := srcConn.QueryRow(ctx, terminateActiveSQL).Scan(&matched, &killed); err != nil {
			if isInsufficientPrivilege(err) {
				return "fail", "", fmt.Errorf("cannot guarantee write-freeze: %d active non-migrator session(s) remain on source and migrator lacks pg_signal_backend: %w", activeCount, err)
			}
			return "fail", "", fmt.Errorf("terminate active source sessions: %w", err)
		}
		terminatedActive = killed

		verifyDeadline := time.Now().Add(3 * time.Second)
		for {
			if err := srcConn.QueryRow(ctx, countActiveSQL).Scan(&activeCount); err != nil {
				return "fail", "", fmt.Errorf("verify active source sessions quiesced: %w", err)
			}
			if activeCount == 0 || !time.Now().Before(verifyDeadline) {
				break
			}
			select {
			case <-ctx.Done():
				return "fail", "", ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		if activeCount > 0 {
			return "fail", "", fmt.Errorf("cannot guarantee write-freeze: %d active non-migrator session(s) remain on source after termination attempt", activeCount)
		}
	}

	// Phase 3: Idle session handling.
	// If PgBouncer is paused, keep idle server sockets intact for zero-drop rollback on /resume.
	if pgbPaused {
		detail := "Source read-only set; 0 active non-migrator writers (PgBouncer idle server sockets preserved)"
		if terminatedActive > 0 {
			detail = fmt.Sprintf("Source read-only set; terminated %d lingering writer(s), 0 active (PgBouncer idle sockets preserved)", terminatedActive)
		}
		return "ok", detail, nil
	}

	// Direct mode (no PgBouncer): best-effort recycle idle non-migrator sessions so pools reconnect read-only.
	const countIdleSQL = `
		SELECT count(*)
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND backend_type = 'client backend'
		  AND usename IS DISTINCT FROM current_user
		  AND state = 'idle'
		  AND xact_start IS NULL
	`
	const terminateIdleSQL = `
		SELECT count(*), count(*) FILTER (WHERE pg_terminate_backend(pid))
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND backend_type = 'client backend'
		  AND usename IS DISTINCT FROM current_user
		  AND state = 'idle'
		  AND xact_start IS NULL
	`
	var idleMatched, idleKilled int
	if err := srcConn.QueryRow(ctx, terminateIdleSQL).Scan(&idleMatched, &idleKilled); err != nil {
		if isInsufficientPrivilege(err) {
			var idleCnt int
			_ = srcConn.QueryRow(ctx, countIdleSQL).Scan(&idleCnt)
			return "warn", fmt.Sprintf("Source read-only set (0 active writers; %d idle sessions not recycled: grant pg_signal_backend)", idleCnt), nil
		}
		return "warn", fmt.Sprintf("Source read-only set (0 active writers; idle session recycle warning: %v)", err), nil
	}

	return "ok", fmt.Sprintf("Source read-only set; 0 active writers (terminated %d active, recycled %d idle sessions)", terminatedActive, idleKilled), nil
}
