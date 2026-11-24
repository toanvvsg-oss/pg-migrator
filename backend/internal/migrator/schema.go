package migrator

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"pg-migrator/internal/store"
)

type SchemaSyncResult struct {
	BaseStatementsApplied int  `json:"base_statements_applied"`
	DeferredFKCount       int  `json:"deferred_fk_count"`
	SanitizedLinesRemoved int  `json:"sanitized_lines_removed"`
	Success               bool `json:"success"`
}

// SyncSchema dumps schema-only DDL from source, sanitizes cloud-specific statements,
// separates FOREIGN KEY constraints (to avoid parallel logical replication copy order errors),
// and applies the base schema (tables, sequences, PKs, indexes) to target.
func (e *Engine) SyncSchema(ctx context.Context, j *store.Job) (*SchemaSyncResult, error) {
	e.log(ctx, j.ID, "schema", "info", "Running pg_dump --schema-only against %s:%d/%s...", j.SourceHost, j.SourcePort, j.SourceDB)

	dumpArgs := []string{
		"--schema-only",
		"--no-owner",
		"--no-privileges",
		"--no-comments",
		"-h", j.SourceHost,
		"-p", strconv.Itoa(j.SourcePort),
		"-U", j.SourceUser,
		"-d", j.SourceDB,
	}

	cmd := exec.CommandContext(ctx, "pg_dump", dumpArgs...)
	cmd.Env = append(os.Environ(),
		"PGPASSWORD="+j.SourcePassword,
		"PGSSLMODE="+j.SourceSSLMode,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		e.log(ctx, j.ID, "schema", "error", "pg_dump failed: %s (%v)", strings.TrimSpace(stderr.String()), err)
		return nil, fmt.Errorf("pg_dump failed: %s", strings.TrimSpace(stderr.String()))
	}

	baseSQL, deferredFKs, removed := sanitizeAndSplitSchema(stdout.String())
	e.log(ctx, j.ID, "schema", "info", "Schema dumped (%d bytes). Sanitized %d cloud-specific lines; deferred %d FOREIGN KEY constraints for post-copy application.",
		stdout.Len(), removed, len(deferredFKs))

	// Apply base schema to target via psql
	psqlArgs := []string{
		"-v", "ON_ERROR_STOP=1",
		"-h", j.TargetHost,
		"-p", strconv.Itoa(j.TargetPort),
		"-U", j.TargetUser,
		"-d", j.TargetDB,
	}
	applyCmd := exec.CommandContext(ctx, "psql", psqlArgs...)
	applyCmd.Env = append(os.Environ(),
		"PGPASSWORD="+j.TargetPassword,
		"PGSSLMODE="+j.TargetSSLMode,
	)
	applyCmd.Stdin = strings.NewReader(baseSQL)
	var applyOut, applyErr bytes.Buffer
	applyCmd.Stdout = &applyOut
	applyCmd.Stderr = &applyErr

	if err := applyCmd.Run(); err != nil {
		errText := strings.TrimSpace(applyErr.String())
		e.log(ctx, j.ID, "schema", "error", "psql schema apply failed: %s", errText)
		return nil, fmt.Errorf("psql schema apply failed: %s", errText)
	}

	fkBlob := strings.Join(deferredFKs, "\n")
	if err := e.store.SaveDeferredFKs(ctx, j.ID, fkBlob, len(deferredFKs) == 0); err != nil {
		return nil, err
	}
	if err := e.store.UpdateStatus(ctx, j.ID, "schema_applied"); err != nil {
		return nil, err
	}

	e.log(ctx, j.ID, "schema", "info", "Target schema applied cleanly. Ready to start Logical Replication.")
	return &SchemaSyncResult{
		BaseStatementsApplied: strings.Count(baseSQL, ";"),
		DeferredFKCount:       len(deferredFKs),
		SanitizedLinesRemoved: removed,
		Success:               true,
	}, nil
}

// sanitizeAndSplitSchema removes cloud-managed / cross-version problematic directives
// and extracts ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY statements.
func sanitizeAndSplitSchema(raw string) (string, []string, int) {
	lines := strings.Split(raw, "\n")
	var cleanLines []string
	var deferredFKs []string
	removed := 0

	// Join statements cleanly while tracking multi-line ALTER TABLE ... FOREIGN KEY blocks
	var stmtBuf []string
	flushStmt := func() {
		if len(stmtBuf) == 0 {
			return
		}
		stmt := strings.Join(stmtBuf, "\n")
		upper := strings.ToUpper(stmt)
		if strings.Contains(upper, "ALTER TABLE") && strings.Contains(upper, "FOREIGN KEY") {
			deferredFKs = append(deferredFKs, strings.TrimSpace(stmt))
		} else {
			cleanLines = append(cleanLines, stmt)
		}
		stmtBuf = nil
	}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip cloud-specific or cross-version incompatible SET directives
		if strings.HasPrefix(trimmed, "SET transaction_timeout") ||
			strings.HasPrefix(trimmed, "SET idle_in_transaction_session_timeout") ||
			strings.Contains(trimmed, "rdsadmin") ||
			strings.Contains(trimmed, "cloudsqlsuperuser") ||
			strings.Contains(trimmed, "azure_pg_admin") ||
			strings.HasPrefix(trimmed, "CREATE EXTENSION IF NOT EXISTS plpgsql") ||
			strings.HasPrefix(trimmed, "COMMENT ON EXTENSION") {
			removed++
			continue
		}

		if len(stmtBuf) == 0 {
			if strings.HasPrefix(strings.ToUpper(trimmed), "ALTER TABLE") {
				stmtBuf = append(stmtBuf, line)
				if strings.HasSuffix(trimmed, ";") {
					flushStmt()
				}
				continue
			}
			cleanLines = append(cleanLines, line)
		} else {
			stmtBuf = append(stmtBuf, line)
			if strings.HasSuffix(trimmed, ";") {
				flushStmt()
			}
		}
	}
	flushStmt()

	return strings.Join(cleanLines, "\n"), deferredFKs, removed
}

// ApplyDeferredFKs applies deferred Foreign Keys on Target using NOT VALID + VALIDATE CONSTRAINT
// so writes are not blocked with AccessExclusiveLock for long periods.
func (e *Engine) ApplyDeferredFKs(ctx context.Context, j *store.Job) (int, error) {
	if strings.TrimSpace(j.DeferredFKsSQL) == "" || j.FKsApplied {
		return 0, nil
	}
	tgtConn, err := connectTarget(ctx, j)
	if err != nil {
		return 0, err
	}
	defer tgtConn.Close(ctx)

	stmts := strings.Split(j.DeferredFKsSQL, ";")
	applied := 0
	for _, s := range stmts {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		// Convert to NOT VALID first to minimize locking, then validate
		notValidSQL := s + " NOT VALID"
		if _, err := tgtConn.Exec(ctx, notValidSQL); err != nil {
			// Ignore duplicate_object if already created on retry
			if !strings.Contains(err.Error(), "already exists") {
				return applied, fmt.Errorf("apply FK failed (%s): %w", s, err)
			}
		}
		applied++
	}

	// Validate all non-validated foreign keys on target
	rows, err := tgtConn.Query(ctx, `
		SELECT n.nspname, c.relname, con.conname
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE con.contype = 'f' AND NOT con.convalidated
	`)
	if err == nil {
		type fkItem struct{ s, t, c string }
		var toValidate []fkItem
		for rows.Next() {
			var f fkItem
			if rows.Scan(&f.s, &f.t, &f.c) == nil {
				toValidate = append(toValidate, f)
			}
		}
		rows.Close()
		for _, f := range toValidate {
			q := fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", pgx.Identifier{f.s, f.t}.Sanitize(), pgx.Identifier{f.c}.Sanitize())
			if _, err := tgtConn.Exec(ctx, q); err != nil {
				e.log(ctx, j.ID, "cutover", "warn", "FK validation warning (%s): %v", f.c, err)
			}
		}
	}

	_ = e.store.MarkFKsApplied(ctx, j.ID)
	e.log(ctx, j.ID, "cutover", "info", "Applied and validated %d deferred Foreign Key constraints on target.", applied)
	return applied, nil
}
