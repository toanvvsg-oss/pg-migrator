package migrator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"pg-migrator/internal/store"
)

func (e *Engine) RunPreflight(ctx context.Context, j *store.Job) (*PreflightReport, error) {
	e.log(ctx, j.ID, "preflight", "info", "Starting pre-flight validation checks...")

	report := &PreflightReport{
		CheckedAt:         time.Now().UTC(),
		Passed:            true,
		TablesWithoutPK:   make([]string, 0),
		PartitionedTables: make([]string, 0),
		UnloggedTables:    make([]string, 0),
		UserTriggers:      make([]string, 0),
		Checks:            make([]CheckItem, 0),
	}

	fail := func(name, detail, fix string) {
		report.Passed = false
		report.Checks = append(report.Checks, CheckItem{Name: name, Status: "fail", Detail: detail, FixSQL: fix})
	}
	warn := func(name, detail, fix string) {
		report.Checks = append(report.Checks, CheckItem{Name: name, Status: "warn", Detail: detail, FixSQL: fix})
	}
	pass := func(name, detail string) {
		report.Checks = append(report.Checks, CheckItem{Name: name, Status: "pass", Detail: detail})
	}

	// 1. Source Connectivity
	t0 := time.Now()
	srcConn, err := connectSource(ctx, j)
	if err != nil {
		fail("Source Connectivity", fmt.Sprintf("Failed to connect to %s:%d/%s: %v", j.SourceHost, j.SourcePort, j.SourceDB, err), "")
		_ = e.store.SavePreflightReport(ctx, j.ID, report, "failed")
		e.log(ctx, j.ID, "preflight", "error", "Source connection failed: %v", err)
		return report, nil
	}
	defer srcConn.Close(ctx)
	srcLatency := time.Since(t0).Milliseconds()

	var srcVer string
	var srcSize int64
	_ = srcConn.QueryRow(ctx, "SHOW server_version").Scan(&srcVer)
	_ = srcConn.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&srcSize)
	report.SourceVersion = srcVer
	report.SourceSizeBytes = srcSize
	pass("Source Connectivity", fmt.Sprintf("Connected to PG %s (%dms, DB size: %.2f GiB)", srcVer, srcLatency, float64(srcSize)/(1024*1024*1024)))

	// 2. Target Connectivity
	t1 := time.Now()
	tgtConn, err := connectTarget(ctx, j)
	if err != nil {
		fail("Target Connectivity", fmt.Sprintf("Failed to connect to %s:%d/%s: %v", j.TargetHost, j.TargetPort, j.TargetDB, err), "")
		_ = e.store.SavePreflightReport(ctx, j.ID, report, "failed")
		e.log(ctx, j.ID, "preflight", "error", "Target connection failed: %v", err)
		return report, nil
	}
	defer tgtConn.Close(ctx)
	tgtLatency := time.Since(t1).Milliseconds()

	var tgtVer string
	_ = tgtConn.QueryRow(ctx, "SHOW server_version").Scan(&tgtVer)
	report.TargetVersion = tgtVer
	pass("Target Connectivity", fmt.Sprintf("Connected to PG %s (%dms)", tgtVer, tgtLatency))

	// 3. wal_level = logical
	var walLevel string
	_ = srcConn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel)
	if walLevel == "logical" {
		pass("Source wal_level", "wal_level = logical")
	} else {
		fail("Source wal_level", fmt.Sprintf("wal_level is %q, must be 'logical'", walLevel), "ALTER SYSTEM SET wal_level = 'logical'; -- requires PostgreSQL restart")
	}

	// 4. Replication Slots & WAL Senders headroom
	var maxSlots, usedSlots, maxSenders, usedSenders int
	_ = srcConn.QueryRow(ctx, "SELECT current_setting('max_replication_slots')::int").Scan(&maxSlots)
	_ = srcConn.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots").Scan(&usedSlots)
	_ = srcConn.QueryRow(ctx, "SELECT current_setting('max_wal_senders')::int").Scan(&maxSenders)
	_ = srcConn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_replication").Scan(&usedSenders)

	if maxSlots-usedSlots >= 1 && maxSenders-usedSenders >= 2 {
		pass("Source Replication Capacity", fmt.Sprintf("Slots: %d/%d used, WAL Senders: %d/%d used", usedSlots, maxSlots, usedSenders, maxSenders))
	} else {
		fail("Source Replication Capacity", fmt.Sprintf("Insufficient headroom (Slots %d/%d, WAL Senders %d/%d)", usedSlots, maxSlots, usedSenders, maxSenders), "Increase max_replication_slots and max_wal_senders on source")
	}

	// 5. Target Logical Replication Workers
	var maxLogWorkers, maxWorkers int
	_ = tgtConn.QueryRow(ctx, "SELECT current_setting('max_logical_replication_workers')::int").Scan(&maxLogWorkers)
	_ = tgtConn.QueryRow(ctx, "SELECT current_setting('max_worker_processes')::int").Scan(&maxWorkers)
	if maxLogWorkers >= 4 {
		pass("Target Logical Workers", fmt.Sprintf("max_logical_replication_workers = %d (max_worker_processes = %d)", maxLogWorkers, maxWorkers))
	} else {
		warn("Target Logical Workers", fmt.Sprintf("Only %d logical workers; initial sync of large DB may be slower (recommended >= 4)", maxLogWorkers), "ALTER SYSTEM SET max_logical_replication_workers = 8;")
	}

	// 6. User Tables & Primary Key / REPLICA IDENTITY check
	rows, err := srcConn.Query(ctx, `
		SELECT n.nspname, c.relname, c.relreplident::text,
		       EXISTS (
		           SELECT 1 FROM pg_index i
		           WHERE i.indrelid = c.oid AND i.indisprimary
		       ) AS has_pk
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, c.relname
	`)
	if err != nil {
		fail("Table Discovery", fmt.Sprintf("Failed to inspect tables: %v", err), "")
	} else {
		defer rows.Close()
		var fixStmts []string
		for rows.Next() {
			var sch, tbl, replIdent string
			var hasPK bool
			if err := rows.Scan(&sch, &tbl, &replIdent, &hasPK); err == nil {
				report.TableCount++
				if !hasPK && replIdent != "f" {
					fq := fmt.Sprintf("%s.%s", sch, tbl)
					report.TablesWithoutPK = append(report.TablesWithoutPK, fq)
					fixStmts = append(fixStmts, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", pgx.Identifier{sch, tbl}.Sanitize()))
				}
			}
		}
		if report.TableCount == 0 {
			warn("Table Discovery", "No user tables found on source yet (run TPC-H Seeder first if testing)", "")
		} else if len(report.TablesWithoutPK) > 0 {
			fail(
				"Primary Keys & Replica Identity",
				fmt.Sprintf("%d of %d tables lack a Primary Key and REPLICA IDENTITY FULL: %s", len(report.TablesWithoutPK), report.TableCount, strings.Join(report.TablesWithoutPK, ", ")),
				strings.Join(fixStmts, "\n"),
			)
		} else {
			pass("Primary Keys & Replica Identity", fmt.Sprintf("All %d user tables have Primary Keys or REPLICA IDENTITY FULL", report.TableCount))
		}
	}

	// 7. Partitioned Tables (relkind = 'p')
	partRows, err := srcConn.Query(ctx, `
		SELECT n.nspname, c.relname,
		       (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid)::int AS part_count
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'p'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, c.relname
	`)
	if err != nil {
		warn("Partitioned Tables", fmt.Sprintf("Failed to inspect partitioned tables: %v", err), "")
	} else {
		var parts []partitionedTableInfo
		for partRows.Next() {
			var p partitionedTableInfo
			if partRows.Scan(&p.Schema, &p.Table, &p.PartCount) == nil {
				parts = append(parts, p)
			}
		}
		partRows.Close()
		chk, summary := evaluatePartitionedTables(parts, j.PublicationName)
		report.PartitionedTables = summary
		report.Checks = append(report.Checks, chk)
		if chk.Status == "fail" {
			report.Passed = false
		}
	}

	// 8. Unlogged Tables (relpersistence = 'u')
	unloggedRows, err := srcConn.Query(ctx, `
		SELECT n.nspname, c.relname, c.relkind::text
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relpersistence = 'u'
		  AND c.relkind IN ('r', 'p')
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, c.relname
	`)
	if err != nil {
		warn("Unlogged Tables", fmt.Sprintf("Failed to inspect unlogged tables: %v", err), "")
	} else {
		var unlogged []unloggedTableInfo
		for unloggedRows.Next() {
			var u unloggedTableInfo
			if unloggedRows.Scan(&u.Schema, &u.Table, &u.RelKind) == nil {
				unlogged = append(unlogged, u)
			}
		}
		unloggedRows.Close()
		chk, summary := evaluateUnloggedTables(unlogged)
		report.UnloggedTables = summary
		report.Checks = append(report.Checks, chk)
		if chk.Status == "fail" {
			report.Passed = false
		}
	}

	// 9. User Triggers (NOT tgisinternal)
	trigRows, err := srcConn.Query(ctx, `
		SELECT n.nspname, c.relname, t.tgname, t.tgenabled::text
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT t.tgisinternal
		  AND c.relkind IN ('r', 'p')
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, c.relname, t.tgname
	`)
	if err != nil {
		warn("User Triggers", fmt.Sprintf("Failed to inspect user triggers: %v", err), "")
	} else {
		var trigs []userTriggerInfo
		for trigRows.Next() {
			var tr userTriggerInfo
			if trigRows.Scan(&tr.Schema, &tr.Table, &tr.Trigger, &tr.Enabled) == nil {
				trigs = append(trigs, tr)
			}
		}
		trigRows.Close()
		chk, summary := evaluateUserTriggers(trigs)
		report.UserTriggers = summary
		report.Checks = append(report.Checks, chk)
		if chk.Status == "fail" {
			report.Passed = false
		}
	}

	// 10. Sequences Inventory
	var seqCount int
	_ = srcConn.QueryRow(ctx, `
		SELECT count(*) FROM pg_sequences
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
	`).Scan(&seqCount)
	report.SequenceCount = seqCount
	if seqCount > 0 {
		pass("Sequences Inventory", fmt.Sprintf("Detected %d sequences — will be automatically synchronized during cutover", seqCount))
	} else {
		pass("Sequences Inventory", "No user sequences detected")
	}

	// 11. Extensions Compatibility
	srcExts := fetchExtensions(ctx, srcConn)
	tgtExts := fetchExtensions(ctx, tgtConn)
	var missingExts []string
	for ext := range srcExts {
		if ext == "plpgsql" {
			continue
		}
		if !tgtExts[ext] {
			missingExts = append(missingExts, ext)
		}
	}
	if len(missingExts) > 0 {
		warn("Extensions Parity", fmt.Sprintf("Extensions on source missing on target: %s", strings.Join(missingExts, ", ")), fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;", missingExts[0]))
	} else {
		pass("Extensions Parity", "All non-default extensions match or are compatible")
	}

	// 12. Existing Subscription / Publication Check
	var pubExists, subExists bool
	_ = srcConn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", j.PublicationName).Scan(&pubExists)
	_ = tgtConn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1)", j.SubscriptionName).Scan(&subExists)
	if pubExists || subExists {
		warn("Replication Name Collision", fmt.Sprintf("Existing objects found (publication=%v, subscription=%v). Abort/cleanup before restarting.", pubExists, subExists), "")
	} else {
		pass("Replication Name Collision", fmt.Sprintf("Publication %q and Subscription %q are available", j.PublicationName, j.SubscriptionName))
	}

	nextStatus := "preflight_ok"
	if !report.Passed {
		nextStatus = "created"
	}
	if err := e.store.SavePreflightReport(ctx, j.ID, report, nextStatus); err != nil {
		return nil, err
	}

	level := "info"
	if !report.Passed {
		level = "warn"
	}
	e.log(ctx, j.ID, "preflight", level, "Pre-flight completed: passed=%v, tables=%d, sequences=%d, size=%.2f GiB",
		report.Passed, report.TableCount, report.SequenceCount, float64(report.SourceSizeBytes)/(1024*1024*1024))

	return report, nil
}

func (e *Engine) FixReplicaIdentity(ctx context.Context, j *store.Job) (int, error) {
	srcConn, err := connectSource(ctx, j)
	if err != nil {
		return 0, err
	}
	defer srcConn.Close(ctx)

	rows, err := srcConn.Query(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		  AND c.relreplident <> 'f'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		  AND NOT EXISTS (
		      SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary
		  )
	`)
	if err != nil {
		return 0, err
	}
	type tbl struct{ s, t string }
	var targets []tbl
	for rows.Next() {
		var item tbl
		if err := rows.Scan(&item.s, &item.t); err == nil {
			targets = append(targets, item)
		}
	}
	rows.Close()

	fixed := 0
	for _, item := range targets {
		sql := fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", pgx.Identifier{item.s, item.t}.Sanitize())
		if _, err := srcConn.Exec(ctx, sql); err != nil {
			return fixed, fmt.Errorf("failed on %s.%s: %w", item.s, item.t, err)
		}
		e.log(ctx, j.ID, "preflight", "info", "Applied: %s", sql)
		fixed++
	}
	return fixed, nil
}

func fetchExtensions(ctx context.Context, conn *pgx.Conn) map[string]bool {
	out := make(map[string]bool)
	rows, err := conn.Query(ctx, "SELECT extname FROM pg_extension")
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			out[name] = true
		}
	}
	return out
}

type partitionedTableInfo struct {
	Schema    string
	Table     string
	PartCount int
}

type unloggedTableInfo struct {
	Schema  string
	Table   string
	RelKind string // 'r' or 'p'
}

type userTriggerInfo struct {
	Schema  string
	Table   string
	Trigger string
	Enabled string // 'O' (origin), 'D' (disabled), 'R' (replica), 'A' (always)
}

func evaluatePartitionedTables(parts []partitionedTableInfo, pubName string) (CheckItem, []string) {
	summary := make([]string, 0, len(parts))
	if len(parts) == 0 {
		return CheckItem{
			Name:   "Partitioned Tables",
			Status: "pass",
			Detail: "No partitioned user tables (relkind='p') detected",
		}, summary
	}

	emptyRoots := 0
	for _, p := range parts {
		summary = append(summary, fmt.Sprintf("%s.%s (%d partitions)", p.Schema, p.Table, p.PartCount))
		if p.PartCount == 0 {
			emptyRoots++
		}
	}

	detail := fmt.Sprintf("Found %d partitioned table(s): %s. StartReplication publishes existing leaf partitions (relkind='r') without publish_via_partition_root = true; newly attached partitions during CDC will not auto-replicate.",
		len(parts), strings.Join(summary, ", "))
	if emptyRoots > 0 {
		detail += fmt.Sprintf(" (%d root table(s) currently have 0 leaf partitions.)", emptyRoots)
	}

	if pubName == "" {
		pubName = "pub_pg_migrator"
	}
	fixSQL := fmt.Sprintf("ALTER PUBLICATION %s SET (publish_via_partition_root = true); -- optional if partition root routing is required; freeze partition DDL during migration",
		pgx.Identifier{pubName}.Sanitize())

	return CheckItem{
		Name:   "Partitioned Tables",
		Status: "warn",
		Detail: detail,
		FixSQL: fixSQL,
	}, summary
}

func evaluateUnloggedTables(unlogged []unloggedTableInfo) (CheckItem, []string) {
	summary := make([]string, 0, len(unlogged))
	if len(unlogged) == 0 {
		return CheckItem{
			Name:   "Unlogged Tables",
			Status: "pass",
			Detail: "No unlogged user tables detected",
		}, summary
	}

	fixStmts := make([]string, 0, len(unlogged))
	for _, u := range unlogged {
		fq := fmt.Sprintf("%s.%s", u.Schema, u.Table)
		summary = append(summary, fq)
		ident := pgx.Identifier{u.Schema, u.Table}.Sanitize()
		if u.RelKind == "p" {
			fixStmts = append(fixStmts, fmt.Sprintf("-- Partitioned table %s cannot use ALTER TABLE SET LOGGED directly; alter leaf partitions or recreate as LOGGED", ident))
		} else {
			fixStmts = append(fixStmts, fmt.Sprintf("ALTER TABLE %s SET LOGGED;", ident))
		}
	}

	return CheckItem{
		Name:   "Unlogged Tables",
		Status: "fail",
		Detail: fmt.Sprintf("Found %d unlogged table(s): %s. PostgreSQL logical replication does not write WAL for unlogged tables and CREATE PUBLICATION fails on them.", len(unlogged), strings.Join(summary, ", ")),
		FixSQL: strings.Join(fixStmts, "\n"),
	}, summary
}

func evaluateUserTriggers(trigs []userTriggerInfo) (CheckItem, []string) {
	summary := make([]string, 0, len(trigs))
	if len(trigs) == 0 {
		return CheckItem{
			Name:   "User Triggers",
			Status: "pass",
			Detail: "No user-defined triggers detected",
		}, summary
	}

	var dangerous []string
	var fixStmts []string
	for _, tr := range trigs {
		mode := triggerModeLabel(tr.Enabled)
		label := fmt.Sprintf("%s.%s.%s (%s)", tr.Schema, tr.Table, tr.Trigger, mode)
		summary = append(summary, label)
		if tr.Enabled == "R" || tr.Enabled == "A" {
			dangerous = append(dangerous, label)
			fixStmts = append(fixStmts, fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER %s; -- convert REPLICA/ALWAYS trigger to ORIGIN mode before Schema Sync",
				pgx.Identifier{tr.Schema, tr.Table}.Sanitize(), pgx.Identifier{tr.Trigger}.Sanitize()))
		}
	}

	if len(dangerous) > 0 {
		return CheckItem{
			Name:   "User Triggers",
			Status: "fail",
			Detail: fmt.Sprintf("Found %d REPLICA/ALWAYS user trigger(s) that will double-fire on Target during logical replication: %s", len(dangerous), strings.Join(dangerous, ", ")),
			FixSQL: strings.Join(fixStmts, "\n"),
		}, summary
	}

	return CheckItem{
		Name:   "User Triggers",
		Status: "warn",
		Detail: fmt.Sprintf("Found %d user trigger(s): %s. Logical Replication skips ORIGIN triggers on Target during CDC (session_replication_role='replica') and activates them once Target receives direct writes at cutover; verify side effects (NOTIFY, sequences, audit tables).", len(trigs), strings.Join(summary, ", ")),
	}, summary
}

func triggerModeLabel(code string) string {
	switch code {
	case "O":
		return "origin"
	case "D":
		return "disabled"
	case "R":
		return "replica"
	case "A":
		return "always"
	default:
		return code
	}
}
