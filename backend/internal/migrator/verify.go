package migrator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"pg-migrator/internal/store"
)

func (e *Engine) VerifyData(ctx context.Context, j *store.Job) (*VerifyReport, error) {
	e.log(ctx, j.ID, "verify", "info", "Starting data & sequence verification across source and target...")

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

	type tableRef struct {
		schema string
		table  string
		pkCols []string
	}

	rows, err := srcConn.Query(ctx, `
		SELECT
			n.nspname,
			c.relname,
			COALESCE(
				(SELECT string_agg(a.attname, ',' ORDER BY u.ord)
				 FROM pg_index i
				 JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS u(attnum, ord) ON true
				 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = u.attnum
				 WHERE i.indrelid = c.oid AND i.indisprimary),
				''
			) AS pk_cols
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, c.relname
	`)
	if err != nil {
		return nil, err
	}
	var tables []tableRef
	for rows.Next() {
		var s, t, pk string
		if rows.Scan(&s, &t, &pk) == nil {
			var cols []string
			if pk != "" {
				cols = strings.Split(pk, ",")
			}
			tables = append(tables, tableRef{schema: s, table: t, pkCols: cols})
		}
	}
	rows.Close()

	report := &VerifyReport{
		VerifiedAt: time.Now().UTC(),
		AllMatched: true,
		Tables:     make([]VerifyTableResult, len(tables)),
		Sequences:  make([]VerifySequenceResult, 0),
	}

	// Parallel table verification (max 4 concurrent workers)
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for idx, tbl := range tables {
		wg.Add(1)
		go func(i int, ref tableRef) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			t0 := time.Now()
			res := VerifyTableResult{
				Schema: ref.schema,
				Table:  ref.table,
			}

			sConn, err1 := connectSource(ctx, j)
			tConn, err2 := connectTarget(ctx, j)
			if err1 != nil || err2 != nil {
				if sConn != nil {
					sConn.Close(ctx)
				}
				if tConn != nil {
					tConn.Close(ctx)
				}
				mu.Lock()
				report.AllMatched = false
				report.Tables[i] = res
				mu.Unlock()
				return
			}
			defer sConn.Close(ctx)
			defer tConn.Close(ctx)

			fq := pgx.Identifier{ref.schema, ref.table}.Sanitize()
			countSQL := fmt.Sprintf("SELECT count(*) FROM %s", fq)

			var cWg sync.WaitGroup
			cWg.Add(2)
			go func() {
				defer cWg.Done()
				_ = sConn.QueryRow(ctx, countSQL).Scan(&res.SourceRows)
			}()
			go func() {
				defer cWg.Done()
				_ = tConn.QueryRow(ctx, countSQL).Scan(&res.TargetRows)
			}()
			cWg.Wait()

			res.RowDiff = res.TargetRows - res.SourceRows

			// Deterministic sample checksum on PK-ordered first 250 rows
			if len(ref.pkCols) > 0 {
				var orderParts []string
				for _, col := range ref.pkCols {
					orderParts = append(orderParts, pgx.Identifier{col}.Sanitize())
				}
				orderClause := strings.Join(orderParts, ", ")
				hashSQL := fmt.Sprintf(
					"SELECT md5(COALESCE(string_agg(sub.r, '|' ORDER BY sub.ord), '')) FROM (SELECT row_number() OVER (ORDER BY %s) AS ord, t::text AS r FROM %s t ORDER BY %s LIMIT 250) sub",
					orderClause, fq, orderClause,
				)
				var sHash, tHash string
				_ = sConn.QueryRow(ctx, hashSQL).Scan(&sHash)
				_ = tConn.QueryRow(ctx, hashSQL).Scan(&tHash)
				res.SampleMatch = sHash == tHash
			} else {
				res.SampleMatch = res.RowDiff == 0
			}

			// Allow small transient drift on live-writing tables if within tolerance?
			// Keep strict reporting so user sees exact live delta before cutover.
			res.Match = (res.RowDiff == 0) && res.SampleMatch
			res.DurationMs = time.Since(t0).Milliseconds()

			mu.Lock()
			if !res.Match {
				report.AllMatched = false
			}
			report.Tables[i] = res
			mu.Unlock()
		}(idx, tbl)
	}
	wg.Wait()

	// Compare sequences
	srcSeqs := fetchSequences(ctx, srcConn)
	tgtSeqs := fetchSequences(ctx, tgtConn)
	for key, sv := range srcSeqs {
		parts := strings.SplitN(key, ".", 2)
		tv := tgtSeqs[key]
		report.Sequences = append(report.Sequences, VerifySequenceResult{
			Schema:      parts[0],
			Sequence:    parts[1],
			SourceValue: sv.val,
			TargetValue: tv.val,
			Drift:       sv.val - tv.val,
		})
	}

	if err := e.store.SaveVerifyReport(ctx, j.ID, report); err != nil {
		return nil, err
	}

	e.log(ctx, j.ID, "verify", "info", "Verification complete: %d tables checked (all_matched=%v), %d sequences inspected.",
		len(report.Tables), report.AllMatched, len(report.Sequences))
	return report, nil
}

type seqInfo struct {
	val      int64
	isCalled bool
}

func fetchSequences(ctx context.Context, conn *pgx.Conn) map[string]seqInfo {
	out := make(map[string]seqInfo)
	rows, err := conn.Query(ctx, `
		SELECT schemaname, sequencename, COALESCE(last_value, start_value)::bigint, (last_value IS NOT NULL)
		FROM pg_sequences
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY schemaname, sequencename
	`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var s, name string
		var val int64
		var isCalled bool
		if rows.Scan(&s, &name, &val, &isCalled) == nil {
			out[s+"."+name] = seqInfo{val: val, isCalled: isCalled}
		}
	}
	return out
}
