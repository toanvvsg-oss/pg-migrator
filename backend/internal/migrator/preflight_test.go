package migrator

import (
	"strings"
	"testing"
)

func TestEvaluatePartitionedTables(t *testing.T) {
	chk, summary := evaluatePartitionedTables(nil, "pub_test")
	if chk.Status != "pass" || len(summary) != 0 {
		t.Fatalf("empty partitioned tables: got (%s, %v), want pass/empty", chk.Status, summary)
	}

	chk, summary = evaluatePartitionedTables([]partitionedTableInfo{
		{Schema: "public", Table: "events", PartCount: 12},
		{Schema: "public", Table: "empty_parent", PartCount: 0},
	}, "pub_test")
	if chk.Status != "warn" {
		t.Fatalf("status = %q, want warn", chk.Status)
	}
	if len(summary) != 2 {
		t.Fatalf("summary len = %d, want 2", len(summary))
	}
	if !strings.Contains(chk.Detail, "0 leaf partitions") {
		t.Errorf("expected detail to note empty root partitions, got: %s", chk.Detail)
	}
	if !strings.Contains(chk.FixSQL, "publish_via_partition_root = true") {
		t.Errorf("expected FixSQL to mention publish_via_partition_root, got: %s", chk.FixSQL)
	}
}

func TestEvaluateUnloggedTables(t *testing.T) {
	chk, summary := evaluateUnloggedTables(nil)
	if chk.Status != "pass" || len(summary) != 0 {
		t.Fatalf("empty unlogged tables: got (%s, %v), want pass/empty", chk.Status, summary)
	}

	chk, summary = evaluateUnloggedTables([]unloggedTableInfo{
		{Schema: "public", Table: "cache_items", RelKind: "r"},
		{Schema: "public", Table: "unlogged_part", RelKind: "p"},
	})
	if chk.Status != "fail" {
		t.Fatalf("status = %q, want fail", chk.Status)
	}
	if len(summary) != 2 {
		t.Fatalf("summary len = %d, want 2", len(summary))
	}
	if !strings.Contains(chk.FixSQL, `ALTER TABLE "public"."cache_items" SET LOGGED;`) {
		t.Errorf("missing SET LOGGED for relkind='r' in FixSQL: %s", chk.FixSQL)
	}
	if !strings.Contains(chk.FixSQL, "cannot use ALTER TABLE SET LOGGED directly") {
		t.Errorf("missing relkind='p' guidance in FixSQL: %s", chk.FixSQL)
	}
}

func TestEvaluateUserTriggers(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		chk, summary := evaluateUserTriggers(nil)
		if chk.Status != "pass" || len(summary) != 0 {
			t.Fatalf("got (%s, %v), want pass/empty", chk.Status, summary)
		}
	})

	t.Run("origin_and_disabled_warn", func(t *testing.T) {
		chk, summary := evaluateUserTriggers([]userTriggerInfo{
			{Schema: "public", Table: "orders", Trigger: "trg_audit", Enabled: "O"},
			{Schema: "public", Table: "orders", Trigger: "trg_old", Enabled: "D"},
		})
		if chk.Status != "warn" {
			t.Fatalf("status = %q, want warn", chk.Status)
		}
		if len(summary) != 2 {
			t.Fatalf("summary len = %d, want 2", len(summary))
		}
	})

	t.Run("replica_and_always_fail", func(t *testing.T) {
		chk, summary := evaluateUserTriggers([]userTriggerInfo{
			{Schema: "public", Table: "orders", Trigger: "trg_origin", Enabled: "O"},
			{Schema: "public", Table: "orders", Trigger: "trg_replica", Enabled: "R"},
			{Schema: "public", Table: "orders", Trigger: "trg_always", Enabled: "A"},
		})
		if chk.Status != "fail" {
			t.Fatalf("status = %q, want fail", chk.Status)
		}
		if len(summary) != 3 {
			t.Fatalf("summary len = %d, want 3", len(summary))
		}
		if !strings.Contains(chk.FixSQL, `ENABLE TRIGGER "trg_replica"`) || !strings.Contains(chk.FixSQL, `ENABLE TRIGGER "trg_always"`) {
			t.Errorf("expected FixSQL for both replica and always triggers, got: %s", chk.FixSQL)
		}
	})
}
