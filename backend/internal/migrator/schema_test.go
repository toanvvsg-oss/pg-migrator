package migrator

import (
	"strings"
	"testing"
)

func TestSanitizeAndSplitSchema(t *testing.T) {
	raw := strings.Join([]string{
		"SET statement_timeout = 0;",
		"SET transaction_timeout = 0;",
		"SET idle_in_transaction_session_timeout = 0;",
		"CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;",
		"COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';",
		"GRANT ALL ON SCHEMA public TO rdsadmin;",
		"ALTER SCHEMA public OWNER TO cloudsqlsuperuser;",
		"GRANT USAGE ON SCHEMA public TO azure_pg_admin;",
		"",
		"CREATE TABLE public.orders (",
		"    id bigint NOT NULL,",
		"    customer_id bigint NOT NULL",
		");",
		"CREATE SEQUENCE public.orders_id_seq START WITH 1;",
		"CREATE INDEX idx_orders_cust ON public.orders (customer_id);",
		"",
		"ALTER TABLE ONLY public.orders",
		"    ADD CONSTRAINT orders_pkey PRIMARY KEY (id);",
		"",
		"ALTER TABLE public.orders ADD CONSTRAINT orders_uniq UNIQUE (id, customer_id);",
		"ALTER TABLE public.orders REPLICA IDENTITY FULL;",
		"",
		"ALTER TABLE ONLY public.orders",
		"    ADD CONSTRAINT orders_customer_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(id);",
		"",
		"ALTER TABLE public.lineitem ADD CONSTRAINT fk_lineitem_order FOREIGN KEY (order_id) REFERENCES public.orders(id);",
	}, "\n")

	baseSQL, deferredFKs, removed := sanitizeAndSplitSchema(raw)

	if removed != 7 {
		t.Fatalf("removed = %d, want 7", removed)
	}
	if len(deferredFKs) != 2 {
		t.Fatalf("len(deferredFKs) = %d, want 2: %#v", len(deferredFKs), deferredFKs)
	}
	if strings.Contains(strings.ToUpper(baseSQL), "FOREIGN KEY") {
		t.Fatalf("baseSQL still contains FOREIGN KEY:\n%s", baseSQL)
	}
	for _, mustKeep := range []string{
		"SET statement_timeout = 0;",
		"CREATE TABLE public.orders",
		"CREATE SEQUENCE public.orders_id_seq",
		"CREATE INDEX idx_orders_cust",
		"PRIMARY KEY (id);",
		"UNIQUE (id, customer_id);",
		"REPLICA IDENTITY FULL;",
	} {
		if !strings.Contains(baseSQL, mustKeep) {
			t.Errorf("baseSQL missing expected DDL %q", mustKeep)
		}
	}
	for _, mustStrip := range []string{
		"transaction_timeout",
		"idle_in_transaction_session_timeout",
		"rdsadmin",
		"cloudsqlsuperuser",
		"azure_pg_admin",
		"CREATE EXTENSION IF NOT EXISTS plpgsql",
		"COMMENT ON EXTENSION",
	} {
		if strings.Contains(baseSQL, mustStrip) {
			t.Errorf("baseSQL should have stripped %q", mustStrip)
		}
	}
	if !strings.Contains(deferredFKs[0], "orders_customer_fkey") || !strings.Contains(deferredFKs[1], "fk_lineitem_order") {
		t.Fatalf("unexpected deferredFKs: %#v", deferredFKs)
	}
}

func TestSanitizeAndSplitSchemaEdgeCases(t *testing.T) {
	t.Run("empty_input", func(t *testing.T) {
		base, fks, removed := sanitizeAndSplitSchema("")
		if base != "" || len(fks) != 0 || removed != 0 {
			t.Fatalf("got (%q, %v, %d), want empty", base, fks, removed)
		}
	})

	t.Run("crlf_and_eof_flush", func(t *testing.T) {
		raw := "SET transaction_timeout = 0;\r\nALTER TABLE public.a\r\n  ADD CONSTRAINT fk_a FOREIGN KEY (b_id) REFERENCES public.b(id);"
		base, fks, removed := sanitizeAndSplitSchema(raw)
		if removed != 1 {
			t.Fatalf("removed = %d, want 1", removed)
		}
		if len(fks) != 1 || !strings.Contains(fks[0], "fk_a") {
			t.Fatalf("expected 1 deferred FK with CRLF, got %#v", fks)
		}
		if strings.Contains(strings.ToUpper(base), "FOREIGN KEY") {
			t.Fatalf("baseSQL should not contain FOREIGN KEY")
		}
	})
}
