.PHONY: init-env up down logs seed-10g seed-1g reset reset-lab audit-supply-chain clean

init-env:
	@if [ -f .env ]; then \
		echo ".env already exists. Remove it first if you want to regenerate secrets."; \
	else \
		umask 077; \
		printf "WEB_PORT=8080\n" > .env; \
		printf "APP_ENCRYPTION_KEY=%s\n" "$$(openssl rand -hex 32)" >> .env; \
		printf "META_DB_PASSWORD=%s\n" "$$(openssl rand -hex 20)" >> .env; \
		printf "SOURCE_ADMIN_PASSWORD=%s\n" "$$(openssl rand -hex 20)" >> .env; \
		printf "TARGET_ADMIN_PASSWORD=%s\n" "$$(openssl rand -hex 20)" >> .env; \
		printf "SOURCE_MIGRATOR_PASSWORD=%s\n" "$$(openssl rand -hex 20)" >> .env; \
		printf "TARGET_MIGRATOR_PASSWORD=%s\n" "$$(openssl rand -hex 20)" >> .env; \
		printf "APP_DB_PASSWORD=%s\n" "$$(openssl rand -hex 20)" >> .env; \
		printf "PGBOUNCER_ADMIN_PASSWORD=%s\n" "$$(openssl rand -hex 20)" >> .env; \
		printf "TPCH_SCALE_FACTOR=10\n" >> .env; \
		printf "WORKLOAD_TPS=15\n" >> .env; \
		chmod 600 .env; \
		echo "Generated .env with random cryptographic secrets (chmod 600)."; \
	fi

up: init-env
	docker compose up -d --build
	@echo ""
	@echo "Web UI running at: http://127.0.0.1:8080"
	@echo "Next: run 'make seed-10g' (~10-14 GiB TPC-H) or 'make seed-1g' (~1.3 GiB quick test)"

seed-10g:
	TPCH_SCALE_FACTOR=10 docker compose --profile seed run --rm tpch-seeder

seed-1g:
	TPCH_SCALE_FACTOR=1 docker compose --profile seed run --rm tpch-seeder

reset: reset-lab

reset-lab:
	@echo "Resetting entire lab for a clean test run (keeping seeded TPC-H tables on pg-source)..."
	@docker compose exec -T pg-target sh -c 'PGPASSWORD="$$POSTGRES_PASSWORD" psql -U postgres -d targetdb -Atc "SELECT subname FROM pg_subscription" | while read -r sub; do [ -n "$$sub" ] && PGPASSWORD="$$POSTGRES_PASSWORD" psql -U postgres -d targetdb -c "ALTER SUBSCRIPTION \"$$sub\" DISABLE; ALTER SUBSCRIPTION \"$$sub\" SET (slot_name = NONE); DROP SUBSCRIPTION \"$$sub\";"; done' || true
	@docker compose exec -T pg-source sh -c 'PGPASSWORD="$$POSTGRES_PASSWORD" psql -U postgres -d postgres -c "ALTER DATABASE sourcedb SET default_transaction_read_only = off;"'
	@docker compose exec -T pg-source sh -c 'PGPASSWORD="$$POSTGRES_PASSWORD" psql -U postgres -d sourcedb -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_type = '\''logical'\'';"' >/dev/null 2>&1 || true
	@docker compose exec -T pg-source sh -c 'PGPASSWORD="$$POSTGRES_PASSWORD" psql -U postgres -d sourcedb -Atc "SELECT pubname FROM pg_publication" | while read -r pub; do [ -n "$$pub" ] && PGPASSWORD="$$POSTGRES_PASSWORD" psql -U postgres -d sourcedb -c "DROP PUBLICATION IF EXISTS \"$$pub\";"; done' || true
	@docker compose exec -T pg-target sh -c 'PGPASSWORD="$$POSTGRES_PASSWORD" psql -U postgres -d targetdb -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public AUTHORIZATION migrator; GRANT USAGE ON SCHEMA public TO appuser; ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO appuser; ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO appuser;"'
	@docker compose exec -T pg-meta sh -c 'PGPASSWORD="$$POSTGRES_PASSWORD" psql -U meta_admin -d pg_migrator_meta -c "TRUNCATE TABLE migration_jobs CASCADE;"'
	@curl -sf -X POST http://127.0.0.1:8080/api/lab/toxiproxy -H 'Content-Type: application/json' -d '{"latency_ms":0,"jitter_ms":0,"enabled":true}' >/dev/null
	@curl -sf -X POST http://127.0.0.1:8080/api/lab/pgbouncer/reset >/dev/null
	@curl -sf -X POST http://127.0.0.1:8081/reset-stats >/dev/null
	@curl -sf -X POST http://127.0.0.1:8081/toggle -H 'Content-Type: application/json' -d '{"running":true,"target_tps":10}' >/dev/null
	@echo "Full lab reset complete! pg-meta cleared, Toxiproxy=0ms, targetdb clean, Demo App (http://127.0.0.1:8081) writing fresh to sourcedb (PG 15.8)."

logs:
	docker compose logs -f backend pgbouncer workload-sim

down:
	docker compose down

clean:
	docker compose --profile seed down -v --remove-orphans

audit-supply-chain:
	@echo "=== 1. Direct Go dependencies ==="
	@cat backend/go.mod
	@echo ""
	@echo "=== 2. Direct Frontend dependencies (ignore-scripts=true) ==="
	@cat frontend/package.json
	@echo ""
	@echo "=== 3. Running Docker Image RepoDigests (for SHA256 pinning) ==="
	@for img in postgres:16.4-alpine postgres:15.8-alpine ghcr.io/shopify/toxiproxy:2.9.0; do \
		docker inspect --format='{{index .RepoDigests 0}}' $$img 2>/dev/null || echo "$$img (not pulled yet)"; \
	done
	@echo ""
	@echo "=== 4. Tip: run 'trivy fs .' or 'docker scout cves' for CVE scanning ==="
