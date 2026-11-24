#!/bin/sh
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  -- Least-privilege migration role (NOSUPERUSER, mimics RDS/Cloud SQL admin)
  CREATE ROLE migrator WITH LOGIN PASSWORD '${SOURCE_MIGRATOR_PASSWORD}' REPLICATION NOSUPERUSER NOCREATEROLE;
  GRANT pg_monitor TO migrator;
  CREATE ROLE appuser WITH LOGIN PASSWORD '${APP_DB_PASSWORD}' NOSUPERUSER NOCREATEROLE NOCREATEDB;

  ALTER DATABASE ${POSTGRES_DB} OWNER TO migrator;
  ALTER SCHEMA public OWNER TO migrator;

  GRANT CONNECT, TEMP ON DATABASE ${POSTGRES_DB} TO appuser;
  GRANT USAGE ON SCHEMA public TO appuser;

  -- Live OLTP table for continuous zero-downtime workload verification
  CREATE TABLE IF NOT EXISTS public.live_transactions (
      tx_id BIGSERIAL PRIMARY KEY,
      account_id INT NOT NULL,
      amount NUMERIC(12, 2) NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'pending',
      payload TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );

  CREATE INDEX IF NOT EXISTS idx_live_tx_account ON public.live_transactions(account_id);
  CREATE INDEX IF NOT EXISTS idx_live_tx_created ON public.live_transactions(created_at);

  INSERT INTO public.live_transactions (account_id, amount, status, payload)
  SELECT (g % 1000) + 1, ROUND((random() * 500)::numeric, 2), 'settled', 'initial-seed-' || g
  FROM generate_series(1, 500) AS g;

  ALTER TABLE public.live_transactions OWNER TO migrator;
  ALTER SEQUENCE public.live_transactions_tx_id_seq OWNER TO migrator;

  GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO appuser;
  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO appuser;
  ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA public
      GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO appuser;
  ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA public
      GRANT USAGE, SELECT ON SEQUENCES TO appuser;
EOSQL
