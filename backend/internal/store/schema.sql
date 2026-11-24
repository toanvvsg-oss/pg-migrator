CREATE TABLE IF NOT EXISTS migration_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'created',

    source_host         TEXT NOT NULL,
    source_port         INT NOT NULL DEFAULT 5432,
    source_db           TEXT NOT NULL,
    source_user         TEXT NOT NULL,
    source_password_enc TEXT NOT NULL,
    source_sslmode      TEXT NOT NULL DEFAULT 'prefer',

    repl_source_host    TEXT NOT NULL DEFAULT '',
    repl_source_port    INT NOT NULL DEFAULT 0,

    target_host         TEXT NOT NULL,
    target_port         INT NOT NULL DEFAULT 5432,
    target_db           TEXT NOT NULL,
    target_user         TEXT NOT NULL,
    target_password_enc TEXT NOT NULL,
    target_sslmode      TEXT NOT NULL DEFAULT 'prefer',

    publication_name    TEXT NOT NULL,
    subscription_name   TEXT NOT NULL,
    wal_guardrail_mb    INT NOT NULL DEFAULT 4096,

    deferred_fks_sql    TEXT NOT NULL DEFAULT '',
    fks_applied         BOOLEAN NOT NULL DEFAULT FALSE,

    preflight_report    JSONB,
    verify_report       JSONB,
    cutover_report      JSONB,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_logs (
    id          BIGSERIAL PRIMARY KEY,
    job_id      UUID NOT NULL REFERENCES migration_jobs(id) ON DELETE CASCADE,
    phase       TEXT NOT NULL,
    level       TEXT NOT NULL DEFAULT 'info',
    message     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_logs_job_created ON job_logs(job_id, id DESC);

CREATE TABLE IF NOT EXISTS metrics_samples (
    id                 BIGSERIAL PRIMARY KEY,
    job_id             UUID NOT NULL REFERENCES migration_jobs(id) ON DELETE CASCADE,
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lag_bytes          BIGINT NOT NULL DEFAULT 0,
    wal_retained_bytes BIGINT NOT NULL DEFAULT 0,
    tables_total       INT NOT NULL DEFAULT 0,
    tables_ready       INT NOT NULL DEFAULT 0,
    source_size_bytes  BIGINT NOT NULL DEFAULT 0,
    target_size_bytes  BIGINT NOT NULL DEFAULT 0,
    replication_state  TEXT NOT NULL DEFAULT '',
    slot_active        BOOLEAN NOT NULL DEFAULT FALSE,
    table_states       JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_metrics_samples_job_time ON metrics_samples(job_id, recorded_at DESC);
