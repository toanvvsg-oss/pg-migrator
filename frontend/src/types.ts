export interface CheckItem {
  name: string;
  status: 'pass' | 'warn' | 'fail';
  detail: string;
  fix_sql?: string;
}

export interface PreflightReport {
  checked_at: string;
  passed: boolean;
  source_version: string;
  target_version: string;
  source_size_bytes: number;
  table_count: number;
  sequence_count: number;
  tables_without_pk: string[];
  checks: CheckItem[];
}

export interface TableSyncState {
  schema: string;
  table: string;
  state: string;
  state_label: string;
  target_bytes: number;
  est_rows: number;
}

export interface MetricSample {
  id: number;
  job_id: string;
  recorded_at: string;
  lag_bytes: number;
  wal_retained_bytes: number;
  tables_total: number;
  tables_ready: number;
  source_size_bytes: number;
  target_size_bytes: number;
  replication_state: string;
  slot_active: boolean;
  table_states: TableSyncState[];
}

export interface VerifyTableResult {
  schema: string;
  table: string;
  source_rows: number;
  target_rows: number;
  row_diff: number;
  sample_match: boolean;
  match: boolean;
  duration_ms: number;
}

export interface VerifySequenceResult {
  schema: string;
  sequence: string;
  source_value: number;
  target_value: number;
  drift: number;
}

export interface VerifyReport {
  verified_at: string;
  all_matched: boolean;
  tables: VerifyTableResult[];
  sequences: VerifySequenceResult[];
}

export interface CutoverStep {
  name: string;
  status: string;
  duration_ms: number;
  detail: string;
}

export interface CutoverReport {
  completed_at: string;
  downtime_ms: number;
  sequences_synced: number;
  fks_applied: number;
  steps: CutoverStep[];
}

export interface Job {
  id: string;
  name: string;
  status: string;
  source_host: string;
  source_port: number;
  source_db: string;
  source_user: string;
  source_sslmode: string;
  repl_source_host: string;
  repl_source_port: number;
  target_host: string;
  target_port: number;
  target_db: string;
  target_user: string;
  target_sslmode: string;
  publication_name: string;
  subscription_name: string;
  wal_guardrail_mb: number;
  deferred_fk_count: number;
  fks_applied: boolean;
  preflight_report?: PreflightReport;
  verify_report?: VerifyReport;
  cutover_report?: CutoverReport;
  created_at: string;
  updated_at: string;
}

export interface JobLog {
  id: number;
  job_id: string;
  phase: string;
  level: string;
  message: string;
  created_at: string;
}

export interface LabPreset {
  name: string;
  source_host: string;
  source_port: number;
  source_db: string;
  source_user: string;
  source_sslmode: string;
  repl_source_host: string;
  repl_source_port: number;
  target_host: string;
  target_port: number;
  target_db: string;
  target_user: string;
  target_sslmode: string;
  publication_name: string;
  subscription_name: string;
  wal_guardrail_mb: number;
  has_lab_credentials: boolean;
}

export interface LabStatus {
  workload?: {
    running: boolean;
    target_tps: number;
    actual_tps: number;
    inserts: number;
    updates: number;
    deletes: number;
    errors: number;
    last_tx_id: number;
    current_db: string;
    avg_latency_ms: number;
    max_latency_ms: number;
    last_error: string;
  };
  pgbouncer?: {
    backend: string;
  };
  toxiproxy?: {
    enabled: boolean;
    toxics?: Array<{
      name: string;
      type: string;
      attributes?: { latency?: number; jitter?: number };
    }>;
  };
}
