import { useEffect, useState, useCallback } from 'react';
import { api, formatBytes } from './api';
import type { Job, JobLog, LabStatus, MetricSample } from './types';
import { MetricsChart } from './components/MetricsChart';
import { CreateJobModal } from './components/CreateJobModal';

type PhaseTab = 'preflight' | 'schema' | 'replicate' | 'verify' | 'cutover';

function statusBadgeClass(st: string): string {
  switch (st) {
    case 'cutover_done':
    case 'preflight_ok':
    case 'pass':
    case 'ok':
    case 'r':
      return 'badge-ok';
    case 'replicating':
    case 'schema_applied':
    case 'd':
    case 's':
    case 'f':
      return 'badge-info';
    case 'warn':
    case 'i':
      return 'badge-warn';
    case 'failed':
    case 'aborted':
    case 'fail':
    case 'error':
      return 'badge-err';
    default:
      return 'badge-purple';
  }
}

export function App() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [selectedId, setSelectedId] = useState<string>('');
  const [job, setJob] = useState<Job | null>(null);
  const [tab, setTab] = useState<PhaseTab>('preflight');
  const [metrics, setMetrics] = useState<MetricSample[]>([]);
  const [logs, setLogs] = useState<JobLog[]>([]);
  const [lab, setLab] = useState<LabStatus>({});
  const [showCreate, setShowCreate] = useState(false);

  const [busyAction, setBusyAction] = useState('');
  const [banner, setBanner] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);

  // Cutover options
  const [usePgBouncer, setUsePgBouncer] = useState(true);
  const [setReadOnly, setSetReadOnly] = useState(true);
  const [applyFKs, setApplyFKs] = useState(true);
  const [seqBuffer, setSeqBuffer] = useState(0);

  // Toxiproxy lab state
  const [latencyMs, setLatencyMs] = useState(0);
  const [linkEnabled, setLinkEnabled] = useState(true);

  const refreshJobs = useCallback(async () => {
    try {
      const list = await api.listJobs();
      setJobs(list);
      if (!selectedId && list.length > 0) {
        setSelectedId(list[0].id);
      }
    } catch {
      // ignore transient error during boot
    }
  }, [selectedId]);

  const refreshSelected = useCallback(async (id: string) => {
    if (!id) return;
    try {
      const [j, m, l] = await Promise.all([
        api.getJob(id),
        api.getMetrics(id),
        api.getLogs(id),
      ]);
      setJob(j);
      setMetrics(m);
      setLogs(l);
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    refreshJobs();
  }, [refreshJobs]);

  useEffect(() => {
    if (!selectedId) {
      setJob(null);
      return;
    }
    refreshSelected(selectedId);

    // Subscribe to real-time SSE metric stream
    const es = new EventSource(`/api/jobs/${selectedId}/stream`);
    es.addEventListener('metric', (ev) => {
      try {
        const sample = JSON.parse((ev as MessageEvent).data) as MetricSample;
        setMetrics((prev) => [...prev.slice(-119), sample]);
      } catch {
        // ignore parse error
      }
    });
    return () => es.close();
  }, [selectedId, refreshSelected]);

  // Poll Docker Lab status (workload TPS, PgBouncer backend, Toxiproxy) every 3s
  useEffect(() => {
    let active = true;
    const poll = async () => {
      try {
        const st = await api.labStatus();
        if (active) setLab(st);
      } catch {
        // lab services optional
      }
    };
    poll();
    const t = setInterval(poll, 3000);
    return () => {
      active = false;
      clearInterval(t);
    };
  }, []);

  async function runAction(label: string, fn: () => Promise<unknown>, nextTab?: PhaseTab) {
    setBusyAction(label);
    setBanner(null);
    try {
      await fn();
      setBanner({ type: 'ok', text: `${label} succeeded.` });
      await refreshJobs();
      if (selectedId) await refreshSelected(selectedId);
      if (nextTab) setTab(nextTab);
    } catch (e) {
      setBanner({ type: 'err', text: `${label} failed: ${(e as Error).message}` });
      if (selectedId) await refreshSelected(selectedId);
    } finally {
      setBusyAction('');
    }
  }

  async function applyNetworkSim(lat: number, enabled: boolean) {
    setLatencyMs(lat);
    setLinkEnabled(enabled);
    try {
      await api.setToxiproxy(lat, Math.round(lat * 0.15), enabled);
    } catch (e) {
      setBanner({ type: 'err', text: (e as Error).message });
    }
  }

  const latest = metrics.length > 0 ? metrics[metrics.length - 1] : null;
  const walLimitBytes = (job?.wal_guardrail_mb || 4096) * 1024 * 1024;
  const walPct = latest ? Math.min(100, (latest.wal_retained_bytes / walLimitBytes) * 100) : 0;

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">
          <h1>PG Zero-Downtime</h1>
          <div className="sub">Logical Replication Orchestrator • pg-meta</div>
        </div>

        <button className="btn btn-primary" onClick={() => setShowCreate(true)}>
          + New Migration Job
        </button>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 8, flex: 1, overflowY: 'auto' }}>
          {jobs.length === 0 && (
            <div style={{ fontSize: 12, color: 'var(--muted)' }}>
              No jobs yet. Click <strong>+ New Migration Job</strong> and select the Docker Lab preset to get started.
            </div>
          )}
          {jobs.map((j) => (
            <div
              key={j.id}
              className={`job-item ${j.id === selectedId ? 'active' : ''}`}
              onClick={() => setSelectedId(j.id)}
            >
              <div className="row" style={{ justifyContent: 'space-between' }}>
                <span className="title">{j.name}</span>
              </div>
              <div className="meta">
                {j.source_host}/{j.source_db} → {j.target_host}/{j.target_db}
              </div>
              <div>
                <span className={`badge ${statusBadgeClass(j.status)}`}>{j.status}</span>
              </div>
            </div>
          ))}
        </div>

        {/* Docker Lab Live Traffic & Router Widget */}
        {lab.workload && (
          <div className="card" style={{ padding: 12, background: 'var(--surface-2)' }}>
            <div className="row" style={{ justifyContent: 'space-between', marginBottom: 8 }}>
              <strong style={{ fontSize: 12 }}>Docker Lab Traffic</strong>
              <span className={`badge ${lab.workload.running ? 'badge-ok' : 'badge-warn'}`}>
                {lab.workload.running ? `${lab.workload.actual_tps} TPS` : 'PAUSED'}
              </span>
            </div>
            <div style={{ fontSize: 11, color: 'var(--muted)', display: 'flex', flexDirection: 'column', gap: 3 }}>
              <div>Active DB: <strong className="mono" style={{ color: 'var(--text)' }}>{lab.workload.current_db || '-'}</strong></div>
              <div>PgBouncer: <span className="mono">{lab.pgbouncer?.backend || '-'}</span></div>
              <div>Tx: <span className="mono">+{lab.workload.inserts} ins / {lab.workload.updates} upd</span></div>
              <div>
                Errors:{' '}
                <strong style={{ color: lab.workload.errors > 0 ? 'var(--err)' : 'var(--ok)' }}>
                  {lab.workload.errors}
                </strong>{' '}
                | Max Lat: {lab.workload.max_latency_ms}ms
              </div>
            </div>
            <div className="row" style={{ marginTop: 8 }}>
              <button
                className="btn btn-sm"
                onClick={() => api.toggleWorkload(!lab.workload?.running, 15).then(() => api.labStatus().then(setLab))}
              >
                {lab.workload.running ? 'Pause App' : 'Start App'}
              </button>
              <button
                className="btn btn-sm"
                onClick={() => api.resetWorkload().then(() => api.labStatus().then(setLab))}
              >
                Reset Err
              </button>
            </div>
          </div>
        )}
      </aside>

      <main className="main">
        {!job ? (
          <div className="card" style={{ padding: 36, textAlign: 'center' }}>
            <h2 style={{ marginTop: 0 }}>PostgreSQL Zero-Downtime Cross-Cloud Migrator</h2>
            <p style={{ color: 'var(--muted)', maxWidth: 560, margin: '0 auto 18px' }}>
              Migration state, pre-flight reports, audit logs, and time-series lag metrics are stored entirely in <strong>PostgreSQL (pg-meta)</strong> on an isolated internal network. Database passwords are encrypted with AES-256-GCM.
            </p>
            <button className="btn btn-primary" onClick={() => setShowCreate(true)}>
              + Create First Migration Job
            </button>
          </div>
        ) : (
          <>
            <div className="row" style={{ justifyContent: 'space-between' }}>
              <div>
                <h2 style={{ margin: 0, fontSize: 20 }}>{job.name}</h2>
                <div className="mono" style={{ fontSize: 12, color: 'var(--muted)', marginTop: 3 }}>
                  {job.source_host}:{job.source_port}/{job.source_db} → {job.target_host}:{job.target_port}/{job.target_db}
                  {job.repl_source_host ? ` (WAL stream via ${job.repl_source_host}:${job.repl_source_port})` : ''}
                </div>
              </div>
              <div className="row">
                <span className={`badge ${statusBadgeClass(job.status)}`}>{job.status}</span>
                <button
                  className="btn btn-danger btn-sm"
                  disabled={!!busyAction}
                  onClick={() => {
                    if (window.confirm('Abort replication and drop the slot on Source immediately?')) {
                      runAction('Abort & Cleanup', () => api.abortJob(job.id, true));
                    }
                  }}
                >
                  Emergency Abort & Drop Slot
                </button>
                <button
                  className="btn btn-sm"
                  disabled={!!busyAction}
                  onClick={() => {
                    if (window.confirm('Delete this job from pg-meta?')) {
                      api.deleteJob(job.id).then(() => {
                        setSelectedId('');
                        refreshJobs();
                      });
                    }
                  }}
                >
                  Delete Job
                </button>
              </div>
            </div>

            {banner && (
              <div className={`alert ${banner.type === 'ok' ? 'alert-ok' : 'alert-err'}`}>
                {banner.text}
              </div>
            )}

            {/* 5-Step Pipeline Stepper */}
            <div className="stepper">
              {([
                ['preflight', 'Step 1', 'Pre-flight Check', !!job.preflight_report?.passed],
                ['schema', 'Step 2', 'Schema & FK Split', ['schema_applied', 'replicating', 'verified', 'cutover_done'].includes(job.status)],
                ['replicate', 'Step 3', 'Initial Sync & CDC', ['replicating', 'verified', 'cutover_done'].includes(job.status)],
                ['verify', 'Step 4', 'Verify & Sequences', !!job.verify_report],
                ['cutover', 'Step 5', 'Guided Cutover', job.status === 'cutover_done'],
              ] as const).map(([key, num, label, done]) => (
                <div
                  key={key}
                  className={`step ${tab === key ? 'active' : ''} ${done ? 'done' : ''}`}
                  onClick={() => setTab(key)}
                >
                  <div className="step-num">{num} {done ? '✓' : ''}</div>
                  <div className="step-label">{label}</div>
                </div>
              ))}
            </div>

            {/* PHASE 1: PREFLIGHT */}
            {tab === 'preflight' && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">1. Pre-flight Validation (Source & Target)</h3>
                  <div className="row">
                    {job.preflight_report && job.preflight_report.tables_without_pk.length > 0 && (
                      <button
                        className="btn btn-sm"
                        disabled={!!busyAction}
                        onClick={() => runAction('Fix REPLICA IDENTITY FULL', () => api.fixReplicaIdentity(job.id))}
                      >
                        Auto-Fix REPLICA IDENTITY
                      </button>
                    )}
                    <button
                      className="btn btn-primary"
                      disabled={!!busyAction}
                      onClick={() => runAction('Pre-flight Check', () => api.runPreflight(job.id))}
                    >
                      {busyAction === 'Pre-flight Check' ? 'Checking...' : 'Run Pre-flight Check'}
                    </button>
                  </div>
                </div>

                {job.preflight_report ? (
                  <>
                    <div className="kpi-grid" style={{ marginBottom: 14 }}>
                      <div className="kpi">
                        <div className="label">Source PG</div>
                        <div className="val" style={{ fontSize: 16 }}>{job.preflight_report.source_version.split(' ')[0]}</div>
                        <div className="sub">Size: {formatBytes(job.preflight_report.source_size_bytes)}</div>
                      </div>
                      <div className="kpi">
                        <div className="label">Target PG</div>
                        <div className="val" style={{ fontSize: 16 }}>{job.preflight_report.target_version.split(' ')[0]}</div>
                        <div className="sub">Major upgrade supported</div>
                      </div>
                      <div className="kpi">
                        <div className="label">User Tables</div>
                        <div className="val">{job.preflight_report.table_count}</div>
                        <div className="sub">{job.preflight_report.tables_without_pk.length} missing PK</div>
                      </div>
                      <div className="kpi">
                        <div className="label">Sequences</div>
                        <div className="val">{job.preflight_report.sequence_count}</div>
                        <div className="sub">Synced at Cutover</div>
                      </div>
                    </div>

                    <table>
                      <thead>
                        <tr>
                          <th>Check</th>
                          <th>Status</th>
                          <th>Details</th>
                        </tr>
                      </thead>
                      <tbody>
                        {job.preflight_report.checks.map((c) => (
                          <tr key={c.name}>
                            <td><strong>{c.name}</strong></td>
                            <td><span className={`badge ${statusBadgeClass(c.status)}`}>{c.status}</span></td>
                            <td>
                              {c.detail}
                              {c.fix_sql && <div className="code-box">{c.fix_sql}</div>}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>

                    {job.preflight_report.passed && (
                      <div className="row" style={{ justifyContent: 'flex-end', marginTop: 14 }}>
                        <button className="btn btn-ok" onClick={() => setTab('schema')}>
                          Continue to Step 2: Schema Sync →
                        </button>
                      </div>
                    )}
                  </>
                ) : (
                  <p style={{ color: 'var(--muted)', fontSize: 13 }}>
                    Click <strong>Run Pre-flight Check</strong> to validate wal_level, replication slots, Primary Keys, extensions, and database size.
                  </p>
                )}
              </div>
            )}

            {/* PHASE 2: SCHEMA SYNC */}
            {tab === 'schema' && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">2. Schema Sync (Cloud Sanitizer + Defer Foreign Keys)</h3>
                  <button
                    className="btn btn-primary"
                    disabled={!!busyAction}
                    onClick={() => runAction('Schema Sync', () => api.syncSchema(job.id), 'replicate')}
                  >
                    {busyAction === 'Schema Sync' ? 'Dumping & applying...' : 'Dump & Apply Schema to Target'}
                  </button>
                </div>
                <p style={{ color: 'var(--muted)', fontSize: 13 }}>
                  Runs <code>pg_dump --schema-only --no-owner --no-privileges</code> (stripping cloud-specific roles such as <code>rdsadmin</code> and <code>cloudsqlsuperuser</code>), creates tables, Primary Keys, and indexes on Target, and <strong>defers {job.deferred_fk_count || 'all'} Foreign Key constraints</strong> into <code>pg-meta</code> so parallel initial table copies never fail on FK ordering. Deferred FKs are applied online via <code>NOT VALID + VALIDATE CONSTRAINT</code> before cutover.
                </p>
                {job.deferred_fk_count > 0 && (
                  <div className="alert alert-ok">
                    Base schema applied successfully! Deferred {job.deferred_fk_count} Foreign Key constraints (status: {job.fks_applied ? 'Applied' : 'Pending before Cutover'}).
                  </div>
                )}
              </div>
            )}

            {/* PHASE 3: REPLICATION & LIVE MONITORING */}
            {tab === 'replicate' && (
              <>
                <div className="card">
                  <div className="card-header">
                    <h3 className="card-title">3. Initial Data Copy & Live Logical Replication (CDC)</h3>
                    <div className="row">
                      <button
                        className="btn btn-primary"
                        disabled={!!busyAction || job.status === 'replicating' || job.status === 'cutover_done'}
                        onClick={() => runAction('Start Replication', () => api.startReplication(job.id))}
                      >
                        {busyAction === 'Start Replication' ? 'Starting...' : 'Start Initial Sync + CDC'}
                      </button>
                      <button className="btn" onClick={() => setTab('verify')}>
                        Next: Step 4 Verify →
                      </button>
                    </div>
                  </div>

                  <div className="kpi-grid" style={{ marginBottom: 16 }}>
                    <div className="kpi">
                      <div className="label">Replication Lag</div>
                      <div className="val" style={{ color: (latest?.lag_bytes || 0) > 10 * 1024 * 1024 ? 'var(--warn)' : 'var(--ok)' }}>
                        {formatBytes(latest?.lag_bytes || 0)}
                      </div>
                      <div className="sub">State: {latest?.replication_state || 'idle'}</div>
                    </div>
                    <div className="kpi">
                      <div className="label">WAL Retained (Source)</div>
                      <div className="val">{formatBytes(latest?.wal_retained_bytes || 0)}</div>
                      <div className="sub">Guardrail: {job.wal_guardrail_mb} MiB ({walPct.toFixed(1)}%)</div>
                      <div className="progress-track" style={{ marginTop: 6 }}>
                        <div
                          className="progress-fill"
                          style={{
                            width: `${walPct}%`,
                            background: walPct > 80 ? 'var(--err)' : walPct > 50 ? 'var(--warn)' : 'var(--ok)',
                          }}
                        />
                      </div>
                    </div>
                    <div className="kpi">
                      <div className="label">Streaming Tables</div>
                      <div className="val">
                        {latest?.tables_ready || 0} / {latest?.tables_total || 0}
                      </div>
                      <div className="sub">Slot active: {latest?.slot_active ? 'YES' : 'NO'}</div>
                    </div>
                    <div className="kpi">
                      <div className="label">Target / Source Size</div>
                      <div className="val" style={{ fontSize: 16 }}>
                        {formatBytes(latest?.target_size_bytes || 0)}
                      </div>
                      <div className="sub">Source: {formatBytes(latest?.source_size_bytes || 0)}</div>
                    </div>
                  </div>

                  <MetricsChart samples={metrics} />
                </div>

                <div className="grid-2">
                  {/* Per-table progress */}
                  <div className="card">
                    <div className="card-header">
                      <h3 className="card-title">Per-Table Sync Progress (pg_subscription_rel)</h3>
                    </div>
                    <table>
                      <thead>
                        <tr>
                          <th>Table</th>
                          <th>State</th>
                          <th>Target Size</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(latest?.table_states || []).length === 0 ? (
                          <tr><td colSpan={3} style={{ color: 'var(--muted)' }}>No subscription data yet.</td></tr>
                        ) : (
                          latest!.table_states.map((t) => (
                            <tr key={`${t.schema}.${t.table}`}>
                              <td className="mono">{t.schema}.{t.table}</td>
                              <td>
                                <span className={`badge ${statusBadgeClass(t.state)}`}>
                                  {t.state} — {t.state_label}
                                </span>
                              </td>
                              <td className="mono">{formatBytes(t.target_bytes)}</td>
                            </tr>
                          ))
                        )}
                      </tbody>
                    </table>
                  </div>

                  {/* Cross-Cloud Network Simulator (Toxiproxy) */}
                  <div className="card">
                    <div className="card-header">
                      <h3 className="card-title">Cross-Cloud Network Simulator (Toxiproxy)</h3>
                      <span className={`badge ${linkEnabled ? 'badge-ok' : 'badge-err'}`}>
                        {linkEnabled ? `LINK UP (${latencyMs}ms)` : 'LINK CUT'}
                      </span>
                    </div>
                    <p style={{ fontSize: 12, color: 'var(--muted)', marginTop: 0 }}>
                      Simulate WAN latency between clouds or sudden link drops to observe WAL Retained growth and Logical Replication auto-recovery.
                    </p>
                    <div className="row" style={{ marginBottom: 12 }}>
                      {[0, 35, 80, 200].map((ms) => (
                        <button
                          key={ms}
                          className={`btn btn-sm ${latencyMs === ms && linkEnabled ? 'btn-primary' : ''}`}
                          onClick={() => applyNetworkSim(ms, true)}
                        >
                          {ms === 0 ? '0ms (LAN)' : `${ms}ms WAN`}
                        </button>
                      ))}
                      <button
                        className="btn btn-danger btn-sm"
                        onClick={() => applyNetworkSim(latencyMs, !linkEnabled)}
                      >
                        {linkEnabled ? 'Cut Link' : 'Restore Link'}
                      </button>
                    </div>
                  </div>
                </div>
              </>
            )}

            {/* PHASE 4: VERIFY */}
            {tab === 'verify' && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">4. Data Consistency & Sequence Drift Verification</h3>
                  <div className="row">
                    <button
                      className="btn btn-primary"
                      disabled={!!busyAction}
                      onClick={() => runAction('Verify Data', () => api.verifyData(job.id))}
                    >
                      {busyAction === 'Verify Data' ? 'Counting & hashing...' : 'Run Verify Now'}
                    </button>
                    <button className="btn btn-ok" onClick={() => setTab('cutover')}>
                      Next: Step 5 Guided Cutover →
                    </button>
                  </div>
                </div>

                {job.verify_report ? (
                  <>
                    <h4 style={{ margin: '8px 0', fontSize: 13 }}>Table Comparison (Exact Count + PK-ordered Sample MD5)</h4>
                    <table>
                      <thead>
                        <tr>
                          <th>Table</th>
                          <th>Source Rows</th>
                          <th>Target Rows</th>
                          <th>Diff</th>
                          <th>Sample MD5</th>
                          <th>Time</th>
                        </tr>
                      </thead>
                      <tbody>
                        {job.verify_report.tables.map((t) => (
                          <tr key={`${t.schema}.${t.table}`}>
                            <td className="mono">{t.schema}.{t.table}</td>
                            <td className="mono">{t.source_rows.toLocaleString()}</td>
                            <td className="mono">{t.target_rows.toLocaleString()}</td>
                            <td className="mono" style={{ color: t.row_diff === 0 ? 'var(--ok)' : 'var(--warn)' }}>
                              {t.row_diff}
                            </td>
                            <td>
                              <span className={`badge ${t.sample_match ? 'badge-ok' : 'badge-warn'}`}>
                                {t.sample_match ? 'MATCH' : 'LIVE DELTA'}
                              </span>
                            </td>
                            <td className="mono">{t.duration_ms}ms</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>

                    <h4 style={{ margin: '18px 0 8px', fontSize: 13 }}>
                      Sequences Drift (PostgreSQL Logical Replication does not sync sequences — automatically synced during Cutover)
                    </h4>
                    <table>
                      <thead>
                        <tr>
                          <th>Sequence</th>
                          <th>Source Value</th>
                          <th>Target Value</th>
                          <th>Drift</th>
                        </tr>
                      </thead>
                      <tbody>
                        {job.verify_report.sequences.length === 0 ? (
                          <tr><td colSpan={4} style={{ color: 'var(--muted)' }}>No sequences found.</td></tr>
                        ) : (
                          job.verify_report.sequences.map((s) => (
                            <tr key={`${s.schema}.${s.sequence}`}>
                              <td className="mono">{s.schema}.{s.sequence}</td>
                              <td className="mono">{s.source_value.toLocaleString()}</td>
                              <td className="mono">{s.target_value.toLocaleString()}</td>
                              <td className="mono" style={{ color: s.drift > 0 ? 'var(--warn)' : 'var(--ok)' }}>
                                {s.drift > 0 ? `-${s.drift.toLocaleString()} (synced at Cutover)` : '0 (Synced)'}
                              </td>
                            </tr>
                          ))
                        )}
                      </tbody>
                    </table>
                  </>
                ) : (
                  <p style={{ color: 'var(--muted)', fontSize: 13 }}>
                    Run Verify once tables reach <strong>Streaming (r)</strong> to compare row counts and inspect sequence drift.
                  </p>
                )}
              </div>
            )}

            {/* PHASE 5: GUIDED CUTOVER */}
            {tab === 'cutover' && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">5. Guided Zero-Downtime Cutover</h3>
                  {lab.pgbouncer && (
                    <button
                      className="btn btn-sm"
                      onClick={() => api.resetPgBouncer().then(() => api.labStatus().then(setLab))}
                    >
                      Reset PgBouncer to pg-source
                    </button>
                  )}
                </div>

                <div className="grid-2" style={{ marginBottom: 16 }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                    <label className="row" style={{ cursor: 'pointer' }}>
                      <input type="checkbox" checked={usePgBouncer} onChange={(e) => setUsePgBouncer(e.target.checked)} />
                      <span>Use PgBouncer <code>PAUSE → SWITCH → RESUME</code> (Zero-drop connections on Docker Lab)</span>
                    </label>
                    <label className="row" style={{ cursor: 'pointer' }}>
                      <input type="checkbox" checked={setReadOnly} onChange={(e) => setSetReadOnly(e.target.checked)} />
                      <span>Set Source DB to <code>default_transaction_read_only = on</code> while draining WAL</span>
                    </label>
                    <label className="row" style={{ cursor: 'pointer' }}>
                      <input type="checkbox" checked={applyFKs} onChange={(e) => setApplyFKs(e.target.checked)} />
                      <span>Apply & validate deferred Foreign Keys (<code>NOT VALID → VALIDATE</code>) before pause</span>
                    </label>
                  </div>
                  <div className="field">
                    <label>Sequence Safety Buffer (added to sequence values after sync)</label>
                    <input type="number" value={seqBuffer} onChange={(e) => setSeqBuffer(Number(e.target.value))} />
                  </div>
                </div>

                <button
                  className="btn btn-ok"
                  disabled={!!busyAction || job.status === 'cutover_done'}
                  onClick={() => {
                    if (window.confirm('Execute Cutover to switch traffic from Source to Target?')) {
                      runAction('Zero-Downtime Cutover', () =>
                        api.cutover(job.id, {
                          use_pgbouncer: usePgBouncer,
                          set_source_readonly: setReadOnly,
                          apply_deferred_fks: applyFKs,
                          sequence_buffer: Number(seqBuffer) || 0,
                          max_wait_lag_seconds: 30,
                        }),
                      );
                    }
                  }}
                >
                  {busyAction === 'Zero-Downtime Cutover' ? 'Executing Cutover...' : 'Execute Zero-Downtime Cutover'}
                </button>

                {job.cutover_report && (
                  <div style={{ marginTop: 18 }}>
                    <div className="alert alert-ok" style={{ marginBottom: 12 }}>
                      <strong>Cutover complete!</strong> Actual write-freeze duration:{' '}
                      <strong>{job.cutover_report.downtime_ms} ms</strong> | Sequences synced:{' '}
                      <strong>{job.cutover_report.sequences_synced}</strong> | FKs applied:{' '}
                      <strong>{job.cutover_report.fks_applied}</strong>
                    </div>
                    <table>
                      <thead>
                        <tr>
                          <th>Cutover Step</th>
                          <th>Status</th>
                          <th>Duration</th>
                          <th>Details</th>
                        </tr>
                      </thead>
                      <tbody>
                        {job.cutover_report.steps.map((st) => (
                          <tr key={st.name}>
                            <td><strong>{st.name}</strong></td>
                            <td><span className={`badge ${statusBadgeClass(st.status)}`}>{st.status}</span></td>
                            <td className="mono">{st.duration_ms} ms</td>
                            <td>{st.detail}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}

            {/* AUDIT LOGS FROM pg-meta */}
            <div className="card">
              <div className="card-header">
                <h3 className="card-title">Audit Logs (stored in PostgreSQL pg-meta)</h3>
                <button className="btn btn-sm" onClick={() => refreshSelected(job.id)}>Refresh</button>
              </div>
              <div className="log-list">
                {logs.map((l) => (
                  <div className="log-line" key={l.id}>
                    <span style={{ color: 'var(--muted)' }}>{new Date(l.created_at).toLocaleTimeString()}</span>
                    <span className={`badge ${statusBadgeClass(l.level)}`}>{l.phase}</span>
                    <span>{l.message}</span>
                  </div>
                ))}
              </div>
            </div>
          </>
        )}
      </main>

      {showCreate && (
        <CreateJobModal
          onClose={() => setShowCreate(false)}
          onCreated={(created) => {
            setShowCreate(false);
            refreshJobs();
            setSelectedId(created.id);
            setTab('preflight');
          }}
        />
      )}
    </div>
  );
}
