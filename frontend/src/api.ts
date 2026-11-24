import type { Job, JobLog, LabPreset, LabStatus, MetricSample } from './types';

async function req<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers || {}),
    },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }
  return data as T;
}

export const api = {
  health: () => req<{ status: string; storage: string }>('/api/health'),
  labPreset: () => req<LabPreset>('/api/lab/preset'),
  labStatus: () => req<LabStatus>('/api/lab/status'),
  setToxiproxy: (latency_ms: number, jitter_ms: number, enabled: boolean) =>
    req('/api/lab/toxiproxy', {
      method: 'POST',
      body: JSON.stringify({ latency_ms, jitter_ms, enabled }),
    }),
  toggleWorkload: (running?: boolean, target_tps?: number) =>
    req('/api/lab/workload/toggle', {
      method: 'POST',
      body: JSON.stringify({ running, target_tps }),
    }),
  resetWorkload: () => req('/api/lab/workload/reset', { method: 'POST', body: '{}' }),
  resetPgBouncer: () => req('/api/lab/pgbouncer/reset', { method: 'POST', body: '{}' }),

  listJobs: () => req<Job[]>('/api/jobs'),
  getJob: (id: string) => req<Job>(`/api/jobs/${id}`),
  createJob: (payload: Record<string, unknown>) =>
    req<Job>('/api/jobs', { method: 'POST', body: JSON.stringify(payload) }),
  deleteJob: (id: string) => req(`/api/jobs/${id}`, { method: 'DELETE' }),

  runPreflight: (id: string) => req(`/api/jobs/${id}/preflight`, { method: 'POST', body: '{}' }),
  fixReplicaIdentity: (id: string) =>
    req<{ fixed_tables: number }>(`/api/jobs/${id}/fix-replica-identity`, { method: 'POST', body: '{}' }),
  syncSchema: (id: string) => req(`/api/jobs/${id}/schema`, { method: 'POST', body: '{}' }),
  startReplication: (id: string) => req(`/api/jobs/${id}/start-replication`, { method: 'POST', body: '{}' }),
  verifyData: (id: string) => req(`/api/jobs/${id}/verify`, { method: 'POST', body: '{}' }),
  cutover: (id: string, payload: Record<string, unknown>) =>
    req(`/api/jobs/${id}/cutover`, { method: 'POST', body: JSON.stringify(payload) }),
  abortJob: (id: string, reset_lab: boolean) =>
    req(`/api/jobs/${id}/abort`, { method: 'POST', body: JSON.stringify({ reset_lab }) }),

  getMetrics: (id: string) => req<MetricSample[]>(`/api/jobs/${id}/metrics`),
  getLogs: (id: string) => req<JobLog[]>(`/api/jobs/${id}/logs`),
};

export function formatBytes(bytes: number): string {
  if (!bytes || bytes <= 0) return '0 B';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let idx = 0;
  let val = bytes;
  while (val >= 1024 && idx < units.length - 1) {
    val /= 1024;
    idx++;
  }
  return `${val.toFixed(val >= 100 || idx === 0 ? 0 : 2)} ${units[idx]}`;
}
