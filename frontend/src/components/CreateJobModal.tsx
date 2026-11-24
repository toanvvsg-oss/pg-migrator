import { useState } from 'react';
import { api } from '../api';
import type { Job } from '../types';

interface Props {
  onClose: () => void;
  onCreated: (job: Job) => void;
}

export function CreateJobModal({ onClose, onCreated }: Props) {
  const [name, setName] = useState('Cross-Cloud PG Migration');
  const [sourceHost, setSourceHost] = useState('');
  const [sourcePort, setSourcePort] = useState(5432);
  const [sourceDB, setSourceDB] = useState('');
  const [sourceUser, setSourceUser] = useState('');
  const [sourcePassword, setSourcePassword] = useState('');
  const [sourceSSLMode, setSourceSSLMode] = useState('require');
  const [replSourceHost, setReplSourceHost] = useState('');
  const [replSourcePort, setReplSourcePort] = useState(0);

  const [targetHost, setTargetHost] = useState('');
  const [targetPort, setTargetPort] = useState(5432);
  const [targetDB, setTargetDB] = useState('');
  const [targetUser, setTargetUser] = useState('');
  const [targetPassword, setTargetPassword] = useState('');
  const [targetSSLMode, setTargetSSLMode] = useState('require');

  const [pubName, setPubName] = useState('pub_pg_migrator');
  const [subName, setSubName] = useState('sub_pg_migrator');
  const [walGuardrail, setWalGuardrail] = useState(4096);
  const [useLabCreds, setUseLabCreds] = useState(false);

  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState('');

  async function loadDockerLabPreset() {
    try {
      setErr('');
      const p = await api.labPreset();
      setName(p.name);
      setSourceHost(p.source_host);
      setSourcePort(p.source_port);
      setSourceDB(p.source_db);
      setSourceUser(p.source_user);
      setSourceSSLMode(p.source_sslmode);
      setReplSourceHost(p.repl_source_host);
      setReplSourcePort(p.repl_source_port);
      setTargetHost(p.target_host);
      setTargetPort(p.target_port);
      setTargetDB(p.target_db);
      setTargetUser(p.target_user);
      setTargetSSLMode(p.target_sslmode);
      setPubName(p.publication_name);
      setSubName(p.subscription_name);
      setWalGuardrail(p.wal_guardrail_mb);
      setUseLabCreds(p.has_lab_credentials);
    } catch (e) {
      setErr((e as Error).message);
    }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setErr('');
    try {
      const job = await api.createJob({
        name,
        source_host: sourceHost,
        source_port: Number(sourcePort),
        source_db: sourceDB,
        source_user: sourceUser,
        source_password: sourcePassword,
        source_sslmode: sourceSSLMode,
        repl_source_host: replSourceHost,
        repl_source_port: Number(replSourcePort) || 0,
        target_host: targetHost,
        target_port: Number(targetPort),
        target_db: targetDB,
        target_user: targetUser,
        target_password: targetPassword,
        target_sslmode: targetSSLMode,
        publication_name: pubName,
        subscription_name: subName,
        wal_guardrail_mb: Number(walGuardrail) || 4096,
        use_lab_credentials: useLabCreds,
      });
      onCreated(job);
    } catch (ex) {
      setErr((ex as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="modal-backdrop">
      <div className="modal">
        <div className="card-header">
          <h2 className="card-title">Create New Migration Job</h2>
          <div className="row">
            <button type="button" className="btn btn-sm" onClick={loadDockerLabPreset}>
              Quick Fill Docker Lab (PG 15 → PG 16)
            </button>
            <button type="button" className="btn btn-sm" onClick={onClose}>
              Close
            </button>
          </div>
        </div>

        {err && <div className="alert alert-err" style={{ marginBottom: 12 }}>{err}</div>}
        {useLabCreds && (
          <div className="alert alert-ok" style={{ marginBottom: 12 }}>
            Docker Lab mode: passwords are loaded directly from backend environment variables & encrypted with AES-256-GCM in pg-meta (never exposed to the browser).
          </div>
        )}

        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          <div className="field">
            <label>Job Name</label>
            <input value={name} onChange={(e) => setName(e.target.value)} required />
          </div>

          <div className="grid-2">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <strong style={{ fontSize: 13 }}>Source PostgreSQL (Cloud A)</strong>
              <div className="form-grid">
                <div className="field">
                  <label>Host</label>
                  <input value={sourceHost} onChange={(e) => setSourceHost(e.target.value)} required />
                </div>
                <div className="field">
                  <label>Port</label>
                  <input type="number" value={sourcePort} onChange={(e) => setSourcePort(Number(e.target.value))} required />
                </div>
                <div className="field">
                  <label>Database</label>
                  <input value={sourceDB} onChange={(e) => setSourceDB(e.target.value)} required />
                </div>
                <div className="field">
                  <label>User</label>
                  <input value={sourceUser} onChange={(e) => setSourceUser(e.target.value)} required />
                </div>
                <div className="field">
                  <label>Password {useLabCreds ? '(from Lab Env)' : ''}</label>
                  <input
                    type="password"
                    value={sourcePassword}
                    placeholder={useLabCreds ? '•••••••• (from environment)' : ''}
                    onChange={(e) => { setSourcePassword(e.target.value); setUseLabCreds(false); }}
                    required={!useLabCreds}
                  />
                </div>
                <div className="field">
                  <label>SSL Mode</label>
                  <select value={sourceSSLMode} onChange={(e) => setSourceSSLMode(e.target.value)}>
                    <option value="disable">disable (Docker Lab)</option>
                    <option value="prefer">prefer</option>
                    <option value="require">require (Cloud)</option>
                    <option value="verify-full">verify-full</option>
                  </select>
                </div>
              </div>
              <div className="form-grid">
                <div className="field">
                  <label>Replication Host (VPC / Toxiproxy)</label>
                  <input value={replSourceHost} onChange={(e) => setReplSourceHost(e.target.value)} placeholder="Default = Source Host" />
                </div>
                <div className="field">
                  <label>Replication Port</label>
                  <input type="number" value={replSourcePort || ''} onChange={(e) => setReplSourcePort(Number(e.target.value))} placeholder="5432" />
                </div>
              </div>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <strong style={{ fontSize: 13 }}>Target PostgreSQL (Cloud B)</strong>
              <div className="form-grid">
                <div className="field">
                  <label>Host</label>
                  <input value={targetHost} onChange={(e) => setTargetHost(e.target.value)} required />
                </div>
                <div className="field">
                  <label>Port</label>
                  <input type="number" value={targetPort} onChange={(e) => setTargetPort(Number(e.target.value))} required />
                </div>
                <div className="field">
                  <label>Database</label>
                  <input value={targetDB} onChange={(e) => setTargetDB(e.target.value)} required />
                </div>
                <div className="field">
                  <label>User</label>
                  <input value={targetUser} onChange={(e) => setTargetUser(e.target.value)} required />
                </div>
                <div className="field">
                  <label>Password {useLabCreds ? '(from Lab Env)' : ''}</label>
                  <input
                    type="password"
                    value={targetPassword}
                    placeholder={useLabCreds ? '•••••••• (from environment)' : ''}
                    onChange={(e) => { setTargetPassword(e.target.value); setUseLabCreds(false); }}
                    required={!useLabCreds}
                  />
                </div>
                <div className="field">
                  <label>SSL Mode</label>
                  <select value={targetSSLMode} onChange={(e) => setTargetSSLMode(e.target.value)}>
                    <option value="disable">disable (Docker Lab)</option>
                    <option value="prefer">prefer</option>
                    <option value="require">require (Cloud)</option>
                    <option value="verify-full">verify-full</option>
                  </select>
                </div>
              </div>
            </div>
          </div>

          <div className="form-grid" style={{ gridTemplateColumns: 'repeat(3, 1fr)' }}>
            <div className="field">
              <label>Publication Name</label>
              <input value={pubName} onChange={(e) => setPubName(e.target.value)} required />
            </div>
            <div className="field">
              <label>Subscription Name</label>
              <input value={subName} onChange={(e) => setSubName(e.target.value)} required />
            </div>
            <div className="field">
              <label>WAL Guardrail (MB)</label>
              <input type="number" value={walGuardrail} onChange={(e) => setWalGuardrail(Number(e.target.value))} required />
            </div>
          </div>

          <div className="row" style={{ justifyContent: 'flex-end', marginTop: 6 }}>
            <button type="button" className="btn" onClick={onClose}>Cancel</button>
            <button type="submit" className="btn btn-primary" disabled={busy}>
              {busy ? 'Creating...' : 'Save to pg-meta'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
