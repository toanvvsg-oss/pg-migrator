import type { MetricSample } from '../types';
import { formatBytes } from '../api';

interface Props {
  samples: MetricSample[];
}

// Zero-dependency pure SVG time-series chart for Replication Lag & WAL Retained
export function MetricsChart({ samples }: Props) {
  if (samples.length < 2) {
    return (
      <div style={{ color: 'var(--muted)', fontSize: 13, padding: '28px 0', textAlign: 'center' }}>
        Collecting real-time metrics from pg_replication_slots (updated every 2 seconds)...
      </div>
    );
  }

  const width = 680;
  const height = 190;
  const padL = 62;
  const padR = 16;
  const padT = 14;
  const padB = 24;
  const innerW = width - padL - padR;
  const innerH = height - padT - padB;

  const maxVal = Math.max(
    1024,
    ...samples.map((s) => Math.max(s.lag_bytes, s.wal_retained_bytes)),
  );

  const toX = (i: number) => padL + (i / (samples.length - 1)) * innerW;
  const toY = (v: number) => padT + innerH - (v / maxVal) * innerH;

  const walPath = samples
    .map((s, i) => `${i === 0 ? 'M' : 'L'} ${toX(i).toFixed(1)} ${toY(s.wal_retained_bytes).toFixed(1)}`)
    .join(' ');
  const lagPath = samples
    .map((s, i) => `${i === 0 ? 'M' : 'L'} ${toX(i).toFixed(1)} ${toY(s.lag_bytes).toFixed(1)}`)
    .join(' ');

  const ticks = [0, 0.5, 1];

  return (
    <div>
      <svg viewBox={`0 0 ${width} ${height}`} style={{ width: '100%', height: 'auto', display: 'block' }}>
        {ticks.map((t) => {
          const y = padT + innerH * (1 - t);
          return (
            <g key={t}>
              <line x1={padL} y1={y} x2={width - padR} y2={y} stroke="#233049" strokeDasharray="3 3" />
              <text x={padL - 8} y={y + 4} textAnchor="end" fill="#8ea0bd" fontSize="10">
                {formatBytes(maxVal * t)}
              </text>
            </g>
          );
        })}

        <path d={walPath} fill="none" stroke="#f59e0b" strokeWidth="2" />
        <path d={lagPath} fill="none" stroke="#3b82f6" strokeWidth="2.4" />

        <text x={padL} y={height - 6} fill="#8ea0bd" fontSize="10">
          {new Date(samples[0].recorded_at).toLocaleTimeString()}
        </text>
        <text x={width - padR} y={height - 6} textAnchor="end" fill="#8ea0bd" fontSize="10">
          {new Date(samples[samples.length - 1].recorded_at).toLocaleTimeString()}
        </text>
      </svg>

      <div className="row" style={{ marginTop: 8, fontSize: 12, color: 'var(--muted)' }}>
        <span>
          <span style={{ display: 'inline-block', width: 10, height: 10, background: '#3b82f6', borderRadius: 2, marginRight: 6 }} />
          Replication Lag (bytes)
        </span>
        <span>
          <span style={{ display: 'inline-block', width: 10, height: 10, background: '#f59e0b', borderRadius: 2, marginRight: 6 }} />
          WAL Retained on Source (bytes)
        </span>
      </div>
    </div>
  );
}
