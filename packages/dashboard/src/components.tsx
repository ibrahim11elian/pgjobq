/** Shared presentational pieces. */
import type { Job, JobState } from './api.js';

const STATE_ORDER: JobState[] = ['available', 'running', 'succeeded', 'dead', 'cancelled'];

/**
 * State badge.
 *
 * Renders the state NAME alongside its colour. Colour alone would exclude anyone with
 * a colour vision deficiency, and a dead job looking merely "slightly different" from a
 * succeeded one is a real operational hazard.
 */
export function StateBadge({ state }: { state: JobState }): React.JSX.Element {
  return <span className={`badge ${state}`}>{state}</span>;
}

export function QueueBar({
  counts,
}: {
  counts: Partial<Record<JobState, number>>;
}): React.JSX.Element {
  const total = STATE_ORDER.reduce((sum, s) => sum + (counts[s] ?? 0), 0);
  if (total === 0) return <div className="bar" aria-hidden="true" />;

  return (
    <div
      className="bar"
      role="img"
      aria-label={STATE_ORDER.filter((s) => (counts[s] ?? 0) > 0)
        .map((s) => `${counts[s] ?? 0} ${s}`)
        .join(', ')}
    >
      {STATE_ORDER.map((s) => {
        const n = counts[s] ?? 0;
        if (n === 0) return null;
        return (
          <span
            key={s}
            style={{ width: `${(n / total) * 100}%`, background: `var(--${s})` }}
            title={`${n} ${s}`}
          />
        );
      })}
    </div>
  );
}

export function Duration({ seconds }: { seconds: number }): React.JSX.Element {
  return <span className="mono">{formatDuration(seconds)}</span>;
}

export function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return '—';
  if (seconds < 1) return '<1s';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  if (seconds < 86_400)
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  return `${Math.floor(seconds / 86_400)}d ${Math.floor((seconds % 86_400) / 3600)}h`;
}

export function RelativeTime({ iso }: { iso: string | null }): React.JSX.Element {
  if (iso === null) return <span className="mono">—</span>;
  const then = new Date(iso).getTime();
  const deltaSeconds = (Date.now() - then) / 1000;
  const label =
    deltaSeconds >= 0
      ? `${formatDuration(deltaSeconds)} ago`
      : `in ${formatDuration(-deltaSeconds)}`;
  return (
    <time dateTime={iso} title={iso} className="mono">
      {label}
    </time>
  );
}

/**
 * Payload viewer with redaction.
 *
 * Redacts keys whose NAME suggests a secret. This is defence in depth for a dashboard
 * an operator may screen-share, not a security boundary — the API already redacts at
 * the logging layer, and a payload that must never be visible should not be in the
 * payload.
 */
const SENSITIVE = /pass|secret|token|key|auth|credential|ssn|card|cvv/i;

export function PayloadView({ value }: { value: unknown }): React.JSX.Element {
  return <pre>{JSON.stringify(redact(value), null, 2)}</pre>;
}

function redact(value: unknown, depth = 0): unknown {
  if (depth > 8 || value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map((v) => redact(v, depth + 1));

  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    out[k] = SENSITIVE.test(k) ? '[redacted]' : redact(v, depth + 1);
  }
  return out;
}

export function JobDetail({ job }: { job: Job }): React.JSX.Element {
  return (
    <div>
      <div className="metric-row">
        <span className="label">Attempt</span>
        <span className="value">
          {job.attempt} of {job.maxAttempts}
        </span>
      </div>
      <div className="metric-row">
        <span className="label">Enqueued</span>
        <span className="value">
          <RelativeTime iso={job.enqueuedAt} />
        </span>
      </div>
      <div className="metric-row">
        <span className="label">Runs at</span>
        <span className="value">
          <RelativeTime iso={job.runAt} />
        </span>
      </div>
      {job.workerId !== null && (
        <div className="metric-row">
          <span className="label">Worker</span>
          <span className="value">{job.workerId}</span>
        </div>
      )}
      {job.leaseExpiresAt !== null && (
        <div className="metric-row">
          <span className="label">Lease expires</span>
          <span className="value">
            <RelativeTime iso={job.leaseExpiresAt} />
          </span>
        </div>
      )}
      {job.deadReason !== null && (
        <div className="metric-row">
          <span className="label">Dead reason</span>
          <span className="value">{job.deadReason}</span>
        </div>
      )}
      {job.idempotencyKey !== null && (
        <div className="metric-row">
          <span className="label">Idempotency key</span>
          <span className="value">{job.idempotencyKey}</span>
        </div>
      )}

      <details open={job.errors.length > 0}>
        <summary>Payload</summary>
        <PayloadView value={job.payload} />
      </details>

      {job.errors.length > 0 && (
        <details open>
          <summary>Failure history ({job.errors.length} recorded, most recent last)</summary>
          {job.errors.map((e, i) => (
            <div key={`${e.attempt}-${i}`} style={{ marginBottom: 10 }}>
              <div className="metric-row">
                <span className="label">
                  Attempt {e.attempt} · {e.kind}
                </span>
                <span className="value">
                  <RelativeTime iso={e.at} />
                </span>
              </div>
              <pre>
                {e.name !== undefined ? `${e.name}: ` : ''}
                {e.message ?? '(no message)'}
                {e.stack !== undefined && e.stack.length > 0 ? `\n\n${e.stack}` : ''}
              </pre>
            </div>
          ))}
        </details>
      )}
    </div>
  );
}
