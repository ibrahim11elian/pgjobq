import { Fragment, useCallback, useEffect, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  api,
  getApiKey,
  setApiKey,
  subscribeToEvents,
  type Job,
  type JobState,
  type QueueSummary,
  type Snapshot,
} from './api.js';
import { formatDuration, JobDetail, QueueBar, RelativeTime, StateBadge } from './components.jsx';

type Tab = 'overview' | 'jobs' | 'dlq' | 'schedules';

export default function App(): React.JSX.Element {
  const [tab, setTab] = useState<Tab>('overview');
  const [key, setKey] = useState(getApiKey());

  if (key.length === 0) {
    return (
      <KeyGate
        onSubmit={(k) => {
          setApiKey(k);
          setKey(k);
        }}
      />
    );
  }

  return (
    <div className="app">
      <header className="top">
        <div>
          <h1>pgjobq</h1>
          <div className="tagline">
            Postgres <code>SKIP LOCKED</code> job queue · at-least-once delivery
          </div>
        </div>
        <nav className="tabs" aria-label="Views">
          {(['overview', 'jobs', 'dlq', 'schedules'] as Tab[]).map((t) => (
            <button key={t} onClick={() => setTab(t)} aria-current={tab === t ? 'page' : undefined}>
              {t === 'dlq' ? 'Dead letter' : t[0]?.toUpperCase() + t.slice(1)}
            </button>
          ))}
        </nav>
      </header>

      <main>
        {tab === 'overview' && <Overview />}
        {tab === 'jobs' && <JobsView />}
        {tab === 'dlq' && <DlqView />}
        {tab === 'schedules' && <SchedulesView />}
      </main>
    </div>
  );
}

function KeyGate({ onSubmit }: { onSubmit: (key: string) => void }): React.JSX.Element {
  const [value, setValue] = useState('');
  return (
    <div className="app">
      <main>
        <div className="gate card">
          <h3>API key required</h3>
          <p>
            This dashboard talks to the pgjobq API, which is closed by default. Paste a key with the{' '}
            <code>read</code> scope (and <code>admin</code> for operator actions). In local
            development it is the value of <code>API_BOOTSTRAP_KEY</code>.
          </p>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              if (value.trim().length > 0) onSubmit(value.trim());
            }}
          >
            <label className="field">
              API key
              <input
                type="password"
                value={value}
                onChange={(e) => setValue(e.target.value)}
                placeholder="pgjq_..."
                autoComplete="off"
                aria-describedby="key-hint"
              />
            </label>
            <p id="key-hint" className="approx">
              Stored in sessionStorage and cleared when this tab closes.
            </p>
            <button className="action" type="submit" disabled={value.trim().length === 0}>
              Connect
            </button>
          </form>
        </div>
      </main>
    </div>
  );
}

/**
 * Overview.
 *
 * Reads from the SSE stream rather than polling: N open browsers must not mean N
 * queries per interval against the job table.
 */
function Overview(): React.JSX.Element {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [live, setLive] = useState(false);

  const initial = useQuery({
    queryKey: ['queues'],
    queryFn: api.queues,
    // The stream supplies updates; this is only the first paint.
    refetchInterval: false,
  });

  useEffect(() => subscribeToEvents(setSnapshot, setLive), []);

  const queues: QueueSummary[] = useMemo(() => {
    if (snapshot !== null) {
      return snapshot.queues.map((q) => ({
        queue: q.queue,
        counts: q.counts as Partial<Record<JobState, number>>,
        ready: q.counts['available'] ?? 0,
        oldestWaitSeconds: q.oldestWaitSeconds,
      }));
    }
    return initial.data?.queues ?? [];
  }, [snapshot, initial.data]);

  const totals = useMemo(() => {
    const acc: Partial<Record<JobState, number>> = {};
    for (const q of queues) {
      for (const [state, n] of Object.entries(q.counts)) {
        acc[state as JobState] = (acc[state as JobState] ?? 0) + n;
      }
    }
    return acc;
  }, [queues]);

  if (initial.isError) return <ErrorBanner error={initial.error} />;

  return (
    <>
      <div className="toolbar">
        <span className={`status ${live ? 'live' : ''}`}>
          <span className="dot" />
          {live ? 'Live' : 'Reconnecting…'}
        </span>
        {snapshot !== null && (
          <span className="approx">
            updated <RelativeTime iso={snapshot.at} />
          </span>
        )}
        <span className="sr-only" role="status" aria-live="polite">
          {live ? 'Live updates connected' : 'Live updates disconnected, retrying'}
        </span>
      </div>

      <div className="card" style={{ marginBottom: 12 }}>
        <h3>All queues</h3>
        <QueueBar counts={totals} />
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
          {(['available', 'running', 'succeeded', 'dead', 'cancelled'] as JobState[]).map((s) => (
            <span key={s} style={{ display: 'inline-flex', gap: 6, alignItems: 'center' }}>
              <StateBadge state={s} />
              <span className="mono">{totals[s] ?? 0}</span>
            </span>
          ))}
        </div>
        <p className="approx">
          Counts are exact for the live backlog. Historical totals are bounded by the retention
          window, so they are not a lifetime count.
        </p>
      </div>

      {queues.length === 0 ? (
        <div className="empty">
          No queues yet. Enqueue a job, or run <code>npm run seed</code>.
        </div>
      ) : (
        <div className="grid">
          {queues.map((q) => (
            <QueueCard key={q.queue} summary={q} />
          ))}
        </div>
      )}
    </>
  );
}

function QueueCard({ summary }: { summary: QueueSummary }): React.JSX.Element {
  const qc = useQueryClient();
  const pause = useMutation({
    mutationFn: () => api.pause(summary.queue),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['queues'] }),
  });
  const resume = useMutation({
    mutationFn: () => api.resume(summary.queue),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['queues'] }),
  });

  return (
    <div className="card">
      <h3>
        <span className="name">{summary.queue}</span>
      </h3>
      <QueueBar counts={summary.counts} />
      {(['available', 'running', 'succeeded', 'dead'] as JobState[]).map((s) => (
        <div className="metric-row" key={s}>
          <span className="label">{s}</span>
          <span className="value">{summary.counts[s] ?? 0}</span>
        </div>
      ))}
      <div className="metric-row">
        <span className="label">oldest ready</span>
        <span className="value">{formatDuration(summary.oldestWaitSeconds)}</span>
      </div>
      <div style={{ display: 'flex', gap: 6, marginTop: 10 }}>
        <button className="action" onClick={() => pause.mutate()} disabled={pause.isPending}>
          Pause
        </button>
        <button className="action" onClick={() => resume.mutate()} disabled={resume.isPending}>
          Resume
        </button>
      </div>
    </div>
  );
}

function JobsView(): React.JSX.Element {
  const [queue, setQueue] = useState('');
  const [state, setState] = useState<JobState | ''>('');
  const [cursor, setCursor] = useState<string | undefined>(undefined);
  const [expanded, setExpanded] = useState<string | null>(null);

  const queues = useQuery({ queryKey: ['queues'], queryFn: api.queues });

  // Pick a default queue once the list loads, so the view is never empty on arrival.
  const effectiveQueue = queue !== '' ? queue : (queues.data?.queues[0]?.queue ?? '');

  const jobs = useQuery({
    queryKey: ['jobs', effectiveQueue, state, cursor],
    queryFn: () =>
      api.jobs(effectiveQueue, {
        state: state === '' ? undefined : state,
        cursor,
        limit: '50',
      }),
    enabled: effectiveQueue !== '',
    refetchInterval: 4000,
  });

  const reset = useCallback((next: () => void) => {
    setCursor(undefined);
    next();
  }, []);

  if (queues.isError) return <ErrorBanner error={queues.error} />;

  const nextCursor = jobs.data?.nextCursor ?? null;

  return (
    <>
      <div className="toolbar">
        <label className="field">
          Queue
          <select value={effectiveQueue} onChange={(e) => reset(() => setQueue(e.target.value))}>
            {(queues.data?.queues ?? []).map((q) => (
              <option key={q.queue} value={q.queue}>
                {q.queue}
              </option>
            ))}
          </select>
        </label>
        <label className="field">
          State
          <select
            value={state}
            onChange={(e) => reset(() => setState(e.target.value as JobState | ''))}
          >
            <option value="">all</option>
            {(['available', 'running', 'succeeded', 'dead', 'cancelled'] as JobState[]).map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
        {cursor !== undefined && (
          <button className="action" onClick={() => setCursor(undefined)}>
            First page
          </button>
        )}
      </div>

      {jobs.isError && <ErrorBanner error={jobs.error} />}

      <JobTable
        jobs={jobs.data?.items ?? []}
        expanded={expanded}
        onExpand={setExpanded}
        loading={jobs.isLoading}
      />

      {nextCursor != null && (
        <button className="action" style={{ marginTop: 12 }} onClick={() => setCursor(nextCursor)}>
          Next page
        </button>
      )}
    </>
  );
}

function DlqView(): React.JSX.Element {
  const qc = useQueryClient();
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [expanded, setExpanded] = useState<string | null>(null);

  const dlq = useQuery({
    queryKey: ['dlq'],
    queryFn: () => api.dlq({ limit: '50' }),
    refetchInterval: 5000,
  });

  const replay = useMutation({
    mutationFn: (ids: string[]) => api.replayMany(ids),
    onSuccess: () => {
      setSelected(new Set());
      void qc.invalidateQueries({ queryKey: ['dlq'] });
      void qc.invalidateQueries({ queryKey: ['queues'] });
    },
  });

  const items = dlq.data?.items ?? [];

  const toggle = (id: string): void => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  return (
    <>
      <div className="toolbar">
        <button
          className="action"
          disabled={selected.size === 0 || replay.isPending}
          onClick={() => {
            // Bulk actions get an explicit confirmation: replaying the wrong 50 jobs
            // is not something you can undo.
            const ok = window.confirm(
              `Replay ${selected.size} dead-lettered job(s)? Attempts reset to zero and they ` +
                `become claimable immediately.`,
            );
            if (ok) replay.mutate([...selected]);
          }}
        >
          Replay selected ({selected.size})
        </button>
        {items.length > 0 && (
          <button
            className="action"
            onClick={() =>
              setSelected((prev) =>
                prev.size === items.length ? new Set() : new Set(items.map((j) => j.id)),
              )
            }
          >
            {selected.size === items.length ? 'Clear selection' : 'Select all on page'}
          </button>
        )}
      </div>

      {dlq.isError && <ErrorBanner error={dlq.error} />}
      {replay.isError && <ErrorBanner error={replay.error} />}

      {items.length === 0 && !dlq.isLoading ? (
        <div className="empty">Nothing dead-lettered. That is the desired state.</div>
      ) : (
        <table>
          <thead>
            <tr>
              <th scope="col">
                <span className="sr-only">Select</span>
              </th>
              <th scope="col">Id</th>
              <th scope="col">Queue</th>
              <th scope="col">Type</th>
              <th scope="col">Reason</th>
              <th scope="col">Attempts</th>
              <th scope="col">Failed</th>
              <th scope="col" />
            </tr>
          </thead>
          <tbody>
            {items.map((job) => (
              // Key belongs on the Fragment, not the inner <tr>: React needs it on the
              // outermost element returned from a map, and a colspan detail row means
              // each item renders two siblings.
              <Fragment key={job.id}>
                <tr>
                  <td>
                    <input
                      type="checkbox"
                      checked={selected.has(job.id)}
                      onChange={() => toggle(job.id)}
                      aria-label={`Select job ${job.id}`}
                    />
                  </td>
                  <td className="mono">{job.id}</td>
                  <td className="mono">{job.queue}</td>
                  <td className="mono">{job.type}</td>
                  <td>{job.deadReason}</td>
                  <td className="mono">
                    {job.attempt}/{job.maxAttempts}
                  </td>
                  <td>
                    <RelativeTime iso={job.finishedAt} />
                  </td>
                  <td>
                    <button
                      className="action"
                      onClick={() => setExpanded(expanded === job.id ? null : job.id)}
                      aria-expanded={expanded === job.id}
                    >
                      {expanded === job.id ? 'Hide' : 'Details'}
                    </button>
                  </td>
                </tr>
                {expanded === job.id && (
                  <tr>
                    <td colSpan={8}>
                      <JobDetail job={job} />
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}

function SchedulesView(): React.JSX.Element {
  const qc = useQueryClient();
  const schedules = useQuery({
    queryKey: ['schedules'],
    queryFn: api.schedules,
    refetchInterval: 10_000,
  });

  const toggle = useMutation({
    mutationFn: ({ id, enabled }: { id: string; enabled: boolean }) =>
      enabled ? api.pauseSchedule(id) : api.resumeSchedule(id),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['schedules'] }),
  });

  if (schedules.isError) return <ErrorBanner error={schedules.error} />;
  const items = schedules.data?.items ?? [];

  return (
    <>
      {items.length === 0 && !schedules.isLoading ? (
        <div className="empty">No schedules defined.</div>
      ) : (
        <table>
          <thead>
            <tr>
              <th scope="col">Name</th>
              <th scope="col">Queue</th>
              <th scope="col">Type</th>
              <th scope="col">Cron</th>
              <th scope="col">Timezone</th>
              <th scope="col">Next run</th>
              <th scope="col">Last run</th>
              <th scope="col">State</th>
              <th scope="col" />
            </tr>
          </thead>
          <tbody>
            {items.map((s) => (
              <tr key={s.id}>
                <td className="mono">{s.name}</td>
                <td className="mono">{s.queue}</td>
                <td className="mono">{s.type}</td>
                <td className="mono">{s.cron}</td>
                <td>{s.timezone}</td>
                <td>
                  <RelativeTime iso={s.nextRunAt} />
                </td>
                <td>
                  <RelativeTime iso={s.lastRunAt} />
                </td>
                <td>{s.enabled ? 'enabled' : 'paused'}</td>
                <td>
                  <button
                    className="action"
                    disabled={toggle.isPending}
                    onClick={() => toggle.mutate({ id: s.id, enabled: s.enabled })}
                  >
                    {s.enabled ? 'Pause' : 'Resume'}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <p className="approx">
        Cron expressions are evaluated in each schedule&apos;s own timezone, not the server&apos;s.
        Resuming a paused schedule advances to the next future occurrence rather than backfilling.
      </p>
    </>
  );
}

function JobTable({
  jobs,
  expanded,
  onExpand,
  loading,
}: {
  jobs: Job[];
  expanded: string | null;
  onExpand: (id: string | null) => void;
  loading: boolean;
}): React.JSX.Element {
  const qc = useQueryClient();
  const cancel = useMutation({
    mutationFn: (id: string) => api.cancel(id),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['jobs'] }),
  });
  const retry = useMutation({
    mutationFn: (id: string) => api.retry(id),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['jobs'] }),
  });

  if (loading && jobs.length === 0) return <div className="empty">Loading…</div>;
  if (jobs.length === 0) return <div className="empty">No jobs match these filters.</div>;

  return (
    <table>
      <thead>
        <tr>
          <th scope="col">Id</th>
          <th scope="col">Type</th>
          <th scope="col">State</th>
          <th scope="col">Attempt</th>
          <th scope="col">Priority</th>
          <th scope="col">Enqueued</th>
          <th scope="col">Runs at</th>
          <th scope="col" />
        </tr>
      </thead>
      <tbody>
        {jobs.map((job) => (
          // Key on the Fragment, per the note in DlqView.
          <Fragment key={job.id}>
            <tr>
              <td className="mono">{job.id}</td>
              <td className="mono">{job.type}</td>
              <td>
                <StateBadge state={job.state} />
              </td>
              <td className="mono">
                {job.attempt}/{job.maxAttempts}
              </td>
              <td className="mono">{job.priority}</td>
              <td>
                <RelativeTime iso={job.enqueuedAt} />
              </td>
              <td>
                <RelativeTime iso={job.runAt} />
              </td>
              <td style={{ display: 'flex', gap: 4 }}>
                <button
                  className="action"
                  onClick={() => onExpand(expanded === job.id ? null : job.id)}
                  aria-expanded={expanded === job.id}
                >
                  {expanded === job.id ? 'Hide' : 'Details'}
                </button>
                {job.state === 'available' && (
                  <>
                    <button className="action" onClick={() => retry.mutate(job.id)}>
                      Run now
                    </button>
                    <button className="action danger" onClick={() => cancel.mutate(job.id)}>
                      Cancel
                    </button>
                  </>
                )}
              </td>
            </tr>
            {expanded === job.id && (
              <tr>
                <td colSpan={8}>
                  <JobDetail job={job} />
                </td>
              </tr>
            )}
          </Fragment>
        ))}
      </tbody>
    </table>
  );
}

function ErrorBanner({ error }: { error: unknown }): React.JSX.Element {
  const message = error instanceof Error ? error.message : String(error);
  return (
    <div className="error-banner" role="alert">
      {message}
      {message.includes('401') || message.toLowerCase().includes('key') ? (
        <>
          {' '}
          <button
            className="action"
            onClick={() => {
              setApiKey('');
              window.location.reload();
            }}
          >
            Change API key
          </button>
        </>
      ) : null}
    </div>
  );
}
