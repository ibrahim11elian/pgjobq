/**
 * API client for the dashboard.
 *
 * The API key lives in sessionStorage, not localStorage: it is cleared when the tab
 * closes, which limits the window in which a shared machine exposes it. A production
 * deployment should front this with a session cookie instead of handing a queue
 * credential to the browser at all.
 */

const KEY_STORAGE = 'pgjobq.apiKey';

export function getApiKey(): string {
  return sessionStorage.getItem(KEY_STORAGE) ?? '';
}

export function setApiKey(key: string): void {
  if (key.length === 0) sessionStorage.removeItem(KEY_STORAGE);
  else sessionStorage.setItem(KEY_STORAGE, key);
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const key = getApiKey();
  const headers = new Headers(init.headers);
  if (key.length > 0) headers.set('authorization', `Bearer ${key}`);
  if (init.body !== undefined) headers.set('content-type', 'application/json');

  const res = await fetch(path, { ...init, headers });
  const text = await res.text();
  const parsed: unknown = text.length > 0 ? safeJson(text) : null;

  if (!res.ok) {
    const err = (parsed as { error?: { code?: string; message?: string } } | null)?.error;
    throw new ApiError(
      res.status,
      err?.code ?? 'UNKNOWN',
      err?.message ?? `Request failed with status ${res.status}`,
    );
  }
  return parsed as T;
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

export type JobState = 'available' | 'running' | 'succeeded' | 'dead' | 'cancelled';

export interface QueueSummary {
  queue: string;
  counts: Partial<Record<JobState, number>>;
  ready: number;
  oldestWaitSeconds: number;
}

export interface JobErrorRecord {
  attempt: number;
  at: string;
  kind: string;
  name?: string;
  message?: string;
  stack?: string;
  worker_id?: string | null;
}

export interface Job {
  id: string;
  queue: string;
  type: string;
  state: JobState;
  payload: unknown;
  metadata: Record<string, unknown> | null;
  priority: number;
  attempt: number;
  maxAttempts: number;
  leaseSeconds: number;
  runAt: string;
  enqueuedAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  leaseExpiresAt: string | null;
  cancelRequestedAt: string | null;
  workerId: string | null;
  idempotencyKey: string | null;
  uniqueKey: string | null;
  deadReason: string | null;
  errors: JobErrorRecord[];
  scheduleId: string | null;
}

export interface Page<T> {
  items: T[];
  nextCursor: string | null;
}

export interface Schedule {
  id: string;
  name: string;
  queue: string;
  type: string;
  cron: string;
  timezone: string;
  catchupPolicy: string;
  enabled: boolean;
  nextRunAt: string;
  lastRunAt: string | null;
  lastJobId: string | null;
}

export const api = {
  queues: () => request<{ queues: QueueSummary[] }>('/v1/queues'),

  jobs: (queue: string, params: Record<string, string | undefined>) => {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== '') qs.set(k, v);
    return request<Page<Job>>(`/v1/queues/${encodeURIComponent(queue)}/jobs?${qs.toString()}`);
  },

  job: (id: string) => request<Job>(`/v1/jobs/${encodeURIComponent(id)}`),

  dlq: (params: Record<string, string | undefined>) => {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== '') qs.set(k, v);
    return request<Page<Job>>(`/v1/dlq?${qs.toString()}`);
  },

  cancel: (id: string) => request(`/v1/jobs/${encodeURIComponent(id)}/cancel`, { method: 'POST' }),
  retry: (id: string) =>
    request<Job>(`/v1/jobs/${encodeURIComponent(id)}/retry`, { method: 'POST' }),
  replay: (id: string) =>
    request<Job>(`/v1/jobs/${encodeURIComponent(id)}/replay`, { method: 'POST' }),

  replayMany: (ids: string[]) =>
    request<{ replayed: string[]; skipped: string[] }>('/v1/dlq/replay', {
      method: 'POST',
      body: JSON.stringify({ ids }),
    }),

  pause: (queue: string) =>
    request(`/v1/queues/${encodeURIComponent(queue)}/pause`, { method: 'POST' }),
  resume: (queue: string) =>
    request(`/v1/queues/${encodeURIComponent(queue)}/resume`, { method: 'POST' }),

  schedules: () => request<{ items: Schedule[] }>('/v1/schedules'),
  pauseSchedule: (id: string) =>
    request<Schedule>(`/v1/schedules/${encodeURIComponent(id)}/pause`, { method: 'POST' }),
  resumeSchedule: (id: string) =>
    request<Schedule>(`/v1/schedules/${encodeURIComponent(id)}/resume`, { method: 'POST' }),
};

export interface Snapshot {
  type: 'snapshot';
  at: string;
  queues: { queue: string; counts: Record<string, number>; oldestWaitSeconds: number }[];
}

/**
 * Subscribes to the SSE stream.
 *
 * Uses fetch rather than EventSource because EventSource cannot set an Authorization
 * header, and the stream requires one. The tradeoff is implementing reconnection
 * manually, which the caller drives.
 */
export function subscribeToEvents(
  onSnapshot: (s: Snapshot) => void,
  onStatus: (connected: boolean) => void,
): () => void {
  const controller = new AbortController();
  let stopped = false;

  const connect = async (): Promise<void> => {
    while (!stopped) {
      try {
        const res = await fetch('/v1/events', {
          headers: { authorization: `Bearer ${getApiKey()}` },
          signal: controller.signal,
        });
        if (!res.ok || res.body === null) throw new Error(`stream failed: ${res.status}`);

        onStatus(true);
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });

          // SSE frames are separated by a blank line. Keep the trailing partial
          // frame in the buffer rather than parsing it half-formed.
          const frames = buffer.split('\n\n');
          buffer = frames.pop() ?? '';

          for (const frame of frames) {
            const dataLine = frame.split('\n').find((l) => l.startsWith('data: '));
            if (dataLine === undefined) continue;
            try {
              const parsed = JSON.parse(dataLine.slice(6)) as Snapshot;
              if (parsed.type === 'snapshot') onSnapshot(parsed);
            } catch {
              // A malformed frame must not kill the stream.
            }
          }
        }
      } catch {
        if (stopped) return;
      }
      onStatus(false);
      await new Promise((r) => setTimeout(r, 3000));
    }
  };

  void connect();

  return () => {
    stopped = true;
    controller.abort();
  };
}
