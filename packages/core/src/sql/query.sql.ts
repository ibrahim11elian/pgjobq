/**
 * Read and administrative statements.
 *
 * Every listing paginates by KEYSET cursor, never OFFSET. Deep offsets degrade into
 * a scan of everything skipped, so a dashboard on page 500 of a million-row table
 * would get slower the further it went. A keyset cursor is O(log n) at any depth and
 * is stable while rows are inserted during traversal.
 */
import { sql, type SafeStatement } from '../db.js';
import { JOB_COLUMNS } from './rows.js';
import type { DeadReason, JobState } from '../types.js';

export function getJob(id: string): SafeStatement {
  return sql(`SELECT ${JOB_COLUMNS} FROM job WHERE id = $1`, [id]);
}

export interface ListJobsParams {
  readonly queue?: string | undefined;
  readonly state?: JobState | undefined;
  readonly type?: string | undefined;
  readonly from?: Date | undefined;
  readonly to?: Date | undefined;
  /** Keyset cursor: return rows with id strictly less than this. */
  readonly beforeId?: string | undefined;
  readonly limit: number;
}

/**
 * Lists jobs newest-first.
 *
 * Filters are expressed as `($n IS NULL OR col = $n)` so one statement text serves
 * every filter combination — Postgres can reuse the plan, and there is no string
 * building to get wrong.
 */
export function listJobs(p: ListJobsParams): SafeStatement {
  return sql(
    `
    SELECT ${JOB_COLUMNS}
      FROM job
     WHERE ($1::text      IS NULL OR queue = $1)
       AND ($2::job_state IS NULL OR state = $2)
       AND ($3::text      IS NULL OR type  = $3)
       AND ($4::timestamptz IS NULL OR enqueued_at >= $4)
       AND ($5::timestamptz IS NULL OR enqueued_at <= $5)
       AND ($6::bigint    IS NULL OR id < $6)
     ORDER BY id DESC
     LIMIT $7
    `,
    [
      p.queue ?? null,
      p.state ?? null,
      p.type ?? null,
      p.from ?? null,
      p.to ?? null,
      p.beforeId ?? null,
      p.limit,
    ],
  );
}

export interface ListDeadParams {
  readonly queue?: string | undefined;
  readonly type?: string | undefined;
  readonly reason?: DeadReason | undefined;
  readonly from?: Date | undefined;
  readonly to?: Date | undefined;
  readonly beforeId?: string | undefined;
  readonly limit: number;
}

export function listDeadJobs(p: ListDeadParams): SafeStatement {
  return sql(
    `
    SELECT ${JOB_COLUMNS}
      FROM job
     WHERE state = 'dead'
       AND ($1::text        IS NULL OR queue = $1)
       AND ($2::text        IS NULL OR type  = $2)
       AND ($3::dead_reason IS NULL OR dead_reason = $3)
       AND ($4::timestamptz IS NULL OR finished_at >= $4)
       AND ($5::timestamptz IS NULL OR finished_at <= $5)
       AND ($6::bigint      IS NULL OR id < $6)
     ORDER BY id DESC
     LIMIT $7
    `,
    [
      p.queue ?? null,
      p.type ?? null,
      p.reason ?? null,
      p.from ?? null,
      p.to ?? null,
      p.beforeId ?? null,
      p.limit,
    ],
  );
}

/**
 * Per-queue counts by state.
 *
 * Grouped aggregate over the whole table rather than one COUNT(*) per state. Still
 * a scan of the job table, which is acceptable ONLY because retention keeps that
 * table proportional to the backlog. The dashboard reads this on an interval into a
 * gauge rather than on every render.
 */
export function queueStats(): SafeStatement {
  return sql(
    `
    SELECT queue, state, count(*)::bigint AS count
      FROM job
     GROUP BY queue, state
     ORDER BY queue, state
    `,
  );
}

export interface QueueStatsRow {
  queue: string;
  state: JobState;
  count: string;
}

/** Oldest eligible job per queue: the direct measure of whether the backlog is draining. */
export function queueLag(): SafeStatement {
  return sql(
    `
    SELECT queue,
           COALESCE(EXTRACT(EPOCH FROM (now() - min(run_at))), 0)::double precision AS oldest_wait_seconds,
           count(*)::bigint AS ready
      FROM job
     WHERE state = 'available' AND run_at <= now()
     GROUP BY queue
    `,
  );
}

export interface QueueLagRow {
  queue: string;
  oldest_wait_seconds: number;
  ready: string;
}

// ---------------------------------------------------------------------------
// Administrative mutations. Each is guarded so it can only act from a state the
// machine permits, and each is idempotent or safely repeatable.
// ---------------------------------------------------------------------------

/**
 * Cancels a job that has not started.
 *
 * Restricted to `state = 'available'` in the WHERE clause, so a job that started
 * between the operator's read and this write is not cancelled behind a running
 * worker's back. Zero rows means the caller should re-read and use requestCancel.
 */
export function cancelAvailableJob(id: string): SafeStatement {
  return sql(
    `
    UPDATE job
       SET state       = 'cancelled',
           finished_at = now(),
           updated_at  = now()
     WHERE id = $1 AND state = 'available'
    RETURNING id, state
    `,
    [id],
  );
}

/**
 * Requests cancellation of a RUNNING job.
 *
 * Does not change state. The worker sees cancel_requested_at on its next heartbeat,
 * aborts the handler, and acknowledges. Marking it cancelled here would report a
 * state the executing process has not agreed to, when its side effects may already
 * have happened.
 */
export function requestCancelRunningJob(id: string): SafeStatement {
  return sql(
    `
    UPDATE job
       SET cancel_requested_at = COALESCE(cancel_requested_at, now()),
           updated_at          = now()
     WHERE id = $1 AND state = 'running'
    RETURNING id, state, cancel_requested_at
    `,
    [id],
  );
}

/** Moves run_at to now. Does NOT consume an attempt: the operator is not a delivery. */
export function retryJobNow(id: string): SafeStatement {
  return sql(
    `
    UPDATE job
       SET run_at     = now(),
           updated_at = now()
     WHERE id = $1 AND state = 'available'
    RETURNING id, state, run_at
    `,
    [id],
  );
}

/**
 * Replays a dead-lettered job.
 *
 * Resets attempt to zero and clears the dead reason, but preserves the error
 * history — the whole point of inspecting a replay later is seeing why it died the
 * first time.
 */
export function replayDeadJob(id: string): SafeStatement {
  return sql(
    `
    UPDATE job
       SET state       = 'available',
           attempt     = 0,
           dead_reason = NULL,
           run_at      = now(),
           finished_at = NULL,
           started_at  = NULL,
           updated_at  = now()
     WHERE id = $1 AND state = 'dead'
    RETURNING id, state, attempt
    `,
    [id],
  );
}

/** Bulk replay, bounded and reporting per-job outcome via RETURNING. */
export function replayDeadJobs(ids: readonly string[]): SafeStatement {
  return sql(
    `
    UPDATE job
       SET state       = 'available',
           attempt     = 0,
           dead_reason = NULL,
           run_at      = now(),
           finished_at = NULL,
           started_at  = NULL,
           updated_at  = now()
     WHERE id = ANY($1::bigint[]) AND state = 'dead'
    RETURNING id
    `,
    [ids],
  );
}

/** Upserts queue config. Used by pause/resume and by configuration management. */
export function setQueuePaused(queue: string, paused: boolean): SafeStatement {
  return sql(
    `
    INSERT INTO queue_config (queue, paused)
    VALUES ($1, $2)
    ON CONFLICT (queue) DO UPDATE
       SET paused = EXCLUDED.paused, updated_at = now()
    RETURNING queue, paused
    `,
    [queue, paused],
  );
}

export function getQueueConfig(queue: string): SafeStatement {
  return sql(
    `
    SELECT queue, paused, max_attempts, lease_seconds, priority_aging,
           aging_threshold_s, concurrency_limit, retain_succeeded::text, retain_dead::text
      FROM queue_config WHERE queue = $1
    `,
    [queue],
  );
}

/**
 * Purges jobs in the given states from a queue, in bounded batches.
 *
 * States are passed as a typed array parameter rather than interpolated, so an
 * arbitrary string can never reach the statement.
 */
export function purgeQueue(
  queue: string,
  states: readonly JobState[],
  limit: number,
): SafeStatement {
  return sql(
    `
    WITH victim AS (
      SELECT id FROM job
       WHERE queue = $1 AND state = ANY($2::job_state[])
       ORDER BY id
       LIMIT $3
       FOR UPDATE SKIP LOCKED
    )
    DELETE FROM job WHERE id IN (SELECT id FROM victim)
    RETURNING id
    `,
    [queue, states, limit],
  );
}

export function insertAuditLog(p: {
  actor: string;
  action: string;
  target: string | null;
  detail: unknown;
}): SafeStatement {
  return sql(
    `INSERT INTO audit_log (actor, action, target, detail) VALUES ($1, $2, $3, $4::jsonb) RETURNING id`,
    [p.actor, p.action, p.target, p.detail === undefined ? null : JSON.stringify(p.detail)],
  );
}

export function listDistinctQueues(): SafeStatement {
  return sql(
    `
    SELECT queue FROM (
      SELECT queue FROM job
      UNION
      SELECT queue FROM queue_config
    ) q ORDER BY queue
    `,
  );
}
