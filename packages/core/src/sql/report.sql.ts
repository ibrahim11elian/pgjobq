/**
 * Statements that report a delivery's outcome: complete, fail, heartbeat.
 *
 * THE OWNERSHIP FENCE
 * -------------------
 * Every statement here carries the same predicate:
 *
 *   WHERE id = $1 AND state = 'running' AND worker_id = $2 AND attempt = $3
 *
 * Zero rows affected means the worker no longer owns the job — its lease expired
 * and the reaper recovered it, or an operator intervened. The worker must discard
 * its result. This is a NORMAL outcome under partition, not an error to retry.
 *
 * Without the fence, this sequence corrupts state: worker A claims job 1 on
 * attempt 1 and stalls; the lease expires; the reaper returns the job to
 * `available`; worker B claims it on attempt 2 and starts running; worker A wakes
 * and writes `succeeded`. The job is now marked done while B is still executing it,
 * and B's own completion will find zero rows.
 *
 * Both `worker_id` AND `attempt` are required. A worker_id-only fence would be
 * passed by a worker that reclaimed the same job on a later attempt.
 */
import { sql, type SafeStatement } from '../db.js';
import type { JobErrorRecord } from '../types.js';

export interface FenceParams {
  readonly id: string;
  readonly workerId: string;
  readonly attempt: number;
}

/** Marks a job succeeded. Returns zero rows if ownership was lost. */
export function completeJob(p: FenceParams): SafeStatement {
  return sql(
    `
    UPDATE job
       SET state            = 'succeeded',
           finished_at      = now(),
           lease_expires_at = NULL,
           worker_id        = NULL,
           updated_at       = now()
     WHERE id = $1
       AND state = 'running'
       AND worker_id = $2
       AND attempt = $3
    RETURNING id
    `,
    [p.id, p.workerId, p.attempt],
  );
}

export interface FailParams extends FenceParams {
  /** Skip remaining attempts and dead-letter immediately. */
  readonly nonRetryable: boolean;
  /** Jittered delay in seconds, computed by the pure backoff function. */
  readonly retryDelaySeconds: number;
  readonly maxErrorHistory: number;
  readonly error: JobErrorRecord;
}

/**
 * Records a failure and decides retry vs. dead-letter IN THE STATEMENT.
 *
 * The decision compares `attempt >= max_attempts` against the row's own committed
 * values. Computing it in application code from a previously read row would race
 * the reaper: the row could have been recovered and re-claimed between the read and
 * the write, and the application would then apply a decision based on stale state.
 *
 * The `errors` array is trimmed in the same expression that appends to it, giving a
 * bounded failure history with no second statement and no cleanup job. `errors - 0`
 * removes the oldest element (jsonb minus integer deletes by index).
 */
export function failJob(p: FailParams): SafeStatement {
  return sql(
    `
    UPDATE job
       SET state = CASE
                     WHEN $4::boolean               THEN 'dead'::job_state
                     WHEN attempt >= max_attempts   THEN 'dead'::job_state
                     ELSE 'available'::job_state
                   END,
           dead_reason = CASE
                     WHEN $4::boolean               THEN 'non_retryable_error'::dead_reason
                     WHEN attempt >= max_attempts   THEN 'attempts_exhausted'::dead_reason
                     ELSE NULL
                   END,
           run_at = CASE
                     WHEN $4::boolean OR attempt >= max_attempts THEN run_at
                     ELSE now() + make_interval(secs => $5::double precision)
                   END,
           finished_at = CASE
                     WHEN $4::boolean OR attempt >= max_attempts THEN now()
                     ELSE NULL
                   END,
           errors = (
                     CASE
                       WHEN jsonb_array_length(errors) >= $6::int THEN errors - 0
                       ELSE errors
                     END
                   ) || jsonb_build_array($7::jsonb),
           lease_expires_at = NULL,
           worker_id        = NULL,
           updated_at       = now()
     WHERE id = $1
       AND state = 'running'
       AND worker_id = $2
       AND attempt = $3
    RETURNING id, state, run_at, attempt, max_attempts, dead_reason
    `,
    [
      p.id,
      p.workerId,
      p.attempt,
      p.nonRetryable,
      p.retryDelaySeconds,
      p.maxErrorHistory,
      JSON.stringify(p.error),
    ],
  );
}

export interface FailResultRow {
  id: string;
  state: 'available' | 'dead';
  run_at: Date;
  attempt: number;
  max_attempts: number;
  dead_reason: string | null;
}

/**
 * Extends a lease for the rightful owner.
 *
 * Returns `cancel_requested_at`, which is how an operator's cancellation reaches a
 * running handler: the worker sees it on the next heartbeat and fires the handler's
 * AbortSignal. Reusing the heartbeat as the cancellation channel avoids a second
 * polling loop for something that is already round-tripping.
 */
export function heartbeatJob(p: FenceParams & { leaseSeconds: number }): SafeStatement {
  return sql(
    `
    UPDATE job
       SET lease_expires_at = now() + make_interval(secs => $4::int),
           updated_at       = now()
     WHERE id = $1
       AND state = 'running'
       AND worker_id = $2
       AND attempt = $3
    RETURNING lease_expires_at, cancel_requested_at
    `,
    [p.id, p.workerId, p.attempt, p.leaseSeconds],
  );
}

export interface HeartbeatResultRow {
  lease_expires_at: Date;
  cancel_requested_at: Date | null;
}

/** The worker acknowledges an operator cancellation. Fenced like every other report. */
export function acknowledgeCancellation(p: FenceParams): SafeStatement {
  return sql(
    `
    UPDATE job
       SET state            = 'cancelled',
           dead_reason      = NULL,
           finished_at      = now(),
           lease_expires_at = NULL,
           worker_id        = NULL,
           updated_at       = now()
     WHERE id = $1
       AND state = 'running'
       AND worker_id = $2
       AND attempt = $3
       AND cancel_requested_at IS NOT NULL
    RETURNING id
    `,
    [p.id, p.workerId, p.attempt],
  );
}

/**
 * Releases a job on graceful shutdown, making it immediately claimable.
 *
 * Without this, every job in flight during a restart stalls for up to
 * lease_seconds before another worker can pick it up — which is what makes an
 * otherwise-invisible rolling deploy look like a latency spike.
 *
 * Does not consume an attempt beyond the one already spent at claim, and does not
 * apply backoff: the job never actually failed.
 */
export function releaseJob(p: FenceParams): SafeStatement {
  return sql(
    `
    UPDATE job
       SET state            = 'available',
           run_at           = now(),
           lease_expires_at = NULL,
           worker_id        = NULL,
           started_at       = NULL,
           updated_at       = now()
     WHERE id = $1
       AND state = 'running'
       AND worker_id = $2
       AND attempt = $3
    RETURNING id
    `,
    [p.id, p.workerId, p.attempt],
  );
}
