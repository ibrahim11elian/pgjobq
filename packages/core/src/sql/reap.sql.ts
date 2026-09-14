/**
 * Lease recovery.
 *
 * SKIP LOCKED on the reaper's own scan is what makes it safe to run on every
 * application instance with no leader election: two reapers racing simply divide
 * the expired rows between them.
 */
import { sql, type SafeStatement } from '../db.js';

export interface ReapParams {
  /** Bounds each pass. */
  readonly limit: number;
  /** Jittered delay applied to recovered jobs, in seconds. */
  readonly retryDelaySeconds: number;
}

/**
 * Recovers jobs whose lease expired.
 *
 * The LIMIT is not an optimization, it is a correctness-adjacent requirement:
 * recovering a large backlog must be many short transactions rather than one long
 * one. A long transaction here holds back the vacuum horizon, which is exactly the
 * bloat mechanism this design guards against.
 *
 * Because `attempt` was already incremented at claim time, the dead-letter decision
 * needs no adjustment: a job on its final attempt whose worker vanished goes
 * straight to `dead` with reason `lease_expired`, rather than being handed to
 * another worker that would also be its last.
 *
 * Jitter matters more here than it appears. When a worker HOST dies, every job it
 * held expires at nearly the same moment; un-jittered recovery would return them all
 * to `available` with the same run_at, producing exactly the thundering herd the
 * jitter exists to prevent — on a system already down one host.
 */
export function reapExpiredLeases(p: ReapParams): SafeStatement {
  return sql(
    `
    WITH expired AS (
      SELECT id FROM job
       WHERE state = 'running'
         AND lease_expires_at < now()
       ORDER BY lease_expires_at
       LIMIT $1
       FOR UPDATE SKIP LOCKED
    )
    UPDATE job j
       SET state = CASE
                     WHEN j.attempt >= j.max_attempts THEN 'dead'::job_state
                     ELSE 'available'::job_state
                   END,
           dead_reason = CASE
                     WHEN j.attempt >= j.max_attempts THEN 'lease_expired'::dead_reason
                     ELSE NULL
                   END,
           run_at = CASE
                     WHEN j.attempt >= j.max_attempts THEN j.run_at
                     ELSE now() + make_interval(secs => $2::double precision)
                   END,
           finished_at = CASE
                     WHEN j.attempt >= j.max_attempts THEN now()
                     ELSE NULL
                   END,
           started_at = CASE
                     WHEN j.attempt >= j.max_attempts THEN j.started_at
                     ELSE NULL
                   END,
           errors = (
                     CASE
                       WHEN jsonb_array_length(j.errors) >= 5 THEN j.errors - 0
                       ELSE j.errors
                     END
                   ) || jsonb_build_array(jsonb_build_object(
                          'attempt',   j.attempt,
                          'at',        to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                          'kind',      'lease_expired',
                          'name',      'LeaseExpired',
                          'message',   'Lease expired without the worker reporting an outcome',
                          'worker_id', j.worker_id
                        )),
           lease_expires_at = NULL,
           worker_id        = NULL,
           updated_at       = now()
      FROM expired e
     WHERE j.id = e.id
    RETURNING j.id, j.queue, j.type, j.state, j.attempt, j.max_attempts
    `,
    [p.limit, p.retryDelaySeconds],
  );
}

export interface ReapedRow {
  id: string;
  queue: string;
  type: string;
  state: 'available' | 'dead';
  attempt: number;
  max_attempts: number;
}

/**
 * Counts jobs whose lease has expired but which have not yet been reaped.
 *
 * Exposed as a health signal: a persistently non-zero value means the reaper is
 * not keeping up with expiries, which would show up to users as jobs mysteriously
 * stuck in `running`.
 */
export function countOverdueLeases(): SafeStatement {
  return sql(
    `SELECT count(*)::int AS overdue FROM job WHERE state = 'running' AND lease_expires_at < now()`,
  );
}
