/**
 * The claim statement. The centre of the system.
 *
 * ONE statement, not two. The obvious implementation is
 *   BEGIN; SELECT ... FOR UPDATE SKIP LOCKED; UPDATE ...; COMMIT;
 * which is four round trips with the row lock held across all of them. Folding it
 * into a single UPDATE ... FROM (CTE with FOR UPDATE SKIP LOCKED) collapses that
 * to one round trip in an implicit transaction. There is then no window in which a
 * row is locked but not yet marked `running`, and no possibility of a worker dying
 * mid-claim while holding locks.
 */
import { sql, type SafeStatement } from '../db.js';

export interface ClaimParams {
  readonly queue: string;
  readonly limit: number;
  readonly workerId: string;
}

/**
 * Claims up to `limit` eligible jobs.
 *
 * Notes on clauses that are not self-evident:
 *
 * `FOR UPDATE OF j` — not a bare `FOR UPDATE`. Two reasons, one of them fatal.
 *   Performance: a bare FOR UPDATE would also lock the joined queue_config row,
 *   serializing every claim in the queue against that single row — precisely the
 *   contention this design exists to avoid.
 *   Correctness: Postgres rejects row locking on the nullable side of an outer
 *   join, so a bare FOR UPDATE with this LEFT JOIN is an error, not just slow.
 *
 * `SKIP LOCKED` — a worker that finds a row locked moves on instead of waiting.
 *   This is what lets N workers scale. The cost is that ordering is approximate:
 *   a contended earlier job may be passed over for a later one. Strict global FIFO
 *   and lock-free concurrent consumption are mutually exclusive; this design picks
 *   concurrency and documents the trade.
 *
 * `ORDER BY` — mirrors job_claim_idx (queue, priority DESC, run_at, id)
 *   WHERE state = 'available' exactly, so the plan is an index scan with no sort
 *   node. The plan shape is a tested property, because a silently unused index is
 *   the likeliest future performance regression.
 *
 * `attempt = attempt + 1` — incremented at CLAIM time, not at failure time. If it
 *   were incremented on failure, a worker that dies without reporting would consume
 *   no attempt, and a job that reliably kills its worker (an OOM payload) would
 *   retry forever, taking down worker after worker. The cost is that an unrelated
 *   crash still burns an attempt; that is the right trade, and it is why
 *   max_attempts defaults to 5 rather than 2.
 *
 * `now()` — transaction-start time from the database clock. Never a client clock,
 *   so skew between application instances cannot affect eligibility.
 */
export function claimJobs(p: ClaimParams): SafeStatement {
  return sql(
    `
    WITH candidate AS (
      SELECT j.id
        FROM job j
        LEFT JOIN queue_config qc ON qc.queue = j.queue
       WHERE j.queue = $1
         AND j.state = 'available'
         AND j.run_at <= now()
         AND COALESCE(qc.paused, false) = false
       ORDER BY j.priority DESC, j.run_at, j.id
       LIMIT $2
       FOR UPDATE OF j SKIP LOCKED
    )
    UPDATE job j
       SET state            = 'running',
           attempt          = j.attempt + 1,
           worker_id        = $3,
           started_at       = now(),
           lease_expires_at = now() + make_interval(secs => j.lease_seconds),
           updated_at       = now()
      FROM candidate c
     WHERE j.id = c.id
    RETURNING j.id, j.queue, j.type, j.payload, j.metadata, j.attempt,
              j.max_attempts, j.lease_seconds, j.lease_expires_at,
              j.trace_context, j.enqueued_at
    `,
    [p.queue, p.limit, p.workerId],
  );
}

/**
 * Claim variant with priority aging, used only for queues that enable it.
 *
 * Raises a job's effective order by its waiting time so a low-priority job cannot
 * be starved indefinitely by a continuous supply of higher-priority work.
 *
 * Deliberately a separate statement rather than a branch in the main one: the
 * ORDER BY here is an expression, so it CANNOT be satisfied by job_claim_idx and
 * requires a sort. That is a real cost, paid only by queues that opt in. Shipping
 * it as the default would slow every queue to fix a problem most do not have.
 */
export function claimJobsWithAging(
  p: ClaimParams & { agingThresholdSeconds: number },
): SafeStatement {
  return sql(
    `
    WITH candidate AS (
      SELECT j.id
        FROM job j
        LEFT JOIN queue_config qc ON qc.queue = j.queue
       WHERE j.queue = $1
         AND j.state = 'available'
         AND j.run_at <= now()
         AND COALESCE(qc.paused, false) = false
       ORDER BY
         -- One effective priority point per aging threshold elapsed since the job
         -- became eligible.
         (j.priority + FLOOR(EXTRACT(EPOCH FROM (now() - j.run_at)) / $4)) DESC,
         j.run_at,
         j.id
       LIMIT $2
       FOR UPDATE OF j SKIP LOCKED
    )
    UPDATE job j
       SET state            = 'running',
           attempt          = j.attempt + 1,
           worker_id        = $3,
           started_at       = now(),
           lease_expires_at = now() + make_interval(secs => j.lease_seconds),
           updated_at       = now()
      FROM candidate c
     WHERE j.id = c.id
    RETURNING j.id, j.queue, j.type, j.payload, j.metadata, j.attempt,
              j.max_attempts, j.lease_seconds, j.lease_expires_at,
              j.trace_context, j.enqueued_at
    `,
    [p.queue, p.limit, p.workerId, p.agingThresholdSeconds],
  );
}

/**
 * EXPLAIN for the claim, so the query plan can be asserted as a property.
 */
export function explainClaim(p: ClaimParams): SafeStatement {
  const base = claimJobs(p);
  return sql(`EXPLAIN (FORMAT JSON) ${base.text}`, base.values);
}
