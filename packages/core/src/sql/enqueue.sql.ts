/**
 * Enqueue statements.
 *
 * Idempotency is enforced by a unique index with ON CONFLICT DO NOTHING, never by
 * a read-then-write check. Two concurrent enqueues with the same key would both
 * pass a read check and both insert; the index makes that impossible.
 */
import { sql, type SafeStatement } from '../db.js';
import type { JsonObject, JsonValue } from '../types.js';

export interface EnqueueRow {
  readonly queue: string;
  readonly type: string;
  readonly payload: JsonValue;
  readonly metadata: JsonObject | null;
  readonly priority: number;
  readonly maxAttempts: number;
  readonly leaseSeconds: number;
  /** Absolute time. Null means now. Resolved by the caller so the DB sees one shape. */
  readonly runAt: Date | null;
  readonly idempotencyKey: string | null;
  readonly uniqueKey: string | null;
  readonly traceContext: string | null;
  readonly scheduleId: string | null;
}

/**
 * Inserts one job.
 *
 * `run_at` uses COALESCE($8, now()) so the default comes from the DATABASE clock
 * rather than the client's — clock skew between application instances must not
 * affect when a job becomes eligible.
 *
 * ON CONFLICT is scoped to the partial unique index's predicate. Returns no row on
 * conflict; the caller then looks up the existing job and reports deduplicated.
 */
export function insertJob(r: EnqueueRow): SafeStatement {
  return sql(
    `
    INSERT INTO job (
      queue, type, payload, metadata, priority, max_attempts, lease_seconds,
      run_at, idempotency_key, unique_key, trace_context, schedule_id
    ) VALUES (
      $1, $2, $3::jsonb, $4::jsonb, $5, $6, $7,
      COALESCE($8::timestamptz, now()), $9, $10, $11, $12::bigint
    )
    ON CONFLICT DO NOTHING
    RETURNING id
    `,
    [
      r.queue,
      r.type,
      JSON.stringify(r.payload),
      r.metadata === null ? null : JSON.stringify(r.metadata),
      r.priority,
      r.maxAttempts,
      r.leaseSeconds,
      r.runAt,
      r.idempotencyKey,
      r.uniqueKey,
      r.traceContext,
      r.scheduleId,
    ],
  );
}

/**
 * Batch insert in ONE statement and one transaction.
 *
 * Uses unnest over parallel arrays rather than generated multi-row VALUES: the
 * statement text is constant regardless of batch size, so Postgres can reuse the
 * plan and there is no risk of exceeding the bind-parameter limit at 1,000 rows
 * (12 parameters total instead of 12,000).
 */
export function insertJobsBatch(rows: readonly EnqueueRow[]): SafeStatement {
  return sql(
    `
    INSERT INTO job (
      queue, type, payload, metadata, priority, max_attempts, lease_seconds,
      run_at, idempotency_key, unique_key, trace_context, schedule_id
    )
    SELECT
      t.queue, t.type, t.payload::jsonb, t.metadata::jsonb, t.priority,
      t.max_attempts, t.lease_seconds, COALESCE(t.run_at, now()),
      t.idempotency_key, t.unique_key, t.trace_context, t.schedule_id
    FROM unnest(
      $1::text[], $2::text[], $3::text[], $4::text[], $5::smallint[],
      $6::smallint[], $7::integer[], $8::timestamptz[], $9::text[], $10::text[],
      $11::text[], $12::bigint[]
    ) AS t(
      queue, type, payload, metadata, priority, max_attempts, lease_seconds,
      run_at, idempotency_key, unique_key, trace_context, schedule_id
    )
    ON CONFLICT DO NOTHING
    RETURNING id, idempotency_key, unique_key
    `,
    [
      rows.map((r) => r.queue),
      rows.map((r) => r.type),
      rows.map((r) => JSON.stringify(r.payload)),
      rows.map((r) => (r.metadata === null ? null : JSON.stringify(r.metadata))),
      rows.map((r) => r.priority),
      rows.map((r) => r.maxAttempts),
      rows.map((r) => r.leaseSeconds),
      rows.map((r) => r.runAt),
      rows.map((r) => r.idempotencyKey),
      rows.map((r) => r.uniqueKey),
      rows.map((r) => r.traceContext),
      rows.map((r) => r.scheduleId),
    ],
  );
}

/** Looks up the winner after an idempotency conflict, so the loser can return its ID. */
export function findByIdempotencyKey(queue: string, key: string): SafeStatement {
  return sql(`SELECT id FROM job WHERE queue = $1 AND idempotency_key = $2`, [queue, key]);
}

/** Looks up the active job holding a unique_key, for debounce reporting. */
export function findByUniqueKey(queue: string, key: string): SafeStatement {
  return sql(
    `
    SELECT id FROM job
     WHERE queue = $1 AND unique_key = $2 AND state IN ('available', 'running')
    `,
    [queue, key],
  );
}

/**
 * Resolves per-queue defaults in one round trip.
 *
 * Returns a row even when no queue_config exists, so the caller does not need a
 * separate "does this queue have config" query.
 */
export function resolveQueueDefaults(queue: string): SafeStatement {
  return sql(
    `
    SELECT
      COALESCE(paused, false)      AS paused,
      max_attempts,
      lease_seconds,
      COALESCE(priority_aging, false) AS priority_aging,
      aging_threshold_s,
      concurrency_limit
    FROM (SELECT $1::text AS q) req
    LEFT JOIN queue_config qc ON qc.queue = req.q
    `,
    [queue],
  );
}

export interface QueueDefaultsRow {
  paused: boolean;
  max_attempts: number | null;
  lease_seconds: number | null;
  priority_aging: boolean;
  aging_threshold_s: number | null;
  concurrency_limit: number | null;
}
