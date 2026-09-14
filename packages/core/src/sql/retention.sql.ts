/**
 * Retention and archival.
 *
 * The job table's size must track BACKLOG, not history, or the claim path degrades
 * as total volume grows. Every deletion is batched with a bounded row count so that
 * pruning never becomes one long transaction holding back the vacuum horizon.
 *
 * Both statements below use `FOR UPDATE OF j`, not a bare `FOR UPDATE`. They LEFT
 * JOIN queue_config to resolve per-queue windows, and Postgres rejects row locking on
 * the nullable side of an outer join outright: a bare FOR UPDATE fails with
 * "FOR UPDATE cannot be applied to the nullable side of an outer join". Same trap as
 * the claim statement.
 */
import { sql, type SafeStatement } from '../db.js';

/**
 * Deletes terminal jobs past their per-queue retention window.
 *
 * Windows come from queue_config with a fallback for queues that have no row, and
 * differ by outcome: a succeeded job is uninteresting within a day, while a
 * dead-lettered one must survive long enough for someone to notice and act.
 *
 * The `id IN (SELECT ... LIMIT)` shape is deliberate. A bare
 * `DELETE ... WHERE finished_at < ...` would delete an unbounded number of rows in
 * one transaction, which is precisely the long-running statement this design avoids.
 */
export function pruneTerminalJobs(p: {
  limit: number;
  defaultRetainSucceeded: string;
  defaultRetainDead: string;
}): SafeStatement {
  return sql(
    `
    WITH victim AS (
      SELECT j.id
        FROM job j
        LEFT JOIN queue_config qc ON qc.queue = j.queue
       WHERE j.state IN ('succeeded', 'dead', 'cancelled')
         AND j.finished_at IS NOT NULL
         AND j.finished_at < now() - (
               CASE j.state
                 WHEN 'dead' THEN COALESCE(qc.retain_dead,      $2::interval)
                 ELSE             COALESCE(qc.retain_succeeded, $3::interval)
               END
             )
       ORDER BY j.finished_at
       LIMIT $1
       FOR UPDATE OF j SKIP LOCKED
    )
    DELETE FROM job
     WHERE id IN (SELECT id FROM victim)
    RETURNING id, queue, state
    `,
    [p.limit, p.defaultRetainDead, p.defaultRetainSucceeded],
  );
}

/**
 * Copies terminal jobs to the archive, then deletes them, in one transaction.
 *
 * A single statement with a CTE means an interruption loses nothing: either both
 * the insert and the delete commit, or neither does. Copying in one statement and
 * deleting in another would risk archiving rows twice on a retry, or deleting rows
 * that were never archived.
 */
export function archiveTerminalJobs(p: {
  limit: number;
  defaultRetainSucceeded: string;
  defaultRetainDead: string;
}): SafeStatement {
  return sql(
    `
    WITH victim AS (
      SELECT j.id
        FROM job j
        LEFT JOIN queue_config qc ON qc.queue = j.queue
       WHERE j.state IN ('succeeded', 'dead', 'cancelled')
         AND j.finished_at IS NOT NULL
         AND j.finished_at < now() - (
               CASE j.state
                 WHEN 'dead' THEN COALESCE(qc.retain_dead,      $2::interval)
                 ELSE             COALESCE(qc.retain_succeeded, $3::interval)
               END
             )
       ORDER BY j.finished_at
       LIMIT $1
       FOR UPDATE OF j SKIP LOCKED
    ),
    moved AS (
      DELETE FROM job
       WHERE id IN (SELECT id FROM victim)
      RETURNING *
    )
    INSERT INTO job_archive (
      id, queue, type, state, payload, metadata, priority, attempt, max_attempts,
      lease_seconds, run_at, enqueued_at, started_at, finished_at, lease_expires_at,
      cancel_requested_at, worker_id, idempotency_key, unique_key, dead_reason,
      errors, trace_context, schedule_id, updated_at, archived_at
    )
    SELECT id, queue, type, state, payload, metadata, priority, attempt, max_attempts,
           lease_seconds, run_at, enqueued_at, started_at, finished_at, lease_expires_at,
           cancel_requested_at, worker_id, idempotency_key, unique_key, dead_reason,
           errors, trace_context, schedule_id, updated_at, now()
      FROM moved
    RETURNING id, queue, state
    `,
    [p.limit, p.defaultRetainDead, p.defaultRetainSucceeded],
  );
}

/**
 * Creates the archive partition covering a given month, if absent.
 *
 * Month-range partitions exist so that expiring history is DROP TABLE on a
 * partition — instant, and creating no dead tuples — rather than a DELETE of
 * millions of rows that would itself generate the bloat the archive exists to avoid.
 *
 * The partition name and bounds are derived from a date computed in TypeScript and
 * validated to a strict format before interpolation, because partition DDL cannot
 * take bind parameters. See ensurePartitionName.
 */
export function createArchivePartition(name: string, from: string, to: string): SafeStatement {
  assertSafeIdentifier(name);
  assertSafeDate(from);
  assertSafeDate(to);
  // Identifiers and DATE literals in DDL cannot be bind parameters. Both are
  // validated against strict patterns above rather than trusted.
  return sql(
    `CREATE TABLE IF NOT EXISTS ${name} PARTITION OF job_archive
       FOR VALUES FROM ('${from}') TO ('${to}')`,
  );
}

export function listArchivePartitions(): SafeStatement {
  return sql(
    `
    SELECT c.relname AS name
      FROM pg_class c
      JOIN pg_inherits i ON i.inhrelid = c.oid
     WHERE i.inhparent = 'job_archive'::regclass
     ORDER BY c.relname
    `,
  );
}

export function dropArchivePartition(name: string): SafeStatement {
  assertSafeIdentifier(name);
  return sql(`DROP TABLE IF EXISTS ${name}`);
}

/** Partition name for a month, e.g. job_archive_2026_08. */
export function partitionName(year: number, month: number): string {
  return `job_archive_${year}_${String(month).padStart(2, '0')}`;
}

function assertSafeIdentifier(name: string): void {
  if (!/^job_archive_\d{4}_\d{2}$/.test(name)) {
    throw new Error(`Refusing to use unsafe partition identifier: ${name}`);
  }
}

function assertSafeDate(d: string): void {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) {
    throw new Error(`Refusing to use unsafe partition bound: ${d}`);
  }
}

/**
 * Database-side health for THIS workload.
 *
 * The documented failure mode of Postgres-as-a-queue is vacuum falling behind, so
 * dead-tuple count and last-autovacuum age are first-class signals rather than
 * something to go looking for during an incident.
 */
export function jobTableHealth(): SafeStatement {
  return sql(
    `
    SELECT
      relname                                              AS table_name,
      n_live_tup                                           AS live_tuples,
      n_dead_tup                                           AS dead_tuples,
      COALESCE(EXTRACT(EPOCH FROM (now() - GREATEST(last_autovacuum, last_vacuum))), -1)::bigint
                                                           AS last_vacuum_age_seconds,
      pg_total_relation_size(relid)                        AS total_bytes,
      pg_indexes_size(relid)                               AS index_bytes
    FROM pg_stat_user_tables
    WHERE relname IN ('job', 'job_archive', 'schedule')
    `,
  );
}

export interface TableHealthRow {
  table_name: string;
  live_tuples: string;
  dead_tuples: string;
  last_vacuum_age_seconds: string;
  total_bytes: string;
  index_bytes: string;
}
