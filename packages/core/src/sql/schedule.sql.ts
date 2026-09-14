/**
 * Scheduler statements.
 *
 * Exactly-one-job-per-occurrence is a DATABASE UNIQUENESS PROPERTY here, not a
 * distributed-locking problem. Each occurrence's job is inserted with a
 * deterministic idempotency key derived from the schedule ID and the planned
 * occurrence time, so two schedulers that both process the same occurrence generate
 * byte-identical keys and the unique index rejects the second insert. No leader to
 * elect, no lock to lose.
 */
import { sql, type SafeStatement } from '../db.js';
import type { CatchupPolicy, JsonValue } from '../types.js';

/**
 * Claims due schedules with SKIP LOCKED, so concurrent schedulers on every instance
 * divide the work rather than colliding.
 *
 * `last_run_at` is set to the occurrence being claimed, and `next_run_at` is left
 * for the caller to advance once it has evaluated the cron expression — cron
 * evaluation needs a timezone library and cannot happen in SQL.
 */
export function claimDueSchedules(limit: number): SafeStatement {
  return sql(
    `
    WITH due AS (
      SELECT id FROM schedule
       WHERE enabled
         AND next_run_at <= now()
       ORDER BY next_run_at
       LIMIT $1
       FOR UPDATE SKIP LOCKED
    )
    UPDATE schedule s
       SET last_run_at = s.next_run_at,
           updated_at  = now()
      FROM due d
     WHERE s.id = d.id
    RETURNING s.id, s.name, s.queue, s.type, s.payload, s.cron, s.timezone,
              s.catchup_policy, s.next_run_at AS planned_for
    `,
    [limit],
  );
}

export interface DueScheduleRow {
  id: string;
  name: string;
  queue: string;
  type: string;
  payload: JsonValue;
  cron: string;
  timezone: string;
  catchup_policy: CatchupPolicy;
  planned_for: Date;
}

/** Advances a schedule to its next computed occurrence and records the job it created. */
export function advanceSchedule(p: {
  id: string;
  nextRunAt: Date;
  lastJobId: string | null;
}): SafeStatement {
  return sql(
    `
    UPDATE schedule
       SET next_run_at = $2,
           last_job_id = COALESCE($3::bigint, last_job_id),
           updated_at  = now()
     WHERE id = $1
    RETURNING id
    `,
    [p.id, p.nextRunAt, p.lastJobId],
  );
}

export function createSchedule(p: {
  name: string;
  queue: string;
  type: string;
  payload: JsonValue;
  cron: string;
  timezone: string;
  catchupPolicy: CatchupPolicy;
  enabled: boolean;
  nextRunAt: Date;
}): SafeStatement {
  return sql(
    `
    INSERT INTO schedule (name, queue, type, payload, cron, timezone, catchup_policy, enabled, next_run_at)
    VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9)
    RETURNING id, name, queue, type, payload, cron, timezone, catchup_policy,
              enabled, next_run_at, last_run_at, last_job_id
    `,
    [
      p.name,
      p.queue,
      p.type,
      JSON.stringify(p.payload),
      p.cron,
      p.timezone,
      p.catchupPolicy,
      p.enabled,
      p.nextRunAt,
    ],
  );
}

export function listSchedules(): SafeStatement {
  return sql(
    `
    SELECT id, name, queue, type, payload, cron, timezone, catchup_policy,
           enabled, next_run_at, last_run_at, last_job_id
      FROM schedule
     ORDER BY name
    `,
  );
}

export function getSchedule(id: string): SafeStatement {
  return sql(
    `
    SELECT id, name, queue, type, payload, cron, timezone, catchup_policy,
           enabled, next_run_at, last_run_at, last_job_id
      FROM schedule WHERE id = $1
    `,
    [id],
  );
}

/**
 * Pauses or resumes a schedule.
 *
 * On resume the caller supplies a freshly computed next_run_at, so the schedule
 * advances from the next FUTURE occurrence rather than backfilling everything
 * missed while paused.
 */
export function setScheduleEnabled(p: {
  id: string;
  enabled: boolean;
  nextRunAt: Date | null;
}): SafeStatement {
  return sql(
    `
    UPDATE schedule
       SET enabled     = $2,
           next_run_at = COALESCE($3::timestamptz, next_run_at),
           updated_at  = now()
     WHERE id = $1
    RETURNING id, name, queue, type, payload, cron, timezone, catchup_policy,
              enabled, next_run_at, last_run_at, last_job_id
    `,
    [p.id, p.enabled, p.nextRunAt],
  );
}

export function deleteSchedule(id: string): SafeStatement {
  return sql(`DELETE FROM schedule WHERE id = $1 RETURNING id`, [id]);
}

/**
 * The deterministic occurrence key.
 *
 * Must be byte-identical across scheduler instances for the same occurrence, so it
 * is built from the schedule ID and the planned time in a fixed ISO-8601 form —
 * never from `now()`, a local format, or anything instance-specific.
 */
export function occurrenceKey(scheduleId: string, plannedFor: Date): string {
  return `sched:${scheduleId}:${plannedFor.toISOString()}`;
}
