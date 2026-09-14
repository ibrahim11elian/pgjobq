/**
 * Cron scheduler.
 *
 * Exactly-one-job-per-occurrence is achieved with a DETERMINISTIC idempotency key,
 * not a distributed lock. Two schedulers that both process the same occurrence
 * generate byte-identical keys and the unique index rejects the second insert.
 */
import { Cron } from 'croner';
import { sleep, type Db } from '../db.js';
import { ValidationError } from '../errors.js';
import type { Metrics } from '../observability/metrics.js';
import { insertJob } from '../sql/enqueue.sql.js';
import {
  advanceSchedule,
  claimDueSchedules,
  createSchedule,
  deleteSchedule,
  getSchedule,
  listSchedules,
  occurrenceKey,
  setScheduleEnabled,
  type DueScheduleRow,
} from '../sql/schedule.sql.js';
import { toSchedule, type ScheduleRow } from '../sql/rows.js';
import type { CatchupPolicy, JsonValue, Logger, Schedule } from '../types.js';

export interface SchedulerOptions {
  readonly db: Db;
  readonly intervalMs: number;
  readonly batchMax: number;
  readonly logger: Logger;
  readonly metrics: Metrics;
}

/**
 * Computes the first occurrence strictly after `from`, in the schedule's timezone.
 *
 * Timezone handling is delegated rather than hand-rolled because DST is genuinely
 * fiddly and getting it wrong is silent. Two cases must be right:
 *
 *  - Spring forward: for `0 2 * * *` in Europe/London on the night 01:00 jumps to
 *    02:00, local 02:00 does not exist. The occurrence must still fire, not vanish.
 *  - Fall back: the same night 02:00 occurs twice. It must fire once, not twice.
 *
 * @throws {ValidationError} on an invalid expression or unknown timezone, at
 * creation time rather than at the first tick.
 */
export function nextOccurrence(cron: string, timezone: string, from: Date = new Date()): Date {
  let next: Date | null;
  try {
    const job = new Cron(cron, { timezone, paused: true });
    // nextRun() must be inside the try, not just the constructor. croner validates
    // the timezone lazily via Intl on first evaluation, so an unknown zone throws a
    // RangeError here rather than at construction. Leaving it outside let that escape
    // unwrapped and surface as a 500 instead of a 400.
    next = job.nextRun(from);
    job.stop();
  } catch (e) {
    throw new ValidationError(
      `Invalid cron expression '${cron}' or timezone '${timezone}': ${
        e instanceof Error ? e.message : String(e)
      }`,
      { details: { cron, timezone } },
    );
  }

  if (next === null) {
    throw new ValidationError(
      `Cron expression '${cron}' has no future occurrence in timezone '${timezone}'`,
      { details: { cron, timezone } },
    );
  }
  return next;
}

/** Validates a cron expression and timezone without computing anything. */
export function validateCron(cron: string, timezone: string): void {
  nextOccurrence(cron, timezone, new Date());
}

export interface SchedulerTickResult {
  readonly processed: number;
  readonly created: number;
  readonly deduplicated: number;
}

export class Scheduler {
  private running = false;
  private loop: Promise<void> | undefined;

  constructor(private readonly opts: SchedulerOptions) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
    this.opts.logger.info({ intervalMs: this.opts.intervalMs }, 'scheduler started');
  }

  private async run(): Promise<void> {
    while (this.running) {
      try {
        await this.tick();
      } catch (e) {
        this.opts.logger.error(
          { err: e instanceof Error ? { name: e.name, message: e.message } : String(e) },
          'scheduler error',
        );
      }
      const base = this.opts.intervalMs;
      await sleep(base / 2 + Math.random() * (base / 2));
    }
  }

  /** One tick. Exposed so tests and benchmarks can drive it deterministically. */
  async tick(): Promise<SchedulerTickResult> {
    const due = await this.opts.db.query<DueScheduleRow>(claimDueSchedules(this.opts.batchMax));

    let created = 0;
    let deduplicated = 0;

    for (const row of due.rows) {
      const key = occurrenceKey(row.id, row.planned_for);

      // Advance from now(), NOT from planned_for. This is what implements
      // skip_missed: a scheduler that was down for six hours moves to the next
      // future occurrence on recovery instead of firing six backfilled jobs.
      // Computing from planned_for would produce exactly that backfill storm.
      const now = new Date();
      let nextRunAt: Date;
      try {
        nextRunAt = nextOccurrence(row.cron, row.timezone, now);
      } catch (e) {
        // A schedule whose expression became invalid must not wedge the loop for
        // every other schedule. Push it out an hour and report it.
        this.opts.logger.error(
          {
            scheduleId: row.id,
            name: row.name,
            cron: row.cron,
            timezone: row.timezone,
            err: e instanceof Error ? e.message : String(e),
          },
          'schedule has an invalid cron expression; deferring one hour',
        );
        await this.opts.db.query(
          advanceSchedule({
            id: row.id,
            nextRunAt: new Date(now.getTime() + 3_600_000),
            lastJobId: null,
          }),
        );
        continue;
      }

      const shouldFire = this.shouldFire(row, now);
      let jobId: string | null = null;

      if (shouldFire) {
        const res = await this.opts.db.query<{ id: string }>(
          insertJob({
            queue: row.queue,
            type: row.type,
            payload: row.payload,
            metadata: null,
            priority: 0,
            maxAttempts: 5,
            leaseSeconds: 30,
            runAt: null,
            idempotencyKey: key,
            uniqueKey: null,
            traceContext: null,
            scheduleId: row.id,
          }),
        );
        const inserted = res.rows[0];
        if (inserted) {
          jobId = inserted.id;
          created += 1;
          this.opts.metrics.scheduleMaterialized.inc({ schedule: row.name });
          this.opts.logger.debug(
            { scheduleId: row.id, name: row.name, jobId, plannedFor: row.planned_for },
            'schedule materialized',
          );
        } else {
          // Another scheduler won this occurrence. Entirely expected; the point of
          // the deterministic key is that the loser proceeds normally.
          deduplicated += 1;
          this.opts.logger.debug(
            { scheduleId: row.id, name: row.name, plannedFor: row.planned_for },
            'occurrence already materialized by another scheduler',
          );
        }
      }

      await this.opts.db.query(advanceSchedule({ id: row.id, nextRunAt, lastJobId: jobId }));
    }

    return { processed: due.rows.length, created, deduplicated };
  }

  /**
   * Applies the catch-up policy.
   *
   * `skip_missed` still fires the occurrence currently being processed — it only
   * declines to backfill the ones between. `run_once` behaves identically for a
   * single occurrence; the difference shows in how far behind the schedule was, which
   * the advance-from-now rule already handles. Both policies therefore fire here; the
   * distinction is documented rather than branching, and a future `run_all` policy
   * would be where a real branch belongs.
   */
  private shouldFire(row: DueScheduleRow, now: Date): boolean {
    const lateBySeconds = (now.getTime() - row.planned_for.getTime()) / 1000;

    if (row.catchup_policy === 'skip_missed') {
      // A grossly late occurrence is almost certainly the tail of an outage. Firing
      // it would run a nightly report at lunchtime.
      const staleAfterSeconds = 3600;
      if (lateBySeconds > staleAfterSeconds) {
        this.opts.logger.warn(
          {
            scheduleId: row.id,
            name: row.name,
            lateBySeconds: Math.round(lateBySeconds),
          },
          'occurrence is stale beyond the skip_missed threshold; skipping',
        );
        return false;
      }
    }
    return true;
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.loop) {
      await this.loop.catch(() => undefined);
      this.loop = undefined;
    }
  }

  // -----------------------------------------------------------------------
  // Management
  // -----------------------------------------------------------------------

  async create(p: {
    name: string;
    queue: string;
    type: string;
    payload?: JsonValue;
    cron: string;
    timezone?: string;
    catchupPolicy?: CatchupPolicy;
    enabled?: boolean;
  }): Promise<Schedule> {
    const timezone = p.timezone ?? 'UTC';
    // Validate BEFORE persisting, so an invalid expression never reaches the table
    // and cannot wedge the tick loop.
    const nextRunAt = nextOccurrence(p.cron, timezone, new Date());

    const res = await this.opts.db.query<ScheduleRow>(
      createSchedule({
        name: p.name,
        queue: p.queue,
        type: p.type,
        payload: p.payload ?? {},
        cron: p.cron,
        timezone,
        catchupPolicy: p.catchupPolicy ?? 'skip_missed',
        enabled: p.enabled ?? true,
        nextRunAt,
      }),
    );
    const row = res.rows[0];
    if (!row) throw new ValidationError('Failed to create schedule');
    return toSchedule(row);
  }

  async list(): Promise<Schedule[]> {
    const res = await this.opts.db.query<ScheduleRow>(listSchedules());
    return res.rows.map(toSchedule);
  }

  async get(id: string): Promise<Schedule | null> {
    const res = await this.opts.db.query<ScheduleRow>(getSchedule(id));
    const row = res.rows[0];
    return row ? toSchedule(row) : null;
  }

  /**
   * Pauses or resumes.
   *
   * On resume, next_run_at is recomputed from now, so the schedule advances from the
   * next FUTURE occurrence rather than backfilling everything missed while paused.
   */
  async setEnabled(id: string, enabled: boolean): Promise<Schedule | null> {
    const existing = await this.get(id);
    if (!existing) return null;

    const nextRunAt = enabled ? nextOccurrence(existing.cron, existing.timezone, new Date()) : null;

    const res = await this.opts.db.query<ScheduleRow>(
      setScheduleEnabled({ id, enabled, nextRunAt }),
    );
    const row = res.rows[0];
    return row ? toSchedule(row) : null;
  }

  async delete(id: string): Promise<boolean> {
    const res = await this.opts.db.query<{ id: string }>(deleteSchedule(id));
    return res.rowCount > 0;
  }
}
