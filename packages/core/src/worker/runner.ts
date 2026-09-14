/**
 * Executes one claimed job: timeout, abort, heartbeat, outcome reporting.
 *
 * Every path out of here is wrapped so no handler exception can escape to the
 * process level and take down the pool, and the slot is always released.
 */
import { performance } from 'node:perf_hooks';
import type { Db } from '../db.js';
import {
  HandlerTimeoutError,
  JobCancelledError,
  NonRetryableError,
  OwnershipLostError,
  RetryAfterError,
  isNonRetryable,
  normalizeError,
} from '../errors.js';
import { nextDelaySeconds, type BackoffOptions } from '../engine/backoff.js';
import { jobLogger } from '../observability/logger.js';
import type { Metrics } from '../observability/metrics.js';
import {
  acknowledgeCancellation,
  completeJob,
  failJob,
  heartbeatJob,
  releaseJob,
  type FailResultRow,
  type HeartbeatResultRow,
} from '../sql/report.sql.js';
import type { ClaimedJob, JobContext, JobErrorRecord, Logger } from '../types.js';
import type { Registry } from './registry.js';

export interface RunnerOptions {
  readonly db: Db;
  readonly registry: Registry;
  readonly workerId: string;
  readonly logger: Logger;
  readonly metrics: Metrics;
  readonly jobTimeoutMs: number;
  readonly heartbeatFraction: number;
  readonly backoff: BackoffOptions;
  readonly maxErrorHistory: number;
  readonly maxErrorTextBytes: number;
}

export type RunOutcome = 'succeeded' | 'retried' | 'dead' | 'cancelled' | 'ownership_lost';

export class Runner {
  constructor(private readonly opts: RunnerOptions) {}

  /**
   * Runs one job to an outcome. Never throws: every failure mode is recorded and
   * reported, because an exception escaping here would kill the pool.
   */
  async run(job: ClaimedJob): Promise<RunOutcome> {
    const { db, metrics, workerId } = this.opts;
    const typeLabel = metrics.labelType(job.type);
    const log = jobLogger(this.opts.logger, job, workerId);

    // Queue wait: enqueue -> claim. The direct measure of backlog health.
    const waitSeconds = Math.max(0, (Date.now() - job.enqueuedAt.getTime()) / 1000);
    metrics.queueWait.observe({ queue: job.queue, type: typeLabel }, waitSeconds);

    const controller = new AbortController();
    let cancelRequested = false;
    let timedOut = false;

    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort(new HandlerTimeoutError(job.id, this.opts.jobTimeoutMs));
    }, this.opts.jobTimeoutMs);
    // Do not hold the event loop open purely for this timer.
    timeout.unref?.();

    const heartbeatMs = Math.max(
      500,
      Math.floor(job.leaseSeconds * 1000 * this.opts.heartbeatFraction),
    );

    const doHeartbeat = async (): Promise<void> => {
      const res = await db.query<HeartbeatResultRow>(
        heartbeatJob({
          id: job.id,
          workerId,
          attempt: job.attempt,
          leaseSeconds: job.leaseSeconds,
        }),
      );
      const row = res.rows[0];
      if (!row) {
        // Ownership gone. Abort the handler rather than let it keep burning
        // resources on work whose result can no longer be recorded.
        controller.abort(new OwnershipLostError(job.id, workerId, job.attempt));
        throw new OwnershipLostError(job.id, workerId, job.attempt);
      }
      if (row.cancel_requested_at !== null && !cancelRequested) {
        cancelRequested = true;
        log.info('cancellation requested by operator; aborting handler');
        controller.abort(new JobCancelledError(`Job ${job.id} cancelled by operator`));
      }
    };

    const heartbeatTimer = setInterval(() => {
      void doHeartbeat().catch((e: unknown) => {
        if (e instanceof OwnershipLostError) {
          log.warn('lease lost during execution');
        } else {
          log.warn({ err: describe(e) }, 'heartbeat failed');
        }
      });
    }, heartbeatMs);
    heartbeatTimer.unref?.();

    const ctx: JobContext = {
      id: job.id,
      queue: job.queue,
      type: job.type,
      attempt: job.attempt,
      maxAttempts: job.maxAttempts,
      signal: controller.signal,
      logger: log,
      heartbeat: doHeartbeat,
    };

    const started = performance.now();
    try {
      await this.opts.registry.run(job.type, job.payload, ctx);

      const durationSeconds = (performance.now() - started) / 1000;
      metrics.jobDuration.observe({ queue: job.queue, type: typeLabel }, durationSeconds);

      // A cancellation that arrived while the handler happened to finish anyway:
      // acknowledge the cancellation rather than recording success, so the operator's
      // intent is reflected.
      if (cancelRequested) {
        const ack = await db.query<{ id: string }>(
          acknowledgeCancellation({ id: job.id, workerId, attempt: job.attempt }),
        );
        if (ack.rows[0]) {
          metrics.jobsCompleted.inc({ queue: job.queue, type: typeLabel, outcome: 'cancelled' });
          return 'cancelled';
        }
      }

      const res = await db.query<{ id: string }>(
        completeJob({ id: job.id, workerId, attempt: job.attempt }),
      );
      if (!res.rows[0]) {
        // The fence rejected the write. Normal under partition, not a bug.
        metrics.staleCompletions.inc({ queue: job.queue });
        log.warn(
          'completion rejected: ownership was lost during execution, discarding result. ' +
            'The job has been or will be re-run by another worker.',
        );
        return 'ownership_lost';
      }

      metrics.jobsCompleted.inc({ queue: job.queue, type: typeLabel, outcome: 'succeeded' });
      log.debug({ durationSeconds }, 'job succeeded');
      return 'succeeded';
    } catch (e) {
      const durationSeconds = (performance.now() - started) / 1000;
      metrics.jobDuration.observe({ queue: job.queue, type: typeLabel }, durationSeconds);
      return await this.reportFailure(job, e, { timedOut, cancelRequested }, log);
    } finally {
      clearTimeout(timeout);
      clearInterval(heartbeatTimer);
    }
  }

  private async reportFailure(
    job: ClaimedJob,
    e: unknown,
    flags: { timedOut: boolean; cancelRequested: boolean },
    log: Logger,
  ): Promise<RunOutcome> {
    const { db, metrics, workerId } = this.opts;
    const typeLabel = metrics.labelType(job.type);

    // Ownership already lost: there is nothing to write, and attempting to would
    // overwrite state belonging to whichever worker now holds the job.
    if (e instanceof OwnershipLostError) {
      metrics.staleCompletions.inc({ queue: job.queue });
      log.warn('ownership lost during execution; discarding result');
      return 'ownership_lost';
    }

    if (flags.cancelRequested || e instanceof JobCancelledError) {
      const ack = await db.query<{ id: string }>(
        acknowledgeCancellation({ id: job.id, workerId, attempt: job.attempt }),
      );
      if (ack.rows[0]) {
        metrics.jobsCompleted.inc({ queue: job.queue, type: typeLabel, outcome: 'cancelled' });
        log.info('cancellation acknowledged');
        return 'cancelled';
      }
      // The ack was fenced out; fall through and record it as a failure instead.
    }

    const kind = flags.timedOut ? 'timeout' : classify(e);
    const normalized = normalizeError(e, job.attempt, kind, this.opts.maxErrorTextBytes);
    const record: JobErrorRecord = { ...normalized, worker_id: workerId };

    // A payload that fails its schema will not start matching on a retry, and a
    // missing handler will not appear by itself. Both are non-retryable.
    const nonRetryable = isNonRetryable(e) || kind === 'validation_error' || kind === 'no_handler';

    const retryDelaySeconds =
      e instanceof RetryAfterError
        ? Math.min(e.retryDelayMs, this.opts.backoff.maxMs) / 1000
        : nextDelaySeconds(job.attempt, this.opts.backoff);

    const res = await db.query<FailResultRow>(
      failJob({
        id: job.id,
        workerId,
        attempt: job.attempt,
        nonRetryable,
        retryDelaySeconds,
        maxErrorHistory: this.opts.maxErrorHistory,
        error: record,
      }),
    );

    const row = res.rows[0];
    if (!row) {
      metrics.staleCompletions.inc({ queue: job.queue });
      log.warn('failure report rejected: ownership was lost during execution');
      return 'ownership_lost';
    }

    if (row.state === 'dead') {
      metrics.jobsCompleted.inc({ queue: job.queue, type: typeLabel, outcome: 'dead' });
      log.error(
        { err: { name: record.name, message: record.message }, reason: row.dead_reason },
        'job dead-lettered',
      );
      return 'dead';
    }

    metrics.jobsCompleted.inc({ queue: job.queue, type: typeLabel, outcome: 'failed' });
    metrics.retriesScheduled.inc({ queue: job.queue, type: typeLabel });
    log.warn(
      {
        err: { name: record.name, message: record.message },
        attempt: job.attempt,
        maxAttempts: row.max_attempts,
        retryInSeconds: Math.round(retryDelaySeconds),
      },
      'job failed, retry scheduled',
    );
    return 'retried';
  }

  /**
   * Releases a job on shutdown, making it immediately claimable rather than
   * stalling for up to lease_seconds.
   */
  async release(job: ClaimedJob): Promise<boolean> {
    const res = await this.opts.db.query<{ id: string }>(
      releaseJob({ id: job.id, workerId: this.opts.workerId, attempt: job.attempt }),
    );
    return res.rowCount > 0;
  }
}

function classify(e: unknown): 'handler_error' | 'timeout' | 'validation_error' | 'no_handler' {
  if (e instanceof HandlerTimeoutError) return 'timeout';
  if (e instanceof NonRetryableError) return 'handler_error';
  if (isZodError(e)) return 'validation_error';
  if (e !== null && typeof e === 'object' && 'code' in e && e.code === 'NO_HANDLER') {
    return 'no_handler';
  }
  return 'handler_error';
}

function isZodError(e: unknown): boolean {
  return e !== null && typeof e === 'object' && 'issues' in e && Array.isArray(e.issues);
}

function describe(e: unknown): { name: string; message: string } {
  if (e instanceof Error) return { name: e.name, message: e.message };
  return { name: 'UnknownError', message: String(e) };
}
