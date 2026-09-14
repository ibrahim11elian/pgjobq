/**
 * Lease recovery loop.
 *
 * Safe to run on EVERY application instance with no leader election: the reap
 * statement uses SKIP LOCKED, so concurrent reapers divide the expired rows between
 * them rather than colliding.
 */
import { sleep, type Db } from '../db.js';
import type { Metrics } from '../observability/metrics.js';
import { countOverdueLeases, reapExpiredLeases, type ReapedRow } from '../sql/reap.sql.js';
import type { Logger } from '../types.js';
import { nextDelaySeconds, type BackoffOptions } from './backoff.js';

export interface ReaperOptions {
  readonly db: Db;
  readonly intervalMs: number;
  readonly batchMax: number;
  readonly backoff: BackoffOptions;
  readonly logger: Logger;
  readonly metrics: Metrics;
}

export interface ReapResult {
  readonly recovered: number;
  readonly deadLettered: number;
}

export class Reaper {
  private running = false;
  private loop: Promise<void> | undefined;

  constructor(private readonly opts: ReaperOptions) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
    this.opts.logger.info(
      { intervalMs: this.opts.intervalMs, batchMax: this.opts.batchMax },
      'reaper started',
    );
  }

  private async run(): Promise<void> {
    while (this.running) {
      try {
        // Drain in bounded passes: if a whole host died, there may be thousands of
        // expired leases, and one giant transaction would hold back the vacuum
        // horizon — the exact bloat mechanism this design guards against.
        let total = 0;
        for (;;) {
          if (!this.running) break;
          const result = await this.reapOnce();
          total += result.recovered + result.deadLettered;
          if (result.recovered + result.deadLettered < this.opts.batchMax) break;
        }
        if (total > 0) {
          this.opts.logger.info({ total }, 'reaper pass complete');
        }

        await this.reportOverdue();
      } catch (e) {
        this.opts.logger.error(
          { err: e instanceof Error ? { name: e.name, message: e.message } : String(e) },
          'reaper error',
        );
      }
      // Jittered so multiple instances' reapers do not fire simultaneously.
      const base = this.opts.intervalMs;
      await sleep(base / 2 + Math.random() * (base / 2));
    }
  }

  /** One bounded pass. Exposed for benchmarks and for driving the loop manually. */
  async reapOnce(): Promise<ReapResult> {
    // Jitter is computed per pass rather than per row: all rows recovered in one
    // pass share a delay, but successive passes differ, and the important case is
    // that a mass expiry does not resynchronize into a single burst.
    const delaySeconds = nextDelaySeconds(1, this.opts.backoff);

    const res = await this.opts.db.query<ReapedRow>(
      reapExpiredLeases({ limit: this.opts.batchMax, retryDelaySeconds: delaySeconds }),
    );

    let recovered = 0;
    let deadLettered = 0;
    for (const row of res.rows) {
      this.opts.metrics.leasesExpired.inc({ queue: row.queue });
      if (row.state === 'dead') {
        deadLettered += 1;
        this.opts.logger.error(
          {
            jobId: row.id,
            queue: row.queue,
            jobType: row.type,
            attempt: row.attempt,
            maxAttempts: row.max_attempts,
          },
          'lease expired on final attempt; dead-lettered',
        );
      } else {
        recovered += 1;
        this.opts.logger.warn(
          {
            jobId: row.id,
            queue: row.queue,
            jobType: row.type,
            attempt: row.attempt,
            maxAttempts: row.max_attempts,
          },
          'lease expired; job recovered for retry',
        );
      }
    }

    return { recovered, deadLettered };
  }

  /**
   * Publishes the count of leases that are expired but not yet reaped.
   *
   * A persistently non-zero value means the reaper is not keeping up, which users
   * would experience as jobs mysteriously stuck in `running`.
   */
  private async reportOverdue(): Promise<void> {
    const res = await this.opts.db.query<{ overdue: number }>(countOverdueLeases());
    this.opts.metrics.overdueLeases.set(res.rows[0]?.overdue ?? 0);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.loop) {
      await this.loop.catch(() => undefined);
      this.loop = undefined;
    }
  }
}
