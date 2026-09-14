/**
 * Retention and archival loop.
 *
 * The job table's size must track BACKLOG, not history. Without this loop the claim
 * index grows without bound and the queue degrades as cumulative volume rises —
 * slowly enough that it looks like an unrelated problem months later.
 */
import { sleep, type Db } from '../db.js';
import type { Metrics } from '../observability/metrics.js';
import {
  archiveTerminalJobs,
  createArchivePartition,
  jobTableHealth,
  partitionName,
  pruneTerminalJobs,
  type TableHealthRow,
} from '../sql/retention.sql.js';
import type { Logger } from '../types.js';

export interface RetentionOptions {
  readonly db: Db;
  readonly intervalMs: number;
  readonly batchMax: number;
  /** Copy to job_archive before deleting, rather than deleting outright. */
  readonly archive: boolean;
  readonly defaultRetainSucceeded: string;
  readonly defaultRetainDead: string;
  readonly logger: Logger;
  readonly metrics: Metrics;
}

export interface RetentionResult {
  readonly pruned: number;
  readonly byQueue: Record<string, number>;
}

export class Retention {
  private running = false;
  private loop: Promise<void> | undefined;

  constructor(private readonly opts: RetentionOptions) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
    this.opts.logger.info(
      {
        intervalMs: this.opts.intervalMs,
        archive: this.opts.archive,
        retainSucceeded: this.opts.defaultRetainSucceeded,
        retainDead: this.opts.defaultRetainDead,
      },
      'retention started',
    );
  }

  private async run(): Promise<void> {
    while (this.running) {
      try {
        if (this.opts.archive) await this.ensurePartitions();

        let total = 0;
        // Bounded passes, each its own short transaction. A single unbounded DELETE
        // would be the long-running statement that holds back the vacuum horizon —
        // creating the very bloat this loop exists to prevent.
        for (;;) {
          if (!this.running) break;
          const result = await this.pruneOnce();
          total += result.pruned;
          if (result.pruned < this.opts.batchMax) break;
        }
        if (total > 0) this.opts.logger.info({ pruned: total }, 'retention pass complete');

        await this.reportHealth();
      } catch (e) {
        this.opts.logger.error(
          { err: e instanceof Error ? { name: e.name, message: e.message } : String(e) },
          'retention error',
        );
      }
      const base = this.opts.intervalMs;
      await sleep(base / 2 + Math.random() * (base / 2));
    }
  }

  /** One bounded pass. */
  async pruneOnce(): Promise<RetentionResult> {
    const statement = this.opts.archive
      ? archiveTerminalJobs({
          limit: this.opts.batchMax,
          defaultRetainSucceeded: this.opts.defaultRetainSucceeded,
          defaultRetainDead: this.opts.defaultRetainDead,
        })
      : pruneTerminalJobs({
          limit: this.opts.batchMax,
          defaultRetainSucceeded: this.opts.defaultRetainSucceeded,
          defaultRetainDead: this.opts.defaultRetainDead,
        });

    const res = await this.opts.db.query<{ id: string; queue: string; state: string }>(statement);

    const byQueue: Record<string, number> = {};
    for (const row of res.rows) {
      byQueue[row.queue] = (byQueue[row.queue] ?? 0) + 1;
      this.opts.metrics.retentionPruned.inc({ queue: row.queue });
    }
    return { pruned: res.rowCount, byQueue };
  }

  /**
   * Creates the archive partitions for the current and next month.
   *
   * Next month is created ahead of time so an archival run at 23:59 on the last day
   * of a month does not fail for want of a partition — a failure that would only ever
   * surface once a month, at the worst possible moment to debug it.
   */
  async ensurePartitions(): Promise<void> {
    const now = new Date();
    for (const offset of [0, 1]) {
      const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + offset, 1));
      const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + offset + 1, 1));
      const name = partitionName(start.getUTCFullYear(), start.getUTCMonth() + 1);
      await this.opts.db.query(createArchivePartition(name, isoDate(start), isoDate(end)));
    }
  }

  /**
   * Publishes database-side health for this workload.
   *
   * Dead tuples and last-vacuum age are first-class signals here, not something to go
   * hunting for during an incident: vacuum falling behind is the documented failure
   * mode of using Postgres as a queue.
   */
  private async reportHealth(): Promise<void> {
    const res = await this.opts.db.query<TableHealthRow>(jobTableHealth());
    for (const row of res.rows) {
      const t = { table: row.table_name };
      this.opts.metrics.dbDeadTuples.set(t, Number(row.dead_tuples));
      this.opts.metrics.dbLastVacuumAge.set(t, Number(row.last_vacuum_age_seconds));
      this.opts.metrics.dbTableBytes.set(t, Number(row.total_bytes));
      this.opts.metrics.dbIndexBytes.set(t, Number(row.index_bytes));
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.loop) {
      await this.loop.catch(() => undefined);
      this.loop = undefined;
    }
  }
}

function isoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}
