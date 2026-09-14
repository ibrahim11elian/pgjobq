/**
 * The worker pool: claim loop, slot accounting, graceful shutdown.
 */
import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import { performance } from 'node:perf_hooks';
import { sleep, type Db } from '../db.js';
import type { BackoffOptions } from '../engine/backoff.js';
import { getMetrics, type Metrics } from '../observability/metrics.js';
import { nullLogger } from '../observability/logger.js';
import { claimJobs, claimJobsWithAging } from '../sql/claim.sql.js';
import { toClaimedJob, type ClaimedJobRow } from '../sql/rows.js';
import { resolveQueueDefaults, type QueueDefaultsRow } from '../sql/enqueue.sql.js';
import type { ClaimedJob, Logger } from '../types.js';
import type { Registry } from './registry.js';
import { Runner } from './runner.js';
import { Listener } from './listener.js';

export interface PoolOptions {
  readonly db: Db;
  readonly registry: Registry;
  readonly queues: readonly string[];
  readonly concurrency: number;
  readonly claimBatchMax: number;
  readonly pollIntervalMs: number;
  readonly jobTimeoutMs: number;
  readonly shutdownGraceMs: number;
  readonly heartbeatFraction: number;
  readonly backoff: BackoffOptions;
  readonly maxErrorHistory: number;
  readonly maxErrorTextBytes: number;
  readonly logger?: Logger;
  readonly metrics?: Metrics;
  /** Enables LISTEN/NOTIFY wakeup. Must be a DIRECT connection, not a pooled one. */
  readonly listenConnectionString?: string;
  readonly workerId?: string;
}

export interface PoolStats {
  readonly workerId: string;
  readonly inFlight: number;
  readonly concurrency: number;
  readonly claimed: number;
  readonly succeeded: number;
  readonly failed: number;
  readonly dead: number;
  readonly cancelled: number;
  readonly ownershipLost: number;
  readonly running: boolean;
}

export class WorkerPool {
  readonly workerId: string;
  private readonly opts: PoolOptions;
  private readonly logger: Logger;
  private readonly metrics: Metrics;
  private readonly runner: Runner;
  private readonly listener: Listener | undefined;

  /** In-flight handlers, keyed by job id so cleanup needs no self-reference. */
  private inFlight = new Map<string, Promise<void>>();
  private active = new Map<string, ClaimedJob>();
  private running = false;
  private draining = false;
  private loop: Promise<void> | undefined;
  private wake: (() => void) | undefined;
  private counters = {
    claimed: 0,
    succeeded: 0,
    failed: 0,
    dead: 0,
    cancelled: 0,
    ownershipLost: 0,
  };
  /** Per-queue aging config, refreshed on the same cadence as the client's cache. */
  private queueConfig = new Map<string, { at: number; row: QueueDefaultsRow }>();

  constructor(opts: PoolOptions) {
    this.opts = opts;
    // Identifies the owner in the fence. Must be unique per PROCESS, not per host:
    // two workers on one host must not be able to pass each other's fence.
    this.workerId = opts.workerId ?? `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
    this.logger = (opts.logger ?? nullLogger()).child({ workerId: this.workerId });
    this.metrics = opts.metrics ?? getMetrics();

    this.runner = new Runner({
      db: opts.db,
      registry: opts.registry,
      workerId: this.workerId,
      logger: this.logger,
      metrics: this.metrics,
      jobTimeoutMs: opts.jobTimeoutMs,
      heartbeatFraction: opts.heartbeatFraction,
      backoff: opts.backoff,
      maxErrorHistory: opts.maxErrorHistory,
      maxErrorTextBytes: opts.maxErrorTextBytes,
    });

    if (opts.listenConnectionString !== undefined) {
      this.listener = new Listener({
        connectionString: opts.listenConnectionString,
        queues: opts.queues,
        logger: this.logger,
        metrics: this.metrics,
        onNotify: () => this.signalWake(),
      });
    }
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.draining = false;
    this.listener?.start();
    this.loop = this.claimLoop();
    this.logger.info(
      {
        queues: this.opts.queues,
        concurrency: this.opts.concurrency,
        notify: this.listener !== undefined,
      },
      'worker pool started',
    );
  }

  private signalWake(): void {
    this.wake?.();
  }

  private async claimLoop(): Promise<void> {
    while (this.running) {
      try {
        const free = this.opts.concurrency - this.inFlight.size;
        if (free <= 0) {
          // All slots busy. Wait for one to free rather than spinning on the DB.
          await Promise.race([...this.inFlight.values()]).catch(() => undefined);
          continue;
        }

        let claimedAny = false;
        for (const queue of this.opts.queues) {
          if (!this.running) break;
          const capacity = this.opts.concurrency - this.inFlight.size;
          if (capacity <= 0) break;

          const jobs = await this.claim(queue, Math.min(capacity, this.opts.claimBatchMax));
          if (jobs.length > 0) {
            claimedAny = true;
            for (const job of jobs) this.spawn(job);
          }
        }

        if (!claimedAny) await this.idleWait();
      } catch (e) {
        // The loop must survive anything: a database blip must not end the worker.
        this.logger.error(
          { err: e instanceof Error ? { name: e.name, message: e.message } : String(e) },
          'claim loop error; backing off',
        );
        await sleep(Math.min(5000, this.opts.pollIntervalMs * 2));
      }
    }
  }

  /**
   * Waits for a notification or the poll interval, whichever comes first.
   *
   * The poll timer is jittered so a fleet of idle workers does not query in
   * lockstep, and a notification-driven wake is staggered by a small random delay so
   * one enqueue does not make 50 workers issue a claim in the same millisecond —
   * 49 of which would find nothing after paying for the round trip.
   */
  private async idleWait(): Promise<void> {
    const base = this.opts.pollIntervalMs;
    const jittered = base / 2 + Math.random() * (base / 2);

    await new Promise<void>((resolve) => {
      let settled = false;
      const finish = (): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.wake = undefined;
        resolve();
      };
      const timer = setTimeout(finish, jittered);
      timer.unref?.();
      this.wake = () => {
        // Small stagger on the notify path, for the thundering-herd reason above.
        setTimeout(finish, Math.random() * 50);
      };
    });
  }

  private async claim(queue: string, limit: number): Promise<ClaimedJob[]> {
    const started = performance.now();
    const config = await this.configFor(queue);

    const statement =
      config.priority_aging && config.aging_threshold_s !== null
        ? claimJobsWithAging({
            queue,
            limit,
            workerId: this.workerId,
            agingThresholdSeconds: config.aging_threshold_s,
          })
        : claimJobs({ queue, limit, workerId: this.workerId });

    const res = await this.opts.db.query<ClaimedJobRow>(statement);
    const seconds = (performance.now() - started) / 1000;

    this.metrics.claimDuration.observe({ queue }, seconds);
    this.metrics.claimBatchSize.observe({ queue }, res.rows.length);
    this.counters.claimed += res.rows.length;

    return res.rows.map(toClaimedJob);
  }

  private async configFor(queue: string): Promise<QueueDefaultsRow> {
    const cached = this.queueConfig.get(queue);
    const now = Date.now();
    if (cached && now - cached.at < 5000) return cached.row;
    const res = await this.opts.db.query<QueueDefaultsRow>(resolveQueueDefaults(queue));
    const row: QueueDefaultsRow = res.rows[0] ?? {
      paused: false,
      max_attempts: null,
      lease_seconds: null,
      priority_aging: false,
      aging_threshold_s: null,
      concurrency_limit: null,
    };
    this.queueConfig.set(queue, { at: now, row });
    return row;
  }

  /**
   * Starts a handler without blocking the claim loop.
   *
   * Slot release is in the promise's finally, so a handler that throws during its own
   * cleanup cannot leak a slot and slowly starve the pool.
   */
  private spawn(job: ClaimedJob): void {
    this.active.set(job.id, job);
    this.metrics.jobsInFlight.set({ queue: job.queue }, this.inFlight.size + 1);

    const promise = (async () => {
      try {
        const outcome = await this.runner.run(job);
        switch (outcome) {
          case 'succeeded':
            this.counters.succeeded += 1;
            break;
          case 'retried':
            this.counters.failed += 1;
            break;
          case 'dead':
            this.counters.dead += 1;
            break;
          case 'cancelled':
            this.counters.cancelled += 1;
            break;
          case 'ownership_lost':
            this.counters.ownershipLost += 1;
            break;
        }
      } catch (e) {
        // Runner is written not to throw. If it ever does, containing it here is the
        // difference between one lost job and a dead worker.
        this.logger.error(
          {
            jobId: job.id,
            err: e instanceof Error ? { name: e.name, message: e.message } : String(e),
          },
          'runner threw unexpectedly; job left for lease recovery',
        );
      } finally {
        this.active.delete(job.id);
        this.inFlight.delete(job.id);
        this.metrics.jobsInFlight.set({ queue: job.queue }, this.inFlight.size);
      }
    })();

    this.inFlight.set(job.id, promise);
  }

  /**
   * Stops claiming, waits out the grace period, then actively RELEASES anything
   * still running rather than leaving it to wait out its lease.
   *
   * That last step is what makes a rolling deploy invisible: without it, every job in
   * flight during a restart stalls for up to lease_seconds before another worker can
   * pick it up.
   */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.draining = true;
    this.signalWake();

    this.logger.info({ inFlight: this.inFlight.size }, 'draining worker pool');

    if (this.loop) {
      await this.loop.catch(() => undefined);
      this.loop = undefined;
    }

    if (this.inFlight.size > 0) {
      const graceful = Promise.allSettled([...this.inFlight.values()]);
      const timedOut = await Promise.race([
        graceful.then(() => false),
        sleep(this.opts.shutdownGraceMs).then(() => true),
      ]);

      if (timedOut && this.active.size > 0) {
        this.logger.warn(
          { stranded: this.active.size, graceMs: this.opts.shutdownGraceMs },
          'grace period elapsed; releasing in-flight jobs for immediate re-claim',
        );
        for (const job of this.active.values()) {
          try {
            const released = await this.runner.release(job);
            if (released) {
              this.logger.info({ jobId: job.id }, 'released job back to available');
            }
          } catch (e) {
            // Best effort. The lease is the backstop if this fails.
            this.logger.warn(
              {
                jobId: job.id,
                err: e instanceof Error ? e.message : String(e),
              },
              'could not release job; the lease will recover it',
            );
          }
        }
      }
    }

    await this.listener?.stop();
    this.draining = false;
    this.logger.info(this.stats, 'worker pool stopped');
  }

  get stats(): PoolStats {
    return {
      workerId: this.workerId,
      inFlight: this.inFlight.size,
      concurrency: this.opts.concurrency,
      running: this.running,
      ...this.counters,
    };
  }

  get isDraining(): boolean {
    return this.draining;
  }
}
