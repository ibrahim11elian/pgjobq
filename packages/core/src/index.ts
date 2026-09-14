/**
 * @pgjobq/core — a durable background-job engine on PostgreSQL SKIP LOCKED.
 *
 * Delivery semantics: AT-LEAST-ONCE with idempotency support. Not exactly-once,
 * which is not achievable across a process boundary. See docs/delivery-guarantees.md.
 */

// Types
export type {
  CatchupPolicy,
  ClaimedJob,
  DeadReason,
  EnqueueOptions,
  EnqueueResult,
  JitterStrategy,
  Job,
  JobContext,
  JobErrorRecord,
  JobHandler,
  JobOutcome,
  JobSpec,
  JobState,
  JsonObject,
  JsonValue,
  Logger,
  QueueConfig,
  Schedule,
  TerminalState,
} from './types.js';
export { DEAD_REASONS, JITTER_STRATEGIES, JOB_STATES, TERMINAL_STATES } from './types.js';

// Errors
export {
  ConfigError,
  ConflictError,
  DatabaseError,
  ERROR_CODES,
  HandlerTimeoutError,
  InvalidTransitionError,
  JobCancelledError,
  JobNotFoundError,
  MigrationError,
  NoHandlerError,
  NonRetryableError,
  OwnershipLostError,
  PayloadTooLargeError,
  PgJobqError,
  RetryAfterError,
  ScheduleNotFoundError,
  ValidationError,
  isNonRetryable,
  isPgJobqError,
  normalizeError,
  truncate,
} from './errors.js';
export type { ErrorCode } from './errors.js';

// Configuration
export { configSchema, loadConfig, redactDatabaseUrl } from './config.js';
export type { Config } from './config.js';

// Database
export {
  Db,
  PG_CODES,
  isTransientDbError,
  pgConstraintName,
  pgErrorCode,
  sleep,
  sql,
  waitForDatabase,
} from './db.js';
export type { DbOptions, Queryable, QueryResult, SafeStatement, Statement } from './db.js';

// Migrations
export {
  assertSchemaCurrent,
  defaultMigrationsDir,
  getApplied,
  loadMigrations,
  migrateUp,
} from './migrate.js';
export type { AppliedMigration, Migration, MigrateResult } from './migrate.js';

// Engine
export {
  TRANSITIONS,
  assertTransition,
  assertTransitionReason,
  canTransition,
  isClaimable,
  isTerminal,
  permittedTargets,
  toMarkdownTable,
  toMermaid,
} from './engine/state-machine.js';
export type { Transition, TransitionReason } from './engine/state-machine.js';

export {
  baseDelayMs,
  describeSchedule,
  nextDelayMs,
  nextDelaySeconds,
  reaperDelaySeconds,
} from './engine/backoff.js';
export type { BackoffOptions } from './engine/backoff.js';

export { Reaper } from './engine/reaper.js';
export type { ReaperOptions, ReapResult } from './engine/reaper.js';

export { Retention } from './engine/retention.js';
export type { RetentionOptions, RetentionResult } from './engine/retention.js';

export { Scheduler, nextOccurrence, validateCron } from './engine/scheduler.js';
export type { SchedulerOptions, SchedulerTickResult } from './engine/scheduler.js';

// Client
export { Client } from './client.js';
export type { BatchEnqueueItem, ClientDefaults, ClientOptions } from './client.js';

// Worker
export { createRegistry } from './worker/registry.js';
export type { Registry, RegistryTypes, TypeMap } from './worker/registry.js';
export { WorkerPool } from './worker/pool.js';
export type { PoolOptions, PoolStats } from './worker/pool.js';
export { Runner } from './worker/runner.js';
export type { RunnerOptions, RunOutcome } from './worker/runner.js';
export { Listener, channelFor } from './worker/listener.js';
export type { ListenerOptions } from './worker/listener.js';

// Observability
export { createLogger, jobLogger, nullLogger } from './observability/logger.js';
export type { LoggerConfig } from './observability/logger.js';
export { Metrics, getMetrics, resetSharedMetrics } from './observability/metrics.js';
export type { MetricsOptions } from './observability/metrics.js';

// SQL — exported so benchmarks and the comparison harness can drive statements
// directly, and so a consumer can inspect exactly what runs against their database.
export { claimJobs, claimJobsWithAging, explainClaim } from './sql/claim.sql.js';
export { completeJob, failJob, heartbeatJob, releaseJob } from './sql/report.sql.js';
export { reapExpiredLeases } from './sql/reap.sql.js';
export { occurrenceKey } from './sql/schedule.sql.js';
export { jobTableHealth } from './sql/retention.sql.js';
export { JOB_COLUMNS, toClaimedJob, toJob, toQueueConfig, toSchedule } from './sql/rows.js';
export type { ClaimedJobRow, JobRow, QueueConfigRow, ScheduleRow } from './sql/rows.js';

import { Db, waitForDatabase } from './db.js';
import { assertSchemaCurrent } from './migrate.js';
import { Client } from './client.js';
import { Reaper } from './engine/reaper.js';
import { Retention } from './engine/retention.js';
import { Scheduler } from './engine/scheduler.js';
import { WorkerPool } from './worker/pool.js';
import { createLogger } from './observability/logger.js';
import { getMetrics } from './observability/metrics.js';
import type { Config } from './config.js';
import type { Registry } from './worker/registry.js';
import type { Logger } from './types.js';

export interface RuntimeOptions {
  readonly config: Config;
  readonly registry?: Registry;
  readonly logger?: Logger;
  /** Skip the startup schema version gate. Only for tooling that manages migrations itself. */
  readonly skipSchemaCheck?: boolean;
}

/**
 * A wired runtime: database, client, worker pool, and the three background loops.
 *
 * Every loop is safe to run on every instance concurrently — each claims its own work
 * with SKIP LOCKED — so scaling out requires no coordination and no leader election.
 */
export class Runtime {
  readonly db: Db;
  readonly client: Client;
  readonly logger: Logger;
  readonly pool: WorkerPool | undefined;
  readonly reaper: Reaper | undefined;
  readonly retention: Retention | undefined;
  readonly scheduler: Scheduler | undefined;
  private started = false;

  constructor(private readonly opts: RuntimeOptions) {
    const { config } = opts;
    this.logger =
      opts.logger ??
      createLogger({
        level: config.logLevel,
        logPayloads: config.logPayloads,
        pretty: config.nodeEnv !== 'production',
      });

    const metrics = getMetrics();

    this.db = new Db({
      connectionString: config.databaseUrl,
      max: config.dbPoolMax,
      min: config.dbPoolMin,
      statementTimeoutMs: config.dbStatementTimeoutMs,
      connectTimeoutMs: config.dbConnectTimeoutMs,
      applicationName: 'pgjobq',
      logger: this.logger,
    });

    this.client = new Client({
      db: this.db,
      logger: this.logger,
      metrics,
      defaults: {
        maxAttempts: config.defaultMaxAttempts,
        leaseSeconds: config.defaultLeaseSeconds,
        maxPayloadBytes: config.maxPayloadBytes,
      },
    });

    const backoff = {
      initialMs: config.backoffInitialMs,
      multiplier: config.backoffMultiplier,
      maxMs: config.backoffMaxMs,
      jitter: config.backoffJitter,
    };

    if (opts.registry) {
      this.pool = new WorkerPool({
        db: this.db,
        registry: opts.registry,
        queues: config.workerQueues,
        concurrency: config.workerConcurrency,
        claimBatchMax: config.workerClaimBatchMax,
        pollIntervalMs: config.workerPollIntervalMs,
        jobTimeoutMs: config.workerJobTimeoutMs,
        shutdownGraceMs: config.workerShutdownGraceMs,
        heartbeatFraction: config.workerHeartbeatFraction,
        backoff,
        maxErrorHistory: config.maxErrorHistory,
        maxErrorTextBytes: config.maxErrorTextBytes,
        logger: this.logger,
        metrics,
        listenConnectionString: config.databaseUrl,
      });
    }

    if (config.reaperEnabled) {
      this.reaper = new Reaper({
        db: this.db,
        intervalMs: config.reaperIntervalMs,
        batchMax: config.reaperBatchMax,
        backoff,
        logger: this.logger,
        metrics,
      });
    }

    if (config.schedulerEnabled) {
      this.scheduler = new Scheduler({
        db: this.db,
        intervalMs: config.schedulerIntervalMs,
        batchMax: config.schedulerBatchMax,
        logger: this.logger,
        metrics,
      });
    }

    if (config.retentionEnabled) {
      this.retention = new Retention({
        db: this.db,
        intervalMs: config.retentionIntervalMs,
        batchMax: config.retentionBatchMax,
        archive: config.retentionArchive,
        defaultRetainSucceeded: '24 hours',
        defaultRetainDead: '30 days',
        logger: this.logger,
        metrics,
      });
    }
  }

  async start(): Promise<void> {
    if (this.started) return;
    await waitForDatabase(this.db, { logger: this.logger });
    if (this.opts.skipSchemaCheck !== true) {
      await assertSchemaCurrent(this.db);
    }
    this.reaper?.start();
    this.scheduler?.start();
    this.retention?.start();
    this.pool?.start();
    this.started = true;
  }

  /**
   * Stops in dependency order: the pool first so no new work is claimed, then the
   * loops, then the connection pool.
   */
  async stop(): Promise<void> {
    if (!this.started) return;
    await this.pool?.stop();
    await Promise.all([this.reaper?.stop(), this.scheduler?.stop(), this.retention?.stop()]);
    await this.db.close();
    this.started = false;
  }

  /**
   * Installs SIGTERM/SIGINT handlers for a graceful drain.
   *
   * A second signal escalates to immediate exit; leases then cover correctness for
   * anything still in flight.
   */
  installSignalHandlers(): void {
    let shuttingDown = false;
    const handle = (signal: string): void => {
      if (shuttingDown) {
        this.logger.warn({ signal }, 'second signal received; exiting immediately');
        process.exit(1);
      }
      shuttingDown = true;
      this.logger.info({ signal }, 'shutdown signal received; draining');
      void this.stop()
        .then(() => {
          this.logger.info('shutdown complete');
          process.exit(0);
        })
        .catch((e: unknown) => {
          this.logger.error(
            { err: e instanceof Error ? e.message : String(e) },
            'error during shutdown',
          );
          process.exit(1);
        });
    };
    process.on('SIGTERM', () => handle('SIGTERM'));
    process.on('SIGINT', () => handle('SIGINT'));
  }
}
