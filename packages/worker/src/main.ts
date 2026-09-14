/**
 * Standalone worker process.
 *
 * Runs the same WorkerPool a host application would embed — one implementation, two
 * deployment shapes.
 *
 * Background loops (reaper, retention) run here as well as in the API. Every one
 * claims its own work with SKIP LOCKED, so duplicating them across processes is safe
 * by construction and means a deployment with no API instance still recovers leases.
 */
import {
  Reaper,
  Retention,
  assertSchemaCurrent,
  createLogger,
  getMetrics,
  loadConfig,
  redactDatabaseUrl,
  Db,
  WorkerPool,
  waitForDatabase,
} from '@pgjobq/core';
import { createDemoRegistry } from './handlers.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger({
    level: config.logLevel,
    logPayloads: config.logPayloads,
    pretty: config.nodeEnv !== 'production',
    name: 'worker',
  });
  const metrics = getMetrics();

  const db = new Db({
    connectionString: config.databaseUrl,
    max: config.dbPoolMax,
    min: config.dbPoolMin,
    statementTimeoutMs: config.dbStatementTimeoutMs,
    connectTimeoutMs: config.dbConnectTimeoutMs,
    applicationName: 'pgjobq-worker',
    logger,
  });

  logger.info(
    { database: redactDatabaseUrl(config.databaseUrl), queues: config.workerQueues },
    'starting worker',
  );
  await waitForDatabase(db, { logger });
  const schema = await assertSchemaCurrent(db);
  logger.info({ schemaVersion: schema.currentVersion }, 'schema verified');

  const backoff = {
    initialMs: config.backoffInitialMs,
    multiplier: config.backoffMultiplier,
    maxMs: config.backoffMaxMs,
    jitter: config.backoffJitter,
  };

  const pool = new WorkerPool({
    db,
    registry: createDemoRegistry(),
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
    logger,
    metrics,
    // Must be a DIRECT connection. LISTEN state does not survive a transaction-mode
    // pooler, and the failure is silent: the queue keeps working via polling, just
    // with poll-interval latency instead of milliseconds.
    listenConnectionString: config.databaseUrl,
  });

  const reaper = config.reaperEnabled
    ? new Reaper({
        db,
        intervalMs: config.reaperIntervalMs,
        batchMax: config.reaperBatchMax,
        backoff,
        logger,
        metrics,
      })
    : undefined;

  const retention = config.retentionEnabled
    ? new Retention({
        db,
        intervalMs: config.retentionIntervalMs,
        batchMax: config.retentionBatchMax,
        archive: config.retentionArchive,
        defaultRetainSucceeded: '24 hours',
        defaultRetainDead: '30 days',
        logger,
        metrics,
      })
    : undefined;

  reaper?.start();
  retention?.start();
  pool.start();

  let shuttingDown = false;
  const shutdown = (signal: string): void => {
    if (shuttingDown) {
      // A second signal means someone is impatient or the orchestrator is escalating.
      // Leases cover correctness for anything still in flight.
      logger.warn({ signal }, 'second signal; exiting immediately');
      process.exit(1);
    }
    shuttingDown = true;
    logger.info({ signal }, 'draining');

    void (async () => {
      // Pool first: stop claiming before stopping the loops that would recover
      // anything left behind.
      await pool.stop();
      await Promise.all([reaper?.stop(), retention?.stop()]);
      await db.close();
      logger.info('shutdown complete');
      process.exit(0);
    })().catch((e: unknown) => {
      logger.error({ err: e instanceof Error ? e.message : String(e) }, 'shutdown error');
      process.exit(1);
    });
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  // An unhandled rejection anywhere else in the process would otherwise terminate it
  // silently, stranding in-flight jobs until their leases expire.
  process.on('unhandledRejection', (reason) => {
    logger.error(
      { err: reason instanceof Error ? reason.message : String(reason) },
      'unhandled rejection; draining',
    );
    shutdown('unhandledRejection');
  });
}

main().catch((e: unknown) => {
  process.stderr.write(`worker failed to start: ${e instanceof Error ? e.message : String(e)}\n`);
  process.exit(1);
});
