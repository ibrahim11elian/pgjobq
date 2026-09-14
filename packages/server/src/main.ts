/**
 * API server entrypoint.
 *
 * Runs the HTTP API plus the background loops. Every loop claims its own work with
 * SKIP LOCKED, so running several instances of this process requires no coordination.
 */
import { createServer } from 'node:http';
import {
  Client,
  Reaper,
  Retention,
  Scheduler,
  assertSchemaCurrent,
  createLogger,
  getMetrics,
  loadConfig,
  redactDatabaseUrl,
  Db,
  waitForDatabase,
} from '@pgjobq/core';
import { createApp } from './app.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger({
    level: config.logLevel,
    logPayloads: config.logPayloads,
    pretty: config.nodeEnv !== 'production',
    name: 'api',
  });
  const metrics = getMetrics();

  const db = new Db({
    connectionString: config.databaseUrl,
    max: config.dbPoolMax,
    min: config.dbPoolMin,
    statementTimeoutMs: config.dbStatementTimeoutMs,
    connectTimeoutMs: config.dbConnectTimeoutMs,
    applicationName: 'pgjobq-api',
    logger,
  });

  logger.info({ database: redactDatabaseUrl(config.databaseUrl) }, 'starting api');
  await waitForDatabase(db, { logger });
  // Serving against an out-of-date schema produces errors that look like application
  // bugs. Refusing to start names the real cause immediately.
  const schema = await assertSchemaCurrent(db);
  logger.info({ schemaVersion: schema.currentVersion }, 'schema verified');

  const client = new Client({
    db,
    logger,
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

  const scheduler = config.schedulerEnabled
    ? new Scheduler({
        db,
        intervalMs: config.schedulerIntervalMs,
        batchMax: config.schedulerBatchMax,
        logger,
        metrics,
      })
    : undefined;

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

  const { app, events, auth } = createApp({ config, db, client, scheduler, metrics, logger });

  if (config.apiBootstrapKey !== undefined && config.apiBootstrapKey.length > 0) {
    await auth.ensureBootstrapKey(config.apiBootstrapKey);
    logger.info('bootstrap API key registered');
  } else if (config.nodeEnv === 'production') {
    // Belt and braces: config validation already rejects this, but an
    // unauthenticated production queue is bad enough to check twice.
    throw new Error('Refusing to start in production without API_BOOTSTRAP_KEY');
  } else {
    logger.warn('no API_BOOTSTRAP_KEY set; the API will reject every request');
  }

  reaper?.start();
  scheduler?.start();
  retention?.start();

  // Feeds the dashboard. Reads aggregates on an interval into one broadcast rather
  // than letting each connected browser poll — N browsers must not mean N queries.
  const snapshotTimer = setInterval(() => {
    void (async () => {
      try {
        if (events.subscriberCount === 0) return;
        const [stats, lag] = await Promise.all([client.queueStats(), client.queueLag()]);
        const lagByQueue = new Map(lag.map((l) => [l.queue, l]));
        events.publish({
          type: 'snapshot',
          at: new Date().toISOString(),
          queues: Object.entries(stats).map(([queue, counts]) => ({
            queue,
            counts: counts,
            oldestWaitSeconds: lagByQueue.get(queue)?.oldest_wait_seconds ?? 0,
          })),
        });
      } catch (e) {
        logger.warn({ err: e instanceof Error ? e.message : String(e) }, 'snapshot publish failed');
      }
    })();
  }, 2000);
  snapshotTimer.unref?.();

  // Keeps queue-depth and DLQ gauges current for Prometheus, independent of whether
  // any dashboard is connected.
  const gaugeTimer = setInterval(() => {
    void (async () => {
      try {
        const stats = await client.queueStats();
        for (const [queue, counts] of Object.entries(stats)) {
          for (const [state, count] of Object.entries(counts)) {
            metrics.queueDepth.set({ queue, state }, count);
          }
          metrics.dlqSize.set({ queue }, counts.dead ?? 0);
        }
      } catch {
        // Gauge refresh is best-effort; a blip must not affect request serving.
      }
    })();
  }, 10_000);
  gaugeTimer.unref?.();

  const server = createServer(app);
  // Slightly above a typical 60s load-balancer idle timeout, so the balancer closes
  // idle connections rather than the server racing it and producing 502s.
  server.keepAliveTimeout = 65_000;
  server.headersTimeout = 70_000;

  await new Promise<void>((resolve) => {
    server.listen(config.port, config.host, resolve);
  });
  logger.info(
    { port: config.port, host: config.host, docs: `http://localhost:${config.port}/docs` },
    'api listening',
  );

  let shuttingDown = false;
  const shutdown = (signal: string): void => {
    if (shuttingDown) {
      logger.warn({ signal }, 'second signal; exiting immediately');
      process.exit(1);
    }
    shuttingDown = true;
    logger.info({ signal }, 'shutting down');

    void (async () => {
      clearInterval(snapshotTimer);
      clearInterval(gaugeTimer);
      events.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await Promise.all([reaper?.stop(), scheduler?.stop(), retention?.stop()]);
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
}

main().catch((e: unknown) => {
  process.stderr.write(`api failed to start: ${e instanceof Error ? e.message : String(e)}\n`);
  process.exit(1);
});
