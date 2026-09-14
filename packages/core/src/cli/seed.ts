/**
 * Seeds demo data so a fresh clone has something to look at.
 *
 * Deliberately seeds a MIX: succeeded jobs, a live backlog, retrying jobs, and
 * dead-lettered ones. A dashboard showing only empty queues demonstrates nothing.
 */
import { loadConfig } from '../config.js';
import { Db, sql, waitForDatabase } from '../db.js';
import { Client } from '../client.js';
import { Scheduler } from '../engine/scheduler.js';
import { createLogger } from '../observability/logger.js';
import { getMetrics } from '../observability/metrics.js';

const QUEUES = ['default', 'email', 'images', 'reports'] as const;

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger({
    level: config.logLevel,
    pretty: config.nodeEnv !== 'production',
    name: 'seed',
  });
  const metrics = getMetrics({ collectDefault: false });

  const db = new Db({
    connectionString: config.databaseUrl,
    max: 5,
    applicationName: 'pgjobq-seed',
    logger,
  });

  try {
    await waitForDatabase(db, { logger });
    const client = new Client({ db, logger, metrics });

    // Per-queue config, so the dashboard shows more than one shape.
    for (const queue of QUEUES) {
      await db.query(
        sql(
          `INSERT INTO queue_config (queue, max_attempts, lease_seconds, retain_succeeded, retain_dead)
           VALUES ($1, $2, $3, $4::interval, $5::interval)
           ON CONFLICT (queue) DO UPDATE
             SET max_attempts = EXCLUDED.max_attempts,
                 lease_seconds = EXCLUDED.lease_seconds,
                 updated_at = now()`,
          [
            queue,
            queue === 'reports' ? 3 : 5,
            queue === 'reports' ? 300 : 30,
            '24 hours',
            '30 days',
          ],
        ),
      );
    }
    logger.info({ queues: QUEUES }, 'queue config seeded');

    // A live backlog for workers to chew through.
    const emails = await client.enqueueBatch(
      'email',
      Array.from({ length: 40 }, (_, i) => ({
        type: 'demo.email',
        payload: {
          to: `user${i}@example.com`,
          subject: `Welcome, user ${i}`,
          body: 'Thanks for signing up.',
        },
        options: { priority: i < 5 ? 10 : 0 },
      })),
    );

    // Jobs that fail then succeed, so retry and backoff are visible.
    const flaky = await client.enqueueBatch(
      'default',
      Array.from({ length: 8 }, () => ({
        type: 'demo.flaky',
        payload: { failUntilAttempt: 3 },
      })),
    );

    // Jobs that will dead-letter, so the DLQ view has content.
    const poison = await client.enqueueBatch(
      'default',
      Array.from({ length: 4 }, (_, i) => ({
        type: 'demo.poison',
        payload: { reason: `unsupported format #${i}` },
      })),
    );

    // Delayed and scheduled work, so the timeline is not all "now".
    for (let i = 1; i <= 5; i++) {
      await client.enqueue(
        'reports',
        'demo.long',
        { seconds: 20 },
        { delaySeconds: i * 120, priority: -5 },
      );
    }

    // A long-running job to demonstrate heartbeat holding a lease open.
    await client.enqueue('images', 'demo.long', { seconds: 45 });

    // Rate-limited upstream, to show an explicit retry delay overriding backoff.
    await client.enqueue('images', 'demo.throttled', { retryAfterSeconds: 15 });

    // Idempotency: a repeat enqueue must return the same job, not a second one.
    const once = await client.enqueue(
      'default',
      'demo.email',
      { to: 'idempotent@example.com', subject: 'Only once' },
      { idempotencyKey: 'seed-demo-idempotent' },
    );
    const twice = await client.enqueue(
      'default',
      'demo.email',
      { to: 'idempotent@example.com', subject: 'Only once' },
      { idempotencyKey: 'seed-demo-idempotent' },
    );

    const scheduler = new Scheduler({
      db,
      intervalMs: 5000,
      batchMax: 10,
      logger,
      metrics,
    });

    const schedules = [
      {
        name: 'nightly-report',
        queue: 'reports',
        type: 'demo.long',
        cron: '0 2 * * *',
        timezone: 'UTC',
      },
      {
        name: 'hourly-digest',
        queue: 'email',
        type: 'demo.email',
        cron: '0 * * * *',
        timezone: 'Europe/London',
      },
      {
        name: 'frequent-demo',
        queue: 'default',
        type: 'demo.flaky',
        cron: '*/5 * * * *',
        timezone: 'UTC',
      },
    ];

    for (const s of schedules) {
      const existing = (await scheduler.list()).find((x) => x.name === s.name);
      if (existing) continue;
      await scheduler.create({
        name: s.name,
        queue: s.queue,
        type: s.type,
        payload:
          s.type === 'demo.email'
            ? { to: 'digest@example.com', subject: 'Your digest' }
            : s.type === 'demo.long'
              ? { seconds: 20 }
              : { failUntilAttempt: 2 },
        cron: s.cron,
        timezone: s.timezone,
      });
    }

    logger.info(
      {
        email: emails.length,
        flaky: flaky.length,
        poison: poison.length,
        idempotencyWorked: once.id === twice.id && twice.deduplicated,
        schedules: schedules.length,
      },
      'seed complete',
    );

    const stats = await client.queueStats();
    for (const [queue, counts] of Object.entries(stats)) {
      logger.info({ queue, ...counts }, 'queue');
    }
  } finally {
    await db.close();
  }
}

main().catch((e: unknown) => {
  process.stderr.write(`seed failed: ${e instanceof Error ? e.message : String(e)}\n`);
  process.exitCode = 1;
});
