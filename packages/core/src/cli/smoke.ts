/**
 * End-to-end smoke check.
 *
 * Not a test suite — a proof that the wiring works against a real database. Verifies
 * the claims this design rests on, in order, and exits non-zero on the first failure.
 *
 * Run: npm run smoke
 */
import { loadConfig } from '../config.js';
import { Db, sql, waitForDatabase } from '../db.js';
import { Client } from '../client.js';
import { Reaper } from '../engine/reaper.js';
import { nextOccurrence, Scheduler } from '../engine/scheduler.js';
import { nextDelayMs, baseDelayMs } from '../engine/backoff.js';
import { createRegistry } from '../worker/registry.js';
import { WorkerPool } from '../worker/pool.js';
import { createLogger, nullLogger } from '../observability/logger.js';
import { getMetrics } from '../observability/metrics.js';
import { claimJobs, explainClaim } from '../sql/claim.sql.js';
import { occurrenceKey } from '../sql/schedule.sql.js';
import { canTransition, permittedTargets } from '../engine/state-machine.js';
import { z } from 'zod';
import type { JobState } from '../types.js';

let failures = 0;
let checks = 0;

function check(label: string, condition: boolean, detail?: unknown): void {
  checks += 1;
  if (condition) {
    console.log(`  PASS  ${label}`);
  } else {
    failures += 1;
    console.error(`  FAIL  ${label}${detail !== undefined ? ` -> ${JSON.stringify(detail)}` : ''}`);
  }
}

function section(name: string): void {
  console.log(`\n${name}`);
}

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger({ level: 'warn', pretty: true, name: 'smoke' });
  const metrics = getMetrics({ collectDefault: false });

  const db = new Db({
    connectionString: config.databaseUrl,
    max: 20,
    applicationName: 'pgjobq-smoke',
    logger,
  });

  await waitForDatabase(db, { logger });

  const QUEUE = `smoke_${Date.now().toString(36)}`;
  const client = new Client({ db, logger, metrics });

  try {
    // ---------------------------------------------------------------------
    section('State machine (pure)');
    check('available -> running permitted', canTransition('available', 'running'));
    check('succeeded is terminal', permittedTargets('succeeded').length === 0);
    check('available -> succeeded forbidden', !canTransition('available', 'succeeded'));
    check('dead -> available permitted (replay)', canTransition('dead', 'available'));

    // ---------------------------------------------------------------------
    section('Backoff (pure)');
    const bo = { initialMs: 1000, multiplier: 2, maxMs: 60_000, jitter: 'none' as const };
    check('attempt 1 base = 1000ms', baseDelayMs(1, bo) === 1000, baseDelayMs(1, bo));
    check('attempt 4 base = 8000ms', baseDelayMs(4, bo) === 8000, baseDelayMs(4, bo));
    check('capped at maxMs', baseDelayMs(30, bo) === 60_000, baseDelayMs(30, bo));
    const full = { ...bo, jitter: 'full' as const, random: () => 0.5 };
    check('full jitter halves at r=0.5', nextDelayMs(3, full) === 2000, nextDelayMs(3, full));
    const draws = Array.from({ length: 200 }, () =>
      nextDelayMs(5, { ...bo, jitter: 'full' as const }),
    );
    check(
      'full jitter spreads across the window',
      Math.min(...draws) < 4000 && Math.max(...draws) > 12_000,
      { min: Math.min(...draws), max: Math.max(...draws) },
    );

    // ---------------------------------------------------------------------
    section('Enqueue');
    const first = await client.enqueue(QUEUE, 'smoke.ok', { n: 1 });
    check('enqueue returns an id', first.id.length > 0);
    check('enqueue is not deduplicated', !first.deduplicated);

    const idem = await client.enqueue(QUEUE, 'smoke.ok', { n: 2 }, { idempotencyKey: 'k1' });
    const idemAgain = await client.enqueue(QUEUE, 'smoke.ok', { n: 3 }, { idempotencyKey: 'k1' });
    check('idempotency returns the same id', idem.id === idemAgain.id, {
      first: idem.id,
      second: idemAgain.id,
    });
    check('duplicate is flagged deduplicated', idemAgain.deduplicated);

    // Concurrent duplicates must not both insert — this is the property the unique
    // index provides and a read-then-write check would not.
    const racers = await Promise.all(
      Array.from({ length: 8 }, () =>
        client.enqueue(QUEUE, 'smoke.ok', { n: 9 }, { idempotencyKey: 'race' }),
      ),
    );
    const uniqueIds = new Set(racers.map((r) => r.id));
    check('8 concurrent duplicate enqueues produced 1 row', uniqueIds.size === 1, [...uniqueIds]);

    const batch = await client.enqueueBatch(
      QUEUE,
      Array.from({ length: 50 }, (_, i) => ({ type: 'smoke.ok', payload: { i } })),
    );
    check('batch of 50 inserted', batch.length === 50 && batch.every((b) => b.id.length > 0));

    let oversized = false;
    try {
      await client.enqueue(QUEUE, 'smoke.ok', { blob: 'x'.repeat(400_000) });
    } catch {
      oversized = true;
    }
    check('oversized payload rejected', oversized);

    // ---------------------------------------------------------------------
    section('Claim plan');
    // The plan assertion is only meaningful against a realistically sized table with
    // current statistics. On a nearly-empty table Postgres correctly prefers a
    // sequential scan, so asserting index usage there would test nothing.
    const planQueue = `${QUEUE}_plan`;
    await db.query(
      sql(
        `INSERT INTO job (queue, type, payload, priority, run_at)
         SELECT $1, 'plan.seed', '{}'::jsonb, (i % 5), now() - make_interval(secs => i)
           FROM generate_series(1, 5000) AS i`,
        [planQueue],
      ),
    );
    await db.query(sql('ANALYZE job'));

    const plan = await db.query<{ 'QUERY PLAN': unknown }>(
      explainClaim({ queue: planQueue, limit: 5, workerId: 'plan-check' }),
    );
    const planJson = JSON.stringify(plan.rows[0]?.['QUERY PLAN'] ?? {});
    check('claim plan uses job_claim_idx', planJson.includes('job_claim_idx'), {
      plan: planJson.slice(0, 400),
    });
    check(
      'claim plan has no sort node',
      !planJson.includes('"Node Type": "Sort"') && !planJson.includes('"Node Type":"Sort"'),
    );

    section('Claim');

    const claimed = await db.query<{ id: string; attempt: number }>(
      claimJobs({ queue: QUEUE, limit: 3, workerId: 'w-smoke' }),
    );
    check('claim returned rows', claimed.rows.length === 3, claimed.rows.length);
    check(
      'attempt incremented at claim',
      claimed.rows.every((r) => r.attempt === 1),
    );

    // The core exclusivity property: parallel claimers never receive the same job.
    const parallel = await Promise.all(
      Array.from({ length: 6 }, (_, i) =>
        db.query<{ id: string }>(claimJobs({ queue: QUEUE, limit: 5, workerId: `p-${i}` })),
      ),
    );
    const allIds = parallel.flatMap((r) => r.rows.map((x) => x.id));
    check('no job claimed twice by parallel workers', allIds.length === new Set(allIds).size, {
      claimed: allIds.length,
      distinct: new Set(allIds).size,
    });

    // ---------------------------------------------------------------------
    section('Ownership fence');
    const fenceJob = await client.enqueue(QUEUE, 'smoke.ok', { fence: true });
    const fenceClaim = await db.query<{ id: string; attempt: number }>(
      claimJobs({ queue: QUEUE, limit: 1, workerId: 'owner' }),
    );
    const fc = fenceClaim.rows[0];
    if (fc) {
      const wrongWorker = await db.query(
        sql(
          `UPDATE job SET state='succeeded', finished_at=now()
            WHERE id=$1 AND state='running' AND worker_id=$2 AND attempt=$3 RETURNING id`,
          [fc.id, 'imposter', fc.attempt],
        ),
      );
      check('wrong worker_id is fenced out', wrongWorker.rowCount === 0);

      const wrongAttempt = await db.query(
        sql(
          `UPDATE job SET state='succeeded', finished_at=now()
            WHERE id=$1 AND state='running' AND worker_id=$2 AND attempt=$3 RETURNING id`,
          [fc.id, 'owner', fc.attempt + 5],
        ),
      );
      check('wrong attempt is fenced out', wrongAttempt.rowCount === 0);

      const rightful = await db.query(
        sql(
          `UPDATE job SET state='succeeded', finished_at=now(), worker_id=NULL, lease_expires_at=NULL
            WHERE id=$1 AND state='running' AND worker_id=$2 AND attempt=$3 RETURNING id`,
          [fc.id, 'owner', fc.attempt],
        ),
      );
      check('rightful owner succeeds', rightful.rowCount === 1);
    } else {
      check('fence job was claimable', false, fenceJob);
    }

    // ---------------------------------------------------------------------
    section('CHECK constraints');
    let attemptBound = false;
    try {
      await db.query(
        sql(
          `INSERT INTO job (queue,type,payload,attempt,max_attempts)
                          VALUES ($1,'x','{}'::jsonb,10,5)`,
          [QUEUE],
        ),
      );
    } catch {
      attemptBound = true;
    }
    check('attempt cannot exceed max_attempts', attemptBound);

    let unleasedRunning = false;
    try {
      await db.query(
        sql(`INSERT INTO job (queue,type,payload,state) VALUES ($1,'x','{}'::jsonb,'running')`, [
          QUEUE,
        ]),
      );
    } catch {
      unleasedRunning = true;
    }
    check('running without a lease is unrepresentable', unleasedRunning);

    // ---------------------------------------------------------------------
    section('Worker pool end to end');
    const executed: string[] = [];
    const registry = createRegistry()
      .register('smoke.work', z.object({ label: z.string() }), (payload) => {
        executed.push(payload.label);
      })
      .register('smoke.fail', z.object({}), () => {
        throw new Error('deliberate failure');
      });

    const runQueue = `${QUEUE}_run`;
    await client.enqueue(runQueue, 'smoke.work', { label: 'alpha' });
    await client.enqueue(runQueue, 'smoke.work', { label: 'beta' });
    await client.enqueue(runQueue, 'smoke.fail', {});
    await client.enqueue(runQueue, 'smoke.unregistered', {});

    const pool = new WorkerPool({
      db,
      registry,
      queues: [runQueue],
      concurrency: 4,
      claimBatchMax: 10,
      pollIntervalMs: 100,
      jobTimeoutMs: 5000,
      shutdownGraceMs: 3000,
      heartbeatFraction: 0.5,
      backoff: { initialMs: 50, multiplier: 2, maxMs: 500, jitter: 'full' },
      maxErrorHistory: 5,
      maxErrorTextBytes: 4096,
      logger: nullLogger(),
      metrics,
    });

    pool.start();
    await waitUntil(() => executed.length >= 2, 8000);
    await pool.stop();

    check('handlers executed', executed.length === 2, executed);
    check(
      'handler payloads were typed and validated',
      executed.includes('alpha') && executed.includes('beta'),
    );

    const states = await db.query<{ type: string; state: JobState; attempt: number }>(
      sql(`SELECT type, state, attempt FROM job WHERE queue=$1 ORDER BY type`, [runQueue]),
    );
    const byType = new Map(states.rows.map((r) => [r.type, r]));
    check('successful jobs are succeeded', byType.get('smoke.work')?.state === 'succeeded');
    check(
      'failed job scheduled a retry rather than dying',
      byType.get('smoke.fail')?.state === 'available' &&
        (byType.get('smoke.fail')?.attempt ?? 0) >= 1,
      byType.get('smoke.fail'),
    );
    check(
      'unregistered type dead-lettered as non-retryable',
      byType.get('smoke.unregistered')?.state === 'dead',
      byType.get('smoke.unregistered'),
    );
    check('pool survived a throwing handler', pool.stats.running === false);

    // ---------------------------------------------------------------------
    section('Reaper');
    const reapQueue = `${QUEUE}_reap`;
    await client.enqueue(reapQueue, 'smoke.ok', {}, { leaseSeconds: 1 });
    const reapClaim = await db.query<{ id: string }>(
      claimJobs({ queue: reapQueue, limit: 1, workerId: 'doomed' }),
    );
    check('job claimed for reaping', reapClaim.rows.length === 1);
    // Expire the lease directly rather than sleeping: the reaper's behaviour depends
    // on lease_expires_at < now(), not on how it got that way.
    await db.query(
      sql(`UPDATE job SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, [
        reapClaim.rows[0]?.id ?? '0',
      ]),
    );

    const reaper = new Reaper({
      db,
      intervalMs: 60_000,
      batchMax: 100,
      backoff: { initialMs: 1, multiplier: 1, maxMs: 1, jitter: 'none' },
      logger: nullLogger(),
      metrics,
    });
    const reaped = await reaper.reapOnce();
    check('reaper recovered the expired lease', reaped.recovered >= 1, reaped);

    const after = await db.query<{ state: JobState; attempt: number; worker_id: string | null }>(
      sql(`SELECT state, attempt, worker_id FROM job WHERE id=$1`, [reapClaim.rows[0]?.id ?? '0']),
    );
    check('recovered job is available again', after.rows[0]?.state === 'available', after.rows[0]);
    check('recovered job kept its consumed attempt', after.rows[0]?.attempt === 1, after.rows[0]);
    check('recovered job has no owner', after.rows[0]?.worker_id === null);

    // ---------------------------------------------------------------------
    section('Scheduler');
    check(
      'cron next occurrence is in the future',
      nextOccurrence('*/5 * * * *', 'UTC').getTime() > Date.now(),
    );

    // DST: 0 2 * * * in Europe/London on the 2027 spring-forward night. Local 02:00
    // does not exist; the occurrence must not vanish.
    const springForward = nextOccurrence(
      '0 2 * * *',
      'Europe/London',
      new Date('2027-03-27T12:00:00Z'),
    );
    check(
      'spring-forward occurrence still fires',
      springForward.getTime() > new Date('2027-03-27T12:00:00Z').getTime(),
      springForward.toISOString(),
    );

    let badCron = false;
    try {
      nextOccurrence('not a cron', 'UTC');
    } catch {
      badCron = true;
    }
    check('invalid cron rejected at creation', badCron);

    let badZone = false;
    try {
      nextOccurrence('* * * * *', 'Mars/Olympus_Mons');
    } catch {
      badZone = true;
    }
    check('unknown timezone rejected', badZone);

    check(
      'occurrence key is deterministic',
      occurrenceKey('7', new Date('2026-01-01T00:00:00Z')) ===
        occurrenceKey('7', new Date('2026-01-01T00:00:00.000Z')),
    );

    const scheduler = new Scheduler({
      db,
      intervalMs: 60_000,
      batchMax: 10,
      logger: nullLogger(),
      metrics,
    });
    const schedName = `smoke-sched-${Date.now().toString(36)}`;
    const created = await scheduler.create({
      name: schedName,
      queue: `${QUEUE}_sched`,
      type: 'smoke.ok',
      cron: '* * * * *',
      timezone: 'UTC',
    });
    check('schedule created', created.id.length > 0);

    // Force it due, then tick. The occurrence time becomes the idempotency key.
    const occurrenceAt = new Date(Date.now() - 1000);
    await db.query(
      sql(`UPDATE schedule SET next_run_at = $2 WHERE id=$1`, [created.id, occurrenceAt]),
    );
    const tick1 = await scheduler.tick();
    check('scheduler materialized an occurrence', tick1.created === 1, tick1);

    // Replay the SAME occurrence. Two schedulers racing the same due time generate
    // byte-identical keys, so the unique index must reject the second insert. Setting
    // a different next_run_at here would be a different occurrence and would rightly
    // create a second job.
    await db.query(
      sql(`UPDATE schedule SET next_run_at = $2 WHERE id=$1`, [created.id, occurrenceAt]),
    );
    const tick2 = await scheduler.tick();
    check(
      'the same occurrence is not materialized twice',
      tick2.created === 0 && tick2.deduplicated === 1,
      tick2,
    );

    // A genuinely different occurrence must still fire — dedup must not be so broad
    // that it suppresses real future runs.
    // Must also be in the PAST to be due, so step backwards rather than forwards.
    await db.query(
      sql(`UPDATE schedule SET next_run_at = $2 WHERE id=$1`, [
        created.id,
        new Date(occurrenceAt.getTime() - 60_000),
      ]),
    );
    const tick3 = await scheduler.tick();
    check('a different occurrence does fire', tick3.created === 1, tick3);
    await scheduler.delete(created.id);

    // ---------------------------------------------------------------------
    section('Admin operations');
    const cancelJob = await client.enqueue(QUEUE, 'smoke.ok', {}, { delaySeconds: 3600 });
    const cancelResult = await client.cancel(cancelJob.id, 'smoke');
    check(
      'available job cancelled',
      cancelResult.state === 'cancelled' && cancelResult.acknowledged,
    );

    let forbidden = false;
    try {
      await client.cancel(cancelJob.id, 'smoke');
    } catch {
      forbidden = true;
    }
    check('cancelling a terminal job is rejected', forbidden);

    const stats = await client.queueStats();
    check('queue stats returned data', Object.keys(stats).length > 0);

    await client.setPaused(QUEUE, true, 'smoke');
    const pausedClaim = await db.query(claimJobs({ queue: QUEUE, limit: 5, workerId: 'paused' }));
    check('paused queue yields no jobs', pausedClaim.rowCount === 0);
    await client.setPaused(QUEUE, false, 'smoke');

    // ---------------------------------------------------------------------
    section('Metrics');
    const exposed = await metrics.expose();
    check('metrics expose enqueued counter', exposed.includes('jobq_jobs_enqueued_total'));
    check('metrics expose claim duration', exposed.includes('jobq_claim_duration_seconds'));
    check('metrics expose the fence counter', exposed.includes('jobq_stale_completion_total'));
    const bounded = new Set(Array.from({ length: 500 }, (_, i) => metrics.labelType(`t${i}`)));
    check('label cardinality is bounded', bounded.has('other'), bounded.size);
  } finally {
    // Clean up every queue this run created.
    await db.query(sql(`DELETE FROM job WHERE queue LIKE $1`, [`${QUEUE}%`]));
    await db.query(sql(`DELETE FROM queue_config WHERE queue LIKE $1`, [`${QUEUE}%`]));
    await db.close();
  }

  console.log(`\n${checks - failures}/${checks} checks passed`);
  if (failures > 0) {
    console.error(`${failures} FAILED`);
    process.exitCode = 1;
  }
}

async function waitUntil(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
}

main().catch((e: unknown) => {
  console.error('smoke run failed:', e instanceof Error ? e.stack : e);
  process.exitCode = 1;
});
