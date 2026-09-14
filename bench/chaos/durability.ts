/**
 * Durability test. Requirement 17.3.
 *
 * Enqueues a batch, then kills worker processes repeatedly mid-drain with SIGKILL, and
 * proves every acknowledged enqueue still reaches a terminal state. Nothing is lost, and
 * nothing is stranded in `running` forever.
 *
 * This is the complement to kill-test.ts. That one proves a single job survives one kill
 * with exactly-once effect; this one proves the *set* survives repeated kills under load.
 *
 *   npm run chaos:durability
 */
import { spawn, type ChildProcess } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  Client,
  Db,
  Reaper,
  claimJobs,
  completeJob,
  getMetrics,
  loadConfig,
  nullLogger,
  sleep,
  sql,
  type ClaimedJobRow,
} from '@pgjobq/core';
import {
  applyEffect,
  countEffects,
  dropChaosTables,
  recordExecution,
  setupChaosTables,
} from './effect.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const VICTIM = path.join(HERE, 'victim.ts');

const QUEUE = 'chaos_durability';
const TOTAL_JOBS = 150;
const KILL_ROUNDS = 3;
/** Short, so leases from killed workers expire inside the test's runtime. */
const LEASE_SECONDS = 2;
/** Generous, because a job may be re-delivered several times as workers keep dying. */
const MAX_ATTEMPTS = 20;

let failures = 0;
let checks = 0;

function check(label: string, ok: boolean, detail?: unknown): void {
  checks += 1;
  if (ok) {
    console.log(`  PASS  ${label}`);
  } else {
    failures += 1;
    console.error(`  FAIL  ${label}${detail !== undefined ? ` -> ${JSON.stringify(detail)}` : ''}`);
  }
}

function spawnDrainer(workerId: string): ChildProcess {
  return spawn(
    process.execPath,
    [
      '--import',
      'tsx',
      '--env-file-if-exists=.env',
      VICTIM,
      '--queue',
      QUEUE,
      '--mode',
      'drain',
      '--worker-id',
      workerId,
    ],
    { cwd: process.cwd(), stdio: ['ignore', 'ignore', 'pipe'] },
  );
}

/** Waits for a killed child to actually be gone, so the next round does not race it. */
function waitForExit(child: ChildProcess): Promise<void> {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      resolve();
      return;
    }
    child.once('exit', () => resolve());
  });
}

async function stateCounts(db: Db): Promise<Record<string, number>> {
  const r = await db.query<{ state: string; n: string }>(
    sql(
      `SELECT state::text AS state, count(*)::text AS n FROM job WHERE queue = $1 GROUP BY state`,
      [QUEUE],
    ),
  );
  const out: Record<string, number> = {};
  for (const row of r.rows) out[row.state] = Number(row.n);
  return out;
}

async function terminalCount(db: Db): Promise<number> {
  const r = await db.query<{ n: string }>(
    sql(
      `SELECT count(*)::text AS n FROM job
        WHERE queue = $1 AND state IN ('succeeded','dead','cancelled')`,
      [QUEUE],
    ),
  );
  return Number(r.rows[0]?.n ?? 0);
}

/** Drains whatever is left, in-process, so the test can reach a settled state. */
async function drainRemaining(db: Db, reaper: Reaper, budgetMs: number): Promise<void> {
  const deadline = Date.now() + budgetMs;
  const workerId = 'durability-finisher';

  while (Date.now() < deadline) {
    const res = await db.query<ClaimedJobRow>(claimJobs({ queue: QUEUE, limit: 20, workerId }));

    if (res.rows.length === 0) {
      // Nothing claimable. Either everything is done, or jobs are sitting in `running`
      // behind an expired lease that needs reaping.
      if ((await terminalCount(db)) >= TOTAL_JOBS) return;
      await reaper.reapOnce();
      await sleep(300);
      continue;
    }

    for (const job of res.rows) {
      await recordExecution(db, job.id, job.attempt, workerId);
      await applyEffect(db, job.id, 'idempotent');
      await db.query(completeJob({ id: job.id, workerId, attempt: job.attempt }));
    }
  }
}

async function main(): Promise<void> {
  const config = loadConfig();
  const db = new Db({
    connectionString: config.databaseUrl,
    max: 12,
    applicationName: 'pgjobq-chaos-durability',
  });

  const metrics = getMetrics({ collectDefault: false });
  const client = new Client({ db, logger: nullLogger(), metrics });
  const reaper = new Reaper({
    db,
    intervalMs: 60_000,
    batchMax: 500,
    backoff: { initialMs: 1, multiplier: 1, maxMs: 1, jitter: 'none' },
    logger: nullLogger(),
    metrics,
  });

  console.log('pgjobq durability test — repeated SIGKILL under load');
  console.log('='.repeat(78));
  console.log(`${TOTAL_JOBS} jobs, ${KILL_ROUNDS} worker kills mid-drain.`);
  console.log('Asserts every acknowledged enqueue reaches a terminal state.');
  console.log('='.repeat(78));

  const enqueuedIds: string[] = [];

  try {
    await setupChaosTables(db);
    await db.query(sql(`DELETE FROM job WHERE queue = $1`, [QUEUE]));
    await db.query(sql(`DELETE FROM chaos_execution`));
    await db.query(sql(`DELETE FROM chaos_effect`));

    // ---- enqueue ----
    const results = await client.enqueueBatch(
      QUEUE,
      Array.from({ length: TOTAL_JOBS }, (_, i) => ({
        type: 'chaos.durable',
        payload: { i },
        options: { leaseSeconds: LEASE_SECONDS, maxAttempts: MAX_ATTEMPTS },
      })),
    );
    for (const r of results) enqueuedIds.push(r.id);
    check(`${TOTAL_JOBS} jobs acknowledged at enqueue`, enqueuedIds.length === TOTAL_JOBS, {
      got: enqueuedIds.length,
    });

    // ---- kill rounds ----
    for (let round = 1; round <= KILL_ROUNDS; round++) {
      const before = await terminalCount(db);
      const child = spawnDrainer(`drainer-${round}`);

      // Let it make real progress, then kill it mid-flight. Killing too early would test
      // nothing; too late and there is nothing left to interrupt.
      const target = before + Math.floor(TOTAL_JOBS / (KILL_ROUNDS + 2));
      const deadline = Date.now() + 20_000;
      while (Date.now() < deadline) {
        if ((await terminalCount(db)) >= target) break;
        await sleep(100);
      }

      const progressed = await terminalCount(db);
      child.kill('SIGKILL');
      await waitForExit(child);

      console.log(
        `  round ${round}: killed worker after ${progressed - before} completions ` +
          `(${progressed}/${TOTAL_JOBS} terminal)`,
      );
      check(`round ${round} worker made progress before being killed`, progressed > before, {
        before,
        after: progressed,
      });
    }

    const midRun = await stateCounts(db);
    console.log(`  after kills: ${JSON.stringify(midRun)}`);

    // ---- settle ----
    // Wait out the leases held by the killed workers, then finish the job.
    await sleep(LEASE_SECONDS * 1000 + 1000);
    await drainRemaining(db, reaper, 90_000);

    // ---- assertions ----
    const final = await stateCounts(db);
    const terminal = await terminalCount(db);
    const effects = await countEffects(db);

    console.log(`\n  final states: ${JSON.stringify(final)}`);

    check(
      'every acknowledged enqueue reached a terminal state — nothing lost',
      terminal === TOTAL_JOBS,
      { terminal, expected: TOTAL_JOBS, final },
    );
    check('no job left stranded in running', (final['running'] ?? 0) === 0, final);
    check('no job left unclaimed in available', (final['available'] ?? 0) === 0, final);

    // Each job's side effect is idempotent and keyed on job id, so the effect count must
    // equal the job count even though many jobs were delivered more than once.
    check(
      'each job applied its side effect exactly once despite repeated kills',
      effects === TOTAL_JOBS,
      { effects, expected: TOTAL_JOBS },
    );

    // Redelivery is the whole point, so executions should exceed the job count.
    const redelivered = await db.query<{ n: string }>(
      sql(`SELECT count(*)::text AS n FROM job WHERE queue = $1 AND attempt > 1`, [QUEUE]),
    );
    const n = Number(redelivered.rows[0]?.n ?? 0);
    console.log(`  ${n} job(s) were re-delivered after a worker died`);
    check('at least one job was actually re-delivered (the kills mattered)', n > 0, { n });

    // Nothing should have exhausted its attempts — the kills were not the job's fault.
    const dead = final['dead'] ?? 0;
    check('no job dead-lettered (worker deaths should not poison jobs)', dead === 0, { dead });
  } finally {
    await db.query(sql(`DELETE FROM job WHERE queue = $1`, [QUEUE]));
    await dropChaosTables(db);
    await db.close();
  }

  console.log(`\n${checks - failures}/${checks} checks passed`);
  if (failures > 0) {
    console.error(`${failures} FAILED`);
    process.exitCode = 1;
  } else {
    console.log('\nRequirement 17.3 satisfied: no acknowledged enqueue is lost across');
    console.log('repeated worker kills.');
  }
}

main().catch((e: unknown) => {
  console.error('durability test failed:', e instanceof Error ? e.stack : e);
  process.exitCode = 1;
});
