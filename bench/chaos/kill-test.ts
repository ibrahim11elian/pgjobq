/**
 * The chaos test. Requirement 17.2.
 *
 * Kills a worker with a real SIGKILL after it has committed its side effect but before it
 * can record completion, then proves:
 *
 *   1. the job is recovered and delivered a second time,
 *   2. an IDEMPOTENT handler's effect happened exactly once,
 *   3. a NAIVE handler's effect happened twice.
 *
 * (3) is not padding. It proves the duplicate-execution window is real and that idempotency
 * is what closes it — not anything the queue does. Without the control run, (2) could pass
 * for the wrong reason.
 *
 * A real SIGKILL matters: it cannot be caught, so no cleanup handler, no `finally`, and no
 * graceful-release path runs. A simulated crash would test the path the developer imagined
 * rather than the one that actually happens.
 *
 *   npm run chaos
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
  countExecutions,
  dropChaosTables,
  recordExecution,
  setupChaosTables,
} from './effect.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const VICTIM = path.join(HERE, 'victim.ts');

/** Short so the lease genuinely expires during the test rather than being forced. */
const LEASE_SECONDS = 3;

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

function section(name: string): void {
  console.log(`\n${name}`);
}

async function cleanup(db: Db, queuePrefix: string): Promise<void> {
  await db.query(sql(`DELETE FROM job WHERE queue LIKE $1`, [`${queuePrefix}%`]));
  await db.query(sql(`DELETE FROM chaos_execution`));
  await db.query(sql(`DELETE FROM chaos_effect`));
}

async function jobRow(
  db: Db,
  id: string,
): Promise<{ state: string; attempt: number; worker_id: string | null } | undefined> {
  const r = await db.query<{ state: string; attempt: number; worker_id: string | null }>(
    sql(`SELECT state, attempt, worker_id FROM job WHERE id = $1`, [id]),
  );
  return r.rows[0];
}

/**
 * Waits until the child has committed its SIDE EFFECT, so the kill lands inside the window
 * this test exists to exercise.
 *
 * Must poll `chaos_effect`, not `chaos_execution`. The victim records the execution first
 * and applies the effect second, so waiting on the execution row would let the parent kill
 * the child *before* the effect was durable — testing the uninteresting case where nothing
 * happened yet, and making the headline assertion pass for the wrong reason.
 */
async function waitForEffectCommitted(db: Db, jobId: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if ((await countEffects(db, jobId)) > 0) return true;
    await sleep(100);
  }
  return false;
}

function spawnVictim(queue: string, mode: string, workerId: string): ChildProcess {
  return spawn(
    process.execPath,
    [
      '--import',
      'tsx',
      '--env-file-if-exists=.env',
      VICTIM,
      '--queue',
      queue,
      '--mode',
      mode,
      '--worker-id',
      workerId,
    ],
    { cwd: process.cwd(), stdio: ['ignore', 'pipe', 'pipe'] },
  );
}

/**
 * Waits for a killed child to actually be gone, so nothing races the next phase.
 *
 * Returns the signal as well as the code, because that is what distinguishes "killed" from
 * "exited on its own" — a SIGKILLed process reports a null exit code and a SIGKILL signal.
 */
function waitForExit(
  child: ChildProcess,
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      resolve({ code: child.exitCode, signal: child.signalCode });
      return;
    }
    child.once('exit', (code, signal) => resolve({ code, signal }));
  });
}

/**
 * One scenario. Returns nothing; asserts as it goes.
 */
async function runScenario(
  db: Db,
  client: Client,
  reaper: Reaper,
  mode: 'idempotent' | 'naive',
): Promise<void> {
  const queue = `chaos_${mode}`;
  section(`Scenario: ${mode} handler`);

  await cleanup(db, queue);

  const enqueued = await client.enqueue(
    queue,
    'chaos.job',
    { mode },
    { leaseSeconds: LEASE_SECONDS, maxAttempts: 5 },
  );
  const jobId = enqueued.id;
  check('job enqueued', jobId.length > 0);

  // ---- first delivery, killed mid-flight ----
  const child = spawnVictim(queue, mode, `victim-${mode}`);
  let stderr = '';
  child.stderr?.on('data', (d: Buffer) => {
    stderr += d.toString();
  });

  const committed = await waitForEffectCommitted(db, jobId, 25_000);
  check('victim claimed the job and committed its side effect', committed, { stderr });
  if (!committed) {
    child.kill('SIGKILL');
    await waitForExit(child);
    return;
  }

  const beforeKill = await jobRow(db, jobId);
  check('job is running and owned by the victim', beforeKill?.state === 'running', beforeKill);
  check('first delivery consumed attempt 1', beforeKill?.attempt === 1, beforeKill);

  // The actual kill. SIGKILL cannot be caught, so nothing in the victim gets to clean up
  // or release the job — exactly like a host loss or an OOM kill.
  child.kill('SIGKILL');
  const exit = await waitForExit(child);
  check('victim died by SIGKILL, not by exiting cleanly', exit.signal === 'SIGKILL', exit);

  const afterKill = await jobRow(db, jobId);
  check(
    'job is still marked running after the kill (no completion was recorded)',
    afterKill?.state === 'running',
    afterKill,
  );

  // ---- recovery ----
  // Wait out the real lease rather than forcing lease_expires_at into the past, so this
  // exercises the actual expiry path.
  await sleep(LEASE_SECONDS * 1000 + 1500);

  const reaped = await reaper.reapOnce();
  check('reaper recovered the abandoned job', reaped.recovered === 1, reaped);

  const recovered = await jobRow(db, jobId);
  check('recovered job is available again', recovered?.state === 'available', recovered);
  check('recovered job kept its consumed attempt', recovered?.attempt === 1, recovered);
  check('recovered job has no owner', recovered?.worker_id === null, recovered);

  // ---- second delivery, in-process, completing properly ----
  const workerId = `redeliver-${mode}`;
  const claimed = await db.query<ClaimedJobRow>(claimJobs({ queue, limit: 1, workerId }));
  const job = claimed.rows[0];
  check('job was re-delivered to a second worker', job !== undefined);
  if (!job) return;

  check('second delivery is attempt 2', job.attempt === 2, { attempt: job.attempt });

  await recordExecution(db, job.id, job.attempt, workerId);
  await applyEffect(db, job.id, mode);
  const done = await db.query<{ id: string }>(
    completeJob({ id: job.id, workerId, attempt: job.attempt }),
  );
  check('second delivery recorded completion', done.rowCount === 1);

  // ---- the assertions that matter ----
  const executions = await countExecutions(db, jobId);
  const effects = await countEffects(db, jobId);
  const final = await jobRow(db, jobId);

  check('job was delivered exactly twice', executions === 2, { executions });
  check('job ended succeeded', final?.state === 'succeeded', final);

  if (mode === 'idempotent') {
    check(
      'THE HEADLINE: idempotent handler applied its effect EXACTLY ONCE despite two deliveries',
      effects === 1,
      { deliveries: executions, effects },
    );
  } else {
    check('CONTROL: naive handler applied its effect TWICE — the window is real', effects === 2, {
      deliveries: executions,
      effects,
    });
  }

  console.log(`        deliveries=${executions}  effects=${effects}  final=${final?.state}`);
}

async function main(): Promise<void> {
  const config = loadConfig();
  const db = new Db({
    connectionString: config.databaseUrl,
    max: 10,
    applicationName: 'pgjobq-chaos',
  });

  const metrics = getMetrics({ collectDefault: false });
  const client = new Client({ db, logger: nullLogger(), metrics });
  const reaper = new Reaper({
    db,
    intervalMs: 60_000,
    batchMax: 100,
    // Effectively no backoff, so the recovered job is immediately claimable and the test
    // is not waiting on jitter.
    backoff: { initialMs: 1, multiplier: 1, maxMs: 1, jitter: 'none' },
    logger: nullLogger(),
    metrics,
  });

  console.log('pgjobq chaos test — SIGKILL mid-handler');
  console.log('='.repeat(78));
  console.log('Kills a worker after its side effect is durable but before completion is');
  console.log('recorded. Proves an idempotent handler lands its effect once, and that a');
  console.log('naive one lands it twice.');
  console.log('='.repeat(78));

  try {
    await setupChaosTables(db);
    await runScenario(db, client, reaper, 'idempotent');
    await runScenario(db, client, reaper, 'naive');
  } finally {
    await cleanup(db, 'chaos_');
    await dropChaosTables(db);
    await db.close();
  }

  console.log(`\n${checks - failures}/${checks} checks passed`);
  if (failures > 0) {
    console.error(`${failures} FAILED`);
    process.exitCode = 1;
  } else {
    console.log('\nRequirement 17.2 satisfied: at-least-once delivery with exactly-once');
    console.log('effect under an idempotent handler, demonstrated by a real SIGKILL.');
  }
}

main().catch((e: unknown) => {
  console.error('chaos test failed:', e instanceof Error ? e.stack : e);
  process.exitCode = 1;
});
