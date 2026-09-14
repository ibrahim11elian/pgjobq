/**
 * The process that gets killed.
 *
 * Run as a child by kill-test.ts and durability.ts. Never imported — it executes on load.
 *
 * It signals progress by INSERTING A ROW, not by writing to stdout. Piped stdout in Node is
 * asynchronous and can buffer, so a parent watching stdout could kill this process before
 * the marker arrives, making the test flaky for reasons unrelated to the queue. A committed
 * row is unambiguous.
 */
import {
  Db,
  claimJobs,
  completeJob,
  loadConfig,
  waitForDatabase,
  type ClaimedJobRow,
} from '@pgjobq/core';
import { applyEffect, recordExecution, type EffectMode } from './effect.js';

type Mode = EffectMode | 'drain';

function parseArgs(argv: string[]): { queue: string; mode: Mode; workerId: string } {
  const get = (flag: string): string | undefined => {
    const i = argv.indexOf(flag);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  return {
    queue: get('--queue') ?? 'chaos',
    mode: (get('--mode') ?? 'idempotent') as Mode,
    workerId: get('--worker-id') ?? `victim-${process.pid}`,
  };
}

/**
 * Processes jobs continuously until killed. Used by the durability test, which decides when
 * to pull the plug.
 */
async function runDrain(db: Db, queue: string, workerId: string): Promise<never> {
  for (;;) {
    const res = await db.query<ClaimedJobRow>(claimJobs({ queue, limit: 1, workerId }));
    const job = res.rows[0];
    if (!job) {
      await new Promise((r) => setTimeout(r, 50));
      continue;
    }
    await recordExecution(db, job.id, job.attempt, workerId);
    await applyEffect(db, job.id, 'idempotent');
    await db.query(completeJob({ id: job.id, workerId, attempt: job.attempt }));
  }
}

/**
 * Claims one job, commits the side effect, then hangs forever without recording completion.
 *
 * That is precisely the window docs/delivery-guarantees.md describes: the effect is durable,
 * the record that it happened is not. The parent SIGKILLs this process here.
 */
async function runSingle(db: Db, queue: string, workerId: string, mode: EffectMode): Promise<void> {
  const res = await db.query<ClaimedJobRow>(claimJobs({ queue, limit: 1, workerId }));
  const job = res.rows[0];
  if (!job) {
    process.stderr.write('victim: no job available to claim\n');
    await db.close();
    process.exit(2);
  }

  await recordExecution(db, job.id, job.attempt, workerId);
  await applyEffect(db, job.id, mode);

  // Keep the event loop alive without spinning the CPU. Never completes the job.
  setInterval(() => undefined, 1000);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();

  const db = new Db({
    connectionString: config.databaseUrl,
    max: 4,
    applicationName: 'pgjobq-chaos-victim',
  });
  await waitForDatabase(db);

  // Branch explicitly rather than relying on control-flow narrowing after an infinite
  // loop, which is subtle enough to be worth avoiding.
  if (args.mode === 'drain') {
    await runDrain(db, args.queue, args.workerId);
    return;
  }

  const effectMode: EffectMode = args.mode === 'naive' ? 'naive' : 'idempotent';
  await runSingle(db, args.queue, args.workerId, effectMode);
}

main().catch((e: unknown) => {
  process.stderr.write(`victim failed: ${e instanceof Error ? e.message : String(e)}\n`);
  process.exit(1);
});
