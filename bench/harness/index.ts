/**
 * Throughput and latency harness.
 *
 * Drives the ENGINE directly, not the HTTP API: the number worth publishing is what
 * the claim path sustains, and routing through Express would measure Express.
 *
 * Reports jobs/sec plus p50/p95/p99 for queue wait and claim statement duration, and
 * sweeps worker concurrency to find where contention rather than worker count becomes
 * the limit.
 *
 *   npm run bench -- --jobs 100000 --concurrency 4,8,16,32
 */
import { performance } from 'node:perf_hooks';
import {
  Client,
  Db,
  claimJobs,
  completeJob,
  loadConfig,
  nullLogger,
  sql,
  waitForDatabase,
  type ClaimedJobRow,
} from '@pgjobq/core';

interface Args {
  jobs: number;
  concurrencies: number[];
  batch: number;
  lockMode: 'for_update' | 'for_no_key_update' | 'both';
  payloadBytes: number;
}

function parseArgs(argv: string[]): Args {
  const get = (flag: string): string | undefined => {
    const i = argv.indexOf(flag);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  return {
    jobs: Number(get('--jobs') ?? 100_000),
    concurrencies: (get('--concurrency') ?? '4,8,16,32').split(',').map((n) => Number(n.trim())),
    batch: Number(get('--batch') ?? 20),
    lockMode: (get('--lock') ?? 'both') as Args['lockMode'],
    payloadBytes: Number(get('--payload') ?? 200),
  };
}

/** Percentile from an unsorted sample. Sorts a copy; fine at harness scale. */
function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx] ?? 0;
}

function fmt(ms: number): string {
  return ms < 1 ? `${(ms * 1000).toFixed(0)}µs` : `${ms.toFixed(2)}ms`;
}

/**
 * The FOR NO KEY UPDATE variant.
 *
 * Sufficient for mutual exclusion between claimers: two concurrent FOR NO KEY UPDATE
 * requests conflict with each other, so SKIP LOCKED still hands each worker a distinct
 * row. It is the weaker lock — it does not conflict with FOR KEY SHARE, so it does not
 * block foreign-key checks referencing the row, and it escalates to MultiXact entries
 * less readily. The claim updates no key column, so the stronger lock buys nothing on
 * paper. This measures whether that holds in practice.
 */
function claimJobsNoKeyUpdate(p: { queue: string; limit: number; workerId: string }) {
  return sql(
    `
    WITH candidate AS (
      SELECT j.id
        FROM job j
        LEFT JOIN queue_config qc ON qc.queue = j.queue
       WHERE j.queue = $1
         AND j.state = 'available'
         AND j.run_at <= now()
         AND COALESCE(qc.paused, false) = false
       ORDER BY j.priority DESC, j.run_at, j.id
       LIMIT $2
       FOR NO KEY UPDATE OF j SKIP LOCKED
    )
    UPDATE job j
       SET state            = 'running',
           attempt          = j.attempt + 1,
           worker_id        = $3,
           started_at       = now(),
           lease_expires_at = now() + make_interval(secs => j.lease_seconds),
           updated_at       = now()
      FROM candidate c
     WHERE j.id = c.id
    RETURNING j.id, j.queue, j.type, j.payload, j.metadata, j.attempt,
              j.max_attempts, j.lease_seconds, j.lease_expires_at,
              j.trace_context, j.enqueued_at
    `,
    [p.queue, p.limit, p.workerId],
  );
}

interface RunResult {
  concurrency: number;
  lockMode: string;
  drained: number;
  wallSeconds: number;
  jobsPerSecond: number;
  claim: { p50: number; p95: number; p99: number; count: number };
  wait: { p50: number; p95: number; p99: number };
  emptyClaims: number;
  avgBatch: number;
}

async function seed(db: Db, queue: string, count: number, payloadBytes: number): Promise<void> {
  const filler = 'x'.repeat(Math.max(0, payloadBytes - 20));
  const CHUNK = 5000;
  for (let done = 0; done < count; done += CHUNK) {
    const n = Math.min(CHUNK, count - done);
    await db.query(
      sql(
        `INSERT INTO job (queue, type, payload, priority)
         SELECT $1, 'bench.noop', jsonb_build_object('i', i, 'f', $3::text), (i % 3)
           FROM generate_series(1, $2::int) AS i`,
        [queue, n, filler],
      ),
    );
  }
  // Fresh statistics, so the planner picks the claim index rather than a seq scan on
  // stale estimates. Without this the first run measures the wrong plan.
  await db.query(sql('ANALYZE job'));
}

async function drain(
  db: Db,
  queue: string,
  concurrency: number,
  batch: number,
  mode: 'for_update' | 'for_no_key_update',
): Promise<RunResult> {
  const claimDurations: number[] = [];
  const waitDurations: number[] = [];
  let drained = 0;
  let emptyClaims = 0;
  let batches = 0;
  let batchTotal = 0;
  let done = false;

  const started = performance.now();

  const worker = async (id: number): Promise<void> => {
    const workerId = `bench-${mode}-${concurrency}-${id}`;
    while (!done) {
      const t0 = performance.now();
      const statement =
        mode === 'for_update'
          ? claimJobs({ queue, limit: batch, workerId })
          : claimJobsNoKeyUpdate({ queue, limit: batch, workerId });
      const res = await db.query<ClaimedJobRow>(statement);
      claimDurations.push(performance.now() - t0);

      if (res.rows.length === 0) {
        emptyClaims += 1;
        done = true;
        return;
      }

      batches += 1;
      batchTotal += res.rows.length;

      for (const row of res.rows) {
        waitDurations.push(Date.now() - new Date(row.enqueued_at).getTime());
      }

      // Complete immediately: the handler is deliberately a no-op so the measurement
      // is of the queue, not of whatever work a handler might do.
      for (const row of res.rows) {
        await db.query(completeJob({ id: row.id, workerId, attempt: row.attempt }));
      }
      drained += res.rows.length;
    }
  };

  await Promise.all(Array.from({ length: concurrency }, (_, i) => worker(i)));
  const wallSeconds = (performance.now() - started) / 1000;

  return {
    concurrency,
    lockMode: mode,
    drained,
    wallSeconds,
    jobsPerSecond: drained / wallSeconds,
    claim: {
      p50: percentile(claimDurations, 50),
      p95: percentile(claimDurations, 95),
      p99: percentile(claimDurations, 99),
      count: claimDurations.length,
    },
    wait: {
      p50: percentile(waitDurations, 50) / 1000,
      p95: percentile(waitDurations, 95) / 1000,
      p99: percentile(waitDurations, 99) / 1000,
    },
    emptyClaims,
    avgBatch: batches === 0 ? 0 : batchTotal / batches,
  };
}

interface BloatSample {
  tableBytes: number;
  indexBytes: number;
  deadTuples: number;
}

async function sampleBloat(db: Db): Promise<BloatSample> {
  const res = await db.query<{
    total_bytes: string;
    index_bytes: string;
    dead: string;
  }>(
    sql(`
      SELECT pg_total_relation_size('job')::text AS total_bytes,
             pg_indexes_size('job')::text        AS index_bytes,
             COALESCE(n_dead_tup, 0)::text       AS dead
        FROM pg_stat_user_tables WHERE relname = 'job'
    `),
  );
  const row = res.rows[0];
  return {
    tableBytes: Number(row?.total_bytes ?? 0),
    indexBytes: Number(row?.index_bytes ?? 0),
    deadTuples: Number(row?.dead ?? 0),
  };
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();

  const db = new Db({
    connectionString: config.databaseUrl,
    // Generous: the harness must not measure pool-acquisition wait instead of the
    // claim statement.
    max: Math.max(20, Math.max(...args.concurrencies) + 10),
    applicationName: 'pgjobq-bench',
    statementTimeoutMs: 120_000,
  });

  await waitForDatabase(db);
  const client = new Client({ db, logger: nullLogger() });
  void client;

  const queue = `bench_${Date.now().toString(36)}`;
  const modes: ('for_update' | 'for_no_key_update')[] =
    args.lockMode === 'both' ? ['for_update', 'for_no_key_update'] : [args.lockMode];

  console.log('pgjobq benchmark');
  console.log('='.repeat(78));
  const version = await db.query<{ v: string }>(
    sql("SELECT current_setting('server_version') AS v"),
  );
  console.log(`postgres           ${version.rows[0]?.v ?? 'unknown'}`);
  console.log(`node               ${process.version}`);
  console.log(`platform           ${process.platform} ${process.arch}`);
  console.log(`jobs per run       ${args.jobs.toLocaleString()}`);
  console.log(`claim batch max    ${args.batch}`);
  console.log(`payload bytes      ${args.payloadBytes}`);
  console.log(`concurrency sweep  ${args.concurrencies.join(', ')}`);
  console.log('='.repeat(78));

  const before = await sampleBloat(db);
  const results: RunResult[] = [];

  try {
    for (const mode of modes) {
      for (const concurrency of args.concurrencies) {
        process.stdout.write(`seeding ${args.jobs.toLocaleString()} jobs... `);
        const seedStart = performance.now();
        await seed(db, queue, args.jobs, args.payloadBytes);
        console.log(`${((performance.now() - seedStart) / 1000).toFixed(1)}s`);

        process.stdout.write(
          `draining  mode=${mode.padEnd(20)} concurrency=${String(concurrency).padStart(3)} ... `,
        );
        const result = await drain(db, queue, concurrency, args.batch, mode);
        results.push(result);
        console.log(
          `${result.jobsPerSecond.toFixed(0).padStart(7)} jobs/s   ` +
            `claim p50=${fmt(result.claim.p50)} p99=${fmt(result.claim.p99)}   ` +
            `batch avg=${result.avgBatch.toFixed(1)}`,
        );

        // Clear between runs so each measures a full drain from a known state.
        await db.query(sql(`DELETE FROM job WHERE queue = $1`, [queue]));
      }
    }

    const after = await sampleBloat(db);

    console.log('\nResults');
    console.log('='.repeat(78));
    console.log(
      'lock mode              conc   jobs/s    claim p50   claim p95   claim p99   batch',
    );
    console.log('-'.repeat(78));
    for (const r of results) {
      console.log(
        `${r.lockMode.padEnd(22)} ${String(r.concurrency).padStart(4)} ` +
          `${r.jobsPerSecond.toFixed(0).padStart(8)} ` +
          `${fmt(r.claim.p50).padStart(11)} ${fmt(r.claim.p95).padStart(11)} ` +
          `${fmt(r.claim.p99).padStart(11)} ${r.avgBatch.toFixed(1).padStart(7)}`,
      );
    }

    // Where does adding workers stop helping? That inflection is the honest answer to
    // "how far does this scale", and it is more useful than a single peak number.
    console.log('\nScaling');
    console.log('-'.repeat(78));
    for (const mode of modes) {
      const forMode = results.filter((r) => r.lockMode === mode);
      let best = forMode[0];
      for (const r of forMode) if (best && r.jobsPerSecond > best.jobsPerSecond) best = r;
      console.log(
        `${mode.padEnd(22)} peak ${best?.jobsPerSecond.toFixed(0) ?? '?'} jobs/s at ` +
          `concurrency ${best?.concurrency ?? '?'}`,
      );
      for (let i = 1; i < forMode.length; i++) {
        const prev = forMode[i - 1];
        const cur = forMode[i];
        if (!prev || !cur) continue;
        const speedup = cur.jobsPerSecond / prev.jobsPerSecond;
        const workerRatio = cur.concurrency / prev.concurrency;
        const efficiency = (speedup / workerRatio) * 100;
        console.log(
          `  ${String(prev.concurrency).padStart(3)} -> ${String(cur.concurrency).padStart(3)} workers: ` +
            `${speedup.toFixed(2)}x throughput for ${workerRatio.toFixed(1)}x workers ` +
            `(${efficiency.toFixed(0)}% scaling efficiency)`,
        );
      }
    }

    console.log('\nBloat');
    console.log('-'.repeat(78));
    console.log(
      `table  ${(before.tableBytes / 1024 / 1024).toFixed(1)} MB -> ` +
        `${(after.tableBytes / 1024 / 1024).toFixed(1)} MB`,
    );
    console.log(
      `index  ${(before.indexBytes / 1024 / 1024).toFixed(1)} MB -> ` +
        `${(after.indexBytes / 1024 / 1024).toFixed(1)} MB`,
    );
    console.log(`dead tuples  ${before.deadTuples} -> ${after.deadTuples}`);
    console.log(
      '\nNote: dead tuples after a run are expected. What matters is whether autovacuum\n' +
        'reclaims them and table size returns to a steady state, which needs a sustained\n' +
        'run rather than a single pass. See docs/scope.md for the scaling limits.',
    );
  } finally {
    await db.query(sql(`DELETE FROM job WHERE queue LIKE $1`, [`bench_%`]));
    await db.close();
  }
}

main().catch((e: unknown) => {
  console.error('benchmark failed:', e instanceof Error ? e.stack : e);
  process.exitCode = 1;
});
