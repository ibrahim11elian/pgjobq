/**
 * Shared side-effect helpers for the chaos harnesses.
 *
 * In its own module deliberately: `victim.ts` runs `main()` at module scope, so importing
 * anything from it would execute a victim inside the parent process.
 */
import { Db, sql } from '@pgjobq/core';

export type EffectMode = 'idempotent' | 'naive';

/**
 * Creates the bookkeeping tables.
 *
 * `chaos_execution` counts DELIVERIES. `chaos_effect` counts SIDE EFFECTS. Neither has a
 * unique constraint on `job_id`, because the entire point is to count how many times each
 * happened — a unique index on `chaos_effect` would make the headline assertion pass by
 * construction rather than by the handler being correct.
 */
export async function setupChaosTables(db: Db): Promise<void> {
  await db.query(
    sql(`
      CREATE TABLE IF NOT EXISTS chaos_execution (
        id        bigserial   PRIMARY KEY,
        job_id    text        NOT NULL,
        attempt   integer     NOT NULL,
        worker_id text        NOT NULL,
        at        timestamptz NOT NULL DEFAULT now()
      )
    `),
  );
  await db.query(
    sql(`
      CREATE TABLE IF NOT EXISTS chaos_effect (
        id     bigserial   PRIMARY KEY,
        job_id text        NOT NULL,
        at     timestamptz NOT NULL DEFAULT now()
      )
    `),
  );
}

export async function dropChaosTables(db: Db): Promise<void> {
  await db.query(sql('DROP TABLE IF EXISTS chaos_execution'));
  await db.query(sql('DROP TABLE IF EXISTS chaos_effect'));
}

/** Records that a delivery happened. Append-only, never deduplicated. */
export async function recordExecution(
  db: Db,
  jobId: string,
  attempt: number,
  workerId: string,
): Promise<void> {
  await db.query(
    sql(`INSERT INTO chaos_execution (job_id, attempt, worker_id) VALUES ($1, $2, $3)`, [
      jobId,
      attempt,
      workerId,
    ]),
  );
}

/**
 * The side effect — the thing that must happen exactly once.
 *
 * `idempotent` guards the insert with `NOT EXISTS` in the SAME statement, so it is atomic:
 * two concurrent attempts cannot both pass the check. This is the pattern
 * docs/delivery-guarantees.md asks handlers to follow.
 *
 * `naive` inserts unconditionally. It exists to prove the duplicate-execution window is
 * real and that idempotency is what closes it — not anything the queue does.
 */
export async function applyEffect(db: Db, jobId: string, mode: EffectMode): Promise<void> {
  if (mode === 'idempotent') {
    await db.query(
      sql(
        `INSERT INTO chaos_effect (job_id)
         SELECT $1
          WHERE NOT EXISTS (SELECT 1 FROM chaos_effect WHERE job_id = $1)`,
        [jobId],
      ),
    );
  } else {
    await db.query(sql(`INSERT INTO chaos_effect (job_id) VALUES ($1)`, [jobId]));
  }
}

export async function countExecutions(db: Db, jobId?: string): Promise<number> {
  const r = jobId
    ? await db.query<{ n: string }>(
        sql(`SELECT count(*)::text AS n FROM chaos_execution WHERE job_id = $1`, [jobId]),
      )
    : await db.query<{ n: string }>(sql(`SELECT count(*)::text AS n FROM chaos_execution`));
  return Number(r.rows[0]?.n ?? 0);
}

export async function countEffects(db: Db, jobId?: string): Promise<number> {
  const r = jobId
    ? await db.query<{ n: string }>(
        sql(`SELECT count(*)::text AS n FROM chaos_effect WHERE job_id = $1`, [jobId]),
      )
    : await db.query<{ n: string }>(sql(`SELECT count(*)::text AS n FROM chaos_effect`));
  return Number(r.rows[0]?.n ?? 0);
}
