/**
 * The client surface: enqueue, read, and administrative operations.
 *
 * Delivery semantics: at-least-once with idempotency support. See
 * docs/delivery-guarantees.md.
 */
import { PG_CODES, pgErrorCode, type Db } from './db.js';
import {
  ConflictError,
  InvalidTransitionError,
  JobNotFoundError,
  PayloadTooLargeError,
  ValidationError,
} from './errors.js';
import { getMetrics, type Metrics } from './observability/metrics.js';
import { nullLogger } from './observability/logger.js';
import {
  findByIdempotencyKey,
  findByUniqueKey,
  insertJob,
  insertJobsBatch,
  resolveQueueDefaults,
  type EnqueueRow,
  type QueueDefaultsRow,
} from './sql/enqueue.sql.js';
import {
  cancelAvailableJob,
  getJob,
  getQueueConfig,
  insertAuditLog,
  listDeadJobs,
  listDistinctQueues,
  listJobs,
  purgeQueue,
  queueLag,
  queueStats,
  replayDeadJob,
  replayDeadJobs,
  requestCancelRunningJob,
  retryJobNow,
  setQueuePaused,
  type ListDeadParams,
  type ListJobsParams,
  type QueueLagRow,
  type QueueStatsRow,
} from './sql/query.sql.js';
import { toJob, toQueueConfig, type JobRow, type QueueConfigRow } from './sql/rows.js';
import { permittedTargets } from './engine/state-machine.js';
import type {
  EnqueueOptions,
  EnqueueResult,
  Job,
  JobState,
  JsonValue,
  Logger,
  QueueConfig,
} from './types.js';

export interface ClientDefaults {
  readonly maxAttempts: number;
  readonly leaseSeconds: number;
  readonly maxPayloadBytes: number;
}

export interface ClientOptions {
  readonly db: Db;
  readonly defaults?: Partial<ClientDefaults>;
  readonly logger?: Logger;
  readonly metrics?: Metrics;
}

const DEFAULTS: ClientDefaults = {
  maxAttempts: 5,
  leaseSeconds: 30,
  maxPayloadBytes: 262_144,
};

export interface BatchEnqueueItem {
  readonly type: string;
  readonly payload: JsonValue;
  readonly options?: EnqueueOptions | undefined;
}

export class Client {
  private readonly db: Db;
  private readonly defaults: ClientDefaults;
  private readonly logger: Logger;
  private readonly metrics: Metrics;
  /** Per-queue defaults, cached briefly to keep enqueue to one round trip. */
  private readonly queueCache = new Map<string, { at: number; row: QueueDefaultsRow }>();
  private readonly queueCacheTtlMs = 5000;

  constructor(opts: ClientOptions) {
    this.db = opts.db;
    this.defaults = { ...DEFAULTS, ...opts.defaults };
    this.logger = opts.logger ?? nullLogger();
    this.metrics = opts.metrics ?? getMetrics();
  }

  /**
   * Enqueues one job.
   *
   * Returns `deduplicated: true` when an idempotency key matched an existing job,
   * so a caller can distinguish "I created this" from "this already existed" without
   * a second lookup.
   */
  async enqueue(
    queue: string,
    type: string,
    payload: JsonValue,
    options: EnqueueOptions = {},
  ): Promise<EnqueueResult> {
    validateName(queue, 'queue');
    validateName(type, 'type');

    const row = await this.buildRow(queue, type, payload, options);

    try {
      const res = await this.db.query<{ id: string }>(insertJob(row));
      const inserted = res.rows[0];
      if (inserted) {
        this.metrics.jobsEnqueued.inc({ queue, type: this.metrics.labelType(type) });
        return { id: inserted.id, deduplicated: false };
      }

      // ON CONFLICT DO NOTHING returned no row: a unique index rejected the insert.
      // Which one determines whether this is benign deduplication or a debounce
      // rejection the caller must be told about.
      return await this.resolveConflict(queue, row);
    } catch (e) {
      if (pgErrorCode(e) === PG_CODES.UNIQUE_VIOLATION) {
        return await this.resolveConflict(queue, row);
      }
      throw e;
    }
  }

  /**
   * Enqueues up to 1,000 jobs in one statement and one transaction.
   *
   * All-or-nothing on validation: one invalid element rejects the whole batch and
   * inserts nothing, so a caller never has to reason about a partially applied batch.
   */
  async enqueueBatch(queue: string, items: readonly BatchEnqueueItem[]): Promise<EnqueueResult[]> {
    validateName(queue, 'queue');
    if (items.length === 0) return [];
    if (items.length > 1000) {
      throw new ValidationError(`Batch size ${items.length} exceeds the maximum of 1000`, {
        details: { size: items.length, max: 1000 },
      });
    }

    // Validate everything BEFORE touching the database, so a rejection costs no work.
    const rows: EnqueueRow[] = [];
    for (const [i, item] of items.entries()) {
      try {
        validateName(item.type, 'type');
        rows.push(await this.buildRow(queue, item.type, item.payload, item.options ?? {}));
      } catch (e) {
        const reason = e instanceof Error ? e.message : String(e);
        throw new ValidationError(`Batch item ${i} is invalid: ${reason}`, {
          details: { index: i },
          cause: e,
        });
      }
    }

    const res = await this.db.query<{
      id: string;
      idempotency_key: string | null;
      unique_key: string | null;
    }>(insertJobsBatch(rows));

    // RETURNING only yields inserted rows, so conflicts are absent. Map back to
    // request order by key; entries with no key cannot conflict and are positional.
    const byIdem = new Map<string, string>();
    for (const r of res.rows) {
      if (r.idempotency_key !== null) byIdem.set(r.idempotency_key, r.id);
    }

    const unkeyed = res.rows.filter((r) => r.idempotency_key === null).map((r) => r.id);
    let unkeyedIdx = 0;
    const out: EnqueueResult[] = [];

    for (const row of rows) {
      if (row.idempotencyKey !== null) {
        const id = byIdem.get(row.idempotencyKey);
        if (id !== undefined) {
          out.push({ id, deduplicated: false });
        } else {
          const existing = await this.db.query<{ id: string }>(
            findByIdempotencyKey(queue, row.idempotencyKey),
          );
          const found = existing.rows[0];
          out.push({ id: found?.id ?? '', deduplicated: true });
        }
      } else {
        const id = unkeyed[unkeyedIdx++];
        out.push({ id: id ?? '', deduplicated: false });
      }
    }

    const created = out.filter((o) => !o.deduplicated).length;
    if (created > 0) {
      for (const item of items) {
        this.metrics.jobsEnqueued.inc({ queue, type: this.metrics.labelType(item.type) });
      }
    }
    return out;
  }

  async getJob(id: string): Promise<Job> {
    const res = await this.db.query<JobRow>(getJob(id));
    const row = res.rows[0];
    if (!row) throw new JobNotFoundError(id);
    return toJob(row);
  }

  async listJobs(params: ListJobsParams): Promise<Job[]> {
    const res = await this.db.query<JobRow>(listJobs(clampLimit(params)));
    return res.rows.map(toJob);
  }

  async listDeadJobs(params: ListDeadParams): Promise<Job[]> {
    const res = await this.db.query<JobRow>(listDeadJobs(clampLimit(params)));
    return res.rows.map(toJob);
  }

  async queueStats(): Promise<Record<string, Partial<Record<JobState, number>>>> {
    const res = await this.db.query<QueueStatsRow>(queueStats());
    const out: Record<string, Partial<Record<JobState, number>>> = {};
    for (const r of res.rows) {
      (out[r.queue] ??= {})[r.state] = Number(r.count);
    }
    return out;
  }

  async queueLag(): Promise<QueueLagRow[]> {
    const res = await this.db.query<QueueLagRow>(queueLag());
    return res.rows;
  }

  async listQueues(): Promise<string[]> {
    const res = await this.db.query<{ queue: string }>(listDistinctQueues());
    return res.rows.map((r) => r.queue);
  }

  async getQueueConfig(queue: string): Promise<QueueConfig | null> {
    const res = await this.db.query<QueueConfigRow>(getQueueConfig(queue));
    const row = res.rows[0];
    return row ? toQueueConfig(row) : null;
  }

  // -------------------------------------------------------------------------
  // Administrative operations. Every one routes through a state-guarded
  // statement, and every one is idempotent or safely repeatable.
  // -------------------------------------------------------------------------

  /**
   * Cancels a job.
   *
   * An `available` job is cancelled outright. A `running` job only gets a
   * cancellation REQUEST: the worker aborts its handler and acknowledges. Reporting
   * it cancelled here would claim a state the executing process has not agreed to.
   */
  async cancel(id: string, actor = 'system'): Promise<{ state: JobState; acknowledged: boolean }> {
    const cancelled = await this.db.query<{ id: string; state: JobState }>(cancelAvailableJob(id));
    if (cancelled.rows[0]) {
      await this.audit(actor, 'job.cancel', id, { from: 'available' });
      return { state: 'cancelled', acknowledged: true };
    }

    const requested = await this.db.query<{ id: string; state: JobState }>(
      requestCancelRunningJob(id),
    );
    if (requested.rows[0]) {
      await this.audit(actor, 'job.cancel_requested', id, { from: 'running' });
      return { state: 'running', acknowledged: false };
    }

    // Neither path matched: the job is missing or already terminal.
    const current = await this.getJob(id);
    throw new InvalidTransitionError(current.state, 'cancelled', permittedTargets(current.state));
  }

  /** Moves an available job's run_at to now. Does not consume an attempt. */
  async retryNow(id: string, actor = 'system'): Promise<Job> {
    const res = await this.db.query<{ id: string }>(retryJobNow(id));
    if (!res.rows[0]) {
      const current = await this.getJob(id);
      throw new ConflictError(
        `Cannot retry job ${id} in state '${current.state}'. Only 'available' jobs can be ` +
          `advanced; use replay for a dead-lettered job.`,
        { details: { id, state: current.state } },
      );
    }
    await this.audit(actor, 'job.retry_now', id, null);
    return this.getJob(id);
  }

  /** Replays a dead-lettered job. Resets attempts, preserves error history. */
  async replay(id: string, actor = 'system'): Promise<Job> {
    const res = await this.db.query<{ id: string }>(replayDeadJob(id));
    if (!res.rows[0]) {
      const current = await this.getJob(id);
      throw new ConflictError(
        `Cannot replay job ${id} in state '${current.state}'. Only dead-lettered jobs can be replayed.`,
        { details: { id, state: current.state } },
      );
    }
    await this.audit(actor, 'job.replay', id, null);
    return this.getJob(id);
  }

  /** Bulk replay. Reports per-job outcome rather than failing the whole call. */
  async replayMany(
    ids: readonly string[],
    actor = 'system',
  ): Promise<{ replayed: string[]; skipped: string[] }> {
    if (ids.length === 0) return { replayed: [], skipped: [] };
    const res = await this.db.query<{ id: string }>(replayDeadJobs(ids));
    const replayed = res.rows.map((r) => r.id);
    const replayedSet = new Set(replayed);
    const skipped = ids.filter((id) => !replayedSet.has(id));
    await this.audit(actor, 'job.replay_bulk', null, {
      requested: ids.length,
      replayed: replayed.length,
    });
    return { replayed, skipped };
  }

  /**
   * Pauses or resumes a queue.
   *
   * Enqueue continues while paused; only claiming is suppressed. That is the useful
   * shape during an incident: stop processing without making callers fail.
   */
  async setPaused(queue: string, paused: boolean, actor = 'system'): Promise<void> {
    validateName(queue, 'queue');
    await this.db.query(setQueuePaused(queue, paused));
    this.queueCache.delete(queue);
    await this.audit(actor, paused ? 'queue.pause' : 'queue.resume', queue, null);
  }

  /**
   * Purges jobs from a queue in the given states, in bounded batches.
   *
   * Requires a confirmation token equal to the queue name, so a mis-aimed call
   * cannot silently destroy a different queue's data.
   */
  async purge(
    queue: string,
    states: readonly JobState[],
    confirm: string,
    opts: { batchSize?: number; maxBatches?: number; actor?: string } = {},
  ): Promise<{ deleted: number }> {
    validateName(queue, 'queue');
    if (confirm !== queue) {
      throw new ValidationError(
        `Purge requires confirmation: pass the queue name '${queue}' as the confirmation token.`,
      );
    }
    if (states.length === 0) {
      throw new ValidationError('Purge requires at least one state');
    }

    const batchSize = opts.batchSize ?? 1000;
    const maxBatches = opts.maxBatches ?? 1000;
    let deleted = 0;

    for (let i = 0; i < maxBatches; i++) {
      const res = await this.db.query<{ id: string }>(purgeQueue(queue, states, batchSize));
      deleted += res.rowCount;
      if (res.rowCount < batchSize) break;
    }

    await this.audit(opts.actor ?? 'system', 'queue.purge', queue, { states, deleted });
    this.logger.warn({ queue, states, deleted }, 'queue purged');
    return { deleted };
  }

  async audit(
    actor: string,
    action: string,
    target: string | null,
    detail: unknown,
  ): Promise<void> {
    await this.db.query(insertAuditLog({ actor, action, target, detail }));
  }

  // -------------------------------------------------------------------------

  private async buildRow(
    queue: string,
    type: string,
    payload: JsonValue,
    options: EnqueueOptions,
  ): Promise<EnqueueRow> {
    const serialized = JSON.stringify(payload ?? null);
    const bytes = Buffer.byteLength(serialized, 'utf8');
    if (bytes > this.defaults.maxPayloadBytes) {
      throw new PayloadTooLargeError(
        `Payload is ${bytes} bytes, exceeding the maximum of ${this.defaults.maxPayloadBytes}`,
        { details: { bytes, max: this.defaults.maxPayloadBytes } },
      );
    }

    if (options.runAt !== undefined && options.delaySeconds !== undefined) {
      throw new ValidationError('Specify either runAt or delaySeconds, not both');
    }
    if (options.delaySeconds !== undefined && options.delaySeconds < 0) {
      throw new ValidationError('delaySeconds cannot be negative');
    }

    const qc = await this.queueDefaults(queue);

    // Precedence: per-job value, then queue config, then system default.
    const maxAttempts = options.maxAttempts ?? qc.max_attempts ?? this.defaults.maxAttempts;
    const leaseSeconds = options.leaseSeconds ?? qc.lease_seconds ?? this.defaults.leaseSeconds;

    if (maxAttempts < 1) throw new ValidationError('maxAttempts must be at least 1');
    if (leaseSeconds < 1) throw new ValidationError('leaseSeconds must be at least 1');

    // Null run_at means "now from the database clock" — see insertJob's COALESCE.
    let runAt: Date | null = null;
    if (options.runAt !== undefined) {
      if (Number.isNaN(options.runAt.getTime())) {
        throw new ValidationError('runAt is not a valid date');
      }
      runAt = options.runAt;
    } else if (options.delaySeconds !== undefined && options.delaySeconds > 0) {
      runAt = new Date(Date.now() + options.delaySeconds * 1000);
    }

    return {
      queue,
      type,
      payload: payload ?? null,
      metadata: options.metadata ?? null,
      priority: options.priority ?? 0,
      maxAttempts,
      leaseSeconds,
      runAt,
      idempotencyKey: options.idempotencyKey ?? null,
      uniqueKey: options.uniqueKey ?? null,
      traceContext: options.traceContext ?? null,
      scheduleId: null,
    };
  }

  /**
   * Distinguishes the two unique indexes after a conflict.
   *
   * An idempotency conflict is benign deduplication — return the winner's ID. A
   * unique_key conflict is a debounce rejection, which the caller needs to know
   * about explicitly, because no new work was scheduled.
   */
  private async resolveConflict(queue: string, row: EnqueueRow): Promise<EnqueueResult> {
    if (row.idempotencyKey !== null) {
      const existing = await this.db.query<{ id: string }>(
        findByIdempotencyKey(queue, row.idempotencyKey),
      );
      const found = existing.rows[0];
      if (found) return { id: found.id, deduplicated: true };
    }

    if (row.uniqueKey !== null) {
      const active = await this.db.query<{ id: string }>(findByUniqueKey(queue, row.uniqueKey));
      const found = active.rows[0];
      if (found) {
        throw new ConflictError(
          `A non-terminal job with uniqueKey '${row.uniqueKey}' already exists on queue '${queue}'`,
          { details: { existingJobId: found.id, uniqueKey: row.uniqueKey } },
        );
      }
    }

    throw new ConflictError('Enqueue conflicted but no matching existing job was found', {
      details: { queue, constraint: 'unknown' },
    });
  }

  private async queueDefaults(queue: string): Promise<QueueDefaultsRow> {
    const cached = this.queueCache.get(queue);
    const now = Date.now();
    if (cached && now - cached.at < this.queueCacheTtlMs) return cached.row;

    const res = await this.db.query<QueueDefaultsRow>(resolveQueueDefaults(queue));
    const row: QueueDefaultsRow = res.rows[0] ?? {
      paused: false,
      max_attempts: null,
      lease_seconds: null,
      priority_aging: false,
      aging_threshold_s: null,
      concurrency_limit: null,
    };
    this.queueCache.set(queue, { at: now, row });
    return row;
  }
}

/** Named things become metric labels and NOTIFY channel names, so keep them tight. */
function validateName(value: string, what: string): void {
  if (typeof value !== 'string' || value.length === 0) {
    throw new ValidationError(`${what} is required`);
  }
  if (value.length > 128) {
    throw new ValidationError(`${what} exceeds 128 characters`);
  }
  if (!/^[a-zA-Z0-9._:-]+$/.test(value)) {
    throw new ValidationError(
      `${what} '${value}' may contain only letters, digits, dot, underscore, colon, and hyphen`,
    );
  }
}

/** Bounds page size so deep pagination cannot be turned into a large scan. */
function clampLimit<T extends { limit: number }>(p: T): T {
  return { ...p, limit: Math.min(Math.max(1, p.limit), 200) };
}
