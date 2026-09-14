/**
 * HTTP routes.
 *
 * Every route validates via middleware and throws domain errors; none of them choose
 * a status code. Async handlers rely on Express 5's native promise rejection
 * forwarding, so no try/catch wrapper is needed.
 */
import { Router, type Request, type Response } from 'express';
import {
  ConflictError,
  ValidationError,
  type Client,
  type Job,
  type Scheduler,
} from '@pgjobq/core';
import { body, params, query, requireScope, validate } from './middleware.js';
import {
  batchEnqueueBodySchema,
  bulkReplaySchema,
  createScheduleSchema,
  enqueueBodySchema,
  idParamSchema,
  listDlqQuerySchema,
  listJobsQuerySchema,
  purgeBodySchema,
  queueParamSchema,
  type BatchEnqueueBody,
  type CreateScheduleBody,
  type EnqueueBody,
  type ListDlqQuery,
  type ListJobsQuery,
  type PurgeBody,
} from './schemas.js';
import type { EventStream } from './events.js';

export interface RouteDeps {
  readonly client: Client;
  readonly scheduler: Scheduler | undefined;
  readonly events: EventStream;
}

/** Serializes a job for the wire: Dates become ISO strings, ids stay strings. */
function serializeJob(job: Job): Record<string, unknown> {
  return {
    id: job.id,
    queue: job.queue,
    type: job.type,
    state: job.state,
    payload: job.payload,
    metadata: job.metadata,
    priority: job.priority,
    attempt: job.attempt,
    maxAttempts: job.maxAttempts,
    leaseSeconds: job.leaseSeconds,
    runAt: job.runAt.toISOString(),
    enqueuedAt: job.enqueuedAt.toISOString(),
    startedAt: job.startedAt?.toISOString() ?? null,
    finishedAt: job.finishedAt?.toISOString() ?? null,
    leaseExpiresAt: job.leaseExpiresAt?.toISOString() ?? null,
    cancelRequestedAt: job.cancelRequestedAt?.toISOString() ?? null,
    workerId: job.workerId,
    idempotencyKey: job.idempotencyKey,
    uniqueKey: job.uniqueKey,
    deadReason: job.deadReason,
    errors: job.errors,
    scheduleId: job.scheduleId,
  };
}

/**
 * Keyset page envelope.
 *
 * `nextCursor` is the last id seen, so the client asks for "before this" rather than
 * "skip N". That is what keeps page 500 as fast as page 1.
 */
function page(jobs: Job[], limit: number): Record<string, unknown> {
  const items = jobs.map(serializeJob);
  const last = jobs[jobs.length - 1];
  return {
    items,
    nextCursor: jobs.length === limit && last ? last.id : null,
  };
}

function actorOf(req: Request): string {
  return req.apiKey ? `key:${req.apiKey.name}` : 'anonymous';
}

export function createRoutes(deps: RouteDeps): Router {
  const r = Router();
  const { client, scheduler, events } = deps;

  // -------------------------------------------------------------------------
  // Enqueue
  // -------------------------------------------------------------------------

  r.post(
    '/queues/:queue/jobs',
    requireScope('enqueue', 'params'),
    validate({ params: queueParamSchema, body: enqueueBodySchema }),
    async (req: Request, res: Response) => {
      const { queue } = params<{ queue: string }>(req);
      const b = body<EnqueueBody>(req);

      const result = await client.enqueue(queue, b.type, b.payload, {
        ...b.options,
        // Continue the caller's trace into the job, so a producer-side trace can be
        // followed through to the worker that later runs it.
        ...(req.get('traceparent') !== undefined ? { traceContext: req.get('traceparent') } : {}),
      });

      // 200 vs 201 is the signal that lets a caller distinguish "created" from
      // "already existed" without a second request.
      res.status(result.deduplicated ? 200 : 201).json(result);
    },
  );

  r.post(
    '/queues/:queue/jobs/batch',
    requireScope('enqueue', 'params'),
    validate({ params: queueParamSchema, body: batchEnqueueBodySchema }),
    async (req: Request, res: Response) => {
      const { queue } = params<{ queue: string }>(req);
      const b = body<BatchEnqueueBody>(req);
      const results = await client.enqueueBatch(
        queue,
        b.jobs.map((j) => ({
          type: j.type,
          payload: j.payload as never,
          ...(j.options ? { options: j.options } : {}),
        })),
      );
      res.status(201).json({ items: results });
    },
  );

  // -------------------------------------------------------------------------
  // Read
  // -------------------------------------------------------------------------

  r.get(
    '/jobs/:id',
    requireScope('read'),
    validate({ params: idParamSchema }),
    async (req: Request, res: Response) => {
      const { id } = params<{ id: string }>(req);
      const job = await client.getJob(id);
      if (req.apiKey && !authorizedForQueue(req, job.queue)) {
        // Do not disclose existence to a key that cannot see this queue.
        res.status(403).json({
          error: { code: 'FORBIDDEN', message: 'This key does not cover the requested resource' },
        });
        return;
      }
      res.json(serializeJob(job));
    },
  );

  r.get(
    '/queues/:queue/jobs',
    requireScope('read', 'params'),
    validate({ params: queueParamSchema, query: listJobsQuerySchema }),
    async (req: Request, res: Response) => {
      const { queue } = params<{ queue: string }>(req);
      const q = query<ListJobsQuery>(req);
      const jobs = await client.listJobs({
        queue,
        state: q.state,
        type: q.type,
        from: q.from,
        to: q.to,
        beforeId: q.cursor,
        limit: q.limit,
      });
      res.json(page(jobs, q.limit));
    },
  );

  r.get('/queues', requireScope('read'), async (_req: Request, res: Response) => {
    const [stats, lag] = await Promise.all([client.queueStats(), client.queueLag()]);
    const lagByQueue = new Map(lag.map((l) => [l.queue, l]));
    const queues = Object.entries(stats).map(([queue, counts]) => ({
      queue,
      counts,
      ready: Number(lagByQueue.get(queue)?.ready ?? 0),
      oldestWaitSeconds: lagByQueue.get(queue)?.oldest_wait_seconds ?? 0,
    }));
    res.json({ queues });
  });

  r.get(
    '/queues/:queue/config',
    requireScope('read', 'params'),
    validate({ params: queueParamSchema }),
    async (req: Request, res: Response) => {
      const { queue } = params<{ queue: string }>(req);
      const config = await client.getQueueConfig(queue);
      res.json({ queue, config });
    },
  );

  // -------------------------------------------------------------------------
  // Administrative
  // -------------------------------------------------------------------------

  r.post(
    '/jobs/:id/cancel',
    requireScope('admin'),
    validate({ params: idParamSchema }),
    async (req: Request, res: Response) => {
      const { id } = params<{ id: string }>(req);
      const result = await client.cancel(id, actorOf(req));
      // 202 when the worker has yet to acknowledge: the request is accepted but the
      // job is still running, and saying 200 would overstate what happened.
      res.status(result.acknowledged ? 200 : 202).json(result);
    },
  );

  r.post(
    '/jobs/:id/retry',
    requireScope('admin'),
    validate({ params: idParamSchema }),
    async (req: Request, res: Response) => {
      const { id } = params<{ id: string }>(req);
      const job = await client.retryNow(id, actorOf(req));
      res.json(serializeJob(job));
    },
  );

  r.post(
    '/jobs/:id/replay',
    requireScope('admin'),
    validate({ params: idParamSchema }),
    async (req: Request, res: Response) => {
      const { id } = params<{ id: string }>(req);
      const job = await client.replay(id, actorOf(req));
      res.json(serializeJob(job));
    },
  );

  r.post(
    '/queues/:queue/pause',
    requireScope('admin', 'params'),
    validate({ params: queueParamSchema }),
    async (req: Request, res: Response) => {
      const { queue } = params<{ queue: string }>(req);
      await client.setPaused(queue, true, actorOf(req));
      res.json({ queue, paused: true });
    },
  );

  r.post(
    '/queues/:queue/resume',
    requireScope('admin', 'params'),
    validate({ params: queueParamSchema }),
    async (req: Request, res: Response) => {
      const { queue } = params<{ queue: string }>(req);
      await client.setPaused(queue, false, actorOf(req));
      res.json({ queue, paused: false });
    },
  );

  r.post(
    '/queues/:queue/purge',
    requireScope('admin', 'params'),
    validate({ params: queueParamSchema, body: purgeBodySchema }),
    async (req: Request, res: Response) => {
      const { queue } = params<{ queue: string }>(req);
      const b = body<PurgeBody>(req);
      const result = await client.purge(queue, b.states, b.confirm, {
        actor: actorOf(req),
      });
      res.json({ queue, ...result });
    },
  );

  // -------------------------------------------------------------------------
  // Dead-letter queue
  // -------------------------------------------------------------------------

  r.get(
    '/dlq',
    requireScope('read', 'query'),
    validate({ query: listDlqQuerySchema }),
    async (req: Request, res: Response) => {
      const q = query<ListDlqQuery>(req);
      const jobs = await client.listDeadJobs({
        queue: q.queue,
        type: q.type,
        reason: q.reason,
        from: q.from,
        to: q.to,
        beforeId: q.cursor,
        limit: q.limit,
      });
      res.json(page(jobs, q.limit));
    },
  );

  r.post(
    '/dlq/replay',
    requireScope('admin'),
    validate({ body: bulkReplaySchema }),
    async (req: Request, res: Response) => {
      const b = body<{ ids: string[] }>(req);
      const result = await client.replayMany(b.ids, actorOf(req));
      res.json(result);
    },
  );

  // -------------------------------------------------------------------------
  // Schedules
  // -------------------------------------------------------------------------

  r.get('/schedules', requireScope('read'), async (_req: Request, res: Response) => {
    requireScheduler(scheduler);
    const schedules = await scheduler.list();
    res.json({ items: schedules.map(serializeSchedule) });
  });

  r.post(
    '/schedules',
    requireScope('admin', 'body'),
    validate({ body: createScheduleSchema }),
    async (req: Request, res: Response) => {
      requireScheduler(scheduler);
      const b = body<CreateScheduleBody>(req);
      const created = await scheduler.create({
        name: b.name,
        queue: b.queue,
        type: b.type,
        payload: b.payload ?? {},
        cron: b.cron,
        timezone: b.timezone,
        catchupPolicy: b.catchupPolicy,
        enabled: b.enabled,
      });
      await client.audit(actorOf(req), 'schedule.create', created.id, { name: b.name });
      res.status(201).json(serializeSchedule(created));
    },
  );

  r.get(
    '/schedules/:id',
    requireScope('read'),
    validate({ params: idParamSchema }),
    async (req: Request, res: Response) => {
      requireScheduler(scheduler);
      const { id } = params<{ id: string }>(req);
      const found = await scheduler.get(id);
      if (!found) {
        res.status(404).json({
          error: { code: 'SCHEDULE_NOT_FOUND', message: `Schedule ${id} not found` },
        });
        return;
      }
      res.json(serializeSchedule(found));
    },
  );

  for (const [suffix, enabled] of [
    ['pause', false],
    ['resume', true],
  ] as const) {
    r.post(
      `/schedules/:id/${suffix}`,
      requireScope('admin'),
      validate({ params: idParamSchema }),
      async (req: Request, res: Response) => {
        requireScheduler(scheduler);
        const { id } = params<{ id: string }>(req);
        const updated = await scheduler.setEnabled(id, enabled);
        if (!updated) {
          res.status(404).json({
            error: { code: 'SCHEDULE_NOT_FOUND', message: `Schedule ${id} not found` },
          });
          return;
        }
        await client.audit(actorOf(req), `schedule.${suffix}`, id, null);
        res.json(serializeSchedule(updated));
      },
    );
  }

  r.delete(
    '/schedules/:id',
    requireScope('admin'),
    validate({ params: idParamSchema }),
    async (req: Request, res: Response) => {
      requireScheduler(scheduler);
      const { id } = params<{ id: string }>(req);
      const deleted = await scheduler.delete(id);
      if (!deleted) {
        res.status(404).json({
          error: { code: 'SCHEDULE_NOT_FOUND', message: `Schedule ${id} not found` },
        });
        return;
      }
      await client.audit(actorOf(req), 'schedule.delete', id, null);
      res.status(204).end();
    },
  );

  // -------------------------------------------------------------------------
  // Event stream
  // -------------------------------------------------------------------------

  r.get('/events', requireScope('read'), (req: Request, res: Response) => {
    events.subscribe(req, res);
  });

  return r;
}

function serializeSchedule(s: {
  id: string;
  name: string;
  queue: string;
  type: string;
  payload: unknown;
  cron: string;
  timezone: string;
  catchupPolicy: string;
  enabled: boolean;
  nextRunAt: Date;
  lastRunAt: Date | null;
  lastJobId: string | null;
}): Record<string, unknown> {
  return {
    id: s.id,
    name: s.name,
    queue: s.queue,
    type: s.type,
    payload: s.payload,
    cron: s.cron,
    timezone: s.timezone,
    catchupPolicy: s.catchupPolicy,
    enabled: s.enabled,
    nextRunAt: s.nextRunAt.toISOString(),
    lastRunAt: s.lastRunAt?.toISOString() ?? null,
    lastJobId: s.lastJobId,
  };
}

function authorizedForQueue(req: Request, queue: string): boolean {
  const record = req.apiKey;
  if (!record) return false;
  if (record.queues === null) return true;
  return record.queues.includes(queue);
}

/** Asserts the scheduler is configured, so route bodies can use it unconditionally. */
function requireScheduler(s: Scheduler | undefined): asserts s is Scheduler {
  if (!s) {
    throw new ConflictError(
      'The scheduler is disabled on this instance. Set SCHEDULER_ENABLED=true to use schedules.',
    );
  }
}

export { ValidationError };
