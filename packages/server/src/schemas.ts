/**
 * Request and response schemas.
 *
 * These are the SINGLE source for runtime validation, static types, and the
 * published OpenAPI document. Hand-maintaining a type alongside its schema is how a
 * spec drifts from its implementation.
 */
import { z } from 'zod';
import { DEAD_REASONS, JOB_STATES, type JsonObject, type JsonValue } from '@pgjobq/core';

/** Queue and type names become metric labels and NOTIFY channel names. */
const name = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[a-zA-Z0-9._:-]+$/, 'may contain only letters, digits, dot, underscore, colon, hyphen');

/** bigint ids travel as strings to avoid precision loss above 2^53. */
const id = z.string().regex(/^\d+$/, 'must be a numeric id');

/**
 * Recursive JSON schema, typed as JsonValue rather than unknown.
 *
 * The annotation matters: inferred as `unknown`, a `Record<string, unknown>` would
 * not satisfy `JsonObject` downstream, forcing a cast at every call site that passes
 * metadata or a payload into the client.
 */
export const jsonValue: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.string(),
    z.number(),
    z.boolean(),
    z.null(),
    z.array(jsonValue),
    z.record(z.string(), jsonValue),
  ]),
);

/** A JSON object specifically, for metadata and schedule payloads. */
export const jsonObject: z.ZodType<JsonObject> = z.record(z.string(), jsonValue);

export const enqueueOptionsSchema = z
  .object({
    priority: z.number().int().min(-32768).max(32767).optional(),
    runAt: z.coerce.date().optional(),
    delaySeconds: z.number().min(0).max(31_536_000).optional(),
    maxAttempts: z.number().int().min(1).max(1000).optional(),
    leaseSeconds: z.number().int().min(1).max(86_400).optional(),
    idempotencyKey: z.string().min(1).max(255).optional(),
    uniqueKey: z.string().min(1).max(255).optional(),
    metadata: jsonObject.optional(),
  })
  .strict()
  .refine((v) => !(v.runAt !== undefined && v.delaySeconds !== undefined), {
    message: 'Specify either runAt or delaySeconds, not both',
  });

export const enqueueBodySchema = z
  .object({
    type: name,
    payload: jsonValue,
    options: enqueueOptionsSchema.optional(),
  })
  .strict();

export const batchEnqueueBodySchema = z
  .object({
    jobs: z.array(enqueueBodySchema).min(1).max(1000),
  })
  .strict();

export const queueParamSchema = z.object({ queue: name });
export const idParamSchema = z.object({ id });

export const listJobsQuerySchema = z
  .object({
    state: z.enum(JOB_STATES).optional(),
    type: name.optional(),
    from: z.coerce.date().optional(),
    to: z.coerce.date().optional(),
    /** Keyset cursor. Never an offset: deep offsets degrade into a scan. */
    cursor: id.optional(),
    limit: z.coerce.number().int().min(1).max(200).default(50),
  })
  .strict();

export const listDlqQuerySchema = z
  .object({
    queue: name.optional(),
    type: name.optional(),
    reason: z.enum(DEAD_REASONS).optional(),
    from: z.coerce.date().optional(),
    to: z.coerce.date().optional(),
    cursor: id.optional(),
    limit: z.coerce.number().int().min(1).max(200).default(50),
  })
  .strict();

export const bulkReplaySchema = z.object({ ids: z.array(id).min(1).max(1000) }).strict();

export const purgeBodySchema = z
  .object({
    states: z.array(z.enum(JOB_STATES)).min(1),
    /** Must equal the queue name. Guards against a mis-aimed destructive call. */
    confirm: z.string().min(1),
  })
  .strict();

export const createScheduleSchema = z
  .object({
    name: name,
    queue: name,
    type: name,
    payload: jsonObject.optional(),
    cron: z.string().min(1).max(255),
    timezone: z.string().min(1).max(64).default('UTC'),
    catchupPolicy: z.enum(['skip_missed', 'run_once']).default('skip_missed'),
    enabled: z.boolean().default(true),
  })
  .strict();

export const jobResponseSchema = z.object({
  id: z.string(),
  queue: z.string(),
  type: z.string(),
  state: z.enum(JOB_STATES),
  payload: jsonValue,
  metadata: jsonObject.nullable(),
  priority: z.number(),
  attempt: z.number(),
  maxAttempts: z.number(),
  leaseSeconds: z.number(),
  runAt: z.string(),
  enqueuedAt: z.string(),
  startedAt: z.string().nullable(),
  finishedAt: z.string().nullable(),
  leaseExpiresAt: z.string().nullable(),
  cancelRequestedAt: z.string().nullable(),
  workerId: z.string().nullable(),
  idempotencyKey: z.string().nullable(),
  uniqueKey: z.string().nullable(),
  deadReason: z.enum(DEAD_REASONS).nullable(),
  errors: z.array(z.unknown()),
  scheduleId: z.string().nullable(),
});

export const enqueueResponseSchema = z.object({
  id: z.string(),
  deduplicated: z.boolean(),
});

/** Problem-details shape. `code` is the stable contract; `message` is for humans. */
export const errorResponseSchema = z.object({
  error: z.object({
    code: z.string(),
    message: z.string(),
    details: z.record(z.string(), z.unknown()).optional(),
  }),
});

export type EnqueueBody = z.infer<typeof enqueueBodySchema>;
export type BatchEnqueueBody = z.infer<typeof batchEnqueueBodySchema>;
export type ListJobsQuery = z.infer<typeof listJobsQuerySchema>;
export type ListDlqQuery = z.infer<typeof listDlqQuerySchema>;
export type CreateScheduleBody = z.infer<typeof createScheduleSchema>;
export type PurgeBody = z.infer<typeof purgeBodySchema>;
