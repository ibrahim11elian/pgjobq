/**
 * OpenAPI 3.1 document, generated from the SAME Zod schemas the routes validate
 * against.
 *
 * That shared origin is the point: a hand-maintained spec drifts from the
 * implementation within weeks, and a drifted spec is worse than none because clients
 * trust it.
 */
import { z } from 'zod';
import { createDocument } from 'zod-openapi';
import {
  batchEnqueueBodySchema,
  bulkReplaySchema,
  createScheduleSchema,
  enqueueBodySchema,
  enqueueResponseSchema,
  errorResponseSchema,
  jobResponseSchema,
  listDlqQuerySchema,
  listJobsQuerySchema,
  purgeBodySchema,
} from './schemas.js';

const bearerAuth = { bearerAuth: [] as string[] };

const errorResponse = (description: string) => ({
  description,
  content: { 'application/json': { schema: errorResponseSchema } },
});

const COMMON_ERRORS = {
  '400': errorResponse('Request validation failed'),
  '401': errorResponse('Missing or invalid API key'),
  '403': errorResponse('The key lacks the required scope or queue coverage'),
  '429': errorResponse('Rate limited'),
};

export function buildOpenApiDocument(): ReturnType<typeof createDocument> {
  return createDocument({
    openapi: '3.1.0',
    info: {
      title: 'pgjobq',
      version: '0.1.0',
      description: [
        'A durable background-job engine built on PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED`.',
        '',
        '**Delivery semantics: at-least-once with idempotency support.**',
        '',
        'Exactly-once delivery is not provided and is not achievable across a process',
        'boundary: a worker can commit a side effect and then die before recording that it',
        'did. What is guaranteed is that a job is never held by two workers concurrently,',
        'that an acknowledged enqueue is never silently lost, and that an idempotent handler',
        'produces its effect exactly once.',
        '',
        'Pass `idempotencyKey` on enqueue to make the enqueue itself idempotent. Use the',
        "job's `id` and `attempt` inside a handler to build a stable key for downstream",
        'side effects.',
      ].join('\n'),
      license: { name: 'MIT' },
    },
    servers: [{ url: '/v1', description: 'Versioned API root' }],
    components: {
      securitySchemes: {
        bearerAuth: {
          type: 'http',
          scheme: 'bearer',
          description:
            'API key as `Authorization: Bearer <key>`. Keys carry scopes (enqueue, read, ' +
            'admin) and may be restricted to specific queues.',
        },
      },
    },
    security: [bearerAuth],
    tags: [
      { name: 'Jobs', description: 'Enqueue and inspect jobs' },
      { name: 'Queues', description: 'Queue statistics and control' },
      { name: 'DLQ', description: 'Dead-letter inspection and replay' },
      { name: 'Schedules', description: 'Recurring job definitions' },
      { name: 'Stream', description: 'Live event stream' },
    ],
    paths: {
      '/queues/{queue}/jobs': {
        post: {
          tags: ['Jobs'],
          summary: 'Enqueue a job',
          description:
            'Returns 201 for a newly created job and 200 when an `idempotencyKey` matched ' +
            'an existing one, so a caller can distinguish the two without a second request.',
          requestParams: { path: enqueueQueueParam() },
          requestBody: { content: { 'application/json': { schema: enqueueBodySchema } } },
          responses: {
            '201': {
              description: 'Job created',
              content: { 'application/json': { schema: enqueueResponseSchema } },
            },
            '200': {
              description: 'Existing job returned (deduplicated by idempotencyKey)',
              content: { 'application/json': { schema: enqueueResponseSchema } },
            },
            '409': errorResponse('A non-terminal job with the same uniqueKey already exists'),
            '413': errorResponse('Payload exceeds the configured maximum'),
            ...COMMON_ERRORS,
          },
        },
        get: {
          tags: ['Jobs'],
          summary: 'List jobs in a queue',
          description:
            'Keyset pagination. Pass the returned `nextCursor` as `cursor` for the next ' +
            'page; page depth does not affect latency.',
          requestParams: { path: enqueueQueueParam(), query: listJobsQuerySchema },
          responses: {
            '200': {
              description: 'A page of jobs',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      items: { type: 'array', items: { $ref: '#/components/schemas/Job' } },
                      nextCursor: { type: ['string', 'null'] },
                    },
                  },
                },
              },
            },
            ...COMMON_ERRORS,
          },
        },
      },
      '/queues/{queue}/jobs/batch': {
        post: {
          tags: ['Jobs'],
          summary: 'Enqueue up to 1000 jobs atomically',
          description:
            'All-or-nothing: one invalid element rejects the whole batch and inserts nothing.',
          requestParams: { path: enqueueQueueParam() },
          requestBody: { content: { 'application/json': { schema: batchEnqueueBodySchema } } },
          responses: {
            '201': { description: 'Jobs created' },
            ...COMMON_ERRORS,
          },
        },
      },
      '/jobs/{id}': {
        get: {
          tags: ['Jobs'],
          summary: 'Get a job',
          requestParams: { path: idParam() },
          responses: {
            '200': {
              description: 'The job',
              content: { 'application/json': { schema: jobResponseSchema } },
            },
            '404': errorResponse('No such job'),
            ...COMMON_ERRORS,
          },
        },
      },
      '/jobs/{id}/cancel': {
        post: {
          tags: ['Jobs'],
          summary: 'Cancel a job',
          description:
            'An `available` job is cancelled immediately (200). A `running` job receives a ' +
            'cancellation request (202): the worker aborts its handler and acknowledges. The ' +
            'job is not reported cancelled until the worker agrees, because its side effects ' +
            'may already have happened.',
          requestParams: { path: idParam() },
          responses: {
            '200': { description: 'Cancelled' },
            '202': { description: 'Cancellation requested; awaiting worker acknowledgement' },
            '409': errorResponse('The job is already terminal'),
            ...COMMON_ERRORS,
          },
        },
      },
      '/jobs/{id}/retry': {
        post: {
          tags: ['Jobs'],
          summary: 'Run an available job now',
          description: 'Moves `runAt` to now. Does not consume an attempt.',
          requestParams: { path: idParam() },
          responses: {
            '200': {
              description: 'Updated job',
              content: { 'application/json': { schema: jobResponseSchema } },
            },
            '409': errorResponse('Only available jobs can be advanced'),
            ...COMMON_ERRORS,
          },
        },
      },
      '/jobs/{id}/replay': {
        post: {
          tags: ['DLQ'],
          summary: 'Replay a dead-lettered job',
          description: 'Resets attempts to zero. Error history is preserved.',
          requestParams: { path: idParam() },
          responses: {
            '200': {
              description: 'Replayed job',
              content: { 'application/json': { schema: jobResponseSchema } },
            },
            '409': errorResponse('Only dead-lettered jobs can be replayed'),
            ...COMMON_ERRORS,
          },
        },
      },
      '/queues': {
        get: {
          tags: ['Queues'],
          summary: 'Queue statistics',
          responses: { '200': { description: 'Per-queue counts and lag' }, ...COMMON_ERRORS },
        },
      },
      '/queues/{queue}/pause': {
        post: {
          tags: ['Queues'],
          summary: 'Pause claiming on a queue',
          description: 'Enqueue continues while paused; only claiming is suppressed.',
          requestParams: { path: enqueueQueueParam() },
          responses: { '200': { description: 'Paused' }, ...COMMON_ERRORS },
        },
      },
      '/queues/{queue}/resume': {
        post: {
          tags: ['Queues'],
          summary: 'Resume claiming on a queue',
          requestParams: { path: enqueueQueueParam() },
          responses: { '200': { description: 'Resumed' }, ...COMMON_ERRORS },
        },
      },
      '/queues/{queue}/purge': {
        post: {
          tags: ['Queues'],
          summary: 'Delete jobs in the given states',
          description: 'Requires `confirm` to equal the queue name. Irreversible.',
          requestParams: { path: enqueueQueueParam() },
          requestBody: { content: { 'application/json': { schema: purgeBodySchema } } },
          responses: { '200': { description: 'Rows deleted' }, ...COMMON_ERRORS },
        },
      },
      '/dlq': {
        get: {
          tags: ['DLQ'],
          summary: 'List dead-lettered jobs',
          requestParams: { query: listDlqQuerySchema },
          responses: { '200': { description: 'A page of dead jobs' }, ...COMMON_ERRORS },
        },
      },
      '/dlq/replay': {
        post: {
          tags: ['DLQ'],
          summary: 'Replay many dead-lettered jobs',
          description: 'Reports per-job outcome rather than failing the whole call.',
          requestBody: { content: { 'application/json': { schema: bulkReplaySchema } } },
          responses: { '200': { description: 'Replayed and skipped ids' }, ...COMMON_ERRORS },
        },
      },
      '/schedules': {
        get: {
          tags: ['Schedules'],
          summary: 'List schedules',
          responses: { '200': { description: 'All schedules' }, ...COMMON_ERRORS },
        },
        post: {
          tags: ['Schedules'],
          summary: 'Create a schedule',
          description:
            'The cron expression is evaluated in the given IANA timezone, not the server ' +
            'timezone. Invalid expressions and unknown zones are rejected here rather than ' +
            'failing silently at the first tick.',
          requestBody: { content: { 'application/json': { schema: createScheduleSchema } } },
          responses: {
            '201': { description: 'Schedule created' },
            ...COMMON_ERRORS,
          },
        },
      },
      '/schedules/{id}': {
        get: {
          tags: ['Schedules'],
          summary: 'Get a schedule',
          requestParams: { path: idParam() },
          responses: {
            '200': { description: 'The schedule' },
            '404': errorResponse('No such schedule'),
            ...COMMON_ERRORS,
          },
        },
        delete: {
          tags: ['Schedules'],
          summary: 'Delete a schedule',
          requestParams: { path: idParam() },
          responses: {
            '204': { description: 'Deleted' },
            '404': errorResponse('No such schedule'),
            ...COMMON_ERRORS,
          },
        },
      },
      '/schedules/{id}/pause': {
        post: {
          tags: ['Schedules'],
          summary: 'Pause a schedule',
          requestParams: { path: idParam() },
          responses: { '200': { description: 'Paused' }, ...COMMON_ERRORS },
        },
      },
      '/schedules/{id}/resume': {
        post: {
          tags: ['Schedules'],
          summary: 'Resume a schedule',
          description:
            'Advances to the next FUTURE occurrence rather than backfilling everything ' +
            'missed while paused.',
          requestParams: { path: idParam() },
          responses: { '200': { description: 'Resumed' }, ...COMMON_ERRORS },
        },
      },
      '/events': {
        get: {
          tags: ['Stream'],
          summary: 'Server-Sent Events stream of queue state',
          description:
            'Emits periodic `snapshot` events. Subscriber count is capped; excess ' +
            'subscribers receive 503.',
          responses: {
            '200': { description: 'text/event-stream' },
            '503': errorResponse('Stream at capacity'),
            ...COMMON_ERRORS,
          },
        },
      },
    },
  });
}

/**
 * Path parameters as Zod schemas.
 *
 * zod-openapi derives the parameter documentation from these, so the description a
 * reader sees and the validation a request undergoes come from the same declaration.
 */
// Return types are inferred rather than annotated: zod-openapi needs the concrete
// ZodObject shape, and widening to ZodType loses the input type it requires.
function enqueueQueueParam() {
  return z.object({
    queue: z.string().meta({ description: 'Queue name' }),
  });
}

function idParam() {
  return z.object({
    id: z.string().meta({ description: 'Numeric job or schedule id, carried as a string' }),
  });
}
