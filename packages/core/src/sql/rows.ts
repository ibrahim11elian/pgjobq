/**
 * Row shapes as Postgres returns them, and the mapping to domain types.
 *
 * Kept separate from the statements so a column rename is a single edit, and so
 * the snake_case/camelCase boundary is explicit in one place rather than smeared
 * across every query.
 */
import type {
  ClaimedJob,
  DeadReason,
  Job,
  JobErrorRecord,
  JobState,
  JsonObject,
  JsonValue,
  QueueConfig,
  Schedule,
  CatchupPolicy,
} from '../types.js';

/** bigint columns arrive as strings; see the type parser in db.ts. */
export interface JobRow {
  id: string;
  queue: string;
  type: string;
  state: JobState;
  payload: JsonValue;
  metadata: JsonObject | null;
  priority: number;
  attempt: number;
  max_attempts: number;
  lease_seconds: number;
  run_at: Date;
  enqueued_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
  lease_expires_at: Date | null;
  cancel_requested_at: Date | null;
  worker_id: string | null;
  idempotency_key: string | null;
  unique_key: string | null;
  dead_reason: DeadReason | null;
  errors: JobErrorRecord[];
  trace_context: string | null;
  schedule_id: string | null;
  updated_at: Date;
}

export interface ClaimedJobRow {
  id: string;
  queue: string;
  type: string;
  payload: JsonValue;
  metadata: JsonObject | null;
  attempt: number;
  max_attempts: number;
  lease_seconds: number;
  lease_expires_at: Date;
  trace_context: string | null;
  enqueued_at: Date;
}

export interface QueueConfigRow {
  queue: string;
  paused: boolean;
  max_attempts: number | null;
  lease_seconds: number | null;
  priority_aging: boolean;
  aging_threshold_s: number | null;
  concurrency_limit: number | null;
  retain_succeeded: string;
  retain_dead: string;
}

export interface ScheduleRow {
  id: string;
  name: string;
  queue: string;
  type: string;
  payload: JsonValue;
  cron: string;
  timezone: string;
  catchup_policy: CatchupPolicy;
  enabled: boolean;
  next_run_at: Date;
  last_run_at: Date | null;
  last_job_id: string | null;
}

export function toJob(r: JobRow): Job {
  return {
    id: r.id,
    queue: r.queue,
    type: r.type,
    state: r.state,
    payload: r.payload,
    metadata: r.metadata,
    priority: r.priority,
    attempt: r.attempt,
    maxAttempts: r.max_attempts,
    leaseSeconds: r.lease_seconds,
    runAt: r.run_at,
    enqueuedAt: r.enqueued_at,
    startedAt: r.started_at,
    finishedAt: r.finished_at,
    leaseExpiresAt: r.lease_expires_at,
    cancelRequestedAt: r.cancel_requested_at,
    workerId: r.worker_id,
    idempotencyKey: r.idempotency_key,
    uniqueKey: r.unique_key,
    deadReason: r.dead_reason,
    errors: Array.isArray(r.errors) ? r.errors : [],
    traceContext: r.trace_context,
    scheduleId: r.schedule_id,
    updatedAt: r.updated_at,
  };
}

export function toClaimedJob(r: ClaimedJobRow): ClaimedJob {
  return {
    id: r.id,
    queue: r.queue,
    type: r.type,
    payload: r.payload,
    metadata: r.metadata,
    attempt: r.attempt,
    maxAttempts: r.max_attempts,
    leaseSeconds: r.lease_seconds,
    leaseExpiresAt: r.lease_expires_at,
    traceContext: r.trace_context,
    enqueuedAt: r.enqueued_at,
  };
}

export function toQueueConfig(r: QueueConfigRow): QueueConfig {
  return {
    queue: r.queue,
    paused: r.paused,
    maxAttempts: r.max_attempts,
    leaseSeconds: r.lease_seconds,
    priorityAging: r.priority_aging,
    agingThresholdSeconds: r.aging_threshold_s,
    concurrencyLimit: r.concurrency_limit,
    retainSucceeded: r.retain_succeeded,
    retainDead: r.retain_dead,
  };
}

export function toSchedule(r: ScheduleRow): Schedule {
  return {
    id: r.id,
    name: r.name,
    queue: r.queue,
    type: r.type,
    payload: r.payload,
    cron: r.cron,
    timezone: r.timezone,
    catchupPolicy: r.catchup_policy,
    enabled: r.enabled,
    nextRunAt: r.next_run_at,
    lastRunAt: r.last_run_at,
    lastJobId: r.last_job_id,
  };
}

/** Columns returned when a full job is needed. Kept as one constant so every read is consistent. */
export const JOB_COLUMNS = `
  id, queue, type, state, payload, metadata, priority, attempt, max_attempts,
  lease_seconds, run_at, enqueued_at, started_at, finished_at, lease_expires_at,
  cancel_requested_at, worker_id, idempotency_key, unique_key, dead_reason,
  errors, trace_context, schedule_id, updated_at
`;
