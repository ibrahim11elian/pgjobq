/**
 * Core domain types.
 *
 * Delivery semantics: at-least-once with idempotency support. Never described as
 * exactly-once — a handler can commit its side effect and then lose its process
 * before the completion is recorded. See docs/delivery-guarantees.md.
 */

/**
 * Job lifecycle states.
 *
 * Deliberately avoids "pending"/"queued"/"completed"/"failed": those are ambiguous
 * about whether a retry follows. A job that threw is back in `available` if attempts
 * remain, so calling it "failed" would be wrong.
 *
 * Only `available` is claimable. `succeeded`, `dead`, and `cancelled` are terminal.
 */
export const JOB_STATES = ['available', 'running', 'succeeded', 'dead', 'cancelled'] as const;
export type JobState = (typeof JOB_STATES)[number];

export const TERMINAL_STATES = [
  'succeeded',
  'dead',
  'cancelled',
] as const satisfies readonly JobState[];
export type TerminalState = (typeof TERMINAL_STATES)[number];

/** Why a job was dead-lettered. Machine-readable; surfaced in the DLQ view. */
export const DEAD_REASONS = [
  'attempts_exhausted',
  'lease_expired',
  'non_retryable_error',
  'cancelled',
] as const;
export type DeadReason = (typeof DEAD_REASONS)[number];

export const JITTER_STRATEGIES = ['full', 'equal', 'none'] as const;
export type JitterStrategy = (typeof JITTER_STRATEGIES)[number];

export const CATCHUP_POLICIES = ['skip_missed', 'run_once'] as const;
export type CatchupPolicy = (typeof CATCHUP_POLICIES)[number];

/** JSON-serializable payload. Treated as untrusted input everywhere. */
export type JsonValue =
  string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

export type JsonObject = { [key: string]: JsonValue };

/** One recorded failure. Bounded history kept per job; see MAX_ERROR_HISTORY. */
export interface JobErrorRecord {
  readonly attempt: number;
  readonly at: string;
  readonly kind: 'handler_error' | 'timeout' | 'lease_expired' | 'validation_error' | 'no_handler';
  readonly name?: string;
  readonly message?: string;
  readonly stack?: string;
  readonly worker_id?: string | null;
}

/** A job row as stored. */
export interface Job {
  readonly id: string;
  readonly queue: string;
  readonly type: string;
  readonly state: JobState;
  readonly payload: JsonValue;
  readonly metadata: JsonObject | null;
  readonly priority: number;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly leaseSeconds: number;
  readonly runAt: Date;
  readonly enqueuedAt: Date;
  readonly startedAt: Date | null;
  readonly finishedAt: Date | null;
  readonly leaseExpiresAt: Date | null;
  readonly cancelRequestedAt: Date | null;
  readonly workerId: string | null;
  readonly idempotencyKey: string | null;
  readonly uniqueKey: string | null;
  readonly deadReason: DeadReason | null;
  readonly errors: readonly JobErrorRecord[];
  readonly traceContext: string | null;
  readonly scheduleId: string | null;
  readonly updatedAt: Date;
}

/**
 * The subset of a job returned by the claim statement. Narrower than {@link Job}
 * on purpose: the claim returns only what a worker needs to execute, keeping the
 * row payload off the wire where it isn't used.
 */
export interface ClaimedJob {
  readonly id: string;
  readonly queue: string;
  readonly type: string;
  readonly payload: JsonValue;
  readonly metadata: JsonObject | null;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly leaseSeconds: number;
  readonly leaseExpiresAt: Date;
  readonly traceContext: string | null;
  readonly enqueuedAt: Date;
}

/**
 * Options accepted at enqueue time. All optional; unset values resolve from queue
 * config, then system defaults.
 *
 * Each property explicitly admits `undefined` rather than merely being optional.
 * Under `exactOptionalPropertyTypes` those differ, and these values routinely arrive
 * from parsed JSON where a key is present with an undefined value. Requiring callers
 * to strip such keys would push boilerplate to every call site for no safety gain.
 */
export interface EnqueueOptions {
  readonly priority?: number | undefined;
  readonly runAt?: Date | undefined;
  readonly delaySeconds?: number | undefined;
  readonly maxAttempts?: number | undefined;
  readonly leaseSeconds?: number | undefined;
  /**
   * Makes enqueue idempotent. A repeat enqueue with the same key on the same
   * queue returns the existing job rather than creating a duplicate. Enforced by
   * a unique index, not by a read-then-write check, so concurrent duplicates
   * cannot both succeed.
   */
  readonly idempotencyKey?: string | undefined;
  /**
   * Debounce. Prevents a second job with this key while an earlier one is still
   * non-terminal. Distinct from idempotencyKey: that one is about retry safety,
   * this one is about not queueing the same work twice.
   */
  readonly uniqueKey?: string | undefined;
  readonly metadata?: JsonObject | undefined;
  /** W3C traceparent, so a worker can continue the producer's trace. */
  readonly traceContext?: string | undefined;
}

export interface EnqueueResult {
  readonly id: string;
  /** True when an existing job was returned instead of a new one being created. */
  readonly deduplicated: boolean;
}

export interface JobSpec<T = JsonValue> {
  readonly type: string;
  readonly payload: T;
  readonly options?: EnqueueOptions;
}

/** Per-queue configuration. Overrides system defaults; overridden by per-job values. */
export interface QueueConfig {
  readonly queue: string;
  readonly paused: boolean;
  readonly maxAttempts: number | null;
  readonly leaseSeconds: number | null;
  readonly priorityAging: boolean;
  readonly agingThresholdSeconds: number | null;
  readonly concurrencyLimit: number | null;
  readonly retainSucceeded: string;
  readonly retainDead: string;
}

export interface Schedule {
  readonly id: string;
  readonly name: string;
  readonly queue: string;
  readonly type: string;
  readonly payload: JsonValue;
  readonly cron: string;
  readonly timezone: string;
  readonly catchupPolicy: CatchupPolicy;
  readonly enabled: boolean;
  readonly nextRunAt: Date;
  readonly lastRunAt: Date | null;
  readonly lastJobId: string | null;
}

/** Context handed to every handler. */
export interface JobContext {
  readonly id: string;
  readonly queue: string;
  readonly type: string;
  /**
   * Current delivery number, 1-based. Incremented at claim time, so a worker that
   * dies without reporting still consumes an attempt. Combine with `id` to build
   * a stable idempotency key for downstream side effects.
   */
  readonly attempt: number;
  readonly maxAttempts: number;
  /** Aborted on handler timeout or on an operator cancellation request. */
  readonly signal: AbortSignal;
  readonly logger: Logger;
  /**
   * Extends the lease. Called automatically by the runner; call it directly from
   * a long-running loop to hold the lease across a slow section.
   *
   * @throws {OwnershipLostError} if the lease was already lost — stop work and
   * discard any result, because another worker now owns this job.
   */
  readonly heartbeat: () => Promise<void>;
}

export type JobHandler<T> = (payload: T, ctx: JobContext) => Promise<void> | void;

/** Structural type so core does not force a logger implementation on consumers. */
export interface Logger {
  debug(obj: object, msg?: string): void;
  debug(msg: string): void;
  info(obj: object, msg?: string): void;
  info(msg: string): void;
  warn(obj: object, msg?: string): void;
  warn(msg: string): void;
  error(obj: object, msg?: string): void;
  error(msg: string): void;
  child(bindings: object): Logger;
}

/** Outcome of one delivery, as reported back to the database. */
export type JobOutcome =
  | { readonly kind: 'succeeded' }
  | {
      readonly kind: 'failed';
      readonly error: JobErrorRecord;
      readonly nonRetryable: boolean;
      readonly retryDelayMs?: number;
    }
  | { readonly kind: 'ownership_lost' };
