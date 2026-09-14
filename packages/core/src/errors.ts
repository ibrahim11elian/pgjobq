/**
 * Typed error hierarchy with stable machine-readable codes.
 *
 * Codes are part of the public contract: the HTTP layer maps them to status codes
 * in exactly one place, and clients switch on them. Never change a code's meaning.
 */

export const ERROR_CODES = [
  'VALIDATION_ERROR',
  'PAYLOAD_TOO_LARGE',
  'JOB_NOT_FOUND',
  'SCHEDULE_NOT_FOUND',
  'INVALID_TRANSITION',
  'OWNERSHIP_LOST',
  'NO_HANDLER',
  'HANDLER_TIMEOUT',
  'NON_RETRYABLE',
  'CANCELLED',
  'CONFIG_ERROR',
  'MIGRATION_ERROR',
  'DATABASE_ERROR',
  'UNAUTHENTICATED',
  'FORBIDDEN',
  'RATE_LIMITED',
  'NOT_READY',
  'CONFLICT',
  'INTERNAL',
] as const;

export type ErrorCode = (typeof ERROR_CODES)[number];

export abstract class PgJobqError extends Error {
  abstract readonly code: ErrorCode;
  /** Safe to return to an API caller. Errors default to unsafe. */
  readonly exposeDetails: boolean = false;
  readonly details?: Readonly<Record<string, unknown>>;

  constructor(message: string, options?: { cause?: unknown; details?: Record<string, unknown> }) {
    super(message, options?.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = new.target.name;
    if (options?.details) this.details = Object.freeze({ ...options.details });
    Error.captureStackTrace?.(this, new.target);
  }
}

export class ValidationError extends PgJobqError {
  readonly code = 'VALIDATION_ERROR' as const;
  override readonly exposeDetails = true;
}

export class PayloadTooLargeError extends PgJobqError {
  readonly code = 'PAYLOAD_TOO_LARGE' as const;
  override readonly exposeDetails = true;
}

export class JobNotFoundError extends PgJobqError {
  readonly code = 'JOB_NOT_FOUND' as const;
  override readonly exposeDetails = true;
  constructor(id: string) {
    super(`Job ${id} not found`, { details: { id } });
  }
}

export class ScheduleNotFoundError extends PgJobqError {
  readonly code = 'SCHEDULE_NOT_FOUND' as const;
  override readonly exposeDetails = true;
}

/**
 * A state transition the machine forbids. Names both the current state and the
 * permitted targets, so an operator gets an actionable message rather than a 409
 * with no explanation.
 */
export class InvalidTransitionError extends PgJobqError {
  readonly code = 'INVALID_TRANSITION' as const;
  override readonly exposeDetails = true;
  constructor(from: string, to: string, permitted: readonly string[]) {
    super(
      `Cannot transition from '${from}' to '${to}'. Permitted from '${from}': ${
        permitted.length > 0 ? permitted.join(', ') : '(none — terminal state)'
      }`,
      { details: { from, to, permitted } },
    );
  }
}

/**
 * The worker no longer owns this job: its lease expired and the reaper recovered
 * it, or an operator intervened.
 *
 * This is a NORMAL outcome under partition, not a bug and not retryable. The
 * worker must discard its result. Forcing the write would mark a job done that
 * another worker is currently executing.
 */
export class OwnershipLostError extends PgJobqError {
  readonly code = 'OWNERSHIP_LOST' as const;
  constructor(jobId: string, workerId: string, attempt: number) {
    super(
      `Lost ownership of job ${jobId} (worker=${workerId}, attempt=${attempt}); ` +
        `the lease expired or the job was reassigned`,
      { details: { jobId, workerId, attempt } },
    );
  }
}

export class NoHandlerError extends PgJobqError {
  readonly code = 'NO_HANDLER' as const;
  constructor(type: string) {
    super(`No handler registered for job type '${type}'`, { details: { type } });
  }
}

export class HandlerTimeoutError extends PgJobqError {
  readonly code = 'HANDLER_TIMEOUT' as const;
  constructor(jobId: string, timeoutMs: number) {
    super(`Handler for job ${jobId} exceeded ${timeoutMs}ms`, { details: { jobId, timeoutMs } });
  }
}

/**
 * Thrown by a handler to dead-letter a job immediately, skipping remaining
 * attempts. For failures where retrying cannot help: a malformed payload, a
 * permanently deleted resource, a rejected-by-policy request.
 */
export class NonRetryableError extends PgJobqError {
  readonly code = 'NON_RETRYABLE' as const;
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
  }
}

export class JobCancelledError extends PgJobqError {
  readonly code = 'CANCELLED' as const;
}

export class ConfigError extends PgJobqError {
  readonly code = 'CONFIG_ERROR' as const;
}

export class MigrationError extends PgJobqError {
  readonly code = 'MIGRATION_ERROR' as const;
}

export class DatabaseError extends PgJobqError {
  readonly code = 'DATABASE_ERROR' as const;
}

export class ConflictError extends PgJobqError {
  readonly code = 'CONFLICT' as const;
  override readonly exposeDetails = true;
}

/**
 * Signals a retry with an explicit delay, overriding the computed backoff.
 * Use for a `Retry-After` from a rate-limited upstream.
 */
export class RetryAfterError extends PgJobqError {
  readonly code = 'NON_RETRYABLE' as const;
  readonly retryDelayMs: number;
  constructor(message: string, retryDelayMs: number, options?: { cause?: unknown }) {
    super(message, options);
    this.retryDelayMs = retryDelayMs;
  }
}

export function isPgJobqError(e: unknown): e is PgJobqError {
  return e instanceof PgJobqError;
}

/**
 * True when a handler's thrown value means "do not retry this job".
 */
export function isNonRetryable(e: unknown): boolean {
  return e instanceof NonRetryableError;
}

/**
 * Normalizes anything a handler might throw into a structured record.
 *
 * Handlers throw non-Error values in practice: a bare string, or `undefined` from
 * `Promise.reject()` with no argument. Without normalization those become
 * unhandled rejections that take down the worker instead of failing one job.
 */
export function normalizeError(
  e: unknown,
  attempt: number,
  kind: 'handler_error' | 'timeout' | 'validation_error' | 'no_handler' = 'handler_error',
  maxTextBytes = 4096,
): {
  name: string;
  message: string;
  stack: string;
  kind: typeof kind;
  attempt: number;
  at: string;
} {
  let name: string;
  let message: string;
  let stack = '';

  if (e instanceof Error) {
    name = e.name || 'Error';
    message = e.message || String(e);
    stack = e.stack ?? '';
  } else if (typeof e === 'string') {
    name = 'ThrownString';
    message = e;
  } else if (e === undefined) {
    name = 'ThrownUndefined';
    message = 'Handler rejected with no reason';
  } else if (e === null) {
    name = 'ThrownNull';
    message = 'Handler rejected with null';
  } else {
    name = 'ThrownValue';
    // Never String(e) as a fallback: on a plain object that yields '[object Object]',
    // which tells a future debugger nothing. Report the type instead.
    try {
      message = JSON.stringify(e) ?? `[${typeof e} with no JSON representation]`;
    } catch {
      message = `[unserializable ${typeof e}, possibly circular]`;
    }
  }

  return {
    name: truncate(name, 256),
    message: truncate(message, maxTextBytes),
    stack: truncate(stack, maxTextBytes),
    kind,
    attempt,
    at: new Date().toISOString(),
  };
}

/** Truncates by UTF-8 byte length, not code units, so the DB limit is respected. */
export function truncate(s: string, maxBytes: number): string {
  if (s.length === 0) return s;
  const buf = Buffer.from(s, 'utf8');
  if (buf.byteLength <= maxBytes) return s;
  const marker = '…[truncated]';
  const markerBytes = Buffer.byteLength(marker, 'utf8');
  const room = Math.max(0, maxBytes - markerBytes);
  // Slicing mid-codepoint is possible; toString replaces the partial sequence
  // rather than throwing, then we strip the replacement char.
  return (
    buf
      .subarray(0, room)
      .toString('utf8')
      .replace(/\uFFFD$/, '') + marker
  );
}
