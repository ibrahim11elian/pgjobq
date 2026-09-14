/**
 * Middleware: authentication, authorization, validation, and error mapping.
 *
 * Status-code selection lives HERE and nowhere else. A route that invents its own
 * mapping is how an API ends up returning 500 for a validation error in one place and
 * 400 in another.
 */
import type { NextFunction, Request, RequestHandler, Response } from 'express';
import { ZodError, type ZodType } from 'zod';
import {
  ConflictError,
  DatabaseError,
  HandlerTimeoutError,
  InvalidTransitionError,
  JobNotFoundError,
  MigrationError,
  PayloadTooLargeError,
  ScheduleNotFoundError,
  ValidationError,
  isPgJobqError,
  type ErrorCode,
  type Logger,
} from '@pgjobq/core';
import { authorizes, type ApiKeyRecord, type Authenticator, type Scope } from './auth.js';

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Express {
    interface Request {
      apiKey?: ApiKeyRecord;
      validated?: { body?: unknown; query?: unknown; params?: unknown };
    }
  }
}

/** Maps a domain error code to a status. One place, so it cannot diverge. */
const STATUS_BY_CODE: Record<ErrorCode, number> = {
  VALIDATION_ERROR: 400,
  PAYLOAD_TOO_LARGE: 413,
  JOB_NOT_FOUND: 404,
  SCHEDULE_NOT_FOUND: 404,
  INVALID_TRANSITION: 409,
  CONFLICT: 409,
  OWNERSHIP_LOST: 409,
  NO_HANDLER: 422,
  HANDLER_TIMEOUT: 504,
  NON_RETRYABLE: 422,
  CANCELLED: 409,
  CONFIG_ERROR: 500,
  MIGRATION_ERROR: 503,
  DATABASE_ERROR: 503,
  UNAUTHENTICATED: 401,
  FORBIDDEN: 403,
  RATE_LIMITED: 429,
  NOT_READY: 503,
  INTERNAL: 500,
};

export function requireAuth(auth: Authenticator): RequestHandler {
  return (req: Request, res: Response, next: NextFunction) => {
    const header = req.get('authorization');
    if (header === undefined || !header.startsWith('Bearer ')) {
      res.status(401).json({
        error: {
          code: 'UNAUTHENTICATED',
          message: 'Provide an API key as: Authorization: Bearer <key>',
        },
      });
      return;
    }

    const presented = header.slice('Bearer '.length).trim();
    void auth
      .authenticate(presented)
      .then((record) => {
        if (!record) {
          // Deliberately identical to the missing-header response: distinguishing
          // "no key" from "bad key" tells an attacker their key format was right.
          res.status(401).json({
            error: { code: 'UNAUTHENTICATED', message: 'Invalid or expired API key' },
          });
          return;
        }
        req.apiKey = record;
        next();
      })
      .catch(next);
  };
}

/**
 * Requires a scope, and optionally that the key covers the target queue.
 *
 * A 403 deliberately does NOT reveal whether the resource exists — otherwise the
 * error becomes an enumeration oracle for queue names.
 */
export function requireScope(
  scope: Scope,
  queueFrom?: 'params' | 'body' | 'query',
): RequestHandler {
  return (req: Request, res: Response, next: NextFunction) => {
    const record = req.apiKey;
    if (!record) {
      res
        .status(401)
        .json({ error: { code: 'UNAUTHENTICATED', message: 'Authentication required' } });
      return;
    }

    let queue: string | undefined;
    if (queueFrom === 'params') queue = asString(req.params['queue']);
    else if (queueFrom === 'body')
      queue = asString((req.body as Record<string, unknown> | undefined)?.['queue']);
    else if (queueFrom === 'query') queue = asString(req.query['queue']);

    if (!authorizes(record, scope, queue)) {
      res.status(403).json({
        error: {
          code: 'FORBIDDEN',
          message: `This key lacks the '${scope}' scope${queue !== undefined ? ` for queue '${queue}'` : ''}`,
        },
      });
      return;
    }
    next();
  };
}

export interface ValidationTargets {
  readonly body?: ZodType;
  readonly query?: ZodType;
  readonly params?: ZodType;
}

/**
 * Validates and REPLACES the request parts with parsed values.
 *
 * Downstream handlers then work with coerced, typed data rather than re-parsing
 * strings. Reports every invalid field at once, not just the first.
 */
export function validate(targets: ValidationTargets): RequestHandler {
  return (req: Request, res: Response, next: NextFunction) => {
    const problems: { field: string; message: string }[] = [];
    const validated: { body?: unknown; query?: unknown; params?: unknown } = {};

    for (const part of ['body', 'query', 'params'] as const) {
      const schema = targets[part];
      if (!schema) continue;
      const result = schema.safeParse(req[part]);
      if (result.success) {
        validated[part] = result.data;
      } else {
        for (const issue of result.error.issues) {
          problems.push({
            field: `${part}.${issue.path.join('.') || '(root)'}`,
            message: issue.message,
          });
        }
      }
    }

    if (problems.length > 0) {
      res.status(400).json({
        error: {
          code: 'VALIDATION_ERROR',
          message: `Request validation failed with ${problems.length} problem(s)`,
          details: { problems },
        },
      });
      return;
    }

    req.validated = validated;
    next();
  };
}

/** Typed accessors for validated parts, so routes need no casts. */
export function body<T>(req: Request): T {
  return req.validated?.body as T;
}
export function query<T>(req: Request): T {
  return req.validated?.query as T;
}
export function params<T>(req: Request): T {
  return req.validated?.params as T;
}

/**
 * The single error-to-response mapper.
 *
 * Only errors marked as safe expose their details. Everything else returns a generic
 * message, because an unexpected error's message can carry a connection string, a SQL
 * fragment, or payload data.
 */
export function errorHandler(
  logger: Logger,
): (err: unknown, req: Request, res: Response, next: NextFunction) => void {
  return (err: unknown, req: Request, res: Response, next: NextFunction) => {
    if (res.headersSent) {
      next(err);
      return;
    }

    if (err instanceof ZodError) {
      res.status(400).json({
        error: {
          code: 'VALIDATION_ERROR',
          message: 'Request validation failed',
          details: {
            problems: err.issues.map((i) => ({
              field: i.path.join('.') || '(root)',
              message: i.message,
            })),
          },
        },
      });
      return;
    }

    if (isPgJobqError(err)) {
      const status = STATUS_BY_CODE[err.code] ?? 500;
      if (status >= 500) {
        logger.error(
          { err: { code: err.code, message: err.message }, path: req.path },
          'request failed',
        );
      }
      res.status(status).json({
        error: {
          code: err.code,
          message:
            err.exposeDetails || status < 500
              ? err.message
              : 'The server encountered an internal error',
          ...(err.exposeDetails && err.details !== undefined ? { details: err.details } : {}),
        },
      });
      return;
    }

    // Express 5 surfaces a body-parser size error as a 413 with a `type` field.
    if (isEntityTooLarge(err)) {
      res.status(413).json({
        error: { code: 'PAYLOAD_TOO_LARGE', message: 'Request body exceeds the configured limit' },
      });
      return;
    }

    if (isBadJson(err)) {
      res.status(400).json({
        error: { code: 'VALIDATION_ERROR', message: 'Request body is not valid JSON' },
      });
      return;
    }

    logger.error(
      {
        err:
          err instanceof Error
            ? { name: err.name, message: err.message, stack: err.stack }
            : String(err),
        path: req.path,
        method: req.method,
      },
      'unhandled error',
    );
    res.status(500).json({
      error: { code: 'INTERNAL', message: 'The server encountered an internal error' },
    });
  };
}

export function notFoundHandler(): RequestHandler {
  return (req: Request, res: Response) => {
    res.status(404).json({
      error: { code: 'JOB_NOT_FOUND', message: `No route for ${req.method} ${req.path}` },
    });
  };
}

function isEntityTooLarge(e: unknown): boolean {
  return (
    e !== null &&
    typeof e === 'object' &&
    'type' in e &&
    (e as { type?: unknown }).type === 'entity.too.large'
  );
}

function isBadJson(e: unknown): boolean {
  return e instanceof SyntaxError && 'body' in e;
}

function asString(v: unknown): string | undefined {
  return typeof v === 'string' ? v : undefined;
}

/** Re-exported so routes can throw domain errors without importing core directly. */
export {
  ConflictError,
  DatabaseError,
  HandlerTimeoutError,
  InvalidTransitionError,
  JobNotFoundError,
  MigrationError,
  PayloadTooLargeError,
  ScheduleNotFoundError,
  ValidationError,
};
