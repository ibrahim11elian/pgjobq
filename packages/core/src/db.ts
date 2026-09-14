/**
 * Database access.
 *
 * Parameterized statements only. The `sql` tag exists so that passing an
 * interpolated string is a type error rather than a code-review question — job
 * payloads are untrusted input and must never reach a statement as text.
 */
import pg from 'pg';
import { DatabaseError } from './errors.js';
import type { Logger } from './types.js';

const { Pool, types } = pg;

/**
 * bigint (OID 20) arrives as a string by default because it can exceed
 * Number.MAX_SAFE_INTEGER. We keep it a string throughout rather than risk a
 * silent precision loss on job IDs, so IDs are `string` in every public type.
 */
types.setTypeParser(20, (v: string) => v);

/** numeric (OID 1700) likewise stays a string. */
types.setTypeParser(1700, (v: string) => v);

export interface DbOptions {
  readonly connectionString: string;
  readonly max?: number;
  readonly min?: number;
  readonly statementTimeoutMs?: number;
  readonly connectTimeoutMs?: number;
  readonly applicationName?: string;
  readonly logger?: Logger;
}

/**
 * A statement plus its parameters. Constructed only by {@link sql}, so a raw
 * string cannot be passed where a query is expected.
 */
export interface Statement {
  readonly text: string;
  readonly values: readonly unknown[];
  readonly name?: string;
}

declare const StatementBrand: unique symbol;
export type SafeStatement = Statement & { readonly [StatementBrand]: true };

/**
 * Builds a parameterized statement.
 *
 * Values are always sent out of band as bind parameters; nothing is interpolated
 * into the SQL text.
 */
export function sql(text: string, values: readonly unknown[] = [], name?: string): SafeStatement {
  return { text, values, ...(name !== undefined ? { name } : {}) } as SafeStatement;
}

export interface QueryResult<R> {
  readonly rows: R[];
  readonly rowCount: number;
}

export interface Queryable {
  query<R extends pg.QueryResultRow>(statement: SafeStatement): Promise<QueryResult<R>>;
}

export class Db implements Queryable {
  readonly pool: pg.Pool;
  private readonly logger: Logger | undefined;
  private closed = false;

  constructor(opts: DbOptions) {
    this.logger = opts.logger;
    this.pool = new Pool({
      connectionString: opts.connectionString,
      max: opts.max ?? 10,
      min: opts.min ?? 0,
      connectionTimeoutMillis: opts.connectTimeoutMs ?? 10_000,
      application_name: opts.applicationName ?? 'pgjobq',
      // Bounds the blast radius of a pathological statement. The claim path is
      // designed to be fast; anything hitting this timeout is a bug worth failing on.
      statement_timeout: opts.statementTimeoutMs ?? 10_000,
      allowExitOnIdle: false,
    });

    // An idle client erroring (server restart, network drop) must not become an
    // unhandled 'error' event and take down the process.
    this.pool.on('error', (err) => {
      this.logger?.warn(
        { err: { name: err.name, message: err.message } },
        'idle database client error',
      );
    });
  }

  async query<R extends pg.QueryResultRow>(statement: SafeStatement): Promise<QueryResult<R>> {
    if (this.closed) throw new DatabaseError('Database pool is closed');
    try {
      const res = await this.pool.query<R>({
        text: statement.text,
        values: statement.values as unknown[],
        ...(statement.name !== undefined ? { name: statement.name } : {}),
      });
      return { rows: res.rows, rowCount: res.rowCount ?? 0 };
    } catch (e) {
      throw wrapDbError(e, statement);
    }
  }

  /**
   * Runs a callback inside a transaction.
   *
   * Used only where genuinely needed. The claim path deliberately does NOT use
   * this: it is a single statement in an implicit transaction, so no lock is held
   * across a round trip. See design.md section 4.
   */
  async withTransaction<T>(fn: (tx: Queryable) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const tx: Queryable = {
        query: async <R extends pg.QueryResultRow>(s: SafeStatement) => {
          try {
            const res = await client.query<R>({
              text: s.text,
              values: s.values as unknown[],
            });
            return { rows: res.rows, rowCount: res.rowCount ?? 0 };
          } catch (e) {
            throw wrapDbError(e, s);
          }
        },
      };
      const result = await fn(tx);
      await client.query('COMMIT');
      return result;
    } catch (e) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // A failed rollback means the connection is already unusable; the original
        // error is the one worth propagating.
      }
      throw e;
    } finally {
      client.release();
    }
  }

  /**
   * Checks in a way that fails fast and cheap, for the readiness probe.
   */
  async ping(): Promise<void> {
    await this.query(sql('SELECT 1'));
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.pool.end();
  }

  get stats(): { total: number; idle: number; waiting: number } {
    return {
      total: this.pool.totalCount,
      idle: this.pool.idleCount,
      waiting: this.pool.waitingCount,
    };
  }
}

/** Postgres error codes we act on rather than merely report. */
export const PG_CODES = {
  UNIQUE_VIOLATION: '23505',
  CHECK_VIOLATION: '23514',
  FOREIGN_KEY_VIOLATION: '23503',
  SERIALIZATION_FAILURE: '40001',
  DEADLOCK_DETECTED: '40P01',
  QUERY_CANCELED: '57014',
  ADMIN_SHUTDOWN: '57P01',
  CANNOT_CONNECT_NOW: '57P03',
  UNDEFINED_TABLE: '42P01',
} as const;

export function pgErrorCode(e: unknown): string | undefined {
  if (e !== null && typeof e === 'object' && 'code' in e) {
    const code = (e as { code?: unknown }).code;
    if (typeof code === 'string') return code;
  }
  return undefined;
}

export function pgConstraintName(e: unknown): string | undefined {
  if (e !== null && typeof e === 'object' && 'constraint' in e) {
    const c = (e as { constraint?: unknown }).constraint;
    if (typeof c === 'string') return c;
  }
  return undefined;
}

/** True when the failure is worth retrying at the connection level. */
export function isTransientDbError(e: unknown): boolean {
  const code = pgErrorCode(e);
  if (code !== undefined) {
    return (
      code === PG_CODES.SERIALIZATION_FAILURE ||
      code === PG_CODES.DEADLOCK_DETECTED ||
      code === PG_CODES.ADMIN_SHUTDOWN ||
      code === PG_CODES.CANNOT_CONNECT_NOW
    );
  }
  if (e instanceof Error) {
    return /ECONNREFUSED|ECONNRESET|ETIMEDOUT|EPIPE|EHOSTUNREACH|ENOTFOUND|terminating connection/i.test(
      e.message,
    );
  }
  return false;
}

function wrapDbError(e: unknown, statement: SafeStatement): unknown {
  // Unique violations are load-bearing control flow for enqueue idempotency and
  // for the scheduler's deterministic key, so they pass through untouched for the
  // caller to interpret.
  if (pgErrorCode(e) === PG_CODES.UNIQUE_VIOLATION) return e;
  if (e instanceof Error) {
    // The statement text is safe to include; the values are not, since they can
    // contain payload data.
    return new DatabaseError(`${e.message} [statement: ${firstLine(statement.text)}]`, {
      cause: e,
    });
  }
  return e;
}

function firstLine(s: string): string {
  const line = s.trim().split('\n')[0] ?? '';
  return line.length > 120 ? `${line.slice(0, 117)}...` : line;
}

/**
 * Connects with capped exponential backoff.
 *
 * A worker that cannot reach the database should report unhealthy and keep trying,
 * not exit immediately — an orchestrator restarting it in a crash loop is worse
 * than a process waiting out a brief outage.
 */
export async function waitForDatabase(
  db: Db,
  opts: { maxAttempts?: number; initialMs?: number; maxMs?: number; logger?: Logger } = {},
): Promise<void> {
  const maxAttempts = opts.maxAttempts ?? 30;
  const initialMs = opts.initialMs ?? 250;
  const maxMs = opts.maxMs ?? 5000;

  let lastError: unknown;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await db.ping();
      return;
    } catch (e) {
      lastError = e;

      // Only connectivity failures are worth waiting out. Retrying a wrong
      // password or a missing database 20 times would bury the real cause under
      // a generic "unreachable" after 30 seconds of silence.
      if (!isTransientDbError(unwrapCause(e))) {
        throw new DatabaseError(
          `Cannot connect to the database: ${errorMessage(e)}. This is not a transient ` +
            `connectivity error, so retrying will not help. Check DATABASE_URL, credentials, ` +
            `and that the target database exists.`,
          { cause: e },
        );
      }

      if (attempt === maxAttempts) break;
      const delay = Math.min(maxMs, initialMs * Math.pow(2, attempt - 1));
      // Jittered, so a fleet restarting together does not reconnect in lockstep.
      const jittered = Math.round(Math.random() * delay);
      opts.logger?.warn(
        { attempt, maxAttempts, retryInMs: jittered, reason: errorMessage(e) },
        'database unreachable, retrying',
      );
      await sleep(jittered);
    }
  }
  throw new DatabaseError(
    `Database unreachable after ${maxAttempts} attempts: ${errorMessage(lastError)}`,
    { cause: lastError },
  );
}

/** Reaches through the DatabaseError wrapper to the driver's original error. */
function unwrapCause(e: unknown): unknown {
  if (e instanceof DatabaseError && e.cause !== undefined) return e.cause;
  return e;
}

function errorMessage(e: unknown): string {
  if (e instanceof Error) {
    const code = pgErrorCode(unwrapCause(e));
    return code !== undefined ? `${e.message} (pg code ${code})` : e.message;
  }
  return String(e);
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      resolve();
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
