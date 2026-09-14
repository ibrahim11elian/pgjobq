/**
 * Configuration, validated once at startup.
 *
 * Every problem is reported at once rather than one per restart, and production
 * mode refuses to start without an API credential — a public demo with a default
 * password is the likeliest way this project would leak.
 */
import { z } from 'zod';
import { ConfigError } from './errors.js';
import { JITTER_STRATEGIES } from './types.js';

const bool = z
  .union([z.boolean(), z.enum(['true', 'false', '1', '0'])])
  .transform((v) => v === true || v === 'true' || v === '1');

const int = (min: number, max: number) => z.coerce.number().int().min(min).max(max);

export const configSchema = z.object({
  nodeEnv: z.enum(['development', 'test', 'production']).default('development'),

  databaseUrl: z.string().min(1, 'DATABASE_URL is required'),
  dbPoolMax: int(1, 500).default(10),
  dbPoolMin: int(0, 500).default(0),
  dbStatementTimeoutMs: int(100, 600_000).default(10_000),
  dbConnectTimeoutMs: int(100, 120_000).default(10_000),

  logLevel: z.enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal', 'silent']).default('info'),
  logPayloads: bool.default(false),

  workerQueues: z
    .string()
    .default('default')
    .transform((s) =>
      s
        .split(',')
        .map((q) => q.trim())
        .filter((q) => q.length > 0),
    ),
  workerConcurrency: int(1, 10_000).default(10),
  workerClaimBatchMax: int(1, 1000).default(20),
  workerPollIntervalMs: int(10, 300_000).default(1000),
  workerJobTimeoutMs: int(100, 86_400_000).default(300_000),
  workerShutdownGraceMs: int(0, 600_000).default(30_000),
  workerHeartbeatFraction: z.coerce.number().min(0.05).max(0.9).default(0.5),

  defaultMaxAttempts: int(1, 1000).default(5),
  defaultLeaseSeconds: int(1, 86_400).default(30),
  maxPayloadBytes: int(1, 10_485_760).default(262_144),

  backoffInitialMs: int(0, 3_600_000).default(1000),
  backoffMultiplier: z.coerce.number().min(1).max(100).default(2),
  backoffMaxMs: int(0, 604_800_000).default(3_600_000),
  backoffJitter: z.enum(JITTER_STRATEGIES).default('full'),
  maxErrorHistory: int(1, 100).default(5),
  maxErrorTextBytes: int(64, 1_048_576).default(4096),

  reaperEnabled: bool.default(true),
  reaperIntervalMs: int(100, 3_600_000).default(5000),
  reaperBatchMax: int(1, 100_000).default(1000),

  schedulerEnabled: bool.default(true),
  schedulerIntervalMs: int(100, 3_600_000).default(5000),
  schedulerBatchMax: int(1, 10_000).default(100),

  retentionEnabled: bool.default(true),
  retentionIntervalMs: int(1000, 86_400_000).default(60_000),
  retentionBatchMax: int(1, 100_000).default(1000),
  retentionArchive: bool.default(false),

  port: int(1, 65_535).default(3000),
  host: z.string().default('0.0.0.0'),
  apiBootstrapKey: z.string().optional(),
  rateLimitWindowMs: int(1000, 3_600_000).default(60_000),
  rateLimitMax: int(1, 1_000_000).default(600),
  sseMaxSubscribers: int(1, 10_000).default(50),
  corsOrigin: z.string().default('http://localhost:5173'),
});

export type Config = z.infer<typeof configSchema>;

const ENV_MAP: Record<keyof Config, string> = {
  nodeEnv: 'NODE_ENV',
  databaseUrl: 'DATABASE_URL',
  dbPoolMax: 'DB_POOL_MAX',
  dbPoolMin: 'DB_POOL_MIN',
  dbStatementTimeoutMs: 'DB_STATEMENT_TIMEOUT_MS',
  dbConnectTimeoutMs: 'DB_CONNECT_TIMEOUT_MS',
  logLevel: 'LOG_LEVEL',
  logPayloads: 'LOG_PAYLOADS',
  workerQueues: 'WORKER_QUEUES',
  workerConcurrency: 'WORKER_CONCURRENCY',
  workerClaimBatchMax: 'WORKER_CLAIM_BATCH_MAX',
  workerPollIntervalMs: 'WORKER_POLL_INTERVAL_MS',
  workerJobTimeoutMs: 'WORKER_JOB_TIMEOUT_MS',
  workerShutdownGraceMs: 'WORKER_SHUTDOWN_GRACE_MS',
  workerHeartbeatFraction: 'WORKER_HEARTBEAT_FRACTION',
  defaultMaxAttempts: 'DEFAULT_MAX_ATTEMPTS',
  defaultLeaseSeconds: 'DEFAULT_LEASE_SECONDS',
  maxPayloadBytes: 'MAX_PAYLOAD_BYTES',
  backoffInitialMs: 'BACKOFF_INITIAL_MS',
  backoffMultiplier: 'BACKOFF_MULTIPLIER',
  backoffMaxMs: 'BACKOFF_MAX_MS',
  backoffJitter: 'BACKOFF_JITTER',
  maxErrorHistory: 'MAX_ERROR_HISTORY',
  maxErrorTextBytes: 'MAX_ERROR_TEXT_BYTES',
  reaperEnabled: 'REAPER_ENABLED',
  reaperIntervalMs: 'REAPER_INTERVAL_MS',
  reaperBatchMax: 'REAPER_BATCH_MAX',
  schedulerEnabled: 'SCHEDULER_ENABLED',
  schedulerIntervalMs: 'SCHEDULER_INTERVAL_MS',
  schedulerBatchMax: 'SCHEDULER_BATCH_MAX',
  retentionEnabled: 'RETENTION_ENABLED',
  retentionIntervalMs: 'RETENTION_INTERVAL_MS',
  retentionBatchMax: 'RETENTION_BATCH_MAX',
  retentionArchive: 'RETENTION_ARCHIVE',
  port: 'PORT',
  host: 'HOST',
  apiBootstrapKey: 'API_BOOTSTRAP_KEY',
  rateLimitWindowMs: 'RATE_LIMIT_WINDOW_MS',
  rateLimitMax: 'RATE_LIMIT_MAX',
  sseMaxSubscribers: 'SSE_MAX_SUBSCRIBERS',
  corsOrigin: 'CORS_ORIGIN',
};

/**
 * Builds config from an environment map.
 *
 * Reports every invalid field at once. Discovering misconfiguration one variable
 * per restart is a miserable way to deploy.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const raw: Record<string, unknown> = {};
  for (const [key, envName] of Object.entries(ENV_MAP)) {
    const value = env[envName];
    if (value !== undefined && value !== '') raw[key] = value;
  }

  const parsed = configSchema.safeParse(raw);
  if (!parsed.success) {
    const problems = parsed.error.issues.map((issue) => {
      const field = String(issue.path[0] ?? '(root)');
      const envName = ENV_MAP[field as keyof Config] ?? field;
      return `  ${envName}: ${issue.message}`;
    });
    throw new ConfigError(
      `Invalid configuration (${problems.length} problem${problems.length === 1 ? '' : 's'}):\n${problems.join('\n')}`,
    );
  }

  const config = parsed.data;
  const extra = validateCrossField(config);
  if (extra.length > 0) {
    throw new ConfigError(
      `Invalid configuration (${extra.length} problem${extra.length === 1 ? '' : 's'}):\n${extra.map((e) => `  ${e}`).join('\n')}`,
    );
  }
  return config;
}

/**
 * Constraints that span more than one field, so Zod cannot express them per-field.
 */
function validateCrossField(c: Config): string[] {
  const problems: string[] = [];

  if (c.dbPoolMin > c.dbPoolMax) {
    problems.push(`DB_POOL_MIN (${c.dbPoolMin}) cannot exceed DB_POOL_MAX (${c.dbPoolMax})`);
  }

  if (c.backoffInitialMs > c.backoffMaxMs) {
    problems.push(
      `BACKOFF_INITIAL_MS (${c.backoffInitialMs}) cannot exceed BACKOFF_MAX_MS (${c.backoffMaxMs})`,
    );
  }

  // A worker needs a connection per concurrent claim-and-report cycle. Undersizing
  // the pool relative to concurrency turns into pool-wait latency that looks like
  // slow job execution and is very hard to diagnose from the outside.
  if (c.workerConcurrency > c.dbPoolMax * 10) {
    problems.push(
      `WORKER_CONCURRENCY (${c.workerConcurrency}) is more than 10x DB_POOL_MAX (${c.dbPoolMax}); ` +
        `handlers will queue on pool acquisition. Raise DB_POOL_MAX or lower concurrency.`,
    );
  }

  // Note: WORKER_CLAIM_BATCH_MAX may exceed WORKER_CONCURRENCY. It is only a
  // ceiling on the claim statement's LIMIT, and the pool always requests
  // min(freeSlots, batchMax), so a larger ceiling is a no-op rather than a bug.

  // The job timeout must fit inside the lease, extended by heartbeats. If the
  // handler can outlive its lease without heartbeating, the reaper hands the job
  // to a second worker while the first is still running it.
  const leaseMs = c.defaultLeaseSeconds * 1000;
  const heartbeatMs = leaseMs * c.workerHeartbeatFraction;
  if (heartbeatMs >= leaseMs) {
    problems.push(
      `WORKER_HEARTBEAT_FRACTION (${c.workerHeartbeatFraction}) must be < 1 so the lease is ` +
        `extended before it expires.`,
    );
  }

  if (c.nodeEnv === 'production') {
    if (!c.apiBootstrapKey || c.apiBootstrapKey.length === 0) {
      problems.push(
        'API_BOOTSTRAP_KEY is required when NODE_ENV=production. Refusing to start an ' +
          'unauthenticated queue.',
      );
    } else if (c.apiBootstrapKey.length < 32) {
      problems.push(
        `API_BOOTSTRAP_KEY must be at least 32 characters in production (got ${c.apiBootstrapKey.length}).`,
      );
    } else if (/dev|test|change.?me|insecure|example|password|secret/i.test(c.apiBootstrapKey)) {
      problems.push('API_BOOTSTRAP_KEY looks like a placeholder. Generate a random key.');
    }
    if (c.logPayloads) {
      problems.push('LOG_PAYLOADS must be false in production: payloads may carry personal data.');
    }
  }

  return problems;
}

/** Redacts credentials from a connection string before it reaches a log. */
export function redactDatabaseUrl(url: string): string {
  try {
    const u = new URL(url);
    if (u.password) u.password = '***';
    return u.toString();
  } catch {
    return '[unparseable DATABASE_URL]';
  }
}
