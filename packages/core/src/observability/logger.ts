/**
 * Structured logging with redaction on by default.
 *
 * Payloads are untrusted input and routinely carry personal data — an email
 * address, an account number, a document body. Logging them by default would turn
 * every log sink into an uncontrolled copy of that data, so it takes an explicit
 * opt-in.
 */
import {
  pino,
  transport as pinoTransport,
  stdTimeFunctions,
  type Logger as PinoLogger,
  type LoggerOptions,
} from 'pino';
import type { Logger } from '../types.js';

export interface LoggerConfig {
  readonly level?: string;
  readonly logPayloads?: boolean;
  readonly pretty?: boolean;
  readonly name?: string;
}

/**
 * Paths redacted unless payload logging is explicitly enabled.
 *
 * Covers the shapes a payload actually appears in across this codebase, plus the
 * credential-bearing fields that must never be logged regardless of the setting.
 */
const PAYLOAD_PATHS = [
  'payload',
  '*.payload',
  'job.payload',
  'jobs[*].payload',
  'metadata',
  '*.metadata',
  'job.metadata',
];

const ALWAYS_REDACT = [
  'password',
  '*.password',
  'apiKey',
  '*.apiKey',
  'api_key',
  '*.api_key',
  'token',
  '*.token',
  'secret',
  '*.secret',
  'authorization',
  '*.authorization',
  'req.headers.authorization',
  'req.headers.cookie',
  'connectionString',
  '*.connectionString',
  'databaseUrl',
  '*.databaseUrl',
  'DATABASE_URL',
];

export function createLogger(config: LoggerConfig = {}): Logger {
  const redactPaths = config.logPayloads ? ALWAYS_REDACT : [...ALWAYS_REDACT, ...PAYLOAD_PATHS];

  const options: LoggerOptions = {
    level: config.level ?? 'info',
    ...(config.name !== undefined ? { name: config.name } : {}),
    redact: {
      paths: redactPaths,
      censor: '[redacted]',
      remove: false,
    },
    // ISO timestamps rather than epoch millis: logs are read by humans during an
    // incident more often than they are parsed by machines.
    timestamp: stdTimeFunctions.isoTime,
    formatters: {
      level: (label) => ({ level: label }),
    },
    // null, not undefined: pino distinguishes "no base bindings" (null) from
    // "use the default pid/hostname bindings" (absent).
    base: null,
  };

  const logger: PinoLogger = config.pretty
    ? pino(
        options,
        pinoTransport({
          target: 'pino-pretty',
          options: { colorize: true, translateTime: 'HH:MM:ss.l', ignore: 'pid,hostname' },
        }),
      )
    : pino(options);

  return logger;
}

/** A logger that discards everything. For embedding contexts that supply their own. */
export function nullLogger(): Logger {
  const noop = (): void => undefined;
  const l: Logger = {
    debug: noop,
    info: noop,
    warn: noop,
    error: noop,
    child: () => l,
  };
  return l;
}

/**
 * Binds a job's identifiers to a logger, so every line a handler emits is
 * correlatable without the handler having to remember to include them.
 */
export function jobLogger(
  parent: Logger,
  job: { id: string; queue: string; type: string; attempt: number },
  workerId: string,
): Logger {
  return parent.child({
    jobId: job.id,
    queue: job.queue,
    jobType: job.type,
    attempt: job.attempt,
    workerId,
  });
}
