/**
 * Retry backoff. Pure: no database, no clock, injectable randomness.
 *
 * Kept out of SQL deliberately — randomness inside a statement would be
 * untestable, and the delay is the one part of the failure path that benefits
 * from being verifiable in isolation.
 */
import type { JitterStrategy } from '../types.js';

export interface BackoffOptions {
  /** Delay before the first retry, in ms. */
  readonly initialMs: number;
  /** Growth factor per attempt. */
  readonly multiplier: number;
  /** Ceiling applied before jitter. */
  readonly maxMs: number;
  readonly jitter: JitterStrategy;
  /** Injectable for deterministic tests. Must return [0, 1). */
  readonly random?: () => number;
}

/**
 * Undecorated exponential delay for the given attempt, before jitter.
 *
 * @param attempt 1-based delivery number that just failed.
 */
export function baseDelayMs(attempt: number, opts: BackoffOptions): number {
  if (attempt < 1) return 0;
  const growth = Math.pow(opts.multiplier, attempt - 1);
  const raw = opts.initialMs * growth;
  // Guard against Infinity from a large multiplier and attempt count before
  // Math.min, which would otherwise propagate NaN through make_interval.
  if (!Number.isFinite(raw)) return opts.maxMs;
  return Math.min(opts.maxMs, raw);
}

/**
 * Delay before the next retry, jittered.
 *
 * Full jitter is the default because the failure that matters is correlated: when
 * a downstream dependency goes down, every in-flight job fails at nearly the same
 * instant. Deterministic backoff reschedules them all to the same future instant,
 * producing a synchronized burst that hits the dependency the moment it recovers
 * and knocks it over again. Drawing uniformly from [0, base] spreads them across
 * the whole window.
 *
 * Cost of full jitter: a lower mean delay and less predictable timing. For a queue
 * that is the right trade, since the objective is spreading load rather than
 * guaranteeing a minimum wait.
 */
export function nextDelayMs(attempt: number, opts: BackoffOptions): number {
  const base = baseDelayMs(attempt, opts);
  if (base <= 0) return 0;
  const rand = opts.random ?? Math.random;

  switch (opts.jitter) {
    case 'none':
      return Math.round(base);
    case 'equal':
      // Half fixed, half random: keeps a floor while still spreading.
      return Math.round(base / 2 + rand() * (base / 2));
    case 'full':
      return Math.round(rand() * base);
  }
}

/** Convenience wrapper returning seconds, which is what make_interval takes. */
export function nextDelaySeconds(attempt: number, opts: BackoffOptions): number {
  return nextDelayMs(attempt, opts) / 1000;
}

/**
 * Jitter for reaper-initiated recovery.
 *
 * Applied for the same reason as handler-failure jitter, and it matters more than
 * it appears: when a worker host dies, every job it held expires at nearly the same
 * moment. Un-jittered recovery would return them all to `available` with the same
 * run_at, producing exactly the thundering herd jitter exists to prevent — on a
 * system already down one host.
 */
export function reaperDelaySeconds(attempt: number, opts: BackoffOptions): number {
  return nextDelaySeconds(attempt, opts);
}

/**
 * The full retry schedule for an attempt budget, for documentation and for
 * sanity-checking a configuration before deploying it.
 */
export function describeSchedule(
  maxAttempts: number,
  opts: BackoffOptions,
): readonly { attempt: number; baseMs: number; worstCaseMs: number }[] {
  const out: { attempt: number; baseMs: number; worstCaseMs: number }[] = [];
  for (let a = 1; a < maxAttempts; a++) {
    const base = baseDelayMs(a, opts);
    out.push({ attempt: a, baseMs: base, worstCaseMs: opts.jitter === 'none' ? base : base });
  }
  return out;
}
