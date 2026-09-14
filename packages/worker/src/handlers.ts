/**
 * Demo handlers.
 *
 * These exist so the project is runnable and demoable out of the box. In real use a
 * host application registers its own handlers and calls `startWorker`.
 *
 * Each one illustrates a different property of the engine, and `demo.flaky` is the
 * one to read: it shows how a handler makes itself safe under at-least-once delivery.
 */
import { z } from 'zod';
import { createRegistry, NonRetryableError, RetryAfterError, type Registry } from '@pgjobq/core';

/** Tracks side effects so the demo can show idempotency working. */
const effects = new Map<string, number>();

export function effectCount(key: string): number {
  return effects.get(key) ?? 0;
}

function recordEffect(key: string): boolean {
  const seen = effects.has(key);
  effects.set(key, (effects.get(key) ?? 0) + 1);
  return !seen;
}

export function createDemoRegistry(): Registry {
  return (
    createRegistry()
      /**
       * Baseline: succeeds after a short delay, respecting its abort signal.
       */
      .register(
        'demo.email',
        z.object({
          to: z.string().email(),
          subject: z.string().min(1),
          body: z.string().optional(),
        }),
        async (payload, ctx) => {
          ctx.logger.info({ to: payload.to }, 'sending email');
          await delay(150, ctx.signal);
          if (ctx.signal.aborted) throw new Error('aborted before send completed');
          ctx.logger.info('email sent');
        },
      )

      /**
       * Idempotent work under at-least-once delivery.
       *
       * Fails on the first two attempts, then succeeds. The side effect is keyed on the
       * JOB ID, not the attempt, so re-delivery cannot double-apply it. This is the
       * pattern the delivery guarantee asks handlers to follow.
       */
      .register(
        'demo.flaky',
        z.object({ failUntilAttempt: z.number().int().min(1).default(3) }),
        async (payload, ctx) => {
          const wasFirst = recordEffect(`flaky:${ctx.id}`);
          ctx.logger.info(
            { attempt: ctx.attempt, effectApplied: wasFirst },
            wasFirst ? 'applying side effect' : 'side effect already applied; skipping',
          );

          if (ctx.attempt < payload.failUntilAttempt) {
            throw new Error(
              `simulated transient failure on attempt ${ctx.attempt} of ${ctx.maxAttempts}`,
            );
          }
          await delay(50, ctx.signal);
        },
      )

      /**
       * Long-running work that outlives a single lease.
       *
       * Heartbeats explicitly so the reaper does not recover a job that is progressing
       * normally. Without this, anything slower than lease_seconds would be handed to a
       * second worker while the first was still running it.
       */
      .register(
        'demo.long',
        z.object({ seconds: z.number().int().min(1).max(600).default(45) }),
        async (payload, ctx) => {
          const steps = payload.seconds;
          for (let i = 0; i < steps; i++) {
            if (ctx.signal.aborted) {
              ctx.logger.warn({ completedSteps: i }, 'aborted mid-work');
              return;
            }
            await delay(1000, ctx.signal);
            // Holds the lease across a slow section.
            await ctx.heartbeat();
            if (i % 10 === 0) ctx.logger.info({ step: i, of: steps }, 'progress');
          }
        },
      )

      /**
       * A failure retrying cannot fix.
       *
       * Dead-letters immediately rather than burning the remaining attempts on work
       * that will fail identically every time.
       */
      .register(
        'demo.poison',
        z.object({ reason: z.string().default('malformed input') }),
        (payload) => {
          throw new NonRetryableError(`Cannot process: ${payload.reason}`);
        },
      )

      /**
       * Honouring an upstream Retry-After.
       *
       * Overrides the computed backoff with the delay the dependency asked for, which is
       * the correct response to a rate limit — guessing shorter just gets throttled again.
       */
      .register(
        'demo.throttled',
        z.object({ retryAfterSeconds: z.number().int().min(1).max(3600).default(30) }),
        (payload, ctx) => {
          if (ctx.attempt < 2) {
            throw new RetryAfterError('upstream returned 429', payload.retryAfterSeconds * 1000);
          }
        },
      )

      /** Deliberately ignores its abort signal, to demonstrate the timeout path. */
      .register('demo.hang', z.object({}), async () => {
        await new Promise((resolve) => setTimeout(resolve, 10 * 60 * 1000));
      })
  );
}

/** Abortable sleep, so a handler stops promptly on timeout or cancellation. */
function delay(ms: number, signal?: AbortSignal): Promise<void> {
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
