/**
 * Typed handler registry.
 *
 * One Zod schema per job type gives runtime validation AND static inference from a
 * single declaration, so a handler's payload is typed without a cast and `enqueue`
 * rejects a wrong payload shape at compile time as well as at runtime.
 */
import type { z } from 'zod';
import { NoHandlerError } from '../errors.js';
import type { JobContext, JobHandler, JsonValue } from '../types.js';

interface Registration {
  readonly type: string;
  readonly schema: z.ZodType<unknown>;
  readonly handler: JobHandler<never>;
}

/**
 * Maps job type -> payload type. Threaded through the builder so each `register`
 * call widens the map and `enqueue` can be checked against it.
 */
export type TypeMap = Record<string, unknown>;

export interface Registry<M extends TypeMap = Record<never, never>> {
  /**
   * Registers a handler for a job type.
   *
   * Returns a NEW registry type that includes this type, so the accumulated map is
   * available to callers for compile-time checking of enqueue payloads.
   */
  register<K extends string, S extends z.ZodType<unknown>>(
    type: K,
    schema: S,
    handler: JobHandler<z.output<S>>,
  ): Registry<M & Record<K, z.input<S>>>;

  has(type: string): boolean;
  types(): string[];

  /** @throws {NoHandlerError} when nothing is registered for `type`. */
  resolve(type: string): { schema: z.ZodType<unknown>; handler: JobHandler<never> };

  /**
   * Validates a raw payload against the registered schema.
   * @throws {NoHandlerError} or a Zod error, which the runner converts to a
   * non-retryable failure — a payload that does not match its schema will not start
   * matching on a retry.
   */
  parse(type: string, payload: JsonValue): unknown;

  run(type: string, payload: JsonValue, ctx: JobContext): Promise<void>;
}

class RegistryImpl<M extends TypeMap> implements Registry<M> {
  private readonly entries = new Map<string, Registration>();

  register<K extends string, S extends z.ZodType<unknown>>(
    type: K,
    schema: S,
    handler: JobHandler<z.output<S>>,
  ): Registry<M & Record<K, z.input<S>>> {
    if (this.entries.has(type)) {
      throw new Error(
        `Handler for job type '${type}' is already registered. Registering twice is ` +
          `almost always a copy-paste error, and silently replacing would make which ` +
          `handler runs depend on module load order.`,
      );
    }
    this.entries.set(type, {
      type,
      schema,
      handler: handler,
    });
    return this;
  }

  has(type: string): boolean {
    return this.entries.has(type);
  }

  types(): string[] {
    return [...this.entries.keys()].sort();
  }

  resolve(type: string): { schema: z.ZodType<unknown>; handler: JobHandler<never> } {
    const entry = this.entries.get(type);
    if (!entry) throw new NoHandlerError(type);
    return { schema: entry.schema, handler: entry.handler };
  }

  parse(type: string, payload: JsonValue): unknown {
    const { schema } = this.resolve(type);
    return schema.parse(payload);
  }

  async run(type: string, payload: JsonValue, ctx: JobContext): Promise<void> {
    const { schema, handler } = this.resolve(type);
    const parsed = schema.parse(payload);
    await (handler as JobHandler<unknown>)(parsed, ctx);
  }
}

export function createRegistry(): Registry {
  return new RegistryImpl<Record<never, never>>();
}

/** Extracts the accumulated type map, for typing an enqueue helper against a registry. */
export type RegistryTypes<R> = R extends Registry<infer M> ? M : never;
