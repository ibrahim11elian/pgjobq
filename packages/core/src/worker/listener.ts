/**
 * LISTEN/NOTIFY wakeup.
 *
 * NOTIFY IS AN OPTIMIZATION, NOT THE GUARANTEE. Notifications are delivered only to
 * currently connected listeners, so they are not durable: a notification emitted
 * while this connection is reconnecting is simply lost. Polling remains the
 * correctness-bearing path and this only removes latency from it. Never make
 * correctness depend on a notification arriving.
 */
import pg from 'pg';
import { sleep } from '../db.js';
import type { Metrics } from '../observability/metrics.js';
import type { Logger } from '../types.js';

const { Client: PgClient } = pg;

export interface ListenerOptions {
  readonly connectionString: string;
  readonly queues: readonly string[];
  readonly logger: Logger;
  readonly metrics: Metrics;
  readonly onNotify: (queue: string) => void;
  readonly reconnectInitialMs?: number;
  readonly reconnectMaxMs?: number;
}

/** Channel name for a queue. Must match the trigger function in the migration. */
export function channelFor(queue: string): string {
  return `pgjobq_${queue}`;
}

/**
 * Holds LISTEN on a DEDICATED long-lived connection.
 *
 * It cannot come from the claim pool: LISTEN registers session state, and a pooled
 * connection handed to someone else mid-session would lose it.
 *
 * DEPLOYMENT TRAP: this connection must bypass any transaction-mode pooler
 * (PgBouncer in transaction mode, Supabase's pooler port). Such a pooler multiplexes
 * sessions across backends, so LISTEN state does not survive between transactions and
 * notifications silently never arrive. The failure is invisible — the queue keeps
 * working via polling, just with poll-interval latency — which is exactly why it is
 * worth stating loudly.
 */
export class Listener {
  private client: pg.Client | undefined;
  private stopped = false;
  private connected = false;
  private attempt = 0;
  private loopPromise: Promise<void> | undefined;

  constructor(private readonly opts: ListenerOptions) {}

  get isConnected(): boolean {
    return this.connected;
  }

  start(): void {
    if (this.loopPromise) return;
    this.loopPromise = this.loop();
  }

  private async loop(): Promise<void> {
    while (!this.stopped) {
      try {
        await this.connectAndListen();
        // connectAndListen resolves only when the connection ends.
      } catch (e) {
        if (this.stopped) break;
        this.connected = false;
        this.attempt += 1;
        const delay = Math.min(
          this.opts.reconnectMaxMs ?? 30_000,
          (this.opts.reconnectInitialMs ?? 500) * Math.pow(2, Math.min(this.attempt - 1, 10)),
        );
        const jittered = Math.round(Math.random() * delay);
        this.opts.logger.warn(
          {
            attempt: this.attempt,
            retryInMs: jittered,
            reason: e instanceof Error ? e.message : String(e),
          },
          'notification listener disconnected; polling continues to serve jobs meanwhile',
        );
        await sleep(jittered);
      }
    }
  }

  private async connectAndListen(): Promise<void> {
    const client = new PgClient({
      connectionString: this.opts.connectionString,
      application_name: 'pgjobq-listener',
      // No statement_timeout: this session is idle by design, waiting for
      // notifications. A timeout here would kill it repeatedly.
    });
    this.client = client;

    const ended = new Promise<void>((resolve, reject) => {
      client.on('error', (err) => reject(err));
      client.on('end', () => resolve());
    });

    await client.connect();

    client.on('notification', (msg) => {
      // Channel names come back lowercased by Postgres unless quoted. Map back to
      // the configured queue name rather than trusting case.
      const queue = this.queueForChannel(msg.channel);
      if (queue === undefined) return;
      this.opts.metrics.notifyReceived.inc({ queue });
      this.opts.onNotify(queue);
    });

    for (const queue of this.opts.queues) {
      // LISTEN cannot take a bind parameter. Queue names are validated at enqueue
      // to [a-zA-Z0-9._:-]+ and re-validated here before being quoted as an
      // identifier, so nothing arbitrary can reach the statement.
      const channel = channelFor(queue);
      assertSafeChannel(channel);
      await client.query(`LISTEN "${channel}"`);
    }

    this.connected = true;
    this.attempt = 0;
    this.opts.logger.info({ queues: this.opts.queues }, 'listening for enqueue notifications');

    await ended;
    this.connected = false;
  }

  private queueForChannel(channel: string): string | undefined {
    const lowered = channel.toLowerCase();
    return this.opts.queues.find((q) => channelFor(q).toLowerCase() === lowered);
  }

  async stop(): Promise<void> {
    this.stopped = true;
    const client = this.client;
    this.client = undefined;
    if (client) {
      try {
        await client.end();
      } catch {
        // Already gone; nothing useful to do here during shutdown.
      }
    }
    this.connected = false;
    if (this.loopPromise) {
      await this.loopPromise.catch(() => undefined);
      this.loopPromise = undefined;
    }
  }
}

function assertSafeChannel(channel: string): void {
  if (!/^pgjobq_[a-zA-Z0-9._:-]+$/.test(channel)) {
    throw new Error(`Refusing to LISTEN on unsafe channel name: ${channel}`);
  }
  // Postgres identifiers are limited to 63 bytes; a longer channel would be
  // silently truncated, so the listener and the trigger would disagree.
  if (Buffer.byteLength(channel, 'utf8') > 63) {
    throw new Error(
      `Channel name '${channel}' exceeds 63 bytes and would be truncated by Postgres, ` +
        `causing notifications to be missed. Use a shorter queue name.`,
    );
  }
}
