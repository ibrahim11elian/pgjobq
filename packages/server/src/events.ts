/**
 * Server-Sent Events fan-out for the dashboard.
 *
 * SSE rather than WebSockets: the traffic is one-directional, it works over plain
 * HTTP with no upgrade negotiation, and browsers reconnect automatically. A WebSocket
 * would add a protocol and a heartbeat scheme for no gain here.
 */
import type { Request, Response } from 'express';
import type { Logger } from '@pgjobq/core';

export interface QueueSnapshot {
  readonly queue: string;
  readonly counts: Record<string, number>;
  readonly oldestWaitSeconds: number;
}

export type ServerEvent =
  | { readonly type: 'snapshot'; readonly queues: QueueSnapshot[]; readonly at: string }
  | { readonly type: 'health'; readonly ready: boolean; readonly at: string };

interface Subscriber {
  readonly id: number;
  readonly res: Response;
}

export class EventStream {
  private subscribers = new Map<number, Subscriber>();
  private nextId = 1;
  private heartbeat: NodeJS.Timeout | undefined;

  constructor(
    private readonly maxSubscribers: number,
    private readonly logger: Logger,
  ) {}

  get subscriberCount(): number {
    return this.subscribers.size;
  }

  /**
   * Attaches a subscriber.
   *
   * The cap is a real defence, not a formality: each subscriber holds an open socket
   * and a write buffer, so an unbounded count is a trivial memory exhaustion vector.
   */
  subscribe(req: Request, res: Response): boolean {
    if (this.subscribers.size >= this.maxSubscribers) {
      res.status(503).json({
        error: {
          code: 'NOT_READY',
          message: `Event stream is at capacity (${this.maxSubscribers} subscribers)`,
        },
      });
      return false;
    }

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      // Defeats proxy buffering, which would otherwise hold events until the
      // buffer filled and make the stream look broken.
      'X-Accel-Buffering': 'no',
    });

    const id = this.nextId++;
    this.subscribers.set(id, { id, res });

    // Tell the browser how long to wait before reconnecting after a drop.
    res.write('retry: 3000\n\n');

    const cleanup = (): void => {
      this.subscribers.delete(id);
      this.logger.debug({ subscriberId: id, remaining: this.subscribers.size }, 'sse disconnected');
    };
    req.on('close', cleanup);
    req.on('error', cleanup);

    this.startHeartbeat();
    this.logger.debug({ subscriberId: id, total: this.subscribers.size }, 'sse connected');
    return true;
  }

  publish(event: ServerEvent): void {
    if (this.subscribers.size === 0) return;
    const frame = `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`;

    for (const sub of [...this.subscribers.values()]) {
      // A slow consumer must not grow the server's buffer without bound. Dropping
      // the connection is better than accumulating memory for a client that cannot
      // keep up; the browser will reconnect and get a fresh snapshot.
      const ok = sub.res.write(frame);
      if (!ok && sub.res.writableLength > 1_000_000) {
        this.logger.warn({ subscriberId: sub.id }, 'sse subscriber too slow; disconnecting');
        this.subscribers.delete(sub.id);
        sub.res.end();
      }
    }
  }

  /**
   * Comment frames keep intermediaries from closing an idle connection.
   *
   * Load balancers commonly drop connections after 30-60s of silence, and a queue
   * with nothing happening is exactly when the stream is idle.
   */
  private startHeartbeat(): void {
    if (this.heartbeat) return;
    this.heartbeat = setInterval(() => {
      if (this.subscribers.size === 0) {
        clearInterval(this.heartbeat);
        this.heartbeat = undefined;
        return;
      }
      for (const sub of this.subscribers.values()) sub.res.write(': keepalive\n\n');
    }, 20_000);
    this.heartbeat.unref?.();
  }

  close(): void {
    if (this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = undefined;
    }
    for (const sub of this.subscribers.values()) sub.res.end();
    this.subscribers.clear();
  }
}
