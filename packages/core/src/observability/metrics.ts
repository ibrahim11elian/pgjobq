/**
 * Prometheus metrics.
 *
 * All metric access goes through this module rather than importing the client
 * library directly. That isolation is not speculative: prom-client 15.x is
 * deprecated in favour of @prometheus-io/client, so the swap is expected, and this
 * keeps it a one-file change.
 *
 * LABEL CARDINALITY is bounded deliberately. `type` comes from callers, and an
 * unbounded set of job types would grow the registry without limit until the
 * process runs out of memory — a denial of service via metric labels. Unknown types
 * collapse into `other`.
 */
import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

export interface MetricsOptions {
  /** Job types allowed as label values. Anything else is recorded as `other`. */
  readonly knownTypes?: readonly string[];
  readonly maxKnownTypes?: number;
  readonly collectDefault?: boolean;
  readonly registry?: Registry;
}

const OTHER = 'other';

export class Metrics {
  readonly registry: Registry;
  private readonly known: Set<string>;
  private readonly maxKnown: number;

  readonly jobsEnqueued: Counter<'queue' | 'type'>;
  readonly jobsCompleted: Counter<'queue' | 'type' | 'outcome'>;
  readonly jobDuration: Histogram<'queue' | 'type'>;
  readonly queueWait: Histogram<'queue' | 'type'>;
  readonly claimDuration: Histogram<'queue'>;
  readonly claimBatchSize: Histogram<'queue'>;
  readonly queueDepth: Gauge<'queue' | 'state'>;
  readonly jobsInFlight: Gauge<'queue'>;
  readonly retriesScheduled: Counter<'queue' | 'type'>;
  readonly leasesExpired: Counter<'queue'>;
  readonly staleCompletions: Counter<'queue'>;
  readonly dlqSize: Gauge<'queue'>;
  readonly scheduleMaterialized: Counter<'schedule'>;
  readonly notifyReceived: Counter<'queue'>;
  readonly dbDeadTuples: Gauge<'table'>;
  readonly dbLastVacuumAge: Gauge<'table'>;
  readonly dbTableBytes: Gauge<'table'>;
  readonly dbIndexBytes: Gauge<'table'>;
  readonly overdueLeases: Gauge;
  readonly retentionPruned: Counter<'queue'>;

  constructor(opts: MetricsOptions = {}) {
    this.registry = opts.registry ?? new Registry();
    this.known = new Set(opts.knownTypes ?? []);
    this.maxKnown = opts.maxKnownTypes ?? 200;

    if (opts.collectDefault !== false) {
      collectDefaultMetrics({ register: this.registry, prefix: 'jobq_process_' });
    }

    const r = this.registry;

    this.jobsEnqueued = new Counter({
      name: 'jobq_jobs_enqueued_total',
      help: 'Jobs accepted by enqueue, excluding deduplicated calls',
      labelNames: ['queue', 'type'],
      registers: [r],
    });

    this.jobsCompleted = new Counter({
      name: 'jobq_jobs_completed_total',
      help: 'Deliveries that reached an outcome',
      labelNames: ['queue', 'type', 'outcome'],
      registers: [r],
    });

    this.jobDuration = new Histogram({
      name: 'jobq_job_duration_seconds',
      help: 'Handler execution time',
      labelNames: ['queue', 'type'],
      // Spread wide: a queue commonly mixes sub-100ms work with multi-minute work.
      buckets: [0.005, 0.025, 0.1, 0.5, 1, 5, 15, 60, 300, 900],
      registers: [r],
    });

    this.queueWait = new Histogram({
      name: 'jobq_job_queue_wait_seconds',
      help: 'Time from enqueue to claim. The direct measure of whether the backlog is draining.',
      labelNames: ['queue', 'type'],
      buckets: [0.01, 0.05, 0.25, 1, 5, 30, 120, 600, 3600],
      registers: [r],
    });

    this.claimDuration = new Histogram({
      name: 'jobq_claim_duration_seconds',
      help: 'Duration of the claim statement itself',
      // Tight buckets: this statement should be sub-millisecond to low-millisecond.
      // If it drifts above 100ms the claim index is probably not being used.
      buckets: [0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1],
      labelNames: ['queue'],
      registers: [r],
    });

    this.claimBatchSize = new Histogram({
      name: 'jobq_claim_batch_size',
      help: 'Jobs returned per claim statement. Low values under load mean contention.',
      labelNames: ['queue'],
      buckets: [0, 1, 2, 5, 10, 20, 50],
      registers: [r],
    });

    this.queueDepth = new Gauge({
      name: 'jobq_queue_depth',
      help: 'Jobs per queue and state',
      labelNames: ['queue', 'state'],
      registers: [r],
    });

    this.jobsInFlight = new Gauge({
      name: 'jobq_jobs_in_flight',
      help: 'Handlers currently executing in this process',
      labelNames: ['queue'],
      registers: [r],
    });

    this.retriesScheduled = new Counter({
      name: 'jobq_retries_scheduled_total',
      help: 'Failures that scheduled a retry rather than dead-lettering',
      labelNames: ['queue', 'type'],
      registers: [r],
    });

    this.leasesExpired = new Counter({
      name: 'jobq_leases_expired_total',
      help: 'Jobs recovered by the reaper. Sustained non-zero means workers are dying.',
      labelNames: ['queue'],
      registers: [r],
    });

    this.staleCompletions = new Counter({
      name: 'jobq_stale_completion_total',
      help: 'Ownership fence rejections: a worker reported on a job it no longer owned',
      labelNames: ['queue'],
      registers: [r],
    });

    this.dlqSize = new Gauge({
      name: 'jobq_dlq_size',
      help: 'Dead-lettered jobs per queue',
      labelNames: ['queue'],
      registers: [r],
    });

    this.scheduleMaterialized = new Counter({
      name: 'jobq_schedule_materialized_total',
      help: 'Jobs created from a schedule occurrence',
      labelNames: ['schedule'],
      registers: [r],
    });

    this.notifyReceived = new Counter({
      name: 'jobq_notify_received_total',
      help: 'LISTEN/NOTIFY wakeups received',
      labelNames: ['queue'],
      registers: [r],
    });

    this.dbDeadTuples = new Gauge({
      name: 'jobq_db_dead_tuples',
      help: 'Dead tuples per table. The documented failure mode of this design is vacuum falling behind.',
      labelNames: ['table'],
      registers: [r],
    });

    this.dbLastVacuumAge = new Gauge({
      name: 'jobq_db_last_autovacuum_age_seconds',
      help: 'Seconds since the last vacuum of each table; -1 when never vacuumed',
      labelNames: ['table'],
      registers: [r],
    });

    this.dbTableBytes = new Gauge({
      name: 'jobq_db_table_bytes',
      help: 'Total relation size per table, for bloat tracking',
      labelNames: ['table'],
      registers: [r],
    });

    this.dbIndexBytes = new Gauge({
      name: 'jobq_db_index_bytes',
      help: 'Index size per table, for bloat tracking',
      labelNames: ['table'],
      registers: [r],
    });

    this.overdueLeases = new Gauge({
      name: 'jobq_overdue_leases',
      help: 'Running jobs past their lease but not yet reaped; sustained non-zero means the reaper is behind',
      registers: [r],
    });

    this.retentionPruned = new Counter({
      name: 'jobq_retention_pruned_total',
      help: 'Terminal jobs removed from the operational table',
      labelNames: ['queue'],
      registers: [r],
    });
  }

  /**
   * Bounds a job type to a safe label value.
   *
   * Types are learned as they are seen, up to a cap. Past the cap everything new
   * becomes `other`, so a caller inventing types cannot exhaust memory.
   */
  labelType(type: string): string {
    if (this.known.has(type)) return type;
    if (this.known.size < this.maxKnown) {
      this.known.add(type);
      return type;
    }
    return OTHER;
  }

  async expose(): Promise<string> {
    return this.registry.metrics();
  }

  get contentType(): string {
    return this.registry.contentType;
  }

  reset(): void {
    this.registry.resetMetrics();
  }
}

/**
 * Shared instance for the common case. Applications embedding the library can
 * construct their own with a supplied registry instead.
 */
let shared: Metrics | undefined;

export function getMetrics(opts?: MetricsOptions): Metrics {
  shared ??= new Metrics(opts);
  return shared;
}

/** Test and benchmark helper: drops the shared instance so a fresh registry is built. */
export function resetSharedMetrics(): void {
  shared = undefined;
}
