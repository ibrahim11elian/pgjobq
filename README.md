# pgjobq

A durable background-job engine whose only infrastructure dependency is PostgreSQL.
Workers claim jobs concurrently with `SELECT ... FOR UPDATE SKIP LOCKED` — no broker, no
Redis, no second system to operate.

**Delivery semantics: at-least-once with idempotency support.** Not exactly-once, which is
not achievable across a process boundary. What is guaranteed: a job is never held by two
workers concurrently, an acknowledged enqueue is never silently lost, no job exceeds its
attempt limit, and an idempotent handler produces its effect exactly once.
[The precise guarantee, including the one window where duplicate execution is possible.](docs/delivery-guarantees.md)

---

## The problem

An app needs to send 50,000 emails, resize uploads, and run a nightly report. The usual
answer is a broker — one more system to provision, monitor, secure, and pay for. And it
introduces a bug you cannot fix cleanly: you enqueue either _before_ your transaction
commits (and may reference a row that never existed) or _after_ (and may lose the job on a crash in between).

If you already run Postgres, `FOR UPDATE SKIP LOCKED` turns one table into a safe
concurrent queue. Enqueue in the same transaction as the row the job concerns. Many workers pull without ever colliding and without any worker blocking on another. A worker crashes mid-job; its lease expires; another picks the job up. Repeated failures land in a dead-letter queue you can inspect and replay.

The interesting part is not the idea — it is making the guarantees hold, and knowing
exactly where they stop.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Producers                                                                   │
│   embedded @pgjobq/core client        │        HTTP + OpenAPI 3.1 (any lang)  │
└───────────────┬──────────────────────────────────────┬───────────────────────┘
                │            parameterized SQL only    │
     ┌──────────▼──────────────────────────────────────▼──────────┐
     │                       PostgreSQL 18                        │
     │                                                            │
     │  job · job_archive · schedule · queue_config · api_key     │
     │                                                            │
     │  claim:  one statement, partial index, FOR UPDATE OF j      │
     │          SKIP LOCKED, attempt++ in the same statement       │
     │  wakeup: NOTIFY pgjobq_<queue>  (optimization only)         │
     └──────────┬──────────────────────────────┬──────────────────┘
                │                              │
     ┌──────────▼───────────┐      ┌───────────▼──────────────┐
     │  Worker process(es)  │      │  Dashboard (React + SSE) │
     │  claim loop · pool   │      └──────────────────────────┘
     │  heartbeat · LISTEN  │
     └──────────────────────┘        Background loops, each safe on every
                                     instance with NO leader election —
                                     all claim their own work SKIP LOCKED:
                                       reaper · scheduler · retention
```

Every background loop uses `SKIP LOCKED` on its own scan, so running _n_ instances
requires no coordination. There is no leader to elect and none to lose.

## Quick start

Needs Docker and Node.js 22.11+.

```bash
git clone <this-repo> && cd pgjobq
npm install
cp .env.example .env

npm run db:up          # Postgres 18.6 on host port 5433
npm run migrate:up     # apply the schema
npm run seed           # demo queues, jobs, and schedules
```

Then in separate terminals:

```bash
npm run dev:api        # API + dashboard backend on :3001
npm run dev:worker     # worker pool
npm run dev:dashboard  # dashboard on :5400
```

Open [http://localhost:5400](http://localhost:5400), paste the `API_BOOTSTRAP_KEY` from your `.env`, and watch the
seeded backlog drain. API docs at [http://localhost:3001/docs](http://localhost:3001/docs).

> **Ports are 5433 and 3001, not 5432 and 3000.** Both defaults collided with software
> already running during development — a native Postgres service and a Vite dev server.
> The Postgres collision is actively misleading: on Windows both can bind 5432 and the
> native one wins for localhost, producing an authentication failure against a completely
> different server. [Why.](docs/engineering-decisions.md#18-ports-5433-and-3001-not-5432-and-3000)

## Verify it yourself

```bash
npm run smoke             # 54 checks against real Postgres        — passing
npm run api:smoke         # 55 checks against the running API      — passing
npm run chaos             # SIGKILL mid-handler                    — NEVER RUN
npm run chaos:durability  # repeated kills under load              — NEVER RUN
```

`smoke` asserts the claims this design rests on: 6 parallel claimers never receive the same
job, 8 concurrent duplicate enqueues produce exactly one row, the ownership fence rejects
both a wrong `worker_id` and a wrong `attempt`, an expired lease is recovered with its
attempt preserved, the claim plan uses the partial index with **no sort node**, and cron
survives a DST spring-forward.

`chaos` is the one that matters most and **has never been executed.** It kills a worker with
a real `SIGKILL` after its side effect is durable but before completion is recorded, then
asserts the job is delivered exactly twice, an idempotent handler's effect lands **once**,
and — as a control — a naive handler's lands **twice**. That control run is the point: it
proves the duplicate-execution window is real and that idempotency is what closes it, rather
than anything the queue does.

It was written in a session where command execution was unavailable, so treat it as unproven
code until you have run it. `npm run verify` runs everything in sequence.

## Usage

```ts
import { z } from 'zod';
import { Client, Db, WorkerPool, createRegistry } from '@pgjobq/core';

const db = new Db({ connectionString: process.env.DATABASE_URL! });

// One schema per job type gives runtime validation AND static inference.
const registry = createRegistry().register(
  'email.send',
  z.object({ to: z.string().email(), subject: z.string() }),
  async (payload, ctx) => {
    // payload.to is `string` — inferred, not cast.
    // ctx.id is stable across re-delivery: use it as your idempotency key.
    await mailer.send(payload, { idempotencyKey: `job-${ctx.id}` });
  },
);

const client = new Client({ db });
await client.enqueue('email', 'email.send', { to: 'a@b.com', subject: 'Hi' });

const pool = new WorkerPool({
  db,
  registry,
  queues: ['email'],
  concurrency: 10,
  claimBatchMax: 20,
  pollIntervalMs: 1000,
  jobTimeoutMs: 300_000,
  shutdownGraceMs: 30_000,
  heartbeatFraction: 0.5,
  backoff: { initialMs: 1000, multiplier: 2, maxMs: 3_600_000, jitter: 'full' },
  maxErrorHistory: 5,
  maxErrorTextBytes: 4096,
  listenConnectionString: process.env.DATABASE_URL!, // must be DIRECT, not pooled
});
pool.start();
```

Enqueue is checked against the registry at compile time — an unknown type or a mismatched
payload will not build.

## The claim statement

The centre of the system. One statement, not four:

```sql
WITH candidate AS (
  SELECT j.id
    FROM job j
    LEFT JOIN queue_config qc ON qc.queue = j.queue
   WHERE j.queue = $1
     AND j.state = 'available'
     AND j.run_at <= now()
     AND COALESCE(qc.paused, false) = false
   ORDER BY j.priority DESC, j.run_at, j.id
   LIMIT $2
   FOR UPDATE OF j SKIP LOCKED
)
UPDATE job j
   SET state = 'running', attempt = j.attempt + 1, worker_id = $3,
       started_at = now(),
       lease_expires_at = now() + make_interval(secs => j.lease_seconds)
  FROM candidate c WHERE j.id = c.id
RETURNING j.id, j.type, j.payload, j.attempt, j.lease_expires_at;
```

Four things in there are load-bearing:

**One statement.** The obvious version is `BEGIN; SELECT ... FOR UPDATE SKIP LOCKED; UPDATE; COMMIT` — four round trips with the lock held across all of them. Folding it into a
single `UPDATE ... FROM (CTE)` means one round trip and no window where a row is locked but
not yet `running`.

**`FOR UPDATE OF j`, not bare `FOR UPDATE`.** Not an optimization — a bare `FOR UPDATE`
_errors_, because Postgres refuses to lock the nullable side of an outer join. Had it
worked it would also lock the `queue_config` row, serializing every claim in the queue
against it.

**`ORDER BY` mirrors `job_claim_idx (queue, priority DESC, run_at, id) WHERE state = 'available'`** exactly, so the plan is an index scan with no sort. The index is _partial_,
so its size tracks backlog depth rather than total history — a finished job leaves it
entirely. The plan shape is an asserted property, not a hope.

**`attempt = attempt + 1` at claim time**, never at failure time. A worker that dies
without reporting still consumes an attempt. Otherwise a job that reliably kills its worker
retries forever, taking down worker after worker.

## Measured performance

From [bench/REPORT.md](bench/REPORT.md). Postgres 18.6 in Docker Desktop on Windows, so
absolute numbers carry virtualization overhead; the shape is the transferable part.

| Workers |    Jobs/s | Claim p50 | Claim p99 | Scaling efficiency |
| ------: | --------: | --------: | --------: | -----------------: |
|       4 |     1,090 |    4.93ms |   11.03ms |                  — |
|       8 |     2,034 |    5.08ms |   15.72ms |                93% |
|      16 | **3,176** |    6.49ms |   30.58ms |                78% |
|      32 |     3,203 |   11.92ms |  198.27ms |            **50%** |

**The knee is 16 workers.** Going to 32 bought 1% more throughput for a 6.5× worse p99.
Past the knee the limit is claim-path contention, not worker count, and adding workers
actively makes things worse. Scale by sharding queues across databases instead.

**A result that contradicted the design:** `FOR NO KEY UPDATE` is theoretically the better
lock here — sufficient for mutual exclusion, doesn't block `FOR KEY SHARE`, escalates to
MultiXact less readily. It **lost at every concurrency level** (2,642 vs 3,203 jobs/s
peak). `FOR UPDATE` stays the default because that is what measured faster. The mechanism is not yet explained, and [the report says so](bench/REPORT.md#lock-mode-measurement-disagrees-with-theory) rather than inventing a reason.

## What it does

- Enqueue with priority, delay, per-job attempt and lease overrides, metadata
- Batch enqueue up to 1,000 jobs in one statement, all-or-nothing
- Idempotent enqueue (`idempotencyKey`) and debounce (`uniqueKey`), enforced by unique
  indexes rather than read-then-write checks
- Concurrent claiming with leases, automatic heartbeat, and crash recovery
- Retries with exponential backoff and full jitter; explicit `RetryAfterError`;
  `NonRetryableError` to skip remaining attempts
- Dead-letter queue with attempt history, stack traces, and single or bulk replay
- Cron schedules with IANA timezones and correct DST handling, materialized
  exactly-once-per-occurrence via a deterministic key — no leader election
- Retention with optional month-partitioned archive
- Graceful shutdown that _releases_ in-flight jobs rather than stranding them for a lease
  period
- HTTP API with generated OpenAPI 3.1, scoped API keys, keyset pagination, rate limiting
- Prometheus metrics including database-side bloat signals
- W3C `traceparent` captured at enqueue and handed to the worker, so a producer's trace
  id survives into the job (span emission itself is not implemented — see
  [scope.md](docs/scope.md#verification-status))
- Operations dashboard with live SSE updates

## Documentation

|                                                        |                                                                                                 |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| [Delivery guarantees](docs/delivery-guarantees.md)     | The exact guarantee, the one duplicate-execution window, and how to write an idempotent handler |
| [Engineering decisions](docs/engineering-decisions.md) | 20 decisions with what was rejected and why — including two the measurements contradicted       |
| [Scope and limitations](docs/scope.md)                 | What it is not, the measured ceiling, and when to switch to a real broker                       |
| [Operations runbook](docs/runbook.md)                  | Symptom-first: backlog growth, retry storms, poison payloads, bloat remediation                 |
| [State machine](docs/state-machine.md)                 | Generated from the transition table, so it cannot drift                                         |
| [Deployment](docs/deployment.md)                       | Connection sizing, the `LISTEN` trap, free-tier hosting, production checklist                   |
| [Benchmark report](bench/REPORT.md)                    | Methodology, results, and what is not measured yet                                              |
| [Contributing](CONTRIBUTING.md)                        | Setup, the invariants a PR is rejected over, testing rules                                      |

## Project layout

```
packages/core/        @pgjobq/core — engine, client, worker runtime (publishable)
  src/sql/            every SQL statement, one file each, commented with WHY
  src/engine/         state-machine · backoff · reaper · scheduler · retention
  src/worker/         pool · runner · registry · listener
packages/server/      HTTP API, SSE, /metrics
packages/worker/      standalone worker entrypoint
packages/dashboard/   React + Vite operations UI
bench/                throughput, latency, and lock-mode harness
docs/                 architecture, decisions, guarantees, runbook, scope
```

`core` never imports from the other packages and has no HTTP dependency, so the
correctness paths can be driven directly.

## Honest status

Working and verified end to end: schema, claim path, leases and recovery, retries, DLQ,
scheduling, retention, HTTP API, dashboard, benchmark. 109 automated checks pass against a
real database.

**Not yet done, and it matters:**

- **The `SIGKILL` chaos test is written but has never been run.** `bench/chaos/` holds it:
  kill a worker after its side effect is durable but before completion is recorded, then
  assert the job is delivered exactly twice, an idempotent handler's effect lands once, and
  a naive handler's lands twice. It was written in a session where command execution was
  unavailable, so **treat it as unproven code, not as evidence.** Run it with
  `npm run chaos` and `npm run chaos:durability`. Until those pass, "zero lost jobs under
  worker failure" is a design argument.
- **A formal test suite.** Deliberately deferred for delivery speed. The smoke runs are
  executable verification, not a test suite — no fixtures, no isolation, no coverage.
- **Sustained bloat measurement**, a comparison against `pg-boss` and `graphile-worker`,
  and Linux numbers.

Full accounting in [scope.md](docs/scope.md#verification-status). Nothing in this README
claims a number that is not in [bench/REPORT.md](bench/REPORT.md).

## Licence

MIT
