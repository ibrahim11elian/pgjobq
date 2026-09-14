# Scope and limitations

What this is, what it is not, and where it stops working. Read this before adopting it.

## What it is

A durable background-job engine for applications already running PostgreSQL, at a scale
of roughly **hundreds to low thousands of jobs per second**. It gives you leases, retries
with jittered backoff, cron scheduling, a dead-letter queue, an HTTP API, and an
operations dashboard, with no infrastructure beyond the database you already have.

It is a good fit when transactional enqueue matters — when a job must be created in the
same transaction as the row it concerns, and must not exist if that transaction rolls
back.

## What it is not

| Not this                                  | Why, and what to use instead                                                                                                              |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Exactly-once delivery**                 | Not achievable across a process boundary. At-least-once plus idempotency support. See [delivery-guarantees.md](./delivery-guarantees.md). |
| **A workflow orchestrator**               | No DAGs, job dependencies, fan-out/fan-in, or compensation. A queue is not an orchestrator. Use Temporal, or a purpose-built engine.      |
| **A pub/sub bus**                         | One job goes to one worker. No fan-out to multiple consumers, no topics.                                                                  |
| **A message broker at scale**             | See the ceiling below. Past it, use Kafka, NATS, or SQS.                                                                                  |
| **Multi-region**                          | Single-primary Postgres only. Read replicas are never used for claiming — a claim must see and lock the authoritative row.                |
| **A multi-tenant SaaS control plane**     | No organizations, user accounts, or billing. API keys with queue and operation scopes, and that is deliberately all.                      |
| **A worker autoscaler**                   | The metrics to drive one are exposed; the controller is not implemented.                                                                  |
| **Encrypted at rest beyond the database** | Payloads are stored as `jsonb` in plain text. Use database-level encryption; do not put secrets in payloads.                              |
| **A non-TypeScript client**               | The HTTP API plus the published OpenAPI 3.1 document is the contract for other languages.                                                 |

## The throughput ceiling

Measured, not estimated. Full methodology and caveats in
[bench/REPORT.md](../bench/REPORT.md).

- **Peak: ~3,200 jobs/s** on Postgres 18.6 in Docker Desktop on Windows, 20,000-job runs,
  batch size 20.
- **The scaling knee is 16 workers.** Efficiency: 93% (4→8), 78% (8→16), **50% (16→32)**.
- **Past the knee, adding workers makes things worse.** 16→32 workers bought 1% more
  throughput while claim p99 went from 30.58ms to 198.27ms — a 6.5× latency regression for
  nothing.

Absolute numbers carry Docker-on-Windows overhead; expect better on Linux with a local
socket. The _shape_ — where the knee is, and that it exists — is the transferable finding.

### Practical implication

Size the worker pool at the knee, not higher. To go beyond it, shard queues across
databases. Adding workers to one database past the knee converts throughput into latency.

## When to stop using this

Switch to a dedicated broker when any of these is true:

1. **Sustained throughput exceeds the low thousands of jobs/s** on one primary, and you
   have already sharded what you reasonably can.
2. **Autovacuum cannot keep pace** despite the tuning in the migration — dead tuples
   trending up, `jobq_db_last_autovacuum_age_seconds` climbing, table size not returning
   to steady state.
3. **MultiXact wait events dominate** (`LWLock:MultiXactOffsetSLRU`,
   `LWLock:MultiXactMemberSLRU`) at the worker concurrency you need. Symptom: CPU rising
   while throughput stays flat — commonly misread as needing more workers, which makes it
   worse.
4. **You need fan-out** to many consumers, cross-region delivery, or true streaming
   semantics.
5. **The queue is competing with your application** for the same database's connections,
   CPU, or I/O. A queue that degrades your transactional workload has stopped being cheap.

## Known limitations, in order of how likely they are to bite

### 1. Ordering is approximate

`SKIP LOCKED` means a contended earlier job can be passed over for a later one. Jobs are
claimed in `priority DESC, run_at, id` order, but that is best-effort, not guaranteed.

Lock-free concurrent consumption and strict global FIFO are mutually exclusive. For strict
ordering on a subset, give it a dedicated queue with concurrency 1 — you get ordering and
lose parallelism, which is the actual trade.

### 2. Strict priority can starve low-priority work

A continuous supply of high-priority jobs can delay a low-priority job indefinitely.
Priority aging is available per queue and **off by default**, because its `ORDER BY` is an
expression that cannot use `job_claim_idx` and forces a sort. Queues that need fairness
pay for it; queues that do not, do not.

### 3. Bloat under sustained churn — not yet proven controlled

Every job is updated at least twice (claim, completion) then deleted by retention.
Copy-on-write leaves a dead tuple each time. Mitigations are in place — partial indexes,
bounded-batch retention, aggressive per-table autovacuum, monthly-partitioned archive.

**But:** `fillfactor = 80` helps less than it appears, because the claim update changes
`state`, which appears in four index predicates, so it **cannot be a HOT update**. That is
a structural cost of this design.

And the sustained-run measurement that would demonstrate steady state has **not been
run**. A single benchmark pass grew the table 74→109 MB and index 34→60 MB. Dead tuples
fell during the run, so autovacuum was working, but one pass cannot distinguish "keeping
up" from "briefly ahead". Until Requirement 17.9 is executed, bloat control here is
reasoning, not evidence.

Watch `jobq_db_dead_tuples` and `jobq_db_last_autovacuum_age_seconds`.
[runbook.md](./runbook.md) has the remediation procedure.

### 4. `LISTEN` silently degrades behind a transaction-mode pooler

The listener connection must be **direct**. Behind PgBouncer in transaction mode or
Supabase's pooler port, `LISTEN` state does not survive between transactions and
notifications never arrive.

Nothing breaks — polling still delivers every job — so the only symptom is latency
quietly rising from milliseconds to the poll interval. Check
`jobq_notify_received_total`: if it is flat while jobs are being enqueued, this is why.

### 5. An attempt is consumed by unrelated crashes

`attempt` increments at claim time, so a deploy, an OOM elsewhere, or a host loss burns an
attempt on every job in flight. Deliberate — see
[engineering-decisions.md](./engineering-decisions.md#6) — but it means `max_attempts`
should not be set to 1 or 2 unless you genuinely want a single try.

### 6. Handler timeouts are cooperative

`AbortSignal` is advisory. A handler that ignores it keeps running after the queue has
given up, and its side effects still land. Node cannot forcibly interrupt synchronous
work. Check `ctx.signal.aborted` around anything expensive.

### 7. Aggregate counts are approximate for history

The dashboard reads exact counts for the live backlog, which is small by design. Totals
over history are bounded by the retention window — they are **not** a lifetime count, and
the UI labels them as such. A full `COUNT(*)` on the job table would not stay fast.

### 8. The dashboard holds an API key in the browser

`sessionStorage`, cleared when the tab closes. Acceptable for an operator tool on a
trusted machine; **not** acceptable for a public deployment. A real deployment should put
a session cookie and a server-side proxy in front of it rather than handing a queue
credential to the browser.

### 9. Payloads are limited to 256 KiB by default

Configurable. Large payloads belong in object storage, with the job carrying a reference.
A queue is not a blob store.

### 10. Queue names are constrained

`[a-zA-Z0-9._:-]`, max 128 characters, and the derived `NOTIFY` channel
(`pgjobq_<queue>`) must fit Postgres's 63-byte identifier limit. A longer name would be
silently truncated, so the listener and the trigger would disagree about the channel —
the listener validates and refuses rather than letting that happen quietly.

## Verification status

What is actually verified, versus asserted. The honest version.

| Area                                        | Status                                                         |
| ------------------------------------------- | -------------------------------------------------------------- |
| Claim exclusivity under concurrency         | Verified — `npm run smoke`                                     |
| Ownership fence (both predicates)           | Verified — `npm run smoke`                                     |
| Attempt bound (database constraint)         | Verified — `npm run smoke`                                     |
| Idempotent enqueue under race               | Verified — 8 concurrent duplicates → 1 row                     |
| Lease recovery                              | Verified — `npm run smoke`                                     |
| Claim uses the partial index, no sort       | Verified — `EXPLAIN` assertion on 5,000 seeded rows            |
| Cron DST spring-forward                     | Verified — fixed-date check                                    |
| Scheduler exactly-one-per-occurrence        | Verified — same occurrence deduped, different occurrence fires |
| HTTP status codes, auth, pagination         | Verified — 55 API checks                                       |
| `traceparent` propagated into the job row   | Verified — covered by the API checks                           |
| Throughput and scaling knee                 | Measured — [bench/REPORT.md](../bench/REPORT.md)               |
| Lock mode comparison                        | Measured — result contradicts theory, mechanism unexplained    |
| **Exactly-once effect under `SIGKILL`**     | **Harness written, NOT YET RUN** — Requirement 17.2            |
| **No loss across worker kills**             | **Harness written, NOT YET RUN** — Requirement 17.3            |
| **OpenTelemetry span emission**             | **Not implemented** — Requirement 13.4                         |
| **Sustained bloat steady state**            | **Not implemented** — Requirement 17.9                         |
| **Comparison vs pg-boss / graphile-worker** | **Not implemented** — Requirement 17.10                        |
| **Formal test suite**                       | **Not implemented** — deferred deliberately                    |

Note on the two "harness written, not yet run" rows: `bench/chaos/` now contains the
`SIGKILL` and durability harnesses, but they have never been executed — they were written
in a session where command execution was unavailable. Until `npm run chaos` passes, treat
them as unproven code, not as evidence. "Zero lost jobs under worker failure" remains a
design claim rather than a measured one.

On tracing: `traceparent` capture and propagation into the job row works, so a producer's
trace id reaches the worker. No spans are created or exported. An `@opentelemetry/api`
dependency was declared and never used; it has been removed rather than left as a phantom
dependency implying a feature that does not exist.
