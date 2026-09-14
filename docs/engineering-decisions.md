# Engineering decisions

Each entry: what was decided, what was rejected, and why. Where a decision was later
contradicted by a measurement, that is recorded rather than quietly corrected.

---

## 1. At-least-once delivery, not exactly-once

**Rejected:** advertising exactly-once.

Exactly-once across a process boundary requires atomically committing a side effect and
the record of that side effect in two different systems. There is always an instant where
one has committed and the other has not. See [delivery-guarantees.md](./delivery-guarantees.md)
for the exact window.

The decision is really about honesty. Plenty of queues advertise exactly-once and deliver
at-least-once plus internal deduplication. That is not worse engineering, but it misleads
users about where their idempotency responsibility lies — and the user who believes it
writes a non-idempotent payment handler.

**Cost:** handler authors must think about idempotency. The library helps (`ctx.id` is
stable across re-delivery) but cannot do it for them.

---

## 2. PostgreSQL as the only dependency

**Rejected:** Redis, RabbitMQ, SQS, Kafka.

- **Transactional enqueue.** A job can be enqueued in the same transaction as the row it
  concerns. With an external broker, either you enqueue before commit (and may reference
  a row that never existed) or after (and may lose the job). Both are real bugs that this
  eliminates by construction.
- **One system to operate.** No second thing to provision, monitor, back up, secure, and
  patch.
- **Inspectable.** Debugging is `SELECT * FROM job WHERE ...` rather than a broker CLI.

**Cost:** a real throughput ceiling. Measured at ~3,200 jobs/s peak in
[bench/REPORT.md](../bench/REPORT.md), with the scaling knee at 16 workers. Past that,
contention on the claim path dominates. [scope.md](./scope.md) states when to switch.

---

## 3. Hand-written SQL, no ORM

**Rejected:** Prisma, Drizzle, Kysely.

Every correctness-critical statement here needs something ORMs escape to raw SQL for:
`FOR UPDATE SKIP LOCKED`, `FOR UPDATE OF j`, partial indexes, `LISTEN`/`NOTIFY`,
`ON CONFLICT` against a partial unique index, CTE-with-`UPDATE`. The whole engine would be
raw strings inside an ORM wrapper — a dependency and a layer of indirection, hiding the
one thing a reader should see.

Instead: every statement lives in `packages/core/src/sql/`, one file per statement, each
commented with _why_ rather than _what_. The `sql()` helper is branded so an interpolated
string is a **type error**, not a code-review question.

**Cost:** no migration DSL, no generated types from schema. Row shapes are declared by
hand in `sql/rows.ts` and can drift from the schema — mitigated by a single
`JOB_COLUMNS` constant and by the smoke run exercising every statement against a real
database.

---

## 4. Single-statement claim

**Rejected:** `BEGIN; SELECT ... FOR UPDATE SKIP LOCKED; UPDATE ...; COMMIT;`

Four round trips with the row lock held across all of them. Folding it into one
`UPDATE ... FROM (CTE with FOR UPDATE SKIP LOCKED)` gives one round trip in an implicit
transaction, no window where a row is locked but not yet `running`, and no possibility of
a worker dying mid-claim while holding locks.

**Cost:** the statement is harder to read. Mitigated with an extensive comment block.

---

## 5. `FOR UPDATE OF j`, not bare `FOR UPDATE`

Not a micro-optimization — a bare `FOR UPDATE` **does not work**.

The claim CTE `LEFT JOIN`s `queue_config` to honour the paused flag. Postgres refuses to
lock rows on the nullable side of an outer join, so a bare `FOR UPDATE` fails outright:

```
ERROR: FOR UPDATE cannot be applied to the nullable side of an outer join
```

And had it worked, it would have locked the `queue_config` row too, serializing every
claim in the queue against a single row — exactly the contention this design exists to
avoid.

**This was learned the hard way.** The claim statement had it right from the start, and
then the retention statements — which `LEFT JOIN queue_config` for the same reason —
shipped with a bare `FOR UPDATE` and threw this error on the first server start. The trap
was documented in one file and walked into in another. Both retention statements now use
`FOR UPDATE OF j`, and the comment in `retention.sql.ts` says why.

---

## 6. `attempt` increments at claim time, not failure time

**Rejected:** incrementing on failure.

A worker that dies without reporting anything must still consume an attempt. Otherwise a
job that reliably kills its worker — an out-of-memory payload — retries forever, taking
down worker after worker. That is the failure mode this prevents, and it is a bad one:
the queue becomes a machine for destroying your fleet.

**Cost:** an unrelated crash (deploy, host loss) also burns an attempt. Hence
`max_attempts` defaults to 5, not 2. And `attempt` counts _deliveries_, not failures,
which surprises people reading the dashboard — documented in
[state-machine.md](./state-machine.md).

---

## 7. Full jitter on backoff by default

**Rejected:** unjittered exponential; equal jitter.

The failure that matters is **correlated**. When a downstream dependency goes down, every
in-flight job fails within the same second. Deterministic backoff reschedules them all to
the same future instant — a synchronized burst that hits the dependency the moment it
recovers and knocks it over again. Full jitter draws uniformly from `[0, base]`, spreading
them across the whole window.

The same jitter applies to reaper-initiated recovery, which matters more than it looks:
when a worker **host** dies, every job it held expires at nearly the same moment.
Un-jittered recovery would return them all to `available` with an identical `run_at` —
the exact thundering herd, on a system already down one host.

**Cost:** lower mean delay and less predictable timing. `equal` and `none` are available
per-deployment for anyone who needs a floor.

---

## 8. Ownership fence on `worker_id` **and** `attempt`

**Rejected:** fencing on `worker_id` alone.

A `worker_id`-only fence is passed by a worker that legitimately reclaimed the same job on
a later attempt. Both predicates are needed. See
[delivery-guarantees.md](./delivery-guarantees.md) for the interleaving table.

Zero rows affected is treated as a **normal outcome**, logged and metered
(`jobq_stale_completion_total`), never retried. Retrying would overwrite state belonging
to whichever worker now holds the job.

---

## 9. `NOTIFY` as an optimization, polling as the guarantee

**Rejected:** `NOTIFY` as the delivery mechanism.

Notifications reach only _currently connected_ listeners. One emitted while the listener
is reconnecting is simply gone. Building correctness on that would mean jobs sitting
unclaimed indefinitely after a network blip.

So polling is the correctness path (default 1s, jittered), and `NOTIFY` removes latency
from it. Payload is the queue name only — never job data — keeping well clear of the
8000-byte limit and avoiding filling the server's notify queue.

**Deployment trap, stated loudly because the failure is silent:** the `LISTEN` connection
must bypass any transaction-mode pooler (PgBouncer transaction mode, Supabase's pooler
port). Such a pooler multiplexes sessions, so `LISTEN` state does not survive between
transactions. Nothing breaks — the queue keeps working via polling, just with
poll-interval latency instead of milliseconds — which is why it goes unnoticed.

---

## 10. Partial indexes on every hot path

An index covering finished rows is a permanent tax on throughput for no benefit to
claiming. `job_claim_idx` is `WHERE state = 'available'`, so its size tracks **backlog
depth**, not total history, and a finished row leaves it entirely.

`job_unique_active_idx` gets a bonus from the same technique: scoped to non-terminal
states, a job reaching a terminal state frees its `unique_key` automatically, with no
cleanup job.

The claim's `ORDER BY` mirrors the index column order exactly, so the plan is an index
scan with no sort node. **The plan shape is an asserted property** — `npm run smoke`
seeds 5,000 rows, runs `ANALYZE`, then checks `EXPLAIN` output. A silently unused index
is the likeliest future performance regression, and it will not be silent here.

---

## 11. Retention deletes; the archive is partitioned by month

**Rejected:** keeping all history in `job`.

Table size must track backlog, not cumulative volume. Retention prunes terminal jobs in
**bounded batches** — an unbounded `DELETE` would be a long-running transaction holding
back the vacuum horizon, creating the exact bloat it exists to prevent.

`job_archive` is range-partitioned by month so expiring history is `DROP TABLE` on a
partition: instant, and creating no dead tuples. A `DELETE` of millions of archive rows
would itself generate the bloat the archive exists to avoid.

---

## 12. Deterministic schedule occurrence keys

**Rejected:** advisory locks, leader election.

Each occurrence's job is inserted with `sched:<schedule_id>:<planned_for ISO>`. Two
schedulers processing the same occurrence generate **byte-identical** keys, so the unique
index rejects the second. Exactly-one-per-occurrence becomes a database uniqueness
property. No leader to elect, no lock to lose, no split-brain.

`next_run_at` advances from **`now()`**, not from `planned_for`. That is what implements
`skip_missed`: a scheduler down for six hours moves to the next future occurrence rather
than firing six backfilled jobs. Computing from `planned_for` would produce exactly that
storm.

---

## 13. Priority aging is off by default, and is a separate statement

Strict priority permits starvation: a continuous supply of high-priority work can delay a
low-priority job indefinitely. The fix is aging — raising effective priority with waiting
time — and it is available per queue.

It is **off by default and implemented as a separate statement** because its `ORDER BY` is
an expression, which _cannot_ be satisfied by `job_claim_idx` and forces a sort. Shipping
it as the default would slow every queue to fix a problem most do not have. Queues that
opt in pay the cost; the caveat is documented in [scope.md](./scope.md).

---

## 14. `FOR UPDATE` beat `FOR NO KEY UPDATE` — theory lost

The design predicted the weaker lock would win. `FOR NO KEY UPDATE` is sufficient for
mutual exclusion between claimers, does not block `FOR KEY SHARE` lockers, and escalates
to MultiXact entries less readily. The claim updates no key column, so the stronger lock
should buy nothing.

**Measured: it lost at every concurrency level.** 2,642 vs 3,203 jobs/s at peak; 1,062 vs
2,034 at 8 workers; worse claim p50 in every run. Full table in
[bench/REPORT.md](../bench/REPORT.md).

`FOR UPDATE` stays the default because that is what measured faster. What the benchmark
does **not** establish is _why_ — candidate explanations (planner differences, the Docker
round trip masking lock costs, MultiXact pressure never becoming the bottleneck at this
scale) are untested, and confirming the mechanism needs wait-event sampling that is not
yet implemented.

Recorded because it happened, not because it is understood. This is the decision most
worth revisiting on Linux with a local socket.

---

## 15. TypeScript 5.9, not 7.0

**Rejected:** TypeScript 7.0.2, which the spec originally pinned.

`typescript-eslint` 8.69.0 declares `typescript >=4.8.4 <6.1.0`. Adopting TS 7 today
means giving up type-aware linting, including the `no-explicit-any` and `no-unsafe-*`
rules this project enforces on `engine/` and `sql/`.

TS 7's benefit is build speed. On a codebase whose entire value is correctness, trading
type-aware lint rules on the claim and lease paths for faster builds is the wrong way
round. Revisit when typescript-eslint supports 7.

---

## 16. Hand-rolled migration runner

**Rejected:** `node-pg-migrate`, which the spec originally pinned.

About 100 lines, and it buys explicit control over the things this schema needs:
partitioned tables, trigger functions, `ALTER TABLE ... SET (autovacuum_*)`. Each
migration runs in one transaction **with its bookkeeping row**, so a failure cannot leave
a half-migrated database.

It also enforces something most runners do not: **a checksum on applied migrations**.
Editing a merged migration is a hard failure, because doing so silently diverges every
environment that already applied the old text.

Forward-only. There is no `down` — a reversal is a new migration. `migrate down` prints
that and exits non-zero rather than pretending.

---

## 17. `prom-client` despite its deprecation notice

`prom-client` 15.1.3 prints a deprecation warning in favour of `@prometheus-io/client`,
which is at 0.16.1.

Kept `prom-client`: the API is known, it is stable, and deprecated is not broken. All
metric access is isolated behind `observability/metrics.ts`, so the swap is a one-file
change. That abstraction is not speculative — the deprecation makes the swap likely.

**Cost:** an `npm install` warning that a reviewer will see. Worth fixing before this is
presented publicly.

---

## 18. Ports 5433 and 3001, not 5432 and 3000

Both defaults collided with software already running on the development machine: a native
PostgreSQL 18 service on 5432, and a Vite dev server on 3000.

The Postgres collision was actively misleading. On Windows both the native service and
the Docker mapping can bind 5432, with the native one winning for `localhost` — producing
`password authentication failed for user "pgjobq"` against a completely different server.
The port-3000 collision was worse: Vite served its SPA `index.html` for every path, so
`/health` returned **200** with HTML and the smoke check "passed" while testing nothing.

Non-default ports are chosen so a fresh clone works without asking anyone to stop their
other services. Both are documented in `.env.example` with the reason.

---

## 19. `exactOptionalPropertyTypes` with explicit `| undefined`

`EnqueueOptions` declares `priority?: number | undefined` rather than `priority?: number`.
Under `exactOptionalPropertyTypes` those differ, and these values routinely arrive from
parsed JSON where a key is present with an undefined value. Requiring every call site to
strip such keys would be boilerplate for no safety gain.

The strictness is kept where it pays — object construction inside the engine — and
relaxed at the API boundary where the data genuinely is "present but undefined".

---

## 20. Deferring the test suite

The user asked for tests to come after the implementation, to move faster.

What was kept: `npm run smoke` (54 checks against real Postgres) and
`api-smoke` (55 checks against the running API). These are not a test suite — no
fixtures, no isolation, no coverage — but they are executable verification, and they
caught six real bugs: the retention `FOR UPDATE` failure, the IPv6 rate-limiter bypass,
the lazily-validated timezone returning 500, the stale-`dist` import, and both port
collisions.

What is missing, and matters: the `SIGKILL` chaos test (Requirement 17.2) and the
durability test (17.3) have now been **written** — see `bench/chaos/` — but never **run**,
because they were authored in a session where command execution was unavailable.

Written-and-unrun is not done, and this is the wrong document to blur that distinction.
Until `npm run chaos` and `npm run chaos:durability` pass, the project's headline
demonstration is unproven code and "zero lost jobs under worker failure" remains a design
argument. [bench/REPORT.md](../bench/REPORT.md) and
[scope.md](./scope.md#verification-status) say the same.

**This remains the largest outstanding gap in the project.**
