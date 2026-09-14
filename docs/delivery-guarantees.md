# Delivery guarantees

## The guarantee, stated precisely

**pgjobq provides at-least-once delivery with idempotency support.**

It does **not** provide exactly-once delivery, and no system that runs your code in a
separate process can. Section "Why not exactly-once" explains why in one paragraph.

### What is guaranteed

1. **No concurrent double-delivery.** A job is never in the `running` state for two
   workers at the same instant. Guaranteed by `SELECT ... FOR UPDATE SKIP LOCKED`
   within a single atomic claim statement.
2. **No silent loss.** Once an enqueue transaction commits, the job either reaches a
   terminal state or remains visible in a non-terminal one. It is never dropped.
3. **Bounded attempts.** No job is delivered more than `max_attempts` times, under any
   combination of handler failure, timeout, worker crash, and lease expiry. Enforced by
   a database `CHECK` constraint, not merely by application logic.
4. **Idempotent enqueue.** With an `idempotencyKey`, concurrent duplicate enqueues
   produce exactly one job. Enforced by a unique index, not a read-then-write check.
5. **Crash recovery.** A job whose worker dies is re-delivered once its lease expires,
   or dead-lettered if it was on its final attempt.
6. **Exactly-once _effect_, conditionally.** If your handler is idempotent, its effect
   occurs exactly once regardless of how many times the job is delivered.

Point 6 is where your code is part of the guarantee, and it is not a get-out clause —
it is the standard contract of every at-least-once queue. The rest of this document is
about holding up your end of it.

## The duplicate-execution window

There is exactly one window in which a job runs twice. It is not hidden and it is not
closable:

```
  worker A                                          database
  ────────────────────────────────────────────────────────────────────
  claim job 42, attempt 1                     →     state = running
                                                    lease expires at T+30
  handler runs
  handler COMMITS its side effect             →     [charge card, send email]
  ─────────────── worker A's process dies here ───────────────
                                                    lease expires at T+30
  reaper recovers job 42                      →     state = available
  worker B claims job 42, attempt 2           →     state = running
  handler runs AGAIN
  handler commits its side effect AGAIN       →     [charge card TWICE]
```

The side effect committed. The record that it committed did not. Nothing the queue can
do closes this: the two commits are to different systems, and there is no moment at
which both are atomically durable.

A narrower variant has the same shape without a crash: worker A stalls (GC pause, host
suspend, network partition), its lease expires, worker B takes the job, and A then wakes
and tries to record completion. **That case is caught** — see the ownership fence
below — but only the _recording_ is caught. If A had already committed its side effect
before stalling, the effect still happened twice.

## What catches the recoverable half: the ownership fence

Every write that reports on a job carries the claiming worker's identity **and** the
attempt number it was claimed on:

```sql
WHERE id = $1 AND state = 'running' AND worker_id = $2 AND attempt = $3
```

Zero rows affected means ownership was lost. The worker discards its result and
increments `jobq_stale_completion_total`. This is a **normal outcome** under partition,
not a bug and not retryable — forcing the write would mark a job done while another
worker is actively running it.

Both predicates are load-bearing. A `worker_id`-only fence would be passed by a worker
that legitimately reclaimed the same job on a later attempt.

Without the fence, this interleaving silently corrupts state:

| Time | Worker A                  | Reaper                  | Worker B           | Job state                          |
| ---- | ------------------------- | ----------------------- | ------------------ | ---------------------------------- |
| T0   | claims, attempt 1         |                         |                    | `running`, A, 1                    |
| T1   | stalls                    |                         |                    | `running`, A, 1                    |
| T2   |                           | lease expired → recover |                    | `available`, —, 1                  |
| T3   |                           |                         | claims, attempt 2  | `running`, B, 2                    |
| T4   | wakes, writes `succeeded` |                         | still running      | **`succeeded` while B executes**   |
| T5   |                           |                         | writes `succeeded` | 0 rows — B is now the confused one |

With the fence, T4 affects zero rows, A logs the loss, and B's completion at T5
succeeds normally.

## Holding up your end: writing an idempotent handler

Every handler receives `ctx.id` and `ctx.attempt`. Key your side effect on **`ctx.id`,
never `ctx.attempt`** — the job id is stable across re-delivery, the attempt number is
not.

```ts
registry.register(
  'payment.charge',
  z.object({ orderId: z.string(), amountCents: z.number().int() }),
  async (payload, ctx) => {
    // The job id is the idempotency key. A re-delivery presents the same key, so the
    // payment processor rejects the duplicate rather than charging twice.
    await stripe.charges.create(
      { amount: payload.amountCents, currency: 'usd', metadata: { orderId: payload.orderId } },
      { idempotencyKey: `job-${ctx.id}` },
    );
  },
);
```

Three patterns, in order of preference:

**1. Pass an idempotency key to the downstream service.** Stripe, most payment
processors, and many modern APIs support this. It is the only pattern where the
guarantee is enforced by the system that actually matters.

**2. Make the operation naturally idempotent.** `UPDATE ... SET status = 'sent'` is safe
to repeat. `UPDATE ... SET count = count + 1` is not.

**3. Deduplicate in your own database, in the same transaction as the effect.**

```ts
await db.transaction(async (tx) => {
  const claimed = await tx.query(
    `INSERT INTO processed_job (job_id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING job_id`,
    [ctx.id],
  );
  if (claimed.rowCount === 0) return; // already applied
  await tx.query(`UPDATE account SET balance = balance - $1 WHERE id = $2`, [amount, id]);
});
```

The marker insert and the effect must be in **one transaction**. Split them and you have
reproduced the original problem one layer down.

### What does not work

- **Checking "have I done this?" before acting, in a separate query.** Two workers both
  read "no", both act. The check must be a constraint, not a read.
- **Keying on `ctx.attempt`.** Every re-delivery has a different attempt, so every
  re-delivery looks new.
- **Assuming the timeout stops your handler.** `AbortSignal` is cooperative. A handler
  that ignores it keeps running after the queue has given up on it, and its side effects
  still land. Check `ctx.signal.aborted` around anything expensive.

## Why not exactly-once

Exactly-once delivery across a process boundary requires atomically committing two
things in different systems: your side effect, and the record that it happened. Without
a distributed transaction spanning both — which the queue cannot impose on an arbitrary
downstream, and which nobody wants operationally — there is always an instant where one
has committed and the other has not. Crash in that instant and you either lose the job
or repeat it. This design chooses to repeat, because a repeated idempotent operation is
harmless while a lost job is a defect.

Systems advertising "exactly-once" are doing one of two things: exactly-once _processing_
within their own transactional boundary (which is real, but only covers effects inside
that boundary), or at-least-once delivery plus deduplication (which is this, with the
deduplication moved somewhere less visible). The second is not worse — but calling it
exactly-once misleads you about where your idempotency responsibility lies.

## Attempt accounting

`attempt` increments **at claim time**, not at failure time. Consequences:

- A worker that dies without reporting anything **still consumes an attempt**. Without
  this, a job that reliably kills its worker — an out-of-memory payload — would retry
  forever, taking down worker after worker.
- An unrelated crash (deploy, OOM elsewhere, host loss) also burns an attempt. That is
  the accepted cost, and it is why `max_attempts` defaults to 5 rather than 2.
- `attempt` is therefore a count of **deliveries**, not of failures. A job that
  succeeded on its third delivery shows `attempt = 3`.

## Ordering

**Strict global FIFO is not provided.** Jobs are claimed in `priority DESC, run_at, id`
order, but `SKIP LOCKED` means a contended earlier job can be passed over for a later
one. Lock-free concurrent consumption and strict ordering are mutually exclusive; this
design picks concurrency.

If you need strict ordering for a subset of work, give that subset a queue with a single
worker and concurrency 1. You will get ordering and lose parallelism, which is the
actual trade.

## Verification status

Honest accounting of which guarantees are currently backed by an automated check:

| Guarantee                              | Verified by                                                                | Status                        |
| -------------------------------------- | -------------------------------------------------------------------------- | ----------------------------- |
| No concurrent double-delivery          | `npm run smoke` — 6 parallel claimers, asserts no id claimed twice         | Verified                      |
| Ownership fence rejects stale writes   | `npm run smoke` — wrong `worker_id` and wrong `attempt` both affect 0 rows | Verified                      |
| Attempt bound enforced by the database | `npm run smoke` — `attempt > max_attempts` insert rejected                 | Verified                      |
| Idempotent enqueue under concurrency   | `npm run smoke` — 8 concurrent duplicate enqueues produce 1 row            | Verified                      |
| Lease recovery re-delivers             | `npm run smoke` — expired lease recovered, attempt preserved               | Verified                      |
| Non-retryable short-circuits           | `npm run smoke` + worker run — dead at attempt 1                           | Verified                      |
| Exactly-once effect under `SIGKILL`    | **Not yet implemented**                                                    | Requirement 17.2, outstanding |
| No loss across DB connection loss      | **Not yet implemented**                                                    | Requirement 17.3, outstanding |

The two outstanding items are the chaos tests. Until they exist, the exactly-once-effect
claim rests on the design and on the deterministic checks above, not on a
crash-under-load demonstration. That distinction is stated here rather than glossed over.
