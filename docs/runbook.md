# Operations runbook

Symptom-first. Each section: what you would see, what it means, what to do.

## Signals worth alerting on

| Metric                                             | Alert when                 | Why                                                                         |
| -------------------------------------------------- | -------------------------- | --------------------------------------------------------------------------- |
| `jobq_job_queue_wait_seconds` p99                  | > 60s for 5 min            | The backlog is not draining. The single most useful queue signal.           |
| `jobq_queue_depth{state="available"}`              | rising for 15 min          | Arrival rate exceeds processing rate.                                       |
| `jobq_leases_expired_total`                        | rate > 0 sustained         | Workers are dying or handlers are outliving leases without heartbeating.    |
| `jobq_stale_completion_total`                      | rate > 0 sustained         | Workers are losing leases mid-execution. Rare is fine; sustained is not.    |
| `jobq_dlq_size`                                    | any increase               | Jobs are being abandoned. Never let this go unwatched.                      |
| `jobq_overdue_leases`                              | > 0 for 2 min              | The reaper is behind. Jobs appear stuck in `running`.                       |
| `jobq_db_dead_tuples{table="job"}`                 | trending up over hours     | Autovacuum is losing. Precedes performance collapse.                        |
| `jobq_db_last_autovacuum_age_seconds{table="job"}` | > 3600                     | The job table has not been vacuumed in an hour.                             |
| `jobq_notify_received_total`                       | flat while enqueues happen | `LISTEN` is broken; you are on poll-interval latency.                       |
| `jobq_claim_duration_seconds` p99                  | > 100ms                    | The claim index is probably not being used, or contention is past the knee. |

Thresholds are starting points, not universals. Tune to your arrival rate.

---

## Backlog is growing

**Symptom:** `jobq_queue_depth{state="available"}` rising, queue wait p99 climbing.

1. **Are workers alive and claiming?**

   ```
   jobq_jobs_in_flight        # should be > 0 and near concurrency under load
   jobq_claim_batch_size      # avg near your batch max means workers are saturated
   ```

   Zero in-flight with a non-empty backlog means workers are not claiming — check the
   next question.

2. **Is the queue paused?** A paused queue accepts enqueues and suppresses claiming,
   which looks exactly like this.

   ```sql
   SELECT queue, paused FROM queue_config WHERE paused;
   ```

   ```
   POST /v1/queues/{queue}/resume
   ```

3. **Do the workers subscribe to this queue?** `WORKER_QUEUES` is a comma-separated
   allowlist. A queue nobody subscribes to fills up silently. This is the most common
   cause and the easiest to overlook.

4. **Is throughput at the ceiling?** Check `jobq_claim_duration_seconds` p99 against the
   knee in [bench/REPORT.md](../bench/REPORT.md). **If you are past the knee, adding
   workers will make it worse** — 16→32 workers measured 1% more throughput for 6.5× worse
   p99. Shard queues across databases instead.

5. **Are handlers just slow?** `jobq_job_duration_seconds` distinguishes "queue is slow"
   from "work is slow". These need opposite responses.

---

## Rising retry rate

**Symptom:** `jobq_retries_scheduled_total` rate climbing; jobs cycling
`available → running → available`.

1. **Which type, and what error?**

   ```sql
   SELECT type, count(*), max(attempt) AS max_attempt,
          (errors -> -1 -> 'message')::text AS latest_error
     FROM job
    WHERE state = 'available' AND attempt > 0
    GROUP BY type, latest_error
    ORDER BY count(*) DESC LIMIT 20;
   ```

2. **A downstream dependency is probably down.** Full jitter is already spreading the
   retries, so you will not see a synchronized burst — but the retries are still load on
   something that is struggling.

3. **Pause the queue** if the retries are making recovery harder. Enqueue continues, so
   nothing is lost:

   ```
   POST /v1/queues/{queue}/pause
   ```

   Resume when the dependency is healthy.

4. **If the upstream returns `Retry-After`,** have the handler throw `RetryAfterError`
   with that delay rather than letting computed backoff guess. Guessing shorter just gets
   throttled again.

---

## A poison payload

**Symptom:** one job type repeatedly dead-lettering, or a job that kills its worker.

1. **Inspect the DLQ:**

   ```
   GET /v1/dlq?type=the.type&limit=50
   ```

   Or in the dashboard's Dead letter tab, which shows the full attempt history and stack
   traces.

2. **Distinguish the two cases.** `dead_reason` tells you which:
   - `non_retryable_error` — the handler correctly identified unfixable input. Working as
     intended.
   - `attempts_exhausted` — it retried and kept failing. Either transient-but-long, or the
     handler should be throwing `NonRetryableError`.
   - `lease_expired` — the worker died every time. **This is the dangerous one:** the job
     may be killing its worker (OOM, infinite loop). Because `attempt` increments at claim
     time, it is bounded and cannot destroy your fleet indefinitely — but check
     `jobq_leases_expired_total` for correlation with worker restarts.

3. **Fix, then replay:**

   ```
   POST /v1/dlq/replay   { "ids": ["123", "124"] }
   ```

   Replay resets `attempt` to zero and preserves error history.

4. **If a handler should never retry a class of input,** throw `NonRetryableError`. It
   dead-letters immediately instead of burning four more attempts on identical failure.

---

## Jobs stuck in `running`

**Symptom:** `jobq_overdue_leases > 0`, jobs in `running` with `lease_expires_at` in the
past.

1. **Is the reaper running?** It runs in both the API and worker processes. If neither is
   up with `REAPER_ENABLED=true`, nothing recovers leases.

   ```sql
   SELECT count(*) FROM job WHERE state = 'running' AND lease_expires_at < now();
   ```

2. **Is it keeping up?** A large simultaneous expiry (a whole host lost) drains in bounded
   batches of `REAPER_BATCH_MAX` per pass — deliberately, so recovery does not become one
   long transaction holding back vacuum. Raise `REAPER_BATCH_MAX` or lower
   `REAPER_INTERVAL_MS` if it is persistently behind.

3. **Are handlers outliving their leases?** A handler slower than `lease_seconds` that
   does not heartbeat gets reaped mid-execution and re-delivered while still running.
   Either call `ctx.heartbeat()` in the handler's loop, or raise `leaseSeconds` for that
   job type. The runner heartbeats automatically at `WORKER_HEARTBEAT_FRACTION` of the
   lease, so this only bites handlers that block the event loop.

---

## Lease-expiry storm

**Symptom:** `jobq_leases_expired_total` spikes, usually right after a deploy or host loss.

Mostly self-correcting. Recovery is jittered specifically for this: every job a dead host
held expires within the same second, and un-jittered recovery would return them all with
an identical `run_at` — a thundering herd on a system already down capacity.

1. **Confirm workers came back:** `jobq_jobs_in_flight` should recover.
2. **Check for burned attempts.** Every job in flight consumed an attempt. Anything on its
   final attempt dead-lettered with `lease_expired`:
   ```sql
   SELECT count(*) FROM job WHERE state='dead' AND dead_reason='lease_expired'
     AND finished_at > now() - interval '1 hour';
   ```
   Those need replaying — they failed for reasons unrelated to their own payload.
3. **Prevent it:** graceful shutdown releases in-flight jobs for immediate re-claim rather
   than leaving them to their leases. If you are seeing storms on every deploy, your
   orchestrator's `SIGTERM` grace period is shorter than `WORKER_SHUTDOWN_GRACE_MS`, so
   the drain never completes.

---

## Bloat remediation

**Symptom:** `jobq_db_dead_tuples` trending up, table and index size growing, claim
latency degrading.

### Diagnose

```sql
SELECT relname,
       n_live_tup, n_dead_tup,
       round(100.0 * n_dead_tup / GREATEST(n_live_tup + n_dead_tup, 1), 1) AS dead_pct,
       last_autovacuum, last_vacuum,
       pg_size_pretty(pg_total_relation_size(relid)) AS total,
       pg_size_pretty(pg_indexes_size(relid))        AS indexes
  FROM pg_stat_user_tables
 WHERE relname IN ('job', 'job_archive');
```

Dead percentage above ~20% sustained means autovacuum is losing.

### Check the usual causes first

1. **Is retention running?** `RETENTION_ENABLED=true`, and `jobq_retention_pruned_total`
   should be increasing. A silently failed retention worker is the most common cause of
   unbounded growth.
2. **Is anything holding the vacuum horizon?** A long-running transaction anywhere in the
   database blocks vacuum for the whole database:
   ```sql
   SELECT pid, state, age(clock_timestamp(), xact_start) AS xact_age, query
     FROM pg_stat_activity
    WHERE xact_start IS NOT NULL
    ORDER BY xact_start LIMIT 10;
   ```
   Long-lived idle-in-transaction sessions are the classic offender, and they are usually
   _not_ the queue.

### Remediate

In increasing order of disruption.

**1. Vacuum manually** (non-blocking, always safe):

```sql
VACUUM (VERBOSE, ANALYZE) job;
```

**2. Rebuild indexes** (`REINDEX CONCURRENTLY` does not block reads or writes):

```sql
REINDEX INDEX CONCURRENTLY job_claim_idx;
REINDEX TABLE CONCURRENTLY job;
```

Needs free disk space equal to the index size. Interrupting it leaves an invalid index
behind, which must be dropped:

```sql
SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;
```

**3. Compact the heap** — pick one:

- **`pg_repack`** (preferred): rebuilds without a long exclusive lock. Needs the extension
  installed and free space roughly equal to the table.
- **`VACUUM FULL job`**: takes an **`ACCESS EXCLUSIVE` lock for its entire duration**. All
  claiming, enqueuing, and reading of the job table blocks until it finishes. On a large
  table that is minutes. **Maintenance window only** — running this during business hours
  will stop your queue dead.

**4. If it keeps recurring,** tune further:

```sql
ALTER TABLE job SET (autovacuum_vacuum_scale_factor = 0.005,
                     autovacuum_vacuum_cost_limit = 4000);
```

And shorten retention windows in `queue_config` so the table stays smaller.

---

## Notifications stopped arriving

**Symptom:** `jobq_notify_received_total` flat while jobs are being enqueued. Jobs still
process, just with poll-interval latency.

1. **This is not a correctness problem.** Polling is the guarantee; `NOTIFY` is a latency
   optimization. Nothing is lost.
2. **Almost always a transaction-mode pooler.** `LISTEN` state does not survive between
   transactions behind PgBouncer transaction mode or Supabase's pooler port. Point
   `DATABASE_URL` at a **direct** connection for the worker.
3. **Check the listener reconnected.** Worker logs will show
   `notification listener disconnected; polling continues to serve jobs meanwhile`.
4. **Check the notify queue is not full:**
   ```sql
   SELECT pg_notification_queue_usage();
   ```
   Approaching 1.0 means a listener is not draining. Payloads here are queue names only,
   so this should not happen — investigate other `LISTEN` users in the database.

---

## Migration rollback

Migrations are **forward-only**. There is no `down`, and `migrate down` refuses and exits
non-zero rather than pretending.

**To reverse a schema change:** write a new migration that undoes it. Every environment
then converges through the same ordered sequence, which is the property that makes
forward-only safe.

**If a migration fails partway:** it cannot. Each migration runs in one transaction
together with its bookkeeping row, so either both commit or neither does. Fix the SQL and
re-run.

**If you edited a merged migration:** the runner refuses to start:

```
Migration 3 (003_x.sql) has changed since it was applied
```

Correct — editing a merged migration silently diverges every environment that already
applied the old text. Revert the edit and add a new migration.

**Local reset** (destroys all data):

```bash
npm run db:reset && npm run migrate:up && npm run seed
```

---

## Emergency: stop processing now

```
POST /v1/queues/{queue}/pause
```

Enqueue continues; claiming stops. Nothing is lost and callers do not start failing. This
is almost always better than scaling workers to zero, which strands in-flight jobs until
their leases expire.

To stop everything, pause each queue — `GET /v1/queues` lists them.

---

## Emergency: discard a queue's backlog

Destructive and irreversible.

```
POST /v1/queues/{queue}/purge
{ "states": ["available"], "confirm": "{queue}" }
```

`confirm` must equal the queue name, so a mis-aimed call cannot destroy a different
queue. Deletes in bounded batches and reports the row count. Prefer purging only
`available` — purging `running` deletes rows out from under executing workers, whose
completions will then fail the ownership fence.
