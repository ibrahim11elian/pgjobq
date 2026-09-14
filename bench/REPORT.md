# Benchmark report

Every number here was produced by `bench/harness/index.ts` on the hardware described
below. Reproduce with the command in each section. Nothing in this file is estimated.

## Methodology

The harness drives the **engine** directly, not the HTTP API. The figure worth
publishing is what the claim path sustains; routing through Express would measure
Express. Handlers are a no-op for the same reason — the measurement is of the queue,
not of whatever work a handler does.

Each run:

1. Seeds N jobs into a fresh queue, then `ANALYZE job` so the planner has current
   statistics. Without the analyze, the first run measures a sequential-scan plan on
   stale estimates rather than the claim index.
2. Starts _C_ concurrent workers, each looping: claim up to 20 jobs, then issue one
   completion statement per job.
3. Stops when a worker finds the queue empty.
4. Reports wall-clock throughput and percentiles from every claim statement.

So each job costs roughly two statements: a shared claim (amortized 20 ways) and its
own completion. Throughput here is therefore a **queue** figure, not a "jobs a real
system will process per second" figure — real handlers do work that dwarfs this.

### Environment

|                 |                                               |
| --------------- | --------------------------------------------- |
| PostgreSQL      | 18.6 (`postgres:18.6-alpine`, Docker Desktop) |
| Node.js         | v22.22.2                                      |
| Platform        | win32 x64                                     |
| Jobs per run    | 20,000                                        |
| Claim batch max | 20                                            |
| Payload size    | ~200 bytes                                    |
| Pool            | 42 connections                                |

**This environment understates absolute throughput.** Postgres is in Docker Desktop on
Windows, which adds virtualized filesystem and network overhead on every statement.
The claim p50 of ~5ms is dominated by that round trip, not by Postgres itself — on
Linux with a local socket, expect materially better absolute numbers. The _shape_ of
the results (scaling efficiency, the concurrency knee, the lock-mode comparison) is
the transferable part.

## Throughput and latency

```
npm run bench -- --jobs 20000 --concurrency 4,8,16,32 --lock both
```

| Lock mode           | Workers |    Jobs/s | Claim p50 | Claim p95 | Claim p99 | Avg batch |
| ------------------- | ------: | --------: | --------: | --------: | --------: | --------: |
| `FOR UPDATE`        |       4 |     1,090 |    4.93ms |    6.95ms |   11.03ms |      20.0 |
| `FOR UPDATE`        |       8 |     2,034 |    5.08ms |    7.70ms |   15.72ms |      20.0 |
| `FOR UPDATE`        |      16 | **3,176** |    6.49ms |   11.00ms |   30.58ms |      20.0 |
| `FOR UPDATE`        |      32 |     3,203 |   11.92ms |   29.64ms |  198.27ms |      20.0 |
| `FOR NO KEY UPDATE` |       4 |       992 |    6.13ms |    9.66ms |   14.69ms |      20.0 |
| `FOR NO KEY UPDATE` |       8 |     1,062 |    9.93ms |   13.29ms |   27.04ms |      20.0 |
| `FOR NO KEY UPDATE` |      16 |     1,913 |    9.86ms |   17.18ms |   33.01ms |      20.0 |
| `FOR NO KEY UPDATE` |      32 |     2,642 |   13.79ms |   28.77ms |  132.39ms |      20.0 |

Average batch was 20.0 — the configured maximum — in every run. Workers never went
hungry, so these figures measure the claim path under real contention rather than an
under-fed queue.

## Where it stops scaling

This is the useful result, more than the peak number.

| Workers | Throughput gain | Worker increase | Scaling efficiency |
| ------- | --------------: | --------------: | -----------------: |
| 4 → 8   |           1.87× |            2.0× |            **93%** |
| 8 → 16  |           1.56× |            2.0× |            **78%** |
| 16 → 32 |           1.01× |            2.0× |            **50%** |

**The knee is at 16 workers.** Going from 16 to 32 bought 1% more throughput while
claim p99 went from 30.58ms to 198.27ms — a 6.5× latency regression for nothing. Past
this point the limit is contention on the claim path, not worker count, and adding
workers actively makes the system worse.

That is the number to act on: for this configuration, size the pool at 16 and scale
further by sharding queues across databases, not by adding workers.

## Lock mode: measurement disagrees with theory

The design predicted `FOR NO KEY UPDATE` would be equal or better. It is sufficient
for mutual exclusion (two concurrent `FOR NO KEY UPDATE` requests conflict, so
`SKIP LOCKED` still hands each worker a distinct row), it does not block `FOR KEY
SHARE` lockers, and it escalates to MultiXact entries less readily. The claim updates
no key column, so the stronger lock should buy nothing.

**It lost, consistently, at every concurrency level** — 2,642 vs 3,203 jobs/s at peak,
and 1,062 vs 2,034 at 8 workers. Claim p50 was worse in every single run.

Honest position: this project keeps `FOR UPDATE` as the default because that is what
measured faster here, and records that the theoretical argument for the weaker lock did
not survive contact with a measurement. What this benchmark does **not** establish is
_why_. Candidate explanations — planner differences, the Docker round trip dominating
and masking lock costs, or MultiXact pressure simply never becoming the bottleneck at
this scale — are untested. Confirming the mechanism would need `pg_stat_activity` wait
event sampling during both runs, which is not yet implemented.

The result is reported because it is what happened, not because it is understood.

## Bloat across one run

| Metric      |  Before |    After |
| ----------- | ------: | -------: |
| Table size  | 73.9 MB | 109.4 MB |
| Index size  | 33.8 MB |  60.2 MB |
| Dead tuples | 169,212 |  121,936 |

A single 8-run pass over 160,000 jobs grew the table and doubled index size. That is
expected and is not the interesting measurement: every job is updated twice (claim,
completion) and copy-on-write leaves a dead tuple each time.

**This does not yet demonstrate that retention plus autovacuum holds size steady.**
Requirement 17.9 asks for a sustained high-churn run with the retention worker active,
measuring whether size returns to a steady state. That measurement has not been run.
Until it has, the bloat claim in `docs/scope.md` is reasoning, not evidence.

Dead tuples fell during the run, so autovacuum was working — but a single pass cannot
distinguish "keeping up" from "briefly ahead".

## What is not measured yet

Stated plainly so no reader assumes otherwise:

- **The kill test.** `bench/chaos/kill-test.ts` and `bench/chaos/durability.ts` now exist
  but have **never been executed** — they were written in a session without command
  execution. No results are reported here because none have been produced. The headline
  exactly-once-effect claim is still demonstrated only by the deterministic smoke checks.
  Run `npm run chaos` and `npm run chaos:durability`, then add the output to this file.
- **Sustained bloat steady state**, per above.
- **MultiXact wait events**, which would explain the lock-mode result.
- **Comparison against `pg-boss` and `graphile-worker`** on identical hardware.
- **Linux numbers.** Everything here carries Docker Desktop on Windows overhead.
- **HTTP API throughput.** The API adds validation, auth, and rate limiting per
  request; none of that is in these figures.

## Reproducing

```bash
npm run db:up
npm run migrate:up
npm run bench -- --jobs 20000 --concurrency 4,8,16,32 --lock both
```

Flags: `--jobs`, `--concurrency` (comma-separated sweep), `--batch`, `--payload`
(bytes), `--lock` (`for_update` | `for_no_key_update` | `both`).
