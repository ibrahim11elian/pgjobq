# Deployment

Covers connection sizing, the one configuration mistake that silently degrades the
system, container deployment, and a production checklist.

## Connection sizing

The most common way to break a Postgres-backed queue. Every process holds a pool, and
they add up faster than people expect.

```
total = api_instances    × DB_POOL_MAX
      + worker_instances × (DB_POOL_MAX + 1 listener + 1 background)
      + your application's own pool
      + headroom for migrations, psql, and monitoring
```

The `+1 listener` is the one people forget. Each worker holds a **dedicated** connection
for `LISTEN` that is deliberately outside the pool, because `LISTEN` registers session
state and a pooled connection handed to someone else would lose it.

Worked example — 3 API instances, 2 workers, `DB_POOL_MAX=10`:

```
api      3 × 10           = 30
workers  2 × (10 + 1 + 1) = 24
                            ──
                            54  plus your app's pool and ~10 headroom
```

Against a managed Postgres with `max_connections=100`, that leaves very little room. Check
your provider's limit before scaling out:

```sql
SHOW max_connections;
SELECT count(*), application_name FROM pg_stat_activity GROUP BY application_name;
```

`application_name` is set per process (`pgjobq-api`, `pgjobq-worker`, `pgjobq-listener`),
so that second query tells you exactly who is using what.

### If you run out

In order of preference:

1. **Lower `DB_POOL_MAX`.** A worker with concurrency 10 rarely needs 10 connections —
   handlers spend most of their time in their own work, not in queries. Start at 5.
2. **Fewer, larger workers.** One worker with concurrency 20 uses far fewer connections
   than two with concurrency 10.
3. **A connection pooler**, with the critical caveat in the next section.

Do not raise `max_connections` as a first move. Each Postgres connection costs real memory,
and a high limit converts a connection problem into a memory problem.

## The `LISTEN` trap

**The worker's `DATABASE_URL` must be a DIRECT connection.**

`LISTEN` state does not survive a transaction-mode pooler — PgBouncer in `transaction`
mode, or Supabase's pooler port (6543). Such a pooler multiplexes sessions across
backends, so the session that issued `LISTEN` is not the session that receives the
notification.

What makes this genuinely nasty is that **nothing breaks**. Polling is the
correctness-bearing path, so every job still runs. The only symptom is latency quietly
rising from milliseconds to the poll interval (default 1 second). No error, no alert.

How to detect it:

```
jobq_notify_received_total     # flat while jobs are being enqueued → LISTEN is dead
```

If you must use a pooler, give the worker two URLs — the pooled one for its query pool,
the direct one for `listenConnectionString`:

```ts
new WorkerPool({
  db: new Db({ connectionString: process.env.DATABASE_URL_POOLED! }),
  listenConnectionString: process.env.DATABASE_URL_DIRECT!,
  // ...
});
```

Session-mode pooling is fine. Only transaction and statement mode break `LISTEN`.

## Processes

Two entry points from one image. The queue is the coordination mechanism, so they need no
knowledge of each other.

```bash
# API + dashboard backend
node packages/server/dist/main.js

# Worker
node packages/worker/dist/main.js
```

```yaml
# docker-compose production sketch
services:
  api:
    image: pgjobq:latest
    command: ['node', 'packages/server/dist/main.js']
    environment:
      DATABASE_URL: ${DATABASE_URL}
      API_BOOTSTRAP_KEY: ${API_BOOTSTRAP_KEY}
      NODE_ENV: production
    ports: ['3001:3001']

  worker:
    image: pgjobq:latest
    command: ['node', 'packages/worker/dist/main.js']
    environment:
      DATABASE_URL: ${DATABASE_URL_DIRECT}
      WORKER_QUEUES: default,email,images
      WORKER_CONCURRENCY: '10'
      NODE_ENV: production
    deploy:
      replicas: 2
```

### Which loops run where

`REAPER_ENABLED`, `SCHEDULER_ENABLED`, and `RETENTION_ENABLED` default to `true` in **both**
processes. That is safe by construction — every loop claims its own work with
`SKIP LOCKED`, so concurrent copies divide the work instead of colliding. There is no
leader to elect.

Leave them on in both. The failure mode of disabling them is worse than the small
duplicate polling cost: if the only process running the reaper is down, expired leases are
never recovered and jobs sit in `running` forever.

### Scaling

**Cap workers at the measured knee.** [bench/REPORT.md](../bench/REPORT.md) found
throughput stops improving past 16 concurrent claimers per database, while claim p99 got
6.5× worse. Past that point, adding workers converts throughput into latency.

To go further, shard queues across separate databases. Do not add workers.

## Migrations

Run **before** the new version starts, as a separate step:

```bash
node -e "
  const { Db, migrateUp, createLogger } = await import('@pgjobq/core');
  const db = new Db({ connectionString: process.env.DATABASE_URL });
  await migrateUp(db, { logger: createLogger({ pretty: false }) });
  await db.close();
"
```

Both the API and the worker verify the schema version at startup and **refuse to serve** if
it does not match. That is deliberate: serving against an out-of-date schema produces
errors that look like application bugs, and failing loudly names the real cause
immediately.

Consequence for rolling deploys: apply migrations first, and keep them
backward-compatible with the version still running. Migrations are forward-only — a
reversal is a new migration. See [runbook.md](./runbook.md#migration-rollback).

## Free-tier hosting

The demo runs at zero cost. Sensible options as of 2026:

| Piece | Option | Notes |
| --- | --- | --- |
| Postgres | Neon, Supabase | Both scale to zero. **Use the direct connection string for workers.** |
| API + worker | Fly.io, Railway, Render | Two processes from one image. Render free tier sleeps, which is fine for a demo. |
| Dashboard | Vercel, Netlify, Cloudflare Pages | Static build; point it at the API origin. |

Two things to get right on a free tier:

**Scale-to-zero and polling interact badly.** A worker polling every second keeps a
scale-to-zero database permanently awake, which can burn a monthly allowance in days. Raise
`WORKER_POLL_INTERVAL_MS` to 10–30 seconds for a demo and rely on `NOTIFY` for latency.

**Put a hard spending cap on any paid account before exposing a public URL.** A demo that
goes viral should cost you nothing.

## Production checklist

Configuration validation already blocks the worst mistakes — production mode refuses to
start without `API_BOOTSTRAP_KEY`, rejects a key under 32 characters, rejects one that looks
like a placeholder, and rejects `LOG_PAYLOADS=true`. The rest is on you.

**Security**

- [ ] `API_BOOTSTRAP_KEY` is a freshly generated random value, not copied from `.env.example`
- [ ] Rotate the bootstrap key: create scoped keys for real callers, then revoke it
- [ ] Scope keys narrowly — an enqueue-only service does not need `admin`
- [ ] `LOG_PAYLOADS=false` (enforced, but confirm)
- [ ] `/metrics` is not publicly reachable; it is not behind API-key auth by design, so use
      network-level access control
- [ ] The dashboard is **not** exposed publicly as-is — it holds an API key in
      `sessionStorage`. Put a session cookie and a server-side proxy in front of it first.
- [ ] TLS on the database connection (`?sslmode=require`)

**Reliability**

- [ ] Connection total is under `max_connections` with headroom, per the maths above
- [ ] The worker's `DATABASE_URL` is direct, not transaction-pooled
- [ ] Orchestrator `SIGTERM` grace period exceeds `WORKER_SHUTDOWN_GRACE_MS`, or the drain
      never completes and every deploy strands in-flight jobs for a lease period
- [ ] Container runs under an init that forwards signals — the shipped `Dockerfile` uses
      `dumb-init`; without one, PID 1 ignores `SIGTERM`
- [ ] `RETENTION_ENABLED=true`, or the job table grows without bound
- [ ] Migrations applied before the new version starts

**Observability**

- [ ] Prometheus is scraping `/metrics`
- [ ] Alerts on the signals in [runbook.md](./runbook.md#signals-worth-alerting-on) — at
      minimum queue wait p99, DLQ size, and `jobq_db_dead_tuples`
- [ ] Liveness probe on `/health`, readiness on `/ready`. **Do not point liveness at
      `/ready`** — that ties container health to the database, so a brief outage restarts
      every container and turns a recoverable blip into a real one.

## Environment variables

Every variable is validated at startup, with all problems reported at once rather than one
per restart. Full annotated list in [`.env.example`](../.env.example). The ones that matter
most in production:

| Variable | Why it matters |
| --- | --- |
| `DATABASE_URL` | Must be **direct** for workers, per the `LISTEN` trap |
| `API_BOOTSTRAP_KEY` | Required in production; ≥32 chars; no placeholder-looking values |
| `DB_POOL_MAX` | Feeds the sizing formula above |
| `WORKER_CONCURRENCY` | Cap at the measured knee (16 across all workers per database) |
| `WORKER_SHUTDOWN_GRACE_MS` | Must be under your orchestrator's termination grace period |
| `WORKER_POLL_INTERVAL_MS` | Raise on scale-to-zero databases to avoid burning quota |
| `RETENTION_ENABLED` | Leave `true`; the table grows without bound otherwise |
| `LOG_PAYLOADS` | Keep `false`; payloads routinely carry personal data |
