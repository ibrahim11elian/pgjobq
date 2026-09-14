# Contributing

## Setup

Needs Docker and Node.js 22.11+.

```bash
npm install
cp .env.example .env
npm run db:up          # Postgres 18.6 on host port 5433
npm run migrate:up
npm run seed
```

Then `npm run dev:api`, `npm run dev:worker`, `npm run dev:dashboard` in separate terminals.

Ports are 5433 and 3001 rather than 5432 and 3000 because both defaults collide with
software commonly already running. If you hit a collision anyway, change it in `.env` —
and note that the dashboard's Vite config uses `strictPort`, so it fails loudly instead of
silently moving and breaking the configured CORS origin.

## Before you open a PR

```bash
npm run build          # tsc --build across the workspace
npm run lint
npm run format:check
npm run smoke          # 54 checks against real Postgres
npm run api:smoke      # 55 checks against a running API
npm run gen:docs       # regenerate docs/state-machine.md
```

CI runs all of these plus the container build and a Trivy scan.

**Rebuild `core` after changing it.** The server and worker import `@pgjobq/core` through
its `exports` map, which points at `dist/`, not `src/`. Editing core and restarting the
server without rebuilding means you are running stale code. This cost real debugging time
during the initial build; do not repeat it.

## The rules that matter

These are the invariants a PR will be rejected over. Every one of them exists because
violating it produces a lost or double-delivered job, and most are explained in
[docs/engineering-decisions.md](docs/engineering-decisions.md):

1. **Every SQL statement lives in `packages/core/src/sql/`**, one per file. No SQL inline in
   engine or route code.
2. **Parameterized statements only.** The `sql()` helper is branded so passing an
   interpolated string is a type error. Do not defeat it.
3. **The claim is one statement.** Never split it into `SELECT ... FOR UPDATE` plus a
   separate `UPDATE` — that reintroduces the window the whole design exists to close.
4. **`FOR UPDATE OF j`**, not bare `FOR UPDATE`, in any statement that joins another table.
   A bare one both errors on an outer join and would serialize every claim against the
   joined row.
5. **`attempt` increments at claim time**, never at failure time.
6. **Every completion, failure, and heartbeat carries the ownership fence:**
   `WHERE id = $1 AND state = 'running' AND worker_id = $2 AND attempt = $3`. Zero rows means
   ownership was lost — log and meter it, never retry or force the state.
7. **Retry-or-dead-letter is decided inside the SQL statement**, from the row's own committed
   values. Deciding it in application code races the reaper.
8. **`now()` for all time.** Never a client clock.
9. **`SKIP LOCKED` on every background scan**, so each loop is safe to run on every instance
   with no leader election.
10. **No long transactions.** Bound every batch. A long transaction holds back the vacuum
    horizon, which is the bloat mechanism the design guards against.
11. **`NOTIFY` is an optimization; polling is the guarantee.** Never make correctness depend
    on a notification arriving.

## Delivery semantics — wording

**At-least-once with idempotency support.** Never describe it as exactly-once, anywhere:
code comments, docs, commit messages, or PR descriptions. See
[docs/delivery-guarantees.md](docs/delivery-guarantees.md).

## Testing

**No mocked database, anywhere.** The behaviour under test — `SKIP LOCKED` semantics, lock
conflict matrices, index selection — only a real Postgres exhibits. A mock would test the
mock. Testcontainers makes a real one cheap enough that there is no excuse.

The chaos test uses a real `SIGKILL` on a real child process. A simulated crash tests the
path the developer imagined; `SIGKILL` tests the one that actually happens, with no chance
for cleanup handlers to run.

Concurrency tests should include the small-backlog-many-workers case. That is the worst case
for lock contention, so it is the one that finds bugs.

## Claims must be backed

If you add a performance or guarantee claim to the README or docs, add the test or benchmark
that establishes it in the same PR. If you cannot, say so explicitly rather than stating it
plainly — `docs/scope.md` has a verification-status table for exactly this, and it lists
what is _not_ proven as prominently as what is.

## Documentation that generates

`docs/state-machine.md` is generated from the transition table in
`packages/core/src/engine/state-machine.ts`. Do not edit it by hand; change the table and run
`npm run gen:docs`. CI fails if the committed file differs from the generated one, because a
diagram that disagrees with the implementation is worse than none — it gets trusted.

## Commits

Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `perf:`, `chore:`). Explain
_why_ in the body, not what — the diff already shows what.

## Adding a migration

Forward-only, numbered, and **never edited once merged**. The runner stores a checksum and
hard-fails if an applied migration's text changes, because editing one silently diverges
every environment that already applied the old text.

To reverse something, add a new migration. There is no `down`.
