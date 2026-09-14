-- pgjobq initial schema.
-- Forward-only. Never edit this file once merged; add a new numbered migration.
--
-- Delivery semantics: at-least-once with idempotency support.

-- ---------------------------------------------------------------------------
-- Enums
-- ---------------------------------------------------------------------------

-- Named to be unambiguous about whether a retry follows. "failed" would not be:
-- a job whose handler threw is back in 'available' if attempts remain.
CREATE TYPE job_state AS ENUM (
  'available',   -- claimable now or at run_at
  'running',     -- leased to exactly one worker
  'succeeded',   -- terminal
  'dead',        -- terminal: attempts exhausted or non-retryable
  'cancelled'    -- terminal: operator intervention
);

CREATE TYPE dead_reason AS ENUM (
  'attempts_exhausted',
  'lease_expired',
  'non_retryable_error',
  'cancelled'
);

-- ---------------------------------------------------------------------------
-- queue_config: per-queue overrides. Created before `job` because the claim
-- statement joins it.
-- ---------------------------------------------------------------------------

CREATE TABLE queue_config (
  queue             text        PRIMARY KEY,
  paused            boolean     NOT NULL DEFAULT false,
  max_attempts      smallint,
  lease_seconds     integer,
  priority_aging    boolean     NOT NULL DEFAULT false,
  aging_threshold_s integer,
  concurrency_limit integer,
  retain_succeeded  interval    NOT NULL DEFAULT '24 hours',
  retain_dead       interval    NOT NULL DEFAULT '30 days',
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT queue_config_max_attempts  CHECK (max_attempts IS NULL OR (max_attempts >= 1 AND max_attempts <= 1000)),
  CONSTRAINT queue_config_lease         CHECK (lease_seconds IS NULL OR lease_seconds > 0),
  CONSTRAINT queue_config_concurrency   CHECK (concurrency_limit IS NULL OR concurrency_limit > 0),
  CONSTRAINT queue_config_aging         CHECK (aging_threshold_s IS NULL OR aging_threshold_s > 0)
);

-- ---------------------------------------------------------------------------
-- schedule: recurring job definitions.
-- ---------------------------------------------------------------------------

CREATE TABLE schedule (
  id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name           text        NOT NULL UNIQUE,
  queue          text        NOT NULL,
  type           text        NOT NULL,
  payload        jsonb       NOT NULL DEFAULT '{}'::jsonb,
  cron           text        NOT NULL,
  -- IANA identifier. Cron is evaluated in this zone, never the server's, so a
  -- server timezone change cannot silently move every schedule.
  timezone       text        NOT NULL DEFAULT 'UTC',
  catchup_policy text        NOT NULL DEFAULT 'skip_missed',
  enabled        boolean     NOT NULL DEFAULT true,
  next_run_at    timestamptz NOT NULL,
  last_run_at    timestamptz,
  last_job_id    bigint,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT schedule_catchup CHECK (catchup_policy IN ('skip_missed', 'run_once'))
);

-- Partial: a disabled schedule is never due, so it does not belong in the index
-- the scheduler scans every tick.
CREATE INDEX schedule_due_idx ON schedule (next_run_at) WHERE enabled;

-- ---------------------------------------------------------------------------
-- job: the queue itself.
-- ---------------------------------------------------------------------------

CREATE TABLE job (
  id                  bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  queue               text        NOT NULL,
  type                text        NOT NULL,
  state               job_state   NOT NULL DEFAULT 'available',

  -- jsonb so anything beyond ~2KB is TOASTed out of line. That is what keeps a
  -- large unchanged payload from being rewritten on every state transition.
  payload             jsonb       NOT NULL,
  metadata            jsonb,

  -- smallint where the range allows: row width drives rows-per-page, and page
  -- density drives claim-index efficiency.
  priority            smallint    NOT NULL DEFAULT 0,
  attempt             smallint    NOT NULL DEFAULT 0,
  max_attempts        smallint    NOT NULL DEFAULT 5,
  lease_seconds       integer     NOT NULL DEFAULT 30,

  run_at              timestamptz NOT NULL DEFAULT now(),
  enqueued_at         timestamptz NOT NULL DEFAULT now(),
  started_at          timestamptz,
  finished_at         timestamptz,
  lease_expires_at    timestamptz,
  cancel_requested_at timestamptz,

  worker_id           text,
  idempotency_key     text,
  unique_key          text,
  dead_reason         dead_reason,
  errors              jsonb       NOT NULL DEFAULT '[]'::jsonb,
  trace_context       text,
  schedule_id         bigint,     -- FK added below, after `schedule` exists
  updated_at          timestamptz NOT NULL DEFAULT now(),

  -- These CHECKs are load-bearing, not decoration.

  -- Makes it impossible for ANY code path -- worker, reaper, or operator -- to
  -- push a job past its attempt limit. A bug surfaces as a failed write rather
  -- than as an unbounded retry loop chewing through workers.
  CONSTRAINT job_attempt_bounded    CHECK (attempt >= 0 AND attempt <= max_attempts),
  CONSTRAINT job_max_attempts_valid CHECK (max_attempts >= 1 AND max_attempts <= 1000),
  CONSTRAINT job_lease_positive     CHECK (lease_seconds > 0),

  -- Makes an unleased 'running' row unrepresentable. That state would leak jobs:
  -- no worker provably owns it and the reaper has no expiry to act on.
  CONSTRAINT job_running_has_lease  CHECK (
    state <> 'running' OR (lease_expires_at IS NOT NULL AND worker_id IS NOT NULL)
  ),
  CONSTRAINT job_dead_has_reason    CHECK (state <> 'dead' OR dead_reason IS NOT NULL),
  CONSTRAINT job_terminal_finished  CHECK (
    state NOT IN ('succeeded', 'dead', 'cancelled') OR finished_at IS NOT NULL
  ),
  CONSTRAINT job_errors_is_array    CHECK (jsonb_typeof(errors) = 'array')
);

ALTER TABLE job
  ADD CONSTRAINT job_schedule_fk
  FOREIGN KEY (schedule_id) REFERENCES schedule (id) ON DELETE SET NULL;

-- ---------------------------------------------------------------------------
-- Indexes. Every hot-path index is partial: an index covering finished rows is a
-- permanent tax on throughput for no benefit to claiming.
-- ---------------------------------------------------------------------------

-- THE claim index. Column order matches the claim statement's ORDER BY exactly so
-- the sort is satisfied by the index with no sort node. Partial on 'available', so
-- its size tracks backlog depth rather than total history -- the single largest
-- contributor to sustained claim performance.
CREATE INDEX job_claim_idx
  ON job (queue, priority DESC, run_at, id)
  WHERE state = 'available';

-- Reaper scan. Only a running row can have an expiring lease.
CREATE INDEX job_lease_idx
  ON job (lease_expires_at)
  WHERE state = 'running';

-- Enqueue idempotency, enforced by the database rather than a read-then-write
-- check, so concurrent duplicate enqueues cannot both succeed.
CREATE UNIQUE INDEX job_idempotency_idx
  ON job (queue, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- Debounce: at most one non-terminal job per unique_key. Because the index covers
-- only non-terminal states, a row reaching a terminal state frees the key
-- automatically -- no cleanup job needed.
CREATE UNIQUE INDEX job_unique_active_idx
  ON job (queue, unique_key)
  WHERE unique_key IS NOT NULL AND state IN ('available', 'running');

-- Retention sweeps.
CREATE INDEX job_retention_idx
  ON job (state, finished_at)
  WHERE state IN ('succeeded', 'dead', 'cancelled');

-- Dashboard keyset pagination. DESC on id because listings are newest-first.
CREATE INDEX job_listing_idx ON job (queue, state, id DESC);
CREATE INDEX job_type_idx    ON job (queue, type, id DESC);

-- DLQ listing filters by reason and time.
CREATE INDEX job_dead_idx ON job (queue, dead_reason, finished_at DESC) WHERE state = 'dead';

CREATE INDEX job_schedule_idx ON job (schedule_id) WHERE schedule_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Storage settings, per R15.5.
--
-- The server defaults assume a read-dominated table. Every row here is updated at
-- least twice (claim, completion) then deleted by retention, so at steady state
-- dead tuples are produced at roughly the job throughput rate. The default 20%
-- scale factor would let a 10M-row table accumulate 2M dead tuples before
-- vacuuming; by then the claim index has already bloated.
--
-- Honest caveat: fillfactor helps most when updates are HOT, and the claim update
-- is NOT HOT because it changes `state`, which appears in four index predicates.
-- The mitigations that actually carry this design are the partial indexes above
-- and retention keeping the table small.
-- ---------------------------------------------------------------------------

ALTER TABLE job SET (
  fillfactor                      = 80,    -- room for update tuples in the same page
  autovacuum_vacuum_scale_factor  = 0.01,  -- 1% vs the 20% default
  autovacuum_vacuum_threshold     = 100,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_vacuum_cost_delay    = 0,     -- never throttle this table's vacuum
  autovacuum_vacuum_cost_limit    = 2000
);

-- ---------------------------------------------------------------------------
-- job_archive: terminal jobs moved out of the hot table.
--
-- Range-partitioned by month specifically so expiring history is DROP TABLE on a
-- partition -- instant, and leaving no dead tuples -- rather than a DELETE of
-- millions of rows that would itself create the bloat the archive exists to avoid.
--
-- LIKE without INCLUDING IDENTITY/CONSTRAINTS is deliberate: `id` becomes a plain
-- bigint preserving the original value, and the CHECKs do not apply to history.
-- ---------------------------------------------------------------------------

CREATE TABLE job_archive (
  LIKE job INCLUDING DEFAULTS,
  archived_at timestamptz NOT NULL DEFAULT now()
) PARTITION BY RANGE (archived_at);

CREATE INDEX job_archive_queue_idx ON job_archive (queue, archived_at DESC);

-- ---------------------------------------------------------------------------
-- api_key: bearer credentials. Only a salted hash is stored.
-- ---------------------------------------------------------------------------

CREATE TABLE api_key (
  id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name         text        NOT NULL,
  key_hash     bytea       NOT NULL UNIQUE,
  -- First few characters, so a key can be identified in the UI and in the audit
  -- log without storing anything that grants access.
  key_prefix   text        NOT NULL,
  scopes       text[]      NOT NULL,
  queues       text[],                   -- NULL means all queues
  expires_at   timestamptz,
  revoked_at   timestamptz,
  last_used_at timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT api_key_scopes_valid CHECK (
    scopes <@ ARRAY['enqueue', 'read', 'admin']::text[] AND array_length(scopes, 1) >= 1
  )
);

CREATE INDEX api_key_active_idx ON api_key (key_hash) WHERE revoked_at IS NULL;

-- ---------------------------------------------------------------------------
-- audit_log: every state-changing administrative action.
-- ---------------------------------------------------------------------------

CREATE TABLE audit_log (
  id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  actor      text        NOT NULL,
  action     text        NOT NULL,
  target     text,
  detail     jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX audit_log_created_idx ON audit_log (created_at DESC);
CREATE INDEX audit_log_target_idx  ON audit_log (target, created_at DESC);

-- ---------------------------------------------------------------------------
-- Wakeup channel helper.
--
-- Emits NOTIFY only AFTER the inserting transaction commits (statement triggers
-- fire within the transaction, but NOTIFY messages are queued until commit), so a
-- woken worker never fails to find the job that woke it.
--
-- Payload is the queue name only -- never job data. Keeps well clear of the 8000
-- byte limit and avoids filling the server's notification queue.
-- ---------------------------------------------------------------------------

CREATE FUNCTION pgjobq_notify_enqueue() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  -- Only for jobs that are immediately eligible. A future-dated job has nothing
  -- to wake anyone for.
  IF NEW.state = 'available' AND NEW.run_at <= now() THEN
    PERFORM pg_notify('pgjobq_' || NEW.queue, '');
  END IF;
  RETURN NULL;
END;
$$;

CREATE TRIGGER job_notify_enqueue
  AFTER INSERT ON job
  FOR EACH ROW
  EXECUTE FUNCTION pgjobq_notify_enqueue();
