# Security

## Reporting a vulnerability

Please **do not open a public issue.** Use GitHub's private vulnerability reporting
(Security → Report a vulnerability) so a fix can ship before details are public.

Useful to include: what an attacker can do, the affected version or commit, and a
reproduction if you have one. Expect an acknowledgement within a few days.

## Status of this project

Built as a portfolio project. It has had no external security review, and `docs/scope.md`
records what is and is not verified. Read that before deploying it anywhere that matters.

## What the design does protect against

**SQL injection.** Every statement is parameterized. The `sql()` helper is branded so
passing an interpolated string is a compile error rather than a code-review question. Job
payloads — which are untrusted input — never reach a statement as text.

The two places SQL cannot take bind parameters are validated against strict allowlists
rather than trusted:

- `LISTEN` channel names must match `^pgjobq_[a-zA-Z0-9._:-]+$` and fit Postgres's 63-byte
  identifier limit.
- Archive partition names must match `^job_archive_\d{4}_\d{2}$` and bounds must be
  `YYYY-MM-DD`.

**Credential exposure.** API keys are stored as salted SHA-256 hashes, never plaintext. The
plaintext is shown once at creation and is unrecoverable afterwards. Comparison is
constant-time via `timingSafeEqual`, so response timing cannot be used to recover a key.

**Data leaking into logs.** Redaction is on by default for payloads, metadata, connection
strings, and authorization headers. Logging payload contents requires an explicit opt-in,
and that opt-in is **rejected in production** by configuration validation.

**Unauthenticated deployment.** Production mode refuses to start without
`API_BOOTSTRAP_KEY`, rejects keys under 32 characters, and rejects values that look like
placeholders (`dev`, `changeme`, `insecure`, `example`, `password`, `secret`).

**Resource exhaustion.** Request bodies are size-limited, payloads capped (default 256 KiB),
list endpoints use keyset pagination with a bounded page size, SSE subscribers are capped and
slow consumers disconnected, and metric label cardinality is bounded so a caller inventing
job types cannot exhaust the registry.

**Enumeration.** A 403 does not disclose whether the target resource exists. A rejected key
and a missing key return identical responses, so an attacker cannot learn that their key
format was right.

**IPv6 rate-limit bypass.** The limiter keys on the API key, falling back to a
subnet-normalized IP. Keying on a raw IPv6 address would let a client rotate through its
allocation and bypass the limit entirely.

## What it does NOT protect against

Be explicit about these before deploying.

**The dashboard holds an API key in the browser.** `sessionStorage`, cleared when the tab
closes. Acceptable for an operator tool on a trusted machine. **Not acceptable on a public
URL** — put a session cookie and a server-side proxy in front of it rather than handing a
queue credential to a browser.

**`/metrics` is not behind API-key auth.** Deliberate, because scrapers use network-level
access control. It exposes counts, timings, and queue and job-type names — no payload data.
Do not expose it publicly.

**Payloads are stored in plain text.** `jsonb` in the database. Use database-level encryption
at rest; do not put secrets in job payloads.

**Handler timeouts are cooperative.** `AbortSignal` is advisory. A handler that ignores it
keeps running, and its side effects still land. This means an untrusted handler can consume
resources indefinitely — handlers are trusted application code, not a sandbox.

**No multi-tenant isolation.** API keys scope to queues and operations, but every tenant
shares one database and one worker pool. A tenant enqueueing a flood affects everyone. This
is not a multi-tenant control plane and does not pretend to be.

**Bearer keys have no expiry by default.** Expiry is supported per key but not required. Set
`expires_at` and rotate.

## Handling secrets

- Never commit `.env`. It is gitignored, along with `apikey.txt`, which the dev setup writes.
- Generate keys with a CSPRNG: `openssl rand -base64 32`.
- Rotate the bootstrap key after creating scoped keys for real callers, then revoke it.
- Scope keys narrowly. A service that only enqueues does not need `admin`.
- Every state-changing administrative action is recorded in `audit_log` with actor, action,
  target, and time.

## Supported versions

Pre-1.0. Only the latest commit on `main` receives fixes.
