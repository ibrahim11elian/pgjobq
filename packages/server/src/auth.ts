/**
 * API key authentication.
 *
 * Keys are stored as a salted SHA-256 hash and compared in constant time. The
 * plaintext is shown exactly once at creation and is unrecoverable afterwards — if a
 * key is lost, it is rotated, not looked up.
 */
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { sql, type Db } from '@pgjobq/core';

export const SCOPES = ['enqueue', 'read', 'admin'] as const;
export type Scope = (typeof SCOPES)[number];

export interface ApiKeyRecord {
  readonly id: string;
  readonly name: string;
  readonly scopes: readonly Scope[];
  /** null means every queue. */
  readonly queues: readonly string[] | null;
}

const PREFIX = 'pgjq_';

/**
 * Generates a key.
 *
 * 32 random bytes, base64url — 256 bits of entropy, which puts brute force out of
 * reach and makes rate limiting the only relevant attack surface.
 */
export function generateKey(): string {
  return PREFIX + randomBytes(32).toString('base64url');
}

/**
 * Hashes a key with a per-deployment salt.
 *
 * The salt means a stolen database is not directly usable against another
 * deployment, and it defeats precomputation. Deliberately a fast hash rather than a
 * KDF: these are 256-bit random tokens, not user-chosen passwords, so there is no
 * low-entropy space to slow an attacker down through — and a KDF here would add
 * latency to every single request.
 */
export function hashKey(key: string, salt: string): Buffer {
  return createHash('sha256').update(salt).update(key).digest();
}

export function keyPrefix(key: string): string {
  return key.slice(0, PREFIX.length + 8);
}

interface ApiKeyRow {
  id: string;
  name: string;
  scopes: string[];
  queues: string[] | null;
  expires_at: Date | null;
  revoked_at: Date | null;
}

export class Authenticator {
  constructor(
    private readonly db: Db,
    private readonly salt: string,
  ) {}

  /**
   * Resolves a presented key, or null when it is unknown, revoked, or expired.
   *
   * The lookup is by hash equality in the database, which is already constant-time
   * with respect to the key's content. The extra timingSafeEqual guards the returned
   * row, so a partial hash collision cannot be probed.
   */
  async authenticate(presented: string): Promise<ApiKeyRecord | null> {
    if (!presented.startsWith(PREFIX)) return null;

    const hash = hashKey(presented, this.salt);
    const res = await this.db.query<ApiKeyRow>(
      sql(
        `SELECT id, name, scopes, queues, expires_at, revoked_at, key_hash
           FROM api_key
          WHERE key_hash = $1`,
        [hash],
      ),
    );

    const row = res.rows[0];
    if (!row) return null;
    if (row.revoked_at !== null) return null;
    if (row.expires_at !== null && row.expires_at.getTime() <= Date.now()) return null;

    const stored = (row as ApiKeyRow & { key_hash: Buffer }).key_hash;
    if (stored.length !== hash.length || !timingSafeEqual(stored, hash)) return null;

    // Best-effort usage tracking. A failure here must not deny an otherwise valid
    // request, so it is deliberately not awaited into the auth decision.
    void this.db
      .query(sql(`UPDATE api_key SET last_used_at = now() WHERE id = $1`, [row.id]))
      .catch(() => undefined);

    return {
      id: row.id,
      name: row.name,
      scopes: row.scopes.filter(isScope),
      queues: row.queues,
    };
  }

  async create(p: {
    name: string;
    scopes: readonly Scope[];
    queues?: readonly string[] | null;
    expiresAt?: Date | null;
  }): Promise<{ id: string; key: string }> {
    const key = generateKey();
    const res = await this.db.query<{ id: string }>(
      sql(
        `INSERT INTO api_key (name, key_hash, key_prefix, scopes, queues, expires_at)
         VALUES ($1, $2, $3, $4::text[], $5::text[], $6)
         RETURNING id`,
        [
          p.name,
          hashKey(key, this.salt),
          keyPrefix(key),
          [...p.scopes],
          p.queues === undefined || p.queues === null ? null : [...p.queues],
          p.expiresAt ?? null,
        ],
      ),
    );
    const row = res.rows[0];
    if (!row) throw new Error('Failed to create API key');
    // The only moment the plaintext exists outside the caller's memory.
    return { id: row.id, key };
  }

  async revoke(id: string): Promise<boolean> {
    const res = await this.db.query(
      sql(`UPDATE api_key SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`, [id]),
    );
    return res.rowCount > 0;
  }

  async list(): Promise<
    { id: string; name: string; prefix: string; scopes: string[]; revoked: boolean }[]
  > {
    const res = await this.db.query<{
      id: string;
      name: string;
      key_prefix: string;
      scopes: string[];
      revoked_at: Date | null;
    }>(
      sql(`SELECT id, name, key_prefix, scopes, revoked_at FROM api_key ORDER BY created_at DESC`),
    );
    return res.rows.map((r) => ({
      id: r.id,
      name: r.name,
      prefix: r.key_prefix,
      scopes: r.scopes,
      revoked: r.revoked_at !== null,
    }));
  }

  /**
   * Ensures a bootstrap key exists matching the configured value.
   *
   * Idempotent, so restarting does not accumulate keys. Without this there would be
   * no way to make the first authenticated call.
   */
  async ensureBootstrapKey(plaintext: string): Promise<void> {
    const hash = hashKey(plaintext, this.salt);
    await this.db.query(
      sql(
        `INSERT INTO api_key (name, key_hash, key_prefix, scopes, queues)
         VALUES ('bootstrap', $1, $2, ARRAY['enqueue','read','admin']::text[], NULL)
         ON CONFLICT (key_hash) DO NOTHING`,
        [hash, plaintext.slice(0, 12)],
      ),
    );
  }
}

function isScope(s: string): s is Scope {
  return (SCOPES as readonly string[]).includes(s);
}

/** True when the key carries the scope and covers the queue. */
export function authorizes(record: ApiKeyRecord, scope: Scope, queue?: string): boolean {
  if (!record.scopes.includes(scope)) return false;
  if (queue === undefined) return true;
  if (record.queues === null) return true;
  return record.queues.includes(queue);
}
