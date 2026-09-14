/**
 * Migration runner: versioned, ordered, forward-only.
 *
 * Hand-rolled rather than pulled from a library. It is ~100 lines, it makes the
 * version gate explicit, and it avoids a dependency whose conventions would have
 * to be worked around for the partition and trigger DDL this schema needs.
 *
 * Each migration runs inside a transaction together with its bookkeeping row, so a
 * failure can never leave the database half-migrated.
 */
import { readdir, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Db } from './db.js';
import { sql } from './db.js';
import { MigrationError } from './errors.js';
import type { Logger } from './types.js';

export interface Migration {
  readonly version: number;
  readonly name: string;
  readonly filename: string;
  readonly sql: string;
  readonly checksum: string;
}

const MIGRATIONS_TABLE = 'pgjobq_migration';

/** Default location: the package's own migrations directory. */
export function defaultMigrationsDir(): string {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'migrations');
}

export async function loadMigrations(dir = defaultMigrationsDir()): Promise<Migration[]> {
  let entries: string[];
  try {
    entries = await readdir(dir);
  } catch (e) {
    throw new MigrationError(`Cannot read migrations directory: ${dir}`, { cause: e });
  }

  const files = entries.filter((f) => f.endsWith('.sql')).sort();
  const migrations: Migration[] = [];

  for (const filename of files) {
    const match = /^(\d+)[_-](.+)\.sql$/.exec(filename);
    if (!match?.[1] || !match[2]) {
      throw new MigrationError(`Migration filename must be <number>_<name>.sql, got: ${filename}`);
    }
    const version = Number.parseInt(match[1], 10);
    const body = await readFile(path.join(dir, filename), 'utf8');
    migrations.push({
      version,
      name: match[2],
      filename,
      sql: body,
      checksum: createHash('sha256').update(body).digest('hex').slice(0, 16),
    });
  }

  const seen = new Set<number>();
  for (const m of migrations) {
    if (seen.has(m.version)) {
      throw new MigrationError(`Duplicate migration version ${m.version} (${m.filename})`);
    }
    seen.add(m.version);
  }

  return migrations.sort((a, b) => a.version - b.version);
}

async function ensureTable(db: Db): Promise<void> {
  await db.query(
    sql(`
    CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
      version     integer     PRIMARY KEY,
      name        text        NOT NULL,
      checksum    text        NOT NULL,
      applied_at  timestamptz NOT NULL DEFAULT now()
    )
  `),
  );
}

export interface AppliedMigration {
  readonly version: number;
  readonly name: string;
  readonly checksum: string;
  readonly applied_at: Date;
}

export async function getApplied(db: Db): Promise<AppliedMigration[]> {
  await ensureTable(db);
  const res = await db.query<AppliedMigration>(
    sql(`SELECT version, name, checksum, applied_at FROM ${MIGRATIONS_TABLE} ORDER BY version`),
  );
  return res.rows;
}

export interface MigrateResult {
  readonly applied: readonly number[];
  readonly alreadyCurrent: boolean;
  readonly currentVersion: number;
}

export async function migrateUp(
  db: Db,
  opts: { dir?: string; logger?: Logger } = {},
): Promise<MigrateResult> {
  const migrations = await loadMigrations(opts.dir);
  const applied = await getApplied(db);
  const appliedByVersion = new Map(applied.map((a) => [a.version, a]));

  // A changed checksum means a merged migration was edited. That silently
  // diverges every environment that already applied the old text, so it is a hard
  // failure rather than a warning.
  for (const m of migrations) {
    const prev = appliedByVersion.get(m.version);
    if (prev && prev.checksum !== m.checksum) {
      throw new MigrationError(
        `Migration ${m.version} (${m.filename}) has changed since it was applied ` +
          `(recorded ${prev.checksum}, found ${m.checksum}). Migrations are forward-only: ` +
          `add a new migration instead of editing this one.`,
      );
    }
  }

  const pending = migrations.filter((m) => !appliedByVersion.has(m.version));
  if (pending.length === 0) {
    return {
      applied: [],
      alreadyCurrent: true,
      currentVersion: latestVersion(migrations),
    };
  }

  const appliedNow: number[] = [];
  for (const m of pending) {
    opts.logger?.info({ version: m.version, name: m.name }, 'applying migration');
    await db.withTransaction(async (tx) => {
      await tx.query(sql(m.sql));
      await tx.query(
        sql(`INSERT INTO ${MIGRATIONS_TABLE} (version, name, checksum) VALUES ($1, $2, $3)`, [
          m.version,
          m.name,
          m.checksum,
        ]),
      );
    });
    appliedNow.push(m.version);
  }

  return {
    applied: appliedNow,
    alreadyCurrent: false,
    currentVersion: latestVersion(migrations),
  };
}

function latestVersion(migrations: readonly Migration[]): number {
  return migrations.length === 0 ? 0 : (migrations[migrations.length - 1]?.version ?? 0);
}

/**
 * The startup version gate.
 *
 * Serving against an out-of-date schema produces errors that look like
 * application bugs. Refusing to start names the real cause immediately.
 */
export async function assertSchemaCurrent(
  db: Db,
  opts: { dir?: string } = {},
): Promise<{ currentVersion: number }> {
  const migrations = await loadMigrations(opts.dir);
  const expected = latestVersion(migrations);

  let applied: AppliedMigration[];
  try {
    applied = await getApplied(db);
  } catch (e) {
    throw new MigrationError(
      'Cannot read migration state. Has the database been initialized? Run: npm run migrate:up',
      { cause: e },
    );
  }

  const actual = applied.length === 0 ? 0 : Math.max(...applied.map((a) => a.version));
  if (actual !== expected) {
    const missing = migrations
      .filter((m) => !applied.some((a) => a.version === m.version))
      .map((m) => m.filename);
    throw new MigrationError(
      `Database schema is at version ${actual}, expected ${expected}. ` +
        `Refusing to serve. Pending: ${missing.join(', ') || '(none — database is ahead of this build)'}. ` +
        `Run: npm run migrate:up`,
    );
  }
  return { currentVersion: actual };
}
