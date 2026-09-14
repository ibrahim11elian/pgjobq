/**
 * Migration CLI.
 *
 * Forward-only by design: there is no `down`. A reversal is a new migration, so
 * that every environment converges through the same ordered sequence. The runbook
 * documents the rollback procedure.
 */
import { loadConfig, redactDatabaseUrl } from '../config.js';
import { Db, waitForDatabase } from '../db.js';
import { getApplied, migrateUp } from '../migrate.js';
import { createLogger } from '../observability/logger.js';

async function main(): Promise<void> {
  const command = process.argv[2] ?? 'up';
  const config = loadConfig();
  const logger = createLogger({
    level: config.logLevel,
    pretty: config.nodeEnv !== 'production',
    name: 'migrate',
  });

  const db = new Db({
    connectionString: config.databaseUrl,
    max: 2,
    applicationName: 'pgjobq-migrate',
    logger,
    // Migrations include index builds and partition DDL that legitimately take
    // longer than the runtime statement timeout.
    statementTimeoutMs: 300_000,
  });

  try {
    logger.info({ database: redactDatabaseUrl(config.databaseUrl) }, 'connecting');
    await waitForDatabase(db, { logger, maxAttempts: 20 });

    switch (command) {
      case 'up': {
        const result = await migrateUp(db, { logger });
        if (result.alreadyCurrent) {
          logger.info({ version: result.currentVersion }, 'schema already current');
        } else {
          logger.info(
            { applied: result.applied, version: result.currentVersion },
            `applied ${result.applied.length} migration(s)`,
          );
        }
        break;
      }
      case 'status': {
        const applied = await getApplied(db);
        if (applied.length === 0) {
          logger.info('no migrations applied');
        } else {
          for (const a of applied) {
            logger.info({ version: a.version, name: a.name, appliedAt: a.applied_at }, 'applied');
          }
        }
        break;
      }
      case 'down': {
        logger.error(
          'Migrations are forward-only. To reverse a change, add a new migration. ' +
            'For a full local reset: npm run db:reset',
        );
        process.exitCode = 1;
        break;
      }
      default: {
        logger.error({ command }, 'unknown command; expected: up | status');
        process.exitCode = 1;
      }
    }
  } finally {
    await db.close();
  }
}

main().catch((e: unknown) => {
  const message = e instanceof Error ? e.message : String(e);
  process.stderr.write(`migration failed: ${message}\n`);
  process.exitCode = 1;
});
