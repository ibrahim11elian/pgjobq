/**
 * Express application assembly.
 *
 * Order matters: security headers, then body limits, then rate limiting, then auth,
 * then routes, then the error mapper last.
 */
import express, { type Express, type Request, type Response } from 'express';
import helmet from 'helmet';
import rateLimit, { ipKeyGenerator } from 'express-rate-limit';
import { pinoHttp } from 'pino-http';
import {
  assertSchemaCurrent,
  type Client,
  type Config,
  type Db,
  type Logger,
  type Metrics,
  type Scheduler,
} from '@pgjobq/core';
import { Authenticator } from './auth.js';
import { EventStream } from './events.js';
import { errorHandler, notFoundHandler, requireAuth } from './middleware.js';
import { createRoutes } from './routes.js';
import { buildOpenApiDocument } from './openapi.js';

export interface AppDeps {
  readonly config: Config;
  readonly db: Db;
  readonly client: Client;
  readonly scheduler: Scheduler | undefined;
  readonly metrics: Metrics;
  readonly logger: Logger;
}

export interface BuiltApp {
  readonly app: Express;
  readonly events: EventStream;
  readonly auth: Authenticator;
}

export function createApp(deps: AppDeps): BuiltApp {
  const { config, db, client, scheduler, metrics, logger } = deps;
  const app = express();

  // Trust the first proxy hop so rate limiting keys on the real client IP rather
  // than the load balancer's. Set to 1, not `true`: trusting all hops lets a client
  // forge X-Forwarded-For and bypass the limiter entirely.
  app.set('trust proxy', 1);
  app.disable('x-powered-by');

  app.use(
    helmet({
      contentSecurityPolicy: {
        directives: {
          defaultSrc: ["'self'"],
          // The API docs page loads its renderer from a CDN.
          scriptSrc: ["'self'", 'https://unpkg.com', "'unsafe-inline'"],
          styleSrc: ["'self'", 'https://unpkg.com', "'unsafe-inline'"],
          imgSrc: ["'self'", 'data:', 'https://unpkg.com'],
          connectSrc: ["'self'"],
        },
      },
      // Allows the dashboard on a different origin to read responses.
      crossOriginResourcePolicy: { policy: 'cross-origin' },
    }),
  );

  app.use((req: Request, res: Response, next) => {
    const origin = req.get('origin');
    if (origin !== undefined && (config.corsOrigin === '*' || origin === config.corsOrigin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
      res.setHeader('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Authorization,Content-Type,traceparent');
      res.setHeader('Access-Control-Max-Age', '600');
    }
    if (req.method === 'OPTIONS') {
      res.status(204).end();
      return;
    }
    next();
  });

  app.use(
    pinoHttp({
      logger: logger as never,
      // Health probes would otherwise dominate the log volume.
      autoLogging: { ignore: (req) => req.url === '/health' || req.url === '/ready' },
      customLogLevel: (_req, res, err) => {
        if (err !== undefined || res.statusCode >= 500) return 'error';
        if (res.statusCode >= 400) return 'warn';
        return 'info';
      },
      redact: {
        paths: ['req.headers.authorization', 'req.headers.cookie', 'req.body.payload'],
        censor: '[redacted]',
      },
    }),
  );

  // Bounded before any parsing work happens.
  app.use(express.json({ limit: config.maxPayloadBytes + 8192 }));

  const events = new EventStream(config.sseMaxSubscribers, logger);
  const auth = new Authenticator(db, config.apiBootstrapKey ?? 'pgjobq-dev-salt');

  // ---------------------------------------------------------------------------
  // Unauthenticated endpoints: probes and the API description only.
  // ---------------------------------------------------------------------------

  app.get('/health', (_req: Request, res: Response) => {
    // Liveness reflects PROCESS health only. Tying it to the database would make a
    // brief outage restart every container, turning a recoverable blip into an outage.
    res.json({ status: 'ok', uptime: process.uptime() });
  });

  app.get('/ready', (_req: Request, res: Response) => {
    void (async () => {
      try {
        await db.ping();
        await assertSchemaCurrent(db);
        res.json({ status: 'ready' });
      } catch (e) {
        res.status(503).json({
          status: 'not_ready',
          reason: e instanceof Error ? e.message : String(e),
        });
      }
    })();
  });

  const openApiDocument = buildOpenApiDocument();
  app.get('/openapi.json', (_req: Request, res: Response) => {
    res.json(openApiDocument);
  });

  app.get('/docs', (_req: Request, res: Response) => {
    res.type('html').send(DOCS_HTML);
  });

  // ---------------------------------------------------------------------------
  // Metrics. Not behind the API-key auth, because scrapers use network-level
  // access control; it carries no payload data, only counts and timings.
  // ---------------------------------------------------------------------------

  app.get('/metrics', (_req: Request, res: Response) => {
    void (async () => {
      res.set('Content-Type', metrics.contentType);
      res.send(await metrics.expose());
    })();
  });

  // ---------------------------------------------------------------------------
  // Authenticated API
  // ---------------------------------------------------------------------------

  const limiter = rateLimit({
    windowMs: config.rateLimitWindowMs,
    limit: config.rateLimitMax,
    standardHeaders: 'draft-7',
    legacyHeaders: false,
    // Key on the API key rather than the IP: many callers share an egress IP, and
    // limiting per-credential is what actually bounds one tenant's impact.
    //
    // The IP fallback goes through ipKeyGenerator, which normalizes IPv6 to a /64
    // subnet. Keying on a raw IPv6 address would let a client rotate through its
    // enormous allocation and bypass the limit entirely.
    keyGenerator: (req: Request) => req.apiKey?.id ?? ipKeyGenerator(req.ip ?? 'unknown'),
    handler: (_req: Request, res: Response) => {
      res.status(429).json({
        error: { code: 'RATE_LIMITED', message: 'Too many requests; slow down and retry' },
      });
    },
  });

  app.use('/v1', requireAuth(auth), limiter, createRoutes({ client, scheduler, events }));

  app.use(notFoundHandler());
  app.use(errorHandler(logger));

  return { app, events, auth };
}

const DOCS_HTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>pgjobq API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({ url: '/openapi.json', dom_id: '#ui', deepLinking: true });
  </script>
</body>
</html>`;
