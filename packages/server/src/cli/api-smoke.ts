/**
 * API smoke check against a running server.
 *
 * Verifies status codes, auth, validation, pagination, and the SSE stream. Not a test
 * suite — a proof that the HTTP surface behaves as documented.
 */
import { readFileSync } from 'node:fs';

const BASE = process.env['SMOKE_BASE'] ?? 'http://localhost:3000';
const KEY = process.env['SMOKE_KEY'] ?? readKeyFile();

let failures = 0;
let checks = 0;

function check(label: string, ok: boolean, detail?: unknown): void {
  checks += 1;
  if (ok) {
    console.log(`  PASS  ${label}`);
  } else {
    failures += 1;
    console.error(`  FAIL  ${label}${detail !== undefined ? ` -> ${JSON.stringify(detail)}` : ''}`);
  }
}

function section(name: string): void {
  console.log(`\n${name}`);
}

function readKeyFile(): string {
  try {
    return readFileSync('apikey.txt', 'utf8').trim();
  } catch {
    return '';
  }
}

interface Res {
  status: number;
  body: unknown;
}

interface CallOptions {
  body?: unknown;
  /** Pass null to send no Authorization header at all. */
  key?: string | null;
  headers?: Record<string, string>;
}

async function call(method: string, path: string, opts: CallOptions = {}): Promise<Res> {
  const headers: Record<string, string> = { ...opts.headers };
  if (opts.key !== null) headers['authorization'] = `Bearer ${opts.key ?? KEY}`;
  if (opts.body !== undefined) headers['content-type'] = 'application/json';

  const res = await fetch(`${BASE}${path}`, {
    method,
    headers,
    ...(opts.body !== undefined ? { body: JSON.stringify(opts.body) } : {}),
  });

  const text = await res.text();
  let body: unknown = text;
  try {
    body = text.length > 0 ? JSON.parse(text) : null;
  } catch {
    /* keep as text */
  }
  return { status: res.status, body };
}

function code(r: Res): string | undefined {
  const b = r.body as { error?: { code?: string } } | null;
  return b?.error?.code;
}

async function main(): Promise<void> {
  const QUEUE = `apismoke_${Date.now().toString(36)}`;

  section('Unauthenticated endpoints');
  const health = await call('GET', '/health', { key: null });
  check('GET /health is public and ok', health.status === 200, health.status);

  const ready = await call('GET', '/ready', { key: null });
  check('GET /ready reports ready', ready.status === 200, ready.body);

  const openapi = await call('GET', '/openapi.json', { key: null });
  const doc = openapi.body as { openapi?: string; paths?: Record<string, unknown> };
  check('GET /openapi.json serves a 3.1 document', doc.openapi === '3.1.0', doc.openapi);
  check(
    'openapi document contains the documented routes',
    Object.keys(doc.paths ?? {}).length >= 14,
    Object.keys(doc.paths ?? {}).length,
  );

  const metrics = await call('GET', '/metrics', { key: null });
  check(
    'GET /metrics exposes prometheus text',
    typeof metrics.body === 'string' && metrics.body.includes('jobq_'),
  );

  section('Authentication');
  const noAuth = await call('POST', `/v1/queues/${QUEUE}/jobs`, {
    key: null,
    body: { type: 'a', payload: {} },
  });
  check('missing key is 401', noAuth.status === 401, noAuth.status);
  check('401 carries UNAUTHENTICATED', code(noAuth) === 'UNAUTHENTICATED', code(noAuth));

  const badAuth = await call('POST', `/v1/queues/${QUEUE}/jobs`, {
    key: 'pgjq_totally-invalid-key',
    body: { type: 'a', payload: {} },
  });
  check('invalid key is 401', badAuth.status === 401, badAuth.status);
  check(
    'invalid key response is indistinguishable from missing',
    JSON.stringify(badAuth.body).length > 0 && code(badAuth) === 'UNAUTHENTICATED',
  );

  section('Enqueue');
  const created = await call('POST', `/v1/queues/${QUEUE}/jobs`, {
    body: { type: 'api.demo', payload: { n: 1 } },
  });
  check('enqueue returns 201', created.status === 201, created.body);
  const createdId = (created.body as { id?: string }).id ?? '';
  check('201 body carries an id', createdId.length > 0);

  const idem1 = await call('POST', `/v1/queues/${QUEUE}/jobs`, {
    body: { type: 'api.demo', payload: { n: 2 }, options: { idempotencyKey: 'api-k1' } },
  });
  const idem2 = await call('POST', `/v1/queues/${QUEUE}/jobs`, {
    body: { type: 'api.demo', payload: { n: 3 }, options: { idempotencyKey: 'api-k1' } },
  });
  check('first keyed enqueue is 201', idem1.status === 201, idem1.status);
  check('repeat keyed enqueue is 200, not 201', idem2.status === 200, idem2.status);
  check(
    'deduplicated flag is set and id matches',
    (idem2.body as { deduplicated?: boolean }).deduplicated === true &&
      (idem2.body as { id?: string }).id === (idem1.body as { id?: string }).id,
    idem2.body,
  );

  const batch = await call('POST', `/v1/queues/${QUEUE}/jobs/batch`, {
    body: { jobs: Array.from({ length: 25 }, (_, i) => ({ type: 'api.demo', payload: { i } })) },
  });
  check('batch enqueue returns 201', batch.status === 201, batch.status);
  check('batch returned 25 ids', ((batch.body as { items?: unknown[] }).items ?? []).length === 25);

  section('Validation');
  const badType = await call('POST', `/v1/queues/${QUEUE}/jobs`, {
    body: { type: 'has spaces!', payload: {} },
  });
  check('invalid type name is 400', badType.status === 400, badType.status);
  check('400 carries VALIDATION_ERROR', code(badType) === 'VALIDATION_ERROR');
  const problems = (badType.body as { error?: { details?: { problems?: unknown[] } } }).error
    ?.details?.problems;
  check(
    'validation error names the field',
    Array.isArray(problems) && problems.length > 0,
    problems,
  );

  const unknownField = await call('POST', `/v1/queues/${QUEUE}/jobs`, {
    body: { type: 'api.demo', payload: {}, nope: 1 },
  });
  check('unknown body field is rejected (strict schema)', unknownField.status === 400);

  const bothTiming = await call('POST', `/v1/queues/${QUEUE}/jobs`, {
    body: {
      type: 'api.demo',
      payload: {},
      options: { runAt: new Date().toISOString(), delaySeconds: 10 },
    },
  });
  check('runAt + delaySeconds together is 400', bothTiming.status === 400, bothTiming.status);

  const badCursor = await call('GET', `/v1/queues/${QUEUE}/jobs?cursor=abc`);
  check('non-numeric cursor is 400', badCursor.status === 400, badCursor.status);

  const overLimit = await call('GET', `/v1/queues/${QUEUE}/jobs?limit=9999`);
  check('limit above max is 400', overLimit.status === 400, overLimit.status);

  section('Read and pagination');
  const list = await call('GET', `/v1/queues/${QUEUE}/jobs?limit=10`);
  check('list returns 200', list.status === 200);
  const listBody = list.body as { items?: unknown[]; nextCursor?: string | null };
  check(
    'list returned a page of 10',
    (listBody.items ?? []).length === 10,
    (listBody.items ?? []).length,
  );
  check('list supplies a nextCursor', typeof listBody.nextCursor === 'string', listBody.nextCursor);

  const page2 = await call(
    'GET',
    `/v1/queues/${QUEUE}/jobs?limit=10&cursor=${listBody.nextCursor ?? ''}`,
  );
  const page2Body = page2.body as { items?: { id: string }[] };
  const page1Ids = new Set(((listBody.items ?? []) as { id: string }[]).map((j) => j.id));
  const page2Ids = ((page2Body.items ?? []) as { id: string }[]).map((j) => j.id);
  check(
    'keyset page 2 does not repeat page 1',
    page2Ids.every((id) => !page1Ids.has(id)),
  );

  const one = await call('GET', `/v1/jobs/${createdId}`);
  check('get job returns 200', one.status === 200);
  check('job carries ISO timestamps', typeof (one.body as { runAt?: string }).runAt === 'string');

  const missing = await call('GET', '/v1/jobs/999999999');
  check('unknown job is 404', missing.status === 404, missing.status);
  check('404 carries JOB_NOT_FOUND', code(missing) === 'JOB_NOT_FOUND');

  const queues = await call('GET', '/v1/queues');
  check('queue stats returns 200', queues.status === 200);
  check('queue stats includes our queue', JSON.stringify(queues.body).includes(QUEUE));

  section('Admin');
  const delayed = await call('POST', `/v1/queues/${QUEUE}/jobs`, {
    body: { type: 'api.demo', payload: {}, options: { delaySeconds: 3600 } },
  });
  const delayedId = (delayed.body as { id?: string }).id ?? '';

  const cancelled = await call('POST', `/v1/jobs/${delayedId}/cancel`);
  check('cancel of available job is 200', cancelled.status === 200, cancelled.body);

  const cancelAgain = await call('POST', `/v1/jobs/${delayedId}/cancel`);
  check('cancel of terminal job is 409', cancelAgain.status === 409, cancelAgain.status);
  check(
    '409 carries INVALID_TRANSITION',
    code(cancelAgain) === 'INVALID_TRANSITION',
    code(cancelAgain),
  );

  const retryTerminal = await call('POST', `/v1/jobs/${delayedId}/retry`);
  check('retry of cancelled job is 409', retryTerminal.status === 409, retryTerminal.status);

  const replayNotDead = await call('POST', `/v1/jobs/${createdId}/replay`);
  check('replay of non-dead job is 409', replayNotDead.status === 409, replayNotDead.status);

  const paused = await call('POST', `/v1/queues/${QUEUE}/pause`);
  check('pause returns 200', paused.status === 200);
  const resumed = await call('POST', `/v1/queues/${QUEUE}/resume`);
  check('resume returns 200', resumed.status === 200);

  const badPurge = await call('POST', `/v1/queues/${QUEUE}/purge`, {
    body: { states: ['succeeded'], confirm: 'wrong-name' },
  });
  check('purge with wrong confirmation is 400', badPurge.status === 400, badPurge.status);

  section('Schedules');
  const schedName = `api-sched-${Date.now().toString(36)}`;
  const schedCreated = await call('POST', '/v1/schedules', {
    body: {
      name: schedName,
      queue: QUEUE,
      type: 'api.demo',
      cron: '*/10 * * * *',
      timezone: 'UTC',
    },
  });
  check('create schedule is 201', schedCreated.status === 201, schedCreated.body);
  const schedId = (schedCreated.body as { id?: string }).id ?? '';
  check(
    'schedule reports a future nextRunAt',
    new Date((schedCreated.body as { nextRunAt: string }).nextRunAt).getTime() > Date.now(),
  );

  const badCron = await call('POST', '/v1/schedules', {
    body: { name: `${schedName}-bad`, queue: QUEUE, type: 'api.demo', cron: 'nonsense' },
  });
  check('invalid cron is rejected', badCron.status === 400, badCron.status);

  const badTz = await call('POST', '/v1/schedules', {
    body: {
      name: `${schedName}-tz`,
      queue: QUEUE,
      type: 'api.demo',
      cron: '* * * * *',
      timezone: 'Mars/Olympus_Mons',
    },
  });
  check('unknown timezone is rejected', badTz.status === 400, badTz.status);

  const schedPaused = await call('POST', `/v1/schedules/${schedId}/pause`);
  check('pause schedule is 200', schedPaused.status === 200);
  check(
    'paused schedule reports enabled=false',
    (schedPaused.body as { enabled?: boolean }).enabled === false,
  );

  const schedDeleted = await call('DELETE', `/v1/schedules/${schedId}`);
  check('delete schedule is 204', schedDeleted.status === 204, schedDeleted.status);

  const schedGone = await call('GET', `/v1/schedules/${schedId}`);
  check('deleted schedule is 404', schedGone.status === 404, schedGone.status);

  section('DLQ');
  const dlq = await call('GET', '/v1/dlq?limit=5');
  check('dlq list returns 200', dlq.status === 200, dlq.status);
  check('dlq returns a page envelope', 'items' in (dlq.body as object));

  section('SSE');
  const controller = new AbortController();
  const sseRes = await fetch(`${BASE}/v1/events`, {
    headers: { authorization: `Bearer ${KEY}` },
    signal: controller.signal,
  });
  check('sse returns 200', sseRes.status === 200, sseRes.status);
  check(
    'sse content-type is text/event-stream',
    (sseRes.headers.get('content-type') ?? '').includes('text/event-stream'),
    sseRes.headers.get('content-type'),
  );
  const reader = sseRes.body?.getReader();
  if (reader) {
    const chunk = await Promise.race([
      reader
        .read()
        .then((r: { value?: Uint8Array }) => new TextDecoder().decode(r.value ?? new Uint8Array())),
      new Promise<string>((r) => setTimeout(() => r(''), 5000)),
    ]);
    check('sse sends an initial frame', chunk.length > 0, chunk.slice(0, 60));
  }
  controller.abort();

  section('Not found');
  const noRoute = await call('GET', '/v1/nope');
  check('unknown route is 404', noRoute.status === 404, noRoute.status);

  // Cleanup
  await call('POST', `/v1/queues/${QUEUE}/purge`, {
    body: {
      states: ['available', 'running', 'succeeded', 'dead', 'cancelled'],
      confirm: QUEUE,
    },
  });

  console.log(`\n${checks - failures}/${checks} checks passed`);
  if (failures > 0) {
    console.error(`${failures} FAILED`);
    process.exitCode = 1;
  }
}

main().catch((e: unknown) => {
  console.error('api smoke failed:', e instanceof Error ? e.stack : e);
  process.exitCode = 1;
});
