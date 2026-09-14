/**
 * Generates docs/state-machine.md from the transition table.
 *
 * Generated rather than hand-written so the diagram cannot drift from the code. A
 * state diagram that disagrees with the implementation is worse than none, because it
 * is trusted.
 */
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { JOB_STATES, TERMINAL_STATES } from '../types.js';
import {
  TRANSITIONS,
  isClaimable,
  permittedTargets,
  toMarkdownTable,
  toMermaid,
} from '../engine/state-machine.js';

async function main(): Promise<void> {
  const root = path.resolve(process.cwd());
  const outDir = path.join(root, 'docs');
  await mkdir(outDir, { recursive: true });

  const body = `# Job state machine

> **Generated file.** Produced from \`packages/core/src/engine/state-machine.ts\` by
> \`npm run gen:docs\`. Do not edit by hand; change the transition table instead.

Both the worker path and the administrative path route every mutation through
\`assertTransition\`, so no operation can produce a state this table does not permit.

## States

| State | Claimable | Terminal |
| ----- | --------- | -------- |
${JOB_STATES.map(
  (s) =>
    `| \`${s}\` | ${isClaimable(s) ? 'yes' : 'no'} | ${
      (TERMINAL_STATES as readonly string[]).includes(s) ? 'yes' : 'no'
    } |`,
).join('\n')}

Names avoid "pending", "queued", "completed", and "failed" deliberately: those are
ambiguous about whether a retry follows. A job whose handler threw is back in
\`available\` when attempts remain, so calling it "failed" would be wrong.

## Diagram

\`\`\`mermaid
${toMermaid()}
\`\`\`

## Transitions

${toMarkdownTable()}

## Permitted targets by state

${JOB_STATES.map((s) => {
  const targets = permittedTargets(s);
  return `- \`${s}\` → ${targets.length > 0 ? targets.map((t) => `\`${t}\``).join(', ') : '_(terminal)_'}`;
}).join('\n')}

## Two rules worth reading twice

**\`attempt\` increments at claim time, not at failure time.** A worker that dies
without reporting anything still consumes an attempt. Without this, a job that
reliably kills its worker — an out-of-memory payload, say — would retry forever,
taking down worker after worker. The cost is that an unrelated crash also burns an
attempt, which is why \`max_attempts\` defaults to 5 rather than 2.

**\`running → cancelled\` requires worker acknowledgement.** An operator cancelling a
running job sets \`cancel_requested_at\`; the worker sees it on its next heartbeat and
aborts the handler. The job stays \`running\` until the worker agrees or the lease
expires. Marking it cancelled unilaterally would report a state the executing process
has not agreed to, when its side effects may already have happened.

## Counts

${TRANSITIONS.length} permitted transitions across ${JOB_STATES.length} states,
${TERMINAL_STATES.length} of them terminal.
`;

  const out = path.join(outDir, 'state-machine.md');
  await writeFile(out, body, 'utf8');
  console.log(`wrote ${path.relative(root, out)}`);
}

main().catch((e: unknown) => {
  console.error('doc generation failed:', e instanceof Error ? e.message : e);
  process.exitCode = 1;
});
