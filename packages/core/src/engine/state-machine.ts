/**
 * The job state machine. Single source of truth for permitted transitions.
 *
 * Both the worker path and the administrative path route every mutation through
 * `assertTransition`, so no operation can produce a state the machine forbids.
 * docs/state-machine.md is generated from this table, so the two cannot drift.
 */
import { InvalidTransitionError } from '../errors.js';
import { JOB_STATES, TERMINAL_STATES, type JobState } from '../types.js';

/** Why a transition happened. Kept alongside the target state so the table reads as intent. */
export type TransitionReason =
  | 'enqueue'
  | 'claim'
  | 'succeed'
  | 'retry'
  | 'exhaust'
  | 'non_retryable'
  | 'reap_retry'
  | 'reap_exhaust'
  | 'cancel'
  | 'cancel_ack'
  | 'operator_retry'
  | 'replay';

export interface Transition {
  readonly from: JobState;
  readonly to: JobState;
  readonly reason: TransitionReason;
  readonly description: string;
}

/**
 * Every permitted transition. Anything absent here is forbidden.
 *
 * Note the asymmetry on cancellation: `running -> cancelled` is only reachable via
 * `cancel_ack`, never directly. An operator cancelling a running job sets
 * cancel_requested_at and the handler is aborted; the job stays `running` until the
 * worker acknowledges or the lease expires. Marking it cancelled unilaterally would
 * report a state the executing process has not agreed to, when its side effects may
 * already have happened.
 */
export const TRANSITIONS: readonly Transition[] = Object.freeze([
  {
    from: 'available',
    to: 'running',
    reason: 'claim',
    description: 'A worker claimed the job; attempt is incremented in the same statement.',
  },
  {
    from: 'available',
    to: 'cancelled',
    reason: 'cancel',
    description: 'An operator cancelled a job that had not started.',
  },
  {
    from: 'running',
    to: 'succeeded',
    reason: 'succeed',
    description: 'The handler resolved and the owning worker recorded completion.',
  },
  {
    from: 'running',
    to: 'available',
    reason: 'retry',
    description: 'The handler failed with attempts remaining; run_at set to a jittered backoff.',
  },
  {
    from: 'running',
    to: 'dead',
    reason: 'exhaust',
    description: 'The handler failed on the final permitted attempt.',
  },
  {
    from: 'running',
    to: 'dead',
    reason: 'non_retryable',
    description: 'The handler signalled a non-retryable failure; remaining attempts are skipped.',
  },
  {
    from: 'running',
    to: 'available',
    reason: 'reap_retry',
    description: 'The lease expired with attempts remaining; the reaper recovered the job.',
  },
  {
    from: 'running',
    to: 'dead',
    reason: 'reap_exhaust',
    description: 'The lease expired on the final attempt; no worker survived to report.',
  },
  {
    from: 'running',
    to: 'cancelled',
    reason: 'cancel_ack',
    description: 'The worker acknowledged an operator cancellation request.',
  },
  {
    from: 'dead',
    to: 'available',
    reason: 'replay',
    description: 'An operator replayed a dead-lettered job; attempt resets to zero.',
  },
  {
    from: 'available',
    to: 'available',
    reason: 'operator_retry',
    description: 'An operator moved run_at to now; does not consume an attempt.',
  },
]);

const PERMITTED: ReadonlyMap<JobState, ReadonlySet<JobState>> = (() => {
  const m = new Map<JobState, Set<JobState>>();
  for (const s of JOB_STATES) m.set(s, new Set());
  for (const t of TRANSITIONS) m.get(t.from)?.add(t.to);
  return m;
})();

const REASONS: ReadonlyMap<string, Transition> = new Map(
  TRANSITIONS.map((t) => [`${t.from}->${t.to}:${t.reason}`, t]),
);

export function isTerminal(state: JobState): boolean {
  return (TERMINAL_STATES as readonly JobState[]).includes(state);
}

export function isClaimable(state: JobState): boolean {
  return state === 'available';
}

export function permittedTargets(from: JobState): readonly JobState[] {
  return [...(PERMITTED.get(from) ?? [])];
}

export function canTransition(from: JobState, to: JobState): boolean {
  return PERMITTED.get(from)?.has(to) ?? false;
}

/**
 * Throws unless the transition is permitted.
 *
 * @throws {InvalidTransitionError} naming the current state and permitted targets.
 */
export function assertTransition(from: JobState, to: JobState): void {
  if (!canTransition(from, to)) {
    throw new InvalidTransitionError(from, to, permittedTargets(from));
  }
}

/** Throws unless the transition is permitted for this specific reason. */
export function assertTransitionReason(
  from: JobState,
  to: JobState,
  reason: TransitionReason,
): void {
  if (!REASONS.has(`${from}->${to}:${reason}`)) {
    throw new InvalidTransitionError(from, to, permittedTargets(from));
  }
}

/**
 * Renders the transition table as Mermaid, for docs/state-machine.md.
 * Generated rather than hand-maintained so the diagram cannot drift from the code.
 */
export function toMermaid(): string {
  const lines = ['stateDiagram-v2', '  [*] --> available : enqueue'];
  for (const t of TRANSITIONS) {
    if (t.from === t.to) {
      lines.push(`  ${t.from} --> ${t.to} : ${t.reason}`);
    } else {
      lines.push(`  ${t.from} --> ${t.to} : ${t.reason}`);
    }
  }
  for (const s of TERMINAL_STATES) lines.push(`  ${s} --> [*]`);
  return lines.join('\n');
}

/** Renders the transition table as Markdown, for docs/state-machine.md. */
export function toMarkdownTable(): string {
  const rows = TRANSITIONS.map(
    (t) => `| \`${t.from}\` | \`${t.to}\` | \`${t.reason}\` | ${t.description} |`,
  );
  return [
    '| From | To | Reason | Description |',
    '| ---- | -- | ------ | ----------- |',
    ...rows,
  ].join('\n');
}
