/**
 * Server-authoritative Operational Transformation engine.
 *
 * Orders incoming ProseMirror-style step operations by canonical revision,
 * transforms concurrent out-of-order steps against the accepted history,
 * and returns broadcast-ready deltas.
 *
 * Step mapping follows ProseMirror transform step position semantics:
 * insert/replace steps shift later positions by (to - from) delta length.
 */

import type { StepJSON, TransformedOp } from '../types.js';

export interface AcceptedOp {
  revision: number;
  clientId: string;
  userId: string;
  steps: StepJSON[];
  acceptedAt: number;
}

export interface SubmitResult {
  ok: true;
  op: TransformedOp;
  latencyMs: number;
}

export interface SubmitError {
  ok: false;
  reason: string;
}

export type SubmitOutcome = SubmitResult | SubmitError;

function isReplaceLike(step: StepJSON): boolean {
  return (
    step.stepType === 'replace' ||
    step.stepType === 'replaceAround' ||
    typeof step.from === 'number'
  );
}

/** Net document-size delta produced by a step (insert positive, delete negative). */
export function stepSizeDelta(step: StepJSON): number {
  if (!isReplaceLike(step)) return 0;
  const from = Number(step.from ?? 0);
  const to = Number(step.to ?? from);
  const deleted = Math.max(0, to - from);
  const inserted = Number(step.insertLength ?? step.sliceSize ?? 0);
  return inserted - deleted;
}

/**
 * Transform a single step against one already-accepted step that happened
 * concurrently (i.e. the incoming step was based on an older revision).
 */
export function transformStepAgainst(step: StepJSON, against: StepJSON): StepJSON {
  if (!isReplaceLike(step) || !isReplaceLike(against)) {
    return { ...step };
  }

  const aFrom = Number(against.from ?? 0);
  const aTo = Number(against.to ?? aFrom);
  const delta = stepSizeDelta(against);

  let from = Number(step.from ?? 0);
  let to = Number(step.to ?? from);

  // If the accepted step is entirely before this step, shift positions.
  if (aTo <= from) {
    from += delta;
    to += delta;
    return { ...step, from, to };
  }

  // If the accepted step is entirely after this step, no change.
  if (aFrom >= to) {
    return { ...step, from, to };
  }

  // Overlap: bias toward preserving the already-accepted change.
  // Collapse the incoming range onto the end of the accepted range (post-transform).
  const newFrom = aFrom + Math.max(0, Number(against.insertLength ?? against.sliceSize ?? 0));
  return { ...step, from: newFrom, to: newFrom };
}

/** Transform a batch of steps against a sequence of already-accepted ops. */
export function transformSteps(
  steps: StepJSON[],
  againstOps: AcceptedOp[],
): StepJSON[] {
  let current = steps.map((s) => ({ ...s }));
  for (const op of againstOps) {
    for (const against of op.steps) {
      current = current.map((s) => transformStepAgainst(s, against));
    }
  }
  return current;
}

export class OtEngine {
  private readonly history = new Map<string, AcceptedOp[]>();
  private readonly revisions = new Map<string, number>();
  /** Max history retained per document for transform lookback. */
  private readonly maxHistory: number;

  constructor(maxHistory = 500) {
    this.maxHistory = maxHistory;
  }

  getRevision(documentId: string): number {
    return this.revisions.get(documentId) ?? 0;
  }

  getHistory(documentId: string): readonly AcceptedOp[] {
    return this.history.get(documentId) ?? [];
  }

  /** Seed or reset a document's canonical revision (e.g. after loading from Cassandra). */
  seed(documentId: string, revision: number, history: AcceptedOp[] = []): void {
    this.revisions.set(documentId, revision);
    this.history.set(documentId, history.slice(-this.maxHistory));
  }

  /**
   * Submit client steps based on `baseRevision`.
   * Concurrent steps (baseRevision < current) are transformed against the gap.
   */
  submit(
    documentId: string,
    clientId: string,
    userId: string,
    baseRevision: number,
    steps: StepJSON[],
  ): SubmitOutcome {
    const started = performance.now();

    if (!steps.length) {
      return { ok: false, reason: 'empty_steps' };
    }

    const current = this.getRevision(documentId);
    if (baseRevision > current) {
      return { ok: false, reason: `future_base_revision:${baseRevision}>${current}` };
    }

    const hist = this.history.get(documentId) ?? [];
    const concurrent = hist.filter((op) => op.revision > baseRevision);
    const transformed = transformSteps(steps, concurrent);

    const revision = current + 1;
    const accepted: AcceptedOp = {
      revision,
      clientId,
      userId,
      steps: transformed,
      acceptedAt: Date.now(),
    };

    const nextHist = [...hist, accepted].slice(-this.maxHistory);
    this.history.set(documentId, nextHist);
    this.revisions.set(documentId, revision);

    const latencyMs = performance.now() - started;
    return {
      ok: true,
      op: {
        revision,
        clientId,
        userId,
        steps: transformed,
        baseRevision,
      },
      latencyMs,
    };
  }
}

/** Singleton used by the gateway; tests construct their own instances. */
export const otEngine = new OtEngine();
