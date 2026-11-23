import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  OtEngine,
  transformStepAgainst,
  transformSteps,
  stepSizeDelta,
  type AcceptedOp,
} from './otEngine.js';
import type { StepJSON } from '../types.js';

function insert(from: number, length: number): StepJSON {
  return { stepType: 'replace', from, to: from, insertLength: length, sliceSize: length };
}

function del(from: number, to: number): StepJSON {
  return { stepType: 'replace', from, to, insertLength: 0, sliceSize: 0 };
}

describe('stepSizeDelta', () => {
  it('computes insert and delete deltas', () => {
    assert.equal(stepSizeDelta(insert(5, 3)), 3);
    assert.equal(stepSizeDelta(del(2, 5)), -3);
  });
});

describe('transformStepAgainst', () => {
  it('shifts later inserts past an earlier insert', () => {
    const incoming = insert(10, 2);
    const accepted = insert(3, 4);
    const out = transformStepAgainst(incoming, accepted);
    assert.equal(out.from, 14);
    assert.equal(out.to, 14);
  });

  it('does not shift steps that precede the accepted change', () => {
    const incoming = insert(1, 1);
    const accepted = insert(8, 5);
    const out = transformStepAgainst(incoming, accepted);
    assert.equal(out.from, 1);
  });

  it('collapses overlapping deletes onto accepted insert end', () => {
    const incoming = del(4, 8);
    const accepted = insert(5, 2);
    const out = transformStepAgainst(incoming, accepted);
    assert.equal(out.from, 7);
    assert.equal(out.to, 7);
  });
});

describe('OtEngine concurrent race resolution', () => {
  it('assigns monotonic revisions and transforms the lagging client', () => {
    const engine = new OtEngine();
    const doc = 'doc-race-1';

    const a = engine.submit(doc, 'client-a', 'user-a', 0, [insert(1, 1)]);
    assert.equal(a.ok, true);
    if (!a.ok) return;
    assert.equal(a.op.revision, 1);
    assert.ok(a.latencyMs < 50);

    // Client B still based on revision 0 — concurrent with A
    const b = engine.submit(doc, 'client-b', 'user-b', 0, [insert(1, 1)]);
    assert.equal(b.ok, true);
    if (!b.ok) return;
    assert.equal(b.op.revision, 2);
    // B's insert at 1 must shift past A's insert of length 1
    assert.equal(b.op.steps[0]?.from, 2);
    assert.ok(b.latencyMs < 50);
  });

  it('resolves three-way concurrent inserts at the same position', () => {
    const engine = new OtEngine();
    const doc = 'doc-race-3way';

    const r1 = engine.submit(doc, 'c1', 'u1', 0, [insert(5, 1)]);
    const r2 = engine.submit(doc, 'c2', 'u2', 0, [insert(5, 1)]);
    const r3 = engine.submit(doc, 'c3', 'u3', 0, [insert(5, 1)]);

    assert.ok(r1.ok && r2.ok && r3.ok);
    if (!(r1.ok && r2.ok && r3.ok)) return;

    assert.deepEqual(
      [r1.op.revision, r2.op.revision, r3.op.revision],
      [1, 2, 3],
    );
    assert.equal(r1.op.steps[0]?.from, 5);
    assert.equal(r2.op.steps[0]?.from, 6);
    assert.equal(r3.op.steps[0]?.from, 7);
    assert.equal(engine.getRevision(doc), 3);
  });

  it('rejects empty steps and future base revisions', () => {
    const engine = new OtEngine();
    const empty = engine.submit('d', 'c', 'u', 0, []);
    assert.equal(empty.ok, false);

    engine.submit('d', 'c', 'u', 0, [insert(0, 1)]);
    const future = engine.submit('d', 'c', 'u', 99, [insert(0, 1)]);
    assert.equal(future.ok, false);
  });

  it('transformSteps applies a gap of accepted ops in order', () => {
    const against: AcceptedOp[] = [
      {
        revision: 1,
        clientId: 'a',
        userId: 'a',
        steps: [insert(0, 2)],
        acceptedAt: 1,
      },
      {
        revision: 2,
        clientId: 'b',
        userId: 'b',
        steps: [insert(2, 1)],
        acceptedAt: 2,
      },
    ];
    const out = transformSteps([insert(5, 1)], against);
    // +2 then +1 => from 5 becomes 8
    assert.equal(out[0]?.from, 8);
  });

  it('keeps sub-50ms latency under a burst of concurrent ops', () => {
    const engine = new OtEngine();
    const doc = 'doc-burst';
    const latencies: number[] = [];

    for (let i = 0; i < 100; i++) {
      const result = engine.submit(doc, `c-${i % 5}`, `u-${i % 5}`, Math.max(0, i - 3), [
        insert(i % 20, 1),
      ]);
      assert.equal(result.ok, true);
      if (result.ok) latencies.push(result.latencyMs);
    }

    const max = Math.max(...latencies);
    assert.ok(max < 50, `expected max latency < 50ms, got ${max}`);
  });
});
