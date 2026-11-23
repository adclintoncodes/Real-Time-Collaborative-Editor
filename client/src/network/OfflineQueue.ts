/**
 * Buffers local ProseMirror steps while offline and flushes on reconnect.
 */

export interface QueuedOp {
  baseRevision: number;
  steps: unknown[];
  enqueuedAt: number;
}

export class OfflineQueue {
  private queue: QueuedOp[] = [];
  private readonly maxSize: number;

  constructor(maxSize = 500) {
    this.maxSize = maxSize;
  }

  enqueue(op: QueuedOp): void {
    this.queue.push(op);
    if (this.queue.length > this.maxSize) {
      this.queue = this.queue.slice(-this.maxSize);
    }
  }

  drain(): QueuedOp[] {
    const items = this.queue.slice();
    this.queue = [];
    return items;
  }

  peek(): readonly QueuedOp[] {
    return this.queue;
  }

  get size(): number {
    return this.queue.length;
  }

  clear(): void {
    this.queue = [];
  }
}
