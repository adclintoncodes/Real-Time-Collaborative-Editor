/**
 * WebSocket client with exponential backoff reconnection and offline op queue.
 * Phase 5 adds BroadcastChannel multi-tab fallback when VITE_WS_URL is unset.
 */

import { OfflineQueue, type QueuedOp } from './OfflineQueue';

export type ConnectionState = 'connecting' | 'open' | 'closed' | 'fallback';

export interface SocketClientOptions {
  url?: string;
  documentId: string;
  clientId: string;
  token?: string;
  onMessage?: (msg: unknown) => void;
  onStateChange?: (state: ConnectionState) => void;
  maxBackoffMs?: number;
}

export class SocketClient {
  private ws: WebSocket | null = null;
  private state: ConnectionState = 'closed';
  private intentionalClose = false;
  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly queue = new OfflineQueue();
  private readonly maxBackoffMs: number;
  private readonly opts: SocketClientOptions;

  constructor(opts: SocketClientOptions) {
    this.opts = opts;
    this.maxBackoffMs = opts.maxBackoffMs ?? 15_000;
  }

  get connectionState(): ConnectionState {
    return this.state;
  }

  get pendingCount(): number {
    return this.queue.size;
  }

  connect(): void {
    this.intentionalClose = false;
    const base = this.opts.url ?? import.meta.env.VITE_WS_URL;
    if (!base) {
      // Phase 5 wires BroadcastChannel; for now stay closed and queue ops.
      this.setState('closed');
      return;
    }

    this.setState('connecting');
    const u = new URL(base);
    u.searchParams.set('documentId', this.opts.documentId);
    u.searchParams.set('clientId', this.opts.clientId);
    if (this.opts.token) u.searchParams.set('token', this.opts.token);

    try {
      this.ws = new WebSocket(u.toString());
    } catch {
      this.scheduleReconnect();
      return;
    }

    this.ws.addEventListener('open', () => {
      this.attempt = 0;
      this.setState('open');
      this.ws?.send(JSON.stringify({ type: 'join', documentId: this.opts.documentId, clientId: this.opts.clientId }));
      this.flushQueue();
    });

    this.ws.addEventListener('message', (ev) => {
      try {
        const msg = JSON.parse(String(ev.data)) as unknown;
        this.opts.onMessage?.(msg);
      } catch {
        /* ignore malformed */
      }
    });

    this.ws.addEventListener('close', () => {
      this.ws = null;
      if (!this.intentionalClose) this.scheduleReconnect();
      else this.setState('closed');
    });

    this.ws.addEventListener('error', () => {
      this.ws?.close();
    });
  }

  sendOp(op: QueuedOp): void {
    const payload = {
      type: 'op',
      documentId: this.opts.documentId,
      clientId: this.opts.clientId,
      baseRevision: op.baseRevision,
      steps: op.steps,
    };

    if (this.state === 'open' && this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
    } else {
      this.queue.enqueue(op);
    }
  }

  sendPresence(presence: Record<string, unknown>): void {
    if (this.state === 'open' && this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(
        JSON.stringify({
          type: 'presence',
          documentId: this.opts.documentId,
          clientId: this.opts.clientId,
          ...presence,
        }),
      );
    }
  }

  disconnect(): void {
    this.intentionalClose = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.ws?.close();
    this.ws = null;
    this.setState('closed');
  }

  private flushQueue(): void {
    const pending = this.queue.drain();
    for (const op of pending) {
      this.sendOp(op);
    }
  }

  private scheduleReconnect(): void {
    this.setState('closed');
    if (this.intentionalClose) return;
    const delay = Math.min(this.maxBackoffMs, 500 * 2 ** this.attempt) + Math.random() * 200;
    this.attempt += 1;
    this.reconnectTimer = setTimeout(() => this.connect(), delay);
  }

  private setState(state: ConnectionState): void {
    this.state = state;
    this.opts.onStateChange?.(state);
  }
}
