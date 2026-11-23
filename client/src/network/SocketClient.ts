/**
 * WebSocket client with exponential backoff reconnection, offline op queue,
 * and BroadcastChannel multi-tab fallback when VITE_WS_URL is unset/unreachable.
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
  /** Force BroadcastChannel demo mode (used by tests / Pages). */
  forceFallback?: boolean;
}

interface BroadcastEnvelope {
  channel: 'collab-docs';
  documentId: string;
  senderId: string;
  payload: unknown;
}

export class SocketClient {
  private ws: WebSocket | null = null;
  private bc: BroadcastChannel | null = null;
  private state: ConnectionState = 'closed';
  private intentionalClose = false;
  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private connectWatchdog: ReturnType<typeof setTimeout> | null = null;
  private readonly queue = new OfflineQueue();
  private readonly maxBackoffMs: number;
  private readonly opts: SocketClientOptions;
  private fallbackArmed = false;

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

    if (this.opts.forceFallback || !base) {
      this.enableFallback('no_ws_url');
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
      this.enableFallback('ws_construct_failed');
      return;
    }

    // If the backend never opens (typical on GitHub Pages), fall back quickly.
    this.connectWatchdog = setTimeout(() => {
      if (this.state === 'connecting') {
        this.ws?.close();
        this.enableFallback('ws_unreachable');
      }
    }, 2500);

    this.ws.addEventListener('open', () => {
      if (this.connectWatchdog) clearTimeout(this.connectWatchdog);
      this.attempt = 0;
      this.fallbackArmed = false;
      this.teardownFallback();
      this.setState('open');
      this.ws?.send(
        JSON.stringify({
          type: 'join',
          documentId: this.opts.documentId,
          clientId: this.opts.clientId,
        }),
      );
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
      if (this.intentionalClose) {
        this.setState('closed');
        return;
      }
      if (this.attempt >= 2 || this.fallbackArmed) {
        this.enableFallback('ws_closed');
        return;
      }
      this.scheduleReconnect();
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
      return;
    }

    if (this.state === 'fallback' && this.bc) {
      this.broadcastLocal(payload);
      // Local ack so the editor advances revision optimistically in demo mode
      this.opts.onMessage?.({
        type: 'ack',
        documentId: this.opts.documentId,
        revision: op.baseRevision + 1,
        clientId: this.opts.clientId,
      });
      return;
    }

    this.queue.enqueue(op);
  }

  sendPresence(presence: Record<string, unknown>): void {
    const payload = {
      type: 'presence',
      documentId: this.opts.documentId,
      clientId: this.opts.clientId,
      ...presence,
    };

    if (this.state === 'open' && this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
      return;
    }

    if (this.state === 'fallback' && this.bc) {
      this.broadcastLocal(payload);
    }
  }

  disconnect(): void {
    this.intentionalClose = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.connectWatchdog) clearTimeout(this.connectWatchdog);
    this.ws?.close();
    this.ws = null;
    this.teardownFallback();
    this.setState('closed');
  }

  private enableFallback(_reason: string): void {
    this.fallbackArmed = true;
    if (this.connectWatchdog) clearTimeout(this.connectWatchdog);
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.ws?.close();
    this.ws = null;

    if (typeof BroadcastChannel === 'undefined') {
      this.setState('closed');
      return;
    }

    if (!this.bc) {
      this.bc = new BroadcastChannel(`collab-docs:${this.opts.documentId}`);
      this.bc.onmessage = (ev: MessageEvent<BroadcastEnvelope>) => {
        const data = ev.data;
        if (!data || data.channel !== 'collab-docs') return;
        if (data.documentId !== this.opts.documentId) return;
        if (data.senderId === this.opts.clientId) return;
        this.opts.onMessage?.(data.payload);
      };
    }

    this.setState('fallback');
    // Announce this tab so siblings can show a collaborator chip.
    this.broadcastLocal({
      type: 'presence',
      documentId: this.opts.documentId,
      clientId: this.opts.clientId,
      userId: this.opts.clientId,
      userName: 'Tab collaborator',
      color: '#5a9fd4',
      from: 1,
      to: 1,
    });
    this.flushQueue();
  }

  private broadcastLocal(payload: unknown): void {
    const envelope: BroadcastEnvelope = {
      channel: 'collab-docs',
      documentId: this.opts.documentId,
      senderId: this.opts.clientId,
      payload,
    };
    this.bc?.postMessage(envelope);
  }

  private teardownFallback(): void {
    if (this.bc) {
      this.bc.close();
      this.bc = null;
    }
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
