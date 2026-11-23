/**
 * WebSocket gateway with JWT auth, per-document room isolation,
 * heartbeat pings, and token-bucket rate limiting.
 */

import { createServer, type IncomingMessage, type Server as HttpServer } from 'node:http';
import { WebSocketServer, WebSocket, type RawData } from 'ws';
import { verifyToken, mintDemoToken } from '../auth/jwt.js';
import { otEngine } from '../ot/otEngine.js';
import { publishMutation } from '../kafka/producer.js';
import { RateLimiterRegistry } from './rateLimiter.js';
import type {
  AuthClaims,
  ClientMessage,
  PresenceMessage,
  ServerMessage,
  StepJSON,
} from '../types.js';

const HEARTBEAT_MS = 25_000;
const COLORS = ['#3d9b6a', '#c47a3a', '#3a7ac4', '#a33d7a', '#7a9b3d', '#5a4fcf'];

interface PeerState {
  ws: WebSocket;
  clientId: string;
  userId: string;
  userName: string;
  color: string;
  documentId: string | null;
  isAlive: boolean;
  lastPresence?: PresenceMessage;
}

function parseUrl(req: IncomingMessage): URL {
  return new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
}

function send(ws: WebSocket, msg: ServerMessage): void {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function extractToken(req: IncomingMessage, url: URL): string | null {
  const q = url.searchParams.get('token');
  if (q) return q;
  const auth = req.headers.authorization;
  if (auth?.startsWith('Bearer ')) return auth.slice(7);
  const proto = req.headers['sec-websocket-protocol'];
  if (typeof proto === 'string' && proto.length > 0) {
    const parts = proto.split(',').map((p) => p.trim());
    return parts[0] ?? null;
  }
  return null;
}

export interface GatewayOptions {
  port?: number;
  server?: HttpServer;
  path?: string;
  allowAnonymous?: boolean;
}

export class WsGateway {
  private readonly wss: WebSocketServer;
  private readonly peers = new Map<WebSocket, PeerState>();
  private readonly rooms = new Map<string, Set<WebSocket>>();
  private readonly limiter = new RateLimiterRegistry(40, 20);
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private colorIdx = 0;
  readonly httpServer: HttpServer;
  private readonly ownServer: boolean;

  constructor(options: GatewayOptions = {}) {
    this.ownServer = !options.server;
    this.httpServer = options.server ?? createServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('collab-ws-gateway');
    });

    this.wss = new WebSocketServer({
      server: this.httpServer,
      path: options.path ?? '/ws',
    });

    this.wss.on('connection', (ws, req) => this.onConnection(ws, req, options.allowAnonymous !== false));
    this.startHeartbeat();

    if (this.ownServer) {
      const port = options.port ?? Number(process.env.PORT ?? 3001);
      this.httpServer.listen(port, () => {
        console.log(`[ws] gateway listening on :${port}${options.path ?? '/ws'}`);
      });
    }
  }

  private onConnection(ws: WebSocket, req: IncomingMessage, allowAnonymous: boolean): void {
    const url = parseUrl(req);
    let claims: AuthClaims;

    try {
      const token = extractToken(req, url);
      if (!token) {
        if (!allowAnonymous) {
          send(ws, { type: 'error', code: 'unauthorized', message: 'JWT required' });
          ws.close(4401, 'unauthorized');
          return;
        }
        const demo = mintDemoToken();
        claims = verifyToken(demo);
      } else {
        claims = verifyToken(token);
      }
    } catch {
      send(ws, { type: 'error', code: 'unauthorized', message: 'Invalid JWT' });
      ws.close(4401, 'unauthorized');
      return;
    }

    const clientId = url.searchParams.get('clientId') ?? `c-${Math.random().toString(36).slice(2, 10)}`;
    const color = COLORS[this.colorIdx++ % COLORS.length]!;
    const peer: PeerState = {
      ws,
      clientId,
      userId: claims.sub,
      userName: claims.name,
      color,
      documentId: null,
      isAlive: true,
    };
    this.peers.set(ws, peer);

    ws.on('pong', () => {
      peer.isAlive = true;
    });

    ws.on('message', (data) => this.onMessage(peer, data));
    ws.on('close', () => this.onClose(peer));
    ws.on('error', () => this.onClose(peer));

    // Optional auto-join via query
    const docId = url.searchParams.get('documentId');
    if (docId) {
      this.joinRoom(peer, docId);
    }
  }

  private onMessage(peer: PeerState, raw: RawData): void {
    if (!this.limiter.allow(peer.clientId)) {
      send(peer.ws, { type: 'error', code: 'rate_limited', message: 'Too many messages' });
      return;
    }

    let msg: ClientMessage;
    try {
      msg = JSON.parse(String(raw)) as ClientMessage;
    } catch {
      send(peer.ws, { type: 'error', code: 'bad_json', message: 'Malformed JSON' });
      return;
    }

    switch (msg.type) {
      case 'ping':
        send(peer.ws, { type: 'pong' });
        return;
      case 'join':
        this.joinRoom(peer, msg.documentId);
        return;
      case 'presence':
        this.handlePresence(peer, msg);
        return;
      case 'op':
        this.handleOp(peer, msg.documentId, msg.baseRevision, msg.steps);
        return;
      default:
        send(peer.ws, { type: 'error', code: 'unknown_type', message: 'Unknown message type' });
    }
  }

  private joinRoom(peer: PeerState, documentId: string): void {
    if (peer.documentId && peer.documentId !== documentId) {
      this.leaveRoom(peer);
    }
    peer.documentId = documentId;
    let room = this.rooms.get(documentId);
    if (!room) {
      room = new Set();
      this.rooms.set(documentId, room);
    }
    room.add(peer.ws);

    send(peer.ws, {
      type: 'snapshot',
      documentId,
      revision: otEngine.getRevision(documentId),
      content: null,
    });

    this.broadcastPeers(documentId);
  }

  private leaveRoom(peer: PeerState): void {
    if (!peer.documentId) return;
    const room = this.rooms.get(peer.documentId);
    room?.delete(peer.ws);
    if (room && room.size === 0) this.rooms.delete(peer.documentId);
    const docId = peer.documentId;
    peer.documentId = null;
    this.broadcastPeers(docId);
  }

  private handlePresence(peer: PeerState, msg: PresenceMessage): void {
    if (!peer.documentId || peer.documentId !== msg.documentId) return;
    const enriched: PresenceMessage = {
      ...msg,
      clientId: peer.clientId,
      userId: peer.userId,
      userName: peer.userName,
      color: peer.color,
    };
    peer.lastPresence = enriched;
    this.broadcast(peer.documentId, enriched, peer.ws);
  }

  private handleOp(
    peer: PeerState,
    documentId: string,
    baseRevision: number,
    steps: StepJSON[],
  ): void {
    if (peer.documentId !== documentId) {
      send(peer.ws, { type: 'error', code: 'not_in_room', message: 'Join document first' });
      return;
    }

    const result = otEngine.submit(documentId, peer.clientId, peer.userId, baseRevision, steps);
    if (!result.ok) {
      send(peer.ws, { type: 'error', code: 'ot_reject', message: result.reason });
      return;
    }

    if (result.latencyMs > 50) {
      console.warn(`[ot] latency ${result.latencyMs.toFixed(2)}ms exceeds 50ms target for ${documentId}`);
    }

    send(peer.ws, {
      type: 'ack',
      documentId,
      revision: result.op.revision,
      clientId: peer.clientId,
    });

    this.broadcast(
      documentId,
      {
        type: 'op',
        documentId,
        revision: result.op.revision,
        clientId: peer.clientId,
        userId: peer.userId,
        steps: result.op.steps,
      },
      peer.ws,
    );

    publishMutation({
      documentId,
      revision: result.op.revision,
      clientId: peer.clientId,
      userId: peer.userId,
      steps: result.op.steps,
      timestamp: new Date().toISOString(),
    });
  }

  private broadcast(documentId: string, msg: ServerMessage, except?: WebSocket): void {
    const room = this.rooms.get(documentId);
    if (!room) return;
    const payload = JSON.stringify(msg);
    for (const ws of room) {
      if (except && ws === except) continue;
      if (ws.readyState === WebSocket.OPEN) ws.send(payload);
    }
  }

  private broadcastPeers(documentId: string): void {
    const room = this.rooms.get(documentId);
    if (!room) return;
    const peers = [...room]
      .map((ws) => this.peers.get(ws))
      .filter((p): p is PeerState => Boolean(p))
      .map((p) => ({
        clientId: p.clientId,
        userId: p.userId,
        userName: p.userName,
        color: p.color,
      }));
    this.broadcast(documentId, { type: 'peers', documentId, peers });
  }

  private onClose(peer: PeerState): void {
    this.leaveRoom(peer);
    this.peers.delete(peer.ws);
    this.limiter.remove(peer.clientId);
  }

  private startHeartbeat(): void {
    this.heartbeatTimer = setInterval(() => {
      for (const [ws, peer] of this.peers) {
        if (!peer.isAlive) {
          ws.terminate();
          this.onClose(peer);
          continue;
        }
        peer.isAlive = false;
        ws.ping();
      }
    }, HEARTBEAT_MS);
    this.heartbeatTimer.unref?.();
  }

  async close(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    for (const ws of this.peers.keys()) ws.close();
    await new Promise<void>((resolve) => this.wss.close(() => resolve()));
    if (this.ownServer) {
      await new Promise<void>((resolve, reject) => {
        this.httpServer.close((err) => (err ? reject(err) : resolve()));
      });
    }
  }
}

export function startGateway(options?: GatewayOptions): WsGateway {
  return new WsGateway(options);
}
