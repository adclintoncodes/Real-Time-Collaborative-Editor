/** Shared protocol types for WebSocket + OT + Kafka. */

export interface AuthClaims {
  sub: string;
  name: string;
  iat?: number;
  exp?: number;
}

export interface StepJSON {
  stepType: string;
  from?: number;
  to?: number;
  slice?: unknown;
  mark?: unknown;
  attr?: string;
  [key: string]: unknown;
}

export interface ClientOpMessage {
  type: 'op';
  documentId: string;
  clientId: string;
  baseRevision: number;
  steps: StepJSON[];
  version?: number;
}

export interface PresenceMessage {
  type: 'presence';
  documentId: string;
  clientId: string;
  userId: string;
  userName: string;
  from: number;
  to: number;
  color: string;
}

export interface JoinMessage {
  type: 'join';
  documentId: string;
  clientId: string;
}

export interface PingMessage {
  type: 'ping';
}

export type ClientMessage = ClientOpMessage | PresenceMessage | JoinMessage | PingMessage;

export interface AckMessage {
  type: 'ack';
  documentId: string;
  revision: number;
  clientId: string;
}

export interface RemoteOpMessage {
  type: 'op';
  documentId: string;
  revision: number;
  clientId: string;
  userId: string;
  steps: StepJSON[];
}

export interface SnapshotMessage {
  type: 'snapshot';
  documentId: string;
  revision: number;
  content: unknown;
}

export interface ErrorMessage {
  type: 'error';
  code: string;
  message: string;
}

export interface PongMessage {
  type: 'pong';
}

export interface PeersMessage {
  type: 'peers';
  documentId: string;
  peers: Array<{
    clientId: string;
    userId: string;
    userName: string;
    color: string;
  }>;
}

export type ServerMessage =
  | AckMessage
  | RemoteOpMessage
  | SnapshotMessage
  | ErrorMessage
  | PongMessage
  | PeersMessage
  | PresenceMessage;

export interface DocMutationEvent {
  documentId: string;
  revision: number;
  clientId: string;
  userId: string;
  steps: StepJSON[];
  timestamp: string;
}

export interface TransformedOp {
  revision: number;
  clientId: string;
  userId: string;
  steps: StepJSON[];
  baseRevision: number;
}
