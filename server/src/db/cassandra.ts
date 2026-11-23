/**
 * Apache Cassandra client for document metadata, revisions, and snapshots.
 */

import cassandra from 'cassandra-driver';
import { types } from 'cassandra-driver';
import type { StepJSON } from '../types.js';

const contactPoints = (process.env.CASSANDRA_CONTACT_POINTS ?? '127.0.0.1').split(',');
const localDataCenter = process.env.CASSANDRA_DC ?? 'datacenter1';
const keyspace = process.env.CASSANDRA_KEYSPACE ?? 'collab_docs';

let client: cassandra.Client | null = null;

export function getCassandraClient(): cassandra.Client {
  if (!client) {
    client = new cassandra.Client({
      contactPoints,
      localDataCenter,
      keyspace,
      protocolOptions: { port: Number(process.env.CASSANDRA_PORT ?? 9042) },
      socketOptions: { connectTimeout: 10_000 },
      queryOptions: { consistency: types.consistencies.one },
    });
  }
  return client;
}

export async function connectCassandra(): Promise<cassandra.Client> {
  const c = getCassandraClient();
  await c.connect();
  return c;
}

export async function disconnectCassandra(): Promise<void> {
  if (client) {
    await client.shutdown();
    client = null;
  }
}

export interface DocumentMetadata {
  documentId: string;
  title: string;
  createdBy: string;
  createdAt: Date;
  updatedAt: Date;
  currentRevision: number;
  collaboratorCount: number;
}

export interface DocumentRevision {
  documentId: string;
  revision: number;
  stepJson: string;
  clientId: string;
  userId: string;
  createdAt: Date;
}

export interface DocumentSnapshot {
  documentId: string;
  revision: number;
  contentJson: string;
  createdAt: Date;
}

export async function insertDocumentMetadata(meta: DocumentMetadata): Promise<void> {
  const c = getCassandraClient();
  await c.execute(
    `INSERT INTO document_metadata
      (document_id, title, created_by, created_at, updated_at, current_revision, collaborator_count)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [
      types.Uuid.fromString(meta.documentId),
      meta.title,
      meta.createdBy,
      meta.createdAt,
      meta.updatedAt,
      meta.currentRevision,
      meta.collaboratorCount,
    ],
    { prepare: true },
  );
}

export async function getDocumentMetadata(documentId: string): Promise<DocumentMetadata | null> {
  const c = getCassandraClient();
  const result = await c.execute(
    `SELECT document_id, title, created_by, created_at, updated_at, current_revision, collaborator_count
     FROM document_metadata WHERE document_id = ?`,
    [types.Uuid.fromString(documentId)],
    { prepare: true },
  );
  const row = result.first();
  if (!row) return null;
  return {
    documentId: row['document_id'].toString(),
    title: row['title'],
    createdBy: row['created_by'],
    createdAt: row['created_at'],
    updatedAt: row['updated_at'],
    currentRevision: row['current_revision'],
    collaboratorCount: row['collaborator_count'],
  };
}

export async function upsertRevision(params: {
  documentId: string;
  revision: number;
  steps: StepJSON[];
  clientId: string;
  userId: string;
  timestamp: Date;
}): Promise<void> {
  const c = getCassandraClient();
  const batch = [
    {
      query: `INSERT INTO document_revisions
        (document_id, revision, step_json, client_id, user_id, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      params: [
        types.Uuid.fromString(params.documentId),
        params.revision,
        JSON.stringify(params.steps),
        params.clientId,
        params.userId,
        params.timestamp,
      ],
    },
    {
      query: `UPDATE document_metadata
       SET current_revision = ?, updated_at = ?
       WHERE document_id = ?`,
      params: [
        params.revision,
        params.timestamp,
        types.Uuid.fromString(params.documentId),
      ],
    },
  ];
  await c.batch(batch, { prepare: true });
}

export async function listRevisions(
  documentId: string,
  fromRevision = 0,
): Promise<DocumentRevision[]> {
  const c = getCassandraClient();
  const result = await c.execute(
    `SELECT document_id, revision, step_json, client_id, user_id, created_at
     FROM document_revisions
     WHERE document_id = ? AND revision > ?`,
    [types.Uuid.fromString(documentId), fromRevision],
    { prepare: true },
  );
  return result.rows.map((row) => ({
    documentId: row['document_id'].toString(),
    revision: row['revision'],
    stepJson: row['step_json'],
    clientId: row['client_id'],
    userId: row['user_id'],
    createdAt: row['created_at'],
  }));
}

export async function saveSnapshot(params: {
  documentId: string;
  revision: number;
  content: unknown;
}): Promise<void> {
  const c = getCassandraClient();
  await c.execute(
    `INSERT INTO document_snapshots (document_id, revision, content_json, created_at)
     VALUES (?, ?, ?, ?)`,
    [
      types.Uuid.fromString(params.documentId),
      params.revision,
      JSON.stringify(params.content),
      new Date(),
    ],
    { prepare: true },
  );
}

export async function getLatestSnapshot(documentId: string): Promise<DocumentSnapshot | null> {
  const c = getCassandraClient();
  const result = await c.execute(
    `SELECT document_id, revision, content_json, created_at
     FROM document_snapshots WHERE document_id = ? LIMIT 1`,
    [types.Uuid.fromString(documentId)],
    { prepare: true },
  );
  const row = result.first();
  if (!row) return null;
  return {
    documentId: row['document_id'].toString(),
    revision: row['revision'],
    contentJson: row['content_json'],
    createdAt: row['created_at'],
  };
}
