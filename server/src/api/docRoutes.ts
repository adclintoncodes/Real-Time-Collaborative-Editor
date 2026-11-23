/**
 * REST endpoints for creating documents and loading canonical state + history.
 */

import { Router, type Request, type Response } from 'express';
import { types } from 'cassandra-driver';
import {
  connectCassandra,
  getDocumentMetadata,
  getLatestSnapshot,
  insertDocumentMetadata,
  listRevisions,
} from '../db/cassandra.js';
import { otEngine } from '../ot/otEngine.js';
import { mintDemoToken, signToken } from '../auth/jwt.js';

export const docRouter = Router();

docRouter.get('/health', (_req, res) => {
  res.json({ ok: true, service: 'collab-api' });
});

/** Mint a demo JWT for local / Pages clients. */
docRouter.post('/auth/demo', (req: Request, res: Response) => {
  const name = typeof req.body?.name === 'string' ? req.body.name : undefined;
  const userId = typeof req.body?.userId === 'string' ? req.body.userId : undefined;
  const token = mintDemoToken(userId, name);
  res.json({ token });
});

docRouter.post('/auth/token', (req: Request, res: Response) => {
  const sub = typeof req.body?.sub === 'string' ? req.body.sub : null;
  const name = typeof req.body?.name === 'string' ? req.body.name : 'User';
  if (!sub) {
    res.status(400).json({ error: 'sub_required' });
    return;
  }
  res.json({ token: signToken({ sub, name }) });
});

/**
 * POST /api/docs — create a new document.
 * Body: { title?: string, createdBy?: string, content?: unknown }
 */
docRouter.post('/docs', async (req: Request, res: Response) => {
  try {
    await connectCassandra();
    const documentId = types.Uuid.random().toString();
    const title = typeof req.body?.title === 'string' ? req.body.title : 'Untitled';
    const createdBy = typeof req.body?.createdBy === 'string' ? req.body.createdBy : 'anonymous';
    const now = new Date();

    await insertDocumentMetadata({
      documentId,
      title,
      createdBy,
      createdAt: now,
      updatedAt: now,
      currentRevision: 0,
      collaboratorCount: 1,
    });

    if (req.body?.content !== undefined) {
      const { saveSnapshot } = await import('../db/cassandra.js');
      await saveSnapshot({ documentId, revision: 0, content: req.body.content });
    }

    otEngine.seed(documentId, 0);

    res.status(201).json({
      documentId,
      title,
      createdBy,
      currentRevision: 0,
      createdAt: now.toISOString(),
    });
  } catch (err) {
    console.error('[api] POST /docs failed', err);
    res.status(500).json({ error: 'create_failed', detail: String(err) });
  }
});

/**
 * GET /api/docs/:id — load metadata, latest snapshot, and revision history.
 */
docRouter.get('/docs/:id', async (req: Request, res: Response) => {
  try {
    await connectCassandra();
    const rawId = req.params.id;
    const id = Array.isArray(rawId) ? rawId[0] : rawId;
    if (!id || typeof id !== 'string') {
      res.status(400).json({ error: 'id_required' });
      return;
    }

    const meta = await getDocumentMetadata(id);
    if (!meta) {
      res.status(404).json({ error: 'not_found' });
      return;
    }

    const snapshot = await getLatestSnapshot(id);
    const fromRev = snapshot?.revision ?? 0;
    const revisions = await listRevisions(id, fromRev);

    // Keep in-memory OT engine aligned with persisted head.
    otEngine.seed(id, meta.currentRevision);

    res.json({
      documentId: meta.documentId,
      title: meta.title,
      createdBy: meta.createdBy,
      createdAt: meta.createdAt,
      updatedAt: meta.updatedAt,
      currentRevision: meta.currentRevision,
      collaboratorCount: meta.collaboratorCount,
      snapshot: snapshot
        ? {
            revision: snapshot.revision,
            content: JSON.parse(snapshot.contentJson) as unknown,
            createdAt: snapshot.createdAt,
          }
        : null,
      revisions: revisions.map((r) => ({
        revision: r.revision,
        clientId: r.clientId,
        userId: r.userId,
        steps: JSON.parse(r.stepJson) as unknown,
        createdAt: r.createdAt,
      })),
      liveRevision: otEngine.getRevision(id),
    });
  } catch (err) {
    console.error('[api] GET /docs/:id failed', err);
    res.status(500).json({ error: 'load_failed', detail: String(err) });
  }
});
