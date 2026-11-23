/**
 * Collab server — Express REST + WebSocket gateway + optional Kafka consumer.
 */

import express from 'express';
import cors from 'cors';
import { createServer } from 'node:http';
import { WsGateway } from './gateway/wsGateway.js';
import { docRouter } from './api/docRoutes.js';
import { disconnectProducer } from './kafka/producer.js';
import { MetadataConsumer } from './consumers/metadataConsumer.js';
import { connectCassandra, disconnectCassandra } from './db/cassandra.js';

const PORT = Number(process.env.PORT ?? 3001);
const RUN_CONSUMER = process.env.RUN_CONSUMER !== 'false';

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use('/api', docRouter);

const httpServer = createServer(app);
const gateway = new WsGateway({ server: httpServer, path: '/ws', allowAnonymous: true });

let consumer: MetadataConsumer | null = null;

async function main(): Promise<void> {
  try {
    await connectCassandra();
    console.log('[server] Cassandra connected');
  } catch (err) {
    console.warn('[server] Cassandra unavailable at boot; API will retry on demand', err);
  }

  if (RUN_CONSUMER) {
    consumer = new MetadataConsumer();
    void consumer.start().catch((err) => {
      console.error('[server] metadata consumer failed to start', err);
    });
  }

  httpServer.listen(PORT, () => {
    console.log(`[server] listening on :${PORT} (REST /api, WS /ws)`);
  });
}

async function shutdown(signal: string): Promise<void> {
  console.log(`[server] shutting down on ${signal}`);
  await gateway.close();
  if (consumer) await consumer.stop();
  await disconnectProducer();
  await disconnectCassandra();
  httpServer.close();
  process.exit(0);
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

void main();
