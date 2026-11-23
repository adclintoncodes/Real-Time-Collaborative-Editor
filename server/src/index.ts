/**
 * Collab server entry — WebSocket gateway + OT engine (Phase 2).
 * REST API + Kafka consumer attach in Phase 3.
 */

import { startGateway } from './gateway/wsGateway.js';
import { disconnectProducer } from './kafka/producer.js';

const gateway = startGateway({
  port: Number(process.env.PORT ?? 3001),
  path: '/ws',
  allowAnonymous: true,
});

async function shutdown(signal: string): Promise<void> {
  console.log(`[server] shutting down on ${signal}`);
  await gateway.close();
  await disconnectProducer();
  process.exit(0);
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));
