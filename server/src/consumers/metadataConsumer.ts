/**
 * Standalone Kafka consumer that persists doc-mutations into Cassandra.
 * Runs decoupled from the WebSocket hot path so persistence never blocks OT.
 */

import { Kafka, logLevel, type Consumer, type EachBatchPayload } from 'kafkajs';
import { DOC_MUTATIONS_TOPIC } from '../kafka/producer.js';
import {
  connectCassandra,
  disconnectCassandra,
  upsertRevision,
  saveSnapshot,
  getDocumentMetadata,
  insertDocumentMetadata,
} from '../db/cassandra.js';
import type { DocMutationEvent } from '../types.js';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const groupId = process.env.KAFKA_GROUP_ID ?? 'collab-metadata-consumer';
const SNAPSHOT_EVERY = Number(process.env.SNAPSHOT_EVERY_REVISIONS ?? 50);

export class MetadataConsumer {
  private consumer: Consumer | null = null;
  private running = false;

  async start(): Promise<void> {
    await connectCassandra();

    const kafka = new Kafka({
      clientId: 'collab-metadata-consumer',
      brokers,
      logLevel: logLevel.ERROR,
      retry: { retries: 8, initialRetryTime: 500 },
    });

    this.consumer = kafka.consumer({
      groupId,
      sessionTimeout: 30_000,
    });

    await this.consumer.connect();
    await this.consumer.subscribe({ topic: DOC_MUTATIONS_TOPIC, fromBeginning: true });
    this.running = true;

    console.log(`[consumer] subscribed to ${DOC_MUTATIONS_TOPIC} as ${groupId}`);

    await this.consumer.run({
      eachBatchAutoResolve: true,
      eachBatch: async (payload) => this.handleBatch(payload),
    });
  }

  private async handleBatch({ batch, resolveOffset, heartbeat }: EachBatchPayload): Promise<void> {
    if (!this.running) return;

    for (const message of batch.messages) {
      if (!message.value) continue;
      let event: DocMutationEvent;
      try {
        event = JSON.parse(message.value.toString()) as DocMutationEvent;
      } catch {
        console.error('[consumer] bad message JSON, skipping');
        resolveOffset(message.offset);
        continue;
      }

      try {
        await this.persist(event);
      } catch (err) {
        console.error('[consumer] persist failed', event.documentId, event.revision, err);
        // Leave unresolved so the batch can retry; rethrow to abort batch commit.
        throw err;
      }

      resolveOffset(message.offset);
      await heartbeat();
    }
  }

  private async persist(event: DocMutationEvent): Promise<void> {
    const existing = await getDocumentMetadata(event.documentId);
    if (!existing) {
      // Auto-create metadata row if the doc was created via live OT before REST.
      await insertDocumentMetadata({
        documentId: event.documentId,
        title: 'Untitled',
        createdBy: event.userId,
        createdAt: new Date(event.timestamp),
        updatedAt: new Date(event.timestamp),
        currentRevision: 0,
        collaboratorCount: 1,
      });
    }

    await upsertRevision({
      documentId: event.documentId,
      revision: event.revision,
      steps: event.steps,
      clientId: event.clientId,
      userId: event.userId,
      timestamp: new Date(event.timestamp),
    });

    if (event.revision > 0 && event.revision % SNAPSHOT_EVERY === 0) {
      await saveSnapshot({
        documentId: event.documentId,
        revision: event.revision,
        content: { note: 'revision-checkpoint', revision: event.revision, steps: event.steps },
      });
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.consumer) {
      await this.consumer.disconnect();
      this.consumer = null;
    }
    await disconnectCassandra();
  }
}

/** CLI entry when run as a standalone process. */
export async function runMetadataConsumer(): Promise<MetadataConsumer> {
  const consumer = new MetadataConsumer();
  await consumer.start();
  return consumer;
}

const isDirectRun =
  process.argv[1]?.endsWith('metadataConsumer.ts') ||
  process.argv[1]?.endsWith('metadataConsumer.js');

if (isDirectRun) {
  const consumer = await runMetadataConsumer();
  const shutdown = async (sig: string) => {
    console.log(`[consumer] shutting down on ${sig}`);
    await consumer.stop();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}
