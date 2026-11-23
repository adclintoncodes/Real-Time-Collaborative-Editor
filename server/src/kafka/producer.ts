/**
 * Asynchronous Kafka producer for document mutation events.
 * Never blocks the WebSocket hot path — failures are logged and dropped.
 */

import { Kafka, type Producer, logLevel } from 'kafkajs';
import type { DocMutationEvent } from '../types.js';

export const DOC_MUTATIONS_TOPIC = 'doc-mutations';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

let producer: Producer | null = null;
let connecting: Promise<Producer> | null = null;
let disabled = false;

async function getProducer(): Promise<Producer | null> {
  if (disabled) return null;
  if (producer) return producer;
  if (connecting) return connecting;

  connecting = (async () => {
    const kafka = new Kafka({
      clientId: 'collab-ot-gateway',
      brokers,
      logLevel: logLevel.ERROR,
      retry: { retries: 3, initialRetryTime: 300 },
    });
    const p = kafka.producer({
      allowAutoTopicCreation: true,
      idempotent: false,
    });
    await p.connect();
    producer = p;
    return p;
  })();

  try {
    return await connecting;
  } catch (err) {
    console.error('[kafka] producer connect failed; mutations will be dropped until reconnect', err);
    connecting = null;
    return null;
  } finally {
    connecting = null;
  }
}

/** Fire-and-forget publish of a mutation event. */
export function publishMutation(event: DocMutationEvent): void {
  void (async () => {
    try {
      const p = await getProducer();
      if (!p) return;
      await p.send({
        topic: DOC_MUTATIONS_TOPIC,
        messages: [
          {
            key: event.documentId,
            value: JSON.stringify(event),
            headers: {
              revision: String(event.revision),
              clientId: event.clientId,
            },
          },
        ],
      });
    } catch (err) {
      console.error('[kafka] publishMutation failed', err);
    }
  })();
}

export async function disconnectProducer(): Promise<void> {
  if (producer) {
    await producer.disconnect();
    producer = null;
  }
}

/** Test helper — force-disable Kafka (e.g. unit tests without broker). */
export function disableKafkaForTests(): void {
  disabled = true;
}

export function enableKafka(): void {
  disabled = false;
}
