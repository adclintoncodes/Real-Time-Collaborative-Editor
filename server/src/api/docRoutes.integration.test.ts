/**
 * End-to-end integration test against running Docker Kafka + Cassandra.
 * Skips automatically when infrastructure is unreachable.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { Kafka, logLevel, CompressionTypes } from 'kafkajs';
import { types } from 'cassandra-driver';
import {
  connectCassandra,
  disconnectCassandra,
  getDocumentMetadata,
  insertDocumentMetadata,
  listRevisions,
  upsertRevision,
} from '../db/cassandra.js';
import { DOC_MUTATIONS_TOPIC } from '../kafka/producer.js';
import type { DocMutationEvent } from '../types.js';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

function makeKafka(clientId: string): Kafka {
  return new Kafka({
    clientId,
    brokers,
    logLevel: logLevel.ERROR,
    connectionTimeout: 5_000,
    requestTimeout: 10_000,
    retry: { retries: 5, initialRetryTime: 300 },
  });
}

async function cassandraReachable(): Promise<boolean> {
  try {
    await connectCassandra();
    return true;
  } catch {
    return false;
  }
}

async function kafkaReachable(): Promise<boolean> {
  const kafka = makeKafka('integration-probe');
  const admin = kafka.admin();
  try {
    await admin.connect();
    await admin.listTopics();
    await admin.disconnect();
    return true;
  } catch {
    try {
      await admin.disconnect();
    } catch {
      /* ignore */
    }
    return false;
  }
}

describe('Cassandra + Kafka integration', () => {
  let ready = false;

  before(async () => {
    const [c, k] = await Promise.all([cassandraReachable(), kafkaReachable()]);
    ready = c && k;
    if (!ready) {
      console.log('SKIP: Docker Kafka/Cassandra not reachable');
      return;
    }

    const admin = makeKafka('integration-admin').admin();
    await admin.connect();
    const topics = await admin.listTopics();
    if (!topics.includes(DOC_MUTATIONS_TOPIC)) {
      await admin.createTopics({
        topics: [{ topic: DOC_MUTATIONS_TOPIC, numPartitions: 1, replicationFactor: 1 }],
        waitForLeaders: true,
      });
    }
    await admin.disconnect();
  });

  after(async () => {
    await disconnectCassandra();
  });

  it('persists document metadata and revisions into Cassandra', async (t) => {
    if (!ready) {
      t.skip('infrastructure unavailable');
      return;
    }

    const documentId = types.Uuid.random().toString();
    const now = new Date();

    await insertDocumentMetadata({
      documentId,
      title: 'Integration Doc',
      createdBy: 'tester',
      createdAt: now,
      updatedAt: now,
      currentRevision: 0,
      collaboratorCount: 1,
    });

    await upsertRevision({
      documentId,
      revision: 1,
      steps: [{ stepType: 'replace', from: 1, to: 1, insertLength: 1, sliceSize: 1 }],
      clientId: 'c-int',
      userId: 'u-int',
      timestamp: new Date(),
    });

    const meta = await getDocumentMetadata(documentId);
    assert.ok(meta);
    assert.equal(meta?.title, 'Integration Doc');
    assert.equal(meta?.currentRevision, 1);

    const revs = await listRevisions(documentId, 0);
    assert.equal(revs.length, 1);
    assert.equal(revs[0]?.revision, 1);
  });

  it('publishes a mutation event to Kafka doc-mutations topic', async (t) => {
    if (!ready) {
      t.skip('infrastructure unavailable');
      return;
    }

    const documentId = types.Uuid.random().toString();
    const event: DocMutationEvent = {
      documentId,
      revision: 1,
      clientId: 'c-kafka',
      userId: 'u-kafka',
      steps: [{ stepType: 'replace', from: 0, to: 0, insertLength: 2, sliceSize: 2 }],
      timestamp: new Date().toISOString(),
    };

    const kafka = makeKafka(`integration-roundtrip-${Date.now()}`);
    const producer = kafka.producer();
    const consumer = kafka.consumer({ groupId: `int-test-${Date.now()}` });

    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: DOC_MUTATIONS_TOPIC, fromBeginning: true });

    const seen = new Promise<DocMutationEvent>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timeout waiting for kafka message')), 20_000);
      void consumer
        .run({
          eachMessage: async ({ message }) => {
            if (!message.value) return;
            const parsed = JSON.parse(message.value.toString()) as DocMutationEvent;
            if (parsed.documentId === documentId) {
              clearTimeout(timer);
              resolve(parsed);
            }
          },
        })
        .catch(reject);
    });

    // Allow group join before producing
    await new Promise((r) => setTimeout(r, 2500));

    await producer.send({
      topic: DOC_MUTATIONS_TOPIC,
      compression: CompressionTypes.None,
      messages: [{ key: documentId, value: JSON.stringify(event) }],
    });

    const received = await seen;
    assert.equal(received.revision, 1);
    assert.equal(received.clientId, 'c-kafka');
    assert.equal(received.documentId, documentId);

    await producer.disconnect();
    await consumer.disconnect();
  });
});
