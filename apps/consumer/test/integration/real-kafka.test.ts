import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { createBroker } from '@nodejs-kafka/broker';
import type { IMessageBroker, KafkaMessage } from '@nodejs-kafka/broker';
import {
  DLQ_TOPIC,
  TOPIC_ORDER_CREATED,
  createSampleOrder,
  parseEvent,
  type OrderCreated,
} from '@nodejs-kafka/domain';
import { DlqManager, createIdempotencyFilter, type AppLogger } from '@nodejs-kafka/infra';
import { createHandlerRunner } from '../../src/handler-runner.js';

const execFileAsync = promisify(execFile);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function isDockerAvailable(): Promise<boolean> {
  try {
    await Promise.race([
      execFileAsync('docker', ['info'], { timeout: 8000 }),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error('timeout')), 8000)),
    ]);
    return true;
  } catch {
    return false;
  }
}

async function waitFor(
  predicate: () => boolean,
  timeoutMs = 30000,
  intervalMs = 200,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
    await sleep(intervalMs);
  }
  throw new Error('waitFor timed out waiting for condition');
}

const dockerAvailable = await isDockerAvailable();

const noopLogger = {
  info: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
} as unknown as AppLogger;

function makeBroker(bootstrapServers: string): IMessageBroker {
  return createBroker({
    driver: 'confluent',
    connection: { brokers: [bootstrapServers], clientId: 'integration-test' },
    autoConnect: true,
  });
}

async function ensureTopics(broker: IMessageBroker): Promise<void> {
  for (let attempt = 0; attempt < 15; attempt++) {
    try {
      await broker.createTopics([
        { name: TOPIC_ORDER_CREATED, numPartitions: 1 },
        { name: DLQ_TOPIC, numPartitions: 1 },
      ]);
      return;
    } catch {
      await sleep(2000);
    }
  }
  throw new Error('failed to create topics after retries');
}

describe.skipIf(!dockerAvailable)('real-Kafka integration suite (Docker-gated, confluent driver)', () => {
  let container: StartedKafkaContainer;
  let bootstrapServers: string;

  beforeAll(async () => {
    container = await new KafkaContainer('confluentinc/cp-kafka:7.6.0').start();
    bootstrapServers = `${container.getHost()}:${container.getMappedPort(9093)}`;
    const bootstrap = makeBroker(bootstrapServers);
    await bootstrap.connect();
    await ensureTopics(bootstrap);
    await bootstrap.disconnect();
  }, 240000);

  afterAll(async () => {
    await container?.stop().catch(() => {});
  });

  it('end-to-end happy path: consumes orders.created exactly once and idempotency suppresses a redelivered duplicate', async () => {
    const broker = makeBroker(bootstrapServers);
    await broker.connect();

    const idempotency = createIdempotencyFilter({ maxSize: 1000 });
    const handled: OrderCreated[] = [];

    try {
      const dlq = new DlqManager(broker);
      await dlq.ensureTopic();

      const runner = createHandlerRunner(
        broker,
        dlq,
        {
          parse: (value) => parseEvent('orders.created', value),
          handler: async (order: OrderCreated) => {
            handled.push(order);
          },
          attempts: 1,
          groupId: 'it-happy',
          idempotency,
        },
        noopLogger,
      );
      await broker.consume([TOPIC_ORDER_CREATED], runner, {
        manualCommit: true,
        groupId: 'it-happy',
        fromBeginning: false,
      });
      await sleep(800);

      const order = createSampleOrder(11);
      await broker.produce(TOPIC_ORDER_CREATED, order, { key: order.orderId });
      await waitFor(() => handled.length === 1);

      await broker.produce(TOPIC_ORDER_CREATED, order, { key: order.orderId });
      await sleep(2500);

      expect(handled).toHaveLength(1);
      expect(handled[0]?.orderId).toBe(order.orderId);
    } finally {
      await broker.disconnect();
    }
  }, 60000);

  it('DLQ path: a handler that always throws exhausts retries and lands on the DLQ topic', async () => {
    const broker = makeBroker(bootstrapServers);
    await broker.connect();

    const deadLettered: KafkaMessage[] = [];

    try {
      const dlq = new DlqManager(broker);
      await dlq.ensureTopic();

      const runner = createHandlerRunner(
        broker,
        dlq,
        {
          parse: (value) => parseEvent('orders.created', value),
          handler: async () => {
            throw new Error('downstream provider unavailable');
          },
          attempts: 1,
          groupId: 'it-dlq',
        },
        noopLogger,
      );
      await broker.consume([TOPIC_ORDER_CREATED], runner, {
        manualCommit: true,
        groupId: 'it-dlq',
        fromBeginning: false,
      });
      await broker.consume([DLQ_TOPIC], (message, ctx) => {
        deadLettered.push(message);
        return ctx.commit();
      }, { fromBeginning: false });
      await sleep(800);

      const order = createSampleOrder(22);
      await broker.produce(TOPIC_ORDER_CREATED, order, { key: order.orderId });
      await waitFor(() => deadLettered.length === 1);

      expect(deadLettered).toHaveLength(1);
      const entry = deadLettered[0]?.value as { attempts: number; original: unknown } | undefined;
      expect(entry?.attempts).toBe(1);
      expect(entry?.original).toMatchObject({ orderId: order.orderId });
    } finally {
      await broker.disconnect();
    }
  }, 60000);

  it('relay/consumer restart idempotency: already-processed events are not reprocessed after reconnect', async () => {
    const broker = makeBroker(bootstrapServers);
    await broker.connect();

    const idempotency = createIdempotencyFilter({ maxSize: 1000 });
    const handled: OrderCreated[] = [];

    try {
      const dlq = new DlqManager(broker);
      await dlq.ensureTopic();

      const orders = [createSampleOrder(31), createSampleOrder(32), createSampleOrder(33)];

      const runner = createHandlerRunner(
        broker,
        dlq,
        {
          parse: (value) => parseEvent('orders.created', value),
          handler: async (order: OrderCreated) => {
            handled.push(order);
          },
          attempts: 1,
          groupId: 'it-restart',
          idempotency,
        },
        noopLogger,
      );
      await broker.consume([TOPIC_ORDER_CREATED], runner, {
        manualCommit: true,
        groupId: 'it-restart',
        fromBeginning: false,
      });
      await sleep(800);

      for (const order of orders) {
        await broker.produce(TOPIC_ORDER_CREATED, order, { key: order.orderId });
      }
      await waitFor(() => handled.length === orders.length);
      await sleep(1000);

      const before = handled.length;

      await broker.disconnect();

      const broker2 = makeBroker(bootstrapServers);
      await broker2.connect();
      const dlq2 = new DlqManager(broker2);
      await dlq2.ensureTopic();
      const runner2 = createHandlerRunner(
        broker2,
        dlq2,
        {
          parse: (value) => parseEvent('orders.created', value),
          handler: async (order: OrderCreated) => {
            handled.push(order);
          },
          attempts: 1,
          groupId: 'it-restart',
          idempotency,
        },
        noopLogger,
      );
      await broker2.consume([TOPIC_ORDER_CREATED], runner2, {
        manualCommit: true,
        groupId: 'it-restart',
        fromBeginning: false,
      });
      await sleep(3000);

      expect(handled).toHaveLength(before);
      await broker2.disconnect();
    } finally {
      await broker.disconnect().catch(() => {});
    }
  }, 60000);
});
