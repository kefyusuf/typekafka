import { describe, expect, it } from 'vitest';
import { createBroker, type KafkaMessage } from '@nodejs-kafka/broker';
import {
  DLQ_TOPIC,
  TOPIC_ORDER_CREATED,
  createSampleOrder,
  parseEvent,
  type OrderCreated,
} from '@nodejs-kafka/domain';
import {
  createMetrics,
  DlqManager,
  TypedPublisher,
  type AppLogger,
} from '@nodejs-kafka/infra';
import type { Counter, Histogram } from 'prom-client';
import { createHandlerRunner } from '../src/handler-runner.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function counterValue(metric: Counter<string>, topic: string): Promise<number> {
  const values = (await metric.get()).values;
  return values.find((v) => v.labels['topic'] === topic)?.value ?? 0;
}

async function handlerObservations(
  metric: Histogram<string>,
  topic: string,
): Promise<number> {
  const values = (await metric.get()).values;
  return values.find((v) => v.labels['topic'] === topic && v.labels['le'] === '+Inf')
    ?.value ?? 0;
}

const noopLogger = {
  info: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
  trace: () => {},
  fatal: () => {},
  silent: () => {},
  child: () => noopLogger,
} as unknown as AppLogger;

describe('consumer pipeline (parse -> retry -> DLQ -> commit)', () => {
  it('delivers valid messages to the handler and dead-letters failing ones', async () => {
    const broker = createBroker({
      driver: 'in-memory',
      connection: { brokers: ['in-memory://'], clientId: 'test' },
      memoryAutoCommit: false,
    });
    await broker.connect();
    await broker.createTopics([
      { name: TOPIC_ORDER_CREATED, numPartitions: 3 },
      { name: DLQ_TOPIC, numPartitions: 3 },
    ]);

    const dlq = new DlqManager(broker);
    await dlq.ensureTopic();

    const handled: string[] = [];
    const deadLettered: KafkaMessage[] = [];

    const runner = createHandlerRunner(
      broker,
      dlq,
      {
        parse: (value) => parseEvent('orders.created', value),
        handler: async (order: OrderCreated) => {
          // Simulate a permanently failing side effect for oversized orders.
          if (order.totalCents > 100_000) {
            throw new Error('provider timed out');
          }
          handled.push(order.orderId);
        },
        attempts: 2,
        baseDelayMs: 1,
      },
      noopLogger,
    );

    await broker.consume([TOPIC_ORDER_CREATED], runner, { manualCommit: true });
    await broker.consume([DLQ_TOPIC], (message, ctx) => {
      deadLettered.push(message);
      return ctx.commit();
    });

    const publisher = new TypedPublisher(broker);
    const order1 = createSampleOrder(1);
    const order3 = createSampleOrder(3);
    await publisher.publishOrder(order1, { key: order1.orderId }); // normal -> handled
    await publisher.publishOrder(order3, { key: order3.orderId }); // oversized -> retry -> DLQ

    await sleep(50);

    expect(handled).toEqual(['ORD-00001']);
    expect(deadLettered).toHaveLength(1);

    const entry = deadLettered[0]!.value as { original: { orderId: string } };
    expect(entry.original.orderId).toBe('ORD-00003');
    expect(deadLettered[0]!.key).toBe('ORD-00003');

    await broker.disconnect();
  });

  it('sends invalid payloads straight to DLQ without invoking the handler', async () => {
    const broker = createBroker({
      driver: 'in-memory',
      connection: { brokers: ['in-memory://'], clientId: 'test' },
      memoryAutoCommit: false,
    });
    await broker.connect();
    await broker.createTopics([
      { name: TOPIC_ORDER_CREATED, numPartitions: 1 },
      { name: DLQ_TOPIC, numPartitions: 1 },
    ]);

    const dlq = new DlqManager(broker);
    await dlq.ensureTopic();

    const deadLettered: KafkaMessage[] = [];

    const runner = createHandlerRunner(
      broker,
      dlq,
      {
        parse: (value) => parseEvent('orders.created', value),
        handler: async () => {
          throw new Error('handler should never be called');
        },
      },
      noopLogger,
    );

    await broker.consume([TOPIC_ORDER_CREATED], runner, { manualCommit: true });
    await broker.consume([DLQ_TOPIC], (message, ctx) => {
      deadLettered.push(message);
      return ctx.commit();
    });

    await broker.produce(TOPIC_ORDER_CREATED, { type: 'order.created' }); // invalid
    await sleep(30);

    expect(deadLettered).toHaveLength(1);
    expect(deadLettered[0]!.value).toMatchObject({ errorType: 'ZodError' });

    await broker.disconnect();
  });

  it('emits telemetry events for each pipeline step in order', async () => {
    const broker = createBroker({
      driver: 'in-memory',
      connection: { brokers: ['in-memory://'], clientId: 'test' },
      memoryAutoCommit: false,
    });
    await broker.connect();
    await broker.createTopics([
      { name: TOPIC_ORDER_CREATED, numPartitions: 3 },
      { name: DLQ_TOPIC, numPartitions: 3 },
    ]);

    const dlq = new DlqManager(broker);
    await dlq.ensureTopic();

    const emitted: string[] = [];
    const telemetry = {
      enabled: true,
      emit: async (input: { type: string }) => {
        emitted.push(input.type);
      },
    } as unknown as { enabled: boolean; emit: (i: { type: string }) => Promise<void> };

    const runner = createHandlerRunner(
      broker,
      dlq,
      {
        parse: (value) => parseEvent('orders.created', value),
        handler: async (order: OrderCreated) => {
          if (order.totalCents > 100_000) {
            throw new Error('provider timed out');
          }
        },
        attempts: 3,
        baseDelayMs: 1,
        telemetry,
        groupId: 'notification-service',
      },
      noopLogger,
    );

    await broker.consume([TOPIC_ORDER_CREATED], runner, { manualCommit: true });

    const publisher = new TypedPublisher(broker);
    await publisher.publishOrder(createSampleOrder(3), { key: 'ORD-00003' }); // oversized -> retry -> DLQ

    await sleep(50);

    expect(emitted[0]).toBe('consumed');
    expect(emitted[1]).toBe('parsed');
    expect(emitted[2]).toBe('retrying');
    expect(emitted[3]).toBe('retrying');
    expect(emitted[4]).toBe('dead-lettered');
    expect(emitted[5]).toBe('committed');

    await broker.disconnect();
  });

  it('records consumer pipeline metrics when metrics are provided', async () => {
    const broker = createBroker({
      driver: 'in-memory',
      connection: { brokers: ['in-memory://'], clientId: 'test' },
      memoryAutoCommit: false,
    });
    await broker.connect();
    await broker.createTopics([
      { name: TOPIC_ORDER_CREATED, numPartitions: 3 },
      { name: DLQ_TOPIC, numPartitions: 3 },
    ]);

    const dlq = new DlqManager(broker);
    await dlq.ensureTopic();

    const metrics = createMetrics();
    const handled: string[] = [];

    const runner = createHandlerRunner(
      broker,
      dlq,
      {
        parse: (value) => parseEvent('orders.created', value),
        handler: async (order: OrderCreated) => {
          if (order.totalCents > 100_000) {
            throw new Error('provider timed out');
          }
          handled.push(order.orderId);
        },
        attempts: 2,
        baseDelayMs: 1,
        metrics,
      },
      noopLogger,
    );

    await broker.consume([TOPIC_ORDER_CREATED], runner, { manualCommit: true });

    const publisher = new TypedPublisher(broker);
    await publisher.publishOrder(createSampleOrder(1), { key: 'ORD-00001' }); // handled
    await publisher.publishOrder(createSampleOrder(3), { key: 'ORD-00003' }); // oversized -> retry -> DLQ
    await broker.produce(TOPIC_ORDER_CREATED, { type: 'order.created' }); // invalid -> DLQ

    await sleep(50);

    // order1 + order3 parse successfully; the invalid payload never reaches the handler.
    expect(await counterValue(metrics.messagesConsumed, TOPIC_ORDER_CREATED)).toBe(2);
    // order3 fails attempt 1, so one retry is recorded before its final attempt.
    expect(await counterValue(metrics.retriesTotal, TOPIC_ORDER_CREATED)).toBe(1);
    // order3 (retries exhausted) and the invalid payload both dead-letter.
    expect(await counterValue(metrics.dlqTotal, TOPIC_ORDER_CREATED)).toBe(2);
    // order1 handled once, order3 handler invoked once per attempt (2).
    expect(await handlerObservations(metrics.handlerDurationMs, TOPIC_ORDER_CREATED)).toBe(3);
    expect(handled).toEqual(['ORD-00001']);

    await broker.disconnect();
  });
});
