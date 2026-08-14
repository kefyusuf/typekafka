import { describe, expect, it } from 'vitest';
import { createBroker, type KafkaMessage } from '@nodejs-kafka/broker';
import {
  DLQ_TOPIC,
  TOPIC_ORDER_CREATED,
  createSampleOrder,
  parseEvent,
  type OrderCreated,
} from '@nodejs-kafka/domain';
import { DlqManager, TypedPublisher, type AppLogger } from '@nodejs-kafka/infra';
import { createHandlerRunner } from '../src/handler-runner.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

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
});
