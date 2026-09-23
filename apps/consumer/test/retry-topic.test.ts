import { describe, expect, it } from 'vitest';
import { createBroker, type KafkaMessage } from '@typekafka/broker';
import {
  DLQ_TOPIC,
  RETRY_TOPIC,
  TOPIC_ORDER_CREATED,
  TOPIC_PAYMENT_COMPLETED,
  createSampleOrder,
  createSamplePayment,
  parseEvent,
  type OrderCreated,
  type PaymentCompleted,
} from '@typekafka/domain';
import {
  createMetrics,
  DlqManager,
  RetryTopicScheduler,
  TypedPublisher,
  type AppLogger,
} from '@typekafka/infra';
import type { Counter, Histogram } from 'prom-client';
import { createHandlerRunner } from '../src/handler-runner.js';
import { createRetryTopicRunner } from '../src/retry-runner.js';

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

const RETRY_POLICY = { delaysMs: [50, 100], maxDeliveries: 2 };

async function createRetryHarness() {
  const broker = createBroker({
    driver: 'in-memory',
    connection: { brokers: ['in-memory://'], clientId: 'test' },
    memoryAutoCommit: false,
  });
  await broker.connect();
  await broker.createTopics([
    { name: TOPIC_ORDER_CREATED, numPartitions: 3 },
    { name: RETRY_TOPIC, numPartitions: 3 },
    { name: DLQ_TOPIC, numPartitions: 3 },
  ]);

  const dlq = new DlqManager(broker);
  await dlq.ensureTopic();
  const scheduler = new RetryTopicScheduler(broker, { policy: RETRY_POLICY });
  await scheduler.ensureTopic();

  return { broker, dlq, scheduler };
}

function retryMessage(overrides?: Partial<KafkaMessage>): KafkaMessage<unknown> {
  return {
    topic: RETRY_TOPIC,
    key: 'ORD-00001',
    value: createSampleOrder(1),
    headers: {
      'retry-count': '1',
      'next-deliver-at': new Date(Date.now() + 5).toISOString(),
    },
    partition: 0,
    offset: '0',
    timestamp: new Date().toISOString(),
    ...overrides,
  };
}

describe('consumer retry topic pipeline (delayed-requeue -> parse -> retry -> schedule/DLQ -> commit)', () => {
  it('processes a due retry message', async () => {
    const { broker, dlq, scheduler } = await createRetryHarness();

    const handled: string[] = [];
    const deadLettered: KafkaMessage[] = [];

    const runner = createRetryTopicRunner(
      scheduler,
      dlq,
      {
        parse: (value) => parseEvent('orders.created', value),
        handler: async (order: OrderCreated) => {
          handled.push(order.orderId);
        },
        attempts: 2,
        baseDelayMs: 1,
      },
      noopLogger,
    );
    await broker.consume([RETRY_TOPIC], runner, { manualCommit: true });
    await broker.consume([DLQ_TOPIC], (message, ctx) => {
      deadLettered.push(message);
      return ctx.commit();
    });

    const msg = retryMessage({
      headers: { 'retry-count': '1', 'next-deliver-at': new Date(Date.now() - 5).toISOString() },
    });
    await broker.produce(RETRY_TOPIC, msg.value, { key: msg.key, headers: msg.headers });
    await sleep(30);

    expect(handled).toEqual(['ORD-00001']);
    expect(deadLettered).toHaveLength(0);

    await broker.disconnect();
  });

  it('re-queues a not-yet-due retry message and processes it once due', async () => {
    const { broker, dlq, scheduler } = await createRetryHarness();

    const handled: string[] = [];
    const requeued: KafkaMessage[] = [];

    const runner = createRetryTopicRunner(
      scheduler,
      dlq,
      {
        parse: (value) => parseEvent('orders.created', value),
        handler: async (order: OrderCreated) => {
          handled.push(order.orderId);
        },
        attempts: 2,
        baseDelayMs: 1,
      },
      noopLogger,
    );
    await broker.consume([RETRY_TOPIC], runner, { manualCommit: true });
    // A second group observes the re-queued copies (the runner commits and the
    // partition is freed instead of being blocked by an in-handler sleep).
    await broker.consume(
      [RETRY_TOPIC],
      (message, ctx) => {
        requeued.push(message);
        return ctx.commit();
      },
      { fromBeginning: false },
    );

    const dueAt = new Date(Date.now() + 80).toISOString();
    const msg = retryMessage({
      headers: { 'retry-count': '1', 'next-deliver-at': dueAt },
    });
    await broker.produce(RETRY_TOPIC, msg.value, { key: msg.key, headers: msg.headers });
    await sleep(40);

    // Not processed before its scheduled time, and it has already been
    // re-queued (throttled) rather than holding the partition with a long sleep.
    expect(handled).toEqual([]);
    expect(requeued.length).toBeGreaterThan(0);
    expect(requeued.some((m) => m.headers?.['retry-count'] === '1')).toBe(true);

    await sleep(120);
    expect(handled).toEqual(['ORD-00001']);

    await broker.disconnect();
  });

  it('re-schedules an exhausted retry message still under max deliveries', async () => {
    const { broker, dlq, scheduler } = await createRetryHarness();

    const runner = createRetryTopicRunner(
      scheduler,
      dlq,
      {
        parse: (value) => parseEvent('orders.created', value),
        handler: async () => {
          throw new Error('provider timed out');
        },
        attempts: 2,
        baseDelayMs: 1,
      },
      noopLogger,
    );
    await broker.consume([RETRY_TOPIC], runner, { manualCommit: true });

    const rescheduled: KafkaMessage[] = [];
    await broker.consume([RETRY_TOPIC], (message, ctx) => {
      rescheduled.push(message);
      return ctx.commit();
    }, { fromBeginning: false });

    const msg = retryMessage({
      headers: { 'retry-count': '1', 'next-deliver-at': new Date(Date.now() - 5).toISOString() },
    });
    await broker.produce(RETRY_TOPIC, msg.value, { key: msg.key, headers: msg.headers });
    await sleep(30);

    expect(rescheduled.some((m) => m.headers?.['retry-count'] === '2')).toBe(true);

    await broker.disconnect();
  });

  it('dead-letters a retry message at max deliveries', async () => {
    const { broker, dlq, scheduler } = await createRetryHarness();

    const deadLettered: KafkaMessage[] = [];

    const runner = createRetryTopicRunner(
      scheduler,
      dlq,
      {
        parse: (value) => parseEvent('orders.created', value),
        handler: async () => {
          throw new Error('provider timed out');
        },
        attempts: 2,
        baseDelayMs: 1,
      },
      noopLogger,
    );
    await broker.consume([RETRY_TOPIC], runner, { manualCommit: true });
    await broker.consume([DLQ_TOPIC], (message, ctx) => {
      deadLettered.push(message);
      return ctx.commit();
    });

    const msg = retryMessage({
      headers: { 'retry-count': '2', 'next-deliver-at': new Date(Date.now() - 5).toISOString() },
    });
    await broker.produce(RETRY_TOPIC, msg.value, { key: msg.key, headers: msg.headers });
    await sleep(30);

    expect(deadLettered).toHaveLength(1);
    expect(deadLettered[0]!.value).toMatchObject({ attempts: 2 });

    await broker.disconnect();
  });

  it('records retry runner metrics when metrics are provided', async () => {
    const { broker, dlq, scheduler } = await createRetryHarness();

    const metrics = createMetrics();

    const runner = createRetryTopicRunner(
      scheduler,
      dlq,
      {
        parse: (value) => parseEvent('orders.created', value),
        handler: async () => {
          throw new Error('provider timed out');
        },
        attempts: 2,
        baseDelayMs: 1,
        metrics,
      },
      noopLogger,
    );
    await broker.consume([RETRY_TOPIC], runner, { manualCommit: true });

    const msg = retryMessage({
      headers: {
        'retry-count': '2',
        'next-deliver-at': new Date(Date.now() - 5).toISOString(),
      },
    });
    await broker.produce(RETRY_TOPIC, msg.value, { key: msg.key, headers: msg.headers });
    await sleep(30);

    // One delivery: parse succeeds, so the message is counted as consumed.
    expect(await counterValue(metrics.messagesConsumed, RETRY_TOPIC)).toBe(1);
    // The handler fails attempt 1, so one retry is recorded before the final attempt.
    expect(await counterValue(metrics.retriesTotal, RETRY_TOPIC)).toBe(1);
    // At max deliveries (retry-count 2) the message dead-letters.
    expect(await counterValue(metrics.dlqTotal, RETRY_TOPIC)).toBe(1);
    // The handler is invoked once per attempt (2).
    expect(await handlerObservations(metrics.handlerDurationMs, RETRY_TOPIC)).toBe(2);

    await broker.disconnect();
  });

  it('routes a parked payment to the payment handler via original-topic', async () => {
    const { broker, dlq, scheduler } = await createRetryHarness();

    const paymentsHandled: string[] = [];
    const ordersHandled: string[] = [];
    const deadLettered: KafkaMessage[] = [];

    // Topic-aware parse + handler, mirroring the wiring in apps/consumer.
    const runner = createRetryTopicRunner<OrderCreated | PaymentCompleted>(
      scheduler,
      dlq,
      {
        parse: (value, originalTopic) =>
          originalTopic === TOPIC_PAYMENT_COMPLETED
            ? parseEvent('payments.completed', value)
            : parseEvent('orders.created', value),
        handler: async (payload) => {
          if (payload.type === 'payment.completed') {
            paymentsHandled.push(payload.orderId);
            return;
          }
          ordersHandled.push(payload.orderId);
        },
        attempts: 2,
        baseDelayMs: 1,
      },
      noopLogger,
    );
    await broker.consume([RETRY_TOPIC], runner, { manualCommit: true });
    await broker.consume([DLQ_TOPIC], (message, ctx) => {
      deadLettered.push(message);
      return ctx.commit();
    });

    const samplePayment = createSamplePayment(createSampleOrder(2));
    const msg = retryMessage({
      key: samplePayment.orderId,
      value: samplePayment,
      headers: {
        'retry-count': '1',
        'next-deliver-at': new Date(Date.now() - 5).toISOString(),
        'retry.original-topic': TOPIC_PAYMENT_COMPLETED,
      },
    });
    await broker.produce(RETRY_TOPIC, msg.value, { key: msg.key, headers: msg.headers });
    await sleep(30);

    // The parked payment is parsed as a payment and handled as one — not
    // mis-parsed as an order (which would have landed it in the DLQ).
    expect(paymentsHandled).toEqual([samplePayment.orderId]);
    expect(ordersHandled).toEqual([]);
    expect(deadLettered).toHaveLength(0);

    await broker.disconnect();
  });

  it('source runner with retryScheduler schedules to the retry topic instead of DLQ', async () => {
    const { broker, dlq, scheduler } = await createRetryHarness();

    const runner = createHandlerRunner(
      broker,
      dlq,
      {
        parse: (value) => parseEvent('orders.created', value),
        handler: async () => {
          throw new Error('provider timed out');
        },
        attempts: 2,
        baseDelayMs: 1,
        retryScheduler: scheduler,
      },
      noopLogger,
    );
    await broker.consume([TOPIC_ORDER_CREATED], runner, { manualCommit: true });

    const scheduled: KafkaMessage[] = [];
    await broker.consume([RETRY_TOPIC], (message, ctx) => {
      scheduled.push(message);
      return ctx.commit();
    }, { fromBeginning: false });

    const deadLettered: KafkaMessage[] = [];
    await broker.consume([DLQ_TOPIC], (message, ctx) => {
      deadLettered.push(message);
      return ctx.commit();
    });

    const publisher = new TypedPublisher(broker);
    const oversized = createSampleOrder(3);
    await publisher.publishOrder(oversized, { key: oversized.orderId }); // oversized -> retry topic

    await sleep(50);

    expect(scheduled).toHaveLength(1);
    expect(scheduled[0]!.headers?.['retry-count']).toBe('1');
    expect(scheduled[0]!.value).toMatchObject({ orderId: oversized.orderId });
    expect(deadLettered).toHaveLength(0);

    await broker.disconnect();
  });
});
