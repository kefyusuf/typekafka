import { afterEach, describe, expect, it } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { createBroker, type IMessageBroker } from '@nodejs-kafka/broker';
import {
  createLogger,
  createMetrics,
  createTelemetryClient,
  OutboxRelay,
  OutboxStore,
} from '@nodejs-kafka/infra';
import type { Registry } from 'prom-client';
import { createWebServer, type WebServer } from '../src/app.js';
import { OrderStore } from '../src/order-store.js';
import { makeProducedHook } from '../src/telemetry.js';

const logger = createLogger('silent');

interface RunningServer {
  ws: WebServer;
  port: number;
  broker: IMessageBroker;
  outboxStore: OutboxStore;
  close: () => Promise<void>;
}

const setup = async (registry?: Registry): Promise<RunningServer> => {
  const broker = createBroker({
    driver: 'in-memory',
    connection: { brokers: ['in-memory://'], clientId: 'web-test' },
    memoryAutoCommit: false,
  });
  await broker.connect();
  await broker.createTopics([
    { name: 'orders.created', numPartitions: 3 },
    { name: 'payments.completed', numPartitions: 3 },
    { name: 'telemetry.events', numPartitions: 3 },
  ]);

  const db = new DatabaseSync(':memory:');
  const orderStore = new OrderStore(db);
  const outboxStore = new OutboxStore(db);
  const relay = new OutboxRelay({
    broker,
    store: outboxStore,
    logger,
    transactional: false,
    onPublished: makeProducedHook(createTelemetryClient(broker, logger)),
  });
  const ws = createWebServer({
    broker,
    logger,
    groupId: 'web-telemetry-test',
    orderStore,
    outboxStore,
    relay,
    registry,
  });
  const { port, close } = await ws.start(0);
  return { ws, port, broker, outboxStore, close };
};

const running: RunningServer[] = [];

afterEach(async () => {
  for (const r of running.splice(0)) {
    await r.close();
    await r.broker.disconnect();
  }
});

describe('web server', () => {
  it('POST /api/orders publishes an order and returns 201', async () => {
    const s = await setup();
    running.push(s);

    const res = await fetch(`http://127.0.0.1:${s.port}/api/orders`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ sku: 'MUG-WHITE', quantity: 2, unitPriceCents: 1500 }),
    });

    expect(res.status).toBe(201);
    const body = (await res.json()) as { orderId: string; totalCents: number; oversized: boolean };
    expect(body.orderId).toMatch(/^ORD-/);
    expect(body.totalCents).toBe(3000);
    expect(body.oversized).toBe(false);

    const topics = await s.broker.listTopics();
    expect(topics).toContain('orders.created');
  });

  it('GET /api/events streams telemetry frames over SSE', async () => {
    const s = await setup();
    running.push(s);

    const sse = await fetch(`http://127.0.0.1:${s.port}/api/events`);
    expect(sse.headers.get('content-type')).toContain('text/event-stream');

    const reader = sse.body!.getReader();
    const decoder = new TextDecoder();

    await fetch(`http://127.0.0.1:${s.port}/api/orders`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ sku: 'HOODIE-GREY', quantity: 1, unitPriceCents: 5000 }),
    });

    let buffer = '';
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      if (buffer.includes('"type":"produced"')) break;
    }
    await reader.cancel();

    expect(buffer).toContain('"type":"produced"');
    expect(buffer).toContain('"topic":"orders.created"');
  });

  it('persists the order locally when the broker is disconnected', async () => {
    const s = await setup();
    running.push(s);
    await s.broker.disconnect();

    const res = await fetch(`http://127.0.0.1:${s.port}/api/orders`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ sku: 'MUG-WHITE', quantity: 1, unitPriceCents: 100 }),
    });
    expect(res.status).toBe(201);
    expect(s.outboxStore.countPending()).toBe(2);
  });

  it('rejects invalid order input with 400 and produces nothing', async () => {
    const s = await setup();
    running.push(s);

    const received: unknown[] = [];
    const dispose = await s.broker.consume(
      ['orders.created'],
      (message) => {
        received.push(message.value);
      },
      { groupId: 'invalid-input-check' },
    );
    try {
      const res = await fetch(`http://127.0.0.1:${s.port}/api/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ sku: 'MUG-WHITE', quantity: 0, unitPriceCents: 1500 }),
      });
      expect(res.status).toBe(400);
      const body = (await res.json()) as { error: string };
      expect(body.error).toMatch(/quantity|unitPriceCents/);
    } finally {
      await dispose();
    }

    expect(received).toHaveLength(0);
    expect(s.outboxStore.countPending()).toBe(0);
  });

  it('GET /metrics returns prometheus text when a registry is provided', async () => {
    const s = await setup(createMetrics().registry);
    running.push(s);

    const res = await fetch(`http://127.0.0.1:${s.port}/metrics`);
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toContain('text/plain');
    expect(await res.text()).toContain('nodejs_kafka_messages_produced_total');
  });

  it('GET /metrics is not mounted when no registry is provided', async () => {
    const s = await setup();
    running.push(s);

    const res = await fetch(`http://127.0.0.1:${s.port}/metrics`);
    expect(res.status).toBe(404);
  });
});

describe('makeProducedHook', () => {
  it('increments outboxPublishedTotal for the published topic when metrics are passed', async () => {
    const metrics = createMetrics();
    const hook = makeProducedHook(createTelemetryClient(null, logger), metrics);

    await hook({
      row: {
        id: 1,
        topic: 'orders.created',
        payload: { eventId: 'evt-1', orderId: 'ORD-1' },
        key: 'ORD-1',
        createdAt: new Date().toISOString(),
      },
      result: { topic: 'orders.created', partition: 0, offset: '0' },
    });

    const values = (await metrics.outboxPublishedTotal.get()).values;
    const topicValue = values.find((v) => v.labels['topic'] === 'orders.created');
    expect(topicValue?.value).toBe(1);
  });
});
