import { afterEach, describe, expect, it } from 'vitest';
import { createBroker, type IMessageBroker } from '@nodejs-kafka/broker';
import { createLogger } from '@nodejs-kafka/infra';
import { createWebServer, type WebServer } from '../src/app.js';

const logger = createLogger('silent');

interface RunningServer {
  ws: WebServer;
  port: number;
  broker: IMessageBroker;
  close: () => Promise<void>;
}

const setup = async (): Promise<RunningServer> => {
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

  const ws = createWebServer({ broker, logger, groupId: 'web-telemetry-test' });
  const { port, close } = await ws.start(0);
  return { ws, port, broker, close };
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

  it('returns 502 when the broker is disconnected', async () => {
    const s = await setup();
    running.push(s);
    await s.broker.disconnect();

    const res = await fetch(`http://127.0.0.1:${s.port}/api/orders`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ sku: 'MUG-WHITE', quantity: 1, unitPriceCents: 100 }),
    });
    expect(res.status).toBe(502);
  });
});
