import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import express from 'express';
import type { Disposer, IMessageBroker } from '@nodejs-kafka/broker';
import {
  TELEMETRY_TOPIC,
  TelemetryEventSchema,
  createSamplePayment,
  OVERSIZED_THRESHOLD_CENTS,
  type OrderCreated,
} from '@nodejs-kafka/domain';
import {
  metricsMiddleware,
  type AppLogger,
  type AppMetrics,
  type OutboxRelay,
  type OutboxStore,
} from '@nodejs-kafka/infra';
import { type OrderStore } from './order-store.js';
import { SseHub } from './sse.js';

export interface WebServerOptions {
  broker: IMessageBroker;
  logger: AppLogger;
  groupId: string;
  staticDir?: string;
  orderStore: OrderStore;
  outboxStore: OutboxStore;
  relay: OutboxRelay;
  registry?: AppMetrics['registry'];
}

export interface WebServer {
  app: express.Express;
  start(port: number): Promise<{ port: number; close(): Promise<void> }>;
}

export function createWebServer(options: WebServerOptions): WebServer {
  const { broker, logger, groupId, staticDir, orderStore, outboxStore, relay, registry } =
    options;
  const hub = new SseHub();

  // Deterministic, process-local customer id generator (no Math.random per
  // request). Only used when the caller does not supply a customerId.
  let nextCustomerSeq = 1000;

  const app = express();
  app.use(express.json());
  if (staticDir) {
    app.use(express.static(staticDir));
  }

  app.get('/api/health', (_req, res) => {
    res.json({ ok: true });
  });

  if (registry) {
    app.get('/metrics', metricsMiddleware(registry));
  }

  app.post('/api/orders', async (req, res) => {
    const body = (req.body ?? {}) as {
      sku?: unknown;
      quantity?: unknown;
      unitPriceCents?: unknown;
      customerId?: unknown;
    };
    const isPositiveInt = (v: unknown): v is number =>
      typeof v === 'number' && Number.isInteger(v) && v > 0;

    // Reject input that would produce a schema-invalid OrderCreated
    // (quantity <= 0 / non-integers / missing numeric fields).
    if (!isPositiveInt(body.quantity) || !isPositiveInt(body.unitPriceCents)) {
      res.status(400).json({
        error: 'quantity and unitPriceCents must be positive integers',
      });
      return;
    }

    const sku = typeof body.sku === 'string' && body.sku ? body.sku : 'TSHIRT-BLACK';
    const quantity = body.quantity;
    const unitPriceCents = body.unitPriceCents;
    const customerId =
      typeof body.customerId === 'string' && body.customerId
        ? body.customerId
        : `CUST-${nextCustomerSeq++}`;

    const order: OrderCreated = {
      type: 'order.created',
      eventId: randomUUID(),
      occurredAt: new Date().toISOString(),
      orderId: `ORD-${randomUUID().slice(0, 8).toUpperCase()}`,
      customerId,
      items: [{ sku, quantity, priceCents: unitPriceCents }],
      totalCents: quantity * unitPriceCents,
    };

    const payment = createSamplePayment(order);

    try {
      orderStore.createOrderWithOutbox(order, payment, outboxStore);
      res.status(201).json({
        orderId: order.orderId,
        totalCents: order.totalCents,
        oversized: order.totalCents > OVERSIZED_THRESHOLD_CENTS,
      });
    } catch (error) {
      logger.error({ err: error }, 'failed to persist order');
      res.status(500).json({ error: 'Failed to persist order' });
    }
  });

  app.get('/api/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
    res.write('retry: 2000\n\n');
    hub.add(res);
    req.on('close', () => hub.remove(res));
  });

  const server = createServer(app);
  let consumer: Disposer | null = null;
  let relayDisposer: Disposer | null = null;

  return {
    app,
    async start(port) {
      consumer = await broker.consume(
        [TELEMETRY_TOPIC],
        async (message) => {
          const parsed = TelemetryEventSchema.safeParse(message.value);
          if (parsed.success) hub.broadcast(parsed.data);
        },
        { groupId, fromBeginning: true, manualCommit: false },
      );

      relayDisposer = await relay.start();

      await new Promise<void>((resolve, reject) => {
        const onError = (err: Error) => reject(err);
        server.once('error', onError);
        server.listen(port, () => {
          server.removeListener('error', onError);
          resolve();
        });
      });
      const address = server.address();
      const actual = typeof address === 'object' && address ? address.port : port;

      return {
        port: actual,
        async close() {
          if (relayDisposer) await relayDisposer().catch(() => {});
          if (consumer) await consumer();
          hub.close();
          await new Promise<void>((r) => server.close(() => r()));
        },
      };
    },
  };
}
