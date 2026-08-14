import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import express from 'express';
import type { Disposer, IMessageBroker } from '@nodejs-kafka/broker';
import {
  TELEMETRY_TOPIC,
  TOPIC_ORDER_CREATED,
  TOPIC_PAYMENT_COMPLETED,
  TelemetryEventSchema,
  createSamplePayment,
  type OrderCreated,
} from '@nodejs-kafka/domain';
import {
  createTelemetryClient,
  type AppLogger,
  type TelemetryClient,
} from '@nodejs-kafka/infra';
import { SseHub } from './sse.js';

export interface WebServerOptions {
  broker: IMessageBroker;
  logger: AppLogger;
  groupId: string;
  staticDir?: string;
}

export interface WebServer {
  app: express.Express;
  start(port: number): Promise<{ port: number; close(): Promise<void> }>;
}

export function createWebServer(options: WebServerOptions): WebServer {
  const { broker, logger, groupId, staticDir } = options;
  const hub = new SseHub(logger);
  const telemetry: TelemetryClient = createTelemetryClient(broker, logger);

  const app = express();
  app.use(express.json());
  if (staticDir) {
    app.use(express.static(staticDir));
  }

  app.get('/api/health', (_req, res) => {
    res.json({ ok: true });
  });

  app.post('/api/orders', async (req, res) => {
    const body = (req.body ?? {}) as {
      sku?: string;
      quantity?: number;
      unitPriceCents?: number;
      customerId?: string;
    };
    const sku = typeof body.sku === 'string' && body.sku ? body.sku : 'TSHIRT-BLACK';
    const quantity = typeof body.quantity === 'number' ? body.quantity : 1;
    const unitPriceCents =
      typeof body.unitPriceCents === 'number' ? body.unitPriceCents : 2_000;
    const customerId =
      typeof body.customerId === 'string' && body.customerId
        ? body.customerId
        : `CUST-${1000 + Math.floor(Math.random() * 9000)}`;

    const order: OrderCreated = {
      type: 'order.created',
      eventId: randomUUID(),
      occurredAt: new Date().toISOString(),
      orderId: `ORD-${randomUUID().slice(0, 8).toUpperCase()}`,
      customerId,
      items: [{ sku, quantity, priceCents: unitPriceCents }],
      totalCents: quantity * unitPriceCents,
    };

    try {
      const orderResult = await broker.produce(TOPIC_ORDER_CREATED, order, {
        key: order.orderId,
      });
      await telemetry.emit({
        type: 'produced',
        topic: TOPIC_ORDER_CREATED,
        eventId: order.eventId,
        orderId: order.orderId,
        partition: orderResult.partition,
        offset: orderResult.offset,
        message: `Order of ${order.totalCents} cents published to ${TOPIC_ORDER_CREATED}`,
        concept: 'partition-key',
      });

      const payment = createSamplePayment(order);
      const paymentResult = await broker.produce(TOPIC_PAYMENT_COMPLETED, payment, {
        key: order.orderId,
      });
      await telemetry.emit({
        type: 'produced',
        topic: TOPIC_PAYMENT_COMPLETED,
        eventId: payment.eventId,
        orderId: order.orderId,
        partition: paymentResult.partition,
        offset: paymentResult.offset,
        message: `Payment of ${payment.amountCents} cents published to ${TOPIC_PAYMENT_COMPLETED}`,
        concept: 'partition-key',
      });

      res.status(201).json({
        orderId: order.orderId,
        totalCents: order.totalCents,
        oversized: order.totalCents > 100_000,
      });
    } catch (error) {
      logger.error({ err: error }, 'failed to publish order');
      res.status(502).json({ error: 'Failed to publish order to Kafka' });
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

  return {
    app,
    async start(port) {
      consumer = await broker.consume(
        [TELEMETRY_TOPIC],
        async (message) => {
          const parsed = TelemetryEventSchema.safeParse(message.value);
          if (parsed.success) hub.broadcast(parsed.data);
        },
        { groupId, fromBeginning: true, manualCommit: true },
      );

      await new Promise<void>((resolve, reject) => {
        server.once('error', reject);
        server.listen(port, resolve);
      });
      const address = server.address();
      const actual = typeof address === 'object' && address ? address.port : port;

      return {
        port: actual,
        async close() {
          if (consumer) await consumer();
          await new Promise<void>((r) => server.close(() => r()));
        },
      };
    },
  };
}
