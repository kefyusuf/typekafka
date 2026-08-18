import { createServer } from 'node:http';
import express from 'express';
import type { IMessageBroker } from '@nodejs-kafka/broker';
import { CUSTOMER_TOPIC } from '@nodejs-kafka/domain';
import {
  createBasicAuthMiddleware,
  metricsMiddleware,
  type AppLogger,
  type AppMetrics,
} from '@nodejs-kafka/infra';
import { type CustomerStore } from './customer-store.js';

export interface CustomerViewServerOptions {
  broker: IMessageBroker;
  logger: AppLogger;
  store: CustomerStore;
  registry?: AppMetrics['registry'];
  /** Demo-only basic-auth (`user:password`). Off unless provided. */
  httpBasicAuth?: string;
}

export interface CustomerViewServer {
  app: express.Express;
  start(port: number): Promise<{ port: number; close(): Promise<void> }>;
}

export function createCustomerViewServer(
  options: CustomerViewServerOptions,
): CustomerViewServer {
  const { broker, store, registry, httpBasicAuth } = options;

  const app = express();
  app.use(express.json());

  // Health probe stays open (no auth) so orchestrators can probe it.
  app.get('/health', (_req, res) => {
    res.json({ ok: true });
  });

  // Gate every other route with optional basic-auth. When httpBasicAuth is
  // unset this is a no-op passthrough, so demos run unchanged.
  app.use(createBasicAuthMiddleware(httpBasicAuth));

  if (registry) {
    app.get('/metrics', metricsMiddleware(registry));
  }

  app.get('/customers', (_req, res) => {
    res.json(store.list());
  });

  app.get('/customers/:id', (req, res) => {
    const customerId = req.params.id;
    const customer = customerId === undefined ? undefined : store.get(customerId);
    if (customer === undefined) {
      res.status(404).json({ error: 'Customer not found' });
      return;
    }
    res.json(customer);
  });

  app.delete('/customers/:id', async (req, res) => {
    const customerId = req.params.id;
    if (customerId === undefined) {
      res.status(404).json({ error: 'Customer not found' });
      return;
    }
    await broker.produce<string | null>(CUSTOMER_TOPIC, null, { key: customerId });
    store.delete(customerId);
    res.status(204).end();
  });

  const server = createServer(app);

  return {
    app,
    async start(port) {
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
          await new Promise<void>((r) => server.close(() => r()));
        },
      };
    },
  };
}
