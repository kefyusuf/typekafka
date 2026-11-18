import type { RequestHandler } from 'express';
import { createServer } from 'node:http';
import { Counter, Histogram, Registry } from 'prom-client';
import { validateBasicCredentials } from './http-auth.js';
import { applySecurityHeaders } from './http-headers.js';

const LABEL_NAMES = ['topic'] as const;
const HANDLER_DURATION_BUCKETS = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1];

export interface AppMetrics {
  registry: Registry;
  messagesProduced: Counter<string>;
  messagesConsumed: Counter<string>;
  handlerDurationSeconds: Histogram<string>;
  retriesTotal: Counter<string>;
  dlqTotal: Counter<string>;
  idempotencySkipped: Counter<string>;
  outboxPublishedTotal: Counter<string>;
}

export function createMetrics(): AppMetrics {
  const registry = new Registry();

  const counter = (name: string, help: string) =>
    new Counter({ name, help, labelNames: LABEL_NAMES, registers: [registry] });

  return {
    registry,
    messagesProduced: counter(
      'nodejs_kafka_messages_produced_total',
      'Total number of messages produced',
    ),
    messagesConsumed: counter(
      'nodejs_kafka_messages_consumed_total',
      'Total number of messages consumed',
    ),
    handlerDurationSeconds: new Histogram({
      name: 'nodejs_kafka_handler_duration_seconds',
      help: 'Duration of message handler execution in seconds',
      labelNames: LABEL_NAMES,
      buckets: HANDLER_DURATION_BUCKETS,
      registers: [registry],
    }),
    retriesTotal: counter(
      'nodejs_kafka_retries_total',
      'Total number of message retries',
    ),
    dlqTotal: counter(
      'nodejs_kafka_dlq_total',
      'Total number of messages dead-lettered',
    ),
    idempotencySkipped: counter(
      'nodejs_kafka_idempotency_skipped_total',
      'Total number of duplicate event ids skipped by the idempotency filter',
    ),
    outboxPublishedTotal: counter(
      'nodejs_kafka_outbox_published_total',
      'Total number of outbox events published',
    ),
  };
}

export async function startMetricsServer(
  port: number,
  registry: Registry,
  credentials?: string,
): Promise<{ port: number; close(): Promise<void> }> {
  const server = createServer(async (req, res) => {
    applySecurityHeaders(res);
    const url = new URL(req.url ?? '/', 'http://localhost');
    if (url.pathname !== '/metrics') {
      res.statusCode = 404;
      res.setHeader('Content-Type', 'text/plain');
      res.end('Not Found');
      return;
    }

    if (credentials && !validateBasicCredentials(req.headers['authorization'], credentials)) {
      res.statusCode = 401;
      res.setHeader('WWW-Authenticate', 'Basic realm="typekafka"');
      res.setHeader('Content-Type', 'text/plain');
      res.end('Unauthorized');
      return;
    }

    try {
      const body = await registry.metrics();
      res.statusCode = 200;
      res.setHeader('Content-Type', registry.contentType);
      res.end(body);
    } catch (error) {
      res.statusCode = 500;
      res.setHeader('Content-Type', 'text/plain');
      res.end(String(error));
    }
  });

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, () => resolve());
  });

  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('metrics server failed to bind to a port');
  }

  return {
    port: address.port,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}

export function metricsMiddleware(registry: Registry): RequestHandler {
  return async (_req, res) => {
    try {
      const body = await registry.metrics();
      res.statusCode = 200;
      res.setHeader('Content-Type', registry.contentType);
      res.end(body);
    } catch (error) {
      res.statusCode = 500;
      res.setHeader('Content-Type', 'text/plain');
      res.end(String(error));
    }
  };
}
