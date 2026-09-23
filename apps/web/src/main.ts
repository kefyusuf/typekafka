import { existsSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';
import { createBroker } from '@typekafka/broker';
import {
  CUSTOMER_TOPIC,
  TELEMETRY_TOPIC,
  TOPIC_ORDER_CREATED,
  TOPIC_PAYMENT_COMPLETED,
} from '@typekafka/domain';
import {
  buildBrokerConfig,
  createLogger,
  createMetrics,
  createTelemetryClient,
  initTracing,
  loadConfig,
  OutboxRelay,
  OutboxStore,
  registerGracefulShutdown,
} from '@typekafka/infra';
import { OrderStore } from './order-store.js';
import { makeProducedHook } from './telemetry.js';
import { createWebServer } from './app.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config.logLevel);

  const tracing = initTracing({
    endpoint: config.otelEndpoint,
    serviceName: config.otelServiceName || 'web',
    logger,
  });
  const metrics = createMetrics();

  if (config.driver !== 'confluent') {
    throw new Error(
      'apps/web requires BROKER_DRIVER=confluent (real Kafka). Use docker compose up, or run the in-memory demo with npm run dev:consumer.',
    );
  }

  const broker = createBroker(
    buildBrokerConfig(config, { clientIdSuffix: '-web', logger }),
  );

  await broker.connect();
  await broker.createTopics([
    { name: TOPIC_ORDER_CREATED, numPartitions: 3 },
    { name: TOPIC_PAYMENT_COMPLETED, numPartitions: 3 },
    { name: TELEMETRY_TOPIC, numPartitions: 3 },
    { name: CUSTOMER_TOPIC, numPartitions: 3, configEntries: { 'cleanup.policy': 'compact' } },
  ]);

  const here = dirname(fileURLToPath(import.meta.url));
  const staticDir = resolve(here, 'client');

  const dbPath = process.env.OUTBOX_DB_PATH ?? 'data/outbox.db';
  mkdirSync(dirname(resolve(dbPath)), { recursive: true });
  const db = new DatabaseSync(dbPath);
  const orderStore = new OrderStore(db);
  const outboxStore = new OutboxStore(db);
  const telemetry = createTelemetryClient(broker, logger);
  const relay = new OutboxRelay({
    broker,
    store: outboxStore,
    logger,
    onPublished: makeProducedHook(telemetry, metrics),
  });

  const server = createWebServer({
    broker,
    logger,
    groupId: config.telemetryGroupId,
    staticDir: existsSync(staticDir) ? staticDir : undefined,
    orderStore,
    outboxStore,
    relay,
    registry: metrics.registry,
    httpBasicAuth: config.httpBasicAuth,
  });

  const { close } = await server.start(config.webPort);
  logger.info({ port: config.webPort }, 'web UI listening');

  registerGracefulShutdown(
    [
      { name: 'outbox-db', shutdown: () => db.close() },
      { name: 'broker', shutdown: () => broker.disconnect() },
      { name: 'web-server', shutdown: () => close() },
      { name: 'tracing', shutdown: () => tracing.shutdown() },
    ],
    { logger },
  );
}

void main().catch((error) => {
  const logger = createLogger('error');
  logger.error({ err: error }, 'web server failed');
  process.exit(1);
});
