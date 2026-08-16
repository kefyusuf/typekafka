import { existsSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';
import { createBroker } from '@nodejs-kafka/broker';
import {
  TELEMETRY_TOPIC,
  TOPIC_ORDER_CREATED,
  TOPIC_PAYMENT_COMPLETED,
} from '@nodejs-kafka/domain';
import {
  buildBrokerConfig,
  createLogger,
  createTelemetryClient,
  loadConfig,
  OutboxRelay,
  OutboxStore,
  registerGracefulShutdown,
} from '@nodejs-kafka/infra';
import { OrderStore } from './order-store.js';
import { makeProducedHook } from './telemetry.js';
import { createWebServer } from './app.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config.logLevel);

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
    onPublished: makeProducedHook(telemetry),
  });

  const server = createWebServer({
    broker,
    logger,
    groupId: config.telemetryGroupId,
    staticDir: existsSync(staticDir) ? staticDir : undefined,
    orderStore,
    outboxStore,
    relay,
  });

  const { close } = await server.start(config.webPort);
  logger.info({ port: config.webPort }, 'web UI listening');

  registerGracefulShutdown(
    [
      { name: 'outbox-db', shutdown: () => db.close() },
      { name: 'web-server', shutdown: () => close() },
      { name: 'broker', shutdown: () => broker.disconnect() },
    ],
    { logger },
  );
}

void main().catch((error) => {
  const logger = createLogger('error');
  logger.error({ err: error }, 'web server failed');
  process.exit(1);
});
