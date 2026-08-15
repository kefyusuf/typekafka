import { existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
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
  loadConfig,
  registerGracefulShutdown,
} from '@nodejs-kafka/infra';
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

  const server = createWebServer({
    broker,
    logger,
    groupId: config.telemetryGroupId,
    staticDir: existsSync(staticDir) ? staticDir : undefined,
  });

  const { close } = await server.start(config.webPort);
  logger.info({ port: config.webPort }, 'web UI listening');

  registerGracefulShutdown(
    [
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
