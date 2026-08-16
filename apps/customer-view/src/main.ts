import { createBroker, type IMessageBroker } from '@nodejs-kafka/broker';
import {
  CUSTOMER_TOPIC,
  applyOrder,
  applyPayment,
  createSampleOrder,
  createSamplePayment,
  toCustomerUpdated,
  type CustomerUpdated,
} from '@nodejs-kafka/domain';
import {
  buildBrokerConfig,
  createLogger,
  loadConfig,
  registerGracefulShutdown,
  type AppLogger,
} from '@nodejs-kafka/infra';
import { createCustomerViewServer } from './app.js';
import { CustomerStore } from './customer-store.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config.logLevel);

  const port = Number(process.env.CUSTOMER_VIEW_PORT ?? '3001');

  const broker = createBroker(
    buildBrokerConfig(config, { clientIdSuffix: '-customer-view', logger }),
  );

  await broker.connect();
  await broker.createTopics([
    {
      name: CUSTOMER_TOPIC,
      numPartitions: 3,
      configEntries: { 'cleanup.policy': 'compact' },
    },
  ]);

  const store = new CustomerStore();

  await broker.consume<CustomerUpdated | null>(
    [CUSTOMER_TOPIC],
    async (message) => {
      store.apply(message);
      logger.debug(
        { customerId: message.key, value: message.value },
        'customer view applied changelog record',
      );
    },
    { groupId: 'customer-view', fromBeginning: true, manualCommit: false },
  );

  // In-memory driver is single-process by design: the view self-generates a
  // sample changelog so the KTable read model is visible end to end. With
  // BROKER_DRIVER=confluent, records come from the standalone producer/web
  // service via the compaction topic instead.
  if (config.driver === 'in-memory') {
    await produceDemoWorkload(broker, logger);
  }

  const server = createCustomerViewServer({ broker, logger, store });
  const { close } = await server.start(port);
  logger.info({ port }, 'customer view listening');

  registerGracefulShutdown(
    [
      { name: 'broker', shutdown: () => broker.disconnect() },
      { name: 'customer-view-server', shutdown: () => close() },
    ],
    { logger },
  );
}

async function produceDemoWorkload(
  broker: IMessageBroker,
  logger: AppLogger,
): Promise<void> {
  logger.info('in-memory demo mode: publishing sample customer changelog');

  for (let seq = 1; seq <= 3; seq++) {
    const order = createSampleOrder(seq);
    const state = applyPayment(
      applyOrder(undefined, order),
      createSamplePayment(order),
    );
    const event = toCustomerUpdated(state);
    await broker.produce(CUSTOMER_TOPIC, event, { key: event.customerId });
  }

  // Tombstone: evicts CUST-1002 (the seq-1 customer) from the compacted view.
  await broker.produce<string | null>(CUSTOMER_TOPIC, null, { key: 'CUST-1002' });
}

void main().catch((error) => {
  const logger = createLogger('error');
  logger.error({ err: error }, 'customer view failed');
  process.exit(1);
});
