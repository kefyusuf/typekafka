import { parseArgs } from 'node:util';
import { createBroker } from '@nodejs-kafka/broker';
import {
  TOPIC_ORDER_CREATED,
  TOPIC_PAYMENT_COMPLETED,
  createSampleOrder,
  createSamplePayment,
} from '@nodejs-kafka/domain';
import {
  createLogger,
  loadConfig,
  registerGracefulShutdown,
  TypedPublisher,
  type AppLogger,
} from '@nodejs-kafka/infra';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config.logLevel);
  logger.info({ driver: config.driver }, 'producer starting');

  const { values } = parseArgs({
    options: {
      count: { type: 'string', default: '10' },
      delay: { type: 'string', default: '500' },
    },
  });

  const count = Math.max(1, Number.parseInt(values.count ?? '10', 10));
  const delayMs = Math.max(0, Number.parseInt(values.delay ?? '500', 10));

  const broker = createBroker({
    driver: config.driver,
    connection: {
      brokers: config.brokers,
      clientId: config.clientId,
      sasl: config.sasl,
    },
    memoryAutoCommit: config.memoryAutoCommit,
    logger,
  });

  registerGracefulShutdown(
    [
      {
        name: 'broker',
        shutdown: () => broker.disconnect(),
      },
    ],
    { logger },
  );

  const publisher = new TypedPublisher(broker);

  await broker.connect();
  await broker.createTopics([
    { name: TOPIC_ORDER_CREATED, numPartitions: 3 },
    { name: TOPIC_PAYMENT_COMPLETED, numPartitions: 3 },
  ]);

  logger.info({ count, delayMs }, 'producing events');

  for (let i = 1; i <= count; i++) {
    const order = createSampleOrder(i);
    const key = order.orderId;

    const orderResult = await publisher.publishOrder(order, { key });
    logProduced(logger, 'orders.created', order.eventId, orderResult.partition, orderResult.offset);

    const payment = createSamplePayment(order);
    const paymentResult = await publisher.publish('payments.completed', payment, { key });
    logProduced(logger, 'payments.completed', payment.eventId, paymentResult.partition, paymentResult.offset);

    if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
  }

  logger.info('producer finished, disconnecting');
  await broker.disconnect();
}

function logProduced(
  logger: AppLogger,
  topic: string,
  eventId: string,
  partition: number,
  offset: string,
): void {
  logger.info({ topic, eventId, partition, offset }, 'event produced');
}

main().catch((error) => {
  const logger = createLogger('error');
  logger.error({ err: error }, 'producer failed');
  process.exit(1);
});
