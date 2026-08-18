import { parseArgs } from 'node:util';
import { createBroker } from '@nodejs-kafka/broker';
import {
  CUSTOMER_TOPIC,
  TOPIC_ORDER_CREATED,
  TOPIC_PAYMENT_COMPLETED,
  applyOrder,
  applyPayment,
  createSampleOrder,
  createSamplePayment,
  toCustomerUpdated,
  type CustomerState,
} from '@nodejs-kafka/domain';
import {
  buildBrokerConfig,
  createLogger,
  createMetrics,
  initTracing,
  loadConfig,
  registerGracefulShutdown,
  startMetricsServer,
  TypedPublisher,
  type AppLogger,
} from '@nodejs-kafka/infra';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config.logLevel);
  logger.info({ driver: config.driver }, 'producer starting');

  const tracing = initTracing({
    endpoint: config.otelEndpoint,
    serviceName: config.otelServiceName || 'producer',
    logger,
  });
  const metrics = createMetrics();
  let metricsServer: Awaited<ReturnType<typeof startMetricsServer>> | undefined;
  if (config.metricsPort) {
    metricsServer = await startMetricsServer(
      Number(config.metricsPort),
      metrics.registry,
      config.httpBasicAuth,
    );
    logger.info({ port: metricsServer.port }, 'prometheus metrics server started');
  }

  const { values } = parseArgs({
    options: {
      count: { type: 'string', default: '10' },
      delay: { type: 'string', default: '500' },
    },
  });

  const count = Math.max(1, Number.parseInt(values.count ?? '10', 10));
  const delayMs = Math.max(0, Number.parseInt(values.delay ?? '500', 10));

  const broker = createBroker(buildBrokerConfig(config, { logger }));

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
    { name: CUSTOMER_TOPIC, numPartitions: 3, configEntries: { 'cleanup.policy': 'compact' } },
  ]);

  logger.info({ count, delayMs }, 'producing events');

  const customers = new Map<string, CustomerState>();

  for (let i = 1; i <= count; i++) {
    const order = createSampleOrder(i);
    const key = order.orderId;

    const orderResult = await publisher.publishOrder(order, { key });
    metrics.messagesProduced.labels({ topic: TOPIC_ORDER_CREATED }).inc();
    logProduced(logger, 'orders.created', order.eventId, orderResult.partition, orderResult.offset);

    const payment = createSamplePayment(order);
    const paymentResult = await publisher.publish('payments.completed', payment, { key });
    metrics.messagesProduced.labels({ topic: TOPIC_PAYMENT_COMPLETED }).inc();
    logProduced(logger, 'payments.completed', payment.eventId, paymentResult.partition, paymentResult.offset);

    const customerState = customers.get(order.customerId);
    const nextState = applyPayment(applyOrder(customerState, order), payment);
    customers.set(order.customerId, nextState);
    const customerEvent = toCustomerUpdated(nextState);
    const customerResult = await publisher.publish(CUSTOMER_TOPIC, customerEvent, { key: customerEvent.customerId });
    metrics.messagesProduced.labels({ topic: CUSTOMER_TOPIC }).inc();
    logProduced(logger, 'customers', customerEvent.eventId, customerResult.partition, customerResult.offset);

    if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
  }

  logger.info('producer finished, disconnecting');
  await broker.disconnect();
  await tracing.shutdown();
  await metricsServer?.close();
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
