import { createBroker, type Disposer } from '@nodejs-kafka/broker';
import {
  DLQ_TOPIC,
  RETRY_TOPIC,
  TELEMETRY_TOPIC,
  TOPIC_ORDER_CREATED,
  TOPIC_PAYMENT_COMPLETED,
  createNotificationHandler,
  createSampleOrder,
  createSamplePayment,
  parseEvent,
} from '@nodejs-kafka/domain';
import {
  createLogger,
  createTelemetryClient,
  loadConfig,
  registerGracefulShutdown,
  DlqManager,
  TypedPublisher,
  type AppLogger,
} from '@nodejs-kafka/infra';
import { createHandlerRunner } from './handler-runner.js';

const DEMO_EVENTS = 5;

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config.logLevel);
  logger.info(
    { driver: config.driver, groupId: config.consumerGroupId },
    'consumer starting',
  );

  const broker = createBroker({
    driver: config.driver,
    connection: {
      brokers: config.brokers,
      clientId: `${config.clientId}-consumer`,
      sasl: config.sasl,
    },
    memoryAutoCommit: false,
    logger,
  });

  const dlq = new DlqManager(broker);
  const publisher = new TypedPublisher(broker);
  // Telemetry is a real-Kafka feature: the in-memory demo keeps working as
  // before but does not emit telemetry (see spec §3.3).
  const telemetry = createTelemetryClient(
    config.driver === 'confluent' ? broker : null,
    logger,
  );
  const notificationHandler = createNotificationHandler(logger);

  await broker.connect();
  await broker.createTopics([
    { name: TOPIC_ORDER_CREATED, numPartitions: 3 },
    { name: TOPIC_PAYMENT_COMPLETED, numPartitions: 3 },
    { name: DLQ_TOPIC, numPartitions: 3 },
    { name: RETRY_TOPIC, numPartitions: 3 },
    { name: TELEMETRY_TOPIC, numPartitions: 3 },
  ]);
  await dlq.ensureTopic();

  // Topic -> handler wiring. Each handler is wrapped with parse/retry/DLQ/commit.
  const runner = createHandlerRunner(
    broker,
    dlq,
    {
      parse: (value) => parseEvent('orders.created', value),
      handler: notificationHandler,
      attempts: 3,
      baseDelayMs: 50,
      telemetry,
      groupId: config.consumerGroupId,
    },
    logger,
  );

  const disposers: Disposer[] = [
    await broker.consume(
      [TOPIC_ORDER_CREATED],
      runner,
      {
        groupId: config.consumerGroupId,
        fromBeginning: config.consumerFromBeginning,
        manualCommit: true,
      },
    ),
    await broker.consume(
      [TOPIC_PAYMENT_COMPLETED],
      async (message, context) => {
        const payment = parseEvent('payments.completed', message.value);
        logger.info(
          {
            eventId: payment.eventId,
            orderId: payment.orderId,
            amountCents: payment.amountCents,
            method: payment.method,
          },
          'payment recorded',
        );
        await telemetry.emit({
          type: 'payment-recorded',
          topic: message.topic,
          eventId: payment.eventId,
          orderId: payment.orderId,
          partition: message.partition,
          offset: message.offset,
          message: `Payment ${payment.amountCents} cents (${payment.method}) recorded`,
          concept: 'consumer-group',
        });
        await context.commit();
      },
      {
        groupId: config.consumerGroupId,
        fromBeginning: config.consumerFromBeginning,
        manualCommit: true,
      },
    ),
  ];

  logger.info('consumers running - press Ctrl+C to stop');

  // In-memory driver is single-process by design: the consumer self-generates a
  // sample workload so the full pipeline (produce -> parse -> retry -> DLQ ->
  // commit) is visible end to end. With BROKER_DRIVER=confluent, events come
  // from the standalone producer service instead.
  if (config.driver === 'in-memory') {
    await produceDemoWorkload(publisher, logger);
  }

  registerGracefulShutdown(
    [
      {
        name: 'broker',
        shutdown: () => broker.disconnect(),
      },
      {
        name: 'consumer-disposers',
        shutdown: async () => {
          for (const dispose of disposers) await dispose();
        },
      },
    ],
    { logger },
  );

  // Keep the worker alive. The graceful shutdown handler exits after draining.
  setInterval(() => {}, 2 ** 31 - 1);
  await new Promise<void>(() => {});
}

async function produceDemoWorkload(
  publisher: TypedPublisher,
  logger: AppLogger,
): Promise<void> {
  logger.info(
    { count: DEMO_EVENTS },
    'in-memory demo mode: self-generating sample events',
  );

  for (let i = 1; i <= DEMO_EVENTS; i++) {
    const order = createSampleOrder(i);
    const key = order.orderId;
    await publisher.publishOrder(order, { key });
    await publisher.publish('payments.completed', createSamplePayment(order), { key });
  }
}

void main().catch((error) => {
  const logger = createLogger('error');
  logger.error({ err: error }, 'consumer failed');
  process.exit(1);
});
