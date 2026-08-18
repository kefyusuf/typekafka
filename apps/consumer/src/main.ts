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
  buildBrokerConfig,
  createLogger,
  createMetrics,
  createRetryTopicScheduler,
  createTelemetryClient,
  createIdempotencyFilter,
  initTracing,
  loadConfig,
  registerGracefulShutdown,
  startMetricsServer,
  DlqManager,
  TypedPublisher,
  type AppLogger,
  type RetryTopicScheduler,
} from '@nodejs-kafka/infra';
import { createHandlerRunner } from './handler-runner.js';
import { createRetryTopicRunner } from './retry-runner.js';

const DEMO_EVENTS = 5;

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config.logLevel);
  logger.info(
    { driver: config.driver, groupId: config.consumerGroupId },
    'consumer starting',
  );

  const tracing = initTracing({
    endpoint: config.otelEndpoint,
    serviceName: config.otelServiceName || 'consumer',
    logger,
  });
  const metrics = createMetrics();
  const idempotency = createIdempotencyFilter();
  let metricsServer: Awaited<ReturnType<typeof startMetricsServer>> | undefined;
  if (config.metricsPort) {
    metricsServer = await startMetricsServer(
      Number(config.metricsPort),
      metrics.registry,
      config.httpBasicAuth,
    );
    logger.info({ port: metricsServer.port }, 'prometheus metrics server started');
  }

  const broker = createBroker(
    buildBrokerConfig(config, {
      clientIdSuffix: '-consumer',
      memoryAutoCommit: false,
      logger,
    }),
  );

  const dlq = new DlqManager(broker);
  // Retry topic is a real-Kafka feature: with the in-memory driver the source
  // runner keeps in-process retry -> DLQ (see spec §3.3 and D11).
  const retryScheduler: RetryTopicScheduler | undefined =
    config.driver === 'confluent' ? createRetryTopicScheduler(broker) : undefined;
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
  // With the retry topic (confluent), the source runner uses 2 fast in-process
  // attempts then hops to orders.retry; otherwise it keeps 3 then DLQ.
  const runner = createHandlerRunner(
    broker,
    dlq,
    {
      parse: (value) => parseEvent('orders.created', value),
      handler: notificationHandler,
      attempts: config.driver === 'confluent' ? 2 : 3,
      baseDelayMs: 50,
      telemetry,
      metrics,
      groupId: config.consumerGroupId,
      idempotency,
      ...(retryScheduler ? { retryScheduler } : {}),
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

  // Retry-topic consumer: re-processes parked/scheduled orders.retry messages,
  // escalating delay until max deliveries, then DLQ. Confluent-only (D11).
  if (config.driver === 'confluent' && retryScheduler) {
    disposers.push(
      await broker.consume(
        [RETRY_TOPIC],
        createRetryTopicRunner(
          retryScheduler,
          dlq,
          {
            parse: (value) => parseEvent('orders.created', value),
            handler: notificationHandler,
            attempts: 2,
            baseDelayMs: 50,
            telemetry,
            metrics,
            groupId: config.consumerGroupId,
            idempotency,
          },
          logger,
        ),
        {
          groupId: config.consumerGroupId,
          fromBeginning: config.consumerFromBeginning,
          manualCommit: true,
        },
      ),
    );
  }

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
      {
        name: 'tracing',
        shutdown: () => tracing.shutdown(),
      },
      {
        name: 'metrics-server',
        shutdown: async () => {
          await metricsServer?.close();
        },
      },
    ],
    { logger },
  );

  // The running consumers hold the event loop open, and `registerGracefulShutdown`
  // keeps the process alive via its SIGINT/SIGTERM listeners (then exits after
  // draining). No artificial keep-alive timer is needed.
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
