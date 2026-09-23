import type { ConsumeContext, ConsumeHandler, IMessageBroker } from '@typekafka/broker';
import type {
  AppLogger,
  IdempotencyFilter,
  RetryTopicScheduler,
  TelemetryClient,
} from '@typekafka/infra';
import { DlqManager } from '@typekafka/infra';
import {
  extractMessageMeta,
  runHandlerWithRetry,
  safeDeadLetter,
  type ConsumerMetrics,
} from './pipeline-shared.js';

export type { ConsumerMetrics } from './pipeline-shared.js';

export interface HandlerConfig<T> {
  /** Validates/transforms the raw payload before calling `handler`. */
  parse: (value: unknown) => T;
  /** Business handler, receives the validated payload. */
  handler: (payload: T) => Promise<void>;
  /** Retry attempts before sending to DLQ. Defaults to 3. */
  attempts?: number;
  baseDelayMs?: number;
  /** Optional telemetry emitter; each pipeline step publishes an event. */
  telemetry?: TelemetryClient;
  /** Consumer group id, used in telemetry copy. */
  groupId?: string;
  /**
   * Optional scheduler: when set, exhausted retries are published to the retry
   * topic instead of the DLQ (offset committed either way).
   */
  retryScheduler?: RetryTopicScheduler;
  /** Optional prometheus metrics; skipped when not provided. */
  metrics?: ConsumerMetrics;
  /** Optional idempotency filter; skips already-processed event ids. */
  idempotency?: IdempotencyFilter;
}

/**
 * Wraps a single handler with the full production pipeline:
 *
 *   1. parse   -> schema validation (invalid messages go straight to DLQ)
 *   2. retry   -> exponential backoff for transient failures
 *   3. dlq     -> dead-letter topic after attempts are exhausted
 *   4. commit  -> offset committed only on success
 */
export function createHandlerRunner<T>(
  broker: IMessageBroker,
  dlq: DlqManager,
  config: HandlerConfig<T>,
  logger: AppLogger,
): ConsumeHandler<unknown> {
  const attempts = config.attempts ?? 3;
  const baseDelayMs = config.baseDelayMs ?? 100;

  return async (message, context: ConsumeContext) => {
    const meta = extractMessageMeta(message);
    const telemetry = config.telemetry;
    const metrics = config.metrics;
    const idempotency = config.idempotency;
    const groupLabel = config.groupId ?? 'consumer';

    const emitCommitted = async (): Promise<void> => {
      await telemetry?.emit({
        type: 'committed',
        topic: message.topic,
        eventId: meta.eventId,
        orderId: meta.orderId,
        partition: message.partition,
        offset: message.offset,
        message: 'Offset committed — message fully processed',
        concept: 'offset-commit',
      });
    };

    // --- 1. parse / validate ---
    let payload: T;
    try {
      payload = config.parse(message.value);
    } catch (error) {
      logger.warn(
        {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          err: error,
        },
        'invalid message, sending to DLQ',
      );
      await telemetry?.emit({
        type: 'invalid-to-dlq',
        topic: message.topic,
        eventId: meta.eventId,
        orderId: meta.orderId,
        partition: message.partition,
        offset: message.offset,
        message: 'Payload failed schema validation — sent straight to DLQ',
        concept: 'schema-validation',
      });
      metrics?.dlqTotal.labels({ topic: message.topic }).inc();
      await safeDeadLetter(dlq, message, error, 0, logger);
      await context.commit();
      await emitCommitted();
      return;
    }

    // --- idempotency guard (at-least-once safety) ---
    // Only already-completed event ids are skipped; a transient retry has not
    // been marked yet, so genuine retries are never suppressed.
    if (meta.eventId !== 'unknown' && idempotency?.isDuplicate(meta.eventId)) {
      logger.debug(
        { topic: message.topic, partition: message.partition, offset: message.offset, eventId: meta.eventId },
        'duplicate eventId already processed, skipping',
      );
      await telemetry?.emit({
        type: 'duplicate-skipped',
        topic: message.topic,
        eventId: meta.eventId,
        orderId: meta.orderId,
        partition: message.partition,
        offset: message.offset,
        message: 'Duplicate eventId already processed — skipped (idempotency)',
        concept: 'idempotency',
      });
      metrics?.idempotencySkipped.labels({ topic: message.topic }).inc();
      await context.commit();
      await emitCommitted();
      return;
    }

    metrics?.messagesConsumed.labels({ topic: message.topic }).inc();

    await telemetry?.emit({
      type: 'consumed',
      topic: message.topic,
      eventId: meta.eventId,
      orderId: meta.orderId,
      partition: message.partition,
      offset: message.offset,
      message: `Message delivered to consumer group '${groupLabel}'`,
      concept: 'consumer-group',
    });
    await telemetry?.emit({
      type: 'parsed',
      topic: message.topic,
      eventId: meta.eventId,
      orderId: meta.orderId,
      partition: message.partition,
      offset: message.offset,
      message: 'Payload validated against its Zod schema',
      concept: 'schema-validation',
    });

    // --- 2. retry business handler ---
    try {
      await runHandlerWithRetry({
        message,
        meta,
        handler: () => config.handler(payload),
        attempts,
        baseDelayMs,
        metrics,
        telemetry,
        logger,
        retryLogMessage: 'handler failed, will retry',
      });
    } catch (error) {
      // --- 3. retry topic or dead-letter after retries exhausted ---
      if (config.retryScheduler) {
        logger.warn(
          {
            topic: message.topic,
            partition: message.partition,
            offset: message.offset,
            err: error,
          },
          'handler exhausted retries, scheduling on retry topic',
        );
        await telemetry?.emit({
          type: 'retry-scheduled',
          topic: message.topic,
          eventId: meta.eventId,
          orderId: meta.orderId,
          partition: message.partition,
          offset: message.offset,
          message: 'Handler exhausted in-process retries — published to orders.retry',
          concept: 'scheduled-retry',
        });
        await config.retryScheduler.schedule(message, error, 0);
        await context.commit();
        await emitCommitted();
        return;
      }
      logger.error(
        {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          err: error,
        },
        'handler exhausted retries, sending to DLQ',
      );
      await telemetry?.emit({
        type: 'dead-lettered',
        topic: message.topic,
        eventId: meta.eventId,
        orderId: meta.orderId,
        partition: message.partition,
        offset: message.offset,
        message: 'Handler exhausted retries — message sent to orders.dlq',
        concept: 'dead-letter-queue',
      });
      metrics?.dlqTotal.labels({ topic: message.topic }).inc();
      await safeDeadLetter(dlq, message, error, attempts, logger);
      await context.commit();
      await emitCommitted();
      return;
    }

    // --- 4. commit on success ---
    // Mark only after successful processing so genuine in-flight retries
    // (which have not completed) are never suppressed by the idempotency guard.
    if (meta.eventId !== 'unknown') idempotency?.mark(meta.eventId);
    await context.commit();
    await emitCommitted();
  };
}
