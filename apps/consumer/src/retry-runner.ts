import type { ConsumeContext, ConsumeHandler } from '@nodejs-kafka/broker';
import type {
  AppLogger,
  DlqManager,
  IdempotencyFilter,
  RetryTopicScheduler,
  TelemetryClient,
} from '@nodejs-kafka/infra';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  extractMessageMeta,
  runHandlerWithRetry,
  safeDeadLetter,
  type ConsumerMetrics,
} from './pipeline-shared.js';

export interface RetryHandlerConfig<T> {
  /** Validates/transforms the raw payload before calling `handler`. */
  parse: (value: unknown) => T;
  /** Business handler, receives the validated payload. */
  handler: (payload: T) => Promise<void>;
  /** In-process retry attempts per retry-topic delivery. Defaults to 2. */
  attempts?: number;
  baseDelayMs?: number;
  /** Optional telemetry emitter; each pipeline step publishes an event. */
  telemetry?: TelemetryClient;
  /** Consumer group id, used in telemetry copy. */
  groupId?: string;
  /** Optional prometheus metrics; skipped when not provided. */
  metrics?: ConsumerMetrics;
  /** Optional idempotency filter; skips already-processed event ids. */
  idempotency?: IdempotencyFilter;
}

/**
 * Wraps a single handler with the retry-topic delivery pipeline:
 *
 *   1. park   -> hold not-yet-due messages until `next-deliver-at`
 *   2. parse  -> schema validation (invalid messages go straight to DLQ)
 *   3. retry  -> exponential backoff for transient failures
 *   4. route  -> re-schedule on the retry topic, or dead-letter at max deliveries
 *   5. commit -> offset committed only after the outcome is published
 */
export function createRetryTopicRunner<T>(
  scheduler: RetryTopicScheduler,
  dlq: DlqManager,
  config: RetryHandlerConfig<T>,
  logger: AppLogger,
): ConsumeHandler<unknown> {
  const attempts = config.attempts ?? 2;
  const baseDelayMs = config.baseDelayMs ?? 100;

  return async (message, context: ConsumeContext) => {
    const meta = extractMessageMeta(message);
    const telemetry = config.telemetry;
    const metrics = config.metrics;
    const idempotency = config.idempotency;
    const groupLabel = config.groupId ?? 'consumer';

    // --- 1. retry headers + parking ---
    // A not-yet-due message is parked in place: we sleep until `next-deliver-at`
    // and then process it. The offset is intentionally left uncommitted during
    // the park, so a crash merely redelivers the message (at-least-once) rather
    // than dropping a retry hop. The alternative — committing before parking and
    // re-scheduling — risks *losing* the delivery if we crash before the
    // re-schedule lands, so the in-place park is the safer choice.
    const { retryCount, nextDeliverAtMs } = scheduler.parseRetryHeaders(message.headers);
    const parkMs = scheduler.parkDelayMs(retryCount, nextDeliverAtMs);
    if (parkMs > 0) {
      logger.info(
        {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          retryCount,
          parkMs,
        },
        'retry message parked until next-deliver-at',
      );
      await telemetry?.emit({
        type: 'retry-parked',
        topic: message.topic,
        eventId: meta.eventId,
        orderId: meta.orderId,
        partition: message.partition,
        offset: message.offset,
        message: `Retry message parked until next-deliver-at (${parkMs}ms)`,
        concept: 'retry-topic',
      });
      await sleep(parkMs);
    }

    // --- 2. parse / validate ---
    let payload: T;
    try {
      payload = config.parse(message.value);
    } catch (error) {
      logger.warn(
        {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          retryCount,
          err: error,
        },
        'invalid retry message, sending to DLQ',
      );
      await telemetry?.emit({
        type: 'invalid-to-dlq',
        topic: message.topic,
        eventId: meta.eventId,
        orderId: meta.orderId,
        partition: message.partition,
        offset: message.offset,
        message: 'Retry payload failed schema validation — sent straight to DLQ',
        concept: 'schema-validation',
      });
      metrics?.dlqTotal.labels({ topic: message.topic }).inc();
      await safeDeadLetter(dlq, message, error, retryCount, logger);
      await context.commit();
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
      message: `Retry message delivered to consumer group '${groupLabel}'`,
      concept: 'retry-topic',
    });

    // --- 3. retry business handler ---
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
        retryLogMessage: 'retry handler failed, will retry',
      });
    } catch (error) {
      // --- 4. exhausted: re-schedule or dead-letter at max deliveries ---
      if (scheduler.isMaxRetries(retryCount)) {
        logger.error(
          {
            topic: message.topic,
            partition: message.partition,
            offset: message.offset,
            retryCount,
            err: error,
          },
          'retry message exhausted max deliveries, sending to DLQ',
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
        await safeDeadLetter(dlq, message, error, retryCount, logger);
        await context.commit();
        return;
      }

      logger.warn(
        {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          retryCount,
          err: error,
        },
        'retry handler exhausted, re-scheduling on retry topic',
      );
      await telemetry?.emit({
        type: 'retry-scheduled',
        topic: message.topic,
        eventId: meta.eventId,
        orderId: meta.orderId,
        partition: message.partition,
        offset: message.offset,
        message: 'Handler exhausted in-process retries — re-scheduled on orders.retry',
        concept: 'scheduled-retry',
      });
      await scheduler.schedule(message, error, retryCount);
      await context.commit();
      return;
    }

    // --- 5. commit on success ---
    // Mark only after successful processing so genuine in-flight retries
    // (which have not completed) are never suppressed by the idempotency guard.
    if (meta.eventId !== 'unknown') idempotency?.mark(meta.eventId);
    await context.commit();
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
}
