import type { ConsumeContext, ConsumeHandler } from '@typekafka/broker';
import { TOPIC_ORDER_CREATED } from '@typekafka/domain';
import type {
  AppLogger,
  DlqManager,
  IdempotencyFilter,
  RetryTopicScheduler,
  TelemetryClient,
} from '@typekafka/infra';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  extractMessageMeta,
  runHandlerWithRetry,
  safeDeadLetter,
  type ConsumerMetrics,
} from './pipeline-shared.js';

export interface RetryHandlerConfig<T> {
  /**
   * Validates/transforms the raw payload before calling `handler`. Receives
   * the original source topic (from the `retry.original-topic` header) so the
   * runner can parse a parked message with the schema of the topic it was
   * first published to — not a hard-coded one.
   */
  parse: (value: unknown, originalTopic: string) => T;
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
    // The topic the message was first published to (carried on the
    // `retry.original-topic` header). Used to parse the parked payload with the
    // correct schema instead of assuming a single source topic.
    const originalTopic = scheduler.parseOriginalTopic(message.headers) ?? TOPIC_ORDER_CREATED;

    // --- 1. retry headers + delayed-requeue ---
    // A not-yet-due message is re-queued onto the retry topic (preserving its
    // retry count and scheduled time) and the offset is committed, so the
    // partition is freed immediately instead of being blocked by an
    // in-handler `await sleep(parkMs)` (head-of-line blocking / rebalance
    // stall). The retry topic redelivers the message once it is due.
    //
    // The wait is throttled by a bounded sleep rather than a single long one:
    // a full `sleep(parkMs)` would block the partition for the whole backoff,
    // while no sleep at all would busy-loop on low-latency brokers (and starve
    // the event loop). REQUEUE_THROTTLE_MS caps each hop; across hops the
    // cumulative wait tracks `next-deliver-at`. Bounded by maxDeliveries, so a
    // permanently-stuck message escalates to the DLQ rather than requeueing
    // forever.
    const REQUEUE_THROTTLE_MS = 1_000;
    const { retryCount, nextDeliverAtMs } = scheduler.parseRetryHeaders(message.headers);
    const parkMs = scheduler.parkDelayMs(retryCount, nextDeliverAtMs);
    if (parkMs > 0) {
      logger.info(
        {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
          retryCount,
          nextDeliverAtMs,
        },
        'retry message not yet due — re-queuing onto retry topic',
      );
      await telemetry?.emit({
        type: 'retry-parked',
        topic: message.topic,
        eventId: meta.eventId,
        orderId: meta.orderId,
        partition: message.partition,
        offset: message.offset,
        message: `Retry message not yet due — re-queued onto ${scheduler.topicName} (due in ${parkMs}ms)`,
        concept: 'retry-topic',
      });
      await sleep(Math.min(parkMs, REQUEUE_THROTTLE_MS));
      await scheduler.requeue(message, retryCount, nextDeliverAtMs);
      await context.commit();
      return;
    }

    // --- 2. parse / validate ---
    let payload: T;
    try {
      payload = config.parse(message.value, originalTopic);
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
