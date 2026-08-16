import type { ConsumeContext, ConsumeHandler } from '@nodejs-kafka/broker';
import type {
  AppLogger,
  DlqManager,
  RetryTopicScheduler,
  TelemetryClient,
} from '@nodejs-kafka/infra';
import { withRetry } from '@nodejs-kafka/infra';
import { setTimeout as sleep } from 'node:timers/promises';

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
    const asRecord = (value: unknown): Record<string, unknown> =>
      typeof value === 'object' && value !== null
        ? (value as Record<string, unknown>)
        : {};
    const raw = asRecord(message.value);
    const eventId = typeof raw['eventId'] === 'string' ? raw['eventId'] : 'unknown';
    const orderId = typeof raw['orderId'] === 'string' ? raw['orderId'] : 'unknown';
    const telemetry = config.telemetry;
    const groupLabel = config.groupId ?? 'consumer';

    // --- 1. retry headers + parking ---
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
        eventId,
        orderId,
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
        eventId,
        orderId,
        partition: message.partition,
        offset: message.offset,
        message: 'Retry payload failed schema validation — sent straight to DLQ',
        concept: 'schema-validation',
      });
      await dlq.deadLetter(message, error, retryCount);
      await context.commit();
      return;
    }

    await telemetry?.emit({
      type: 'consumed',
      topic: message.topic,
      eventId,
      orderId,
      partition: message.partition,
      offset: message.offset,
      message: `Retry message delivered to consumer group '${groupLabel}'`,
      concept: 'retry-topic',
    });

    // --- 3. retry business handler ---
    try {
      await withRetry(
        () => config.handler(payload),
        { attempts, baseDelayMs },
        (state, error) => {
          void telemetry?.emit({
            type: 'retrying',
            topic: message.topic,
            eventId,
            orderId,
            partition: message.partition,
            offset: message.offset,
            attempt: state.attempt,
            message: `Handler failed (attempt ${state.attempt}) — backing off`,
            concept: 'retry',
          });
          logger.warn(
            {
              topic: message.topic,
              partition: message.partition,
              offset: message.offset,
              attempt: state.attempt,
              nextDelayMs: state.delayMs,
              err: error,
            },
            'retry handler failed, will retry',
          );
        },
      );
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
          eventId,
          orderId,
          partition: message.partition,
          offset: message.offset,
          message: 'Handler exhausted retries — message sent to orders.dlq',
          concept: 'dead-letter-queue',
        });
        await dlq.deadLetter(message, error, retryCount);
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
        eventId,
        orderId,
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
    await context.commit();
    await telemetry?.emit({
      type: 'committed',
      topic: message.topic,
      eventId,
      orderId,
      partition: message.partition,
      offset: message.offset,
      message: 'Offset committed — message fully processed',
      concept: 'offset-commit',
    });
  };
}
