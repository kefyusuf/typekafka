import type {
  ConsumeContext,
  ConsumeHandler,
  IMessageBroker,
} from '@nodejs-kafka/broker';
import type { AppLogger, TelemetryClient } from '@nodejs-kafka/infra';
import { withRetry } from '@nodejs-kafka/infra';
import { DlqManager } from '@nodejs-kafka/infra';

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
    const asRecord = (value: unknown): Record<string, unknown> =>
      typeof value === 'object' && value !== null
        ? (value as Record<string, unknown>)
        : {};
    const raw = asRecord(message.value);
    const eventId = typeof raw['eventId'] === 'string' ? raw['eventId'] : 'unknown';
    const orderId = typeof raw['orderId'] === 'string' ? raw['orderId'] : 'unknown';
    const telemetry = config.telemetry;
    const groupLabel = config.groupId ?? 'consumer';

    const emitCommitted = async (): Promise<void> => {
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
        eventId,
        orderId,
        partition: message.partition,
        offset: message.offset,
        message: 'Payload failed schema validation — sent straight to DLQ',
        concept: 'schema-validation',
      });
      await dlq.deadLetter(message, error, 0);
      await context.commit();
      await emitCommitted();
      return;
    }

    await telemetry?.emit({
      type: 'consumed',
      topic: message.topic,
      eventId,
      orderId,
      partition: message.partition,
      offset: message.offset,
      message: `Message delivered to consumer group '${groupLabel}'`,
      concept: 'consumer-group',
    });
    await telemetry?.emit({
      type: 'parsed',
      topic: message.topic,
      eventId,
      orderId,
      partition: message.partition,
      offset: message.offset,
      message: 'Payload validated against its Zod schema',
      concept: 'schema-validation',
    });

    // --- 2. retry business handler ---
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
            'handler failed, will retry',
          );
        },
      );
    } catch (error) {
      // --- 3. dead letter after retries exhausted ---
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
        eventId,
        orderId,
        partition: message.partition,
        offset: message.offset,
        message: 'Handler exhausted retries — message sent to orders.dlq',
        concept: 'dead-letter-queue',
      });
      await dlq.deadLetter(message, error, attempts);
      await context.commit();
      await emitCommitted();
      return;
    }

    // --- 4. commit on success ---
    await context.commit();
    await emitCommitted();
  };
}
