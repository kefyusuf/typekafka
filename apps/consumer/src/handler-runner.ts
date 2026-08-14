import type {
  ConsumeContext,
  ConsumeHandler,
  IMessageBroker,
} from '@nodejs-kafka/broker';
import type { AppLogger } from '@nodejs-kafka/infra';
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
      await dlq.deadLetter(message, error, 0);
      await context.commit();
      return;
    }

    // --- 2. retry business handler ---
    try {
      await withRetry(
        () => config.handler(payload),
        { attempts, baseDelayMs },
        (state, error) => {
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
      await dlq.deadLetter(message, error, attempts);
      await context.commit();
      return;
    }

    // --- 4. commit on success ---
    await context.commit();
  };
}
