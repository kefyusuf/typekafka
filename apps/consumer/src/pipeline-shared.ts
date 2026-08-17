import type { AppLogger, DlqManager, TelemetryClient } from '@nodejs-kafka/infra';
import type { KafkaMessage } from '@nodejs-kafka/broker';
import type { Counter, Histogram } from 'prom-client';
import { withRetry } from '@nodejs-kafka/infra';

export interface ConsumerMetrics {
  messagesConsumed: Counter<string>;
  handlerDurationMs: Histogram<string>;
  retriesTotal: Counter<string>;
  dlqTotal: Counter<string>;
}

export interface MessageMeta {
  raw: Record<string, unknown>;
  eventId: string;
  orderId: string;
}

/** Pull `eventId`/`orderId` off the raw payload for logs + telemetry. */
export function extractMessageMeta(message: KafkaMessage<unknown>): MessageMeta {
  const raw =
    typeof message.value === 'object' && message.value !== null
      ? (message.value as Record<string, unknown>)
      : {};
  const eventId = typeof raw['eventId'] === 'string' ? raw['eventId'] : 'unknown';
  const orderId = typeof raw['orderId'] === 'string' ? raw['orderId'] : 'unknown';
  return { raw, eventId, orderId };
}

export interface RunHandlerWithRetryArgs<T> {
  message: KafkaMessage<unknown>;
  meta: MessageMeta;
  handler: () => Promise<T>;
  attempts: number;
  baseDelayMs: number;
  metrics?: ConsumerMetrics;
  telemetry?: TelemetryClient;
  logger: AppLogger;
  /** Log message emitted on each retry attempt (varies by runner). */
  retryLogMessage: string;
}

/**
 * Shared `parse -> retry(handler) -> metrics/telemetry` step used by both the
 * handler and retry-topic runners, so the retry/backoff/observability logic
 * lives in exactly one place. Throws after `attempts` are exhausted so the
 * caller can route to the retry topic or the DLQ.
 */
export async function runHandlerWithRetry<T>({
  message,
  meta,
  handler,
  attempts,
  baseDelayMs,
  metrics,
  telemetry,
  logger,
  retryLogMessage,
}: RunHandlerWithRetryArgs<T>): Promise<void> {
  await withRetry(
    async () => {
      const startedAt = performance.now();
      try {
        return await handler();
      } finally {
        metrics?.handlerDurationMs
          .labels({ topic: message.topic })
          .observe(performance.now() - startedAt);
      }
    },
    { attempts, baseDelayMs },
    (state, error) => {
      metrics?.retriesTotal.labels({ topic: message.topic }).inc();
      void telemetry?.emit({
        type: 'retrying',
        topic: message.topic,
        eventId: meta.eventId,
        orderId: meta.orderId,
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
        retryLogMessage,
      );
    },
  );
}

/**
 * Write a message to the DLQ, tolerating a DLQ-produce failure. A failing
 * `deadLetter` must NOT block the offset commit, otherwise a poison message
 * is redelivered forever and wedges its partition. On DLQ failure we still
 * commit so the message leaves the consumer and is not redelivered.
 */
export async function safeDeadLetter(
  dlq: DlqManager,
  message: KafkaMessage<unknown>,
  error: unknown,
  attempts: number,
  logger: AppLogger,
): Promise<void> {
  try {
    await dlq.deadLetter(message, error, attempts);
  } catch (dlqErr) {
    logger.error(
      {
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
        err: dlqErr,
      },
      'dead-letter write failed; committing to avoid infinite redelivery of a poison message',
    );
  }
}
