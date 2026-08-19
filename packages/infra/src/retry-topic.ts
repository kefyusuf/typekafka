import type { IMessageBroker, KafkaMessage } from '@nodejs-kafka/broker';
import { RETRY_TOPIC } from '@nodejs-kafka/domain';

export const RETRY_COUNT_HEADER = 'retry-count';
export const NEXT_DELIVER_AT_HEADER = 'next-deliver-at';
export const RETRY_ORIGINAL_TOPIC_HEADER = 'retry.original-topic';

export interface RetryTopicPolicy {
  /** Escalating delays (ms) before each retry-topic delivery. Default [2000, 10000, 60000]. */
  delaysMs?: number[];
  /** Max retry-topic deliveries before DLQ. Defaults to delaysMs.length. */
  maxDeliveries?: number;
}

export const DEFAULT_RETRY_DELAYS_MS = [2_000, 10_000, 60_000];

export interface RetryHeaders {
  retryCount: number;
  nextDeliverAtMs: number;
}

/**
 * Schedules failed messages onto the retry topic with `retry-count` and
 * `next-deliver-at` headers so a retry-topic consumer can park not-yet-due
 * messages and re-process due ones. Escalating delays, bounded by a max
 * delivery count (beyond which the message must go to the DLQ).
 */
export class RetryTopicScheduler {
  private readonly topic: string;
  private readonly delaysMs: number[];
  private readonly max: number;

  constructor(
    private readonly broker: IMessageBroker,
    options?: { topic?: string; policy?: RetryTopicPolicy },
  ) {
    this.topic = options?.topic ?? RETRY_TOPIC;
    this.delaysMs = options?.policy?.delaysMs ?? DEFAULT_RETRY_DELAYS_MS;
    this.max = options?.policy?.maxDeliveries ?? this.delaysMs.length;
  }

  get topicName(): string {
    return this.topic;
  }

  get maxDeliveries(): number {
    return this.max;
  }

  nextDelayMs(priorDeliveries: number): number {
    const index = Math.min(Math.max(0, priorDeliveries), this.delaysMs.length - 1);
    return this.delaysMs[index] ?? 0;
  }

  isMaxRetries(retryCount: number): boolean {
    return retryCount >= this.max;
  }

  parseRetryHeaders(headers?: Record<string, string | string[]>): RetryHeaders {
    const rawCount = pickHeader(headers, RETRY_COUNT_HEADER);
    const rawNext = pickHeader(headers, NEXT_DELIVER_AT_HEADER);

    let retryCount = 0;
    if (rawCount !== undefined) {
      const parsedCount = parseInt(rawCount, 10);
      if (Number.isFinite(parsedCount) && rawCount.trim() !== '') retryCount = parsedCount;
    }
    retryCount = Math.max(0, retryCount);

    let nextDeliverAtMs = 0;
    if (rawNext !== undefined) {
      const asNumber = Number(rawNext);
      if (Number.isFinite(asNumber) && rawNext.trim() !== '') {
        nextDeliverAtMs = asNumber;
      } else {
        const parsed = Date.parse(rawNext);
        if (Number.isFinite(parsed)) nextDeliverAtMs = parsed;
      }
    }

    return { retryCount, nextDeliverAtMs: Math.max(0, nextDeliverAtMs) };
  }

  parkDelayMs(retryCount: number, nextDeliverAtMs: number, nowMs?: number): number {
    return Math.max(0, nextDeliverAtMs - (nowMs ?? Date.now()));
  }

  /** Read the original source topic a retry message was parked from. */
  parseOriginalTopic(headers?: Record<string, string | string[]>): string | undefined {
    return pickHeader(headers, RETRY_ORIGINAL_TOPIC_HEADER);
  }

  async ensureTopic(): Promise<void> {
    await this.broker.createTopics([
      { name: this.topic, numPartitions: 3, replicationFactor: 1 },
    ]);
  }

  /**
   * Publish `message` to the retry topic after `priorDeliveries` earlier
   * retry-topic deliveries, escalating the retry count and pushing the next
   * scheduled delivery time forward. Called when a handler fails.
   */
  async schedule(
    message: KafkaMessage<unknown>,
    _error: unknown,
    priorDeliveries: number,
  ): Promise<void> {
    const retryCount = priorDeliveries + 1;
    const delayMs = this.nextDelayMs(priorDeliveries);
    const nextDeliverAt = new Date(Date.now() + delayMs).toISOString();

    await this.broker.produce(this.topic, message.value, {
      key: message.key ?? undefined,
      headers: {
        [RETRY_COUNT_HEADER]: String(retryCount),
        [NEXT_DELIVER_AT_HEADER]: nextDeliverAt,
        [RETRY_ORIGINAL_TOPIC_HEADER]: message.topic,
      },
    });
  }

  /**
   * Re-publish `message` onto the retry topic WITHOUT escalating the retry
   * count or advancing the scheduled time — used to hold a not-yet-due message
   * until its `next-deliver-at`. The consumer commits and returns, so the
   * partition is freed immediately (no in-handler sleep / head-of-line
   * blocking) and the retry topic simply redelivers the message once it is due.
   */
  async requeue(
    message: KafkaMessage<unknown>,
    retryCount: number,
    nextDeliverAtMs: number,
  ): Promise<void> {
    await this.broker.produce(this.topic, message.value, {
      key: message.key ?? undefined,
      headers: {
        [RETRY_COUNT_HEADER]: String(retryCount),
        [NEXT_DELIVER_AT_HEADER]: new Date(nextDeliverAtMs).toISOString(),
        [RETRY_ORIGINAL_TOPIC_HEADER]: message.topic,
      },
    });
  }
}

function pickHeader(
  headers: Record<string, string | string[]> | undefined,
  name: string,
): string | undefined {
  const value = headers?.[name];
  if (value === undefined || value === null) return undefined;
  return Array.isArray(value) ? (value[0] as string | undefined) : value;
}

export function createRetryTopicScheduler(
  broker: IMessageBroker,
  options?: { topic?: string; policy?: RetryTopicPolicy },
): RetryTopicScheduler {
  return new RetryTopicScheduler(broker, options);
}
