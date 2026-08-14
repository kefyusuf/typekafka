import type { IMessageBroker, KafkaMessage } from '@nodejs-kafka/broker';
import { DLQ_TOPIC } from '@nodejs-kafka/domain';

export interface DlqEntry {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  error: string;
  errorType: string;
  attempts: number;
  failedAt: string;
  original: unknown;
}

export interface DlqManagerOptions {
  topic?: string;
}

/**
 * Writes failed messages to a dead-letter topic so they are never silently
 * dropped. Keeps the original payload plus diagnostics for offline replay.
 */
export class DlqManager {
  private readonly topic: string;

  constructor(
    private readonly broker: IMessageBroker,
    options: DlqManagerOptions = {},
  ) {
    this.topic = options.topic ?? DLQ_TOPIC;
  }

  get topicName(): string {
    return this.topic;
  }

  async ensureTopic(): Promise<void> {
    await this.broker.createTopics([
      { name: this.topic, numPartitions: 3, replicationFactor: 1 },
    ]);
  }

  async deadLetter(message: KafkaMessage<unknown>, error: unknown, attempts: number): Promise<void> {
    const entry: DlqEntry = {
      topic: message.topic ?? 'unknown',
      partition: message.partition,
      offset: message.offset,
      key: message.key,
      error: error instanceof Error ? error.message : String(error),
      errorType: error instanceof Error ? error.name : 'UnknownError',
      attempts,
      failedAt: new Date().toISOString(),
      original: message.value,
    };

    await this.broker.produce(this.topic, entry, {
      key: message.key ?? undefined,
      headers: {
        'dlq.original-topic': message.topic ?? '',
        'dlq.error-type': entry.errorType,
      },
    });
  }
}
