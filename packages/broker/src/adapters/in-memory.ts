import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import type { IMessageBroker } from '../port.js';
import type {
  ConsumeContext,
  ConsumeHandler,
  ConsumeOptions,
  Disposer,
  KafkaMessage,
  MessageTransaction,
  ProduceOptions,
  ProduceResult,
  TopicConfig,
} from '../types.js';
import type { BrokerConfig } from '../config.js';
import { JsonCodec, type MessageCodec } from '../codec/index.js';
import { BrokerError, BrokerStateError } from '../errors.js';
import { extractParentContext, injectTraceContext, withSpan } from '../trace.js';

interface StoredRecord {
  id: string;
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  value: Buffer | string | null;
  headers?: Record<string, string | string[]>;
  timestamp: string;
  sequence: number;
}

interface Subscription {
  topics: Set<string>;
  handler: ConsumeHandler<unknown>;
  options: ConsumeOptions;
  fromSequence: number;
  disposed: boolean;
  /** Number of handler invocations currently in flight (concurrency pool). */
  active: number;
  /** Records waiting for a free concurrency slot. */
  queue: StoredRecord[];
}

/** Simple murmur2-like string hash used to pick a partition (stable for tests). */
function partitionForKey(key: string | null | undefined, partitions: number): number {
  if (key === null || key === undefined) return 0;
  let hash = 0;
  for (let i = 0; i < key.length; i++) {
    hash = (hash * 31 + key.charCodeAt(i)) | 0;
  }
  return ((hash % partitions) + partitions) % partitions;
}

/**
 * Zero-dependency in-memory broker used for local development and tests.
 *
 * It implements the same port contract as the real Kafka adapter so you can
 * swap drivers via `BROKER_DRIVER=in-memory|confluent` without touching
 * application code. Messages are kept in memory, offset semantics are
 * emulated per (topic, partition).
 */
export class InMemoryBrokerAdapter implements IMessageBroker {
  private readonly emitter = new EventEmitter();
  private topics = new Map<string, { partitions: number }>();
  private records: StoredRecord[] = [];
  private nextSequence = 0;
  private connected = false;

  private readonly codec: MessageCodec;

  constructor(private readonly config: BrokerConfig) {
    this.codec = config.codec ?? new JsonCodec();
  }

  get isConnected(): boolean {
    return this.connected;
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    this.connected = true;
    this.topics = new Map(this.topics);
  }

  async disconnect(): Promise<void> {
    this.connected = false;
    this.records = [];
    this.topics = new Map();
    this.emitter.removeAllListeners();
  }

  async createTopics(topics: TopicConfig[]): Promise<void> {
    for (const topic of topics) {
      const existing = this.topics.get(topic.name);
      const partitions = topic.numPartitions ?? 1;
      this.topics.set(topic.name, { partitions: existing?.partitions ?? partitions });
    }
  }

  async listTopics(): Promise<string[]> {
    return [...this.topics.keys()];
  }

  async produce<T>(
    topic: string,
    value: T,
    options: ProduceOptions = {},
  ): Promise<ProduceResult> {
    const topicMeta = this.topics.get(topic);
    if (!topicMeta) {
      throw new BrokerStateError(`Topic "${topic}" does not exist. Create it first.`);
    }

    const partition =
      options.partition ??
      partitionForKey(options.key ?? null, topicMeta.partitions);

    return withSpan(
      'produce',
      {
        'messaging.system': 'kafka',
        'messaging.destination': topic,
        'messaging.destination_partition': partition,
      },
      async () => {
        const lastOffset =
          this.records
            .filter((r) => r.topic === topic && r.partition === partition)
            .reduce((max, r) => Math.max(max, r.offset), -1) + 1;

        const record: StoredRecord = {
          id: randomUUID(),
          topic,
          partition,
          offset: lastOffset,
          key: options.key ?? null,
          value: await this.codec.serialize(topic, value),
          headers: injectTraceContext(options.headers),
          timestamp: new Date().toISOString(),
          sequence: this.nextSequence++,
        };

        this.records.push(record);
        this.emitter.emit('message', record);
        this.emitter.emit(`message:${topic}`, record);

        return { topic, partition, offset: String(lastOffset) };
      },
    );
  }

  async consume<T>(
    topics: string[],
    handler: ConsumeHandler<T>,
    options: ConsumeOptions = {},
  ): Promise<Disposer> {
    const subscription: Subscription = {
      topics: new Set(topics),
      handler: handler as ConsumeHandler<unknown>,
      options,
      fromSequence: this.nextSequence,
      disposed: false,
      active: 0,
      queue: [],
    };

    const onMessage = (record: StoredRecord) => {
      if (!subscription.topics.has(record.topic)) return;
      if (record.sequence < subscription.fromSequence) return;
      this.enqueueDispatch(record, subscription);
    };

    this.emitter.on('message', onMessage);

    // Deliver historical records only when the caller opted in
    // (a fresh consumer group reading from the beginning).
    if (subscription.options.fromBeginning !== false) {
      for (const record of this.records) {
        if (record.sequence >= subscription.fromSequence) continue;
        if (!subscription.topics.has(record.topic)) continue;
        this.enqueueDispatch(record, subscription);
      }
    }

    return async () => {
      subscription.disposed = true;
      this.emitter.off('message', onMessage);
    };
  }

  async consumeFromNow<T>(
    topics: string[],
    handler: ConsumeHandler<T>,
    options: ConsumeOptions = {},
  ): Promise<Disposer> {
    return this.consume(topics, handler, { ...options, fromBeginning: false });
  }

  async beginTransaction(): Promise<MessageTransaction> {
    throw new BrokerError(
      'In-memory broker does not support transactions; use BROKER_DRIVER=confluent (see the driver capability matrix).',
    );
  }

  private async dispatch(record: StoredRecord, subscription: Subscription): Promise<void> {
    const message: KafkaMessage<unknown> = {
      topic: record.topic,
      key: record.key,
      value: await this.codec.deserialize(record.topic, record.value),
      headers: record.headers,
      partition: record.partition,
      offset: String(record.offset),
      timestamp: record.timestamp,
    };

    // In-memory driver has no real offsets to commit; the hook exists so the
    // handler pipeline (parse/retry/DLQ/commit) is exercised identically to
    // the real Kafka driver.
    const context: ConsumeContext = {
      commit: async () => {},
    };

    try {
      // Continue the trace that crossed the (in-memory) boundary.
      const parentContext = extractParentContext(message.headers);
      await withSpan(
        'consume',
        {
          'messaging.destination': message.topic,
          topic: message.topic,
          partition: message.partition,
          offset: message.offset,
        },
        () => subscription.handler(message, context),
        parentContext,
      );
    } catch (error) {
      // Handler errors are surfaced through the app's retry/DLQ wrapper.
      // Log so dispatch regressions are visible instead of silently swallowed.
      if (this.config.logger) {
        this.config.logger.warn({ err: error }, 'in-memory dispatch failed');
      } else {
        console.warn('in-memory dispatch failed', error);
      }
    }
  }

  /**
   * Dispatch a record respecting the subscription's `concurrency` limit.
   * With `concurrency <= 1` dispatch is serialized; otherwise a small pool
   * of at most `concurrency` in-flight handlers runs concurrently.
   */
  private enqueueDispatch(record: StoredRecord, subscription: Subscription): void {
    const limit = subscription.options.concurrency ?? 1;
    if (limit <= 1) {
      void this.dispatch(record, subscription).catch(() => {
        /* dispatch handles its own errors */
      });
      return;
    }

    if (subscription.active < limit) {
      subscription.active++;
      void this.runPooled(record, subscription);
    } else {
      subscription.queue.push(record);
    }
  }

  private async runPooled(record: StoredRecord, subscription: Subscription): Promise<void> {
    try {
      await this.dispatch(record, subscription);
    } finally {
      const next = subscription.queue.shift();
      if (next) {
        void this.runPooled(next, subscription);
      } else {
        subscription.active--;
      }
    }
  }
}
