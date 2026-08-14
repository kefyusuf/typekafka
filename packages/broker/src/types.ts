/**
 * Broker-agnostic message envelope.
 * Each driver adapter is responsible for mapping its native message
 * representation (kafkajs, confluent, in-memory, ...) to this shape.
 */
export interface KafkaMessage<T = unknown> {
  topic: string;
  key: string | null;
  value: T;
  headers?: Record<string, string | string[]>;
  partition: number;
  offset: string;
  timestamp: string;
}

export interface ProduceOptions {
  key?: string | null;
  headers?: Record<string, string | string[]>;
  partition?: number;
  /** Enables broker-level idempotence where supported. Defaults to true. */
  idempotent?: boolean;
}

export interface ProduceResult {
  topic: string;
  partition: number;
  offset: string;
}

export interface ConsumeOptions {
  /** Consumer group id. In-memory driver uses it to route messages. */
  groupId?: string;
  /** Number of concurrent handler invocations. Defaults to 1. */
  concurrency?: number;
  /** When true, offsets are committed only after the handler resolves. */
  manualCommit?: boolean;
  fromBeginning?: boolean;
}

export interface TopicConfig {
  name: string;
  numPartitions?: number;
  replicationFactor?: number;
  /** Optional per-topic config overrides (retention, cleanup, ...). */
  configEntries?: Record<string, string>;
}

export interface TopicOptions {
  fromBeginning?: boolean;
}

/**
 * A consumer callback. Implementations may call `commit()` to acknowledge
 * the message. When `manualCommit` is disabled, the adapter commits on resolve.
 */
export interface ConsumeContext {
  /** Explicitly acknowledge the message (manual offset commit). */
  commit: () => Promise<void>;
  /** Called when the message could not be processed (-> DLQ). */
  nack: () => Promise<void>;
}

export type ConsumeHandler<T> = (
  message: KafkaMessage<T>,
  context: ConsumeContext,
) => Promise<void>;

export type Disposer = () => Promise<void>;
